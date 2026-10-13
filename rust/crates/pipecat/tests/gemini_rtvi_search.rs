//! Real native Gemini grounding composed with the public RTVI observer tap.
use pipecat::{
    core::{self, Direction, Frame, FrameData, Pipeline, PipelineConfig},
    rtvi::{GoogleRtviObserver, RtviObserverParams},
    services::{ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider},
};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);

async fn server(packets: Vec<Value>, hold: bool) -> (String, JoinHandle<Value>) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/completion", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(WAIT, async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let mut chunk = [0; 4096];
            let end = loop {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                assert!(bytes.len() <= 64 * 1024);
                if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
            };
            let length: usize = String::from_utf8_lossy(&bytes[..end])
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|value| value.trim().parse().unwrap())
                })
                .unwrap();
            assert!(length <= 64 * 1024);
            while bytes.len() < end + length {
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            let request = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            let body: String = packets.into_iter().map(|packet| format!("data: {packet}\n\n")).collect();
            let length_header = if hold { String::new() } else { format!("Content-Length: {}\r\n", body.len()) };
            let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\n{length_header}Connection: close\r\n\r\n{body}");
            socket.write_all(response.as_bytes()).await.unwrap();
            if hold {
                let mut byte = [0; 1];
                assert_eq!(socket.read(&mut byte).await.unwrap(), 0, "cancel must close the real held socket");
            } else {
                socket.shutdown().await.unwrap();
            }
            request
        })
        .await
        .unwrap()
    });
    (url, task)
}

fn expected() -> Value {
    json!({
        "search_result":"Grounded Rust answer",
        "rendered_content":"<div>Rust source</div>",
        "origins":[{
            "site_uri":"https://example.test/rust",
            "site_title":"Rust source",
            "results":[{"text":"Grounded Rust answer","confidence":[0.95]}]
        }]
    })
}
fn packet() -> Value {
    json!({
        "candidates":[{
            "index":0,
            "content":{"role":"model","parts":[{"text":"Grounded Rust answer"}]},
            "groundingMetadata":{
                "groundingChunks":[{"web":{"uri":"https://example.test/rust","title":"Rust source"}}],
                "groundingSupports":[{"groundingChunkIndices":[0],"segment":{"text":"Grounded Rust answer"},"confidenceScores":[0.95]}],
                "searchEntryPoint":{"renderedContent":"<div>Rust source</div>"}
            },
            "finishReason":"STOP"
        }],
        "usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}
    })
}
async fn start(url: String) -> core::PipelineWorker {
    let mut config = ClientConfig::new(Provider::Gemini, "fixture");
    config.endpoint = url;
    config.api_key = None;
    config.request_timeout = WAIT;
    config.idle_timeout = WAIT;
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let observer = Arc::new(GoogleRtviObserver::new(RtviObserverParams::default(), None).unwrap());
    let worker = timeout(
        WAIT,
        Pipeline::new(vec![
            processor,
            Arc::new(observer.tap("gemini-search-output")),
        ])
        .start(PipelineConfig::default()),
    )
    .await
    .unwrap()
    .unwrap();
    let context = core::LlmContextData {
        messages: vec![core::ChatMessage::new(core::ChatRole::User, "Search Rust").into()],
        tools: core::ToolSet::Schema(core::ToolsSchema {
            standard_tools: vec![],
            custom_tools: Some(BTreeMap::from([(
                core::AdapterType::Gemini,
                vec![BTreeMap::from([("google_search".into(), json!({}))])],
            )])),
        }),
        tool_choice: None,
    };
    timeout(
        WAIT,
        worker.sender().send(
            Frame::new(FrameData::LlmContextDetailed(
                core::DetailedLlmContextFrame {
                    context,
                    speculation: false,
                },
            )),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    worker
}

#[tokio::test]
async fn native_gemini_grounding_reaches_rtvi_without_consuming_search_frame() {
    let (url, server) = server(vec![packet()], false).await;
    let mut worker = start(url).await;
    let (search, wire, text) = timeout(WAIT, async {
        let mut search = vec![];
        let mut wire = vec![];
        let mut text = String::new();
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::LlmSearchResponse(response) => {
                    assert_eq!(frame.direction, Direction::Downstream);
                    search.push(serde_json::to_value(response).unwrap());
                }
                FrameData::OutputTransportMessageUrgent(message)
                    if message["type"] == "bot-llm-search-response" =>
                {
                    wire.push(message);
                }
                FrameData::LlmText(delta) => text.push_str(&delta.text),
                FrameData::Error(error) => panic!("unexpected native error: {error:?}"),
                FrameData::LlmResponseEnd => break,
                _ => {}
            }
        }
        (search, wire, text)
    })
    .await
    .unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, server).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(request["tools"], json!([{"googleSearch":{}}]));
    assert_eq!(text, "Grounded Rust answer");
    assert_eq!(
        search,
        vec![expected()],
        "the original search frame must survive the RTVI tap"
    );
    assert_eq!(
        wire.len(),
        1,
        "real native Gemini grounding must produce one urgent RTVI search response"
    );
    assert_eq!(
        wire[0],
        json!({"label":"rtvi-ai","type":"bot-llm-search-response","data":expected()})
    );
}

#[tokio::test]
async fn native_gemini_provider_error_projects_one_committed_partial_search() {
    let mut prefix = packet();
    prefix["candidates"][0]
        .as_object_mut()
        .unwrap()
        .remove("finishReason");
    let (url, server) = server(
        vec![
            prefix,
            json!({"error":{"status":"UNAVAILABLE","message":"fixture provider failure"}}),
        ],
        false,
    )
    .await;
    let mut worker = start(url).await;
    let (search, wire, errors, failed_end) = timeout(WAIT, async {
        let mut search = vec![];
        let mut wire = vec![];
        let mut errors = vec![];
        let mut failed_end = false;
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::LlmSearchResponse(response) => {
                    search.push(serde_json::to_value(response).unwrap())
                }
                FrameData::OutputTransportMessageUrgent(message)
                    if message["type"] == "bot-llm-search-response" =>
                {
                    wire.push(message)
                }
                FrameData::Error(error) => errors.push(error),
                FrameData::LlmResponseEnd => {
                    failed_end = frame.frame.metadata.get("failed") == Some(&json!(true));
                    break;
                }
                _ => {}
            }
        }
        (search, wire, errors, failed_end)
    })
    .await
    .unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, server).await.unwrap().unwrap();
    assert_eq!(request["tools"], json!([{"googleSearch":{}}]));
    assert!(failed_end);
    assert_eq!(errors.len(), 1);
    assert!(errors[0]
        .error
        .contains("provider error (UNAVAILABLE): fixture provider failure"));
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert_eq!(search, vec![expected()]);
    assert_eq!(
        wire,
        vec![json!({"label":"rtvi-ai","type":"bot-llm-search-response","data":expected()})]
    );
}

#[tokio::test]
async fn cancelling_native_gemini_pipeline_closes_socket_without_late_search_projection() {
    let mut prefix = packet();
    prefix["candidates"][0]
        .as_object_mut()
        .unwrap()
        .remove("finishReason");
    let (url, server) = server(vec![prefix], true).await;
    let mut worker = start(url).await;
    let observed = timeout(WAIT, async {
        let mut observed = vec![];
        loop {
            let frame = worker.recv().await.expect("native Gemini prefix must reach the tap");
            let visible = matches!(&frame.frame.data, FrameData::LlmText(text) if text.text == "Grounded Rust answer");
            observed.push(frame);
            if visible { break observed; }
        }
    }).await.unwrap();
    timeout(WAIT, worker.cancel("cancel held Gemini grounding"))
        .await
        .unwrap()
        .unwrap();
    let (remaining, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, server).await.unwrap().unwrap();
    assert_eq!(request["tools"], json!([{"googleSearch":{}}]));
    assert_eq!(report.state, core::WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    for frame in observed.iter().chain(remaining.iter()) {
        assert!(
            !matches!(&frame.frame.data, FrameData::LlmSearchResponse(_)),
            "cancelled stream must not flush a search frame"
        );
        assert!(
            !matches!(&frame.frame.data, FrameData::OutputTransportMessageUrgent(message) if message["type"] == "bot-llm-search-response"),
            "cancelled source must not emit a late RTVI search response"
        );
    }
}
