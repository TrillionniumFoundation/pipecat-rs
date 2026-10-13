use pipecat_core::{Direction, Frame, FrameData, Pipeline, PipelineConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, GeminiDeclaredRequest, GeminiToolDeclarations,
    LlmClient, LlmProcessor, Message, Provider, Role, StreamEvent,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::Notify,
    task::JoinHandle,
};

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
    closed: Arc<Notify>,
}
impl Server {
    async fn start(packets: Vec<Value>) -> Self {
        Self::start_with_hold(packets, false).await
    }
    async fn start_with_hold(packets: Vec<Value>, hold: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/completion", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let recorded = requests.clone();
        let closed = Arc::new(Notify::new());
        let observed_close = closed.clone();
        let task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let end = loop {
                let mut chunk = [0; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
                if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
                assert!(bytes.len() < 64 * 1024);
            };
            let length: usize = String::from_utf8_lossy(&bytes[..end])
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .map(|s| s.trim().parse().unwrap())
                })
                .unwrap();
            while bytes.len() < end + length {
                let mut chunk = [0; 4096];
                let n = socket.read(&mut chunk).await.unwrap();
                assert!(n > 0);
                bytes.extend_from_slice(&chunk[..n]);
            }
            recorded
                .lock()
                .unwrap()
                .push(serde_json::from_slice(&bytes[end..end + length]).unwrap());
            let body: String = packets
                .into_iter()
                .map(|packet| format!("data: {packet}\n\n"))
                .collect();
            let length_header = if hold {
                String::new()
            } else {
                format!("Content-Length: {}\r\n", body.len())
            };
            let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\n{length_header}Connection: close\r\n\r\n{body}");
            socket.write_all(response.as_bytes()).await.unwrap();
            if hold {
                let mut byte = [0; 1];
                match socket.read(&mut byte).await {
                    Ok(0) | Err(_) => {}
                    Ok(_) => panic!("unexpected data after response"),
                }
            } else {
                socket.shutdown().await.unwrap();
            }
            observed_close.notify_one();
        });
        Self {
            url,
            requests,
            task,
            closed,
        }
    }
    fn client(&self) -> LlmClient {
        let mut config = ClientConfig::new(Provider::Gemini, "fixture");
        config.endpoint = self.url.clone();
        config.api_key = None;
        config.channel_capacity = 1;
        LlmClient::new(config).unwrap()
    }
    fn assert_request(&self) {
        let requests = self.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0]["tools"], json!([{"googleSearch":{}}]));
    }
}

async fn collect(server: &Server) -> (Vec<StreamEvent>, Vec<pipecat_services::ServiceError>) {
    let mut stream = server
        .client()
        .stream_gemini_declared(declared_request())
        .unwrap();
    let mut events = vec![];
    let mut errors = vec![];
    while let Some(event) = stream.next().await {
        match event {
            Ok(event) => events.push(event),
            Err(error) => errors.push(error),
        }
    }
    server.assert_request();
    (events, errors)
}
fn search_values(events: &[StreamEvent]) -> Vec<Value> {
    events
        .iter()
        .filter_map(|event| match event {
            StreamEvent::SearchResponse(response) => Some(serde_json::to_value(response).unwrap()),
            _ => None,
        })
        .collect()
}
fn old_expected(text: &str) -> Value {
    json!({"search_result":text,"rendered_content":null,"origins":[{"site_uri":"https://old.test","site_title":"superseded","results":[]}]})
}

#[tokio::test]
async fn native_grounding_preserves_committed_partial_response_on_provider_error() {
    let server = Server::start(vec![
        packets()[0].clone(),
        json!({"error":{"status":"UNAVAILABLE","message":"fixture failure"}}),
    ])
    .await;
    let (events, errors) = collect(&server).await;
    assert!(matches!(
        errors.as_slice(),
        [pipecat_services::ServiceError::Provider { .. }]
    ));
    assert_eq!(search_values(&events), vec![old_expected("Rust ")]);
    assert!(matches!(events.last(), Some(StreamEvent::Finished { .. })));
}

#[tokio::test]
async fn native_grounding_rejects_invalid_or_expanding_record_without_publishing_its_text() {
    let chunks: Vec<_> = (0..33)
        .map(|i| json!({"web":{"uri":format!("https://fanout.test/{i}")}}))
        .collect();
    let supports: Vec<_> = (0..33).map(|_| json!({"groundingChunkIndices":(0..33).collect::<Vec<_>>(),"segment":{"text":"expanded"},"confidenceScores":[0.5]})).collect();
    for invalid in [
        json!({"candidates":[{"content":{"parts":[{"text":"undelivered"},{"text":17}]},"groundingMetadata":grounding()}]}),
        json!({"candidates":[{"content":{"parts":[{"text":"undelivered"}]},"groundingMetadata":{"groundingChunks":chunks,"groundingSupports":supports}}]}),
    ] {
        let server = Server::start(vec![packets()[0].clone(), invalid]).await;
        let (events, errors) = collect(&server).await;
        assert_eq!(errors.len(), 1);
        assert!(matches!(
            &errors[0],
            pipecat_services::ServiceError::Protocol(_)
        ));
        assert_eq!(search_values(&events), vec![old_expected("Rust ")]);
        let text: String = events
            .iter()
            .filter_map(|event| match event {
                StreamEvent::TextDelta(text) => Some(text.as_str()),
                _ => None,
            })
            .collect();
        assert_eq!(text, "Rust ");
    }
}

#[tokio::test]
async fn native_grounding_without_nonempty_chunks_generates_no_search_response() {
    let server = Server::start(vec![json!({"candidates":[{"content":{"parts":[{"text":"plain"}]},"groundingMetadata":{"groundingChunks":[]},"finishReason":"STOP"}]} )]).await;
    let (events, errors) = collect(&server).await;
    assert!(errors.is_empty());
    assert!(search_values(&events).is_empty());
}

#[tokio::test]
async fn native_grounding_cancellation_keeps_only_delivered_text_and_committed_record() {
    let parts: Vec<_> = (0..100)
        .map(|i| json!({"text":format!("middle-{i}")}))
        .collect();
    let server = Server::start_with_hold(
        vec![
            packets()[0].clone(),
            json!({"candidates":[{"content":{"parts":parts},"groundingMetadata":grounding()}]}),
        ],
        true,
    )
    .await;
    let mut stream = server
        .client()
        .stream_gemini_declared(declared_request())
        .unwrap();
    let mut text = String::new();
    let mut events = vec![];
    let mut cancelled = false;
    let mut saw_cancelled = false;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(event) = stream.next().await {
            match event {
                Ok(event) => {
                    if let StreamEvent::TextDelta(delta) = &event {
                        text.push_str(delta);
                        if delta.starts_with("middle-") && !cancelled {
                            cancelled = true;
                            stream.cancel();
                        }
                    }
                    events.push(event);
                }
                Err(error) => {
                    assert!(matches!(error, pipecat_services::ServiceError::Cancelled));
                    saw_cancelled = true;
                }
            }
        }
    })
    .await
    .unwrap();
    assert!(cancelled && saw_cancelled);
    assert!(text.starts_with("Rust middle-0"));
    assert!(!text.contains("middle-99"));
    assert_eq!(search_values(&events), vec![old_expected(&text)]);
    server.assert_request();
    tokio::time::timeout(Duration::from_secs(3), server.closed.notified())
        .await
        .unwrap();
}

#[tokio::test]
async fn native_grounding_consumer_drop_closes_the_real_socket() {
    let server = Server::start_with_hold(vec![packets()[0].clone()], true).await;
    let mut stream = server
        .client()
        .stream_gemini_declared(declared_request())
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if matches!(
                stream.next().await.unwrap().unwrap(),
                StreamEvent::TextDelta(_)
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(3), server.closed.notified())
        .await
        .unwrap();
    server.assert_request();
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn grounding() -> Value {
    json!({
        "groundingChunks":[{"web":{"uri":"https://example.test/rust","title":"Rust docs"}},{"retrievedContext":{"uri":"gs://source","title":"Retrieved","text":"context"}}],
        "groundingSupports":[
            {"groundingChunkIndices":[0,1,0,-1,99],"segment":{"text":"Rust answer"},"confidenceScores":[0.8,0.6]},
            {"groundingChunkIndices":[0],"confidenceScores":null},
            {"groundingChunkIndices":[1],"segment":{"text":null},"confidenceScores":[]},
            {"groundingChunkIndices":[1,99],"confidenceScores":[0.3]}
        ],
        "searchEntryPoint":{"renderedContent":"<div>Source HTML stays literal</div>"}
    })
}
fn packets() -> Vec<Value> {
    vec![
        json!({"candidates":[{"content":{"parts":[{"text":"Rust "}]},"groundingMetadata":{"groundingChunks":[{"web":{"uri":"https://old.test","title":"superseded"}}]}}]}),
        json!({"candidates":[{"content":{"parts":[{"text":"private thought","thought":true},{"text":"answer"}]},"groundingMetadata":grounding()}]}),
        json!({"candidates":[{"groundingMetadata":{"groundingChunks":[]}}]}),
        json!({"candidates":[{"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}}),
    ]
}
fn expected() -> Value {
    json!({
        "search_result":"Rust answer","rendered_content":"<div>Source HTML stays literal</div>",
        "origins":[
            {"site_uri":"https://example.test/rust","site_title":"Rust docs","results":[{"text":"Rust answer","confidence":[0.8,0.6]},{"text":"","confidence":null}]},
            {"site_uri":null,"site_title":null,"results":[{"text":"Rust answer","confidence":[0.8,0.6]},{"text":null,"confidence":[]},{"text":"","confidence":[0.3]}]}
        ]
    })
}
fn declared_request() -> GeminiDeclaredRequest {
    let mut context = Context::default();
    context
        .messages
        .push(Message::text(Role::User, "Search Rust"));
    GeminiDeclaredRequest::new(
        CompletionRequest::new("fixture", context),
        GeminiToolDeclarations::new(vec![BTreeMap::from([("google_search".into(), json!({}))])])
            .unwrap(),
    )
}

#[tokio::test]
async fn native_gemini_stream_preserves_real_search_response() {
    let server = Server::start(packets()).await;
    let mut stream = server
        .client()
        .stream_gemini_declared(declared_request())
        .unwrap();
    let mut text = String::new();
    let mut search = vec![];
    let mut order = vec![];
    while let Some(event) = stream.next().await {
        let event = event.unwrap();
        if let StreamEvent::TextDelta(delta) = &event {
            text.push_str(delta);
        }
        let value = serde_json::to_value(event).unwrap();
        if let Some(response) = value.get("SearchResponse") {
            search.push(response.clone());
            order.push("search");
        }
        if value.get("Usage").is_some() {
            order.push("usage");
        }
        if value.get("Finished").is_some() {
            order.push("finished");
        }
    }
    server.assert_request();
    assert_eq!(text, "Rust answer");
    assert_eq!(
        search.len(),
        1,
        "real native Gemini HTTP must retain grounding as a search response"
    );
    assert_eq!(search[0], expected());
    assert_eq!(order, ["search", "usage", "finished"]);
}

#[tokio::test]
async fn native_gemini_pipeline_emits_real_search_frame_before_response_end() {
    let server = Server::start(packets()).await;
    let processor = Arc::new(LlmProcessor::new(
        server.client(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let input = pipecat_core::LlmContextData {
        messages: vec![
            pipecat_core::ChatMessage::new(pipecat_core::ChatRole::User, "Search Rust").into(),
        ],
        tools: pipecat_core::ToolSet::Schema(pipecat_core::ToolsSchema {
            standard_tools: vec![],
            custom_tools: Some(BTreeMap::from([(
                pipecat_core::AdapterType::Gemini,
                vec![BTreeMap::from([("google_search".into(), json!({}))])],
            )])),
        }),
        tool_choice: None,
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContextDetailed(
                pipecat_core::DetailedLlmContextFrame {
                    context: input,
                    speculation: false,
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (search, text, order) = tokio::time::timeout(Duration::from_secs(3), async {
        let mut search = vec![];
        let mut text = String::new();
        let mut order = vec![];
        while let Some(frame) = worker.recv().await {
            match &frame.frame.data {
                FrameData::LlmText(delta) => text.push_str(&delta.text),
                FrameData::Error(error) => panic!("unexpected native error: {error:?}"),
                FrameData::LlmResponseEnd => {
                    order.push("end");
                    break;
                }
                _ => {}
            }
            if frame.frame.data.source_class_name() == "LLMSearchResponseFrame" {
                assert_eq!(frame.direction, Direction::Downstream);
                assert_eq!(frame.frame.priority(), pipecat_core::FramePriority::Normal);
                let value = serde_json::to_value(&frame.frame.data).unwrap();
                assert_eq!(value["type"], "llm_search_response");
                search.push(value["payload"].clone());
                order.push("search");
            }
        }
        (search, text, order)
    })
    .await
    .unwrap();
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    server.assert_request();
    assert_eq!(text, "Rust answer");
    assert_eq!(
        search.len(),
        1,
        "native pipeline must emit the public Google search frame from real grounding"
    );
    assert_eq!(search[0], expected());
    assert_eq!(order, ["search", "end"]);
}
