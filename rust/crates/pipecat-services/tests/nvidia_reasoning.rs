//! Pinned NVIDIA reasoning behavior over actual localhost HTTP/SSE, no SDK runtime.
use pipecat_core::{Direction, Frame, FrameData, Metric, Pipeline, PipelineConfig};
use pipecat_services::presets::{OpenAiPreset, OpenAiPresetConfig};
use pipecat_services::*;
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};

async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(6), f)
        .await
        .expect("localhost fixture stalled")
}
async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = Vec::new();
    let mut part = [0; 4096];
    let end = loop {
        let n = socket.read(&mut part).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&part[..n]);
        assert!(bytes.len() < 1024 * 1024);
        if let Some(i) = bytes.windows(4).position(|p| p == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = std::str::from_utf8(&bytes[..end]).unwrap().to_owned();
    let length: usize = headers
        .lines()
        .find_map(|s| {
            s.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut part).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&part[..n]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
fn record(delta: Value) -> Value {
    json!({"choices":[{"index":0,"delta":delta}]})
}
fn wire(records: Vec<Value>, done: bool) -> Vec<u8> {
    let mut body: String = records
        .into_iter()
        .map(|r| format!("data: {r}\r\n\r\n"))
        .collect();
    if done {
        body.push_str("data: [DONE]\r\n\r\n");
    }
    body.into_bytes()
}
fn configuration(endpoint: String) -> ClientConfig {
    let mut config = ClientConfig::openai_compatible(
        OpenAiPresetConfig::new(OpenAiPreset::Nvidia),
        "synthetic-nvidia",
    )
    .unwrap();
    config.endpoint = endpoint;
    config.request_timeout = Duration::from_secs(3);
    config.idle_timeout = Duration::from_secs(2);
    config
}
fn request() -> CompletionRequest {
    OpenAiPresetConfig::new(OpenAiPreset::Nvidia)
        .request(Context::new(vec![Message::text(Role::User, "question")]))
}
async fn peer(body: Vec<u8>, fragment: usize) -> (ClientConfig, JoinHandle<(String, Value)>) {
    peer_with_rejection(body, fragment, false).await
}
async fn peer_with_rejection(
    body: Vec<u8>,
    fragment: usize,
    allow_rejection: bool,
) -> (ClientConfig, JoinHandle<(String, Value)>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(format!(
        "http://{}/v1/chat/completions",
        listener.local_addr().unwrap()
    ));
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await.unwrap();
        for part in body.chunks(fragment) {
            if let Err(error) = socket.write_all(part).await {
                assert!(
                    allow_rejection
                        && matches!(
                            error.kind(),
                            std::io::ErrorKind::BrokenPipe | std::io::ErrorKind::ConnectionReset
                        ),
                    "{error}"
                );
                break;
            }
            tokio::task::yield_now().await;
        }
        request
    });
    (config, task)
}
async fn collect(mut stream: LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    bounded(async move {
        let mut result = Vec::new();
        while let Some(event) = stream.next().await {
            result.push(event);
        }
        result
    })
    .await
}
fn trace(events: &[Result<StreamEvent, ServiceError>]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Ok(StreamEvent::ThoughtStart) => Some("start".into()),
            Ok(StreamEvent::ThoughtEnd) => Some("end".into()),
            Ok(StreamEvent::ThoughtDelta(s)) => Some(format!("thought:{s}")),
            Ok(StreamEvent::TextDelta(s)) => Some(format!("text:{s}")),
            _ => None,
        })
        .collect()
}
async fn run(records: Vec<Value>, done: bool) -> Vec<Result<StreamEvent, ServiceError>> {
    let events = run_inner(records, done, false).await;
    if done {
        assert!(events.iter().all(Result::is_ok), "{events:?}");
        assert!(matches!(
            events.last(),
            Some(Ok(StreamEvent::Finished { .. }))
        ));
    }
    events
}
async fn run_rejected(records: Vec<Value>, done: bool) -> Vec<Result<StreamEvent, ServiceError>> {
    let events = run_inner(records, done, true).await;
    assert!(
        events.iter().any(Result::is_err),
        "rejection fixture unexpectedly succeeded"
    );
    events
}
async fn run_inner(
    records: Vec<Value>,
    done: bool,
    allow_rejection: bool,
) -> Vec<Result<StreamEvent, ServiceError>> {
    let (config, peer) = peer_with_rejection(wire(records, done), 1, allow_rejection).await;
    let events = collect(LlmClient::new(config).unwrap().stream(request()).unwrap()).await;
    let (headers, body) = bounded(peer).await.unwrap();
    assert!(headers
        .to_ascii_lowercase()
        .contains("authorization: bearer synthetic-nvidia"));
    assert_eq!(body["model"], "nvidia/nemotron-3-super-120b-a12b");
    assert_eq!(body["stream"], true);
    assert_eq!(body["stream_options"]["include_usage"], true);
    events
}
#[tokio::test]
async fn nvidia_fragments_unicode_tags_and_preserves_tools_usage_and_finish() {
    let events = run(vec![
        record(json!({"content":"<th"})), record(json!({"content":"ink>秘🙂"})),
        record(json!({"content":"</thi"})), record(json!({"content":"nk>answer"})),
        record(json!({"tool_calls":[{"index":0,"id":"call","type":"function","function":{"name":"lookup","arguments":"{"}}]})),
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"function":{"arguments":"}"}}]},"finish_reason":"tool_calls"}]}),
        json!({"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":3,"total_tokens":5,"completion_tokens_details":{"reasoning_tokens":1}}}),
    ], true).await;
    assert!(events.iter().all(Result::is_ok));
    assert_eq!(
        trace(&events),
        ["start", "thought:秘🙂", "end", "text:answer"]
    );
    assert!(events.iter().any(|e| matches!(e,Ok(StreamEvent::ToolCall(c)) if c.id=="call"&&c.name=="lookup"&&c.arguments==json!({}))));
    assert!(events.iter().any(
        |e| matches!(e,Ok(StreamEvent::Usage(u)) if u.total_tokens==5&&u.reasoning_tokens==Some(1))
    ));
    assert!(matches!(
        events.last(),
        Some(Ok(StreamEvent::Finished {
            reason: Some(FinishReason::ToolCalls)
        }))
    ));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(StreamEvent::ResponseProgress)))
            .count(),
        1
    );
}
#[tokio::test]
async fn nvidia_structured_truthy_fallback_and_content_order_are_source_exact() {
    let events = run(
        vec![
            record(json!({"reasoning_content":null,"reasoning":"A","content":"visible"})),
            record(json!({"reasoning_content":"","reasoning":"B"})),
            record(json!({"reasoning_content":"C","reasoning":"unused","content":"+"})),
            record(json!({"content":"last"})),
            record(json!({"reasoning":"D"})),
        ],
        true,
    )
    .await;
    assert_eq!(
        trace(&events),
        [
            "start",
            "thought:A",
            "text:visible",
            "thought:B",
            "thought:C",
            "text:+",
            "end",
            "text:last",
            "start",
            "thought:D",
            "end"
        ]
    );
}
#[tokio::test]
async fn nvidia_mixed_reasoning_channels_keep_independent_explicit_boundaries() {
    let events = run(
        vec![
            record(json!({"reasoning_content":"field","content":"<think></think>visible"})),
            record(json!({"reasoning":"more"})),
            record(json!({"content":"tail"})),
        ],
        true,
    )
    .await;
    assert_eq!(
        trace(&events),
        [
            "start",
            "thought:field",
            "start",
            "end",
            "text:visible",
            "thought:more",
            "end",
            "text:tail"
        ]
    );
}
#[tokio::test]
async fn nvidia_normal_completion_flushes_only_the_selected_text_channel() {
    for (input, expected) in [
        ("<thi", vec!["text:<thi"]),
        ("<think>甲乙🙂ab", vec!["start", "thought:甲乙🙂ab", "end"]),
        ("<think></think>", vec!["start", "end"]),
        (" <think>x</think>", vec!["text: <think>x</think>"]),
        ("<Think>x</Think>", vec!["text:<Think>x</Think>"]),
        (
            "answer<think>x</think>",
            vec!["text:answer<think>x</think>"],
        ),
    ] {
        let events = run(vec![record(json!({"content":input}))], true).await;
        assert!(events.iter().all(Result::is_ok));
        assert_eq!(trace(&events), expected, "{input}");
    }
}
#[tokio::test]
async fn nvidia_missing_done_discards_unicode_tail_and_retains_usage() {
    for (input, expected) in [("<thi", vec![]), ("<think>甲乙🙂ab", vec!["start", "end"])] {
        let events = run(
            vec![
                record(json!({"content":input})),
                json!({"choices":[],"usage":{"prompt_tokens":2,"completion_tokens":1}}),
            ],
            false,
        )
        .await;
        assert_eq!(trace(&events), expected);
        assert_eq!(events.iter().filter(|e| e.is_err()).count(), 1);
        assert!(events
            .iter()
            .any(|e| matches!(e,Ok(StreamEvent::Usage(u)) if u.total_tokens==3)));
        assert!(matches!(
            events.last(),
            Some(Ok(StreamEvent::Finished { .. }))
        ));
    }
}
#[tokio::test]
async fn generic_openai_retains_literal_markup_and_content_before_reasoning() {
    let (mut config, task) = peer(
        wire(
            vec![record(
                json!({"content":"<think>x</think>","reasoning_content":"reason"}),
            )],
            true,
        ),
        1,
    )
    .await;
    config.openai_preset = None;
    let events = collect(LlmClient::new(config).unwrap().stream(request()).unwrap()).await;
    assert_eq!(trace(&events), ["text:<think>x</think>", "thought:reason"]);
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(StreamEvent::ResponseProgress)))
            .count(),
        1
    );
    bounded(task).await.unwrap();
}

async fn stalled(mut prefix: Value) -> (ClientConfig, oneshot::Receiver<()>, JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(format!("http://{}/stream", listener.local_addr().unwrap()));
    let (sent, received) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket
            .write_all(
                b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nConnection: close\r\n\r\n",
            )
            .await
            .unwrap();
        prefix["usage"] = json!({"prompt_tokens":7});
        socket.write_all(&wire(vec![prefix], false)).await.unwrap();
        sent.send(()).unwrap();
        let mut byte = [0];
        assert_eq!(
            bounded(socket.read(&mut byte)).await.unwrap(),
            0,
            "client must eagerly close"
        );
    });
    (config, received, task)
}
#[tokio::test]
async fn nvidia_cancel_and_drop_close_real_socket_without_flushing_hidden_tail() {
    for drop_stream in [false, true] {
        let (config, sent, peer) = stalled(record(json!({"content":"<think>甲乙🙂ab"}))).await;
        let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
        let mut events = Vec::new();
        while !events
            .iter()
            .any(|e| matches!(e, Ok(StreamEvent::ThoughtStart)))
        {
            events.push(bounded(stream.next()).await.unwrap());
        }
        sent.await.unwrap();
        if drop_stream {
            drop(stream);
        } else {
            stream.cancel();
            events.extend(collect(stream).await);
            assert_eq!(trace(&events), ["start", "end"]);
            assert!(events
                .iter()
                .any(|e| matches!(e, Ok(StreamEvent::Usage(u)) if u.prompt_tokens == 7)));
            assert!(events
                .iter()
                .any(|e| matches!(e, Err(ServiceError::Cancelled))));
        }
        bounded(peer).await.unwrap();
    }
}
#[tokio::test]
async fn nvidia_timeout_and_saturated_channel_finish_without_flushing_prefix() {
    let (mut config, sent, peer) = stalled(record(json!({"content":"<thi"}))).await;
    config.channel_capacity = 1;
    config.idle_timeout = Duration::from_millis(80);
    config.request_timeout = Duration::from_millis(160);
    let stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
    sent.await.unwrap();
    tokio::time::sleep(Duration::from_millis(230)).await;
    let events = collect(stream).await;
    assert_eq!(trace(&events), Vec::<String>::new());
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Timeout))));
    bounded(peer).await.unwrap();
}

#[tokio::test]
async fn nvidia_pipeline_explicit_boundaries_suppression_flags_and_ttfb_order() {
    let records = vec![
        record(json!({"role":"assistant"})),
        record(json!({"reasoning":"field","content":"<think></think>visible"})),
        record(json!({"reasoning":"more"})),
        record(json!({"content":"tail"})),
    ];
    let (config, peer) = peer(wire(records, true), 1).await;
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        request(),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig {
            start: pipecat_core::StartFrame {
                enable_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    let trace = bounded(async {
        let mut trace = Vec::<String>::new();
        loop {
            let event = worker.recv().await.unwrap();
            match event.frame.data {
                FrameData::LlmThoughtStart => trace.push("start".into()),
                FrameData::LlmThoughtEnd => trace.push("end".into()),
                FrameData::LlmThoughtText(text) => {
                    assert_eq!(text.skip_tts, Some(true));
                    assert!(!text.append_to_context);
                    trace.push(format!("thought:{}", text.text));
                }
                FrameData::LlmText(text) => {
                    assert!(text.append_to_context);
                    trace.push(format!("text:{}", text.text));
                }
                FrameData::Metrics(metrics)
                    if metrics.iter().any(|m| matches!(m, Metric::Ttfb { .. })) =>
                {
                    trace.push("ttfb".into())
                }
                FrameData::LlmResponseEnd => break trace,
                FrameData::Error(e) => panic!("{}", e.error),
                _ => {}
            }
        }
    })
    .await;
    assert_eq!(
        trace,
        [
            "ttfb",
            "start",
            "thought:field",
            "start",
            "end",
            "text:visible",
            "thought:more",
            "end",
            "text:tail"
        ]
    );
    bounded(peer).await.unwrap();
    worker.end().await.unwrap();
    bounded(worker.join()).await.unwrap();
}
#[tokio::test]
async fn nvidia_pipeline_reports_ttfb_while_prefix_is_still_buffered() {
    let (config, sent, peer) = stalled(record(json!({"content":"<th"}))).await;
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        request(),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig {
            start: pipecat_core::StartFrame {
                enable_metrics: true,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    sent.await.unwrap();
    bounded(async {
        loop {
            match worker.recv().await.unwrap().frame.data {
                FrameData::Metrics(m) if m.iter().any(|m| matches!(m, Metric::Ttfb { .. })) => {
                    break
                }
                FrameData::LlmText(_)
                | FrameData::LlmThoughtText(_)
                | FrameData::LlmThoughtStart => panic!("prefix was prematurely emitted"),
                _ => {}
            }
        }
    })
    .await;
    worker.cancel("test cancellation").await.unwrap();
    bounded(worker.join()).await.unwrap();
    bounded(peer).await.unwrap();
}

#[tokio::test]
async fn nvidia_cancel_during_one_record_delivery_closes_only_observed_boundaries() {
    for consumed in [1, 2, 3] {
        let (mut config, sent, peer) =
            stalled(record(json!({"content":"<think></think>visible"}))).await;
        config.channel_capacity = 1;
        let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
        let mut events = Vec::new();
        for _ in 0..consumed {
            events.push(bounded(stream.next()).await.unwrap());
        }
        sent.await.unwrap();
        // The only free channel slot fills, leaving the rest of the same record blocked.
        tokio::time::sleep(Duration::from_millis(40)).await;
        stream.cancel();
        // Let cancellation win before reopening the event channel.
        tokio::time::sleep(Duration::from_millis(40)).await;
        events.extend(collect(stream).await);
        let bounds = trace(&events);
        if consumed == 1 {
            assert!(bounds.is_empty(), "{bounds:?}");
        } else {
            assert_eq!(bounds, ["start", "end"]);
        }
        assert!(events
            .iter()
            .any(|e| matches!(e, Err(ServiceError::Cancelled))));
        bounded(peer).await.unwrap();
    }
}

#[tokio::test]
async fn nvidia_json_truthiness_falls_back_but_truthy_nontext_fails_explicitly() {
    for primary in [
        Value::Null,
        json!(false),
        json!(0),
        json!(0.0),
        json!(""),
        json!([]),
        json!({}),
    ] {
        let events = run(
            vec![record(
                json!({"reasoning_content":primary,"reasoning":"fallback"}),
            )],
            true,
        )
        .await;
        assert!(events.iter().all(Result::is_ok));
        assert_eq!(trace(&events), ["start", "thought:fallback", "end"]);
    }
    for primary in [
        json!(true),
        json!(1),
        json!(["not text"]),
        json!({"not":"text"}),
    ] {
        let events = run_rejected(
            vec![record(
                json!({"reasoning_content":primary,"reasoning":"do not bypass primary"}),
            )],
            true,
        )
        .await;
        assert!(trace(&events).is_empty());
        assert!(events.iter().any(
            |e| matches!(e,Err(ServiceError::Protocol(s)) if s=="NVIDIA reasoning must be text")
        ));
    }
}

#[tokio::test]
async fn nvidia_malformed_record_does_not_publish_unobserved_boundaries() {
    let events = run_rejected(
        vec![record(
            json!({"content":"<think>secret","function_call":{"name":"old"}}),
        )],
        true,
    )
    .await;
    assert!(trace(&events).is_empty());
    assert!(events.iter().any(Result::is_err));
    let events = run_rejected(
        vec![
            record(json!({"reasoning":"accepted"})),
            record(json!({"content":"<think>not accepted","function_call":{}})),
        ],
        true,
    )
    .await;
    assert_eq!(trace(&events), ["start", "thought:accepted", "end"]);
    assert!(!events
        .iter()
        .any(|e| matches!(e, Ok(StreamEvent::ToolCall(_)))));
}

#[tokio::test]
async fn nvidia_consecutive_responses_reset_reasoning_detection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = configuration(format!("http://{}/stream", listener.local_addr().unwrap()));
    let peer = tokio::spawn(async move {
        for text in [
            "<think>private",
            "<think>other</think>visible",
            "ordinary<think>literal",
        ] {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let body = wire(vec![record(json!({"content":text}))], true);
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
            socket.write_all(&body).await.unwrap();
        }
    });
    let client = LlmClient::new(config).unwrap();
    for expected in [
        vec!["start", "thought:private", "end"],
        vec!["start", "thought:other", "end", "text:visible"],
        vec!["text:ordinary<think>literal"],
    ] {
        let events = collect(client.stream(request()).unwrap()).await;
        assert!(events.iter().all(Result::is_ok));
        assert_eq!(trace(&events), expected);
    }
    bounded(peer).await.unwrap();
}
