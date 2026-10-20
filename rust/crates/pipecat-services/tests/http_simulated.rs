//! Real sockets, synthetic local provider responses. These are not provider-live tests.
use pipecat_services::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

struct FakeServer {
    url: String,
    request: oneshot::Receiver<(String, Value)>,
    task: tokio::task::JoinHandle<()>,
}
async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut chunk = [0; 1024];
    let header_end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..header_end].to_vec()).unwrap();
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < header_end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap(),
    )
}
async fn server(
    status: &str,
    content_type: &str,
    parts: Vec<Vec<u8>>,
    delay: Duration,
    header_delay: Duration,
) -> FakeServer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/stream", listener.local_addr().unwrap());
    let (tx, request) = oneshot::channel();
    let status = status.to_owned();
    let content_type = content_type.to_owned();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let captured = read_request(&mut socket).await;
        let _ = tx.send(captured);
        tokio::time::sleep(header_delay).await;
        if socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nRetry-After: 2\r\nConnection: close\r\n\r\n").as_bytes()).await.is_err() {return;}
        for part in parts {
            if socket
                .write_all(format!("{:x}\r\n", part.len()).as_bytes())
                .await
                .is_err()
            {
                return;
            }
            if socket.write_all(&part).await.is_err() {
                return;
            }
            if socket.write_all(b"\r\n").await.is_err() {
                return;
            }
            tokio::time::sleep(delay).await;
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    });
    FakeServer { url, request, task }
}
fn record(value: Value) -> Vec<u8> {
    format!("data: {value}\n\n").into_bytes()
}
fn config(provider: Provider, url: String) -> ClientConfig {
    let mut c = ClientConfig::new(provider, "synthetic-test-key");
    c.endpoint = url;
    c.request_timeout = Duration::from_secs(3);
    c.idle_timeout = Duration::from_secs(1);
    c
}
fn request() -> CompletionRequest {
    CompletionRequest::new(
        "synthetic-model",
        Context::new(vec![Message::text(Role::User, "synthetic hello")]),
    )
}
async fn collect(stream: &mut LlmStream) -> Vec<Result<StreamEvent, ServiceError>> {
    tokio::time::timeout(Duration::from_secs(5), async {
        let mut out = vec![];
        while let Some(item) = stream.next().await {
            out.push(item);
        }
        out
    })
    .await
    .unwrap()
}
#[tokio::test]
async fn simulated_openai_http_streams_text_before_response_ends_and_sends_auth_body() {
    let fake=server("200 OK","text/event-stream",vec![record(json!({"choices":[{"index":0,"delta":{"content":"你好🙂"}}]})),record(json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":3,"completion_tokens":2,"total_tokens":5}})),b"data: [DONE]\n\n".to_vec()],Duration::from_millis(80),Duration::ZERO).await;
    let client = LlmClient::new(config(Provider::OpenAi, fake.url)).unwrap();
    let mut stream = client.stream(request()).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), StreamEvent::Started);
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        StreamEvent::ResponseProgress
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        StreamEvent::TextDelta("你好🙂".into())
    );
    assert!(
        !fake.task.is_finished(),
        "must deliver text while HTTP response remains open"
    );
    let events = collect(&mut stream).await;
    assert_eq!(events.len(), 2);
    assert!(matches!(events[0], Ok(StreamEvent::Usage(_))));
    assert_eq!(
        events[1].as_ref().unwrap(),
        &StreamEvent::Finished {
            reason: Some(FinishReason::Stop)
        }
    );
    let (headers, body) = fake.request.await.unwrap();
    assert!(headers
        .to_lowercase()
        .contains("authorization: bearer synthetic-test-key"));
    assert_eq!(body["stream"], true);
    assert_eq!(body["messages"][0]["content"], "synthetic hello");
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_openai_byte_chunks_and_multiple_tools() {
    let wire=[record(json!({"choices":[{"delta":{"content":"你好"}}]})),record(json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"a","function":{"name":"f","arguments":"{\"x\":"}},{"index":1,"id":"b","function":{"name":"g","arguments":"{}"}}]}}]})),record(json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"1}"}}]},"finish_reason":"tool_calls"}]})),b"data: [DONE]\n\n".to_vec()].concat();
    let parts = wire.into_iter().map(|b| vec![b]).collect();
    let fake = server(
        "200 OK",
        "text/event-stream",
        parts,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut stream = LlmClient::new(config(Provider::OpenAi, fake.url))
        .unwrap()
        .stream(request())
        .unwrap();
    let events = collect(&mut stream).await;
    assert!(events.iter().all(Result::is_ok));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(StreamEvent::ToolCall(_))))
            .count(),
        2
    );
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_anthropic_actual_messages_wire() {
    let parts = vec![
        record(
            json!({"type":"message_start","message":{"usage":{"input_tokens":4,"output_tokens":0}}}),
        ),
        record(
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        ),
        record(
            json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Hello"}}),
        ),
        record(json!({"type":"content_block_stop","index":0})),
        record(
            json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":2}}),
        ),
        record(json!({"type":"message_stop"})),
    ];
    let fake = server(
        "200 OK",
        "text/event-stream; charset=utf-8",
        parts,
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut stream = LlmClient::new(config(Provider::Anthropic, fake.url))
        .unwrap()
        .stream(request())
        .unwrap();
    let events = collect(&mut stream).await;
    assert!(events.iter().all(Result::is_ok));
    assert!(events
        .iter()
        .any(|e| matches!(e,Ok(StreamEvent::TextDelta(t)) if t=="Hello")));
    let (headers, body) = fake.request.await.unwrap();
    assert!(headers.contains("x-api-key: synthetic-test-key"));
    assert!(headers.contains("anthropic-version: 2023-06-01"));
    assert_eq!(body["max_tokens"], 4096);
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_gemini_actual_generate_content_wire_and_usage() {
    let fake=server("200 OK","text/event-stream",vec![record(json!({"candidates":[{"index":0,"content":{"parts":[{"text":"Hello"}]} }]})),record(json!({"candidates":[{"index":0,"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":4,"candidatesTokenCount":2,"thoughtsTokenCount":3,"totalTokenCount":9}}))],Duration::ZERO,Duration::ZERO).await;
    let mut stream = LlmClient::new(config(Provider::Gemini, fake.url))
        .unwrap()
        .stream(request())
        .unwrap();
    let events = collect(&mut stream).await;
    assert!(events.iter().all(Result::is_ok));
    assert!(events.iter().any(
        |e| matches!(e,Ok(StreamEvent::Usage(u)) if u.completion_tokens==5 && u.total_tokens==9)
    ));
    let (headers, body) = fake.request.await.unwrap();
    assert!(headers.contains("x-goog-api-key: synthetic-test-key"));
    assert_eq!(body["contents"][0]["parts"][0]["text"], "synthetic hello");
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_gemini_empty_context_settings_seed_reaches_http_wire() {
    let fake = server(
        "200 OK",
        "text/event-stream",
        vec![record(
            json!({"candidates":[{"content":{"parts":[{"text":"Hello"}]},"finishReason":"STOP"}]}),
        )],
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut request = CompletionRequest::new("gemini-3.8-flash", Context::new(vec![]));
    request.settings.system_instruction = Setting::Value("Begin the conversation".into());
    let before = serde_json::to_value(&request).unwrap();
    let mut stream = LlmClient::new(config(Provider::Gemini, fake.url))
        .unwrap()
        .stream(request.clone())
        .unwrap();
    let events = collect(&mut stream).await;
    assert!(events.iter().all(Result::is_ok));
    assert!(events
        .iter()
        .any(|event| matches!(event, Ok(StreamEvent::TextDelta(text)) if text == "Hello")));
    let (_, body) = fake.request.await.unwrap();
    assert_eq!(
        body["contents"],
        json!([{"role":"user","parts":[{"text":"Begin the conversation"}]}])
    );
    assert_eq!(
        body["systemInstruction"],
        json!({"parts":[{"text":"Begin the conversation"}]})
    );
    assert_eq!(serde_json::to_value(&request).unwrap(), before);
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_http_error_keeps_status_and_retry_after_and_brackets_response() {
    let fake = server(
        "429 Too Many Requests",
        "application/json",
        vec![br#"{"error":{"message":"quota synthetic"}}"#.to_vec()],
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut stream = LlmClient::new(config(Provider::OpenAi, fake.url))
        .unwrap()
        .stream(request())
        .unwrap();
    let events = collect(&mut stream).await;
    assert_eq!(events.len(), 3);
    assert!(
        matches!(&events[1],Err(ServiceError::Http{status:429,message,retry_after}) if message=="quota synthetic" && retry_after.as_deref()==Some("2"))
    );
    assert!(events[1].as_ref().unwrap_err().retryable());
    assert!(matches!(events[2], Ok(StreamEvent::Finished { .. })));
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_negative_content_type_invalid_json_truncation_and_provider_error() {
    for (content_type, body) in [
        ("application/json", b"{}".to_vec()),
        ("text/event-stream", b"data: {bad}\n\n".to_vec()),
        (
            "text/event-stream",
            record(json!({"choices":[{"delta":{"content":"partial"}}]})),
        ),
        ("text/event-stream", b"data: [DONE]".to_vec()),
        (
            "text/event-stream",
            record(json!({"error":{"type":"rate_limit","message":"wait"}})),
        ),
    ] {
        let fake = server(
            "200 OK",
            content_type,
            vec![body],
            Duration::ZERO,
            Duration::ZERO,
        )
        .await;
        let mut stream = LlmClient::new(config(Provider::OpenAi, fake.url))
            .unwrap()
            .stream(request())
            .unwrap();
        let events = collect(&mut stream).await;
        assert_eq!(events.iter().filter(|e| e.is_err()).count(), 1);
        assert!(matches!(
            events.last(),
            Some(Ok(StreamEvent::Finished { .. }))
        ));
        fake.task.await.unwrap();
    }
}
#[tokio::test]
async fn simulated_idle_timeout_preserves_latest_usage_exactly_once() {
    let fake=server("200 OK","text/event-stream",vec![record(json!({"choices":[],"usage":{"prompt_tokens":20,"completion_tokens":5,"total_tokens":25}}))],Duration::from_millis(200),Duration::ZERO).await;
    let mut c = config(Provider::OpenAi, fake.url);
    c.idle_timeout = Duration::from_millis(30);
    let mut stream = LlmClient::new(c).unwrap().stream(request()).unwrap();
    let events = collect(&mut stream).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Timeout))));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(StreamEvent::Usage(_))))
            .count(),
        1
    );
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_overall_timeout_covers_response_headers() {
    let fake = server(
        "200 OK",
        "text/event-stream",
        vec![],
        Duration::ZERO,
        Duration::from_millis(200),
    )
    .await;
    let mut c = config(Provider::OpenAi, fake.url);
    c.request_timeout = Duration::from_millis(30);
    let mut stream = LlmClient::new(c).unwrap().stream(request()).unwrap();
    let events = collect(&mut stream).await;
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Timeout))));
    fake.task.await.unwrap();
}
#[tokio::test]
async fn simulated_cancellation_preserves_usage_and_ends_without_waiting_for_network() {
    let fake=server("200 OK","text/event-stream",vec![record(json!({"choices":[{"delta":{"content":"first"}}],"usage":{"prompt_tokens":20,"completion_tokens":5,"total_tokens":25}}))],Duration::from_millis(200),Duration::ZERO).await;
    let mut stream = LlmClient::new(config(Provider::OpenAi, fake.url))
        .unwrap()
        .stream(request())
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), StreamEvent::Started);
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        StreamEvent::ResponseProgress
    );
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        StreamEvent::TextDelta(_)
    ));
    stream.cancel();
    let events = tokio::time::timeout(Duration::from_millis(100), collect(&mut stream))
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, Ok(StreamEvent::Usage(_))))
            .count(),
        1
    );
    assert!(events
        .iter()
        .any(|e| matches!(e, Err(ServiceError::Cancelled))));
    assert!(matches!(
        events.last(),
        Some(Ok(StreamEvent::Finished { .. }))
    ));
    fake.task.await.unwrap();
}
#[tokio::test]
async fn pre_cancelled_request_does_not_connect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/stream", listener.local_addr().unwrap());
    let token = CancellationToken::new();
    token.cancel();
    let mut stream = LlmClient::new(config(Provider::OpenAi, url))
        .unwrap()
        .stream_with_cancel(request(), token)
        .unwrap();
    let events = collect(&mut stream).await;
    assert!(matches!(events[1], Err(ServiceError::Cancelled)));
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn dropping_stream_closes_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/stream", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let body = record(json!({"choices":[{"delta":{"content":"hi"}}]}));
        socket
            .write_all(format!("{:x}\r\n", body.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        let mut buf = [0; 1];
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut buf))
            .await
            .unwrap()
            .unwrap()
    });
    let mut stream = LlmClient::new(config(Provider::OpenAi, url))
        .unwrap()
        .stream(request())
        .unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap(), StreamEvent::Started);
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        StreamEvent::ResponseProgress
    );
    assert!(matches!(
        stream.next().await.unwrap().unwrap(),
        StreamEvent::TextDelta(_)
    ));
    drop(stream);
    assert_eq!(task.await.unwrap(), 0);
}
#[tokio::test]
async fn secrets_redacted_and_bad_endpoint_rejected_before_network() {
    let mut c = config(
        Provider::OpenAi,
        "http://not-loopback.invalid/stream".into(),
    );
    assert!(!format!("{c:?}").contains("synthetic-test-key"));
    assert!(LlmClient::new(c.clone())
        .unwrap()
        .stream(request())
        .is_err());
    c.endpoint = "https://user:password@example.invalid/".into();
    assert!(LlmClient::new(c).unwrap().stream(request()).is_err());
}

#[tokio::test]
async fn simulated_pipeline_maps_context_text_usage_and_response_frames() {
    use pipecat_core::{
        ChatMessage, ChatRole, Direction, Frame, FrameData, LlmContextFrame, Metric, Pipeline,
        PipelineConfig, WorkerState,
    };
    let fake=server("200 OK","text/event-stream",vec![record(json!({"choices":[{"delta":{"content":"pipeline answer"}}]})),record(json!({"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":10,"completion_tokens_details":{"reasoning_tokens":1}}})),b"data: [DONE]\n\n".to_vec()],Duration::ZERO,Duration::ZERO).await;
    let processor = LlmProcessor::new(
        LlmClient::new(config(Provider::OpenAi, fake.url)).unwrap(),
        request(),
    );
    let mut config = PipelineConfig::default();
    config.start.enable_metrics = true;
    config.start.enable_usage_metrics = true;
    let mut worker = Pipeline::new(vec![std::sync::Arc::new(processor)])
        .start(config)
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(LlmContextFrame {
                messages: vec![ChatMessage::new(ChatRole::User, "frame context")],
                ..Default::default()
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let frames = tokio::time::timeout(Duration::from_secs(3), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        frames
    })
    .await
    .unwrap();
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data,FrameData::LlmText(t) if t.text=="pipeline answer")));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart))
            .count(),
        1
    );
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
            .count(),
        1
    );
    assert!(frames.iter().any(|f|matches!(&f.frame.data,FrameData::Metrics(metrics) if metrics.iter().any(|m|matches!(m,Metric::LlmUsage{total_tokens:10,..}))) && f.frame.metadata["llm_usage"]["reasoning_tokens"]==1));
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    let (_, body) = fake.request.await.unwrap();
    assert_eq!(body["messages"][0]["content"], "frame context");
    fake.task.await.unwrap();
}

#[tokio::test]
async fn simulated_pipeline_network_error_is_nonfatal_and_bracketed() {
    use pipecat_core::{
        Direction, ErrorCategory, Frame, FrameData, Pipeline, PipelineConfig, WorkerState,
    };
    let fake = server(
        "503 Unavailable",
        "application/json",
        vec![br#"{"error":{"message":"synthetic unavailable"}}"#.to_vec()],
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let processor = LlmProcessor::new(
        LlmClient::new(config(Provider::OpenAi, fake.url)).unwrap(),
        request(),
    );
    let mut worker = Pipeline::new(vec![std::sync::Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    let mut errors = vec![];
    let mut ends = 0;
    while let Some(frame) = worker.recv().await {
        match frame.frame.data {
            FrameData::Error(e) => {
                assert_eq!(frame.direction, Direction::Upstream);
                errors.push(e);
            }
            FrameData::LlmResponseEnd => ends += 1,
            _ => {}
        }
    }
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0].category, ErrorCategory::Server);
    assert!(!errors[0].fatal);
    assert_eq!(ends, 1);
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    fake.task.await.unwrap();
}

#[tokio::test]
async fn simulated_pipeline_interruption_cancels_http_without_stalling_control() {
    use pipecat_core::{Direction, Frame, FrameData, Pipeline, PipelineConfig, WorkerState};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/stream", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n").await.unwrap();
        let body = record(json!({"choices":[{"delta":{"content":"first"}}]}));
        socket
            .write_all(format!("{:x}\r\n", body.len()).as_bytes())
            .await
            .unwrap();
        socket.write_all(&body).await.unwrap();
        socket.write_all(b"\r\n").await.unwrap();
        let mut buf = [0; 1];
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut buf))
            .await
            .unwrap()
            .unwrap()
    });
    let processor = LlmProcessor::new(
        LlmClient::new(config(Provider::OpenAi, url)).unwrap(),
        request(),
    );
    let mut worker = Pipeline::new(vec![std::sync::Arc::new(processor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::LlmText(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(task.await.unwrap(), 0);
    worker.end().await.unwrap();
    while worker.recv().await.is_some() {}
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[tokio::test]
async fn simulated_anthropic_trailing_user_is_in_outgoing_cached_body() {
    let fake = server(
        "200 OK",
        "text/event-stream",
        vec![record(json!({"type":"message_stop"}))],
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut request = CompletionRequest::new(
        "unknown-synthetic-model",
        Context::new(vec![
            Message::text(Role::User, "hello"),
            Message::text(Role::Assistant, "prefill"),
        ]),
    )
    .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
        enabled: true,
        system_ttl: Some(AnthropicCacheTtl::OneHour),
    });
    request.system_instruction = Some("system".into());
    request.max_tokens = Some(17);
    let mut stream = LlmClient::new(config(Provider::Anthropic, fake.url))
        .unwrap()
        .stream(request)
        .unwrap();
    assert!(collect(&mut stream).await.iter().all(Result::is_ok));
    let (_, body) = fake.request.await.unwrap();
    assert_eq!(
        body,
        json!({
            "model":"unknown-synthetic-model","stream":true,"max_tokens":17,
            "system":[{"type":"text","text":"system","cache_control":{"type":"ephemeral","ttl":"1h"}}],
            "messages":[
                {"role":"user","content":[{"type":"text","text":"hello","cache_control":{"type":"ephemeral"}}]},
                {"role":"assistant","content":[{"type":"text","text":"prefill"}]},
                {"role":"user","content":[{"type":"text","text":".","cache_control":{"type":"ephemeral"}}]}
            ]
        })
    );
    fake.task.await.unwrap();
}

#[tokio::test]
async fn simulated_anthropic_single_pdf_precedes_text_and_cache_in_outgoing_body() {
    let fake = server(
        "200 OK",
        "text/event-stream",
        vec![record(json!({"type":"message_stop"}))],
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut message = Message::text(Role::User, "unused");
    message.content = Some(Content::Parts(vec![
        ContentPart::Text {
            text: "read".into(),
        },
        ContentPart::File {
            data: "JVBERi0xLjQ=".into(),
            mime_type: "application/pdf".into(),
            filename: Some("original.pdf".into()),
        },
    ]));
    let request = CompletionRequest::new("synthetic-fixture", Context::new(vec![message]))
        .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
            enabled: true,
            system_ttl: None,
        });
    let mut stream = LlmClient::new(config(Provider::Anthropic, fake.url))
        .unwrap()
        .stream(request)
        .unwrap();
    assert!(collect(&mut stream).await.iter().all(Result::is_ok));
    let (_, body) = fake.request.await.unwrap();
    assert_eq!(
        body["messages"],
        json!([{"role":"user","content":[
            {"type":"document","source":{"type":"base64","media_type":"application/pdf","data":"JVBERi0xLjQ="},"title":"original.pdf"},
            {"type":"text","text":"read","cache_control":{"type":"ephemeral"}}
        ]}])
    );
    fake.task.await.unwrap();
}

#[tokio::test]
async fn simulated_openai_detailed_image_files_keep_native_identity_and_use_image_wire() {
    use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig};
    use std::sync::Arc;

    let fake = server(
        "200 OK",
        "text/event-stream",
        vec![
            record(json!({"choices":[{"delta":{"content":"seen"},"finish_reason":"stop"}]})),
            b"data: [DONE]\n\n".to_vec(),
        ],
        Duration::ZERO,
        Duration::ZERO,
    )
    .await;
    let mut message: core::ContextMessage = core::ChatMessage::new(core::ChatRole::User, "").into();
    message.content = core::MessageContent::Parts(vec![
        core::MessagePart::Text {
            text: "look".into(),
        },
        core::MessagePart::File {
            data: "YQ==".into(),
            mime_type: "image/png".into(),
            filename: Some("original.png".into()),
        },
        core::MessagePart::Text {
            text: "then".into(),
        },
        core::MessagePart::File {
            data: "/9j/+w==".into(),
            mime_type: "image/jpeg".into(),
            filename: None,
        },
        core::MessagePart::File {
            data: "JVBERg==".into(),
            mime_type: "application/pdf".into(),
            filename: Some("note.pdf".into()),
        },
    ]);
    let context = core::LlmContextData {
        messages: vec![message],
        tools: core::ToolSet::NotGiven,
        tool_choice: None,
    };
    let original = context.clone();
    let expected_native = frame_context::context(&context).unwrap();
    assert!(
        matches!(&expected_native.messages[0].content, Some(Content::Parts(parts)) if matches!(&parts[1], ContentPart::File { filename: Some(name), .. } if name == "original.png"))
    );
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config(Provider::OpenAi, fake.url)).unwrap(),
        request(),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContextDetailed(
                core::DetailedLlmContextFrame {
                    context: context.clone(),
                    speculation: false,
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let frames = tokio::time::timeout(Duration::from_secs(5), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            frames.push(frame);
        }
        frames
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::LlmText(t) if t.text == "seen")));
    assert_eq!(
        processor.request_snapshot().unwrap().context,
        expected_native
    );
    assert_eq!(context, original);
    let (_, body) = tokio::time::timeout(Duration::from_secs(3), fake.request)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        body,
        json!({
            "model":"synthetic-model","stream":true,"stream_options":{"include_usage":true},
            "messages":[{"role":"user","content":[
                {"type":"text","text":"look"},
                {"type":"image_url","image_url":{"url":"data:image/png;base64,YQ=="}},
                {"type":"text","text":"then"},
                {"type":"image_url","image_url":{"url":"data:image/jpeg;base64,/9j/+w=="}},
                {"type":"file","file":{"filename":"note.pdf","file_data":"data:application/pdf;base64,JVBERg=="}}
            ]}]
        })
    );
    fake.task.await.unwrap();
}

#[tokio::test]
async fn simulated_gemini_remote_image_is_serialized_without_local_fetch() {
    // Keep a real media listener open: any attempted fetch connects here rather
    // than contacting a public image host, and fails the assertion below.
    let media = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let uri = format!("http://{}/photo%20one.JpEg", media.local_addr().unwrap());
    let fake = server(
        "200 OK", "text/event-stream",
        vec![record(json!({"candidates":[{"content":{"parts":[{"text":"Synthetic description"}]},"finishReason":"STOP"}]}))],
        Duration::ZERO, Duration::ZERO,
    ).await;
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(vec![
        ContentPart::Text {
            text: "Describe".into(),
        },
        ContentPart::ImageUrl { url: uri.clone() },
    ]));
    let request = CompletionRequest::new("synthetic-model", Context::new(vec![message]));
    let before = serde_json::to_value(&request).unwrap();
    let mut stream = LlmClient::new(config(Provider::Gemini, fake.url))
        .unwrap()
        .stream(request.clone())
        .unwrap();
    let events = collect(&mut stream).await;
    assert!(events.iter().all(Result::is_ok));
    assert!(events.iter().any(
        |event| matches!(event, Ok(StreamEvent::TextDelta(text)) if text == "Synthetic description")
    ));
    assert!(events.iter().any(|event| matches!(
        event,
        Ok(StreamEvent::Finished {
            reason: Some(FinishReason::Stop)
        })
    )));
    let (_, body) = fake.request.await.unwrap();
    assert_eq!(
        body["contents"],
        json!([{"role":"user","parts":[{"text":"Describe"},{"fileData":{"fileUri":uri,"mimeType":"image/jpeg"}}]}])
    );
    assert_eq!(serde_json::to_value(&request).unwrap(), before);
    fake.task.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), media.accept())
            .await
            .is_err(),
        "the client must not connect to the media URI"
    );
}
