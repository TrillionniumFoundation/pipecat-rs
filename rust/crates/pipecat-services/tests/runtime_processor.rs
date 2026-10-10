//! Loopback tests pin future-only update semantics and immutable retries.
use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig, Setting};
use pipecat_services::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};

async fn read_request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
async fn reply(socket: &mut TcpStream, text: &str) {
    let body = format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})
    );
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
}
fn request() -> CompletionRequest {
    let mut request = CompletionRequest::new(
        "before",
        Context::new(vec![Message::text(Role::User, "hello")]),
    );
    request.temperature = Some(0.2);
    request
}
async fn send(worker: &core::PipelineWorker, data: FrameData, direction: Direction) {
    worker
        .sender()
        .send(Frame::new(data), direction)
        .await
        .unwrap();
}
fn settings(model: &str, temperature: Setting<f64>) -> FrameData {
    let mut delta = core::LlmSettings {
        temperature,
        max_tokens: Setting::Value(72),
        ..Default::default()
    };
    delta.base.model = Setting::Value(model.into());
    FrameData::LlmUpdateSettings(Box::new(core::ServiceUpdateSettingsFrame {
        delta: Some(delta),
        ..Default::default()
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn updates_change_future_snapshots_without_rewriting_active_or_queued_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let (capture, mut captured) = mpsc::channel(3);
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        capture
            .send(read_request(&mut first).await.1)
            .await
            .unwrap();
        wait.await.unwrap();
        reply(&mut first, "first").await;
        for output in ["second", "third"] {
            let (mut socket, _) = listener.accept().await.unwrap();
            capture
                .send(read_request(&mut socket).await.1)
                .await
                .unwrap();
            reply(&mut socket, output).await;
        }
    });
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        request(),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::LlmRun, Direction::Upstream).await;
    let first = tokio::time::timeout(Duration::from_secs(3), captured.recv())
        .await
        .unwrap()
        .unwrap();
    send(
        &worker,
        settings("second", Setting::Value(0.6)),
        Direction::Downstream,
    )
    .await;
    send(
        &worker,
        FrameData::LlmSetTools(core::ToolSet::Functions(vec![core::ToolDefinition {
            name: "lookup".into(),
            description: "lookup".into(),
            parameters: json!({"type":"object"}),
        }])),
        Direction::Downstream,
    )
    .await;
    send(
        &worker,
        FrameData::LlmSetToolChoice(core::ToolChoice::Required),
        Direction::Downstream,
    )
    .await;
    send(
        &worker,
        FrameData::LlmConfigureOutput { skip_tts: true },
        Direction::Downstream,
    )
    .await;
    send(&worker, FrameData::LlmRun, Direction::Upstream).await;
    send(
        &worker,
        settings("third", Setting::Null),
        Direction::Downstream,
    )
    .await;
    send(&worker, FrameData::LlmRun, Direction::Upstream).await;
    send(
        &worker,
        FrameData::Text(core::TextFrame::new("fence")),
        Direction::Downstream,
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(out) = worker.recv().await {
            if matches!(out.frame.data, FrameData::Text(_)) {
                break;
            }
        }
    })
    .await
    .unwrap();
    release.send(()).unwrap();
    worker.end().await.unwrap();
    let mut text = vec![];
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(out) = worker.recv().await {
            if let FrameData::LlmText(frame) = out.frame.data {
                text.push((frame.text, frame.skip_tts));
            }
        }
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    server.await.unwrap();
    let second = captured.recv().await.unwrap();
    let third = captured.recv().await.unwrap();
    assert_eq!(first["model"], "before");
    assert_eq!(first["temperature"], 0.2);
    assert!(first.get("tools").is_none());
    assert_eq!(second["model"], "second");
    assert_eq!(second["temperature"], 0.6);
    assert_eq!(second["max_tokens"], 72);
    assert_eq!(second["tools"][0]["function"]["name"], "lookup");
    assert_eq!(second["tool_choice"], "required");
    assert_eq!(third["model"], "third");
    assert!(third["temperature"].is_null());
    assert!(third.get("temperature").is_some());
    assert_eq!(
        text,
        vec![
            ("first".into(), None),
            ("second".into(), Some(true)),
            ("third".into(), Some(true))
        ]
    );
}

#[tokio::test]
async fn rich_upstream_context_append_transform_and_tool_clear_reach_wire() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut captured = vec![];
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            captured.push(read_request(&mut socket).await.1);
            reply(&mut socket, "done").await;
        }
        captured
    });
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        request(),
    ));
    let weak = Arc::downgrade(&processor);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut rich: core::ContextMessage = core::ChatMessage::new(core::ChatRole::User, "").into();
    rich.content = core::MessageContent::Parts(vec![
        core::MessagePart::Text {
            text: "look".into(),
        },
        core::MessagePart::Image {
            url: "data:image/png;base64,YQ==".into(),
            detail: Some("low".into()),
        },
    ]);
    send(
        &worker,
        FrameData::LlmContextDetailed(core::DetailedLlmContextFrame {
            context: core::LlmContextData {
                messages: vec![rich],
                tools: core::ToolSet::Functions(vec![]),
                tool_choice: None,
            },
            speculation: false,
        }),
        Direction::Upstream,
    )
    .await;
    send(
        &worker,
        FrameData::LlmMessagesAppendDetailed(core::DetailedMessagesFrame {
            messages: vec![core::ChatMessage::new(core::ChatRole::Assistant, "answer").into()],
            run_llm: None,
        }),
        Direction::Downstream,
    )
    .await;
    send(
        &worker,
        FrameData::LlmSetTools(core::ToolSet::NotGiven),
        Direction::Downstream,
    )
    .await;
    send(
        &worker,
        FrameData::LlmMessagesTransform(core::LlmMessagesTransformFrame {
            transform: core::LocalMessageTransform::new(move |mut messages| {
                // Reentry must not deadlock on the processor's state mutex.
                assert!(weak.upgrade().unwrap().request_snapshot().is_ok());
                messages.push(core::ChatMessage::new(core::ChatRole::User, "again").into());
                Ok(messages)
            }),
            run_llm: Some(true),
        }),
        Direction::Downstream,
    )
    .await;
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while worker.recv().await.is_some() {}
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    let bodies = server.await.unwrap();
    assert_eq!(
        bodies[0]["messages"][0]["content"][1],
        json!({"type":"image_url","image_url":{"url":"data:image/png;base64,YQ==","detail":"low"}})
    );
    assert_eq!(bodies[0]["tools"], json!([]));
    assert!(bodies[1].get("tools").is_none());
    assert_eq!(bodies[1]["messages"].as_array().unwrap().len(), 3);
    assert_eq!(bodies[1]["messages"][2]["content"], "again");
}

#[tokio::test]
async fn target_mismatch_passes_through_and_bad_updates_do_not_poison_state() {
    let config = ClientConfig::new(Provider::OpenAi, "synthetic");
    let processor = Arc::new(
        LlmProcessor::new(LlmClient::new(config.clone()).unwrap(), request()).with_name("same"),
    );
    let different =
        Arc::new(LlmProcessor::new(LlmClient::new(config).unwrap(), request()).with_name("same"));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut delta = core::LlmSettings::default();
    delta.base.model = Setting::Value("wrong".into());
    send(
        &worker,
        FrameData::LlmUpdateSettings(Box::new(core::ServiceUpdateSettingsFrame {
            delta: Some(delta.clone()),
            service: Some(core::ProcessorReference::Local(
                core::LocalProcessorReference(different),
            )),
            ..Default::default()
        })),
        Direction::Upstream,
    )
    .await;
    delta.base.model = Setting::Null;
    send(
        &worker,
        FrameData::LlmUpdateSettings(Box::new(core::ServiceUpdateSettingsFrame {
            delta: Some(delta),
            ..Default::default()
        })),
        Direction::Downstream,
    )
    .await;
    let mut bad: core::ContextMessage = core::ChatMessage::new(core::ChatRole::User, "bad").into();
    bad.extra.insert("vendor".into(), json!(true));
    send(
        &worker,
        FrameData::LlmMessagesUpdateDetailed(core::DetailedMessagesFrame {
            messages: vec![bad],
            run_llm: Some(true),
        }),
        Direction::Downstream,
    )
    .await;
    worker.end().await.unwrap();
    let mut errors = 0;
    let mut forwarded = false;
    while let Some(out) = worker.recv().await {
        match out.frame.data {
            FrameData::Error(error) => {
                assert_eq!(error.category, core::ErrorCategory::InvalidRequest);
                errors += 1;
            }
            FrameData::LlmUpdateSettings(_) => forwarded = out.direction == Direction::Upstream,
            _ => {}
        }
    }
    worker.join().await.unwrap();
    assert!(forwarded);
    assert_eq!(errors, 2);
    let body =
        adapters::request_body(Provider::OpenAi, &processor.request_snapshot().unwrap()).unwrap();
    assert_eq!(body["model"], "before");
    assert_eq!(body["messages"][0]["content"], "hello");
}

#[tokio::test]
async fn opt_in_header_timeout_retries_exact_snapshot_once() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(30),
    };
    let (first_ready, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let first_body = read_request(&mut first).await.1;
        first_ready.send(()).unwrap();
        let (mut second, _) = listener.accept().await.unwrap();
        let second_body = read_request(&mut second).await.1;
        let mut byte = [0];
        assert_eq!(first.read(&mut byte).await.unwrap(), 0);
        reply(&mut second, "retried").await;
        (first_body, second_body)
    });
    let processor = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        request(),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::LlmRun, Direction::Downstream).await;
    tokio::time::timeout(Duration::from_secs(3), ready)
        .await
        .unwrap()
        .unwrap();
    let mut delta = core::LlmSettings::default();
    delta.base.model = Setting::Value("future".into());
    processor.update_settings(&delta).unwrap();
    worker.end().await.unwrap();
    let mut responses = 0;
    let mut errors = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(out) = worker.recv().await {
            match out.frame.data {
                FrameData::LlmResponseStart => responses += 1,
                FrameData::Error(_) => errors += 1,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    worker.join().await.unwrap();
    let (first, second) = server.await.unwrap();
    assert_eq!(first, second);
    assert_eq!(first["model"], "before");
    assert_eq!(responses, 1);
    assert_eq!(errors, 0);
    assert_eq!(
        adapters::request_body(Provider::OpenAi, &processor.request_snapshot().unwrap()).unwrap()
            ["model"],
        "future"
    );
}

#[tokio::test]
async fn header_retry_never_replays_a_stream_that_already_started() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    config.retry = RetryPolicy::OnceOnHeaderTimeout {
        timeout: Duration::from_millis(100),
    };
    config.idle_timeout = Duration::from_millis(30);
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        let record = format!(
            "data: {}\n\n",
            json!({"choices":[{"delta":{"content":"partial"}}]})
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n{:x}\r\n{record}\r\n",record.len()).as_bytes()).await.unwrap();
        let mut byte = [0];
        assert_eq!(socket.read(&mut byte).await.unwrap(), 0);
        assert!(
            tokio::time::timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
    });
    let mut stream = LlmClient::new(config).unwrap().stream(request()).unwrap();
    let mut text = vec![];
    let mut timeouts = 0;
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(event) = stream.next().await {
            match event {
                Ok(StreamEvent::TextDelta(part)) => text.push(part),
                Err(ServiceError::Timeout) => timeouts += 1,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    server.await.unwrap();
    assert_eq!(text, vec!["partial"]);
    assert_eq!(timeouts, 1);
}

#[tokio::test]
async fn preset_transport_uses_transformed_body_and_normalized_usage() {
    use pipecat_services::presets::{OpenAiPreset, OpenAiPresetConfig};
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/wire", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut bodies = vec![];
        for _ in 0..2 {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (headers, body) = read_request(&mut socket).await;
            assert!(headers
                .to_ascii_lowercase()
                .contains("authorization: bearer synthetic"));
            bodies.push(body);
            let response = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":{},"finish_reason":"stop"}],"usage":{"prompt_tokens":7,"completion_tokens":3,"total_tokens":12,"completion_tokens_details":{"reasoning_tokens":2}}})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        }
        bodies
    });
    let mut usage = vec![];
    for preset in [OpenAiPreset::Fireworks, OpenAiPreset::Xai] {
        let preset = OpenAiPresetConfig::new(preset);
        let mut request = preset.request(Context::new(vec![Message::text(
            Role::Developer,
            "instruction",
        )]));
        request.max_tokens = Some(16);
        let mut config = ClientConfig::openai_compatible(preset, "synthetic").unwrap();
        config.endpoint = endpoint.clone();
        let mut stream = LlmClient::new(config).unwrap().stream(request).unwrap();
        while let Some(event) = stream.next().await {
            if let StreamEvent::Usage(value) = event.unwrap() {
                usage.push(value);
            }
        }
    }
    let bodies = server.await.unwrap();
    assert!(bodies[0].get("stream_options").is_none());
    assert_eq!(bodies[0]["max_tokens"], 16);
    assert!(bodies[0].get("max_completion_tokens").is_none());
    assert_eq!(bodies[1]["max_completion_tokens"], 16);
    assert_eq!(bodies[1]["stream_options"], json!({"include_usage":true}));
    assert_eq!(usage[0].completion_tokens, 3);
    assert_eq!(usage[1].completion_tokens, 5);
    assert_eq!(usage[1].total_tokens, 12);
}

#[tokio::test]
async fn gemini_dynamic_model_updates_url_and_body_from_same_snapshot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::Gemini, "synthetic");
    config.endpoint = format!(
        "http://{}/models/{{model}}:streamGenerateContent?alt=sse",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let captured = read_request(&mut socket).await;
        let response = format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":[{"text":"done"}]},"finishReason":"STOP"}]})
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{response}",response.len()).as_bytes()).await.unwrap();
        captured
    });
    let mut request = request();
    request.settings.base.model = Setting::Value("models/new-model".into());
    request.settings.temperature = Setting::Value(0.8);
    let mut stream = LlmClient::new(config).unwrap().stream(request).unwrap();
    while let Some(event) = stream.next().await {
        event.unwrap();
    }
    let (headers, body) = server.await.unwrap();
    assert!(headers.starts_with("POST /models/new-model:streamGenerateContent?alt=sse HTTP/1.1"));
    assert_eq!(body["generationConfig"]["temperature"], 0.8);
}

struct FenceBoundary;
#[core::async_trait]
impl core::FrameProcessor for FenceBoundary {
    fn name(&self) -> &str {
        "FenceBoundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &core::ProcessorContext,
    ) -> Result<(), core::ProcessorError> {
        if matches!(&frame.data,FrameData::FlushDetailed(flush) if flush.origin.as_deref()==Some("pipecat.flows"))
        {
            ctx.push(
                Frame::text("fence completed").with_interruptible(false),
                direction,
            )
            .await
        } else {
            ctx.push(frame, direction).await
        }
    }
}
fn flow_fence() -> Frame {
    Frame::new(FrameData::FlushDetailed(core::PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        ..Default::default()
    }))
    .with_interruptible(false)
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flow_fence_is_fifo_between_accepted_completions() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let (entered, wait_entered) = oneshot::channel();
    let (release, wait_release) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        read_request(&mut first).await;
        entered.send(()).unwrap();
        wait_release.await.unwrap();
        reply(&mut first, "first").await;
        let (mut second, _) = listener.accept().await.unwrap();
        read_request(&mut second).await;
        reply(&mut second, "second").await;
    });
    let worker = Pipeline::new(vec![
        Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            request(),
        )),
        Arc::new(FenceBoundary),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, FrameData::LlmRun, Direction::Downstream).await;
    tokio::time::timeout(Duration::from_secs(3), wait_entered)
        .await
        .unwrap()
        .unwrap();
    worker
        .sender()
        .send(flow_fence(), Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::LlmRun, Direction::Downstream).await;
    release.send(()).unwrap();
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let markers: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::LlmResponseStart => Some("start"),
            FrameData::LlmResponseEnd => Some("end"),
            FrameData::Text(t) if t.text == "fence completed" => Some("fence"),
            _ => None,
        })
        .collect();
    assert_eq!(markers, vec!["start", "end", "fence", "start", "end"]);
    server.await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flow_fence_survives_interruption_of_prior_completion() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "synthetic");
    config.endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
    let (entered, wait_entered) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        entered.send(()).unwrap();
        let mut byte = [0; 1];
        assert_eq!(socket.read(&mut byte).await.unwrap_or(0), 0);
    });
    let worker = Pipeline::new(vec![
        Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            request(),
        )),
        Arc::new(FenceBoundary),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, FrameData::LlmRun, Direction::Downstream).await;
    wait_entered.await.unwrap();
    worker
        .sender()
        .send(flow_fence(), Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::Interruption, Direction::Downstream).await;
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(&f.frame.data,FrameData::Text(t) if t.text=="fence completed"))
            .count(),
        1
    );
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
