//! Actual native HTTP/SSE summarization and owned-task lifecycle; loopback only.
use pipecat_core::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Semaphore},
};

fn history() -> SharedContext {
    SharedContext::new(Context::new(vec![
        Message::text(Role::System, "keep rules"),
        Message::text(Role::User, "Remember Paris"),
        Message::text(Role::Assistant, "Paris remembered"),
        Message::text(Role::User, "recent question"),
        Message::text(Role::Assistant, "recent answer"),
    ]))
}
fn settings() -> ContextSummarizationConfig {
    ContextSummarizationConfig {
        summary: LlmContextSummaryConfig {
            min_messages_after_summary: 2,
            target_context_tokens: 123,
            summarization_timeout: 10.0,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn llm(endpoint: String) -> SummaryLlm {
    let mut config = ClientConfig::new(Provider::OpenAi, "");
    config.api_key = None;
    config.endpoint = endpoint;
    let mut request = CompletionRequest::new("loopback-test", Context::default());
    request.max_tokens = Some(999);
    request.settings.max_tokens = Setting::Value(888);
    request.system_instruction = Some("conversation prompt must not leak".into());
    request.context.tools = Some(vec![pipecat_services::ToolDefinition {
        name: "danger".into(),
        description: "must not be enabled".into(),
        parameters: json!({"type":"object"}),
    }]);
    request.context.tool_choice = Some(pipecat_services::ToolChoice::Required);
    SummaryLlm::new(LlmClient::new(config).unwrap(), request)
}
async fn read_request(socket: &mut TcpStream) -> Value {
    let mut request = Vec::new();
    let mut bytes = [0; 4096];
    loop {
        let count = socket.read(&mut bytes).await.unwrap();
        assert!(count > 0);
        request.extend_from_slice(&bytes[..count]);
        assert!(request.len() < 1024 * 1024);
        if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..end]);
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|text| text.trim().parse::<usize>().ok())
                })
                .unwrap();
            if request.len() >= end + 4 + length {
                return serde_json::from_slice(&request[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}
async fn reply(socket: &mut TcpStream, content: &str, finish: &str) {
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":content},"finish_reason":null}]}),
        json!({"choices":[{"delta":{},"finish_reason":finish}]})
    );
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
}
async fn recv_until(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .expect("output deadline")
            .expect("worker output");
        if predicate(&frame.frame.data) {
            return frame;
        }
    }
}
async fn barrier(worker: &mut PipelineWorker) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::Flush(FlushFrame {
                returning: false,
                origin: Some("summary-barrier".into()),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    recv_until(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
async fn compressed(worker: &mut PipelineWorker, context: &SharedContext, expected: usize) {
    tokio::time::timeout(Duration::from_secs(3),async {
        while context.snapshot().unwrap().messages.len()!=expected {
            tokio::select! {
                _=tokio::time::sleep(Duration::from_millis(5))=>{},
                out=worker.recv()=>{
                    let out=out.expect("worker output");
                    assert!(!matches!(out.frame.data,FrameData::Error(_)),"unexpected error: {:?}",out.frame.data);
                }
            }
        }
    }).await.expect("summary completion");
}

#[tokio::test]
async fn dedicated_and_request_result_routes_make_native_tool_free_http_inference() {
    for dedicated in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!(
            "http://{}/v1/chat/completions",
            listener.local_addr().unwrap()
        );
        let (sent, received) = oneshot::channel();
        let gate = Arc::new(Semaphore::new(0));
        let release = gate.clone();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let body = read_request(&mut socket).await;
            sent.send(body).unwrap();
            release.acquire().await.unwrap().forget();
            reply(&mut socket, "User wants Paris remembered.", "stop").await;
        });
        let context = history();
        let mut config = settings();
        config.auto_trigger = true;
        config.max_context_tokens = None;
        config.max_unsummarized_messages = Some(4);
        let summary_llm = llm(endpoint);
        let mut assistant = LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(config)
            .unwrap();
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![];
        if dedicated {
            assistant = assistant.with_summary_llm(summary_llm);
        } else {
            processors.push(Arc::new(LLMContextSummaryProcessor::new(summary_llm)));
        }
        let assistant = Arc::new(assistant);
        processors.push(assistant.clone());
        let mut worker = Pipeline::new(processors)
            .start(PipelineConfig {
                max_background_tasks: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmResponseStart),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let body = tokio::time::timeout(Duration::from_secs(3), received)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(body["max_tokens"], 123);
        assert!(body.get("tools").is_none());
        assert!(body.get("tool_choice").is_none());
        let text = body.to_string();
        assert!(text.contains("Remember Paris"));
        assert!(!text.contains("keep rules"));
        assert!(!text.contains("recent question"));
        assert!(!text.contains("conversation prompt must not leak"));
        assert_eq!(body["messages"][1]["role"], "user");
        // Inference runs out of band: a normal response can finish while held.
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmText(TextFrame::new("concurrent answer"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        assert_eq!(context.snapshot().unwrap().messages.len(), 6);
        assert!(assistant.summarizer().is_pending());
        gate.add_permits(1);
        compressed(&mut worker, &context, 5).await;
        let messages = context.snapshot().unwrap().messages;
        assert_eq!(messages[0], Message::text(Role::System, "keep rules"));
        assert_eq!(
            messages[1],
            Message::text(
                Role::User,
                "Conversation summary: User wants Paris remembered."
            )
        );
        assert_eq!(
            messages[4],
            Message::text(Role::Assistant, "concurrent answer")
        );
        worker.end().await.unwrap();
        assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
        server.await.unwrap();
    }
}
#[tokio::test(start_paused = true)]
async fn absent_external_result_times_out_reports_error_and_allows_retry() {
    let context = history();
    let mut config = settings();
    config.summary.summarization_timeout = 1.0;
    let assistant = Arc::new(
        LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(config)
            .unwrap(),
    );
    let mut worker = Pipeline::new(vec![assistant.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSummarizeContext(None)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let request = recv_until(&mut worker, |data| {
        matches!(data, FrameData::LlmContextSummaryRequest(_))
    })
    .await;
    tokio::time::advance(Duration::from_secs(2)).await;
    let error = recv_until(&mut worker, |data| matches!(data, FrameData::Error(_))).await;
    let FrameData::Error(error) = error.frame.data else {
        panic!()
    };
    assert!(!error.fatal);
    assert!(error.error.contains("timed out"));
    assert!(!assistant.summarizer().is_pending());
    assert_eq!(context.snapshot().unwrap().messages.len(), 5);
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSummarizeContext(None)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let next = recv_until(&mut worker, |data| {
        matches!(data, FrameData::LlmContextSummaryRequest(_))
    })
    .await;
    assert_ne!(request.frame.data, next.frame.data);
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
#[tokio::test]
async fn end_stop_cancel_and_worker_drop_close_owned_http_requests_without_waiting_for_timeout() {
    for end in ["end", "stop", "cancel", "drop"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let (sent, received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let _ = sent.send(());
            let mut byte = [0];
            tokio::time::timeout(Duration::from_secs(2), socket.read(&mut byte))
                .await
                .expect("HTTP request should be cancelled")
                .unwrap()
        });
        let context = history();
        let assistant = Arc::new(
            LLMAssistantAggregator::new(context.clone(), Default::default())
                .with_summarization(settings())
                .unwrap()
                .with_summary_llm(llm(endpoint)),
        );
        let mut worker = Pipeline::new(vec![assistant.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSummarizeContext(None)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), received)
            .await
            .unwrap()
            .unwrap();
        if end == "drop" {
            drop(worker);
        } else {
            match end {
                "end" => worker.end().await.unwrap(),
                "stop" => worker
                    .sender()
                    .send(Frame::new(FrameData::Stop), Direction::Downstream)
                    .await
                    .unwrap(),
                _ => worker.cancel("test").await.unwrap(),
            };
            tokio::time::timeout(Duration::from_secs(2), worker.join())
                .await
                .expect("terminal cleanup must not drain summary timeout")
                .unwrap();
        }
        assert_eq!(server.await.unwrap(), 0);
        assert_eq!(context.snapshot().unwrap().messages.len(), 5);
    }
}
#[tokio::test]
async fn interruption_rejects_late_results_and_cancels_native_stream() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/", listener.local_addr().unwrap());
    let (sent, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        sent.send(()).unwrap();
        let mut b = [0];
        tokio::time::timeout(Duration::from_secs(2), socket.read(&mut b))
            .await
            .unwrap()
            .unwrap()
    });
    let context = history();
    let assistant = Arc::new(
        LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(settings())
            .unwrap()
            .with_summary_llm(llm(endpoint)),
    );
    let mut worker = Pipeline::new(vec![assistant.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSummarizeContext(None)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    received.await.unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Interruption)).await;
    assert_eq!(server.await.unwrap(), 0);
    assert!(!assistant.summarizer().is_pending());
    assert_eq!(context.snapshot().unwrap().messages.len(), 5);
    worker.end().await.unwrap();
    worker.join().await.unwrap();
}
#[tokio::test]
async fn real_http_provider_error_empty_and_truncated_output_leave_history_intact() {
    for (content, finish) in [
        ("", "stop"),
        ("partial", "length"),
        ("blocked", "content_filter"),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            reply(&mut socket, content, finish).await;
        });
        let context = history();
        let assistant = Arc::new(
            LLMAssistantAggregator::new(context.clone(), Default::default())
                .with_summarization(settings())
                .unwrap()
                .with_summary_llm(llm(endpoint)),
        );
        let mut worker = Pipeline::new(vec![assistant.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSummarizeContext(None)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let out = recv_until(&mut worker, |data| matches!(data, FrameData::Error(_))).await;
        let FrameData::Error(error) = out.frame.data else {
            panic!()
        };
        assert!(!error.fatal);
        assert_eq!(context.snapshot().unwrap().messages.len(), 5);
        assert!(!assistant.summarizer().is_pending());
        worker.end().await.unwrap();
        worker.join().await.unwrap();
        server.await.unwrap();
    }
}
#[tokio::test(start_paused = true)]
async fn repeated_external_requests_reuse_bounded_capacity() {
    let context = history();
    let assistant = Arc::new(
        LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(settings())
            .unwrap(),
    );
    let mut worker = Pipeline::new(vec![assistant])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    for _ in 0..100 {
        context.replace(history().snapshot().unwrap()).unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSummarizeContext(None)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let out = recv_until(&mut worker, |data| {
            matches!(data, FrameData::LlmContextSummaryRequest(_))
        })
        .await;
        let FrameData::LlmContextSummaryRequest(request) = out.frame.data else {
            panic!()
        };
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContextSummaryResult(
                    LlmContextSummaryResultFrame {
                        request_id: request.request_id,
                        summary: "brief".into(),
                        last_summarized_index: 2,
                        error: None,
                    },
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
        barrier(&mut worker).await;
        assert_eq!(context.snapshot().unwrap().messages.len(), 4);
    }
    worker.end().await.unwrap();
    worker.join().await.unwrap();
}

#[tokio::test]
async fn anthropic_and_gemini_summary_clients_use_actual_native_wire_protocols() {
    for provider in [Provider::Anthropic, Provider::Gemini] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = read_request(&mut socket).await;
            assert!(request.get("tools").is_none());
            assert!(!request.to_string().contains("recent question"));
            let records = match provider {
                Provider::Anthropic => {
                    assert_eq!(request["max_tokens"], 123);
                    vec![
                        json!({"type":"message_start","message":{"usage":{"input_tokens":4,"output_tokens":0}}}),
                        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
                        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"Paris remembered"}}),
                        json!({"type":"content_block_stop","index":0}),
                        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"},"usage":{"output_tokens":3}}),
                        json!({"type":"message_stop"}),
                    ]
                }
                Provider::Gemini => {
                    assert_eq!(request["generationConfig"]["maxOutputTokens"], 123);
                    vec![
                        json!({"candidates":[{"index":0,"content":{"parts":[{"text":"Paris remembered"}]},"finishReason":"STOP"}]}),
                    ]
                }
                _ => unreachable!(),
            };
            let body = records
                .iter()
                .map(|record| format!("data: {record}\n\n"))
                .collect::<String>();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
        });
        let mut config = ClientConfig::new(provider, "");
        config.api_key = None;
        config.endpoint = endpoint;
        let summary = SummaryLlm::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("loopback", Context::default()),
        );
        let context = history();
        let assistant = Arc::new(
            LLMAssistantAggregator::new(context.clone(), Default::default())
                .with_summary_llm(summary)
                .with_summarization(settings())
                .unwrap(),
        );
        let mut worker = Pipeline::new(vec![assistant])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSummarizeContext(None)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        compressed(&mut worker, &context, 4).await;
        assert_eq!(
            context.snapshot().unwrap().messages[1],
            Message::text(Role::User, "Conversation summary: Paris remembered")
        );
        worker.end().await.unwrap();
        worker.join().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn native_http_errors_and_returned_tool_calls_are_nonfatal() {
    for tool_call in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            let (status, mime, body) = if tool_call {
                (
                    "200 OK",
                    "text/event-stream",
                    format!(
                        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"unexpected","type":"function","function":{"name":"bad","arguments":"{}"}}]},"finish_reason":null}]}),
                        json!({"choices":[{"delta":{},"finish_reason":"tool_calls"}]})
                    ),
                )
            } else {
                (
                    "500 Internal Server Error",
                    "application/json",
                    json!({"error":{"message":"test failure"}}).to_string(),
                )
            };
            let _ = socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
        });
        let context = history();
        let assistant = Arc::new(
            LLMAssistantAggregator::new(context.clone(), Default::default())
                .with_summarization(settings())
                .unwrap()
                .with_summary_llm(llm(endpoint)),
        );
        let mut worker = Pipeline::new(vec![assistant.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        recv_until(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSummarizeContext(None)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let out = recv_until(&mut worker, |data| matches!(data, FrameData::Error(_))).await;
        let FrameData::Error(error) = out.frame.data else {
            panic!()
        };
        assert!(!error.fatal);
        assert!(error
            .error
            .contains(if tool_call { "tool call" } else { "500" }));
        assert!(!assistant.summarizer().is_pending());
        assert_eq!(context.snapshot().unwrap().messages.len(), 5);
        worker.end().await.unwrap();
        worker.join().await.unwrap();
        server.await.unwrap();
    }
}
