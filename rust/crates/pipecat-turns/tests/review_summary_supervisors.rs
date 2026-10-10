//! Independent native-summary supervisor regressions; HTTP stays on loopback.
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
};

fn llm(listener: &TcpListener) -> SummaryLlm {
    let mut config = ClientConfig::new(Provider::OpenAi, "");
    config.api_key = None;
    config.endpoint = format!("http://{}/", listener.local_addr().unwrap());
    SummaryLlm::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("loopback-summary", Context::default()),
    )
}
fn history() -> SharedContext {
    SharedContext::new(Context::new(vec![
        Message::text(Role::User, "Old question"),
        Message::text(Role::Assistant, "Old answer"),
        Message::text(Role::User, "Keep this recent message"),
    ]))
}
fn settings() -> ContextSummarizationConfig {
    ContextSummarizationConfig {
        summary: LlmContextSummaryConfig {
            min_messages_after_summary: 1,
            target_context_tokens: 128,
            summarization_timeout: 30.0,
            ..Default::default()
        },
        ..Default::default()
    }
}
fn request(id: &str) -> LlmContextSummaryRequestFrame {
    let summarizer = LLMContextSummarizer::new(history(), settings()).unwrap();
    let mut request = summarizer.request(None, true).unwrap().unwrap();
    request.request_id = id.into();
    request
}
async fn receive(
    worker: &mut PipelineWorker,
    predicate: impl Fn(&FrameData) -> bool,
) -> DirectedFrame {
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .expect("pipeline output deadline")
            .expect("pipeline remains open");
        assert!(
            !matches!(frame.frame.data, FrameData::Error(_)),
            "unexpected pipeline error: {:?}",
            frame.frame.data
        );
        if predicate(&frame.frame.data) {
            return frame;
        }
    }
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    send(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("supervisor-review".into()),
        }),
    )
    .await;
    receive(worker, |data| matches!(data, FrameData::Flush(_))).await;
}
async fn accepted(listener: &TcpListener) -> TcpStream {
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
        .await
        .expect("summary HTTP request deadline")
        .unwrap();
    let mut request = Vec::new();
    let mut bytes = [0; 4096];
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let count = socket.read(&mut bytes).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&bytes[..count]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let length = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap();
                if request.len() >= end + 4 + length {
                    let body: Value =
                        serde_json::from_slice(&request[end + 4..end + 4 + length]).unwrap();
                    assert_eq!(body["max_tokens"], 128);
                    break;
                }
            }
        }
    })
    .await
    .expect("summary HTTP body deadline");
    socket
}
async fn assert_closed(socket: &mut TcpStream) {
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
            .await
            .expect("obsolete summary HTTP stream must close promptly")
            .unwrap(),
        0
    );
}
async fn reply(socket: &mut TcpStream, text: &str) {
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"finish_reason":null}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop"}]})
    );
    socket
        .write_all(
            format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            )
            .as_bytes(),
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn standalone_concurrency_limit_and_cancelled_same_id_reuse() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let responder = Arc::new(
        LLMContextSummaryProcessor::new(llm(&listener))
            .with_max_in_flight(2)
            .unwrap(),
    );
    let mut worker = Pipeline::new(vec![responder])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    for cycle in 0..12 {
        send(&worker, FrameData::LlmContextSummaryRequest(request("a"))).await;
        let mut first = accepted(&listener).await;
        send(&worker, FrameData::LlmContextSummaryRequest(request("b"))).await;
        let mut second = accepted(&listener).await;
        send(&worker, FrameData::LlmContextSummaryRequest(request("c"))).await;
        let rejected = receive(&mut worker, |data| {
            matches!(data, FrameData::LlmContextSummaryResult(_))
        })
        .await;
        let FrameData::LlmContextSummaryResult(result) = rejected.frame.data else {
            unreachable!()
        };
        assert_eq!(result.request_id, "c");
        assert!(result.error.unwrap().contains("concurrency limit"));
        send(&worker, FrameData::Interruption).await;
        receive(&mut worker, |data| matches!(data, FrameData::Interruption)).await;
        // Admit the replacement before requiring cancelled HTTP work to settle.
        send(&worker, FrameData::LlmContextSummaryRequest(request("a"))).await;
        let mut fresh = accepted(&listener).await;
        assert_closed(&mut first).await;
        assert_closed(&mut second).await;
        let expected = format!("Current summary {cycle}");
        reply(&mut fresh, &expected).await;
        let completed = receive(&mut worker, |data| {
            matches!(data, FrameData::LlmContextSummaryResult(_))
        })
        .await;
        assert_eq!(completed.direction, Direction::Upstream);
        let FrameData::LlmContextSummaryResult(result) = completed.frame.data else {
            unreachable!()
        };
        assert_eq!(result.request_id, "a");
        assert_eq!(result.summary, expected);
        assert_eq!(result.error, None);
    }
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[tokio::test]
async fn standalone_terminal_paths_close_all_inflight_http_requests() {
    for terminal in ["end", "stop", "cancel", "drop"] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let responder = Arc::new(
            LLMContextSummaryProcessor::new(llm(&listener))
                .with_max_in_flight(2)
                .unwrap(),
        );
        let mut worker = Pipeline::new(vec![responder])
            .start(PipelineConfig {
                max_background_tasks: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        receive(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        send(&worker, FrameData::LlmContextSummaryRequest(request("a"))).await;
        let mut first = accepted(&listener).await;
        send(&worker, FrameData::LlmContextSummaryRequest(request("b"))).await;
        let mut second = accepted(&listener).await;
        if terminal == "drop" {
            drop(worker);
        } else {
            match terminal {
                "end" => worker.end().await.unwrap(),
                "stop" => send(&worker, FrameData::Stop).await,
                _ => worker.cancel("supervisor test").await.unwrap(),
            }
            tokio::time::timeout(Duration::from_secs(2), worker.join())
                .await
                .expect("shutdown must not wait for summary deadline")
                .unwrap();
        }
        assert_closed(&mut first).await;
        assert_closed(&mut second).await;
    }
}

#[tokio::test]
async fn context_replacement_cancels_http_in_both_summary_routes() {
    for dedicated in [true, false] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let context = history();
        let mut assistant = LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(settings())
            .unwrap();
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![];
        if dedicated {
            assistant = assistant.with_summary_llm(llm(&listener));
        } else {
            processors.push(Arc::new(LLMContextSummaryProcessor::new(llm(&listener))));
        }
        let assistant = Arc::new(assistant);
        processors.push(assistant.clone());
        let mut worker = Pipeline::new(processors)
            .start(PipelineConfig {
                max_background_tasks: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        receive(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
        send(&worker, FrameData::LlmSummarizeContext(None)).await;
        let mut old = accepted(&listener).await;
        send(
            &worker,
            FrameData::LlmMessagesUpdateDetailed(DetailedMessagesFrame {
                messages: history().snapshot_rich().unwrap().messages,
                run_llm: Some(false),
            }),
        )
        .await;
        barrier(&mut worker).await;
        assert!(!assistant.summarizer().is_pending());
        assert_closed(&mut old).await;
        send(&worker, FrameData::LlmSummarizeContext(None)).await;
        let mut new = accepted(&listener).await;
        reply(&mut new, "Summary after replacement").await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while assistant.summarizer().is_pending() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap();
        assert_eq!(context.snapshot().unwrap().messages.len(), 2);
        worker.end().await.unwrap();
        assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
    }
}

#[tokio::test]
async fn dedicated_supervisor_survives_repeated_interruptions_and_new_requests() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let context = history();
    let assistant = Arc::new(
        LLMAssistantAggregator::new(context.clone(), Default::default())
            .with_summarization(settings())
            .unwrap()
            .with_summary_llm(llm(&listener)),
    );
    let mut worker = Pipeline::new(vec![assistant.clone()])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    for cycle in 0..12 {
        context.replace(history().snapshot().unwrap()).unwrap();
        send(&worker, FrameData::LlmSummarizeContext(None)).await;
        let mut cancelled = accepted(&listener).await;
        send(&worker, FrameData::Interruption).await;
        receive(&mut worker, |data| matches!(data, FrameData::Interruption)).await;
        assert!(!assistant.summarizer().is_pending());
        send(&worker, FrameData::LlmSummarizeContext(None)).await;
        let mut replacement = accepted(&listener).await;
        assert_closed(&mut cancelled).await;
        let expected = format!("Correct context after interruption {cycle}");
        reply(&mut replacement, &expected).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            while assistant.summarizer().is_pending() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("fresh result must apply after each interruption");
        let messages = context.snapshot().unwrap().messages;
        assert_eq!(messages.len(), 2);
        assert_eq!(
            messages[0],
            Message::text(Role::User, format!("Conversation summary: {expected}"))
        );
        assert_eq!(
            messages[1],
            Message::text(Role::User, "Keep this recent message")
        );
    }
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}

#[tokio::test]
async fn concurrent_results_keep_request_identity_and_reverse_each_request_direction() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let responder = Arc::new(
        LLMContextSummaryProcessor::new(llm(&listener))
            .with_max_in_flight(2)
            .unwrap(),
    );
    let mut worker = Pipeline::new(vec![responder])
        .start(PipelineConfig {
            max_background_tasks: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    receive(&mut worker, |data| matches!(data, FrameData::Start(_))).await;
    send(&worker, FrameData::LlmContextSummaryRequest(request("a"))).await;
    let mut first = accepted(&listener).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContextSummaryRequest(request("b"))),
            Direction::Upstream,
        )
        .await
        .unwrap();
    let mut second = accepted(&listener).await;
    // The later request finishes first, in the opposite pipeline direction.
    for (socket, id, direction) in [
        (&mut second, "b", Direction::Downstream),
        (&mut first, "a", Direction::Upstream),
    ] {
        reply(socket, id).await;
        let completed = receive(&mut worker, |data| {
            matches!(data, FrameData::LlmContextSummaryResult(_))
        })
        .await;
        assert_eq!(completed.direction, direction);
        let FrameData::LlmContextSummaryResult(result) = completed.frame.data else {
            unreachable!()
        };
        assert_eq!(result.request_id, id);
        assert_eq!(result.summary, id);
        assert_eq!(result.error, None);
    }
    worker.end().await.unwrap();
    assert_eq!(worker.join().await.unwrap().state, WorkerState::Ended);
}
