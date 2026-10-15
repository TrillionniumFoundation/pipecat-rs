//! Native Anthropic redacted history through real TCP/SSE and owned pipelines.
//! Opaque fixtures are synthetic strings, not provider-issued encrypted content.
// Reuse the four existing signed-history cases without editing their source or
// claiming their older helpers have this fixture's bounded/owned cleanup.
#[path = "anthropic_history_runtime.rs"]
mod existing_signed_history;

use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    frame_context, protocol::ProtocolDecoder, sse::SseEvent, ClientConfig, CompletionRequest,
    Content, ContentPart, Context, LlmClient, LlmProcessor, Message, Provider, Role, StreamEvent,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

const WAIT: Duration = Duration::from_secs(8);
const MAX_HEADER: usize = 64 * 1024;
const MAX_BODY: usize = 1024 * 1024;
const OPAQUE_A: &str = "opaque:not-base64+/=雪\n\"quoted\"\\tail";
const OPAQUE_B: &str = "second-redaction::\u{0000}::opaque==";
const UNFINISHED: &str = "unfinished-redaction-must-not-survive";
const PROGRESS: &str = "unfinished thought progress marker";
type Check<T> = Result<T, String>;
fn error(value: impl Display) -> String {
    value.to_string()
}

async fn accept(listener: &TcpListener) -> Check<TcpStream> {
    tokio::time::timeout(WAIT, listener.accept())
        .await
        .map_err(|_| "HTTP accept timeout".to_string())?
        .map(|(socket, _)| socket)
        .map_err(error)
}

async fn request(socket: &mut TcpStream) -> Check<Value> {
    tokio::time::timeout(WAIT, async {
        let mut bytes = Vec::new();
        let mut buffer = [0; 4096];
        let header = loop {
            let count = socket.read(&mut buffer).await.map_err(error)?;
            if count == 0 {
                return Err("EOF before HTTP headers".into());
            }
            bytes.extend_from_slice(&buffer[..count]);
            if let Some(end) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
                if end + 4 > MAX_HEADER {
                    return Err("HTTP headers exceed fixture bound".into());
                }
                break end + 4;
            }
            if bytes.len() > MAX_HEADER {
                return Err("HTTP headers exceed fixture bound".into());
            }
        };
        let length = std::str::from_utf8(&bytes[..header])
            .map_err(error)?
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|value| value.trim().parse::<usize>())
            })
            .ok_or_else(|| "HTTP content-length missing".to_string())?
            .map_err(error)?;
        if length > MAX_BODY {
            return Err("HTTP body exceeds fixture bound".into());
        }
        let end = header + length;
        while bytes.len() < end {
            let remaining = (end - bytes.len()).min(buffer.len());
            let count = socket.read(&mut buffer[..remaining]).await.map_err(error)?;
            if count == 0 {
                return Err("EOF before complete HTTP body".into());
            }
            bytes.extend_from_slice(&buffer[..count]);
        }
        serde_json::from_slice(&bytes[header..end]).map_err(error)
    })
    .await
    .map_err(|_| "HTTP read timeout".to_string())?
}

async fn respond(socket: &mut TcpStream, events: &[Value]) -> Check<()> {
    let body: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    if body.len() > MAX_BODY {
        return Err("SSE fixture exceeds bound".into());
    }
    tokio::time::timeout(WAIT, async {
        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n", body.len()).as_bytes()).await.map_err(error)?;
        // Split inside JSON strings and multibyte opaque characters as well.
        for chunk in body.as_bytes().chunks(7) {
            socket.write_all(chunk).await.map_err(error)?;
            tokio::task::yield_now().await;
        }
        Ok(())
    })
    .await
    .map_err(|_| "SSE write timeout".to_string())?
}

fn empty_response() -> [Value; 2] {
    [
        json!({"type":"message_start","message":{}}),
        json!({"type":"message_stop"}),
    ]
}

async fn join_peer<T>(peer: &mut JoinHandle<Check<T>>) -> Check<T> {
    match tokio::time::timeout(WAIT, &mut *peer).await {
        Ok(result) => result.map_err(error)?,
        Err(_) => {
            peer.abort();
            let joined = peer.await;
            Err(format!(
                "HTTP peer timeout; aborted peer actually joined: {}",
                joined.is_err()
            ))
        }
    }
}

async fn finish(worker: &mut PipelineWorker) -> Check<WorkerReport> {
    let ending = if worker.has_finished() {
        Ok(())
    } else {
        worker.end().await
    };
    if ending.is_err() {
        let _ = worker.cancel("fixture cleanup after failed end").await;
    }
    // A worker is never detached on a failed assertion or a dropped timeout.
    let joined = worker.join().await.map_err(error);
    ending.map_err(error)?;
    joined
}

fn llm(endpoint: String, context: Context) -> Check<Arc<LlmProcessor>> {
    let mut config = ClientConfig::new(Provider::Anthropic, "synthetic-local-fixture-key");
    config.endpoint = endpoint;
    config.request_timeout = WAIT;
    Ok(Arc::new(LlmProcessor::new(
        LlmClient::new(config).map_err(error)?,
        CompletionRequest::new("fixture", context),
    )))
}

fn tools(effects: Arc<AtomicUsize>) -> Check<Arc<ToolExecutor>> {
    let mut tools = ToolExecutor::new(3, 128, 4096).map_err(error)?;
    tools
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let effects = effects.clone();
                Box::pin(async move {
                    effects.fetch_add(1, Ordering::SeqCst);
                    Ok(call.arguments)
                })
            })),
        )
        .map_err(error)?;
    Ok(Arc::new(tools))
}

#[derive(Default)]
struct Payloads {
    text: Vec<String>,
    thought: Vec<String>,
    checkpoint_redactions: Vec<String>,
}
struct Capture(Arc<Mutex<Payloads>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "RedactedFixtureCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            let mut captured = self
                .0
                .lock()
                .map_err(|_| ProcessorError::new("fixture capture poisoned"))?;
            let target = match &frame.data {
                FrameData::LlmText(text) => Some((&mut captured.text, &text.text)),
                FrameData::LlmThoughtText(text) => Some((&mut captured.thought, &text.text)),
                _ => None,
            };
            if let Some((target, text)) = target {
                if target.len() >= 64 || text.len() > 4096 {
                    return Err(ProcessorError::new("fixture capture bound exceeded"));
                }
                target.push(text.clone());
            }
            if matches!(frame.data, FrameData::LlmContinuationCheckpoint) {
                if let Some(history) = frame
                    .native_llm_continuation()
                    .map_err(|e| ProcessorError::new(e.to_string()))?
                {
                    for block in history.blocks() {
                        if let LlmContinuationBlock::RedactedThinking(data) = block {
                            if captured.checkpoint_redactions.len() >= 64 || data.len() > 4096 {
                                return Err(ProcessorError::new(
                                    "fixture checkpoint bound exceeded",
                                ));
                            }
                            captured.checkpoint_redactions.push(data.clone());
                        }
                    }
                }
            }
        }
        ctx.push(frame, direction).await
    }
}
struct Progress(Arc<tokio::sync::Semaphore>);
#[async_trait]
impl PipelineObserver for Progress {
    async fn on_event(&self, event: ObservationEvent) {
        if matches!(&*event.observation, PipelineObservation::Process { processor, frame, .. }
            if processor.name.as_ref() == "LLMAssistantAggregator" && frame.kind == "LLMThoughtTextFrame")
        {
            self.0.add_permits(1);
        }
    }
}

async fn start_worker(
    endpoint: String,
    shared: SharedContext,
    effects: Arc<AtomicUsize>,
    capture: Arc<Mutex<Payloads>>,
    progress: Option<Arc<tokio::sync::Semaphore>>,
) -> Check<PipelineWorker> {
    let processors: Vec<Arc<dyn FrameProcessor>> = vec![
        llm(endpoint, shared.snapshot().map_err(error)?)?,
        Arc::new(Capture(capture)),
        tools(effects)?,
        Arc::new(LLMAssistantAggregator::new(shared, Default::default())),
    ];
    let pipeline = Pipeline::new(processors);
    let pipeline = match progress {
        Some(ready) => {
            pipeline.with_observer(Arc::new(Progress(ready)), ObserverOptions::default())
        }
        None => pipeline,
    };
    pipeline.start(Default::default()).await.map_err(error)
}

#[tokio::test]
async fn mixed_redacted_history_and_real_tools_replay_exact_second_http_order() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let shared = SharedContext::new(Context::new(vec![Message::text(Role::User, "question")]));
    let effects = Arc::new(AtomicUsize::new(0));
    let capture = Arc::new(Mutex::new(Payloads::default()));
    // No HTTP child exists if startup fails.
    let mut worker = start_worker(endpoint, shared, effects.clone(), capture.clone(), None)
        .await
        .unwrap();
    let mut peer = tokio::spawn(async move {
        let mut first = accept(&listener).await?;
        let original = request(&mut first).await?;
        respond(&mut first, &[
            json!({"type":"message_start","message":{}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"prefix"}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":"visible thought"}}),
            json!({"type":"content_block_delta","index":1,"delta":{"type":"signature_delta","signature":"signed+/=="}}),
            json!({"type":"content_block_stop","index":1}),
            // Provider indexes, not arrival/closure order, control replay.
            json!({"type":"content_block_start","index":4,"content_block":{"type":"redacted_thinking","data":OPAQUE_B}}),
            json!({"type":"content_block_stop","index":4}),
            json!({"type":"content_block_start","index":2,"content_block":{"type":"redacted_thinking","data":OPAQUE_A}}),
            json!({"type":"content_block_stop","index":2}),
            json!({"type":"content_block_start","index":6,"content_block":{"type":"tool_use","id":"tool-b","name":"echo","input":{"n":2}}}),
            json!({"type":"content_block_stop","index":6}),
            json!({"type":"content_block_start","index":3,"content_block":{"type":"tool_use","id":"tool-a","name":"echo","input":{"n":1}}}),
            json!({"type":"content_block_stop","index":3}),
            json!({"type":"content_block_start","index":5,"content_block":{"type":"text","text":"suffix"}}),
            json!({"type":"content_block_stop","index":5}),
            json!({"type":"message_delta","delta":{"stop_reason":"tool_use"}}),
            json!({"type":"message_stop"}),
        ]).await?;
        drop(first);
        let mut second = accept(&listener).await?;
        let continuation = request(&mut second).await?;
        respond(&mut second, &empty_response()).await?;
        Ok((original, continuation))
    });
    let sent = worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await;
    if sent.is_err() {
        peer.abort();
    }
    let exchange = join_peer(&mut peer).await;
    let joined = finish(&mut worker).await;
    // Every spawned owner has been joined before the first assertion.
    sent.unwrap();
    let (first, second) = exchange.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    assert_eq!(
        first["messages"],
        json!([{"role":"user","content":[{"type":"text","text":"question"}]}])
    );
    assert_eq!(second["messages"].as_array().unwrap().len(), 3);
    assert_eq!(
        second["messages"][1],
        json!({"role":"assistant","content":[
            {"type":"text","text":"prefix"},
            {"type":"thinking","thinking":"visible thought","signature":"signed+/=="},
            {"type":"redacted_thinking","data":OPAQUE_A},
            {"type":"tool_use","id":"tool-a","name":"echo","input":{"n":1}},
            {"type":"redacted_thinking","data":OPAQUE_B},
            {"type":"text","text":"suffix"},
            {"type":"tool_use","id":"tool-b","name":"echo","input":{"n":2}}
        ]})
    );
    assert_eq!(
        second["messages"][2],
        json!({"role":"user","content":[
            {"type":"tool_result","tool_use_id":"tool-a","content":[{"type":"text","text":"{\"n\":1}"}]},
            {"type":"tool_result","tool_use_id":"tool-b","content":[{"type":"text","text":"{\"n\":2}"}]}
        ]})
    );
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    let captured = capture.lock().unwrap();
    assert_eq!(captured.text.concat(), "prefixsuffix");
    assert_eq!(captured.thought.concat(), "visible thought");
    assert!(captured
        .checkpoint_redactions
        .iter()
        .any(|data| data == OPAQUE_A));
    assert!(captured
        .checkpoint_redactions
        .iter()
        .any(|data| data == OPAQUE_B));
}

#[derive(Clone, Copy)]
enum Retirement {
    Interrupt,
    Cancel,
}
#[tokio::test]
async fn redacted_only_completed_response_is_committed_without_public_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let shared = SharedContext::new(Context::new(vec![Message::text(Role::User, "question")]));
    let effects = Arc::new(AtomicUsize::new(0));
    let capture = Arc::new(Mutex::new(Payloads::default()));
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut stopped = assistant.subscribe_stopped(Default::default()).unwrap();
    let mut worker = Pipeline::new(vec![
        llm(endpoint, shared.snapshot().unwrap()).unwrap(),
        Arc::new(Capture(capture.clone())),
        tools(effects.clone()).unwrap(),
        assistant,
    ])
    .start(Default::default())
    .await
    .unwrap();
    let mut peer = tokio::spawn(async move {
        let mut first = accept(&listener).await?;
        request(&mut first).await?;
        respond(&mut first, &[
            json!({"type":"message_start","message":{}}),
            json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":OPAQUE_A}}),
            json!({"type":"content_block_stop","index":0}),
            json!({"type":"message_stop"}),
        ]).await?;
        drop(first);
        let mut second = accept(&listener).await?;
        let captured = request(&mut second).await?;
        respond(&mut second, &empty_response()).await?;
        Ok(captured)
    });
    let flow: Check<()> = async {
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .map_err(error)?;
        // The aggregator consumes ResponseEnd. Its caller-owned stopped receipt
        // is published after reconciliation, including for an empty text turn.
        let record = tokio::time::timeout(WAIT, stopped.recv())
            .await
            .map_err(|_| "redacted-only committed-turn receipt timeout".to_string())?
            .ok_or_else(|| "assistant stopped stream closed before completion".to_string())?;
        if !record.completion().text().is_empty() || record.completion().interrupted() {
            return Err("unexpected redacted-only completion receipt".into());
        }
        let expected = Context::new(vec![
            Message::text(Role::User, "question"),
            redacted_message(Role::Assistant, OPAQUE_A.into()),
        ]);
        if shared.snapshot().map_err(error)? != expected {
            return Err("committed redacted-only context differs from exact opaque history".into());
        }
        drop(record);
        worker
            .sender()
            .send(
                Frame::new(shared.frame_data().map_err(error)?),
                Direction::Downstream,
            )
            .await
            .map_err(error)
    }
    .await;
    if flow.is_err() {
        let _ = worker.cancel("redacted-only fixture failure cleanup").await;
        peer.abort();
    }
    let exchange = join_peer(&mut peer).await;
    let joined = finish(&mut worker).await;
    flow.unwrap();
    let body = exchange.unwrap();
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    assert_eq!(body["messages"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["messages"][1],
        json!({"role":"assistant","content":[{"type":"redacted_thinking","data":OPAQUE_A}]})
    );
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let captured = capture.lock().unwrap();
    assert!(captured.text.is_empty());
    assert!(captured.thought.is_empty());
    assert_eq!(captured.checkpoint_redactions, [OPAQUE_A]);
}

#[tokio::test]
async fn interruption_retains_closed_redaction_and_heard_prefix_only() {
    retirement(Retirement::Interrupt, true).await;
}
#[tokio::test]
async fn interruption_discards_unclosed_redaction_without_inventing_history() {
    retirement(Retirement::Interrupt, false).await;
}
#[tokio::test]
async fn cancellation_retains_closed_redaction_into_fresh_owned_pipeline() {
    retirement(Retirement::Cancel, true).await;
}
#[tokio::test]
async fn cancellation_discards_unclosed_redaction_into_fresh_owned_pipeline() {
    retirement(Retirement::Cancel, false).await;
}

async fn retirement(mode: Retirement, closed: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}/v1/messages", listener.local_addr().unwrap());
    let shared = SharedContext::new(Context::new(vec![Message::text(Role::User, "question")]));
    let effects = Arc::new(AtomicUsize::new(0));
    let capture = Arc::new(Mutex::new(Payloads::default()));
    let ready = Arc::new(tokio::sync::Semaphore::new(0));
    let mut worker = start_worker(
        endpoint.clone(),
        shared.clone(),
        effects.clone(),
        capture.clone(),
        Some(ready.clone()),
    )
    .await
    .unwrap();
    let (closed_tx, closed_rx) = tokio::sync::oneshot::channel();
    let mut peer = tokio::spawn(async move {
        let mut first = accept(&listener).await?;
        let _original = request(&mut first).await?;
        let mut events = vec![json!({"type":"message_start","message":{}})];
        if closed {
            events.extend([
                json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":OPAQUE_A}}),
                json!({"type":"content_block_stop","index":0}),
                json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"heard prefix"}}),
                json!({"type":"content_block_stop","index":1}),
                json!({"type":"content_block_start","index":2,"content_block":{"type":"tool_use","id":"never-run","name":"echo","input":{}}}),
                json!({"type":"content_block_stop","index":2}),
            ]);
        }
        let index = if closed { 3 } else { 0 };
        events.extend([
            json!({"type":"content_block_start","index":index,"content_block":{"type":"redacted_thinking","data":UNFINISHED}}),
            json!({"type":"content_block_start","index":index+1,"content_block":{"type":"thinking","thinking":PROGRESS}}),
            json!({"type":"content_block_delta","index":index+1,"delta":{"type":"signature_delta","signature":"unfinished-signature"}}),
        ]);
        let body: String = events
            .iter()
            .map(|event| format!("data: {event}\n\n"))
            .collect();
        tokio::time::timeout(WAIT, async {
            first.write_all(b"HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ntransfer-encoding: chunked\r\nconnection: close\r\n\r\n").await.map_err(error)?;
            first.write_all(format!("{:x}\r\n{}\r\n", body.len(), body).as_bytes()).await.map_err(error)
        }).await.map_err(|_| "partial SSE write timeout".to_string())??;
        let mut byte = [0];
        let count = tokio::time::timeout(WAIT, first.read(&mut byte))
            .await
            .map_err(|_| "cancelled HTTP did not close".to_string())?
            .map_err(error)?;
        if count != 0 {
            return Err("cancelled HTTP emitted unexpected bytes".into());
        }
        drop(first);
        closed_tx
            .send(())
            .map_err(|_| "socket-closure receiver dropped".to_string())?;
        let mut second = accept(&listener).await?;
        let captured = request(&mut second).await?;
        respond(&mut second, &empty_response()).await?;
        Ok(captured)
    });
    let mut replacement = None;
    let mut first_report = None;
    let mut first_joined = false;
    let flow: Check<()> = async {
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .map_err(error)?;
        tokio::time::timeout(WAIT, ready.acquire())
            .await
            .map_err(|_| "partial block did not reach assistant".to_string())?
            .map_err(error)?
            .forget();
        match mode {
            Retirement::Interrupt => {
                worker
                    .sender()
                    .send(Frame::interruption(), Direction::Downstream)
                    .await
                    .map_err(error)?;
                tokio::time::timeout(WAIT, async {
                    loop {
                        match worker.recv().await {
                            Some(frame) if matches!(frame.frame.data, FrameData::Interruption) => {
                                return Ok(())
                            }
                            Some(_) => {}
                            None => {
                                return Err("worker ended before interruption boundary".to_string())
                            }
                        }
                    }
                })
                .await
                .map_err(|_| "interruption boundary timeout".to_string())??;
            }
            Retirement::Cancel => {
                worker
                    .cancel("redacted history fixture cancellation")
                    .await
                    .map_err(error)?;
                let report = worker.join().await.map_err(error);
                first_joined = true;
                first_report = Some(report?);
            }
        }
        tokio::time::timeout(WAIT, closed_rx)
            .await
            .map_err(|_| "HTTP cancellation acknowledgement timeout".to_string())?
            .map_err(error)?;
        let context = Frame::new(shared.frame_data().map_err(error)?);
        match mode {
            Retirement::Interrupt => worker
                .sender()
                .send(context, Direction::Downstream)
                .await
                .map_err(error)?,
            Retirement::Cancel => {
                let next = start_worker(
                    endpoint,
                    shared.clone(),
                    effects.clone(),
                    capture.clone(),
                    None,
                )
                .await?;
                replacement = Some(next);
                replacement
                    .as_ref()
                    .ok_or_else(|| "replacement owner absent".to_string())?
                    .sender()
                    .send(context, Direction::Downstream)
                    .await
                    .map_err(error)?;
            }
        }
        Ok(())
    }
    .await;
    if flow.is_err() {
        if !first_joined {
            let _ = worker.cancel("fixture failure cleanup").await;
        }
        if let Some(next) = replacement.as_mut() {
            let _ = next.cancel("fixture failure cleanup").await;
        }
        peer.abort();
    }
    let exchange = join_peer(&mut peer).await;
    let original_join = if first_joined {
        Ok(first_report)
    } else {
        finish(&mut worker).await.map(Some)
    };
    let replacement_join = match replacement.as_mut() {
        Some(next) => finish(next).await.map(Some),
        None => Ok(None),
    };
    // Both pipeline lifetimes, their child/observer owners, and HTTP are joined.
    flow.unwrap();
    let body = exchange.unwrap();
    let report = original_join.unwrap().unwrap();
    assert_eq!(
        report.state,
        if matches!(mode, Retirement::Cancel) {
            WorkerState::Cancelled
        } else {
            WorkerState::Ended
        }
    );
    assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    if let Some(report) = replacement_join.unwrap() {
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0, "{:?}", report.errors);
    }
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(!body.to_string().contains(UNFINISHED));
    assert!(!body.to_string().contains("unfinished-signature"));
    assert!(!body.to_string().contains("never-run"));
    if closed {
        assert_eq!(body["messages"].as_array().unwrap().len(), 2);
        assert_eq!(
            body["messages"][1],
            json!({"role":"assistant","content":[
                {"type":"redacted_thinking","data":OPAQUE_A},
                {"type":"text","text":"heard prefix"}
            ]})
        );
    } else {
        assert_eq!(body["messages"].as_array().unwrap().len(), 1);
    }
    let captured = capture.lock().unwrap();
    assert_eq!(
        captured.text.concat(),
        if closed { "heard prefix" } else { "" }
    );
    assert_eq!(captured.thought.concat(), PROGRESS);
    assert!(!captured
        .checkpoint_redactions
        .iter()
        .any(|value| value == UNFINISHED));
    assert_eq!(
        captured
            .checkpoint_redactions
            .iter()
            .any(|value| value == OPAQUE_A),
        closed
    );
}

fn redacted_message(role: Role, data: String) -> Message {
    let mut message = Message::text(role, "");
    message.content = Some(Content::Parts(vec![ContentPart::RedactedThinking { data }]));
    message
}
fn body(provider: Provider, context: Context) -> Result<Value, pipecat_services::ServiceError> {
    LlmClient::new(ClientConfig::new(provider, "synthetic-local-fixture-key"))?
        .request_body(&CompletionRequest::new("fixture", context))
}
fn decode(
    decoder: &mut ProtocolDecoder,
    event: Value,
) -> Result<Vec<StreamEvent>, pipecat_services::ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: event.to_string(),
    })
}

#[test]
fn typed_redacted_parts_round_trip_without_decoding_or_debug_payloads() {
    let part = ContentPart::RedactedThinking {
        data: OPAQUE_A.into(),
    };
    let core = MessagePart::RedactedThinking {
        data: OPAQUE_A.into(),
    };
    let json = json!({"type":"redacted_thinking","data":OPAQUE_A});
    assert_eq!(serde_json::to_value(&part).unwrap(), json);
    assert_eq!(
        serde_json::from_value::<ContentPart>(json.clone()).unwrap(),
        part
    );
    assert_eq!(serde_json::to_value(&core).unwrap(), json);
    assert_eq!(serde_json::from_value::<MessagePart>(json).unwrap(), core);
    let original = redacted_message(Role::Assistant, OPAQUE_A.into());
    let converted = frame_context::to_core_message(&original);
    assert_eq!(converted.content, MessageContent::Parts(vec![core.clone()]));
    assert_eq!(frame_context::message(&converted).unwrap(), original);
    let shared = SharedContext::new(Context::new(vec![original.clone()]));
    let rich = shared.snapshot_rich().unwrap();
    assert_eq!(rich.messages[0], converted);
    assert_eq!(
        frame_context::context(&rich).unwrap(),
        Context::new(vec![original.clone()])
    );
    assert_eq!(
        SharedContext::new_rich(rich).snapshot().unwrap(),
        Context::new(vec![original])
    );
    let block = LlmContinuationBlock::RedactedThinking(OPAQUE_A.into());
    let checkpoint = NativeLlmContinuation::checkpoint(
        17,
        LlmContinuationProvider::Anthropic,
        vec![block.clone()],
    )
    .unwrap();
    assert!(checkpoint.is_checkpoint());
    assert_eq!(checkpoint.blocks(), std::slice::from_ref(&block));
    for debug in [
        format!("{part:?}"),
        format!("{core:?}"),
        format!("{block:?}"),
        format!("{checkpoint:?}"),
    ] {
        assert!(!debug.contains("opaque:not-base64"));
        assert!(!debug.contains("quoted"));
    }
    let request = CompletionRequest::new(
        "fixture",
        Context::new(vec![redacted_message(Role::Assistant, OPAQUE_A.into())]),
    );
    let logged =
        pipecat_services::adapters::request_body_for_logging(Provider::Anthropic, &request)
            .unwrap();
    assert_eq!(logged["messages"][0]["content"][0]["data"], "[redacted]");
    assert!(!logged.to_string().contains("opaque:not-base64"));
    assert_eq!(
        body(Provider::Anthropic, request.context).unwrap()["messages"][0]["content"],
        json!([{"type":"redacted_thinking","data":OPAQUE_A}])
    );
}

#[test]
fn redacted_invalid_fields_budget_and_provider_boundaries_fail_closed() {
    for invalid in [
        json!({"type":"redacted_thinking"}),
        json!({"type":"redacted_thinking","data":null}),
        json!({"type":"redacted_thinking","data":4}),
        json!({"type":"redacted_thinking","data":false}),
        json!({"type":"redacted_thinking","data":[]}),
        json!({"type":"redacted_thinking","data":{}}),
    ] {
        assert!(serde_json::from_value::<ContentPart>(invalid.clone()).is_err());
        assert!(serde_json::from_value::<MessagePart>(invalid.clone()).is_err());
        let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
        assert!(decode(
            &mut decoder,
            json!({"type":"content_block_start","index":0,"content_block":invalid})
        )
        .is_err());
        assert!(decoder.end_of_stream().is_err());
    }
    for role in [Role::User, Role::System, Role::Developer, Role::Tool] {
        assert!(body(
            Provider::Anthropic,
            Context::new(vec![redacted_message(role, OPAQUE_A.into())])
        )
        .is_err());
    }
    assert!(
        Context::new(vec![redacted_message(Role::Assistant, String::new())])
            .validate()
            .is_err()
    );
    assert!(NativeLlmContinuation::new(
        1,
        LlmContinuationProvider::Anthropic,
        vec![LlmContinuationBlock::RedactedThinking(String::new())]
    )
    .is_err());
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    assert!(decode(&mut decoder, json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":""}})).is_err());
    assert!(decoder.end_of_stream().is_err());
    let limit = pipecat_services::protocol::MAX_HISTORY_BYTES;
    // Context charges string length; the native carrier additionally charges
    // allocation capacity, block containers, and its own retained overhead.
    assert!(
        Context::new(vec![redacted_message(Role::Assistant, "x".repeat(limit))])
            .validate()
            .is_ok()
    );
    assert!(Context::new(vec![redacted_message(
        Role::Assistant,
        "x".repeat(limit + 1)
    )])
    .validate()
    .is_err());
    let mut thinking = Message::text(Role::Assistant, "");
    thinking.content = Some(Content::Parts(vec![ContentPart::Thinking {
        text: "t".repeat(limit / 2),
        signature: Some("s".into()),
    }]));
    assert!(Context::new(vec![
        redacted_message(Role::Assistant, "x".repeat(limit / 2)),
        thinking
    ])
    .validate()
    .is_err());
    assert!(NativeLlmContinuation::new(
        1,
        LlmContinuationProvider::Anthropic,
        vec![LlmContinuationBlock::RedactedThinking(
            "x".repeat(NativeLlmContinuation::MAX_RETAINED_BYTES)
        )]
    )
    .is_err());
    let mut spare = String::with_capacity(NativeLlmContinuation::MAX_RETAINED_BYTES + 1);
    spare.push('x');
    assert!(NativeLlmContinuation::new(
        1,
        LlmContinuationProvider::Anthropic,
        vec![LlmContinuationBlock::RedactedThinking(spare)]
    )
    .is_err());
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    assert!(decode(&mut decoder, json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"x".repeat(limit + 1)}})).is_err());
    assert!(decoder.end_of_stream().is_err());
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    decode(&mut decoder, json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":"r".repeat(limit / 2)}})).unwrap();
    // This already-closed prefix remains legal; only the overflowing next
    // block is rejected. A later error must not retroactively erase it.
    let closed_prefix =
        decode(&mut decoder, json!({"type":"content_block_stop","index":0})).unwrap();
    assert!(closed_prefix
        .iter()
        .any(|event| matches!(event, StreamEvent::AnthropicCheckpoint(_))));
    assert!(decode(&mut decoder, json!({"type":"content_block_start","index":1,"content_block":{"type":"thinking","thinking":"t".repeat(limit / 2),"signature":"s"}})).is_err());
    assert!(decoder.end_of_stream().is_err());
    for provider in [Provider::OpenAi, Provider::Gemini] {
        assert!(body(
            provider,
            Context::new(vec![redacted_message(Role::Assistant, OPAQUE_A.into())])
        )
        .is_err());
    }
    for provider in [
        LlmContinuationProvider::Gemini,
        LlmContinuationProvider::AwsBedrock,
    ] {
        assert!(NativeLlmContinuation::new(
            1,
            provider,
            vec![LlmContinuationBlock::RedactedThinking(OPAQUE_A.into())]
        )
        .is_err());
    }
    // A redacted block cannot accept public text/thinking/signature mutations.
    for delta in [
        json!({"type":"text_delta","text":"leak"}),
        json!({"type":"thinking_delta","thinking":"leak"}),
        json!({"type":"signature_delta","signature":"invented"}),
    ] {
        let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
        decode(&mut decoder, json!({"type":"content_block_start","index":0,"content_block":{"type":"redacted_thinking","data":OPAQUE_A}})).unwrap();
        assert!(decode(
            &mut decoder,
            json!({"type":"content_block_delta","index":0,"delta":delta})
        )
        .is_err());
        assert!(decoder.end_of_stream().is_err());
    }
    let start = |index| json!({"type":"content_block_start","index":index,"content_block":{"type":"redacted_thinking","data":OPAQUE_A}});
    let stop = |index| json!({"type":"content_block_stop","index":index});
    for (prefix, rejected) in [
        (vec![start(0)], start(0)),
        (vec![start(0), stop(0)], stop(0)),
        (vec![start(1), stop(1)], json!({"type":"message_stop"})),
        (vec![start(0)], json!({"type":"message_stop"})),
    ] {
        let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
        for event in prefix {
            decode(&mut decoder, event).unwrap();
        }
        assert!(decode(&mut decoder, rejected).is_err());
        assert!(decoder.end_of_stream().is_err());
    }
}
