//! Real unsigned HTTP/SSE completion filtering through the standard tool owner.
//! Initial inference is caller-admitted from an already committed question. The
//! second request is exclusively the native assistant's tool-result continuation.
//! No provider account, speculative admission, or signed-history rewriting claim.
// Retain these existing cases verbatim, including their older helper contracts.
#[path = "llm_completion.rs"]
mod existing_completion;
#[path = "review_llm_completion.rs"]
mod existing_completion_review;
#[path = "llm_completion_runtime.rs"]
mod existing_completion_runtime;

use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolOutcomeSnapshot};
use pipecat_services::{
    ClientConfig, CompletionRequest, Content, Context, LlmClient, LlmProcessor, Message, Provider,
    Role, ToolDefinition,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    fmt::Display,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{watch, Notify, Semaphore},
    task::JoinHandle,
};

const WAIT: Duration = Duration::from_secs(5);
const MAX_HTTP: usize = 64 * 1024;
const MAX_OBSERVATIONS: usize = 128;
type Check<T> = Result<T, String>;
fn error(e: impl Display) -> String {
    e.to_string()
}

#[derive(Clone, Copy, Debug)]
enum Case {
    Filler,
    Burst,
    ToolOnly,
    Tail,
    Short,
    Long,
    CancelStream,
    CancelTool,
}
impl Case {
    fn cancelled(self) -> bool {
        matches!(self, Self::CancelStream | Self::CancelTool)
    }
    fn first_text(self) -> Option<&'static str> {
        match self {
            Self::Filler | Self::Burst | Self::Tail => Some("● filler"),
            Self::Short | Self::CancelStream => Some("◐ hidden"),
            Self::Long => Some("○ hidden"),
            _ => None,
        }
    }
    fn visible_first(self) -> &'static str {
        match self {
            Self::Filler | Self::Burst => "filler",
            Self::Tail => "filler tail",
            _ => "",
        }
    }
}
#[derive(Default)]
struct Seen {
    delivered: Vec<FrameData>,
    settled_ends: usize,
    settled_user_starts: usize,
    settled_user_stops: usize,
    settled_vad_stops: usize,
    gate_entered: bool,
    callback_entered: usize,
    callback_returned: usize,
    callback_dropped: usize,
    requests: Vec<Value>,
    partial_tool_sent: bool,
    peer_eof: bool,
}
#[derive(Default)]
struct Trace {
    seen: Mutex<Seen>,
    changed: Notify,
}
impl Trace {
    fn update(&self, f: impl FnOnce(&mut Seen)) -> Check<()> {
        let mut seen = self.seen.lock().map_err(error)?;
        f(&mut seen);
        drop(seen);
        self.changed.notify_waiters();
        Ok(())
    }
    fn has(&self, f: impl FnOnce(&Seen) -> bool) -> Check<bool> {
        let seen = self.seen.lock().map_err(error)?;
        Ok(f(&seen))
    }
    async fn wait(&self, f: impl Fn(&Seen) -> bool) -> Check<()> {
        tokio::time::timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if self.has(&f)? {
                    return Ok(());
                }
                changed.await;
            }
        })
        .await
        .map_err(|_| "fixture causal receipt timeout".to_string())?
    }
}
fn delivered_text(s: &Seen) -> String {
    s.delivered
        .iter()
        .filter_map(|f| match f {
            FrameData::LlmText(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect()
}
fn started(s: &Seen) -> usize {
    s.delivered
        .iter()
        .filter(|f| matches!(f, FrameData::FunctionCallsStarted(c) if !c.is_empty()))
        .count()
}
fn completions(s: &Seen) -> usize {
    s.delivered
        .iter()
        .filter(|f| matches!(f, FrameData::UserTurnInferenceCompleted))
        .count()
}
fn marker_seen(s: &Seen) -> bool {
    s.delivered
        .iter()
        .any(|f| matches!(f, FrameData::LlmMarker(_)))
}

struct NormalGate {
    case: Case,
    used: AtomicBool,
    release: Arc<Semaphore>,
    trace: Arc<Trace>,
}
#[async_trait]
impl FrameProcessor for NormalGate {
    fn name(&self) -> &str {
        "CompletionToolNormalGate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let matches = direction == Direction::Downstream
            && match self.case {
                Case::Burst => matches!(frame.data, FrameData::LlmResponseStart),
                Case::Tail => matches!(&frame.data, FrameData::LlmText(t) if t.text == " tail"),
                _ => false,
            };
        if matches && !self.used.swap(true, Ordering::SeqCst) {
            self.trace
                .update(|s| s.gate_entered = true)
                .map_err(ProcessorError::new)?;
            tokio::time::timeout(WAIT, self.release.acquire())
                .await
                .map_err(|_| ProcessorError::new("normal gate timeout"))?
                .map_err(|e| ProcessorError::new(e.to_string()))?
                .forget();
        }
        ctx.push(frame, direction).await
    }
}
struct DeliveryTap(Arc<Trace>);
#[async_trait]
impl FrameProcessor for DeliveryTap {
    fn name(&self) -> &str {
        "CompletionToolDeliveryTap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::LlmText(_)
                    | FrameData::LlmMarker(_)
                    | FrameData::LlmMarkerResponse(_)
                    | FrameData::FunctionCallsStarted(_)
                    | FrameData::UserTurnInferenceCompleted
            )
        {
            let mut seen = self
                .0
                .seen
                .lock()
                .map_err(|e| ProcessorError::new(e.to_string()))?;
            if seen.delivered.len() >= MAX_OBSERVATIONS {
                return Err(ProcessorError::new("observation bound"));
            }
            seen.delivered.push(frame.data.clone());
            drop(seen);
            self.0.changed.notify_waiters();
        }
        ctx.push(frame, direction).await
    }
}
/// Receipt follows the real assistant's completed process invocation, not its
/// observer Process-entry event or an assumed root ResponseEnd forwarding.
struct AssistantReceipt {
    inner: Arc<LLMAssistantAggregator>,
    trace: Arc<Trace>,
}
#[async_trait]
impl FrameProcessor for AssistantReceipt {
    fn name(&self) -> &str {
        "CompletionToolAssistantReceipt"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let end =
            direction == Direction::Downstream && matches!(frame.data, FrameData::LlmResponseEnd);
        let start = direction == Direction::Downstream
            && matches!(frame.data, FrameData::UserStartedSpeaking);
        let stop = direction == Direction::Downstream
            && matches!(frame.data, FrameData::UserStoppedSpeaking);
        let vad_stop = direction == Direction::Downstream
            && matches!(frame.data, FrameData::VadUserStoppedSpeakingDetailed(_));
        self.inner.process(frame, direction, ctx).await?;
        self.trace
            .update(|s| {
                s.settled_ends += usize::from(end);
                s.settled_user_starts += usize::from(start);
                s.settled_user_stops += usize::from(stop);
                s.settled_vad_stops += usize::from(vad_stop);
            })
            .map_err(ProcessorError::new)
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
struct CallbackDrop(Arc<Trace>);
impl Drop for CallbackDrop {
    fn drop(&mut self) {
        let _ = self.0.update(|s| s.callback_dropped += 1);
    }
}

async fn request(socket: &mut TcpStream) -> Check<Value> {
    tokio::time::timeout(WAIT, async {
        let mut bytes = Vec::new();
        let mut buffer = [0u8; 2048];
        let end = loop {
            let n = socket.read(&mut buffer).await.map_err(error)?;
            if n == 0 {
                return Err("EOF before HTTP header".into());
            }
            bytes.extend_from_slice(&buffer[..n]);
            if bytes.len() > MAX_HTTP {
                return Err("HTTP fixture input bound".into());
            }
            if let Some(p) = bytes.windows(4).position(|x| x == b"\r\n\r\n") {
                break p + 4;
            }
        };
        let length: usize = std::str::from_utf8(&bytes[..end])
            .map_err(error)?
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|s| s.trim().to_owned())
            })
            .ok_or("missing content-length")?
            .parse()
            .map_err(error)?;
        if length > MAX_HTTP - end {
            return Err("HTTP fixture body bound".into());
        }
        while bytes.len() < end + length {
            let room = (end + length - bytes.len()).min(buffer.len());
            let n = socket.read(&mut buffer[..room]).await.map_err(error)?;
            if n == 0 {
                return Err("EOF before HTTP body".into());
            }
            bytes.extend_from_slice(&buffer[..n]);
        }
        serde_json::from_slice(&bytes[end..end + length]).map_err(error)
    })
    .await
    .map_err(|_| "HTTP read timeout".to_string())?
}
async fn write(socket: &mut TcpStream, bytes: &[u8]) -> Check<()> {
    tokio::time::timeout(WAIT, socket.write_all(bytes))
        .await
        .map_err(|_| "HTTP write timeout".to_string())?
        .map_err(error)
}
async fn chunk(socket: &mut TcpStream, body: &str) -> Check<()> {
    if body.len() > MAX_HTTP {
        return Err("SSE fixture bound".into());
    }
    write(socket, format!("{:x}\r\n{body}\r\n", body.len()).as_bytes()).await
}
fn event(value: Value) -> String {
    format!("data: {value}\n\n")
}
fn words(value: &str) -> String {
    event(json!({"choices":[{"delta":{"content":value},"finish_reason":null}]}))
}
fn tool_delta() -> String {
    event(
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"tool-99","type":"function","function":{"name":"echo","arguments":"{\"value\":99}"}}]},"finish_reason":null}]}),
    )
}
fn done(reason: &str) -> String {
    format!(
        "{}data: [DONE]\n\n",
        event(json!({"choices":[{"delta":{},"finish_reason":reason}]}))
    )
}
async fn serve(
    listener: TcpListener,
    case: Case,
    trace: Arc<Trace>,
    mut stop: watch::Receiver<bool>,
) -> Check<()> {
    let operation = async {
        loop {
            let (mut socket, _) = listener.accept().await.map_err(error)?;
            let body = request(&mut socket).await?;
            let n = {
                let mut seen = trace.seen.lock().map_err(error)?;
                if seen.requests.len() >= 3 {
                    return Err("unbounded fixture requests".into());
                }
                seen.requests.push(body);
                seen.requests.len()
            };
            trace.changed.notify_waiters();
            if n > 2 || (n > 1 && case.cancelled()) {
                return Err("unexpected extra HTTP inference".into());
            }
            write(&mut socket,b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await?;
            if n == 2 {
                chunk(
                    &mut socket,
                    &format!("{}{}", words("● final answer"), done("stop")),
                )
                .await?;
                write(&mut socket, b"0\r\n\r\n").await?;
                continue;
            }
            if let Some(text) = case.first_text() {
                chunk(&mut socket, &words(text)).await?;
                if !matches!(case, Case::Burst) {
                    trace
                        .wait(|s| {
                            if matches!(case, Case::Short | Case::Long | Case::CancelStream) {
                                marker_seen(s)
                            } else {
                                delivered_text(s) == "filler"
                            }
                        })
                        .await?;
                }
            }
            if matches!(case, Case::Tail) {
                chunk(&mut socket, &words(" tail")).await?;
                trace.wait(|s| s.gate_entered).await?;
            }
            if matches!(case, Case::CancelStream) {
                chunk(&mut socket, &tool_delta()).await?;
                trace.update(|s| s.partial_tool_sent = true)?;
                let mut byte = [0];
                let n = tokio::time::timeout(WAIT, socket.read(&mut byte))
                    .await
                    .map_err(|_| "cancelled provider EOF timeout".to_string())?
                    .map_err(error)?;
                if n != 0 {
                    return Err("cancelled provider did not close".into());
                }
                trace.update(|s| s.peer_eof = true)?;
            } else {
                chunk(
                    &mut socket,
                    &format!("{}{}", tool_delta(), done("tool_calls")),
                )
                .await?;
                write(&mut socket, b"0\r\n\r\n").await?;
            }
        }
    };
    tokio::select! { result=operation=>result, _=stop.changed()=>Ok(()) }
}
async fn send(worker: &PipelineWorker, data: FrameData) -> Check<()> {
    tokio::time::timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream),
    )
    .await
    .map_err(|_| "frame admission timeout".to_string())?
    .map_err(error)
}
async fn until(worker: &mut PipelineWorker, trace: &Trace, f: impl Fn(&Seen) -> bool) -> Check<()> {
    tokio::time::timeout(WAIT,async {
        loop {
            let changed=trace.changed.notified();tokio::pin!(changed);changed.as_mut().enable();
            if trace.has(&f)? {return Ok(());}
            tokio::select! {
                _=&mut changed=>{},
                out=worker.recv()=>{
                    let out=out.ok_or("pipeline ended before causal receipt")?;
                    if let FrameData::Error(e)=out.frame.data {return Err(format!("pipeline error: {e:?}"));}
                }
            }
        }
    }).await.map_err(|_|"pipeline causal receipt timeout".to_string())?
}
async fn join_peer(peer: &mut JoinHandle<Check<()>>) -> Check<()> {
    match tokio::time::timeout(WAIT, &mut *peer).await {
        Ok(result) => result.map_err(error)?,
        Err(_) => {
            peer.abort();
            let _ = peer.await;
            Err("HTTP peer timeout; aborted owner actually joined".into())
        }
    }
}
async fn join_worker(worker: &mut PipelineWorker, cancel: bool) -> Check<WorkerReport> {
    let terminal = if worker.has_finished() {
        Ok(())
    } else if cancel {
        tokio::time::timeout(WAIT, worker.cancel("completion tool fixture cleanup"))
            .await
            .map_err(|_| "cancel admission timeout".to_string())
            .and_then(|r| r.map_err(error))
    } else {
        tokio::time::timeout(WAIT, worker.end())
            .await
            .map_err(|_| "end admission timeout".to_string())
            .and_then(|r| r.map_err(error))
    };
    if terminal.is_err() {
        let _ = worker.cancel("terminal admission failed").await;
    }
    let joined = match tokio::time::timeout(WAIT, worker.join()).await {
        Ok(result) => result.map_err(error),
        Err(_) => {
            let _ = worker.cancel("join deadline").await;
            let result = worker.join().await;
            result
                .map_err(error)
                .and_then(|_| Err("worker join deadline; owner joined after cancellation".into()))
        }
    };
    terminal?;
    joined
}

struct Outcome {
    seen: Seen,
    history: Context,
    journal: ToolOutcomeSnapshot,
    instructions: String,
}
async fn run(case: Case) -> Outcome {
    let trace = Arc::new(Trace::default());
    let normal_release = Arc::new(Semaphore::new(0));
    let tool_release = Arc::new(Semaphore::new(0));
    let config = LLMTurnCompletionConfig::default();
    let instructions = config.completion_instructions();
    let mut context = Context::new(vec![
        Message::text(Role::System, &instructions),
        Message::text(Role::User, "already committed question"),
    ]);
    context.tools = Some(vec![ToolDefinition {
        name: "echo".into(),
        description: "bounded fixture echo".into(),
        parameters: json!({"type":"object","properties":{"value":{"type":"integer"}},"required":["value"]}),
    }]);
    let pair = LLMContextAggregatorPair::new(
        context,
        UserAggregatorConfig {
            turn: UserTurnConfig {
                enable_interruptions: false,
                stop_timeout: Duration::from_secs(30),
                stop_strategies: Some(
                    UserTurnStopStrategies::new(vec![UserTurnStopStrategy::external_completion()])
                        .unwrap(),
                ),
                ..Default::default()
            },
            empty_user_turn: None,
            ..Default::default()
        },
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Text,
        },
    );
    let shared = pair.context();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut client = ClientConfig::new(Provider::OpenAi, "synthetic-completion-tool-fixture");
    client.endpoint = format!("http://{}/completion", listener.local_addr().unwrap());
    client.request_timeout = WAIT;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(client).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    let callback_trace = trace.clone();
    let release = tool_release.clone();
    tools
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let trace = callback_trace.clone();
                let release = release.clone();
                Box::pin(async move {
                    let _drop = CallbackDrop(trace.clone());
                    trace.update(|s| s.callback_entered += 1)?;
                    release.acquire().await.map_err(error)?.forget();
                    trace.update(|s| s.callback_returned += 1)?;
                    Ok(json!({"echo":call.arguments}))
                })
            })),
        )
        .unwrap();
    let journal = tools.outcomes();
    let mut worker = Pipeline::new(vec![
        pair.user(),
        llm,
        Arc::new(NormalGate {
            case,
            used: AtomicBool::new(false),
            release: normal_release.clone(),
            trace: trace.clone(),
        }),
        Arc::new(LLMTurnCompletionProcessor::new(config).unwrap()),
        Arc::new(DeliveryTap(trace.clone())),
        Arc::new(tools),
        Arc::new(AssistantReceipt {
            inner: pair.assistant(),
            trace: trace.clone(),
        }),
    ])
    .start(PipelineConfig {
        shutdown_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .await
    .unwrap();
    // No HTTP child exists if pipeline construction fails.
    let (stop, stopped) = watch::channel(false);
    let mut peer = tokio::spawn(serve(listener, case, trace.clone(), stopped));
    let work: Check<()> = async {
        send(
            &worker,
            FrameData::VadUserStartedSpeakingDetailed(Default::default()),
        )
        .await?;
        until(&mut worker, &trace, |s| s.settled_user_starts == 1).await?;
        send(
            &worker,
            FrameData::VadUserStoppedSpeakingDetailed(Default::default()),
        )
        .await?;
        until(&mut worker, &trace, |s| s.settled_vad_stops == 1).await?;
        // The only manually admitted inference. The tool result must own request 2.
        send(&worker, FrameData::LlmRun).await?;
        if matches!(case, Case::CancelStream) {
            until(&mut worker, &trace, |s| {
                marker_seen(s) && s.partial_tool_sent
            })
            .await?;
            tokio::time::timeout(WAIT, worker.cancel("unfinished provider response"))
                .await
                .map_err(error)?
                .map_err(error)?;
            trace.wait(|s| s.peer_eof).await?;
            return Ok(());
        }
        if matches!(case, Case::Burst | Case::Tail) {
            until(&mut worker, &trace, |s| s.gate_entered && started(s) == 1).await?;
            normal_release.add_permits(1);
        }
        until(&mut worker, &trace, |s| {
            s.callback_entered == 1 && s.settled_user_stops == 1 && s.settled_ends == 1
        })
        .await?;
        if matches!(case, Case::CancelTool) {
            tokio::time::timeout(WAIT, worker.cancel("cancel an entered tool callback"))
                .await
                .map_err(error)?
                .map_err(error)?;
        } else {
            tool_release.add_permits(1);
            until(&mut worker, &trace, |s| {
                s.requests.len() == 2 && s.settled_ends == 2 && s.callback_dropped == 1
            })
            .await?;
        }
        Ok(())
    }
    .await;
    // Do not assert, return an observation error, or detach an owner before both
    // the pipeline (including callbacks/provider reader) and HTTP child are joined.
    let joined = join_worker(&mut worker, case.cancelled() || work.is_err()).await;
    let _ = stop.send(true);
    let peer_result = join_peer(&mut peer).await;
    let history = shared.snapshot();
    let outcome = journal.snapshot();
    work.unwrap();
    let report = joined.unwrap();
    peer_result.unwrap();
    assert_eq!(
        report.state,
        if case.cancelled() {
            WorkerState::Cancelled
        } else {
            WorkerState::Ended
        },
        "{case:?}: {report:?}"
    );
    assert_eq!(report.total_errors, 0, "{case:?}: {:?}", report.errors);
    let seen = std::mem::take(&mut *trace.seen.lock().unwrap());
    Outcome {
        seen,
        history: history.unwrap(),
        journal: outcome,
        instructions,
    }
}
fn assert_success(case: Case, out: &Outcome) {
    assert_eq!(out.seen.requests.len(), 2, "{case:?}");
    assert_eq!(
        out.seen.requests[0]["messages"][0]["content"],
        out.instructions
    );
    assert_eq!(
        out.seen.requests[0]["messages"][1]["content"],
        "already committed question"
    );
    assert_eq!(out.seen.requests[0]["tools"][0]["function"]["name"], "echo");
    assert_eq!(
        delivered_text(&out.seen),
        format!("{}final answer", case.visible_first()),
        "{case:?}"
    );
    assert_eq!(started(&out.seen), 1);
    assert_eq!(completions(&out.seen), 2, "{case:?}");
    assert_eq!(out.seen.settled_user_stops, 1);
    assert_eq!(
        (
            out.seen.callback_entered,
            out.seen.callback_returned,
            out.seen.callback_dropped
        ),
        (1, 1, 1)
    );
    assert!(out.journal.active.is_empty());
    assert_eq!(out.journal.total_terminal, 1);
    assert_eq!(out.journal.unadmitted_terminal, 0);
    assert_eq!(
        out.journal.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
    let messages = out.seen.requests[1]["messages"].as_array().unwrap();
    let calls: Vec<_> = messages
        .iter()
        .flat_map(|m| m["tool_calls"].as_array().into_iter().flatten())
        .collect();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0]["id"], "tool-99");
    assert_eq!(calls[0]["function"]["name"], "echo");
    assert_eq!(
        serde_json::from_str::<Value>(calls[0]["function"]["arguments"].as_str().unwrap()).unwrap(),
        json!({"value":99})
    );
    let results: Vec<_> = messages.iter().filter(|m| m["role"] == "tool").collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0]["tool_call_id"], "tool-99");
    assert_eq!(
        serde_json::from_str::<Value>(results[0]["content"].as_str().unwrap()).unwrap(),
        json!({"echo":{"value":99}})
    );
    assert_eq!(messages.iter().filter(|m| m["role"] == "user").count(), 1);
    if let Some(text) = case.first_text() {
        let expected = if matches!(case, Case::Short) {
            "◐".to_string()
        } else if matches!(case, Case::Long) {
            "○".to_string()
        } else {
            format!(
                "{text}{}",
                if matches!(case, Case::Tail) {
                    " tail"
                } else {
                    ""
                }
            )
        };
        assert!(
            messages
                .iter()
                .any(|m| m["role"] == "assistant" && m["content"] == expected),
            "{case:?}: {messages:?}"
        );
    }
    assert!(out
        .history
        .messages
        .iter()
        .any(|m| m.role == Role::Assistant
            && m.content == Some(Content::Text("● final answer".into()))));
}

#[tokio::test]
async fn consumed_filler_then_tool_result_speaks_final_in_second_http_request() {
    let out = run(Case::Filler).await;
    assert_success(Case::Filler, &out);
}
#[tokio::test]
async fn system_tool_start_overtakes_first_response_start_without_suppressing_final() {
    let out = run(Case::Burst).await;
    assert_success(Case::Burst, &out);
    assert!(out.seen.gate_entered);
}
#[tokio::test]
async fn tool_only_response_ends_user_turn_and_continues_via_standard_executor() {
    let out = run(Case::ToolOnly).await;
    assert_success(Case::ToolOnly, &out);
}
#[tokio::test]
async fn started_between_complete_chunks_preserves_same_response_tail() {
    let out = run(Case::Tail).await;
    assert_success(Case::Tail, &out);
    assert!(out.seen.gate_entered);
}
#[tokio::test]
async fn short_and_long_tool_responses_hide_text_without_an_extra_retry_request() {
    for case in [Case::Short, Case::Long] {
        let out = run(case).await;
        assert_success(case, &out);
    }
}
#[tokio::test]
async fn cancellation_joins_unfinished_http_and_started_tool_without_continuation() {
    for case in [Case::CancelStream, Case::CancelTool] {
        let out = run(case).await;
        assert_eq!(out.seen.requests.len(), 1);
        assert_eq!(out.seen.callback_returned, 0);
        assert!(out.journal.active.is_empty());
        assert!(delivered_text(&out.seen).is_empty());
        if matches!(case, Case::CancelStream) {
            assert!(out.seen.peer_eof);
            assert_eq!(started(&out.seen), 0);
            assert_eq!(completions(&out.seen), 0);
            assert_eq!(out.seen.settled_ends, 0);
            assert_eq!(out.seen.callback_entered, 0);
            assert_eq!(out.journal.total_terminal, 0);
        } else {
            assert_eq!(out.seen.callback_entered, 1);
            assert_eq!(out.seen.callback_dropped, 1);
            assert_eq!(out.journal.total_terminal, 1);
            assert_eq!(
                out.journal.terminal[0].outcome.state(),
                ToolCallTerminalState::Cancelled
            );
        }
    }
}

fn apply(s: &mut LLMTurnCompletionStrategy, data: FrameData) -> AggregationOutput {
    s.process_frame(Frame::new(data), Direction::Downstream, Duration::ZERO)
        .unwrap()
}
fn text_output(o: &AggregationOutput) -> String {
    o.actions
        .iter()
        .filter_map(|a| match a {
            AggregationAction::Push(f) => match &f.frame.data {
                FrameData::LlmText(t) => Some(t.text.as_str()),
                _ => None,
            },
            _ => None,
        })
        .collect()
}
fn broadcasts(o: &AggregationOutput) -> usize {
    o.actions.iter().filter(|a|matches!(a,AggregationAction::Broadcast(f)if matches!(f.data,FrameData::UserTurnInferenceCompleted))).count()
}
fn calls() -> FrameData {
    FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "echo".into(),
        tool_call_id: "direct-tool".into(),
        arguments: json!({}),
    }])
}
fn detailed_calls() -> FrameData {
    FrameData::FunctionCallsStartedDetailed(vec![FunctionCallFromLlm {
        function_name: "echo".into(),
        tool_call_id: "direct-detailed-tool".into(),
        arguments: Default::default(),
        context: None,
    }])
}
async fn receive_frame(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&DirectedFrame) -> bool,
) -> Check<DirectedFrame> {
    tokio::time::timeout(WAIT, async {
        loop {
            let out = worker.recv().await.ok_or("pipeline output closed")?;
            if let FrameData::Error(e) = &out.frame.data {
                return Err(format!("pipeline error: {e:?}"));
            }
            if wanted(&out) {
                return Ok(out);
            }
        }
    })
    .await
    .map_err(|_| "pipeline frame receipt timeout".to_string())?
}
async fn queued_retry_tool_fences() {
    for (boundary, direction, stale) in [
        (calls(), Direction::Downstream, true),
        (detailed_calls(), Direction::Downstream, true),
        (
            FrameData::FunctionCallsStarted(vec![]),
            Direction::Downstream,
            false,
        ),
        (
            FrameData::FunctionCallsStartedDetailed(vec![]),
            Direction::Downstream,
            false,
        ),
        (calls(), Direction::Upstream, false),
        (detailed_calls(), Direction::Upstream, false),
    ] {
        let marker = Arc::new(LLMTurnCompletionProcessor::new(Default::default()).unwrap());
        let mut worker = Pipeline::new(vec![marker])
            .start(PipelineConfig {
                shutdown_timeout: Duration::from_secs(1),
                ..Default::default()
            })
            .await
            .unwrap();
        let result: Check<Result<Vec<ContextMessage>, ProcessorError>> = async {
            send(&worker, FrameData::LlmResponseStart).await?;
            send(&worker, FrameData::LlmText(TextFrame::llm("◐"))).await?;
            send(&worker, FrameData::LlmResponseEnd).await?;
            receive_frame(&mut worker, |f| {
                matches!(f.frame.data, FrameData::LlmResponseEnd)
            })
            .await?;
            tokio::time::advance(Duration::from_secs(6)).await;
            let queued = receive_frame(&mut worker, |f| {
                matches!(f.frame.data, FrameData::LlmMessagesTransform(_))
            })
            .await?;
            let FrameData::LlmMessagesTransform(queued) = queued.frame.data else {
                return Err("retry frame shape".into());
            };
            let frame = Frame::new(boundary);
            let id = frame.id;
            tokio::time::timeout(WAIT, worker.sender().send(frame, direction))
                .await
                .map_err(error)?
                .map_err(error)?;
            // Root delivery follows actual processor state mutation. Consume the
            // already emitted capability only after this tool boundary receipt.
            receive_frame(&mut worker, |f| {
                f.frame.id == id && f.direction == direction
            })
            .await?;
            Ok(queued.transform.apply(vec![]))
        }
        .await;
        let joined = join_worker(&mut worker, result.is_err()).await;
        // All assertions follow the real deadline-supervisor owner's join.
        let applied = result.unwrap();
        let report = joined.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0);
        if stale {
            assert!(applied.unwrap_err().to_string().contains("stale"));
        } else {
            assert_eq!(applied.unwrap().len(), 1);
        }
    }
}
#[tokio::test(start_paused = true)]
async fn tool_boundaries_preserve_direction_identity_latch_and_retire_pending_deadlines() {
    for (data, direction) in [
        (
            FrameData::FunctionCallsStarted(vec![]),
            Direction::Downstream,
        ),
        (calls(), Direction::Upstream),
        (
            FrameData::FunctionCallsStartedDetailed(vec![]),
            Direction::Downstream,
        ),
        (detailed_calls(), Direction::Upstream),
    ] {
        let mut s = LLMTurnCompletionStrategy::new(Default::default()).unwrap();
        apply(&mut s, FrameData::LlmResponseStart);
        apply(&mut s, FrameData::LlmText(TextFrame::llm("● first")));
        apply(&mut s, FrameData::LlmResponseEnd);
        let f = Frame::new(data);
        let id = f.id;
        let out = s.process_frame(f, direction, Duration::ZERO).unwrap();
        assert_eq!(broadcasts(&out), 0);
        assert!(out.actions.iter().any(
            |a| matches!(a,AggregationAction::Push(f)if f.direction==direction&&f.frame.id==id)
        ));
        apply(&mut s, FrameData::LlmResponseStart);
        assert!(
            text_output(&apply(
                &mut s,
                FrameData::LlmText(TextFrame::llm("● suppressed"))
            ))
            .is_empty(),
            "empty/upstream tool frames must not authorize another spoken completion"
        );
    }
    for data in [calls(), detailed_calls()] {
        let mut s = LLMTurnCompletionStrategy::new(Default::default()).unwrap();
        assert_eq!(broadcasts(&apply(&mut s, data)), 1);
        apply(&mut s, FrameData::LlmResponseStart);
        let out = apply(&mut s, FrameData::LlmText(TextFrame::llm("● after tool")));
        assert_eq!(text_output(&out), "after tool");
        assert_eq!(
            broadcasts(&out),
            0,
            "overtaken response start preserves the tool broadcast latch"
        );
    }
    let mut s = LLMTurnCompletionStrategy::new(Default::default()).unwrap();
    apply(&mut s, FrameData::LlmResponseStart);
    let first = apply(&mut s, FrameData::LlmText(TextFrame::llm("● first")));
    assert_eq!(broadcasts(&first), 1);
    let f = Frame::new(calls());
    let tool = s
        .process_frame(f.clone(), Direction::Downstream, Duration::ZERO)
        .unwrap();
    assert_eq!(broadcasts(&tool), 0);
    let duplicate = s
        .process_frame(f, Direction::Downstream, Duration::ZERO)
        .unwrap();
    assert_eq!(broadcasts(&duplicate), 0);
    assert_eq!(
        text_output(&apply(&mut s, FrameData::LlmText(TextFrame::llm(" tail")))),
        " tail"
    );
    apply(&mut s, FrameData::LlmResponseEnd);
    apply(&mut s, FrameData::LlmResponseStart);
    let next = apply(&mut s, FrameData::LlmText(TextFrame::llm("● next")));
    assert_eq!(text_output(&next), "next");
    assert_eq!(broadcasts(&next), 1);
    for marker in ["◐", "○"] {
        for before_text in [true, false] {
            let mut s = LLMTurnCompletionStrategy::new(Default::default()).unwrap();
            if before_text {
                assert_eq!(broadcasts(&apply(&mut s, calls())), 1);
            }
            apply(&mut s, FrameData::LlmResponseStart);
            let out = apply(
                &mut s,
                FrameData::LlmText(TextFrame::llm(format!("{marker} hidden"))),
            );
            assert!(text_output(&out).is_empty());
            if !before_text {
                assert!(s.deadline().is_some());
                assert_eq!(broadcasts(&apply(&mut s, calls())), 1);
            }
            assert!(s.deadline().is_none());
            assert!(s
                .advance(Duration::from_secs(100))
                .unwrap()
                .actions
                .is_empty());
            apply(&mut s, FrameData::LlmResponseEnd);
            assert!(s
                .advance(Duration::from_secs(200))
                .unwrap()
                .actions
                .is_empty());
            apply(&mut s, FrameData::LlmResponseStart);
            let out = apply(&mut s, FrameData::LlmText(TextFrame::llm("● final")));
            assert_eq!(text_output(&out), "final");
            assert_eq!(broadcasts(&out), 1);
        }
        for retirement in [FrameData::LlmResponseEnd, FrameData::Interruption] {
            let mut s = LLMTurnCompletionStrategy::new(Default::default()).unwrap();
            apply(&mut s, calls());
            // No text was seen: both boundaries still have to retire tool_pending.
            apply(&mut s, retirement);
            apply(&mut s, FrameData::LlmResponseStart);
            apply(&mut s, FrameData::LlmText(TextFrame::llm(marker)));
            assert!(s.deadline().is_some());
            apply(&mut s, FrameData::Cancel(Default::default()));
            assert!(s.is_closed());
            assert!(s.deadline().is_none());
            assert!(s
                .advance(Duration::from_secs(200))
                .unwrap()
                .actions
                .is_empty());
        }
    }
    queued_retry_tool_fences().await;
}
