//! Independent native receipt boundary tests with real ToolExecutor and a local synthetic peer.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_grok_realtime::{FunctionTool, GrokRealtimeProcessor, RealtimeClient, RealtimeConfig};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{Context, Message as ContextMessage, Role};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(5);

async fn read(ws: &mut Ws) -> Value {
    let frame = timeout(WAIT, ws.next())
        .await
        .expect("provider wire deadline")
        .unwrap()
        .unwrap();
    serde_json::from_str(frame.to_text().unwrap()).unwrap()
}
async fn send(ws: &mut Ws, mut value: Value) {
    value["event_id"] = json!("independent-native-receipt");
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn handshake(ws: &mut Ws) {
    send(ws, json!({"type":"session.created","session":{"id":"s"}})).await;
    send(
        ws,
        json!({"type":"conversation.created","conversation":{"id":"c"}}),
    )
    .await;
    assert_eq!(read(ws).await["type"], "session.update");
    send(ws, json!({"type":"session.updated","session":{"id":"s"}})).await;
    loop {
        let action = read(ws).await;
        match action["type"].as_str().unwrap() {
            "session.update" | "conversation.item.create" => {}
            "response.create" => break,
            other => panic!("unexpected initialization action {other}"),
        }
    }
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    send(ws, json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
async fn announce(ws: &mut Ws) {
    response(ws, "r1", false).await;
    send(ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"i1","call_id":"c1","name":"echo","arguments":""}})).await;
    send(ws, json!({"type":"response.function_call_arguments.done","call_id":"c1","name":"echo","arguments":"{\"n\":7}"})).await;
}
async fn fixture() -> (TcpListener, Arc<GrokRealtimeProcessor>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "Echo",
        json!({"type":"object"}),
    )
    .into()]);
    config.limits.connect_timeout = WAIT;
    config.limits.operation_timeout = WAIT;
    let processor = Arc::new(
        GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap())
            .unwrap()
            .with_legacy_context_requests(),
    );
    (listener, processor)
}
fn pipeline_config() -> PipelineConfig {
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24000;
    config.start.audio_out_sample_rate = 24000;
    config
}
fn context() -> SharedContext {
    SharedContext::new(Context::new(vec![ContextMessage::text(
        Role::User,
        "question",
    )]))
}
struct DropProbe(Arc<AtomicUsize>);
impl Drop for DropProbe {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
fn pending_executor(entered: Arc<Notify>, dropped: Arc<AtomicUsize>) -> ToolExecutor {
    let mut executor = ToolExecutor::new(2, 16, 4096).unwrap();
    executor
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |_| {
                let entered = entered.clone();
                let dropped = dropped.clone();
                Box::pin(async move {
                    let _guard = DropProbe(dropped);
                    entered.notify_one();
                    std::future::pending().await
                })
            })),
        )
        .unwrap();
    executor
}
struct DelayedPolicy {
    release: Arc<Notify>,
    declared: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for DelayedPolicy {
    fn name(&self) -> &str {
        "IndependentDelayedPolicy"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallInProgress(_))
        {
            let release = self.release.clone();
            let ctx = ctx.clone();
            let output = ctx.clone();
            ctx.spawn("review-delayed-policy", false, async move {
                release.notified().await;
                output.push(frame, direction).await
            })?;
            return Ok(());
        }
        if matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
            self.declared.notify_one();
        }
        ctx.push(frame, direction).await
    }
}
struct ReceiptBoundaryProbe {
    cancelled: Arc<Notify>,
    inference: Arc<Notify>,
    terminal_count: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for ReceiptBoundaryProbe {
    fn name(&self) -> &str {
        "IndependentReceiptBoundaryProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallCancel(_))
        {
            assert!(
                frame.native_tool_outcome()?.is_some(),
                "relay must retain validated native provenance until assistant consumption"
            );
            assert!(
                serde_json::to_vec(&frame).is_err(),
                "native terminal provenance must block serialization before assistant consumption"
            );
            self.terminal_count.fetch_add(1, Ordering::SeqCst);
            self.cancelled.notify_one();
        }
        if direction == Direction::Upstream && matches!(frame.data, FrameData::LlmContext(_)) {
            self.inference.notify_one();
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn independent_delayed_native_policy_preserves_the_one_upstream_inference_request() {
    let (listener, processor) = fixture().await;
    let relay = processor.tool_result_relay();
    let release = Arc::new(Notify::new());
    let declared = Arc::new(Notify::new());
    let cancelled = Arc::new(Notify::new());
    let inference = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let terminals = Arc::new(AtomicUsize::new(0));
    let server_cancelled = cancelled.clone();
    let server_inference = inference.clone();
    let server_release = release.clone();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        handshake(&mut ws).await;
        announce(&mut ws).await;
        timeout(WAIT, server_cancelled.notified()).await.unwrap();
        response(&mut ws, "r1", true).await;
        timeout(WAIT, server_inference.notified())
            .await
            .expect("assistant must request inference");
        assert!(
            timeout(Duration::from_millis(100), ws.next())
                .await
                .is_err(),
            "no output or inference before the missing policy is validated"
        );
        server_release.notify_one();
        let result = read(&mut ws).await;
        assert_eq!(result["item"]["type"], "function_call_output");
        assert_eq!(result["item"]["call_id"], "c1");
        assert_eq!(result["item"]["output"], "CANCELLED");
        assert_eq!(
            read(&mut ws).await["type"],
            "response.create",
            "queued inference must follow provider settlement"
        );
        response(&mut ws, "r2", false).await;
        response(&mut ws, "r2", true).await;
        assert!(
            timeout(Duration::from_millis(100), ws.next())
                .await
                .is_err(),
            "one settlement and one inference only"
        );
        ws.close(None).await.unwrap();
    });
    let executor = pending_executor(entered.clone(), dropped.clone());
    let journal = executor.outcomes();
    let context = context();
    let worker = Pipeline::new(vec![
        processor,
        Arc::new(executor),
        Arc::new(DelayedPolicy {
            release,
            declared: declared.clone(),
        }),
        Arc::new(relay),
        Arc::new(ReceiptBoundaryProbe {
            cancelled,
            inference,
            terminal_count: terminals.clone(),
        }),
        compat_assistant(context.clone(), Default::default()),
    ])
    .start(pipeline_config())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, entered.notified()).await.unwrap();
    timeout(WAIT, declared.notified()).await.unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "echo".into(),
                tool_call_id: "c1".into(),
                run_llm: true,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT * 3, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(terminals.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let snapshot = journal.snapshot();
    assert!(snapshot.active.is_empty());
    assert_eq!(snapshot.total_terminal, 1);
    assert_eq!(
        snapshot.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
}

#[derive(Clone, Copy)]
enum Mismatch {
    Name,
    Arguments,
    Policy,
}
struct NativeMismatch {
    entered: Arc<Notify>,
    mismatch: Mismatch,
}
#[async_trait]
impl FrameProcessor for NativeMismatch {
    fn name(&self) -> &str {
        "IndependentNativeMismatch"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let FrameData::FunctionCallInProgress(policy) = &frame.data {
                let mut call = FunctionCall {
                    function_name: policy.function_name.clone(),
                    tool_call_id: policy.tool_call_id.clone(),
                    arguments: policy.arguments.clone(),
                };
                let mut cancel_on_interruption = policy.cancel_on_interruption;
                match self.mismatch {
                    Mismatch::Name => call.function_name = "wrong-name".into(),
                    Mismatch::Arguments => call.arguments = json!({"n":8}),
                    Mismatch::Policy => cancel_on_interruption = !cancel_on_interruption,
                }
                let publisher = ctx.tool_outcome_publisher(call, cancel_on_interruption)?;
                ctx.push(frame, direction).await?;
                self.entered.notified().await;
                // Native construction is valid; the independent provider admission must still reject it.
                publisher.cancel(
                    ToolCallEffectStatus::NotStarted,
                    ToolCallEndReason::AdmissionRejected,
                    false,
                );
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
}
async fn native_mismatch(mismatch: Mismatch) {
    let (listener, processor) = fixture().await;
    let relay = processor.tool_result_relay();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        handshake(&mut ws).await;
        announce(&mut ws).await;
        loop {
            match timeout(WAIT, ws.next())
                .await
                .expect("invalid native receipt must close provider session")
            {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Err(_)) => break,
                Some(Ok(Message::Ping(payload))) => ws.send(Message::Pong(payload)).await.unwrap(),
                Some(Ok(Message::Text(value))) => {
                    panic!("mismatched receipt reached provider: {value}")
                }
                _ => {}
            }
        }
    });
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let executor = pending_executor(entered.clone(), dropped.clone());
    let journal = executor.outcomes();
    let context = context();
    let worker = Pipeline::new(vec![
        processor,
        Arc::new(executor),
        Arc::new(NativeMismatch { entered, mismatch }),
        Arc::new(relay),
        // Isolate the provider admission check: an assistant could independently
        // reject this mismatch first and mask a missing actor identity check.
    ])
    .start(pipeline_config())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, server).await.unwrap().unwrap();
    let terminal = timeout(WAIT, worker.collect())
        .await
        .expect("provider receipt rejection must terminate runtime and callbacks");
    match terminal {
        Err(PipelineError::Execution(message)) => {
            assert!(message.contains("unsettled native tools"), "{message}")
        }
        Ok((_, report)) => assert!(
            report.errors.iter().any(|error| error.fatal),
            "actor identity rejection must be fatal: {:?}",
            report.errors
        ),
        other => panic!("unexpected terminal outcome: {other:?}"),
    }
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        1,
        "entered callback must be dropped"
    );
    let snapshot = journal.snapshot();
    assert!(
        snapshot.active.is_empty(),
        "no active lease may outlive fatal provider settlement failure"
    );
    assert_eq!(snapshot.total_terminal, 1);
    assert_eq!(
        snapshot.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
}
#[tokio::test]
async fn independent_native_receipt_wrong_name_retires_pending_execution() {
    native_mismatch(Mismatch::Name).await;
}
#[tokio::test]
async fn independent_native_receipt_wrong_arguments_retires_pending_execution() {
    native_mismatch(Mismatch::Arguments).await;
}
#[tokio::test]
async fn independent_native_receipt_wrong_policy_retires_pending_execution() {
    native_mismatch(Mismatch::Policy).await;
}

struct RelayThenInterrupt {
    relay: pipecat_grok_realtime::GrokToolResultRelay,
    processor: Arc<GrokRealtimeProcessor>,
}
#[async_trait]
impl FrameProcessor for RelayThenInterrupt {
    fn name(&self) -> &str {
        "IndependentRelayThenInterrupt"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let completed = direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallResult(_));
        self.relay.process(frame, direction, ctx).await?;
        if completed {
            // On the current-thread runtime, ready bounded pushes do not yield.
            // Change the sender generation after native enqueue, before actor polling.
            self.processor.sender()?.interrupt(None);
            // Forward the same control downstream to the assistant. Broadcasting
            // it upstream here would make Grok emit a second interruption and
            // intentionally clear a later response debt a second time.
            ctx.push(Frame::new(FrameData::Interruption), Direction::Downstream)
                .await?;
        }
        Ok(())
    }
}
struct UserSpeakingProbe(Arc<Notify>);
#[async_trait]
impl FrameProcessor for UserSpeakingProbe {
    fn name(&self) -> &str {
        "IndependentUserSpeakingProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::UserStartedSpeaking)
        {
            self.0.notify_one();
        }
        ctx.push(frame, direction).await
    }
}
async fn admitted_success_race() -> SharedContext {
    let (listener, processor) = fixture().await;
    let relay = processor.tool_result_relay();
    let can_stop = Arc::new(Notify::new());
    let server_can_stop = can_stop.clone();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        handshake(&mut ws).await;
        announce(&mut ws).await;
        let mut cancelled = false;
        loop {
            let action = read(&mut ws).await;
            match action["type"].as_str().unwrap() {
                "input_audio_buffer.clear" => {}
                "response.cancel" => cancelled = true,
                "conversation.item.create" => {
                    assert!(
                        cancelled,
                        "priority control must overtake native outcome command"
                    );
                    assert_eq!(action["item"]["type"], "function_call_output");
                    assert_eq!(action["item"]["output"], "won");
                    break;
                }
                other => panic!("unexpected race wire {other}"),
            }
        }
        response(&mut ws, "r1", true).await;
        // A real user-speaking interval defers inference without changing any
        // field sealed into the immutable native result.
        loop {
            match timeout(Duration::from_millis(100), ws.next()).await {
                Err(_) => break,
                Ok(Some(Ok(Message::Text(text)))) => {
                    let action: Value = serde_json::from_str(&text).unwrap();
                    assert!(
                        matches!(
                            action["type"].as_str(),
                            Some("response.cancel" | "input_audio_buffer.clear")
                        ),
                        "unexpected second terminal/inference: {action}"
                    );
                }
                other => panic!("unexpected wire end {other:?}"),
            }
        }
        server_can_stop.notify_one();
        assert_eq!(
            read(&mut ws).await["type"],
            "response.create",
            "deferred native inference must resume when the user stops"
        );
        response(&mut ws, "r2", false).await;
        response(&mut ws, "r2", true).await;
        assert!(
            timeout(Duration::from_millis(100), ws.next())
                .await
                .is_err(),
            "exactly one resumed inference"
        );
        ws.close(None).await.unwrap();
    });
    let release = Arc::new(Notify::new());
    let entered = Arc::new(Notify::new());
    let releases = release.clone();
    let enters = entered.clone();
    let mut executor = ToolExecutor::new(1, 16, 4096).unwrap();
    executor
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |_| {
                let release = releases.clone();
                let entered = enters.clone();
                Box::pin(async move {
                    entered.notify_one();
                    release.notified().await;
                    Ok(json!("won"))
                })
            })),
        )
        .unwrap();
    let journal = executor.outcomes();
    let context = context();
    let speaking = Arc::new(Notify::new());
    let worker = Pipeline::new(vec![
        processor.clone(),
        Arc::new(executor),
        Arc::new(RelayThenInterrupt { relay, processor }),
        compat_assistant(context.clone(), Default::default()),
        Arc::new(UserSpeakingProbe(speaking.clone())),
    ])
    .start(pipeline_config())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, entered.notified()).await.unwrap();
    timeout(WAIT, async {
        loop {
            if context.snapshot().unwrap().messages.iter().any(|message| {
                message.tool_call_id.as_deref() == Some("c1")
                    && message.content
                        == Some(pipecat_services::Content::Text("IN_PROGRESS".into()))
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, speaking.notified()).await.unwrap();
    release.notify_one();
    timeout(WAIT, can_stop.notified()).await.unwrap();
    let mut stopped = Frame::new(FrameData::UserStoppedSpeaking);
    stopped
        .metadata
        .insert("user_context_follows".into(), json!(false));
    worker
        .sender()
        .send(stopped, Direction::Downstream)
        .await
        .unwrap();
    timeout(WAIT * 2, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let snapshot = journal.snapshot();
    assert_eq!(snapshot.total_terminal, 1);
    assert_eq!(
        snapshot.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
    assert_eq!(
        snapshot.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    context
}
#[tokio::test]
async fn independent_native_success_command_survives_sender_generation_change() {
    admitted_success_race().await;
}
#[tokio::test]
async fn required_assistant_history_matches_admitted_success_after_interruption() {
    let context = admitted_success_race().await;
    // Future common-layer gate: an admitted terminal success owns local history too.
    assert!(context
        .snapshot()
        .unwrap()
        .messages
        .iter()
        .any(|message| message.tool_call_id.as_deref() == Some("c1")
            && message.content
                == Some(pipecat_services::Content::Text(
                    serde_json::to_string(&json!("won")).unwrap()
                ))));
}

struct DelayedRegistration {
    queued: Arc<std::sync::Mutex<Vec<Frame>>>,
    release: Arc<Notify>,
    terminal: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for DelayedRegistration {
    fn name(&self) -> &str {
        "IndependentDelayedRegistration"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if matches!(
                frame.data,
                FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallInProgress(_)
            ) {
                let last = matches!(frame.data, FrameData::FunctionCallInProgress(_));
                self.queued.lock().unwrap().push(frame);
                if last {
                    let release = self.release.clone();
                    let queued = self.queued.clone();
                    let output = ctx.clone();
                    ctx.spawn("review-delayed-registration", false, async move {
                        release.notified().await;
                        let frames = std::mem::take(&mut *queued.lock().unwrap());
                        for frame in frames {
                            output.push(frame, Direction::Downstream).await?;
                        }
                        Ok(())
                    })?;
                }
                return Ok(());
            }
            if frame.native_tool_outcome()?.is_some() {
                self.terminal.notify_one();
            }
        }
        ctx.push(frame, direction).await
    }
}
#[derive(Clone, Copy)]
enum DelayedTerminal {
    Success,
    Cancel,
    Timeout,
}
async fn delayed_registration(terminal: DelayedTerminal) {
    let (listener, processor) = fixture().await;
    let relay = processor.tool_result_relay();
    let release = Arc::new(Notify::new());
    let terminal_seen = Arc::new(Notify::new());
    let server_release = release.clone();
    let server_terminal = terminal_seen.clone();
    let expected = if matches!(terminal, DelayedTerminal::Success) {
        "won"
    } else {
        "CANCELLED"
    };
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        handshake(&mut ws).await;
        announce(&mut ws).await;
        timeout(WAIT, server_terminal.notified()).await.unwrap();
        assert!(
            timeout(Duration::from_millis(100), ws.next())
                .await
                .is_err(),
            "native outcome without policy must not reach provider"
        );
        server_release.notify_one();
        let output = read(&mut ws).await;
        assert_eq!(output["item"]["call_id"], "c1");
        assert_eq!(output["item"]["output"], expected);
        response(&mut ws, "r1", true).await;
        assert_eq!(
            read(&mut ws).await["type"],
            "response.create",
            "terminal before declarations must still settle assistant and request inference"
        );
        response(&mut ws, "r2", false).await;
        response(&mut ws, "r2", true).await;
        assert!(timeout(Duration::from_millis(100), ws.next())
            .await
            .is_err());
        ws.close(None).await.unwrap();
    });
    let entered = Arc::new(Notify::new());
    let enters = entered.clone();
    let mut executor = ToolExecutor::new(1, 16, 4096).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        let entered = enters.clone();
        Box::pin(async move {
            entered.notify_one();
            if matches!(terminal, DelayedTerminal::Success) {
                Ok(json!("won"))
            } else {
                std::future::pending().await
            }
        })
    }));
    if matches!(terminal, DelayedTerminal::Timeout) {
        tool.timeout = Duration::from_millis(20);
    }
    executor.register("echo", tool).unwrap();
    let journal = executor.outcomes();
    let context = context();
    let worker = Pipeline::new(vec![
        processor,
        Arc::new(executor),
        Arc::new(DelayedRegistration {
            queued: Default::default(),
            release,
            terminal: terminal_seen,
        }),
        Arc::new(relay),
        compat_assistant(context.clone(), Default::default()),
    ])
    .start(pipeline_config())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, entered.notified()).await.unwrap();
    if matches!(terminal, DelayedTerminal::Cancel) {
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "echo".into(),
                    tool_call_id: "c1".into(),
                    run_llm: true,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    timeout(WAIT * 3, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(journal.snapshot().total_terminal, 1);
    assert!(
        context
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .any(|message| message.tool_call_id.as_deref() == Some("c1")
                && message.content
                    == Some(pipecat_services::Content::Text(
                        if matches!(terminal, DelayedTerminal::Success) {
                            serde_json::to_string(&json!("won")).unwrap()
                        } else {
                            expected.into()
                        }
                    ))),
        "assistant must record the same terminal content as the provider"
    );
}
#[tokio::test]
async fn independent_cancel_before_both_declarations_settles_assistant_once() {
    delayed_registration(DelayedTerminal::Cancel).await;
}
#[tokio::test]
async fn independent_success_before_both_declarations_settles_assistant_once() {
    delayed_registration(DelayedTerminal::Success).await;
}
#[tokio::test]
async fn independent_timeout_before_both_declarations_settles_assistant_once() {
    delayed_registration(DelayedTerminal::Timeout).await;
}

// Provider-only fixtures deliberately keep cascade aggregation. Real paired
// realtime activation is covered by paired_realtime.rs.
fn compat_assistant(
    context: SharedContext,
    config: pipecat_turns::AssistantAggregatorConfig,
) -> Arc<LLMAssistantAggregator> {
    pipecat_turns::LLMContextAggregatorPair::with_shared_context_and_realtime_mode(
        context,
        Default::default(),
        config,
        pipecat_turns::RealtimeServiceMode::Disabled,
    )
    .unwrap()
    .assistant()
}
