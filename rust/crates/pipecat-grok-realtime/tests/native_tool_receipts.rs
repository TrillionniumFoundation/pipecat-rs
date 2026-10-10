//! Source-derived executor settlement across a real WebSocket and native assistant.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_grok_realtime::{FunctionTool, GrokRealtimeProcessor, RealtimeClient, RealtimeConfig};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolOutcomeJournal};
use pipecat_services::{Context, Message as ContextMessage, Role};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
const WAIT: Duration = Duration::from_secs(5);
type Ws = WebSocketStream<TcpStream>;
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    timeout(WAIT, future)
        .await
        .expect("native receipt operation did not finish")
}
async fn wire(ws: &mut Ws, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn read(ws: &mut Ws) -> Value {
    let message = bounded(ws.next()).await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(Duration::from_millis(60), ws.next()).await.is_err(),
        "unexpected provider operation"
    );
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    wire(ws, json!({"event_id":"response","type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
#[derive(Default)]
struct BoundaryProbe {
    native: Mutex<Vec<Arc<NativeToolCallOutcome>>>,
    mapped: Mutex<Vec<FrameData>>,
    gate_entered: Notify,
    gate_release: Notify,
}
struct BeforeBoundary(Arc<BoundaryProbe>);
struct AfterBoundary(Arc<BoundaryProbe>);
#[async_trait]
impl FrameProcessor for BeforeBoundary {
    fn name(&self) -> &str {
        "BeforeNativeBoundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data,FrameData::InputText(text) if text.text == "block receipt route") {
            self.0.gate_entered.notify_one();
            self.0.gate_release.notified().await;
        }
        if let Some(outcome) = frame.native_tool_outcome()? {
            self.0.native.lock().unwrap().push(outcome.clone());
        }
        ctx.push(frame, direction).await
    }
}
#[async_trait]
impl FrameProcessor for AfterBoundary {
    fn name(&self) -> &str {
        "AfterNativeBoundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::FunctionCallCancel(_) | FrameData::FunctionCallResult(_)
        ) {
            assert!(
                frame.native_tool_outcome()?.is_some(),
                "relay must preserve terminal provenance for the assistant"
            );
            assert!(
                frame.require_no_native_llm_continuation().is_err(),
                "native pipeline receipt must not leak into an ordinary wire encoder"
            );
            self.0.mapped.lock().unwrap().push(frame.data.clone());
        }
        ctx.push(frame, direction).await
    }
}
struct Harness {
    ws: Ws,
    worker: PipelineWorker,
    processor: Arc<GrokRealtimeProcessor>,
    journal: ToolOutcomeJournal,
    executor_context: Arc<Mutex<Option<ProcessorContext>>>,
    context: SharedContext,
    probe: Arc<BoundaryProbe>,
}
impl Harness {
    async fn start(
        executor: ToolExecutor,
        before: Option<Arc<dyn FrameProcessor>>,
        pre_entry: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("native-receipt-fixture");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        config.limits.connect_timeout = WAIT;
        config.session.tools = Some(
            ["sync", "async"]
                .into_iter()
                .map(|name| FunctionTool::new(name, "fixture", json!({"type":"object"})).into())
                .collect(),
        );
        let processor = Arc::new(
            GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap())
                .unwrap()
                .with_legacy_context_requests(),
        );
        let journal = executor.outcomes();
        let context = SharedContext::new(Context::new(vec![ContextMessage::text(
            Role::User,
            "question",
        )]));
        let probe = Arc::new(BoundaryProbe::default());
        let executor_context = Arc::new(Mutex::new(None));
        let execution: Arc<dyn FrameProcessor> = Arc::new(ExecutorStage {
            executor,
            pre_entry,
            context: executor_context.clone(),
        });
        let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![
            processor.clone(),
            execution,
            Arc::new(BeforeBoundary(probe.clone())),
        ];
        stages.extend(before);
        stages.extend([
            Arc::new(processor.tool_result_relay()) as Arc<dyn FrameProcessor>,
            Arc::new(AfterBoundary(probe.clone())),
            compat_assistant(context.clone(), Default::default()),
        ]);
        let start = tokio::spawn(async move {
            let mut config = PipelineConfig::default();
            config.start.audio_in_sample_rate = 24000;
            config.start.audio_out_sample_rate = 24000;
            Pipeline::new(stages).start(config).await.unwrap()
        });
        let (tcp, _) = bounded(listener.accept()).await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        wire(
            &mut ws,
            json!({"event_id":"s","type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        wire(
            &mut ws,
            json!({"event_id":"s","type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        let worker = bounded(start).await.unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContext(context.frame().unwrap())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        loop {
            if read(&mut ws).await["type"] == "response.create" {
                break;
            }
        }
        response(&mut ws, "r1", false).await;
        Self {
            ws,
            worker,
            processor,
            journal,
            executor_context,
            context,
            probe,
        }
    }
    async fn call(&mut self, id: &str, name: &str) {
        wire(&mut self.ws,json!({"event_id":id,"type":"conversation.item.added","item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":name,"arguments":""}})).await;
        wire(&mut self.ws,json!({"event_id":id,"type":"response.function_call_arguments.done","call_id":id,"name":name,"arguments":"{}"})).await;
    }
    async fn request(&self, id: &str, name: &str, run: bool) {
        self.worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: name.into(),
                    tool_call_id: id.into(),
                    run_llm: run,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    async fn output(&mut self) -> Value {
        loop {
            let event = read(&mut self.ws).await;
            match event["type"].as_str().unwrap() {
                "input_audio_buffer.clear" | "response.cancel" => {}
                "conversation.item.create" if event["item"]["type"] == "function_call_output" => {
                    return event["item"].clone()
                }
                other => panic!("unexpected event while waiting for tool output: {other}: {event}"),
            }
        }
    }
    async fn finish(mut self) {
        quiet(&mut self.ws).await;
        self.ws.close(None).await.unwrap();
        self.worker.end().await.unwrap();
        let (_, report) = bounded(self.worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(self.journal.snapshot().active.is_empty());
        assert!(!self.processor.sender().is_ok_and(|s| s.is_ready()));
    }
    async fn inferred(&mut self) {
        response(&mut self.ws, "r1", true).await;
        assert_eq!(read(&mut self.ws).await["type"], "response.create");
        response(&mut self.ws, "r2", false).await;
        response(&mut self.ws, "r2", true).await;
    }
}
struct ExecutorStage {
    executor: ToolExecutor,
    pre_entry: bool,
    context: Arc<Mutex<Option<ProcessorContext>>>,
}
#[async_trait]
impl FrameProcessor for ExecutorStage {
    fn name(&self) -> &str {
        "NativeReceiptExecutor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        let calls = matches!(frame.data, FrameData::FunctionCallsStarted(_));
        self.executor.process(frame, direction, ctx).await?;
        if calls && self.pre_entry {
            ctx.broadcast_interruption().await?;
        }
        Ok(())
    }
}
fn pending(
    executor: &mut ToolExecutor,
    name: &str,
    interruptible: bool,
    count: &Arc<AtomicUsize>,
    entered: &Arc<Notify>,
    release: Option<Arc<Notify>>,
) {
    let (count, entered) = (count.clone(), entered.clone());
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        entered.notify_one();
        let release = release.clone();
        Box::pin(async move {
            if let Some(release) = release {
                release.notified().await;
                Ok(json!("success"))
            } else {
                std::future::pending().await
            }
        })
    }));
    tool.cancel_on_interruption = interruptible;
    executor.register(name, tool).unwrap();
}
#[tokio::test]
async fn pre_entry_abort_is_one_native_cancellation_and_no_effect() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut executor = ToolExecutor::new(2, 16, 1024).unwrap();
    pending(
        &mut executor,
        "sync",
        true,
        &count,
        &Arc::new(Notify::new()),
        None,
    );
    let mut h = Harness::start(executor, None, true).await;
    h.call("pre-entry", "sync").await;
    let item = h.output().await;
    assert_eq!(item["output"], "CANCELLED");
    assert_eq!(count.load(Ordering::SeqCst), 0);
    let snapshot = h.journal.snapshot();
    assert_eq!(snapshot.total_terminal, 1);
    assert_eq!(
        snapshot.terminal[0].outcome.effects(),
        ToolCallEffectStatus::NotStarted
    );
    assert_eq!(
        snapshot.terminal[0].admission,
        ToolOutcomeAdmission::Admitted
    );
    response(&mut h.ws, "r1", true).await;
    h.finish().await;
}
#[tokio::test]
async fn explicit_request_waits_for_receipt_and_preserves_inference_without_replay() {
    let (count, entered) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
    let mut executor = ToolExecutor::new(2, 16, 1024).unwrap();
    pending(&mut executor, "async", false, &count, &entered, None);
    let mut h = Harness::start(executor, None, false).await;
    h.call("explicit", "async").await;
    bounded(entered.notified()).await;
    h.request("unknown", "async", true).await;
    quiet(&mut h.ws).await;
    assert_eq!(h.journal.snapshot().total_terminal, 0);
    h.request("explicit", "async", true).await;
    assert_eq!(h.output().await["output"], "CANCELLED");
    quiet(&mut h.ws).await;
    h.inferred().await;
    h.request("explicit", "async", true).await;
    quiet(&mut h.ws).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let s = h.journal.snapshot();
    assert_eq!(s.total_terminal, 1);
    assert_eq!(
        s.terminal[0].outcome.reason(),
        ToolCallEndReason::ExplicitCancel
    );
    assert!(s.terminal[0].outcome.run_llm());
    assert!(!s.terminal[0].outcome.cancel_on_interruption());
    h.finish().await;
}
#[tokio::test]
async fn interruption_cancels_running_call_and_preserves_async_sibling() {
    let (count, entered, release) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    );
    let mut executor = ToolExecutor::new(2, 16, 1024).unwrap();
    pending(&mut executor, "sync", true, &count, &entered, None);
    pending(
        &mut executor,
        "async",
        false,
        &count,
        &entered,
        Some(release.clone()),
    );
    let mut h = Harness::start(executor, None, false).await;
    h.call("cancel", "sync").await;
    bounded(entered.notified()).await;
    h.call("survive", "async").await;
    bounded(entered.notified()).await;
    h.worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    let cancelled = h.output().await;
    assert_eq!(cancelled["call_id"], "cancel");
    assert_eq!(cancelled["output"], "CANCELLED");
    release.notify_one();
    let success = h.output().await;
    assert_eq!(success["call_id"], "survive");
    assert_eq!(success["output"], "success");
    // Interruption's assistant state owns whether a later user turn requests inference.
    response(&mut h.ws, "r1", true).await;
    assert_eq!(count.load(Ordering::SeqCst), 2);
    let s = h.journal.snapshot();
    assert_eq!(s.total_terminal, 2);
    assert_eq!(
        s.terminal
            .iter()
            .filter(|r| r.outcome.state() == ToolCallTerminalState::Cancelled)
            .count(),
        1
    );
    // Explicitly close this interrupted response; no synthetic playback receipt exists.
    h.worker.cancel("fixture finished").await.unwrap();
    bounded(h.worker.collect()).await.unwrap();
    assert!(h.journal.snapshot().active.is_empty());
}
#[derive(Clone, Copy, Debug)]
enum Failure {
    Timeout,
    Error,
    Panic,
    ConstructorPanic,
}
async fn failed_cycle(mode: Failure) {
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        calls.fetch_add(1, Ordering::SeqCst);
        assert!(
            !matches!(mode, Failure::ConstructorPanic),
            "constructor panic fixture"
        );
        Box::pin(async move {
            match mode {
                Failure::Timeout => std::future::pending().await,
                Failure::Error => Err("callback failure".into()),
                Failure::Panic => panic!("future panic fixture"),
                Failure::ConstructorPanic => unreachable!(),
            }
        })
    }));
    tool.timeout = Duration::from_millis(30);
    executor.register("sync", tool).unwrap();
    let mut h = Harness::start(executor, None, false).await;
    h.call("failed", "sync").await;
    let result = h.output().await;
    match mode {
        Failure::Timeout => assert_eq!(result["output"], "CANCELLED"),
        _ => assert!(
            serde_json::from_str::<Value>(result["output"].as_str().unwrap()).unwrap()["error"]
                .is_string()
        ),
    }
    quiet(&mut h.ws).await;
    h.inferred().await;
    let s = h.journal.snapshot();
    assert_eq!(s.total_terminal, 1);
    assert_eq!(s.terminal[0].outcome.state(), ToolCallTerminalState::Failed);
    assert_eq!(
        s.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert_eq!(
        s.terminal[0].outcome.reason(),
        match mode {
            Failure::Timeout => ToolCallEndReason::TimedOut,
            Failure::Error => ToolCallEndReason::CallbackError,
            _ => ToolCallEndReason::CallbackPanicked,
        }
    );
    let mapped = h.probe.mapped.lock().unwrap().clone();
    if matches!(mode, Failure::Timeout) {
        assert!(matches!(&mapped[0],FrameData::FunctionCallCancel(c) if c.run_llm));
        assert!(h
            .context
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .any(|m| m.content == Some(pipecat_services::Content::Text("CANCELLED".into()))));
    }
    assert_eq!(count.load(Ordering::SeqCst), 1);
    h.finish().await;
}
#[tokio::test]
async fn typed_timeout_maps_to_source_cancellation_and_upstream_inference() {
    failed_cycle(Failure::Timeout).await;
}
#[tokio::test]
async fn callback_error_remains_failed_result_and_requests_inference() {
    failed_cycle(Failure::Error).await;
}
#[tokio::test]
async fn callback_panic_remains_failed_result_and_never_replays() {
    failed_cycle(Failure::Panic).await;
    failed_cycle(Failure::ConstructorPanic).await;
}
struct HoldResult {
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl FrameProcessor for HoldResult {
    fn name(&self) -> &str {
        "HoldAdmittedResult"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::FunctionCallResult(_)) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn completed_result_beats_later_cancel_request_while_receipt_is_queued() {
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    executor
        .register(
            "sync",
            RegisteredTool::new(Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!("won")) })
            })),
        )
        .unwrap();
    let hold = Arc::new(HoldResult {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let mut h = Harness::start(executor, Some(hold.clone()), false).await;
    h.call("winner", "sync").await;
    bounded(hold.entered.notified()).await;
    assert_eq!(h.journal.snapshot().total_terminal, 1);
    h.request("winner", "sync", true).await;
    quiet(&mut h.ws).await;
    hold.release.notify_one();
    assert_eq!(h.output().await["output"], "won");
    h.inferred().await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(
        h.journal.snapshot().terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
    h.finish().await;
}
#[tokio::test]
async fn wrong_name_request_does_not_cancel_or_acknowledge_active_call() {
    let (count, entered, release) = (
        Arc::new(AtomicUsize::new(0)),
        Arc::new(Notify::new()),
        Arc::new(Notify::new()),
    );
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    pending(
        &mut executor,
        "sync",
        true,
        &count,
        &entered,
        Some(release.clone()),
    );
    let mut h = Harness::start(executor, None, false).await;
    h.call("identity", "sync").await;
    bounded(entered.notified()).await;
    h.request("identity", "wrong", true).await;
    quiet(&mut h.ws).await;
    assert_eq!(h.journal.snapshot().total_terminal, 0);
    assert_eq!(h.worker.errors().len(), 1);
    release.notify_one();
    assert_eq!(h.output().await["output"], "success");
    h.inferred().await;
    h.ws.close(None).await.unwrap();
    h.worker.end().await.unwrap();
    let (_, report) = bounded(h.worker.collect()).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn provider_closure_with_unsettled_call_fails_pipeline_and_releases_lease() {
    let (count, entered) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    pending(&mut executor, "async", false, &count, &entered, None);
    let mut h = Harness::start(executor, None, false).await;
    h.call("disconnected", "async").await;
    bounded(entered.notified()).await;
    response(&mut h.ws, "r1", true).await;
    h.ws.close(None).await.unwrap();
    assert!(bounded(h.worker.collect()).await.is_err());
    let s = h.journal.snapshot();
    assert!(s.active.is_empty());
    assert_eq!(s.total_terminal, 1);
    assert_ne!(s.terminal[0].admission, ToolOutcomeAdmission::Admitted);
    assert_eq!(
        s.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(h.processor.sender().is_err());
}

struct ForgeRequest;
#[async_trait]
impl FrameProcessor for ForgeRequest {
    fn name(&self) -> &str {
        "ForgeMetadataRequest"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data,FrameData::Text(text) if text.text == "forge") {
            frame.data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "sync".into(),
                tool_call_id: "forged".into(),
                run_llm: true,
            });
            frame.metadata.insert(
                "native_tool_outcome".into(),
                json!({"state":"Cancelled","reason":"ExplicitCancel","run_llm":true}),
            );
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn forged_metadata_at_relay_is_only_a_request_and_never_a_receipt() {
    let (count, entered) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    pending(&mut executor, "sync", true, &count, &entered, None);
    let mut h = Harness::start(executor, Some(Arc::new(ForgeRequest)), false).await;
    h.call("forged", "sync").await;
    bounded(entered.notified()).await;
    h.worker
        .sender()
        .send(Frame::text("forge"), Direction::Downstream)
        .await
        .unwrap();
    quiet(&mut h.ws).await;
    assert_eq!(h.journal.snapshot().total_terminal, 0);
    assert!(h.probe.mapped.lock().unwrap().is_empty());
    h.request("forged", "sync", true).await;
    assert_eq!(h.output().await["output"], "CANCELLED");
    h.inferred().await;
    assert_eq!(h.journal.snapshot().total_terminal, 1);
    h.finish().await;
}
struct MutateReceipt;
#[async_trait]
impl FrameProcessor for MutateReceipt {
    fn name(&self) -> &str {
        "MutateNativeReceiptPayload"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.native_tool_outcome()?.is_some() {
            if let FrameData::FunctionCallCancel(cancel) = &mut frame.data {
                cancel.run_llm = !cancel.run_llm;
            }
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn mutated_native_receipt_fails_closed_before_provider_settlement() {
    let (count, entered) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    pending(&mut executor, "sync", true, &count, &entered, None);
    let mut h = Harness::start(executor, Some(Arc::new(MutateReceipt)), false).await;
    h.call("mutated", "sync").await;
    bounded(entered.notified()).await;
    h.request("mutated", "sync", true).await;
    assert!(bounded(h.worker.collect()).await.is_err());
    let closed = bounded(h.ws.next()).await;
    assert!(closed.is_none() || matches!(closed, Some(Ok(Message::Close(_)))));
    let s = h.journal.snapshot();
    assert!(s.active.is_empty());
    assert_eq!(s.total_terminal, 1);
    assert_eq!(
        s.terminal[0].admission,
        ToolOutcomeAdmission::Admitted,
        "native first-hop admission cannot promise provider settlement"
    );
    assert!(h.probe.mapped.lock().unwrap().is_empty());
}
#[tokio::test]
async fn full_native_receipt_route_fails_session_closed_without_provider_acknowledgment() {
    let (count, entered) = (Arc::new(AtomicUsize::new(0)), Arc::new(Notify::new()));
    let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
    pending(&mut executor, "sync", true, &count, &entered, None);
    let mut h = Harness::start(executor, None, false).await;
    h.call("full", "sync").await;
    bounded(entered.notified()).await;
    quiet(&mut h.ws).await;
    let ctx = h.executor_context.lock().unwrap().clone().unwrap();
    ctx.push(
        Frame::new(FrameData::InputText(TextFrame::new("block receipt route"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    bounded(h.probe.gate_entered.notified()).await;
    for _ in 0..PipelineConfig::default().system_queue_capacity {
        ctx.push(
            Frame::new(FrameData::InputText(TextFrame::new("queued pressure"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    }
    h.request("full", "sync", true).await;
    assert!(bounded(h.worker.collect()).await.is_err());
    let closed = bounded(h.ws.next()).await;
    assert!(closed.is_none() || matches!(closed, Some(Ok(Message::Close(_)))));
    let s = h.journal.snapshot();
    assert!(s.active.is_empty());
    assert_eq!(s.total_terminal, 1);
    assert!(matches!(
        s.terminal[0].admission,
        ToolOutcomeAdmission::Undelivered(_)
    ));
    assert_eq!(
        s.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(h.probe.mapped.lock().unwrap().is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert!(h.processor.sender().is_err());
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
