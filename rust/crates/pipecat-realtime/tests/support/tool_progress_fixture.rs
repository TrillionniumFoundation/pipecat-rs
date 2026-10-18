//! Real WS and real-pipeline fixtures. No receipt, lease, or carrier is minted here.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::{RealtimeClient, RealtimeConfig};
use pipecat_services::Content;
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

pub const DEADLINE: Duration = Duration::from_secs(3);
pub const CALLBACK_TIMEOUT: Duration = Duration::from_secs(10);
pub const END_DRAIN: Duration = Duration::from_secs(15);
pub const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
pub const OPERATION_TIMEOUT: Duration = Duration::from_secs(1);
pub const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(25);

pub fn pipeline_config() -> PipelineConfig {
    assert_eq!(
        SHUTDOWN_TIMEOUT,
        END_DRAIN
            + RESPONSE_TIMEOUT
            + OPERATION_TIMEOUT * 2
            + CLOSE_TIMEOUT
            + Duration::from_secs(5),
        "selected shutdown configuration must satisfy the approved formula",
    );
    assert!(END_DRAIN >= CALLBACK_TIMEOUT + Duration::from_secs(5));
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    config.shutdown_timeout = SHUTDOWN_TIMEOUT;
    config
}

pub struct Peer {
    events: mpsc::UnboundedSender<PeerEvent>,
    commands: mpsc::UnboundedReceiver<Value>,
    pub observed: Vec<Value>,
    task: JoinHandle<()>,
}
enum PeerEvent {
    Event(Value),
    Close,
}
impl Peer {
    pub async fn with_response_timeout(response_timeout: Duration) -> (Self, RealtimeClient) {
        Self::with_command_capacity(response_timeout, 64).await
    }
    pub async fn with_command_capacity(
        response_timeout: Duration,
        queue_capacity: usize,
    ) -> (Self, RealtimeClient) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("loopback-reporting-test-key");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits.response_timeout = response_timeout;
        config.limits.queue_capacity = queue_capacity;
        config.limits.operation_timeout = OPERATION_TIMEOUT;
        config.limits.close_timeout = CLOSE_TIMEOUT;
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<PeerEvent>();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let initial = socket.next().await.unwrap().unwrap();
            let initial: Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
            assert_eq!(initial["type"], "session.update");
            socket
                .send(Message::Text(
                    json!({
                        "type":"session.updated", "event_id":"reporting-ready",
                        "session":{"id":"reporting-session","type":"realtime"}
                    })
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            let mut sequence = 0_u64;
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        let mut event = match event {
                            Some(PeerEvent::Event(event)) => event,
                            Some(PeerEvent::Close) => {
                                socket.close(None).await.unwrap();
                                break;
                            }
                            None => break,
                        };
                        sequence += 1;
                        event["event_id"] = json!(format!("reporting-event-{sequence}"));
                        if socket.send(Message::Text(event.to_string().into())).await.is_err() {
                            break;
                        }
                    }
                    message = socket.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            if command_tx.send(serde_json::from_str(&text).unwrap()).is_err() {
                                break;
                            }
                        }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => {
                            let _ = socket.close(None).await;
                            break;
                        }
                        _ => {}
                    }
                }
            }
        });
        (
            Self {
                events: event_tx,
                commands: command_rx,
                observed: vec![],
                task,
            },
            RealtimeClient::new(config).unwrap(),
        )
    }
    pub fn event(&self, event: Value) {
        self.events.send(PeerEvent::Event(event)).unwrap();
    }
    pub fn close(&self) {
        self.events.send(PeerEvent::Close).unwrap();
    }
    pub fn response_start(&self, id: &str) {
        self.event(json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}));
    }
    pub fn response_end(&self, id: &str) {
        self.event(
            json!({"type":"response.done","response":{"id":id,"status":"completed","output":[]}}),
        );
    }
    pub fn call(&self, response: &str, id: &str, name: &str, index: usize, arguments: Value) {
        let item = format!("item-{id}");
        self.event(
            json!({"type":"response.output_item.added","response_id":response,
            "output_index":index,"item":{"type":"function_call","id":item,
            "call_id":id,"name":name,"arguments":""}}),
        );
        self.event(
            json!({"type":"response.function_call_arguments.done","response_id":response,
            "item_id":item,"output_index":index,"call_id":id,"arguments":arguments.to_string()}),
        );
    }
    pub async fn command(&mut self) -> Value {
        let command = timeout(DEADLINE, self.commands.recv())
            .await
            .expect("real provider command deadline")
            .expect("real peer closed");
        self.observed.push(command.clone());
        command
    }
    pub async fn substantive(&mut self) -> Value {
        loop {
            let command = self.command().await;
            if command["type"] != "session.update" {
                return command;
            }
        }
    }
    /// A negative wire assertion only, never a history-consumption receipt.
    pub async fn quiet(&mut self) {
        match timeout(Duration::from_millis(60), self.commands.recv()).await {
            Err(_) => (),
            Ok(value) => panic!("unexpected provider command: {value:?}"),
        }
    }
    pub fn formal_outputs(&self, call_id: &str) -> usize {
        self.observed
            .iter()
            .filter(|command| {
                command["type"] == "conversation.item.create"
                    && command["item"]["type"] == "function_call_output"
                    && command["item"]["call_id"] == call_id
            })
            .count()
    }
    pub async fn join(&mut self) {
        timeout(DEADLINE, &mut self.task)
            .await
            .expect("actual WS peer join deadline")
            .unwrap();
        while let Ok(command) = self.commands.try_recv() {
            self.observed.push(command);
        }
    }
    pub async fn closed(mut self) {
        self.join().await;
    }
}

/// Retains the real frame and its actual delivery origin, not an observer ACK.
/// Returning from process leaves both normal and system lanes available, so an
/// actual cancellation terminal may overtake the retained normal predecessor.
pub struct HeldFrame {
    pub frame: Frame,
    pub context: ProcessorContext,
    pub direction: Direction,
}
impl HeldFrame {
    pub async fn deliver(&self) {
        self.context
            .push(self.frame.clone(), self.direction)
            .await
            .unwrap();
    }
}

pub struct Boundary {
    pub hold_progress: AtomicBool,
    pub hold_upstream_progress: AtomicBool,
    pub hold_continuation: AtomicBool,
    pub hold_terminal: AtomicBool,
    pub fail_continuation: AtomicBool,
    pub captured: mpsc::UnboundedSender<HeldFrame>,
    pub terminals: mpsc::UnboundedSender<Frame>,
}
impl Boundary {
    pub fn new(
        hold_progress: bool,
    ) -> (
        Arc<Self>,
        mpsc::UnboundedReceiver<HeldFrame>,
        mpsc::UnboundedReceiver<Frame>,
    ) {
        let (captured, receiver) = mpsc::unbounded_channel();
        let (terminals, terminal_rx) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                hold_progress: hold_progress.into(),
                hold_upstream_progress: false.into(),
                hold_continuation: false.into(),
                hold_terminal: false.into(),
                fail_continuation: false.into(),
                captured,
                terminals,
            }),
            receiver,
            terminal_rx,
        )
    }
}
#[async_trait]
impl FrameProcessor for Boundary {
    fn name(&self) -> &str {
        "real-tool-progress-delivery-boundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let terminal = (direction == Direction::Downstream
            && frame.native_tool_outcome()?.is_some())
        .then(|| frame.clone());
        let progress = frame.native_tool_progress()?.is_some();
        let continuation = direction == Direction::Upstream
            && frame.native_realtime_context_cause()?.is_some_and(|cause| {
                cause.intent() == RealtimeContextIntent::AssistantContinuation
            });
        if continuation && self.fail_continuation.load(Ordering::SeqCst) {
            return Err(ProcessorError::fatal(
                "real continuation delivery failed at test boundary",
            ));
        }
        if (progress
            && ((direction == Direction::Downstream && self.hold_progress.load(Ordering::SeqCst))
                || (direction == Direction::Upstream
                    && self.hold_upstream_progress.load(Ordering::SeqCst))))
            || (continuation && self.hold_continuation.load(Ordering::SeqCst))
            || (terminal.is_some() && self.hold_terminal.load(Ordering::SeqCst))
        {
            self.captured
                .send(HeldFrame {
                    frame,
                    context: ctx.clone(),
                    direction,
                })
                .map_err(|_| ProcessorError::fatal("test delivery receiver closed"))?;
            return Ok(());
        }
        ctx.push(frame, direction).await?;
        if let Some(terminal) = terminal {
            let _ = self.terminals.send(terminal);
        }
        Ok(())
    }
}

pub async fn channel<T>(receiver: &mut mpsc::UnboundedReceiver<T>) -> T {
    timeout(DEADLINE, receiver.recv())
        .await
        .expect("test observation deadline")
        .expect("test observation channel closed")
}
pub async fn inject(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
pub async fn receive(worker: &mut PipelineWorker, predicate: impl Fn(&FrameData) -> bool) -> Frame {
    timeout(DEADLINE, async {
        loop {
            let event = worker
                .recv()
                .await
                .expect("pipeline closed before observation");
            if let FrameData::Error(error) = &event.frame.data {
                panic!("unexpected pipeline error: {error:?}");
            }
            if predicate(&event.frame.data) {
                return event.frame;
            }
        }
    })
    .await
    .expect("actual pipeline observation deadline")
}
pub async fn barrier(worker: &mut PipelineWorker) {
    inject(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("tool-progress-barrier".into()),
        }),
    )
    .await;
    receive(worker, |data| matches!(data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("tool-progress-barrier"))).await;
}
pub fn envelopes(pair: &LLMContextAggregatorPair, call_id: &str) -> Vec<Value> {
    pair.context()
        .snapshot()
        .unwrap()
        .messages
        .iter()
        .filter_map(|message| {
            let Some(Content::Text(text)) = &message.content else {
                return None;
            };
            let value: Value = serde_json::from_str(text).ok()?;
            (value["type"] == "async_tool" && value["tool_call_id"] == call_id).then_some(value)
        })
        .collect()
}
pub fn result_envelopes(pair: &LLMContextAggregatorPair, call_id: &str) -> Vec<Value> {
    envelopes(pair, call_id)
        .into_iter()
        .filter(|value| value.get("result").is_some())
        .collect()
}
pub fn assert_progress_command(command: &Value, call_id: &str, result: &Value) {
    assert_eq!(command["type"], "conversation.item.create");
    assert_eq!(command["item"]["type"], "message");
    assert_eq!(command["item"]["role"], "system");
    let text = command["item"]["content"][0]["text"].as_str().unwrap();
    let envelope: Value = serde_json::from_str(text).unwrap();
    assert_eq!(envelope["type"], "async_tool");
    assert_eq!(envelope["status"], "running");
    assert_eq!(envelope["tool_call_id"], call_id);
    let expected = if result.is_null() {
        "COMPLETED".into()
    } else {
        result.to_string()
    };
    assert_eq!(envelope["result"], expected);
}
pub async fn ended(mut worker: PipelineWorker, peer: Peer, route: &NativeToolProgressRoute) {
    worker.end().await.unwrap();
    let report = timeout(DEADLINE, worker.join())
        .await
        .expect("actual PipelineWorker join deadline")
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        route.is_closed(),
        "tracked receipt owner must close before the worker joins"
    );
    peer.closed().await;
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssistantObservation {
    ResponseStarted,
    ResponseEnded,
    Interrupted,
    Continuation,
    UserContext,
    PlaybackStopped,
    ToolTerminal,
}
/// Delegates to the real processor at the same runtime node. This only observes
/// successful return; it never mutates history, resolves a receipt, or changes
/// the context/name/index used for real setup and admission. Separate instances
/// observe the actual assistant and the actual provider.
pub struct AssistantWitness {
    inner: Arc<dyn FrameProcessor>,
    observations: mpsc::UnboundedSender<AssistantObservation>,
}
impl AssistantWitness {
    pub fn new(
        inner: Arc<dyn FrameProcessor>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<AssistantObservation>) {
        let (observations, receiver) = mpsc::unbounded_channel();
        (
            Arc::new(Self {
                inner,
                observations,
            }),
            receiver,
        )
    }
}
#[async_trait]
impl FrameProcessor for AssistantWitness {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Some(error) = frame.data.as_error_frame() {
            eprintln!(
                "real {} observed pipeline error: {error:?}",
                self.inner.name()
            );
        }
        let terminal = frame.native_tool_outcome()?.is_some();
        let observation = match &frame.data {
            FrameData::LlmResponseStart => Some(AssistantObservation::ResponseStarted),
            FrameData::LlmResponseEnd => Some(AssistantObservation::ResponseEnded),
            FrameData::Interruption => Some(AssistantObservation::Interrupted),
            FrameData::BotStoppedSpeaking => Some(AssistantObservation::PlaybackStopped),
            _ if terminal => Some(AssistantObservation::ToolTerminal),
            _ if frame.native_realtime_context_cause()?.is_some_and(|cause| {
                cause.intent() == RealtimeContextIntent::AssistantContinuation
            }) =>
            {
                Some(AssistantObservation::Continuation)
            }
            _ if frame.native_realtime_context_cause()?.is_some() => {
                Some(AssistantObservation::UserContext)
            }
            _ => None,
        };
        let result = self.inner.process(frame, direction, ctx).await;
        if let Err(error) = &result {
            eprintln!("real {} process error: {error:?}", self.inner.name());
        }
        result?;
        if let Some(observation) = observation {
            let _ = self.observations.send(observation);
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
pub async fn assistant_observed(
    receiver: &mut mpsc::UnboundedReceiver<AssistantObservation>,
    wanted: AssistantObservation,
) {
    timeout(DEADLINE, async {
        loop {
            if receiver
                .recv()
                .await
                .expect("real assistant observation closed")
                == wanted
            {
                return;
            }
        }
    })
    .await
    .expect("real assistant after-process deadline");
}

/// The real executor still owns dispatch, its actual task and every capability.
/// This wrapper can preserve an independently originating invocation scope and
/// exposes its actual self-queue for negative carrier re-entry tests.
pub struct ExecutorWitness {
    inner: Arc<dyn FrameProcessor>,
    context: std::sync::Mutex<Option<ProcessorContext>>,
    pub foreign: std::sync::Mutex<Option<ProcessorContext>>,
}
impl ExecutorWitness {
    pub fn new(inner: Arc<dyn FrameProcessor>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            context: std::sync::Mutex::new(None),
            foreign: std::sync::Mutex::new(None),
        })
    }
    pub async fn queue(&self, frame: Frame) -> Result<(), ProcessorError> {
        let context = self.context.lock().unwrap().as_ref().unwrap().clone();
        context.queue(frame, Direction::Downstream).await
    }
}
#[async_trait]
impl FrameProcessor for ExecutorWitness {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await?;
        *self.context.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let foreign = if matches!(frame.data, FrameData::FunctionCallsStarted(_)) {
            self.foreign.lock().unwrap().clone()
        } else {
            None
        };
        let result = if let Some(foreign) = foreign {
            self.inner
                .process(frame, direction, &ctx.forwarding_context(&foreign))
                .await
        } else {
            self.inner.process(frame, direction, ctx).await
        };
        if let Err(error) = &result {
            eprintln!("real {} process error: {error:?}", self.inner.name());
        }
        result
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.inner.cleanup().await;
        // Retained test handles must not keep either runtime through cleanup.
        let context = self.context.lock().unwrap().take();
        let foreign = self.foreign.lock().unwrap().take();
        drop((context, foreign));
        result
    }
}

pub struct CaptureInvocation(pub mpsc::UnboundedSender<ProcessorContext>);
#[async_trait]
impl FrameProcessor for CaptureInvocation {
    fn name(&self) -> &str {
        "independent-progress-source"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            self.0.send(ctx.clone()).unwrap();
        }
        ctx.push(frame, direction).await
    }
}

#[derive(Debug)]
pub enum SetupLifecycleObservation {
    Setup(usize, Result<(), ProcessorError>),
    Cleanup(usize, Result<(), ProcessorError>),
}
/// Observe the real component at its original runtime node. Registration,
/// lifecycle effects, errors, and cleanup remain entirely with the delegate.
pub struct SetupCleanupWitness {
    inner: Arc<dyn FrameProcessor>,
    index: usize,
    observations: mpsc::UnboundedSender<SetupLifecycleObservation>,
}
impl SetupCleanupWitness {
    pub fn new(
        inner: Arc<dyn FrameProcessor>,
        index: usize,
        observations: mpsc::UnboundedSender<SetupLifecycleObservation>,
    ) -> Arc<Self> {
        Arc::new(Self {
            inner,
            index,
            observations,
        })
    }
}
#[async_trait]
impl FrameProcessor for SetupCleanupWitness {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let outcome = self.inner.setup(config, ctx).await;
        let _ = self.observations.send(SetupLifecycleObservation::Setup(
            self.index,
            outcome.clone(),
        ));
        outcome
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let outcome = self.inner.cleanup().await;
        let _ = self.observations.send(SetupLifecycleObservation::Cleanup(
            self.index,
            outcome.clone(),
        ));
        outcome
    }
}
