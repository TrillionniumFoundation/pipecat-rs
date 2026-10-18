//! Synthetic loopback peer and real pipeline probes. Nothing here can mint a
//! user-history receipt. A WebSocket write acknowledgment only proves that the
//! fixture wrote a packet; production Metrics witnesses prove its consumption.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::{GeminiClient, GeminiConfig};
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, Notify, Semaphore};
use tokio::task::JoinHandle;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite::Message;

pub const WAIT: Duration = Duration::from_secs(8);
pub const INPUT_PCM: &[u8] = &[3, 0, 4, 0];
pub const OUTPUT_PCM: &[u8] = &[1, 0, 2, 0];
const TRACE_BOUND: usize = 1024;

#[derive(Clone, Debug, Default)]
pub struct WireTrace {
    pub setup: Option<Value>,
    pub input: Vec<Value>,
    pub peer_closed: bool,
}
#[derive(Default)]
struct PeerState {
    trace: Mutex<WireTrace>,
    changed: Notify,
}
enum Command {
    Packet(Value, oneshot::Sender<()>),
    Close(oneshot::Sender<()>),
}
pub struct Peer {
    commands: mpsc::Sender<Command>,
    state: Arc<PeerState>,
    task: Option<JoinHandle<Result<(), String>>>,
}
impl Peer {
    pub async fn open(configure: impl FnOnce(&mut GeminiConfig)) -> (Self, GeminiClient) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("paired-history-synthetic-key", "gemini-3.8-live");
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        config.limits.connect_timeout = Duration::from_secs(2);
        config.limits.setup_timeout = Duration::from_secs(2);
        config.limits.write_timeout = Duration::from_secs(1);
        config.limits.finish_timeout = Duration::from_secs(1);
        configure(&mut config);
        let client = GeminiClient::new(config).unwrap();
        let state = Arc::new(PeerState::default());
        let owner = state.clone();
        let (commands, mut command) = mpsc::channel(32);
        let task = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "peer accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut ws = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            let setup = timeout(WAIT, ws.next())
                .await
                .map_err(|_| "peer setup deadline")?
                .ok_or("peer closed before setup")?
                .map_err(|e| e.to_string())?;
            let setup: Value = serde_json::from_str(setup.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            if !setup["setup"].is_object() {
                return Err(format!("expected Gemini setup, got {setup}"));
            }
            owner.trace.lock().unwrap().setup = Some(setup);
            owner.changed.notify_waiters();
            ws.send(Message::Text(
                json!({"setupComplete":{}}).to_string().into(),
            ))
            .await
            .map_err(|e| e.to_string())?;
            loop {
                tokio::select! {
                    next = command.recv() => match next {
                        Some(Command::Packet(value, sent)) => {
                            ws.send(Message::Text(value.to_string().into()))
                                .await.map_err(|e| e.to_string())?;
                            let _ = sent.send(());
                        }
                        Some(Command::Close(sent)) => {
                            ws.close(None).await.map_err(|e| e.to_string())?;
                            let _ = sent.send(());
                        }
                        None => return Err("fixture commands closed before peer shutdown".into()),
                    },
                    incoming = ws.next() => match incoming {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                            {
                                let mut trace = owner.trace.lock().unwrap();
                                if trace.input.len() == TRACE_BOUND {
                                    return Err("fixture wire trace bound exceeded".into());
                                }
                                trace.input.push(value);
                            }
                            owner.changed.notify_waiters();
                        }
                        Some(Ok(Message::Ping(bytes))) => {
                            ws.send(Message::Pong(bytes)).await.map_err(|e| e.to_string())?;
                        }
                        Some(Ok(Message::Pong(_))) => {},
                        None | Some(Ok(Message::Close(_))) => {
                            owner.trace.lock().unwrap().peer_closed = true;
                            owner.changed.notify_waiters();
                            let _ = ws.flush().await;
                            return Ok(());
                        }
                        Some(Err(error)) => {
                            // Forced teardown may abort the socket without a close
                            // handshake. This is peer/resource closure, never proof
                            // that production awaited its inner worker handle.
                            if matches!(error, tokio_tungstenite::tungstenite::Error::ConnectionClosed
                                | tokio_tungstenite::tungstenite::Error::AlreadyClosed
                                | tokio_tungstenite::tungstenite::Error::Io(_)
                                | tokio_tungstenite::tungstenite::Error::Protocol(
                                    tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)) {
                                owner.trace.lock().unwrap().peer_closed = true;
                                owner.changed.notify_waiters();
                                return Ok(());
                            }
                            return Err(error.to_string());
                        }
                        other => return Err(format!("unexpected fixture peer event: {other:?}")),
                    }
                }
            }
        });
        (
            Self {
                commands,
                state,
                task: Some(task),
            },
            client,
        )
    }
    pub async fn send(&self, packet: Value) {
        let (sent, done) = oneshot::channel();
        self.commands
            .send(Command::Packet(packet, sent))
            .await
            .unwrap();
        timeout(WAIT, done).await.unwrap().unwrap();
    }
    pub async fn close(&self) {
        let (sent, done) = oneshot::channel();
        self.commands.send(Command::Close(sent)).await.unwrap();
        timeout(WAIT, done).await.unwrap().unwrap();
    }
    pub fn snapshot(&self) -> WireTrace {
        self.state.trace.lock().unwrap().clone()
    }
    pub async fn wait_for(&self, predicate: impl Fn(&WireTrace) -> bool) -> WireTrace {
        timeout(WAIT, async {
            loop {
                let changed = self.state.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                let trace = self.snapshot();
                if predicate(&trace) {
                    return trace;
                }
                changed.await;
            }
        })
        .await
        .unwrap_or_else(|error| {
            panic!(
                "wire witness deadline: {error}; actual wire: {:#?}",
                self.snapshot()
            )
        })
    }
    pub async fn join(mut self) -> WireTrace {
        let mut task = self.task.take().unwrap();
        match timeout(WAIT, &mut task).await {
            Ok(result) => result.unwrap().unwrap(),
            Err(error) => {
                task.abort();
                let _ = task.await;
                panic!("fixture peer timed out; aborted and joined: {error}");
            }
        }
        let trace = self.snapshot();
        assert!(
            trace.peer_closed,
            "peer join did not observe resource closure"
        );
        trace
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
    }
}

#[derive(Clone, Debug)]
pub struct Observation {
    pub frame: Frame,
    pub direction: Direction,
    pub history: LlmContextData,
}
#[derive(Default)]
pub struct ProbeState {
    observations: Mutex<Vec<Observation>>,
    changed: Notify,
}
impl ProbeState {
    pub fn snapshot(&self) -> Vec<Observation> {
        self.observations.lock().unwrap().clone()
    }
    async fn wait_result(
        &self,
        predicate: impl Fn(&Observation) -> bool,
    ) -> Result<Observation, tokio::time::error::Elapsed> {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                tokio::pin!(changed);
                changed.as_mut().enable();
                if let Some(item) = self.snapshot().into_iter().find(&predicate) {
                    return item;
                }
                changed.await;
            }
        })
        .await
    }
    pub async fn wait_for(&self, predicate: impl Fn(&Observation) -> bool) -> Observation {
        self.wait_result(predicate).await.unwrap_or_else(|error| {
            panic!(
                "actual pipeline observation deadline: {error}; observations: {:#?}",
                self.snapshot()
            )
        })
    }
    pub async fn wait_for_in(
        &self,
        worker: &PipelineWorker,
        predicate: impl Fn(&Observation) -> bool,
    ) -> Observation {
        self.wait_result(predicate).await.unwrap_or_else(|error| {
            panic!("actual pipeline observation deadline: {error}; runtime state: {:?}; runtime errors: {:#?}; observations: {:#?}", worker.state(), worker.errors(), self.snapshot())
        })
    }
}
pub struct Probe {
    pub name: &'static str,
    pub context: SharedContext,
    pub state: Arc<ProbeState>,
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        self.name
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let observed = Observation {
            frame: frame.clone(),
            direction,
            history: self.context.snapshot_rich()?,
        };
        {
            let mut observations = self.state.observations.lock().unwrap();
            if observations.len() == TRACE_BOUND {
                return Err(ProcessorError::fatal("fixture observation bound"));
            }
            observations.push(observed);
        }
        self.state.changed.notify_waiters();
        ctx.push(frame, direction).await
    }
}

#[derive(Clone, Copy)]
pub enum HoldKind {
    FinalTranscription,
    InputAudio,
}
pub struct Hold {
    kind: HoldKind,
    held: Mutex<Option<Frame>>,
    entered: Notify,
    released: Semaphore,
}
impl Hold {
    pub fn new(kind: HoldKind) -> Arc<Self> {
        Arc::new(Self {
            kind,
            held: Mutex::new(None),
            entered: Notify::new(),
            released: Semaphore::new(0),
        })
    }
    pub async fn wait_entered(&self) -> Frame {
        timeout(WAIT, async {
            loop {
                let entered = self.entered.notified();
                tokio::pin!(entered);
                entered.as_mut().enable();
                if let Some(frame) = self.held.lock().unwrap().clone() {
                    return frame;
                }
                entered.await;
            }
        })
        .await
        .expect("delegating hold entry deadline")
    }
    pub fn release(&self) {
        self.released.add_permits(1);
    }
    async fn before(&self, frame: &Frame, direction: Direction) {
        let selected = match self.kind {
            HoldKind::FinalTranscription => {
                direction == Direction::Upstream
                    && matches!(frame.data, FrameData::Transcription(_))
            }
            HoldKind::InputAudio => {
                direction == Direction::Downstream && matches!(frame.data, FrameData::InputAudio(_))
            }
        };
        let should_hold = if selected {
            let mut held = self.held.lock().unwrap();
            if held.is_none() {
                *held = Some(frame.clone());
                true
            } else {
                false
            }
        } else {
            false
        };
        if should_hold {
            self.entered.notify_waiters();
            self.released.acquire().await.unwrap().forget();
        }
    }
}
/// Same node/index, same original processor, same original contexts. This
/// delays actual consumption; it neither replaces the consumer nor fabricates
/// an acknowledgment. Lifecycle delegation is deliberately complete.
pub struct HeldProcessor {
    pub inner: Arc<dyn FrameProcessor>,
    pub hold: Arc<Hold>,
}
#[async_trait]
impl FrameProcessor for HeldProcessor {
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
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.hold.before(&frame, direction).await;
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

pub fn history_text(history: &LlmContextData) -> Vec<(ChatRole, String)> {
    history
        .messages
        .iter()
        .map(|message| {
            let MessageContent::Text(text) = &message.content else {
                panic!("unexpected non-text history: {message:?}")
            };
            (message.role, text.clone())
        })
        .collect()
}
pub fn production(frame: &Frame) -> bool {
    matches!(
        frame.data,
        FrameData::LlmResponseStart
            | FrameData::LlmResponseEnd
            | FrameData::LlmText(_)
            | FrameData::LlmThoughtStart
            | FrameData::LlmThoughtText(_)
            | FrameData::LlmThoughtEnd
            | FrameData::OutputAudio(_)
            | FrameData::TtsAudio(_)
            | FrameData::TtsStarted(_)
            | FrameData::TtsStopped(_)
            | FrameData::TtsText(_)
    )
}
pub fn usage(packet: &mut Value, token: u64) {
    packet["usageMetadata"] =
        json!({"promptTokenCount":token,"responseTokenCount":0,"totalTokenCount":token});
}
pub fn is_usage(observation: &Observation, token: u64) -> bool {
    matches!(&observation.frame.data, FrameData::Metrics(metrics)
        if metrics.iter().any(|metric| matches!(metric, Metric::LlmUsage { total_tokens, .. } if *total_tokens == token)))
}
pub fn audio_frame() -> Frame {
    Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(INPUT_PCM.to_vec(), 16_000, 1).unwrap(),
    ))
}
pub fn pipeline_config() -> PipelineConfig {
    let mut config = PipelineConfig {
        startup_timeout: Duration::from_secs(4),
        shutdown_timeout: Duration::from_secs(7),
        ..Default::default()
    };
    config.start.enable_usage_metrics = true;
    config
}
pub async fn join_worker(worker: &mut PipelineWorker) -> Result<WorkerReport, PipelineError> {
    timeout(WAIT, worker.join()).await.unwrap_or_else(|error| {
        panic!(
            "outer PipelineWorker join deadline: {error}; state: {:?}; errors: {:#?}",
            worker.state(),
            worker.errors()
        )
    })
}

/// Test-only observation at a real component invocation. Entry observations do
/// not imply successful consumption; completion observations follow the real
/// delegated call and are still never user-history commit receipts.
pub struct TappedProcessor {
    pub inner: Arc<dyn FrameProcessor>,
    pub context: SharedContext,
    pub state: Arc<ProbeState>,
    pub after: bool,
}
impl TappedProcessor {
    fn observe(&self, frame: &Frame, direction: Direction) -> Result<(), ProcessorError> {
        let observed = Observation {
            frame: frame.clone(),
            direction,
            history: self.context.snapshot_rich()?,
        };
        {
            let mut items = self.state.observations.lock().unwrap();
            if items.len() == TRACE_BOUND {
                return Err(ProcessorError::fatal("fixture component trace bound"));
            }
            items.push(observed);
        }
        self.state.changed.notify_waiters();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for TappedProcessor {
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
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !self.after {
            self.observe(&frame, direction)?;
        }
        let original = frame.clone();
        self.inner.process(frame, direction, ctx).await?;
        if self.after {
            self.observe(&original, direction)?;
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

/// Captures the actual selected user's successful setup context. This permits
/// a service-boundary test to enqueue a delayed local effect from the original
/// user node, without creating a fake receipt or claiming a real timer source.
#[derive(Default)]
pub struct UserOutput {
    context: Mutex<Option<ProcessorContext>>,
}
impl UserOutput {
    pub fn emit(&self, frame: Frame) {
        let context = self
            .context
            .lock()
            .unwrap()
            .clone()
            .expect("actual user setup completed");
        context.try_push(frame, Direction::Downstream).unwrap();
    }
    pub fn is_closed(&self) -> bool {
        self.context.lock().unwrap().is_none()
    }
}
pub struct CapturedUser {
    pub inner: Arc<dyn FrameProcessor>,
    pub output: Arc<UserOutput>,
}
#[async_trait]
impl FrameProcessor for CapturedUser {
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
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await?;
        *self.output.context.lock().unwrap() = Some(ctx.clone());
        Ok(())
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
        self.output.context.lock().unwrap().take();
        self.inner.cleanup().await
    }
}
