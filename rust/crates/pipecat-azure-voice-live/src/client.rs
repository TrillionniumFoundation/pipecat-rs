//! Single owned Voice Live socket actor. No retry, redirect, credential refresh or replay.
use crate::{
    parse_server_event, ConversationItem, ItemKind, ResponseStatus, Role, ServerEvent, TokenUsage,
    VoiceLiveConfig, VoiceLiveError, VoiceLiveLimits,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{Sink, SinkExt, StreamExt};
use pipecat_core::{
    Frame, FrameData, FrameObservationToken, FunctionCall, InvocationFence, LocalFlushEvent,
    NativeToolCallOutcome, ToolCallEndReason, ToolCallTerminalState,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    io::Write,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::Poll,
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, watch},
    task::{AbortHandle, JoinHandle},
    time::{timeout, Instant},
};
use tokio_tungstenite::{
    tungstenite::{self, protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// Immutable-profile actions. Native tool results intentionally have no public variant.
#[derive(Debug, Clone)]
pub enum Action {
    AppendAudio(Vec<u8>),
    CommitAudio,
    ClearAudio,
    CreateItem(ConversationItem),
    CreateResponse,
    SetPrerollBytes(usize),
    Finish,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlaybackPosition {
    pub item_id: String,
    pub content_index: u32,
    pub audio_end_ms: u32,
}
/// Events have passed protocol, identity, byte and generation checks. PCM is decoded once.
#[derive(Debug, Clone)]
pub enum Event {
    Ready,
    ResponseStarted {
        response_id: String,
    },
    ResponseDone {
        response_id: String,
        status: ResponseStatus,
        usage: Option<TokenUsage>,
    },
    Audio {
        response_id: String,
        item_id: String,
        content_index: u32,
        audio: Vec<u8>,
    },
    Text {
        response_id: String,
        item_id: String,
        delta: String,
        audio_transcript: bool,
    },
    Transcript {
        item_id: String,
        text: String,
        is_final: bool,
    },
    TranscriptFailed {
        item_id: String,
    },
    ToolCall {
        response_id: String,
        call: FunctionCall,
    },
    SpeechStarted,
    SpeechStopped,
    ProviderError {
        code: Option<String>,
        message: String,
        fatal: bool,
    },
}
impl Event {
    pub fn response_id(&self) -> Option<&str> {
        match self {
            Self::ResponseStarted { response_id }
            | Self::ResponseDone { response_id, .. }
            | Self::Audio { response_id, .. }
            | Self::Text { response_id, .. }
            | Self::ToolCall { response_id, .. } => Some(response_id),
            _ => None,
        }
    }
}
#[derive(Debug, Clone)]
pub struct VoiceLiveEvent {
    pub event: Event,
    pub generation: u64,
}
#[derive(Clone)]
pub struct VoiceLiveClient {
    config: VoiceLiveConfig,
    native_semantic_admission: bool,
}
impl fmt::Debug for VoiceLiveClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VoiceLiveClient")
            .field("config", &self.config)
            .finish()
    }
}
impl VoiceLiveClient {
    pub fn new(config: VoiceLiveConfig) -> Result<Self, VoiceLiveError> {
        config.validate()?;
        Ok(Self {
            config,
            native_semantic_admission: false,
        })
    }
    pub fn config(&self) -> &VoiceLiveConfig {
        &self.config
    }
    pub(crate) fn require_native_receipts(self) -> Self {
        self
    }
    pub(crate) fn with_native_semantic_admission(mut self) -> Self {
        self.native_semantic_admission = true;
        self
    }
    /// Connect once, wait for session.created → session.update → session.updated.
    /// The returned session owns the actor, including if a setup waiter is cancelled.
    pub async fn connect(&self) -> Result<VoiceLiveSession, VoiceLiveError> {
        self.config.validate()?;
        let c = &self.config;
        let ws_config = WebSocketConfig::default()
            .write_buffer_size(0)
            .max_write_buffer_size(c.limits.max_message_bytes * 2 + 1024)
            .max_message_size(Some(c.limits.max_message_bytes))
            .max_frame_size(Some(c.limits.max_message_bytes));
        let (mut socket, _) = timeout(
            c.limits.connect_timeout,
            tokio_tungstenite::connect_async_with_config(
                c.socket_request()?,
                Some(ws_config),
                true,
            ),
        )
        .await
        .map_err(|_| VoiceLiveError::Timeout("connect"))?
        .map_err(transport)?;
        let (tx, rx) = mpsc::channel(c.limits.queue_capacity);
        let (event_tx, event_rx) = mpsc::channel(c.limits.queue_capacity);
        let (control_tx, control_rx) = watch::channel(Control::default());
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let (ready_tx, mut ready_rx) = watch::channel(false);
        let generation = Arc::new(AtomicU64::new(0));
        let admission = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Mutex::new(()));
        let playback = Arc::new(Mutex::new(PlaybackState::default()));
        let fences = Arc::new(Mutex::new(HashMap::new()));
        let deliveries = Arc::new(Mutex::new(HashMap::new()));
        let semantic_admission = Arc::new(Mutex::new(None));
        let joined = Arc::new(AtomicBool::new(false));
        let cleanup = Arc::new(Mutex::new(None));
        let sender = VoiceLiveSender {
            tx,
            control: control_tx,
            terminal: terminal_rx,
            generation: generation.clone(),
            admission,
            gate: gate.clone(),
            playback: playback.clone(),
            fences: fences.clone(),
            deliveries: deliveries.clone(),
            semantic_admission: semantic_admission.clone(),
            limits: c.limits.clone(),
        };
        let mut actor = Actor::new(
            c.clone(),
            generation,
            gate,
            playback,
            fences,
            deliveries,
            semantic_admission,
            self.native_semantic_admission,
            event_tx,
            ready_tx,
        );
        let actor_cleanup = cleanup.clone();
        let actor_task = tokio::spawn(async move {
            let primary = actor.run(&mut socket, rx, control_rx).await;
            let close = timeout(
                actor.config.limits.close_timeout,
                SinkExt::close(&mut socket),
            )
            .await;
            let secondary = match close {
                Ok(Ok(()))
                | Ok(Err(
                    tungstenite::Error::AlreadyClosed | tungstenite::Error::ConnectionClosed,
                )) => None,
                Ok(Err(error)) => Some(transport(error)),
                Err(_) => Some(VoiceLiveError::Timeout("close handshake")),
            };
            *actor_cleanup.lock().unwrap_or_else(|e| e.into_inner()) = secondary.clone();
            match (primary, secondary) {
                (Err(primary), Some(cleanup)) => Err(VoiceLiveError::Cleanup {
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
                (Err(primary), None) => Err(primary),
                (Ok(()), Some(cleanup)) => Err(cleanup),
                (Ok(()), None) => Ok(()),
            }
        });
        let actor_abort = actor_task.abort_handle();
        let owner_joined = joined.clone();
        let owner = tokio::spawn(async move {
            let result = actor_task.await.unwrap_or(Err(VoiceLiveError::Transport));
            owner_joined.store(true, Ordering::Release);
            terminal_tx.send_replace(Some(result.clone()));
            result
        });
        let mut session = VoiceLiveSession {
            sender,
            rx: event_rx,
            task: Some(owner),
            actor_abort,
            runtime: tokio::runtime::Handle::current(),
            joined,
            cleanup,
            ended: false,
        };
        let ready = timeout(c.limits.connect_timeout * 2 + c.limits.operation_timeout, async {
            loop {
                if *ready_rx.borrow_and_update() { return Ok(()); }
                tokio::select! {
                    result = ready_rx.changed() => if result.is_err() { return session.sender.wait_finished().await.and(Err(VoiceLiveError::Closed)); },
                    result = session.sender.wait_finished() => return result.and(Err(VoiceLiveError::Closed)),
                }
            }
        }).await.unwrap_or(Err(VoiceLiveError::Timeout("session readiness")));
        if let Err(primary) = ready {
            let shutdown = session.shutdown().await.err();
            let cleanup = session
                .cleanup_error()
                .or(shutdown.filter(|error| error != &primary));
            return Err(match cleanup {
                Some(cleanup) if !matches!(primary, VoiceLiveError::Cleanup { .. }) => {
                    VoiceLiveError::Cleanup {
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup),
                    }
                }
                _ => primary,
            });
        }
        Ok(session)
    }
}
#[derive(Clone, Default)]
struct Control {
    generation: u64,
    barrier: u64,
    close: bool,
    playback: Option<PlaybackPosition>,
}
struct Command {
    sequence: u64,
    work: Work,
    generation: u64,
    fence: Option<InvocationFence>,
    reply: Option<oneshot::Sender<Result<(), VoiceLiveError>>>,
}
enum Work {
    Actions(Vec<Action>),
    NativeResult {
        outcome: Arc<NativeToolCallOutcome>,
        output: Value,
    },
}
/// The native pipeline must consume a semantic input boundary before an adjacent
/// server response can bind its generation. Finish may abandon this admission,
/// but retains the opaque identity and never marks abandonment as acknowledgment.
struct SemanticAdmission {
    marker: LocalFlushEvent,
    abandoned_by_finish: bool,
}
/// Private delivery ownership is separate from native execution/settlement proof.
struct ToolDelivery {
    call: FunctionCall,
    generation: u64,
    fence: Option<InvocationFence>,
    stage: ToolDeliveryStage,
}
enum ToolDeliveryStage {
    PendingMap,
    Mapped(FrameObservationToken),
    ExecutorEntered,
    Discarded,
}
#[derive(Default)]
struct PlaybackState {
    parts: HashMap<(String, u32), PlayedPart>,
    latest: Option<(String, u32)>,
}
struct PlayedPart {
    generation: u64,
    generated_bytes: u64,
    acknowledged_ms: Option<u32>,
}
#[derive(Clone)]
pub struct VoiceLiveSender {
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    terminal: watch::Receiver<Option<Result<(), VoiceLiveError>>>,
    generation: Arc<AtomicU64>,
    admission: Arc<AtomicU64>,
    gate: Arc<Mutex<()>>,
    playback: Arc<Mutex<PlaybackState>>,
    fences: Arc<Mutex<HashMap<String, InvocationFence>>>,
    deliveries: Arc<Mutex<HashMap<String, ToolDelivery>>>,
    semantic_admission: Arc<Mutex<Option<SemanticAdmission>>>,
    limits: VoiceLiveLimits,
}
impl fmt::Debug for VoiceLiveSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VoiceLiveSender")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}
impl VoiceLiveSender {
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    pub(crate) fn register_tool_declaration(
        &self,
        frame: &Frame,
        generation: u64,
    ) -> Result<(), VoiceLiveError> {
        let FrameData::FunctionCallsStarted(calls) = &frame.data else {
            return Err(VoiceLiveError::Protocol(
                "tool declaration has wrong payload",
            ));
        };
        let [call] = calls.as_slice() else {
            return Err(VoiceLiveError::Protocol(
                "tool declaration must contain one exact call",
            ));
        };
        let mut deliveries = self
            .deliveries
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("tool delivery lock"))?;
        let delivery = deliveries
            .get_mut(&call.tool_call_id)
            .ok_or(VoiceLiveError::UnknownToolCall)?;
        if delivery.call != *call
            || delivery.generation != generation
            || !matches!(delivery.stage, ToolDeliveryStage::PendingMap)
        {
            return Err(VoiceLiveError::Protocol(
                "tool declaration changed private ownership",
            ));
        }
        delivery.stage = ToolDeliveryStage::Mapped(frame.observation_token());
        Ok(())
    }
    pub(crate) fn discard_unmapped_tool(
        &self,
        call: &FunctionCall,
        generation: u64,
    ) -> Result<(), VoiceLiveError> {
        let mut deliveries = self
            .deliveries
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("tool delivery lock"))?;
        let delivery = deliveries
            .get_mut(&call.tool_call_id)
            .ok_or(VoiceLiveError::UnknownToolCall)?;
        if delivery.call != *call
            || delivery.generation != generation
            || !matches!(delivery.stage, ToolDeliveryStage::PendingMap)
        {
            return Err(VoiceLiveError::Protocol(
                "discard requires provably unmapped tool",
            ));
        }
        delivery.stage = ToolDeliveryStage::Discarded;
        Ok(())
    }
    pub(crate) fn enter_tool_executor(&self, frame: &Frame) -> Result<bool, VoiceLiveError> {
        let FrameData::FunctionCallsStarted(calls) = &frame.data else {
            return Err(VoiceLiveError::Protocol(
                "executor declaration has wrong payload",
            ));
        };
        let [call] = calls.as_slice() else {
            return Err(VoiceLiveError::Protocol(
                "executor declaration must contain one exact call",
            ));
        };
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        let mut deliveries = self
            .deliveries
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("tool delivery lock"))?;
        let delivery = deliveries
            .get_mut(&call.tool_call_id)
            .ok_or(VoiceLiveError::UnknownToolCall)?;
        if delivery.call != *call
            || !matches!(&delivery.stage, ToolDeliveryStage::Mapped(token) if *token == frame.observation_token())
        {
            return Err(VoiceLiveError::Protocol(
                "executor lacks original tool declaration",
            ));
        }
        if delivery.generation != self.generation()
            || delivery.fence.as_ref().is_some_and(|f| !f.is_current())
        {
            delivery.stage = ToolDeliveryStage::Discarded;
            return Ok(false);
        }
        // This conservative marker only forbids inferring nonadmission from
        // frame destruction. It NEVER proves native settlement or wire delivery.
        delivery.stage = ToolDeliveryStage::ExecutorEntered;
        Ok(true)
    }
    pub fn try_send(&self, action: Action) -> Result<(), VoiceLiveError> {
        self.try_send_batch_with_fence(vec![action], None)
    }
    pub(crate) fn try_send_batch_with_fence(
        &self,
        actions: Vec<Action>,
        fence: Option<InvocationFence>,
    ) -> Result<(), VoiceLiveError> {
        validate_actions(&actions, &self.limits)?;
        self.enqueue(Command {
            sequence: 0,
            work: Work::Actions(actions),
            generation: self.generation(),
            fence,
            reply: None,
        })
    }
    fn enqueue(&self, mut command: Command) -> Result<(), VoiceLiveError> {
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.control.borrow().close {
            return Err(VoiceLiveError::Closed);
        }
        let next = self
            .admission
            .load(Ordering::Acquire)
            .checked_add(1)
            .ok_or(VoiceLiveError::TooLarge)?;
        command.sequence = next;
        self.tx.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => VoiceLiveError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => VoiceLiveError::Closed,
        })?;
        self.admission.store(next, Ordering::Release);
        Ok(())
    }
    pub async fn send(&self, action: Action) -> Result<(), VoiceLiveError> {
        validate_actions(std::slice::from_ref(&action), &self.limits)?;
        let (reply, received) = oneshot::channel();
        self.enqueue(Command {
            sequence: 0,
            work: Work::Actions(vec![action]),
            generation: self.generation(),
            fence: None,
            reply: Some(reply),
        })?;
        timeout(self.limits.operation_timeout, received)
            .await
            .map_err(|_| VoiceLiveError::Timeout("action"))?
            .map_err(|_| VoiceLiveError::Closed)?
    }
    /// Terminal bytes and state come only from a validated native carrier, never public payload alone.
    pub(crate) fn try_send_native_result(&self, frame: &Frame) -> Result<(), VoiceLiveError> {
        frame
            .native_tool_batch()
            .map_err(|_| VoiceLiveError::Invalid("invalid native batch"))?;
        let outcome = frame
            .native_tool_outcome()
            .map_err(|_| VoiceLiveError::Invalid("invalid native terminal carrier"))?
            .ok_or(VoiceLiveError::Invalid("native terminal receipt required"))?;
        if !outcome.cancel_on_interruption()
            || frame.has_native_tool_progress_restriction()
            || frame
                .native_tool_progress()
                .map_err(|_| VoiceLiveError::Invalid("invalid tool progress"))?
                .is_some()
        {
            return Err(VoiceLiveError::Invalid(
                "only final interruptible native tools are supported",
            ));
        }
        if outcome.retained_bytes() > self.limits.max_message_bytes {
            return Err(VoiceLiveError::TooLarge);
        }
        let output = match &frame.data {
            FrameData::FunctionCallResult(result) if result.is_final => result.result.clone(),
            FrameData::FunctionCallCancel(_) => json!({"status":"cancelled"}),
            _ => return Err(VoiceLiveError::Invalid("native terminal payload family")),
        };
        bounded_serialized(&output, self.limits.max_message_bytes)?;
        self.enqueue(Command {
            sequence: 0,
            work: Work::NativeResult {
                outcome: outcome.clone(),
                output,
            },
            generation: self.generation(),
            fence: None,
            reply: None,
        })
    }
    pub fn acknowledge_playback(&self, position: PlaybackPosition) -> Result<(), VoiceLiveError> {
        let mut playback = self
            .playback
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("playback lock"))?;
        let part = playback
            .parts
            .get_mut(&(position.item_id, position.content_index))
            .ok_or(VoiceLiveError::Invalid("unknown playback item"))?;
        let generated_ms = part.generated_bytes / 48;
        if part.generation != self.generation()
            || u64::from(position.audio_end_ms) > generated_ms
            || part
                .acknowledged_ms
                .is_some_and(|ms| position.audio_end_ms < ms)
        {
            return Err(VoiceLiveError::Invalid(
                "playback position must be current, monotonic and generated",
            ));
        }
        part.acknowledged_ms = Some(position.audio_end_ms);
        Ok(())
    }
    pub fn interrupt(&self, playback: Option<PlaybackPosition>) {
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.control.send_modify(|c| {
            c.generation = generation;
            c.barrier = self.admission.load(Ordering::Acquire);
            c.playback = playback;
        });
    }
    pub fn close(&self) {
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.control.send_modify(|c| c.close = true);
    }
    pub async fn finish(&self) -> Result<(), VoiceLiveError> {
        self.send(Action::Finish).await
    }
    /// Published only by the supervisor after the real socket actor joins.
    pub async fn wait_finished(&self) -> Result<(), VoiceLiveError> {
        let mut terminal = self.terminal.clone();
        loop {
            if let Some(result) = terminal.borrow_and_update().clone() {
                return result;
            }
            terminal
                .changed()
                .await
                .map_err(|_| VoiceLiveError::Closed)?;
        }
    }
    /// Only the owned native processor requests this marker; no public action
    /// or origin string can release semantic socket admission.
    pub(crate) fn semantic_admission_barrier(
        &self,
    ) -> Result<Option<LocalFlushEvent>, VoiceLiveError> {
        self.semantic_admission
            .lock()
            .map(|pending| {
                pending
                    .as_ref()
                    .filter(|p| !p.abandoned_by_finish)
                    .map(|p| p.marker.clone())
            })
            .map_err(|_| VoiceLiveError::Protocol("semantic admission lock"))
    }
    pub(crate) fn acknowledge_semantic_barrier(
        &self,
        marker: &LocalFlushEvent,
    ) -> Result<bool, VoiceLiveError> {
        let pending = self
            .semantic_admission
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("semantic admission lock"))?;
        if pending.as_ref().is_some_and(|p| p.marker == *marker) {
            if !pending
                .as_ref()
                .expect("matched semantic marker")
                .abandoned_by_finish
            {
                marker.set();
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }
    pub(crate) fn response_fence(&self, response_id: &str) -> Option<InvocationFence> {
        self.fences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(response_id)
            .cloned()
    }
    pub(crate) fn with_current_generation<T>(
        &self,
        generation: u64,
        run: impl FnOnce() -> T,
    ) -> Option<T> {
        let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
        (self.generation() == generation).then(run)
    }
    pub(crate) async fn while_current<F: Future>(
        &self,
        generation: u64,
        future: F,
    ) -> Option<F::Output> {
        tokio::pin!(future);
        futures_util::future::poll_fn(|cx| {
            let _guard = self.gate.lock().unwrap_or_else(|e| e.into_inner());
            if self.generation() != generation {
                Poll::Ready(None)
            } else {
                future.as_mut().poll(cx).map(Some)
            }
        })
        .await
    }
}
pub struct VoiceLiveSession {
    sender: VoiceLiveSender,
    rx: mpsc::Receiver<VoiceLiveEvent>,
    task: Option<JoinHandle<Result<(), VoiceLiveError>>>,
    actor_abort: AbortHandle,
    runtime: tokio::runtime::Handle,
    joined: Arc<AtomicBool>,
    cleanup: Arc<Mutex<Option<VoiceLiveError>>>,
    ended: bool,
}
impl VoiceLiveSession {
    pub fn sender(&self) -> VoiceLiveSender {
        self.sender.clone()
    }
    pub fn session_joined(&self) -> bool {
        self.joined.load(Ordering::Acquire)
    }
    pub(crate) fn join_observer(&self) -> Arc<AtomicBool> {
        self.joined.clone()
    }
    pub fn cleanup_error(&self) -> Option<VoiceLiveError> {
        self.cleanup
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub async fn next_event(&mut self) -> Option<Result<VoiceLiveEvent, VoiceLiveError>> {
        if !self.ended {
            while let Some(event) = self.rx.recv().await {
                if event.event.response_id().is_some()
                    && event.generation != self.sender.generation()
                {
                    if let Event::ToolCall { call, .. } = &event.event {
                        if let Err(error) =
                            self.sender.discard_unmapped_tool(call, event.generation)
                        {
                            return Some(Err(error));
                        }
                    }
                    continue;
                }
                return Some(Ok(event));
            }
            self.ended = true;
        }
        let result = self.task.as_mut()?.await;
        self.task.take();
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(VoiceLiveError::Transport)),
        }
    }
    pub async fn shutdown(&mut self) -> Result<(), VoiceLiveError> {
        self.sender.close();
        self.rx.close();
        self.ended = true;
        let Some(task) = self.task.as_mut() else {
            return self.sender.terminal.borrow().clone().unwrap_or(Ok(()));
        };
        let result = match timeout(
            self.sender.limits.close_timeout + self.sender.limits.operation_timeout,
            &mut *task,
        )
        .await
        {
            Ok(result) => result.unwrap_or(Err(VoiceLiveError::Transport)),
            Err(_) => {
                self.actor_abort.abort();
                let _ = task.await;
                Err(VoiceLiveError::Timeout("close"))
            }
        };
        self.task.take();
        result
    }
}
impl Drop for VoiceLiveSession {
    fn drop(&mut self) {
        self.sender.close();
        if let Some(mut task) = self.task.take() {
            let abort = self.actor_abort.clone();
            let bound = self.sender.limits.close_timeout + self.sender.limits.operation_timeout;
            self.runtime.spawn(async move {
                if timeout(bound, &mut task).await.is_err() {
                    abort.abort();
                    let _ = task.await;
                }
            });
        }
    }
}
struct Budget(usize);
impl Write for Budget {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0 = self
            .0
            .checked_sub(bytes.len())
            .ok_or_else(|| std::io::Error::other("encoded bound"))?;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
pub(crate) fn bounded_serialized(
    value: &impl Serialize,
    limit: usize,
) -> Result<(), VoiceLiveError> {
    serde_json::to_writer(Budget(limit), value).map_err(|_| VoiceLiveError::TooLarge)
}
fn validate_actions(actions: &[Action], limits: &VoiceLiveLimits) -> Result<(), VoiceLiveError> {
    if actions.is_empty() || actions.len() > limits.queue_capacity {
        return Err(VoiceLiveError::TooLarge);
    }
    let mut total = 0usize;
    for action in actions {
        match action {
            Action::AppendAudio(audio) => {
                if audio.is_empty() || !audio.len().is_multiple_of(2) {
                    return Err(VoiceLiveError::Invalid(
                        "audio must be nonempty aligned PCM16",
                    ));
                }
                if audio.len() > limits.max_audio_chunk_bytes {
                    return Err(VoiceLiveError::TooLarge);
                }
                total = total
                    .checked_add(audio.len())
                    .ok_or(VoiceLiveError::TooLarge)?;
            }
            Action::CreateItem(item) => {
                if item.kind != ItemKind::Message
                    || item.role != Some(Role::User)
                    || item.call_id.is_some()
                    || item.name.is_some()
                    || item.arguments.is_some()
                    || item.output.is_some()
                    || item.content.as_ref().is_none_or(|parts| {
                        parts.iter().any(|part| {
                            part.kind != crate::ContentKind::InputText
                                || part.audio.is_some()
                                || part.transcript.is_some()
                        })
                    })
                {
                    return Err(VoiceLiveError::Invalid(
                        "client items require ordinary user input text",
                    ));
                }
                bounded_serialized(item, limits.max_message_bytes)?;
                total = total
                    .checked_add(
                        serde_json::to_vec(item)
                            .map_err(|_| VoiceLiveError::TooLarge)?
                            .len(),
                    )
                    .ok_or(VoiceLiveError::TooLarge)?;
            }
            Action::SetPrerollBytes(bytes)
                if *bytes > limits.max_preroll_ms as usize * 48 || !bytes.is_multiple_of(2) =>
            {
                return Err(VoiceLiveError::Invalid(
                    "preroll must be aligned and within the configured duration",
                ))
            }
            _ => {}
        }
        if total > limits.max_message_bytes {
            return Err(VoiceLiveError::TooLarge);
        }
    }
    Ok(())
}
fn transport(error: tungstenite::Error) -> VoiceLiveError {
    match error {
        tungstenite::Error::Http(response) => VoiceLiveError::Http(response.status().as_u16()),
        tungstenite::Error::Capacity(_) => VoiceLiveError::TooLarge,
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            VoiceLiveError::Closed
        }
        _ => VoiceLiveError::Transport,
    }
}

#[derive(Clone)]
struct ResponseScope {
    generation: u64,
    fence: Option<InvocationFence>,
    interrupted: bool,
    started: bool,
    done: bool,
}
struct PendingResponse {
    request_id: String,
    scope: ResponseScope,
}
struct ItemState {
    response_id: String,
    output_index: Option<u32>,
    kind: ItemKind,
    call_id: Option<String>,
    name: Option<String>,
    initial_arguments: Option<String>,
}
struct ToolState {
    response_id: String,
    item_id: String,
    name: String,
    initial_arguments: Option<String>,
    call: Option<FunctionCall>,
    deadline: Instant,
}
#[derive(Default)]
struct TranscriptState {
    interim: String,
    final_text: Option<String>,
    failed: bool,
}
struct Actor {
    config: VoiceLiveConfig,
    generation: Arc<AtomicU64>,
    gate: Arc<Mutex<()>>,
    playback: Arc<Mutex<PlaybackState>>,
    fences: Arc<Mutex<HashMap<String, InvocationFence>>>,
    deliveries: Arc<Mutex<HashMap<String, ToolDelivery>>>,
    semantic_admission: Arc<Mutex<Option<SemanticAdmission>>>,
    native_semantic_admission: bool,
    semantic_deadline: Option<Instant>,
    tx: mpsc::Sender<VoiceLiveEvent>,
    ready_tx: watch::Sender<bool>,
    stage: u8,
    ready_deadline: Instant,
    update_request: Option<String>,
    sequence: u64,
    active: Option<String>,
    pending: Option<PendingResponse>,
    responses: HashMap<String, ResponseScope>,
    items: HashMap<String, ItemState>,
    tools: HashMap<String, ToolState>,
    settled: HashMap<String, FunctionCall>,
    transcripts: HashMap<String, TranscriptState>,
    unknown_events: usize,
    response_deadline: Option<Instant>,
    debt: Option<(u64, Option<InvocationFence>)>,
    finishing: bool,
    drain_deadline: Option<Instant>,
    preroll: VecDeque<u8>,
    preroll_bytes: usize,
    input_pending: bool,
    omitted_after_interrupt: bool,
    last_command: u64,
    handled_generation: u64,
    retained_bytes: usize,
}
impl Actor {
    #[allow(clippy::too_many_arguments)] // Explicitly transferred channels and shared ownership.
    fn new(
        config: VoiceLiveConfig,
        generation: Arc<AtomicU64>,
        gate: Arc<Mutex<()>>,
        playback: Arc<Mutex<PlaybackState>>,
        fences: Arc<Mutex<HashMap<String, InvocationFence>>>,
        deliveries: Arc<Mutex<HashMap<String, ToolDelivery>>>,
        semantic_admission: Arc<Mutex<Option<SemanticAdmission>>>,
        native_semantic_admission: bool,
        tx: mpsc::Sender<VoiceLiveEvent>,
        ready_tx: watch::Sender<bool>,
    ) -> Self {
        let ready_deadline = Instant::now() + config.limits.connect_timeout;
        Self {
            config,
            generation,
            gate,
            playback,
            fences,
            deliveries,
            semantic_admission,
            native_semantic_admission,
            semantic_deadline: None,
            tx,
            ready_tx,
            stage: 0,
            ready_deadline,
            update_request: None,
            sequence: 0,
            active: None,
            pending: None,
            responses: HashMap::new(),
            items: HashMap::new(),
            tools: HashMap::new(),
            settled: HashMap::new(),
            transcripts: HashMap::new(),
            unknown_events: 0,
            response_deadline: None,
            debt: None,
            finishing: false,
            drain_deadline: None,
            preroll: VecDeque::new(),
            preroll_bytes: 24_000,
            input_pending: false,
            omitted_after_interrupt: false,
            last_command: 0,
            handled_generation: 0,
            retained_bytes: 0,
        }
    }
    fn epoch(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    fn next_id(&mut self) -> Result<String, VoiceLiveError> {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(VoiceLiveError::TooLarge)?;
        Ok(format!("pc_vl_{}", self.sequence))
    }
    async fn write(
        &mut self,
        socket: &mut Socket,
        mut value: Value,
    ) -> Result<String, VoiceLiveError> {
        let id = self.next_id()?;
        value["event_id"] = json!(id);
        bounded_serialized(&value, self.config.limits.max_message_bytes)?;
        let wire = serde_json::to_string(&value)
            .map_err(|_| VoiceLiveError::Invalid("event serialization"))?;
        timeout(
            self.config.limits.operation_timeout,
            socket.send(Message::Text(wire.into())),
        )
        .await
        .map_err(|_| VoiceLiveError::Timeout("write"))?
        .map_err(transport)?;
        Ok(id)
    }
    async fn write_current(
        &mut self,
        socket: &mut Socket,
        mut value: Value,
        generation: u64,
        fence: Option<&InvocationFence>,
    ) -> Result<Option<String>, VoiceLiveError> {
        let id = self.next_id()?;
        value["event_id"] = json!(id);
        bounded_serialized(&value, self.config.limits.max_message_bytes)?;
        let wire = serde_json::to_string(&value)
            .map_err(|_| VoiceLiveError::Invalid("event serialization"))?;
        let admitted = timeout(
            self.config.limits.operation_timeout,
            send_current(
                socket,
                Message::Text(wire.into()),
                &self.generation,
                generation,
                &self.gate,
                fence,
            ),
        )
        .await
        .map_err(|_| VoiceLiveError::Timeout("write"))??;
        Ok(admitted.then_some(id))
    }
    async fn emit(&self, event: Event, generation: u64) -> Result<(), VoiceLiveError> {
        timeout(
            self.config.limits.operation_timeout,
            self.tx.send(VoiceLiveEvent { event, generation }),
        )
        .await
        .map_err(|_| VoiceLiveError::Timeout("output queue"))?
        .map_err(|_| VoiceLiveError::Closed)
    }
    async fn run(
        &mut self,
        socket: &mut Socket,
        mut commands: mpsc::Receiver<Command>,
        mut control: watch::Receiver<Control>,
    ) -> Result<(), VoiceLiveError> {
        loop {
            if self.retire_undelivered_tools()? {
                self.maybe_continue(socket).await?;
            }
            if self.finishing
                && self.active.is_none()
                && self.pending.is_none()
                && self.tools.is_empty()
            {
                return Ok(());
            }
            let semantic_barrier = self
                .semantic_admission
                .lock()
                .map_err(|_| VoiceLiveError::Protocol("semantic admission lock"))?
                .as_ref()
                .filter(|p| !p.abandoned_by_finish)
                .map(|p| p.marker.clone());
            let far = Instant::now() + Duration::from_secs(86_400);
            let tool_deadline = self
                .tools
                .values()
                .map(|tool| tool.deadline)
                .min()
                .unwrap_or(far);
            tokio::select! { biased;
                changed = control.changed() => {
                    if changed.is_err() { return Ok(()); }
                    let value = control.borrow_and_update().clone();
                    if value.close { return Ok(()); }
                    self.debt = None;
                    while self.last_command < value.barrier {
                        if control.borrow().close { return Ok(()); }
                        let command = commands.try_recv().map_err(|_| VoiceLiveError::Protocol("interruption admission barrier lost"))?;
                        self.last_command = command.sequence;
                        let result = match command.work {
                            Work::NativeResult { outcome, output } => self.native_result(socket, outcome, output).await,
                            Work::Actions(actions) => {
                                for action in actions { match action {
                                    Action::AppendAudio(audio) => self.append_admitted(socket, audio).await?,
                                    Action::SetPrerollBytes(bytes) => self.resize_preroll(bytes),
                                    Action::Finish => self.begin_finish()?,
                                    _ => {},
                                } }
                                Err(VoiceLiveError::Interrupted)
                            }
                        };
                        if let Some(reply) = command.reply { let _ = reply.send(result.clone()); }
                        if result.is_err() && result != Err(VoiceLiveError::Interrupted) { return result; }
                    }
                    self.handled_generation = value.generation;
                    self.interrupt(socket, value).await?;
                }
                _ = tokio::time::sleep_until(self.ready_deadline), if self.stage < 2 => return Err(VoiceLiveError::Timeout("session readiness")),
                _ = tokio::time::sleep_until(self.response_deadline.unwrap_or(far)), if self.response_deadline.is_some() => return Err(VoiceLiveError::Timeout("response")),
                _ = tokio::time::sleep_until(self.drain_deadline.unwrap_or(far)), if self.drain_deadline.is_some() => return Err(VoiceLiveError::Timeout("final drain")),
                _ = tokio::time::sleep_until(tool_deadline), if !self.tools.is_empty() => return Err(VoiceLiveError::Timeout("tool settlement")),
                _ = tokio::time::sleep(Duration::from_millis(10)), if !self.tools.is_empty() => {},
                _ = tokio::time::sleep_until(self.semantic_deadline.unwrap_or(far)), if self.semantic_deadline.is_some() => return Err(VoiceLiveError::Timeout("native semantic admission")),
                _ = async { semantic_barrier.as_ref().expect("pending semantic marker").wait().await }, if semantic_barrier.is_some() && self.handled_generation == self.epoch() => {
                    {
                        let mut pending = self.semantic_admission.lock().map_err(|_| VoiceLiveError::Protocol("semantic admission lock"))?;
                        if pending.as_ref().map(|p| &p.marker) != semantic_barrier.as_ref() { return Err(VoiceLiveError::Protocol("semantic marker replaced")); }
                        pending.take();
                    }
                    self.semantic_deadline = None;
                    self.maybe_continue(socket).await?;
                }
                // Keep controls and actual native terminal writes live, but do not
                // bind any later wire event before the native boundary is admitted.
                item = socket.next(), if semantic_barrier.is_none() => match item {
                    Some(Ok(Message::Text(text))) => {
                        let event = parse_server_event(text.as_bytes(), self.config.limits.max_message_bytes)?;
                        self.event(socket, event).await?;
                    }
                    Some(Ok(Message::Ping(bytes))) => { timeout(self.config.limits.operation_timeout, socket.send(Message::Pong(bytes))).await.map_err(|_| VoiceLiveError::Timeout("pong"))?.map_err(transport)?; }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) | None => return Err(if !self.tools.is_empty() { VoiceLiveError::UnsettledTools } else { VoiceLiveError::Closed }),
                    Some(Ok(_)) => return Err(VoiceLiveError::Protocol("expected JSON text")),
                    Some(Err(error)) => return Err(transport(error)),
                },
                command = commands.recv(), if self.stage == 2 => {
                    let Some(command) = command else { return Ok(()); };
                    self.last_command = command.sequence;
                    let result = self.command(socket, command.work, command.generation, command.fence).await;
                    let fatal = matches!(result, Err(VoiceLiveError::Transport | VoiceLiveError::Closed | VoiceLiveError::Timeout(_) | VoiceLiveError::Protocol(_) | VoiceLiveError::UnknownToolCall));
                    let failed = result.clone(); let acknowledged = command.reply.is_some();
                    if let Some(reply) = command.reply { let _ = reply.send(result); }
                    if fatal || (!acknowledged && failed.is_err() && failed != Err(VoiceLiveError::Interrupted)) { return failed; }
                }
            }
        }
    }
    async fn command(
        &mut self,
        socket: &mut Socket,
        work: Work,
        generation: u64,
        fence: Option<InvocationFence>,
    ) -> Result<(), VoiceLiveError> {
        match work {
            Work::NativeResult { outcome, output } => {
                self.native_result(socket, outcome, output).await
            }
            Work::Actions(actions) => {
                for action in actions {
                    if matches!(action, Action::Finish) {
                        self.begin_finish()?;
                        continue;
                    }
                    if self.finishing {
                        return Err(VoiceLiveError::Invalid("session is draining"));
                    }
                    if let Action::AppendAudio(audio) = action {
                        // Already admitted caller PCM precedes a later interruption's clear/replay.
                        if generation < self.handled_generation {
                            return Err(VoiceLiveError::Interrupted);
                        }
                        self.append_admitted(socket, audio).await?;
                        continue;
                    }
                    if generation != self.epoch() || fence.as_ref().is_some_and(|f| !f.is_current())
                    {
                        return Err(VoiceLiveError::Interrupted);
                    }
                    match action {
                        Action::AppendAudio(_) => unreachable!("handled admitted PCM"),
                        Action::CommitAudio => {
                            if !self.config.session.is_manual() {
                                return Err(VoiceLiveError::Invalid(
                                    "manual commit is disabled in semantic VAD",
                                ));
                            }
                            if self.input_pending {
                                self.write_current(
                                    socket,
                                    json!({"type":"input_audio_buffer.commit"}),
                                    generation,
                                    fence.as_ref(),
                                )
                                .await?
                                .ok_or(VoiceLiveError::Interrupted)?;
                                self.input_pending = false;
                            }
                        }
                        Action::ClearAudio => {
                            self.write_current(
                                socket,
                                json!({"type":"input_audio_buffer.clear"}),
                                generation,
                                fence.as_ref(),
                            )
                            .await?
                            .ok_or(VoiceLiveError::Interrupted)?;
                            self.input_pending = false;
                            self.preroll.clear();
                        }
                        Action::CreateItem(item) => {
                            self.write_current(
                                socket,
                                json!({"type":"conversation.item.create","item":item}),
                                generation,
                                fence.as_ref(),
                            )
                            .await?
                            .ok_or(VoiceLiveError::Interrupted)?;
                        }
                        Action::CreateResponse => {
                            self.debt = Some((generation, fence.clone()));
                            self.maybe_continue(socket).await?;
                        }
                        Action::SetPrerollBytes(bytes) => self.resize_preroll(bytes),
                        Action::Finish => unreachable!("handled before generation checks"),
                    }
                }
                Ok(())
            }
        }
    }
    fn begin_finish(&mut self) -> Result<(), VoiceLiveError> {
        self.finishing = true;
        self.debt = None;
        self.drain_deadline = Some(Instant::now() + self.config.limits.response_timeout);
        // End's gate can hold the normal lane needed by the semantic round trip.
        // Retire only admission, never claim an acknowledgment/native settlement.
        if let Some(pending) = self
            .semantic_admission
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("semantic admission lock"))?
            .as_mut()
        {
            pending.abandoned_by_finish = true;
        }
        self.semantic_deadline = None;
        Ok(())
    }
    fn begin_semantic_admission(&mut self) -> Result<(), VoiceLiveError> {
        if self.native_semantic_admission {
            let mut pending = self
                .semantic_admission
                .lock()
                .map_err(|_| VoiceLiveError::Protocol("semantic admission lock"))?;
            if pending.is_some() {
                return Err(VoiceLiveError::Protocol("overlapping semantic admission"));
            }
            *pending = Some(SemanticAdmission {
                marker: LocalFlushEvent::default(),
                abandoned_by_finish: false,
            });
            self.semantic_deadline = Some(Instant::now() + self.config.limits.operation_timeout);
        }
        Ok(())
    }
    fn resize_preroll(&mut self, bytes: usize) {
        self.preroll_bytes = bytes;
        while self.preroll.len() > bytes {
            self.preroll.pop_front();
        }
    }
    fn retain(&mut self, bytes: usize) -> Result<(), VoiceLiveError> {
        self.retained_bytes = self
            .retained_bytes
            .checked_add(bytes)
            .filter(|n| *n <= self.config.limits.max_message_bytes * 4)
            .ok_or(VoiceLiveError::TooLarge)?;
        Ok(())
    }
    async fn append_admitted(
        &mut self,
        socket: &mut Socket,
        audio: Vec<u8>,
    ) -> Result<(), VoiceLiveError> {
        self.write(
            socket,
            json!({"type":"input_audio_buffer.append","audio":STANDARD.encode(&audio)}),
        )
        .await?;
        self.input_pending = true;
        if self.config.session.is_manual() {
            self.preroll.extend(audio);
            self.resize_preroll(self.preroll_bytes);
        }
        Ok(())
    }
    async fn maybe_continue(&mut self, socket: &mut Socket) -> Result<(), VoiceLiveError> {
        if self.finishing
            || self.semantic_deadline.is_some()
            || self.active.is_some()
            || self.pending.is_some()
            || !self.tools.is_empty()
        {
            return Ok(());
        }
        let Some((generation, fence)) = self.debt.take() else {
            return Ok(());
        };
        if generation != self.epoch() || fence.as_ref().is_some_and(|f| !f.is_current()) {
            return Ok(());
        }
        if self.responses.len() >= self.config.limits.max_session_responses {
            return Err(VoiceLiveError::TooLarge);
        }
        if let Some(request_id) = self
            .write_current(
                socket,
                json!({"type":"response.create"}),
                generation,
                fence.as_ref(),
            )
            .await?
        {
            self.pending = Some(PendingResponse {
                request_id,
                scope: ResponseScope {
                    generation,
                    fence,
                    interrupted: false,
                    started: false,
                    done: false,
                },
            });
            self.response_deadline = Some(Instant::now() + self.config.limits.response_timeout);
        }
        Ok(())
    }
    fn retire_undelivered_tools(&mut self) -> Result<bool, VoiceLiveError> {
        let epoch = self.epoch();
        let mut deliveries = self
            .deliveries
            .lock()
            .map_err(|_| VoiceLiveError::Protocol("tool delivery lock"))?;
        let before = self.tools.len();
        self.tools.retain(|id, _| {
            let Some(delivery) = deliveries.get_mut(id) else {
                return true;
            };
            let stale = delivery.generation != epoch
                || delivery.fence.as_ref().is_some_and(|f| !f.is_current());
            if stale
                && matches!(&delivery.stage, ToolDeliveryStage::Mapped(token) if !token.is_alive())
            {
                delivery.stage = ToolDeliveryStage::Discarded;
            }
            !matches!(delivery.stage, ToolDeliveryStage::Discarded)
        });
        Ok(before != self.tools.len())
    }
    async fn native_result(
        &mut self,
        socket: &mut Socket,
        outcome: Arc<NativeToolCallOutcome>,
        output: Value,
    ) -> Result<(), VoiceLiveError> {
        let call = outcome.call();
        let tool = self
            .tools
            .get(&call.tool_call_id)
            .ok_or(VoiceLiveError::UnknownToolCall)?;
        if tool.call.as_ref() != Some(call) || !outcome.cancel_on_interruption() {
            return Err(VoiceLiveError::Protocol(
                "native result does not match admitted final call",
            ));
        }
        // A durable native terminal already admitted before interruption keeps
        // its sealed state. Generation fences inference/output, not settlement.
        let projected = match outcome.state() {
            ToolCallTerminalState::Completed => match output { Value::String(text) => text, other => serde_json::to_string(&other).map_err(|_| VoiceLiveError::TooLarge)? },
            ToolCallTerminalState::Failed => serde_json::to_string(&json!({"status":"failed","reason":if outcome.reason() == ToolCallEndReason::TimedOut {"timed_out"} else {"callback_failed"}})).map_err(|_| VoiceLiveError::TooLarge)?,
            ToolCallTerminalState::Cancelled => "{\"status\":\"cancelled\"}".into(),
        };
        let item = ConversationItem::function_output(&call.tool_call_id, projected);
        self.write(
            socket,
            json!({"type":"conversation.item.create","item":item}),
        )
        .await?;
        let id = call.tool_call_id.clone();
        self.tools.remove(&id);
        self.settled.insert(id, call.clone());
        self.maybe_continue(socket).await
    }
    async fn interrupt(
        &mut self,
        socket: &mut Socket,
        control: Control,
    ) -> Result<(), VoiceLiveError> {
        self.omitted_after_interrupt = true;
        self.debt = None;
        if self.config.session.is_manual() {
            self.write(socket, json!({"type":"input_audio_buffer.clear"}))
                .await?;
            self.input_pending = false;
            if !self.preroll.is_empty() {
                let bytes: Vec<u8> = self.preroll.iter().copied().collect();
                for chunk in bytes.chunks(self.config.limits.max_audio_chunk_bytes) {
                    self.write(
                        socket,
                        json!({"type":"input_audio_buffer.append","audio":STANDARD.encode(chunk)}),
                    )
                    .await?;
                }
                self.input_pending = true;
            }
        }
        if self.active.is_some() || self.pending.is_some() {
            self.write(socket, json!({"type":"response.cancel"}))
                .await?;
            if let Some(id) = &self.active {
                if let Some(scope) = self.responses.get_mut(id) {
                    scope.interrupted = true;
                }
            }
            if let Some(pending) = &mut self.pending {
                pending.scope.interrupted = true;
            }
        }
        let position = {
            let playback = self
                .playback
                .lock()
                .map_err(|_| VoiceLiveError::Protocol("playback lock"))?;
            let key = control
                .playback
                .as_ref()
                .map(|p| (p.item_id.clone(), p.content_index))
                .or_else(|| playback.latest.clone());
            if let Some(key) = key {
                let part = playback.parts.get(&key).ok_or(VoiceLiveError::Invalid(
                    "unknown interruption playback item",
                ))?;
                let supplied = control.playback.as_ref().map(|p| p.audio_end_ms);
                let end = supplied.or(part.acknowledged_ms);
                if let Some(ms) = end {
                    if supplied.is_some() && part.acknowledged_ms.is_none_or(|ack| ms > ack) {
                        return Err(VoiceLiveError::Invalid(
                            "interruption requires an actual playback acknowledgment",
                        ));
                    }
                    if u64::from(ms) > part.generated_bytes / 48
                        || part.generation >= control.generation
                    {
                        return Err(VoiceLiveError::Invalid(
                            "invalid interruption playback position",
                        ));
                    }
                    Some(PlaybackPosition {
                        item_id: key.0,
                        content_index: key.1,
                        audio_end_ms: ms,
                    })
                } else {
                    None
                }
            } else {
                None
            }
        };
        if let Some(position) = position {
            self.write(socket, json!({"type":"conversation.item.truncate","item_id":position.item_id,"content_index":position.content_index,"audio_end_ms":position.audio_end_ms})).await?;
        }
        self.tools.retain(|_, tool| tool.call.is_some());
        Ok(())
    }
    fn response(
        &self,
        id: Option<&str>,
    ) -> Result<Option<(String, ResponseScope)>, VoiceLiveError> {
        if id.is_none() && self.omitted_after_interrupt {
            return Err(VoiceLiveError::Protocol(
                "omitted response identity after interruption",
            ));
        }
        let id = id
            .or(self.active.as_deref())
            .ok_or(VoiceLiveError::Protocol("response has not been announced"))?;
        let scope = self
            .responses
            .get(id)
            .ok_or(VoiceLiveError::Protocol("unknown response identity"))?;
        if scope.interrupted
            || scope.generation != self.epoch()
            || scope.fence.as_ref().is_some_and(|f| !f.is_current())
        {
            return Ok(None);
        }
        if scope.done || self.active.as_deref() != Some(id) {
            return Err(VoiceLiveError::Protocol("output after response completion"));
        }
        Ok(Some((id.to_owned(), scope.clone())))
    }
    fn item(
        &self,
        response_id: &str,
        item_id: Option<&str>,
        required: ItemKind,
    ) -> Result<String, VoiceLiveError> {
        let id = match item_id {
            Some(id) => id.to_owned(),
            None => {
                if self.omitted_after_interrupt {
                    return Err(VoiceLiveError::Protocol(
                        "omitted item identity after interruption",
                    ));
                }
                let mut candidates = self
                    .items
                    .iter()
                    .filter(|(_, item)| item.response_id == response_id && item.kind == required);
                let Some((id, _)) = candidates.next() else {
                    return Err(VoiceLiveError::Protocol("item has not been announced"));
                };
                if candidates.next().is_some() {
                    return Err(VoiceLiveError::Protocol("ambiguous omitted item identity"));
                }
                id.clone()
            }
        };
        let item = self
            .items
            .get(&id)
            .ok_or(VoiceLiveError::Protocol("unknown item identity"))?;
        if item.response_id != response_id || item.kind != required {
            return Err(VoiceLiveError::Protocol(
                "contradictory response item identity",
            ));
        }
        Ok(id)
    }
    fn known_call(&self, id: &str) -> Option<FunctionCall> {
        self.tools
            .get(id)
            .and_then(|tool| tool.call.clone())
            .or_else(|| self.settled.get(id).cloned())
            .or_else(|| {
                self.deliveries
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .get(id)
                    .map(|delivery| delivery.call.clone())
            })
    }
    fn validate_item_identity(
        &self,
        response_id: &str,
        item: &ConversationItem,
    ) -> Result<(), VoiceLiveError> {
        let id = item.id.as_ref().ok_or(VoiceLiveError::Protocol(
            "completed output item requires identity",
        ))?;
        let known = self.items.get(id).ok_or(VoiceLiveError::Protocol(
            "completed output contains an unannounced item",
        ))?;
        if known.response_id != response_id
            || known.kind != item.kind
            || known.call_id != item.call_id
            || known.name != item.name
        {
            return Err(VoiceLiveError::Protocol(
                "completed output item identity changed",
            ));
        }
        if item.kind == ItemKind::Message && item.role != Some(Role::Assistant) {
            return Err(VoiceLiveError::Protocol("completed assistant role changed"));
        }
        if let Some(arguments) = item.arguments.as_ref().filter(|value| !value.is_empty()) {
            let call_id = item
                .call_id
                .as_ref()
                .ok_or(VoiceLiveError::Protocol("completed arguments lack call"))?;
            let call = self.known_call(call_id).ok_or(VoiceLiveError::Protocol(
                "completed output has unadmitted arguments",
            ))?;
            if serde_json::from_str::<Value>(arguments).ok().as_ref() != Some(&call.arguments) {
                return Err(VoiceLiveError::Protocol(
                    "completed output arguments changed",
                ));
            }
        }
        Ok(())
    }
    async fn announce(
        &mut self,
        item: ConversationItem,
        response_id: Option<String>,
        output_index: Option<u32>,
    ) -> Result<(), VoiceLiveError> {
        if item.kind == ItemKind::Message && item.role != Some(Role::Assistant) {
            return Ok(());
        }
        if item.kind == ItemKind::FunctionCallOutput {
            return Ok(());
        }
        let Some((response_id, scope)) = self.response(response_id.as_deref())? else {
            return Ok(());
        };
        let item_id = item.id.clone().ok_or(VoiceLiveError::Protocol(
            "first item announcement requires identity",
        ))?;
        if let Some(prior) = self.items.get(&item_id) {
            if prior.response_id != response_id
                || prior.kind != item.kind
                || prior.call_id != item.call_id
                || prior.name != item.name
                || output_index
                    .zip(prior.output_index)
                    .is_some_and(|(current, previous)| current != previous)
            {
                return Err(VoiceLiveError::Protocol(
                    "contradictory duplicate item announcement",
                ));
            }
            if let Some(arguments) = item.arguments.as_ref().filter(|value| !value.is_empty()) {
                let parsed: Value = serde_json::from_str(arguments).map_err(|_| {
                    VoiceLiveError::Protocol("duplicate item arguments are invalid JSON")
                })?;
                let call_id = item
                    .call_id
                    .as_ref()
                    .ok_or(VoiceLiveError::Protocol("duplicate arguments without call"))?;
                let completed = self.known_call(call_id);
                if let Some(call) = completed {
                    if call.arguments != parsed {
                        return Err(VoiceLiveError::Protocol("duplicate item arguments changed"));
                    }
                } else if let Some(initial) = prior
                    .initial_arguments
                    .as_ref()
                    .filter(|value| !value.is_empty())
                {
                    if serde_json::from_str::<Value>(initial).ok().as_ref() != Some(&parsed) {
                        return Err(VoiceLiveError::Protocol(
                            "duplicate initial arguments changed",
                        ));
                    }
                } else {
                    self.retain(arguments.len().saturating_mul(2))?;
                    self.items
                        .get_mut(&item_id)
                        .expect("known item")
                        .initial_arguments = Some(arguments.clone());
                    if let Some(tool) = self.tools.get_mut(call_id) {
                        tool.initial_arguments = Some(arguments.clone());
                    }
                }
            }
            if output_index.is_some() {
                self.items
                    .get_mut(&item_id)
                    .expect("known item")
                    .output_index = output_index;
            }
            return Ok(());
        }
        if self.items.len() >= self.config.limits.max_session_items {
            return Err(VoiceLiveError::TooLarge);
        }
        if self.finishing && item.kind == ItemKind::FunctionCall {
            return Err(VoiceLiveError::Protocol(
                "new function work after final drain boundary",
            ));
        }
        self.retain(
            serde_json::to_vec(&item)
                .map_err(|_| VoiceLiveError::TooLarge)?
                .len()
                .saturating_mul(2),
        )?;
        if item.kind == ItemKind::FunctionCall {
            let call_id = item.call_id.clone().ok_or(VoiceLiveError::Protocol(
                "first call announcement requires call identity",
            ))?;
            let name = item.name.clone().ok_or(VoiceLiveError::Protocol(
                "first call announcement requires name",
            ))?;
            if self
                .items
                .values()
                .any(|item| item.call_id.as_ref() == Some(&call_id))
            {
                return Err(VoiceLiveError::Protocol("call identity reused"));
            }
            if self.config.session.tool_choice == crate::ToolChoice::None
                || !self
                    .config
                    .session
                    .tools
                    .iter()
                    .any(|tool| tool.name == name)
            {
                return Err(VoiceLiveError::Protocol(
                    "provider selected an unoffered local tool",
                ));
            }
            if self.tools.len() >= self.config.limits.max_outstanding_tools
                || self
                    .items
                    .values()
                    .filter(|item| item.kind == ItemKind::FunctionCall)
                    .count()
                    >= self.config.limits.max_session_tool_calls
            {
                return Err(VoiceLiveError::TooLarge);
            }
            self.tools.insert(
                call_id,
                ToolState {
                    response_id: response_id.clone(),
                    item_id: item_id.clone(),
                    name,
                    initial_arguments: item.arguments.clone(),
                    call: None,
                    deadline: Instant::now() + self.config.limits.response_timeout,
                },
            );
        } else if item.role == Some(Role::Assistant) {
            let response = self
                .responses
                .get_mut(&response_id)
                .expect("validated response");
            if !response.started {
                response.started = true;
                self.emit(
                    Event::ResponseStarted {
                        response_id: response_id.clone(),
                    },
                    scope.generation,
                )
                .await?;
            }
        }
        self.items.insert(
            item_id,
            ItemState {
                response_id,
                output_index,
                kind: item.kind,
                call_id: item.call_id,
                name: item.name,
                initial_arguments: item.arguments,
            },
        );
        Ok(())
    }
    async fn event(
        &mut self,
        socket: &mut Socket,
        event: ServerEvent,
    ) -> Result<(), VoiceLiveError> {
        if matches!(event, ServerEvent::Unknown) {
            self.unknown_events += 1;
            if self.unknown_events > self.config.limits.max_unknown_events {
                return Err(VoiceLiveError::Protocol("unknown event budget exhausted"));
            }
            return Ok(());
        }
        match event {
            ServerEvent::SessionCreated { .. } if self.stage == 0 => {
                self.stage = 1;
                self.update_request = Some(
                    self.write(
                        socket,
                        json!({"type":"session.update","session":self.config.session}),
                    )
                    .await?,
                );
                self.ready_deadline = Instant::now() + self.config.limits.connect_timeout;
                return Ok(());
            }
            ServerEvent::SessionUpdated { .. } if self.stage == 1 => {
                self.stage = 2;
                self.update_request = None;
                self.emit(Event::Ready, self.epoch()).await?;
                self.ready_tx.send_replace(true);
                return Ok(());
            }
            ServerEvent::SessionCreated { .. } | ServerEvent::SessionUpdated { .. } => {
                return Err(VoiceLiveError::Protocol(
                    "out of order or duplicate session readiness",
                ))
            }
            ServerEvent::Error { error, .. } => {
                if self.stage < 2
                    || error
                        .event_id
                        .as_ref()
                        .is_some_and(|id| self.update_request.as_ref() == Some(id))
                {
                    return Err(VoiceLiveError::Protocol("session configuration rejected"));
                }
                let code = error.code.as_deref();
                let matching = error.event_id.as_ref().is_some_and(|id| {
                    self.pending
                        .as_ref()
                        .is_some_and(|pending| &pending.request_id == id)
                });
                if matching && code != Some("conversation_already_has_active_response") {
                    self.pending = None;
                    if self.active.is_none() {
                        self.response_deadline = None;
                    }
                }
                let quiet = matches!(
                    code,
                    Some(
                        "conversation_already_has_active_response"
                            | "response_cancel_not_active"
                            | "item_already_truncated"
                    )
                );
                if !quiet {
                    self.emit(
                        Event::ProviderError {
                            code: Some(
                                if matching {
                                    "response_request_rejected"
                                } else {
                                    "provider_request_error"
                                }
                                .into(),
                            ),
                            message: "Voice Live rejected a request".into(),
                            fatal: false,
                        },
                        self.epoch(),
                    )
                    .await?;
                }
                self.maybe_continue(socket).await?;
                return Ok(());
            }
            _ if self.stage != 2 => {
                return Err(VoiceLiveError::Protocol(
                    "provider work before session readiness",
                ))
            }
            _ => {}
        }
        match event {
            ServerEvent::ResponseCreated { response, .. } => {
                let id = response.id.ok_or(VoiceLiveError::Protocol(
                    "response announcement requires identity",
                ))?;
                if self.active.is_some() || self.responses.contains_key(&id) {
                    return Err(VoiceLiveError::Protocol(
                        "duplicate or overlapping response",
                    ));
                }
                if self.responses.len() >= self.config.limits.max_session_responses {
                    return Err(VoiceLiveError::TooLarge);
                }
                let scope = if let Some(pending) = self.pending.take() {
                    pending.scope
                } else if !self.config.session.is_manual() && !self.finishing {
                    ResponseScope {
                        generation: self.epoch(),
                        fence: None,
                        interrupted: false,
                        started: false,
                        done: false,
                    }
                } else {
                    return Err(VoiceLiveError::Protocol("unsolicited response"));
                };
                if let Some(fence) = &scope.fence {
                    self.fences
                        .lock()
                        .map_err(|_| VoiceLiveError::Protocol("response fence lock"))?
                        .insert(id.clone(), fence.clone());
                }
                self.active = Some(id.clone());
                self.responses.insert(id, scope);
                self.response_deadline = Some(Instant::now() + self.config.limits.response_timeout);
            }
            ServerEvent::ResponseDone { response, .. } => {
                if response.id.is_none() && self.omitted_after_interrupt {
                    return Err(VoiceLiveError::Protocol(
                        "omitted completion identity after interruption",
                    ));
                }
                let id =
                    response
                        .id
                        .or_else(|| self.active.clone())
                        .ok_or(VoiceLiveError::Protocol(
                            "completion has no announced response",
                        ))?;
                if self.active.as_deref() != Some(&id) {
                    return Err(VoiceLiveError::Protocol(
                        "completion identity does not match active response",
                    ));
                }
                if let Some(output) = &response.output {
                    for item in output {
                        self.validate_item_identity(&id, item)?;
                    }
                }
                let status = response.status.ok_or(VoiceLiveError::Protocol(
                    "response completion requires status",
                ))?;
                if status == ResponseStatus::InProgress {
                    return Err(VoiceLiveError::Protocol("in-progress response.done"));
                }
                let scope = self
                    .responses
                    .get_mut(&id)
                    .ok_or(VoiceLiveError::Protocol("unknown completed response"))?;
                scope.done = true;
                let generation = scope.generation;
                let interrupted = scope.interrupted || generation != self.epoch();
                self.active = None;
                self.response_deadline = None;
                if !interrupted {
                    self.emit(
                        Event::ResponseDone {
                            response_id: id.clone(),
                            status,
                            usage: response.usage,
                        },
                        generation,
                    )
                    .await?;
                }
                if status == ResponseStatus::Failed {
                    self.emit(
                        Event::ProviderError {
                            code: Some("response_failed".into()),
                            message: "Voice Live response failed".into(),
                            fatal: false,
                        },
                        self.epoch(),
                    )
                    .await?;
                }
                self.maybe_continue(socket).await?;
            }
            ServerEvent::OutputItemAdded {
                response_id,
                item,
                output_index,
                ..
            } => self.announce(item, response_id, Some(output_index)).await?,
            ServerEvent::ConversationItemCreated { item, .. } => {
                // This second source announcement lacks response_id. A known item
                // can supply its original binding even after a later response starts.
                let known = item
                    .id
                    .as_ref()
                    .and_then(|id| self.items.get(id))
                    .map(|item| item.response_id.clone());
                if let Some(id) = &known {
                    if self.responses.get(id).is_some_and(|scope| scope.done) {
                        self.validate_item_identity(id, &item)?;
                        return Ok(());
                    }
                }
                self.announce(item, known, None).await?;
            }
            ServerEvent::AudioDelta {
                response_id,
                item_id,
                output_index,
                content_index,
                delta,
                ..
            } => {
                let Some((response_id, scope)) = self.response(response_id.as_deref())? else {
                    return Ok(());
                };
                let item_id = self.item(&response_id, item_id.as_deref(), ItemKind::Message)?;
                self.check_part(&item_id, output_index, content_index)?;
                if content_index > 63
                    || delta.len() > self.config.limits.max_audio_chunk_bytes.div_ceil(3) * 4
                {
                    return Err(VoiceLiveError::TooLarge);
                }
                let audio = STANDARD
                    .decode(delta)
                    .map_err(|_| VoiceLiveError::Protocol("invalid audio base64"))?;
                if audio.len() > self.config.limits.max_audio_chunk_bytes
                    || !audio.len().is_multiple_of(2)
                {
                    return Err(VoiceLiveError::Protocol("invalid PCM output length"));
                }
                if audio.is_empty() {
                    return Ok(());
                }
                {
                    let mut playback = self
                        .playback
                        .lock()
                        .map_err(|_| VoiceLiveError::Protocol("playback lock"))?;
                    let key = (item_id.clone(), content_index);
                    let part = playback.parts.entry(key.clone()).or_insert(PlayedPart {
                        generation: scope.generation,
                        generated_bytes: 0,
                        acknowledged_ms: None,
                    });
                    if part.generation != scope.generation {
                        return Err(VoiceLiveError::Protocol("audio part generation changed"));
                    }
                    part.generated_bytes = part
                        .generated_bytes
                        .checked_add(audio.len() as u64)
                        .filter(|n| *n <= u64::from(u32::MAX) * 48)
                        .ok_or(VoiceLiveError::TooLarge)?;
                    playback.latest = Some(key);
                }
                self.emit(
                    Event::Audio {
                        response_id,
                        item_id,
                        content_index,
                        audio,
                    },
                    scope.generation,
                )
                .await?;
            }
            ServerEvent::TextDelta {
                response_id,
                item_id,
                output_index,
                content_index,
                delta,
                ..
            } => {
                self.text(
                    response_id,
                    item_id,
                    output_index,
                    content_index,
                    delta,
                    false,
                )
                .await?
            }
            ServerEvent::AudioTranscriptDelta {
                response_id,
                item_id,
                output_index,
                content_index,
                delta,
                ..
            } => {
                self.text(
                    response_id,
                    item_id,
                    output_index,
                    content_index,
                    delta,
                    true,
                )
                .await?
            }
            ServerEvent::AudioDone {
                response_id,
                item_id,
                output_index,
                content_index,
                ..
            } => {
                if let Some((id, _)) = self.response(response_id.as_deref())? {
                    let item = self.item(&id, item_id.as_deref(), ItemKind::Message)?;
                    self.check_part(&item, output_index, content_index)?;
                    if content_index > 63 {
                        return Err(VoiceLiveError::TooLarge);
                    }
                }
            }
            ServerEvent::FunctionCallArgumentsDone {
                response_id,
                item_id,
                call_id,
                name,
                arguments,
                output_index,
                ..
            } => {
                let Some((response_id, scope)) = self.response(response_id.as_deref())? else {
                    return Ok(());
                };
                let item_id = match (item_id, call_id.as_ref()) {
                    (Some(id), _) => self.item(&response_id, Some(&id), ItemKind::FunctionCall)?,
                    (None, Some(call_id)) => {
                        let known = self
                            .tools
                            .get(call_id)
                            .ok_or(VoiceLiveError::Protocol("unannounced function completion"))?;
                        self.item(&response_id, Some(&known.item_id), ItemKind::FunctionCall)?
                    }
                    (None, None) => self.item(&response_id, None, ItemKind::FunctionCall)?,
                };
                self.check_part(&item_id, output_index, 0)?;
                let item = self.items.get(&item_id).expect("validated item");
                let known_call = item
                    .call_id
                    .as_ref()
                    .ok_or(VoiceLiveError::Protocol("function item without call"))?;
                if call_id.as_ref().is_some_and(|id| id != known_call) {
                    return Err(VoiceLiveError::Protocol(
                        "contradictory function call identity",
                    ));
                }
                if let Some(prior) = self.settled.get(known_call) {
                    if name
                        .as_ref()
                        .is_some_and(|name| name != &prior.function_name)
                        || serde_json::from_str::<Value>(&arguments).ok().as_ref()
                            != Some(&prior.arguments)
                    {
                        return Err(VoiceLiveError::Protocol(
                            "replayed settled arguments changed",
                        ));
                    }
                    return Ok(());
                }
                self.retained_bytes = self
                    .retained_bytes
                    .checked_add(arguments.len().saturating_mul(3))
                    .filter(|n| *n <= self.config.limits.max_message_bytes * 4)
                    .ok_or(VoiceLiveError::TooLarge)?;
                let tool = self
                    .tools
                    .get_mut(known_call)
                    .ok_or(VoiceLiveError::Protocol("unannounced function completion"))?;
                if name.as_ref().is_some_and(|name| name != &tool.name)
                    || tool.response_id != response_id
                {
                    return Err(VoiceLiveError::Protocol(
                        "function name or response changed",
                    ));
                }
                let arguments: Value = serde_json::from_str(&arguments)
                    .map_err(|_| VoiceLiveError::Protocol("invalid function JSON arguments"))?;
                if !arguments.is_object() {
                    return Err(VoiceLiveError::Protocol(
                        "function arguments must be object",
                    ));
                }
                if let Some(initial) = tool
                    .initial_arguments
                    .as_ref()
                    .filter(|args| !args.is_empty())
                {
                    if serde_json::from_str::<Value>(initial).ok().as_ref() != Some(&arguments) {
                        return Err(VoiceLiveError::Protocol(
                            "announced function arguments changed",
                        ));
                    }
                }
                let call = FunctionCall {
                    function_name: tool.name.clone(),
                    tool_call_id: known_call.clone(),
                    arguments,
                };
                if let Some(prior) = &tool.call {
                    if *prior != call {
                        return Err(VoiceLiveError::Protocol(
                            "duplicate completed arguments changed",
                        ));
                    }
                    return Ok(());
                }
                if self.finishing {
                    return Err(VoiceLiveError::Protocol(
                        "new function execution after final drain",
                    ));
                }
                tool.call = Some(call.clone());
                self.deliveries
                    .lock()
                    .map_err(|_| VoiceLiveError::Protocol("tool delivery lock"))?
                    .insert(
                        call.tool_call_id.clone(),
                        ToolDelivery {
                            call: call.clone(),
                            generation: scope.generation,
                            fence: scope.fence.clone(),
                            stage: ToolDeliveryStage::PendingMap,
                        },
                    );
                self.emit(Event::ToolCall { response_id, call }, scope.generation)
                    .await?;
            }
            ServerEvent::InputAudioTranscriptionDelta {
                item_id,
                delta,
                content_index,
                ..
            } => {
                if content_index != 0 {
                    return Err(VoiceLiveError::Protocol(
                        "input transcript content index unsupported",
                    ));
                }
                let limit = self.config.limits.max_message_bytes;
                self.retain(delta.as_ref().map_or(0, String::len))?;
                let state = self.transcript_state(&item_id)?;
                if state.final_text.is_some() || state.failed {
                    return Err(VoiceLiveError::Protocol("transcript delta after terminal"));
                }
                if let Some(delta) = delta.filter(|text| !text.is_empty()) {
                    if state
                        .interim
                        .len()
                        .checked_add(delta.len())
                        .is_none_or(|n| n > limit)
                    {
                        return Err(VoiceLiveError::TooLarge);
                    }
                    state.interim.push_str(&delta);
                    let text = state.interim.clone();
                    self.emit(
                        Event::Transcript {
                            item_id,
                            text,
                            is_final: false,
                        },
                        self.epoch(),
                    )
                    .await?;
                }
            }
            ServerEvent::InputAudioTranscriptionCompleted {
                item_id,
                transcript,
                content_index,
                ..
            } => {
                if content_index != 0 {
                    return Err(VoiceLiveError::Protocol(
                        "input transcript content index unsupported",
                    ));
                }
                let text = transcript.trim().to_owned();
                self.retain(text.len())?;
                let state = self.transcript_state(&item_id)?;
                if text.is_empty() {
                    return Ok(());
                }
                if state.failed {
                    return Err(VoiceLiveError::Protocol(
                        "transcript completion after failure",
                    ));
                }
                if let Some(prior) = &state.final_text {
                    if prior != &text {
                        return Err(VoiceLiveError::Protocol("transcript completion changed"));
                    }
                    return Ok(());
                }
                state.interim.clear();
                state.final_text = Some(text.clone());
                self.emit(
                    Event::Transcript {
                        item_id,
                        text,
                        is_final: true,
                    },
                    self.epoch(),
                )
                .await?;
            }
            ServerEvent::InputAudioTranscriptionFailed { item_id, .. } => {
                let state = self.transcript_state(&item_id)?;
                if state.final_text.is_some() {
                    return Err(VoiceLiveError::Protocol(
                        "transcript failed after completion",
                    ));
                }
                state.interim.clear();
                state.failed = true;
                self.emit(Event::TranscriptFailed { item_id }, self.epoch())
                    .await?;
                self.emit(
                    Event::ProviderError {
                        code: Some("transcription_failed".into()),
                        message: "Voice Live input transcription failed".into(),
                        fatal: false,
                    },
                    self.epoch(),
                )
                .await?;
            }
            ServerEvent::SpeechStarted { .. }
                if !self.config.session.is_manual() && !self.finishing =>
            {
                self.begin_semantic_admission()?;
                self.emit(Event::SpeechStarted, self.epoch()).await?
            }
            ServerEvent::SpeechStopped { .. }
                if !self.config.session.is_manual() && !self.finishing =>
            {
                self.begin_semantic_admission()?;
                self.emit(Event::SpeechStopped, self.epoch()).await?
            }
            ServerEvent::SpeechStarted { .. }
            | ServerEvent::SpeechStopped { .. }
            | ServerEvent::Unknown => {}
            ServerEvent::SessionCreated { .. }
            | ServerEvent::SessionUpdated { .. }
            | ServerEvent::Error { .. } => unreachable!("handled before ready validation"),
        }
        Ok(())
    }
    async fn text(
        &self,
        response_id: Option<String>,
        item_id: Option<String>,
        output_index: u32,
        content_index: u32,
        delta: String,
        audio_transcript: bool,
    ) -> Result<(), VoiceLiveError> {
        let Some((response_id, scope)) = self.response(response_id.as_deref())? else {
            return Ok(());
        };
        let item_id = self.item(&response_id, item_id.as_deref(), ItemKind::Message)?;
        self.check_part(&item_id, output_index, content_index)?;
        if !delta.is_empty() {
            self.emit(
                Event::Text {
                    response_id,
                    item_id,
                    delta,
                    audio_transcript,
                },
                scope.generation,
            )
            .await?;
        }
        Ok(())
    }
    fn check_part(
        &self,
        item_id: &str,
        output_index: u32,
        content_index: u32,
    ) -> Result<(), VoiceLiveError> {
        if content_index > 63 || output_index as usize >= self.config.limits.max_session_items {
            return Err(VoiceLiveError::TooLarge);
        }
        if self
            .items
            .get(item_id)
            .and_then(|item| item.output_index)
            .is_some_and(|known| known != output_index)
        {
            return Err(VoiceLiveError::Protocol(
                "output index contradicts announced item",
            ));
        }
        Ok(())
    }
    fn transcript_state(&mut self, id: &str) -> Result<&mut TranscriptState, VoiceLiveError> {
        if !self.transcripts.contains_key(id)
            && self.transcripts.len() >= self.config.limits.max_session_items
        {
            return Err(VoiceLiveError::TooLarge);
        }
        Ok(self.transcripts.entry(id.to_owned()).or_default())
    }
}
/// Generation and causal checks cover every readiness poll. Once bytes enter
/// the sink they are flushed exactly once; cancellation never retries them.
async fn send_current<S>(
    socket: &mut S,
    message: Message,
    generation: &AtomicU64,
    expected: u64,
    gate: &Mutex<()>,
    fence: Option<&InvocationFence>,
) -> Result<bool, VoiceLiveError>
where
    S: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let mut message = Some(message);
    let admitted = futures_util::future::poll_fn(|cx| {
        let _guard = gate.lock().unwrap_or_else(|e| e.into_inner());
        let mut send = || {
            if generation.load(Ordering::Acquire) != expected {
                return Poll::Ready(Ok(false));
            }
            match Pin::new(&mut *socket).poll_ready(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(transport(error))),
                Poll::Ready(Ok(())) => Poll::Ready(
                    Pin::new(&mut *socket)
                        .start_send(message.take().expect("one admitted write"))
                        .map(|()| true)
                        .map_err(transport),
                ),
            }
        };
        match fence {
            Some(fence) => fence.with_current(send).unwrap_or(Poll::Ready(Ok(false))),
            None => send(),
        }
    })
    .await?;
    if admitted {
        futures_util::future::poll_fn(|cx| Pin::new(&mut *socket).poll_flush(cx))
            .await
            .map_err(transport)?;
    }
    Ok(admitted)
}

#[cfg(test)]
pub(crate) mod delivery_tests {
    use super::*;

    fn fixture() -> (VoiceLiveSender, Actor, FunctionCall) {
        let config = VoiceLiveConfig::new(
            "ws://127.0.0.1:1/voice-live/realtime",
            "synthetic",
            crate::VoiceLiveCredentials::ApiKey("invented".into()),
            "synthetic-voice",
        );
        let (tx, _) = mpsc::channel(8);
        let (events, _) = mpsc::channel(8);
        let (control, _) = watch::channel(Control::default());
        let (_, terminal) = watch::channel(None);
        let (ready, _) = watch::channel(false);
        let generation = Arc::new(AtomicU64::new(0));
        let gate = Arc::new(Mutex::new(()));
        let playback = Arc::new(Mutex::new(PlaybackState::default()));
        let fences = Arc::new(Mutex::new(HashMap::new()));
        let deliveries = Arc::new(Mutex::new(HashMap::new()));
        let semantic_admission = Arc::new(Mutex::new(None));
        let sender = VoiceLiveSender {
            tx,
            control,
            terminal,
            generation: generation.clone(),
            admission: Arc::new(AtomicU64::new(0)),
            gate: gate.clone(),
            playback: playback.clone(),
            fences: fences.clone(),
            deliveries: deliveries.clone(),
            semantic_admission: semantic_admission.clone(),
            limits: config.limits.clone(),
        };
        let mut actor = Actor::new(
            config,
            generation,
            gate,
            playback,
            fences,
            deliveries.clone(),
            semantic_admission,
            false,
            events,
            ready,
        );
        let call = FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "call".into(),
            arguments: json!({"value":1}),
        };
        deliveries.lock().unwrap().insert(
            "call".into(),
            ToolDelivery {
                call: call.clone(),
                generation: 0,
                fence: None,
                stage: ToolDeliveryStage::PendingMap,
            },
        );
        actor.tools.insert(
            "call".into(),
            ToolState {
                response_id: "response".into(),
                item_id: "item".into(),
                name: "lookup".into(),
                initial_arguments: None,
                call: Some(call.clone()),
                deadline: Instant::now() + Duration::from_secs(1),
            },
        );
        (sender, actor, call)
    }

    pub(crate) fn test_sender() -> (VoiceLiveSender, FunctionCall, Arc<AtomicU64>) {
        let (sender, _, call) = fixture();
        let generation = sender.generation.clone();
        (sender, call, generation)
    }

    #[test]
    fn only_expired_original_unentered_declaration_proves_queue_drop() {
        let (sender, mut actor, call) = fixture();
        let frame = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
        sender.register_tool_declaration(&frame, 0).unwrap();
        sender.generation.store(1, Ordering::Release);
        assert!(!actor.retire_undelivered_tools().unwrap());
        let retained = frame.clone();
        drop(frame);
        assert!(!actor.retire_undelivered_tools().unwrap());
        drop(retained);
        assert!(actor.retire_undelivered_tools().unwrap());
        assert!(actor.tools.is_empty());
        assert!(actor.settled.is_empty(), "nonadmission is never settlement");
        actor.items.insert(
            "item".into(),
            ItemState {
                response_id: "response".into(),
                output_index: Some(0),
                kind: ItemKind::FunctionCall,
                call_id: Some("call".into()),
                name: Some("lookup".into()),
                initial_arguments: None,
            },
        );
        let item: ConversationItem = serde_json::from_value(json!({"id":"item","type":"function_call","call_id":"call","name":"lookup","arguments":"{\"value\":1}"})).unwrap();
        actor.validate_item_identity("response", &item).unwrap();
    }

    #[test]
    fn entered_executor_requires_real_terminal_even_after_frame_dies() {
        let (sender, mut actor, call) = fixture();
        let frame = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
        sender.register_tool_declaration(&frame, 0).unwrap();
        assert!(sender.enter_tool_executor(&frame).unwrap());
        sender.generation.store(1, Ordering::Release);
        drop(frame);
        assert!(!actor.retire_undelivered_tools().unwrap());
        assert_eq!(actor.tools.len(), 1);
        assert!(actor.settled.is_empty());
    }

    #[test]
    fn stale_queued_declaration_never_enters_executor() {
        let (sender, mut actor, call) = fixture();
        let frame = Frame::new(FrameData::FunctionCallsStarted(vec![call]));
        sender.register_tool_declaration(&frame, 0).unwrap();
        sender.generation.store(1, Ordering::Release);
        assert!(!sender.enter_tool_executor(&frame).unwrap());
        assert!(actor.retire_undelivered_tools().unwrap());
        assert!(actor.settled.is_empty());
    }

    #[test]
    fn forged_identity_and_wrong_discard_cannot_erase_entered_call() {
        let (sender, mut actor, call) = fixture();
        let frame = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
        sender.register_tool_declaration(&frame, 0).unwrap();
        let mut forged = Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()]));
        forged.id = frame.id;
        assert!(sender.enter_tool_executor(&forged).is_err());
        assert!(sender.enter_tool_executor(&frame).unwrap());
        assert!(sender.discard_unmapped_tool(&call, 0).is_err());
        sender.generation.store(1, Ordering::Release);
        drop(frame);
        assert!(!actor.retire_undelivered_tools().unwrap());
    }

    #[test]
    fn unmapped_drop_requires_exact_call_and_does_not_claim_terminal() {
        let (sender, mut actor, call) = fixture();
        let mut changed = call.clone();
        changed.arguments = json!({"value":2});
        assert!(sender.discard_unmapped_tool(&changed, 0).is_err());
        assert!(sender.discard_unmapped_tool(&call, 1).is_err());
        sender.discard_unmapped_tool(&call, 0).unwrap();
        assert!(actor.retire_undelivered_tools().unwrap());
        assert!(actor.settled.is_empty());
    }
}

#[cfg(test)]
#[path = "semantic_tests.rs"]
mod semantic_tests;
