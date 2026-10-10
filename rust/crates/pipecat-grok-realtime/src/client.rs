use crate::types::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::{
    FunctionCallInProgressFrame, NativeToolCallOutcome, ToolCallEndReason, ToolCallTerminalState,
};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    net::IpAddr,
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
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::{
    tungstenite::{
        self, client::IntoClientRequest, http::HeaderValue, protocol::WebSocketConfig, Message,
    },
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RealtimeError {
    #[error("invalid realtime configuration or action: {0}")]
    Invalid(&'static str),
    #[error("realtime handshake returned HTTP {0}")]
    Http(u16),
    #[error("realtime transport failed")]
    Transport,
    #[error("invalid realtime provider message: {0}")]
    Protocol(&'static str),
    #[error("realtime {0} timed out")]
    Timeout(&'static str),
    #[error("realtime queue is full")]
    Backpressure,
    #[error("realtime session is not ready for audio")]
    NotReady,
    #[error("realtime session is closed")]
    Closed,
    #[error("realtime response is already active")]
    ResponseActive,
    #[error("realtime action was interrupted before transmission")]
    Interrupted,
    #[error("realtime message or state exceeds configured limit")]
    TooLarge,
    #[error("unknown, stale, or already completed tool call")]
    UnknownToolCall,
    #[error("realtime connection ended with unsettled native tools")]
    UnsettledTools,
}
#[derive(Debug, Clone)]
pub struct RealtimeLimits {
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_audio_chunk_bytes: usize,
    pub max_outstanding_tools: usize,
    pub max_session_responses: usize,
    pub max_session_tool_calls: usize,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub close_timeout: Duration,
}
impl Default for RealtimeLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_message_bytes: 1024 * 1024,
            max_audio_chunk_bytes: 192_000,
            max_outstanding_tools: 64,
            max_session_responses: 4096,
            max_session_tool_calls: 4096,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(120),
            close_timeout: Duration::from_secs(2),
        }
    }
}
impl RealtimeLimits {
    fn validate(&self) -> Result<(), RealtimeError> {
        if self.queue_capacity == 0
            || self.queue_capacity > 65_536
            || self.max_message_bytes < 256
            || self.max_message_bytes > 16 * 1024 * 1024
            || self.max_audio_chunk_bytes == 0
            || self.max_audio_chunk_bytes > self.max_message_bytes / 2
            || self.max_outstanding_tools == 0
            || self.max_outstanding_tools > 4096
            || self.max_session_responses == 0
            || self.max_session_responses > 65_536
            || self.max_session_tool_calls < self.max_outstanding_tools
            || self.max_session_tool_calls > 65_536
            || self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.response_timeout.is_zero()
            || self.close_timeout.is_zero()
            || [
                self.connect_timeout,
                self.operation_timeout,
                self.response_timeout,
                self.close_timeout,
            ]
            .iter()
            .any(|d| *d > Duration::from_secs(86_400))
        {
            return Err(RealtimeError::Invalid("limits outside supported bounds"));
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct RealtimeConfig {
    pub endpoint: String,
    pub model: String,
    pub session: SessionProperties,
    pub context_id: Option<String>,
    pub limits: RealtimeLimits,
    api_key: String,
}
impl fmt::Debug for RealtimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealtimeConfig")
            .field("endpoint", &"[configured]")
            .field("model", &self.model)
            .field("api_key", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl RealtimeConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.x.ai/v1/realtime".into(),
            model: "grok-voice-latest".into(),
            session: SessionProperties::pcm(),
            context_id: None,
            limits: RealtimeLimits::default(),
            api_key: api_key.into(),
        }
    }
    pub fn validate(&self) -> Result<(), RealtimeError> {
        self.limits.validate()?;
        endpoint(&self.endpoint)?;
        if self.context_id.as_ref().is_some_and(|id| id.len() > 256) {
            return Err(RealtimeError::Invalid("context ID exceeds 256 bytes"));
        }
        if self.model.trim().is_empty()
            || self.model.len() > 256
            || self.api_key.trim().is_empty()
            || self.api_key.len() > 4096
        {
            return Err(RealtimeError::Invalid("model and API key are required"));
        }
        HeaderValue::from_str(&format!("Bearer {}", self.api_key))
            .map_err(|_| RealtimeError::Invalid("authentication header"))?;
        validate_session(&self.session)?;
        bounded_serialized(&self.session, self.limits.max_message_bytes)?;
        if self
            .session
            .model
            .as_ref()
            .is_some_and(|m| m != &self.model)
        {
            return Err(RealtimeError::Invalid(
                "session model differs from connection model",
            ));
        }
        Ok(())
    }
}
fn endpoint(s: &str) -> Result<url::Url, RealtimeError> {
    let url = url::Url::parse(s).map_err(|_| RealtimeError::Invalid("endpoint URL"))?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(RealtimeError::Invalid(
            "endpoint must be a WebSocket URL without credentials, query, or fragment",
        ));
    }
    if url.scheme() == "ws"
        && !url
            .host_str()
            .unwrap_or_default()
            .trim_matches(['[', ']'])
            .parse::<IpAddr>()
            .is_ok_and(|v| v.is_loopback())
    {
        return Err(RealtimeError::Invalid(
            "plaintext requires an explicit loopback IP address",
        ));
    }
    Ok(url)
}
fn validate_session(s: &SessionProperties) -> Result<(), RealtimeError> {
    if s.voice
        .as_ref()
        .is_some_and(|v| v.trim().is_empty() || v.len() > 256)
    {
        return Err(RealtimeError::Invalid(
            "voice must be a nonempty identifier",
        ));
    }
    if let Some(Some(vad)) = &s.turn_detection {
        if !vad.valid_kind()
            || vad
                .threshold
                .is_some_and(|v| !v.is_finite() || !(0.1..=0.9).contains(&v))
        {
            return Err(RealtimeError::Invalid("server VAD configuration"));
        }
    }
    if let Some(audio) = &s.audio {
        for format in [
            audio.input.as_ref().map(|i| &i.format),
            audio.output.as_ref().map(|o| &o.format),
        ]
        .into_iter()
        .flatten()
        {
            if matches!(format, AudioFormat::Pcm { rate } if ![8000,11025,16000,22050,24000,32000,44100,48000].contains(rate))
            {
                return Err(RealtimeError::Invalid("unsupported Grok PCM sample rate"));
            }
        }
        if [
            audio.input.as_ref().and_then(|i| i.transport),
            audio.output.as_ref().and_then(|o| o.transport),
        ]
        .contains(&Some(AudioTransport::Binary))
        {
            return Err(RealtimeError::Invalid(
                "binary audio transport is not implemented; use JSON",
            ));
        }
        if audio
            .output
            .as_ref()
            .and_then(|o| o.speed)
            .is_some_and(|v| !v.is_finite() || !(0.7..=1.5).contains(&v))
        {
            return Err(RealtimeError::Invalid("audio speed"));
        }
        if let Some(t) = audio.input.as_ref().and_then(|i| i.transcription.as_ref()) {
            if t.keyterms
                .as_ref()
                .is_some_and(|v| v.len() > 100 || v.iter().any(|t| t.chars().count() > 50))
            {
                return Err(RealtimeError::Invalid("transcription keyterms"));
            }
        }
    }
    if let Some(tools) = &s.tools {
        if tools.len() > 128 {
            return Err(RealtimeError::TooLarge);
        }
        let mut names = HashSet::new();
        for tool in tools {
            match tool {
                GrokTool::Function {
                    name, parameters, ..
                } if name.is_empty()
                    || name.len() > 256
                    || !parameters.is_object()
                    || !names.insert(name) =>
                {
                    return Err(RealtimeError::Invalid(
                        "function tool schema or duplicate name",
                    ))
                }
                GrokTool::Mcp { properties } if properties.contains_key("type") => {
                    return Err(RealtimeError::Invalid(
                        "MCP properties cannot override type",
                    ))
                }
                _ => {}
            }
        }
    }
    Ok(())
}

fn transport(e: tungstenite::Error) -> RealtimeError {
    match e {
        tungstenite::Error::Http(r) => RealtimeError::Http(r.status().as_u16()),
        tungstenite::Error::Capacity(_) => RealtimeError::TooLarge,
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            RealtimeError::Closed
        }
        _ => RealtimeError::Transport,
    }
}
#[derive(Debug, Clone)]
pub struct RealtimeClient {
    config: RealtimeConfig,
}
impl RealtimeClient {
    pub fn config(&self) -> &RealtimeConfig {
        &self.config
    }
    pub fn new(config: RealtimeConfig) -> Result<Self, RealtimeError> {
        config.validate()?;
        Ok(Self { config })
    }
    /// Opens a single authenticated connection. Redirects and automatic retries
    /// are deliberately unsupported, so credentials and audio are never replayed.
    pub async fn connect(&self) -> Result<RealtimeSession, RealtimeError> {
        let c = &self.config;
        let mut url = endpoint(&c.endpoint)?;
        url.query_pairs_mut().append_pair("model", &c.model);
        let mut req = url
            .as_str()
            .into_client_request()
            .map_err(|_| RealtimeError::Invalid("WebSocket request"))?;
        let mut auth = HeaderValue::from_str(&format!("Bearer {}", c.api_key))
            .map_err(|_| RealtimeError::Invalid("authentication header"))?;
        auth.set_sensitive(true);
        req.headers_mut().insert("authorization", auth);
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(c.limits.max_message_bytes))
            .max_frame_size(Some(c.limits.max_message_bytes));
        let (mut socket, _) = timeout(
            c.limits.connect_timeout,
            tokio_tungstenite::connect_async_with_config(req, Some(ws_config), true),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("connect"))?
        .map_err(transport)?;
        let (tx, rx) = mpsc::channel(c.limits.queue_capacity);
        let (events_tx, events_rx) = mpsc::channel(c.limits.queue_capacity);
        let (control_tx, control_rx) = watch::channel(Control::default());
        let (terminal_tx, terminal_rx) = watch::channel(None);
        let generation = Arc::new(AtomicU64::new(0));
        let emission_gate = Arc::new(Mutex::new(()));
        let ready = Arc::new(AtomicBool::new(false));
        let health_gate = Arc::new(Mutex::new(None));
        let sender = RealtimeSender {
            ready: ready.clone(),
            health_gate: health_gate.clone(),
            tx,
            control: control_tx,
            generation: generation.clone(),
            emission_gate: emission_gate.clone(),
            limits: c.limits.clone(),
            projected_session: Arc::new(Mutex::new(c.session.clone())),
            connection_model: Arc::from(c.model.as_str()),
            terminal: terminal_rx,
        };
        let mut actor = Actor::new(c.clone(), generation.clone(), events_tx, ready, health_gate);
        let task = tokio::spawn(async move {
            let result = actor.run(&mut socket, rx, control_rx).await;
            let result = if result.is_err() && !actor.tools.is_empty() {
                Err(RealtimeError::UnsettledTools)
            } else {
                result
            };
            terminal_tx.send_replace(Some(result.clone()));
            actor.shared_ready.store(false, Ordering::Release);
            let _ = timeout(actor.config.limits.close_timeout, socket.close(None)).await;
            result
        });
        Ok(RealtimeSession {
            sender,
            rx: events_rx,
            task: Some(task),
            ended: false,
        })
    }
}
#[derive(Debug, Clone, Default)]
struct Control {
    generation: u64,
    playback: Option<PlaybackPosition>,
    close: bool,
}
enum QueuedAction {
    Public(Action),
    ToolPolicy(FunctionCallInProgressFrame),
    ToolOutcome {
        outcome: Arc<NativeToolCallOutcome>,
        output: Value,
    },
}
struct Command {
    action: QueuedAction,
    generation: u64,
    reply: Option<oneshot::Sender<Result<(), RealtimeError>>>,
}
type HealthGate = Arc<
    Mutex<
        Option<(
            pipecat_core::ProcessorHealth,
            pipecat_core::ProcessorHealthGeneration,
        )>,
    >,
>;
fn health_current(gate: &HealthGate) -> bool {
    gate.lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .is_none_or(|(health, generation)| health.is_usable() && health.generation() == *generation)
}
#[derive(Clone)]
pub struct RealtimeSender {
    ready: Arc<AtomicBool>,
    health_gate: HealthGate,
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    generation: Arc<AtomicU64>,
    emission_gate: Arc<Mutex<()>>,
    limits: RealtimeLimits,
    projected_session: Arc<Mutex<SessionProperties>>,
    connection_model: Arc<str>,
    terminal: watch::Receiver<Option<Result<(), RealtimeError>>>,
}
impl fmt::Debug for RealtimeSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealtimeSender")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}
impl RealtimeSender {
    /// Enqueues without waiting for socket I/O. Rejected actions are reported as
    /// terminal stream errors through `RealtimeSession::next_event`.
    pub fn try_send(&self, action: Action) -> Result<(), RealtimeError> {
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        check_queued_action(&action, &self.limits)?;
        if matches!(action, Action::AppendAudio(_)) && !self.is_ready() {
            return Err(RealtimeError::NotReady);
        }
        if let Action::AppendAudio(bytes) = &action {
            if bytes.len() > self.limits.max_audio_chunk_bytes {
                return Err(RealtimeError::TooLarge);
            }
        }
        if matches!(action, Action::CancelResponse) {
            self.interrupt(None);
            return Ok(());
        }
        self.enqueue_command(action, None)
    }
    // These commands carry native admission state and survive later turn changes.
    // Queue admission is not proof of a socket write or provider observation.
    fn enqueue_native(&self, action: QueuedAction) -> Result<(), RealtimeError> {
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        self.tx
            .try_send(Command {
                action,
                generation: self.generation(),
                reply: None,
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })
    }
    pub(crate) fn register_tool_policy(
        &self,
        call: FunctionCallInProgressFrame,
    ) -> Result<(), RealtimeError> {
        bounded_serialized(&call, self.limits.max_message_bytes)?;
        self.enqueue_native(QueuedAction::ToolPolicy(call))
    }
    pub(crate) fn send_tool_outcome(
        &self,
        outcome: Arc<NativeToolCallOutcome>,
        output: Value,
    ) -> Result<(), RealtimeError> {
        bounded_serialized(&(outcome.call(), &output), self.limits.max_message_bytes)?;
        self.enqueue_native(QueuedAction::ToolOutcome { outcome, output })
    }
    fn projected_update(
        &self,
        current: &SessionProperties,
        actions: &[Action],
    ) -> Result<Option<SessionProperties>, RealtimeError> {
        let mut updated = None;
        for action in actions {
            if let Action::UpdateSession(patch) = action {
                if patch
                    .model
                    .as_deref()
                    .is_some_and(|m| m != self.connection_model.as_ref())
                {
                    return Err(RealtimeError::Invalid(
                        "model cannot change during a session",
                    ));
                }
                let next = updated.get_or_insert_with(|| current.clone());
                next.apply_patch(patch);
                bounded_serialized(next, self.limits.max_message_bytes)?;
            }
        }
        Ok(updated)
    }
    fn enqueue_command(
        &self,
        action: Action,
        reply: Option<oneshot::Sender<Result<(), RealtimeError>>>,
    ) -> Result<(), RealtimeError> {
        // This mutex orders the projected settings with every queue admission.
        // The actor retains a separate, wire-applied settings snapshot.
        let mut projected = self
            .projected_session
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let updated = self.projected_update(&projected, std::slice::from_ref(&action))?;
        let permit = self.tx.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
        })?;
        permit.send(Command {
            action: QueuedAction::Public(action),
            generation: self.generation(),
            reply,
        });
        if let Some(updated) = updated {
            *projected = updated;
        }
        Ok(())
    }
    /// Locally projected settings after successful bounded queue admission.
    /// This is not a provider acknowledgement; a failed write retires the session.
    pub fn projected_session(&self) -> SessionProperties {
        self.projected_session
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    pub fn try_send_batch(&self, actions: Vec<Action>) -> Result<(), RealtimeError> {
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        for action in &actions {
            if matches!(action, Action::CancelResponse | Action::Finish) {
                return Err(RealtimeError::Invalid("control action in batch"));
            }
            check_queued_action(action, &self.limits)?;
            if matches!(action, Action::AppendAudio(_)) && !self.is_ready() {
                return Err(RealtimeError::NotReady);
            }
        }
        let mut projected = self
            .projected_session
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let updated = self.projected_update(&projected, &actions)?;
        let generation = self.generation();
        let permits = self
            .tx
            .try_reserve_many(actions.len())
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })?;
        for (permit, action) in permits.zip(actions) {
            permit.send(Command {
                action: QueuedAction::Public(action),
                generation,
                reply: None,
            });
        }
        if let Some(updated) = updated {
            *projected = updated;
        }
        Ok(())
    }
    pub(crate) fn bind_health(
        &self,
        health: pipecat_core::ProcessorHealth,
        generation: pipecat_core::ProcessorHealthGeneration,
    ) {
        *self.health_gate.lock().unwrap_or_else(|e| e.into_inner()) = Some((health, generation));
    }
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::Acquire)
    }
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    /// Bounded enqueue plus local write/admission acknowledgement, not provider
    /// acceptance. An explicit deferred inference request waits for response.done. A timeout is ambiguous:
    /// do not blindly retry non-idempotent actions such as response.create.
    pub async fn send(&self, action: Action) -> Result<(), RealtimeError> {
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        check_queued_action(&action, &self.limits)?;
        if matches!(action, Action::AppendAudio(_)) && !self.is_ready() {
            return Err(RealtimeError::NotReady);
        }
        if let Action::AppendAudio(bytes) = &action {
            if bytes.len() > self.limits.max_audio_chunk_bytes {
                return Err(RealtimeError::TooLarge);
            }
        }
        if matches!(action, Action::CancelResponse) {
            self.interrupt(None);
            return Ok(());
        }
        let (reply, result) = oneshot::channel();
        self.enqueue_command(action, Some(reply))?;
        timeout(self.limits.operation_timeout, result)
            .await
            .map_err(|_| RealtimeError::Timeout("action"))?
            .map_err(|_| RealtimeError::Closed)?
    }
    /// Invalidates already queued output immediately. Cancellation uses a
    /// separate watch lane and cannot wait behind queued audio or tool work.
    pub fn interrupt(&self, playback: Option<PlaybackPosition>) {
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.control.send_modify(|c| {
            c.generation = generation;
            c.playback = playback;
        });
    }
    /// Ordered graceful end, unlike immediate `close`. Continue draining events.
    pub async fn finish(&self) -> Result<(), RealtimeError> {
        if let Some(result) = self.terminal.borrow().clone() {
            return result;
        }
        match self.send(Action::Finish).await {
            Err(RealtimeError::Closed) => {
                let mut terminal = self.terminal.clone();
                timeout(self.limits.operation_timeout, async {
                    loop {
                        if let Some(result) = terminal.borrow().clone() {
                            return result;
                        }
                        terminal
                            .changed()
                            .await
                            .map_err(|_| RealtimeError::Closed)?;
                    }
                })
                .await
                .map_err(|_| RealtimeError::Timeout("close outcome"))?
            }
            result => result,
        }
    }
    pub fn close(&self) {
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        self.ready.store(false, Ordering::Release);
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.control.send_modify(|c| c.close = true);
    }
    /// The gate spans each poll, never an await, so interruption and nonblocking
    /// pipeline admission cannot race between a generation check and emission.
    pub(crate) async fn while_current<F: Future>(
        &self,
        generation: u64,
        future: F,
    ) -> Option<F::Output> {
        tokio::pin!(future);
        futures_util::future::poll_fn(|cx| {
            let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
            if self.generation() != generation || !health_current(&self.health_gate) {
                return Poll::Ready(None);
            }
            future.as_mut().poll(cx).map(Some)
        })
        .await
    }
}
pub struct RealtimeSession {
    sender: RealtimeSender,
    rx: mpsc::Receiver<RealtimeEvent>,
    task: Option<JoinHandle<Result<(), RealtimeError>>>,
    ended: bool,
}
impl RealtimeSession {
    pub fn sender(&self) -> RealtimeSender {
        self.sender.clone()
    }
    pub async fn next_event(&mut self) -> Option<Result<RealtimeEvent, RealtimeError>> {
        if self.ended {
            return None;
        }
        while let Some(event) = self.rx.recv().await {
            if event.event.response_id().is_some() && event.generation != self.sender.generation() {
                continue;
            }
            return Some(Ok(event));
        }
        self.ended = true;
        match self.task.take()?.await {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(Err(e)),
            Err(_) => Some(Err(RealtimeError::Transport)),
        }
    }
    pub async fn shutdown(&mut self) -> Result<(), RealtimeError> {
        self.sender.close();
        self.rx.close();
        self.ended = true;
        if let Some(mut task) = self.task.take() {
            match timeout(
                self.sender.limits.close_timeout + self.sender.limits.operation_timeout,
                &mut task,
            )
            .await
            {
                Ok(result) => result.map_err(|_| RealtimeError::Transport)?,
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    Err(RealtimeError::Timeout("close"))
                }
            }
        } else {
            Ok(())
        }
    }
}
impl Drop for RealtimeSession {
    fn drop(&mut self) {
        self.sender.close();
    }
}
#[derive(Clone)]
struct ResponseContext {
    generation: u64,
    context_id: Option<String>,
    allowed_tools: Arc<HashSet<String>>,
}
struct ToolState {
    name: String,
    item_id: String,
    generation: u64,
    arguments: String,
    announced: bool,
    cancel_on_interruption: Option<bool>,
    pending_outcome: Option<(Arc<NativeToolCallOutcome>, Value)>,
    context_id: Option<String>,
    response_id: String,
}
struct Actor {
    config: RealtimeConfig,
    generation: Arc<AtomicU64>,
    tx: mpsc::Sender<RealtimeEvent>,
    session_id: Option<String>,
    conversation_id: Option<String>,
    sequence: u64,
    active: Option<String>,
    pending: Option<ResponseContext>,
    pending_event_id: Option<String>,
    ready: bool,
    shared_ready: Arc<AtomicBool>,
    health_gate: HealthGate,
    finishing: bool,
    ready_deadline: Instant,
    responses: VecDeque<(String, ResponseContext)>,
    deadline: Option<Instant>,
    tools: HashMap<String, ToolState>,
    completed_tools: HashSet<String>,
    cancelled_tools: HashSet<String>,
    seen_responses: HashSet<String>,
    seen_calls: HashSet<String>,
    initial_update_sent: bool,
    results_awaiting_response: bool,
    deferred_response: Option<(ResponseOptions, u64)>,
    discarded_calls: HashSet<String>,
}
impl Actor {
    fn new(
        config: RealtimeConfig,
        generation: Arc<AtomicU64>,
        tx: mpsc::Sender<RealtimeEvent>,
        shared_ready: Arc<AtomicBool>,
        health_gate: HealthGate,
    ) -> Self {
        let ready_deadline = Instant::now() + config.limits.connect_timeout;
        Self {
            config,
            generation,
            tx,
            session_id: None,
            conversation_id: None,
            sequence: 0,
            active: None,
            pending: None,
            pending_event_id: None,
            ready: false,
            shared_ready,
            health_gate,
            finishing: false,
            ready_deadline,
            responses: VecDeque::new(),
            deadline: None,
            tools: HashMap::new(),
            completed_tools: HashSet::new(),
            cancelled_tools: HashSet::new(),
            seen_responses: HashSet::new(),
            seen_calls: HashSet::new(),
            initial_update_sent: false,
            results_awaiting_response: false,
            deferred_response: None,
            discarded_calls: HashSet::new(),
        }
    }
    fn allowed_tools(&self) -> Arc<HashSet<String>> {
        Arc::new(
            self.config
                .session
                .tools
                .as_deref()
                .unwrap_or_default()
                .iter()
                .filter_map(|t| match t {
                    GrokTool::Function { name, .. } => Some(name.clone()),
                    _ => None,
                })
                .collect(),
        )
    }
    fn epoch(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    async fn write(&mut self, socket: &mut Socket, mut value: Value) -> Result<(), RealtimeError> {
        if !health_current(&self.health_gate) {
            return Err(RealtimeError::Closed);
        }
        self.sequence += 1;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
        bounded_serialized(&value, self.config.limits.max_message_bytes)?;
        let wire = serde_json::to_string(&value)
            .map_err(|_| RealtimeError::Invalid("event serialization"))?;
        if wire.len() > self.config.limits.max_message_bytes {
            return Err(RealtimeError::TooLarge);
        }
        timeout(
            self.config.limits.operation_timeout,
            socket.send(Message::Text(wire.into())),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
        .map_err(transport)
    }
    async fn write_action(
        &mut self,
        socket: &mut Socket,
        action: Action,
        generation: u64,
    ) -> Result<(), RealtimeError> {
        let cancellation = matches!(action, Action::CancelTool { .. });
        let action = match action {
            Action::CheckedToolResult {
                call,
                output,
                is_final,
            } => {
                if self.completed_tools.contains(&call.tool_call_id)
                    || self.cancelled_tools.contains(&call.tool_call_id)
                {
                    return Ok(());
                }
                let tool = self
                    .tools
                    .get(&call.tool_call_id)
                    .ok_or(RealtimeError::UnknownToolCall)?;
                if tool.name != call.function_name
                    || serde_json::from_str::<Value>(&tool.arguments).ok().as_ref()
                        != Some(&call.arguments)
                {
                    return Err(RealtimeError::Invalid(
                        "tool result does not match admitted call",
                    ));
                }
                Action::ToolResult {
                    call_id: call.tool_call_id,
                    output,
                    is_final,
                }
            }
            Action::CancelTool {
                call_id,
                function_name,
            } => {
                if self.completed_tools.contains(&call_id)
                    || self.cancelled_tools.contains(&call_id)
                    || self.discarded_calls.contains(&call_id)
                {
                    return Ok(());
                }
                let tool = self
                    .tools
                    .get(&call_id)
                    .ok_or(RealtimeError::UnknownToolCall)?;
                if tool.name != function_name {
                    return Err(RealtimeError::Invalid(
                        "cancel does not match admitted call",
                    ));
                }
                Action::ToolResult {
                    call_id,
                    output: json!("CANCELLED"),
                    is_final: true,
                }
            }
            action => action,
        };
        let value = match action {
            Action::CheckedToolResult { .. } | Action::CancelTool { .. } => {
                unreachable!("normalized above")
            }
            Action::Finish => {
                self.finishing = true;
                return Ok(());
            }
            Action::UpdateSession(mut session) => {
                validate_session(&session)?;
                if session
                    .model
                    .as_ref()
                    .is_some_and(|m| m != &self.config.model)
                {
                    return Err(RealtimeError::Invalid(
                        "model cannot change during a session",
                    ));
                }
                // Connection model and server snapshot IDs are never update parameters.
                session.model = None;
                session.id = None;
                self.write(socket, json!({"type":"session.update", "session":session}))
                    .await?;
                self.config.session.apply_patch(&session);
                return Ok(());
            }
            Action::AppendAudio(audio) => {
                if audio.is_empty() {
                    return Err(RealtimeError::Invalid("empty audio chunk"));
                }
                if audio.len() > self.config.limits.max_audio_chunk_bytes {
                    return Err(RealtimeError::TooLarge);
                }
                json!({"type":"input_audio_buffer.append", "audio":STANDARD.encode(audio)})
            }
            Action::CommitAudio => json!({"type":"input_audio_buffer.commit"}),
            Action::ClearAudio => json!({"type":"input_audio_buffer.clear"}),
            Action::CreateItem {
                item,
                previous_item_id,
            } => {
                if !matches!(item, ConversationItem::ForceMessage { .. }) {
                    self.results_awaiting_response = true;
                }
                let mut v = json!({"type":"conversation.item.create", "item":item});
                if let Some(id) = previous_item_id {
                    v["previous_item_id"] = json!(id);
                }
                v
            }
            Action::DeleteItem { item_id } => {
                json!({"type":"conversation.item.delete", "item_id":item_id})
            }
            Action::TruncateItem(position) => {
                json!({"type":"conversation.item.truncate", "item_id":position.item_id, "content_index":position.content_index, "audio_end_ms":position.audio_end_ms})
            }
            Action::RequestInference(options) => {
                if !self.results_awaiting_response {
                    if self
                        .tools
                        .values()
                        .any(|tool| tool.pending_outcome.is_some())
                    {
                        self.deferred_response = Some((options, generation));
                    }
                    return Ok(());
                }
                if self.active.is_some() || self.pending.is_some() {
                    self.deferred_response = Some((options, generation));
                    return Ok(());
                }
                self.deferred_response = Some((options, generation));
                return self.maybe_continue(socket).await;
            }
            Action::CreateResponse(response) => {
                if response
                    .modalities
                    .as_ref()
                    .is_some_and(|v| v.is_empty() || v.len() > 2 || (v.len() == 2 && v[0] == v[1]))
                {
                    return Err(RealtimeError::Invalid(
                        "one or two distinct response modalities are required",
                    ));
                }
                if self.active.is_some() || self.pending.is_some() {
                    self.deferred_response = Some((response, generation));
                    return Ok(());
                }
                self.write(
                    socket,
                    json!({"type":"response.create", "response":response}),
                )
                .await?;
                self.pending_event_id = Some(format!("pc_evt_{}", self.sequence));
                self.pending = Some(ResponseContext {
                    allowed_tools: self.allowed_tools(),
                    generation,
                    context_id: response
                        .context_id
                        .or_else(|| self.config.context_id.clone()),
                });
                self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
                return Ok(());
            }
            Action::CancelResponse => json!({"type":"response.cancel"}),
            Action::ToolResult {
                call_id,
                output,
                is_final,
            } => {
                return self
                    .write_tool_result(
                        socket,
                        call_id,
                        output,
                        is_final,
                        cancellation.then_some(false),
                    )
                    .await;
            }
        };
        self.write(socket, value).await
    }
    async fn write_tool_result(
        &mut self,
        socket: &mut Socket,
        call_id: String,
        output: Value,
        is_final: bool,
        cancellation: Option<bool>,
    ) -> Result<(), RealtimeError> {
        if self.completed_tools.contains(&call_id)
            || self.cancelled_tools.contains(&call_id)
            || self.discarded_calls.contains(&call_id)
        {
            return Ok(());
        }
        let tool = self
            .tools
            .get(&call_id)
            .ok_or(RealtimeError::UnknownToolCall)?;
        if !tool.announced {
            return Err(RealtimeError::UnknownToolCall);
        }
        let output = match output {
            Value::String(text) => text,
            Value::Null => "COMPLETED".into(),
            other => other.to_string(),
        };
        // An intermediate result is context, never the call's one final output.
        if !is_final {
            let text = json!({
                        "type":"async_tool", "status":"running", "tool_call_id":call_id,
                        "description":"This is a partial result and the task is still running. More may follow. Do not call this tool again and do not treat this as the final answer.",
                        "result":output
                    }).to_string();
            let item = ConversationItem::Message {
                id: format!("pc_progress_{}", self.sequence + 1),
                role: Role::User,
                content: vec![ItemContent::InputText { text }],
            };
            self.results_awaiting_response = true;
            return self
                .write(
                    socket,
                    json!({"type":"conversation.item.create", "item":item}),
                )
                .await;
        }
        let item = ConversationItem::FunctionCallOutput {
            id: format!("pc_tool_{}", self.sequence + 1),
            call_id: call_id.clone(),
            output,
        };
        self.write(
            socket,
            json!({"type":"conversation.item.create", "item":item}),
        )
        .await?;
        self.tools.remove(&call_id);
        if let Some(run_llm) = cancellation {
            self.cancelled_tools.insert(call_id);
            self.results_awaiting_response |= run_llm;
        } else {
            self.completed_tools.insert(call_id);
            self.results_awaiting_response = true;
        }
        // Grok's aggregator owns inference. A tool result never creates
        // a response, even when all calls have completed.
        Ok(())
    }
    async fn write_command(
        &mut self,
        socket: &mut Socket,
        action: QueuedAction,
        generation: u64,
    ) -> Result<(), RealtimeError> {
        match action {
            QueuedAction::Public(action) => self.write_action(socket, action, generation).await,
            QueuedAction::ToolPolicy(call) => {
                if self.completed_tools.contains(&call.tool_call_id)
                    || self.cancelled_tools.contains(&call.tool_call_id)
                {
                    return Ok(());
                }
                let tool = self
                    .tools
                    .get_mut(&call.tool_call_id)
                    .ok_or(RealtimeError::UnknownToolCall)?;
                if !tool.announced
                    || tool.name != call.function_name
                    || serde_json::from_str::<Value>(&tool.arguments).ok().as_ref()
                        != Some(&call.arguments)
                    || tool
                        .cancel_on_interruption
                        .is_some_and(|policy| policy != call.cancel_on_interruption)
                {
                    return Err(RealtimeError::Invalid(
                        "native tool policy does not match admitted call",
                    ));
                }
                tool.cancel_on_interruption = Some(call.cancel_on_interruption);
                let pending = tool.pending_outcome.take();
                if let Some((outcome, output)) = pending {
                    self.write_native_outcome(socket, outcome, output).await?;
                    self.maybe_continue(socket).await?;
                }
                Ok(())
            }
            QueuedAction::ToolOutcome { outcome, output } => {
                self.write_native_outcome(socket, outcome, output).await
            }
        }
    }
    async fn write_native_outcome(
        &mut self,
        socket: &mut Socket,
        outcome: Arc<NativeToolCallOutcome>,
        output: Value,
    ) -> Result<(), RealtimeError> {
        let call = outcome.call();
        if self.completed_tools.contains(&call.tool_call_id)
            || self.cancelled_tools.contains(&call.tool_call_id)
        {
            return Ok(());
        }
        let tool = self
            .tools
            .get_mut(&call.tool_call_id)
            .ok_or(RealtimeError::UnknownToolCall)?;
        if !tool.announced
            || tool.name != call.function_name
            || serde_json::from_str::<Value>(&tool.arguments).ok().as_ref() != Some(&call.arguments)
            || tool
                .cancel_on_interruption
                .is_some_and(|policy| policy != outcome.cancel_on_interruption())
        {
            return Err(RealtimeError::Invalid(
                "native tool outcome does not match admitted call policy",
            ));
        }
        // System cancellation may overtake the uninterruptible normal-lane
        // declaration. Retain at most one bounded receipt per admitted call;
        // never guess the missing execution policy or fabricate settlement.
        if tool.cancel_on_interruption.is_none() {
            if tool.pending_outcome.is_none() {
                tool.pending_outcome = Some((outcome, output));
            }
            return Ok(());
        }
        let timeout = outcome.state() == ToolCallTerminalState::Failed
            && outcome.reason() == ToolCallEndReason::TimedOut;
        let cancellation = timeout || outcome.state() == ToolCallTerminalState::Cancelled;
        let run_llm = timeout || outcome.run_llm();
        let output = if cancellation {
            json!("CANCELLED")
        } else {
            output
        };
        self.write_tool_result(
            socket,
            call.tool_call_id.clone(),
            output,
            true,
            cancellation.then_some(run_llm),
        )
        .await
    }

    async fn maybe_continue(&mut self, socket: &mut Socket) -> Result<(), RealtimeError> {
        if self.deferred_response.is_some()
            && self.active.is_none()
            && self.pending.is_none()
            && self.results_awaiting_response
        {
            let (options, generation) = self
                .deferred_response
                .take()
                .expect("checked deferred response");
            if !self.results_awaiting_response {
                return Ok(());
            }
            if generation != self.epoch() {
                return Ok(());
            }
            self.write(
                socket,
                json!({"type":"response.create", "response":options}),
            )
            .await?;
            self.pending_event_id = Some(format!("pc_evt_{}", self.sequence));
            self.pending = Some(ResponseContext {
                allowed_tools: self.allowed_tools(),
                generation,
                context_id: options
                    .context_id
                    .or_else(|| self.config.context_id.clone()),
            });
            self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
        }
        Ok(())
    }
    async fn interrupt(
        &mut self,
        socket: &mut Socket,
        control: Control,
    ) -> Result<(), RealtimeError> {
        if self
            .config
            .session
            .turn_detection
            .as_ref()
            .is_none_or(Option::is_none)
        {
            self.write(socket, json!({"type":"input_audio_buffer.clear"}))
                .await?;
        }
        self.write(socket, json!({"type":"response.cancel"}))
            .await?;
        if let Some(position) = control.playback {
            self.write_action(socket, Action::TruncateItem(position), control.generation)
                .await?;
        }
        self.tools.retain(|id, tool| {
            if !tool.announced {
                self.discarded_calls.insert(id.clone());
            }
            tool.announced
        });
        self.deferred_response = None;
        self.results_awaiting_response = false;
        Ok(())
    }
    async fn run(
        &mut self,
        socket: &mut Socket,
        mut commands: mpsc::Receiver<Command>,
        mut control: watch::Receiver<Control>,
    ) -> Result<(), RealtimeError> {
        loop {
            if self.finishing && self.active.is_none() && self.pending.is_none() {
                return Ok(());
            }
            let deadline = self
                .deadline
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            tokio::select! { biased;
                changed = control.changed() => {
                    if changed.is_err() { return Ok(()); }
                    let value = control.borrow_and_update().clone();
                    if value.close { return Ok(()); }
                    self.interrupt(socket, value).await?;
                }
                _ = tokio::time::sleep_until(self.ready_deadline), if !self.ready => return Err(RealtimeError::Timeout("session readiness")),
                _ = tokio::time::sleep_until(deadline), if self.deadline.is_some() => return Err(RealtimeError::Timeout("response")),
                item = socket.next() => {
                    match item {
                        Some(Ok(Message::Text(text))) => {
                            let wire = parse_wire(&text, self.config.limits.max_audio_chunk_bytes)?;
                            self.event(socket, wire).await?;
                        }
                        Some(Ok(Message::Ping(p))) => { timeout(self.config.limits.operation_timeout, socket.send(Message::Pong(p))).await.map_err(|_| RealtimeError::Timeout("pong"))?.map_err(transport)?; }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None if !self.tools.is_empty() => return Err(RealtimeError::UnsettledTools),
                        Some(Ok(Message::Close(_))) | None => return if self.active.is_some() || self.pending.is_some() { Err(RealtimeError::Protocol("response ended before response.done")) } else { Ok(()) },
                        Some(Ok(_)) => return Err(RealtimeError::Protocol("expected JSON text")),
                        Some(Err(e)) => return Err(transport(e)),
                    }
                }
                command = commands.recv(), if self.ready => {
                    let Some(command) = command else { return Ok(()); };
                    // Settings are connection state, not interruptible turn work.
                    // Once admitted they must not disappear after projection.
                    let persistent = !matches!(command.action, QueuedAction::Public(ref action) if !matches!(action, Action::Finish | Action::UpdateSession(_)));
                    if !persistent && command.reply.as_ref().is_some_and(|r| r.is_closed()) { continue; }
                    if !persistent && command.generation != self.epoch() {
                        if let Some(reply) = command.reply { let _ = reply.send(Err(RealtimeError::Interrupted)); } continue;
                    }
                    let result = self.write_command(socket, command.action, command.generation).await;
                    let fatal = matches!(result, Err(RealtimeError::Transport | RealtimeError::Closed | RealtimeError::Timeout(_)));
                    let failure = result.clone();
                    let unacknowledged = command.reply.is_none();
                    if let Some(reply) = command.reply { let _ = reply.send(result); }
                    if fatal || (unacknowledged && failure.is_err()) { return failure; }
                }
            }
        }
    }
    async fn event(&mut self, socket: &mut Socket, wire: WireEvent) -> Result<(), RealtimeError> {
        let mut event = wire.event;
        if matches!(event, Event::SessionUpdated { .. }) {
            if !self.initial_update_sent {
                return Err(RealtimeError::Protocol(
                    "session updated before initial configuration",
                ));
            }
            self.ready = true;
            self.shared_ready.store(true, Ordering::Release);
        }
        match &event {
            Event::SessionCreated { session } | Event::SessionUpdated { session } => {
                self.session_id = session.id.clone().or_else(|| self.session_id.clone())
            }
            Event::ConversationCreated { conversation } => {
                self.conversation_id = Some(conversation.id.clone());
                if !self.initial_update_sent {
                    self.write_action(
                        socket,
                        Action::UpdateSession(self.config.session.clone().into()),
                        self.epoch(),
                    )
                    .await?;
                    self.initial_update_sent = true;
                }
            }
            Event::Error { error }
                if error.event_id.is_some() && error.event_id == self.pending_event_id =>
            {
                // Rejected creates never emit response.created/response.done. Only
                // their exact request ID can release pending response admission.
                self.pending = None;
                self.pending_event_id = None;
                self.deadline = None;
                self.deferred_response = None;
            }
            Event::ResponseCreated { response } => {
                if self.seen_responses.contains(&response.id) {
                    return Ok(());
                }
                if self.active.as_ref().is_some_and(|id| id != &response.id) {
                    return Err(RealtimeError::Protocol("overlapping responses"));
                }
                if response.id.is_empty()
                    || self.seen_responses.len() >= self.config.limits.max_session_responses
                {
                    return Err(RealtimeError::TooLarge);
                }
                self.seen_responses.insert(response.id.clone());
                self.results_awaiting_response = false;
                self.pending_event_id = None;
                let context = self.pending.take().unwrap_or_else(|| ResponseContext {
                    allowed_tools: self.allowed_tools(),
                    generation: self.epoch(),
                    context_id: self.config.context_id.clone(),
                });
                self.responses.push_back((response.id.clone(), context));
                while self.responses.len() > 64 {
                    self.responses.pop_front();
                }
                self.active = Some(response.id.clone());
                self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
                if response.conversation_id.is_some() {
                    self.conversation_id = response.conversation_id.clone();
                }
            }
            _ => {}
        }
        // Grok omits response/item IDs on tool completion. Resolve them only
        // through the tracked call, never by attaching an unknown call to the
        // currently active response.
        match &mut event {
            Event::FunctionArgumentsDone {
                call_id,
                response_id,
                item_id,
                ..
            }
            | Event::FunctionArgumentsDelta {
                call_id,
                response_id,
                item_id,
                ..
            } => {
                if self.completed_tools.contains(call_id)
                    || self.cancelled_tools.contains(call_id)
                    || self.discarded_calls.contains(call_id)
                {
                    return Ok(());
                }
                let tool = self.tools.get(call_id).ok_or(RealtimeError::Protocol(
                    "tool event without conversation item",
                ))?;
                if (!response_id.is_empty() && *response_id != tool.response_id)
                    || (!item_id.is_empty() && *item_id != tool.item_id)
                {
                    return Err(RealtimeError::Protocol("tool correlation mismatch"));
                }
                *response_id = tool.response_id.clone();
                *item_id = tool.item_id.clone();
            }
            Event::TextDelta { response_id, .. } if response_id.is_empty() => {
                *response_id = self
                    .active
                    .clone()
                    .ok_or(RealtimeError::Protocol("text without active response"))?;
            }
            _ => {}
        }
        let tool_context = match &event {
            Event::FunctionArgumentsDone { call_id, .. }
            | Event::FunctionArgumentsDelta { call_id, .. } => {
                self.tools.get(call_id).map(|t| ResponseContext {
                    generation: t.generation,
                    context_id: t.context_id.clone(),
                    allowed_tools: Arc::new(HashSet::new()),
                })
            }
            _ => None,
        };
        let context = tool_context.clone().or_else(|| {
            event.response_id().and_then(|id| {
                self.responses
                    .iter()
                    .rev()
                    .find(|(key, _)| key == id)
                    .map(|(_, c)| c.clone())
            })
        });
        if event.response_id().is_some() && context.is_none() {
            return Err(RealtimeError::Protocol("event references unknown response"));
        }
        if tool_context.is_none()
            && event
                .response_id()
                .is_some_and(|id| self.active.as_deref() != Some(id))
        {
            return Ok(());
        }
        let generation = context
            .as_ref()
            .map(|c| c.generation)
            .unwrap_or_else(|| self.epoch());
        let stale = event.response_id().is_some() && generation != self.epoch();
        if !stale {
            match &mut event {
                Event::OutputItemAdded {
                    item:
                        ConversationItem::FunctionCall {
                            id,
                            call_id,
                            name,
                            arguments,
                        },
                    ..
                }
                | Event::ItemAdded {
                    item:
                        ConversationItem::FunctionCall {
                            id,
                            call_id,
                            name,
                            arguments,
                        },
                    ..
                } => {
                    if self.completed_tools.contains(call_id)
                        || self.cancelled_tools.contains(call_id)
                        || self.discarded_calls.contains(call_id)
                    {
                        return Ok(());
                    }
                    let response_id = self
                        .active
                        .clone()
                        .ok_or(RealtimeError::Protocol("tool item without active response"))?;
                    let response_context = self
                        .responses
                        .iter()
                        .rev()
                        .find(|(id, _)| id == &response_id)
                        .map(|(_, c)| c.clone())
                        .ok_or(RealtimeError::Protocol("missing response context"))?;
                    if !response_context.allowed_tools.contains(name) {
                        return Err(RealtimeError::Protocol(
                            "provider requested an unadvertised function",
                        ));
                    }
                    if response_context.generation != self.epoch() {
                        return Ok(());
                    }
                    if let Some(tool) = self.tools.get(call_id) {
                        if tool.item_id != *id
                            || tool.name != *name
                            || tool.response_id != response_id
                        {
                            return Err(RealtimeError::Protocol("reused tool call identity"));
                        }
                    } else {
                        if call_id.is_empty()
                            || id.is_empty()
                            || name.is_empty()
                            || self.tools.len() >= self.config.limits.max_outstanding_tools
                            || self.seen_calls.len() >= self.config.limits.max_session_tool_calls
                        {
                            return Err(RealtimeError::TooLarge);
                        }
                        if !self.seen_calls.insert(call_id.clone()) {
                            return Err(RealtimeError::Protocol("reused tool call identity"));
                        }
                        self.tools.insert(
                            call_id.clone(),
                            ToolState {
                                name: name.clone(),
                                item_id: id.clone(),
                                generation: response_context.generation,
                                arguments: arguments.clone(),
                                announced: false,
                                cancel_on_interruption: None,
                                pending_outcome: None,
                                context_id: response_context.context_id,
                                response_id,
                            },
                        );
                    }
                }
                Event::FunctionArgumentsDelta { call_id, delta, .. } => {
                    let tool = self
                        .tools
                        .get_mut(call_id)
                        .ok_or(RealtimeError::UnknownToolCall)?;
                    if tool.arguments.len().saturating_add(delta.len())
                        > self.config.limits.max_message_bytes
                    {
                        return Err(RealtimeError::TooLarge);
                    }
                    if !tool.announced {
                        tool.arguments.push_str(delta);
                    }
                }
                Event::FunctionArgumentsDone {
                    call_id,
                    name,
                    arguments,
                    ..
                } => {
                    let tool = self
                        .tools
                        .get_mut(call_id)
                        .ok_or(RealtimeError::UnknownToolCall)?;
                    if tool.announced {
                        return Ok(());
                    }
                    if name.as_ref().is_none_or(|name| name != &tool.name) {
                        return Err(RealtimeError::Protocol("tool name mismatch"));
                    }
                    if !serde_json::from_str::<Value>(arguments).is_ok_and(|v| v.is_object()) {
                        return Err(RealtimeError::Protocol(
                            "tool arguments must be a JSON object",
                        ));
                    }
                    tool.arguments = arguments.clone();
                    tool.announced = true;
                }
                Event::AudioDelta { audio, .. }
                    if audio.len() > self.config.limits.max_audio_chunk_bytes =>
                {
                    return Err(RealtimeError::TooLarge)
                }
                _ => {}
            }
        }
        if let Event::ResponseDone { response, .. } = &event {
            if self.active.as_deref() == Some(&response.id) {
                self.active = None;
                self.deadline = None;
            }
            if !self.finishing || self.deferred_response.is_some() {
                self.maybe_continue(socket).await?;
            }
        }
        if stale || matches!(event, Event::Unknown) {
            return Ok(());
        }
        let context_id = context
            .and_then(|c| c.context_id)
            .or_else(|| self.config.context_id.clone());
        redact_event(&mut event, &self.config.api_key);
        self.tx
            .try_send(RealtimeEvent {
                event_id: wire.event_id,
                event_type: wire.event_type,
                session_id: self.session_id.clone(),
                conversation_id: self.conversation_id.clone(),
                context_id,
                generation,
                event,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })
    }
}
fn redact_event(event: &mut Event, key: &str) {
    fn redact(e: &mut ProviderError, key: &str) {
        e.message = e.message.replace(key, "[REDACTED]");
        e.kind = e.kind.replace(key, "[REDACTED]");
        for s in [&mut e.code, &mut e.param, &mut e.event_id]
            .into_iter()
            .flatten()
        {
            *s = s.replace(key, "[REDACTED]");
        }
    }
    match event {
        Event::Error { error } | Event::InputTranscriptionFailed { error, .. } => {
            redact(error, key)
        }
        Event::ResponseDone { response, .. } | Event::ResponseCreated { response } => {
            if let Some(error) = response
                .status_details
                .as_mut()
                .and_then(|s| s.error.as_mut())
            {
                redact(error, key);
            }
        }
        _ => {}
    }
}

/// Checks queue payload size without constructing a second unbounded buffer.
pub(crate) fn bounded_serialized(
    value: &impl serde::Serialize,
    limit: usize,
) -> Result<(), RealtimeError> {
    struct Budget(usize);
    impl std::io::Write for Budget {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self
                .0
                .checked_sub(bytes.len())
                .ok_or_else(|| std::io::Error::other("payload limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Budget(limit), value).map_err(|_| RealtimeError::TooLarge)
}

fn check_queued_action(action: &Action, limits: &RealtimeLimits) -> Result<(), RealtimeError> {
    if let Action::TruncateItem(position) = action {
        if position.item_id.is_empty() || position.item_id.len() > 256 {
            return Err(RealtimeError::Invalid("playback item ID"));
        }
    }
    if let Action::AppendAudio(bytes) = action {
        return if bytes.len() > limits.max_audio_chunk_bytes {
            Err(RealtimeError::TooLarge)
        } else {
            Ok(())
        };
    }
    if let Action::UpdateSession(session) = action {
        validate_session(session)?;
    }
    if let Action::CreateResponse(options) | Action::RequestInference(options) = action {
        if options
            .modalities
            .as_ref()
            .is_some_and(|v| v.is_empty() || v.len() > 2 || (v.len() == 2 && v[0] == v[1]))
        {
            return Err(RealtimeError::Invalid("response modalities"));
        }
        if options.context_id.as_ref().is_some_and(|id| id.len() > 256) {
            return Err(RealtimeError::Invalid("context ID exceeds 256 bytes"));
        }
    }
    bounded_serialized(action, limits.max_message_bytes)
}
