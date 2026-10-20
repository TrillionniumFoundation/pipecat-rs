use crate::types::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{Sink, SinkExt, StreamExt};
use pipecat_core::{
    FunctionCallInProgressFrame, NativeToolCallOutcome, ToolCallEndReason, ToolCallTerminalState,
};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    fmt,
    future::Future,
    net::IpAddr,
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
    #[error("realtime action was interrupted")]
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
    pub max_unknown_events: usize,
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
            max_unknown_events: 64,
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
            || self.max_unknown_events == 0
            || self.max_unknown_events > 65_536
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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AuthType {
    #[default]
    Basic,
    Bearer,
}
impl AuthType {
    fn scheme(self) -> &'static str {
        match self {
            Self::Basic => "Basic",
            Self::Bearer => "Bearer",
        }
    }
}
/// Compatibility names for shared native integration patterns, not other protocols.
pub type RealtimeConfig = InworldConfig;
pub type RealtimeClient = InworldClient;
pub type RealtimeSender = InworldSender;
pub type RealtimeSession = InworldSession;
#[derive(Clone)]
pub struct InworldConfig {
    pub endpoint: String,
    pub session: SessionProperties,
    pub context_id: Option<String>,
    pub limits: RealtimeLimits,
    pub auth_type: AuthType,
    require_native_receipts: bool,
    api_key: String,
}
impl fmt::Debug for InworldConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InworldConfig")
            .field("endpoint", &"[configured]")
            .field("auth_type", &self.auth_type)
            .field("api_key", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl InworldConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.inworld.ai/api/v1/realtime/session".into(),
            auth_type: AuthType::Basic,
            require_native_receipts: false,
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
        if self.api_key.trim().is_empty() || self.api_key.len() > 4096 {
            return Err(RealtimeError::Invalid("API credential is required"));
        }
        HeaderValue::from_str(&format!("{} {}", self.auth_type.scheme(), self.api_key))
            .map_err(|_| RealtimeError::Invalid("authentication header"))?;
        validate_session(&self.session)?;
        bounded_serialized(&self.session.wire_snapshot(), self.limits.max_message_bytes)?;
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
/// Validates a full immutable configuration snapshot before queue admission.
pub(crate) fn validate_session(s: &SessionProperties) -> Result<(), RealtimeError> {
    fn identifier(value: Option<&str>) -> bool {
        value.is_some_and(|v| !v.trim().is_empty() && v.len() <= 256)
    }
    if s.kind.as_deref() != Some("realtime") || !identifier(s.model.as_deref()) {
        return Err(RealtimeError::Invalid(
            "realtime session type and model are required",
        ));
    }
    if s.temperature
        .is_some_and(|t| !t.is_finite() || !(0.0..=2.0).contains(&t))
    {
        return Err(RealtimeError::Invalid("temperature"));
    }
    if s.output_modalities.is_none() {
        return Err(RealtimeError::Invalid(
            "audio output modality must be explicit",
        ));
    }
    validate_modalities(s.output_modalities.as_deref())?;
    let audio = s
        .audio
        .as_ref()
        .ok_or(RealtimeError::Invalid("PCM audio configuration required"))?;
    let input = audio
        .input
        .as_ref()
        .ok_or(RealtimeError::Invalid("PCM input required"))?;
    let output = audio
        .output
        .as_ref()
        .ok_or(RealtimeError::Invalid("PCM output required"))?;
    for format in [&input.format, &output.format] {
        if ![8000, 16000, 24000, 32000, 44100, 48000].contains(&format.sample_rate()) {
            return Err(RealtimeError::Invalid(
                "unsupported Inworld PCM sample rate",
            ));
        }
    }
    if !identifier(output.voice.as_deref())
        || !identifier(output.model.as_deref())
        || !identifier(
            input
                .transcription
                .as_ref()
                .and_then(|t| t.model.as_deref()),
        )
    {
        return Err(RealtimeError::Invalid(
            "voice, TTS and STT model are required",
        ));
    }
    match &input.turn_detection {
        None => return Err(RealtimeError::Invalid("turn detection must be explicit")),
        Some(Some(vad)) => {
            if vad
                .eagerness
                .as_ref()
                .is_some_and(|v| !matches!(v.as_str(), "low" | "medium" | "high"))
                || vad.create_response != Some(true)
                || vad.interrupt_response != Some(true)
            {
                return Err(RealtimeError::Invalid(
                    "automatic VAD requires response creation and interruption",
                ));
            }
        }
        Some(None) => {}
    }
    if let Some(data) = &s.provider_data {
        if data
            .get("auto_tool_response")
            .is_some_and(|v| v != &Value::Bool(false))
        {
            return Err(RealtimeError::Invalid(
                "automatic tool continuation must remain disabled",
            ));
        }
        if data.get("metadata").is_some_and(|v| !v.is_object()) {
            return Err(RealtimeError::Invalid(
                "provider metadata must be an object",
            ));
        }
        if data
            .get("metadata")
            .and_then(|v| v.get("sdk"))
            .is_some_and(|v| v != "pipecat-realtime")
        {
            return Err(RealtimeError::Invalid("provider SDK metadata is reserved"));
        }
    }
    if let Some(tools) = &s.tools {
        if tools.len() > 128 {
            return Err(RealtimeError::TooLarge);
        }
        let mut names = HashSet::new();
        for InworldTool::Function {
            name, parameters, ..
        } in tools
        {
            if !identifier(Some(name))
                || !parameters.is_object()
                || parameters.get("type").and_then(Value::as_str) != Some("object")
                || !names.insert(name)
            {
                return Err(RealtimeError::Invalid(
                    "function tool schema or duplicate name",
                ));
            }
        }
    }
    Ok(())
}
fn validate_modalities(modalities: Option<&[Modality]>) -> Result<(), RealtimeError> {
    if modalities.is_some_and(|v| {
        !v.contains(&Modality::Audio) || v.len() > 2 || (v.len() == 2 && v[0] == v[1])
    }) {
        return Err(RealtimeError::Invalid(
            "only the audio and transcript response profile is supported",
        ));
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
pub struct InworldClient {
    config: InworldConfig,
}
impl InworldClient {
    pub(crate) fn require_native_receipts(mut self) -> Self {
        self.config.require_native_receipts = true;
        self
    }
    pub fn config(&self) -> &InworldConfig {
        &self.config
    }
    pub fn new(config: InworldConfig) -> Result<Self, RealtimeError> {
        config.validate()?;
        Ok(Self { config })
    }
    /// Opens a single authenticated connection. Redirects and automatic retries
    /// are deliberately unsupported, so credentials and audio are never replayed.
    pub async fn connect(&self) -> Result<InworldSession, RealtimeError> {
        let c = &self.config;
        let mut url = endpoint(&c.endpoint)?;
        url.query_pairs_mut()
            .append_pair(
                "key",
                &format!("pipecat-realtime-{}", uuid::Uuid::new_v4().simple()),
            )
            .append_pair("protocol", "realtime");
        let mut req = url
            .as_str()
            .into_client_request()
            .map_err(|_| RealtimeError::Invalid("WebSocket request"))?;
        let mut auth = HeaderValue::from_str(&format!("{} {}", c.auth_type.scheme(), c.api_key))
            .map_err(|_| RealtimeError::Invalid("authentication header"))?;
        auth.set_sensitive(true);
        req.headers_mut().insert("authorization", auth);
        let ws_config = WebSocketConfig::default()
            .write_buffer_size(0)
            .max_write_buffer_size(
                c.limits
                    .max_message_bytes
                    .saturating_mul(2)
                    .saturating_add(1024),
            )
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
        let sender = InworldSender {
            ready: ready.clone(),
            health_gate: health_gate.clone(),
            tx,
            control: control_tx,
            generation: generation.clone(),
            emission_gate: emission_gate.clone(),
            limits: c.limits.clone(),
            require_native_receipts: c.require_native_receipts,
            projected_session: Arc::new(Mutex::new(c.session.clone())),
            terminal: terminal_rx,
        };
        let mut actor = Actor::new(
            c.clone(),
            generation.clone(),
            events_tx,
            ready,
            health_gate,
            emission_gate,
        );
        let cleanup_error = Arc::new(Mutex::new(None));
        let actor_cleanup_error = cleanup_error.clone();
        let actor_task = tokio::spawn(async move {
            let result = actor.run(&mut socket, rx, control_rx).await;
            actor.shared_ready.store(false, Ordering::Release);
            // Sink closure flushes tungstenite's queued peer-close reply. The
            // inherent close(None) would enqueue a new Close after Closing and
            // can fail SendAfterClosing instead of completing owned cleanup.
            let cleanup = timeout(
                actor.config.limits.close_timeout,
                futures_util::SinkExt::close(&mut socket),
            )
            .await;
            let cleanup = match cleanup {
                Ok(Ok(())) => None,
                Ok(Err(
                    tungstenite::Error::AlreadyClosed | tungstenite::Error::ConnectionClosed,
                )) => None,
                Ok(Err(error)) => Some(transport(error)),
                Err(_) => Some(RealtimeError::Timeout("close handshake")),
            };
            *actor_cleanup_error
                .lock()
                .unwrap_or_else(|e| e.into_inner()) = cleanup.clone();
            match result {
                Err(primary) => Err(primary),
                Ok(()) => cleanup.map_or(Ok(()), Err),
            }
        });
        let (task, actor_abort) = spawn_cleanup_owner(actor_task, terminal_tx);
        Ok(InworldSession {
            sender,
            rx: events_rx,
            task: Some(task),
            actor_abort,
            runtime: tokio::runtime::Handle::current(),
            cleanup_error,
            ended: false,
        })
    }
}
/// The supervisor is the only owner allowed to publish terminal state: it
/// first joins the actor, including bounded transport cleanup.
fn spawn_cleanup_owner(
    actor_task: JoinHandle<Result<(), RealtimeError>>,
    terminal: watch::Sender<Option<Result<(), RealtimeError>>>,
) -> (JoinHandle<Result<(), RealtimeError>>, AbortHandle) {
    let abort = actor_task.abort_handle();
    let owner = tokio::spawn(async move {
        let result = actor_task.await.unwrap_or(Err(RealtimeError::Transport));
        terminal.send_replace(Some(result.clone()));
        result
    });
    (owner, abort)
}
#[derive(Debug, Clone, Default)]
struct Control {
    explicit_cancel_generation: u64,
    close: bool,
}
enum QueuedAction {
    Public(Action),
    Context {
        actions: Vec<Action>,
        fence: pipecat_core::InvocationFence,
    },
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
pub struct InworldSender {
    ready: Arc<AtomicBool>,
    health_gate: HealthGate,
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    generation: Arc<AtomicU64>,
    emission_gate: Arc<Mutex<()>>,
    limits: RealtimeLimits,
    require_native_receipts: bool,
    projected_session: Arc<Mutex<SessionProperties>>,
    terminal: watch::Receiver<Option<Result<(), RealtimeError>>>,
}
impl fmt::Debug for InworldSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InworldSender")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}
impl InworldSender {
    fn check_action(&self, action: &Action) -> Result<(), RealtimeError> {
        if self.require_native_receipts
            && matches!(
                action,
                Action::ToolResult { .. }
                    | Action::CheckedToolResult { .. }
                    | Action::CancelTool { .. }
            )
        {
            return Err(RealtimeError::Invalid(
                "native tool settlement requires its terminal receipt",
            ));
        }
        check_queued_action(action, &self.limits)
    }
    fn ensure_open(&self) -> Result<(), RealtimeError> {
        if self.control.borrow().close || self.terminal.borrow().is_some() || self.tx.is_closed() {
            Err(RealtimeError::Closed)
        } else {
            Ok(())
        }
    }
    /// Enqueues without waiting for socket I/O. Rejected actions are reported as
    /// terminal stream errors through `InworldSession::next_event`.
    pub fn try_send(&self, action: Action) -> Result<(), RealtimeError> {
        self.ensure_open()?;
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        self.check_action(&action)?;
        if matches!(action, Action::Finish) && !self.is_ready() {
            self.close();
            return Ok(());
        }
        if matches!(action, Action::AppendAudio(_)) && !self.is_ready() {
            return Err(RealtimeError::NotReady);
        }
        if let Action::AppendAudio(bytes) = &action {
            if bytes.len() > self.limits.max_audio_chunk_bytes {
                return Err(RealtimeError::TooLarge);
            }
        }
        if matches!(action, Action::CancelResponse) {
            self.cancel_response();
            return Ok(());
        }
        self.enqueue_command(action, None)
    }
    // These commands carry native admission state and survive later turn changes.
    // Queue admission is not proof of a socket write or provider observation.
    fn enqueue_native(&self, action: QueuedAction) -> Result<(), RealtimeError> {
        self.ensure_open()?;
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
        if !call.cancel_on_interruption {
            return Err(RealtimeError::Invalid(
                "Inworld does not support noninterruptible tools",
            ));
        }
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
                validate_session(patch)?;
                let next = updated.get_or_insert_with(|| current.clone());
                if next.input_sample_rate() != patch.input_sample_rate()
                    || next.output_sample_rate() != patch.output_sample_rate()
                    || next.is_manual() != patch.is_manual()
                {
                    return Err(RealtimeError::Invalid(
                        "PCM rates and turn mode cannot change during a session",
                    ));
                }
                *next = (**patch).clone();
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

    /// Admit durable context edits atomically to the local bounded queue. Only
    /// inference uses the original invocation fence; this is not a provider ack.
    pub(crate) fn try_send_context(
        &self,
        actions: Vec<Action>,
        fence: pipecat_core::InvocationFence,
    ) -> Result<(), RealtimeError> {
        self.ensure_open()?;
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        for action in &actions {
            if !matches!(
                action,
                Action::UpdateSession(_)
                    | Action::CreateItem { .. }
                    | Action::CreateResponse(_)
                    | Action::RequestInference(_)
            ) {
                return Err(RealtimeError::Invalid(
                    "non-context action in durable context command",
                ));
            }
            self.check_action(action)?;
        }
        bounded_serialized(&actions, self.limits.max_message_bytes)?;
        let mut projected = self
            .projected_session
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        let updated = self.projected_update(&projected, &actions)?;
        let permit = self.tx.try_reserve().map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
            mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
        })?;
        permit.send(Command {
            action: QueuedAction::Context { actions, fence },
            generation: self.generation(),
            reply: None,
        });
        if let Some(updated) = updated {
            *projected = updated;
        }
        Ok(())
    }
    pub fn try_send_batch(&self, actions: Vec<Action>) -> Result<(), RealtimeError> {
        self.ensure_open()?;
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        for action in &actions {
            if matches!(action, Action::CancelResponse | Action::Finish) {
                return Err(RealtimeError::Invalid("control action in batch"));
            }
            self.check_action(action)?;
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
        self.ensure_open()?;
        if !health_current(&self.health_gate) {
            self.close();
            return Err(RealtimeError::Closed);
        }
        self.check_action(&action)?;
        if matches!(action, Action::Finish) && !self.is_ready() {
            self.close();
            return Ok(());
        }
        if matches!(action, Action::AppendAudio(_)) && !self.is_ready() {
            return Err(RealtimeError::NotReady);
        }
        if let Action::AppendAudio(bytes) = &action {
            if bytes.len() > self.limits.max_audio_chunk_bytes {
                return Err(RealtimeError::TooLarge);
            }
        }
        if matches!(action, Action::CancelResponse) {
            self.cancel_response();
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
    pub fn interrupt(&self) {
        self.signal_interruption(false);
    }
    fn cancel_response(&self) {
        self.signal_interruption(true);
    }
    fn signal_interruption(&self, explicit_cancel: bool) {
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        let generation = self.generation.fetch_add(1, Ordering::AcqRel) + 1;
        self.control.send_modify(|c| {
            if explicit_cancel {
                c.explicit_cancel_generation = generation;
            }
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
pub(crate) struct FencedEvent {
    pub event: RealtimeEvent,
    pub fence: Option<pipecat_core::InvocationFence>,
}
pub struct InworldSession {
    sender: InworldSender,
    rx: mpsc::Receiver<FencedEvent>,
    task: Option<JoinHandle<Result<(), RealtimeError>>>,
    actor_abort: AbortHandle,
    runtime: tokio::runtime::Handle,
    cleanup_error: Arc<Mutex<Option<RealtimeError>>>,
    ended: bool,
}
impl InworldSession {
    /// Secondary socket cleanup failure, available after the owned task is joined.
    /// It never replaces the primary protocol, response or tool failure.
    pub fn cleanup_error(&self) -> Option<RealtimeError> {
        self.cleanup_error
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
    pub fn sender(&self) -> InworldSender {
        self.sender.clone()
    }
    pub async fn next_event(&mut self) -> Option<Result<RealtimeEvent, RealtimeError>> {
        self.next_event_fenced()
            .await
            .map(|result| result.map(|event| event.event))
    }
    pub(crate) async fn next_event_fenced(&mut self) -> Option<Result<FencedEvent, RealtimeError>> {
        if self.ended {
            return None;
        }
        while let Some(event) = self.rx.recv().await {
            if (event.event.event.response_id().is_some()
                && event.event.generation != self.sender.generation())
                || event
                    .fence
                    .as_ref()
                    .is_some_and(|fence| !fence.is_current())
            {
                continue;
            }
            return Some(Ok(event));
        }
        let result = self.task.as_mut()?.await;
        self.task.take();
        self.ended = true;
        match result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(Err(e)),
            Err(_) => Some(Err(RealtimeError::Transport)),
        }
    }
    pub async fn shutdown(&mut self) -> Result<(), RealtimeError> {
        self.sender.close();
        self.rx.close();
        self.ended = true;
        if let Some(task) = self.task.as_mut() {
            match timeout(
                self.sender.limits.close_timeout + self.sender.limits.operation_timeout,
                &mut *task,
            )
            .await
            {
                Ok(result) => {
                    self.task.take();
                    result.map_err(|_| RealtimeError::Transport)?
                }
                Err(_) => {
                    self.actor_abort.abort();
                    let _ = task.await;
                    self.task.take();
                    Err(RealtimeError::Timeout("close"))
                }
            }
        } else {
            self.sender.terminal.borrow().clone().unwrap_or(Ok(()))
        }
    }
}
impl Drop for InworldSession {
    fn drop(&mut self) {
        self.sender.close();
        if let Some(mut task) = self.task.take() {
            let actor_abort = self.actor_abort.clone();
            let deadline = self.sender.limits.close_timeout + self.sender.limits.operation_timeout;
            // Dropping a waiter must not drop the only owner of ongoing socket
            // cleanup. The bounded reaper joins the supervisor, which joins actor.
            self.runtime.spawn(async move {
                if timeout(deadline, &mut task).await.is_err() {
                    actor_abort.abort();
                    let _ = task.await;
                }
            });
        }
    }
}
#[derive(Clone)]
struct ResponseContext {
    generation: u64,
    fence: Option<pipecat_core::InvocationFence>,
    context_id: Option<String>,
    allowed_tools: Arc<HashSet<String>>,
}
struct ToolState {
    name: String,
    fence: Option<pipecat_core::InvocationFence>,
    item_id: String,
    generation: u64,
    arguments: String,
    announced: bool,
    cancel_on_interruption: Option<bool>,
    pending_outcome: Option<(Arc<NativeToolCallOutcome>, Value)>,
    context_id: Option<String>,
    response_id: String,
    deadline: Instant,
}
// Recheck every pending poll until poll_ready/start_send admits the bytes.
// Once admitted, flush exactly those bytes even if cancellation arrives later.
async fn send_inference<S>(
    socket: &mut S,
    message: Message,
    generation: &AtomicU64,
    expected: u64,
    emission_gate: &Mutex<()>,
    health_gate: &HealthGate,
    fence: Option<&pipecat_core::InvocationFence>,
) -> Result<bool, RealtimeError>
where
    S: Sink<Message, Error = tungstenite::Error> + Unpin,
{
    let mut message = Some(message);
    let admitted = futures_util::future::poll_fn(|cx| {
        let _emission = emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        let mut admit = || {
            if generation.load(Ordering::Acquire) != expected || !health_current(health_gate) {
                return Poll::Ready(Ok(false));
            }
            match Pin::new(&mut *socket).poll_ready(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(transport(error))),
                Poll::Ready(Ok(())) => Poll::Ready(
                    Pin::new(&mut *socket)
                        .start_send(message.take().expect("one inference admission"))
                        .map(|()| true)
                        .map_err(transport),
                ),
            }
        };
        match fence {
            Some(fence) => fence.with_current(admit).unwrap_or(Poll::Ready(Ok(false))),
            None => admit(),
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
struct Actor {
    emission_gate: Arc<Mutex<()>>,
    config: InworldConfig,
    generation: Arc<AtomicU64>,
    tx: mpsc::Sender<FencedEvent>,
    session_id: Option<String>,
    conversation_id: Option<String>,
    sequence: u64,
    active: Option<String>,
    pending: Option<ResponseContext>,
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
    pending_session_updates: VecDeque<(SessionProperties, Instant)>,
    unknown_events: usize,
    audio_buffer: Vec<u8>,
    handled_explicit_cancel: u64,
    results_awaiting_response: bool,
    deferred_response: Option<(
        ResponseOptions,
        u64,
        Option<pipecat_core::InvocationFence>,
        bool,
    )>,
    discarded_calls: HashSet<String>,
    inference_fence: Option<pipecat_core::InvocationFence>,
}
impl Actor {
    fn new(
        config: InworldConfig,
        generation: Arc<AtomicU64>,
        tx: mpsc::Sender<FencedEvent>,
        shared_ready: Arc<AtomicBool>,
        health_gate: HealthGate,
        emission_gate: Arc<Mutex<()>>,
    ) -> Self {
        let ready_deadline = Instant::now() + config.limits.connect_timeout;
        Self {
            config,
            emission_gate,
            generation,
            tx,
            session_id: None,
            conversation_id: None,
            sequence: 0,
            active: None,
            pending: None,
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
            pending_session_updates: VecDeque::new(),
            unknown_events: 0,
            audio_buffer: Vec::new(),
            handled_explicit_cancel: 0,
            results_awaiting_response: false,
            deferred_response: None,
            discarded_calls: HashSet::new(),
            inference_fence: None,
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
                .map(|InworldTool::Function { name, .. }| name.clone())
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
    async fn write_response(
        &mut self,
        socket: &mut Socket,
        options: &ResponseOptions,
        generation: u64,
        fence: Option<&pipecat_core::InvocationFence>,
    ) -> Result<bool, RealtimeError> {
        self.sequence += 1;
        let value = json!({"type":"response.create", "response":options, "event_id":format!("pc_evt_{}", self.sequence)});
        bounded_serialized(&value, self.config.limits.max_message_bytes)?;
        let message = Message::Text(value.to_string().into());
        timeout(
            self.config.limits.operation_timeout,
            send_inference(
                socket,
                message,
                &self.generation,
                generation,
                &self.emission_gate,
                &self.health_gate,
                fence,
            ),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
    }
    async fn write_current<S>(
        &mut self,
        socket: &mut S,
        mut value: Value,
        generation: u64,
    ) -> Result<bool, RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        self.sequence += 1;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
        bounded_serialized(&value, self.config.limits.max_message_bytes)?;
        timeout(
            self.config.limits.operation_timeout,
            send_inference(
                socket,
                Message::Text(value.to_string().into()),
                &self.generation,
                generation,
                &self.emission_gate,
                &self.health_gate,
                None,
            ),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
    }
    async fn append_audio<S>(
        &mut self,
        socket: &mut S,
        audio: &[u8],
        generation: u64,
    ) -> Result<(), RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        if audio.is_empty() {
            return Err(RealtimeError::Invalid("empty audio chunk"));
        }
        if audio.len() > self.config.limits.max_audio_chunk_bytes {
            return Err(RealtimeError::TooLarge);
        }
        let chunk_bytes = self
            .config
            .session
            .input_sample_rate()
            .ok_or(RealtimeError::Invalid("PCM rate"))? as usize
            * 60
            / 1000
            * 2;
        // Keep at most one partial 60ms chunk, including one split PCM byte.
        let mut remaining = audio;
        while !remaining.is_empty() {
            if generation != self.epoch() {
                self.audio_buffer.clear();
                return Err(RealtimeError::Interrupted);
            }
            let take = (chunk_bytes - self.audio_buffer.len()).min(remaining.len());
            self.audio_buffer.extend_from_slice(&remaining[..take]);
            remaining = &remaining[take..];
            if self.audio_buffer.len() == chunk_bytes {
                let chunk = std::mem::take(&mut self.audio_buffer);
                if !self
                    .write_current(
                        socket,
                        json!({"type":"input_audio_buffer.append", "audio":STANDARD.encode(chunk)}),
                        generation,
                    )
                    .await?
                {
                    self.audio_buffer.clear();
                    return Err(RealtimeError::Interrupted);
                }
            }
        }
        Ok(())
    }
    async fn flush_audio<S>(
        &mut self,
        socket: &mut S,
        generation: u64,
    ) -> Result<bool, RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        if generation != self.epoch() {
            self.audio_buffer.clear();
            return Ok(false);
        }
        if !self.audio_buffer.len().is_multiple_of(2) {
            return Err(RealtimeError::Invalid(
                "incomplete PCM sample at input boundary",
            ));
        }
        if !self.audio_buffer.is_empty() {
            let tail = std::mem::take(&mut self.audio_buffer);
            return self
                .write_current(
                    socket,
                    json!({"type":"input_audio_buffer.append", "audio":STANDARD.encode(tail)}),
                    generation,
                )
                .await;
        }
        Ok(true)
    }
    async fn write_action(
        &mut self,
        socket: &mut Socket,
        action: Action,
        generation: u64,
    ) -> Result<(), RealtimeError> {
        if self.config.require_native_receipts
            && matches!(
                action,
                Action::ToolResult { .. }
                    | Action::CheckedToolResult { .. }
                    | Action::CancelTool { .. }
            )
        {
            return Err(RealtimeError::Invalid(
                "native tool settlement requires its terminal receipt",
            ));
        }
        if self.finishing
            && !matches!(
                action,
                Action::Finish
                    | Action::ToolResult { .. }
                    | Action::CheckedToolResult { .. }
                    | Action::CancelTool { .. }
            )
        {
            return Err(RealtimeError::Invalid("session is draining"));
        }
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
                    return Err(RealtimeError::UnknownToolCall);
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
                    return Err(RealtimeError::UnknownToolCall);
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
                self.flush_audio(socket, generation).await?;
                self.finishing = true;
                return Ok(());
            }
            Action::UpdateSession(session) => {
                validate_session(&session)?;
                if self.config.session.input_sample_rate() != session.input_sample_rate()
                    || self.config.session.output_sample_rate() != session.output_sample_rate()
                    || self.config.session.is_manual() != session.is_manual()
                {
                    return Err(RealtimeError::Invalid(
                        "PCM rates and turn mode cannot change during a session",
                    ));
                }
                if self.pending_session_updates.len() >= self.config.limits.queue_capacity {
                    return Err(RealtimeError::Backpressure);
                }
                let snapshot = session.wire_snapshot();
                self.write(socket, json!({"type":"session.update", "session":snapshot}))
                    .await?;
                self.pending_session_updates.push_back((
                    snapshot,
                    Instant::now() + self.config.limits.operation_timeout,
                ));
                self.config.session = *session;
                return Ok(());
            }
            Action::AppendAudio(audio) => {
                self.append_audio(socket, &audio, generation).await?;
                return Ok(());
            }
            Action::CommitAudio => {
                if !self.config.session.is_manual() {
                    return Err(RealtimeError::Invalid(
                        "manual commit is disabled in provider VAD mode",
                    ));
                }
                if !self.flush_audio(socket, generation).await? {
                    return Err(RealtimeError::Interrupted);
                }
                if !self
                    .write_current(
                        socket,
                        json!({"type":"input_audio_buffer.commit"}),
                        generation,
                    )
                    .await?
                {
                    return Err(RealtimeError::Interrupted);
                }
                return Ok(());
            }
            Action::DiscardBufferedAudio => {
                self.audio_buffer.clear();
                return Ok(());
            }
            Action::ClearAudio => {
                self.audio_buffer.clear();
                json!({"type":"input_audio_buffer.clear"})
            }
            Action::CreateItem {
                item,
                previous_item_id,
            } => {
                self.results_awaiting_response = true;
                let mut v = json!({"type":"conversation.item.create", "item":item});
                if let Some(id) = previous_item_id {
                    v["previous_item_id"] = json!(id);
                }
                v
            }
            Action::RequestInference(options) => {
                if !self.results_awaiting_response {
                    if self
                        .tools
                        .values()
                        .any(|tool| tool.pending_outcome.is_some())
                    {
                        self.deferred_response =
                            Some((options, generation, self.inference_fence.clone(), false));
                    }
                    return Ok(());
                }
                if self.active.is_some() || self.pending.is_some() {
                    self.deferred_response =
                        Some((options, generation, self.inference_fence.clone(), false));
                    return Ok(());
                }
                self.deferred_response =
                    Some((options, generation, self.inference_fence.clone(), false));
                return self.maybe_continue(socket).await;
            }
            Action::CreateResponse(response) => {
                validate_modalities(response.modalities.as_deref())?;
                if self.active.is_some() || self.pending.is_some() {
                    self.deferred_response =
                        Some((response, generation, self.inference_fence.clone(), true));
                    return Ok(());
                }
                let fence = self.inference_fence.clone();
                if !self
                    .write_response(socket, &response, generation, fence.as_ref())
                    .await?
                {
                    return Ok(());
                }
                self.pending = Some(ResponseContext {
                    fence: self.inference_fence.clone(),
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
                if self.tools.get(&call_id).is_some_and(|tool| {
                    tool.cancel_on_interruption.is_some() || tool.pending_outcome.is_some()
                }) {
                    return Err(RealtimeError::Invalid(
                        "native tool settlement requires its terminal receipt",
                    ));
                }
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
            return Err(RealtimeError::UnknownToolCall);
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
            other => other.to_string(),
        };
        if !is_final {
            return Err(RealtimeError::Invalid(
                "Inworld supports only final tool results",
            ));
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
        // The paired native aggregator owns inference. A tool result never creates
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
            QueuedAction::Context { actions, fence } => {
                for action in actions {
                    let inference = matches!(
                        action,
                        Action::CreateResponse(_) | Action::RequestInference(_)
                    );
                    if inference && (generation != self.epoch() || !fence.is_current()) {
                        continue;
                    }
                    if inference {
                        self.inference_fence = Some(fence.clone());
                    }
                    let result = self.write_action(socket, action, generation).await;
                    self.inference_fence = None;
                    result?;
                    if inference {
                        if let Some((_, _, existing_fence, _)) = self.deferred_response.as_mut() {
                            *existing_fence = Some(fence.clone());
                        }
                    }
                }
                Ok(())
            }
            QueuedAction::ToolPolicy(call) => {
                if !call.cancel_on_interruption {
                    return Err(RealtimeError::Invalid(
                        "Inworld does not support noninterruptible tools",
                    ));
                }
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
            && (self.results_awaiting_response
                || self.deferred_response.as_ref().is_some_and(|value| value.3))
        {
            let (options, generation, fence, unconditional) = self
                .deferred_response
                .take()
                .expect("checked deferred response");
            if !self.results_awaiting_response && !unconditional {
                return Ok(());
            }
            if generation != self.epoch() || fence.as_ref().is_some_and(|fence| !fence.is_current())
            {
                return Ok(());
            }
            if !self
                .write_response(socket, &options, generation, fence.as_ref())
                .await?
            {
                return Ok(());
            }
            self.pending = Some(ResponseContext {
                fence: fence.clone(),
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
        self.audio_buffer.clear();
        if self.config.session.is_manual() {
            self.write(socket, json!({"type":"input_audio_buffer.clear"}))
                .await?;
        }
        if self.config.session.is_manual()
            || control.explicit_cancel_generation > self.handled_explicit_cancel
        {
            self.write(socket, json!({"type":"response.cancel"}))
                .await?;
            self.handled_explicit_cancel = control.explicit_cancel_generation;
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
            if self.finishing
                && self.active.is_none()
                && self.pending.is_none()
                && self.tools.is_empty()
                && self.pending_session_updates.is_empty()
            {
                return Ok(());
            }
            let deadline = self
                .deadline
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            let update_deadline = self
                .pending_session_updates
                .front()
                .map(|entry| entry.1)
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            let tool_deadline = self
                .tools
                .values()
                .map(|tool| tool.deadline)
                .min()
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
                _ = tokio::time::sleep_until(update_deadline), if self.ready && !self.pending_session_updates.is_empty() => return Err(RealtimeError::Timeout("session update")),
                _ = tokio::time::sleep_until(tool_deadline), if !self.tools.is_empty() => return Err(RealtimeError::Timeout("tool result")),
                item = socket.next() => {
                    match item {
                        Some(Ok(Message::Text(text))) => {
                            let wire = parse_wire(&text, self.config.limits.max_audio_chunk_bytes)?;
                            self.event(socket, wire).await?;
                        }
                        Some(Ok(Message::Ping(p))) => { timeout(self.config.limits.operation_timeout, socket.send(Message::Pong(p))).await.map_err(|_| RealtimeError::Timeout("pong"))?.map_err(transport)?; }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None if !self.ready => return Err(RealtimeError::Protocol("connection closed before session readiness")),
                        Some(Ok(Message::Close(_))) | None if self.active.is_some() || self.pending.is_some() => return Err(RealtimeError::Protocol("response ended before response.done")),
                        Some(Ok(Message::Close(_))) | None if !self.pending_session_updates.is_empty() => return Err(RealtimeError::Protocol("connection closed before session update acknowledgement")),
                        Some(Ok(Message::Close(_))) | None if !self.tools.is_empty() => return Err(RealtimeError::UnsettledTools),
                        Some(Ok(Message::Close(_))) | None => return Ok(()),
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
                    if fatal || (unacknowledged && failure.is_err() && !matches!(failure, Err(RealtimeError::Interrupted))) { return failure; }
                }
            }
        }
    }
    async fn event(&mut self, socket: &mut Socket, wire: WireEvent) -> Result<(), RealtimeError> {
        let mut event = wire.event;
        if matches!(event, Event::Unknown) {
            self.unknown_events += 1;
            if self.unknown_events > self.config.limits.max_unknown_events {
                return Err(RealtimeError::Protocol("unknown event budget exhausted"));
            }
            return Ok(());
        }
        if !self.ready
            && !matches!(
                event,
                Event::SessionCreated { .. }
                    | Event::SessionUpdated { .. }
                    | Event::ConversationCreated { .. }
                    | Event::Unknown
                    | Event::Error { .. }
            )
        {
            return Err(RealtimeError::Protocol(
                "provider work before session readiness",
            ));
        }
        if let Event::SessionUpdated { session } = &event {
            let (expected, _) = self
                .pending_session_updates
                .pop_front()
                .ok_or(RealtimeError::Protocol("unsolicited session update"))?;
            if !session_matches(&expected, session) {
                return Err(RealtimeError::Protocol(
                    "session acknowledgement differs from requested configuration",
                ));
            }
            self.ready = true;
            self.shared_ready.store(true, Ordering::Release);
        }
        match &event {
            Event::SessionCreated { session } => {
                if self.initial_update_sent {
                    return Err(RealtimeError::Protocol("duplicate session creation"));
                }
                self.session_id = session.as_ref().and_then(|s| s.id.clone());
                self.write_action(
                    socket,
                    Action::UpdateSession(Box::new(self.config.session.clone())),
                    self.epoch(),
                )
                .await?;
                self.initial_update_sent = true;
            }
            Event::SessionUpdated { session } => {
                if let Some(id) = &session.id {
                    if self
                        .session_id
                        .as_ref()
                        .is_some_and(|existing| existing != id)
                    {
                        return Err(RealtimeError::Protocol("session identity changed"));
                    }
                    self.session_id = Some(id.clone());
                }
            }
            Event::ConversationCreated { conversation } => {
                self.conversation_id = Some(conversation.id.clone());
            }
            Event::Error { .. } => return Err(RealtimeError::Protocol("provider error")),
            Event::ResponseDone { response, .. }
                if matches!(
                    response.status,
                    ResponseStatus::Failed | ResponseStatus::Incomplete
                ) =>
            {
                return Err(RealtimeError::Protocol(
                    "provider response did not complete",
                ));
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
                self.deferred_response = None;
                let context = self.pending.take().unwrap_or_else(|| ResponseContext {
                    fence: self.inference_fence.clone(),
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
        // Inworld omits response/item IDs on tool completion. Resolve them only
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
            _ => {}
        }
        let tool_context = match &event {
            Event::FunctionArgumentsDone { call_id, .. }
            | Event::FunctionArgumentsDelta { call_id, .. } => {
                self.tools.get(call_id).map(|t| ResponseContext {
                    fence: t.fence.clone(),
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
        let stale = event.response_id().is_some()
            && (generation != self.epoch()
                || context
                    .as_ref()
                    .and_then(|context| context.fence.as_ref())
                    .is_some_and(|fence| !fence.is_current()));
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
                        return Err(RealtimeError::Protocol("reused tool call identity"));
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
                                fence: response_context.fence.clone(),
                                name: name.clone(),
                                item_id: id.clone(),
                                generation: response_context.generation,
                                arguments: arguments.clone(),
                                announced: false,
                                cancel_on_interruption: None,
                                pending_outcome: None,
                                context_id: response_context.context_id,
                                response_id,
                                deadline: Instant::now() + self.config.limits.response_timeout,
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
                        if name.as_ref().is_some_and(|name| name != &tool.name)
                            || serde_json::from_str::<Value>(&tool.arguments).ok()
                                != serde_json::from_str::<Value>(arguments).ok()
                        {
                            return Err(RealtimeError::Protocol(
                                "conflicting repeated tool completion",
                            ));
                        }
                        return Ok(());
                    }
                    if name.as_ref().is_some_and(|name| name != &tool.name) {
                        return Err(RealtimeError::Protocol("tool name mismatch"));
                    }
                    if !serde_json::from_str::<Value>(arguments).is_ok_and(|v| v.is_object()) {
                        return Err(RealtimeError::Protocol(
                            "tool arguments must be a JSON object",
                        ));
                    }
                    if !tool.arguments.is_empty()
                        && serde_json::from_str::<Value>(&tool.arguments).ok().as_ref()
                            != serde_json::from_str::<Value>(arguments).ok().as_ref()
                    {
                        return Err(RealtimeError::Protocol(
                            "tool arguments differ from tracked input",
                        ));
                    }
                    *name = Some(tool.name.clone());
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
        let fence = context.as_ref().and_then(|context| context.fence.clone());
        let context_id = context
            .and_then(|c| c.context_id)
            .or_else(|| self.config.context_id.clone());
        redact_event(&mut event, &self.config.api_key);
        self.tx
            .try_send(FencedEvent {
                fence,
                event: RealtimeEvent {
                    event_id: wire.event_id,
                    event_type: wire.event_type,
                    session_id: self.session_id.clone(),
                    conversation_id: self.conversation_id.clone(),
                    context_id,
                    generation,
                    event,
                },
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })
    }
}
fn session_matches(expected: &SessionProperties, actual: &SessionProperties) -> bool {
    fn contains(expected: &Value, actual: &Value) -> bool {
        match (expected, actual) {
            (Value::Object(expected), Value::Object(actual)) => {
                expected.iter().all(|(key, value)| {
                    actual
                        .get(key)
                        .is_some_and(|actual| contains(value, actual))
                })
            }
            _ => expected == actual,
        }
    }
    match (serde_json::to_value(expected), serde_json::to_value(actual)) {
        (Ok(expected), Ok(actual)) => contains(&expected, &actual),
        _ => false,
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
        Event::Error { error } => redact(error, key),
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
    if let Action::AppendAudio(bytes) = action {
        return if bytes.is_empty() {
            Err(RealtimeError::Invalid("empty audio chunk"))
        } else if bytes.len() > limits.max_audio_chunk_bytes {
            Err(RealtimeError::TooLarge)
        } else {
            Ok(())
        };
    }
    if matches!(
        action,
        Action::CheckedToolResult {
            is_final: false,
            ..
        } | Action::ToolResult {
            is_final: false,
            ..
        }
    ) {
        return Err(RealtimeError::Invalid(
            "Inworld supports only final tool results",
        ));
    }
    if let Action::CreateItem {
        item,
        previous_item_id,
    } = action
    {
        if previous_item_id.is_some() {
            return Err(RealtimeError::Invalid("history insertion is unsupported"));
        }
        if !matches!(item, ConversationItem::Message { role: Role::User, content, .. } if !content.is_empty() && content.iter().all(|p| matches!(p, ItemContent::InputText { .. })))
        {
            return Err(RealtimeError::Invalid(
                "only explicit user text items are supported",
            ));
        }
        if item.id().is_empty() || item.id().len() > 256 {
            return Err(RealtimeError::Invalid("item ID"));
        }
    }
    if let Action::UpdateSession(session) = action {
        validate_session(session)?;
    }
    if let Action::CreateResponse(options) | Action::RequestInference(options) = action {
        validate_modalities(options.modalities.as_deref())?;
        if options.context_id.as_ref().is_some_and(|id| id.len() > 256) {
            return Err(RealtimeError::Invalid("context ID exceeds 256 bytes"));
        }
    }
    bounded_serialized(action, limits.max_message_bytes)
}

#[cfg(test)]
mod inference_admission_tests {
    use super::*;
    use std::task::Context;
    struct PendingSink {
        before: bool,
        ready: bool,
        flush_ready: bool,
        sent: usize,
        polls: usize,
    }
    impl Sink<Message> for PendingSink {
        type Error = tungstenite::Error;
        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            self.polls += 1;
            if self.before && !self.ready {
                self.ready = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn start_send(mut self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
            self.sent += 1;
            Ok(())
        }
        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if !self.flush_ready {
                self.flush_ready = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test]
    async fn pending_before_ready_rechecks_generation_without_start_send() {
        let mut socket = PendingSink {
            before: true,
            ready: false,
            flush_ready: false,
            sent: 0,
            polls: 0,
        };
        let generation = AtomicU64::new(0);
        let gate = Mutex::new(());
        let health = Arc::new(Mutex::new(None));
        {
            let operation = send_inference(
                &mut socket,
                Message::Text("inference".into()),
                &generation,
                0,
                &gate,
                &health,
                None,
            );
            tokio::pin!(operation);
            assert!(futures_util::poll!(&mut operation).is_pending());
            generation.store(1, Ordering::SeqCst);
            assert!(!operation.await.unwrap());
        }
        assert_eq!(socket.sent, 0);
    }
    #[tokio::test]
    async fn pending_flush_after_admission_flushes_once_without_retry() {
        let mut socket = PendingSink {
            before: false,
            ready: true,
            flush_ready: false,
            sent: 0,
            polls: 0,
        };
        let generation = AtomicU64::new(0);
        let gate = Mutex::new(());
        let health = Arc::new(Mutex::new(None));
        {
            let operation = send_inference(
                &mut socket,
                Message::Text("inference".into()),
                &generation,
                0,
                &gate,
                &health,
                None,
            );
            tokio::pin!(operation);
            assert!(futures_util::poll!(&mut operation).is_pending());
            generation.store(1, Ordering::SeqCst);
            assert!(operation.await.unwrap());
        }
        assert_eq!(socket.sent, 1);
        assert_eq!(socket.polls, 1);
    }
}

#[cfg(test)]
mod original_fence_admission_tests {
    use super::*;
    use pipecat_core::{
        async_trait, Direction, Frame, FrameData, FrameProcessor, InvocationFence, Pipeline,
        PipelineConfig, ProcessorContext, ProcessorError,
    };
    use std::task::Context;
    struct Capture(Arc<Mutex<Option<InvocationFence>>>);
    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "InferenceFenceCapture"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Text(_)) {
                *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
            }
            ctx.push(frame, direction).await
        }
    }
    struct PendingSink {
        before: bool,
        ready: bool,
        flushed: bool,
        sent: usize,
    }
    impl Sink<Message> for PendingSink {
        type Error = tungstenite::Error;
        fn poll_ready(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.before && !self.ready {
                self.ready = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn start_send(mut self: Pin<&mut Self>, _: Message) -> Result<(), Self::Error> {
            self.sent += 1;
            Ok(())
        }
        fn poll_flush(
            mut self: Pin<&mut Self>,
            cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if !self.flushed {
                self.flushed = true;
                cx.waker().wake_by_ref();
                Poll::Pending
            } else {
                Poll::Ready(Ok(()))
            }
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test]
    async fn original_fence_rechecks_ready_but_does_not_revoke_admitted_flush() {
        for before in [true, false] {
            let saved = Arc::new(Mutex::new(None));
            let mut worker = Pipeline::new(vec![Arc::new(Capture(saved.clone()))])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            worker
                .sender()
                .send(Frame::text("capture"), Direction::Downstream)
                .await
                .unwrap();
            loop {
                if matches!(worker.recv().await.unwrap().frame.data, FrameData::Text(_)) {
                    break;
                }
            }
            let fence = saved.lock().unwrap().take().unwrap();
            let generation = AtomicU64::new(0);
            let gate = Mutex::new(());
            let health = Arc::new(Mutex::new(None));
            let mut socket = PendingSink {
                before,
                ready: false,
                flushed: false,
                sent: 0,
            };
            {
                let operation = send_inference(
                    &mut socket,
                    Message::Text("response.create".into()),
                    &generation,
                    0,
                    &gate,
                    &health,
                    Some(&fence),
                );
                tokio::pin!(operation);
                assert!(futures_util::poll!(&mut operation).is_pending());
                worker
                    .sender()
                    .send(Frame::interruption(), Direction::Downstream)
                    .await
                    .unwrap();
                loop {
                    if matches!(
                        worker.recv().await.unwrap().frame.data,
                        FrameData::Interruption
                    ) {
                        break;
                    }
                }
                assert!(!fence.is_current());
                assert_eq!(operation.await.unwrap(), !before);
            }
            assert_eq!(socket.sent, usize::from(!before));
            assert_eq!(generation.load(Ordering::SeqCst), 0);
            worker.end().await.unwrap();
            worker.collect().await.unwrap();
        }
    }
}

#[cfg(test)]
mod cleanup_owner_tests {
    use super::*;

    fn gated_session(
        primary: Result<(), RealtimeError>,
        cleanup: Option<RealtimeError>,
    ) -> (InworldSession, oneshot::Sender<()>) {
        let (release, gate) = oneshot::channel();
        let (tx, _) = mpsc::channel(4);
        let (events, rx) = mpsc::channel(4);
        let (control, _) = watch::channel(Control::default());
        let (terminal_tx, terminal) = watch::channel(None);
        let cleanup_error = Arc::new(Mutex::new(None));
        let cleanup_observer = cleanup_error.clone();
        let actor = tokio::spawn(async move {
            let _ = gate.await;
            *cleanup_observer.lock().unwrap() = cleanup;
            drop(events);
            primary
        });
        let (task, actor_abort) = spawn_cleanup_owner(actor, terminal_tx);
        let sender = InworldSender {
            ready: Arc::new(AtomicBool::new(true)),
            health_gate: Arc::new(Mutex::new(None)),
            tx,
            control,
            generation: Arc::new(AtomicU64::new(0)),
            emission_gate: Arc::new(Mutex::new(())),
            limits: RealtimeLimits {
                operation_timeout: Duration::from_secs(2),
                close_timeout: Duration::from_secs(2),
                ..RealtimeLimits::default()
            },
            require_native_receipts: false,
            projected_session: Arc::new(Mutex::new(SessionProperties::pcm())),
            terminal,
        };
        (
            InworldSession {
                sender,
                rx,
                task: Some(task),
                actor_abort,
                runtime: tokio::runtime::Handle::current(),
                cleanup_error,
                ended: false,
            },
            release,
        )
    }

    #[tokio::test]
    async fn cancelled_shutdown_waiter_keeps_owner_until_cleanup_gate_opens() {
        let (mut session, release) = gated_session(Ok(()), None);
        {
            let shutdown = session.shutdown();
            tokio::pin!(shutdown);
            assert!(futures_util::poll!(&mut shutdown).is_pending());
        }
        assert!(
            session.task.is_some(),
            "cancelled waiter lost the join owner"
        );
        assert!(session.sender.terminal.borrow().is_none());
        release.send(()).unwrap();
        session.shutdown().await.unwrap();
        assert!(session.task.is_none());
        assert_eq!(*session.sender.terminal.borrow(), Some(Ok(())));
        session.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn dropped_session_transfers_join_owner_and_terminal_waits_for_actor() {
        let (session, release) = gated_session(Ok(()), None);
        let sender = session.sender();
        let mut terminal = sender.terminal.clone();
        drop(session);
        tokio::task::yield_now().await;
        assert!(terminal.borrow().is_none());
        assert_eq!(
            sender.try_send(Action::ClearAudio),
            Err(RealtimeError::Closed)
        );
        release.send(()).unwrap();
        timeout(Duration::from_secs(2), terminal.changed())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(*terminal.borrow(), Some(Ok(())));
    }

    #[tokio::test]
    async fn primary_failure_is_retained_separately_from_cleanup_failure() {
        let primary = RealtimeError::Protocol("synthetic primary failure");
        let cleanup = RealtimeError::Timeout("synthetic cleanup");
        let (mut session, release) = gated_session(Err(primary.clone()), Some(cleanup.clone()));
        release.send(()).unwrap();
        assert_eq!(session.shutdown().await, Err(primary.clone()));
        assert_eq!(session.cleanup_error(), Some(cleanup));
        assert_eq!(session.shutdown().await, Err(primary));
    }

    #[test]
    fn strict_configuration_refuses_unsupported_fields_and_forced_tool_continuation() {
        let mut snapshot = serde_json::to_value(SessionProperties::pcm()).unwrap();
        snapshot["unknown_capability"] = json!(true);
        assert!(serde_json::from_value::<SessionProperties>(snapshot).is_err());
        assert!(serde_json::from_value::<FunctionTool>(json!({"type":"mcp","name":"not-function","description":"synthetic","parameters":{"type":"object"}})).is_err());
        let mut session = SessionProperties::pcm();
        session.provider_data =
            Some(serde_json::from_value(json!({"auto_tool_response":true})).unwrap());
        assert!(session.validate().is_err());
    }

    #[test]
    fn wire_defaults_are_reserved_without_mutating_original_settings() {
        let mut session = SessionProperties::manual();
        session.provider_data =
            Some(serde_json::from_value(json!({"metadata":{"tenant":"synthetic"}})).unwrap());
        let original = session.clone();
        let wire = serde_json::to_value(session.wire_snapshot()).unwrap();
        assert_eq!(session, original);
        assert_eq!(wire["providerData"]["auto_tool_response"], false);
        assert_eq!(wire["providerData"]["metadata"]["tenant"], "synthetic");
        assert_eq!(wire["providerData"]["metadata"]["sdk"], "pipecat-realtime");
        assert!(wire["audio"]["input"]["turn_detection"].is_null());
    }
}

#[cfg(test)]
mod interrupted_audio_tests {
    use super::*;
    use std::task::Context;

    struct InterruptAfterFlush {
        epoch: Arc<AtomicU64>,
        messages: Vec<Message>,
        interrupt: bool,
    }
    impl Sink<Message> for InterruptAfterFlush {
        type Error = tungstenite::Error;
        fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn start_send(mut self: Pin<&mut Self>, message: Message) -> Result<(), Self::Error> {
            self.messages.push(message);
            Ok(())
        }
        fn poll_flush(
            mut self: Pin<&mut Self>,
            _: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            if self.interrupt {
                self.epoch.fetch_add(1, Ordering::SeqCst);
                self.interrupt = false;
            }
            Poll::Ready(Ok(()))
        }
        fn poll_close(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
    }
    fn actor_and_sink() -> (Actor, InterruptAfterFlush) {
        let generation = Arc::new(AtomicU64::new(0));
        let (events, _) = mpsc::channel(4);
        let actor = Actor::new(
            InworldConfig::new("synthetic"),
            generation.clone(),
            events,
            Arc::new(AtomicBool::new(true)),
            Arc::new(Mutex::new(None)),
            Arc::new(Mutex::new(())),
        );
        (
            actor,
            InterruptAfterFlush {
                epoch: generation,
                messages: Vec::new(),
                interrupt: true,
            },
        )
    }
    #[tokio::test]
    async fn interruption_after_first_chunk_never_admits_remaining_chunks_or_tail() {
        let (mut actor, mut sink) = actor_and_sink();
        let bytes = vec![7; 2880 * 3 + 2];
        assert_eq!(
            actor.append_audio(&mut sink, &bytes, 0).await,
            Err(RealtimeError::Interrupted)
        );
        assert_eq!(sink.messages.len(), 1);
        let message: Value = serde_json::from_str(sink.messages[0].to_text().unwrap()).unwrap();
        assert_eq!(
            STANDARD.decode(message["audio"].as_str().unwrap()).unwrap(),
            vec![7; 2880]
        );
        assert!(actor.audio_buffer.is_empty());
        assert!(!actor.flush_audio(&mut sink, 0).await.unwrap());
        assert_eq!(sink.messages.len(), 1);
    }
    #[tokio::test]
    async fn stale_tail_is_discarded_before_validation_or_any_socket_admission() {
        let (mut actor, mut sink) = actor_and_sink();
        actor.audio_buffer = vec![7];
        sink.epoch.store(1, Ordering::SeqCst);
        assert!(!actor.flush_audio(&mut sink, 0).await.unwrap());
        assert!(actor.audio_buffer.is_empty());
        assert!(sink.messages.is_empty());
    }
    #[tokio::test]
    async fn admitted_tail_flush_does_not_admit_a_commit_after_interruption() {
        let (mut actor, mut sink) = actor_and_sink();
        actor.audio_buffer = vec![1, 2];
        assert!(actor.flush_audio(&mut sink, 0).await.unwrap());
        assert!(!actor
            .write_current(&mut sink, json!({"type":"input_audio_buffer.commit"}), 0)
            .await
            .unwrap());
        assert_eq!(sink.messages.len(), 1);
    }
}
