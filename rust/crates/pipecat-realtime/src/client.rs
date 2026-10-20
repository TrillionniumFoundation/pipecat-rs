use crate::types::*;
use crate::usage_details::UsageDetails;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{Sink, SinkExt, StreamExt};
use pipecat_core::{InvocationFence, NativeToolProgressAdmission, NativeToolProgressPermission};
#[path = "client_progress.rs"]
mod progress;
pub(crate) use progress::{selected_options_bytes, SelectedAlternative};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
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
    task::JoinHandle,
    time::{timeout, timeout_at, Instant},
};
use tokio_tungstenite::{
    tungstenite::{
        self, client::IntoClientRequest, http::HeaderValue, protocol::WebSocketConfig, Message,
    },
    MaybeTlsStream, WebSocketStream,
};

mod cancel_receipts;
use cancel_receipts::{CancelReceipt, CancelReceipts};

mod preroll;
pub(crate) use preroll::bytes_for_duration as preroll_bytes_for_duration;
use preroll::Preroll;

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type ResponseFences = Arc<Mutex<HashMap<String, InvocationFence>>>;

/// Readiness is not admission. Check the original fence on every readiness
/// poll and hold it through start_send. Once accepted, flush exactly once even
/// if the source is then interrupted; there is no safe remote rollback/retry.
async fn send_fenced<S, Item>(
    sink: &mut S,
    item: Item,
    fence: Option<&InvocationFence>,
) -> Result<bool, S::Error>
where
    S: Sink<Item> + Unpin,
{
    let mut item = Some(item);
    let admitted = futures_util::future::poll_fn(|cx| {
        let mut admit = || match Pin::new(&mut *sink).poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(
                Pin::new(&mut *sink)
                    .start_send(item.take().expect("one sink admission"))
                    .map(|()| true),
            ),
        };
        match fence {
            Some(fence) => fence.with_current(admit).unwrap_or(Poll::Ready(Ok(false))),
            None => admit(),
        }
    })
    .await?;
    if admitted {
        futures_util::future::poll_fn(|cx| Pin::new(&mut *sink).poll_flush(cx)).await?;
    }
    Ok(admitted)
}

// Epoch admission shares the sender gate through readiness and start_send.
// An admitted frame is flushed once even if interrupted while flushing.
async fn send_epoch_fenced<S, Item>(
    sink: &mut S,
    item: Item,
    generation: &AtomicU64,
    gate: &Mutex<()>,
    expected: u64,
) -> Result<bool, S::Error>
where
    S: Sink<Item> + Unpin,
{
    let mut item = Some(item);
    let admitted = futures_util::future::poll_fn(|cx| {
        let _guard = gate.lock().unwrap_or_else(|e| e.into_inner());
        if generation.load(Ordering::Acquire) != expected {
            return Poll::Ready(Ok(false));
        }
        match Pin::new(&mut *sink).poll_ready(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Ready(Ok(())) => Poll::Ready(
                Pin::new(&mut *sink)
                    .start_send(item.take().expect("one epoch admission"))
                    .map(|()| true),
            ),
        }
    })
    .await?;
    if admitted {
        futures_util::future::poll_fn(|cx| Pin::new(&mut *sink).poll_flush(cx)).await?;
    }
    Ok(admitted)
}

/// Camera items are ephemeral input, unlike public durable CreateItem edits.
/// Both original qualifications cover each readiness/start_send poll. Preserve
/// the established sender-gate -> runtime-admission lock order, with no await
/// under either lock. An admitted frame gets exactly one flush afterward.
async fn send_camera_fenced<S, Item>(
    sink: &mut S,
    item: Item,
    generation: &AtomicU64,
    gate: &Mutex<()>,
    expected: u64,
    fence: &InvocationFence,
) -> Result<bool, S::Error>
where
    S: Sink<Item> + Unpin,
{
    let mut item = Some(item);
    let admitted = futures_util::future::poll_fn(|cx| {
        let _guard = gate.lock().unwrap_or_else(|e| e.into_inner());
        if generation.load(Ordering::Acquire) != expected {
            return Poll::Ready(Ok(false));
        }
        fence
            .with_current(|| match Pin::new(&mut *sink).poll_ready(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
                Poll::Ready(Ok(())) => Poll::Ready(
                    Pin::new(&mut *sink)
                        .start_send(item.take().expect("one camera admission"))
                        .map(|()| true),
                ),
            })
            .unwrap_or(Poll::Ready(Ok(false)))
    })
    .await?;
    if admitted {
        futures_util::future::poll_fn(|cx| Pin::new(&mut *sink).poll_flush(cx)).await?;
    }
    Ok(admitted)
}

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
}
#[derive(Debug, Clone)]
pub struct RealtimeLimits {
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_audio_chunk_bytes: usize,
    pub max_outstanding_tools: usize,
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
            || self.connect_timeout.is_zero()
            || self.operation_timeout.is_zero()
            || self.response_timeout.is_zero()
            || self.close_timeout.is_zero()
        {
            return Err(RealtimeError::Invalid("limits outside supported bounds"));
        }
        Ok(())
    }
}
/// An explicitly supplied Azure OpenAI Realtime credential. Tokens are not
/// obtained, refreshed, persisted, or replaced with another credential.
#[derive(Clone)]
pub enum AzureRealtimeCredential {
    ApiKey(String),
    EntraBearer(String),
}
impl fmt::Debug for AzureRealtimeCredential {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiKey(_) => f.write_str("ApiKey([REDACTED])"),
            Self::EntraBearer(_) => f.write_str("EntraBearer([REDACTED])"),
        }
    }
}
impl AzureRealtimeCredential {
    fn secret(&self) -> &str {
        match self {
            Self::ApiKey(value) | Self::EntraBearer(value) => value,
        }
    }
}
#[derive(Clone)]
enum ConnectionProfile {
    OpenAi {
        api_key: String,
    },
    Azure {
        deployment: String,
        credential: AzureRealtimeCredential,
    },
}
impl ConnectionProfile {
    fn secret(&self) -> &str {
        match self {
            Self::OpenAi { api_key } => api_key,
            Self::Azure { credential, .. } => credential.secret(),
        }
    }
    fn authentication(&self) -> Result<(&'static str, HeaderValue), RealtimeError> {
        let (name, value) = match self {
            Self::Azure {
                credential: AzureRealtimeCredential::ApiKey(key),
                ..
            } => ("api-key", HeaderValue::from_str(key)),
            _ => (
                "authorization",
                HeaderValue::from_str(&format!("Bearer {}", self.secret())),
            ),
        };
        let mut value = value.map_err(|_| RealtimeError::Invalid("authentication header"))?;
        value.set_sensitive(true);
        Ok((name, value))
    }
}
#[derive(Clone)]
pub struct RealtimeConfig {
    pub endpoint: String,
    pub model: String,
    pub session: SessionProperties,
    pub context_id: Option<String>,
    pub limits: RealtimeLimits,
    profile: ConnectionProfile,
}
impl fmt::Debug for RealtimeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealtimeConfig")
            .field("endpoint", &"[configured]")
            .field(
                "model",
                &if self.is_azure() {
                    "[configured deployment]"
                } else {
                    &self.model
                },
            )
            .field("api_key", &"[REDACTED]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl RealtimeConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.openai.com/v1/realtime".into(),
            model: "gpt-realtime-2.1".into(),
            session: SessionProperties::pcm(),
            context_id: None,
            limits: RealtimeLimits::default(),
            profile: ConnectionProfile::OpenAi {
                api_key: api_key.into(),
            },
        }
    }
    /// Select Azure OpenAI's GA v1 endpoint and an explicit deployment.
    ///
    /// The endpoint must end in `/openai/v1/realtime`, with either no query or
    /// exactly one `model` query value equal to the deployment. Preview and
    /// Voice Live endpoints are deliberately unsupported. Production requires
    /// WSS; plaintext is allowed only for literal loopback-IP test peers.
    ///
    /// Endpoint, deployment, and credential bounds are 8192, 256, and 16384
    /// bytes respectively. Public-field edits are revalidated before connection;
    /// they cannot change the deployment selected by this constructor.
    pub fn azure(
        endpoint: impl Into<String>,
        deployment: impl Into<String>,
        credential: AzureRealtimeCredential,
    ) -> Result<Self, RealtimeError> {
        let deployment = deployment.into();
        let mut session = SessionProperties::pcm();
        session.model = Some(deployment.clone());
        let config = Self {
            endpoint: endpoint.into(),
            model: deployment.clone(),
            session,
            context_id: None,
            limits: RealtimeLimits::default(),
            profile: ConnectionProfile::Azure {
                deployment,
                credential,
            },
        };
        config.validate()?;
        Ok(config)
    }
    pub fn is_azure(&self) -> bool {
        matches!(self.profile, ConnectionProfile::Azure { .. })
    }
    pub fn service_name(&self) -> &'static str {
        if self.is_azure() {
            "AzureOpenAiRealtimeService"
        } else {
            "OpenAiRealtimeService"
        }
    }
    fn connection_url(&self) -> Result<url::Url, RealtimeError> {
        let mut url = match &self.profile {
            ConnectionProfile::OpenAi { .. } => endpoint(&self.endpoint)?,
            ConnectionProfile::Azure { deployment, .. } => {
                azure_endpoint(&self.endpoint, deployment)?
            }
        };
        if url.query().is_none() {
            url.query_pairs_mut().append_pair("model", &self.model);
        }
        Ok(url)
    }
    pub fn validate(&self) -> Result<(), RealtimeError> {
        self.limits.validate()?;
        if let ConnectionProfile::Azure {
            deployment,
            credential,
        } = &self.profile
        {
            if deployment.trim().is_empty()
                || deployment.len() > 256
                || deployment.chars().any(char::is_control)
            {
                return Err(RealtimeError::Invalid("Azure deployment"));
            }
            if self.model != *deployment {
                return Err(RealtimeError::Invalid(
                    "Azure deployment cannot change after configuration",
                ));
            }
            let secret = credential.secret();
            if secret.trim().is_empty()
                || secret.len() > 16_384
                || secret.chars().any(char::is_control)
            {
                return Err(RealtimeError::Invalid("Azure credential"));
            }
        }
        self.connection_url()?;
        if self.context_id.as_ref().is_some_and(|id| id.len() > 256) {
            return Err(RealtimeError::Invalid("context ID exceeds 256 bytes"));
        }
        if self.model.trim().is_empty()
            || self.model.len() > 256
            || self.profile.secret().is_empty()
        {
            return Err(RealtimeError::Invalid("model and API key are required"));
        }
        self.profile.authentication()?;
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
fn azure_endpoint(s: &str, deployment: &str) -> Result<url::Url, RealtimeError> {
    if s.len() > 8192 || s.trim() != s || s.chars().any(char::is_control) {
        return Err(RealtimeError::Invalid("Azure endpoint URL"));
    }
    let url = endpoint_base(s, true)?;
    // URL parsing normalizes dot paths, abbreviated IPv4 hosts, and empty
    // userinfo. Those are not the explicit canonical endpoint contract.
    let remainder = s
        .split_once("://")
        .ok_or(RealtimeError::Invalid("Azure endpoint URL"))?
        .1;
    let authority_end = remainder.find(['/', '?', '#']).unwrap_or(remainder.len());
    let authority = &remainder[..authority_end];
    let raw_path = remainder[authority_end..]
        .split(['?', '#'])
        .next()
        .unwrap_or_default();
    if authority.contains('@') || raw_path != "/openai/v1/realtime" {
        return Err(RealtimeError::Invalid(
            "Azure requires the GA v1 realtime endpoint",
        ));
    }
    if url.scheme() == "ws" {
        let host = if let Some(ipv6) = authority.strip_prefix('[') {
            ipv6.split(']').next().unwrap_or_default()
        } else {
            authority.split(':').next().unwrap_or_default()
        };
        if !host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback()) {
            return Err(RealtimeError::Invalid(
                "plaintext requires an explicit loopback IP address",
            ));
        }
    }
    if let Some(query) = url.query() {
        let mut pairs = url.query_pairs();
        if query.contains('&')
            || !matches!(pairs.next(), Some((key, value)) if key == "model" && value == deployment)
            || pairs.next().is_some()
        {
            return Err(RealtimeError::Invalid(
                "Azure endpoint query must name exactly the configured deployment",
            ));
        }
    }
    Ok(url)
}
fn endpoint(s: &str) -> Result<url::Url, RealtimeError> {
    endpoint_base(s, false)
}
fn endpoint_base(s: &str, allow_query: bool) -> Result<url::Url, RealtimeError> {
    let url = url::Url::parse(s).map_err(|_| RealtimeError::Invalid("endpoint URL"))?;
    if !matches!(url.scheme(), "ws" | "wss")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || !allow_query && url.query().is_some()
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
    if let Some(modalities) = &s.output_modalities {
        if modalities.len() != 1 {
            return Err(RealtimeError::Invalid(
                "exactly one output modality is required",
            ));
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
            if matches!(format, AudioFormat::Pcm { rate } if *rate != 24_000) {
                return Err(RealtimeError::Invalid("Realtime PCM requires 24000 Hz"));
            }
        }
        if let Some(TurnDetection::ServerVad { threshold, .. }) =
            audio.input.as_ref().and_then(|i| i.turn_detection.as_ref())
        {
            if !threshold.is_finite() || !(0.0..=1.0).contains(threshold) {
                return Err(RealtimeError::Invalid("VAD threshold"));
            }
        }
        if audio
            .output
            .as_ref()
            .and_then(|o| o.speed)
            .is_some_and(|s| !s.is_finite() || !(0.25..=1.5).contains(&s))
        {
            return Err(RealtimeError::Invalid("audio speed"));
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
    pub(crate) pipeline_turns: bool,
    pub(crate) audio_preroll_bytes: Option<usize>,
    pub(crate) final_tool_app: bool,
}
impl RealtimeClient {
    pub fn config(&self) -> &RealtimeConfig {
        &self.config
    }
    pub fn new(config: RealtimeConfig) -> Result<Self, RealtimeError> {
        config.validate()?;
        Ok(Self {
            config,
            pipeline_turns: false,
            audio_preroll_bytes: None,
            final_tool_app: false,
        })
    }
    /// Opens a single authenticated connection. Redirects and automatic retries
    /// are deliberately unsupported. The paired manual-turn processor alone may
    /// replay its bounded PCM suffix within this same connection on interruption.
    pub async fn connect(&self) -> Result<RealtimeSession, RealtimeError> {
        let c = &self.config;
        c.validate()?;
        let url = c.connection_url()?;
        let mut req = url
            .as_str()
            .into_client_request()
            .map_err(|_| RealtimeError::Invalid("WebSocket request"))?;
        let (header, value) = c.profile.authentication()?;
        req.headers_mut().insert(header, value);
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
        let generation = Arc::new(AtomicU64::new(0));
        let emission_gate = Arc::new(Mutex::new(()));
        let response_fences = Arc::new(Mutex::new(HashMap::new()));
        let sender = RealtimeSender {
            tx,
            control: control_tx,
            generation: generation.clone(),
            emission_gate: emission_gate.clone(),
            limits: c.limits.clone(),
            response_fences: response_fences.clone(),
        };
        let mut actor = Actor::new(c.clone(), generation.clone(), emission_gate, events_tx);
        actor.response_fences = response_fences;
        actor.pipeline_turns = self.pipeline_turns;
        actor.final_tool_app = self.final_tool_app;
        actor.audio_preroll = self.audio_preroll_bytes.map(Preroll::new);
        // session.update precedes every user action, including appended audio.
        actor
            .write_action(
                &mut socket,
                Action::UpdateSession(c.session.clone()),
                0,
                None,
            )
            .await?;
        let task = tokio::spawn(async move {
            let result = actor.run(&mut socket, rx, control_rx).await;
            let _ = timeout(actor.config.limits.close_timeout, socket.close(None)).await;
            result
        });
        Ok(RealtimeSession {
            sender,
            rx: events_rx,
            task: Some(task),
            ended: false,
            joined: Arc::new(AtomicBool::new(false)),
        })
    }
}
#[derive(Debug, Clone, Default)]
struct Control {
    generation: u64,
    playback: Option<PlaybackPosition>,
    close: bool,
    preroll_bytes: Option<usize>,
}
struct Command {
    action: Option<Action>,
    progress_admission: Option<NativeToolProgressAdmission>,
    selected_permission: Option<NativeToolProgressPermission>,
    selected_alternatives: Option<Vec<progress::SelectedAlternative>>,
    generation: u64,
    reply: Option<oneshot::Sender<Result<(), RealtimeError>>>,
    inference_fence: Option<InvocationFence>,
    // Only the adapter's private camera admission may set this. Public image
    // CreateItem actions retain their existing durable-history semantics.
    camera_fence: Option<InvocationFence>,
}
impl Command {
    fn is_durable(&self) -> bool {
        self.progress_admission.is_some()
            || (self.camera_fence.is_none()
                && matches!(
                    self.action,
                    Some(
                        Action::ToolResult { .. }
                            | Action::CreateItem { .. }
                            | Action::UpdateSession(_)
                    )
                ))
    }
    fn camera_is_stale(&self, generation: u64, finishing: bool) -> bool {
        self.camera_fence
            .as_ref()
            .is_some_and(|fence| finishing || self.generation != generation || !fence.is_current())
    }
}
#[derive(Clone)]
pub struct RealtimeSender {
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    generation: Arc<AtomicU64>,
    emission_gate: Arc<Mutex<()>>,
    limits: RealtimeLimits,
    response_fences: ResponseFences,
}
impl fmt::Debug for RealtimeSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealtimeSender")
            .field("generation", &self.generation())
            .finish_non_exhaustive()
    }
}
impl RealtimeSender {
    pub(crate) fn try_send_camera(
        &self,
        action: Action,
        generation: u64,
        fence: InvocationFence,
    ) -> Result<bool, RealtimeError> {
        crate::video::check_camera(&action, self.limits.max_message_bytes)?;
        // Never resample generation after an encoder suspension. The final
        // socket admission repeats both checks; enqueue is not a wire receipt.
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        if self.generation() != generation || self.control.borrow().close {
            return Ok(false);
        }
        let check = fence.clone();
        check
            .with_current(|| {
                self.tx
                    .try_send(Command {
                        action: Some(action),
                        progress_admission: None,
                        selected_permission: None,
                        selected_alternatives: None,
                        generation,
                        reply: None,
                        inference_fence: None,
                        camera_fence: Some(fence),
                    })
                    .map(|()| true)
                    .map_err(|e| match e {
                        mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                        mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
                    })
            })
            .unwrap_or(Ok(false))
    }
    /// Admit an adapter transaction without partially enqueueing its wire effects.
    pub(crate) fn response_fence(&self, id: &str) -> Option<InvocationFence> {
        self.response_fences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(id)
            .cloned()
    }
    pub(crate) fn with_current_generation<T>(
        &self,
        generation: u64,
        operation: impl FnOnce() -> T,
    ) -> Option<T> {
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        (self.generation() == generation).then(operation)
    }
    pub(crate) fn try_send_batch_with_fence(
        &self,
        actions: Vec<Action>,
        inference_fence: Option<InvocationFence>,
    ) -> Result<(), RealtimeError> {
        for action in &actions {
            check_queued_action(action, &self.limits)?;
        }
        let permits = self
            .tx
            .try_reserve_many(actions.len())
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })?;
        let generation = self.generation();
        for (permit, action) in permits.zip(actions) {
            let fence = matches!(action, Action::CreateResponse(_))
                .then(|| inference_fence.clone())
                .flatten();
            let command = Command {
                action: Some(action),
                progress_admission: None,
                selected_permission: None,
                selected_alternatives: None,
                generation,
                reply: None,
                inference_fence: fence.clone(),
                camera_fence: None,
            };
            if let Some(fence) = fence {
                fence.with_current(|| permit.send(command));
            } else {
                permit.send(command);
            }
        }
        Ok(())
    }
    /// Enqueues without waiting for socket I/O. Rejected actions are reported as
    /// terminal stream errors through `RealtimeSession::next_event`.
    pub fn try_send(&self, action: Action) -> Result<(), RealtimeError> {
        check_queued_action(&action, &self.limits)?;
        if let Action::AppendAudio(bytes) = &action {
            if bytes.len() > self.limits.max_audio_chunk_bytes {
                return Err(RealtimeError::TooLarge);
            }
        }
        if matches!(action, Action::CancelResponse) {
            self.interrupt(None);
            return Ok(());
        }
        self.tx
            .try_send(Command {
                action: Some(action),
                progress_admission: None,
                selected_permission: None,
                selected_alternatives: None,
                generation: self.generation(),
                reply: None,
                inference_fence: None,
                camera_fence: None,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })
    }
    /// Ordered control barrier: resize is applied before a coalesced interruption.
    /// It never changes generation, playback, or the sticky close request.
    pub(crate) fn resize_audio_preroll(&self, bytes: usize) {
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        self.control.send_modify(|c| c.preroll_bytes = Some(bytes));
    }
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    /// Bounded enqueue plus acceptance acknowledgement. Tool continuation and
    /// one deferred response wait for the active response to finish. A timeout is ambiguous:
    /// do not blindly retry non-idempotent actions such as response.create.
    pub async fn send(&self, action: Action) -> Result<(), RealtimeError> {
        check_queued_action(&action, &self.limits)?;
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
        self.tx
            .try_send(Command {
                action: Some(action),
                progress_admission: None,
                selected_permission: None,
                selected_alternatives: None,
                generation: self.generation(),
                reply: Some(reply),
                inference_fence: None,
                camera_fence: None,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })?;
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
        self.send(Action::Finish).await
    }
    pub fn close(&self) {
        let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
        self.generation.fetch_add(1, Ordering::AcqRel);
        self.control.send_modify(|c| c.close = true);
        self.response_fences
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
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
            if self.generation() != generation {
                return Poll::Ready(None);
            }
            future.as_mut().poll(cx).map(Some)
        })
        .await
    }
}
pub(crate) struct ReceivedEvent {
    pub event: RealtimeEvent,
    pub usage_details: Option<UsageDetails>,
}
pub struct RealtimeSession {
    sender: RealtimeSender,
    rx: mpsc::Receiver<ReceivedEvent>,
    task: Option<JoinHandle<Result<(), RealtimeError>>>,
    ended: bool,
    joined: Arc<AtomicBool>,
}
impl RealtimeSession {
    pub fn sender(&self) -> RealtimeSender {
        self.sender.clone()
    }
    pub async fn next_event(&mut self) -> Option<Result<RealtimeEvent, RealtimeError>> {
        self.next_received()
            .await
            .map(|result| result.map(|received| received.event))
    }
    /// Read-only evidence for the selected processor's actual actor join.
    pub(crate) fn join_observer(&self) -> Arc<AtomicBool> {
        self.joined.clone()
    }
    pub(crate) async fn next_received(&mut self) -> Option<Result<ReceivedEvent, RealtimeError>> {
        if !self.ended {
            while let Some(received) = self.rx.recv().await {
                let event = &received.event;
                if event.event.response_id().is_some()
                    && event.generation != self.sender.generation()
                {
                    continue;
                }
                return Some(Ok(received));
            }
            self.ended = true;
        }
        // Keep the handle in self across this await. An aborted receiver leaves
        // the selected processor's session owner able to reclaim the real task.
        // ended=true only suppresses channel reads, never an outstanding join.
        let result = self.task.as_mut()?.await;
        self.task.take();
        self.joined.store(true, Ordering::Release);
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
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        let result = match timeout(
            self.sender.limits.close_timeout + self.sender.limits.operation_timeout,
            &mut *task,
        )
        .await
        {
            Ok(result) => result
                .map_err(|_| RealtimeError::Transport)
                .and_then(|result| result),
            Err(_) => {
                task.abort();
                // Aborting is not joining. This actual await is mandatory and
                // the handle remains recoverable if this waiter is cancelled.
                let _ = task.await;
                Err(RealtimeError::Timeout("close"))
            }
        };
        self.task.take();
        self.joined.store(true, Ordering::Release);
        result
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
    inference_fence: Option<InvocationFence>,
}
struct ToolState {
    name: String,
    item_id: String,
    generation: u64,
    arguments: String,
    announced: bool,
    context_id: Option<String>,
}
struct Actor {
    cancel_receipts: CancelReceipts,
    audio_preroll: Option<Preroll>,
    response_fences: ResponseFences,
    pipeline_turns: bool,
    final_tool_app: bool,
    config: RealtimeConfig,
    generation: Arc<AtomicU64>,
    emission_gate: Arc<Mutex<()>>,
    tx: mpsc::Sender<ReceivedEvent>,
    session_id: Option<String>,
    conversation_id: Option<String>,
    sequence: u64,
    active: Option<String>,
    pending: Option<ResponseContext>,
    pending_event_id: Option<String>,
    ready: bool,
    finishing: bool,
    ready_deadline: Instant,
    responses: VecDeque<(String, ResponseContext)>,
    deadline: Option<Instant>,
    tools: HashMap<String, ToolState>,
    completed_tools: VecDeque<String>,
    continue_response: bool,
    continuation_context: Option<String>,
    continuation_generation: u64,
    deferred_response: Option<(ResponseOptions, u64, Option<InvocationFence>)>,
    selected_debts: Vec<progress::SelectedDebt>,
    progress_policies: HashMap<String, NativeToolProgressAdmission>,
    settlement_deadline: Option<Instant>,
    discarded_calls: VecDeque<String>,
}
impl Actor {
    fn new(
        config: RealtimeConfig,
        generation: Arc<AtomicU64>,
        emission_gate: Arc<Mutex<()>>,
        tx: mpsc::Sender<ReceivedEvent>,
    ) -> Self {
        let ready_deadline = Instant::now() + config.limits.connect_timeout;
        Self {
            cancel_receipts: CancelReceipts::new(config.limits.max_message_bytes),
            audio_preroll: None,
            response_fences: Arc::new(Mutex::new(HashMap::new())),
            pipeline_turns: false,
            final_tool_app: false,
            config,
            generation,
            emission_gate,
            tx,
            session_id: None,
            conversation_id: None,
            sequence: 0,
            active: None,
            pending: None,
            pending_event_id: None,
            ready: false,
            finishing: false,
            ready_deadline,
            responses: VecDeque::new(),
            deadline: None,
            tools: HashMap::new(),
            completed_tools: VecDeque::new(),
            continue_response: false,
            continuation_context: None,
            continuation_generation: 0,
            deferred_response: None,
            selected_debts: Vec::new(),
            progress_policies: HashMap::new(),
            settlement_deadline: None,
            discarded_calls: VecDeque::new(),
        }
    }
    fn epoch(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    async fn write(&mut self, socket: &mut Socket, value: Value) -> Result<(), RealtimeError> {
        self.write_before(
            socket,
            value,
            Instant::now() + self.config.limits.operation_timeout,
        )
        .await
    }
    async fn write_before<S>(
        &mut self,
        socket: &mut S,
        mut value: Value,
        deadline: Instant,
    ) -> Result<(), RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RealtimeError::TooLarge)?;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
        let wire = serde_json::to_string(&value)
            .map_err(|_| RealtimeError::Invalid("event serialization"))?;
        if wire.len() > self.config.limits.max_message_bytes {
            return Err(RealtimeError::TooLarge);
        }
        timeout_at(deadline, socket.send(Message::Text(wire.into())))
            .await
            .map_err(|_| RealtimeError::Timeout("write"))?
            .map_err(transport)
    }
    async fn write_epoch_before<S>(
        &mut self,
        socket: &mut S,
        mut value: Value,
        deadline: Instant,
        generation: u64,
    ) -> Result<bool, RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        if Instant::now() >= deadline {
            return Err(RealtimeError::Timeout("write"));
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RealtimeError::TooLarge)?;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
        let wire = serde_json::to_string(&value)
            .map_err(|_| RealtimeError::Invalid("event serialization"))?;
        if wire.len() > self.config.limits.max_message_bytes {
            return Err(RealtimeError::TooLarge);
        }
        timeout_at(
            deadline,
            send_epoch_fenced(
                socket,
                Message::Text(wire.into()),
                &self.generation,
                &self.emission_gate,
                generation,
            ),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
        .map_err(transport)
    }
    async fn write_inference(
        &mut self,
        socket: &mut Socket,
        mut value: Value,
        fence: Option<&InvocationFence>,
    ) -> Result<bool, RealtimeError> {
        self.sequence += 1;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
        let wire = serde_json::to_string(&value)
            .map_err(|_| RealtimeError::Invalid("event serialization"))?;
        if wire.len() > self.config.limits.max_message_bytes {
            return Err(RealtimeError::TooLarge);
        }
        timeout(
            self.config.limits.operation_timeout,
            send_fenced(socket, Message::Text(wire.into()), fence),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
        .map_err(transport)
    }
    async fn write_command(
        &mut self,
        socket: &mut Socket,
        command: &mut Command,
    ) -> Result<(), RealtimeError> {
        let action = command.action.take();
        let generation = command.generation;
        let inference_fence = command.inference_fence.take();
        let camera_fence = command.camera_fence.take();
        let progress_admission = command.progress_admission.take();
        let selected_permission = command.selected_permission.take();
        let alternatives = command.selected_alternatives.take();
        if let Some(admission) = progress_admission {
            self.admit_progress_policy(admission)?;
        }
        let Some(action) = action else {
            return self.maybe_continue(socket).await;
        };
        if self.final_tool_app && self.finishing && matches!(action, Action::CreateResponse(_)) {
            return Ok(());
        }
        if let Some(alternatives) = alternatives {
            let Action::CreateResponse(options) = action else {
                return Err(RealtimeError::Invalid(
                    "selected alternatives require response command",
                ));
            };
            for alternative in alternatives {
                self.defer_selected_response(
                    alternative.options.unwrap_or_else(|| options.clone()),
                    generation,
                    Some(alternative.fence),
                    alternative.permission,
                )?;
            }
            return self.maybe_selected_continue(socket).await;
        }
        let Some(fence) = camera_fence else {
            return self
                .write_selected_action(
                    socket,
                    action,
                    generation,
                    inference_fence,
                    selected_permission,
                )
                .await;
        };
        self.write_camera(socket, action, generation, &fence).await
    }
    async fn write_camera<S>(
        &mut self,
        socket: &mut S,
        action: Action,
        generation: u64,
        fence: &InvocationFence,
    ) -> Result<(), RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        // Valid pre-Finish samples are ordered before Finish in the command
        // lane. A private camera after Finish is never new graceful-end work.
        if self.finishing || generation != self.epoch() || !fence.is_current() {
            return Err(RealtimeError::Interrupted);
        }
        self.sequence = self
            .sequence
            .checked_add(1)
            .ok_or(RealtimeError::TooLarge)?;
        let event_id = format!("pc_evt_{}", self.sequence);
        let wire =
            crate::video::camera_wire(&action, &event_id, self.config.limits.max_message_bytes)?;
        // Release the data URI representation before the complete JSON waits
        // for sink readiness. Both remain independently size bounded.
        drop(action);
        let admitted = timeout(
            self.config.limits.operation_timeout,
            send_camera_fenced(
                socket,
                Message::Text(wire.into()),
                &self.generation,
                &self.emission_gate,
                generation,
                fence,
            ),
        )
        .await
        .map_err(|_| RealtimeError::Timeout("write"))?
        .map_err(transport)?;
        if admitted {
            Ok(())
        } else {
            Err(RealtimeError::Interrupted)
        }
    }
    async fn write_action(
        &mut self,
        socket: &mut Socket,
        action: Action,
        generation: u64,
        inference_fence: Option<InvocationFence>,
    ) -> Result<(), RealtimeError> {
        self.write_selected_action(socket, action, generation, inference_fence, None)
            .await
    }
    async fn write_selected_action(
        &mut self,
        socket: &mut Socket,
        action: Action,
        generation: u64,
        inference_fence: Option<InvocationFence>,
        selected_permission: Option<NativeToolProgressPermission>,
    ) -> Result<(), RealtimeError> {
        let value = match action {
            Action::Finish => {
                self.finishing = true;
                if self.final_tool_app {
                    self.deferred_response = None;
                    self.selected_debts.clear();
                    self.continue_response = false;
                    self.continuation_context = None;
                    if self.active.is_none()
                        && self.pending.is_none()
                        && !self.tools.is_empty()
                        && self.settlement_deadline.is_none()
                    {
                        self.settlement_deadline =
                            Some(Instant::now() + self.config.limits.response_timeout);
                    }
                }
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
                session.normalize();
                self.write(socket, json!({"type":"session.update", "session":session}))
                    .await?;
                if let Some(audio) = session.audio {
                    let current = self
                        .config
                        .session
                        .audio
                        .get_or_insert_with(AudioConfiguration::default);
                    if audio.input.is_some() {
                        current.input = audio.input;
                    }
                    if audio.output.is_some() {
                        current.output = audio.output;
                    }
                }
                return Ok(());
            }
            Action::AppendAudio(audio) => {
                if audio.is_empty() {
                    return Err(RealtimeError::Invalid("empty audio chunk"));
                }
                if audio.len() > self.config.limits.max_audio_chunk_bytes {
                    return Err(RealtimeError::TooLarge);
                }
                if self.audio_preroll.is_some() && audio.len() % 2 != 0 {
                    return Err(RealtimeError::Invalid(
                        "audio preroll requires aligned PCM16",
                    ));
                }
                let value =
                    json!({"type":"input_audio_buffer.append", "audio":STANDARD.encode(&audio)});
                if self.audio_preroll.is_some() {
                    if !self
                        .write_epoch_before(
                            socket,
                            value,
                            Instant::now() + self.config.limits.operation_timeout,
                            generation,
                        )
                        .await?
                    {
                        return Err(RealtimeError::Interrupted);
                    }
                } else {
                    self.write(socket, value).await?;
                }
                // Local sink flush succeeded; no claim of provider acceptance.
                if let Some(preroll) = &mut self.audio_preroll {
                    preroll.append(&audio);
                }
                return Ok(());
            }
            Action::CommitAudio => json!({"type":"input_audio_buffer.commit"}),
            Action::ClearAudio => json!({"type":"input_audio_buffer.clear"}),
            Action::CreateItem {
                item,
                previous_item_id,
            } => {
                let mut v = json!({"type":"conversation.item.create", "item":item});
                if let Some(id) = previous_item_id {
                    v["previous_item_id"] = json!(id);
                }
                v
            }
            Action::DeleteItem { item_id } => {
                json!({"type":"conversation.item.delete", "item_id":item_id})
            }
            Action::RetrieveItem { item_id } => {
                json!({"type":"conversation.item.retrieve", "item_id":item_id})
            }
            Action::TruncateItem(position) => {
                json!({"type":"conversation.item.truncate", "item_id":position.item_id, "content_index":position.content_index, "audio_end_ms":position.audio_end_ms})
            }
            Action::CreateResponse(response) => {
                if self.final_tool_app && self.finishing {
                    return Ok(());
                }
                if inference_fence
                    .as_ref()
                    .is_some_and(|fence| !fence.is_current())
                {
                    return Ok(());
                }
                if response
                    .output_modalities
                    .as_ref()
                    .is_some_and(|v| v.len() != 1)
                {
                    return Err(RealtimeError::Invalid(
                        "exactly one response modality is required",
                    ));
                }
                if selected_permission
                    .as_ref()
                    .is_some_and(|p| !p.is_allowed())
                {
                    return Ok(());
                }
                if self.active.is_some()
                    || self.pending.is_some()
                    || !self.can_continue_selected(selected_permission.as_ref())
                {
                    if let Some(permission) = selected_permission {
                        self.defer_selected_response(
                            response,
                            generation,
                            inference_fence,
                            permission,
                        )?;
                        return Ok(());
                    }
                    if self.deferred_response.is_some() {
                        return Err(RealtimeError::ResponseActive);
                    }
                    self.deferred_response = Some((response, generation, inference_fence));

                    return Ok(());
                }
                if !self
                    .write_selected_inference(
                        socket,
                        json!({"type":"response.create", "response":response}),
                        inference_fence.as_ref(),
                        selected_permission.as_ref(),
                    )
                    .await?
                {
                    return Ok(());
                }
                if selected_permission.is_some() {
                    self.selected_debts.clear();
                }
                self.pending_event_id = Some(format!("pc_evt_{}", self.sequence));
                self.pending = Some(ResponseContext {
                    generation,
                    inference_fence,
                    context_id: response
                        .context_id
                        .or_else(|| self.config.context_id.clone()),
                });
                self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
                return Ok(());
            }
            Action::CancelResponse => {
                // Internal batches retain their ordinary write path. Public
                // send/try_send route cancellation through the control lane.
                self.write_cancel_before(
                    socket,
                    Instant::now() + self.config.limits.operation_timeout,
                    generation,
                    false,
                )
                .await?;
                return Ok(());
            }
            Action::ToolResult {
                call_id,
                output,
                is_final,
                run_response,
            } => {
                if self.completed_tools.contains(&call_id)
                    || self.discarded_calls.contains(&call_id)
                {
                    return Ok(());
                }
                let tool = self
                    .tools
                    .get(&call_id)
                    .ok_or(RealtimeError::UnknownToolCall)?;
                if (!self.pipeline_turns && tool.generation != generation) || !tool.announced {
                    return Err(RealtimeError::UnknownToolCall);
                }
                let tool_context = tool.context_id.clone();
                let output = match output {
                    Value::String(s) => s,
                    v => v.to_string(),
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
                        role: Role::System,
                        content: vec![ItemContent::InputText { text }],
                    };
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
                self.progress_policies.remove(&call_id);
                if self.tools.is_empty() || self.all_tools_selected() {
                    self.settlement_deadline = None;
                }
                self.completed_tools.push_back(call_id);
                while self.completed_tools.len() > self.config.limits.max_outstanding_tools {
                    self.completed_tools.pop_front();
                }
                let run_response = run_response && !(self.final_tool_app && self.finishing);
                self.continue_response |= run_response;
                if run_response {
                    self.continuation_context = tool_context;
                    self.continuation_generation = generation;
                }
                return self.maybe_continue(socket).await;
            }
        };
        self.write(socket, value).await
    }
    async fn maybe_continue(&mut self, socket: &mut Socket) -> Result<(), RealtimeError> {
        if self.final_tool_app && self.finishing {
            return Ok(());
        }
        self.maybe_selected_continue(socket).await?;
        if (self.continue_response || self.deferred_response.is_some())
            && self.tools.is_empty()
            && self.active.is_none()
            && self.pending.is_none()
        {
            let continuation_context = self.continuation_context.take();

            let (options, generation, inference_fence) =
                self.deferred_response.take().unwrap_or_else(|| {
                    (
                        ResponseOptions {
                            context_id: continuation_context,
                            ..Default::default()
                        },
                        self.continuation_generation,
                        None,
                    )
                });
            self.continue_response = false;
            if generation != self.epoch() {
                return Ok(());
            }
            if !self
                .write_selected_inference(
                    socket,
                    json!({"type":"response.create", "response":options}),
                    inference_fence.as_ref(),
                    None,
                )
                .await?
            {
                return Ok(());
            }
            self.pending_event_id = Some(format!("pc_evt_{}", self.sequence));
            self.pending = Some(ResponseContext {
                generation,
                inference_fence,
                context_id: options
                    .context_id
                    .or_else(|| self.config.context_id.clone()),
            });
            self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
        }
        Ok(())
    }
    async fn write_interrupt_before<S>(
        &mut self,
        socket: &mut S,
        value: Value,
        deadline: Instant,
        generation: u64,
    ) -> Result<bool, RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        if self.audio_preroll.is_some() {
            self.write_epoch_before(socket, value, deadline, generation)
                .await
        } else {
            // Standalone/server-VAD retain their existing write behavior.
            self.write_before(
                socket,
                value,
                Instant::now() + self.config.limits.operation_timeout,
            )
            .await?;
            Ok(true)
        }
    }
    /// Register only a known active response and a successful local cancel flush.
    /// This is not a provider acknowledgment, nor permission to release busy state.
    async fn write_cancel_before<S>(
        &mut self,
        socket: &mut S,
        deadline: Instant,
        generation: u64,
        epoch_fenced: bool,
    ) -> Result<bool, RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        let target = self
            .active
            .as_deref()
            .filter(|_| self.pipeline_turns)
            .filter(|id| !id.is_empty() && id.len() <= self.config.limits.max_message_bytes)
            .and_then(|id| {
                self.responses
                    .iter()
                    .rev()
                    .find(|(response, _)| response == id)
                    .map(|(_, context)| (Box::<str>::from(id), context.generation))
            });
        let written = if epoch_fenced {
            self.write_interrupt_before(
                socket,
                json!({"type":"response.cancel"}),
                deadline,
                generation,
            )
            .await?
        } else {
            self.write_before(socket, json!({"type":"response.cancel"}), deadline)
                .await?;
            true
        };
        if let Some((response_id, response_generation)) = target.filter(|_| written) {
            // Actor owns sequence. Capture this cancel's ID immediately,
            // before any later truncate can advance it.
            let request_id = format!("pc_evt_{}", self.sequence).into_boxed_str();
            self.cancel_receipts.insert(CancelReceipt {
                request_id,
                response_id,
                response_generation,
                interrupt_generation: generation,
            });
        }
        Ok(written)
    }
    async fn interrupt<S>(&mut self, socket: &mut S, control: Control) -> Result<(), RealtimeError>
    where
        S: Sink<Message, Error = tungstenite::Error> + Unpin,
    {
        let deadline = Instant::now() + self.config.limits.operation_timeout;
        if self.audio_preroll.is_some() && control.generation != self.epoch() {
            return Ok(());
        }
        if let Some(preroll) = &self.audio_preroll {
            let suffix = preroll.snapshot();
            if !self
                .write_interrupt_before(
                    socket,
                    json!({"type":"input_audio_buffer.clear"}),
                    deadline,
                    control.generation,
                )
                .await?
            {
                return Ok(());
            }
            // Reserve envelope/event-id bytes; base64 expands each 3 bytes to4.
            let max_chunk = self
                .config
                .limits
                .max_message_bytes
                .saturating_sub(128)
                .checked_div(4)
                .and_then(|n| n.checked_mul(3))
                .ok_or(RealtimeError::TooLarge)?
                .min(self.config.limits.max_audio_chunk_bytes)
                & !1;
            if max_chunk == 0 && !suffix.is_empty() {
                return Err(RealtimeError::TooLarge);
            }
            for chunk in suffix.chunks(max_chunk.max(2)) {
                if self.audio_preroll.is_some() && control.generation != self.epoch() {
                    return Ok(());
                }
                if !self
                    .write_interrupt_before(
                        socket,
                        json!({"type":"input_audio_buffer.append", "audio":STANDARD.encode(chunk)}),
                        deadline,
                        control.generation,
                    )
                    .await?
                {
                    return Ok(());
                }
            }
        }
        if self.audio_preroll.is_some() && control.generation != self.epoch() {
            return Ok(());
        }
        // Keep native cancel eligibility: unlike Python, no idle cancel is sent.
        if (self.active.is_some() || self.pending.is_some())
            && !self
                .write_cancel_before(socket, deadline, control.generation, true)
                .await?
        {
            return Ok(());
        }
        if self.audio_preroll.is_some() && control.generation != self.epoch() {
            return Ok(());
        }
        if let Some(position) = control.playback {
            if !self.write_interrupt_before(socket, json!({"type":"conversation.item.truncate", "item_id":position.item_id, "content_index":position.content_index, "audio_end_ms":position.audio_end_ms}), deadline, control.generation).await? { return Ok(()); }
        }
        // The paired executor owns terminal settlement. A successful receipt
        // can already be admitted but delayed before its upstream relay.
        // Do not invent CANCELLED or discard that identity on interruption.
        self.discarded_calls.extend(
            self.tools
                .iter()
                .filter(|(_, tool)| !self.pipeline_turns || !tool.announced)
                .map(|(id, _)| id.clone()),
        );
        while self.discarded_calls.len() > self.config.limits.max_outstanding_tools {
            self.discarded_calls.pop_front();
        }
        if self.pipeline_turns {
            self.tools.retain(|_, tool| tool.announced);
            if !self.tools.is_empty()
                && !self.all_tools_selected()
                && self.settlement_deadline.is_none()
            {
                self.settlement_deadline =
                    Some(Instant::now() + self.config.limits.response_timeout);
            }
        } else {
            self.tools.clear();
        }
        self.deferred_response = None;
        self.selected_debts.clear();
        self.continue_response = false;
        self.continuation_context = None;
        Ok(())
    }
    async fn run(
        &mut self,
        socket: &mut Socket,
        mut commands: mpsc::Receiver<Command>,
        mut control: watch::Receiver<Control>,
    ) -> Result<(), RealtimeError> {
        let mut preserved = VecDeque::new();
        let mut pending_control = false;
        let mut handled_generation = 0;
        loop {
            if self.ready && pending_control {
                let value = control.borrow_and_update().clone();
                pending_control = false;
                if value.close {
                    return Ok(());
                }
                if let (Some(preroll), Some(bytes)) = (&mut self.audio_preroll, value.preroll_bytes)
                {
                    preroll.resize(bytes);
                }
                if value.generation != handled_generation {
                    if self.pipeline_turns {
                        let admitted = commands.len();
                        for _ in 0..admitted {
                            let Ok(mut command) = commands.try_recv() else {
                                break;
                            };
                            if command.camera_is_stale(self.epoch(), self.finishing) {
                                if let Some(reply) = command.reply {
                                    let _ = reply.send(Err(RealtimeError::Interrupted));
                                }
                                continue;
                            }
                            if command.is_durable() {
                                let result = self.write_command(socket, &mut command).await;
                                if let Some(reply) = command.reply {
                                    let _ = reply.send(result.clone());
                                }
                                result?;
                            } else {
                                if preserved.len() >= self.config.limits.queue_capacity {
                                    return Err(RealtimeError::Backpressure);
                                }
                                preserved.push_back(command);
                            }
                        }
                    }
                    self.interrupt(socket, value.clone()).await?;
                    if value.generation == self.epoch() {
                        handled_generation = value.generation;
                    }
                }
                continue;
            }
            if self.finishing
                && self.active.is_none()
                && self.pending.is_none()
                && (!self.final_tool_app || self.tools.is_empty())
            {
                return Ok(());
            }
            let deadline = self
                .deadline
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            let settlement_deadline = self
                .settlement_deadline
                .unwrap_or_else(|| Instant::now() + Duration::from_secs(86_400));
            tokio::select! { biased;
                changed = control.changed() => {
                    if changed.is_err() { return Ok(()); }
                    let value = control.borrow_and_update().clone();
                    if value.close { return Ok(()); }
                    if self.audio_preroll.is_some() {
                        pending_control = true;
                    } else {
                        // Preserve pre-existing standalone/server-VAD control timing.
                        if self.pipeline_turns && self.ready {
                            let admitted = commands.len();
                            for _ in 0..admitted {
                                let Ok(mut command) = commands.try_recv() else { break; };
                                if command.camera_is_stale(self.epoch(), self.finishing) {
                                    if let Some(reply) = command.reply { let _ = reply.send(Err(RealtimeError::Interrupted)); }
                                    continue;
                                }
                                if command.is_durable() {
                                    let result = self.write_command(socket, &mut command).await;
                                    if let Some(reply) = command.reply { let _ = reply.send(result.clone()); }
                                    result?;
                                } else {
                                    if preserved.len() >= self.config.limits.queue_capacity { return Err(RealtimeError::Backpressure); }
                                    preserved.push_back(command);
                                }
                            }
                        }
                        self.interrupt(socket, value).await?;
                    }
                }
                _ = tokio::time::sleep_until(self.ready_deadline), if !self.ready => return Err(RealtimeError::Timeout("session readiness")),
                _ = tokio::time::sleep_until(deadline), if self.deadline.is_some() => return Err(RealtimeError::Timeout("response")),
                _ = tokio::time::sleep_until(settlement_deadline), if self.settlement_deadline.is_some() => return Err(RealtimeError::Timeout("tool settlement")),
                item = socket.next() => {
                    match item {
                        Some(Ok(Message::Text(text))) => {
                            let wire: WireEvent = serde_json::from_str(&text).map_err(|_| RealtimeError::Protocol("invalid typed event"))?;
                            let usage_details = UsageDetails::parse(&text, &wire.event)?;
                            self.event_with_usage(socket, wire, usage_details).await?;
                        }
                        Some(Ok(Message::Ping(p))) => { timeout(self.config.limits.operation_timeout, socket.send(Message::Pong(p))).await.map_err(|_| RealtimeError::Timeout("pong"))?.map_err(transport)?; }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None => return if self.active.is_some() || self.pending.is_some() { Err(RealtimeError::Protocol("response ended before response.done")) } else if self.final_tool_app && !self.tools.is_empty() { Err(RealtimeError::Protocol("provider ended before final tool settlement")) } else if self.final_tool_app && !self.finishing { Err(RealtimeError::Protocol("provider ended before final-tool Finish")) } else { Ok(()) },
                        Some(Ok(_)) => return Err(RealtimeError::Protocol("expected JSON text")),
                        Some(Err(e)) => return Err(transport(e)),
                    }
                }
                command = async { match preserved.pop_front() { Some(command) => Some(command), None => commands.recv().await } }, if self.ready => {
                    let Some(mut command) = command else { return Ok(()); };
                    if command.camera_is_stale(self.epoch(), self.finishing) {
                        if let Some(reply) = command.reply { let _ = reply.send(Err(RealtimeError::Interrupted)); }
                        continue;
                    }
                    let finishing = matches!(command.action, Some(Action::Finish));
                    if !finishing && command.reply.as_ref().is_some_and(|r| r.is_closed()) { continue; }
                    let durable = self.pipeline_turns && command.is_durable();
                    if !finishing && !durable && command.generation != self.epoch() {
                        if let Some(reply) = command.reply { let _ = reply.send(Err(RealtimeError::Interrupted)); } continue;
                    }
                    let result = self.write_command(socket, &mut command).await;
                    let fatal = matches!(result, Err(RealtimeError::Transport | RealtimeError::Closed | RealtimeError::Timeout(_)));
                    let failure = result.clone();
                    let unacknowledged = command.reply.is_none();
                    if let Some(reply) = command.reply { let _ = reply.send(result); }
                    if fatal || (unacknowledged && failure.is_err() && failure != Err(RealtimeError::Interrupted)) { return failure; }
                }
            }
        }
    }
    async fn event_with_usage(
        &mut self,
        socket: &mut Socket,
        wire: WireEvent,
        usage_details: Option<UsageDetails>,
    ) -> Result<(), RealtimeError> {
        let mut event = wire.event;
        // Classify before generic create-error recovery. Only the nested
        // error.event_id names a client request; wire.event_id names a server event.
        // A consumed cancellation receipt changes no response, deadline, tool,
        // input-buffer or continuation state, even across later generations.
        let fenced_control_error = self.pipeline_turns
            && matches!(&event, Event::Error { error } if matches!(error.code.as_deref(),
                Some("response_cancel_not_active" | "conversation_already_has_active_response")));
        if fenced_control_error
            && matches!(&event, Event::Error { error } if self.cancel_receipts.consume(error))
        {
            return Ok(());
        }
        // Unmatched control errors retain the original redacted Event::Error
        // delivery and paired processor fatal path. They must not release a
        // pending create or invoke maybe_continue, even with its exact request ID.
        if matches!(event, Event::SpeechStarted { .. }) {
            let interrupt_response = match self
                .config
                .session
                .audio
                .as_ref()
                .and_then(|a| a.input.as_ref())
                .and_then(|i| i.turn_detection.as_ref())
            {
                Some(
                    TurnDetection::ServerVad {
                        interrupt_response, ..
                    }
                    | TurnDetection::SemanticVad {
                        interrupt_response, ..
                    },
                ) => *interrupt_response,
                None => false,
            };
            if interrupt_response && !self.pipeline_turns {
                let generation = {
                    let _guard = self.emission_gate.lock().unwrap_or_else(|e| e.into_inner());
                    self.generation.fetch_add(1, Ordering::AcqRel) + 1
                };
                self.interrupt(
                    socket,
                    Control {
                        generation,
                        playback: None,
                        close: false,
                        preroll_bytes: None,
                    },
                )
                .await?;
            }
        }
        if matches!(event, Event::SessionUpdated { .. }) {
            self.ready = true;
        }
        match &event {
            Event::SessionCreated { session } | Event::SessionUpdated { session } => {
                self.session_id = session.id.clone().or_else(|| self.session_id.clone())
            }
            Event::ConversationCreated { conversation } => {
                self.conversation_id = Some(conversation.id.clone())
            }
            Event::Error { error }
                if !fenced_control_error
                    && error.event_id.is_some()
                    && error.event_id == self.pending_event_id =>
            {
                // Rejected creates never emit response.created/response.done. Only
                // their exact request ID can release pending response admission.
                self.pending = None;
                self.pending_event_id = None;
                self.deadline = None;
                if self.pipeline_turns
                    && !self.tools.is_empty()
                    && !self.all_tools_selected()
                    && self.settlement_deadline.is_none()
                {
                    self.settlement_deadline =
                        Some(Instant::now() + self.config.limits.response_timeout);
                }
                self.maybe_continue(socket).await?;
            }
            Event::ResponseCreated { response } => {
                if self.responses.iter().any(|(id, _)| id == &response.id) {
                    return Ok(());
                }
                if self.final_tool_app
                    && self.finishing
                    && self.active.is_none()
                    && self.pending.is_none()
                {
                    return Err(RealtimeError::Protocol(
                        "new response announced after final-tool drain began",
                    ));
                }
                if self.active.as_ref().is_some_and(|id| id != &response.id) {
                    return Err(RealtimeError::Protocol("overlapping responses"));
                }
                self.pending_event_id = None;
                let context = self.pending.take().unwrap_or_else(|| ResponseContext {
                    generation: self.epoch(),
                    context_id: self.config.context_id.clone(),
                    inference_fence: None,
                });
                if let Some(fence) = &context.inference_fence {
                    self.response_fences
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .insert(response.id.clone(), fence.clone());
                }
                self.responses.push_back((response.id.clone(), context));
                while self.responses.len() > 64 {
                    if let Some((id, _)) = self.responses.pop_front() {
                        self.response_fences
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .remove(&id);
                    }
                }
                self.active = Some(response.id.clone());
                self.deadline = Some(Instant::now() + self.config.limits.response_timeout);
                if response.conversation_id.is_some() {
                    self.conversation_id = response.conversation_id.clone();
                }
            }
            _ => {}
        }
        let context = event.response_id().and_then(|id| {
            self.responses
                .iter()
                .rev()
                .find(|(key, _)| key == id)
                .map(|(_, c)| c.clone())
        });
        // Unknown response IDs cannot resurrect interrupted or completed output.
        if event.response_id().is_some() && context.is_none() {
            return Err(RealtimeError::Protocol("event references unknown response"));
        }
        if event
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
                    .and_then(|context| context.inference_fence.as_ref())
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
                } => {
                    if self.completed_tools.contains(call_id) {
                        return Ok(());
                    }
                    if self.final_tool_app && self.finishing && !self.tools.contains_key(call_id) {
                        return Err(RealtimeError::Protocol(
                            "new tool announced after final-tool drain began",
                        ));
                    }
                    if !self.tools.contains_key(call_id)
                        && self.tools.len() >= self.config.limits.max_outstanding_tools
                    {
                        return Err(RealtimeError::TooLarge);
                    }
                    self.tools.entry(call_id.clone()).or_insert(ToolState {
                        name: name.clone(),
                        item_id: id.clone(),
                        generation,
                        arguments: arguments.clone(),
                        announced: false,
                        context_id: context.as_ref().and_then(|c| c.context_id.clone()),
                    });
                }
                Event::FunctionArgumentsDelta { call_id, delta, .. } => {
                    let tool = self
                        .tools
                        .get_mut(call_id)
                        .ok_or(RealtimeError::Protocol("tool delta without output item"))?;
                    if tool.arguments.len().saturating_add(delta.len())
                        > self.config.limits.max_message_bytes
                    {
                        return Err(RealtimeError::TooLarge);
                    }
                    tool.arguments.push_str(delta);
                }
                Event::FunctionArgumentsDone {
                    call_id,
                    item_id,
                    name,
                    arguments,
                    ..
                } => {
                    if self.completed_tools.contains(call_id) {
                        return Ok(());
                    }
                    let tool = self.tools.get_mut(call_id).ok_or(RealtimeError::Protocol(
                        "tool completion without output item",
                    ))?;
                    if tool.item_id != *item_id {
                        return Err(RealtimeError::Protocol("tool item mismatch"));
                    }
                    if tool.announced {
                        return Ok(());
                    }
                    if self.final_tool_app && self.finishing {
                        return Err(RealtimeError::Protocol(
                            "new tool announced after final-tool drain began",
                        ));
                    }
                    if !serde_json::from_str::<Value>(arguments).is_ok_and(|v| v.is_object()) {
                        return Err(RealtimeError::Protocol(
                            "tool arguments must be a JSON object",
                        ));
                    }
                    if arguments.len() > self.config.limits.max_message_bytes {
                        return Err(RealtimeError::TooLarge);
                    }
                    tool.arguments.clone_from(arguments);
                    *name = Some(tool.name.clone());
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
        if let Event::ResponseDone { response } = &event {
            if self.active.as_deref() == Some(&response.id) {
                self.active = None;
                self.deadline = None;
                if self.pipeline_turns
                    && !self.tools.is_empty()
                    && !self.all_tools_selected()
                    && self.settlement_deadline.is_none()
                {
                    self.settlement_deadline =
                        Some(Instant::now() + self.config.limits.response_timeout);
                }
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
        redact_event(&mut event, self.config.profile.secret());
        self.tx
            .try_send(ReceivedEvent {
                usage_details,
                event: RealtimeEvent {
                    event_id: wire.event_id,
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
        Event::ResponseDone { response } | Event::ResponseCreated { response } => {
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
        return if bytes.len() > limits.max_audio_chunk_bytes {
            Err(RealtimeError::TooLarge)
        } else {
            Ok(())
        };
    }
    if let Action::CreateResponse(options) = action {
        if options.context_id.as_ref().is_some_and(|id| id.len() > 256) {
            return Err(RealtimeError::Invalid("context ID exceeds 256 bytes"));
        }
    }
    bounded_serialized(action, limits.max_message_bytes)
}

#[cfg(test)]
mod review_fenced_sink;

#[cfg(test)]
mod review_preroll_sink;

#[cfg(test)]
mod review_cancel_receipts;

#[cfg(test)]
mod camera_admission_tests {
    use super::*;
    use pipecat_core::{
        async_trait, Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig,
        PipelineWorker, ProcessorContext, ProcessorError,
    };
    use std::{
        sync::atomic::{AtomicBool, AtomicUsize},
        task::{Context, Waker},
    };

    #[derive(Default)]
    struct State {
        ready: AtomicBool,
        flush_ready: AtomicBool,
        ready_polls: AtomicUsize,
        flush_polls: AtomicUsize,
        values: Mutex<Vec<Value>>,
        waker: Mutex<Option<Waker>>,
    }
    impl State {
        fn wake(&self) {
            if let Some(waker) = self.waker.lock().unwrap().take() {
                waker.wake();
            }
        }
        async fn polled(&self, count: &AtomicUsize) -> bool {
            timeout(Duration::from_secs(3), async {
                while count.load(Ordering::SeqCst) == 0 {
                    tokio::task::yield_now().await;
                }
            })
            .await
            .is_ok()
        }
    }
    struct ControlledSink(Arc<State>);
    impl Sink<Message> for ControlledSink {
        type Error = tungstenite::Error;
        fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.0.ready_polls.fetch_add(1, Ordering::SeqCst);
            *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.0.ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
        fn start_send(self: Pin<&mut Self>, item: Message) -> Result<(), Self::Error> {
            let Message::Text(text) = item else {
                panic!("camera requires text wire")
            };
            self.0
                .values
                .lock()
                .unwrap()
                .push(serde_json::from_str(&text).unwrap());
            Ok(())
        }
        fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.0.flush_polls.fetch_add(1, Ordering::SeqCst);
            *self.0.waker.lock().unwrap() = Some(cx.waker().clone());
            if self.0.flush_ready.load(Ordering::SeqCst) {
                Poll::Ready(Ok(()))
            } else {
                Poll::Pending
            }
        }
        fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            self.poll_flush(cx)
        }
    }
    #[derive(Default)]
    struct Capture(Mutex<Option<InvocationFence>>);
    #[async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "camera-fence-source"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(&frame.data, FrameData::Text(t) if t.text == "capture") {
                *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
            }
            ctx.push(frame, direction).await
        }
    }
    async fn source() -> (PipelineWorker, InvocationFence) {
        let capture = Arc::new(Capture::default());
        let mut worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        assert!(timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame
            .data
            .is_start());
        worker
            .sender()
            .send(Frame::text("capture"), Direction::Downstream)
            .await
            .unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(3), worker.recv())
                .await
                .unwrap()
                .unwrap()
                .frame
                .data,
            FrameData::Text(_)
        ));
        let fence = capture.0.lock().unwrap().take().unwrap();
        (worker, fence)
    }
    async fn interrupt(worker: &mut PipelineWorker) -> bool {
        if !matches!(
            timeout(
                Duration::from_secs(3),
                worker
                    .sender()
                    .send(Frame::interruption(), Direction::Downstream)
            )
            .await,
            Ok(Ok(()))
        ) {
            return false;
        }
        matches!(timeout(Duration::from_secs(3), worker.recv()).await, Ok(Some(item)) if matches!(item.frame.data, FrameData::Interruption))
    }
    async fn close(mut worker: PipelineWorker) {
        let ended = timeout(Duration::from_secs(3), worker.end()).await;
        if !matches!(ended, Ok(Ok(()))) {
            let _ = timeout(
                Duration::from_secs(3),
                worker.cancel("camera source End failed"),
            )
            .await;
        }
        let mut join_timeout = false;
        let joined = match timeout(Duration::from_secs(3), worker.join()).await {
            Ok(result) => result,
            Err(_) => {
                join_timeout = true;
                let _ = timeout(
                    Duration::from_secs(3),
                    worker.cancel("camera source join deadline"),
                )
                .await;
                worker.join().await
            }
        };
        assert!(matches!(ended, Ok(Ok(()))), "{ended:?}");
        assert!(!join_timeout, "camera source join deadline");
        let report = joined.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }
    fn actor() -> Actor {
        let (tx, _rx) = mpsc::channel(8);
        Actor::new(
            RealtimeConfig::new("local-camera-model"),
            Arc::new(AtomicU64::new(0)),
            Arc::new(Mutex::new(())),
            tx,
        )
    }
    fn camera() -> Action {
        crate::video::camera_action(vec![1, 2, 3], "pc_camera_1".into()).unwrap()
    }

    async fn operation_result(
        mut operation: JoinHandle<Result<(), RealtimeError>>,
    ) -> Result<Result<(), RealtimeError>, String> {
        match timeout(Duration::from_secs(3), &mut operation).await {
            Ok(result) => result.map_err(|error| error.to_string()),
            Err(error) => {
                operation.abort();
                let _ = operation.await;
                Err(error.to_string())
            }
        }
    }
    #[tokio::test]
    async fn camera_pending_readiness_rechecks_raw_fence_and_provider_epoch() {
        for raw_fence in [false, true] {
            let (mut worker, fence) = source().await;
            let mut actor = actor();
            let generation = actor.generation.clone();
            let gate = actor.emission_gate.clone();
            let state = Arc::new(State::default());
            let operation_state = state.clone();
            let operation = tokio::spawn(async move {
                actor
                    .write_camera(&mut ControlledSink(operation_state), camera(), 0, &fence)
                    .await
            });
            let polled = state.polled(&state.ready_polls).await;
            let interrupted = if raw_fence {
                interrupt(&mut worker).await
            } else {
                let _guard = gate.lock().unwrap();
                generation.fetch_add(1, Ordering::AcqRel);
                true
            };
            state.ready.store(true, Ordering::SeqCst);
            state.flush_ready.store(true, Ordering::SeqCst);
            state.wake();
            let result = operation_result(operation).await;
            let values = state.values.lock().unwrap().clone();
            let flushes = state.flush_polls.load(Ordering::SeqCst);
            close(worker).await;
            assert!(polled && interrupted);
            assert_eq!(result.unwrap(), Err(RealtimeError::Interrupted));
            assert!(values.is_empty());
            assert_eq!(flushes, 0);
        }
    }
    #[tokio::test]
    async fn camera_admitted_flush_survives_both_retirements_without_resending() {
        let (mut worker, fence) = source().await;
        let mut actor = actor();
        let generation = actor.generation.clone();
        let gate = actor.emission_gate.clone();
        let state = Arc::new(State::default());
        state.ready.store(true, Ordering::SeqCst);
        let operation_state = state.clone();
        let operation = tokio::spawn(async move {
            actor
                .write_camera(&mut ControlledSink(operation_state), camera(), 0, &fence)
                .await
        });
        let polled = state.polled(&state.flush_polls).await;
        let interrupted = interrupt(&mut worker).await;
        {
            let _guard = gate.lock().unwrap();
            generation.fetch_add(1, Ordering::AcqRel);
        }
        state.flush_ready.store(true, Ordering::SeqCst);
        state.wake();
        let result = operation_result(operation).await;
        let ready_polls = state.ready_polls.load(Ordering::SeqCst);
        let values = state.values.lock().unwrap().clone();
        close(worker).await;
        assert!(polled && interrupted);
        assert_eq!(result.unwrap(), Ok(()));
        assert_eq!(ready_polls, 1);
        assert_eq!(values.len(), 1);
        assert_eq!(values[0]["type"], "conversation.item.create");
        assert_eq!(values[0]["item"]["content"][0]["detail"], "auto");
    }
    #[tokio::test]
    async fn camera_pending_flush_timeout_never_retries_ambiguous_item() {
        let (worker, fence) = source().await;
        let mut actor = actor();
        actor.config.limits.operation_timeout = Duration::from_millis(25);
        let state = Arc::new(State::default());
        state.ready.store(true, Ordering::SeqCst);
        let result = actor
            .write_camera(&mut ControlledSink(state.clone()), camera(), 0, &fence)
            .await;
        let count = state.values.lock().unwrap().len();
        let ready_polls = state.ready_polls.load(Ordering::SeqCst);
        close(worker).await;
        assert_eq!(result, Err(RealtimeError::Timeout("write")));
        assert_eq!(count, 1);
        assert_eq!(ready_polls, 1);
    }
    #[tokio::test]
    async fn camera_finishing_event_id_overflow_and_full_wire_budget_reject_before_sink() {
        let (worker, fence) = source().await;
        let mut actor = actor();
        let state = Arc::new(State::default());
        state.ready.store(true, Ordering::SeqCst);
        state.flush_ready.store(true, Ordering::SeqCst);
        let mut sink = ControlledSink(state.clone());
        actor.finishing = true;
        let finishing = actor.write_camera(&mut sink, camera(), 0, &fence).await;
        actor.finishing = false;
        actor.sequence = u64::MAX;
        let overflow = actor.write_camera(&mut sink, camera(), 0, &fence).await;
        actor.sequence = u64::MAX - 1;
        actor.config.limits.max_message_bytes = 64;
        let budget = actor.write_camera(&mut sink, camera(), 0, &fence).await;
        let rejected_polls = state.ready_polls.load(Ordering::SeqCst);
        actor.sequence = u64::MAX - 1;
        actor.config.limits.max_message_bytes = 4096;
        let accepted = actor.write_camera(&mut sink, camera(), 0, &fence).await;
        let values = state.values.lock().unwrap().clone();
        close(worker).await;
        assert_eq!(finishing, Err(RealtimeError::Interrupted));
        assert_eq!(overflow, Err(RealtimeError::TooLarge));
        assert_eq!(budget, Err(RealtimeError::TooLarge));
        assert_eq!(rejected_polls, 0);
        assert_eq!(accepted, Ok(()));
        assert_eq!(values[0]["event_id"], "pc_evt_18446744073709551615");
    }
    #[tokio::test]
    async fn private_camera_queue_capacity_and_retirement_do_not_change_public_durable_items() {
        let (worker, fence) = source().await;
        let (tx, mut rx) = mpsc::channel(1);
        let (control, _control_rx) = watch::channel(Control::default());
        let sender = RealtimeSender {
            tx,
            control,
            generation: Arc::new(AtomicU64::new(0)),
            emission_gate: Arc::new(Mutex::new(())),
            limits: RealtimeLimits::default(),
            response_fences: Default::default(),
        };
        let first = sender.try_send_camera(camera(), 0, fence.clone());
        let full = sender.try_send_camera(camera(), 0, fence.clone());
        let private = rx.try_recv().ok();
        let qualifications = private.as_ref().map(|command| {
            (
                command.is_durable(),
                command.camera_is_stale(0, false),
                command.camera_is_stale(1, false),
                command.camera_is_stale(0, true),
            )
        });
        let private_empty = rx.try_recv().is_err();
        let sent_public = sender.try_send(camera());
        let public = rx.try_recv().ok();
        sender.interrupt(None);
        let stale = sender.try_send_camera(camera(), 0, fence);
        let stale_empty = rx.try_recv().is_err();
        close(worker).await;
        assert_eq!(first, Ok(true));
        assert_eq!(full, Err(RealtimeError::Backpressure));
        assert_eq!(qualifications, Some((false, false, true, true)));
        assert!(private_empty);
        assert_eq!(sent_public, Ok(()));
        let public = public.expect("ordinary CreateItem must enqueue");
        assert!(public.is_durable());
        assert!(!public.camera_is_stale(1, true));
        assert_eq!(stale, Ok(false));
        assert!(stale_empty);
    }
}

#[cfg(test)]
mod session_join_cancellation_tests {
    use super::*;
    use tokio::net::TcpListener;

    async fn connected() -> (RealtimeSession, JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(socket).await.unwrap();
            while let Some(message) = socket.next().await {
                match message {
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        });
        let mut config = RealtimeConfig::new("local-join-test");
        config.endpoint = format!("ws://{address}/v1/realtime");
        let session = RealtimeClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        (session, peer)
    }

    #[tokio::test]
    async fn cancelled_shutdown_retains_the_same_actual_actor_handle() {
        let (mut session, peer) = connected().await;
        let actor = session.task.as_ref().unwrap().id();
        let joined = session.join_observer();
        // On this current-thread runtime no actor poll occurs between the
        // synchronous close request and our first Pending join observation.
        let mut shutdown = Box::pin(session.shutdown());
        assert!(futures_util::poll!(&mut shutdown).is_pending());
        drop(shutdown);
        assert_eq!(session.task.as_ref().unwrap().id(), actor);
        assert!(!joined.load(Ordering::Acquire));
        session.shutdown().await.unwrap();
        assert!(session.task.is_none());
        assert!(joined.load(Ordering::Acquire));
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn cancelled_receive_join_remains_recoverable_after_ended_is_set() {
        let (mut session, peer) = connected().await;
        let actor = session.task.as_ref().unwrap().id();
        let joined = session.join_observer();
        session.rx.close();
        let mut receive = Box::pin(session.next_received());
        assert!(futures_util::poll!(&mut receive).is_pending());
        drop(receive);
        assert!(session.ended);
        assert_eq!(session.task.as_ref().unwrap().id(), actor);
        let mut retry = Box::pin(session.next_received());
        assert!(futures_util::poll!(&mut retry).is_pending());
        drop(retry);
        assert!(!joined.load(Ordering::Acquire));
        session.shutdown().await.unwrap();
        assert!(session.task.is_none());
        assert!(joined.load(Ordering::Acquire));
        peer.await.unwrap();
    }
}

#[cfg(test)]
mod azure_authentication_tests {
    use super::*;

    #[test]
    fn selected_authentication_values_are_sensitive() {
        let configs = [
            RealtimeConfig::new("synthetic-openai-key"),
            RealtimeConfig::azure(
                "wss://synthetic-resource.example/openai/v1/realtime",
                "deployment",
                AzureRealtimeCredential::ApiKey("synthetic-azure-key".into()),
            )
            .unwrap(),
            RealtimeConfig::azure(
                "wss://synthetic-resource.example/openai/v1/realtime",
                "deployment",
                AzureRealtimeCredential::EntraBearer("synthetic-entra-token".into()),
            )
            .unwrap(),
        ];
        for config in configs {
            let (_, value) = config.profile.authentication().unwrap();
            assert!(value.is_sensitive());
            assert!(!format!("{value:?}").contains(config.profile.secret()));
        }
    }
}
