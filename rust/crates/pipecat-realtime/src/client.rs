use crate::types::*;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    future::Future,
    net::IpAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
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
            endpoint: "wss://api.openai.com/v1/realtime".into(),
            model: "gpt-realtime-2.1".into(),
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
        if self.model.trim().is_empty() || self.model.len() > 256 || self.api_key.is_empty() {
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
        let generation = Arc::new(AtomicU64::new(0));
        let emission_gate = Arc::new(Mutex::new(()));
        let sender = RealtimeSender {
            tx,
            control: control_tx,
            generation: generation.clone(),
            emission_gate: emission_gate.clone(),
            limits: c.limits.clone(),
        };
        let mut actor = Actor::new(c.clone(), generation.clone(), emission_gate, events_tx);
        // session.update precedes every user action, including appended audio.
        actor
            .write_action(&mut socket, Action::UpdateSession(c.session.clone()), 0)
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
        })
    }
}
#[derive(Debug, Clone, Default)]
struct Control {
    generation: u64,
    playback: Option<PlaybackPosition>,
    close: bool,
}
struct Command {
    action: Action,
    generation: u64,
    reply: Option<oneshot::Sender<Result<(), RealtimeError>>>,
}
#[derive(Clone)]
pub struct RealtimeSender {
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    generation: Arc<AtomicU64>,
    emission_gate: Arc<Mutex<()>>,
    limits: RealtimeLimits,
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
                action,
                generation: self.generation(),
                reply: None,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => RealtimeError::Backpressure,
                mpsc::error::TrySendError::Closed(_) => RealtimeError::Closed,
            })
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
                action,
                generation: self.generation(),
                reply: Some(reply),
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
    config: RealtimeConfig,
    generation: Arc<AtomicU64>,
    emission_gate: Arc<Mutex<()>>,
    tx: mpsc::Sender<RealtimeEvent>,
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
    deferred_response: Option<(ResponseOptions, u64)>,
    discarded_calls: VecDeque<String>,
}
impl Actor {
    fn new(
        config: RealtimeConfig,
        generation: Arc<AtomicU64>,
        emission_gate: Arc<Mutex<()>>,
        tx: mpsc::Sender<RealtimeEvent>,
    ) -> Self {
        let ready_deadline = Instant::now() + config.limits.connect_timeout;
        Self {
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
            discarded_calls: VecDeque::new(),
        }
    }
    fn epoch(&self) -> u64 {
        self.generation.load(Ordering::Acquire)
    }
    async fn write(&mut self, socket: &mut Socket, mut value: Value) -> Result<(), RealtimeError> {
        self.sequence += 1;
        value["event_id"] = json!(format!("pc_evt_{}", self.sequence));
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
        let value = match action {
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
                json!({"type":"input_audio_buffer.append", "audio":STANDARD.encode(audio)})
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
                if response
                    .output_modalities
                    .as_ref()
                    .is_some_and(|v| v.len() != 1)
                {
                    return Err(RealtimeError::Invalid(
                        "exactly one response modality is required",
                    ));
                }
                if self.active.is_some() || self.pending.is_some() {
                    if self.deferred_response.is_some() {
                        return Err(RealtimeError::ResponseActive);
                    }
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
                if tool.generation != generation || !tool.announced {
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
                self.completed_tools.push_back(call_id);
                while self.completed_tools.len() > self.config.limits.max_outstanding_tools {
                    self.completed_tools.pop_front();
                }
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
        if (self.continue_response || self.deferred_response.is_some())
            && self.tools.is_empty()
            && self.active.is_none()
            && self.pending.is_none()
        {
            let continuation_context = self.continuation_context.take();
            let (options, generation) = self.deferred_response.take().unwrap_or_else(|| {
                (
                    ResponseOptions {
                        context_id: continuation_context,
                        ..Default::default()
                    },
                    self.continuation_generation,
                )
            });
            self.continue_response = false;
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
        if self.active.is_some() || self.pending.is_some() {
            self.write(socket, json!({"type":"response.cancel"}))
                .await?;
        }
        if let Some(position) = control.playback {
            self.write_action(socket, Action::TruncateItem(position), control.generation)
                .await?;
        }
        self.discarded_calls.extend(self.tools.keys().cloned());
        while self.discarded_calls.len() > self.config.limits.max_outstanding_tools {
            self.discarded_calls.pop_front();
        }
        self.tools.clear();
        self.deferred_response = None;
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
                            let wire: WireEvent = serde_json::from_str(&text).map_err(|_| RealtimeError::Protocol("invalid typed event"))?;
                            self.event(socket, wire).await?;
                        }
                        Some(Ok(Message::Ping(p))) => { timeout(self.config.limits.operation_timeout, socket.send(Message::Pong(p))).await.map_err(|_| RealtimeError::Timeout("pong"))?.map_err(transport)?; }
                        Some(Ok(Message::Pong(_))) => {}
                        Some(Ok(Message::Close(_))) | None => return if self.active.is_some() || self.pending.is_some() { Err(RealtimeError::Protocol("response ended before response.done")) } else { Ok(()) },
                        Some(Ok(_)) => return Err(RealtimeError::Protocol("expected JSON text")),
                        Some(Err(e)) => return Err(transport(e)),
                    }
                }
                command = commands.recv(), if self.ready => {
                    let Some(command) = command else { return Ok(()); };
                    let finishing = matches!(command.action, Action::Finish);
                    if !finishing && command.reply.as_ref().is_some_and(|r| r.is_closed()) { continue; }
                    if !finishing && command.generation != self.epoch() {
                        if let Some(reply) = command.reply { let _ = reply.send(Err(RealtimeError::Interrupted)); } continue;
                    }
                    let result = self.write_action(socket, command.action, command.generation).await;
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
            if interrupt_response {
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
                if error.event_id.is_some() && error.event_id == self.pending_event_id =>
            {
                // Rejected creates never emit response.created/response.done. Only
                // their exact request ID can release pending response admission.
                self.pending = None;
                self.pending_event_id = None;
                self.deadline = None;
                self.maybe_continue(socket).await?;
            }
            Event::ResponseCreated { response } => {
                if self.responses.iter().any(|(id, _)| id == &response.id) {
                    return Ok(());
                }
                if self.active.as_ref().is_some_and(|id| id != &response.id) {
                    return Err(RealtimeError::Protocol("overlapping responses"));
                }
                self.pending_event_id = None;
                let context = self.pending.take().unwrap_or_else(|| ResponseContext {
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
                } => {
                    if self.completed_tools.contains(call_id) {
                        return Ok(());
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
                    if !serde_json::from_str::<Value>(arguments).is_ok_and(|v| v.is_object()) {
                        return Err(RealtimeError::Protocol(
                            "tool arguments must be a JSON object",
                        ));
                    }
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
fn bounded_serialized(value: &impl serde::Serialize, limit: usize) -> Result<(), RealtimeError> {
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
