use crate::{protocol, *};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
const LOCAL_TOOL_ID_PREFIX: &str = "pipecat-gemini-local-";
static NEXT_TOOL_SESSION: AtomicU64 = AtomicU64::new(1);

struct ToolIds {
    session: u64,
    next_call: u64,
}
impl ToolIds {
    fn new() -> Result<Self, GeminiError> {
        let session = NEXT_TOOL_SESSION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| GeminiError::TooLarge)?;
        Ok(Self {
            session,
            next_call: 0,
        })
    }
    fn assign(&mut self, call: &mut FunctionCall) -> Result<(), GeminiError> {
        match call.id_source {
            ToolCallIdSource::Provider => {
                // Reserve the complete local namespace, including completed
                // IDs, without retaining an ever-growing replay history.
                if call.id.starts_with(LOCAL_TOOL_ID_PREFIX) {
                    return Err(GeminiError::Protocol(
                        "provider tool ID uses reserved local namespace",
                    ));
                }
            }
            ToolCallIdSource::LocalGenerated => {
                self.next_call = self.next_call.checked_add(1).ok_or(GeminiError::TooLarge)?;
                call.id = format!(
                    "{LOCAL_TOOL_ID_PREFIX}{:016x}-{:016x}",
                    self.session, self.next_call
                );
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
struct Control {
    cancel: bool,
    generation: u64,
}
struct Command {
    action: Option<Action>,
    origin: Origin,
    generation: u64,
}
struct PendingTool {
    call: FunctionCall,
    origin: Origin,
    generation: u64,
    non_blocking: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum CallAdmission {
    Available,
    Claimed,
    Retired,
}

/// A session-local, non-evicting identity ledger. Keeping tombstones prevents
/// an old queued control event/result from claiming a later call with the same
/// provider ID. Exhaustion is terminal instead of silently recycling identity.
#[derive(Default)]
struct ToolAdmissions {
    ids: HashMap<Box<str>, CallAdmission>,
    id_bytes: usize,
    closed: bool,
}
impl ToolAdmissions {
    fn register(
        &mut self,
        calls: &[&PendingTool],
        cancellations: &HashSet<String>,
        limits: &Limits,
    ) -> Result<(), GeminiError> {
        if self.closed {
            return Err(GeminiError::Closed);
        }
        let mut unique = HashSet::new();
        let mut bytes = self.id_bytes;
        for tool in calls {
            if !tool.non_blocking || tool.call.id.is_empty() || tool.call.id.len() > 1024 {
                return Err(GeminiError::Invalid("non-blocking tool ID"));
            }
            if self.ids.contains_key(tool.call.id.as_str()) || !unique.insert(&tool.call.id) {
                return Err(GeminiError::Protocol("reused tool ID"));
            }
            bytes = bytes
                .checked_add(tool.call.id.len())
                .ok_or(GeminiError::TooLarge)?;
        }
        if self.ids.len().saturating_add(calls.len())
            > limits.max_pending_tools.saturating_mul(32).min(4096)
            || bytes > limits.max_message_bytes
        {
            return Err(GeminiError::TooLarge);
        }
        // All fallible validation precedes the first insertion.
        for tool in calls {
            let state = if cancellations.contains(&tool.call.id) {
                CallAdmission::Retired
            } else {
                CallAdmission::Available
            };
            self.ids
                .insert(tool.call.id.clone().into_boxed_str(), state);
        }
        self.id_bytes = bytes;
        Ok(())
    }
    fn claim(&mut self, calls: &mut Vec<FunctionCall>) {
        // No await separates receipt, this claim and returning the event.
        // Cancellation takes this same lock: exactly one ordering wins.
        calls.retain(|call| {
            if self.closed {
                return false;
            }
            match self.ids.get_mut(call.id.as_str()) {
                Some(state) if *state == CallAdmission::Available => {
                    *state = CallAdmission::Claimed;
                    true
                }
                _ => false,
            }
        });
    }
    fn revoke(&mut self, id: &str) {
        if let Some(state) = self.ids.get_mut(id) {
            *state = CallAdmission::Retired;
        }
    }
    fn close(&mut self) {
        self.closed = true;
        self.ids.clear();
        self.id_bytes = 0;
    }
}

fn tool_call_runs(
    calls: &[PendingTool],
    suppressed: bool,
    cancellations: &HashSet<String>,
) -> Vec<(bool, Vec<FunctionCall>)> {
    let mut runs: Vec<(bool, Vec<FunctionCall>)> = Vec::new();
    for tool in calls {
        if runs
            .last()
            .is_none_or(|(selected, _)| *selected != tool.non_blocking)
        {
            runs.push((tool.non_blocking, Vec::new()));
        }
        if (tool.non_blocking && !cancellations.contains(&tool.call.id))
            || (!tool.non_blocking && !suppressed)
        {
            runs.last_mut()
                .expect("run was inserted")
                .1
                .push(tool.call.clone());
        }
    }
    runs.retain(|(_, calls)| !calls.is_empty());
    runs
}

fn validate_non_blocking_responses(
    responses: &[NonBlockingFunctionResponse],
    pending: &HashMap<String, PendingTool>,
    admissions: &ToolAdmissions,
) -> Result<(), GeminiError> {
    if admissions.closed {
        return Err(GeminiError::Closed);
    }
    for response in responses {
        if !pending
            .get(&response.id)
            .is_some_and(|tool| tool.non_blocking && tool.call.name == response.name)
            || admissions.ids.get(response.id.as_str()) != Some(&CallAdmission::Claimed)
        {
            return Err(GeminiError::Invalid(
                "unknown, unclaimed, canceled, or mismatched non-blocking tool result",
            ));
        }
    }
    Ok(())
}

struct ToolAdmissionLifetime(Arc<std::sync::Mutex<ToolAdmissions>>);
impl Drop for ToolAdmissionLifetime {
    fn drop(&mut self) {
        // Also runs on abort/panic, not only the normal run() return path.
        self.0
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .close();
    }
}
fn retire_tool(retired: &mut VecDeque<PendingTool>, tool: PendingTool, limit: usize) {
    if retired.len() == limit {
        retired.pop_front();
    }
    retired.push_back(tool);
}
fn retire_pending(
    pending: &mut HashMap<String, PendingTool>,
    retired: &mut VecDeque<PendingTool>,
    limit: usize,
) {
    let blocking: Vec<_> = pending
        .iter()
        .filter(|(_, tool)| !tool.non_blocking)
        .map(|(id, _)| id.clone())
        .collect();
    for id in blocking {
        if let Some(tool) = pending.remove(&id) {
            retire_tool(retired, tool, limit);
        }
    }
}

#[derive(Clone)]
pub struct GeminiClient {
    config: Arc<GeminiConfig>,
    resumption: Option<ResumptionPolicy>,
    thinking: Option<ThinkingPolicy>,
    non_blocking_tools: Arc<HashSet<String>>,
}
impl GeminiClient {
    pub fn new(config: GeminiConfig) -> Result<Self, GeminiError> {
        config.limits.validate()?;
        config.session.validate(config.mode, &config.limits)?;
        protocol::check_json_size(&config.session, config.limits.max_message_bytes)?;
        if config.api_key.is_empty() || config.api_key.contains(['\r', '\n']) {
            return Err(GeminiError::Invalid("API key"));
        }
        if !(8_000..=96_000).contains(&config.input_sample_rate) {
            return Err(GeminiError::Invalid("input sample rate"));
        }
        endpoint(&config)?;
        if serde_json::to_vec(&protocol::setup(&config))
            .map_err(|_| GeminiError::Invalid("setup"))?
            .len()
            > config.limits.max_message_bytes
        {
            return Err(GeminiError::TooLarge);
        }
        Ok(Self {
            config: Arc::new(config),
            resumption: None,
            thinking: None,
            non_blocking_tools: Arc::new(HashSet::new()),
        })
    }
    /// Opt in to bounded receive-side recovery with an opaque server handle.
    /// Connect/setup/write/protocol failures are terminal; no action is replayed.
    /// Handles are private to each session and never shared by cloned clients.
    pub fn with_session_resumption(
        mut self,
        policy: ResumptionPolicy,
    ) -> Result<Self, GeminiError> {
        policy.validate()?;
        self.resumption = Some(policy);
        Ok(self)
    }
    /// Enable typed thinking setup and defer utterance completion while the
    /// provider reports IN_PROGRESS. Select a compatible model explicitly.
    /// Thinking tools and transcription-only mode are outside this subset.
    pub fn with_thinking(mut self, policy: ThinkingPolicy) -> Result<Self, GeminiError> {
        policy.validate()?;
        if self.config.mode == SessionMode::Transcription
            || !self.config.session.tools.is_empty()
            || !self.non_blocking_tools.is_empty()
        {
            return Err(GeminiError::Unsupported(
                "thinking requires conversation mode without tools",
            ));
        }
        let mut setup = protocol::setup(&self.config);
        setup["setup"]["generationConfig"]["thinkingConfig"] =
            serde_json::json!({"thinkingLevel": policy.level});
        protocol::check_json_size(&setup, self.config.limits.max_message_bytes)?;
        self.thinking = Some(policy);
        Ok(self)
    }
    /// Select explicitly declared functions for bounded, call-authorized
    /// background control delivery. Only the two exact ordinary Live model
    /// profiles below are supported. Thinking and empty selections are rejected.
    ///
    /// Opted-in sessions never reuse selected tool IDs: their lifetime ledger retains
    /// at most min(max_pending_tools * 32, 4096) IDs and max_message_bytes ID
    /// bytes, then fails closed. Each selected ID is at most 1024 bytes. These
    /// are payload/count limits, not a process RSS claim; keys use boxed strings
    /// and map overhead is bounded by the entry limit. Each session has its own ledger.
    pub fn with_non_blocking_tools(mut self, names: Vec<String>) -> Result<Self, GeminiError> {
        let model = self
            .config
            .session
            .model
            .strip_prefix("models/")
            .unwrap_or(&self.config.session.model);
        if self.thinking.is_some()
            || self.config.mode != SessionMode::Conversation
            || !matches!(
                model,
                "gemini-3.8-live" | "gemini-2.5-flash-native-audio-preview-12-2025"
            )
        {
            return Err(GeminiError::Unsupported(
                "non-blocking tool model or session",
            ));
        }
        if names.is_empty() || names.len() > self.config.limits.max_pending_tools {
            return Err(GeminiError::Invalid("non-blocking tool selection"));
        }
        let mut selected = HashSet::new();
        for name in names {
            if name.trim().is_empty()
                || !self.config.session.tools.iter().any(|t| t.name == name)
                || !selected.insert(name)
            {
                return Err(GeminiError::Invalid("non-blocking tool selection"));
            }
        }
        let setup = configured_setup(&self.config, self.thinking.as_ref(), &selected);
        protocol::check_json_size(&setup, self.config.limits.max_message_bytes)?;
        self.non_blocking_tools = Arc::new(selected);
        Ok(self)
    }
    pub(crate) fn is_non_blocking_tool(&self, name: &str) -> bool {
        self.non_blocking_tools.contains(name)
    }
    pub fn config(&self) -> &GeminiConfig {
        &self.config
    }
    /// Returns only after the peer acknowledges setup. Dropping this future
    /// drops its socket; it does not leave a detached handshake task.
    pub async fn connect(&self) -> Result<GeminiSession, GeminiError> {
        let config = &self.config;
        let (socket, update) = open_socket(
            config,
            self.resumption.as_ref(),
            None,
            self.thinking.as_ref(),
            &self.non_blocking_tools,
        )
        .await?;
        let mut handle = None;
        apply_resumption(&mut handle, update);
        let (tx, rx) = mpsc::channel(config.limits.queue_capacity);
        let (events_tx, events_rx) = mpsc::channel(config.limits.queue_capacity);
        let (control_tx, control_rx) = watch::channel(Control::default());
        let sender = GeminiSender {
            config: config.clone(),
            tx,
            control: control_tx,
            generation: Arc::new(AtomicU64::new(0)),
            finishing: Arc::new(AtomicBool::new(false)),
            gate: Arc::new(Mutex::new(())),
            admission: Arc::new(std::sync::Mutex::new(())),
            tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
            non_blocking_tools: self.non_blocking_tools.clone(),
        };
        let worker_sender = sender.clone();
        let policy = self.resumption.clone();
        let thinking = self.thinking.clone();
        let worker = tokio::spawn(async move {
            run(
                socket,
                rx,
                events_tx,
                control_rx,
                worker_sender,
                WorkerOptions {
                    policy,
                    handle,
                    thinking,
                },
            )
            .await
        });
        Ok(GeminiSession {
            sender,
            events: events_rx,
            worker: Some(worker),
        })
    }
}
fn configured_setup(
    config: &GeminiConfig,
    thinking: Option<&ThinkingPolicy>,
    non_blocking_tools: &HashSet<String>,
) -> serde_json::Value {
    let mut setup = protocol::setup(config);
    if let Some(thinking) = thinking {
        setup["setup"]["generationConfig"]["thinkingConfig"] =
            serde_json::json!({"thinkingLevel": thinking.level});
    }
    if !non_blocking_tools.is_empty() {
        let model = config
            .session
            .model
            .strip_prefix("models/")
            .unwrap_or(&config.session.model);
        if let Some(declarations) =
            setup["setup"]["tools"][0]["functionDeclarations"].as_array_mut()
        {
            for declaration in declarations {
                if declaration["name"]
                    .as_str()
                    .is_some_and(|name| non_blocking_tools.contains(name))
                {
                    declaration["behavior"] = serde_json::json!("NON_BLOCKING");
                } else if model == "gemini-3.8-live" {
                    declaration["behavior"] = serde_json::json!("BLOCKING");
                }
            }
        }
    }
    setup
}
fn apply_resumption(handle: &mut Option<String>, update: Option<protocol::ResumptionUpdate>) {
    if let Some(update) = update {
        // Unlike the pinned source, a non-resumable update revokes the old token.
        *handle = if update.resumable {
            update.new_handle
        } else {
            None
        };
    }
}

async fn open_socket(
    config: &GeminiConfig,
    policy: Option<&ResumptionPolicy>,
    handle: Option<&str>,
    thinking: Option<&ThinkingPolicy>,
    non_blocking_tools: &HashSet<String>,
) -> Result<(Socket, Option<protocol::ResumptionUpdate>), GeminiError> {
    let mut setup = configured_setup(config, thinking, non_blocking_tools);
    if policy.is_some() {
        setup["setup"]["sessionResumption"] = match handle {
            Some(handle) => serde_json::json!({"handle": handle}),
            None => serde_json::json!({}),
        };
    }
    protocol::check_json_size(&setup, config.limits.max_message_bytes)?;
    let url = endpoint(config)?;
    let request = url
        .as_str()
        .into_client_request()
        .map_err(|_| GeminiError::Invalid("WebSocket request"))?;
    let ws_config = WebSocketConfig::default()
        .max_message_size(Some(config.limits.max_message_bytes))
        .max_frame_size(Some(config.limits.max_message_bytes));
    let (mut socket, _) = timeout(
        config.limits.connect_timeout,
        tokio_tungstenite::connect_async_with_config(request, Some(ws_config), true),
    )
    .await
    .map_err(|_| GeminiError::Timeout("connect"))?
    .map_err(transport)?;
    let update = timeout(config.limits.setup_timeout, async {
        socket
            .send(Message::Text(setup.to_string().into()))
            .await
            .map_err(transport)?;
        loop {
            let message = socket
                .next()
                .await
                .ok_or(GeminiError::Closed)?
                .map_err(transport)?;
            let bytes = match message {
                Message::Text(s) => s.as_bytes().to_vec(),
                Message::Binary(b) => b.to_vec(),
                Message::Ping(bytes) => {
                    socket.send(Message::Pong(bytes)).await.map_err(transport)?;
                    continue;
                }
                Message::Pong(_) => continue,
                Message::Close(_) => return Err(GeminiError::Closed),
                _ => return Err(GeminiError::Protocol("unexpected WebSocket frame")),
            };
            let packet = protocol::parse(&bytes, config, policy)?;
            if !packet.setup_complete
                || !packet.events.is_empty()
                || !packet.cancellations.is_empty()
            {
                return Err(GeminiError::Protocol(
                    "expected setupComplete before content",
                ));
            }
            return Ok(packet.resumption);
        }
    })
    .await
    .map_err(|_| GeminiError::Timeout("setup"))??;
    Ok((socket, update))
}

fn endpoint(config: &GeminiConfig) -> Result<Url, GeminiError> {
    let mut url = Url::parse(&config.endpoint).map_err(|_| GeminiError::Invalid("endpoint"))?;
    let loopback = url.host_str().is_some_and(|h| {
        h.parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
            || h == "[::1]"
    });
    if !(url.scheme() == "wss" || (url.scheme() == "ws" && loopback))
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GeminiError::Invalid(
            "endpoint requires TLS (or literal loopback), without userinfo, query, or fragment",
        ));
    }
    url.query_pairs_mut().append_pair("key", &config.api_key);
    Ok(url)
}
fn transport(error: tokio_tungstenite::tungstenite::Error) -> GeminiError {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            GeminiError::Http(response.status().as_u16())
        }
        tokio_tungstenite::tungstenite::Error::Capacity(_) => GeminiError::TooLarge,
        _ => GeminiError::Transport,
    }
}

#[derive(Clone)]
pub struct GeminiSender {
    config: Arc<GeminiConfig>,
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    generation: Arc<AtomicU64>,
    finishing: Arc<AtomicBool>,
    gate: Arc<Mutex<()>>,
    admission: Arc<std::sync::Mutex<()>>,
    tool_admissions: Arc<std::sync::Mutex<ToolAdmissions>>,
    non_blocking_tools: Arc<HashSet<String>>,
}
impl GeminiSender {
    pub(crate) fn is_finishing(&self) -> bool {
        self.finishing.load(Ordering::SeqCst) || self.control.borrow().cancel
    }
    /// Admission never waits behind network I/O. A full queue is an explicit error.
    pub fn try_send(&self, action: Action, origin: Origin) -> Result<(), GeminiError> {
        self.try_send_generation(action, origin, None)
    }
    pub(crate) fn try_send_video_at_generation(
        &self,
        jpeg: Vec<u8>,
        origin: Origin,
        generation: u64,
    ) -> Result<(), GeminiError> {
        self.try_send_generation(Action::VideoJpeg(jpeg), origin, Some(generation))
    }
    fn try_send_generation(
        &self,
        action: Action,
        origin: Origin,
        expected: Option<u64>,
    ) -> Result<(), GeminiError> {
        let _admission = self.admission.lock().map_err(|_| GeminiError::Closed)?;
        if (self.finishing.load(Ordering::SeqCst)
            && !matches!(
                action,
                Action::ToolResponses(_) | Action::NonBlockingToolResponses(_)
            ))
            || self.control.borrow().cancel
        {
            return Err(GeminiError::Closed);
        }
        if origin.context_id.as_ref().is_some_and(|s| s.len() > 1024) {
            return Err(GeminiError::TooLarge);
        }
        if expected.is_some_and(|generation| generation != self.generation()) {
            return Ok(());
        }
        protocol::action_messages(&action, &self.config)?;
        if let Action::NonBlockingToolResponses(responses) = &action {
            let admissions = self
                .tool_admissions
                .lock()
                .map_err(|_| GeminiError::Closed)?;
            if admissions.closed
                || responses.iter().any(|response| {
                    !self.non_blocking_tools.contains(&response.name)
                        || admissions.ids.get(response.id.as_str()) != Some(&CallAdmission::Claimed)
                })
            {
                return Err(GeminiError::Invalid(
                    "unknown, unclaimed, or canceled non-blocking tool result",
                ));
            }
        }
        // Preserve ordinary callers' admission-time sampling; encoded video uses
        // its explicitly captured generation even if interrupt raced validation.
        let generation = expected.unwrap_or_else(|| self.generation());
        self.tx
            .try_send(Command {
                action: Some(action),
                origin,
                generation,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("input"),
                mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
            })
    }
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
    /// Invalidate queued/generated output locally, without claiming a provider
    /// cancel or playback acknowledgement. Manual VAD activityStart, or actual
    /// audio under server VAD, is needed to interrupt the provider's generation.
    pub async fn interrupt(&self) {
        let _guard = self.gate.lock().await;
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.control.send_modify(|c| c.generation = generation);
    }
    pub(crate) async fn while_current<F: std::future::Future>(
        &self,
        generation: u64,
        future: F,
    ) -> Option<F::Output> {
        let _guard = self.gate.lock().await;
        if generation != self.generation() {
            return None;
        }
        Some(future.await)
    }
    /// Cancellation bypasses both bounded queues and interrupts pending writes.
    pub fn cancel(&self) {
        self.control.send_modify(|c| c.cancel = true);
    }
    /// Drain admitted input, then wait for the active turn/final STT transcript.
    /// A missing completion is an error at the configured finish deadline.
    pub async fn finish(&self) -> Result<(), GeminiError> {
        if self.finishing.load(Ordering::SeqCst) {
            return Ok(());
        }
        // Reserving is cancellation-safe: the stop-admission flag is committed
        // only together with the queued finish command, without an await.
        let permit = timeout(self.config.limits.write_timeout, self.tx.reserve())
            .await
            .map_err(|_| GeminiError::Timeout("finish admission"))?
            .map_err(|_| GeminiError::Closed)?;
        let _admission = self.admission.lock().map_err(|_| GeminiError::Closed)?;
        if !self.finishing.swap(true, Ordering::SeqCst) {
            permit.send(Command {
                action: None,
                origin: Origin::default(),
                generation: self.generation(),
            });
            self.control.send_modify(|_| {});
        }
        Ok(())
    }
}

pub struct GeminiSession {
    sender: GeminiSender,
    events: mpsc::Receiver<GeminiEvent>,
    worker: Option<JoinHandle<Result<(), GeminiError>>>,
}
impl GeminiSession {
    pub fn sender(&self) -> GeminiSender {
        self.sender.clone()
    }
    pub async fn next_event(&mut self) -> Option<Result<GeminiEvent, GeminiError>> {
        while let Some(mut event) = self.events.recv().await {
            if let Event::NonBlockingToolCalls(calls) = &mut event.event {
                let claimed = match self.sender.tool_admissions.lock() {
                    Ok(mut admissions) => {
                        // Session cancel revokes delivery immediately, including
                        // the interval before the worker observes it and drops
                        // its admission lifetime. Local interrupt is independent.
                        if self.sender.control.borrow().cancel {
                            false
                        } else {
                            admissions.claim(calls);
                            !calls.is_empty()
                        }
                    }
                    Err(_) => return Some(Err(GeminiError::Closed)),
                };
                if claimed {
                    return Some(Ok(event));
                }
                continue;
            }
            if !event.event.is_output() || event.generation == self.sender.generation() {
                return Some(Ok(event));
            }
        }
        let result = self.worker.as_mut()?.await;
        self.worker.take();
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(GeminiError::Transport)),
        }
    }
    pub async fn shutdown(mut self) -> Result<(), GeminiError> {
        self.sender.cancel();
        let result = match self.worker.as_mut() {
            Some(worker) => worker.await.map_err(|_| GeminiError::Transport)?,
            None => Ok(()),
        };
        self.worker.take();
        result
    }
}
impl Drop for GeminiSession {
    fn drop(&mut self) {
        self.sender.cancel();
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}

async fn write(
    socket: &mut Socket,
    message: Message,
    control: &mut watch::Receiver<Control>,
    duration: std::time::Duration,
) -> Result<(), GeminiError> {
    let sending = timeout(duration, socket.send(message));
    tokio::pin!(sending);
    loop {
        tokio::select! { biased;
            changed = control.changed() => { if changed.is_err() || control.borrow_and_update().cancel { return Err(GeminiError::Closed); } }
            result = &mut sending => return result.map_err(|_| GeminiError::Timeout("write"))?.map_err(transport),
        }
    }
}

async fn video_write_gate<'a>(
    sender: &'a GeminiSender,
    control: &mut watch::Receiver<Control>,
) -> Result<tokio::sync::MutexGuard<'a, ()>, GeminiError> {
    let locking = timeout(sender.config.limits.write_timeout, sender.gate.lock());
    tokio::pin!(locking);
    loop {
        if control.borrow().cancel {
            return Err(GeminiError::Closed);
        }
        tokio::select! { biased;
            changed=control.changed()=>{if changed.is_err() {return Err(GeminiError::Closed);}},
            result=&mut locking=>return result.map_err(|_|GeminiError::Timeout("video write gate")),
        }
    }
}

fn recoverable_read(
    message: &Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    go_away: bool,
) -> bool {
    use tokio_tungstenite::tungstenite::{error::ProtocolError, Error};
    match message {
        None => true,
        Some(Err(Error::Io(_) | Error::ConnectionClosed | Error::AlreadyClosed)) => true,
        Some(Err(Error::Protocol(ProtocolError::ResetWithoutClosingHandshake))) => true,
        Some(Ok(Message::Close(frame))) => {
            match frame.as_ref().map(|frame| u16::from(frame.code)) {
                Some(1001 | 1011 | 1012 | 1013) => true,
                Some(1000) | None => go_away,
                _ => false,
            }
        }
        _ => false,
    }
}

struct WorkerOptions {
    policy: Option<ResumptionPolicy>,
    handle: Option<String>,
    thinking: Option<ThinkingPolicy>,
}

async fn run(
    mut socket: Socket,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<GeminiEvent>,
    mut control: watch::Receiver<Control>,
    sender: GeminiSender,
    options: WorkerOptions,
) -> Result<(), GeminiError> {
    let _admission_lifetime = ToolAdmissionLifetime(sender.tool_admissions.clone());
    let WorkerOptions {
        policy,
        mut handle,
        thinking,
    } = options;
    struct HeldCompletion {
        origin: Origin,
        generation: u64,
        deadline: Instant,
    }
    let mut held: Option<HeldCompletion> = None;
    let mut reconnects = 0usize;
    let mut go_away = false;
    let mut tool_ids = ToolIds::new()?;
    let mut pending = HashMap::<String, PendingTool>::new();
    // Retain a bounded cancellation window without making abandoned calls
    // actionable or allowing them to hold graceful shutdown open.
    let mut retired = VecDeque::<PendingTool>::new();
    let mut input_origin = Origin::default();
    let mut output_origin = Origin::default();
    let mut generation = sender.generation();
    let mut output_active = false;
    let mut busy = false;
    let mut busy_generation = generation;
    let mut suppressed = false;
    let mut activity = false;
    let mut has_audio = false;
    let mut finishing = false;
    let mut finish_at = Instant::now() + sender.config.limits.finish_timeout;
    let result = async {
        loop {
            if control.borrow().cancel {
                return Ok(());
            }
            let current = sender.generation();
            if current != generation {
                retire_pending(
                    &mut pending,
                    &mut retired,
                    sender.config.limits.max_pending_tools,
                );
                generation = current;
                held = None;
                suppressed = busy || output_active;
                output_active = false;
            }
            if finishing && !busy && pending.is_empty() {
                write(
                    &mut socket,
                    Message::Close(None),
                    &mut control,
                    sender.config.limits.write_timeout,
                )
                .await?;
                return Ok(());
            }
            enum Wake {
                Control,
                Deadline,
                ThinkingDeadline,
                Message(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
                Command(Option<Command>),
            }
            let thinking_at = held.as_ref().map(|h| h.deadline);
            let wake = tokio::select! { biased;
                _ = control.changed() => Wake::Control,
                _ = tokio::time::sleep_until(finish_at), if finishing => Wake::Deadline,
                _ = tokio::time::sleep_until(thinking_at.unwrap_or(finish_at)), if thinking_at.is_some() => Wake::ThinkingDeadline,
                message = socket.next() => Wake::Message(message),
                command = commands.recv() => Wake::Command(command),
            };
            match wake {
                Wake::Control => {
                    if control.borrow_and_update().cancel {
                        return Ok(());
                    }
                }
                Wake::Deadline => return Err(GeminiError::Timeout("finish")),
                Wake::ThinkingDeadline => {
                    if held.as_ref().is_some_and(|h| h.generation == sender.generation()) {
                        return Err(GeminiError::Timeout("thinking silence"));
                    }
                    held = None;
                }
                Wake::Message(message) => {
                    if let Some(policy) = policy.as_ref().filter(|_| {
                        recoverable_read(&message, go_away)
                            && !finishing
                            && !sender.finishing.load(Ordering::SeqCst)
                    }) {
                        let token = handle.as_deref().ok_or(GeminiError::Closed)?;
                        if reconnects >= policy.max_reconnects {
                            return Err(GeminiError::Protocol(
                                "session resumption attempts exhausted",
                            ));
                        }
                        reconnects += 1;
                        // No old socket, reader or writer survives the replacement.
                        // Only setup is sent again. Commands remain in the existing
                        // bounded queue until setupComplete; no sent action is replayed.
                        drop(socket);
                        let (replacement, update) = {
                            let connecting = open_socket(&sender.config, Some(policy), Some(token), thinking.as_ref(), &sender.non_blocking_tools);
                            tokio::pin!(connecting);
                            loop {
                                if held.as_ref().is_some_and(|h| h.generation != sender.generation()) {
                                    held = None;
                                }
                                let thinking_at = held.as_ref().map(|h| h.deadline);
                                if control.borrow().cancel
                                    || sender.finishing.load(Ordering::SeqCst)
                                {
                                    return Err(GeminiError::Closed);
                                }
                                tokio::select! { biased;
                                    changed = control.changed() => {
                                        if changed.is_err() { return Err(GeminiError::Closed); }
                                        control.borrow_and_update();
                                    }
                                    _ = tokio::time::sleep_until(thinking_at.unwrap_or(finish_at)), if thinking_at.is_some() => {
                                        if held.as_ref().is_some_and(|h| h.generation == sender.generation()) {
                                            return Err(GeminiError::Timeout("thinking silence"));
                                        }
                                        held = None;
                                    }
                                    result = &mut connecting => break result?,
                                }
                            }
                        };
                        socket = replacement;
                        // End/Cancel racing with the handshake win before any new
                        // command or provider content can be consumed.
                        if control.borrow().cancel || sender.finishing.load(Ordering::SeqCst) {
                            return Err(GeminiError::Closed);
                        }
                        apply_resumption(&mut handle, update);
                        go_away = false;
                        // The top of the loop applies interruption generation changes
                        // before reading either the replacement socket or commands.
                        continue;
                    }
                    let bytes = match message.ok_or(GeminiError::Closed)?.map_err(transport)? {
                        Message::Text(s) => s.as_bytes().to_vec(),
                        Message::Binary(b) => b.to_vec(),
                        Message::Ping(b) => {
                            write(
                                &mut socket,
                                Message::Pong(b),
                                &mut control,
                                sender.config.limits.write_timeout,
                            )
                            .await?;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        Message::Close(_) => {
                            return if finishing && !busy && pending.is_empty() {
                                Ok(())
                            } else {
                                Err(GeminiError::Closed)
                            }
                        }
                        _ => return Err(GeminiError::Protocol("unexpected WebSocket frame")),
                    };
                    let packet = if thinking.is_some() {
                        protocol::parse_with_thinking(&bytes, &sender.config, policy.as_ref(), true)?
                    } else {
                        protocol::parse(&bytes, &sender.config, policy.as_ref())?
                    };
                    if packet.setup_complete {
                        return Err(GeminiError::Protocol("duplicate setupComplete"));
                    }
                    let cancellation_ids: HashSet<_> = packet.cancellations.iter().cloned().collect();
                    if !cancellation_ids.is_empty() && !sender.non_blocking_tools.is_empty() {
                        let mut admissions = sender.tool_admissions.lock().map_err(|_| GeminiError::Closed)?;
                        for id in &cancellation_ids {
                            admissions.revoke(id);
                        }
                    }
                    if let (Some(held), Some(thinking)) = (&mut held, &thinking) {
                        held.deadline = Instant::now() + thinking.silence_timeout;
                    }
                    let interaction_status = packet.interaction_status;
                    apply_resumption(&mut handle, packet.resumption);
                    for mut event in packet.events {
                        if policy.is_some() && matches!(event, Event::GoAway { .. }) {
                            go_away = true;
                            continue;
                        }
                        if matches!(event, Event::Interrupted) {
                            held = None;
                            sender.interrupt().await;
                            retire_pending(
                                &mut pending,
                                &mut retired,
                                sender.config.limits.max_pending_tools,
                            );
                            generation = sender.generation();
                            suppressed = false;
                            output_active = false;
                            // The protocol follows interrupted with turnComplete.
                            busy = true;
                            busy_generation = generation;
                        }
                        if let Event::ToolCalls(calls) = &mut event {
                            let mut staged = Vec::with_capacity(calls.len());
                            let mut batch_ids = HashSet::new();
                            // Validate the entire provider batch before admitting
                            // any call, including a mixed blocking/background batch.
                            for call in calls.iter_mut() {
                                tool_ids.assign(call)?;
                                if sender.non_blocking_tools.contains(&call.name)
                                    && (finishing || sender.finishing.load(Ordering::SeqCst))
                                {
                                    return Err(GeminiError::Protocol("new non-blocking tool call during finish"));
                                }
                                if pending.len().saturating_add(staged.len())
                                    >= sender.config.limits.max_pending_tools {
                                    return Err(GeminiError::TooLarge);
                                }
                                if pending.contains_key(&call.id)
                                    || retired.iter().any(|tool| tool.call.id == call.id)
                                    || !batch_ids.insert(call.id.clone())
                                {
                                    return Err(GeminiError::Protocol("duplicate pending tool ID"));
                                }
                                if !sender.config.session.tools.iter().any(|tool| tool.name == call.name) {
                                    return Err(GeminiError::Protocol("undeclared function call"));
                                }
                                staged.push(PendingTool {
                                    call: call.clone(),
                                    origin: if output_active { output_origin.clone() } else { input_origin.clone() },
                                    generation,
                                    non_blocking: sender.non_blocking_tools.contains(&call.name),
                                });
                            }
                            // Preserve provider ordering across alternating
                            // authority classes. Reserve the whole emitted batch
                            // before registering any selected call or pending tool.
                            let runs = tool_call_runs(&staged, suppressed, &cancellation_ids);
                            let permits = events.try_reserve_many(runs.len()).map_err(|error| match error {
                                mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("output"),
                                mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
                            })?;
                            if !sender.non_blocking_tools.is_empty() {
                                // Share finish()'s synchronous admission barrier.
                                // A selected batch either registers before finish
                                // commits, or is rejected without partial state.
                                let _input_admission = sender.admission.lock().map_err(|_| GeminiError::Closed)?;
                                if staged.iter().any(|tool| tool.non_blocking)
                                    && (finishing || sender.finishing.load(Ordering::SeqCst))
                                {
                                    return Err(GeminiError::Protocol("new non-blocking tool call during finish"));
                                }
                                let mut admissions = sender.tool_admissions.lock().map_err(|_| GeminiError::Closed)?;
                                // A previously selected identity cannot reappear
                                // with a blocking name either. Ordinary blocking
                                // IDs never enter this selected-call ledger.
                                if staged.iter().any(|tool| admissions.ids.contains_key(tool.call.id.as_str())) {
                                    return Err(GeminiError::Protocol("reused tool ID"));
                                }
                                let selected: Vec<_> = staged.iter().filter(|tool| tool.non_blocking).collect();
                                admissions.register(&selected, &cancellation_ids, &sender.config.limits)?;
                            }
                            let selected_origin = if output_active { output_origin.clone() } else { input_origin.clone() };
                            for tool in staged {
                                if suppressed && !tool.non_blocking {
                                    retire_tool(&mut retired, tool, sender.config.limits.max_pending_tools);
                                } else {
                                    pending.insert(tool.call.id.clone(), tool);
                                }
                            }
                            for ((selected, calls), permit) in runs.into_iter().zip(permits) {
                                let (event, origin) = if selected {
                                    (Event::NonBlockingToolCalls(calls), selected_origin.clone())
                                } else {
                                    if !output_active {
                                        output_origin = input_origin.clone();
                                        output_active = true;
                                        busy = true;
                                        busy_generation = generation;
                                    }
                                    (Event::ToolCalls(calls), output_origin.clone())
                                };
                                permit.send(GeminiEvent { event, origin, generation });
                            }
                            continue;
                        }
                        if matches!(event, Event::TurnComplete) {
                            if let Some(thinking) = &thinking {
                                if interaction_status == Some(protocol::InteractionStatus::InProgress) {
                                    if !suppressed {
                                        held.get_or_insert_with(|| HeldCompletion {
                                            origin: if output_active { output_origin.clone() } else { input_origin.clone() },
                                            generation,
                                            deadline: Instant::now() + thinking.silence_timeout,
                                        });
                                        busy = true;
                                        busy_generation = generation;
                                    }
                                    continue;
                                }
                                held = None;
                            }
                            // An identifiable locally suppressed old turn must not
                            // clear a newer admitted input's busy state.
                            if thinking.is_none() || !suppressed || busy_generation != generation {
                                busy = false;
                                has_audio = false;
                            }
                        }
                        if sender.config.mode == SessionMode::Transcription
                            && matches!(event, Event::InputTranscription { is_final: true, .. })
                        {
                            busy = false;
                            has_audio = false;
                        }
                        if event.is_output() && suppressed {
                            if matches!(event, Event::TurnComplete) {
                                suppressed = false;
                            }
                            continue;
                        }
                        if event.is_output() && !output_active {
                            output_origin = input_origin.clone();
                            output_active = true;
                            if !matches!(event, Event::TurnComplete) {
                                busy = true;
                                busy_generation = generation;
                            }
                        }
                        let origin = if event.is_output() || matches!(event, Event::ToolCalls(_)) {
                            output_origin.clone()
                        } else {
                            Origin::default()
                        };
                        let ended = matches!(event, Event::TurnComplete);
                        emit(
                            &events,
                            GeminiEvent {
                                event,
                                origin,
                                generation,
                            },
                        )?;
                        if ended {
                            output_active = false;
                        }
                    }
                    if !packet.cancellations.is_empty() {
                        let mut groups: Vec<(Origin, u64, bool, Vec<FunctionCall>)> = Vec::new();
                        for id in packet.cancellations {
                            let tool = pending.remove(&id).or_else(|| {
                                let index = retired.iter().position(|tool| tool.call.id == id)?;
                                retired.remove(index)
                            });
                            if let Some(tool) = tool {
                                let call_generation = if tool.non_blocking { tool.generation } else { generation };
                                if let Some((_, _, _, calls)) = groups.iter_mut().find(|(origin, g, selected, _)| {
                                    *origin == tool.origin && *g == call_generation && *selected == tool.non_blocking
                                }) {
                                    calls.push(tool.call);
                                } else {
                                    groups.push((tool.origin, call_generation, tool.non_blocking, vec![tool.call]));
                                }
                            } else {
                                return Err(GeminiError::Protocol(
                                    "cancellation references unknown tool ID",
                                ));
                            }
                        }
                        for (origin, call_generation, selected, calls) in groups {
                            emit(&events, GeminiEvent {
                                event: if selected { Event::NonBlockingToolCallsCancelled(calls) }
                                    else { Event::ToolCallsCancelled(calls) },
                                origin,
                                generation: call_generation,
                            })?;
                        }
                    }
                    if thinking.is_some() && interaction_status == Some(protocol::InteractionStatus::Idle) {
                        if let Some(completion) = held.take() {
                            if completion.generation == generation && generation == sender.generation() {
                                emit(&events, GeminiEvent {
                                    event: Event::TurnComplete,
                                    origin: completion.origin,
                                    generation: completion.generation,
                                })?;
                                busy = false;
                                has_audio = false;
                                output_active = false;
                            }
                        } else if suppressed {
                            // IDLE acknowledges the old drain, but is not a new
                            // completion and cannot finish newer queued input.
                            if busy_generation != generation { busy = false; }
                            suppressed = false;
                        }
                    }
                }
                Wake::Command(command) => {
                    let Some(command) = command else {
                        return Ok(());
                    };
                    let Some(action) = command.action else {
                        let flush = if activity {
                            Some(Action::ActivityEnd)
                        } else if has_audio && !sender.config.session.activity_detection.disabled {
                            Some(Action::AudioStreamEnd)
                        } else {
                            None
                        };
                        if let Some(flush) = flush {
                            for message in protocol::action_messages(&flush, &sender.config)? {
                                write(
                                    &mut socket,
                                    Message::Text(message.to_string().into()),
                                    &mut control,
                                    sender.config.limits.write_timeout,
                                )
                                .await?;
                            }
                            busy = true;
                            busy_generation = generation;
                        }
                        finishing = true;
                        finish_at = Instant::now() + sender.config.limits.finish_timeout;
                        continue;
                    };
                    // A video result keeps its original generation even if an external
                    // sender interrupts between encoding, enqueue and network write.
                    // The gate linearizes its write against interrupt(); cancellation
                    // still bypasses this gate and the write has a finite deadline.
                    let _video_gate = if matches!(action, Action::VideoJpeg(_)) {
                        Some(video_write_gate(&sender, &mut control).await?)
                    } else {
                        None
                    };
                    if matches!(action, Action::VideoJpeg(_))
                        && command.generation != sender.generation()
                    {
                        continue;
                    }
                    match &action {
                        Action::Audio(_)
                            if sender.config.session.activity_detection.disabled && !activity =>
                        {
                            return Err(GeminiError::Invalid(
                                "manual VAD audio outside activity window",
                            ))
                        }
                        Action::ActivityStart { .. } if activity => {
                            return Err(GeminiError::Invalid("activity already started"))
                        }
                        Action::ActivityEnd if !activity => {
                            return Err(GeminiError::Invalid("activity not started"))
                        }
                        Action::ToolResponses(responses) => {
                            for response in responses {
                                if !pending.get(&response.id).is_some_and(|tool| {
                                    !tool.non_blocking
                                        && tool.call.name == response.name
                                        && tool.generation == command.generation
                                        && command.generation == sender.generation()
                                }) {
                                    return Err(GeminiError::Invalid(
                                        "unknown, canceled, or mismatched tool result",
                                    ));
                                }
                            }
                        }
                        Action::NonBlockingToolResponses(responses) => {
                            let admissions = sender.tool_admissions.lock().map_err(|_| GeminiError::Closed)?;
                            validate_non_blocking_responses(responses, &pending, &admissions)?;
                        }
                        _ => {}
                    }
                    let messages = protocol::action_messages(&action, &sender.config)?;
                    for message in messages {
                        write(
                            &mut socket,
                            Message::Text(message.to_string().into()),
                            &mut control,
                            sender.config.limits.write_timeout,
                        )
                        .await?;
                    }
                    // Background results retain their call's origin. SILENT
                    // never changes turn ownership; WHEN_IDLE may establish an
                    // idle turn, but cannot replace even an input-only active
                    // turn, nor a generation interrupted during the wire write.
                    if !matches!(action, Action::NonBlockingToolResponses(_)) {
                        input_origin = command.origin;
                    }
                    if matches!(action, Action::ActivityEnd | Action::Audio(_) | Action::AudioStreamEnd | Action::RealtimeText(_) | Action::ClientContent { turn_complete: true, .. } | Action::ToolResponses(_)) {
                        busy_generation = command.generation;
                    }
                    match action {
                        Action::ActivityStart { preroll } => {
                            activity = true;
                            has_audio |= !preroll.is_empty();
                        }
                        Action::ActivityEnd => {
                            activity = false;
                            busy = true;
                        }
                        Action::Audio(_) => {
                            busy = true;
                            has_audio = true;
                        }
                        Action::AudioStreamEnd => {
                            busy = true;
                            has_audio = false;
                        }
                        Action::RealtimeText(_)
                        | Action::ClientContent {
                            turn_complete: true,
                            ..
                        } => busy = true,
                        Action::ToolResponses(responses) => {
                            for response in responses {
                                pending.remove(&response.id);
                            }
                            busy = true;
                        }
                        Action::NonBlockingToolResponses(responses) => {
                            if !busy && !output_active && !suppressed && !activity && !has_audio
                                && generation == sender.generation()
                            {
                                if let Some(tool) = responses.iter()
                                    .find(|response| response.scheduling == ToolResponseScheduling::WhenIdle)
                                    .and_then(|response| pending.get(&response.id))
                                {
                                    input_origin = tool.origin.clone();
                                    busy = true;
                                    busy_generation = generation;
                                }
                            }
                            // No final call is removed until the complete,
                            // validated response message was written successfully.
                            let mut admissions = sender.tool_admissions.lock().map_err(|_| GeminiError::Closed)?;
                            for response in responses {
                                if !response.will_continue {
                                    admissions.revoke(&response.id);
                                    pending.remove(&response.id);
                                }
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    .await;
    sender.finishing.store(true, Ordering::SeqCst);
    if sender.control.borrow().cancel {
        Ok(())
    } else {
        result
    }
}
fn emit(events: &mpsc::Sender<GeminiEvent>, event: GeminiEvent) -> Result<(), GeminiError> {
    events.try_send(event).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("output"),
        mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
    })
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use std::{task::Poll, time::Duration};
    use tokio::sync::oneshot;

    // Model the terminal cleanup interval: event delivery is closed, but the
    // connection task still owns an actual loopback WebSocket until its join.
    async fn held_terminal_session() -> (GeminiSession, oneshot::Sender<()>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/live", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            assert!(matches!(
                timeout(Duration::from_secs(2), socket.next())
                    .await
                    .expect("owned socket task leaked"),
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
        });
        let (socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (release_tx, release_rx) = oneshot::channel();
        let (tx, commands) = mpsc::channel(1);
        let (events, events_rx) = mpsc::channel(1);
        drop(events);
        let (control, control_rx) = watch::channel(Control::default());
        let sender = GeminiSender {
            config: Arc::new(GeminiConfig::new("synthetic-key", "source-model")),
            tx,
            control,
            generation: Arc::new(AtomicU64::new(0)),
            finishing: Arc::new(AtomicBool::new(true)),
            gate: Arc::new(Mutex::new(())),
            admission: Arc::new(std::sync::Mutex::new(())),
            tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
            non_blocking_tools: Arc::new(HashSet::new()),
        };
        let worker = tokio::spawn(async move {
            let _ = release_rx.await;
            drop((socket, commands, control_rx));
            Err(GeminiError::Protocol("synthetic terminal failure"))
        });
        (
            GeminiSession {
                sender,
                events: events_rx,
                worker: Some(worker),
            },
            release_tx,
            peer,
        )
    }

    #[tokio::test]
    async fn canceled_terminal_read_keeps_join_and_failure_for_retry() {
        let (mut session, release, peer) = held_terminal_session().await;
        {
            let read = session.next_event();
            tokio::pin!(read);
            assert!(matches!(futures_util::poll!(&mut read), Poll::Pending));
        }
        assert!(
            session.worker.is_some(),
            "terminal read detached the owned worker"
        );
        release.send(()).unwrap();
        assert_eq!(
            session.next_event().await,
            Some(Err(GeminiError::Protocol("synthetic terminal failure")))
        );
        assert!(session.next_event().await.is_none());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn drop_after_canceled_terminal_read_aborts_owned_socket() {
        let (mut session, release, peer) = held_terminal_session().await;
        {
            let read = session.next_event();
            tokio::pin!(read);
            assert!(matches!(futures_util::poll!(&mut read), Poll::Pending));
        }
        drop(session);
        peer.await.unwrap();
        assert!(release.send(()).is_err());
    }

    #[tokio::test]
    async fn canceled_shutdown_still_aborts_owned_socket() {
        let (session, release, peer) = held_terminal_session().await;
        {
            let shutdown = session.shutdown();
            tokio::pin!(shutdown);
            assert!(matches!(futures_util::poll!(&mut shutdown), Poll::Pending));
        }
        peer.await.unwrap();
        assert!(release.send(()).is_err());
    }
}

#[cfg(test)]
mod video_generation_tests {
    use super::*;
    #[tokio::test]
    async fn captured_video_generation_is_never_resampled_after_interrupt() {
        let (tx, mut rx) = mpsc::channel(2);
        let (control, _) = watch::channel(Control::default());
        let sender = GeminiSender {
            config: Arc::new(GeminiConfig::new("fixture", "model")),
            tx,
            control,
            generation: Arc::new(AtomicU64::new(0)),
            finishing: Arc::new(AtomicBool::new(false)),
            gate: Arc::new(Mutex::new(())),
            admission: Arc::new(std::sync::Mutex::new(())),
            tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
            non_blocking_tools: Arc::new(HashSet::new()),
        };
        let captured = sender.generation();
        sender
            .try_send_video_at_generation(vec![255, 216, 255, 217], Origin::default(), captured)
            .unwrap();
        sender.interrupt().await;
        let queued = rx.try_recv().unwrap();
        assert_eq!(queued.generation, captured);
        assert_ne!(queued.generation, sender.generation());
        sender
            .try_send_video_at_generation(vec![255, 216, 255, 217], Origin::default(), captured)
            .unwrap();
        assert!(rx.try_recv().is_err());
        sender
            .try_send(
                Action::VideoJpeg(vec![255, 216, 255, 217]),
                Origin::default(),
            )
            .unwrap();
        assert_eq!(rx.try_recv().unwrap().generation, sender.generation());
    }
    #[tokio::test]
    async fn cancel_bypasses_held_video_write_gate_and_joins_socket() {
        let wait = std::time::Duration::from_secs(2);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("fixture", "model");
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let client = GeminiClient::new(config).unwrap();
        let mut peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let mut socket = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            socket
                .next()
                .await
                .ok_or("no setup")?
                .map_err(|e| e.to_string())?;
            socket
                .send(Message::Text("{\"setupComplete\":{}}".into()))
                .await
                .map_err(|e| e.to_string())?;
            match socket.next().await {
                None | Some(Ok(Message::Close(_))) | Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)))=>Ok::<_,String>(()),
                other=>Err(format!("unexpected late video: {other:?}")),
            }
        });
        let connected = client.connect().await;
        let mut session = match connected {
            Ok(s) => s,
            Err(e) => {
                peer.abort();
                let _ = peer.await;
                panic!("connect {e}");
            }
        };
        let sender = session.sender();
        let held = sender.gate.lock().await;
        let sent = sender.try_send(
            Action::VideoJpeg(vec![255, 216, 255, 217]),
            Origin::default(),
        );
        let dequeued = timeout(wait, async {
            while sender.tx.capacity() != sender.config.limits.queue_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await;
        sender.cancel();
        let result = timeout(wait, session.next_event()).await;
        let bypassed = result.is_ok();
        drop(held); // Always release, including the old-deadlock failure path.
        let result = match result {
            Ok(r) => r,
            Err(_) => session.next_event().await,
        };
        let shutdown = session.shutdown().await;
        let peer_result = match timeout(wait, &mut peer).await {
            Ok(r) => r,
            Err(e) => {
                peer.abort();
                let _ = peer.await;
                panic!("peer aborted and joined {e}");
            }
        };
        assert!(sent.is_ok());
        assert!(dequeued.is_ok());
        assert!(bypassed);
        assert!(result.is_none());
        assert!(shutdown.is_ok());
        assert!(peer_result.unwrap().is_ok());
    }
}

#[cfg(test)]
mod resumption_tests {
    use super::*;
    use tokio_tungstenite::tungstenite::protocol::{frame::coding::CloseCode, CloseFrame};

    #[test]
    fn go_away_does_not_override_fatal_close_codes() {
        for code in [
            1000u16, 1001, 1002, 1003, 1007, 1008, 1009, 1011, 1012, 1013,
        ] {
            let frame = Some(Ok(Message::Close(Some(CloseFrame {
                code: CloseCode::from(code),
                reason: "fixture".into(),
            }))));
            assert_eq!(
                recoverable_read(&frame, false),
                matches!(code, 1001 | 1011 | 1012 | 1013)
            );
            assert_eq!(
                recoverable_read(&frame, true),
                matches!(code, 1000 | 1001 | 1011 | 1012 | 1013)
            );
        }
        assert!(!recoverable_read(&Some(Ok(Message::Close(None))), false));
        assert!(recoverable_read(&Some(Ok(Message::Close(None))), true));
    }

    #[test]
    fn resumption_policy_is_bounded_and_false_revokes_saved_token() {
        for (max_reconnects, max_handle_bytes) in [(0, 1), (9, 1), (1, 0), (1, 65537)] {
            assert!(ResumptionPolicy {
                max_reconnects,
                max_handle_bytes
            }
            .validate()
            .is_err());
        }
        let mut handle = Some("previous".into());
        apply_resumption(&mut handle, None);
        assert_eq!(handle.as_deref(), Some("previous"));
        apply_resumption(
            &mut handle,
            Some(protocol::ResumptionUpdate {
                resumable: false,
                new_handle: Some("must-not-replace".into()),
            }),
        );
        assert!(handle.is_none());
    }

    #[test]
    fn resumption_updates_are_validated_before_bundled_content() {
        let config = GeminiConfig::new("fixture", "model");
        let policy = ResumptionPolicy {
            max_reconnects: 1,
            max_handle_bytes: 4,
        };
        for update in [
            serde_json::json!({"resumable": true}),
            serde_json::json!({"resumable": true, "newHandle": ""}),
            serde_json::json!({"resumable": false, "newHandle": "too-long"}),
            serde_json::json!({"resumable": "true", "newHandle": "x"}),
        ] {
            let packet = serde_json::json!({
                "sessionResumptionUpdate": update,
                "serverContent": {"modelTurn": {"parts": [{"text": "not admitted"}]}}
            });
            assert!(protocol::parse(
                &serde_json::to_vec(&packet).unwrap(),
                &config,
                Some(&policy)
            )
            .is_err());
        }
        assert!(matches!(
            protocol::parse(
                br#"{"sessionResumptionUpdate":{"resumable":true,"newHandle":"x"}}"#,
                &config,
                None
            ),
            Err(GeminiError::Unsupported("session resumption"))
        ));
    }
}

#[cfg(test)]
mod thinking_configuration_tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn thinking_policy_rejects_unbounded_and_incompatible_settings_before_connect() {
        for duration in [Duration::ZERO, Duration::from_secs(86_401)] {
            assert!(matches!(
                GeminiClient::new(GeminiConfig::new("fixture", "model"))
                    .unwrap()
                    .with_thinking(ThinkingPolicy {
                        level: ThinkingLevel::Low,
                        silence_timeout: duration
                    }),
                Err(GeminiError::Invalid("thinking silence timeout"))
            ));
        }
        let mut config = GeminiConfig::new("fixture", "model");
        config.session.tools.push(FunctionDeclaration {
            name: "tool".into(),
            description: String::new(),
            parameters: serde_json::json!({}),
        });
        assert!(matches!(
            GeminiClient::new(config)
                .unwrap()
                .with_thinking(ThinkingPolicy::default()),
            Err(GeminiError::Unsupported(_))
        ));
        let config = GeminiConfig::transcription("fixture", "model");
        assert!(matches!(
            GeminiClient::new(config)
                .unwrap()
                .with_thinking(ThinkingPolicy::default()),
            Err(GeminiError::Unsupported(_))
        ));
        for level in [
            ThinkingLevel::Low,
            ThinkingLevel::Medium,
            ThinkingLevel::High,
        ] {
            assert!(GeminiClient::new(GeminiConfig::new("fixture", "model"))
                .unwrap()
                .with_thinking(ThinkingPolicy {
                    level,
                    ..ThinkingPolicy::default()
                })
                .is_ok());
        }
    }
}

#[cfg(test)]
mod non_blocking_admission_tests {
    use super::*;

    fn config(model: &str) -> GeminiConfig {
        let mut config = GeminiConfig::new("fixture", model);
        config.session.tools = ["background", "ordinary"]
            .into_iter()
            .map(|name| FunctionDeclaration {
                name: name.into(),
                description: String::new(),
                parameters: serde_json::json!({}),
            })
            .collect();
        config
    }
    fn tool(id: &str, non_blocking: bool) -> PendingTool {
        PendingTool {
            call: FunctionCall {
                id: id.into(),
                id_source: ToolCallIdSource::Provider,
                name: if non_blocking {
                    "background"
                } else {
                    "ordinary"
                }
                .into(),
                args: serde_json::json!({}),
            },
            origin: Origin {
                frame_id: Some(42),
                context_id: Some("original".into()),
            },
            generation: 7,
            non_blocking,
        }
    }
    fn sender() -> (GeminiSender, mpsc::Receiver<Command>) {
        let (tx, rx) = mpsc::channel(8);
        let (control, _) = watch::channel(Control::default());
        (
            GeminiSender {
                config: Arc::new(config("gemini-3.8-live")),
                tx,
                control,
                generation: Arc::new(AtomicU64::new(0)),
                finishing: Arc::new(AtomicBool::new(false)),
                gate: Arc::new(Mutex::new(())),
                admission: Arc::new(std::sync::Mutex::new(())),
                tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
                non_blocking_tools: Arc::new(HashSet::from(["background".into()])),
            },
            rx,
        )
    }

    #[test]
    fn explicit_profiles_selection_and_unselected_setup_are_closed() {
        for model in [
            "gemini-3.8-live",
            "models/gemini-3.8-live",
            "gemini-2.5-flash-native-audio-preview-12-2025",
        ] {
            let config = config(model);
            assert_eq!(
                configured_setup(&config, None, &HashSet::new()),
                protocol::setup(&config)
            );
            let client = GeminiClient::new(config.clone())
                .unwrap()
                .with_non_blocking_tools(vec!["background".into()])
                .unwrap();
            assert!(client.is_non_blocking_tool("background"));
            assert!(!client.is_non_blocking_tool("ordinary"));
            let setup = configured_setup(&config, None, &client.non_blocking_tools);
            let declarations = &setup["setup"]["tools"][0]["functionDeclarations"];
            assert_eq!(declarations[0]["behavior"], "NON_BLOCKING");
            if model.ends_with("gemini-3.8-live") {
                assert_eq!(declarations[1]["behavior"], "BLOCKING");
            } else {
                assert!(declarations[1].get("behavior").is_none());
            }
            assert!(client.with_thinking(ThinkingPolicy::default()).is_err());
        }
        for model in [
            "gemini-3.8-live-preview",
            "gemini-3.8-live-thinking",
            "gemini-2.5-flash-native-audio-preview",
            "models/models/gemini-3.8-live",
        ] {
            let result = GeminiClient::new(config(model))
                .and_then(|client| client.with_non_blocking_tools(vec!["background".into()]));
            assert!(result.is_err(), "unexpected model {model}");
        }
        for names in [
            vec![],
            vec!["".into()],
            vec!["unknown".into()],
            vec!["background".into(), "background".into()],
        ] {
            assert!(GeminiClient::new(config("gemini-3.8-live"))
                .unwrap()
                .with_non_blocking_tools(names)
                .is_err());
        }
        let thinking = GeminiClient::new(GeminiConfig::new("fixture", "gemini-3.8-live"))
            .unwrap()
            .with_thinking(ThinkingPolicy::default())
            .unwrap();
        assert!(thinking
            .with_non_blocking_tools(vec!["background".into()])
            .is_err());
    }

    #[test]
    fn complete_batch_rejection_never_partially_registers_or_charges() {
        let mut ledger = ToolAdmissions::default();
        let limits = Limits::default();
        let first = tool("first", true);
        let duplicate = tool("first", true);
        assert!(ledger
            .register(&[&first, &duplicate], &HashSet::new(), &limits)
            .is_err());
        assert!(ledger.ids.is_empty());
        assert_eq!(ledger.id_bytes, 0);
        let long = tool(&"x".repeat(1025), true);
        assert!(ledger
            .register(&[&first, &long], &HashSet::new(), &limits)
            .is_err());
        assert!(ledger.ids.is_empty());
        let mut tiny = limits.clone();
        tiny.max_message_bytes = 6;
        let second = tool("second", true);
        assert_eq!(
            ledger.register(&[&first, &second], &HashSet::new(), &tiny),
            Err(GeminiError::TooLarge)
        );
        assert!(ledger.ids.is_empty());
        assert_eq!(ledger.id_bytes, 0);
        ledger
            .register(&[&first], &HashSet::new(), &limits)
            .unwrap();
        let charged = ledger.id_bytes;
        assert!(ledger
            .register(&[&second, &first], &HashSet::new(), &limits)
            .is_err());
        assert_eq!(ledger.ids.len(), 1);
        assert_eq!(ledger.id_bytes, charged);
        assert!(!ledger.ids.contains_key("second"));
    }

    #[test]
    fn tombstones_never_recycle_and_count_exhaustion_rejects_whole_batch() {
        let mut ledger = ToolAdmissions::default();
        let mut limits = Limits::default();
        limits.max_pending_tools = 1;
        for n in 0..31 {
            let tool = tool(&format!("id-{n}"), true);
            ledger.register(&[&tool], &HashSet::new(), &limits).unwrap();
            ledger.revoke(&tool.call.id);
        }
        let a = tool("last-a", true);
        let b = tool("last-b", true);
        assert_eq!(
            ledger.register(&[&a, &b], &HashSet::new(), &limits),
            Err(GeminiError::TooLarge)
        );
        assert_eq!(ledger.ids.len(), 31);
        assert!(!ledger.ids.contains_key("last-a"));
        let old = tool("id-0", true);
        assert_eq!(
            ledger.register(&[&old], &HashSet::new(), &limits),
            Err(GeminiError::Protocol("reused tool ID"))
        );
        ledger.register(&[&a], &HashSet::new(), &limits).unwrap();
        assert_eq!(ledger.ids.len(), 32);
    }

    #[test]
    fn claim_and_revoke_have_one_linearized_order_and_same_packet_cancel_wins() {
        for cancel_first in [false, true] {
            let t = tool("selected", true);
            let mut ledger = ToolAdmissions::default();
            ledger
                .register(&[&t], &HashSet::new(), &Limits::default())
                .unwrap();
            let mut calls = vec![t.call.clone()];
            if cancel_first {
                ledger.revoke("selected");
            }
            ledger.claim(&mut calls);
            assert_eq!(calls.len(), usize::from(!cancel_first));
            ledger.revoke("selected");
            let mut duplicate = vec![t.call.clone()];
            ledger.claim(&mut duplicate);
            assert!(duplicate.is_empty());
            assert_eq!(ledger.ids.get("selected"), Some(&CallAdmission::Retired));
        }
        let t = tool("same-packet", true);
        let mut ledger = ToolAdmissions::default();
        ledger
            .register(
                &[&t],
                &HashSet::from(["same-packet".into()]),
                &Limits::default(),
            )
            .unwrap();
        let mut calls = vec![t.call];
        ledger.claim(&mut calls);
        assert!(calls.is_empty());
    }

    #[test]
    fn interrupt_preserves_selected_identity_and_origin_but_retires_blocking() {
        let selected = tool("selected", true);
        let expected = selected.call.clone();
        let origin = selected.origin.clone();
        let ordinary = tool("ordinary", false);
        let mut pending =
            HashMap::from([("selected".into(), selected), ("ordinary".into(), ordinary)]);
        let mut retired = VecDeque::new();
        retire_pending(&mut pending, &mut retired, 2);
        assert_eq!(pending.len(), 1);
        assert_eq!(pending["selected"].call, expected);
        assert_eq!(pending["selected"].origin, origin);
        assert_eq!(pending["selected"].generation, 7);
        assert_eq!(retired.len(), 1);
        assert!(!retired[0].non_blocking);
    }

    #[tokio::test]
    async fn next_event_claim_is_generation_independent_and_cancel_drops_unclaimed_call() {
        for cancel_first in [false, true] {
            let (sender, _commands) = sender();
            let t = tool("selected", true);
            sender
                .tool_admissions
                .lock()
                .unwrap()
                .register(&[&t], &HashSet::new(), &sender.config.limits)
                .unwrap();
            let (events, rx) = mpsc::channel(4);
            events
                .try_send(GeminiEvent {
                    event: Event::NonBlockingToolCalls(vec![t.call.clone()]),
                    origin: t.origin.clone(),
                    generation: t.generation,
                })
                .unwrap();
            sender.interrupt().await;
            assert_ne!(sender.generation(), t.generation);
            if cancel_first {
                sender.tool_admissions.lock().unwrap().revoke("selected");
                events
                    .try_send(GeminiEvent {
                        event: Event::NonBlockingToolCallsCancelled(vec![t.call]),
                        origin: t.origin.clone(),
                        generation: t.generation,
                    })
                    .unwrap();
            }
            drop(events);
            let mut session = GeminiSession {
                sender,
                events: rx,
                worker: None,
            };
            let event = session.next_event().await.unwrap().unwrap();
            assert_eq!(event.origin, t.origin);
            assert_eq!(event.generation, 7);
            if cancel_first {
                assert!(matches!(
                    event.event,
                    Event::NonBlockingToolCallsCancelled(_)
                ));
            } else {
                assert!(matches!(event.event, Event::NonBlockingToolCalls(_)));
            }
            assert!(session.next_event().await.is_none());
        }

        // The worker is deliberately alive after cancel() returns. Its cleanup
        // cannot supply the revocation that next_event must enforce itself.
        let (sender, _commands) = sender();
        let t = tool("queued-before-session-cancel", true);
        sender
            .tool_admissions
            .lock()
            .unwrap()
            .register(&[&t], &HashSet::new(), &sender.config.limits)
            .unwrap();
        let (events, rx) = mpsc::channel(1);
        events
            .try_send(GeminiEvent {
                event: Event::NonBlockingToolCalls(vec![t.call]),
                origin: t.origin,
                generation: t.generation,
            })
            .unwrap();
        let (release, released) = tokio::sync::oneshot::channel();
        let (entered, started) = tokio::sync::oneshot::channel();
        let admissions = sender.tool_admissions.clone();
        let worker = tokio::spawn(async move {
            let _lifetime = ToolAdmissionLifetime(admissions);
            let _ = entered.send(());
            let _ = released.await;
            drop(events);
            Ok(())
        });
        started.await.unwrap();
        sender.cancel();
        assert!(!sender.tool_admissions.lock().unwrap().closed);
        let mut session = GeminiSession {
            sender,
            events: rx,
            worker: Some(worker),
        };
        {
            let next = session.next_event();
            tokio::pin!(next);
            assert!(
                matches!(futures_util::poll!(&mut next), std::task::Poll::Pending),
                "session cancellation must prevent the queued call from being claimed"
            );
        }
        release.send(()).unwrap();
        assert!(
            timeout(std::time::Duration::from_secs(2), session.next_event())
                .await
                .unwrap()
                .is_none()
        );
        assert!(
            session.worker.is_none(),
            "terminal read must join the worker"
        );
        assert!(session.sender.tool_admissions.lock().unwrap().closed);
    }

    #[tokio::test]
    async fn selected_response_admission_survives_interrupt_and_finishing_but_needs_claim() {
        let (sender, mut commands) = sender();
        let t = tool("selected", true);
        sender
            .tool_admissions
            .lock()
            .unwrap()
            .register(&[&t], &HashSet::new(), &sender.config.limits)
            .unwrap();
        let response = Action::NonBlockingToolResponses(vec![NonBlockingFunctionResponse {
            id: t.call.id.clone(),
            name: t.call.name.clone(),
            response: serde_json::json!({"ok": true}),
            will_continue: false,
            scheduling: ToolResponseScheduling::Silent,
        }]);
        assert!(sender
            .try_send(response.clone(), Origin::default())
            .is_err());
        sender
            .tool_admissions
            .lock()
            .unwrap()
            .claim(&mut vec![t.call]);
        sender.finishing.store(true, Ordering::SeqCst);
        sender
            .try_send(response.clone(), Origin::default())
            .unwrap();
        sender.interrupt().await;
        let queued = commands.try_recv().unwrap();
        assert_ne!(queued.generation, sender.generation());
        sender
            .try_send(response.clone(), Origin::default())
            .unwrap();
        sender.tool_admissions.lock().unwrap().revoke("selected");
        assert!(sender.try_send(response, Origin::default()).is_err());
    }

    #[test]
    fn worker_lifetime_drop_revokes_and_clears_all_queued_control_authority() {
        let ledger = Arc::new(std::sync::Mutex::new(ToolAdmissions::default()));
        let t = tool("selected", true);
        ledger
            .lock()
            .unwrap()
            .register(&[&t], &HashSet::new(), &Limits::default())
            .unwrap();
        drop(ToolAdmissionLifetime(ledger.clone()));
        let mut ledger = ledger.lock().unwrap();
        assert!(ledger.closed);
        assert!(ledger.ids.is_empty());
        assert_eq!(ledger.id_bytes, 0);
        let mut queued = vec![t.call];
        ledger.claim(&mut queued);
        assert!(queued.is_empty());
    }

    #[test]
    fn alternating_authority_runs_preserve_provider_call_order() {
        let tools = vec![
            tool("b1", false),
            tool("s1", true),
            tool("b2", false),
            tool("s2", true),
            tool("s3", true),
        ];
        let runs = tool_call_runs(&tools, false, &HashSet::new());
        assert_eq!(
            runs.iter()
                .map(|(selected, _)| *selected)
                .collect::<Vec<_>>(),
            [false, true, false, true]
        );
        assert_eq!(
            runs.iter()
                .flat_map(|(_, calls)| calls.iter().map(|call| call.id.as_str()))
                .collect::<Vec<_>>(),
            ["b1", "s1", "b2", "s2", "s3"]
        );
        let suppressed = tool_call_runs(&tools, true, &HashSet::from(["s2".into()]));
        assert!(suppressed.iter().all(|(selected, _)| *selected));
        assert_eq!(
            suppressed
                .iter()
                .flat_map(|(_, calls)| calls.iter().map(|call| call.id.as_str()))
                .collect::<Vec<_>>(),
            ["s1", "s3"]
        );
    }

    #[test]
    fn mixed_batch_reservation_fails_before_selected_registration() {
        let tools = vec![tool("b", false), tool("s", true), tool("b2", false)];
        let runs = tool_call_runs(&tools, false, &HashSet::new());
        let (events, mut received) = mpsc::channel::<GeminiEvent>(2);
        let ledger = ToolAdmissions::default();
        assert!(matches!(
            events.try_reserve_many(runs.len()),
            Err(mpsc::error::TrySendError::Full(()))
        ));
        assert_eq!(events.capacity(), 2);
        assert!(received.try_recv().is_err());
        assert!(ledger.ids.is_empty());
        assert_eq!(ledger.id_bytes, 0);
    }

    #[test]
    fn worker_response_validation_rejects_unknown_unclaimed_canceled_and_mismatched_results() {
        let t = tool("selected", true);
        let mut ledger = ToolAdmissions::default();
        ledger
            .register(&[&t], &HashSet::new(), &Limits::default())
            .unwrap();
        let call = t.call.clone();
        let pending = HashMap::from([("selected".into(), t)]);
        let response = NonBlockingFunctionResponse {
            id: "selected".into(),
            name: "background".into(),
            response: serde_json::json!({}),
            will_continue: true,
            scheduling: ToolResponseScheduling::Silent,
        };
        assert!(validate_non_blocking_responses(
            std::slice::from_ref(&response),
            &pending,
            &ledger
        )
        .is_err());
        ledger.claim(&mut vec![call]);
        assert!(validate_non_blocking_responses(
            std::slice::from_ref(&response),
            &pending,
            &ledger
        )
        .is_ok());
        let mut unknown = response.clone();
        unknown.id = "unknown".into();
        assert!(validate_non_blocking_responses(&[unknown], &pending, &ledger).is_err());
        let mut mismatched = response.clone();
        mismatched.name = "ordinary".into();
        assert!(validate_non_blocking_responses(&[mismatched], &pending, &ledger).is_err());
        ledger.revoke("selected");
        assert!(validate_non_blocking_responses(&[response], &pending, &ledger).is_err());
        assert!(
            pending.contains_key("selected"),
            "validation must not finalize a call"
        );
    }
}
