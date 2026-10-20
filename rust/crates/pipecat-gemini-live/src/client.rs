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
    selected: Option<SelectedPlan>,
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
    stt_keepalive: Option<SttKeepalivePolicy>,
    thinking: Option<ThinkingPolicy>,
    initial_history: Option<(InitialHistoryPolicy, protocol::InitialHistoryModel)>,
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
            stt_keepalive: None,
            thinking: None,
            initial_history: None,
            non_blocking_tools: Arc::new(HashSet::new()),
        })
    }
    /// Send 100 ms of zero PCM when a ready STT session is idle. Synthetic
    /// input does not establish a user turn or delay an otherwise idle finish.
    /// A failed write is terminal, not retried on a possibly partial socket.
    pub fn with_stt_keepalive(mut self, policy: SttKeepalivePolicy) -> Result<Self, GeminiError> {
        policy.validate()?;
        if self.config.mode != SessionMode::Transcription {
            return Err(GeminiError::Unsupported(
                "STT keepalive requires transcription mode",
            ));
        }
        // Validate the exact encoded wire payload before opening any socket.
        keepalive_message(&self.config)?;
        self.stt_keepalive = Some(policy);
        Ok(self)
    }
    /// Opt in to bounded receive-side recovery with an opaque server handle.
    /// Connect/setup/write/protocol failures are terminal; no action is replayed.
    /// Handles are private to each session and never shared by cloned clients.
    pub fn with_session_resumption(
        mut self,
        policy: ResumptionPolicy,
    ) -> Result<Self, GeminiError> {
        policy.validate()?;
        if self.initial_history.is_some() {
            return Err(GeminiError::Unsupported(
                "initial history with session resumption",
            ));
        }
        self.resumption = Some(policy);
        Ok(self)
    }
    /// Enable bounded initial-history startup owned by `GeminiLiveProcessor`.
    /// Only complete Gemini 2.5 or Gemini 3 model tokens are accepted; model
    /// parsing does not establish hosted availability. Session resumption and
    /// transcription-only sessions are deliberately unsupported.
    pub fn with_initial_history(
        mut self,
        policy: InitialHistoryPolicy,
    ) -> Result<Self, GeminiError> {
        if self.resumption.is_some() {
            return Err(GeminiError::Unsupported(
                "initial history with session resumption",
            ));
        }
        if self.config.mode != SessionMode::Conversation {
            return Err(GeminiError::Unsupported(
                "initial history requires conversation mode",
            ));
        }
        let model = protocol::initial_history_model(&self.config.session.model)?;
        let setup = configured_setup(
            &self.config,
            self.thinking.as_ref(),
            &self.non_blocking_tools,
            true,
        );
        protocol::check_json_size(&setup, self.config.limits.max_message_bytes)?;
        self.initial_history = Some((policy, model));
        Ok(self)
    }
    pub(crate) fn initial_history_policy(&self) -> Option<InitialHistoryPolicy> {
        self.initial_history.map(|(policy, _)| policy)
    }
    pub(crate) fn plan_initial_history(
        &self,
        context: &pipecat_core::LlmContextFrame,
    ) -> Result<InitialHistorySeed, GeminiError> {
        let (policy, model) = self
            .initial_history
            .ok_or(GeminiError::Unsupported("initial history is not selected"))?;
        let turns = protocol::initial_history_turns(context, &self.config, model)?;
        let pending_first_stop = !turns.is_empty()
            && !policy.inference_on_initialization
            && model == protocol::InitialHistoryModel::Gemini25;
        let actions = if turns.is_empty() {
            Vec::new()
        } else {
            completion_actions(turns, policy.inference_on_initialization, model)
        };
        let messages = selected_messages(&actions, &self.config)?;
        Ok(InitialHistorySeed {
            config: self.config.clone(),
            policy,
            model,
            actions,
            messages,
            pending_first_stop,
        })
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
        let mut setup = configured_setup(
            &self.config,
            None,
            &self.non_blocking_tools,
            self.initial_history.is_some(),
        );
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
        let setup = configured_setup(
            &self.config,
            self.thinking.as_ref(),
            &selected,
            self.initial_history.is_some(),
        );
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
    /// The selected route is deliberately single-session and tool-free.
    /// Keep this preflight separate so its constructor can fail before setup.
    #[cfg(feature = "paired-history")]
    pub(crate) fn validate_paired_history(&self) -> Result<(), GeminiError> {
        if self.config.mode != SessionMode::Conversation || !self.config.session.input_transcription
        {
            return Err(GeminiError::Unsupported(
                "paired history requires conversation input transcription",
            ));
        }
        if self.resumption.is_some() {
            return Err(GeminiError::Unsupported(
                "paired history session resumption",
            ));
        }
        if !self.config.session.tools.is_empty() || !self.non_blocking_tools.is_empty() {
            return Err(GeminiError::Unsupported("paired history tools"));
        }
        Ok(())
    }

    #[cfg(feature = "paired-history")]
    pub(crate) async fn connect_paired_history(&self) -> Result<GeminiSession, GeminiError> {
        self.validate_paired_history()?;
        self.connect_with_policy(ReceivePolicy::PairedHistory, None)
            .await
    }

    /// Returns only after the peer acknowledges setup. Dropping this future
    /// drops its socket; it does not leave a detached handshake task.
    pub async fn connect(&self) -> Result<GeminiSession, GeminiError> {
        if self.initial_history.is_some() {
            return Err(GeminiError::Unsupported(
                "initial history requires a processor-owned connection",
            ));
        }
        self.connect_with_policy(ReceivePolicy::Raw, None).await
    }

    pub(crate) async fn connect_owned(&self, run_id: u64) -> Result<GeminiSession, GeminiError> {
        self.connect_with_policy(ReceivePolicy::Raw, Some(run_id))
            .await
    }
    #[cfg(feature = "paired-history")]
    pub(crate) async fn connect_paired_history_owned(
        &self,
        run_id: u64,
    ) -> Result<GeminiSession, GeminiError> {
        self.validate_paired_history()?;
        self.connect_with_policy(ReceivePolicy::PairedHistory, Some(run_id))
            .await
    }

    async fn connect_with_policy(
        &self,
        receive_policy: ReceivePolicy,
        run_id: Option<u64>,
    ) -> Result<GeminiSession, GeminiError> {
        let selected = match self.initial_history {
            Some((policy, model)) => Some(Arc::new(SelectedSessionTag {
                run_id: run_id.ok_or(GeminiError::Unsupported(
                    "initial history requires a processor-owned connection",
                ))?,
                policy,
                model,
                closed: AtomicBool::new(false),
                #[cfg(test)]
                write_boundary: std::sync::Mutex::new(None),
                #[cfg(test)]
                validation_boundary: std::sync::Mutex::new(None),
                #[cfg(test)]
                join_boundary: std::sync::Mutex::new(None),
                #[cfg(test)]
                abort_join_pending: std::sync::Mutex::new(None),
            })),
            None => None,
        };
        let config = &self.config;
        let (socket, update) = open_socket(
            config,
            self.resumption.as_ref(),
            None,
            self.thinking.as_ref(),
            &self.non_blocking_tools,
            self.initial_history.is_some(),
            receive_policy,
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
            selected,
        };
        let worker_sender = sender.clone();
        let policy = self.resumption.clone();
        let thinking = self.thinking.clone();
        let stt_keepalive = self
            .stt_keepalive
            .map(|policy| SttKeepalive::new(policy, config))
            .transpose()?;
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
                    receive_policy,
                    stt_keepalive,
                },
            )
            .await
        });
        Ok(GeminiSession {
            sender,
            events: events_rx,
            worker: Some(worker),
            joined: SelectedJoinObserver::default(),
            selected_terminal: SelectedTerminal::default(),
        })
    }
}
fn configured_setup(
    config: &GeminiConfig,
    thinking: Option<&ThinkingPolicy>,
    non_blocking_tools: &HashSet<String>,
    initial_history: bool,
) -> serde_json::Value {
    let mut setup = protocol::setup(config);
    if initial_history {
        setup["setup"]["historyConfig"] =
            serde_json::json!({"initialHistoryInClientContent": true});
    }
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
    initial_history: bool,
    receive_policy: ReceivePolicy,
) -> Result<(Socket, Option<protocol::ResumptionUpdate>), GeminiError> {
    #[cfg(not(feature = "paired-history"))]
    let _ = receive_policy;
    let mut setup = configured_setup(config, thinking, non_blocking_tools, initial_history);
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
            #[cfg(feature = "paired-history")]
            if matches!(receive_policy, ReceivePolicy::PairedHistory) && packet.has_tool_fields {
                return Err(GeminiError::Unsupported("paired history provider tools"));
            }
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

/// Read-only evidence that the selected session's actual socket JoinHandle was
/// awaited. EOF, cancellation and a task's own completion signal do not set it.
#[derive(Clone, Default)]
pub struct SelectedJoinObserver {
    joined: Arc<AtomicBool>,
}
impl SelectedJoinObserver {
    pub fn joined(&self) -> bool {
        self.joined.load(Ordering::SeqCst)
    }
}

#[derive(Default)]
struct SelectedTerminal {
    deadline: Option<Instant>,
    forced: bool,
    outcome: Option<Result<(), GeminiError>>,
}

#[cfg(test)]
type SelectedWriteBoundary = (
    tokio::sync::oneshot::Sender<()>,
    tokio::sync::oneshot::Receiver<()>,
);

struct SelectedSessionTag {
    run_id: u64,
    policy: InitialHistoryPolicy,
    model: protocol::InitialHistoryModel,
    closed: AtomicBool,
    #[cfg(test)]
    write_boundary: std::sync::Mutex<Option<SelectedWriteBoundary>>,
    #[cfg(test)]
    validation_boundary: std::sync::Mutex<Option<SelectedWriteBoundary>>,
    #[cfg(test)]
    join_boundary: std::sync::Mutex<Option<SelectedWriteBoundary>>,
    #[cfg(test)]
    abort_join_pending: std::sync::Mutex<Option<tokio::sync::oneshot::Sender<()>>>,
}

pub(crate) struct InitialHistorySeed {
    config: Arc<GeminiConfig>,
    policy: InitialHistoryPolicy,
    model: protocol::InitialHistoryModel,
    actions: Vec<Action>,
    messages: Vec<String>,
    pending_first_stop: bool,
}
impl InitialHistorySeed {
    pub(crate) fn pending_first_stop(&self) -> bool {
        self.pending_first_stop
    }
}

/// This private value has no public/deserializable constructor. Its immutable
/// connection capability, generation and complete bounded wire plan travel in
/// one queue slot, including a zero-wire empty initialization transition.
pub(crate) struct SelectedPlan {
    tag: Arc<SelectedSessionTag>,
    generation: u64,
    origin: Origin,
    actions: Vec<Action>,
    messages: Vec<String>,
    atomic_transition: bool,
    strict_generation: bool,
}

fn completion_actions(
    turns: Vec<TextTurn>,
    complete: bool,
    model: protocol::InitialHistoryModel,
) -> Vec<Action> {
    let mut actions = vec![Action::ClientContent {
        turns,
        turn_complete: complete,
    }];
    if complete && model == protocol::InitialHistoryModel::Gemini3 {
        actions.push(Action::RealtimeText(" ".into()));
    }
    actions
}
fn selected_messages(
    actions: &[Action],
    config: &GeminiConfig,
) -> Result<Vec<String>, GeminiError> {
    let mut messages = Vec::new();
    let mut bytes = 0usize;
    for action in actions {
        for message in protocol::action_messages(action, config)? {
            let message =
                serde_json::to_string(&message).map_err(|_| GeminiError::Invalid("JSON"))?;
            bytes = bytes
                .checked_add(message.len())
                .ok_or(GeminiError::TooLarge)?;
            if bytes > config.limits.max_message_bytes {
                return Err(GeminiError::TooLarge);
            }
            messages.push(message);
        }
    }
    Ok(messages)
}

#[cfg(test)]
pub(crate) struct SelectedQueueHold {
    _permits: Vec<mpsc::OwnedPermit<Command>>,
}
#[cfg(test)]
impl GeminiSender {
    pub(crate) fn test_hold_queue(&self) -> Result<SelectedQueueHold, GeminiError> {
        let mut permits = Vec::new();
        for _ in 0..self.tx.capacity() {
            permits.push(
                self.tx
                    .clone()
                    .try_reserve_owned()
                    .map_err(|_| GeminiError::Backpressure("test queue hold"))?,
            );
        }
        Ok(SelectedQueueHold { _permits: permits })
    }
    fn test_notify_pending_abort_join(&self) -> tokio::sync::oneshot::Receiver<()> {
        let (entered, notified) = tokio::sync::oneshot::channel();
        *self
            .selected
            .as_ref()
            .expect("selected session")
            .abort_join_pending
            .lock()
            .unwrap() = Some(entered);
        notified
    }
    pub(crate) fn test_pause_before_selected_join(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self
            .selected
            .as_ref()
            .expect("selected session")
            .join_boundary
            .lock()
            .unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }
    fn test_pause_before_validation(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self
            .selected
            .as_ref()
            .expect("selected session")
            .validation_boundary
            .lock()
            .unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }
    pub(crate) fn test_pause_second_write(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        tokio::sync::oneshot::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel();
        *self
            .selected
            .as_ref()
            .expect("selected session")
            .write_boundary
            .lock()
            .unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }
}

impl GeminiSender {
    pub(crate) fn selected_run_id(&self) -> Option<u64> {
        self.selected.as_ref().map(|tag| tag.run_id)
    }
    pub(crate) fn close_selected_admission(&self, run_id: u64) -> Result<(), GeminiError> {
        let _guard = self.admission.lock().map_err(|_| GeminiError::Closed)?;
        let tag = self.selected_tag(run_id)?;
        tag.closed.store(true, Ordering::SeqCst);
        Ok(())
    }
    fn selected_tag(&self, run_id: u64) -> Result<Arc<SelectedSessionTag>, GeminiError> {
        let tag = self
            .selected
            .as_ref()
            .ok_or(GeminiError::Unsupported("initial history is not selected"))?;
        if self.selected_run_id() != Some(run_id) {
            return Err(GeminiError::Closed);
        }
        Ok(tag.clone())
    }
    fn selected_plan(
        &self,
        run_id: u64,
        actions: Vec<Action>,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        let tag = self.selected_tag(run_id)?;
        if origin.context_id.as_ref().is_some_and(|id| id.len() > 1024) {
            return Err(GeminiError::TooLarge);
        }
        let messages = selected_messages(&actions, &self.config)?;
        Ok(SelectedPlan {
            tag,
            generation,
            origin,
            actions,
            messages,
            atomic_transition: false,
            strict_generation: false,
        })
    }
    pub(crate) fn plan_seed(
        &self,
        run_id: u64,
        seed: InitialHistorySeed,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        let tag = self.selected_tag(run_id)?;
        if !Arc::ptr_eq(&seed.config, &self.config)
            || seed.policy != tag.policy
            || seed.model != tag.model
        {
            return Err(GeminiError::Invalid(
                "initial history belongs to a different client",
            ));
        }
        if origin.context_id.as_ref().is_some_and(|id| id.len() > 1024) {
            return Err(GeminiError::TooLarge);
        }
        let has_wire = !seed.messages.is_empty();
        Ok(SelectedPlan {
            tag,
            generation,
            origin,
            actions: seed.actions,
            messages: seed.messages,
            atomic_transition: has_wire,
            strict_generation: has_wire,
        })
    }
    pub(crate) fn plan_content(
        &self,
        run_id: u64,
        turns: Vec<TextTurn>,
        turn_complete: bool,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        let model = self.selected_tag(run_id)?.model;
        let mut plan = self.selected_plan(
            run_id,
            completion_actions(turns, turn_complete, model),
            origin,
            generation,
        )?;
        plan.atomic_transition = plan.messages.len() > 1;
        Ok(plan)
    }
    pub(crate) fn plan_completion(
        &self,
        run_id: u64,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.plan_content(run_id, Vec::new(), true, origin, generation)
    }
    pub(crate) fn plan_first_stop(
        &self,
        run_id: u64,
        activity_end: bool,
        complete: bool,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        let model = self.selected_tag(run_id)?.model;
        let mut actions = Vec::new();
        if activity_end {
            actions.push(Action::ActivityEnd);
        }
        if complete {
            actions.extend(completion_actions(Vec::new(), true, model));
        }
        let mut plan = self.selected_plan(run_id, actions, origin, generation)?;
        plan.atomic_transition = complete;
        plan.strict_generation = complete;
        Ok(plan)
    }
    pub(crate) fn plan_audio(
        &self,
        run_id: u64,
        audio: Vec<u8>,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(run_id, vec![Action::Audio(audio)], origin, generation)
    }
    pub(crate) fn plan_activity_start(
        &self,
        run_id: u64,
        preroll: Vec<u8>,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(
            run_id,
            vec![Action::ActivityStart { preroll }],
            origin,
            generation,
        )
    }
    pub(crate) fn plan_realtime_text(
        &self,
        run_id: u64,
        text: String,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(run_id, vec![Action::RealtimeText(text)], origin, generation)
    }
    pub(crate) fn plan_video(
        &self,
        run_id: u64,
        jpeg: Vec<u8>,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(run_id, vec![Action::VideoJpeg(jpeg)], origin, generation)
    }
    pub(crate) fn plan_audio_stream_end(
        &self,
        run_id: u64,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(run_id, vec![Action::AudioStreamEnd], origin, generation)
    }
    pub(crate) fn plan_tool_responses(
        &self,
        run_id: u64,
        responses: Vec<FunctionResponse>,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(
            run_id,
            vec![Action::ToolResponses(responses)],
            origin,
            generation,
        )
    }
    pub(crate) fn plan_non_blocking_tool_responses(
        &self,
        run_id: u64,
        responses: Vec<NonBlockingFunctionResponse>,
        origin: Origin,
        generation: u64,
    ) -> Result<SelectedPlan, GeminiError> {
        self.selected_plan(
            run_id,
            vec![Action::NonBlockingToolResponses(responses)],
            origin,
            generation,
        )
    }
    pub(crate) fn try_send_selected(&self, plan: SelectedPlan) -> Result<(), GeminiError> {
        let _guard = self.admission.lock().map_err(|_| GeminiError::Closed)?;
        let tag = self
            .selected
            .as_ref()
            .ok_or(GeminiError::Unsupported("initial history is not selected"))?;
        let tool_settlement = matches!(
            plan.actions.as_slice(),
            [Action::ToolResponses(_)] | [Action::NonBlockingToolResponses(_)]
        );
        if !Arc::ptr_eq(tag, &plan.tag)
            || self.control.borrow().cancel
            || (!tool_settlement
                && (tag.closed.load(Ordering::SeqCst) || self.finishing.load(Ordering::SeqCst)))
        {
            return Err(GeminiError::Closed);
        }
        if plan.generation != self.generation() {
            return if matches!(plan.actions.as_slice(), [Action::VideoJpeg(_)]) {
                Ok(())
            } else {
                Err(GeminiError::Closed)
            };
        }
        for action in &plan.actions {
            if let Action::NonBlockingToolResponses(responses) = action {
                let admissions = self
                    .tool_admissions
                    .lock()
                    .map_err(|_| GeminiError::Closed)?;
                if admissions.closed
                    || responses.iter().any(|response| {
                        !self.non_blocking_tools.contains(&response.name)
                            || admissions.ids.get(response.id.as_str())
                                != Some(&CallAdmission::Claimed)
                    })
                {
                    return Err(GeminiError::Invalid(
                        "unknown, unclaimed, or canceled non-blocking tool result",
                    ));
                }
            }
        }
        self.tx
            .try_send(Command {
                action: None,
                origin: plan.origin.clone(),
                generation: plan.generation,
                selected: Some(plan),
            })
            .map_err(|error| match error {
                mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("input"),
                mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
            })
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
    selected: Option<Arc<SelectedSessionTag>>,
}
impl GeminiSender {
    pub(crate) fn is_finishing(&self) -> bool {
        self.finishing.load(Ordering::SeqCst) || self.control.borrow().cancel
    }
    /// Admission never waits behind network I/O. A full queue is an explicit error.
    pub fn try_send(&self, action: Action, origin: Origin) -> Result<(), GeminiError> {
        if self.selected.is_some() {
            return Err(GeminiError::Unsupported(
                "raw actions on an initial-history session",
            ));
        }
        self.try_send_generation(action, origin, None)
    }
    pub(crate) fn try_send_video_at_generation(
        &self,
        jpeg: Vec<u8>,
        origin: Origin,
        generation: u64,
    ) -> Result<(), GeminiError> {
        if self.selected.is_some() {
            return Err(GeminiError::Unsupported(
                "raw actions on an initial-history session",
            ));
        }
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
                selected: None,
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
        // This short synchronous gate linearizes selected admission with
        // generation changes. It is never held across an await.
        let _admission = self
            .selected
            .as_ref()
            .map(|_| self.admission.lock().unwrap_or_else(|e| e.into_inner()));
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
                selected: None,
                origin: Origin::default(),
                generation: self.generation(),
            });
            self.control.send_modify(|_| {});
        }
        Ok(())
    }
}

/// One bounded queue slot carries an event and only its own packet's details.
#[derive(Debug)]
pub(crate) struct ReceivedEvent {
    pub event: GeminiEvent,
    pub usage_details: Option<protocol::UsageDetails>,
    #[cfg(feature = "paired-history")]
    pub packet: Option<Arc<protocol::InputPacket>>,
}
impl ReceivedEvent {
    fn from_packet(
        event: GeminiEvent,
        details: &mut Option<protocol::UsageDetails>,
        #[cfg(feature = "paired-history")] packet: Option<Arc<protocol::InputPacket>>,
    ) -> Self {
        let usage_details = if matches!(event.event, Event::Usage(_)) {
            details.take()
        } else {
            None
        };
        Self {
            event,
            usage_details,
            #[cfg(feature = "paired-history")]
            packet,
        }
    }
}
impl From<GeminiEvent> for ReceivedEvent {
    fn from(event: GeminiEvent) -> Self {
        Self {
            event,
            usage_details: None,
            #[cfg(feature = "paired-history")]
            packet: None,
        }
    }
}

pub struct GeminiSession {
    sender: GeminiSender,
    events: mpsc::Receiver<ReceivedEvent>,
    worker: Option<JoinHandle<Result<(), GeminiError>>>,
    joined: SelectedJoinObserver,
    selected_terminal: SelectedTerminal,
}
impl GeminiSession {
    pub fn sender(&self) -> GeminiSender {
        self.sender.clone()
    }
    /// Obtain immutable actual-join evidence for this selected connection.
    pub fn selected_join_observer(&self) -> Option<SelectedJoinObserver> {
        self.sender.selected.as_ref().map(|_| self.joined.clone())
    }
    pub(crate) fn join_observer(&self) -> SelectedJoinObserver {
        self.joined.clone()
    }
    fn record_join(
        &mut self,
        result: Result<Result<(), GeminiError>, tokio::task::JoinError>,
    ) -> Result<(), GeminiError> {
        self.worker.take();
        self.joined.joined.store(true, Ordering::SeqCst);
        let result = if self.selected_terminal.forced {
            Err(GeminiError::Timeout("selected shutdown"))
        } else {
            result.unwrap_or(Err(GeminiError::Transport))
        };
        if self.sender.selected.is_some() {
            self.selected_terminal.outcome = Some(result.clone());
        }
        result
    }
    /// Borrow the sole real JoinHandle across cancellation. The first shutdown
    /// fixes a finish+write budget; retries retain it and any forced failure.
    /// Timeout requests abort then actually awaits the same handle. Tokio abort
    /// remains cooperative; no extra reaper or cleanup task is introduced.
    pub(crate) async fn selected_shutdown(&mut self) -> Result<(), GeminiError> {
        self.sender.cancel();
        if let Some(result) = &self.selected_terminal.outcome {
            return result.clone();
        }
        if self.worker.is_none() {
            return Ok(());
        }
        let deadline = match self.selected_terminal.deadline {
            Some(deadline) => deadline,
            None => {
                let budget = self
                    .sender
                    .config
                    .limits
                    .finish_timeout
                    .checked_add(self.sender.config.limits.write_timeout)
                    .ok_or(GeminiError::Invalid("selected shutdown budget"))?;
                let deadline = Instant::now()
                    .checked_add(budget)
                    .ok_or(GeminiError::Invalid("selected shutdown deadline"))?;
                self.selected_terminal.deadline = Some(deadline);
                deadline
            }
        };
        #[cfg(test)]
        if let Some(tag) = &self.sender.selected {
            let boundary = tag.join_boundary.lock().unwrap().take();
            if let Some((entered, release)) = boundary {
                // Test-only: cancel and the fixed deadline already exist, but
                // the same sole actor handle has not yet been awaited here.
                let _ = entered.send(());
                let _ = release.await;
            }
        }
        let result = if self.selected_terminal.forced {
            self.worker.as_mut().expect("owned worker").await
        } else {
            match tokio::time::timeout_at(deadline, self.worker.as_mut().expect("owned worker"))
                .await
            {
                Ok(result) => result,
                Err(_) => {
                    self.selected_terminal.forced = true;
                    let worker = self.worker.as_mut().expect("owned worker");
                    worker.abort();
                    #[cfg(test)]
                    {
                        let mut notification = self
                            .sender
                            .selected
                            .as_ref()
                            .and_then(|tag| tag.abort_join_pending.lock().unwrap().take());
                        std::future::poll_fn(|cx| {
                            // This notification proves that abort was requested
                            // and the actual retained JoinHandle was polled and
                            // remained pending. It neither delays the actor nor
                            // replaces the handle with a synthetic future.
                            let result =
                                std::future::Future::poll(std::pin::Pin::new(&mut *worker), cx);
                            if result.is_pending() {
                                if let Some(entered) = notification.take() {
                                    let _ = entered.send(());
                                }
                            }
                            result
                        })
                        .await
                    }
                    #[cfg(not(test))]
                    worker.await
                }
            }
        };
        self.record_join(result)
    }
    pub async fn next_event(&mut self) -> Option<Result<GeminiEvent, GeminiError>> {
        self.next_event_with_details()
            .await
            .map(|result| result.map(|received| received.event))
    }
    pub(crate) async fn next_event_with_details(
        &mut self,
    ) -> Option<Result<ReceivedEvent, GeminiError>> {
        while let Some(mut received) = self.events.recv().await {
            let event = &mut received.event;
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
                    return Some(Ok(received));
                }
                continue;
            }
            if !event.event.is_output() || event.generation == self.sender.generation() {
                return Some(Ok(received));
            }
        }
        let result = self.worker.as_mut()?.await;
        match self.record_join(result) {
            Ok(()) => None,
            Err(error) => Some(Err(error)),
        }
    }
    pub async fn shutdown(mut self) -> Result<(), GeminiError> {
        self.sender.cancel();
        if self.sender.selected.is_some() {
            return self.selected_shutdown().await;
        }
        let result = match self.worker.as_mut() {
            Some(worker) => worker.await,
            None => return Ok(()),
        };
        self.record_join(result)
    }

    /// Retain the sole worker owner across a dropped selected terminal wait.
    /// Only an actual JoinHandle await can complete this operation; a peer EOF
    /// or a signal from the worker is insufficient. Forced Session drop keeps
    /// its existing, explicitly weaker abort-without-await semantics.
    #[cfg(feature = "paired-history")]
    pub(crate) async fn cancel_and_join(&mut self) -> Result<(), GeminiError> {
        if self.sender.selected.is_some() {
            return self.selected_shutdown().await;
        }
        self.sender.cancel();
        let result = match self.worker.as_mut() {
            Some(worker) => worker.await.map_err(|_| GeminiError::Transport),
            None => return Ok(()),
        };
        self.worker.take();
        result?
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

/// Unlike a raw write, interruption invalidates an entire selected command.
/// Cancellation or generation change during any member's write terminates the
/// socket actor, rather than permitting an unknown partial seed to be retried.
async fn selected_write(
    socket: &mut Socket,
    message: String,
    control: &mut watch::Receiver<Control>,
    duration: std::time::Duration,
    generation: u64,
) -> Result<(), GeminiError> {
    let sending = timeout(duration, socket.send(Message::Text(message.into())));
    tokio::pin!(sending);
    loop {
        if control.borrow().cancel {
            return Err(GeminiError::Closed);
        }
        if control.borrow().generation != generation {
            return Err(GeminiError::Protocol(
                "initial-history command generation changed",
            ));
        }
        tokio::select! { biased;
            changed = control.changed() => { if changed.is_err() { return Err(GeminiError::Closed); } }
            result = &mut sending => return result.map_err(|_| GeminiError::Timeout("write"))?.map_err(transport),
        }
    }
}

#[cfg(test)]
async fn selected_test_validation_boundary(
    tag: &SelectedSessionTag,
    control: &mut watch::Receiver<Control>,
) -> Result<(), GeminiError> {
    let boundary = tag.validation_boundary.lock().unwrap().take();
    if let Some((entered, release)) = boundary {
        let _ = entered.send(());
        tokio::pin!(release);
        loop {
            if control.borrow().cancel {
                return Err(GeminiError::Closed);
            }
            tokio::select! { biased;
                changed = control.changed() => { if changed.is_err() { return Err(GeminiError::Closed); } }
                _ = &mut release => return Ok(()),
            }
        }
    }
    Ok(())
}

#[cfg(test)]
async fn selected_test_write_boundary(
    tag: &SelectedSessionTag,
    control: &mut watch::Receiver<Control>,
    generation: u64,
) -> Result<(), GeminiError> {
    let boundary = tag.write_boundary.lock().unwrap().take();
    if let Some((entered, release)) = boundary {
        let _ = entered.send(());
        tokio::pin!(release);
        loop {
            if control.borrow().cancel {
                return Err(GeminiError::Closed);
            }
            if control.borrow().generation != generation {
                return Err(GeminiError::Protocol(
                    "initial-history command generation changed",
                ));
            }
            tokio::select! { biased;
                changed = control.changed() => { if changed.is_err() { return Err(GeminiError::Closed); } }
                _ = &mut release => return Ok(()),
            }
        }
    }
    Ok(())
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

#[derive(Clone, Copy)]
enum ReceivePolicy {
    Raw,
    #[cfg(feature = "paired-history")]
    PairedHistory,
}

#[cfg(feature = "paired-history")]
#[derive(Default)]
struct InputWatermarks {
    sequence: u64,
    received: u64,
}

#[cfg(feature = "paired-history")]
impl InputWatermarks {
    fn observe(
        &mut self,
        packet: &mut protocol::Parsed,
    ) -> Result<Arc<protocol::InputPacket>, GeminiError> {
        // Validate both additions before changing either counter. Empty actual
        // input and explicit interim input do not consume an input ordinal.
        let sequence = self.sequence.checked_add(1).ok_or(GeminiError::TooLarge)?;
        let received = self
            .received
            .checked_add(u64::from(packet.actual_input.is_some()))
            .ok_or(GeminiError::TooLarge)?;
        self.sequence = sequence;
        self.received = received;
        Ok(Arc::new(protocol::InputPacket {
            sequence,
            received_input_watermark: received,
            actual_input: packet.actual_input.take(),
        }))
    }
}

fn keepalive_message(config: &GeminiConfig) -> Result<String, GeminiError> {
    let silence = vec![0; (config.input_sample_rate / 10) as usize * 2];
    let messages = protocol::action_messages(&Action::Audio(silence), config)?;
    Ok(messages
        .into_iter()
        .next()
        .expect("audio has one message")
        .to_string())
}

struct SttKeepalive {
    policy: SttKeepalivePolicy,
    message: String,
    last_audio: Instant,
    next_check: Instant,
}
impl SttKeepalive {
    fn new(policy: SttKeepalivePolicy, config: &GeminiConfig) -> Result<Self, GeminiError> {
        let now = Instant::now();
        Ok(Self {
            policy,
            message: keepalive_message(config)?,
            last_audio: now,
            next_check: now + policy.check_interval,
        })
    }
    fn ready(&mut self) {
        self.last_audio = Instant::now();
        self.next_check = self.last_audio + self.policy.check_interval;
    }
}

struct WorkerOptions {
    stt_keepalive: Option<SttKeepalive>,
    policy: Option<ResumptionPolicy>,
    handle: Option<String>,
    thinking: Option<ThinkingPolicy>,
    receive_policy: ReceivePolicy,
}

async fn run(
    mut socket: Socket,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<ReceivedEvent>,
    mut control: watch::Receiver<Control>,
    sender: GeminiSender,
    options: WorkerOptions,
) -> Result<(), GeminiError> {
    let _admission_lifetime = ToolAdmissionLifetime(sender.tool_admissions.clone());
    let WorkerOptions {
        policy,
        mut handle,
        thinking,
        receive_policy,
        stt_keepalive: mut keepalive,
    } = options;
    #[cfg(feature = "paired-history")]
    let mut input_watermarks = InputWatermarks::default();
    struct HeldCompletion {
        origin: Origin,
        generation: u64,
        deadline: Instant,
        #[cfg(feature = "paired-history")]
        packet: Option<Arc<protocol::InputPacket>>,
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
                Keepalive,
                Message(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
                Command(Option<Command>),
            }
            let thinking_at = held.as_ref().map(|h| h.deadline);
            let keepalive_at = keepalive.as_ref().filter(|_|
                !finishing && !sender.finishing.load(Ordering::SeqCst)
            ).map(|state| state.next_check);
            let wake = tokio::select! { biased;
                _ = control.changed() => Wake::Control,
                _ = tokio::time::sleep_until(finish_at), if finishing => Wake::Deadline,
                _ = tokio::time::sleep_until(thinking_at.unwrap_or(finish_at)), if thinking_at.is_some() => Wake::ThinkingDeadline,
                message = socket.next() => Wake::Message(message),
                command = commands.recv() => Wake::Command(command),
                _ = tokio::time::sleep_until(keepalive_at.unwrap_or(finish_at)), if keepalive_at.is_some() => Wake::Keepalive,
            };
            match wake {
                Wake::Control => {
                    if control.borrow_and_update().cancel {
                        return Ok(());
                    }
                }
                Wake::Deadline => return Err(GeminiError::Timeout("finish")),
                Wake::Keepalive => {
                    let state = keepalive.as_mut().expect("enabled timer");
                    // Delay missed checks; never catch up with a burst. Explicit
                    // queued commands and socket/control input win the select.
                    state.next_check = Instant::now() + state.policy.check_interval;
                    if Instant::now().duration_since(state.last_audio) < state.policy.idle_timeout
                        || sender.finishing.load(Ordering::SeqCst) || control.borrow().cancel {
                        continue;
                    }
                    // This is deliberately not ordinary Action::Audio admission:
                    // no busy/has_audio/attribution/generation/usage transition.
                    write(&mut socket, Message::Text(state.message.clone().into()),
                        &mut control, sender.config.limits.write_timeout).await?;
                    state.last_audio = Instant::now();
                    state.next_check = state.last_audio + state.policy.check_interval;
                }
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
                            let connecting = open_socket(&sender.config, Some(policy), Some(token), thinking.as_ref(), &sender.non_blocking_tools, false, receive_policy);
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
                        if let Some(state) = &mut keepalive { state.ready(); }
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
                    let mut packet = if thinking.is_some() {
                        protocol::parse_with_thinking(&bytes, &sender.config, policy.as_ref(), true)?
                    } else {
                        protocol::parse(&bytes, &sender.config, policy.as_ref())?
                    };
                    if packet.setup_complete {
                        return Err(GeminiError::Protocol("duplicate setupComplete"));
                    }
                    #[cfg(feature = "paired-history")]
                    let packet_descriptor = match receive_policy {
                        ReceivePolicy::Raw => None,
                        ReceivePolicy::PairedHistory => {
                            // Validate the complete envelope before dispatching
                            // even its earlier model/input/usage fields. Empty
                            // cancellation lists and null tool fields are not
                            // an escape from the selected tool-free contract.
                            if packet.has_tool_fields {
                                return Err(GeminiError::Unsupported("paired history provider tools"));
                            }
                            Some(input_watermarks.observe(&mut packet)?)
                        }
                    };
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
                        #[cfg(feature = "paired-history")]
                        let mut event_packet = packet_descriptor.clone();
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
                                permit.send(ReceivedEvent::from_packet(
                                    GeminiEvent { event, origin, generation },
                                    &mut packet.usage_details,
                                    #[cfg(feature = "paired-history")]
                                    packet_descriptor.clone(),
                                ));
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
                                            #[cfg(feature = "paired-history")]
                                            packet: packet_descriptor.clone(),
                                        });
                                        busy = true;
                                        busy_generation = generation;
                                    }
                                    continue;
                                }
                                #[cfg(feature = "paired-history")]
                                if let Some(completion) = &held {
                                    // An IDLE envelope may repeat turnComplete.
                                    // Preserve the original held dependency just
                                    // as in the synthetic IDLE-only path below.
                                    event_packet = completion.packet.clone();
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
                            ReceivedEvent::from_packet(
                                GeminiEvent { event, origin, generation },
                                &mut packet.usage_details,
                                #[cfg(feature = "paired-history")]
                                event_packet,
                            ),
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
                            emit(&events, ReceivedEvent::from_packet(
                                GeminiEvent {
                                    event: if selected { Event::NonBlockingToolCallsCancelled(calls) }
                                        else { Event::ToolCallsCancelled(calls) },
                                    origin,
                                    generation: call_generation,
                                },
                                &mut packet.usage_details,
                                #[cfg(feature = "paired-history")]
                                packet_descriptor.clone(),
                            ))?;
                        }
                    }
                    if thinking.is_some() && interaction_status == Some(protocol::InteractionStatus::Idle) {
                        if let Some(completion) = held.take() {
                            if completion.generation == generation && generation == sender.generation() {
                                emit(&events, ReceivedEvent::from_packet(
                                    GeminiEvent {
                                        event: Event::TurnComplete,
                                        origin: completion.origin,
                                        generation: completion.generation,
                                    },
                                    &mut None,
                                    #[cfg(feature = "paired-history")]
                                    completion.packet,
                                ))?;
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
                    if let Some(plan) = command.selected {
                        let tag = sender.selected.as_ref().ok_or(GeminiError::Closed)?;
                        if !Arc::ptr_eq(tag, &plan.tag) { return Err(GeminiError::Closed); }
                        let video_only = matches!(plan.actions.as_slice(), [Action::VideoJpeg(_)]);
                        let _video_gate = if video_only {
                            Some(video_write_gate(&sender, &mut control).await?)
                        } else { None };
                        if command.generation != sender.generation() {
                            // Media results retain their existing stale suppression;
                            // a published state transition must never silently vanish.
                            if video_only { continue; }
                            if plan.strict_generation {
                                return Err(GeminiError::Protocol("initial-history command generation changed"));
                            }
                        }
                        #[cfg(test)]
                        selected_test_validation_boundary(tag, &mut control).await?;
                        let mut planned_activity = activity;
                        for action in &plan.actions {
                            match action {
                                Action::Audio(_) if sender.config.session.activity_detection.disabled && !planned_activity => {
                                    return Err(GeminiError::Invalid("manual VAD audio outside activity window"));
                                }
                                Action::ActivityStart { .. } => {
                                    if planned_activity { return Err(GeminiError::Invalid("activity already started")); }
                                    planned_activity = true;
                                }
                                Action::ActivityEnd => {
                                    if !planned_activity { return Err(GeminiError::Invalid("activity not started")); }
                                    planned_activity = false;
                                }
                                Action::ToolResponses(responses) => {
                                    for response in responses {
                                        if !pending.get(&response.id).is_some_and(|tool| {
                                            !tool.non_blocking && tool.call.name == response.name
                                                && tool.generation == command.generation
                                                && command.generation == sender.generation()
                                        }) { return Err(GeminiError::Invalid("unknown, canceled, or mismatched tool result")); }
                                    }
                                }
                                Action::NonBlockingToolResponses(responses) => {
                                    let admissions = sender.tool_admissions.lock().map_err(|_| GeminiError::Closed)?;
                                    validate_non_blocking_responses(responses, &pending, &admissions)?;
                                }
                                _ => {}
                            }
                        }
                        // All messages/effects have been preflighted before the
                        // first write. One selected command is never interleaved
                        // with another command; interrupted partial delivery is
                        // terminal and is never resumed/reseeded.
                        // Already-admitted ordinary run/append retain their raw
                        // origin/generation even when queued across interruption.
                        // Only their compound write is fenced from its start.
                        let wire_generation = if plan.strict_generation { command.generation }
                            else { control.borrow().generation };
                        for (index, message) in plan.messages.into_iter().enumerate() {
                            #[cfg(not(test))]
                            let _ = index;
                            #[cfg(test)]
                            if index == 1 && plan.atomic_transition {
                                selected_test_write_boundary(tag, &mut control, wire_generation).await?;
                            }
                            if plan.atomic_transition {
                                selected_write(&mut socket, message, &mut control,
                                    sender.config.limits.write_timeout, wire_generation).await?;
                            } else {
                                write(&mut socket, Message::Text(message.into()), &mut control,
                                    sender.config.limits.write_timeout).await?;
                            }
                        }
                        if plan.atomic_transition && wire_generation != sender.generation() {
                            return Err(GeminiError::Protocol("initial-history command generation changed"));
                        }
                        for action in plan.actions {
                            if !matches!(action, Action::NonBlockingToolResponses(_)) {
                                input_origin = command.origin.clone();
                            }
                            if matches!(action, Action::ActivityEnd | Action::Audio(_) | Action::AudioStreamEnd
                                | Action::RealtimeText(_) | Action::ClientContent { turn_complete: true, .. } | Action::ToolResponses(_)) {
                                busy_generation = command.generation;
                            }
                            match action {
                                Action::ActivityStart { preroll } => { activity = true; has_audio |= !preroll.is_empty(); }
                                Action::ActivityEnd => { activity = false; busy = true; }
                                Action::Audio(_) => { busy = true; has_audio = true; }
                                Action::AudioStreamEnd => { busy = true; has_audio = false; }
                                Action::RealtimeText(_) | Action::ClientContent { turn_complete: true, .. } => busy = true,
                                Action::ToolResponses(responses) => {
                                    for response in responses { pending.remove(&response.id); }
                                    busy = true;
                                }
                                Action::NonBlockingToolResponses(responses) => {
                                    if !busy && !output_active && !suppressed && !activity && !has_audio
                                        && generation == sender.generation() {
                                        if let Some(tool) = responses.iter()
                                            .find(|response| response.scheduling == ToolResponseScheduling::WhenIdle)
                                            .and_then(|response| pending.get(&response.id)) {
                                            input_origin = tool.origin.clone(); busy = true; busy_generation = generation;
                                        }
                                    }
                                    let mut admissions = sender.tool_admissions.lock().map_err(|_| GeminiError::Closed)?;
                                    for response in responses {
                                        if !response.will_continue { admissions.revoke(&response.id); pending.remove(&response.id); }
                                    }
                                }
                                _ => {}
                            }
                        }
                        continue;
                    }
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
                            if let Some(state) = &mut keepalive { state.last_audio = Instant::now(); }
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
fn emit(
    events: &mpsc::Sender<ReceivedEvent>,
    event: impl Into<ReceivedEvent>,
) -> Result<(), GeminiError> {
    events.try_send(event.into()).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("output"),
        mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
    })
}

#[cfg(all(test, feature = "paired-history"))]
#[path = "client_paired_history_tests.rs"]
mod paired_history_tests;

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use std::{task::Poll, time::Duration};
    use tokio::sync::oneshot;

    // Model the terminal cleanup interval: event delivery is closed, but the
    // connection task still owns an actual loopback WebSocket until its join.
    pub(super) async fn held_terminal_session(
    ) -> (GeminiSession, oneshot::Sender<()>, JoinHandle<()>) {
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
            selected: None,
        };
        let (running, running_ack) = oneshot::channel();
        let worker = tokio::spawn(async move {
            let _ = running.send(());
            let _ = release_rx.await;
            drop((socket, commands, control_rx));
            Err(GeminiError::Protocol("synthetic terminal failure"))
        });
        running_ack.await.unwrap();
        (
            GeminiSession {
                sender,
                events: events_rx,
                worker: Some(worker),
                joined: SelectedJoinObserver::default(),
                selected_terminal: SelectedTerminal::default(),
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
            selected: None,
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
                selected: None,
            },
            rx,
        )
    }

    #[test]
    fn usage_packet_envelope_takes_details_only_for_its_usage_event() {
        for (wire, expected) in [
            (
                serde_json::json!({"serverContent": {"generationComplete": true, "turnComplete": true},
                    "usageMetadata": {"promptTokenCount": 2, "responseTokenCount": 1,
                        "totalTokenCount": 9, "cachedContentTokenCount": 3}}),
                protocol::UsageDetails {
                    cache_read_input_tokens: Some(3),
                    ..Default::default()
                },
            ),
            (
                serde_json::json!({"usageMetadata": {"thoughtsTokenCount": 0}}),
                protocol::UsageDetails {
                    reasoning_tokens: Some(0),
                    ..Default::default()
                },
            ),
            (
                serde_json::json!({"usageMetadata": {"promptTokenCount": 2,
                    "responseTokenCount": 1, "totalTokenCount": 3}}),
                protocol::UsageDetails::default(),
            ),
        ] {
            let mut packet = protocol::parse(
                &serde_json::to_vec(&wire).unwrap(),
                &config("source-model"),
                None,
            )
            .unwrap();
            for event in packet.events {
                let is_usage = matches!(event, Event::Usage(_));
                let received = ReceivedEvent::from_packet(
                    GeminiEvent {
                        event,
                        origin: Origin::default(),
                        generation: 0,
                    },
                    &mut packet.usage_details,
                    #[cfg(feature = "paired-history")]
                    None,
                );
                assert_eq!(received.usage_details, is_usage.then_some(expected));
                assert_eq!(packet.usage_details, (!is_usage).then_some(expected));
            }
            assert!(packet.usage_details.is_none());
            let repeated = ReceivedEvent::from_packet(
                GeminiEvent {
                    event: Event::Usage(Usage::default()),
                    origin: Origin::default(),
                    generation: 0,
                },
                &mut packet.usage_details,
                #[cfg(feature = "paired-history")]
                None,
            );
            assert!(repeated.usage_details.is_none());
        }
    }

    #[tokio::test]
    async fn usage_internal_receive_preserves_filter_claim_cancel_and_public_projection() {
        for cancel_first in [false, true] {
            let (sender, _commands) = sender();
            let selected = tool("selected-with-usage", true);
            sender
                .tool_admissions
                .lock()
                .unwrap()
                .register(&[&selected], &HashSet::new(), &sender.config.limits)
                .unwrap();
            let (events, rx) = mpsc::channel(5);
            emit(
                &events,
                GeminiEvent {
                    event: Event::Text {
                        text: "stale".into(),
                        thought: false,
                    },
                    origin: Origin::default(),
                    generation: 0,
                },
            )
            .unwrap();
            emit(
                &events,
                GeminiEvent {
                    event: Event::NonBlockingToolCalls(vec![selected.call.clone()]),
                    origin: selected.origin.clone(),
                    generation: selected.generation,
                },
            )
            .unwrap();
            for count in [0, 5, 9] {
                emit(
                    &events,
                    ReceivedEvent {
                        event: GeminiEvent {
                            event: Event::Usage(Usage {
                                prompt_token_count: Some(1),
                                response_token_count: Some(2),
                                total_token_count: Some(count),
                            }),
                            origin: Origin::default(),
                            generation: 0,
                        },
                        usage_details: Some(protocol::UsageDetails {
                            reasoning_tokens: Some(count),
                            ..Default::default()
                        }),
                        #[cfg(feature = "paired-history")]
                        packet: None,
                    },
                )
                .unwrap();
            }
            assert_eq!(
                events.capacity(),
                0,
                "each event retains exactly one queue slot"
            );
            drop(events);
            sender.interrupt().await;
            if cancel_first {
                sender.cancel();
            }
            let mut session = GeminiSession {
                sender,
                events: rx,
                worker: None,
                joined: SelectedJoinObserver::default(),
                selected_terminal: SelectedTerminal::default(),
            };
            if !cancel_first {
                let received = session.next_event_with_details().await.unwrap().unwrap();
                assert!(matches!(
                    received.event.event,
                    Event::NonBlockingToolCalls(_)
                ));
                assert_eq!(received.event.origin, selected.origin);
                assert_eq!(received.event.generation, selected.generation);
                assert!(received.usage_details.is_none());
                assert_eq!(
                    session.sender.tool_admissions.lock().unwrap().ids[selected.call.id.as_str()],
                    CallAdmission::Claimed
                );
            }
            let first = session.next_event_with_details().await.unwrap().unwrap();
            assert!(matches!(first.event.event, Event::Usage(_)));
            assert_eq!(
                first.event.generation, 0,
                "usage survives output generation filtering"
            );
            assert_eq!(first.usage_details.unwrap().reasoning_tokens, Some(0));
            let public = session.next_event().await.unwrap().unwrap();
            assert!(matches!(
                public.event,
                Event::Usage(Usage {
                    total_token_count: Some(5),
                    ..
                })
            ));
            let last = session.next_event_with_details().await.unwrap().unwrap();
            assert_eq!(last.usage_details.unwrap().reasoning_tokens, Some(9));
            assert!(session.next_event_with_details().await.is_none());
            if cancel_first {
                assert_eq!(
                    session.sender.tool_admissions.lock().unwrap().ids[selected.call.id.as_str()],
                    CallAdmission::Available
                );
            }
        }
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
                configured_setup(&config, None, &HashSet::new(), false),
                protocol::setup(&config)
            );
            let client = GeminiClient::new(config.clone())
                .unwrap()
                .with_non_blocking_tools(vec!["background".into()])
                .unwrap();
            assert!(client.is_non_blocking_tool("background"));
            assert!(!client.is_non_blocking_tool("ordinary"));
            let setup = configured_setup(&config, None, &client.non_blocking_tools, false);
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
        let limits = Limits {
            max_pending_tools: 1,
            ..Limits::default()
        };
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
                .try_send(
                    GeminiEvent {
                        event: Event::NonBlockingToolCalls(vec![t.call.clone()]),
                        origin: t.origin.clone(),
                        generation: t.generation,
                    }
                    .into(),
                )
                .unwrap();
            sender.interrupt().await;
            assert_ne!(sender.generation(), t.generation);
            if cancel_first {
                sender.tool_admissions.lock().unwrap().revoke("selected");
                events
                    .try_send(
                        GeminiEvent {
                            event: Event::NonBlockingToolCallsCancelled(vec![t.call]),
                            origin: t.origin.clone(),
                            generation: t.generation,
                        }
                        .into(),
                    )
                    .unwrap();
            }
            drop(events);
            let mut session = GeminiSession {
                sender,
                events: rx,
                worker: None,
                joined: SelectedJoinObserver::default(),
                selected_terminal: SelectedTerminal::default(),
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
            .try_send(
                GeminiEvent {
                    event: Event::NonBlockingToolCalls(vec![t.call]),
                    origin: t.origin,
                    generation: t.generation,
                }
                .into(),
            )
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
            joined: SelectedJoinObserver::default(),
            selected_terminal: SelectedTerminal::default(),
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

#[cfg(test)]
mod initial_history_tests {
    use super::*;
    use pipecat_core::{ChatMessage, ChatRole, LlmContextFrame};
    use std::{task::Poll, time::Duration};

    fn client(model: &str, inference: bool) -> GeminiClient {
        GeminiClient::new(GeminiConfig::new("synthetic-key", model))
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: inference,
            })
            .unwrap()
    }
    fn tag(run_id: u64, inference: bool) -> Arc<SelectedSessionTag> {
        Arc::new(SelectedSessionTag {
            run_id,
            policy: InitialHistoryPolicy {
                inference_on_initialization: inference,
            },
            model: protocol::InitialHistoryModel::Gemini3,
            closed: AtomicBool::new(false),
            write_boundary: std::sync::Mutex::new(None),
            validation_boundary: std::sync::Mutex::new(None),
            join_boundary: std::sync::Mutex::new(None),
            abort_join_pending: std::sync::Mutex::new(None),
        })
    }
    fn sender(
        client: &GeminiClient,
        run_id: u64,
        capacity: usize,
    ) -> (GeminiSender, mpsc::Receiver<Command>) {
        let (tx, rx) = mpsc::channel(capacity);
        let (control, _) = watch::channel(Control::default());
        let (policy, model) = client.initial_history.unwrap();
        (
            GeminiSender {
                config: client.config.clone(),
                tx,
                control,
                generation: Arc::new(AtomicU64::new(0)),
                finishing: Arc::new(AtomicBool::new(false)),
                gate: Arc::new(Mutex::new(())),
                admission: Arc::new(std::sync::Mutex::new(())),
                tool_admissions: Arc::new(std::sync::Mutex::new(ToolAdmissions::default())),
                non_blocking_tools: Arc::new(HashSet::new()),
                selected: Some(Arc::new(SelectedSessionTag {
                    run_id,
                    policy,
                    model,
                    closed: AtomicBool::new(false),
                    write_boundary: std::sync::Mutex::new(None),
                    validation_boundary: std::sync::Mutex::new(None),
                    join_boundary: std::sync::Mutex::new(None),
                    abort_join_pending: std::sync::Mutex::new(None),
                })),
            },
            rx,
        )
    }
    fn context(role: ChatRole, text: &str) -> LlmContextFrame {
        LlmContextFrame {
            messages: vec![ChatMessage::new(role, text)],
            ..Default::default()
        }
    }
    fn actions() -> Vec<Action> {
        vec![
            Action::Audio(Vec::new()),
            Action::VideoJpeg(Vec::new()),
            Action::RealtimeText(String::new()),
            Action::RealtimeText("x".repeat(2 << 20)),
            Action::ClientContent {
                turns: Vec::new(),
                turn_complete: false,
            },
            Action::ClientContent {
                turns: Vec::new(),
                turn_complete: true,
            },
            Action::ActivityStart { preroll: vec![1] },
            Action::ActivityEnd,
            Action::AudioStreamEnd,
            Action::ToolResponses(Vec::new()),
            Action::NonBlockingToolResponses(Vec::new()),
        ]
    }
    fn assert_sealed(sender: &GeminiSender) {
        for clone in [sender.clone(), sender.clone()] {
            for action in actions() {
                assert_eq!(
                    clone.try_send(action, Origin::default()),
                    Err(GeminiError::Unsupported(
                        "raw actions on an initial-history session"
                    ))
                );
            }
        }
    }
    #[test]
    fn complete_model_tokens_have_explicit_boundaries() {
        for (name, expected) in [
            (
                "gemini-2.5-flash-native-audio-preview-12-2025",
                protocol::InitialHistoryModel::Gemini25,
            ),
            (
                "models/gemini-3.8-live",
                protocol::InitialHistoryModel::Gemini3,
            ),
            ("gemini-3-live", protocol::InitialHistoryModel::Gemini3),
            (
                "gemini-live-3.12-preview",
                protocol::InitialHistoryModel::Gemini3,
            ),
        ] {
            assert_eq!(protocol::initial_history_model(name), Ok(expected));
        }
        for name in [
            "anything3",
            "other-gemini-3-live",
            "models/models/gemini-3-live",
            "gemini-2.0-live",
            "gemini-4.0-live",
            "gemini-3.8bad-live",
            "gemini-3.-live",
            "gemini-3.8.1-live",
            "gemini--3-live",
            "gemini-3-live-",
            "gemini-03-live",
            "gemini-3.08-live",
            "gemini-3_live",
            "gemini-3-live.preview",
        ] {
            assert!(
                protocol::initial_history_model(name).is_err(),
                "accepted {name}"
            );
        }
    }
    #[test]
    fn projection_validates_original_system_fields_and_transformed_limits() {
        let mut config = GeminiConfig::new("synthetic", "gemini-2.5-live");
        config.session.system_instruction = Some("system".into());
        let client = GeminiClient::new(config)
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        for role in [ChatRole::System, ChatRole::Developer] {
            let seed = client
                .plan_initial_history(&context(role, "system"))
                .unwrap();
            assert!(
                matches!(&seed.actions[0], Action::ClientContent { turns, turn_complete: false }
                if turns == &vec![TextTurn { role: Role::User, text: "system".into() }])
            );
        }
        let mut snapshot = LlmContextFrame {
            messages: vec![
                ChatMessage::new(ChatRole::System, "system"),
                ChatMessage::new(ChatRole::Assistant, "answer"),
            ],
            ..Default::default()
        };
        let seed = client.plan_initial_history(&snapshot).unwrap();
        assert!(seed.pending_first_stop());
        assert!(
            matches!(&seed.actions[0], Action::ClientContent { turns, .. }
            if turns.len() == 2 && turns[0].role == Role::Model && turns[1].text == " ")
        );
        snapshot.messages[0].tool_call_id = Some("hidden".into());
        assert!(client.plan_initial_history(&snapshot).is_err());
        snapshot.messages[0].tool_call_id = None;
        snapshot
            .messages
            .push(ChatMessage::new(ChatRole::Tool, "bad tail"));
        assert!(client.plan_initial_history(&snapshot).is_err());
        assert!(client
            .plan_initial_history(&context(ChatRole::User, ""))
            .is_err());
        assert!(client
            .plan_initial_history(&context(ChatRole::User, " "))
            .is_ok());
        let empty = client
            .plan_initial_history(&LlmContextFrame::default())
            .unwrap();
        assert!(
            empty.actions.is_empty() && empty.messages.is_empty() && !empty.pending_first_stop()
        );
        let thousand = LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::Assistant, "x"); 1024],
            ..Default::default()
        };
        assert!(matches!(
            client.plan_initial_history(&thousand),
            Err(GeminiError::TooLarge)
        ));
    }
    #[test]
    fn aggregate_wire_budget_includes_every_synthetic_message() {
        let mut config = GeminiConfig::new("synthetic", "gemini-3-live");
        config.limits.max_message_bytes = 256;
        config.limits.max_audio_bytes = 128;
        let actions = vec![Action::RealtimeText("\\\"".repeat(32)); 2];
        assert!(actions
            .iter()
            .all(|action| protocol::action_messages(action, &config).is_ok()));
        assert!(matches!(
            selected_messages(&actions, &config),
            Err(GeminiError::TooLarge)
        ));
    }
    #[tokio::test]
    async fn sealed_clone_admission_is_identity_generation_and_queue_atomic() {
        let client = client("gemini-3-live", true);
        let (sender, mut rx) = sender(&client, 7, 1);
        assert_sealed(&sender);
        let seed = || {
            client
                .plan_initial_history(&context(ChatRole::User, "seed"))
                .unwrap()
        };
        sender
            .try_send_selected(sender.plan_seed(7, seed(), Origin::default(), 0).unwrap())
            .unwrap();
        assert_sealed(&sender);
        assert!(matches!(
            sender.try_send_selected(
                sender
                    .plan_first_stop(7, false, true, Origin::default(), 0)
                    .unwrap()
            ),
            Err(GeminiError::Backpressure("input"))
        ));
        let admitted = rx.try_recv().unwrap().selected.unwrap();
        assert_eq!(admitted.messages.len(), 2);
        let (other, _) = sender_fixture(&client, 7);
        assert_eq!(
            other.try_send_selected(sender.plan_completion(7, Origin::default(), 0).unwrap()),
            Err(GeminiError::Closed)
        );
        assert!(sender.plan_completion(8, Origin::default(), 0).is_err());
        let stale = sender.plan_completion(7, Origin::default(), 0).unwrap();
        sender.interrupt().await;
        assert_eq!(sender.generation(), 1);
        assert_eq!(sender.try_send_selected(stale), Err(GeminiError::Closed));
        assert!(rx.try_recv().is_err());
        sender
            .try_send_selected(sender.plan_completion(7, Origin::default(), 1).unwrap())
            .unwrap();
        rx.try_recv().unwrap();
        sender.close_selected_admission(7).unwrap();
        assert_eq!(
            sender.try_send_selected(sender.plan_completion(7, Origin::default(), 1).unwrap()),
            Err(GeminiError::Closed)
        );
        sender.finish().await.unwrap();
        let finish = rx.try_recv().unwrap();
        assert!(finish.action.is_none() && finish.selected.is_none());
        assert_sealed(&sender);
        sender.cancel();
        assert!(sender.control.borrow().cancel);
    }
    fn sender_fixture(client: &GeminiClient, id: u64) -> (GeminiSender, mpsc::Receiver<Command>) {
        sender(client, id, 1)
    }

    #[tokio::test]
    async fn first_stop_is_one_retryable_bounded_queue_slot() {
        let mut config = GeminiConfig::new("synthetic", "gemini-2.5-live");
        config.session.activity_detection.disabled = true;
        let client = GeminiClient::new(config)
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let (sender, mut rx) = sender(&client, 1, 1);
        sender
            .try_send_selected(
                sender
                    .plan_activity_start(1, Vec::new(), Origin::default(), 0)
                    .unwrap(),
            )
            .unwrap();
        let stop = || {
            sender
                .plan_first_stop(1, true, true, Origin::default(), 0)
                .unwrap()
        };
        assert!(matches!(
            sender.try_send_selected(stop()),
            Err(GeminiError::Backpressure("input"))
        ));
        assert!(matches!(
            rx.try_recv().unwrap().selected.unwrap().actions.as_slice(),
            [Action::ActivityStart { .. }]
        ));
        sender.try_send_selected(stop()).unwrap();
        let stop = rx.try_recv().unwrap().selected.unwrap();
        assert!(matches!(
            stop.actions.as_slice(),
            [
                Action::ActivityEnd,
                Action::ClientContent {
                    turn_complete: true,
                    ..
                }
            ]
        ));
        assert_eq!(stop.messages.len(), 2);
    }

    async fn connected(
        inference: bool,
    ) -> (
        GeminiClient,
        GeminiSession,
        mpsc::UnboundedReceiver<serde_json::Value>,
        JoinHandle<()>,
    ) {
        connected_model("gemini-3-live", inference).await
    }
    async fn connected_model(
        model: &str,
        inference: bool,
    ) -> (
        GeminiClient,
        GeminiSession,
        mpsc::UnboundedReceiver<serde_json::Value>,
        JoinHandle<()>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("synthetic", model);
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let client = GeminiClient::new(config)
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: inference,
            })
            .unwrap();
        let (tx, rx) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            let setup = socket.next().await.unwrap().unwrap();
            let setup: serde_json::Value = serde_json::from_str(setup.to_text().unwrap()).unwrap();
            assert_eq!(
                setup["setup"]["historyConfig"]["initialHistoryInClientContent"],
                true
            );
            socket
                .send(Message::Text("{\"setupComplete\":{}}".into()))
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                match message {
                    Message::Text(text) => {
                        tx.send(serde_json::from_str(&text).unwrap()).unwrap();
                    }
                    Message::Close(_) => break,
                    _ => {}
                }
            }
        });
        let session = client.connect_owned(19).await.unwrap();
        (client, session, rx, peer)
    }
    #[tokio::test]
    async fn real_selected_senders_stay_sealed_and_private_audio_follows_seed() {
        for inference in [false, true] {
            let (client, mut session, mut wire, peer) = connected(inference).await;
            let sender = session.sender();
            assert_sealed(&sender);
            let seed = client
                .plan_initial_history(&context(ChatRole::User, "hello"))
                .unwrap();
            sender
                .try_send_selected(sender.plan_seed(19, seed, Origin::default(), 0).unwrap())
                .unwrap();
            let first = timeout(Duration::from_secs(2), wire.recv())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(first["clientContent"]["turnComplete"], inference);
            if inference {
                assert_eq!(wire.recv().await.unwrap()["realtimeInput"]["text"], " ");
            }
            assert_sealed(&session.sender());
            sender
                .try_send_selected(
                    sender
                        .plan_audio(19, vec![0, 0], Origin::default(), 0)
                        .unwrap(),
                )
                .unwrap();
            assert!(wire.recv().await.unwrap()["realtimeInput"]["audio"].is_object());
            let joined = session.selected_join_observer().unwrap();
            assert!(!joined.joined());
            session.selected_shutdown().await.unwrap();
            assert!(joined.joined());
            assert_sealed(&sender);
            peer.await.unwrap();
        }
    }
    #[tokio::test]
    async fn selected_empty_seed_is_zero_wire_ready_for_real_audio() {
        let (client, mut session, mut wire, peer) = connected(true).await;
        let sender = session.sender();
        let seed = client
            .plan_initial_history(&LlmContextFrame::default())
            .unwrap();
        sender
            .try_send_selected(sender.plan_seed(19, seed, Origin::default(), 0).unwrap())
            .unwrap();
        sender
            .try_send_selected(
                sender
                    .plan_audio(19, vec![0, 0], Origin::default(), 0)
                    .unwrap(),
            )
            .unwrap();
        assert!(wire.recv().await.unwrap()["realtimeInput"]["audio"].is_object());
        session.selected_shutdown().await.unwrap();
        peer.await.unwrap();
        assert!(wire.try_recv().is_err());
    }
    #[tokio::test]
    async fn cancelled_terminal_read_keeps_real_worker_and_failure() {
        let (mut session, release, peer) = super::ownership_tests::held_terminal_session().await;
        session.sender.selected = Some(tag(1, false));
        let observed = session.selected_join_observer().unwrap();
        {
            let read = session.next_event();
            tokio::pin!(read);
            assert!(matches!(futures_util::poll!(&mut read), Poll::Pending));
        }
        assert!(!observed.joined());
        assert!(session.worker.is_some());
        release.send(()).unwrap();
        assert_eq!(
            session.next_event().await,
            Some(Err(GeminiError::Protocol("synthetic terminal failure")))
        );
        assert!(observed.joined());
        assert_eq!(
            session.selected_shutdown().await,
            Err(GeminiError::Protocol("synthetic terminal failure"))
        );
        peer.await.unwrap();
    }
    #[tokio::test(flavor = "current_thread")]
    async fn cancelled_normal_and_abort_waiters_retain_handle_and_forced_outcome() {
        let (mut session, _release, peer) = super::ownership_tests::held_terminal_session().await;
        session.sender.selected = Some(tag(1, false));
        let limits = &mut Arc::make_mut(&mut session.sender.config).limits;
        limits.finish_timeout = Duration::from_millis(10);
        limits.write_timeout = Duration::from_millis(10);
        let observed = session.selected_join_observer().unwrap();
        {
            let shutdown = session.selected_shutdown();
            tokio::pin!(shutdown);
            assert!(matches!(futures_util::poll!(&mut shutdown), Poll::Pending));
        }
        let original_deadline = session.selected_terminal.deadline;
        assert!(original_deadline.is_some() && session.worker.is_some() && !observed.joined());
        // Observe the actual configured timeout and the real post-abort
        // JoinHandle Pending poll. A Pending timeout future alone says nothing
        // about whether Tokio's millisecond timer wheel has fired yet.
        let pending_abort_join = session.sender.test_notify_pending_abort_join();
        {
            let shutdown = session.selected_shutdown();
            tokio::pin!(shutdown);
            timeout(Duration::from_secs(1), async {
                tokio::select! { biased;
                    notified = pending_abort_join => notified.unwrap(),
                    result = &mut shutdown => panic!("shutdown completed before the required cancelled-abort-wait boundary: {result:?}"),
                }
            }).await.expect("actual aborted JoinHandle did not reach its pending wait");
        }
        assert_eq!(session.selected_terminal.deadline, original_deadline);
        assert!(session.selected_terminal.forced && session.worker.is_some() && !observed.joined());
        assert_eq!(
            session.selected_shutdown().await,
            Err(GeminiError::Timeout("selected shutdown"))
        );
        assert!(observed.joined() && session.worker.is_none());
        assert_eq!(
            session.selected_shutdown().await,
            Err(GeminiError::Timeout("selected shutdown"))
        );
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn interrupt_between_seed_writes_is_terminal_and_never_replays_nudge() {
        let (client, mut session, mut wire, peer) = connected(true).await;
        let sender = session.sender();
        let (entered_rx, _release_tx) = sender.test_pause_second_write();
        let seed = client
            .plan_initial_history(&context(ChatRole::User, "hello"))
            .unwrap();
        sender
            .try_send_selected(sender.plan_seed(19, seed, Origin::default(), 0).unwrap())
            .unwrap();
        timeout(Duration::from_secs(2), entered_rx)
            .await
            .unwrap()
            .unwrap();
        assert!(wire.recv().await.unwrap()["clientContent"].is_object());
        sender.interrupt().await;
        let result = timeout(Duration::from_secs(2), session.next_event())
            .await
            .unwrap();
        assert_eq!(
            result,
            Some(Err(GeminiError::Protocol(
                "initial-history command generation changed"
            )))
        );
        assert!(session.selected_join_observer().unwrap().joined());
        assert_eq!(
            session.selected_shutdown().await,
            Err(GeminiError::Protocol(
                "initial-history command generation changed"
            ))
        );
        peer.await.unwrap();
        assert!(wire.try_recv().is_err());
    }
    #[tokio::test]
    async fn queued_audio_and_compound_run_preserve_admitted_semantics_across_interrupt() {
        let (client, mut session, mut wire, peer) = connected(false).await;
        let sender = session.sender();
        let empty = client
            .plan_initial_history(&LlmContextFrame::default())
            .unwrap();
        sender
            .try_send_selected(sender.plan_seed(19, empty, Origin::default(), 0).unwrap())
            .unwrap();
        sender
            .try_send_selected(
                sender
                    .plan_audio(19, vec![0, 0], Origin::default(), 0)
                    .unwrap(),
            )
            .unwrap();
        sender
            .try_send_selected(sender.plan_completion(19, Origin::default(), 0).unwrap())
            .unwrap();
        // This current-thread poll completes before the socket actor runs. All
        // three plans remain queued under generation zero at this boundary.
        sender.interrupt().await;
        assert_eq!(sender.generation(), 1);
        assert!(timeout(Duration::from_secs(2), wire.recv())
            .await
            .unwrap()
            .unwrap()["realtimeInput"]["audio"]
            .is_object());
        assert_eq!(
            wire.recv().await.unwrap()["clientContent"]["turnComplete"],
            true
        );
        assert_eq!(wire.recv().await.unwrap()["realtimeInput"]["text"], " ");
        assert!(!session.selected_join_observer().unwrap().joined());
        session.selected_shutdown().await.unwrap();
        peer.await.unwrap();
    }
    #[tokio::test]
    async fn queued_seed_generation_change_is_terminal_without_any_seed_write() {
        let (client, mut session, mut wire, peer) = connected(false).await;
        let sender = session.sender();
        let seed = client
            .plan_initial_history(&context(ChatRole::User, "seed"))
            .unwrap();
        sender
            .try_send_selected(sender.plan_seed(19, seed, Origin::default(), 0).unwrap())
            .unwrap();
        sender.interrupt().await;
        assert_eq!(
            timeout(Duration::from_secs(2), session.next_event())
                .await
                .unwrap(),
            Some(Err(GeminiError::Protocol(
                "initial-history command generation changed"
            )))
        );
        assert!(session.selected_join_observer().unwrap().joined());
        peer.await.unwrap();
        assert!(wire.try_recv().is_err());
    }
    #[tokio::test]
    async fn finish_seals_input_but_claimed_tool_settlement_and_cancel_keep_their_contracts() {
        let mut config = GeminiConfig::new("synthetic", "gemini-3.8-live");
        config.session.tools = vec![FunctionDeclaration {
            name: "background".into(),
            description: "fixture".into(),
            parameters: serde_json::json!({}),
        }];
        let client = GeminiClient::new(config)
            .unwrap()
            .with_non_blocking_tools(vec!["background".into()])
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let (mut sender, mut rx) = sender(&client, 1, 2);
        sender.non_blocking_tools = client.non_blocking_tools.clone();
        sender
            .tool_admissions
            .lock()
            .unwrap()
            .ids
            .insert("call".into(), CallAdmission::Claimed);
        let held = sender.test_hold_queue().unwrap();
        assert!(matches!(
            sender.try_send_selected(sender.plan_completion(1, Origin::default(), 0).unwrap()),
            Err(GeminiError::Backpressure("input"))
        ));
        drop(held);
        sender.close_selected_admission(1).unwrap();
        sender.finish().await.unwrap();
        rx.try_recv().unwrap();
        sender.interrupt().await;
        let result = || {
            sender
                .plan_non_blocking_tool_responses(
                    1,
                    vec![NonBlockingFunctionResponse {
                        id: "call".into(),
                        name: "background".into(),
                        response: serde_json::json!({"ok": true}),
                        will_continue: false,
                        scheduling: ToolResponseScheduling::Silent,
                    }],
                    Origin::default(),
                    sender.generation(),
                )
                .unwrap()
        };
        sender.try_send_selected(result()).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().selected.unwrap().actions.as_slice(),
            [Action::NonBlockingToolResponses(_)]
        ));
        let blocking = || {
            sender
                .plan_tool_responses(
                    1,
                    vec![FunctionResponse {
                        id: "blocking-call".into(),
                        name: "background".into(),
                        response: serde_json::json!({"ok": true}),
                    }],
                    Origin::default(),
                    sender.generation(),
                )
                .unwrap()
        };
        // Blocking call identity is checked against the actor's live pending
        // map, exactly as on the raw path; input closure alone cannot preclude it.
        sender.try_send_selected(blocking()).unwrap();
        assert!(matches!(
            rx.try_recv().unwrap().selected.unwrap().actions.as_slice(),
            [Action::ToolResponses(_)]
        ));
        sender.cancel();
        assert_eq!(sender.try_send_selected(result()), Err(GeminiError::Closed));
        assert_eq!(
            sender.try_send_selected(blocking()),
            Err(GeminiError::Closed)
        );
    }
    #[tokio::test]
    async fn second_write_hold_proves_no_command_interleaving_and_cancel_reclaims_actor() {
        for cancel in [false, true] {
            let (client, mut session, mut wire, peer) = connected(true).await;
            let sender = session.sender();
            let (entered, release) = sender.test_pause_second_write();
            let seed = client
                .plan_initial_history(&context(ChatRole::User, "seed"))
                .unwrap();
            sender
                .try_send_selected(sender.plan_seed(19, seed, Origin::default(), 0).unwrap())
                .unwrap();
            timeout(Duration::from_secs(2), entered)
                .await
                .unwrap()
                .unwrap();
            assert!(wire.recv().await.unwrap()["clientContent"].is_object());
            sender
                .try_send_selected(
                    sender
                        .plan_audio(19, vec![0, 0], Origin::default(), 0)
                        .unwrap(),
                )
                .unwrap();
            assert!(wire.try_recv().is_err());
            if cancel {
                sender.cancel();
                session.selected_shutdown().await.unwrap();
                assert!(session.selected_join_observer().unwrap().joined());
                peer.await.unwrap();
                assert!(wire.try_recv().is_err());
            } else {
                release.send(()).unwrap();
                assert_eq!(wire.recv().await.unwrap()["realtimeInput"]["text"], " ");
                assert!(wire.recv().await.unwrap()["realtimeInput"]["audio"].is_object());
                session.selected_shutdown().await.unwrap();
                peer.await.unwrap();
            }
        }
    }
    #[tokio::test]
    async fn dequeued_blocking_result_cannot_cross_a_new_interrupt_generation() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("synthetic", "gemini-3.8-live");
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        config.session.tools = vec![FunctionDeclaration {
            name: "blocking".into(),
            description: "fixture".into(),
            parameters: serde_json::json!({}),
        }];
        let client = GeminiClient::new(config)
            .unwrap()
            .with_initial_history(InitialHistoryPolicy {
                inference_on_initialization: false,
            })
            .unwrap();
        let (release_call, call_ready) = tokio::sync::oneshot::channel();
        let (wire_tx, mut wire) = mpsc::unbounded_channel();
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            socket.next().await.unwrap().unwrap();
            socket
                .send(Message::Text("{\"setupComplete\":{}}".into()))
                .await
                .unwrap();
            call_ready.await.unwrap();
            socket
                .send(Message::Text(
                    serde_json::json!({"toolCall": {"functionCalls": [
                        {"id": "call", "name": "blocking", "args": {}}
                    ]}})
                    .to_string()
                    .into(),
                ))
                .await
                .unwrap();
            while let Some(Ok(message)) = socket.next().await {
                if let Message::Text(text) = message {
                    wire_tx.send(text).unwrap();
                }
            }
        });
        let mut session = client.connect_owned(29).await.unwrap();
        let sender = session.sender();
        let seed = client
            .plan_initial_history(&LlmContextFrame::default())
            .unwrap();
        sender
            .try_send_selected(sender.plan_seed(29, seed, Origin::default(), 0).unwrap())
            .unwrap();
        release_call.send(()).unwrap();
        let call = timeout(Duration::from_secs(2), session.next_event())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(call.event, Event::ToolCalls(_)));
        let (entered, release) = sender.test_pause_before_validation();
        let response = vec![FunctionResponse {
            id: "call".into(),
            name: "blocking".into(),
            response: serde_json::json!({"ok": true}),
        }];
        sender
            .try_send_selected(
                sender
                    .plan_tool_responses(29, response, Origin::default(), 0)
                    .unwrap(),
            )
            .unwrap();
        timeout(Duration::from_secs(2), entered)
            .await
            .unwrap()
            .unwrap();
        sender.interrupt().await;
        release.send(()).unwrap();
        assert_eq!(
            timeout(Duration::from_secs(2), session.next_event())
                .await
                .unwrap(),
            Some(Err(GeminiError::Invalid(
                "unknown, canceled, or mismatched tool result"
            )))
        );
        assert!(session.selected_join_observer().unwrap().joined());
        peer.await.unwrap();
        assert!(wire.try_recv().is_err());
    }
    #[tokio::test]
    async fn selected_video_captured_generation_race_is_suppressed_without_resampling() {
        let client = client("gemini-3-live", false);
        let (sender, mut rx) = sender(&client, 1, 1);
        let stale = sender
            .plan_video(1, vec![255, 216, 255, 217], Origin::default(), 0)
            .unwrap();
        sender.interrupt().await;
        assert!(sender.try_send_selected(stale).is_ok());
        assert!(rx.try_recv().is_err());
        sender
            .try_send_selected(
                sender
                    .plan_video(1, vec![255, 216, 255, 217], Origin::default(), 1)
                    .unwrap(),
            )
            .unwrap();
        assert_eq!(rx.try_recv().unwrap().generation, 1);
    }
    #[tokio::test]
    async fn queued_first_stop_generation_change_is_terminal_without_partial_completion() {
        let (client, mut session, mut wire, peer) = connected_model("gemini-2.5-live", false).await;
        let sender = session.sender();
        let seed = client
            .plan_initial_history(&context(ChatRole::User, "silent seed"))
            .unwrap();
        assert!(seed.pending_first_stop());
        sender
            .try_send_selected(sender.plan_seed(19, seed, Origin::default(), 0).unwrap())
            .unwrap();
        let seeded = timeout(Duration::from_secs(2), wire.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(seeded["clientContent"]["turnComplete"], false);
        sender
            .try_send_selected(
                sender
                    .plan_first_stop(19, false, true, Origin::default(), 0)
                    .unwrap(),
            )
            .unwrap();
        sender.interrupt().await;
        assert_eq!(
            timeout(Duration::from_secs(2), session.next_event())
                .await
                .unwrap(),
            Some(Err(GeminiError::Protocol(
                "initial-history command generation changed"
            )))
        );
        assert!(session.selected_join_observer().unwrap().joined());
        peer.await.unwrap();
        assert!(wire.try_recv().is_err());
    }
}

#[cfg(test)]
#[path = "stt_keepalive_tests.rs"]
mod stt_keepalive_tests;
