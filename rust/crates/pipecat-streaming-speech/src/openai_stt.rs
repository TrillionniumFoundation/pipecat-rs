//! Transcription-only OpenAI WebSocket STT, from `services/openai/stt.py`.
//!
//! This deliberately small, immutable profile uses local VAD and 24 kHz mono
//! PCM16LE. It has no conversation responses, server VAD, tools or reconnect.
//! Sender success means bounded queue admission, not provider delivery.
use crate::common::{self, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use serde_json::{json, Value};
use std::{
    collections::HashMap,
    fmt,
    net::IpAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle, time::Instant};
use tokio_tungstenite::tungstenite::{Error as WsError, Message};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiSttNoiseReduction {
    NearField,
    FarField,
}
impl OpenAiSttNoiseReduction {
    fn wire(self) -> &'static str {
        match self {
            Self::NearField => "near_field",
            Self::FarField => "far_field",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenAiSttDelay {
    Minimal,
    Low,
    Medium,
    High,
    XHigh,
}
impl OpenAiSttDelay {
    fn wire(self) -> &'static str {
        match self {
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
            Self::XHigh => "xhigh",
        }
    }
}

#[derive(Clone)]
pub struct OpenAiSttConfig {
    /// Canonical OpenAI endpoint, or a loopback endpoint for synthetic tests.
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub language: Option<String>,
    /// Omitted for gpt-realtime-whisper, matching the source's omission.
    pub prompt: Option<String>,
    pub noise_reduction: Option<OpenAiSttNoiseReduction>,
    pub delay: Option<OpenAiSttDelay>,
    pub sample_rate: u32,
    pub channels: u16,
    pub limits: StreamingLimits,
    /// Covers both created and updated, without resetting on intermediate events.
    pub ready_timeout: Duration,
    pub max_audio_bytes: usize,
    /// Session-wide unique item-ID budget. IDs are never evicted/reused.
    pub max_tracked_items: usize,
}
impl fmt::Debug for OpenAiSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiSttConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("channels", &self.channels)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl OpenAiSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.openai.com/v1/realtime".into(),
            api_key: api_key.into(),
            model: "gpt-realtime-whisper".into(),
            language: Some("en".into()),
            prompt: None,
            noise_reduction: None,
            delay: None,
            sample_rate: 24000,
            channels: 1,
            limits: StreamingLimits::default(),
            ready_timeout: Duration::from_secs(10),
            max_audio_bytes: 64 * 1024,
            max_tracked_items: 1024,
        }
    }
    fn url(&self) -> Result<String, StreamingError> {
        self.limits.validate()?;
        if self.api_key.is_empty()
            || self.model.is_empty()
            || self.sample_rate != 24000
            || self.channels != 1
            || self.ready_timeout.is_zero()
            || self.max_audio_bytes == 0
            || self.max_tracked_items == 0
        {
            return Err(StreamingError::Invalid(
                "OpenAI STT configuration requires 24 kHz mono PCM",
            ));
        }
        if [
            self.ready_timeout,
            self.limits.connect_timeout,
            self.limits.write_timeout,
            self.limits.response_timeout,
            self.limits.close_timeout,
        ]
        .iter()
        .any(|duration| *duration > Duration::from_secs(24 * 60 * 60))
        {
            return Err(StreamingError::Invalid(
                "OpenAI STT deadlines must be at most 24 hours",
            ));
        }
        if self.endpoint.len() > 4096
            || self.api_key.len() > 16384
            || self.model.len() > 256
            || self.language.as_ref().is_some_and(|v| v.len() > 256)
            || self
                .prompt
                .as_ref()
                .is_some_and(|v| v.len() > self.limits.max_message_bytes)
            || self.limits.queue_capacity > 65536
            || self.max_tracked_items > 65536
            || self.limits.max_message_bytes > 16 * 1024 * 1024
            || self.max_audio_bytes > self.limits.max_message_bytes
            || self
                .limits
                .queue_capacity
                .checked_mul(self.limits.max_message_bytes)
                .is_none_or(|n| n > 64 * 1024 * 1024)
        {
            return Err(StreamingError::TooLarge);
        }
        let mut url = common::endpoint(&self.endpoint)?;
        let host = url.host_str().unwrap_or_default().trim_matches(['[', ']']);
        let loopback =
            host == "localhost" || host.parse::<IpAddr>().is_ok_and(|ip| ip.is_loopback());
        if !loopback
            && !(url.scheme() == "wss"
                && host == "api.openai.com"
                && url.path() == "/v1/realtime"
                && url.port().is_none())
        {
            return Err(StreamingError::Invalid(
                "OpenAI STT endpoint must be canonical or loopback",
            ));
        }
        url.query_pairs_mut().append_pair("intent", "transcription");
        if self.update().to_string().len() > self.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(url.into())
    }
    fn update(&self) -> Value {
        let mut transcription = json!({"model": self.model});
        if let Some(language) = &self.language {
            let code = language
                .split('-')
                .next()
                .unwrap_or_default()
                .to_lowercase();
            if !code.is_empty() {
                transcription["language"] = code.into();
            }
        }
        if self.model != "gpt-realtime-whisper" {
            if let Some(prompt) = self.prompt.as_ref().filter(|p| !p.is_empty()) {
                transcription["prompt"] = prompt.clone().into();
            }
        }
        if let Some(delay) = self.delay {
            transcription["delay"] = delay.wire().into();
        }
        let mut input = json!({"format":{"type":"audio/pcm","rate":24000}, "transcription":transcription, "turn_detection":null});
        if let Some(noise) = self.noise_reduction {
            input["noise_reduction"] = json!({"type":noise.wire()});
        }
        json!({"type":"session.update","session":{"type":"transcription","audio":{"input":input}}})
    }
}

/// Provider text and result are untrusted metadata, not pipeline turn authority.
/// The full result is retained only for these known bounded transcription events.
#[derive(Clone, PartialEq)]
pub enum OpenAiSttEvent {
    Delta {
        delta: String,
        item_id: String,
        content_index: u64,
        result: Value,
    },
    Completed {
        transcript: String,
        item_id: String,
        content_index: u64,
        result: Value,
    },
    Failed {
        item_id: String,
        content_index: u64,
        result: Value,
    },
}
impl fmt::Debug for OpenAiSttEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Do not accidentally log provider text, IDs or error bodies.
        f.write_str(match self {
            Self::Delta { .. } => "OpenAiSttEvent::Delta([redacted])",
            Self::Completed { .. } => "OpenAiSttEvent::Completed([redacted])",
            Self::Failed { .. } => "OpenAiSttEvent::Failed([redacted])",
        })
    }
}

#[derive(Debug, Clone)]
pub struct OpenAiSttClient {
    config: OpenAiSttConfig,
}
impl OpenAiSttClient {
    pub fn new(config: OpenAiSttConfig) -> Result<Self, StreamingError> {
        common::request(
            &config.url()?,
            &[("authorization", &format!("Bearer {}", config.api_key))],
        )?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &OpenAiSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<OpenAiSttSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<OpenAiSttSession, StreamingError> {
        let request = common::request(
            &self.config.url()?,
            &[("authorization", &format!("Bearer {}", self.config.api_key))],
        )?;
        let mut socket =
            common::cancelled(&cancel, common::connect(request, &self.config.limits)).await?;
        let config = self.config.clone();
        let (tx, commands) = mpsc::channel(config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(config.limits.queue_capacity);
        let sender = OpenAiSttSender {
            input: Input {
                tx,
                cancel: cancel.clone(),
                limits: config.limits.clone(),
                finished: Arc::new(Mutex::new(false)),
            },
            max_audio_bytes: config.max_audio_bytes,
        };
        let worker_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result =
                common::cancelled(&worker_cancel, run(&mut socket, commands, &events, &config))
                    .await;
            let closed = close_socket(&mut socket, &config.limits).await;
            result.and(closed)
        });
        Ok(OpenAiSttSession {
            sender,
            rx,
            cancel,
            task: Some(task),
            terminal: None,
            ended: false,
        })
    }
}

enum Command {
    Audio(Vec<u8>),
    Commit,
    Finish,
}
#[derive(Clone)]
pub struct OpenAiSttSender {
    input: Input<Command>,
    max_audio_bytes: usize,
}
impl OpenAiSttSender {
    fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        audio
            .validate()
            .map_err(|_| StreamingError::Invalid("OpenAI STT requires complete PCM16 samples"))?;
        if audio.sample_rate != 24000 || audio.num_channels != 1 {
            return Err(StreamingError::Invalid(
                "OpenAI STT requires 24 kHz mono PCM",
            ));
        }
        // Bound both raw data and its base64 JSON representation before enqueue.
        let encoded = audio
            .audio
            .len()
            .checked_add(2)
            .and_then(|n| n.checked_div(3))
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| n.checked_add(47));
        if audio.audio.len() > self.max_audio_bytes
            || encoded.is_none_or(|n| n > self.input.limits.max_message_bytes)
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        self.input.send(Command::Audio(audio.audio), false).await
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        self.input.try_send(Command::Audio(audio.audio), false)
    }
    pub async fn commit(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Commit, false).await
    }
    pub fn try_commit(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Commit, false)
    }
    /// Ordered after prior admitted commands. Does not commit buffered audio.
    pub async fn finish(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finish, true).await
    }
    pub fn try_finish(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Finish, true)
    }
    pub fn cancel(&self) {
        self.input.cancel.cancel();
    }
}

/// Owns the actor join handle. Use `shutdown().await` for joined cancellation.
/// Drop requests cancellation and bounded socket cleanup but cannot synchronously
/// join an async task; a dropped session's actor may briefly remain running.
pub struct OpenAiSttSession {
    sender: OpenAiSttSender,
    rx: mpsc::Receiver<OpenAiSttEvent>,
    cancel: CancellationToken,
    task: Option<JoinHandle<Result<(), StreamingError>>>,
    terminal: Option<Result<(), StreamingError>>,
    ended: bool,
}
impl OpenAiSttSession {
    pub fn sender(&self) -> OpenAiSttSender {
        self.sender.clone()
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    async fn join(&mut self) -> Result<(), StreamingError> {
        if let Some(task) = self.task.as_mut() {
            // Keep ownership in self across await: cancelling next/shutdown cannot
            // detach the actor. A later owner can resume joining the same handle.
            let result = task.await.unwrap_or(Err(StreamingError::Transport));
            self.task.take();
            self.terminal = Some(result);
        }
        self.terminal.clone().unwrap_or(Ok(()))
    }
    pub async fn next(&mut self) -> Option<Result<OpenAiSttEvent, StreamingError>> {
        if self.ended {
            return None;
        }
        tokio::select! { biased;
            _ = self.cancel.cancelled() => { self.rx.close(); while self.rx.try_recv().is_ok() {} }
            event = self.rx.recv() => { if let Some(event) = event { return Some(Ok(event)); } }
        }
        let result = self.join().await;
        self.ended = true;
        result.err().map(Err)
    }
    /// Cancel, discard queued events and join. Cancellation is returned as
    /// `StreamingError::Cancelled`; an already completed actor keeps its result.
    /// Dropping this future leaves the join handle inside this session.
    pub async fn shutdown(&mut self) -> Result<(), StreamingError> {
        self.cancel.cancel();
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
        let result = self.join().await;
        self.ended = true;
        result
    }
}
impl Drop for OpenAiSttSession {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

#[derive(Default)]
struct Item {
    acknowledged: bool,
    terminal: bool,
}
/// No ACK-order assumption: the server binds an outstanding anonymous commit to
/// an ID. A terminal observed before that ID's ACK is emitted but never earns
/// settlement credit. Without request-correlated ACKs, this conservative rule
/// prevents unknown text settling a future commit. Completion requires a new
/// terminal observed after the ACK; otherwise the finite drain times out.
struct Ledger {
    items: HashMap<String, Item>,
    unacknowledged: usize,
    pending: usize,
    commits: usize,
    limit: usize,
    deadline: Option<Instant>,
}
impl Ledger {
    fn new(limit: usize) -> Self {
        Self {
            items: HashMap::new(),
            unacknowledged: 0,
            pending: 0,
            commits: 0,
            limit,
            deadline: None,
        }
    }
    fn commit(&mut self, timeout: Duration) -> Result<(), StreamingError> {
        if self.commits == self.limit {
            return Err(StreamingError::TooLarge);
        }
        self.commits += 1;
        self.unacknowledged += 1;
        self.pending += 1;
        self.deadline
            .get_or_insert_with(|| Instant::now() + timeout);
        Ok(())
    }
    fn item(&mut self, id: &str) -> Result<&mut Item, StreamingError> {
        if !self.items.contains_key(id) && self.items.len() == self.limit {
            return Err(StreamingError::TooLarge);
        }
        Ok(self.items.entry(id.to_owned()).or_default())
    }
    fn acknowledge(&mut self, id: &str) -> Result<(), StreamingError> {
        if self.items.get(id).is_some_and(|item| item.acknowledged) {
            return Ok(());
        }
        if self.unacknowledged == 0 {
            return Err(StreamingError::Protocol(
                "OpenAI STT unsolicited commit acknowledgement",
            ));
        }
        let item = self.item(id)?;
        item.acknowledged = true;
        self.unacknowledged -= 1;
        Ok(())
    }
    fn terminal(&mut self, id: &str) -> Result<(), StreamingError> {
        let item = self.item(id)?;
        let release = item.acknowledged && !item.terminal;
        if item.acknowledged {
            item.terminal = true;
        }
        if release {
            self.pending -= 1;
        }
        if self.pending == 0 {
            self.deadline = None;
        }
        Ok(())
    }
}

async fn send_json(
    socket: &mut Socket,
    value: Value,
    limits: &StreamingLimits,
) -> Result<(), StreamingError> {
    let text = value.to_string();
    if text.len() > limits.max_message_bytes {
        return Err(StreamingError::TooLarge);
    }
    common::write(socket, Message::Text(text.into()), limits).await
}
async fn close_socket(socket: &mut Socket, limits: &StreamingLimits) -> Result<(), StreamingError> {
    tokio::time::timeout(limits.close_timeout, async {
        // run() may already have received the peer's Close. Flush tungstenite's
        // queued automatic reply before trying to initiate our own Close: send
        // rejects a new Close after receiving one and would leave it unflushed.
        match socket.flush().await {
            Ok(()) => (),
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => return Ok(()),
            Err(error) => return Err(common::transport(error)),
        }
        match socket.close(None).await {
            Ok(()) => (),
            Err(WsError::ConnectionClosed | WsError::AlreadyClosed) => return Ok(()),
            Err(error) => return Err(common::transport(error)),
        }
        loop {
            match socket.next().await {
                Some(Ok(Message::Close(_))) | None => return Ok(()),
                Some(Err(WsError::ConnectionClosed | WsError::AlreadyClosed)) => return Ok(()),
                Some(Err(error)) => return Err(common::transport(error)),
                _ => (),
            }
        }
    })
    .await
    .map_err(|_| StreamingError::Timeout("OpenAI STT close"))?
}
fn id(value: &Value, key: &str) -> Result<String, StreamingError> {
    let id = value
        .get(key)
        .and_then(Value::as_str)
        .filter(|id| !id.is_empty())
        .ok_or(StreamingError::Protocol("OpenAI STT item or session ID"))?;
    if id.len() > 512 {
        return Err(StreamingError::TooLarge);
    }
    Ok(id.into())
}
fn session_id(value: &Value) -> Result<String, StreamingError> {
    let session = value
        .get("session")
        .filter(|s| s.get("type").and_then(Value::as_str) == Some("transcription"))
        .ok_or(StreamingError::Protocol("OpenAI STT transcription session"))?;
    id(session, "id")
}
fn validate_updated(
    value: &Value,
    expected: &str,
    config: &OpenAiSttConfig,
) -> Result<(), StreamingError> {
    if session_id(value)? != expected {
        return Err(StreamingError::Protocol("OpenAI STT session ID mismatch"));
    }
    let input = value
        .pointer("/session/audio/input")
        .ok_or(StreamingError::Protocol("OpenAI STT input configuration"))?;
    if input.pointer("/format/type").and_then(Value::as_str) != Some("audio/pcm")
        || input.pointer("/format/rate").and_then(Value::as_u64) != Some(24000)
        || input.get("turn_detection") != Some(&Value::Null)
        || input
            .pointer("/transcription/model")
            .and_then(Value::as_str)
            != Some(config.model.as_str())
        || value.pointer("/session/audio/output").is_some()
    {
        return Err(StreamingError::Protocol(
            "OpenAI STT incompatible session acknowledgement",
        ));
    }
    Ok(())
}
fn transcription(value: Value) -> Result<OpenAiSttEvent, StreamingError> {
    let item_id = id(&value, "item_id")?;
    let content_index = value
        .get("content_index")
        .and_then(Value::as_u64)
        .ok_or(StreamingError::Protocol("OpenAI STT content index"))?;
    match value.get("type").and_then(Value::as_str) {
        Some("conversation.item.input_audio_transcription.delta") => {
            let delta = value
                .get("delta")
                .and_then(Value::as_str)
                .ok_or(StreamingError::Protocol("OpenAI STT delta"))?
                .into();
            Ok(OpenAiSttEvent::Delta {
                delta,
                item_id,
                content_index,
                result: value,
            })
        }
        Some("conversation.item.input_audio_transcription.completed") => {
            let transcript = value
                .get("transcript")
                .and_then(Value::as_str)
                .ok_or(StreamingError::Protocol("OpenAI STT transcript"))?
                .into();
            Ok(OpenAiSttEvent::Completed {
                transcript,
                item_id,
                content_index,
                result: value,
            })
        }
        Some("conversation.item.input_audio_transcription.failed") => {
            if !value.get("error").is_some_and(Value::is_object) {
                return Err(StreamingError::Protocol("OpenAI STT transcription error"));
            }
            Ok(OpenAiSttEvent::Failed {
                item_id,
                content_index,
                result: value,
            })
        }
        _ => Err(StreamingError::Protocol("OpenAI STT transcription event")),
    }
}

async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<OpenAiSttEvent>,
    config: &OpenAiSttConfig,
) -> Result<(), StreamingError> {
    let ready_deadline = Instant::now() + config.ready_timeout;
    let mut created: Option<String> = None;
    let mut ready = false;
    let mut finishing = None;
    let mut ledger = Ledger::new(config.max_tracked_items);
    loop {
        if finishing.is_some() && ledger.pending == 0 {
            return Ok(());
        }
        let deadline = if !ready {
            Some((ready_deadline, "OpenAI STT ready"))
        } else {
            match (ledger.deadline, finishing) {
                (Some(a), Some(b)) => Some((a.min(b), "OpenAI STT response")),
                (Some(a), None) | (None, Some(a)) => Some((a, "OpenAI STT response")),
                (None, None) => None,
            }
        };
        // Apply the absolute readiness/response deadline to the *entire* step,
        // including socket writes and a full event queue, not only socket reads.
        let step = async {
            tokio::select! {
                _ = events.closed() => Err(StreamingError::Closed),
                incoming = socket.next() => {
                    let value: Value = match incoming {
                        Some(Ok(Message::Text(text))) => serde_json::from_str(&text).map_err(|_| StreamingError::Protocol("OpenAI STT JSON"))?,
                        Some(Ok(Message::Ping(bytes))) => { common::write(socket, Message::Pong(bytes), &config.limits).await?; return Ok(()); }
                        Some(Ok(Message::Pong(_))) => return Ok(()),
                        Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Closed),
                        Some(Ok(_)) => return Err(StreamingError::Protocol("OpenAI STT requires text events")),
                        Some(Err(error)) => return Err(common::transport(error)),
                    };
                    match value.get("type").and_then(Value::as_str).ok_or(StreamingError::Protocol("OpenAI STT message type"))? {
                        "session.created" => {
                            if created.is_some() || ready { return Err(StreamingError::Protocol("OpenAI STT duplicate created")); }
                            created = Some(session_id(&value)?);
                            send_json(socket, config.update(), &config.limits).await?;
                        }
                        "session.updated" => {
                            let expected = created.as_deref().ok_or(StreamingError::Protocol("OpenAI STT updated before created"))?;
                            if ready { return Err(StreamingError::Protocol("OpenAI STT unsolicited update")); }
                            validate_updated(&value, expected, config)?;
                            ready = true;
                        }
                        "input_audio_buffer.committed" => {
                            if !ready { return Err(StreamingError::Protocol("OpenAI STT acknowledgement before ready")); }
                            ledger.acknowledge(&id(&value, "item_id")?)?;
                        }
                        "conversation.item.input_audio_transcription.delta"
                        | "conversation.item.input_audio_transcription.completed"
                        | "conversation.item.input_audio_transcription.failed" => {
                            if !ready { return Err(StreamingError::Protocol("OpenAI STT transcription before ready")); }
                            let event = transcription(value)?;
                            match &event {
                                OpenAiSttEvent::Completed { item_id, .. } | OpenAiSttEvent::Failed { item_id, .. } => ledger.terminal(item_id)?,
                                OpenAiSttEvent::Delta { .. } => (),
                            }
                            tokio::time::timeout(config.limits.write_timeout, events.send(event)).await
                                .map_err(|_| StreamingError::Timeout("OpenAI STT output queue"))?
                                .map_err(|_| StreamingError::Closed)?;
                        }
                        "error" => {
                            if !value.get("error").is_some_and(Value::is_object) { return Err(StreamingError::Protocol("OpenAI STT provider error")); }
                            return Err(StreamingError::Provider { status: None });
                        }
                        _ => (), // Unknown events have no retention or authority.
                    }
                    Ok(())
                }
                command = commands.recv(), if ready && finishing.is_none() => {
                    match command {
                        Some(Command::Audio(audio)) => {
                            if !audio.is_empty() { send_json(socket, json!({"type":"input_audio_buffer.append","audio":STANDARD.encode(audio)}), &config.limits).await?; }
                        }
                        Some(Command::Commit) => {
                            if ledger.commits == ledger.limit { return Err(StreamingError::TooLarge); }
                            send_json(socket, json!({"type":"input_audio_buffer.commit"}), &config.limits).await?;
                            ledger.commit(config.limits.response_timeout)?;
                        }
                        Some(Command::Finish) => { commands.close(); finishing = Some(Instant::now() + config.limits.response_timeout); }
                        None => return Err(StreamingError::Closed),
                    }
                    Ok(())
                }
            }
        };
        match deadline {
            Some((deadline, name)) => tokio::time::timeout_at(deadline, step)
                .await
                .map_err(|_| StreamingError::Timeout(name))??,
            None => step.await?,
        }
    }
}
