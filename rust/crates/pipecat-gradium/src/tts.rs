// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    client::{connect_socket, transport, Socket},
    protocol::validate_json,
    tts_settings, CancellationToken, GradiumError, GradiumLimits, GradiumTtsSettings,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use serde_json::{json, Value};
use std::{
    collections::{HashMap, HashSet},
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, watch, Notify, OwnedSemaphorePermit, Semaphore},
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::tungstenite::Message;

/// Native, source-pinned mono PCM16LE at 48 kHz. No implicit resampling.
#[derive(Clone, PartialEq, Eq)]
pub struct GradiumTtsConfig {
    pub endpoint: String,
    pub api_key: String,
    pub allow_loopback: bool,
    pub sample_rate: u32,
    pub settings: GradiumTtsSettings,
    pub max_streams: usize,
    /// Lifetime request identity budget, including cancelled/retired requests.
    pub max_streams_per_connection: usize,
    pub max_text_bytes: usize,
    pub max_audio_bytes: usize,
    pub max_timestamp_characters: usize,
    /// Absolute lifetime of one context, independent of traffic or finish.
    pub context_timeout: Duration,
    pub limits: GradiumLimits,
}
impl fmt::Debug for GradiumTtsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GradiumTtsConfig")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl GradiumTtsConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.gradium.ai/api/speech/tts".into(),
            api_key: api_key.into(),
            allow_loopback: false,
            sample_rate: 48000,
            settings: GradiumTtsSettings::default(),
            max_streams: 8,
            max_streams_per_connection: 1024,
            max_text_bytes: 64 * 1024,
            max_audio_bytes: 64 * 1024 * 1024,
            max_timestamp_characters: 64 * 1024,
            context_timeout: Duration::from_secs(120),
            limits: GradiumLimits::default(),
        }
    }
    pub fn validate(&self) -> Result<(), GradiumError> {
        self.limits.validate()?;
        self.settings.validate()?;
        crate::security::endpoint_url(&self.endpoint, self.allow_loopback)?;
        if self.api_key.is_empty()
            || self.api_key.len() > 4096
            || !self.api_key.bytes().all(|b| (0x21..=0x7e).contains(&b))
            || self.sample_rate != 48000
            || !(1..=64).contains(&self.max_streams)
            || !(1..=65536).contains(&self.max_streams_per_connection)
            || self.max_streams > self.max_streams_per_connection
            || !(1..=1024 * 1024).contains(&self.max_text_bytes)
            || !(2..=256 * 1024 * 1024).contains(&self.max_audio_bytes)
            || !(1..=1024 * 1024).contains(&self.max_timestamp_characters)
            || self.context_timeout.is_zero()
            || self.context_timeout > Duration::from_secs(300)
            || self
                .limits
                .queue_capacity
                .saturating_mul(self.max_text_bytes)
                > 16 * 1024 * 1024
            || self
                .max_streams
                .saturating_mul(self.limits.max_buffered_audio_bytes)
                > 64 * 1024 * 1024
            || self
                .max_streams
                .saturating_mul(self.limits.queue_capacity)
                .saturating_mul(self.max_text_bytes)
                > 64 * 1024 * 1024
        {
            return Err(GradiumError::Invalid("TTS format or limits"));
        }
        if self.setup_message("pipecat-stream").to_string().len() > self.limits.max_message_bytes {
            return Err(GradiumError::TooLarge);
        }
        Ok(())
    }
    pub fn setup_message(&self, id: &str) -> Value {
        let mut msg = json!({"type":"setup","voice_id":self.settings.voice,"output_format":"pcm","close_ws_on_eos":false,"client_req_id":id});
        if let Some(config) = &self.settings.json_config {
            msg["json_config"] = Value::String(config.clone());
        }
        msg
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct GradiumWordTimestamp {
    pub word: String,
    pub start: f64,
    pub end: Option<f64>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum GradiumTtsEvent {
    Audio(AudioRawFrame),
    WordTimestamps {
        context_id: String,
        words: Vec<GradiumWordTimestamp>,
    },
    Done {
        context_id: String,
    },
}
#[derive(Default)]
struct Admission {
    text_bytes: usize,
    text_messages: u64,
    finish_at: Option<Instant>,
}
struct SharedStream {
    cancel: CancellationToken,
    admission: Mutex<Admission>,
    done: watch::Sender<Option<Result<(), GradiumError>>>,
    source_audio: AtomicBool,
    created: Instant,
    read_at: Mutex<Instant>,
}
struct Hub {
    tx: mpsc::Sender<Command>,
    cancel: CancellationToken,
    wake: Arc<Notify>,
    registry: Arc<Mutex<HashMap<String, Arc<SharedStream>>>>,
    used: Mutex<HashSet<String>>,
    task: Mutex<Option<JoinHandle<()>>>,
    completion: watch::Sender<bool>,
}
// Captured before spawn, so panic or an unpolled abort also settles waiters.
struct ActorCompletion {
    registry: Arc<Mutex<HashMap<String, Arc<SharedStream>>>>,
    completion: watch::Sender<bool>,
}
impl Drop for ActorCompletion {
    fn drop(&mut self) {
        if let Ok(mut entries) = self.registry.lock() {
            for (_, state) in entries.drain() {
                state.done.send_replace(Some(Err(GradiumError::Closed)));
            }
        }
        self.completion.send_replace(true);
    }
}
impl Drop for Hub {
    fn drop(&mut self) {
        self.cancel.cancel();
        if let Ok(slot) = self.task.get_mut() {
            if let Some(task) = slot.take() {
                task.abort();
            }
        }
    }
}
/// Clones/settings snapshots share one owned WebSocket. Failure retires it;
/// construct a new client explicitly. Requests are never replayed.
#[derive(Clone)]
pub struct GradiumTtsClient {
    config: GradiumTtsConfig,
    hub: Arc<Mutex<Option<Arc<Hub>>>>,
}
impl fmt::Debug for GradiumTtsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GradiumTtsClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl GradiumTtsClient {
    pub fn new(config: GradiumTtsConfig) -> Result<Self, GradiumError> {
        config.validate()?;
        Ok(Self {
            config,
            hub: Arc::new(Mutex::new(None)),
        })
    }
    pub fn config(&self) -> &GradiumTtsConfig {
        &self.config
    }
    pub fn with_settings(&self, settings: GradiumTtsSettings) -> Result<Self, GradiumError> {
        let mut config = self.config.clone();
        config.settings = settings;
        config.validate()?;
        Ok(Self {
            config,
            hub: self.hub.clone(),
        })
    }
    fn hub(&self) -> Result<Arc<Hub>, GradiumError> {
        let mut slot = self.hub.lock().map_err(|_| GradiumError::Closed)?;
        if let Some(hub) = slot.as_ref() {
            if hub.tx.is_closed() || hub.cancel.is_cancelled() {
                return Err(GradiumError::Closed);
            }
            return Ok(hub.clone());
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| GradiumError::Invalid("active Tokio runtime required"))?;
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let cancel = CancellationToken::new();
        let wake = Arc::new(Notify::new());
        let registry = Arc::new(Mutex::new(HashMap::new()));
        let (completion, _) = watch::channel(false);
        let hub = Arc::new(Hub {
            tx,
            cancel: cancel.clone(),
            wake: wake.clone(),
            registry: registry.clone(),
            used: Mutex::new(HashSet::new()),
            task: Mutex::new(None),
            completion: completion.clone(),
        });
        let config = self.config.clone();
        let completion_guard = ActorCompletion {
            registry: registry.clone(),
            completion,
        };
        let task = handle.spawn(async move {
            let _completion_guard = completion_guard;
            let result = tokio::select! { biased;
                _ = cancel.cancelled() => Err(GradiumError::Cancelled),
                result = timeout(config.limits.session_timeout, async {
                    let mut socket = tokio::select! { biased;
                        error = operation_deadline(&registry, &config) => return Err(error),
                        result = timeout(config.limits.connect_timeout, connect_socket(&config.endpoint, &config.api_key, config.allow_loopback, &config.limits)) => result.map_err(|_| GradiumError::Timeout("TTS connect"))??,
                    };
                    let result = run(&mut socket, rx, &registry, &wake, &config).await;
                    if !matches!(result, Err(GradiumError::Timeout(_))) {
                        let _ = timeout(config.limits.close_timeout, socket.close(None)).await;
                    }
                    result
                }) => result.unwrap_or(Err(GradiumError::Timeout("TTS session"))),
            };
            let error = result.err().unwrap_or(GradiumError::Closed);
            if let Ok(mut entries) = registry.lock() {
                for (_, state) in entries.drain() { state.done.send_replace(Some(Err(error.clone()))); }
            }
        });
        *hub.task.lock().map_err(|_| GradiumError::Closed)? = Some(task);
        *slot = Some(hub.clone());
        Ok(hub)
    }
    pub fn stream(&self, id: impl AsRef<str>) -> Result<GradiumTtsStream, GradiumError> {
        self.stream_with_cancel(id, CancellationToken::new())
    }
    pub fn stream_with_cancel(
        &self,
        id: impl AsRef<str>,
        cancel: CancellationToken,
    ) -> Result<GradiumTtsStream, GradiumError> {
        tts_settings::name(id.as_ref())?;
        let id = id.as_ref().to_owned();
        if cancel.is_cancelled() {
            return Err(GradiumError::Cancelled);
        }
        let hub = self.hub()?;
        let mut registry = hub.registry.lock().map_err(|_| GradiumError::Closed)?;
        if registry.len() >= self.config.max_streams {
            return Err(GradiumError::Backpressure);
        }
        let mut used = hub.used.lock().map_err(|_| GradiumError::Closed)?;
        if used.contains(&id) {
            return Err(GradiumError::Invalid(
                "request IDs cannot be reused on a connection",
            ));
        }
        if used.len() >= self.config.max_streams_per_connection {
            return Err(GradiumError::TooLarge);
        }
        let permit = hub.tx.try_reserve().map_err(queue_error)?;
        if self.config.setup_message(&id).to_string().len() > self.config.limits.max_message_bytes {
            return Err(GradiumError::TooLarge);
        }
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (done, terminal) = watch::channel(None);
        let state = Arc::new(SharedStream {
            cancel,
            admission: Mutex::new(Admission::default()),
            done,
            source_audio: AtomicBool::new(false),
            created: Instant::now(),
            read_at: Mutex::new(Instant::now() + self.config.limits.read_timeout),
        });
        permit.send(Command::Open {
            id: id.clone(),
            config: Box::new(self.config.clone()),
            events: tx,
            shared: state.clone(),
        });
        registry.insert(id.clone(), state.clone());
        used.insert(id.clone());
        drop(used);
        drop(registry);
        Ok(GradiumTtsStream {
            sender: GradiumTtsSender {
                hub,
                id,
                state,
                config: self.config.clone(),
            },
            rx,
            terminal,
            ended: false,
            events_closed: false,
        })
    }
    pub fn cancel(&self) {
        if let Ok(slot) = self.hub.lock() {
            if let Some(hub) = slot.as_ref() {
                hub.cancel.cancel();
            }
        }
    }
    /// Cancels all requests and waits for the owned socket actor to exit.
    pub async fn shutdown(&self) {
        let hub = self.hub.lock().ok().and_then(|slot| slot.clone());
        if let Some(hub) = hub {
            hub.cancel.cancel();
            let mut done = hub.completion.subscribe();
            while !*done.borrow_and_update() {
                if done.changed().await.is_err() {
                    break;
                }
            }
            // Keep the handle owned by Hub across the await above: dropping a
            // shutdown future must never detach the actor.
            let task = hub.task.lock().ok().and_then(|mut task| task.take());
            if let Some(task) = task {
                let _ = task.await;
            }
        }
    }
}
enum Command {
    Open {
        id: String,
        config: Box<GradiumTtsConfig>,
        events: mpsc::Sender<Event>,
        shared: Arc<SharedStream>,
    },
    Text {
        id: String,
        text: String,
    },
}
struct Event {
    event: GradiumTtsEvent,
    _permit: Option<OwnedSemaphorePermit>,
}
#[derive(Clone)]
pub struct GradiumTtsSender {
    hub: Arc<Hub>,
    id: String,
    state: Arc<SharedStream>,
    config: GradiumTtsConfig,
}
impl GradiumTtsSender {
    pub fn context_id(&self) -> &str {
        &self.id
    }
    fn admissible(&self, admission: &Admission) -> Result<(), GradiumError> {
        if self.state.cancel.is_cancelled() || self.hub.cancel.is_cancelled() {
            Err(GradiumError::Cancelled)
        } else if admission.finish_at.is_some() || self.state.done.borrow().is_some() {
            Err(GradiumError::Closed)
        } else {
            Ok(())
        }
    }
    fn command(&self, text: &str, admission: &mut Admission) -> Result<Command, GradiumError> {
        self.admissible(admission)?;
        if text.is_empty() {
            return Err(GradiumError::Invalid("empty TTS text"));
        }
        let total = admission
            .text_bytes
            .checked_add(text.len())
            .ok_or(GradiumError::TooLarge)?;
        const OVERHEAD: usize = r#"{"type":"text","client_req_id":,"text":}"#.len();
        if total > self.config.max_text_bytes
            || quoted_len(text)
                .saturating_add(quoted_len(&self.id))
                .saturating_add(OVERHEAD)
                > self.config.limits.max_message_bytes
        {
            return Err(GradiumError::TooLarge);
        }
        let messages = admission
            .text_messages
            .checked_add(1)
            .ok_or(GradiumError::TooLarge)?;
        if messages > self.config.limits.max_session_messages {
            return Err(GradiumError::TooLarge);
        }
        admission.text_bytes = total;
        admission.text_messages = messages;
        Ok(Command::Text {
            id: self.id.clone(),
            text: text.into(),
        })
    }
    pub async fn send_text(&self, text: impl AsRef<str>) -> Result<(), GradiumError> {
        tokio::select! { biased;
            _ = self.state.cancel.cancelled() => Err(GradiumError::Cancelled),
            _ = self.hub.cancel.cancelled() => Err(GradiumError::Cancelled),
            result = async {
                let permit = timeout(self.config.limits.write_timeout, self.hub.tx.reserve()).await
                    .map_err(|_| GradiumError::Timeout("TTS input queue"))?.map_err(|_| GradiumError::Closed)?;
                let mut admission = self.state.admission.lock().map_err(|_| GradiumError::Closed)?;
                permit.send(self.command(text.as_ref(), &mut admission)?);
                Ok(())
            } => result,
        }
    }
    pub fn try_send_text(&self, text: impl AsRef<str>) -> Result<(), GradiumError> {
        let permit = self.hub.tx.try_reserve().map_err(queue_error)?;
        let mut admission = self
            .state
            .admission
            .lock()
            .map_err(|_| GradiumError::Closed)?;
        permit.send(self.command(text.as_ref(), &mut admission)?);
        Ok(())
    }
    /// Flush in the pinned TTS protocol means request EOS, not STT flush.
    pub async fn finish(&self) -> Result<(), GradiumError> {
        self.try_finish()
    }
    pub fn try_finish(&self) -> Result<(), GradiumError> {
        let mut admission = self
            .state
            .admission
            .lock()
            .map_err(|_| GradiumError::Closed)?;
        self.admissible(&admission)?;
        admission.finish_at = Some(Instant::now() + self.config.limits.finish_timeout);
        self.hub.wake.notify_one();
        Ok(())
    }
    pub fn cancel(&self) {
        self.state.cancel.cancel();
        self.hub.wake.notify_one();
    }
}
fn quoted_len(value: &str) -> usize {
    value.bytes().fold(2usize, |size, byte| {
        size.saturating_add(match byte {
            b'"' | b'\\' | b'\n' | b'\r' | b'\t' | 8 | 12 => 2,
            0..=31 => 6,
            _ => 1,
        })
    })
}
fn queue_error<T>(error: mpsc::error::TrySendError<T>) -> GradiumError {
    match error {
        mpsc::error::TrySendError::Full(_) => GradiumError::Backpressure,
        mpsc::error::TrySendError::Closed(_) => GradiumError::Closed,
    }
}
pub struct GradiumTtsStream {
    sender: GradiumTtsSender,
    rx: mpsc::Receiver<Event>,
    terminal: watch::Receiver<Option<Result<(), GradiumError>>>,
    ended: bool,
    events_closed: bool,
}
impl GradiumTtsStream {
    pub fn sender(&self) -> GradiumTtsSender {
        self.sender.clone()
    }
    pub fn context_id(&self) -> &str {
        self.sender.context_id()
    }
    pub fn source_audio_observed(&self) -> bool {
        self.sender.state.source_audio.load(Ordering::Acquire)
    }
    pub fn cancel(&self) {
        self.sender.cancel();
    }
    pub async fn next(&mut self) -> Option<Result<GradiumTtsEvent, GradiumError>> {
        if self.ended {
            return None;
        }
        loop {
            if self.sender.state.cancel.is_cancelled() || self.sender.hub.cancel.is_cancelled() {
                self.ended = true;
                self.rx.close();
                while self.rx.try_recv().is_ok() {}
                return Some(Err(GradiumError::Cancelled));
            }
            if let Some(result) = self.terminal.borrow().clone() {
                if result == Err(GradiumError::Cancelled) {
                    self.ended = true;
                    self.rx.close();
                    while self.rx.try_recv().is_ok() {}
                    return Some(Err(GradiumError::Cancelled));
                }
                match self.rx.try_recv() {
                    Ok(event) => {
                        self.ended = matches!(event.event, GradiumTtsEvent::Done { .. });
                        return Some(Ok(event.event));
                    }
                    Err(_) => {
                        self.ended = true;
                        return result.err().map(Err);
                    }
                }
            }
            tokio::select! { biased;
                _ = self.sender.state.cancel.cancelled() => continue,
                _ = self.sender.hub.cancel.cancelled() => continue,
                result = self.terminal.changed() => if result.is_err() { self.ended = true; return Some(Err(GradiumError::Closed)); },
                event = self.rx.recv(), if !self.events_closed => match event {
                    Some(event) => { self.ended = matches!(event.event, GradiumTtsEvent::Done { .. }); return Some(Ok(event.event)); },
                    None => self.events_closed = true,
                },
            }
        }
    }
}
impl Drop for GradiumTtsStream {
    fn drop(&mut self) {
        self.sender.cancel();
    }
}
struct LiveStream {
    config: GradiumTtsConfig,
    events: mpsc::Sender<Event>,
    shared: Arc<SharedStream>,
    budget: Arc<Semaphore>,
    audio_bytes: usize,
    timestamp_characters: usize,
    last_timestamp: Option<f64>,
    pcm: Option<u8>,
    finished: bool,
    ready: bool,
    sent_text: u64,
}
impl LiveStream {
    fn admit(&self, event: GradiumTtsEvent) -> Result<(), GradiumError> {
        let bytes = match &event {
            GradiumTtsEvent::Audio(audio) => audio.audio.len(),
            _ => 0,
        };
        let permit = if bytes > 0 {
            Some(
                self.budget
                    .clone()
                    .try_acquire_many_owned(
                        u32::try_from(bytes).map_err(|_| GradiumError::TooLarge)?,
                    )
                    .map_err(|_| GradiumError::Backpressure)?,
            )
        } else {
            None
        };
        self.events
            .try_send(Event {
                event,
                _permit: permit,
            })
            .map_err(queue_error)
    }
    fn absolute_deadline(&self) -> Result<Instant, GradiumError> {
        let admission = self
            .shared
            .admission
            .lock()
            .map_err(|_| GradiumError::Closed)?;
        let read_at = *self
            .shared
            .read_at
            .lock()
            .map_err(|_| GradiumError::Closed)?;
        Ok(admission
            .finish_at
            .unwrap_or(read_at)
            .min(self.shared.created + self.config.context_timeout))
    }
    fn message(&mut self, id: &str, value: &Value) -> Result<bool, GradiumError> {
        match value.get("type").and_then(Value::as_str) {
            Some("ready") => {
                if self.ready {
                    return Err(GradiumError::Protocol("duplicate TTS ready"));
                }
                if value
                    .get("sample_rate")
                    .is_some_and(|v| v.as_u64() != Some(u64::from(self.config.sample_rate)))
                    || value
                        .get("frame_size")
                        .is_some_and(|v| !v.as_u64().is_some_and(|n| (1..=192000).contains(&n)))
                    || value.get("audio_stream_names").is_some_and(|v| {
                        !v.as_array().is_some_and(|v| {
                            v.len() <= 1
                                && v.iter().all(|v| v.as_str().is_some_and(|s| s.len() <= 256))
                        })
                    })
                    || value.get("text_stream_names").is_some_and(|v| {
                        !v.as_array().is_some_and(|v| {
                            v.len() <= 1
                                && v.iter().all(|v| v.as_str().is_some_and(|s| s.len() <= 256))
                        })
                    })
                {
                    return Err(GradiumError::Protocol("TTS ready PCM geometry"));
                }
                self.ready = true;
            }
            Some("audio") => {
                if self.sent_text == 0 {
                    return Err(GradiumError::Protocol("TTS output before text"));
                }
                if value
                    .get("stream_id")
                    .is_some_and(|v| v.as_u64() != Some(0))
                {
                    return Err(GradiumError::Protocol(
                        "multiple TTS audio streams unsupported",
                    ));
                }
                let audio = value
                    .get("audio")
                    .and_then(Value::as_str)
                    .ok_or(GradiumError::Protocol("TTS base64 audio"))?;
                if !audio.len().is_multiple_of(4) {
                    return Err(GradiumError::Protocol("TTS base64 length"));
                }
                let padding = audio
                    .as_bytes()
                    .iter()
                    .rev()
                    .take_while(|b| **b == b'=')
                    .count();
                if padding > 2 {
                    return Err(GradiumError::Protocol("TTS base64 padding"));
                }
                let length = (audio.len() / 4)
                    .checked_mul(3)
                    .and_then(|n| n.checked_sub(padding))
                    .ok_or(GradiumError::Protocol("TTS base64 length"))?;
                if length > self.config.limits.max_audio_chunk_bytes
                    || length > self.config.max_audio_bytes.saturating_sub(self.audio_bytes)
                {
                    return Err(GradiumError::TooLarge);
                }
                let decoded = STANDARD
                    .decode(audio)
                    .map_err(|_| GradiumError::Protocol("TTS base64 audio"))?;
                self.audio_bytes += decoded.len();
                if !decoded.is_empty() {
                    self.shared.source_audio.store(true, Ordering::Release);
                }
                let mut pcm = Vec::with_capacity(decoded.len() + 1);
                if let Some(byte) = self.pcm.take() {
                    pcm.push(byte);
                }
                pcm.extend(decoded);
                if pcm.len() % 2 == 1 {
                    self.pcm = pcm.pop();
                }
                if !pcm.is_empty() {
                    let mut frame = AudioRawFrame::new(pcm, self.config.sample_rate, 1)
                        .map_err(|_| GradiumError::PartialSample)?;
                    frame.context_id = Some(id.into());
                    self.admit(GradiumTtsEvent::Audio(frame))?;
                }
            }
            Some("text") => {
                if self.sent_text == 0 {
                    return Err(GradiumError::Protocol("TTS output before text"));
                }
                if value
                    .get("stream_id")
                    .is_some_and(|v| v.as_u64() != Some(0))
                {
                    return Err(GradiumError::Protocol(
                        "multiple TTS text streams unsupported",
                    ));
                }
                let text = value
                    .get("text")
                    .and_then(Value::as_str)
                    .ok_or(GradiumError::Protocol("TTS timestamp text"))?;
                let start = value
                    .get("start_s")
                    .and_then(Value::as_f64)
                    .filter(|n| n.is_finite() && (0.0..=86400.0).contains(n))
                    .ok_or(GradiumError::Protocol("TTS timestamp time"))?;
                if text.is_empty() || text.len() > self.config.max_text_bytes {
                    return Err(GradiumError::Protocol("TTS timestamp text"));
                }
                self.timestamp_characters = self
                    .timestamp_characters
                    .checked_add(text.chars().count())
                    .ok_or(GradiumError::TooLarge)?;
                if self.timestamp_characters > self.config.max_timestamp_characters {
                    return Err(GradiumError::TooLarge);
                }
                if self.last_timestamp.is_some_and(|last| start < last) {
                    return Err(GradiumError::Protocol("TTS timestamps moved backwards"));
                }
                self.last_timestamp = Some(start);
                self.admit(GradiumTtsEvent::WordTimestamps {
                    context_id: id.into(),
                    words: vec![GradiumWordTimestamp {
                        word: text.into(),
                        start,
                        end: None,
                    }],
                })?;
            }
            Some("end_of_stream") => {
                if !self.finished {
                    return Err(GradiumError::Protocol("TTS ended before client EOS"));
                }
                if self.pcm.is_some() {
                    return Err(GradiumError::PartialSample);
                }
                self.admit(GradiumTtsEvent::Done {
                    context_id: id.into(),
                })?;
                return Ok(true);
            }
            Some("error") => return Err(GradiumError::Provider),
            _ => return Err(GradiumError::Protocol("unexpected TTS message type")),
        }
        Ok(false)
    }
}
fn retire(
    id: &str,
    live: &mut HashMap<String, LiveStream>,
    registry: &Mutex<HashMap<String, Arc<SharedStream>>>,
    result: Result<(), GradiumError>,
) {
    if let Some(stream) = live.remove(id) {
        stream.shared.done.send_replace(Some(result));
    }
    if let Ok(mut entries) = registry.lock() {
        entries.remove(id);
    }
}
async fn operation_deadline(
    registry: &Mutex<HashMap<String, Arc<SharedStream>>>,
    config: &GradiumTtsConfig,
) -> GradiumError {
    loop {
        let result = (|| -> Result<Instant, GradiumError> {
            let entries = registry.lock().map_err(|_| GradiumError::Closed)?;
            let mut at = Instant::now() + config.limits.session_timeout;
            for state in entries.values() {
                if state.cancel.is_cancelled() {
                    continue;
                }
                let admission = state.admission.lock().map_err(|_| GradiumError::Closed)?;
                let read_at = *state.read_at.lock().map_err(|_| GradiumError::Closed)?;
                at = at
                    .min(state.created + config.context_timeout)
                    .min(admission.finish_at.unwrap_or(read_at));
            }
            Ok(at)
        })();
        let earliest = match result {
            Ok(at) => at,
            Err(error) => return error,
        };
        if Instant::now() >= earliest {
            return GradiumError::Timeout("TTS operation");
        }
        tokio::time::sleep_until(earliest.min(Instant::now() + Duration::from_millis(10))).await;
    }
}
async fn wire(
    socket: &mut Socket,
    message: Message,
    config: &GradiumTtsConfig,
    registry: &Mutex<HashMap<String, Arc<SharedStream>>>,
    output_bytes: &mut u64,
) -> Result<(), GradiumError> {
    if message.len() > config.limits.max_message_bytes {
        return Err(GradiumError::TooLarge);
    }
    *output_bytes = output_bytes
        .checked_add(message.len() as u64)
        .ok_or(GradiumError::TooLarge)?;
    if *output_bytes > config.limits.max_session_input_bytes {
        return Err(GradiumError::TooLarge);
    }

    tokio::select! { biased;
        error = operation_deadline(registry, config) => Err(error),
        result = timeout(config.limits.write_timeout, socket.send(message)) => result.map_err(|_| GradiumError::Timeout("TTS write"))?.map_err(transport),
    }
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    registry: &Mutex<HashMap<String, Arc<SharedStream>>>,
    wake: &Notify,
    config: &GradiumTtsConfig,
) -> Result<(), GradiumError> {
    let mut live: HashMap<String, LiveStream> = HashMap::new();
    let mut check = tokio::time::interval(Duration::from_millis(10));
    let mut input_bytes = 0u64;
    let mut output_bytes = 0u64;
    let mut messages = 0u64;
    loop {
        let mut expired = vec![];
        let mut finishing = vec![];
        for (id, stream) in &live {
            if stream.shared.cancel.is_cancelled() {
                expired.push((id.clone(), GradiumError::Cancelled));
            } else if Instant::now() >= stream.absolute_deadline()? {
                expired.push((id.clone(), GradiumError::Timeout("TTS response")));
            } else if !stream.finished {
                let admission = stream
                    .shared
                    .admission
                    .lock()
                    .map_err(|_| GradiumError::Closed)?;
                if admission.finish_at.is_some() && stream.sent_text == admission.text_messages {
                    finishing.push(id.clone());
                }
            }
        }
        // There is no request-cancel wire message in the pinned protocol. IDs
        // stay permanently tombstoned; lifetime budgets bound abandoned work.
        for (id, error) in expired {
            retire(&id, &mut live, registry, Err(error));
        }
        for id in finishing {
            wire(
                socket,
                Message::Text(
                    json!({"type":"end_of_stream","client_req_id":id})
                        .to_string()
                        .into(),
                ),
                config,
                registry,
                &mut output_bytes,
            )
            .await?;
            if let Some(stream) = live.get_mut(&id) {
                stream.finished = true;
            }
        }
        tokio::select! {
            _ = wake.notified() => (),
            _ = check.tick() => (),
            command = commands.recv() => match command {
                Some(Command::Open { id, config: stream_config, events, shared }) => {
                    if shared.cancel.is_cancelled() {
                        shared.done.send_replace(Some(Err(GradiumError::Cancelled)));
                        if let Ok(mut entries) = registry.lock() { entries.remove(&id); }
                        continue;
                    }
                    wire(socket, Message::Text(stream_config.setup_message(&id).to_string().into()), config, registry, &mut output_bytes).await?;
                    live.insert(id, LiveStream {
                        budget: Arc::new(Semaphore::new(stream_config.limits.max_buffered_audio_bytes)),
                        config: *stream_config, events, shared, audio_bytes: 0, timestamp_characters: 0,
                        last_timestamp: None, pcm: None, finished: false, ready: false, sent_text: 0,
                    });
                }
                Some(Command::Text { id, text }) => {
                    if live.get(&id).is_some_and(|s| !s.shared.cancel.is_cancelled()) {
                        wire(socket, Message::Text(json!({"type":"text","client_req_id":id,"text":text}).to_string().into()), config, registry, &mut output_bytes).await?;
                        if let Some(stream) = live.get_mut(&id) {
                            stream.sent_text += 1;
                            *stream.shared.read_at.lock().map_err(|_| GradiumError::Closed)? = Instant::now() + config.limits.read_timeout;
                        }
                    }
                }
                None => return Ok(()),
            },
            message = socket.next() => {
                let message = match message { Some(Ok(message)) => message, Some(Err(error)) => return Err(transport(error)), None => return Err(GradiumError::Closed) };
                input_bytes = input_bytes.checked_add(message.len() as u64).ok_or(GradiumError::TooLarge)?;
                messages = messages.checked_add(1).ok_or(GradiumError::TooLarge)?;
                if input_bytes > config.limits.max_session_output_bytes || messages > config.limits.max_session_messages { return Err(GradiumError::TooLarge); }
                match message {
                    Message::Text(text) => {
                        let value: Value = serde_json::from_str(&text).map_err(|_| GradiumError::Protocol("TTS JSON"))?;
                        validate_json(&value, 0, &mut 0)?;
                        if !value.is_object() { return Err(GradiumError::Protocol("TTS message is not an object")); }
                        let Some(id) = value.get("client_req_id").and_then(Value::as_str) else {
                            return Err(if value.get("type").and_then(Value::as_str) == Some("error") { GradiumError::Provider } else { GradiumError::Protocol("TTS client_req_id missing") });
                        };
                        tts_settings::name(id).map_err(|_| GradiumError::Protocol("TTS client_req_id invalid"))?;
                        if let Some(stream) = live.get_mut(id) {
                            if stream.shared.cancel.is_cancelled() { continue; }
                            *stream.shared.read_at.lock().map_err(|_| GradiumError::Closed)? = Instant::now() + config.limits.read_timeout;
                            match stream.message(id, &value) {
                                Ok(false) => (), Ok(true) => retire(id, &mut live, registry, Ok(())),
                                Err(error) => retire(id, &mut live, registry, Err(error)),
                            }
                        }
                    }
                    Message::Ping(bytes) => wire(socket, Message::Pong(bytes), config, registry, &mut output_bytes).await?,
                    Message::Pong(_) => (),
                    Message::Close(_) => return Err(GradiumError::Closed),
                    _ => return Err(GradiumError::Protocol("unexpected TTS binary message")),
                }
            },
        }
    }
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    #[tokio::test]
    async fn actor_guard_settles_waiters_after_panic_or_unpolled_abort() {
        for panic in [false, true] {
            let (done, terminal) = watch::channel(None);
            let state = Arc::new(SharedStream {
                cancel: CancellationToken::new(),
                admission: Mutex::new(Admission::default()),
                done,
                source_audio: AtomicBool::new(false),
                created: Instant::now(),
                read_at: Mutex::new(Instant::now() + Duration::from_secs(1)),
            });
            let registry = Arc::new(Mutex::new(HashMap::from([("request".into(), state)])));
            let (completion, complete) = watch::channel(false);
            let guard = ActorCompletion {
                registry: registry.clone(),
                completion,
            };
            let task = tokio::spawn(async move {
                let _guard = guard;
                assert!(!panic, "synthetic actor failure");
                std::future::pending::<()>().await;
            });
            if !panic {
                task.abort();
            }
            assert!(timeout(Duration::from_secs(1), task)
                .await
                .unwrap()
                .is_err());
            assert_eq!(*terminal.borrow(), Some(Err(GradiumError::Closed)));
            assert!(*complete.borrow());
            assert!(registry.lock().unwrap().is_empty());
        }
    }
}
