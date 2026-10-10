// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    common::{self, Socket},
    settings, CancellationToken, SonioxTtsSettings, StreamingError, StreamingLimits,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
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
use tokio::sync::{mpsc, watch, Notify};
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, PartialEq)]
pub struct SonioxTtsConfig {
    pub endpoint: String,
    pub api_key: String,
    pub sample_rate: u32,
    pub settings: SonioxTtsSettings,
    pub max_streams: usize,
    pub max_streams_per_connection: usize,
    pub max_text_bytes: usize,
    pub max_audio_bytes: usize,
    pub max_timestamp_characters: usize,
    pub limits: StreamingLimits,
}
impl fmt::Debug for SonioxTtsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SonioxTtsConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl SonioxTtsConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://tts-rt.soniox.com/tts-websocket".into(),
            api_key: api_key.into(),
            sample_rate: 24000,
            settings: SonioxTtsSettings::default(),
            max_streams: 5,
            max_streams_per_connection: 65536,
            max_text_bytes: 128 * 1024,
            max_audio_bytes: 64 * 1024 * 1024,
            max_timestamp_characters: 128 * 1024,
            limits: StreamingLimits::default(),
        }
    }
    pub fn validate(&self) -> Result<(), StreamingError> {
        self.limits.validate()?;
        self.settings.validate()?;
        common::endpoint(&self.endpoint)?;
        if self.api_key.is_empty()
            || self.api_key.len() > 8192
            || ![8000, 16000, 24000, 44100, 48000].contains(&self.sample_rate)
            || !(1..=5).contains(&self.max_streams)
            || !(1..=65536).contains(&self.max_streams_per_connection)
            || !(1..=1024 * 1024).contains(&self.max_text_bytes)
            || !(2..=256 * 1024 * 1024).contains(&self.max_audio_bytes)
            || !(1..=1024 * 1024).contains(&self.max_timestamp_characters)
        {
            return Err(StreamingError::Invalid("Soniox TTS format or limits"));
        }
        if self.message("pipecat-stream").to_string().len() > self.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    fn message(&self, id: &str) -> Value {
        let mut msg = json!({"api_key":self.api_key,"stream_id":id,"model":self.settings.model,"voice":self.settings.voice,"audio_format":"pcm_s16le","sample_rate":self.sample_rate,"return_timestamps":true});
        let obj = msg.as_object_mut().unwrap();
        if let Some(language) = &self.settings.language {
            obj.insert(
                "language".into(),
                settings::language(language, true)
                    .expect("validated language")
                    .into(),
            );
        }
        if let Some(speed) = self.settings.speed {
            obj.insert("speed".into(), json!(speed));
        }
        if let Some(reduce) = self.settings.reduce_silence {
            obj.insert("reduce_silence".into(), reduce.into());
        }
        if let Some(reference) = &self.settings.client_reference_id {
            obj.insert("client_reference_id".into(), reference.clone().into());
        }
        msg
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct WordTimestamp {
    pub word: String,
    pub start: f64,
    pub end: Option<f64>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum SonioxTtsEvent {
    Audio(AudioRawFrame),
    WordTimestamps {
        context_id: String,
        words: Vec<WordTimestamp>,
    },
    Done {
        context_id: String,
    },
}
struct SharedStream {
    cancel: CancellationToken,
    finished: Mutex<bool>,
    done: watch::Sender<Option<Result<(), StreamingError>>>,
    source_audio: AtomicBool,
}
struct Hub {
    tx: mpsc::Sender<Command>,
    cancel: CancellationToken,
    wake: Arc<Notify>,
    registry: Arc<Mutex<HashMap<String, Arc<SharedStream>>>>,
    used: Mutex<HashSet<String>>,
}
impl Drop for Hub {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
/// Clones and settings snapshots share a single connection. A socket failure
/// retires the client; construct a new client explicitly to accept new work.
#[derive(Clone)]
pub struct SonioxTtsClient {
    config: SonioxTtsConfig,
    hub: Arc<Mutex<Option<Arc<Hub>>>>,
}
impl fmt::Debug for SonioxTtsClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SonioxTtsClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl SonioxTtsClient {
    pub fn new(mut config: SonioxTtsConfig) -> Result<Self, StreamingError> {
        config.validate()?;
        config.settings.language = config
            .settings
            .language
            .as_deref()
            .map(|v| settings::language(v, true).expect("validated language"));
        Ok(Self {
            config,
            hub: Arc::new(Mutex::new(None)),
        })
    }
    pub fn config(&self) -> &SonioxTtsConfig {
        &self.config
    }
    pub fn with_settings(&self, settings: SonioxTtsSettings) -> Result<Self, StreamingError> {
        let mut config = self.config.clone();
        config.settings = settings;
        config.validate()?;
        config.settings.language = config
            .settings
            .language
            .as_deref()
            .map(|v| settings::language(v, true).expect("validated language"));
        Ok(Self {
            config,
            hub: self.hub.clone(),
        })
    }
    fn hub(&self) -> Result<Arc<Hub>, StreamingError> {
        let mut slot = self.hub.lock().map_err(|_| StreamingError::Closed)?;
        if let Some(hub) = slot.as_ref() {
            if hub.tx.is_closed() || hub.cancel.is_cancelled() {
                return Err(StreamingError::Closed);
            }
            return Ok(hub.clone());
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| StreamingError::Invalid("active Tokio runtime required"))?;
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let cancel = CancellationToken::new();
        let wake = Arc::new(Notify::new());
        let registry = Arc::new(Mutex::new(HashMap::new()));
        let hub = Arc::new(Hub {
            tx,
            cancel: cancel.clone(),
            wake: wake.clone(),
            registry: registry.clone(),
            used: Mutex::new(HashSet::new()),
        });
        let config = self.config.clone();
        handle.spawn(async move {
            let result = match common::cancelled(
                &cancel,
                common::connect(
                    common::request(&config.endpoint, &[]).expect("validated endpoint"),
                    &config.limits,
                ),
            )
            .await
            {
                Ok(mut socket) => {
                    let result =
                        common::cancelled(&cancel, run(&mut socket, rx, &registry, &wake, &config))
                            .await;
                    common::close(&mut socket, &config.limits).await;
                    result
                }
                Err(error) => Err(error),
            };
            let error = result.err().unwrap_or(StreamingError::Closed);
            if let Ok(mut entries) = registry.lock() {
                for (_, state) in entries.drain() {
                    state.done.send_replace(Some(Err(error.clone())));
                }
            }
        });
        *slot = Some(hub.clone());
        Ok(hub)
    }
    pub fn stream(&self, id: impl AsRef<str>) -> Result<SonioxTtsStream, StreamingError> {
        self.stream_with_cancel(id, CancellationToken::new())
    }
    pub fn stream_with_cancel(
        &self,
        id: impl AsRef<str>,
        cancel: CancellationToken,
    ) -> Result<SonioxTtsStream, StreamingError> {
        settings::name(id.as_ref())?;
        let id = id.as_ref().to_owned();
        if cancel.is_cancelled() {
            return Err(StreamingError::Cancelled);
        }
        let hub = self.hub()?;
        let mut registry = hub.registry.lock().map_err(|_| StreamingError::Closed)?;
        if registry.len() >= self.config.max_streams {
            return Err(StreamingError::Backpressure);
        }
        let mut used = hub.used.lock().map_err(|_| StreamingError::Closed)?;
        if used.contains(&id) {
            return Err(StreamingError::Invalid(
                "stream IDs cannot be reused on a connection",
            ));
        }
        if used.len() >= self.config.max_streams_per_connection {
            return Err(StreamingError::TooLarge);
        }
        let permit = hub.tx.try_reserve().map_err(queue_error)?;
        if self.config.message(&id).to_string().len() > self.config.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let (done, terminal) = watch::channel(None);
        let state = Arc::new(SharedStream {
            cancel,
            finished: Mutex::new(false),
            done,
            source_audio: AtomicBool::new(false),
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
        Ok(SonioxTtsStream {
            sender: SonioxTtsSender {
                hub,
                id,
                state,
                config: self.config.clone(),
            },
            rx,
            terminal,
            ended: false,
        })
    }
    /// Ends the shared connection and every stream without replaying text.
    pub fn cancel(&self) {
        if let Ok(slot) = self.hub.lock() {
            if let Some(hub) = slot.as_ref() {
                hub.cancel.cancel();
            }
        }
    }
}
enum Command {
    Open {
        id: String,
        config: Box<SonioxTtsConfig>,
        events: mpsc::Sender<SonioxTtsEvent>,
        shared: Arc<SharedStream>,
    },
    Text {
        id: String,
        text: String,
    },
    Finish {
        id: String,
    },
}
#[derive(Clone)]
pub struct SonioxTtsSender {
    hub: Arc<Hub>,
    id: String,
    state: Arc<SharedStream>,
    config: SonioxTtsConfig,
}
impl SonioxTtsSender {
    pub fn context_id(&self) -> &str {
        &self.id
    }
    fn command(&self, text: Option<&str>) -> Result<Command, StreamingError> {
        if let Some(text) = text {
            if text.is_empty() {
                return Err(StreamingError::Invalid("empty TTS text"));
            }
            const OVERHEAD: usize = r#"{"stream_id":,"text":,"text_end":false}"#.len();
            if text.len() > self.config.max_text_bytes
                || quoted_len(text)
                    .saturating_add(quoted_len(&self.id))
                    .saturating_add(OVERHEAD)
                    > self.config.limits.max_message_bytes
            {
                return Err(StreamingError::TooLarge);
            }
            Ok(Command::Text {
                id: self.id.clone(),
                text: text.to_owned(),
            })
        } else {
            Ok(Command::Finish {
                id: self.id.clone(),
            })
        }
    }
    async fn send(&self, text: Option<&str>) -> Result<(), StreamingError> {
        common::cancelled(&self.state.cancel, async {
            let permit =
                tokio::time::timeout(self.config.limits.write_timeout, self.hub.tx.reserve())
                    .await
                    .map_err(|_| StreamingError::Timeout("input queue"))?
                    .map_err(|_| StreamingError::Closed)?;
            let mut finished = self
                .state
                .finished
                .lock()
                .map_err(|_| StreamingError::Closed)?;
            self.admissible(*finished)?;
            let command = self.command(text)?;
            permit.send(command);
            *finished = text.is_none();
            Ok(())
        })
        .await
    }
    fn try_send(&self, text: Option<&str>) -> Result<(), StreamingError> {
        let mut finished = self
            .state
            .finished
            .lock()
            .map_err(|_| StreamingError::Closed)?;
        self.admissible(*finished)?;
        let permit = self.hub.tx.try_reserve().map_err(queue_error)?;
        permit.send(self.command(text)?);
        *finished = text.is_none();
        Ok(())
    }
    fn admissible(&self, finished: bool) -> Result<(), StreamingError> {
        if self.state.cancel.is_cancelled() || self.hub.cancel.is_cancelled() {
            Err(StreamingError::Cancelled)
        } else if finished || self.state.done.borrow().is_some() {
            Err(StreamingError::Closed)
        } else {
            Ok(())
        }
    }
    pub async fn send_text(&self, text: impl AsRef<str>) -> Result<(), StreamingError> {
        self.send(Some(text.as_ref())).await
    }
    pub fn try_send_text(&self, text: impl AsRef<str>) -> Result<(), StreamingError> {
        self.try_send(Some(text.as_ref()))
    }
    pub async fn finish(&self) -> Result<(), StreamingError> {
        self.send(None).await
    }
    pub fn try_finish(&self) -> Result<(), StreamingError> {
        self.try_send(None)
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
fn queue_error<T>(e: mpsc::error::TrySendError<T>) -> StreamingError {
    match e {
        mpsc::error::TrySendError::Full(_) => StreamingError::Backpressure,
        mpsc::error::TrySendError::Closed(_) => StreamingError::Closed,
    }
}
pub struct SonioxTtsStream {
    sender: SonioxTtsSender,
    rx: mpsc::Receiver<SonioxTtsEvent>,
    terminal: watch::Receiver<Option<Result<(), StreamingError>>>,
    ended: bool,
}
impl SonioxTtsStream {
    pub fn sender(&self) -> SonioxTtsSender {
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
    pub async fn next(&mut self) -> Option<Result<SonioxTtsEvent, StreamingError>> {
        if self.ended {
            return None;
        }
        loop {
            if self.sender.state.cancel.is_cancelled() {
                self.ended = true;
                self.rx.close();
                while self.rx.try_recv().is_ok() {}
                return Some(Err(StreamingError::Cancelled));
            }
            if let Some(result) = self.terminal.borrow().clone() {
                if result == Err(StreamingError::Cancelled) {
                    self.ended = true;
                    self.rx.close();
                    while self.rx.try_recv().is_ok() {}
                    return Some(Err(StreamingError::Cancelled));
                }
                // A provider failure cannot retract audio or timestamps already
                // admitted ahead of it. Cancellation alone discards that prefix.
                match self.rx.try_recv() {
                    Ok(event) => return Some(Ok(event)),
                    Err(_) => {
                        self.ended = true;
                        return result.err().map(Err);
                    }
                }
            }
            tokio::select! {biased;
                _=self.sender.state.cancel.cancelled()=>continue,
                result=self.terminal.changed()=>{if result.is_err(){self.ended=true;return Some(Err(StreamingError::Closed));}},
                event=self.rx.recv()=>{if let Some(event)=event{return Some(Ok(event));}},
            }
        }
    }
}
impl Drop for SonioxTtsStream {
    fn drop(&mut self) {
        self.sender.cancel();
    }
}
struct LiveStream {
    config: SonioxTtsConfig,
    events: mpsc::Sender<SonioxTtsEvent>,
    shared: Arc<SharedStream>,
    text_bytes: usize,
    audio_bytes: usize,
    timestamp_characters: usize,
    partial: String,
    partial_start: f64,
    last_timestamp: Option<f64>,
    pcm: Option<u8>,
    finished: bool,
    deadline: tokio::time::Instant,
}
impl LiveStream {
    fn admit(&self, events: Vec<SonioxTtsEvent>) -> Result<(), StreamingError> {
        if events.is_empty() {
            return Ok(());
        }
        let permits = self
            .events
            .try_reserve_many(events.len())
            .map_err(queue_error)?;
        for (permit, event) in permits.zip(events) {
            permit.send(event);
        }
        Ok(())
    }
    fn timestamps(&mut self, value: &Value) -> Result<Vec<WordTimestamp>, StreamingError> {
        let chars = value
            .get("characters")
            .and_then(Value::as_array)
            .ok_or(StreamingError::Protocol("TTS timestamp characters"))?;
        let starts = value
            .get("character_start_times_seconds")
            .and_then(Value::as_array)
            .ok_or(StreamingError::Protocol("TTS timestamp starts"))?;
        if chars.len() != starts.len() {
            return Err(StreamingError::Protocol("TTS timestamp lengths differ"));
        }
        self.timestamp_characters = self
            .timestamp_characters
            .checked_add(chars.len())
            .ok_or(StreamingError::TooLarge)?;
        if self.timestamp_characters > self.config.max_timestamp_characters {
            return Err(StreamingError::TooLarge);
        }
        let cjk = self
            .config
            .settings
            .language
            .as_deref()
            .is_some_and(|l| l.starts_with("zh") || l.starts_with("ja"));
        let mut words = vec![];
        for (character, start) in chars.iter().zip(starts) {
            let character = character
                .as_str()
                .ok_or(StreamingError::Protocol("TTS timestamp character"))?;
            if character.chars().count() != 1 {
                return Err(StreamingError::Protocol(
                    "TTS timestamps require single characters",
                ));
            }
            let start = start
                .as_f64()
                .filter(|n| n.is_finite() && *n >= 0.0 && *n <= 86400.0)
                .ok_or(StreamingError::Protocol("TTS timestamp time"))?;
            if self.last_timestamp.is_some_and(|last| start < last) {
                return Err(StreamingError::Protocol("TTS timestamps moved backwards"));
            }
            self.last_timestamp = Some(start);
            if cjk {
                if character.chars().all(char::is_alphanumeric) {
                    words.push(WordTimestamp {
                        word: character.into(),
                        start,
                        end: None,
                    });
                }
            } else if character == " " {
                if !self.partial.is_empty() {
                    words.push(WordTimestamp {
                        word: std::mem::take(&mut self.partial),
                        start: self.partial_start,
                        end: None,
                    });
                }
            } else {
                if self.partial.is_empty() {
                    self.partial_start = start;
                }
                if self.partial.len().saturating_add(character.len()) > self.config.max_text_bytes {
                    return Err(StreamingError::TooLarge);
                }
                self.partial.push_str(character);
            }
        }
        Ok(words)
    }
    fn message(&mut self, id: &str, value: &Value) -> Result<bool, StreamingError> {
        if value.get("error_code").is_some_and(|v| !v.is_null()) {
            return Err(StreamingError::Provider {
                status: value
                    .get("error_code")
                    .and_then(Value::as_u64)
                    .and_then(|n| u16::try_from(n).ok()),
            });
        }
        for flag in ["terminated", "audio_end"] {
            if value.get(flag).is_some_and(|v| !v.is_boolean()) {
                return Err(StreamingError::Protocol(
                    "TTS terminal flags must be boolean",
                ));
            }
        }
        // At most two events are staged. A malformed packet cannot contribute
        // audio or words; earlier fully admitted packets remain observable.
        let mut pending = Vec::with_capacity(2);
        if value.get("terminated") == Some(&Value::Bool(true)) {
            if !self.finished {
                return Err(StreamingError::Protocol("TTS terminated before text_end"));
            }
            if self.pcm.is_some() {
                return Err(StreamingError::PartialSample);
            }
            if !self.partial.is_empty() {
                pending.push(SonioxTtsEvent::WordTimestamps {
                    context_id: id.into(),
                    words: vec![WordTimestamp {
                        word: self.partial.clone(),
                        start: self.partial_start,
                        end: None,
                    }],
                });
            }
            pending.push(SonioxTtsEvent::Done {
                context_id: id.into(),
            });
            self.admit(pending)?;
            return Ok(true);
        }
        let words = match value.get("timestamps") {
            Some(timestamps) => self.timestamps(timestamps)?,
            None => Vec::new(),
        };
        if let Some(audio) = value.get("audio") {
            let audio = audio
                .as_str()
                .ok_or(StreamingError::Protocol("TTS audio must be base64 text"))?;
            if !audio.len().is_multiple_of(4) {
                return Err(StreamingError::Protocol("TTS base64 length"));
            }
            let padding = audio
                .as_bytes()
                .iter()
                .rev()
                .take_while(|b| **b == b'=')
                .count();
            if padding > 2 {
                return Err(StreamingError::Protocol("TTS base64 padding"));
            }
            let decoded_len = (audio.len() / 4)
                .checked_mul(3)
                .and_then(|n| n.checked_sub(padding))
                .ok_or(StreamingError::Protocol("TTS base64 length"))?;
            if decoded_len > self.config.max_audio_bytes.saturating_sub(self.audio_bytes) {
                return Err(StreamingError::TooLarge);
            }
            let decoded = STANDARD
                .decode(audio)
                .map_err(|_| StreamingError::Protocol("TTS base64 audio"))?;
            if !decoded.is_empty() {
                self.shared.source_audio.store(true, Ordering::Release);
            }
            self.audio_bytes = self
                .audio_bytes
                .checked_add(decoded.len())
                .ok_or(StreamingError::TooLarge)?;
            if self.audio_bytes > self.config.max_audio_bytes {
                return Err(StreamingError::TooLarge);
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
                self.shared.source_audio.store(true, Ordering::Release);
                let mut frame = AudioRawFrame::new(pcm, self.config.sample_rate, 1)
                    .map_err(|_| StreamingError::PartialSample)?;
                frame.context_id = Some(id.into());
                pending.push(SonioxTtsEvent::Audio(frame));
            }
        }
        if !words.is_empty() {
            pending.push(SonioxTtsEvent::WordTimestamps {
                context_id: id.into(),
                words,
            });
        }
        self.admit(pending)?;
        Ok(false)
    }
}
fn retire(
    id: &str,
    live: &mut HashMap<String, LiveStream>,
    registry: &Mutex<HashMap<String, Arc<SharedStream>>>,
    result: Result<(), StreamingError>,
) {
    if let Some(stream) = live.remove(id) {
        stream.shared.done.send_replace(Some(result));
    }
    if let Ok(mut entries) = registry.lock() {
        entries.remove(id);
    }
}
async fn cancel_stream(
    socket: &mut Socket,
    id: &str,
    limits: &StreamingLimits,
) -> Result<(), StreamingError> {
    common::write(
        socket,
        Message::Text(json!({"stream_id":id,"cancel":true}).to_string().into()),
        limits,
    )
    .await
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    registry: &Mutex<HashMap<String, Arc<SharedStream>>>,
    wake: &Notify,
    config: &SonioxTtsConfig,
) -> Result<(), StreamingError> {
    let mut live: HashMap<String, LiveStream> = HashMap::new();
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(20),
        Duration::from_secs(20),
    );
    let mut check = tokio::time::interval(Duration::from_millis(10));
    loop {
        // Cancellation and response deadlines bypass full text and event queues.
        let expired: Vec<_> = live
            .iter()
            .filter_map(|(id, s)| {
                if s.shared.cancel.is_cancelled() {
                    Some((id.clone(), StreamingError::Cancelled))
                } else if tokio::time::Instant::now() >= s.deadline {
                    Some((id.clone(), StreamingError::Timeout("TTS response")))
                } else {
                    None
                }
            })
            .collect();
        for (id, error) in expired {
            let finished = live.get(&id).is_some_and(|s| s.finished);
            if !finished {
                cancel_stream(socket, &id, &config.limits).await?;
            }
            retire(&id, &mut live, registry, Err(error));
        }
        tokio::select! {
            _=wake.notified()=>(),
            _=check.tick()=>(),
            _=keepalive.tick()=>common::write(socket,Message::Text(json!({"keep_alive":true}).to_string().into()),&config.limits).await?,
            command=commands.recv()=>match command{
                Some(Command::Open{id,config:stream_config,events,shared})=>{
                    if shared.cancel.is_cancelled(){shared.done.send_replace(Some(Err(StreamingError::Cancelled)));if let Ok(mut r)=registry.lock(){r.remove(&id);}continue;}
                    common::write(socket,Message::Text(stream_config.message(&id).to_string().into()),&config.limits).await?;
                    live.insert(id,LiveStream{deadline:tokio::time::Instant::now()+stream_config.limits.response_timeout,config:*stream_config,events,shared,text_bytes:0,audio_bytes:0,timestamp_characters:0,partial:String::new(),partial_start:0.0,last_timestamp:None,pcm:None,finished:false});
                }
                Some(Command::Text{id,text})=>{if let Some(stream)=live.get_mut(&id){
                    if stream.shared.cancel.is_cancelled(){continue;}
                    stream.text_bytes=stream.text_bytes.saturating_add(text.len());
                    if stream.text_bytes>stream.config.max_text_bytes{cancel_stream(socket,&id,&config.limits).await?;retire(&id,&mut live,registry,Err(StreamingError::TooLarge));continue;}
                    common::write(socket,Message::Text(json!({"stream_id":id,"text":text,"text_end":false}).to_string().into()),&config.limits).await?;
                    stream.deadline=tokio::time::Instant::now()+config.limits.response_timeout;
                }}
                Some(Command::Finish{id})=>{if let Some(stream)=live.get_mut(&id){if stream.shared.cancel.is_cancelled(){continue;} common::write(socket,Message::Text(json!({"stream_id":id,"text":"","text_end":true}).to_string().into()),&config.limits).await?;stream.finished=true;stream.deadline=tokio::time::Instant::now()+config.limits.response_timeout;}}
                None=>return Ok(()),
            },
            message=socket.next()=>match message{
                Some(Ok(Message::Text(text)))=>{
                    let value:Value=serde_json::from_str(&text).map_err(|_|StreamingError::Protocol("TTS JSON"))?;
                    if !value.is_object(){return Err(StreamingError::Protocol("TTS message is not an object"));}
                    let Some(id)=value.get("stream_id").and_then(Value::as_str) else{if value.get("error_code").is_some(){return Err(StreamingError::Provider{status:None});}return Err(StreamingError::Protocol("TTS stream_id missing"));};
                    if let Some(stream)=live.get_mut(id){
                        if stream.shared.cancel.is_cancelled(){continue;}
                        if !stream.finished { stream.deadline=tokio::time::Instant::now()+config.limits.response_timeout; }
                        let result=stream.message(id,&value);
                        match result{Ok(false)=>(),Ok(true)=>retire(id,&mut live,registry,Ok(())),Err(error)=>{let finished=stream.finished;if !finished{cancel_stream(socket,id,&config.limits).await?;}retire(id,&mut live,registry,Err(error));}}
                    }
                    // Late events for retired stream IDs never reach another context.
                }
                Some(Ok(Message::Ping(data)))=>common::write(socket,Message::Pong(data),&config.limits).await?,
                Some(Ok(Message::Pong(_)))=>(),
                Some(Ok(Message::Close(_)))|None=>return Err(StreamingError::Closed),
                Some(Ok(_))=>return Err(StreamingError::Protocol("unexpected TTS binary message")),
                Some(Err(error))=>return Err(common::transport(error)),
            },
        }
    }
}
