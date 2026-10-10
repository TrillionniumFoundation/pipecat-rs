//! Cartesia Live STT (`services/cartesia/stt.py`), distinct from TTS and Turns.
use crate::common::{self, EventStream, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits};
use futures_util::StreamExt;
use pipecat_core::{AudioRawFrame, Setting, SttSettings};
use serde_json::Value;
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub const CARTESIA_STT_VERSION: &str = "2026-03-01";

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CartesiaSttSettings {
    pub model: String,
    pub language: String,
    pub keyterm: Vec<String>,
}
impl Default for CartesiaSttSettings {
    fn default() -> Self {
        Self {
            model: "ink-whisper".into(),
            language: "en".into(),
            keyterm: vec![],
        }
    }
}
/// Source-compatible normalization, including stopping at the first overflow.
/// `ignored_model` exposes the source's otherwise warning-only option omission.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CartesiaKeyterms {
    pub terms: Vec<String>,
    pub dropped: usize,
    pub ignored_model: bool,
}
impl CartesiaSttSettings {
    pub fn prepared_keyterms(&self) -> CartesiaKeyterms {
        let nonblank: Vec<_> = self
            .keyterm
            .iter()
            .map(|s| {
                s.trim_matches(|c: char| c.is_whitespace() || matches!(c, '\u{1c}'..='\u{1f}'))
            })
            .filter(|s| !s.is_empty())
            .collect();
        let mut terms = vec![];
        let mut chars = 0;
        for term in &nonblank {
            let count = term.chars().count();
            if terms.len() == 100 || chars + count > 1200 {
                break;
            }
            terms.push((*term).to_owned());
            chars += count;
        }
        let dropped = nonblank.len() - terms.len();
        let ignored_model = !terms.is_empty()
            && !(self.model.starts_with("ink-2") || self.model.starts_with("ink-preview"));
        if ignored_model {
            terms.clear();
        }
        CartesiaKeyterms {
            terms,
            dropped,
            ignored_model,
        }
    }
    pub fn merge(&self, delta: &SttSettings) -> Result<Self, StreamingError> {
        let mut values = delta.base.extra.clone();
        for (key, setting) in [("model", &delta.base.model), ("language", &delta.language)] {
            match setting {
                Setting::NotGiven => (),
                Setting::Null => {
                    values.insert(key.into(), Value::Null);
                }
                Setting::Value(value) => {
                    values.insert(key.into(), Value::String(value.clone()));
                }
            }
        }
        let mut next = self.clone();
        for (key, value) in values {
            match key.as_str() {
                "model" => {
                    next.model = value
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .ok_or(StreamingError::Invalid("Cartesia model"))?
                        .into()
                }
                "language" => {
                    next.language = value
                        .as_str()
                        .filter(|s| !s.is_empty())
                        .ok_or(StreamingError::Invalid("Cartesia language"))?
                        .into()
                }
                "keyterm" => {
                    next.keyterm = match value {
                        Value::Null => vec![],
                        Value::Array(values) => values
                            .into_iter()
                            .map(|v| {
                                v.as_str()
                                    .map(str::to_owned)
                                    .ok_or(StreamingError::Invalid("Cartesia keyterm"))
                            })
                            .collect::<Result<_, _>>()?,
                        _ => {
                            return Err(StreamingError::Invalid(
                                "Cartesia keyterm must be an array or null",
                            ))
                        }
                    }
                }
                _ => return Err(StreamingError::Invalid("unsupported Cartesia STT setting")),
            }
        }
        next.validate()?;
        Ok(next)
    }
    fn validate(&self) -> Result<(), StreamingError> {
        if self.model.is_empty() || self.language.is_empty() {
            return Err(StreamingError::Invalid("Cartesia model or language"));
        }
        if self.model.len() > 4096
            || self.language.len() > 256
            || self.keyterm.len() > 4096
            || self
                .keyterm
                .iter()
                .map(String::len)
                .try_fold(0usize, |a, b| a.checked_add(b))
                .is_none_or(|n| n > 65536)
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
}
#[derive(Clone, PartialEq)]
pub struct CartesiaSttConfig {
    /// Exact endpoint; loopback ws allowed for gateways/tests, otherwise wss.
    pub endpoint: String,
    pub api_key: String,
    pub sample_rate: u32,
    pub settings: CartesiaSttSettings,
    pub keepalive_timeout: Duration,
    pub keepalive_interval: Duration,
    pub limits: StreamingLimits,
}
impl fmt::Debug for CartesiaSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CartesiaSttConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl CartesiaSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.cartesia.ai/stt/websocket".into(),
            api_key: api_key.into(),
            sample_rate: 16000,
            settings: Default::default(),
            keepalive_timeout: Duration::from_secs(120),
            keepalive_interval: Duration::from_secs(30),
            limits: Default::default(),
        }
    }
    fn url(&self) -> Result<String, StreamingError> {
        self.limits.validate()?;
        self.settings.validate()?;
        if self.limits.queue_capacity > 1024
            || self.limits.max_message_bytes > 16 * 1024 * 1024
            || [
                self.limits.connect_timeout,
                self.limits.write_timeout,
                self.limits.response_timeout,
                self.limits.close_timeout,
            ]
            .into_iter()
            .any(|value| value > Duration::from_secs(300))
        {
            return Err(StreamingError::Invalid(
                "Cartesia limits exceed native maximum",
            ));
        }
        if self.api_key.is_empty()
            || !(8000..=192000).contains(&self.sample_rate)
            || self.keepalive_timeout.is_zero()
            || self.keepalive_timeout > Duration::from_secs(120)
            || self.keepalive_interval.is_zero()
            || self.keepalive_interval > Duration::from_secs(30)
        {
            return Err(StreamingError::Invalid(
                "Cartesia key, PCM rate or keepalive",
            ));
        }
        if self.endpoint.len() > 8192 || self.api_key.len() > 8192 {
            return Err(StreamingError::TooLarge);
        }
        if usize::try_from(self.sample_rate / 10)
            .unwrap_or(usize::MAX)
            .saturating_mul(2)
            > self.limits.max_message_bytes
        {
            return Err(StreamingError::Invalid(
                "message limit cannot fit 100ms keepalive",
            ));
        }
        let mut url = common::endpoint(&self.endpoint)?;
        let mut pairs = vec![
            ("model", self.settings.model.clone()),
            ("language", self.settings.language.clone()),
            ("encoding", "pcm_s16le".into()),
            ("sample_rate", self.sample_rate.to_string()),
        ];
        pairs.extend(
            self.settings
                .prepared_keyterms()
                .terms
                .into_iter()
                .map(|s| ("keyterm", s)),
        );
        // Python urlencode(..., quote_via=quote) uses RFC3986 unreserved bytes,
        // unlike form encoding (which emits '+' and leaves '*' unescaped).
        let query = pairs
            .into_iter()
            .map(|(k, v)| format!("{}={}", quote(k), quote(&v)))
            .collect::<Vec<_>>()
            .join("&");
        url.set_query(Some(&query));
        Ok(url.into())
    }
}
fn quote(value: &str) -> String {
    let mut out = String::new();
    for byte in value.bytes() {
        if byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.' | b'_' | b'~') {
            out.push(char::from(byte));
        } else {
            use std::fmt::Write;
            let _ = write!(out, "%{byte:02X}");
        }
    }
    out
}
#[derive(Debug, Clone, PartialEq)]
pub struct CartesiaSttTranscript {
    pub text: String,
    pub is_final: bool,
    pub language: Option<String>,
    pub result: Value,
    /// Successfully written user PCM since the preceding nonempty final.
    /// Silence keepalives are excluded. This is not a provider billing receipt.
    pub audio_seconds: f64,
}
#[derive(Debug, Clone, PartialEq)]
pub enum CartesiaSttEvent {
    Transcript(CartesiaSttTranscript),
    /// Unreported user PCM at a graceful finish, after any final transcripts.
    Usage {
        audio_seconds: f64,
    },
}
#[derive(Debug, Clone)]
pub struct CartesiaSttClient {
    config: CartesiaSttConfig,
}
impl CartesiaSttClient {
    pub fn new(config: CartesiaSttConfig) -> Result<Self, StreamingError> {
        common::request(
            &config.url()?,
            &[
                ("x-api-key", &config.api_key),
                ("cartesia-version", CARTESIA_STT_VERSION),
            ],
        )?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &CartesiaSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<CartesiaSttSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<CartesiaSttSession, StreamingError> {
        let request = common::request(
            &self.config.url()?,
            &[
                ("x-api-key", &self.config.api_key),
                ("cartesia-version", CARTESIA_STT_VERSION),
            ],
        )?;
        let mut socket =
            common::cancelled(&cancel, common::connect(request, &self.config.limits)).await?;
        let config = self.config.clone();
        let (tx, commands) = mpsc::channel(config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(config.limits.queue_capacity);
        let input = Input {
            tx,
            cancel: cancel.clone(),
            limits: config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let worker_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result =
                common::cancelled(&worker_cancel, run(&mut socket, commands, &events, &config))
                    .await;
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(CartesiaSttSession {
            sender: CartesiaSttSender {
                input,
                sample_rate: self.config.sample_rate,
            },
            events: EventStream {
                rx,
                cancel,
                task,
                ended: false,
            },
        })
    }
}
enum Command {
    Audio(Vec<u8>),
    Finalize,
    Finish,
}
#[derive(Clone)]
pub struct CartesiaSttSender {
    input: Input<Command>,
    sample_rate: u32,
}
impl CartesiaSttSender {
    fn validate(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        audio
            .validate()
            .map_err(|_| StreamingError::Invalid("complete PCM16 samples required"))?;
        if audio.sample_rate != self.sample_rate || audio.num_channels != 1 {
            return Err(StreamingError::Invalid("Cartesia PCM format"));
        }
        if audio.audio.len() > self.input.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub fn is_closed(&self) -> bool {
        self.input.cancel.is_cancelled()
            || self.input.tx.is_closed()
            || self
                .input
                .finished
                .lock()
                .map_or(true, |finished| *finished)
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate(&audio)?;
        self.input.send(Command::Audio(audio.audio), false).await
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate(&audio)?;
        self.input.try_send(Command::Audio(audio.audio), false)
    }
    pub async fn finalize(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finalize, false).await
    }
    pub fn try_finalize(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Finalize, false)
    }
    /// Send literal `done` after all admitted audio; await peer close with a deadline.
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
pub struct CartesiaSttSession {
    sender: CartesiaSttSender,
    events: EventStream<CartesiaSttEvent>,
}
impl CartesiaSttSession {
    pub fn sender(&self) -> CartesiaSttSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<CartesiaSttEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}
async fn emit(
    events: &mpsc::Sender<CartesiaSttEvent>,
    event: CartesiaSttEvent,
    deadline: Option<tokio::time::Instant>,
    config: &CartesiaSttConfig,
) -> Result<(), StreamingError> {
    let deadline =
        deadline.unwrap_or_else(|| tokio::time::Instant::now() + config.limits.write_timeout);
    tokio::time::timeout_at(deadline, events.send(event))
        .await
        .map_err(|_| StreamingError::Timeout("Cartesia output queue"))?
        .map_err(|_| StreamingError::Closed)
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<CartesiaSttEvent>,
    config: &CartesiaSttConfig,
) -> Result<(), StreamingError> {
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + config.keepalive_interval,
        config.keepalive_interval,
    );
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut last_audio = tokio::time::Instant::now();
    let mut closing = None;
    let mut pending_samples: u64 = 0;
    loop {
        let deadline = async {
            match closing {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = events.closed() => return Err(StreamingError::Closed),
            _ = deadline => return Err(StreamingError::Timeout("Cartesia final response")),
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(&text).map_err(|_| StreamingError::Protocol("Cartesia JSON"))?;
                    if let Some(event) = decode(value, &mut pending_samples, config.sample_rate)? { emit(events,event,closing,config).await?; }
                },
                Some(Ok(Message::Close(_))) | None => {
                    if closing.is_none() { return Err(StreamingError::Closed); }
                    if pending_samples > 0 { emit(events, CartesiaSttEvent::Usage { audio_seconds: pending_samples as f64 / f64::from(config.sample_rate) }, closing, config).await?; }
                    return Ok(());
                },
                Some(Ok(Message::Ping(bytes))) => common::write(socket, Message::Pong(bytes), &config.limits).await?,
                Some(Ok(Message::Pong(_))) => (),
                Some(Ok(_)) => return Err(StreamingError::Protocol("Cartesia sent non-text data")),
                Some(Err(error)) => return Err(common::transport(error)),
            },
            command = commands.recv(), if closing.is_none() => match command {
                Some(Command::Audio(audio)) => {
                    let samples = (audio.len() / 2) as u64;
                    common::write(socket, Message::Binary(audio.into()), &config.limits).await?;
                    pending_samples = pending_samples.checked_add(samples).ok_or(StreamingError::TooLarge)?;
                    last_audio = tokio::time::Instant::now();
                },
                Some(Command::Finalize) => common::write(socket, Message::Text("finalize".into()), &config.limits).await?,
                Some(Command::Finish) | None => {
                    common::write(socket, Message::Text("done".into()), &config.limits).await?;
                    commands.close(); closing = Some(tokio::time::Instant::now() + config.limits.response_timeout);
                },
            },
            _ = keepalive.tick(), if closing.is_none() => {
                if last_audio.elapsed() >= config.keepalive_timeout {
                    common::write(socket, Message::Binary(vec![0; (config.sample_rate / 10 * 2) as usize].into()), &config.limits).await?;
                    last_audio = tokio::time::Instant::now();
                }
            },
        }
    }
}
fn decode(
    value: Value,
    pending_samples: &mut u64,
    sample_rate: u32,
) -> Result<Option<CartesiaSttEvent>, StreamingError> {
    match value.get("type").and_then(Value::as_str) {
        Some("error") => Err(StreamingError::Provider {
            status: value
                .get("status_code")
                .and_then(Value::as_u64)
                .and_then(|n| u16::try_from(n).ok()),
        }),
        Some("transcript") => {
            let Some(text) = value.get("text") else {
                return Ok(None);
            };
            let text = text
                .as_str()
                .ok_or(StreamingError::Protocol("Cartesia transcript text"))?;
            let is_final = match value.get("is_final") {
                None => false,
                Some(v) => v
                    .as_bool()
                    .ok_or(StreamingError::Protocol("Cartesia is_final"))?,
            };
            if text.is_empty() {
                return Ok(None);
            }
            let language = match value.get("language") {
                None | Some(Value::Null) => None,
                Some(Value::String(s)) if s.len() <= 256 => Some(s.clone()),
                _ => return Err(StreamingError::Protocol("Cartesia language")),
            };
            let audio_seconds = if is_final {
                std::mem::take(pending_samples) as f64 / f64::from(sample_rate)
            } else {
                0.0
            };
            Ok(Some(CartesiaSttEvent::Transcript(CartesiaSttTranscript {
                text: text.into(),
                is_final,
                language,
                result: value,
                audio_seconds,
            })))
        }
        Some(_) => Ok(None),
        None => Err(StreamingError::Protocol("Cartesia message type")),
    }
}
