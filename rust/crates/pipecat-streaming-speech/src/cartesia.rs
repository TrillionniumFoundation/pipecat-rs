//! Cartesia's 2026-03-01 streaming protocol from `services/cartesia/tts.py`.
use crate::common::{self, EventStream, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
use pipecat_core::AudioRawFrame;
use serde_json::{json, Value};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub const CARTESIA_API_VERSION: &str = "2026-03-01";

#[derive(Clone, PartialEq)]
pub struct CartesiaConfig {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub voice_id: String,
    pub language: Option<String>,
    pub sample_rate: u32,
    pub add_timestamps: bool,
    pub max_buffer_delay_ms: Option<u32>,
    pub generation_config: Option<Value>,
    pub pronunciation_dict_id: Option<String>,
    pub max_text_bytes: usize,
    pub max_audio_bytes: usize,
    pub limits: StreamingLimits,
}
impl fmt::Debug for CartesiaConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CartesiaConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl CartesiaConfig {
    pub fn new(api_key: impl Into<String>, voice_id: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.cartesia.ai/tts/websocket".into(),
            api_key: api_key.into(),
            model: "sonic-3.6".into(),
            voice_id: voice_id.into(),
            language: Some("en".into()),
            sample_rate: 24000,
            add_timestamps: true,
            max_buffer_delay_ms: Some(0),
            generation_config: None,
            pronunciation_dict_id: None,
            max_text_bytes: 128 * 1024,
            max_audio_bytes: 64 * 1024 * 1024,
            limits: StreamingLimits::default(),
        }
    }
    fn validate(&self) -> Result<(), StreamingError> {
        self.limits.validate()?;
        common::endpoint(&self.endpoint)?;
        if self.api_key.is_empty()
            || self.model.is_empty()
            || self.voice_id.is_empty()
            || self.sample_rate == 0
            || self.max_text_bytes == 0
            || self.max_audio_bytes == 0
            || self.language.as_ref().is_some_and(String::is_empty)
            || self.max_buffer_delay_ms.is_some_and(|n| n > 5000)
            || self
                .generation_config
                .as_ref()
                .is_some_and(|v| !v.is_object())
        {
            return Err(StreamingError::Invalid(
                "Cartesia key, model, voice, audio format, language or limits",
            ));
        }
        Ok(())
    }
    fn message(&self, context_id: &str, text: &str, continuing: bool) -> Value {
        let mut message = json!({
            "transcript": text, "continue": continuing, "context_id": context_id,
            "model_id": self.model, "voice": {"mode":"id", "id": self.voice_id},
            "output_format": {"container":"raw", "encoding":"pcm_s16le", "sample_rate":self.sample_rate},
            "add_timestamps":self.add_timestamps, "use_normalized_timestamps":false,
        });
        if let Some(delay) = self.max_buffer_delay_ms {
            message["max_buffer_delay_ms"] = delay.into();
        }
        if let Some(language) = &self.language {
            message["language"] = language.clone().into();
        }
        if let Some(generation) = &self.generation_config {
            message["generation_config"] = generation.clone();
        }
        if let Some(dictionary) = &self.pronunciation_dict_id {
            message["pronunciation_dict_id"] = dictionary.clone().into();
        }
        message
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct WordTimestamp {
    pub word: String,
    /// Seconds from the beginning of this context's audio.
    pub start: f64,
    pub end: Option<f64>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum CartesiaEvent {
    Audio(AudioRawFrame),
    WordTimestamps {
        context_id: String,
        words: Vec<WordTimestamp>,
    },
    FlushDone {
        context_id: String,
        flush_id: Option<u64>,
    },
    Done {
        context_id: String,
    },
}

#[derive(Debug, Clone)]
pub struct CartesiaClient {
    config: CartesiaConfig,
}
impl CartesiaClient {
    pub fn new(config: CartesiaConfig) -> Result<Self, StreamingError> {
        config.validate()?;
        common::request(
            &config.endpoint,
            &[
                ("x-api-key", &config.api_key),
                ("cartesia-version", CARTESIA_API_VERSION),
            ],
        )?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &CartesiaConfig {
        &self.config
    }
    /// Opens an independently cancellable context. Text can be enqueued while
    /// its handshake is pending. A context never silently retries paid work.
    pub fn stream(&self, context_id: impl Into<String>) -> Result<CartesiaStream, StreamingError> {
        self.stream_with_cancel(context_id, CancellationToken::new())
    }
    pub fn stream_with_cancel(
        &self,
        context_id: impl Into<String>,
        cancel: CancellationToken,
    ) -> Result<CartesiaStream, StreamingError> {
        let context_id = context_id.into();
        if context_id.is_empty()
            || context_id.len() > 256
            || context_id.chars().any(char::is_control)
        {
            return Err(StreamingError::Invalid("Cartesia context ID"));
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| StreamingError::Invalid("active Tokio runtime required"))?;
        let request = common::request(
            &self.config.endpoint,
            &[
                ("x-api-key", &self.config.api_key),
                ("cartesia-version", CARTESIA_API_VERSION),
            ],
        )?;
        let (commands, incoming) = mpsc::channel(self.config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let input = Input {
            tx: commands,
            cancel: cancel.clone(),
            limits: self.config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let config = self.config.clone();
        let worker_cancel = cancel.clone();
        let worker_context = context_id.clone();
        let source_audio = Arc::new(AtomicBool::new(false));
        let observed = source_audio.clone();
        let task = handle.spawn(async move {
            let mut socket =
                common::cancelled(&worker_cancel, common::connect(request, &config.limits)).await?;
            let result = common::cancelled(
                &worker_cancel,
                run(
                    &mut socket,
                    incoming,
                    &events,
                    &config,
                    &worker_context,
                    &observed,
                ),
            )
            .await;
            if result.is_err() {
                let cancel_message = Message::Text(
                    json!({"context_id":worker_context,"cancel":true})
                        .to_string()
                        .into(),
                );
                let _ = tokio::time::timeout(
                    config.limits.close_timeout,
                    common::write(&mut socket, cancel_message, &config.limits),
                )
                .await;
            }
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(CartesiaStream {
            source_audio,
            sender: CartesiaSender {
                input,
                context_id,
                max_text_bytes: self.config.max_text_bytes,
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
    Text(String),
    Finish,
}
#[derive(Clone)]
pub struct CartesiaSender {
    input: Input<Command>,
    context_id: String,
    max_text_bytes: usize,
}
impl CartesiaSender {
    pub fn context_id(&self) -> &str {
        &self.context_id
    }
    fn text(&self, text: String) -> Result<Command, StreamingError> {
        if text.is_empty() {
            return Err(StreamingError::Invalid("empty Cartesia transcript"));
        }
        if text.len() > self.max_text_bytes || text.len() > self.input.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(Command::Text(text))
    }
    pub async fn send_text(&self, text: impl Into<String>) -> Result<(), StreamingError> {
        self.input.send(self.text(text.into())?, false).await
    }
    pub fn try_send_text(&self, text: impl Into<String>) -> Result<(), StreamingError> {
        self.input.try_send(self.text(text.into())?, false)
    }
    /// Closes the text side with continue=false; queued audio still drains.
    pub async fn finish(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finish, true).await
    }
    pub fn try_finish(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Finish, true)
    }
    /// Immediately suppresses buffered output, then sends the context cancel.
    pub fn cancel(&self) {
        self.input.cancel.cancel();
    }
}
pub struct CartesiaStream {
    source_audio: Arc<AtomicBool>,
    sender: CartesiaSender,
    events: EventStream<CartesiaEvent>,
}
impl CartesiaStream {
    pub(crate) fn source_audio_observed(&self) -> bool {
        self.source_audio.load(Ordering::Acquire)
    }
    pub fn sender(&self) -> CartesiaSender {
        self.sender.clone()
    }
    pub fn context_id(&self) -> &str {
        self.sender.context_id()
    }
    pub async fn next(&mut self) -> Option<Result<CartesiaEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}

async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<CartesiaEvent>,
    config: &CartesiaConfig,
    context_id: &str,
    source_audio: &AtomicBool,
) -> Result<(), StreamingError> {
    let mut decoder = Decoder {
        source_audio,
        config,
        context_id,
        trailing: None,
        total_audio: 0,
    };
    let mut deadline = tokio::time::Instant::now() + config.limits.response_timeout;
    let mut finished = false;
    let mut total_text = 0usize;
    loop {
        tokio::select! {
            _ = events.closed() => return Err(StreamingError::Closed),
            _ = tokio::time::sleep_until(deadline) => return Err(StreamingError::Timeout("Cartesia response")),
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(&text).map_err(|_| StreamingError::Protocol("Cartesia JSON"))?;
                    if let Some(event) = decoder.decode(value)? {
                        let done = matches!(event, CartesiaEvent::Done { .. });
                        if done && !finished { return Err(StreamingError::Protocol("Cartesia completed before text was finalized")); }
                        tokio::time::timeout_at(deadline, events.send(event)).await
                            .map_err(|_| StreamingError::Timeout("Cartesia output queue"))?
                            .map_err(|_| StreamingError::Closed)?;
                        if done { return Ok(()); }
                        if !finished { deadline = tokio::time::Instant::now() + config.limits.response_timeout; }
                    }
                }
                Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Protocol("Cartesia closed before done")),
                Some(Ok(Message::Ping(bytes))) => common::write(socket, Message::Pong(bytes), &config.limits).await?,
                Some(Ok(Message::Pong(_))) => {},
                Some(Ok(_)) => return Err(StreamingError::Protocol("Cartesia sent non-text data")),
                Some(Err(error)) => return Err(common::transport(error)),
            },
            command = commands.recv(), if !finished => {
                let (text, continuing) = match command {
                    Some(Command::Text(text)) => {
                        total_text = total_text.checked_add(text.len()).ok_or(StreamingError::TooLarge)?;
                        if total_text > config.max_text_bytes { return Err(StreamingError::TooLarge); }
                        (text, true)
                    }
                    Some(Command::Finish) | None => { finished = true; commands.close(); (String::new(), false) }
                };
                let message = config.message(context_id, &text, continuing).to_string();
                if message.len() > config.limits.max_message_bytes { return Err(StreamingError::TooLarge); }
                common::write(socket, Message::Text(message.into()), &config.limits).await?;
                deadline = tokio::time::Instant::now() + config.limits.response_timeout;
            },
        }
    }
}

struct Decoder<'a> {
    source_audio: &'a AtomicBool,
    config: &'a CartesiaConfig,
    context_id: &'a str,
    trailing: Option<u8>,
    total_audio: usize,
}
impl Decoder<'_> {
    fn decode(&mut self, value: Value) -> Result<Option<CartesiaEvent>, StreamingError> {
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .ok_or(StreamingError::Protocol("Cartesia message type"))?;
        let context = value.get("context_id").and_then(Value::as_str);
        if kind == "error" && context.is_none() {
            return Err(provider_error(&value));
        }
        let context = context.ok_or(StreamingError::Protocol("Cartesia context ID"))?;
        if context != self.context_id {
            return Ok(None);
        }
        match kind {
            "chunk" => {
                let encoded = value
                    .get("data")
                    .and_then(Value::as_str)
                    .ok_or(StreamingError::Protocol("Cartesia chunk data"))?;
                let decoded = STANDARD
                    .decode(encoded)
                    .map_err(|_| StreamingError::Protocol("Cartesia base64 audio"))?;
                self.total_audio = self
                    .total_audio
                    .checked_add(decoded.len())
                    .ok_or(StreamingError::TooLarge)?;
                if self.total_audio > self.config.max_audio_bytes {
                    return Err(StreamingError::TooLarge);
                }
                // Source Cartesia queues an explicit empty chunk as an audio
                // frame. This is health evidence, never playable PCM.
                if decoded.is_empty() && self.trailing.is_none() {
                    self.source_audio.store(true, Ordering::Release);
                }
                let mut audio = Vec::with_capacity(decoded.len() + 1);
                if let Some(byte) = self.trailing.take() {
                    audio.push(byte);
                }
                audio.extend_from_slice(&decoded);
                if audio.len() % 2 != 0 {
                    self.trailing = audio.pop();
                }
                if audio.is_empty() {
                    return Ok(None);
                }
                self.source_audio.store(true, Ordering::Release);
                Ok(Some(CartesiaEvent::Audio(AudioRawFrame {
                    audio,
                    sample_rate: self.config.sample_rate,
                    num_channels: 1,
                    context_id: Some(self.context_id.into()),
                })))
            }
            "timestamps" => {
                let timestamps = value
                    .get("word_timestamps")
                    .ok_or(StreamingError::Protocol("Cartesia word timestamps"))?;
                let words = timestamps
                    .get("words")
                    .and_then(Value::as_array)
                    .ok_or(StreamingError::Protocol("Cartesia words"))?;
                let starts = timestamps
                    .get("start")
                    .and_then(Value::as_array)
                    .ok_or(StreamingError::Protocol("Cartesia word starts"))?;
                let ends = timestamps.get("end").and_then(Value::as_array);
                if starts.len() != words.len() || ends.is_some_and(|ends| ends.len() != words.len())
                {
                    return Err(StreamingError::Protocol("Cartesia timestamp lengths"));
                }
                let mut result = Vec::with_capacity(words.len());
                for (index, word) in words.iter().enumerate() {
                    let word = word
                        .as_str()
                        .ok_or(StreamingError::Protocol("Cartesia word text"))?;
                    let start = time(&starts[index])?;
                    let end = ends.map(|ends| time(&ends[index])).transpose()?;
                    if end.is_some_and(|end| end < start) {
                        return Err(StreamingError::Protocol("Cartesia word end precedes start"));
                    }
                    let word = strip_tags(word);
                    if !word.is_empty() {
                        result.push(WordTimestamp { word, start, end });
                    }
                }
                Ok(Some(CartesiaEvent::WordTimestamps {
                    context_id: self.context_id.into(),
                    words: result,
                }))
            }
            "flush_done" => Ok(Some(CartesiaEvent::FlushDone {
                context_id: self.context_id.into(),
                flush_id: value.get("flush_id").and_then(Value::as_u64),
            })),
            "done" => {
                if self.trailing.is_some() {
                    return Err(StreamingError::PartialSample);
                }
                Ok(Some(CartesiaEvent::Done {
                    context_id: self.context_id.into(),
                }))
            }
            "error" => Err(provider_error(&value)),
            _ => Err(StreamingError::Protocol(
                "unsupported Cartesia message type",
            )),
        }
    }
}
fn provider_error(value: &Value) -> StreamingError {
    StreamingError::Provider {
        status: value
            .get("status_code")
            .and_then(Value::as_u64)
            .and_then(|n| u16::try_from(n).ok()),
    }
}
fn time(value: &Value) -> Result<f64, StreamingError> {
    value
        .as_f64()
        .filter(|n| n.is_finite() && *n >= 0.0 && *n < (u64::MAX as f64 / 1_000_000_000.0))
        .ok_or(StreamingError::Protocol("Cartesia word timestamp"))
}
/// Remove supported Cartesia SSML tags while retaining unknown literal text.
fn strip_tags(text: &str) -> String {
    let mut result = String::new();
    let mut remaining = text;
    while let Some(start) = remaining.find('<') {
        result.push_str(&remaining[..start]);
        let candidate = &remaining[start..];
        let Some(end) = candidate.find('>') else {
            result.push_str(candidate);
            return result.split_whitespace().collect::<Vec<_>>().join(" ");
        };
        let tag = candidate[1..end].trim_start_matches('/').trim();
        let name = tag.split([' ', '=', '/']).next().unwrap_or("");
        if matches!(name, "spell" | "emotion" | "break" | "volume" | "speed") {
            if result.chars().last().is_some_and(char::is_alphanumeric)
                && candidate[end + 1..]
                    .chars()
                    .next()
                    .is_some_and(char::is_alphanumeric)
            {
                result.push(' ');
            }
        } else {
            result.push_str(&candidate[..=end]);
        }
        remaining = &candidate[end + 1..];
    }
    result.push_str(remaining);
    result.split_whitespace().collect::<Vec<_>>().join(" ")
}
