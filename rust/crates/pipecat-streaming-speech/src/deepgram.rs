//! Deepgram Listen v1 protocol, mapped from `services/deepgram/stt.py`.
use crate::common::{self, EventStream, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits};
use futures_util::StreamExt;
use pipecat_core::AudioRawFrame;
use serde_json::{json, Value};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Endpointing {
    Disabled,
    Milliseconds(u32),
}

#[derive(Clone, PartialEq)]
pub struct DeepgramConfig {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub language: Option<String>,
    pub sample_rate: u32,
    pub channels: u16,
    pub interim_results: bool,
    pub punctuate: bool,
    pub smart_format: bool,
    pub diarize: bool,
    pub multichannel: bool,
    pub vad_events: bool,
    pub endpointing: Option<Endpointing>,
    pub utterance_end_ms: Option<u32>,
    /// Repeated options, such as `keyterm`, `keywords`, `search`, or `redact`.
    /// Explicit fields and authentication cannot be overridden here.
    pub extra_query: Vec<(String, String)>,
    pub keepalive_interval: Duration,
    pub limits: StreamingLimits,
}
impl fmt::Debug for DeepgramConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeepgramConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("channels", &self.channels)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl DeepgramConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.deepgram.com/v1/listen".into(),
            api_key: api_key.into(),
            model: "nova-3-general".into(),
            language: Some("en".into()),
            sample_rate: 16000,
            channels: 1,
            interim_results: true,
            punctuate: true,
            smart_format: false,
            diarize: false,
            multichannel: false,
            vad_events: false,
            endpointing: None,
            utterance_end_ms: None,
            extra_query: vec![],
            keepalive_interval: Duration::from_secs(5),
            limits: StreamingLimits::default(),
        }
    }
    fn url(&self) -> Result<String, StreamingError> {
        self.limits.validate()?;
        if self.api_key.is_empty()
            || self.model.is_empty()
            || self.sample_rate == 0
            || self.channels == 0
            || self.keepalive_interval.is_zero()
            || self.keepalive_interval > Duration::from_secs(5)
            || self.language.as_ref().is_some_and(String::is_empty)
        {
            return Err(StreamingError::Invalid(
                "Deepgram key, model, audio format, language or keepalive",
            ));
        }
        let mut url = common::endpoint(&self.endpoint)?;
        {
            let mut query = url.query_pairs_mut();
            query
                .append_pair("model", &self.model)
                .append_pair("encoding", "linear16")
                .append_pair("sample_rate", &self.sample_rate.to_string())
                .append_pair("channels", &self.channels.to_string());
            for (key, value) in [
                ("interim_results", self.interim_results),
                ("punctuate", self.punctuate),
                ("smart_format", self.smart_format),
                ("diarize", self.diarize),
                ("multichannel", self.multichannel),
                ("vad_events", self.vad_events),
            ] {
                query.append_pair(key, if value { "true" } else { "false" });
            }
            if let Some(language) = &self.language {
                query.append_pair("language", language);
            }
            if let Some(endpointing) = &self.endpointing {
                query.append_pair(
                    "endpointing",
                    &match endpointing {
                        Endpointing::Disabled => "false".into(),
                        Endpointing::Milliseconds(ms) => ms.to_string(),
                    },
                );
            }
            if let Some(ms) = self.utterance_end_ms {
                query.append_pair("utterance_end_ms", &ms.to_string());
            }
            for (key, value) in &self.extra_query {
                if key.is_empty()
                    || matches!(
                        key.as_str(),
                        "model"
                            | "encoding"
                            | "sample_rate"
                            | "channels"
                            | "interim_results"
                            | "punctuate"
                            | "smart_format"
                            | "diarize"
                            | "multichannel"
                            | "vad_events"
                            | "language"
                            | "endpointing"
                            | "utterance_end_ms"
                            | "api_key"
                            | "token"
                    )
                {
                    return Err(StreamingError::Invalid(
                        "reserved or empty Deepgram query option",
                    ));
                }
                query.append_pair(key, value);
            }
        }
        Ok(url.into())
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct DeepgramTranscript {
    pub text: String,
    pub is_final: bool,
    pub speech_final: bool,
    pub from_finalize: bool,
    /// True only when this acknowledges an outstanding client Finalize.
    pub finalized: bool,
    pub language: Option<String>,
    /// Provider word confidence, timing, speaker and channel information.
    pub result: Value,
}
#[derive(Debug, Clone, PartialEq)]
pub enum DeepgramEvent {
    Transcript(DeepgramTranscript),
    /// Includes an empty final response, which must not create an empty turn.
    Finalized,
    SpeechStarted {
        timestamp: f64,
    },
    UtteranceEnd {
        last_word_end: f64,
    },
    Metadata(Value),
}

#[derive(Debug, Clone)]
pub struct DeepgramClient {
    config: DeepgramConfig,
}
impl DeepgramClient {
    pub fn new(config: DeepgramConfig) -> Result<Self, StreamingError> {
        let url = config.url()?;
        common::request(
            &url,
            &[("authorization", &format!("Token {}", config.api_key))],
        )?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &DeepgramConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<DeepgramSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<DeepgramSession, StreamingError> {
        let request = common::request(
            &self.config.url()?,
            &[("authorization", &format!("Token {}", self.config.api_key))],
        )?;
        let mut socket =
            common::cancelled(&cancel, common::connect(request, &self.config.limits)).await?;
        let config = self.config.clone();
        let (commands, incoming) = mpsc::channel(config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(config.limits.queue_capacity);
        let input = Input {
            tx: commands,
            cancel: cancel.clone(),
            limits: config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let worker_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result =
                common::cancelled(&worker_cancel, run(&mut socket, incoming, &events, &config))
                    .await;
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(DeepgramSession {
            sender: DeepgramSender {
                input,
                sample_rate: self.config.sample_rate,
                channels: self.config.channels,
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
pub struct DeepgramSender {
    input: Input<Command>,
    sample_rate: u32,
    channels: u16,
}
impl DeepgramSender {
    fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        audio.validate().map_err(|_| {
            StreamingError::Invalid("PCM must contain complete signed 16-bit samples")
        })?;
        if audio.sample_rate != self.sample_rate || audio.num_channels != self.channels {
            return Err(StreamingError::Invalid(
                "PCM format differs from Deepgram configuration",
            ));
        }
        if audio.audio.len() > self.input.limits.max_message_bytes {
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
    pub async fn finalize(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finalize, false).await
    }
    pub fn try_finalize(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Finalize, false)
    }
    /// Send CloseStream after accepted audio and drain final provider messages.
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
pub struct DeepgramSession {
    sender: DeepgramSender,
    events: EventStream<DeepgramEvent>,
}
impl DeepgramSession {
    pub fn sender(&self) -> DeepgramSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<DeepgramEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}

async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<DeepgramEvent>,
    config: &DeepgramConfig,
) -> Result<(), StreamingError> {
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + config.keepalive_interval,
        config.keepalive_interval,
    );
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut closing = None;
    let mut awaiting_finalize = false;
    loop {
        let deadline = async {
            match closing {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _ = events.closed() => return Err(StreamingError::Closed),
            _ = deadline => return Err(StreamingError::Timeout("Deepgram final response")),
            incoming = socket.next() => match incoming {
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(&text).map_err(|_| StreamingError::Protocol("Deepgram JSON"))?;
                    for event in decode(value, &mut awaiting_finalize)? {
                        let deadline = closing.unwrap_or_else(|| tokio::time::Instant::now() + config.limits.write_timeout);
                        tokio::time::timeout_at(deadline, events.send(event)).await
                            .map_err(|_| StreamingError::Timeout("Deepgram output queue"))?
                            .map_err(|_| StreamingError::Closed)?;
                    }
                }
                Some(Ok(Message::Close(_))) | None => return if closing.is_some() { Ok(()) } else { Err(StreamingError::Closed) },
                Some(Ok(Message::Ping(bytes))) => common::write(socket, Message::Pong(bytes), &config.limits).await?,
                Some(Ok(Message::Pong(_))) => {},
                Some(Ok(_)) => return Err(StreamingError::Protocol("Deepgram sent non-text data")),
                Some(Err(error)) => return Err(common::transport(error)),
            },
            command = commands.recv(), if closing.is_none() => match command {
                Some(Command::Audio(audio)) => common::write(socket, Message::Binary(audio.into()), &config.limits).await?,
                Some(Command::Finalize) => {
                    common::write(socket, Message::Text(json!({"type":"Finalize"}).to_string().into()), &config.limits).await?;
                    awaiting_finalize = true;
                }
                Some(Command::Finish) | None => {
                    common::write(socket, Message::Text(json!({"type":"CloseStream"}).to_string().into()), &config.limits).await?;
                    commands.close();
                    closing = Some(tokio::time::Instant::now() + config.limits.response_timeout);
                }
            },
            _ = keepalive.tick(), if closing.is_none() => common::write(socket, Message::Text(json!({"type":"KeepAlive"}).to_string().into()), &config.limits).await?,
        }
    }
}

fn decode(
    value: Value,
    awaiting_finalize: &mut bool,
) -> Result<Vec<DeepgramEvent>, StreamingError> {
    let mut events = vec![];
    match value.get("type").and_then(Value::as_str) {
        Some("Results") => {
            let is_final = value
                .get("is_final")
                .and_then(Value::as_bool)
                .ok_or(StreamingError::Protocol("Deepgram is_final"))?;
            let from_finalize = is_final
                && value
                    .get("from_finalize")
                    .and_then(Value::as_bool)
                    .unwrap_or(false);
            let finalized = from_finalize && *awaiting_finalize;
            if finalized {
                *awaiting_finalize = false;
                events.push(DeepgramEvent::Finalized);
            }
            let Some(alt) = value.pointer("/channel/alternatives/0") else {
                return Ok(events);
            };
            let text = alt
                .get("transcript")
                .and_then(Value::as_str)
                .ok_or(StreamingError::Protocol("Deepgram transcript"))?;
            if !text.is_empty() {
                events.push(DeepgramEvent::Transcript(DeepgramTranscript {
                    text: text.into(),
                    is_final,
                    speech_final: value
                        .get("speech_final")
                        .and_then(Value::as_bool)
                        .unwrap_or(false),
                    from_finalize,
                    finalized,
                    language: alt
                        .pointer("/languages/0")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    result: value,
                }));
            }
        }
        Some("Metadata") => events.push(DeepgramEvent::Metadata(value)),
        Some("SpeechStarted") => events.push(DeepgramEvent::SpeechStarted {
            timestamp: number(&value, "timestamp")?,
        }),
        Some("UtteranceEnd") => events.push(DeepgramEvent::UtteranceEnd {
            last_word_end: number(&value, "last_word_end")?,
        }),
        Some("Error") => {
            return Err(StreamingError::Provider {
                status: value
                    .get("status_code")
                    .and_then(Value::as_u64)
                    .and_then(|n| u16::try_from(n).ok()),
            })
        }
        Some(_) => {} // Ignore additive provider control events.
        None => return Err(StreamingError::Protocol("Deepgram message type")),
    }
    Ok(events)
}
fn number(value: &Value, key: &str) -> Result<f64, StreamingError> {
    value
        .get(key)
        .and_then(Value::as_f64)
        .filter(|n| n.is_finite() && *n >= 0.0)
        .ok_or(StreamingError::Protocol("Deepgram event timestamp"))
}
