// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    common::{self, EventStream, Input, Socket},
    settings, CancellationToken, SonioxSttSettings, StreamingError, StreamingLimits,
};
use futures_util::StreamExt;
use pipecat_core::AudioRawFrame;
use serde_json::{json, Map, Value};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Clone, PartialEq)]
pub struct SonioxSttConfig {
    pub endpoint: String,
    pub api_key: String,
    pub sample_rate: u32,
    pub num_channels: u16,
    pub settings: SonioxSttSettings,
    pub vad_force_turn_endpoint: bool,
    pub should_interrupt: bool,
    pub max_transcript_bytes: usize,
    pub max_tokens: usize,
    pub limits: StreamingLimits,
}
impl fmt::Debug for SonioxSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SonioxSttConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl SonioxSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://stt-rt.soniox.com/transcribe-websocket".into(),
            api_key: api_key.into(),
            sample_rate: 16000,
            num_channels: 1,
            settings: SonioxSttSettings::default(),
            vad_force_turn_endpoint: true,
            should_interrupt: true,
            max_transcript_bytes: 128 * 1024,
            max_tokens: 8192,
            limits: StreamingLimits::default(),
        }
    }
    pub fn validate(&self) -> Result<(), StreamingError> {
        self.limits.validate()?;
        common::endpoint(&self.endpoint)?;
        self.settings.validate()?;
        if self.api_key.is_empty()
            || self.api_key.len() > 8192
            || !(8000..=48000).contains(&self.sample_rate)
            || !(1..=8).contains(&self.num_channels)
            || !(1..=1024 * 1024).contains(&self.max_transcript_bytes)
            || !(1..=65536).contains(&self.max_tokens)
            || self.limits.max_message_bytes < usize::from(self.num_channels) * 2
        {
            return Err(StreamingError::Invalid(
                "Soniox audio configuration or limits",
            ));
        }
        if self.message().to_string().len() > self.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    fn message(&self) -> Value {
        let mut value = serde_json::to_value(&self.settings).expect("serializable settings");
        let obj = value.as_object_mut().unwrap();
        obj.remove("language");
        obj.extend(json!({"api_key":self.api_key,"audio_format":"pcm_s16le","num_channels":self.num_channels,"sample_rate":self.sample_rate,"enable_endpoint_detection":!self.vad_force_turn_endpoint,"language_hints":self.settings.normalized_hints()}).as_object().unwrap().clone());
        value
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct SonioxTranscript {
    pub text: String,
    pub tokens: Vec<Value>,
    pub language: Option<String>,
    pub finalized: bool,
}
#[derive(Debug, Clone, PartialEq)]
pub enum SonioxSttEvent {
    Transcript(SonioxTranscript),
    TurnStarted,
    TurnStopped,
    AudioSent {
        seconds: f64,
    },
    /// Settings reconnect discards the old session's unfinalized tokens.
    Reset,
}
#[derive(Debug, Clone)]
pub struct SonioxSttClient {
    config: SonioxSttConfig,
}
impl SonioxSttClient {
    pub fn new(config: SonioxSttConfig) -> Result<Self, StreamingError> {
        config.validate()?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &SonioxSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<SonioxSttSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<SonioxSttSession, StreamingError> {
        let mut socket = common::cancelled(&cancel, connect(&self.config)).await?;
        let (tx, incoming) = mpsc::channel(self.config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let input = Input {
            tx,
            cancel: cancel.clone(),
            limits: self.config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let config = Arc::new(Mutex::new(self.config.clone()));
        let actor_config = self.config.clone();
        let limits = self.config.limits.clone();
        let worker_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            let result = common::cancelled(
                &worker_cancel,
                run(&mut socket, incoming, &events, actor_config),
            )
            .await;
            common::close(&mut socket, &limits).await;
            result
        });
        Ok(SonioxSttSession {
            sender: SonioxSttSender { input, config },
            events: EventStream {
                rx,
                cancel,
                task,
                ended: false,
            },
        })
    }
}
async fn connect(config: &SonioxSttConfig) -> Result<Socket, StreamingError> {
    let mut socket =
        common::connect(common::request(&config.endpoint, &[])?, &config.limits).await?;
    common::write(
        &mut socket,
        Message::Text(config.message().to_string().into()),
        &config.limits,
    )
    .await?;
    Ok(socket)
}
enum Command {
    Audio(AudioRawFrame),
    Finalize,
    StartTurn,
    Update(Box<SonioxSttConfig>),
    Finish,
}
#[derive(Clone)]
pub struct SonioxSttSender {
    input: Input<Command>,
    config: Arc<Mutex<SonioxSttConfig>>,
}
impl SonioxSttSender {
    pub fn config_snapshot(&self) -> Result<SonioxSttConfig, StreamingError> {
        self.config
            .lock()
            .map(|s| s.clone())
            .map_err(|_| StreamingError::Closed)
    }
    pub fn validate_audio(&self, audio: &AudioRawFrame) -> Result<(), StreamingError> {
        let config = self.config.lock().map_err(|_| StreamingError::Closed)?;
        if audio.sample_rate != config.sample_rate || audio.num_channels != config.num_channels {
            return Err(StreamingError::Invalid(
                "PCM format differs from Soniox session",
            ));
        }
        if !audio
            .audio
            .len()
            .is_multiple_of(usize::from(config.num_channels) * 2)
        {
            return Err(StreamingError::PartialSample);
        }
        if audio.audio.is_empty() {
            return Err(StreamingError::Invalid("empty PCM audio"));
        }
        if audio.audio.len() > config.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        self.input.send(Command::Audio(audio), false).await
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.validate_audio(&audio)?;
        self.input.try_send(Command::Audio(audio), false)
    }
    pub async fn finalize(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finalize, false).await
    }
    pub fn try_finalize(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Finalize, false)
    }
    pub fn try_start_turn(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::StartTurn, false)
    }
    pub fn merge_settings(&self, delta: &Map<String, Value>) -> Result<(), StreamingError> {
        let mut config = self.config.lock().map_err(|_| StreamingError::Closed)?;
        let mut candidate = config.clone();
        candidate.settings = config.settings.merge(delta)?;
        candidate.validate()?;
        if candidate == *config {
            return Ok(());
        }
        self.input
            .try_send(Command::Update(Box::new(candidate.clone())), false)?;
        *config = candidate;
        Ok(())
    }
    /// Sends Soniox's empty text end-of-audio frame, then closes. Like the
    /// source service, stop does not wait for a trailing turn transcript.
    pub async fn finish(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finish, true).await
    }
    pub fn cancel(&self) {
        self.input.cancel.cancel();
    }
    pub fn is_closed(&self) -> bool {
        self.input.cancel.is_cancelled()
            || self.input.tx.is_closed()
            || self.input.finished.lock().map_or(true, |v| *v)
    }
}
pub struct SonioxSttSession {
    sender: SonioxSttSender,
    events: EventStream<SonioxSttEvent>,
}
impl SonioxSttSession {
    pub fn sender(&self) -> SonioxSttSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<SonioxSttEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.sender.cancel();
    }
}
#[derive(Default)]
struct TranscriptState {
    tokens: Vec<Value>,
    bytes: usize,
    open: bool,
}
impl TranscriptState {
    async fn start(
        &mut self,
        config: &SonioxSttConfig,
        events: &mpsc::Sender<SonioxSttEvent>,
    ) -> Result<(), StreamingError> {
        if !config.vad_force_turn_endpoint && !self.open {
            self.open = true;
            emit(events, SonioxSttEvent::TurnStarted).await?;
        }
        Ok(())
    }
    async fn finish(
        &mut self,
        events: &mpsc::Sender<SonioxSttEvent>,
    ) -> Result<(), StreamingError> {
        if !self.tokens.is_empty() {
            let tokens = std::mem::take(&mut self.tokens);
            self.bytes = 0;
            emit(events, SonioxSttEvent::Transcript(transcript(tokens, true))).await?;
        }
        if self.open {
            self.open = false;
            emit(events, SonioxSttEvent::TurnStopped).await?;
        }
        Ok(())
    }
    async fn message(
        &mut self,
        value: Value,
        config: &SonioxSttConfig,
        events: &mpsc::Sender<SonioxSttEvent>,
    ) -> Result<bool, StreamingError> {
        let obj = value
            .as_object()
            .ok_or(StreamingError::Protocol("STT message is not an object"))?;
        let tokens = match obj.get("tokens") {
            None if obj.contains_key("error_code") || obj.contains_key("finished") => &[][..],
            Some(Value::Array(v)) => v.as_slice(),
            _ => return Err(StreamingError::Protocol("STT tokens missing or invalid")),
        };
        if tokens.len() > config.max_tokens {
            return Err(StreamingError::TooLarge);
        }
        // Validate the whole event before mutating transcript or turn state.
        let mut event_bytes = 0usize;
        for token in tokens {
            let _text = token
                .get("text")
                .and_then(Value::as_str)
                .ok_or(StreamingError::Protocol("STT token text"))?;
            if token.get("is_final").and_then(Value::as_bool).is_none() {
                return Err(StreamingError::Protocol("STT token finality"));
            }
            event_bytes = event_bytes.saturating_add(token.to_string().len());
        }
        if event_bytes.saturating_add(self.bytes) > config.max_transcript_bytes
            || tokens.len().saturating_add(self.tokens.len()) > config.max_tokens
        {
            return Err(StreamingError::TooLarge);
        }
        let mut interim = vec![];
        for token in tokens {
            let text = token["text"].as_str().unwrap();
            let endpoint = matches!(text, "<end>" | "<fin>");
            if !endpoint {
                self.start(config, events).await?;
            }
            if token["is_final"].as_bool().unwrap() {
                if endpoint {
                    self.finish(events).await?;
                } else {
                    self.bytes += token.to_string().len();
                    self.tokens.push(token.clone());
                }
            } else if !endpoint {
                interim.push(token.clone());
            }
        }
        if !self.tokens.is_empty() || !interim.is_empty() {
            let mut all = self.tokens.clone();
            all.extend(interim);
            emit(events, SonioxSttEvent::Transcript(transcript(all, false))).await?;
        }
        if obj.get("error_code").is_some_and(|v| !v.is_null())
            || obj.get("error_message").is_some_and(|v| !v.is_null())
        {
            self.finish(events).await?;
            return Err(StreamingError::Provider {
                status: obj
                    .get("error_code")
                    .and_then(Value::as_u64)
                    .and_then(|n| u16::try_from(n).ok()),
            });
        }
        match obj.get("finished") {
            Some(Value::Bool(true)) => {
                self.finish(events).await?;
                Ok(true)
            }
            None | Some(Value::Bool(false)) => Ok(false),
            _ => Err(StreamingError::Protocol("STT finished must be boolean")),
        }
    }
}
fn transcript(tokens: Vec<Value>, finalized: bool) -> SonioxTranscript {
    let text = tokens.iter().filter_map(|t| t["text"].as_str()).collect();
    let mut counts: Vec<(String, usize)> = vec![];
    for token in &tokens {
        if let Some(language) = token
            .get("language")
            .and_then(Value::as_str)
            .and_then(settings::transcript_language)
        {
            if let Some((_, count)) = counts.iter_mut().find(|(l, _)| l == &language) {
                *count += 1;
            } else {
                counts.push((language, 1));
            }
        }
    }
    let language = counts
        .iter()
        .enumerate()
        .max_by_key(|(index, (_, n))| (*n, std::cmp::Reverse(*index)))
        .map(|(_, (l, _))| l.clone());
    SonioxTranscript {
        text,
        tokens,
        language,
        finalized,
    }
}
async fn emit(
    events: &mpsc::Sender<SonioxSttEvent>,
    event: SonioxSttEvent,
) -> Result<(), StreamingError> {
    events.send(event).await.map_err(|_| StreamingError::Closed)
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<SonioxSttEvent>,
    mut config: SonioxSttConfig,
) -> Result<(), StreamingError> {
    let mut state = TranscriptState::default();
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + Duration::from_secs(5),
        Duration::from_secs(5),
    );
    loop {
        tokio::select! {
            command = commands.recv() => match command {
                Some(Command::Audio(audio)) => { common::write(socket,Message::Binary(audio.audio.clone().into()), &config.limits).await?; emit(events, SonioxSttEvent::AudioSent { seconds: audio.audio.len() as f64 / (f64::from(audio.sample_rate)*f64::from(audio.num_channels)*2.0) }).await?; }
                Some(Command::Finalize) if config.vad_force_turn_endpoint => common::write(socket,Message::Text(json!({"type":"finalize"}).to_string().into()), &config.limits).await?,
                Some(Command::Finalize) => (),
                Some(Command::StartTurn) => state.start(&config,events).await?,
                Some(Command::Update(next)) => { if state.open { emit(events,SonioxSttEvent::TurnStopped).await?; } state=TranscriptState::default(); common::close(socket,&config.limits).await; config = *next; *socket =connect(&config).await?; emit(events,SonioxSttEvent::Reset).await?; }
                Some(Command::Finish) => { common::write(socket,Message::Text("".into()), &config.limits).await?; return Ok(()); }
                None => return Ok(()),
            },
            message = socket.next() => match message {
                Some(Ok(Message::Text(text))) => { let value = serde_json::from_str(&text).map_err(|_| StreamingError::Protocol("STT JSON"))?; if state.message(value,&config,events).await? { return Ok(()); } }
                Some(Ok(Message::Ping(data))) => common::write(socket, Message::Pong(data), &config.limits).await?,
                Some(Ok(Message::Pong(_))) => (),
                Some(Ok(Message::Close(_))) | None => return Err(StreamingError::Closed),
                Some(Ok(_)) => return Err(StreamingError::Protocol("unexpected STT binary message")),
                Some(Err(error)) => return Err(common::transport(error)),
            },
            _ = keepalive.tick() => common::write(socket,Message::Text(json!({"type":"keepalive"}).to_string().into()),&config.limits).await?,
        }
    }
}
