// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    common::{self, EventStream, Input, Socket},
    is_u3_pro_model, prepare_language_codes,
    settings::clip_context,
    CancellationToken, StreamingError, StreamingLimits, StreamingSettings,
};
use futures_util::StreamExt;
use serde::Deserialize;
use serde_json::{json, Map, Value};
use std::{
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::{mpsc, oneshot};
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Encoding {
    PcmS16Le,
    PcmMulaw,
}
#[derive(Clone)]
pub struct AssemblyAiConfig {
    pub endpoint: String,
    pub api_key: String,
    /// None inherits the pipeline StartFrame rate; direct connections use 16kHz.
    pub sample_rate: Option<u32>,
    pub encoding: Encoding,
    pub vad_force_turn_endpoint: bool,
    pub should_interrupt: bool,
    pub ttfs_p99_latency: f64,
    pub speaker_format: Option<String>,
    pub settings: StreamingSettings,
    pub limits: StreamingLimits,
}
impl fmt::Debug for AssemblyAiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssemblyAiConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl AssemblyAiConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://streaming.assemblyai.com/v3/ws".into(),
            api_key: api_key.into(),
            sample_rate: None,
            encoding: Encoding::PcmS16Le,
            vad_force_turn_endpoint: true,
            should_interrupt: true,
            ttfs_p99_latency: 0.42,
            speaker_format: None,
            settings: StreamingSettings::default(),
            limits: StreamingLimits {
                response_timeout: std::time::Duration::from_secs(5),
                ..StreamingLimits::default()
            },
        }
    }
    fn rate(&self) -> u32 {
        self.sample_rate.unwrap_or(16000)
    }
    fn chunk_size(&self) -> usize {
        self.rate() as usize / 20
            * if self.encoding == Encoding::PcmS16Le {
                2
            } else {
                1
            }
    }
    fn url(&self) -> Result<String, StreamingError> {
        let mut url = common::endpoint(&self.endpoint)?;
        url.query_pairs_mut()
            .append_pair("sample_rate", &self.rate().to_string())
            .append_pair(
                "encoding",
                if self.encoding == Encoding::PcmS16Le {
                    "pcm_s16le"
                } else {
                    "pcm_mulaw"
                },
            )
            .extend_pairs(self.settings.query()?);
        Ok(url.into())
    }
    fn normalized(mut self) -> Result<Self, StreamingError> {
        self.limits.validate()?;
        if !self.ttfs_p99_latency.is_finite() || self.ttfs_p99_latency < 0.0 {
            return Err(StreamingError::Invalid("TTFS latency"));
        }
        if self.api_key.is_empty()
            || self.api_key.len() > 4096
            || !(8000..=192000).contains(&self.rate())
            || !self.rate().is_multiple_of(20)
            || self.chunk_size() > self.limits.max_message_bytes
        {
            return Err(StreamingError::Invalid("key, sample rate or message bound"));
        }
        if self.speaker_format.as_ref().is_some_and(|s| {
            s.len() > 4096
                || s.replace("{speaker}", "")
                    .replace("{text}", "")
                    .contains(['{', '}'])
        }) {
            return Err(StreamingError::Invalid(
                "speaker format supports only {speaker} and {text}",
            ));
        }
        self.settings = self.settings.normalized(self.vad_force_turn_endpoint)?;
        let url = self.url()?;
        common::request(
            &url,
            &[("authorization", &self.api_key), ("user-agent", USER_AGENT)],
        )?;
        Ok(self)
    }
}
const USER_AGENT: &str = concat!(
    "AssemblyAI/1.0 (integration=Pipecat/",
    env!("CARGO_PKG_VERSION"),
    ")"
);

#[derive(Clone)]
pub struct AssemblyAiClient {
    config: AssemblyAiConfig,
}
impl fmt::Debug for AssemblyAiClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AssemblyAiClient")
            .field("config", &self.config)
            .finish()
    }
}
impl AssemblyAiClient {
    pub fn new(config: AssemblyAiConfig) -> Result<Self, StreamingError> {
        Ok(Self {
            config: config.normalized()?,
        })
    }
    pub fn config(&self) -> &AssemblyAiConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<AssemblyAiSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<AssemblyAiSession, StreamingError> {
        let mut socket = common::cancelled(&cancel, open(&self.config)).await?;
        let (tx, commands) = mpsc::channel(self.config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let settings = Arc::new(Mutex::new(self.config.settings.clone()));
        let input = Input {
            tx,
            cancel: cancel.clone(),
            limits: self.config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let config = self.config.clone();
        let current = settings.clone();
        let token = cancel.clone();
        let task = tokio::spawn(async move {
            let result = common::cancelled(
                &token,
                run(&mut socket, commands, &events, config.clone(), current),
            )
            .await;
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(AssemblyAiSession {
            sender: AssemblyAiSender {
                input,
                settings,
                encoding: self.config.encoding,
                max_audio_bytes: self.config.limits.max_message_bytes,
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
async fn open(config: &AssemblyAiConfig) -> Result<Socket, StreamingError> {
    let request = common::request(
        &config.url()?,
        &[
            ("authorization", &config.api_key),
            ("user-agent", USER_AGENT),
        ],
    )?;
    common::connect(request, &config.limits).await
}
#[derive(Debug, Clone, PartialEq)]
pub struct Transcript {
    pub text: String,
    pub speaker: Option<String>,
    pub language: String,
    pub is_final: bool,
    pub finalized: bool,
    pub turn_order: u64,
    pub result: Value,
}
#[derive(Debug, Clone, PartialEq)]
pub enum AssemblyAiEvent {
    Begin {
        id: String,
        expires_at: u64,
    },
    Transcript(Transcript),
    ProposedUserStartedSpeaking,
    ProposedUserStoppedSpeaking,
    EndOfTurn {
        text: String,
    },
    Terminated {
        audio_duration_seconds: f64,
        session_duration_seconds: f64,
    },
    SettingsRejected(StreamingError),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateOutcome {
    pub reconnected: bool,
    pub dropped_language_codes: bool,
}
enum Command {
    Audio(Vec<u8>),
    Finalize,
    StartUtterance,
    Finish,
    Update(
        Map<String, Value>,
        Option<oneshot::Sender<Result<UpdateOutcome, StreamingError>>>,
    ),
    Context(String, bool),
}
#[derive(Clone)]
pub struct AssemblyAiSender {
    input: Input<Command>,
    settings: Arc<Mutex<StreamingSettings>>,
    encoding: Encoding,
    max_audio_bytes: usize,
}
impl AssemblyAiSender {
    fn audio(&self, audio: Vec<u8>) -> Result<Command, StreamingError> {
        if audio.len() > self.max_audio_bytes {
            return Err(StreamingError::TooLarge);
        }
        if self.encoding == Encoding::PcmS16Le && !audio.len().is_multiple_of(2) {
            return Err(StreamingError::PartialSample);
        }
        Ok(Command::Audio(audio))
    }
    pub async fn send_audio(&self, audio: Vec<u8>) -> Result<(), StreamingError> {
        self.input.send(self.audio(audio)?, false).await
    }
    pub fn try_send_audio(&self, audio: Vec<u8>) -> Result<(), StreamingError> {
        self.input.try_send(self.audio(audio)?, false)
    }
    pub fn start_utterance(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::StartUtterance, false)
    }
    pub async fn finalize(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finalize, false).await
    }
    pub fn try_finalize(&self) -> Result<(), StreamingError> {
        self.input.try_send(Command::Finalize, false)
    }
    pub async fn finish(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Finish, true).await
    }
    pub fn cancel(&self) {
        self.input.cancel.cancel();
    }
    pub fn settings(&self) -> Result<StreamingSettings, StreamingError> {
        self.settings
            .lock()
            .map(|s| s.clone())
            .map_err(|_| StreamingError::Closed)
    }
    pub async fn update_settings(
        &self,
        delta: Map<String, Value>,
    ) -> Result<UpdateOutcome, StreamingError> {
        self.validate_delta(&delta)?;
        let (tx, rx) = oneshot::channel();
        self.input
            .send(Command::Update(delta, Some(tx)), false)
            .await?;
        common::cancelled(&self.input.cancel, async {
            rx.await.map_err(|_| StreamingError::Closed)?
        })
        .await
    }
    fn validate_delta(&self, delta: &Map<String, Value>) -> Result<(), StreamingError> {
        if serde_json::to_vec(delta)
            .map_err(|_| StreamingError::Invalid("settings"))?
            .len()
            > 64 * 1024
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub fn try_update_settings(&self, delta: Map<String, Value>) -> Result<(), StreamingError> {
        self.validate_delta(&delta)?;
        self.input.try_send(Command::Update(delta, None), false)
    }
    pub async fn update_agent_context(&self, text: &str) -> Result<(), StreamingError> {
        self.input
            .send(Command::Context(clip_context(text), false), false)
            .await
    }
    pub fn assistant_turn(&self, text: &str) -> Result<(), StreamingError> {
        self.input
            .try_send(Command::Context(clip_context(text), true), false)
    }
}
pub struct AssemblyAiSession {
    sender: AssemblyAiSender,
    events: EventStream<AssemblyAiEvent>,
}
impl AssemblyAiSession {
    pub fn sender(&self) -> AssemblyAiSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<AssemblyAiEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.sender.cancel();
    }
}
#[derive(Default)]
struct TurnState {
    latest: Option<u64>,
    complete: bool,
    speaking: bool,
    finalize_pending: bool,
}
async fn emit(
    events: &mpsc::Sender<AssemblyAiEvent>,
    event: AssemblyAiEvent,
    config: &AssemblyAiConfig,
) -> Result<(), StreamingError> {
    tokio::time::timeout(config.limits.write_timeout, events.send(event))
        .await
        .map_err(|_| StreamingError::Timeout("output queue"))?
        .map_err(|_| StreamingError::Closed)
}
async fn send_json(
    socket: &mut Socket,
    value: Value,
    config: &AssemblyAiConfig,
) -> Result<(), StreamingError> {
    common::write(
        socket,
        Message::Text(value.to_string().into()),
        &config.limits,
    )
    .await
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<AssemblyAiEvent>,
    mut config: AssemblyAiConfig,
    current: Arc<Mutex<StreamingSettings>>,
) -> Result<(), StreamingError> {
    let mut buffer = Vec::new();
    let mut turns = TurnState::default();
    loop {
        tokio::select! {
            message = socket.next() => {
                if handle_wire(message, socket, events, &config, &mut turns, true).await? { return Ok(()); }
            }
            command = commands.recv() => match command {
                Some(Command::Audio(audio)) => {
                    buffer.extend(audio);
                    let size = config.chunk_size();
                    let count = buffer.len() / size * size;
                    for chunk in buffer[..count].chunks(size) { common::write(socket, Message::Binary(chunk.to_vec().into()), &config.limits).await?; }
                    buffer.drain(..count);
                }
                Some(Command::StartUtterance) => { turns.finalize_pending = false; },
                Some(Command::Finalize) if config.vad_force_turn_endpoint => {
                    // Preserve source ordering: ForceEndpoint does not flush the
                    // sub-50ms buffer; graceful termination does.
                    send_json(socket, json!({"type":"ForceEndpoint"}), &config).await?;
                    turns.finalize_pending = true;
                }
                Some(Command::Finalize) => (),
                Some(Command::Context(text, automatic)) => {
                    if !text.is_empty() && is_u3_pro_model(&config.settings.model) && !(automatic && config.settings.previous_context_n_turns == Some(0)) {
                        send_json(socket, json!({"type":"UpdateConfiguration", "agent_context":text}), &config).await?;
                        config.settings.agent_context = Some(text);
                        *current.lock().map_err(|_| StreamingError::Closed)? = config.settings.clone();
                    }
                }
                Some(Command::Update(delta, response)) => {
                    let (candidate, dropped) = match config.settings.patch(delta, config.vad_force_turn_endpoint) {
                        Ok(value) => value,
                        Err(error) => { if let Some(tx) = response { let _ = tx.send(Err(error)); } else { emit(events, AssemblyAiEvent::SettingsRejected(error), &config).await?; } continue; }
                    };
                    let reconnect = config.settings.reconnect_differs(&candidate);
                    if reconnect {
                        drain(socket, events, &config, &mut buffer, &mut turns, false).await?;
                        common::close(socket, &config.limits).await;
                        config.settings = candidate;
                        *socket = open(&config).await?;
                        turns = TurnState::default();
                    } else {
                        if is_u3_pro_model(&candidate.model) {
                            if candidate.agent_context != config.settings.agent_context {
                                if let Some(text) = &candidate.agent_context { if !text.is_empty() { send_json(socket, json!({"type":"UpdateConfiguration","agent_context":text}), &config).await?; } }
                            }
                            if candidate.language_codes != config.settings.language_codes {
                                if let Some(codes) = &candidate.language_codes { send_json(socket, json!({"type":"UpdateConfiguration","language_codes":prepare_language_codes(codes, Some(10))?}), &config).await?; }
                            }
                        }
                        config.settings = candidate;
                    }
                    *current.lock().map_err(|_| StreamingError::Closed)? = config.settings.clone();
                    if let Some(tx) = response { let _ = tx.send(Ok(UpdateOutcome { reconnected: reconnect, dropped_language_codes: dropped })); }
                }
                Some(Command::Finish) | None => return drain(socket, events, &config, &mut buffer, &mut turns, true).await,
            }
        }
    }
}
async fn drain(
    socket: &mut Socket,
    events: &mpsc::Sender<AssemblyAiEvent>,
    config: &AssemblyAiConfig,
    buffer: &mut Vec<u8>,
    turns: &mut TurnState,
    terminal: bool,
) -> Result<(), StreamingError> {
    tokio::time::timeout(config.limits.response_timeout, async {
        if !buffer.is_empty() {
            common::write(
                socket,
                Message::Binary(std::mem::take(buffer).into()),
                &config.limits,
            )
            .await?;
        }
        send_json(socket, json!({"type":"Terminate"}), config).await?;
        loop {
            let message = socket.next().await;
            if handle_wire(message, socket, events, config, turns, terminal).await? {
                return Ok(());
            }
        }
    })
    .await
    .map_err(|_| StreamingError::Timeout("termination"))?
}
#[derive(Deserialize)]
#[serde(tag = "type")]
enum Wire {
    Begin {
        id: String,
        expires_at: u64,
    },
    SpeechStarted {
        timestamp: u64,
    },
    Turn(Turn),
    Termination {
        audio_duration_seconds: f64,
        session_duration_seconds: f64,
    },
}
#[derive(Deserialize)]
struct Word {
    start: u64,
    end: u64,
    text: String,
    confidence: f64,
    word_is_final: bool,
}
#[derive(Deserialize)]
struct Turn {
    turn_order: u64,
    turn_is_formatted: bool,
    end_of_turn: bool,
    transcript: String,
    end_of_turn_confidence: f64,
    words: Vec<Word>,
    language_code: Option<String>,
    language_confidence: Option<f64>,
    #[serde(alias = "speaker")]
    speaker_label: Option<String>,
}
async fn handle_wire(
    message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    socket: &mut Socket,
    events: &mpsc::Sender<AssemblyAiEvent>,
    config: &AssemblyAiConfig,
    turns: &mut TurnState,
    terminal: bool,
) -> Result<bool, StreamingError> {
    let text = match message
        .ok_or(StreamingError::Closed)?
        .map_err(common::transport)?
    {
        Message::Text(text) => text,
        Message::Ping(data) => {
            common::write(socket, Message::Pong(data), &config.limits).await?;
            return Ok(false);
        }
        Message::Pong(_) => return Ok(false),
        Message::Close(_) => return Err(StreamingError::Closed),
        _ => return Err(StreamingError::Protocol("expected JSON text")),
    };
    let raw: Value =
        serde_json::from_str(&text).map_err(|_| StreamingError::Protocol("malformed JSON"))?;
    let wire: Wire = serde_json::from_value(raw.clone())
        .map_err(|_| StreamingError::Protocol("invalid message schema"))?;
    match wire {
        Wire::Begin { id, expires_at } => {
            emit(events, AssemblyAiEvent::Begin { id, expires_at }, config).await?
        }
        Wire::SpeechStarted { timestamp } => {
            let _ = timestamp;
            if !config.vad_force_turn_endpoint && !turns.speaking {
                emit(events, AssemblyAiEvent::ProposedUserStartedSpeaking, config).await?;
                turns.speaking = true;
            }
        }
        Wire::Termination {
            audio_duration_seconds,
            session_duration_seconds,
        } => {
            if !audio_duration_seconds.is_finite()
                || !session_duration_seconds.is_finite()
                || audio_duration_seconds < 0.0
                || session_duration_seconds < 0.0
            {
                return Err(StreamingError::Protocol("termination duration"));
            }
            if turns.speaking && !config.vad_force_turn_endpoint {
                emit(events, AssemblyAiEvent::ProposedUserStoppedSpeaking, config).await?;
                turns.speaking = false;
            }
            if terminal {
                emit(
                    events,
                    AssemblyAiEvent::Terminated {
                        audio_duration_seconds,
                        session_duration_seconds,
                    },
                    config,
                )
                .await?;
            }
            return Ok(true);
        }
        Wire::Turn(turn) => {
            if !turn.end_of_turn_confidence.is_finite()
                || !(0.0..=1.0).contains(&turn.end_of_turn_confidence)
                || turn
                    .language_confidence
                    .is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
                || turn.words.iter().any(|w| {
                    w.end < w.start
                        || !w.confidence.is_finite()
                        || !(0.0..=1.0).contains(&w.confidence)
                })
            {
                return Err(StreamingError::Protocol("turn timing or confidence"));
            }
            // Validate each required word field while preserving raw metadata.
            for word in &turn.words {
                let _ = (&word.text, word.word_is_final);
            }
            if turns
                .latest
                .is_some_and(|n| turn.turn_order < n || (turn.turn_order == n && turns.complete))
            {
                return Ok(false);
            }
            if turns.latest != Some(turn.turn_order) {
                turns.latest = Some(turn.turn_order);
                turns.complete = false;
            }
            let final_turn =
                turn.end_of_turn && (!config.settings.format_turns || turn.turn_is_formatted);
            if !turn.transcript.is_empty() {
                let language = if turn.language_confidence.is_some_and(|c| c >= 0.7) {
                    detected_language(turn.language_code.as_deref())
                } else {
                    "en".into()
                };
                let text = match (&config.speaker_format, &turn.speaker_label) {
                    (Some(format), Some(speaker)) if !speaker.is_empty() => speaker_text(
                        format,
                        speaker,
                        &turn.transcript,
                        config.limits.max_message_bytes,
                    )?,
                    _ => turn.transcript,
                };
                let finalized = final_turn
                    && (!config.vad_force_turn_endpoint
                        || (turn.turn_is_formatted && turns.finalize_pending));
                emit(
                    events,
                    AssemblyAiEvent::Transcript(Transcript {
                        text: text.clone(),
                        speaker: turn.speaker_label,
                        language,
                        is_final: final_turn,
                        finalized,
                        turn_order: turn.turn_order,
                        result: raw,
                    }),
                    config,
                )
                .await?;
                if final_turn {
                    if !config.vad_force_turn_endpoint {
                        emit(events, AssemblyAiEvent::ProposedUserStoppedSpeaking, config).await?;
                        turns.speaking = false;
                    }
                    emit(events, AssemblyAiEvent::EndOfTurn { text }, config).await?;
                }
            } else if final_turn && turns.speaking && !config.vad_force_turn_endpoint {
                emit(events, AssemblyAiEvent::ProposedUserStoppedSpeaking, config).await?;
                turns.speaking = false;
            }
            if final_turn {
                turns.complete = true;
                if turn.turn_is_formatted {
                    turns.finalize_pending = false;
                }
            }
        }
    }
    Ok(false)
}
fn detected_language(code: Option<&str>) -> String {
    let code = code.unwrap_or("en").to_ascii_lowercase();
    // Source Language enum accepts more languages than the model's declared set.
    const KNOWN: &[&str] = &[
        "af", "am", "ar", "ar-001", "as", "ast", "az", "ba", "be", "bg", "bn", "bo", "br", "brx",
        "bs", "ca", "ceb", "ckb", "cmn", "cs", "cy", "da", "de", "doi", "el", "en", "eo", "es",
        "es-419", "et", "eu", "fa", "ff", "fi", "fil", "fo", "fr", "ga", "gd", "gl", "gu", "ha",
        "haw", "he", "hi", "hr", "ht", "hu", "hy", "id", "ig", "is", "it", "ja", "jv", "jw", "ka",
        "kab", "kea", "kk", "km", "kn", "ko", "kok", "ks", "ku", "ky", "la", "lb", "lg", "ln",
        "lo", "lt", "luo", "lv", "mai", "mg", "mi", "mk", "ml", "mn", "mni", "mr", "ms", "mt",
        "my", "mymr", "nb", "ne", "nl", "nn", "no", "nso", "ny", "oc", "or", "pa", "pl", "ps",
        "pt", "ro", "ru", "sa", "sat", "sd", "si", "sk", "sl", "sn", "so", "sq", "sr", "su", "sv",
        "sw", "ta", "te", "tg", "th", "tk", "tl", "tr", "tt", "ug", "uk", "umb", "ur", "uz", "vi",
        "wo", "wuu", "yi", "yo", "yue", "zh", "zu",
    ];
    if KNOWN.contains(&code.as_str()) {
        code
    } else {
        "en".into()
    }
}

fn speaker_text(
    format: &str,
    speaker: &str,
    text: &str,
    limit: usize,
) -> Result<String, StreamingError> {
    let bound = format
        .len()
        .saturating_add(
            format
                .matches("{speaker}")
                .count()
                .saturating_mul(speaker.len()),
        )
        .saturating_add(format.matches("{text}").count().saturating_mul(text.len()));
    if bound > limit {
        return Err(StreamingError::TooLarge);
    }
    let mut result = String::with_capacity(bound);
    let mut tail = format;
    while let Some(index) = tail.find('{') {
        result.push_str(&tail[..index]);
        let token = &tail[index..];
        if let Some(rest) = token.strip_prefix("{speaker}") {
            result.push_str(speaker);
            tail = rest;
        } else if let Some(rest) = token.strip_prefix("{text}") {
            result.push_str(text);
            tail = rest;
        } else {
            unreachable!("validated speaker format");
        }
    }
    result.push_str(tail);
    Ok(result)
}
