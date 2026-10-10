//! Realtime Scribe JSON audio, manual/VAD commits and paired final transcripts.
use crate::common::{self, EventStream, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits};
use base64::{engine::general_purpose::STANDARD, Engine};
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CommitStrategy {
    Manual,
    Vad,
}
#[derive(Clone, PartialEq)]
pub struct ElevenLabsSttConfig {
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    /// ISO 639-1, ISO 639-3 or BCP-47 language, normalized to source mapping.
    pub language: Option<String>,
    pub sample_rate: u32,
    pub commit_strategy: CommitStrategy,
    pub include_timestamps: bool,
    pub include_language_detection: bool,
    pub enable_logging: bool,
    pub keyterms: Vec<String>,
    pub no_verbatim: Option<bool>,
    pub filter_background_audio: Option<bool>,
    pub vad_silence_threshold_secs: Option<f64>,
    pub vad_threshold: Option<f64>,
    pub min_speech_duration_ms: Option<u32>,
    pub min_silence_duration_ms: Option<u32>,
    pub keepalive_interval: Duration,
    pub max_audio_chunk_bytes: usize,
    pub max_session_audio_bytes: usize,
    pub limits: StreamingLimits,
}
impl fmt::Debug for ElevenLabsSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ElevenLabsSttConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("commit_strategy", &self.commit_strategy)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl ElevenLabsSttConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.elevenlabs.io/v1/speech-to-text/realtime".into(),
            api_key: api_key.into(),
            model: "scribe_v2_realtime".into(),
            language: None,
            sample_rate: 16000,
            commit_strategy: CommitStrategy::Manual,
            include_timestamps: false,
            include_language_detection: false,
            enable_logging: false,
            keyterms: vec![],
            no_verbatim: None,
            filter_background_audio: None,
            vad_silence_threshold_secs: None,
            vad_threshold: None,
            min_speech_duration_ms: None,
            min_silence_duration_ms: None,
            keepalive_interval: Duration::from_secs(5),
            max_audio_chunk_bytes: 128 * 1024,
            max_session_audio_bytes: 512 * 1024 * 1024,
            limits: StreamingLimits::default(),
        }
    }
    pub fn websocket_url(&self) -> Result<String, StreamingError> {
        common::validate_limits(&self.limits)?;
        if self.api_key.is_empty()
            || self.model.is_empty()
            || self.model.len() > 256
            || !matches!(
                self.sample_rate,
                8000 | 16000 | 22050 | 24000 | 44100 | 48000
            )
            || self.keepalive_interval.is_zero()
            || self.max_audio_chunk_bytes == 0
            || self.max_session_audio_bytes == 0
            || self.keyterms.len() > 100
            || self.keyterms.iter().any(|s| s.is_empty() || s.len() > 1024)
        {
            return Err(StreamingError::Invalid("ElevenLabs STT configuration"));
        }
        for (value, min, max) in [
            (self.vad_threshold, 0.1, 0.9),
            (self.vad_silence_threshold_secs, 0.3, 3.0),
        ] {
            if value.is_some_and(|v| !v.is_finite() || v < min || v > max) {
                return Err(StreamingError::Invalid("VAD threshold range"));
            }
        }
        if [self.min_speech_duration_ms, self.min_silence_duration_ms]
            .into_iter()
            .flatten()
            .any(|v| !(50..=2000).contains(&v))
        {
            return Err(StreamingError::Invalid("VAD duration range"));
        }
        let mut url = common::endpoint(&self.endpoint)?;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("model_id", &self.model)
                .append_pair("audio_format", &format!("pcm_{}", self.sample_rate))
                .append_pair(
                    "commit_strategy",
                    if self.commit_strategy == CommitStrategy::Manual {
                        "manual"
                    } else {
                        "vad"
                    },
                );
            if let Some(language) = &self.language {
                q.append_pair("language_code", &crate::settings::stt_language(language)?);
            }
            for keyterm in &self.keyterms {
                q.append_pair("keyterms", keyterm);
            }
            for (key, value) in [
                ("include_timestamps", Some(self.include_timestamps)),
                (
                    "include_language_detection",
                    Some(self.include_language_detection),
                ),
                ("enable_logging", Some(self.enable_logging)),
                ("no_verbatim", self.no_verbatim),
                ("filter_background_audio", self.filter_background_audio),
            ] {
                if let Some(value) = value {
                    q.append_pair(key, if value { "true" } else { "false" });
                }
            }
            if self.commit_strategy == CommitStrategy::Vad {
                for (key, value) in [
                    ("vad_threshold", self.vad_threshold),
                    (
                        "vad_silence_threshold_secs",
                        self.vad_silence_threshold_secs,
                    ),
                ] {
                    if let Some(value) = value {
                        q.append_pair(key, &value.to_string());
                    }
                }
                for (key, value) in [
                    ("min_speech_duration_ms", self.min_speech_duration_ms),
                    ("min_silence_duration_ms", self.min_silence_duration_ms),
                ] {
                    if let Some(value) = value {
                        q.append_pair(key, &value.to_string());
                    }
                }
            }
        }
        let url: String = url.into();
        if url.len() > self.limits.max_message_bytes {
            return Err(StreamingError::TooLarge);
        }
        Ok(url)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ElevenLabsTranscript {
    pub text: String,
    pub is_final: bool,
    pub finalized: bool,
    pub language: Option<String>,
    pub result: Value,
}
#[derive(Debug, Clone, PartialEq)]
pub enum ElevenLabsSttEvent {
    SessionStarted(Value),
    Transcript(ElevenLabsTranscript),
    Committed,
}
#[derive(Debug, Clone)]
pub struct ElevenLabsSttClient {
    config: ElevenLabsSttConfig,
}
impl ElevenLabsSttClient {
    pub fn new(config: ElevenLabsSttConfig) -> Result<Self, StreamingError> {
        common::request(&config.websocket_url()?, &[("xi-api-key", &config.api_key)])?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &ElevenLabsSttConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<ElevenLabsSttSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<ElevenLabsSttSession, StreamingError> {
        let request = common::request(
            &self.config.websocket_url()?,
            &[("xi-api-key", &self.config.api_key)],
        )?;
        let mut socket =
            common::cancelled(&cancel, common::connect(request, &self.config.limits)).await?;
        let config = self.config.clone();
        let (tx, incoming) = mpsc::channel(config.limits.queue_capacity);
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
                common::cancelled(&worker_cancel, run(&mut socket, incoming, &events, &config))
                    .await;
            common::close(&mut socket, &config.limits).await;
            result
        });
        Ok(ElevenLabsSttSession {
            sender: ElevenLabsSttSender {
                input,
                config: self.config.clone(),
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
    Commit,
    Finish,
}
#[derive(Clone)]
pub struct ElevenLabsSttSender {
    input: Input<Command>,
    config: ElevenLabsSttConfig,
}
impl ElevenLabsSttSender {
    fn audio(&self, audio: AudioRawFrame) -> Result<Command, StreamingError> {
        audio
            .validate()
            .map_err(|_| StreamingError::Invalid("incomplete PCM16 samples"))?;
        if audio.sample_rate != self.config.sample_rate || audio.num_channels != 1 {
            return Err(StreamingError::Invalid(
                "STT requires configured mono PCM16",
            ));
        }
        if audio.audio.is_empty() {
            return Err(StreamingError::Invalid("empty STT audio"));
        }
        if audio.audio.len() > self.config.max_audio_chunk_bytes
            || audio.audio.len() > self.config.limits.max_message_bytes
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(Command::Audio(audio.audio))
    }
    pub async fn send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.input.send(self.audio(audio)?, false).await
    }
    pub fn try_send_audio(&self, audio: AudioRawFrame) -> Result<(), StreamingError> {
        self.input.try_send(self.audio(audio)?, false)
    }
    pub async fn commit(&self) -> Result<(), StreamingError> {
        self.manual()?;
        self.input.send(Command::Commit, false).await
    }
    pub fn try_commit(&self) -> Result<(), StreamingError> {
        self.manual()?;
        self.input.try_send(Command::Commit, false)
    }
    fn manual(&self) -> Result<(), StreamingError> {
        if self.config.commit_strategy != CommitStrategy::Manual {
            Err(StreamingError::Invalid(
                "manual commit is disabled in VAD mode",
            ))
        } else {
            Ok(())
        }
    }
    /// Manual mode commits pending audio and drains all outstanding commits.
    /// VAD mode awaits its last detected segment; absent final yields a timeout.
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
pub struct ElevenLabsSttSession {
    sender: ElevenLabsSttSender,
    events: EventStream<ElevenLabsSttEvent>,
}
impl ElevenLabsSttSession {
    pub fn sender(&self) -> ElevenLabsSttSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<ElevenLabsSttEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}
async fn audio_message(
    socket: &mut Socket,
    audio: &[u8],
    commit: bool,
    config: &ElevenLabsSttConfig,
) -> Result<(), StreamingError> {
    let text=json!({"message_type":"input_audio_chunk","audio_base_64":STANDARD.encode(audio),"commit":commit,"sample_rate":config.sample_rate}).to_string();
    if text.len() > config.limits.max_message_bytes {
        return Err(StreamingError::TooLarge);
    }
    common::write(socket, Message::Text(text.into()), &config.limits).await
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<ElevenLabsSttEvent>,
    config: &ElevenLabsSttConfig,
) -> Result<(), StreamingError> {
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + config.keepalive_interval,
        config.keepalive_interval,
    );
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut closing = None;
    let mut pending = 0usize;
    let mut dirty = false;
    let mut total_audio = 0usize;
    let mut last_audio = tokio::time::Instant::now();
    loop {
        let deadline = async {
            match closing {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _=events.closed()=>return Err(StreamingError::Closed),
            _=deadline=>return Err(StreamingError::Timeout("ElevenLabs STT final response")),
            incoming=socket.next()=>match incoming {
                Some(Ok(Message::Text(text)))=>{
                    let value:Value=serde_json::from_str(&text).map_err(|_|StreamingError::Protocol("ElevenLabs STT JSON"))?;
                    for event in decode(value,config)? {
                        if matches!(event,ElevenLabsSttEvent::Committed) {pending=pending.saturating_sub(1);if config.commit_strategy==CommitStrategy::Vad {dirty=false;}}
                        tokio::time::timeout(config.limits.write_timeout,events.send(event)).await.map_err(|_|StreamingError::Timeout("ElevenLabs STT output queue"))?.map_err(|_|StreamingError::Closed)?;
                    }
                    if closing.is_some()&&pending==0&&!dirty {return Ok(());}
                },
                Some(Ok(Message::Ping(bytes)))=>common::write(socket,Message::Pong(bytes),&config.limits).await?,
                Some(Ok(Message::Pong(_)))=>{},
                Some(Ok(Message::Close(_)))|None=>return Err(StreamingError::Closed),
                Some(Ok(_))=>return Err(StreamingError::Protocol("ElevenLabs STT non-text response")),
                Some(Err(error))=>return Err(common::transport(error)),
            },
            command=commands.recv(),if closing.is_none()=>match command {
                Some(Command::Audio(audio))=>{
                    total_audio=total_audio.checked_add(audio.len()).ok_or(StreamingError::TooLarge)?;
                    if total_audio>config.max_session_audio_bytes {return Err(StreamingError::TooLarge);}
                    audio_message(socket,&audio,false,config).await?;dirty=true;last_audio=tokio::time::Instant::now();
                },
                Some(Command::Commit)=>{if pending>=config.limits.queue_capacity {return Err(StreamingError::Backpressure);}audio_message(socket,&[],true,config).await?;dirty=false;pending+=1;},
                Some(Command::Finish)|None=>{
                    commands.close();
                    if dirty&&config.commit_strategy==CommitStrategy::Manual {audio_message(socket,&[],true,config).await?;dirty=false;pending+=1;}
                    if !dirty&&pending==0 {return Ok(());}
                    closing=Some(tokio::time::Instant::now()+config.limits.response_timeout);
                }
            },
            _=keepalive.tick(),if closing.is_none()=>{
                if last_audio.elapsed()>=config.keepalive_interval {
                    // Round the 10 ms sample count before converting to bytes.
                    // 22050 / 50 bytes would otherwise split a PCM16 sample.
                    let silence=vec![0;(config.sample_rate as usize/100)*2];
                    audio_message(socket,&silence,false,config).await?;
                    last_audio=tokio::time::Instant::now();
                }
            }
        }
    }
}
fn decode(
    value: Value,
    config: &ElevenLabsSttConfig,
) -> Result<Vec<ElevenLabsSttEvent>, StreamingError> {
    let kind = value
        .get("message_type")
        .and_then(Value::as_str)
        .ok_or(StreamingError::Protocol("STT message_type"))?;
    match kind {
        "session_started" => Ok(vec![ElevenLabsSttEvent::SessionStarted(value)]),
        "partial_transcript" | "committed_transcript" | "committed_transcript_with_timestamps" => {
            let final_transcript = kind != "partial_transcript";
            if kind == "committed_transcript"
                && (config.include_timestamps || config.include_language_detection)
            {
                return Ok(vec![]);
            }
            let text = value
                .get("text")
                .and_then(Value::as_str)
                .ok_or(StreamingError::Protocol("STT transcript text"))?
                .trim();
            let mut result = vec![];
            if !text.is_empty() {
                result.push(ElevenLabsSttEvent::Transcript(ElevenLabsTranscript {
                    text: text.into(),
                    is_final: final_transcript,
                    finalized: final_transcript && config.commit_strategy == CommitStrategy::Manual,
                    language: value
                        .get("language_code")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    result: value,
                }));
            }
            if final_transcript {
                result.push(ElevenLabsSttEvent::Committed);
            }
            Ok(result)
        }
        "error"
        | "auth_error"
        | "quota_exceeded"
        | "transcriber_error"
        | "input_error"
        | "invalid_request"
        | "commit_throttled"
        | "unaccepted_terms"
        | "rate_limited"
        | "queue_overflow"
        | "resource_exhausted"
        | "session_time_limit_exceeded"
        | "chunk_size_exceeded"
        | "insufficient_audio_activity" => Err(StreamingError::Provider {
            status: match kind {
                "auth_error" => Some(401),
                "quota_exceeded" | "rate_limited" | "commit_throttled" => Some(429),
                "input_error" | "invalid_request" | "chunk_size_exceeded" => Some(400),
                _ => None,
            },
        }),
        _ => Ok(vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn paired_finals_emit_once_in_either_order() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/source_contract.json")).unwrap();
        for (timestamps, language_detection) in [(true, false), (false, true), (true, true)] {
            for reverse in [false, true] {
                let mut config = ElevenLabsSttConfig::new("secret");
                config.include_timestamps = timestamps;
                config.include_language_detection = language_detection;
                let pair = if reverse {
                    vec![
                        fixture["stt_timestamped"].clone(),
                        fixture["stt_plain"].clone(),
                    ]
                } else {
                    vec![
                        fixture["stt_plain"].clone(),
                        fixture["stt_timestamped"].clone(),
                    ]
                };
                let events: Vec<_> = pair
                    .into_iter()
                    .flat_map(|v| decode(v, &config).unwrap())
                    .collect();
                let transcripts: Vec<_> = events
                    .iter()
                    .filter_map(|e| {
                        if let ElevenLabsSttEvent::Transcript(t) = e {
                            Some(t)
                        } else {
                            None
                        }
                    })
                    .collect();
                assert_eq!(transcripts.len(), 1);
                assert_eq!(transcripts[0].language.as_deref(), Some("en"));
                assert!(transcripts[0].finalized);
                assert_eq!(
                    events
                        .iter()
                        .filter(|e| matches!(e, ElevenLabsSttEvent::Committed))
                        .count(),
                    1
                );
            }
        }
    }
    #[test]
    fn vad_final_is_segment_final_but_not_turn_finalized() {
        let mut config = ElevenLabsSttConfig::new("secret");
        config.commit_strategy = CommitStrategy::Vad;
        let events = decode(
            json!({"message_type":"committed_transcript","text":" hi "}),
            &config,
        )
        .unwrap();
        let ElevenLabsSttEvent::Transcript(t) = &events[0] else {
            panic!()
        };
        assert_eq!(t.text, "hi");
        assert!(t.is_final);
        assert!(!t.finalized);
        assert!(matches!(
            decode(
                json!({"message_type":"committed_transcript","text":" "}),
                &config
            )
            .unwrap()
            .as_slice(),
            [ElevenLabsSttEvent::Committed]
        ));
    }
    #[test]
    fn all_source_errors_fail_closed_without_echoing_provider_text() {
        let config = ElevenLabsSttConfig::new("secret");
        for kind in [
            "error",
            "auth_error",
            "quota_exceeded",
            "transcriber_error",
            "input_error",
            "invalid_request",
            "commit_throttled",
            "unaccepted_terms",
            "rate_limited",
            "queue_overflow",
            "resource_exhausted",
            "session_time_limit_exceeded",
            "chunk_size_exceeded",
            "insufficient_audio_activity",
        ] {
            let error = decode(json!({"message_type":kind,"error":"secret"}), &config).unwrap_err();
            assert!(matches!(error, StreamingError::Provider { .. }));
            assert!(!error.to_string().contains("secret"));
        }
    }
}
