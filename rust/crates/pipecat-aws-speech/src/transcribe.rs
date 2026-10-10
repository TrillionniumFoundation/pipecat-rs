//! Transcribe streaming over freshly presigned WebSockets and binary eventstream.
use crate::{
    auth,
    common::{self, EventStream, Input, Socket},
    eventstream, AwsCredentials, CancellationToken, PresignedUrl, StreamingError, StreamingLimits,
};
use futures_util::StreamExt;
use pipecat_audio::SincResampler;
use pipecat_core::AudioRawFrame;
use serde_json::Value;
use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, Mutex},
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::{protocol::frame::coding::CloseCode, Message};
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartialResultsStability {
    High,
    Medium,
    Low,
}
impl PartialResultsStability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::High => "high",
            Self::Medium => "medium",
            Self::Low => "low",
        }
    }
}
#[derive(Clone, PartialEq)]
pub struct TranscribeConfig {
    pub credentials: AwsCredentials,
    pub region: String,
    pub endpoint: String,
    pub language: String,
    /// Input PCM rate. 8 kHz stays 8 kHz; other rates are resampled to 16 kHz.
    pub sample_rate: u32,
    pub partial_results_stability: PartialResultsStability,
    pub max_audio_chunk_bytes: usize,
    pub max_session_audio_bytes: usize,
    pub limits: StreamingLimits,
}
impl fmt::Debug for TranscribeConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TranscribeConfig")
            .field("credentials", &self.credentials)
            .field("region", &self.region)
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl TranscribeConfig {
    pub fn new(credentials: AwsCredentials, region: impl Into<String>) -> Self {
        let region = region.into();
        Self {
            endpoint: format!(
                "wss://transcribestreaming.{region}.{}:8443/stream-transcription-websocket",
                auth::domain(&region)
            ),
            region,
            credentials,
            language: "en-US".into(),
            sample_rate: 16000,
            partial_results_stability: PartialResultsStability::High,
            max_audio_chunk_bytes: 128 * 1024,
            max_session_audio_bytes: 512 * 1024 * 1024,
            limits: StreamingLimits::default(),
        }
    }
    pub fn wire_sample_rate(&self) -> u32 {
        if self.sample_rate == 8000 {
            8000
        } else {
            16000
        }
    }
    pub fn validate(&self) -> Result<(), StreamingError> {
        common::validate_limits(&self.limits)?;
        auth::region(&self.region)?;
        if self.language.is_empty()
            || self.language.len() > 64
            || !self
                .language
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !(8000..=48000).contains(&self.sample_rate)
            || self.max_audio_chunk_bytes == 0
            || self.max_audio_chunk_bytes > 1024 * 1024
            || self.max_session_audio_bytes == 0
            || self.max_session_audio_bytes > 2 * 1024 * 1024 * 1024usize
            || self.limits.max_message_bytes < 128
            || self.limits.max_message_bytes > 16 * 1024 * 1024
        {
            return Err(StreamingError::Invalid("Transcribe configuration"));
        }
        self.url()?;
        Ok(())
    }
    fn url(&self) -> Result<url::Url, StreamingError> {
        auth::endpoint(
            &self.endpoint,
            &format!(
                "wss://transcribestreaming.{}.{}:8443/stream-transcription-websocket",
                self.region,
                auth::domain(&self.region)
            ),
            "/stream-transcription-websocket",
            true,
        )
    }
    /// Creates an expiring capability. Do not log, persist, or share the returned URL.
    pub fn presigned_url_at(&self, timestamp: &str) -> Result<PresignedUrl, StreamingError> {
        self.validate()?;
        let params = BTreeMap::from([
            (
                "language-code".into(),
                crate::transcribe_language(&self.language).into(),
            ),
            ("media-encoding".into(), "pcm".into()),
            ("sample-rate".into(), self.wire_sample_rate().to_string()),
            ("enable-partial-results-stabilization".into(), "true".into()),
            (
                "partial-results-stability".into(),
                self.partial_results_stability.as_str().into(),
            ),
        ]);
        auth::presign(
            &self.url()?,
            &self.region,
            &self.credentials,
            params,
            timestamp,
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct TranscribeTranscript {
    pub text: String,
    pub is_final: bool,
    pub language: Option<String>,
    pub result: Value,
}
#[derive(Debug, Clone, PartialEq)]
pub enum TranscribeEvent {
    Transcript(TranscribeTranscript),
}
#[derive(Debug, Clone)]
pub struct TranscribeClient {
    config: TranscribeConfig,
}
impl TranscribeClient {
    pub fn new(config: TranscribeConfig) -> Result<Self, StreamingError> {
        config.validate()?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &TranscribeConfig {
        &self.config
    }
    pub async fn connect(&self) -> Result<TranscribeSession, StreamingError> {
        self.connect_with_cancel(CancellationToken::new()).await
    }
    pub async fn connect_with_cancel(
        &self,
        cancel: CancellationToken,
    ) -> Result<TranscribeSession, StreamingError> {
        let url = self.config.presigned_url_at(&auth::now())?;
        let request = common::request(url.as_str(), &[("origin", "https://localhost")])?;
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
        Ok(TranscribeSession {
            sender: TranscribeSender {
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
    Finish,
}
#[derive(Clone)]
pub struct TranscribeSender {
    input: Input<Command>,
    config: TranscribeConfig,
}
impl TranscribeSender {
    fn audio(&self, audio: AudioRawFrame) -> Result<Command, StreamingError> {
        audio
            .validate()
            .map_err(|_| StreamingError::Invalid("incomplete PCM16 samples"))?;
        if audio.sample_rate != self.config.sample_rate
            || audio.num_channels != 1
            || audio.audio.is_empty()
        {
            return Err(StreamingError::Invalid(
                "Transcribe requires configured mono PCM16",
            ));
        }
        if audio.audio.len() > self.config.max_audio_chunk_bytes {
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
    /// Sends a zero-payload AudioEvent, then drains transcript events until normal close.
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
pub struct TranscribeSession {
    sender: TranscribeSender,
    events: EventStream<TranscribeEvent>,
}
impl TranscribeSession {
    pub fn sender(&self) -> TranscribeSender {
        self.sender.clone()
    }
    pub async fn next(&mut self) -> Option<Result<TranscribeEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}
async fn audio(
    socket: &mut Socket,
    pcm: &[u8],
    config: &TranscribeConfig,
) -> Result<(), StreamingError> {
    // 100 ms chunks remain beneath the one-second AudioEvent limit.
    let size = ((config.wire_sample_rate() as usize / 10) * 2)
        .min((config.limits.max_message_bytes - 104) & !1);
    if size == 0 {
        return Err(StreamingError::Invalid("Transcribe message limit"));
    }
    for chunk in pcm.chunks(size) {
        common::write(
            socket,
            Message::Binary(eventstream::audio(chunk, config.limits.max_message_bytes)?.into()),
            &config.limits,
        )
        .await?;
    }
    Ok(())
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<TranscribeEvent>,
    config: &TranscribeConfig,
) -> Result<(), StreamingError> {
    let mut resampler = SincResampler::new(config.sample_rate, config.wire_sample_rate(), 1)
        .map_err(|_| StreamingError::Invalid("Transcribe resampling"))?;
    let mut closing = None;
    let mut total = 0usize;
    loop {
        let deadline = async {
            match closing {
                Some(deadline) => tokio::time::sleep_until(deadline).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            _=events.closed()=>return Err(StreamingError::Closed),
            _=deadline=>return Err(StreamingError::Timeout("Transcribe final response")),
            incoming=socket.next()=>match incoming {
                Some(Ok(Message::Binary(bytes)))=>{
                    for event in decode(&bytes,config)? {
                        tokio::time::timeout(config.limits.write_timeout,events.send(event)).await.map_err(|_|StreamingError::Timeout("Transcribe output queue"))?.map_err(|_|StreamingError::Closed)?;
                    }
                },
                Some(Ok(Message::Ping(bytes)))=>common::write(socket,Message::Pong(bytes),&config.limits).await?,
                Some(Ok(Message::Pong(_)))=>{},
                Some(Ok(Message::Close(frame)))=>return if closing.is_some()&&frame.as_ref().is_none_or(|f|f.code==CloseCode::Normal){Ok(())}else{Err(StreamingError::Closed)},
                None=>return Err(StreamingError::Closed),
                Some(Ok(_))=>return Err(StreamingError::Protocol("Transcribe expects binary eventstream")),
                Some(Err(error))=>return Err(common::transport(error)),
            },
            command=commands.recv(),if closing.is_none()=>match command {
                Some(Command::Audio(bytes))=>{
                    total=total.checked_add(bytes.len()).ok_or(StreamingError::TooLarge)?;
                    if total>config.max_session_audio_bytes {return Err(StreamingError::TooLarge);}
                    let pcm=resampler.process(&bytes).map_err(|_|StreamingError::Protocol("Transcribe PCM"))?;
                    audio(socket,&pcm,config).await?;
                },
                Some(Command::Finish)|None=>{
                    commands.close();
                    audio(socket,&resampler.flush().map_err(|_|StreamingError::PartialSample)?,config).await?;
                    common::write(socket,Message::Binary(eventstream::audio(&[],config.limits.max_message_bytes)?.into()),&config.limits).await?;
                    closing=Some(tokio::time::Instant::now()+config.limits.response_timeout);
                },
            }
        }
    }
}
fn decode(bytes: &[u8], config: &TranscribeConfig) -> Result<Vec<TranscribeEvent>, StreamingError> {
    let event = eventstream::decode(bytes, config.limits.max_message_bytes)?;
    match event.header(":message-type") {
        Some("exception") | Some("error") => Err(StreamingError::Provider {
            status: match event
                .header(":exception-type")
                .or_else(|| event.header(":event-type"))
            {
                Some("BadRequestException") => Some(400),
                Some("UnrecognizedClientException") => Some(403),
                Some("LimitExceededException") => Some(429),
                Some("InternalFailureException" | "ServiceUnavailableException") => Some(503),
                _ => None,
            },
        }),
        Some("event") => {
            if event.header(":event-type") != Some("TranscriptEvent")
                || event.header(":content-type") != Some("application/json")
            {
                return Err(StreamingError::Protocol("Transcribe event type"));
            }
            let value: Value = serde_json::from_slice(event.payload)
                .map_err(|_| StreamingError::Protocol("Transcribe JSON"))?;
            let results = value
                .get("Transcript")
                .and_then(|v| v.get("Results"))
                .and_then(Value::as_array)
                .ok_or(StreamingError::Protocol("Transcribe results"))?;
            if results.len() > 128 {
                return Err(StreamingError::TooLarge);
            }
            let mut output = Vec::new();
            for result in results {
                let partial = result
                    .get("IsPartial")
                    .and_then(Value::as_bool)
                    .ok_or(StreamingError::Protocol("Transcribe partial flag"))?;
                let alternatives = result
                    .get("Alternatives")
                    .and_then(Value::as_array)
                    .ok_or(StreamingError::Protocol("Transcribe alternatives"))?;
                let Some(first) = alternatives.first() else {
                    continue;
                };
                let text = first
                    .get("Transcript")
                    .and_then(Value::as_str)
                    .ok_or(StreamingError::Protocol("Transcribe transcript"))?;
                if !text.is_empty() {
                    output.push(TranscribeEvent::Transcript(TranscribeTranscript {
                        text: text.into(),
                        is_final: !partial,
                        language: Some(crate::transcribe_language(&config.language).into()),
                        result: result.clone(),
                    }));
                }
            }
            Ok(output)
        }
        _ => Err(StreamingError::Protocol("Transcribe message type")),
    }
}
