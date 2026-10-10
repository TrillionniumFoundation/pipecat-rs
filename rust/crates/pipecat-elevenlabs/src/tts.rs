//! Single-context subset of the source `multi-stream-input` TTS protocol.
use crate::common::{self, EventStream, Input, Socket};
use crate::{CancellationToken, StreamingError, StreamingLimits, WordTimestamp};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
use pipecat_core::AudioRawFrame;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
    fmt,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VoiceSettings {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stability: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub similarity_boost: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub style: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_speaker_boost: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,
}
impl VoiceSettings {
    fn validate(&self) -> Result<(), StreamingError> {
        for value in [self.stability, self.similarity_boost, self.style]
            .into_iter()
            .flatten()
        {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(StreamingError::Invalid("voice setting must be in 0..=1"));
            }
        }
        if self
            .speed
            .is_some_and(|n| !n.is_finite() || !(0.7..=1.2).contains(&n))
        {
            return Err(StreamingError::Invalid(
                "WebSocket voice speed must be in 0.7..=1.2",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, PartialEq)]
pub struct ElevenLabsTtsConfig {
    /// API origin; the voice-specific multi-stream-input path is appended safely.
    pub endpoint: String,
    pub api_key: String,
    pub model: String,
    pub voice_id: String,
    /// Provider-native base language code. Unsupported model/language pairs error.
    pub language: Option<String>,
    pub sample_rate: u32,
    pub voice_settings: VoiceSettings,
    pub auto_mode: bool,
    pub enable_logging: Option<bool>,
    pub apply_text_normalization: Option<String>,
    pub keepalive_interval: Duration,
    pub max_text_bytes: usize,
    pub max_audio_bytes: usize,
    pub limits: StreamingLimits,
}
impl fmt::Debug for ElevenLabsTtsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ElevenLabsTtsConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl ElevenLabsTtsConfig {
    pub fn new(api_key: impl Into<String>, voice_id: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.elevenlabs.io".into(),
            api_key: api_key.into(),
            voice_id: voice_id.into(),
            model: "eleven_flash_v2_5".into(),
            language: None,
            sample_rate: 24000,
            voice_settings: VoiceSettings::default(),
            auto_mode: true,
            enable_logging: None,
            apply_text_normalization: None,
            keepalive_interval: Duration::from_secs(10),
            max_text_bytes: 128 * 1024,
            max_audio_bytes: 64 * 1024 * 1024,
            limits: StreamingLimits::default(),
        }
    }
    pub fn websocket_url(&self) -> Result<String, StreamingError> {
        common::validate_limits(&self.limits)?;
        self.voice_settings.validate()?;
        if self.api_key.is_empty()
            || self.model.is_empty()
            || self.model.len() > 256
            || self.voice_id.is_empty()
            || self.voice_id.len() > 256
            || self
                .voice_id
                .chars()
                .any(|c| c.is_control() || "/?#%".contains(c))
            || !matches!(
                self.sample_rate,
                8000 | 16000 | 22050 | 24000 | 32000 | 44100 | 48000
            )
            || self.max_text_bytes == 0
            || self.max_audio_bytes == 0
            || self.keepalive_interval.is_zero()
        {
            return Err(StreamingError::Invalid(
                "ElevenLabs key, model, voice, PCM rate or limits",
            ));
        }
        if self
            .apply_text_normalization
            .as_deref()
            .is_some_and(|s| !matches!(s, "auto" | "on" | "off"))
        {
            return Err(StreamingError::Invalid("text normalization"));
        }
        if let Some(language) = &self.language {
            if !crate::settings::tts_language_supported(&self.model, language) {
                return Err(StreamingError::Invalid(
                    "unsupported TTS model/language pair",
                ));
            }
        }
        let mut url = common::endpoint(&self.endpoint)?;
        if url.path() != "/" && !url.path().is_empty() {
            return Err(StreamingError::Invalid("TTS endpoint must be an origin"));
        }
        url.path_segments_mut()
            .map_err(|_| StreamingError::Invalid("TTS origin"))?
            .clear()
            .extend(["v1", "text-to-speech", &self.voice_id, "multi-stream-input"]);
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("model_id", &self.model)
                .append_pair("output_format", &format!("pcm_{}", self.sample_rate))
                .append_pair("auto_mode", if self.auto_mode { "true" } else { "false" });
            if let Some(language) = &self.language {
                q.append_pair("language_code", language);
            }
            if let Some(logging) = self.enable_logging {
                q.append_pair("enable_logging", if logging { "true" } else { "false" });
            }
            if let Some(normalization) = &self.apply_text_normalization {
                q.append_pair("apply_text_normalization", normalization);
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
pub enum ElevenLabsTtsEvent {
    Audio(AudioRawFrame),
    WordTimestamps {
        context_id: String,
        words: Vec<WordTimestamp>,
    },
    Done {
        context_id: String,
    },
}
#[derive(Debug, Clone)]
pub struct ElevenLabsTtsClient {
    config: ElevenLabsTtsConfig,
}
impl ElevenLabsTtsClient {
    pub fn new(config: ElevenLabsTtsConfig) -> Result<Self, StreamingError> {
        common::request(&config.websocket_url()?, &[("xi-api-key", &config.api_key)])?;
        Ok(Self { config })
    }
    pub fn config(&self) -> &ElevenLabsTtsConfig {
        &self.config
    }
    pub fn stream(
        &self,
        context_id: impl Into<String>,
    ) -> Result<ElevenLabsTtsStream, StreamingError> {
        self.stream_with_cancel(context_id, CancellationToken::new())
    }
    pub fn stream_with_cancel(
        &self,
        context_id: impl Into<String>,
        cancel: CancellationToken,
    ) -> Result<ElevenLabsTtsStream, StreamingError> {
        let context_id = context_id.into();
        if context_id.is_empty()
            || context_id.len() > 256
            || context_id.chars().any(char::is_control)
        {
            return Err(StreamingError::Invalid("ElevenLabs context ID"));
        }
        let handle = tokio::runtime::Handle::try_current()
            .map_err(|_| StreamingError::Invalid("active Tokio runtime required"))?;
        let request = common::request(
            &self.config.websocket_url()?,
            &[("xi-api-key", &self.config.api_key)],
        )?;
        let (tx, incoming) = mpsc::channel(self.config.limits.queue_capacity);
        let (events, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let input = Input {
            tx,
            cancel: cancel.clone(),
            limits: self.config.limits.clone(),
            finished: Arc::new(Mutex::new(false)),
        };
        let config = self.config.clone();
        let worker_cancel = cancel.clone();
        let context = context_id.clone();
        let task = handle.spawn(async move {
            let mut socket =
                common::cancelled(&worker_cancel, common::connect(request, &config.limits)).await?;
            let result = common::cancelled(
                &worker_cancel,
                run(&mut socket, incoming, &events, &config, &context),
            )
            .await;
            cleanup(&mut socket, &config, &context, result.is_err()).await;
            result
        });
        Ok(ElevenLabsTtsStream {
            sender: ElevenLabsTtsSender {
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
    Flush,
    Finish,
}
#[derive(Clone)]
pub struct ElevenLabsTtsSender {
    input: Input<Command>,
    context_id: String,
    max_text_bytes: usize,
}
impl ElevenLabsTtsSender {
    pub fn context_id(&self) -> &str {
        &self.context_id
    }
    fn text(&self, text: String) -> Result<Command, StreamingError> {
        if text.is_empty() {
            return Err(StreamingError::Invalid(
                "empty TTS text is a control message",
            ));
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
    /// Force generation without closing the context. There is no flush acknowledgement.
    pub async fn flush(&self) -> Result<(), StreamingError> {
        self.input.send(Command::Flush, false).await
    }
    /// Drain queued text, flush it, close the context, and await provider isFinal.
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
pub struct ElevenLabsTtsStream {
    sender: ElevenLabsTtsSender,
    events: EventStream<ElevenLabsTtsEvent>,
}
impl ElevenLabsTtsStream {
    pub fn sender(&self) -> ElevenLabsTtsSender {
        self.sender.clone()
    }
    pub fn context_id(&self) -> &str {
        self.sender.context_id()
    }
    pub async fn next(&mut self) -> Option<Result<ElevenLabsTtsEvent, StreamingError>> {
        self.events.next().await
    }
    pub fn cancel(&self) {
        self.events.cancel.cancel();
    }
}
async fn send(
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
async fn cleanup(socket: &mut Socket, config: &ElevenLabsTtsConfig, context: &str, abort: bool) {
    let _ = tokio::time::timeout(config.limits.close_timeout, async {
        if abort {
            let _ = send(
                socket,
                json!({"context_id":context,"close_context":true}),
                &config.limits,
            )
            .await;
        }
        if send(socket, json!({"close_socket":true}), &config.limits)
            .await
            .is_ok()
        {
            while let Some(message) = socket.next().await {
                if matches!(message, Ok(Message::Close(_)) | Err(_)) {
                    break;
                }
            }
        }
    })
    .await;
    common::close(socket, &config.limits).await;
}
async fn run(
    socket: &mut Socket,
    mut commands: mpsc::Receiver<Command>,
    events: &mpsc::Sender<ElevenLabsTtsEvent>,
    config: &ElevenLabsTtsConfig,
    context: &str,
) -> Result<(), StreamingError> {
    let mut init = json!({"text":" ","context_id":context});
    if config.voice_settings != VoiceSettings::default() {
        init["voice_settings"] = serde_json::to_value(&config.voice_settings)
            .map_err(|_| StreamingError::Invalid("voice settings"))?;
    }
    send(socket, init, &config.limits).await?;
    let mut decoder = Decoder::new(config, context);
    let mut finished = false;
    let mut total_text = 0usize;
    let mut deadline = tokio::time::Instant::now() + config.limits.response_timeout;
    let mut keepalive = tokio::time::interval_at(
        tokio::time::Instant::now() + config.keepalive_interval,
        config.keepalive_interval,
    );
    keepalive.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _=events.closed()=>return Err(StreamingError::Closed),
            _=tokio::time::sleep_until(deadline)=>return Err(StreamingError::Timeout("ElevenLabs TTS response")),
            incoming=socket.next()=>match incoming {
                Some(Ok(Message::Text(text)))=>{
                    let value:Value=serde_json::from_str(&text).map_err(|_|StreamingError::Protocol("ElevenLabs TTS JSON"))?;
                    let decoded = decoder.decode(value)?;
                    let progressed = !decoded.is_empty();
                    for event in decoded {
                        let done=matches!(event,ElevenLabsTtsEvent::Done{..});
                        if done&&!finished {return Err(StreamingError::Protocol("ElevenLabs completed before context close"));}
                        tokio::time::timeout_at(deadline,events.send(event)).await.map_err(|_|StreamingError::Timeout("ElevenLabs output queue"))?.map_err(|_|StreamingError::Closed)?;
                        if done {return Ok(());}
                    }
                    if !finished && progressed { deadline=tokio::time::Instant::now()+config.limits.response_timeout; }
                },
                Some(Ok(Message::Ping(data)))=>common::write(socket,Message::Pong(data),&config.limits).await?,
                Some(Ok(Message::Pong(_)))=>{},
                Some(Ok(Message::Close(_)))|None=>return Err(StreamingError::Protocol("ElevenLabs closed before isFinal")),
                Some(Ok(_))=>return Err(StreamingError::Protocol("ElevenLabs TTS non-text response")),
                Some(Err(error))=>return Err(common::transport(error)),
            },
            command=commands.recv(),if !finished=>{
                match command {
                    Some(Command::Text(text))=>{
                        total_text=total_text.checked_add(text.len()).ok_or(StreamingError::TooLarge)?;
                        if total_text>config.max_text_bytes {return Err(StreamingError::TooLarge);}
                        send(socket,json!({"text":text,"context_id":context}),&config.limits).await?;
                    },
                    Some(Command::Flush)=>send(socket,json!({"context_id":context,"flush":true}),&config.limits).await?,
                    Some(Command::Finish)|None=>{
                        finished=true;commands.close();
                        send(socket,json!({"context_id":context,"flush":true}),&config.limits).await?;
                        send(socket,json!({"context_id":context,"close_context":true}),&config.limits).await?;
                    }
                }
                deadline=tokio::time::Instant::now()+config.limits.response_timeout;
            },
            _=keepalive.tick(),if !finished=>send(socket,json!({"text":"","context_id":context}),&config.limits).await?,
        }
    }
}
struct Decoder<'a> {
    config: &'a ElevenLabsTtsConfig,
    context: &'a str,
    trailing: Option<u8>,
    total_audio: usize,
    alignment: AlignmentDecoder,
}
impl<'a> Decoder<'a> {
    fn new(config: &'a ElevenLabsTtsConfig, context: &'a str) -> Self {
        Self {
            config,
            context,
            trailing: None,
            total_audio: 0,
            alignment: AlignmentDecoder::default(),
        }
    }
    fn decode(&mut self, value: Value) -> Result<Vec<ElevenLabsTtsEvent>, StreamingError> {
        if value.get("error").is_some()
            || value.get("message_type").and_then(Value::as_str) == Some("error")
        {
            return Err(StreamingError::Provider {
                status: value
                    .get("code")
                    .and_then(Value::as_u64)
                    .and_then(|n| u16::try_from(n).ok()),
            });
        }
        let context = value
            .get("contextId")
            .and_then(Value::as_str)
            .ok_or(StreamingError::Protocol("ElevenLabs contextId"))?;
        if context != self.context {
            return Err(StreamingError::Protocol(
                "unexpected multiplexed TTS context",
            ));
        }
        let mut events = vec![];
        if let Some(audio) = value.get("audio").filter(|v| !v.is_null()) {
            let encoded = audio
                .as_str()
                .ok_or(StreamingError::Protocol("ElevenLabs audio string"))?;
            let bytes = STANDARD
                .decode(encoded)
                .map_err(|_| StreamingError::Protocol("ElevenLabs base64 audio"))?;
            self.total_audio = self
                .total_audio
                .checked_add(bytes.len())
                .ok_or(StreamingError::TooLarge)?;
            if self.total_audio > self.config.max_audio_bytes {
                return Err(StreamingError::TooLarge);
            }
            let mut data = Vec::with_capacity(bytes.len() + 1);
            if let Some(byte) = self.trailing.take() {
                data.push(byte);
            }
            data.extend(bytes);
            if data.len() % 2 != 0 {
                self.trailing = data.pop();
            }
            if !data.is_empty() {
                events.push(ElevenLabsTtsEvent::Audio(AudioRawFrame {
                    audio: data,
                    sample_rate: self.config.sample_rate,
                    num_channels: 1,
                    context_id: Some(self.context.into()),
                }));
            }
        }
        let mut words = vec![];
        if let Some(alignment) = value
            .get("alignment")
            .filter(|v| !v.is_null() && v.as_object().is_none_or(|o| !o.is_empty()))
            .or_else(|| value.get("normalizedAlignment").filter(|v| !v.is_null()))
        {
            words = self
                .alignment
                .decode(alignment, self.config.max_text_bytes)?;
        }
        let done = match value.get("isFinal") {
            None | Some(Value::Null) => false,
            Some(Value::Bool(done)) => *done,
            _ => return Err(StreamingError::Protocol("ElevenLabs isFinal boolean")),
        };
        if done {
            if self.trailing.is_some() {
                return Err(StreamingError::PartialSample);
            }
            if let Some(word) = self.alignment.finish() {
                words.push(word);
            }
        }
        if !words.is_empty() {
            events.push(ElevenLabsTtsEvent::WordTimestamps {
                context_id: self.context.into(),
                words,
            });
        }
        if done {
            events.push(ElevenLabsTtsEvent::Done {
                context_id: self.context.into(),
            });
        }
        Ok(events)
    }
}
/// Character alignments are chunk-relative. Pending words survive chunk splits.
#[derive(Default)]
struct AlignmentDecoder {
    offset: f64,
    partial: String,
    start: f64,
    end: f64,
    started: bool,
    total_text: usize,
}
impl AlignmentDecoder {
    fn decode(
        &mut self,
        value: &Value,
        limit: usize,
    ) -> Result<Vec<WordTimestamp>, StreamingError> {
        let chars = value
            .get("chars")
            .and_then(Value::as_array)
            .ok_or(StreamingError::Protocol("alignment chars"))?;
        let starts = value
            .get("charStartTimesMs")
            .and_then(Value::as_array)
            .ok_or(StreamingError::Protocol("alignment starts"))?;
        let durations = value
            .get("charDurationsMs")
            .and_then(Value::as_array)
            .ok_or(StreamingError::Protocol("alignment durations"))?;
        if chars.len() != starts.len() || chars.len() != durations.len() {
            return Err(StreamingError::Protocol("alignment array lengths"));
        }
        let mut words = vec![];
        let mut chunk_end = 0.0_f64;
        for (i, c) in chars.iter().enumerate() {
            let c = c
                .as_str()
                .filter(|c| c.chars().count() == 1)
                .ok_or(StreamingError::Protocol("alignment character"))?;
            let start = time(&starts[i])? / 1000.0;
            let duration = time(&durations[i])? / 1000.0;
            chunk_end = chunk_end.max(start + duration);
            self.total_text = self
                .total_text
                .checked_add(c.len())
                .ok_or(StreamingError::TooLarge)?;
            if self.total_text > limit {
                return Err(StreamingError::TooLarge);
            }
            if !self.started && c == " " {
                continue;
            }
            self.started = true;
            if c == " " {
                if let Some(word) = self.finish() {
                    words.push(word);
                }
            } else {
                if self.partial.is_empty() {
                    self.start = self.offset + start;
                }
                self.partial.push_str(c);
                self.end = self.offset + start + duration;
            }
        }
        self.offset += chunk_end;
        if !self.offset.is_finite() || self.offset > 1e9 {
            return Err(StreamingError::Protocol("alignment time overflow"));
        }
        Ok(words)
    }
    fn finish(&mut self) -> Option<WordTimestamp> {
        if self.partial.is_empty() {
            None
        } else {
            Some(WordTimestamp {
                word: std::mem::take(&mut self.partial),
                start: self.start,
                end: Some(self.end),
            })
        }
    }
}
fn time(v: &Value) -> Result<f64, StreamingError> {
    v.as_f64()
        .filter(|v| v.is_finite() && *v >= 0.0 && *v <= 1e9)
        .ok_or(StreamingError::Protocol("alignment time"))
}

#[cfg(test)]
mod tests {
    use super::*;
    fn alignment(text: &str) -> Value {
        let chars: Vec<_> = text.chars().map(|c| c.to_string()).collect();
        json!({"chars":chars,"charStartTimesMs":(0..chars.len()).map(|i|i*100).collect::<Vec<_>>(),"charDurationsMs":vec![100;chars.len()]})
    }
    #[test]
    fn source_alignment_fixtures_preserve_chunk_spaces_and_partial_words() {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/source_contract.json")).unwrap();
        for language in ["flash", "japanese", "chinese"] {
            let mut decoder = AlignmentDecoder::default();
            let mut words = vec![];
            for chunk in fixture[format!("{language}_chunks")].as_array().unwrap() {
                words.extend(
                    decoder
                        .decode(&alignment(chunk.as_str().unwrap()), 10000)
                        .unwrap(),
                );
            }
            words.extend(decoder.finish());
            assert_eq!(
                words.iter().map(|w| w.word.as_str()).collect::<Vec<_>>(),
                fixture[format!("{language}_words")]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|s| s.as_str().unwrap())
                    .collect::<Vec<_>>()
            );
            assert!(words.windows(2).all(|w| w[0].start <= w[1].start));
        }
    }
    #[test]
    fn selects_original_alignment_with_normalized_fallback_and_final_audio() {
        let config = ElevenLabsTtsConfig::new("key", "voice");
        let mut decoder = Decoder::new(&config, "ctx");
        let events=decoder.decode(json!({"contextId":"ctx","audio":"AQI=","alignment":alignment("原文 "),"normalizedAlignment":alignment("wrong ")})).unwrap();
        assert!(
            matches!(&events[1],ElevenLabsTtsEvent::WordTimestamps{words,..} if words[0].word=="原文")
        );
        let events=decoder.decode(json!({"contextId":"ctx","audio":"AwQ=","isFinal":true,"alignment":null,"normalizedAlignment":alignment("ending")})).unwrap();
        assert!(matches!(&events[0],ElevenLabsTtsEvent::Audio(a) if a.audio==[3,4]));
        assert!(
            matches!(&events[1],ElevenLabsTtsEvent::WordTimestamps{words,..} if words[0].word=="ending")
        );
        assert!(matches!(&events[2], ElevenLabsTtsEvent::Done { .. }));
    }
    #[test]
    fn malformed_alignments_audio_and_cross_context_are_rejected() {
        let config = ElevenLabsTtsConfig::new("key", "voice");
        for value in [
            json!({"contextId":"other","audio":"AQI="}),
            json!({"contextId":"ctx","audio":"bad"}),
            json!({"contextId":"ctx","alignment":{"chars":["a"],"charStartTimesMs":[],"charDurationsMs":[]}}),
            json!({"contextId":"ctx","alignment":{"chars":["a"],"charStartTimesMs":[-1],"charDurationsMs":[1]}}),
        ] {
            assert!(Decoder::new(&config, "ctx").decode(value).is_err());
        }
        let mut decoder = Decoder::new(&config, "ctx");
        decoder
            .decode(json!({"contextId":"ctx","audio":"AQ=="}))
            .unwrap();
        assert_eq!(
            decoder
                .decode(json!({"contextId":"ctx","isFinal":true}))
                .unwrap_err(),
            StreamingError::PartialSample
        );
    }
}
