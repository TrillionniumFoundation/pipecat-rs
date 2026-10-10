//! ElevenLabs HTTP speech-to-text and streaming TTS with character timestamps.
use crate::{
    common::{self, AudioDecoder, HttpClient, RawEncoding},
    ResponseStream, SpeechChunk, SpeechConfig, SpeechError,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
use pipecat_services::CancellationToken;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::mpsc;

#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TextNormalization {
    Auto,
    On,
    Off,
}
#[derive(Debug, Clone, Default, Serialize)]
pub struct ElevenLabsVoiceSettings {
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
#[derive(Debug, Clone, Serialize)]
pub struct PronunciationDictionaryLocator {
    pub pronunciation_dictionary_id: String,
    pub version_id: String,
}
#[derive(Debug, Clone)]
pub struct ElevenLabsSynthesisRequest {
    pub text: String,
    pub model: String,
    pub voice_id: String,
    /// Service-native language code, e.g. `en`. Unsupported model/language pairs
    /// are rejected instead of silently changing the requested language.
    pub language_code: Option<String>,
    pub sample_rate: u32,
    pub previous_text: Option<String>,
    pub voice_settings: Option<ElevenLabsVoiceSettings>,
    pub pronunciation_dictionary_locators: Vec<PronunciationDictionaryLocator>,
    pub apply_text_normalization: Option<TextNormalization>,
    pub optimize_streaming_latency: Option<u8>,
    pub enable_logging: Option<bool>,
}
impl ElevenLabsSynthesisRequest {
    pub fn new(text: impl Into<String>, voice_id: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            model: "eleven_flash_v2_5".into(),
            voice_id: voice_id.into(),
            language_code: None,
            sample_rate: 24000,
            previous_text: None,
            voice_settings: None,
            pronunciation_dictionary_locators: vec![],
            apply_text_normalization: None,
            optimize_streaming_latency: None,
            enable_logging: None,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        if self.text.is_empty()
            || self.model.is_empty()
            || self.voice_id.is_empty()
            || matches!(self.voice_id.as_str(), "." | "..")
            || !matches!(
                self.sample_rate,
                8000 | 16000 | 22050 | 24000 | 32000 | 44100 | 48000
            )
            || self.optimize_streaming_latency.is_some_and(|v| v > 4)
            || self
                .language_code
                .as_deref()
                .is_some_and(|language| !supported_language(&self.model, language))
            || self.voice_settings.as_ref().is_some_and(|s| {
                !common::finite_range(s.stability, 0.0, 1.0)
                    || !common::finite_range(s.similarity_boost, 0.0, 1.0)
                    || !common::finite_range(s.style, 0.0, 1.0)
                    || !common::finite_range(s.speed, 0.25, 4.0)
            })
            || self
                .pronunciation_dictionary_locators
                .iter()
                .any(|d| d.pronunciation_dictionary_id.is_empty() || d.version_id.is_empty())
        {
            Err(SpeechError::Invalid(
                "ElevenLabs text, model/language, voice, rate or settings",
            ))
        } else {
            Ok(())
        }
    }
}
fn supported_language(model: &str, language: &str) -> bool {
    // Model-language sets from services/elevenlabs/tts_base.py.
    const V25: &str = "ar bg cs da de el en es fi fil fr hi hr hu id it ja ko ms nl no pl pt ro ru sk sv ta tr uk vi zh";
    const V3: &str = "af ar as az be bg bn bs ca ceb cs cy da de el en es et fa fi fil fr ga gl gu ha he hi hr hu hy id is it ja jv ka kk kn ko ky lb ln lt lv mk ml mr ms ne nl no ny pa pl ps pt ro ru sd sk sl so sr sv sw ta te th tr uk ur vi zh";
    match model {
        "eleven_flash_v2_5" | "eleven_turbo_v2_5" => V25.split_whitespace().any(|v| v == language),
        "eleven_v3" | "eleven_v3_conversational" => V3.split_whitespace().any(|v| v == language),
        "eleven_v4_turbo" => V3
            .split_whitespace()
            .chain("ast mi mn mt my oc or tg uz yue".split_whitespace())
            .any(|v| v == language),
        _ => false,
    }
}
#[derive(Debug, Clone)]
pub struct ElevenLabsTranscriptionRequest {
    pub model: String,
    /// Service-native ISO 639-3 code (`eng` by default); `None` permits detection.
    pub language_code: Option<String>,
    pub tag_audio_events: Option<bool>,
    /// Repeated multipart fields, not a JSON array or comma-separated string.
    pub keyterms: Vec<String>,
    pub no_verbatim: Option<bool>,
}
impl Default for ElevenLabsTranscriptionRequest {
    fn default() -> Self {
        Self {
            model: "scribe_v2".into(),
            language_code: Some("eng".into()),
            tag_audio_events: None,
            keyterms: vec![],
            no_verbatim: None,
        }
    }
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ElevenLabsWord {
    pub text: String,
    #[serde(rename = "type")]
    pub kind: String,
    pub start: Option<f64>,
    pub end: Option<f64>,
    pub speaker_id: Option<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ElevenLabsTranscript {
    pub text: String,
    pub language_code: Option<String>,
    pub language_probability: Option<f64>,
    #[serde(default)]
    pub words: Vec<ElevenLabsWord>,
}
/// Character timing relative to this synthesis request, not actual playout.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct CharacterAlignment {
    pub characters: Vec<String>,
    pub character_start_times_seconds: Vec<f64>,
    pub character_end_times_seconds: Vec<f64>,
}
impl CharacterAlignment {
    fn validate(&self) -> Result<(), SpeechError> {
        if self.characters.len() != self.character_start_times_seconds.len()
            || self.characters.len() != self.character_end_times_seconds.len()
            || self
                .character_start_times_seconds
                .iter()
                .zip(&self.character_end_times_seconds)
                .any(|(start, end)| {
                    !start.is_finite() || !end.is_finite() || *start < 0.0 || end < start
                })
            || self
                .character_start_times_seconds
                .windows(2)
                .any(|pair| pair[1] < pair[0])
        {
            Err(SpeechError::Protocol("invalid character alignment"))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum ElevenLabsEvent {
    Audio(SpeechChunk),
    Alignment {
        alignment: Option<CharacterAlignment>,
        normalized_alignment: Option<CharacterAlignment>,
    },
}
pub type ElevenLabsStream = ResponseStream<ElevenLabsEvent>;
#[derive(Deserialize)]
struct WireEvent {
    audio_base64: Option<String>,
    alignment: Option<CharacterAlignment>,
    normalized_alignment: Option<CharacterAlignment>,
}
#[derive(Clone)]
pub struct ElevenLabsHttpClient {
    http: HttpClient,
}
impl ElevenLabsHttpClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        Ok(Self {
            http: HttpClient::new(config)?,
        })
    }
    pub async fn transcribe_pcm(
        &self,
        pcm: &[u8],
        sample_rate: u32,
        channels: u16,
        request: ElevenLabsTranscriptionRequest,
        cancel: CancellationToken,
    ) -> Result<ElevenLabsTranscript, SpeechError> {
        if request.model.is_empty()
            || !common::optional_text(&request.language_code)
            || request.keyterms.iter().any(String::is_empty)
            || (request.no_verbatim.is_some() && request.model != "scribe_v2")
        {
            return Err(SpeechError::Invalid("ElevenLabs transcription settings"));
        }
        let part = self
            .http
            .wav_part(pcm, sample_rate, channels, "audio/x-wav")?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model_id", request.model);
        if let Some(language) = request.language_code {
            form = form.text("language_code", language);
        }
        if let Some(tag) = request.tag_audio_events {
            form = form.text("tag_audio_events", tag.to_string());
        }
        for keyterm in request.keyterms {
            form = form.text("keyterms", keyterm);
        }
        if let Some(no_verbatim) = request.no_verbatim {
            form = form.text("no_verbatim", no_verbatim.to_string());
        }
        let http = self
            .http
            .post(&["v1", "speech-to-text"])?
            .header("xi-api-key", common::secret(&self.http.config.api_key)?)
            .multipart(form);
        let transcript: ElevenLabsTranscript = self.http.json(http, cancel).await?;
        if transcript
            .language_probability
            .is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
            || transcript.words.iter().any(|word| {
                word.start.is_some_and(|v| !v.is_finite() || v < 0.0)
                    || word.end.is_some_and(|v| !v.is_finite() || v < 0.0)
                    || word
                        .start
                        .zip(word.end)
                        .is_some_and(|(start, end)| end < start)
            })
        {
            return Err(SpeechError::Protocol(
                "invalid transcript timing or probability",
            ));
        }
        Ok(transcript)
    }
    pub fn synthesize(
        &self,
        request: ElevenLabsSynthesisRequest,
    ) -> Result<ElevenLabsStream, SpeechError> {
        request.validate()?;
        let mut body = json!({"text":request.text,"model_id":request.model});
        if !matches!(
            request.model.as_str(),
            "eleven_v3" | "eleven_v3_conversational"
        ) {
            if let Some(previous) = request.previous_text.filter(|text| !text.is_empty()) {
                body["previous_text"] = previous.into();
            }
        }
        if let Some(settings) = request.voice_settings {
            let settings = serde_json::to_value(settings)?;
            if settings.as_object().is_some_and(|v| !v.is_empty()) {
                body["voice_settings"] = settings;
            }
        }
        if !request.pronunciation_dictionary_locators.is_empty() {
            body["pronunciation_dictionary_locators"] =
                serde_json::to_value(request.pronunciation_dictionary_locators)?;
        }
        if let Some(normalization) = request.apply_text_normalization {
            body["apply_text_normalization"] = serde_json::to_value(normalization)?;
        }
        if let Some(language) = request.language_code {
            body["language_code"] = language.into();
        }
        let mut query = vec![("output_format", format!("pcm_{}", request.sample_rate))];
        if let Some(latency) = request.optimize_streaming_latency {
            query.push(("optimize_streaming_latency", latency.to_string()));
        }
        if let Some(logging) = request.enable_logging {
            query.push(("enable_logging", logging.to_string()));
        }
        let http = self
            .http
            .post(&[
                "v1",
                "text-to-speech",
                &request.voice_id,
                "stream",
                "with-timestamps",
            ])?
            .header("xi-api-key", common::secret(&self.http.config.api_key)?)
            .query(&query)
            .json(&body);
        let audio_limit = self.http.config.max_audio_bytes;
        let json_limit = self.http.config.max_json_bytes;
        let wire_limit = audio_limit
            .checked_mul(4)
            .and_then(|v| v.checked_add(json_limit))
            .ok_or(SpeechError::Invalid("limits overflow"))?;
        Ok(self.http.spawn(move |tx, observed| async move {
            let response = common::send(http).await?;
            common::check_content_type(
                &response,
                &[
                    "application/json",
                    "application/x-ndjson",
                    "application/ndjson",
                ],
                None,
            )?;
            let mut stream = response.bytes_stream();
            let mut line = Vec::new();
            let mut total = 0;
            let mut decoder =
                AudioDecoder::new(request.sample_rate, audio_limit, RawEncoding::Pcm16);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| SpeechError::Transport)?;
                common::checked_total(&mut total, chunk.len(), wire_limit)?;
                // A record may span any number of HTTP chunks; a chunk may carry many records.
                for part in chunk.split_inclusive(|byte| *byte == b'\n') {
                    if line
                        .len()
                        .checked_add(part.len())
                        .is_none_or(|size| size > json_limit)
                    {
                        return Err(SpeechError::TooLarge);
                    }
                    line.extend_from_slice(part);
                    if part.last() == Some(&b'\n') {
                        emit_line(&line, &mut decoder, &tx, &observed).await?;
                        line.clear();
                    }
                }
            }
            if !line.is_empty() {
                emit_line(&line, &mut decoder, &tx, &observed).await?;
            }
            decoder.finish()
        }))
    }
}
async fn emit_line(
    line: &[u8],
    decoder: &mut AudioDecoder,
    tx: &mpsc::Sender<ElevenLabsEvent>,
    observed: &AtomicBool,
) -> Result<(), SpeechError> {
    if line.iter().all(u8::is_ascii_whitespace) {
        return Ok(());
    }
    let event: WireEvent = serde_json::from_slice(line)?;
    if event.audio_base64.is_none()
        && event.alignment.is_none()
        && event.normalized_alignment.is_none()
    {
        return Err(SpeechError::Protocol("missing audio and alignment"));
    }
    for alignment in [&event.alignment, &event.normalized_alignment]
        .into_iter()
        .flatten()
    {
        alignment.validate()?;
    }
    if let Some(encoded) = event.audio_base64 {
        let audio = STANDARD
            .decode(encoded)
            .map_err(|_| SpeechError::Protocol("invalid base64 audio"))?;
        // The source HTTP adapter yields an audio frame for an explicit empty
        // audio_base64 field. Preserve that evidence without emitting fake PCM.
        if audio.is_empty() && !decoder.has_partial_sample() {
            observed.store(true, Ordering::Release);
        }
        if let Some(audio) = decoder.push(&audio)? {
            observed.store(true, Ordering::Release);
            tx.send(ElevenLabsEvent::Audio(audio))
                .await
                .map_err(|_| SpeechError::ConsumerClosed)?;
        }
    }
    if event.alignment.is_some() || event.normalized_alignment.is_some() {
        tx.send(ElevenLabsEvent::Alignment {
            alignment: event.alignment,
            normalized_alignment: event.normalized_alignment,
        })
        .await
        .map_err(|_| SpeechError::ConsumerClosed)?;
    }
    Ok(())
}
