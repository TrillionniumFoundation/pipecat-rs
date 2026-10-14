//! Bounded MiniMax HTTP SSE synthesis, based on Python source
//! `af9f9da9d7afddaa8811c0e0251b32566c980bdf` (`services/minimax/tts.py`).
//!
//! This native profile corrects the source's dropped EOF record and unchecked
//! provider status. It requires status 2 plus a complete HTTP body, rejects
//! malformed hex and incomplete PCM, and never replays aggregated final audio.
//! No timestamps are fabricated. Errors and Debug output redact private inputs.
use crate::{
    common::{self, AudioDecoder, HttpClient, RawEncoding},
    SpeechConfig, SpeechError, SpeechStream,
};
use futures_util::StreamExt;
use pipecat_services::sse::{SseDecoder, SseEvent};
use serde::Serialize;
use serde_json::{json, Value};
use std::{fmt, sync::atomic::Ordering};
use tokio::sync::mpsc;

pub const MINIMAX_PCM_SAMPLE_RATES: [u32; 6] = [8000, 16000, 22050, 24000, 32000, 44100];
pub const MINIMAX_DEFAULT_SAMPLE_RATE: u32 = 24000;

/// The pinned source's 40 mappings and case-insensitive regional base fallback.
/// Unmapped tags fall back to their lowercase base code, as the original helper
/// does. This conversion does not guarantee provider/model language support.
pub fn language_to_minimax_language(language: &str) -> String {
    let base = language
        .split('-')
        .next()
        .unwrap_or_default()
        .to_lowercase();
    match base.as_str() {
        "af" => "Afrikaans",
        "ar" => "Arabic",
        "bg" => "Bulgarian",
        "ca" => "Catalan",
        "cs" => "Czech",
        "da" => "Danish",
        "de" => "German",
        "el" => "Greek",
        "en" => "English",
        "es" => "Spanish",
        "fa" => "Persian",
        "fi" => "Finnish",
        "fil" => "Filipino",
        "fr" => "French",
        "he" => "Hebrew",
        "hi" => "Hindi",
        "hr" => "Croatian",
        "hu" => "Hungarian",
        "id" => "Indonesian",
        "it" => "Italian",
        "ja" => "Japanese",
        "ko" => "Korean",
        "ms" => "Malay",
        "nb" => "Norwegian",
        "nn" => "Nynorsk",
        "nl" => "Dutch",
        "pl" => "Polish",
        "pt" => "Portuguese",
        "ro" => "Romanian",
        "ru" => "Russian",
        "sk" => "Slovak",
        "sl" => "Slovenian",
        "sv" => "Swedish",
        "ta" => "Tamil",
        "th" => "Thai",
        "tr" => "Turkish",
        "uk" => "Ukrainian",
        "vi" => "Vietnamese",
        "yue" => "Chinese,Yue",
        "zh" => "Chinese",
        other => other,
    }
    .into()
}

/// The documented provider emotion set, not the deprecated Python params list.
/// Model-specific availability is not validated by this type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MiniMaxEmotion {
    Happy,
    Sad,
    Angry,
    Fearful,
    Disgusted,
    Surprised,
    Calm,
    Fluent,
    Whisper,
}

/// Immutable voice settings for one request. Optional false values are retained.
#[derive(Debug, Clone, PartialEq)]
pub struct MiniMaxVoiceSettings {
    pub speed: f64,
    pub volume: f64,
    pub pitch: i32,
    pub emotion: Option<MiniMaxEmotion>,
    pub text_normalization: Option<bool>,
    pub latex_read: Option<bool>,
}
impl Default for MiniMaxVoiceSettings {
    fn default() -> Self {
        Self {
            speed: 1.0,
            volume: 1.0,
            pitch: 0,
            emotion: None,
            text_normalization: None,
            latex_read: None,
        }
    }
}

#[derive(Clone)]
pub struct MiniMaxSynthesisRequest {
    pub text: String,
    pub model: String,
    pub voice: String,
    pub sample_rate: u32,
    /// Source language tag, mapped to a provider `language_boost` name.
    /// `None` omits the field, matching the source's canonical settings defaults.
    pub language: Option<String>,
    pub voice_settings: MiniMaxVoiceSettings,
}
impl MiniMaxSynthesisRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            model: "speech-2.8-turbo".into(),
            voice: "Calm_Woman".into(),
            sample_rate: MINIMAX_DEFAULT_SAMPLE_RATE,
            language: None,
            voice_settings: MiniMaxVoiceSettings::default(),
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        if self.text.is_empty()
            || self.model.is_empty()
            || self.voice.is_empty()
            || self.model.len() > 1024
            || self.voice.len() > 1024
            || !MINIMAX_PCM_SAMPLE_RATES.contains(&self.sample_rate)
            || self
                .language
                .as_ref()
                .is_some_and(|value| value.len() > 128)
            || !common::finite_range(Some(self.voice_settings.speed), 0.5, 2.0)
            || !common::finite_range(Some(self.voice_settings.volume), 0.0, 10.0)
            || self.voice_settings.volume == 0.0
            || !(-12..=12).contains(&self.voice_settings.pitch)
        {
            return Err(SpeechError::Invalid("MiniMax synthesis settings"));
        }
        Ok(())
    }
}
impl fmt::Debug for MiniMaxSynthesisRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MiniMaxSynthesisRequest")
            .field("text", &"[redacted]")
            .field("model", &"[redacted]")
            .field("voice", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .finish_non_exhaustive()
    }
}

/// Independent transport budgets. `SpeechConfig::max_json_bytes` bounds both
/// serialized request bytes and each raw SSE block, including comments/fields.
/// `max_audio_bytes` counts incremental decoded bytes, including odd-byte carry;
/// ignored aggregate audio remains subject to event and wire byte budgets.
#[derive(Debug, Clone, Copy)]
pub struct MiniMaxLimits {
    pub max_wire_bytes: usize,
    /// Number of JSON data records, including ignored metadata records.
    pub max_events: usize,
    /// Request text, model, voice, language, and group ID, in UTF-8 bytes.
    pub max_text_field_bytes: usize,
}
impl Default for MiniMaxLimits {
    fn default() -> Self {
        Self {
            max_wire_bytes: 128 * 1024 * 1024,
            max_events: 65_536,
            max_text_field_bytes: 64 * 1024,
        }
    }
}
impl MiniMaxLimits {
    fn validate(self) -> Result<(), SpeechError> {
        if self.max_wire_bytes == 0 || self.max_events == 0 || self.max_text_field_bytes == 0 {
            Err(SpeechError::Invalid("MiniMax limits"))
        } else {
            Ok(())
        }
    }
}

#[derive(Clone)]
pub struct MiniMaxHttpClient {
    http: HttpClient,
    group_id: String,
    limits: MiniMaxLimits,
}
impl fmt::Debug for MiniMaxHttpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MiniMaxHttpClient")
            .field("group_id", &"[redacted]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl MiniMaxHttpClient {
    /// `config.base_url` is the provider root; the source's required GroupId is
    /// encoded as a query parameter rather than interpolated into a URL.
    pub fn new(config: SpeechConfig, group_id: impl Into<String>) -> Result<Self, SpeechError> {
        Self::with_limits(config, group_id, MiniMaxLimits::default())
    }
    pub fn with_limits(
        config: SpeechConfig,
        group_id: impl Into<String>,
        limits: MiniMaxLimits,
    ) -> Result<Self, SpeechError> {
        limits.validate()?;
        let group_id = group_id.into();
        if config.base_url.len() > 4096
            || config.api_key.len() > 8192
            || group_id.is_empty()
            || group_id.len() > 1024
        {
            return Err(SpeechError::Invalid("MiniMax URL, credential or GroupId"));
        }
        if group_id.len() > limits.max_text_field_bytes {
            return Err(SpeechError::TooLarge);
        }
        Ok(Self {
            http: HttpClient::direct(config)?,
            group_id,
            limits,
        })
    }
    pub fn synthesize(
        &self,
        request: MiniMaxSynthesisRequest,
    ) -> Result<SpeechStream, SpeechError> {
        request.validate()?;
        let limits = self.limits;
        for field in [&request.text, &request.model, &request.voice] {
            if field.len() > limits.max_text_field_bytes {
                return Err(SpeechError::TooLarge);
            }
        }
        if request
            .language
            .as_ref()
            .is_some_and(|value| value.len() > limits.max_text_field_bytes)
        {
            return Err(SpeechError::TooLarge);
        }
        let rate = request.sample_rate;
        let settings = request.voice_settings;
        let mut voice_setting = json!({
            "voice_id": request.voice,
            "speed": settings.speed,
            "vol": settings.volume,
            "pitch": settings.pitch,
        });
        if let Some(emotion) = settings.emotion {
            voice_setting["emotion"] = serde_json::to_value(emotion)?;
        }
        if let Some(enabled) = settings.text_normalization {
            voice_setting["text_normalization"] = enabled.into();
        }
        if let Some(enabled) = settings.latex_read {
            voice_setting["latex_read"] = enabled.into();
        }
        let mut body = json!({
            "stream": true,
            "voice_setting": voice_setting,
            "audio_setting": { "bitrate": 128000, "format": "pcm", "channel": 1, "sample_rate": rate },
            "model": request.model,
            "text": request.text,
        });
        if let Some(language) = request.language {
            body["language_boost"] = language_to_minimax_language(&language).into();
        }
        let body = serde_json::to_vec(&body)?;
        let event_limit = self.http.config.max_json_bytes;
        if body.len() > event_limit {
            return Err(SpeechError::TooLarge);
        }
        let http = self
            .http
            .post(&["v1", "t2a_v2"])?
            .query(&[("GroupId", &self.group_id)])
            .header(
                reqwest::header::AUTHORIZATION,
                common::secret(&format!("Bearer {}", self.http.config.api_key))?,
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "application/json, text/plain, */*")
            .body(body);
        let audio_limit = self.http.config.max_audio_bytes;
        Ok(self.http.spawn(move |tx, observed| async move {
            // Direct HTTP/1, without proxy, redirects, retries or live fallback.
            let response = http.send().await.map_err(|_| SpeechError::Transport)?;
            validate_response(&response, limits.max_wire_bytes)?;
            let mut stream = response.bytes_stream();
            let mut sse = BoundedSse::new(event_limit);
            let mut state = StreamState {
                audio: AudioDecoder::new(rate, audio_limit, RawEncoding::Pcm16),
                audio_limit,
                sample_rate: rate,
                decoded_bytes: 0,
                events: 0,
                max_events: limits.max_events,
                final_seen: false,
            };
            let mut wire_bytes = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| SpeechError::Transport)?;
                common::checked_total(&mut wire_bytes, chunk.len(), limits.max_wire_bytes)?;
                // Bound the temporary event batch even if the transport yields
                // many small records in one large received chunk.
                for bytes in chunk.chunks(8192) {
                    for event in sse.feed(bytes)? {
                        state.event(event, &tx, &observed).await?;
                    }
                }
            }
            // Deliberate source correction: process the last valid data record
            // at a clean HTTP EOF, even without an SSE blank-line terminator.
            for event in sse.finish()? {
                state.event(event, &tx, &observed).await?;
            }
            if !state.final_seen {
                return Err(SpeechError::Protocol("missing MiniMax final status"));
            }
            match state.audio.finish() {
                Err(SpeechError::EmptyAudio) => Err(SpeechError::Protocol("empty MiniMax PCM")),
                result => result,
            }
        }))
    }
}

fn validate_response(response: &reqwest::Response, wire_limit: usize) -> Result<(), SpeechError> {
    if response.headers().len() > 64 {
        return Err(SpeechError::TooLarge);
    }
    let mut header_bytes = 0;
    for (name, value) in response.headers() {
        common::checked_total(
            &mut header_bytes,
            name.as_str().len() + value.len() + 4,
            16 * 1024,
        )?;
    }
    if response.status() != reqwest::StatusCode::OK {
        return Err(SpeechError::Http(response.status().as_u16()));
    }
    common::check_content_type(response, &["text/event-stream"], None)?;
    if response
        .headers()
        .get_all(reqwest::header::CONTENT_TYPE)
        .iter()
        .count()
        > 1
        || response
            .headers()
            .get_all(reqwest::header::CONTENT_ENCODING)
            .iter()
            .any(|value| value.to_str().ok() != Some("identity"))
    {
        return Err(SpeechError::ContentType);
    }
    if response
        .content_length()
        .is_some_and(|size| size > wire_limit as u64)
    {
        return Err(SpeechError::TooLarge);
    }
    Ok(())
}

/// The generic SSE parser bounds decoded data. This wrapper additionally charges
/// raw newlines, comment fields and ignored fields, and handles CR/LF splits.
struct BoundedSse {
    decoder: SseDecoder,
    bytes: usize,
    line_empty: bool,
    skip_lf: bool,
    after_block: bool,
    limit: usize,
}
impl BoundedSse {
    fn new(limit: usize) -> Self {
        Self {
            decoder: SseDecoder::new(limit),
            bytes: 0,
            line_empty: true,
            skip_lf: false,
            after_block: false,
            limit,
        }
    }
    fn feed(&mut self, bytes: &[u8]) -> Result<Vec<SseEvent>, SpeechError> {
        for &byte in bytes {
            let paired_lf = self.skip_lf && byte == b'\n';
            self.skip_lf = false;
            if self.after_block && !paired_lf {
                self.bytes = 0;
                self.after_block = false;
            }
            common::checked_total(&mut self.bytes, 1, self.limit)?;
            if paired_lf {
                continue;
            }
            if byte == b'\r' || byte == b'\n' {
                if self.line_empty {
                    self.after_block = true;
                }
                self.line_empty = true;
                self.skip_lf = byte == b'\r';
            } else {
                self.line_empty = false;
            }
        }
        self.decoder
            .feed(bytes)
            .map_err(|_| SpeechError::Protocol("invalid MiniMax SSE"))
    }
    fn finish(&mut self) -> Result<Vec<SseEvent>, SpeechError> {
        // Synthetic framing is not charged as received wire/event bytes.
        self.decoder
            .feed(b"\n\n")
            .map_err(|_| SpeechError::Protocol("invalid MiniMax SSE"))
    }
}

struct StreamState {
    audio: AudioDecoder,
    audio_limit: usize,
    sample_rate: u32,
    decoded_bytes: usize,
    events: usize,
    max_events: usize,
    final_seen: bool,
}
impl StreamState {
    async fn event(
        &mut self,
        event: SseEvent,
        tx: &mpsc::Sender<crate::SpeechChunk>,
        observed: &std::sync::atomic::AtomicBool,
    ) -> Result<(), SpeechError> {
        common::checked_total(&mut self.events, 1, self.max_events)?;
        let value: Value = serde_json::from_str(&event.data)
            .map_err(|_| SpeechError::Protocol("invalid MiniMax JSON"))?;
        let object = value
            .as_object()
            .ok_or(SpeechError::Protocol("MiniMax event is not an object"))?;
        if event.event.as_deref() == Some("error")
            || object.get("error").is_some_and(|value| !value.is_null())
        {
            return Err(SpeechError::Protocol("MiniMax provider error"));
        }
        // Errors cannot hide behind an extra_info marker or final status.
        if let Some(base) = object.get("base_resp") {
            let code = base
                .as_object()
                .and_then(|base| base.get("status_code"))
                .and_then(Value::as_i64)
                .ok_or(SpeechError::Protocol("invalid MiniMax base response"))?;
            if code != 0 {
                return Err(SpeechError::Protocol("MiniMax provider error"));
            }
        }
        if let Some(extra) = object.get("extra_info").filter(|value| !value.is_null()) {
            validate_audio_metadata(extra, self.sample_rate)?;
        }
        let Some(data) = object.get("data").filter(|value| !value.is_null()) else {
            // Metadata-only records and extra_info do not establish completion.
            return Ok(());
        };
        let data = data
            .as_object()
            .ok_or(SpeechError::Protocol("invalid MiniMax data"))?;
        let status = data
            .get("status")
            .and_then(Value::as_u64)
            .ok_or(SpeechError::Protocol("invalid MiniMax data status"))?;
        if !matches!(status, 1 | 2) {
            return Err(SpeechError::Protocol("invalid MiniMax data status"));
        }
        if self.final_seen {
            return Err(SpeechError::Protocol("MiniMax data after final status"));
        }
        let audio = match data.get("audio") {
            None | Some(Value::Null) => None,
            Some(Value::String(value)) => Some(value.as_str()),
            Some(_) => return Err(SpeechError::Protocol("invalid MiniMax audio")),
        };
        if status == 2 {
            self.final_seen = true;
            // The default provider final audio is the whole utterance. Never
            // emit it again, even when extra_info is absent. Do not allocate or
            // charge ignored aggregate audio against the incremental PCM limit.
            return Ok(());
        }
        if object.contains_key("extra_info") {
            // Preserve the source's key-presence behavior, even for JSON null.
            return Ok(());
        }
        if let Some(encoded) = audio.filter(|value| !value.is_empty()) {
            let bytes = decode_hex(encoded, self.audio_limit.saturating_sub(self.decoded_bytes))?;
            common::checked_total(&mut self.decoded_bytes, bytes.len(), self.audio_limit)?;
            if let Some(chunk) = self.audio.push(&bytes)? {
                observed.store(true, Ordering::Release);
                tx.send(chunk)
                    .await
                    .map_err(|_| SpeechError::ConsumerClosed)?;
            }
        }
        Ok(())
    }
}
/// Advertised format fields are optional, but must match when non-null.
fn validate_audio_metadata(extra: &Value, sample_rate: u32) -> Result<(), SpeechError> {
    let extra = extra
        .as_object()
        .ok_or(SpeechError::Protocol("invalid MiniMax audio metadata"))?;
    for (field, expected) in [
        ("audio_sample_rate", u64::from(sample_rate)),
        ("audio_channel", 1),
    ] {
        if extra
            .get(field)
            .filter(|value| !value.is_null())
            .is_some_and(|value| value.as_u64() != Some(expected))
        {
            return Err(SpeechError::Protocol("MiniMax audio format mismatch"));
        }
    }
    if extra
        .get("audio_format")
        .filter(|value| !value.is_null())
        .is_some_and(|value| value.as_str() != Some("pcm"))
    {
        return Err(SpeechError::Protocol("MiniMax audio format mismatch"));
    }
    Ok(())
}

fn decode_hex(encoded: &str, remaining: usize) -> Result<Vec<u8>, SpeechError> {
    if !encoded.len().is_multiple_of(2) {
        return Err(SpeechError::Protocol("invalid MiniMax hex audio"));
    }
    if encoded.len() / 2 > remaining {
        return Err(SpeechError::TooLarge);
    }
    let mut bytes = Vec::with_capacity(encoded.len() / 2);
    for pair in encoded.as_bytes().as_chunks::<2>().0 {
        let high = hex_digit(pair[0])?;
        let low = hex_digit(pair[1])?;
        bytes.push(high * 16 + low);
    }
    Ok(bytes)
}
fn hex_digit(byte: u8) -> Result<u8, SpeechError> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(SpeechError::Protocol("invalid MiniMax hex audio")),
    }
}
