//! Typed Speechify timestamped SSE synthesis, pinned to Python source
//! `8af3abc008195182d26afe5105300831a8d9908d` (`services/speechify/tts.py`).
//!
//! Words use Unicode code-point spans into the original input. Offsets restart
//! for each request; a pipeline may add preceding utterance durations. Unlike
//! the Python adapter, malformed JSON, invalid timestamps, provider errors,
//! missing `speech.done`, empty audio and incomplete PCM fail closed. Error
//! messages never include credentials, request text or provider response bodies.
use crate::{
    common::{self, AudioDecoder, HttpClient, RawEncoding},
    ResponseStream, SpeechChunk, SpeechConfig, SpeechError,
};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::StreamExt;
use pipecat_services::sse::{SseDecoder, SseEvent};
use serde_json::{json, Map, Value};
use std::{fmt, sync::atomic::Ordering};
use tokio::sync::mpsc;

pub const SPEECHIFY_API_VERSION: &str = "2026-09-30";
/// Native attribution deliberately does not impersonate the Python integration.
pub const SPEECHIFY_CALLER: &str = "pipecat-rs";
pub const SPEECHIFY_CALLER_VERSION: &str = env!("CARGO_PKG_VERSION");
pub const SPEECHIFY_PCM_SAMPLE_RATES: [u32; 6] = [8000, 16000, 22050, 24000, 44100, 48000];
pub const SPEECHIFY_DEFAULT_SAMPLE_RATE: u32 = 24000;

/// The source's six base-language mappings; regional and unmapped tags pass through.
pub fn language_to_speechify_language(language: &str) -> String {
    match language {
        "de" => "de-DE",
        "en" => "en-US",
        "es" => "es-ES",
        "fr" => "fr-FR",
        "it" => "it-IT",
        "pt" => "pt-BR",
        other => other,
    }
    .into()
}

/// Immutable settings for one synthesis request. `None` options are omitted,
/// whereas `Some(false)` is sent explicitly. An empty language is omitted.
#[derive(Clone)]
pub struct SpeechifySynthesisRequest {
    pub text: String,
    pub model: String,
    pub voice: String,
    pub sample_rate: u32,
    pub language: Option<String>,
    pub loudness_normalization: Option<bool>,
    pub text_normalization: Option<bool>,
}
impl SpeechifySynthesisRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            model: "simba-3.2".into(),
            voice: "geffen_32".into(),
            sample_rate: SPEECHIFY_DEFAULT_SAMPLE_RATE,
            language: None,
            loudness_normalization: None,
            text_normalization: None,
        }
    }
    /// Actual wire PCM rate; unsupported positive rates fall back to 24 kHz.
    pub fn output_sample_rate(&self) -> u32 {
        if SPEECHIFY_PCM_SAMPLE_RATES.contains(&self.sample_rate) {
            self.sample_rate
        } else {
            SPEECHIFY_DEFAULT_SAMPLE_RATE
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        if self.text.is_empty()
            || self.model.is_empty()
            || self.voice.is_empty()
            || self.model.len() > 1024
            || self.voice.len() > 1024
            || self
                .language
                .as_ref()
                .is_some_and(|value| value.len() > 128)
            || self.sample_rate == 0
        {
            return Err(SpeechError::Invalid(
                "Speechify text, model, voice, language or rate",
            ));
        }
        Ok(())
    }
}
impl fmt::Debug for SpeechifySynthesisRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeechifySynthesisRequest")
            .field("text", &"[redacted]")
            .field("model", &"[redacted]")
            .field("voice", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .finish_non_exhaustive()
    }
}

/// Additional per-request limits. `SpeechConfig::max_json_bytes` bounds the
/// serialized request and each SSE block; `max_audio_bytes` bounds all decoded
/// PCM, including a pending odd byte. These budgets are independent.
#[derive(Debug, Clone, Copy)]
pub struct SpeechifyLimits {
    /// All received body bytes, including ignored events and SSE comments.
    pub max_wire_bytes: usize,
    /// Word marks inspected (including fragments/fallbacks), and emitted words.
    pub max_word_count: usize,
    /// Total UTF-8 bytes of words emitted, including repeated source spans.
    pub max_word_bytes: usize,
    /// Request text and each non-audio text field (including fallback values).
    pub max_text_field_bytes: usize,
}
impl Default for SpeechifyLimits {
    fn default() -> Self {
        Self {
            max_wire_bytes: 128 * 1024 * 1024,
            max_word_count: 65_536,
            max_word_bytes: 4 * 1024 * 1024,
            max_text_field_bytes: 64 * 1024,
        }
    }
}
impl SpeechifyLimits {
    fn validate(self) -> Result<(), SpeechError> {
        if self.max_wire_bytes == 0
            || self.max_word_count == 0
            || self.max_word_bytes == 0
            || self.max_text_field_bytes == 0
        {
            Err(SpeechError::Invalid("Speechify limits"))
        } else {
            Ok(())
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum SpeechifyEvent {
    Audio(SpeechChunk),
    Word {
        word: String,
        offset_seconds: f64,
    },
    /// Sent only after a complete HTTP body with a `speech.done` event.
    /// Includes trailing silence and late marks: max(done durations, mark ends).
    Done {
        duration_seconds: f64,
    },
}
pub type SpeechifyStream = ResponseStream<SpeechifyEvent>;

#[derive(Clone)]
pub struct SpeechifyHttpClient {
    http: HttpClient,
    limits: SpeechifyLimits,
}
impl SpeechifyHttpClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        Self::with_limits(config, SpeechifyLimits::default())
    }
    pub fn with_limits(config: SpeechConfig, limits: SpeechifyLimits) -> Result<Self, SpeechError> {
        limits.validate()?;
        if config.base_url.len() > 4096 || config.api_key.len() > 8192 {
            return Err(SpeechError::Invalid("Speechify URL or credential length"));
        }
        Ok(Self {
            http: HttpClient::direct(config)?,
            limits,
        })
    }
    pub fn synthesize(
        &self,
        request: SpeechifySynthesisRequest,
    ) -> Result<SpeechifyStream, SpeechError> {
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
        let rate = request.output_sample_rate();
        let mut body = json!({
            "input": request.text,
            "voice_id": request.voice,
            "model": request.model,
            "output_format": format!("pcm_{rate}"),
        });
        if let Some(language) = request.language.as_ref().filter(|value| !value.is_empty()) {
            body["language"] = language_to_speechify_language(language).into();
        }
        let mut options = Map::new();
        if let Some(value) = request.loudness_normalization {
            options.insert("loudness_normalization".into(), value.into());
        }
        if let Some(value) = request.text_normalization {
            options.insert("text_normalization".into(), value.into());
        }
        if !options.is_empty() {
            body["options"] = options.into();
        }
        let body = serde_json::to_vec(&body)?;
        let event_limit = self.http.config.max_json_bytes;
        if body.len() > event_limit {
            return Err(SpeechError::TooLarge);
        }
        let http = self
            .http
            .post(&["v1", "audio", "stream", "with-timestamps"])?
            .header(
                reqwest::header::AUTHORIZATION,
                common::secret(&format!("Bearer {}", self.http.config.api_key))?,
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .header(reqwest::header::ACCEPT, "text/event-stream")
            .header("Speechify-Version", SPEECHIFY_API_VERSION)
            .header("Speechify-Caller", SPEECHIFY_CALLER)
            .header("Speechify-Caller-Version", SPEECHIFY_CALLER_VERSION)
            .body(body);
        let audio_limit = self.http.config.max_audio_bytes;
        Ok(self.http.spawn(move |tx, observed| async move {
            // A single direct request: no redirects and no automatic retries.
            let response = http.send().await.map_err(|_| SpeechError::Transport)?;
            validate_response(&response, limits.max_wire_bytes)?;
            let mut stream = response.bytes_stream();
            let mut sse = BoundedSse::new(event_limit);
            let mut state = StreamState {
                audio: AudioDecoder::new(rate, audio_limit, RawEncoding::Pcm16),
                decoded_bytes: 0,
                audio_limit,
                words: WordAccumulator::new(request.text, limits),
                done: false,
                duration_seconds: 0.0,
            };
            let mut wire_bytes = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| SpeechError::Transport)?;
                common::checked_total(&mut wire_bytes, chunk.len(), limits.max_wire_bytes)?;
                for event in sse.feed(&chunk)? {
                    state.event(event, &tx, &observed).await?;
                }
            }
            // The pinned Python iterator dispatches the last block at EOF even
            // without a terminating empty line. Keep the shared decoder intact.
            for event in sse.finish()? {
                state.event(event, &tx, &observed).await?;
            }
            if !state.done {
                return Err(SpeechError::Protocol("missing Speechify speech.done"));
            }
            match state.audio.finish() {
                Err(SpeechError::EmptyAudio) => {
                    return Err(SpeechError::Protocol("empty Speechify PCM"))
                }
                result => result?,
            }
            if let Some(word) = state.words.flush()? {
                emit(&tx, word).await?;
            }
            emit(
                &tx,
                SpeechifyEvent::Done {
                    duration_seconds: state.duration_seconds.max(state.words.end_time),
                },
            )
            .await
        }))
    }
}

fn validate_response(response: &reqwest::Response, wire_limit: usize) -> Result<(), SpeechError> {
    let mut header_bytes = 0;
    if response.headers().len() > 64 {
        return Err(SpeechError::TooLarge);
    }
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

/// Counts raw block bytes as well as the shared decoder's decoded data. Its own
/// bound includes newlines and ignored/comment fields, which the generic parser
/// does not charge in full. All counters reset only at an SSE block boundary.
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
            .map_err(|_| SpeechError::Protocol("invalid Speechify SSE"))
    }
    fn finish(&mut self) -> Result<Vec<SseEvent>, SpeechError> {
        // Synthetic terminators are framing, not received wire/event bytes.
        self.decoder
            .feed(b"\n\n")
            .map_err(|_| SpeechError::Protocol("invalid Speechify SSE"))
    }
}
struct StreamState {
    audio: AudioDecoder,
    decoded_bytes: usize,
    audio_limit: usize,
    words: WordAccumulator,
    done: bool,
    duration_seconds: f64,
}
impl StreamState {
    async fn event(
        &mut self,
        event: SseEvent,
        tx: &mpsc::Sender<SpeechifyEvent>,
        observed: &std::sync::atomic::AtomicBool,
    ) -> Result<(), SpeechError> {
        // The native profile rejects malformed JSON even on unknown events.
        let data: Value = serde_json::from_str(&event.data)?;
        bounded_text_fields(&data, self.words.limits.max_text_field_bytes, true)?;
        let data = data
            .as_object()
            .ok_or(SpeechError::Protocol("Speechify event is not an object"))?;
        if event
            .event
            .as_ref()
            .is_some_and(|name| name.len() > self.words.limits.max_text_field_bytes)
        {
            return Err(SpeechError::TooLarge);
        }
        match event.event.as_deref() {
            Some("speech.chunk") => {
                if let Some(value) = data.get("audio").filter(|value| !value.is_null()) {
                    let encoded = value
                        .as_str()
                        .ok_or(SpeechError::Protocol("invalid Speechify audio"))?;
                    if !encoded.is_empty() {
                        // Bound allocation before decoding. Canonical padded base64
                        // decodes to at most three bytes per four encoded bytes.
                        let upper = encoded
                            .len()
                            .checked_add(3)
                            .and_then(|v| (v / 4).checked_mul(3))
                            .ok_or(SpeechError::TooLarge)?;
                        let remaining = self.audio_limit.saturating_sub(self.decoded_bytes);
                        if upper > remaining.saturating_add(2) {
                            return Err(SpeechError::TooLarge);
                        }
                        let bytes = STANDARD
                            .decode(encoded)
                            .map_err(|_| SpeechError::Protocol("invalid Speechify base64"))?;
                        common::checked_total(
                            &mut self.decoded_bytes,
                            bytes.len(),
                            self.audio_limit,
                        )?;
                        if let Some(audio) = self.audio.push(&bytes)? {
                            observed.store(true, Ordering::Release);
                            emit(tx, SpeechifyEvent::Audio(audio)).await?;
                        }
                    }
                }
                // Audio always precedes words from the same event. Marks may also
                // appear later, including marks-only events after speech.done.
                if let Some(marks) = data.get("speech_marks").filter(|value| !value.is_null()) {
                    let marks = marks
                        .as_array()
                        .ok_or(SpeechError::Protocol("invalid Speechify marks"))?;
                    for mark in marks {
                        let mark = mark
                            .as_object()
                            .ok_or(SpeechError::Protocol("invalid Speechify mark"))?;
                        for word in self.words.add(mark)? {
                            emit(tx, word).await?;
                        }
                    }
                }
            }
            Some("speech.done") => {
                self.duration_seconds = self
                    .duration_seconds
                    .max(milliseconds(data, "audio_duration_ms")?);
                self.done = true;
            }
            Some("speech.error") => return Err(SpeechError::Protocol("Speechify provider error")),
            _ => {}
        }
        Ok(())
    }
}
async fn emit(tx: &mpsc::Sender<SpeechifyEvent>, event: SpeechifyEvent) -> Result<(), SpeechError> {
    tx.send(event)
        .await
        .map_err(|_| SpeechError::ConsumerClosed)
}
fn bounded_text_fields(value: &Value, limit: usize, root: bool) -> Result<(), SpeechError> {
    match value {
        Value::String(value) if value.len() > limit => Err(SpeechError::TooLarge),
        Value::Array(values) => {
            for value in values {
                bounded_text_fields(value, limit, false)?;
            }
            Ok(())
        }
        Value::Object(fields) => {
            for (name, value) in fields {
                // Only the root audio string is encoded binary and has its own
                // event/decoded-PCM budgets. Unknown text fields are bounded too.
                if !(root && name == "audio") {
                    bounded_text_fields(value, limit, false)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}
fn python_whitespace(value: char) -> bool {
    // CPython str.isspace/strip also recognize the four information separators.
    value.is_whitespace() || matches!(value, '\u{001c}'..='\u{001f}')
}
fn milliseconds(data: &Map<String, Value>, field: &str) -> Result<f64, SpeechError> {
    let Some(value) = data.get(field) else {
        return Ok(0.0);
    };
    let value = value
        .as_f64()
        .filter(|v| v.is_finite() && *v >= 0.0)
        .ok_or(SpeechError::Protocol("invalid Speechify timestamp"))?;
    Ok(value / 1000.0)
}

struct WordAccumulator {
    text: String,
    /// UTF-8 byte boundaries indexed by Unicode code point, including text end.
    boundaries: Vec<usize>,
    pending: Option<(usize, usize, f64)>,
    end_time: f64,
    mark_count: usize,
    word_count: usize,
    word_bytes: usize,
    limits: SpeechifyLimits,
}
impl WordAccumulator {
    fn new(text: String, limits: SpeechifyLimits) -> Self {
        let mut boundaries: Vec<_> = text.char_indices().map(|(offset, _)| offset).collect();
        boundaries.push(text.len());
        Self {
            text,
            boundaries,
            pending: None,
            end_time: 0.0,
            mark_count: 0,
            word_count: 0,
            word_bytes: 0,
            limits,
        }
    }
    fn add(&mut self, mark: &Map<String, Value>) -> Result<Vec<SpeechifyEvent>, SpeechError> {
        let mut result = Vec::with_capacity(2);
        if let Some(kind) = mark.get("type") {
            if kind
                .as_str()
                .is_some_and(|value| value.len() > self.limits.max_text_field_bytes)
            {
                return Err(SpeechError::TooLarge);
            }
            if kind.as_str() != Some("word") {
                return Ok(result);
            }
        }
        common::checked_total(&mut self.mark_count, 1, self.limits.max_word_count)?;
        let start_time = milliseconds(mark, "start_time")?;
        self.end_time = self.end_time.max(milliseconds(mark, "end_time")?);
        let value = match mark.get("value") {
            None => "",
            Some(value) => value
                .as_str()
                .ok_or(SpeechError::Protocol("invalid Speechify mark value"))?,
        };
        if value.len() > self.limits.max_text_field_bytes {
            return Err(SpeechError::TooLarge);
        }
        let span = mark
            .get("start")
            .and_then(source_index)
            .zip(mark.get("end").and_then(source_index))
            .filter(|(start, end)| start < end && *end < self.boundaries.len());
        let Some((start, end)) = span else {
            if let Some(word) = self.flush()? {
                result.push(word);
            }
            if let Some(word) =
                self.word(value.trim_matches(python_whitespace).to_owned(), start_time)?
            {
                result.push(word);
            }
            return Ok(result);
        };
        if let Some((pending_start, pending_end, time)) = self.pending {
            if pending_end == start {
                self.pending = Some((pending_start, end, time));
            } else {
                if let Some(word) = self.flush()? {
                    result.push(word);
                }
                self.pending = Some((start, end, start_time));
            }
        } else {
            self.pending = Some((start, end, start_time));
        }
        if end + 1 == self.boundaries.len()
            || self.text[self.boundaries[end]..]
                .chars()
                .next()
                .is_some_and(python_whitespace)
        {
            if let Some(word) = self.flush()? {
                result.push(word);
            }
        }
        Ok(result)
    }
    fn flush(&mut self) -> Result<Option<SpeechifyEvent>, SpeechError> {
        let Some((start, end, time)) = self.pending.take() else {
            return Ok(None);
        };
        let word = self.text[self.boundaries[start]..self.boundaries[end]]
            .trim_matches(python_whitespace)
            .to_owned();
        self.word(word, time)
    }
    fn word(
        &mut self,
        word: String,
        offset_seconds: f64,
    ) -> Result<Option<SpeechifyEvent>, SpeechError> {
        if word.is_empty() {
            return Ok(None);
        }
        common::checked_total(&mut self.word_count, 1, self.limits.max_word_count)?;
        common::checked_total(&mut self.word_bytes, word.len(), self.limits.max_word_bytes)?;
        Ok(Some(SpeechifyEvent::Word {
            word,
            offset_seconds,
        }))
    }
}
fn source_index(value: &Value) -> Option<usize> {
    // Python's isinstance(value, int) also accepts bool; preserve that edge case.
    match value {
        Value::Bool(value) => Some(usize::from(*value)),
        _ => value.as_u64().and_then(|value| usize::try_from(value).ok()),
    }
}
