//! Speechmatics HTTP PCM synthesis through the shared bounded speech stream.
use crate::{
    common::{self, AudioDecoder, HttpClient, RawEncoding},
    SpeechConfig, SpeechError, SpeechStream,
};
use futures_util::StreamExt;
use serde_json::json;
use std::{fmt, sync::atomic::Ordering, time::Duration};

/// Native client identity, carried in the source-shaped `sm-app` query field.
pub const SPEECHMATICS_APP: &str = concat!("pipecat-rust/", env!("CARGO_PKG_VERSION"));
const MAX_HEADER_BYTES: usize = 16 * 1024;

/// Immutable settings captured when an utterance enters `HttpTtsProcessor`.
#[derive(Clone)]
pub struct SpeechmaticsSynthesisRequest {
    pub text: String,
    pub voice: String,
    /// Positive rates preserve the pinned source contract. Hosted support is 16 kHz.
    pub sample_rate: u32,
    /// Source semantics: total 503 attempts, with one attempt even for zero.
    /// The native finite upper bound is 32; default is 5.
    pub max_retries: u8,
}
impl fmt::Debug for SpeechmaticsSynthesisRequest {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeechmaticsSynthesisRequest")
            .field("text", &"[redacted]")
            .field("voice", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("max_retries", &self.max_retries)
            .finish()
    }
}
impl SpeechmaticsSynthesisRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            voice: "sarah".into(),
            sample_rate: 16000,
            max_retries: 5,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        if self.text.is_empty()
            || self.voice.is_empty()
            || self.voice.len() > 1024
            || matches!(self.voice.as_str(), "." | "..")
            || self.sample_rate == 0
            || self.max_retries > 32
        {
            return Err(SpeechError::Invalid(
                "Speechmatics text, voice, rate or retries",
            ));
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct SpeechmaticsHttpClient {
    http: HttpClient,
}
impl SpeechmaticsHttpClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        if config.base_url.len() > 4096 || config.api_key.len() > 8192 {
            return Err(SpeechError::Invalid(
                "Speechmatics URL or credential length",
            ));
        }
        Ok(Self {
            http: HttpClient::direct(config)?,
        })
    }
    pub fn synthesize(
        &self,
        request: SpeechmaticsSynthesisRequest,
    ) -> Result<SpeechStream, SpeechError> {
        request.validate()?;
        if request.text.len() > self.http.config.max_json_bytes {
            return Err(SpeechError::TooLarge);
        }
        let body = serde_json::to_vec(&json!({"text": request.text}))?;
        if body.len() > self.http.config.max_json_bytes {
            return Err(SpeechError::TooLarge);
        }
        let http = self
            .http
            .post(&["generate", &request.voice])?
            .header(
                reqwest::header::AUTHORIZATION,
                common::secret(&format!("Bearer {}", self.http.config.api_key))?,
            )
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .query(&[
                ("output_format", format!("pcm_{}", request.sample_rate)),
                ("sm-app", SPEECHMATICS_APP.into()),
            ])
            .body(body);
        let limit = self.http.config.max_audio_bytes;
        Ok(self.http.spawn(move |tx, observed| async move {
            let mut attempts = 0u8;
            let response = loop {
                attempts += 1;
                let response = http
                    .try_clone()
                    .ok_or(SpeechError::Invalid("non-replayable Speechmatics request"))?
                    .send()
                    .await
                    .map_err(|_| SpeechError::Transport)?;
                bounded_headers(&response)?;
                if response.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE
                    && attempts < request.max_retries
                {
                    // Error bodies are never buffered or exposed; dropping also closes
                    // an unconsumed response before the bounded retry delay.
                    drop(response);
                    let millis = (250u64 << u32::from((attempts - 1).min(5))).min(8000);
                    tokio::time::sleep(Duration::from_millis(millis)).await;
                    continue;
                }
                if response.status() != reqwest::StatusCode::OK {
                    return Err(SpeechError::Http(response.status().as_u16()));
                }
                break response;
            };
            validate_format(&response, request.sample_rate)?;
            if response
                .content_length()
                .is_some_and(|size| size > limit as u64)
            {
                return Err(SpeechError::TooLarge);
            }
            let mut stream = response.bytes_stream();
            let mut decoder = AudioDecoder::new(request.sample_rate, limit, RawEncoding::Pcm16);
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(|_| SpeechError::Transport)?;
                if let Some(audio) = decoder.push(&chunk)? {
                    observed.store(true, Ordering::Release);
                    tx.send(audio)
                        .await
                        .map_err(|_| SpeechError::ConsumerClosed)?;
                }
            }
            // The shared processor treats EmptyAudio as source-compatible silent
            // completion. This protocol requires actual PCM before success.
            match decoder.finish() {
                Err(SpeechError::EmptyAudio) => Err(SpeechError::Protocol("empty PCM response")),
                result => result,
            }
        }))
    }
}
fn bounded_headers(response: &reqwest::Response) -> Result<(), SpeechError> {
    let mut bytes = 0;
    if response.headers().len() > 64 {
        return Err(SpeechError::TooLarge);
    }
    for (name, value) in response.headers() {
        common::checked_total(
            &mut bytes,
            name.as_str().len() + value.len() + 4,
            MAX_HEADER_BYTES,
        )?;
    }
    Ok(())
}
fn validate_format(response: &reqwest::Response, rate: u32) -> Result<(), SpeechError> {
    common::check_content_type(
        response,
        &["audio/pcm", "audio/x-pcm", "application/octet-stream"],
        Some((rate, 1)),
    )?;
    for value in response
        .headers()
        .get_all(reqwest::header::CONTENT_ENCODING)
    {
        if value.to_str().ok() != Some("identity") {
            return Err(SpeechError::ContentType);
        }
    }
    if response
        .headers()
        .get_all(reqwest::header::CONTENT_TYPE)
        .iter()
        .count()
        > 1
    {
        return Err(SpeechError::ContentType);
    }
    if let Some(mime) = response.headers().get(reqwest::header::CONTENT_TYPE) {
        for parameter in mime
            .to_str()
            .map_err(|_| SpeechError::ContentType)?
            .split(';')
            .skip(1)
        {
            if let Some((key, value)) = parameter.split_once('=') {
                let value = value.trim().trim_matches('"').to_ascii_lowercase();
                let valid = match key.trim().to_ascii_lowercase().as_str() {
                    "bits" | "bit-depth" => value == "16",
                    "encoding" => value == "pcm_s16le",
                    "endianness" => matches!(value.as_str(), "little" | "little-endian"),
                    _ => true,
                };
                if !valid {
                    return Err(SpeechError::ContentType);
                }
            }
        }
    }
    Ok(())
}
