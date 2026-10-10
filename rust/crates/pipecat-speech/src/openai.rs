//! OpenAI Audio REST endpoints. Realtime APIs are separate protocols.
use crate::{
    common::{self, HttpClient, RawEncoding},
    SpeechConfig, SpeechError, SpeechStream,
};
use pipecat_services::CancellationToken;
use serde::{Deserialize, Serialize};
use std::fmt;
#[derive(Debug, Clone, Serialize)]
pub struct SynthesisRequest {
    pub model: String,
    pub input: String,
    pub voice: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    pub speed: f64,
}
impl SynthesisRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            model: "gpt-4o-mini-tts".into(),
            input: text.into(),
            voice: "alloy".into(),
            instructions: None,
            speed: 1.0,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        if self.input.is_empty()
            || self.model.is_empty()
            || self.voice.is_empty()
            || !common::finite_range(Some(self.speed), 0.25, 4.0)
        {
            Err(SpeechError::Invalid("text, model, voice or speed"))
        } else {
            Ok(())
        }
    }
}
#[derive(Debug, Clone)]
pub struct TranscriptionRequest {
    pub model: String,
    pub language: Option<String>,
    pub prompt: Option<String>,
}
impl Default for TranscriptionRequest {
    fn default() -> Self {
        Self {
            model: "gpt-transcribe".into(),
            language: Some("en".into()),
            prompt: None,
        }
    }
}
#[derive(Debug, Deserialize, PartialEq, Eq)]
pub struct Transcript {
    pub text: String,
}
/// Optional controls for the documented OpenAI transcription model names.
///
/// Empty options keep compatible-endpoint model names usable. A nonempty option
/// is rejected when its model capability is not documented; model names are not
/// guessed from prefixes, and this does not verify hosted model availability.
/// Text limits are UTF-8 bytes: model 256, language 64, prompt 8192, up to 100
/// keywords of 256 bytes each and 8192 bytes together, and 16 KiB in total.
#[derive(Clone, Default, PartialEq)]
pub struct OpenAiTranscriptionOptions {
    pub temperature: Option<f64>,
    /// Sent as repeated `keywords[]` fields, supported by `gpt-transcribe`.
    pub keywords: Vec<String>,
    /// Requests verbose Whisper metadata or GPT token log probabilities.
    /// Unsupported models return an error instead of dropping the request.
    pub include_prob_metrics: bool,
}
impl fmt::Debug for OpenAiTranscriptionOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiTranscriptionOptions")
            .field("temperature", &self.temperature)
            .field("keyword_count", &self.keywords.len())
            .field("include_prob_metrics", &self.include_prob_metrics)
            .finish_non_exhaustive()
    }
}
impl OpenAiTranscriptionOptions {
    pub(crate) fn validate(&self, request: &TranscriptionRequest) -> Result<(), SpeechError> {
        if request.model.is_empty() {
            return Err(SpeechError::Invalid("model"));
        }
        if !common::finite_range(self.temperature, 0.0, 1.0) {
            return Err(SpeechError::Invalid("OpenAI transcription temperature"));
        }
        if request.model.len() > 256
            || request.language.as_ref().is_some_and(|v| v.len() > 64)
            || request.prompt.as_ref().is_some_and(|v| v.len() > 8192)
            || self.keywords.len() > 100
            || self.keywords.iter().any(|v| v.len() > 256)
        {
            return Err(SpeechError::Invalid("OpenAI transcription settings limits"));
        }
        let keyword_bytes = self
            .keywords
            .iter()
            .try_fold(0usize, |total, value| total.checked_add(value.len()))
            .ok_or(SpeechError::Invalid("OpenAI transcription settings limits"))?;
        let total_bytes = [
            request.model.len(),
            request.language.as_ref().map_or(0, String::len),
            request.prompt.as_ref().map_or(0, String::len),
            keyword_bytes,
        ]
        .into_iter()
        .try_fold(0usize, usize::checked_add)
        .ok_or(SpeechError::Invalid("OpenAI transcription settings limits"))?;
        if keyword_bytes > 8192 || total_bytes > 16 * 1024 {
            return Err(SpeechError::Invalid("OpenAI transcription settings limits"));
        }
        let documented_model = matches!(
            request.model.as_str(),
            "whisper-1"
                | "gpt-transcribe"
                | "gpt-4o-transcribe"
                | "gpt-4o-mini-transcribe"
                | "gpt-4o-mini-transcribe-2025-12-15"
                | "gpt-4o-transcribe-diarize"
        );
        if self.temperature.is_some() && !documented_model {
            return Err(SpeechError::Invalid(
                "OpenAI temperature is unsupported for this model",
            ));
        }
        if !self.keywords.is_empty()
            && (request.model != "gpt-transcribe" || self.keywords.iter().any(String::is_empty))
        {
            return Err(SpeechError::Invalid(
                "OpenAI keywords are empty or unsupported for this model",
            ));
        }
        if self.include_prob_metrics
            && !matches!(
                request.model.as_str(),
                "whisper-1"
                    | "gpt-4o-transcribe"
                    | "gpt-4o-mini-transcribe"
                    | "gpt-4o-mini-transcribe-2025-12-15"
            )
        {
            return Err(SpeechError::Invalid(
                "OpenAI probability metrics are unsupported for this model",
            ));
        }
        if request.model == "gpt-4o-transcribe-diarize" && request.prompt.is_some() {
            return Err(SpeechError::Invalid(
                "OpenAI prompt is unsupported for this model",
            ));
        }
        Ok(())
    }
}
/// A typed text projection alongside the complete parsed provider JSON.
///
/// The HTTP body is bounded by `SpeechConfig::max_json_bytes`. Unknown fields
/// and explicit nulls are retained; JSON formatting and duplicate keys are not.
/// Present token/segment log probabilities must be finite and nonpositive;
/// segment no-speech probabilities must be in 0..=1. Missing/null values remain
/// absent/null. Other metadata is retained opaquely, not validated or inferred.
/// Debug intentionally omits transcript and provider metadata.
pub struct OpenAiTranscriptionResult {
    pub transcript: Transcript,
    pub result: serde_json::Value,
}
impl fmt::Debug for OpenAiTranscriptionResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OpenAiTranscriptionResult")
            .finish_non_exhaustive()
    }
}
fn validate_probability_metadata(result: &serde_json::Value) -> Result<(), SpeechError> {
    for (collection, fields) in [
        ("logprobs", &[("logprob", f64::NEG_INFINITY, 0.0)][..]),
        (
            "segments",
            &[
                ("avg_logprob", f64::NEG_INFINITY, 0.0),
                ("no_speech_prob", 0.0, 1.0),
            ][..],
        ),
    ] {
        let Some(entries) = result.get(collection).filter(|value| !value.is_null()) else {
            continue;
        };
        let entries = entries.as_array().ok_or(SpeechError::Protocol(
            "invalid transcription probability metadata",
        ))?;
        for entry in entries {
            let entry = entry.as_object().ok_or(SpeechError::Protocol(
                "invalid transcription probability metadata",
            ))?;
            for &(field, minimum, maximum) in fields {
                if let Some(value) = entry.get(field).filter(|value| !value.is_null()) {
                    if !value
                        .as_f64()
                        .is_some_and(|v| v.is_finite() && v >= minimum && v <= maximum)
                    {
                        return Err(SpeechError::Protocol(
                            "invalid transcription probability metadata",
                        ));
                    }
                }
            }
        }
    }
    Ok(())
}
#[derive(Clone)]
pub struct SpeechClient {
    http: HttpClient,
}
impl SpeechClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        Ok(Self {
            http: HttpClient::new(config)?,
        })
    }
    pub(crate) fn config(&self) -> &SpeechConfig {
        &self.http.config
    }
    pub async fn transcribe_pcm(
        &self,
        pcm: &[u8],
        sample_rate: u32,
        channels: u16,
        request: TranscriptionRequest,
        cancel: CancellationToken,
    ) -> Result<Transcript, SpeechError> {
        if request.model.is_empty() {
            return Err(SpeechError::Invalid("model"));
        }
        let part = self
            .http
            .wav_part(pcm, sample_rate, channels, "audio/wav")?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", request.model)
            .text("response_format", "json");
        if let Some(lang) = request.language {
            form = form.text("language", lang);
        }
        if let Some(prompt) = request.prompt {
            form = form.text("prompt", prompt);
        }
        let request = self
            .http
            .post(&["audio", "transcriptions"])?
            .bearer_auth(&self.http.config.api_key)
            .multipart(form);
        self.http.json(request, cancel).await
    }
    /// Transcribes PCM with validated optional controls and bounded raw metadata.
    ///
    /// Whisper probability requests select `verbose_json`; the documented GPT
    /// probability models use `json` with `include[]=logprobs`. Other requests
    /// retain the client's JSON response format. No provider retries are made.
    /// `gpt-4o-transcribe-diarize` is limited to 30 seconds because this method
    /// does not expose the chunking strategy required for longer inputs.
    pub async fn transcribe_pcm_with_options(
        &self,
        pcm: &[u8],
        sample_rate: u32,
        channels: u16,
        request: TranscriptionRequest,
        options: OpenAiTranscriptionOptions,
        cancel: CancellationToken,
    ) -> Result<OpenAiTranscriptionResult, SpeechError> {
        options.validate(&request)?;
        if request.model == "gpt-4o-transcribe-diarize" {
            let format = pipecat_audio::PcmFormat::new(sample_rate, channels)?;
            let frames = u64::try_from(format.frame_count(pcm)?)
                .map_err(|_| SpeechError::Invalid("OpenAI diarization audio duration"))?;
            let maximum = u64::from(sample_rate)
                .checked_mul(30)
                .ok_or(SpeechError::Invalid("OpenAI diarization audio duration"))?;
            if frames > maximum {
                return Err(SpeechError::Invalid(
                    "OpenAI diarization above 30 seconds requires a chunking strategy",
                ));
            }
        }
        let verbose = options.include_prob_metrics && request.model == "whisper-1";
        let part = self
            .http
            .wav_part(pcm, sample_rate, channels, "audio/wav")?;
        let mut form = reqwest::multipart::Form::new()
            .part("file", part)
            .text("model", request.model)
            .text(
                "response_format",
                if verbose { "verbose_json" } else { "json" },
            );
        if let Some(language) = request.language {
            form = form.text("language", language);
        }
        if let Some(prompt) = request.prompt {
            form = form.text("prompt", prompt);
        }
        if let Some(temperature) = options.temperature {
            form = form.text("temperature", temperature.to_string());
        }
        // openai-python 3.16.2 BaseClient::_serialize_multipartform uses brackets.
        for keyword in options.keywords {
            form = form.text("keywords[]", keyword);
        }
        if options.include_prob_metrics && !verbose {
            form = form.text("include[]", "logprobs");
        }
        let http = self
            .http
            .post(&["audio", "transcriptions"])?
            .bearer_auth(&self.http.config.api_key)
            .multipart(form);
        let result: serde_json::Value = self.http.json(http, cancel).await?;
        let transcript = Transcript::deserialize(&result)?;
        validate_probability_metadata(&result)?;
        Ok(OpenAiTranscriptionResult { transcript, result })
    }
    pub fn synthesize(&self, request: SynthesisRequest) -> Result<SpeechStream, SpeechError> {
        request.validate()?;
        let mut body = serde_json::to_value(request)?;
        body["response_format"] = "pcm".into();
        let request = self
            .http
            .post(&["audio", "speech"])?
            .bearer_auth(&self.http.config.api_key)
            .json(&body);
        Ok(self.http.raw_stream(
            request,
            24000,
            RawEncoding::Pcm16,
            &["audio/pcm", "audio/x-pcm", "application/octet-stream"],
            false,
        ))
    }
}
