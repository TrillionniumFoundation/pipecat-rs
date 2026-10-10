//! OpenAI Audio REST endpoints. Realtime APIs are separate protocols.
use crate::{
    common::{self, HttpClient, RawEncoding},
    SpeechConfig, SpeechError, SpeechStream,
};
use pipecat_services::CancellationToken;
use serde::{Deserialize, Serialize};
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
