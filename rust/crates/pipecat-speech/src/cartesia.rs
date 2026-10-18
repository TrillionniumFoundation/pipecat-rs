//! Cartesia HTTP TTS (`services/cartesia/tts.py::CartesiaHttpTTSService`).
use crate::{
    common::{self, HttpClient, RawEncoding},
    SpeechConfig, SpeechError, SpeechStream,
};
use serde::Serialize;
use serde_json::json;
pub const CARTESIA_API_VERSION: &str = "2026-03-01";
#[derive(Debug, Clone, Default, Serialize)]
pub struct GenerationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub volume: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub emotion: Option<String>,
}
/// The wire format is deliberately raw PCM16LE mono. Other containers and codecs
/// cannot be represented as `SpeechChunk` without an explicit decoder.
#[derive(Debug, Clone)]
pub struct CartesiaSynthesisRequest {
    pub text: String,
    pub model: String,
    pub voice_id: String,
    pub language: Option<String>,
    pub sample_rate: u32,
    pub generation_config: Option<GenerationConfig>,
    pub pronunciation_dict_id: Option<String>,
}
impl CartesiaSynthesisRequest {
    pub fn new(text: impl Into<String>, voice_id: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            model: "sonic-3.6".into(),
            voice_id: voice_id.into(),
            language: Some("en".into()),
            sample_rate: 24000,
            generation_config: None,
            pronunciation_dict_id: None,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        if self.text.is_empty()
            || self.model.is_empty()
            || self.voice_id.is_empty()
            || self.sample_rate == 0
            || !common::optional_text(&self.language)
            || !common::optional_text(&self.pronunciation_dict_id)
            || self.generation_config.as_ref().is_some_and(|g| {
                !common::finite_range(g.volume, 0.5, 2.0)
                    || !common::finite_range(g.speed, 0.6, 1.5)
                    || !common::optional_text(&g.emotion)
            })
        {
            Err(SpeechError::Invalid(
                "Cartesia text, model, voice, format or generation settings",
            ))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone)]
pub struct CartesiaHttpClient {
    http: HttpClient,
}
impl CartesiaHttpClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        Ok(Self {
            http: HttpClient::new(config)?,
        })
    }
    /// Construct an HTTP/1-only client without environment proxies or transport
    /// retries. Redirects remain disabled, as with [`Self::new`].
    pub fn new_direct(config: SpeechConfig) -> Result<Self, SpeechError> {
        Ok(Self {
            http: HttpClient::direct(config)?,
        })
    }
    pub fn synthesize(
        &self,
        request: CartesiaSynthesisRequest,
    ) -> Result<SpeechStream, SpeechError> {
        request.validate()?;
        let mut payload = json!({ "model_id": request.model, "transcript": request.text,
            "voice": { "mode": "id", "id": request.voice_id },
            "output_format": { "container": "raw", "encoding": "pcm_s16le", "sample_rate": request.sample_rate } });
        if let Some(language) = request.language {
            payload["language"] = language.into();
        }
        if let Some(generation) = request.generation_config {
            payload["generation_config"] = serde_json::to_value(generation)?;
        }
        if let Some(id) = request.pronunciation_dict_id {
            payload["pronunciation_dict_id"] = id.into();
        }
        let http = self
            .http
            .post(&["tts", "bytes"])?
            .header("x-api-key", common::secret(&self.http.config.api_key)?)
            .header("cartesia-version", CARTESIA_API_VERSION)
            .json(&payload);
        Ok(self.http.raw_stream(
            http,
            request.sample_rate,
            RawEncoding::Pcm16,
            &["audio/pcm", "audio/x-pcm", "application/octet-stream"],
            true,
        ))
    }
}
