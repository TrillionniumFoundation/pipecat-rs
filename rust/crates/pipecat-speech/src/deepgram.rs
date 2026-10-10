//! Deepgram HTTP TTS (`services/deepgram/tts.py::DeepgramHttpTTSService`).
//! The source's Deepgram STT service is WebSocket-based; it is not a REST adapter.
use crate::{
    common::{self, HttpClient, RawEncoding},
    SpeechConfig, SpeechError, SpeechStream,
};
use serde_json::json;

/// Raw provider encodings with a native PCM16 decoder. Container is always `none`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DeepgramEncoding {
    #[default]
    Linear16,
    Mulaw,
    Alaw,
}
impl DeepgramEncoding {
    fn wire(self) -> &'static str {
        match self {
            Self::Linear16 => "linear16",
            Self::Mulaw => "mulaw",
            Self::Alaw => "alaw",
        }
    }
    fn decoder(self) -> RawEncoding {
        match self {
            Self::Linear16 => RawEncoding::Pcm16,
            Self::Mulaw => RawEncoding::Mulaw,
            Self::Alaw => RawEncoding::Alaw,
        }
    }
    fn mime(self) -> &'static [&'static str] {
        match self {
            Self::Linear16 => &[
                "audio/l16",
                "audio/pcm",
                "audio/x-pcm",
                "application/octet-stream",
            ],
            Self::Mulaw => &["audio/mulaw", "audio/basic", "application/octet-stream"],
            Self::Alaw => &["audio/alaw", "application/octet-stream"],
        }
    }
}
#[derive(Debug, Clone)]
pub struct DeepgramSynthesisRequest {
    pub text: String,
    pub voice: String,
    pub sample_rate: u32,
    pub encoding: DeepgramEncoding,
    pub speed: Option<f64>,
    pub mip_opt_out: Option<bool>,
}
impl DeepgramSynthesisRequest {
    pub fn new(text: impl Into<String>) -> Self {
        Self {
            text: text.into(),
            voice: "aura-2-helena-en".into(),
            sample_rate: 24000,
            encoding: DeepgramEncoding::Linear16,
            speed: None,
            mip_opt_out: None,
        }
    }
    pub(crate) fn validate(&self) -> Result<(), SpeechError> {
        let rate_valid = match self.encoding {
            DeepgramEncoding::Linear16 => {
                matches!(self.sample_rate, 8000 | 16000 | 24000 | 32000 | 48000)
            }
            _ => matches!(self.sample_rate, 8000 | 16000),
        };
        if self.text.is_empty()
            || self.voice.is_empty()
            || !rate_valid
            || !common::finite_range(self.speed, 0.7, 1.5)
        {
            Err(SpeechError::Invalid(
                "Deepgram text, voice, encoding/rate or speed",
            ))
        } else {
            Ok(())
        }
    }
}
#[derive(Clone)]
pub struct DeepgramHttpClient {
    http: HttpClient,
}
impl DeepgramHttpClient {
    pub fn new(config: SpeechConfig) -> Result<Self, SpeechError> {
        Ok(Self {
            http: HttpClient::new(config)?,
        })
    }
    pub fn synthesize(
        &self,
        request: DeepgramSynthesisRequest,
    ) -> Result<SpeechStream, SpeechError> {
        request.validate()?;
        let mut query = vec![
            ("model", request.voice),
            ("encoding", request.encoding.wire().into()),
            ("sample_rate", request.sample_rate.to_string()),
            ("container", "none".into()),
        ];
        if let Some(speed) = request.speed {
            query.push(("speed", speed.to_string()));
        }
        if let Some(opt_out) = request.mip_opt_out {
            query.push(("mip_opt_out", opt_out.to_string()));
        }
        let http = self
            .http
            .post(&["v1", "speak"])?
            .header(
                reqwest::header::AUTHORIZATION,
                common::secret(&format!("Token {}", self.http.config.api_key))?,
            )
            .query(&query)
            .json(&json!({"text": request.text}));
        Ok(self.http.raw_stream(
            http,
            request.sample_rate,
            request.encoding.decoder(),
            request.encoding.mime(),
            false,
        ))
    }
}
