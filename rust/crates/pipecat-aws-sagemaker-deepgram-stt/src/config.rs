use crate::{DuplexLimits, Error};
use pipecat_aws_common::{auth, AwsCredentials};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default, deny_unknown_fields)]
pub struct TranscriptionSettings {
    pub model: String,
    pub language: String,
    pub sample_rate_hz: u32,
    pub user_id: String,
    pub detect_entities: bool,
    pub diarize: bool,
    pub dictation: bool,
    pub interim_results: bool,
    pub numerals: bool,
    pub punctuate: bool,
    pub smart_format: bool,
    #[serde(default, deserialize_with = "present_bool")]
    pub mip_opt_out: Option<bool>,
}
impl Default for TranscriptionSettings {
    fn default() -> Self {
        Self {
            model: "nova-3".into(),
            language: "en".into(),
            sample_rate_hz: 16_000,
            user_id: String::new(),
            detect_entities: false,
            diarize: false,
            dictation: false,
            interim_results: true,
            numerals: false,
            punctuate: true,
            smart_format: false,
            mip_opt_out: None,
        }
    }
}
fn present_bool<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    bool::deserialize(d).map(Some)
}
impl TranscriptionSettings {
    /// Pinned dataclass declaration order, followed by model and connection fields.
    pub fn model_query_string(&self) -> Result<String, Error> {
        self.validate()?;
        let mut query = format!("detect_entities={}&diarize={}&dictation={}&interim_results={}&numerals={}&punctuate={}&smart_format={}&model={}&language={}&encoding=linear16&channels=1&multichannel=false&sample_rate={}",
            self.detect_entities, self.diarize, self.dictation, self.interim_results,
            self.numerals, self.punctuate, self.smart_format, self.model, self.language, self.sample_rate_hz);
        if let Some(value) = self.mip_opt_out {
            query.push_str(&format!("&mip_opt_out={value}"));
        }
        if query.len() > 2048 {
            return Err(Error::TooLarge);
        }
        Ok(query)
    }

    pub fn from_json(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() > 4096 {
            return Err(Error::TooLarge);
        }
        crate::protocol::check_json_depth(raw, 4)?;
        let shape: serde_json::Value =
            serde_json::from_slice(raw).map_err(|_| Error::Invalid("settings JSON"))?;
        if !shape.is_object() {
            return Err(Error::Invalid("settings object required"));
        }
        let settings: Self =
            serde_json::from_slice(raw).map_err(|_| Error::Invalid("settings JSON"))?;
        settings.validate()?;
        Ok(settings)
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.model.is_empty()
            || self.model.len() > 256
            || !self
                .model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || self.language.is_empty()
            || self.language.len() > 63
            || !self
                .language
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !(8_000..=48_000).contains(&self.sample_rate_hz)
            || self.user_id.len() > 256
            || self.user_id.chars().any(char::is_control)
        {
            return Err(Error::Invalid("model/language/rate/user"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct SttLimits {
    pub duplex: DuplexLimits,
    pub max_pcm_bytes: usize,
    pub max_audio_seconds: u32,
    pub chunk_bytes: usize,
    pub max_chunks: usize,
    pub max_response_events: usize,
    pub max_finalize_events: usize,
    pub max_result_bytes: usize,
    pub max_json_depth: usize,
    pub max_transcript_bytes: usize,
    pub max_output_bytes: usize,
    pub output_timeout: Duration,
}
impl Default for SttLimits {
    fn default() -> Self {
        Self {
            duplex: DuplexLimits::default(),
            max_pcm_bytes: 8 * 1024 * 1024,
            max_audio_seconds: 120,
            chunk_bytes: 3200,
            max_chunks: 2400,
            max_response_events: 8192,
            max_finalize_events: 64,
            max_result_bytes: 64 * 1024,
            max_json_depth: 32,
            max_transcript_bytes: 1024 * 1024,
            max_output_bytes: 8 * 1024 * 1024,
            output_timeout: Duration::from_secs(5),
        }
    }
}
impl SttLimits {
    pub fn validate(&self) -> Result<(), Error> {
        self.duplex.validate()?;
        if self.max_pcm_bytes == 0
            || self.max_pcm_bytes > 32 * 1024 * 1024
            || self.max_audio_seconds == 0
            || self.max_audio_seconds > 300
            || self.chunk_bytes == 0
            || self.chunk_bytes > 64 * 1024
            || !self.chunk_bytes.is_multiple_of(2)
            || self.max_chunks == 0
            || self.max_chunks > 8192
            || self.max_finalize_events == 0
            || self.max_finalize_events > 256
            || self.max_response_events == 0
            || self.max_response_events > 65536
            || !(64..=256 * 1024).contains(&self.max_result_bytes)
            || !(1..=64).contains(&self.max_json_depth)
            || self.max_transcript_bytes == 0
            || self.max_transcript_bytes > 16 * 1024 * 1024
            || self.max_output_bytes == 0
            || self.max_output_bytes > 32 * 1024 * 1024
            || self.output_timeout.is_zero()
            || self.output_timeout > self.duplex.session_timeout
            || self.chunk_bytes + 512 > self.duplex.max_input_frame_bytes
            || self.max_result_bytes + 1024 > self.duplex.max_response_frame_bytes
        {
            return Err(Error::Invalid("finite limits"));
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct SageMakerSttConfig {
    pub(crate) region: String,
    pub(crate) endpoint_name: String,
    pub(crate) credentials: AwsCredentials,
    pub(crate) origin: Option<Url>,
    pub(crate) address: Option<SocketAddr>,
    pub(crate) settings: TranscriptionSettings,
    pub(crate) limits: SttLimits,
}
impl fmt::Debug for SageMakerSttConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SageMakerSttConfig")
            .field("credentials", &"[redacted]")
            .finish_non_exhaustive()
    }
}
impl SageMakerSttConfig {
    pub fn new(
        region: impl Into<String>,
        endpoint_name: impl Into<String>,
        credentials: AwsCredentials,
    ) -> Result<Self, Error> {
        let value = Self {
            region: region.into().into_boxed_str().into_string(),
            endpoint_name: endpoint_name.into().into_boxed_str().into_string(),
            credentials,
            origin: None,
            address: None,
            settings: Default::default(),
            limits: Default::default(),
        };
        value.validate()?;
        Ok(value)
    }
    pub fn with_settings(mut self, mut settings: TranscriptionSettings) -> Result<Self, Error> {
        settings.validate()?;
        settings.model = settings.model.into_boxed_str().into_string();
        settings.language = settings.language.into_boxed_str().into_string();
        settings.user_id = settings.user_id.into_boxed_str().into_string();
        self.settings = settings;
        self.validate()?;
        Ok(self)
    }
    pub fn with_limits(mut self, limits: SttLimits) -> Result<Self, Error> {
        self.limits = limits;
        self.validate()?;
        Ok(self)
    }
    pub fn with_address(mut self, address: SocketAddr) -> Result<Self, Error> {
        if address.port() != 8443
            || address.ip().is_unspecified()
            || address.ip().is_multicast()
            || matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast())
            || matches!(address, SocketAddr::V6(ip) if ip.scope_id()!=0 || ip.flowinfo()!=0)
            || self.origin.is_some()
        {
            return Err(Error::Invalid("explicit HTTPS route"));
        }
        self.address = Some(address);
        Ok(self)
    }
    /// Plain HTTP is available only for literal-loopback deterministic fixtures.
    pub fn with_loopback_origin(mut self, origin: &str) -> Result<Self, Error> {
        let url = Url::parse(origin).map_err(|_| Error::Invalid("loopback origin"))?;
        if url.scheme() != "http"
            || url
                .host_str()
                .and_then(|s| s.trim_matches(['[', ']']).parse::<IpAddr>().ok())
                .is_none_or(|ip| !ip.is_loopback())
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port_or_known_default().is_none_or(|p| p == 0)
            || self.address.is_some()
        {
            return Err(Error::Invalid("literal loopback origin"));
        }
        self.origin = Some(url);
        Ok(self)
    }
    pub fn settings(&self) -> &TranscriptionSettings {
        &self.settings
    }
    pub fn limits(&self) -> &SttLimits {
        &self.limits
    }
    pub fn validate(&self) -> Result<(), Error> {
        auth::region(&self.region).map_err(|_| Error::Invalid("region"))?;
        if self.region.starts_with("cn-")
            || self.region.starts_with("us-gov-")
            || self.region.starts_with("us-iso")
            || self.endpoint_name.is_empty()
            || self.endpoint_name.len() > 63
            || !self
                .endpoint_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !self.endpoint_name.as_bytes()[0].is_ascii_alphanumeric()
            || !self.endpoint_name.as_bytes()[self.endpoint_name.len() - 1].is_ascii_alphanumeric()
        {
            return Err(Error::Invalid("commercial region/endpoint name"));
        }
        self.settings.validate()?;
        self.limits.validate()?;
        self.settings.model_query_string()?;
        Ok(())
    }
    pub(crate) fn url(&self) -> Result<Url, Error> {
        let origin = self
            .origin
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| {
                format!(
                    "https://runtime.sagemaker.{}.amazonaws.com:8443/",
                    self.region
                )
            });
        Url::parse(&format!(
            "{}endpoints/{}/invocations-bidirectional-stream",
            origin, self.endpoint_name
        ))
        .map_err(|_| Error::Invalid("endpoint"))
    }
    pub(crate) fn validate_pcm(&self, pcm: &[u8]) -> Result<(), Error> {
        self.validate()?;
        if pcm.is_empty() || !pcm.len().is_multiple_of(2) {
            return Err(Error::Invalid("nonempty whole PCM16 samples"));
        }
        if pcm.len() > self.limits.max_pcm_bytes
            || pcm.len().div_ceil(self.limits.chunk_bytes) > self.limits.max_chunks
            || pcm.len() as u64
                > u64::from(self.settings.sample_rate_hz)
                    * 2
                    * u64::from(self.limits.max_audio_seconds)
        {
            return Err(Error::TooLarge);
        }
        Ok(())
    }
}

/// Explicit finite-file controls at completed chunk boundaries; no initial pause.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct TimelineMarker {
    pub after_chunk: usize,
    #[serde(default)]
    pub pause_ms: u64,
    #[serde(default)]
    pub vad_stopped: bool,
}
#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(transparent)]
pub struct Timeline(pub Vec<TimelineMarker>);
impl Timeline {
    pub fn validate(&self, chunks: usize, limits: &SttLimits) -> Result<(), Error> {
        if self.0.len() > 64 {
            return Err(Error::TooLarge);
        }
        if self.0.iter().filter(|m| m.vad_stopped).count() > limits.max_finalize_events {
            return Err(Error::TooLarge);
        }
        let (mut prior, mut total) = (0, 0u64);
        for marker in &self.0 {
            if marker.after_chunk <= prior || marker.after_chunk > chunks || marker.pause_ms > 15000
            {
                return Err(Error::Invalid("ordered bounded timeline"));
            }
            total = total.checked_add(marker.pause_ms).ok_or(Error::TooLarge)?;
            prior = marker.after_chunk;
        }
        if total > 60000 {
            return Err(Error::TooLarge);
        }
        let silence = Duration::from_millis(total)
            .checked_add(Duration::from_secs(15))
            .ok_or(Error::TooLarge)?;
        let session = silence
            .checked_add(limits.duplex.connect_timeout)
            .and_then(|d| d.checked_add(limits.duplex.write_timeout))
            .and_then(|d| d.checked_add(limits.duplex.close_timeout))
            .ok_or(Error::TooLarge)?;
        if limits.duplex.idle_timeout < silence || limits.duplex.session_timeout < session {
            return Err(Error::Invalid("timeline deadline reserve"));
        }
        Ok(())
    }
}
/// Strict CLI manifest, separate from Python's legacy settings merge contract.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct RunManifest {
    pub settings: TranscriptionSettings,
    pub timeline: Timeline,
}
impl RunManifest {
    pub fn from_json(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() > 16384 {
            return Err(Error::TooLarge);
        }
        crate::protocol::check_json_depth(raw, 5)?;
        let shape: serde_json::Value =
            serde_json::from_slice(raw).map_err(|_| Error::Invalid("run manifest"))?;
        if !shape.is_object()
            || shape.get("settings").is_some_and(|s| !s.is_object())
            || shape.get("timeline").is_some_and(|t| {
                t.as_array()
                    .is_none_or(|markers| markers.iter().any(|m| !m.is_object()))
            })
        {
            return Err(Error::Invalid("manifest object shapes"));
        }
        let value: Self =
            serde_json::from_slice(raw).map_err(|_| Error::Invalid("run manifest"))?;
        value.settings.validate()?;
        if value.timeline.0.len() > 64 {
            return Err(Error::TooLarge);
        }
        Ok(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn pinned_query_order_and_strict_settings() {
        assert_eq!(TranscriptionSettings::default().model_query_string().unwrap(), "detect_entities=false&diarize=false&dictation=false&interim_results=true&numerals=false&punctuate=true&smart_format=false&model=nova-3&language=en&encoding=linear16&channels=1&multichannel=false&sample_rate=16000");
        let value = TranscriptionSettings::from_json(
            br#"{"language":"fr-CA","mip_opt_out":false,"punctuate":false}"#,
        )
        .unwrap();
        assert!(value
            .model_query_string()
            .unwrap()
            .ends_with("sample_rate=16000&mip_opt_out=false"));
        for raw in [
            b"[]".as_slice(),
            br#"{"extra":{}}"#,
            br#"{"model":null}"#,
            br#"{"mip_opt_out":null}"#,
            br#"{"mip_opt_out":false,"mip_opt_out":true}"#,
            br#"{"language":"en","language":"fr"}"#,
            br#"{"model":"bad/path"}"#,
            br#"{"model":"x&y"}"#,
            br#"{"keywords":[]}"#,
        ] {
            assert!(TranscriptionSettings::from_json(raw).is_err());
        }
    }
    #[test]
    fn bounded_timeline_and_manifest_are_atomic() {
        let limits = SttLimits::default();
        for timeline in [
            Timeline(vec![TimelineMarker {
                after_chunk: 0,
                pause_ms: 0,
                vad_stopped: false,
            }]),
            Timeline(vec![TimelineMarker {
                after_chunk: 3,
                pause_ms: 0,
                vad_stopped: false,
            }]),
            Timeline(vec![TimelineMarker {
                after_chunk: 1,
                pause_ms: 15001,
                vad_stopped: false,
            }]),
            Timeline(vec![
                TimelineMarker {
                    after_chunk: 1,
                    pause_ms: 0,
                    vad_stopped: false
                };
                2
            ]),
        ] {
            assert!(timeline.validate(2, &limits).is_err());
        }
        let valid = Timeline(vec![TimelineMarker {
            after_chunk: 1,
            pause_ms: 12000,
            vad_stopped: false,
        }]);
        valid.validate(2, &limits).unwrap();
        let mut short = limits;
        short.duplex.idle_timeout = Duration::from_secs(26);
        assert!(valid.validate(2, &short).is_err());
        for raw in [
            b"[]".as_slice(),
            br#"{"settings":[]}"#,
            br#"{"timeline":[[1,0,false]]}"#,
            br#"{"timeline":null}"#,
            br#"{"settings":{},"settings":{}}"#,
            br#"{"timeline":[{"after_chunk":1,"pause_ms":null}]}"#,
            br#"{"timeline":[{"after_chunk":1,"extra":true}]}"#,
        ] {
            assert!(RunManifest::from_json(raw).is_err());
        }
    }
}
