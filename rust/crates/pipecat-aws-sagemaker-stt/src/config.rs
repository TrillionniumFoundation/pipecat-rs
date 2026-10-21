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
}
impl Default for TranscriptionSettings {
    fn default() -> Self {
        Self {
            model: "cache-aware-parakeet-rnnt-en-US-asr-streaming-sortformer".into(),
            language: "en-US".into(),
            sample_rate_hz: 16_000,
            user_id: String::new(),
        }
    }
}
impl TranscriptionSettings {
    pub fn from_json(raw: &[u8]) -> Result<Self, Error> {
        if raw.len() > 4096 {
            return Err(Error::TooLarge);
        }
        crate::protocol::check_json_depth(raw, 4)?;
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
                .all(|b| b.is_ascii_alphanumeric() || b"-_./".contains(&b))
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
            || self.chunk_bytes.div_ceil(3) * 4 + 512 > self.duplex.max_input_frame_bytes
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
            region: region.into(),
            endpoint_name: endpoint_name.into(),
            credentials,
            origin: None,
            address: None,
            settings: Default::default(),
            limits: Default::default(),
        };
        value.validate()?;
        Ok(value)
    }
    pub fn with_settings(mut self, settings: TranscriptionSettings) -> Result<Self, Error> {
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
        // Validate actual immutable startup before pipeline/output/network effects,
        // not merely the individual frame ceiling used by the transport.
        let startup =
            crate::protocol::session(&self.settings, self.limits.duplex.max_input_frame_bytes)?;
        if startup.len() > self.limits.duplex.startup_max_bytes {
            return Err(Error::TooLarge);
        }
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
