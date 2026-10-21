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
pub struct SynthesisSettings {
    pub voice_name: String,
    pub language_code: String,
    pub sample_rate_hz: u32,
}
impl Default for SynthesisSettings {
    fn default() -> Self {
        Self {
            voice_name: "Magpie-Multilingual.EN-US.Aria".into(),
            language_code: "en-US".into(),
            sample_rate_hz: 24_000,
        }
    }
}
impl SynthesisSettings {
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
        if self.voice_name.is_empty()
            || self.voice_name.len() > 256
            || self.voice_name.chars().any(char::is_control)
            || self.language_code.is_empty()
            || self.language_code.len() > 63
            || !self
                .language_code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !(8_000..=48_000).contains(&self.sample_rate_hz)
        {
            return Err(Error::Invalid("voice/language/rate"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug)]
pub struct TtsLimits {
    pub duplex: DuplexLimits,
    pub max_utterances: usize,
    pub max_text_bytes: usize,
    pub max_result_bytes: usize,
    pub max_json_depth: usize,
    pub max_response_events: usize,
    pub max_output_bytes: usize,
    pub output_timeout: Duration,
    pub interruption_grace: Duration,
    pub app_timeout: Duration,
}
impl Default for TtsLimits {
    fn default() -> Self {
        Self {
            duplex: DuplexLimits::default(),
            max_utterances: 16,
            max_text_bytes: 16 * 1024,
            max_result_bytes: 64 * 1024,
            max_json_depth: 32,
            max_response_events: 8192,
            max_output_bytes: 8 * 1024 * 1024,
            output_timeout: Duration::from_secs(5),
            interruption_grace: Duration::from_millis(500),
            app_timeout: Duration::from_secs(300),
        }
    }
}
impl TtsLimits {
    pub fn validate(&self) -> Result<(), Error> {
        self.duplex.validate()?;
        if !(1..=64).contains(&self.max_utterances)
            || !(1..=64 * 1024).contains(&self.max_text_bytes)
            || !(64..=256 * 1024).contains(&self.max_result_bytes)
            || !(1..=64).contains(&self.max_json_depth)
            || !(1..=65536).contains(&self.max_response_events)
            || !(1..=32 * 1024 * 1024).contains(&self.max_output_bytes)
            || self.output_timeout.is_zero()
            || self.output_timeout > self.duplex.session_timeout
            || self.interruption_grace.is_zero()
            || self.interruption_grace > Duration::from_secs(5)
            || self.app_timeout.is_zero()
            || self.app_timeout > Duration::from_secs(420)
            || self.max_result_bytes + 1024 > self.duplex.max_response_frame_bytes
        {
            return Err(Error::Invalid("finite limits"));
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct SageMakerBidiTtsConfig {
    pub(crate) region: String,
    pub(crate) endpoint_name: String,
    pub(crate) credentials: AwsCredentials,
    pub(crate) origin: Option<Url>,
    pub(crate) address: Option<SocketAddr>,
    pub(crate) settings: SynthesisSettings,
    pub(crate) limits: TtsLimits,
}
impl fmt::Debug for SageMakerBidiTtsConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SageMakerBidiTtsConfig")
            .field("credentials", &"[redacted]")
            .finish_non_exhaustive()
    }
}
impl SageMakerBidiTtsConfig {
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
    pub fn with_settings(mut self, settings: SynthesisSettings) -> Result<Self, Error> {
        self.settings = settings;
        self.validate()?;
        Ok(self)
    }
    pub fn with_limits(mut self, limits: TtsLimits) -> Result<Self, Error> {
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
    pub fn settings(&self) -> &SynthesisSettings {
        &self.settings
    }
    pub fn limits(&self) -> &TtsLimits {
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
}
