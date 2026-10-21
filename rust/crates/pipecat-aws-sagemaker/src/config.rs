use crate::Error;
use pipecat_aws_common::{auth, AwsCredentials};
use serde::{Deserialize, Serialize};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

/// Bounds apply before retention; wire, queued PCM and total session bytes differ.
#[derive(Clone, Debug)]
pub struct SageMakerLimits {
    pub max_utterances: usize,
    pub max_text_bytes: usize,
    pub max_request_bytes: usize,
    pub max_pending_invocations: usize,
    pub max_pending_bytes: usize,
    pub max_response_bytes: usize,
    pub max_output_bytes: usize,
    pub max_frame_bytes: usize,
    pub max_header_bytes: usize,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
    pub output_timeout: Duration,
    pub total_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for SageMakerLimits {
    fn default() -> Self {
        Self {
            max_utterances: 64,
            max_text_bytes: 65_536,
            max_request_bytes: 262_144,
            max_pending_invocations: 8,
            max_pending_bytes: 1_048_576,
            max_response_bytes: 8 * 1024 * 1024,
            max_output_bytes: 16 * 1024 * 1024,
            max_frame_bytes: 24_000,
            max_header_bytes: 16_384,
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            output_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(120),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}
impl SageMakerLimits {
    pub fn validate(&self) -> Result<(), Error> {
        if [
            (self.max_utterances, 1024),
            (self.max_text_bytes, 1_048_576),
            (self.max_request_bytes, 6_291_456),
            (self.max_pending_invocations, 64),
            (self.max_pending_bytes, 16 * 1024 * 1024),
            (self.max_response_bytes, 64 * 1024 * 1024),
            (self.max_output_bytes, 64 * 1024 * 1024),
            (self.max_frame_bytes, 65_536),
        ]
        .iter()
        .any(|&(n, max)| n == 0 || n > max)
            || !self.max_frame_bytes.is_multiple_of(2)
            || !(8192..=65_536).contains(&self.max_header_bytes)
            || [
                self.connect_timeout,
                self.write_timeout,
                self.idle_timeout,
                self.output_timeout,
            ]
            .iter()
            .any(|t| t.is_zero() || *t > Duration::from_secs(300))
            || self.total_timeout.is_zero()
            || self.total_timeout > Duration::from_secs(3600)
            || self.shutdown_timeout.is_zero()
            || self.shutdown_timeout > Duration::from_secs(120)
        {
            return Err(Error::Invalid("finite limits"));
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
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
    pub fn validate(&self) -> Result<(), Error> {
        if self.voice_name.is_empty()
            || self.voice_name.len() > 256
            || !self
                .voice_name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
            || self.language_code.is_empty()
            || self.language_code.len() > 63
            || !self
                .language_code
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
            || !self.language_code.as_bytes()[0].is_ascii_alphabetic()
            || self.language_code.ends_with('-')
            || !(8000..=96000).contains(&self.sample_rate_hz)
        {
            return Err(Error::Invalid("voice/language/sample rate"));
        }
        Ok(())
    }
}
/// One complete utterance, not incremental tokens. The finite app generates local
/// context IDs and never claims assistant history or physical playback.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Utterance {
    pub text: String,
}
impl Utterance {
    pub fn new(text: impl Into<String>) -> Self {
        Self { text: text.into() }
    }
}

#[derive(Clone)]
pub struct SageMakerConfig {
    region: String,
    endpoint_name: String,
    credentials: AwsCredentials,
    loopback_origin: Option<String>,
    connect_address: Option<SocketAddr>,
    limits: SageMakerLimits,
    settings: SynthesisSettings,
}
impl fmt::Debug for SageMakerConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SageMakerConfig")
            .field("route", &"[redacted]")
            .field("credentials", &"[redacted]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl SageMakerConfig {
    pub fn new(
        region: impl Into<String>,
        endpoint_name: impl Into<String>,
        credentials: AwsCredentials,
    ) -> Result<Self, Error> {
        let mut c = Self {
            region: region.into(),
            endpoint_name: endpoint_name.into(),
            credentials,
            loopback_origin: None,
            connect_address: None,
            limits: Default::default(),
            settings: Default::default(),
        };
        c.validate_fields()?;
        c.region = c.region.into_boxed_str().into_string();
        c.endpoint_name = c.endpoint_name.into_boxed_str().into_string();
        c.credentials = c.credentials.clone();
        Ok(c)
    }
    pub fn with_loopback_endpoint(mut self, origin: &str) -> Result<Self, Error> {
        if self.connect_address.is_some() {
            return Err(Error::Invalid("loopback rejects connect address"));
        }
        let url = loopback_origin(origin)?;
        self.loopback_origin = Some(url.as_str().trim_end_matches('/').to_owned());
        Ok(self)
    }
    pub fn with_connect_address(mut self, address: SocketAddr) -> Result<Self, Error> {
        if self.loopback_origin.is_some() {
            return Err(Error::Invalid("loopback rejects connect address"));
        }
        validate_address(address)?;
        self.connect_address = Some(address);
        Ok(self)
    }
    pub fn with_limits(mut self, limits: SageMakerLimits) -> Result<Self, Error> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }
    pub fn with_settings(mut self, settings: SynthesisSettings) -> Result<Self, Error> {
        settings.validate()?;
        self.settings = settings.clone();
        Ok(self)
    }
    pub fn limits(&self) -> &SageMakerLimits {
        &self.limits
    }
    pub fn settings(&self) -> &SynthesisSettings {
        &self.settings
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn endpoint_name(&self) -> &str {
        &self.endpoint_name
    }
    pub(crate) fn credentials(&self) -> &AwsCredentials {
        &self.credentials
    }
    pub(crate) fn connect_address(&self) -> Option<SocketAddr> {
        self.connect_address
    }
    pub fn validate(&self) -> Result<(), Error> {
        self.validate_fields()?;
        match (&self.loopback_origin, self.connect_address) {
            (Some(_), None) => Ok(()),
            (None, Some(a)) => validate_address(a),
            _ => Err(Error::Invalid(
                "regional route requires caller-resolved address",
            )),
        }
    }
    fn validate_fields(&self) -> Result<(), Error> {
        auth::region(&self.region).map_err(|_| Error::Invalid("region"))?;
        if [
            "cn-", "us-gov-", "us-iso-", "us-isob-", "eu-isoe-", "us-isof-", "eusc-",
        ]
        .iter()
        .any(|p| self.region.starts_with(p))
        {
            return Err(Error::Unsupported("region partition"));
        }
        let n = self.endpoint_name.as_bytes();
        if n.is_empty()
            || n.len() > 63
            || !n[0].is_ascii_alphanumeric()
            || !n[n.len() - 1].is_ascii_alphanumeric()
            || !n.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'-')
        {
            return Err(Error::Invalid("endpoint name"));
        }
        self.limits.validate()?;
        self.settings.validate()?;
        if let Some(o) = &self.loopback_origin {
            loopback_origin(o)?;
        }
        Ok(())
    }
    pub(crate) fn url(&self) -> Result<url::Url, Error> {
        let origin = self
            .loopback_origin
            .clone()
            .unwrap_or_else(|| format!("https://runtime.sagemaker.{}.amazonaws.com", self.region));
        url::Url::parse(&format!(
            "{origin}/endpoints/{}/invocations",
            self.endpoint_name
        ))
        .map_err(|_| Error::Invalid("route"))
    }
}
fn validate_address(address: SocketAddr) -> Result<(), Error> {
    if address.port() != 443
        || address.ip().is_unspecified()
        || address.ip().is_multicast()
        || matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast())
        || matches!(address, SocketAddr::V6(ip) if ip.scope_id() != 0 || ip.flowinfo() != 0)
    {
        return Err(Error::Invalid("SageMaker caller-resolved connect address"));
    }
    Ok(())
}

fn loopback_origin(origin: &str) -> Result<url::Url, Error> {
    let invalid = || Error::Invalid("SageMaker literal-loopback HTTP(S) origin");
    if origin.len() > 128 {
        return Err(invalid());
    }
    let (authority, default_port) = if let Some(rest) = origin.strip_prefix("http://") {
        (rest, 80)
    } else if let Some(rest) = origin.strip_prefix("https://") {
        (rest, 443)
    } else {
        return Err(invalid());
    };
    let authority = authority.strip_suffix('/').unwrap_or(authority);
    // Parse the caller's original authority, rather than URL-normalized host text.
    let address = authority.parse::<SocketAddr>().or_else(|_| {
        authority
            .trim_start_matches('[')
            .trim_end_matches(']')
            .parse::<IpAddr>()
            .map(|ip| SocketAddr::new(ip, default_port))
    });
    let address = address.map_err(|_| invalid())?;
    if !address.ip().is_loopback()
        || address.port() == 0
        || matches!(address, SocketAddr::V6(ip) if ip.scope_id() != 0 || ip.flowinfo() != 0)
    {
        return Err(invalid());
    }
    let url = url::Url::parse(origin).map_err(|_| invalid())?;
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.port_or_known_default() != Some(address.port())
        || url
            .host_str()
            .and_then(|h| h.trim_matches(['[', ']']).parse::<IpAddr>().ok())
            != Some(address.ip())
    {
        return Err(invalid());
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn c(name: &str) -> Result<SageMakerConfig, Error> {
        SageMakerConfig::new(
            "us-east-1",
            name,
            AwsCredentials::new("FAKE", "fake-secret", None).unwrap(),
        )
    }
    #[test]
    fn endpoint_name_is_anchored_and_aws_service_route_is_exact() {
        for n in ["a", "0", "a--b", "Endpoint-01"] {
            assert!(c(n).is_ok());
        }
        assert!(c(&"a".repeat(63)).is_ok());
        assert!(c(&"a".repeat(64)).is_err());
        for n in [
            "", "-a", "a-", "a_b", "a/b", "a%2Fb", "a?x", "a#x", "x@y", "a.b", "é", "a\n",
        ] {
            assert!(c(n).is_err(), "{n}");
        }
        assert_eq!(
            c("magpie--01").unwrap().url().unwrap().as_str(),
            "https://runtime.sagemaker.us-east-1.amazonaws.com/endpoints/magpie--01/invocations"
        );
    }
    #[test]
    fn literal_origin_and_hosted_address_separation() {
        for o in [
            "http://127.0.0.1:1234",
            "http://[::1]:1234/",
            "https://127.0.0.1:443",
        ] {
            assert!(c("x")
                .unwrap()
                .with_loopback_endpoint(o)
                .unwrap()
                .validate()
                .is_ok());
        }
        for o in [
            "http://localhost:1234",
            "http://127.1:1234",
            "http://127.0.0.1:0",
            "http://127.0.0.1:1/x",
            "http://127.0.0.1:1?x",
            "http://a@127.0.0.1:1",
            "http://127.0.0.1:1#x",
            "http://8.8.8.8:1",
        ] {
            assert!(c("x").unwrap().with_loopback_endpoint(o).is_err(), "{o}");
        }
        assert!(c("x").unwrap().validate().is_err());
        let hosted = c("x")
            .unwrap()
            .with_connect_address("192.0.2.1:443".parse().unwrap())
            .unwrap();
        hosted.validate().unwrap();
        assert_eq!(
            hosted.url().unwrap().host_str(),
            Some("runtime.sagemaker.us-east-1.amazonaws.com")
        );
        assert!(hosted.with_loopback_endpoint("http://127.0.0.1:1").is_err());
        assert!(c("x")
            .unwrap()
            .with_connect_address("192.0.2.1:80".parse().unwrap())
            .is_err());
    }
    #[test]
    fn settings_bounds_and_debug_redaction() {
        let s = SynthesisSettings {
            voice_name: "x\r\n".into(),
            ..Default::default()
        };
        assert!(s.validate().is_err());
        let s = SynthesisSettings {
            sample_rate_hz: 0,
            ..Default::default()
        };
        assert!(s.validate().is_err());
        let debug = format!("{:?}", c("secret-endpoint").unwrap());
        assert!(!debug.contains("secret-endpoint"));
        assert!(!debug.contains("fake-secret"));
        let l = SageMakerLimits {
            max_frame_bytes: 3,
            ..Default::default()
        };
        assert!(l.validate().is_err());
    }
}

#[cfg(test)]
mod signing_vectors {
    use super::*;
    #[test]
    fn independent_fixed_sigv4_vectors_pin_regional_ipv4_ipv6_service_and_body() {
        // Oracle: independent Python stdlib hashlib/hmac, not this signer.
        let body=br#"{"text":"Hello","voice_name":"Magpie-Multilingual.EN-US.Aria","language_code":"en-US","sample_rate_hz":24000}"#;
        let credentials =
            AwsCredentials::new("FAKEACCESSKEY", "fake-secret", Some("fake-token".into())).unwrap();
        for (host, signature) in [
            (
                "https://runtime.sagemaker.us-west-2.amazonaws.com",
                "55856a6aa09db53885ee6fffaf655e97c9e1ff3bc0046d0dd0dd6fcea47caad1",
            ),
            (
                "http://127.0.0.1:18999",
                "7120a4ce932b8bdc02ffc49cc4d66c1b0c1ff1026ff3553eeb1a39822eca7d5e",
            ),
            (
                "http://[::1]:18999",
                "0afe0ecb93269fb8cba369075b7e1b718f67a0f4f1108d89d7e903d99708ecdf",
            ),
        ] {
            let url = url::Url::parse(&format!("{host}/endpoints/magpie--01/invocations")).unwrap();
            let headers = auth::signed_body_post_headers(
                &url,
                "us-west-2",
                &credentials,
                body,
                "20261020T230000Z",
                "sagemaker",
                url.path(),
                &[("accept", "application/octet-stream")],
            )
            .unwrap();
            assert_eq!(headers["authorization"],format!("AWS4-HMAC-SHA256 Credential=FAKEACCESSKEY/20261020/us-west-2/sagemaker/aws4_request, SignedHeaders=accept;content-type;host;x-amz-date;x-amz-security-token, Signature={signature}"));
        }
    }
}
