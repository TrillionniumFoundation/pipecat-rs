//! Immutable explicit AgentCore routes and finite resource limits.
use crate::Error;
use pipecat_aws_common::{auth, AwsCredentials};
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
    time::Duration,
};

/// Independent bounds, copied into a configuration by [`AgentCoreConfig::with_limits`].
///
/// Counts and bytes must be positive and have fixed implementation ceilings. Timeout
/// bounds are independent of the source-profile, non-configurable one-second quiet gap.
#[derive(Clone, Debug)]
pub struct AgentCoreLimits {
    pub max_contexts: usize,
    pub max_messages: usize,
    pub max_context_bytes: usize,
    pub max_string_bytes: usize,
    pub max_pending_invocations: usize,
    pub max_pending_bytes: usize,
    pub max_response_bytes: usize,
    pub max_line_bytes: usize,
    pub max_output_bytes: usize,
    pub max_header_bytes: usize,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub idle_timeout: Duration,
    pub output_timeout: Duration,
    pub total_timeout: Duration,
    pub shutdown_timeout: Duration,
}
impl Default for AgentCoreLimits {
    fn default() -> Self {
        Self {
            max_contexts: 64,
            max_messages: 128,
            max_context_bytes: 262_144,
            max_string_bytes: 65_536,
            max_pending_invocations: 16,
            max_pending_bytes: 1_048_576,
            max_response_bytes: 1_048_576,
            max_line_bytes: 65_536,
            max_output_bytes: 1_048_576,
            max_header_bytes: 16_384,
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            output_timeout: Duration::from_secs(5),
            total_timeout: Duration::from_secs(300),
            shutdown_timeout: Duration::from_secs(10),
        }
    }
}
impl AgentCoreLimits {
    /// Check fixed finite ceilings without imposing dependencies between bounds.
    pub fn validate(&self) -> Result<(), Error> {
        let bounded = [
            (self.max_contexts, 1024),
            (self.max_messages, 1024),
            (self.max_context_bytes, 4 * 1024 * 1024),
            (self.max_string_bytes, 1024 * 1024),
            (self.max_pending_invocations, 64),
            (self.max_pending_bytes, 16 * 1024 * 1024),
            (self.max_response_bytes, 16 * 1024 * 1024),
            (self.max_line_bytes, 1024 * 1024),
            (self.max_output_bytes, 16 * 1024 * 1024),
        ];
        if bounded.iter().any(|&(n, max)| n == 0 || n > max)
            || !(8192..=65_536).contains(&self.max_header_bytes)
            || [
                self.connect_timeout,
                self.write_timeout,
                self.idle_timeout,
                self.output_timeout,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(300))
            || self.total_timeout.is_zero()
            || self.total_timeout > Duration::from_secs(3600)
            || self.shutdown_timeout.is_zero()
            || self.shutdown_timeout > Duration::from_secs(120)
        {
            return Err(Error::Invalid("finite AgentCore limits"));
        }
        Ok(())
    }
}

/// Ordinary-partition, default-schema AgentCore runtime configuration.
///
/// Credentials are explicit. No SDK credential chain, DNS lookup, session reuse,
/// redirect, retry, proxy, endpoint qualifier, or control-plane operation is enabled.
/// Construction validates fields; [`Self::validate`] additionally requires the hosted
/// caller-resolved connection address (or a literal-loopback test origin).
#[derive(Clone)]
pub struct AgentCoreConfig {
    region: String,
    arn: String,
    credentials: AwsCredentials,
    loopback_origin: Option<String>,
    connect_address: Option<SocketAddr>,
    limits: AgentCoreLimits,
}
impl fmt::Debug for AgentCoreConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AgentCoreConfig")
            .field("route", &"[redacted]")
            .field("credentials", &"[redacted]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl AgentCoreConfig {
    pub fn new(
        region: impl Into<String>,
        arn: impl Into<String>,
        credentials: AwsCredentials,
    ) -> Result<Self, Error> {
        let mut value = Self {
            region: region.into(),
            arn: arn.into(),
            credentials,
            loopback_origin: None,
            connect_address: None,
            limits: AgentCoreLimits::default(),
        };
        value.validate_fields()?;
        // Valid short values may arrive in extremely over-allocated caller buffers.
        // Box conversion drops spare string capacity; credential cloning copies only
        // live string lengths, and assignment drops the original credential buffers.
        value.region = value.region.into_boxed_str().into_string();
        value.arn = value.arn.into_boxed_str().into_string();
        value.credentials = value.credentials.clone();
        Ok(value)
    }

    /// Use only a literal-loopback HTTP(S) origin, not a caller-selected request path.
    /// An optional single trailing slash is allowed. Hostnames, userinfo, query,
    /// fragments, encoded hosts, URL normalization tricks, and port zero are rejected.
    pub fn with_loopback_endpoint(mut self, origin: &str) -> Result<Self, Error> {
        if self.connect_address.is_some() {
            return Err(Error::Invalid("loopback route rejects connect address"));
        }
        let url = loopback_origin(origin)?;
        self.loopback_origin = Some(url.as_str().trim_end_matches('/').to_owned());
        self.validate_fields()?;
        Ok(self)
    }

    /// Connect to exactly this numeric port-443 address while retaining the regional
    /// AWS name for HTTP authority, TLS SNI and certificate verification. No DNS runs.
    pub fn with_connect_address(mut self, address: SocketAddr) -> Result<Self, Error> {
        if self.loopback_origin.is_some() {
            return Err(Error::Invalid("loopback route rejects connect address"));
        }
        validate_address(address)?;
        self.connect_address = Some(address);
        Ok(self)
    }

    pub fn with_limits(mut self, limits: AgentCoreLimits) -> Result<Self, Error> {
        limits.validate()?;
        self.limits = limits;
        Ok(self)
    }

    pub fn limits(&self) -> &AgentCoreLimits {
        &self.limits
    }
    pub fn region(&self) -> &str {
        &self.region
    }
    pub fn arn(&self) -> &str {
        &self.arn
    }
    pub(crate) fn credentials(&self) -> &AwsCredentials {
        &self.credentials
    }
    pub(crate) fn connect_address(&self) -> Option<SocketAddr> {
        self.connect_address
    }

    /// Validate the complete connection plan before admission or external effects.
    pub fn validate(&self) -> Result<(), Error> {
        self.validate_fields()?;
        match (&self.loopback_origin, self.connect_address) {
            (Some(_), Some(_)) => Err(Error::Invalid("loopback route rejects connect address")),
            (Some(_), None) => Ok(()),
            (None, Some(address)) => validate_address(address),
            (None, None) => Err(Error::Invalid(
                "regional AgentCore route requires caller-resolved connect address",
            )),
        }
    }

    fn validate_fields(&self) -> Result<(), Error> {
        auth::region(&self.region).map_err(|_| Error::Invalid("AgentCore region"))?;
        // This finite profile has no partition resolver or non-commercial endpoint.
        if [
            "cn-", "us-gov-", "us-iso-", "us-isob-", "eu-isoe-", "us-isof-", "eusc-",
        ]
        .iter()
        .any(|prefix| self.region.starts_with(prefix))
        {
            return Err(Error::Unsupported("AgentCore region partition"));
        }
        validate_arn(&self.arn, &self.region)?;
        self.limits.validate()?;
        if let Some(origin) = &self.loopback_origin {
            loopback_origin(origin)?;
        }
        Ok(())
    }

    pub(crate) fn url(&self) -> Result<url::Url, Error> {
        let origin = self
            .loopback_origin
            .clone()
            .unwrap_or_else(|| format!("https://bedrock-agentcore.{}.amazonaws.com", self.region));
        let encoded = encode_arn(&self.arn);
        url::Url::parse(&format!("{origin}/runtimes/{encoded}/invocations"))
            .map_err(|_| Error::Invalid("AgentCore route"))
    }

    /// The wire ARN is one encoded non-greedy URI label. SigV4 encodes its percent
    /// signs a second time; only the surrounding route slashes remain separators.
    pub(crate) fn canonical_uri(&self) -> Result<String, Error> {
        Ok(self.url()?.path().replace('%', "%25"))
    }
}

fn validate_arn(arn: &str, region: &str) -> Result<(), Error> {
    let invalid = || Error::Invalid("AgentCore runtime ARN");
    if arn.len() > 256 {
        return Err(invalid());
    }
    let mut parts = arn.split(':');
    if parts.next() != Some("arn")
        || parts.next() != Some("aws")
        || parts.next() != Some("bedrock-agentcore")
        || parts.next() != Some(region)
    {
        return Err(invalid());
    }
    let account = parts.next().ok_or_else(invalid)?;
    if account.len() != 12 || !account.bytes().all(|b| b.is_ascii_digit()) {
        return Err(invalid());
    }
    let resource = parts.next().ok_or_else(invalid)?;
    if parts.next().is_some() {
        return Err(invalid());
    }
    let runtime = resource.strip_prefix("runtime/").ok_or_else(invalid)?;
    let (name, suffix) = runtime.rsplit_once('-').ok_or_else(invalid)?;
    if name.is_empty()
        || name.len() > 48
        || !name.as_bytes()[0].is_ascii_alphabetic()
        || !name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
        || suffix.len() != 10
        || !suffix.bytes().all(|b| b.is_ascii_alphanumeric())
    {
        return Err(invalid());
    }
    Ok(())
}

fn validate_address(address: SocketAddr) -> Result<(), Error> {
    if address.port() != 443
        || address.ip().is_unspecified()
        || address.ip().is_multicast()
        || matches!(address.ip(), IpAddr::V4(ip) if ip.is_broadcast())
        || matches!(address, SocketAddr::V6(ip) if ip.scope_id() != 0 || ip.flowinfo() != 0)
    {
        return Err(Error::Invalid("AgentCore caller-resolved connect address"));
    }
    Ok(())
}

fn loopback_origin(origin: &str) -> Result<url::Url, Error> {
    let invalid = || Error::Invalid("AgentCore literal-loopback HTTP(S) origin");
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

fn encode_arn(arn: &str) -> String {
    const HEX: &[u8; 16] = b"0123456789ABCDEF";
    let mut result = String::with_capacity(arn.len() * 3);
    for b in arn.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            result.push(char::from(b));
        } else {
            result.push('%');
            result.push(char::from(HEX[(b >> 4) as usize]));
            result.push(char::from(HEX[(b & 15) as usize]));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    const ARN: &str = "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/Agent_1-Ab01234567";
    fn config() -> AgentCoreConfig {
        AgentCoreConfig::new(
            "us-east-1",
            ARN,
            AwsCredentials::new(
                "SYNTHETICACCESS",
                "syntheticsecret",
                Some("synthetictoken".into()),
            )
            .unwrap(),
        )
        .unwrap()
    }

    #[test]
    fn full_connection_plan_is_required_but_builders_can_supply_it() {
        assert!(config().validate().is_err());
        assert!(config()
            .with_connect_address("192.0.2.1:443".parse().unwrap())
            .unwrap()
            .validate()
            .is_ok());
        for origin in [
            "http://127.0.0.1:12345",
            "http://[::1]:12345/",
            "http://127.0.0.1:80",
        ] {
            assert!(config()
                .with_loopback_endpoint(origin)
                .unwrap()
                .validate()
                .is_ok());
        }
    }

    #[test]
    fn route_encodes_complete_arn_and_canonicalizes_again() {
        let c = config();
        assert_eq!(c.url().unwrap().as_str(), "https://bedrock-agentcore.us-east-1.amazonaws.com/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aus-east-1%3A123456789012%3Aruntime%2FAgent_1-Ab01234567/invocations");
        assert_eq!(c.canonical_uri().unwrap(), "/runtimes/arn%253Aaws%253Abedrock-agentcore%253Aus-east-1%253A123456789012%253Aruntime%252FAgent_1-Ab01234567/invocations");
        let c = c.with_loopback_endpoint("http://127.0.0.1:12345").unwrap();
        assert_eq!(c.url().unwrap().host_str(), Some("127.0.0.1"));
        assert!(c.url().unwrap().path().ends_with("/invocations"));
    }

    #[test]
    fn arn_is_full_match_and_matches_region() {
        for arn in [
            ARN.replace("arn:aws:", "arn:aws-cn:"),
            ARN.replace("us-east-1", "us-west-2"),
            ARN.replace("123456789012", "12345678901"),
            ARN.replace("runtime/", "runtime-endpoint/"),
            ARN.replace("Agent_1", "1Agent"),
            ARN.replace("Agent_1", "Agent-1"),
            ARN.replace("Ab01234567", "Ab0123456"),
            format!("{ARN}:1"),
            format!("{ARN}/endpoint"),
            format!("{ARN}?qualifier=prod"),
            format!("{ARN}#fragment"),
            format!(" {ARN}"),
            format!("{ARN}\n"),
        ] {
            assert!(AgentCoreConfig::new("us-east-1", arn, config().credentials.clone()).is_err());
        }
        let max_name = format!("A{}", "_".repeat(47));
        assert!(AgentCoreConfig::new(
            "us-east-1",
            ARN.replace("Agent_1", &max_name),
            config().credentials.clone()
        )
        .is_ok());
        assert!(AgentCoreConfig::new(
            "us-east-1",
            ARN.replace("Agent_1", &(max_name + "A")),
            config().credentials.clone()
        )
        .is_err());
    }

    #[test]
    fn loopback_does_not_accept_normalized_or_unrelated_routes() {
        assert!(config()
            .with_loopback_endpoint("https://127.0.0.1:12345")
            .is_ok());
        for origin in [
            "http://localhost:12345",
            "http://192.0.2.1:12345",
            "http://127.0.0.1:0",
            "http://user@127.0.0.1:12345",
            "http://127.0.0.1:12345/x",
            "http://127.0.0.1:12345/a/..",
            "http://127.0.0.1:12345/%2e%2e/",
            "http://127.0.0.1:12345//",
            "http://127.0.0.1:12345/?x",
            "http://127.0.0.1:12345/#x",
            "http://127.1:12345",
            "http://2130706433:12345",
            "http://0x7f000001:12345",
            "http://127.0.0.1:12345\n",
            "http://[::ffff:127.0.0.1]:12345",
        ] {
            assert!(config().with_loopback_endpoint(origin).is_err(), "{origin}");
        }
        let address = "192.0.2.1:443".parse().unwrap();
        assert!(config()
            .with_connect_address(address)
            .unwrap()
            .with_loopback_endpoint("http://127.0.0.1:12345")
            .is_err());
        assert!(config()
            .with_loopback_endpoint("http://127.0.0.1:12345")
            .unwrap()
            .with_connect_address(address)
            .is_err());
    }

    #[test]
    fn numeric_route_and_limit_ceilings_are_checked() {
        for address in [
            "192.0.2.1:80",
            "0.0.0.0:443",
            "224.0.0.1:443",
            "255.255.255.255:443",
            "[::]:443",
            "[ff02::1]:443",
            "[fe80::1%1]:443",
        ] {
            assert!(config()
                .with_connect_address(address.parse().unwrap())
                .is_err());
        }
        for limits in [
            AgentCoreLimits {
                max_contexts: 0,
                ..Default::default()
            },
            AgentCoreLimits {
                max_response_bytes: usize::MAX,
                ..Default::default()
            },
            AgentCoreLimits {
                max_header_bytes: 8191,
                ..Default::default()
            },
            AgentCoreLimits {
                max_header_bytes: 65537,
                ..Default::default()
            },
            AgentCoreLimits {
                total_timeout: Duration::ZERO,
                ..Default::default()
            },
            AgentCoreLimits {
                shutdown_timeout: Duration::MAX,
                ..Default::default()
            },
        ] {
            assert!(config().with_limits(limits).is_err());
        }
    }

    #[test]
    fn config_normalizes_caller_owned_capacity() {
        let mut region = String::with_capacity(1_000_000);
        region.push_str("us-east-1");
        let mut arn = String::with_capacity(1_000_000);
        arn.push_str(ARN);
        let mut access = String::with_capacity(1_000_000);
        access.push_str("SYNTHETICACCESS");
        let mut secret = String::with_capacity(1_000_000);
        secret.push_str("syntheticsecret");
        let credentials = AwsCredentials::new(access, secret, None).unwrap();
        let value = AgentCoreConfig::new(region, arn, credentials).unwrap();
        assert_eq!(value.region.capacity(), value.region.len());
        assert_eq!(value.arn.capacity(), value.arn.len());
        assert_eq!(
            format!("{:?}", value.credentials),
            "AwsCredentials([redacted])"
        );
        let cloned = value.clone();
        assert_eq!(cloned.region.capacity(), cloned.region.len());
        assert_eq!(cloned.arn.capacity(), cloned.arn.len());
        assert_eq!(cloned.credentials, value.credentials);
    }

    #[test]
    fn debug_and_errors_do_not_expose_route_or_credentials() {
        let rendered = format!("{:?}", config());
        for secret in [
            ARN,
            "123456789012",
            "Agent_1",
            "SYNTHETICACCESS",
            "syntheticsecret",
            "synthetictoken",
        ] {
            assert!(!rendered.contains(secret));
        }
        let err = config()
            .with_loopback_endpoint("http://sensitive.invalid/")
            .unwrap_err();
        assert!(!format!("{err:?}").contains("sensitive.invalid"));
    }
}
