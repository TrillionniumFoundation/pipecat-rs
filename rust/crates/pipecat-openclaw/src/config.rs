use std::{fmt, time::Duration};
use url::{Host, Url};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GatewayError {
    #[error("invalid OpenClaw configuration: {0}")]
    Invalid(&'static str),
    #[error("OpenClaw capacity exceeded: {0}")]
    Capacity(&'static str),
    #[error("OpenClaw request rejected: {0}")]
    Rejected(&'static str),
    #[error("OpenClaw protocol error: {0}")]
    Protocol(&'static str),
    #[error("OpenClaw request not submitted: {0}")]
    BeforeWrite(&'static str),
    #[error("OpenClaw remote outcome unknown after submission: {0}; do not automatically retry")]
    OutcomeUnknown(&'static str),
    #[error("OpenClaw connection closed")]
    Closed,
    #[error("OpenClaw timeout: {0}")]
    Timeout(&'static str),
}
/// Finite connection and per-run bounds. No request is retried.
#[derive(Debug, Clone)]
pub struct GatewayLimits {
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub request_timeout: Duration,
    pub run_timeout: Duration,
    pub close_timeout: Duration,
    pub output_timeout: Duration,
    pub max_message_bytes: usize,
    pub max_text_bytes: usize,
    pub max_pending: usize,
    pub max_runs: usize,
    pub max_aliases: usize,
    pub max_unrouted_frames: usize,
    pub max_unrouted_bytes: usize,
    pub max_output_events: usize,
    pub max_output_bytes: usize,
}
impl Default for GatewayLimits {
    fn default() -> Self {
        Self {
            connect_timeout: Duration::from_secs(15),
            write_timeout: Duration::from_secs(5),
            request_timeout: Duration::from_secs(30),
            run_timeout: Duration::from_secs(300),
            close_timeout: Duration::from_secs(1),
            output_timeout: Duration::from_secs(5),
            max_message_bytes: 1024 * 1024,
            max_text_bytes: 1024 * 1024,
            max_pending: 32,
            max_runs: 16,
            max_aliases: 8,
            max_unrouted_frames: 100,
            max_unrouted_bytes: 1024 * 1024,
            max_output_events: 64,
            max_output_bytes: 1024 * 1024,
        }
    }
}
/// WSS is required unless explicit literal-loopback cleartext is enabled.
/// URL userinfo, query, and fragment are rejected to avoid credential leakage.
#[derive(Clone)]
pub struct GatewayConfig {
    pub url: String,
    pub token: Option<String>,
    pub password: Option<String>,
    pub session_key: String,
    pub role: String,
    pub scopes: Vec<String>,
    pub allow_loopback_cleartext: bool,
    pub limits: GatewayLimits,
}
impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            url: "ws://127.0.0.1:18789".into(),
            token: None,
            password: None,
            session_key: "agent:main:main".into(),
            role: "operator".into(),
            scopes: vec!["operator.write".into()],
            allow_loopback_cleartext: false,
            limits: GatewayLimits::default(),
        }
    }
}
impl fmt::Debug for GatewayConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayConfig")
            .field("url", &"[redacted]")
            .field("token", &self.token.as_ref().map(|_| "[redacted]"))
            .field("password", &self.password.as_ref().map(|_| "[redacted]"))
            .field("session_key", &"[redacted]")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl GatewayConfig {
    pub fn validate(&self) -> Result<(), GatewayError> {
        let u = Url::parse(&self.url).map_err(|_| GatewayError::Invalid("URL"))?;
        let loopback = matches!(u.host(), Some(Host::Ipv4(a)) if a.is_loopback())
            || matches!(u.host(), Some(Host::Ipv6(a)) if a.is_loopback());
        if self.url.len() > 4096
            || u.host().is_none()
            || !u.username().is_empty()
            || u.password().is_some()
            || u.query().is_some()
            || u.fragment().is_some()
            || !(u.scheme() == "wss"
                || (u.scheme() == "ws" && loopback && self.allow_loopback_cleartext))
        {
            return Err(GatewayError::Invalid(
                "WSS or explicit literal-loopback WS required",
            ));
        }
        bounded(&self.session_key, 1024, "session key")?;
        bounded(&self.role, 128, "role")?;
        if self.scopes.is_empty() || self.scopes.len() > 16 {
            return Err(GatewayError::Invalid("scopes"));
        }
        for scope in &self.scopes {
            bounded(scope, 128, "scope")?;
        }
        for s in [self.token.as_ref(), self.password.as_ref()]
            .into_iter()
            .flatten()
        {
            bounded(s, 8192, "credential")?;
        }
        let l = &self.limits;
        for t in [
            l.connect_timeout,
            l.write_timeout,
            l.request_timeout,
            l.run_timeout,
            l.close_timeout,
            l.output_timeout,
        ] {
            if t.is_zero() || t > Duration::from_secs(86400) {
                return Err(GatewayError::Invalid("deadline"));
            }
        }
        for n in [
            l.max_pending,
            l.max_runs,
            l.max_aliases,
            l.max_unrouted_frames,
            l.max_output_events,
        ] {
            if n == 0 || n > 4096 {
                return Err(GatewayError::Invalid("count bound"));
            }
        }
        for n in [
            l.max_message_bytes,
            l.max_text_bytes,
            l.max_unrouted_bytes,
            l.max_output_bytes,
        ] {
            if n == 0 || n > 32 * 1024 * 1024 {
                return Err(GatewayError::Invalid("byte bound"));
            }
        }
        Ok(())
    }
}
pub(crate) fn bounded(s: &str, max: usize, field: &'static str) -> Result<(), GatewayError> {
    if s.is_empty() || s.len() > max || s.contains('\0') {
        Err(GatewayError::Invalid(field))
    } else {
        Ok(())
    }
}
