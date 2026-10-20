//! Explicit configuration and bounded transport admission. No discovery,
//! redirects, refresh, retry, or ambient credential fallback is performed here.
use std::{fmt, time::Duration};

use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    handshake::client::Request,
    http::{HeaderMap, HeaderValue},
};
use url::Url;

use crate::types::{SessionProperties, API_VERSION};

/// Errors intentionally contain no arbitrary endpoint, credential, server
/// message, handshake body, parser input, or underlying transport error.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum VoiceLiveError {
    #[error("{primary}; cleanup: {cleanup}")]
    Cleanup {
        primary: Box<VoiceLiveError>,
        cleanup: Box<VoiceLiveError>,
    },
    #[error("invalid Voice Live configuration or action: {0}")]
    Invalid(&'static str),
    #[error("Voice Live handshake returned HTTP {0}")]
    Http(u16),
    #[error("Voice Live transport failed")]
    Transport,
    #[error("invalid Voice Live provider message: {0}")]
    Protocol(&'static str),
    #[error("Voice Live {0} timed out")]
    Timeout(&'static str),
    #[error("Voice Live queue is full")]
    Backpressure,
    #[error("Voice Live session is not ready")]
    NotReady,
    #[error("Voice Live session is closed")]
    Closed,
    #[error("Voice Live action was interrupted")]
    Interrupted,
    #[error("Voice Live message or state exceeds configured limit")]
    TooLarge,
    #[error("unknown, stale, or already completed Voice Live tool call")]
    UnknownToolCall,
    #[error("Voice Live connection ended with unsettled native tools")]
    UnsettledTools,
}

#[derive(Debug, Clone)]
pub struct VoiceLiveLimits {
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_audio_chunk_bytes: usize,
    pub max_outstanding_tools: usize,
    pub max_session_responses: usize,
    pub max_session_tool_calls: usize,
    pub max_session_items: usize,
    pub max_history_messages: usize,
    pub max_unknown_events: usize,
    pub max_preroll_ms: u32,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub response_timeout: Duration,
    pub close_timeout: Duration,
}
impl Default for VoiceLiveLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_message_bytes: 1024 * 1024,
            max_audio_chunk_bytes: 192_000,
            max_outstanding_tools: 64,
            max_session_responses: 4096,
            max_session_tool_calls: 4096,
            max_session_items: 4096,
            max_history_messages: 4096,
            max_unknown_events: 64,
            max_preroll_ms: 2_000,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(5),
            response_timeout: Duration::from_secs(120),
            close_timeout: Duration::from_secs(2),
        }
    }
}
impl VoiceLiveLimits {
    pub fn validate(&self) -> Result<(), VoiceLiveError> {
        // The base64 calculation also demonstrates no overflow can reach the
        // actor's PCM/write buffers. Leave space for envelope and bounded IDs.
        let encoded_audio = self
            .max_audio_chunk_bytes
            .checked_add(2)
            .and_then(|n| n.checked_div(3))
            .and_then(|n| n.checked_mul(4))
            .and_then(|n| n.checked_add(1024));
        if !(1..=4096).contains(&self.queue_capacity)
            || !(2048..=16 * 1024 * 1024).contains(&self.max_message_bytes)
            || self.max_audio_chunk_bytes == 0
            || !self.max_audio_chunk_bytes.is_multiple_of(2)
            || encoded_audio.is_none_or(|n| n > self.max_message_bytes)
            || !(1..=128).contains(&self.max_outstanding_tools)
            || !(1..=65_536).contains(&self.max_session_responses)
            || !(self.max_outstanding_tools..=65_536).contains(&self.max_session_tool_calls)
            || !(1..=65_536).contains(&self.max_session_items)
            || !(1..=65_536).contains(&self.max_history_messages)
            || !(1..=65_536).contains(&self.max_unknown_events)
            || !(500..=5_000).contains(&self.max_preroll_ms)
            || [
                self.connect_timeout,
                self.operation_timeout,
                self.response_timeout,
                self.close_timeout,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(86_400))
        {
            return Err(VoiceLiveError::Invalid("limits outside supported bounds"));
        }
        // Aggregate queue allocation is also bounded, not just each message.
        if self
            .queue_capacity
            .checked_mul(self.max_message_bytes)
            .is_none_or(|n| n > 256 * 1024 * 1024)
        {
            return Err(VoiceLiveError::Invalid("aggregate queue budget"));
        }
        Ok(())
    }
}

/// Already supplied credentials only. The bearer token's lifetime belongs to
/// the caller; this crate never runs a token callback or refreshes credentials.
#[derive(Clone)]
pub enum VoiceLiveCredentials {
    ApiKey(String),
    EntraBearer(String),
}
impl fmt::Debug for VoiceLiveCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ApiKey(_) => f.write_str("ApiKey([REDACTED])"),
            Self::EntraBearer(_) => f.write_str("EntraBearer([REDACTED])"),
        }
    }
}
impl VoiceLiveCredentials {
    fn authentication(&self) -> Result<(&'static str, HeaderValue), VoiceLiveError> {
        let secret = match self {
            Self::ApiKey(secret) | Self::EntraBearer(secret) => secret,
        };
        if secret.trim().is_empty()
            || secret.len() > 16_384
            || secret.trim() != secret
            || secret.chars().any(char::is_control)
        {
            return Err(VoiceLiveError::Invalid(
                "explicit authentication credential",
            ));
        }
        let (name, value) = match self {
            Self::ApiKey(secret) => ("api-key", HeaderValue::from_str(secret)),
            Self::EntraBearer(secret) => (
                "authorization",
                HeaderValue::from_str(&format!("Bearer {secret}")),
            ),
        };
        let mut value = value.map_err(|_| VoiceLiveError::Invalid("authentication header"))?;
        value.set_sensitive(true);
        Ok((name, value))
    }
}

/// The input endpoint is query-free: either a canonical Voice Live WebSocket
/// URL, or a HTTPS resource root normalized to `/voice-live/realtime`.
/// Plaintext is supported only for the exact literals 127.0.0.1 and [::1].
#[derive(Clone)]
pub struct VoiceLiveConfig {
    pub endpoint: String,
    pub model: String,
    pub credentials: VoiceLiveCredentials,
    pub session: SessionProperties,
    pub limits: VoiceLiveLimits,
}
impl fmt::Debug for VoiceLiveConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VoiceLiveConfig")
            .field("endpoint", &"[configured]")
            .field("model", &"[configured]")
            .field("credentials", &self.credentials)
            .field("session", &"[configured]")
            .field("limits", &self.limits)
            .finish()
    }
}
impl VoiceLiveConfig {
    pub fn new(
        endpoint: impl Into<String>,
        model: impl Into<String>,
        credentials: VoiceLiveCredentials,
        voice: impl Into<String>,
    ) -> Self {
        Self {
            endpoint: endpoint.into(),
            model: model.into(),
            credentials,
            session: SessionProperties::new(voice),
            limits: VoiceLiveLimits::default(),
        }
    }
    pub fn validate(&self) -> Result<(), VoiceLiveError> {
        self.limits.validate()?;
        normalize_endpoint(&self.endpoint)?;
        if self.model.trim().is_empty()
            || self.model.len() > 256
            || self.model.trim() != self.model
            || self.model.chars().any(char::is_control)
        {
            return Err(VoiceLiveError::Invalid("explicit connection model"));
        }
        self.credentials.authentication()?;
        self.session.validate()?;
        let session_bytes = serde_json::to_vec(&self.session)
            .map_err(|_| VoiceLiveError::Invalid("session serialization"))?
            .len();
        if session_bytes
            .checked_add(1024)
            .is_none_or(|n| n > self.limits.max_message_bytes)
        {
            return Err(VoiceLiveError::TooLarge);
        }
        Ok(())
    }
    pub fn request_url(&self) -> Result<Url, VoiceLiveError> {
        self.validate()?;
        let mut url = normalize_endpoint(&self.endpoint)?;
        url.query_pairs_mut()
            .append_pair("api-version", API_VERSION)
            .append_pair("model", &self.model);
        Ok(url)
    }
    /// Returns exactly one sensitive authentication header, without GA/Beta
    /// headers. Socket handshake headers are added by `socket_request`.
    pub fn request_headers(&self) -> Result<HeaderMap, VoiceLiveError> {
        self.validate()?;
        let (name, value) = self.credentials.authentication()?;
        let mut headers = HeaderMap::new();
        headers.insert(name, value);
        Ok(headers)
    }
    pub fn socket_request(&self) -> Result<Request, VoiceLiveError> {
        let url = self.request_url()?;
        let mut request = url
            .as_str()
            .into_client_request()
            .map_err(|_| VoiceLiveError::Invalid("WebSocket request"))?;
        request.headers_mut().extend(self.request_headers()?);
        Ok(request)
    }
}

fn normalize_endpoint(value: &str) -> Result<Url, VoiceLiveError> {
    const INVALID: VoiceLiveError =
        VoiceLiveError::Invalid("query-free canonical Voice Live endpoint");
    if value.len() > 2048
        || value.chars().any(|c| c.is_control() || c.is_whitespace())
        || value.contains(['?', '#', '\\'])
    {
        return Err(INVALID);
    }
    // Validate raw components as well as parsed components: URL parsers
    // normalize dot paths, abbreviated IPv4, numeric hosts, and credentials.
    let (scheme, rest) = value.split_once("://").ok_or(INVALID.clone())?;
    let (authority, raw_path) = rest
        .split_once('/')
        .map_or((rest, ""), |(host, path)| (host, path));
    if authority.is_empty() || authority.contains('@') {
        return Err(INVALID);
    }
    let mut url = Url::parse(value).map_err(|_| INVALID.clone())?;
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.port() == Some(0)
    {
        return Err(INVALID);
    }
    match scheme {
        "https" if raw_path.is_empty() && url.path() == "/" => {
            url.set_scheme("wss").map_err(|_| INVALID.clone())?;
            url.set_path("/voice-live/realtime");
        }
        "wss" | "ws"
            if raw_path == "voice-live/realtime" && url.path() == "/voice-live/realtime" =>
        {
            if scheme == "ws" {
                let literal = if authority.starts_with('[') {
                    authority
                        .split_once(']')
                        .map(|(host, _)| format!("{host}]"))
                } else {
                    Some(authority.split(':').next().unwrap_or_default().to_owned())
                };
                if !matches!(literal.as_deref(), Some("127.0.0.1" | "[::1]")) {
                    return Err(VoiceLiveError::Invalid(
                        "plaintext requires a literal loopback endpoint",
                    ));
                }
            }
        }
        _ => return Err(INVALID),
    }
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{AzureSemanticVad, TurnDetection};

    fn config() -> VoiceLiveConfig {
        VoiceLiveConfig::new(
            "https://synthetic-resource.invalid",
            "synthetic-model",
            VoiceLiveCredentials::ApiKey("invented-key".into()),
            "en-US-SyntheticNeural",
        )
    }

    #[test]
    fn https_resource_normalizes_to_exact_dated_voice_live_path() {
        let config = config();
        let url = config.request_url().unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.path(), "/voice-live/realtime");
        assert_eq!(
            url.query_pairs().collect::<Vec<_>>(),
            [
                ("api-version".into(), "2026-07-15".into()),
                ("model".into(), "synthetic-model".into()),
            ]
        );
    }

    #[test]
    fn reserved_model_characters_are_one_encoded_query_value() {
        let mut config = config();
        config.model = "model&api-version=evil/#? +中文".into();
        let url = config.request_url().unwrap();
        let pairs = url.query_pairs().collect::<Vec<_>>();
        assert_eq!(pairs.len(), 2);
        assert_eq!(pairs[1].0, "model");
        assert_eq!(pairs[1].1, config.model);
        assert_eq!(url.fragment(), None);
    }

    #[test]
    fn only_literal_loopback_allows_plaintext() {
        for endpoint in [
            "ws://127.0.0.1:1234/voice-live/realtime",
            "ws://[::1]:1234/voice-live/realtime",
            "wss://synthetic-resource.invalid/voice-live/realtime",
            "https://synthetic-resource.invalid/",
        ] {
            let mut config = config();
            config.endpoint = endpoint.into();
            assert!(config.validate().is_ok(), "{endpoint}");
        }
        for endpoint in [
            "ws://localhost:1234/voice-live/realtime",
            "ws://synthetic-resource.invalid/voice-live/realtime",
            "ws://127.1:1234/voice-live/realtime",
            "ws://127.0.0.2:1234/voice-live/realtime",
            "ws://2130706433:1234/voice-live/realtime",
            "ws://0x7f000001:1234/voice-live/realtime",
            "ws://[::ffff:127.0.0.1]:1234/voice-live/realtime",
        ] {
            let mut config = config();
            config.endpoint = endpoint.into();
            assert!(config.validate().is_err(), "{endpoint}");
        }
    }

    #[test]
    fn endpoints_reject_queries_credentials_fragments_wrong_paths_and_normalization() {
        for endpoint in [
            "",
            "not a url",
            "http://synthetic-resource.invalid",
            "https://synthetic-resource.invalid/voice-live/realtime",
            "wss://synthetic-resource.invalid",
            "wss://synthetic-resource.invalid/voice-live/realtime/",
            "wss://synthetic-resource.invalid/openai/v1/realtime",
            "wss://synthetic-resource.invalid/openai/realtime",
            "wss://synthetic-resource.invalid/voice-live/realtime?model=x",
            "wss://synthetic-resource.invalid/voice-live/realtime?",
            "wss://synthetic-resource.invalid/voice-live/realtime#",
            "wss://user:invented-secret@synthetic-resource.invalid/voice-live/realtime",
            "wss://@synthetic-resource.invalid/voice-live/realtime",
            "wss://synthetic-resource.invalid/a/../voice-live/realtime",
            "wss://synthetic-resource.invalid/%76oice-live/realtime",
            "wss://synthetic-resource.invalid\\voice-live/realtime",
            " wss://synthetic-resource.invalid/voice-live/realtime",
            "wss://synthetic-resource.invalid/voice-live/real\ntime",
            "ws://127.0.0.1:0/voice-live/realtime",
        ] {
            let mut config = config();
            config.endpoint = endpoint.into();
            assert!(config.request_url().is_err(), "{endpoint}");
        }
    }

    #[test]
    fn auth_selects_exactly_one_sensitive_header() {
        for bearer in [false, true] {
            let mut config = config();
            config.credentials = if bearer {
                VoiceLiveCredentials::EntraBearer("invented-bearer".into())
            } else {
                VoiceLiveCredentials::ApiKey("invented-key".into())
            };
            let headers = config.request_headers().unwrap();
            assert_eq!(headers.len(), 1);
            let (name, expected) = if bearer {
                ("authorization", "Bearer invented-bearer")
            } else {
                ("api-key", "invented-key")
            };
            assert_eq!(headers[name], expected);
            assert!(headers[name].is_sensitive());
            assert!(!headers.contains_key("openai-beta"));
            assert!(!format!("{headers:?}").contains("invented-"));
            let request = config.socket_request().unwrap();
            assert_eq!(request.uri().path(), "/voice-live/realtime");
            assert_eq!(request.headers()[name], expected);
            assert_eq!(request.headers().contains_key("api-key"), !bearer);
            assert_eq!(request.headers().contains_key("authorization"), bearer);
        }
    }

    #[test]
    fn malformed_credentials_are_rejected_without_secret_echo() {
        for bearer in [false, true] {
            for secret in [
                "".to_owned(),
                " ".into(),
                " invented-secret".into(),
                "invented-secret\nheader".into(),
                "invented-secret\rheader".into(),
                "invented-secret\theader".into(),
                "invented-secret\u{7f}".into(),
                "x".repeat(16_385),
            ] {
                let mut config = config();
                config.credentials = if bearer {
                    VoiceLiveCredentials::EntraBearer(secret)
                } else {
                    VoiceLiveCredentials::ApiKey(secret)
                };
                let error = config.validate().unwrap_err();
                assert!(!format!("{error:?} {error} {config:?}").contains("invented-secret"));
            }
        }
    }

    #[test]
    fn config_debug_hides_endpoint_model_and_session_content_too() {
        let mut config = config();
        config.endpoint = "invalid-invented-secret-endpoint".into();
        config.model = "invented-secret-model".into();
        config.session.instructions = Some("invented-secret-instructions".into());
        assert!(!format!("{config:?}").contains("invented-secret"));
        assert!(!config
            .validate()
            .unwrap_err()
            .to_string()
            .contains("invented-secret"));
    }

    #[test]
    fn blank_control_and_oversized_models_fail() {
        for model in [
            "".to_owned(),
            " ".into(),
            " model".into(),
            "model\0secret".into(),
            "x".repeat(257),
        ] {
            let mut config = config();
            config.model = model;
            assert!(config.validate().is_err());
        }
    }

    #[test]
    fn limit_overflow_zero_and_unbounded_timeout_fail() {
        let invalid: Vec<VoiceLiveLimits> = vec![
            VoiceLiveLimits {
                queue_capacity: 0,
                ..Default::default()
            },
            VoiceLiveLimits {
                queue_capacity: usize::MAX,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_message_bytes: usize::MAX,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_audio_chunk_bytes: usize::MAX,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_audio_chunk_bytes: 3,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_audio_chunk_bytes: 900_000,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_session_items: 0,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_history_messages: 0,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_session_tool_calls: 1,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_unknown_events: 0,
                ..Default::default()
            },
            VoiceLiveLimits {
                max_preroll_ms: u32::MAX,
                ..Default::default()
            },
            VoiceLiveLimits {
                connect_timeout: Duration::ZERO,
                ..Default::default()
            },
            VoiceLiveLimits {
                response_timeout: Duration::MAX,
                ..Default::default()
            },
            VoiceLiveLimits {
                queue_capacity: 4096,
                ..Default::default()
            },
        ];
        assert!(VoiceLiveLimits::default().validate().is_ok());
        for limits in invalid {
            assert!(limits.validate().is_err(), "{limits:?}");
        }
    }

    #[test]
    fn session_update_envelope_is_inside_message_budget() {
        let mut config = config();
        config.limits.max_audio_chunk_bytes = 2;
        config.limits.max_message_bytes = 2048;
        config.session.instructions = Some("x".repeat(1800));
        assert_eq!(config.validate(), Err(VoiceLiveError::TooLarge));
    }

    #[test]
    fn semantic_ownership_cannot_be_relaxed() {
        let mut config = config();
        let mut vad = AzureSemanticVad::default();
        vad.auto_truncate = true;
        config.session.turn_detection = TurnDetection::AzureSemanticVad(vad);
        assert!(config.validate().is_err());
    }
}
