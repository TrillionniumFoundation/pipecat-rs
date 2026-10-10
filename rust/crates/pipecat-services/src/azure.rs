//! Explicit Azure OpenAI Chat Completions routing and authentication.
//!
//! Applications own Entra credential acquisition and refresh. This module never
//! inspects environment credentials, identity SDKs, or instance metadata.

use crate::{FinishReason, ServiceError};
use reqwest::header::{HeaderName, HeaderValue, AUTHORIZATION};
use std::{fmt, future::Future, pin::Pin, sync::Arc};

pub const AZURE_DATED_API_VERSION: &str = "2025-04-01-preview";

type TokenFuture = Pin<Box<dyn Future<Output = Result<String, ServiceError>> + Send>>;

/// An application-supplied asynchronous Entra token callback. Each actual HTTP
/// attempt obtains a fresh value, including an explicitly enabled header retry.
#[derive(Clone)]
pub struct AzureTokenProvider(Arc<dyn Fn() -> TokenFuture + Send + Sync>);
impl AzureTokenProvider {
    pub fn new<F, Fut>(provider: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, ServiceError>> + Send + 'static,
    {
        Self(Arc::new(move || Box::pin(provider())))
    }
}
impl fmt::Debug for AzureTokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("AzureTokenProvider([redacted])")
    }
}

#[derive(Clone)]
pub enum AzureAuth {
    ApiKey(String),
    EntraTokenProvider(AzureTokenProvider),
}
impl fmt::Debug for AzureAuth {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ApiKey(_) => "ApiKey([redacted])",
            Self::EntraTokenProvider(_) => "EntraTokenProvider([redacted])",
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AzureApiSurface {
    V1,
    Dated,
}

/// A resource-root endpoint selects dated deployment routing. An endpoint
/// ending in `/openai/v1` selects the v1 surface. HTTPS is required except for
/// loopback test servers. Credentials, queries, fragments and other base paths
/// are rejected rather than interpreted as routing instructions.
#[derive(Clone)]
pub struct AzureConfig {
    endpoint: reqwest::Url,
    surface: AzureApiSurface,
    auth: AzureAuth,
    api_version: String,
}
impl fmt::Debug for AzureConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("AzureConfig")
            .field("endpoint", &"[configured]")
            .field("surface", &self.surface)
            .field("auth", &self.auth)
            .field("api_version", &"[configured]")
            .finish()
    }
}
impl AzureConfig {
    pub fn new(endpoint: impl AsRef<str>, auth: AzureAuth) -> Result<Self, ServiceError> {
        let raw = endpoint.as_ref();
        if raw.is_empty()
            || raw.chars().any(|c| c.is_whitespace() || c.is_control())
            || raw.contains(['\\', '?', '#', '@', '%'])
            || !(raw.starts_with("https://") || raw.starts_with("http://"))
        {
            return Err(invalid_endpoint());
        }
        let mut endpoint = reqwest::Url::parse(raw).map_err(|_| invalid_endpoint())?;
        let loopback = endpoint.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if endpoint.host_str().is_none()
            || (endpoint.scheme() != "https" && !(endpoint.scheme() == "http" && loopback))
            || !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
        {
            return Err(invalid_endpoint());
        }
        // Inspect the raw path too: URL parsing must not silently normalize
        // dot segments or missing authority separators into an accepted route.
        let authority_and_path = raw.split_once("://").ok_or_else(invalid_endpoint)?.1;
        if authority_and_path.starts_with('/') {
            return Err(invalid_endpoint());
        }
        let path = authority_and_path
            .find('/')
            .map_or("", |i| &authority_and_path[i..]);
        let surface = match path.trim_end_matches('/') {
            "" => AzureApiSurface::Dated,
            "/openai/v1" => AzureApiSurface::V1,
            _ => return Err(invalid_endpoint()),
        };
        endpoint.set_path(match surface {
            AzureApiSurface::V1 => "/openai/v1",
            AzureApiSurface::Dated => "/",
        });
        if let AzureAuth::ApiKey(key) = &auth {
            credential_header(key, surface == AzureApiSurface::V1)?;
        }
        Ok(Self {
            endpoint,
            surface,
            auth,
            api_version: AZURE_DATED_API_VERSION.into(),
        })
    }

    /// Dated API version override. It has no effect on a v1 endpoint.
    pub fn with_api_version(mut self, version: impl Into<String>) -> Result<Self, ServiceError> {
        let version = version.into();
        if version.is_empty()
            || !version
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-')
        {
            return Err(ServiceError::InvalidRequest(
                "invalid Azure API version".into(),
            ));
        }
        self.api_version = version;
        Ok(self)
    }
    pub fn surface(&self) -> AzureApiSurface {
        self.surface
    }

    pub(crate) fn base_endpoint(&self) -> &str {
        self.endpoint.as_str()
    }

    pub(crate) fn completion_endpoint(
        &self,
        deployment: &str,
    ) -> Result<reqwest::Url, ServiceError> {
        if deployment.trim().is_empty()
            || matches!(deployment, "." | "..")
            || deployment.chars().any(char::is_control)
        {
            return Err(ServiceError::InvalidRequest(
                "invalid Azure deployment".into(),
            ));
        }
        let mut url = self.endpoint.clone();
        {
            let mut path = url.path_segments_mut().map_err(|_| invalid_endpoint())?;
            path.pop_if_empty();
            if self.surface == AzureApiSurface::Dated {
                path.extend(["openai", "deployments", deployment]);
            }
            path.extend(["chat", "completions"]);
        }
        if self.surface == AzureApiSurface::Dated {
            url.query_pairs_mut()
                .append_pair("api-version", &self.api_version);
        }
        Ok(url)
    }

    pub(crate) async fn authorization(&self) -> Result<(HeaderName, HeaderValue), ServiceError> {
        let (value, bearer) = match &self.auth {
            AzureAuth::ApiKey(key) => (key.clone(), self.surface == AzureApiSurface::V1),
            AzureAuth::EntraTokenProvider(provider) => {
                let token = (provider.0)().await.map_err(|_| ServiceError::Provider {
                    kind: "azure_auth".into(),
                    message: "Azure token provider failed".into(),
                })?;
                (token, true)
            }
        };
        let header = if bearer {
            AUTHORIZATION
        } else {
            HeaderName::from_static("api-key")
        };
        Ok((header, credential_header(&value, bearer)?))
    }
}
fn invalid_endpoint() -> ServiceError {
    ServiceError::InvalidRequest("invalid Azure endpoint; use HTTPS resource root or /openai/v1 (HTTP loopback only for tests)".into())
}
fn credential_header(value: &str, bearer: bool) -> Result<HeaderValue, ServiceError> {
    if value.is_empty() || value.chars().any(|c| c.is_whitespace() || c.is_control()) {
        return Err(ServiceError::InvalidRequest(
            "invalid Azure credential".into(),
        ));
    }
    let encoded = if bearer {
        format!("Bearer {value}")
    } else {
        value.to_owned()
    };
    let mut header = HeaderValue::from_str(&encoded)
        .map_err(|_| ServiceError::InvalidRequest("invalid Azure credential".into()))?;
    header.set_sensitive(true);
    Ok(header)
}

/// Error categories survive; untrusted provider text, request contents, token
/// callback errors and transport details must not enter diagnostics.
pub(crate) fn redact_error(error: ServiceError) -> ServiceError {
    match error {
        ServiceError::InvalidRequest(_) => {
            ServiceError::InvalidRequest("invalid Azure request".into())
        }
        ServiceError::Unsupported(_) => {
            ServiceError::Unsupported("unsupported Azure request feature".into())
        }
        ServiceError::Http { status, .. } => ServiceError::Http {
            status,
            message: "Azure HTTP request failed".into(),
            retry_after: None,
        },
        ServiceError::Provider { .. } => ServiceError::Provider {
            kind: "azure".into(),
            message: "Azure provider request failed".into(),
        },
        ServiceError::Transport(_) => ServiceError::Transport("Azure transport failed".into()),
        ServiceError::Protocol(_) => ServiceError::Protocol("invalid Azure stream".into()),
        other => other,
    }
}
pub(crate) fn finish_reason(reason: Option<FinishReason>) -> Option<FinishReason> {
    reason.map(|reason| match reason {
        FinishReason::Other(_) => FinishReason::Other("azure_other".into()),
        reason => reason,
    })
}
