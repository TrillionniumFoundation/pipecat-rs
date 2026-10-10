//! Rust implementation of the client template's server-side start/session proxy.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use reqwest::{
    header::{HeaderMap, HeaderValue, AUTHORIZATION, CONTENT_TYPE},
    Method, Url,
};
use serde_json::Value;
pub mod server;

use std::{fmt, net::IpAddr, sync::Arc, time::Duration};
use tokio::sync::Semaphore;

#[derive(Clone, Debug)]
pub struct ProxyLimits {
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub concurrent_requests: usize,
    /// Includes waiting for admission, connection setup, and the complete response.
    pub deadline: Duration,
}
impl Default for ProxyLimits {
    fn default() -> Self {
        Self {
            request_bytes: 1 << 20,
            response_bytes: 1 << 20,
            concurrent_requests: 16,
            deadline: Duration::from_secs(30),
        }
    }
}
#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ProxyError {
    #[error("invalid bot request")]
    InvalidRequest,
    #[error("bot capacity is full")]
    Busy,
    #[error("bot service is closed")]
    Closed,
    #[error("invalid proxy configuration")]
    Configuration,
    #[error("invalid session path")]
    Path,
    #[error("request exceeds configured byte limit")]
    RequestTooLarge,
    #[error("response exceeds configured byte limit")]
    ResponseTooLarge,
    #[error("upstream connection failed")]
    Connection,
    #[error("upstream returned HTTP {0}")]
    Status(u16),
    #[error("upstream returned an error response")]
    Upstream,
    #[error("upstream response is not JSON")]
    Json,
    #[error("proxy request deadline exceeded")]
    Deadline,
}
/// Supported original Next.js session route verbs. No arbitrary-method forwarding.
#[derive(Clone, Copy, Debug)]
pub enum SessionMethod {
    Post,
    Patch,
}

#[derive(Clone)]
pub struct ClientProxy {
    client: reqwest::Client,
    start: Url,
    sessions: Url,
    limits: ProxyLimits,
    permits: Arc<Semaphore>,
}
impl fmt::Debug for ClientProxy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientProxy")
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl ClientProxy {
    /// `bearer_token` stays server-side. This type never exports browser configuration.
    /// HTTPS is required except for literal loopback addresses and localhost.
    pub fn new(
        start_url: &str,
        bearer_token: Option<&str>,
        limits: ProxyLimits,
    ) -> Result<Self, ProxyError> {
        if limits.request_bytes == 0
            || limits.response_bytes == 0
            || limits.concurrent_requests == 0
            || limits.concurrent_requests > 65536
            || limits.deadline.is_zero()
        {
            return Err(ProxyError::Configuration);
        }
        let start = Url::parse(start_url).map_err(|_| ProxyError::Configuration)?;
        let host = start.host_str().ok_or(ProxyError::Configuration)?;
        let loopback = host.eq_ignore_ascii_case("localhost")
            || host
                .trim_matches(['[', ']'])
                .parse::<IpAddr>()
                .is_ok_and(|ip| ip.is_loopback());
        if !(start.scheme() == "https" || (start.scheme() == "http" && loopback))
            || !start.username().is_empty()
            || start.password().is_some()
            || start.query().is_some()
            || start.fragment().is_some()
            || !start.path().ends_with("/start")
        {
            return Err(ProxyError::Configuration);
        }
        let mut sessions = start.clone();
        let base = start
            .path()
            .strip_suffix("/start")
            .ok_or(ProxyError::Configuration)?;
        sessions.set_path(&format!("{base}/sessions/"));
        let mut headers = HeaderMap::new();
        headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));
        if let Some(token) = bearer_token {
            if token.is_empty() {
                return Err(ProxyError::Configuration);
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
                .map_err(|_| ProxyError::Configuration)?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        let client = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| ProxyError::Configuration)?;
        let permits = Arc::new(Semaphore::new(limits.concurrent_requests));
        Ok(Self {
            client,
            start,
            sessions,
            limits,
            permits,
        })
    }
    pub async fn start(&self, body: &Value) -> Result<Value, ProxyError> {
        self.request(Method::POST, self.start.clone(), body).await
    }
    pub async fn session(
        &self,
        method: SessionMethod,
        session_id: &str,
        path: &[&str],
        body: &Value,
    ) -> Result<Value, ProxyError> {
        let url = self.session_url(session_id, path)?;
        self.request(
            match method {
                SessionMethod::Post => Method::POST,
                SessionMethod::Patch => Method::PATCH,
            },
            url,
            body,
        )
        .await
    }
    fn session_url(&self, session_id: &str, path: &[&str]) -> Result<Url, ProxyError> {
        validate_session_path(session_id, path)?;
        let mut url = self.sessions.clone();
        url.path_segments_mut()
            .map_err(|_| ProxyError::Path)?
            .pop_if_empty()
            .push(session_id)
            .extend(path.iter().copied());
        Ok(url)
    }
    async fn request(&self, method: Method, url: Url, body: &Value) -> Result<Value, ProxyError> {
        // The bounded writer avoids allocating an unbounded serialized copy.
        let mut writer = LimitedWriter {
            bytes: Vec::new(),
            limit: self.limits.request_bytes,
        };
        serde_json::to_writer(&mut writer, body).map_err(|_| ProxyError::RequestTooLarge)?;
        tokio::time::timeout(self.limits.deadline, async {
            let _permit = self
                .permits
                .acquire()
                .await
                .map_err(|_| ProxyError::Connection)?;
            let mut response = self
                .client
                .request(method, url)
                .body(writer.bytes)
                .send()
                .await
                .map_err(|_| ProxyError::Connection)?;
            if !response.status().is_success() {
                return Err(ProxyError::Status(response.status().as_u16()));
            }
            if response
                .content_length()
                .is_some_and(|len| len > self.limits.response_bytes as u64)
            {
                return Err(ProxyError::ResponseTooLarge);
            }
            let mut bytes = Vec::new();
            while let Some(chunk) = response.chunk().await.map_err(|_| ProxyError::Connection)? {
                if chunk.len() > self.limits.response_bytes.saturating_sub(bytes.len()) {
                    return Err(ProxyError::ResponseTooLarge);
                }
                bytes.extend_from_slice(&chunk);
            }
            let value: Value = serde_json::from_slice(&bytes).map_err(|_| ProxyError::Json)?;
            // Match JavaScript's truthy `data.error` test without copying secrets into errors.
            if value.is_null() || value.get("error").is_some_and(js_truthy) {
                return Err(ProxyError::Upstream);
            }
            Ok(value)
        })
        .await
        .map_err(|_| ProxyError::Deadline)?
    }
}
pub(crate) fn validate_session_path(session_id: &str, path: &[&str]) -> Result<(), ProxyError> {
    if path.is_empty() || path.len() > 16 {
        return Err(ProxyError::Path);
    }
    for part in std::iter::once(session_id).chain(path.iter().copied()) {
        if part.is_empty()
            || part.len() > 256
            || part == "."
            || part == ".."
            || part
                .chars()
                .any(|c| c.is_control() || matches!(c, '/' | '\\' | '%' | '?' | '#'))
        {
            return Err(ProxyError::Path);
        }
    }
    Ok(())
}
fn js_truthy(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Bool(v) => *v,
        Value::Number(v) => v.as_f64() != Some(0.0),
        Value::String(v) => !v.is_empty(),
        Value::Array(_) | Value::Object(_) => true,
    }
}
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl std::io::Write for LimitedWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if buf.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("body limit"));
        }
        self.bytes.extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
