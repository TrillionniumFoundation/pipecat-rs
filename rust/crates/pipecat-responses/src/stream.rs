// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{CancellationToken, ResponseEvent, ResponsesRequest, ServiceError};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION};
use std::{collections::BTreeMap, fmt, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Clone)]
pub struct ResponsesConfig {
    pub http_url: String,
    pub websocket_url: String,
    pub api_key: Option<String>,
    pub organization: Option<String>,
    pub project: Option<String>,
    pub headers: BTreeMap<String, String>,
    pub connect_timeout: Duration,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
    pub drain_timeout: Duration,
    pub max_connection_age: Duration,
    pub max_event_bytes: usize,
    pub max_response_bytes: usize,
    pub max_request_bytes: usize,
    pub channel_capacity: usize,
    /// Retry one explicitly rejected create request, never an ambiguous one.
    pub retry_rejected_create: bool,
}
impl fmt::Debug for ResponsesConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ResponsesConfig")
            .field("http_url", &"[configured]")
            .field("websocket_url", &"[configured]")
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("headers", &"[redacted]")
            .field("request_timeout", &self.request_timeout)
            .finish_non_exhaustive()
    }
}
impl Default for ResponsesConfig {
    fn default() -> Self {
        Self {
            http_url: "https://api.openai.com/v1/responses".into(),
            websocket_url: "wss://api.openai.com/v1/responses".into(),
            api_key: None,
            organization: None,
            project: None,
            headers: BTreeMap::new(),
            connect_timeout: Duration::from_secs(10),
            request_timeout: Duration::from_secs(120),
            idle_timeout: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(5),
            max_connection_age: Duration::from_secs(55 * 60),
            max_event_bytes: 1024 * 1024,
            max_response_bytes: 16 * 1024 * 1024,
            max_request_bytes: 16 * 1024 * 1024,
            channel_capacity: 64,
            retry_rejected_create: true,
        }
    }
}
impl ResponsesConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: Some(api_key.into()),
            ..Self::default()
        }
    }
    pub(crate) fn validate(&self) -> Result<(), ServiceError> {
        for duration in [
            self.connect_timeout,
            self.request_timeout,
            self.idle_timeout,
            self.drain_timeout,
            self.max_connection_age,
        ] {
            if duration.is_zero() || std::time::Instant::now().checked_add(duration).is_none() {
                return Err(invalid(
                    "timeouts must be positive and fit a monotonic deadline",
                ));
            }
        }
        if self.max_event_bytes == 0
            || self.max_response_bytes == 0
            || self.max_request_bytes == 0
            || self.channel_capacity == 0
            || self.channel_capacity > tokio::sync::Semaphore::MAX_PERMITS
        {
            return Err(invalid(
                "size limits must be positive and channel capacity must fit Tokio bounds",
            ));
        }
        validate_url(&self.http_url, false)?;
        validate_url(&self.websocket_url, true)?;
        self.auth_headers()?;
        Ok(())
    }
    pub(crate) fn auth_headers(&self) -> Result<HeaderMap, ServiceError> {
        let mut headers = HeaderMap::new();
        for (name, value) in &self.headers {
            let name = HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| invalid("invalid custom header name"))?;
            if [
                "authorization",
                "host",
                "connection",
                "upgrade",
                "content-length",
                "content-type",
                "accept",
                "sec-websocket-key",
                "sec-websocket-version",
                "sec-websocket-protocol",
            ]
            .contains(&name.as_str())
            {
                return Err(invalid("custom header overrides reserved protocol header"));
            }
            let mut value =
                HeaderValue::from_str(value).map_err(|_| invalid("invalid custom header value"))?;
            value.set_sensitive(true);
            headers.insert(name, value);
        }
        for (name, value) in [
            ("openai-organization", &self.organization),
            ("openai-project", &self.project),
        ] {
            if let Some(value) = value {
                let mut value =
                    HeaderValue::from_str(value).map_err(|_| invalid("invalid account header"))?;
                value.set_sensitive(true);
                headers.insert(HeaderName::from_static(name), value);
            }
        }
        if let Some(key) = &self.api_key {
            if key.is_empty() {
                return Err(invalid("API key cannot be empty"));
            }
            let mut value = HeaderValue::from_str(&format!("Bearer {key}"))
                .map_err(|_| invalid("invalid authorization header"))?;
            value.set_sensitive(true);
            headers.insert(AUTHORIZATION, value);
        }
        Ok(headers)
    }
    pub(crate) fn request_body(
        &self,
        request: &ResponsesRequest,
        stream: bool,
    ) -> Result<serde_json::Value, ServiceError> {
        let body = request.body(stream)?;
        if body.to_string().len() > self.max_request_bytes {
            return Err(invalid("request exceeds configured size limit"));
        }
        Ok(body)
    }
}
fn invalid(message: &str) -> ServiceError {
    ServiceError::InvalidRequest(message.into())
}
fn validate_url(value: &str, ws: bool) -> Result<(), ServiceError> {
    let url = url::Url::parse(value).map_err(|_| invalid("invalid Responses endpoint"))?;
    let loopback = url.host_str().is_some_and(|h| {
        h == "localhost"
            || h.trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    let secure = if ws { "wss" } else { "https" };
    let insecure = if ws { "ws" } else { "http" };
    if url.scheme() != secure && !(url.scheme() == insecure && loopback) {
        return Err(invalid(
            "Responses endpoint requires TLS except loopback fixtures",
        ));
    }
    if url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.query().is_some()
    {
        return Err(invalid(
            "Responses endpoint cannot contain credentials, query or fragment",
        ));
    }
    Ok(())
}
pub struct ResponsesStream {
    pub(crate) rx: mpsc::Receiver<Result<ResponseEvent, ServiceError>>,
    pub(crate) cancel: CancellationToken,
    pub(crate) task: Option<JoinHandle<()>>,
}
impl ResponsesStream {
    pub async fn recv(&mut self) -> Option<Result<ResponseEvent, ServiceError>> {
        self.rx.recv().await
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
    /// Cancel, release backpressure and join all owned work, including WS drain.
    pub async fn close(mut self) {
        self.cancel.cancel();
        self.rx.close();
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
    }
}
impl Drop for ResponsesStream {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.rx.close();
    }
}
pub(crate) async fn emit(
    tx: &mpsc::Sender<Result<ResponseEvent, ServiceError>>,
    event: Result<ResponseEvent, ServiceError>,
    timeout: Duration,
) -> Result<(), ServiceError> {
    tokio::time::timeout(timeout, tx.send(event))
        .await
        .map_err(|_| ServiceError::Timeout)?
        .map_err(|_| ServiceError::ConsumerClosed)
}
pub(crate) fn transport_error() -> ServiceError {
    ServiceError::Transport("Responses transport failure (details redacted)".into())
}
