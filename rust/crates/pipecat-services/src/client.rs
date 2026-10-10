use crate::adapters::request_body;
use crate::protocol::ProtocolDecoder;
use crate::sse::SseDecoder;
use crate::{CancellationToken, CompletionRequest, Provider, ServiceError, StreamEvent};
use futures_util::StreamExt;
use reqwest::header::{HeaderValue, ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use std::fmt;
use std::time::Duration;
use tokio::sync::mpsc;

/// Retry only before response headers, never after streaming output begins.
#[derive(Clone, Debug, Default)]
pub enum RetryPolicy {
    #[default]
    Never,
    /// Opt-in may duplicate provider work. Both attempts share one total timeout.
    OnceOnHeaderTimeout { timeout: Duration },
}

#[derive(Clone)]
pub struct ClientConfig {
    pub provider: Provider,
    /// Exact POST URL. For Gemini, {model} is replaced with a validated model id.
    pub endpoint: String,
    pub api_key: Option<String>,
    pub request_timeout: Duration,
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
    pub max_event_bytes: usize,
    pub channel_capacity: usize,
    pub retry: RetryPolicy,
    pub openai_preset: Option<crate::presets::OpenAiPresetConfig>,
    /// Dedicated Azure routing and credentials; configure with `ClientConfig::azure`.
    pub azure: Option<crate::azure::AzureConfig>,
}
impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig")
            .field("provider", &self.provider)
            .field("endpoint", &"[configured]")
            .field("api_key", &self.api_key.as_ref().map(|_| "[redacted]"))
            .field("request_timeout", &self.request_timeout)
            .field("idle_timeout", &self.idle_timeout)
            .finish_non_exhaustive()
    }
}
impl ClientConfig {
    pub fn new(provider: Provider, api_key: impl Into<String>) -> Self {
        let endpoint = match provider {
            Provider::OpenAi => "https://api.openai.com/v1/chat/completions",
            Provider::Anthropic => "https://api.anthropic.com/v1/messages",
            Provider::Gemini => "https://generativelanguage.googleapis.com/v1beta/models/{model}:streamGenerateContent?alt=sse",
        };
        Self {
            provider,
            endpoint: endpoint.into(),
            api_key: Some(api_key.into()),
            request_timeout: Duration::from_secs(120),
            idle_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(10),
            max_event_bytes: 4 * 1024 * 1024,
            channel_capacity: 64,
            retry: RetryPolicy::Never,
            openai_preset: None,
            azure: None,
        }
    }
    /// Explicit Azure Chat configuration. Generic endpoint/key/preset fields must
    /// not be overridden; timeouts, retry policy and limits remain configurable.
    pub fn azure(azure: crate::azure::AzureConfig) -> Self {
        let mut config = Self::new(Provider::OpenAi, "");
        config.endpoint = azure.base_endpoint().into();
        config.api_key = None;
        config.azure = Some(azure);
        config
    }
    /// Local Ollama uses the source's public dummy token, not a secret key.
    pub fn ollama() -> Result<Self, ServiceError> {
        Self::openai_compatible(
            crate::presets::OpenAiPresetConfig::new(crate::presets::OpenAiPreset::Ollama),
            "ollama",
        )
    }
    /// Build a source-compatible preset; callers can override the endpoint for
    /// loopback protocol tests without changing request behavior.
    pub fn openai_compatible(
        preset: crate::presets::OpenAiPresetConfig,
        api_key: impl Into<String>,
    ) -> Result<Self, ServiceError> {
        let mut config = Self::new(Provider::OpenAi, api_key);
        config.endpoint = preset.endpoint()?.into();
        config.openai_preset = Some(preset);
        Ok(config)
    }
}

#[derive(Clone, Debug)]
pub struct LlmClient {
    config: ClientConfig,
    http: reqwest::Client,
}
impl LlmClient {
    pub fn new(config: ClientConfig) -> Result<Self, ServiceError> {
        let mut builder = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none());
        if config.azure.is_some() {
            builder = builder.no_proxy().retry(reqwest::retry::never());
        }
        let http = builder.build().map_err(|error| {
            let error = transport_error(error);
            if config.azure.is_some() {
                crate::azure::redact_error(error)
            } else {
                error
            }
        })?;
        Self::validated(config, http)
    }
    /// Caller-supplied HTTP clients own their proxy/TLS/redirect configuration.
    pub fn with_http_client(
        config: ClientConfig,
        http: reqwest::Client,
    ) -> Result<Self, ServiceError> {
        if config.azure.is_some() {
            return Err(ServiceError::InvalidRequest(
                "Azure does not accept a custom HTTP client".into(),
            ));
        }
        Self::validated(config, http)
    }
    fn validated(config: ClientConfig, http: reqwest::Client) -> Result<Self, ServiceError> {
        if let Some(azure) = &config.azure {
            if config.provider != Provider::OpenAi
                || config.api_key.is_some()
                || config.openai_preset.is_some()
                || config.endpoint != azure.base_endpoint()
            {
                return Err(ServiceError::InvalidRequest(
                    "Azure configuration conflicts with generic client settings".into(),
                ));
            }
        }
        if config.channel_capacity == 0
            || config.max_event_bytes == 0
            || config.request_timeout.is_zero()
            || config.idle_timeout.is_zero()
            || config.connect_timeout.is_zero()
            || matches!(config.retry, RetryPolicy::OnceOnHeaderTimeout { timeout } if timeout.is_zero())
        {
            return Err(ServiceError::InvalidRequest(
                "timeouts, event limit and channel capacity must be positive".into(),
            ));
        }
        if config.openai_preset.is_some() && config.provider != Provider::OpenAi {
            return Err(ServiceError::InvalidRequest(
                "OpenAI-compatible preset requires OpenAI wire provider".into(),
            ));
        }
        Ok(Self { config, http })
    }
    pub fn provider(&self) -> Provider {
        self.config.provider
    }
    /// Pure validation and provider serialization, without sending a request.
    pub fn request_body(
        &self,
        request: &CompletionRequest,
    ) -> Result<serde_json::Value, ServiceError> {
        let result = self.request_body_inner(request);
        if self.config.azure.is_some() {
            result.map_err(crate::azure::redact_error)
        } else {
            result
        }
    }
    fn request_body_inner(
        &self,
        request: &CompletionRequest,
    ) -> Result<serde_json::Value, ServiceError> {
        // Model settings also determine template endpoints, so validation must
        // reject a bad model before a processor commits a settings update.
        let resolved = crate::settings::resolve(request)?;
        self.endpoint(&resolved.model)?;
        match &self.config.openai_preset {
            Some(preset) => preset.request_body(request),
            None => request_body(self.config.provider, request),
        }
    }
    /// Starts a cancellable background request. Reading this stream applies
    /// bounded backpressure; dropping it cancels the HTTP request.
    pub fn stream(&self, request: CompletionRequest) -> Result<LlmStream, ServiceError> {
        self.stream_with_cancel(request, CancellationToken::new())
    }
    pub fn stream_with_cancel(
        &self,
        request: CompletionRequest,
        cancel: CancellationToken,
    ) -> Result<LlmStream, ServiceError> {
        let runtime = tokio::runtime::Handle::try_current().map_err(|_| {
            ServiceError::InvalidRequest("stream requires an active Tokio runtime".into())
        })?;
        let body = self.request_body(&request)?;
        let resolved = crate::settings::resolve(&request)?;
        let endpoint = self.endpoint(&resolved.model)?;
        let mut builder = self
            .http
            .post(endpoint)
            .header(ACCEPT, "text/event-stream")
            .json(&body);
        if let Some(key) = &self.config.api_key {
            let (header, key) = match self.config.provider {
                Provider::OpenAi => (AUTHORIZATION, format!("Bearer {key}")),
                Provider::Anthropic => (
                    reqwest::header::HeaderName::from_static("x-api-key"),
                    key.clone(),
                ),
                Provider::Gemini => (
                    reqwest::header::HeaderName::from_static("x-goog-api-key"),
                    key.clone(),
                ),
            };
            let mut value = HeaderValue::from_str(&key)
                .map_err(|_| ServiceError::InvalidRequest("invalid API key header".into()))?;
            value.set_sensitive(true);
            builder = builder.header(header, value);
        }
        if self.config.provider == Provider::Anthropic {
            builder = builder.header("anthropic-version", "2023-06-01");
        }
        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let worker_cancel = cancel.clone();
        let config = self.config.clone();
        runtime.spawn(async move {
            if tx.send(Ok(StreamEvent::Started)).await.is_err() {
                return;
            }
            let mut decoder = ProtocolDecoder::new(config.provider);
            let result = tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => Err(ServiceError::Cancelled),
                result = tokio::time::timeout(config.request_timeout, run(builder,&config,&mut decoder,&tx,&worker_cancel)) => match result { Ok(result) => result, Err(_) => Err(ServiceError::Timeout) },
                _ = tx.closed() => Err(ServiceError::ConsumerClosed),
            };
            if let Some(mut usage) = decoder.usage().cloned() {
                if let Some(preset) = &config.openai_preset { preset.normalize_usage(&mut usage); }
                if tx
                    .send(Ok(StreamEvent::Usage(usage)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if let Err(error) = result {
                let error = if config.azure.is_some() { crate::azure::redact_error(error) } else { error };
                if tx.send(Err(error)).await.is_err() {
                    return;
                }
            }
            let _ = tx
                .send(Ok(StreamEvent::Finished {
                    reason: if config.azure.is_some() {
                        crate::azure::finish_reason(decoder.finish_reason().cloned())
                    } else { decoder.finish_reason().cloned() },
                }))
                .await;
        });
        Ok(LlmStream { rx, cancel })
    }
    fn endpoint(&self, model: &str) -> Result<reqwest::Url, ServiceError> {
        if let Some(azure) = &self.config.azure {
            return azure.completion_endpoint(model);
        }
        let endpoint = if self.config.endpoint.contains("{model}") {
            let model = model.strip_prefix("models/").unwrap_or(model);
            if model.is_empty()
                || !model
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
            {
                return Err(ServiceError::InvalidRequest(
                    "invalid model id in endpoint".into(),
                ));
            }
            self.config.endpoint.replace("{model}", model)
        } else {
            self.config.endpoint.clone()
        };
        let url = reqwest::Url::parse(&endpoint)
            .map_err(|_| ServiceError::InvalidRequest("invalid endpoint URL".into()))?;
        let loopback = url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        });
        if url.scheme() != "https" && !(url.scheme() == "http" && loopback) {
            return Err(ServiceError::InvalidRequest(
                "endpoint requires HTTPS; HTTP is permitted only for loopback simulation".into(),
            ));
        }
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err(ServiceError::InvalidRequest(
                "endpoint cannot contain credentials or fragments".into(),
            ));
        }
        Ok(url)
    }
}

pub struct LlmStream {
    rx: mpsc::Receiver<Result<StreamEvent, ServiceError>>,
    cancel: CancellationToken,
}
impl LlmStream {
    pub async fn next(&mut self) -> Option<Result<StreamEvent, ServiceError>> {
        self.rx.recv().await
    }
    pub fn cancel(&self) {
        self.cancel.cancel();
    }
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
}
impl Drop for LlmStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

async fn run(
    builder: reqwest::RequestBuilder,
    config: &ClientConfig,
    decoder: &mut ProtocolDecoder,
    tx: &mpsc::Sender<Result<StreamEvent, ServiceError>>,
    cancel: &CancellationToken,
) -> Result<(), ServiceError> {
    let response = match config.retry {
        RetryPolicy::Never => send_attempt(builder, config, cancel).await?,
        RetryPolicy::OnceOnHeaderTimeout { timeout } => {
            let retry = builder.try_clone().ok_or_else(|| {
                ServiceError::InvalidRequest("request body cannot be retried".into())
            })?;
            // Token acquisition belongs to the total deadline, not the header
            // timeout. A slow credential callback must not create a retry.
            let builder = authorize(builder, config, cancel).await?;
            match tokio::time::timeout(timeout, builder.send()).await {
                Ok(Ok(response)) => response,
                Ok(Err(error)) if !error.is_timeout() => return Err(transport_error(error)),
                // The exact serialized body and routing are reused. Azure Entra
                // credentials are reacquired for each actual network attempt.
                _ => send_attempt(retry, config, cancel).await?,
            }
        }
    };
    if !response.status().is_success() {
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .map(str::to_string);
        if config.azure.is_some() {
            return Err(ServiceError::Http {
                status,
                message: "Azure HTTP request failed".into(),
                retry_after: None,
            });
        }
        let mut stream = response.bytes_stream();
        let mut body = Vec::new();
        while body.len() < 8192 {
            match tokio::time::timeout(config.idle_timeout, stream.next()).await {
                Ok(Some(Ok(bytes))) => {
                    body.extend_from_slice(&bytes[..bytes.len().min(8192 - body.len())])
                }
                _ => break,
            }
        }
        let parsed = serde_json::from_slice::<serde_json::Value>(&body).ok();
        let message = parsed
            .as_ref()
            .and_then(|v| v.pointer("/error/message").and_then(|v| v.as_str()))
            .map(str::to_string)
            .unwrap_or_else(|| String::from_utf8_lossy(&body).into_owned());
        return Err(ServiceError::Http {
            status,
            message,
            retry_after,
        });
    }
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    if !content_type
        .split(';')
        .next()
        .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"))
    {
        return Err(ServiceError::Protocol(
            "successful response is not text/event-stream".into(),
        ));
    }
    let mut stream = response.bytes_stream();
    let mut sse = SseDecoder::new(config.max_event_bytes);
    loop {
        let chunk = tokio::time::timeout(config.idle_timeout, stream.next())
            .await
            .map_err(|_| ServiceError::Timeout)?;
        match chunk {
            Some(Ok(bytes)) => {
                for record in sse.feed(&bytes)? {
                    for event in decoder.decode(record)? {
                        tx.send(Ok(event))
                            .await
                            .map_err(|_| ServiceError::ConsumerClosed)?;
                    }
                    if decoder.is_done() {
                        return Ok(());
                    }
                }
            }
            Some(Err(error)) => return Err(transport_error(error)),
            None => {
                if sse.has_pending_data() {
                    return Err(ServiceError::Protocol(
                        "unterminated SSE event at EOF".into(),
                    ));
                }
                for event in decoder.end_of_stream()? {
                    tx.send(Ok(event))
                        .await
                        .map_err(|_| ServiceError::ConsumerClosed)?;
                }
                return Ok(());
            }
        }
    }
}
async fn authorize(
    builder: reqwest::RequestBuilder,
    config: &ClientConfig,
    cancel: &CancellationToken,
) -> Result<reqwest::RequestBuilder, ServiceError> {
    match &config.azure {
        Some(azure) => {
            let (header, value) = azure.authorization().await?;
            if cancel.is_cancelled() {
                return Err(ServiceError::Cancelled);
            }
            Ok(builder.header(header, value))
        }
        None => Ok(builder),
    }
}
async fn send_attempt(
    builder: reqwest::RequestBuilder,
    config: &ClientConfig,
    cancel: &CancellationToken,
) -> Result<reqwest::Response, ServiceError> {
    authorize(builder, config, cancel)
        .await?
        .send()
        .await
        .map_err(transport_error)
}
fn transport_error(error: reqwest::Error) -> ServiceError {
    if error.is_timeout() {
        ServiceError::Timeout
    } else {
        ServiceError::Transport(error.without_url().to_string())
    }
}
