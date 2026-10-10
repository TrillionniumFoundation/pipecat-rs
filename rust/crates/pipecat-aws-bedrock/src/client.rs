//! Signed Bedrock REST calls and incremental binary response consumption.
use crate::{
    adapter,
    protocol::{BedrockEvent, ConverseDecoder},
    AwsCredentials, CompletionRequest, ServiceError,
};
use futures_util::StreamExt;
use pipecat_aws_common::auth;
use pipecat_services::{CancellationToken, RetryPolicy};
use serde_json::Value;
use std::{fmt, io::Write, time::Duration};
use tokio::{sync::mpsc, task::JoinHandle};
#[derive(Debug, Clone)]
pub struct BedrockLimits {
    pub max_request_bytes: usize,
    pub max_event_bytes: usize,
    pub max_response_bytes: usize,
    pub max_output_bytes: usize,
    pub queue_capacity: usize,
    pub connect_timeout: Duration,
    pub idle_timeout: Duration,
    pub request_timeout: Duration,
}
impl Default for BedrockLimits {
    fn default() -> Self {
        Self {
            max_request_bytes: 24 * 1024 * 1024,
            max_event_bytes: 4 * 1024 * 1024,
            max_response_bytes: 64 * 1024 * 1024,
            max_output_bytes: 8 * 1024 * 1024,
            queue_capacity: 16,
            connect_timeout: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            request_timeout: Duration::from_secs(300),
        }
    }
}
#[derive(Clone)]
pub struct BedrockConfig {
    pub credentials: AwsCredentials,
    pub region: String,
    /// Exact regional origin, or a literal loopback origin for protocol tests.
    pub endpoint: String,
    pub limits: BedrockLimits,
    pub retry: RetryPolicy,
}
impl fmt::Debug for BedrockConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BedrockConfig")
            .field("credentials", &self.credentials)
            .field("region", &self.region)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl BedrockConfig {
    pub fn new(credentials: AwsCredentials, region: impl Into<String>) -> Self {
        let region = region.into();
        Self {
            endpoint: format!(
                "https://bedrock-runtime.{region}.{}/",
                auth::domain(&region)
            ),
            credentials,
            region,
            limits: BedrockLimits::default(),
            retry: RetryPolicy::Never,
        }
    }
    fn url(&self) -> Result<url::Url, ServiceError> {
        auth::region(&self.region).map_err(|_| invalid("AWS region"))?;
        let l = &self.limits;
        if l.max_request_bytes == 0
            || l.max_request_bytes > 64 * 1024 * 1024
            || l.max_event_bytes < 16
            || l.max_event_bytes > 16 * 1024 * 1024
            || l.max_response_bytes == 0
            || l.max_response_bytes > 256 * 1024 * 1024
            || l.max_output_bytes == 0
            || l.max_output_bytes > 8 * 1024 * 1024
            || l.queue_capacity == 0
            || l.queue_capacity > 1024
            || l.queue_capacity.saturating_mul(l.max_output_bytes) > 256 * 1024 * 1024
            || l.connect_timeout.is_zero()
            || l.idle_timeout.is_zero()
            || l.request_timeout.is_zero()
            || matches!(self.retry,RetryPolicy::OnceOnHeaderTimeout{timeout} if timeout.is_zero())
        {
            return Err(invalid(
                "Bedrock limits must be positive and within supported bounds",
            ));
        }
        auth::endpoint(
            &self.endpoint,
            &format!(
                "https://bedrock-runtime.{}.{}/",
                self.region,
                auth::domain(&self.region)
            ),
            "/",
            false,
        )
        .map_err(|_| invalid("Bedrock endpoint must be regional AWS origin or literal loopback"))
    }
}
fn invalid(s: &str) -> ServiceError {
    ServiceError::InvalidRequest(s.into())
}
fn transport(e: reqwest::Error) -> ServiceError {
    if e.is_timeout() {
        ServiceError::Timeout
    } else {
        ServiceError::Transport("Bedrock request failed".into())
    }
}
fn protocol(s: &str) -> ServiceError {
    ServiceError::Protocol(s.into())
}
#[derive(Clone, Debug)]
pub struct BedrockClient {
    config: BedrockConfig,
    http: reqwest::Client,
    origin: url::Url,
}
impl BedrockClient {
    pub fn new(config: BedrockConfig) -> Result<Self, ServiceError> {
        let origin = config.url()?;
        let http = reqwest::Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy()
            .connect_timeout(config.limits.connect_timeout)
            .build()
            .map_err(transport)?;
        Ok(Self {
            config,
            http,
            origin,
        })
    }
    pub fn limits(&self) -> &BedrockLimits {
        &self.config.limits
    }
    pub fn prepare(
        &self,
        request: &CompletionRequest,
        streaming: bool,
    ) -> Result<adapter::PreparedRequest, ServiceError> {
        bounded_json(request, self.config.limits.max_request_bytes)?;
        let result = adapter::prepare(request, streaming)?;
        bounded_json(&result.body, self.config.limits.max_request_bytes)?;
        Ok(result)
    }
    pub fn request_body(&self, request: &CompletionRequest) -> Result<Value, ServiceError> {
        Ok(self.prepare(request, true)?.body)
    }
    fn route(&self, model: &str, streaming: bool) -> Result<(url::Url, String), ServiceError> {
        let encoded = auth::encode_component(model);
        let suffix = if streaming {
            "converse-stream"
        } else {
            "converse"
        };
        let path = format!("/model/{encoded}/{suffix}");
        let canonical = format!("/model/{}/{suffix}", auth::encode_component(&encoded));
        let mut url = self.origin.clone();
        url.set_path(&path);
        if url.path() != path {
            return Err(invalid("Bedrock model route encoding"));
        }
        Ok((url, canonical))
    }
    /// Signs a fresh request using an explicit timestamp, useful for offline protocol vectors.
    pub fn signed_request(
        &self,
        request: &CompletionRequest,
        streaming: bool,
        timestamp: &str,
    ) -> Result<reqwest::Request, ServiceError> {
        let p = self.prepare(request, streaming)?;
        let body = bounded_json(&p.body, self.config.limits.max_request_bytes)?;
        let (url, canonical) = self.route(&p.model, streaming)?;
        let headers = auth::signed_post_headers(
            &url,
            &self.config.region,
            &self.config.credentials,
            &body,
            timestamp,
            "bedrock",
            &canonical,
        )
        .map_err(|_| invalid("Bedrock signing input"))?;
        self.http
            .post(url)
            .headers(headers)
            .header(
                "accept",
                if streaming {
                    "application/vnd.amazon.eventstream"
                } else {
                    "application/json"
                },
            )
            .body(body)
            .build()
            .map_err(transport)
    }
    async fn response(
        &self,
        request: &CompletionRequest,
        streaming: bool,
    ) -> Result<reqwest::Response, ServiceError> {
        let send = || -> Result<_, ServiceError> {
            let request = self.signed_request(request, streaming, &auth::now())?;
            Ok(self.http.execute(request))
        };
        let retry = if streaming {
            &self.config.retry
        } else {
            &RetryPolicy::Never
        };
        let response = match retry {
            RetryPolicy::Never => send()?.await.map_err(transport)?,
            RetryPolicy::OnceOnHeaderTimeout { timeout } => {
                match tokio::time::timeout(*timeout, send()?).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(e)) if !e.is_timeout() => return Err(transport(e)),
                    _ => send()?.await.map_err(transport)?,
                }
            }
        };
        if !response.status().is_success() {
            return Err(ServiceError::Http {
                status: response.status().as_u16(),
                message: "Bedrock rejected the request".into(),
                retry_after: None,
            });
        }
        let expected = if streaming {
            "application/vnd.amazon.eventstream"
        } else {
            "application/json"
        };
        if response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .and_then(|s| s.split(';').next())
            .map(str::trim)
            != Some(expected)
        {
            return Err(protocol("unexpected Bedrock content type"));
        }
        if response
            .content_length()
            .is_some_and(|n| n > self.config.limits.max_response_bytes as u64)
        {
            return Err(protocol("Bedrock response exceeds configured limit"));
        }
        Ok(response)
    }
    pub fn stream(&self, request: CompletionRequest) -> Result<BedrockStream, ServiceError> {
        self.prepare(&request, true)?;
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| invalid("Bedrock streaming requires a Tokio runtime"))?;
        let client = self.clone();
        let cancel = CancellationToken::new();
        let c = cancel.clone();
        let (tx, rx) = mpsc::channel(self.config.limits.queue_capacity);
        let timeout = self.config.limits.request_timeout;
        let task = runtime.spawn(async move {
            tokio::select! {biased;_ = c.cancelled()=>Err(ServiceError::Cancelled),result=tokio::time::timeout(timeout,client.run_stream(request,tx))=>result.map_err(|_|ServiceError::Timeout)?}
        });
        Ok(BedrockStream {
            rx,
            task,
            cancel,
            ended: false,
        })
    }
    async fn run_stream(
        &self,
        request: CompletionRequest,
        tx: mpsc::Sender<BedrockEvent>,
    ) -> Result<(), ServiceError> {
        let response = self.response(&request, true).await?;
        let mut chunks = response.bytes_stream();
        let l = &self.config.limits;
        let mut decoder = ConverseDecoder::new(l.max_event_bytes, l.max_output_bytes);
        let mut frame = Vec::new();
        let mut target = 12;
        let mut total = 0usize;
        while let Some(chunk) = tokio::time::timeout(l.idle_timeout, chunks.next())
            .await
            .map_err(|_| ServiceError::Timeout)?
        {
            let chunk = chunk.map_err(transport)?;
            if chunk.len() > l.max_response_bytes.saturating_sub(total) {
                return Err(protocol("Bedrock response exceeds configured limit"));
            }
            total += chunk.len();
            let mut bytes = chunk.as_ref();
            while !bytes.is_empty() {
                let n = (target - frame.len()).min(bytes.len());
                frame.extend_from_slice(&bytes[..n]);
                bytes = &bytes[n..];
                if frame.len() != target {
                    continue;
                }
                if target == 12 {
                    let n = u32::from_be_bytes(frame[..4].try_into().expect("prelude")) as usize;
                    let h = u32::from_be_bytes(frame[4..8].try_into().expect("prelude")) as usize;
                    // Validate the prelude before trusting its allocation length.
                    pipecat_aws_common::eventstream::validate_prelude(&frame, l.max_event_bytes)
                        .map_err(|_| protocol("invalid Bedrock EventStream prelude"))?;
                    if n < 16 || h > n - 16 || n > l.max_event_bytes {
                        return Err(protocol("Bedrock event exceeds configured limit"));
                    }
                    target = n;
                    continue;
                }
                for event in decoder.event(&frame)? {
                    tx.send(event)
                        .await
                        .map_err(|_| ServiceError::ConsumerClosed)?;
                }
                frame.clear();
                target = 12;
            }
        }
        if !frame.is_empty() {
            return Err(protocol("truncated Bedrock EventStream record"));
        }
        tx.send(decoder.finish()?)
            .await
            .map_err(|_| ServiceError::ConsumerClosed)?;
        Ok(())
    }
    /// Calls the non-streaming Converse route. Tools are never executed.
    pub async fn converse(
        &self,
        request: CompletionRequest,
        max_output_bytes: usize,
    ) -> Result<Option<String>, ServiceError> {
        if max_output_bytes == 0 {
            return Err(invalid("inference output limit must be positive"));
        }
        self.prepare(&request, false)?;
        tokio::time::timeout(self.config.limits.request_timeout, async {
            let mut stream = self.response(&request, false).await?.bytes_stream();
            let mut data = Vec::new();
            while let Some(chunk) =
                tokio::time::timeout(self.config.limits.idle_timeout, stream.next())
                    .await
                    .map_err(|_| ServiceError::Timeout)?
            {
                let bytes = chunk.map_err(transport)?;
                if bytes.len()
                    > self
                        .config
                        .limits
                        .max_response_bytes
                        .saturating_sub(data.len())
                {
                    return Err(protocol("Bedrock response exceeds configured limit"));
                }
                data.extend_from_slice(&bytes);
            }
            let value: Value = serde_json::from_slice(&data)
                .map_err(|_| protocol("invalid Bedrock Converse JSON"))?;
            let content = value
                .pointer("/output/message/content")
                .and_then(Value::as_array)
                .ok_or_else(|| protocol("missing Bedrock Converse content"))?;
            for part in content {
                if let Some(s) = part
                    .get("text")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    if s.len() > max_output_bytes {
                        return Err(protocol(
                            "Bedrock inference output exceeds configured limit",
                        ));
                    }
                    return Ok(Some(s.into()));
                }
            }
            Ok(None)
        })
        .await
        .map_err(|_| ServiceError::Timeout)?
    }
}
pub struct BedrockStream {
    rx: mpsc::Receiver<BedrockEvent>,
    task: JoinHandle<Result<(), ServiceError>>,
    cancel: CancellationToken,
    ended: bool,
}
impl BedrockStream {
    pub fn cancellation_token(&self) -> CancellationToken {
        self.cancel.clone()
    }
    pub async fn next(&mut self) -> Option<Result<BedrockEvent, ServiceError>> {
        if self.ended {
            return None;
        }
        if !self.cancel.is_cancelled() {
            tokio::select! {biased;_=self.cancel.cancelled()=>{},event=self.rx.recv()=>{if let Some(event)=event{return Some(Ok(event));}}}
        }
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
        let result = (&mut self.task).await;
        self.ended = true;
        if self.cancel.is_cancelled() {
            return Some(Err(ServiceError::Cancelled));
        }
        match result {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(Err(e)),
            Err(_) => Some(Err(ServiceError::Transport(
                "Bedrock stream task stopped".into(),
            ))),
        }
    }
}
impl Drop for BedrockStream {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}
struct LimitedWriter {
    bytes: Vec<u8>,
    limit: usize,
}
impl Write for LimitedWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(std::io::Error::other("size limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
fn bounded_json(value: &impl serde::Serialize, limit: usize) -> Result<Vec<u8>, ServiceError> {
    let mut writer = LimitedWriter {
        bytes: vec![],
        limit,
    };
    serde_json::to_writer(&mut writer, value)
        .map_err(|_| invalid("Bedrock request exceeds limit or contains invalid values"))?;
    Ok(writer.bytes)
}
