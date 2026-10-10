use crate::{cleanup::CleanupStatus, config::HttpConfig, Limits, McpError};
use bytes::Bytes;
use futures_util::{future, stream::BoxStream, StreamExt};
use http::{HeaderName, HeaderValue};
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{
        common::client_side_sse::SseRetryPolicy,
        streamable_http_client::{
            SseError, StreamableHttpClient, StreamableHttpClientTransportConfig,
            StreamableHttpError, StreamableHttpPostResponse,
        },
        StreamableHttpClientTransport,
    },
};
use sse_stream::{Sse, SseStream};
use std::{collections::HashMap, io, sync::Arc, time::Duration};

type HttpError = StreamableHttpError<io::Error>;
#[derive(Clone)]
pub(crate) struct BoundedHttp {
    client: reqwest::Client,
    max_bytes: usize,
    terminate: bool,
    cleanup: CleanupStatus,
    close_timeout: Duration,
}
#[derive(Debug)]
struct NoRetry;
impl SseRetryPolicy for NoRetry {
    fn retry(&self, _: usize) -> Option<Duration> {
        None
    }
}

pub(crate) fn transport(
    config: &HttpConfig,
    limits: &Limits,
    cleanup: CleanupStatus,
) -> Result<StreamableHttpClientTransport<BoundedHttp>, McpError> {
    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .retry(reqwest::retry::never())
        .no_proxy()
        .connect_timeout(limits.connect_timeout)
        .read_timeout(limits.request_timeout)
        .build()
        .map_err(|_| McpError::Initialization)?;
    let mut headers = HashMap::new();
    for (name, value) in &config.headers {
        let mut value =
            HeaderValue::from_str(value).map_err(|_| McpError::Config("invalid MCP header"))?;
        value.set_sensitive(true);
        headers.insert(
            HeaderName::from_bytes(name.as_bytes())
                .map_err(|_| McpError::Config("invalid MCP header"))?,
            value,
        );
    }
    let mut settings = StreamableHttpClientTransportConfig::with_uri(config.url.clone())
        .custom_headers(headers)
        .max_concurrent_requests(limits.max_in_flight)
        .max_sse_event_size(limits.max_wire_bytes)
        .control_request_timeout(limits.close_timeout)
        .reinit_on_expired_session(false);
    settings.retry_config = Arc::new(NoRetry);
    settings.channel_buffer_capacity = limits.max_in_flight;
    Ok(StreamableHttpClientTransport::with_client(
        BoundedHttp {
            client,
            max_bytes: limits.max_wire_bytes,
            terminate: config.terminate_on_close,
            cleanup,
            close_timeout: limits.close_timeout,
        },
        settings,
    ))
}
fn fail(message: &'static str) -> HttpError {
    StreamableHttpError::Client(io::Error::other(message))
}
fn request_headers(
    mut request: reqwest::RequestBuilder,
    session: Option<Arc<str>>,
    auth: Option<String>,
    headers: HashMap<HeaderName, HeaderValue>,
) -> reqwest::RequestBuilder {
    if let Some(session) = session {
        request = request.header("mcp-session-id", session.as_ref());
    }
    if let Some(auth) = auth {
        request = request.bearer_auth(auth);
    }
    for (key, value) in headers {
        request = request.header(key, value);
    }
    request
}
async fn body(response: reqwest::Response, limit: usize) -> Result<Vec<u8>, HttpError> {
    if response
        .content_length()
        .is_some_and(|size| size > limit as u64)
    {
        return Err(fail("MCP response exceeds limit"));
    }
    let mut stream = response.bytes_stream();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|_| fail("MCP HTTP body failed"))?;
        if chunk.len() > limit.saturating_sub(bytes.len()) {
            return Err(fail("MCP response exceeds limit"));
        }
        bytes.extend_from_slice(&chunk);
    }
    Ok(bytes)
}

/// Tracks raw bytes before the SDK's SSE parser can allocate an event. Empty
/// lines reset the budget; CRLF split between network chunks counts once.
#[derive(Default)]
struct EventBudget {
    bytes: usize,
    line: usize,
    cr: bool,
    failed: bool,
}
impl EventBudget {
    fn observe(&mut self, bytes: &[u8], max: usize) -> Result<(), io::Error> {
        for &byte in bytes {
            if self.cr {
                self.cr = false;
                if byte == b'\n' {
                    continue;
                }
            }
            if matches!(byte, b'\r' | b'\n') {
                if self.line == 0 {
                    self.bytes = 0;
                } else {
                    self.bytes = self.bytes.saturating_add(1);
                }
                self.line = 0;
                self.cr = byte == b'\r';
            } else {
                self.bytes = self.bytes.saturating_add(1);
                self.line += 1;
            }
            if self.bytes > max {
                return Err(io::Error::other("MCP SSE event exceeds limit"));
            }
        }
        Ok(())
    }
}
fn events(response: reqwest::Response, limit: usize) -> BoxStream<'static, Result<Sse, SseError>> {
    let stream = response
        .bytes_stream()
        .scan(EventBudget::default(), move |state, chunk| {
            let result: Option<Result<Bytes, io::Error>> = if state.failed {
                None
            } else {
                let result = chunk
                    .map_err(|_| io::Error::other("MCP SSE stream failed"))
                    .and_then(|bytes| {
                        state.observe(&bytes, limit)?;
                        Ok(bytes)
                    });
                state.failed = result.is_err();
                Some(result)
            };
            future::ready(result)
        });
    SseStream::from_bytes_stream(stream).boxed()
}
fn media_type(response: &reqwest::Response) -> Option<&str> {
    response
        .headers()
        .get("content-type")?
        .to_str()
        .ok()?
        .split(';')
        .next()
        .map(str::trim)
}
impl StreamableHttpClient for BoundedHttp {
    type Error = io::Error;
    async fn post_message(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        self.post_message_with_max_sse_event_size(
            uri,
            message,
            session,
            auth,
            headers,
            self.max_bytes,
        )
        .await
    }
    async fn post_message_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        message: ClientJsonRpcMessage,
        session: Option<Arc<str>>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        max_sse: usize,
    ) -> Result<StreamableHttpPostResponse, HttpError> {
        let request = request_headers(
            self.client
                .post(uri.as_ref())
                .header("accept", "application/json, text/event-stream")
                .header("content-type", "application/json"),
            session.clone(),
            auth,
            headers,
        );
        let bytes = serde_json::to_vec(&message).map_err(|_| fail("invalid MCP message"))?;
        if bytes.len() > self.max_bytes {
            return Err(fail("MCP request exceeds limit"));
        }
        let response = request
            .body(bytes)
            .send()
            .await
            .map_err(|_| fail("MCP HTTP request failed"))?;
        let status = response.status();
        if status == reqwest::StatusCode::NOT_FOUND && session.is_some() {
            return Err(StreamableHttpError::SessionExpired);
        }
        if status.is_redirection() {
            return Err(fail("MCP HTTP redirects are disabled"));
        }
        if matches!(status.as_u16(), 202 | 204) {
            return Ok(StreamableHttpPostResponse::Accepted);
        }
        let session = response
            .headers()
            .get("mcp-session-id")
            .map(|s| s.to_str().map(str::to_owned))
            .transpose()
            .map_err(|_| fail("invalid MCP session header"))?;
        if session
            .as_ref()
            .is_some_and(|s| s.len() > 1024 || s.is_empty())
        {
            return Err(fail("invalid MCP session header"));
        }
        let content_type = media_type(&response).map(str::to_owned);
        if status.is_success() && content_type.as_deref() == Some("text/event-stream") {
            return Ok(StreamableHttpPostResponse::Sse(
                events(response, max_sse.min(self.max_bytes)),
                session,
            ));
        }
        if content_type.as_deref() != Some("application/json") {
            return Err(fail("unexpected MCP HTTP status or content type"));
        }
        let bytes = body(response, self.max_bytes).await?;
        let message: ServerJsonRpcMessage =
            serde_json::from_slice(&bytes).map_err(|_| fail("invalid MCP JSON response"))?;
        if !status.is_success() && !matches!(message, ServerJsonRpcMessage::Error(_)) {
            return Err(fail("MCP HTTP request rejected"));
        }
        Ok(StreamableHttpPostResponse::Json(message, session))
    }
    async fn delete_session(
        &self,
        uri: Arc<str>,
        session: Arc<str>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<(), HttpError> {
        let attempt = self.cleanup.begin();
        if !self.terminate {
            attempt.success();
            return Ok(());
        }
        let response = request_headers(
            self.client.delete(uri.as_ref()).timeout(self.close_timeout),
            Some(session),
            auth,
            headers,
        )
        .send()
        .await
        .map_err(|_| fail("MCP HTTP cleanup failed"))?;
        if response.status().is_success() || matches!(response.status().as_u16(), 404 | 405) {
            attempt.success();
            Ok(())
        } else {
            Err(fail("MCP HTTP cleanup rejected"))
        }
    }
    async fn get_stream(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        event_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        self.get_stream_with_max_sse_event_size(
            uri,
            session,
            event_id,
            auth,
            headers,
            self.max_bytes,
        )
        .await
    }
    async fn get_stream_with_max_sse_event_size(
        &self,
        uri: Arc<str>,
        session: Option<Arc<str>>,
        event_id: Option<String>,
        auth: Option<String>,
        headers: HashMap<HeaderName, HeaderValue>,
        max_sse: usize,
    ) -> Result<BoxStream<'static, Result<Sse, SseError>>, HttpError> {
        let mut request = request_headers(
            self.client
                .get(uri.as_ref())
                .header("accept", "text/event-stream"),
            session,
            auth,
            headers,
        );
        if let Some(event_id) = event_id {
            request = request.header("last-event-id", event_id);
        }
        let response = request
            .send()
            .await
            .map_err(|_| fail("MCP HTTP stream failed"))?;
        if response.status() == reqwest::StatusCode::METHOD_NOT_ALLOWED {
            return Err(StreamableHttpError::ServerDoesNotSupportSse);
        }
        if !response.status().is_success() || media_type(&response) != Some("text/event-stream") {
            return Err(fail("MCP HTTP stream rejected"));
        }
        Ok(events(response, max_sse.min(self.max_bytes)))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn event_limits_apply_to_fragmented_lines_crlf_and_empty_data() {
        let mut budget = EventBudget::default();
        budget.observe(b"data: x\r", 9).unwrap();
        budget.observe(b"\n\r\n", 9).unwrap();
        budget.observe(b"data: x\n\n", 9).unwrap();
        assert!(budget.observe(b"data: 123456", 9).is_err());
        let mut budget = EventBudget::default();
        for _ in 0..4 {
            budget.observe(b"data:\n", 25).unwrap();
        }
        assert!(budget.observe(b"data:\n", 25).is_err());
    }
}
