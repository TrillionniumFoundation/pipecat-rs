// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    protocol::Decoder,
    stream::{emit, transport_error},
    CancellationToken, ResponseEvent, ResponseStatus, ResponsesConfig, ResponsesRequest,
    ResponsesStream, ServiceError,
};
use futures_util::StreamExt;
use reqwest::header::{ACCEPT, CONTENT_TYPE};
use serde_json::Value;
use tokio::sync::mpsc;

#[derive(Debug, Clone)]
pub struct ResponsesHttpClient {
    config: ResponsesConfig,
    http: reqwest::Client,
}
impl ResponsesHttpClient {
    pub fn new(config: ResponsesConfig) -> Result<Self, ServiceError> {
        config.validate()?;
        let http = reqwest::Client::builder()
            .connect_timeout(config.connect_timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| transport_error())?;
        Ok(Self { config, http })
    }
    pub fn request_body(&self, request: &ResponsesRequest) -> Result<Value, ServiceError> {
        self.config.request_body(request, true)
    }
    fn post(&self, body: &Value, stream: bool) -> Result<reqwest::RequestBuilder, ServiceError> {
        Ok(self
            .http
            .post(&self.config.http_url)
            .headers(self.config.auth_headers()?)
            .header(
                ACCEPT,
                if stream {
                    "text/event-stream"
                } else {
                    "application/json"
                },
            )
            .json(body))
    }
    pub fn stream(&self, request: ResponsesRequest) -> Result<ResponsesStream, ServiceError> {
        self.stream_with_cancel(request, CancellationToken::new())
    }
    pub fn stream_with_cancel(
        &self,
        request: ResponsesRequest,
        cancel: CancellationToken,
    ) -> Result<ResponsesStream, ServiceError> {
        let runtime = tokio::runtime::Handle::try_current()
            .map_err(|_| ServiceError::InvalidRequest("stream requires Tokio runtime".into()))?;
        let body = self.request_body(&request)?;
        let post = self.post(&body, true)?;
        let config = self.config.clone();
        let (tx, rx) = mpsc::channel(config.channel_capacity);
        let owned_cancel = cancel.clone();
        let task = runtime.spawn(async move {
            let mut decoder = Decoder::new(config.max_response_bytes);
            let result = tokio::select! { biased;
                _ = owned_cancel.cancelled() => Err(ServiceError::Cancelled),
                _ = tx.closed() => Err(ServiceError::ConsumerClosed),
                result = tokio::time::timeout(config.request_timeout, async {
                    emit(&tx, Ok(ResponseEvent::Started), config.idle_timeout).await?;
                    run_sse(post, &config, &mut decoder, &tx).await
                }) => result.unwrap_or(Err(ServiceError::Timeout)),
            };
            if matches!(
                &result,
                Err(ServiceError::Cancelled | ServiceError::ConsumerClosed)
            ) {
                decoder.status = Some(ResponseStatus::Cancelled);
                let _ = tx.try_send(Err(ServiceError::Cancelled));
                let _ = tx.try_send(Ok(decoder.finish_event()));
                return;
            }
            if let Err(error) = result {
                decoder.status = Some(ResponseStatus::Failed);
                let _ = emit(&tx, Err(error), config.idle_timeout).await;
            } else if let Some(error) = decoder.error.take() {
                let _ = emit(&tx, Err(error), config.idle_timeout).await;
            }
            let _ = emit(&tx, Ok(decoder.finish_event()), config.idle_timeout).await;
        });
        Ok(ResponsesStream {
            rx,
            cancel,
            task: Some(task),
        })
    }
    /// Non-streaming Responses inference, bounded and cancelled by dropping the
    /// future or the supplied token. This never executes returned function calls.
    pub async fn run_inference(
        &self,
        request: &ResponsesRequest,
        cancel: CancellationToken,
    ) -> Result<Option<String>, ServiceError> {
        let body = self.config.request_body(request, false)?;
        let post = self.post(&body, false)?;
        tokio::select! { biased;
            _ = cancel.cancelled() => Err(ServiceError::Cancelled),
            result = tokio::time::timeout(self.config.request_timeout, async {
                let response = post.send().await.map_err(|_| transport_error())?;
                check_status(&response)?;
                let mut bytes = vec![]; let mut stream = response.bytes_stream();
                while let Some(chunk) = tokio::time::timeout(self.config.idle_timeout, stream.next()).await.map_err(|_| ServiceError::Timeout)? {
                    let chunk = chunk.map_err(|_| transport_error())?;
                    if chunk.len() > self.config.max_response_bytes.saturating_sub(bytes.len()) { return Err(ServiceError::Protocol("inference response exceeds size limit".into())); }
                    bytes.extend_from_slice(&chunk);
                }
                let response: Value = serde_json::from_slice(&bytes).map_err(|_| ServiceError::Protocol("invalid inference JSON".into()))?;
                if response["status"] != "completed" { return Err(crate::protocol::provider_error("response_failed")); }
                let output = response["output"].as_array().ok_or_else(|| ServiceError::Protocol("missing inference output".into()))?;
                let mut text = String::new();
                for item in output {
                    match item["type"].as_str() {
                        Some("message") => {
                            for part in item["content"].as_array().ok_or_else(|| ServiceError::Protocol("missing message content".into()))? {
                                let text_field = match part["type"].as_str() {
                                    Some("output_text") => "text",
                                    Some("refusal") => "refusal",
                                    _ => return Err(ServiceError::Unsupported("non-text inference output".into())),
                                };
                                text.push_str(part[text_field].as_str().ok_or_else(|| ServiceError::Protocol("missing output text".into()))?);
                            }
                        }
                        Some("reasoning" | "function_call") => {},
                        _ => return Err(ServiceError::Unsupported("Responses hosted inference output".into())),
                    }
                }
                Ok(if text.is_empty() { None } else { Some(text) })
            }) => result.unwrap_or(Err(ServiceError::Timeout))
        }
    }
}
async fn run_sse(
    post: reqwest::RequestBuilder,
    config: &ResponsesConfig,
    decoder: &mut Decoder,
    tx: &mpsc::Sender<Result<ResponseEvent, ServiceError>>,
) -> Result<(), ServiceError> {
    let response = post.send().await.map_err(|_| transport_error())?;
    check_status(&response)?;
    if !response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .is_some_and(|v| {
            v.split(';')
                .next()
                .is_some_and(|v| v.trim().eq_ignore_ascii_case("text/event-stream"))
        })
    {
        return Err(ServiceError::Protocol("expected text/event-stream".into()));
    }
    let mut sse = pipecat_services::sse::SseDecoder::new(config.max_event_bytes);
    let mut bytes = 0usize;
    let mut chunks = response.bytes_stream();
    while let Some(chunk) = tokio::time::timeout(config.idle_timeout, chunks.next())
        .await
        .map_err(|_| ServiceError::Timeout)?
    {
        let chunk = chunk.map_err(|_| transport_error())?;
        bytes = bytes
            .checked_add(chunk.len())
            .ok_or_else(|| ServiceError::Protocol("response size limit".into()))?;
        if bytes > config.max_response_bytes {
            return Err(ServiceError::Protocol("response size limit".into()));
        }
        for event in sse.feed(&chunk)? {
            if event.data == "[DONE]" {
                return Err(ServiceError::Protocol(
                    "Responses stream lacks terminal event".into(),
                ));
            }
            let value: Value = serde_json::from_str(&event.data)
                .map_err(|_| ServiceError::Protocol("invalid Responses event JSON".into()))?;
            if event
                .event
                .as_deref()
                .is_some_and(|kind| value["type"].as_str() != Some(kind))
            {
                return Err(ServiceError::Protocol("SSE event type mismatch".into()));
            }
            for event in decoder.feed(value)? {
                emit(tx, Ok(event), config.idle_timeout).await?;
            }
            if decoder.terminal() {
                return Ok(());
            }
        }
    }
    Err(ServiceError::Protocol(
        "Responses stream ended before terminal event".into(),
    ))
}
fn check_status(response: &reqwest::Response) -> Result<(), ServiceError> {
    if response.status().is_success() {
        return Ok(());
    }
    let retry_after = response
        .headers()
        .get("retry-after")
        .and_then(|v| v.to_str().ok())
        .filter(|v| v.len() <= 10 && v.bytes().all(|b| b.is_ascii_digit()))
        .map(str::to_owned);
    Err(ServiceError::Http {
        status: response.status().as_u16(),
        message: "Responses HTTP error (body redacted)".into(),
        retry_after,
    })
}
