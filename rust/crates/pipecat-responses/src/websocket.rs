// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{
    protocol::Decoder,
    stream::{emit, transport_error},
    CancellationToken, ResponseEvent, ResponseStatus, ResponsesConfig, ResponsesRequest,
    ResponsesStream, ServiceError,
};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{collections::HashSet, sync::Arc, time::Instant};
use tokio::{
    net::TcpStream,
    sync::{mpsc, Mutex},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
struct Previous {
    id: String,
    input: Vec<Value>,
    output: Vec<Value>,
    options: Value,
}
struct Connection {
    socket: Socket,
    opened: Instant,
    previous: Option<Previous>,
    seen_response_ids: HashSet<String>,
}
#[derive(Default)]
struct Session {
    connection: Option<Connection>,
}
/// Clones share one serialized, connection-local Responses session. Concurrent
/// callers queue with bounded total time; independent conversations need separate clients.
#[derive(Clone)]
pub struct ResponsesWebSocketClient {
    config: ResponsesConfig,
    session: Arc<Mutex<Session>>,
}
impl std::fmt::Debug for ResponsesWebSocketClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ResponsesWebSocketClient")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl ResponsesWebSocketClient {
    pub fn new(config: ResponsesConfig) -> Result<Self, ServiceError> {
        config.validate()?;
        Ok(Self {
            config,
            session: Arc::new(Mutex::new(Session::default())),
        })
    }
    pub fn request_body(&self, request: &ResponsesRequest) -> Result<Value, ServiceError> {
        let mut body = self.config.request_body(request, true)?;
        body.as_object_mut().unwrap().remove("stream");
        body["type"] = json!("response.create");
        if body.to_string().len() > self.config.max_request_bytes {
            return Err(ServiceError::InvalidRequest(
                "WebSocket request exceeds configured size limit".into(),
            ));
        }
        Ok(body)
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
        let client = self.clone();
        let owned_cancel = cancel.clone();
        let (tx, rx) = mpsc::channel(self.config.channel_capacity);
        let task = runtime.spawn(async move {
            client.run(body, owned_cancel, tx).await;
        });
        Ok(ResponsesStream {
            rx,
            cancel,
            task: Some(task),
        })
    }
    /// Explicitly close a reusable session. Ongoing generation is serialized
    /// before closure; cancel its stream first to interrupt it immediately.
    pub async fn close(&self) {
        let mut guard = self.session.lock().await;
        if let Some(mut conn) = guard.connection.take() {
            let _ = tokio::time::timeout(self.config.drain_timeout, conn.socket.close(None)).await;
        }
    }
    async fn run(
        &self,
        body: Value,
        cancel: CancellationToken,
        tx: mpsc::Sender<Result<ResponseEvent, ServiceError>>,
    ) {
        let deadline = tokio::time::Instant::now() + self.config.request_timeout;
        let mut decoder = Decoder::new(self.config.max_response_bytes);
        let guard = tokio::select! { biased;
            _ = cancel.cancelled() => Err(ServiceError::Cancelled),
            _ = tx.closed() => Err(ServiceError::ConsumerClosed),
            result = tokio::time::timeout_at(deadline, self.session.lock()) => result.map_err(|_| ServiceError::Timeout),
        };
        let mut guard = match guard {
            Ok(guard) => guard,
            Err(error) => {
                decoder.status = Some(
                    if matches!(
                        error,
                        ServiceError::Cancelled | ServiceError::ConsumerClosed
                    ) {
                        ResponseStatus::Cancelled
                    } else {
                        ResponseStatus::Failed
                    },
                );
                let _ = tx.try_send(Err(error));
                let _ = tx.try_send(Ok(decoder.finish_event()));
                return;
            }
        };
        // The socket leaves shared state while in use. Dropping or aborting this
        // task can never leave a half-consumed connection available for reuse.
        let mut connection = guard.connection.take();
        if connection.as_ref().is_some_and(|c| {
            c.opened.elapsed() >= self.config.max_connection_age
                || c.seen_response_ids.len() >= 1024
        }) {
            connection = None;
        }
        let mut sent = false;
        let result = tokio::select! { biased;
            _ = cancel.cancelled() => Err(ServiceError::Cancelled),
            _ = tx.closed() => Err(ServiceError::ConsumerClosed),
            result = tokio::time::timeout_at(deadline, async {
                emit(&tx, Ok(ResponseEvent::Started), self.config.idle_timeout).await?;
                self.exchange(&body, &mut connection, &mut decoder, &mut sent, &tx).await
            }) => result.unwrap_or(Err(ServiceError::Timeout)),
        };
        let reusable = match &result {
            Ok(()) => true,
            Err(ServiceError::Cancelled | ServiceError::ConsumerClosed) => {
                decoder.status = Some(ResponseStatus::Cancelled);
                if let Some(conn) = &mut connection {
                    conn.previous = None;
                    !sent || self.drain(conn, decoder.id.as_deref()).await
                } else {
                    false
                }
            }
            _ => false,
        };
        if reusable {
            guard.connection = connection;
        }
        drop(guard);
        if matches!(
            result,
            Err(ServiceError::Cancelled | ServiceError::ConsumerClosed)
        ) {
            let _ = tx.try_send(Err(ServiceError::Cancelled));
            let _ = tx.try_send(Ok(decoder.finish_event()));
            return;
        }
        if let Err(error) = result {
            decoder.status = Some(ResponseStatus::Failed);
            let _ = emit(&tx, Err(error), self.config.idle_timeout).await;
        } else if let Some(error) = decoder.error.take() {
            let _ = emit(&tx, Err(error), self.config.idle_timeout).await;
        }
        let _ = emit(&tx, Ok(decoder.finish_event()), self.config.idle_timeout).await;
    }
    async fn connect(&self) -> Result<Connection, ServiceError> {
        let mut request = self
            .config
            .websocket_url
            .clone()
            .into_client_request()
            .map_err(|_| ServiceError::InvalidRequest("invalid WebSocket request".into()))?;
        request.headers_mut().extend(self.config.auth_headers()?);
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(self.config.max_event_bytes))
            .max_frame_size(Some(self.config.max_event_bytes));
        let (socket, _) = tokio::time::timeout(
            self.config.connect_timeout,
            connect_async_with_config(request, Some(ws_config), false),
        )
        .await
        .map_err(|_| ServiceError::Timeout)?
        .map_err(|_| transport_error())?;
        Ok(Connection {
            socket,
            opened: Instant::now(),
            previous: None,
            seen_response_ids: HashSet::new(),
        })
    }
    async fn exchange(
        &self,
        body: &Value,
        connection: &mut Option<Connection>,
        decoder: &mut Decoder,
        sent: &mut bool,
        tx: &mpsc::Sender<Result<ResponseEvent, ServiceError>>,
    ) -> Result<(), ServiceError> {
        let full_input = body["input"].as_array().expect("validated input").clone();
        for attempt in 0..=1 {
            if connection.is_none() {
                *connection = Some(self.connect().await?);
            }
            let conn = connection.as_mut().unwrap();
            let mut request = body.clone();
            if attempt == 0 {
                apply_previous(&mut request, conn.previous.as_ref());
            }
            let used_previous = request.get("previous_response_id").is_some();
            conn.previous = None;
            let payload = request.to_string();
            if payload.len() > self.config.max_request_bytes {
                return Err(ServiceError::InvalidRequest(
                    "WebSocket request exceeds configured size limit".into(),
                ));
            }
            *sent = true;
            tokio::time::timeout(
                self.config.idle_timeout,
                conn.socket.send(Message::Text(payload.into())),
            )
            .await
            .map_err(|_| ServiceError::Timeout)?
            .map_err(|_| transport_error())?;
            loop {
                let event = self.receive(&mut conn.socket).await?;
                let kind = event["type"].as_str().unwrap_or("");
                if kind == "response.created" {
                    let id = event["response"]["id"]
                        .as_str()
                        .filter(|id| !id.is_empty() && id.len() <= 256)
                        .ok_or_else(|| {
                            ServiceError::Protocol("invalid Responses response id".into())
                        })?;
                    if !conn.seen_response_ids.insert(id.to_owned()) {
                        return Err(ServiceError::Protocol("stale Responses response id".into()));
                    }
                } else if kind != "error" && decoder.id.is_none() {
                    return Err(ServiceError::Protocol(
                        "WebSocket output arrived before response.created".into(),
                    ));
                }
                let expected_id = if kind == "response.created" {
                    event["response"]["id"].as_str()
                } else {
                    decoder.id.as_deref()
                };
                validate_explicit_identity(&event, expected_id)?;
                if matches!(
                    kind,
                    "response.completed" | "response.failed" | "response.incomplete"
                ) && event["response"]["id"].as_str() != expected_id
                {
                    return Err(ServiceError::Protocol(
                        "WebSocket terminal response id missing or mismatched".into(),
                    ));
                }
                for event in decoder.feed(event)? {
                    emit(tx, Ok(event), self.config.idle_timeout).await?;
                }
                if decoder.terminal() {
                    break;
                }
            }
            let retry = attempt == 0
                && self.config.retry_rejected_create
                && match decoder.retry_code {
                    Some("previous_response_not_found") => used_previous,
                    Some("websocket_connection_limit_reached") => true,
                    _ => false,
                };
            if retry {
                if decoder.retry_code == Some("websocket_connection_limit_reached") {
                    *connection = None;
                }
                *decoder = Decoder::new(self.config.max_response_bytes);
                continue;
            }
            if decoder.status == Some(ResponseStatus::Completed) && decoder.output_complete {
                if let Some(id) = &decoder.id {
                    conn.previous = Some(Previous {
                        id: id.clone(),
                        input: full_input.clone(),
                        output: decoder.output.clone(),
                        options: options(body),
                    });
                }
            }
            // A provider failure has a terminal boundary, but an uncorrelated
            // error may leave remote work running. Never reuse that socket.
            if decoder.error.is_some() && decoder.retry_code.is_none() {
                *connection = None;
            }
            return Ok(());
        }
        unreachable!("two attempts always return")
    }
    async fn receive(&self, socket: &mut Socket) -> Result<Value, ServiceError> {
        // Ping/control traffic cannot extend the application idle deadline.
        tokio::time::timeout(self.config.idle_timeout, async {
            loop {
                let message = socket
                    .next()
                    .await
                    .ok_or_else(transport_error)?
                    .map_err(|_| transport_error())?;
                match message {
                    Message::Text(text) => {
                        return serde_json::from_str(&text).map_err(|_| {
                            ServiceError::Protocol("invalid Responses WebSocket JSON".into())
                        })
                    }
                    Message::Ping(payload) => socket
                        .send(Message::Pong(payload))
                        .await
                        .map_err(|_| transport_error())?,
                    Message::Pong(_) => {}
                    Message::Close(_) => return Err(transport_error()),
                    _ => {
                        return Err(ServiceError::Protocol(
                            "Responses expects text WebSocket messages".into(),
                        ))
                    }
                }
            }
        })
        .await
        .map_err(|_| ServiceError::Timeout)?
    }
    async fn drain(&self, conn: &mut Connection, id: Option<&str>) -> bool {
        tokio::time::timeout(self.config.drain_timeout, async {
            let mut response_id = id.map(str::to_owned);
            if let Some(id) = &response_id {
                conn.socket
                    .send(Message::Text(
                        json!({"type":"response.cancel","response_id":id})
                            .to_string()
                            .into(),
                    ))
                    .await
                    .map_err(|_| transport_error())?;
            }
            let mut bytes = 0usize;
            loop {
                let value = self.receive(&mut conn.socket).await?;
                bytes = bytes
                    .checked_add(value.to_string().len())
                    .ok_or_else(transport_error)?;
                if bytes > self.config.max_response_bytes {
                    return Err(transport_error());
                }
                let expected_id = if value["type"] == "response.created" && response_id.is_none() {
                    value["response"]["id"].as_str()
                } else {
                    response_id.as_deref()
                };
                validate_explicit_identity(&value, expected_id)?;
                match value["type"].as_str() {
                    Some("response.created") if response_id.is_none() => {
                        let id = value["response"]["id"]
                            .as_str()
                            .filter(|id| !id.is_empty() && id.len() <= 256)
                            .ok_or_else(transport_error)?;
                        if !conn.seen_response_ids.insert(id.to_owned()) {
                            return Err(transport_error());
                        }
                        conn.socket
                            .send(Message::Text(
                                json!({"type":"response.cancel","response_id":id})
                                    .to_string()
                                    .into(),
                            ))
                            .await
                            .map_err(|_| transport_error())?;
                        response_id = Some(id.to_owned());
                    }
                    Some("response.completed" | "response.failed" | "response.incomplete") => {
                        let id = response_id.as_deref().ok_or_else(transport_error)?;
                        if value["response"]["id"].as_str() != Some(id) {
                            return Err(transport_error());
                        }
                        return Ok(());
                    }
                    Some(
                        "response.output_text.delta"
                        | "response.output_text.done"
                        | "response.reasoning_summary_text.delta"
                        | "response.reasoning_summary_text.done"
                        | "response.reasoning_summary_part.added"
                        | "response.reasoning_summary_part.done"
                        | "response.output_item.added"
                        | "response.output_item.done"
                        | "response.function_call_arguments.delta"
                        | "response.function_call_arguments.done"
                        | "response.content_part.added"
                        | "response.content_part.done"
                        | "response.in_progress"
                        | "response.refusal.delta"
                        | "response.refusal.done",
                    ) if response_id.is_some() => {}
                    _ => return Err(transport_error()),
                }
            }
        })
        .await
        .is_ok_and(|r: Result<(), ServiceError>| r.is_ok())
    }
}
fn options(body: &Value) -> Value {
    let mut options = body.clone();
    options.as_object_mut().unwrap().remove("input");
    options
}
fn apply_previous(body: &mut Value, previous: Option<&Previous>) {
    let Some(previous) = previous else {
        return;
    };
    if options(body) != previous.options {
        return;
    }
    let Some(input) = body["input"].as_array() else {
        return;
    };
    if input.len() <= previous.input.len() || !input.starts_with(&previous.input) {
        return;
    }
    let remaining = &input[previous.input.len()..];
    if remaining.len() < previous.output.len()
        || !remaining
            .iter()
            .zip(&previous.output)
            .all(|(input, output)| matches_output(input, output))
    {
        return;
    }
    let input = remaining[previous.output.len()..].to_vec();
    body["input"] = json!(input);
    body["previous_response_id"] = json!(previous.id);
}
fn matches_output(input: &Value, output: &Value) -> bool {
    match output["type"].as_str() {
        Some("message") => {
            let expected_role = output["role"].as_str().unwrap_or("assistant");
            if input["role"].as_str() != Some(expected_role) {
                return false;
            }
            let text = |value: &Value, kind: &str| -> Option<String> {
                if let Some(text) = value.as_str() {
                    return Some(text.into());
                }
                let parts = value.as_array()?;
                parts
                    .iter()
                    .map(|part| {
                        if part["type"] == kind {
                            part["text"].as_str()
                        } else if kind == "output_text" && part["type"] == "refusal" {
                            part["refusal"].as_str()
                        } else {
                            None
                        }
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(|parts| parts.concat())
            };
            match (
                text(&input["content"], "input_text"),
                text(&output["content"], "output_text"),
            ) {
                (Some(a), Some(b)) => a == b,
                _ => false,
            }
        }
        Some("function_call") => {
            input["type"] == "function_call"
                && ["call_id", "name", "arguments"]
                    .iter()
                    .all(|key| input[key] == output[key])
        }
        Some("reasoning") => {
            input["type"] == "reasoning"
                && ["id", "summary", "encrypted_content"]
                    .iter()
                    .all(|key| input[key] == output[key])
        }
        _ => false,
    }
}

fn validate_explicit_identity(event: &Value, expected: Option<&str>) -> Result<(), ServiceError> {
    if let Some(id) = event.get("response_id") {
        if id.as_str().is_none() || id.as_str() != expected {
            return Err(ServiceError::Protocol(
                "WebSocket response id mismatch".into(),
            ));
        }
    }
    if let Some(response) = event.get("response") {
        if !response.is_object() {
            return Err(ServiceError::Protocol(
                "invalid WebSocket response object".into(),
            ));
        }
        if let Some(id) = response.get("id") {
            if id.as_str().is_none() || id.as_str() != expected {
                return Err(ServiceError::Protocol(
                    "WebSocket nested response id mismatch".into(),
                ));
            }
        }
    }
    Ok(())
}
