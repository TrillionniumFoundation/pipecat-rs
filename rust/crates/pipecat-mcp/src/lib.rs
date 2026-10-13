#![doc = include_str!("../README.md")]
//! Native MCP session ownership and LLM tool integration.
//!
//! Connections are explicit, tools are never called during discovery, and a
//! failed call is never automatically replayed. See the crate README for
//! transport, lifecycle, schema, and cleanup guarantees.
mod cleanup;
mod config;
mod error;
mod http_transport;
mod integration;
mod schema;
mod stdio;
pub use config::{HttpConfig, Limits, McpConfig, OutputFilter, ServerConfig, StdioConfig};
pub use error::McpError;
pub use integration::{McpLifecycle, McpToolExecutor, McpTools};
use pipecat_core::ToolDefinition;
pub use rmcp::model::ContentBlock;
use rmcp::{
    model::{
        CallToolRequest, CallToolRequestParams, CancelledNotificationParam, ClientCapabilities,
        ClientInfo, ClientRequest, Implementation, ListToolsRequest, PaginatedRequestParams,
        ProtocolVersion, ServerResult,
    },
    service::{Peer, PeerRequestOptions, RunningService},
    RoleClient, ServiceExt,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    sync::Arc,
};
use tokio::sync::{oneshot, Mutex, Semaphore};
pub use tokio_util::sync::CancellationToken;
use tokio_util::task::TaskTracker;

type Service = RunningService<RoleClient, ClientInfo>;
struct ActiveSession {
    service: Service,
    handle: Arc<SessionHandle>,
    cleanup: cleanup::CleanupStatus,
}

struct SessionHandle {
    generation: u64,
    peer: Peer<RoleClient>,
    cancelled: CancellationToken,
    requests: TaskTracker,
}
impl Drop for SessionHandle {
    fn drop(&mut self) {
        self.cancelled.cancel();
    }
}
#[derive(Default)]
struct State {
    requested: bool,
    generation: u64,
    active: Option<ActiveSession>,
    closing: Option<tokio::task::JoinHandle<Result<(), McpError>>>,
    schemas: BTreeMap<String, ToolDefinition>,
}
struct Inner {
    config: McpConfig,
    state: Mutex<State>,
    slots: Arc<Semaphore>,
}
/// Clones share one owned SDK session. Dropping all client and handler owners
/// signals SDK cleanup; await `close` for an observed cleanup result.
#[derive(Clone)]
pub struct McpClient {
    inner: Arc<Inner>,
}
impl fmt::Debug for McpClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("McpClient")
            .field("config", &self.inner.config)
            .finish_non_exhaustive()
    }
}
/// Full native result, including non-text content. Only text blocks contribute
/// to `text`. Debug deliberately hides tool output.
#[derive(Clone)]
pub struct ToolResponse {
    pub text: String,
    pub is_error: bool,
    pub content: Vec<ContentBlock>,
    pub structured_content: Option<Value>,
}
impl fmt::Debug for ToolResponse {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ToolResponse")
            .field("is_error", &self.is_error)
            .field("content_count", &self.content.len())
            .field("has_structured_content", &self.structured_content.is_some())
            .finish_non_exhaustive()
    }
}
impl McpClient {
    /// Validates configuration without connecting, discovering credentials, or
    /// spawning a process. No authentication flow is implemented.
    pub fn new(config: McpConfig) -> Result<Self, McpError> {
        config.validate()?;
        Ok(Self {
            inner: Arc::new(Inner {
                slots: Arc::new(Semaphore::new(config.limits.max_in_flight)),
                config,
                state: Mutex::new(State::default()),
            }),
        })
    }
    pub async fn start(&self) -> Result<(), McpError> {
        self.session(true).await.map(|_| ())
    }
    async fn session(&self, owner: bool) -> Result<Arc<SessionHandle>, McpError> {
        self.session_for_tool(owner, None).await
    }
    async fn session_for_tool(
        &self,
        owner: bool,
        name: Option<&str>,
    ) -> Result<Arc<SessionHandle>, McpError> {
        let mut state = self.inner.state.lock().await;
        Self::finish_close_locked(&mut state).await?;
        if owner {
            state.requested = true;
        }
        if !state.requested {
            return Err(McpError::NotConnected);
        }
        // Schema refresh or close/reopen can invalidate a name while a caller
        // waits for this lock. Admission and session selection must agree.
        if name.is_some_and(|name| {
            !self.inner.config.allowed(name) || !state.schemas.contains_key(name)
        }) {
            return Err(McpError::UnknownTool);
        }
        if let Some(active) = &state.active {
            return Ok(active.handle.clone());
        }
        let info = ClientInfo::new(
            ClientCapabilities::default(),
            Implementation::new("pipecat-rust", env!("CARGO_PKG_VERSION")),
        )
        .with_protocol_version(ProtocolVersion::V_2025_11_25);
        let config = &self.inner.config;
        let cleanup = cleanup::CleanupStatus::default();
        let service = tokio::time::timeout(config.limits.connect_timeout, async {
            match &config.server {
                ServerConfig::Stdio(stdio) => info
                    .serve(stdio::NativeStdio::spawn(
                        stdio,
                        &config.limits,
                        cleanup.clone(),
                    )?)
                    .await
                    .map_err(|_| McpError::Initialization),
                ServerConfig::StreamableHttp(http) => info
                    .serve(http_transport::transport(
                        http,
                        &config.limits,
                        cleanup.clone(),
                    )?)
                    .await
                    .map_err(|_| McpError::Initialization),
            }
        })
        .await
        .map_err(|_| McpError::Timeout)??;
        if !service.peer_info().is_some_and(|info| {
            info.capabilities.tools.is_some()
                && ProtocolVersion::known_up_to(&ProtocolVersion::V_2025_11_25)
                    .contains(&info.protocol_version)
        }) {
            // The local service is dropped here, cancelling its owned transport.
            return Err(McpError::Protocol);
        }
        state.generation = state
            .generation
            .checked_add(1)
            .ok_or(McpError::Limit("session generations"))?;
        let handle = Arc::new(SessionHandle {
            generation: state.generation,
            peer: service.peer().clone(),
            cancelled: CancellationToken::new(),
            requests: TaskTracker::new(),
        });
        state.active = Some(ActiveSession {
            service,
            handle: handle.clone(),
            cleanup,
        });
        Ok(handle)
    }
    /// Idempotent and task-independent. A call cannot reopen an explicitly
    /// closed client; a subsequent explicit `start` or `tools` may do so.
    pub async fn close(&self) -> Result<(), McpError> {
        let mut state = self.inner.state.lock().await;
        state.requested = false;
        state.schemas.clear();
        self.close_locked(&mut state).await
    }
    async fn close_locked(&self, state: &mut State) -> Result<(), McpError> {
        if let Some(mut active) = state.active.take() {
            active.handle.cancelled.cancel();
            active.handle.requests.close();
            let timeout = self.inner.config.limits.close_timeout;
            // Keep the join owner in shared state: cancelling a close caller
            // must not detach teardown or let a replacement session overtake it.
            state.closing = Some(tokio::spawn(async move {
                tokio::time::timeout(timeout, async {
                    active
                        .service
                        .close()
                        .await
                        .map_err(|_| McpError::Cleanup)?;
                    active.handle.requests.wait().await;
                    if active.cleanup.failed() {
                        return Err(McpError::Cleanup);
                    }
                    Ok(())
                })
                .await
                .map_err(|_| McpError::Cleanup)?
            }));
        }
        Self::finish_close_locked(state).await
    }
    async fn finish_close_locked(state: &mut State) -> Result<(), McpError> {
        if let Some(closing) = &mut state.closing {
            let result = closing.await.unwrap_or(Err(McpError::Cleanup));
            state.closing = None;
            result
        } else {
            Ok(())
        }
    }
    async fn drop_failed(&self, session: &SessionHandle) {
        let mut state = self.inner.state.lock().await;
        if state
            .active
            .as_ref()
            .is_some_and(|active| active.handle.generation == session.generation)
        {
            // Failure of cleanup must not hide the original call failure.
            let _ = self.close_locked(&mut state).await;
        }
    }
    /// Discovers every bounded page and attaches executable native handlers.
    /// Publication is atomic: failed discovery never exposes a partial list.
    pub async fn tools(&self) -> Result<McpTools, McpError> {
        let session = self.session(true).await?;
        let mut cursor = None;
        let mut seen_cursors = BTreeSet::new();
        let mut schemas = vec![];
        let mut tool_count = 0usize;
        let mut schema_bytes = 0usize;
        let mut complete = false;
        for _ in 0..self.inner.config.limits.max_pages {
            let params = PaginatedRequestParams::default().with_cursor(cursor.take());
            let result = self
                .request(
                    session.clone(),
                    ClientRequest::ListToolsRequest(ListToolsRequest::with_param(params)),
                    CancellationToken::new(),
                )
                .await;
            let result = self.handle_failure(&session, result).await?;
            let ServerResult::ListToolsResult(page) = result else {
                return Err(McpError::Protocol);
            };
            tool_count = tool_count.saturating_add(page.tools.len());
            if tool_count > self.inner.config.limits.max_tools {
                return Err(McpError::Limit("discovered tools"));
            }
            for tool in page.tools {
                schema_bytes = schema_bytes.saturating_add(
                    serde_json::to_vec(&tool)
                        .map_err(|_| McpError::Protocol)?
                        .len(),
                );
                if schema_bytes > self.inner.config.limits.max_schema_bytes {
                    return Err(McpError::Limit("tool schemas"));
                }
                if self.inner.config.allowed(&tool.name) {
                    schemas.push(schema::convert(tool, &self.inner.config)?);
                }
            }
            cursor = page.next_cursor;
            if let Some(cursor) = &cursor {
                if !seen_cursors.insert(cursor.clone()) {
                    return Err(McpError::Protocol);
                }
            } else {
                complete = true;
                break;
            }
        }
        if !complete {
            return Err(McpError::Limit("discovery pages"));
        }
        let indexed = schema::index(&schemas)?;
        let mut state = self.inner.state.lock().await;
        if !state
            .active
            .as_ref()
            .is_some_and(|active| active.handle.generation == session.generation)
            || !state.requested
        {
            return Err(McpError::NotConnected);
        }
        state.schemas = indexed;
        Ok(McpTools::new(self.clone(), schemas))
    }
    /// Dispatches at most one tools/call. No automatic replay, elicitation,
    /// sampling, nested tool execution, resource reads, or OAuth is performed.
    pub async fn call(&self, name: &str, arguments: Value) -> Result<ToolResponse, McpError> {
        self.call_with_cancel(name, arguments, CancellationToken::new())
            .await
    }
    pub async fn call_with_cancel(
        &self,
        name: &str,
        arguments: Value,
        cancel: CancellationToken,
    ) -> Result<ToolResponse, McpError> {
        let mut result = self.call_unfiltered(name, arguments, cancel).await?;
        result.text = self.filter_text(name, result.text)?;
        Ok(result)
    }
    async fn call_unfiltered(
        &self,
        name: &str,
        arguments: Value,
        cancel: CancellationToken,
    ) -> Result<ToolResponse, McpError> {
        if cancel.is_cancelled() {
            return Err(McpError::Cancelled);
        }
        {
            let state = tokio::select! { biased;
                _ = cancel.cancelled() => return Err(McpError::Cancelled),
                state = self.inner.state.lock() => state,
            };
            if !state.requested {
                return Err(McpError::NotConnected);
            }
            if !self.inner.config.allowed(name) || !state.schemas.contains_key(name) {
                return Err(McpError::UnknownTool);
            }
        }
        let mut args = match arguments {
            Value::Null => Default::default(),
            Value::Object(args) => args,
            _ => return Err(McpError::InvalidArguments),
        };
        if let Some(fixed) = self.inner.config.tools_arguments.get(name) {
            args.extend(fixed.clone());
        }
        if serde_json::to_vec(&args)
            .map_err(|_| McpError::InvalidArguments)?
            .len()
            > self.inner.config.limits.max_argument_bytes
        {
            return Err(McpError::Limit("tool arguments"));
        }
        let session = tokio::select! { biased; _ = cancel.cancelled() => return Err(McpError::Cancelled), result = self.session_for_tool(false, Some(name)) => result? };
        let request =
            CallToolRequest::new(CallToolRequestParams::new(name.to_owned()).with_arguments(args));
        let result = self
            .request(
                session.clone(),
                ClientRequest::CallToolRequest(request),
                cancel,
            )
            .await;
        let result = self.handle_failure(&session, result).await?;
        let ServerResult::CallToolResult(result) = result else {
            return Err(McpError::Protocol);
        };
        if serde_json::to_vec(&result)
            .map_err(|_| McpError::Protocol)?
            .len()
            > self.inner.config.limits.max_output_bytes
        {
            return Err(McpError::Limit("tool output"));
        }
        let mut text = String::new();
        for content in &result.content {
            if let ContentBlock::Text(block) = content {
                text.push_str(&block.text);
            }
        }
        Ok(ToolResponse {
            text,
            is_error: result.is_error.unwrap_or(false),
            content: result.content,
            structured_content: result.structured_content,
        })
    }
    fn filter_text(&self, name: &str, mut text: String) -> Result<String, McpError> {
        if let Some(filter) = self.inner.config.tools_output_filters.get(name) {
            text = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| filter(text)))
                .map_err(|_| McpError::OutputFilter)?
                .map_err(|_| McpError::OutputFilter)?;
            if text.len() > self.inner.config.limits.max_output_bytes {
                return Err(McpError::Limit("filtered tool output"));
            }
        }
        Ok(text)
    }
    /// Source-compatible model-facing text behavior: join text blocks, apply
    /// output filters, and use a fallback for empty/non-text results. Local
    /// cancellation propagates without running filters or becoming tool text.
    /// Closed clients still return a wiring error instead of reconnecting.
    pub async fn call_text(&self, name: &str, arguments: Value) -> Result<String, McpError> {
        let (text, error) = match self
            .call_unfiltered(name, arguments, CancellationToken::new())
            .await
        {
            Ok(result) => (result.text, None),
            Err(error @ (McpError::NotConnected | McpError::Cancelled)) => return Err(error),
            Err(error) => (String::new(), Some(error)),
        };
        // The source passes empty text to filters after call failures, allowing
        // an application-supplied fallback without exposing remote error data.
        match self.filter_text(name, text) {
            Ok(text) if !text.is_empty() => Ok(text),
            _ => Ok(error.map_or_else(
                || "Sorry, could not call the mcp tool".into(),
                |error| format!("Error calling mcp tool: {error}"),
            )),
        }
    }
    async fn handle_failure(
        &self,
        session: &SessionHandle,
        result: Result<ServerResult, McpError>,
    ) -> Result<ServerResult, McpError> {
        if matches!(result, Err(McpError::Transport)) {
            self.drop_failed(session).await;
        }
        result
    }
    async fn request(
        &self,
        session: Arc<SessionHandle>,
        request: ClientRequest,
        cancel: CancellationToken,
    ) -> Result<ServerResult, McpError> {
        let permit = self
            .inner
            .slots
            .clone()
            .try_acquire_owned()
            .map_err(|_| McpError::Limit("in-flight requests"))?;
        let timeout = self.inner.config.limits.request_timeout;
        let close_timeout = self.inner.config.limits.close_timeout;
        let (mut sender, receiver) = oneshot::channel();
        let tracker = session.requests.clone();
        tracker.spawn(async move {
            let _permit = permit;
            let request = tokio::select! { biased;
                _ = sender.closed() => return,
                _ = session.cancelled.cancelled() => { let _ = sender.send(Err(McpError::Cancelled)); return; },
                _ = cancel.cancelled() => { let _ = sender.send(Err(McpError::Cancelled)); return; },
                result = session.peer.send_request_with_option(request, PeerRequestOptions::no_options()) => result,
            };
            let handle = match request { Ok(handle) => handle, Err(error) => { let _ = sender.send(Err(McpError::from_service(error))); return; } };
            let id = handle.id.clone();
            let mut cancelled = false;
            let result = tokio::select! { biased;
                _ = sender.closed() => { cancelled = true; Err(McpError::Cancelled) },
                _ = session.cancelled.cancelled() => { cancelled = true; Err(McpError::Cancelled) },
                _ = cancel.cancelled() => { cancelled = true; Err(McpError::Cancelled) },
                _ = tokio::time::sleep(timeout) => { cancelled = true; Err(McpError::Timeout) },
                result = handle.await_response() => result.map_err(McpError::from_service),
            };
            if cancelled {
                let notification = CancelledNotificationParam::new(Some(id), Some("local invocation ended".into()));
                let _ = tokio::time::timeout(close_timeout, session.peer.notify_cancelled(notification)).await;
            }
            let _ = sender.send(result);
        });
        receiver.await.unwrap_or(Err(McpError::Transport))
    }
}

#[cfg(test)]
#[path = "../tests/support/mod.rs"]
mod test_support;

#[cfg(test)]
mod lifecycle_tests {
    use super::*;

    #[tokio::test]
    async fn call_validated_before_close_cannot_use_undiscovered_replacement_session() {
        let server = test_support::Server::start("").await;
        let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
            HttpConfig::new(&server.url),
        )))
        .unwrap();
        client.tools().await.unwrap();
        let guard = client.inner.state.lock().await;
        let mut call = Box::pin(client.call("other", Value::Null));
        let mut closing = Box::pin(client.close());
        let mut starting = Box::pin(client.start());
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        assert!(futures_util::poll!(closing.as_mut()).is_pending());
        assert!(futures_util::poll!(starting.as_mut()).is_pending());
        drop(guard);
        // The call's initial allowlist check runs before close, but its session
        // acquisition queues behind close and the explicit replacement start.
        assert!(futures_util::poll!(call.as_mut()).is_pending());
        let (result, closed, started) = tokio::join!(call, closing, starting);
        closed.unwrap();
        started.unwrap();
        assert_eq!(result.unwrap_err(), McpError::UnknownTool);
        assert!(server.state.lock().unwrap().calls.is_empty());
        client.close().await.unwrap();
        server.wait_for(|state| state.deletes == 2).await;
    }
}
