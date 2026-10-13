use crate::{cleanup::CleanupStatus, config::HttpConfig, http_transport::events, Limits, McpError};
use futures_util::{stream::BoxStream, StreamExt};
use http::{HeaderMap, HeaderName, HeaderValue};
use rmcp::{
    model::{ClientJsonRpcMessage, ServerJsonRpcMessage},
    transport::{streamable_http_client::SseError, Transport},
    RoleClient,
};
use sse_stream::Sse;
use std::{future::Future, io, sync::Arc, sync::Mutex, time::Duration};
use tokio::{
    sync::{mpsc, oneshot, Semaphore},
    task::JoinHandle,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

type EventStream = BoxStream<'static, Result<Sse, SseError>>;

fn failed(message: &'static str) -> io::Error {
    io::Error::other(message)
}

/// One owned GET stream and bounded POST tasks. The endpoint is fixed before
/// any MCP message or application header can be sent to it.
pub(crate) struct NativeLegacySse {
    posts: Arc<Posts>,
    messages: mpsc::Receiver<ServerJsonRpcMessage>,
    reader: Option<JoinHandle<()>>,
    close_timeout: Duration,
    cleanup: CleanupStatus,
}

struct Posts {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    cancelled: CancellationToken,
    requests: TaskTracker,
    slots: Arc<Semaphore>,
    // Spawn and close are serialized here; no guard crosses an await.
    closed: Mutex<bool>,
    timeout: Duration,
    max_bytes: usize,
}
impl Posts {
    fn stop(&self) {
        let mut closed = self
            .closed
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        *closed = true;
        self.cancelled.cancel();
        self.requests.close();
    }
}

pub(crate) async fn transport(
    config: &HttpConfig,
    sse_read_timeout: Duration,
    limits: &Limits,
    cleanup: CleanupStatus,
) -> Result<NativeLegacySse, McpError> {
    let origin =
        reqwest::Url::parse(&config.url).map_err(|_| McpError::Config("invalid MCP URL"))?;
    let mut headers = HeaderMap::new();
    for (name, value) in &config.headers {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| McpError::Config("invalid MCP header"))?;
        let mut value =
            HeaderValue::from_str(value).map_err(|_| McpError::Config("invalid MCP header"))?;
        value.set_sensitive(true);
        headers.insert(name, value);
    }
    let builder = || {
        reqwest::Client::builder()
            .default_headers(headers.clone())
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .no_proxy()
            .connect_timeout(limits.connect_timeout)
    };
    // The GET uses raw chunk idle time, including keep-alives, while POST uses
    // its independent request budget. A GET total deadline would expire a live
    // stream; a shared client idle deadline would prematurely end legal POSTs.
    let get_client = builder()
        .read_timeout(sse_read_timeout)
        .build()
        .map_err(|_| McpError::Initialization)?;
    let client = builder()
        .read_timeout(limits.request_timeout)
        .build()
        .map_err(|_| McpError::Initialization)?;
    // Before endpoint admission there are no detached tasks. Cancellation of
    // this initializer drops the response and its connection directly.
    let (stream, endpoint) = tokio::time::timeout(limits.connect_timeout, async {
        let response = get_client
            .get(origin.clone())
            .header("accept", "text/event-stream")
            .send()
            .await
            .map_err(|_| McpError::Initialization)?;
        let event_stream = response
            .headers()
            .get("content-type")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .is_some_and(|value| value.trim().eq_ignore_ascii_case("text/event-stream"));
        if !response.status().is_success() || !event_stream {
            return Err(McpError::Protocol);
        }
        let mut stream = events(response, limits.max_wire_bytes);
        while let Some(event) = stream.next().await {
            let event = event.map_err(|_| McpError::Protocol)?;
            match event.event.as_deref() {
                Some("endpoint") => {
                    let endpoint = endpoint(&origin, event.data.as_deref())?;
                    return Ok((stream, endpoint));
                }
                // A response cannot precede our first initialized request.
                None | Some("") | Some("message")
                    if event.data.as_deref().is_some_and(|data| !data.is_empty()) =>
                {
                    return Err(McpError::Protocol);
                }
                _ => {}
            }
        }
        Err(McpError::Transport)
    })
    .await
    .map_err(|_| McpError::Timeout)??;
    let posts = Arc::new(Posts {
        client,
        endpoint: endpoint.clone(),
        cancelled: CancellationToken::new(),
        requests: TaskTracker::new(),
        // Keep one bounded slot for initialized/cancellation control messages.
        slots: Arc::new(Semaphore::new(limits.max_in_flight + 1)),
        closed: Mutex::new(false),
        timeout: limits.request_timeout,
        max_bytes: limits.max_wire_bytes,
    });
    let (sender, messages) = mpsc::channel(limits.max_in_flight);
    let cancelled = posts.cancelled.clone();
    let reader = tokio::spawn(read_messages(stream, sender, origin, endpoint, cancelled));
    Ok(NativeLegacySse {
        posts,
        messages,
        reader: Some(reader),
        close_timeout: limits.close_timeout,
        cleanup,
    })
}

fn endpoint(origin: &reqwest::Url, data: Option<&str>) -> Result<reqwest::Url, McpError> {
    let data = data
        .filter(|value| !value.trim().is_empty())
        .ok_or(McpError::Protocol)?;
    let endpoint = origin.join(data).map_err(|_| McpError::Protocol)?;
    if endpoint.origin() != origin.origin()
        || !endpoint.username().is_empty()
        || endpoint.password().is_some()
        || endpoint.fragment().is_some()
    {
        return Err(McpError::Protocol);
    }
    Ok(endpoint)
}

async fn read_messages(
    mut stream: EventStream,
    sender: mpsc::Sender<ServerJsonRpcMessage>,
    origin: reqwest::Url,
    fixed_endpoint: reqwest::Url,
    cancelled: CancellationToken,
) {
    loop {
        let event = tokio::select! { biased;
            _ = cancelled.cancelled() => break,
            event = stream.next() => event,
        };
        let Some(Ok(event)) = event else {
            break;
        };
        match event.event.as_deref() {
            Some("endpoint") => {
                // Never reroute an admitted connection or replay to a new URL.
                if endpoint(&origin, event.data.as_deref()).as_ref() != Ok(&fixed_endpoint) {
                    break;
                }
            }
            None | Some("") | Some("message") => {
                let Some(data) = event.data.filter(|data| !data.is_empty()) else {
                    continue;
                };
                let Ok(message) = serde_json::from_str(&data) else {
                    break;
                };
                let sent = tokio::select! { biased;
                    _ = cancelled.cancelled() => break,
                    sent = sender.send(message) => sent,
                };
                if sent.is_err() {
                    break;
                }
            }
            _ => {}
        }
    }
    // EOF, invalid data, and network failures close the SDK receive channel.
    // They also end pending writes; no reconnect or event resume occurs here.
    cancelled.cancel();
}

async fn post(
    client: reqwest::Client,
    endpoint: reqwest::Url,
    bytes: Vec<u8>,
    timeout: Duration,
    max_bytes: usize,
) -> Result<(), io::Error> {
    let response = client
        .post(endpoint)
        .header("content-type", "application/json")
        .timeout(timeout)
        .body(bytes)
        .send()
        .await
        .map_err(|_| failed("MCP legacy SSE POST failed"))?;
    if !response.status().is_success() {
        return Err(failed("MCP legacy SSE POST was not accepted"));
    }
    if response
        .content_length()
        .is_some_and(|length| length > max_bytes as u64)
    {
        return Err(failed("MCP legacy SSE POST response exceeds limit"));
    }
    let mut size = 0usize;
    let mut body = response.bytes_stream();
    while let Some(chunk) = body.next().await {
        let chunk = chunk.map_err(|_| failed("MCP legacy SSE POST response failed"))?;
        if chunk.len() > max_bytes.saturating_sub(size) {
            return Err(failed("MCP legacy SSE POST response exceeds limit"));
        }
        size += chunk.len();
    }
    // A POST body is only an HTTP acknowledgment. JSON-RPC responses must
    // arrive on the GET event stream and are never synthesized from this body.
    Ok(())
}

impl Transport<RoleClient> for NativeLegacySse {
    type Error = io::Error;
    fn send(
        &mut self,
        message: ClientJsonRpcMessage,
    ) -> impl Future<Output = Result<(), io::Error>> + Send + 'static {
        let posts = self.posts.clone();
        async move {
            let bytes =
                serde_json::to_vec(&message).map_err(|_| failed("invalid MCP outbound message"))?;
            if bytes.len() > posts.max_bytes {
                return Err(failed("MCP outbound message exceeds limit"));
            }
            let receiver = {
                let closed = posts
                    .closed
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                if *closed || posts.cancelled.is_cancelled() {
                    return Err(failed("MCP legacy SSE transport closed"));
                }
                let permit = posts
                    .slots
                    .clone()
                    .try_acquire_owned()
                    .map_err(|_| failed("MCP legacy SSE write capacity exceeded"))?;
                let client = posts.client.clone();
                let endpoint = posts.endpoint.clone();
                let cancelled = posts.cancelled.clone();
                let timeout = posts.timeout;
                let max_bytes = posts.max_bytes;
                let (mut sender, receiver) = oneshot::channel();
                posts.requests.spawn(async move {
                    let _permit = permit;
                    let result = tokio::select! { biased;
                        _ = sender.closed() => return,
                        _ = cancelled.cancelled() => Err(failed("MCP legacy SSE transport closed")),
                        result = tokio::time::timeout(timeout, post(client, endpoint, bytes, timeout, max_bytes)) => {
                            result.unwrap_or_else(|_| Err(failed("MCP legacy SSE POST timed out")))
                        },
                    };
                    if result.is_err() {
                        cancelled.cancel();
                    }
                    let _ = sender.send(result);
                });
                receiver
            };
            receiver
                .await
                .unwrap_or_else(|_| Err(failed("MCP legacy SSE writer stopped")))
        }
    }
    async fn receive(&mut self) -> Option<ServerJsonRpcMessage> {
        self.messages.recv().await
    }
    async fn close(&mut self) -> Result<(), io::Error> {
        let attempt = self.cleanup.begin();
        self.posts.stop();
        tokio::time::timeout(self.close_timeout, async {
            if let Some(reader) = &mut self.reader {
                reader
                    .await
                    .map_err(|_| failed("MCP legacy SSE reader cleanup failed"))?;
            }
            self.reader = None;
            self.posts.requests.wait().await;
            Ok::<_, io::Error>(())
        })
        .await
        .map_err(|_| failed("MCP legacy SSE cleanup timed out"))??;
        attempt.success();
        Ok(())
    }
}
impl Drop for NativeLegacySse {
    fn drop(&mut self) {
        self.posts.stop();
        if let Some(reader) = &self.reader {
            reader.abort();
        }
    }
}
