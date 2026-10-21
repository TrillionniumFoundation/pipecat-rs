//! Bounded, cancellation-aware WebSocket sessions for native Pipecat transports.
//! `RtviClientProcessor` adds an explicit, single-run RTVI text/LLM pipeline
//! boundary. See `RTVI_CLIENT.md` for its IPv4, framing and lifecycle profile.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
mod fenced_send;
pub mod rtvi_client;
pub use rtvi_client::{RtviClientConfig, RtviClientControl, RtviClientProcessor};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio_tungstenite::{
    tungstenite::{protocol::WebSocketConfig, Message},
    WebSocketStream,
};

/// Browser origin checking is not client authentication. A non-browser client can forge Origin.
#[derive(Debug, Clone, Copy)]
pub enum OriginPolicy {
    LoopbackOnlyAllowMissing,
}
impl OriginPolicy {
    fn accepts(self, headers: &tokio_tungstenite::tungstenite::http::HeaderMap) -> bool {
        let mut origins = headers.get_all("origin").iter();
        let Some(origin) = origins.next() else {
            return true;
        };
        if origins.next().is_some() {
            return false;
        }
        let Ok(origin) = origin.to_str() else {
            return false;
        };
        let Ok(url) = url::Url::parse(origin) else {
            return false;
        };
        if !matches!(url.scheme(), "http" | "https")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
            || url.path() != "/"
        {
            return false;
        }
        url.host_str().is_some_and(|host| {
            host == "localhost"
                || host
                    .trim_matches(['[', ']'])
                    .parse::<std::net::IpAddr>()
                    .is_ok_and(|ip| ip.is_loopback())
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Text(String),
    Binary(Bytes),
}
#[derive(Debug, Clone)]
pub struct SessionConfig {
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub connect_timeout: Duration,
    pub write_timeout: Duration,
    pub close_timeout: Duration,
}
impl Default for SessionConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_message_bytes: 1024 * 1024,
            connect_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(10),
            close_timeout: Duration::from_secs(2),
        }
    }
}
impl SessionConfig {
    /// Validated framing and finite automatic-control/write-buffer policy for
    /// an externally authenticated upgrade that will be adopted by a session.
    pub fn websocket_config(&self) -> Result<WebSocketConfig, TransportError> {
        validate(self)?;
        Ok(websocket_config(self))
    }
}
#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("invalid transport configuration")]
    InvalidConfig,
    #[error("WebSocket: {0}")]
    WebSocket(#[from] tokio_tungstenite::tungstenite::Error),
    #[error("{0} timed out")]
    Timeout(&'static str),
    #[error("session closed")]
    Closed,
    #[error("message exceeds size limit")]
    TooLarge,
    #[error("inbound queue is full")]
    Backpressure,
    #[error("session task: {0}")]
    Join(#[from] tokio::task::JoinError),
}
enum Outbound {
    Packet {
        packet: Packet,
        fence: Option<pipecat_core::InvocationFence>,
    },
    Flush(tokio::sync::oneshot::Sender<()>),
}
#[derive(Clone)]
pub struct SessionSender {
    tx: mpsc::Sender<Outbound>,
    max: usize,
}
impl SessionSender {
    pub async fn send(&self, packet: Packet) -> Result<(), TransportError> {
        self.enqueue(packet, None).await
    }
    /// Enqueue under the original pipeline invocation. Stale items are dropped
    /// before socket admission, including while waiting for sink readiness.
    /// Already admitted bytes are flushed once and cannot be retracted. This
    /// reports queue acceptance, not socket delivery or audible playback.
    pub async fn send_fenced(
        &self,
        packet: Packet,
        fence: pipecat_core::InvocationFence,
    ) -> Result<(), TransportError> {
        self.enqueue(packet, Some(fence)).await
    }
    /// Ordered socket-flush barrier, including queue admission, within five
    /// seconds. This does not acknowledge remote receipt.
    /// Dropping the waiter does not cancel or replay already admitted writes.
    pub async fn flush(&self) -> Result<(), TransportError> {
        tokio::time::timeout(Duration::from_secs(5), async {
            let (reply, wait) = tokio::sync::oneshot::channel();
            self.tx
                .send(Outbound::Flush(reply))
                .await
                .map_err(|_| TransportError::Closed)?;
            wait.await.map_err(|_| TransportError::Closed)
        })
        .await
        .map_err(|_| TransportError::Timeout("flush barrier"))?
    }
    async fn enqueue(
        &self,
        packet: Packet,
        fence: Option<pipecat_core::InvocationFence>,
    ) -> Result<(), TransportError> {
        let size = match &packet {
            Packet::Text(s) => s.len(),
            Packet::Binary(b) => b.len(),
        };
        if size > self.max {
            return Err(TransportError::TooLarge);
        }
        self.tx
            .send(Outbound::Packet { packet, fence })
            .await
            .map_err(|_| TransportError::Closed)
    }
}
pub struct WebSocketSession {
    sender: Option<SessionSender>,
    incoming: mpsc::Receiver<Packet>,
    cancel: watch::Sender<u8>,
    task: Option<JoinHandle<Result<(), TransportError>>>,
}
impl WebSocketSession {
    pub async fn connect(url: &str, config: SessionConfig) -> Result<Self, TransportError> {
        validate(&config)?;
        let ws_config = websocket_config(&config);
        let (stream, _) = tokio::time::timeout(
            config.connect_timeout,
            tokio_tungstenite::connect_async_with_config(url, Some(ws_config), false),
        )
        .await
        .map_err(|_| TransportError::Timeout("connect"))??;
        Self::from_stream(stream, config)
    }
    /// Connect with a caller-built upgrade request, preserving bounded transport
    /// limits and the handshake deadline. Callers must protect credentials with
    /// TLS and validate any custom application headers before constructing it.
    pub async fn connect_with_request(
        request: tokio_tungstenite::tungstenite::handshake::client::Request,
        config: SessionConfig,
    ) -> Result<Self, TransportError> {
        validate(&config)?;
        let (stream, _) = tokio::time::timeout(
            config.connect_timeout,
            tokio_tungstenite::connect_async_with_config(
                request,
                Some(websocket_config(&config)),
                false,
            ),
        )
        .await
        .map_err(|_| TransportError::Timeout("connect"))??;
        Self::from_stream(stream, config)
    }
    pub async fn accept(stream: TcpStream, config: SessionConfig) -> Result<Self, TransportError> {
        validate(&config)?;
        let ws_config = websocket_config(&config);
        let stream = tokio::time::timeout(
            config.connect_timeout,
            tokio_tungstenite::accept_async_with_config(stream, Some(ws_config)),
        )
        .await
        .map_err(|_| TransportError::Timeout("accept"))??;
        Self::from_stream(stream, config)
    }
    /// Apply an application-selected Origin check before completing a server handshake.
    pub async fn accept_with_origin_policy(
        stream: TcpStream,
        config: SessionConfig,
        policy: OriginPolicy,
    ) -> Result<Self, TransportError> {
        validate(&config)?;
        let ws_config = websocket_config(&config);
        // Tungstenite requires its unboxed HTTP ErrorResponse in this callback.
        #[allow(clippy::result_large_err)]
        let callback = move |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if policy.accepts(request.headers()) { Ok(response) }
            else {
                let mut denied = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some("WebSocket origin rejected".into()));
                *denied.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
                Err(denied)
            }
        };
        let stream = tokio::time::timeout(
            config.connect_timeout,
            tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(ws_config)),
        )
        .await
        .map_err(|_| TransportError::Timeout("accept"))??;
        Self::from_stream(stream, config)
    }
    /// Validate the complete upgrade request before accepting a bounded session.
    /// The policy can enforce exact Origin, path, and ephemeral session tokens.
    /// Frame/message limits are always supplied by this transport, not callers.
    pub async fn accept_with_request_policy<F>(
        stream: TcpStream,
        config: SessionConfig,
        policy: F,
    ) -> Result<Self, TransportError>
    where
        F: FnOnce(&tokio_tungstenite::tungstenite::handshake::server::Request) -> bool
            + Send
            + Unpin,
    {
        validate(&config)?;
        let ws_config = websocket_config(&config);
        #[allow(clippy::result_large_err)]
        let callback = move |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response: tokio_tungstenite::tungstenite::handshake::server::Response| {
            if policy(request) { Ok(response) } else {
                let mut denied = tokio_tungstenite::tungstenite::handshake::server::ErrorResponse::new(Some("WebSocket request rejected".into()));
                *denied.status_mut() = tokio_tungstenite::tungstenite::http::StatusCode::FORBIDDEN;
                Err(denied)
            }
        };
        let stream = tokio::time::timeout(
            config.connect_timeout,
            tokio_tungstenite::accept_hdr_async_with_config(stream, callback, Some(ws_config)),
        )
        .await
        .map_err(|_| TransportError::Timeout("accept"))??;
        Self::from_stream(stream, config)
    }
    /// Adopt an already authenticated WebSocket upgrade. The caller owns HTTP
    /// authentication; this session owns bounded I/O and joined cleanup.
    /// Build the supplied stream with `SessionConfig::websocket_config`.
    /// Frame/message and write-buffer limits larger than that policy are rejected.
    pub fn from_websocket<S>(
        stream: WebSocketStream<S>,
        config: SessionConfig,
    ) -> Result<Self, TransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let expected = config.websocket_config()?;
        let limits = stream.get_config();
        if limits
            .max_message_size
            .is_none_or(|max| max > config.max_message_bytes)
            || limits
                .max_frame_size
                .is_none_or(|max| max > config.max_message_bytes)
            || limits.write_buffer_size > expected.write_buffer_size
            || limits.max_write_buffer_size > expected.max_write_buffer_size
        {
            return Err(TransportError::InvalidConfig);
        }
        Self::from_stream(stream, config)
    }

    fn from_stream<S>(
        stream: WebSocketStream<S>,
        config: SessionConfig,
    ) -> Result<Self, TransportError>
    where
        S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
    {
        let (out_tx, mut out_rx) = mpsc::channel::<Outbound>(config.queue_capacity);
        let (in_tx, in_rx) = mpsc::channel(config.queue_capacity);
        let (cancel, mut cancelled) = watch::channel(0u8);
        let task = tokio::spawn(async move {
            let (mut sink, mut source) = stream.split();
            let (peer_closed_tx, mut peer_closed) = watch::channel(false);
            let flush = std::sync::Arc::new(tokio::sync::Notify::new());
            let reader_flush = flush.clone();
            let reader = async move {
                while let Some(message) = source.next().await {
                    let packet = match message? {
                        Message::Text(text) => Packet::Text(text.to_string()),
                        Message::Binary(bytes) => Packet::Binary(bytes),
                        Message::Close(_) => {
                            let _ = peer_closed_tx.send(true);
                            return Ok::<(), TransportError>(());
                        }
                        Message::Ping(_) => {
                            reader_flush.notify_one();
                            continue;
                        }
                        _ => continue,
                    };
                    in_tx
                        .send(packet)
                        .await
                        .map_err(|_| TransportError::Closed)?;
                }
                let _ = peer_closed_tx.send(true);
                Ok(())
            };
            let writer = async move {
                loop {
                    if *cancelled.borrow() == 1 {
                        out_rx.close();
                    }
                    if *peer_closed.borrow() {
                        out_rx.close();
                        let pending = out_rx.capacity() < out_rx.max_capacity();
                        return flush_peer_close(
                            &mut sink,
                            &mut cancelled,
                            config.close_timeout,
                            pending,
                        )
                        .await;
                    }
                    tokio::select! {
                        _ = cancelled.changed() => {
                            if *cancelled.borrow() == 1 { out_rx.close(); continue; }
                            let _ = tokio::time::timeout(config.close_timeout, sink.close()).await;
                            return Ok(());
                        }
                        _ = peer_closed.changed() => { continue; }
                        _ = flush.notified() => {
                            tokio::select! {
                                biased;
                                _ = wait_for_cancel(&mut cancelled) => return Ok(()),
                                _ = peer_closed.changed() => {
                                    out_rx.close();
                                    let pending = out_rx.capacity() < out_rx.max_capacity();
                                    return flush_peer_close(&mut sink, &mut cancelled, config.close_timeout, pending).await;
                                },
                                result = tokio::time::timeout(config.write_timeout, sink.flush()) => {
                                    result.map_err(|_|TransportError::Timeout("pong"))??;
                                }
                            }
                        }
                        packet = out_rx.recv() => {
                            let Some(packet) = packet else {
                                tokio::time::timeout(config.close_timeout,sink.close()).await.map_err(|_|TransportError::Timeout("close"))??;
                                return Ok(());
                            };
                            let (packet, fence) = match packet {
                                Outbound::Flush(reply) => {
                                    tokio::select! {
                                        biased;
                                        _ = wait_for_cancel(&mut cancelled) => return Ok(()),
                                        _ = peer_closed.changed() => { return Err(TransportError::Closed); },
                                        result = tokio::time::timeout(config.write_timeout, sink.flush()) => { result.map_err(|_| TransportError::Timeout("flush barrier"))??; }
                                    }
                                    let _ = reply.send(());
                                    continue;
                                }
                                Outbound::Packet { packet, fence } => (packet, fence),
                            };
                            let message = match packet { Packet::Text(s)=>Message::Text(s.into()), Packet::Binary(b)=>Message::Binary(b) };
                            tokio::select! {
                                biased;
                                _ = wait_for_cancel(&mut cancelled) => return Ok(()),
                                _ = peer_closed.changed() => {
                                    out_rx.close();
                                    return flush_peer_close(&mut sink, &mut cancelled, config.close_timeout, true).await;
                                },
                                result = tokio::time::timeout(config.write_timeout,fenced_send::send(&mut sink, message, fence.as_ref())) => { result.map_err(|_|TransportError::Timeout("write"))??; }
                            }
                        }
                    }
                }
            };
            tokio::pin!(reader, writer);
            tokio::select! {
                result = &mut writer => result,
                result = &mut reader => { result?; writer.await }
            }
        });
        Ok(Self {
            sender: Some(SessionSender {
                tx: out_tx,
                max: config.max_message_bytes,
            }),
            incoming: in_rx,
            cancel,
            task: Some(task),
        })
    }
    pub fn sender(&self) -> SessionSender {
        self.sender
            .as_ref()
            .expect("sender present before shutdown begins")
            .clone()
    }
    pub async fn recv(&mut self) -> Option<Packet> {
        self.incoming.recv().await
    }
    /// Receive without polling the runtime. Only `Empty` proves that no packet
    /// is currently admitted; `Disconnected` remains a distinct terminal state.
    pub fn try_recv(&mut self) -> Result<Packet, mpsc::error::TryRecvError> {
        self.incoming.try_recv()
    }
    /// Close admission and join the socket task, retaining its handle if this
    /// wait is cancelled. This is terminal: do not call `sender()` after the
    /// first poll. A later `cancel_and_join` can escalate a pending finish.
    pub async fn finish_and_join(&mut self) -> Result<(), TransportError> {
        self.shutdown_owned(true).await
    }
    /// Cancel and actually join the retained socket task. Cancelling this wait
    /// preserves ownership for a later call; dropping the session only aborts.
    /// Repeated shutdown after retirement succeeds without joining twice.
    pub async fn cancel_and_join(&mut self) -> Result<(), TransportError> {
        self.shutdown_owned(false).await
    }
    pub async fn cancel(mut self) -> Result<(), TransportError> {
        self.shutdown_owned(false).await
    }
    /// Close outgoing admission and drain already admitted packets, including with live sender clones.
    pub async fn finish(mut self) -> Result<(), TransportError> {
        self.shutdown_owned(true).await
    }
    /// A borrowed shutdown keeps the join handle in its resource owner if the
    /// caller's future is cancelled. The RTVI bridge can subsequently finish
    /// joining from cleanup. Public consuming shutdown retains its old API.
    async fn shutdown_owned(&mut self, graceful: bool) -> Result<(), TransportError> {
        let mode = if graceful { 1 } else { 2 };
        self.cancel
            .send_modify(|current| *current = (*current).max(mode));
        self.sender.take();
        let Some(task) = self.task.as_mut() else {
            return Ok(());
        };
        let result = task.await;
        self.task.take();
        result?
    }
}
impl Drop for WebSocketSession {
    fn drop(&mut self) {
        let _ = self.cancel.send(2);
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
const WRITE_BUFFER_BYTES: usize = 128 * 1024;
fn websocket_config(config: &SessionConfig) -> WebSocketConfig {
    WebSocketConfig::default()
        .write_buffer_size(WRITE_BUFFER_BYTES)
        // Automatic pong responses share this buffer while the peer is not reading.
        .max_write_buffer_size(config.max_message_bytes.saturating_add(WRITE_BUFFER_BYTES))
        .max_message_size(Some(config.max_message_bytes))
        .max_frame_size(Some(config.max_message_bytes))
}

async fn flush_peer_close<S>(
    sink: &mut S,
    cancelled: &mut watch::Receiver<u8>,
    timeout: Duration,
    pending: bool,
) -> Result<(), TransportError>
where
    S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::select! {
        biased;
        _ = wait_for_cancel(cancelled) => return Ok(()),
        result = tokio::time::timeout(timeout, sink.flush()) => {
            result.map_err(|_| TransportError::Timeout("close"))??;
        }
    }
    if pending {
        Err(TransportError::Closed)
    } else {
        Ok(())
    }
}

async fn wait_for_cancel(rx: &mut watch::Receiver<u8>) {
    loop {
        if *rx.borrow_and_update() == 2 {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}
fn validate(c: &SessionConfig) -> Result<(), TransportError> {
    if c.queue_capacity == 0
        || c.max_message_bytes == 0
        || c.max_message_bytes
            .checked_add(WRITE_BUFFER_BYTES)
            .is_none_or(|max| max == usize::MAX)
        || c.connect_timeout.is_zero()
        || c.write_timeout.is_zero()
        || c.close_timeout.is_zero()
    {
        Err(TransportError::InvalidConfig)
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn control_write_buffer_has_a_finite_ceiling() {
        let config = SessionConfig {
            max_message_bytes: 1024,
            ..Default::default()
        };
        let websocket = websocket_config(&config);
        assert_eq!(websocket.max_write_buffer_size, 128 * 1024 + 1024);
        assert!(websocket.max_write_buffer_size > websocket.write_buffer_size);
    }

    #[tokio::test]
    async fn actual_loopback_binary_text_and_close() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            assert_eq!(session.recv().await, Some(Packet::Text("hello λ".into())));
            assert_eq!(
                session.recv().await,
                Some(Packet::Binary(Bytes::from_static(&[0, 255, 0, 1])))
            );
            session
                .sender()
                .send(Packet::Text("reply".into()))
                .await
                .unwrap();
            session.finish().await.unwrap();
        });
        let mut client =
            WebSocketSession::connect(&format!("ws://{address}"), SessionConfig::default())
                .await
                .unwrap();
        client
            .sender()
            .send(Packet::Text("hello λ".into()))
            .await
            .unwrap();
        client
            .sender()
            .send(Packet::Binary(Bytes::from_static(&[0, 255, 0, 1])))
            .await
            .unwrap();
        assert_eq!(client.recv().await, Some(Packet::Text("reply".into())));
        assert_eq!(client.recv().await, None);
        client.finish().await.unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn cancel_idle_connection_finishes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut s = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            assert!(s.recv().await.is_none());
            s.finish().await.unwrap();
        });
        let client = WebSocketSession::connect(&format!("ws://{addr}"), SessionConfig::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), client.cancel())
            .await
            .unwrap()
            .unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn invalid_configuration_rejected_before_network() {
        let config = SessionConfig {
            queue_capacity: 0,
            ..Default::default()
        };
        assert!(matches!(
            WebSocketSession::connect("not a url", config).await,
            Err(TransportError::InvalidConfig)
        ));
    }
}

#[cfg(test)]
mod lifecycle_tests {
    use super::*;
    #[tokio::test]
    async fn finish_drains_even_with_sender_clone_alive() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            let mut count = 0;
            while session.recv().await.is_some() {
                count += 1;
            }
            session.finish().await.unwrap();
            count
        });
        let client = WebSocketSession::connect(&format!("ws://{addr}"), SessionConfig::default())
            .await
            .unwrap();
        let sender = client.sender();
        for _ in 0..5 {
            sender.send(Packet::Text("x".into())).await.unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), client.finish())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(
            sender.send(Packet::Text("late".into())).await,
            Err(TransportError::Closed)
        ));
        assert_eq!(server.await.unwrap(), 5);
    }
    #[tokio::test]
    async fn oversized_outgoing_packet_is_rejected_locally() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut s = WebSocketSession::accept(stream, SessionConfig::default())
                .await
                .unwrap();
            assert_eq!(s.recv().await, None);
            s.finish().await.unwrap();
        });
        let config = SessionConfig {
            max_message_bytes: 3,
            ..Default::default()
        };
        let client = WebSocketSession::connect(&format!("ws://{addr}"), config)
            .await
            .unwrap();
        assert!(matches!(
            client.sender().send(Packet::Text("1234".into())).await,
            Err(TransportError::TooLarge)
        ));
        client.cancel().await.unwrap();
        server.await.unwrap();
    }
    #[tokio::test]
    async fn handshake_timeout_closes_unresponsive_peer() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(Duration::from_millis(100)).await;
            drop(stream);
        });
        let config = SessionConfig {
            connect_timeout: Duration::from_millis(20),
            ..Default::default()
        };
        assert!(matches!(
            WebSocketSession::connect(&format!("ws://{addr}"), config).await,
            Err(TransportError::Timeout("connect"))
        ));
        server.await.unwrap();
    }
}

#[cfg(test)]
mod request_policy_tests {
    use super::*;
    #[tokio::test]
    async fn request_policy_rejects_invalid_config_before_handshake() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let peer = TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (stream, _) = listener.accept().await.unwrap();
        let result = WebSocketSession::accept_with_request_policy(
            stream,
            SessionConfig {
                queue_capacity: 0,
                ..Default::default()
            },
            |_| panic!("invalid configuration must fail first"),
        )
        .await;
        assert!(matches!(result, Err(TransportError::InvalidConfig)));
        drop(peer);
    }
    #[tokio::test]
    async fn request_policy_keeps_incoming_message_size_bounds() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut session = WebSocketSession::accept_with_request_policy(
                stream,
                SessionConfig {
                    max_message_bytes: 32,
                    ..Default::default()
                },
                |request| request.uri().path() == "/bounded",
            )
            .await
            .unwrap();
            assert!(session.recv().await.is_none());
            assert!(session.finish().await.is_err());
        });
        let client =
            WebSocketSession::connect(&format!("ws://{addr}/bounded"), SessionConfig::default())
                .await
                .unwrap();
        client
            .sender()
            .send(Packet::Binary(vec![0; 256].into()))
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), server)
            .await
            .unwrap()
            .unwrap();
        let _ = client.cancel().await;
    }
}

#[cfg(test)]
mod borrowed_shutdown_tests {
    use super::*;

    // This is a deterministic ownership-boundary fixture. The held task stands
    // in for the session's owned socket task; real sockets remain covered by the
    // public cancel/finish transport tests and the RTVI client integration.
    #[tokio::test]
    async fn cancelled_borrowed_shutdown_retains_handle_for_later_join() {
        let (out_tx, _out_rx) = mpsc::channel(1);
        let (_in_tx, in_rx) = mpsc::channel(1);
        let (cancel, mut observed) = watch::channel(0u8);
        let (release, held) = tokio::sync::oneshot::channel();
        let (retired, retired_rx) = tokio::sync::oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = held.await;
            let _ = retired.send(());
            Ok(())
        });
        let mut session = WebSocketSession {
            sender: Some(SessionSender {
                tx: out_tx,
                max: 64,
            }),
            incoming: in_rx,
            cancel,
            task: Some(task),
        };
        {
            let shutdown = session.finish_and_join();
            tokio::pin!(shutdown);
            tokio::time::timeout(Duration::from_secs(1), async {
                tokio::select! {
                    biased;
                    result = &mut shutdown => panic!("held owner joined early: {result:?}"),
                    changed = observed.changed() => changed.unwrap(),
                }
            })
            .await
            .unwrap();
            // Dropping this pending borrowed future must not drop the session.
        }
        assert!(session
            .task
            .as_ref()
            .is_some_and(|task| !task.is_finished()));
        assert_eq!(*observed.borrow(), 1);
        assert!(session.sender.is_none());
        {
            let shutdown = session.cancel_and_join();
            tokio::pin!(shutdown);
            tokio::select! {
                biased;
                result = &mut shutdown => panic!("held owner joined early: {result:?}"),
                changed = observed.changed() => changed.unwrap(),
            }
        }
        assert_eq!(*observed.borrow(), 2);
        assert!(
            session.task.is_some(),
            "cancelled wait retains the same handle"
        );
        let result = tokio::time::timeout(Duration::from_secs(1), async {
            tokio::join!(session.finish_and_join(), async {
                tokio::task::yield_now().await;
                assert_eq!(*observed.borrow(), 2, "finish cannot downgrade cancel");
                release.send(()).unwrap();
            })
        })
        .await
        .unwrap();
        result.0.unwrap();
        retired_rx.await.unwrap();
        assert!(session.task.is_none());
        session.cancel_and_join().await.unwrap();
        session.finish_and_join().await.unwrap();
    }

    #[tokio::test]
    async fn synchronous_receive_distinguishes_empty_and_disconnected_under_cooperative_exhaustion()
    {
        use std::{future::Future, task::Poll};
        let (out_tx, _out_rx) = mpsc::channel(1);
        let (in_tx, in_rx) = mpsc::channel(1);
        let (cancel, _observed) = watch::channel(0u8);
        let mut session = WebSocketSession {
            sender: Some(SessionSender {
                tx: out_tx,
                max: 64,
            }),
            incoming: in_rx,
            cancel,
            task: Some(tokio::spawn(async { Ok(()) })),
        };
        assert_eq!(session.try_recv(), Err(mpsc::error::TryRecvError::Empty));
        let interruption = Packet::Text("bot-interrupted".into());
        in_tx.try_send(interruption.clone()).unwrap();
        let (exhausted, async_pending, received) = std::future::poll_fn(|cx| {
            let mut exhausted = false;
            for _ in 0..1024 {
                let consume = tokio::task::consume_budget();
                tokio::pin!(consume);
                if consume.as_mut().poll(cx).is_pending() {
                    exhausted = true;
                    break;
                }
            }
            let async_pending = {
                let receive = session.recv();
                tokio::pin!(receive);
                receive.as_mut().poll(cx).is_pending()
            };
            Poll::Ready((exhausted, async_pending, session.try_recv()))
        })
        .await;
        let empty = session.try_recv();
        drop(in_tx);
        let disconnected = session.try_recv();
        session.cancel_and_join().await.unwrap();
        assert!(session.task.is_none());
        assert!(exhausted);
        assert!(async_pending, "async Pending is not an Empty receipt");
        assert_eq!(received, Ok(interruption));
        assert_eq!(empty, Err(mpsc::error::TryRecvError::Empty));
        assert_eq!(disconnected, Err(mpsc::error::TryRecvError::Disconnected));
    }

    #[tokio::test]
    async fn borrowed_shutdown_records_task_error_before_idempotent_repeat() {
        let (out_tx, _out_rx) = mpsc::channel(1);
        let (_in_tx, in_rx) = mpsc::channel(1);
        let (cancel, _observed) = watch::channel(0u8);
        let mut session = WebSocketSession {
            sender: Some(SessionSender {
                tx: out_tx,
                max: 64,
            }),
            incoming: in_rx,
            cancel,
            task: Some(tokio::spawn(async {
                Err(TransportError::Timeout("write"))
            })),
        };
        let first = session.finish_and_join().await;
        assert!(
            session.task.is_none(),
            "the error was observed after an actual await"
        );
        assert!(matches!(first, Err(TransportError::Timeout("write"))));
        session.cancel_and_join().await.unwrap();
        session.finish_and_join().await.unwrap();
    }
}
