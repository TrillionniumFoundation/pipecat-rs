#![doc = include_str!("../README.md")]
// Copyright (c) 2026, Daily. SPDX-License-Identifier: BSD-2-Clause
mod attachment;
mod routing;

pub use attachment::{ProxyAttachment, ProxyEvent, ProxyState};
use pipecat_network_bus::PythonJsonCodec;
use pipecat_transports::{SessionConfig, WebSocketSession};
use pipecat_workers::{LocalBus, WorkerRegistry};
pub use routing::ForwardKind;
use std::{collections::BTreeSet, time::Duration};
use tokio::net::TcpStream;
pub use tokio_tungstenite::tungstenite::handshake::server::Request as UpgradeRequest;
use tokio_tungstenite::tungstenite::{client::IntoClientRequest, http::HeaderMap};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ProxyError {
    #[error("invalid proxy configuration: {0}")]
    Configuration(String),
    #[error("transport failed; delivery may be ambiguous: {0}")]
    Transport(String),
    #[error("peer disconnected; requests are not replayed")]
    Disconnected,
    #[error("source protocol error: {0}")]
    Protocol(String),
    #[error("proxy capacity exhausted: {0}")]
    Capacity(&'static str),
    #[error("{0} timed out")]
    Timeout(&'static str),
    #[error("worker bus: {0}")]
    Bus(String),
    #[error("proxy is already active")]
    AlreadyActive,
    #[error("proxy task stopped")]
    Stopped,
}
impl From<pipecat_transports::TransportError> for ProxyError {
    fn from(error: pipecat_transports::TransportError) -> Self {
        Self::Transport(error.to_string())
    }
}
impl From<pipecat_workers::BusError> for ProxyError {
    fn from(error: pipecat_workers::BusError) -> Self {
        Self::Bus(error.to_string())
    }
}
impl From<pipecat_network_bus::CodecError> for ProxyError {
    fn from(error: pipecat_network_bus::CodecError) -> Self {
        Self::Protocol(error.to_string())
    }
}

#[derive(Debug, Clone)]
pub struct ProxyOptions {
    pub name: String,
    pub local_worker: String,
    pub remote_worker: String,
    /// Additional source-scoped frame or speak broadcasts. RPC always remains addressed.
    pub forward_messages: BTreeSet<ForwardKind>,
    /// Optionally pin discovery to the authenticated peer's expected runner.
    pub remote_runner: Option<String>,
    pub max_pending_jobs: usize,
    pub event_capacity: usize,
    pub admission_timeout: Duration,
    pub cleanup_timeout: Duration,
    pub codec: PythonJsonCodec,
    pub session: SessionConfig,
}
impl ProxyOptions {
    pub fn new(
        name: impl Into<String>,
        local: impl Into<String>,
        remote: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            local_worker: local.into(),
            remote_worker: remote.into(),
            forward_messages: BTreeSet::new(),
            remote_runner: None,
            max_pending_jobs: 128,
            event_capacity: 64,
            admission_timeout: Duration::from_secs(5),
            cleanup_timeout: Duration::from_secs(2),
            codec: PythonJsonCodec::default(),
            session: SessionConfig::default(),
        }
    }
    fn validate(&self) -> Result<(), ProxyError> {
        if self.name.is_empty()
            || self.local_worker.is_empty()
            || self.remote_worker.is_empty()
            || self.local_worker == self.remote_worker
            || self.max_pending_jobs == 0
            || self.event_capacity == 0
            || self.admission_timeout.is_zero()
            || self.cleanup_timeout.is_zero()
            || self.codec.max_message_bytes == 0
            || self.session.queue_capacity == 0
            || self.session.max_message_bytes == 0
            || self.session.connect_timeout.is_zero()
            || self.session.write_timeout.is_zero()
            || self.session.close_timeout.is_zero()
            || self.remote_runner.as_ref().is_some_and(String::is_empty)
            || [
                self.admission_timeout,
                self.cleanup_timeout,
                self.session.connect_timeout,
                self.session.write_timeout,
                self.session.close_timeout,
            ]
            .iter()
            .any(|duration| tokio::time::Instant::now().checked_add(*duration).is_none())
        {
            return Err(ProxyError::Configuration(
                "distinct nonempty workers and nonzero limits/deadlines required".into(),
            ));
        }
        Ok(())
    }
}

/// Inactive until `activate`; reconnection is explicit and always uses a new attachment.
/// No disconnected queue or retry ledger is retained.
pub struct WebSocketProxyClient {
    url: String,
    headers: HeaderMap,
    options: ProxyOptions,
    bus: LocalBus,
    registry: WorkerRegistry,
    attachment: Option<ProxyAttachment>,
}
impl WebSocketProxyClient {
    pub fn new(
        url: impl Into<String>,
        bus: LocalBus,
        registry: WorkerRegistry,
        options: ProxyOptions,
    ) -> Result<Self, ProxyError> {
        options.validate()?;
        let url = url.into();
        request(&url, &HeaderMap::new())?;
        Ok(Self {
            url,
            headers: HeaderMap::new(),
            options,
            bus,
            registry,
            attachment: None,
        })
    }
    /// Custom headers cannot replace WebSocket handshake fields. Credentialed
    /// endpoints require WSS, except literal loopback addresses and localhost.
    pub fn with_headers(mut self, headers: HeaderMap) -> Result<Self, ProxyError> {
        request(&self.url, &headers)?;
        self.headers = headers;
        Ok(self)
    }
    pub fn attachment(&self) -> Option<&ProxyAttachment> {
        self.attachment.as_ref()
    }
    pub fn attachment_mut(&mut self) -> Option<&mut ProxyAttachment> {
        self.attachment.as_mut()
    }
    pub async fn activate(&mut self) -> Result<&mut ProxyAttachment, ProxyError> {
        if self.attachment.as_ref().is_some_and(|a| !a.is_stopped()) {
            return Err(ProxyError::AlreadyActive);
        }
        self.attachment.take();
        let session = WebSocketSession::connect_with_request(
            request(&self.url, &self.headers)?,
            self.options.session.clone(),
        )
        .await?;
        self.attachment = Some(ProxyAttachment::attach(
            session,
            self.bus.clone(),
            self.registry.clone(),
            self.options.clone(),
            false,
        )?);
        Ok(self.attachment.as_mut().expect("attached"))
    }
    pub async fn stop(&mut self) -> Result<(), ProxyError> {
        match self.attachment.as_mut() {
            Some(a) => a.shutdown().await,
            None => Ok(()),
        }
    }
}

/// Each accepted connection has independent ownership, bounded queues and cleanup.
pub struct WebSocketProxyServer;
impl WebSocketProxyServer {
    /// Authenticate and validate Origin/path before completing the handshake.
    /// The policy must authenticate non-loopback clients; Origin alone is not authentication.
    pub async fn accept_with_request_policy<F>(
        stream: TcpStream,
        bus: LocalBus,
        registry: WorkerRegistry,
        options: ProxyOptions,
        policy: F,
    ) -> Result<ProxyAttachment, ProxyError>
    where
        F: FnOnce(&UpgradeRequest) -> bool + Send + Unpin,
    {
        options.validate()?;
        let session =
            WebSocketSession::accept_with_request_policy(stream, options.session.clone(), policy)
                .await?;
        Self::attach(session, bus, registry, options)
    }
    /// Attach a session whose caller has already performed authentication and Origin validation.
    pub fn attach(
        session: WebSocketSession,
        bus: LocalBus,
        registry: WorkerRegistry,
        options: ProxyOptions,
    ) -> Result<ProxyAttachment, ProxyError> {
        ProxyAttachment::attach(session, bus, registry, options, true)
    }
}

fn request(
    url: &str,
    headers: &HeaderMap,
) -> Result<tokio_tungstenite::tungstenite::handshake::client::Request, ProxyError> {
    let parsed = url::Url::parse(url).map_err(|e| ProxyError::Configuration(e.to_string()))?;
    if !matches!(parsed.scheme(), "ws" | "wss")
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.fragment().is_some()
    {
        return Err(ProxyError::Configuration(
            "expected ws/wss URL without userinfo or fragment".into(),
        ));
    }
    let loopback = parsed.host_str().is_some_and(|host| {
        host == "localhost"
            || host
                .trim_matches(['[', ']'])
                .parse::<std::net::IpAddr>()
                .is_ok_and(|ip| ip.is_loopback())
    });
    if !headers.is_empty() && parsed.scheme() != "wss" && !loopback {
        return Err(ProxyError::Configuration(
            "custom headers require WSS outside literal loopback/localhost".into(),
        ));
    }
    for name in headers.keys() {
        if matches!(name.as_str(), "host" | "connection" | "upgrade")
            || name.as_str().starts_with("sec-websocket-")
        {
            return Err(ProxyError::Configuration(
                "custom headers cannot replace WebSocket handshake fields".into(),
            ));
        }
    }
    let mut request = url
        .into_client_request()
        .map_err(|e| ProxyError::Configuration(e.to_string()))?;
    for (name, value) in headers {
        request.headers_mut().append(name, value.clone());
    }
    Ok(request)
}
