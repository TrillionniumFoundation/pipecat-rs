use crate::{
    queue::Hub,
    resp::{Connection, Resp},
    CodecError, MessageEnvelope, NetworkSubscription, PythonJsonCodec, ReceiveError,
};
use pipecat_workers::BusMessage;
use std::{fmt, sync::Arc, time::Duration};
use tokio::{
    net::TcpStream,
    sync::{broadcast, watch, Mutex},
    task::JoinHandle,
    time::{sleep, timeout},
};

#[derive(Debug, thiserror::Error)]
pub enum RedisError {
    #[error("invalid Redis configuration: {0}")]
    Configuration(&'static str),
    #[error("Redis I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Redis server error: {0}")]
    Server(String),
    #[error("Redis protocol error: {0}")]
    Protocol(String),
    #[error("Redis operation timed out; a publish may already have been delivered")]
    Timeout,
    #[error("Redis bus has stopped")]
    Closed,
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Receive(#[from] ReceiveError),
}

/// Plain TCP Redis endpoint. TLS URLs are deliberately unsupported; use a trusted
/// local connection or an independently secured tunnel for remote deployments.
#[derive(Clone)]
pub struct RedisConfig {
    pub address: String,
    pub channel: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub database: u32,
    pub capacity_per_priority: usize,
    pub command_timeout: Duration,
    pub reconnect_min: Duration,
    pub reconnect_max: Duration,
    pub codec: PythonJsonCodec,
}
impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            address: "127.0.0.1:6379".into(),
            channel: "pipecat:bus".into(),
            username: None,
            password: None,
            database: 0,
            capacity_per_priority: 256,
            command_timeout: Duration::from_secs(5),
            reconnect_min: Duration::from_millis(100),
            reconnect_max: Duration::from_secs(5),
            codec: PythonJsonCodec::default(),
        }
    }
}
impl fmt::Debug for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RedisConfig")
            .field("address", &self.address)
            .field("channel", &self.channel)
            .field("database", &self.database)
            .field("authenticated", &self.password.is_some())
            .finish_non_exhaustive()
    }
}
impl RedisConfig {
    fn validate(&self) -> Result<(), RedisError> {
        if self.address.contains("://") {
            return Err(RedisError::Configuration(
                "address must be host:port, not a URL",
            ));
        }
        if self.channel.is_empty() || self.channel.len() > 4096 {
            return Err(RedisError::Configuration(
                "channel must contain 1..4096 bytes",
            ));
        }
        if self.capacity_per_priority == 0 {
            return Err(RedisError::Configuration("capacity must be nonzero"));
        }
        if self.codec.max_message_bytes == 0 || self.codec.max_message_bytes > 128 * 1024 * 1024 {
            return Err(RedisError::Configuration(
                "message limit must be 1..128 MiB",
            ));
        }
        if self.command_timeout.is_zero()
            || self.reconnect_min.is_zero()
            || self.reconnect_max < self.reconnect_min
        {
            return Err(RedisError::Configuration("invalid timeout/backoff"));
        }
        if self.username.is_some() && self.password.is_none() {
            return Err(RedisError::Configuration("username requires password"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConnectionState {
    Connected,
    Reconnecting { attempt: u64 },
    Stopped,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RedisEvent {
    /// Redis pub/sub cannot replay messages missed during disconnection.
    Disconnected,
    Reconnected,
    InvalidMessage(String),
}

/// Owns one reader task and a cancellation-safe serialized publishing connection.
/// Dropping the bus cancels the reader. `shutdown` also waits for it to exit.
pub struct RedisBus {
    config: Arc<RedisConfig>,
    hub: Hub,
    publisher: Mutex<Option<Connection>>,
    stop: watch::Sender<bool>,
    state: watch::Receiver<ConnectionState>,
    events: broadcast::Sender<RedisEvent>,
    reader: Mutex<Option<JoinHandle<()>>>,
}
impl RedisBus {
    /// Returns only after Redis has acknowledged SUBSCRIBE. Startup failures are
    /// returned to the caller; established readers reconnect with capped backoff.
    pub async fn connect(config: RedisConfig) -> Result<Self, RedisError> {
        config.validate()?;
        let connection = timeout(config.command_timeout, connect_subscriber(&config))
            .await
            .map_err(|_| RedisError::Timeout)??;
        let config = Arc::new(config);
        let hub = Hub::new(config.capacity_per_priority);
        let (stop, stop_rx) = watch::channel(false);
        let (state_tx, state) = watch::channel(ConnectionState::Connected);
        let (events, _) = broadcast::channel(64);
        let reader = tokio::spawn(reader_loop(
            config.clone(),
            hub.clone(),
            connection,
            stop_rx,
            state_tx,
            events.clone(),
        ));
        Ok(Self {
            config,
            hub,
            publisher: Mutex::new(None),
            stop,
            state,
            events,
            reader: Mutex::new(Some(reader)),
        })
    }
    pub fn subscribe(&self) -> Result<NetworkSubscription, RedisError> {
        self.ensure_open()?;
        Ok(self.hub.subscribe(None)?)
    }
    /// Receive broadcasts plus messages targeted at this worker name.
    pub fn subscribe_target(
        &self,
        name: impl Into<String>,
    ) -> Result<NetworkSubscription, RedisError> {
        self.ensure_open()?;
        Ok(self.hub.subscribe(Some(name.into()))?)
    }
    pub fn connection_state(&self) -> watch::Receiver<ConnectionState> {
        self.state.clone()
    }
    pub fn events(&self) -> broadcast::Receiver<RedisEvent> {
        self.events.subscribe()
    }
    fn ensure_open(&self) -> Result<(), RedisError> {
        if *self.stop.borrow() {
            Err(RedisError::Closed)
        } else {
            Ok(())
        }
    }
    pub async fn send(&self, message: &BusMessage) -> Result<usize, RedisError> {
        self.send_envelope(&self.config.codec.from_bus_message(message)?)
            .await
    }
    /// Local error messages bypass Redis. In-memory worker references have no
    /// serializable envelope and should be sent on the worker's LocalBus.
    pub async fn send_envelope(&self, envelope: &MessageEnvelope) -> Result<usize, RedisError> {
        self.ensure_open()?;
        if envelope.is_local_only() {
            return Ok(self.hub.deliver(envelope.clone())?);
        }
        let bytes = self.config.codec.encode(envelope)?;
        let operation = async {
            let mut slot = self.publisher.lock().await;
            // Taking ownership prevents a cancelled/failed command from leaving
            // unread responses on a connection subsequently reused by another call.
            let mut connection = match slot.take() {
                Some(connection) => connection,
                None => connect_socket(&self.config).await?,
            };
            let result = connection
                .command(&[b"PUBLISH", self.config.channel.as_bytes(), &bytes])
                .await?;
            let count = match result {
                Resp::Integer(n) if n >= 0 => usize::try_from(n)
                    .map_err(|_| RedisError::Protocol("publish count overflow".into()))?,
                _ => {
                    return Err(RedisError::Protocol(
                        "PUBLISH did not return a count".into(),
                    ))
                }
            };
            *slot = Some(connection);
            Ok(count)
        };
        let mut stopped = self.stop.subscribe();
        tokio::select! {
            biased;
            _ = wait_stopped(&mut stopped) => Err(RedisError::Closed),
            result = timeout(self.config.command_timeout, operation) => result.map_err(|_| RedisError::Timeout)?,
        }
    }
    pub async fn shutdown(&self) {
        self.stop.send_replace(true);
        self.hub.close();
        if let Some(task) = self.reader.lock().await.take() {
            let _ = task.await;
        }
        self.publisher.lock().await.take();
    }
}
impl Drop for RedisBus {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.hub.close();
    }
}

async fn connect_socket(config: &RedisConfig) -> Result<Connection, RedisError> {
    let stream = TcpStream::connect(&config.address).await?;
    if config.password.is_some() && !stream.peer_addr()?.ip().is_loopback() {
        return Err(RedisError::Configuration(
            "plaintext credentials are restricted to loopback peers",
        ));
    }
    stream.set_nodelay(true)?;
    let mut connection = Connection::new(stream, config.codec.max_message_bytes + 8192);
    if let Some(password) = &config.password {
        let result = if let Some(username) = &config.username {
            connection
                .command(&[b"AUTH", username.as_bytes(), password.as_bytes()])
                .await?
        } else {
            connection.command(&[b"AUTH", password.as_bytes()]).await?
        };
        if result != Resp::Simple(b"OK".to_vec()) {
            return Err(RedisError::Protocol("AUTH did not return OK".into()));
        }
    }
    if config.database != 0
        && connection
            .command(&[b"SELECT", config.database.to_string().as_bytes()])
            .await?
            != Resp::Simple(b"OK".to_vec())
    {
        return Err(RedisError::Protocol("SELECT did not return OK".into()));
    }
    Ok(connection)
}
async fn connect_subscriber(config: &RedisConfig) -> Result<Connection, RedisError> {
    let mut connection = connect_socket(config).await?;
    let ack = connection
        .command(&[b"SUBSCRIBE", config.channel.as_bytes()])
        .await?;
    match ack {
        Resp::Array(Some(values))
            if values
                == vec![
                    Resp::Bulk(Some(b"subscribe".to_vec())),
                    Resp::Bulk(Some(config.channel.as_bytes().to_vec())),
                    Resp::Integer(1),
                ] =>
        {
            Ok(connection)
        }
        _ => Err(RedisError::Protocol(
            "invalid SUBSCRIBE acknowledgement".into(),
        )),
    }
}
async fn wait_stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}
async fn reader_loop(
    config: Arc<RedisConfig>,
    hub: Hub,
    mut connection: Connection,
    mut stop: watch::Receiver<bool>,
    state: watch::Sender<ConnectionState>,
    events: broadcast::Sender<RedisEvent>,
) {
    loop {
        let read = tokio::select! { biased; _ = wait_stopped(&mut stop) => break, result = connection.read() => result };
        match read {
            Ok(Resp::Array(Some(values))) => {
                if let [Resp::Bulk(Some(kind)), Resp::Bulk(Some(channel)), Resp::Bulk(Some(payload))] =
                    values.as_slice()
                {
                    if kind == b"message" && channel == config.channel.as_bytes() {
                        match config.codec.decode(payload) {
                            Ok(message) if !message.is_local_only() => {
                                let _ = hub.deliver(message);
                            }
                            Ok(_) => {
                                let _ = events.send(RedisEvent::InvalidMessage(
                                    "local-only message received from Redis".into(),
                                ));
                            }
                            Err(error) => {
                                let _ = events.send(RedisEvent::InvalidMessage(error.to_string()));
                            }
                        }
                    }
                }
            }
            Ok(_) | Err(_) => {
                let _ = events.send(RedisEvent::Disconnected);
                let mut delay = config.reconnect_min;
                let mut attempt = 0u64;
                loop {
                    attempt = attempt.saturating_add(1);
                    state.send_replace(ConnectionState::Reconnecting { attempt });
                    tokio::select! { biased; _ = wait_stopped(&mut stop) => { state.send_replace(ConnectionState::Stopped); hub.close(); return; }, _ = sleep(delay) => {} }
                    let retry = tokio::select! { biased; _ = wait_stopped(&mut stop) => { state.send_replace(ConnectionState::Stopped); hub.close(); return; }, result = timeout(config.command_timeout, connect_subscriber(&config)) => result };
                    if let Ok(Ok(new_connection)) = retry {
                        connection = new_connection;
                        state.send_replace(ConnectionState::Connected);
                        let _ = events.send(RedisEvent::Reconnected);
                        break;
                    }
                    delay = delay.saturating_mul(2).min(config.reconnect_max);
                }
            }
        }
    }
    state.send_replace(ConnectionState::Stopped);
    hub.close();
}
