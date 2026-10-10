//! Native PostgreSQL PGMQ transport. Requires the PGMQ extension or the source
//! deployment's `public.bus_*` security-definer wrappers; installs neither.
use crate::{
    queue::Hub, CodecError, MessageEnvelope, NetworkSubscription, PythonJsonCodec, ReceiveError,
};
use pipecat_workers::BusMessage;
use serde_json::Value;
use std::{
    fmt,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{
    sync::{broadcast, watch, Mutex},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_postgres::{
    types::{FromSql, ToSql, Type},
    Client, NoTls, Row,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgmqMode {
    Direct,
    Isolated,
}
#[derive(Clone)]
pub struct PgmqConfig {
    /// This NoTls driver requires a loopback endpoint (including a TLS tunnel).
    pub address: SocketAddr,
    pub database: String,
    pub user: String,
    pub password: Option<String>,
    pub channel: String,
    pub mode: PgmqMode,
    pub visibility_timeout: i32,
    pub batch_size: i32,
    pub poll_interval_ms: i32,
    pub max_poll_seconds: i32,
    pub command_timeout: Duration,
    pub retry_delay: Duration,
    pub capacity_per_priority: usize,
    pub codec: PythonJsonCodec,
}
impl Default for PgmqConfig {
    fn default() -> Self {
        Self {
            address: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5432),
            database: "postgres".into(),
            user: "postgres".into(),
            password: None,
            channel: "pipecat_bus".into(),
            mode: PgmqMode::Direct,
            visibility_timeout: 30,
            batch_size: 10,
            poll_interval_ms: 100,
            max_poll_seconds: 5,
            command_timeout: Duration::from_secs(5),
            retry_delay: Duration::from_secs(1),
            capacity_per_priority: 256,
            codec: PythonJsonCodec::default(),
        }
    }
}
impl fmt::Debug for PgmqConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PgmqConfig")
            .field("address", &self.address)
            .field("mode", &self.mode)
            .finish_non_exhaustive()
    }
}
impl PgmqConfig {
    fn validate(&self) -> Result<(), PgmqError> {
        if !self.address.ip().is_loopback() {
            return Err(PgmqError::Configuration(
                "NoTls PostgreSQL requires a loopback endpoint",
            ));
        }
        if self.channel.is_empty()
            || self.channel.len() > 4096
            || self.database.is_empty()
            || self.user.is_empty()
        {
            return Err(PgmqError::Configuration("invalid channel/database/user"));
        }
        if self.visibility_timeout <= 0
            || !(1..=1000).contains(&self.batch_size)
            || self.poll_interval_ms <= 0
            || !(0..=3600).contains(&self.max_poll_seconds)
        {
            return Err(PgmqError::Configuration(
                "invalid lease/batch/poll parameters",
            ));
        }
        if self.command_timeout.is_zero()
            || self.retry_delay.is_zero()
            || self.capacity_per_priority == 0
            || self.codec.max_message_bytes == 0
            || self.codec.max_message_bytes > 128 * 1024 * 1024
        {
            return Err(PgmqError::Configuration(
                "invalid timeout/capacity/message limit",
            ));
        }
        Ok(())
    }
}
#[derive(Debug, thiserror::Error)]
pub enum PgmqError {
    #[error("invalid PGMQ configuration: {0}")]
    Configuration(&'static str),
    #[error("PostgreSQL operation failed: {0}")]
    Postgres(#[from] tokio_postgres::Error),
    #[error("PostgreSQL operation timed out; its result may be ambiguous")]
    Timeout,
    #[error("PGMQ bus has stopped")]
    Closed,
    #[error("invalid PostgreSQL result: {0}")]
    InvalidResult(&'static str),
    #[error(transparent)]
    Codec(#[from] CodecError),
    #[error(transparent)]
    Receive(#[from] ReceiveError),
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PgmqEvent {
    ReadFailed,
    InvalidMessage { msg_id: i64, error: String },
    AcknowledgeFailed { msg_id: i64 },
}
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct PublishReport {
    pub delivered: usize,
    pub failed_peers: Vec<String>,
}

/// Source-compatible direct queue prefix. Different channels can sanitize to
/// the same prefix; use isolated mode when channels are security capabilities.
pub fn sanitize_pgmq_channel(channel: &str) -> String {
    let mut safe = channel
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' {
                c
            } else {
                '_'
            }
        })
        .collect::<String>();
    if safe.starts_with(|c: char| c.is_ascii_digit()) {
        safe.insert_str(0, "q_");
    }
    safe.truncate(safe.len().min(30));
    if safe.is_empty() {
        "pipecat_bus".into()
    } else {
        safe
    }
}

struct Session {
    client: Client,
    driver: Option<JoinHandle<()>>,
    in_flight: bool,
}
impl Drop for Session {
    fn drop(&mut self) {
        let Some(driver) = self.driver.take() else {
            return;
        };
        if self.in_flight {
            let token = self.client.cancel_token();
            if let Ok(runtime) = tokio::runtime::Handle::try_current() {
                runtime.spawn(async move {
                    let _ = timeout(Duration::from_millis(500), token.cancel_query(NoTls)).await;
                    driver.abort();
                });
                return;
            }
        }
        driver.abort();
    }
}
#[derive(Debug)]
enum Parameter {
    Text(String),
    Integer(i32),
    Bigint(i64),
    Json(Value),
}
impl Parameter {
    fn typed(&self) -> (&(dyn ToSql + Sync), Type) {
        match self {
            Self::Text(v) => (v, Type::TEXT),
            Self::Integer(v) => (v, Type::INT4),
            Self::Bigint(v) => (v, Type::INT8),
            Self::Json(v) => (v, Type::JSONB),
        }
    }
}
struct Database {
    config: Arc<PgmqConfig>,
    control: Mutex<Option<Session>>,
    reader: Mutex<Option<Session>>,
}
impl Database {
    async fn connect(&self) -> Result<Session, PgmqError> {
        let mut config = tokio_postgres::Config::new();
        config
            .host(self.config.address.ip().to_string())
            .port(self.config.address.port())
            .user(&self.config.user)
            .dbname(&self.config.database)
            .ssl_mode(tokio_postgres::config::SslMode::Disable)
            .connect_timeout(self.config.command_timeout);
        if let Some(password) = &self.config.password {
            config.password(password);
        }
        let (client, connection) = config.connect(NoTls).await?;
        let driver = tokio::spawn(async move {
            let _ = connection.await;
        });
        Ok(Session {
            client,
            driver: Some(driver),
            in_flight: false,
        })
    }
    async fn query(
        &self,
        reader: bool,
        sql: &str,
        parameters: Vec<Parameter>,
    ) -> Result<Vec<Row>, PgmqError> {
        let duration = self.config.command_timeout
            + if reader {
                Duration::from_secs(self.config.max_poll_seconds as u64)
            } else {
                Duration::ZERO
            };
        timeout(duration, async {
            let mut slot = if reader {
                self.reader.lock().await
            } else {
                self.control.lock().await
            };
            let mut session = match slot.take() {
                Some(session) if !session.client.is_closed() => session,
                _ => self.connect().await?,
            };
            session.in_flight = true;
            let params = parameters.iter().map(Parameter::typed).collect::<Vec<_>>();
            let result = session.client.query_typed(sql, &params).await;
            session.in_flight = false;
            // Completed server errors have consumed ReadyForQuery; no transaction
            // is left open. A failed connection is never returned to the slot.
            if !session.client.is_closed() {
                *slot = Some(session);
            }
            result.map_err(PgmqError::from)
        })
        .await
        .map_err(|_| PgmqError::Timeout)?
    }
    async fn close(&self) {
        self.reader.lock().await.take();
        self.control.lock().await.take();
    }
}
fn text(value: &str) -> Parameter {
    Parameter::Text(value.into())
}

/// Borrow the wire JSON so limits are checked before serde allocates a value.
/// Payload validation failures belong to the leased message, not the batch.
struct RawJson<'a>(&'a [u8]);
impl<'a> FromSql<'a> for RawJson<'a> {
    fn from_sql(
        ty: &Type,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn std::error::Error + Sync + Send>> {
        if *ty == Type::JSONB {
            match raw.split_first() {
                Some((1, data)) => Ok(Self(data)),
                _ => Err("unsupported PostgreSQL JSONB wire version".into()),
            }
        } else {
            Ok(Self(raw))
        }
    }
    fn accepts(ty: &Type) -> bool {
        *ty == Type::JSONB || *ty == Type::JSON
    }
}
struct LeasedMessage {
    id: i64,
    message: Result<MessageEnvelope, CodecError>,
}
struct Backend {
    database: Database,
    peers: Mutex<Option<(Instant, Vec<String>)>>,
}
impl Backend {
    async fn join(&self) -> Result<String, PgmqError> {
        let c = &self.database.config;
        match c.mode {
            PgmqMode::Direct => {
                let suffix = uuid::Uuid::new_v4().simple().to_string();
                let name = format!("{}_{}", sanitize_pgmq_channel(&c.channel), &suffix[..12]);
                self.database
                    .query(false, "SELECT pgmq.create($1)", vec![text(&name)])
                    .await?;
                Ok(name)
            }
            PgmqMode::Isolated => {
                let rows = self
                    .database
                    .query(false, "SELECT public.bus_join($1)", vec![text(&c.channel)])
                    .await?;
                let name: String = rows
                    .first()
                    .ok_or(PgmqError::InvalidResult("bus_join returned no row"))?
                    .try_get(0)?;
                if name.is_empty() {
                    return Err(PgmqError::InvalidResult("empty queue name"));
                }
                Ok(name)
            }
        }
    }
    async fn publish(&self, queue: &str, payload: Value) -> Result<PublishReport, PgmqError> {
        let c = &self.database.config;
        if c.mode == PgmqMode::Isolated {
            let rows = self
                .database
                .query(
                    false,
                    "SELECT public.bus_publish($1, $2, $3::jsonb)",
                    vec![text(&c.channel), text(queue), Parameter::Json(payload)],
                )
                .await?;
            let ids: Vec<i64> = rows
                .first()
                .ok_or(PgmqError::InvalidResult("bus_publish returned no row"))?
                .try_get(0)?;
            return Ok(PublishReport {
                delivered: ids.len(),
                failed_peers: vec![],
            });
        }
        let peers = {
            let mut cache = self.peers.lock().await;
            if cache
                .as_ref()
                .is_none_or(|(time, _)| time.elapsed() >= Duration::from_secs(1))
            {
                let rows = self
                    .database
                    .query(false, "SELECT queue_name FROM pgmq.list_queues()", vec![])
                    .await?;
                let prefix = format!("{}_", sanitize_pgmq_channel(&c.channel));
                let names = rows
                    .iter()
                    .map(|row| row.try_get::<_, String>(0))
                    .collect::<Result<Vec<_>, _>>()?
                    .into_iter()
                    .filter(|name| name.starts_with(&prefix))
                    .collect();
                *cache = Some((Instant::now(), names));
            }
            cache.as_ref().expect("initialized peer cache").1.clone()
        };
        let mut report = PublishReport::default();
        // The original direct implementation includes its own queue in fanout.
        for peer in peers {
            match self
                .database
                .query(
                    false,
                    "SELECT pgmq.send($1, $2::jsonb)",
                    vec![text(&peer), Parameter::Json(payload.clone())],
                )
                .await
            {
                Ok(_) => report.delivered += 1,
                Err(_) => {
                    self.peers.lock().await.take();
                    report.failed_peers.push(peer);
                }
            }
        }
        Ok(report)
    }
    async fn read(&self, queue: &str) -> Result<Vec<LeasedMessage>, PgmqError> {
        let c = &self.database.config;
        let (sql, parameters) = match c.mode {
            PgmqMode::Direct => (
                "SELECT msg_id, message FROM pgmq.read_with_poll($1, $2, $3, $4, $5)",
                vec![
                    text(queue),
                    Parameter::Integer(c.visibility_timeout),
                    Parameter::Integer(c.batch_size),
                    Parameter::Integer(c.max_poll_seconds),
                    Parameter::Integer(c.poll_interval_ms),
                ],
            ),
            PgmqMode::Isolated => (
                "SELECT msg_id, message FROM public.bus_subscribe($1, $2, $3, $4, $5)",
                vec![
                    text(queue),
                    text(&c.channel),
                    Parameter::Integer(c.visibility_timeout),
                    Parameter::Integer(c.batch_size),
                    Parameter::Integer(c.max_poll_seconds),
                ],
            ),
        };
        self.database
            .query(true, sql, parameters)
            .await?
            .iter()
            .map(|row| {
                let id = row.try_get(0)?;
                let payload: Option<RawJson<'_>> = row.try_get(1)?;
                Ok(LeasedMessage {
                    id,
                    message: match payload {
                        Some(payload) => c.codec.decode(payload.0),
                        None => Err(CodecError::InvalidField("SQL NULL message".into())),
                    },
                })
            })
            .collect()
    }
    async fn archive(&self, queue: &str, msg_id: i64) -> Result<bool, PgmqError> {
        let c = &self.database.config;
        let (sql, parameters) = match c.mode {
            PgmqMode::Direct => (
                "SELECT pgmq.delete($1, $2)",
                vec![text(queue), Parameter::Bigint(msg_id)],
            ),
            PgmqMode::Isolated => (
                "SELECT public.bus_archive($1, $2, $3)",
                vec![text(queue), text(&c.channel), Parameter::Bigint(msg_id)],
            ),
        };
        Ok(self
            .database
            .query(false, sql, parameters)
            .await?
            .first()
            .ok_or(PgmqError::InvalidResult("archive returned no row"))?
            .try_get(0)?)
    }
    async fn leave(&self, queue: &str) -> Result<(), PgmqError> {
        let c = &self.database.config;
        let (sql, parameters) = match c.mode {
            PgmqMode::Direct => ("SELECT pgmq.drop_queue($1)", vec![text(queue)]),
            PgmqMode::Isolated => (
                "SELECT public.bus_leave($1, $2)",
                vec![text(queue), text(&c.channel)],
            ),
        };
        self.database.query(false, sql, parameters).await?;
        self.peers.lock().await.take();
        Ok(())
    }
}

/// Each instance owns an ephemeral private queue. Always call `shutdown` to
/// delete that queue; Drop cancels I/O but cannot promise server-side cleanup.
pub struct PgmqBus {
    backend: Arc<Backend>,
    queue: String,
    hub: Hub,
    stop: watch::Sender<bool>,
    events: broadcast::Sender<PgmqEvent>,
    reader: Mutex<Option<JoinHandle<()>>>,
    left: Mutex<bool>,
}
impl PgmqBus {
    pub async fn connect(config: PgmqConfig) -> Result<Self, PgmqError> {
        config.validate()?;
        let config = Arc::new(config);
        let hub = Hub::new(config.capacity_per_priority);
        let backend = Arc::new(Backend {
            database: Database {
                config,
                control: Mutex::new(None),
                reader: Mutex::new(None),
            },
            peers: Mutex::new(None),
        });
        let queue = backend.join().await?;
        let (stop, stopped) = watch::channel(false);
        let (events, _) = broadcast::channel(64);
        let task = tokio::spawn(pgmq_reader(
            backend.clone(),
            queue.clone(),
            hub.clone(),
            stopped,
            events.clone(),
        ));
        Ok(Self {
            backend,
            queue,
            hub,
            stop,
            events,
            reader: Mutex::new(Some(task)),
            left: Mutex::new(false),
        })
    }
    pub fn queue_name(&self) -> &str {
        &self.queue
    }
    pub fn events(&self) -> broadcast::Receiver<PgmqEvent> {
        self.events.subscribe()
    }
    pub fn subscribe(&self) -> Result<NetworkSubscription, PgmqError> {
        self.ensure_open()?;
        Ok(self.hub.subscribe(None)?)
    }
    pub fn subscribe_target(
        &self,
        name: impl Into<String>,
    ) -> Result<NetworkSubscription, PgmqError> {
        self.ensure_open()?;
        Ok(self.hub.subscribe(Some(name.into()))?)
    }
    fn ensure_open(&self) -> Result<(), PgmqError> {
        if *self.stop.borrow() {
            Err(PgmqError::Closed)
        } else {
            Ok(())
        }
    }
    pub async fn send(&self, message: &BusMessage) -> Result<PublishReport, PgmqError> {
        self.send_envelope(
            &self
                .backend
                .database
                .config
                .codec
                .from_bus_message(message)?,
        )
        .await
    }
    pub async fn send_envelope(
        &self,
        message: &MessageEnvelope,
    ) -> Result<PublishReport, PgmqError> {
        self.ensure_open()?;
        self.backend.database.config.codec.encode(message)?;
        let mut stop = self.stop.subscribe();
        tokio::select! {biased;
            _=stopped(&mut stop)=>Err(PgmqError::Closed),
            result=async {if message.is_local_only(){Ok(PublishReport{delivered:self.hub.deliver_wait(message.clone()).await?,failed_peers:vec![]})}else{self.backend.publish(&self.queue,message.wire_value().clone()).await}}=>result,
        }
    }
    /// Cancels long-polls, waits for dispatch to stop, then removes the private
    /// queue. If removal fails, returns the error and permits a later retry.
    pub async fn shutdown(&self) -> Result<(), PgmqError> {
        self.stop.send_replace(true);
        self.hub.close();
        if let Some(task) = self.reader.lock().await.take() {
            let _ = task.await;
        }
        let mut left = self.left.lock().await;
        if !*left {
            self.backend.leave(&self.queue).await?;
            *left = true;
        }
        self.backend.database.close().await;
        Ok(())
    }
}
impl Drop for PgmqBus {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        self.hub.close();
    }
}
async fn stopped(stop: &mut watch::Receiver<bool>) {
    while !*stop.borrow_and_update() {
        if stop.changed().await.is_err() {
            return;
        }
    }
}
async fn pgmq_reader(
    backend: Arc<Backend>,
    queue: String,
    hub: Hub,
    mut stop: watch::Receiver<bool>,
    events: broadcast::Sender<PgmqEvent>,
) {
    loop {
        let messages =
            tokio::select! {biased;_=stopped(&mut stop)=>break,result=backend.read(&queue)=>result};
        let messages = match messages {
            Ok(messages) => messages,
            Err(_) => {
                let _ = events.send(PgmqEvent::ReadFailed);
                tokio::select! {biased;_=stopped(&mut stop)=>break,_=sleep(backend.database.config.retry_delay)=>{}}
                continue;
            }
        };
        if messages.is_empty() {
            tokio::select! {biased;_=stopped(&mut stop)=>break,_=sleep(Duration::from_millis(backend.database.config.poll_interval_ms as u64))=>{}}
        }
        for LeasedMessage {
            id: msg_id,
            message,
        } in messages
        {
            match message {
                Ok(message) if !message.is_local_only() => {
                    let delivered = tokio::select! {biased;_=stopped(&mut stop)=>return,result=hub.deliver_wait(message)=>result};
                    if delivered.is_err() {
                        return;
                    }
                }
                Ok(_) => {
                    let _ = events.send(PgmqEvent::InvalidMessage {
                        msg_id,
                        error: "local-only message received from PostgreSQL".into(),
                    });
                }
                Err(error) => {
                    let _ = events.send(PgmqEvent::InvalidMessage {
                        msg_id,
                        error: error.to_string(),
                    });
                }
            }
            // Like Python, poison messages are acknowledged after reporting.
            // Valid messages are acknowledged after bounded queue admission,
            // not after application processing (duplicates remain possible).
            let ack = tokio::select! {biased;_=stopped(&mut stop)=>return,result=backend.archive(&queue,msg_id)=>result};
            if !matches!(ack, Ok(true)) {
                let _ = events.send(PgmqEvent::AcknowledgeFailed { msg_id });
            }
        }
    }
    hub.close();
}
