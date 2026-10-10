//! Bounded forwarding between the native worker bus and network transports.
//! A publication is attempted once. Transport ambiguity stops this attachment;
//! no job is replayed, and a network acknowledgement is not execution completion.
use crate::{
    ConnectionState, DecodedMessage, MessageEnvelope, NetworkSubscription, PythonJsonCodec,
    RedisBus, RedisEvent, REGISTRY_ENTRY_TYPE,
};
use pipecat_workers::{
    BusError, LocalBus, MessageData, WorkerReady, WorkerRegistration, WorkerRegistry,
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    sync::{watch, Mutex},
    task::JoinSet,
    time::Instant,
};

static NEXT_ATTACHMENT: AtomicU64 = AtomicU64::new(1);
const ORIGIN: &str = "__native_bus_origin";
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ForwardError {
    #[error("network transport failed; delivery may be ambiguous: {0}")]
    Transport(String),
    #[error("network envelope cannot be admitted: {0}")]
    Protocol(String),
    #[error(transparent)]
    Bus(#[from] BusError),
    #[error("forwarding attachment stopped")]
    Stopped,
    #[error("invalid forwarding configuration: {0}")]
    Configuration(&'static str),
}
/// The transport supplies its existing bounded subscriber and one-shot send.
/// Implementations must never automatically retry a possibly delivered publish.
#[pipecat_core::async_trait]
pub trait WorkerNetworkTransport: Send + Sync + 'static {
    fn subscribe_workers(&self) -> Result<NetworkSubscription, ForwardError>;
    async fn publish_workers(&self, envelope: &MessageEnvelope) -> Result<(), ForwardError>;
    /// Create the loss monitor synchronously, before any forwarding begins.
    fn loss_monitor(&self) -> Pin<Box<dyn Future<Output = ForwardError> + Send>>;
}
#[pipecat_core::async_trait]
impl WorkerNetworkTransport for RedisBus {
    fn subscribe_workers(&self) -> Result<NetworkSubscription, ForwardError> {
        self.subscribe()
            .map_err(|error| ForwardError::Transport(error.to_string()))
    }
    async fn publish_workers(&self, envelope: &MessageEnvelope) -> Result<(), ForwardError> {
        self.send_envelope(envelope)
            .await
            .map(|_| ())
            .map_err(|error| ForwardError::Transport(error.to_string()))
    }
    fn loss_monitor(&self) -> Pin<Box<dyn Future<Output = ForwardError> + Send>> {
        let mut state = self.connection_state();
        let mut events = self.events();
        Box::pin(async move {
            loop {
                if *state.borrow_and_update() != ConnectionState::Connected {
                    return ForwardError::Transport(
                        "Redis subscription interrupted; pending jobs were not replayed".into(),
                    );
                }
                tokio::select! {
                    change = state.changed() => if change.is_err() { return ForwardError::Stopped; },
                    event = events.recv() => match event {
                        Ok(RedisEvent::Reconnected) => {},
                        other => return ForwardError::Transport(format!("Redis receive continuity lost: {other:?}")),
                    }
                }
            }
        })
    }
}
#[cfg(feature = "pgmq")]
#[pipecat_core::async_trait]
impl WorkerNetworkTransport for crate::PgmqBus {
    fn subscribe_workers(&self) -> Result<NetworkSubscription, ForwardError> {
        self.subscribe()
            .map_err(|error| ForwardError::Transport(error.to_string()))
    }
    async fn publish_workers(&self, envelope: &MessageEnvelope) -> Result<(), ForwardError> {
        let report = self
            .send_envelope(envelope)
            .await
            .map_err(|error| ForwardError::Transport(error.to_string()))?;
        if report.failed_peers.is_empty() {
            Ok(())
        } else {
            Err(ForwardError::Transport(format!(
                "PGMQ partial fanout: {} failed peers",
                report.failed_peers.len()
            )))
        }
    }
    fn loss_monitor(&self) -> Pin<Box<dyn Future<Output = ForwardError> + Send>> {
        let mut events = self.events();
        Box::pin(async move {
            ForwardError::Transport(format!(
                "PGMQ receive continuity lost: {:?}",
                events.recv().await
            ))
        })
    }
}
#[derive(Debug, Clone)]
pub struct WorkerNetworkOptions {
    pub codec: PythonJsonCodec,
    pub max_remote_runners: usize,
    pub max_workers_per_runner: usize,
    /// Periodic discovery also lets a newly attached peer find existing workers.
    pub registry_interval: Duration,
    /// Optional lease for peers that announce periodically. Leave None for source
    /// Python hosts, which do not promise periodic announcements.
    pub peer_timeout: Option<Duration>,
}
impl Default for WorkerNetworkOptions {
    fn default() -> Self {
        Self {
            codec: PythonJsonCodec::default(),
            max_remote_runners: 128,
            max_workers_per_runner: 1024,
            registry_interval: Duration::from_secs(5),
            peer_timeout: None,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ForwardState {
    Starting,
    Running,
    Stopped(Result<(), ForwardError>),
}
/// One attachment owns bounded local egress lanes and network ingress tasks.
/// Drop cancels forwarding and closes local admission, resolving pending RPC.
/// The underlying Redis/PGMQ connection remains caller-owned.
pub struct WorkerNetworkAdapter {
    bus: LocalBus,
    stop: watch::Sender<bool>,
    state: watch::Receiver<ForwardState>,
}
impl WorkerNetworkAdapter {
    pub fn attach<T: WorkerNetworkTransport>(
        bus: LocalBus,
        registry: WorkerRegistry,
        transport: Arc<T>,
        options: WorkerNetworkOptions,
    ) -> Result<Self, ForwardError> {
        if options.max_remote_runners == 0
            || options.max_workers_per_runner == 0
            || options.registry_interval.is_zero()
            || options
                .peer_timeout
                .is_some_and(|timeout| timeout <= options.registry_interval)
        {
            return Err(ForwardError::Configuration(
                "nonzero limits and interval required; lease must exceed interval",
            ));
        }
        let outbound =
            Arc::new(bus.subscribe_outbound(format!("{}/network-egress", registry.runner_name()))?);
        let inbound = Arc::new(transport.subscribe_workers()?);
        inbound
            .enable_native_control()
            .map_err(|error| ForwardError::Protocol(error.to_string()))?;
        let loss = transport.loss_monitor();
        let origin = format!(
            "{}:{}:{}",
            registry.runner_name(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos(),
            NEXT_ATTACHMENT.fetch_add(1, Ordering::Relaxed)
        );
        let (stop, mut stop_rx) = watch::channel(false);
        let (state_tx, state) = watch::channel(ForwardState::Starting);
        let transport: Arc<dyn WorkerNetworkTransport> = transport;
        let handle = Self {
            bus: bus.clone(),
            stop,
            state,
        };
        tokio::spawn(async move {
            let peers = Arc::new(Mutex::new(BTreeMap::<String, Instant>::new()));
            let mut tasks: JoinSet<Result<(), ForwardError>> = JoinSet::new();
            let mut egress: JoinSet<Result<(), ForwardError>> = JoinSet::new();
            // Separate bounded lanes keep cancellation live while normal local
            // admission is backpressured. Transport sockets retain their own limits.
            for system in [true, false] {
                let (outbound, inbound, bus, registry, transport, origin, options, peers) = (
                    outbound.clone(),
                    inbound.clone(),
                    bus.clone(),
                    registry.clone(),
                    transport.clone(),
                    origin.clone(),
                    options.clone(),
                    peers.clone(),
                );
                let out_bus = bus.clone();
                let out_transport = transport.clone();
                let out_codec = options.codec.clone();
                let out_origin = origin.clone();
                egress.spawn(async move {
                    loop {
                        let message = if system {
                            outbound.recv_system().await?
                        } else {
                            outbound.recv_data().await?
                        };
                        let Some(message) = message else {
                            return Ok(());
                        };
                        // Local lookup wins, including synthetic per-job reply names.
                        if let Some(target) = message.target.as_deref() {
                            if out_bus.has_target_subscriber(target)? {
                                continue;
                            }
                        }
                        let envelope = tag_origin(
                            &out_codec,
                            out_codec.from_bus_message(&message).map_err(protocol)?,
                            &out_origin,
                        )?;
                        out_transport.publish_workers(&envelope).await?;
                    }
                });
                tasks.spawn(async move {
                    loop {
                        let envelope = if system {
                            inbound.recv_system().await
                        } else {
                            inbound.recv_data().await
                        }
                        .map_err(|error| ForwardError::Transport(error.to_string()))?;
                        let Some(envelope) = envelope else {
                            return Err(ForwardError::Stopped);
                        };
                        if envelope
                            .wire_value()
                            .get(ORIGIN)
                            .and_then(|value| value.as_str())
                            == Some(&origin)
                        {
                            continue;
                        }
                        match envelope.decoded() {
                            DecodedMessage::Registry { runner, workers } => {
                                if runner == registry.runner_name() {
                                    return Err(ForwardError::Protocol(
                                        "duplicate remote runner identity".into(),
                                    ));
                                }
                                if workers.len() > options.max_workers_per_runner {
                                    return Err(ForwardError::Protocol(
                                        "remote registry exceeds worker limit".into(),
                                    ));
                                }
                                let mut known = peers.lock().await;
                                let new = !known.contains_key(runner);
                                if new && known.len() >= options.max_remote_runners {
                                    return Err(ForwardError::Protocol(
                                        "remote registry exceeds runner limit".into(),
                                    ));
                                }
                                known.insert(runner.clone(), Instant::now());
                                drop(known);
                                registry.replace_remote(
                                    runner,
                                    workers
                                        .iter()
                                        .map(|entry| WorkerRegistration {
                                            ready: WorkerReady {
                                                worker_name: entry.name.clone(),
                                                runner: runner.clone(),
                                            },
                                            parent: entry.parent.clone(),
                                            active: entry.active,
                                            bridged: entry.bridged,
                                        })
                                        .collect(),
                                );
                                if new {
                                    transport
                                        .publish_workers(&registry_envelope(
                                            &options.codec,
                                            &registry,
                                            &origin,
                                            false,
                                        )?)
                                        .await?;
                                }
                            }
                            DecodedMessage::Bus(message) => {
                                if let MessageData::Ready {
                                    runner,
                                    parent,
                                    active,
                                    bridged,
                                } = &message.data
                                {
                                    if runner != registry.runner_name() {
                                        let mut known = peers.lock().await;
                                        if !known.contains_key(runner)
                                            && known.len() >= options.max_remote_runners
                                        {
                                            return Err(ForwardError::Protocol(
                                                "remote registry exceeds runner limit".into(),
                                            ));
                                        }
                                        known.insert(runner.clone(), Instant::now());
                                        drop(known);
                                        let entries = registry.entries(runner);
                                        if entries.len() >= options.max_workers_per_runner
                                            && !entries.iter().any(|entry| {
                                                entry.ready.worker_name == message.source
                                            })
                                        {
                                            return Err(ForwardError::Protocol(
                                                "remote registry exceeds worker limit".into(),
                                            ));
                                        }
                                        registry.register_entry(WorkerRegistration {
                                            ready: WorkerReady {
                                                worker_name: message.source.clone(),
                                                runner: runner.clone(),
                                            },
                                            parent: parent.clone(),
                                            active: *active,
                                            bridged: *bridged,
                                        });
                                    }
                                }
                                bus.inject_wait(message.clone()).await?;
                            }
                            DecodedMessage::Base => {}
                            _ => {
                                return Err(ForwardError::Protocol(format!(
                                    "unsupported runtime message: {}",
                                    envelope.type_name()
                                )))
                            }
                        }
                    }
                });
            }
            let (
                announcement_transport,
                announcement_registry,
                announcement_options,
                announcement_origin,
                announcement_peers,
            ) = (
                transport.clone(),
                registry.clone(),
                options.clone(),
                origin.clone(),
                peers.clone(),
            );
            let final_state = state_tx.clone();
            tasks.spawn(async move {
                let mut revision = announcement_registry.local_changes();
                let mut interval = tokio::time::interval(announcement_options.registry_interval);
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! { _ = interval.tick() => {}, _ = revision.changed() => {} }
                    announcement_transport
                        .publish_workers(&registry_envelope(
                            &announcement_options.codec,
                            &announcement_registry,
                            &announcement_origin,
                            false,
                        )?)
                        .await?;
                    state_tx.send_replace(ForwardState::Running);
                    if let Some(lease) = announcement_options.peer_timeout {
                        let mut peers = announcement_peers.lock().await;
                        peers.retain(|runner, seen| {
                            if seen.elapsed() >= lease {
                                announcement_registry.remove_runner(runner);
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
            });
            // Retain a separate state sender outside the announcement task.
            let outcome = tokio::select! { biased;
                _ = stop_rx.changed() => {
                    // Close new local admission, stop incoming traffic and registry
                    // announcements, then drain each admitted outbound lane once.
                    let _ = bus.close();
                    tasks.abort_all();
                    while tasks.join_next().await.is_some() {}
                    let mut failure = None;
                    while let Some(result) = egress.join_next().await {
                        match result {
                            Ok(Ok(())) => {},
                            Ok(Err(error)) => { failure.get_or_insert(error); },
                            Err(error) => { failure.get_or_insert(ForwardError::Transport(error.to_string())); },
                        }
                    }
                    match failure {
                        Some(error) => Err(error),
                        None => match registry_envelope(&options.codec, &registry, &origin, true) {
                            Ok(envelope) => transport.publish_workers(&envelope).await,
                            Err(error) => Err(error),
                        },
                    }
                },
                error = loss => Err(error),
                result = egress.join_next() => match result { Some(Ok(Err(error))) => Err(error), Some(Err(error)) => Err(ForwardError::Transport(error.to_string())), _ => Err(ForwardError::Bus(BusError::Closed)) },
                result = tasks.join_next() => match result { Some(Ok(result)) => result, Some(Err(error)) => Err(ForwardError::Transport(error.to_string())), None => Err(ForwardError::Stopped) },
            };
            tasks.abort_all();
            egress.abort_all();
            while tasks.join_next().await.is_some() {}
            while egress.join_next().await.is_some() {}
            let _ = bus.close();
            for runner in peers.lock().await.keys() {
                registry.remove_runner(runner);
            }
            final_state.send_replace(ForwardState::Stopped(outcome));
        });
        Ok(handle)
    }
    pub fn watch(&self) -> watch::Receiver<ForwardState> {
        self.state.clone()
    }
    pub async fn ready(&mut self) -> Result<(), ForwardError> {
        loop {
            match self.state.borrow_and_update().clone() {
                ForwardState::Running => return Ok(()),
                ForwardState::Stopped(result) => return result.and(Err(ForwardError::Stopped)),
                _ => {}
            }
            self.state
                .changed()
                .await
                .map_err(|_| ForwardError::Stopped)?;
        }
    }
    /// Close new local admission, drain admitted outbound messages, and publish
    /// a final empty registry. Stop workers first for graceful job completion.
    /// The caller remains responsible for shutting down its network connection.
    pub async fn shutdown(&mut self) -> Result<(), ForwardError> {
        self.stop.send_replace(true);
        self.wait().await
    }
    pub async fn wait(&mut self) -> Result<(), ForwardError> {
        loop {
            if let ForwardState::Stopped(result) = self.state.borrow_and_update().clone() {
                return result;
            }
            self.state
                .changed()
                .await
                .map_err(|_| ForwardError::Stopped)?;
        }
    }
}
impl Drop for WorkerNetworkAdapter {
    fn drop(&mut self) {
        self.stop.send_replace(true);
        let _ = self.bus.close();
    }
}
fn protocol(error: crate::CodecError) -> ForwardError {
    ForwardError::Protocol(error.to_string())
}
fn tag_origin(
    codec: &PythonJsonCodec,
    envelope: MessageEnvelope,
    origin: &str,
) -> Result<MessageEnvelope, ForwardError> {
    let mut wire = envelope.wire_value().clone();
    wire[ORIGIN] = origin.into();
    codec.decode_value(wire).map_err(protocol)
}
fn registry_envelope(
    codec: &PythonJsonCodec,
    registry: &WorkerRegistry,
    origin: &str,
    empty: bool,
) -> Result<MessageEnvelope, ForwardError> {
    let entries = if empty {
        vec![]
    } else {
        registry.entries(registry.runner_name())
    };
    let workers: Vec<_> = entries.into_iter().map(|entry| json!({"__type__": REGISTRY_ENTRY_TYPE, "__data__": {
        "name": entry.ready.worker_name, "parent": entry.parent, "active": entry.active, "bridged": entry.bridged, "started_at": null
    }})).collect();
    let wire = json!({"__type__": "pipecat.bus.messages.BusWorkerRegistryMessage", "__data__": {
        "source": registry.runner_name(), "runner": registry.runner_name(), "workers": workers
    }, ORIGIN: origin});
    codec.decode_value(wire).map_err(protocol)
}
