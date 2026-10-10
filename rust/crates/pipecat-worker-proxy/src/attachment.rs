use crate::{
    routing::{admit_request, allowed, finish_response, Pending},
    ProxyError, ProxyOptions,
};
use pipecat_network_bus::{DecodedMessage, MessageEnvelope, REGISTRY_ENTRY_TYPE};
use pipecat_transports::{Packet, SessionSender, WebSocketSession};
use pipecat_workers::{
    BusMessage, JobStatus, LocalBus, MessageData, RemoteEntryClaim, WorkerReady,
    WorkerRegistration, WorkerRegistry,
};
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{broadcast, mpsc, watch},
    task::JoinSet,
    time::timeout,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProxyState {
    Connected,
    Stopped {
        result: Result<(), ProxyError>,
        cleanup_failed: bool,
    },
}
#[derive(Debug, Clone)]
pub enum ProxyEvent {
    Rejected {
        source: String,
        target: Option<String>,
    },
    /// Base/flush messages have no native bus projection; an application may consume this envelope.
    Unprojected(MessageEnvelope),
    ProjectionWarning(Vec<&'static str>),
    Disconnected {
        result: Result<(), ProxyError>,
        cleanup_failed: bool,
    },
}
#[derive(Clone)]
struct RemoteRegistration {
    runner: String,
    claim: RemoteEntryClaim,
}
/// Owns one socket lifetime. Drop requests cleanup; shutdown/wait observe its result.
/// Terminal state is retained independently of the lossy diagnostic event ring.
pub struct ProxyAttachment {
    stop: watch::Sender<bool>,
    state: watch::Receiver<ProxyState>,
    events: broadcast::Sender<ProxyEvent>,
}
impl ProxyAttachment {
    pub(crate) fn attach(
        session: WebSocketSession,
        bus: LocalBus,
        registry: WorkerRegistry,
        options: ProxyOptions,
        server: bool,
    ) -> Result<Self, ProxyError> {
        options.validate()?;
        let outbound =
            Arc::new(bus.subscribe_outbound(format!("{}/websocket-egress", options.name))?);
        let pending = Arc::new(Mutex::new(Pending::default()));
        let remote_runner = Arc::new(Mutex::new(None::<RemoteRegistration>));
        let (stop, mut stopped) = watch::channel(false);
        let (state_tx, state) = watch::channel(ProxyState::Connected);
        let (events, _) = broadcast::channel(options.event_capacity);
        let handle = Self {
            stop,
            state,
            events: events.clone(),
        };
        tokio::spawn(async move {
            let sender = session.sender();
            let mut session = session;
            let mut tasks = JoinSet::new();
            let (system_tx, system_rx) =
                mpsc::channel::<BusMessage>(options.session.queue_capacity);
            let (data_tx, data_rx) = mpsc::channel::<BusMessage>(options.session.queue_capacity);
            for mut incoming in [system_rx, data_rx] {
                let (bus, options, pending) = (bus.clone(), options.clone(), pending.clone());
                tasks.spawn(async move {
                    while let Some(message) = incoming.recv().await {
                        timeout(options.admission_timeout, bus.inject_wait(message.clone()))
                            .await
                            .map_err(|_| ProxyError::Timeout("local bus admission"))??;
                        finish_response(
                            &message,
                            &mut pending.lock().expect("pending lock poisoned"),
                            true,
                        );
                    }
                    Err::<(), ProxyError>(ProxyError::Disconnected)
                });
            }
            for system in [true, false] {
                let (outbound, pending, sender, options, events) = (
                    outbound.clone(),
                    pending.clone(),
                    sender.clone(),
                    options.clone(),
                    events.clone(),
                );
                tasks.spawn(async move {
                    loop {
                        let message = if system {
                            outbound.recv_system().await?
                        } else {
                            outbound.recv_data().await?
                        };
                        let Some(message) = message else {
                            return Err::<(), ProxyError>(ProxyError::Bus("closed".into()));
                        };
                        {
                            let mut pending = pending.lock().expect("pending lock poisoned");
                            if !allowed(&message, &options, &pending, false) {
                                continue;
                            }
                            admit_request(&message, &mut pending, false, options.max_pending_jobs)?;
                        }
                        let envelope = options.codec.from_bus_message(&message)?;
                        let warnings = envelope.projection_warnings();
                        if !warnings.is_empty() {
                            let _ = events.send(ProxyEvent::ProjectionWarning(warnings));
                        }
                        send(&sender, &options, &envelope).await?;
                        finish_response(
                            &message,
                            &mut pending.lock().expect("pending lock poisoned"),
                            false,
                        );
                    }
                });
            }
            if server {
                let (sender, registry, options) =
                    (sender.clone(), registry.clone(), options.clone());
                tasks.spawn(async move {
                    let mut revision = registry.local_changes();
                    loop {
                        revision.borrow_and_update();
                        send(&sender, &options, &registry_envelope(&registry, &options)?).await?;
                        revision.changed().await.map_err(|_| ProxyError::Stopped)?;
                    }
                });
            }
            let result = tokio::select! {
                biased;
                _ = stopped.changed() => Ok(()),
                closed = bus.closed() => { closed.map_err(ProxyError::from).and(Err(ProxyError::Bus("closed".into()))) },
                outcome = tasks.join_next() => match outcome {
                    Some(Ok(outcome)) => outcome,
                    Some(Err(error)) => Err(ProxyError::Transport(error.to_string())),
                    None => Err(ProxyError::Stopped),
                },
                outcome = ingress(&mut session, &registry, &options, &pending, &remote_runner, &events, server, &system_tx, &data_tx) => outcome,
            };
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
            if let Some(owner) = remote_runner.lock().expect("registry lock poisoned").take() {
                registry.release_remote_entry(owner.claim);
            }
            // Snapshot already admitted egress requests without retaining a disconnected queue.
            for _ in 0..bus.capacity_per_priority().saturating_mul(2) {
                let Ok(Some(message)) = outbound.try_recv() else {
                    break;
                };
                let mut pending = pending.lock().expect("pending lock poisoned");
                if allowed(&message, &options, &pending, false) {
                    if let MessageData::JobRequest { job_id, .. } = &message.data {
                        pending
                            .outgoing
                            .insert((message.source.clone(), job_id.clone()));
                    }
                }
            }
            drop(outbound);
            let transport_result = session.cancel().await.map_err(ProxyError::from);
            let result = result.and(transport_result);
            let work = {
                let mut pending = pending.lock().expect("pending lock poisoned");
                std::mem::take(&mut *pending)
            };
            let cleanup = async {
                for (target, job_id) in work.outgoing.into_iter().chain(work.rejected_outgoing) {
                    bus.inject_wait(BusMessage {source: options.remote_worker.clone(), target: Some(target), data: MessageData::JobResponse {
                        job_id, status: JobStatus::Error, response: Some(json!({"error": "proxy disconnected; execution may have occurred; request was not replayed"})), urgent: true,
                    }}).await?;
                }
                for (source, job_id) in work.incoming {
                    bus.inject_wait(BusMessage {
                        source,
                        target: Some(options.local_worker.clone()),
                        data: MessageData::JobCancel {
                            job_id,
                            reason: Some("remote proxy disconnected".into()),
                        },
                    })
                    .await?;
                }
                Ok::<(), ProxyError>(())
            };
            let cleanup_failed =
                !matches!(timeout(options.cleanup_timeout, cleanup).await, Ok(Ok(())));
            let _ = events.send(ProxyEvent::Disconnected {
                result: result.clone(),
                cleanup_failed,
            });
            state_tx.send_replace(ProxyState::Stopped {
                result,
                cleanup_failed,
            });
        });
        Ok(handle)
    }
    pub fn watch(&self) -> watch::Receiver<ProxyState> {
        self.state.clone()
    }
    /// Broadcast receivers explicitly report Lagged; diagnostics never drive job completion.
    pub fn events(&self) -> broadcast::Receiver<ProxyEvent> {
        self.events.subscribe()
    }
    pub fn is_stopped(&self) -> bool {
        matches!(*self.state.borrow(), ProxyState::Stopped { .. })
    }
    pub async fn wait(&mut self) -> Result<(), ProxyError> {
        loop {
            if let ProxyState::Stopped {
                result,
                cleanup_failed,
            } = self.state.borrow_and_update().clone()
            {
                return if cleanup_failed && result.is_ok() {
                    Err(ProxyError::Timeout("pending job cleanup"))
                } else {
                    result
                };
            }
            self.state
                .changed()
                .await
                .map_err(|_| ProxyError::Stopped)?;
        }
    }
    pub async fn shutdown(&mut self) -> Result<(), ProxyError> {
        self.stop.send_replace(true);
        self.wait().await
    }
}
impl Drop for ProxyAttachment {
    fn drop(&mut self) {
        self.stop.send_replace(true);
    }
}
async fn send(
    sender: &SessionSender,
    options: &ProxyOptions,
    envelope: &MessageEnvelope,
) -> Result<(), ProxyError> {
    let bytes = options.codec.encode(envelope)?;
    timeout(
        options.admission_timeout,
        sender.send(Packet::Binary(bytes.into())),
    )
    .await
    .map_err(|_| ProxyError::Timeout("WebSocket queue admission"))??;
    Ok(())
}
#[allow(clippy::too_many_arguments)]
async fn ingress(
    session: &mut WebSocketSession,
    registry: &WorkerRegistry,
    options: &ProxyOptions,
    pending: &Mutex<Pending>,
    remote_runner: &Mutex<Option<RemoteRegistration>>,
    events: &broadcast::Sender<ProxyEvent>,
    server: bool,
    system_tx: &mpsc::Sender<BusMessage>,
    data_tx: &mpsc::Sender<BusMessage>,
) -> Result<(), ProxyError> {
    while let Some(packet) = session.recv().await {
        let bytes = match &packet {
            Packet::Text(text) => text.as_bytes(),
            Packet::Binary(bytes) => bytes.as_ref(),
        };
        let envelope = options.codec.decode(bytes)?;
        match envelope.decoded() {
            DecodedMessage::Registry { runner, workers } if !server => {
                if runner == registry.runner_name()
                    || runner.is_empty()
                    || workers.len() > 1
                    || workers
                        .iter()
                        .any(|entry| entry.name != options.remote_worker)
                    || options
                        .remote_runner
                        .as_ref()
                        .is_some_and(|expected| expected != runner)
                {
                    return Err(ProxyError::Protocol(
                        "unexpected registry identity or worker set".into(),
                    ));
                }
                let mut owner = remote_runner.lock().expect("registry lock poisoned");
                if owner
                    .as_ref()
                    .is_some_and(|previous| previous.runner != *runner)
                {
                    return Err(ProxyError::Protocol("peer changed runner identity".into()));
                }
                let replacement = workers.first().map(|entry| WorkerRegistration {
                    ready: WorkerReady {
                        worker_name: entry.name.clone(),
                        runner: runner.clone(),
                    },
                    parent: entry.parent.clone(),
                    active: entry.active,
                    bridged: entry.bridged,
                });
                if let Some(owner) = owner.as_ref() {
                    if !registry.update_remote_entry(&owner.claim, replacement) {
                        return Err(ProxyError::Protocol(
                            "remote worker ownership was invalidated".into(),
                        ));
                    }
                } else {
                    let claim = registry
                        .claim_remote_entry(runner, &options.remote_worker, replacement)
                        .ok_or_else(|| {
                            ProxyError::Protocol(
                                "remote worker is owned by another attachment".into(),
                            )
                        })?;
                    *owner = Some(RemoteRegistration {
                        runner: runner.clone(),
                        claim,
                    });
                }
            }
            DecodedMessage::Bus(message) => {
                {
                    let mut pending = pending.lock().expect("pending lock poisoned");
                    if !allowed(message, options, &pending, true) {
                        let _ = events.send(ProxyEvent::Rejected {
                            source: message.source.clone(),
                            target: message.target.clone(),
                        });
                        continue;
                    }
                    admit_request(message, &mut pending, true, options.max_pending_jobs)?;
                }
                let warnings = envelope.projection_warnings();
                if !warnings.is_empty() {
                    let _ = events.send(ProxyEvent::ProjectionWarning(warnings));
                }
                let lane = if message.data.is_system() {
                    system_tx
                } else {
                    data_tx
                };
                lane.try_send(message.clone())
                    .map_err(|_| ProxyError::Capacity("inbound priority lane"))?;
            }
            DecodedMessage::Base | DecodedMessage::FlushProgress { .. }
                if envelope.source() == options.remote_worker
                    && envelope.target() == Some(&options.local_worker) =>
            {
                let _ = events.send(ProxyEvent::Unprojected(envelope));
            }
            DecodedMessage::LocalError { .. } => {
                return Err(ProxyError::Protocol("local-only message from peer".into()))
            }
            _ => {
                let _ = events.send(ProxyEvent::Rejected {
                    source: envelope.source().into(),
                    target: envelope.target().map(str::to_owned),
                });
            }
        }
    }
    Err(ProxyError::Disconnected)
}
fn registry_envelope(
    registry: &WorkerRegistry,
    options: &ProxyOptions,
) -> Result<MessageEnvelope, ProxyError> {
    let workers = registry.entries(registry.runner_name()).into_iter().filter(|entry| entry.ready.worker_name == options.local_worker).map(|entry| json!({
        "__type__": REGISTRY_ENTRY_TYPE, "__data__": {"name":entry.ready.worker_name,"parent":entry.parent,"active":entry.active,"bridged":entry.bridged,"started_at":null}
    })).collect::<Vec<_>>();
    Ok(options.codec.decode_value(json!({"__type__":"pipecat.bus.messages.BusWorkerRegistryMessage", "__data__": {
        "source": options.name, "target":null, "runner":registry.runner_name(), "workers":workers
    }}))?)
}
