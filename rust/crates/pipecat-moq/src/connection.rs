use crate::{config::TlsTrust, media, transcript, wire, AudioRawFrame, Error, MoqConfig, Result};
use serde_json::Value;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    sync::{mpsc, oneshot, watch, Notify},
    task::{JoinHandle, JoinSet},
};

#[cfg(test)]
mod lifecycle_tests;

#[derive(Debug, Clone)]
pub enum MoqEvent {
    PeerConnected,
    Audio {
        audio: AudioRawFrame,
        timestamp_us: u64,
    },
    Message(Value),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Shutdown {
    Running,
    End,
    Cancel,
}

struct Shared {
    accepting: Mutex<bool>,
    shutdown: watch::Sender<Shutdown>,
    interrupt: watch::Sender<u64>,
    tasks: AtomicUsize,
    idle: Notify,
}
impl Shared {
    /// Linearize the last local publication step with interruption and Cancel.
    /// End closes admission but permits already admitted writes to drain.
    fn commit(&self, generation: Option<u64>, publish: impl FnOnce() -> Result<()>) -> Result<()> {
        let _accepting = self.accepting.lock().unwrap_or_else(|e| e.into_inner());
        if *self.shutdown.borrow() == Shutdown::Cancel {
            return Err(Error::Closed);
        }
        if generation.is_some_and(|expected| expected != *self.interrupt.borrow()) {
            return Err(Error::Interrupted);
        }
        publish()
    }

    fn stop(&self, how: Shutdown) {
        let mut accepting = self.accepting.lock().unwrap_or_else(|e| e.into_inner());
        *accepting = false;
        if *self.shutdown.borrow() != Shutdown::Cancel {
            self.shutdown.send_replace(how);
        }
    }
}
struct TaskGuard(Arc<Shared>);
impl TaskGuard {
    fn new(shared: &Arc<Shared>) -> Self {
        shared.tasks.fetch_add(1, Ordering::AcqRel);
        Self(shared.clone())
    }
}
impl Drop for TaskGuard {
    fn drop(&mut self) {
        self.0.tasks.fetch_sub(1, Ordering::AcqRel);
        self.0.idle.notify_waiters();
    }
}
struct DoneGuard(watch::Sender<Option<Result<()>>>);
impl Drop for DoneGuard {
    fn drop(&mut self) {
        if self.0.borrow().is_none() {
            self.0.send_replace(Some(Err(Error::Closed)));
        }
    }
}

/// A monitor does not keep the network connection or its tasks alive.
#[derive(Clone)]
pub struct MoqMonitor {
    shared: Arc<Shared>,
    done: watch::Receiver<Option<Result<()>>>,
}
impl MoqMonitor {
    pub fn active_tasks(&self) -> usize {
        self.shared.tasks.load(Ordering::Acquire)
    }
    pub fn result(&self) -> Option<Result<()>> {
        self.done.borrow().clone()
    }
    pub async fn wait_closed(&mut self) -> Result<()> {
        loop {
            if let Some(result) = self.done.borrow_and_update().clone() {
                return result;
            }
            self.done.changed().await.map_err(|_| Error::Closed)?;
        }
    }
    /// Wait for all application-owned tasks, including aborted Drop cleanup.
    pub async fn wait_idle(&self) {
        loop {
            let notified = self.shared.idle.notified();
            if self.active_tasks() == 0 {
                return;
            }
            notified.await;
        }
    }
}

enum Content {
    Audio(AudioRawFrame),
    Message(Value),
}
struct Command {
    content: Content,
    generation: u64,
    reply: oneshot::Sender<Result<()>>,
    fenced: bool,
}

/// Cloning a sender does not prolong the connection owner's lifetime.
#[derive(Clone)]
pub struct MoqSender {
    shared: Arc<Shared>,
    tx: mpsc::Sender<Command>,
    limits: crate::MoqLimits,
    audio_enabled: bool,
}
impl MoqSender {
    pub(crate) fn ensure_audio_output(&self) -> Result<()> {
        if !self.audio_enabled {
            return Err(Error::Unsupported("audio output is disabled"));
        }
        Ok(())
    }
    pub(crate) fn validate_json(&self, message: &Value) -> Result<()> {
        if !message.is_object() {
            return Err(Error::InvalidConfig("transcript message must be an object"));
        }
        if message.get("label").and_then(Value::as_str) == Some("moq-transport") {
            return Err(Error::InvalidConfig("transport control label is reserved"));
        }
        transcript::json_bytes(
            message,
            self.limits.expanded_record_bytes.saturating_sub(256),
        )?;
        Ok(())
    }
    async fn publish(&self, content: Content, generation: Option<u64>) -> Result<()> {
        let (reply, receiver) = oneshot::channel();
        {
            let accepting = self
                .shared
                .accepting
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            if !*accepting {
                return Err(Error::Closed);
            }
            let current = *self.shared.interrupt.borrow();
            if generation.is_some_and(|expected| expected != current) {
                return Err(Error::Interrupted);
            }
            self.tx
                .try_send(Command {
                    content,
                    generation: current,
                    reply,
                    fenced: generation.is_some(),
                })
                .map_err(|e| match e {
                    mpsc::error::TrySendError::Full(_) => Error::Backpressure,
                    mpsc::error::TrySendError::Closed(_) => Error::Closed,
                })?;
        }
        receiver.await.map_err(|_| Error::Closed)?
    }
    /// Publish exactly one 20 ms PCM frame. Success is local publication, not playback.
    pub async fn publish_audio(&self, audio: AudioRawFrame) -> Result<()> {
        self.ensure_audio_output()?;
        media::validate(&audio)?;
        self.publish(Content::Audio(audio), None).await
    }
    /// Publish a JSON object, assigning transport-owned sequence and epoch fields.
    pub async fn publish_json(&self, message: Value) -> Result<()> {
        self.validate_json(&message)?;
        self.publish(Content::Message(message), None).await
    }
    /// Current local interruption generation. This is not a playback acknowledgement.
    pub fn generation(&self) -> u64 {
        *self.shared.interrupt.borrow()
    }
    /// Publish only while the caller's generation still owns admission and the write.
    pub async fn publish_audio_for_generation(
        &self,
        audio: AudioRawFrame,
        generation: u64,
    ) -> Result<()> {
        self.ensure_audio_output()?;
        media::validate(&audio)?;
        self.publish(Content::Audio(audio), Some(generation)).await
    }
    /// Generation-fenced JSON for interruptible pipeline messages. The ordinary
    /// `publish_json` method remains an interruption-independent append log.
    pub async fn publish_json_for_generation(&self, message: Value, generation: u64) -> Result<()> {
        self.validate_json(&message)?;
        self.publish(Content::Message(message), Some(generation))
            .await
    }
    /// Re-anchor local pacing. Already transmitted audio cannot be recalled.
    pub async fn interrupt(&self) -> Result<()> {
        self.interrupt_now()
    }
    pub(crate) fn interrupt_now(&self) -> Result<()> {
        self.interrupt_generation(None)
    }
    pub(crate) fn interrupt_if_generation(&self, generation: u64) -> Result<()> {
        self.interrupt_generation(Some(generation))
    }
    fn interrupt_generation(&self, expected: Option<u64>) -> Result<()> {
        let accepting = self
            .shared
            .accepting
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if !*accepting {
            return Err(Error::Closed);
        }
        let current = *self.shared.interrupt.borrow();
        if expected.is_some_and(|expected| expected != current) {
            return Ok(());
        }
        let next = current
            .checked_add(1)
            .ok_or(Error::Limit("interruption generation"))?;
        self.shared.interrupt.send_replace(next);
        Ok(())
    }
}

/// Owns the endpoint, protocol drivers and media pumps. Drop requests immediate cancellation.
pub struct MoqConnection {
    sender: MoqSender,
    events: Option<mpsc::Receiver<MoqEvent>>,
    monitor: MoqMonitor,
    endpoint: moq_noq::Endpoint,
    task: Option<JoinHandle<()>>,
    end_timeout: Duration,
}
impl MoqConnection {
    pub async fn connect(config: MoqConfig) -> Result<Self> {
        let url = config.validate()?;
        let (endpoint, quic) = endpoint(&config)?;
        let mut guard = EndpointGuard(Some(endpoint));
        let endpoint = guard.0.as_ref().expect("endpoint exists");
        let client = web_transport_moq::Client::new(endpoint.clone(), quic);
        let mut request = web_transport_moq::proto::ConnectRequest::new(url);
        request.protocols = vec!["moq-lite-06".into()];
        let transport =
            tokio::time::timeout(config.limits.connect_timeout, client.connect(request))
                .await
                .map_err(|_| Error::Timeout("WebTransport handshake"))?
                .map_err(Error::network)?;
        if transport.protocol() != Some("moq-lite-06") {
            return Err(Error::Unsupported("peer did not negotiate moq-lite-06"));
        }

        let (publish, publish_driver) = origin(&config);
        let (subscribe, subscribe_driver) = origin(&config);
        let allowed = moq_net::Patterns::from(
            config
                .peer_path
                .parse::<moq_net::Pattern>()
                .map_err(Error::protocol)?,
        );
        let incoming_origin = subscribe.scope("", &allowed).map_err(Error::protocol)?;
        let outgoing = Publication::new(&publish, &config)?;
        let (session, driver) = moq_net::Client::new()
            .with_publisher(&publish)
            .with_subscriber(incoming_origin)
            .with_versions(wire::version().into())
            .connect(
                tokio::time::Instant::now().into_std(),
                moq_tokio::transport::Session::new(transport),
            )
            .await
            .map_err(Error::protocol)?;

        let (shutdown, _) = watch::channel(Shutdown::Running);
        let shared = Arc::new(Shared {
            accepting: Mutex::new(true),
            shutdown,
            interrupt: watch::channel(0).0,
            tasks: AtomicUsize::new(0),
            idle: Notify::new(),
        });
        let (tx, rx) = mpsc::channel(config.limits.outgoing_queue);
        let (events, event_rx) = mpsc::channel(config.limits.incoming_queue);
        let (done, done_rx) = watch::channel(None);
        let (ready, ready_rx) = oneshot::channel();
        let sender = MoqSender {
            shared: shared.clone(),
            tx,
            limits: config.limits.clone(),
            audio_enabled: config.audio_out_enabled,
        };
        let monitor = MoqMonitor {
            shared: shared.clone(),
            done: done_rx,
        };
        let mut jobs = JoinSet::new();
        spawn(&mut jobs, &shared, async move {
            moq_net::time::run(publish_driver).await;
            Ok(Exit::Driver)
        });
        spawn(&mut jobs, &shared, async move {
            moq_net::time::run(subscribe_driver).await;
            Ok(Exit::Driver)
        });
        spawn(&mut jobs, &shared, async move {
            let error = moq_net::time::run(driver).await;
            match error {
                moq_net::Error::Closed | moq_net::Error::Cancel | moq_net::Error::Dropped => {
                    Ok(Exit::PeerEnded)
                }
                error => Err(Error::protocol(error)),
            }
        });
        let peer_origin = subscribe.consume();
        let peer_path = config.peer_path.clone();
        spawn(&mut jobs, &shared, async move {
            Ok(Exit::Peer(
                peer_origin
                    .routed_broadcast(peer_path)
                    .await
                    .map_err(Error::protocol)?,
            ))
        });
        let writer_shared = shared.clone();
        spawn(&mut jobs, &shared, async move {
            Ok(Exit::WriterEnded(Box::new(
                writer(outgoing, rx, writer_shared).await?,
            )))
        });
        let endpoint = guard.0.take().expect("endpoint exists");
        let supervisor_endpoint = endpoint.clone();
        let supervisor_shared = shared.clone();
        let task_guard = TaskGuard::new(&shared);
        let end_timeout = config.limits.end_timeout;
        let connect_timeout = config.limits.connect_timeout;
        let task = tokio::spawn(async move {
            let _task_guard = task_guard;
            let done = DoneGuard(done);
            let result =
                supervise(&mut jobs, supervisor_shared.clone(), &config, events, ready).await;
            supervisor_shared.stop(Shutdown::Cancel);
            session.abort(moq_net::Error::Cancel);
            supervisor_endpoint.close(0u32.into(), b"pipecat session closed");
            jobs.abort_all();
            while jobs.join_next().await.is_some() {}
            drop((publish, subscribe));
            done.0.send_replace(Some(result));
        });
        let mut connection = Self {
            sender,
            events: Some(event_rx),
            monitor,
            endpoint,
            task: Some(task),
            end_timeout,
        };
        match tokio::time::timeout(connect_timeout, ready_rx).await {
            Ok(Ok(())) => Ok(connection),
            outcome => {
                let error = match outcome {
                    Err(_) => Error::Timeout("peer announcement"),
                    _ => connection
                        .monitor
                        .result()
                        .and_then(|r| r.err())
                        .unwrap_or(Error::Closed),
                };
                connection.cancel().await?;
                Err(error)
            }
        }
    }
    pub fn sender(&self) -> MoqSender {
        self.sender.clone()
    }
    pub fn monitor(&self) -> MoqMonitor {
        self.monitor.clone()
    }
    pub async fn recv(&mut self) -> Option<MoqEvent> {
        self.events.as_mut()?.recv().await
    }
    /// Transfer event consumption once without transferring connection ownership.
    /// After transfer `recv` returns `None`; End and Cancel remain owned here.
    pub fn take_events(&mut self) -> Option<mpsc::Receiver<MoqEvent>> {
        self.events.take()
    }
    /// Drain already admitted publications, finish tracks, and close within the configured deadline.
    /// No remote playback or delivery acknowledgement is inferred from this operation.
    pub async fn end(&mut self) -> Result<()> {
        self.sender.shared.stop(Shutdown::End);
        let result = match tokio::time::timeout(self.end_timeout, self.monitor.wait_closed()).await
        {
            Ok(result) => result,
            Err(_) => {
                self.cancel().await?;
                return Err(Error::Timeout("end drain"));
            }
        };
        self.join().await?;
        result
    }
    pub async fn cancel(&mut self) -> Result<()> {
        self.sender.shared.stop(Shutdown::Cancel);
        self.endpoint.close(0u32.into(), b"pipecat cancelled");
        self.join().await
    }
    async fn join(&mut self) -> Result<()> {
        let mut result = Ok(());
        if let Some(task) = self.task.as_mut() {
            match tokio::time::timeout(self.end_timeout, &mut *task).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => result = Err(Error::Task(error.to_string())),
                Err(_) => {
                    task.abort();
                    let _ = task.await;
                    result = Err(Error::Timeout("shutdown join"));
                }
            }
            self.task.take();
        }
        self.monitor.wait_idle().await;
        result
    }
}
impl Drop for MoqConnection {
    fn drop(&mut self) {
        self.sender.shared.stop(Shutdown::Cancel);
        self.endpoint.close(0u32.into(), b"pipecat owner dropped");
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}
struct EndpointGuard(Option<moq_noq::Endpoint>);
impl Drop for EndpointGuard {
    fn drop(&mut self) {
        if let Some(endpoint) = &self.0 {
            endpoint.close(0u32.into(), b"pipecat connect cancelled");
        }
    }
}
fn endpoint(config: &MoqConfig) -> Result<(moq_noq::Endpoint, moq_noq::ClientConfig)> {
    let mut roots = rustls::RootCertStore::empty();
    match &config.trust {
        TlsTrust::WebPki => roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned()),
        TlsTrust::Roots(certs) => {
            for cert in certs {
                roots
                    .add(rustls::pki_types::CertificateDer::from(cert.clone()))
                    .map_err(Error::network)?;
            }
        }
        TlsTrust::MutualTls => return Err(Error::Unsupported("mutual TLS")),
    }
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut tls = rustls::ClientConfig::builder_with_provider(provider)
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(Error::network)?
        .with_root_certificates(roots)
        .with_no_client_auth();
    tls.alpn_protocols = vec![b"h3".to_vec()];
    let crypto =
        moq_noq::crypto::rustls::QuicClientConfig::try_from(tls).map_err(Error::network)?;
    let mut quic = moq_noq::ClientConfig::new(Arc::new(crypto));
    let mut transport = moq_noq::TransportConfig::default();
    transport
        .receive_window((2u32 * 1024 * 1024).into())
        .stream_receive_window((256u32 * 1024).into())
        .send_window(2 * 1024 * 1024)
        .max_concurrent_uni_streams(32u32.into())
        .max_concurrent_bidi_streams(16u32.into())
        .datagram_receive_buffer_size(Some(64 * 1024));
    quic.transport_config(Arc::new(transport));
    Ok((
        moq_noq::Endpoint::client(config.bind).map_err(Error::network)?,
        quic,
    ))
}
fn origin(config: &MoqConfig) -> (moq_net::origin::Producer, moq_net::origin::Driver) {
    let mut origin = moq_net::origin::Config::default();
    origin.cache_duration = config.limits.audio_max_age;
    origin.pool = moq_net::cache::Pool::new(
        moq_net::cache::Config::default()
            .with_capacity(8 * 1024 * 1024u64)
            .with_expiry(Duration::from_secs(2)),
    );
    moq_net::origin::Producer::new(origin)
}

enum Exit {
    Driver,
    Peer(moq_net::broadcast::Consumer),
    PeerEnded,
    WriterEnded(Box<Publication>),
    AudioEnded,
}
fn spawn<F>(jobs: &mut JoinSet<Result<Exit>>, shared: &Arc<Shared>, future: F)
where
    F: Future<Output = Result<Exit>> + Send + 'static,
{
    let guard = TaskGuard::new(shared);
    jobs.spawn(async move {
        let _guard = guard;
        future.await
    });
}
async fn supervise(
    jobs: &mut JoinSet<Result<Exit>>,
    shared: Arc<Shared>,
    config: &MoqConfig,
    events: mpsc::Sender<MoqEvent>,
    ready: oneshot::Sender<()>,
) -> Result<()> {
    let mut shutdown = shared.shutdown.subscribe();
    let mut ready = Some(ready);
    loop {
        tokio::select! {
            biased;
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() == Shutdown::Cancel { return Ok(()); }
            }
            exit = jobs.join_next() => match exit {
                Some(Ok(Ok(Exit::Peer(peer)))) => {
                    events.try_send(MoqEvent::PeerConnected).map_err(|_| Error::Backpressure)?;
                    let transcript_peer = peer.clone(); let transcript_config = config.clone(); let transcript_events = events.clone();
                    spawn(jobs, &shared, async move { receive_transcript(transcript_peer, transcript_config, transcript_events).await?; Ok(Exit::PeerEnded) });
                    if config.audio_in_enabled {
                        let audio_config = config.clone(); let audio_events = events.clone();
                        spawn(jobs, &shared, async move { receive_audio(peer, audio_config, audio_events).await?; Ok(Exit::AudioEnded) });
                    }
                    if let Some(ready) = ready.take() { let _ = ready.send(()); }
                }
                Some(Ok(Ok(Exit::WriterEnded(_publication)))) => {
                    tokio::select! {
                        _ = tokio::time::sleep(config.limits.close_grace) => {},
                        _ = async {
                            loop {
                                if *shutdown.borrow_and_update() == Shutdown::Cancel { break; }
                                if shutdown.changed().await.is_err() { break; }
                            }
                        } => {},
                    }
                    return Ok(());
                }
                Some(Ok(Ok(Exit::PeerEnded))) => return Err(Error::Closed),
                Some(Ok(Ok(Exit::AudioEnded))) => {},
                Some(Ok(Ok(Exit::Driver))) => return Err(Error::Protocol("origin driver ended".into())),
                Some(Ok(Err(error))) => return Err(error),
                Some(Err(error)) => return Err(Error::Task(error.to_string())),
                None => return Err(Error::Closed),
            }
        }
    }
}

struct Publication {
    _broadcast: moq_net::broadcast::Producer,
    transcript: transcript::Publisher,
    catalogs: Vec<moq_json::snapshot::Producer<hang::catalog::Catalog>>,
    audio: Option<(moq_net::track::Producer, media::Encoder)>,
    timestamp_us: u64,
    clock: Option<tokio::time::Instant>,
    generation: u64,
}
impl Publication {
    fn new(origin: &moq_net::origin::Producer, config: &MoqConfig) -> Result<Self> {
        let broadcast = origin
            .create_broadcast(config.publish_path.clone())
            .map_err(Error::protocol)?;
        let transcript = transcript::Publisher::new(
            broadcast
                .create_track(config.transcript_track.clone(), None)
                .map_err(Error::protocol)?,
            config.limits.clone(),
        )?;
        let mut catalog = hang::catalog::Catalog::default();
        let audio = if config.audio_out_enabled {
            catalog
                .audio
                .insert(
                    &config.audio_track,
                    hang::catalog::AudioConfig::new(hang::catalog::AudioCodec::Opus, 48_000, 1),
                )
                .map_err(Error::media)?;
            let track = broadcast
                .create_track(
                    config.audio_track.clone(),
                    hang::container::track_info(hang::catalog::PRIORITY.audio)
                        .with_max_age(config.limits.audio_max_age),
                )
                .map_err(Error::protocol)?;
            Some((track, media::Encoder::new()?))
        } else {
            None
        };
        let mut catalogs = Vec::new();
        for (name, compressed) in [("catalog.json", false), ("catalog.json.z", true)] {
            let track = broadcast
                .create_track(name, hang::catalog::Catalog::default_track_info())
                .map_err(Error::protocol)?;
            let mut options = moq_json::snapshot::Config::default();
            if compressed {
                options.compression = moq_json::Compression::Deflate;
            }
            let mut producer = moq_json::snapshot::Producer::new(track, options);
            producer.update(&catalog).map_err(Error::protocol)?;
            catalogs.push(producer);
        }
        broadcast
            .announce(Default::default())
            .map_err(Error::protocol)?;
        Ok(Self {
            _broadcast: broadcast,
            transcript,
            catalogs,
            audio,
            timestamp_us: 0,
            clock: None,
            generation: 0,
        })
    }
    async fn write(
        &mut self,
        content: Content,
        generation: u64,
        fenced: bool,
        shared: &Shared,
        shutdown: &mut watch::Receiver<Shutdown>,
        interrupt: &mut watch::Receiver<u64>,
    ) -> Result<()> {
        match content {
            Content::Message(message) => shared.commit(fenced.then_some(generation), || {
                self.transcript.append(message, false)
            }),
            Content::Audio(audio) => {
                if generation != *interrupt.borrow_and_update() {
                    return Err(Error::Interrupted);
                }
                if generation != self.generation {
                    self.clock = None;
                    self.generation = generation;
                }
                let (track, encoder) = self
                    .audio
                    .as_mut()
                    .ok_or(Error::Unsupported("audio output is disabled"))?;
                let now = tokio::time::Instant::now();
                let target = self.clock.unwrap_or(now).max(now);
                loop {
                    if *shutdown.borrow_and_update() == Shutdown::Cancel {
                        return Err(Error::Closed);
                    }
                    tokio::select! {
                        biased;
                        changed = shutdown.changed() => if changed.is_err() { return Err(Error::Closed); },
                        _ = interrupt.changed() => { self.clock = None; return Err(Error::Interrupted); },
                        _ = tokio::time::sleep_until(target) => break,
                    }
                }
                let next_timestamp = self
                    .timestamp_us
                    .checked_add(20_000)
                    .ok_or(Error::Limit("audio timestamp"))?;
                shared.commit(Some(generation), || {
                    let frame = hang::container::Frame {
                        timestamp: moq_net::Timestamp::from_micros(self.timestamp_us)
                            .map_err(Error::protocol)?,
                        payload: encoder.encode(&audio)?,
                    };
                    let mut group = track.append_group().map_err(Error::protocol)?;
                    frame.write_to(&mut group).map_err(Error::media)?;
                    group.finish().map_err(Error::protocol)
                })?;
                self.timestamp_us = next_timestamp;
                self.clock = Some(target + Duration::from_millis(20));
                Ok(())
            }
        }
    }
    fn finish(&mut self) -> Result<()> {
        self.transcript.finish()?;
        if let Some((track, _)) = &mut self.audio {
            track.finish().map_err(Error::protocol)?;
        }
        for catalog in &mut self.catalogs {
            catalog.finish().map_err(Error::protocol)?;
        }
        Ok(())
    }
}
async fn writer(
    mut publication: Publication,
    mut rx: mpsc::Receiver<Command>,
    shared: Arc<Shared>,
) -> Result<Publication> {
    let mut shutdown = shared.shutdown.subscribe();
    let mut ending = false;
    let mut interrupt = shared.interrupt.subscribe();
    loop {
        match *shutdown.borrow_and_update() {
            Shutdown::Cancel => return Ok(publication),
            Shutdown::End => {
                ending = true;
                rx.close();
            }
            Shutdown::Running => {}
        }
        let command = tokio::select! {
            biased;
            _ = shutdown.changed() => continue,
            command = rx.recv() => command,
        };
        let Some(command) = command else {
            if ending {
                shared.commit(None, || publication.finish())?;
            }
            return Ok(publication);
        };
        if command.fenced && command.generation != *shared.interrupt.borrow() {
            let _ = command.reply.send(Err(Error::Interrupted));
            continue;
        }
        let result = publication
            .write(
                command.content,
                command.generation,
                command.fenced,
                &shared,
                &mut shutdown,
                &mut interrupt,
            )
            .await;
        let _ = command.reply.send(result.clone());
        if result != Err(Error::Interrupted) {
            result?;
        }
    }
}
async fn receive_transcript(
    peer: moq_net::broadcast::Consumer,
    config: MoqConfig,
    events: mpsc::Sender<MoqEvent>,
) -> Result<()> {
    let mut track = wire::subscribe(&peer, &config.transcript_track, Duration::ZERO).await?;
    let mut group = track
        .recv_group()
        .await
        .map_err(Error::protocol)?
        .ok_or(Error::Closed)?;
    let mut decoder = transcript::Subscriber::new(config.limits.clone());
    let mut track_ended = false;
    loop {
        let frame = tokio::select! {
            biased;
            next = track.recv_group(), if !track_ended => {
                if next.map_err(Error::protocol)?.is_some() { return Err(Error::Protocol("transcript rolled to a second group".into())); }
                track_ended = true; continue;
            }
            frame = group.next_frame() => frame.map_err(Error::protocol)?,
        };
        let Some(frame) = frame else {
            if !track_ended && track.recv_group().await.map_err(Error::protocol)?.is_some() {
                return Err(Error::Protocol(
                    "transcript rolled to a second group".into(),
                ));
            }
            return Ok(());
        };
        let bytes = tokio::time::timeout(
            config.limits.connect_timeout,
            wire::payload(frame, config.limits.compressed_record_bytes),
        )
        .await
        .map_err(|_| Error::Timeout("transcript payload"))??;
        match decoder.decode(&bytes)? {
            transcript::Record::Message(value) => events
                .send(MoqEvent::Message(value))
                .await
                .map_err(|_| Error::Closed)?,
            transcript::Record::Goodbye => return Ok(()),
            transcript::Record::Replay | transcript::Record::Internal => {}
        }
    }
}
async fn receive_audio(
    peer: moq_net::broadcast::Consumer,
    config: MoqConfig,
    events: mpsc::Sender<MoqEvent>,
) -> Result<()> {
    let (name, audio_config) = tokio::time::timeout(
        config.limits.connect_timeout,
        wire::audio_catalog(&peer, config.limits.catalog_bytes),
    )
    .await
    .map_err(|_| Error::Timeout("audio catalog"))??;
    let mut decoder = media::Decoder::new(&audio_config)?;
    let mut track = wire::subscribe(&peer, &name, config.limits.audio_max_age)
        .await?
        .ordered();
    let mut timestamp = None;
    'groups: while let Some(mut group) = track.next_group().await.map_err(Error::protocol)? {
        loop {
            let frame = match group.next_frame().await {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(moq_net::Error::Old) => continue 'groups,
                Err(error) => return Err(Error::protocol(error)),
            };
            let bytes = match tokio::time::timeout(
                config.limits.connect_timeout,
                wire::payload(frame, media::MAX_PACKET + 8),
            )
            .await
            .map_err(|_| Error::Timeout("audio payload"))?
            {
                Ok(bytes) => bytes,
                Err(Error::Stale) => continue 'groups,
                Err(error) => return Err(error),
            };
            let frame = hang::container::Frame::decode(bytes).map_err(Error::media)?;
            let pts = frame
                .timestamp
                .convert(moq_net::Timescale::MICRO)
                .map_err(Error::protocol)?
                .value();
            if timestamp.is_some_and(|last| pts < last) {
                return Err(Error::Protocol("audio timestamp moved backwards".into()));
            }
            timestamp = Some(pts);
            let audio = decoder.decode(&frame.payload)?;
            events
                .send(MoqEvent::Audio {
                    audio,
                    timestamp_us: pts,
                })
                .await
                .map_err(|_| Error::Closed)?;
        }
    }
    Ok(())
}
