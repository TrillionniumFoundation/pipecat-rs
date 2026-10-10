use crate::{config::bounded, GatewayConfig, GatewayError, RemoteOutcome, Result, RunStatus};
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::{
    collections::{HashMap, VecDeque},
    fmt,
    sync::{
        atomic::{AtomicBool, AtomicU8, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};
use tokio::{
    net::TcpStream,
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::{
    connect_async_with_config,
    tungstenite::{protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;
type Reply<T> = oneshot::Sender<Result<T>>;

#[derive(Clone, PartialEq, Eq)]
pub struct RunIdentity {
    pub run_id: String,
    pub session_key: String,
}
impl fmt::Debug for RunIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("RunIdentity([redacted])")
    }
}
#[derive(Clone, PartialEq, Eq)]
pub struct RunResult {
    pub run_id: String,
    pub status: RunStatus,
    pub text: String,
    pub remote_outcome: RemoteOutcome,
}
impl fmt::Debug for RunResult {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunResult")
            .field("status", &self.status)
            .field("remote_outcome", &self.remote_outcome)
            .finish_non_exhaustive()
    }
}
#[derive(Clone, PartialEq, Eq)]
pub enum RunEvent {
    Text { run_id: String, text: String },
    End(RunResult),
}
impl fmt::Debug for RunEvent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text { .. } => f.write_str("RunEvent::Text([redacted])"),
            Self::End(r) => f.debug_tuple("RunEvent::End").field(r).finish(),
        }
    }
}
struct SharedRun {
    logical: String,
    connection: String,
    identity: Mutex<RunIdentity>,
    done: AtomicBool,
    queued_bytes: AtomicUsize,
}
/// Addressable after the stream ends, so abort can report the normal false race.
#[derive(Clone)]
pub struct RunHandle(Arc<SharedRun>);
impl fmt::Debug for RunHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunHandle")
            .field("done", &self.is_done())
            .finish_non_exhaustive()
    }
}
impl RunHandle {
    pub fn identity(&self) -> RunIdentity {
        self.0.identity.lock().unwrap().clone()
    }
    pub fn is_done(&self) -> bool {
        self.0.done.load(Ordering::Acquire)
    }
}
/// A single-consumer stream with independent, non-droppable terminal storage.
/// Dropping it stops local observation; it does not certify remote cancellation.
pub struct GatewayRun {
    handle: RunHandle,
    events: mpsc::Receiver<RunEvent>,
    terminal: watch::Receiver<Option<RunResult>>,
    ended: bool,
}
impl GatewayRun {
    pub fn handle(&self) -> RunHandle {
        self.handle.clone()
    }
    pub fn identity(&self) -> RunIdentity {
        self.handle.identity()
    }
    pub async fn next(&mut self) -> Option<RunEvent> {
        if self.ended {
            return None;
        }
        loop {
            if let Ok(event) = self.events.try_recv() {
                if let RunEvent::Text { text, .. } = &event {
                    self.handle
                        .0
                        .queued_bytes
                        .fetch_sub(text.len(), Ordering::AcqRel);
                }
                return Some(event);
            }
            let terminal = self.terminal.borrow().clone();
            if let Some(result) = terminal {
                // A producer may enqueue a final delta between the first
                // try_recv and terminal observation. Drain it before End.
                if let Ok(event) = self.events.try_recv() {
                    if let RunEvent::Text { text, .. } = &event {
                        self.handle
                            .0
                            .queued_bytes
                            .fetch_sub(text.len(), Ordering::AcqRel);
                    }
                    return Some(event);
                }
                self.ended = true;
                return Some(RunEvent::End(result));
            }
            tokio::select! {
                biased;
                event = self.events.recv() => if let Some(event) = event {
                    if let RunEvent::Text{text,..} = &event { self.handle.0.queued_bytes.fetch_sub(text.len(), Ordering::AcqRel); }
                    return Some(event);
                } else if self.terminal.borrow().is_none() {
                    self.ended = true;
                    return Some(RunEvent::End(RunResult {run_id:self.identity().run_id,status:RunStatus::Failed,text:"OpenClaw observer closed".into(),remote_outcome:RemoteOutcome::Unknown}));
                },
                changed = self.terminal.changed() => if changed.is_err() && self.terminal.borrow().is_none() {
                    self.ended = true;
                    return Some(RunEvent::End(RunResult {run_id:self.identity().run_id,status:RunStatus::Failed,text:"OpenClaw observer closed".into(),remote_outcome:RemoteOutcome::Unknown}));
                }
            }
        }
    }
    pub async fn collect(mut self) -> RunResult {
        let mut text = String::new();
        while let Some(event) = self.next().await {
            match event {
                RunEvent::Text { text: part, .. } => text.push_str(&part),
                RunEvent::End(mut result) => {
                    if result.status == RunStatus::Completed && result.text.is_empty() {
                        result.text = text.trim().into();
                    }
                    return result;
                }
            }
        }
        RunResult {
            run_id: self.identity().run_id,
            status: RunStatus::Failed,
            text: "OpenClaw observer closed".into(),
            remote_outcome: RemoteOutcome::Unknown,
        }
    }
}
struct Owner {
    normal: mpsc::Sender<Command>,
    urgent: mpsc::Sender<Command>,
    cancel: watch::Sender<bool>,
    stopped: watch::Receiver<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
    config: GatewayConfig,
    connection: String,
}
impl Drop for Owner {
    fn drop(&mut self) {
        self.cancel.send_replace(true);
        // Cancellation wakes the actor to settle streams. Tasks never retain Owner.
    }
}
/// One authenticated connection. A closed client never resumes old runs or
/// retries requests. Call connect again for a fresh, unrelated connection.
#[derive(Clone)]
pub struct GatewayClient {
    owner: Arc<Owner>,
}
impl fmt::Debug for GatewayClient {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GatewayClient")
            .field("closed", &self.is_closed())
            .finish_non_exhaustive()
    }
}
impl GatewayClient {
    pub async fn connect(config: GatewayConfig) -> Result<Self> {
        config.validate()?;
        let socket = timeout(config.limits.connect_timeout, async {
            let ws_config = WebSocketConfig::default()
                .max_message_size(Some(config.limits.max_message_bytes))
                .max_frame_size(Some(config.limits.max_message_bytes))
                .write_buffer_size(0)
                .max_write_buffer_size(config.limits.max_message_bytes.saturating_add(1024));
            let (mut ws, _) = connect_async_with_config(&config.url,Some(ws_config),true).await.map_err(|_| GatewayError::BeforeWrite("connect"))?;
            let challenge = read_json(&mut ws,config.limits.max_message_bytes).await?;
            if challenge.get("type").and_then(Value::as_str)!=Some("event") || challenge.get("event").and_then(Value::as_str)!=Some("connect.challenge") {
                return Err(GatewayError::Protocol("expected challenge"));
            }
            let id = request_id();
            let mut params = json!({"minProtocol":4,"maxProtocol":4,"client":{"id":"gateway-client","displayName":"pipecat","version":"0.1.0","platform":std::env::consts::OS,"mode":"backend"},"caps":[],"role":config.role,"scopes":config.scopes});
            let mut auth = serde_json::Map::new();
            if let Some(s)=&config.token {auth.insert("token".into(),s.clone().into());}
            if let Some(s)=&config.password {auth.insert("password".into(),s.clone().into());}
            if !auth.is_empty() {params["auth"]=auth.into();}
            let wire = json!({"type":"req","id":id,"method":"connect","params":params}).to_string();
            if wire.len()>config.limits.max_message_bytes {return Err(GatewayError::Capacity("handshake"));}
            timeout(config.limits.write_timeout,ws.send(Message::Text(wire.into()))).await.map_err(|_| GatewayError::Timeout("handshake write"))?.map_err(|_| GatewayError::Closed)?;
            let response = read_json(&mut ws,config.limits.max_message_bytes).await?;
            if response.get("type").and_then(Value::as_str)!=Some("res") || response.get("id").and_then(Value::as_str)!=Some(id.as_str()) {return Err(GatewayError::Protocol("hello correlation"));}
            if response.get("ok").and_then(Value::as_bool)==Some(false) {return Err(GatewayError::Rejected("connect"));}
            if response.get("ok").and_then(Value::as_bool)!=Some(true) || response["payload"]["type"]!="hello-ok" || response["payload"]["protocol"]!=4 {return Err(GatewayError::Protocol("hello protocol"));}
            Ok(ws)
        }).await.map_err(|_| GatewayError::Timeout("connect/challenge/hello"))??;
        // All owned tasks are bounded and hold no strong reference to the client.
        let (normal_tx, normal_rx) = mpsc::channel(config.limits.max_pending);
        let (urgent_tx, urgent_rx) = mpsc::channel(config.limits.max_pending);
        let (write_tx, write_rx) = mpsc::channel(config.limits.max_pending);
        let (control_tx, control_rx) = mpsc::channel(config.limits.max_pending);
        let (writer_errors_tx, writer_errors_rx) = mpsc::channel(1);
        let (cancel_tx, cancel_rx) = watch::channel(false);
        let (stop_tx, stop_rx) = watch::channel(false);
        let connection = uuid::Uuid::new_v4().to_string();
        let (sink, reader) = socket.split();
        let writer_config = config.clone();
        let writer_cancel = cancel_rx.clone();
        let writer = tokio::spawn(writer(
            sink,
            write_rx,
            control_rx,
            writer_errors_tx,
            writer_cancel,
            writer_config,
        ));
        let actor_config = config.clone();
        let actor_connection = connection.clone();
        let writer_abort = writer.abort_handle();
        let actor = tokio::spawn(async move {
            let mut actor = Actor {
                config: actor_config,
                connection: actor_connection,
                runs: HashMap::new(),
                aliases: HashMap::new(),
                pending: HashMap::new(),
                unrouted: VecDeque::new(),
                unrouted_bytes: 0,
                writes: write_tx,
                controls: control_tx,
            };
            actor
                .run(reader, normal_rx, urgent_rx, writer_errors_rx, cancel_rx)
                .await;
            writer_abort.abort();
            stop_tx.send_replace(true);
        });
        Ok(Self {
            owner: Arc::new(Owner {
                normal: normal_tx,
                urgent: urgent_tx,
                cancel: cancel_tx,
                stopped: stop_rx,
                tasks: Mutex::new(vec![actor, writer]),
                config,
                connection,
            }),
        })
    }
    pub fn config(&self) -> &GatewayConfig {
        &self.owner.config
    }
    pub fn is_closed(&self) -> bool {
        *self.owner.stopped.borrow() || *self.owner.cancel.borrow()
    }
    pub async fn start(
        &self,
        message: impl Into<String>,
        session_key: Option<&str>,
    ) -> Result<GatewayRun> {
        let message = message.into();
        bounded(&message, self.config().limits.max_text_bytes, "message")?;
        let session = session_key.unwrap_or(&self.config().session_key);
        bounded(session, 1024, "session key")?;
        let (tx, rx) = oneshot::channel();
        self.enqueue(
            Command::Start {
                message,
                session: session.into(),
                reply: tx,
            },
            false,
        )?;
        rx.await.unwrap_or(Err(GatewayError::Closed))
    }
    pub async fn steer(&self, run: &RunHandle, message: impl Into<String>) -> Result<()> {
        self.check_handle(run)?;
        let message = message.into();
        bounded(&message, self.config().limits.max_text_bytes, "message")?;
        if run.is_done() {
            return Ok(());
        }
        let (tx, rx) = oneshot::channel();
        self.enqueue(
            Command::Steer {
                handle: run.clone(),
                message,
                reply: tx,
            },
            false,
        )?;
        rx.await.unwrap_or(Err(GatewayError::Closed))
    }
    pub async fn abort(&self, run: &RunHandle) -> Result<bool> {
        self.check_handle(run)?;
        let (tx, rx) = oneshot::channel();
        self.enqueue(
            Command::Abort {
                handle: run.clone(),
                reply: tx,
            },
            true,
        )?;
        rx.await.unwrap_or(Err(GatewayError::Closed))
    }
    /// Settle local observation while explicitly preserving remote ambiguity.
    /// This performs no network abort and is useful after bounded best-effort abort.
    pub fn end_observation(&self, run: &RunHandle) -> Result<()> {
        self.check_handle(run)?;
        self.enqueue(
            Command::EndObservation {
                logical: run.0.logical.clone(),
            },
            true,
        )
    }
    fn check_handle(&self, run: &RunHandle) -> Result<()> {
        if run.0.connection != self.owner.connection {
            Err(GatewayError::Invalid(
                "handle belongs to another connection",
            ))
        } else {
            Ok(())
        }
    }
    fn enqueue(&self, command: Command, urgent: bool) -> Result<()> {
        if self.is_closed() {
            return Err(GatewayError::BeforeWrite("closed"));
        }
        let queue = if urgent {
            &self.owner.urgent
        } else {
            &self.owner.normal
        };
        queue.try_send(command).map_err(|e| match e {
            mpsc::error::TrySendError::Full(_) => GatewayError::Capacity("command queue"),
            mpsc::error::TrySendError::Closed(_) => GatewayError::BeforeWrite("closed"),
        })
    }
    pub fn cancel(&self) {
        self.owner.cancel.send_replace(true);
    }
    pub async fn disconnect(&self) {
        self.cancel();
        let mut stopped = self.owner.stopped.clone();
        let _ = timeout(self.config().limits.close_timeout, async {
            while !*stopped.borrow() {
                if stopped.changed().await.is_err() {
                    break;
                }
            }
        })
        .await;
        let tasks = std::mem::take(&mut *self.owner.tasks.lock().unwrap());
        for task in tasks {
            if !task.is_finished() {
                task.abort();
            }
            let _ = task.await;
        }
    }
}
fn request_id() -> String {
    format!("request-{}", uuid::Uuid::new_v4())
}
fn run_id() -> String {
    format!("run-{}", uuid::Uuid::new_v4())
}
async fn read_json(ws: &mut Socket, max: usize) -> Result<Value> {
    loop {
        match ws.next().await {
            Some(Ok(Message::Text(t))) if t.len() <= max => {
                return serde_json::from_str(&t).map_err(|_| GatewayError::Protocol("invalid JSON"))
            }
            Some(Ok(Message::Ping(_) | Message::Pong(_))) => (),
            Some(Ok(_)) => return Err(GatewayError::Protocol("expected text")),
            _ => return Err(GatewayError::Closed),
        }
    }
}

enum Command {
    Start {
        message: String,
        session: String,
        reply: Reply<GatewayRun>,
    },
    Steer {
        handle: RunHandle,
        message: String,
        reply: Reply<()>,
    },
    Abort {
        handle: RunHandle,
        reply: Reply<bool>,
    },
    EndObservation {
        logical: String,
    },
}
struct Write {
    wire: String,
    stage: Arc<AtomicU8>,
}
async fn writer(
    mut sink: futures_util::stream::SplitSink<Socket, Message>,
    mut normal: mpsc::Receiver<Write>,
    mut urgent: mpsc::Receiver<Write>,
    errors: mpsc::Sender<()>,
    mut cancel: watch::Receiver<bool>,
    config: GatewayConfig,
) {
    loop {
        let item = tokio::select! {biased;_=cancel.changed()=>break,item=urgent.recv()=>item,item=normal.recv()=>item};
        let Some(item) = item else {
            break;
        };
        if item
            .stage
            .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            continue;
        }
        let result = tokio::select! {biased;_=cancel.changed()=>break,r=timeout(config.limits.write_timeout,sink.send(Message::Text(item.wire.into())))=>r};
        if !matches!(result, Ok(Ok(()))) {
            let _ = errors.try_send(());
            break;
        }
        item.stage.store(2, Ordering::Release);
    }
    let _ = timeout(config.limits.close_timeout, sink.close()).await;
}
struct LiveRun {
    shared: Arc<SharedRun>,
    aliases: Vec<String>,
    events: Option<mpsc::Sender<RunEvent>>,
    terminal: watch::Sender<Option<RunResult>>,
    accumulated: String,
    deadline: Instant,
}
impl LiveRun {
    fn finish(&mut self, status: RunStatus, text: String, outcome: RemoteOutcome) {
        if self.shared.done.swap(true, Ordering::AcqRel) {
            return;
        }
        let run_id = self.shared.identity.lock().unwrap().run_id.clone();
        self.terminal.send_replace(Some(RunResult {
            run_id,
            status,
            text,
            remote_outcome: outcome,
        }));
        self.events.take();
    }
}
enum PendingKind {
    Start {
        logical: String,
        run: GatewayRun,
        reply: Reply<GatewayRun>,
    },
    Steer {
        logical: String,
        old_id: String,
        old_aliases: Vec<String>,
        reply: Reply<()>,
    },
    Abort {
        reply: Reply<bool>,
    },
}
impl PendingKind {
    fn logical(&self) -> Option<&str> {
        match self {
            Self::Start { logical, .. } | Self::Steer { logical, .. } => Some(logical),
            Self::Abort { .. } => None,
        }
    }
    fn method(&self) -> &'static str {
        match self {
            Self::Start { .. } => "chat.send",
            Self::Steer { .. } => "sessions.steer",
            Self::Abort { .. } => "chat.abort",
        }
    }
}
struct Pending {
    kind: PendingKind,
    stage: Arc<AtomicU8>,
    deadline: Instant,
}
#[derive(Clone)]
struct Chat {
    id: String,
    session: Option<String>,
    state: String,
    text: String,
    bytes: usize,
}
struct Actor {
    config: GatewayConfig,
    connection: String,
    runs: HashMap<String, LiveRun>,
    aliases: HashMap<String, String>,
    pending: HashMap<String, Pending>,
    unrouted: VecDeque<Chat>,
    unrouted_bytes: usize,
    writes: mpsc::Sender<Write>,
    controls: mpsc::Sender<Write>,
}
impl Actor {
    async fn run(
        &mut self,
        mut reader: futures_util::stream::SplitStream<Socket>,
        mut normal: mpsc::Receiver<Command>,
        mut urgent: mpsc::Receiver<Command>,
        mut writer_errors: mpsc::Receiver<()>,
        mut cancel: watch::Receiver<bool>,
    ) {
        enum Input {
            Command(Option<Command>),
            Message(Option<std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>),
            Tick,
        }
        let mut tick = tokio::time::interval(std::time::Duration::from_millis(10));
        let mut prefer_command = true;
        loop {
            // Alternate admission and wire processing so an always-ready peer
            // cannot starve queued work, or continuous callers starve replies.
            let input = tokio::select! {biased;
                _=cancel.changed()=>break,
                _=writer_errors.recv()=>break,
                _=tick.tick()=>Input::Tick,
                command=urgent.recv()=>Input::Command(command),
                input=async {
                    if prefer_command {
                        tokio::select! {biased;command=normal.recv()=>Input::Command(command),message=reader.next()=>Input::Message(message)}
                    } else {
                        tokio::select! {biased;message=reader.next()=>Input::Message(message),command=normal.recv()=>Input::Command(command)}
                    }
                }=>input,
            };
            prefer_command = !prefer_command;
            let result = match input {
                Input::Command(Some(command)) => self.command(command),
                Input::Message(Some(Ok(Message::Text(t))))
                    if t.len() <= self.config.limits.max_message_bytes =>
                {
                    self.message(&t)
                }
                Input::Message(Some(Ok(Message::Ping(_) | Message::Pong(_)))) => Ok(()),
                Input::Tick => self.expire(),
                _ => break,
            };
            if result.is_err() {
                break;
            }
        }
        for run in self.runs.values_mut() {
            run.finish(
                RunStatus::Failed,
                "OpenClaw connection closed; remote outcome unknown".into(),
                RemoteOutcome::Unknown,
            );
        }
        let pending = std::mem::take(&mut self.pending);
        for (_, pending) in pending {
            let e = Self::submission_error(&pending);
            self.fail(pending, e);
        }
    }
    fn submission_error(p: &Pending) -> GatewayError {
        if p.stage
            .compare_exchange(0, 3, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
        {
            GatewayError::BeforeWrite(p.kind.method())
        } else {
            GatewayError::OutcomeUnknown(p.kind.method())
        }
    }
    fn command(&mut self, c: Command) -> Result<()> {
        match c {
            Command::Start {
                message,
                session,
                reply,
            } => {
                if self.runs.len() >= self.config.limits.max_runs {
                    let _ = reply.send(Err(GatewayError::Capacity("live runs")));
                    return Ok(());
                }
                let id = run_id();
                let logical = uuid::Uuid::new_v4().to_string();
                let shared = Arc::new(SharedRun {
                    logical: logical.clone(),
                    connection: self.connection.clone(),
                    identity: Mutex::new(RunIdentity {
                        run_id: id.clone(),
                        session_key: session.clone(),
                    }),
                    done: AtomicBool::new(false),
                    queued_bytes: AtomicUsize::new(0),
                });
                let (tx, rx) = mpsc::channel(self.config.limits.max_output_events);
                let (end_tx, end_rx) = watch::channel(None);
                let run = GatewayRun {
                    handle: RunHandle(shared.clone()),
                    events: rx,
                    terminal: end_rx,
                    ended: false,
                };
                self.aliases.insert(id.clone(), logical.clone());
                self.runs.insert(
                    logical.clone(),
                    LiveRun {
                        shared,
                        aliases: vec![id.clone()],
                        events: Some(tx),
                        terminal: end_tx,
                        accumulated: String::new(),
                        deadline: Instant::now() + self.config.limits.run_timeout,
                    },
                );
                let params = json!({"sessionKey":session,"message":message,"timeoutMs":self.config.limits.run_timeout.as_millis() as u64,"idempotencyKey":id});
                self.submit(
                    PendingKind::Start {
                        logical,
                        run,
                        reply,
                    },
                    params,
                    false,
                );
            }
            Command::Steer {
                handle,
                message,
                reply,
            } => {
                let logical = handle.0.logical.clone();
                if self
                    .pending
                    .values()
                    .any(|p| p.kind.logical() == Some(logical.as_str()))
                {
                    let _ = reply.send(Err(GatewayError::BeforeWrite("run has pending request")));
                    return Ok(());
                }
                let Some(run) = self.runs.get_mut(&logical) else {
                    let _ = reply.send(Ok(()));
                    return Ok(());
                };
                if run.shared.done.load(Ordering::Acquire) {
                    let _ = reply.send(Ok(()));
                    return Ok(());
                }
                let mut identity = run.shared.identity.lock().unwrap();
                let old_id = identity.run_id.clone();
                let old_aliases = run.aliases.clone();
                let id = run_id();
                identity.run_id = id.clone();
                let params =
                    json!({"key":identity.session_key,"message":message,"idempotencyKey":id});
                drop(identity);
                for old in &old_aliases {
                    self.aliases.remove(old);
                }
                run.aliases = vec![id.clone()];
                self.aliases.insert(id, logical.clone());
                self.submit(
                    PendingKind::Steer {
                        logical,
                        old_id,
                        old_aliases,
                        reply,
                    },
                    params,
                    false,
                );
            }
            Command::Abort { handle, reply } => {
                // An urgent abort must not overtake a queued, unwritten steer
                // and accidentally target an identity that does not yet exist.
                let queued=self.pending.iter().find_map(|(id,p)|{
                    if matches!(&p.kind,PendingKind::Steer{logical,..} if logical==&handle.0.logical)
                        && p.stage.compare_exchange(0,3,Ordering::AcqRel,Ordering::Acquire).is_ok(){Some(id.clone())}else{None}
                });
                if let Some(id) = queued {
                    let p = self.pending.remove(&id).unwrap();
                    self.fail(p, GatewayError::BeforeWrite("steer superseded by abort"));
                }
                let identity = handle.identity();
                self.submit(
                    PendingKind::Abort { reply },
                    json!({"sessionKey":identity.session_key,"runId":identity.run_id}),
                    true,
                );
            }
            Command::EndObservation { logical } => {
                if let Some(run) = self.runs.get_mut(&logical) {
                    run.finish(
                        RunStatus::Cancelled,
                        "Local observation cancelled; remote outcome unknown".into(),
                        RemoteOutcome::Unknown,
                    );
                }
            }
        }
        Ok(())
    }
    fn submit(&mut self, kind: PendingKind, params: Value, urgent: bool) {
        let id = request_id();
        let stage = Arc::new(AtomicU8::new(0));
        let p = Pending {
            kind,
            stage: stage.clone(),
            deadline: Instant::now() + self.config.limits.request_timeout,
        };
        // One separate urgent reservation prevents normal work exhausting abort.
        let max = self.config.limits.max_pending + usize::from(urgent);
        if self.pending.len() >= max {
            self.fail(p, GatewayError::BeforeWrite("pending request capacity"));
            return;
        }
        let wire =
            json!({"type":"req","id":id,"method":p.kind.method(),"params":params}).to_string();
        if wire.len() > self.config.limits.max_message_bytes {
            self.fail(p, GatewayError::BeforeWrite("request too large"));
            return;
        }
        let writes = if urgent { &self.controls } else { &self.writes };
        if writes.try_send(Write { wire, stage }).is_err() {
            self.fail(p, GatewayError::BeforeWrite("write queue full"));
            return;
        }
        self.pending.insert(id, p);
    }
    fn fail(&mut self, p: Pending, error: GatewayError) {
        let uncertain = matches!(error, GatewayError::OutcomeUnknown(_));
        match p.kind {
            PendingKind::Start { logical, reply, .. } => {
                if let Some(run) = self.runs.get_mut(&logical) {
                    run.finish(
                        RunStatus::Failed,
                        error.to_string(),
                        if uncertain {
                            RemoteOutcome::Unknown
                        } else {
                            RemoteOutcome::Settled
                        },
                    );
                }
                self.remove(&logical);
                let _ = reply.send(Err(error));
            }
            PendingKind::Steer {
                logical,
                old_id,
                old_aliases,
                reply,
            } => {
                if let Some(run) = self.runs.get_mut(&logical) {
                    if uncertain {
                        run.finish(RunStatus::Failed, error.to_string(), RemoteOutcome::Unknown);
                    } else {
                        for id in &run.aliases {
                            self.aliases.remove(id);
                        }
                        run.aliases = old_aliases;
                        run.shared.identity.lock().unwrap().run_id = old_id;
                        for id in &run.aliases {
                            self.aliases.insert(id.clone(), logical.clone());
                        }
                    }
                }
                let _ = reply.send(Err(error));
                if !uncertain {
                    let _ = self.replay();
                }
            }
            PendingKind::Abort { reply } => {
                let _ = reply.send(Err(error));
            }
        }
    }
    fn message(&mut self, text: &str) -> Result<()> {
        let v: Value =
            serde_json::from_str(text).map_err(|_| GatewayError::Protocol("invalid JSON"))?;
        match v.get("type").and_then(Value::as_str) {
            Some("res") => {
                let id = v
                    .get("id")
                    .and_then(Value::as_str)
                    .ok_or(GatewayError::Protocol("response id"))?;
                let Some(p) = self.pending.remove(id) else {
                    return Ok(());
                };
                match v.get("ok").and_then(Value::as_bool) {
                    Some(false) => {
                        let method = p.kind.method();
                        self.fail(p, GatewayError::Rejected(method));
                    }
                    Some(true) => self.success(p, v.get("payload").unwrap_or(&Value::Null))?,
                    None => {
                        let method = p.kind.method();
                        self.fail(p, GatewayError::OutcomeUnknown(method));
                        return Err(GatewayError::Protocol("response status"));
                    }
                }
            }
            Some("event") if v.get("event").and_then(Value::as_str) == Some("chat") => {
                let chat = parse_chat(
                    v.get("payload")
                        .ok_or(GatewayError::Protocol("chat payload"))?,
                    text.len(),
                    self.config.limits.max_text_bytes,
                )?;
                if let Some(chat) = chat {
                    self.route(chat, true)?;
                }
            }
            Some("event") => (),
            _ => return Err(GatewayError::Protocol("envelope type")),
        }
        Ok(())
    }
    fn success(&mut self, p: Pending, payload: &Value) -> Result<()> {
        match &p.kind {
            PendingKind::Start { logical, .. } | PendingKind::Steer { logical, .. } => {
                let Some(id) = payload
                    .get("runId")
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty() && s.len() <= 1024)
                else {
                    let method = p.kind.method();
                    self.fail(p, GatewayError::OutcomeUnknown(method));
                    return Err(GatewayError::Protocol("accepted run identity"));
                };
                if matches!(p.kind, PendingKind::Steer { .. })
                    && payload.get("status").and_then(Value::as_str) != Some("started")
                {
                    self.fail(p, GatewayError::OutcomeUnknown("sessions.steer"));
                    return Err(GatewayError::Protocol("steer status"));
                }
                let logical = logical.clone();
                if id.starts_with("request-") || self.aliases.get(id).is_some_and(|owner|owner!=&logical) || self.pending.values().any(|pending|matches!(&pending.kind,PendingKind::Steer{logical:owner,old_aliases,..} if owner!=&logical && old_aliases.iter().any(|s|s==id))) {
                    let method=p.kind.method();self.fail(p,GatewayError::OutcomeUnknown(method));return Err(GatewayError::Protocol("run alias collision"));
                }
                let run = self
                    .runs
                    .get_mut(&logical)
                    .ok_or(GatewayError::Protocol("missing live run"))?;
                if !run.aliases.iter().any(|alias| alias == id) {
                    if run.aliases.len() >= self.config.limits.max_aliases {
                        let method = p.kind.method();
                        self.fail(p, GatewayError::OutcomeUnknown(method));
                        return Err(GatewayError::Capacity("run aliases"));
                    }
                    run.aliases.push(id.into());
                    self.aliases.insert(id.into(), logical);
                }
                run.shared.identity.lock().unwrap().run_id = id.into();
                let terminal = run.terminal.borrow().clone();
                if let Some(mut terminal) = terminal {
                    terminal.run_id = id.into();
                    run.terminal.send_replace(Some(terminal));
                }
                if let Err(error) = self.replay() {
                    let method = p.kind.method();
                    self.fail(p, GatewayError::OutcomeUnknown(method));
                    return Err(error);
                }
            }
            PendingKind::Abort { .. } => {
                if payload.get("aborted").and_then(Value::as_bool).is_none() {
                    self.fail(p, GatewayError::OutcomeUnknown("chat.abort"));
                    return Err(GatewayError::Protocol("abort response"));
                }
            }
        }
        match p.kind {
            PendingKind::Start { run, reply, .. } => {
                let _ = reply.send(Ok(run));
            }
            PendingKind::Steer { reply, .. } => {
                let _ = reply.send(Ok(()));
            }
            PendingKind::Abort { reply } => {
                let _ = reply.send(Ok(payload["aborted"].as_bool().unwrap()));
            }
        }
        Ok(())
    }
    fn route(&mut self, chat: Chat, buffer: bool) -> Result<()> {
        let Some(logical) = self.aliases.get(&chat.id).cloned() else {
            if buffer {
                // Buffer only while identity negotiation can explain unknown IDs.
                if !self.pending.values().any(|p| p.kind.logical().is_some()) {
                    return Ok(());
                }
                if self.unrouted.len() >= self.config.limits.max_unrouted_frames
                    || self.unrouted_bytes.saturating_add(chat.bytes)
                        > self.config.limits.max_unrouted_bytes
                {
                    return Err(GatewayError::Capacity("unrouted events"));
                }
                self.unrouted_bytes += chat.bytes;
                self.unrouted.push_back(chat);
            }
            return Ok(());
        };
        // Replacement output is provisional until steer is explicitly accepted.
        // Rejection restores old IDs and replays their buffered terminal/deltas.
        if self
            .pending
            .values()
            .any(|p| matches!(&p.kind,PendingKind::Steer{logical:owner,..} if owner==&logical))
        {
            if self.unrouted.len() >= self.config.limits.max_unrouted_frames
                || self.unrouted_bytes.saturating_add(chat.bytes)
                    > self.config.limits.max_unrouted_bytes
            {
                return Err(GatewayError::Capacity("provisional steer events"));
            }
            self.unrouted_bytes += chat.bytes;
            self.unrouted.push_back(chat);
            return Ok(());
        }
        let Some(run) = self.runs.get_mut(&logical) else {
            return Ok(());
        };
        if run.shared.done.load(Ordering::Acquire) {
            return Ok(());
        }
        if chat
            .session
            .as_ref()
            .is_some_and(|session| session != &run.shared.identity.lock().unwrap().session_key)
        {
            return Ok(());
        }
        if chat.state == "delta" {
            if chat.text.is_empty() {
                return Ok(());
            }
            if run.accumulated.len().saturating_add(chat.text.len())
                > self.config.limits.max_text_bytes
            {
                run.finish(
                    RunStatus::Failed,
                    "OpenClaw accumulated text limit".into(),
                    RemoteOutcome::Unknown,
                );
                return Ok(());
            }
            let queued = run.shared.queued_bytes.load(Ordering::Acquire);
            if queued.saturating_add(chat.text.len()) > self.config.limits.max_output_bytes {
                run.finish(
                    RunStatus::Failed,
                    "OpenClaw output byte limit".into(),
                    RemoteOutcome::Unknown,
                );
                return Ok(());
            }
            let n = chat.text.len();
            run.shared.queued_bytes.fetch_add(n, Ordering::AcqRel);
            run.accumulated.push_str(&chat.text);
            if run.events.as_ref().is_none_or(|tx| {
                tx.try_send(RunEvent::Text {
                    run_id: chat.id,
                    text: chat.text,
                })
                .is_err()
            }) {
                run.shared.queued_bytes.fetch_sub(n, Ordering::AcqRel);
                run.finish(
                    RunStatus::Failed,
                    "OpenClaw output event limit or consumer closed".into(),
                    RemoteOutcome::Unknown,
                );
            }
        } else {
            let text = chat.text;
            let status = match chat.state.as_str() {
                "final" => RunStatus::Completed,
                "aborted" => RunStatus::Cancelled,
                _ => RunStatus::Failed,
            };
            run.finish(status, text, RemoteOutcome::Settled);
        }
        Ok(())
    }
    fn replay(&mut self) -> Result<()> {
        let events = std::mem::take(&mut self.unrouted);
        self.unrouted_bytes = 0;
        for chat in events {
            self.route(chat, true)?;
        }
        Ok(())
    }
    fn remove(&mut self, logical: &str) {
        if let Some(run) = self.runs.remove(logical) {
            for id in run.aliases {
                self.aliases.remove(&id);
            }
        }
    }
    fn expire(&mut self) -> Result<()> {
        let now = Instant::now();
        let expired: Vec<_> = self
            .pending
            .iter()
            .filter(|(_, p)| p.deadline <= now)
            .map(|(id, _)| id.clone())
            .collect();
        for id in expired {
            let p = self.pending.remove(&id).unwrap();
            let error = Self::submission_error(&p);
            self.fail(p, error);
        }
        let mut remove = Vec::new();
        for (logical, run) in &mut self.runs {
            if run.deadline <= now {
                run.finish(
                    RunStatus::Failed,
                    "OpenClaw local run deadline; remote outcome unknown".into(),
                    RemoteOutcome::Unknown,
                );
            }
            if run.events.as_ref().is_some_and(mpsc::Sender::is_closed) {
                run.finish(
                    RunStatus::Cancelled,
                    "OpenClaw consumer dropped; remote outcome unknown".into(),
                    RemoteOutcome::Unknown,
                );
            }
            if run.shared.done.load(Ordering::Acquire)
                && !self
                    .pending
                    .values()
                    .any(|p| p.kind.logical() == Some(logical.as_str()))
            {
                remove.push(logical.clone());
            }
        }
        for logical in remove {
            self.remove(&logical);
        }
        if !self.pending.values().any(|p| p.kind.logical().is_some()) {
            self.unrouted.clear();
            self.unrouted_bytes = 0;
        }
        Ok(())
    }
}
fn parse_chat(v: &Value, bytes: usize, max: usize) -> Result<Option<Chat>> {
    let id = v
        .get("runId")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty() && s.len() <= 1024)
        .ok_or(GatewayError::Protocol("chat run id"))?;
    let session = match v.get("sessionKey") {
        None => None,
        Some(Value::String(s)) if !s.is_empty() && s.len() <= 1024 => Some(s.clone()),
        _ => return Err(GatewayError::Protocol("chat session")),
    };
    let state = v
        .get("state")
        .and_then(Value::as_str)
        .ok_or(GatewayError::Protocol("chat state"))?;
    let text = match state {
        "delta" => match v.get("deltaText") {
            None | Some(Value::Null) => String::new(),
            Some(Value::String(s)) => s.clone(),
            _ => return Err(GatewayError::Protocol("delta text")),
        },
        "final" | "aborted" => extract_text(v.get("message"), max)?,
        "error" => match v.get("errorMessage") {
            Some(Value::String(s)) => s.clone(),
            None | Some(Value::Null) => extract_text(v.get("message"), max)?,
            _ => return Err(GatewayError::Protocol("error message")),
        },
        _ => return Ok(None),
    };
    if text.len() > max {
        return Err(GatewayError::Capacity("chat text"));
    }
    Ok(Some(Chat {
        id: id.into(),
        session,
        state: state.into(),
        text,
        bytes,
    }))
}
fn extract_text(value: Option<&Value>, max: usize) -> Result<String> {
    let Some(v) = value else {
        return Ok(String::new());
    };
    if v.is_null() {
        return Ok(String::new());
    }
    if let Some(s) = v.as_str() {
        return if s.len() <= max {
            Ok(s.into())
        } else {
            Err(GatewayError::Capacity("terminal text"))
        };
    }
    let object = v
        .as_object()
        .ok_or(GatewayError::Protocol("terminal message"))?;
    for key in ["text", "content", "message", "output", "summary"] {
        if let Some(Value::String(s)) = object.get(key) {
            return if s.len() <= max {
                Ok(s.clone())
            } else {
                Err(GatewayError::Capacity("terminal text"))
            };
        }
    }
    if let Some(Value::Array(parts)) = object.get("content") {
        let mut text = String::new();
        for part in parts {
            let part = part
                .as_object()
                .ok_or(GatewayError::Protocol("terminal content part"))?;
            if let Some(value) = part.get("text") {
                let s = value
                    .as_str()
                    .ok_or(GatewayError::Protocol("terminal content text"))?;
                if text.len().saturating_add(s.len()) > max {
                    return Err(GatewayError::Capacity("terminal text"));
                }
                text.push_str(s);
            }
        }
        return Ok(text);
    }
    Err(GatewayError::Protocol("unsupported terminal message"))
}

#[cfg(test)]
#[path = "review_actor.rs"]
mod review_actor;
