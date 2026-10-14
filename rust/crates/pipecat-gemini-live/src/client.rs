use crate::{protocol, *};
use futures_util::{SinkExt, StreamExt};
use std::{
    collections::{HashMap, VecDeque},
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
};
use tokio::{
    sync::{mpsc, watch, Mutex},
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::{
    tungstenite::{client::IntoClientRequest, protocol::WebSocketConfig, Message},
    MaybeTlsStream, WebSocketStream,
};
use url::Url;

type Socket = WebSocketStream<MaybeTlsStream<tokio::net::TcpStream>>;
const LOCAL_TOOL_ID_PREFIX: &str = "pipecat-gemini-local-";
static NEXT_TOOL_SESSION: AtomicU64 = AtomicU64::new(1);

struct ToolIds {
    session: u64,
    next_call: u64,
}
impl ToolIds {
    fn new() -> Result<Self, GeminiError> {
        let session = NEXT_TOOL_SESSION
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| GeminiError::TooLarge)?;
        Ok(Self {
            session,
            next_call: 0,
        })
    }
    fn assign(&mut self, call: &mut FunctionCall) -> Result<(), GeminiError> {
        match call.id_source {
            ToolCallIdSource::Provider => {
                // Reserve the complete local namespace, including completed
                // IDs, without retaining an ever-growing replay history.
                if call.id.starts_with(LOCAL_TOOL_ID_PREFIX) {
                    return Err(GeminiError::Protocol(
                        "provider tool ID uses reserved local namespace",
                    ));
                }
            }
            ToolCallIdSource::LocalGenerated => {
                self.next_call = self.next_call.checked_add(1).ok_or(GeminiError::TooLarge)?;
                call.id = format!(
                    "{LOCAL_TOOL_ID_PREFIX}{:016x}-{:016x}",
                    self.session, self.next_call
                );
            }
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default)]
struct Control {
    cancel: bool,
    generation: u64,
}
struct Command {
    action: Option<Action>,
    origin: Origin,
    generation: u64,
}
struct PendingTool {
    call: FunctionCall,
    origin: Origin,
    generation: u64,
}
fn retire_tool(retired: &mut VecDeque<PendingTool>, tool: PendingTool, limit: usize) {
    if retired.len() == limit {
        retired.pop_front();
    }
    retired.push_back(tool);
}
fn retire_pending(
    pending: &mut HashMap<String, PendingTool>,
    retired: &mut VecDeque<PendingTool>,
    limit: usize,
) {
    for (_, tool) in pending.drain() {
        retire_tool(retired, tool, limit);
    }
}

#[derive(Clone)]
pub struct GeminiClient {
    config: Arc<GeminiConfig>,
}
impl GeminiClient {
    pub fn new(config: GeminiConfig) -> Result<Self, GeminiError> {
        config.limits.validate()?;
        config.session.validate(config.mode, &config.limits)?;
        protocol::check_json_size(&config.session, config.limits.max_message_bytes)?;
        if config.api_key.is_empty() || config.api_key.contains(['\r', '\n']) {
            return Err(GeminiError::Invalid("API key"));
        }
        if !(8_000..=96_000).contains(&config.input_sample_rate) {
            return Err(GeminiError::Invalid("input sample rate"));
        }
        endpoint(&config)?;
        if serde_json::to_vec(&protocol::setup(&config))
            .map_err(|_| GeminiError::Invalid("setup"))?
            .len()
            > config.limits.max_message_bytes
        {
            return Err(GeminiError::TooLarge);
        }
        Ok(Self {
            config: Arc::new(config),
        })
    }
    pub fn config(&self) -> &GeminiConfig {
        &self.config
    }
    /// Returns only after the peer acknowledges setup. Dropping this future
    /// drops its socket; it does not leave a detached handshake task.
    pub async fn connect(&self) -> Result<GeminiSession, GeminiError> {
        let config = &self.config;
        let url = endpoint(config)?;
        let request = url
            .as_str()
            .into_client_request()
            .map_err(|_| GeminiError::Invalid("WebSocket request"))?;
        let ws_config = WebSocketConfig::default()
            .max_message_size(Some(config.limits.max_message_bytes))
            .max_frame_size(Some(config.limits.max_message_bytes));
        let (mut socket, _) = timeout(
            config.limits.connect_timeout,
            tokio_tungstenite::connect_async_with_config(request, Some(ws_config), true),
        )
        .await
        .map_err(|_| GeminiError::Timeout("connect"))?
        .map_err(transport)?;
        timeout(config.limits.setup_timeout, async {
            socket
                .send(Message::Text(protocol::setup(config).to_string().into()))
                .await
                .map_err(transport)?;
            loop {
                let message = socket
                    .next()
                    .await
                    .ok_or(GeminiError::Closed)?
                    .map_err(transport)?;
                let bytes = match message {
                    Message::Text(s) => s.as_bytes().to_vec(),
                    Message::Binary(b) => b.to_vec(),
                    Message::Ping(bytes) => {
                        socket.send(Message::Pong(bytes)).await.map_err(transport)?;
                        continue;
                    }
                    Message::Pong(_) => continue,
                    Message::Close(_) => return Err(GeminiError::Closed),
                    _ => return Err(GeminiError::Protocol("unexpected WebSocket frame")),
                };
                let packet = protocol::parse(&bytes, config)?;
                if !packet.setup_complete
                    || !packet.events.is_empty()
                    || !packet.cancellations.is_empty()
                {
                    return Err(GeminiError::Protocol(
                        "expected setupComplete before content",
                    ));
                }
                return Ok(());
            }
        })
        .await
        .map_err(|_| GeminiError::Timeout("setup"))??;
        let (tx, rx) = mpsc::channel(config.limits.queue_capacity);
        let (events_tx, events_rx) = mpsc::channel(config.limits.queue_capacity);
        let (control_tx, control_rx) = watch::channel(Control::default());
        let sender = GeminiSender {
            config: config.clone(),
            tx,
            control: control_tx,
            generation: Arc::new(AtomicU64::new(0)),
            finishing: Arc::new(AtomicBool::new(false)),
            gate: Arc::new(Mutex::new(())),
            admission: Arc::new(std::sync::Mutex::new(())),
        };
        let worker_sender = sender.clone();
        let worker =
            tokio::spawn(
                async move { run(socket, rx, events_tx, control_rx, worker_sender).await },
            );
        Ok(GeminiSession {
            sender,
            events: events_rx,
            worker: Some(worker),
        })
    }
}
fn endpoint(config: &GeminiConfig) -> Result<Url, GeminiError> {
    let mut url = Url::parse(&config.endpoint).map_err(|_| GeminiError::Invalid("endpoint"))?;
    let loopback = url.host_str().is_some_and(|h| {
        h.parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
            || h == "[::1]"
    });
    if !(url.scheme() == "wss" || (url.scheme() == "ws" && loopback))
        || url.host().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err(GeminiError::Invalid(
            "endpoint requires TLS (or literal loopback), without userinfo, query, or fragment",
        ));
    }
    url.query_pairs_mut().append_pair("key", &config.api_key);
    Ok(url)
}
fn transport(error: tokio_tungstenite::tungstenite::Error) -> GeminiError {
    match error {
        tokio_tungstenite::tungstenite::Error::Http(response) => {
            GeminiError::Http(response.status().as_u16())
        }
        tokio_tungstenite::tungstenite::Error::Capacity(_) => GeminiError::TooLarge,
        _ => GeminiError::Transport,
    }
}

#[derive(Clone)]
pub struct GeminiSender {
    config: Arc<GeminiConfig>,
    tx: mpsc::Sender<Command>,
    control: watch::Sender<Control>,
    generation: Arc<AtomicU64>,
    finishing: Arc<AtomicBool>,
    gate: Arc<Mutex<()>>,
    admission: Arc<std::sync::Mutex<()>>,
}
impl GeminiSender {
    pub(crate) fn is_finishing(&self) -> bool {
        self.finishing.load(Ordering::SeqCst) || self.control.borrow().cancel
    }
    /// Admission never waits behind network I/O. A full queue is an explicit error.
    pub fn try_send(&self, action: Action, origin: Origin) -> Result<(), GeminiError> {
        self.try_send_generation(action, origin, None)
    }
    pub(crate) fn try_send_video_at_generation(
        &self,
        jpeg: Vec<u8>,
        origin: Origin,
        generation: u64,
    ) -> Result<(), GeminiError> {
        self.try_send_generation(Action::VideoJpeg(jpeg), origin, Some(generation))
    }
    fn try_send_generation(
        &self,
        action: Action,
        origin: Origin,
        expected: Option<u64>,
    ) -> Result<(), GeminiError> {
        let _admission = self.admission.lock().map_err(|_| GeminiError::Closed)?;
        if (self.finishing.load(Ordering::SeqCst) && !matches!(action, Action::ToolResponses(_)))
            || self.control.borrow().cancel
        {
            return Err(GeminiError::Closed);
        }
        if origin.context_id.as_ref().is_some_and(|s| s.len() > 1024) {
            return Err(GeminiError::TooLarge);
        }
        if expected.is_some_and(|generation| generation != self.generation()) {
            return Ok(());
        }
        protocol::action_messages(&action, &self.config)?;
        // Preserve ordinary callers' admission-time sampling; encoded video uses
        // its explicitly captured generation even if interrupt raced validation.
        let generation = expected.unwrap_or_else(|| self.generation());
        self.tx
            .try_send(Command {
                action: Some(action),
                origin,
                generation,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("input"),
                mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
            })
    }
    pub fn generation(&self) -> u64 {
        self.generation.load(Ordering::SeqCst)
    }
    /// Invalidate queued/generated output locally, without claiming a provider
    /// cancel or playback acknowledgement. Manual VAD activityStart, or actual
    /// audio under server VAD, is needed to interrupt the provider's generation.
    pub async fn interrupt(&self) {
        let _guard = self.gate.lock().await;
        let generation = self.generation.fetch_add(1, Ordering::SeqCst) + 1;
        self.control.send_modify(|c| c.generation = generation);
    }
    pub(crate) async fn while_current<F: std::future::Future>(
        &self,
        generation: u64,
        future: F,
    ) -> Option<F::Output> {
        let _guard = self.gate.lock().await;
        if generation != self.generation() {
            return None;
        }
        Some(future.await)
    }
    /// Cancellation bypasses both bounded queues and interrupts pending writes.
    pub fn cancel(&self) {
        self.control.send_modify(|c| c.cancel = true);
    }
    /// Drain admitted input, then wait for the active turn/final STT transcript.
    /// A missing completion is an error at the configured finish deadline.
    pub async fn finish(&self) -> Result<(), GeminiError> {
        if self.finishing.load(Ordering::SeqCst) {
            return Ok(());
        }
        // Reserving is cancellation-safe: the stop-admission flag is committed
        // only together with the queued finish command, without an await.
        let permit = timeout(self.config.limits.write_timeout, self.tx.reserve())
            .await
            .map_err(|_| GeminiError::Timeout("finish admission"))?
            .map_err(|_| GeminiError::Closed)?;
        let _admission = self.admission.lock().map_err(|_| GeminiError::Closed)?;
        if !self.finishing.swap(true, Ordering::SeqCst) {
            permit.send(Command {
                action: None,
                origin: Origin::default(),
                generation: self.generation(),
            });
        }
        Ok(())
    }
}

pub struct GeminiSession {
    sender: GeminiSender,
    events: mpsc::Receiver<GeminiEvent>,
    worker: Option<JoinHandle<Result<(), GeminiError>>>,
}
impl GeminiSession {
    pub fn sender(&self) -> GeminiSender {
        self.sender.clone()
    }
    pub async fn next_event(&mut self) -> Option<Result<GeminiEvent, GeminiError>> {
        while let Some(event) = self.events.recv().await {
            if !event.event.is_output() || event.generation == self.sender.generation() {
                return Some(Ok(event));
            }
        }
        let result = self.worker.as_mut()?.await;
        self.worker.take();
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(GeminiError::Transport)),
        }
    }
    pub async fn shutdown(mut self) -> Result<(), GeminiError> {
        self.sender.cancel();
        let result = match self.worker.as_mut() {
            Some(worker) => worker.await.map_err(|_| GeminiError::Transport)?,
            None => Ok(()),
        };
        self.worker.take();
        result
    }
}
impl Drop for GeminiSession {
    fn drop(&mut self) {
        self.sender.cancel();
        if let Some(worker) = &self.worker {
            worker.abort();
        }
    }
}

async fn write(
    socket: &mut Socket,
    message: Message,
    control: &mut watch::Receiver<Control>,
    duration: std::time::Duration,
) -> Result<(), GeminiError> {
    let sending = timeout(duration, socket.send(message));
    tokio::pin!(sending);
    loop {
        tokio::select! { biased;
            changed = control.changed() => { if changed.is_err() || control.borrow_and_update().cancel { return Err(GeminiError::Closed); } }
            result = &mut sending => return result.map_err(|_| GeminiError::Timeout("write"))?.map_err(transport),
        }
    }
}

async fn video_write_gate<'a>(
    sender: &'a GeminiSender,
    control: &mut watch::Receiver<Control>,
) -> Result<tokio::sync::MutexGuard<'a, ()>, GeminiError> {
    let locking = timeout(sender.config.limits.write_timeout, sender.gate.lock());
    tokio::pin!(locking);
    loop {
        if control.borrow().cancel {
            return Err(GeminiError::Closed);
        }
        tokio::select! { biased;
            changed=control.changed()=>{if changed.is_err() {return Err(GeminiError::Closed);}},
            result=&mut locking=>return result.map_err(|_|GeminiError::Timeout("video write gate")),
        }
    }
}

async fn run(
    mut socket: Socket,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<GeminiEvent>,
    mut control: watch::Receiver<Control>,
    sender: GeminiSender,
) -> Result<(), GeminiError> {
    let mut tool_ids = ToolIds::new()?;
    let mut pending = HashMap::<String, PendingTool>::new();
    // Retain a bounded cancellation window without making abandoned calls
    // actionable or allowing them to hold graceful shutdown open.
    let mut retired = VecDeque::<PendingTool>::new();
    let mut input_origin = Origin::default();
    let mut output_origin = Origin::default();
    let mut generation = sender.generation();
    let mut output_active = false;
    let mut busy = false;
    let mut suppressed = false;
    let mut activity = false;
    let mut has_audio = false;
    let mut finishing = false;
    let mut finish_at = Instant::now() + sender.config.limits.finish_timeout;
    let result = async {
        loop {
            if control.borrow().cancel {
                return Ok(());
            }
            let current = sender.generation();
            if current != generation {
                retire_pending(
                    &mut pending,
                    &mut retired,
                    sender.config.limits.max_pending_tools,
                );
                generation = current;
                suppressed = busy || output_active;
                output_active = false;
            }
            if finishing && !busy && pending.is_empty() {
                write(
                    &mut socket,
                    Message::Close(None),
                    &mut control,
                    sender.config.limits.write_timeout,
                )
                .await?;
                return Ok(());
            }
            enum Wake {
                Control,
                Deadline,
                Message(Option<Result<Message, tokio_tungstenite::tungstenite::Error>>),
                Command(Option<Command>),
            }
            let wake = tokio::select! { biased;
                _ = control.changed() => Wake::Control,
                _ = tokio::time::sleep_until(finish_at), if finishing => Wake::Deadline,
                message = socket.next() => Wake::Message(message),
                command = commands.recv() => Wake::Command(command),
            };
            match wake {
                Wake::Control => {
                    if control.borrow_and_update().cancel {
                        return Ok(());
                    }
                }
                Wake::Deadline => return Err(GeminiError::Timeout("finish")),
                Wake::Message(message) => {
                    let bytes = match message.ok_or(GeminiError::Closed)?.map_err(transport)? {
                        Message::Text(s) => s.as_bytes().to_vec(),
                        Message::Binary(b) => b.to_vec(),
                        Message::Ping(b) => {
                            write(
                                &mut socket,
                                Message::Pong(b),
                                &mut control,
                                sender.config.limits.write_timeout,
                            )
                            .await?;
                            continue;
                        }
                        Message::Pong(_) => continue,
                        Message::Close(_) => {
                            return if finishing && !busy && pending.is_empty() {
                                Ok(())
                            } else {
                                Err(GeminiError::Closed)
                            }
                        }
                        _ => return Err(GeminiError::Protocol("unexpected WebSocket frame")),
                    };
                    let packet = protocol::parse(&bytes, &sender.config)?;
                    if packet.setup_complete {
                        return Err(GeminiError::Protocol("duplicate setupComplete"));
                    }
                    for mut event in packet.events {
                        if matches!(event, Event::Interrupted) {
                            sender.interrupt().await;
                            retire_pending(
                                &mut pending,
                                &mut retired,
                                sender.config.limits.max_pending_tools,
                            );
                            generation = sender.generation();
                            suppressed = false;
                            output_active = false;
                            // The protocol follows interrupted with turnComplete.
                            busy = true;
                        }
                        if let Event::ToolCalls(calls) = &mut event {
                            for call in calls {
                                tool_ids.assign(call)?;
                                if pending.len() >= sender.config.limits.max_pending_tools {
                                    return Err(GeminiError::TooLarge);
                                }
                                if pending.contains_key(&call.id)
                                    || retired.iter().any(|tool| tool.call.id == call.id)
                                {
                                    return Err(GeminiError::Protocol("duplicate pending tool ID"));
                                }
                                if !sender
                                    .config
                                    .session
                                    .tools
                                    .iter()
                                    .any(|tool| tool.name == call.name)
                                {
                                    return Err(GeminiError::Protocol("undeclared function call"));
                                }
                                let tool = PendingTool {
                                    call: call.clone(),
                                    origin: if output_active {
                                        output_origin.clone()
                                    } else {
                                        input_origin.clone()
                                    },
                                    generation,
                                };
                                if suppressed {
                                    retire_tool(
                                        &mut retired,
                                        tool,
                                        sender.config.limits.max_pending_tools,
                                    );
                                } else {
                                    pending.insert(call.id.clone(), tool);
                                }
                            }
                        }
                        if matches!(event, Event::TurnComplete) {
                            busy = false;
                            has_audio = false;
                        }
                        if sender.config.mode == SessionMode::Transcription
                            && matches!(event, Event::InputTranscription { is_final: true, .. })
                        {
                            busy = false;
                            has_audio = false;
                        }
                        if event.is_output() && suppressed {
                            if matches!(event, Event::TurnComplete) {
                                suppressed = false;
                            }
                            continue;
                        }
                        if event.is_output() && !output_active {
                            output_origin = input_origin.clone();
                            output_active = true;
                            if !matches!(event, Event::TurnComplete) {
                                busy = true;
                            }
                        }
                        let origin = if event.is_output() || matches!(event, Event::ToolCalls(_)) {
                            output_origin.clone()
                        } else {
                            Origin::default()
                        };
                        let ended = matches!(event, Event::TurnComplete);
                        emit(
                            &events,
                            GeminiEvent {
                                event,
                                origin,
                                generation,
                            },
                        )?;
                        if ended {
                            output_active = false;
                        }
                    }
                    if !packet.cancellations.is_empty() {
                        let mut groups: Vec<(Origin, Vec<FunctionCall>)> = Vec::new();
                        for id in packet.cancellations {
                            let tool = pending.remove(&id).or_else(|| {
                                let index = retired.iter().position(|tool| tool.call.id == id)?;
                                retired.remove(index)
                            });
                            if let Some(tool) = tool {
                                if let Some((_, calls)) =
                                    groups.iter_mut().find(|(origin, _)| *origin == tool.origin)
                                {
                                    calls.push(tool.call);
                                } else {
                                    groups.push((tool.origin, vec![tool.call]));
                                }
                            } else {
                                return Err(GeminiError::Protocol(
                                    "cancellation references unknown tool ID",
                                ));
                            }
                        }
                        for (origin, calls) in groups {
                            emit(
                                &events,
                                GeminiEvent {
                                    event: Event::ToolCallsCancelled(calls),
                                    origin,
                                    generation,
                                },
                            )?;
                        }
                    }
                }
                Wake::Command(command) => {
                    let Some(command) = command else {
                        return Ok(());
                    };
                    let Some(action) = command.action else {
                        let flush = if activity {
                            Some(Action::ActivityEnd)
                        } else if has_audio && !sender.config.session.activity_detection.disabled {
                            Some(Action::AudioStreamEnd)
                        } else {
                            None
                        };
                        if let Some(flush) = flush {
                            for message in protocol::action_messages(&flush, &sender.config)? {
                                write(
                                    &mut socket,
                                    Message::Text(message.to_string().into()),
                                    &mut control,
                                    sender.config.limits.write_timeout,
                                )
                                .await?;
                            }
                            busy = true;
                        }
                        finishing = true;
                        finish_at = Instant::now() + sender.config.limits.finish_timeout;
                        continue;
                    };
                    // A video result keeps its original generation even if an external
                    // sender interrupts between encoding, enqueue and network write.
                    // The gate linearizes its write against interrupt(); cancellation
                    // still bypasses this gate and the write has a finite deadline.
                    let _video_gate = if matches!(action, Action::VideoJpeg(_)) {
                        Some(video_write_gate(&sender, &mut control).await?)
                    } else {
                        None
                    };
                    if matches!(action, Action::VideoJpeg(_))
                        && command.generation != sender.generation()
                    {
                        continue;
                    }
                    match &action {
                        Action::Audio(_)
                            if sender.config.session.activity_detection.disabled && !activity =>
                        {
                            return Err(GeminiError::Invalid(
                                "manual VAD audio outside activity window",
                            ))
                        }
                        Action::ActivityStart { .. } if activity => {
                            return Err(GeminiError::Invalid("activity already started"))
                        }
                        Action::ActivityEnd if !activity => {
                            return Err(GeminiError::Invalid("activity not started"))
                        }
                        Action::ToolResponses(responses) => {
                            for response in responses {
                                if !pending.get(&response.id).is_some_and(|tool| {
                                    tool.call.name == response.name
                                        && tool.generation == command.generation
                                        && command.generation == sender.generation()
                                }) {
                                    return Err(GeminiError::Invalid(
                                        "unknown, canceled, or mismatched tool result",
                                    ));
                                }
                            }
                        }
                        _ => {}
                    }
                    let messages = protocol::action_messages(&action, &sender.config)?;
                    for message in messages {
                        write(
                            &mut socket,
                            Message::Text(message.to_string().into()),
                            &mut control,
                            sender.config.limits.write_timeout,
                        )
                        .await?;
                    }
                    input_origin = command.origin;
                    match action {
                        Action::ActivityStart { preroll } => {
                            activity = true;
                            has_audio |= !preroll.is_empty();
                        }
                        Action::ActivityEnd => {
                            activity = false;
                            busy = true;
                        }
                        Action::Audio(_) => {
                            busy = true;
                            has_audio = true;
                        }
                        Action::AudioStreamEnd => {
                            busy = true;
                            has_audio = false;
                        }
                        Action::RealtimeText(_)
                        | Action::ClientContent {
                            turn_complete: true,
                            ..
                        } => busy = true,
                        Action::ToolResponses(responses) => {
                            for response in responses {
                                pending.remove(&response.id);
                            }
                            busy = true;
                        }
                        _ => {}
                    }
                }
            }
        }
    }
    .await;
    sender.finishing.store(true, Ordering::SeqCst);
    if sender.control.borrow().cancel {
        Ok(())
    } else {
        result
    }
}
fn emit(events: &mpsc::Sender<GeminiEvent>, event: GeminiEvent) -> Result<(), GeminiError> {
    events.try_send(event).map_err(|error| match error {
        mpsc::error::TrySendError::Full(_) => GeminiError::Backpressure("output"),
        mpsc::error::TrySendError::Closed(_) => GeminiError::Closed,
    })
}

#[cfg(test)]
mod ownership_tests {
    use super::*;
    use std::{task::Poll, time::Duration};
    use tokio::sync::oneshot;

    // Model the terminal cleanup interval: event delivery is closed, but the
    // connection task still owns an actual loopback WebSocket until its join.
    async fn held_terminal_session() -> (GeminiSession, oneshot::Sender<()>, JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/live", listener.local_addr().unwrap());
        let peer = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
            assert!(matches!(
                timeout(Duration::from_secs(2), socket.next())
                    .await
                    .expect("owned socket task leaked"),
                None | Some(Err(_)) | Some(Ok(Message::Close(_)))
            ));
        });
        let (socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
        let (release_tx, release_rx) = oneshot::channel();
        let (tx, commands) = mpsc::channel(1);
        let (events, events_rx) = mpsc::channel(1);
        drop(events);
        let (control, control_rx) = watch::channel(Control::default());
        let sender = GeminiSender {
            config: Arc::new(GeminiConfig::new("synthetic-key", "source-model")),
            tx,
            control,
            generation: Arc::new(AtomicU64::new(0)),
            finishing: Arc::new(AtomicBool::new(true)),
            gate: Arc::new(Mutex::new(())),
            admission: Arc::new(std::sync::Mutex::new(())),
        };
        let worker = tokio::spawn(async move {
            let _ = release_rx.await;
            drop((socket, commands, control_rx));
            Err(GeminiError::Protocol("synthetic terminal failure"))
        });
        (
            GeminiSession {
                sender,
                events: events_rx,
                worker: Some(worker),
            },
            release_tx,
            peer,
        )
    }

    #[tokio::test]
    async fn canceled_terminal_read_keeps_join_and_failure_for_retry() {
        let (mut session, release, peer) = held_terminal_session().await;
        {
            let read = session.next_event();
            tokio::pin!(read);
            assert!(matches!(futures_util::poll!(&mut read), Poll::Pending));
        }
        assert!(
            session.worker.is_some(),
            "terminal read detached the owned worker"
        );
        release.send(()).unwrap();
        assert_eq!(
            session.next_event().await,
            Some(Err(GeminiError::Protocol("synthetic terminal failure")))
        );
        assert!(session.next_event().await.is_none());
        peer.await.unwrap();
    }

    #[tokio::test]
    async fn drop_after_canceled_terminal_read_aborts_owned_socket() {
        let (mut session, release, peer) = held_terminal_session().await;
        {
            let read = session.next_event();
            tokio::pin!(read);
            assert!(matches!(futures_util::poll!(&mut read), Poll::Pending));
        }
        drop(session);
        peer.await.unwrap();
        assert!(release.send(()).is_err());
    }

    #[tokio::test]
    async fn canceled_shutdown_still_aborts_owned_socket() {
        let (session, release, peer) = held_terminal_session().await;
        {
            let shutdown = session.shutdown();
            tokio::pin!(shutdown);
            assert!(matches!(futures_util::poll!(&mut shutdown), Poll::Pending));
        }
        peer.await.unwrap();
        assert!(release.send(()).is_err());
    }
}

#[cfg(test)]
mod video_generation_tests {
    use super::*;
    #[tokio::test]
    async fn captured_video_generation_is_never_resampled_after_interrupt() {
        let (tx, mut rx) = mpsc::channel(2);
        let (control, _) = watch::channel(Control::default());
        let sender = GeminiSender {
            config: Arc::new(GeminiConfig::new("fixture", "model")),
            tx,
            control,
            generation: Arc::new(AtomicU64::new(0)),
            finishing: Arc::new(AtomicBool::new(false)),
            gate: Arc::new(Mutex::new(())),
            admission: Arc::new(std::sync::Mutex::new(())),
        };
        let captured = sender.generation();
        sender
            .try_send_video_at_generation(vec![255, 216, 255, 217], Origin::default(), captured)
            .unwrap();
        sender.interrupt().await;
        let queued = rx.try_recv().unwrap();
        assert_eq!(queued.generation, captured);
        assert_ne!(queued.generation, sender.generation());
        sender
            .try_send_video_at_generation(vec![255, 216, 255, 217], Origin::default(), captured)
            .unwrap();
        assert!(rx.try_recv().is_err());
        sender
            .try_send(
                Action::VideoJpeg(vec![255, 216, 255, 217]),
                Origin::default(),
            )
            .unwrap();
        assert_eq!(rx.try_recv().unwrap().generation, sender.generation());
    }
    #[tokio::test]
    async fn cancel_bypasses_held_video_write_gate_and_joins_socket() {
        let wait = std::time::Duration::from_secs(2);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GeminiConfig::new("fixture", "model");
        config.endpoint = format!("ws://{}/live", listener.local_addr().unwrap());
        let client = GeminiClient::new(config).unwrap();
        let mut peer = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
            let mut socket = tokio_tungstenite::accept_async(tcp)
                .await
                .map_err(|e| e.to_string())?;
            socket
                .next()
                .await
                .ok_or("no setup")?
                .map_err(|e| e.to_string())?;
            socket
                .send(Message::Text("{\"setupComplete\":{}}".into()))
                .await
                .map_err(|e| e.to_string())?;
            match socket.next().await {
                None | Some(Ok(Message::Close(_))) | Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake)))=>Ok::<_,String>(()),
                other=>Err(format!("unexpected late video: {other:?}")),
            }
        });
        let connected = client.connect().await;
        let mut session = match connected {
            Ok(s) => s,
            Err(e) => {
                peer.abort();
                let _ = peer.await;
                panic!("connect {e}");
            }
        };
        let sender = session.sender();
        let held = sender.gate.lock().await;
        let sent = sender.try_send(
            Action::VideoJpeg(vec![255, 216, 255, 217]),
            Origin::default(),
        );
        let dequeued = timeout(wait, async {
            while sender.tx.capacity() != sender.config.limits.queue_capacity {
                tokio::task::yield_now().await;
            }
        })
        .await;
        sender.cancel();
        let result = timeout(wait, session.next_event()).await;
        let bypassed = result.is_ok();
        drop(held); // Always release, including the old-deadlock failure path.
        let result = match result {
            Ok(r) => r,
            Err(_) => session.next_event().await,
        };
        let shutdown = session.shutdown().await;
        let peer_result = match timeout(wait, &mut peer).await {
            Ok(r) => r,
            Err(e) => {
                peer.abort();
                let _ = peer.await;
                panic!("peer aborted and joined {e}");
            }
        };
        assert!(sent.is_ok());
        assert!(dequeued.is_ok());
        assert!(bypassed);
        assert!(result.is_none());
        assert!(shutdown.is_ok());
        assert!(peer_result.unwrap().is_ok());
    }
}
