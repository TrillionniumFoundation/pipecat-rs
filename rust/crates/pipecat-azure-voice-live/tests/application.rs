//! Public Voice Live app acceptance against literal-loopback synthetic peers.
//! Real paired aggregators, ToolExecutor callbacks, terminal carriers and output
//! processors are used. No live provider, credentials or playback is certified.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{FutureExt, SinkExt, StreamExt};
use pipecat_azure_voice_live::{
    FunctionTool, PlaybackPosition, TurnDetection, VoiceLiveApp, VoiceLiveConfig,
    VoiceLiveCredentials, VoiceLiveHandle, VoiceLiveHistory, VoiceLiveLocalTool,
};
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolOutcomeJournal};
use pipecat_services::{
    Content, ContentPart, Context, Message as HistoryMessage, Role as HistoryRole,
};
use serde_json::{json, Value};
use std::{
    cell::RefCell,
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, Notify},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::tungstenite::{handshake::server::Request, Message};

const WAIT: Duration = Duration::from_secs(8);
const QUIET: Duration = Duration::from_millis(60);
const MODEL: &str = "synthetic-model";
const VOICE: &str = "en-US-SyntheticNeural";
const KEY: &str = "synthetic-voice-live-key-not-a-credential";
const BEARER: &str = "synthetic-voice-live-bearer-not-a-credential";
async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(WAIT, future)
        .await
        .expect("bounded synthetic app operation")
}
#[derive(Clone, Copy)]
struct Mode {
    bearer: bool,
    semantic: bool,
}
impl Mode {
    const MANUAL: Self = Self {
        bearer: false,
        semantic: false,
    };
    fn config(self, endpoint: String) -> VoiceLiveConfig {
        let credentials = if self.bearer {
            VoiceLiveCredentials::EntraBearer(BEARER.into())
        } else {
            VoiceLiveCredentials::ApiKey(KEY.into())
        };
        let mut c = VoiceLiveConfig::new(endpoint, MODEL, credentials, VOICE);
        c.limits.connect_timeout = Duration::from_secs(2);
        c.limits.operation_timeout = Duration::from_secs(1);
        c.limits.response_timeout = Duration::from_secs(3);
        c.limits.close_timeout = Duration::from_millis(200);
        if self.semantic {
            c.session.turn_detection = TurnDetection::semantic_vad();
        }
        c
    }
}
enum PeerInput {
    Event(Value),
    Ping(oneshot::Sender<()>),
    Close,
}
struct Peer {
    events: mpsc::UnboundedSender<PeerInput>,
    commands: mpsc::UnboundedReceiver<Value>,
    pongs: mpsc::UnboundedReceiver<Vec<u8>>,
    owner: Arc<PeerOwner>,
}
struct PeerOwner {
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
    events: mpsc::UnboundedSender<PeerInput>,
}
impl PeerOwner {
    async fn join(&self) -> Result<(), String> {
        // Keep the real handle in its owner across every await. If the outer
        // case deadline cancels this waiter, cleanup can still join the task.
        let mut slot = self.task.lock().await;
        let Some(task) = slot.as_mut() else {
            return Ok(());
        };
        let result = match timeout(WAIT, &mut *task).await {
            Ok(Ok(())) => Ok(()),
            Ok(Err(error)) => Err(format!("synthetic peer task failed: {error}")),
            Err(_) => {
                task.abort();
                let _ = task.await;
                Err("synthetic peer timed out; task was aborted and joined".into())
            }
        };
        slot.take();
        result
    }
}
#[derive(Default)]
struct CaseResources {
    peers: Vec<Arc<PeerOwner>>,
    apps: Vec<(VoiceLiveHandle, ToolOutcomeJournal)>,
}
tokio::task_local! { static RESOURCES: RefCell<CaseResources>; }
/// Keep cleanup ownership outside every assertion future. A fixture panic or
/// timeout first drops its workers, then joins every retained peer and observes
/// the actual provider owner and executor journal before propagating failure.
async fn run_case(body: impl Future<Output = ()>) {
    RESOURCES
        .scope(RefCell::new(CaseResources::default()), async {
            let result = timeout(
                Duration::from_secs(60),
                AssertUnwindSafe(body).catch_unwind(),
            )
            .await;
            let resources =
                RESOURCES.with(|resources| std::mem::take(&mut *resources.borrow_mut()));
            let mut cleanup_errors = vec![];
            for peer in resources.peers {
                if peer.task.lock().await.is_some() {
                    let _ = peer.events.send(PeerInput::Close);
                }
                if let Err(error) = peer.join().await {
                    cleanup_errors.push(error);
                }
            }
            for (provider, outcomes) in resources.apps {
                if timeout(WAIT, async {
                    while !provider.session_joined() || !outcomes.snapshot().active.is_empty() {
                        tokio::time::sleep(Duration::from_millis(1)).await;
                    }
                })
                .await
                .is_err()
                {
                    cleanup_errors.push(format!(
                        "application cleanup incomplete: actor_joined={}, active_tools={}",
                        provider.session_joined(),
                        outcomes.snapshot().active.len()
                    ));
                }
            }
            match result {
                Ok(Ok(())) => assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}"),
                Ok(Err(panic)) => {
                    if !cleanup_errors.is_empty() {
                        eprintln!("cleanup after fixture panic: {cleanup_errors:?}");
                    }
                    std::panic::resume_unwind(panic);
                }
                Err(_) => {
                    panic!("synthetic application case timed out; cleanup: {cleanup_errors:?}")
                }
            }
        })
        .await;
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite owns this callback error type.
    async fn new(mode: Mode) -> (Self, VoiceLiveConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = mode.config(format!(
            "ws://{}/voice-live/realtime",
            listener.local_addr().unwrap()
        ));
        let (events, mut incoming) = mpsc::unbounded_channel();
        let (outgoing, commands) = mpsc::unbounded_channel();
        let (pong_tx, pongs) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (tcp, _) = bounded(listener.accept()).await.unwrap();
            let mut ws = bounded(tokio_tungstenite::accept_hdr_async(
                tcp,
                move |request: &Request, response| {
                    assert_eq!(request.uri().path(), "/voice-live/realtime");
                    assert_eq!(
                        request.uri().query(),
                        Some("api-version=2026-07-15&model=synthetic-model")
                    );
                    assert!(!request.headers().contains_key("openai-beta"));
                    if mode.bearer {
                        assert_eq!(
                            request.headers()["authorization"],
                            format!("Bearer {BEARER}")
                        );
                        assert!(!request.headers().contains_key("api-key"));
                    } else {
                        assert_eq!(request.headers()["api-key"], KEY);
                        assert!(!request.headers().contains_key("authorization"));
                    }
                    Ok(response)
                },
            ))
            .await
            .unwrap();
            ws.send(Message::Text(
                json!({"type":"session.created","session":{"id":"synthetic-session"}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            let mut serial = 0_u64;
            let mut update_seen = false;
            let mut closing = false;
            loop {
                tokio::select! {
                    event = incoming.recv() => {
                        let Some(event) = event else { break; };
                        match event {
                            PeerInput::Event(mut event) => {
                                serial += 1;
                                event["event_id"] = json!(format!("synthetic-{serial}"));
                                if ws.send(Message::Text(event.to_string().into())).await.is_err() { break; }
                            }
                            PeerInput::Ping(written) => {
                                if ws.send(Message::Ping(b"synthetic-wire-barrier".to_vec().into())).await.is_err() { break; }
                                let _ = written.send(());
                            }
                            PeerInput::Close => { let _ = ws.close(None).await; closing = true; }
                        }
                    }
                    message = ws.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value = serde_json::from_str(&text).unwrap();
                            assert!(value["event_id"].as_str().is_some_and(|id| !id.is_empty()));
                            let update = value["type"] == "session.update";
                            if !update_seen { assert!(update, "work preceded session.update: {value}"); }
                            if update { assert!(!update_seen, "unexpected session rebinding"); update_seen = true; }
                            if outgoing.send(value).is_err() { break; }
                            if update {
                                ws.send(Message::Text(json!({"type":"session.updated","session":{"id":"synthetic-session","model":"canonical-alias"}}).to_string().into())).await.unwrap();
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => { let _ = ws.close(None).await; break; }
                        Some(Ok(Message::Pong(bytes))) => { let _ = pong_tx.send(bytes.to_vec()); }
                        Some(Err(tokio_tungstenite::tungstenite::Error::AlreadyClosed | tokio_tungstenite::tungstenite::Error::ConnectionClosed)) if closing => break,
                        Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake))) if closing => break,
                        Some(Err(error)) => panic!("unexpected synthetic peer failure: {error}"),
                        _ => {}
                    }
                }
            }
        });
        let owner = Arc::new(PeerOwner {
            task: tokio::sync::Mutex::new(Some(task)),
            events: events.clone(),
        });
        RESOURCES.with(|resources| resources.borrow_mut().peers.push(owner.clone()));
        (
            Self {
                events,
                commands,
                pongs,
                owner,
            },
            config,
        )
    }
    fn event(&self, value: Value) {
        self.events.send(PeerInput::Event(value)).unwrap();
    }
    /// Confirms only that earlier queued events and this ping reached the
    /// socket. A valid semantic admission pause must not answer it yet.
    async fn write_ping(&self) {
        let (written, completion) = oneshot::channel();
        self.events.send(PeerInput::Ping(written)).unwrap();
        bounded(completion).await.unwrap();
    }
    async fn pong(&mut self) {
        assert_eq!(
            bounded(self.pongs.recv()).await.as_deref(),
            Some(b"synthetic-wire-barrier".as_slice())
        );
    }
    async fn command(&mut self) -> Value {
        self.command_at("expected synthetic wire command").await
    }
    async fn command_at(&mut self, stage: &str) -> Value {
        timeout(WAIT, self.commands.recv())
            .await
            .unwrap_or_else(|_| panic!("wire timeout while waiting for {stage}"))
            .unwrap_or_else(|| panic!("peer closed while waiting for {stage}"))
    }
    async fn quiet(&mut self) {
        assert!(
            timeout(QUIET, self.commands.recv()).await.is_err(),
            "unexpected wire command or peer closure"
        );
    }
    async fn startup(&mut self) -> Vec<Value> {
        let mut commands = vec![self.command().await];
        while let Ok(command) = timeout(QUIET, self.commands.recv()).await {
            commands.push(command.expect("peer closed during startup"));
        }
        commands
    }
    fn created(&self, response: &str) {
        self.event(json!({"type":"response.created","response":{"id":response,"status":"in_progress","output":[]}}));
    }
    fn assistant(&self, response: &str, item: &str) {
        self.event(json!({"type":"response.output_item.added","response_id":response,"output_index":0,"item":{"type":"message","id":item,"role":"assistant","content":[]}}));
    }
    fn duplicate_assistant(&self, item: &str) {
        self.event(json!({"type":"conversation.item.created","previous_item_id":"","item":{"type":"message","id":item,"role":"assistant","content":[]}}));
    }
    fn done(&self, response: &str) {
        self.event(json!({"type":"response.done","response":{"id":response,"status":"completed","output":[],"usage":{"input_tokens":2,"output_tokens":3,"total_tokens":5}}}));
    }
    fn transcript(&self, item: &str, text: &str) {
        self.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":item,"content_index":0,"transcript":text}));
    }
    fn audio(&self, response: &str, item: &str, bytes: &[u8]) {
        self.event(json!({"type":"response.audio.delta","response_id":response,"item_id":item,"content_index":0,"delta":STANDARD.encode(bytes)}));
    }
    fn spoken(&self, response: &str, item: &str, text: &str) {
        self.audio(response, item, &[1, 2]);
        self.audio(response, item, &[3, 4, 5, 6]);
        self.event(json!({"type":"response.audio_transcript.delta","response_id":response,"item_id":item,"content_index":0,"delta":text}));
        self.event(json!({"type":"response.audio.done","response_id":response,"item_id":item,"content_index":0}));
    }
    fn tool_item(&self, response: &str, id: &str, name: &str, duplicate: bool) {
        let item = json!({"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":name,"arguments":""});
        self.event(json!({"type":"response.output_item.added","response_id":response,"output_index":0,"item":item}));
        if duplicate {
            self.event(
                json!({"type":"conversation.item.created","previous_item_id":"","item":item}),
            );
        }
    }
    fn arguments(&self, response: &str, id: &str, arguments: &str) {
        // Omitted name must bind to the unique previously announced function.
        self.event(json!({"type":"response.function_call_arguments.done","response_id":response,"item_id":format!("item-{id}"),"call_id":id,"arguments":arguments}));
    }
    fn tool(&self, response: &str, id: &str, name: &str, arguments: &str) {
        self.tool_item(response, id, name, false);
        self.arguments(response, id, arguments);
    }
    async fn closed(mut self) -> Vec<Value> {
        self.owner.join().await.expect("synthetic peer task joined");
        let mut remaining = vec![];
        while let Ok(command) = self.commands.try_recv() {
            remaining.push(command);
        }
        remaining
    }
}
#[derive(Default)]
struct Probe {
    frames: Mutex<Vec<DirectedFrame>>,
    gate_terminal: AtomicBool,
    fail_audio: AtomicBool,
    gate_audio: AtomicBool,
    entered: Notify,
    release: Notify,
    dropped: Arc<AtomicBool>,
}
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
/// The app starts processors sequentially. Its provider has connected by the
/// time this output setup runs, but native processing and provider event mapping
/// cannot start until setup returns. This is a real public output processor,
/// not an alternate topology or a production-only test hook.
#[derive(Default)]
struct SetupGate {
    probe: Arc<Probe>,
    entered: Notify,
    release: Notify,
    dropped: Arc<AtomicBool>,
    cleaned: AtomicBool,
}
#[async_trait]
impl FrameProcessor for SetupGate {
    fn name(&self) -> &str {
        "SyntheticOutputSetupGate"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let _drop = DropFlag(self.dropped.clone());
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.probe.process(frame, direction, ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.store(true, Ordering::SeqCst);
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "SyntheticVoiceLiveOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        if matches!(frame.data, FrameData::TtsAudio(_)) && self.fail_audio.load(Ordering::SeqCst) {
            return Err(ProcessorError::fatal("synthetic output failure"));
        }
        let terminal = frame.native_tool_outcome()?.is_some();
        if (terminal && self.gate_terminal.swap(false, Ordering::SeqCst))
            || (matches!(frame.data, FrameData::TtsAudio(_))
                && self.gate_audio.swap(false, Ordering::SeqCst))
        {
            let _drop = DropFlag(self.dropped.clone());
            self.entered.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}
struct Harness {
    worker: PipelineWorker,
    pair: VoiceLiveHistory,
    provider: VoiceLiveHandle,
    outcomes: ToolOutcomeJournal,
    peer: Peer,
    probe: Arc<Probe>,
    startup: Vec<Value>,
}
impl Harness {
    async fn new(mode: Mode, context: Context, tools: Vec<VoiceLiveLocalTool>) -> Self {
        Self::configured(mode, context, tools, Arc::new(Probe::default()), |_| {}).await
    }
    async fn configured(
        mode: Mode,
        context: Context,
        tools: Vec<VoiceLiveLocalTool>,
        probe: Arc<Probe>,
        configure: impl FnOnce(&mut VoiceLiveConfig),
    ) -> Self {
        let (mut peer, mut config) = Peer::new(mode).await;
        configure(&mut config);
        let running = bounded(
            VoiceLiveApp::new(config, context, tools, probe.clone())
                .unwrap()
                .start(),
        )
        .await
        .unwrap();
        RESOURCES.with(|resources| {
            resources
                .borrow_mut()
                .apps
                .push((running.provider.clone(), running.outcomes.clone()))
        });
        let startup = peer.startup().await;
        let session = &startup[0]["session"];
        assert_eq!(startup[0]["type"], "session.update");
        assert_eq!(session["modalities"], json!(["text", "audio"]));
        assert_eq!(
            session["voice"],
            json!({"type":"azure-standard","name":VOICE})
        );
        assert_eq!(session["input_audio_format"], "pcm16");
        assert_eq!(session["input_audio_sampling_rate"], 24000);
        assert_eq!(session["output_audio_format"], "pcm16");
        assert_eq!(
            session["input_audio_transcription"],
            json!({"model":"azure-speech"})
        );
        assert!(
            session.get("model").is_none()
                && session.get("audio").is_none()
                && session.get("output_modalities").is_none()
        );
        if mode.semantic {
            assert_eq!(
                session["turn_detection"],
                json!({"type":"azure_semantic_vad","silence_duration_ms":500,"create_response":true,"interrupt_response":true,"auto_truncate":false,"remove_filler_words":true})
            );
        } else {
            assert!(session.get("turn_detection").is_some_and(Value::is_null));
        }
        assert!(startup.iter().all(|v| matches!(
            v["type"].as_str(),
            Some("session.update" | "conversation.item.create")
        )));
        assert_eq!(
            startup
                .iter()
                .filter(|v| v["type"] == "session.update")
                .count(),
            1
        );
        let mut h = Self {
            worker: running.worker,
            pair: running.pair,
            provider: running.provider,
            outcomes: running.outcomes,
            peer,
            probe,
            startup,
        };
        h.barrier().await;
        assert!(h.probe.frames.lock().unwrap().iter().any(|f| matches!(&f.frame.data, FrameData::LlmServiceMetadata(m) if m.is_realtime_service && m.base.service_name == "AzureVoiceLiveService")));
        h
    }
    async fn send(&self, data: FrameData) {
        self.send_frame(Frame::new(data)).await;
    }
    async fn send_frame(&self, frame: Frame) {
        bounded(self.worker.sender().send(frame, Direction::Downstream))
            .await
            .unwrap();
    }
    async fn barrier(&mut self) {
        self.send(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("synthetic-app-barrier".into()),
        }))
        .await;
        bounded(async {
            loop {
                let f = self.worker.recv().await.expect("pipeline closed before barrier");
                if let FrameData::Error(e) | FrameData::FatalError(e) = &f.frame.data { panic!("unexpected pipeline error: {e:?}"); }
                if matches!(&f.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("synthetic-app-barrier")) { break; }
            }
        }).await;
    }
    async fn observed(&self, predicate: impl Fn(&FrameData) -> bool) {
        bounded(async {
            loop {
                if self
                    .probe
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|f| predicate(&f.frame.data))
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
    }
    async fn observed_count(&self, predicate: impl Fn(&FrameData) -> bool, expected: usize) {
        timeout(WAIT, async {
            loop {
                if self
                    .probe
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|f| predicate(&f.frame.data))
                    .count()
                    >= expected
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("expected fresh per-turn frame count {expected}"));
    }
    async fn completed(&mut self, count: u64) {
        bounded(async {
            while self.provider.responses_completed() < count {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        self.barrier().await;
    }
    async fn run(&mut self, response: &str, assistant: bool) {
        self.send(FrameData::LlmRun).await;
        assert_create(self.peer.command().await);
        self.peer.created(response);
        if assistant {
            self.peer.assistant(response, &format!("audio-{response}"));
            self.observed(|f| matches!(f, FrameData::LlmResponseStart))
                .await;
        }
    }
    async fn rejected(&mut self) {
        bounded(async {
            loop {
                let f = self
                    .worker
                    .recv()
                    .await
                    .expect("pipeline closed before recoverable rejection");
                if let FrameData::Error(e) = f.frame.data {
                    assert!(!e.fatal, "{e:?}");
                    break;
                }
            }
        })
        .await;
    }
    async fn finish(self, expected_errors: u64) {
        bounded(self.worker.end()).await.unwrap();
        let (result, commands) = tokio::join!(bounded(self.worker.collect()), self.peer.closed());
        let (_, report) = result.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, expected_errors, "{:?}", report.errors);
        assert_eq!(report.errors.len() as u64, expected_errors);
        assert_eq!(report.evicted_errors, 0);
        assert_eq!(report.dropped_frames, 0);
        assert!(
            commands.is_empty(),
            "unexpected terminal commands: {commands:?}"
        );
        assert!(self.outcomes.snapshot().active.is_empty());
        assert!(self.provider.session_joined());
    }
}
fn assert_create(value: Value) {
    assert_eq!(value["type"], "response.create");
    assert!(
        value.get("response").is_none(),
        "ordinary create must inherit immutable session"
    );
    assert_eq!(value.as_object().unwrap().len(), 2);
}
fn assert_text(value: Value, text: &str) {
    assert_eq!(value["type"], "conversation.item.create");
    assert_eq!(value["item"]["type"], "message");
    assert_eq!(value["item"]["role"], "user");
    assert_eq!(
        value["item"]["content"],
        json!([{"type":"input_text","text":text}])
    );
}
fn assert_result(value: Value, call: &str, output: &str) {
    assert_eq!(value["type"], "conversation.item.create");
    assert_eq!(value["item"]["type"], "function_call_output");
    assert_eq!(value["item"]["call_id"], call);
    assert_eq!(value["item"]["output"], output);
}
fn history() -> Context {
    Context::new(vec![
        HistoryMessage::text(HistoryRole::System, "seed instructions"),
        HistoryMessage::text(HistoryRole::User, "old question"),
        HistoryMessage::text(HistoryRole::Assistant, "old answer"),
    ])
}
fn texts(pair: &VoiceLiveHistory, role: HistoryRole) -> Vec<String> {
    pair.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter(|m| m.role == role)
        .filter_map(|m| match m.content {
            Some(Content::Text(text)) => Some(text),
            _ => None,
        })
        .collect()
}
fn pcm(bytes: Vec<u8>, user: bool) -> Frame {
    let audio = AudioRawFrame {
        audio: bytes,
        sample_rate: 24000,
        num_channels: 1,
        context_id: Some("synthetic-capture".into()),
    };
    let mut frame = Frame::new(if user {
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "synthetic-speaker".into(),
        })
    } else {
        FrameData::InputAudio(audio)
    });
    frame.metadata.insert("sequence".into(), json!(9));
    frame.transport_source = Some("synthetic-recorder".into());
    frame
}
fn append(text: &str, run_llm: bool) -> FrameData {
    FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        run_llm,
    })
}
fn echo(counter: Arc<AtomicUsize>) -> VoiceLiveLocalTool {
    let mut handler = RegisteredTool::new(Arc::new(move |call| {
        let counter = counter.clone();
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(call.arguments)
        })
    }));
    handler.timeout = Duration::from_secs(1);
    VoiceLiveLocalTool {
        schema: FunctionTool::new(
            "echo",
            "Synthetic echo",
            json!({"type":"object","properties":{"n":{"type":"number"}},"required":["n"]}),
        ),
        handler,
    }
}
fn delayed(
    entered: Arc<Notify>,
    release: Arc<Notify>,
    counter: Arc<AtomicUsize>,
    value: Value,
) -> VoiceLiveLocalTool {
    let mut handler = RegisteredTool::new(Arc::new(move |_| {
        let (entered, release, counter, value) = (
            entered.clone(),
            release.clone(),
            counter.clone(),
            value.clone(),
        );
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            entered.notify_one();
            release.notified().await;
            Ok(value)
        })
    }));
    handler.timeout = Duration::from_secs(2);
    VoiceLiveLocalTool {
        schema: FunctionTool::new(
            "wait",
            "Synthetic bounded callback",
            json!({"type":"object"}),
        ),
        handler,
    }
}

async fn matrix(mode: Mode) {
    let callbacks = Arc::new(AtomicUsize::new(0));
    let mut previous_receipt = None;
    for fresh in 0..2 {
        let mut h = Harness::new(mode, history(), vec![echo(callbacks.clone())]).await;
        assert_eq!(h.outcomes.snapshot().total_terminal, 0);
        assert_eq!(texts(&h.pair, HistoryRole::User), ["old question"]);
        assert_eq!(h.startup[0]["session"]["instructions"], "seed instructions");
        let seed: Vec<_> = h
            .startup
            .iter()
            .filter(|v| v["type"] == "conversation.item.create")
            .collect();
        assert_eq!(seed.len(), 1);
        let packed = seed[0]["item"]["content"][0]["text"].as_str().unwrap();
        assert!(packed.contains("old question") && packed.contains("old answer"));
        assert!(!packed.contains("seed instructions"));
        let mut completions = 0;
        for turn in 0..2 {
            for user in [false, true] {
                let bytes = vec![1 + turn as u8, 2, 3, 4];
                let input = pcm(bytes.clone(), user);
                let expected = input.clone();
                h.send_frame(input).await;
                let wire = h
                    .peer
                    .command_at(&format!("matrix fresh {fresh} turn {turn} input PCM"))
                    .await;
                assert_eq!(wire["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD.decode(wire["audio"].as_str().unwrap()).unwrap(),
                    bytes
                );
                h.barrier().await;
                assert!(h
                    .probe
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|f| f.frame.id == expected.id
                        && f.frame.data == expected.data
                        && f.frame.metadata == expected.metadata
                        && f.frame.transport_source == expected.transport_source));
            }
            let user_item = format!("spoken-{turn}");
            if mode.semantic {
                h.peer.event(json!({"type":"input_audio_buffer.speech_started","item_id":user_item,"audio_start_ms":0}));
                h.observed_count(|f| matches!(f, FrameData::UserStartedSpeaking), turn + 1)
                    .await;
                h.peer.event(json!({"type":"input_audio_buffer.speech_stopped","item_id":user_item,"audio_end_ms":10}));
                h.observed_count(|f| matches!(f, FrameData::UserStoppedSpeaking), turn + 1)
                    .await;
                h.barrier().await;
                h.peer.quiet().await;
            } else {
                h.send(FrameData::UserStoppedSpeaking).await;
                assert_eq!(h.peer.command().await["type"], "input_audio_buffer.commit");
                assert_create(h.peer.command().await);
                h.send(FrameData::UserStoppedSpeaking).await;
                h.peer.event(json!({"type":"input_audio_buffer.speech_started","item_id":"spurious","audio_start_ms":0}));
                h.peer.event(json!({"type":"input_audio_buffer.speech_stopped","item_id":"spurious","audio_end_ms":1}));
                h.barrier().await;
                h.peer.quiet().await;
            }
            let response = format!("response-{turn}");
            let item = format!("audio-{turn}");
            h.peer.created(&response);
            h.peer.assistant(&response, &item);
            h.peer.duplicate_assistant(&item);
            let question = format!("question {fresh}-{turn}");
            h.peer.transcript(&user_item, &question);
            h.peer.transcript(&user_item, "  ");
            h.peer.transcript(&user_item, &question);
            if turn == 0 {
                h.peer
                    .spoken(&response, &item, &format!("answer {fresh}-{turn}"));
                h.peer.done(&response);
                completions += 1;
            } else {
                h.peer
                    .tool_item(&response, "shared-id-across-fresh-sessions", "echo", true);
                h.peer
                    .arguments(&response, "shared-id-across-fresh-sessions", "{\"n\":7}");
                assert_result(
                    h.peer
                        .command_at(&format!(
                            "matrix fresh {fresh} turn {turn} actual native result"
                        ))
                        .await,
                    "shared-id-across-fresh-sessions",
                    "{\"n\":7}",
                );
                h.observed(|f| matches!(f, FrameData::FunctionCallResult(_)))
                    .await;
                h.barrier().await;
                h.peer.quiet().await;
                let terminal = h
                    .probe
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
                    .unwrap()
                    .frame
                    .clone();
                let receipt = terminal.native_tool_outcome().unwrap().unwrap();
                assert_eq!(receipt.state(), ToolCallTerminalState::Completed);
                assert_ne!(previous_receipt, Some(receipt.id()));
                previous_receipt = Some(receipt.id());
                h.peer.done(&response);
                completions += 1;
                assert_create(
                    h.peer
                        .command_at(&format!(
                            "matrix fresh {fresh} turn {turn} sealed continuation"
                        ))
                        .await,
                );
                h.peer.created("continuation");
                h.peer.assistant("continuation", "continuation-audio");
                h.peer.spoken(
                    "continuation",
                    "continuation-audio",
                    &format!("answer {fresh}-{turn}"),
                );
                h.peer.done("continuation");
                completions += 1;
            }
            h.completed(completions).await;
            bounded(async {
                loop {
                    if texts(&h.pair, HistoryRole::Assistant)
                        .iter()
                        .any(|text| text == &format!("answer {fresh}-{turn}"))
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await;
            h.barrier().await;
            h.peer.quiet().await;
        }
        assert_eq!(
            texts(&h.pair, HistoryRole::User),
            [
                "old question".into(),
                format!("question {fresh}-0"),
                format!("question {fresh}-1")
            ]
        );
        let assistant = texts(&h.pair, HistoryRole::Assistant);
        for expected in [
            "old answer".to_owned(),
            format!("answer {fresh}-0"),
            format!("answer {fresh}-1"),
        ] {
            assert_eq!(
                assistant.iter().filter(|text| **text == expected).count(),
                1
            );
        }
        {
            let frames = h.probe.frames.lock().unwrap();
            let audio: Vec<u8> = frames
                .iter()
                .filter_map(|f| match &f.frame.data {
                    FrameData::TtsAudio(a) => {
                        assert_eq!((a.sample_rate, a.num_channels), (24000, 1));
                        Some(a.audio.as_slice())
                    }
                    _ => None,
                })
                .flatten()
                .copied()
                .collect();
            assert_eq!(audio, [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6]);
            for (predicate, count) in [
                (
                    (|d: &FrameData| matches!(d, FrameData::LlmResponseStart))
                        as fn(&FrameData) -> bool,
                    3,
                ),
                (
                    (|d| matches!(d, FrameData::LlmResponseEnd)) as fn(&FrameData) -> bool,
                    3,
                ),
                (
                    (|d| matches!(d, FrameData::TtsStarted(_))) as fn(&FrameData) -> bool,
                    2,
                ),
                (
                    (|d| matches!(d, FrameData::TtsStopped(_))) as fn(&FrameData) -> bool,
                    2,
                ),
            ] {
                assert_eq!(
                    frames.iter().filter(|f| predicate(&f.frame.data)).count(),
                    count
                );
            }
            assert_eq!(
                frames
                    .iter()
                    .filter(
                        |f| matches!(&f.frame.data, FrameData::LlmText(t) if !t.append_to_context)
                    )
                    .count(),
                2
            );
        }
        let journal = h.outcomes.snapshot();
        assert_eq!(journal.total_terminal, 1);
        assert!(journal.active.is_empty());
        assert_eq!(journal.unadmitted_terminal, 0);
        assert_eq!(callbacks.load(Ordering::SeqCst), fresh + 1);
        h.finish(0).await;
    }
}
#[tokio::test]
async fn key_manual_two_turns_in_each_of_two_fresh_apps() {
    run_case(async {
        matrix(Mode::MANUAL).await;
    })
    .await;
}
#[tokio::test]
async fn key_semantic_vad_two_turns_in_each_of_two_fresh_apps() {
    run_case(async {
        matrix(Mode {
            bearer: false,
            semantic: true,
        })
        .await;
    })
    .await;
}
#[tokio::test]
async fn bearer_manual_two_turns_in_each_of_two_fresh_apps() {
    run_case(async {
        matrix(Mode {
            bearer: true,
            semantic: false,
        })
        .await;
    })
    .await;
}
#[tokio::test]
async fn bearer_semantic_vad_two_turns_in_each_of_two_fresh_apps() {
    run_case(async {
        matrix(Mode {
            bearer: true,
            semantic: true,
        })
        .await;
    })
    .await;
}

#[tokio::test]
async fn configured_instructions_win_seed_is_once_and_explicit_run_is_required() {
    run_case(async {
        let mut context = history();
        context.messages.push(HistoryMessage::text(
            HistoryRole::Developer,
            "later developer text",
        ));
        let original = context.messages.clone();
        let mut h = Harness::configured(
            Mode::MANUAL,
            context,
            vec![],
            Arc::new(Probe::default()),
            |c| c.session.instructions = Some("configured instructions".into()),
        )
        .await;
        assert_eq!(h.startup.len(), 2);
        assert_eq!(
            h.startup[0]["session"]["instructions"],
            "configured instructions"
        );
        let packed = h.startup[1]["item"]["content"][0]["text"].as_str().unwrap();
        assert!(!packed.contains("seed instructions"));
        assert!(
            packed.contains("later developer text")
                && packed.contains("previously saved conversation")
        );
        assert!(!packed.contains("\"role\": \"developer\""));
        assert_eq!(h.pair.context().snapshot().unwrap().messages, original);
        h.peer.quiet().await;
        h.run("greeting", true).await;
        h.peer.spoken("greeting", "audio-greeting", "ready");
        h.peer.done("greeting");
        h.completed(1).await;
        h.peer.quiet().await;
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn append_only_repeated_text_and_mid_response_debt_keep_distinct_turns() {
    run_case(async {
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
        h.send(append("same text", false)).await;
        assert_text(h.peer.command().await, "same text");
        h.barrier().await;
        h.peer.quiet().await;
        h.send(append("same text", true)).await;
        assert_text(h.peer.command().await, "same text");
        assert_create(h.peer.command().await);
        h.peer.created("first");
        h.peer.assistant("first", "first-item");
        h.send(append("next typed", true)).await;
        assert_text(h.peer.command().await, "next typed");
        h.barrier().await;
        h.peer.quiet().await;
        h.peer.spoken("first", "first-item", "first answer");
        h.peer.done("first");
        assert_create(h.peer.command().await);
        h.peer.created("second");
        h.peer.assistant("second", "second-item");
        h.peer.spoken("second", "second-item", "second answer");
        h.peer.done("second");
        h.completed(2).await;
        assert_eq!(
            texts(&h.pair, HistoryRole::User),
            ["same text", "same text", "next typed"]
        );
        h.peer.quiet().await;
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn delayed_transcript_after_tool_does_not_echo_or_swallow_next_typed_input() {
    run_case(async {

    let count = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![echo(count.clone())]).await;
    h.send_frame(pcm(vec![2, 4, 6, 8], false)).await;
    assert_eq!(h.peer.command().await["type"], "input_audio_buffer.append");
    h.send(FrameData::UserStoppedSpeaking).await;
    assert_eq!(h.peer.command().await["type"], "input_audio_buffer.commit");
    assert_create(h.peer.command().await);
    h.peer.created("audio-turn");
    h.peer.assistant("audio-turn", "audio-item");
    h.peer.tool("audio-turn", "fast-call", "echo", "{\"n\":9}");
    assert_result(h.peer.command().await, "fast-call", "{\"n\":9}");
    h.observed(|f| matches!(f, FrameData::FunctionCallResult(_)))
        .await;
    h.barrier().await;
    h.peer.event(json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"input-audio","content_index":0,"delta":"same "}));
    h.peer.transcript("input-audio", "same words");
    h.peer.transcript("input-audio", "");
    h.peer.transcript("input-audio", "same words");
    bounded(async {
        while texts(&h.pair, HistoryRole::User) != ["same words"] {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    h.barrier().await;
    h.peer.quiet().await;
    h.peer.done("audio-turn");
    assert_create(h.peer.command().await);
    h.peer.created("tool-answer");
    h.peer.assistant("tool-answer", "tool-answer-item");
    h.peer
        .spoken("tool-answer", "tool-answer-item", "tool answer");
    h.peer.done("tool-answer");
    h.completed(2).await;
    h.send(FrameData::InputText(TextFrame::new("same words")))
        .await;
    // Native typed input opens a user turn and intentionally interrupts first.
    // Manual Voice Live clears and replays admitted onset audio even when the
    // previous response has already completed; neither is a transcript echo.
    assert_eq!(h.peer.command().await["type"], "input_audio_buffer.clear");
    let replay = h.peer.command().await;
    assert_eq!(replay["type"], "input_audio_buffer.append");
    assert_eq!(STANDARD.decode(replay["audio"].as_str().unwrap()).unwrap(), [2, 4, 6, 8]);
    assert_text(h.peer.command().await, "same words");
    assert_create(h.peer.command().await);
    h.peer.created("typed-turn");
    h.peer.assistant("typed-turn", "typed-item");
    h.peer.spoken("typed-turn", "typed-item", "typed answer");
    h.peer.done("typed-turn");
    h.completed(3).await;
    assert_eq!(
        texts(&h.pair, HistoryRole::User),
        ["same words", "same words"]
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    h.peer.quiet().await;
    h.finish(0).await;

    }).await;
}

#[tokio::test]
async fn duplicate_announcements_and_arguments_run_callback_once_before_or_after_done() {
    run_case(async {
        for done_first in [false, true] {
            let (entered, release, count) = (
                Arc::new(Notify::new()),
                Arc::new(Notify::new()),
                Arc::new(AtomicUsize::new(0)),
            );
            let mut h = Harness::new(
                Mode::MANUAL,
                Context::default(),
                vec![delayed(
                    entered.clone(),
                    release.clone(),
                    count.clone(),
                    json!("{\"preencoded\":true}"),
                )],
            )
            .await;
            h.run("calls-only", false).await;
            h.peer.tool_item("calls-only", "call", "wait", true);
            h.peer.arguments("calls-only", "call", "{}");
            h.peer.arguments("calls-only", "call", "{}");
            timeout(WAIT, entered.notified()).await.unwrap_or_else(|_| {
                panic!("function-only callback did not enter; done_first={done_first}")
            });
            h.peer.quiet().await;
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(h.outcomes.snapshot().active.len(), 1);
            if done_first {
                h.peer.done("calls-only");
                h.completed(1).await;
                h.peer.quiet().await;
            }
            release.notify_one();
            assert_result(
                h.peer.command_at("function-only native final output").await,
                "call",
                "{\"preencoded\":true}",
            );
            h.observed(|f| matches!(f, FrameData::FunctionCallResult(_)))
                .await;
            if !done_first {
                h.barrier().await;
                h.peer.quiet().await;
                h.peer.done("calls-only");
            }
            assert_create(
                h.peer
                    .command_at(if done_first {
                        "function-only continuation after result following response.done"
                    } else {
                        "function-only continuation after response.done following result"
                    })
                    .await,
            );
            h.peer.created("answer");
            h.peer.assistant("answer", "answer-item");
            h.peer.spoken("answer", "answer-item", "after tool");
            h.peer.done("answer");
            h.completed(2).await;
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(h.outcomes.snapshot().total_terminal, 1);
            {
                let frames = h.probe.frames.lock().unwrap();
                assert_eq!(
                    frames
                        .iter()
                        .filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart))
                        .count(),
                    2,
                    "actual tool inference and answer each own one native LLM lifecycle"
                );
                assert_eq!(
                    frames
                        .iter()
                        .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
                        .count(),
                    2
                );
                assert_eq!(
                    frames
                        .iter()
                        .filter(|f| matches!(f.frame.data, FrameData::TtsStarted(_)))
                        .count(),
                    1,
                    "function-only response cannot invent spoken output"
                );
                assert_eq!(
                    frames
                        .iter()
                        .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
                        .count(),
                    1
                );
            }
            h.peer.quiet().await;
            h.finish(0).await;
        }
    })
    .await;
}

#[tokio::test]
async fn terminal_output_reaches_assistant_before_native_continuation_is_requested() {
    run_case(async {
        let probe = Arc::new(Probe::default());
        probe.gate_terminal.store(true, Ordering::SeqCst);
        let mut h = Harness::configured(
            Mode::MANUAL,
            Context::default(),
            vec![echo(Arc::new(AtomicUsize::new(0)))],
            probe.clone(),
            |_| {},
        )
        .await;
        h.run("tool", true).await;
        h.peer.tool("tool", "call", "echo", "{\"n\":2}");
        assert_result(h.peer.command().await, "call", "{\"n\":2}");
        bounded(probe.entered.notified()).await;
        h.peer.done("tool");
        bounded(async {
            while h.provider.responses_completed() != 1 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        h.peer.quiet().await;
        assert_eq!(
            h.outcomes.snapshot().total_terminal,
            1,
            "journal alone must not schedule inference past output"
        );
        probe.release.notify_one();
        assert_create(h.peer.command().await);
        h.peer.created("answer");
        h.peer.assistant("answer", "answer-item");
        h.peer.done("answer");
        h.completed(2).await;
        h.peer.quiet().await;
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn two_local_calls_complete_out_of_order_with_one_continuation() {
    run_case(async {
        let first = Arc::new(Notify::new());
        let second = Arc::new(Notify::new());
        let entered = Arc::new(AtomicUsize::new(0));
        let mut handler = RegisteredTool::new(Arc::new({
            let (first, second, entered) = (first.clone(), second.clone(), entered.clone());
            move |call| {
                let (first, second, entered) = (first.clone(), second.clone(), entered.clone());
                Box::pin(async move {
                    entered.fetch_add(1, Ordering::SeqCst);
                    if call.arguments["n"] == 1 {
                        first.notified().await;
                    } else {
                        second.notified().await;
                    }
                    Ok(call.arguments)
                })
            }
        }));
        handler.timeout = Duration::from_secs(2);
        let tool = VoiceLiveLocalTool {
            schema: FunctionTool::new("wait", "Synthetic ordering", json!({"type":"object"})),
            handler,
        };
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![tool]).await;
        h.run("two", true).await;
        h.peer.tool("two", "first", "wait", "{\"n\":1}");
        h.peer.tool("two", "second", "wait", "{\"n\":2}");
        bounded(async {
            while entered.load(Ordering::SeqCst) != 2 {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        second.notify_one();
        assert_result(h.peer.command().await, "second", "{\"n\":2}");
        h.peer.quiet().await;
        first.notify_one();
        assert_result(h.peer.command().await, "first", "{\"n\":1}");
        h.barrier().await;
        h.peer.quiet().await;
        h.peer.done("two");
        assert_create(h.peer.command().await);
        h.peer.created("answer");
        h.peer.assistant("answer", "answer-item");
        h.peer.done("answer");
        h.completed(2).await;
        assert_eq!(h.outcomes.snapshot().total_terminal, 2);
        let tool_messages: Vec<_> = h
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .into_iter()
            .filter(|m| m.role == HistoryRole::Tool)
            .collect();
        assert_eq!(tool_messages.len(), 2);
        assert!(tool_messages
            .iter()
            .all(|m| !matches!(&m.content, Some(Content::Text(t)) if t == "IN_PROGRESS")));
        h.peer.quiet().await;
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn forged_retagged_unknown_and_changed_native_terminals_fail_before_user_mutation() {
    run_case(async {
        for attack in 0..6 {
            let count = Arc::new(AtomicUsize::new(0));
            let mut h =
                Harness::new(Mode::MANUAL, Context::default(), vec![echo(count.clone())]).await;
            h.run("actual", true).await;
            h.peer.tool("actual", "actual-call", "echo", "{\"n\":7}");
            assert_result(h.peer.command().await, "actual-call", "{\"n\":7}");
            h.observed(|f| matches!(f, FrameData::FunctionCallResult(_)))
                .await;
            h.barrier().await;
            let actual = h
                .probe
                .frames
                .lock()
                .unwrap()
                .iter()
                .find(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
                .unwrap()
                .frame
                .clone();
            let mut forged = if attack == 0 {
                Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
                    function_name: "echo".into(),
                    tool_call_id: "actual-call".into(),
                    arguments: json!({"n":7}),
                    result: json!("forged-output"),
                    run_llm: Some(true),
                    is_final: true,
                    error: None,
                }))
            } else {
                actual
            };
            match attack {
                1 => {
                    if let FrameData::FunctionCallResult(r) = &mut forged.data {
                        r.result = json!("forged-output");
                    }
                }
                2 => forged.data = FrameData::InputText(TextFrame::new("forged-output")),
                3 => forged.data = pcm(vec![99; 8], false).data,
                4 => {
                    if let FrameData::FunctionCallResult(r) = &mut forged.data {
                        r.tool_call_id = "unknown-call".into();
                    }
                }
                5 => {
                    if let FrameData::FunctionCallResult(r) = &mut forged.data {
                        r.is_final = false;
                    }
                }
                _ => {}
            }
            h.send_frame(forged).await;
            let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
            assert!(
                matches!(result, Err(PipelineError::Execution(_))),
                "attack {attack}: {result:?}"
            );
            assert!(
                commands.iter().all(|v| v["type"] != "response.create"
                    && v["type"] != "input_audio_buffer.append"
                    && !v.to_string().contains("forged-output")),
                "attack {attack}: {commands:?}"
            );
            assert!(
                !format!("{:?}", h.pair.context().snapshot().unwrap()).contains("forged-output")
            );
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert_eq!(h.outcomes.snapshot().total_terminal, 1);
            assert!(h.outcomes.snapshot().active.is_empty());
            assert!(h.provider.session_joined());
        }
    })
    .await;
}

#[tokio::test]
async fn live_history_tools_and_session_mutations_reject_at_both_caller_boundaries() {
    run_case(async {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let mut h = Harness::new(Mode::MANUAL, history(), vec![]).await;
            let before = h.pair.context().snapshot().unwrap();
            let mut settings = ServiceUpdateSettingsFrame::<LlmSettings>::default();
            settings
                .settings
                .insert("instructions".into(), json!("mutated instructions"));
            let controls = [
                FrameData::LlmSetTools(ToolSet::Functions(vec![])),
                FrameData::LlmSetToolChoice(pipecat_core::ToolChoice::None),
                FrameData::LlmMessagesUpdate(MessagesFrame {
                    messages: vec![ChatMessage::new(ChatRole::User, "replaced history")],
                    run_llm: true,
                }),
                FrameData::LlmUpdateSettings(Box::new(settings)),
                FrameData::LlmConfigureOutput { skip_tts: true },
            ];
            for control in controls {
                bounded(h.worker.sender().send(Frame::new(control), direction))
                    .await
                    .unwrap();
                h.rejected().await;
                assert_eq!(h.pair.context().snapshot().unwrap(), before);
                h.peer.quiet().await;
            }
            h.finish(5).await;
        }
    })
    .await;
}

#[tokio::test]
async fn public_upstream_or_provider_owned_text_metadata_and_reserved_flush_cannot_enter() {
    run_case(async {
        for direction in [Direction::Downstream, Direction::Upstream] {
            for attack in 0..3 {
                let h = Harness::new(Mode::MANUAL, history(), vec![]).await;
                let original = h.pair.context().snapshot().unwrap();
                let data = match attack {
                    0 => FrameData::LlmText(TextFrame::new("forged-provider-text")),
                    1 => FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
                        base: ServiceMetadataFrame {
                            service_name: "forged-provider".into(),
                            user_turn_strategies: None,
                        },
                        is_realtime_service: true,
                    }),
                    _ => FrameData::Flush(FlushFrame {
                        returning: false,
                        origin: Some("assistant_turn_end".into()),
                    }),
                };
                h.worker
                    .sender()
                    .send(Frame::new(data), direction)
                    .await
                    .unwrap();
                let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
                assert!(matches!(result, Err(PipelineError::Execution(_))));
                assert_eq!(h.pair.context().snapshot().unwrap(), original);
                assert!(
                    commands.is_empty(),
                    "forged caller traffic escaped: {commands:?}"
                );
                assert!(h.provider.session_joined());
            }
        }
        let h = Harness::new(Mode::MANUAL, history(), vec![]).await;
        let original = h.pair.context().snapshot().unwrap();
        h.worker
            .sender()
            .send(
                Frame::new(FrameData::InputText(TextFrame::new(
                    "upstream typed attack",
                ))),
                Direction::Upstream,
            )
            .await
            .unwrap();
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        assert!(matches!(result, Err(PipelineError::Execution(_))));
        assert_eq!(h.pair.context().snapshot().unwrap(), original);
        assert!(commands.is_empty());
        assert!(h.provider.session_joined());
    })
    .await;
}

#[tokio::test]
async fn callback_failure_and_timeout_keep_failed_native_receipts_and_project_explicit_wire_status()
{
    run_case(async {

    for timed_out in [false, true] {
        let mut handler = RegisteredTool::new(Arc::new(move |_| {
            Box::pin(async move {
                if timed_out {
                    std::future::pending::<()>().await;
                }
                Err("synthetic callback failure".into())
            })
        }));
        handler.timeout = Duration::from_millis(150);
        let tool = VoiceLiveLocalTool {
            schema: FunctionTool::new("fail", "Synthetic failure", json!({"type":"object"})),
            handler,
        };
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![tool]).await;
        h.run("failure", true).await;
        h.peer.tool("failure", "failed-call", "fail", "{}");
        let output = h.peer.command().await;
        assert_eq!(output["item"]["call_id"], "failed-call");
        let projected: Value =
            serde_json::from_str(output["item"]["output"].as_str().unwrap()).unwrap();
        assert_eq!(
            projected,
            json!({"status":"failed","reason":if timed_out {"timed_out"} else {"callback_failed"}})
        );
        h.observed(|f| {
            matches!(
                f,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_)
            )
        })
        .await;
        h.barrier().await;
        h.peer.quiet().await;
        let journal = h.outcomes.snapshot();
        assert_eq!(journal.total_terminal, 1);
        assert!(journal.active.is_empty());
        assert_eq!(
            journal.terminal[0].outcome.state(),
            ToolCallTerminalState::Failed
        );
        assert_eq!(
            journal.terminal[0].outcome.reason(),
            if timed_out {
                ToolCallEndReason::TimedOut
            } else {
                ToolCallEndReason::CallbackError
            }
        );
        h.peer.done("failure");
        assert_create(h.peer.command().await);
        h.peer.created("recovered");
        h.peer.assistant("recovered", "recovered-item");
        h.peer.done("recovered");
        h.completed(2).await;
        h.peer.quiet().await;
        h.finish(0).await;
    }

    }).await;
}

#[tokio::test]
async fn explicit_callback_cancel_is_not_a_result_and_preserves_no_run_policy() {
    run_case(async {
        let (entered, release, count) = (
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut h = Harness::new(
            Mode::MANUAL,
            Context::default(),
            vec![delayed(
                entered.clone(),
                release,
                count.clone(),
                json!("never-returned"),
            )],
        )
        .await;
        h.run("cancel-call", true).await;
        h.peer.tool("cancel-call", "cancelled", "wait", "{}");
        bounded(entered.notified()).await;
        h.send(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "wait".into(),
            tool_call_id: "cancelled".into(),
            run_llm: false,
        }))
        .await;
        assert_result(
            h.peer.command().await,
            "cancelled",
            "{\"status\":\"cancelled\"}",
        );
        h.observed(|f| matches!(f, FrameData::FunctionCallCancel(_)))
            .await;
        h.barrier().await;
        h.peer.done("cancel-call");
        h.completed(1).await;
        h.peer.quiet().await;
        let journal = h.outcomes.snapshot();
        assert_eq!(journal.total_terminal, 1);
        assert_eq!(
            journal.terminal[0].outcome.state(),
            ToolCallTerminalState::Cancelled
        );
        assert_eq!(
            journal.terminal[0].outcome.reason(),
            ToolCallEndReason::ExplicitCancel
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn invalid_pcm_fails_without_wire_audio_and_constructor_rejects_unsupported_seed_authority() {
    run_case(async {
        for (rate, channels, bytes) in [
            (16000, 1, vec![0, 0]),
            (24000, 2, vec![0; 4]),
            (24000, 1, vec![0; 3]),
        ] {
            let h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
            h.send(FrameData::InputAudio(AudioRawFrame {
                audio: bytes,
                sample_rate: rate,
                num_channels: channels,
                context_id: None,
            }))
            .await;
            let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
            assert!(matches!(result, Err(PipelineError::Execution(_))));
            assert!(commands.is_empty());
            assert!(h.provider.session_joined());
        }
        let config = || Mode::MANUAL.config("ws://127.0.0.1:9/voice-live/realtime".into());
        let mut image = HistoryMessage::text(HistoryRole::User, "unsupported");
        image.content = Some(Content::Parts(vec![ContentPart::ImageUrl {
            url: "https://synthetic.invalid/image".into(),
        }]));
        let mut historical_tool = HistoryMessage::text(HistoryRole::Tool, "not native authority");
        historical_tool.tool_call_id = Some("invented-call".into());
        for message in [image, historical_tool] {
            assert!(VoiceLiveApp::new(
                config(),
                Context::new(vec![message]),
                vec![],
                Arc::new(Probe::default())
            )
            .is_err());
        }
        for kind in 0..5 {
            let mut tool = echo(Arc::new(AtomicUsize::new(0)));
            match kind {
                0 => tool.handler.timeout = Duration::ZERO,
                1 => tool.handler.timeout = Duration::from_secs(4),
                2 => tool.handler.cancel_on_interruption = false,
                3 => tool.schema.name.clear(),
                _ => tool.schema.parameters = json!("not-object"),
            }
            assert!(VoiceLiveApp::new(
                config(),
                Context::default(),
                vec![tool],
                Arc::new(Probe::default())
            )
            .is_err());
        }
    })
    .await;
}

#[tokio::test]
async fn multiple_audio_items_share_one_speech_lifecycle_and_transcript_history_once() {
    run_case(async {
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
        h.run("multi", true).await;
        h.peer.duplicate_assistant("audio-multi");
        h.peer.spoken("multi", "audio-multi", "first ");
        h.peer.assistant("multi", "second-audio");
        h.peer.spoken("multi", "second-audio", "second");
        h.peer.done("multi");
        h.completed(1).await;
        bounded(async {
            while texts(&h.pair, HistoryRole::Assistant) != ["first second"] {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        {
            let frames = h.probe.frames.lock().unwrap();
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::TtsStarted(_)))
                    .count(),
                1
            );
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
                    .count(),
                1
            );
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart))
                    .count(),
                1
            );
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
                    .count(),
                1
            );
        }
        h.peer.quiet().await;
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn end_waits_for_actual_tool_wire_after_response_done_without_new_inference() {
    run_case(async {
        let (entered, release, count) = (
            Arc::new(Notify::new()),
            Arc::new(Notify::new()),
            Arc::new(AtomicUsize::new(0)),
        );
        let mut h = Harness::new(
            Mode::MANUAL,
            Context::default(),
            vec![delayed(
                entered.clone(),
                release.clone(),
                count.clone(),
                json!("drained"),
            )],
        )
        .await;
        h.run("end-tool", true).await;
        h.peer.tool("end-tool", "end-call", "wait", "{}");
        bounded(entered.notified()).await;
        h.worker.end().await.unwrap();
        assert!(
            timeout(QUIET, h.worker.join()).await.is_err(),
            "cancelled join waiter must not abandon End owner"
        );
        h.peer.done("end-tool");
        h.peer.quiet().await;
        assert_eq!(h.outcomes.snapshot().active.len(), 1);
        release.notify_one();
        assert_result(h.peer.command().await, "end-call", "drained");
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        let (_, report) = result.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0, "{:?}", report.errors);
        assert!(commands.is_empty());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(h.outcomes.snapshot().total_terminal, 1);
        assert!(h.outcomes.snapshot().active.is_empty());
        assert!(h.provider.session_joined());
    })
    .await;
}

#[tokio::test]
async fn end_before_first_assistant_output_drains_existing_response_and_prior_input() {
    run_case(async {
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
        h.run("late-output", false).await;
        h.send_frame(pcm(vec![8, 6, 4, 2], false)).await;
        h.worker.end().await.unwrap();
        let append = h.peer.command().await;
        assert_eq!(append["type"], "input_audio_buffer.append");
        assert_eq!(
            STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
            [8, 6, 4, 2]
        );
        h.peer.quiet().await;
        h.peer.assistant("late-output", "late-item");
        h.peer.spoken("late-output", "late-item", "last answer");
        h.peer.done("late-output");
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        let (_, report) = result.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0, "{:?}", report.errors);
        assert!(commands.is_empty());
        assert_eq!(h.provider.responses_completed(), 1);
        assert!(h.provider.session_joined());
        let frames = h.probe.frames.lock().unwrap();
        assert!(frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_))));
    })
    .await;
}

#[tokio::test]
async fn cancel_supersedes_end_and_joins_delayed_callback_with_retained_sender() {
    run_case(async {
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let mut handler = RegisteredTool::new(Arc::new({
            let (entered, dropped) = (entered.clone(), dropped.clone());
            move |_| {
                let (entered, dropped) = (entered.clone(), dropped.clone());
                Box::pin(async move {
                    let _guard = DropFlag(dropped);
                    entered.notify_one();
                    std::future::pending::<Result<Value, String>>().await
                })
            }
        }));
        handler.timeout = Duration::from_secs(2);
        let tool = VoiceLiveLocalTool {
            schema: FunctionTool::new(
                "wait",
                "Synthetic cancelled callback",
                json!({"type":"object"}),
            ),
            handler,
        };
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![tool]).await;
        let sender = h.worker.sender();
        h.run("cancel-end", true).await;
        h.peer.tool("cancel-end", "wait-call", "wait", "{}");
        bounded(entered.notified()).await;
        h.worker.end().await.unwrap();
        h.peer.quiet().await;
        h.worker
            .cancel("synthetic Cancel supersedes End")
            .await
            .unwrap();
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        let (_, report) = result.unwrap();
        assert_eq!(report.state, WorkerState::Cancelled);
        assert_eq!(report.total_errors, 0, "{:?}", report.errors);
        assert!(dropped.load(Ordering::SeqCst));
        assert!(h.provider.session_joined());
        assert!(h.outcomes.snapshot().active.is_empty());
        assert_eq!(h.outcomes.snapshot().total_terminal, 1);
        assert!(commands.iter().all(|v| v["type"] != "response.create"));
        assert!(sender
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .is_err());
    })
    .await;
}

#[tokio::test]
async fn output_failure_and_cancelled_blocked_output_both_join_the_real_actor() {
    run_case(async {
        for blocked in [false, true] {
            let probe = Arc::new(Probe::default());
            if blocked {
                probe.gate_audio.store(true, Ordering::SeqCst);
            } else {
                probe.fail_audio.store(true, Ordering::SeqCst);
            }
            let mut h = Harness::configured(
                Mode::MANUAL,
                Context::default(),
                vec![],
                probe.clone(),
                |_| {},
            )
            .await;
            h.run("output", true).await;
            h.peer.audio("output", "audio-output", &[1, 2, 3, 4]);
            if blocked {
                bounded(probe.entered.notified()).await;
                h.worker
                    .cancel("synthetic blocked output cancellation")
                    .await
                    .unwrap();
            }
            let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
            if blocked {
                let (_, report) = result.unwrap();
                assert_eq!(report.state, WorkerState::Cancelled);
                assert_eq!(report.total_errors, 0, "{:?}", report.errors);
                assert!(probe.dropped.load(Ordering::SeqCst));
            } else {
                assert!(result
                    .unwrap_err()
                    .to_string()
                    .contains("synthetic output failure"));
            }
            assert!(commands.iter().all(|v| v["type"] != "response.create"));
            assert!(h.provider.session_joined());
            assert!(h.outcomes.snapshot().active.is_empty());
        }
    })
    .await;
}

#[tokio::test]
async fn dropping_app_with_retained_sender_and_provider_still_joins_socket() {
    run_case(async {
        let h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
        let sender = h.worker.sender();
        drop(h.worker);
        assert!(h.peer.closed().await.is_empty());
        bounded(async {
            while !h.provider.session_joined() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(sender
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .is_err());
        assert!(h.outcomes.snapshot().active.is_empty());
    })
    .await;
}

#[tokio::test]
async fn unexpected_peer_close_is_not_a_clean_end_and_owner_is_joined() {
    run_case(async {
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
        h.run("uncompleted", true).await;
        h.peer.events.send(PeerInput::Close).unwrap();
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        assert!(matches!(result, Err(PipelineError::Execution(_))));
        assert!(commands.is_empty());
        assert!(h.provider.session_joined());
    })
    .await;
}

#[tokio::test]
async fn interruption_uses_only_monotonic_actual_receipts_and_keeps_next_response_usable() {
    run_case(async {
        for receipt in [false, true] {
            let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
            h.send_frame(pcm(vec![5; 48], false)).await;
            assert_eq!(h.peer.command().await["type"], "input_audio_buffer.append");
            h.run("interrupted", true).await;
            h.peer.audio("interrupted", "audio-interrupted", &[7; 480]);
            h.observed(|f| matches!(f, FrameData::TtsAudio(_))).await;
            assert!(h
                .provider
                .acknowledge_playback(PlaybackPosition {
                    item_id: "unknown".into(),
                    content_index: 0,
                    audio_end_ms: 0
                })
                .is_err());
            if receipt {
                h.provider
                    .acknowledge_playback(PlaybackPosition {
                        item_id: "audio-interrupted".into(),
                        content_index: 0,
                        audio_end_ms: 5,
                    })
                    .unwrap();
                assert!(h
                    .provider
                    .acknowledge_playback(PlaybackPosition {
                        item_id: "audio-interrupted".into(),
                        content_index: 0,
                        audio_end_ms: 4
                    })
                    .is_err());
                assert!(h
                    .provider
                    .acknowledge_playback(PlaybackPosition {
                        item_id: "audio-interrupted".into(),
                        content_index: 0,
                        audio_end_ms: 11
                    })
                    .is_err());
            }
            h.send(FrameData::Interruption).await;
            assert_eq!(h.peer.command().await["type"], "input_audio_buffer.clear");
            let replay = h.peer.command().await;
            assert_eq!(replay["type"], "input_audio_buffer.append");
            assert_eq!(
                STANDARD.decode(replay["audio"].as_str().unwrap()).unwrap(),
                [5; 48]
            );
            assert_eq!(h.peer.command().await["type"], "response.cancel");
            if receipt {
                let truncate = h.peer.command().await;
                assert_eq!(truncate["type"], "conversation.item.truncate");
                assert_eq!(truncate["item_id"], "audio-interrupted");
                assert_eq!(truncate["audio_end_ms"], 5);
            }
            h.peer.quiet().await;
            h.peer.audio("interrupted", "audio-interrupted", &[99; 48]);
            h.peer.done("interrupted");
            h.barrier().await;
            h.peer.quiet().await;
            h.run("fresh", true).await;
            h.peer.spoken("fresh", "audio-fresh", "fresh answer");
            h.peer.done("fresh");
            h.completed(1).await;
            {
                let frames = h.probe.frames.lock().unwrap();
                assert!(!frames.iter().any(
                    |f| matches!(&f.frame.data, FrameData::TtsAudio(a) if a.audio.contains(&99))
                ));
            }
            h.peer.quiet().await;
            h.finish(0).await;
        }
    })
    .await;
}

#[tokio::test]
async fn durable_completed_or_failed_tool_receipt_survives_interruption_without_stale_inference() {
    run_case(async {
        for failed in [false, true] {
            let probe = Arc::new(Probe::default());
            probe.gate_terminal.store(true, Ordering::SeqCst);
            let count = Arc::new(AtomicUsize::new(0));
            let mut handler = RegisteredTool::new(Arc::new({
                let count = count.clone();
                move |_| {
                    let count = count.clone();
                    Box::pin(async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        if failed {
                            Err("synthetic durable failure".into())
                        } else {
                            Ok(json!("durable completed output"))
                        }
                    })
                }
            }));
            handler.timeout = Duration::from_secs(1);
            let tool = VoiceLiveLocalTool {
                schema: FunctionTool::new(
                    "final",
                    "Synthetic durable terminal",
                    json!({"type":"object"}),
                ),
                handler,
            };
            let mut h = Harness::configured(
                Mode::MANUAL,
                Context::default(),
                vec![tool],
                probe.clone(),
                |_| {},
            )
            .await;
            h.run("interrupted-terminal", true).await;
            h.peer
                .tool("interrupted-terminal", "durable-call", "final", "{}");
            bounded(async {
                while h.outcomes.snapshot().total_terminal != 1 {
                    tokio::time::sleep(Duration::from_millis(1)).await;
                }
            })
            .await;
            let before = h.outcomes.snapshot().terminal[0].outcome.clone();
            let expected = if failed {
                ToolCallTerminalState::Failed
            } else {
                ToolCallTerminalState::Completed
            };
            assert_eq!(before.state(), expected);
            h.send(FrameData::Interruption).await;
            // The terminal write may precede or follow epoch control admission.
            // Either way it is one durable native outcome, never a synthesized
            // cancellation, and no inference may escape the interrupted turn.
            let mut kinds = vec![];
            for _ in 0..3 {
                let command = h.peer.command().await;
                let kind = command["type"].as_str().unwrap().to_owned();
                if kind == "conversation.item.create" {
                    if failed {
                        assert_result(
                            command,
                            "durable-call",
                            "{\"reason\":\"callback_failed\",\"status\":\"failed\"}",
                        );
                    } else {
                        assert_result(command, "durable-call", "durable completed output");
                    }
                } else {
                    assert!(
                        matches!(
                            kind.as_str(),
                            "input_audio_buffer.clear" | "response.cancel"
                        ),
                        "{command}"
                    );
                }
                kinds.push(kind);
            }
            kinds.sort();
            assert_eq!(
                kinds,
                [
                    "conversation.item.create",
                    "input_audio_buffer.clear",
                    "response.cancel"
                ]
            );
            bounded(probe.entered.notified()).await;
            probe.release.notify_one();
            h.peer.done("interrupted-terminal");
            h.barrier().await;
            h.peer.quiet().await;
            let after = h.outcomes.snapshot();
            assert_eq!(after.total_terminal, 1);
            assert_eq!(after.terminal[0].outcome.id(), before.id());
            assert_eq!(after.terminal[0].outcome.state(), expected);
            assert_eq!(count.load(Ordering::SeqCst), 1);
            assert!(after.active.is_empty());
            h.finish(0).await;
        }
    })
    .await;
}

#[tokio::test]
async fn failed_provider_response_releases_real_paired_tool_debt_once_without_rewriting_success() {
    run_case(async {
        let calls = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![echo(calls.clone())]).await;
        h.run("failed-response", true).await;
        h.peer.tool("failed-response", "successful-call", "echo", "{\"n\":11}");
        assert_result(h.peer.command().await, "successful-call", "{\"n\":11}");
        h.observed(|f| matches!(f, FrameData::FunctionCallResult(_))).await;
        h.barrier().await;
        h.peer.quiet().await;
        let native = h.outcomes.snapshot().terminal[0].outcome.clone();
        assert_eq!(native.state(), ToolCallTerminalState::Completed);
        h.peer.event(json!({"type":"response.done","response":{"id":"failed-response","status":"failed","output":[],"status_details":{"type":"failed","error":{"code":"synthetic_failure","message":"synthetic provider failure"}}}}));
        h.rejected().await;
        assert_create(h.peer.command_at("one native paired continuation after failed response.done").await);
        h.peer.created("recovered-answer");
        h.peer.assistant("recovered-answer", "recovered-audio");
        h.peer.spoken("recovered-answer", "recovered-audio", "recovered answer");
        h.peer.done("recovered-answer");
        h.completed(2).await;
        h.peer.quiet().await;
        let journal = h.outcomes.snapshot();
        assert_eq!(journal.total_terminal, 1);
        assert_eq!(journal.terminal[0].outcome.id(), native.id());
        assert_eq!(journal.terminal[0].outcome.state(), ToolCallTerminalState::Completed);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(journal.active.is_empty());
        {
            let frames = h.probe.frames.lock().unwrap();
            assert_eq!(frames.iter().filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart)).count(), 2);
            assert_eq!(frames.iter().filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd)).count(), 2);
            assert_eq!(frames.iter().filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd) && f.frame.metadata.get("failed") == Some(&json!(true))).count(), 1, "failed lifecycle must not be reported as successful");
        }
        h.finish(1).await;
    }).await;
}

#[tokio::test]
async fn adjacent_semantic_burst_waits_for_native_boundaries_and_preserves_fresh_response() {
    run_case(async {
        let mode = Mode {
            bearer: false,
            semantic: true,
        };
        let (mut peer, config) = Peer::new(mode).await;
        let gate = Arc::new(SetupGate::default());
        let calls = Arc::new(AtomicUsize::new(0));
        let app = VoiceLiveApp::new(
            config,
            Context::default(),
            vec![echo(calls.clone())],
            gate.clone(),
        )
        .unwrap();
        let (running, startup) = tokio::join!(bounded(app.start()), async {
            bounded(gate.entered.notified()).await;
            let startup = peer.startup().await;
            assert_eq!(startup.len(), 1);
            assert_eq!(startup[0]["type"], "session.update");
            peer.created("old-semantic");
            peer.assistant("old-semantic", "old-semantic-item");
            peer.event(json!({"type":"input_audio_buffer.speech_started","item_id":"new-user","audio_start_ms":0}));
            // This old work follows the boundary on the wire. Nothing here asks
            // interruption to retroactively undo already delivered old output.
            peer.audio("old-semantic", "old-semantic-item", &[99, 98, 97, 96]);
            peer.tool("old-semantic", "must-not-enter", "echo", "{\"n\":99}");
            peer.event(json!({"type":"response.done","response":{"id":"old-semantic","status":"cancelled","output":[]}}));
            peer.event(json!({"type":"input_audio_buffer.speech_stopped","item_id":"new-user","audio_end_ms":10}));
            peer.created("fresh-semantic");
            peer.assistant("fresh-semantic", "fresh-semantic-item");
            peer.spoken("fresh-semantic", "fresh-semantic-item", "fresh semantic answer");
            peer.write_ping().await;
            // The actor must pause reading at each semantic boundary, so waiting
            // for its Pong before releasing native setup would deadlock a correct
            // implementation. This acknowledgement proves socket writes only.
            assert!(gate.probe.frames.lock().unwrap().is_empty());
            assert_eq!(calls.load(Ordering::SeqCst), 0);
            gate.release.notify_one();
            peer.pong().await;
            startup
        });
        let running = running.unwrap();
        RESOURCES.with(|resources| {
            resources
                .borrow_mut()
                .apps
                .push((running.provider.clone(), running.outcomes.clone()))
        });
        let mut h = Harness {
            worker: running.worker,
            pair: running.pair,
            provider: running.provider,
            outcomes: running.outcomes,
            peer,
            probe: gate.probe.clone(),
            startup,
        };
        // The old response was active before speech start, so its one native
        // interruption legitimately cancels it. No second cancellation, client
        // create, commit, clear, or tool result may target the fresh response.
        assert_eq!(
            h.peer.command_at("old semantic response cancellation").await["type"],
            "response.cancel"
        );
        h.observed(|f| matches!(f, FrameData::TtsAudio(a) if a.context_id.as_deref() == Some("fresh-semantic")))
            .await;
        h.barrier().await;
        h.peer.quiet().await;
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(h.outcomes.snapshot().total_terminal, 0);
        {
            let frames = h.probe.frames.lock().unwrap();
            let audio: Vec<u8> = frames
                .iter()
                .filter_map(|f| match &f.frame.data {
                    FrameData::TtsAudio(a) => Some(a.audio.clone()),
                    _ => None,
                })
                .flatten()
                .collect();
            assert_eq!(audio, [1, 2, 3, 4, 5, 6]);
            assert_eq!(frames.iter().filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart) && f.frame.metadata.get("response_id") == Some(&json!("fresh-semantic"))).count(), 1);
            assert!(!frames.iter().any(|f| matches!(f.frame.data, FrameData::FunctionCallsStarted(_))));
        }
        h.peer.done("fresh-semantic");
        h.completed(1).await;
        assert_eq!(texts(&h.pair, HistoryRole::Assistant), ["fresh semantic answer"]);
        assert!(h.pair.context().snapshot().unwrap().messages.iter().all(|m| m.role != HistoryRole::Tool));
        {
            let frames = h.probe.frames.lock().unwrap();
            assert_eq!(frames.iter().filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd) && f.frame.metadata.get("response_id") == Some(&json!("fresh-semantic"))).count(), 1);
        }
        h.peer.quiet().await;
        h.finish(0).await;
        assert!(gate.cleaned.load(Ordering::SeqCst));
    })
    .await;
}

#[tokio::test]
async fn delayed_input_transcript_is_preserved_across_later_unrelated_interruption() {
    run_case(async {
        let mut h = Harness::new(Mode::MANUAL, Context::default(), vec![]).await;
        h.send_frame(pcm(vec![4, 3, 2, 1], false)).await;
        assert_eq!(h.peer.command().await["type"], "input_audio_buffer.append");
        h.send(FrameData::UserStoppedSpeaking).await;
        assert_eq!(h.peer.command().await["type"], "input_audio_buffer.commit");
        assert_create(h.peer.command().await);
        h.peer.created("before-late-transcript");
        h.peer
            .assistant("before-late-transcript", "before-late-item");
        h.peer.spoken(
            "before-late-transcript",
            "before-late-item",
            "earlier answer",
        );
        h.peer.done("before-late-transcript");
        h.completed(1).await;
        h.peer.transcript("late-input-item", "delayed final input");
        // A final arriving after response end remains buffered until a native
        // handoff. This response is explicitly caller-requested, not inferred
        // from receipt of the transcript.
        h.run("flush-late-transcript", true).await;
        bounded(async {
            while texts(&h.pair, HistoryRole::User) != ["delayed final input"] {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        h.peer.done("flush-late-transcript");
        h.completed(2).await;
        h.barrier().await;
        h.peer.quiet().await;
        // This public integration case proves preservation, not deterministic
        // old-generation receiver admission: native history has consumed the
        // transcript before this unrelated explicit interruption.
        h.send(FrameData::Interruption).await;
        assert_eq!(h.peer.command().await["type"], "input_audio_buffer.clear");
        let replay = h.peer.command().await;
        assert_eq!(replay["type"], "input_audio_buffer.append");
        assert_eq!(
            STANDARD.decode(replay["audio"].as_str().unwrap()).unwrap(),
            [4, 3, 2, 1]
        );
        h.barrier().await;
        h.peer.transcript("late-input-item", "delayed final input");
        h.peer.write_ping().await;
        h.peer.pong().await;
        h.barrier().await;
        assert_eq!(texts(&h.pair, HistoryRole::User), ["delayed final input"]);
        h.peer.quiet().await;
        h.run("after-unrelated-interruption", true).await;
        h.peer.spoken(
            "after-unrelated-interruption",
            "audio-after-unrelated-interruption",
            "later answer",
        );
        h.peer.done("after-unrelated-interruption");
        h.completed(3).await;
        assert_eq!(texts(&h.pair, HistoryRole::User), ["delayed final input"]);
        assert_eq!(
            texts(&h.pair, HistoryRole::Assistant),
            ["earlier answer", "later answer"]
        );
        h.peer.quiet().await;
        h.finish(0).await;
    })
    .await;
}

#[tokio::test]
async fn dropping_start_waiter_during_semantic_admission_cancels_setup_and_joins_peer() {
    run_case(async {
        let (mut peer, config) = Peer::new(Mode {
            bearer: false,
            semantic: true,
        })
        .await;
        let gate = Arc::new(SetupGate::default());
        let app = VoiceLiveApp::new(config, Context::default(), vec![], gate.clone()).unwrap();
        let mut start = Box::pin(app.start());
        tokio::select! {
            result = &mut start => panic!("setup gate unexpectedly returned: {}", result.is_ok()),
            _ = async {
                bounded(gate.entered.notified()).await;
                let startup = peer.startup().await;
                assert_eq!(startup.len(), 1);
                assert_eq!(startup[0]["type"], "session.update");
                peer.event(json!({"type":"input_audio_buffer.speech_started","item_id":"cancelled-start-input","audio_start_ms":0}));
                peer.write_ping().await;
            } => {}
        }
        // No worker has been returned. This cancels the actual startup owner,
        // not a fabricated worker Cancel or a manually completed private marker.
        drop(start);
        assert!(peer.closed().await.is_empty());
        bounded(async {
            while !gate.dropped.load(Ordering::SeqCst) || !gate.cleaned.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
        assert!(gate.probe.frames.lock().unwrap().is_empty());
    })
    .await;
}
