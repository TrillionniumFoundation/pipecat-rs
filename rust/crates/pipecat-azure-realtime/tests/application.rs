//! Real public Azure application construction with a synthetic loopback GA peer.
//! These tests do not create a second, test-only pipeline topology or use live keys.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_azure_realtime::*;
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolOutcomeJournal};
use pipecat_services::{Content, Context, Message as ContextMessage, Role as ContextRole};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::mpsc, sync::Notify, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::{handshake::server::Request, Message};

const WAIT: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(70);
const DEPLOYMENT: &str = "synthetic-deployment";
const KEY: &str = "synthetic-azure-api-key-not-a-credential";
const BEARER: &str = "synthetic-entra-token-not-a-credential";

async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(WAIT, future)
        .await
        .expect("local application deadline")
}

#[derive(Clone, Copy)]
struct Mode {
    bearer: bool,
    server_vad: bool,
}
impl Mode {
    const MANUAL_KEY: Self = Self {
        bearer: false,
        server_vad: false,
    };
    fn config(self, endpoint: String) -> RealtimeConfig {
        let credential = if self.bearer {
            AzureRealtimeCredential::EntraBearer(BEARER.into())
        } else {
            AzureRealtimeCredential::ApiKey(KEY.into())
        };
        let mut config = RealtimeConfig::azure(endpoint, DEPLOYMENT, credential).unwrap();
        config.limits.connect_timeout = Duration::from_secs(2);
        config.limits.operation_timeout = Duration::from_secs(1);
        config.limits.response_timeout = Duration::from_secs(3);
        config.limits.close_timeout = Duration::from_millis(200);
        if self.server_vad {
            config
                .session
                .audio
                .as_mut()
                .unwrap()
                .input
                .as_mut()
                .unwrap()
                .turn_detection = Some(TurnDetection::ServerVad {
                threshold: 0.5,
                prefix_padding_ms: 300,
                silence_duration_ms: 500,
                create_response: true,
                interrupt_response: true,
            });
        }
        config
    }
}

enum PeerInput {
    Event(Value),
    Close,
    Drop,
}

struct Peer {
    events: mpsc::UnboundedSender<PeerInput>,
    commands: mpsc::UnboundedReceiver<Value>,
    task: JoinHandle<()>,
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite defines the callback's error type.
    async fn new(mode: Mode, ready: bool) -> (Self, RealtimeConfig) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut endpoint = format!("ws://{}/openai/v1/realtime", listener.local_addr().unwrap());
        // Both accepted endpoint forms enter the actual application constructor.
        if mode.bearer {
            endpoint.push_str("?model=synthetic-deployment");
        }
        let config = mode.config(endpoint);
        let (events, mut event_rx) = mpsc::unbounded_channel::<PeerInput>();
        let (command_tx, commands) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (tcp, _) = bounded(listener.accept()).await.unwrap();
            let mut ws =
                tokio_tungstenite::accept_hdr_async(tcp, move |request: &Request, response| {
                    assert_eq!(request.uri().path(), "/openai/v1/realtime");
                    assert_eq!(request.uri().query(), Some("model=synthetic-deployment"));
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
                })
                .await
                .unwrap();
            let mut serial = 0_u64;
            let mut closing = false;
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        let mut event = match event {
                            Some(PeerInput::Event(event)) => event,
                            Some(PeerInput::Close) => {
                                ws.close(None).await.unwrap();
                                closing = true;
                                continue;
                            }
                            Some(PeerInput::Drop) | None => break,
                        };
                        serial += 1;
                        event["event_id"] = json!(format!("azure-app-event-{serial}"));
                        if ws.send(Message::Text(event.to_string().into())).await.is_err() { break; }
                    }
                    message = ws.next() => match message {
                        Some(Ok(Message::Text(text))) => {
                            let value: Value = serde_json::from_str(&text).unwrap();
                            let update = value["type"] == "session.update";
                            command_tx.send(value).unwrap();
                            if update && ready {
                                serial += 1;
                                // A returned canonical provider model must never change
                                // the connection's caller-selected deployment.
                                let event = json!({"type":"session.updated","event_id":format!("ready-{serial}"),"session":{"id":"synthetic-session","type":"realtime","model":"provider-canonical-alias"}});
                                if ws.send(Message::Text(event.to_string().into())).await.is_err() { break; }
                            }
                        }
                        Some(Ok(Message::Close(_))) | None => { let _ = ws.close(None).await; break; }
                        Some(Err(tokio_tungstenite::tungstenite::Error::ConnectionClosed | tokio_tungstenite::tungstenite::Error::AlreadyClosed)) if closing => break,
                        // Fault-injected peer Close may make the failed actor
                        // drop transport without replying. Never allow this
                        // protocol error on an ordinary successful lifecycle.
                        Some(Err(tokio_tungstenite::tungstenite::Error::Protocol(
                            tokio_tungstenite::tungstenite::error::ProtocolError::ResetWithoutClosingHandshake,
                        ))) if closing => break,
                        Some(Err(error)) => panic!("unexpected loopback socket error: {error}"),
                        _ => {}
                    }
                }
            }
        });
        (
            Self {
                events,
                commands,
                task,
            },
            config,
        )
    }
    fn event(&self, event: Value) {
        self.events.send(PeerInput::Event(event)).unwrap();
    }
    async fn command(&mut self) -> Value {
        bounded(self.commands.recv())
            .await
            .expect("peer closed before command")
    }
    async fn quiet(&mut self) {
        assert!(
            timeout(QUIET, self.commands.recv()).await.is_err(),
            "unexpected command or closure"
        );
    }
    async fn startup(&mut self) -> Vec<Value> {
        let mut commands = vec![self.command().await];
        while let Ok(value) = timeout(QUIET, self.commands.recv()).await {
            commands.push(value.expect("peer closed during startup"));
        }
        commands
    }
    fn response_start(&self, id: &str) {
        self.event(json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}));
    }
    fn response_end(&self, id: &str) {
        self.event(
            json!({"type":"response.done","response":{"id":id,"status":"completed","output":[]}}),
        );
    }
    fn tool(&self, response: &str, id: &str, name: &str, arguments: &str) {
        self.event(json!({"type":"response.output_item.added","response_id":response,"output_index":0,"item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":name,"arguments":""}}));
        self.arguments_done(response, id, arguments);
    }
    fn arguments_done(&self, response: &str, id: &str, arguments: &str) {
        self.event(json!({"type":"response.function_call_arguments.done","response_id":response,"item_id":format!("item-{id}"),"output_index":0,"call_id":id,"arguments":arguments}));
    }
    fn transcript(&self, id: &str, text: &str) {
        self.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":id,"content_index":0,"transcript":text}));
    }
    fn audio(&self, response: &str, item: &str, bytes: &[u8]) {
        self.event(json!({"type":"response.output_audio.delta","response_id":response,"item_id":item,"output_index":0,"content_index":0,"delta":STANDARD.encode(bytes)}));
    }
    fn spoken(&self, response: &str, text: &str) {
        // An odd delta boundary is legal: the mapper must preserve the split sample.
        self.audio(response, "audio", &[1, 2, 3]);
        self.audio(response, "audio", &[4, 5, 6]);
        self.event(json!({"type":"response.output_audio_transcript.delta","response_id":response,"item_id":"audio","output_index":0,"content_index":0,"delta":text}));
        self.event(json!({"type":"response.output_audio.done","response_id":response,"item_id":"audio","output_index":0,"content_index":0}));
        self.response_end(response);
    }
    async fn closed(mut self) -> Vec<Value> {
        bounded(self.task).await.unwrap();
        let mut remaining = vec![];
        while let Ok(command) = self.commands.try_recv() {
            remaining.push(command);
        }
        remaining
    }
}

#[derive(Default)]
struct Probe(Mutex<Vec<DirectedFrame>>);
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "AzureApplicationOutputProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
struct Harness {
    worker: PipelineWorker,
    pair: LLMContextAggregatorPair,
    provider: AzureRealtimeHandle,
    outcomes: ToolOutcomeJournal,
    peer: Peer,
    probe: Arc<Probe>,
    cursor: usize,
    startup: Vec<Value>,
}
impl Harness {
    async fn new(mode: Mode, context: Context, tools: Vec<AzureLocalTool>) -> Self {
        let (peer, config) = Peer::new(mode, true).await;
        let probe = Arc::new(Probe::default());
        let app = AzureRealtimeApp::new(config, context, tools, probe.clone()).unwrap();
        let running = bounded(app.start()).await.unwrap();
        let mut h = Self {
            worker: running.worker,
            pair: running.pair,
            provider: running.provider,
            outcomes: running.outcomes,
            peer,
            probe,
            cursor: 0,
            startup: vec![],
        };
        h.startup = h.peer.startup().await;
        let first = &h.startup[0];
        assert_eq!(first["type"], "session.update");
        assert_eq!(first["session"]["type"], "realtime");
        assert_eq!(
            first["session"]["audio"]["input"]["format"],
            json!({"type":"audio/pcm","rate":24000})
        );
        assert_eq!(
            first["session"]["audio"]["output"]["format"],
            json!({"type":"audio/pcm","rate":24000})
        );
        assert_eq!(first["session"]["output_modalities"], json!(["audio"]));
        assert_eq!(
            first["session"]["audio"]["input"]["turn_detection"].is_null(),
            !mode.server_vad
        );
        if mode.server_vad {
            assert_eq!(
                first["session"]["audio"]["input"]["turn_detection"],
                json!({"type":"server_vad","threshold":0.5,"prefix_padding_ms":300,"silence_duration_ms":500,"create_response":true,"interrupt_response":true})
            );
        }
        assert!(first["session"].get("modalities").is_none());
        assert!(first["session"].get("turn_detection").is_none());
        assert!(h.startup.iter().all(|value| matches!(
            value["type"].as_str(),
            Some("session.update" | "conversation.item.create")
        )));
        h.observe(|data| matches!(data, FrameData::LlmServiceMetadata(_)))
            .await;
        h.barrier().await;
        assert!(h.pair.user().realtime_service_enabled().unwrap());
        assert!(h.pair.assistant().realtime_service_enabled().unwrap());
        assert!(h.probe.0.lock().unwrap().iter().any(|f| matches!(&f.frame.data, FrameData::LlmServiceMetadata(metadata) if metadata.base.service_name == "AzureOpenAiRealtimeService")));
        h
    }
    async fn send(&self, data: FrameData) {
        self.send_frame(Frame::new(data)).await;
    }
    async fn send_frame(&self, frame: Frame) {
        self.worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    async fn observe(&mut self, wanted: impl Fn(&FrameData) -> bool) {
        bounded(async {
            loop {
                {
                    let frames = self.probe.0.lock().unwrap();
                    while self.cursor < frames.len() {
                        let frame = &frames[self.cursor];
                        self.cursor += 1;
                        if let FrameData::Error(error) = &frame.frame.data {
                            panic!("unexpected output error: {error:?}");
                        }
                        if wanted(&frame.frame.data) {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;
    }
    async fn barrier(&mut self) {
        self.send(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("azure-test-barrier".into()),
        }))
        .await;
        bounded(async {
            loop {
                let frame = self.worker.recv().await.expect("pipeline closed before barrier");
                if let FrameData::Error(error) = &frame.frame.data { panic!("unexpected pipeline error: {error:?}"); }
                if matches!(&frame.frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("azure-test-barrier")) { return; }
            }
        }).await;
    }
    async fn rejected(&mut self) {
        bounded(async {
            loop {
                let frame = self
                    .worker
                    .recv()
                    .await
                    .expect("pipeline closed before rejection");
                if let FrameData::Error(error) = frame.frame.data {
                    assert!(
                        !error.fatal,
                        "expected recoverable control/input rejection: {error:?}"
                    );
                    return;
                }
            }
        })
        .await;
    }
    async fn finish(self, expected_errors: u64) {
        self.worker.end().await.unwrap();
        let (result, remaining) = tokio::join!(bounded(self.worker.collect()), self.peer.closed());
        let (_, report) = result.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(
            report.errors.len() as u64,
            expected_errors,
            "{:?}",
            report.errors
        );
        assert_eq!(report.total_errors, expected_errors);
        assert_eq!(report.evicted_errors, 0);
        assert_eq!(report.dropped_frames, 0);
        assert!(
            remaining.is_empty(),
            "unexpected terminal commands: {remaining:?}"
        );
        assert!(self.outcomes.snapshot().active.is_empty());
        assert!(self.provider.session_joined());
    }
}
fn history() -> Context {
    Context::new(vec![
        ContextMessage::text(ContextRole::System, "synthetic instructions"),
        ContextMessage::text(ContextRole::User, "old question"),
        ContextMessage::text(ContextRole::Assistant, "old answer"),
    ])
}
fn texts(h: &Harness, role: ContextRole) -> Vec<String> {
    h.pair
        .context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter(|message| message.role == role)
        .filter_map(|message| match message.content {
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
        context_id: Some("capture".into()),
    };
    let mut frame = Frame::new(if user {
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "synthetic-speaker".into(),
        })
    } else {
        FrameData::InputAudio(audio)
    });
    frame.metadata.insert("capture_sequence".into(), json!(7));
    frame.transport_source = Some("synthetic-recorder".into());
    frame
}
fn echo(counter: Arc<AtomicUsize>) -> AzureLocalTool {
    let mut handler = RegisteredTool::new(Arc::new(move |call| {
        let counter = counter.clone();
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(call.arguments)
        })
    }));
    handler.timeout = Duration::from_secs(1);
    AzureLocalTool {
        schema: FunctionTool::new(
            "echo",
            "Synthetic echo",
            json!({"type":"object","properties":{"n":{"type":"number"}},"required":["n"]}),
        ),
        handler,
    }
}
async fn matrix(mode: Mode) {
    let total = Arc::new(AtomicUsize::new(0));
    let mut previous_native_id = None;
    for fresh in 0..2 {
        let mut h = Harness::new(mode, history(), vec![echo(total.clone())]).await;
        assert_eq!(
            h.outcomes.snapshot().total_terminal,
            0,
            "a new app must own a fresh tool journal"
        );
        assert_eq!(texts(&h, ContextRole::User), ["old question"]);
        let items: Vec<_> = h
            .startup
            .iter()
            .filter(|value| value["type"] == "conversation.item.create")
            .collect();
        assert_eq!(items.len(), 1, "initial history must be seeded only once");
        assert_eq!(items[0]["item"]["role"], "user");
        let packed = items[0]["item"]["content"][0]["text"].as_str().unwrap();
        assert!(packed.contains("old question") && packed.contains("old answer"));
        assert!(!packed.contains("synthetic instructions"));
        assert_eq!(
            h.startup
                .iter()
                .filter(|v| v["type"] == "session.update")
                .count(),
            2
        );
        assert!(h
            .startup
            .iter()
            .any(|v| v["session"]["instructions"] == "synthetic instructions"));
        for turn in 0..2 {
            for user in [false, true] {
                let bytes = vec![1 + turn as u8, 2, 3, 4];
                let input = pcm(bytes.clone(), user);
                let expected = input.clone();
                h.send_frame(input).await;
                let append = h.peer.command().await;
                assert_eq!(append["type"], "input_audio_buffer.append");
                assert_eq!(
                    STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
                    bytes
                );
                assert!(append.get("user_id").is_none());
                h.observe(|f| {
                    if user {
                        matches!(f, FrameData::UserAudio(_))
                    } else {
                        matches!(f, FrameData::InputAudio(_))
                    }
                })
                .await;
                assert!(h
                    .probe
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|f| f.frame.id == expected.id
                        && f.frame.data == expected.data
                        && f.frame.metadata == expected.metadata
                        && f.frame.transport_source == expected.transport_source));
            }
            let user = format!("user-{turn}");
            if mode.server_vad {
                h.peer.event(json!({"type":"input_audio_buffer.speech_started","item_id":user,"audio_start_ms":0}));
                h.observe(|f| matches!(f, FrameData::UserStartedSpeaking))
                    .await;
                h.barrier().await;
                h.peer.event(json!({"type":"input_audio_buffer.speech_stopped","item_id":user,"audio_end_ms":100}));
                h.observe(|f| matches!(f, FrameData::UserStoppedSpeaking))
                    .await;
                h.barrier().await;
                h.peer.quiet().await;
            } else {
                h.send(FrameData::UserStoppedSpeaking).await;
                assert_eq!(h.peer.command().await["type"], "input_audio_buffer.commit");
                assert_eq!(h.peer.command().await["type"], "response.create");
                // Late server VAD notifications cannot drive a manual second turn.
                h.peer.event(json!({"type":"input_audio_buffer.speech_started","item_id":"irrelevant-vad","audio_start_ms":0}));
                h.peer.event(json!({"type":"input_audio_buffer.speech_stopped","item_id":"irrelevant-vad","audio_end_ms":100}));
            }
            let response = format!("response-{turn}");
            h.peer.response_start(&response);
            h.observe(|f| matches!(f, FrameData::LlmResponseStart))
                .await;
            let question = format!("question {fresh}-{turn}");
            // One actual completion must enter history once, not once through
            // notification and again through aggregation. Provider replay
            // deduplication is outside this inherited mapper's contract.
            h.peer.transcript(&user, &question);
            if turn == 0 {
                h.peer.spoken(&response, &format!("answer {fresh}-{turn}"));
                h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
            } else {
                h.peer.tool(
                    &response,
                    "same-call-id-in-fresh-session",
                    "echo",
                    "{\"n\":7}",
                );
                h.peer
                    .arguments_done(&response, "same-call-id-in-fresh-session", "{\"n\":7}");
                let result = h.peer.command().await;
                assert_eq!(result["type"], "conversation.item.create");
                assert_eq!(result["item"]["type"], "function_call_output");
                assert_eq!(result["item"]["call_id"], "same-call-id-in-fresh-session");
                assert_eq!(result["item"]["output"], "{\"n\":7}");
                h.observe(|f| matches!(f, FrameData::FunctionCallResult(_)))
                    .await;
                h.barrier().await;
                h.peer.quiet().await;
                let terminal = h
                    .probe
                    .0
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
                    .unwrap()
                    .frame
                    .clone();
                let receipt = terminal.native_tool_outcome().unwrap().unwrap();
                assert_eq!(receipt.state(), ToolCallTerminalState::Completed);
                assert_ne!(
                    previous_native_id,
                    Some(receipt.id()),
                    "fresh session cannot reuse a native outcome capability"
                );
                previous_native_id = Some(receipt.id());
                h.peer.response_end(&response);
                let mut continuation = h.peer.command().await;
                if continuation["type"] == "session.update" {
                    // The inherited context mapper re-emits the unchanged system
                    // instruction once when its first caused context omits the
                    // already-installed tool fields. This is not a tool edit or
                    // extra inference; every field is checked, and repetition
                    // cannot be hidden by an unbounded skip loop.
                    assert_eq!(
                        continuation["session"],
                        json!({
                            "type":"realtime", "instructions":"synthetic instructions"
                        })
                    );
                    continuation = h.peer.command().await;
                }
                assert_eq!(continuation["type"], "response.create");
                h.peer.response_start("continuation");
                h.peer
                    .spoken("continuation", &format!("answer {fresh}-{turn}"));
                h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
                h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
            }
            h.barrier().await;
            h.peer.quiet().await;
        }
        assert_eq!(
            texts(&h, ContextRole::User),
            [
                "old question".into(),
                format!("question {fresh}-0"),
                format!("question {fresh}-1")
            ]
        );
        let assistant = texts(&h, ContextRole::Assistant);
        assert_eq!(
            assistant
                .iter()
                .filter(|text| text.as_str() == "old answer")
                .count(),
            1
        );
        for turn in 0..2 {
            assert_eq!(
                assistant
                    .iter()
                    .filter(|text| *text == &format!("answer {fresh}-{turn}"))
                    .count(),
                1
            );
        }
        {
            let frames = h.probe.0.lock().unwrap();
            let audio: Vec<u8> = frames
                .iter()
                .filter_map(|f| match &f.frame.data {
                    FrameData::TtsAudio(audio) => {
                        assert_eq!((audio.sample_rate, audio.num_channels), (24000, 1));
                        Some(audio.audio.as_slice())
                    }
                    _ => None,
                })
                .flatten()
                .copied()
                .collect();
            assert_eq!(audio, [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6]);
            assert_eq!(frames.iter().filter(|f| matches!(&f.frame.data, FrameData::LlmText(text) if !text.append_to_context)).count(), 2);
        }
        let journal = h.outcomes.snapshot();
        assert_eq!(journal.total_terminal, 1);
        assert!(journal.active.is_empty());
        assert_eq!(journal.unadmitted_terminal, 0);
        assert_eq!(total.load(Ordering::SeqCst), fresh + 1);
        h.finish(0).await;
    }
}
#[tokio::test]
async fn api_key_manual_two_turns_and_fresh_session_isolation() {
    matrix(Mode::MANUAL_KEY).await;
}
#[tokio::test]
async fn api_key_server_vad_two_turns_and_fresh_session_isolation() {
    matrix(Mode {
        bearer: false,
        server_vad: true,
    })
    .await;
}
#[tokio::test]
async fn supplied_bearer_manual_two_turns_and_fresh_session_isolation() {
    matrix(Mode {
        bearer: true,
        server_vad: false,
    })
    .await;
}
#[tokio::test]
async fn supplied_bearer_server_vad_two_turns_and_fresh_session_isolation() {
    matrix(Mode {
        bearer: true,
        server_vad: true,
    })
    .await;
}

#[tokio::test]
async fn forged_retagged_unknown_and_mismatched_native_results_fail_before_user_mutation() {
    for attack in 0..6 {
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::new(
            Mode::MANUAL_KEY,
            Context::default(),
            vec![echo(count.clone())],
        )
        .await;
        h.peer.response_start("actual-call");
        h.peer
            .tool("actual-call", "native-call", "echo", "{\"n\":7}");
        assert_eq!(h.peer.command().await["item"]["output"], "{\"n\":7}");
        h.observe(|f| matches!(f, FrameData::FunctionCallResult(_)))
            .await;
        h.barrier().await;
        let actual = h
            .probe
            .0
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
                tool_call_id: "native-call".into(),
                arguments: json!({"n":7}),
                result: json!("forged-result"),
                run_llm: Some(true),
                is_final: true,
                error: None,
            }))
        } else {
            actual
        };
        match attack {
            1 => {
                if let FrameData::FunctionCallResult(result) = &mut forged.data {
                    result.result = json!("forged-result");
                }
            }
            2 => forged.data = FrameData::InputText(TextFrame::new("forged-result")),
            3 => forged.data = pcm(vec![17; 8], false).data,
            4 => {
                if let FrameData::FunctionCallResult(result) = &mut forged.data {
                    result.tool_call_id = "unknown-call".into();
                }
            }
            5 => {
                if let FrameData::FunctionCallResult(result) = &mut forged.data {
                    result.function_name = "mismatched-name".into();
                }
            }
            _ => {}
        }
        h.send_frame(forged).await;
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        assert!(
            matches!(result, Err(PipelineError::Execution(_))),
            "invalid native ingress must fail closed for attack {attack}: {result:?}"
        );
        assert!(
            commands.iter().all(|v| v["type"] != "response.create"
                && v["type"] != "input_audio_buffer.append"
                && !v.to_string().contains("forged-result")),
            "attack {attack} crossed ingress: {commands:?}"
        );
        assert!(!format!("{:?}", h.pair.context().snapshot().unwrap()).contains("forged-result"));
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(h.outcomes.snapshot().total_terminal, 1);
        assert!(h.outcomes.snapshot().active.is_empty());
        assert!(h.provider.session_joined());
    }
}

#[tokio::test]
async fn live_tool_history_and_session_controls_reject_before_pair_or_wire_mutation() {
    let mut h = Harness::new(
        Mode::MANUAL_KEY,
        history(),
        vec![echo(Arc::new(AtomicUsize::new(0)))],
    )
    .await;
    let original = h.pair.context().snapshot().unwrap();
    let mut settings = ServiceUpdateSettingsFrame::<LlmSettings>::default();
    settings.settings.insert(
        "session_properties".into(),
        json!({"type":"realtime","tools":[]}),
    );
    let mut forged_tool = ChatMessage::new(ChatRole::Tool, "untracked-output");
    forged_tool.tool_call_id = Some("untracked-call".into());
    let controls = vec![
        FrameData::LlmSetTools(ToolSet::Functions(vec![])),
        FrameData::LlmSetToolChoice(pipecat_core::ToolChoice::Auto),
        FrameData::LlmMessagesUpdate(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "replacement")],
            run_llm: true,
        }),
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![forged_tool],
            run_llm: true,
        }),
        FrameData::LlmUpdateSettings(Box::new(settings)),
        FrameData::LlmConfigureOutput { skip_tts: true },
    ];
    let errors = controls.len() as u64;
    for control in controls {
        h.send(control).await;
        h.rejected().await;
        assert_eq!(h.pair.context().snapshot().unwrap(), original);
        h.peer.quiet().await;
    }
    h.finish(errors).await;
}

#[tokio::test]
async fn mutating_exposed_context_cannot_replace_the_offered_callback_bindings() {
    for change_choice in [false, true] {
        let h = Harness::new(
            Mode::MANUAL_KEY,
            history(),
            vec![echo(Arc::new(AtomicUsize::new(0)))],
        )
        .await;
        let mut context = h.pair.context().snapshot().unwrap();
        if change_choice {
            context.tool_choice = Some(pipecat_services::ToolChoice::None);
        } else {
            context.tools = Some(vec![pipecat_services::ToolDefinition {
                name: "unregistered".into(),
                description: String::new(),
                parameters: json!({"type":"object"}),
            }]);
        }
        h.pair.context().replace(context).unwrap();
        h.send(FrameData::InputText(TextFrame::new(
            "must-not-reach-provider",
        )))
        .await;
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        assert!(matches!(result, Err(PipelineError::Execution(_))));
        assert!(
            commands.iter().all(|v| v["type"] != "session.update"
                && v["type"] != "conversation.item.create"
                && v["type"] != "response.create"),
            "mutated shared bindings reached the provider: {commands:?}"
        );
        assert!(h.outcomes.snapshot().active.is_empty());
    }
}

#[tokio::test]
async fn callback_error_and_timeout_keep_real_terminal_receipts_and_one_continuation() {
    for timed_out in [false, true] {
        let mut handler = RegisteredTool::new(Arc::new(move |_| {
            Box::pin(async move {
                if timed_out {
                    std::future::pending::<()>().await;
                }
                Err("synthetic callback failure".into())
            })
        }));
        handler.timeout = Duration::from_millis(120);
        let tool = AzureLocalTool {
            schema: FunctionTool::new("fail", "Synthetic failure", json!({"type":"object"})),
            handler,
        };
        let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![tool]).await;
        h.peer.response_start("failing-tool");
        h.peer.tool("failing-tool", "failure-call", "fail", "{}");
        let output = h.peer.command().await;
        assert_eq!(output["item"]["call_id"], "failure-call");
        assert_eq!(
            output["item"]["output"],
            if timed_out {
                "CANCELLED"
            } else {
                "{\"error\":\"synthetic callback failure\"}"
            }
        );
        h.observe(|f| {
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
        assert!(h
            .probe
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|f| matches!(
                f.frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_)
            ))
            .all(|f| f.frame.native_tool_outcome().unwrap().is_some()));
        h.peer.response_end("failing-tool");
        assert_eq!(h.peer.command().await["type"], "response.create");
        h.peer.response_start("after-failure");
        h.peer.response_end("after-failure");
        h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
        h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
        h.barrier().await;
        h.peer.quiet().await;
        h.finish(0).await;
    }
}

#[tokio::test]
async fn invalid_pcm_rejects_without_wire_effect_and_valid_input_still_works() {
    let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![]).await;
    for (rate, channels, bytes) in [
        (16000, 1, vec![0, 0]),
        (24000, 2, vec![0; 4]),
        (24000, 1, vec![0; 3]),
    ] {
        h.send(FrameData::InputAudio(AudioRawFrame {
            audio: bytes,
            sample_rate: rate,
            num_channels: channels,
            context_id: None,
        }))
        .await;
        h.rejected().await;
        h.peer.quiet().await;
    }
    h.send_frame(pcm(vec![9, 8], false)).await;
    assert_eq!(h.peer.command().await["type"], "input_audio_buffer.append");
    h.barrier().await;
    h.finish(3).await;
}

#[test]
fn constructor_rejects_nonazure_rich_history_schema_and_timeout_mismatches() {
    let config = || Mode::MANUAL_KEY.config("ws://127.0.0.1:9/openai/v1/realtime".into());
    let build = |config, context, tools| {
        AzureRealtimeApp::new(config, context, tools, Arc::new(Probe::default()))
    };
    assert!(build(
        RealtimeConfig::new("synthetic-openai"),
        Context::default(),
        vec![]
    )
    .is_err());
    let mut rich = ContextMessage::text(ContextRole::User, "not plain text");
    rich.content = Some(Content::Parts(vec![
        pipecat_services::ContentPart::ImageUrl {
            url: "https://example.invalid/synthetic.png".into(),
        },
    ]));
    assert!(build(config(), Context::new(vec![rich]), vec![]).is_err());
    let tool = || echo(Arc::new(AtomicUsize::new(0)));
    for kind in 0..5 {
        let mut invalid = tool();
        match kind {
            0 => invalid.handler.timeout = Duration::ZERO,
            1 => invalid.handler.timeout = Duration::from_secs(4),
            2 => invalid.handler.cancel_on_interruption = false,
            3 => invalid.schema.name = String::new(),
            _ => invalid.schema.parameters = json!("not-an-object-schema"),
        }
        assert!(
            build(config(), Context::default(), vec![invalid]).is_err(),
            "constructor accepted invalid local tool variant {kind}"
        );
    }
    assert!(build(config(), Context::default(), vec![tool(), tool()]).is_err());
    let mut excluded = config();
    excluded.session.tools = Some(vec![]);
    assert!(build(excluded, Context::default(), vec![tool()]).is_err());
    let mut missing = config();
    missing.session.tools = Some(vec![FunctionTool::new(
        "missing",
        "No callback",
        json!({"type":"object"}),
    )]);
    assert!(build(missing, Context::default(), vec![]).is_err());
    let mut mismatch = config();
    let mut wrong_schema = tool().schema;
    wrong_schema.description = "different callback contract".into();
    mismatch.session.tools = Some(vec![wrong_schema]);
    assert!(build(mismatch, Context::default(), vec![tool()]).is_err());
}

#[tokio::test]
async fn upstream_controls_cannot_mutate_assistant_history_or_offered_tools() {
    let mut h = Harness::new(Mode::MANUAL_KEY, history(), vec![]).await;
    let original = h.pair.context().snapshot().unwrap();
    for control in [
        FrameData::LlmSetTools(ToolSet::Functions(vec![])),
        FrameData::LlmMessagesUpdate(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "upstream-replacement")],
            run_llm: false,
        }),
    ] {
        h.worker
            .sender()
            .send(Frame::new(control), Direction::Upstream)
            .await
            .unwrap();
        h.rejected().await;
        assert_eq!(h.pair.context().snapshot().unwrap(), original);
        h.peer.quiet().await;
    }
    h.finish(2).await;
}

#[tokio::test]
async fn forged_provider_text_and_metadata_cannot_enter_either_public_pipeline_endpoint() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        for metadata in [false, true] {
            let h = Harness::new(Mode::MANUAL_KEY, history(), vec![]).await;
            let original = h.pair.context().snapshot().unwrap();
            let data = if metadata {
                FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
                    base: ServiceMetadataFrame {
                        service_name: "forged-provider".into(),
                        user_turn_strategies: None,
                    },
                    is_realtime_service: true,
                })
            } else {
                FrameData::LlmText(TextFrame::new("forged-provider-text"))
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
                commands
                    .iter()
                    .all(|v| v["type"] != "conversation.item.create"
                        && v["type"] != "response.create")
            );
        }
    }
}

struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
fn waiting_tool(entered: Arc<Notify>, dropped: Arc<AtomicBool>) -> AzureLocalTool {
    let mut handler = RegisteredTool::new(Arc::new(move |_| {
        let entered = entered.clone();
        let dropped = dropped.clone();
        Box::pin(async move {
            let _guard = DropFlag(dropped);
            entered.notify_one();
            std::future::pending::<Result<Value, String>>().await
        })
    }));
    handler.timeout = Duration::from_secs(2);
    AzureLocalTool {
        schema: FunctionTool::new(
            "wait",
            "Synthetic pending callback",
            json!({"type":"object"}),
        ),
        handler,
    }
}

#[tokio::test]
async fn cancellation_joins_active_response_callback_and_socket_with_retained_sender() {
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let h = Harness::new(
        Mode::MANUAL_KEY,
        Context::default(),
        vec![waiting_tool(entered.clone(), dropped.clone())],
    )
    .await;
    let pipeline_sender = h.worker.sender();
    h.peer.response_start("active-response");
    h.peer.tool("active-response", "active-call", "wait", "{}");
    bounded(entered.notified()).await;
    assert_eq!(h.outcomes.snapshot().active.len(), 1);
    h.worker
        .cancel("synthetic active cancellation")
        .await
        .unwrap();
    let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.total_errors, 0);
    assert!(
        dropped.load(Ordering::SeqCst),
        "callback future was not joined/dropped"
    );
    assert_eq!(h.outcomes.snapshot().total_terminal, 1);
    assert!(h.outcomes.snapshot().active.is_empty());
    assert!(h.provider.session_joined());
    assert!(pipeline_sender
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .is_err());
    assert!(
        commands.iter().all(|v| v["type"] != "response.create"),
        "Cancel must not revive continuation: {commands:?}"
    );
}

#[tokio::test]
async fn dropped_running_app_closes_provider_despite_retained_pipeline_sender_and_handle() {
    let h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![]).await;
    let sender = h.worker.sender();
    drop(h.worker);
    let commands = h.peer.closed().await;
    bounded(async {
        while !h.provider.session_joined() {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
    assert!(commands.is_empty());
    assert!(sender
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .is_err());
    assert!(h.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn cancelled_join_waiter_does_not_abandon_end_cleanup_of_active_response() {
    let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![]).await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(h.peer.command().await["type"], "response.create");
    h.peer.response_start("active-end");
    h.observe(|f| matches!(f, FrameData::LlmResponseStart))
        .await;
    h.barrier().await;
    h.worker.end().await.unwrap();
    assert!(
        timeout(QUIET, h.worker.join()).await.is_err(),
        "End must still own the active response"
    );
    h.peer.response_end("active-end");
    let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.total_errors, 0);
    assert!(commands.is_empty());
    assert!(h.provider.session_joined());
    assert!(h.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn end_drains_active_final_callback_then_response_without_new_continuation() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let mut handler = RegisteredTool::new(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let completed = completed.clone();
        move |_| {
            let entered = entered.clone();
            let release = release.clone();
            let completed = completed.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                completed.store(true, Ordering::SeqCst);
                Ok(json!("drained-result"))
            })
        }
    }));
    handler.timeout = Duration::from_secs(2);
    let tool = AzureLocalTool {
        schema: FunctionTool::new("wait", "Synthetic bounded drain", json!({"type":"object"})),
        handler,
    };
    let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![tool]).await;
    h.peer.response_start("end-tool");
    h.peer.tool("end-tool", "end-call", "wait", "{}");
    bounded(entered.notified()).await;
    h.worker.end().await.unwrap();
    h.peer.quiet().await;
    assert!(!completed.load(Ordering::SeqCst));
    release.notify_one();
    let output = bounded(h.peer.commands.recv()).await;
    let Some(output) = output else {
        let report = timeout(WAIT, h.worker.collect()).await;
        panic!("End closed before final tool delivery; pipeline={report:?}; journal={:?}; callback_completed={}",
            h.outcomes.snapshot(), completed.load(Ordering::SeqCst));
    };
    assert_eq!(output["item"]["call_id"], "end-call");
    assert_eq!(output["item"]["output"], "\"drained-result\"");
    h.peer.quiet().await;
    h.peer.response_end("end-tool");
    let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        commands.is_empty(),
        "End created fresh inference: {commands:?}"
    );
    assert!(completed.load(Ordering::SeqCst));
    assert_eq!(h.outcomes.snapshot().total_terminal, 1);
    assert!(h.outcomes.snapshot().active.is_empty());
    assert!(h.provider.session_joined());
}

#[tokio::test]
async fn downstream_setup_and_cleanup_failures_remain_visible_and_close_connected_peer() {
    struct FailSetup {
        release: Arc<Notify>,
        cleaned: Arc<AtomicBool>,
    }
    #[async_trait]
    impl FrameProcessor for FailSetup {
        fn name(&self) -> &str {
            "SyntheticFailSetupAndCleanup"
        }
        async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
            self.release.notified().await;
            Err(ProcessorError::new("synthetic setup failure"))
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.cleaned.store(true, Ordering::SeqCst);
            Err(ProcessorError::new("synthetic cleanup failure"))
        }
    }
    let (mut peer, config) = Peer::new(Mode::MANUAL_KEY, true).await;
    let release = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    let app = AzureRealtimeApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(FailSetup {
            release: release.clone(),
            cleaned: cleaned.clone(),
        }),
    )
    .unwrap();
    let start = tokio::spawn(app.start());
    assert_eq!(peer.command().await["type"], "session.update");
    release.notify_one();
    let error = bounded(start)
        .await
        .unwrap()
        .err()
        .expect("startup failure must not produce running app");
    let text = error.to_string();
    assert!(
        text.contains("setup") || text.contains("startup"),
        "primary failure lost: {text}"
    );
    assert!(text.contains("cleanup"), "cleanup diagnostic lost: {text}");
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(peer
        .closed()
        .await
        .iter()
        .all(|v| v["type"] == "session.update"));
}

#[tokio::test]
async fn aborting_start_waiter_during_connected_output_setup_does_not_orphan_socket() {
    struct BlockSetup {
        entered: Arc<Notify>,
        dropped: Arc<AtomicBool>,
    }
    #[async_trait]
    impl FrameProcessor for BlockSetup {
        fn name(&self) -> &str {
            "SyntheticBlockingSetup"
        }
        async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
            let _guard = DropFlag(self.dropped.clone());
            self.entered.notify_one();
            std::future::pending::<Result<(), ProcessorError>>().await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.push(frame, direction).await
        }
    }
    let (mut peer, config) = Peer::new(Mode::MANUAL_KEY, true).await;
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let app = AzureRealtimeApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(BlockSetup {
            entered: entered.clone(),
            dropped: dropped.clone(),
        }),
    )
    .unwrap();
    let start = tokio::spawn(app.start());
    assert_eq!(peer.command().await["type"], "session.update");
    bounded(entered.notified()).await;
    assert!(!start.is_finished());
    start.abort();
    assert!(bounded(start)
        .await
        .is_err_and(|error| error.is_cancelled()));
    let commands = peer.closed().await;
    assert!(commands.iter().all(|v| v["type"] == "session.update"));
    bounded(async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await;
}

#[tokio::test]
async fn output_failure_is_terminal_and_joins_provider_before_returning() {
    struct FailAudio;
    #[async_trait]
    impl FrameProcessor for FailAudio {
        fn name(&self) -> &str {
            "SyntheticFailAudioOutput"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::TtsAudio(_)) {
                return Err(ProcessorError::fatal("synthetic output write failure"));
            }
            ctx.push(frame, direction).await
        }
    }
    let (mut peer, config) = Peer::new(Mode::MANUAL_KEY, true).await;
    let running = bounded(
        AzureRealtimeApp::new(config, Context::default(), vec![], Arc::new(FailAudio))
            .unwrap()
            .start(),
    )
    .await
    .unwrap();
    peer.startup().await;
    peer.response_start("output-failure");
    peer.audio("output-failure", "audio", &[1, 2]);
    let (result, commands) = tokio::join!(bounded(running.worker.collect()), peer.closed());
    let error = result.expect_err("output failure must surface in complete report");
    assert!(error.to_string().contains("synthetic output write failure"));
    assert!(commands.iter().all(|v| v["type"] != "response.create"));
    assert!(running.provider.session_joined());
    assert!(running.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn readiness_gates_seed_and_pcm_until_acknowledgment_without_inference() {
    let (mut peer, config) = Peer::new(Mode::MANUAL_KEY, false).await;
    let running = bounded(
        AzureRealtimeApp::new(config, history(), vec![], Arc::new(Probe::default()))
            .unwrap()
            .start(),
    )
    .await
    .unwrap();
    assert_eq!(peer.command().await["type"], "session.update");
    running
        .worker
        .sender()
        .send(pcm(vec![7, 8, 9, 10], false), Direction::Downstream)
        .await
        .unwrap();
    peer.quiet().await;
    peer.event(json!({"type":"session.updated","session":{"id":"ready-later","type":"realtime"}}));
    assert_eq!(peer.command().await["type"], "session.update");
    let seed = peer.command().await;
    assert_eq!(seed["type"], "conversation.item.create");
    assert!(seed["item"]["content"][0]["text"]
        .as_str()
        .unwrap()
        .contains("old question"));
    let audio = peer.command().await;
    assert_eq!(audio["type"], "input_audio_buffer.append");
    assert_eq!(
        STANDARD.decode(audio["audio"].as_str().unwrap()).unwrap(),
        [7, 8, 9, 10]
    );
    peer.quiet().await;
    running.worker.end().await.unwrap();
    let (result, commands) = tokio::join!(bounded(running.worker.collect()), peer.closed());
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(commands.is_empty());
    assert!(running.provider.session_joined());
}

#[tokio::test]
async fn readiness_timeout_joins_session_and_never_replays_buffered_audio() {
    let (mut peer, mut config) = Peer::new(Mode::MANUAL_KEY, false).await;
    config.limits.connect_timeout = Duration::from_millis(300);
    let running = bounded(
        AzureRealtimeApp::new(
            config,
            Context::default(),
            vec![],
            Arc::new(Probe::default()),
        )
        .unwrap()
        .start(),
    )
    .await
    .unwrap();
    assert_eq!(peer.command().await["type"], "session.update");
    running
        .worker
        .sender()
        .send(pcm(vec![7, 8], false), Direction::Downstream)
        .await
        .unwrap();
    let (result, commands) = tokio::join!(timeout(WAIT, running.worker.collect()), peer.closed());
    let result =
        result.expect("readiness failure did not terminate and join the complete pipeline");
    assert!(result.is_err(), "readiness timeout must remain terminal");
    assert!(
        commands.is_empty(),
        "readiness failure replayed pending input: {commands:?}"
    );
    assert!(running.provider.session_joined());
    assert!(running.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn interruption_preserves_manual_preroll_suppresses_stale_output_and_truncates_only_receipted_audio(
) {
    for acknowledged in [false, true] {
        let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![]).await;
        h.send_frame(pcm(vec![7, 8], false)).await;
        assert_eq!(h.peer.command().await["type"], "input_audio_buffer.append");
        h.send(FrameData::LlmRun).await;
        assert_eq!(h.peer.command().await["type"], "response.create");
        h.peer.response_start("old-response");
        h.peer.audio("old-response", "old-audio", &vec![0; 4800]);
        h.observe(|f| matches!(f, FrameData::TtsAudio(_))).await;
        h.barrier().await;
        if acknowledged {
            // Synthetic transport confirmation is explicit. The Probe/file output
            // alone never manufactures this played position.
            h.provider
                .acknowledge_playback(PlaybackPosition {
                    item_id: "old-audio".into(),
                    content_index: 0,
                    audio_end_ms: 25,
                })
                .unwrap();
        }
        h.send(FrameData::Interruption).await;
        assert_eq!(h.peer.command().await["type"], "input_audio_buffer.clear");
        let replay = h.peer.command().await;
        assert_eq!(replay["type"], "input_audio_buffer.append");
        assert_eq!(
            STANDARD.decode(replay["audio"].as_str().unwrap()).unwrap(),
            [7, 8]
        );
        assert_eq!(h.peer.command().await["type"], "response.cancel");
        if acknowledged {
            let truncate = h.peer.command().await;
            assert_eq!(truncate["type"], "conversation.item.truncate");
            assert_eq!(truncate["item_id"], "old-audio");
            assert_eq!(truncate["audio_end_ms"], 25);
        }
        h.peer.quiet().await;
        h.peer.audio("old-response", "old-audio", &[99, 100]);
        h.peer.event(json!({"type":"response.output_audio_transcript.delta","response_id":"old-response","item_id":"old-audio","output_index":0,"content_index":0,"delta":"stale text"}));
        h.peer.event(json!({"type":"response.done","response":{"id":"old-response","status":"cancelled","output":[]}}));
        h.send(FrameData::UserStartedSpeaking).await;
        h.send(FrameData::UserStoppedSpeaking).await;
        assert_eq!(h.peer.command().await["type"], "input_audio_buffer.commit");
        assert_eq!(h.peer.command().await["type"], "response.create");
        h.peer.response_start("fresh-response");
        h.peer.audio("fresh-response", "fresh-audio", &[11, 12]);
        h.peer.event(json!({"type":"response.output_audio_transcript.delta","response_id":"fresh-response","item_id":"fresh-audio","output_index":0,"content_index":0,"delta":"fresh text"}));
        h.peer.response_end("fresh-response");
        h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
        h.barrier().await;
        h.peer.quiet().await;
        assert!(!h.probe.0.lock().unwrap().iter().any(
            |f| matches!(&f.frame.data, FrameData::TtsAudio(a) if a.audio == [99,100])
                || matches!(&f.frame.data, FrameData::LlmText(t) if t.text.contains("stale text"))
        ));
        assert_eq!(texts(&h, ContextRole::Assistant), ["fresh text"]);
        h.finish(0).await;
    }
}

#[tokio::test]
async fn fatal_start_processing_preserves_primary_and_cleanup_diagnostics() {
    struct FatalStart {
        release: Arc<Notify>,
        cleaned: Arc<AtomicBool>,
    }
    #[async_trait]
    impl FrameProcessor for FatalStart {
        fn name(&self) -> &str {
            "SyntheticFatalStartAndCleanup"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(
                frame.data,
                FrameData::Start(_) | FrameData::StartDetailed(_)
            ) {
                self.release.notified().await;
                return Err(ProcessorError::fatal("synthetic fatal Start primary"));
            }
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.cleaned.store(true, Ordering::SeqCst);
            Err(ProcessorError::new("synthetic fatal Start cleanup"))
        }
    }
    let (mut peer, config) = Peer::new(Mode::MANUAL_KEY, true).await;
    let release = Arc::new(Notify::new());
    let cleaned = Arc::new(AtomicBool::new(false));
    let app = AzureRealtimeApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(FatalStart {
            release: release.clone(),
            cleaned: cleaned.clone(),
        }),
    )
    .unwrap();
    let start = tokio::spawn(app.start());
    assert_eq!(peer.command().await["type"], "session.update");
    release.notify_one();
    let error = bounded(start)
        .await
        .unwrap()
        .err()
        .expect("fatal Start must fail before returning a running app");
    let text = error.to_string();
    assert!(
        text.contains("synthetic fatal Start primary"),
        "primary diagnostic lost: {text}"
    );
    assert!(
        text.contains("synthetic fatal Start cleanup"),
        "cleanup diagnostic lost: {text}"
    );
    assert!(cleaned.load(Ordering::SeqCst));
    assert!(peer
        .closed()
        .await
        .iter()
        .all(|v| v["type"] == "session.update"));
}

#[tokio::test]
async fn malformed_known_event_provider_error_and_active_eof_fail_closed_and_join() {
    for fault in 0..4 {
        let mode = Mode {
            bearer: fault % 2 == 1,
            server_vad: false,
        };
        let mut h = Harness::new(mode, Context::default(), vec![]).await;
        h.send(FrameData::LlmRun).await;
        assert_eq!(h.peer.command().await["type"], "response.create");
        h.peer.response_start("broken-response");
        h.observe(|f| matches!(f, FrameData::LlmResponseStart))
            .await;
        h.barrier().await;
        match fault {
            0 => h.peer.event(json!({"type":"response.output_audio.delta","response_id":"broken-response","item_id":"audio","output_index":0,"content_index":0,"delta":17})),
            1 => h.peer.event(json!({"type":"error","error":{"type":"invalid_request_error","code":"synthetic_error","message":format!("synthetic provider rejection {BEARER}")}})),
            2 => h.peer.events.send(PeerInput::Close).unwrap(),
            _ => h.peer.events.send(PeerInput::Drop).unwrap(),
        }
        let (result, commands) = tokio::join!(timeout(WAIT, h.worker.collect()), h.peer.closed());
        let result = result.unwrap_or_else(|_| {
            panic!("fault {fault} did not terminate and join complete pipeline")
        });
        let failure = result
            .err()
            .unwrap_or_else(|| panic!("fault {fault} must remain terminal"));
        let diagnostic = failure.to_string();
        assert!(
            !diagnostic.contains(KEY) && !diagnostic.contains(BEARER),
            "provider failure leaked a configured credential"
        );
        if fault == 1 {
            assert!(diagnostic.contains("synthetic provider rejection"));
        }
        assert!(!matches!(failure, PipelineError::ShutdownTimeout));
        assert!(commands.iter().all(|v| v["type"] != "response.create" && v["type"] != "input_audio_buffer.append"), "provider failure triggered replay: {commands:?}");
        assert!(h.provider.session_joined());
        assert!(h.outcomes.snapshot().active.is_empty());
        assert!(!format!("{:?}", h.pair.context().snapshot().unwrap())
            .contains("synthetic provider rejection"));
    }
}

#[tokio::test]
async fn failed_provider_response_is_fatal_and_retains_primary_after_actual_join() {
    let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![]).await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(h.peer.command().await["type"], "response.create");
    h.peer.response_start("failed-response");
    h.observe(|f| matches!(f, FrameData::LlmResponseStart))
        .await;
    h.peer.event(json!({"type":"response.done","response":{"id":"failed-response","status":"failed","output":[],"status_details":{"error":{"code":"synthetic_failure","message":"synthetic failed response"}}}}));
    // Opted-in app ownership deliberately treats failed responses as fatal.
    // Keep the worker after join so the primary and recorded diagnostics can
    // both be checked, rather than manufacturing a successful End report.
    let (result, commands) = tokio::join!(bounded(h.worker.join()), h.peer.closed());
    let failure = result.expect_err("failed response must fail the complete app join");
    assert!(
        failure.to_string().contains("synthetic failed response"),
        "primary response failure lost: {failure}"
    );
    assert_eq!(h.worker.state(), WorkerState::Failed);
    assert!(h.worker.total_errors() >= 1);
    assert_eq!(h.worker.evicted_errors(), 0);
    assert!(
        h.worker
            .errors()
            .iter()
            .any(|error| error.fatal && error.error.contains("synthetic failed response")),
        "fatal response diagnostic missing from the joined worker"
    );
    assert!(commands.is_empty());
    assert!(h.provider.session_joined());
    assert!(h.outcomes.snapshot().active.is_empty());
    assert!(h.pair.context().snapshot().unwrap().messages.is_empty());
}

#[tokio::test]
async fn end_after_response_done_still_waits_for_native_final_delivery_without_continuation() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let mut handler = RegisteredTool::new(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let completed = completed.clone();
        move |_| {
            let entered = entered.clone();
            let release = release.clone();
            let completed = completed.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                completed.store(true, Ordering::SeqCst);
                Ok(json!({"final":"after-response-done"}))
            })
        }
    }));
    handler.timeout = Duration::from_secs(2);
    let tool = AzureLocalTool {
        schema: FunctionTool::new(
            "wait",
            "Synthetic delayed final result",
            json!({"type":"object"}),
        ),
        handler,
    };
    let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![tool]).await;
    h.peer.response_start("done-before-callback");
    h.peer
        .tool("done-before-callback", "delayed-final-call", "wait", "{}");
    bounded(entered.notified()).await;
    h.peer.response_end("done-before-callback");
    h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
    h.barrier().await;
    assert_eq!(h.outcomes.snapshot().active.len(), 1);
    h.worker.end().await.unwrap();
    assert!(
        timeout(QUIET, h.worker.join()).await.is_err(),
        "response.done cannot discharge the outstanding final tool receipt"
    );
    h.peer.quiet().await;
    assert!(!completed.load(Ordering::SeqCst));
    release.notify_one();
    let output = h.peer.command().await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], "delayed-final-call");
    assert_eq!(
        output["item"]["output"],
        "{\"final\":\"after-response-done\"}"
    );
    let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0);
    assert!(report.errors.is_empty());
    assert!(
        commands.is_empty(),
        "drained tool settlement created a fresh continuation: {commands:?}"
    );
    assert!(completed.load(Ordering::SeqCst));
    assert!(h.provider.session_joined());
    let journal = h.outcomes.snapshot();
    assert_eq!(journal.total_terminal, 1);
    assert!(journal.active.is_empty());
    assert_eq!(
        journal.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
    assert_eq!(journal.unadmitted_terminal, 0);
}

#[tokio::test]
async fn peer_close_after_response_done_with_unsettled_callback_is_fatal_even_during_end() {
    for ending in [false, true] {
        let entered = Arc::new(Notify::new());
        let dropped = Arc::new(AtomicBool::new(false));
        let mut h = Harness::new(
            Mode::MANUAL_KEY,
            Context::default(),
            vec![waiting_tool(entered.clone(), dropped.clone())],
        )
        .await;
        h.peer.response_start("done-but-unsettled");
        h.peer
            .tool("done-but-unsettled", "unsettled-call", "wait", "{}");
        bounded(entered.notified()).await;
        h.peer.response_end("done-but-unsettled");
        h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
        h.barrier().await;
        assert_eq!(h.outcomes.snapshot().active.len(), 1);
        if ending {
            h.worker.end().await.unwrap();
            assert!(
                timeout(QUIET, h.worker.join()).await.is_err(),
                "End completed with an unsettled native callback"
            );
        }
        h.peer.events.send(PeerInput::Close).unwrap();
        let (result, commands) = tokio::join!(timeout(WAIT, h.worker.collect()), h.peer.closed());
        let result = result
            .unwrap_or_else(|_| panic!("unsettled close failed to join, intentional End={ending}"));
        let failure = result.err().unwrap_or_else(|| {
            panic!("unsettled peer close was reported as clean, intentional End={ending}")
        });
        assert!(!matches!(failure, PipelineError::ShutdownTimeout));
        assert!(
            commands.is_empty(),
            "failed session emitted settlement or continuation after close: {commands:?}"
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert!(h.provider.session_joined());
        let journal = h.outcomes.snapshot();
        assert!(journal.active.is_empty());
        assert_eq!(journal.total_terminal, 1);
        assert_ne!(
            journal.terminal[0].outcome.state(),
            ToolCallTerminalState::Completed
        );
    }
}

#[tokio::test]
async fn reserved_assistant_flush_cannot_commit_unfinished_text_from_either_endpoint() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        for detailed in [false, true] {
            let mut h = Harness::new(Mode::MANUAL_KEY, history(), vec![]).await;
            h.peer.response_start("unfinished-assistant");
            h.peer.event(json!({"type":"response.output_text.delta","response_id":"unfinished-assistant","item_id":"unfinished-item","output_index":0,"content_index":0,"delta":"not-yet-final"}));
            h.observe(|f| matches!(f, FrameData::LlmText(_))).await;
            h.barrier().await;
            let original = h.pair.context().snapshot().unwrap();
            assert!(
                !format!("{original:?}").contains("not-yet-final"),
                "fixture text was already committed before the forged boundary"
            );
            let forged = if detailed {
                FrameData::FlushDetailed(PipelineFlushFrame {
                    event: None,
                    returning: false,
                    origin: Some("assistant_turn_end".into()),
                })
            } else {
                FrameData::Flush(FlushFrame {
                    returning: false,
                    origin: Some("assistant_turn_end".into()),
                })
            };
            h.worker
                .sender()
                .send(Frame::new(forged), direction)
                .await
                .unwrap();
            let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
            assert!(matches!(result, Err(PipelineError::Execution(_))), "reserved flush must be a fatal boundary rejection: detailed={detailed}, direction={direction:?}, result={result:?}");
            assert_eq!(
                h.pair.context().snapshot().unwrap(),
                original,
                "reserved flush committed unfinished assistant text"
            );
            assert!(
                commands.is_empty(),
                "reserved flush caused a provider action: {commands:?}"
            );
            assert!(h.provider.session_joined());
            assert!(h.outcomes.snapshot().active.is_empty());
        }
    }
}

#[tokio::test]
async fn cancel_supersedes_end_waiting_for_active_callback_and_joins_promptly() {
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let mut h = Harness::new(
        Mode::MANUAL_KEY,
        Context::default(),
        vec![waiting_tool(entered.clone(), dropped.clone())],
    )
    .await;
    h.peer.response_start("end-then-cancel");
    h.peer
        .tool("end-then-cancel", "cancel-draining-call", "wait", "{}");
    bounded(entered.notified()).await;
    h.worker.end().await.unwrap();
    assert!(
        timeout(QUIET, h.worker.join()).await.is_err(),
        "End must be pending on the active callback"
    );
    assert!(!dropped.load(Ordering::SeqCst));
    h.worker
        .cancel("synthetic Cancel supersedes End")
        .await
        .unwrap();
    // Less than this callback's two-second deadline and the three-second
    // response deadline: success must be cancellation, not passive expiry.
    let (result, commands) = timeout(Duration::from_secs(1), async {
        tokio::join!(h.worker.collect(), h.peer.closed())
    })
    .await
    .expect("Cancel failed to supersede the active End gate promptly");
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(report.total_errors, 0);
    assert!(report.errors.is_empty());
    assert!(commands
        .iter()
        .all(|command| command["type"] != "response.create"));
    assert!(dropped.load(Ordering::SeqCst));
    assert!(h.provider.session_joined());
    let journal = h.outcomes.snapshot();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_ne!(
        journal.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
}

#[tokio::test]
async fn tiny_pre_ready_actor_queue_reports_audio_backpressure_then_cancel_joins_without_replay() {
    let (mut peer, mut config) = Peer::new(Mode::MANUAL_KEY, false).await;
    config.limits.queue_capacity = 4;
    let mut running = bounded(
        AzureRealtimeApp::new(
            config,
            Context::default(),
            vec![],
            Arc::new(Probe::default()),
        )
        .unwrap()
        .start(),
    )
    .await
    .unwrap();
    assert_eq!(peer.command().await["type"], "session.update");
    let mut admitted = 0_u64;
    let mut rejected = 0_u64;
    // Five two-byte frames saturate a four-command actor queue. The app's
    // already-admitted seed may occupy one slot; do not mistake it for audio.
    for index in 0..5 {
        let frame = pcm(vec![index, 0], index % 2 == 1);
        let id = frame.id;
        running
            .worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        let accepted = bounded(async {
            loop {
                let output = running
                    .worker
                    .recv()
                    .await
                    .expect("pipeline closed before bounded audio admission result");
                if let FrameData::Error(error) = &output.frame.data {
                    assert!(!error.fatal);
                    assert!(
                        error.error.contains("queue is full"),
                        "unexpected saturation diagnostic: {error:?}"
                    );
                    break false;
                }
                if output.frame.id == id
                    && matches!(
                        output.frame.data,
                        FrameData::InputAudio(_) | FrameData::UserAudio(_)
                    )
                {
                    break true;
                }
            }
        })
        .await;
        if accepted {
            admitted += 1;
        } else {
            rejected += 1;
        }
    }
    assert!(admitted > 0 && rejected > 0);
    assert_eq!(admitted + rejected, 5);
    peer.quiet().await;
    running
        .worker
        .cancel("synthetic pre-ready audio saturation")
        .await
        .unwrap();
    let (result, commands) = timeout(Duration::from_secs(1), async {
        tokio::join!(running.worker.collect(), peer.closed())
    })
    .await
    .expect("Cancel did not bypass the saturated pre-ready actor queue");
    let (_, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(report.total_errors, rejected);
    assert_eq!(report.errors.len() as u64, rejected);
    assert!(
        commands.is_empty(),
        "unacknowledged seed/audio was replayed during Cancel: {commands:?}"
    );
    assert!(running.provider.session_joined());
    assert!(running.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn cancel_drops_and_joins_simulated_blocked_output_without_claiming_delivery() {
    struct BlockAudio {
        entered: Arc<Notify>,
        dropped: Arc<AtomicBool>,
        delivered: Arc<AtomicBool>,
    }
    #[async_trait]
    impl FrameProcessor for BlockAudio {
        fn name(&self) -> &str {
            "SyntheticCancellableBlockedAudioOutput"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::TtsAudio(_)) {
                let _guard = DropFlag(self.dropped.clone());
                self.entered.notify_one();
                // This models a cancellable output future, not an OS/device
                // write timeout or actual physical playback qualification.
                std::future::pending::<()>().await;
                self.delivered.store(true, Ordering::SeqCst);
            }
            ctx.push(frame, direction).await
        }
    }
    let (mut peer, config) = Peer::new(Mode::MANUAL_KEY, true).await;
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let delivered = Arc::new(AtomicBool::new(false));
    let running = bounded(
        AzureRealtimeApp::new(
            config,
            Context::default(),
            vec![],
            Arc::new(BlockAudio {
                entered: entered.clone(),
                dropped: dropped.clone(),
                delivered: delivered.clone(),
            }),
        )
        .unwrap()
        .start(),
    )
    .await
    .unwrap();
    peer.startup().await;
    peer.response_start("blocked-output");
    peer.audio("blocked-output", "blocked-audio", &[1, 2]);
    bounded(entered.notified()).await;
    assert!(!dropped.load(Ordering::SeqCst));
    assert!(!delivered.load(Ordering::SeqCst));
    running
        .worker
        .cancel("synthetic blocked output cancellation")
        .await
        .unwrap();
    let (result, commands) = timeout(Duration::from_secs(1), async {
        tokio::join!(running.worker.collect(), peer.closed())
    })
    .await
    .expect("Cancel did not interrupt and join the simulated blocked output future");
    let (frames, report) = result.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(report.total_errors, 0);
    assert!(report.errors.is_empty());
    assert!(dropped.load(Ordering::SeqCst));
    assert!(!delivered.load(Ordering::SeqCst));
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.frame.data, FrameData::TtsAudio(_))));
    assert!(commands.is_empty());
    assert!(running.provider.session_joined());
    assert!(running.outcomes.snapshot().active.is_empty());
    assert!(running
        .pair
        .context()
        .snapshot()
        .unwrap()
        .messages
        .is_empty());
}

#[tokio::test]
async fn end_rejects_new_tool_admission_and_post_finish_argument_completion_without_callback() {
    for declared_before_end in [false, true] {
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::new(
            Mode::MANUAL_KEY,
            Context::default(),
            vec![echo(count.clone())],
        )
        .await;
        h.send(FrameData::LlmRun).await;
        assert_eq!(h.peer.command().await["type"], "response.create");
        h.peer.response_start("end-admission-boundary");
        h.observe(|f| matches!(f, FrameData::LlmResponseStart))
            .await;
        if declared_before_end {
            h.peer.event(json!({"type":"response.output_item.added","response_id":"end-admission-boundary","output_index":0,"item":{"type":"function_call","id":"item-late-call","call_id":"late-call","name":"echo","arguments":""}}));
            // Processing a following text event proves the earlier declaration
            // reached the actor, without completing or executing that callback.
            h.peer.event(json!({"type":"response.output_text.delta","response_id":"end-admission-boundary","item_id":"ordering-item","output_index":1,"content_index":0,"delta":"waiting for arguments"}));
            h.observe(|f| matches!(f, FrameData::LlmText(_))).await;
        }
        h.barrier().await;
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert!(h.outcomes.snapshot().active.is_empty());
        h.worker.end().await.unwrap();
        assert!(
            timeout(QUIET, h.worker.join()).await.is_err(),
            "End must remain pending on the active response before the late call"
        );
        if declared_before_end {
            h.peer
                .arguments_done("end-admission-boundary", "late-call", "{\"n\":7}");
        } else {
            h.peer
                .tool("end-admission-boundary", "late-call", "echo", "{\"n\":7}");
        }
        let (result, commands) = tokio::join!(timeout(WAIT, h.worker.collect()), h.peer.closed());
        let result = result.unwrap_or_else(|_| {
            panic!("late tool admission did not terminate/join, predeclared={declared_before_end}")
        });
        let failure = result.err().unwrap_or_else(|| panic!("late tool admission after End was reported as clean, predeclared={declared_before_end}"));
        assert!(!matches!(failure, PipelineError::ShutdownTimeout));
        assert_eq!(
            count.load(Ordering::SeqCst),
            0,
            "End admitted a new callback"
        );
        assert!(
            commands
                .iter()
                .all(|v| v["type"] != "response.create"
                    && v["item"]["type"] != "function_call_output"),
            "late tool admission emitted output or new inference: {commands:?}"
        );
        assert!(h.provider.session_joined());
        let journal = h.outcomes.snapshot();
        assert!(journal.active.is_empty());
        assert_eq!(
            journal.total_terminal, 0,
            "an unadmitted call must not synthesize a native terminal receipt"
        );
    }
}

#[tokio::test]
async fn immediate_end_preserves_preceding_queued_text_and_run_inference() {
    for iteration in 0..4 {
        let input_text = iteration % 2 == 0;
        let mut h = Harness::new(Mode::MANUAL_KEY, Context::default(), vec![]).await;
        let caller_event = LocalFlushEvent::default();
        h.send(FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(caller_event.clone()),
            returning: false,
            origin: Some("azure-provider-end-order".into()),
        }))
        .await;
        bounded(async {
            loop {
                let returned =
                    h.worker.recv().await.expect(
                        "caller look-alike flush was swallowed as a private acknowledgment",
                    );
                if let FrameData::Error(error) = &returned.frame.data {
                    panic!("ordinary caller barrier rejected: {error:?}");
                }
                if let FrameData::FlushDetailed(flush) = &returned.frame.data {
                    if flush.event.as_ref() == Some(&caller_event) {
                        assert!(flush.returning);
                        assert_eq!(returned.direction, Direction::Downstream);
                        assert_eq!(flush.origin.as_deref(), Some("azure-provider-end-order"));
                        assert!(caller_event.is_set());
                        break;
                    }
                }
            }
        })
        .await;
        assert!(
            h.peer.commands.try_recv().is_err(),
            "ordinary caller flush emitted a provider command"
        );
        let text = format!("queued-before-End-{iteration}");
        h.send(if input_text {
            FrameData::InputText(TextFrame::new(&text))
        } else {
            FrameData::LlmRun
        })
        .await;
        // Deliberately no peer read, sleep, flush or output observation here.
        // Queue admission of the user input must retain its order before End.
        h.worker.end().await.unwrap();
        let mut items = 0;
        let mut clears = 0;
        let mut created = false;
        for _ in 0..3 {
            let command = h.peer.command().await;
            match command["type"].as_str() {
                Some("input_audio_buffer.clear") => {
                    assert!(input_text, "LlmRun cannot fabricate a manual interruption");
                    clears += 1;
                    assert_eq!(clears, 1, "typed input must clear at most once");
                }
                Some("conversation.item.create") => {
                    assert!(input_text);
                    items += 1;
                    assert_eq!(items, 1, "queued text must not duplicate its durable item");
                    assert_eq!(command["item"]["type"], "message");
                    assert_eq!(command["item"]["role"], "user");
                    assert_eq!(command["item"]["content"][0]["text"], text);
                }
                Some("response.create") => {
                    assert_eq!(items, usize::from(input_text));
                    created = true;
                    break;
                }
                _ => panic!("unexpected command before queued input inference: {command:?}"),
            }
        }
        assert!(
            created,
            "End overtook the queued input's response.create on iteration {iteration}"
        );
        let response = format!("queued-response-{iteration}");
        h.peer.response_start(&response);
        h.peer.response_end(&response);
        let (result, commands) = tokio::join!(bounded(h.worker.collect()), h.peer.closed());
        let (_, report) = result.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 0);
        assert!(report.errors.is_empty());
        assert_eq!(report.dropped_frames, 0);
        assert!(
            commands.is_empty(),
            "End duplicated inference or history: {commands:?}"
        );
        assert!(h.provider.session_joined());
        assert!(h.outcomes.snapshot().active.is_empty());
        let users: Vec<_> = h
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .into_iter()
            .filter(|message| message.role == ContextRole::User)
            .collect();
        assert_eq!(users.len(), usize::from(input_text));
        if input_text {
            assert_eq!(users[0].content, Some(Content::Text(text)));
        }
    }
}
