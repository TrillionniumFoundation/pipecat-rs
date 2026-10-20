//! Full-constructor, real-WebSocket causal-edge tests. These private hooks only
//! observe or fault genuine production traffic; they cannot create native proof.
use crate::{app_state::AppState, *};
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_media_transport::{
    OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt,
};
use pipecat_processors::tools::RegisteredTool;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Notify, Semaphore},
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

const WAIT: Duration = Duration::from_secs(10);
const NEGATIVE: Duration = Duration::from_millis(60);
const KEY: &str = "synthetic-app-private-edge-key";
type Ws = WebSocketStream<TcpStream>;

pub(crate) struct TestGate {
    armed: AtomicBool,
    entered: Notify,
    release: Semaphore,
    resumed: Notify,
    passes: AtomicUsize,
}
impl Default for TestGate {
    fn default() -> Self {
        Self {
            armed: AtomicBool::new(false),
            entered: Notify::new(),
            release: Semaphore::new(0),
            resumed: Notify::new(),
            passes: AtomicUsize::new(0),
        }
    }
}
impl TestGate {
    fn arm(&self) {
        assert!(!self.armed.swap(true, Ordering::SeqCst));
    }
    /// Hook callers clone the hook Arc before awaiting. No ledger/provider
    /// mutex is retained; the input hook deliberately holds input admission.
    /// Cancelling the app releases a failed test's held operation.
    pub(crate) async fn pause(&self, state: &AppState) {
        if !self.armed.swap(false, Ordering::SeqCst) {
            return;
        }
        self.entered.notify_one();
        tokio::select! { biased;
            _ = state.cancelled() => {}
            permit = self.release.acquire() => {
                permit.expect("private test gate closed").forget();
                self.passes.fetch_add(1, Ordering::SeqCst);
                self.resumed.notify_one();
            }
        }
    }
    async fn entered(&self) {
        timeout(WAIT, self.entered.notified())
            .await
            .expect("real production edge never reached test gate");
    }
    fn release(&self) {
        self.release.add_permits(1);
    }
    async fn resumed(&self) {
        timeout(WAIT, self.resumed.notified())
            .await
            .expect("retained production operation never resumed");
    }
}
#[derive(Default)]
pub(crate) struct TestHooks {
    pub(crate) declaration: TestGate,
    pub(crate) input: TestGate,
    pub(crate) completion_wait: TestGate,
    pub(crate) interruption: TestGate,
    pub(crate) callback: TestGate,
    pub(crate) guard: TestGate,
    guard_rechecks: AtomicUsize,
    guard_destination_current: AtomicBool,
    guard_source_current: AtomicBool,
    guard_checked: Notify,
    pub(crate) continuation: TestGate,
    pub(crate) bot_stopped: TestGate,
    receive_panic_armed: AtomicBool,
    receive_panics: AtomicUsize,
    audio_write: AudioWriteProbe,
}

impl TestHooks {
    pub(crate) fn panic_after_receive_event(&self) {
        if self.receive_panic_armed.swap(false, Ordering::SeqCst) {
            self.receive_panics.fetch_add(1, Ordering::SeqCst);
            panic!("synthetic panic in actual Grok receive owner");
        }
    }
    /// Polls the exact socket.send future. It never changes readiness, returns
    /// an invented Pending, delays a poll, or participates in cancellation.
    pub(crate) async fn observe_audio_write<F: Future>(&self, send: F) -> F::Output {
        let mut observation = AudioWriteObservation::new(&self.audio_write);
        tokio::pin!(send);
        futures_util::future::poll_fn(|cx| {
            let result = send.as_mut().poll(cx);
            observation.polled(result.is_pending());
            result
        })
        .await
    }
    /// Observation only: the production guard supplies the actual retained
    /// source and destination liveness after resuming its real process call.
    pub(crate) fn note_guard_recheck(&self, destination_current: bool, source_current: bool) {
        self.guard_destination_current
            .store(destination_current, Ordering::SeqCst);
        self.guard_source_current
            .store(source_current, Ordering::SeqCst);
        self.guard_rechecks.fetch_add(1, Ordering::SeqCst);
        self.guard_checked.notify_one();
    }
    async fn guard_rechecked(&self) {
        timeout(WAIT, self.guard_checked.notified())
            .await
            .expect("held guard invocation was dropped instead of rechecking its actual source");
    }
}

#[derive(Clone, Copy, Debug, Default)]
struct AudioWriteSnapshot {
    started: usize,
    pending_polls: usize,
    completed: usize,
    dropped_pending: usize,
    inflight: Option<usize>,
    pending: bool,
}
#[derive(Default)]
struct AudioWriteProbe {
    state: Mutex<AudioWriteSnapshot>,
    changed: Notify,
}
impl AudioWriteProbe {
    fn snapshot(&self) -> AudioWriteSnapshot {
        *self.state.lock().unwrap()
    }
    async fn observed(&self, sequence: usize) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                let state = self.snapshot();
                if state.completed >= sequence
                    || (state.inflight == Some(sequence) && state.pending)
                {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("admitted PCM never reached its actual socket send");
    }
    async fn finished(&self, sequence: usize) {
        loop {
            let changed = self.changed.notified();
            let state = self.snapshot();
            if state.started >= sequence && state.inflight != Some(sequence) {
                return;
            }
            changed.await;
        }
    }
}
struct AudioWriteObservation<'a> {
    probe: &'a AudioWriteProbe,
    sequence: usize,
    ready: bool,
}
impl<'a> AudioWriteObservation<'a> {
    fn new(probe: &'a AudioWriteProbe) -> Self {
        let mut state = probe.state.lock().unwrap();
        assert!(state.inflight.is_none(), "actor writes must be serial");
        state.started += 1;
        let sequence = state.started;
        state.inflight = Some(sequence);
        state.pending = false;
        Self {
            probe,
            sequence,
            ready: false,
        }
    }
    fn polled(&mut self, pending: bool) {
        let mut state = self.probe.state.lock().unwrap();
        assert_eq!(state.inflight, Some(self.sequence));
        state.pending = pending;
        if pending {
            state.pending_polls += 1;
        } else {
            state.completed += 1;
            state.inflight = None;
            self.ready = true;
        }
        drop(state);
        self.probe.changed.notify_one();
    }
}
impl Drop for AudioWriteObservation<'_> {
    fn drop(&mut self) {
        if self.ready {
            return;
        }
        let mut state = self.probe.state.lock().unwrap();
        assert_eq!(state.inflight, Some(self.sequence));
        state.dropped_pending += usize::from(state.pending);
        state.pending = false;
        state.inflight = None;
        drop(state);
        self.probe.changed.notify_one();
    }
}

#[derive(Default)]
struct Probe {
    pcm: Mutex<Vec<u8>>,
    outcomes: Mutex<Vec<Arc<NativeToolCallOutcome>>>,
    changed: Notify,
    setups: AtomicUsize,
    cleaned: AtomicUsize,
    interruptions: AtomicUsize,
    response_frames: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "PrivateAppEdgeRecordingOnly"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.setups.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if matches!(
                &frame.data,
                FrameData::LlmResponseStart | FrameData::LlmResponseEnd
            ) {
                self.response_frames.fetch_add(1, Ordering::SeqCst);
            }
            if matches!(&frame.data, FrameData::Interruption) {
                self.interruptions.fetch_add(1, Ordering::SeqCst);
                self.changed.notify_one();
            }
            if let FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) = &frame.data {
                assert_eq!(audio.sample_rate, 24_000);
                assert_eq!(audio.num_channels, 1);
                assert!(audio.audio.len().is_multiple_of(2));
                let mut pcm = self.pcm.lock().unwrap();
                if pcm.len() + audio.audio.len() > 4096 {
                    return Err(ProcessorError::fatal("private fixture recording bound"));
                }
                pcm.extend_from_slice(&audio.audio);
            }
            if let Some(outcome) = frame.native_tool_outcome()? {
                let mut outcomes = self.outcomes.lock().unwrap();
                if outcomes.len() >= 8 {
                    return Err(ProcessorError::fatal("private fixture outcome bound"));
                }
                outcomes.push(outcome.clone());
                self.changed.notify_one();
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl Probe {
    async fn interrupted(&self) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if self.interruptions.load(Ordering::SeqCst) > 0 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .expect("real held interruption never reached output");
    }
    async fn outcomes(&self, count: usize) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if self.outcomes.lock().unwrap().len() >= count {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
fn config(listener: &TcpListener) -> GrokAppConfig {
    let mut config = GrokAppConfig::loopback(KEY, listener.local_addr().unwrap());
    config.limits.session_timeout = Duration::from_secs(20);
    config.limits.drain_timeout = Duration::from_secs(5);
    config.limits.shutdown_timeout = Duration::from_secs(2);
    config.limits.max_tool_duration = Duration::from_secs(5);
    config.connection.limits.connect_timeout = Duration::from_secs(3);
    config.connection.limits.operation_timeout = Duration::from_secs(3);
    config.connection.limits.response_timeout = Duration::from_secs(5);
    config.connection.limits.close_timeout = Duration::from_secs(1);
    config
}
fn tool(callback: pipecat_processors::tools::ToolCallback) -> GrokLocalTool {
    let mut handler = RegisteredTool::new(callback);
    handler.timeout = Duration::from_secs(4);
    GrokLocalTool {
        schema: FunctionTool::new(
            "echo",
            "Synthetic echo",
            json!({"type":"object","properties":{"n":{"type":"integer"}},"required":["n"],"additionalProperties":false}),
        ),
        handler,
    }
}
fn counted(calls: Arc<AtomicUsize>) -> GrokLocalTool {
    tool(Arc::new(move |call| {
        let calls = calls.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(call.arguments)
        })
    }))
}
async fn wire(ws: &mut Ws, mut value: Value) {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    value["event_id"] = json!(format!(
        "private-app-{}",
        EVENT.fetch_add(1, Ordering::Relaxed)
    ));
    timeout(WAIT, ws.send(Message::Text(value.to_string().into())))
        .await
        .unwrap()
        .unwrap();
}
async fn read(ws: &mut Ws) -> Value {
    let message = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
    assert!(
        message.is_text(),
        "unexpected WebSocket message {message:?}"
    );
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(NEGATIVE, ws.next()).await.is_err(),
        "unexpected command or premature close at held causal edge"
    );
}
#[allow(clippy::result_large_err)]
async fn ready(listener: &TcpListener) -> Ws {
    let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let mut ws = tokio_tungstenite::accept_hdr_async(
        tcp,
        |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            assert_eq!(request.headers()["authorization"], format!("Bearer {KEY}"));
            assert_eq!(request.uri().path(), "/v1/realtime");
            assert_eq!(request.uri().query(), Some("model=grok-voice-latest"));
            Ok(response)
        },
    )
    .await
    .unwrap();
    wire(
        &mut ws,
        json!({"type":"conversation.created","conversation":{"id":"private-app-c"}}),
    )
    .await;
    assert_eq!(read(&mut ws).await["type"], "session.update");
    wire(
        &mut ws,
        json!({"type":"session.updated","session":{"id":"private-app-s"}}),
    )
    .await;
    ws
}
async fn response(ws: &mut Ws, id: &str, status: &str) {
    wire(ws, json!({"type":if status == "in_progress" {"response.created"} else {"response.done"},"response":{"id":id,"status":status,"output":[]}})).await;
}
async fn declaration(ws: &mut Ws, call: &str, n: u64) {
    wire(ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":format!("item-{call}"),"call_id":call,"name":"echo","arguments":""}})).await;
    wire(ws, json!({"type":"response.function_call_arguments.done","call_id":call,"name":"echo","arguments":json!({"n":n}).to_string()})).await;
}
async fn pcm(ws: &mut Ws, id: &str, bytes: &[u8]) {
    wire(ws, json!({"type":"response.output_audio.delta","response_id":id,"item_id":format!("audio-{id}"),"output_index":0,"content_index":0,"delta":STANDARD.encode(bytes)})).await;
}
async fn speech(ws: &mut Ws, id: &str) {
    response(ws, id, "in_progress").await;
    pcm(ws, id, &[1, 2, 3, 4]).await;
    wire(ws, json!({"type":"response.output_audio_transcript.delta","response_id":id,"item_id":format!("audio-{id}"),"delta":"real assistant continuation"})).await;
    response(ws, id, "completed").await;
}
async fn closed(ws: &mut Ws, clean: bool) {
    timeout(WAIT, async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_))) => {
                    let _ = ws.flush().await;
                    break;
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                None | Some(Err(_)) if !clean => break,
                other => panic!("unexpected terminal wire command {other:?}"),
            }
        }
    })
    .await
    .expect("real peer did not close");
}
async fn released(listener: TcpListener) {
    assert!(timeout(NEGATIVE, listener.accept()).await.is_err());
    let address = listener.local_addr().unwrap();
    drop(listener);
    drop(TcpListener::bind(address).await.unwrap());
}
struct Harness {
    running: GrokRunningApp,
    completion: GrokAppCompletion,
    ws: Ws,
    listener: TcpListener,
}
impl Harness {
    async fn start(
        hooks: Arc<TestHooks>,
        tools: Vec<GrokLocalTool>,
        output: Arc<dyn FrameProcessor>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let app = GrokRealtimeApp::new(config(&listener), vec![], tools, output)
            .unwrap()
            .set_test_hooks(hooks);
        let completion = app.completion();
        let (running, mut ws) =
            timeout(WAIT, async { tokio::join!(app.start(), ready(&listener)) })
                .await
                .unwrap();
        let running = running.unwrap();
        assert_eq!(read(&mut ws).await["type"], "session.update");
        quiet(&mut ws).await;
        Self {
            running,
            completion,
            ws,
            listener,
        }
    }
    async fn run(&mut self) {
        self.running.run().await.unwrap();
        assert_eq!(read(&mut self.ws).await["type"], "response.create");
    }
    async fn fresh_tool(&mut self, calls: &AtomicUsize, expected_calls: usize) {
        self.running
            .append_text("fresh authorized turn", true)
            .await
            .unwrap();
        let item = read(&mut self.ws).await;
        assert_eq!(item["item"]["content"][0]["text"], "fresh authorized turn");
        assert_eq!(read(&mut self.ws).await["type"], "response.create");
        response(&mut self.ws, "fresh-tool", "in_progress").await;
        declaration(&mut self.ws, "fresh-call", 2).await;
        response(&mut self.ws, "fresh-tool", "completed").await;
        let result = read(&mut self.ws).await;
        assert_eq!(result["item"]["type"], "function_call_output");
        assert_eq!(result["item"]["call_id"], "fresh-call");
        assert_eq!(result["item"]["output"], "{\"n\":2}");
        assert_eq!(read(&mut self.ws).await["type"], "response.create");
        speech(&mut self.ws, "fresh-speech").await;
        self.running
            .wait_for_speech_responses(1, WAIT)
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), expected_calls);
    }
    async fn finish(mut self) -> GrokAppReport {
        let (report, ()) = timeout(WAIT, async {
            tokio::join!(self.running.end(), closed(&mut self.ws, true))
        })
        .await
        .unwrap();
        let report = report.unwrap();
        assert!(report.successful(), "{report:?}");
        assert!(report.owners_joined());
        assert!(self.completion.wait().await.unwrap().owners_joined());
        drop(self.ws);
        released(self.listener).await;
        report
    }
}

#[tokio::test]
async fn same_app_retained_mapped_declaration_released_after_interrupt_never_enters_native_executor(
) {
    let hooks = Arc::new(TestHooks::default());
    hooks.declaration.arm();
    let calls = Arc::new(AtomicUsize::new(0));
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![counted(calls.clone())], output.clone()).await;
    h.run().await;
    response(&mut h.ws, "stale-mapped", "in_progress").await;
    declaration(&mut h.ws, "stale-call", 1).await;
    hooks.declaration.entered().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(h.running.outcomes().active.is_empty());
    assert_eq!(h.running.outcomes().total_terminal, 0);
    h.running.interrupt().await.unwrap();
    assert_eq!(read(&mut h.ws).await["type"], "response.cancel");
    response(&mut h.ws, "stale-mapped", "cancelled").await;
    hooks.declaration.release();
    hooks.declaration.resumed().await;
    // This hook retains the actual mapped frame in the real receive owner.
    // Release re-enters the real original-fence enqueue, not a fabricated frame.
    quiet(&mut h.ws).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(h.running.outcomes().total_terminal, 0);
    assert!(h.running.outcomes().active.is_empty());
    h.fresh_tool(&calls, 1).await;
    let completion = h.completion.clone();
    let report = h.finish().await;
    assert_eq!(report.mapped_tool_calls, 2);
    assert_eq!(report.settled_tool_calls, 1);
    let journal = completion.outcomes();
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(
        journal.terminal[0].outcome.call().tool_call_id,
        "fresh-call"
    );
    assert!(!completion
        .history()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| &message.tool_calls)
        .any(|call| call.tool_call_id == "stale-call"));
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_app_interrupt_after_native_entry_before_user_callback_returns_real_cancellation() {
    let hooks = Arc::new(TestHooks::default());
    hooks.callback.arm();
    let calls = Arc::new(AtomicUsize::new(0));
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![counted(calls.clone())], output.clone()).await;
    h.run().await;
    response(&mut h.ws, "entered-native", "in_progress").await;
    declaration(&mut h.ws, "entered-call", 1).await;
    hooks.callback.entered().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let entered = h.running.outcomes();
    assert_eq!(entered.active.len(), 1);
    assert_eq!(entered.active[0].tool_call_id, "entered-call");
    h.running.interrupt().await.unwrap();
    // Public application interruption invalidates the original provider source
    // AND the executor lane. This full-app test does not substitute for the
    // separate-source unit proof where the destination executor lane stays live.
    let mut cancelled_response = false;
    let mut cancelled_tool = false;
    for _ in 0..2 {
        let command = read(&mut h.ws).await;
        if command["type"] == "response.cancel" {
            assert!(!cancelled_response);
            cancelled_response = true;
            response(&mut h.ws, "entered-native", "cancelled").await;
        } else {
            assert_eq!(command["item"]["type"], "function_call_output");
            assert_eq!(command["item"]["call_id"], "entered-call");
            assert_eq!(command["item"]["output"], "CANCELLED");
            assert!(!cancelled_tool);
            cancelled_tool = true;
        }
    }
    assert!(cancelled_response && cancelled_tool);
    output.outcomes(1).await;
    hooks.callback.release();
    let journal = h.running.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(
        journal.terminal[0].outcome.state(),
        ToolCallTerminalState::Cancelled
    );
    assert_eq!(
        journal.terminal[0].outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    quiet(&mut h.ws).await;
    h.fresh_tool(&calls, 1).await;
    let completion = h.completion.clone();
    h.finish().await;
    assert_eq!(completion.outcomes().total_terminal, 2);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn same_app_end_waits_for_genuine_sealed_assistant_continuation_before_provider_enqueue() {
    let hooks = Arc::new(TestHooks::default());
    hooks.continuation.arm();
    let calls = Arc::new(AtomicUsize::new(0));
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![counted(calls.clone())], output.clone()).await;
    h.run().await;
    response(&mut h.ws, "sealed-tool", "in_progress").await;
    declaration(&mut h.ws, "sealed-call", 3).await;
    response(&mut h.ws, "sealed-tool", "completed").await;
    let result = read(&mut h.ws).await;
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["output"], "{\"n\":3}");
    hooks.continuation.entered().await;
    assert_eq!(h.running.outcomes().total_terminal, 1);
    assert!(h
        .running
        .history()
        .unwrap()
        .messages
        .iter()
        .any(|message| message.role == ChatRole::Tool
            && message.tool_call_id.as_deref() == Some("sealed-call")));
    let mut end = Box::pin(h.running.end());
    assert!(timeout(NEGATIVE, &mut end).await.is_err());
    quiet(&mut h.ws).await;
    hooks.continuation.release();
    hooks.continuation.resumed().await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    speech(&mut h.ws, "sealed-followup").await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut end, closed(&mut h.ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert!(report.owners_joined());
    drop(end);
    assert_eq!(report.generated_speech_responses, 1);
    assert_eq!(report.settled_tool_calls, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(*output.pcm.lock().unwrap(), [1, 2, 3, 4]);
    assert_eq!(
        h.completion
            .history()
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.content == "real assistant continuation")
            .count(),
        1
    );
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
    drop(h.ws);
    released(h.listener).await;
}

/// A synthetic playback boundary, not a physical playback assertion. The actual
/// OutputTransport generates all speaking frames observed by the private hook.
#[derive(Default)]
struct SpeakingBackend {
    played: AtomicUsize,
    stopped: AtomicUsize,
    changed: Notify,
}
#[async_trait]
impl OutputBackend for SpeakingBackend {
    async fn start(&self, format: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        assert_eq!(format.sample_rate(), 24_000);
        assert_eq!(format.channels(), 1);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let audio = match &frame.data {
            FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => audio,
            other => panic!("unexpected actual output payload: {other:?}"),
        };
        assert!(audio.audio.len() <= 480 && audio.audio.len().is_multiple_of(2));
        self.played.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn same_app_end_waits_for_actual_output_speaking_gate_before_assistant_tool_continuation() {
    let hooks = Arc::new(TestHooks::default());
    hooks.bot_stopped.arm();
    let callback_entered = Arc::new(Notify::new());
    let callback_release = Arc::new(Notify::new());
    let local = tool(Arc::new({
        let entered = callback_entered.clone();
        let release = callback_release.clone();
        move |call| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                Ok(call.arguments)
            })
        }
    }));
    let backend = Arc::new(SpeakingBackend::default());
    let output = Arc::new(
        OutputTransport::new(
            "SameAppRealSpeakingOutput",
            OutputTransportConfig {
                sample_rate: Some(24_000),
                chunk_duration: Duration::from_millis(10),
                queue_capacity: 16,
                queue_byte_capacity: 4096,
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: WAIT,
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let mut h = Harness::start(hooks.clone(), vec![local], output).await;
    h.run().await;
    response(&mut h.ws, "speaking-tool", "in_progress").await;
    pcm(&mut h.ws, "speaking-tool", &[1_u8, 0].repeat(240)).await;
    declaration(&mut h.ws, "speaking-call", 4).await;
    timeout(WAIT, callback_entered.notified()).await.unwrap();
    response(&mut h.ws, "speaking-tool", "completed").await;
    hooks.bot_stopped.entered().await;
    assert_eq!(backend.played.load(Ordering::SeqCst), 1);
    callback_release.notify_one();
    let result = read(&mut h.ws).await;
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["output"], "{\"n\":4}");
    let mut end = Box::pin(h.running.end());
    assert!(timeout(NEGATIVE, &mut end).await.is_err());
    quiet(&mut h.ws).await;
    // Real native settlement is complete, but actual BotStoppedSpeaking has
    // not reached the standard assistant. Tool output alone grants no inference.
    assert_eq!(h.running.outcomes().total_terminal, 1);
    hooks.bot_stopped.release();
    hooks.bot_stopped.resumed().await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    speech(&mut h.ws, "after-speaking-gate").await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut end, closed(&mut h.ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert!(report.owners_joined());
    drop(end);
    assert_eq!(backend.played.load(Ordering::SeqCst), 2);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    assert_eq!(report.settled_tool_calls, 1);
    assert_eq!(
        h.completion
            .history()
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.content == "real assistant continuation")
            .count(),
        1
    );
    drop(h.ws);
    released(h.listener).await;
}

struct HeldSetup {
    entered: Notify,
    release: Semaphore,
    cleaned: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for HeldSetup {
    fn name(&self) -> &str {
        "SameAppHeldOutputSetup"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        Ok(())
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
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn same_app_malformed_provider_after_readiness_during_held_output_setup_joins_every_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let output = Arc::new(HeldSetup {
        entered: Notify::new(),
        release: Semaphore::new(0),
        cleaned: AtomicUsize::new(0),
    });
    let app = GrokRealtimeApp::new(config(&listener), vec![], vec![], output.clone()).unwrap();
    let completion = app.completion();
    let mut starting = Box::pin(app.start());
    let mut ws = tokio::select! {
        result = &mut starting => panic!("startup completed while output setup must be held: {}", result.is_ok()),
        ws = ready(&listener) => ws,
    };
    timeout(WAIT, output.entered.notified()).await.unwrap();
    assert!(timeout(NEGATIVE, &mut starting).await.is_err());
    // The actual socket actor parses these bytes while the real output's setup
    // remains held, before normal pipeline Start can activate the receive owner.
    ws.send(Message::Text(
        "{malformed-after-ready-during-output-setup".into(),
    ))
    .await
    .unwrap();
    closed(&mut ws, false).await;
    output.release.add_permits(1);
    assert!(timeout(WAIT, &mut starting).await.unwrap().is_err());
    drop(starting);
    let report = timeout(WAIT, completion.wait()).await.unwrap().unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert!(report.primary.is_some());
    assert!(!report.forced_cleanup, "{report:?}");
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
    drop(ws);
    released(listener).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_app_held_actual_guard_rechecks_stale_source_while_destination_is_live() {
    let hooks = Arc::new(TestHooks::default());
    hooks.guard.arm();
    hooks.interruption.arm();
    let calls = Arc::new(AtomicUsize::new(0));
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![counted(calls.clone())], output.clone()).await;
    h.run().await;
    response(&mut h.ws, "held-guard", "in_progress").await;
    declaration(&mut h.ws, "held-guard-call", 1).await;
    hooks.guard.entered().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(h.running.outcomes().active.is_empty());
    assert_eq!(h.running.outcomes().total_terminal, 0);
    // The real provider consumes interruption and invalidates its original
    // response scope. Hold that same Interruption before forwarding it into
    // the executor, rather than assuming a queued System frame preserves the
    // destination's invocation scope.
    h.running.interrupt().await.unwrap();
    hooks.interruption.entered().await;
    assert_eq!(read(&mut h.ws).await["type"], "response.cancel");
    response(&mut h.ws, "held-guard", "cancelled").await;
    hooks.guard.release();
    hooks.guard.resumed().await;
    hooks.guard_rechecked().await;
    assert_eq!(hooks.guard_rechecks.load(Ordering::SeqCst), 1);
    assert!(
        hooks.guard_destination_current.load(Ordering::SeqCst),
        "destination lane was not live at actual guard recheck"
    );
    assert!(
        !hooks.guard_source_current.load(Ordering::SeqCst),
        "original source was not invalidated at actual guard recheck"
    );
    quiet(&mut h.ws).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert!(h.running.outcomes().active.is_empty());
    assert_eq!(
        h.running.outcomes().total_terminal,
        0,
        "pre-entry rejection fabricated native settlement"
    );
    assert!(output.outcomes.lock().unwrap().is_empty());
    hooks.interruption.release();
    hooks.interruption.resumed().await;
    output.interrupted().await;
    h.fresh_tool(&calls, 1).await;
    let completion = h.completion.clone();
    let report = h.finish().await;
    assert_eq!(report.mapped_tool_calls, 2);
    assert_eq!(report.settled_tool_calls, 1);
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(
        journal.terminal[0].outcome.call().tool_call_id,
        "fresh-call"
    );
    assert!(!completion
        .history()
        .unwrap()
        .messages
        .iter()
        .flat_map(|message| &message.tool_calls)
        .any(|call| call.tool_call_id == "held-guard-call"));
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn same_app_cancelled_end_before_input_lock_does_not_seal_or_abandon_accepted_input() {
    let hooks = Arc::new(TestHooks::default());
    hooks.input.arm();
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![], output.clone()).await;
    let input = [1_u8, 2, 3, 4];
    let mut append = Box::pin(h.running.append_pcm(&input));
    tokio::select! {
        result = &mut append => panic!("input escaped its held admission boundary: {result:?}"),
        _ = hooks.input.entered() => {}
    }
    // The input has reserved its bounded admission, but still owns the mutex
    // before the actual provider send. End must wait without publishing a seal.
    let mut abandoned_end = Box::pin(h.running.end());
    assert!(timeout(NEGATIVE, &mut abandoned_end).await.is_err());
    drop(abandoned_end);
    quiet(&mut h.ws).await;
    hooks.input.release();
    timeout(WAIT, &mut append).await.unwrap().unwrap();
    drop(append);
    hooks.input.resumed().await;
    let command = read(&mut h.ws).await;
    assert_eq!(command["type"], "input_audio_buffer.append");
    assert_eq!(
        STANDARD.decode(command["audio"].as_str().unwrap()).unwrap(),
        input
    );
    // No retry or app replacement: the same running owner must still admit a
    // fresh explicit run after the pre-linearization End waiter was cancelled.
    h.run().await;
    speech(&mut h.ws, "after-abandoned-end").await;
    h.running.wait_for_speech_responses(1, WAIT).await.unwrap();
    let report = h.finish().await;
    assert_eq!(report.generated_speech_responses, 1);
    assert_eq!(report.output_bytes, 4);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_app_start_waits_for_public_completion_observer_before_spawning_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hooks = Arc::new(TestHooks::default());
    hooks.completion_wait.arm();
    let output = Arc::new(Probe::default());
    let app = GrokRealtimeApp::new(config(&listener), vec![], vec![], output.clone())
        .unwrap()
        .set_test_hooks(hooks.clone());
    let completion = app.completion();
    let first_observer = completion.clone();
    let first_waiter = tokio::spawn(async move { first_observer.wait().await });
    hooks.completion_wait.entered().await;
    let mut starting = Box::pin(app.start());
    tokio::select! {
        result = &mut starting => panic!("startup escaped the held publication slot: {}", result.is_ok()),
        _ = listener.accept() => panic!("manager connected before its join slot was available"),
        _ = tokio::time::sleep(NEGATIVE) => {}
    }
    assert_eq!(output.setups.load(Ordering::SeqCst), 0);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.completion_wait.passes.load(Ordering::SeqCst), 0);
    first_waiter.abort();
    let stopped = timeout(WAIT, first_waiter).await.unwrap().unwrap_err();
    assert!(stopped.is_cancelled());
    // Releasing the public observer, not replacing the app, permits this exact
    // pending start to publish its owner and perform real setup and networking.
    let (running, mut ws) = timeout(WAIT, async {
        tokio::join!(&mut starting, ready(&listener))
    })
    .await
    .unwrap();
    let running = running.unwrap();
    drop(starting);
    assert_eq!(read(&mut ws).await["type"], "session.update");
    assert_eq!(output.setups.load(Ordering::SeqCst), 1);
    running.run().await.unwrap();
    assert_eq!(read(&mut ws).await["type"], "response.create");
    speech(&mut ws, "after-publication-observer").await;
    running.wait_for_speech_responses(1, WAIT).await.unwrap();
    let observer_a = completion.clone();
    let observer_b = completion.clone();
    let waiter_a = tokio::spawn(async move { observer_a.wait().await });
    let waiter_b = tokio::spawn(async move { observer_b.wait().await });
    let (cancelled, observed_a, observed_b, ()) = timeout(WAIT, async {
        tokio::join!(running.cancel(), waiter_a, waiter_b, closed(&mut ws, false))
    })
    .await
    .expect("published manager or retained completion observer was abandoned");
    for report in [
        cancelled.unwrap(),
        observed_a.unwrap().unwrap(),
        observed_b.unwrap().unwrap(),
    ] {
        assert!(!report.successful(), "{report:?}");
        assert_eq!(report.termination, GrokAppTermination::Cancelled);
        assert_eq!(report.primary, Some(GrokAppError::Cancelled));
        assert!(report.pipeline_joined && report.provider_joined && report.owners_joined());
        assert_eq!(report.generated_speech_responses, 1);
    }
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
    assert!(completion.wait().await.unwrap().owners_joined());
    drop(ws);
    released(listener).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_app_dropped_start_behind_public_completion_slot_never_creates_owner() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let hooks = Arc::new(TestHooks::default());
    hooks.completion_wait.arm();
    let output = Arc::new(Probe::default());
    let app = GrokRealtimeApp::new(config(&listener), vec![], vec![], output.clone())
        .unwrap()
        .set_test_hooks(hooks.clone());
    let completion = app.completion();
    let first_observer = completion.clone();
    let first_waiter = tokio::spawn(async move { first_observer.wait().await });
    hooks.completion_wait.entered().await;
    let mut starting = Box::pin(app.start());
    tokio::select! {
        result = &mut starting => panic!("startup escaped the held publication slot: {}", result.is_ok()),
        _ = listener.accept() => panic!("unpublished manager performed network effects"),
        _ = tokio::time::sleep(NEGATIVE) => {}
    }
    drop(starting);
    // Dropping start cancels the held test waiter, but cannot invent a manager
    // handle or a joined report. The original public observer remains awaitable.
    assert_eq!(
        timeout(WAIT, first_waiter)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        GrokAppError::Invalid("application not started")
    );
    assert_eq!(
        completion.wait().await.unwrap_err(),
        GrokAppError::Invalid("application not started")
    );
    assert_eq!(output.setups.load(Ordering::SeqCst), 0);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 0);
    assert_eq!(hooks.completion_wait.passes.load(Ordering::SeqCst), 0);
    assert!(completion.history().unwrap().messages.is_empty());
    assert!(completion.outcomes().active.is_empty());
    assert_eq!(completion.outcomes().total_terminal, 0);
    released(listener).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn same_app_cancel_while_real_vad_ack_is_pending_keeps_socket_control_live() {
    let hooks = Arc::new(TestHooks::default());
    hooks.interruption.arm();
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![], output.clone()).await;
    let initial_history = h.running.history().unwrap().messages;
    assert!(initial_history.is_empty());
    wire(
        &mut h.ws,
        json!({"type":"input_audio_buffer.speech_started","item_id":"held-vad-user","audio_start_ms":0}),
    )
    .await;
    hooks.interruption.entered().await;
    // This is the real user-generated Interruption, held inside the actual
    // provider after source expiry. Its System FIFO acknowledgment cannot yet
    // pass, but the independent socket actor must still service its control.
    assert_eq!(read(&mut h.ws).await["type"], "response.cancel");
    let events = [
        json!({"type":"input_audio_buffer.speech_stopped","item_id":"held-vad-user","audio_end_ms":20}),
        json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"held-vad-user","transcript":"held user transcript"}),
        json!({"type":"response.created","response":{"id":"held-vad-response","status":"in_progress","output":[]}}),
        json!({"type":"response.output_audio.delta","response_id":"held-vad-response","item_id":"held-vad-audio","output_index":0,"content_index":0,"delta":STANDARD.encode([1_u8, 2, 3, 4])}),
        json!({"type":"response.output_audio_transcript.delta","response_id":"held-vad-response","item_id":"held-vad-audio","delta":"held assistant transcript"}),
        json!({"type":"response.done","response":{"id":"held-vad-response","status":"completed","output":[]}}),
    ];
    timeout(WAIT, async {
        for (index, mut event) in events.into_iter().enumerate() {
            event["event_id"] = json!(format!("held-vad-burst-{index}"));
            h.ws.feed(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        h.ws.flush().await.unwrap();
    })
    .await
    .unwrap();
    assert_eq!(
        h.running.wait_for_speech_responses(1, NEGATIVE).await,
        Err(GrokAppError::Timeout("speech response"))
    );
    quiet(&mut h.ws).await;
    assert_eq!(output.response_frames.load(Ordering::SeqCst), 0);
    assert!(output.pcm.lock().unwrap().is_empty());
    assert_eq!(h.running.history().unwrap().messages, initial_history);
    assert_eq!(hooks.interruption.passes.load(Ordering::SeqCst), 0);
    // The unresolved real VAD boundary must not turn into successful End.
    // Dropping only this waiter leaves the manager's accepted End request live.
    let mut end = Box::pin(h.running.end());
    assert!(timeout(NEGATIVE, &mut end).await.is_err());
    drop(end);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(h.running.cancel(), closed(&mut h.ws, false))
    })
    .await
    .expect("pending VAD acknowledgment stranded cancellation or socket cleanup");
    let report = report.unwrap();
    assert!(!report.successful(), "{report:?}");
    assert_eq!(report.termination, GrokAppTermination::Cancelled);
    assert_eq!(report.primary, Some(GrokAppError::Cancelled));
    assert!(report.pipeline_joined && report.provider_joined && report.owners_joined());
    // Exactly conversation.created, session.updated, and speech_started reached
    // the real actor/mapper. No event from the queued response burst was read.
    assert_eq!(report.provider_events_emitted, 3, "{report:?}");
    assert_eq!(report.provider_events_mapped, 3, "{report:?}");
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    assert_eq!(report.mapped_tool_calls, 0);
    assert_eq!(output.response_frames.load(Ordering::SeqCst), 0);
    assert!(output.pcm.lock().unwrap().is_empty());
    assert_eq!(h.completion.history().unwrap().messages, initial_history);
    assert!(h.completion.outcomes().active.is_empty());
    assert_eq!(h.completion.outcomes().total_terminal, 0);
    assert_eq!(hooks.interruption.passes.load(Ordering::SeqCst), 0);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
    let retained = h.completion.wait().await.unwrap();
    assert!(retained.owners_joined() && !retained.successful());
    drop(h.ws);
    released(h.listener).await;
}

#[tokio::test]
async fn same_app_receive_owner_panic_still_joins_retained_socket_owner() {
    let hooks = Arc::new(TestHooks::default());
    let output = Arc::new(Probe::default());
    let mut h = Harness::start(hooks.clone(), vec![], output.clone()).await;
    hooks.receive_panic_armed.store(true, Ordering::SeqCst);
    // A real, valid post-start event enters the real receive task. Its panic
    // unwinds past session.shutdown, so only retained ownership can join it.
    wire(
        &mut h.ws,
        json!({"type":"session.updated","session":{"id":"receive-panic-session"}}),
    )
    .await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(h.completion.wait(), closed(&mut h.ws, false))
    })
    .await
    .expect("panic abandoned an application or socket owner");
    let report = report.unwrap();
    assert_eq!(hooks.receive_panics.load(Ordering::SeqCst), 1);
    assert!(!report.successful(), "{report:?}");
    assert_eq!(report.termination, GrokAppTermination::Failed);
    assert!(report.primary.is_some());
    assert_eq!(report.pipeline_failure, Some(GrokPipelineFailure::Panic));
    assert_eq!(
        report.pipeline_cleanup,
        GrokPipelineCleanup::UnknownAfterFailure
    );
    assert!(report.pipeline_joined && report.provider_joined && report.owners_joined());
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.mapped_tool_calls, 0);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
    assert!(h.completion.outcomes().active.is_empty());
    assert_eq!(h.completion.outcomes().total_terminal, 0);
    assert!(h.completion.wait().await.unwrap().owners_joined());
    drop(h.ws);
    released(h.listener).await;
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn same_app_cancels_during_real_pending_socket_write_and_joins_forced_cleanup() {
    const CHUNK: usize = 128 * 1024;
    const MESSAGES: usize = 256;
    const TOTAL: usize = CHUNK * MESSAGES;
    let socket = tokio::net::TcpSocket::new_v4().unwrap();
    // Linux inherits this explicit receive bound on accept. After readiness,
    // the peer keeps its socket open but stops consuming any incoming bytes.
    socket.set_recv_buffer_size(4096).unwrap();
    assert!(socket.recv_buffer_size().unwrap() <= 16 * 1024);
    socket.bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let listener = socket.listen(1).unwrap();
    let mut config = config(&listener);
    config.limits.max_input_bytes = TOTAL;
    config.limits.max_input_messages = MESSAGES;
    config.limits.max_audio_chunk_bytes = CHUNK;
    config.limits.shutdown_timeout = Duration::from_millis(250);
    config.connection.limits.max_audio_chunk_bytes = CHUNK;
    config.connection.limits.operation_timeout = Duration::from_secs(15);
    config.connection.limits.close_timeout = Duration::from_millis(100);
    let hooks = Arc::new(TestHooks::default());
    let output = Arc::new(Probe::default());
    let app = GrokRealtimeApp::new(config, vec![], vec![], output.clone())
        .unwrap()
        .set_test_hooks(hooks.clone());
    let completion = app.completion();
    let (running, mut ws) = timeout(WAIT, async { tokio::join!(app.start(), ready(&listener)) })
        .await
        .unwrap();
    let running = running.unwrap();
    assert_eq!(read(&mut ws).await["type"], "session.update");
    assert_eq!(hooks.audio_write.snapshot().started, 0);
    let pcm = vec![0_u8; CHUNK];
    let blocked_sequence = timeout(WAIT, async {
        for sequence in 1..=MESSAGES {
            running.append_pcm(&pcm).await.unwrap();
            // One admitted write at a time avoids conflating command/pipeline
            // capacity with a genuine pending socket.send future.
            hooks.audio_write.observed(sequence).await;
            if timeout(NEGATIVE, hooks.audio_write.finished(sequence))
                .await
                .is_err()
            {
                let observed = hooks.audio_write.snapshot();
                if observed.inflight == Some(sequence) && observed.pending {
                    return sequence;
                }
            }
        }
        panic!("finite 32 MiB input did not establish real socket backpressure");
    })
    .await
    .expect("bounded input failed to reach genuine socket backpressure");
    let before_cancel = hooks.audio_write.snapshot();
    assert_eq!(before_cancel.started, blocked_sequence);
    assert_eq!(before_cancel.completed + 1, blocked_sequence);
    assert_eq!(before_cancel.inflight, Some(blocked_sequence));
    assert!(before_cancel.pending && before_cancel.pending_polls > 0);
    // No await between the live Pending check and polling public cancel. The
    // current-thread runtime cannot complete the send in that interval.
    let mut cancel = Box::pin(running.cancel());
    assert!(futures_util::poll!(&mut cancel).is_pending());
    let after_cancel = hooks.audio_write.snapshot();
    assert_eq!(after_cancel.inflight, Some(blocked_sequence));
    assert!(after_cancel.pending);
    // Intentionally do not read the peer until completion: the real actor is
    // stuck in write, not an artificial gate. Its retained owner must abort
    // and await it within the configured shutdown budget, reporting failure.
    let report = timeout(WAIT, &mut cancel)
        .await
        .expect("cancel detached or abandoned the backpressured socket actor")
        .unwrap();
    drop(cancel);
    assert!(!report.successful(), "{report:?}");
    assert_eq!(report.termination, GrokAppTermination::Cancelled);
    assert_eq!(report.primary, Some(GrokAppError::Cancelled));
    assert!(report.forced_cleanup, "{report:?}");
    assert!(report.pipeline_joined && report.provider_joined && report.owners_joined());
    let finished = hooks.audio_write.snapshot();
    assert_eq!(finished.inflight, None);
    assert!(!finished.pending);
    assert_eq!(finished.completed, before_cancel.completed);
    assert_eq!(finished.dropped_pending, 1);
    assert_eq!(output.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    assert!(completion.outcomes().active.is_empty());
    assert_eq!(completion.outcomes().total_terminal, 0);
    let retained = completion.wait().await.unwrap();
    assert!(retained.owners_joined() && retained.forced_cleanup);
    // After the join, observe actual peer termination. Complete append frames
    // may precede an EOF/reset or an incomplete final write; none imply remote
    // acceptance. Bound both decoded bytes and command count while draining.
    timeout(WAIT, async {
        let mut commands = 0;
        let mut bytes = 0;
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    commands += 1;
                    assert!(commands <= blocked_sequence);
                    assert!(text.len() < CHUNK * 2);
                    let command: Value = serde_json::from_str(text.as_str()).unwrap();
                    assert_eq!(command["type"], "input_audio_buffer.append");
                    let audio = STANDARD.decode(command["audio"].as_str().unwrap()).unwrap();
                    assert_eq!(audio.len(), CHUNK);
                    assert!(audio.iter().all(|byte| *byte == 0));
                    bytes += audio.len();
                    assert!(bytes <= TOTAL);
                }
                Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                _ => panic!("unexpected non-PCM command after stalled write cancellation"),
            }
        }
    })
    .await
    .expect("joined socket actor left the real peer open");
    drop(ws);
    released(listener).await;
}
