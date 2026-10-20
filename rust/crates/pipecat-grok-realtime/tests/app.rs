//! The public, final-only application against real loopback WebSocket peers.
//!
//! Every case constructs the same production app as `grok_pcm`. The callback
//! runtime, immutable native receipts, relay and paired aggregators are real.
//! The bounded output below records generated PCM, not playback or user hearing.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_grok_realtime::*;
use pipecat_media_transport::{
    OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt,
};
use pipecat_processors::tools::RegisteredTool;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering},
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
const KEY: &str = "synthetic-public-app-key-never-live";
const PCM: &[u8] = &[1, 2, 3, 4];
type Ws = WebSocketStream<TcpStream>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mark {
    ResponseStart,
    SpeechStart,
    Audio,
    SpeechStop,
    ResponseEnd,
    Outcome,
    End,
    Cancel,
}
#[derive(Default, Clone)]
struct Captured {
    pcm: Vec<u8>,
    marks: Vec<Mark>,
    outcomes: Vec<Arc<NativeToolCallOutcome>>,
    errors: Vec<String>,
}
/// This is a trusted bounded output integration, not an alternate aggregator.
/// In particular, all private flush/bracket frames are forwarded unchanged.
struct RecordingOutput {
    captured: Mutex<Captured>,
    changed: Notify,
    setup_calls: AtomicUsize,
    cleanup_calls: AtomicUsize,
    hold: AtomicU8,
    held: Notify,
    release: Notify,
    failure: AtomicU8,
}
impl Default for RecordingOutput {
    fn default() -> Self {
        Self {
            captured: Mutex::new(Captured::default()),
            changed: Notify::new(),
            setup_calls: AtomicUsize::new(0),
            cleanup_calls: AtomicUsize::new(0),
            hold: AtomicU8::new(0),
            held: Notify::new(),
            release: Notify::new(),
            failure: AtomicU8::new(0),
        }
    }
}
impl RecordingOutput {
    fn snapshot(&self) -> Captured {
        self.captured.lock().unwrap().clone()
    }
    async fn observed(&self, wanted: impl Fn(&Captured) -> bool) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if wanted(&self.snapshot()) {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("real output did not observe the expected frame");
    }
    async fn held(&self) {
        timeout(WAIT, self.held.notified())
            .await
            .expect("real output did not reach its held boundary");
    }
    fn assert_cleaned(&self) {
        assert_eq!(self.setup_calls.load(Ordering::SeqCst), 1);
        assert_eq!(self.cleanup_calls.load(Ordering::SeqCst), 1);
    }
}
#[async_trait]
impl FrameProcessor for RecordingOutput {
    fn name(&self) -> &str {
        "BoundedAppRecordingOutput"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.setup_calls.fetch_add(1, Ordering::SeqCst);
        if self.failure.load(Ordering::SeqCst) == 1 {
            return Err(ProcessorError::fatal("synthetic output setup failure"));
        }
        if self
            .hold
            .compare_exchange(3, 0, Ordering::SeqCst, Ordering::SeqCst)
            .is_ok()
        {
            self.held.notify_one();
            self.release.notified().await;
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let outcome = frame.native_tool_outcome()?.cloned();
        let audio = matches!(
            frame.data,
            FrameData::OutputAudio(_) | FrameData::TtsAudio(_)
        );
        if direction == Direction::Downstream {
            if audio && matches!(self.failure.load(Ordering::SeqCst), 2 | 4) {
                return Err(ProcessorError::fatal("synthetic output process failure"));
            }
            {
                let mut captured = self.captured.lock().unwrap();
                let mark = match &frame.data {
                    FrameData::LlmResponseStart => Some(Mark::ResponseStart),
                    FrameData::TtsStarted(_) => Some(Mark::SpeechStart),
                    FrameData::OutputAudio(audio) | FrameData::TtsAudio(audio) => {
                        assert_eq!(audio.sample_rate, 24_000);
                        assert_eq!(audio.num_channels, 1);
                        assert!(audio.audio.len().is_multiple_of(2));
                        if captured.pcm.len().saturating_add(audio.audio.len()) > 4096 {
                            return Err(ProcessorError::fatal("fixture recording byte limit"));
                        }
                        captured.pcm.extend_from_slice(&audio.audio);
                        Some(Mark::Audio)
                    }
                    FrameData::TtsStopped(_) => Some(Mark::SpeechStop),
                    FrameData::LlmResponseEnd => Some(Mark::ResponseEnd),
                    FrameData::End(_) => Some(Mark::End),
                    FrameData::Cancel(_) | FrameData::CancelDetailed(_) => Some(Mark::Cancel),
                    _ if outcome.is_some() => Some(Mark::Outcome),
                    _ => None,
                };
                if let Some(mark) = mark {
                    if captured.marks.len() >= 128 {
                        return Err(ProcessorError::fatal("fixture recording event limit"));
                    }
                    captured.marks.push(mark);
                }
                if let FrameData::FunctionCallResult(result) = &frame.data {
                    if let Some(error) = &result.error {
                        if error.len() > 128
                            || error.capacity() > 256
                            || captured.errors.len() >= 16
                        {
                            return Err(ProcessorError::fatal(
                                "fixture callback error retention limit",
                            ));
                        }
                        captured.errors.push(error.clone());
                    }
                }
                if let Some(outcome) = &outcome {
                    if captured.outcomes.len() >= 16 {
                        return Err(ProcessorError::fatal("fixture native outcome limit"));
                    }
                    captured.outcomes.push(outcome.clone());
                }
            }
            self.changed.notify_one();
            let wanted_hold = if outcome.is_some() {
                1
            } else if audio {
                2
            } else {
                0
            };
            if wanted_hold != 0
                && self
                    .hold
                    .compare_exchange(wanted_hold, 0, Ordering::SeqCst, Ordering::SeqCst)
                    .is_ok()
            {
                self.held.notify_one();
                self.release.notified().await;
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleanup_calls.fetch_add(1, Ordering::SeqCst);
        if matches!(self.failure.load(Ordering::SeqCst), 3 | 4) {
            return Err(ProcessorError::fatal("synthetic output cleanup failure"));
        }
        Ok(())
    }
}

async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
fn config(address: SocketAddr) -> GrokAppConfig {
    let mut config = GrokAppConfig::loopback(KEY, address);
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
fn schema() -> FunctionTool {
    FunctionTool::new(
        "echo",
        "Return the supplied synthetic value",
        json!({"type":"object","properties":{"n":{"type":"integer"}},"additionalProperties":false,"required":["n"]}),
    )
}
fn tool(callback: pipecat_processors::tools::ToolCallback) -> GrokLocalTool {
    let mut handler = RegisteredTool::new(callback);
    handler.timeout = Duration::from_secs(4);
    GrokLocalTool {
        schema: schema(),
        handler,
    }
}
fn echo() -> GrokLocalTool {
    tool(Arc::new(|call| Box::pin(async move { Ok(call.arguments) })))
}
async fn wire(ws: &mut Ws, mut value: Value) {
    static EVENT: AtomicU64 = AtomicU64::new(1);
    value["event_id"] = json!(format!(
        "app-event-{}",
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
        "expected a provider command, got {message:?}"
    );
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(NEGATIVE, ws.next()).await.is_err(),
        "unexpected provider command or premature socket close"
    );
}
#[allow(clippy::result_large_err)] // tungstenite fixes the callback's error type.
async fn accept(listener: &TcpListener, key: &str) -> Ws {
    let (tcp, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let address = listener.local_addr().unwrap();
    timeout(
        WAIT,
        tokio_tungstenite::accept_hdr_async(
            tcp,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(request.headers()["authorization"], format!("Bearer {key}"));
                assert_eq!(request.headers()["host"], address.to_string());
                assert_eq!(request.uri().path(), "/v1/realtime");
                assert_eq!(request.uri().query(), Some("model=grok-voice-latest"));
                Ok(response)
            },
        ),
    )
    .await
    .unwrap()
    .unwrap()
}
async fn ready(listener: &TcpListener, key: &str) -> Ws {
    let mut ws = accept(listener, key).await;
    wire(
        &mut ws,
        json!({"type":"conversation.created","conversation":{"id":"app-conversation"}}),
    )
    .await;
    let update = read(&mut ws).await;
    assert_eq!(update["type"], "session.update");
    assert_eq!(update["session"]["turn_detection"]["type"], "server_vad");
    assert_eq!(
        update["session"]["audio"]["input"]["format"]["rate"],
        24_000
    );
    assert_eq!(
        update["session"]["audio"]["output"]["format"]["rate"],
        24_000
    );
    wire(
        &mut ws,
        json!({"type":"session.updated","session":{"id":"app-session"}}),
    )
    .await;
    ws
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    wire(ws, json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
async fn call(ws: &mut Ws, response_id: &str, call_id: &str, n: u64) {
    response(ws, response_id, false).await;
    wire(ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":format!("item-{call_id}"),"call_id":call_id,"name":"echo","arguments":""}})).await;
    wire(ws, json!({"type":"response.function_call_arguments.done","call_id":call_id,"name":"echo","arguments":json!({"n":n}).to_string()})).await;
}
async fn audio(ws: &mut Ws, id: &str, bytes: &[u8]) {
    wire(ws, json!({"type":"response.output_audio.delta","response_id":id,"item_id":format!("speech-{id}"),"output_index":0,"content_index":0,"delta":STANDARD.encode(bytes)})).await;
}
async fn speech(ws: &mut Ws, id: &str, text: &str) {
    response(ws, id, false).await;
    // Individual wire deltas split samples. The real mapper must emit only
    // aligned PCM, preserve every byte, and refuse a final partial sample.
    audio(ws, id, &PCM[..1]).await;
    audio(ws, id, &PCM[1..3]).await;
    audio(ws, id, &PCM[3..]).await;
    wire(ws, json!({"type":"response.output_audio_transcript.delta","response_id":id,"item_id":format!("speech-{id}"),"delta":text})).await;
    wire(ws, json!({"type":"response.output_audio.done","response_id":id,"item_id":format!("speech-{id}"),"output_index":0,"content_index":0})).await;
    response(ws, id, true).await;
}
async fn closed(ws: &mut Ws, clean: bool) {
    timeout(WAIT, async {
        loop {
            match ws.next().await {
                Some(Ok(Message::Close(_))) => {
                    let _ = ws.flush().await;
                    return;
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                None | Some(Err(_)) if !clean => return,
                other => panic!("unexpected terminal provider command: {other:?}"),
            }
        }
    })
    .await
    .expect("actual application socket was not closed");
}
async fn released(listener: TcpListener) {
    assert!(
        timeout(NEGATIVE, listener.accept()).await.is_err(),
        "application reconnected unexpectedly"
    );
    let address = listener.local_addr().unwrap();
    drop(listener);
    drop(
        TcpListener::bind(address)
            .await
            .expect("fixture listener was not released"),
    );
}

struct Fixture {
    running: GrokRunningApp,
    completion: GrokAppCompletion,
    output: Arc<RecordingOutput>,
    ws: Ws,
    listener: TcpListener,
    startup: Vec<Value>,
}
impl Fixture {
    async fn new(tools: Vec<GrokLocalTool>) -> Self {
        let listener = listener().await;
        let config = config(listener.local_addr().unwrap());
        Self::with(
            listener,
            config,
            vec![],
            tools,
            Arc::new(RecordingOutput::default()),
        )
        .await
    }
    async fn with(
        listener: TcpListener,
        config: GrokAppConfig,
        seed: Vec<ChatMessage>,
        tools: Vec<GrokLocalTool>,
        output: Arc<RecordingOutput>,
    ) -> Self {
        Self::with_key(listener, config, seed, tools, output, KEY).await
    }
    async fn with_key(
        listener: TcpListener,
        config: GrokAppConfig,
        seed: Vec<ChatMessage>,
        tools: Vec<GrokLocalTool>,
        output: Arc<RecordingOutput>,
        key: &str,
    ) -> Self {
        let app = GrokRealtimeApp::new(config, seed, tools, output.clone()).unwrap();
        let completion = app.completion();
        let (running, ws) = timeout(WAIT, async {
            tokio::join!(app.start(), ready(&listener, key))
        })
        .await
        .unwrap();
        let mut fixture = Self {
            running: running.unwrap(),
            completion,
            output,
            ws,
            listener,
            startup: vec![],
        };
        fixture.startup_without_inference().await;
        fixture
    }
    async fn startup_without_inference(&mut self) {
        // A single source-backed context seed may produce one additional
        // session update and one packed ordinary history item. Neither grants
        // inference. This negative assertion is a test, not an app drain timer.
        let mut updates = 0;
        let mut items = 0;
        loop {
            let next = timeout(NEGATIVE, self.ws.next()).await;
            let Ok(next) = next else {
                break;
            };
            let message = next.unwrap().unwrap();
            assert!(message.is_text());
            let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
            match value["type"].as_str() {
                Some("session.update") => {
                    updates += 1;
                    assert!(updates <= 1);
                }
                Some("conversation.item.create") => {
                    items += 1;
                    assert!(items <= 1);
                    assert_eq!(value["item"]["type"], "message");
                    assert_eq!(value["item"]["role"], "user");
                }
                _ => panic!("startup unexpectedly inferred or emitted a tool: {value}"),
            }
            self.startup.push(value);
        }
    }
    async fn run(&mut self) {
        self.running.run().await.unwrap();
        assert_eq!(read(&mut self.ws).await["type"], "response.create");
    }
    async fn finish(mut self) -> GrokAppReport {
        let (report, ()) = timeout(WAIT, async {
            tokio::join!(self.running.end(), closed(&mut self.ws, true))
        })
        .await
        .unwrap();
        let report = report.unwrap();
        assert!(report.successful(), "{report:?}");
        assert!(report.owners_joined(), "{report:?}");
        let observed = timeout(WAIT, self.completion.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(observed.successful());
        assert!(observed.owners_joined());
        self.output.assert_cleaned();
        drop(self.ws);
        released(self.listener).await;
        report
    }
    async fn cancel(mut self) -> GrokAppReport {
        let (_, ()) = timeout(WAIT, async {
            tokio::join!(self.running.cancel(), closed(&mut self.ws, false))
        })
        .await
        .unwrap();
        let report = timeout(WAIT, self.completion.wait())
            .await
            .unwrap()
            .unwrap();
        assert!(!report.successful(), "{report:?}");
        assert!(report.owners_joined(), "{report:?}");
        self.output.assert_cleaned();
        drop(self.ws);
        released(self.listener).await;
        report
    }
}

#[tokio::test]
async fn end_waits_for_held_native_callback_then_independent_continuation_and_pcm() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let local = tool(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        move |call| {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.notify_one();
                release.notified().await;
                Ok(call.arguments)
            })
        }
    }));
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    call(&mut fixture.ws, "held-tool", "held-call", 7).await;
    timeout(WAIT, entered.notified()).await.unwrap();
    response(&mut fixture.ws, "held-tool", true).await;
    fixture
        .output
        .observed(|captured| captured.marks.contains(&Mark::ResponseEnd))
        .await;
    let mut ending = Box::pin(fixture.running.end());
    assert!(
        timeout(NEGATIVE, &mut ending).await.is_err(),
        "response.done incorrectly completed End with an entered callback"
    );
    quiet(&mut fixture.ws).await;
    assert!(fixture.output.snapshot().outcomes.is_empty());
    release.notify_one();
    let result = read(&mut fixture.ws).await;
    assert_eq!(result["type"], "conversation.item.create");
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["call_id"], "held-call");
    assert_eq!(result["item"]["output"], "{\"n\":7}");
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    speech(
        &mut fixture.ws,
        "actual-continuation",
        "generated after final tool",
    )
    .await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert!(report.owners_joined());
    drop(ending);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    let captured = fixture.output.snapshot();
    assert_eq!(captured.pcm, PCM);
    assert_eq!(captured.outcomes.len(), 1);
    assert_eq!(
        captured.outcomes[0].reason(),
        ToolCallEndReason::CallbackReturned
    );
    assert_eq!(
        captured.outcomes[0].state(),
        ToolCallTerminalState::Completed
    );
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(journal.terminal.len(), 1);
    assert_eq!(journal.terminal[0].outcome.id(), captured.outcomes[0].id());
    let history = fixture.completion.history().unwrap();
    assert_eq!(history.messages.len(), 3);
    assert_eq!(history.messages[0].tool_calls.len(), 1);
    assert_eq!(history.messages[0].tool_calls[0].tool_call_id, "held-call");
    assert_eq!(history.messages[1].role, ChatRole::Tool);
    assert_eq!(
        history.messages[1].tool_call_id.as_deref(),
        Some("held-call")
    );
    assert_eq!(history.messages[1].content, "{\"n\":7}");
    assert_eq!(
        history.messages[2],
        ChatMessage::new(ChatRole::Assistant, "generated after final tool")
    );
    assert_eq!(
        captured
            .marks
            .iter()
            .filter(|&&mark| mark == Mark::ResponseStart)
            .count(),
        2
    );
    assert_eq!(
        captured
            .marks
            .iter()
            .filter(|&&mark| mark == Mark::ResponseEnd)
            .count(),
        2
    );
    fixture.output.assert_cleaned();
    assert!(fixture.completion.wait().await.unwrap().successful());
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn end_waits_after_wire_tool_output_for_real_assistant_receipt_and_scheduling() {
    let output = Arc::new(RecordingOutput::default());
    output.hold.store(1, Ordering::SeqCst);
    let listener = listener().await;
    let config = config(listener.local_addr().unwrap());
    let mut fixture = Fixture::with(listener, config, vec![], vec![echo()], output).await;
    fixture.run().await;
    call(&mut fixture.ws, "receipt-tool", "receipt-call", 9).await;
    response(&mut fixture.ws, "receipt-tool", true).await;
    fixture.output.held().await;
    let result = read(&mut fixture.ws).await;
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["output"], "{\"n\":9}");
    let mut ending = Box::pin(fixture.running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    // The actor has delivered and removed this call, but the actual assistant
    // has not seen the immutable native receipt. No inference is authorized.
    quiet(&mut fixture.ws).await;
    fixture.output.release.notify_one();
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    speech(
        &mut fixture.ws,
        "receipt-continuation",
        "assistant independently scheduled",
    )
    .await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, true))
    })
    .await
    .unwrap();
    assert!(report.as_ref().unwrap().successful(), "{report:?}");
    assert!(report.unwrap().owners_joined());
    drop(ending);
    assert_eq!(fixture.output.snapshot().pcm, PCM);
    assert_eq!(fixture.output.snapshot().outcomes.len(), 1);
    let history = fixture.completion.history().unwrap();
    assert_eq!(history.messages.len(), 3);
    assert_eq!(history.messages[1].role, ChatRole::Tool);
    assert_eq!(
        history.messages[1].tool_call_id.as_deref(),
        Some("receipt-call")
    );
    assert_eq!(
        history.messages[2].content,
        "assistant independently scheduled"
    );
    assert_eq!(fixture.completion.outcomes().total_terminal, 1);
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn native_null_final_settles_without_manufacturing_continuation() {
    let local = tool(Arc::new(|_| Box::pin(async { Ok(Value::Null) })));
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    call(&mut fixture.ws, "null-response", "null-call", 0).await;
    let result = read(&mut fixture.ws).await;
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["output"], "COMPLETED");
    response(&mut fixture.ws, "null-response", true).await;
    fixture
        .output
        .observed(|captured| {
            captured.marks.contains(&Mark::ResponseEnd) && captured.outcomes.len() == 1
        })
        .await;
    quiet(&mut fixture.ws).await;
    assert_eq!(
        fixture.output.snapshot().outcomes[0].reason(),
        ToolCallEndReason::CallbackReturned
    );
    let report = fixture.finish().await;
    assert_eq!(report.mapped_tool_calls, 1);
    assert_eq!(report.settled_tool_calls, 1);
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
}

#[tokio::test]
async fn tool_only_response_cannot_satisfy_required_generated_speech() {
    let local = tool(Arc::new(|_| Box::pin(async { Ok(Value::Null) })));
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    call(&mut fixture.ws, "no-speech", "no-speech-call", 0).await;
    assert_eq!(
        read(&mut fixture.ws).await["item"]["type"],
        "function_call_output"
    );
    response(&mut fixture.ws, "no-speech", true).await;
    let result = fixture.running.wait_for_speech_responses(1, NEGATIVE).await;
    assert!(matches!(result, Err(GrokAppError::Timeout(_))));
    let report = fixture.cancel().await;
    assert_eq!(report.generated_speech_responses, 0);
}

#[tokio::test]
async fn accepted_pcm_and_text_reach_provider_before_immediate_end() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture.running.append_pcm(PCM).await.unwrap();
    fixture
        .running
        .append_text("last accepted input", false)
        .await
        .unwrap();
    let mut ending = Box::pin(fixture.running.end());
    let provider = async {
        let append = read(&mut fixture.ws).await;
        assert_eq!(append["type"], "input_audio_buffer.append");
        assert_eq!(
            STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
            PCM
        );
        let item = read(&mut fixture.ws).await;
        assert_eq!(item["type"], "conversation.item.create");
        assert_eq!(item["item"]["content"][0]["text"], "last accepted input");
        closed(&mut fixture.ws, true).await;
    };
    let (report, ()) = timeout(WAIT, async { tokio::join!(&mut ending, provider) })
        .await
        .unwrap();
    assert!(report.as_ref().unwrap().successful(), "{report:?}");
    assert!(report.unwrap().owners_joined());
    drop(ending);
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn accepted_run_before_end_waits_for_actual_generated_output() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture.running.run().await.unwrap();
    let mut ending = Box::pin(fixture.running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    speech(&mut fixture.ws, "accepted-run", "accepted run completed").await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert_eq!(report.generated_speech_responses, 1);
    assert_eq!(report.output_bytes, PCM.len());
    drop(ending);
    assert_eq!(fixture.output.snapshot().pcm, PCM);
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn held_output_prevents_end_and_late_public_inputs_are_rejected() {
    let listener = listener().await;
    let config = config(listener.local_addr().unwrap());
    let output = Arc::new(RecordingOutput::default());
    output.hold.store(2, Ordering::SeqCst);
    let mut fixture = Fixture::with(listener, config, vec![], vec![], output).await;
    fixture.run().await;
    speech(&mut fixture.ws, "held-output", "generated not played").await;
    fixture.output.held().await;
    let mut ending = Box::pin(fixture.running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    assert_eq!(
        fixture.running.append_pcm(PCM).await,
        Err(GrokAppError::Closed)
    );
    assert_eq!(
        fixture.running.append_text("late", true).await,
        Err(GrokAppError::Closed)
    );
    assert_eq!(fixture.running.run().await, Err(GrokAppError::Closed));
    quiet(&mut fixture.ws).await;
    fixture.output.release.notify_one();
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, true))
    })
    .await
    .unwrap();
    assert!(report.as_ref().unwrap().successful(), "{report:?}");
    assert!(report.unwrap().owners_joined());
    drop(ending);
    assert_eq!(fixture.output.snapshot().pcm, PCM);
    let marks = fixture.output.snapshot().marks;
    let position = |mark| marks.iter().position(|value| *value == mark).unwrap();
    assert!(position(Mark::SpeechStart) < position(Mark::Audio));
    assert!(position(Mark::Audio) < position(Mark::SpeechStop));
    assert!(position(Mark::SpeechStop) < position(Mark::End));
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn two_instances_keep_credentials_history_tools_and_output_independent() {
    let first_listener = listener().await;
    let first_config = config(first_listener.local_addr().unwrap());
    let first_seed = vec![
        ChatMessage::new(ChatRole::System, "first system"),
        ChatMessage::new(ChatRole::User, "first seed"),
    ];
    let mut first = Fixture::with(
        first_listener,
        first_config,
        first_seed,
        vec![echo()],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    let second_listener = listener().await;
    let second_address = second_listener.local_addr().unwrap();
    let mut second_config = config(second_address);
    second_config.connection = RealtimeConfig::new("synthetic-independent-second-key");
    second_config.connection.endpoint = format!("ws://{second_address}/v1/realtime");
    // Retain the validated finite connection profile, changing only identity.
    let baseline = config(second_address);
    second_config.connection.limits = baseline.connection.limits;
    if let Some(audio) = second_config.connection.session.audio.as_mut() {
        audio.input.as_mut().unwrap().transport = Some(AudioTransport::Json);
        audio.output.as_mut().unwrap().transport = Some(AudioTransport::Json);
    }
    let second_seed = vec![
        ChatMessage::new(ChatRole::System, "second system"),
        ChatMessage::new(ChatRole::User, "second seed"),
    ];
    let mut second = Fixture::with_key(
        second_listener,
        second_config,
        second_seed,
        vec![],
        Arc::new(RecordingOutput::default()),
        "synthetic-independent-second-key",
    )
    .await;
    let initial = |fixture: &Fixture| {
        let update = fixture
            .startup
            .iter()
            .find(|value| value["type"] == "session.update")
            .unwrap();
        let item = fixture
            .startup
            .iter()
            .find(|value| value["type"] == "conversation.item.create")
            .unwrap();
        (update.clone(), item.clone())
    };
    let (first_update, first_item) = initial(&first);
    let (second_update, second_item) = initial(&second);
    assert_eq!(first_update["session"]["instructions"], "first system");
    assert_eq!(second_update["session"]["instructions"], "second system");
    assert_eq!(first_item["item"]["content"][0]["text"], "first seed");
    assert_eq!(second_item["item"]["content"][0]["text"], "second seed");
    for turn in 0..2 {
        first
            .running
            .append_text("same legitimate text", true)
            .await
            .unwrap();
        let item = read(&mut first.ws).await;
        assert_eq!(item["item"]["content"][0]["text"], "same legitimate text");
        assert_eq!(read(&mut first.ws).await["type"], "response.create");
        speech(
            &mut first.ws,
            &format!("first-{turn}"),
            "first generated answer",
        )
        .await;
        first
            .running
            .wait_for_speech_responses(turn + 1, WAIT)
            .await
            .unwrap();
        quiet(&mut second.ws).await;
    }
    assert!(second.output.snapshot().pcm.is_empty());
    second.run().await;
    speech(&mut second.ws, "second-only", "second generated answer").await;
    second
        .running
        .wait_for_speech_responses(1, WAIT)
        .await
        .unwrap();
    let first_output = first.output.clone();
    let second_output = second.output.clone();
    let first_completion = first.completion.clone();
    let second_completion = second.completion.clone();
    let first_report = first.finish().await;
    assert_eq!(first_report.generated_speech_responses, 2);
    quiet(&mut second.ws).await;
    let second_report = second.finish().await;
    assert_eq!(second_report.generated_speech_responses, 1);
    assert_eq!(first_output.snapshot().pcm, PCM.repeat(2));
    assert_eq!(second_output.snapshot().pcm, PCM);
    let mut expected_first = vec![
        ChatMessage::new(ChatRole::System, "first system"),
        ChatMessage::new(ChatRole::User, "first seed"),
    ];
    for _ in 0..2 {
        expected_first.push(ChatMessage::new(ChatRole::User, "same legitimate text"));
        expected_first.push(ChatMessage::new(
            ChatRole::Assistant,
            "first generated answer",
        ));
    }
    assert_eq!(first_completion.history().unwrap().messages, expected_first);
    assert_eq!(
        second_completion.history().unwrap().messages,
        vec![
            ChatMessage::new(ChatRole::System, "second system"),
            ChatMessage::new(ChatRole::User, "second seed"),
            ChatMessage::new(ChatRole::Assistant, "second generated answer")
        ]
    );
    assert_eq!(first_completion.outcomes().total_terminal, 0);
    assert_eq!(second_completion.outcomes().total_terminal, 0);
    let mut detached = first_completion.history().unwrap();
    detached.messages.clear();
    assert_eq!(first_completion.history().unwrap().messages.len(), 6);
}

#[tokio::test]
async fn dropped_and_concurrent_completion_waiters_do_not_take_join_ownership() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture.run().await;
    let completion = fixture.completion.clone();
    let mut cancelled_waiter = Box::pin(completion.wait());
    assert!(timeout(NEGATIVE, &mut cancelled_waiter).await.is_err());
    drop(cancelled_waiter);
    let first = fixture.completion.clone();
    let second = fixture.completion.clone();
    speech(
        &mut fixture.ws,
        "waiter-response",
        "waiters share actual completion",
    )
    .await;
    let (end, first, second, ()) = timeout(WAIT, async {
        tokio::join!(
            fixture.running.end(),
            first.wait(),
            second.wait(),
            closed(&mut fixture.ws, true)
        )
    })
    .await
    .unwrap();
    for report in [end.unwrap(), first.unwrap(), second.unwrap()] {
        assert!(report.successful(), "{report:?}");
        assert!(report.owners_joined());
    }
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

struct CallbackDropped(Arc<AtomicUsize>);
impl Drop for CallbackDropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn cancel_during_entered_callback_joins_callback_pipeline_and_actual_socket() {
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicUsize::new(0));
    let local = tool(Arc::new({
        let entered = entered.clone();
        let dropped = dropped.clone();
        move |_| {
            let entered = entered.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _drop = CallbackDropped(dropped);
                entered.notify_one();
                std::future::pending::<()>().await;
                Ok(Value::Null)
            })
        }
    }));
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    call(&mut fixture.ws, "cancel-tool", "cancel-call", 3).await;
    timeout(WAIT, entered.notified()).await.unwrap();
    response(&mut fixture.ws, "cancel-tool", true).await;
    let report = fixture.cancel().await;
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(!report.successful());
}

#[tokio::test]
async fn dropping_running_handle_keeps_independent_completion_and_closes_peer() {
    let Fixture {
        running,
        completion,
        output,
        mut ws,
        listener,
        ..
    } = Fixture::new(vec![]).await;
    drop(running);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(completion.wait(), closed(&mut ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    output.assert_cleaned();
    drop(ws);
    released(listener).await;
}

#[tokio::test]
async fn cancelled_start_future_during_websocket_handshake_keeps_cleanup_owner() {
    use tokio::io::AsyncReadExt;
    let listener = listener().await;
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        Arc::new(RecordingOutput::default()),
    )
    .unwrap();
    let completion = app.completion();
    let mut starting = Box::pin(app.start());
    let (mut tcp, _) = tokio::select! {
        result = &mut starting => panic!("startup completed before peer handshake: {}", result.is_ok()),
        accepted = listener.accept() => accepted.unwrap(),
    };
    let mut request = [0; 4096];
    let count = timeout(WAIT, tcp.read(&mut request))
        .await
        .unwrap()
        .unwrap();
    assert!(
        count > 0,
        "fixture did not observe the actual WebSocket request"
    );
    drop(starting);
    let peer = async {
        loop {
            match tcp.read(&mut request).await {
                Ok(0) | Err(_) => break,
                Ok(_) => {}
            }
        }
    };
    let (report, ()) = timeout(WAIT, async { tokio::join!(completion.wait(), peer) })
        .await
        .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    drop(tcp);
    released(listener).await;
}

#[tokio::test]
async fn cancelled_start_future_during_readiness_closes_upgraded_socket() {
    let listener = listener().await;
    let output = Arc::new(RecordingOutput::default());
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        output,
    )
    .unwrap();
    let completion = app.completion();
    let mut starting = Box::pin(app.start());
    let mut ws = tokio::select! {
        result = &mut starting => panic!("startup completed before readiness: {}", result.is_ok()),
        ws = accept(&listener, KEY) => ws,
    };
    wire(
        &mut ws,
        json!({"type":"conversation.created","conversation":{"id":"held-readiness"}}),
    )
    .await;
    assert_eq!(read(&mut ws).await["type"], "session.update");
    drop(starting);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(completion.wait(), closed(&mut ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    drop(ws);
    released(listener).await;
}

#[tokio::test]
async fn public_pcm_text_and_total_input_bounds_reject_without_wire_effects() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.max_audio_chunk_bytes = 4;
    config.limits.max_input_bytes = 8;
    config.limits.max_text_bytes = 4;
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    for bytes in [&[][..], &[1][..], &[1, 2, 3, 4, 5, 6][..]] {
        assert!(matches!(
            fixture.running.append_pcm(bytes).await,
            Err(GrokAppError::Invalid(_))
        ));
    }
    assert!(matches!(
        fixture.running.append_text("", true).await,
        Err(GrokAppError::Invalid(_))
    ));
    assert!(matches!(
        fixture.running.append_text("12345", true).await,
        Err(GrokAppError::Invalid(_))
    ));
    quiet(&mut fixture.ws).await;
    fixture.running.append_pcm(PCM).await.unwrap();
    let append = read(&mut fixture.ws).await;
    assert_eq!(append["type"], "input_audio_buffer.append");
    assert_eq!(
        STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
        PCM
    );
    fixture.running.append_text("last", false).await.unwrap();
    assert_eq!(
        read(&mut fixture.ws).await["item"]["content"][0]["text"],
        "last"
    );
    assert!(matches!(
        fixture.running.append_pcm(&[1, 2]).await,
        Err(GrokAppError::Invalid(_))
    ));
    assert!(matches!(
        fixture.running.append_text("x", false).await,
        Err(GrokAppError::Invalid(_))
    ));
    quiet(&mut fixture.ws).await;
    fixture.finish().await;
}

#[tokio::test]
async fn input_operation_count_and_speech_wait_conditions_are_finite() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.max_input_messages = 2;
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    for text in ["one", "two"] {
        fixture.running.append_text(text, false).await.unwrap();
        assert_eq!(
            read(&mut fixture.ws).await["item"]["content"][0]["text"],
            text
        );
    }
    assert!(matches!(
        fixture.running.run().await,
        Err(GrokAppError::Invalid(_))
    ));
    assert!(matches!(
        fixture.running.append_text("three", false).await,
        Err(GrokAppError::Invalid(_))
    ));
    for (count, duration) in [
        (0, WAIT),
        (65, WAIT),
        (1, Duration::ZERO),
        (1, Duration::from_secs(3601)),
    ] {
        assert!(matches!(
            fixture
                .running
                .wait_for_speech_responses(count, duration)
                .await,
            Err(GrokAppError::Invalid(_))
        ));
    }
    quiet(&mut fixture.ws).await;
    fixture.finish().await;
}

#[tokio::test]
async fn output_total_bound_fails_closed_and_preserves_actual_join_evidence() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.max_output_bytes = 4;
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    fixture.run().await;
    response(&mut fixture.ws, "output-overflow", false).await;
    audio(&mut fixture.ws, "output-overflow", &[1, 2, 3, 4, 5, 6]).await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert!(report.primary.is_some());
    assert!(report.output_bytes <= 4);
    assert!(fixture.output.snapshot().pcm.len() <= 4);
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn final_partial_pcm_sample_is_failure_not_silent_success() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture.run().await;
    response(&mut fixture.ws, "partial-pcm", false).await;
    audio(&mut fixture.ws, "partial-pcm", &[1]).await;
    wire(&mut fixture.ws, json!({"type":"response.output_audio.done","response_id":"partial-pcm","item_id":"speech-partial-pcm","output_index":0,"content_index":0})).await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::Mapping)
    );
    assert!(!report.cleanup_output_failed);
    assert!(report.owners_joined(), "{report:?}");
    assert_eq!(report.generated_speech_responses, 0);
    assert!(fixture.output.snapshot().pcm.is_empty());
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn malformed_provider_json_preserves_non_success_and_owned_cleanup() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture
        .ws
        .send(Message::Text("{not valid provider json".into()))
        .await
        .unwrap();
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert!(report.primary.is_some());
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn provider_readiness_rejection_cleans_real_socket_without_leaking_payload() {
    let listener = listener().await;
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        Arc::new(RecordingOutput::default()),
    )
    .unwrap();
    let completion = app.completion();
    let peer = async {
        let mut ws = accept(&listener, KEY).await;
        wire(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"rejected-session"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        wire(&mut ws, json!({"type":"error","error":{"type":"invalid_request_error","code":"fixture_rejected","message":"private-provider-rejection-payload"}})).await;
        closed(&mut ws, false).await;
    };
    let (started, ()) = timeout(WAIT, async { tokio::join!(app.start(), peer) })
        .await
        .unwrap();
    assert!(started.is_err());
    let report = completion.wait().await.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    let diagnostic = format!("{report:?}");
    assert!(!diagnostic.contains(KEY));
    assert!(!diagnostic.contains("private-provider-rejection-payload"));
    released(listener).await;
}

#[tokio::test]
async fn actual_output_process_failure_is_retained_with_small_bounded_event_queue() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    // Small enough to exercise bounded reporting, but permits real bootstrap.
    // Actual queue exhaustion is covered separately at the provider event queue.
    config.limits.output_event_capacity = 4;
    let output = Arc::new(RecordingOutput::default());
    output.failure.store(2, Ordering::SeqCst);
    let mut fixture = Fixture::with(listener, config, vec![], vec![], output).await;
    fixture.run().await;
    response(&mut fixture.ws, "output-error", false).await;
    audio(&mut fixture.ws, "output-error", PCM).await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.primary.is_some());
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::OutputProcess)
    );
    assert!(!report.cleanup_output_failed);
    assert!(report.owners_joined(), "{report:?}");
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn actual_output_cleanup_failure_makes_otherwise_clean_end_unsuccessful() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture.output.failure.store(3, Ordering::SeqCst);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.running.end(), closed(&mut fixture.ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.primary.is_some());
    assert_eq!(report.first_failure_stage, None);
    assert!(report.cleanup_output_failed);
    assert!(report.owners_joined(), "{report:?}");
    fixture.output.assert_cleaned();
    assert!(!fixture.completion.wait().await.unwrap().successful());
    drop(fixture.ws);
    released(fixture.listener).await;
}

async fn rejected_before_effects(
    config: GrokAppConfig,
    seed: Vec<ChatMessage>,
    tools: Vec<GrokLocalTool>,
    listener: &TcpListener,
) {
    let output = Arc::new(RecordingOutput::default());
    let result = GrokRealtimeApp::new(config, seed, tools, output.clone());
    assert!(matches!(result, Err(GrokAppError::Invalid(_))));
    assert_eq!(output.setup_calls.load(Ordering::SeqCst), 0);
    assert_eq!(output.cleanup_calls.load(Ordering::SeqCst), 0);
    assert!(timeout(NEGATIVE, listener.accept()).await.is_err());
}

#[tokio::test]
async fn constructor_rejects_unbounded_and_unsupported_configuration_before_network_or_output() {
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let invalid: [fn(&mut GrokAppConfig); 10] = [
        |config| config.limits.queue_capacity = 0,
        |config| config.limits.output_event_capacity = 1025,
        |config| config.limits.max_audio_chunk_bytes = 3,
        |config| config.limits.max_input_bytes = usize::MAX,
        |config| config.limits.max_schema_depth = 33,
        |config| config.limits.drain_timeout = Duration::ZERO,
        |config| config.connection.limits.max_session_responses = 0,
        |config| config.connection.session.turn_detection = Some(None),
        |config| config.connection.session.id = Some("historical-session".into()),
        |config| config.connection.endpoint.push_str("/other"),
    ];
    for invalidate in invalid {
        let mut config = config(address);
        invalidate(&mut config);
        rejected_before_effects(config, vec![], vec![], &listener).await;
    }
    released(listener).await;
}

#[tokio::test]
async fn constructor_rejects_tool_history_and_conflicting_or_noninterruptible_bindings() {
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    rejected_before_effects(
        config(address),
        vec![ChatMessage::new(ChatRole::Tool, "old result")],
        vec![],
        &listener,
    )
    .await;
    let mut named = ChatMessage::new(ChatRole::User, "not ordinary");
    named.name = Some("provider-envelope".into());
    rejected_before_effects(config(address), vec![named], vec![], &listener).await;
    rejected_before_effects(config(address), vec![], vec![echo(), echo()], &listener).await;
    let mut noninterruptible = echo();
    noninterruptible.handler.cancel_on_interruption = false;
    rejected_before_effects(config(address), vec![], vec![noninterruptible], &listener).await;
    let mut overlong = echo();
    overlong.handler.timeout = Duration::from_secs(6);
    rejected_before_effects(config(address), vec![], vec![overlong], &listener).await;
    let mut conflicting = config(address);
    conflicting.connection.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "conflicting schema",
        json!({"type":"object"}),
    )
    .into()]);
    rejected_before_effects(conflicting, vec![], vec![echo()], &listener).await;
    let mut missing = config(address);
    missing.connection.session.tools = Some(vec![schema().into()]);
    rejected_before_effects(missing, vec![], vec![], &listener).await;
    released(listener).await;
}

#[tokio::test]
async fn constructor_bounds_deep_schema_and_retained_capacity_before_cloning() {
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let mut parameters = Value::Null;
    for _ in 0..40 {
        parameters = json!({"nested":parameters});
    }
    let mut deep = echo();
    deep.schema.parameters = parameters;
    rejected_before_effects(config(address), vec![], vec![deep], &listener).await;
    let mut bounded = config(address);
    bounded.limits.max_retained_bytes = 16 * 1024;
    bounded.limits.max_history_bytes = 4096;
    bounded.limits.max_schema_bytes = 4096;
    bounded.limits.max_text_bytes = 1024;
    let mut content = String::with_capacity(32 * 1024);
    content.push('x');
    rejected_before_effects(
        bounded,
        vec![ChatMessage::new(ChatRole::User, content)],
        vec![],
        &listener,
    )
    .await;
    let mut large = echo();
    large.schema.parameters = json!({"enum":vec![Value::Null; 5000]});
    rejected_before_effects(config(address), vec![], vec![large], &listener).await;
    released(listener).await;
}

/// These are synthetic acknowledgments from a controlled backend. They qualify
/// native receipt/drain mechanics only, never a physical speaker or user hearing.
struct ControlledBackend {
    receipt: PlaybackReceipt,
    entered: Notify,
    interrupted: Notify,
    entered_contexts: Mutex<Vec<Option<String>>>,
    release: Semaphore,
    calls: AtomicUsize,
    finished: AtomicUsize,
    dropped: Arc<AtomicUsize>,
    interrupts: AtomicUsize,
    stops: AtomicUsize,
    pcm: Mutex<Vec<u8>>,
}
impl ControlledBackend {
    fn new(receipt: PlaybackReceipt) -> Self {
        Self {
            receipt,
            entered: Notify::new(),
            interrupted: Notify::new(),
            entered_contexts: Mutex::new(vec![]),
            release: Semaphore::new(0),
            calls: AtomicUsize::new(0),
            finished: AtomicUsize::new(0),
            dropped: Arc::new(AtomicUsize::new(0)),
            interrupts: AtomicUsize::new(0),
            stops: AtomicUsize::new(0),
            pcm: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl OutputBackend for ControlledBackend {
    async fn start(&self, format: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        assert_eq!(format.sample_rate(), 24_000);
        assert_eq!(format.channels(), 1);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let _drop = CallbackDropped(self.dropped.clone());
        let audio = match &frame.data {
            FrameData::OutputAudio(audio) | FrameData::TtsAudio(audio) => audio,
            other => panic!("unexpected controlled backend data: {other:?}"),
        };
        assert_eq!(audio.sample_rate, 24_000);
        assert_eq!(audio.num_channels, 1);
        assert_eq!(audio.audio.len(), 480);
        {
            let mut contexts = self.entered_contexts.lock().unwrap();
            assert!(contexts.len() < 8);
            contexts.push(audio.context_id.clone());
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        let mut pcm = self.pcm.lock().unwrap();
        assert!(pcm.len() + audio.audio.len() <= 1920);
        pcm.extend_from_slice(&audio.audio);
        self.finished.fetch_add(1, Ordering::SeqCst);
        Ok(self.receipt)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupts.fetch_add(1, Ordering::SeqCst);
        self.interrupted.notify_one();
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
fn actual_output(backend: Arc<ControlledBackend>) -> Arc<OutputTransport> {
    Arc::new(
        OutputTransport::new(
            "ActualAppOutputTransport",
            OutputTransportConfig {
                sample_rate: Some(24_000),
                chunk_duration: Duration::from_millis(10),
                queue_capacity: 8,
                queue_byte_capacity: 4096,
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: WAIT,
                ..Default::default()
            },
            backend,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    )
}
async fn backend_speech(ws: &mut Ws, id: &str, value: u8) {
    response(ws, id, false).await;
    audio(ws, id, &[value, 0].repeat(240)).await;
    wire(ws, json!({"type":"response.output_audio_transcript.delta","response_id":id,"item_id":format!("speech-{id}"),"delta":"generated history is not playback history"})).await;
    response(ws, id, true).await;
}
async fn real_output_end_case(receipt: PlaybackReceipt) {
    let listener = listener().await;
    let backend = Arc::new(ControlledBackend::new(receipt));
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        actual_output(backend.clone()),
    )
    .unwrap();
    let completion = app.completion();
    let (running, mut ws) = timeout(WAIT, async {
        tokio::join!(app.start(), ready(&listener, KEY))
    })
    .await
    .unwrap();
    let running = running.unwrap();
    assert_eq!(read(&mut ws).await["type"], "session.update");
    quiet(&mut ws).await;
    running.run().await.unwrap();
    assert_eq!(read(&mut ws).await["type"], "response.create");
    backend_speech(&mut ws, "real-output", 1).await;
    timeout(WAIT, backend.entered.notified()).await.unwrap();
    let mut ending = Box::pin(running.end());
    assert!(
        timeout(NEGATIVE, &mut ending).await.is_err(),
        "End passed an unacknowledged real output operation"
    );
    assert_eq!(backend.finished.load(Ordering::SeqCst), 0);
    quiet(&mut ws).await;
    backend.release.add_permits(1);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert!(report.owners_joined());
    drop(ending);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.finished.load(Ordering::SeqCst), 1);
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
    assert_eq!(*backend.pcm.lock().unwrap(), [1_u8, 0].repeat(240));
    assert_eq!(
        completion.history().unwrap().messages,
        vec![ChatMessage::new(
            ChatRole::Assistant,
            "generated history is not playback history"
        )]
    );
    drop(ws);
    released(listener).await;
}
#[tokio::test]
async fn real_output_played_receipt_is_held_until_actual_backend_completion() {
    real_output_end_case(PlaybackReceipt::Played).await;
}
#[tokio::test]
async fn real_output_discarded_receipt_does_not_turn_generated_history_into_playback() {
    real_output_end_case(PlaybackReceipt::Discarded).await;
}
#[tokio::test]
async fn cancel_during_real_output_drain_drops_unacknowledged_playback_and_joins() {
    let listener = listener().await;
    let backend = Arc::new(ControlledBackend::new(PlaybackReceipt::Played));
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        actual_output(backend.clone()),
    )
    .unwrap();
    let completion = app.completion();
    let (running, mut ws) = timeout(WAIT, async {
        tokio::join!(app.start(), ready(&listener, KEY))
    })
    .await
    .unwrap();
    let running = running.unwrap();
    assert_eq!(read(&mut ws).await["type"], "session.update");
    running.run().await.unwrap();
    assert_eq!(read(&mut ws).await["type"], "response.create");
    backend_speech(&mut ws, "cancel-output", 2).await;
    timeout(WAIT, backend.entered.notified()).await.unwrap();
    let mut ending = Box::pin(running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    drop(ending);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(running.cancel(), closed(&mut ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert_eq!(backend.finished.load(Ordering::SeqCst), 0);
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
    assert!(!completion.wait().await.unwrap().successful());
    drop(ws);
    released(listener).await;
}

#[tokio::test]
async fn callback_error_with_excess_spare_capacity_is_sanitized_before_native_settlement() {
    let local = tool(Arc::new(|_| {
        Box::pin(async {
            let mut error = String::with_capacity(1024 * 1024);
            error.push('x');
            Err(error)
        })
    }));
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    call(&mut fixture.ws, "error-tool", "error-call", 1).await;
    response(&mut fixture.ws, "error-tool", true).await;
    let output = read(&mut fixture.ws).await;
    assert_eq!(output["item"]["type"], "function_call_output");
    let error_text = output["item"]["output"].as_str().unwrap();
    assert!(error_text.len() < 128);
    assert!(error_text.contains("application final callback failed"));
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    speech(
        &mut fixture.ws,
        "after-error",
        "bounded native failure was handled",
    )
    .await;
    fixture
        .running
        .wait_for_speech_responses(1, WAIT)
        .await
        .unwrap();
    let completion = fixture.completion.clone();
    let capture = fixture.output.clone();
    fixture.finish().await;
    assert_eq!(
        capture.snapshot().errors,
        vec!["application final callback failed"]
    );
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(
        journal.terminal[0].outcome.reason(),
        ToolCallEndReason::CallbackError
    );
    assert_eq!(
        journal.terminal[0].outcome.state(),
        ToolCallTerminalState::Failed
    );
    assert!(journal.retained_payload_bytes < 4096);
}

async fn declare_arguments(ws: &mut Ws, call_id: &str, arguments: Value) {
    wire(ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":format!("item-{call_id}"),"call_id":call_id,"name":"echo","arguments":""}})).await;
    wire(ws, json!({"type":"response.function_call_arguments.done","call_id":call_id,"name":"echo","arguments":arguments.to_string()})).await;
}

async fn callback_failure_case(timed_out: bool) {
    let calls = Arc::new(AtomicUsize::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let mut local = tool(Arc::new({
        let calls = calls.clone();
        let dropped = dropped.clone();
        move |_| {
            let calls = calls.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                let _drop = CallbackDropped(dropped);
                if timed_out {
                    std::future::pending::<()>().await;
                }
                panic!("synthetic final callback panic");
            })
        }
    }));
    local.handler.timeout = Duration::from_millis(120);
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    call(&mut fixture.ws, "failed-callback", "failed-call", 1).await;
    response(&mut fixture.ws, "failed-callback", true).await;
    let output = read(&mut fixture.ws).await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], "failed-call");
    let content = output["item"]["output"].as_str().unwrap();
    if timed_out {
        assert_eq!(content, "CANCELLED");
    } else {
        assert_eq!(
            serde_json::from_str::<Value>(content).unwrap()["error"],
            "tool callback panicked"
        );
    }
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    speech(
        &mut fixture.ws,
        "failure-handled",
        "native failure handled once",
    )
    .await;
    fixture
        .running
        .wait_for_speech_responses(1, WAIT)
        .await
        .unwrap();
    let completion = fixture.completion.clone();
    let output_capture = fixture.output.clone();
    let expected_content = content.to_owned();
    let report = fixture.finish().await;
    assert_eq!(report.mapped_tool_calls, 1);
    assert_eq!(report.settled_tool_calls, 1);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(journal.terminal.len(), 1);
    let outcome = &journal.terminal[0].outcome;
    assert_eq!(outcome.state(), ToolCallTerminalState::Failed);
    assert_eq!(
        outcome.reason(),
        if timed_out {
            ToolCallEndReason::TimedOut
        } else {
            ToolCallEndReason::CallbackPanicked
        }
    );
    assert_eq!(
        outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    let captured = output_capture.snapshot();
    assert_eq!(captured.outcomes.len(), 1);
    assert_eq!(captured.outcomes[0].id(), outcome.id());
    assert_eq!(captured.pcm, PCM);
    let history = completion.history().unwrap();
    let results: Vec<_> = history
        .messages
        .iter()
        .filter(|message| message.role == ChatRole::Tool)
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tool_call_id.as_deref(), Some("failed-call"));
    assert_eq!(results[0].content, expected_content);
}

#[tokio::test]
async fn callback_panic_keeps_native_failed_receipt_and_one_assistant_continuation() {
    callback_failure_case(false).await;
}

#[tokio::test]
async fn callback_timeout_keeps_native_failure_while_source_cancellation_requests_continuation() {
    callback_failure_case(true).await;
}

#[tokio::test]
async fn sibling_final_results_hold_end_until_both_native_receipts_and_one_continuation() {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new([Semaphore::new(0), Semaphore::new(0)]);
    let calls = Arc::new(AtomicUsize::new(0));
    let local = tool(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        move |call| {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            Box::pin(async move {
                let index = call.arguments["n"].as_u64().unwrap() as usize;
                calls.fetch_add(1, Ordering::SeqCst);
                entered.add_permits(1);
                release[index].acquire().await.unwrap().forget();
                Ok(call.arguments)
            })
        }
    }));
    let mut fixture = Fixture::new(vec![local]).await;
    fixture.run().await;
    response(&mut fixture.ws, "siblings", false).await;
    declare_arguments(&mut fixture.ws, "sibling-first", json!({"n":0})).await;
    declare_arguments(&mut fixture.ws, "sibling-second", json!({"n":1})).await;
    timeout(WAIT, entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let mut ending = Box::pin(fixture.running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    release[1].add_permits(1);
    let second = read(&mut fixture.ws).await;
    assert_eq!(second["item"]["type"], "function_call_output");
    assert_eq!(second["item"]["call_id"], "sibling-second");
    assert_eq!(second["item"]["output"], "{\"n\":1}");
    fixture
        .output
        .observed(|capture| capture.outcomes.len() == 1)
        .await;
    quiet(&mut fixture.ws).await;
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    release[0].add_permits(1);
    let first = read(&mut fixture.ws).await;
    assert_eq!(first["item"]["type"], "function_call_output");
    assert_eq!(first["item"]["call_id"], "sibling-first");
    assert_eq!(first["item"]["output"], "{\"n\":0}");
    fixture
        .output
        .observed(|capture| capture.outcomes.len() == 2)
        .await;
    quiet(&mut fixture.ws).await;
    response(&mut fixture.ws, "siblings", true).await;
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    speech(&mut fixture.ws, "siblings-handled", "both siblings settled").await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert!(report.owners_joined());
    assert_eq!(report.mapped_tool_calls, 2);
    assert_eq!(report.settled_tool_calls, 2);
    drop(ending);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 2);
    assert!(journal.terminal.iter().all(|record| record.outcome.state()
        == ToolCallTerminalState::Completed
        && record.outcome.reason() == ToolCallEndReason::CallbackReturned));
    let capture = fixture.output.snapshot();
    assert_eq!(capture.outcomes.len(), 2);
    assert_ne!(capture.outcomes[0].id(), capture.outcomes[1].id());
    assert_eq!(capture.pcm, PCM);
    let history = fixture.completion.history().unwrap();
    for (id, content) in [
        ("sibling-first", "{\"n\":0}"),
        ("sibling-second", "{\"n\":1}"),
    ] {
        let results: Vec<_> = history
            .messages
            .iter()
            .filter(|message| {
                message.role == ChatRole::Tool && message.tool_call_id.as_deref() == Some(id)
            })
            .collect();
        assert_eq!(results.len(), 1);
        assert_eq!(results[0].content, content);
    }
    assert_eq!(
        history.messages.last().unwrap(),
        &ChatMessage::new(ChatRole::Assistant, "both siblings settled")
    );
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

async fn repeated_tool_chain_cap(response_cap: bool) {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.connection.limits.max_outstanding_tools = 1;
    if response_cap {
        config.connection.limits.max_session_responses = 2;
    } else {
        config.connection.limits.max_session_tool_calls = 2;
    }
    let calls = Arc::new(AtomicUsize::new(0));
    let local = tool(Arc::new({
        let calls = calls.clone();
        move |call| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(call.arguments) })
        }
    }));
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![local],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    fixture.run().await;
    call(&mut fixture.ws, "chain-one", "chain-call-one", 1).await;
    response(&mut fixture.ws, "chain-one", true).await;
    assert_eq!(
        read(&mut fixture.ws).await["item"]["call_id"],
        "chain-call-one"
    );
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    let mut ending = Box::pin(fixture.running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    call(&mut fixture.ws, "chain-two", "chain-call-two", 2).await;
    response(&mut fixture.ws, "chain-two", true).await;
    assert_eq!(
        read(&mut fixture.ws).await["item"]["call_id"],
        "chain-call-two"
    );
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    response(&mut fixture.ws, "chain-over-limit", false).await;
    if !response_cap {
        // The actor rejects this third declaration before it can reach the mapper
        // or executor. Do not fabricate a native cancellation for unentered work.
        wire(&mut fixture.ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"item-chain-over-limit","call_id":"chain-over-limit","name":"echo","arguments":""}})).await;
    }
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert_eq!(report.provider_primary, Some(RealtimeError::TooLarge));
    assert_eq!(report.generated_speech_responses, 0);
    drop(ending);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 2);
    assert!(journal.terminal.iter().all(|record| record.outcome.state()
        == ToolCallTerminalState::Completed
        && record.outcome.reason() == ToolCallEndReason::CallbackReturned));
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn repeated_tool_chain_fails_finitely_at_call_and_response_caps_during_end() {
    repeated_tool_chain_cap(false).await;
    repeated_tool_chain_cap(true).await;
}

#[tokio::test]
async fn repeated_tool_continuations_do_not_reset_the_absolute_end_drain_deadline() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.drain_timeout = Duration::from_secs(2);
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicUsize::new(0));
    let local = tool(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let dropped = dropped.clone();
        move |call| {
            let entered = entered.clone();
            let release = release.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _drop = CallbackDropped(dropped);
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(call.arguments)
            })
        }
    }));
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![local],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    fixture.run().await;
    call(&mut fixture.ws, "deadline-first", "deadline-call-first", 0).await;
    timeout(WAIT, entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    response(&mut fixture.ws, "deadline-first", true).await;
    let started = tokio::time::Instant::now();
    let mut ending = Box::pin(fixture.running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    release.add_permits(1);
    assert_eq!(
        read(&mut fixture.ws).await["item"]["call_id"],
        "deadline-call-first"
    );
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    for index in 1..=3 {
        tokio::time::sleep_until(started + Duration::from_millis(index * 500)).await;
        let response_id = format!("deadline-{index}");
        let call_id = format!("deadline-call-{index}");
        call(&mut fixture.ws, &response_id, &call_id, index).await;
        timeout(WAIT, entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        response(&mut fixture.ws, &response_id, true).await;
        if index < 3 {
            release.add_permits(1);
            assert_eq!(read(&mut fixture.ws).await["item"]["call_id"], call_id);
            assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
        }
    }
    // The final callback remains held. A fresh two-second deadline at the last
    // continuation would outlive this bound; only the original End deadline wins.
    let (report, ()) = tokio::time::timeout_at(started + Duration::from_secs(3), async {
        tokio::join!(&mut ending, closed(&mut fixture.ws, false))
    })
    .await
    .expect("tool continuations extended the absolute drain deadline");
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert_eq!(report.primary, Some(GrokAppError::Timeout("causal drain")));
    drop(ending);
    assert_eq!(dropped.load(Ordering::SeqCst), 4);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 4);
    assert_eq!(
        journal
            .terminal
            .iter()
            .filter(
                |record| record.outcome.state() == ToolCallTerminalState::Completed
                    && record.outcome.reason() == ToolCallEndReason::CallbackReturned
            )
            .count(),
        3
    );
    assert_eq!(
        journal
            .terminal
            .iter()
            .filter(|record| record.outcome.state() == ToolCallTerminalState::Cancelled)
            .count(),
        1
    );
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

async fn rejected_provider_arguments(call_id: &str, arguments: Value, limit: usize) {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.max_tool_argument_bytes = limit;
    let calls = Arc::new(AtomicUsize::new(0));
    let local = tool(Arc::new({
        let calls = calls.clone();
        move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Value::Null) })
        }
    }));
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![local],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    fixture.run().await;
    response(&mut fixture.ws, "invalid-provider-arguments", false).await;
    declare_arguments(&mut fixture.ws, call_id, arguments).await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(!report.successful());
    assert!(report.owners_joined(), "{report:?}");
    assert!(report.primary.is_some());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    assert!(fixture.output.snapshot().outcomes.is_empty());
    assert!(fixture
        .completion
        .history()
        .unwrap()
        .messages
        .iter()
        .all(|message| message.tool_calls.is_empty() && message.role != ChatRole::Tool));
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn provider_origin_oversized_arguments_fail_before_real_callback_or_native_lease() {
    rejected_provider_arguments(
        "invalid-arguments-call",
        json!({"padding":"x".repeat(256)}),
        64,
    )
    .await;
    // Valid small arguments cannot hide an overlarge provider identity from the
    // pre-ledger guard; this must not allocate a native callback lease either.
    rejected_provider_arguments(&"x".repeat(256), json!({"n":1}), 64).await;
}

#[tokio::test]
async fn provider_origin_deep_arguments_fail_before_real_callback_or_native_lease() {
    let mut arguments = json!({"n":1});
    for _ in 0..40 {
        arguments = json!({"nested":arguments});
    }
    rejected_provider_arguments("invalid-arguments-call", arguments, 4096).await;
}

#[tokio::test]
async fn empty_audio_delta_and_done_cannot_satisfy_required_generated_speech() {
    let mut fixture = Fixture::new(vec![]).await;
    fixture.run().await;
    response(&mut fixture.ws, "empty-audio", false).await;
    audio(&mut fixture.ws, "empty-audio", &[]).await;
    wire(&mut fixture.ws, json!({"type":"response.output_audio_transcript.delta","response_id":"empty-audio","item_id":"speech-empty-audio","delta":"transcript without PCM is not generated speech"})).await;
    wire(&mut fixture.ws, json!({"type":"response.output_audio.done","response_id":"empty-audio","item_id":"speech-empty-audio","output_index":0,"content_index":0})).await;
    response(&mut fixture.ws, "empty-audio", true).await;
    fixture
        .output
        .observed(|capture| capture.marks.contains(&Mark::ResponseEnd))
        .await;
    assert!(matches!(
        fixture.running.wait_for_speech_responses(1, NEGATIVE).await,
        Err(GrokAppError::Timeout("speech response"))
    ));
    assert!(fixture.output.snapshot().pcm.is_empty());
    let report = fixture.finish().await;
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
}

fn assert_failed_owners(report: &GrokAppReport) {
    assert!(!report.successful(), "{report:?}");
    assert!(report.primary.is_some(), "{report:?}");
    assert!(report.pipeline_joined, "{report:?}");
    assert!(report.provider_joined, "{report:?}");
    assert!(report.owners_joined(), "{report:?}");
    // Preserve secondary evidence rather than silently treating an observed
    // cleanup failure as a clean terminal result.
    if report.provider_cleanup.is_some() {
        assert!(report.secondary_failure, "{report:?}");
    }
    if report.pipeline_failure.is_some() {
        assert_ne!(report.pipeline_cleanup, GrokPipelineCleanup::Clean);
    }
    assert!(!report.forced_cleanup, "{report:?}");
}

#[tokio::test]
async fn real_output_setup_error_fails_start_and_joins_the_ready_socket_and_pipeline() {
    let listener = listener().await;
    let output = Arc::new(RecordingOutput::default());
    output.failure.store(1, Ordering::SeqCst);
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        output.clone(),
    )
    .unwrap();
    let completion = app.completion();
    let peer = async {
        let mut ws = ready(&listener, KEY).await;
        let mut initial_updates = 0;
        loop {
            match ws.next().await {
                Some(Ok(Message::Text(text))) => {
                    let command: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(command["type"], "session.update");
                    initial_updates += 1;
                    assert!(
                        initial_updates <= 1,
                        "startup repeated session configuration"
                    );
                }
                Some(Ok(Message::Close(_))) => {
                    let _ = ws.flush().await;
                    break;
                }
                None | Some(Err(_)) => break,
                other => panic!("unexpected setup-failure peer event: {other:?}"),
            }
        }
    };
    let (started, ()) = timeout(WAIT, async { tokio::join!(app.start(), peer) })
        .await
        .unwrap();
    assert!(
        started.is_err(),
        "failed output setup returned a running app"
    );
    let report = timeout(WAIT, completion.wait()).await.unwrap().unwrap();
    assert_failed_owners(&report);
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::OutputSetup)
    );
    assert!(!report.cleanup_output_failed);
    assert_eq!(
        report.pipeline_failure,
        Some(GrokPipelineFailure::Execution)
    );
    assert_eq!(
        report.pipeline_cleanup,
        GrokPipelineCleanup::UnknownAfterFailure
    );
    assert!(report.provider_primary.is_none(), "{report:?}");
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    assert_eq!(completion.outcomes().total_terminal, 0);
    assert!(completion.outcomes().active.is_empty());
    assert!(output.snapshot().pcm.is_empty());
    output.assert_cleaned();
    let diagnostic = format!("{report:?}");
    assert!(!diagnostic.contains(KEY));
    assert!(!diagnostic.contains("synthetic output setup failure"));
    released(listener).await;
}

#[tokio::test]
async fn remote_tcp_eof_during_response_preserves_transport_failure_and_owned_cleanup() {
    use tokio::io::AsyncWriteExt;
    let mut fixture = Fixture::new(vec![]).await;
    fixture.run().await;
    response(&mut fixture.ws, "remote-eof", false).await;
    fixture
        .output
        .observed(|capture| capture.marks.contains(&Mark::ResponseStart))
        .await;
    // Send TCP EOF without a WebSocket Close or response.done, retaining the
    // read half to observe the application's actual socket close/EOF too.
    fixture.ws.get_mut().shutdown().await.unwrap();
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert_failed_owners(&report);
    assert_eq!(report.provider_primary, Some(RealtimeError::Transport));
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn oversized_wire_message_fails_with_original_capacity_error_and_owned_cleanup() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.connection.limits.max_message_bytes = 4096;
    config.connection.limits.max_audio_chunk_bytes = 1024;
    config.limits.max_audio_chunk_bytes = 1024;
    config.limits.max_text_bytes = 256;
    config.limits.max_tool_argument_bytes = 512;
    config.limits.max_tool_result_bytes = 256;
    let mut fixture = Fixture::with(
        listener,
        config,
        vec![],
        vec![],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    // Valid JSON and a recognized event distinguish the WebSocket byte limit
    // from malformed-JSON, callback-argument, or output-recording bounds.
    wire(
        &mut fixture.ws,
        json!({"type":"input_audio_buffer.cleared","padding":"x".repeat(8192)}),
    )
    .await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert_failed_owners(&report);
    assert_eq!(report.provider_primary, Some(RealtimeError::TooLarge));
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    assert!(fixture.output.snapshot().outcomes.is_empty());
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn real_provider_event_queue_exhaustion_fails_before_receive_start_and_still_joins() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.connection.limits.queue_capacity = 2;
    let output = Arc::new(RecordingOutput::default());
    output.hold.store(3, Ordering::SeqCst);
    let app = GrokRealtimeApp::new(config, vec![], vec![], output.clone()).unwrap();
    let completion = app.completion();
    let mut starting = Box::pin(app.start());
    let mut ws = tokio::select! {
        result = &mut starting => panic!("startup crossed held output setup: {}", result.is_ok()),
        ws = ready(&listener, KEY) => ws,
    };
    output.held().await;
    // Provider setup consumed session.updated, but the real receive task starts
    // only at pipeline Start, after output setup returns. Exactly three valid
    // events therefore overflow the actual two-slot actor event queue without
    // relying on scheduler speed, output errors, or a fabricated frame route.
    for _ in 0..3 {
        wire(&mut ws, json!({"type":"input_audio_buffer.cleared"})).await;
    }
    closed(&mut ws, false).await;
    assert_eq!(output.setup_calls.load(Ordering::SeqCst), 1);
    assert_eq!(output.cleanup_calls.load(Ordering::SeqCst), 0);
    output.release.notify_one();
    let started = timeout(WAIT, &mut starting).await.unwrap();
    assert!(started.is_err(), "queue exhaustion returned a running app");
    drop(starting);
    let report = timeout(WAIT, completion.wait()).await.unwrap().unwrap();
    assert_failed_owners(&report);
    assert_eq!(report.provider_primary, Some(RealtimeError::Backpressure));
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    assert!(output.snapshot().pcm.is_empty());
    output.assert_cleaned();
    drop(ws);
    released(listener).await;
}

async fn server_vad_burst_case(duplicate_start: bool) {
    let listener = listener().await;
    let config = config(listener.local_addr().unwrap());
    let seed = vec![
        ChatMessage::new(ChatRole::System, "burst system instruction"),
        ChatMessage::new(ChatRole::User, "ordinary seed retained once"),
    ];
    let mut fixture = Fixture::with(
        listener,
        config,
        seed.clone(),
        vec![echo()],
        Arc::new(RecordingOutput::default()),
    )
    .await;
    assert_eq!(
        fixture
            .startup
            .iter()
            .filter(|command| command["type"] == "conversation.item.create")
            .count(),
        1
    );
    fixture.running.append_pcm(PCM).await.unwrap();
    let append = read(&mut fixture.ws).await;
    assert_eq!(append["type"], "input_audio_buffer.append");
    assert_eq!(
        STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
        PCM
    );
    // Queue the entire server-VAD turn and server-owned response before one
    // flush. In particular, do not wait for the pipeline's interruption/cancel
    // between speech_started and the subsequent provider response events.
    let mut events = vec![
        json!({"type":"input_audio_buffer.speech_started","item_id":"burst-user","audio_start_ms":0}),
        json!({"type":"input_audio_buffer.speech_stopped","item_id":"burst-user","audio_end_ms":20}),
        json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"burst-user","transcript":"final upstream user transcript"}),
        json!({"type":"response.created","response":{"id":"burst-response","status":"in_progress","output":[]}}),
        json!({"type":"response.output_audio.delta","response_id":"burst-response","item_id":"speech-burst-response","output_index":0,"content_index":0,"delta":STANDARD.encode(&PCM[..1])}),
        json!({"type":"response.output_audio.delta","response_id":"burst-response","item_id":"speech-burst-response","output_index":0,"content_index":0,"delta":STANDARD.encode(&PCM[1..3])}),
        json!({"type":"response.output_audio.delta","response_id":"burst-response","item_id":"speech-burst-response","output_index":0,"content_index":0,"delta":STANDARD.encode(&PCM[3..])}),
        json!({"type":"response.output_audio_transcript.delta","response_id":"burst-response","item_id":"speech-burst-response","delta":"generated server-owned answer"}),
        json!({"type":"response.output_audio.done","response_id":"burst-response","item_id":"speech-burst-response","output_index":0,"content_index":0}),
        json!({"type":"response.done","response":{"id":"burst-response","status":"completed","output":[]}}),
    ];
    if duplicate_start {
        // A second genuine provider proposal with a fresh event identity is a
        // no-op while the user is already speaking, not a second interruption.
        // Keep it in the same flush so the private proposal barrier must also
        // release without relying on a cancellation that must never be sent.
        events.insert(
            1,
            json!({"type":"input_audio_buffer.speech_started","item_id":"burst-user","audio_start_ms":0}),
        );
    }
    timeout(WAIT, async {
        for (index, mut event) in events.into_iter().enumerate() {
            event["event_id"] = json!(format!("server-vad-burst-{index}"));
            fixture
                .ws
                .feed(Message::Text(event.to_string().into()))
                .await
                .unwrap();
        }
        fixture.ws.flush().await.unwrap();
    })
    .await
    .unwrap();
    // The real user aggregator owns interruption. Its exact cancellation is
    // expected, not a fixture error, and never authorizes clear/truncate/create.
    assert_eq!(read(&mut fixture.ws).await["type"], "response.cancel");
    fixture
        .running
        .wait_for_speech_responses(1, WAIT)
        .await
        .unwrap();
    quiet(&mut fixture.ws).await;
    let completion = fixture.completion.clone();
    let output = fixture.output.clone();
    let report = fixture.finish().await;
    assert_eq!(report.generated_speech_responses, 1);
    assert_eq!(report.output_bytes, PCM.len());
    assert_eq!(output.snapshot().pcm, PCM);
    let mut expected = seed;
    expected.push(ChatMessage::new(
        ChatRole::User,
        "final upstream user transcript",
    ));
    expected.push(ChatMessage::new(
        ChatRole::Assistant,
        "generated server-owned answer",
    ));
    let history = completion.history().unwrap();
    assert_eq!(history.messages, expected);
    assert!(history.messages.iter().all(|message| {
        message.tool_calls.is_empty()
            && message.tool_call_id.is_none()
            && message.role != ChatRole::Tool
    }));
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn server_vad_rapid_burst_keeps_final_user_and_generated_assistant_history_once() {
    server_vad_burst_case(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn duplicate_server_vad_start_burst_is_noop_and_does_not_owe_another_cancel() {
    server_vad_burst_case(true).await;
}

#[tokio::test]
async fn actual_output_process_and_cleanup_failures_preserve_independent_diagnostics() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.output_event_capacity = 4;
    let output = Arc::new(RecordingOutput::default());
    output.failure.store(4, Ordering::SeqCst);
    let mut fixture = Fixture::with(listener, config, vec![], vec![], output).await;
    fixture.run().await;
    response(&mut fixture.ws, "output-two-failures", false).await;
    audio(&mut fixture.ws, "output-two-failures", PCM).await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert_failed_owners(&report);
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::OutputProcess)
    );
    assert!(report.cleanup_output_failed);
    assert!(report.secondary_failure);
    assert_eq!(
        report.pipeline_failure,
        Some(GrokPipelineFailure::Execution)
    );
    assert_eq!(
        report.pipeline_cleanup,
        GrokPipelineCleanup::UnknownAfterFailure
    );
    let observed = fixture.completion.wait().await.unwrap();
    assert_eq!(observed.first_failure_stage, report.first_failure_stage);
    assert!(observed.cleanup_output_failed);
    assert!(!observed.successful());
    assert!(observed.owners_joined());
    assert!(fixture.output.snapshot().pcm.is_empty());
    fixture.output.assert_cleaned();
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    let diagnostic = format!("{report:?}");
    assert!(!diagnostic.contains(KEY));
    assert!(!diagnostic.contains("synthetic output process failure"));
    assert!(!diagnostic.contains("synthetic output cleanup failure"));
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn held_trusted_output_setup_respects_session_startup_deadline_and_retains_cleanup_owner() {
    let listener = listener().await;
    let mut config = config(listener.local_addr().unwrap());
    config.limits.session_timeout = Duration::from_millis(250);
    config.limits.drain_timeout = Duration::from_millis(200);
    config.limits.shutdown_timeout = Duration::from_millis(200);
    config.limits.max_tool_duration = Duration::from_millis(200);
    config.connection.limits.connect_timeout = Duration::from_millis(200);
    config.connection.limits.operation_timeout = Duration::from_millis(200);
    config.connection.limits.response_timeout = Duration::from_millis(200);
    config.connection.limits.close_timeout = Duration::from_millis(100);
    let calls = Arc::new(AtomicUsize::new(0));
    let mut local = tool(Arc::new({
        let calls = calls.clone();
        move |call| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move { Ok(call.arguments) })
        }
    }));
    local.handler.timeout = Duration::from_millis(100);
    let output = Arc::new(RecordingOutput::default());
    output.hold.store(3, Ordering::SeqCst);
    let seed = vec![ChatMessage::new(
        ChatRole::User,
        "prepared history must not trigger startup inference",
    )];
    let app = GrokRealtimeApp::new(config, seed.clone(), vec![local], output.clone()).unwrap();
    let completion = app.completion();
    let peer = async {
        let mut ws = ready(&listener, KEY).await;
        output.held().await;
        assert_eq!(output.setup_calls.load(Ordering::SeqCst), 1);
        // Never release the trusted setup gate. The actual startup deadline
        // must cancel its owner, then retain and join the later cleanup work.
        // No seed replay, inference, or tool command is permitted before close.
        closed(&mut ws, false).await;
    };
    let (started, ()) = timeout(WAIT, async { tokio::join!(app.start(), peer) })
        .await
        .expect("held output setup prevented finite startup cleanup");
    assert_eq!(
        started.err(),
        Some(GrokAppError::Timeout("session startup"))
    );
    let report = timeout(WAIT, completion.wait()).await.unwrap().unwrap();
    assert!(!report.successful(), "{report:?}");
    assert_eq!(
        report.primary,
        Some(GrokAppError::Timeout("session startup"))
    );
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::OutputSetup)
    );
    assert!(!report.cleanup_output_failed);
    assert!(report.pipeline_joined, "{report:?}");
    assert!(report.provider_joined, "{report:?}");
    assert!(report.owners_joined(), "{report:?}");
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(report.output_bytes, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(completion.history().unwrap().messages, seed);
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    assert!(output.snapshot().pcm.is_empty());
    output.assert_cleaned();
    // WAIT bounds the whole owned completion. This does not assert that cleanup
    // and joins fit within one session interval or erase a setup-task abort.
    released(listener).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_output_interruption_discards_held_and_late_pcm_then_plays_fresh_same_app_turn() {
    const OLD: &str = "interrupted-playback-response";
    const FRESH: &str = "fresh-playback-response";
    let listener = listener().await;
    let backend = Arc::new(ControlledBackend::new(PlaybackReceipt::Played));
    let app = GrokRealtimeApp::new(
        config(listener.local_addr().unwrap()),
        vec![],
        vec![],
        actual_output(backend.clone()),
    )
    .unwrap();
    let completion = app.completion();
    let (running, mut ws) = timeout(WAIT, async {
        tokio::join!(app.start(), ready(&listener, KEY))
    })
    .await
    .unwrap();
    let running = running.unwrap();
    assert_eq!(read(&mut ws).await["type"], "session.update");
    quiet(&mut ws).await;
    running.run().await.unwrap();
    assert_eq!(read(&mut ws).await["type"], "response.create");
    response(&mut ws, OLD, false).await;
    audio(&mut ws, OLD, &[1, 0].repeat(240)).await;
    timeout(WAIT, backend.entered.notified()).await.unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    assert_eq!(backend.finished.load(Ordering::SeqCst), 0);
    assert_eq!(
        *backend.entered_contexts.lock().unwrap(),
        vec![Some(OLD.to_owned())]
    );
    // Additional old-generation PCM can already be queued behind the held
    // backend operation. No playback receipt has been returned for either chunk.
    audio(&mut ws, OLD, &[2, 0].repeat(240)).await;
    running.interrupt().await.unwrap();
    assert_eq!(read(&mut ws).await["type"], "response.cancel");
    timeout(WAIT, backend.interrupted.notified()).await.unwrap();
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(backend.finished.load(Ordering::SeqCst), 0);
    assert!(backend.pcm.lock().unwrap().is_empty());
    // This is genuine late provider traffic for the interrupted response, not
    // a forged frame or receipt. Its PCM and transcript must stay stale.
    audio(&mut ws, OLD, &[3, 0].repeat(240)).await;
    wire(&mut ws, json!({"type":"response.output_audio_transcript.delta","response_id":OLD,"item_id":format!("speech-{OLD}"),"delta":"late interrupted text must not enter fresh history"})).await;
    wire(
        &mut ws,
        json!({"type":"response.done","response":{"id":OLD,"status":"cancelled","output":[]}}),
    )
    .await;
    quiet(&mut ws).await;
    assert_eq!(backend.calls.load(Ordering::SeqCst), 1);
    running.run().await.unwrap();
    assert_eq!(read(&mut ws).await["type"], "response.create");
    response(&mut ws, FRESH, false).await;
    // The old response stays rejected even after another response is active.
    audio(&mut ws, OLD, &[4, 0].repeat(240)).await;
    audio(&mut ws, FRESH, &[9, 0].repeat(240)).await;
    wire(&mut ws, json!({"type":"response.output_audio_transcript.delta","response_id":FRESH,"item_id":format!("speech-{FRESH}"),"delta":"fresh generated history is not physical playback"})).await;
    response(&mut ws, FRESH, true).await;
    timeout(WAIT, backend.entered.notified()).await.unwrap();
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    assert_eq!(backend.finished.load(Ordering::SeqCst), 0);
    assert_eq!(
        *backend.entered_contexts.lock().unwrap(),
        vec![Some(OLD.to_owned()), Some(FRESH.to_owned())]
    );
    let mut ending = Box::pin(running.end());
    assert!(timeout(NEGATIVE, &mut ending).await.is_err());
    quiet(&mut ws).await;
    // Only the fresh callback survives to consume this permit and return the
    // controlled backend's actual Played receipt. This is not physical audio.
    backend.release.add_permits(1);
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(&mut ending, closed(&mut ws, true))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert!(report.successful(), "{report:?}");
    assert!(report.owners_joined(), "{report:?}");
    assert_eq!(report.generated_speech_responses, 1);
    drop(ending);
    assert_eq!(backend.calls.load(Ordering::SeqCst), 2);
    assert_eq!(backend.finished.load(Ordering::SeqCst), 1);
    assert_eq!(backend.dropped.load(Ordering::SeqCst), 2);
    assert!(backend.interrupts.load(Ordering::SeqCst) >= 1);
    assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
    assert_eq!(*backend.pcm.lock().unwrap(), [9_u8, 0].repeat(240));
    assert_eq!(
        completion.history().unwrap().messages,
        vec![ChatMessage::new(
            ChatRole::Assistant,
            "fresh generated history is not physical playback"
        )]
    );
    let journal = completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    assert!(completion.wait().await.unwrap().successful());
    drop(ws);
    released(listener).await;
}

fn counted_echo(calls: Arc<AtomicUsize>) -> GrokLocalTool {
    tool(Arc::new(move |call| {
        calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Ok(call.arguments) })
    }))
}

async fn rejected_provider_declaration(malformed_arguments: bool) {
    let calls = Arc::new(AtomicUsize::new(0));
    let mut fixture = Fixture::new(vec![counted_echo(calls.clone())]).await;
    fixture.run().await;
    response(&mut fixture.ws, "rejected-declaration", false).await;
    let expected = if malformed_arguments {
        wire(&mut fixture.ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"malformed-item","call_id":"malformed-call","name":"echo","arguments":""}})).await;
        // The outer event is valid JSON. Only the provider's function-argument
        // string is malformed, so the actor rejects it before native mapping.
        wire(&mut fixture.ws, json!({"type":"response.function_call_arguments.done","call_id":"malformed-call","name":"echo","arguments":"{malformed"})).await;
        RealtimeError::Protocol("tool arguments must be a JSON object")
    } else {
        wire(&mut fixture.ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"unadvertised-item","call_id":"unadvertised-call","name":"not-offered","arguments":"{\"n\":1}"}})).await;
        RealtimeError::Protocol("provider requested an unadvertised function")
    };
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert_failed_owners(&report);
    assert_eq!(report.provider_primary, Some(expected));
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::Receive)
    );
    assert_eq!(report.mapping_primary, None);
    assert!(!report.cleanup_output_failed);
    assert_eq!(report.mapped_tool_calls, 0);
    assert_eq!(report.settled_tool_calls, 0);
    assert_eq!(report.generated_speech_responses, 0);
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 0);
    assert!(fixture.output.snapshot().outcomes.is_empty());
    assert!(fixture.output.snapshot().pcm.is_empty());
    assert!(fixture.completion.history().unwrap().messages.is_empty());
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}

#[tokio::test]
async fn unadvertised_provider_call_fails_before_callback_native_lease_or_history() {
    rejected_provider_declaration(false).await;
}

#[tokio::test]
async fn malformed_provider_function_arguments_preserve_actor_protocol_error_without_native_lease()
{
    rejected_provider_declaration(true).await;
}

#[tokio::test]
async fn reused_completed_call_id_fails_in_real_continuation_without_second_native_lease() {
    const CALL: &str = "completed-call-cannot-be-reused";
    let calls = Arc::new(AtomicUsize::new(0));
    let mut fixture = Fixture::new(vec![counted_echo(calls.clone())]).await;
    fixture.run().await;
    call(&mut fixture.ws, "original-call-response", CALL, 7).await;
    response(&mut fixture.ws, "original-call-response", true).await;
    let output = read(&mut fixture.ws).await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], CALL);
    assert_eq!(output["item"]["output"], "{\"n\":7}");
    // This is the actual assistant's sealed continuation, not a caller run or
    // a fabricated native receipt. Its history already includes the real result.
    assert_eq!(read(&mut fixture.ws).await["type"], "response.create");
    let history_before = fixture.completion.history().unwrap().messages;
    assert_eq!(history_before.len(), 2);
    assert_eq!(history_before[0].tool_calls.len(), 1);
    assert_eq!(history_before[0].tool_calls[0].tool_call_id, CALL);
    assert_eq!(history_before[1].role, ChatRole::Tool);
    assert_eq!(history_before[1].tool_call_id.as_deref(), Some(CALL));
    assert_eq!(history_before[1].content, "{\"n\":7}");
    let before = fixture.completion.outcomes();
    assert!(before.active.is_empty());
    assert_eq!(before.total_terminal, 1);
    let outcome_id = before.terminal[0].outcome.id();
    response(&mut fixture.ws, "real-continuation-response", false).await;
    // A fresh declaration in a different response is not a harmless replay of
    // late terminal events. Reject the identity even though its old call settled.
    wire(&mut fixture.ws, json!({"type":"conversation.item.added","item":{"type":"function_call","id":"new-item-reusing-completed-call","call_id":CALL,"name":"echo","arguments":"{\"n\":99}"}})).await;
    let (report, ()) = timeout(WAIT, async {
        tokio::join!(fixture.completion.wait(), closed(&mut fixture.ws, false))
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert_failed_owners(&report);
    assert_eq!(
        report.provider_primary,
        Some(RealtimeError::Protocol("reused tool call identity"))
    );
    assert_eq!(
        report.first_failure_stage,
        Some(GrokAppFailureStage::Receive)
    );
    assert_eq!(report.mapping_primary, None);
    assert!(!report.cleanup_output_failed);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(report.mapped_tool_calls, 1);
    assert_eq!(report.settled_tool_calls, 1);
    let journal = fixture.completion.outcomes();
    assert!(journal.active.is_empty());
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(journal.terminal.len(), 1);
    let outcome = &journal.terminal[0].outcome;
    assert_eq!(outcome.id(), outcome_id);
    assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
    assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
    let captured = fixture.output.snapshot();
    assert_eq!(captured.outcomes.len(), 1);
    assert_eq!(captured.outcomes[0].id(), outcome_id);
    assert!(captured.pcm.is_empty());
    assert_eq!(
        fixture.completion.history().unwrap().messages,
        history_before
    );
    fixture.output.assert_cleaned();
    drop(fixture.ws);
    released(fixture.listener).await;
}
