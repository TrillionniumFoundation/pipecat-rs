//! Native bot PCM -> EvalBotHost -> real WebSocket -> bounded EvalSession capture.
//! Synthetic PCM fixtures exercise transport/ownership, not speech quality or playback.
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*, matcher::ScriptResult, BotAudioChunk, BotAudioLimits, BotAudioRun, Cancellation,
    EvalError, EvalSession, ScenarioFile, ScriptScenario, SessionOptions,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, task::JoinHandle};

type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(5);

fn transport() -> SessionConfig {
    SessionConfig {
        queue_capacity: 64,
        connect_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_millis(300),
        close_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}
fn config() -> EvalBotHostConfig {
    EvalBotHostConfig {
        transport: transport(),
        pipeline: PipelineConfig {
            queue_capacity: 64,
            system_queue_capacity: 64,
            output_capacity: 64,
            ..Default::default()
        },
        event_capacity: 128,
        startup_timeout: Duration::from_secs(2),
        session_timeout: WAIT,
        host_timeout: Duration::from_secs(20),
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    }
}
fn options(stop_bot: bool) -> SessionOptions {
    SessionOptions {
        transport: transport(),
        handshake_timeout: Duration::from_secs(2),
        default_timeout: Duration::from_secs(2),
        max_duration: WAIT,
        stop_bot,
        ..Default::default()
    }
}
fn scenario(turns: Value) -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"bot PCM capture","turns":turns}))
        .unwrap()
        .scenarios
        .remove(0)
}
fn capture_scenario() -> ScriptScenario {
    scenario(json!([
        {"user":"play", "expect":[
            {"event":"llm_response", "text_contains":"fixture response"},
            {"event":"user_transcription", "text_contains":"PCM wire fence"}
        ]},
        {"user":"finish", "send_after":{"event":"user_transcription"},
         "expect":[{"event":"bot_stopped_speaking"}]}
    ]))
}
fn wire(kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai","type":kind,"data":data})
}
fn audio(bytes: &[u8], rate: u32) -> Value {
    wire(
        "eval-bot-audio",
        json!({"audio":STANDARD.encode(bytes),"sampleRate":rate}),
    )
}
fn stopped() -> Value {
    wire("bot-stopped-speaking", json!({}))
}
async fn send(socket: &WebSocketSession, message: Value) -> Result<(), Error> {
    socket
        .sender()
        .send(Packet::Text(message.to_string()))
        .await?;
    Ok(())
}

fn typed_audio(bytes: usize, sample_rate: u32, num_channels: u16) -> AudioRawFrame {
    // Direct construction deliberately permits the invalid typed-host test cases.
    AudioRawFrame {
        audio: vec![0; bytes],
        sample_rate,
        num_channels,
        context_id: None,
    }
}
async fn pcm_wire_fence(ctx: &ProcessorContext, text: &str) -> Result<(), ProcessorError> {
    // A returning flush reaches root admission without roundtripping through
    // this blocked process invocation. Its original event proves earlier PCM
    // already passed the host's capture boundary.
    let delivered = LocalFlushEvent::default();
    ctx.push(
        Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(delivered.clone()),
            returning: true,
            origin: Some("bot-audio-fixture".into()),
        })),
        Direction::Downstream,
    )
    .await?;
    tokio::time::timeout(Duration::from_secs(2), delivered.wait())
        .await
        .map_err(|_| ProcessorError::fatal("bot audio fixture root fence timed out"))?;
    // A synthetic fixture receipt, not STT. The assistant observer uses the
    // SAME queue as the admitted PCM. Only after receiving this marker may a
    // later client input cause system-lane BotStoppedSpeaking.
    ctx.push(
        Frame::new(FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            finalized: true,
            ..Default::default()
        })),
        Direction::Downstream,
    )
    .await
}

// Timeout borrows the owner. Every fallback cancels, then actually awaits it;
// no timeout consumes a JoinHandle and silently detaches the owned task.
async fn join<T>(
    mut task: JoinHandle<T>,
    cancel: &Cancellation,
) -> (Result<T, tokio::task::JoinError>, bool) {
    let (joined, late) = match tokio::time::timeout(WAIT, &mut task).await {
        Ok(joined) => (joined, false),
        Err(_) => {
            cancel.cancel();
            (task.await, true)
        }
    };
    (joined, late)
}

// Inspect lifecycle messages on the real host wire; EvalSession deliberately
// does not expose TTS-response judging or claim transport playback.
async fn host_lifecycle_probe(url: &str) -> Result<Vec<Value>, Error> {
    let mut socket =
        WebSocketSession::connect(&format!("{url}?capture_bot_audio=true"), transport()).await?;
    let work = tokio::time::timeout(WAIT, async {
        let mut ready = wire("client-ready", json!({"version":"2.2.0"}));
        ready["id"] = "lifecycle-ready".into();
        send(&socket, ready).await?;
        let mut events = Vec::new();
        let mut stage = 0;
        loop {
            let Some(Packet::Text(text)) = socket.recv().await else {
                return Err::<Vec<Value>, Error>("host lifecycle probe closed before stopped speaking".into());
            };
            let event: Value = serde_json::from_str(&text)?;
            let next = match (stage, event["type"].as_str()) {
                (0, Some("bot-ready")) => Some("play"),
                (1, Some("user-transcription")) if event["data"]["text"] == "PCM wire fence" => Some("finish"),
                _ => None,
            };
            let complete = stage == 2 && event["type"] == "bot-stopped-speaking";
            events.push(event);
            if let Some(text) = next {
                let mut message = wire("send-text", json!({"content":text,"options":{"run_immediately":true,"audio_response":true}}));
                message["id"] = format!("lifecycle-{stage}").into();
                send(&socket, message).await?;
                stage += 1;
            }
            if complete { return Ok(events); }
        }
    }).await;
    // The timeout only borrowed the socket. Cancellation actually joins its
    // transport owner before any result is returned to the host-owning test.
    let closed = socket.cancel().await;
    let events = work??;
    closed?;
    Ok(events)
}

#[derive(Default)]
struct Trace {
    settings: Mutex<Vec<bool>>,
    configured: Mutex<Vec<bool>>,
    inputs: Mutex<Vec<(String, bool)>>,
    finish_received: Notify,
    completed_fences: AtomicUsize,
    cleaned: AtomicUsize,
    dropped: AtomicUsize,
}
#[derive(Clone, Copy)]
enum Tail {
    StopSpeaking,
    Wait,
    Disconnect,
    Malformed,
    Typed {
        bytes: usize,
        sample_rate: u32,
        num_channels: u16,
    },
}
struct Fixture {
    trace: Arc<Trace>,
    skip_tts: AtomicBool,
    index: usize,
    tail: Tail,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
fn expected_chunks(index: usize) -> Vec<BotAudioChunk> {
    let rate = if index == 0 { 16_000 } else { 48_000 };
    (0..3)
        .map(|part| BotAudioChunk {
            audio: vec![1 + index as u8, 0, 10 + part, 0],
            sample_rate: rate,
        })
        .collect()
}
#[async_trait]
impl FrameProcessor for Fixture {
    fn name(&self) -> &str {
        "native-bot-pcm-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::LlmConfigureOutput { skip_tts } = &frame.data {
            self.skip_tts.store(*skip_tts, Ordering::SeqCst);
            self.trace.configured.lock().unwrap().push(*skip_tts);
        }
        if let FrameData::LlmMessagesAppend(messages) = &frame.data {
            let text = messages
                .messages
                .first()
                .map(|m| m.content.as_str())
                .unwrap_or("");
            let skip = self.skip_tts.load(Ordering::SeqCst);
            self.trace.inputs.lock().unwrap().push((text.into(), skip));
            if text == "play" {
                for data in [
                    FrameData::LlmResponseStart,
                    FrameData::LlmText(TextFrame::new("fixture response")),
                    FrameData::LlmResponseEnd,
                ] {
                    ctx.push(Frame::new(data), Direction::Downstream).await?;
                }
                if !skip {
                    ctx.push(
                        Frame::new(FrameData::BotStartedSpeaking),
                        Direction::Downstream,
                    )
                    .await?;
                    ctx.push(
                        Frame::new(FrameData::TtsStarted(TtsStartedFrame::default())),
                        Direction::Downstream,
                    )
                    .await?;
                    ctx.push(
                        Frame::new(FrameData::TtsText(AggregatedTextFrame {
                            text: TextFrame::new("fixture response"),
                            aggregated_by: "sentence".into(),
                            context_id: None,
                            raw_text: None,
                            will_be_spoken: true,
                        })),
                        Direction::Downstream,
                    )
                    .await?;
                    // The same output families travelling upstream must never be captured.
                    for data in [
                        FrameData::OutputAudio,
                        FrameData::TtsAudio,
                        FrameData::SpeechOutputAudio,
                    ] {
                        ctx.push(
                            Frame::new(data(AudioRawFrame::new(vec![99, 0], 8_000, 1).unwrap())),
                            Direction::Upstream,
                        )
                        .await?;
                    }
                    for (chunk, data) in expected_chunks(self.index).into_iter().zip([
                        FrameData::OutputAudio,
                        FrameData::TtsAudio,
                        FrameData::SpeechOutputAudio,
                    ]) {
                        ctx.push(
                            Frame::new(data(
                                AudioRawFrame::new(chunk.audio, chunk.sample_rate, 1).unwrap(),
                            )),
                            Direction::Downstream,
                        )
                        .await?;
                    }
                    // An input-audio frame at the root is not synthesized output.
                    ctx.push(
                        Frame::new(FrameData::InputAudio(
                            AudioRawFrame::new(vec![88, 0], 24_000, 1).unwrap(),
                        )),
                        Direction::Downstream,
                    )
                    .await?;
                    ctx.push(
                        Frame::new(FrameData::TtsStopped(TtsStoppedFrame::default())),
                        Direction::Downstream,
                    )
                    .await?;
                }
                pcm_wire_fence(ctx, "PCM wire fence").await?;
                self.trace.completed_fences.fetch_add(1, Ordering::SeqCst);
            } else if text == "finish" {
                self.trace.finish_received.notify_one();
                match self.tail {
                    Tail::StopSpeaking => {
                        ctx.push(
                            Frame::new(FrameData::BotStoppedSpeaking),
                            Direction::Downstream,
                        )
                        .await?
                    }
                    Tail::Wait => {}
                    Tail::Disconnect => {
                        ctx.push(
                            Frame::new(FrameData::EndWorker(ReasonFrame::default())),
                            Direction::Downstream,
                        )
                        .await?
                    }
                    Tail::Typed {
                        bytes,
                        sample_rate,
                        num_channels,
                    } => {
                        ctx.push(
                            Frame::new(FrameData::OutputAudio(typed_audio(
                                bytes,
                                sample_rate,
                                num_channels,
                            ))),
                            Direction::Downstream,
                        )
                        .await?;
                        pcm_wire_fence(ctx, "typed PCM wire fence").await?;
                    }
                    Tail::Malformed => {
                        ctx.push(
                            Frame::new(FrameData::OutputTransportMessage(wire(
                                "eval-bot-audio",
                                json!({"audio":"!!!!", "sampleRate":16000}),
                            ))),
                            Direction::Downstream,
                        )
                        .await?
                    }
                }
            } else if text == "stop" {
                ctx.push(
                    Frame::new(FrameData::BotStoppedSpeaking),
                    Direction::Downstream,
                )
                .await?;
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.trace.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Factory {
    trace: Arc<Trace>,
    tail: Tail,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        let index = {
            let mut seen = self.trace.settings.lock().unwrap();
            let index = seen.len();
            seen.push(settings.skip_tts);
            index
        };
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Fixture {
                trace: self.trace.clone(),
                skip_tts: AtomicBool::new(true),
                index,
                tail: self.tail,
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
struct Host {
    url: String,
    task: JoinHandle<Result<EvalBotHostReport, BotHostError>>,
    cancel: Cancellation,
    trace: Arc<Trace>,
}
async fn host(permission: Option<bool>, tail: Tail) -> Host {
    host_with_config(permission, tail, config()).await
}
async fn host_with_config(permission: Option<bool>, tail: Tail, config: EvalBotHostConfig) -> Host {
    let mut host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), config)
        .await
        .unwrap();
    if let Some(permission) = permission {
        host = host.with_audio_output(permission);
    }
    let url = format!("ws://{}/", host.local_addr().unwrap());
    let trace = Arc::new(Trace::default());
    let factory = Factory {
        trace: trace.clone(),
        tail,
    };
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let task = tokio::spawn(async move { host.serve(&factory, c).await });
    Host {
        url,
        task,
        cancel,
        trace,
    }
}
async fn settle(
    host: Host,
    cancel_now: bool,
) -> (Result<EvalBotHostReport, Error>, bool, Arc<Trace>) {
    if cancel_now {
        host.cancel.cancel();
    }
    let (result, late) = join(host.task, &host.cancel).await;
    (
        result
            .map_err(|e| -> Error { e.into() })
            .and_then(|r| r.map_err(Into::into)),
        late,
        host.trace,
    )
}
async fn capture(
    url: &str,
    s: ScriptScenario,
    opt: SessionOptions,
    cancel: Cancellation,
    limits: BotAudioLimits,
) -> Result<BotAudioRun, EvalError> {
    EvalSession::new(s, url, opt)?
        .run_with_bot_audio(cancel, limits)
        .await
}
fn assert_clean(trace: &Trace, count: usize) {
    assert_eq!(trace.completed_fences.load(Ordering::SeqCst), count);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), count);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), count);
}
fn assert_failure(result: &ScriptResult, kind: &str, reason: &str) {
    assert!(!result.passed, "{result:?}");
    assert_eq!(result.failures.len(), 1, "{result:?}");
    assert_eq!(result.failures[0].kind, kind, "{result:?}");
    assert_eq!(result.failures[0].reason, reason, "{result:?}");
}

#[tokio::test]
async fn real_host_two_runs_capture_all_output_families_at_original_rates() {
    let s = capture_scenario();
    let h = host(Some(true), Tail::StopSpeaking).await;
    let mut runs = Vec::new();
    for _ in 0..2 {
        runs.push(
            capture(
                &h.url,
                s.clone(),
                options(false),
                Cancellation::new(),
                BotAudioLimits::default(),
            )
            .await,
        );
    }
    let lifecycle = host_lifecycle_probe(&h.url).await;
    let (report, late, trace) = settle(h, true).await;
    // All behavioral assertions are after actual host/pipeline ownership cleanup.
    assert!(!late);
    assert_eq!(report.unwrap().sessions, 3);
    assert_clean(&trace, 3);
    assert_eq!(*trace.settings.lock().unwrap(), [false, false, false]);
    assert_eq!(*trace.configured.lock().unwrap(), [false, false, false]);
    assert_eq!(
        *trace.inputs.lock().unwrap(),
        [
            ("play".into(), false),
            ("finish".into(), false),
            ("play".into(), false),
            ("finish".into(), false),
            ("play".into(), false),
            ("finish".into(), false),
        ]
    );
    let lifecycle = lifecycle.unwrap();
    for kind in [
        "bot-tts-started",
        "bot-tts-stopped",
        "bot-started-speaking",
        "bot-stopped-speaking",
    ] {
        assert_eq!(
            lifecycle
                .iter()
                .filter(|event| event["type"] == kind)
                .count(),
            1,
            "{lifecycle:?}"
        );
    }
    let tts_start = lifecycle
        .iter()
        .position(|event| event["type"] == "bot-tts-started")
        .unwrap();
    let tts_stop = lifecycle
        .iter()
        .position(|event| event["type"] == "bot-tts-stopped")
        .unwrap();
    assert!(tts_start < tts_stop);
    let pcm: Vec<_> = lifecycle
        .iter()
        .filter(|event| event["type"] == "eval-bot-audio")
        .map(|event| event["data"].clone())
        .collect();
    assert_eq!(pcm, expected_chunks(2).into_iter().map(|chunk| json!({"audio":STANDARD.encode(chunk.audio), "sampleRate":chunk.sample_rate})).collect::<Vec<_>>());
    assert_eq!(lifecycle.last().unwrap()["type"], "bot-stopped-speaking");
    for (index, run) in runs.into_iter().enumerate() {
        let run = run.unwrap();
        assert!(run.result.passed, "{run:?}");
        assert_eq!(run.chunks, expected_chunks(index));
        assert_eq!(
            run.result.turns[1].expectations[0].event_name,
            "bot_stopped_speaking"
        );
        assert!(run
            .result
            .events_seen
            .iter()
            .any(|e| e["type"] == "bot_started_speaking"));
        assert!(run
            .result
            .events_seen
            .iter()
            .any(|e| e["type"] == "bot_stopped_speaking"));
    }
}

#[tokio::test]
async fn audio_permission_alone_preserves_default_text_run_and_skip_tts() {
    for permission in [None, Some(false), Some(true)] {
        let s = scenario(json!([{"user":"play", "expect":[
            {"event":"llm_response", "text_contains":"fixture response"},
            {"event":"user_transcription", "text_contains":"PCM wire fence"}
        ]}]));
        let h = host(permission, Tail::StopSpeaking).await;
        let result = match EvalSession::new(s, &h.url, options(false)) {
            Ok(session) => Ok(session.run(Cancellation::new()).await),
            Err(error) => Err(error),
        };
        let (report, late, trace) = settle(h, true).await;
        assert!(!late);
        assert_eq!(report.unwrap().sessions, 1);
        assert_clean(&trace, 1);
        let result = result.unwrap();
        assert!(result.passed, "{result:?}");
        assert_eq!(*trace.settings.lock().unwrap(), [true]);
        assert_eq!(*trace.configured.lock().unwrap(), [true]);
        assert_eq!(*trace.inputs.lock().unwrap(), [("play".into(), true)]);
        assert!(!result
            .events_seen
            .iter()
            .any(|e| e["type"] == "bot_started_speaking"));
    }
    // Supplying a capture flag in the original URL does not opt ordinary run()
    // into capture. Unsolicited PCM still fails the original text-only contract.
    let s = scenario(json!([{"user":"play", "expect":[{"event":"bot_stopped_speaking"}]}]));
    let p = peer(vec![audio(&[7, 0], 16_000)]).await;
    let result = match EvalSession::new(s, &p.url, options(true)) {
        Ok(session) => Ok(session.run(Cancellation::new()).await),
        Err(error) => {
            p.cancel.cancel();
            Err(error)
        }
    };
    let (trace, late) = join(p.task, &p.cancel).await;
    assert!(!late);
    let trace = trace.unwrap().unwrap();
    assert_peer_closed(&trace);
    let url = url::Url::parse(&format!("ws://fixture{}", trace.request)).unwrap();
    assert_eq!(
        url.query_pairs().into_owned().collect::<Vec<_>>(),
        [
            ("custom".into(), "kept".into()),
            ("skip_tts".into(), "true".into()),
        ]
    );
    let sent = trace
        .messages
        .iter()
        .find(|m| m["type"] == "send-text")
        .unwrap();
    assert_eq!(
        sent["data"]["options"],
        json!({"run_immediately":true,"audio_response":false})
    );
    assert_failure(
        &result.unwrap(),
        "error",
        "unsupported native evaluation feature: bot audio received in text-only evaluation",
    );
}

#[tokio::test]
async fn capture_handshake_requires_permission_and_rejects_conflicting_flags() {
    for (permission, query) in [
        (None, "capture_bot_audio=true"),
        (Some(false), "capture_bot_audio=true"),
        (Some(true), "capture_bot_audio=true&skip_tts=true"),
        (Some(true), "capture_bot_audio=true&capture_bot_audio=true"),
    ] {
        let h = host(permission, Tail::StopSpeaking).await;
        let connected = WebSocketSession::connect(&format!("{}?{query}", h.url), transport()).await;
        let error = match connected {
            Ok(socket) => {
                let closed = socket.cancel().await;
                format!("unexpectedly connected; close={closed:?}")
            }
            Err(error) => error.to_string(),
        };
        let (report, late, trace) = settle(h, true).await;
        assert!(!late);
        let report = report.unwrap();
        assert_eq!(report.rejected_handshakes, 1);
        assert_eq!(report.sessions, 0);
        assert!(trace.settings.lock().unwrap().is_empty());
        assert_clean(&trace, 0);
        assert_eq!(error, "WebSocket: HTTP error: 403 Forbidden");
    }
}

#[tokio::test]
async fn cancellation_and_disconnect_retain_admitted_audio_and_join_real_host_owners() {
    for tail in [Tail::Wait, Tail::Disconnect] {
        let h = host(Some(true), tail).await;
        let url = h.url.clone();
        let cancel = Cancellation::new();
        let owned_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            capture(
                &url,
                capture_scenario(),
                options(true),
                owned_cancel,
                BotAudioLimits::default(),
            )
            .await
        });
        let entered = tokio::time::timeout(WAIT, h.trace.finish_received.notified())
            .await
            .is_ok();
        if matches!(tail, Tail::Wait) || !entered {
            cancel.cancel();
        }
        let (run, session_late) = join(task, &cancel).await;
        let (report, host_late, trace) =
            settle(h, matches!(tail, Tail::Disconnect) || !entered).await;
        assert!(entered, "second input did not prove the PCM wire fence");
        assert!(!session_late && !host_late);
        let report = report.unwrap();
        assert_eq!(report.sessions, 1);
        assert_clean(&trace, 1);
        let run = run.unwrap().unwrap();
        assert_eq!(run.chunks, expected_chunks(0));
        match tail {
            Tail::Wait => {
                assert_failure(&run.result, "cancelled", "evaluation cancelled");
                assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
            }
            Tail::Disconnect => {
                assert_eq!(report.last_exit, Some(EvalBotExit::Ended));
                assert_failure(
                    &run.result,
                    "disconnected",
                    "bot disconnected before evaluation completed",
                );
            }
            Tail::StopSpeaking | Tail::Malformed | Tail::Typed { .. } => unreachable!(),
        }
    }
}

#[derive(Debug, Default)]
struct PeerTrace {
    request: String,
    messages: Vec<Value>,
    // A peer-requested eval-cancel may close the client before our close ACK.
    close_reset_after_cancel: bool,
}
struct Peer {
    url: String,
    task: JoinHandle<Result<PeerTrace, Error>>,
    cancel: Cancellation,
}
fn connection_reset(error: &(dyn std::error::Error + 'static)) -> bool {
    let mut current = Some(error);
    while let Some(error) = current {
        if let Some(error) = error.downcast_ref::<std::io::Error>() {
            return error.kind() == std::io::ErrorKind::ConnectionReset;
        }
        current = error.source();
    }
    false
}
async fn peer(output: Vec<Value>) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "ws://{}/eval?custom=kept&skip_tts=false&capture_bot_audio=false",
        listener.local_addr().unwrap()
    );
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let request = Arc::new(Mutex::new(String::new()));
    let captured = request.clone();
    let task = tokio::spawn(async move {
        let accepted = tokio::select! {
            _ = c.cancelled() => return Ok(PeerTrace::default()),
            result = listener.accept() => result?,
        };
        let mut socket =
            WebSocketSession::accept_with_request_policy(accepted.0, transport(), move |r| {
                *captured.lock().unwrap() = r.uri().to_string();
                true
            })
            .await?;
        let mut trace = PeerTrace {
            request: request.lock().unwrap().clone(),
            messages: Vec::new(),
            close_reset_after_cancel: false,
        };
        let mut output = Some(output);
        let work: Result<(), Error> = async {
            loop {
                let packet =
                    tokio::select! { _ = c.cancelled() => break, packet = socket.recv() => packet };
                let Some(packet) = packet else {
                    break;
                };
                let Packet::Text(text) = packet else {
                    return Err("unexpected binary input".into());
                };
                let message: Value = serde_json::from_str(&text)?;
                trace.messages.push(message.clone());
                match message["type"].as_str() {
                    Some("client-ready") => {
                        send(&socket, wire("bot-ready", json!({"version":"2.2.0"}))).await?
                    }
                    Some("send-text") => {
                        for message in output.take().unwrap_or_default() {
                            send(&socket, message).await?;
                        }
                    }
                    Some("client-message") if message["data"]["t"] == "eval-cancel" => break,
                    _ => {}
                }
            }
            Ok(())
        }
        .await;
        let closed = socket.finish().await;
        work?;
        if let Err(error) = closed {
            let requested_cancel = trace.messages.last().is_some_and(|message| {
                message["type"] == "client-message" && message["data"]["t"] == "eval-cancel"
            });
            // Only this precise peer-close race is ordinary cancellation. Send,
            // parse, write/close deadline, protocol and task errors still fail.
            if requested_cancel && connection_reset(&error) {
                trace.close_reset_after_cancel = true;
            } else {
                return Err(error.into());
            }
        }
        Ok(trace)
    });
    Peer { url, task, cancel }
}
async fn peer_case(
    output: Vec<Value>,
    limits: BotAudioLimits,
) -> (
    Result<BotAudioRun, EvalError>,
    Result<PeerTrace, Error>,
    bool,
) {
    let s = scenario(json!([{"user":"play", "expect":[{"event":"bot_stopped_speaking"}]}]));
    let p = peer(output).await;
    let run = capture(&p.url, s, options(true), Cancellation::new(), limits).await;
    if run.is_err() {
        p.cancel.cancel();
    }
    let (trace, late) = join(p.task, &p.cancel).await;
    (
        run,
        trace.map_err(|e| -> Error { e.into() }).and_then(|r| r),
        late,
    )
}
fn assert_peer_closed(trace: &PeerTrace) {
    let last = trace.messages.last().unwrap();
    assert_eq!(last["data"]["t"], "eval-cancel");
    if trace.close_reset_after_cancel {
        assert_eq!(last["type"], "client-message", "{trace:?}");
    }
}

#[tokio::test]
async fn capture_wire_flags_and_audio_response_are_explicit_and_stop_at_scenario_completion() {
    let (run, trace, late) = peer_case(
        vec![audio(&[7, 0], 8_000), stopped(), audio(&[9, 0], 48_000)],
        BotAudioLimits {
            max_bytes: 2,
            max_chunks: 1,
        },
    )
    .await;
    assert!(!late);
    let trace = trace.unwrap();
    assert_peer_closed(&trace);
    let url = url::Url::parse(&format!("ws://fixture{}", trace.request)).unwrap();
    assert_eq!(
        url.query_pairs().into_owned().collect::<Vec<_>>(),
        [
            ("custom".into(), "kept".into()),
            ("capture_bot_audio".into(), "true".into()),
        ]
    );
    let sent = trace
        .messages
        .iter()
        .find(|m| m["type"] == "send-text")
        .unwrap();
    assert_eq!(
        sent["data"]["options"],
        json!({"run_immediately":true,"audio_response":true})
    );
    let run = run.unwrap();
    assert!(run.result.passed, "{run:?}");
    assert_eq!(
        run.chunks,
        [BotAudioChunk {
            audio: vec![7, 0],
            sample_rate: 8_000
        }]
    );
}

#[tokio::test]
async fn malformed_audio_fails_atomically_without_discarding_the_valid_prefix() {
    let pcm_error =
        "invalid scenario: bot audio requires nonempty whole PCM16 samples at 8/16/24/48 kHz";
    let rate_error = "invalid scenario: bot audio sampleRate must be 8000, 16000, 24000 or 48000";
    for (data, reason) in [
        (
            json!({"audio":"!!!!", "sampleRate":16000}),
            "invalid scenario: invalid bot audio base64",
        ),
        (json!({"audio":"", "sampleRate":16000}), pcm_error),
        (json!({"audio":"AQ==", "sampleRate":16000}), pcm_error),
        (
            json!({"sampleRate":16000}),
            "invalid scenario: bot audio requires base64 audio",
        ),
        (json!({"audio":"AQA=", "sampleRate":44100}), rate_error),
        (json!({"audio":"AQA=", "sampleRate":16000.5}), rate_error),
        (json!({"audio":"AQA=", "sampleRate":"16000"}), rate_error),
        (json!({"audio":"AQA="}), rate_error),
    ] {
        let (run, trace, late) = peer_case(
            vec![
                audio(&[7, 0], 24_000),
                wire("eval-bot-audio", data),
                audio(&[9, 0], 16_000),
                stopped(),
            ],
            BotAudioLimits::default(),
        )
        .await;
        assert!(!late);
        assert_peer_closed(&trace.unwrap());
        let run = run.unwrap();
        assert_failure(&run.result, "error", reason);
        assert_eq!(
            run.chunks,
            [BotAudioChunk {
                audio: vec![7, 0],
                sample_rate: 24_000
            }]
        );
    }
}

#[tokio::test]
async fn byte_and_chunk_budgets_are_inclusive_and_reject_whole_chunks() {
    for (limits, second, passes) in [
        (
            BotAudioLimits {
                max_bytes: 4,
                max_chunks: 2,
            },
            vec![9, 0],
            true,
        ),
        (
            BotAudioLimits {
                max_bytes: 3,
                max_chunks: 2,
            },
            vec![9, 0],
            false,
        ),
        (
            BotAudioLimits {
                max_bytes: 4,
                max_chunks: 2,
            },
            vec![9, 0, 10, 0],
            false,
        ),
        (
            BotAudioLimits {
                max_bytes: 8,
                max_chunks: 1,
            },
            vec![9, 0],
            false,
        ),
        (
            BotAudioLimits {
                max_bytes: 16 * 1024 * 1024,
                max_chunks: 16_384,
            },
            vec![9, 0],
            true,
        ),
    ] {
        let (run, trace, late) = peer_case(
            vec![audio(&[7, 0], 16_000), audio(&second, 48_000), stopped()],
            limits,
        )
        .await;
        assert!(!late);
        assert_peer_closed(&trace.unwrap());
        let run = run.unwrap();
        let mut expected = vec![BotAudioChunk {
            audio: vec![7, 0],
            sample_rate: 16_000,
        }];
        if passes {
            expected.push(BotAudioChunk {
                audio: second,
                sample_rate: 48_000,
            });
            assert!(run.result.passed, "{run:?}");
        } else {
            assert_failure(
                &run.result,
                "limit_exceeded",
                "evaluation limit exceeded: bot audio capture",
            );
        }
        assert_eq!(run.chunks, expected);
    }
}

#[tokio::test]
async fn invalid_capture_limits_fail_before_connecting() {
    let defaults = BotAudioLimits::default();
    assert_eq!(defaults.max_bytes, 8 * 1024 * 1024);
    assert_eq!(defaults.max_chunks, 4096);
    for limits in [
        BotAudioLimits {
            max_bytes: 0,
            ..defaults
        },
        BotAudioLimits {
            max_chunks: 0,
            ..defaults
        },
        BotAudioLimits {
            max_bytes: 16 * 1024 * 1024 + 1,
            ..defaults
        },
        BotAudioLimits {
            max_chunks: 16_385,
            ..defaults
        },
        BotAudioLimits {
            max_bytes: usize::MAX,
            max_chunks: usize::MAX,
        },
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", listener.local_addr().unwrap());
        let result = capture(
            &url,
            capture_scenario(),
            options(true),
            Cancellation::new(),
            limits,
        )
        .await;
        let accepted = tokio::time::timeout(Duration::from_millis(1), listener.accept()).await;
        let contacted = accepted.is_ok();
        drop(accepted);
        assert!(
            matches!(result, Err(EvalError::Scenario(ref reason)) if reason == "invalid bot audio capture limits"),
            "{result:?}"
        );
        assert!(!contacted, "invalid limits contacted the endpoint");
    }
}

#[tokio::test]
async fn native_host_application_audio_envelopes_cannot_bypass_client_validation() {
    let s = capture_scenario();
    let h = host(Some(true), Tail::Malformed).await;
    let run = capture(
        &h.url,
        s,
        options(true),
        Cancellation::new(),
        BotAudioLimits::default(),
    )
    .await;
    let (report, late, trace) = settle(h, false).await;
    assert!(!late);
    let report = report.unwrap();
    assert_eq!(report.sessions, 1);
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
    assert_clean(&trace, 1);
    let run = run.unwrap();
    assert_failure(
        &run.result,
        "error",
        "invalid scenario: invalid bot audio base64",
    );
    assert_eq!(run.chunks, expected_chunks(0));
}

#[tokio::test]
async fn typed_host_pcm_validation_and_frame_budget_reject_atomically_at_the_exact_boundary() {
    const WIRE_LIMIT: usize = 1024;
    let native_size = |bytes| {
        let frame = Frame::new(FrameData::OutputAudio(typed_audio(bytes, 8_000, 1)));
        serde_json::to_vec(&frame.data).unwrap().len()
            + serde_json::to_vec(&frame.metadata).unwrap().len()
    };
    let boundary = (2..WIRE_LIMIT)
        .step_by(2)
        .take_while(|bytes| native_size(*bytes) <= WIRE_LIMIT)
        .last()
        .unwrap();
    assert!(native_size(boundary) <= WIRE_LIMIT);
    assert!(native_size(boundary + 2) > WIRE_LIMIT);
    // The native numeric-u8 JSON budget is stricter than base64 expansion:
    // even zero bytes occupy >=2 JSON characters each, versus ~4/3 in base64.
    // At the host's minimum 1024-byte limit, every PCM payload that could fail
    // base64_len + 128 already fails native serialization first. Do not claim
    // this case reaches the later defensive base64 guard.
    let conservative_overflow = (2..WIRE_LIMIT)
        .step_by(2)
        .find(|bytes| bytes.div_ceil(3) * 4 + 128 > WIRE_LIMIT)
        .unwrap();
    let overflow = FrameData::OutputAudio(typed_audio(conservative_overflow, 8_000, 1));
    assert!(serde_json::to_vec(&overflow).unwrap().len() > WIRE_LIMIT);
    let boundary_overflow = FrameData::OutputAudio(typed_audio(boundary + 2, 8_000, 1));
    assert!(serde_json::to_vec(&boundary_overflow).unwrap().len() <= WIRE_LIMIT);
    let turns = json!([
        {"user":"play", "expect":[
            {"event":"llm_response", "text_contains":"fixture response"},
            {"event":"user_transcription", "text_contains":"PCM wire fence"}
        ]},
        {"user":"finish", "send_after":{"event":"user_transcription"},
         "expect":[{"event":"user_transcription", "text_contains":"typed PCM wire fence"}]},
        {"user":"stop", "send_after":{"event":"user_transcription"},
         "expect":[{"event":"bot_stopped_speaking"}]}
    ]);
    for (bytes, sample_rate, num_channels, expected_error) in [
        (4, 8_000, 2, Some("bot audio capture requires mono PCM16")),
        (2, 44_100, 1, Some("invalid scenario: bot audio requires nonempty whole PCM16 samples at 8/16/24/48 kHz")),
        (boundary + 2, 8_000, 1, Some("eval frame envelope byte limit exceeded")),
        (conservative_overflow, 8_000, 1, Some("eval host limit exceeded: serialized bytes")),
        (boundary, 8_000, 1, None),
    ] {
        let s = scenario(turns.clone());
        let cfg = EvalBotHostConfig {
            transport: SessionConfig { max_message_bytes: WIRE_LIMIT, ..transport() },
            ..config()
        };
        let h = host_with_config(Some(true), Tail::Typed { bytes, sample_rate, num_channels }, cfg).await;
        let run = capture(&h.url, s, options(true), Cancellation::new(), BotAudioLimits::default()).await;
        let (report, late, trace) = settle(h, false).await;
        assert!(!late);
        assert_clean(&trace, 1);
        let run = run.unwrap();
        let mut expected = expected_chunks(0);
        match expected_error {
            Some(reason) => {
                let error = report.expect_err("invalid typed PCM was accepted");
                let error = error.downcast_ref::<BotHostError>().expect("expected a host error");
                assert!(matches!(error, BotHostError::Pipeline(PipelineError::Execution(actual)) if actual == reason), "{error:?}");
                assert!(!run.result.passed, "{run:?}");
            }
            None => {
                let report = report.unwrap();
                assert_eq!(report.sessions, 1);
                assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
                assert!(run.result.passed, "{run:?}");
                expected.push(BotAudioChunk { audio: vec![0; bytes], sample_rate });
            }
        }
        assert_eq!(run.chunks, expected);
    }
}
