//! Genuine Silero + recorded PCM + real loopback WebSocket/HTTP wiring.
//! HTTP replies are scripted protocol fixtures, not evidence of STT accuracy.
#![cfg(feature = "bot-audio-transcription")]

use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::VadParams;
use pipecat_evals::{
    matcher::ScriptResult, BotTranscriptionConfig, Cancellation, EvalSession, ScenarioFile,
    ScriptScenario, SessionOptions,
};
use pipecat_speech::{SpeechConfig, TranscriptionRequest};
use pipecat_transports::{Packet, SessionConfig, TransportError, WebSocketSession};
use pipecat_vad::{RuntimeOptions, SileroModel, SileroVadAnalyzer};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Notify},
    task::JoinHandle,
};

type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(10);
const PCM: &[u8] = include_bytes!("../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16");
// Leave room for the real 400 ms loudness window, smoothing, and VAD start
// hysteresis. This 960 ms clip remains below the one-second wire-frame cap.
const CLIP_BYTES: usize = 30_720;

fn analyzer() -> SileroVadAnalyzer {
    let runtime = RuntimeOptions {
        library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0")),
    };
    let path = std::env::var_os("PIPECAT_SILERO_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
        });
    let model = SileroModel::load(&runtime, path)
        .expect("genuine ONNX runtime/model required; see pipecat-vad README");
    SileroVadAnalyzer::new(
        model,
        16_000,
        VadParams {
            stop_secs: 0.4,
            ..Default::default()
        },
    )
    .unwrap()
}
fn transport() -> SessionConfig {
    SessionConfig {
        queue_capacity: 64,
        connect_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_millis(500),
        close_timeout: Duration::from_millis(150),
        ..Default::default()
    }
}
fn options() -> SessionOptions {
    SessionOptions {
        transport: transport(),
        handshake_timeout: Duration::from_secs(2),
        default_timeout: Duration::from_secs(7),
        bot_quiet_timeout: Duration::from_secs(7),
        max_duration: WAIT,
        stop_bot: true,
        ..Default::default()
    }
}
fn scenario(turns: Value) -> ScriptScenario {
    let mut scenario = ScenarioFile::from_value(json!({
        "name": "native spoken response protocol fixture", "turns": turns
    }))
    .unwrap()
    .scenarios
    .remove(0);
    // The ordinary loader intentionally retains its text-only response alias.
    // Explicit typed construction selects the opt-in acoustic response event.
    for turn in &mut scenario.turns {
        for expectation in &mut turn.expect {
            if expectation.event == "llm_response" {
                expectation.event = "response".into();
            }
        }
    }
    scenario
}
fn expected(text: &str) -> ScriptScenario {
    scenario(json!([{"user":"play the fixture", "expect":[
        {"event":"response", "text_contains":text, "text_excludes":"forbidden"}
    ]}]))
}
fn wire(kind: &str, data: Value) -> Value {
    json!({"label":"rtvi-ai", "type":kind, "data":data})
}
fn audio(bytes: &[u8], rate: u32) -> Value {
    wire(
        "eval-bot-audio",
        json!({"audio":STANDARD.encode(bytes), "sampleRate":rate}),
    )
}
fn speech() -> Value {
    audio(&PCM[..CLIP_BYTES], 16_000)
}
fn marker(text: &str) -> Value {
    wire("user-transcription", json!({"text":text, "final":true}))
}
fn start_speech() -> Vec<Value> {
    vec![wire("bot-llm-started", json!({})), speech()]
}
async fn send(socket: &WebSocketSession, value: Value) -> Result<(), Error> {
    socket
        .sender()
        .send(Packet::Text(value.to_string()))
        .await?;
    Ok(())
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

// WebSocketSession::Drop aborts its I/O owner rather than awaiting a close
// handshake. Only the explicit future-drop test accepts this precise outcome.
// The inner enum is not re-exported by pipecat-transports; match its typed
// WebSocket wrapper and the complete tungstenite 0.28 Display string.
fn reset_without_closing_handshake(error: &TransportError) -> bool {
    matches!(error, TransportError::WebSocket(inner)
        if inner.to_string() == "WebSocket protocol error: Connection reset without closing handshake")
}

#[derive(Debug, Default)]
struct WireTrace {
    request: String,
    inputs: Vec<Value>,
    cancelled: bool,
    finished: bool,
    reset_after_cancel: bool,
    reset_after_drop: bool,
}
struct Command {
    messages: Vec<Value>,
    sent: Arc<Notify>,
}
struct Peer {
    url: String,
    command: mpsc::Sender<Command>,
    inputs: Arc<AtomicUsize>,
    input_changed: Arc<Notify>,
    task: JoinHandle<Result<WireTrace, Error>>,
    known_cancel: Arc<AtomicBool>,
    known_drop: Arc<AtomicBool>,
}
async fn websocket(outputs: Vec<Vec<Value>>) -> Result<Peer, Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let url = format!(
        "ws://{}/eval?custom=kept&skip_tts=true&capture_bot_audio=false",
        listener.local_addr()?
    );
    let (command, mut commands) = mpsc::channel::<Command>(16);
    let inputs = Arc::new(AtomicUsize::new(0));
    let count = inputs.clone();
    let input_changed = Arc::new(Notify::new());
    let changed = input_changed.clone();
    let known_cancel = Arc::new(AtomicBool::new(false));
    let cancelled = known_cancel.clone();
    let known_drop = Arc::new(AtomicBool::new(false));
    let dropped = known_drop.clone();
    let task = tokio::spawn(async move {
        tokio::time::timeout(WAIT, async {
            let (stream, _) = listener.accept().await?;
            let request = Arc::new(Mutex::new(String::new()));
            let captured = request.clone();
            let mut socket = WebSocketSession::accept_with_request_policy(stream, transport(), move |r| {
                *captured.lock().unwrap() = r.uri().to_string();
                true
            }).await?;
            let mut trace = WireTrace { request: request.lock().unwrap().clone(), ..Default::default() };
            let mut outputs = outputs.into_iter();
            let work: Result<(), Error> = async {
                loop {
                    tokio::select! {
                        command = commands.recv(), if !commands.is_closed() => {
                            if let Some(command) = command {
                                for message in command.messages { send(&socket, message).await?; }
                                command.sent.notify_one();
                            }
                        }
                        packet = socket.recv() => {
                            let Some(packet) = packet else { break; };
                            let Packet::Text(text) = packet else { return Err("unexpected binary input".into()); };
                            let value: Value = serde_json::from_str(&text)?;
                            match value["type"].as_str() {
                                Some("client-ready") => send(&socket, wire("bot-ready", json!({"version":"2.2.0"}))).await?,
                                Some("send-text") => {
                                    trace.inputs.push(value);
                                    count.fetch_add(1, Ordering::SeqCst);
                                    changed.notify_one();
                                    for message in outputs.next().unwrap_or_default() { send(&socket, message).await?; }
                                }
                                Some("client-message") if value["data"]["t"] == "eval-cancel" => { trace.cancelled = true; break; }
                                _ => {}
                            }
                        }
                    }
                }
                Ok(())
            }.await;
            let closed = socket.finish().await;
            work?;
            match closed {
                Ok(()) => trace.finished = true,
                Err(error) if (trace.cancelled || cancelled.load(Ordering::SeqCst)) && connection_reset(&error) => {
                    trace.reset_after_cancel = true;
                }
                Err(error) if dropped.load(Ordering::SeqCst) && reset_without_closing_handshake(&error) => {
                    trace.reset_after_drop = true;
                }
                Err(error) => return Err(error.into()),
            }
            Ok(trace)
        }).await.map_err(|_| -> Error { "WebSocket fixture exceeded deadline".into() })?
    });
    Ok(Peer {
        url,
        command,
        inputs,
        input_changed,
        task,
        known_cancel,
        known_drop,
    })
}
impl Peer {
    async fn send(&self, messages: Vec<Value>) -> Result<(), Error> {
        let sent = Arc::new(Notify::new());
        self.command
            .send(Command {
                messages,
                sent: sent.clone(),
            })
            .await?;
        tokio::time::timeout(WAIT, sent.notified()).await?;
        Ok(())
    }
    async fn wait_inputs(&self, wanted: usize) -> Result<(), Error> {
        tokio::time::timeout(WAIT, async {
            while self.inputs.load(Ordering::SeqCst) < wanted {
                self.input_changed.notified().await;
            }
        })
        .await?;
        Ok(())
    }
}

#[derive(Default)]
struct HttpGate {
    received: Notify,
    release: Notify,
    completed: Notify,
    request: Mutex<Vec<u8>>,
    replied: AtomicBool,
    closed: AtomicBool,
    expected_disconnect: AtomicBool,
}
struct HttpPeer {
    config: SpeechConfig,
    gates: Vec<Arc<HttpGate>>,
    cancel: Cancellation,
    task: JoinHandle<Result<(), Error>>,
}
async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    const HEADER_LIMIT: usize = 16 * 1024;
    const BODY_LIMIT: usize = 512 * 1024;
    let mut bytes = Vec::new();
    let mut block = [0; 4096];
    let end = loop {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("HTTP EOF before headers".into());
        }
        bytes.extend_from_slice(&block[..n]);
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            if i + 4 > HEADER_LIMIT {
                return Err("HTTP header budget exceeded".into());
            }
            break i + 4;
        }
        if bytes.len() > HEADER_LIMIT {
            return Err("HTTP header budget exceeded".into());
        }
    };
    let headers = std::str::from_utf8(&bytes[..end])?;
    let length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .ok_or("HTTP request lacks content length")?
        .parse::<usize>()?;
    if length > BODY_LIMIT {
        return Err("HTTP body budget exceeded".into());
    }
    let total = end.checked_add(length).ok_or("HTTP length overflow")?;
    while bytes.len() < total {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("HTTP EOF before body".into());
        }
        bytes.extend_from_slice(&block[..n]);
    }
    if bytes.len() != total {
        return Err("unexpected HTTP bytes after body".into());
    }
    validate_wav(&bytes)?;
    Ok(bytes)
}
fn validate_wav(request: &[u8]) -> Result<(), Error> {
    if !request.starts_with(b"POST /audio/transcriptions ") {
        return Err("wrong STT endpoint".into());
    }
    let offset = request
        .windows(4)
        .position(|w| w == b"RIFF")
        .ok_or("multipart lacks WAV")?;
    let wav = request.get(offset..offset + 44).ok_or("short WAV header")?;
    if &wav[8..12] != b"WAVE"
        || &wav[12..16] != b"fmt "
        || &wav[36..40] != b"data"
        || u16::from_le_bytes([wav[20], wav[21]]) != 1
        || u16::from_le_bytes([wav[22], wav[23]]) != 1
        || u32::from_le_bytes(wav[24..28].try_into()?) != 16_000
        || u16::from_le_bytes([wav[34], wav[35]]) != 16
    {
        return Err("expected mono 16 kHz PCM16 WAV".into());
    }
    let length = u32::from_le_bytes(wav[40..44].try_into()?) as usize;
    let pcm = request
        .get(offset + 44..offset + 44 + length)
        .ok_or("short WAV PCM")?;
    // A late voiced sequence survives VAD pre-roll trimming. Checking original
    // contiguous bytes verifies actual nonzero fixture data and byte order.
    let voiced = &PCM[25_600..27_648];
    if !voiced.iter().any(|byte| *byte != 0) || !pcm.windows(voiced.len()).any(|w| w == voiced) {
        return Err("STT WAV omitted or reordered actual fixture speech".into());
    }
    Ok(())
}
async fn http(replies: &[&str]) -> Result<HttpPeer, Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let mut config = SpeechConfig::openai("loopback-protocol-fixture");
    config.base_url = format!("http://{}", listener.local_addr()?);
    config.timeout = Duration::from_secs(8);
    config.max_audio_bytes = 512 * 1024;
    config.max_json_bytes = 16 * 1024;
    let gates: Vec<_> = replies
        .iter()
        .map(|_| Arc::new(HttpGate::default()))
        .collect();
    let plans: Vec<_> = replies
        .iter()
        .zip(&gates)
        .map(|(text, gate)| (text.to_string(), gate.clone()))
        .collect();
    let cancel = Cancellation::new();
    let cancelled = cancel.clone();
    let task = tokio::spawn(async move {
        let work = async {
            for (text, gate) in plans {
                let (mut stream, _) = listener.accept().await?;
                let request = read_request(&mut stream).await?;
                *gate.request.lock().unwrap() = request;
                gate.received.notify_one();
                let mut probe = [0; 1];
                tokio::select! {
                    _ = gate.release.notified() => {
                        let body = json!({"text": text}).to_string();
                        let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                        let write = async {
                            stream.write_all(response.as_bytes()).await?;
                            stream.shutdown().await
                        }.await;
                        match write {
                            Ok(()) => gate.replied.store(true, Ordering::SeqCst),
                            Err(error) if gate.expected_disconnect.load(Ordering::SeqCst)
                                && matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe) => {
                                gate.closed.store(true, Ordering::SeqCst);
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    read = stream.read(&mut probe) => {
                        match read {
                            Ok(0) => {},
                            Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => {},
                            Ok(_) => return Err::<(), Error>("unexpected data after complete HTTP request".into()),
                            Err(e) => return Err(e.into()),
                        }
                        if !gate.expected_disconnect.load(Ordering::SeqCst) {
                            return Err("HTTP client closed before authorized response release".into());
                        }
                        gate.closed.store(true, Ordering::SeqCst);
                    }
                }
                gate.completed.notify_one();
            }
            Ok::<(), Error>(())
        };
        tokio::select! {
            result = tokio::time::timeout(WAIT, work) => result.map_err(|_| -> Error { "HTTP fixture exceeded deadline".into() })?,
            _ = cancelled.cancelled() => Ok(()),
        }
    });
    Ok(HttpPeer {
        config,
        gates,
        cancel,
        task,
    })
}
impl HttpGate {
    async fn received(&self) -> Result<(), Error> {
        tokio::time::timeout(WAIT, self.received.notified()).await?;
        Ok(())
    }
    async fn completed(&self) -> Result<(), Error> {
        tokio::time::timeout(WAIT, self.completed.notified()).await?;
        Ok(())
    }
}

// Deadlines borrow task owners. Any fallback aborts AND awaits the owner;
// neither a timeout nor a failed test assertion may silently detach a peer.
async fn settle<T>(mut task: JoinHandle<T>) -> Result<T, Error> {
    match tokio::time::timeout(WAIT, &mut task).await {
        Ok(result) => result.map_err(Into::into),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("owned task exceeded shutdown deadline".into())
        }
    }
}
struct Fixture {
    peer: Peer,
    http: HttpPeer,
    cancel: Cancellation,
    run: JoinHandle<ScriptResult>,
    workers_before: WorkerSnapshot,
}
#[derive(Debug)]
struct GateState {
    received: bool,
    request_bytes: usize,
    replied: bool,
    closed: bool,
    expected_disconnect: bool,
}
#[derive(Debug)]
struct Outcome {
    run: Result<ScriptResult, Error>,
    peer: Result<WireTrace, Error>,
    http: Result<(), Error>,
    workers_before: WorkerSnapshot,
    workers_first: WorkerSnapshot,
    workers_after: WorkerSnapshot,
    worker_observations: Vec<WorkerObservation>,
    gates: Vec<GateState>,
}
impl Fixture {
    async fn new(
        scenario: ScriptScenario,
        outputs: Vec<Vec<Value>>,
        replies: &[&str],
        options: SessionOptions,
    ) -> Result<Self, Error> {
        Self::with_config(scenario, outputs, replies, options, |config| config).await
    }
    async fn with_config(
        scenario: ScriptScenario,
        outputs: Vec<Vec<Value>>,
        replies: &[&str],
        options: SessionOptions,
        configure: impl FnOnce(BotTranscriptionConfig) -> BotTranscriptionConfig,
    ) -> Result<Self, Error> {
        // Fail missing-model setup before creating asynchronous owners.
        let workers_before = vad_workers();
        let analyzer = analyzer();
        let http = http(replies).await?;
        let peer = match websocket(outputs).await {
            Ok(peer) => peer,
            Err(error) => {
                http.cancel.cancel();
                let _ = settle(http.task).await;
                return Err(error);
            }
        };
        let config = BotTranscriptionConfig::new(
            analyzer,
            http.config.clone(),
            TranscriptionRequest::default(),
        );
        let session = config.and_then(|config| {
            EvalSession::with_bot_transcription(scenario, &peer.url, options, configure(config))
        });
        let session = match session {
            Ok(session) => session,
            Err(error) => {
                peer.task.abort();
                let _ = peer.task.await;
                http.cancel.cancel();
                let _ = settle(http.task).await;
                return Err(error.into());
            }
        };
        let cancel = Cancellation::new();
        let run = tokio::spawn(session.run(cancel.clone()));
        Ok(Self {
            peer,
            http,
            cancel,
            run,
            workers_before,
        })
    }
    async fn finish(self, stop: bool) -> Outcome {
        if stop {
            self.peer.known_cancel.store(true, Ordering::SeqCst);
            self.cancel.cancel();
        }
        let run = settle(self.run).await;
        // HTTP is explicitly stopped only after the session owner has joined.
        self.http.cancel.cancel();
        let peer = settle(self.peer.task).await.and_then(|r| r);
        let http = settle(self.http.task).await.and_then(|r| r);
        let gates = self
            .http
            .gates
            .iter()
            .map(|gate| {
                let request_bytes = gate.request.lock().unwrap().len();
                GateState {
                    received: request_bytes != 0,
                    request_bytes,
                    replied: gate.replied.load(Ordering::SeqCst),
                    closed: gate.closed.load(Ordering::SeqCst),
                    expected_disconnect: gate.expected_disconnect.load(Ordering::SeqCst),
                }
            })
            .collect();
        // Every task and native owner has already been joined above. Linux's
        // /proc task directory is an independently observed view; retain the
        // first snapshot and a short bounded convergence trace rather than
        // claiming eventual disappearance was immediate join evidence.
        let worker_observations = observe_vad_worker_cleanup(&self.workers_before).await;
        let workers_first = worker_observations.first().unwrap().workers.clone();
        let workers_after = worker_observations.last().unwrap().workers.clone();
        Outcome {
            run,
            peer,
            http,
            workers_before: self.workers_before,
            workers_first,
            workers_after,
            worker_observations,
            gates,
        }
    }
}
impl Outcome {
    fn report(&self) {
        // All task/thread owners have settled before diagnostics or assertions.
        // Rust's harness preserves this captured output on any later failure,
        // including a barrier unwrap after checked() has returned its results.
        eprintln!("Settled transcription fixture: {self:#?}");
        eprintln!(
            "Native worker first post-join snapshot: {:?}",
            self.workers_first
        );
        for sample in &self.worker_observations {
            eprintln!(
                "Native worker /proc sample +{} ms: {:?}",
                sample.elapsed_ms, sample.workers
            );
            for worker in sample.workers.as_deref().unwrap_or_default() {
                eprintln!(
                    "Native worker tid={} starttime={} state={}",
                    worker.tid, worker.starttime, worker.state
                );
            }
        }
        for (index, gate) in self.gates.iter().enumerate() {
            eprintln!("HTTP gate {index}: received={}, request_bytes={}, replied={}, closed={}, expected_disconnect={}",
                gate.received, gate.request_bytes, gate.replied, gate.closed, gate.expected_disconnect);
        }
    }
    fn checked(self) -> (ScriptResult, WireTrace) {
        self.report();
        self.http.unwrap();
        assert!(
            same_worker_identities(&self.workers_after, &self.workers_before),
            "native VAD thread identities did not return to baseline after all owners joined"
        );
        let peer = self.peer.unwrap();
        assert!(peer.finished || peer.reset_after_cancel, "{peer:?}");
        (self.run.unwrap(), peer)
    }
}
fn responses(result: &ScriptResult) -> Vec<&str> {
    result
        .events_seen
        .iter()
        .filter(|e| e["type"] == "response")
        .map(|e| e["text"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn genuine_pcm_response_waits_for_http_and_remote_stop_cannot_cut_queued_audio() {
    let mut output = vec![
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":"forbidden LLM-only text"})),
        wire("bot-llm-stopped", json!({})),
        wire("bot-stopped-speaking", json!({})),
    ];
    output.push(speech());
    let fixture = Fixture::new(
        expected("scripted recognized phrase"),
        vec![output],
        &["scripted recognized phrase"],
        options(),
    )
    .await
    .unwrap();
    let barrier = fixture.http.gates[0].received().await;
    let prematurely_finished = fixture.run.is_finished();
    fixture.http.gates[0].release.notify_one();
    let (result, peer) = fixture.finish(barrier.is_err()).await.checked();
    barrier.unwrap();
    assert!(
        !prematurely_finished,
        "response appeared before HTTP transcript was supplied"
    );
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["scripted recognized phrase"]);
    assert!(peer.cancelled);
    let uri = url::Url::parse(&format!("ws://fixture{}", peer.request)).unwrap();
    assert_eq!(
        uri.query_pairs().into_owned().collect::<Vec<_>>(),
        [
            ("custom".into(), "kept".into()),
            ("capture_bot_audio".into(), "true".into())
        ]
    );
    assert_eq!(
        peer.inputs[0]["data"]["options"],
        json!({"run_immediately":true,"audio_response":true})
    );
}

#[tokio::test]
async fn empty_transcript_emits_no_response_and_later_real_segment_recovers() {
    let fixture = Fixture::new(
        expected("later valid phrase"),
        vec![start_speech()],
        &["", "later valid phrase"],
        options(),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[0].completed().await?;
        fixture.peer.send(vec![speech()]).await?;
        fixture.http.gates[1].received().await?;
        fixture.http.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["later valid phrase"]);
}

#[tokio::test]
async fn interruption_and_new_llm_epoch_discard_delayed_old_http_result() {
    let fixture = Fixture::new(
        expected("new epoch phrase"),
        vec![start_speech()],
        &["stale forbidden phrase", "new epoch phrase"],
        options(),
    )
    .await
    .unwrap();
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        fixture
            .peer
            .send(vec![
                wire("bot-interrupted", json!({})),
                wire("bot-llm-started", json!({})),
                speech(),
            ])
            .await?;
        // Queue interruption/new-epoch packets together on the ordered wire
        // before releasing the delayed old HTTP response. The explicit-send
        // case below additionally observes the evaluator's new-input boundary.
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[1].received().await?;
        fixture.http.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["new epoch phrase"]);
}

#[tokio::test]
async fn explicit_next_send_discards_previous_pending_recognition() {
    let s = scenario(json!([
        {"user":"first", "expect":[{"event":"user_transcription", "text_contains":"change epoch"}]},
        {"user":"second", "send_after":{"event":"user_transcription"}, "expect":[{"event":"response", "text_contains":"second phrase", "text_excludes":"stale"}]}
    ]));
    let fixture = Fixture::new(
        s,
        vec![start_speech(), start_speech()],
        &["stale phrase", "second phrase"],
        options(),
    )
    .await
    .unwrap();
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        fixture.peer.send(vec![marker("change epoch")]).await?;
        fixture.peer.wait_inputs(2).await?;
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[1].received().await?;
        fixture.http.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["second phrase"]);
}

#[tokio::test]
async fn ordinary_next_send_waits_for_queued_audio_and_pending_http_accounting() {
    let s = scenario(json!([
        {"user":"first", "expect":[{"event":"user_transcription", "text_contains":"audio queued"}]},
        {"user":"second", "expect":[{"event":"user_transcription", "text_contains":"second received"}]}
    ]));
    let mut output = start_speech();
    output.push(marker("audio queued"));
    let fixture = Fixture::new(
        s,
        vec![output, vec![marker("second received")]],
        &["first phrase"],
        options(),
    )
    .await
    .unwrap();
    let barrier = fixture.http.gates[0].received().await;
    let inputs_while_pending = fixture.peer.inputs.load(Ordering::SeqCst);
    fixture.http.gates[0].release.notify_one();
    let (result, peer) = fixture.finish(barrier.is_err()).await.checked();
    barrier.unwrap();
    assert_eq!(
        inputs_while_pending, 1,
        "ordinary next input overtook pending recognition"
    );
    assert!(result.passed, "{result:?}");
    assert_eq!(peer.inputs.len(), 2);
    assert_eq!(responses(&result), ["first phrase"]);
}

#[tokio::test]
async fn ordinary_next_send_settlement_timeout_fails_without_sending_it() {
    let s = scenario(json!([
        {"user":"first", "expect":[{"event":"user_transcription", "text_contains":"audio queued"}]},
        {"user":"must not send", "expect":[{"event":"response"}]}
    ]));
    let mut output = start_speech();
    output.push(marker("audio queued"));
    let mut options = options();
    options.bot_quiet_timeout = Duration::from_millis(2_000);
    let fixture = Fixture::new(s, vec![output], &["never released"], options)
        .await
        .unwrap();
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        fixture.http.gates[0].completed().await?;
        Ok(())
    }
    .await;
    let closed = fixture.http.gates[0].closed.load(Ordering::SeqCst);
    let (result, peer) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(closed, "pending HTTP connection was not closed on timeout");
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|f| f.reason.contains("bot transcription settlement")),
        "{result:?}"
    );
    assert_eq!(peer.inputs.len(), 1);
    assert!(responses(&result).is_empty());
}

#[tokio::test]
async fn cancellation_during_http_closes_connection_and_joins_session_and_peers() {
    let fixture = Fixture::new(
        expected("never released"),
        vec![start_speech()],
        &["never released"],
        options(),
    )
    .await
    .unwrap();
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        fixture.peer.known_cancel.store(true, Ordering::SeqCst);
        fixture.cancel.cancel();
        fixture.http.gates[0].completed().await?;
        Ok(())
    }
    .await;
    let closed = fixture.http.gates[0].closed.load(Ordering::SeqCst);
    let (result, peer) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(
        closed,
        "actual HTTP peer never saw EOF/reset after cancellation"
    );
    assert!(!result.passed);
    assert!(
        result.failures.iter().any(|f| f.reason.contains("cancel")),
        "{result:?}"
    );
    assert!(responses(&result).is_empty());
    assert!(peer.cancelled);
}

#[tokio::test]
async fn dropping_run_future_with_queued_vad_work_closes_owned_websocket() {
    let s = scenario(
        json!([{"user":"play", "expect":[{"event":"response", "text_contains":"never"}]}]),
    );
    let fixture = Fixture::new(s, vec![start_speech()], &["never"], options())
        .await
        .unwrap();
    // A genuine multipart request proves the native VAD thread actually ran.
    let work = fixture.http.gates[0].received().await;
    let queued = fixture
        .peer
        .send(vec![speech(), speech(), marker("queued VAD work")])
        .await;
    fixture.peer.known_cancel.store(true, Ordering::SeqCst);
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    fixture.peer.known_drop.store(true, Ordering::SeqCst);
    fixture.run.abort();
    let closure = fixture.http.gates[0].completed().await;
    let closed = fixture.http.gates[0].closed.load(Ordering::SeqCst);
    let outcome = fixture.finish(true).await;
    outcome.report();
    work.unwrap();
    queued.unwrap();
    closure.unwrap();
    assert!(
        closed,
        "dropping the session failed to close the actual HTTP owner"
    );
    assert!(
        same_worker_identities(&outcome.workers_after, &outcome.workers_before),
        "native VAD worker identities survived session drop and bounded observation"
    );
    outcome.http.unwrap();
    assert!(outcome.run.is_err());
    let peer = outcome.peer.unwrap();
    assert!(
        peer.finished || peer.reset_after_cancel || peer.reset_after_drop,
        "{peer:?}"
    );
}

#[tokio::test]
async fn malformed_and_unsupported_pcm_fail_without_any_stt_upload() {
    for value in [
        wire(
            "eval-bot-audio",
            json!({"audio":"!!!!", "sampleRate":16000}),
        ),
        audio(&[1], 16_000),
        audio(&[1, 0], 8_000),
        audio(&[1, 0], 48_000),
    ] {
        let fixture = Fixture::new(
            expected("never"),
            vec![vec![wire("bot-llm-started", json!({})), value]],
            &["never"],
            options(),
        )
        .await
        .unwrap();
        let gate = fixture.http.gates[0].clone();
        let (result, peer) = fixture.finish(false).await.checked();
        assert!(!result.passed, "{result:?}");
        assert!(responses(&result).is_empty());
        assert!(
            gate.request.lock().unwrap().is_empty(),
            "invalid PCM reached HTTP STT"
        );
        assert!(peer.cancelled);
    }
}

#[test]
fn ordinary_constructor_rejects_typed_response_and_opt_in_rejects_classifier_eval() {
    let s = expected("spoken phrase");
    assert!(EvalSession::new(s.clone(), "ws://127.0.0.1:1", options()).is_err());
    let mut s = s;
    s.turns[0].expect[0].eval = Some("judge this".into());
    let config = bounded_config();
    assert!(EvalSession::with_bot_transcription(s, "ws://127.0.0.1:1", options(), config).is_err());
}

// Linux integration runs use --test-threads=1. Check actual OS identities,
// including starttime to detect TID reuse. State is retained for diagnostics.
// Non-Linux tests still require socket/task ownership to settle.
#[derive(Debug, Clone, PartialEq, Eq)]
struct VadWorker {
    tid: u32,
    starttime: u64,
    state: String,
}
type WorkerSnapshot = Option<Vec<VadWorker>>;
#[derive(Debug)]
struct WorkerObservation {
    elapsed_ms: u128,
    workers: WorkerSnapshot,
}
fn vad_workers() -> WorkerSnapshot {
    let entries = std::fs::read_dir("/proc/self/task").ok()?;
    let mut workers = Vec::new();
    for entry in entries {
        let entry = entry.ok()?;
        let name = match std::fs::read_to_string(entry.path().join("comm")) {
            Ok(name) => name,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        if name.trim() != "eval-bot-vad" {
            continue;
        }
        let stat = match std::fs::read_to_string(entry.path().join("stat")) {
            Ok(stat) => stat,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => return None,
        };
        // /proc/<pid>/stat field 3 is state; field 22 is starttime. Split
        // after the final comm delimiter because comm may contain spaces.
        let (_, fields) = stat.rsplit_once(") ")?;
        let fields: Vec<_> = fields.split_whitespace().collect();
        workers.push(VadWorker {
            tid: entry.file_name().to_str()?.parse().ok()?,
            starttime: fields.get(19)?.parse().ok()?,
            state: fields.first()?.to_string(),
        });
    }
    workers.sort_by_key(|worker| (worker.tid, worker.starttime));
    Some(workers)
}
fn same_worker_identities(left: &WorkerSnapshot, right: &WorkerSnapshot) -> bool {
    match (left, right) {
        (Some(left), Some(right)) => left
            .iter()
            .map(|w| (w.tid, w.starttime))
            .eq(right.iter().map(|w| (w.tid, w.starttime))),
        (None, None) => !cfg!(target_os = "linux"),
        _ => false,
    }
}
async fn observe_vad_worker_cleanup(baseline: &WorkerSnapshot) -> Vec<WorkerObservation> {
    let start = tokio::time::Instant::now();
    let mut samples = Vec::new();
    loop {
        let workers = vad_workers();
        let converged = same_worker_identities(&workers, baseline);
        samples.push(WorkerObservation {
            elapsed_ms: start.elapsed().as_millis(),
            workers,
        });
        if converged || start.elapsed() >= Duration::from_millis(200) {
            return samples;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

fn bounded_config() -> BotTranscriptionConfig {
    let mut speech = SpeechConfig::openai("local-fixture");
    speech.base_url = "http://127.0.0.1:1".into();
    speech.timeout = Duration::from_secs(2);
    speech.max_audio_bytes = 512 * 1024;
    speech.max_json_bytes = 16 * 1024;
    BotTranscriptionConfig::new(analyzer(), speech, TranscriptionRequest::default()).unwrap()
}

#[test]
fn invalid_bounds_are_rejected_before_connecting_or_starting_workers() {
    let before = vad_workers();
    for invalid_bound in 0..6 {
        let config = bounded_config();
        let config = match invalid_bound {
            0 => config.with_max_total_pcm_bytes(0),
            1 => config.with_max_buffered_pcm_bytes(0),
            2 => config.with_max_segment_duration(Duration::ZERO),
            3 => config.with_max_pending_segments(0),
            4 => config.with_max_transcript_bytes(0),
            _ => config.with_max_total_pcm_bytes(16 * 1024 * 1024 + 1),
        };
        assert!(EvalSession::with_bot_transcription(
            expected("never"),
            "ws://127.0.0.1:1",
            options(),
            config
        )
        .is_err());
    }
    assert!(same_worker_identities(&vad_workers(), &before));
}

#[tokio::test]
async fn two_acoustic_segments_with_first_http_pending_aggregate_in_spoken_order() {
    let fixture = Fixture::new(
        expected("first phrase second phrase"),
        vec![start_speech()],
        &["first phrase", "second phrase"],
        options(),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        // This is real audio pacing, not an assertion based on an arbitrary
        // sleep. The second voiced clip spans twenty-four 40 ms VAD input ticks
        // while the first HTTP reply remains held at an explicit barrier.
        let mut cadence = tokio::time::interval(Duration::from_millis(40));
        cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        for chunk in PCM[..CLIP_BYTES].chunks(1280) {
            cadence.tick().await;
            fixture.peer.send(vec![audio(chunk, 16_000)]).await?;
        }
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[1].received().await?;
        fixture.http.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["first phrase second phrase"]);
}

#[tokio::test]
async fn pcm_admission_budgets_reject_whole_frame_without_partial_stt_upload() {
    for total_limit in [false, true] {
        let fixture = Fixture::with_config(
            expected("never"),
            vec![start_speech()],
            &["never"],
            options(),
            |config| {
                if total_limit {
                    config.with_max_total_pcm_bytes(CLIP_BYTES - 2)
                } else {
                    config.with_max_buffered_pcm_bytes(CLIP_BYTES - 2)
                }
            },
        )
        .await
        .unwrap();
        let gate = fixture.http.gates[0].clone();
        let (result, _) = fixture.finish(false).await.checked();
        assert!(!result.passed, "{result:?}");
        assert!(
            result.failures.iter().any(|f| f.reason.contains("limit")),
            "{result:?}"
        );
        assert!(responses(&result).is_empty());
        assert!(
            gate.request.lock().unwrap().is_empty(),
            "a prefix of the rejected frame reached STT"
        );
    }
}

// The second STT response is held until after its PCM is processed. For the
// fresh variant, observed history below verifies the first recognition was
// published before the remote start; exact completion ordering is separately
// tested with controlled session instants.
async fn spoken_absence_pcm_case(fresh: Option<&str>) {
    let fixture = Fixture::new(
        scenario(json!([{"user":"play two segments", "expect":[
            {"event":"response", "text_contains":"first phrase"},
            {"event":"response", "absent":true, "within_ms":7000}
        ]}])),
        vec![start_speech()],
        &["first phrase", "held second phrase"],
        options(),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        fixture.http.gates[0].received().await?;
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[0].completed().await?;
        // Let the real 600 ms speech-timeout aggregator publish the first
        // turn before admitting another PCM segment. This pacing is not the
        // exact-ordering proof: history assertions and unit gates supply it.
        tokio::time::sleep(Duration::from_millis(800)).await;
        fixture.peer.send(vec![speech()]).await?;
        fixture.http.gates[1].received().await?;
        if let Some(kind) = fresh {
            fixture.peer.send(vec![wire(kind, json!({}))]).await?;
        }
        fixture.http.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert_eq!(responses(&result), ["first phrase", "held second phrase"]);
    assert!(result.turns[0].expectations[0].passed, "{result:?}");
    if let Some(kind) = fresh {
        assert!(!result.passed, "{result:?}");
        assert_eq!(result.failures[0].kind, "unexpected_event");
        assert!(result.failures[0].reason.contains("held second phrase"));
        let response = result
            .events_seen
            .iter()
            .position(|e| e["type"] == "response")
            .unwrap();
        let friendly = if kind == "bot-llm-started" {
            "llm_started"
        } else {
            "bot_started_speaking"
        };
        let start = result
            .events_seen
            .iter()
            .rposition(|e| e["type"] == friendly)
            .unwrap();
        assert!(
            response < start,
            "fresh start must be observed after first response: {result:?}"
        );
    } else {
        assert!(result.passed, "{result:?}");
        assert!(
            result.turns[0].duration_ms >= 7000,
            "continuation must not finish the absence window early"
        );
    }
}

#[tokio::test]
async fn spoken_absence_held_pcm_continuation_keeps_original_deadline() {
    spoken_absence_pcm_case(None).await;
}

#[tokio::test]
async fn spoken_absence_held_pcm_new_llm_start_fails() {
    spoken_absence_pcm_case(Some("bot-llm-started")).await;
}

#[tokio::test]
async fn spoken_absence_held_pcm_new_remote_speaking_start_fails() {
    spoken_absence_pcm_case(Some("bot-started-speaking")).await;
}
