//! Real loopback WebSocket -> paced Silero -> Smart Turn -> HTTP -> response.
//! The tracked ONNX predictor proves genuine model execution. Scripted predictor
//! tests separately exercise endpoint races; scripted HTTP is not STT accuracy.
//! Owners are released and joined before assertions, including failed barriers.
#![cfg(feature = "bot-audio-transcription")]

use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::VadParams;
use pipecat_evals::{
    matcher::ScriptResult, BotTranscriptionConfig, Cancellation, EvalSession, ScenarioFile,
    ScriptScenario, SessionOptions,
};
use pipecat_speech::{SpeechConfig, TranscriptionRequest};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use pipecat_vad::{
    EndOfTurnState, MonotonicSmartTurnClock, RuntimeOptions, SileroModel, SileroVadAnalyzer,
    SmartTurnAnalyzer, SmartTurnCancellation, SmartTurnModel, SmartTurnParams, SmartTurnPrediction,
    SmartTurnPredictor, VadError,
};
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, Notify},
    task::JoinHandle,
};

type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(20);
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
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
        default_timeout: Duration::from_secs(15),
        bot_quiet_timeout: Duration::from_secs(15),
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

#[derive(Debug, Default)]
struct WireTrace {
    request: String,
    inputs: Vec<Value>,
    cancelled: bool,
    finished: bool,
    reset_after_cancel: bool,
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
    config.timeout = Duration::from_secs(16);
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
    probe: Arc<PredictorProbe>,
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
    predictor_dropped: bool,
}
impl Fixture {
    async fn new(
        scenario: ScriptScenario,
        outputs: Vec<Vec<Value>>,
        replies: &[&str],
        endpoint: SmartTurnAnalyzer,
        probe: Arc<PredictorProbe>,
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
            EvalSession::with_bot_transcription(
                scenario,
                &peer.url,
                options(),
                config.with_smart_turn(endpoint)?,
            )
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
            probe,
        })
    }
    async fn finish(self, stop: bool) -> Outcome {
        // The predictor's own finite watchdog is a last resort. Normal and
        // failed-barrier cleanup explicitly releases every blocking call first.
        self.probe.release_all();
        if stop {
            for gate in &self.http.gates {
                gate.expected_disconnect.store(true, Ordering::SeqCst);
            }
            self.peer.known_cancel.store(true, Ordering::SeqCst);
            self.cancel.cancel();
        }
        let run = settle(self.run).await;
        let predictor_dropped = self.probe.dropped.load(Ordering::SeqCst);
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
            predictor_dropped,
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
            self.predictor_dropped,
            "Smart Turn predictor owner survived session settlement"
        );
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

// Tests in this binary hold TEST_LOCK. Check actual OS identities,
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
        if name.trim() != "eval-bot-vad" && !name.trim().starts_with("pipecat-smart-") {
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

// No helper starts an unowned thread. Blocking test predictors run only on the
// production Smart Turn owner, with an explicit release and a finite watchdog.
#[derive(Default)]
struct PredictorGate {
    release: AtomicBool,
    entered: AtomicBool,
    saw_cancellation: AtomicBool,
}
#[derive(Debug, Clone)]
struct PredictionRecord {
    samples: usize,
    rate: u32,
    voiced_spans: usize,
    genuine_model: bool,
    prediction: Option<SmartTurnPrediction>,
    error: Option<String>,
}
struct PredictorProbe {
    gates: Vec<Arc<PredictorGate>>,
    calls: Mutex<Vec<PredictionRecord>>,
    dropped: AtomicBool,
}
impl PredictorProbe {
    fn new(blocked: &[usize]) -> Arc<Self> {
        Arc::new(Self {
            gates: (0..8)
                .map(|index| {
                    Arc::new(PredictorGate {
                        release: AtomicBool::new(!blocked.contains(&index)),
                        ..Default::default()
                    })
                })
                .collect(),
            calls: Mutex::new(Vec::new()),
            dropped: AtomicBool::new(false),
        })
    }
    fn release_all(&self) {
        for gate in &self.gates {
            gate.release.store(true, Ordering::SeqCst);
        }
    }
    fn snapshot(&self) -> Vec<PredictionRecord> {
        self.calls.lock().unwrap().clone()
    }
    async fn finished(&self, count: usize) -> Result<(), Error> {
        wait_until(|| {
            self.calls
                .lock()
                .unwrap()
                .iter()
                .filter(|call| call.prediction.is_some() || call.error.is_some())
                .count()
                >= count
        })
        .await
    }
    async fn entered(&self, index: usize) -> Result<(), Error> {
        wait_until(|| self.gates[index].entered.load(Ordering::SeqCst)).await
    }
}
async fn wait_until(mut condition: impl FnMut() -> bool) -> Result<(), Error> {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !condition() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await?;
    Ok(())
}
enum PredictorBackend {
    Model(Box<SmartTurnModel>),
    Scripted(Vec<bool>),
}
struct TrackedPredictor {
    backend: PredictorBackend,
    probe: Arc<PredictorProbe>,
}
impl SmartTurnPredictor for TrackedPredictor {
    fn predict(
        &mut self,
        samples: &[i16],
        sample_rate: u32,
        cancellation: &SmartTurnCancellation,
    ) -> pipecat_vad::Result<SmartTurnPrediction> {
        let voiced = pcm(&PCM[25_600..27_648]);
        let index = {
            let mut calls = self.probe.calls.lock().unwrap();
            let index = calls.len();
            calls.push(PredictionRecord {
                samples: samples.len(),
                rate: sample_rate,
                voiced_spans: samples
                    .windows(voiced.len())
                    .filter(|span| *span == voiced.as_slice())
                    .count(),
                genuine_model: matches!(&self.backend, PredictorBackend::Model(_)),
                prediction: None,
                error: None,
            });
            index
        };
        let result = (|| {
            if let Some(gate) = self.probe.gates.get(index) {
                gate.entered.store(true, Ordering::SeqCst);
                let start = Instant::now();
                while !gate.release.load(Ordering::SeqCst) {
                    if cancellation.is_cancelled() {
                        gate.saw_cancellation.store(true, Ordering::SeqCst);
                    }
                    // Intentionally do not exit on cancellation: the join test
                    // verifies the owning session waits for actual native exit.
                    if start.elapsed() >= Duration::from_secs(12) {
                        return Err(VadError::Runtime(
                            "test predictor release watchdog expired".into(),
                        ));
                    }
                    std::thread::sleep(Duration::from_millis(2));
                }
            }
            match &mut self.backend {
                PredictorBackend::Model(model) => {
                    model.predict_pcm16_cancellable(samples, sample_rate, cancellation)
                }
                PredictorBackend::Scripted(decisions) => {
                    let complete = decisions.get(index).copied().unwrap_or(false);
                    Ok(SmartTurnPrediction {
                        probability: if complete { 0.9 } else { 0.1 },
                        complete,
                    })
                }
            }
        })();
        let mut calls = self.probe.calls.lock().unwrap();
        match &result {
            Ok(prediction) => calls[index].prediction = Some(*prediction),
            Err(error) => calls[index].error = Some(error.to_string()),
        }
        result
    }
}
impl Drop for TrackedPredictor {
    fn drop(&mut self) {
        self.probe.dropped.store(true, Ordering::SeqCst);
    }
}
fn pcm(bytes: &[u8]) -> Vec<i16> {
    bytes
        .as_chunks::<2>()
        .0
        .iter()
        .map(|sample| i16::from_le_bytes([sample[0], sample[1]]))
        .collect()
}
fn runtime() -> RuntimeOptions {
    RuntimeOptions {
        library_path: std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into()),
    }
}
fn smart_model_path() -> PathBuf {
    std::env::var_os("PIPECAT_SMART_TURN_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/turn/smart_turn/data/smart-turn-v3.2-cpu.onnx")
        })
}
fn tracked(
    backend: PredictorBackend,
    blocked: &[usize],
) -> (SmartTurnAnalyzer, Arc<PredictorProbe>) {
    let probe = PredictorProbe::new(blocked);
    let analyzer = SmartTurnAnalyzer::with_predictor(
        TrackedPredictor {
            backend,
            probe: probe.clone(),
        },
        16_000,
        SmartTurnParams::default(),
        Arc::new(MonotonicSmartTurnClock::default()),
        None,
    )
    .unwrap();
    (analyzer, probe)
}
fn scripted(decisions: &[bool], blocked: &[usize]) -> (SmartTurnAnalyzer, Arc<PredictorProbe>) {
    tracked(PredictorBackend::Scripted(decisions.to_vec()), blocked)
}
fn genuine() -> (SmartTurnAnalyzer, Arc<PredictorProbe>) {
    let model = SmartTurnModel::load(&runtime(), smart_model_path())
        .expect("real local Smart Turn ONNX graph and runtime are required");
    tracked(PredictorBackend::Model(Box::new(model)), &[])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_smart_turn_prediction_is_on_the_wire_response_path_and_http_stays_authoritative() {
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = genuine();
    let fixture = Fixture::new(
        expected("genuine model wire phrase"),
        vec![start_speech()],
        &["genuine model wire phrase"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    let work: Result<bool, Error> = async {
        fixture.http.gates[0].received().await?;
        probe.finished(1).await?;
        let premature = fixture.run.is_finished();
        fixture.http.gates[0].release.notify_one();
        Ok(premature)
    }
    .await;
    let (result, peer) = fixture.finish(work.is_err()).await.checked();
    let premature = work.unwrap();
    let calls = probe.snapshot();
    eprintln!("Actual ONNX decisions for the paced, trimmed speech prefix: {calls:#?}");
    assert!(
        !premature,
        "even genuine endpoint completion cannot replace an HTTP transcript"
    );
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["genuine model wire phrase"]);
    assert!(!calls.is_empty());
    assert!(calls
        .iter()
        .all(|call| call.genuine_model && call.rate == 16_000 && call.samples > 0));
    assert!(calls.iter().any(|call| call.voiced_spans >= 1));
    assert!(calls
        .iter()
        .any(|call| call.prediction.is_some_and(|p| p.probability.is_finite())));
    assert!(calls.iter().all(|call| call.error.is_none()));
    let uri = url::Url::parse(&format!("ws://fixture{}", peer.request)).unwrap();
    assert_eq!(uri.path(), "/eval");
    assert_eq!(
        uri.query_pairs().into_owned().collect::<Vec<_>>(),
        [
            ("custom".into(), "kept".into()),
            ("capture_bot_audio".into(), "true".into()),
        ]
    );
    assert_eq!(peer.inputs[0]["data"]["options"]["audio_response"], true);
    // The prefix's acoustic verdict is deliberately not prescribed. An actual
    // INCOMPLETE can finish via the real three-second sample-silence fallback.
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_full_fixtures_retain_incomplete_and_complete_with_the_recorded_oracles() {
    let _serial = TEST_LOCK.lock().await;
    let before = vad_workers();
    let (mut endpoint, probe) = genuine();
    let tone = pcm(include_bytes!(
        "../../pipecat-vad/tests/fixtures/smart_turn_tone.pcm16"
    ));
    let speech = pcm(PCM);
    let work: Result<_, Error> = async {
        endpoint.append_pcm16(&tone, true)?;
        let first = endpoint.analyze_end_of_turn().await?;
        let retained = endpoint.buffer().analysis_samples();
        let second = endpoint.analyze_end_of_turn().await?;
        endpoint.clear();
        endpoint.append_pcm16(&speech, true)?;
        let final_result = endpoint.analyze_end_of_turn().await?;
        let cleared = endpoint.buffer().buffered_samples() == 0;
        Ok((first, retained, second, final_result, cleared))
    }
    .await;
    let cleaned = endpoint.cleanup().await;
    drop(endpoint);
    let observations = observe_vad_worker_cleanup(&before).await;
    eprintln!("Real model cleanup: {observations:#?}");
    cleaned.unwrap();
    let (first, retained, second, final_result, cleared) = work.unwrap();
    assert!(probe.dropped.load(Ordering::SeqCst));
    assert!(same_worker_identities(
        &observations.last().unwrap().workers,
        &before
    ));
    let expected_tone: f32 =
        include_str!("../../pipecat-vad/tests/fixtures/smart_turn_tone.probability.txt")
            .trim()
            .parse()
            .unwrap();
    let expected_speech: f32 =
        include_str!("../../pipecat-vad/tests/fixtures/smart_turn_speech.probability.txt")
            .trim()
            .parse()
            .unwrap();
    for result in [first, second] {
        assert_eq!(result.state, EndOfTurnState::Incomplete);
        assert!((result.metrics.unwrap().probability - expected_tone).abs() < 1e-5);
    }
    assert_eq!(retained, tone);
    assert_eq!(final_result.state, EndOfTurnState::Complete);
    assert!((final_result.metrics.unwrap().probability - expected_speech).abs() < 1e-5);
    assert!(cleared);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn injected_complete_cannot_emit_before_the_actual_http_response() {
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = scripted(&[true], &[]);
    let fixture = Fixture::new(
        expected("HTTP gated phrase"),
        vec![start_speech()],
        &["HTTP gated phrase"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    let work: Result<bool, Error> = async {
        probe.finished(1).await?;
        fixture.http.gates[0].received().await?;
        let premature = fixture.run.is_finished();
        fixture.http.gates[0].release.notify_one();
        Ok(premature)
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    assert!(!work.unwrap());
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["HTTP gated phrase"]);
    assert!(probe.snapshot()[0].prediction.unwrap().complete);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn resumed_speech_invalidates_complete_while_old_http_is_pending_and_requires_fresh_complete()
{
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = scripted(&[true, true], &[1]);
    let fixture = Fixture::new(
        expected("first phrase second phrase"),
        vec![start_speech()],
        &["first phrase", "second phrase"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    let work: Result<bool, Error> = async {
        fixture.http.gates[0].received().await?;
        probe.finished(1).await?;
        fixture.peer.send(vec![speech()]).await?;
        // A second native endpoint call proves resumed PCM actually traversed
        // paced Silero, including its new local start and stop.
        probe.entered(1).await?;
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[1].received().await?;
        fixture.http.gates[1].release.notify_one();
        fixture.http.gates[1].completed().await?;
        // Give the event consumer a scheduling opportunity; this is not a
        // substitute for the call/HTTP barriers above and remains below 3 s.
        tokio::time::sleep(Duration::from_millis(120)).await;
        let premature = fixture.run.is_finished();
        probe.gates[1].release.store(true, Ordering::SeqCst);
        Ok(premature)
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    assert!(
        !work.unwrap(),
        "the earlier COMPLETE escaped after resumed speech"
    );
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["first phrase second phrase"]);
    assert_eq!(probe.snapshot().len(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_retains_audio_across_short_pause_then_fresh_complete_releases_one_response() {
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = scripted(&[false, true], &[1]);
    let fixture = Fixture::new(
        expected("first half final half"),
        vec![start_speech()],
        &["first half", "final half"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    let work: Result<(bool, bool), Error> = async {
        probe.finished(1).await?;
        fixture.http.gates[0].received().await?;
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[0].completed().await?;
        let after_incomplete = fixture.run.is_finished();
        fixture.peer.send(vec![speech()]).await?;
        probe.entered(1).await?;
        fixture.http.gates[1].received().await?;
        fixture.http.gates[1].release.notify_one();
        fixture.http.gates[1].completed().await?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let before_fresh_complete = fixture.run.is_finished();
        probe.gates[1].release.store(true, Ordering::SeqCst);
        Ok((after_incomplete, before_fresh_complete))
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    let (after_incomplete, before_fresh_complete) = work.unwrap();
    let calls = probe.snapshot();
    assert!(!after_incomplete && !before_fresh_complete);
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["first half final half"]);
    assert_eq!(calls.len(), 2);
    assert!(
        calls[1].samples > calls[0].samples,
        "INCOMPLETE discarded the earlier PCM: {calls:?}"
    );
    assert!(
        calls[1].voiced_spans >= 2,
        "both exact recorded voiced spans must survive: {calls:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn incomplete_uses_three_seconds_of_paced_pcm_silence_without_an_extra_model_verdict() {
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = scripted(&[false], &[]);
    let fixture = Fixture::new(
        expected("fallback phrase"),
        vec![start_speech()],
        &["fallback phrase"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    let work: Result<(bool, Duration), Error> = async {
        probe.finished(1).await?;
        fixture.http.gates[0].received().await?;
        let local_stop = Instant::now();
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[0].completed().await?;
        // Smart Turn treats latched Speaking/Stopping as speech. Its three
        // seconds start with locally Quiet paced zero PCM; the aggregator
        // alone must not turn the earlier 400 ms VAD stop into a response.
        tokio::time::sleep(Duration::from_millis(900)).await;
        let premature = fixture.run.is_finished();
        wait_until(|| fixture.run.is_finished()).await?;
        Ok((premature, local_stop.elapsed()))
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    let (premature, elapsed) = work.unwrap();
    assert!(
        !premature,
        "INCOMPLETE was treated as the ordinary 400 ms VAD stop"
    );
    assert!(
        elapsed >= Duration::from_millis(2_700),
        "fallback occurred too early after local stop: {elapsed:?}"
    );
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["fallback phrase"]);
    assert_eq!(
        probe.snapshot().len(),
        1,
        "silence fallback must not invent another model verdict"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stale_epoch_endpoint_and_http_cannot_emit_into_the_next_wire_turn() {
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = scripted(&[true, true], &[0, 1]);
    let script = scenario(json!([
        {"user":"first", "expect":[{"event":"user_transcription", "text_contains":"change epoch"}]},
        {"user":"second", "send_after":{"event":"user_transcription"}, "expect":[
            {"event":"response", "text_contains":"new epoch phrase", "text_excludes":"stale"}]}
    ]));
    let fixture = Fixture::new(
        script,
        vec![start_speech(), start_speech()],
        &["stale phrase", "new epoch phrase"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<bool, Error> = async {
        fixture.http.gates[0].received().await?;
        probe.entered(0).await?;
        fixture.peer.send(vec![marker("change epoch")]).await?;
        fixture.peer.wait_inputs(2).await?;
        wait_until(|| probe.gates[0].saw_cancellation.load(Ordering::SeqCst)).await?;
        probe.gates[0].release.store(true, Ordering::SeqCst);
        fixture.http.gates[0].release.notify_one();
        fixture.http.gates[1].received().await?;
        probe.entered(1).await?;
        fixture.http.gates[1].release.notify_one();
        fixture.http.gates[1].completed().await?;
        tokio::time::sleep(Duration::from_millis(120)).await;
        let premature = fixture.run.is_finished();
        probe.gates[1].release.store(true, Ordering::SeqCst);
        Ok(premature)
    }
    .await;
    let (result, peer) = fixture.finish(work.is_err()).await.checked();
    assert!(!work.unwrap(), "stale COMPLETE released the current epoch");
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["new epoch phrase"]);
    assert_eq!(peer.inputs.len(), 2);
    assert!(probe.gates[0].saw_cancellation.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_joins_the_blocked_predictor_before_the_owned_session_finishes() {
    let _serial = TEST_LOCK.lock().await;
    let (endpoint, probe) = scripted(&[true], &[0]);
    let fixture = Fixture::new(
        expected("must never emit"),
        vec![start_speech()],
        &["must never emit"],
        endpoint,
        probe.clone(),
    )
    .await
    .unwrap();
    fixture.http.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(bool, bool), Error> = async {
        fixture.http.gates[0].received().await?;
        probe.entered(0).await?;
        fixture.peer.known_cancel.store(true, Ordering::SeqCst);
        fixture.cancel.cancel();
        wait_until(|| probe.gates[0].saw_cancellation.load(Ordering::SeqCst)).await?;
        let finished_while_native_blocked = fixture.run.is_finished();
        let dropped_while_native_blocked = probe.dropped.load(Ordering::SeqCst);
        probe.gates[0].release.store(true, Ordering::SeqCst);
        Ok((finished_while_native_blocked, dropped_while_native_blocked))
    }
    .await;
    let (result, _) = fixture.finish(work.is_err()).await.checked();
    let (finished, dropped) = work.unwrap();
    assert!(
        !finished && !dropped,
        "session abandoned its still-running native predictor"
    );
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|failure| failure.reason.contains("cancel")),
        "{result:?}"
    );
    assert!(responses(&result).is_empty());
    assert!(probe.dropped.load(Ordering::SeqCst));
}

struct SettingsFiles(PathBuf);
impl SettingsFiles {
    fn new() -> Result<Self, Error> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "pipecat-smart-turn-settings-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&root)?;
        Ok(Self(root))
    }
    fn write(&self, name: &str, bytes: impl AsRef<[u8]>) -> Result<PathBuf, Error> {
        let path = self.0.join(name);
        std::fs::write(&path, bytes)?;
        Ok(path)
    }
    fn json(&self, name: &str, value: &Value) -> Result<PathBuf, Error> {
        self.write(name, serde_json::to_vec(value)?)
    }
}
impl Drop for SettingsFiles {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn absolute(path: PathBuf) -> Result<PathBuf, Error> {
    Ok(if path.is_absolute() {
        path
    } else {
        std::env::current_dir()?.join(path)
    })
}
fn settings_json(base: &str) -> Result<Value, Error> {
    // A unique fixture-only variable avoids dependence on actual credentials.
    // It is set before model/session owners and is never used by another test.
    static KEY: std::sync::Once = std::sync::Once::new();
    KEY.call_once(|| std::env::set_var("PIPECAT_SMART_TURN_TEST_DUMMY_KEY", "loopback-dummy"));
    let silero = std::env::var_os("PIPECAT_SILERO_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
        });
    Ok(json!({
        "base_url":base, "model":"fixture-stt", "api_key_env":"PIPECAT_SMART_TURN_TEST_DUMMY_KEY",
        "onnx_runtime":absolute(runtime().library_path)?, "silero_model":absolute(silero)?,
    }))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn optional_smart_turn_settings_are_strict_and_bad_graphs_fail_before_connect() {
    use pipecat_evals::BotTranscriptionSettings;
    let _serial = TEST_LOCK.lock().await;
    let before = vad_workers();
    let files = SettingsFiles::new().unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws.local_addr().unwrap());
    let base = settings_json(&format!("http://{}", http.local_addr().unwrap())).unwrap();
    let scenario_path = files
        .json(
            "scenario.json",
            &json!({"name":"preflight", "turns":[
                {"user":"play", "expect":[{"event":"response", "text_contains":"never connected"}]}]
            }),
        )
        .unwrap();
    let invalid_graph = files.write("invalid.onnx", b"not an ONNX graph").unwrap();
    let empty_graph = files.write("empty.onnx", b"").unwrap();
    let missing = files.0.join("missing.onnx");
    let mut load_checks = Vec::new();
    for (name, optional, accepted) in [
        ("null", Value::Null, true),
        ("real", json!(absolute(smart_model_path()).unwrap()), true),
        ("boolean", json!(true), false),
        ("number", json!(3), false),
        ("array", json!([]), false),
        ("missing", json!(missing), false),
        ("directory", json!(&files.0), false),
        ("empty", json!(empty_graph), false),
    ] {
        let mut value = base.clone();
        value["smart_turn_model"] = optional;
        let path = files.json(&format!("{name}.json"), &value).unwrap();
        let loaded = BotTranscriptionSettings::load(path);
        load_checks.push((name, accepted, loaded.is_ok()));
        drop(loaded);
    }
    let mut unknown = base.clone();
    unknown["smart_turn_modle"] = json!(smart_model_path());
    let unknown_path = files.json("unknown.json", &unknown).unwrap();
    let unknown_rejected = BotTranscriptionSettings::load(unknown_path).is_err();
    let serialized = serde_json::to_string(&base).unwrap();
    let duplicate = format!(
        "{},\"smart_turn_model\":null,\"smart_turn_model\":null}}",
        serialized.trim_end_matches('}')
    );
    let duplicate_path = files.write("duplicate.json", duplicate).unwrap();
    let duplicate_rejected = BotTranscriptionSettings::load(duplicate_path).is_err();
    let mut prepared = Vec::new();
    // Both existing default behavior and the explicit opt-in must prepare a
    // real session without dialing either listener. The malformed graph must
    // fail in the same owned asynchronous preparation path before any connect.
    for (name, optional, accepted) in [
        ("omitted", None, true),
        ("enabled", Some(absolute(smart_model_path()).unwrap()), true),
        ("invalid_graph", Some(invalid_graph), false),
    ] {
        let mut value = base.clone();
        if let Some(path) = optional {
            value["smart_turn_model"] = json!(path);
        }
        let path = files.json(&format!("prepare-{name}.json"), &value).unwrap();
        let result: Result<_, Error> = async {
            let settings = BotTranscriptionSettings::load(path)?;
            let sessions = settings
                .into_sessions_with_cancellation(
                    &scenario_path,
                    &ws_url,
                    options(),
                    false,
                    Cancellation::new(),
                )
                .await?;
            Ok(sessions)
        }
        .await;
        let error = result.as_ref().err().map(ToString::to_string);
        prepared.push((name, accepted, result.is_ok(), error));
        // Dropping prepared sessions settles their native owners before any
        // assertions or diagnostic failures below.
        drop(result);
    }
    let ws_connected = tokio::time::timeout(Duration::from_millis(30), ws.accept())
        .await
        .is_ok();
    let http_connected = tokio::time::timeout(Duration::from_millis(30), http.accept())
        .await
        .is_ok();
    drop(ws);
    drop(http);
    let observations = observe_vad_worker_cleanup(&before).await;
    eprintln!("Settings preparation outcomes: {prepared:#?}; cleanup: {observations:#?}");
    assert!(
        !ws_connected && !http_connected,
        "preparation connected before validation completed"
    );
    assert!(same_worker_identities(
        &observations.last().unwrap().workers,
        &before
    ));
    for (name, accepted, loaded) in load_checks {
        assert_eq!(loaded, accepted, "optional setting case {name}");
    }
    assert!(unknown_rejected && duplicate_rejected);
    for (name, accepted, actual, error) in prepared {
        assert_eq!(actual, accepted, "preparation case {name}: {error:?}");
        if name == "invalid_graph" {
            assert!(error.unwrap().contains("Smart Turn"));
        }
    }
}
