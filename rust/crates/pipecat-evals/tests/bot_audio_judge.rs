//! Real PCM -> genuine Silero -> HTTP STT -> native Jev HTTP -> report.
//! Scripted HTTP replies verify protocol semantics, not STT or judge accuracy.
//! All owned tasks/native workers settle before behavioral assertions.
#![cfg(feature = "bot-audio-transcription")]

use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_classifiers::{JevClassifier, JevConfig};
use pipecat_core::VadParams;
use pipecat_evals::{
    matcher::ScriptResult, BotTranscriptionConfig, BotTranscriptionSettings, Cancellation,
    EvalJudge, EvalSession, JudgeDecision, JudgeOptions, JudgeSettings, ScenarioFile,
    ScriptScenario, SessionOptions,
};
use pipecat_speech::{SpeechConfig, TranscriptionRequest};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
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
const WAIT: Duration = Duration::from_secs(12);
const JUDGE_SECRET: &str = "spoken-judge-synthetic-key-never-print";
const KEY_ENV: &str = "PIPECAT_SPOKEN_JUDGE_TEST_KEY";
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
fn expected() -> ScriptScenario {
    scenario(json!([{"user":"play the fixture", "expect":[
        {"event":"response", "eval":"satisfies the spoken criterion"}
    ]}]))
}
fn judge_options() -> JudgeOptions {
    JudgeOptions {
        timeout: Duration::from_secs(6),
        no_grace: Duration::from_millis(120),
        ..Default::default()
    }
}
fn settings(base: &str) -> JudgeSettings {
    static KEY: std::sync::Once = std::sync::Once::new();
    KEY.call_once(|| std::env::set_var(KEY_ENV, JUDGE_SECRET));
    serde_json::from_value(json!({"modality":"text", "eval":{
        "service":"jev", "base_url":base, "api_key_env":KEY_ENV,
        "timeout_ms":6000
    }}))
    .unwrap()
}
fn injected_judge(base: &str, options: JudgeOptions) -> Result<EvalJudge, Error> {
    let mut config = JevConfig::new(JUDGE_SECRET);
    config.base_url = base.into();
    config.max_retries = 0;
    config.timeout = options.timeout;
    Ok(EvalJudge::new(
        Arc::new(JevClassifier::owned(config)?),
        options,
    )?)
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
}

#[derive(Default)]
struct HttpGate {
    received: Notify,
    release: Notify,
    completed: Notify,
    request: Mutex<Vec<u8>>,
    replied: AtomicBool,
    closed: AtomicBool,
    disconnected: Notify,
    expected_disconnect: AtomicBool,
}
struct HttpPeer {
    base: String,
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
#[derive(Clone, Copy)]
enum HttpKind {
    Speech,
    Judge,
}
fn choice(decision: &str) -> Value {
    if decision == "call_yes" {
        return json!({"answers":{"verdict":{"type":"noul", "noul":0.9}}});
    }
    json!({"answers":{"verdict":{"type":"choice","choice":decision,"confidence":0.9,
        "probabilities":{"yes":0.4,"no":0.3,"continue":0.3}}}})
}
fn request_json(request: &[u8]) -> Result<Value, Error> {
    let end = request
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("HTTP request lacks header terminator")?
        + 4;
    Ok(serde_json::from_slice(&request[end..])?)
}
fn validate_judge(request: &[u8]) -> Result<(), Error> {
    let end = request
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or("HTTP request lacks header terminator")?
        + 4;
    let headers = std::str::from_utf8(&request[..end])?;
    if !headers.starts_with("POST /v1/systemone ")
        || !headers
            .to_ascii_lowercase()
            .contains(&format!("authorization: bearer {JUDGE_SECRET}\r\n"))
    {
        return Err("wrong classifier endpoint or synthetic credential".into());
    }
    let body = request_json(request)?;
    if !body["state"].is_object()
        || !matches!(
            body["questions"]["verdict"]["type"].as_str(),
            Some("choice" | "noul")
        )
    {
        return Err("unexpected classifier request schema".into());
    }
    Ok(())
}
async fn http(kind: HttpKind, replies: Vec<Value>) -> Result<HttpPeer, Error> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let base = format!("http://{}", listener.local_addr()?);
    let gates: Vec<_> = replies
        .iter()
        .map(|_| Arc::new(HttpGate::default()))
        .collect();
    let plans: Vec<_> = replies.into_iter().zip(gates.iter().cloned()).collect();
    let cancel = Cancellation::new();
    let cancelled = cancel.clone();
    let task = tokio::spawn(async move {
        let work = async {
            for (body, gate) in plans {
                let (mut stream, _) = listener.accept().await?;
                let request = read_request(&mut stream).await?;
                match kind {
                    HttpKind::Speech => validate_wav(&request)?,
                    HttpKind::Judge => validate_judge(&request)?,
                }
                *gate.request.lock().unwrap() = request;
                gate.received.notify_one();
                let mut probe = [0; 1];
                tokio::select! {
                    _ = gate.release.notified() => {
                        let body = body.to_string();
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
                                gate.disconnected.notify_one();
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    read = stream.read(&mut probe) => {
                        match read {
                            Ok(0) => {},
                            Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {},
                            Ok(_) => return Err::<(), Error>("unexpected data after complete HTTP request".into()),
                            Err(error) => return Err(error.into()),
                        }
                        if !gate.expected_disconnect.load(Ordering::SeqCst) {
                            return Err("HTTP client closed before authorized response release".into());
                        }
                        gate.closed.store(true, Ordering::SeqCst);
                        gate.disconnected.notify_one();
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
        base,
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
    async fn disconnected(&self) -> Result<(), Error> {
        tokio::time::timeout(WAIT, self.disconnected.notified()).await?;
        Ok(())
    }
    fn json(&self) -> Result<Value, Error> {
        request_json(&self.request.lock().unwrap())
    }
}
fn transcription_config(
    analyzer: SileroVadAnalyzer,
    base: &str,
) -> Result<BotTranscriptionConfig, Error> {
    let mut speech = SpeechConfig::openai("loopback-synthetic-stt-key");
    speech.base_url = base.into();
    speech.timeout = Duration::from_secs(8);
    speech.max_audio_bytes = 512 * 1024;
    speech.max_json_bytes = 16 * 1024;
    Ok(BotTranscriptionConfig::new(
        analyzer,
        speech,
        TranscriptionRequest::default(),
    )?)
}
// Always await aborted owners as well as successful owners. Assertions are
// deferred until this cleanup has completed, including on a barrier failure.
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
    speech: HttpPeer,
    judge: HttpPeer,
    cancel: Cancellation,
    run: JoinHandle<ScriptResult>,
    workers_before: WorkerSnapshot,
}
#[derive(Debug)]
struct GateState {
    request_bytes: usize,
    replied: bool,
    closed: bool,
    expected_disconnect: bool,
}
#[derive(Debug)]
struct Outcome {
    run: Result<ScriptResult, Error>,
    peer: Result<WireTrace, Error>,
    speech: Result<(), Error>,
    judge: Result<(), Error>,
    requests: Vec<Result<Value, String>>,
    workers_before: WorkerSnapshot,
    worker_observations: Vec<WorkerObservation>,
    speech_gates: Vec<GateState>,
    judge_gates: Vec<GateState>,
}
fn gate_states(peer: &HttpPeer) -> Vec<GateState> {
    peer.gates
        .iter()
        .map(|gate| GateState {
            request_bytes: gate.request.lock().unwrap().len(),
            replied: gate.replied.load(Ordering::SeqCst),
            closed: gate.closed.load(Ordering::SeqCst),
            expected_disconnect: gate.expected_disconnect.load(Ordering::SeqCst),
        })
        .collect()
}
async fn peers(
    outputs: Vec<Vec<Value>>,
    transcripts: &[&str],
    decisions: &[&str],
) -> Result<(Peer, HttpPeer, HttpPeer), Error> {
    let speech = http(
        HttpKind::Speech,
        transcripts
            .iter()
            .map(|text| json!({"text":text}))
            .collect(),
    )
    .await?;
    let judge = match http(
        HttpKind::Judge,
        decisions.iter().map(|decision| choice(decision)).collect(),
    )
    .await
    {
        Ok(peer) => peer,
        Err(error) => {
            speech.cancel.cancel();
            let _ = settle(speech.task).await;
            return Err(error);
        }
    };
    let peer = match websocket(outputs).await {
        Ok(peer) => peer,
        Err(error) => {
            speech.cancel.cancel();
            judge.cancel.cancel();
            let _ = settle(speech.task).await;
            let _ = settle(judge.task).await;
            return Err(error);
        }
    };
    Ok((peer, speech, judge))
}
impl Fixture {
    async fn new(
        mut scenario: ScriptScenario,
        outputs: Vec<Vec<Value>>,
        transcripts: &[&str],
        decisions: &[&str],
        options: SessionOptions,
        injected: Option<JudgeOptions>,
    ) -> Result<Self, Error> {
        let workers_before = vad_workers();
        let analyzer = analyzer();
        let (peer, speech, judge) = peers(outputs, transcripts, decisions).await?;
        let session: Result<EvalSession, Error> = (|| {
            let config = transcription_config(analyzer, &speech.base)?;
            Ok(match injected {
                Some(judge_options) => EvalSession::with_bot_transcription_and_judge(
                    scenario,
                    &peer.url,
                    options,
                    config,
                    injected_judge(&judge.base, judge_options)?,
                )?,
                None => {
                    scenario.judge = Some(settings(&judge.base));
                    EvalSession::with_bot_transcription(scenario, &peer.url, options, config)?
                }
            })
        })();
        let session = match session {
            Ok(session) => session,
            Err(error) => {
                peer.task.abort();
                let _ = peer.task.await;
                speech.cancel.cancel();
                judge.cancel.cancel();
                let _ = settle(speech.task).await;
                let _ = settle(judge.task).await;
                return Err(error);
            }
        };
        let cancel = Cancellation::new();
        let run = tokio::spawn(session.run(cancel.clone()));
        Ok(Self {
            peer,
            speech,
            judge,
            cancel,
            run,
            workers_before,
        })
    }
    async fn first_judgment(&self) -> Result<(), Error> {
        self.speech.gates[0].received().await?;
        self.speech.gates[0].release.notify_one();
        self.judge.gates[0].received().await
    }
    async fn finish(self, stop: bool) -> Outcome {
        if stop {
            self.peer.known_cancel.store(true, Ordering::SeqCst);
            self.cancel.cancel();
        }
        let run = settle(self.run).await;
        let speech_gates = gate_states(&self.speech);
        let judge_gates = gate_states(&self.judge);
        let requests = self
            .judge
            .gates
            .iter()
            .filter(|gate| !gate.request.lock().unwrap().is_empty())
            .map(|gate| gate.json().map_err(|error| error.to_string()))
            .collect();
        // Cancellation/timeout tests separately wait for actual HTTP EOF/reset
        // before stopping idle listeners; stopping this listener is not evidence.
        self.speech.cancel.cancel();
        self.judge.cancel.cancel();
        let peer = settle(self.peer.task).await.and_then(|result| result);
        let speech = settle(self.speech.task).await.and_then(|result| result);
        let judge = settle(self.judge.task).await.and_then(|result| result);
        let worker_observations = observe_vad_worker_cleanup(&self.workers_before).await;
        Outcome {
            run,
            peer,
            speech,
            judge,
            requests,
            workers_before: self.workers_before,
            worker_observations,
            speech_gates,
            judge_gates,
        }
    }
}
impl Outcome {
    fn checked(self) -> (ScriptResult, WireTrace, Vec<Value>) {
        eprintln!("Settled spoken-judge fixture: {self:#?}");
        for sample in &self.worker_observations {
            eprintln!(
                "Native worker /proc sample +{} ms: {:?}",
                sample.elapsed_ms, sample.workers
            );
        }
        for (kind, gates) in [("STT", &self.speech_gates), ("judge", &self.judge_gates)] {
            for (index, gate) in gates.iter().enumerate() {
                eprintln!("{kind} gate {index}: request_bytes={}, replied={}, closed={}, expected_disconnect={}",
                    gate.request_bytes, gate.replied, gate.closed, gate.expected_disconnect);
            }
        }
        self.speech.unwrap();
        self.judge.unwrap();
        assert!(
            same_worker_identities(
                &self.worker_observations.last().unwrap().workers,
                &self.workers_before
            ),
            "native VAD worker identities did not return to baseline after all owners joined"
        );
        let peer = self.peer.unwrap();
        assert!(peer.finished || peer.reset_after_cancel, "{peer:?}");
        let requests = self.requests.into_iter().map(Result::unwrap).collect();
        (self.run.unwrap(), peer, requests)
    }
}
fn responses(result: &ScriptResult) -> Vec<&str> {
    result
        .events_seen
        .iter()
        .filter(|event| event["type"] == "response")
        .map(|event| event["text"].as_str().unwrap())
        .collect()
}
fn decisions(result: &ScriptResult) -> Vec<JudgeDecision> {
    result.turns[0].expectations[0]
        .judgments
        .iter()
        .map(|verdict| verdict.decision)
        .collect()
}
// Tests hold TEST_LOCK. Check actual OS identities,
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

#[tokio::test]
async fn configured_judge_uses_actual_stt_text_and_reports_verdict_not_llm_text() {
    let _serial = TEST_LOCK.lock().await;
    let outputs = vec![vec![
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":"forbidden LLM-only answer"})),
        wire("bot-llm-stopped", json!({})),
        wire("bot-stopped-speaking", json!({})),
        speech(),
    ]];
    let fixture = Fixture::new(
        expected(),
        outputs,
        &["actual spoken fixture answer"],
        &["yes"],
        options(),
        None,
    )
    .await
    .unwrap();
    let work = fixture.first_judgment().await;
    let finished_before_verdict = fixture.run.is_finished();
    fixture.judge.gates[0].release.notify_one();
    let (result, peer, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(
        !finished_before_verdict,
        "run completed before actual classifier HTTP reply"
    );
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["actual spoken fixture answer"]);
    assert_eq!(decisions(&result), [JudgeDecision::Yes]);
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["state"],
        json!({
            "conversation":[{"speaker":"user", "text":"play the fixture"}],
            "latest_bot_reply":"actual spoken fixture answer"
        })
    );
    assert!(!requests[0].to_string().contains("forbidden LLM-only"));
    assert!(result.events_seen.iter().any(
        |event| event["type"] == "llm_response" && event["text"] == "forbidden LLM-only answer"
    ));
    let report = serde_json::to_value(&result).unwrap();
    assert_eq!(
        report["turns"][0]["expectations"][0]["event_name"],
        "response"
    );
    assert_eq!(
        report["turns"][0]["expectations"][0]["judgments"][0]["decision"],
        "yes"
    );
    assert!(!report.to_string().contains(JUDGE_SECRET));
    assert!(peer.cancelled);
    let url = url::Url::parse(&format!("ws://fixture{}", peer.request)).unwrap();
    assert_eq!(
        url.query_pairs().into_owned().collect::<Vec<_>>(),
        [
            ("custom".into(), "kept".into()),
            ("capture_bot_audio".into(), "true".into())
        ]
    );
    assert_eq!(peer.inputs[0]["data"]["options"]["audio_response"], true);
}

#[tokio::test]
async fn injected_continue_merges_two_completed_spoken_segments_once() {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(
        expected(),
        vec![start_speech()],
        &["first spoken segment", "second spoken segment"],
        &["continue", "yes"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        fixture.first_judgment().await?;
        fixture.judge.gates[0].release.notify_one();
        fixture.judge.gates[0].completed().await?;
        fixture.peer.send(vec![speech()]).await?;
        fixture.speech.gates[1].received().await?;
        fixture.speech.gates[1].release.notify_one();
        fixture.judge.gates[1].received().await?;
        fixture.judge.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(
        responses(&result),
        ["first spoken segment", "second spoken segment"]
    );
    assert_eq!(
        decisions(&result),
        [JudgeDecision::Continue, JudgeDecision::Yes]
    );
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["state"]["latest_bot_reply"],
        "first spoken segment"
    );
    assert_eq!(
        requests[1]["state"],
        json!({
            "conversation":[{"speaker":"user", "text":"play the fixture"}],
            "latest_bot_reply":"first spoken segment second spoken segment"
        })
    );
    let judgments = &result.turns[0].expectations[0].judgments;
    assert!(judgments[1].transcript_revision > judgments[0].transcript_revision);
    assert!(judgments.iter().all(|verdict| !verdict.cached));
}

#[tokio::test]
async fn no_grace_waits_for_local_pending_stt_despite_remote_stopped_speaking() {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(
        expected(),
        vec![start_speech()],
        &["incomplete spoken answer", "finishing spoken answer"],
        &["no", "yes"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    let mut premature = false;
    let work: Result<(), Error> = async {
        fixture.first_judgment().await?;
        fixture
            .peer
            .send(vec![speech(), wire("bot-stopped-speaking", json!({}))])
            .await?;
        // This real multipart admission proves local speech has produced a
        // second segment, whose transcript remains unavailable to the judge.
        fixture.speech.gates[1].received().await?;
        fixture.judge.gates[0].release.notify_one();
        fixture.judge.gates[0].completed().await?;
        // Deliberately cross the configured grace boundary while local HTTP
        // recognition is held. This duration tests the actual grace contract.
        tokio::time::sleep(judge_options().no_grace * 3).await;
        premature = fixture.run.is_finished();
        fixture.speech.gates[1].release.notify_one();
        fixture.judge.gates[1].received().await?;
        fixture.judge.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(
        !premature,
        "remote stop let No grace expire over pending local recognition"
    );
    assert!(result.passed, "{result:?}");
    assert!(
        result
            .events_seen
            .iter()
            .any(|event| event["type"] == "bot_stopped_speaking"),
        "remote stop report was never ingested"
    );
    assert_eq!(decisions(&result), [JudgeDecision::No, JudgeDecision::Yes]);
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[1]["state"]["latest_bot_reply"],
        "incomplete spoken answer finishing spoken answer"
    );
}

#[tokio::test]
async fn remote_speaking_and_continuous_silent_pcm_cannot_extend_local_no_grace() {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(
        expected(),
        vec![start_speech()],
        &["completed spoken answer"],
        &["no"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        fixture.first_judgment().await?;
        fixture
            .peer
            .send(vec![
                wire("bot-started-speaking", json!({})),
                audio(&[0; 1280], 16_000),
                marker("silent PCM pulse"),
                marker("remote speaking only"),
            ])
            .await?;
        Ok(())
    }
    .await;
    let released = tokio::time::Instant::now();
    fixture.judge.gates[0].release.notify_one();
    if work.is_ok() {
        let mut cadence = tokio::time::interval(Duration::from_millis(20));
        cadence.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        while !fixture.run.is_finished() && released.elapsed() < Duration::from_millis(1500) {
            cadence.tick().await;
            // Each marker shares the ordered wire with the immediately prior
            // PCM frame. Seeing that marker proves the client ingested silence,
            // whereas the sender's command acknowledgement alone would not.
            let sent = tokio::time::timeout(
                Duration::from_millis(100),
                fixture
                    .peer
                    .send(vec![audio(&[0; 1280], 16_000), marker("silent PCM pulse")]),
            )
            .await;
            if !matches!(sent, Ok(Ok(()))) {
                break;
            }
        }
    }
    let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
    let elapsed = released.elapsed();
    work.unwrap();
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|failure| failure.kind == "judge_no"),
        "{result:?}"
    );
    assert!(
        result
            .events_seen
            .iter()
            .any(|event| event["type"] == "bot_started_speaking"),
        "remote speaking report was never ingested"
    );
    assert!(
        result
            .events_seen
            .iter()
            .filter(|event| event["type"] == "user_transcription"
                && event["transcript"] == "silent PCM pulse")
            .count()
            >= 2,
        "continuous silent PCM was not observed on the actual wire"
    );
    assert_eq!(decisions(&result), [JudgeDecision::No]);
    assert_eq!(requests.len(), 1);
    assert!(
        elapsed < Duration::from_millis(1500),
        "remote speaking prolonged local-idle No beyond its grace: {elapsed:?}"
    );
}

#[tokio::test]
async fn interruption_cancels_old_classifier_before_its_late_yes_can_commit() {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(
        expected(),
        vec![start_speech()],
        &["old spoken answer"],
        &["yes"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    fixture.judge.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.first_judgment().await?;
        fixture
            .peer
            .send(vec![
                wire("bot-interrupted", json!({})),
                wire("bot-llm-started", json!({})),
                marker("new epoch boundary"),
            ])
            .await?;
        // A socket EOF/reset is a causal observation that the session has
        // invalidated and dropped the old classifier request. Peer.send alone
        // only proves enqueueing, so it is deliberately not the barrier here.
        fixture.judge.gates[0].disconnected().await?;
        fixture.judge.gates[0].release.notify_one();
        fixture.judge.gates[0].completed().await?;
        Ok(())
    }
    .await;
    let closed = fixture.judge.gates[0].closed.load(Ordering::SeqCst);
    let replied = fixture.judge.gates[0].replied.load(Ordering::SeqCst);
    let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(
        closed && !replied,
        "late Yes was released before actual request cancellation"
    );
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|failure| failure.kind == "judge_interrupted"),
        "{result:?}"
    );
    assert!(
        decisions(&result).is_empty(),
        "late old-epoch Yes entered the report"
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["state"]["latest_bot_reply"],
        "old spoken answer"
    );
}

#[tokio::test]
async fn cancellation_closes_actual_classifier_http_and_joins_all_owners() {
    let _serial = TEST_LOCK.lock().await;
    let fixture = Fixture::new(
        expected(),
        vec![start_speech()],
        &["recognized before cancel"],
        &["yes"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    fixture.judge.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.first_judgment().await?;
        fixture.peer.known_cancel.store(true, Ordering::SeqCst);
        fixture.cancel.cancel();
        fixture.judge.gates[0].completed().await?;
        Ok(())
    }
    .await;
    let closed = fixture.judge.gates[0].closed.load(Ordering::SeqCst);
    let (result, peer, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(
        closed,
        "classifier peer never observed actual EOF/reset on cancellation"
    );
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|failure| failure.kind == "cancelled"),
        "{result:?}"
    );
    assert!(result.turns[0]
        .expectations
        .iter()
        .all(|expectation| expectation.judgments.is_empty()));
    assert_eq!(requests.len(), 1);
    assert!(peer.cancelled);
}

#[tokio::test]
async fn classifier_timeout_closes_actual_http_and_does_not_report_a_verdict() {
    let _serial = TEST_LOCK.lock().await;
    let mut judge = judge_options();
    judge.timeout = Duration::from_millis(180);
    let fixture = Fixture::new(
        expected(),
        vec![start_speech()],
        &["recognized before deadline"],
        &["yes"],
        options(),
        Some(judge),
    )
    .await
    .unwrap();
    fixture.judge.gates[0]
        .expected_disconnect
        .store(true, Ordering::SeqCst);
    let work: Result<(), Error> = async {
        fixture.first_judgment().await?;
        fixture.judge.gates[0].completed().await?;
        Ok(())
    }
    .await;
    let closed = fixture.judge.gates[0].closed.load(Ordering::SeqCst);
    let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(
        closed,
        "classifier peer never observed actual EOF/reset on timeout"
    );
    assert!(!result.passed);
    assert!(
        result
            .failures
            .iter()
            .any(|failure| failure.kind == "judge_timeout"),
        "{result:?}"
    );
    assert!(result.turns[0]
        .expectations
        .iter()
        .all(|expectation| expectation.judgments.is_empty()));
    assert_eq!(requests.len(), 1);
}

struct SettingsFiles(PathBuf);
impl SettingsFiles {
    fn new() -> Result<Self, Error> {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let root = std::env::temp_dir().join(format!(
            "pipecat-spoken-judge-settings-{}-{}",
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

#[tokio::test]
async fn missing_or_invalid_spoken_judge_configuration_fails_before_models_or_connections() {
    let _serial = TEST_LOCK.lock().await;
    let before = vad_workers();
    let files = SettingsFiles::new().unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let speech = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let judge = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_url = format!("ws://{}", ws.local_addr().unwrap());
    let judge_settings =
        serde_json::to_value(settings(&format!("http://{}", judge.local_addr().unwrap()))).unwrap();
    let graph = files
        .write(
            "invalid.onnx",
            b"must reject judge before loading this invalid graph",
        )
        .unwrap();
    let runtime = std::env::var_os("PIPECAT_ONNX_RUNTIME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0"));
    let runtime = if runtime.is_absolute() {
        runtime
    } else {
        std::env::current_dir().unwrap().join(runtime)
    };
    let config_path = files
        .json(
            "speech.json",
            &json!({
                "base_url":format!("http://{}", speech.local_addr().unwrap()),
                "model":"fixture-stt", "api_key_env":KEY_ENV,
                "onnx_runtime":runtime, "silero_model":graph
            }),
        )
        .unwrap();
    let source = json!({"name":"all judged cases before native load", "turns":[
        {"user":"play", "expect":[{"event":"response", "eval":"spoken criterion"}]}
    ]});
    let mut checks = Vec::new();
    for case in [
        "missing_settings",
        "missing_eval",
        "missing_key",
        "invalid_timeout",
        "invalid_endpoint",
        "audio_modality",
        "llm_semantic",
    ] {
        let mut source = source.clone();
        if case != "missing_settings" {
            source["judge"] = judge_settings.clone();
        }
        match case {
            "missing_settings" => {}
            "missing_eval" => {
                source["judge"] = json!({"modality":"text"});
            }
            "missing_key" => {
                source["judge"]["eval"]["api_key_env"] =
                    format!("PIPECAT_SPOKEN_JUDGE_ABSENT_{}", std::process::id()).into();
            }
            "invalid_timeout" => {
                source["judge"]["eval"]["timeout_ms"] = 0.into();
            }
            "invalid_endpoint" => {
                source["judge"]["eval"]["base_url"] = "ftp://127.0.0.1".into();
            }
            "audio_modality" => {
                source["judge"]["modality"] = "audio".into();
            }
            "llm_semantic" => {
                source["turns"][0]["expect"][0]["event"] = "llm_response".into();
            }
            _ => unreachable!(),
        }
        let source_path = files.json(&format!("{case}.json"), &source).unwrap();
        let result = async {
            BotTranscriptionSettings::load(&config_path)?
                .into_sessions_with_cancellation(
                    source_path,
                    &ws_url,
                    options(),
                    false,
                    Cancellation::new(),
                )
                .await
        }
        .await;
        checks.push((case, result.as_ref().err().map(ToString::to_string)));
        drop(result);
    }
    let ws_connected = tokio::time::timeout(Duration::from_millis(30), ws.accept())
        .await
        .is_ok();
    let speech_connected = tokio::time::timeout(Duration::from_millis(30), speech.accept())
        .await
        .is_ok();
    let judge_connected = tokio::time::timeout(Duration::from_millis(30), judge.accept())
        .await
        .is_ok();
    drop((ws, speech, judge));
    let observations = observe_vad_worker_cleanup(&before).await;
    eprintln!("Spoken judge preflight cases: {checks:#?}; cleanup: {observations:#?}");
    assert!(
        !ws_connected && !speech_connected && !judge_connected,
        "invalid semantic-judge configuration contacted a server"
    );
    assert!(same_worker_identities(
        &observations.last().unwrap().workers,
        &before
    ));
    for (case, error) in checks {
        let error = error.unwrap_or_else(|| panic!("preflight accepted {case}"));
        assert!(
            error.contains("judge") || error.contains("classifier"),
            "wrong {case} failure: {error}"
        );
        assert!(
            !error.contains("could not be loaded") && !error.contains("Silero"),
            "native loading preceded semantic-judge preflight in {case}: {error}"
        );
        if case == "llm_semantic" {
            assert!(error.contains("llm_response"), "{error}");
        }
    }
}

#[test]
fn ordinary_response_alias_and_explicit_text_judge_constructor_remain_unchanged() {
    let raw = json!({"name":"default compatibility", "turns":[{"user":"question", "expect":[
        {"event":"response", "eval":"criterion"},
        {"event":"llm_response", "text_contains":"reported LLM answer"}
    ]}]});
    let text = ScenarioFile::from_value(raw).unwrap().scenarios.remove(0);
    assert_eq!(text.turns[0].expect[0].event, "llm_response");
    assert_eq!(text.turns[0].expect[1].event, "llm_response");
    assert!(EvalSession::with_judge(
        text,
        "ws://127.0.0.1:1",
        options(),
        injected_judge("http://127.0.0.1:1", judge_options()).unwrap()
    )
    .is_ok());
    assert!(
        EvalSession::new(expected(), "ws://127.0.0.1:1", options()).is_err(),
        "ordinary text constructor unexpectedly accepted typed spoken response"
    );
}

#[cfg(unix)]
async fn cli_output(
    files: &SettingsFiles,
    source: &std::path::Path,
    config: &std::path::Path,
    url: &str,
) -> Result<std::process::Output, Error> {
    use std::process::Stdio;
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut child = tokio::process::Command::new(executable)
        .arg("run")
        .arg(source)
        .arg(url)
        .arg("--stop-bot")
        .arg("--bot-audio-response")
        .arg(config)
        .current_dir(&files.0)
        .env(KEY_ENV, JUDGE_SECRET)
        .env("PIPECAT_ONNX_RUNTIME", "/missing/default-runtime.so")
        .env("PIPECAT_SILERO_MODEL", "/missing/default-silero.onnx")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let (stdout, stderr) = match (child.stdout.take(), child.stderr.take()) {
        (Some(stdout), Some(stderr)) => (stdout, stderr),
        _ => {
            let _ = child.start_kill();
            let waited = child.wait().await;
            return Err(format!("missing owned CLI output pipe; actual wait={waited:?}").into());
        }
    };
    async fn read(mut pipe: impl tokio::io::AsyncRead + Unpin) -> Result<Vec<u8>, Error> {
        let mut bytes = Vec::new();
        tokio::time::timeout(
            WAIT + Duration::from_secs(6),
            (&mut pipe).take(256 * 1024 + 1).read_to_end(&mut bytes),
        )
        .await??;
        if bytes.len() > 256 * 1024 {
            return Err("CLI output limit".into());
        }
        Ok(bytes)
    }
    // These three futures remain directly owned until join! completes. A pipe
    // read failure never drops the child waiter, and no pipe task is detached.
    let wait = async {
        match tokio::time::timeout(WAIT, child.wait()).await {
            Ok(Ok(status)) => Ok(status),
            failed => {
                let signal = child.id().map(|pid| {
                    std::process::Command::new("kill")
                        .args(["-INT", &pid.to_string()])
                        .stdin(Stdio::null())
                        .stdout(Stdio::null())
                        .stderr(Stdio::null())
                        .status()
                });
                let status = match tokio::time::timeout(Duration::from_secs(3), child.wait()).await
                {
                    Ok(status) => status,
                    Err(_) => {
                        let _ = child.start_kill();
                        child.wait().await
                    }
                };
                Err::<std::process::ExitStatus, Error>(format!("CLI initial wait failed={failed:?}; SIGINT={signal:?}; actual final wait={status:?}").into())
            }
        }
    };
    let (status, stdout, stderr) = tokio::join!(wait, read(stdout), read(stderr));
    Ok(std::process::Output {
        status: status?,
        stdout: stdout?,
        stderr: stderr?,
    })
}

#[cfg(unix)]
#[tokio::test]
async fn current_cli_run_consumes_explicit_spoken_judge_configuration() {
    let _serial = TEST_LOCK.lock().await;
    let files = SettingsFiles::new().unwrap();
    let outputs = vec![vec![
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":"CLI LLM-only decoy"})),
        wire("bot-llm-stopped", json!({})),
        speech(),
    ]];
    let (peer, speech, judge) = peers(outputs, &["CLI actual spoken answer"], &["yes"])
        .await
        .unwrap();
    let result: Result<std::process::Output, Error> = async {
        let runtime = std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0"));
        let model = std::env::var_os("PIPECAT_SILERO_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
            });
        let absolute = |path: PathBuf| -> Result<PathBuf, Error> {
            Ok(if path.is_absolute() {
                path
            } else {
                std::env::current_dir()?.join(path)
            })
        };
        let config = files.json(
            "speech.json",
            &json!({
                "base_url":speech.base, "model":"fixture-stt", "api_key_env":KEY_ENV,
                "onnx_runtime":absolute(runtime)?, "silero_model":absolute(model)?
            }),
        )?;
        let source = files.json("scenario.json", &json!({
            "name":"current CLI spoken semantic judgment", "judge":settings(&judge.base),
            "turns":[{"user":"play the fixture", "expect":[
                {"event":"llm_response", "text_contains":"CLI LLM-only decoy", "within_ms":7000},
                {"event":"response", "eval":"spoken criterion", "within_ms":7000}
            ]}]
        }))?;
        let wire = async {
            speech.gates[0].received().await?;
            speech.gates[0].release.notify_one();
            judge.gates[0].received().await?;
            judge.gates[0].release.notify_one();
            judge.gates[0].completed().await?;
            Ok::<(), Error>(())
        };
        let (output, wire) = tokio::join!(cli_output(&files, &source, &config, &peer.url), wire);
        wire?;
        output
    }
    .await;
    // The CLI, including its stdout/stderr futures, has already been reaped.
    // Settle both HTTP owners and the real WebSocket owner before assertions.
    speech.cancel.cancel();
    judge.cancel.cancel();
    let request = judge.gates[0].json();
    let peer = if result.is_err() {
        peer.task.abort();
        let _ = peer.task.await;
        Err("CLI fixture failed before normal WebSocket shutdown".into())
    } else {
        settle(peer.task).await.and_then(|result| result)
    };
    let speech = settle(speech.task).await.and_then(|result| result);
    let judge = settle(judge.task).await.and_then(|result| result);
    eprintln!(
        "Reaped CLI outcome: {result:?}; WebSocket={peer:?}; STT={speech:?}; judge={judge:?}"
    );
    speech.unwrap();
    judge.unwrap();
    let output = result.unwrap();
    let peer = peer.unwrap();
    assert!(peer.finished || peer.reset_after_cancel);
    assert!(peer.cancelled);
    assert!(output.status.success(), "{output:?}");
    assert!(!String::from_utf8_lossy(&output.stdout).contains(JUDGE_SECRET));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(JUDGE_SECRET));
    let result: ScriptResult = serde_json::from_slice(&output.stdout).unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["CLI actual spoken answer"]);
    assert_eq!(
        result.turns[0].expectations[1].judgments[0].decision,
        JudgeDecision::Yes
    );
    let request = request.unwrap();
    assert_eq!(
        request["state"]["latest_bot_reply"],
        "CLI actual spoken answer"
    );
    assert!(!request.to_string().contains("CLI LLM-only decoy"));
}

#[tokio::test]
async fn function_call_judge_uses_already_consumed_spoken_context_without_llm_text() {
    let _serial = TEST_LOCK.lock().await;
    let scenario = scenario(json!([{"user":"play the fixture", "expect":[
        {"event":"response", "text_contains":"spoken context for lookup", "eval":"spoken context is relevant"},
        {"event":"function_call", "name":"lookup", "args":{"city":"Paris"}, "eval":"right city"}
    ]}]));
    let outputs = vec![vec![
        wire("bot-llm-started", json!({})),
        wire("bot-llm-text", json!({"text":"forbidden LLM context"})),
        wire("bot-llm-stopped", json!({})),
        speech(),
    ]];
    let fixture = Fixture::new(
        scenario,
        outputs,
        &["spoken context for lookup"],
        &["yes", "call_yes"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        // A real first classifier request proves the spoken response was
        // consumed and recorded. HTTP STT write completion alone cannot prove
        // that the local aggregator has published its response event yet.
        fixture.first_judgment().await?;
        fixture
            .peer
            .send(vec![wire(
                "llm-function-call-in-progress",
                json!({
                    "function_name":"lookup", "arguments":{"city":"Paris", "extra":true}
                }),
            )])
            .await?;
        fixture.judge.gates[0].release.notify_one();
        fixture.judge.gates[1].received().await?;
        fixture.judge.gates[1].release.notify_one();
        Ok(())
    }
    .await;
    let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(responses(&result), ["spoken context for lookup"]);
    assert_eq!(decisions(&result), [JudgeDecision::Yes]);
    assert_eq!(
        result.turns[0].expectations[1].judgments[0].decision,
        JudgeDecision::Yes
    );
    assert_eq!(requests.len(), 2);
    assert_eq!(
        requests[0]["state"]["latest_bot_reply"],
        "spoken context for lookup"
    );
    assert_eq!(requests[1]["questions"]["verdict"]["type"], "noul");
    assert_eq!(
        requests[1]["state"],
        json!({
            "conversation":[{"speaker":"user", "text":"play the fixture"},
                {"speaker":"bot", "turn":1, "text":"spoken context for lookup"}],
            "call":{"name":"lookup", "arguments":{"city":"Paris", "extra":true}}
        })
    );
    assert!(requests
        .iter()
        .all(|request| !request.to_string().contains("forbidden LLM context")));
}

#[tokio::test]
async fn continue_exhausts_shared_deadline_and_malformed_verdict_fails_closed() {
    let _serial = TEST_LOCK.lock().await;
    for (decision, failure_kind) in [
        ("continue", "judge_continue"),
        ("invalid_verdict", "judge_error"),
    ] {
        let mut scenario = expected();
        scenario.turns[0].expect[0].within_ms = Some(3000);
        let fixture = Fixture::new(
            scenario,
            vec![start_speech()],
            &["recognized bounded answer"],
            &[decision],
            options(),
            Some(judge_options()),
        )
        .await
        .unwrap();
        let work = fixture.first_judgment().await;
        fixture.judge.gates[0].release.notify_one();
        let (result, _, requests) = fixture.finish(work.is_err()).await.checked();
        work.unwrap();
        assert!(!result.passed, "{decision}: {result:?}");
        assert!(
            result
                .failures
                .iter()
                .any(|failure| failure.kind == failure_kind),
            "{decision}: {result:?}"
        );
        assert_eq!(requests.len(), 1, "no retry is permitted for {decision}");
        assert_eq!(
            requests[0]["state"]["latest_bot_reply"],
            "recognized bounded answer"
        );
        if decision == "continue" {
            assert_eq!(decisions(&result), [JudgeDecision::Continue]);
            assert!(
                result.turns[0].duration_ms < 4000,
                "Continue reset the 3000 ms shared turn deadline: {result:?}"
            );
        } else {
            assert!(
                result.turns[0]
                    .expectations
                    .iter()
                    .all(|expectation| expectation.judgments.is_empty()),
                "invalid classifier choice entered the report: {result:?}"
            );
        }
        assert!(!serde_json::to_string(&result)
            .unwrap()
            .contains(JUDGE_SECRET));
    }
}

#[tokio::test]
async fn ordinary_spoken_predicate_enters_the_next_turn_judge_context_once() {
    let _serial = TEST_LOCK.lock().await;
    let scenario = scenario(json!([
        {"user":"first request", "expect":[{"event":"response", "text_contains":"first spoken answer"}]},
        {"user":"second request", "expect":[{"event":"response", "eval":"second answer is appropriate"}]}
    ]));
    let output = || {
        vec![
            wire("bot-llm-started", json!({})),
            wire(
                "bot-llm-text",
                json!({"text":"forbidden cross-turn LLM decoy"}),
            ),
            wire("bot-llm-stopped", json!({})),
            speech(),
        ]
    };
    let fixture = Fixture::new(
        scenario,
        vec![output(), output()],
        &["first spoken answer", "second spoken answer"],
        &["yes"],
        options(),
        Some(judge_options()),
    )
    .await
    .unwrap();
    let work: Result<(), Error> = async {
        fixture.speech.gates[0].received().await?;
        fixture.speech.gates[0].release.notify_one();
        // The second input's actual STT request is a causal barrier: the first
        // ordinary response expectation completed and the next user was sent.
        fixture.speech.gates[1].received().await?;
        fixture.speech.gates[1].release.notify_one();
        fixture.judge.gates[0].received().await?;
        fixture.judge.gates[0].release.notify_one();
        Ok(())
    }
    .await;
    let (result, peer, requests) = fixture.finish(work.is_err()).await.checked();
    work.unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(peer.inputs.len(), 2);
    assert_eq!(
        responses(&result),
        ["first spoken answer", "second spoken answer"]
    );
    assert!(result.turns[0].expectations[0].judgments.is_empty());
    assert_eq!(
        result.turns[1].expectations[0].judgments[0].decision,
        JudgeDecision::Yes
    );
    assert_eq!(requests.len(), 1);
    assert_eq!(
        requests[0]["state"],
        json!({
            "conversation":[{"speaker":"user", "text":"first request"},
                {"speaker":"bot", "turn":1, "text":"first spoken answer"},
                {"speaker":"user", "text":"second request"}],
            "latest_bot_reply":"second spoken answer"
        })
    );
    assert!(!requests[0]
        .to_string()
        .contains("forbidden cross-turn LLM decoy"));
}
