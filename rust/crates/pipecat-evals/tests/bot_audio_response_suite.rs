//! Real CLI suite -> owned compiled child -> real PCM/Silero -> loopback HTTP.
//! Scripted transcripts exercise protocol semantics, not recognition accuracy.
//! Child, pipe and HTTP owners are joined before behavioral assertions.
#![cfg(all(unix, feature = "bot-audio-transcription"))]

use pipecat_core::*;
use pipecat_evals::{
    bot_host::*,
    process::ProcessOptions,
    release::{run_release, run_release_with_budget, ReleaseManifest},
    BotTranscriptionSettings, Cancellation, EvalError, SessionOptions,
};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{Barrier, Notify},
    task::{JoinHandle, JoinSet},
};

#[path = "support/ports.rs"]
mod fixture_ports;
type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(30);
const CLEANUP: Duration = Duration::from_secs(3);
const PCM: &[u8] = include_bytes!("../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16");
const CLIP_BYTES: usize = 30_720;
const KEY_ENV: &str = "PIPECAT_SUITE_STT_KEY";
const SECRET: &str = "pipecat-suite-local-dummy-key-never-print-91427";
const SPOKEN: &str = "recognized fixture phrase";
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Bundle(PathBuf);
impl Bundle {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pipecat-spoken-suite-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn put(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }
    fn json(&self, name: &str, value: Value) -> PathBuf {
        self.put(name, serde_json::to_vec(&value).unwrap())
    }
    fn fifo(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        assert!(std::process::Command::new("mkfifo")
            .arg(&path)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .unwrap()
            .success());
        path
    }
    fn started(&self) -> Vec<PathBuf> {
        std::fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().path())
            .filter(|path| {
                path.file_name()
                    .unwrap()
                    .to_string_lossy()
                    .starts_with("started-")
            })
            .collect()
    }
}
impl Drop for Bundle {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn config(base: &str) -> Value {
    let runtime = std::env::var_os("PIPECAT_ONNX_RUNTIME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0"));
    let model = std::env::var_os("PIPECAT_SILERO_MODEL")
        .map(PathBuf::from)
        .unwrap_or_else(|| {
            PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
        });
    let absolute = |path: PathBuf| {
        if path.is_absolute() {
            path
        } else {
            std::env::current_dir().unwrap().join(path)
        }
    };
    json!({"base_url":base,"model":"fixture-stt","api_key_env":KEY_ENV,
        "onnx_runtime":absolute(runtime),"silero_model":absolute(model),"timeout_ms":10000})
}
fn scenario(name: &str, expected: &str, recorded: bool) -> Value {
    let mut value = json!({"name":name,"turns":[{"user":"play","expect":[
        {"event":"llm_response","text_contains":"correct LLM text","within_ms":7000},
        {"event":"response","text_contains":expected,"within_ms":7000}
    ]}]});
    if recorded {
        value["user"] = json!({"modality":"audio"});
        value["turns"][0]["audio"] = "input.wav".into();
    }
    value
}
fn manifest(b: &Bundle, files: &[&str], repeat: usize, concurrency: usize) -> Value {
    json!({"native_version":1,"repeat":repeat,"concurrency":concurrency,
        "base_port":fixture_ports::port(),"suite":[{"name":"spoken-fixture",
        "bot":std::env::current_exe().unwrap(),"scenarios":files,
        "args":["--exact","spoken_suite_bot","--nocapture","--test-threads=1","--skip","{port}"],
        "env":{"PIPECAT_SPOKEN_SUITE_CHILD":"1","PIPECAT_SPOKEN_SUITE_DIR":b.0},
        "concurrency":concurrency}]})
}
fn input_wav() -> Vec<u8> {
    let pcm = [7u8, 0].repeat(640);
    let mut bytes = Vec::new();
    bytes.extend(b"RIFF");
    bytes.extend((36 + pcm.len() as u32).to_le_bytes());
    bytes.extend(b"WAVEfmt ");
    bytes.extend(16u32.to_le_bytes());
    bytes.extend(1u16.to_le_bytes());
    bytes.extend(1u16.to_le_bytes());
    bytes.extend(16000u32.to_le_bytes());
    bytes.extend(32000u32.to_le_bytes());
    bytes.extend(2u16.to_le_bytes());
    bytes.extend(16u16.to_le_bytes());
    bytes.extend(b"data");
    bytes.extend((pcm.len() as u32).to_le_bytes());
    bytes.extend(pcm);
    bytes
}

struct Fixture {
    directory: PathBuf,
    port: u16,
    skip_tts: AtomicBool,
}
impl Fixture {
    fn write(&self, kind: &str, bytes: impl AsRef<[u8]>) -> Result<(), ProcessorError> {
        std::fs::write(self.directory.join(format!("{kind}-{}", self.port)), bytes)
            .map_err(|error| ProcessorError::fatal(error.to_string()))
    }
}
#[async_trait]
impl FrameProcessor for Fixture {
    fn name(&self) -> &str {
        "spoken-suite-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let input = match &frame.data {
            FrameData::LlmConfigureOutput { skip_tts } => {
                self.skip_tts.store(*skip_tts, Ordering::SeqCst);
                self.write("configured", skip_tts.to_string())?;
                false
            }
            FrameData::LlmMessagesAppend(messages) => {
                self.write(
                    "text",
                    messages
                        .messages
                        .first()
                        .map(|message| message.content.as_bytes())
                        .unwrap_or_default(),
                )?;
                true
            }
            FrameData::InputAudio(audio) if audio.audio == [7, 0].repeat(640) => {
                self.write("audio", &audio.audio)?;
                true
            }
            _ => false,
        };
        if input {
            for data in [
                FrameData::LlmResponseStart,
                FrameData::LlmText(TextFrame::new("correct LLM text")),
                FrameData::LlmResponseEnd,
            ] {
                ctx.push(Frame::new(data), Direction::Downstream).await?;
            }
            if !self.skip_tts.load(Ordering::SeqCst) {
                ctx.push(
                    Frame::new(FrameData::OutputAudio(
                        AudioRawFrame::new(PCM[..CLIP_BYTES].to_vec(), 16_000, 1).unwrap(),
                    )),
                    Direction::Downstream,
                )
                .await?;
            }
            // A returning data-queue fence, not a potentially overtaking system event.
            let delivered = LocalFlushEvent::default();
            ctx.push(
                Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                    event: Some(delivered.clone()),
                    returning: true,
                    origin: Some("spoken-suite-fixture".into()),
                })),
                Direction::Downstream,
            )
            .await?;
            tokio::time::timeout(CLEANUP, delivered.wait())
                .await
                .map_err(|_| ProcessorError::fatal("spoken suite PCM fence deadline"))?;
            ctx.push(
                Frame::new(FrameData::Transcription(TranscriptionFrame {
                    text: "PCM wire fence".into(),
                    finalized: true,
                    ..Default::default()
                })),
                Direction::Downstream,
            )
            .await?;
            self.write("fenced", b"done")?;
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.write("cleaned", b"done")
    }
}
struct Factory {
    directory: PathBuf,
    port: u16,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        std::fs::write(
            self.directory.join(format!("settings-{}", self.port)),
            settings.skip_tts.to_string(),
        )
        .unwrap();
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Fixture {
                directory: self.directory.clone(),
                port: self.port,
                skip_tts: AtomicBool::new(true),
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
// Auxiliary bootstrap entry, not a business test. No flag means no child work.
#[tokio::test]
async fn spoken_suite_bot() {
    if std::env::var_os("PIPECAT_SPOKEN_SUITE_CHILD").is_none() {
        return;
    }
    let args: Vec<_> = std::env::args().collect();
    let port: u16 = args[args.iter().position(|arg| arg == "--skip").unwrap() + 1]
        .parse()
        .unwrap();
    let directory = PathBuf::from(std::env::var_os("PIPECAT_SPOKEN_SUITE_DIR").unwrap());
    std::fs::write(
        directory.join(format!("started-{port}")),
        std::process::id().to_string(),
    )
    .unwrap();
    let host = EvalBotHost::bind(
        format!("127.0.0.1:{port}").parse().unwrap(),
        EvalBotHostConfig {
            startup_timeout: CLEANUP,
            session_timeout: WAIT,
            host_timeout: WAIT,
            shutdown_timeout: CLEANUP,
            transport: SessionConfig {
                queue_capacity: 64,
                close_timeout: Duration::from_millis(100),
                write_timeout: Duration::from_millis(300),
                ..Default::default()
            },
            pipeline: PipelineConfig {
                queue_capacity: 64,
                system_queue_capacity: 64,
                output_capacity: 64,
                ..Default::default()
            },
            event_capacity: 128,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_audio_output(true)
    .with_raw_audio_input(true);
    let report = host
        .serve(&Factory { directory, port }, Cancellation::new())
        .await
        .unwrap();
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
}

#[derive(Default)]
struct HttpGate {
    received: Notify,
    request: Mutex<Vec<u8>>,
    done: AtomicBool,
    closed: AtomicBool,
}
enum Reply {
    Text(&'static str),
    Together(Arc<Barrier>),
    Hold,
    InvalidateModel(PathBuf),
}
async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    let mut block = [0; 4096];
    let end = loop {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("HTTP EOF before headers".into());
        }
        bytes.extend_from_slice(&block[..n]);
        if let Some(i) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            if i + 4 > 16 * 1024 {
                return Err("HTTP header budget".into());
            }
            break i + 4;
        }
        if bytes.len() > 16 * 1024 {
            return Err("HTTP header budget".into());
        }
    };
    let length = std::str::from_utf8(&bytes[..end])?
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .ok_or("missing HTTP content length")?
        .parse::<usize>()?;
    if length > 512 * 1024 {
        return Err("HTTP body budget".into());
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
        return Err("HTTP trailing bytes".into());
    }
    Ok(bytes)
}
async fn serve_request(
    mut stream: TcpStream,
    reply: Reply,
    gate: Arc<HttpGate>,
) -> Result<(), Error> {
    let request = read_request(&mut stream).await?;
    *gate.request.lock().unwrap() = request;
    gate.received.notify_one();
    let text = match reply {
        Reply::Text(text) => text,
        Reply::Together(barrier) => {
            tokio::time::timeout(WAIT, barrier.wait())
                .await
                .map_err(|_| "concurrent HTTP barrier deadline")?;
            SPOKEN
        }
        Reply::InvalidateModel(path) => {
            // First HTTP means global preflight, first model load, child startup,
            // PCM capture and real native inference have already happened.
            std::fs::write(path, b"invalidated after first admitted spoken request")?;
            SPOKEN
        }
        Reply::Hold => {
            let mut byte = [0; 1];
            match stream.read(&mut byte).await {
                Ok(0) => {}
                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
                Ok(_) => return Err("bytes after held HTTP request".into()),
                Err(error) => return Err(error.into()),
            }
            gate.closed.store(true, Ordering::SeqCst);
            gate.done.store(true, Ordering::SeqCst);
            return Ok(());
        }
    };
    let body = json!({"text":text}).to_string();
    let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    stream.write_all(response.as_bytes()).await?;
    stream.shutdown().await?;
    gate.done.store(true, Ordering::SeqCst);
    Ok(())
}
struct HttpServer {
    base: String,
    gates: Vec<Arc<HttpGate>>,
    cancel: Cancellation,
    owner: JoinHandle<Vec<String>>,
}
struct HttpReport {
    gates: Vec<Arc<HttpGate>>,
    problems: Vec<String>,
}
impl HttpServer {
    async fn start(plans: Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}/v1", listener.local_addr().unwrap());
        let gates: Vec<_> = plans
            .iter()
            .map(|_| Arc::new(HttpGate::default()))
            .collect();
        let mut plans: VecDeque<_> = plans.into_iter().zip(gates.iter().cloned()).collect();
        let cancel = Cancellation::new();
        let stop = cancel.clone();
        let owner = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut problems = Vec::new();
            loop {
                tokio::select! {
                    biased;
                    _ = stop.cancelled() => break,
                    accepted = listener.accept() => match accepted {
                        Ok((stream, _)) => if let Some((reply, gate)) = plans.pop_front() {
                            tasks.spawn(serve_request(stream, reply, gate));
                        } else { problems.push("unexpected HTTP connection".into()); },
                        Err(error) => { problems.push(error.to_string()); break; }
                    },
                    result = tasks.join_next(), if !tasks.is_empty() => {
                        if let Some(result) = result {
                            match result { Ok(Ok(())) => {}, other => problems.push(format!("HTTP handler: {other:?}")) }
                        }
                    }
                }
            }
            // Listener cancellation never manufactures EOF on a held request.
            // Existing handlers finish or are aborted AND actually joined.
            // Also inspect the accept backlog: a ready cancellation branch
            // must not conceal a forbidden preflight connection.
            for _ in 0..32 {
                match tokio::time::timeout(Duration::from_millis(10), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        drop(stream);
                        problems.push("HTTP connection remained in shutdown backlog".into());
                    }
                    Ok(Err(error)) => {
                        problems.push(error.to_string());
                        break;
                    }
                    Err(_) => break,
                }
            }
            drop(listener);
            let drained = tokio::time::timeout(CLEANUP, async {
                while let Some(result) = tasks.join_next().await {
                    match result {
                        Ok(Ok(())) => {}
                        other => problems.push(format!("HTTP handler: {other:?}")),
                    }
                }
            })
            .await;
            if drained.is_err() {
                problems.push("HTTP handler drain deadline".into());
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
            }
            problems
        });
        Self {
            base,
            gates,
            cancel,
            owner,
        }
    }
    async fn finish(self) -> HttpReport {
        let mut problems = Vec::new();
        // Observe natural reply/EOF before requesting listener-owner shutdown.
        for gate in &self.gates {
            if !gate.request.lock().unwrap().is_empty()
                && !gate.done.load(Ordering::SeqCst)
                && tokio::time::timeout(CLEANUP, async {
                    while !gate.done.load(Ordering::SeqCst) {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                })
                .await
                .is_err()
            {
                problems.push("HTTP request did not reach reply/EOF".into());
            }
        }
        self.cancel.cancel();
        // The server owns every handler, so do not detach it on timeout.
        let mut owner = self.owner;
        let joined = match tokio::time::timeout(WAIT, &mut owner).await {
            Ok(joined) => joined,
            Err(_) => {
                problems.push("HTTP owner deadline".into());
                owner.await
            }
        };
        match joined {
            Ok(mut errors) => problems.append(&mut errors),
            Err(error) => problems.push(error.to_string()),
        }
        HttpReport {
            gates: self.gates,
            problems,
        }
    }
}
impl HttpReport {
    fn checked(&self, requests: usize) {
        assert!(self.problems.is_empty(), "{:?}", self.problems);
        assert_eq!(
            self.gates
                .iter()
                .filter(|gate| !gate.request.lock().unwrap().is_empty())
                .count(),
            requests
        );
        for gate in self.gates.iter().take(requests) {
            assert!(
                gate.done.load(Ordering::SeqCst),
                "HTTP reply/EOF not observed"
            );
            validate_request(&gate.request.lock().unwrap());
        }
    }
}
fn validate_request(request: &[u8]) {
    assert!(request.starts_with(b"POST /v1/audio/transcriptions "));
    let text = String::from_utf8_lossy(request);
    assert!(text
        .to_ascii_lowercase()
        .contains(&format!("authorization: bearer {SECRET}")));
    assert!(text.contains("multipart/form-data; boundary="));
    assert!(text.contains("name=\"model\"\r\n\r\nfixture-stt"));
    let offset = request
        .windows(4)
        .position(|window| window == b"RIFF")
        .unwrap();
    let len = u32::from_le_bytes(request[offset + 4..offset + 8].try_into().unwrap()) as usize + 8;
    let wav = pipecat_audio::read_pcm16_wav(&request[offset..offset + len]).unwrap();
    assert_eq!(wav.format.sample_rate(), 16_000);
    assert_eq!(wav.format.channels(), 1);
    let voiced = &PCM[25_600..27_648];
    assert!(voiced.iter().any(|byte| *byte != 0));
    assert!(
        wav.pcm.windows(voiced.len()).any(|window| window == voiced),
        "real speech missing or reordered"
    );
}

async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    (&mut pipe)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 1024 * 1024 {
        return Err(std::io::Error::other("suite CLI output bound"));
    }
    Ok(bytes)
}
fn interrupt(pid: Option<u32>) -> Result<(), Error> {
    let pid = pid.ok_or("suite CLI lacks live PID")?;
    let status = std::process::Command::new("kill")
        .args(["-INT", &pid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(format!("SIGINT failed: {status}").into());
    }
    Ok(())
}
async fn join_pipe(
    mut owner: JoinHandle<std::io::Result<Vec<u8>>>,
    problems: &mut Vec<String>,
) -> Vec<u8> {
    match tokio::time::timeout(CLEANUP, &mut owner).await {
        Ok(Ok(Ok(bytes))) => bytes,
        Ok(other) => {
            problems.push(format!("pipe owner: {other:?}"));
            Vec::new()
        }
        Err(_) => {
            owner.abort();
            let joined = owner.await;
            problems.push(format!("pipe deadline, actual join: {joined:?}"));
            Vec::new()
        }
    }
}
async fn await_fixture_children(directory: &Path, problems: &mut Vec<String>) {
    let entries = match std::fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) => {
            problems.push(format!("fixture child inventory: {error}"));
            return;
        }
    };
    let mut pids = Vec::new();
    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(error) => {
                problems.push(error.to_string());
                continue;
            }
        };
        if !entry.file_name().to_string_lossy().starts_with("started-") {
            continue;
        }
        match std::fs::read_to_string(entry.path())
            .ok()
            .and_then(|pid| pid.parse::<u32>().ok())
        {
            Some(pid) => pids.push(pid),
            None => problems.push("unreadable fixture child identity".into()),
        }
    }
    let live = |pid: &u32| {
        #[cfg(target_os = "linux")]
        {
            Path::new(&format!("/proc/{pid}")).exists()
        }
        #[cfg(not(target_os = "linux"))]
        {
            std::process::Command::new("kill")
                .args(["-0", &pid.to_string()])
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .is_ok_and(|status| status.success())
        }
    };
    // Even a failed CLI must not skip its compiled-child inventory. A hard
    // parent-kill fallback can bypass normal reaping: wait through the fixture's
    // own bounded host lifetime and report every surviving PID. Never blindly
    // kill a potentially reused PID or treat this wait as normal cleanup.
    if tokio::time::timeout(WAIT + CLEANUP, async {
        while pids.iter().any(&live) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .is_err()
    {
        problems.push(format!(
            "fixture child PID(s) remain after bounded host lifetime: {:?}",
            pids.into_iter().filter(live).collect::<Vec<_>>()
        ));
    }
}
async fn cli(
    path: &Path,
    settings: Option<&Path>,
    recorded_first: Option<bool>,
    cancel_after: Option<&Notify>,
) -> Result<Output, Error> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut command = Command::new(executable);
    command
        .arg("suite")
        .arg(path)
        .current_dir(path.parent().unwrap())
        .env(KEY_ENV, SECRET)
        .env("PIPECAT_ONNX_RUNTIME", "/missing/default-runtime.so")
        .env("PIPECAT_SILERO_MODEL", "/missing/default-silero.onnx")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if recorded_first == Some(true) {
        command.arg("--recorded-audio");
    }
    if let Some(settings) = settings {
        command.arg("--bot-audio-response").arg(settings);
    }
    if recorded_first == Some(false) {
        command.arg("--recorded-audio");
    }
    let mut child = command.spawn()?;
    let stdout = tokio::spawn(read_output(child.stdout.take().unwrap()));
    let stderr = tokio::spawn(read_output(child.stderr.take().unwrap()));
    let mut problems = Vec::new();
    if let Some(ready) = cancel_after {
        if tokio::time::timeout(WAIT, ready.notified()).await.is_err() {
            problems.push("CLI did not reach held HTTP cancellation barrier".into());
        }
        if let Err(error) = interrupt(child.id()) {
            problems.push(error.to_string());
        }
    }
    let status = match tokio::time::timeout(WAIT, child.wait()).await {
        Ok(Ok(status)) => Ok(status),
        other => {
            problems.push(format!("CLI wait: {other:?}"));
            if let Err(error) = interrupt(child.id()) {
                problems.push(error.to_string());
            }
            match tokio::time::timeout(CLEANUP, child.wait()).await {
                Ok(Ok(status)) => Ok(status),
                other => {
                    problems.push(format!("CLI cancellation wait: {other:?}"));
                    if let Err(error) = child.start_kill() {
                        problems.push(error.to_string());
                    }
                    child.wait().await
                }
            }
        }
    };
    let stdout = join_pipe(stdout, &mut problems).await;
    let stderr = join_pipe(stderr, &mut problems).await;
    await_fixture_children(path.parent().unwrap(), &mut problems).await;
    if !problems.is_empty() {
        return Err(format!(
            "{}; actual wait={status:?}; stdout={}; stderr={}",
            problems.join("; "),
            String::from_utf8_lossy(&stdout),
            String::from_utf8_lossy(&stderr)
        )
        .into());
    }
    Ok(Output {
        status: status?,
        stdout,
        stderr,
    })
}
fn output(out: Result<Output, Error>, code: i32) -> Output {
    let out = out.unwrap();
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains(SECRET),
        "dummy key leaked to stdout"
    );
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains(SECRET),
        "dummy key leaked to stderr"
    );
    assert_eq!(
        out.status.code(),
        Some(code),
        "stdout={}; stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    out
}
fn result(out: Result<Output, Error>, code: i32) -> Value {
    serde_json::from_slice(&output(out, code).stdout).unwrap()
}
fn reaped(value: &Value) {
    for run in value["runs"].as_array().unwrap() {
        if let Some(pid) = run["hosted"]["process"]["pid"].as_u64() {
            assert_eq!(run["hosted"]["process"]["reaped"], true, "{value}");
            #[cfg(target_os = "linux")]
            assert!(
                !Path::new(&format!("/proc/{pid}")).exists(),
                "child PID {pid} remains"
            );
        }
    }
}
fn child_markers(b: &Bundle, value: &Value, capture: bool, recorded: bool) {
    let runs = value["runs"].as_array().unwrap();
    assert_eq!(b.started().len(), runs.len());
    for run in runs {
        let port = run["port"].as_u64().unwrap();
        for kind in ["settings", "configured"] {
            assert_eq!(
                std::fs::read_to_string(b.0.join(format!("{kind}-{port}"))).unwrap(),
                (!capture).to_string()
            );
        }
        assert!(b.0.join(format!("cleaned-{port}")).exists());
        if recorded {
            assert_eq!(
                std::fs::read(b.0.join(format!("audio-{port}"))).unwrap(),
                [7, 0].repeat(640)
            );
            assert!(!b.0.join(format!("text-{port}")).exists());
        } else {
            assert!(b.0.join(format!("text-{port}")).exists());
        }
    }
}
fn responses(run: &Value) -> Vec<&str> {
    run["hosted"]["result"]["events_seen"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == "response")
        .map(|event| event["text"].as_str().unwrap())
        .collect()
}
fn library_settings(b: &Bundle, base: &str, bad_model: bool) -> BotTranscriptionSettings {
    let mut settings = config(base);
    // These library-only tests never issue HTTP, asserted with a zero-plan
    // server. Use an existing benign key value rather than mutate global env.
    settings["api_key_env"] = ["CARGO_MANIFEST_DIR", "HOME"]
        .into_iter()
        .find(|key| std::env::var(key).is_ok_and(|value| !value.is_empty()))
        .expect("library preflight needs a benign existing environment variable")
        .into();
    if bad_model {
        settings["silero_model"] = b
            .put("invalid-model.onnx", b"invalid native model")
            .to_string_lossy()
            .into_owned()
            .into();
    }
    BotTranscriptionSettings::load(b.json("library-config.json", settings)).unwrap()
}

#[tokio::test]
async fn cli_spoken_repeat_has_independent_concurrent_runs_and_reaps_each_child() {
    let b = Bundle::new();
    b.json("a.json", scenario("a", SPOKEN, false));
    b.json("b.json", scenario("b", SPOKEN, false));
    let path = b.json("manifest.json", manifest(&b, &["a.json", "b.json"], 2, 2));
    let first = Arc::new(Barrier::new(2));
    let second = Arc::new(Barrier::new(2));
    let http = HttpServer::start(vec![
        Reply::Together(first.clone()),
        Reply::Together(first),
        Reply::Together(second.clone()),
        Reply::Together(second),
    ])
    .await;
    let settings = b.json("config.json", config(&http.base));
    let out = cli(&path, Some(&settings), None, None).await;
    let http = http.finish().await;
    http.checked(4);
    let value = result(out, 0);
    reaped(&value);
    child_markers(&b, &value, true, false);
    assert_eq!(value["passed"], true);
    let runs = value["runs"].as_array().unwrap();
    assert_eq!(
        runs.iter()
            .map(|run| (
                run["attempt"].as_u64().unwrap(),
                run["scenario"].as_str().unwrap()
            ))
            .collect::<Vec<_>>(),
        [(1, "a"), (1, "b"), (2, "a"), (2, "b")]
    );
    let pids: std::collections::HashSet<_> = runs
        .iter()
        .map(|run| run["hosted"]["process"]["pid"].as_u64().unwrap())
        .collect();
    assert_eq!(pids.len(), 4);
    for run in runs {
        assert_eq!(responses(run), [SPOKEN]);
    }
}

#[tokio::test]
async fn cli_recorded_input_and_spoken_output_work_in_both_flag_orders() {
    for recorded_first in [false, true] {
        let b = Bundle::new();
        b.put("input.wav", input_wav());
        b.json("a.json", scenario("recorded", SPOKEN, true));
        let path = b.json("manifest.json", manifest(&b, &["a.json"], 1, 1));
        let http = HttpServer::start(vec![Reply::Text(SPOKEN)]).await;
        let settings = b.json("config.json", config(&http.base));
        let out = cli(&path, Some(&settings), Some(recorded_first), None).await;
        let http = http.finish().await;
        http.checked(1);
        let value = result(out, 0);
        reaped(&value);
        child_markers(&b, &value, true, true);
        assert_eq!(responses(&value["runs"][0]), [SPOKEN]);
    }
}

#[tokio::test]
async fn cli_spoken_mismatch_fails_independently_of_correct_llm_text() {
    let b = Bundle::new();
    b.json("a.json", scenario("independent", "correct LLM text", false));
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 1, 1));
    let http = HttpServer::start(vec![Reply::Text("wrong spoken phrase")]).await;
    let settings = b.json("config.json", config(&http.base));
    let out = cli(&path, Some(&settings), None, None).await;
    let http = http.finish().await;
    http.checked(1);
    let value = result(out, 1);
    reaped(&value);
    let run = &value["runs"][0];
    assert_eq!(
        run["hosted"]["result"]["turns"][0]["expectations"][0]["passed"],
        true
    );
    assert_eq!(
        run["hosted"]["result"]["turns"][0]["expectations"][0]["event_name"],
        "llm_response"
    );
    assert_eq!(responses(run), ["wrong spoken phrase"]);
    assert_eq!(value["passed"], false);
}

#[tokio::test]
async fn cli_cancellation_closes_http_reaps_active_child_and_never_spawns_pending() {
    let b = Bundle::new();
    b.json("a.json", scenario("cancel", SPOKEN, false));
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 3, 1));
    let http = HttpServer::start(vec![Reply::Hold]).await;
    let settings = b.json("config.json", config(&http.base));
    let out = cli(&path, Some(&settings), None, Some(&http.gates[0].received)).await;
    let http = http.finish().await;
    http.checked(1);
    let value = result(out, 1);
    reaped(&value);
    assert!(
        http.gates[0].closed.load(Ordering::SeqCst),
        "held request did not reach real EOF/reset"
    );
    assert_eq!(b.started().len(), 1);
    let runs = value["runs"].as_array().unwrap();
    assert!(runs[0]["hosted"]["process"]["pid"].is_number());
    assert!(runs[1..]
        .iter()
        .all(|run| run["hosted"]["process"]["pid"].is_null()));
    assert!(runs.iter().all(|run| run["hosted"]["result"]["failures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| failure["kind"] == "cancelled")));
    // Forced native-child cancellation cannot guarantee an application marker.
    if runs[0]["hosted"]["process"]["forced"] == false {
        assert!(b
            .0
            .join(format!("cleaned-{}", runs[0]["port"].as_u64().unwrap()))
            .exists());
    }
}

#[tokio::test]
async fn cli_invalid_second_scenario_or_classifier_rejects_before_all_children() {
    for recorded in [false, true] {
        let b = Bundle::new();
        b.put("input.wav", input_wav());
        b.json("a.json", scenario("valid", SPOKEN, recorded));
        let mut bad = scenario("bad", SPOKEN, recorded);
        if recorded {
            bad["turns"][0]["audio"] = "missing.wav".into();
        } else {
            bad["turns"][0]["expect"][1]["eval"] = "is accurate".into();
        }
        b.json("bad.json", bad);
        let path = b.json("manifest.json", manifest(&b, &["a.json", "bad.json"], 1, 2));
        let http = HttpServer::start(vec![]).await;
        let mut cfg = config(&http.base);
        cfg["silero_model"] = b
            .put("bad-model.onnx", b"native loading must not happen")
            .to_string_lossy()
            .into_owned()
            .into();
        let settings = b.json("config.json", cfg);
        let out = cli(&path, Some(&settings), recorded.then_some(true), None).await;
        let http = http.finish().await;
        http.checked(0);
        let out = output(out, 2);
        assert!(b.started().is_empty());
        let error = String::from_utf8_lossy(&out.stderr);
        assert!(
            error.contains(if recorded {
                "file I/O"
            } else {
                "not classifier judgments"
            }),
            "{error}"
        );
        assert!(
            !error.contains("could not be loaded"),
            "scenario validation must precede native inference"
        );
    }
    // Audio-only manifests and runner bodies use bounded regular-file reads,
    // even without the recorded-input flag. A FIFO must reject without waiting
    // for a writer or reaching native model loading.
    for body in [false, true] {
        let b = Bundle::new();
        b.json("a.json", scenario("a", SPOKEN, false));
        let fifo = b.fifo("not-regular.json");
        let path = if body {
            let mut value = manifest(&b, &["a.json"], 1, 1);
            value["suite"][0]["runner_body"] = json!({"path":fifo});
            value["suite"][0]["args"]
                .as_array_mut()
                .unwrap()
                .push("{runner_body}".into());
            b.json("manifest.json", value)
        } else {
            fifo
        };
        let http = HttpServer::start(vec![]).await;
        let mut cfg = config(&http.base);
        cfg["silero_model"] = b
            .put("bad-model.onnx", b"native loading must not happen")
            .to_string_lossy()
            .into_owned()
            .into();
        let settings = b.json("config.json", cfg);
        let out = cli(&path, Some(&settings), None, None).await;
        let http = http.finish().await;
        http.checked(0);
        let out = output(out, 2);
        assert!(String::from_utf8_lossy(&out.stderr).contains("regular files"));
        assert!(b.started().is_empty());
    }
}

#[tokio::test]
async fn cli_bad_model_global_preflight_prevents_every_child() {
    let b = Bundle::new();
    b.json("a.json", scenario("a", SPOKEN, false));
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 2, 2));
    let http = HttpServer::start(vec![]).await;
    let mut cfg = config(&http.base);
    cfg["silero_model"] = b
        .put("bad-model.onnx", b"not an ONNX model")
        .to_string_lossy()
        .into_owned()
        .into();
    let settings = b.json("config.json", cfg);
    let out = cli(&path, Some(&settings), None, None).await;
    let http = http.finish().await;
    http.checked(0);
    let out = output(out, 2);
    assert!(String::from_utf8_lossy(&out.stderr).contains("could not be loaded"));
    assert!(b.started().is_empty());
}

#[tokio::test]
async fn cli_model_invalidated_after_first_http_fails_later_attempt_without_spawn() {
    let b = Bundle::new();
    b.json("a.json", scenario("fresh model per attempt", SPOKEN, false));
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 2, 1));
    let mut cfg = config("http://127.0.0.1:1/v1");
    let copy = b.0.join("copied-model.onnx");
    std::fs::copy(cfg["silero_model"].as_str().unwrap(), &copy).unwrap();
    cfg["silero_model"] = copy.to_string_lossy().into_owned().into();
    let http = HttpServer::start(vec![Reply::InvalidateModel(copy)]).await;
    cfg["base_url"] = http.base.clone().into();
    let settings = b.json("config.json", cfg);
    let out = cli(&path, Some(&settings), None, None).await;
    let http = http.finish().await;
    http.checked(1);
    let value = result(out, 1);
    reaped(&value);
    assert_eq!(b.started().len(), 1);
    assert_eq!(value["runs"][0]["hosted"]["result"]["passed"], true);
    assert_eq!(responses(&value["runs"][0]), [SPOKEN]);
    assert!(value["runs"][1]["hosted"]["process"]["pid"].is_null());
    assert!(value["runs"][1]["hosted"]["result"]["failures"]
        .as_array()
        .unwrap()
        .iter()
        .any(|failure| failure["kind"] == "process_error"));
}

#[tokio::test]
async fn spoken_concurrency_caps_and_repeat_planning_are_bounded_before_native_load() {
    let b = Bundle::new();
    b.json("a.json", scenario("a", SPOKEN, false));
    let http = HttpServer::start(vec![]).await;
    let mut observations = Vec::new();
    for global in [false, true] {
        let mut source = manifest(&b, &["a.json"], 1, 1);
        if global {
            source["concurrency"] = 17.into();
        } else {
            source["suite"][0]["concurrency"] = 17.into();
        }
        let path = b.json("manifest.json", source);
        observations.push(
            ReleaseManifest::load_audio_response(
                path,
                library_settings(&b, &http.base, true),
                false,
            )
            .err(),
        );
    }
    // Seventeen attempts are metadata, not seventeen preloaded native models.
    // The deliberately invalid model must not be instantiated by plan loading.
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 17, 1));
    let planned =
        ReleaseManifest::load_audio_response(path, library_settings(&b, &http.base, true), false);
    let at_cap = ReleaseManifest::load_audio_response(
        b.json("manifest.json", manifest(&b, &["a.json"], 1, 16)),
        library_settings(&b, &http.base, true),
        false,
    );
    // Each source fits its 1 MiB file budget, but logical repeats and public
    // job clones must still fit the audio suite's total 4 MiB scenario budget.
    let mut large = scenario("large", SPOKEN, false);
    large["context"] = json!([{"role":"system","content":"x".repeat(600_000)}]);
    b.json("large.json", large);
    let repeated = ReleaseManifest::load_audio_response(
        b.json("manifest.json", manifest(&b, &["large.json"], 8, 1)),
        library_settings(&b, &http.base, true),
        false,
    )
    .err();
    let mut expanded = ReleaseManifest::load_audio_response(
        b.json("manifest.json", manifest(&b, &["large.json"], 1, 1)),
        library_settings(&b, &http.base, true),
        false,
    )
    .unwrap();
    let prototype = expanded.jobs[0].clone();
    expanded.jobs = (0..8)
        .map(|index| {
            let mut job = prototype.clone();
            job.attempt = index + 1;
            job.port += index as u16;
            job
        })
        .collect();
    let expanded = run_release(
        expanded,
        SessionOptions::default(),
        ProcessOptions::default(),
        Cancellation::new(),
    )
    .await;
    let http = http.finish().await;
    http.checked(0);
    assert!(observations.iter().all(|error| matches!(error, Some(EvalError::Scenario(message)) if message.contains("concurrency"))));
    assert_eq!(planned.unwrap().jobs.len(), 17);
    assert_eq!(at_cap.unwrap().concurrency, 16);
    assert!(matches!(
        repeated,
        Some(EvalError::Limit("release audio response scenario bytes"))
    ));
    assert!(matches!(
        expanded,
        Err(EvalError::Limit("release audio response scenario bytes"))
    ));
    assert!(b.started().is_empty());
}

#[tokio::test]
async fn public_spoken_plan_mutations_are_revalidated_before_any_child_or_model() {
    let b = Bundle::new();
    b.put("input.wav", input_wav());
    b.json("a.json", scenario("a", SPOKEN, true));
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 1, 1));
    let http = HttpServer::start(vec![]).await;
    let mut errors = Vec::new();
    for mutation in 0..4 {
        let mut plan = ReleaseManifest::load_audio_response(
            &path,
            library_settings(&b, &http.base, true),
            true,
        )
        .unwrap();
        match mutation {
            0 => plan.jobs[0].scenario.turns[0].expect[1].eval = Some("must be judged".into()),
            1 => {
                let turn = plan.jobs[0].scenario.turns[0].clone();
                plan.jobs[0].scenario.turns.push(turn);
            }
            2 => plan.concurrency = 17,
            _ => plan.jobs[0].concurrency = 17,
        }
        errors.push(
            run_release(
                plan,
                SessionOptions::default(),
                ProcessOptions::default(),
                Cancellation::new(),
            )
            .await
            .err(),
        );
    }
    let http = http.finish().await;
    http.checked(0);
    assert!(b.started().is_empty());
    assert!(
        matches!(&errors[0], Some(EvalError::Unsupported(message)) if message.contains("not classifier judgments"))
    );
    assert!(
        matches!(&errors[1], Some(EvalError::Scenario(message)) if message.contains("exactly one recording"))
    );
    assert!(errors[2..].iter().all(Option::is_some));
}

#[tokio::test]
async fn pre_cancel_and_summary_floor_return_without_starting_spoken_children() {
    let b = Bundle::new();
    b.json("a.json", scenario("a", SPOKEN, false));
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 4, 1));
    let http = HttpServer::start(vec![]).await;
    let cancelled_plan =
        ReleaseManifest::load_audio_response(&path, library_settings(&b, &http.base, true), false)
            .unwrap();
    let cancel = Cancellation::new();
    cancel.cancel();
    let cancelled = run_release(
        cancelled_plan,
        SessionOptions::default(),
        ProcessOptions::default(),
        cancel,
    )
    .await;
    let small_plan =
        ReleaseManifest::load_audio_response(&path, library_settings(&b, &http.base, true), false)
            .unwrap();
    let small = run_release_with_budget(
        small_plan,
        SessionOptions::default(),
        ProcessOptions::default(),
        Cancellation::new(),
        1024,
    )
    .await;
    let http = http.finish().await;
    http.checked(0);
    let cancelled = cancelled.unwrap();
    assert!(!cancelled.passed);
    assert_eq!(cancelled.runs.len(), 4);
    assert!(cancelled
        .runs
        .iter()
        .all(|run| run.hosted.process.pid.is_none()
            && run
                .hosted
                .result
                .failures
                .iter()
                .any(|failure| failure.kind == "cancelled")));
    assert!(matches!(
        small,
        Err(EvalError::Limit("release summary budget"))
    ));
    assert!(b.started().is_empty());
}

#[tokio::test]
async fn default_suite_keeps_text_response_alias_and_does_not_capture_pcm() {
    let b = Bundle::new();
    b.json(
        "a.json",
        json!({"name":"default text","turns":[{"user":"play","expect":[
            {"event":"response","text_contains":"correct LLM text","within_ms":7000}
        ]}]}),
    );
    let path = b.json("manifest.json", manifest(&b, &["a.json"], 1, 1));
    let http = HttpServer::start(vec![]).await;
    let out = cli(&path, None, None, None).await;
    let http = http.finish().await;
    http.checked(0);
    let value = result(out, 0);
    reaped(&value);
    child_markers(&b, &value, false, false);
    assert_eq!(value["passed"], true);
    assert!(responses(&value["runs"][0]).is_empty());
    let expectations = value["runs"][0]["hosted"]["result"]["turns"][0]["expectations"]
        .as_array()
        .unwrap();
    assert_eq!(expectations.len(), 1);
    assert_eq!(expectations[0]["event_name"], "llm_response");
    assert_eq!(expectations[0]["matched"], "correct LLM text");
}
