//! Real CLI -> native application host -> recorded PCM -> genuine Silero -> HTTP.
//! Scripted HTTP transcripts test the protocol, never recognition accuracy.
//! Every behavioral assertion follows child reaping and all server-owner joins.
#![cfg(all(unix, feature = "bot-audio-transcription"))]

use pipecat_core::*;
use pipecat_evals::{bot_host::*, matcher::ScriptResult, Cancellation};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
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
    sync::Notify,
    task::JoinHandle,
};

type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(15);
const CLEANUP: Duration = Duration::from_secs(3);
const PCM: &[u8] = include_bytes!("../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16");
const CLIP_BYTES: usize = 30_720;
const SECRET: &str = "pipecat-cli-local-dummy-key-never-print-90173";
const KEY_ENV: &str = "PIPECAT_TEST_STT_KEY";
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Bundle(PathBuf);
impl Bundle {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pipecat-spoken-cli-{}-{}",
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
    fn json(&self, name: &str, value: &Value) -> PathBuf {
        self.put(name, serde_json::to_vec(value).unwrap())
    }
    fn fifo(&self, name: &str) -> PathBuf {
        let path = self.0.join(name);
        // A local FIFO with no writer catches blocking reads before regular-file checks.
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
    let cwd = std::env::current_dir().unwrap();
    let runtime = if runtime.is_absolute() {
        runtime
    } else {
        cwd.join(runtime)
    };
    let model = if model.is_absolute() {
        model
    } else {
        cwd.join(model)
    };
    json!({
        "base_url": base,
        "model": "fixture-stt",
        "api_key_env": KEY_ENV,
        "onnx_runtime": runtime,
        "silero_model": model
    })
}
fn expectation(event: &str, text: &str) -> Value {
    json!({"event":event,"text_contains":text,"within_ms":6500})
}
fn one_turn(expected: &str) -> Value {
    json!({"name":"spoken CLI", "turns":[{"user":"play", "expect":[
        expectation("llm_response", "correct LLM text"),
        expectation("response", expected)
    ]}]})
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
    bytes.extend(16_000u32.to_le_bytes());
    bytes.extend(32_000u32.to_le_bytes());
    bytes.extend(2u16.to_le_bytes());
    bytes.extend(16u16.to_le_bytes());
    bytes.extend(b"data");
    bytes.extend((pcm.len() as u32).to_le_bytes());
    bytes.extend(pcm);
    bytes
}

#[derive(Default)]
struct Trace {
    settings: Mutex<Vec<bool>>,
    configured: Mutex<Vec<bool>>,
    inputs: Mutex<Vec<String>>,
    input_audio: Mutex<Vec<Vec<u8>>>,
    fences: AtomicUsize,
    cleaned: AtomicUsize,
    dropped: AtomicUsize,
    second_input: Notify,
}
#[derive(Default)]
struct HttpGate {
    received: Notify,
    completed: Notify,
    request: Mutex<Vec<u8>>,
    done: AtomicBool,
    closed: AtomicBool,
    replied: AtomicBool,
}
struct Fixture {
    trace: Arc<Trace>,
    skip_tts: AtomicBool,
    old_request: Option<Arc<HttpGate>>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
async fn push(ctx: &ProcessorContext, data: FrameData) -> Result<(), ProcessorError> {
    ctx.push(Frame::new(data), Direction::Downstream).await
}
async fn marker(ctx: &ProcessorContext, text: &str) -> Result<(), ProcessorError> {
    push(
        ctx,
        FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            finalized: true,
            ..Default::default()
        }),
    )
    .await
}
async fn pcm_fence(ctx: &ProcessorContext) -> Result<(), ProcessorError> {
    let delivered = LocalFlushEvent::default();
    push(
        ctx,
        FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(delivered.clone()),
            returning: true,
            origin: Some("spoken-cli-fixture".into()),
        }),
    )
    .await?;
    tokio::time::timeout(CLEANUP, delivered.wait())
        .await
        .map_err(|_| ProcessorError::fatal("spoken fixture root fence timeout"))?;
    // This uses the SAME observer queue as admitted PCM. System speaking
    // events can overtake PCM and therefore are never completion barriers.
    marker(ctx, "PCM wire fence").await
}
#[async_trait]
impl FrameProcessor for Fixture {
    fn name(&self) -> &str {
        "spoken-cli-application-fixture"
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
                self.trace.configured.lock().unwrap().push(*skip_tts);
                None
            }
            FrameData::LlmMessagesAppend(messages) => {
                let input = messages.messages.first().map(|m| m.content.clone());
                if let Some(input) = &input {
                    self.trace.inputs.lock().unwrap().push(input.clone());
                }
                input
            }
            FrameData::InputAudio(audio) => {
                self.trace
                    .input_audio
                    .lock()
                    .unwrap()
                    .push(audio.audio.clone());
                (audio.audio == [7, 0].repeat(640)).then(|| "play".into())
            }
            _ => None,
        };
        if let Some(input) = input {
            if input == "second" {
                // The CLI has already crossed its explicit send/epoch boundary.
                self.trace.second_input.notify_one();
            }
            for data in [
                FrameData::LlmResponseStart,
                FrameData::LlmText(TextFrame::new("correct LLM text")),
                FrameData::LlmResponseEnd,
            ] {
                push(ctx, data).await?;
            }
            if !self.skip_tts.load(Ordering::SeqCst) {
                push(
                    ctx,
                    FrameData::OutputAudio(
                        AudioRawFrame::new(PCM[..CLIP_BYTES].to_vec(), 16_000, 1).unwrap(),
                    ),
                )
                .await?;
            }
            pcm_fence(ctx).await?;
            self.trace.fences.fetch_add(1, Ordering::SeqCst);
            if input == "first" {
                if let Some(gate) = &self.old_request {
                    tokio::time::timeout(WAIT, gate.received.notified())
                        .await
                        .map_err(|_| ProcessorError::fatal("old HTTP request never arrived"))?;
                    marker(ctx, "change epoch").await?;
                }
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
    old_request: Option<Arc<HttpGate>>,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        self.trace.settings.lock().unwrap().push(settings.skip_tts);
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Fixture {
                trace: self.trace.clone(),
                skip_tts: AtomicBool::new(true),
                old_request: self.old_request.clone(),
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}

#[derive(Clone, Copy)]
enum Reply {
    Text(&'static str),
    Status(u16),
    InvalidJson,
    Hold,
    Stale,
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
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            if i + 4 > 16 * 1024 {
                return Err("HTTP header budget exceeded".into());
            }
            break i + 4;
        }
        if bytes.len() > 16 * 1024 {
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
    if length > 512 * 1024 {
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
        return Err("HTTP bytes after complete body".into());
    }
    Ok(bytes)
}
async fn http_server(
    listener: TcpListener,
    plans: Vec<Reply>,
    gates: Vec<Arc<HttpGate>>,
    trace: Arc<Trace>,
    cancel: Cancellation,
) -> Result<(), Error> {
    let mut plans: VecDeque<_> = plans.into_iter().zip(gates).collect();
    let work = async {
        loop {
            let (mut stream, _) = listener.accept().await?;
            let (plan, gate) = plans.pop_front().ok_or("unexpected extra HTTP request")?;
            let request = read_request(&mut stream).await?;
            *gate.request.lock().unwrap() = request;
            gate.received.notify_one();
            let body = match plan {
                Reply::Text(text) => Some((200, json!({"text":text}).to_string())),
                Reply::Status(status) => Some((status, json!({"error":SECRET}).to_string())),
                Reply::InvalidJson => Some((200, format!("invalid-json-{SECRET}"))),
                Reply::Hold | Reply::Stale => {
                    let mut byte = [0; 1];
                    tokio::select! {
                        _ = trace.second_input.notified(), if matches!(plan, Reply::Stale) => {
                            Some((200, json!({"text":"stale forbidden transcript"}).to_string()))
                        }
                        result = stream.read(&mut byte) => {
                            match result {
                                Ok(0) => {},
                                Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {},
                                Ok(_) => return Err::<(), Error>("unexpected bytes after request".into()),
                                Err(error) => return Err(error.into()),
                            }
                            gate.closed.store(true, Ordering::SeqCst);
                            None
                        }
                    }
                }
            };
            if let Some((status, body)) = body {
                let reply = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let result = async {
                    stream.write_all(reply.as_bytes()).await?;
                    stream.shutdown().await
                }
                .await;
                match result {
                    Ok(()) => gate.replied.store(true, Ordering::SeqCst),
                    Err(error)
                        if matches!(plan, Reply::Stale)
                            && matches!(
                                error.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::BrokenPipe
                            ) =>
                    {
                        gate.closed.store(true, Ordering::SeqCst);
                    }
                    Err(error) => return Err(error.into()),
                }
            }
            gate.done.store(true, Ordering::SeqCst);
            gate.completed.notify_one();
        }
    };
    tokio::select! {
        result = work => result,
        _ = cancel.cancelled() => Ok(()),
    }
}

async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    (&mut pipe)
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 256 * 1024 {
        return Err(std::io::Error::other("CLI output limit"));
    }
    Ok(bytes)
}
fn interrupt(pid: Option<u32>) -> Result<(), Error> {
    let pid = pid.ok_or("CLI has no live PID for SIGINT")?;
    let status = std::process::Command::new("kill")
        .args(["-INT", &pid.to_string()])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(format!("SIGINT command failed: {status}").into());
    }
    Ok(())
}
async fn join_pipe(
    mut owner: JoinHandle<std::io::Result<Vec<u8>>>,
    label: &str,
    problems: &mut Vec<String>,
) -> Vec<u8> {
    match tokio::time::timeout(CLEANUP, &mut owner).await {
        Ok(Ok(Ok(bytes))) => bytes,
        Ok(other) => {
            problems.push(format!("{label} owner: {other:?}"));
            Vec::new()
        }
        Err(_) => {
            owner.abort();
            let joined = owner.await;
            problems.push(format!("{label} deadline; actual join: {joined:?}"));
            Vec::new()
        }
    }
}
#[derive(Clone, Copy)]
enum Key {
    Present,
    Missing,
    Empty,
}
async fn cli(
    args: &[String],
    cwd: &Path,
    key: Key,
    cancel_after: Option<&Notify>,
) -> Result<Output, Error> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut command = Command::new(executable);
    command
        .args(args)
        .current_dir(cwd)
        .env_remove(KEY_ENV)
        .env("PIPECAT_ONNX_RUNTIME", "/missing/default-runtime.so")
        .env("PIPECAT_SILERO_MODEL", "/missing/default-silero.onnx")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    match key {
        Key::Present => {
            command.env(KEY_ENV, SECRET);
        }
        Key::Empty => {
            command.env(KEY_ENV, "");
        }
        Key::Missing => {}
    }
    let mut child = command.spawn()?;
    let stdout = tokio::spawn(read_output(child.stdout.take().unwrap()));
    let stderr = tokio::spawn(read_output(child.stderr.take().unwrap()));
    let mut problems = Vec::new();
    if let Some(ready) = cancel_after {
        if tokio::time::timeout(WAIT, ready.notified()).await.is_err() {
            problems.push("CLI never reached the HTTP cancellation barrier".into());
        }
        if let Err(error) = interrupt(child.id()) {
            problems.push(error.to_string());
        }
    }
    // Never detach a timed-out child. A fallback is recorded as failure even
    // when SIGINT/kill subsequently succeeds, and actual wait always occurs.
    let status: std::io::Result<ExitStatus> = match tokio::time::timeout(WAIT, child.wait()).await {
        Ok(Ok(status)) => Ok(status),
        other => {
            problems.push(format!("CLI wait failed or expired: {other:?}"));
            if let Err(error) = interrupt(child.id()) {
                problems.push(format!("timeout SIGINT: {error}"));
            }
            match tokio::time::timeout(CLEANUP, child.wait()).await {
                Ok(Ok(status)) => Ok(status),
                other => {
                    problems.push(format!("CLI SIGINT wait failed or expired: {other:?}"));
                    if let Err(error) = child.start_kill() {
                        problems.push(format!("CLI kill: {error}"));
                    }
                    child.wait().await
                }
            }
        }
    };
    let stdout = join_pipe(stdout, "stdout", &mut problems).await;
    let stderr = join_pipe(stderr, "stderr", &mut problems).await;
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
async fn settle<T>(mut owner: JoinHandle<T>) -> Result<T, Error> {
    match tokio::time::timeout(WAIT, &mut owner).await {
        Ok(joined) => joined.map_err(Into::into),
        Err(_) => {
            owner.abort();
            let actual = owner.await;
            Err(format!(
                "server owner deadline; aborted and actually joined: {}",
                actual.is_err()
            )
            .into())
        }
    }
}
struct Outcome {
    output: Result<Output, Error>,
    host: Result<Result<EvalBotHostReport, BotHostError>, Error>,
    http: Result<Result<(), Error>, Error>,
    barriers: Vec<String>,
    trace: Arc<Trace>,
    gates: Vec<Arc<HttpGate>>,
}
#[derive(Default)]
struct RunOptions {
    recorded: bool,
    text_only: bool,
    multiple: bool,
    cancel_http: bool,
    stale: bool,
    timeout_ms: Option<u64>,
    language: Option<Value>,
    symlink_config: bool,
}
async fn hosted(b: &Bundle, source: Value, plans: Vec<Reply>, options: RunOptions) -> Outcome {
    let scenario = b.json("case.json", &source);
    if options.recorded {
        b.put("input.wav", input_wav());
    }
    // Complete fallible setup before spawning either asynchronous server owner.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}/v1", listener.local_addr().unwrap());
    let mut settings = config(&base);
    if let Some(timeout) = options.timeout_ms {
        settings["timeout_ms"] = timeout.into();
    }
    if let Some(language) = options.language {
        settings["language"] = language;
    }
    let settings = if options.symlink_config {
        let target_dir = b.0.join("settings-target");
        std::fs::create_dir(&target_dir).unwrap();
        std::os::unix::fs::symlink(
            settings["onnx_runtime"].as_str().unwrap(),
            target_dir.join("local-runtime.so"),
        )
        .unwrap();
        std::os::unix::fs::symlink(
            settings["silero_model"].as_str().unwrap(),
            target_dir.join("local-model.onnx"),
        )
        .unwrap();
        settings["onnx_runtime"] = "local-runtime.so".into();
        settings["silero_model"] = "local-model.onnx".into();
        let target = b.json("settings-target/config.json", &settings);
        let link = b.0.join("config-link.json");
        std::os::unix::fs::symlink(target, &link).unwrap();
        link
    } else {
        b.json("config.json", &settings)
    };
    let host = EvalBotHost::bind(
        "127.0.0.1:0".parse().unwrap(),
        EvalBotHostConfig {
            transport: SessionConfig {
                queue_capacity: 64,
                connect_timeout: CLEANUP,
                write_timeout: Duration::from_millis(300),
                close_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            pipeline: PipelineConfig {
                queue_capacity: 64,
                system_queue_capacity: 64,
                output_capacity: 64,
                ..Default::default()
            },
            event_capacity: 128,
            startup_timeout: CLEANUP,
            session_timeout: WAIT,
            host_timeout: Duration::from_secs(40),
            shutdown_timeout: CLEANUP,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_audio_output(true)
    .with_raw_audio_input(options.recorded);
    let url = format!("ws://{}/", host.local_addr().unwrap());
    let trace = Arc::new(Trace::default());
    let gates: Vec<_> = plans
        .iter()
        .map(|_| Arc::new(HttpGate::default()))
        .collect();
    let factory = Factory {
        trace: trace.clone(),
        old_request: options.stale.then(|| gates[0].clone()),
    };
    let host_cancel = Cancellation::new();
    let http_cancel = Cancellation::new();
    let owned_cancel = host_cancel.clone();
    let host_owner = tokio::spawn(async move { host.serve(&factory, owned_cancel).await });
    let http_owner = tokio::spawn(http_server(
        listener,
        plans,
        gates.clone(),
        trace.clone(),
        http_cancel.clone(),
    ));
    let mut args = vec!["run".into(), scenario.display().to_string(), url];
    if !options.multiple {
        args.push("--stop-bot".into());
    }
    if options.recorded {
        args.push("--recorded-audio".into());
    }
    if !options.text_only {
        args.extend([
            "--bot-audio-response".into(),
            settings.display().to_string(),
        ]);
    }
    let output = cli(
        &args,
        &b.0,
        Key::Present,
        options.cancel_http.then(|| &gates[0].received),
    )
    .await;
    let mut barriers = Vec::new();
    // A received request must reach a real reply/EOF before server cancellation.
    // This keeps cancellation from manufacturing a false HTTP-cleanup success.
    for gate in &gates {
        let seen = !gate.request.lock().unwrap().is_empty();
        if seen
            && !gate.done.load(Ordering::SeqCst)
            && tokio::time::timeout(CLEANUP, gate.completed.notified())
                .await
                .is_err()
        {
            barriers.push("HTTP request did not reach reply/EOF before shutdown".into());
        }
    }
    host_cancel.cancel();
    http_cancel.cancel();
    let host = settle(host_owner).await;
    let http = settle(http_owner).await;
    Outcome {
        output,
        host,
        http,
        barriers,
        trace,
        gates,
    }
}
fn private_output(out: &Output) {
    assert!(
        !String::from_utf8_lossy(&out.stdout).contains(SECRET),
        "dummy key leaked to stdout"
    );
    assert!(
        !String::from_utf8_lossy(&out.stderr).contains(SECRET),
        "dummy key leaked to stderr"
    );
}
fn validate_request(request: &[u8], language: Option<&str>) {
    assert!(request.starts_with(b"POST /v1/audio/transcriptions "));
    let text = String::from_utf8_lossy(request);
    assert!(text
        .to_ascii_lowercase()
        .contains(&format!("authorization: bearer {SECRET}")));
    assert!(text.contains("multipart/form-data; boundary="));
    assert!(text.contains("name=\"model\"\r\n\r\nfixture-stt"));
    if let Some(language) = language {
        assert!(text.contains(&format!("name=\"language\"\r\n\r\n{language}")));
    } else {
        assert!(!text.contains("name=\"language\""));
    }
    let offset = request.windows(4).position(|w| w == b"RIFF").unwrap();
    let length =
        u32::from_le_bytes(request[offset + 4..offset + 8].try_into().unwrap()) as usize + 8;
    let wav = pipecat_audio::read_pcm16_wav(&request[offset..offset + length]).unwrap();
    assert_eq!(wav.format.sample_rate(), 16_000);
    assert_eq!(wav.format.channels(), 1);
    let voiced = &PCM[25_600..27_648];
    assert!(voiced.iter().any(|b| *b != 0));
    assert!(
        wav.pcm.windows(voiced.len()).any(|window| window == voiced),
        "actual fixture speech missing/reordered"
    );
}
impl Outcome {
    fn checked(
        self,
        code: i32,
        sessions: usize,
        capture: bool,
        language: Option<&str>,
    ) -> (Vec<ScriptResult>, Arc<Trace>, Vec<Arc<HttpGate>>) {
        // There are no remaining child, pipe, native host or HTTP task owners.
        assert!(self.barriers.is_empty(), "{:?}", self.barriers);
        self.http.unwrap().unwrap();
        let report = self.host.unwrap().unwrap();
        let out = self.output.unwrap();
        private_output(&out);
        assert_eq!(
            out.status.code(),
            Some(code),
            "stdout={}; stderr={}",
            String::from_utf8_lossy(&out.stdout),
            String::from_utf8_lossy(&out.stderr)
        );
        assert_eq!(report.sessions, sessions);
        assert_eq!(report.connections, sessions);
        assert_eq!(report.rejected_handshakes, 0);
        assert_eq!(self.trace.cleaned.load(Ordering::SeqCst), sessions);
        assert_eq!(self.trace.dropped.load(Ordering::SeqCst), sessions);
        assert_eq!(
            *self.trace.settings.lock().unwrap(),
            vec![!capture; sessions]
        );
        assert_eq!(
            *self.trace.configured.lock().unwrap(),
            vec![!capture; sessions]
        );
        for gate in &self.gates {
            assert!(
                gate.done.load(Ordering::SeqCst),
                "planned HTTP request never completed"
            );
            validate_request(&gate.request.lock().unwrap(), language);
        }
        let results = String::from_utf8(out.stdout)
            .unwrap()
            .lines()
            .map(|line| {
                let value: Value = serde_json::from_str(line).unwrap();
                assert!(
                    value.get("result").is_none(),
                    "stdout must retain ScriptResult NDJSON"
                );
                assert!(
                    value.get("chunks").is_none(),
                    "PCM must not leak into stdout"
                );
                serde_json::from_value(value).unwrap()
            })
            .collect::<Vec<ScriptResult>>();
        assert_eq!(results.len(), sessions);
        (results, self.trace, self.gates)
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
async fn cli_judges_spoken_response_independently_of_correct_llm_text() {
    for (spoken, wanted, pass, language) in [
        (
            "recognized fixture phrase",
            "recognized fixture phrase",
            true,
            None,
        ),
        (
            "wrong spoken phrase",
            "correct LLM text",
            false,
            Some(Value::Null),
        ),
        (
            "recognized English phrase",
            "recognized English phrase",
            true,
            Some(json!("en")),
        ),
    ] {
        let b = Bundle::new();
        let language_text = language.as_ref().and_then(Value::as_str).map(str::to_owned);
        let (results, trace, _) = hosted(
            &b,
            one_turn(wanted),
            vec![Reply::Text(spoken)],
            RunOptions {
                symlink_config: language_text.as_deref() == Some("en"),
                language,
                ..Default::default()
            },
        )
        .await
        .checked(if pass { 0 } else { 1 }, 1, true, language_text.as_deref());
        let result = &results[0];
        assert_eq!(result.passed, pass, "{result:?}");
        assert!(
            result.turns[0].expectations[0].passed,
            "LLM assertion must independently pass"
        );
        assert_eq!(result.turns[0].expectations[0].event_name, "llm_response");
        assert_eq!(result.turns[0].expectations[1].event_name, "response");
        assert_eq!(responses(result), [spoken]);
        assert_eq!(trace.fences.load(Ordering::SeqCst), 1);
        assert_eq!(*trace.inputs.lock().unwrap(), ["play"]);
    }
}

#[tokio::test]
async fn cli_two_scenarios_get_fresh_capture_and_distinct_http_responses() {
    let b = Bundle::new();
    let source = json!({"name":"two spoken scenarios", "scenarios":[
        {"name":"first", "turns":[{"user":"play", "expect":[expectation("response", "first scenario phrase")]}]},
        {"name":"second", "turns":[{"user":"play", "expect":[expectation("response", "second scenario phrase")]}]}
    ]});
    let (results, trace, _) = hosted(
        &b,
        source,
        vec![
            Reply::Text("first scenario phrase"),
            Reply::Text("second scenario phrase"),
        ],
        RunOptions {
            multiple: true,
            ..Default::default()
        },
    )
    .await
    .checked(0, 2, true, None);
    assert!(results.iter().all(|r| r.passed), "{results:?}");
    assert_eq!(results[0].scenario_name, "two spoken scenarios/first");
    assert_eq!(results[1].scenario_name, "two spoken scenarios/second");
    assert_eq!(responses(&results[0]), ["first scenario phrase"]);
    assert_eq!(responses(&results[1]), ["second scenario phrase"]);
    assert_eq!(trace.fences.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cli_send_after_response_waits_for_http_instead_of_llm_alias() {
    let b = Bundle::new();
    // Turn one finishes on LLM text. Only the send_after.response barrier can
    // prevent the next input from cancelling the not-yet-recognized PCM.
    let source = json!({"name":"spoken transcript send boundary", "turns":[
        {"user":"first", "expect":[expectation("llm_response", "correct LLM text")]},
        {"user":"second", "send_after":{"event":"response"},
         "expect":[expectation("response", "second spoken phrase")]}
    ]});
    let (results, trace, _) = hosted(
        &b,
        source,
        vec![
            Reply::Text("first spoken phrase"),
            Reply::Text("second spoken phrase"),
        ],
        RunOptions::default(),
    )
    .await
    .checked(0, 1, true, None);
    assert!(results[0].passed, "{:?}", results[0]);
    assert_eq!(
        responses(&results[0]),
        ["first spoken phrase", "second spoken phrase"]
    );
    assert_eq!(*trace.inputs.lock().unwrap(), ["first", "second"]);
    assert_eq!(trace.fences.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cli_explicit_next_send_discards_pending_old_generation_http() {
    let b = Bundle::new();
    let source = json!({"name":"old generation isolation", "turns":[
        {"user":"first", "expect":[expectation("user_transcription", "change epoch")]},
        {"user":"second", "send_after":{"event":"user_transcription"}, "expect":[
            {"event":"response", "text_contains":"new generation phrase", "text_excludes":"stale", "within_ms":6500}
        ]}
    ]});
    let (results, trace, gates) = hosted(
        &b,
        source,
        vec![Reply::Stale, Reply::Text("new generation phrase")],
        RunOptions {
            stale: true,
            ..Default::default()
        },
    )
    .await
    .checked(0, 1, true, None);
    assert!(results[0].passed, "{:?}", results[0]);
    assert_eq!(responses(&results[0]), ["new generation phrase"]);
    assert!(gates[0].closed.load(Ordering::SeqCst) || gates[0].replied.load(Ordering::SeqCst));
    assert_eq!(*trace.inputs.lock().unwrap(), ["first", "second"]);
    assert_eq!(trace.fences.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn cli_sigint_during_http_reaps_child_and_closes_real_request() {
    let b = Bundle::new();
    let (results, trace, gates) = hosted(
        &b,
        one_turn("never released"),
        vec![Reply::Hold],
        RunOptions {
            cancel_http: true,
            ..Default::default()
        },
    )
    .await
    .checked(1, 1, true, None);
    assert!(!results[0].passed);
    assert!(results[0]
        .failures
        .iter()
        .any(|failure| failure.kind == "cancelled" || failure.reason.contains("cancel")));
    assert!(responses(&results[0]).is_empty());
    assert!(
        gates[0].closed.load(Ordering::SeqCst),
        "HTTP owner never observed EOF/reset"
    );
    assert!(!gates[0].replied.load(Ordering::SeqCst));
    assert_eq!(trace.fences.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cli_http_timeout_status_and_invalid_json_are_run_failures_not_config_errors() {
    for (reply, timeout) in [
        (Reply::Hold, Some(80)),
        (Reply::Status(503), None),
        (Reply::InvalidJson, None),
    ] {
        let b = Bundle::new();
        let (results, _, gates) = hosted(
            &b,
            one_turn("never recognized"),
            vec![reply],
            RunOptions {
                timeout_ms: timeout,
                ..Default::default()
            },
        )
        .await
        .checked(1, 1, true, None);
        assert!(!results[0].passed);
        assert!(!results[0].failures.is_empty());
        assert!(responses(&results[0]).is_empty());
        if timeout.is_some() {
            assert!(
                gates[0].closed.load(Ordering::SeqCst),
                "HTTP timeout did not close request"
            );
        }
    }
}

#[tokio::test]
async fn cli_default_text_preserves_response_alias_without_loading_models() {
    let b = Bundle::new();
    let source = json!({"name":"default text alias", "turns":[{"user":"play", "expect":[
        expectation("response", "correct LLM text"),
        expectation("user_transcription", "PCM wire fence")
    ]}]});
    let (results, trace, _) = hosted(
        &b,
        source,
        vec![],
        RunOptions {
            text_only: true,
            ..Default::default()
        },
    )
    .await
    .checked(0, 1, false, None);
    assert!(results[0].passed);
    assert_eq!(
        results[0].turns[0].expectations[0].event_name,
        "llm_response"
    );
    assert!(responses(&results[0]).is_empty());
    assert_eq!(trace.fences.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn cli_recorded_user_input_combines_with_spoken_bot_response() {
    let b = Bundle::new();
    let mut source = one_turn("recorded mode reply");
    source["user"] = json!({"modality":"audio"});
    source["turns"][0]["audio"] = "input.wav".into();
    let (results, trace, _) = hosted(
        &b,
        source,
        vec![Reply::Text("recorded mode reply")],
        RunOptions {
            recorded: true,
            ..Default::default()
        },
    )
    .await
    .checked(0, 1, true, None);
    assert!(results[0].passed);
    assert_eq!(responses(&results[0]), ["recorded mode reply"]);
    assert!(trace.inputs.lock().unwrap().is_empty());
    let active = trace
        .input_audio
        .lock()
        .unwrap()
        .iter()
        .filter(|bytes| bytes.iter().any(|b| *b != 0))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(active, [[7, 0].repeat(640)]);
    assert_eq!(trace.fences.load(Ordering::SeqCst), 1);
}

struct Probe {
    address: String,
    count: Arc<AtomicUsize>,
    cancel: Cancellation,
    owner: JoinHandle<Result<(), Error>>,
}
impl Probe {
    fn start(listener: TcpListener, address: String) -> Self {
        let count = Arc::new(AtomicUsize::new(0));
        let counted = count.clone();
        let cancel = Cancellation::new();
        let cancelled = cancel.clone();
        let owner = tokio::spawn(async move {
            loop {
                tokio::select! {
                    // Drain connections already pending when cancellation is
                    // requested, so a late forbidden connect cannot be hidden.
                    biased;
                    result = listener.accept() => {
                        let (stream, _) = result?;
                        counted.fetch_add(1, Ordering::SeqCst);
                        drop(stream);
                    }
                    _ = cancelled.cancelled() => return Ok(()),
                }
            }
        });
        Self {
            address,
            count,
            cancel,
            owner,
        }
    }
}
async fn preflight(
    b: &Bundle,
    source: &Path,
    config_path: Option<&Path>,
    extra: &[String],
    key: Key,
    settings: Option<Value>,
    suite: bool,
) -> String {
    // Bind both listeners and capture addresses before starting either owner.
    let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_address = ws_listener.local_addr().unwrap().to_string();
    let http_address = http_listener.local_addr().unwrap().to_string();
    let ws = Probe::start(ws_listener, ws_address);
    let http = Probe::start(http_listener, http_address);
    // Settings were prepared before owners started. File I/O errors are stored
    // and asserted only after every owner has settled.
    let config_write = if let (Some(path), Some(mut settings)) = (config_path, settings) {
        if settings["base_url"] == "LOOPBACK" {
            settings["base_url"] = format!("http://{}/v1", http.address).into();
        }
        serde_json::to_vec(&settings)
            .map_err(std::io::Error::other)
            .and_then(|bytes| std::fs::write(path, bytes))
    } else {
        Ok(())
    };
    let mut args = if suite {
        vec!["suite".into(), source.display().to_string()]
    } else {
        vec![
            "run".into(),
            source.display().to_string(),
            format!("ws://{}/", ws.address),
        ]
    };
    if let Some(path) = config_path {
        args.extend(["--bot-audio-response".into(), path.display().to_string()]);
    }
    args.extend_from_slice(extra);
    let output = cli(&args, &b.0, key, None).await;
    ws.cancel.cancel();
    http.cancel.cancel();
    let ws_join = settle(ws.owner).await;
    let http_join = settle(http.owner).await;
    // Even a broken preflight implementation cannot strand a process or owner.
    ws_join.unwrap().unwrap();
    http_join.unwrap().unwrap();
    config_write.unwrap();
    let out = output.unwrap();
    private_output(&out);
    assert_eq!(
        out.status.code(),
        Some(2),
        "source={}; config={config_path:?}; extra={extra:?}; stdout={}; stderr={}",
        source.display(),
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(
        out.stdout.is_empty(),
        "preflight must not emit a partial scenario result"
    );
    assert!(!out.stderr.is_empty());
    assert_eq!(
        ws.count.load(Ordering::SeqCst),
        0,
        "invalid input reached WebSocket listener"
    );
    assert_eq!(
        http.count.load(Ordering::SeqCst),
        0,
        "invalid input reached HTTP listener"
    );
    String::from_utf8(out.stderr).unwrap()
}
fn rejects_with(stderr: &str, expected: &str) {
    assert!(
        stderr.contains(expected),
        "expected {expected:?}; stderr={stderr}"
    );
}

#[tokio::test]
async fn cli_strict_config_env_and_model_preflight_never_contacts_servers() {
    let b = Bundle::new();
    let source = b.json("case.json", &one_turn("unused"));
    let path = b.0.join("config.json");
    let valid = config("LOOPBACK");
    let mut variants: Vec<(String, Value)> = Vec::new();
    for field in [
        "base_url",
        "model",
        "api_key_env",
        "onnx_runtime",
        "silero_model",
    ] {
        let mut value = valid.clone();
        value.as_object_mut().unwrap().remove(field);
        variants.push((format!("missing {field}"), value));
    }
    for (field, bad) in [
        ("model", json!("")),
        ("api_key_env", json!("")),
        ("api_key_env", json!("BAD=ENV")),
        ("timeout_ms", json!(0)),
        ("timeout_ms", json!(30001)),
        ("timeout_ms", json!("8000")),
        ("language", json!(7)),
        ("onnx_runtime", json!("missing-runtime.so")),
        ("silero_model", json!("missing-silero.onnx")),
        ("unknown", json!(SECRET)),
        ("api_key", json!(SECRET)),
    ] {
        let mut value = valid.clone();
        value[field] = bad;
        variants.push((field.into(), value));
    }
    for bad in [
        "not-a-url",
        "file:///v1",
        "ftp://127.0.0.1/v1",
        "http://user:secret@127.0.0.1/v1",
        "http://127.0.0.1/v1?key=secret",
        "http://127.0.0.1/v1#fragment",
    ] {
        let mut value = valid.clone();
        value["base_url"] = bad.into();
        variants.push(("endpoint".into(), value));
    }
    let corrupt_model = b.put("bad.onnx", b"not an ONNX model");
    let mut invalid_model = valid.clone();
    invalid_model["silero_model"] = corrupt_model.display().to_string().into();
    variants.push(("model probe".into(), invalid_model));
    let corrupt_runtime = b.put("bad-runtime.so", b"not a native shared library");
    let mut invalid_runtime = valid.clone();
    invalid_runtime["onnx_runtime"] = corrupt_runtime.display().to_string().into();
    variants.push(("runtime probe".into(), invalid_runtime));
    for (label, value) in variants {
        eprintln!("preflight case: {label}");
        let stderr = preflight(
            &b,
            &source,
            Some(&path),
            &[],
            Key::Present,
            Some(value),
            false,
        )
        .await;
        if label == "endpoint" {
            rejects_with(
                &stderr,
                "bot transcription endpoint or credential is invalid",
            );
        } else if matches!(label.as_str(), "model probe" | "runtime probe") {
            rejects_with(
                &stderr,
                "bot transcription native runtime or model could not be loaded",
            );
        } else if label.starts_with("missing ") || matches!(label.as_str(), "unknown" | "api_key") {
            rejects_with(
                &stderr,
                "bot transcription configuration JSON fields or types",
            );
        }
    }
    for key in [Key::Missing, Key::Empty] {
        preflight(
            &b,
            &source,
            Some(&path),
            &[],
            key,
            Some(valid.clone()),
            false,
        )
        .await;
    }
    for (bytes, expected) in [
        (b"{".to_vec(), "configuration JSON fields or types"),
        (b"[]".to_vec(), "configuration JSON fields or types"),
        (b" ".repeat(32 * 1024 + 1), "recorded bundle file bytes"),
        (
            serde_json::to_string(&config("http://127.0.0.1:1/v1"))
                .unwrap()
                .replacen(
                    "\"model\":\"fixture-stt\"",
                    "\"model\":\"fixture-stt\",\"model\":\"fixture-stt\"",
                    1,
                )
                .into_bytes(),
            "configuration JSON fields or types",
        ),
    ] {
        b.put("config.json", bytes);
        let stderr = preflight(&b, &source, Some(&path), &[], Key::Present, None, false).await;
        rejects_with(&stderr, expected);
    }
    for (path, expected) in [
        (b.0.join("absent.json"), None),
        (b.0.clone(), Some("regular files")),
        (b.fifo("config.fifo"), Some("regular files")),
    ] {
        let stderr = preflight(&b, &source, Some(&path), &[], Key::Present, None, false).await;
        if let Some(expected) = expected {
            rejects_with(&stderr, expected);
        }
    }
}

#[tokio::test]
async fn cli_source_recording_scenario_limit_and_flags_preflight_is_atomic() {
    let b = Bundle::new();
    let source = b.json("case.json", &one_turn("unused"));
    let settings = b.0.join("config.json");
    let valid = config("LOOPBACK");
    let second_bad = b.json("second-bad.json", &json!({"name":"all before connect", "scenarios":[
        {"name":"valid", "turns":[{"user":"play", "expect":[expectation("response", "unused")]}]},
        {"name":"classifier", "turns":[{"user":"play", "expect":[{"event":"response", "eval":"is accurate"}]}]}
    ]}));
    let seventeen = b.json("seventeen.json", &json!({"name":"over limit", "scenarios":(0..17).map(|i| json!({
        "name":format!("scenario {i}"), "turns":[{"user":"play", "expect":[expectation("response", "unused")]}]
    })).collect::<Vec<_>>()}));
    let multi = b.json(
        "multi.json",
        &json!({"name":"multi", "scenarios":[
            {"name":"one", "turns":[{"user":"play"}]}, {"name":"two", "turns":[{"user":"play"}]}
        ]}),
    );
    let source_fifo = b.fifo("source.fifo");
    let recording_fifo = b.fifo("recording.fifo");
    b.put("input.wav", input_wav());
    let bad_recordings = ["missing.wav", "recording.fifo", "bad.wav"];
    b.put("bad.wav", b"invalid WAV");
    for bad in bad_recordings {
        let recording = b.json("recorded.json", &json!({"name":"all recordings before connect", "user":{"modality":"audio"}, "scenarios":[
            {"name":"valid", "turns":[{"user":"play", "audio":"input.wav", "expect":[expectation("response", "unused")]}]},
            {"name":"bad", "turns":[{"user":"play", "audio":bad, "expect":[expectation("response", "unused")]}]}
        ]}));
        let stderr = preflight(
            &b,
            &recording,
            Some(&settings),
            &["--recorded-audio".into()],
            Key::Present,
            Some(valid.clone()),
            false,
        )
        .await;
        if bad == "recording.fifo" {
            rejects_with(&stderr, "regular files");
        }
    }
    assert!(recording_fifo.exists());
    // Existing but invalid native assets pass bounded regular-file validation.
    // The exact scenario error proves rejection precedes native loading.
    let invalid_asset = b.put("native-must-not-load", b"invalid native asset bytes");
    let mut invalid_native = valid.clone();
    invalid_native["onnx_runtime"] = invalid_asset.display().to_string().into();
    invalid_native["silero_model"] = invalid_asset.display().to_string().into();
    for (bad_source, config, expected) in [
        (
            second_bad,
            invalid_native.clone(),
            "eval assertions require judge.eval configuration or EvalSession::with_judge",
        ),
        (
            seventeen,
            invalid_native,
            "bot transcription scenarios (1..16)",
        ),
        (source_fifo, valid.clone(), "regular files"),
    ] {
        let stderr = preflight(
            &b,
            &bad_source,
            Some(&settings),
            &[],
            Key::Present,
            Some(config),
            false,
        )
        .await;
        rejects_with(&stderr, expected);
    }
    preflight(
        &b,
        &multi,
        Some(&settings),
        &["--stop-bot".into()],
        Key::Present,
        Some(valid.clone()),
        false,
    )
    .await;
    for extra in [
        vec![
            "--bot-audio-response".into(),
            settings.display().to_string(),
        ],
        vec![
            "--bot-audio-wav".into(),
            b.0.join("never.wav").display().to_string(),
        ],
        vec!["--recorded-audio".into(), "--recorded-audio".into()],
        vec!["--stop-bot".into(), "--stop-bot".into()],
        vec!["--unknown".into()],
    ] {
        preflight(
            &b,
            &source,
            Some(&settings),
            &extra,
            Key::Present,
            Some(valid.clone()),
            false,
        )
        .await;
    }
    for missing_value in [
        vec!["--bot-audio-response".into()],
        vec!["--bot-audio-response".into(), "--stop-bot".into()],
    ] {
        preflight(&b, &source, None, &missing_value, Key::Present, None, false).await;
    }
    preflight(
        &b,
        &source,
        Some(&settings),
        &[],
        Key::Present,
        Some(valid),
        true,
    )
    .await;
    let default_send_after = b.json(
        "default-send-after.json",
        &json!({
            "name":"legacy text send-after contract", "turns":[
                {"user":"first", "expect":[expectation("response", "correct LLM text")]},
                {"user":"second", "send_after":{"event":"response"},
                 "expect":[expectation("llm_response", "correct LLM text")]}
            ]
        }),
    );
    let stderr = preflight(
        &b,
        &default_send_after,
        None,
        &[],
        Key::Present,
        None,
        false,
    )
    .await;
    assert_eq!(stderr.trim(), "unsupported native evaluation feature: event \"response\" (native session supports text mode only)");
    assert!(!b.0.join("never.wav").exists());
}

#[test]
fn default_parser_group_headers_includes_and_classifier_validation_remain_stable() {
    use pipecat_evals::ScenarioFile;

    let source = ScenarioFile::load(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/script.yaml"
    ))
    .unwrap();
    assert_eq!(
        source.scenarios[0].name,
        "native_contract/tools_and_markers"
    );
    assert_eq!(source.scenarios[0].turns[0].expect[2].event, "llm_response");
    assert_eq!(source.scenarios[0].turns[1].dtmf.as_deref(), Some("012#"));

    let b = Bundle::new();
    b.put("turns.yaml", "- user: first\n  expect:\n    - {event: response, text_contains: hello}\n    - {event: llm_response, eval: answers clearly}\n- dtmf: '012'\n");
    let path = b.put("group.yaml", "name: default-parser\ncontext: [{role: system, content: Be terse}]\nstop_on_failure: false\nturns: !include turns.yaml\nscenarios:\n  - {name: inherited}\n  - {name: replaced, context: []}\n");
    let file = ScenarioFile::load(path).unwrap();
    assert_eq!(file.scenarios.len(), 2);
    assert_eq!(file.scenarios[0].name, "default-parser/inherited");
    assert_eq!(file.scenarios[1].name, "default-parser/replaced");
    assert_eq!(file.scenarios[0].context.len(), 1);
    assert!(file.scenarios[1].context.is_empty());
    for scenario in &file.scenarios {
        assert!(!scenario.stop_on_failure);
        assert_eq!(scenario.turns[1].dtmf.as_deref(), Some("012"));
        assert_eq!(scenario.turns[0].expect[0].event, "llm_response");
        assert_eq!(scenario.turns[0].expect[1].event, "llm_response");
        // This checks existing classifier-criterion parsing, never a classifier run.
        assert_eq!(
            scenario.turns[0].expect[1].eval.as_deref(),
            Some("answers clearly")
        );
    }
    for (turns, expected) in [
        (json!([{"expect":[{"event":"typo"}]}]), "event \"typo\""),
        (
            json!([{"expect":[{"event":"user_transcription", "eval":"answers clearly"}]}]),
            "eval is supported only for text llm_response and function_call",
        ),
        (
            json!([{"expect":[{"event":"response", "eval":" "}]}]),
            "eval criterion must be nonempty",
        ),
        (
            json!([{"user":"next", "send_after":{"event":"response"}}]),
            "event \"response\" (native session supports text mode only)",
        ),
    ] {
        let error =
            ScenarioFile::from_value(json!({"name":"default invalid", "turns":turns})).unwrap_err();
        rejects_with(&error.to_string(), expected);
    }
}
