//! Joint hosted fixtures: complete user PCM precedes captured bot PCM. HTTP
//! replies are synthetic; multipart validation still requires genuine Silero PCM.
use super::fixture::{self, enabled as input, Bundle};
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*,
    process::ProcessOptions,
    release::{run_release, ReleaseManifest, ReleaseResult},
    BotTranscriptionSettings, Cancellation, EvalError, SessionOptions,
};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    io::Write,
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    process::Command,
    sync::{Notify, Semaphore},
    task::{JoinHandle, JoinSet},
};

pub const WAIT: Duration = Duration::from_secs(30);
pub const LLM: &str = "reported LLM decoy is not the spoken answer";
pub const TTS: &str = "reported TTS observer decoy is not acoustic truth";
pub const SPOKEN: &str = "actual hosted duplex spoken response";
pub const REFERENCE: &str = "literal user reference remains judgment context";
pub const STT_KEY: &str = "synthetic-hosted-duplex-stt-key-129";
pub const STT_ENV: &str = "PIPECAT_HOSTED_DUPLEX_STT_KEY_129";
pub const JUDGE_KEY: &str = "synthetic-hosted-duplex-judge-key-129";
pub const JUDGE_ENV: &str = "PIPECAT_HOSTED_DUPLEX_JUDGE_KEY_129";
const PCM: &[u8] = include_bytes!("../../../pipecat-vad/tests/fixtures/smart_turn_speech.pcm16");
const CLIP_BYTES: usize = 30_720;
type Error = Box<dyn std::error::Error + Send + Sync>;

// Process-local monotonic evidence; PID identifies independent clock origins.
fn elapsed_us() -> u128 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_micros()
}
pub fn stage(label: &str) {
    eprintln!(
        "joint-stage pid={} us={} {label}",
        std::process::id(),
        elapsed_us()
    );
}
pub fn snapshot(b: &Bundle, label: &str) {
    stage(label);
    for prefix in [
        "trace-",
        "tail-",
        "consumed-",
        "emitted-",
        "complete-",
        "invalid-",
        "cleaned-",
        "started-",
    ] {
        for path in b.markers(prefix) {
            eprintln!(
                "joint-marker {} {:?}",
                path.file_name().unwrap().to_string_lossy(),
                std::fs::read_to_string(&path).unwrap()
            );
        }
    }
}

pub fn source(name: &str) -> Value {
    let mut value = fixture::source(name, &[REFERENCE]);
    value["turns"][0]["expect"] = json!([
        expectation("llm_response", LLM),
        expectation("response", SPOKEN)
    ]);
    value
}
pub fn expectation(event: &str, text: &str) -> Value {
    json!({"event":event,"text_contains":text,"within_ms":7000})
}
pub fn entry(b: &Bundle, label: &str, file: &str, profile: &str) -> Value {
    let mut value = fixture::entry(b, label, file, profile);
    value["args"][1] = "audio_to_audio_suite_bot".into();
    value
}
pub fn one(b: &Bundle, name: &str, clips: &[Vec<u8>], mode: &str) -> PathBuf {
    input::profile(b, name, clips, &format!("isolated {name}"), mode, 0);
    b.json(&format!("{name}.json"), &source(name));
    b.json(
        &format!("{name}-manifest.json"),
        &fixture::manifest(vec![entry(b, name, &format!("{name}.json"), name)], 1, 1),
    )
}
pub fn stt_runtime(base: &str) -> Value {
    let absolute = |path: PathBuf| {
        if path.is_absolute() {
            path
        } else {
            std::env::current_dir().unwrap().join(path)
        }
    };
    let runtime = absolute(
        std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into()),
    );
    let model = absolute(
        std::env::var_os("PIPECAT_SILERO_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
            }),
    );
    json!({"base_url":base,"model":"fixture-stt","api_key_env":STT_ENV,
        "onnx_runtime":runtime,"silero_model":model,"timeout_ms":10000})
}
pub fn stt_settings(b: &Bundle, base: &str) -> BotTranscriptionSettings {
    static KEY: std::sync::Once = std::sync::Once::new();
    KEY.call_once(|| std::env::set_var(STT_ENV, STT_KEY));
    BotTranscriptionSettings::load(b.json("joint-stt-runtime.json", &stt_runtime(base))).unwrap()
}
pub fn judge_settings(base: &str) -> Value {
    static KEY: std::sync::Once = std::sync::Once::new();
    KEY.call_once(|| std::env::set_var(JUDGE_ENV, JUDGE_KEY));
    json!({"modality":"text","eval":{"service":"jev","base_url":base,"api_key_env":JUDGE_ENV,"timeout_ms":4000}})
}
pub fn options() -> SessionOptions {
    SessionOptions {
        stop_bot: true,
        default_timeout: Duration::from_secs(7),
        handshake_timeout: Duration::from_secs(2),
        bot_quiet_timeout: Duration::from_secs(7),
        max_duration: WAIT,
        ..Default::default()
    }
}
pub fn process_options() -> ProcessOptions {
    ProcessOptions {
        startup_timeout: Duration::from_secs(3),
        shutdown_grace: Duration::from_millis(500),
        output_drain_timeout: Duration::from_millis(500),
        ..Default::default()
    }
}
pub async fn run(
    plan: ReleaseManifest,
    options: SessionOptions,
    cancel: Cancellation,
) -> Result<ReleaseResult, EvalError> {
    let future = run_release(plan, options, process_options(), cancel.clone());
    tokio::pin!(future);
    match tokio::time::timeout(WAIT * 2, &mut future).await {
        Ok(result) => result,
        Err(_) => {
            cancel.cancel();
            let _actual = future.await;
            // Return the failure so the caller can join its fixture owners first.
            Err(EvalError::Timeout("joint fixture cooperative deadline"))
        }
    }
}
pub fn arguments(
    manifest: &Path,
    speech: &Path,
    transcription: &Path,
    reverse: bool,
) -> Vec<String> {
    let mut args = vec!["suite".into(), manifest.display().to_string()];
    let speech = ["--user-speech".into(), speech.display().to_string()];
    let stt = [
        "--bot-audio-response".into(),
        transcription.display().to_string(),
    ];
    if reverse {
        args.extend(stt);
        args.extend(speech);
    } else {
        args.extend(speech);
        args.extend(stt);
    }
    args
}
async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = vec![];
    (&mut pipe)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 1024 * 1024 {
        return Err(std::io::Error::other("joint CLI output budget"));
    }
    Ok(bytes)
}
pub async fn cli(args: &[String], cwd: &Path) -> Result<Output, String> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut command = Command::new(executable);
    command
        .args(args)
        .current_dir(cwd)
        .env(fixture::KEY_ENV, fixture::SECRET)
        .env(STT_ENV, STT_KEY)
        .env(JUDGE_ENV, JUDGE_KEY);
    // Only the loopback fixture is exempted. Production STT proxy policy is unchanged.
    for key in [
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        command.env_remove(key);
    }
    let mut child = command
        .env("NO_PROXY", "127.0.0.1,localhost")
        .env("no_proxy", "127.0.0.1,localhost")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|error| format!("joint CLI spawn: {error}"))?;
    let mut out = tokio::spawn(read_output(child.stdout.take().unwrap()));
    let mut err = tokio::spawn(read_output(child.stderr.take().unwrap()));
    let mut timed_out = false;
    let status = match tokio::time::timeout(WAIT * 2, child.wait()).await {
        Ok(status) => status,
        Err(_) => {
            if let Some(pid) = child.id() {
                let _ = Command::new("kill")
                    .arg("-INT")
                    .arg(pid.to_string())
                    .status()
                    .await;
            }
            timed_out = true;
            child.wait().await
        }
    };
    // Join both pipe owners before inspecting either result. Return diagnostics
    // to callers, which finish all HTTP fixtures before asserting success.
    let stdout = (&mut out).await;
    let stderr = (&mut err).await;
    let status = status.map_err(|error| format!("joint CLI wait: {error}"))?;
    let stdout = stdout
        .map_err(|error| format!("joint CLI stdout join: {error}"))?
        .map_err(|error| format!("joint CLI stdout: {error}"))?;
    let mut stderr = stderr
        .map_err(|error| format!("joint CLI stderr join: {error}"))?
        .map_err(|error| format!("joint CLI stderr: {error}"))?;
    if timed_out {
        stderr.extend_from_slice(b"\njoint CLI fixture deadline after cooperative child/pipe join");
    }
    Ok(Output {
        status,
        stdout,
        stderr,
    })
}
pub fn result(output: &Result<Output, String>, code: i32) -> Value {
    let output = output.as_ref().expect("joint CLI after fixture cleanup");
    fixture::assert_exit(output, code);
    assert!(!String::from_utf8_lossy(&output.stderr).contains("joint CLI fixture deadline"));
    for secret in [STT_KEY, JUDGE_KEY] {
        assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
        assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
    }
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    input::assert_reaped(&value);
    value
}
pub fn responses(run: &Value) -> Vec<&str> {
    run["hosted"]["result"]["events_seen"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|event| event["type"] == "response")
        .filter_map(|event| event["text"].as_str())
        .collect()
}
pub async fn observed(mut predicate: impl FnMut() -> bool) -> bool {
    tokio::time::timeout(WAIT, async {
        while !predicate() {
            // Poll a concrete observation; elapsed time is never the evidence.
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .is_ok()
}
/// The only remaining async owners must be these live fixture listeners. Each
/// handler completes naturally first; their cancellation has not been requested.
pub async fn retired_except_fixtures(count: usize) -> bool {
    tokio::time::timeout(Duration::from_secs(2), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            != count
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}
pub async fn retired() -> bool {
    tokio::time::timeout(Duration::from_secs(2), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .is_ok()
}
pub fn child_pids_gone(b: &Bundle) -> bool {
    b.markers("started-").iter().all(|path| {
        let pid = std::fs::read_to_string(path).unwrap();
        !Path::new(&format!("/proc/{pid}")).exists()
    })
}

#[derive(Clone, Deserialize)]
struct Profile {
    clips: Vec<PathBuf>,
    context: String,
    mode: String,
    wait_delta: i32,
}
struct Consumer(Arc<ResponseState>);
impl std::ops::Deref for Consumer {
    type Target = ResponseState;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
struct ResponseState {
    directory: PathBuf,
    port: u16,
    profile: Profile,
    context: SharedContext,
    clips: Mutex<VecDeque<VecDeque<Vec<u8>>>>,
    in_clip: AtomicBool,
    number: AtomicUsize,
}
// A marker witnesses cancellation dropping the tracked response future before
// processor cleanup. The marker is evidence only, never a lifetime mechanism.
struct FinalResponseOwner(Arc<ResponseState>);
impl Drop for FinalResponseOwner {
    fn drop(&mut self) {
        let _ = self.0.trace("response_owner_dropped");
        let _ = self.0.mark("response-owner-dropped", b"done");
    }
}
impl ResponseState {
    async fn finish_response(
        &self,
        n: usize,
        last_clip: bool,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.profile.mode == "gated"
            && !observed(|| {
                self.directory
                    .join(format!("release-{}", self.port))
                    .exists()
            })
            .await
        {
            return Err(ProcessorError::fatal("tail gate deadline"));
        }
        if self.profile.wait_delta != 0 && last_clip {
            self.trace("peer_gate_enter")?;
            self.mark("peer-enter", b"waiting")?;
            let peer = i32::from(self.port) + self.profile.wait_delta;
            if !observed(|| {
                self.directory.join(format!("complete-{peer}")).exists()
                    && (!self.directory.join("controlled-peer-hold").exists()
                        || self.directory.join("controlled-peer-release").exists())
            })
            .await
            {
                return Err(ProcessorError::fatal("peer completion gate deadline"));
            }
            self.trace("peer_gate_exit")?;
        }
        self.mark("consumed", n.to_string())?;
        self.answer(ctx).await?;
        if last_clip {
            std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(self.directory.join("completion-order"))
                .and_then(|mut file| writeln!(file, "{}", self.port))
                .map_err(|error| ProcessorError::new(error.to_string()))?;
            self.mark("complete", b"done")?;
        }
        Ok(())
    }

    fn trace(&self, label: &str) -> Result<(), ProcessorError> {
        let line = format!(
            "pid={} us={} port={} turn={} {label}\n",
            std::process::id(),
            elapsed_us(),
            self.port,
            self.number.load(Ordering::SeqCst)
        );
        std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(self.directory.join(format!("trace-{}", self.port)))
            .and_then(|mut f| f.write_all(line.as_bytes()))
            .map_err(|e| ProcessorError::new(e.to_string()))
    }

    fn mark(&self, prefix: &str, bytes: impl AsRef<[u8]>) -> Result<(), ProcessorError> {
        std::fs::write(
            self.directory.join(format!("{prefix}-{}", self.port)),
            bytes,
        )
        .map_err(|error| ProcessorError::new(error.to_string()))
    }
    async fn answer(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.profile.mode == "large"
            && !observed(|| self.directory.join("stt-held").exists()).await
        {
            return Err(ProcessorError::fatal("active STT barrier missing"));
        }
        let text = if self.profile.mode == "large" {
            format!("{LLM} {}", "result retention padding ".repeat(1024))
        } else {
            LLM.into()
        };
        for data in [
            FrameData::LlmResponseStart,
            FrameData::LlmText(TextFrame::new(text)),
            FrameData::OutputTransportMessageUrgent(
                json!({"label":"rtvi-ai","type":"bot-tts-text","data":{"text":TTS}}),
            ),
            FrameData::BotStoppedSpeaking,
        ] {
            ctx.push(Frame::new(data), Direction::Downstream).await?;
        }
        if self.profile.mode != "silent" {
            self.trace("output_pcm_enqueue")?;
            ctx.push(
                Frame::new(FrameData::OutputAudio(
                    AudioRawFrame::new(PCM[..CLIP_BYTES].to_vec(), 16_000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await?;
        }
        let delivered = LocalFlushEvent::default();
        ctx.push(
            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: Some(delivered.clone()),
                returning: true,
                origin: Some("hosted-duplex-fixture".into()),
            })),
            Direction::Downstream,
        )
        .await?;
        self.trace("flush_wait")?;
        tokio::time::timeout(WAIT, delivered.wait())
            .await
            .map_err(|_| ProcessorError::fatal("bot PCM flush deadline"))?;
        self.trace("flush_complete")?;
        ctx.push(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
            .await?;
        self.mark("emitted", b"captured")
    }
}
#[async_trait]
impl FrameProcessor for Consumer {
    fn name(&self) -> &str {
        "hosted-duplex-complete-pcm-consumer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            FrameData::InputAudio(audio)
                if self.in_clip.load(Ordering::SeqCst) || audio.audio.iter().any(|&b| b != 0) =>
            {
                self.in_clip.store(true, Ordering::SeqCst);
                std::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(self.directory.join(format!("audio-{}", self.port)))
                    .and_then(|mut file| file.write_all(&audio.audio))
                    .map_err(|error| ProcessorError::new(error.to_string()))?;
                let (last, valid) = {
                    let mut clips = self.clips.lock().unwrap();
                    match clips.front_mut() {
                        Some(clip) => {
                            let want = clip.pop_front().unwrap();
                            let last = clip.is_empty();
                            if last {
                                clips.pop_front();
                            }
                            (
                                last,
                                audio.audio == want
                                    && audio.sample_rate == 16_000
                                    && audio.num_channels == 1,
                            )
                        }
                        None => (false, false),
                    }
                };
                if !valid {
                    self.mark("invalid", b"wrong complete PCM/order/format/tail")?;
                }
                if last {
                    self.in_clip.store(false, Ordering::SeqCst);
                }
                if last
                    && valid
                    && !self
                        .directory
                        .join(format!("invalid-{}", self.port))
                        .exists()
                {
                    let context = serde_json::to_value(self.context.snapshot_rich()?)
                        .map_err(|error| ProcessorError::new(error.to_string()))?;
                    let actual = Value::Array(
                        context["messages"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|m| json!({"role":m["role"],"content":m["content"]}))
                            .collect(),
                    );
                    if actual != json!([{"role":"system","content":self.profile.context}]) {
                        self.mark("invalid", b"context changed")?;
                        return Err(ProcessorError::new("wrong context"));
                    }
                    let n = self.number.fetch_add(1, Ordering::SeqCst) + 1;
                    self.mark(&format!("tail-{n}"), b"arrived")?;
                    self.trace("final_pcm")?;
                    let last_clip = self.clips.lock().unwrap().is_empty();
                    if self.profile.wait_delta != 0 && last_clip {
                        // Only the final peer-ordered response can wait for another
                        // child. Keep input draining while it waits. The pipeline
                        // owns this task: Cancel aborts it and cleanup joins it.
                        // Earlier responses completed inline, so no response can
                        // overtake this final one or reorder the PCM contract.
                        let owner = FinalResponseOwner(self.0.clone());
                        let response_ctx = ctx.clone();
                        ctx.spawn("joint-final-peer-response", false, async move {
                            let response_owner = owner;
                            response_owner.0.finish_response(n, true, &response_ctx).await
                        })?;
                    } else {
                        self.finish_response(n, last_clip, ctx).await?;
                    }
                }
            }
            FrameData::Text(_) | FrameData::LlmMessagesAppend(_) | FrameData::Transcription(_) => {
                self.mark("text", b"reference shortcut")?
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.trace("cleanup")?;
        self.mark("cleaned", b"done")
    }
}
pub async fn cancel_held_peer_pipeline(b: &Bundle) {
    let text = "isolated native peer cancellation";
    let context = SharedContext::default();
    context
        .append_rich(ContextMessage {
            role: ChatRole::System,
            content: MessageContent::Text(text.into()),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
            extra: Default::default(),
        })
        .unwrap();
    let packets = input::packets(&input::pcm(223));
    let state = Arc::new(ResponseState {
        directory: b.0.clone(),
        port: 1,
        profile: Profile {
            clips: vec![],
            context: text.into(),
            mode: "normal".into(),
            wait_delta: 1,
        },
        context,
        clips: Mutex::new(VecDeque::from([packets.clone().into()])),
        in_clip: AtomicBool::new(false),
        number: AtomicUsize::new(0),
    });
    let weak = Arc::downgrade(&state);
    let mut worker = Pipeline::new(vec![Arc::new(Consumer(state)), Arc::new(IdentityProcessor)])
        .start(EvalBotHostConfig::default().pipeline)
        .await
        .unwrap();
    for packet in packets {
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(packet, 16_000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    let entered = observed(|| b.0.join("peer-enter-1").exists()).await;
    let cancel = worker.cancel("cancel held final peer response").await;
    let joined = worker.join().await;
    drop(worker);
    snapshot(b, "native_peer_cancel_joined");
    assert!(entered, "{joined:?}");
    cancel.unwrap();
    joined.unwrap();
    assert!(
        weak.upgrade().is_none(),
        "response owner remains alive after pipeline join"
    );
    assert!(b.0.join("response-owner-dropped-1").exists());
    assert!(b.0.join("cleaned-1").exists());
    assert!(b.markers("complete-").is_empty());
    assert!(b.markers("emitted-").is_empty());
    let trace = std::fs::read_to_string(b.0.join("trace-1")).unwrap();
    assert!(trace.find("response_owner_dropped").unwrap() < trace.find("cleanup").unwrap());
}

struct Factory {
    directory: PathBuf,
    port: u16,
    profile: Profile,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if settings.skip_tts {
            return Err(BotHostError::Invalid(
                "joint route lost capture_bot_audio".into(),
            ));
        }
        let context = SharedContext::default();
        let clips = self
            .profile
            .clips
            .iter()
            .map(|p| input::packets(&std::fs::read(self.directory.join(p)).unwrap()).into())
            .collect();
        Ok(EvalBotPipeline {
            context: context.clone(),
            processors: vec![Arc::new(Consumer(Arc::new(ResponseState {
                directory: self.directory.clone(),
                port: self.port,
                profile: self.profile.clone(),
                context,
                clips: Mutex::new(clips),
                in_clip: AtomicBool::new(false),
                number: AtomicUsize::new(0),
            })))],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
pub async fn child() {
    if std::env::var_os("HOSTED_SPEECH_CHILD").is_none() {
        return;
    }
    let args = std::env::args().collect::<Vec<_>>();
    let port: u16 = args[args.iter().position(|s| s == "--skip").unwrap() + 1]
        .parse()
        .unwrap();
    let directory = PathBuf::from(std::env::var_os("HOSTED_SPEECH_DIR").unwrap());
    let name = std::env::var("HOSTED_SPEECH_PROFILE").unwrap();
    let profile: Profile = serde_json::from_slice(
        &std::fs::read(directory.join(format!("{name}.profile.json"))).unwrap(),
    )
    .unwrap();
    std::fs::write(
        directory.join(format!("started-{port}")),
        std::process::id().to_string(),
    )
    .unwrap();
    std::fs::write(directory.join(format!("environment-{port}")),serde_json::to_vec(&json!({
        "credential_present":([fixture::KEY_ENV,"CARTESIA_API_KEY",STT_ENV,JUDGE_ENV].iter().any(|key|std::env::var_os(key).is_some())),
        "secret_present":std::env::vars().any(|(_,value)|[fixture::SECRET,STT_KEY,JUDGE_KEY].contains(&value.as_str()))})).unwrap()).unwrap();
    let host = EvalBotHost::bind(
        format!("127.0.0.1:{port}").parse().unwrap(),
        EvalBotHostConfig {
            startup_timeout: Duration::from_secs(3),
            session_timeout: WAIT,
            host_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(2),
            transport: SessionConfig {
                close_timeout: Duration::from_millis(100),
                write_timeout: Duration::from_millis(300),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_raw_audio_input(true)
    .with_audio_output(true);
    let report = host
        .serve(
            &Factory {
                directory,
                port,
                profile,
            },
            Cancellation::new(),
        )
        .await
        .unwrap();
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
}

#[derive(Clone, Copy, Debug)]
pub enum Kind {
    Tts,
    Stt,
    Judge,
}
#[derive(Clone)]
pub enum Reply {
    Pcm(Vec<u8>),
    // Gate a named request before input playback, not inside the bot actor.
    // Both accept-order slots use this plan so concurrency cannot swap roles.
    PcmAfterMarker {
        bytes: Vec<u8>,
        transcript: &'static str,
        marker: PathBuf,
    },
    Text(&'static str),
    Judge,
    Hold,
    Gate(&'static str),
}
pub struct Gate {
    pub request: Mutex<Vec<u8>>,
    pub received: Notify,
    pub released: Semaphore,
    pub closed: AtomicBool,
    pub done: AtomicBool,
}
impl Default for Gate {
    fn default() -> Self {
        Self {
            request: Mutex::new(vec![]),
            received: Notify::new(),
            released: Semaphore::new(0),
            closed: AtomicBool::new(false),
            done: AtomicBool::new(false),
        }
    }
}
pub struct Http {
    pub url: String,
    pub gates: Vec<Arc<Gate>>,
    cancel: Cancellation,
    task: JoinHandle<Vec<String>>,
}
pub struct Report {
    pub gates: Vec<Arc<Gate>>,
    pub problems: Vec<String>,
}
impl Report {
    pub fn checked(&self, count: usize) {
        assert!(self.problems.is_empty(), "{:?}", self.problems);
        assert_eq!(
            self.gates
                .iter()
                .filter(|g| !g.request.lock().unwrap().is_empty())
                .count(),
            count
        );
        assert!(
            self.gates.iter().all(|gate| {
                gate.request.lock().unwrap().is_empty() || gate.done.load(Ordering::SeqCst)
            }),
            "accepted HTTP handler did not retire naturally"
        );
    }
}
impl Http {
    pub async fn start(kind: Kind, replies: Vec<Reply>) -> Self {
        Self::with_marker(kind, replies, None).await
    }
    pub async fn with_marker(kind: Kind, replies: Vec<Reply>, marker: Option<PathBuf>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let gates: Vec<_> = replies.iter().map(|_| Arc::new(Gate::default())).collect();
        let mut plans: VecDeque<_> = replies.into_iter().zip(gates.iter().cloned()).collect();
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let task = tokio::spawn(async move {
            let mut tasks = JoinSet::new();
            let mut problems = vec![];
            loop {
                tokio::select! {biased;_=c.cancelled()=>break,accept=listener.accept()=>match accept{
                    Ok((stream,_))=>if let Some((reply,gate))=plans.pop_front(){let marker=marker.clone();tasks.spawn(serve(stream,kind,reply,gate,marker));}else{drop(stream);problems.push("unexpected extra HTTP request".into());},
                    Err(error)=>{problems.push(error.to_string());break;}
                },result=tasks.join_next(),if !tasks.is_empty()=>{if let Some(result)=result{if !matches!(result,Ok(Ok(()))){problems.push(format!("HTTP handler: {result:?}"));}}}}
            }
            // Cancellation stops admission, but cannot manufacture a held peer EOF.
            for _ in 0..8 {
                match tokio::time::timeout(Duration::from_millis(10), listener.accept()).await {
                    Ok(Ok((stream, _))) => {
                        drop(stream);
                        problems.push("HTTP request remained in shutdown backlog".into());
                    }
                    _ => break,
                }
            }
            drop(listener);
            if tokio::time::timeout(Duration::from_secs(2), async {
                while let Some(result) = tasks.join_next().await {
                    if !matches!(result, Ok(Ok(()))) {
                        problems.push(format!("HTTP handler: {result:?}"));
                    }
                }
            })
            .await
            .is_err()
            {
                problems.push("fixture handler needed fallback abort".into());
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
            }
            problems
        });
        Self {
            url,
            gates,
            cancel,
            task,
        }
    }
    pub async fn finish(self) -> Report {
        self.cancel.cancel();
        let problems = self
            .task
            .await
            .unwrap_or_else(|error| vec![format!("HTTP owner join failed: {error}")]);
        Report {
            gates: self.gates,
            problems,
        }
    }
}
async fn serve(
    mut stream: TcpStream,
    kind: Kind,
    reply: Reply,
    gate: Arc<Gate>,
    marker: Option<PathBuf>,
) -> Result<(), Error> {
    let request = read_request(&mut stream).await?;
    validate_request(kind, &request)?;
    *gate.request.lock().unwrap() = request;
    stage(&format!("http_enter kind={kind:?}"));
    gate.received.notify_one();
    if let Some(marker) = marker {
        if matches!(reply, Reply::Hold) {
            std::fs::write(marker, b"actual held STT multipart received")?;
        }
    }
    let reply = match reply {
        Reply::PcmAfterMarker {
            bytes,
            transcript,
            marker,
        } => {
            let matches = request_json(&gate.request.lock().unwrap())["transcript"] == transcript;
            if matches && !observed(|| marker.exists()).await {
                return Err("TTS causal STT-entry barrier deadline".into());
            }
            Reply::Pcm(bytes)
        }
        Reply::Hold | Reply::Gate(_) => {
            let mut byte = [0; 1];
            tokio::select! {read=stream.read(&mut byte)=>{
                match read{Ok(0)=>{},Err(e)if e.kind()==std::io::ErrorKind::ConnectionReset=>{},other=>return Err(format!("held HTTP peer read: {other:?}").into())}
                gate.closed.store(true,Ordering::SeqCst);gate.done.store(true,Ordering::SeqCst);return Ok(());
            },permit=gate.released.acquire()=>{permit?.forget();}}
            match reply {
                Reply::Gate(text) => Reply::Text(text),
                _ => return Err("held request unexpectedly released".into()),
            }
        }
        other => other,
    };
    let (content, body) = match reply {
        Reply::Pcm(bytes) => ("audio/pcm", bytes),
        Reply::Text(text) => (
            "application/json",
            serde_json::to_vec(&json!({"text":text}))?,
        ),
        Reply::Judge => (
            "application/json",
            serde_json::to_vec(
                &json!({"answers":{"verdict":{"type":"choice","choice":"yes","confidence":0.9,"probabilities":{"yes":0.4,"no":0.3,"continue":0.3}}}}),
            )?,
        ),
        _ => unreachable!(),
    };
    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await?;
    stream.write_all(&body).await?;
    stream.shutdown().await?;
    stage(&format!("http_reply kind={kind:?}"));
    gate.done.store(true, Ordering::SeqCst);
    Ok(())
}
async fn read_request(stream: &mut TcpStream) -> Result<Vec<u8>, Error> {
    let mut bytes = vec![];
    let mut block = [0; 4096];
    let end = loop {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("HTTP EOF before header".into());
        }
        bytes.extend_from_slice(&block[..n]);
        if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            if i + 4 > 16 * 1024 {
                return Err("HTTP header budget".into());
            }
            break i + 4;
        }
        if bytes.len() > 16 * 1024 {
            return Err("HTTP header budget".into());
        }
    };
    let header = std::str::from_utf8(&bytes[..end])?;
    let length = header
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then_some(value.trim())
        })
        .ok_or("missing content length")?
        .parse::<usize>()?;
    if length > 512 * 1024 {
        return Err("HTTP body budget".into());
    }
    let total = end.checked_add(length).ok_or("HTTP overflow")?;
    while bytes.len() < total {
        let n = stream.read(&mut block).await?;
        if n == 0 {
            return Err("truncated HTTP".into());
        }
        bytes.extend_from_slice(&block[..n]);
    }
    if bytes.len() != total {
        return Err("extra HTTP bytes".into());
    }
    Ok(bytes)
}
pub fn request_json(bytes: &[u8]) -> Value {
    let end = bytes.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    serde_json::from_slice(&bytes[end..]).unwrap()
}
fn validate_request(kind: Kind, request: &[u8]) -> Result<(), Error> {
    let text = String::from_utf8_lossy(request);
    let lower = text.to_ascii_lowercase();
    match kind {
        Kind::Tts => {
            if !request.starts_with(b"POST /tts/bytes ")
                || !lower.contains(&format!("x-api-key: {}\r\n", fixture::SECRET))
            {
                return Err("wrong TTS endpoint/key".into());
            }
            let body = request_json(request);
            if body["output_format"]["encoding"] != "pcm_s16le"
                || body["output_format"]["sample_rate"] != 16000
            {
                return Err("wrong TTS PCM format".into());
            }
        }
        Kind::Judge => {
            if !request.starts_with(b"POST /v1/systemone ")
                || !lower.contains(&format!("authorization: bearer {JUDGE_KEY}\r\n"))
            {
                return Err("wrong judge endpoint/key".into());
            }
            let body = request_json(request);
            if !body["state"].is_object() || body["questions"]["verdict"]["type"] != "choice" {
                return Err("wrong judge schema".into());
            }
        }
        Kind::Stt => {
            if !request.starts_with(b"POST /audio/transcriptions ")
                || !lower.contains(&format!("authorization: bearer {STT_KEY}\r\n"))
                || !text.contains("multipart/form-data; boundary=")
                || !text.contains("name=\"model\"\r\n\r\nfixture-stt")
            {
                return Err("wrong STT endpoint/key/multipart".into());
            }
            let offset = request
                .windows(4)
                .position(|w| w == b"RIFF")
                .ok_or("missing WAV")?;
            let header = request.get(offset..offset + 8).ok_or("short WAV header")?;
            let length = u32::from_le_bytes(header[4..8].try_into()?) as usize + 8;
            let wav = pipecat_audio::read_pcm16_wav(
                request
                    .get(offset..offset + length)
                    .ok_or("short WAV body")?,
            )?;
            let voiced = &PCM[25_600..27_648];
            if wav.format.sample_rate() != 16_000
                || wav.format.channels() != 1
                || !wav.pcm.windows(voiced.len()).any(|w| w == voiced)
            {
                return Err("multipart lacks genuine captured mono16k bot speech".into());
            }
        }
    }
    Ok(())
}
