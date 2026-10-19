//! Test-only compiled host and loopback provider for the hosted speech route.
//! Markers are observation/barrier aids, never substitutes for actual process waits.
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};

pub const WAIT: Duration = Duration::from_secs(15);
pub const SECRET: &str = "synthetic-hosted-cartesia-secret-127";
pub const KEY_ENV: &str = "PIPECAT_HOSTED_SPEECH_FIXTURE_KEY";
static NEXT: AtomicUsize = AtomicUsize::new(0);
#[path = "ports.rs"]
mod ports;

pub struct Bundle(pub PathBuf);
impl Bundle {
    pub fn new() -> Self {
        use std::os::unix::fs::DirBuilderExt;
        let path = std::env::temp_dir().join(format!(
            "pipecat-hosted-speech-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        Self(path)
    }
    pub fn put(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let path = self.0.join(name);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, bytes).unwrap();
        path
    }
    pub fn json(&self, name: &str, value: &Value) -> PathBuf {
        self.put(name, serde_json::to_vec(value).unwrap())
    }
    pub fn markers(&self, prefix: &str) -> Vec<PathBuf> {
        std::fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.file_name().unwrap().to_string_lossy().starts_with(prefix))
            .collect()
    }
    pub fn cache(&self) -> PathBuf {
        use std::os::unix::fs::DirBuilderExt;
        let path = self.0.join("cache");
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&path)
            .unwrap();
        path
    }
}
impl Drop for Bundle {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn runtime(base: &str, cache: Option<&Path>) -> Value {
    json!({"provider":"cartesia","base_url":base,"api_key_env":KEY_ENV,
        "request_timeout_ms":600,"cache_dir":cache})
}
pub fn expected(n: usize) -> Value {
    json!({"event":"llm_response","text_contains":format!("complete PCM {n}"),"within_ms":3000})
}
pub fn source(name: &str, references: &[&str]) -> Value {
    json!({"name":name,"user":{"modality":"audio","speech":{"service":"cartesia","voice":"Fixture-VOICE"}},
        "context":[{"role":"system","content":format!("isolated {name}")}],
        "turns":references.iter().enumerate().map(|(i, text)|json!({"user":text,"expect":[expected(i+1)]})).collect::<Vec<_>>()})
}
pub fn entry(b: &Bundle, label: &str, file: &str, profile: &str) -> Value {
    json!({"name":label,"bot":std::env::current_exe().unwrap(),"scenarios":[file],
        "args":["--exact","user_speech_suite_bot","--nocapture","--test-threads=1","--skip","{port}"],
        "env":{"HOSTED_SPEECH_CHILD":"1","HOSTED_SPEECH_DIR":b.0,"HOSTED_SPEECH_PROFILE":profile},"concurrency":1})
}
pub fn manifest(entries: Vec<Value>, repeat: usize, concurrency: usize) -> Value {
    json!({"native_version":1,"concurrency":concurrency,"repeat":repeat,"base_port":ports::port(),"suite":entries})
}
pub fn arguments(manifest: &Path, runtime: &Path) -> Vec<String> {
    vec![
        "suite".into(),
        manifest.display().to_string(),
        "--user-speech".into(),
        runtime.display().to_string(),
    ]
}
async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    (&mut pipe)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 1024 * 1024 {
        return Err(std::io::Error::other("CLI fixture output limit"));
    }
    Ok(bytes)
}
pub async fn cli(args: &[String], cwd: &Path) -> Output {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut child = Command::new(executable)
        .args(args)
        .current_dir(cwd)
        .env(KEY_ENV, SECRET)
        .env("CARTESIA_API_KEY", "synthetic-unapproved-fallback")
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("ALL_PROXY", "http://127.0.0.1:9")
        .env("http_proxy", "http://127.0.0.1:9")
        .env("https_proxy", "http://127.0.0.1:9")
        .env("all_proxy", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let mut out_task = tokio::spawn(read_output(stdout));
    let mut err_task = tokio::spawn(read_output(stderr));
    let status = match tokio::time::timeout(WAIT, child.wait()).await {
        Ok(status) => status.unwrap(),
        Err(_) => {
            // Cooperative CLI cancellation retains its direct-child ownership.
            if let Some(pid) = child.id() {
                let _ = Command::new("kill")
                    .arg("-INT")
                    .arg(pid.to_string())
                    .status()
                    .await;
            }
            let joined = child.wait().await;
            let out = (&mut out_task).await;
            let err = (&mut err_task).await;
            panic!("CLI deadline; actual wait={joined:?}; stdout={out:?}; stderr={err:?}");
        }
    };
    let stdout = (&mut out_task).await.unwrap().unwrap();
    let stderr = (&mut err_task).await.unwrap().unwrap();
    Output {
        status,
        stdout,
        stderr,
    }
}
pub fn assert_exit(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains(SECRET));
    assert!(!String::from_utf8_lossy(&output.stderr).contains(SECRET));
}
pub async fn assert_idle(listener: &tokio::net::TcpListener) {
    assert!(
        tokio::time::timeout(Duration::from_millis(25), listener.accept())
            .await
            .is_err(),
        "rejected preflight made a provider connection"
    );
}

#[cfg(feature = "user-speech")]
pub mod enabled {
    use super::*;
    use pipecat_core::*;
    use pipecat_evals::{
        bot_host::*, Cancellation, CartesiaUserSpeechSettings, UserSpeechCachePolicy,
    };
    use pipecat_speech::SpeechConfig;
    use pipecat_transports::SessionConfig;
    use pipecat_turns::SharedContext;
    use serde::Deserialize;
    use std::{
        collections::VecDeque,
        io::Write,
        sync::{Arc, Mutex},
    };
    use tokio::{
        io::AsyncWriteExt,
        net::{TcpListener, TcpStream},
        task::{JoinHandle, JoinSet},
    };

    pub const PCM_BUDGET: usize = 15_360_000;
    pub fn pcm(seed: u8) -> Vec<u8> {
        [
            [seed, 0x31].repeat(640),
            [seed.wrapping_add(1), 0x52].repeat(640),
            [seed.wrapping_add(2), 0x73].repeat(640),
            [seed.wrapping_add(3), 0x24].repeat(37),
        ]
        .concat()
    }
    pub fn packets(bytes: &[u8]) -> Vec<Vec<u8>> {
        bytes
            .chunks(1280)
            .map(|chunk| {
                let mut out = vec![0; 1280];
                out[..chunk.len()].copy_from_slice(chunk);
                out
            })
            .collect()
    }
    pub fn wav(bytes: &[u8]) -> Vec<u8> {
        pipecat_audio::pcm_to_wav(bytes, 16_000, 1).unwrap()
    }
    pub fn settings(url: &str, cache: Option<&Path>) -> CartesiaUserSpeechSettings {
        assert!(url.starts_with("http://127.0.0.1:"));
        let mut config = SpeechConfig::cartesia(SECRET);
        config.base_url = url.into();
        config.timeout = Duration::from_millis(600);
        CartesiaUserSpeechSettings::new(
            config,
            cache.map_or(UserSpeechCachePolicy::Disabled, |p| {
                UserSpeechCachePolicy::Private {
                    directory: p.into(),
                    max_entries: 16,
                    max_bytes: 16 * 1024 * 1024,
                }
            }),
        )
        .unwrap()
    }
    pub fn profile(
        b: &Bundle,
        name: &str,
        clips: &[Vec<u8>],
        context: &str,
        mode: &str,
        wait_delta: i32,
    ) {
        let paths = clips
            .iter()
            .enumerate()
            .map(|(i, bytes)| {
                let file = format!("{name}-clip-{i}.pcm");
                b.put(&file, bytes);
                file
            })
            .collect::<Vec<_>>();
        b.json(
            &format!("{name}.profile.json"),
            &json!({"clips":paths,"context":context,"mode":mode,"wait_delta":wait_delta}),
        );
    }
    #[derive(Deserialize)]
    struct ChildProfile {
        clips: Vec<PathBuf>,
        context: String,
        mode: String,
        wait_delta: i32,
    }
    struct Consumer {
        directory: PathBuf,
        port: u16,
        profile: ChildProfile,
        context: SharedContext,
        clips: Mutex<VecDeque<VecDeque<Vec<u8>>>>,
        number: AtomicUsize,
    }
    impl Consumer {
        fn mark(&self, prefix: &str, bytes: impl AsRef<[u8]>) -> Result<(), ProcessorError> {
            std::fs::write(
                self.directory.join(format!("{prefix}-{}", self.port)),
                bytes,
            )
            .map_err(|e| ProcessorError::new(e.to_string()))
        }
        async fn answer(&self, ctx: &ProcessorContext, text: String) -> Result<(), ProcessorError> {
            let text = if self.profile.mode == "large" {
                format!("{text} {}", "bounded-result-padding".repeat(1024))
            } else {
                text
            };
            for data in [
                FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
                    function_name: "complete_pcm".into(),
                    tool_call_id: format!("pcm-{}", self.port),
                    arguments: json!({"port":self.port}),
                    cancel_on_interruption: true,
                    group_id: None,
                }),
                FrameData::LlmResponseStart,
                FrameData::LlmText(TextFrame::new(text)),
                FrameData::LlmResponseEnd,
            ] {
                ctx.push(Frame::new(data), Direction::Downstream).await?;
            }
            Ok(())
        }
    }
    #[async_trait]
    impl FrameProcessor for Consumer {
        fn name(&self) -> &str {
            "hosted-complete-multipacket-consumer"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            match &frame.data {
                FrameData::InputAudio(audio) if audio.audio.iter().any(|&b| b != 0) => {
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(self.directory.join(format!("audio-{}", self.port)))
                        .and_then(|mut f| f.write_all(&audio.audio))
                        .map_err(|e| ProcessorError::new(e.to_string()))?;
                    let (first, last, valid) = {
                        let mut clips = self.clips.lock().unwrap();
                        match clips.front_mut() {
                            Some(clip) => {
                                let first = clip.len() == 4;
                                let want = clip.pop_front().unwrap();
                                let last = clip.is_empty();
                                if last {
                                    clips.pop_front();
                                }
                                (
                                    first,
                                    last,
                                    audio.audio == want
                                        && audio.sample_rate == 16_000
                                        && audio.num_channels == 1,
                                )
                            }
                            None => (false, false, false),
                        }
                    };
                    if !valid {
                        self.mark("invalid", b"unexpected PCM/order/format/tail")?;
                    }
                    if first && self.profile.mode == "early" {
                        self.answer(ctx, "early assertion".into()).await?;
                    }
                    if last
                        && valid
                        && !self
                            .directory
                            .join(format!("invalid-{}", self.port))
                            .exists()
                    {
                        let context = serde_json::to_value(self.context.snapshot_rich()?)
                            .map_err(|e| ProcessorError::new(e.to_string()))?;
                        let expected = json!([{"role":"system","content":self.profile.context}]);
                        let actual=Value::Array(context["messages"].as_array().unwrap().iter().map(|message|json!({"role":message["role"],"content":message["content"]})).collect());
                        if actual != expected {
                            self.mark("invalid", serde_json::to_vec(&context).unwrap())?;
                            return Err(ProcessorError::new("context not fresh or wrong scenario"));
                        }
                        let n = self.number.fetch_add(1, Ordering::SeqCst) + 1;
                        self.mark(&format!("tail-{n}"), b"arrived")?;
                        if self.profile.mode == "gated" {
                            let gate = self.directory.join(format!("release-{}", self.port));
                            tokio::time::timeout(WAIT, async {
                                while !gate.exists() {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            })
                            .await
                            .map_err(|_| ProcessorError::new("fixture tail gate deadline"))?;
                        }
                        if self.profile.wait_delta != 0 && self.clips.lock().unwrap().is_empty() {
                            let peer = i32::from(self.port) + self.profile.wait_delta;
                            let gate = self.directory.join(format!("complete-{peer}"));
                            tokio::time::timeout(Duration::from_secs(5), async {
                                while !gate.exists() {
                                    tokio::time::sleep(Duration::from_millis(5)).await;
                                }
                            })
                            .await
                            .map_err(|_| ProcessorError::new("fixture peer completion deadline"))?;
                        }
                        self.mark("consumed", n.to_string())?;
                        self.answer(ctx, format!("complete PCM {n}")).await?;
                        if self.clips.lock().unwrap().is_empty() {
                            std::fs::OpenOptions::new()
                                .create(true)
                                .append(true)
                                .open(self.directory.join("completion-order"))
                                .and_then(|mut f| writeln!(f, "{}", self.port))
                                .map_err(|e| ProcessorError::new(e.to_string()))?;
                            // Publish the peer barrier only after its order entry
                            // exists, so the waiting child cannot overtake it.
                            self.mark("complete", b"done")?;
                        }
                    }
                }
                FrameData::Text(_) | FrameData::LlmMessagesAppend(_) => {
                    self.mark("text", b"reference was injected")?;
                }
                _ => {}
            }
            ctx.push(frame, direction).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.mark("cleaned", b"done")
        }
    }
    struct Factory {
        directory: PathBuf,
        port: u16,
        profile: ChildProfile,
    }
    #[async_trait]
    impl EvalBotFactory for Factory {
        async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
            assert!(settings.skip_tts);
            let context = SharedContext::default();
            let clips = self
                .profile
                .clips
                .iter()
                .map(|p| packets(&std::fs::read(self.directory.join(p)).unwrap()).into())
                .collect();
            Ok(EvalBotPipeline {
                context: context.clone(),
                processors: vec![Arc::new(Consumer {
                    directory: self.directory.clone(),
                    port: self.port,
                    profile: ChildProfile {
                        clips: self.profile.clips.clone(),
                        context: self.profile.context.clone(),
                        mode: self.profile.mode.clone(),
                        wait_delta: self.profile.wait_delta,
                    },
                    context,
                    clips: Mutex::new(clips),
                    number: AtomicUsize::new(0),
                })],
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
        let profile_name = std::env::var("HOSTED_SPEECH_PROFILE").unwrap();
        let profile: ChildProfile = serde_json::from_slice(
            &std::fs::read(directory.join(format!("{profile_name}.profile.json"))).unwrap(),
        )
        .unwrap();
        std::fs::write(
            directory.join(format!("started-{port}")),
            std::process::id().to_string(),
        )
        .unwrap();
        // Capture only names plus a leak boolean; never publish synthetic secrets.
        std::fs::write(directory.join(format!("environment-{port}")),serde_json::to_vec(&json!({
            "credential_present":std::env::var_os(KEY_ENV).is_some()||std::env::var_os("CARTESIA_API_KEY").is_some(),
            "secret_present":std::env::vars().any(|(_,value)|value==SECRET)})).unwrap()).unwrap();
        if profile.mode == "crash" {
            std::process::exit(23);
        }
        if profile.mode == "never-ready" {
            tokio::time::sleep(WAIT).await;
            return;
        }
        if profile.mode == "flood" {
            println!("{}", "fixture output flood ".repeat(8192));
        }
        let host = EvalBotHost::bind(
            format!("127.0.0.1:{port}").parse().unwrap(),
            EvalBotHostConfig {
                startup_timeout: Duration::from_secs(2),
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
        .with_raw_audio_input(true);
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
    pub fn assert_reaped(result: &Value) {
        for run in result["runs"].as_array().unwrap() {
            if let Some(pid) = run["hosted"]["process"]["pid"].as_u64() {
                assert_eq!(run["hosted"]["process"]["reaped"], true, "{result}");
                assert_eq!(
                    run["hosted"]["process"]["output_incomplete"], false,
                    "pipe ownership incomplete: {result}"
                );
                #[cfg(target_os = "linux")]
                assert!(
                    !Path::new(&format!("/proc/{pid}")).exists(),
                    "reported-reaped PID remains"
                );
            }
        }
    }
    pub fn assert_consumed(b: &Bundle, result: &Value, clips: &[Vec<Vec<u8>>]) {
        assert_reaped(result);
        assert_eq!(result["runs"].as_array().unwrap().len(), clips.len());
        for (run, want) in result["runs"].as_array().unwrap().iter().zip(clips) {
            let port = run["port"].as_u64().unwrap();
            assert_eq!(
                std::fs::read(b.0.join(format!("audio-{port}"))).unwrap(),
                want.iter()
                    .flat_map(|c| packets(c).concat())
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                std::fs::read_to_string(b.0.join(format!("consumed-{port}"))).unwrap(),
                want.len().to_string()
            );
            assert!(!b.0.join(format!("invalid-{port}")).exists());
            assert!(!b.0.join(format!("text-{port}")).exists());
            assert!(b.0.join(format!("cleaned-{port}")).exists());
            let env: Value = serde_json::from_slice(
                &std::fs::read(b.0.join(format!("environment-{port}"))).unwrap(),
            )
            .unwrap();
            assert_eq!(
                env,
                json!({"credential_present":false,"secret_present":false})
            );
        }
    }
    pub fn assert_no_audio(b: &Bundle) {
        assert!(
            b.markers("audio-").is_empty(),
            "refused speech reached bot wire"
        );
    }
    pub async fn wait_for(b: &Bundle, prefix: &str) {
        tokio::time::timeout(WAIT, async {
            while b.markers(prefix).is_empty() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("fixture observation deadline");
    }
    pub fn cache_wavs(root: &Path) -> Vec<PathBuf> {
        fn visit(root: &Path, out: &mut Vec<PathBuf>) {
            for e in std::fs::read_dir(root).unwrap() {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    visit(&e.path(), out)
                } else if e.path().extension().is_some_and(|s| s == "wav") {
                    out.push(e.path());
                }
            }
        }
        let mut out = vec![];
        visit(root, &mut out);
        out.sort();
        out
    }
    pub fn assert_no_temporaries(root: &Path) {
        fn visit(root: &Path) {
            for e in std::fs::read_dir(root).unwrap() {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    visit(&e.path());
                } else {
                    let n = e.file_name();
                    let n = n.to_string_lossy();
                    assert!(
                        n == ".lock" || n.ends_with(".wav"),
                        "unsettled cache file {n}"
                    );
                }
            }
        }
        visit(root);
    }
    pub fn assert_cache_unlocked(root: &Path) {
        fn visit(root: &Path) {
            for e in std::fs::read_dir(root).unwrap() {
                let e = e.unwrap();
                if e.file_type().unwrap().is_dir() {
                    visit(&e.path());
                } else if e.file_name() == ".lock" {
                    let f = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(e.path())
                        .unwrap();
                    rustix::fs::flock(&f, rustix::fs::FlockOperation::NonBlockingLockExclusive)
                        .expect("cache transaction still owns lock after return");
                    rustix::fs::flock(&f, rustix::fs::FlockOperation::Unlock).unwrap();
                }
            }
        }
        visit(root);
    }
    #[derive(Clone)]
    pub struct Reply {
        pub status: u16,
        pub body: Vec<u8>,
        pub stalled: bool,
        pub truncated: bool,
        pub delay: Duration,
    }
    impl Reply {
        pub fn pcm(body: Vec<u8>) -> Self {
            Self {
                status: 200,
                body,
                stalled: false,
                truncated: false,
                delay: Duration::ZERO,
            }
        }
    }
    #[derive(Default)]
    pub struct HttpTrace {
        pub requests: Mutex<Vec<Value>>,
        pub active: AtomicUsize,
        pub joined: AtomicUsize,
        pub closed: AtomicUsize,
    }
    struct HandlerGuard(Arc<HttpTrace>);
    impl Drop for HandlerGuard {
        fn drop(&mut self) {
            self.0.active.fetch_sub(1, Ordering::SeqCst);
            self.0.joined.fetch_add(1, Ordering::SeqCst);
        }
    }
    pub struct Http {
        pub url: String,
        pub trace: Arc<HttpTrace>,
        cancel: Cancellation,
        task: JoinHandle<()>,
    }
    impl Http {
        pub async fn start(replies: Vec<Reply>) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("http://{}", listener.local_addr().unwrap());
            let cancel = Cancellation::new();
            let c = cancel.clone();
            let trace = Arc::new(HttpTrace::default());
            let t = trace.clone();
            let task = tokio::spawn(async move {
                let mut handlers = JoinSet::new();
                let mut next = 0;
                loop {
                    tokio::select! {biased;_=c.cancelled()=>break,accept=listener.accept()=>{
                        let (stream,_)=accept.unwrap();let reply=replies.get(next).cloned().unwrap_or_else(||{let mut r=Reply::pcm(vec![]);r.status=503;r});next+=1;
                        let t=t.clone();let c=c.clone();t.active.fetch_add(1,Ordering::SeqCst);
                        handlers.spawn(async move{let _guard=HandlerGuard(t.clone());let _=handle_http(stream,reply,t,c).await;});
                    }}
                }
                while let Some(result) = handlers.join_next().await {
                    result.unwrap();
                }
            });
            Self {
                url,
                trace,
                cancel,
                task,
            }
        }
        pub async fn finish(mut self) -> Arc<HttpTrace> {
            self.cancel.cancel();
            let joined = tokio::time::timeout(WAIT, &mut self.task).await;
            match joined {
                Ok(r) => r.unwrap(),
                Err(_) => {
                    let result = self.task.await;
                    panic!("HTTP fixture join deadline: {result:?}");
                }
            }
            assert_eq!(self.trace.active.load(Ordering::SeqCst), 0);
            self.trace
        }
    }
    async fn handle_http(
        mut stream: TcpStream,
        reply: Reply,
        trace: Arc<HttpTrace>,
        cancel: Cancellation,
    ) -> std::io::Result<()> {
        let mut bytes = vec![];
        let end;
        loop {
            let mut b = [0; 2048];
            let n = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),r=stream.read(&mut b)=>r?};
            if n == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&b[..n]);
            assert!(bytes.len() <= 64 * 1024);
            if let Some(i) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                end = i + 4;
                break;
            }
        }
        let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
        assert!(header.starts_with("POST /tts/bytes HTTP/1.1\r\n"));
        assert!(header
            .to_ascii_lowercase()
            .contains(&format!("x-api-key: {}\r\n", SECRET.to_ascii_lowercase())));
        let len: usize = header
            .lines()
            .find_map(|s| {
                let (k, v) = s.split_once(':')?;
                k.eq_ignore_ascii_case("content-length")
                    .then(|| v.trim().parse().unwrap())
            })
            .unwrap();
        assert!(len <= 64 * 1024);
        while bytes.len() - end < len {
            let mut b = [0; 2048];
            let n = tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),r=stream.read(&mut b)=>r?};
            if n == 0 {
                return Ok(());
            }
            bytes.extend_from_slice(&b[..n]);
        }
        trace
            .requests
            .lock()
            .unwrap()
            .push(serde_json::from_slice(&bytes[end..end + len]).unwrap());
        if !reply.delay.is_zero() {
            tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),_=tokio::time::sleep(reply.delay)=>{}}
        }
        let claimed = reply.body.len()
            + if reply.stalled || reply.truncated {
                1280
            } else {
                0
            };
        let write = async {
            stream.write_all(format!("HTTP/1.1 {} Fixture\r\nContent-Type: audio/pcm\r\nContent-Length: {claimed}\r\nConnection: close\r\n\r\n",reply.status).as_bytes()).await?;
            for chunk in reply.body.chunks(997) {
                stream.write_all(chunk).await?;
            }
            stream.flush().await
        };
        let written =
            tokio::select! {biased;_=cancel.cancelled()=>return Ok(()),result=write=>result};
        if written.is_err() {
            trace.closed.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        if reply.stalled {
            let mut b = [0; 1];
            tokio::select! {biased;_=cancel.cancelled()=>{},read=stream.read(&mut b)=>{if !matches!(read,Ok(n)if n!=0){trace.closed.fetch_add(1,Ordering::SeqCst);}}}
        }
        Ok(())
    }
}
