//! Actual compiled-child recorded suite, using this harness only as a narrow fixture.
#![cfg(unix)]
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*,
    process::ProcessOptions,
    release::{run_release, ReleaseManifest},
    Cancellation, SessionOptions,
};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};
const WAIT: Duration = Duration::from_secs(12);
static NEXT: AtomicUsize = AtomicUsize::new(0);
#[path = "support/ports.rs"]
mod fixture_ports;
struct Bundle(PathBuf);
impl Bundle {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "pipecat-recorded-suite-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn put(&self, name: &str, data: impl AsRef<[u8]>) -> PathBuf {
        let p = self.0.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, data).unwrap();
        p
    }
    fn json(&self, name: &str, v: Value) -> PathBuf {
        self.put(name, serde_json::to_vec(&v).unwrap())
    }
    fn wav(&self, name: &str, value: i16, samples: usize) -> PathBuf {
        let pcm = value.to_le_bytes().repeat(samples);
        let mut b = Vec::new();
        b.extend(b"RIFF");
        b.extend((36 + pcm.len() as u32).to_le_bytes());
        b.extend(b"WAVEfmt ");
        b.extend(16u32.to_le_bytes());
        b.extend(1u16.to_le_bytes());
        b.extend(1u16.to_le_bytes());
        b.extend(16000u32.to_le_bytes());
        b.extend(32000u32.to_le_bytes());
        b.extend(2u16.to_le_bytes());
        b.extend(16u16.to_le_bytes());
        b.extend(b"data");
        b.extend((pcm.len() as u32).to_le_bytes());
        b.extend(pcm);
        self.put(name, b)
    }
    fn markers(&self) -> Vec<PathBuf> {
        std::fs::read_dir(&self.0)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| {
                p.file_name()
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
fn scenario(name: &str, tone: &str, expected: &str) -> Value {
    json!({"name":name,"user":{"modality":"audio"},"turns":[{"user":"reference never sent as text","audio":tone,"expect":[{"event":"response","text_contains":expected,"within_ms":300}]}]})
}
fn manifest(b: &Bundle, files: Vec<&str>, repeat: usize) -> Value {
    json!({"native_version":1,"concurrency":2,"repeat":repeat,"base_port":fixture_ports::port(),"suite":[{"name":"fixture","bot":std::env::current_exe().unwrap(),"scenarios":files,"args":["--exact","recorded_suite_bot","--nocapture","--test-threads=1","--skip","{port}"],"env":{"RECORDED_SUITE_CHILD":"1","RECORDED_SUITE_DIR":b.0},"concurrency":1}]})
}
struct Recognizer {
    directory: PathBuf,
    port: u16,
}
#[async_trait]
impl FrameProcessor for Recognizer {
    fn name(&self) -> &str {
        "recorded-suite-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let answer = match &frame.data {
            FrameData::InputAudio(a) => {
                let audio = &a.audio;
                if audio.iter().any(|b| *b != 0) {
                    use std::io::Write;
                    std::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(self.directory.join(format!("audio-{}", self.port)))
                        .and_then(|mut file| file.write_all(audio))
                        .map_err(|e| ProcessorError::new(e.to_string()))?;
                    Some(if audio.starts_with(&[7, 0]) {
                        "tone-seven"
                    } else {
                        "tone-eight"
                    })
                } else {
                    None
                }
            }
            FrameData::LlmMessagesAppend(_) => {
                std::fs::write(self.directory.join(format!("text-{}", self.port)), b"text")
                    .map_err(|e| ProcessorError::new(e.to_string()))?;
                Some("text-echo")
            }
            _ => None,
        };
        if let Some(answer) = answer {
            ctx.push(
                Frame::new(FrameData::Transcription(TranscriptionFrame {
                    text: "decoded".into(),
                    finalized: true,
                    ..Default::default()
                })),
                Direction::Downstream,
            )
            .await?;
            ctx.push(
                Frame::new(FrameData::LlmResponseStart),
                Direction::Downstream,
            )
            .await?;
            ctx.push(
                Frame::new(FrameData::LlmText(TextFrame::new(answer))),
                Direction::Downstream,
            )
            .await?;
            ctx.push(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
                .await?;
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        std::fs::write(
            self.directory.join(format!("cleaned-{}", self.port)),
            b"done",
        )
        .map_err(|e| ProcessorError::new(e.to_string()))
    }
}
struct Factory {
    directory: PathBuf,
    port: u16,
}
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, _: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(Recognizer {
                directory: self.directory.clone(),
                port: self.port,
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
// Parent invokes exactly this test; an ordinary harness run has no child flag.
#[tokio::test]
async fn recorded_suite_bot() {
    if std::env::var_os("RECORDED_SUITE_CHILD").is_none() {
        return;
    }
    let args = std::env::args().collect::<Vec<_>>();
    let port: u16 = args[args.iter().position(|s| s == "--skip").unwrap() + 1]
        .parse()
        .unwrap();
    let directory = PathBuf::from(std::env::var_os("RECORDED_SUITE_DIR").unwrap());
    std::fs::write(
        directory.join(format!("started-{port}")),
        std::process::id().to_string(),
    )
    .unwrap();
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
        .serve(&Factory { directory, port }, Cancellation::new())
        .await
        .unwrap();
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
}
async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut b = Vec::new();
    (&mut pipe)
        .take(1024 * 1024 + 1)
        .read_to_end(&mut b)
        .await?;
    if b.len() > 1024 * 1024 {
        return Err(std::io::Error::other("CLI output bound"));
    }
    Ok(b)
}
async fn cli(path: &Path, recorded: bool) -> Result<Output, String> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut cmd = Command::new(executable);
    cmd.arg("suite").arg(path);
    if recorded {
        cmd.arg("--recorded-audio");
    }
    let mut child = cmd
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .map_err(|e| e.to_string())?;
    let out = child.stdout.take().unwrap();
    let err = child.stderr.take().unwrap();
    let result = tokio::time::timeout(WAIT, async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read_output(out), read_output(err))?;
        Ok::<_, std::io::Error>(Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;
    match result {
        Ok(Ok(output)) => Ok(output),
        other => {
            // The CLI owns native bot children. Request its normal cancellation
            // path instead of killing the parent and bypassing child teardown.
            let interrupted = if let Some(pid) = child.id() {
                Some(
                    Command::new("kill")
                        .arg("-INT")
                        .arg(pid.to_string())
                        .status()
                        .await,
                )
            } else {
                None
            };
            let joined = child.wait().await;
            Err(format!(
                "CLI {other:?}; interrupt {interrupted:?}; actual wait {joined:?}"
            ))
        }
    }
}
fn assert_reaped(v: &Value) {
    for run in v["runs"].as_array().unwrap() {
        assert_eq!(run["hosted"]["process"]["reaped"], true, "{v}");
        let pid = run["hosted"]["process"]["pid"].as_u64().unwrap();
        #[cfg(target_os = "linux")]
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }
}
#[tokio::test]
async fn cli_recorded_repeat_preserves_order_and_reaps_each_native_host() {
    let b = Bundle::new();
    b.wav("seven.wav", 7, 640);
    b.wav("eight.wav", 8, 640);
    b.json("a.json", scenario("a", "seven.wav", "tone-seven"));
    b.json("b.json", scenario("b", "eight.wav", "tone-eight"));
    let path = b.json("manifest.json", manifest(&b, vec!["a.json", "b.json"], 2));
    let out = cli(&path, true).await.unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["passed"], true);
    assert_reaped(&v);
    let runs = v["runs"].as_array().unwrap();
    assert_eq!(
        runs.iter()
            .map(|r| (
                r["attempt"].as_u64().unwrap(),
                r["scenario"].as_str().unwrap()
            ))
            .collect::<Vec<_>>(),
        vec![(1, "a"), (1, "b"), (2, "a"), (2, "b")]
    );
    assert_eq!(b.markers().len(), 4);
    for (i, run) in runs.iter().enumerate() {
        let p = run["port"].as_u64().unwrap();
        assert_eq!(
            std::fs::read(b.0.join(format!("audio-{p}"))).unwrap(),
            if i % 2 == 0 {
                [7, 0].repeat(640)
            } else {
                [8, 0].repeat(640)
            }
        );
        assert!(!b.0.join(format!("text-{p}")).exists());
        assert!(b.0.join(format!("cleaned-{p}")).exists());
    }
}
#[tokio::test]
async fn cli_failure_exit_and_default_text_suite_are_preserved() {
    let b = Bundle::new();
    b.wav("tone.wav", 7, 640);
    b.json("a.json", scenario("a", "tone.wav", "wrong"));
    let path = b.json("manifest.json", manifest(&b, vec!["a.json"], 1));
    let out = cli(&path, true).await.unwrap();
    assert_eq!(
        out.status.code(),
        Some(1),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["passed"], false);
    assert_reaped(&v);
    let b = Bundle::new();
    b.json("text.json",json!({"name":"text","turns":[{"user":"hello","expect":[{"event":"response","text_contains":"text-echo"}]}]}));
    let path = b.json("manifest.json", manifest(&b, vec!["text.json"], 1));
    let out = cli(&path, false).await.unwrap();
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_reaped(&v);
    assert_eq!(v["passed"], true);
}
#[tokio::test]
async fn all_inputs_and_repeat_budgets_reject_before_any_child() {
    let b = Bundle::new();
    b.wav("tone.wav", 7, 640);
    b.json("a.json", scenario("a", "tone.wav", "tone-seven"));
    b.json("bad.json", scenario("bad", "missing.wav", "x"));
    let path = b.json("manifest.json", manifest(&b, vec!["a.json", "bad.json"], 1));
    let out = cli(&path, true).await.unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(String::from_utf8_lossy(&out.stderr).contains("file I/O"));
    assert!(b.markers().is_empty());
    let out = cli(
        &b.json("manifest.json", manifest(&b, vec!["a.json"], 1)),
        false,
    )
    .await
    .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(b.markers().is_empty());
    b.wav("long.wav", 7, 960000);
    b.json("long.json", scenario("long", "long.wav", "x"));
    let path = b.json("manifest.json", manifest(&b, vec!["long.json"], 9));
    assert!(matches!(
        ReleaseManifest::load_recorded(path),
        Err(pipecat_evals::EvalError::Limit(
            "recording PCM bundle bytes"
        ))
    ));
    assert!(b.markers().is_empty());
}
#[tokio::test]
async fn public_plan_mutation_is_revalidated_before_launch() {
    let b = Bundle::new();
    b.wav("tone.wav", 7, 640);
    b.json("a.json", scenario("a", "tone.wav", "tone-seven"));
    let path = b.json("manifest.json", manifest(&b, vec!["a.json"], 1));
    let mut plan = ReleaseManifest::load_recorded(&path).unwrap();
    let extra = plan.jobs[0].scenario.turns[0].clone();
    plan.jobs[0].scenario.turns.push(extra);
    let result = run_release(
        plan,
        SessionOptions::default(),
        ProcessOptions::default(),
        Cancellation::new(),
    )
    .await;
    assert!(
        matches!(result,Err(pipecat_evals::EvalError::Scenario(ref s)) if s.contains("exactly one recording"))
    );
    assert!(b.markers().is_empty());
    let plan = ReleaseManifest::load_recorded(path).unwrap();
    let c = Cancellation::new();
    c.cancel();
    let result = run_release(
        plan,
        SessionOptions::default(),
        ProcessOptions::default(),
        c,
    )
    .await
    .unwrap();
    assert!(!result.passed);
    assert!(result.runs.iter().all(|r| r.hosted.process.pid.is_none()));
    assert!(b.markers().is_empty());
    let plan =
        ReleaseManifest::load_recorded(b.json("manifest.json", manifest(&b, vec!["a.json"], 4)))
            .unwrap();
    let result = pipecat_evals::release::run_release_with_budget(
        plan,
        SessionOptions::default(),
        ProcessOptions::default(),
        Cancellation::new(),
        1024,
    )
    .await;
    assert!(matches!(
        result,
        Err(pipecat_evals::EvalError::Limit("release summary budget"))
    ));
    assert!(b.markers().is_empty());
}
#[tokio::test]
async fn cancelling_active_recording_reaps_child_and_pending_runs_do_not_spawn() {
    let b = Bundle::new();
    b.wav("tone.wav", 7, 16000 * 5);
    let mut s = scenario("a", "tone.wav", "never");
    s["turns"][0]["expect"][0]["within_ms"] = 10000.into();
    b.json("a.json", s);
    let path = b.json("manifest.json", manifest(&b, vec!["a.json"], 2));
    let plan = ReleaseManifest::load_recorded(path).unwrap();
    let c = Cancellation::new();
    let child_cancel = c.clone();
    let mut task = tokio::spawn(async move {
        run_release(
            plan,
            SessionOptions::default(),
            ProcessOptions {
                startup_timeout: Duration::from_secs(2),
                shutdown_grace: Duration::from_secs(1),
                ..Default::default()
            },
            child_cancel,
        )
        .await
    });
    let observed = tokio::time::timeout(Duration::from_secs(4), async {
        loop {
            if std::fs::read_dir(&b.0).unwrap().any(|e| {
                e.unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("audio-")
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await;
    c.cancel();
    let joined = tokio::time::timeout(WAIT, &mut task).await;
    let (deadline, result) = match joined {
        Ok(r) => (false, r),
        Err(_) => (true, task.await),
    };
    assert!(!deadline);
    let result = result.unwrap().unwrap();
    assert!(observed.is_ok());
    assert!(!result.passed);
    assert_eq!(b.markers().len(), 1);
    let launched = result
        .runs
        .iter()
        .filter(|r| r.hosted.process.pid.is_some())
        .collect::<Vec<_>>();
    assert_eq!(launched.len(), 1);
    assert!(launched[0].hosted.process.reaped);
    let port = launched[0].port;
    let pcm = std::fs::read(b.0.join(format!("audio-{port}"))).unwrap();
    assert!(!pcm.is_empty() && pcm.len() < 16000 * 5 * 2);
    eprintln!(
        "active Cancel process report: {:?}",
        launched[0].hosted.process
    );
    // Existing process ownership deliberately force-kills a still-running child
    // on external cancellation; such a process cannot promise application cleanup.
    if !launched[0].hosted.process.forced {
        assert!(b.0.join(format!("cleaned-{port}")).exists());
    }
    tokio::time::timeout(Duration::from_secs(1), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("parent playback/session/pipe tasks remained alive");
    #[cfg(target_os = "linux")]
    assert!(!Path::new(&format!(
        "/proc/{}",
        launched[0].hosted.process.pid.unwrap()
    ))
    .exists());
    assert!(result.runs.iter().all(|r| r
        .hosted
        .result
        .failures
        .iter()
        .any(|f| f.kind == "cancelled")));
}

#[test]
fn global_cross_file_allowances_and_nonregular_manifests_reject() {
    let b = Bundle::new();
    b.wav("tone.wav", 7, 640);
    for name in ["a", "b"] {
        let mut s = scenario(name, "tone.wav", "x");
        let turn = s["turns"][0].clone();
        s["turns"] = Value::Array(vec![turn; 65]);
        b.json(&format!("{name}.json"), s);
    }
    let path = b.json("manifest.json", manifest(&b, vec!["a.json", "b.json"], 1));
    assert!(matches!(
        ReleaseManifest::load_recorded(path),
        Err(pipecat_evals::EvalError::Limit("recording references"))
    ));
    b.wav("large.wav", 7, 960000);
    for name in ["a", "b"] {
        let mut s = scenario(name, "large.wav", "x");
        let turn = s["turns"][0].clone();
        s["turns"] = Value::Array(vec![turn; 5]);
        b.json(&format!("{name}.json"), s);
    }
    let path = b.json("manifest.json", manifest(&b, vec!["a.json", "b.json"], 1));
    assert!(matches!(
        ReleaseManifest::load_recorded(path),
        Err(pipecat_evals::EvalError::Limit("recording bundle bytes"))
    ));
    let fifo = b.0.join("fifo.yaml");
    assert!(std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    assert!(
        matches!(ReleaseManifest::load_recorded(fifo), Err(pipecat_evals::EvalError::Scenario(ref s)) if s.contains("regular files"))
    );
    assert!(b.markers().is_empty());
}
