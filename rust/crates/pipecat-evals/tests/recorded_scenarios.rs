//! Unix trusted-local recording bundles and the actual CLI -> native host path.
#![cfg(unix)]
use pipecat_core::*;
use pipecat_evals::{bot_host::*, Cancellation, EvalError, RecordedScenarioFile, ScenarioFile};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    path::{Path, PathBuf},
    process::{Output, Stdio},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command};
type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(6);
static NEXT: AtomicUsize = AtomicUsize::new(0);
struct Bundle(PathBuf);
impl Bundle {
    fn new() -> Self {
        let p = std::env::temp_dir().join(format!(
            "pipecat-recorded-bundle-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&p).unwrap();
        Self(p)
    }
    fn put(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let p = self.0.join(name);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, bytes).unwrap();
        p
    }
    fn scenario(&self, value: Value) -> PathBuf {
        self.put("case.json", serde_json::to_vec(&value).unwrap())
    }
}
impl Drop for Bundle {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn wav(samples: usize, tone: i16) -> Vec<u8> {
    let pcm: Vec<u8> = (0..samples).flat_map(|_| tone.to_le_bytes()).collect();
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
    b
}
fn script() -> Value {
    json!({"name":"recorded","user":{"modality":"audio"},"turns":[{"user":"REFERENCE NEVER SENT","audio":"tone.wav","expect":[{"event":"user_transcription","text_contains":"tone seven","within_ms":1000},{"event":"llm_response","text_contains":"accepted recording","within_ms":1000}]}]})
}
#[test]
fn loader_preserves_public_text_schema_and_extracts_audio_without_fallback() {
    let b = Bundle::new();
    b.put("tone.wav", wav(640, 7));
    let path = b.scenario(script());
    assert!(matches!(
        ScenarioFile::load(&path),
        Err(EvalError::Unsupported(_))
    ));
    let file = RecordedScenarioFile::load(&path).unwrap();
    assert_eq!(file.name(), "recorded");
    assert_eq!(file.len(), 1);
    assert!(!file.is_empty());
    assert_eq!(
        file.into_sessions("ws://127.0.0.1:1", Default::default())
            .unwrap()
            .len(),
        1
    );
    for (i, mut v) in [script(), script(), script(), script(), script(), script()]
        .into_iter()
        .enumerate()
    {
        match i {
            0 => {
                v["user"]["modality"] = "text".into();
            }
            1 => {
                v["turns"][0].as_object_mut().unwrap().remove("audio");
            }
            2 => {
                v["turns"][0]["user"] = " ".into();
            }
            3 => {
                v["turns"][0]["dtmf"] = "1".into();
            }
            4 => {
                v["user"]["speech"] = json!({"provider":"anything"});
            }
            _ => {
                v["turns"][0]["audio"] = 3.into();
            }
        }
        assert!(RecordedScenarioFile::load(b.scenario(v)).is_err());
    }
}
#[test]
fn includes_inherit_shallowly_and_media_uses_outer_entry_directory() {
    let b = Bundle::new();
    b.put("tone.wav", wav(640, 7));
    b.put("sub/tone.wav", b"invalid decoy");
    b.put(
        "sub/deeper/turns.yaml",
        b"- user: reference\n  audio: tone.wav\n",
    );
    b.put("sub/turns.yaml", b"!include deeper/turns.yaml\n");
    let main=b.put("main.yaml",b"name: grouped\nuser: {modality: audio}\nturns: !include sub/turns.yaml\nscenarios:\n  - name: one\n  - name: two\n");
    let file = RecordedScenarioFile::load(&main).unwrap();
    assert_eq!(file.len(), 2);
    // A symlink entry inside the bundle retains its entry parent as the media base.
    b.put(
        "sub/main.yaml",
        b"name: link\nuser: {modality: audio}\nturns:\n  - user: ref\n    audio: tone.wav\n",
    );
    std::os::unix::fs::symlink("sub/main.yaml", b.0.join("link.yaml")).unwrap();
    assert_eq!(
        RecordedScenarioFile::load(b.0.join("link.yaml"))
            .unwrap()
            .len(),
        1
    );
    let bad=b.put("replace.yaml",b"name: group\nuser: {modality: audio}\nturns: [{user: ref, audio: tone.wav}]\nscenarios:\n - name: child\n   user: {}\n");
    assert!(matches!(
        RecordedScenarioFile::load(bad),
        Err(EvalError::Scenario(_))
    ));
}
#[test]
fn all_schema_and_metadata_validate_before_recording_decode() {
    let b = Bundle::new();
    b.put("bad.wav", b"not WAV");
    let v = json!({"name":"group","user":{"modality":"audio"},"scenarios":[{"name":"first","turns":[{"user":"ref","audio":"bad.wav"}]},{"name":"second","turns":[{"user":"ref","audio":"missing.wav"}]}]});
    assert!(
        matches!(RecordedScenarioFile::load(b.scenario(v)),Err(EvalError::Io(ref e)) if e.kind()==std::io::ErrorKind::NotFound)
    );
    let mut v = script();
    v["turns"][0]["audio"] = "missing.wav".into();
    v["turns"]
        .as_array_mut()
        .unwrap()
        .push(json!({"user":"forgot audio"}));
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(v)),
        Err(EvalError::Unsupported(_))
    ));
}
#[test]
fn bundle_limits_count_repeated_references_across_all_scenarios() {
    let b = Bundle::new();
    b.put("tone.wav", wav(855000, 7));
    let v = json!({"name":"group","user":{"modality":"audio"},"turns":[{"user":"ref","audio":"tone.wav"}],"scenarios":(0..9).map(|i|json!({"name":format!("s{i}")})).collect::<Vec<_>>()});
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(v)),
        Err(EvalError::Limit("recording PCM bundle bytes"))
    ));
    let mut v = script();
    v["turns"] = Value::Array(
        (0..129)
            .map(|_| json!({"user":"ref","audio":"missing.wav"}))
            .collect(),
    );
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(v)),
        Err(EvalError::Limit("recording references"))
    ));
    let f = std::fs::File::create(b.0.join("large.wav")).unwrap();
    f.set_len(1_985_537).unwrap();
    let mut v = script();
    v["turns"][0]["audio"] = "large.wav".into();
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(v)),
        Err(EvalError::Limit("recording WAV bytes"))
    ));
}
#[test]
fn nonregular_and_outside_bundle_paths_are_rejected_without_blocking() {
    let b = Bundle::new();
    let outside = Bundle::new();
    let out = outside.put("tone.wav", wav(640, 7));
    std::os::unix::fs::symlink(out, b.0.join("escape.wav")).unwrap();
    let mut v = script();
    v["turns"][0]["audio"] = "escape.wav".into();
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(v)),
        Err(EvalError::Scenario(_))
    ));
    let fifo = b.0.join("pipe");
    assert!(std::process::Command::new("mkfifo")
        .arg(&fifo)
        .status()
        .unwrap()
        .success());
    assert!(matches!(
        RecordedScenarioFile::load(&fifo),
        Err(EvalError::Scenario(_))
    ));
    let main = b.put(
        "include.yaml",
        b"name: fifo\nuser: {modality: audio}\nturns: !include pipe\n",
    );
    assert!(matches!(
        RecordedScenarioFile::load(main),
        Err(EvalError::Scenario(_))
    ));
    let mut v = script();
    v["turns"][0]["audio"] = "pipe".into();
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(v)),
        Err(EvalError::Scenario(_))
    ));
}
#[derive(Default)]
struct Trace {
    audio: Mutex<Vec<Vec<u8>>>,
    text: AtomicUsize,
    cleaned: AtomicUsize,
}
struct PcmFixture(Arc<Trace>);
#[async_trait]
impl FrameProcessor for PcmFixture {
    fn name(&self) -> &str {
        "recorded-cli-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let response = match &frame.data {
            FrameData::InputAudio(a) => {
                self.0.audio.lock().unwrap().push(a.audio.clone());
                a.audio == [7, 0].repeat(640)
            }
            FrameData::LlmMessagesAppend(_) => {
                self.0.text.fetch_add(1, Ordering::SeqCst);
                true
            }
            _ => false,
        };
        if response {
            for data in [
                FrameData::Transcription(TranscriptionFrame {
                    text: "tone seven".into(),
                    finalized: true,
                    ..Default::default()
                }),
                FrameData::LlmResponseStart,
                FrameData::LlmText(TextFrame::new("accepted recording")),
                FrameData::LlmResponseEnd,
            ] {
                ctx.push(Frame::new(data), direction).await?;
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.0.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Factory(Arc<Trace>);
#[async_trait]
impl EvalBotFactory for Factory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        if !settings.skip_tts {
            return Err(BotHostError::Invalid("skip_tts must be true".into()));
        }
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(PcmFixture(self.0.clone()))],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut b = Vec::new();
    (&mut pipe).take(256 * 1024 + 1).read_to_end(&mut b).await?;
    if b.len() > 256 * 1024 {
        return Err(std::io::Error::other("CLI output limit"));
    }
    Ok(b)
}
async fn cli(args: &[String], cwd: &Path) -> Result<Output, Error> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut child = Command::new(executable)
        .args(args)
        .current_dir(cwd)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let run = tokio::time::timeout(WAIT, async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read_output(stdout), read_output(stderr))?;
        Ok::<_, std::io::Error>(Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;
    match run {
        Ok(Ok(output)) => Ok(output),
        other => {
            let killed = child.start_kill();
            let joined = child.wait().await;
            Err(format!("CLI operation {other:?}; kill={killed:?}; actual wait={joined:?}").into())
        }
    }
}
async fn hosted_cli(recorded: bool, expected_pass: bool) -> (Output, Arc<Trace>) {
    let b = Bundle::new();
    b.put("tone.wav", wav(640, if expected_pass { 7 } else { 8 }));
    let mut v = script();
    if !expected_pass {
        v["turns"][0]["expect"][0]["within_ms"] = 40.into();
    }
    if !recorded {
        v.as_object_mut().unwrap().remove("user");
        v["turns"][0].as_object_mut().unwrap().remove("audio");
    }
    let path = b.scenario(v);
    let trace = Arc::new(Trace::default());
    let t = trace.clone();
    let host = EvalBotHost::bind(
        "127.0.0.1:0".parse().unwrap(),
        EvalBotHostConfig {
            startup_timeout: Duration::from_secs(2),
            session_timeout: WAIT,
            host_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(2),
            transport: SessionConfig {
                write_timeout: Duration::from_millis(300),
                close_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_raw_audio_input(recorded);
    let url = format!("ws://{}/", host.local_addr().unwrap());
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let mut task = tokio::spawn(async move { host.serve(&Factory(t), c).await });
    let mut args = vec![
        "run".into(),
        path.display().to_string(),
        url,
        "--stop-bot".into(),
    ];
    if recorded {
        args.push("--recorded-audio".into());
    }
    let result = cli(&args, &std::env::temp_dir()).await;
    let done = tokio::time::timeout(WAIT, &mut task).await;
    let (deadline, report) = match done {
        Ok(r) => (false, r),
        Err(_) => {
            cancel.cancel();
            (true, task.await)
        }
    };
    assert!(!deadline, "host cleanup deadline; actual={report:?}");
    let report = report.unwrap().unwrap();
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    (result.unwrap(), trace)
}
#[tokio::test]
async fn actual_cli_recording_reaches_native_host_and_assertions_pass() {
    let (out, trace) = hosted_cli(true, true).await;
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let v: Value = serde_json::from_slice(&out.stdout).unwrap();
    assert_eq!(v["passed"], true);
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert!(trace
        .audio
        .lock()
        .unwrap()
        .iter()
        .any(|a| a == &[7, 0].repeat(640)));
}
#[tokio::test]
async fn actual_cli_reports_failed_audio_expectation_and_preserves_old_text_run() {
    let (out, trace) = hosted_cli(true, false).await;
    assert_eq!(out.status.code(), Some(1));
    assert_eq!(
        serde_json::from_slice::<Value>(&out.stdout).unwrap()["passed"],
        false
    );
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    let (out, trace) = hosted_cli(false, true).await;
    assert_eq!(
        out.status.code(),
        Some(0),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    assert_eq!(trace.text.load(Ordering::SeqCst), 1);
    assert!(trace.audio.lock().unwrap().is_empty());
}
#[tokio::test]
async fn cli_invalid_flags_missing_recordings_and_default_audio_reject_before_connect() {
    let b = Bundle::new();
    let path = b.scenario(script());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    for flags in [
        vec![],
        vec!["--recorded-audio"],
        vec!["--recorded-audio", "--recorded-audio"],
        vec!["--unknown"],
        vec!["--stop-bot", "--stop-bot"],
    ] {
        let mut args = vec!["run".into(), path.display().to_string(), url.clone()];
        args.extend(flags.into_iter().map(str::to_owned));
        let out = cli(&args, &b.0).await.unwrap();
        assert_eq!(
            out.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err(),
            "invalid CLI connected"
        );
    }
    let out = cli(
        &[
            "suite".into(),
            path.display().to_string(),
            "--recorded-audio".into(),
        ],
        &b.0,
    )
    .await
    .unwrap();
    assert_eq!(out.status.code(), Some(2));
}

#[test]
fn expanded_header_context_has_a_shared_budget_even_without_recordings() {
    let b = Bundle::new();
    let value = json!({"name":"group","user":{"modality":"audio"},"context":[{"role":"user","content":"x".repeat(65536)}],"turns":[],"scenarios":(0..100).map(|i|json!({"name":format!("s{i}")})).collect::<Vec<_>>()});
    assert!(matches!(
        RecordedScenarioFile::load(b.scenario(value)),
        Err(EvalError::Limit("expanded recorded scenario bytes"))
    ));
}
