//! Real CLI subprocess -> native host -> PCM16 WAV; synthetic PCM is not a
//! speech-quality or playback test. Every assertion follows child reaping and
//! host joining. Replay evidence must retain the CLI selected by the override.
#![cfg(unix)]

use pipecat_audio::read_pcm16_wav;
use pipecat_core::*;
use pipecat_evals::{
    bot_host::*, matcher::ScriptResult, BotAudioChunk, BotAudioRun, Cancellation, EvalError,
};
use pipecat_transports::SessionConfig;
use pipecat_turns::SharedContext;
use serde_json::{json, Value};
use std::{
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    process::{ExitStatus, Output, Stdio},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{io::AsyncReadExt, process::Command, sync::Notify, task::JoinHandle};

type Error = Box<dyn std::error::Error + Send + Sync>;
const WAIT: Duration = Duration::from_secs(8);
const CLEANUP: Duration = Duration::from_secs(3);
const RATE: u32 = 24_000;
const FIRST: &[u8] = &[0, 128, 246, 255, 0, 0];
const SECOND: &[u8] = &[1, 0, 255, 127];
const COMPETITOR: &[u8] = b"another writer owns this path";
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct Bundle(PathBuf);
impl Bundle {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pipecat-bot-wav-cli-{}-{}",
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
    fn scenario(&self, recorded: bool, fail: bool) -> PathBuf {
        let mut value = json!({"name":"native bot WAV", "turns":[
            {"user":"play", "expect":[
                {"event":"llm_response", "text_contains":"fixture response", "within_ms":2000},
                {"event":"user_transcription", "text_contains":"PCM wire fence", "within_ms":2000}
            ]},
            {"user":"finish", "send_after":{"event":"user_transcription"},
             "expect":[{"event":"bot_stopped_speaking", "within_ms":2000}]}
        ]});
        if fail {
            value["turns"][1]["expect"][0] = json!({
                "event":"llm_response", "text_contains":"deliberately absent", "within_ms":80
            });
        }
        if recorded {
            value["user"] = json!({"modality":"audio"});
            value["turns"][0]["audio"] = "play.wav".into();
            value["turns"][1]["audio"] = "finish.wav".into();
            self.put("play.wav", input_wav(7));
            self.put("finish.wav", input_wav(9));
        }
        self.put("case.json", serde_json::to_vec(&value).unwrap())
    }
    fn names(&self) -> Vec<PathBuf> {
        let mut names = std::fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().file_name().into())
            .collect::<Vec<_>>();
        names.sort();
        names
    }
}
impl Drop for Bundle {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).unwrap();
    }
}
fn input_wav(tone: i16) -> Vec<u8> {
    let pcm = tone.to_le_bytes().repeat(640);
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

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Normal,
    MixedRate,
    Empty,
    WaitForCancel,
    CompetingFile,
    CompetingDirectory,
}
#[derive(Default)]
struct Trace {
    settings: Mutex<Vec<bool>>,
    configured: Mutex<Vec<bool>>,
    text: Mutex<Vec<String>>,
    input_audio: Mutex<Vec<Vec<u8>>>,
    finish: Notify,
    fences: AtomicUsize,
    cleaned: AtomicUsize,
    dropped: AtomicUsize,
    changed_target: AtomicBool,
}
struct Fixture {
    trace: Arc<Trace>,
    skip_tts: AtomicBool,
    mode: Mode,
    output: PathBuf,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
async fn push(ctx: &ProcessorContext, data: FrameData) -> Result<(), ProcessorError> {
    ctx.push(Frame::new(data), Direction::Downstream).await
}
async fn pcm_fence(ctx: &ProcessorContext) -> Result<(), ProcessorError> {
    // Returning flush reaches root admission while this processor is blocked.
    // The later transcription uses the SAME observer queue as captured PCM.
    // A subsequent client input proves the client received both chunks first.
    let delivered = LocalFlushEvent::default();
    push(
        ctx,
        FrameData::FlushDetailed(PipelineFlushFrame {
            event: Some(delivered.clone()),
            returning: true,
            origin: Some("bot-wav-cli-fixture".into()),
        }),
    )
    .await?;
    tokio::time::timeout(CLEANUP, delivered.wait())
        .await
        .map_err(|_| ProcessorError::fatal("WAV fixture root fence timeout"))?;
    push(
        ctx,
        FrameData::Transcription(TranscriptionFrame {
            text: "PCM wire fence".into(),
            finalized: true,
            ..Default::default()
        }),
    )
    .await
}
#[async_trait]
impl FrameProcessor for Fixture {
    fn name(&self) -> &str {
        "native-bot-wav-cli-fixture"
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
                let text = messages.messages.first().map(|m| m.content.clone());
                if let Some(text) = &text {
                    self.trace.text.lock().unwrap().push(text.clone());
                }
                text
            }
            FrameData::InputAudio(audio) => {
                self.trace
                    .input_audio
                    .lock()
                    .unwrap()
                    .push(audio.audio.clone());
                if audio.audio == [7, 0].repeat(640) {
                    Some("play".into())
                } else if audio.audio == [9, 0].repeat(640) {
                    Some("finish".into())
                } else {
                    None
                }
            }
            _ => None,
        };
        match input.as_deref() {
            Some("play") => {
                for data in [
                    FrameData::LlmResponseStart,
                    FrameData::LlmText(TextFrame::new("fixture response")),
                    FrameData::LlmResponseEnd,
                ] {
                    push(ctx, data).await?;
                }
                if !self.skip_tts.load(Ordering::SeqCst) && self.mode != Mode::Empty {
                    push(ctx, FrameData::BotStartedSpeaking).await?;
                    push(
                        ctx,
                        FrameData::OutputAudio(
                            AudioRawFrame::new(FIRST.to_vec(), RATE, 1).unwrap(),
                        ),
                    )
                    .await?;
                    push(
                        ctx,
                        FrameData::TtsAudio(
                            AudioRawFrame::new(
                                SECOND.to_vec(),
                                if self.mode == Mode::MixedRate {
                                    48_000
                                } else {
                                    RATE
                                },
                                1,
                            )
                            .unwrap(),
                        ),
                    )
                    .await?;
                    // Neither input audio nor upstream output belongs in the WAV.
                    push(
                        ctx,
                        FrameData::InputAudio(AudioRawFrame::new(vec![88, 0], 16_000, 1).unwrap()),
                    )
                    .await?;
                    ctx.push(
                        Frame::new(FrameData::OutputAudio(
                            AudioRawFrame::new(vec![99, 0], 8_000, 1).unwrap(),
                        )),
                        Direction::Upstream,
                    )
                    .await?;
                }
                pcm_fence(ctx).await?;
                self.trace.fences.fetch_add(1, Ordering::SeqCst);
            }
            Some("finish") => {
                if self.mode == Mode::CompetingFile {
                    std::fs::write(&self.output, COMPETITOR)
                        .map_err(|e| ProcessorError::fatal(e.to_string()))?;
                    self.trace.changed_target.store(true, Ordering::SeqCst);
                } else if self.mode == Mode::CompetingDirectory {
                    std::fs::create_dir(&self.output)
                        .map_err(|e| ProcessorError::fatal(e.to_string()))?;
                    self.trace.changed_target.store(true, Ordering::SeqCst);
                }
                self.trace.finish.notify_one();
                if self.mode != Mode::WaitForCancel {
                    push(ctx, FrameData::BotStoppedSpeaking).await?;
                }
            }
            _ => {}
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
    mode: Mode,
    output: PathBuf,
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
                mode: self.mode,
                output: self.output.clone(),
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
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
async fn cli(args: &[String], cwd: &Path, cancel_after: Option<&Notify>) -> Result<Output, Error> {
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
    let stdout = tokio::spawn(read_output(child.stdout.take().unwrap()));
    let stderr = tokio::spawn(read_output(child.stderr.take().unwrap()));
    let mut problems = Vec::new();
    if let Some(ready) = cancel_after {
        if tokio::time::timeout(WAIT, ready.notified()).await.is_err() {
            problems.push("CLI never consumed the PCM fence before cancellation".into());
        }
        if let Err(error) = interrupt(child.id()) {
            problems.push(error.to_string());
        }
    }
    // Timeout borrows the child; it cannot detach the process wait. Even a
    // successful recovery is a test failure, never a silently accepted pass.
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

struct Finished {
    output: Result<Output, Error>,
    report: Result<Result<EvalBotHostReport, BotHostError>, tokio::task::JoinError>,
    host_late: bool,
    trace: Arc<Trace>,
}
impl Finished {
    fn checked(self, capture: bool) -> (Output, Arc<Trace>) {
        // Both owners have actually completed before any behavioral assertion.
        assert!(
            !self.host_late,
            "host deadline; actual join: {:?}; CLI: {:?}",
            self.report, self.output
        );
        let report = self.report.unwrap().unwrap();
        assert_eq!(report.sessions, 1);
        assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
        assert_eq!(self.trace.cleaned.load(Ordering::SeqCst), 1);
        assert_eq!(self.trace.dropped.load(Ordering::SeqCst), 1);
        assert_eq!(self.trace.fences.load(Ordering::SeqCst), 1);
        assert_eq!(*self.trace.settings.lock().unwrap(), [!capture]);
        assert_eq!(*self.trace.configured.lock().unwrap(), [!capture]);
        (self.output.unwrap(), self.trace)
    }
}
async fn hosted(
    bundle: &Bundle,
    scenario: &Path,
    output: &Path,
    mode: Mode,
    capture: bool,
    recorded: bool,
) -> Finished {
    let trace = Arc::new(Trace::default());
    let factory = Factory {
        trace: trace.clone(),
        mode,
        output: output.into(),
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
            host_timeout: Duration::from_secs(30),
            shutdown_timeout: CLEANUP,
            ..Default::default()
        },
    )
    .await
    .unwrap()
    .with_audio_output(true)
    .with_raw_audio_input(recorded);
    let url = format!("ws://{}/", host.local_addr().unwrap());
    let cancel = Cancellation::new();
    let host_cancel = cancel.clone();
    let mut owner = tokio::spawn(async move { host.serve(&factory, host_cancel).await });
    let mut args = vec![
        "run".into(),
        scenario.display().to_string(),
        url,
        "--stop-bot".into(),
    ];
    if recorded {
        args.push("--recorded-audio".into());
    }
    if capture {
        args.extend(["--bot-audio-wav".into(), output.display().to_string()]);
    }
    let result = cli(
        &args,
        &bundle.0,
        (mode == Mode::WaitForCancel).then_some(&trace.finish),
    )
    .await;
    let (report, host_late) = match tokio::time::timeout(WAIT, &mut owner).await {
        Ok(report) => (report, false),
        Err(_) => {
            cancel.cancel();
            (owner.await, true)
        }
    };
    Finished {
        output: result,
        report,
        host_late,
        trace,
    }
}
fn result(output: &Output, code: i32, passed: bool) -> ScriptResult {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    let value: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(
        value.get("result").is_none(),
        "stdout must stay ScriptResult"
    );
    assert!(
        value.get("chunks").is_none(),
        "PCM must not wrap/leak into stdout"
    );
    let result: ScriptResult = serde_json::from_value(value).unwrap();
    assert_eq!(result.passed, passed);
    result
}
fn assert_wav(path: &Path) {
    assert_eq!(
        std::fs::metadata(path).unwrap().permissions().mode() & 0o777,
        0o600,
        "published bot PCM must be owner-readable/writable only"
    );
    let bytes = std::fs::read(path).unwrap();
    let wav = read_pcm16_wav(&bytes).unwrap();
    assert_eq!(wav.format.sample_rate(), RATE);
    assert_eq!(wav.format.channels(), 1);
    assert_eq!(wav.pcm, [FIRST, SECOND].concat());
}

fn public_run(chunks: Vec<BotAudioChunk>) -> BotAudioRun {
    BotAudioRun {
        result: ScriptResult::new("publicly assembled capture", 0),
        chunks,
    }
}
fn chunk(audio: Vec<u8>, sample_rate: u32) -> BotAudioChunk {
    BotAudioChunk { audio, sample_rate }
}

#[test]
fn public_wav_encoder_empty_and_ordered_pcm_round_trip() {
    assert!(public_run(Vec::new()).to_wav().unwrap().is_none());
    let run = public_run(vec![
        chunk(FIRST.to_vec(), RATE),
        chunk(SECOND.to_vec(), RATE),
    ]);
    let original = run.chunks.clone();
    let bytes = run.to_wav().unwrap().unwrap();
    let wav = read_pcm16_wav(&bytes).unwrap();
    assert_eq!(wav.format.sample_rate(), RATE);
    assert_eq!(wav.format.channels(), 1);
    assert_eq!(wav.pcm, [FIRST, SECOND].concat());
    assert_eq!(run.chunks, original);
    assert!(
        !run.result.passed,
        "export must not turn a failed result into a pass"
    );
}

#[test]
fn public_wav_encoder_revalidates_modified_chunks_before_export() {
    for invalid in [
        chunk(vec![], RATE),
        chunk(vec![1], RATE),
        chunk(vec![1, 0], 0),
        chunk(vec![1, 0], 22_050),
        chunk(vec![1, 0], u32::MAX),
    ] {
        let run = public_run(vec![chunk(FIRST.to_vec(), RATE), invalid]);
        assert!(matches!(run.to_wav(), Err(EvalError::Scenario(_))));
    }
    let mixed = public_run(vec![
        chunk(FIRST.to_vec(), RATE),
        chunk(SECOND.to_vec(), 48_000),
    ]);
    assert!(
        matches!(mixed.to_wav(), Err(EvalError::Unsupported(message)) if message.contains("mixed-rate"))
    );
}

#[test]
fn public_wav_encoder_enforces_absolute_chunk_and_byte_budgets() {
    let too_many = public_run(vec![chunk(vec![1, 0], RATE); 16_385]);
    assert!(matches!(
        too_many.to_wav(),
        Err(EvalError::Limit("bot audio WAV chunks"))
    ));
    let too_large = public_run(vec![
        chunk(vec![0; 16 * 1024 * 1024], RATE),
        chunk(vec![1, 0], RATE),
    ]);
    assert!(matches!(
        too_large.to_wav(),
        Err(EvalError::Limit("bot audio WAV bytes"))
    ));
    let at_chunk_limit = public_run(vec![chunk(vec![1, 0], RATE); 16_384]);
    let wav = at_chunk_limit.to_wav().unwrap().unwrap();
    assert_eq!(read_pcm16_wav(&wav).unwrap().pcm, [1, 0].repeat(16_384));
}

#[tokio::test]
async fn actual_cli_exports_two_chunks_at_original_rate_and_keeps_script_result() {
    let b = Bundle::new();
    let scenario = b.scenario(false, false);
    let target = b.0.join("bot.wav");
    let (out, trace) = hosted(&b, &scenario, &target, Mode::Normal, true, false)
        .await
        .checked(true);
    let result = result(&out, 0, true);
    assert!(result.failures.is_empty());
    assert_eq!(*trace.text.lock().unwrap(), ["play", "finish"]);
    assert_wav(&target);
    assert_eq!(
        b.names(),
        [PathBuf::from("bot.wav"), PathBuf::from("case.json")]
    );
}

#[tokio::test]
async fn actual_cli_default_text_does_not_request_capture_or_touch_files() {
    let b = Bundle::new();
    let scenario = b.scenario(false, false);
    // The default observer exposes text/transcription, not audio lifecycle.
    // Finish on the first turn's text and fence instead of requiring a
    // bot-stopped-speaking event that belongs to the opt-in audio contract.
    let mut script: Value = serde_json::from_slice(&std::fs::read(&scenario).unwrap()).unwrap();
    script["turns"].as_array_mut().unwrap().truncate(1);
    std::fs::write(&scenario, serde_json::to_vec(&script).unwrap()).unwrap();
    let before = b.names();
    let target = b.0.join("bot.wav");
    let (out, trace) = hosted(&b, &scenario, &target, Mode::Normal, false, false)
        .await
        .checked(false);
    result(&out, 0, true);
    assert_eq!(*trace.text.lock().unwrap(), ["play"]);
    let after = b.names();
    assert_eq!(after, before);
    assert_eq!(
        after,
        [PathBuf::from("case.json")],
        "default text run must leave no WAV or .pipecat-bot-audio-*.tmp artifact"
    );
    assert!(!target.exists());
    assert!(!String::from_utf8_lossy(&out.stderr)
        .to_lowercase()
        .contains("wav"));
}

#[tokio::test]
async fn actual_cli_empty_capture_is_explicit_and_does_not_create_a_file() {
    let b = Bundle::new();
    let scenario = b.scenario(false, false);
    let before = b.names();
    let target = b.0.join("bot.wav");
    let (out, _) = hosted(&b, &scenario, &target, Mode::Empty, true, false)
        .await
        .checked(true);
    result(&out, 0, true);
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("empty") || stderr.contains("no bot audio"),
        "{stderr}"
    );
    assert!(!target.exists());
    assert_eq!(b.names(), before);
}

#[tokio::test]
async fn actual_cli_mixed_rates_preserve_original_result_and_create_no_artifact() {
    for failed_evaluation in [false, true] {
        let b = Bundle::new();
        let scenario = b.scenario(false, failed_evaluation);
        let before = b.names();
        let target = b.0.join("bot.wav");
        let (out, _) = hosted(&b, &scenario, &target, Mode::MixedRate, true, false)
            .await
            .checked(true);
        result(&out, 2, !failed_evaluation);
        let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
        assert!(
            stderr.contains("rate") && (stderr.contains("mixed") || stderr.contains("same")),
            "{stderr}"
        );
        assert!(!target.exists());
        assert_eq!(b.names(), before);
    }
}

#[tokio::test]
async fn actual_cli_failed_evaluation_still_exports_pcm_and_preserves_exit_one() {
    let b = Bundle::new();
    let scenario = b.scenario(false, true);
    let target = b.0.join("bot.wav");
    let (out, _) = hosted(&b, &scenario, &target, Mode::Normal, true, false)
        .await
        .checked(true);
    assert!(!result(&out, 1, false).failures.is_empty());
    assert_wav(&target);
}

#[tokio::test]
async fn actual_cli_sigint_keeps_partial_pcm_after_child_reap_and_host_join() {
    let b = Bundle::new();
    let scenario = b.scenario(false, false);
    let target = b.0.join("bot.wav");
    let (out, trace) = hosted(&b, &scenario, &target, Mode::WaitForCancel, true, false)
        .await
        .checked(true);
    let result = result(&out, 1, false);
    assert!(result
        .failures
        .iter()
        .any(|failure| failure.kind == "cancelled"));
    assert_eq!(*trace.text.lock().unwrap(), ["play", "finish"]);
    assert_wav(&target);
}

#[tokio::test]
async fn actual_cli_racing_destination_is_preserved_and_temporary_file_is_removed() {
    let b = Bundle::new();
    let scenario = b.scenario(false, false);
    let target = b.0.join("bot.wav");
    let (out, trace) = hosted(&b, &scenario, &target, Mode::CompetingFile, true, false)
        .await
        .checked(true);
    result(&out, 2, true);
    assert!(trace.changed_target.load(Ordering::SeqCst));
    assert_eq!(std::fs::read(&target).unwrap(), COMPETITOR);
    assert!(String::from_utf8_lossy(&out.stderr)
        .to_lowercase()
        .contains("exist"));
    assert_eq!(
        b.names(),
        [PathBuf::from("bot.wav"), PathBuf::from("case.json")]
    );
}

#[tokio::test]
async fn actual_cli_artifact_io_failure_keeps_the_failed_script_result() {
    let b = Bundle::new();
    let scenario = b.scenario(false, true);
    let target = b.0.join("bot.wav");
    let (out, trace) = hosted(
        &b,
        &scenario,
        &target,
        Mode::CompetingDirectory,
        true,
        false,
    )
    .await
    .checked(true);
    assert!(!result(&out, 2, false).failures.is_empty());
    assert!(trace.changed_target.load(Ordering::SeqCst));
    let stderr = String::from_utf8_lossy(&out.stderr).to_lowercase();
    assert!(
        stderr.contains("wav") && stderr.contains("exist"),
        "{stderr}"
    );
    assert!(target.is_dir());
    assert_eq!(std::fs::read_dir(&target).unwrap().count(), 0);
    assert_eq!(
        b.names(),
        [PathBuf::from("bot.wav"), PathBuf::from("case.json")]
    );
}

#[tokio::test]
async fn actual_cli_recorded_input_and_bot_output_remain_separate() {
    let b = Bundle::new();
    let scenario = b.scenario(true, false);
    let target = b.0.join("bot.wav");
    let (out, trace) = hosted(&b, &scenario, &target, Mode::Normal, true, true)
        .await
        .checked(true);
    result(&out, 0, true);
    assert!(trace.text.lock().unwrap().is_empty());
    let audio = trace.input_audio.lock().unwrap();
    let active = audio
        .iter()
        .filter(|chunk| chunk.iter().any(|byte| *byte != 0))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(active, [[7, 0].repeat(640), [9, 0].repeat(640)]);
    assert_wav(&target);
}

#[tokio::test]
async fn invalid_export_requests_and_existing_targets_are_rejected_before_connect() {
    let b = Bundle::new();
    let scenario = b.scenario(false, false);
    let multi = b.put(
        "multi.json",
        serde_json::to_vec(&json!({
            "name":"group", "turns":[{"user":"play"}],
            "scenarios":[{"name":"one"},{"name":"two"}]
        }))
        .unwrap(),
    );
    let existing = b.put("existing.wav", COMPETITOR);
    let target = b.0.join("new.wav");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let requests = [
        (scenario.clone(), vec!["--bot-audio-wav".into()]),
        (
            scenario.clone(),
            vec!["--bot-audio-wav".into(), "--stop-bot".into()],
        ),
        (
            scenario.clone(),
            vec![
                "--bot-audio-wav".into(),
                target.display().to_string(),
                "--bot-audio-wav".into(),
                target.display().to_string(),
            ],
        ),
        (
            scenario.clone(),
            vec!["--bot-audio-wav".into(), existing.display().to_string()],
        ),
        (
            multi,
            vec!["--bot-audio-wav".into(), target.display().to_string()],
        ),
    ];
    let before = b.names();
    for (scenario, flags) in requests {
        let mut args = vec!["run".into(), scenario.display().to_string(), url.clone()];
        args.extend(flags);
        let out = cli(&args, &b.0, None).await.unwrap();
        assert_eq!(
            out.status.code(),
            Some(2),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
        assert!(out.stdout.is_empty());
        assert!(!out.stderr.is_empty());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err(),
            "invalid export connected"
        );
        assert_eq!(std::fs::read(&existing).unwrap(), COMPETITOR);
        assert!(!target.exists());
        assert_eq!(b.names(), before);
    }
    let out = cli(
        &[
            "suite".into(),
            scenario.display().to_string(),
            "--bot-audio-wav".into(),
            target.display().to_string(),
        ],
        &b.0,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert_eq!(b.names(), before);
}
