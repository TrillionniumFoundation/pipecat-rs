//! Direct two-sided timeline acceptance. All providers are synthetic loopback.
//! Host acknowledgements require the exact complete input, including wire tail
//! padding; the diagnostic WAV must retain source bytes without that padding.
use super::*;
use pipecat_evals::{
    EvalSession, RecordedAudio, ScenarioFile, SessionOptions, TimelineRecordingLimits,
    TimelineRecordingRun,
};
use std::collections::VecDeque;
use tokio::sync::Semaphore;

#[cfg(feature = "user-speech")]
#[allow(dead_code)] // Reuse the established bounded HTTP fixture, not its host.
#[path = "user_speech_common.rs"]
mod common;
#[cfg(feature = "user-speech")]
#[allow(dead_code)]
#[path = "user_speech.rs"]
mod speech_fixture;

const USER_RATE: u32 = 16_000;
#[cfg(feature = "user-speech")]
const KEY: &str = "PIPECAT_TIMELINE_FIXTURE_KEY";
fn source_pcm() -> Vec<u8> {
    [(111i16, 640), (222, 640), (333, 640), (444, 37)]
        .into_iter()
        .flat_map(|(value, count)| value.to_le_bytes().repeat(count))
        .collect()
}
fn wav_bytes(pcm: &[u8]) -> Vec<u8> {
    pipecat_audio::pcm_to_wav(pcm, USER_RATE, 1).unwrap()
}
fn packets(pcm: &[u8]) -> Vec<Vec<u8>> {
    pcm.chunks(1280)
        .map(|part| {
            let mut packet = vec![0; 1280];
            packet[..part.len()].copy_from_slice(part);
            packet
        })
        .collect()
}
fn expected(index: usize) -> Value {
    json!({"event":"llm_response","text_contains":format!("complete timeline PCM {index}"),"within_ms":3000})
}
fn source(clips: usize, fail: bool) -> Value {
    let turns = (0..clips)
        .map(|i| {
            let mut expect = vec![expected(i + 1)];
            if fail && i + 1 == clips {
                // Both deadlines retain the same turn anchor. The second
                // assertion is evaluated only after the complete-input ACK;
                // its 80ms deadline is not restarted after that ACK.
                expect.push(json!({"event":"llm_response","text_contains":"deliberately absent timeline response","within_ms":80}));
            }
            json!({"user":format!("reference {i}"),"audio":format!("clip-{i}.wav"),"expect":expect})
        })
        .collect::<Vec<_>>();
    json!({"name":"two-sided source timeline","user":{"modality":"audio"},"turns":turns})
}
fn recorded_case(b: &Bundle, clips: &[Vec<u8>], fail: bool) -> PathBuf {
    for (index, pcm) in clips.iter().enumerate() {
        b.put(&format!("clip-{index}.wav"), wav_bytes(pcm));
    }
    b.put(
        "timeline.json",
        serde_json::to_vec(&source(clips.len(), fail)).unwrap(),
    )
}
fn session(scenario: Value, url: &str, clips: &[Vec<u8>]) -> EvalSession {
    // The typed direct builder consumes already validated in-memory source PCM.
    let mut value = scenario;
    value.as_object_mut().unwrap().remove("user");
    for turn in value["turns"].as_array_mut().unwrap() {
        turn.as_object_mut().unwrap().remove("audio");
    }
    let scenario = ScenarioFile::from_value(value).unwrap().scenarios.remove(0);
    EvalSession::new(
        scenario,
        url,
        SessionOptions {
            stop_bot: true,
            default_timeout: Duration::from_secs(3),
            handshake_timeout: CLEANUP,
            max_duration: WAIT,
            transport: SessionConfig {
                queue_capacity: 64,
                connect_timeout: CLEANUP,
                write_timeout: Duration::from_millis(300),
                close_timeout: Duration::from_millis(100),
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap()
    .with_recorded_audio_inputs(
        clips
            .iter()
            .enumerate()
            .map(|(i, pcm)| (i, RecordedAudio::from_pcm16(pcm).unwrap())),
    )
    .unwrap()
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Behavior {
    Normal,
    Interrupt,
    CancelSecond,
    MixedRate,
    RaceFile,
    RaceDirectory,
    RaceSymlink,
    RejectHandshake,
}
struct InputState {
    remaining: VecDeque<VecDeque<Vec<u8>>>,
    clip: usize,
    packet: usize,
}
struct TimelineTrace {
    state: Mutex<InputState>,
    input: Mutex<Vec<Vec<u8>>>,
    errors: Mutex<Vec<String>>,
    settings: Mutex<Vec<bool>>,
    completed: AtomicUsize,
    text: AtomicUsize,
    cleaned: AtomicUsize,
    dropped: AtomicUsize,
    full_arrived: Notify,
    second_started: Notify,
    gate: Semaphore,
    gated: bool,
    interrupts: AtomicUsize,
}
struct TimelineProcessor {
    trace: Arc<TimelineTrace>,
    behavior: Behavior,
    output: PathBuf,
}
impl Drop for TimelineProcessor {
    fn drop(&mut self) {
        self.trace.dropped.fetch_add(1, Ordering::SeqCst);
    }
}
async fn output_pcm(
    ctx: &ProcessorContext,
    value: i16,
    frames: usize,
    rate: u32,
) -> Result<(), ProcessorError> {
    push(
        ctx,
        FrameData::OutputAudio(
            AudioRawFrame::new(value.to_le_bytes().repeat(frames), rate, 1).unwrap(),
        ),
    )
    .await
}
#[async_trait]
impl FrameProcessor for TimelineProcessor {
    fn name(&self) -> &str {
        "complete-source-timeline-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::InputAudio(audio) = &frame.data {
            self.trace.input.lock().unwrap().push(audio.audio.clone());
            if audio.audio.iter().any(|v| *v != 0) {
                let (clip, packet, last, valid) = {
                    let mut state = self.trace.state.lock().unwrap();
                    let clip = state.clip;
                    let packet = state.packet;
                    let (last, valid) = if let Some(next) = state.remaining.front_mut() {
                        let wanted = next.pop_front();
                        (
                            next.is_empty(),
                            wanted.as_ref() == Some(&audio.audio)
                                && audio.sample_rate == USER_RATE
                                && audio.num_channels == 1,
                        )
                    } else {
                        (false, false)
                    };
                    state.packet += 1;
                    if last {
                        state.remaining.pop_front();
                        state.clip += 1;
                        state.packet = 0;
                    }
                    (clip, packet, last, valid)
                };
                if !valid {
                    self.trace
                        .errors
                        .lock()
                        .unwrap()
                        .push("unexpected full PCM packet/order/rate/padding".into());
                }
                if clip == 0 && packet == 0 {
                    push(ctx, FrameData::BotStartedSpeaking).await?;
                    let frames =
                        if matches!(self.behavior, Behavior::Interrupt | Behavior::CancelSecond) {
                            64_000
                        } else {
                            640
                        };
                    output_pcm(ctx, 1000, frames, USER_RATE).await?;
                    pcm_fence(ctx).await?;
                }
                if clip == 0 && packet == 1 && self.behavior == Behavior::Interrupt {
                    push(
                        ctx,
                        FrameData::OutputTransportMessageUrgent(
                            json!({"label":"rtvi-ai","type":"bot-interrupted","data":{}}),
                        ),
                    )
                    .await?;
                    self.trace.interrupts.fetch_add(1, Ordering::SeqCst);
                }
                if clip == 1 && packet == 0 {
                    // The second clip follows consumption of the first turn's
                    // expected event and completion of its source playback.
                    self.trace.second_started.notify_one();
                    if self.behavior == Behavior::MixedRate {
                        output_pcm(ctx, 2000, 480, 48_000).await?;
                    } else if self.behavior == Behavior::Interrupt {
                        // The client has already consumed bot_interrupted.
                        // PCM2 and its later complete-input ACK use the same
                        // observed-event queue. No cross-queue order is assumed.
                        output_pcm(ctx, 2000, 160, USER_RATE).await?;
                        pcm_fence(ctx).await?;
                    }
                }
                if last && valid {
                    self.trace.full_arrived.notify_one();
                    if self.trace.gated {
                        let permit = self
                            .trace
                            .gate
                            .acquire()
                            .await
                            .map_err(|_| ProcessorError::fatal("timeline gate closed"))?;
                        permit.forget();
                    }
                    if self.behavior == Behavior::RaceFile {
                        std::fs::write(&self.output, COMPETITOR)
                            .map_err(|e| ProcessorError::fatal(e.to_string()))?;
                    } else if self.behavior == Behavior::RaceDirectory {
                        std::fs::create_dir(&self.output)
                            .map_err(|e| ProcessorError::fatal(e.to_string()))?;
                    } else if self.behavior == Behavior::RaceSymlink {
                        let victim = self.output.with_extension("victim");
                        std::fs::write(&victim, COMPETITOR)
                            .map_err(|e| ProcessorError::fatal(e.to_string()))?;
                        std::os::unix::fs::symlink(&victim, &self.output)
                            .map_err(|e| ProcessorError::fatal(e.to_string()))?;
                    }
                    self.trace.completed.fetch_add(1, Ordering::SeqCst);
                    if self.behavior != Behavior::CancelSecond || clip == 0 {
                        push(ctx, FrameData::BotStoppedSpeaking).await?;
                        for data in [
                            FrameData::LlmResponseStart,
                            FrameData::LlmText(TextFrame::new(format!(
                                "complete timeline PCM {}",
                                clip + 1
                            ))),
                            FrameData::LlmResponseEnd,
                        ] {
                            push(ctx, data).await?;
                        }
                    }
                }
            }
        } else if matches!(
            frame.data,
            FrameData::Text(_) | FrameData::LlmMessagesAppend(_)
        ) {
            self.trace.text.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.trace.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct TimelineFactory {
    trace: Arc<TimelineTrace>,
    behavior: Behavior,
    output: PathBuf,
}
#[async_trait]
impl EvalBotFactory for TimelineFactory {
    async fn create(&self, settings: EvalBotSettings) -> Result<EvalBotPipeline, BotHostError> {
        self.trace.settings.lock().unwrap().push(settings.skip_tts);
        if self.behavior == Behavior::RejectHandshake {
            return Err(BotHostError::Invalid(
                "synthetic handshake rejection".into(),
            ));
        }
        Ok(EvalBotPipeline {
            context: SharedContext::default(),
            processors: vec![Arc::new(TimelineProcessor {
                trace: self.trace.clone(),
                behavior: self.behavior,
                output: self.output.clone(),
            })],
            assistant: Arc::new(IdentityProcessor),
            introduction: None,
        })
    }
}
struct TimelineHost {
    url: String,
    trace: Arc<TimelineTrace>,
    cancel: Cancellation,
    owner: JoinHandle<Result<EvalBotHostReport, BotHostError>>,
}
impl TimelineHost {
    async fn start(clips: &[Vec<u8>], behavior: Behavior, output: &Path, gated: bool) -> Self {
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
                host_timeout: Duration::from_secs(20),
                shutdown_timeout: CLEANUP,
                ..Default::default()
            },
        )
        .await
        .unwrap()
        .with_audio_output(true)
        .with_raw_audio_input(true);
        let url = format!("ws://{}/", host.local_addr().unwrap());
        let trace = Arc::new(TimelineTrace {
            state: Mutex::new(InputState {
                remaining: clips.iter().map(|c| packets(c).into()).collect(),
                clip: 0,
                packet: 0,
            }),
            input: Mutex::new(Vec::new()),
            errors: Mutex::new(Vec::new()),
            settings: Mutex::new(Vec::new()),
            completed: AtomicUsize::new(0),
            text: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
            dropped: AtomicUsize::new(0),
            full_arrived: Notify::new(),
            second_started: Notify::new(),
            gate: Semaphore::new(0),
            gated,
            interrupts: AtomicUsize::new(0),
        });
        let factory = TimelineFactory {
            trace: trace.clone(),
            behavior,
            output: output.into(),
        };
        let cancel = Cancellation::new();
        let c = cancel.clone();
        let owner = tokio::spawn(async move { host.serve(&factory, c).await });
        Self {
            url,
            trace,
            cancel,
            owner,
        }
    }
    async fn finish(
        mut self,
    ) -> (
        Result<EvalBotHostReport, BotHostError>,
        Arc<TimelineTrace>,
        bool,
    ) {
        self.trace.gate.add_permits(16);
        let (joined, late) = match tokio::time::timeout(WAIT, &mut self.owner).await {
            Ok(r) => (r, false),
            Err(_) => {
                self.cancel.cancel();
                (self.owner.await, true)
            }
        };
        (
            joined.expect("timeline host task panicked after actual join"),
            self.trace,
            late,
        )
    }
}
fn checked_host(
    report: Result<EvalBotHostReport, BotHostError>,
    trace: &TimelineTrace,
    late: bool,
    clips: &[Vec<u8>],
) {
    assert!(!late, "host deadline after actual join: {report:?}");
    let report = report.unwrap();
    assert_eq!(report.sessions, 1);
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
    assert_eq!(*trace.settings.lock().unwrap(), [false]);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(trace.text.load(Ordering::SeqCst), 0);
    assert!(trace.errors.lock().unwrap().is_empty());
    assert_eq!(trace.completed.load(Ordering::SeqCst), clips.len());
    let active = trace
        .input
        .lock()
        .unwrap()
        .iter()
        .filter(|p| p.iter().any(|b| *b != 0))
        .cloned()
        .collect::<Vec<_>>();
    assert_eq!(
        active,
        clips.iter().flat_map(|c| packets(c)).collect::<Vec<_>>()
    );
}
async fn joined_run(
    mut owner: JoinHandle<Result<TimelineRecordingRun, EvalError>>,
    cancel: &Cancellation,
) -> Result<TimelineRecordingRun, Error> {
    match tokio::time::timeout(WAIT, &mut owner).await {
        Ok(result) => Ok(result??),
        Err(_) => {
            cancel.cancel();
            let actual = owner.await;
            Err(format!("timeline deadline after actual join: {actual:?}").into())
        }
    }
}
fn recording_args(scenario: &Path, url: &str, target: &Path) -> Vec<String> {
    vec![
        "run".into(),
        scenario.display().to_string(),
        url.into(),
        "--recorded-audio".into(),
        "--recording-wav".into(),
        target.display().to_string(),
        "--stop-bot".into(),
    ]
}
// All complete-source cases use this exact synthetic PCM. The user's first
// enqueue precedes the host's first bot PCM; the first source starts at index 0.
// Later source clips may have a real >200ms layout gap (floor: >=3200 frames).
// Bot values are synthetic, positive, and cannot saturate these source values.
fn check_complete_source_samples(
    samples: &[i16],
    clips: usize,
    interrupted: bool,
) -> Result<(), &'static str> {
    if !matches!(clips, 1..=3) || (interrupted && !matches!(clips, 1 | 3)) {
        return Err("unsupported complete-source shape");
    }
    if samples.len() >= 16_000 {
        return Err("future bot tail was retained");
    }
    let source = source_pcm();
    let expected = source
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| i16::from_le_bytes(*bytes))
        .collect::<Vec<_>>();
    let allowed = |delta| delta == 0 || delta == 1000 || (interrupted && delta == 2000);
    let mut bot = samples
        .iter()
        .map(|sample| i32::from(*sample))
        .collect::<Vec<_>>();
    let mut source_end = 0;
    for clip in 0..clips {
        let start = if clip == 0 {
            0
        } else {
            let offset = samples[source_end..]
                .iter()
                .position(|sample| sample.rem_euclid(1000) != 0)
                .ok_or("next source missing")?;
            if offset != 0 && offset < 3200 {
                return Err("subthreshold source gap or wire padding");
            }
            source_end + offset
        };
        let end = start + expected.len();
        let actual = samples.get(start..end).ok_or("complete source truncated")?;
        // A residue only locates a candidate. Validate every source sample,
        // including the entire 111x640 opening and the final 37x444 separately.
        for (index, (sample, source)) in actual[..1920].iter().zip(&expected[..1920]).enumerate() {
            let delta = i32::from(*sample) - i32::from(*source);
            if !allowed(delta) {
                return Err("source body differs");
            }
            bot[start + index] = delta;
        }
        for (index, sample) in actual[1920..].iter().enumerate() {
            let delta = i32::from(*sample) - 444;
            if !allowed(delta) {
                return Err("source tail differs");
            }
            bot[start + 1920 + index] = delta;
        }
        source_end = end;
    }
    if bot.iter().any(|delta| !allowed(*delta)) {
        return Err("unattributed sample outside source");
    }
    let interval = |value| -> Result<Option<(usize, usize)>, &'static str> {
        let Some(start) = bot.iter().position(|sample| *sample == value) else {
            return Ok(None);
        };
        let end = bot.iter().rposition(|sample| *sample == value).unwrap() + 1;
        if bot[start..end].iter().any(|sample| *sample != value) {
            return Err("bot contribution is discontinuous");
        }
        Ok(Some((start, end)))
    };
    let old = interval(1000)?;
    let new = interval(2000)?;
    if interrupted {
        let (new_start, new_end) = new.ok_or("post-interruption bot missing")?;
        if new_end - new_start > 160 {
            return Err("post-interruption bot exceeds source");
        }
        if clips == 3 && new_end - new_start != 160 {
            return Err("post-interruption bot is not fully retained");
        }
        if old.is_some_and(|(_, end)| end > new_start) {
            return Err("bot generations out of order");
        }
        // The original source is 4s/64000 frames. The strict total <16000
        // bound above retains the original future-tail cropping witness.
    } else {
        let (start, end) = old.ok_or("normal bot missing")?;
        // A real close may crop pending virtual playout. This is one positive
        // continuous prefix, never an internal hole or more than its 640 frames.
        if end - start > 640 {
            return Err("normal bot exceeds source");
        }
    }
    let bot_end = old
        .map_or(0, |(_, end)| end)
        .max(new.map_or(0, |(_, end)| end));
    if samples.len() != source_end.max(bot_end) {
        return Err("unattributed trailing samples");
    }
    Ok(())
}
fn checked_ack_then_expected_timeout(run: &ScriptResult) {
    assert_eq!(run.turns.len(), 1);
    let expectations = &run.turns[0].expectations;
    assert_eq!(expectations.len(), 2);
    assert_eq!(expectations[0].expectation_index, 0);
    assert!(expectations[0].passed);
    assert_eq!(expectations[0].event_name, "llm_response");
    assert!(expectations[0].matched.contains("complete timeline PCM 1"));
    assert_eq!(expectations[1].expectation_index, 1);
    assert!(!expectations[1].passed);
    assert_eq!(expectations[1].event_name, "llm_response");
    assert_eq!(run.failures.len(), 1);
    assert_eq!(run.failures[0].turn_index, 0);
    assert_eq!(run.failures[0].expectation_index, 1);
    assert_eq!(run.failures[0].kind, "timeout");
}
fn checked_wav(bytes: &[u8], require_bot: bool, complete: Option<(usize, bool)>) {
    let wav = read_pcm16_wav(bytes).unwrap();
    assert_eq!(wav.format.sample_rate(), USER_RATE);
    assert_eq!(wav.format.channels(), 1);
    assert!(wav.pcm.len() < 32_000, "future bot tail was retained");
    let samples = wav
        .pcm
        .as_chunks::<2>()
        .0
        .iter()
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect::<Vec<_>>();
    assert!(samples.iter().any(|v| *v != 0));
    if require_bot {
        assert!(
            samples.iter().any(|v| *v >= 1000),
            "actual bot PCM missing from mixed output"
        );
    }
    if let Some((clips, interrupted)) = complete {
        assert_eq!(
            check_complete_source_samples(&samples, clips, interrupted),
            Ok(())
        );
    }
}
fn no_temp(b: &Bundle) {
    assert!(b
        .names()
        .iter()
        .all(|name| !name.to_string_lossy().starts_with(".pipecat-bot-audio-")));
}

#[test]
fn complete_source_checks_reject_padding_loss_and_illegal_bot_contributions() {
    let raw = source_pcm();
    let source = raw
        .as_chunks::<2>()
        .0
        .iter()
        .map(|bytes| i16::from_le_bytes(*bytes))
        .collect::<Vec<_>>();
    let mut mixed = source.clone();
    for sample in &mut mixed[1335..] {
        *sample += 1000;
    }
    mixed.extend([1000; 18]);
    assert_eq!(check_complete_source_samples(&mixed, 1, false), Ok(()));
    let mut partial_bot = source.clone();
    for sample in &mut partial_bot[1335..1385] {
        *sample += 1000;
    }
    assert_eq!(
        check_complete_source_samples(&partial_bot, 1, false),
        Ok(())
    );
    let mut padded = mixed.clone();
    padded.extend([0; 603]);
    assert_eq!(
        check_complete_source_samples(&padded, 1, false),
        Err("unattributed trailing samples")
    );
    let mut missing_tail = mixed.clone();
    drop(missing_tail.drain(1920..1957));
    assert_eq!(
        check_complete_source_samples(&missing_tail, 1, false),
        Err("complete source truncated")
    );
    let mut body_error = mixed.clone();
    body_error[17] += 1;
    assert_eq!(
        check_complete_source_samples(&body_error, 1, false),
        Err("source body differs")
    );
    let mut tail_error = mixed.clone();
    tail_error[1940] += 1;
    assert_eq!(
        check_complete_source_samples(&tail_error, 1, false),
        Err("source tail differs")
    );
    let mut split_bot = mixed.clone();
    split_bot[1500] -= 1000;
    assert_eq!(
        check_complete_source_samples(&split_bot, 1, false),
        Err("bot contribution is discontinuous")
    );
    let mut excess_bot = source.clone();
    for sample in &mut excess_bot[..641] {
        *sample += 1000;
    }
    assert_eq!(
        check_complete_source_samples(&excess_bot, 1, false),
        Err("normal bot exceeds source")
    );
    let mut two = partial_bot.clone();
    two.extend(&source);
    assert_eq!(check_complete_source_samples(&two, 2, false), Ok(()));
    let mut padded_between = partial_bot.clone();
    padded_between.extend([0; 603]);
    padded_between.extend(&source);
    assert_eq!(
        check_complete_source_samples(&padded_between, 2, false),
        Err("subthreshold source gap or wire padding")
    );
    let mut real_gap = partial_bot;
    real_gap.extend([0; 3200]);
    real_gap.extend(&source);
    assert_eq!(check_complete_source_samples(&real_gap, 2, false), Ok(()));
    let mut interrupted = source;
    for sample in &mut interrupted[100..500] {
        *sample += 1000;
    }
    for sample in &mut interrupted[500..660] {
        *sample += 2000;
    }
    assert_eq!(check_complete_source_samples(&interrupted, 1, true), Ok(()));
    let mut three = interrupted.clone();
    for _ in 0..2 {
        three.extend(
            raw.as_chunks::<2>()
                .0
                .iter()
                .map(|bytes| i16::from_le_bytes(*bytes)),
        );
    }
    assert_eq!(check_complete_source_samples(&three, 3, true), Ok(()));
    three[659] -= 2000;
    assert_eq!(
        check_complete_source_samples(&three, 3, true),
        Err("post-interruption bot is not fully retained")
    );
    interrupted[600] -= 2000;
    assert_eq!(
        check_complete_source_samples(&interrupted, 1, true),
        Err("bot contribution is discontinuous")
    );
}

#[tokio::test]
async fn direct_library_timeline_requires_complete_native_input_and_mixes_real_bot_pcm() {
    let b = Bundle::new();
    let pcm = source_pcm();
    let clips = vec![pcm];
    let host = TimelineHost::start(&clips, Behavior::Normal, &b.0.join("unused.wav"), true).await;
    let session = session(source(1, false), &host.url, &clips);
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let owner = tokio::spawn(async move {
        session
            .run_with_timeline_recording(c, TimelineRecordingLimits::default())
            .await
    });
    let arrived = tokio::time::timeout(WAIT, host.trace.full_arrived.notified())
        .await
        .is_ok();
    let returned_before_ack = owner.is_finished();
    let completed_before_ack = host.trace.completed.load(Ordering::SeqCst);
    host.trace.gate.add_permits(1);
    let run = joined_run(owner, &cancel).await;
    let (report, trace, late) = host.finish().await;
    checked_host(report, &trace, late, &clips);
    assert!(arrived);
    assert!(!returned_before_ack);
    assert_eq!(completed_before_ack, 0);
    let run = run.unwrap();
    assert!(run.result.passed, "{:?}", run.result);
    checked_wav(&run.to_wav().unwrap().unwrap(), true, Some((1, false)));
}

#[tokio::test]
async fn actual_cli_timeline_records_both_sides_and_failure_prefix_without_wire_padding() {
    for failed in [false, true] {
        let b = Bundle::new();
        let clips = vec![source_pcm()];
        let scenario = recorded_case(&b, &clips, failed);
        let target = b.0.join("timeline.wav");
        let host = TimelineHost::start(&clips, Behavior::Normal, &target, false).await;
        let out = cli(&recording_args(&scenario, &host.url, &target), &b.0, None).await;
        let (report, trace, late) = host.finish().await;
        checked_host(report, &trace, late, &clips);
        let out = out.unwrap();
        let run = result(&out, if failed { 1 } else { 0 }, !failed);
        if failed {
            checked_ack_then_expected_timeout(&run);
        }
        checked_wav(&std::fs::read(&target).unwrap(), true, Some((1, false)));
        assert_eq!(
            std::fs::metadata(&target).unwrap().permissions().mode() & 0o777,
            0o600
        );
        no_temp(&b);
    }
}

#[tokio::test]
async fn actual_cli_timeline_interruption_crops_only_future_bot_audio_and_keeps_full_input() {
    let b = Bundle::new();
    let clips = vec![source_pcm(), source_pcm(), source_pcm()];
    for (index, pcm) in clips.iter().enumerate() {
        b.put(&format!("clip-{index}.wav"), wav_bytes(pcm));
    }
    let mut value = source(3, false);
    value["turns"][0]["expect"] = json!([{"event":"bot_interrupted","within_ms":3000}]);
    let scenario = b.put("timeline.json", serde_json::to_vec(&value).unwrap());
    let target = b.0.join("timeline.wav");
    let host = TimelineHost::start(&clips, Behavior::Interrupt, &target, false).await;
    let out = cli(&recording_args(&scenario, &host.url, &target), &b.0, None).await;
    let (report, trace, late) = host.finish().await;
    checked_host(report, &trace, late, &clips);
    assert_eq!(trace.interrupts.load(Ordering::SeqCst), 1);
    let run = result(&out.unwrap(), 0, true);
    assert_eq!(run.turns.len(), 3);
    for (index, turn) in run.turns.iter().enumerate() {
        assert_eq!(turn.expectations.len(), 1);
        let expectation = &turn.expectations[0];
        assert!(expectation.passed);
        assert_eq!(expectation.expectation_index, 0);
        if index == 0 {
            assert_eq!(expectation.event_name, "bot_interrupted");
        } else {
            assert_eq!(expectation.event_name, "llm_response");
            assert!(expectation
                .matched
                .contains(&format!("complete timeline PCM {}", index + 1)));
        }
    }
    // ACK2 is consumed after new PCM on the same observed-event queue. Only
    // then can clip3 be enqueued; its later non-burst40ms packets and ACK3
    // advance real time beyond the new10ms source before the final cutoff.
    let bytes = std::fs::read(&target).unwrap();
    checked_wav(&bytes, true, Some((3, true)));
    let wav = read_pcm16_wav(&bytes).unwrap();
    assert!(
        wav.pcm
            .as_chunks::<2>()
            .0
            .iter()
            .any(|b| i16::from_le_bytes([b[0], b[1]]) >= 2000),
        "new post-interruption bot generation missing"
    );
    no_temp(&b);
}

#[tokio::test]
async fn actual_cli_timeline_sigint_after_peer_barrier_keeps_prefix_and_joins() {
    let b = Bundle::new();
    let clips = vec![source_pcm(), 555i16.to_le_bytes().repeat(160_000)];
    let scenario = recorded_case(&b, &clips, false);
    let target = b.0.join("timeline.wav");
    let host = TimelineHost::start(&clips, Behavior::CancelSecond, &target, false).await;
    let out = cli(
        &recording_args(&scenario, &host.url, &target),
        &b.0,
        Some(&host.trace.second_started),
    )
    .await;
    let (report, trace, late) = host.finish().await;
    assert!(!late, "{report:?}");
    let report = report.unwrap();
    assert_eq!(report.last_exit, Some(EvalBotExit::EvalCancel));
    assert!(trace.errors.lock().unwrap().is_empty());
    assert_eq!(trace.completed.load(Ordering::SeqCst), 1);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    let out = out.unwrap();
    let r = result(&out, 1, false);
    assert!(r.failures.iter().any(|f| f.kind == "cancelled"));
    checked_wav(&std::fs::read(&target).unwrap(), true, None);
    no_temp(&b);
}

#[tokio::test]
async fn direct_pre_cancelled_recording_is_empty_and_invalid_limits_do_not_connect() {
    let b = Bundle::new();
    let clips = vec![source_pcm()];
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let cancel = Cancellation::new();
    cancel.cancel();
    let run = session(source(1, false), &url, &clips)
        .run_with_timeline_recording(cancel, TimelineRecordingLimits::default())
        .await
        .unwrap();
    assert!(!run.result.passed);
    assert!(run.to_wav().unwrap().is_none());
    let bad = TimelineRecordingLimits {
        max_raw_bytes: 0,
        ..Default::default()
    };
    let invalid = session(source(1, false), &url, &clips)
        .run_with_timeline_recording(Cancellation::new(), bad)
        .await;
    assert!(invalid.is_err());
    let mut dtmf = source(2, false);
    dtmf["turns"][1] = json!({"dtmf":"1"});
    let unsupported = session(dtmf, &url, &clips)
        .run_with_timeline_recording(Cancellation::new(), TimelineRecordingLimits::default())
        .await;
    assert!(
        matches!(unsupported, Err(EvalError::Unsupported(message)) if message.contains("DTMF"))
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    no_temp(&b);
}

#[tokio::test]
async fn actual_cli_timeline_handshake_failure_has_empty_recording_and_no_output_file() {
    let b = Bundle::new();
    let clips = vec![source_pcm()];
    let scenario = recorded_case(&b, &clips, false);
    let target = b.0.join("timeline.wav");
    let host = TimelineHost::start(&clips, Behavior::RejectHandshake, &target, false).await;
    let out = cli(&recording_args(&scenario, &host.url, &target), &b.0, None).await;
    let (_report, trace, late) = host.finish().await;
    assert!(!late);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 0);
    assert!(trace.input.lock().unwrap().is_empty());
    let out = out.unwrap();
    result(&out, 1, false);
    assert!(String::from_utf8_lossy(&out.stderr).contains("empty timeline recording"));
    assert!(!target.exists());
    no_temp(&b);
}

#[tokio::test]
async fn actual_cli_timeline_mixed_bot_rate_rejects_late_chunk_and_preserves_valid_prefix() {
    let b = Bundle::new();
    let clips = vec![source_pcm(), source_pcm()];
    let scenario = recorded_case(&b, &clips, false);
    let target = b.0.join("timeline.wav");
    let host = TimelineHost::start(&clips, Behavior::MixedRate, &target, false).await;
    let out = cli(&recording_args(&scenario, &host.url, &target), &b.0, None).await;
    let (report, trace, late) = host.finish().await;
    assert!(!late, "{report:?}");
    assert!(report.is_ok());
    assert!(trace.errors.lock().unwrap().is_empty());
    assert!(trace.completed.load(Ordering::SeqCst) >= 1);
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    let out = out.unwrap();
    let run = result(&out, 1, false);
    assert!(
        run.failures
            .iter()
            .any(|failure| failure.reason.contains("sample rate changed")),
        "{run:?}"
    );
    let bytes = std::fs::read(&target).unwrap();
    checked_wav(&bytes, true, None);
    let wav = read_pcm16_wav(&bytes).unwrap();
    assert!(
        wav.pcm
            .as_chunks::<2>()
            .0
            .iter()
            .all(|sample| i16::from_le_bytes([sample[0], sample[1]]) < 2000),
        "rejected48k PCM leaked into valid prefix"
    );
    no_temp(&b);
}

#[tokio::test]
async fn actual_cli_timeline_races_file_directory_and_symlink_without_clobber_or_json_loss() {
    for mode in [
        Behavior::RaceFile,
        Behavior::RaceDirectory,
        Behavior::RaceSymlink,
    ] {
        let b = Bundle::new();
        let clips = vec![source_pcm()];
        let scenario = recorded_case(&b, &clips, true);
        let target = b.0.join("timeline.wav");
        let host = TimelineHost::start(&clips, mode, &target, false).await;
        let out = cli(&recording_args(&scenario, &host.url, &target), &b.0, None).await;
        let (report, trace, late) = host.finish().await;
        checked_host(report, &trace, late, &clips);
        let out = out.unwrap();
        let run = result(&out, 2, false);
        checked_ack_then_expected_timeout(&run);
        match mode {
            Behavior::RaceDirectory => assert!(target.is_dir()),
            Behavior::RaceSymlink => {
                assert!(target.is_symlink());
                assert_eq!(
                    std::fs::read(target.with_extension("victim")).unwrap(),
                    COMPETITOR
                );
            }
            _ => assert_eq!(std::fs::read(&target).unwrap(), COMPETITOR),
        }
        no_temp(&b);
    }
}

#[tokio::test]
async fn recording_rejects_unsupported_cli_compositions_before_external_actions() {
    let b = Bundle::new();
    let clips = vec![source_pcm()];
    let scenario = recorded_case(&b, &clips, false);
    let target = b.0.join("timeline.wav");
    let forbidden_config = b.put("never-read.json", b"invalid runtime JSON must not be read");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    let existing = b.put("existing.wav", COMPETITOR);
    let link = b.0.join("linked.wav");
    std::os::unix::fs::symlink(&existing, &link).unwrap();
    let before = b.names();
    let variants = [
        vec![
            "--bot-audio-response".into(),
            forbidden_config.display().to_string(),
        ],
        vec![
            "--user-speech".into(),
            forbidden_config.display().to_string(),
            "--bot-audio-response".into(),
            forbidden_config.display().to_string(),
        ],
        vec!["--user-images".into()],
        vec![
            "--bot-audio-wav".into(),
            b.0.join("other.wav").display().to_string(),
        ],
        vec!["--recording-wav".into(), target.display().to_string()],
    ];
    for extra in variants {
        let duplicate = extra.first().is_some_and(|flag| flag == "--recording-wav");
        let mut args = recording_args(&scenario, &url, &target);
        args.extend(extra);
        let out = cli(&args, &b.0, None).await.unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert!(
            !String::from_utf8_lossy(&out.stderr).contains("invalid runtime JSON must not be read")
        );
        let stderr = String::from_utf8_lossy(&out.stderr);
        assert!(stderr.contains(if duplicate {
            "unknown or duplicate run flag"
        } else {
            "--recording-wav"
        }));
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
        assert_eq!(b.names(), before);
    }
    #[cfg(not(feature = "user-speech"))]
    {
        let out = cli(
            &[
                "run".into(),
                scenario.display().to_string(),
                url.clone(),
                "--user-speech".into(),
                forbidden_config.display().to_string(),
                "--recording-wav".into(),
                target.display().to_string(),
            ],
            &b.0,
            None,
        )
        .await
        .unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert!(String::from_utf8_lossy(&out.stderr).contains("user-speech build feature"));
        assert_eq!(b.names(), before);
    }
    for flags in [
        vec!["--recorded-audio", "--recording-wav"],
        vec!["--recording-wav", "absent.wav"],
    ] {
        let mut args = vec!["run".into(), scenario.display().to_string(), url.clone()];
        args.extend(flags.into_iter().map(str::to_owned));
        let out = cli(&args, &b.0, None).await.unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
        assert_eq!(b.names(), before);
    }
    for destination in [&existing, &link] {
        let out = cli(&recording_args(&scenario, &url, destination), &b.0, None)
            .await
            .unwrap();
        assert_eq!(out.status.code(), Some(2));
        assert!(out.stdout.is_empty());
    }
    let mut multi = source(1, false);
    multi["scenarios"] = json!([{"name":"first"},{"name":"second"}]);
    let multi = b.put("multiple.json", serde_json::to_vec(&multi).unwrap());
    let out = cli(&recording_args(&multi, &url, &target), &b.0, None)
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(String::from_utf8_lossy(&out.stderr).contains("exactly one scenario"));
    let dtmf = b.put(
        "dtmf.json",
        serde_json::to_vec(&json!({"name":"DTMF rejected", "turns":[{"dtmf":"1"}]})).unwrap(),
    );
    let out = cli(&recording_args(&dtmf, &url, &target), &b.0, None)
        .await
        .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    let out = cli(
        &[
            "suite".into(),
            scenario.display().to_string(),
            "--recording-wav".into(),
            target.display().to_string(),
        ],
        &b.0,
        None,
    )
    .await
    .unwrap();
    assert_eq!(out.status.code(), Some(2));
    assert!(out.stdout.is_empty());
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
    assert_eq!(std::fs::read(&existing).unwrap(), COMPETITOR);
    assert!(!target.exists());
    no_temp(&b);
}

#[tokio::test]
async fn direct_library_cancel_after_actual_second_input_preserves_diagnostic_prefix() {
    let b = Bundle::new();
    let clips = vec![source_pcm(), 555i16.to_le_bytes().repeat(160_000)];
    let host = TimelineHost::start(
        &clips,
        Behavior::CancelSecond,
        &b.0.join("unused.wav"),
        false,
    )
    .await;
    let configured = session(source(2, false), &host.url, &clips);
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let owner = tokio::spawn(async move {
        configured
            .run_with_timeline_recording(c, TimelineRecordingLimits::default())
            .await
    });
    let barrier = tokio::time::timeout(WAIT, host.trace.second_started.notified())
        .await
        .is_ok();
    cancel.cancel();
    let run = joined_run(owner, &cancel).await;
    let (report, trace, late) = host.finish().await;
    assert!(barrier);
    assert!(!late, "{report:?}");
    assert!(report.is_ok());
    assert_eq!(trace.completed.load(Ordering::SeqCst), 1);
    assert!(trace.errors.lock().unwrap().is_empty());
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    let run = run.unwrap();
    assert!(!run.result.passed);
    assert!(run.result.failures.iter().any(|f| f.kind == "cancelled"));
    checked_wav(&run.to_wav().unwrap().unwrap(), true, None);
}

#[cfg(feature = "user-speech")]
fn speech_settings(base: &str, cache: Option<&Path>) -> pipecat_evals::CartesiaUserSpeechSettings {
    use pipecat_evals::UserSpeechCachePolicy;
    assert!(base.starts_with("http://127.0.0.1:"));
    let mut config = pipecat_speech::SpeechConfig::cartesia(common::SECRET);
    config.base_url = base.into();
    config.timeout = CLEANUP;
    pipecat_evals::CartesiaUserSpeechSettings::new(
        config,
        cache.map_or(UserSpeechCachePolicy::Disabled, |directory| {
            UserSpeechCachePolicy::Private {
                directory: directory.into(),
                max_entries: 8,
                max_bytes: 2 * 1024 * 1024,
            }
        }),
    )
    .unwrap()
}
#[cfg(feature = "user-speech")]
fn speech_source() -> Value {
    let mut value = source(2, false);
    value["user"]["speech"] = json!({"service":"cartesia","voice":"Fixture-VOICE"});
    value["turns"][0].as_object_mut().unwrap().remove("audio");
    value["turns"][0]["user"] = common::REFERENCE.into();
    value
}
#[cfg(feature = "user-speech")]
#[tokio::test]
async fn speech_direct_library_records_loopback_synthesis_and_recorded_override_with_real_bot_pcm()
{
    let b = Bundle::new();
    let clips = vec![source_pcm(), source_pcm()];
    let http =
        speech_fixture::Http::start(vec![speech_fixture::Reply::pcm(clips[0].clone())]).await;
    let host = TimelineHost::start(&clips, Behavior::Normal, &b.0.join("unused.wav"), false).await;
    let mut value = speech_source();
    value.as_object_mut().unwrap().remove("user");
    for turn in value["turns"].as_array_mut().unwrap() {
        turn.as_object_mut().unwrap().remove("audio");
    }
    let script = ScenarioFile::from_value(value).unwrap().scenarios.remove(0);
    let configured = EvalSession::new(
        script,
        &host.url,
        SessionOptions {
            stop_bot: true,
            default_timeout: CLEANUP,
            handshake_timeout: CLEANUP,
            max_duration: WAIT,
            ..Default::default()
        },
    )
    .unwrap()
    .with_recorded_audio_inputs([(1, RecordedAudio::from_pcm16(&clips[1]).unwrap())])
    .unwrap()
    .with_cartesia_user_speech(
        speech_settings(&http.url, None),
        pipecat_evals::CartesiaUserSpeechVoice::new("Fixture-VOICE", None, None).unwrap(),
    )
    .unwrap();
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let run = joined_run(
        tokio::spawn(async move {
            configured
                .run_with_timeline_recording(c, TimelineRecordingLimits::default())
                .await
        }),
        &cancel,
    )
    .await;
    let (report, trace, late) = host.finish().await;
    let http_trace = http.finish().await;
    checked_host(report, &trace, late, &clips);
    assert_eq!(http_trace.active.load(Ordering::SeqCst), 0);
    assert_eq!(http_trace.joined.load(Ordering::SeqCst), 1);
    let requests = http_trace.requests.lock().unwrap();
    assert_eq!(requests.len(), 1);
    speech_fixture::assert_request(&requests[0], common::REFERENCE);
    let run = run.unwrap();
    assert!(run.result.passed, "{:?}", run.result);
    speech_fixture::assert_redacted(&run.result);
    checked_wav(&run.to_wav().unwrap().unwrap(), true, Some((2, false)));
}

#[cfg(feature = "user-speech")]
#[tokio::test]
async fn speech_actual_cli_cold_warm_cache_and_wav_override_keep_complete_two_sided_recording() {
    let b = Bundle::new();
    let clips = vec![source_pcm(), source_pcm()];
    let cache = b.0.join("cache");
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&cache)
            .unwrap();
    }
    b.put("clip-1.wav", wav_bytes(&clips[1]));
    let scenario = b.put("speech.json", serde_json::to_vec(&speech_source()).unwrap());
    let http =
        speech_fixture::Http::start(vec![speech_fixture::Reply::pcm(clips[0].clone())]).await;
    let runtime = b.put("speech-runtime.json", serde_json::to_vec(&json!({"provider":"cartesia","base_url":http.url,"api_key_env":KEY,"request_timeout_ms":2000,"cache_dir":cache})).unwrap());
    let mut http = Some(http);
    for label in ["cold", "warm"] {
        let target = b.0.join(format!("{label}.wav"));
        let host = TimelineHost::start(&clips, Behavior::Normal, &target, false).await;
        let args = vec![
            "run".into(),
            scenario.display().to_string(),
            host.url.clone(),
            "--user-speech".into(),
            runtime.display().to_string(),
            "--recording-wav".into(),
            target.display().to_string(),
            "--stop-bot".into(),
        ];
        let out = cli_env(&args, &b.0, None, Some((KEY, common::SECRET))).await;
        let (report, trace, late) = host.finish().await;
        // Stop and join HTTP after cold before asserting or starting warm: a
        // warm pass cannot have contacted the now-closed provider endpoint.
        let requests = if let Some(server) = http.take() {
            Some(server.finish().await)
        } else {
            None
        };
        checked_host(report, &trace, late, &clips);
        let out = out.unwrap();
        result(&out, 0, true);
        assert!(!String::from_utf8_lossy(&out.stdout).contains(common::SECRET));
        assert!(!String::from_utf8_lossy(&out.stderr).contains(common::SECRET));
        checked_wav(&std::fs::read(&target).unwrap(), true, Some((2, false)));
        no_temp(&b);
        if let Some(trace) = requests {
            assert_eq!(trace.active.load(Ordering::SeqCst), 0);
            assert_eq!(trace.joined.load(Ordering::SeqCst), 1);
            let requests = trace.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            speech_fixture::assert_request(&requests[0], common::REFERENCE);
        }
        speech_fixture::assert_private_cache(&cache, &clips[0]);
    }
}

#[cfg(feature = "user-speech")]
#[tokio::test]
async fn speech_cancel_while_http_source_is_held_joins_and_does_not_record_uncommitted_pcm() {
    let b = Bundle::new();
    let clips = vec![source_pcm()];
    let cache = b.0.join("cache");
    {
        use std::os::unix::fs::DirBuilderExt;
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(&cache)
            .unwrap();
    }
    let mut reply = speech_fixture::Reply::pcm(clips[0].clone());
    reply.stalled = true;
    let http = speech_fixture::Http::start(vec![reply]).await;
    let host = TimelineHost::start(&clips, Behavior::Normal, &b.0.join("unused.wav"), false).await;
    let mut value = source(1, false);
    value.as_object_mut().unwrap().remove("user");
    value["turns"][0].as_object_mut().unwrap().remove("audio");
    value["turns"][0]["user"] = common::REFERENCE.into();
    let script = ScenarioFile::from_value(value).unwrap().scenarios.remove(0);
    let configured = EvalSession::new(
        script,
        &host.url,
        SessionOptions {
            stop_bot: true,
            default_timeout: CLEANUP,
            handshake_timeout: CLEANUP,
            max_duration: WAIT,
            ..Default::default()
        },
    )
    .unwrap()
    .with_cartesia_user_speech(
        speech_settings(&http.url, Some(&cache)),
        pipecat_evals::CartesiaUserSpeechVoice::new("Fixture-VOICE", None, None).unwrap(),
    )
    .unwrap();
    let cancel = Cancellation::new();
    let c = cancel.clone();
    let owner = tokio::spawn(async move {
        configured
            .run_with_timeline_recording(c, TimelineRecordingLimits::default())
            .await
    });
    let admitted = tokio::time::timeout(WAIT, http.trace.admitted.notified())
        .await
        .is_ok();
    cancel.cancel();
    let run = joined_run(owner, &cancel).await;
    let (report, trace, late) = host.finish().await;
    let provider = http.finish().await;
    assert!(
        admitted,
        "provider never accepted the held synthesis request"
    );
    assert!(!late, "{report:?}");
    assert!(report.is_ok());
    assert_eq!(trace.completed.load(Ordering::SeqCst), 0);
    assert!(trace
        .input
        .lock()
        .unwrap()
        .iter()
        .all(|packet| packet.iter().all(|byte| *byte == 0)));
    assert_eq!(trace.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(trace.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(provider.active.load(Ordering::SeqCst), 0);
    assert_eq!(provider.joined.load(Ordering::SeqCst), 1);
    let run = run.unwrap();
    assert!(!run.result.passed);
    assert!(run.to_wav().unwrap().is_none());
    speech_fixture::assert_redacted(&run.result);
    speech_fixture::assert_no_cached_audio(&cache);
    no_temp(&b);
}
