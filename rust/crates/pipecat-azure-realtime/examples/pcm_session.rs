//! One recorded mono PCM16LE utterance → Azure GA Realtime → recorded PCM16LE.
//! No microphone, speaker, browser, public listener or ambient credential lookup.
use pipecat_azure_realtime::{
    AzureRealtimeApp, AzureRealtimeCredential, RealtimeConfig, TurnDetection,
};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, LocalFlushEvent,
    PipelineError, PipelineFlushFrame, PipelineSender, PipelineWorker, ProcessorContext,
    ProcessorError, WorkerState,
};
use pipecat_services::Context;
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_PATH_BYTES: usize = 4096;
const CHUNK_BYTES: usize = 960; // 20 ms, 24 kHz, mono signed PCM16LE.
const CHUNK_DURATION: Duration = Duration::from_millis(20);

const HELP: &str = "\
Recorded Azure OpenAI Realtime PCM session

Usage:
  pcm_session --help
  pcm_session --check-config [--manual|--server-vad]
  pcm_session --manual INPUT.pcm OUTPUT.pcm
  pcm_session --server-vad INPUT.pcm OUTPUT.pcm

Required explicit environment:
  AZURE_REALTIME_BASE_URL
    wss://RESOURCE.openai.azure.com/openai/v1/realtime
    An optional single ?model=DEPLOYMENT must match the explicit deployment.
    ws is accepted only for literal loopback IPs at the same GA path.
  AZURE_REALTIME_DEPLOYMENT
    Azure deployment name, not a guessed underlying model alias.
  Exactly one of AZURE_REALTIME_API_KEY or AZURE_REALTIME_BEARER_TOKEN.
    The bearer token must already exist; this program never mints or refreshes it.

Input is one recorded utterance: raw mono signed PCM16 little-endian, 24000 Hz,
nonempty, even length, at most 8 MiB. WAV headers are not decoded. Server-VAD
input must include end-of-speech silence. Paths are limited to 4096 bytes.
The output must be a NEW file, distinct from input, and is limited to 16 MiB.
No existing file is overwritten. A failed session may leave a partial output.

Audio is sent in paced chunks of at most 20 ms while output is drained.
A finite feed/response deadline and complete shutdown/report checks apply.
The output is generated audio, not proof of playback; history is generated text.
No device, browser, public listener, credential chain or token store is used.
--help reads no credentials. --check-config defaults to manual mode and validates
the same app constructor without opening a connection or creating any file.
Only session mode contacts the explicitly configured endpoint.";

#[derive(Clone, Copy)]
enum Mode {
    Manual,
    ServerVad,
}
impl Mode {
    fn parse(value: &std::ffi::OsStr) -> Option<Self> {
        match value.to_str()? {
            "--manual" => Some(Self::Manual),
            "--server-vad" => Some(Self::ServerVad),
            _ => None,
        }
    }
}

enum Command {
    Help,
    Check(Mode),
    Session {
        mode: Mode,
        input: PathBuf,
        output: PathBuf,
    },
}
fn command() -> Result<Command, &'static str> {
    // Never collect an unbounded command line or print caller-supplied values.
    let args: Vec<OsString> = std::env::args_os().skip(1).take(4).collect();
    match args.as_slice() {
        [arg] if arg == "--help" => Ok(Command::Help),
        [arg] if arg == "--check-config" => Ok(Command::Check(Mode::Manual)),
        [arg, mode] if arg == "--check-config" => Mode::parse(mode)
            .map(Command::Check)
            .ok_or("invalid turn mode; use --help"),
        [mode, input, output] => {
            let mode = Mode::parse(mode).ok_or("invalid arguments; use --help")?;
            if input.is_empty()
                || output.is_empty()
                || input.len() > MAX_PATH_BYTES
                || output.len() > MAX_PATH_BYTES
            {
                return Err("input and output paths must contain 1..4096 bytes");
            }
            Ok(Command::Session {
                mode,
                input: input.into(),
                output: output.into(),
            })
        }
        _ => Err("invalid arguments; use --help"),
    }
}

fn configuration(mode: Mode) -> Result<RealtimeConfig, Box<dyn std::error::Error>> {
    let endpoint = std::env::var("AZURE_REALTIME_BASE_URL")
        .map_err(|_| "AZURE_REALTIME_BASE_URL must be explicitly configured as UTF-8")?;
    let deployment = std::env::var("AZURE_REALTIME_DEPLOYMENT")
        .map_err(|_| "AZURE_REALTIME_DEPLOYMENT must be explicitly configured as UTF-8")?;
    let credential = match (
        std::env::var_os("AZURE_REALTIME_API_KEY"),
        std::env::var_os("AZURE_REALTIME_BEARER_TOKEN"),
    ) {
        (Some(value), None) => AzureRealtimeCredential::ApiKey(
            value
                .into_string()
                .map_err(|_| "AZURE_REALTIME_API_KEY must be UTF-8")?,
        ),
        (None, Some(value)) => AzureRealtimeCredential::EntraBearer(
            value
                .into_string()
                .map_err(|_| "AZURE_REALTIME_BEARER_TOKEN must be UTF-8")?,
        ),
        _ => {
            return Err(
                "configure exactly one of AZURE_REALTIME_API_KEY or AZURE_REALTIME_BEARER_TOKEN"
                    .into(),
            );
        }
    };
    let mut config = RealtimeConfig::azure(endpoint, deployment, credential)?;
    let input = config
        .session
        .audio
        .as_mut()
        .and_then(|audio| audio.input.as_mut())
        .ok_or("Azure PCM input profile missing")?;
    input.turn_detection = match mode {
        Mode::Manual => None,
        Mode::ServerVad => Some(TurnDetection::ServerVad {
            threshold: 0.5,
            prefix_padding_ms: 300,
            silence_duration_ms: 500,
            create_response: true,
            interrupt_response: true,
        }),
    };
    config.validate()?;
    Ok(config)
}

fn read_input(input: &Path, output: &Path) -> Result<Vec<u8>, &'static str> {
    // Resolve identity before opening the output. create_new is the final,
    // race-safe no-overwrite guard, including existing symlink/hard-link aliases.
    let canonical_input = input.canonicalize().map_err(|_| "input file unavailable")?;
    let output_name = output.file_name().ok_or("output must name a new file")?;
    let output_parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|_| "output parent directory unavailable")?;
    if canonical_input == output_parent.join(output_name) {
        return Err("input and output must be different files");
    }
    let metadata = std::fs::metadata(&canonical_input).map_err(|_| "input file unavailable")?;
    if !metadata.is_file() {
        return Err("input must be a regular file");
    }
    let file = File::open(canonical_input).map_err(|_| "input file read failed")?;
    let opened = file.metadata().map_err(|_| "input metadata unavailable")?;
    if !opened.is_file()
        || opened.len() == 0
        || opened.len() > MAX_INPUT_BYTES as u64
        || !opened.len().is_multiple_of(2)
    {
        return Err("input must be a nonempty regular PCM16 file, aligned and at most 8 MiB");
    }
    let mut pcm = Vec::with_capacity(opened.len() as usize);
    file.take(MAX_INPUT_BYTES as u64 + 1)
        .read_to_end(&mut pcm)
        .map_err(|_| "input file read failed")?;
    if pcm.len() as u64 != opened.len() {
        return Err("input changed while reading");
    }
    Ok(pcm)
}

#[derive(Default)]
struct PcmOutput {
    recording: Mutex<Option<(File, usize)>>,
    response_completed: LocalFlushEvent,
}
impl PcmOutput {
    fn create(&self, path: &Path) -> Result<(), &'static str> {
        let mut recording = self.recording.lock().map_err(|_| "output lock failed")?;
        if recording.is_some() {
            return Err("output already initialized");
        }
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|_| "output must be a new writable file")?;
        *recording = Some((file, 0));
        Ok(())
    }
    fn bytes(&self) -> Result<usize, &'static str> {
        self.recording
            .lock()
            .map_err(|_| "output lock failed")?
            .as_ref()
            .map(|(_, bytes)| *bytes)
            .ok_or("output was not initialized")
    }
}
#[async_trait]
impl FrameProcessor for PcmOutput {
    fn name(&self) -> &str {
        "RecordedAzurePcmOutput"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if let FrameData::TtsAudio(audio) = &frame.data {
                if audio.sample_rate != 24_000
                    || audio.num_channels != 1
                    || !audio.audio.len().is_multiple_of(2)
                {
                    return Err(ProcessorError::fatal(
                        "output is not aligned mono 24000 Hz PCM16",
                    ));
                }
                let mut recording = self
                    .recording
                    .lock()
                    .map_err(|_| ProcessorError::fatal("output lock failed"))?;
                let (file, bytes) = recording
                    .as_mut()
                    .ok_or_else(|| ProcessorError::fatal("output was not initialized"))?;
                if bytes.saturating_add(audio.audio.len()) > MAX_OUTPUT_BYTES {
                    return Err(ProcessorError::fatal("recorded output exceeds 16 MiB"));
                }
                file.write_all(&audio.audio)
                    .map_err(|_| ProcessorError::fatal("recorded output write failed"))?;
                *bytes += audio.audio.len();
            }
        }
        let response_completed =
            direction == Direction::Downstream && matches!(&frame.data, FrameData::LlmResponseEnd);
        // The paired assistant consumes response-end frames. Observe the real
        // output boundary rather than expecting that frame at worker.recv().
        // This single-session, one-shot observation is not playback evidence.
        ctx.push(frame, direction).await?;
        if response_completed {
            self.response_completed.set();
        }
        // In particular, never acknowledge_playback for bytes written to a file.
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some((file, _)) = self
            .recording
            .lock()
            .map_err(|_| ProcessorError::fatal("output lock failed"))?
            .as_mut()
        {
            file.flush()
                .map_err(|_| ProcessorError::fatal("recorded output flush failed"))?;
        }
        Ok(())
    }
}

fn pipeline_error(error: &PipelineError) -> &'static str {
    // Do not print arbitrary provider/frame payloads even on an error path.
    match error {
        PipelineError::InvalidConfiguration(_) => "invalid pipeline configuration",
        PipelineError::StartupTimeout => "pipeline startup timeout",
        PipelineError::ShutdownTimeout => "pipeline shutdown timeout",
        PipelineError::Closed => "pipeline closed",
        PipelineError::DuplicateStart => "duplicate pipeline start",
        PipelineError::TerminalUpstream => "invalid terminal direction",
        PipelineError::Backpressure(_) => "pipeline backpressure",
        PipelineError::ProcessorPanic(_) => "pipeline task panicked",
        PipelineError::Execution(_) => "pipeline execution failure",
        PipelineError::AlreadyJoined => "pipeline was already joined",
    }
}

async fn input_barrier(
    sender: &PipelineSender,
    operation_timeout: Duration,
    origin: &'static str,
) -> Result<(), &'static str> {
    let completed = LocalFlushEvent::default();
    tokio::time::timeout(operation_timeout, async {
        sender
            .send(
                Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                    event: Some(completed.clone()),
                    returning: false,
                    origin: Some(origin.into()),
                }))
                .with_interruptible(false),
                Direction::Downstream,
            )
            .await
            .map_err(|_| "manual input barrier admission failed")?;
        // Core sets this private event only after the full down/up/down round
        // trip. This is local ordering, not a remote audio acknowledgment.
        completed.wait().await;
        Ok(())
    })
    .await
    .map_err(|_| "manual input barrier timed out")?
}

async fn feed(
    sender: &PipelineSender,
    pcm: &[u8],
    mode: Mode,
    operation_timeout: Duration,
) -> Result<(), &'static str> {
    if matches!(mode, Mode::Manual) {
        sender
            .send(
                Frame::new(FrameData::UserStartedSpeaking),
                Direction::Downstream,
            )
            .await
            .map_err(|_| "manual turn start failed")?;
        // Normal queues wait for the current system invocation and queued
        // system frames. The user's start emits its interruption before that
        // invocation finishes, so this surviving normal probe follows both.
        input_barrier(sender, operation_timeout, "azure-pcm-start-order").await?;
    }
    for chunk in pcm.chunks(CHUNK_BYTES) {
        sender
            .send(
                Frame::new(FrameData::InputAudio(AudioRawFrame {
                    audio: chunk.to_vec(),
                    sample_rate: 24_000,
                    num_channels: 1,
                    context_id: None,
                })),
                Direction::Downstream,
            )
            .await
            .map_err(|_| "input feed failed")?;
        tokio::time::sleep(CHUNK_DURATION).await;
    }
    if matches!(mode, Mode::Manual) {
        // Stop is a system frame and could overtake normal PCM already
        // admitted into another node. Drain that local path before committing.
        input_barrier(sender, operation_timeout, "azure-pcm-stop-order").await?;
        sender
            .send(
                Frame::new(FrameData::UserStoppedSpeaking),
                Direction::Downstream,
            )
            .await
            .map_err(|_| "manual turn stop failed")?;
    }
    Ok(())
}

async fn receive_response(
    worker: &mut PipelineWorker,
    sender: &PipelineSender,
    pcm: &[u8],
    mode: Mode,
    budget: Duration,
    operation_timeout: Duration,
    output: &PcmOutput,
) -> Result<(), &'static str> {
    let feed = feed(sender, pcm, mode, operation_timeout);
    tokio::pin!(feed);
    let response = output.response_completed.wait();
    tokio::pin!(response);
    let deadline = tokio::time::sleep(budget);
    tokio::pin!(deadline);
    let mut fed = false;
    let mut responded = false;
    // Do not stop draining if the response arrives before the recording's tail.
    while !fed || !responded {
        tokio::select! {
            _ = &mut deadline => return Err("recorded input/response deadline exceeded"),
            result = &mut feed, if !fed => {
                result?;
                fed = true;
            }
            _ = &mut response, if !responded => responded = true,
            frame = worker.recv() => {
                match frame.map(|frame| frame.frame.data) {
                    Some(FrameData::Error(_) | FrameData::ErrorDetailed(_) | FrameData::FatalError(_)) => {
                        return Err("provider or output session failed");
                    }
                    Some(_) => {}
                    None => return Err("session closed before input and response completed"),
                }
            }
        }
    }
    Ok(())
}

async fn finish(
    worker: &mut PipelineWorker,
    sender: &PipelineSender,
    successful_response: bool,
    operation_timeout: Duration,
) -> Vec<String> {
    let terminate = async {
        let mut failures = Vec::new();
        if !sender.state().is_finished() {
            if successful_response {
                match tokio::time::timeout(operation_timeout, sender.end()).await {
                    Ok(Ok(())) => return failures,
                    Ok(Err(error)) => {
                        failures.push(format!("End admission: {}", pipeline_error(&error)));
                    }
                    Err(_) => failures.push("End admission timed out".into()),
                }
            }
            if !sender.state().is_finished() {
                match tokio::time::timeout(
                    operation_timeout,
                    sender.cancel("recorded Azure session failed"),
                )
                .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        failures.push(format!("Cancel admission: {}", pipeline_error(&error)));
                    }
                    Err(_) => failures.push("Cancel admission timed out".into()),
                }
            }
        }
        failures
    };
    let drain_and_join = async {
        // Consume rather than collect: output has a finite queue, not an
        // unbounded in-memory log. Do not detach or discard the owned join.
        while worker.recv().await.is_some() {}
        let report = worker.join().await;
        let mut failures = Vec::new();
        match report {
            Ok(report) => {
                let expected_state = if successful_response {
                    WorkerState::Ended
                } else {
                    WorkerState::Cancelled
                };
                if report.state != expected_state
                    || !report.errors.is_empty()
                    || report.total_errors != 0
                    || report.evicted_errors != 0
                    || (successful_response && report.dropped_frames != 0)
                {
                    failures.push(format!(
                        "pipeline report: state={:?}, retained_errors={}, total_errors={}, evicted_errors={}, dropped_frames={}",
                        report.state, report.errors.len(), report.total_errors,
                        report.evicted_errors, report.dropped_frames,
                    ));
                }
            }
            Err(error) => {
                failures.push(format!("pipeline join: {}", pipeline_error(&error)));
                // A failed join has no WorkerReport. Its retained diagnostics
                // still include cleanup errors; never print provider payloads.
                failures.push(format!(
                    "pipeline diagnostics: retained_errors={}, total_errors={}, evicted_errors={}",
                    worker.errors().len(),
                    worker.total_errors(),
                    worker.evicted_errors(),
                ));
            }
        }
        failures
    };
    let (mut terminal, cleanup) = tokio::join!(terminate, drain_and_join);
    terminal.extend(cleanup);
    terminal
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let command = command()?;
    let (mode, paths) = match command {
        Command::Help => {
            println!("{HELP}");
            return Ok(());
        }
        Command::Check(mode) => (mode, None),
        Command::Session {
            mode,
            input,
            output,
        } => (mode, Some((input, output))),
    };
    let config = configuration(mode)?;
    let response_timeout = config.limits.response_timeout;
    let operation_timeout = config.limits.operation_timeout;
    let output = Arc::new(PcmOutput::default());
    // Validate the actual public app before any output artifact or connection.
    let app = AzureRealtimeApp::new(config, Context::default(), vec![], output.clone())?;
    let Some((input_path, output_path)) = paths else {
        println!("Azure GA PCM application configuration valid; no connection or file opened");
        return Ok(());
    };
    let pcm = read_input(&input_path, &output_path)?;
    // Account for the two actual manual barrier phases, without extending the
    // response or per-operation deadline. Server-VAD retains one allowance.
    let operation_budget = operation_timeout
        * match mode {
            Mode::Manual => 2,
            Mode::ServerVad => 1,
        };
    let budget = CHUNK_DURATION * pcm.len().div_ceil(CHUNK_BYTES) as u32
        + response_timeout
        + operation_budget;
    output.create(&output_path)?;
    let running = app.start().await.map_err(|error| match error {
        // The app composes this sanitized startup diagnostic specifically
        // to preserve the primary, cancellation and joined cleanup result.
        PipelineError::Execution(message) => format!("application startup: {message}"),
        error => format!("application startup: {}", pipeline_error(&error)),
    })?;
    let mut worker = running.worker;
    let sender = worker.sender();
    let primary = receive_response(
        &mut worker,
        &sender,
        &pcm,
        mode,
        budget,
        operation_timeout,
        &output,
    )
    .await
    .err();
    let mut cleanup = finish(&mut worker, &sender, primary.is_none(), operation_timeout).await;
    if !running.provider.session_joined() {
        cleanup.push("provider actor was not joined".into());
    }
    let tools = running.outcomes.snapshot();
    if !tools.active.is_empty() || tools.total_terminal != 0 || tools.unadmitted_terminal != 0 {
        cleanup.push(format!(
            "unexpected tool journal: active={}, terminal={}, unadmitted_terminal={}",
            tools.active.len(),
            tools.total_terminal,
            tools.unadmitted_terminal,
        ));
    }
    let bytes = match output.bytes() {
        Ok(bytes) => bytes,
        Err(error) => {
            cleanup.push(error.into());
            0
        }
    };
    if bytes == 0 && primary.is_none() {
        cleanup.push("response completed without recorded audio".into());
    }
    if primary.is_some() || !cleanup.is_empty() {
        return Err(format!(
            "recorded session failed: primary={}; cleanup/report={}",
            primary.unwrap_or("none"),
            if cleanup.is_empty() {
                "clean".into()
            } else {
                cleanup.join("; ")
            }
        )
        .into());
    }
    println!(
        "Recorded {bytes} bytes of mono 24000 Hz PCM16; full pipeline report clean. Generated audio and text history are not playback evidence."
    );
    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("{error}");
        std::process::exit(1);
    }
}
