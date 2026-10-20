//! One finite raw PCM recording through the public paired Nova Sonic application.
//! No microphone, speaker, credential discovery, playback receipt or second client.
use pipecat_aws_common::AwsCredentials;
use pipecat_aws_nova_sonic::{NovaSonicApp, NovaSonicConfig, NovaSonicInput, NovaSonicWorker};
use pipecat_core::{
    async_trait, AudioRawFrame, Direction, Frame, FrameData, FrameProcessor, LocalFlushEvent,
    PipelineError, ProcessorContext, ProcessorError, WorkerState,
};
use pipecat_services::Context;
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_INPUT_BYTES: usize = 8 * 1024 * 1024;
const MAX_OUTPUT_BYTES: usize = 16 * 1024 * 1024;
const MAX_ARGUMENTS: usize = 15;
const MAX_ARGUMENT_BYTES: usize = 4096;
const MAX_CONNECT_ADDRESS_BYTES: usize = 64;
const CHUNK_BYTES: usize = 1024;
const CHUNK_DURATION: Duration = Duration::from_millis(32); // 16 kHz mono PCM16LE.

const HELP: &str = "\
Recorded AWS Nova Sonic PCM session

Usage:
  pcm_session --help
  pcm_session --check-config --region REGION --model MODEL --voice VOICE [OPTIONS]
  pcm_session --session INPUT.pcm OUTPUT.pcm --region REGION --model MODEL --voice VOICE [OPTIONS]

Options:
  --endpoint URL            Explicit regional or literal-loopback endpoint override.
  --connect-address IP:PORT Caller-resolved address for hosted regional HTTPS.
                            Required for hosted HTTPS, with port 443; bracket IPv6.
  --credentials-env PREFIX  Read only PREFIX_ACCESS_KEY_ID, PREFIX_SECRET_ACCESS_KEY,
                            and optional PREFIX_SESSION_TOKEN. Default prefix: AWS.

For hosted HTTPS, the caller resolves the exact regional hostname and supplies
one IP:443 address. This program performs no DNS resolution, fallback or retry.
The original regional hostname remains the TLS SNI/certificate name, HTTP
authority and SigV4 host. Literal-loopback endpoints connect directly and reject
--connect-address. An address option must be an exact numeric IP:port, at most
64 bytes; hostnames, URLs and surrounding whitespace are not accepted.

Model, region and voice are explicit; configuration validation does not verify
hosted AWS access or model/voice availability. There is no profile, IMDS, SSO,
SDK credential chain, login, token minting, refresh or credential persistence.
Environment names, not credential values, may be supplied on the command line.
--help reads no credentials. --check-config constructs the same public app,
without opening a network connection or creating an output file.

Input must be a nonempty regular non-symlink file of raw mono signed PCM16LE,
16000 Hz, even length, at most 8 MiB, and short enough for the finite session
budget including response and shutdown reserves. WAV is not decoded.
Include sufficient end-of-speech silence: Nova decides turns; this program
does not invent a client-side commit or user-speaking event.

Output is raw mono PCM16LE at 24000 Hz, limited to 16 MiB. It must be a NEW
file: existing files, symlinks and hard-link aliases are never overwritten.
Paths and other argument values are limited to 4096 bytes. Race-resistant
input identity checks require Unix; help/configuration checks are portable.

All input is sent in paced chunks of at most 1024 bytes while output drains.
After feeding, wait at most the configured response timeout for at least one
complete response to traverse the output, then End the actual paired app.
A response may already have completed while trailing input silence was sent.
Every failure after output creation may leave a partial file; it is retained,
not silently deleted. Already written audio cannot be recalled by interruption.
Recording and generated-text history are not evidence of speaker playback.";

#[derive(Debug)]
struct Options {
    region: String,
    model: String,
    voice: String,
    endpoint: Option<String>,
    connect_address: Option<SocketAddr>,
    credentials_env: String,
}

#[derive(Debug)]
enum Command {
    Help,
    Check(Options),
    Session {
        options: Options,
        input: PathBuf,
        output: PathBuf,
    },
}

fn parse_command(args: impl IntoIterator<Item = OsString>) -> Result<Command, &'static str> {
    // Bound both the number of retained values and each value. The extra item
    // detects excess arguments rather than silently ignoring their tail.
    let args: Vec<_> = args.into_iter().take(MAX_ARGUMENTS + 1).collect();
    if args.len() > MAX_ARGUMENTS
        || args
            .iter()
            .any(|arg| arg.is_empty() || arg.len() > MAX_ARGUMENT_BYTES)
    {
        return Err("too many, empty or oversized arguments; use --help");
    }
    if matches!(args.as_slice(), [arg] if arg == "--help") {
        return Ok(Command::Help);
    }
    let (paths, option_start) = match args.first().and_then(|arg| arg.to_str()) {
        Some("--check-config") => (None, 1),
        Some("--session") if args.len() >= 3 => {
            (Some((PathBuf::from(&args[1]), PathBuf::from(&args[2]))), 3)
        }
        _ => return Err("choose --help, --check-config or --session; use --help"),
    };
    let mut region = None;
    let mut model = None;
    let mut voice = None;
    let mut endpoint = None;
    let mut connect_address = None;
    let mut credentials_env = None;
    let (pairs, remainder) = args[option_start..].as_chunks::<2>();
    for pair in pairs {
        let value = pair[1]
            .to_str()
            .ok_or("option values must be valid UTF-8")?
            .to_owned();
        let slot = match pair[0].to_str() {
            Some("--region") => &mut region,
            Some("--model") => &mut model,
            Some("--voice") => &mut voice,
            Some("--endpoint") => &mut endpoint,
            Some("--connect-address") => &mut connect_address,
            Some("--credentials-env") => &mut credentials_env,
            _ => return Err("unknown option; use --help"),
        };
        if slot.replace(value).is_some() {
            return Err("duplicate option; use --help");
        }
    }
    if !remainder.is_empty() {
        return Err("option requires a value; use --help");
    }
    let connect_address = connect_address
        .map(|value| {
            if value.len() > MAX_CONNECT_ADDRESS_BYTES {
                return Err("connect address exceeds its 64-byte bound");
            }
            value
                .parse::<SocketAddr>()
                .map_err(|_| "connect address must be an exact numeric IP:port")
        })
        .transpose()?;
    let credentials_env = credentials_env.unwrap_or_else(|| "AWS".into());
    if credentials_env.len() > 64
        || !credentials_env
            .bytes()
            .all(|byte| byte.is_ascii_uppercase() || byte.is_ascii_digit() || byte == b'_')
        || !credentials_env
            .as_bytes()
            .first()
            .is_some_and(|byte| byte.is_ascii_uppercase() || *byte == b'_')
    {
        return Err("credential environment prefix must be 1..64 uppercase ASCII identifier bytes");
    }
    let options = Options {
        region: region.ok_or("--region is required")?,
        model: model.ok_or("--model is required")?,
        voice: voice.ok_or("--voice is required")?,
        endpoint,
        connect_address,
        credentials_env,
    };
    Ok(match paths {
        None => Command::Check(options),
        Some((input, output)) => Command::Session {
            options,
            input,
            output,
        },
    })
}

fn required_env(name: &str, max_bytes: usize) -> Result<String, &'static str> {
    let value =
        std::env::var_os(name).ok_or("required explicit credential environment is missing")?;
    if value.is_empty() || value.len() > max_bytes {
        return Err("explicit credential environment value is empty or exceeds its bound");
    }
    value
        .into_string()
        .map_err(|_| "explicit credential environment must contain valid UTF-8")
}

fn configuration(options: Options) -> Result<NovaSonicConfig, &'static str> {
    // Called only after a complete session/check command has been selected.
    // No other environment variable or AWS provider is consulted.
    let access = required_env(&format!("{}_ACCESS_KEY_ID", options.credentials_env), 256)?;
    let secret = required_env(
        &format!("{}_SECRET_ACCESS_KEY", options.credentials_env),
        4096,
    )?;
    let token = match std::env::var_os(format!("{}_SESSION_TOKEN", options.credentials_env)) {
        None => None,
        Some(value) if !value.is_empty() && value.len() <= 16_384 => Some(
            value
                .into_string()
                .map_err(|_| "explicit session token must contain valid UTF-8")?,
        ),
        Some(_) => return Err("explicit session token is empty or exceeds its bound"),
    };
    let credentials = AwsCredentials::new(access, secret, token)
        .map_err(|_| "invalid explicit AWS credentials")?;
    let mut config =
        NovaSonicConfig::new(options.region, options.model, options.voice, credentials)
            .map_err(|_| "invalid Nova Sonic configuration")?;
    config.endpoint = options.endpoint;
    config.connect_address = options.connect_address;
    config
        .validate()
        .map_err(|_| "invalid Nova Sonic configuration")?;
    Ok(config)
}

fn output_location(input: &Path, output: &Path) -> Result<PathBuf, &'static str> {
    let name = output.file_name().ok_or("output must name a new file")?;
    let parent = output
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .canonicalize()
        .map_err(|_| "output parent directory unavailable")?;
    let target = parent.join(name);
    if target == input {
        return Err("input and output must be different files");
    }
    match std::fs::symlink_metadata(&target) {
        Ok(_) => return Err("output must be a new file, not an existing file or alias"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
        Err(_) => return Err("output location unavailable"),
    }
    Ok(target)
}

#[cfg(unix)]
fn read_input(input: &Path, output: &Path) -> Result<(Vec<u8>, PathBuf), &'static str> {
    use std::io::Read;
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt};

    // Reject symlinks before opening; O_NOFOLLOW also closes the last-component
    // race. O_NONBLOCK prevents a concurrent FIFO substitution from hanging.
    // Descriptor/path identity and timestamps are checked again after reading.
    let identity = |metadata: &std::fs::Metadata| {
        (
            metadata.dev(),
            metadata.ino(),
            metadata.len(),
            metadata.mtime(),
            metadata.mtime_nsec(),
            metadata.ctime(),
            metadata.ctime_nsec(),
        )
    };
    let before = std::fs::symlink_metadata(input).map_err(|_| "input file unavailable")?;
    if !before.is_file()
        || before.len() == 0
        || before.len() > MAX_INPUT_BYTES as u64
        || !before.len().is_multiple_of(2)
    {
        return Err(
            "input must be a regular non-symlink PCM16 file, nonempty, aligned and at most 8 MiB",
        );
    }
    let canonical = input
        .canonicalize()
        .map_err(|_| "input identity unavailable")?;
    let output = output_location(&canonical, output)?;
    let mut file = OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK)
        .open(input)
        .map_err(|_| "input regular-file open failed")?;
    let opened = file.metadata().map_err(|_| "input metadata unavailable")?;
    if !opened.is_file() || identity(&before) != identity(&opened) {
        return Err("input identity changed before reading");
    }
    let mut pcm = Vec::with_capacity(opened.len() as usize);
    std::io::Read::by_ref(&mut file)
        .take(MAX_INPUT_BYTES as u64 + 1)
        .read_to_end(&mut pcm)
        .map_err(|_| "input file read failed")?;
    let after = file.metadata().map_err(|_| "input metadata unavailable")?;
    let named =
        std::fs::symlink_metadata(input).map_err(|_| "input identity changed while reading")?;
    if pcm.len() as u64 != opened.len()
        || !named.is_file()
        || identity(&opened) != identity(&after)
        || identity(&opened) != identity(&named)
    {
        return Err("input identity or contents changed while reading");
    }
    Ok((pcm, output))
}

#[cfg(not(unix))]
fn read_input(_input: &Path, _output: &Path) -> Result<(Vec<u8>, PathBuf), &'static str> {
    Err("race-resistant recording file validation is supported on Unix only")
}

struct Recording {
    file: File,
    bytes: usize,
}

#[derive(Default)]
struct PcmOutput {
    recording: Mutex<Option<Recording>>,
    response_completed: LocalFlushEvent,
}

impl PcmOutput {
    fn create(&self, path: &Path) -> Result<(), &'static str> {
        let mut recording = self.recording.lock().map_err(|_| "output lock failed")?;
        if recording.is_some() {
            return Err("output already initialized");
        }
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options
                .mode(0o600)
                .custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
        }
        let file = options
            .open(path)
            .map_err(|_| "output must be a new writable file; no existing file was overwritten")?;
        *recording = Some(Recording { file, bytes: 0 });
        Ok(())
    }

    fn bytes(&self) -> Result<usize, &'static str> {
        self.recording
            .lock()
            .map_err(|_| "output lock failed")?
            .as_ref()
            .map(|recording| recording.bytes)
            .ok_or("output was not initialized")
    }
}

#[async_trait]
impl FrameProcessor for PcmOutput {
    fn name(&self) -> &str {
        "RecordedNovaSonicPcmOutput"
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
                    || audio.audio.is_empty()
                    || !audio.audio.len().is_multiple_of(2)
                {
                    return Err(ProcessorError::fatal(
                        "recorded output is not nonempty aligned mono 24000 Hz PCM16",
                    ));
                }
                if !ctx.is_current() {
                    return Ok(());
                }
                let mut guard = self
                    .recording
                    .lock()
                    .map_err(|_| ProcessorError::fatal("output lock failed"))?;
                let recording = guard
                    .as_mut()
                    .ok_or_else(|| ProcessorError::fatal("output was not initialized"))?;
                if recording.bytes.saturating_add(audio.audio.len()) > MAX_OUTPUT_BYTES {
                    return Err(ProcessorError::fatal("recorded output exceeds 16 MiB"));
                }
                // Account for each successful write, including a partial write
                // before a subsequent error. Never claim those bytes played.
                let mut remaining = audio.audio.as_slice();
                while !remaining.is_empty() {
                    match recording.file.write(remaining) {
                        Ok(0) => {
                            return Err(ProcessorError::fatal(
                                "recorded output write made no progress",
                            ));
                        }
                        Ok(bytes) => {
                            recording.bytes += bytes;
                            remaining = &remaining[bytes..];
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::Interrupted => continue,
                        Err(_) => {
                            return Err(ProcessorError::fatal("recorded output write failed"))
                        }
                    }
                }
            }
        }
        let completed = direction == Direction::Downstream
            && matches!(&frame.data, FrameData::LlmResponseEnd)
            && ctx.is_current();
        // The real assistant consumes this terminal, so observe it here rather
        // than expecting it to remain visible at NovaSonicWorker::recv.
        ctx.push(frame, direction).await?;
        if completed && ctx.is_current() {
            self.response_completed.set();
        }
        Ok(())
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if let Some(recording) = self
            .recording
            .lock()
            .map_err(|_| ProcessorError::fatal("output lock failed"))?
            .as_mut()
        {
            recording
                .file
                .flush()
                .map_err(|_| ProcessorError::fatal("recorded output flush failed"))?;
        }
        Ok(())
    }
}

fn pipeline_error(error: &PipelineError) -> &'static str {
    // Never print caller/provider text, credentials, endpoint URLs or paths.
    match error {
        PipelineError::InvalidConfiguration(_) => "invalid pipeline configuration",
        PipelineError::StartupTimeout => "pipeline startup timeout",
        PipelineError::ShutdownTimeout => "pipeline shutdown timeout",
        PipelineError::Closed => "pipeline closed",
        PipelineError::DuplicateStart => "duplicate pipeline start",
        PipelineError::TerminalUpstream => "invalid terminal direction",
        PipelineError::Backpressure(_) => "pipeline backpressure",
        PipelineError::ProcessorPanic(_) => "pipeline task panicked",
        PipelineError::Execution(_) => "pipeline execution or cleanup failure",
        PipelineError::AlreadyJoined => "pipeline was already joined",
    }
}

fn startup_diagnostic(error: &PipelineError) -> String {
    match error {
        // This executable fixes every processor, including its recorder. The
        // app combines only sanitized protocol/transport errors and our static
        // recorder errors here, preserving primary versus cleanup failures.
        // This is not a general-purpose printer for arbitrary app output sinks.
        PipelineError::Execution(message) => message.chars().take(8192).collect(),
        other => pipeline_error(other).into(),
    }
}

fn feed_duration(bytes: usize) -> Duration {
    // read_input already limits this to 8192 chunks.
    CHUNK_DURATION * bytes.div_ceil(CHUNK_BYTES) as u32
}

async fn feed(
    sender: &NovaSonicInput,
    pcm: &[u8],
    operation_timeout: Duration,
) -> Result<(), &'static str> {
    for chunk in pcm.chunks(CHUNK_BYTES) {
        tokio::time::timeout(
            operation_timeout,
            sender.send(
                Frame::new(FrameData::InputAudio(AudioRawFrame {
                    audio: chunk.to_vec(),
                    sample_rate: 16_000,
                    num_channels: 1,
                    context_id: None,
                })),
                Direction::Downstream,
            ),
        )
        .await
        .map_err(|_| "input admission timed out")?
        .map_err(|_| "input admission failed")?;
        tokio::time::sleep(CHUNK_DURATION).await;
    }
    Ok(())
}

async fn receive_response(
    worker: &mut NovaSonicWorker,
    sender: &NovaSonicInput,
    pcm: &[u8],
    operation_timeout: Duration,
    response_timeout: Duration,
    deadline: tokio::time::Instant,
    output: &PcmOutput,
) -> Result<(), &'static str> {
    // These futures are polled in this scope, not detached tasks. Worker output
    // continues draining during every input send, pacing delay and response wait.
    let feed = feed(sender, pcm, operation_timeout);
    tokio::pin!(feed);
    let response = output.response_completed.wait();
    tokio::pin!(response);
    let session = tokio::time::sleep_until(deadline);
    tokio::pin!(session);
    let response_deadline = tokio::time::sleep_until(deadline);
    tokio::pin!(response_deadline);
    let mut fed = false;
    let mut responded = false;
    while !fed || !responded {
        tokio::select! {
            _ = &mut session => return Err("finite recording session deadline exceeded"),
            _ = &mut response_deadline, if fed && !responded => {
                return Err("response completion deadline exceeded");
            }
            result = &mut feed, if !fed => {
                result?;
                fed = true;
                response_deadline.as_mut().reset(
                    std::cmp::min(deadline, tokio::time::Instant::now() + response_timeout),
                );
            }
            _ = &mut response, if !responded => responded = true,
            frame = worker.recv() => {
                match frame.map(|frame| frame.frame.data) {
                    Some(FrameData::Error(_) | FrameData::ErrorDetailed(_) | FrameData::FatalError(_)) => {
                        return Err("provider or recording output failed");
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
    worker: &mut NovaSonicWorker,
    sender: &NovaSonicInput,
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
                    sender.cancel("recorded Nova Sonic session failed"),
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
        // The public app owns its finite shutdown deadlines. Keep its worker's
        // retained join alive and drain concurrently instead of collecting frames
        // or dropping a join future as soon as a terminal is admitted.
        while worker.recv().await.is_some() {}
        let mut failures = Vec::new();
        match worker.join().await {
            Ok(report) => {
                let expected = if successful_response {
                    WorkerState::Ended
                } else {
                    WorkerState::Cancelled
                };
                if report.state != expected
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
    let (mut failures, joined) = tokio::join!(terminate, drain_and_join);
    failures.extend(joined);
    failures
}

async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let (options, paths) = match parse_command(std::env::args_os().skip(1))? {
        Command::Help => {
            println!("{HELP}");
            return Ok(());
        }
        Command::Check(options) => (options, None),
        Command::Session {
            options,
            input,
            output,
        } => (options, Some((input, output))),
    };
    let config = configuration(options)?;
    let operation_timeout = config.limits.operation_timeout;
    let response_timeout = config.limits.response_timeout;
    let session_timeout = config.limits.session_timeout;
    let drain_timeout = config.limits.drain_timeout;
    let output = Arc::new(PcmOutput::default());
    // The recorder deliberately has no file yet. This is exactly the same
    // production app constructor used in session mode, with no socket client.
    let app = NovaSonicApp::new(config, Context::default(), output.clone())
        .map_err(|_| "invalid paired Nova Sonic application configuration")?;
    let Some((input_path, output_path)) = paths else {
        println!("Nova Sonic paired PCM application configuration valid; no connection or output file opened");
        return Ok(());
    };
    let (pcm, output_path) = read_input(&input_path, &output_path)?;
    let reserve = response_timeout
        .checked_add(drain_timeout)
        .and_then(|duration| duration.checked_add(operation_timeout))
        .ok_or("invalid finite session budget")?;
    let feed_budget = session_timeout
        .checked_sub(reserve)
        .ok_or("session budget has no recording allowance")?;
    if feed_duration(pcm.len()) >= feed_budget {
        return Err("input duration leaves insufficient response and shutdown reserve".into());
    }
    output.create(&output_path)?;
    let deadline = tokio::time::Instant::now()
        + session_timeout
            .checked_sub(drain_timeout)
            .ok_or("session budget has no drain reserve")?;
    let mut running = app.start().await.map_err(|error| {
        format!(
            "application startup failed: {}; output was created and may be partial; no successful joined session is claimed",
            startup_diagnostic(&error),
        )
    })?;
    let sender = running.worker.sender();
    let primary = receive_response(
        &mut running.worker,
        &sender,
        &pcm,
        operation_timeout,
        response_timeout,
        deadline,
        &output,
    )
    .await
    .err();
    let mut cleanup = finish(
        &mut running.worker,
        &sender,
        primary.is_none(),
        operation_timeout,
    )
    .await;
    if running.provider.wait_joined().await.is_err() {
        cleanup.push("provider retained shutdown result is failure".into());
    }
    if !running.provider.session_joined() {
        cleanup.push("provider session join is unproven".into());
    }
    if primary.is_none() && running.provider.responses_completed() == 0 {
        cleanup.push("no actual provider response completion observed".into());
    }
    let bytes = match output.bytes() {
        Ok(bytes) => bytes,
        Err(error) => {
            cleanup.push(error.into());
            0
        }
    };
    if primary.is_none() && bytes == 0 {
        cleanup.push("response completed without recorded audio".into());
    }
    if primary.is_some() || !cleanup.is_empty() {
        return Err(format!(
            "recorded session failed: primary={}; cleanup/report={}; output retained and may be partial ({bytes} bytes acknowledged by file writes)",
            primary.unwrap_or("none"),
            if cleanup.is_empty() { "clean".into() } else { cleanup.join("; ") },
        )
        .into());
    }
    println!(
        "Recorded {bytes} bytes of mono 24000 Hz PCM16LE; pipeline report clean and provider session joined. Generated audio and text history are not playback evidence."
    );
    Ok(())
}

#[tokio::main]
async fn main() -> std::process::ExitCode {
    match run().await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            std::process::ExitCode::FAILURE
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(values: &[&str]) -> Vec<OsString> {
        values.iter().map(OsString::from).collect()
    }

    #[test]
    fn help_needs_no_configuration() {
        assert!(matches!(
            parse_command(args(&["--help"])).unwrap(),
            Command::Help
        ));
        assert!(parse_command(args(&["--help", "--region", "us-east-1"])).is_err());
    }

    #[test]
    fn explicit_options_and_named_synthetic_environment() {
        let command = parse_command(args(&[
            "--check-config",
            "--region",
            "us-east-1",
            "--model",
            "amazon.nova-2-sonic-v1:0",
            "--voice",
            "tiffany",
            "--credentials-env",
            "NOVA_FIXTURE",
        ]))
        .unwrap();
        let Command::Check(options) = command else {
            panic!("wrong command");
        };
        assert_eq!(options.credentials_env, "NOVA_FIXTURE");
        assert!(options.endpoint.is_none());
        assert!(options.connect_address.is_none());
        assert_eq!(options.region, "us-east-1");
    }

    #[test]
    fn parses_only_bounded_numeric_connect_addresses() {
        let base = [
            "--check-config",
            "--region",
            "us-east-1",
            "--model",
            "amazon.nova-2-sonic-v1:0",
            "--voice",
            "tiffany",
            "--connect-address",
        ];
        for value in ["192.0.2.1:443", "[2001:db8::1]:443"] {
            let mut command_args = args(&base);
            command_args.push(value.into());
            let Command::Check(options) = parse_command(command_args).unwrap() else {
                panic!("wrong command");
            };
            assert_eq!(options.connect_address, Some(value.parse().unwrap()));
        }
        for value in [
            "localhost:443",
            "https://192.0.2.1:443",
            " 192.0.2.1:443",
            "192.0.2.1:443 ",
            "192.0.2.1",
            "192.0.2.1:65536",
            "::1:443",
            "[2001:db8::1]:443/trailing",
        ] {
            let mut command_args = args(&base);
            command_args.push(value.into());
            assert!(parse_command(command_args).is_err());
        }
        let mut oversized = args(&base);
        oversized.push("1".repeat(MAX_CONNECT_ADDRESS_BYTES + 1).into());
        assert!(parse_command(oversized).is_err());
        let mut duplicate = args(&base);
        duplicate.extend(args(&[
            "192.0.2.1:443",
            "--connect-address",
            "192.0.2.2:443",
        ]));
        assert!(parse_command(duplicate).is_err());
    }

    #[test]
    fn accepts_all_options_without_unbounded_argument_collection() {
        let command = parse_command(args(&[
            "--session",
            "input.pcm",
            "output.pcm",
            "--region",
            "us-east-1",
            "--model",
            "amazon.nova-2-sonic-v1:0",
            "--voice",
            "tiffany",
            "--endpoint",
            "https://bedrock-runtime.us-east-1.amazonaws.com/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream",
            "--connect-address",
            "192.0.2.1:443",
            "--credentials-env",
            "NOVA_FIXTURE",
        ]))
        .unwrap();
        assert!(matches!(command, Command::Session { .. }));
    }

    #[test]
    fn rejects_excess_arguments_including_help_tail() {
        assert!(parse_command((0..100).map(|_| OsString::from("--help"))).is_err());
        assert!(parse_command(args(&["--session", "input", "output", "--region"])).is_err());
        assert!(parse_command(args(&["--check-config", "--bogus", "value"])).is_err());
        assert!(parse_command(vec![OsString::from("x".repeat(MAX_ARGUMENT_BYTES + 1))]).is_err());
    }

    #[test]
    fn rejects_duplicate_options_and_invalid_environment_names() {
        for prefix in ["lowercase", "2START", "BAD=VALUE", "WITH SPACE", ""] {
            assert!(parse_command(args(&[
                "--check-config",
                "--region",
                "us-east-1",
                "--model",
                "amazon.nova-2-sonic-v1:0",
                "--voice",
                "tiffany",
                "--credentials-env",
                prefix,
            ]))
            .is_err());
        }
        assert!(parse_command(args(&[
            "--check-config",
            "--region",
            "us-east-1",
            "--region",
            "us-west-2",
        ]))
        .is_err());
    }

    #[test]
    fn chunk_pacing_is_exactly_32_ms_at_16khz() {
        assert_eq!(feed_duration(1024), Duration::from_millis(32));
        assert_eq!(feed_duration(1026), Duration::from_millis(64));
        assert_eq!(CHUNK_BYTES / 2 * 1000 / 16_000, 32);
    }

    #[test]
    fn startup_diagnostics_preserve_safe_cleanup_but_not_panic_text() {
        let safe = "Nova HTTP2 startup failed; pipeline cleanup: joined; session cleanup: failed";
        assert_eq!(
            startup_diagnostic(&PipelineError::Execution(safe.into())),
            safe,
        );
        assert_eq!(
            startup_diagnostic(&PipelineError::ProcessorPanic("PRIVATE_MARKER".into())),
            "pipeline task panicked",
        );
        assert_eq!(
            startup_diagnostic(&PipelineError::Execution("x".repeat(9000))).len(),
            8192,
        );
    }

    #[cfg(unix)]
    struct FixtureDir(PathBuf);
    #[cfg(unix)]
    impl FixtureDir {
        fn new() -> Self {
            use std::sync::atomic::{AtomicU64, Ordering};
            static NEXT: AtomicU64 = AtomicU64::new(0);
            let name = format!(
                "nova-pcm-example-{}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos(),
                NEXT.fetch_add(1, Ordering::Relaxed),
            );
            let path = std::env::temp_dir().join(name);
            std::fs::create_dir(&path).unwrap();
            Self(path)
        }
    }
    #[cfg(unix)]
    impl Drop for FixtureDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    #[test]
    #[cfg(unix)]
    fn regular_input_and_new_output_are_distinct_and_bounded() {
        let dir = FixtureDir::new();
        let input = dir.0.join("input.pcm");
        let output = dir.0.join("output.pcm");
        std::fs::write(&input, [1, 2, 3, 4]).unwrap();
        let (pcm, target) = read_input(&input, &output).unwrap();
        assert_eq!(pcm, [1, 2, 3, 4]);
        assert!(!output.exists());
        let recorder = PcmOutput::default();
        recorder.create(&target).unwrap();
        assert_eq!(recorder.bytes().unwrap(), 0);
        assert!(PcmOutput::default().create(&target).is_err());
        assert!(read_input(&input, &output).is_err());
        assert!(read_input(&input, &input).is_err());
        assert_eq!(std::fs::read(&input).unwrap(), [1, 2, 3, 4]);
    }

    #[test]
    #[cfg(unix)]
    fn rejects_symlinks_hardlink_output_and_invalid_pcm_before_output_creation() {
        use std::os::unix::fs::symlink;
        let dir = FixtureDir::new();
        let input = dir.0.join("input.pcm");
        let output = dir.0.join("output.pcm");
        let alias = dir.0.join("alias.pcm");
        std::fs::write(&input, [1, 2]).unwrap();
        symlink(&input, &alias).unwrap();
        assert!(read_input(&alias, &output).is_err());
        assert!(!output.exists());
        std::fs::remove_file(&alias).unwrap();
        std::fs::hard_link(&input, &alias).unwrap();
        assert!(read_input(&input, &alias).is_err());
        std::fs::remove_file(&alias).unwrap();
        symlink(dir.0.join("missing"), &alias).unwrap();
        assert!(read_input(&input, &alias).is_err());
        for contents in [&[][..], &[1][..], &[1, 2, 3][..]] {
            std::fs::write(&input, contents).unwrap();
            assert!(read_input(&input, &output).is_err());
            assert!(!output.exists());
        }
        assert!(read_input(&dir.0, &output).is_err());
    }
}
