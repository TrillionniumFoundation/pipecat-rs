//! Real finite-file application; all filesystem persistence follows network joins.
use pipecat_aws_common::AwsCredentials;
use pipecat_aws_sagemaker_bidi_tts::{
    DuplexLimits, SageMakerBidiTtsApp, SageMakerBidiTtsConfig, SynthesisSettings, TtsLimits,
};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
};
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};
const MAX_INPUT: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const HELP: &str = "Finite NVIDIA Magpie SageMaker BiDi TTS (Linux files)\nUsage: sagemaker_bidi_tts [--help | --check-config]\n       sagemaker_bidi_tts --run INPUT.json OUTPUT.pcm [--timeout-ms N] [--max-output-bytes N]\nINPUT.json is a bounded nonempty JSON array of utterance strings, max 16 utterances.\nOUTPUT.pcm is a NEW mode-0600 mono little-endian PCM16 file, default 24 kHz.\nExplicit PIPECAT_SAGEMAKER_BIDI_TTS_ENDPOINT_NAME; REGION defaults us-west-2.\nExactly one CONNECT_ADDRESS (numeric IP:8443, verified regional TLS) or LOOPBACK_ORIGIN\n(http://literal-loopback:port, fixture only) under PIPECAT_SAGEMAKER_BIDI_TTS_.\nOptional SETTINGS_JSON: voice_name, language_code, sample_rate_hz.\nOnly --run reads namespaced ACCESS_KEY_ID, SECRET_ACCESS_KEY, optional SESSION_TOKEN.\nHelp/check are side-effect free; no ambient AWS discovery, DNS, SDK, retry or replay.\nText strip/isalnum uses pinned Unicode 15. Skipped text opens no network session.\nOne joined H2 session per utterance; half-second initial buffer then streaming PCM.\nValid short initial tail is flushed on completion (deliberate Python difference).\nNo overwrite, symlink, traversal, input alias; finite audio buffered until joins, then written.\nReport on stderr distinguishes audio/no-audio/failure and local owner joins.\nNo physical playback, remote cancellation, provider quality or crash-durability claim.\n--timeout-ms: 100..300000 (default 30000); --max-output-bytes: 4096..8388608.\nDisk persistence can outlive network deadlines. No real credentials needed for fixtures.";
fn env(name: &str, max: usize) -> Result<Option<String>, &'static str> {
    match std::env::var(format!("PIPECAT_SAGEMAKER_BIDI_TTS_{name}")) {
        Ok(v) if v.len() <= max => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        _ => Err("invalid bounded environment value"),
    }
}
fn configuration(
    active: bool,
    total: Duration,
    max_output: usize,
) -> Result<SageMakerBidiTtsConfig, &'static str> {
    let region = env("REGION", 63)?.unwrap_or_else(|| "us-west-2".into());
    let endpoint = env("ENDPOINT_NAME", 63)?.ok_or("explicit endpoint required")?;
    let creds = if active {
        AwsCredentials::new(
            env("ACCESS_KEY_ID", 256)?.ok_or("explicit access key required")?,
            env("SECRET_ACCESS_KEY", 4096)?.ok_or("explicit secret key required")?,
            env("SESSION_TOKEN", 16384)?,
        )
    } else {
        AwsCredentials::new("OfflineCheckOnly", "synthetic-offline-check", None)
    }
    .map_err(|_| "invalid explicit credentials")?;
    let config = SageMakerBidiTtsConfig::new(region, endpoint, creds)
        .map_err(|_| "invalid endpoint configuration")?;
    let config = match (env("CONNECT_ADDRESS", 64)?, env("LOOPBACK_ORIGIN", 256)?) {
        (Some(a), None) => config.with_address(a.parse().map_err(|_| "numeric IP:8443 required")?),
        (None, Some(o)) => config.with_loopback_origin(&o),
        _ => return Err("exactly one explicit route required"),
    }
    .map_err(|_| "invalid explicit route")?;
    let settings = match env("SETTINGS_JSON", 4096)? {
        Some(s) => SynthesisSettings::from_json(s.as_bytes())
            .map_err(|_| "invalid immutable settings JSON")?,
        None => SynthesisSettings::default(),
    };
    let duplex = DuplexLimits {
        session_timeout: total,
        connect_timeout: total.min(Duration::from_secs(5)),
        write_timeout: total.min(Duration::from_secs(5)),
        idle_timeout: total,
        consumer_timeout: total.min(Duration::from_secs(5)),
        close_timeout: total.min(Duration::from_secs(3)),
        ..Default::default()
    };
    config
        .with_settings(settings)
        .and_then(|c| {
            c.with_limits(TtsLimits {
                duplex,
                output_timeout: total.min(Duration::from_secs(5)),
                max_output_bytes: max_output,
                app_timeout: total,
                interruption_grace: total.min(Duration::from_millis(500)),
                ..Default::default()
            })
        })
        .map_err(|_| "invalid finite configuration")
}
struct Options {
    input: PathBuf,
    output: PathBuf,
    timeout: Duration,
    max_output: usize,
}
fn options(args: &[OsString]) -> Result<Options, &'static str> {
    if args.len() < 3 || args.len() > 7 || args[0] != "--run" {
        return Err("use --run INPUT.json OUTPUT.pcm");
    }
    let (mut timeout, mut maximum) = (None, None);
    for pair in args[3..].chunks(2) {
        if pair.len() != 2 {
            return Err("missing option value");
        }
        let n = pair[1]
            .to_str()
            .filter(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()))
            .and_then(|s| s.parse::<usize>().ok())
            .ok_or("invalid numeric option")?;
        if pair[0] == "--timeout-ms" && timeout.is_none() && (100..=300000).contains(&n) {
            timeout = Some(n);
        } else if pair[0] == "--max-output-bytes"
            && maximum.is_none()
            && (4096..=MAX_OUTPUT).contains(&n)
        {
            maximum = Some(n);
        } else {
            return Err("unknown, duplicate or out-of-range option");
        }
    }
    Ok(Options {
        input: args[1].clone().into(),
        output: args[2].clone().into(),
        timeout: Duration::from_millis(timeout.unwrap_or(30000) as u64),
        max_output: maximum.unwrap_or(MAX_OUTPUT),
    })
}
struct Output {
    bytes: Mutex<Vec<u8>>,
    maximum: usize,
}
#[async_trait]
impl FrameProcessor for Output {
    fn name(&self) -> &str {
        "BoundedPcmRecording"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::TtsAudio(audio) = &frame.data {
            let mut bytes = self
                .bytes
                .lock()
                .map_err(|_| ProcessorError::fatal("output lock failed"))?;
            if audio.audio.len() > self.maximum.saturating_sub(bytes.len()) {
                return Err(ProcessorError::fatal("recording budget exceeded"));
            }
            bytes.extend_from_slice(&audio.audio);
        }
        ctx.push(frame, direction).await
    }
}
#[cfg(target_os = "linux")]
#[path = "sagemaker_bidi_tts/files.rs"]
mod files;
#[cfg(target_os = "linux")]
async fn active(options: Options) -> Result<bool, &'static str> {
    let config = configuration(true, options.timeout, options.max_output)?;
    let input = files::Input::read(&options.input)?;
    let target = files::Target::prepare(&options.output, &input)?;
    let output = Arc::new(Output {
        bytes: Mutex::new(Vec::with_capacity(options.max_output)),
        maximum: options.max_output,
    });
    let texts: Vec<String> =
        serde_json::from_slice(&input.bytes).map_err(|_| "input must be JSON utterance array")?;
    let app = SageMakerBidiTtsApp::new(config, texts, output.clone())
        .map_err(|_| "invalid finite utterance input")?;
    let mut recording = files::Recording::create(target, &input)?;
    let running = app.start().map_err(|_| "application startup failed")?;
    let report = running.finish().await;
    let joined = report.manager_joined
        && report.utterances.iter().all(|u| {
            u.pipeline_joined
                && (u.progress.skipped
                    || u.transport
                        .as_ref()
                        .is_some_and(|t| t.writer_joined && t.reader_joined && t.connection_joined))
        });
    let identity = input.verify().is_ok();
    let persisted = joined
        && recording
            .persist(&output.bytes.lock().map_err(|_| "output lock failed")?)
            .is_ok();
    let success = report.is_success() && identity && persisted;
    eprintln!(
        "{}",
        serde_json::json!({"success":success,"terminal":report.terminal,"network_joined":joined,"recorded":persisted,"input_identity_preserved":identity,"utterances":report.utterances.iter().map(|u|serde_json::json!({"context_id":u.context_id,"progress":u.progress,"pipeline_joined":u.pipeline_joined,"output_cleaned":u.output_cleaned,"output_cleanup_failed":u.output_cleanup_failed})).collect::<Vec<_>>(),"error":report.primary.as_ref().map(ToString::to_string)})
    );
    Ok(success)
}
#[cfg(not(target_os = "linux"))]
async fn active(_: Options) -> Result<bool, &'static str> {
    Err("race-resistant file execution requires Linux")
}
#[tokio::main]
async fn main() {
    let args: Vec<_> = std::env::args_os().skip(1).take(9).collect();
    let result = if args.is_empty() || (args.len() == 1 && args[0] == "--help") {
        println!("{HELP}");
        Ok(true)
    } else if args.len() == 1 && args[0] == "--check-config" {
        configuration(false, Duration::from_secs(30), MAX_OUTPUT).map(|_| {
            println!("configuration valid; no connection attempted");
            true
        })
    } else {
        match options(&args) {
            Ok(o) => active(o).await,
            Err(e) => Err(e),
        }
    };
    match result {
        Ok(true) => {}
        Ok(false) => std::process::exit(1),
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"success":false,"stage":"preflight_or_recording","error":error})
            );
            std::process::exit(2);
        }
    }
}
