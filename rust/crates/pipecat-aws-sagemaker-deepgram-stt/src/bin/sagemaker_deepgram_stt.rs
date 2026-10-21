//! Real finite-file application; all filesystem persistence follows network joins.
use pipecat_aws_common::AwsCredentials;
use pipecat_aws_sagemaker_deepgram_stt::{
    DuplexLimits, RunManifest, SageMakerSttApp, SageMakerSttConfig, SttLimits,
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
const MAX_INPUT: usize = 8 * 1024 * 1024;
const MAX_OUTPUT: usize = 8 * 1024 * 1024;
const HELP: &str = "Finite Deepgram SageMaker BiDi STT (Linux file execution)
Usage: sagemaker_deepgram_stt [--help | --check-config]
       sagemaker_deepgram_stt --run INPUT.pcm OUTPUT.jsonl [--settings SETTINGS.json]
Explicit PIPECAT_SAGEMAKER_DEEPGRAM_ENDPOINT_NAME; REGION defaults us-west-2.
Exactly one CONNECT_ADDRESS (numeric IP:8443, verified regional TLS) or LOOPBACK_ORIGIN
(http://literal-loopback:port, fixture only), all under PIPECAT_SAGEMAKER_DEEPGRAM_.
Only --run reads namespaced ACCESS_KEY_ID, SECRET_ACCESS_KEY, optional SESSION_TOKEN.
No ambient credentials, DNS, retry/reconnect or hosted validation claim.
Help/check-config read no files and perform no network or real-credential access.
Settings file is a strict <=16384-byte object with settings and timeline keys.
Timeline markers: after_chunk (>=1), pause_ms (0..15000), vad_stopped (boolean).
At most64 increasing unique markers and60000ms total pause. No implicit final VAD.
Raw mono little-endian PCM16, not WAV. Default nova-3, en, 16000Hz,3200-byte chunks.
Maximum120 seconds,8MiB,2400 chunks. Pauses use real time and are cancellable.
No startup event. KeepAlive starts5s after accepted response head. VAD sends Finalize.
End sends CloseStream and a signed empty close. Final is not necessarily finalized.
New mode0600 JSONL only, no overwrite, symlinks, traversal, aliases or settings/input alias.
Output buffering <=8MiB; persistence only after retained network/pipeline joins.
No fsync/crash-durability promise. Disk I/O can outlive network deadlines.
Default connect/head/write/close10s, response-idle90s, absolute session300s.
No full Python/SDK/service parity, microphone, accuracy, billing or exactly-once claim.";
fn env(name: &str, max: usize) -> Result<Option<String>, &'static str> {
    match std::env::var(format!("PIPECAT_SAGEMAKER_DEEPGRAM_{name}")) {
        Ok(v) if v.len() <= max => Ok(Some(v)),
        Err(std::env::VarError::NotPresent) => Ok(None),
        _ => Err("invalid bounded environment value"),
    }
}
fn configuration(
    active: bool,
    total: Duration,
    max_output: usize,
) -> Result<SageMakerSttConfig, &'static str> {
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
    let config = SageMakerSttConfig::new(region, endpoint, creds)
        .map_err(|_| "invalid endpoint configuration")?;
    let config = match (env("CONNECT_ADDRESS", 64)?, env("LOOPBACK_ORIGIN", 256)?) {
        (Some(a), None) => config.with_address(a.parse().map_err(|_| "numeric IP:8443 required")?),
        (None, Some(o)) => config.with_loopback_origin(&o),
        _ => return Err("exactly one explicit route required"),
    }
    .map_err(|_| "invalid explicit route")?;
    let duplex = DuplexLimits {
        session_timeout: total,
        ..Default::default()
    };
    config
        .with_limits(SttLimits {
            duplex,
            max_output_bytes: max_output - 2048,
            ..Default::default()
        })
        .map_err(|_| "invalid finite configuration")
}
struct Options {
    input: PathBuf,
    output: PathBuf,
    settings: Option<PathBuf>,
}
fn options(args: &[OsString]) -> Result<Options, &'static str> {
    if !(args.len() == 3 || args.len() == 5)
        || args[0] != "--run"
        || (args.len() == 5 && args[3] != "--settings")
    {
        return Err("use --run INPUT.pcm OUTPUT.jsonl [--settings SETTINGS.json]");
    }
    Ok(Options {
        input: args[1].clone().into(),
        output: args[2].clone().into(),
        settings: args.get(4).map(|p| PathBuf::from(p.clone())),
    })
}

struct Output {
    bytes: Mutex<Vec<u8>>,
    maximum: usize,
}
impl Output {
    fn record(&self, value: serde_json::Value) -> Result<(), ProcessorError> {
        let mut record =
            serde_json::to_vec(&value).map_err(|_| ProcessorError::fatal("JSONL encode failed"))?;
        record.push(b'\n');
        let mut bytes = self
            .bytes
            .lock()
            .map_err(|_| ProcessorError::fatal("output lock failed"))?;
        if bytes.len() + record.len() > self.maximum {
            return Err(ProcessorError::fatal("JSONL budget exceeded"));
        }
        bytes.extend_from_slice(&record);
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for Output {
    fn name(&self) -> &str {
        "BoundedTranscriptJsonl"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data{FrameData::TranscriptionDetailed(f)|FrameData::InterimTranscriptionDetailed(f)=>self.record(serde_json::json!({"kind":if matches!(&frame.data,FrameData::TranscriptionDetailed(_)){"final"}else{"interim"},"text":f.text.text,"user_id":f.user_id,"language":f.language,"timestamp":f.timestamp,"finalized":f.finalized,"result":f.result}))?,_=>{}}
        ctx.push(frame, direction).await
    }
}
#[cfg(target_os = "linux")]
#[path = "sagemaker_deepgram_stt/files.rs"]
mod files;
#[cfg(target_os = "linux")]
async fn active(options: Options) -> Result<bool, &'static str> {
    let config = configuration(true, Duration::from_secs(300), MAX_OUTPUT)?;
    let input = files::Input::read(&options.input)?;
    let settings = options
        .settings
        .as_ref()
        .map(|path| files::Input::read_bounded(path, 16384))
        .transpose()?;
    if settings.as_ref().is_some_and(|s| s.aliases(&input)) {
        return Err("settings/input alias rejected");
    }
    let manifest = settings
        .as_ref()
        .map(|s| RunManifest::from_json(&s.bytes))
        .transpose()
        .map_err(|_| "invalid strict run manifest")?
        .unwrap_or_default();
    let config = config
        .with_settings(manifest.settings)
        .map_err(|_| "invalid immutable settings")?;
    let target = files::Target::prepare(&options.output, &input)?;
    let output = Arc::new(Output {
        bytes: Mutex::new(Vec::with_capacity(MAX_OUTPUT)),
        maximum: MAX_OUTPUT,
    });
    let app = SageMakerSttApp::new(config, input.bytes.clone(), output.clone())
        .and_then(|app| app.with_timeline(manifest.timeline))
        .map_err(|_| "invalid finite PCM input")?;
    if let Some(s) = &settings {
        s.verify()?;
    }
    let mut recording = files::Recording::create(target, &input)?;
    let running = app.start().map_err(|_| "application startup failed")?;
    let report = running.finish().await;
    let joined = report.pipeline_joined
        && report
            .transport
            .as_ref()
            .is_some_and(|r| r.writer_joined && r.reader_joined && r.connection_joined);
    let identity = input.verify().is_ok() && settings.as_ref().is_none_or(|s| s.verify().is_ok());
    output.record(serde_json::json!({"kind":"application_summary","terminal":report.terminal,"progress":report.progress,"pipeline_joined":report.pipeline_joined,"network_joined":joined,"output_cleaned":report.output_cleaned,"input_identity_preserved":identity,"error":report.primary.as_ref().map(ToString::to_string)})).map_err(|_|"terminal record budget exceeded")?;
    let persisted = joined
        && recording
            .persist(&output.bytes.lock().map_err(|_| "output lock failed")?)
            .is_ok();
    let success = report.is_success() && identity && persisted;
    eprintln!(
        "{}",
        serde_json::json!({"success":success,"terminal":report.terminal,"network_joined":joined,"recorded":persisted,"input_identity_preserved":identity,"bytes_persisted":recording.persisted_bytes(),"saved_final_frames":if persisted {Some(report.progress.delivered_final_frames)} else {None},"saved_interim_frames":if persisted {Some(report.progress.delivered_interim_frames)} else {None}})
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
        configuration(false, Duration::from_secs(300), MAX_OUTPUT).map(|_| {
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
