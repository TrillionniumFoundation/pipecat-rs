//! Explicit finite JSON utterances through the public SageMakerTtsApp.
//! PCM acceptance into bounded memory and final file recording are distinct.
use pipecat_aws_common::AwsCredentials;
use pipecat_aws_sagemaker::{
    SageMakerConfig, SageMakerLimits, SageMakerReport, SageMakerTtsApp, SynthesisSettings,
    Utterance,
};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
};
use serde::{de, Deserialize, Deserializer};
use std::{
    ffi::OsString,
    fs::{File, OpenOptions},
    io::{Read, Write},
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    sync::{Arc, Mutex},
    time::Duration,
};

const MAX_INPUT: usize = 1024 * 1024;
const MAX_OUTPUT: usize = 16 * 1024 * 1024;
const MAX_UTTERANCES: usize = 64;
const MAX_ARGS: usize = 7;
const HELP: &str = "SageMaker finite NVIDIA Magpie text-to-PCM (Linux only)\n\
Usage: sagemaker_tts [--help | --check-config]\n\
  sagemaker_tts --run INPUT.json OUTPUT.pcm\n\
    [--timeout-ms N] [--max-output-bytes N]\n\
No arguments and help read no configuration, credentials or files and connect\n\
nowhere. Check validates explicit nonsecret configuration with synthetic\n\
credentials, without inspecting credential environment variables or any files.\n\
Configuration: PIPECAT_SAGEMAKER_REGION, PIPECAT_SAGEMAKER_ENDPOINT_NAME and\n\
exactly one PIPECAT_SAGEMAKER_CONNECT_ADDRESS (numeric IP:443) or\n\
PIPECAT_SAGEMAKER_LOOPBACK_ORIGIN (http[s]://literal-loopback:port fixture only).\n\
Only --run reads PIPECAT_SAGEMAKER_ACCESS_KEY_ID,\n\
PIPECAT_SAGEMAKER_SECRET_ACCESS_KEY and optional PIPECAT_SAGEMAKER_SESSION_TOKEN.\n\
No ambient AWS credentials, profiles, metadata, DNS, redirects or retries.\n\
Input: nonempty regular non-symlink UTF-8 JSON, at most 1 MiB, for example\n\
{\"utterances\":[{\"text\":\"Hello\"},{\"text\":\"World\"}]}. Optional top-level\n\
voice_name, language_code and sample_rate_hz select immutable synthesis settings.\n\
Defaults: Magpie-Multilingual.EN-US.Aria, en-US, 24000 Hz. Unknown/duplicate\n\
fields, null settings, more than 64 utterances and invalid settings fail before\n\
any connection or output creation. Whitespace/no-alphanumeric entries skip.\n\
Output is a NEW mode-0600 regular file containing mono signed PCM16 little-endian\n\
at the requested rate. It is never overwritten. Symlinks, parent traversals\n\
and aliases are rejected. The complete input and configuration are preflighted.\n\
PCM is accepted into bounded preallocated memory while the application runs.\n\
Only after all application/network owners join is it persisted through the\n\
retained original file descriptor. Filesystem persistence is outside the\n\
network deadline and may block; no crash durability or physical playback claim.\n\
--timeout-ms is 100..120000, default 30000. --max-output-bytes must be even,\n\
2..16777216, default 16777216. Both provider and output are finitely bounded.\n\
The sanitized terminal JSON goes to stderr, never the PCM file. It distinguishes\n\
complete audio, all_skipped, partial output, memory delivery and recorded bytes.\n\
Failure retains the identified partial file. Zero exit requires successful\n\
application completion, real owner joins and final file recording.\n\
No live AWS/model acceptance, remote cancellation, speech playback, sentence\n\
aggregation, bidirectional streaming or full voice-bot parity is claimed.";

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct InputDocument {
    #[serde(default, deserialize_with = "present")]
    voice_name: Option<String>,
    #[serde(default, deserialize_with = "present")]
    language_code: Option<String>,
    #[serde(default, deserialize_with = "present")]
    sample_rate_hz: Option<u32>,
    #[serde(deserialize_with = "utterances")]
    utterances: Vec<Utterance>,
}
fn present<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    T::deserialize(deserializer).map(Some)
}
fn utterances<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Vec<Utterance>, D::Error> {
    struct Finite;
    impl<'de> de::Visitor<'de> for Finite {
        type Value = Vec<Utterance>;
        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("a nonempty bounded array of complete utterances")
        }
        fn visit_seq<A: de::SeqAccess<'de>>(self, mut seq: A) -> Result<Self::Value, A::Error> {
            let mut result = Vec::with_capacity(MAX_UTTERANCES);
            while let Some(utterance) = seq.next_element()? {
                if result.len() == MAX_UTTERANCES {
                    return Err(de::Error::custom("too many utterances"));
                }
                result.push(utterance);
            }
            if result.is_empty() {
                return Err(de::Error::custom("utterances must not be empty"));
            }
            Ok(result)
        }
    }
    deserializer.deserialize_seq(Finite)
}
impl InputDocument {
    fn into_parts(self) -> (SynthesisSettings, Vec<Utterance>) {
        let mut settings = SynthesisSettings::default();
        if let Some(voice) = self.voice_name {
            settings.voice_name = voice;
        }
        if let Some(language) = self.language_code {
            settings.language_code = language;
        }
        if let Some(rate) = self.sample_rate_hz {
            settings.sample_rate_hz = rate;
        }
        (settings, self.utterances)
    }
}
struct Options {
    input: PathBuf,
    output: PathBuf,
    timeout: Duration,
    max_output: usize,
}
fn number(value: &OsString, min: usize, max: usize) -> Result<usize, &'static str> {
    let value = value.to_str().ok_or("numeric argument is not UTF-8")?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err("invalid numeric argument");
    }
    value
        .parse::<usize>()
        .ok()
        .filter(|n| (min..=max).contains(n))
        .ok_or("numeric argument outside supported bounds")
}
fn options(args: &[OsString]) -> Result<Options, &'static str> {
    if args.len() < 3 || args[0] != "--run" {
        return Err("active execution requires --run INPUT.json OUTPUT.pcm; use --help");
    }
    let (mut timeout, mut max_output) = (None, None);
    for pair in args[3..].chunks(2) {
        let [name, value] = pair else {
            return Err("option requires a value");
        };
        match name.to_str() {
            Some("--timeout-ms") if timeout.is_none() => {
                timeout = Some(number(value, 100, 120_000)?);
            }
            Some("--max-output-bytes") if max_output.is_none() => {
                let value = number(value, 2, MAX_OUTPUT)?;
                if !value.is_multiple_of(2) {
                    return Err("output bound must contain whole PCM16 samples");
                }
                max_output = Some(value);
            }
            _ => return Err("unknown or duplicate option; use --help"),
        }
    }
    Ok(Options {
        input: PathBuf::from(&args[1]),
        output: PathBuf::from(&args[2]),
        timeout: Duration::from_millis(timeout.unwrap_or(30_000) as u64),
        max_output: max_output.unwrap_or(MAX_OUTPUT),
    })
}
fn environment(name: &str, max: usize) -> Result<Option<String>, &'static str> {
    match std::env::var_os(format!("PIPECAT_SAGEMAKER_{name}")) {
        None => Ok(None),
        Some(value) if value.is_empty() || value.len() > max => {
            Err("explicit SageMaker environment is empty or exceeds its bound")
        }
        Some(value) => value
            .into_string()
            .map(Some)
            .map_err(|_| "explicit SageMaker environment is not UTF-8"),
    }
}
fn configuration(
    active: bool,
    timeout: Duration,
    max_output: usize,
) -> Result<SageMakerConfig, &'static str> {
    let region = environment("REGION", 63)?.ok_or("explicit SageMaker region is required")?;
    let endpoint =
        environment("ENDPOINT_NAME", 63)?.ok_or("explicit SageMaker endpoint name is required")?;
    // Credential variables are not inspected at all for default/help/check.
    let credentials = if active {
        AwsCredentials::new(
            environment("ACCESS_KEY_ID", 256)?
                .ok_or("explicit SageMaker access key is required")?,
            environment("SECRET_ACCESS_KEY", 4096)?
                .ok_or("explicit SageMaker secret key is required")?,
            environment("SESSION_TOKEN", 16384)?,
        )
    } else {
        AwsCredentials::new("OfflineCheckOnly", "offline-check-not-a-secret", None)
    }
    .map_err(|_| "invalid explicit credentials")?;
    let config = SageMakerConfig::new(region, endpoint, credentials)
        .map_err(|_| "invalid SageMaker identity")?;
    let address = environment("CONNECT_ADDRESS", 64)?;
    let loopback = environment("LOOPBACK_ORIGIN", 256)?;
    let config = match (address, loopback) {
        (Some(address), None) => config
            .with_connect_address(
                address
                    .parse::<SocketAddr>()
                    .map_err(|_| "numeric IP:443 required")?,
            )
            .map_err(|_| "invalid hosted connect address")?,
        (None, Some(origin)) => config
            .with_loopback_endpoint(&origin)
            .map_err(|_| "invalid literal-loopback origin")?,
        _ => return Err("exactly one explicit connect address or loopback origin is required"),
    };
    let defaults = SageMakerLimits::default();
    let limits = SageMakerLimits {
        max_utterances: MAX_UTTERANCES,
        max_output_bytes: max_output,
        max_frame_bytes: defaults.max_frame_bytes.min(max_output),
        connect_timeout: timeout.min(Duration::from_secs(10)),
        write_timeout: timeout.min(Duration::from_secs(5)),
        idle_timeout: timeout,
        output_timeout: timeout.min(Duration::from_secs(5)),
        total_timeout: timeout,
        shutdown_timeout: Duration::from_secs(2),
        ..defaults
    };
    let config = config
        .with_limits(limits)
        .map_err(|_| "invalid finite SageMaker limits")?;
    config
        .validate()
        .map_err(|_| "invalid SageMaker configuration")?;
    Ok(config)
}

#[cfg(target_os = "linux")]
#[path = "sagemaker_tts/files.rs"]
mod files;

#[cfg(target_os = "linux")]
async fn active(options: Options) -> Result<bool, &'static str> {
    let config = configuration(true, options.timeout, options.max_output)?;
    let input = files::Input::read(&options.input)?;
    let target = files::Target::prepare(&options.output, &input)?;
    let document: InputDocument = serde_json::from_slice(&input.bytes)
        .map_err(|_| "malformed UTF-8 JSON or unsupported bounded input schema")?;
    let (settings, utterances) = document.into_parts();
    let rate = settings.sample_rate_hz;
    let config = config
        .with_settings(settings)
        .map_err(|_| "invalid synthesis settings")?;
    let output = Arc::new(files::PcmOutput::new(
        options.max_output,
        rate,
        config.limits().max_frame_bytes,
    ));
    // The public application constructor preflights EVERY utterance and settings
    // before creating any file or attempting any connection.
    let app = SageMakerTtsApp::new(config, utterances, output.clone())
        .map_err(|_| "invalid or excessive complete utterance input")?;
    if output.create(target, &input).is_err() {
        output.report(
            false,
            "output_create",
            None,
            false,
            false,
            input.verify().is_ok(),
        );
        return Ok(false);
    }
    let running = match app.start().await {
        Ok(running) => running,
        Err(_) => {
            output.report(false, "startup", None, false, false, input.verify().is_ok());
            return Ok(false);
        }
    };
    let report = running.finish().await;
    let joined = report.pipeline_joined
        && !report.join_unproven
        && report.progress.admitted == report.progress.owners_released;
    let finalized = joined && output.persist().is_ok();
    let input_unchanged = input.verify().is_ok();
    let success = report.is_success() && joined && finalized && input_unchanged;
    let stage = if !joined {
        "owners"
    } else if !finalized {
        "persistence"
    } else if !input_unchanged {
        "input_identity"
    } else if success {
        "joined"
    } else {
        "terminal"
    };
    output.report(
        success,
        stage,
        Some(&report),
        finalized,
        joined,
        input_unchanged,
    );
    Ok(success)
}
#[cfg(not(target_os = "linux"))]
async fn active(_: Options) -> Result<bool, &'static str> {
    Err("race-resistant JSON-to-recorded-PCM execution requires Linux")
}
async fn run(args: Vec<OsString>) -> Result<bool, &'static str> {
    if args.len() > MAX_ARGS || args.iter().any(|a| a.is_empty() || a.len() > 4096) {
        return Err("invalid bounded arguments; use --help");
    }
    if args.is_empty() {
        println!("disabled; use --help for explicit finite SageMaker synthesis");
        return Ok(true);
    }
    if matches!(args.as_slice(), [a] if a == "--help") {
        println!("{HELP}");
        return Ok(true);
    }
    if matches!(args.as_slice(), [a] if a == "--check-config") {
        configuration(false, Duration::from_secs(30), MAX_OUTPUT)?;
        println!("configuration valid; no credentials read, network or file effects");
        return Ok(true);
    }
    active(options(&args)?).await
}
#[tokio::main(flavor = "current_thread")]
async fn main() {
    let args = std::env::args_os().skip(1).take(MAX_ARGS + 1).collect();
    let success = match run(args).await {
        Ok(success) => success,
        Err(error) => {
            eprintln!(
                "{}",
                serde_json::json!({"status":"rejected", "stage":"preflight", "error":error})
            );
            false
        }
    };
    if !success {
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn whole_json_schema_is_strict_and_bounded() {
        for value in [
            r#"{"utterances":[]}"#,
            r#"{"utterances":[{"text":"ok","url":"https://invalid"}]}"#,
            r#"{"utterances":[{"text":null}]}"#,
            r#"{"utterances":[{"text":1}]}"#,
            r#"{"utterances":[{"text":"ok"}],"settings":{}}"#,
            r#"{"utterances":[{"text":"ok"}],"voice_name":null}"#,
            r#"{"utterances":[{"text":"ok"}],"sample_rate_hz":24000.5}"#,
            r#"{"utterances":[{"text":"ok"}],"language_code":"en-US","language_code":"de-DE"}"#,
            r#"{"utterances":[{"text":"ok","text":"duplicate"}]}"#,
        ] {
            assert!(serde_json::from_str::<InputDocument>(value).is_err());
        }
        let value = serde_json::json!({"utterances":(0..65).map(|_| serde_json::json!({"text":"ok"})).collect::<Vec<_>>()});
        assert!(serde_json::from_value::<InputDocument>(value).is_err());
    }
    #[test]
    fn defaults_and_explicit_settings_are_preserved() {
        let document: InputDocument =
            serde_json::from_str(r#"{"utterances":[{"text":" hello "}]}"#).unwrap();
        let (settings, utterances) = document.into_parts();
        assert_eq!(settings.voice_name, "Magpie-Multilingual.EN-US.Aria");
        assert_eq!(settings.language_code, "en-US");
        assert_eq!(settings.sample_rate_hz, 24_000);
        assert_eq!(utterances[0].text, " hello ");
        let document: InputDocument = serde_json::from_str(r#"{"voice_name":"voice","language_code":"de-DE","sample_rate_hz":16000,"utterances":[{"text":"Hallo"}]}"#).unwrap();
        let (settings, _) = document.into_parts();
        assert_eq!(settings.voice_name, "voice");
        assert_eq!(settings.language_code, "de-DE");
        assert_eq!(settings.sample_rate_hz, 16_000);
    }
}
