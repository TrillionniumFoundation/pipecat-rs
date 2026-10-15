//! Explicit trusted-local configuration for the spoken-response CLI/library loader.
use crate::{
    recorded_scenario::read_regular, BotTranscriptionConfig, EvalError, EvalSession,
    RecordedScenarioFile, ScenarioFile, SessionOptions,
};
use pipecat_speech::{SpeechClient, SpeechConfig, TranscriptionRequest};
use pipecat_vad::{RuntimeOptions, SileroModel, SileroVadAnalyzer, VadParams};
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};

const MAX_CONFIG_BYTES: usize = 32 * 1024;
const MAX_SCENARIOS: usize = 16;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSettings {
    base_url: String,
    model: String,
    api_key_env: String,
    onnx_runtime: PathBuf,
    silero_model: PathBuf,
    language: Option<String>,
    #[serde(default = "timeout_ms")]
    timeout_ms: u64,
}
fn timeout_ms() -> u64 {
    8_000
}

/// A bounded JSON configuration with explicit endpoint, credential environment
/// variable and local native model/runtime paths. No downloads or default remote
/// service are selected. Secrets are intentionally not Debug/Serialize values.
pub struct BotTranscriptionSettings {
    speech: SpeechConfig,
    request: TranscriptionRequest,
    runtime: PathBuf,
    model: PathBuf,
}
impl BotTranscriptionSettings {
    /// Read at most 32 KiB from a trusted-local regular JSON file (Unix).
    /// Relative model/runtime paths use the canonical configuration target's directory.
    /// Only the explicitly named credential environment variable is read.
    pub fn load(path: impl AsRef<Path>) -> Result<Self, EvalError> {
        let path = path.as_ref().canonicalize()?;
        let root = path
            .parent()
            .ok_or_else(|| invalid("configuration directory"))?;
        let bytes = read_regular(&path, root, MAX_CONFIG_BYTES)?;
        // Deserialize directly into a strict struct: duplicate and unknown keys
        // are rejected rather than silently overwritten through Value.
        let raw: FileSettings = serde_json::from_slice(&bytes)
            .map_err(|_| invalid("bot transcription configuration JSON fields or types"))?;
        if raw.base_url.len() > 4096
            || raw.model.trim().is_empty()
            || raw.model.len() > 256
            || raw.model.chars().any(char::is_control)
            || raw
                .language
                .as_ref()
                .is_some_and(|v| v.is_empty() || v.len() > 64 || v.chars().any(char::is_control))
            || raw.timeout_ms == 0
            || raw.timeout_ms > 30_000
            || raw.api_key_env.is_empty()
            || raw.api_key_env.len() > 128
            || !raw
                .api_key_env
                .bytes()
                .enumerate()
                .all(|(i, c)| c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit()))
        {
            return Err(invalid("bot transcription configuration limits"));
        }
        let key = std::env::var(&raw.api_key_env).map_err(|_| {
            invalid("bot transcription credential environment variable is missing or not Unicode")
        })?;
        if key.is_empty() || key.len() > 4096 || key.chars().any(char::is_control) {
            return Err(invalid("bot transcription credential is invalid"));
        }
        let speech = SpeechConfig {
            base_url: raw.base_url,
            api_key: key,
            timeout: Duration::from_millis(raw.timeout_ms),
            max_audio_bytes: 2 * 1024 * 1024,
            max_json_bytes: 64 * 1024,
            queue_capacity: 1,
        };
        // Reuse the speech client's HTTPS/loopback, no-userinfo/query/fragment,
        // no-redirect and credential checks. Construction performs no request.
        SpeechClient::new(speech.clone())
            .map_err(|_| invalid("bot transcription endpoint or credential is invalid"))?;
        let runtime = checked_asset(&root.join(raw.onnx_runtime), 128 * 1024 * 1024)?;
        let model = checked_asset(&root.join(raw.silero_model), 16 * 1024 * 1024)?;
        Ok(Self {
            speech,
            request: TranscriptionRequest {
                model: raw.model,
                language: raw.language,
                prompt: None,
            },
            runtime,
            model,
        })
    }

    /// Prepare every scenario, recording and model before any connection.
    /// Explicit `response` events remain spoken transcripts; `llm_response`
    /// remains reported text. At most sixteen independent model instances are
    /// retained. Recorded input uses the existing outer-scenario-directory rules.
    pub fn into_sessions(
        self,
        scenario_path: impl AsRef<Path>,
        url: &str,
        options: SessionOptions,
        recorded_audio: bool,
    ) -> Result<Vec<EvalSession>, EvalError> {
        let parts = if recorded_audio {
            RecordedScenarioFile::load_audio_response(scenario_path)?.into_parts()
        } else {
            ScenarioFile::load_audio_response(scenario_path)?
                .scenarios
                .into_iter()
                .map(|scenario| (scenario, Vec::new()))
                .collect()
        };
        if parts.is_empty() || parts.len() > MAX_SCENARIOS {
            return Err(EvalError::Limit("bot transcription scenarios (1..16)"));
        }
        if options.stop_bot && parts.len() > 1 {
            return Err(invalid("--stop-bot with several scenarios needs independent bot endpoints; use separate runs"));
        }
        // Validate the whole collection, including URL/options and recordings,
        // before the first native model is instantiated. These temporary
        // sessions have no socket, analyzer thread or HTTP request.
        for (scenario, clips) in &parts {
            EvalSession::new(
                scenario.audio_response_validation_copy()?,
                url,
                options.clone(),
            )?
            .with_recorded_audio_inputs(clips.iter().cloned())?;
        }
        let runtime = RuntimeOptions {
            library_path: self.runtime,
        };
        let mut sessions = Vec::with_capacity(parts.len());
        for (scenario, clips) in parts {
            let mut model = SileroModel::load(&runtime, &self.model).map_err(|_| {
                invalid("bot transcription native runtime or model could not be loaded")
            })?;
            // Exercise actual input/output shapes before connecting, then start
            // each independent analyzer from clean recurrent state.
            let probability = model
                .predict_pcm16(&[0; 512], 16_000)
                .map_err(|_| invalid("bot transcription model preflight inference failed"))?;
            if !probability.is_finite() || !(0.0..=1.0).contains(&probability) {
                return Err(invalid(
                    "bot transcription model preflight probability is invalid",
                ));
            }
            model.reset();
            let analyzer = SileroVadAnalyzer::new(
                model,
                16_000,
                VadParams {
                    stop_secs: 0.4,
                    ..Default::default()
                },
            )
            .map_err(|_| invalid("bot transcription analyzer configuration is invalid"))?;
            let config =
                BotTranscriptionConfig::new(analyzer, self.speech.clone(), self.request.clone())?;
            sessions.push(
                EvalSession::with_bot_transcription(scenario, url, options.clone(), config)?
                    .with_recorded_audio_inputs(clips)?,
            );
        }
        Ok(sessions)
    }
}
fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}

fn checked_asset(path: &Path, limit: u64) -> Result<PathBuf, EvalError> {
    let path = path.canonicalize()?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        let file = std::fs::OpenOptions::new()
            .read(true)
            .custom_flags(libc::O_NONBLOCK | libc::O_NOFOLLOW)
            .open(&path)?;
        let metadata = file.metadata()?;
        if !metadata.is_file() || metadata.len() == 0 || metadata.len() > limit {
            return Err(invalid(
                "bot transcription assets must be bounded regular files",
            ));
        }
        Ok(path)
    }
    #[cfg(not(unix))]
    {
        let _ = (path, limit);
        Err(EvalError::Unsupported(
            "bot transcription file configuration requires Unix regular-file semantics".into(),
        ))
    }
}
