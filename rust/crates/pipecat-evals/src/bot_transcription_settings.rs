//! Explicit trusted-local configuration for the spoken-response CLI/library loader.
use crate::{
    native_loader::{load_native, LoaderRegistrar, LoaderRegistry},
    recorded_scenario::read_regular,
    BotTranscriptionConfig, Cancellation, EvalError, EvalSession, RecordedAudio,
    RecordedScenarioFile, ScenarioFile, ScriptScenario, SessionOptions,
};
use pipecat_speech::{SpeechClient, SpeechConfig, TranscriptionRequest};
use pipecat_vad::{
    RuntimeOptions, SileroModel, SileroVadAnalyzer, SmartTurnAnalyzer, SmartTurnModel,
    SmartTurnParams, VadParams,
};
use serde::Deserialize;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

const MAX_CONFIG_BYTES: usize = 32 * 1024;
const MAX_SCENARIOS: usize = 16;
const MAX_SMART_TURN_MODEL_BYTES: u64 = 128 * 1024 * 1024;
type ScenarioParts = Vec<(ScriptScenario, Vec<(usize, RecordedAudio)>)>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileSettings {
    base_url: String,
    model: String,
    api_key_env: String,
    onnx_runtime: PathBuf,
    silero_model: PathBuf,
    smart_turn_model: Option<PathBuf>,
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
    smart_turn_model: Option<PathBuf>,
    #[cfg(test)]
    test_loaders: Option<TestLoaders>,
}

#[cfg(test)]
struct TestLoaders {
    preflight: Box<dyn Fn() -> Result<(), EvalError> + Send + Sync>,
    attempt: Box<dyn Fn() -> Result<BotTranscriptionConfig, EvalError> + Send + Sync>,
}
impl BotTranscriptionSettings {
    /// The joint finite profile uses Silero plus speech-timeout aggregation.
    /// Reject the optional graph before entering any owned native loader.
    #[cfg(feature = "user-speech")]
    pub(crate) fn validate_audio_to_audio(&self) -> Result<(), EvalError> {
        if self.smart_turn_model.is_some() {
            return Err(EvalError::Unsupported(
                "audio-to-audio scenarios do not support Smart Turn".into(),
            ));
        }
        Ok(())
    }

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
        let smart_turn_model = raw
            .smart_turn_model
            .map(|path| checked_asset(&root.join(path), MAX_SMART_TURN_MODEL_BYTES))
            .transpose()?;
        Ok(Self {
            speech,
            request: TranscriptionRequest {
                model: raw.model,
                language: raw.language,
                prompt: None,
            },
            runtime,
            model,
            smart_turn_model,
            #[cfg(test)]
            test_loaders: None,
        })
    }

    /// Prepare every scenario, recording and model before any connection.
    /// Explicit `response` events remain spoken transcripts; `llm_response`
    /// remains reported text. At most sixteen independent model sets are
    /// retained, each with Silero and an optional Smart Turn model. Recorded
    /// input uses the existing outer-scenario-directory rules.
    pub fn into_sessions(
        self,
        scenario_path: impl AsRef<Path>,
        url: &str,
        options: SessionOptions,
        recorded_audio: bool,
    ) -> Result<Vec<EvalSession>, EvalError> {
        let parts = prepare_scenarios(scenario_path, url, &options, recorded_audio)?;
        let mut sessions = Vec::with_capacity(parts.len());
        for (scenario, clips) in parts {
            let config = self.build_config()?;
            sessions.push(
                EvalSession::with_bot_transcription(scenario, url, options.clone(), config)?
                    .with_recorded_audio_inputs(clips)?,
            );
        }
        Ok(sessions)
    }

    /// Prepare all scenarios and recordings, then load models sequentially on
    /// owned native threads without blocking Tokio while waiting. At most
    /// sixteen model sets (loading or retained in sessions) belong to this call.
    ///
    /// Cancellation prevents further admission and discards prepared sessions,
    /// but an already-entered native call must finish and be joined first. This
    /// is scheduling isolation, not a hard cancellation or startup deadline.
    /// Externally dropping this future synchronously joins any admitted loader.
    pub async fn into_sessions_with_cancellation(
        self,
        scenario_path: impl AsRef<Path>,
        url: &str,
        options: SessionOptions,
        recorded_audio: bool,
        cancel: Cancellation,
    ) -> Result<Vec<EvalSession>, EvalError> {
        if cancel.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        // The complete collection is validated before entering native code.
        let parts = prepare_scenarios(scenario_path, url, &options, recorded_audio)?;
        let settings = Arc::new(self);
        let loaders = LoaderRegistry::new();
        let registrar = loaders.registrar();
        let mut sessions = Vec::with_capacity(parts.len());
        for (scenario, clips) in parts {
            let Some(config) = settings
                .clone()
                .build_config_owned(&cancel, &registrar)
                .await?
            else {
                // Returning drops previously prepared configurations only after
                // the outstanding loader has actually been joined.
                return Err(EvalError::Cancelled);
            };
            if cancel.is_cancelled() {
                return Err(EvalError::Cancelled);
            }
            sessions.push(
                EvalSession::with_bot_transcription(scenario, url, options.clone(), config)?
                    .with_recorded_audio_inputs(clips)?,
            );
        }
        if cancel.is_cancelled() {
            return Err(EvalError::Cancelled);
        }
        Ok(sessions)
    }

    /// The invocation owns the registry through preflight and every attempt.
    pub(crate) async fn build_config_owned(
        self: Arc<Self>,
        cancel: &Cancellation,
        loaders: &LoaderRegistrar,
    ) -> Result<Option<BotTranscriptionConfig>, EvalError> {
        load_native(cancel, loaders, move || self.build_config()).await
    }

    /// Run model validation and destruction on the same owned native thread.
    /// No unused preflight configuration is returned to a Tokio worker.
    pub(crate) async fn preflight_owned(
        self: Arc<Self>,
        cancel: &Cancellation,
        loaders: &LoaderRegistrar,
    ) -> Result<Option<()>, EvalError> {
        load_native(cancel, loaders, move || {
            #[cfg(test)]
            if let Some(test) = &self.test_loaders {
                return (test.preflight)();
            }
            self.build_config().map(drop)
        })
        .await
    }

    #[cfg(test)]
    pub(crate) fn for_native_loader_test(
        preflight: impl Fn() -> Result<(), EvalError> + Send + Sync + 'static,
        attempt: impl Fn() -> Result<BotTranscriptionConfig, EvalError> + Send + Sync + 'static,
    ) -> Self {
        Self {
            speech: SpeechConfig {
                base_url: "http://127.0.0.1:1/v1".into(),
                api_key: "test-only".into(),
                timeout: Duration::from_secs(1),
                max_audio_bytes: 1024,
                max_json_bytes: 1024,
                queue_capacity: 1,
            },
            request: TranscriptionRequest::default(),
            runtime: PathBuf::new(),
            model: PathBuf::new(),
            smart_turn_model: None,
            test_loaders: Some(TestLoaders {
                preflight: Box::new(preflight),
                attempt: Box::new(attempt),
            }),
        }
    }

    #[cfg(all(test, feature = "user-speech"))]
    pub(crate) fn with_smart_turn_for_test(mut self) -> Self {
        self.smart_turn_model = Some(PathBuf::from("never-open-smart-turn.onnx"));
        self
    }

    /// Build fresh models with checked inference and clean recurrent state.
    /// Native loading/inference is synchronous and has no hard deadline. The
    /// caller owns the result; no background loader or analyzer is started here.
    pub(crate) fn build_config(&self) -> Result<BotTranscriptionConfig, EvalError> {
        #[cfg(test)]
        if let Some(test) = &self.test_loaders {
            return (test.attempt)();
        }
        // Suite attempts may start long after settings were read. Reject
        // disappeared, nonregular or oversized assets again before native code
        // opens them. These trusted-local path checks are not a TOCTOU sandbox.
        let runtime = RuntimeOptions {
            library_path: checked_asset(&self.runtime, 128 * 1024 * 1024)?,
        };
        let model_path = checked_asset(&self.model, 16 * 1024 * 1024)?;
        let smart_turn_path = self
            .smart_turn_model
            .as_ref()
            .map(|path| checked_asset(path, MAX_SMART_TURN_MODEL_BYTES))
            .transpose()?;
        let mut model = SileroModel::load(&runtime, &model_path).map_err(|_| {
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
        let Some(path) = smart_turn_path else {
            return Ok(config);
        };
        // Use the same explicit runtime and owned loading thread as Silero.
        // The real graph is probed before any session can connect; constructing
        // the analyzer does not start its inference worker.
        let model = SmartTurnModel::load(&runtime, path)
            .map_err(|_| invalid("bot transcription Smart Turn model could not be loaded"))?;
        let prediction = model.predict_pcm16(&[0; 512], 16_000).map_err(|_| {
            invalid("bot transcription Smart Turn model preflight inference failed")
        })?;
        if !prediction.probability.is_finite() || !(0.0..=1.0).contains(&prediction.probability) {
            return Err(invalid(
                "bot transcription Smart Turn model preflight probability is invalid",
            ));
        }
        let analyzer = SmartTurnAnalyzer::new(model, SmartTurnParams::default()).map_err(|_| {
            invalid("bot transcription Smart Turn analyzer configuration is invalid")
        })?;
        config.with_smart_turn(analyzer)
    }
}

fn prepare_scenarios(
    scenario_path: impl AsRef<Path>,
    url: &str,
    options: &SessionOptions,
    recorded_audio: bool,
) -> Result<ScenarioParts, EvalError> {
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
        return Err(invalid(
            "--stop-bot with several scenarios needs independent bot endpoints; use separate runs",
        ));
    }
    // These validation copies create no socket, analyzer thread or HTTP request.
    for (scenario, clips) in &parts {
        EvalSession::new(
            scenario.audio_response_validation_copy()?,
            url,
            options.clone(),
        )?
        .with_recorded_audio_inputs(clips.iter().cloned())?;
    }
    Ok(parts)
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

#[cfg(all(test, feature = "user-speech"))]
mod combined_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn combined_smart_turn_rejection_precedes_any_native_entry() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let calls = attempts.clone();
        let mut settings = BotTranscriptionSettings::for_native_loader_test(
            || unreachable!(),
            move || {
                calls.fetch_add(1, Ordering::SeqCst);
                Err(EvalError::Scenario("synthetic native entry".into()))
            },
        );
        assert!(settings.validate_audio_to_audio().is_ok());
        settings.smart_turn_model = Some(PathBuf::from("never-open-smart-turn.onnx"));
        assert!(matches!(
            settings.validate_audio_to_audio(),
            Err(EvalError::Unsupported(message)) if message.contains("Smart Turn")
        ));
        assert_eq!(attempts.load(Ordering::SeqCst), 0);
    }
}
