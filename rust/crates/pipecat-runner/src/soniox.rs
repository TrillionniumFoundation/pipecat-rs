//! Typed, WebSocket-only Soniox → OpenAI → Soniox composition.
use crate::{RunnerError, VoiceCascade};
use pipecat_core::{FrameProcessor, ProcessorError};
use pipecat_processors::tools::ToolExecutor;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, Provider, ToolDefinition,
};
use pipecat_soniox::{
    SonioxSttClient, SonioxSttConfig, SonioxSttProcessor, SonioxTtsClient, SonioxTtsConfig,
    SonioxTtsProcessor,
};
use std::{fmt, sync::Arc};

/// Application tools for one session. Construct a new executor and any mutable
/// callback state on each factory call. Callbacks must be cancellation-safe.
pub struct SonioxSessionTools {
    pub definitions: Vec<ToolDefinition>,
    pub executor: ToolExecutor,
}
/// Synchronous, nonblocking construction only; inference and callbacks run in
/// the existing bounded pipeline and ToolExecutor owners.
pub type SonioxToolsFactory =
    Arc<dyn Fn() -> Result<SonioxSessionTools, ProcessorError> + Send + Sync>;

#[derive(Clone)]
pub struct SonioxVoiceConfig {
    pub stt: SonioxSttConfig,
    pub tts: SonioxTtsConfig,
    pub llm: ClientConfig,
    pub model: String,
    pub system_prompt: String,
    pub session_tools: Option<SonioxToolsFactory>,
}
impl fmt::Debug for SonioxVoiceConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SonioxVoiceConfig")
            .field("stt", &self.stt)
            .field("tts", &self.tts)
            .field("llm", &self.llm)
            .field("session_tools", &self.session_tools.is_some())
            .finish_non_exhaustive()
    }
}
use crate::cascade::SessionParts;
impl SonioxVoiceConfig {
    pub fn new(soniox_key: impl Into<String>, openai_key: impl Into<String>) -> Self {
        let key = soniox_key.into();
        let mut stt = SonioxSttConfig::new(key.clone());
        stt.vad_force_turn_endpoint = false;
        Self {
            stt,
            tts: SonioxTtsConfig::new(key),
            llm: ClientConfig::new(Provider::OpenAi, openai_key),
            model: "gpt-4.1-mini".into(),
            system_prompt: pipecat_cli::config::DEFAULT_SYSTEM.into(),
            session_tools: None,
        }
    }
    pub fn from_env() -> Result<Self, RunnerError> {
        Self::from_lookup(|key| std::env::var(key).ok())
    }
    /// Reads only this composition's credentials. Endpoints remain explicit
    /// typed overrides for applications and local fixtures, never browser input.
    pub fn from_lookup(mut get: impl FnMut(&str) -> Option<String>) -> Result<Self, RunnerError> {
        // Apply the same conflict checks as the CLI, without requiring a selector
        // for applications that explicitly call this typed constructor.
        VoiceCascade::from_lookup(|key| {
            if key == "PIPECAT_VOICE_CASCADE" {
                Some("soniox".into())
            } else {
                get(key)
            }
        })?;
        let key = required(
            &mut get,
            "SONIOX_API_KEY",
            "Soniox cascade requires SONIOX_API_KEY",
        )?;
        let openai = required(
            &mut get,
            "OPENAI_API_KEY",
            "Soniox cascade requires OPENAI_API_KEY",
        )?;
        let mut config = Self::new(key, openai);
        if let Some(value) = get("SONIOX_STT_MODEL") {
            config.stt.settings.model = value;
        }
        if let Some(value) = get("SONIOX_TTS_MODEL") {
            config.tts.settings.model = value;
        }
        if let Some(value) = get("SONIOX_VOICE") {
            config.tts.settings.voice = value;
        }
        if let Some(value) = get("OPENAI_MODEL") {
            config.model = value;
        }
        if let Some(value) = get("PIPECAT_SYSTEM_PROMPT") {
            config.system_prompt = value;
        }
        config.stt.sample_rate = rate(get("PIPECAT_INPUT_SAMPLE_RATE"), 16000)?;
        config.tts.sample_rate = rate(get("PIPECAT_OUTPUT_SAMPLE_RATE"), 24000)?;
        config.validate()?;
        Ok(config)
    }
    pub fn validate(&self) -> Result<(), RunnerError> {
        if self.stt.api_key.trim().is_empty() || self.tts.api_key.trim().is_empty() {
            return Err(RunnerError::CascadeConfiguration(
                "Soniox credentials must be nonempty",
            ));
        }
        if self.stt.num_channels != 1 || self.stt.vad_force_turn_endpoint {
            return Err(RunnerError::CascadeConfiguration(
                "Soniox cascade requires mono audio and provider endpoint detection",
            ));
        }
        self.stt
            .validate()
            .map_err(|_| RunnerError::CascadeConfiguration("invalid Soniox STT configuration"))?;
        self.tts
            .validate()
            .map_err(|_| RunnerError::CascadeConfiguration("invalid Soniox TTS configuration"))?;
        if self.llm.provider != Provider::OpenAi
            || self.llm.openai_preset.is_some()
            || self.llm.azure.is_some()
        {
            return Err(RunnerError::CascadeConfiguration(
                "Soniox cascade requires the native OpenAI chat provider",
            ));
        }
        if self
            .llm
            .api_key
            .as_ref()
            .is_none_or(|key| key.trim().is_empty())
        {
            return Err(RunnerError::CascadeConfiguration(
                "OpenAI credentials must be nonempty",
            ));
        }
        if self.model.trim().is_empty()
            || self.model.len() > 1024
            || self.system_prompt.trim().is_empty()
            || self.system_prompt.len() > 64 * 1024
        {
            return Err(RunnerError::CascadeConfiguration(
                "OpenAI model and system prompt must be nonempty and bounded",
            ));
        }
        let client = LlmClient::new(self.llm.clone()).map_err(|_| {
            RunnerError::CascadeConfiguration("invalid OpenAI client configuration")
        })?;
        client
            .request_body(&CompletionRequest::new(
                self.model.clone(),
                Context::default(),
            ))
            .map_err(|_| {
                RunnerError::CascadeConfiguration("invalid OpenAI request configuration")
            })?;
        Ok(())
    }
    pub(crate) fn build_session(&self) -> Result<SessionParts, RunnerError> {
        let stt = SonioxSttProcessor::new(
            SonioxSttClient::new(self.stt.clone()).map_err(|_| RunnerError::Provider)?,
        );
        let user = stt.recommended_user_config();
        let tts = SonioxTtsProcessor::new(
            SonioxTtsClient::new(self.tts.clone()).map_err(|_| RunnerError::Provider)?,
        );
        let mut context = Context::default();
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![];
        if let Some(factory) = &self.session_tools {
            let tools = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| factory()))
                .map_err(|_| {
                    RunnerError::CascadeConfiguration("session tool construction panicked")
                })?
                .map_err(|_| {
                    RunnerError::CascadeConfiguration("session tool construction failed")
                })?;
            if tools.definitions.len() > 32
                || serde_json::to_vec(&tools.definitions)
                    .map_err(|_| {
                        RunnerError::CascadeConfiguration("invalid session tool definitions")
                    })?
                    .len()
                    > 64 * 1024
            {
                return Err(RunnerError::CascadeConfiguration(
                    "session tool definitions exceed limits",
                ));
            }
            context.tools = Some(tools.definitions);
            context.validate().map_err(|_| {
                RunnerError::CascadeConfiguration("invalid session tool definitions")
            })?;
            processors.push(Arc::new(tools.executor));
        }
        Ok(SessionParts {
            stt: Arc::new(stt),
            tts: Arc::new(tts),
            llm: self.llm.clone(),
            user_config: user,
            initial_context: context,
            tools: processors,
            model: self.model.clone(),
            system_prompt: self.system_prompt.clone(),
        })
    }
}
fn required(
    get: &mut impl FnMut(&str) -> Option<String>,
    key: &str,
    error: &'static str,
) -> Result<String, RunnerError> {
    get(key)
        .filter(|value| !value.trim().is_empty())
        .ok_or(RunnerError::CascadeConfiguration(error))
}
fn rate(value: Option<String>, default: u32) -> Result<u32, RunnerError> {
    value.map_or(Ok(default), |value| {
        value
            .parse::<u32>()
            .ok()
            .filter(|rate| (8000..=48000).contains(rate))
            .ok_or(RunnerError::CascadeConfiguration(
                "sample rate must be an integer from 8000 to 48000",
            ))
    })
}
