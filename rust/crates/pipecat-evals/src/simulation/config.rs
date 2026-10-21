use crate::{
    bounded_yaml::{parse_yaml, ConstructionBudget},
    EvalError,
};
use pipecat_services::{ClientConfig, CompletionRequest, Context, LlmClient, Provider};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{collections::HashSet, time::Duration};

pub(super) const MAX_TEXT: usize = 32_768;
pub(super) const MAX_HISTORY: usize = 512 * 1024;
pub(super) const MAX_TOOLS: usize = 256;
pub(super) fn invalid(message: &str) -> EvalError {
    EvalError::Scenario(message.into())
}
fn turns() -> usize {
    20
}
fn duration() -> u64 {
    300_000
}
fn silence() -> u64 {
    30_000
}
fn closing() -> u64 {
    15_000
}
fn handshake() -> u64 {
    10_000
}
fn request() -> u64 {
    30_000
}

/// Strict native single-scenario schema. Unknown Python/audio/suite fields fail preflight.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationScenario {
    pub name: String,
    pub persona: String,
    pub goal: String,
    pub success: String,
    #[serde(default = "turns")]
    pub max_turns: usize,
    #[serde(default = "duration")]
    pub max_duration_ms: u64,
    #[serde(default = "silence")]
    pub max_silence_ms: u64,
    #[serde(default = "closing")]
    pub closing_timeout_ms: u64,
    #[serde(default = "handshake")]
    pub handshake_timeout_ms: u64,
    #[serde(default)]
    pub metrics: Vec<Metric>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "measure", rename_all = "snake_case", deny_unknown_fields)]
pub enum Metric {
    Turns {
        name: String,
        max: u64,
    },
    DurationMs {
        name: String,
        max: u64,
    },
    LongestBotReplyWords {
        name: String,
        max: u64,
    },
    SlowestTextResponseMs {
        name: String,
        max: u64,
    },
    FunctionCall {
        name: String,
        function: String,
        arguments: Value,
    },
}
impl Metric {
    pub fn name(&self) -> &str {
        match self {
            Self::Turns { name, .. }
            | Self::DurationMs { name, .. }
            | Self::LongestBotReplyWords { name, .. }
            | Self::SlowestTextResponseMs { name, .. }
            | Self::FunctionCall { name, .. } => name,
        }
    }
}
impl SimulationScenario {
    pub fn from_yaml(bytes: &[u8]) -> Result<Self, EvalError> {
        let scenario: Self = parse(bytes)?;
        scenario.validate()?;
        Ok(scenario)
    }
    pub fn validate(&self) -> Result<(), EvalError> {
        for value in [&self.name, &self.persona, &self.goal, &self.success] {
            if value.trim().is_empty() || value.len() > MAX_TEXT {
                return Err(invalid(
                    "simulation requires bounded nonempty name, persona, goal and success",
                ));
            }
        }
        if !(1..=100).contains(&self.max_turns)
            || !(1..=3_600_000).contains(&self.max_duration_ms)
            || !(1..=300_000).contains(&self.max_silence_ms)
            || !(1..=60_000).contains(&self.closing_timeout_ms)
            || !(1..=60_000).contains(&self.handshake_timeout_ms)
            || self.metrics.len() > 64
        {
            return Err(invalid("simulation bounds exceed native limits"));
        }
        let mut names = HashSet::new();
        for metric in &self.metrics {
            if metric.name().is_empty() || metric.name().len() > 128 || !names.insert(metric.name())
            {
                return Err(invalid("metric names must be unique, nonempty and bounded"));
            }
            if let Metric::FunctionCall {
                function,
                arguments,
                ..
            } = metric
            {
                if function.is_empty()
                    || function.len() > 256
                    || !arguments.is_object()
                    || serde_json::to_vec(arguments)?.len() > MAX_TEXT
                {
                    return Err(invalid(
                        "function metric requires bounded function and argument mapping",
                    ));
                }
            }
        }
        Ok(())
    }
}
/// Explicit OpenAI-compatible Chat Completions configuration; no inferred key name.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelConfig {
    pub endpoint: String,
    pub model: String,
    pub api_key_env: Option<String>,
    #[serde(default = "request")]
    pub timeout_ms: u64,
}
impl std::fmt::Debug for ModelConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ModelConfig")
            .field("timeout_ms", &self.timeout_ms)
            .finish_non_exhaustive()
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SimulationRuntime {
    pub caller: ModelConfig,
    pub judge: Option<ModelConfig>,
}
impl SimulationRuntime {
    pub fn from_yaml(bytes: &[u8]) -> Result<Self, EvalError> {
        let runtime: Self = parse(bytes)?;
        runtime.validate()?;
        Ok(runtime)
    }
    pub fn validate(&self) -> Result<(), EvalError> {
        self.caller.validate()?;
        if let Some(judge) = &self.judge {
            judge.validate()?;
        }
        Ok(())
    }
}
impl ModelConfig {
    pub fn validate(&self) -> Result<(), EvalError> {
        if self.endpoint.len() > 4096
            || self.model.trim().is_empty()
            || self.model.len() > 256
            || !(1..=300_000).contains(&self.timeout_ms)
        {
            return Err(invalid("invalid simulation model configuration"));
        }
        if self.api_key_env.as_ref().is_some_and(|name| {
            name.is_empty()
                || name.len() > 128
                || !name.bytes().enumerate().all(|(i, c)| {
                    c == b'_' || c.is_ascii_alphabetic() || (i > 0 && c.is_ascii_digit())
                })
        }) {
            return Err(invalid("invalid credential environment variable name"));
        }
        // Pure request validation: no environment reads or requests.
        let client = LlmClient::new(self.client_config(None))
            .map_err(|_| invalid("invalid simulation model configuration"))?;
        client
            .request_body(&CompletionRequest::new(&self.model, Context::default()))
            .map_err(|_| invalid("invalid simulation model endpoint or model"))?;
        Ok(())
    }
    fn client_config(&self, key: Option<String>) -> ClientConfig {
        let mut config = ClientConfig::new(Provider::OpenAi, "");
        config.endpoint = self.endpoint.clone();
        config.api_key = key;
        config.request_timeout = Duration::from_millis(self.timeout_ms);
        config.idle_timeout = config.request_timeout;
        config.max_event_bytes = MAX_TEXT;
        config.channel_capacity = 16;
        config
    }
    pub(super) fn build(&self) -> Result<LlmClient, EvalError> {
        let key = self
            .api_key_env
            .as_ref()
            .map(|name| {
                std::env::var(name).map_err(|_| {
                    invalid("selected simulation credential is missing or not Unicode")
                })
            })
            .transpose()?;
        LlmClient::new(self.client_config(key))
            .map_err(|_| invalid("simulation client initialization failed"))
    }
}
pub(super) fn parse<T: serde::de::DeserializeOwned>(bytes: &[u8]) -> Result<T, EvalError> {
    if bytes.len() > 1024 * 1024 {
        return Err(EvalError::Limit("simulation input bytes"));
    }
    serde_yaml::from_value(parse_yaml(bytes, &mut ConstructionBudget::default(), 0)?)
        .map_err(|error| EvalError::Scenario(error.to_string()))
}
