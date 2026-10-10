//! Sparse Bedrock settings are carried through the shared LLM settings contract.
use pipecat_core::{LlmSettings, Setting};
use pipecat_services::{CompletionRequest, ServiceError};
use serde_json::{json, Map, Value};
#[derive(Debug, Clone)]
pub struct BedrockSettings {
    pub model: String,
    pub system_instruction: Option<String>,
    pub inference: Map<String, Value>,
    pub latency: Option<String>,
    pub effort: Option<String>,
    pub prompt_caching: bool,
    pub additional: Value,
}
pub(crate) fn invalid(message: &str) -> ServiceError {
    ServiceError::InvalidRequest(message.into())
}
impl BedrockSettings {
    pub fn resolve(request: &CompletionRequest) -> Result<Self, ServiceError> {
        let s = &request.settings;
        let model = match &s.base.model {
            Setting::Value(v) => v.clone(),
            Setting::Null => return Err(invalid("Bedrock model cannot be null")),
            Setting::NotGiven => request.model.clone(),
        };
        if model.is_empty()
            || model.len() > 2048
            || !model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-._:/".contains(&b))
            || model.split('/').any(|p| p == "." || p == "..")
        {
            return Err(invalid("invalid Bedrock model identifier"));
        }
        let system_instruction = match &s.system_instruction {
            Setting::Value(v) => Some(v.clone()),
            Setting::Null => None,
            Setting::NotGiven => request.system_instruction.clone(),
        };
        let mut inference = Map::new();
        let max = match s.max_tokens {
            Setting::Value(v) => Some(v),
            Setting::Null => None,
            Setting::NotGiven => request.max_tokens.map(u64::from),
        };
        if let Some(n) = max {
            if n == 0 || n > u32::MAX as u64 {
                return Err(invalid("invalid Bedrock max_tokens"));
            }
            inference.insert("maxTokens".into(), json!(n));
        }
        let temperature = match s.temperature {
            Setting::Value(v) => Some(v),
            Setting::Null => None,
            Setting::NotGiven => request.temperature,
        };
        for (name, value) in [
            ("temperature", temperature),
            (
                "topP",
                match s.top_p {
                    Setting::Value(v) => Some(v),
                    _ => None,
                },
            ),
        ] {
            if let Some(v) = value {
                if !v.is_finite() || !(0.0..=1.0).contains(&v) {
                    return Err(invalid(
                        "Bedrock sampling values must be between zero and one",
                    ));
                }
                inference.insert(name.into(), json!(v));
            }
        }
        if matches!(s.top_k, Setting::Value(_))
            || matches!(s.frequency_penalty, Setting::Value(_))
            || matches!(s.presence_penalty, Setting::Value(_))
            || matches!(s.seed, Setting::Value(_))
            || matches!(s.filter_incomplete_user_turns, Setting::Value(true))
            || matches!(s.user_turn_completion_config, Setting::Value(_))
        {
            return Err(ServiceError::Unsupported(
                "Bedrock setting requires a model-specific field or turn gate".into(),
            ));
        }
        let mut extras = request.extra.clone();
        extras.extend(s.base.extra.clone());
        let mut value = Self {
            model,
            system_instruction,
            inference,
            latency: None,
            effort: None,
            prompt_caching: false,
            additional: json!({}),
        };
        for (key, item) in extras {
            if item.is_null() {
                continue;
            }
            match key.as_str() {
                "stop_sequences" => {
                    let seq = item
                        .as_array()
                        .ok_or_else(|| invalid("stop_sequences must be an array"))?;
                    if seq.len() > 8192
                        || seq
                            .iter()
                            .any(|v| v.as_str().is_none_or(|s| s.is_empty() || s.len() > 1024))
                    {
                        return Err(invalid("invalid stop_sequences"));
                    }
                    if !seq.is_empty() {
                        value.inference.insert("stopSequences".into(), item);
                    }
                }
                "latency" => {
                    let v = item
                        .as_str()
                        .filter(|v| matches!(*v, "standard" | "optimized"))
                        .ok_or_else(|| invalid("invalid Bedrock latency"))?;
                    value.latency = Some(v.into());
                }
                "effort" => {
                    let v = item
                        .as_str()
                        .filter(|v| matches!(*v, "low" | "medium" | "high" | "xhigh" | "max"))
                        .ok_or_else(|| invalid("invalid Bedrock effort"))?;
                    value.effort = Some(v.into());
                }
                "enable_prompt_caching" => {
                    value.prompt_caching = item
                        .as_bool()
                        .ok_or_else(|| invalid("prompt caching must be boolean"))?
                }
                "additional_model_request_fields" => {
                    if !item.is_object() {
                        return Err(invalid("additional model fields must be an object"));
                    }
                    value.additional = item;
                }
                _ => return Err(ServiceError::Unsupported("unknown Bedrock setting".into())),
            }
        }
        Ok(value)
    }
    pub fn defaults() -> LlmSettings {
        let mut s = LlmSettings::default();
        s.base.model = Setting::Value("us.amazon.nova-lite-v1:0".into());
        s
    }
    pub fn inject_trailing_user(&self) -> bool {
        self.model.contains("claude")
            && ![
                "claude-2",
                "claude-instant",
                "claude-3",
                "claude-opus-4-0",
                "claude-opus-4-1",
                "claude-sonnet-4-0",
                "claude-sonnet-4-5",
                "claude-haiku-4-5",
            ]
            .iter()
            .any(|p| self.model.contains(p))
    }
}
