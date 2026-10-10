// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{InputItem, ResponsesContext, ServiceError, ToolChoice};
use pipecat_core::{LlmSettings, Setting};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningConfig {
    pub effort: Option<String>,
    pub summary: Option<String>,
    pub mode: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResponsesSettings {
    #[serde(default)]
    pub llm: LlmSettings,
    #[serde(default)]
    pub max_completion_tokens: Setting<u64>,
    #[serde(default)]
    pub reasoning: Setting<ReasoningConfig>,
    #[serde(default)]
    pub service_tier: Setting<String>,
}
impl ResponsesSettings {
    /// Decode provider-specific runtime fields carried in common LLM extras.
    /// Unknown extras remain present and are rejected by request validation.
    pub fn from_llm(delta: &LlmSettings) -> Result<Self, ServiceError> {
        fn parse<T: serde::de::DeserializeOwned>(value: Value) -> Result<Setting<T>, ServiceError> {
            if value.is_null() {
                return Ok(Setting::Null);
            }
            serde_json::from_value(value)
                .map(Setting::Value)
                .map_err(|_| invalid("invalid Responses runtime setting"))
        }
        let mut result = Self {
            llm: delta.clone(),
            ..Default::default()
        };
        if let Some(value) = result.llm.base.extra.remove("max_completion_tokens") {
            result.max_completion_tokens = parse(value)?;
        }
        if let Some(value) = result.llm.base.extra.remove("reasoning") {
            result.reasoning = parse(value)?;
        }
        if let Some(value) = result.llm.base.extra.remove("service_tier") {
            result.service_tier = parse(value)?;
        }
        Ok(result)
    }
    pub fn merge(&mut self, delta: &Self) {
        pipecat_services::settings::merge_settings(&mut self.llm, &delta.llm);
        if !matches!(delta.max_completion_tokens, Setting::NotGiven) {
            self.max_completion_tokens = delta.max_completion_tokens.clone();
        }
        if !matches!(delta.reasoning, Setting::NotGiven) {
            self.reasoning = delta.reasoning.clone();
        }
        if !matches!(delta.service_tier, Setting::NotGiven) {
            self.service_tier = delta.service_tier.clone();
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponsesRequest {
    pub model: String,
    pub context: ResponsesContext,
    #[serde(default)]
    pub settings: ResponsesSettings,
    /// JSON schema enforcement is sent using Responses text.format.
    pub response_schema: Option<Value>,
}
impl ResponsesRequest {
    pub fn new(model: impl Into<String>, context: ResponsesContext) -> Self {
        Self {
            model: model.into(),
            context,
            settings: ResponsesSettings::default(),
            response_schema: None,
        }
    }
    pub fn effective_model(&self) -> Result<&str, ServiceError> {
        let model = match &self.settings.llm.base.model {
            Setting::NotGiven => &self.model,
            Setting::Value(model) => model,
            Setting::Null => return Err(invalid("model cannot be null")),
        };
        if model.is_empty() || model.len() > 256 || model.chars().any(char::is_control) {
            return Err(invalid("invalid model"));
        }
        Ok(model)
    }
    /// Pure serialization of an immutable effective snapshot.
    pub fn body(&self, streaming: bool) -> Result<Value, ServiceError> {
        let model = self.effective_model()?;
        let settings = &self.settings.llm;
        if matches!(settings.filter_incomplete_user_turns, Setting::Value(true))
            || matches!(settings.user_turn_completion_config, Setting::Value(_))
        {
            return Err(ServiceError::Unsupported(
                "incomplete user-turn filtering requires a turn-completion gate".into(),
            ));
        }
        for (field, present) in [
            (
                "max_tokens (use max_completion_tokens)",
                matches!(settings.max_tokens, Setting::Value(_)),
            ),
            ("top_k", matches!(settings.top_k, Setting::Value(_))),
            (
                "frequency_penalty",
                matches!(settings.frequency_penalty, Setting::Value(_)),
            ),
            (
                "presence_penalty",
                matches!(settings.presence_penalty, Setting::Value(_)),
            ),
            ("seed", matches!(settings.seed, Setting::Value(_))),
        ] {
            if present {
                return Err(ServiceError::Unsupported(format!("Responses {field}")));
            }
        }
        let mut call_ids = std::collections::HashSet::new();
        let mut reasoning_ids = std::collections::HashSet::new();
        for item in &self.context.items {
            match item {
                InputItem::FunctionCall(call) if !call_ids.insert(&call.id) => {
                    return Err(invalid("duplicate function call id"))
                }
                InputItem::Reasoning(item) if !reasoning_ids.insert(&item.id) => {
                    return Err(invalid("duplicate reasoning item id"))
                }
                _ => {}
            }
        }
        let mut input = self
            .context
            .items
            .iter()
            .map(InputItem::wire)
            .collect::<Result<Vec<_>, _>>()?;
        let mut body = Map::new();
        if let Setting::Value(instruction) = &settings.system_instruction {
            if !instruction.is_empty() {
                if input.is_empty() {
                    input.push(json!({"role":"developer","content":instruction}));
                } else {
                    body.insert("instructions".into(), json!(instruction));
                }
            }
        }
        body.extend([
            ("model".into(), json!(model)),
            ("input".into(), json!(input)),
            ("store".into(), json!(false)),
            ("stream".into(), json!(streaming)),
        ]);
        for (name, setting, max) in [
            ("temperature", &settings.temperature, 2.0),
            ("top_p", &settings.top_p, 1.0),
        ] {
            if let Setting::Value(value) = setting {
                if !value.is_finite() || !(0.0..=max).contains(value) {
                    return Err(invalid("sampling parameter out of range"));
                }
                body.insert(name.into(), json!(value));
            }
        }
        if let Setting::Value(tokens) = self.settings.max_completion_tokens {
            if tokens == 0 {
                return Err(invalid("max_completion_tokens must be positive"));
            }
            body.insert("max_output_tokens".into(), json!(tokens));
        }
        if let Setting::Value(tier) = &self.settings.service_tier {
            if tier.is_empty() {
                return Err(invalid("service_tier cannot be empty"));
            }
            body.insert("service_tier".into(), json!(tier));
        }
        if let Some(tools) = &self.context.tools {
            let mut names = std::collections::HashSet::new();
            let mut wire = vec![];
            for tool in tools {
                if tool.name.is_empty() || !tool.parameters.is_object() || !names.insert(&tool.name)
                {
                    return Err(invalid("tools require unique names and object schemas"));
                }
                wire.push(json!({"type":"function","name":tool.name,"description":tool.description,"parameters":tool.parameters,"strict":null}));
            }
            body.insert("tools".into(), json!(wire));
        }
        if let Some(choice) = &self.context.tool_choice {
            let choice = match choice {
                ToolChoice::Auto => json!("auto"),
                ToolChoice::None => json!("none"),
                ToolChoice::Required => json!("required"),
                ToolChoice::Function(name) => {
                    if !self
                        .context
                        .tools
                        .as_ref()
                        .is_some_and(|tools| tools.iter().any(|tool| &tool.name == name))
                    {
                        return Err(invalid("tool choice names an undeclared function"));
                    }
                    json!({"type":"function","name":name})
                }
            };
            body.insert("tool_choice".into(), choice);
        }
        let reasoning = match &self.settings.reasoning {
            Setting::Value(reasoning) => {
                let mut value = Map::new();
                for (name, setting) in [
                    ("effort", &reasoning.effort),
                    ("summary", &reasoning.summary),
                    ("mode", &reasoning.mode),
                ] {
                    if let Some(setting) = setting {
                        if setting.is_empty() {
                            return Err(invalid("reasoning configuration strings cannot be empty"));
                        }
                        value.insert(name.into(), json!(setting));
                    }
                }
                value
            }
            _ => Map::new(),
        };
        if !reasoning.is_empty() {
            if model_supports_reasoning(model) == Some(false) {
                return Err(ServiceError::Unsupported(
                    "reasoning is configured for a known non-reasoning model".into(),
                ));
            }
            body.insert("reasoning".into(), Value::Object(reasoning));
            body.insert("include".into(), json!(["reasoning.encrypted_content"]));
        } else if let Some(effort) = default_reasoning_effort(model) {
            body.insert("reasoning".into(), json!({"effort":effort}));
        }
        if let Some(schema) = &self.response_schema {
            if !model_supports_response_schema(model) {
                return Err(ServiceError::Unsupported(
                    "response schema is unsupported by this model".into(),
                ));
            }
            if !schema.is_object() {
                return Err(invalid("response_schema must be an object"));
            }
            body.insert("text".into(), json!({"format":{"type":"json_schema","name":"response","schema":schema,"strict":true}}));
        }
        // Only options with implemented wire semantics are accepted. Ownership,
        // storage, tools, input and continuation cannot be replaced by extras.
        for (key, value) in &settings.base.extra {
            match key.as_str() {
                "parallel_tool_calls" if value.is_boolean() => {},
                "truncation" if matches!(value.as_str(), Some("auto" | "disabled")) => {},
                _ => return Err(ServiceError::Unsupported("Responses extra setting (only parallel_tool_calls and truncation are supported)".into())),
            }
            body.insert(key.clone(), value.clone());
        }
        Ok(Value::Object(body))
    }
}
fn invalid(message: &str) -> ServiceError {
    ServiceError::InvalidRequest(message.into())
}
/// Source-pinned classification; unrecognized model families remain unknown.
pub fn model_supports_reasoning(model: &str) -> Option<bool> {
    let model = model.to_ascii_lowercase();
    if model.starts_with('o') && model.as_bytes().get(1).is_some_and(u8::is_ascii_digit) {
        return Some(true);
    }
    let suffix = model.strip_prefix("gpt-")?;
    let major: String = suffix.chars().take_while(char::is_ascii_digit).collect();
    major
        .parse::<u32>()
        .ok()
        .map(|major| major >= 5 && !model.contains("chat"))
}
pub fn default_reasoning_effort(model: &str) -> Option<&'static str> {
    if model_supports_reasoning(model) != Some(true) {
        return None;
    }
    let model = model.to_ascii_lowercase();
    if model.starts_with('o')
        || model.starts_with("gpt-6-astra")
        || model.starts_with("gpt-6.1-sol")
    {
        return None;
    }
    let name = if model.len() >= 11 {
        let suffix = &model.as_bytes()[model.len() - 11..];
        if suffix[0] == b'-'
            && suffix[5] == b'-'
            && suffix[8] == b'-'
            && suffix
                .iter()
                .enumerate()
                .all(|(i, b)| [0, 5, 8].contains(&i) || b.is_ascii_digit())
        {
            &model[..model.len() - 11]
        } else {
            &model
        }
    } else {
        &model
    };
    if name.ends_with("-pro") {
        None
    } else if ["gpt-5", "gpt-5-mini", "gpt-5-nano"].contains(&name) {
        Some("minimal")
    } else {
        Some("none")
    }
}

/// Source-pinned response schema exclusions. Unknown models defer validation to
/// the configured provider rather than being classified as known unsupported.
pub fn model_supports_response_schema(model: &str) -> bool {
    !(model.starts_with("gpt-3.5")
        || model == "gpt-4"
        || model.starts_with("gpt-4-")
        || model.starts_with("gpt-4o-2024-05-13")
        || model.starts_with("chatgpt-4o")
        || model.starts_with("o1-mini")
        || model.starts_with("o1-preview"))
}
