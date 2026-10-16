//! Source-backed request presets for OpenAI-compatible Chat Completions.
//!
//! A preset selects Python service defaults and request transformations; it is
//! not a complete provider integration. Realtime APIs, provider SDKs, automatic
//! tool execution, and provider-specific response metadata are outside this API.
//! Sarvam adds model capabilities, routing and headers. NVIDIA selects its native
//! reasoning stream adapter. Azure has a
//! dedicated `AzureConfig` transport rather than a generic endpoint alias.

use crate::{adapters, CompletionRequest, Context, Provider, ServiceError, Usage};
use pipecat_core::Setting;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

/// The OpenAI service and its distinct compatible subclasses in
/// `src/pipecat/services/*/llm.py`. The deprecated `grok` module re-exports `xai`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenAiPreset {
    OpenAi,
    Azure,
    Baseten,
    Cerebras,
    Crusoe,
    DeepSeek,
    Fireworks,
    Groq,
    Inception,
    Mistral,
    Nebius,
    Novita,
    Nvidia,
    Ollama,
    OpenRouter,
    Perplexity,
    Qwen,
    SambaNova,
    Sarvam,
    Together,
    Xai,
}

impl OpenAiPreset {
    pub const ALL: [Self; 21] = [
        Self::OpenAi,
        Self::Azure,
        Self::Baseten,
        Self::Cerebras,
        Self::Crusoe,
        Self::DeepSeek,
        Self::Fireworks,
        Self::Groq,
        Self::Inception,
        Self::Mistral,
        Self::Nebius,
        Self::Novita,
        Self::Nvidia,
        Self::Ollama,
        Self::OpenRouter,
        Self::Perplexity,
        Self::Qwen,
        Self::SambaNova,
        Self::Sarvam,
        Self::Together,
        Self::Xai,
    ];

    pub const fn default_model(self) -> &'static str {
        match self {
            Self::OpenAi | Self::Azure => "gpt-4.1",
            Self::Baseten => "zai-org/GLM-5.3",
            Self::Cerebras => "gpt-oss-120b",
            Self::Crusoe | Self::Groq => "openai/gpt-oss-120b",
            Self::DeepSeek => "deepseek-flash",
            Self::Fireworks => "accounts/fireworks/models/nemotron-3-ultra-nvfp4",
            Self::Inception => "mercury-2",
            Self::Mistral => "mistral-small-latest",
            Self::Nebius => "Qwen/Qwen3-30B-A3B-Instruct-2507",
            Self::Novita => "moonshotai/kimi-k2.5",
            Self::Nvidia => "nvidia/nemotron-3-super-120b-a12b",
            Self::Ollama => "llama2",
            Self::OpenRouter => "openai/gpt-4.1",
            Self::Perplexity => "sonar",
            Self::Qwen => "qwen-plus",
            Self::SambaNova => "Meta-Llama-3.3-70B-Instruct",
            Self::Sarvam => "sarvam-105b",
            Self::Together => "zai-org/GLM-5.2",
            Self::Xai => "grok-4.20-non-reasoning",
        }
    }

    /// Exact Chat Completions URL, not an API base URL. Sarvam's recorded URL
    /// applies to its default model; its client resolves a version template.
    pub const fn default_endpoint(self) -> Option<&'static str> {
        Some(match self {
            Self::OpenAi => "https://api.openai.com/v1/chat/completions",
            Self::Azure => return None,
            Self::Baseten => "https://inference.baseten.co/v1/chat/completions",
            Self::Cerebras => "https://api.cerebras.ai/v1/chat/completions",
            Self::Crusoe => "https://api.inference.crusoecloud.com/v1/chat/completions",
            Self::DeepSeek => "https://api.deepseek.com/v1/chat/completions",
            Self::Fireworks => "https://api.fireworks.ai/inference/v1/chat/completions",
            Self::Groq => "https://api.groq.com/openai/v1/chat/completions",
            Self::Inception => "https://api.inceptionlabs.ai/v1/chat/completions",
            Self::Mistral => "https://api.mistral.ai/v1/chat/completions",
            Self::Nebius => "https://api.tokenfactory.nebius.com/v1/chat/completions",
            Self::Novita => "https://api.novita.ai/openai/chat/completions",
            Self::Nvidia => "https://integrate.api.nvidia.com/v1/chat/completions",
            Self::Ollama => "http://localhost:11434/v1/chat/completions",
            Self::OpenRouter => "https://openrouter.ai/api/v1/chat/completions",
            Self::Perplexity => "https://api.perplexity.ai/chat/completions",
            Self::Qwen => "https://dashscope-intl.aliyuncs.com/compatible-mode/v1/chat/completions",
            Self::SambaNova => "https://api.sambanova.ai/v1/chat/completions",
            Self::Sarvam => "https://api.sarvam.ai/v2/chat/completions",
            Self::Together => "https://api.together.xyz/v1/chat/completions",
            Self::Xai => "https://api.x.ai/v1/chat/completions",
        })
    }

    /// Ollama's source service supplies this public dummy token; it is not a
    /// secret. Hosted services require caller-supplied credentials.
    pub const fn default_api_key(self) -> Option<&'static str> {
        match self {
            Self::Ollama => Some("ollama"),
            _ => None,
        }
    }

    /// Follows class attributes in Python, including inherited `true` values.
    pub const fn supports_developer_role(self) -> bool {
        matches!(
            self,
            Self::OpenAi
                | Self::Azure
                | Self::Cerebras
                | Self::Fireworks
                | Self::Groq
                | Self::Novita
                | Self::Nvidia
                | Self::Xai
        )
    }

    pub const fn requests_stream_usage(self) -> bool {
        !matches!(
            self,
            Self::Fireworks | Self::Mistral | Self::Perplexity | Self::Sarvam
        )
    }

    /// Why the preset cannot safely use the generic native client.
    pub const fn unsupported_reason(self) -> Option<&'static str> {
        match self {
            Self::Azure => Some("Azure requires the dedicated AzureConfig transport for endpoint routing and authentication"),
            _ => None,
        }
    }

    /// Provider-specific capabilities deliberately outside these request presets.
    pub const fn unsupported_features(self) -> &'static [&'static str] {
        match self {
            Self::Azure => &[
                "ambient Azure identity discovery",
                "Azure realtime, speech and Voice Live protocols",
            ],
            Self::Sarvam => &[
                "Python constructor-only routing and validation bypasses",
                "nonstreaming inference transport",
            ],
            Self::Nvidia => &["server-reported model updates"],
            Self::Perplexity => &[
                "tool declarations",
                "citation and search-result response metadata",
                "provider extra settings",
            ],
            Self::DeepSeek => &[
                "strict JSON response schemas",
                "seed",
                "max_completion_tokens",
            ],
            Self::Fireworks => &["seed", "max_completion_tokens", "requested streaming usage"],
            Self::Mistral => &[
                "max_completion_tokens",
                "requested streaming usage",
                "automatic tool execution deduplication",
            ],
            Self::SambaNova => &["service_tier", "tool streams without valid indices"],
            _ => &[],
        }
    }

    fn uses_legacy_token_limit(self) -> bool {
        matches!(
            self,
            Self::DeepSeek | Self::Fireworks | Self::Mistral | Self::Perplexity
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeepSeekThinking {
    Enabled,
    Disabled,
}

/// The union of source-provider values; applicability is validated per preset.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    None,
    Default,
    Instant,
    Low,
    Medium,
    High,
    Xhigh,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RoutingSort {
    Name(String),
    Options(Map<String, Value>),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum RoutingThreshold {
    Number(f64),
    Percentiles(BTreeMap<String, f64>),
}

/// Mirrors `OpenRouterProviderPreferences`; unknown routing fields are retained.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpenRouterProviderPreferences {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub order: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub allow_fallbacks: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub require_parameters: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub data_collection: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub zdr: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub enforce_distillable_text: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub only: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ignore: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quantizations: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort: Option<RoutingSort>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_min_throughput: Option<RoutingThreshold>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferred_max_latency: Option<RoutingThreshold>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_price: Option<BTreeMap<String, f64>>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Common sampling controls live on `CompletionRequest::settings`.
/// Here `NotGiven` uses a provider default, while `Null` explicitly omits a
/// provider-specific option. DeepSeek thinking defaults to disabled; Inception
/// realtime defaults to true. Explicit false values are always preserved.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct OpenAiPresetSettings {
    #[serde(default)]
    pub max_completion_tokens: Setting<u64>,
    #[serde(default)]
    pub service_tier: Setting<String>,
    #[serde(default)]
    pub reasoning_effort: Setting<ReasoningEffort>,
    #[serde(default)]
    pub thinking: Setting<DeepSeekThinking>,
    #[serde(default)]
    pub realtime: Setting<bool>,
    #[serde(default)]
    pub provider: Setting<OpenRouterProviderPreferences>,
    #[serde(default)]
    pub reasoning: Setting<Map<String, Value>>,
    #[serde(default)]
    pub wiki_grounding: Setting<bool>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct OpenAiPresetConfig {
    pub preset: OpenAiPreset,
    #[serde(default)]
    pub settings: OpenAiPresetSettings,
}

impl OpenAiPresetConfig {
    pub fn new(preset: OpenAiPreset) -> Self {
        Self {
            preset,
            settings: OpenAiPresetSettings::default(),
        }
    }

    pub fn endpoint(&self) -> Result<&'static str, ServiceError> {
        self.ensure_supported()?;
        self.preset
            .default_endpoint()
            .ok_or_else(|| ServiceError::Unsupported("provider endpoint must be configured".into()))
    }

    /// Build an ordinary request using the default model pinned in the source.
    pub fn request(&self, context: Context) -> CompletionRequest {
        CompletionRequest::new(self.preset.default_model(), context)
    }

    /// Build the final HTTP JSON body without changing context or settings.
    /// SDK `extra_body` is flattened, with the same provider-option precedence
    /// as Python. Reserved streaming/context fields remain protected.
    pub fn request_body(&self, request: &CompletionRequest) -> Result<Value, ServiceError> {
        self.ensure_supported()?;
        self.validate_settings()?;
        let mut prepared = request.clone();
        let sarvam_options = (self.preset == OpenAiPreset::Sarvam)
            .then(|| crate::sarvam::options(&mut prepared, &self.settings));
        prepared.extra.extend(prepared.settings.base.extra.clone());
        prepared.settings.base.extra.clear();
        let extra_body = match prepared.extra.remove("extra_body") {
            None | Some(Value::Null) => Map::new(),
            Some(Value::Object(fields)) => fields,
            Some(_) => {
                return Err(ServiceError::InvalidRequest(
                    "extra_body must be an object or null".into(),
                ))
            }
        };
        for key in extra_body.keys() {
            if reserved(key) {
                return Err(ServiceError::InvalidRequest(format!(
                    "extra_body field {key} would override the streaming contract"
                )));
            }
        }
        if self.preset == OpenAiPreset::Sarvam {
            // These top-level options are removed by the source builder. The
            // native streaming/context reserved-field checks still apply.
            prepared.extra.remove("max_completion_tokens");
            prepared.extra.remove("service_tier");
        }
        if self.preset.uses_legacy_token_limit()
            && (prepared.extra.contains_key("max_completion_tokens")
                || extra_body.contains_key("max_completion_tokens"))
        {
            return Err(ServiceError::Unsupported(format!(
                "{:?} max_completion_tokens; use max_tokens",
                self.preset
            )));
        }
        if self.preset == OpenAiPreset::Perplexity {
            if !prepared.extra.is_empty() || !extra_body.is_empty() {
                return Err(ServiceError::Unsupported(
                    "Perplexity's Python request builder does not forward extra settings".into(),
                ));
            }
            if prepared
                .context
                .messages
                .iter()
                .any(|message| message.role == crate::Role::Tool || !message.tool_calls.is_empty())
            {
                return Err(ServiceError::Unsupported(
                    "Perplexity tool-call history".into(),
                ));
            }
            if prepared
                .context
                .tools
                .as_ref()
                .is_some_and(|tools| !tools.is_empty())
                || prepared.context.tool_choice.is_some()
            {
                return Err(ServiceError::Unsupported(
                    "Perplexity tool declarations or tool_choice".into(),
                ));
            }
        }
        let mut body = adapters::request_body(Provider::OpenAi, &prepared)?;
        let root = body.as_object_mut().expect("OpenAI adapter returns object");
        // These settings correspond to source fields missing from common LLMSettings.
        if !prepared.extra.contains_key("max_completion_tokens") {
            put(
                root,
                "max_completion_tokens",
                &self.settings.max_completion_tokens,
            )?;
        }
        if let Setting::Value(tier) = &self.settings.service_tier {
            root.entry("service_tier").or_insert_with(|| json!(tier));
        }
        if self.preset.uses_legacy_token_limit() {
            if let Some(limit) = root.remove("max_completion_tokens") {
                root.entry("max_tokens").or_insert(limit);
            }
        }
        if !self.preset.requests_stream_usage() {
            root.remove("stream_options");
        }
        match self.preset {
            OpenAiPreset::DeepSeek | OpenAiPreset::Fireworks | OpenAiPreset::Perplexity => {
                reject_field(root, "seed", self.preset)?;
                reject_field(root, "service_tier", self.preset)?;
            }
            OpenAiPreset::Mistral => {
                reject_field(root, "service_tier", self.preset)?;
                if let Some(seed) = root
                    .remove("seed")
                    .filter(|s| !s.is_null() && s.as_i64() != Some(0))
                {
                    root.entry("random_seed").or_insert(seed);
                }
            }
            OpenAiPreset::SambaNova => {
                reject_field(root, "service_tier", self.preset)?;
            }
            _ => {}
        }
        self.apply_provider_settings(root)?;
        if let Some(options) = sarvam_options {
            for (key, value) in options {
                match value {
                    Some(Value::Null) => {
                        root.remove(key);
                    }
                    Some(value) => {
                        root.insert(key.into(), value);
                    }
                    None => {}
                }
            }
        }
        if self.preset == OpenAiPreset::Perplexity {
            root.remove("tools");
            root.remove("tool_choice");
            for key in [
                "frequency_penalty",
                "presence_penalty",
                "temperature",
                "top_p",
                "max_tokens",
            ] {
                if root.get(key).is_some_and(Value::is_null) {
                    root.remove(key);
                }
            }
        }
        let model = root
            .get("model")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        let messages = root
            .get_mut("messages")
            .and_then(Value::as_array_mut)
            .expect("OpenAI adapter returns messages");
        if !self.preset.supports_developer_role() {
            for message in messages.iter_mut() {
                if message["role"] == "developer" {
                    message["role"] = json!("user");
                }
            }
        }
        match self.preset {
            OpenAiPreset::DeepSeek => {
                for message in messages.iter_mut().filter(|m| m["role"] == "assistant") {
                    message
                        .as_object_mut()
                        .expect("OpenAI message is object")
                        .entry("reasoning_content")
                        .or_insert_with(|| json!(""));
                }
            }
            OpenAiPreset::Mistral => mistral_messages(messages),
            OpenAiPreset::Perplexity => perplexity_messages(messages),
            OpenAiPreset::OpenRouter if model.to_lowercase().contains("gemini") => {
                let mut seen = false;
                for message in messages.iter_mut() {
                    if message["role"] == "system" {
                        if seen {
                            message["role"] = json!("user");
                        }
                        seen = true;
                    }
                }
            }
            _ => {}
        }
        if self.preset == OpenAiPreset::Perplexity
            && !prepared.context.messages.is_empty()
            && messages.is_empty()
        {
            return Err(ServiceError::InvalidRequest(
                "preset transformation left no conversation messages".into(),
            ));
        }
        for (key, value) in extra_body {
            root.insert(key, value);
        }
        if self.preset == OpenAiPreset::Sarvam {
            crate::sarvam::finish_body(root)?;
        }
        // Flattening SDK extras must not bypass the explicit feature boundary.
        match self.preset {
            OpenAiPreset::DeepSeek | OpenAiPreset::Fireworks => {
                reject_field(root, "seed", self.preset)?;
                reject_field(root, "service_tier", self.preset)?;
            }
            OpenAiPreset::Mistral | OpenAiPreset::SambaNova => {
                reject_field(root, "service_tier", self.preset)?;
            }
            _ => {}
        }
        if self.preset == OpenAiPreset::DeepSeek
            && root
                .get("response_format")
                .is_some_and(|v| v["type"] == "json_schema")
        {
            return Err(ServiceError::Unsupported(
                "DeepSeek strict JSON response schema".into(),
            ));
        }
        Ok(body)
    }

    /// xAI may report reasoning outside completion_tokens. Avoid double counting
    /// when the provider already includes reasoning in completion_tokens.
    pub fn normalize_usage(&self, usage: &mut Usage) {
        if self.preset != OpenAiPreset::Xai {
            return;
        }
        let reasoning = usage.reasoning_tokens.unwrap_or(0);
        if reasoning > 0
            && usage
                .prompt_tokens
                .checked_add(usage.completion_tokens)
                .and_then(|n| n.checked_add(reasoning))
                == Some(usage.total_tokens)
        {
            if let Some(completion) = usage.completion_tokens.checked_add(reasoning) {
                usage.completion_tokens = completion;
            }
        }
    }

    fn ensure_supported(&self) -> Result<(), ServiceError> {
        if let Some(reason) = self.preset.unsupported_reason() {
            return Err(ServiceError::Unsupported(reason.into()));
        }
        Ok(())
    }

    fn validate_settings(&self) -> Result<(), ServiceError> {
        if matches!(self.settings.max_completion_tokens, Setting::Value(0)) {
            return Err(ServiceError::InvalidRequest(
                "max_completion_tokens must be positive".into(),
            ));
        }
        if self.preset.uses_legacy_token_limit()
            && !matches!(self.settings.max_completion_tokens, Setting::NotGiven)
        {
            return Err(ServiceError::Unsupported(format!(
                "{:?} max_completion_tokens; use request.settings.max_tokens",
                self.preset
            )));
        }
        for (name, supplied, valid) in [
            (
                "thinking",
                matches!(self.settings.thinking, Setting::Value(_)),
                self.preset == OpenAiPreset::DeepSeek,
            ),
            (
                "realtime",
                matches!(self.settings.realtime, Setting::Value(_)),
                self.preset == OpenAiPreset::Inception,
            ),
            (
                "provider",
                matches!(self.settings.provider, Setting::Value(_)),
                self.preset == OpenAiPreset::OpenRouter,
            ),
            (
                "reasoning",
                matches!(self.settings.reasoning, Setting::Value(_)),
                self.preset == OpenAiPreset::Together,
            ),
            (
                "wiki_grounding",
                matches!(self.settings.wiki_grounding, Setting::Value(_)),
                self.preset == OpenAiPreset::Sarvam,
            ),
        ] {
            if supplied && !valid {
                return Err(ServiceError::Unsupported(format!(
                    "{:?} {name} setting",
                    self.preset
                )));
            }
        }
        if let Setting::Value(effort) = self.settings.reasoning_effort {
            let supported = match self.preset {
                // Validate the resolved value after runtime and nested extras.
                OpenAiPreset::Sarvam => true,
                OpenAiPreset::Groq => matches!(
                    effort,
                    ReasoningEffort::None
                        | ReasoningEffort::Default
                        | ReasoningEffort::Low
                        | ReasoningEffort::Medium
                        | ReasoningEffort::High
                ),
                OpenAiPreset::Inception => matches!(
                    effort,
                    ReasoningEffort::Instant
                        | ReasoningEffort::Low
                        | ReasoningEffort::Medium
                        | ReasoningEffort::High
                ),
                OpenAiPreset::Xai => matches!(
                    effort,
                    ReasoningEffort::None
                        | ReasoningEffort::Low
                        | ReasoningEffort::Medium
                        | ReasoningEffort::High
                        | ReasoningEffort::Xhigh
                ),
                _ => false,
            };
            if !supported {
                return Err(ServiceError::Unsupported(format!(
                    "{:?} reasoning_effort {effort:?}",
                    self.preset
                )));
            }
        }
        if let Setting::Value(preferences) = &self.settings.provider {
            validate_preferences(preferences)?;
        }
        Ok(())
    }

    fn apply_provider_settings(&self, root: &mut Map<String, Value>) -> Result<(), ServiceError> {
        if let Setting::Value(effort) = &self.settings.reasoning_effort {
            if self.preset == OpenAiPreset::Sarvam {
                // Sarvam resolves client defaults and runtime options together.
            } else if self.preset == OpenAiPreset::Xai {
                root.entry("reasoning_effort").or_insert(json!(effort));
            } else {
                root.insert("reasoning_effort".into(), json!(effort));
            }
        }
        match self.preset {
            OpenAiPreset::DeepSeek => {
                let thinking = match self.settings.thinking {
                    Setting::NotGiven => Some(DeepSeekThinking::Disabled),
                    Setting::Null => None,
                    Setting::Value(thinking) => Some(thinking),
                };
                if let Some(thinking) = thinking {
                    root.entry("thinking").or_insert(json!({"type": thinking}));
                }
                if root
                    .get("response_format")
                    .is_some_and(|v| v["type"] == "json_schema")
                {
                    return Err(ServiceError::Unsupported(
                        "DeepSeek strict JSON response schema".into(),
                    ));
                }
            }
            OpenAiPreset::Inception => {
                let realtime = match self.settings.realtime {
                    Setting::NotGiven => Some(true),
                    Setting::Null => None,
                    Setting::Value(value) => Some(value),
                };
                if let Some(realtime) = realtime {
                    root.entry("realtime").or_insert(json!(realtime));
                }
            }
            OpenAiPreset::OpenRouter => {
                if let Setting::Value(preferences) = &self.settings.provider {
                    let mut value = serde_json::to_value(preferences)
                        .map_err(|e| ServiceError::InvalidRequest(e.to_string()))?;
                    value
                        .as_object_mut()
                        .expect("preferences object")
                        .retain(|_, v| !v.is_null());
                    root.entry("provider").or_insert(value);
                }
            }
            OpenAiPreset::Together => {
                if let Setting::Value(reasoning) = &self.settings.reasoning {
                    root.entry("reasoning").or_insert(json!(reasoning));
                }
            }
            _ => {}
        }
        Ok(())
    }
}

fn put<T: Serialize>(
    root: &mut Map<String, Value>,
    key: &str,
    setting: &Setting<T>,
) -> Result<(), ServiceError> {
    let value = match setting {
        Setting::NotGiven => return Ok(()),
        Setting::Null => Value::Null,
        Setting::Value(value) => {
            serde_json::to_value(value).map_err(|e| ServiceError::InvalidRequest(e.to_string()))?
        }
    };
    root.insert(key.into(), value);
    Ok(())
}

fn reject_field(
    root: &mut Map<String, Value>,
    key: &str,
    preset: OpenAiPreset,
) -> Result<(), ServiceError> {
    match root.remove(key) {
        None | Some(Value::Null) => Ok(()),
        Some(_) => Err(ServiceError::Unsupported(format!("{preset:?} {key}"))),
    }
}

fn reserved(key: &str) -> bool {
    matches!(
        key,
        "model"
            | "stream"
            | "messages"
            | "tools"
            | "tool_choice"
            | "system"
            | "contents"
            | "systemInstruction"
            | "toolConfig"
            | "stream_options"
            | "n"
            | "candidateCount"
            | "extra_body"
    )
}

fn mistral_messages(messages: &mut Vec<Value>) {
    let mut transformed = Vec::with_capacity(messages.len());
    for (index, message) in messages.iter().enumerate() {
        transformed.push(message.clone());
        if message["role"] == "tool"
            && messages
                .get(index + 1)
                .is_none_or(|next| next["role"] != "assistant")
        {
            transformed.push(json!({"role":"assistant", "content":" "}));
        }
    }
    convert_late_system(&mut transformed);
    if let Some(last) = transformed.last_mut().filter(|m| m["role"] == "assistant") {
        last.as_object_mut()
            .expect("message object")
            .entry("prefix")
            .or_insert(json!(true));
    }
    *messages = transformed;
}

fn convert_late_system(messages: &mut [Value]) {
    let mut initial = true;
    for message in messages {
        if message["role"] == "system" {
            if !initial {
                message["role"] = json!("user");
            }
        } else {
            initial = false;
        }
    }
}

fn perplexity_messages(messages: &mut Vec<Value>) {
    convert_late_system(messages);
    let mut merged: Vec<Value> = Vec::with_capacity(messages.len());
    for mut message in messages.drain(..) {
        if let Some(previous) = merged
            .last_mut()
            .filter(|m| m["role"] == message["role"] && message["role"] != "system")
        {
            for item in [&mut *previous, &mut message] {
                if let Some(text) = item["content"].as_str() {
                    item["content"] = json!([{"type":"text", "text":text}]);
                }
            }
            if let (Some(previous), Some(next)) = (
                previous["content"].as_array_mut(),
                message["content"].as_array(),
            ) {
                previous.extend(next.iter().cloned());
            }
        } else {
            merged.push(message);
        }
    }
    while merged.last().is_some_and(|m| m["role"] == "assistant") {
        merged.pop();
    }
    *messages = merged;
}

fn validate_preferences(preferences: &OpenRouterProviderPreferences) -> Result<(), ServiceError> {
    let finite_nonnegative = |value: f64| value.is_finite() && value >= 0.0;
    for threshold in [
        &preferences.preferred_min_throughput,
        &preferences.preferred_max_latency,
    ]
    .into_iter()
    .flatten()
    {
        let valid = match threshold {
            RoutingThreshold::Number(value) => finite_nonnegative(*value),
            RoutingThreshold::Percentiles(values) => {
                values.values().copied().all(finite_nonnegative)
            }
        };
        if !valid {
            return Err(ServiceError::InvalidRequest(
                "OpenRouter routing thresholds must be finite and nonnegative".into(),
            ));
        }
    }
    if preferences
        .max_price
        .as_ref()
        .is_some_and(|values| !values.values().copied().all(finite_nonnegative))
    {
        return Err(ServiceError::InvalidRequest(
            "OpenRouter max_price must be finite and nonnegative".into(),
        ));
    }
    Ok(())
}
