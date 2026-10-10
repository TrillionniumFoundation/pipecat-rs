//! Sparse, three-state runtime settings with immutable per-request snapshots.
use crate::{CompletionRequest, Provider, ServiceError};
use pipecat_core::{LlmSettings, ServiceSettings, Setting};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::BTreeMap;

/// Merge only fields present in a delta. Null is a value, not omission.
pub fn merge_settings(current: &mut LlmSettings, delta: &LlmSettings) {
    macro_rules! merge { ($($field:ident),+ $(,)?) => {$(
        if !matches!(delta.$field, Setting::NotGiven) { current.$field = delta.$field.clone(); }
    )+}; }
    if !matches!(delta.base.model, Setting::NotGiven) {
        current.base.model = delta.base.model.clone();
    }
    current.base.extra.extend(delta.base.extra.clone());
    merge!(
        system_instruction,
        temperature,
        max_tokens,
        top_p,
        top_k,
        frequency_penalty,
        presence_penalty,
        seed,
        filter_incomplete_user_turns,
        user_turn_completion_config
    );
}

/// Parse the deprecated mapping form without silently dropping unknown fields.
pub fn settings_from_mapping(map: &BTreeMap<String, Value>) -> Result<LlmSettings, ServiceError> {
    fn parse<T: serde::de::DeserializeOwned>(
        key: &str,
        value: &Value,
    ) -> Result<Setting<T>, ServiceError> {
        if value.is_null() {
            return Ok(Setting::Null);
        }
        serde_json::from_value(value.clone())
            .map(Setting::Value)
            .map_err(|e| ServiceError::InvalidRequest(format!("setting {key}: {e}")))
    }
    let mut out = LlmSettings::default();
    for (key, value) in map {
        match key.as_str() {
            "model" => out.base.model = parse(key, value)?,
            "system_instruction" => out.system_instruction = parse(key, value)?,
            "temperature" => out.temperature = parse(key, value)?,
            "max_tokens" => out.max_tokens = parse(key, value)?,
            "top_p" => out.top_p = parse(key, value)?,
            "top_k" => out.top_k = parse(key, value)?,
            "frequency_penalty" => out.frequency_penalty = parse(key, value)?,
            "presence_penalty" => out.presence_penalty = parse(key, value)?,
            "seed" => out.seed = parse(key, value)?,
            "filter_incomplete_user_turns" => out.filter_incomplete_user_turns = parse(key, value)?,
            "user_turn_completion_config" => out.user_turn_completion_config = parse(key, value)?,
            "extra" => {
                let extras: BTreeMap<String, Value> = serde_json::from_value(value.clone())
                    .map_err(|e| ServiceError::InvalidRequest(format!("setting extra: {e}")))?;
                out.base.extra.extend(extras);
            }
            _ => {
                out.base.extra.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(out)
}

/// Apply identity/system settings before context conversion, and sampling fields
/// to the actual provider object afterward. The original request is unchanged.
pub(crate) fn resolve(request: &CompletionRequest) -> Result<CompletionRequest, ServiceError> {
    let mut resolved = request.clone();
    match &request.settings.base.model {
        Setting::NotGiven => {}
        Setting::Value(model) => resolved.model = model.clone(),
        Setting::Null => return Err(ServiceError::InvalidRequest("model cannot be null".into())),
    }
    match &request.settings.system_instruction {
        Setting::NotGiven => {}
        Setting::Value(system) => resolved.system_instruction = Some(system.clone()),
        Setting::Null => resolved.system_instruction = None,
    }
    match request.settings.temperature {
        Setting::NotGiven => {}
        Setting::Value(value) => resolved.temperature = Some(value),
        Setting::Null => resolved.temperature = None,
    }
    if !matches!(request.settings.max_tokens, Setting::NotGiven) {
        // This field follows Python LLMSettings; the legacy convenience field
        // maps to max_completion_tokens on OpenAI and is superseded.
        resolved.max_tokens = None;
    }
    if matches!(
        request.settings.filter_incomplete_user_turns,
        Setting::Value(true)
    ) || matches!(
        request.settings.user_turn_completion_config,
        Setting::Value(_)
    ) {
        return Err(ServiceError::Unsupported(
            "incomplete user-turn filtering requires a turn-completion gate".into(),
        ));
    }
    resolved.extra.extend(request.settings.base.extra.clone());
    Ok(resolved)
}

pub(crate) fn apply_wire(
    provider: Provider,
    settings: &LlmSettings,
    body: &mut Value,
) -> Result<(), ServiceError> {
    fn put<T: Serialize>(
        target: &mut Map<String, Value>,
        key: &str,
        setting: &Setting<T>,
    ) -> Result<(), ServiceError> {
        let value = match setting {
            Setting::NotGiven => return Ok(()),
            Setting::Null => Value::Null,
            Setting::Value(value) => serde_json::to_value(value)
                .map_err(|e| ServiceError::InvalidRequest(e.to_string()))?,
        };
        target.insert(key.into(), value);
        Ok(())
    }
    fn range(value: &Setting<f64>, name: &str, min: f64, max: f64) -> Result<(), ServiceError> {
        if let Setting::Value(value) = value {
            if !value.is_finite() || !(min..=max).contains(value) {
                return Err(ServiceError::InvalidRequest(format!(
                    "{name} must be between {min} and {max}"
                )));
            }
        }
        Ok(())
    }
    range(&settings.temperature, "temperature", 0.0, 2.0)?;
    range(&settings.top_p, "top_p", 0.0, 1.0)?;
    range(&settings.frequency_penalty, "frequency_penalty", -2.0, 2.0)?;
    range(&settings.presence_penalty, "presence_penalty", -2.0, 2.0)?;
    if matches!(settings.max_tokens, Setting::Value(0)) {
        return Err(ServiceError::InvalidRequest(
            "max_tokens must be positive".into(),
        ));
    }
    let root = body.as_object_mut().expect("request body is object");
    if provider == Provider::Gemini {
        let mut config = root
            .get("generationConfig")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for (key, value) in [
            ("temperature", &settings.temperature),
            ("topP", &settings.top_p),
            ("frequencyPenalty", &settings.frequency_penalty),
            ("presencePenalty", &settings.presence_penalty),
        ] {
            put(&mut config, key, value)?;
        }
        put(&mut config, "topK", &settings.top_k)?;
        put(&mut config, "maxOutputTokens", &settings.max_tokens)?;
        put(&mut config, "seed", &settings.seed)?;
        // Google generation settings omit explicit None after clearing the
        // stored value, unlike the OpenAI/Anthropic null wire semantics.
        config.retain(|_, value| !value.is_null());
        if !config.is_empty() {
            root.insert("generationConfig".into(), Value::Object(config));
        } else {
            root.remove("generationConfig");
        }
    } else {
        put(root, "temperature", &settings.temperature)?;
        put(root, "top_p", &settings.top_p)?;
        put(root, "max_tokens", &settings.max_tokens)?;
        if provider == Provider::Anthropic {
            if matches!(settings.max_tokens, Setting::Null) {
                return Err(ServiceError::InvalidRequest(
                    "Anthropic max_tokens cannot be null".into(),
                ));
            }
            for (name, provided) in [
                (
                    "frequency_penalty",
                    !matches!(settings.frequency_penalty, Setting::NotGiven),
                ),
                (
                    "presence_penalty",
                    !matches!(settings.presence_penalty, Setting::NotGiven),
                ),
                ("seed", !matches!(settings.seed, Setting::NotGiven)),
            ] {
                if provided {
                    return Err(ServiceError::Unsupported(format!("Anthropic {name}")));
                }
            }
            put(root, "top_k", &settings.top_k)?;
        } else {
            if !matches!(settings.top_k, Setting::NotGiven | Setting::Null) {
                return Err(ServiceError::Unsupported("OpenAI top_k".into()));
            }
            put(root, "frequency_penalty", &settings.frequency_penalty)?;
            put(root, "presence_penalty", &settings.presence_penalty)?;
            put(root, "seed", &settings.seed)?;
        }
    }
    Ok(())
}

pub(crate) fn service_delta(base: &ServiceSettings) -> LlmSettings {
    LlmSettings {
        base: base.clone(),
        ..Default::default()
    }
}
