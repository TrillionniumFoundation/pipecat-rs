//! Explicit source-style Anthropic cache breakpoints, not automatic caching.
use crate::{CompletionRequest, Provider, ServiceError};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

const ENABLE: &str = "enable_prompt_caching";
const TTL: &str = "system_prompt_cache_ttl";

/// System-prefix lifetime only. Message breakpoints always omit a TTL.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AnthropicCacheTtl {
    #[serde(rename = "5m")]
    FiveMinutes,
    #[serde(rename = "1h")]
    OneHour,
}
impl AnthropicCacheTtl {
    fn wire(self) -> &'static str {
        match self {
            Self::FiveMinutes => "5m",
            Self::OneHour => "1h",
        }
    }
}

/// Opt-in policy for the system prefix and last two converted user messages.
/// `None` omits system TTL (the provider's default), not an infinite lifetime.
/// The provider decides eligibility/hits; this type makes no cache-hit promise.
/// A one-hour cache write can cost more than a default-lifetime write.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AnthropicPromptCachePolicy {
    pub enabled: bool,
    pub system_ttl: Option<AnthropicCacheTtl>,
}

impl CompletionRequest {
    /// Set the typed Anthropic policy through the same canonical settings keys
    /// used by runtime deltas. Settings override raw request `extra` values;
    /// these internal keys are consumed locally and never sent to the provider.
    /// Validation occurs with the complete request and actual selected provider.
    pub fn with_anthropic_prompt_cache(mut self, policy: AnthropicPromptCachePolicy) -> Self {
        self.settings
            .base
            .extra
            .insert(ENABLE.into(), json!(policy.enabled));
        self.settings.base.extra.insert(
            TTL.into(),
            policy
                .system_ttl
                .map_or(Value::Null, |ttl| json!(ttl.wire())),
        );
        self
    }
}

/// Resolve only after sparse settings have overridden request extras. Remove
/// policy keys before arbitrary extra fields are copied into the wire object.
pub(crate) fn take_policy(
    provider: Provider,
    extra: &mut Map<String, Value>,
) -> Result<Option<AnthropicPromptCachePolicy>, ServiceError> {
    let enabled = extra.remove(ENABLE);
    let ttl = extra.remove(TTL);
    if enabled.is_none() && ttl.is_none() {
        return Ok(None);
    }
    if provider != Provider::Anthropic {
        return Err(ServiceError::Unsupported(
            "prompt cache policy requires Anthropic Messages".into(),
        ));
    }
    if extra.contains_key("cache_control") {
        return Err(ServiceError::InvalidRequest(
            "explicit prompt cache policy conflicts with automatic cache_control".into(),
        ));
    }
    let enabled = match enabled {
        None => false,
        Some(Value::Bool(enabled)) => enabled,
        Some(_) => {
            return Err(ServiceError::InvalidRequest(
                "enable_prompt_caching must be a boolean".into(),
            ))
        }
    };
    let system_ttl = match ttl {
        None | Some(Value::Null) => None,
        Some(Value::String(ttl)) if ttl == "5m" => Some(AnthropicCacheTtl::FiveMinutes),
        Some(Value::String(ttl)) if ttl == "1h" => Some(AnthropicCacheTtl::OneHour),
        Some(_) => {
            return Err(ServiceError::InvalidRequest(
                "system_prompt_cache_ttl must be null, 5m or 1h".into(),
            ))
        }
    };
    Ok(Some(AnthropicPromptCachePolicy {
        enabled,
        system_ttl,
    }))
}

/// Body is freshly built by the typed Anthropic adapter after role merging.
/// A malformed internal body fails closed rather than returning partial markers.
pub(crate) fn apply(
    body: &mut Value,
    policy: Option<AnthropicPromptCachePolicy>,
) -> Result<(), ServiceError> {
    let Some(policy) = policy.filter(|p| p.enabled) else {
        return Ok(());
    };
    if let Some(system) = body.get("system") {
        let text = system.as_str().ok_or_else(invalid_body)?;
        let mut marker = json!({"type":"ephemeral"});
        if let Some(ttl) = policy.system_ttl {
            marker["ttl"] = json!(ttl.wire());
        }
        body["system"] = json!([{"type":"text","text":text,"cache_control":marker}]);
    }
    let messages = body
        .get_mut("messages")
        .and_then(Value::as_array_mut)
        .ok_or_else(invalid_body)?;
    for message in messages
        .iter_mut()
        .rev()
        .filter(|m| m.get("role").and_then(Value::as_str) == Some("user"))
        .take(2)
    {
        let last = message
            .get_mut("content")
            .and_then(Value::as_array_mut)
            .and_then(|content| content.last_mut())
            .and_then(Value::as_object_mut)
            .ok_or_else(invalid_body)?;
        last.insert("cache_control".into(), json!({"type":"ephemeral"}));
    }
    Ok(())
}
fn invalid_body() -> ServiceError {
    ServiceError::InvalidRequest("invalid Anthropic cache-breakpoint body".into())
}
