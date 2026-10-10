//! Sarvam request validation, model routing and native identification.
//!
//! All response handling uses the ordinary OpenAI HTTP/SSE implementation.
use crate::{presets::OpenAiPresetSettings, CompletionRequest, ServiceError, Setting};
use reqwest::header::{HeaderMap, HeaderName, HeaderValue, AUTHORIZATION, USER_AGENT};
use serde_json::{json, Map, Value};
use std::collections::BTreeMap;

pub const MODELS: [&str; 5] = [
    "deepseekv4-flash",
    "gemma4",
    "glm5.2",
    "sarvam-105b",
    "sarvam-105b-conversations",
];

/// Resolved for each request. An explicit endpoint without this placeholder
/// remains fixed when runtime settings select a different model.
pub const DEFAULT_ENDPOINT: &str = "https://api.sarvam.ai/{version}/chat/completions";
pub const USER_AGENT_VALUE: &str = concat!("Pipecat/", env!("CARGO_PKG_VERSION"), " Rust/native");

pub fn api_version(model: &str) -> Result<&'static str, ServiceError> {
    if !MODELS.contains(&model) {
        return Err(ServiceError::InvalidRequest(
            "unsupported Sarvam model; expected deepseekv4-flash, gemma4, glm5.2, sarvam-105b or sarvam-105b-conversations".into(),
        ));
    }
    Ok(if model == "sarvam-105b-conversations" {
        "v1"
    } else {
        "v2"
    })
}

/// Expand only the Sarvam version segment immediately before chat/completions.
/// Fixed endpoints retain their exact path. URL security checks belong to the
/// shared client, which calls this function before sending or committing state.
pub(crate) fn resolve_endpoint(endpoint: &str, model: &str) -> Result<String, ServiceError> {
    let version = api_version(model)?;
    if endpoint.contains(['{', '}']) {
        let prefix = endpoint
            .strip_suffix("/{version}/chat/completions")
            .ok_or_else(|| {
                ServiceError::InvalidRequest(
                    "Sarvam endpoint template must end in /{version}/chat/completions".into(),
                )
            })?;
        if prefix.contains(['{', '}']) {
            return Err(ServiceError::InvalidRequest(
                "invalid or repeated Sarvam endpoint placeholder".into(),
            ));
        }
        let resolved = format!("{prefix}/{version}/chat/completions");
        let url = reqwest::Url::parse(&resolved)
            .map_err(|_| ServiceError::InvalidRequest("invalid Sarvam endpoint URL".into()))?;
        if url.query().is_some()
            || url.fragment().is_some()
            || !url
                .path()
                .ends_with(&format!("/{version}/chat/completions"))
        {
            return Err(ServiceError::InvalidRequest(
                "Sarvam version placeholder must be a path segment".into(),
            ));
        }
        Ok(resolved)
    } else {
        Ok(endpoint.into())
    }
}

pub(crate) fn headers(
    api_key: Option<&str>,
    defaults: &BTreeMap<String, String>,
) -> Result<HeaderMap, ServiceError> {
    let key = api_key
        .filter(|key| !key.trim().is_empty())
        .ok_or_else(|| ServiceError::InvalidRequest("Sarvam requires a nonempty API key".into()))?;
    let mut headers = HeaderMap::new();
    for (name, value) in defaults {
        let name = HeaderName::from_bytes(name.as_bytes())
            .map_err(|_| ServiceError::InvalidRequest("invalid Sarvam header name".into()))?;
        // Framing and the streaming protocol are owned by the transport.
        if matches!(
            name.as_str(),
            "host"
                | "content-length"
                | "transfer-encoding"
                | "content-type"
                | "accept"
                | "connection"
        ) {
            return Err(ServiceError::InvalidRequest(
                "reserved Sarvam transport header".into(),
            ));
        }
        if name == USER_AGENT || name == AUTHORIZATION || name == "api-subscription-key" {
            continue;
        }
        if headers.contains_key(&name) {
            return Err(ServiceError::InvalidRequest(
                "duplicate Sarvam header name".into(),
            ));
        }
        let mut value = HeaderValue::from_str(value)
            .map_err(|_| ServiceError::InvalidRequest("invalid Sarvam header value".into()))?;
        value.set_sensitive(true);
        headers.insert(name, value);
    }
    for (name, value) in [
        (AUTHORIZATION, format!("Bearer {key}")),
        (HeaderName::from_static("api-subscription-key"), key.into()),
    ] {
        let mut value = HeaderValue::from_str(&value)
            .map_err(|_| ServiceError::InvalidRequest("invalid Sarvam API key header".into()))?;
        value.set_sensitive(true);
        headers.insert(name, value);
    }
    headers.insert(USER_AGENT, HeaderValue::from_static(USER_AGENT_VALUE));
    Ok(headers)
}

/// Core extras are the runtime-updatable representation of these two settings.
/// Extract them before ordinary extras merge so clearing one also clears a
/// client-level default. Nested extra_body retains final per-key precedence.
pub(crate) fn options(
    request: &mut CompletionRequest,
    defaults: &OpenAiPresetSettings,
) -> [(&'static str, Option<Value>); 2] {
    let reasoning = request
        .settings
        .base
        .extra
        .remove("reasoning_effort")
        .or_else(|| {
            if let Setting::Value(value) = defaults.reasoning_effort {
                Some(json!(value))
            } else {
                None
            }
        });
    let wiki = request
        .settings
        .base
        .extra
        .remove("wiki_grounding")
        .or_else(|| {
            if let Setting::Value(value) = defaults.wiki_grounding {
                Some(json!(value))
            } else {
                None
            }
        });
    [("reasoning_effort", reasoning), ("wiki_grounding", wiki)]
}

pub(crate) fn finish_body(root: &mut Map<String, Value>) -> Result<(), ServiceError> {
    for name in ["stream_options", "max_completion_tokens", "service_tier"] {
        root.remove(name);
    }
    for name in ["reasoning_effort", "wiki_grounding"] {
        if root.get(name).is_some_and(Value::is_null) {
            root.remove(name);
        }
    }
    let model = root.get("model").and_then(Value::as_str).unwrap_or("");
    api_version(model)?;
    if root
        .get("tool_choice")
        .is_some_and(|value| !value.is_null())
        && !root
            .get("tools")
            .and_then(Value::as_array)
            .is_some_and(|tools| !tools.is_empty())
    {
        return Err(ServiceError::InvalidRequest(
            "Sarvam tool_choice requires nonempty tools".into(),
        ));
    }
    if let Some(effort) = root.get("reasoning_effort") {
        if !matches!(effort.as_str(), Some("low" | "medium" | "high")) {
            return Err(ServiceError::InvalidRequest(
                "Sarvam reasoning_effort must be low, medium or high".into(),
            ));
        }
        if model == "sarvam-105b-conversations" {
            return Err(ServiceError::Unsupported(
                "Sarvam conversations reasoning_effort".into(),
            ));
        }
    }
    if let Some(wiki) = root.get("wiki_grounding") {
        if !wiki.is_boolean() {
            return Err(ServiceError::InvalidRequest(
                "Sarvam wiki_grounding must be boolean".into(),
            ));
        }
        if !matches!(model, "gemma4" | "sarvam-105b") {
            return Err(ServiceError::Unsupported(
                "Sarvam wiki_grounding requires gemma4 or sarvam-105b".into(),
            ));
        }
    }
    if model != "gemma4"
        && root
            .get("messages")
            .and_then(Value::as_array)
            .is_some_and(|messages| {
                messages.iter().any(|message| {
                    message
                        .get("content")
                        .and_then(Value::as_array)
                        .is_some_and(|parts| {
                            parts.iter().any(|part| {
                                part.get("type").and_then(Value::as_str) == Some("image_url")
                            })
                        })
                })
            })
    {
        return Err(ServiceError::Unsupported(
            "Sarvam image input requires gemma4".into(),
        ));
    }
    if root
        .get("max_tokens")
        .is_some_and(|value| !value.is_null() && !value.as_u64().is_some_and(|limit| limit > 0))
    {
        return Err(ServiceError::InvalidRequest(
            "Sarvam max_tokens must be positive".into(),
        ));
    }
    Ok(())
}
