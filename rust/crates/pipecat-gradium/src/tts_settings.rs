// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{GradiumError, GradiumTtsClient};
use pipecat_core::{FrameProcessor, ProcessorReference, Setting, TtsSettings};
use serde_json::{Map, Value};
use std::{collections::BTreeMap, fmt};

/// Pinned TTS wire settings. The source's model/language fields are not sent;
/// unsupported updates are rejected rather than reported as applied.
#[derive(Clone, PartialEq, Eq)]
pub struct GradiumTtsSettings {
    pub voice: String,
    /// Serialized JSON is sent as a STRING, unlike Gradium STT's object.
    pub json_config: Option<String>,
}
impl fmt::Debug for GradiumTtsSettings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GradiumTtsSettings").finish_non_exhaustive()
    }
}
impl Default for GradiumTtsSettings {
    fn default() -> Self {
        Self {
            voice: "_6Aslh2DxfmnRLmP".into(),
            json_config: None,
        }
    }
}
impl GradiumTtsSettings {
    pub fn validate(&self) -> Result<(), GradiumError> {
        name(&self.voice)?;
        if let Some(value) = &self.json_config {
            if value.len() > 16384 {
                return Err(GradiumError::TooLarge);
            }
            let parsed: Value = serde_json::from_str(value).map_err(|_| invalid())?;
            if !parsed.is_object() {
                return Err(invalid());
            }
            crate::protocol::validate_json(&parsed, 0, &mut 0)?;
        }
        Ok(())
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, GradiumError> {
        validate_values(delta)?;
        let mut next = self.clone();
        for (key, value) in delta {
            match key.as_str() {
                "voice" => next.voice = value.as_str().ok_or_else(invalid)?.into(),
                "json_config" => {
                    next.json_config = if value.is_null() {
                        None
                    } else {
                        Some(value.as_str().ok_or_else(invalid)?.into())
                    }
                }
                _ => return Err(invalid()),
            }
        }
        next.validate()?;
        Ok(next)
    }
}
pub(crate) fn validate_values<'a>(
    values: impl IntoIterator<Item = (&'a String, &'a Value)>,
) -> Result<(), GradiumError> {
    let mut remaining = 32768;
    let mut count = 0;
    for (key, value) in values {
        count += 1;
        if count > 32 || key.len() > 64 || !bounded_value(value, 0, &mut remaining) {
            return Err(GradiumError::TooLarge);
        }
    }
    Ok(())
}
fn bounded_value(value: &Value, depth: usize, remaining: &mut usize) -> bool {
    if depth > 8 {
        return false;
    }
    let bytes = match value {
        Value::String(s) => s.len().saturating_add(2),
        _ => 16,
    };
    let Some(next) = remaining.checked_sub(bytes) else {
        return false;
    };
    *remaining = next;
    match value {
        Value::Array(v) => {
            v.len() <= 512 && v.iter().all(|v| bounded_value(v, depth + 1, remaining))
        }
        Value::Object(v) => {
            v.len() <= 32
                && v.iter()
                    .all(|(k, v)| k.len() <= 64 && bounded_value(v, depth + 1, remaining))
        }
        _ => true,
    }
}
pub(crate) fn name(value: &str) -> Result<(), GradiumError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        Err(invalid())
    } else {
        Ok(())
    }
}
fn invalid() -> GradiumError {
    GradiumError::Invalid("unsupported or incorrectly typed Gradium setting")
}
pub(crate) fn targets(processor: &dyn FrameProcessor, target: &Option<ProcessorReference>) -> bool {
    match target {
        None => true,
        Some(ProcessorReference::Name(n)) => n == processor.name(),
        Some(ProcessorReference::Local(p)) => std::ptr::addr_eq(p.0.as_ref(), processor),
    }
}
pub(crate) fn tts_mapping(values: &BTreeMap<String, Value>) -> Result<TtsSettings, GradiumError> {
    validate_values(values)?;
    let mut settings = TtsSettings::default();
    for (key, value) in values {
        let field = match key.as_str() {
            "model" => Some(&mut settings.base.model),
            "voice" => Some(&mut settings.voice),
            "language" => Some(&mut settings.language),
            _ => None,
        };
        if let Some(field) = field {
            *field = match value {
                Value::Null => Setting::Null,
                Value::String(v) => Setting::Value(v.clone()),
                _ => return Err(invalid()),
            };
        } else {
            settings.base.extra.insert(key.clone(), value.clone());
        }
    }
    Ok(settings)
}
pub(crate) fn tts(
    client: &GradiumTtsClient,
    delta: &TtsSettings,
) -> Result<GradiumTtsClient, GradiumError> {
    validate_values(&delta.base.extra)?;
    let mut values: Map<String, Value> = delta.base.extra.clone().into_iter().collect();
    for (name, setting) in [
        ("model", &delta.base.model),
        ("voice", &delta.voice),
        ("language", &delta.language),
    ] {
        add(&mut values, name, setting);
    }
    client.with_settings(client.config().settings.merge(&values)?)
}
pub(crate) fn add(values: &mut Map<String, Value>, name: &str, setting: &Setting<String>) {
    match setting {
        Setting::NotGiven => (),
        Setting::Null => {
            values.insert(name.into(), Value::Null);
        }
        Setting::Value(v) => {
            values.insert(name.into(), v.clone().into());
        }
    }
}

pub(crate) fn validate_setting(value: &Setting<String>) -> Result<(), GradiumError> {
    if let Setting::Value(v) = value {
        name(v)?;
    }
    Ok(())
}
