//! Atomic, typed settings deltas; credentials and connection format stay fixed.
use crate::{CartesiaClient, DeepgramClient, Endpointing, StreamingError};
use pipecat_core::{
    FrameProcessor, ProcessorReference, ServiceSettings, Setting, SttSettings, TtsSettings,
};
use serde_json::Value;
use std::collections::BTreeMap;

type Result<T> = std::result::Result<T, StreamingError>;

pub(crate) fn targets(processor: &dyn FrameProcessor, target: &Option<ProcessorReference>) -> bool {
    match target {
        None => true,
        Some(ProcessorReference::Name(name)) => name == processor.name(),
        Some(ProcessorReference::Local(target)) => std::ptr::addr_eq(target.0.as_ref(), processor),
    }
}
fn invalid() -> StreamingError {
    StreamingError::Invalid("unsupported setting or invalid setting value")
}
fn required(target: &mut String, value: &Setting<String>) -> Result<()> {
    match value {
        Setting::NotGiven => {}
        Setting::Value(value) if !value.is_empty() => *target = value.clone(),
        _ => return Err(invalid()),
    }
    Ok(())
}
fn optional(target: &mut Option<String>, value: &Setting<String>) -> Result<()> {
    match value {
        Setting::NotGiven => {}
        Setting::Null => *target = None,
        Setting::Value(value) if !value.is_empty() => *target = Some(value.clone()),
        _ => return Err(invalid()),
    }
    Ok(())
}
fn string(value: &Value) -> Result<Setting<String>> {
    match value {
        Value::Null => Ok(Setting::Null),
        Value::String(value) => Ok(Setting::Value(value.clone())),
        _ => Err(invalid()),
    }
}
pub(crate) fn tts_mapping(values: &BTreeMap<String, Value>) -> Result<TtsSettings> {
    let mut settings = TtsSettings::default();
    for (key, value) in values {
        match key.as_str() {
            "model" => settings.base.model = string(value)?,
            "voice" => settings.voice = string(value)?,
            "language" => settings.language = string(value)?,
            _ => {
                settings.base.extra.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(settings)
}
pub(crate) fn stt_mapping(values: &BTreeMap<String, Value>) -> Result<SttSettings> {
    let mut settings = SttSettings::default();
    for (key, value) in values {
        match key.as_str() {
            "model" => settings.base.model = string(value)?,
            "language" => settings.language = string(value)?,
            _ => {
                settings.base.extra.insert(key.clone(), value.clone());
            }
        }
    }
    Ok(settings)
}
/// Fields promoted from extra must not silently override explicit typed fields.
fn promoted(
    base: &ServiceSettings,
    language: &Setting<String>,
    voice: Option<&Setting<String>>,
) -> Result<BTreeMap<String, Value>> {
    let mut values = base.extra.clone();
    for (key, field) in [("model", &base.model), ("language", language)] {
        match field {
            Setting::NotGiven => {}
            Setting::Null => {
                values.insert(key.into(), Value::Null);
            }
            Setting::Value(value) => {
                values.insert(key.into(), value.clone().into());
            }
        }
    }
    if let Some(field) = voice {
        match field {
            Setting::NotGiven => {}
            Setting::Null => {
                values.insert("voice".into(), Value::Null);
            }
            Setting::Value(value) => {
                values.insert("voice".into(), value.clone().into());
            }
        }
    }
    Ok(values)
}
pub(crate) fn cartesia(client: &CartesiaClient, delta: &TtsSettings) -> Result<CartesiaClient> {
    let mut config = client.config().clone();
    let values = promoted(&delta.base, &delta.language, Some(&delta.voice))?;
    for (key, value) in values {
        match key.as_str() {
            "model" => required(&mut config.model, &string(&value)?)?,
            "voice" => required(&mut config.voice_id, &string(&value)?)?,
            "language" => optional(&mut config.language, &string(&value)?)?,
            "generation_config" => {
                if !value.is_null() && !value.is_object() {
                    return Err(invalid());
                }
                config.generation_config = (!value.is_null()).then_some(value);
            }
            "pronunciation_dict_id" => {
                optional(&mut config.pronunciation_dict_id, &string(&value)?)?
            }
            _ => return Err(invalid()),
        }
    }
    CartesiaClient::new(config)
}
pub(crate) fn deepgram(client: &DeepgramClient, delta: &SttSettings) -> Result<DeepgramClient> {
    let mut config = client.config().clone();
    let values = promoted(&delta.base, &delta.language, None)?;
    for (key, value) in values {
        match key.as_str() {
            "model" => required(&mut config.model, &string(&value)?)?,
            "language" => optional(&mut config.language, &string(&value)?)?,
            "interim_results" => config.interim_results = value.as_bool().ok_or_else(invalid)?,
            "punctuate" => config.punctuate = value.as_bool().ok_or_else(invalid)?,
            "smart_format" => config.smart_format = value.as_bool().ok_or_else(invalid)?,
            "diarize" => config.diarize = value.as_bool().ok_or_else(invalid)?,
            "multichannel" => config.multichannel = value.as_bool().ok_or_else(invalid)?,
            "vad_events" => config.vad_events = value.as_bool().ok_or_else(invalid)?,
            "endpointing" => {
                config.endpointing = match value {
                    Value::Null => None,
                    Value::Bool(false) => Some(Endpointing::Disabled),
                    _ => Some(Endpointing::Milliseconds(
                        u32::try_from(value.as_u64().ok_or_else(invalid)?)
                            .map_err(|_| invalid())?,
                    )),
                }
            }
            "utterance_end_ms" => {
                config.utterance_end_ms = match value {
                    Value::Null => None,
                    _ => Some(
                        u32::try_from(value.as_u64().ok_or_else(invalid)?)
                            .map_err(|_| invalid())?,
                    ),
                }
            }
            "detect_entities" | "dictation" | "mip_opt_out" | "numerals" | "profanity_filter" => {
                config.extra_query.retain(|(existing, _)| existing != &key);
                if !value.is_null() {
                    config
                        .extra_query
                        .push((key, value.as_bool().ok_or_else(invalid)?.to_string()));
                }
            }
            "keyterm" | "keywords" | "redact" | "replace" | "search" | "tag" | "version" => {
                config.extra_query.retain(|(existing, _)| existing != &key);
                if let Value::Array(values) = value {
                    for value in values {
                        config
                            .extra_query
                            .push((key.clone(), value.as_str().ok_or_else(invalid)?.into()));
                    }
                } else if !value.is_null() {
                    config
                        .extra_query
                        .push((key, value.as_str().ok_or_else(invalid)?.into()));
                }
            }
            _ => return Err(invalid()),
        }
    }
    DeepgramClient::new(config)
}
