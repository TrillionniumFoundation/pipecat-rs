//! Atomic, typed settings deltas; credentials and connection format stay fixed.
use crate::{PartialResultsStability, PollyClient, SpeechError, StreamingError, TranscribeClient};
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
pub(crate) fn polly(
    client: &PollyClient,
    delta: &TtsSettings,
) -> std::result::Result<Option<PollyClient>, SpeechError> {
    let update = || -> Result<_> {
        let mut config = client.config().clone();
        for (key, value) in promoted(&delta.base, &delta.language, Some(&delta.voice))? {
            match key.as_str() {
                "model" if value.is_null() => {}
                "voice" => required(&mut config.settings.voice, &string(&value)?)?,
                "language" => required(&mut config.settings.language, &string(&value)?)?,
                "engine" => optional(&mut config.settings.engine, &string(&value)?)?,
                "pitch" => optional(&mut config.settings.pitch, &string(&value)?)?,
                "rate" => optional(&mut config.settings.rate, &string(&value)?)?,
                "volume" => optional(&mut config.settings.volume, &string(&value)?)?,
                "lexicon_names" => {
                    config.settings.lexicon_names = if value.is_null() {
                        None
                    } else {
                        Some(
                            value
                                .as_array()
                                .ok_or_else(invalid)?
                                .iter()
                                .map(|v| v.as_str().map(str::to_owned).ok_or_else(invalid))
                                .collect::<Result<Vec<_>>>()?,
                        )
                    }
                }
                _ => return Err(invalid()),
            }
        }
        Ok(config)
    };
    let config = update().map_err(|_| SpeechError::Invalid("Polly settings"))?;
    config.settings.validate()?;
    if config.settings == client.config().settings {
        return Ok(None);
    }
    PollyClient::new(config).map(Some)
}
pub(crate) fn stt(client: &TranscribeClient, delta: &SttSettings) -> Result<TranscribeClient> {
    let mut config = client.config().clone();
    for (key, value) in promoted(&delta.base, &delta.language, None)? {
        match key.as_str() {
            "model" if value.is_null() => {}
            "language" => required(&mut config.language, &string(&value)?)?,
            "partial_results_stability" => {
                config.partial_results_stability = match value.as_str() {
                    Some("high") => PartialResultsStability::High,
                    Some("medium") => PartialResultsStability::Medium,
                    Some("low") => PartialResultsStability::Low,
                    _ => return Err(invalid()),
                }
            }
            _ => return Err(invalid()),
        }
    }
    TranscribeClient::new(config)
}
