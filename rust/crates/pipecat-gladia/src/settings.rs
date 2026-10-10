// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::GladiaError;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::io::{self, Write};
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaLanguageConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub languages: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub code_switching: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaPreProcessingConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio_enhancer: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speech_threshold: Option<f64>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaCustomVocabularyConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub vocabulary: Option<Vec<GladiaVocabulary>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_intensity: Option<f64>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaCustomSpellingConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub spelling_dictionary: Option<std::collections::BTreeMap<String, Vec<String>>>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaTranslationConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub target_languages: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub match_original_utterances: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lipsync: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context_adaptation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub context: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub informal: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaRealtimeProcessingConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub words_accurate_timestamps: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_vocabulary: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_vocabulary_config: Option<GladiaCustomVocabularyConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_spelling: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub custom_spelling_config: Option<GladiaCustomSpellingConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub translation: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub translation_config: Option<GladiaTranslationConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub named_entity_recognition: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sentiment_analysis: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaMessagesConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_partial_transcripts: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_final_transcripts: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_speech_events: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_pre_processing_events: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_realtime_processing_events: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_post_processing_events: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_acknowledgments: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_errors: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub receive_lifecycle_events: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum GladiaVocabulary {
    Word(String),
    Item(GladiaVocabularyItem),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GladiaVocabularyItem {
    pub value: String,
    pub intensity: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pronunciations: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
}
/// Complete future-session settings. Delta omissions preserve values and null
/// clears optional values. No update is sent to an already active socket.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct GladiaSttSettings {
    pub model: String,
    /// Source-compatible generic language snapshot; language_config is the wire setting.
    pub language: Option<String>,
    pub language_config: Option<GladiaLanguageConfig>,
    pub custom_metadata: Option<Map<String, Value>>,
    pub endpointing: Option<f64>,
    pub maximum_duration_without_endpointing: Option<u32>,
    pub pre_processing: Option<GladiaPreProcessingConfig>,
    pub realtime_processing: Option<GladiaRealtimeProcessingConfig>,
    pub messages_config: Option<GladiaMessagesConfig>,
    /// Local turn policy only; omitted from REST JSON.
    pub enable_vad: bool,
}
impl Default for GladiaSttSettings {
    fn default() -> Self {
        Self {
            model: "solaria-1".into(),
            language: None,
            language_config: None,
            custom_metadata: None,
            endpointing: None,
            maximum_duration_without_endpointing: Some(5),
            pre_processing: None,
            realtime_processing: None,
            messages_config: None,
            enable_vad: false,
        }
    }
}
impl GladiaSttSettings {
    pub fn validate(&self) -> Result<(), GladiaError> {
        if let Some(metadata) = &self.custom_metadata {
            for value in metadata.values() {
                validate_value(value, 0)?;
            }
        }
        bounded_serialized(self)?;
        if self
            .messages_config
            .as_ref()
            .is_some_and(|m| m.receive_acknowledgments == Some(false))
        {
            return Err(GladiaError::Invalid(
                "acknowledgments are required for bounded audio accounting",
            ));
        }
        if self.model.is_empty()
            || self.model.len() > 256
            || self.model.chars().any(char::is_control)
            || self.language.as_ref().is_some_and(|v| v.len() > 64)
        {
            return Err(GladiaError::Invalid("model or language"));
        }
        if self
            .endpointing
            .is_some_and(|v| !v.is_finite() || !(0.01..=10.0).contains(&v))
            || self
                .maximum_duration_without_endpointing
                .is_some_and(|v| !(5..=60).contains(&v))
        {
            return Err(GladiaError::Invalid("endpointing"));
        }
        if let Some(p) = &self.pre_processing {
            unit(p.speech_threshold)?;
        }
        if let Some(r) = &self.realtime_processing {
            if let Some(c) = &r.custom_vocabulary_config {
                unit(c.default_intensity)?;
                if let Some(items) = &c.vocabulary {
                    for item in items {
                        if let GladiaVocabulary::Item(i) = item {
                            unit(Some(i.intensity))?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, GladiaError> {
        for value in delta.values() {
            validate_value(value, 0)?;
        }
        bounded_serialized(delta)?;
        let mut value = serde_json::to_value(self).map_err(|_| GladiaError::Invalid("settings"))?;
        value.as_object_mut().unwrap().extend(delta.clone());
        let candidate: Self =
            serde_json::from_value(value).map_err(|_| GladiaError::Invalid("settings delta"))?;
        candidate.validate()?;
        Ok(candidate)
    }
}
fn unit(value: Option<f64>) -> Result<(), GladiaError> {
    if value.is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v)) {
        Err(GladiaError::Invalid("normalized setting"))
    } else {
        Ok(())
    }
}
pub(crate) fn validate_value(v: &Value, depth: usize) -> Result<(), GladiaError> {
    if depth > 16 {
        return Err(GladiaError::TooLarge);
    }
    match v {
        Value::Array(a) => {
            if a.len() > 4096 {
                return Err(GladiaError::TooLarge);
            }
            for v in a {
                validate_value(v, depth + 1)?;
            }
        }
        Value::Object(a) => {
            if a.len() > 4096 {
                return Err(GladiaError::TooLarge);
            }
            for v in a.values() {
                validate_value(v, depth + 1)?;
            }
        }
        _ => (),
    };
    Ok(())
}
pub(crate) fn bounded_serialized(value: &impl Serialize) -> Result<(), GladiaError> {
    struct Limit(usize);
    impl Write for Limit {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self.0.saturating_add(bytes.len());
            if self.0 > 65536 {
                Err(io::Error::other("limit"))
            } else {
                Ok(bytes.len())
            }
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Limit(0), value).map_err(|_| GladiaError::TooLarge)
}
