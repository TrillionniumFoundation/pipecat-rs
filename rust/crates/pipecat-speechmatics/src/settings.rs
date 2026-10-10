// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::StreamingError;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnDetectionMode {
    #[default]
    External,
    Vad,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KnownSpeaker {
    pub label: String,
    pub speaker_identifiers: Vec<String>,
}
impl std::fmt::Debug for KnownSpeaker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("KnownSpeaker([redacted])")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdditionalVocab {
    pub content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sounds_like: Option<Vec<String>>,
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SpeechmaticsSttSettings {
    pub model: String,
    pub language: String,
    pub domain: Option<String>,
    pub turn_detection_mode: TurnDetectionMode,
    pub speaker_active_format: String,
    pub known_speakers: Option<Vec<KnownSpeaker>>,
    pub additional_vocab: Option<Vec<AdditionalVocab>>,
    pub enable_partials: Option<bool>,
    pub punctuation_overrides: Option<Value>,
    pub enable_diarization: bool,
    pub speaker_sensitivity: Option<f64>,
    pub max_speakers: Option<u32>,
    pub prefer_current_speaker: Option<bool>,
}
impl std::fmt::Debug for SpeechmaticsSttSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SpeechmaticsSttSettings")
            .field("turn_detection_mode", &self.turn_detection_mode)
            .finish_non_exhaustive()
    }
}
impl Default for SpeechmaticsSttSettings {
    fn default() -> Self {
        Self {
            model: "linden-1".into(),
            language: "en".into(),
            domain: None,
            turn_detection_mode: TurnDetectionMode::External,
            speaker_active_format: "{text}".into(),
            known_speakers: None,
            additional_vocab: None,
            enable_partials: None,
            punctuation_overrides: None,
            enable_diarization: false,
            speaker_sensitivity: None,
            max_speakers: None,
            prefer_current_speaker: None,
        }
    }
}
impl SpeechmaticsSttSettings {
    /// Resolves the deprecated operating_point alias without retaining two sources of truth.
    pub fn from_value(value: Value) -> Result<Self, StreamingError> {
        let delta = value
            .as_object()
            .ok_or(StreamingError::Invalid("settings object"))?;
        Self::default().merge(delta)
    }
    pub fn validate(&self) -> Result<(), StreamingError> {
        bounded(&self.model, 128, false)?;
        bounded(&self.language, 32, false)?;
        self.wire_language()?;
        if let Some(v) = &self.domain {
            bounded(v, 128, false)?;
        }
        validate_template(&self.speaker_active_format)?;
        if self
            .speaker_sensitivity
            .is_some_and(|n| !n.is_finite() || !(0.0..=1.0).contains(&n))
            || self.max_speakers.is_some_and(|n| n == 0 || n > 100)
        {
            return Err(StreamingError::Invalid("speaker configuration"));
        }
        if let Some(speakers) = &self.known_speakers {
            if speakers.len() > 100 {
                return Err(StreamingError::TooLarge);
            }
            for s in speakers {
                bounded(&s.label, 256, false)?;
                if s.speaker_identifiers.len() > 64 {
                    return Err(StreamingError::TooLarge);
                }
                for id in &s.speaker_identifiers {
                    bounded(id, 4096, false)?;
                }
            }
        }
        if let Some(vocab) = &self.additional_vocab {
            if vocab.len() > 1024 {
                return Err(StreamingError::TooLarge);
            }
            for v in vocab {
                bounded(&v.content, 1024, false)?;
                if let Some(hints) = &v.sounds_like {
                    if hints.len() > 32 {
                        return Err(StreamingError::TooLarge);
                    }
                    for hint in hints {
                        bounded(hint, 1024, false)?;
                    }
                }
            }
        }
        if let Some(v) = &self.punctuation_overrides {
            let obj = v
                .as_object()
                .ok_or(StreamingError::Invalid("punctuation overrides object"))?;
            for (key, value) in obj {
                match key.as_str() {
                    "permitted_marks" => {
                        let marks = value
                            .as_array()
                            .ok_or(StreamingError::Invalid("permitted marks"))?;
                        if marks.len() > 64 {
                            return Err(StreamingError::TooLarge);
                        }
                        for mark in marks {
                            bounded(
                                mark.as_str()
                                    .ok_or(StreamingError::Invalid("punctuation mark"))?,
                                16,
                                false,
                            )?;
                        }
                    }
                    "sensitivity" => {
                        let n = value
                            .as_f64()
                            .ok_or(StreamingError::Invalid("punctuation sensitivity"))?;
                        if !n.is_finite() || !(0.0..=1.0).contains(&n) {
                            return Err(StreamingError::Invalid("punctuation sensitivity"));
                        }
                    }
                    _ => return Err(StreamingError::Invalid("unknown punctuation setting")),
                }
            }
        }
        if serde_json::to_vec(self)
            .map_err(|_| StreamingError::Invalid("settings serialization"))?
            .len()
            > 128 * 1024
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(())
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, StreamingError> {
        if delta.len() > 32 || delta.values().any(|v| v.to_string().len() > 128 * 1024) {
            return Err(StreamingError::TooLarge);
        }
        let mut delta = delta.clone();
        if let Some(alias) = delta.remove("operating_point") {
            let alias = alias
                .as_str()
                .ok_or(StreamingError::Invalid("operating point"))?;
            if delta
                .get("model")
                .is_some_and(|v| v.as_str() != Some(alias))
            {
                return Err(StreamingError::Invalid("conflicting model alias"));
            }
            delta.insert("model".into(), Value::String(alias.into()));
        }
        let mut base = serde_json::to_value(self)
            .map_err(|_| StreamingError::Invalid("settings serialization"))?;
        base.as_object_mut().unwrap().extend(delta);
        let result: Self = serde_json::from_value(base)
            .map_err(|_| StreamingError::Invalid("settings value or field"))?;
        result.validate()?;
        Ok(result)
    }
    pub fn wire_language(&self) -> Result<&str, StreamingError> {
        let base = self.language.split('-').next().unwrap_or_default();
        match base {
            "fil" => Ok("tl"),
            "ar" | "ba" | "eu" | "be" | "bg" | "bn" | "yue" | "ca" | "hr" | "cs" | "da" | "nl"
            | "en" | "eo" | "et" | "fa" | "fi" | "fr" | "gl" | "de" | "el" | "he" | "hi" | "hu"
            | "it" | "id" | "ga" | "ja" | "ko" | "lv" | "lt" | "ms" | "mt" | "cmn" | "mr"
            | "mn" | "no" | "pl" | "pt" | "ro" | "ru" | "sk" | "sl" | "es" | "sv" | "sw" | "tl"
            | "ta" | "th" | "tr" | "ug" | "uk" | "ur" | "vi" | "cy" => Ok(base),
            _ => Err(StreamingError::Invalid("unsupported language")),
        }
    }
    pub fn output_locale(&self) -> Option<&str> {
        matches!(self.language.as_str(), "en-GB" | "en-US" | "en-AU").then_some(&self.language)
    }
    pub fn locale_falls_back(&self) -> bool {
        self.language.starts_with("en-") && self.output_locale().is_none()
    }
    pub fn start_message(&self) -> Result<Value, StreamingError> {
        self.validate()?;
        let mut tc = json!({"language":self.wire_language()?,"model":self.model});
        let obj = tc.as_object_mut().unwrap();
        if let Some(locale) = self.output_locale() {
            obj.insert("output_locale".into(), json!(locale));
        }
        if let Some(v) = &self.domain {
            obj.insert("domain".into(), json!(v));
        }
        if let Some(v) = self.enable_partials {
            obj.insert("enable_partials".into(), json!(v));
        }
        if let Some(v) = &self.additional_vocab {
            obj.insert("additional_vocab".into(), json!(v));
        }
        if let Some(v) = &self.punctuation_overrides {
            obj.insert("punctuation_overrides".into(), v.clone());
        }
        if self.enable_diarization {
            obj.insert("diarization".into(), json!("speaker"));
            let mut speaker = Map::new();
            if let Some(v) = self.speaker_sensitivity {
                speaker.insert("speaker_sensitivity".into(), json!(v));
            }
            if let Some(v) = self.max_speakers {
                speaker.insert("max_speakers".into(), json!(v));
            }
            if let Some(v) = self.prefer_current_speaker {
                speaker.insert("prefer_current_speaker".into(), json!(v));
            }
            if let Some(v) = &self.known_speakers {
                speaker.insert("speakers".into(), json!(v));
            }
            if !speaker.is_empty() {
                obj.insert("speaker_diarization_config".into(), Value::Object(speaker));
            }
        }
        Ok(
            json!({"message":"StartRecognition","audio_format":{"type":"raw","encoding":"pcm_s16le","sample_rate":16000},"transcription_config":tc,"turn_config":{"turn_detection_mode":self.turn_detection_mode}}),
        )
    }
    pub fn format_text(
        &self,
        text: &str,
        speaker: Option<&str>,
        timestamp: &str,
        max: usize,
    ) -> Result<String, StreamingError> {
        render_template(
            &self.speaker_active_format,
            |key| match key {
                "text" => text,
                "speaker_id" => speaker.filter(|s| !s.is_empty()).unwrap_or("UU"),
                "ts" => timestamp,
                "lang" => &self.language,
                _ => "",
            },
            max,
        )
    }
}
fn bounded(s: &str, max: usize, empty: bool) -> Result<(), StreamingError> {
    if s.len() > max {
        Err(StreamingError::TooLarge)
    } else if (!empty && s.is_empty()) || s.chars().any(|c| c.is_control()) {
        Err(StreamingError::Invalid("bounded setting text"))
    } else {
        Ok(())
    }
}
fn validate_template(s: &str) -> Result<(), StreamingError> {
    if s.len() > 4096 {
        return Err(StreamingError::TooLarge);
    }
    render_template(s, |_| "", 4096).map(|_| ())
}
fn render_template<'a>(
    s: &str,
    field: impl Fn(&str) -> &'a str,
    max: usize,
) -> Result<String, StreamingError> {
    let mut output = String::new();
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        match c {
            '{' if chars.peek() == Some(&'{') => {
                chars.next();
                output.push('{');
            }
            '}' if chars.peek() == Some(&'}') => {
                chars.next();
                output.push('}');
            }
            '{' => {
                let mut key = String::new();
                let mut closed = false;
                for c in chars.by_ref() {
                    if c == '}' {
                        closed = true;
                        break;
                    }
                    key.push(c);
                }
                if !closed || !matches!(key.as_str(), "text" | "speaker_id" | "ts" | "lang") {
                    return Err(StreamingError::Invalid("speaker format template"));
                }
                output.push_str(field(&key));
            }
            '}' => return Err(StreamingError::Invalid("speaker format template")),
            c => output.push(c),
        }
        if output.len() > max {
            return Err(StreamingError::TooLarge);
        }
    }
    Ok(output)
}
