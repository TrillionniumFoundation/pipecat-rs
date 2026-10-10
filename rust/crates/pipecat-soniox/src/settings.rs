// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{SonioxTtsClient, StreamingError};
use pipecat_core::{FrameProcessor, ProcessorReference, Setting, TtsSettings};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SonioxContextItem {
    pub key: String,
    pub value: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SonioxTranslationTerm {
    pub source: String,
    pub target: String,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SonioxContextObject {
    pub general: Option<Vec<SonioxContextItem>>,
    pub text: Option<String>,
    pub terms: Option<Vec<String>>,
    pub translation_terms: Option<Vec<SonioxTranslationTerm>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SonioxContext {
    Text(String),
    Structured(SonioxContextObject),
}

/// Complete desired settings. A JSON delta preserves omitted fields and clears
/// optional fields on explicit null. Unknown or incorrectly typed fields fail.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SonioxSttSettings {
    pub model: String,
    /// The source's generic STT language is accepted but is not a wire field.
    pub language: Option<String>,
    pub language_hints: Option<Vec<String>>,
    pub language_hints_strict: Option<bool>,
    pub context: Option<SonioxContext>,
    pub enable_speaker_diarization: Option<bool>,
    pub enable_language_identification: Option<bool>,
    pub max_endpoint_delay_ms: Option<u32>,
    pub endpoint_sensitivity: Option<f64>,
    pub endpoint_latency_adjustment_level: Option<u8>,
    pub client_reference_id: Option<String>,
}
impl Default for SonioxSttSettings {
    fn default() -> Self {
        Self {
            model: "stt-rt-v5".into(),
            language: None,
            language_hints: None,
            language_hints_strict: None,
            context: None,
            enable_speaker_diarization: Some(false),
            enable_language_identification: Some(false),
            max_endpoint_delay_ms: None,
            endpoint_sensitivity: None,
            endpoint_latency_adjustment_level: None,
            client_reference_id: None,
        }
    }
}
impl SonioxSttSettings {
    pub fn validate(&self) -> Result<(), StreamingError> {
        self.validate_borrowed()?;
        name(&self.model)?;
        if let Some(v) = &self.language {
            language(v, false)?;
        }
        if let Some(hints) = &self.language_hints {
            if hints.len() > 64 {
                return Err(StreamingError::TooLarge);
            }
            for hint in hints {
                language(hint, false)?;
            }
        }
        if self
            .max_endpoint_delay_ms
            .is_some_and(|n| !(500..=3000).contains(&n))
            || self
                .endpoint_sensitivity
                .is_some_and(|n| !n.is_finite() || !(-1.0..=1.0).contains(&n))
            || self
                .endpoint_latency_adjustment_level
                .is_some_and(|n| n > 3)
        {
            return Err(invalid());
        }
        if let Some(v) = &self.client_reference_id {
            name(v)?;
        }
        Ok(())
    }
    fn validate_borrowed(&self) -> Result<(), StreamingError> {
        let mut bytes = 0usize;
        let mut count = |value: &str| -> Result<(), StreamingError> {
            bytes = bytes.saturating_add(value.len());
            if bytes > 16384 {
                Err(StreamingError::TooLarge)
            } else {
                Ok(())
            }
        };
        count(&self.model)?;
        for v in [&self.language, &self.client_reference_id]
            .into_iter()
            .flatten()
        {
            count(v)?;
        }
        if let Some(hints) = &self.language_hints {
            if hints.len() > 64 {
                return Err(StreamingError::TooLarge);
            }
            for hint in hints {
                count(hint)?;
            }
        }
        if let Some(context) = &self.context {
            match context {
                SonioxContext::Text(v) => count(v)?,
                SonioxContext::Structured(v) => {
                    if let Some(text) = &v.text {
                        count(text)?;
                    }
                    if let Some(terms) = &v.terms {
                        if terms.len() > 512 {
                            return Err(StreamingError::TooLarge);
                        }
                        for term in terms {
                            count(term)?;
                        }
                    }
                    if let Some(items) = &v.general {
                        if items.len() > 512 {
                            return Err(StreamingError::TooLarge);
                        }
                        for item in items {
                            count(&item.key)?;
                            count(&item.value)?;
                        }
                    }
                    if let Some(items) = &v.translation_terms {
                        if items.len() > 512 {
                            return Err(StreamingError::TooLarge);
                        }
                        for item in items {
                            count(&item.source)?;
                            count(&item.target)?;
                        }
                    }
                }
            }
        }
        Ok(())
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, StreamingError> {
        self.validate()?;
        let result: Self = merge(self, delta)?;
        result.validate()?;
        Ok(result)
    }
    pub(crate) fn normalized_hints(&self) -> Option<Vec<String>> {
        self.language_hints.as_ref().map(|hints| {
            let mut result = vec![];
            for hint in hints {
                let code = language(hint, false).expect("validated language");
                if !result.contains(&code) {
                    result.push(code);
                }
            }
            result
        })
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct SonioxTtsSettings {
    pub model: String,
    pub voice: String,
    pub language: Option<String>,
    pub speed: Option<f64>,
    pub reduce_silence: Option<bool>,
    pub client_reference_id: Option<String>,
}
impl Default for SonioxTtsSettings {
    fn default() -> Self {
        Self {
            model: "tts-rt-v2".into(),
            voice: "Bryce".into(),
            language: Some("en".into()),
            speed: None,
            reduce_silence: None,
            client_reference_id: None,
        }
    }
}
impl SonioxTtsSettings {
    pub fn validate(&self) -> Result<(), StreamingError> {
        name(&self.model)?;
        name(&self.voice)?;
        if let Some(v) = &self.language {
            language(v, true)?;
        }
        if self
            .speed
            .is_some_and(|n| !n.is_finite() || !(0.7..=1.3).contains(&n))
        {
            return Err(invalid());
        }
        if let Some(v) = &self.client_reference_id {
            name(v)?;
        }
        Ok(())
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, StreamingError> {
        self.validate()?;
        let result: Self = merge(self, delta)?;
        result.validate()?;
        Ok(result)
    }
}
fn merge<T: Serialize + for<'de> Deserialize<'de>>(
    old: &T,
    delta: &Map<String, Value>,
) -> Result<T, StreamingError> {
    // Inspect the borrowed tree before cloning user-supplied values.
    let mut remaining = 32768;
    for (key, value) in delta {
        if key.len() > 64 || !bounded_value(value, 0, &mut remaining) {
            return Err(StreamingError::TooLarge);
        }
    }
    let mut value = serde_json::to_value(old).map_err(|_| invalid())?;
    value
        .as_object_mut()
        .ok_or_else(invalid)?
        .extend(delta.clone());
    serde_json::from_value(value).map_err(|_| invalid())
}
pub(crate) fn validate_values<'a>(
    values: impl IntoIterator<Item = (&'a String, &'a Value)>,
) -> Result<(), StreamingError> {
    let mut remaining = 32768;
    let mut count = 0;
    for (key, value) in values {
        count += 1;
        if count > 32 || key.len() > 64 || !bounded_value(value, 0, &mut remaining) {
            return Err(StreamingError::TooLarge);
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
pub(crate) fn name(value: &str) -> Result<(), StreamingError> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        Err(invalid())
    } else {
        Ok(())
    }
}
fn invalid() -> StreamingError {
    StreamingError::Invalid("unsupported or incorrectly typed Soniox setting")
}
pub(crate) fn language(value: &str, tts: bool) -> Result<String, StreamingError> {
    name(value)?;
    let base = value
        .split(['-', '_'])
        .next()
        .unwrap_or_default()
        .to_ascii_lowercase();
    const SHARED: &str = "af ar az be bg bn bs ca cs cy da de el en es et eu fa fi fr gl gu he hi hr hu id it ja kk kn ko lt lv mk ml mr ms nl no pa pl pt ro ru sk sl sq sr sv sw ta te th tl tr uk ur vi zh";
    if SHARED.split_whitespace().any(|c| c == base)
        || (tts && matches!(base.as_str(), "is" | "su" | "uz"))
    {
        Ok(base)
    } else {
        Err(invalid())
    }
}
pub(crate) fn targets(processor: &dyn FrameProcessor, target: &Option<ProcessorReference>) -> bool {
    match target {
        None => true,
        Some(ProcessorReference::Name(n)) => n == processor.name(),
        Some(ProcessorReference::Local(p)) => std::ptr::addr_eq(p.0.as_ref(), processor),
    }
}
pub(crate) fn tts_mapping(values: &BTreeMap<String, Value>) -> Result<TtsSettings, StreamingError> {
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
    client: &SonioxTtsClient,
    delta: &TtsSettings,
) -> Result<SonioxTtsClient, StreamingError> {
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

// Pinned Language enum values, including region tags, for provider results.
pub(crate) fn transcript_language(value: &str) -> Option<String> {
    const LANGUAGES: &[&str] = &[
        "af",
        "af-ZA",
        "am",
        "am-ET",
        "ar",
        "ar-AE",
        "ar-BH",
        "ar-DZ",
        "ar-EG",
        "ar-IQ",
        "ar-JO",
        "ar-KW",
        "ar-LB",
        "ar-LY",
        "ar-MA",
        "ar-OM",
        "ar-QA",
        "ar-SA",
        "ar-SY",
        "ar-TN",
        "ar-XA",
        "ar-YE",
        "ar-001",
        "as",
        "as-IN",
        "ast",
        "az",
        "az-AZ",
        "ba",
        "be",
        "be-BY",
        "bg",
        "bg-BG",
        "bn",
        "bn-BD",
        "bn-IN",
        "bo",
        "br",
        "brx",
        "brx-IN",
        "bs",
        "bs-BA",
        "ca",
        "ca-ES",
        "ceb",
        "ceb-PH",
        "ckb",
        "ckb-IQ",
        "ckb-IR",
        "cmn",
        "cmn-CN",
        "cs",
        "cs-CZ",
        "cy",
        "cy-GB",
        "da",
        "da-DK",
        "de",
        "de-AT",
        "de-BE",
        "de-CH",
        "de-DE",
        "doi",
        "doi-IN",
        "el",
        "el-GR",
        "en",
        "en-AB",
        "en-AU",
        "en-CA",
        "en-GB",
        "en-GH",
        "en-HK",
        "en-IE",
        "en-IN",
        "en-KE",
        "en-NG",
        "en-NZ",
        "en-PH",
        "en-SG",
        "en-TZ",
        "en-US",
        "en-WL",
        "en-ZA",
        "eo",
        "es",
        "es-AR",
        "es-BO",
        "es-CL",
        "es-CO",
        "es-CR",
        "es-CU",
        "es-DO",
        "es-EC",
        "es-ES",
        "es-GQ",
        "es-GT",
        "es-HN",
        "es-MX",
        "es-NI",
        "es-PA",
        "es-PE",
        "es-PR",
        "es-PY",
        "es-SV",
        "es-US",
        "es-UY",
        "es-VE",
        "es-419",
        "et",
        "et-EE",
        "eu",
        "eu-ES",
        "fa",
        "fa-AF",
        "fa-IR",
        "ff",
        "fi",
        "fi-FI",
        "fil",
        "fil-PH",
        "fo",
        "fr",
        "fr-BE",
        "fr-CA",
        "fr-CH",
        "fr-FR",
        "ga",
        "ga-IE",
        "gd",
        "gl",
        "gl-ES",
        "gu",
        "gu-IN",
        "ha",
        "haw",
        "he",
        "he-IL",
        "hi",
        "hi-IN",
        "hr",
        "hr-HR",
        "ht",
        "ht-HT",
        "hu",
        "hu-HU",
        "hy",
        "hy-AM",
        "id",
        "id-ID",
        "ig",
        "is",
        "is-IS",
        "it",
        "it-IT",
        "it-CH",
        "iu-Cans",
        "iu-Cans-CA",
        "iu-Latn",
        "iu-Latn-CA",
        "ja",
        "ja-JP",
        "jv",
        "jv-ID",
        "jv-JV",
        "jw",
        "ka",
        "ka-GE",
        "kea",
        "kab",
        "kab-DZ",
        "kk",
        "kk-KZ",
        "km",
        "km-KH",
        "kn",
        "kn-IN",
        "kok",
        "kok-IN",
        "ko",
        "ko-KR",
        "ks",
        "ks-IN",
        "ku",
        "ky",
        "ky-KG",
        "la",
        "la-VA",
        "lb",
        "lb-LU",
        "ln",
        "lo",
        "lo-LA",
        "lt",
        "lt-LT",
        "lg",
        "luo",
        "lv",
        "lv-LV",
        "mg",
        "mg-MG",
        "mi",
        "mk",
        "mk-MK",
        "mai",
        "mai-IN",
        "ml",
        "ml-IN",
        "mn",
        "mn-MN",
        "mni",
        "mni-IN",
        "mr",
        "mr-IN",
        "ms",
        "ms-MY",
        "mt",
        "mt-MT",
        "my",
        "my-MM",
        "mymr",
        "nb",
        "nb-NO",
        "no",
        "nn",
        "nn-NO",
        "ne",
        "ne-IN",
        "ne-NP",
        "nl",
        "nl-BE",
        "nl-NL",
        "nso",
        "ny",
        "oc",
        "or",
        "or-IN",
        "pa",
        "pa-IN",
        "pl",
        "pl-PL",
        "ps",
        "ps-AF",
        "pt",
        "pt-BR",
        "pt-PT",
        "ro",
        "ro-RO",
        "ru",
        "ru-RU",
        "sa",
        "sa-IN",
        "sat",
        "sat-IN",
        "sd",
        "sd-IN",
        "si",
        "si-LK",
        "sk",
        "sk-SK",
        "sl",
        "sl-SI",
        "sn",
        "so",
        "so-SO",
        "sq",
        "sq-AL",
        "sr",
        "sr-RS",
        "sr-Latn",
        "sr-Latn-RS",
        "su",
        "su-ID",
        "sv",
        "sv-SE",
        "sw",
        "sw-BI",
        "sw-KE",
        "sw-RW",
        "sw-TZ",
        "sw-UG",
        "ta",
        "ta-IN",
        "ta-LK",
        "ta-MY",
        "ta-SG",
        "te",
        "te-IN",
        "tg",
        "th",
        "th-TH",
        "tk",
        "tl",
        "tr",
        "tr-TR",
        "tt",
        "ug",
        "uk",
        "uk-UA",
        "umb",
        "ur",
        "ur-IN",
        "ur-PK",
        "uz",
        "uz-UZ",
        "vi",
        "vi-VN",
        "wo",
        "wuu",
        "wuu-CN",
        "yi",
        "yo",
        "yue",
        "yue-CN",
        "zh",
        "zh-CN",
        "zh-CN-guangxi",
        "zh-CN-henan",
        "zh-CN-liaoning",
        "zh-CN-shaanxi",
        "zh-CN-shandong",
        "zh-CN-sichuan",
        "zh-HK",
        "zh-TW",
        "xh-ZA",
        "zu",
        "zu-ZA",
    ];
    LANGUAGES.contains(&value).then(|| value.to_owned())
}

pub(crate) fn validate_setting(value: &Setting<String>) -> Result<(), StreamingError> {
    if let Setting::Value(v) = value {
        name(v)?;
    }
    Ok(())
}
