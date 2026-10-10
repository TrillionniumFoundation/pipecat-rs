// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{common, StreamingError, StreamingLimits};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use std::{fmt, time::Duration};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum FluxModel {
    #[default]
    #[serde(rename = "flux-general-en")]
    English,
    #[serde(rename = "flux-general-multi")]
    Multilingual,
}
impl FluxModel {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::English => "flux-general-en",
            Self::Multilingual => "flux-general-multi",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LanguageHint {
    De,
    En,
    Es,
    Fr,
    Hi,
    It,
    Ja,
    Nl,
    Pt,
    Ru,
}
impl LanguageHint {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::De => "de",
            Self::En => "en",
            Self::Es => "es",
            Self::Fr => "fr",
            Self::Hi => "hi",
            Self::It => "it",
            Self::Ja => "ja",
            Self::Nl => "nl",
            Self::Pt => "pt",
            Self::Ru => "ru",
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Redaction {
    Numbers,
    AggressiveNumbers,
}
impl Redaction {
    fn as_str(self) -> &'static str {
        match self {
            Self::Numbers => "numbers",
            Self::AggressiveNumbers => "aggressive_numbers",
        }
    }
}

/// Complete desired settings. `merge` applies a validated JSON delta atomically;
/// omitted fields remain unchanged and explicit null clears optional fields.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct FluxSettings {
    pub model: FluxModel,
    pub eager_eot_threshold: Option<f64>,
    pub eot_threshold: Option<f64>,
    pub eot_timeout_ms: Option<u32>,
    pub keyterm: Vec<String>,
    pub min_confidence: Option<f64>,
    pub numerals: Option<bool>,
    pub profanity_filter: Option<bool>,
    pub redact: Option<Redaction>,
    pub language_hints: Option<Vec<LanguageHint>>,
}
impl FluxSettings {
    pub fn validate(&self) -> Result<(), StreamingError> {
        for value in [
            self.eager_eot_threshold,
            self.eot_threshold,
            self.min_confidence,
        ]
        .into_iter()
        .flatten()
        {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(StreamingError::Invalid(
                    "confidence must be finite and within 0..1",
                ));
            }
        }
        if self
            .eot_timeout_ms
            .is_some_and(|ms| !(100..=30_000).contains(&ms))
        {
            return Err(StreamingError::Invalid(
                "eot_timeout_ms must be within 100..30000",
            ));
        }
        bounded_strings(&self.keyterm)?;
        if self.language_hints.as_ref().is_some_and(|v| v.len() > 10) {
            return Err(StreamingError::Invalid("at most ten language hints"));
        }
        Ok(())
    }
    pub fn merge(&self, delta: &Map<String, Value>) -> Result<Self, StreamingError> {
        let mut remaining = 32 * 1024;
        if delta.len() > 11
            || delta
                .iter()
                .any(|(key, value)| key.len() > 64 || !bounded_value(value, 0, &mut remaining))
        {
            return Err(StreamingError::TooLarge);
        }
        let mut value =
            serde_json::to_value(self).map_err(|_| StreamingError::Invalid("settings JSON"))?;
        value
            .as_object_mut()
            .expect("settings object")
            .extend(delta.clone());
        let updated: Self = serde_json::from_value(value)
            .map_err(|_| StreamingError::Invalid("unknown or incorrectly typed Flux setting"))?;
        updated.validate()?;
        Ok(updated)
    }
    pub(crate) fn reconnects(&self, other: &Self) -> bool {
        self.model != other.model || self.numerals != other.numerals || self.redact != other.redact
    }
    pub(crate) fn configure_delta(&self, desired: &Self) -> Option<Value> {
        let mut msg = Map::new();
        msg.insert("type".into(), json!("Configure"));
        if self.keyterm != desired.keyterm {
            msg.insert("keyterms".into(), json!(desired.keyterm));
        }
        if self.profanity_filter != desired.profanity_filter {
            msg.insert("profanity_filter".into(), json!(desired.profanity_filter));
        }
        let mut thresholds = Map::new();
        if self.eot_threshold != desired.eot_threshold {
            thresholds.insert("eot_threshold".into(), json!(desired.eot_threshold));
        }
        if self.eager_eot_threshold != desired.eager_eot_threshold {
            thresholds.insert(
                "eager_eot_threshold".into(),
                json!(desired.eager_eot_threshold),
            );
        }
        if self.eot_timeout_ms != desired.eot_timeout_ms {
            thresholds.insert("eot_timeout_ms".into(), json!(desired.eot_timeout_ms));
        }
        if !thresholds.is_empty() {
            msg.insert("thresholds".into(), Value::Object(thresholds));
        }
        if desired.model == FluxModel::Multilingual && self.language_hints != desired.language_hints
        {
            msg.insert("language_hints".into(), json!(desired.hints()));
        }
        (msg.len() > 1).then_some(Value::Object(msg))
    }
    fn hints(&self) -> Vec<&'static str> {
        let mut values = vec![];
        for hint in self.language_hints.iter().flatten() {
            if !values.contains(&hint.as_str()) {
                values.push(hint.as_str());
            }
        }
        values
    }
}
fn bounded_strings(values: &[String]) -> Result<(), StreamingError> {
    if values.len() > 100
        || values
            .iter()
            .any(|v| v.is_empty() || v.len() > 256 || v.chars().any(char::is_control))
        || values.iter().map(String::len).sum::<usize>() > 16 * 1024
    {
        return Err(StreamingError::Invalid(
            "strings exceed count, size or character limits",
        ));
    }
    Ok(())
}
#[derive(Clone)]
pub struct FluxConfig {
    pub endpoint: String,
    pub api_key: String,
    pub sample_rate: u32,
    pub settings: FluxSettings,
    pub eager_end_of_turn: bool,
    pub should_interrupt: bool,
    pub mip_opt_out: Option<bool>,
    pub tags: Vec<String>,
    pub watchdog_min_timeout: Duration,
    pub ready_timeout: Duration,
    pub configure_ack_timeout: Duration,
    pub limits: StreamingLimits,
}
impl fmt::Debug for FluxConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("FluxConfig")
            .field("api_key", &"[redacted]")
            .field("sample_rate", &self.sample_rate)
            .field("eager_end_of_turn", &self.eager_end_of_turn)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl FluxConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            endpoint: "wss://api.deepgram.com/v2/listen".into(),
            api_key: api_key.into(),
            sample_rate: 16000,
            settings: FluxSettings::default(),
            eager_end_of_turn: false,
            should_interrupt: true,
            mip_opt_out: None,
            tags: vec![],
            watchdog_min_timeout: Duration::from_millis(500),
            ready_timeout: Duration::from_secs(10),
            configure_ack_timeout: Duration::from_secs(5),
            limits: StreamingLimits::default(),
        }
    }
    pub(crate) fn normalize(&mut self) {
        if self.eager_end_of_turn && self.settings.eager_eot_threshold.is_none() {
            self.settings.eager_eot_threshold = Some(0.5);
        }
    }
    pub fn validate(&self) -> Result<(), StreamingError> {
        self.limits.validate()?;
        if self.limits.max_message_bytes < 2 {
            return Err(StreamingError::Invalid(
                "PCM messages require at least two bytes",
            ));
        }
        self.settings.validate()?;
        bounded_strings(&self.tags)?;
        if self.api_key.is_empty()
            || self.api_key.len() > 8192
            || !(8000..=48000).contains(&self.sample_rate)
        {
            return Err(StreamingError::Invalid("key or sample rate"));
        }
        for timeout in [
            self.watchdog_min_timeout,
            self.ready_timeout,
            self.configure_ack_timeout,
        ] {
            if timeout.is_zero() || timeout > Duration::from_secs(300) {
                return Err(StreamingError::Invalid(
                    "Flux timeouts must be positive and at most five minutes",
                ));
            }
        }
        common::endpoint(&self.endpoint)?;
        Ok(())
    }
    pub fn url(&self) -> Result<String, StreamingError> {
        self.validate()?;
        let mut url = common::endpoint(&self.endpoint)?;
        let s = &self.settings;
        {
            let mut q = url.query_pairs_mut();
            q.append_pair("model", s.model.as_str())
                .append_pair("sample_rate", &self.sample_rate.to_string())
                .append_pair("encoding", "linear16");
            for (key, value) in [
                ("eager_eot_threshold", s.eager_eot_threshold),
                ("eot_threshold", s.eot_threshold),
            ] {
                if let Some(v) = value {
                    q.append_pair(key, &v.to_string());
                }
            }
            if let Some(v) = s.eot_timeout_ms {
                q.append_pair("eot_timeout_ms", &v.to_string());
            }
            for (key, value) in [
                ("numerals", s.numerals),
                ("profanity_filter", s.profanity_filter),
                ("mip_opt_out", self.mip_opt_out),
            ] {
                if let Some(v) = value {
                    q.append_pair(key, if v { "true" } else { "false" });
                }
            }
            if let Some(v) = s.redact {
                q.append_pair("redact", v.as_str());
            }
            for v in &s.keyterm {
                q.append_pair("keyterm", v);
            }
            for v in &self.tags {
                q.append_pair("tag", v);
            }
            if s.model == FluxModel::Multilingual {
                for v in s.hints() {
                    q.append_pair("language_hint", v);
                }
            }
        }
        Ok(url.into())
    }
}

// Bound externally constructed deltas before serializing or cloning them.
fn bounded_value(value: &Value, depth: usize, remaining: &mut usize) -> bool {
    if depth > 16 {
        return false;
    }
    let size = match value {
        Value::String(text) => text.len().saturating_add(2),
        _ => 24,
    };
    let Some(next) = remaining.checked_sub(size) else {
        return false;
    };
    *remaining = next;
    match value {
        Value::Array(values) => {
            values.len() <= 4096
                && values
                    .iter()
                    .all(|v| bounded_value(v, depth + 1, remaining))
        }
        Value::Object(values) => {
            values.len() <= 32
                && values
                    .iter()
                    .all(|(k, v)| k.len() <= 64 && bounded_value(v, depth + 1, remaining))
        }
        _ => true,
    }
}
