// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::StreamingError;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

pub const MAX_AGENT_CONTEXT_CHARS: usize = 1500;
pub fn is_u3_pro_model(model: &str) -> bool {
    ["u3-rt-pro", "universal-3-5-pro", "universal-3-6-pro"]
        .iter()
        .any(|prefix| model.starts_with(prefix))
}
/// Resolve regional variants and preserve first declaration order. Sync has no
/// ten-language service cap; callers choose the appropriate limit.
pub fn prepare_language_codes(
    codes: &[String],
    limit: Option<usize>,
) -> Result<Vec<String>, StreamingError> {
    let mut output = Vec::new();
    for code in codes {
        if code.is_empty() || code.len() > 64 {
            return Err(StreamingError::Invalid("language code"));
        }
        let mut parts = code.split(['-', '_']);
        let base = parts.next().unwrap_or_default();
        if base.is_empty()
            || !base.bytes().all(|b| b.is_ascii_alphabetic())
            || parts.any(|part| part.is_empty() || !part.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            return Err(StreamingError::Invalid("language code"));
        }
        let base = base.to_ascii_lowercase();
        if !output.contains(&base) {
            output.push(base);
        }
    }
    if limit.is_some_and(|n| output.len() > n) {
        return Err(StreamingError::Invalid(
            "at most ten distinct streaming languages",
        ));
    }
    Ok(output)
}
pub(crate) fn clip_context(text: &str) -> String {
    let skip = text.chars().count().saturating_sub(MAX_AGENT_CONTEXT_CHARS);
    text.chars().skip(skip).collect()
}
/// Fully resolved settings. Runtime deltas are JSON objects: omission retains a
/// value, null clears an optional value, and [] clears language steering.
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct StreamingSettings {
    pub model: String,
    pub language: Option<String>,
    pub formatted_finals: bool,
    pub word_finalization_max_wait_time: Option<u32>,
    pub end_of_turn_confidence_threshold: Option<f64>,
    pub min_turn_silence: Option<u32>,
    pub max_turn_silence: Option<u32>,
    pub keyterms_prompt: Option<Vec<String>>,
    pub prompt: Option<String>,
    pub language_detection: Option<bool>,
    pub language_code: Option<String>,
    pub language_codes: Option<Vec<String>>,
    pub format_turns: bool,
    pub speaker_labels: Option<bool>,
    pub vad_threshold: Option<f64>,
    pub domain: Option<String>,
    pub continuous_partials: bool,
    pub interruption_delay: Option<u32>,
    pub agent_context: Option<String>,
    pub previous_context_n_turns: Option<u32>,
    pub voice_focus: Option<String>,
    pub voice_focus_threshold: Option<f64>,
    pub mode: Option<String>,
}
impl std::fmt::Debug for StreamingSettings {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamingSettings").finish_non_exhaustive()
    }
}
impl Default for StreamingSettings {
    fn default() -> Self {
        Self {
            model: "universal-3-6-pro".into(),
            language: Some("en".into()),
            formatted_finals: true,
            word_finalization_max_wait_time: None,
            end_of_turn_confidence_threshold: None,
            min_turn_silence: None,
            max_turn_silence: None,
            keyterms_prompt: None,
            prompt: None,
            language_detection: None,
            language_code: None,
            language_codes: None,
            format_turns: true,
            speaker_labels: None,
            vad_threshold: None,
            domain: None,
            continuous_partials: true,
            interruption_delay: None,
            agent_context: None,
            previous_context_n_turns: None,
            voice_focus: None,
            voice_focus_threshold: None,
            mode: None,
        }
    }
}
impl StreamingSettings {
    pub(crate) fn normalized(mut self, pipecat_turns: bool) -> Result<Self, StreamingError> {
        let u3 = is_u3_pro_model(&self.model);
        if self.model.is_empty()
            || self.model.len() > 128
            || (!pipecat_turns && !u3)
            || (!u3 && self.prompt.is_some())
        {
            return Err(StreamingError::Invalid(
                "model, prompt or provider turn mode",
            ));
        }
        if self.interruption_delay.is_some_and(|v| v > 1000)
            || self.previous_context_n_turns.is_some_and(|v| v > 100)
        {
            return Err(StreamingError::Invalid("interruption/context range"));
        }
        for value in [
            self.voice_focus_threshold,
            self.vad_threshold,
            self.end_of_turn_confidence_threshold,
        ]
        .into_iter()
        .flatten()
        {
            if !value.is_finite() || !(0.0..=1.0).contains(&value) {
                return Err(StreamingError::Invalid("confidence range"));
            }
        }
        if self
            .voice_focus
            .as_deref()
            .is_some_and(|v| !matches!(v, "near-field" | "far-field"))
            || self
                .mode
                .as_deref()
                .is_some_and(|v| !matches!(v, "min_latency" | "balanced" | "max_accuracy"))
        {
            return Err(StreamingError::Invalid("voice focus or mode"));
        }
        if let Some(codes) = &self.language_codes {
            prepare_language_codes(codes, Some(10))?;
        }
        if let Some(text) = &mut self.agent_context {
            *text = clip_context(text);
        }
        // Source configures these once at construction. Native revalidates on
        // every reconnect to preserve the selected turn-detection contract.
        if pipecat_turns {
            if u3 {
                let min = self.min_turn_silence.unwrap_or(100);
                self.min_turn_silence = Some(min);
                self.max_turn_silence = Some(min);
            } else {
                self.end_of_turn_confidence_threshold = Some(1.0);
                self.min_turn_silence = Some(160);
            }
        }
        if serde_json::to_vec(&self)
            .map_err(|_| StreamingError::Invalid("settings"))?
            .len()
            > 64 * 1024
        {
            return Err(StreamingError::TooLarge);
        }
        Ok(self)
    }
    pub(crate) fn patch(
        &self,
        mut delta: Map<String, Value>,
        mode: bool,
    ) -> Result<(Self, bool), StreamingError> {
        let mut dropped_languages = false;
        if let Some(Value::Array(values)) = delta.get("language_codes") {
            let codes = values
                .iter()
                .map(|v| {
                    v.as_str()
                        .map(str::to_owned)
                        .ok_or(StreamingError::Invalid("language list"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            if prepare_language_codes(&codes, None)?.len() > 10 {
                delta.remove("language_codes");
                dropped_languages = true;
            }
        }
        let Value::Object(mut value) =
            serde_json::to_value(self).map_err(|_| StreamingError::Invalid("settings"))?
        else {
            unreachable!()
        };
        value.extend(delta);
        let candidate: Self = serde_json::from_value(Value::Object(value))
            .map_err(|_| StreamingError::Invalid("settings delta"))?;
        Ok((candidate.normalized(mode)?, dropped_languages))
    }
    pub(crate) fn reconnect_differs(&self, other: &Self) -> bool {
        let mut left = self.clone();
        let mut right = other.clone();
        left.agent_context = None;
        right.agent_context = None;
        left.language_codes = None;
        right.language_codes = None;
        left != right
    }
    pub(crate) fn query(&self) -> Result<Vec<(String, String)>, StreamingError> {
        let Value::Object(mut fields) =
            serde_json::to_value(self).map_err(|_| StreamingError::Invalid("settings"))?
        else {
            unreachable!()
        };
        fields.remove("language");
        fields.remove("model");
        if !is_u3_pro_model(&self.model) {
            for key in [
                "continuous_partials",
                "interruption_delay",
                "agent_context",
                "previous_context_n_turns",
                "voice_focus",
                "voice_focus_threshold",
                "mode",
                "language_codes",
            ] {
                fields.remove(key);
            }
        }
        if let Some(codes) = &self.language_codes {
            if is_u3_pro_model(&self.model) {
                fields.insert(
                    "language_codes".into(),
                    serde_json::to_value(prepare_language_codes(codes, Some(10))?).unwrap(),
                );
            }
        }
        let mut pairs = vec![("speech_model".into(), self.model.clone())];
        for (key, value) in fields {
            match value {
                Value::Null => (),
                Value::String(s) => pairs.push((key, s)),
                other => pairs.push((key, other.to_string())),
            }
        }
        Ok(pairs)
    }
}
