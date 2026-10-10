use crate::{Result, WhisperError};
use serde::{Deserialize, Serialize};
use std::{path::PathBuf, str::FromStr, time::Duration};

pub const SAMPLE_RATE: u32 = 16_000;

/// A language recognized by the linked Whisper tokenizer. Regional tags are
/// normalized to their base language (`en-US` becomes `en`). None in settings
/// requests automatic detection; `auto` is deliberately not a language code.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct Language(String);
impl Language {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}
impl FromStr for Language {
    type Err = WhisperError;
    fn from_str(value: &str) -> Result<Self> {
        if value.is_empty()
            || value.len() > 35
            || !value
                .split('-')
                .all(|s| !s.is_empty() && s.bytes().all(|b| b.is_ascii_alphanumeric()))
        {
            return Err(WhisperError::Invalid("invalid language tag"));
        }
        let code = value.split('-').next().unwrap().to_ascii_lowercase();
        if !(2..=3).contains(&code.len())
            || !code.bytes().all(|b| b.is_ascii_alphabetic())
            || whisper_rs::get_lang_id(&code).is_none()
        {
            return Err(WhisperError::UnsupportedLanguage(value.into()));
        }
        Ok(Self(code))
    }
}
impl TryFrom<String> for Language {
    type Error = WhisperError;
    fn try_from(value: String) -> Result<Self> {
        value.parse()
    }
}
impl From<Language> for String {
    fn from(value: Language) -> Self {
        value.0
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DecodingStrategy {
    Greedy { best_of: u8 },
    BeamSearch { beam_size: u8 },
}
impl Default for DecodingStrategy {
    fn default() -> Self {
        Self::BeamSearch { beam_size: 5 }
    }
}

/// Per-request settings are snapshotted when a segment is accepted.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct WhisperSettings {
    pub language: Option<Language>,
    /// Segments with probability >= this threshold are omitted. None disables
    /// the probability filter. This is distinct from Whisper's decoding fallback.
    pub no_speech_probability: Option<f32>,
    pub initial_prompt: Option<String>,
    pub temperature: f32,
    pub translate_to_english: bool,
    pub strategy: DecodingStrategy,
}
impl Default for WhisperSettings {
    fn default() -> Self {
        Self {
            language: Some(Language("en".into())),
            no_speech_probability: Some(0.4),
            initial_prompt: None,
            temperature: 0.0,
            translate_to_english: false,
            strategy: DecodingStrategy::default(),
        }
    }
}
impl WhisperSettings {
    pub fn validate(&self) -> Result<()> {
        if !self.temperature.is_finite() || !(0.0..=1.0).contains(&self.temperature) {
            return Err(WhisperError::Invalid(
                "temperature must be finite and between 0 and 1",
            ));
        }
        if self
            .no_speech_probability
            .is_some_and(|p| !p.is_finite() || !(0.0..=1.0).contains(&p))
        {
            return Err(WhisperError::Invalid(
                "no-speech threshold must be finite and between 0 and 1",
            ));
        }
        if self
            .initial_prompt
            .as_ref()
            .is_some_and(|p| p.len() > 16_384 || p.contains('\0'))
        {
            return Err(WhisperError::Invalid(
                "initial prompt must be NUL-free and at most 16384 bytes",
            ));
        }
        let width = match self.strategy {
            DecodingStrategy::Greedy { best_of } => best_of,
            DecodingStrategy::BeamSearch { beam_size } => beam_size,
        };
        // The pinned whisper.cpp 1.8.3 engine supports at most eight decoders.
        // Reject wider configurations here, before accepting native work.
        if !(1..=8).contains(&width) {
            return Err(WhisperError::Invalid(
                "decoding width must be between 1 and 8",
            ));
        }
        Ok(())
    }
    pub(crate) fn validate_model(&self, multilingual: bool) -> Result<()> {
        self.validate()?;
        if !multilingual && self.language.as_ref().is_some_and(|l| l.as_str() != "en") {
            return Err(WhisperError::UnsupportedLanguage(format!(
                "{} requires a multilingual model",
                self.language.as_ref().unwrap().as_str()
            )));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct WhisperConfig {
    /// An existing whisper.cpp GGML model; never interpreted as a URL or hub ID.
    pub model_path: PathBuf,
    pub inference_threads: usize,
    /// Includes queued, running, and completed-but-uncollected requests.
    pub queue_capacity: usize,
    pub max_segment_duration: Duration,
}
impl WhisperConfig {
    pub fn new(model_path: impl Into<PathBuf>) -> Self {
        Self {
            model_path: model_path.into(),
            inference_threads: 4,
            queue_capacity: 4,
            max_segment_duration: Duration::from_secs(60),
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self.model_path.as_os_str().is_empty() || !self.model_path.is_file() {
            return Err(WhisperError::Invalid(
                "model_path must identify an existing local file",
            ));
        }
        if !(1..=256).contains(&self.inference_threads)
            || !(1..=1024).contains(&self.queue_capacity)
        {
            return Err(WhisperError::Invalid(
                "threads must be 1..=256 and capacity 1..=1024",
            ));
        }
        if self.max_samples() == 0 || self.max_segment_duration > Duration::from_secs(600) {
            return Err(WhisperError::Invalid(
                "segment duration must be positive and at most 600 seconds",
            ));
        }
        Ok(())
    }
    pub(crate) fn max_samples(&self) -> usize {
        (self.max_segment_duration.as_secs_f64() * f64::from(SAMPLE_RATE)).floor() as usize
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn regional_language_tags_normalize_and_reject_unsupported_values() {
        for (input, output) in [
            ("en-US", "en"),
            ("zh-CN", "zh"),
            ("pt-BR", "pt"),
            ("EN", "en"),
        ] {
            assert_eq!(input.parse::<Language>().unwrap().as_str(), output);
        }
        for language in [
            "", "auto", "english", "xx", "en\0", "-en", "en-", "en_uk", "en--US",
        ] {
            assert!(language.parse::<Language>().is_err(), "{language:?}");
        }
        assert!(serde_json::from_str::<Language>("\"unknown\"").is_err());
    }
    #[test]
    fn settings_reject_nonfinite_thresholds_prompts_and_unbounded_decoding() {
        for probability in [f32::NAN, f32::INFINITY, -0.1, 1.01] {
            assert!(WhisperSettings {
                no_speech_probability: Some(probability),
                ..Default::default()
            }
            .validate()
            .is_err());
            assert!(WhisperSettings {
                temperature: probability,
                ..Default::default()
            }
            .validate()
            .is_err());
        }
        for prompt in ["bad\0prompt".into(), "x".repeat(16_385)] {
            assert!(WhisperSettings {
                initial_prompt: Some(prompt),
                ..Default::default()
            }
            .validate()
            .is_err());
        }
        assert!(WhisperSettings {
            strategy: DecodingStrategy::Greedy { best_of: 0 },
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(WhisperSettings {
            strategy: DecodingStrategy::BeamSearch { beam_size: 17 },
            ..Default::default()
        }
        .validate()
        .is_err());
        assert!(WhisperSettings {
            language: Some("es".parse().unwrap()),
            ..Default::default()
        }
        .validate_model(false)
        .is_err());
        assert!(WhisperSettings {
            language: None,
            ..Default::default()
        }
        .validate_model(false)
        .is_ok());
        assert!(WhisperSettings {
            language: Some("es".parse().unwrap()),
            ..Default::default()
        }
        .validate_model(true)
        .is_ok());
    }
    #[test]
    fn decoding_width_matches_the_linked_native_decoder_limit() {
        // whisper-rs-sys 0.15.0 bundles whisper.cpp 1.8.3, whose
        // WHISPER_MAX_DECODERS is 8 for both sampling strategies.
        for width in 0..=u8::MAX {
            for strategy in [
                DecodingStrategy::Greedy { best_of: width },
                DecodingStrategy::BeamSearch { beam_size: width },
            ] {
                let settings = WhisperSettings {
                    strategy,
                    ..Default::default()
                };
                assert_eq!(settings.validate().is_ok(), (1..=8).contains(&width));
            }
        }
    }
    #[test]
    fn nonexistent_model_is_not_downloaded() {
        assert!(WhisperConfig::new("/definitely/missing/ggml-tiny.en.bin")
            .validate()
            .is_err());
        assert!(WhisperConfig::new("https://huggingface.co/model")
            .validate()
            .is_err());
    }
}
