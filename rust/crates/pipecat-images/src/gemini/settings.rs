use crate::{ImageError, ImageGenerationLimits, Result};
use pipecat_core::Setting;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Pinned source compatibility identifier, not a hosted availability guarantee.
pub const GEMINI_SOURCE_DEFAULT_MODEL: &str = "gemini-3.1-flash-image";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GeminiImageSettings {
    pub model: String,
    pub number_of_images: u8,
    pub aspect_ratio: Option<String>,
    /// Accepted for source compatibility, bounded locally, never sent to Gemini.
    pub negative_prompt: Option<String>,
}
impl Default for GeminiImageSettings {
    fn default() -> Self {
        Self {
            model: GEMINI_SOURCE_DEFAULT_MODEL.into(),
            number_of_images: 1,
            aspect_ratio: Some("1:1".into()),
            negative_prompt: None,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct GeminiImageSettingsDelta {
    pub model: Setting<String>,
    pub number_of_images: Setting<u8>,
    pub aspect_ratio: Setting<String>,
    pub negative_prompt: Setting<String>,
}
impl GeminiImageSettings {
    pub(crate) fn validate(&self) -> Result<()> {
        // Model is exactly one REST resource segment. No query/path injection.
        if self.model.is_empty()
            || self.model.len() > 256
            || !self
                .model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
            || self.model == "."
            || self.model == ".."
        {
            return Err(ImageError::Invalid("invalid Gemini image model"));
        }
        if !(1..=8).contains(&self.number_of_images) {
            return Err(ImageError::Invalid("image count must be in 1..=8"));
        }
        if self.aspect_ratio.as_ref().is_some_and(|value| {
            !matches!(
                value.as_str(),
                "1:1"
                    | "1:4"
                    | "4:1"
                    | "1:8"
                    | "8:1"
                    | "2:3"
                    | "3:2"
                    | "3:4"
                    | "4:3"
                    | "4:5"
                    | "5:4"
                    | "9:16"
                    | "16:9"
                    | "21:9"
            )
        }) {
            return Err(ImageError::Invalid("unsupported Gemini aspect ratio"));
        }
        if self
            .negative_prompt
            .as_ref()
            .is_some_and(|s| s.len() > 65536)
        {
            return Err(ImageError::Invalid("ignored negative prompt is too long"));
        }
        Ok(())
    }
    pub fn apply(&self, delta: &GeminiImageSettingsDelta) -> Result<Self> {
        self.validate()?;
        crate::settings::validate_setting(&delta.model, 256)?;
        crate::settings::validate_setting(&delta.aspect_ratio, 16)?;
        crate::settings::validate_setting(&delta.negative_prompt, 65536)?;
        let mut next = self.clone();
        match &delta.model {
            Setting::NotGiven => (),
            Setting::Null => return Err(ImageError::Invalid("Gemini image model is required")),
            Setting::Value(value) => next.model = value.clone(),
        }
        match delta.number_of_images {
            Setting::NotGiven => (),
            Setting::Null => return Err(ImageError::Invalid("image count is required")),
            Setting::Value(value) => next.number_of_images = value,
        }
        fn update(target: &mut Option<String>, setting: &Setting<String>) {
            match setting {
                Setting::NotGiven => (),
                Setting::Null => *target = None,
                Setting::Value(value) => *target = Some(value.clone()),
            }
        }
        update(&mut next.aspect_ratio, &delta.aspect_ratio);
        update(&mut next.negative_prompt, &delta.negative_prompt);
        next.validate()?;
        Ok(next)
    }
}

#[derive(Clone, Debug)]
pub struct GeminiImageLimits {
    /// Per-image HTTP/codec limits and concurrent sequence admission bound.
    pub image: ImageGenerationLimits,
    pub max_images: u8,
    pub max_candidates: usize,
    /// Total parts across all candidates in a single response.
    pub max_parts: usize,
    pub max_total_response_bytes: usize,
    pub max_total_encoded_bytes: usize,
    pub max_total_output_bytes: usize,
    /// Whole prompt lifetime, including time spent between next_image calls.
    pub generation_timeout: Duration,
}
impl Default for GeminiImageLimits {
    fn default() -> Self {
        Self {
            image: ImageGenerationLimits::default(),
            max_images: 8,
            max_candidates: 16,
            max_parts: 128,
            max_total_response_bytes: 64 * 1024 * 1024,
            max_total_encoded_bytes: 32 * 1024 * 1024,
            max_total_output_bytes: 128 * 1024 * 1024,
            generation_timeout: Duration::from_secs(600),
        }
    }
}
impl GeminiImageLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        self.image.validate()?;
        if !(1..=8).contains(&self.max_images)
            || !(1..=16).contains(&self.max_candidates)
            || !(1..=128).contains(&self.max_parts)
            || self.generation_timeout.is_zero()
            || self.generation_timeout > Duration::from_secs(3600)
            || [
                self.max_total_response_bytes,
                self.max_total_encoded_bytes,
                self.max_total_output_bytes,
            ]
            .iter()
            .any(|&n| n == 0 || n > 512 * 1024 * 1024)
        {
            return Err(ImageError::Invalid("invalid Gemini image resource limit"));
        }
        Ok(())
    }
}
#[derive(Clone)]
pub struct GeminiImageConfig {
    pub(crate) api_key: String,
    pub base_url: String,
    pub settings: GeminiImageSettings,
    pub limits: GeminiImageLimits,
    /// Allows only a literal loopback HTTP API endpoint for fixtures.
    pub allow_loopback_api: bool,
}
impl GeminiImageConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://generativelanguage.googleapis.com/v1beta".into(),
            settings: Default::default(),
            limits: Default::default(),
            allow_loopback_api: false,
        }
    }
}
impl std::fmt::Debug for GeminiImageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GeminiImageConfig")
            .field("credentials_endpoint_and_settings", &"[redacted]")
            .field("limits", &self.limits)
            .finish()
    }
}
