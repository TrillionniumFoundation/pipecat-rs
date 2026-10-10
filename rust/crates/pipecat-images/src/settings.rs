use crate::{HostedImagePolicy, ImageError, Result};
use pipecat_core::Setting;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// Compatibility identifier from pinned source af9f9da, not a hosted availability claim.
pub const SOURCE_DEFAULT_MODEL: &str = "gpt-image-2.5-flare";
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ImageGenerationSettings {
    pub model: Option<String>,
    pub image_size: Option<String>,
}
impl Default for ImageGenerationSettings {
    fn default() -> Self {
        Self {
            model: Some(SOURCE_DEFAULT_MODEL.into()),
            image_size: None,
        }
    }
}
#[derive(Clone, Debug, Default)]
pub struct ImageGenerationSettingsDelta {
    pub model: Setting<String>,
    pub image_size: Setting<String>,
}
impl ImageGenerationSettingsDelta {
    pub(crate) fn validate(&self) -> Result<()> {
        validate_setting(&self.model, 1024)?;
        validate_setting(&self.image_size, 128)
    }
}
pub(crate) fn validate_setting(value: &Setting<String>, limit: usize) -> Result<()> {
    if matches!(value, Setting::Value(s) if s.len() > limit) {
        return Err(ImageError::Invalid("model or image_size is too long"));
    }
    Ok(())
}
impl ImageGenerationSettings {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.model.as_ref().is_some_and(|s| s.len() > 1024)
            || self.image_size.as_ref().is_some_and(|s| s.len() > 128)
        {
            return Err(ImageError::Invalid("model or image_size is too long"));
        }
        Ok(())
    }
    pub fn apply(&self, delta: &ImageGenerationSettingsDelta) -> Result<Self> {
        self.validate()?;
        delta.validate()?;
        fn update(target: &mut Option<String>, value: &Setting<String>) {
            match value {
                Setting::NotGiven => (),
                Setting::Null => *target = None,
                Setting::Value(s) => *target = Some(s.clone()),
            }
        }
        let mut next = self.clone();
        update(&mut next.model, &delta.model);
        update(&mut next.image_size, &delta.image_size);
        next.validate()?;
        Ok(next)
    }
}
#[derive(Clone, Debug)]
pub struct ImageGenerationLimits {
    pub max_prompt_bytes: usize,
    pub max_response_bytes: usize,
    pub max_encoded_image_bytes: usize,
    pub max_pixels: usize,
    pub max_dimension: u32,
    /// Conservative aggregate scratch allowance, including decoder coefficients.
    pub max_decode_bytes: usize,
    /// Total accepted processor jobs, including the active job.
    pub max_pending: usize,
    pub request_timeout: Duration,
}
impl Default for ImageGenerationLimits {
    fn default() -> Self {
        Self {
            max_prompt_bytes: 64 * 1024,
            max_response_bytes: 24 * 1024 * 1024,
            max_encoded_image_bytes: 16 * 1024 * 1024,
            max_pixels: 4096 * 4096,
            max_dimension: 4096,
            max_decode_bytes: 256 * 1024 * 1024,
            max_pending: 4,
            request_timeout: Duration::from_secs(180),
        }
    }
}
impl ImageGenerationLimits {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_prompt_bytes == 0
            || self.max_prompt_bytes > 1024 * 1024
            || self.max_response_bytes == 0
            || self.max_response_bytes > 64 * 1024 * 1024
            || self.max_encoded_image_bytes == 0
            || self.max_encoded_image_bytes > 32 * 1024 * 1024
            || self.max_pixels == 0
            || self.max_pixels > 64 * 1024 * 1024
            || self.max_dimension == 0
            || self.max_dimension > 16384
            || self.max_decode_bytes == 0
            || self.max_decode_bytes > 512 * 1024 * 1024
            || self.max_pending == 0
            || self.max_pending > 64
            || self.request_timeout.is_zero()
            || self.request_timeout > Duration::from_secs(600)
        {
            return Err(ImageError::Invalid("invalid resource limit"));
        }
        Ok(())
    }
    pub(crate) fn pixels(
        &self,
        width: u32,
        height: u32,
        scratch_per_pixel: usize,
    ) -> Result<usize> {
        let pixels = (width as usize)
            .checked_mul(height as usize)
            .ok_or(ImageError::TooLarge)?;
        if width == 0
            || height == 0
            || width > self.max_dimension
            || height > self.max_dimension
            || pixels > self.max_pixels
            || pixels
                .checked_mul(scratch_per_pixel)
                .and_then(|v| v.checked_add(65536))
                .is_none_or(|v| v > self.max_decode_bytes)
        {
            return Err(ImageError::TooLarge);
        }
        Ok(pixels)
    }
}
#[derive(Clone)]
pub struct ImageGenerationConfig {
    pub(crate) api_key: String,
    pub base_url: String,
    pub settings: ImageGenerationSettings,
    pub limits: ImageGenerationLimits,
    pub hosted_images: HostedImagePolicy,
    /// Allows only a literal loopback HTTP API endpoint for local test fixtures.
    pub allow_loopback_api: bool,
}
impl ImageGenerationConfig {
    pub fn new(api_key: impl Into<String>) -> Self {
        Self {
            api_key: api_key.into(),
            base_url: "https://api.openai.com/v1".into(),
            settings: Default::default(),
            limits: Default::default(),
            hosted_images: Default::default(),
            allow_loopback_api: false,
        }
    }
}
impl std::fmt::Debug for ImageGenerationConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ImageGenerationConfig")
            .field("credentials_and_endpoint", &"[redacted]")
            .field("limits", &self.limits)
            .finish()
    }
}
