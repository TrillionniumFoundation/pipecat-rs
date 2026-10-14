// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Bounded native OpenAI/Gemini/Azure image generation, RGB decoding, and pipeline delivery.
mod azure;
mod budget;
mod client;
mod decode;
mod gemini;
mod processor;
mod security;
mod settings;

pub use azure::{AzureImageClient, AzureImageConfig};
pub use client::{GeneratedImage, OpenAiImageClient};
pub use gemini::{
    GeminiImageClient, GeminiImageConfig, GeminiImageGenProcessor, GeminiImageGeneration,
    GeminiImageLimits, GeminiImageSettings, GeminiImageSettingsDelta, GEMINI_SOURCE_DEFAULT_MODEL,
};
pub use pipecat_services::CancellationToken;
pub use processor::ImageGenProcessor;
pub use security::HostedImagePolicy;
pub use settings::{
    ImageGenerationConfig, ImageGenerationLimits, ImageGenerationSettings,
    ImageGenerationSettingsDelta, SOURCE_DEFAULT_MODEL,
};

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ImageError {
    #[error("invalid image configuration: {0}")]
    Invalid(&'static str),
    #[error("image request HTTP status {0}")]
    Http(u16),
    #[error("image transport failed")]
    Transport,
    #[error("image operation timed out")]
    Timeout,
    #[error("image operation cancelled")]
    Cancelled,
    #[error("image resource limit exceeded")]
    TooLarge,
    #[error("invalid image generation response")]
    Protocol,
    #[error("image generation returned no image")]
    NoImage,
    #[error("hosted image address is not allowed")]
    AddressDenied,
    #[error("unsupported or malformed image")]
    Decode,
    #[error("bounded image work queue is full")]
    Backpressure,
    #[error("image service is closed")]
    Closed,
}
impl ImageError {
    pub fn category(&self) -> pipecat_core::ErrorCategory {
        use pipecat_core::ErrorCategory;
        match self {
            Self::Http(status) => ErrorCategory::from_http_status(*status),
            Self::Transport | Self::Timeout => ErrorCategory::Connectivity,
            Self::Invalid(_) | Self::TooLarge | Self::AddressDenied => {
                ErrorCategory::InvalidRequest
            }
            _ => ErrorCategory::Application,
        }
    }
    pub fn is_permanent(&self) -> bool {
        matches!(self, Self::Http(400 | 401 | 403 | 404 | 422))
    }
    pub(crate) fn processor(self) -> pipecat_core::ProcessorError {
        pipecat_core::ProcessorError::new(self.to_string())
            .with_category(self.category())
            .with_permanence_override(self.is_permanent())
    }
}
pub type Result<T> = std::result::Result<T, ImageError>;
