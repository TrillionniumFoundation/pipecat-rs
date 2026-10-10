//! Shared, backend-independent media transport processors.
//!
//! Device operations are explicit traits. A backend must acknowledge playback,
//! not merely acceptance into a network or device queue.
mod fence;
mod input;
mod input_video;
pub mod mixer;
mod output;
mod queue;
pub mod recording;
mod spoken;
#[cfg(feature = "assistant-history")]
pub use spoken::AssistantSpokenHistorySink;
pub mod video;

use async_trait::async_trait;
pub use input::{InputBackend, InputIngress, InputTransport, InputTransportConfig};
pub use output::{
    OutputAdmissionPolicy, OutputBackend, OutputControl, OutputTransport, OutputTransportConfig,
    PlaybackReceipt,
};
use pipecat_audio::PcmFormat;
use pipecat_core::{FrameData, ProcessorError};
pub use spoken::{
    SpokenContext, SpokenHistory, SpokenHistoryConfig, SpokenHistoryEffect, SpokenHistoryReadiness,
    SpokenHistorySink, SpokenHistoryStatus, SpokenSourceBoundary,
};

/// Stateful PCM transformation. Calls within a destination are serialized.
#[async_trait]
pub trait AudioFilter: Send + Sync {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError>;
    async fn filter(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError>;
    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError>;
    async fn stop(&self) -> Result<(), ProcessorError>;
}

/// Background audio source mixed after filtering and gain.
#[async_trait]
pub trait AudioMixer: Send + Sync {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError>;
    async fn mix(&self, audio: Vec<u8>) -> Result<Vec<u8>, ProcessorError>;
    async fn control(&self, frame: &FrameData) -> Result<(), ProcessorError>;
    async fn stop(&self) -> Result<(), ProcessorError>;
}

pub(crate) fn error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(error.to_string())
}

/// A pair of pipeline endpoints backed by explicit capture/playback adapters.
/// Constructing this value performs no device I/O; the pipeline owns lifecycle.
pub struct MediaTransport {
    input: std::sync::Arc<InputTransport>,
    output: std::sync::Arc<OutputTransport>,
}
impl MediaTransport {
    pub fn new(input: InputTransport, output: OutputTransport) -> Self {
        Self {
            input: std::sync::Arc::new(input),
            output: std::sync::Arc::new(output),
        }
    }
    pub fn input(&self) -> std::sync::Arc<InputTransport> {
        self.input.clone()
    }
    pub fn output(&self) -> std::sync::Arc<OutputTransport> {
        self.output.clone()
    }
}
