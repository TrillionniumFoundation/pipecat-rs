//! Local CPU Whisper transcription through the native whisper.cpp engine.
//! Models are explicit local files. No Python, subprocess inference, network
//! access, or automatic model downloads are used.
mod config;
mod model;
mod processor;
mod service;
pub use config::{DecodingStrategy, Language, WhisperConfig, WhisperSettings, SAMPLE_RATE};
pub use model::{Cancellation, Transcript, TranscriptSegment, WhisperModel};
pub use processor::{SegmentationMode, WhisperProcessorOptions, WhisperSttProcessor};
pub use service::{PendingTranscription, WhisperService};

#[derive(Debug, thiserror::Error)]
pub enum WhisperError {
    #[error("invalid Whisper configuration or audio: {0}")]
    Invalid(&'static str),
    #[error("unsupported Whisper language: {0}")]
    UnsupportedLanguage(String),
    #[error("Whisper inference cancelled")]
    Cancelled,
    #[error("Whisper queue is full")]
    Backpressure,
    #[error("Whisper service is closed")]
    Closed,
    #[error("Whisper worker failed: {0}")]
    Worker(String),
    #[error("native Whisper error: {0}")]
    Native(#[from] whisper_rs::WhisperError),
}
pub type Result<T> = std::result::Result<T, WhisperError>;
