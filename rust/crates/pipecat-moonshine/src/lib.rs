//! Local Moonshine speech recognition through the vendor's in-process C ABI.
//! Models and native libraries are explicit, trusted, SHA-256 verified local
//! artifacts. No Python, subprocess inference, network or automatic downloads.
#![deny(unsafe_op_in_unsafe_fn)]
mod config;
mod model;
mod native;
mod processor;
mod service;
pub use config::{
    Artifact, Language, Model, ModelBundle, ModelFile, MoonshineConfig, MoonshineSettings,
    NativeLibraries, SAMPLE_RATE,
};
pub use model::{Cancellation, MoonshineModel, Transcript, TranscriptLine};
pub use processor::{MoonshineProcessorOptions, MoonshineSttProcessor, SegmentationMode};
pub use service::{MoonshineService, PendingTranscription};

#[derive(Debug, thiserror::Error)]
pub enum MoonshineError {
    #[error("invalid Moonshine configuration or audio: {0}")]
    Invalid(&'static str),
    #[error("unsupported Moonshine language: {0}")]
    UnsupportedLanguage(String),
    #[error("Moonshine model artifacts are unavailable for {0}")]
    MissingModel(String),
    #[error("legacy non-English Moonshine models require an explicit license grant")]
    LicenseRequired,
    #[error("Moonshine artifact SHA-256 mismatch")]
    HashMismatch,
    #[error("Moonshine artifact or result exceeds configured bound")]
    TooLarge,
    #[error("Moonshine inference cancelled")]
    Cancelled,
    #[error("Moonshine queue is full")]
    Backpressure,
    #[error("Moonshine service is closed")]
    Closed,
    #[error("Moonshine worker failed: {0}")]
    Worker(String),
    #[error("native Moonshine error: {0}")]
    Native(String),
    #[error("Moonshine artifact I/O: {0}")]
    Io(#[from] std::io::Error),
}
pub type Result<T> = std::result::Result<T, MoonshineError>;
