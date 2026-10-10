//! Local CPU Piper synthesis. No downloads, Python, HTTP server, or default GPL backend.
//! `espeak-gpl` explicitly enables a caller-supplied GPL-3.0-or-later phonemizer.
#![deny(unsafe_code)]
mod config;
#[cfg(feature = "onnx")]
mod model;
#[cfg(feature = "onnx")]
#[allow(unsafe_code)]
mod native;
mod processor;
mod service;
pub use processor::{PiperProcessorOptions, PiperTtsProcessor};
#[cfg(test)]
mod processor_tests;
pub use config::*;
pub use service::*;

pub type Result<T> = std::result::Result<T, PiperError>;
#[derive(Debug, thiserror::Error)]
pub enum PiperError {
    #[error("invalid Piper configuration or request: {0}")]
    Invalid(&'static str),
    #[error("unsupported Piper option: {0}")]
    Unsupported(&'static str),
    #[error("Piper artifact IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("Piper artifact SHA-256 mismatch")]
    HashMismatch,
    #[error("Piper artifact or response exceeds configured bound")]
    TooLarge,
    #[error("invalid Piper model/config ABI: {0}")]
    Model(String),
    #[error("Piper phoneme is not represented by this model: {0:?}")]
    UnknownPhoneme(char),
    #[error("native Piper backend failed: {0}")]
    Native(String),
    #[error("Piper queue is full")]
    Backpressure,
    #[error("Piper request cancelled")]
    Cancelled,
    #[error("Piper service is closed")]
    Closed,
    #[error("Piper worker panicked")]
    Worker,
    #[error("invalid PCM: {0}")]
    Audio(#[from] pipecat_audio::AudioError),
}
