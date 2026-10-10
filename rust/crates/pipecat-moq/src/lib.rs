//! Native MoQ Lite 06 over HTTPS WebTransport, with Hang Opus and ordered JSON.
//!
//! Publication acknowledges local acceptance, never remote audio playback.
//! This client does not implement browser, serving, video, or reconnect modes.
mod config;
mod connection;
mod media;
mod pipeline;
mod transcript;
mod wire;

pub use config::{MoqConfig, MoqLimits, MoqMode, TlsTrust};
pub use connection::{MoqConnection, MoqEvent, MoqMonitor, MoqSender};
pub use pipecat_core::AudioRawFrame;
pub use pipeline::{
    MoqInputTransport, MoqOutputTransport, MoqTransport, MoqTransportConfig, MoqTransportState,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid MoQ configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("unsupported MoQ mode: {0}")]
    Unsupported(&'static str),
    #[error("MoQ protocol: {0}")]
    Protocol(String),
    #[error("MoQ TLS/QUIC: {0}")]
    Network(String),
    #[error("MoQ media: {0}")]
    Media(String),
    #[error("invalid audio: {0}")]
    InvalidAudio(&'static str),
    #[error("MoQ limit exceeded: {0}")]
    Limit(&'static str),
    #[error("audio publication interrupted before local write")]
    Interrupted,
    #[error("media group expired")]
    Stale,
    #[error("MoQ session closed")]
    Closed,
    #[error("MoQ {0} timed out")]
    Timeout(&'static str),
    #[error("MoQ outgoing queue is full")]
    Backpressure,
    #[error("MoQ background task failed: {0}")]
    Task(String),
}
impl Error {
    fn protocol(error: impl std::fmt::Display) -> Self {
        Self::Protocol(error.to_string())
    }
    fn network(error: impl std::fmt::Display) -> Self {
        Self::Network(error.to_string())
    }
    fn media(error: impl std::fmt::Display) -> Self {
        Self::Media(error.to_string())
    }
}
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests;
