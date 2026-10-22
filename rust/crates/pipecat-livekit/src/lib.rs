//! Native LiveKit room/audio/data transport using the official Rust SDK.
//!
//! Audio publication reports native source admission, not remote playback.
mod config;
mod connection;
mod protocol;
mod publication;
mod publisher;
#[cfg(test)]
mod tests;
mod transport;
mod video;
mod video_input;
pub use video_input::{
    ImageRequestConfig, VideoCaptureMode, VideoInputConfig, VideoInputStats, VideoSource,
};

pub use config::LiveKitConfig;
pub use connection::LiveKitConnection;
pub use protocol::{ConnectionState, LiveKitEvent, ParticipantInfo, SourceAccepted};
pub use transport::{LiveKitInput, LiveKitOutput, LiveKitTransport};
pub use video::{VideoCaptureOutcome, VideoOutputConfig};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("LiveKit SDK: {0}")]
    Sdk(String),
    #[error("invalid PCM: {0}")]
    Audio(String),
    #[error("invalid video: {0}")]
    Video(&'static str),
    #[error("video publication is disabled")]
    VideoDisabled,
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("resource limit exceeded: {0}")]
    Limit(&'static str),
    #[error("operation timed out: {0}")]
    Timeout(&'static str),
    #[error("connection is not ready")]
    NotReady,
    #[error("connection is closed")]
    Closed,
    #[error("operation was interrupted")]
    Interrupted,
    #[error("audio publication is disabled")]
    AudioDisabled,
    #[error("event consumer is too slow: bounded receive queue is full")]
    QueueFull,
}
pub type Result<T> = std::result::Result<T, Error>;
