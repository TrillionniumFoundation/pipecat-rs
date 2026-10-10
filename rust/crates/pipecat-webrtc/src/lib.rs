//! Native SmallWebRTC transport: real ICE/DTLS/SRTP/SCTP and bounded PCM/JSON I/O.
//!
//! [`SmallWebRtcConnection`] negotiates SDP and carries actual compressed audio
//! over RTP. HTTP routing, authentication, device capture, and video are separate.
#![doc = include_str!("../README.md")]

mod codec;
mod connection;
mod signaling;

pub use codec::AudioCodec;
pub use connection::{
    ConnectionState, IncomingAudio, SmallWebRtcConfig, SmallWebRtcConnection, TransportEvent,
    TransportStats,
};
pub use signaling::{
    ConnectionMode, IceCandidate, SmallWebRtcAnswer, SmallWebRtcPatchRequest, SmallWebRtcRequest,
    SmallWebRtcRequestHandler,
};
pub use webrtc::ice_transport::ice_candidate::RTCIceCandidateInit;
pub use webrtc::ice_transport::ice_server::RTCIceServer;
pub use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("WebRTC: {0}")]
    WebRtc(#[from] webrtc::Error),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid configuration: {0}")]
    InvalidConfig(&'static str),
    #[error("invalid PCM audio: {0}")]
    InvalidAudio(String),
    #[error("codec {0:?} is unavailable; rebuild with the opus feature for Opus")]
    CodecUnavailable(AudioCodec),
    #[error("codec: {0}")]
    Codec(String),
    #[error("peer shutdown failed: {0}")]
    Shutdown(String),
    #[error("connection is closed or cancelled")]
    Closed,
    #[error("bounded outgoing queue is full")]
    QueueFull,
    #[error("message size {size} exceeds the {limit} byte limit")]
    MessageTooLarge { size: usize, limit: usize },
    #[error("timed out waiting for {0}")]
    Timeout(&'static str),
    #[error("audio is disabled")]
    AudioDisabled,
    #[error("invalid signaling request: {0}")]
    Signaling(String),
    #[error("unknown peer connection: {0}")]
    UnknownPeer(String),
    #[error("single-connection handler already has an active peer")]
    ConnectionLimit,
}
pub type Result<T> = std::result::Result<T, Error>;
