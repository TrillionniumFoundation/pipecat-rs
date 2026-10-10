//! Pure Rust wire codecs for Pipecat telephony, RTVI, and protobuf frames.
//!
//! Parsing is separate from network I/O and call control. No codec initiates a
//! call, sends a request, or stores provider credentials.
#![forbid(unsafe_code)]

pub mod adapters;
pub mod genesys;
pub mod protobuf;
pub mod rtvi;
pub mod telephony;

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// The twelve keys supported by Pipecat's `KeypadEntry`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DtmfDigit {
    #[serde(rename = "0")]
    Zero,
    #[serde(rename = "1")]
    One,
    #[serde(rename = "2")]
    Two,
    #[serde(rename = "3")]
    Three,
    #[serde(rename = "4")]
    Four,
    #[serde(rename = "5")]
    Five,
    #[serde(rename = "6")]
    Six,
    #[serde(rename = "7")]
    Seven,
    #[serde(rename = "8")]
    Eight,
    #[serde(rename = "9")]
    Nine,
    #[serde(rename = "*")]
    Star,
    #[serde(rename = "#")]
    Pound,
}

impl DtmfDigit {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Zero => "0",
            Self::One => "1",
            Self::Two => "2",
            Self::Three => "3",
            Self::Four => "4",
            Self::Five => "5",
            Self::Six => "6",
            Self::Seven => "7",
            Self::Eight => "8",
            Self::Nine => "9",
            Self::Star => "*",
            Self::Pound => "#",
        }
    }
}

impl std::str::FromStr for DtmfDigit {
    type Err = ProtocolError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "0" => Ok(Self::Zero),
            "1" => Ok(Self::One),
            "2" => Ok(Self::Two),
            "3" => Ok(Self::Three),
            "4" => Ok(Self::Four),
            "5" => Ok(Self::Five),
            "6" => Ok(Self::Six),
            "7" => Ok(Self::Seven),
            "8" => Ok(Self::Eight),
            "9" => Ok(Self::Nine),
            "*" => Ok(Self::Star),
            "#" => Ok(Self::Pound),
            _ => Err(ProtocolError::InvalidField("DTMF digit")),
        }
    }
}

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid base64 audio: {0}")]
    Base64(#[from] base64::DecodeError),
    #[error("missing field: {0}")]
    MissingField(&'static str),
    #[error("invalid field: {0}")]
    InvalidField(&'static str),
    #[error("unsupported encoding: {0}")]
    UnsupportedEncoding(String),
    #[error("unsupported message: {0}")]
    UnsupportedMessage(String),
    #[error("message exceeds {limit} bytes")]
    MessageTooLarge { limit: usize },
    #[error("audio sample rate {actual} does not match wire rate {expected}")]
    SampleRateMismatch { expected: u32, actual: u32 },
    #[error("audio error: {0}")]
    Audio(#[from] pipecat_audio::AudioError),
    #[error("invalid stream state: {0}")]
    State(&'static str),
    #[error("stream identifier does not match the active stream")]
    StreamMismatch,
    #[error("sequence {received} is not greater than {previous}")]
    Sequence { previous: u64, received: u64 },
    #[error("invalid protobuf: {0}")]
    Protobuf(#[from] prost::DecodeError),
}

/// WebSocket text and binary payloads remain distinct (Vonage uses binary PCM).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WireMessage {
    Text(String),
    Binary(Vec<u8>),
}

pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 1024 * 1024;

pub(crate) fn check_size(len: usize, limit: usize) -> Result<(), ProtocolError> {
    if len > limit {
        Err(ProtocolError::MessageTooLarge { limit })
    } else {
        Ok(())
    }
}
