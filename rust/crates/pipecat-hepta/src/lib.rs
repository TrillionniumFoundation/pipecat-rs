//! Hepta-owned HMI identity and local media ports, without an LLM or tool loop.
//! Values bind an already authenticated caller; this crate grants no authority.
#![doc = include_str!("../README.md")]

mod session;
pub use session::*;
mod media_store;
pub use media_store::*;

#[cfg(feature = "local-speech")]
pub mod local_speech;

#[cfg(feature = "process-supervision")]
pub mod process_supervision;

#[cfg(feature = "process-supervision")]
pub mod parent_guard;

mod speech_segments;
pub use speech_segments::speech_segments;

mod media_command;
pub use media_command::{
    MediaCommand, MediaCommandDecoder, MediaCommandError, MediaCommandGate, MediaConnectionBinding,
    MediaDecodeError, MAX_MEDIA_COMMAND_BYTES, MAX_MEDIA_CONNECTION_REQUESTS,
};
