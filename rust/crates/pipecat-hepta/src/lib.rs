//! Hepta-owned HMI identity and local media ports, without an LLM or tool loop.
//! Values bind an already authenticated caller; this crate grants no authority.
#![doc = include_str!("../README.md")]

mod session;
pub use session::*;
mod media_store;
pub use media_store::*;

#[cfg(feature = "local-speech")]
pub mod local_speech;
