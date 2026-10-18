#![doc = include_str!("../README.md")]
//! Native Gemini Live WebSocket sessions and Pipecat frame composition.
//!
//! This is an explicitly bounded protocol subset, not a Python service alias.
//! See the crate README for supported semantics and deliberate rejections.
mod client;
mod processor;
mod protocol;
mod types;
mod video;

pub use client::*;
pub use processor::*;
pub use types::*;

#[cfg(feature = "paired-history")]
mod history;
#[cfg(feature = "paired-history")]
pub use history::{GeminiHistoryOptions, GeminiHistoryRoute};
