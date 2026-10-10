//! Native OpenAI GA Realtime WebSocket sessions and frame processing.
//!
//! The protocol follows the repository's OpenAI service: nested audio settings,
//! `response.output_audio.*` events, and bearer authentication without a beta header.
mod client;
mod processor;
mod types;

pub use client::*;
pub use processor::*;
pub use types::*;
