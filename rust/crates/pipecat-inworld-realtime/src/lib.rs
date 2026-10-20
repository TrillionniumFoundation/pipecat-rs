//! Native Inworld Realtime PCM speech sessions and Pipecat frame processing.
//!
//! Local tools are synchronous/final-only. Explicit paired-context inference,
//! not a tool result, owns continuation. No reconnect, truncate or history replay.
mod app;
mod client;
mod processor;
mod types;
pub use app::*;
pub use client::*;
pub use processor::*;
pub use types::*;
