//! Native xAI/Grok Realtime WebSocket sessions and Pipecat frame processing.
//!
//! Tool outputs are delivered immediately. Inference is requested separately by
//! the upstream aggregator; final results never implicitly create a response.
mod client;
mod processor;
mod types;
pub use client::*;
pub use processor::*;
pub use types::*;
