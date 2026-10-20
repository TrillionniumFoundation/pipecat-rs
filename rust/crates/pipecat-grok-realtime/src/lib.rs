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

mod app;
mod app_config;
mod app_state;
pub use app::*;
pub use app_config::*;
pub use app_state::{GrokAppDrainGates, GrokAppFailureStage};

#[cfg(test)]
mod app_tests;
