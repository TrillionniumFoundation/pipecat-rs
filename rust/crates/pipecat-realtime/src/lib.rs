//! Native OpenAI and explicit Azure OpenAI GA Realtime sessions and processing.
//!
//! The protocol follows the repository's OpenAI service: nested audio settings,
//! `response.output_audio.*` events, without a beta header. Azure's GA v1 profile
//! selects either an API key or caller-supplied Entra bearer credential while
//! reusing the same socket actor and frame processor.
mod client;
mod context;
mod processor;
mod progress;
mod types;
mod video;

pub use client::*;
pub use processor::*;
pub use progress::*;
pub use types::*;

mod usage_details;
