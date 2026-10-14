//! Typed frames and a bounded, bidirectional asynchronous processing runtime.
//!
//! See the crate README for concurrency, backpressure, and compatibility details.
mod frame_contract;
mod frame_payloads;
mod health;
mod heartbeat;
pub use heartbeat::{HeartbeatConfig, HeartbeatMonitor, HeartbeatSnapshot};
mod llm_continuation;
mod realtime_context;
mod speculation;
mod tool_outcome;
pub use frame_contract::{FrameContract, FRAME_CONTRACTS, RTVI_FRAME_CONTRACTS};
pub mod frames;
pub mod observer;
mod observer_capture;
pub mod pipeline;
pub mod processor;
pub mod queue;

pub use async_trait::async_trait;
pub use frames::*;
pub use health::*;
pub use llm_continuation::*;
pub use observer::*;
pub use observer_capture::*;
pub use pipeline::*;
pub use processor::*;
pub use queue::{FrameQueue, QueueError};
pub use realtime_context::*;
pub use speculation::*;
pub use tool_outcome::*;
