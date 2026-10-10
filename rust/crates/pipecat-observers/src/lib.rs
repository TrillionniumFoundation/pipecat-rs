#![doc = include_str!("../README.md")]
//! Privacy-safe pipeline observation and explicitly measured service metrics.

pub mod metrics;
pub use metrics::*;

pub mod latency;
pub mod startup;
pub mod stream;
pub use latency::*;
pub use startup::*;
pub use stream::*;

pub mod turn_tracking;
pub use turn_tracking::*;

pub mod error;
pub mod events;
pub mod function_call;
pub mod speaking;
pub use error::*;
pub use events::*;
pub use function_call::*;
pub use pipecat_core::{
    CaptureOmission, Captured, FunctionCallPhase, FunctionCaptureOptions, SpeechEventKind,
};
pub use speaking::*;
