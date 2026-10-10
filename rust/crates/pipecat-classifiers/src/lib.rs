//! Bounded, reusable typed classification without a pipeline or conversation mutation.
//! See README.md for probability provenance and deliberate validation tightening.
mod jev;
mod limits;
mod llm;
mod types;

pub use jev::{
    JevClassifier, JevClient, JevConfig, JevResponse, DEFAULT_JEV_BASE_URL, DEFAULT_JEV_MODEL,
    JEV_MAX_CHOICE_OPTIONS,
};
pub use limits::ClassifierLimits;
pub use llm::{LlmClassifier, LlmClassifierConfig, DEFAULT_INSTRUCTIONS};
pub use pipecat_services::CancellationToken;
pub use types::*;
