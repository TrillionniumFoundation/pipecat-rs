//! Native configured conversation flows, acknowledged actions, and tool transitions.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
mod actions;
mod adapters;
mod bounded_yaml;
mod config;
mod handlers;
mod loading;
mod manager;
mod pipeline;

pub use actions::*;
pub use adapters::*;
pub use config::*;
pub use handlers::*;
pub use loading::{FlowIncludePolicy, FlowLoadLimits, FlowLoadOptions};
pub use manager::*;
pub use pipeline::*;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FlowError {
    #[error("invalid flow: {0}")]
    Config(String),
    #[error("flow load limit exceeded: {0}")]
    Limit(&'static str),
    #[error("unresolved flow references: {0:?}")]
    References(Vec<String>),
    #[error("flow is not initialized")]
    NotInitialized,
    #[error("flow is closed")]
    Closed,
    #[error("flow operation cancelled")]
    Cancelled,
    #[error("stale flow generation")]
    Stale,
    #[error("flow operation already in progress")]
    Busy,
    #[error("unknown node: {0}")]
    UnknownNode(String),
    #[error("tool failed: {0}")]
    Tool(String),
    #[error("action failed: {0}")]
    Action(String),
    #[error("pipeline failed: {0}")]
    Pipeline(String),
    #[error("unsupported flow operation: {0}")]
    Unsupported(String),
}
impl From<pipecat_core::ProcessorError> for FlowError {
    fn from(error: pipecat_core::ProcessorError) -> Self {
        Self::Pipeline(error.to_string())
    }
}
impl From<pipecat_core::PipelineError> for FlowError {
    fn from(error: pipecat_core::PipelineError) -> Self {
        Self::Pipeline(error.to_string())
    }
}
