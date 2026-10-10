//! Native conversational workers with executable model/tool rounds and owned history.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
mod engine;
mod tools;
pub use engine::*;
pub use tools::*;

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum AgentError {
    #[error("invalid agent configuration: {0}")]
    Config(String),
    #[error("agent is closed")]
    Closed,
    #[error("operation was cancelled")]
    Cancelled,
    #[error("agent capacity exceeded: {0}")]
    Capacity(String),
    #[error("agent operation failed: {0}")]
    Failed(String),
    #[error("unsupported operation: {0}")]
    Unsupported(String),
}
impl From<pipecat_core::PipelineError> for AgentError {
    fn from(value: pipecat_core::PipelineError) -> Self {
        Self::Failed(value.to_string())
    }
}
impl From<pipecat_services::ServiceError> for AgentError {
    fn from(value: pipecat_services::ServiceError) -> Self {
        Self::Failed(value.to_string())
    }
}
impl From<pipecat_workers::WorkerError> for AgentError {
    fn from(value: pipecat_workers::WorkerError) -> Self {
        Self::Failed(value.to_string())
    }
}
impl From<pipecat_workers::BusError> for AgentError {
    fn from(value: pipecat_workers::BusError) -> Self {
        Self::Failed(value.to_string())
    }
}
mod backend;
pub use backend::*;
mod ui;
pub use ui::*;
