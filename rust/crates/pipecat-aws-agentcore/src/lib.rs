//! Native, finite AgentCore default-schema context-to-text applications.
//! No SDK, credential discovery, tools, retries, hidden connection pool or DNS.
mod app;
mod config;
mod processor;
mod protocol;
mod transport;
pub use app::{AgentCoreCompletion, AgentCoreReport, AgentCoreRunningApp, AgentCoreTextApp};
pub use config::{AgentCoreConfig, AgentCoreLimits};
pub use processor::{AgentCoreClient, AgentCoreProcessor, AgentCoreProgress};
pub use protocol::default_payload;

/// Redacted categories only: no prompt, ARN, credentials, URL or peer body.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid AgentCore configuration/input: {0}")]
    Invalid(&'static str),
    #[error("unsupported AgentCore profile: {0}")]
    Unsupported(&'static str),
    #[error("AgentCore bounded budget exceeded")]
    TooLarge,
    #[error("invalid AgentCore protocol: {0}")]
    Protocol(&'static str),
    #[error("AgentCore HTTP status {0}")]
    HttpStatus(u16),
    #[error("AgentCore local observation cancelled; remote execution status unknown")]
    Cancelled,
    #[error("AgentCore deadline elapsed: {0}; remote execution status may be unknown")]
    Timeout(&'static str),
    #[error("AgentCore transport operation failed; remote execution status may be unknown")]
    Io,
    #[error("AgentCore caller output failed")]
    Output,
    #[error("AgentCore owned task failed")]
    Task,
}
