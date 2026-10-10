//! Native, bounded RTVI scripted behavioral evaluations. No Python runtime or fallback.
#![forbid(unsafe_code)]
mod bounded_yaml;
pub mod events;
pub mod matcher;
pub mod process;
pub mod release;
pub mod scenario;
pub mod serializer;
pub mod session;
pub mod suite;
pub use scenario::{
    Expectation, FunctionCall, ScenarioFile, ScriptScenario, ScriptTurn, SendAfter,
};
pub use session::{Cancellation, EvalSession, SessionOptions};
#[derive(Debug, thiserror::Error)]
pub enum EvalError {
    #[error("invalid scenario: {0}")]
    Scenario(String),
    #[error("unsupported native evaluation feature: {0}")]
    Unsupported(String),
    #[error("evaluation limit exceeded: {0}")]
    Limit(&'static str),
    #[error("JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("file I/O: {0}")]
    Io(#[from] std::io::Error),
    #[error("protocol: {0}")]
    Protocol(#[from] pipecat_protocol::ProtocolError),
    #[error("transport: {0}")]
    Transport(#[from] pipecat_transports::TransportError),
    #[error("{0} timed out")]
    Timeout(&'static str),
    #[error("evaluation cancelled")]
    Cancelled,
    #[error("bot disconnected before evaluation completed")]
    Disconnected,
}
