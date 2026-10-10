//! Bounded OpenClaw Gateway protocol 4 client and typed pipeline service.
//! Remote agent output is data. This crate never executes remote tools or prose.
mod client;
mod config;
mod processor;
pub use client::{GatewayClient, GatewayRun, RunEvent, RunHandle, RunIdentity, RunResult};
pub use config::{GatewayConfig, GatewayError, GatewayLimits};
pub use pipecat_core::{OpenClawRemoteOutcome as RemoteOutcome, OpenClawRunStatus as RunStatus};
pub use processor::OpenClawGatewayProcessor;
pub const PROTOCOL_VERSION: u32 = 4;
pub type Result<T> = std::result::Result<T, GatewayError>;
