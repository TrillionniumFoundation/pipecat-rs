//! Explicit chunk-v2 client reports driving the real shared media output.
//!
//! Played means a matching client completion report, never network enqueue or
//! physical audibility. The default CLI remains the legacy context protocol.
mod backend;
mod driver;
mod provider;
mod state;

use crate::AgentError;
pub use backend::{ReceiptBackend, ReceiptOutput};
pub use driver::{ReceiptRunConfig, ReceiptRunReport, ReceiptSession};
pub(crate) use provider::run_provider_session;
use std::time::Duration;

pub const MAX_ID: u64 = (1u64 << 53) - 1;
pub const MAX_PACKET_BYTES: usize = 64 * 1024;
pub const MAX_PCM_BYTES: usize = 48 * 1024;
pub const INPUT_CAPACITY: usize = 8;
pub const INPUT_BYTE_CAPACITY: usize = 256 * 1024;
pub const GATE_TOKEN: &str = "pipecat.cli.receipt-gate";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum PlaybackProtocol {
    #[default]
    LegacyV1,
    ChunkV2,
}

/// Each deadline is absolute from operation admission; notifications and invalid
/// receipts never restart it. Small nonzero values are supported for testing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReceiptConfig {
    pub handshake_timeout: Duration,
    pub chunk_timeout: Duration,
    pub enqueue_timeout: Duration,
    pub cutoff_timeout: Duration,
    pub drain_timeout: Duration,
}
impl Default for ReceiptConfig {
    fn default() -> Self {
        Self {
            handshake_timeout: Duration::from_secs(5),
            chunk_timeout: Duration::from_secs(5),
            enqueue_timeout: Duration::from_secs(2),
            cutoff_timeout: Duration::from_secs(5),
            drain_timeout: Duration::from_secs(15),
        }
    }
}
impl ReceiptConfig {
    pub fn validate(&self) -> Result<(), AgentError> {
        if [
            self.handshake_timeout,
            self.chunk_timeout,
            self.enqueue_timeout,
            self.cutoff_timeout,
            self.drain_timeout,
        ]
        .into_iter()
        .any(|d| d.is_zero() || d > Duration::from_secs(60))
        {
            return Err(AgentError::Config(
                "v2 deadlines must be nonzero and at most 60 seconds",
            ));
        }
        Ok(())
    }
}

fn nonce() -> Result<String, AgentError> {
    let mut bytes = [0u8; 16];
    getrandom::fill(&mut bytes)
        .map_err(|_| AgentError::Config("OS entropy is unavailable for v2 session identity"))?;
    use std::fmt::Write;
    let mut out = String::with_capacity(32);
    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }
    Ok(out)
}

#[cfg(test)]
mod tests;
