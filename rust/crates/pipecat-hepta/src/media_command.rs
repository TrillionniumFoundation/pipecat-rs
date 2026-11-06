//! The media process's wire boundary. The authenticated Hepta owner supplies the
//! connection incarnation; this module neither creates authority nor owns turns.
use crate::{CapturedAudioRef, HeptaOutputScope, HmiError, OutputIdentity, StreamIdentity};
use serde::Deserialize;
use std::collections::BTreeSet;
use std::num::NonZeroU64;

pub const MAX_MEDIA_COMMAND_BYTES: usize = 65_536;
pub const MAX_MEDIA_CONNECTION_REQUESTS: usize = 65_536;

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum MediaCommand {
    Speak {
        request_id: u64,
        scope: HeptaOutputScope,
        text: String,
    },
    Interrupt {},
    Turn {
        request_id: u64,
        scope: HeptaOutputScope,
    },
    Retire {
        request_id: u64,
        output_epoch: u64,
    },
    VerifySource {
        request_id: u64,
        source: CapturedAudioRef,
    },
    ReleaseSource {
        request_id: u64,
        source: CapturedAudioRef,
    },
    RevokeCapture {
        request_id: u64,
    },
    Shutdown {},
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MediaDecodeError {
    #[error("JSONL command exceeds 64 KiB")]
    LineTooLong,
    #[error("invalid JSONL command")]
    InvalidCommand,
    #[error("incomplete JSONL command")]
    IncompleteCommand,
    #[error("media command decoder is terminal")]
    Terminal,
}

/// Bounded state retained by the caller across cancelled reads/select branches.
/// It consumes at most one line per call, including its newline in the limit.
/// Parsing/EOF failure is terminal; bytes cannot be silently resynchronized into
/// a different command. This decoder performs no media or owner operations.
#[derive(Default)]
pub struct MediaCommandDecoder {
    pending: Vec<u8>,
    terminal: bool,
}

impl MediaCommandDecoder {
    pub fn buffered_bytes(&self) -> usize {
        self.pending.len()
    }

    pub fn push(
        &mut self,
        available: &[u8],
    ) -> Result<(usize, Option<MediaCommand>), MediaDecodeError> {
        if self.terminal {
            return Err(MediaDecodeError::Terminal);
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if count > MAX_MEDIA_COMMAND_BYTES - self.pending.len() {
            self.terminal = true;
            return Err(MediaDecodeError::LineTooLong);
        }
        self.pending.extend_from_slice(&available[..count]);
        if self.pending.last() != Some(&b'\n') {
            return Ok((count, None));
        }
        let command = match serde_json::from_slice(&self.pending) {
            Ok(command) => command,
            Err(_) => {
                self.terminal = true;
                return Err(MediaDecodeError::InvalidCommand);
            }
        };
        self.pending.clear();
        Ok((count, Some(command)))
    }

    pub fn finish(&mut self) -> Result<(), MediaDecodeError> {
        if self.terminal {
            return Err(MediaDecodeError::Terminal);
        }
        self.terminal = true;
        if self.pending.is_empty() {
            Ok(())
        } else {
            Err(MediaDecodeError::IncompleteCommand)
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum MediaCommandError {
    #[error("media connection requires an existing nonzero owner incarnation")]
    InvalidConnection,
    #[error("correlated media command requires a nonzero owner request ID")]
    InvalidRequest,
    #[error("media command belongs to a different owner connection or stream")]
    ForeignScope,
    #[error("invalid media output scope: {0}")]
    InvalidScope(HmiError),
    #[error("media request ID was already admitted; do not replay unknown effects")]
    Replay,
    #[error("media connection request budget exhausted; owner must retire this connection")]
    Capacity,
    #[error("invalid bounded media connection request budget")]
    InvalidCapacity,
}

/// Immutable binding for one owner-created media process and its private pipe.
/// Reconnection requires a new binding/process, not learning the first incoming
/// scope. A matching label is not authentication or a permission grant.
pub struct MediaConnectionBinding {
    stream: StreamIdentity,
    connection_id: NonZeroU64,
}

impl MediaConnectionBinding {
    pub fn new(stream: StreamIdentity, connection_id: u64) -> Result<Self, MediaCommandError> {
        let connection_id =
            NonZeroU64::new(connection_id).ok_or(MediaCommandError::InvalidConnection)?;
        if stream.device_generation == 0
            || stream.organ_generation == 0
            || [
                &stream.principal_id,
                &stream.device_id,
                &stream.organ_id,
                &stream.logical_session_id,
            ]
            .into_iter()
            .any(|value| !crate::session::valid_id(value))
        {
            return Err(MediaCommandError::InvalidConnection);
        }
        Ok(Self {
            stream,
            connection_id,
        })
    }

    /// Check before queueing, changing an epoch, touching a source or opening a
    /// turn. Existing HmiSession and MediaStore still check turn/epoch/sequence,
    /// byte identity and state transitions at their actual effect boundaries.
    /// Request IDs correlate acknowledgements; they are not sequence numbers.
    /// Concurrent owner calls may reach the pipe out of request-ID order.
    pub fn validate(&self, command: &MediaCommand) -> Result<(), MediaCommandError> {
        let request_id = match command {
            MediaCommand::Speak {
                request_id, scope, ..
            }
            | MediaCommand::Turn { request_id, scope } => {
                if scope.connection_id != self.connection_id.get() {
                    return Err(MediaCommandError::ForeignScope);
                }
                let identity = OutputIdentity::try_from(scope.clone())
                    .map_err(MediaCommandError::InvalidScope)?;
                if identity.sequence == 0 {
                    return Err(MediaCommandError::InvalidScope(HmiError::InvalidIdentity));
                }
                if identity.stream != self.stream {
                    return Err(MediaCommandError::ForeignScope);
                }
                *request_id
            }
            MediaCommand::VerifySource { request_id, source }
            | MediaCommand::ReleaseSource { request_id, source } => {
                if source.stream != self.stream {
                    return Err(MediaCommandError::ForeignScope);
                }
                *request_id
            }
            MediaCommand::Retire { request_id, .. }
            | MediaCommand::RevokeCapture { request_id } => *request_id,
            // Uncorrelated stop commands may only arrive through this binding's
            // trusted pipe. They cannot admit output or observations.
            MediaCommand::Interrupt {} | MediaCommand::Shutdown {} => return Ok(()),
        };
        if request_id == 0 {
            return Err(MediaCommandError::InvalidRequest);
        }
        Ok(())
    }
}

/// Process-local, no-eviction request admission. This is not a conversation
/// database, a persistent effect receipt, or authority to retry after reconnect.
/// Failed/unknown operations retain their ID. A full gate fails closed; only the
/// existing owner may establish a fresh pipe/connection and media process.
pub struct MediaCommandGate {
    binding: MediaConnectionBinding,
    admitted: BTreeSet<u64>,
    limit: usize,
}

impl MediaCommandGate {
    pub fn new(binding: MediaConnectionBinding, limit: usize) -> Result<Self, MediaCommandError> {
        if !(1..=MAX_MEDIA_CONNECTION_REQUESTS).contains(&limit) {
            return Err(MediaCommandError::InvalidCapacity);
        }
        Ok(Self {
            binding,
            admitted: BTreeSet::new(),
            limit,
        })
    }

    /// Call once before effects, not again when returning an acknowledgement.
    /// Any error must retire/terminate the endpoint; the caller must not catch
    /// it and continue admitting commands on the same stream.
    pub fn admit(&mut self, command: &MediaCommand) -> Result<(), MediaCommandError> {
        self.binding.validate(command)?;
        let request_id = match command {
            MediaCommand::Speak { request_id, .. }
            | MediaCommand::Turn { request_id, .. }
            | MediaCommand::Retire { request_id, .. }
            | MediaCommand::VerifySource { request_id, .. }
            | MediaCommand::ReleaseSource { request_id, .. }
            | MediaCommand::RevokeCapture { request_id } => *request_id,
            MediaCommand::Interrupt {} | MediaCommand::Shutdown {} => return Ok(()),
        };
        if self.admitted.contains(&request_id) {
            return Err(MediaCommandError::Replay);
        }
        if self.admitted.len() >= self.limit {
            return Err(MediaCommandError::Capacity);
        }
        self.admitted.insert(request_id);
        Ok(())
    }
}
