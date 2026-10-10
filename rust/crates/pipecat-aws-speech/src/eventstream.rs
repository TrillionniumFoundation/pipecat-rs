//! Speech API compatibility wrappers over the shared bounded EventStream codec.
use crate::{auth::error, StreamingError};
pub use pipecat_aws_common::eventstream::{Event, HeaderValue};
pub fn decode(message: &[u8], max_bytes: usize) -> Result<Event<'_>, StreamingError> {
    pipecat_aws_common::eventstream::decode(message, max_bytes).map_err(error)
}
pub fn encode(
    headers: &[(&str, &str)],
    payload: &[u8],
    max_bytes: usize,
) -> Result<Vec<u8>, StreamingError> {
    pipecat_aws_common::eventstream::encode(headers, payload, max_bytes).map_err(error)
}
pub fn audio(payload: &[u8], max_bytes: usize) -> Result<Vec<u8>, StreamingError> {
    pipecat_aws_common::eventstream::audio(payload, max_bytes).map_err(error)
}
