use crate::{Cancellation, Result, Transcript, WhisperError};
use std::sync::Arc;

/// An already authorized media owner retains one exact captured segment.
/// Called synchronously on the service's owned native worker after successful
/// inference, before its admission permit is released. Implementations must
/// enforce private storage and retention quotas. This grants no device access,
/// authenticates no speaker, and writes no conversation or neuron event.
pub trait AudioSourceRetainer: Send + Sync {
    fn retain_source(&self, pcm16: &[u8], sequence: u64) -> Result<serde_json::Value>;
}

pub(crate) struct SourceCapture {
    pub retainer: Arc<dyn AudioSourceRetainer>,
    pub pcm16: Vec<u8>,
    pub sequence: u64,
}

pub(crate) fn attach_source(
    mut transcript: Transcript,
    source: Option<SourceCapture>,
    cancel: &Cancellation,
    shutdown: &Cancellation,
) -> Result<Transcript> {
    if cancel.is_cancelled() || shutdown.is_cancelled() {
        return Err(WhisperError::Cancelled);
    }
    if let Some(source) = source.filter(|_| !transcript.text.trim().is_empty()) {
        let metadata = source
            .retainer
            .retain_source(&source.pcm16, source.sequence)?;
        if serde_json::to_vec(&metadata)
            .map_err(|e| WhisperError::Worker(e.to_string()))?
            .len()
            > 4096
        {
            return Err(WhisperError::Invalid(
                "audio source metadata exceeds 4096 bytes",
            ));
        }
        if cancel.is_cancelled() || shutdown.is_cancelled() {
            return Err(WhisperError::Cancelled);
        }
        transcript.audio_source = Some(metadata);
    }
    Ok(transcript)
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
