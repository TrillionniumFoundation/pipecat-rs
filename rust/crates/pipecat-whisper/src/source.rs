use crate::{Cancellation, Result, Transcript, WhisperError};
use std::sync::Arc;

/// An already authorized media owner retains one exact captured segment.
/// Called synchronously on the service's owned native worker after successful
/// inference, before its admission permit is released. Implementations must
/// enforce private storage and retention quotas. This grants no device access,
/// authenticates no speaker, and writes no conversation or neuron event.
pub trait AudioSourceRetainer: Send + Sync {
    fn retain_source(&self, pcm16: &[u8], sequence: u64) -> Result<serde_json::Value>;

    /// Reclaim only a successfully retained source that has not been handed off.
    /// Resource-owning implementations should override this compatibility default.
    /// Cleanup runs synchronously on the dropping owner, must not panic, and must
    /// keep assets retained if exact identity validation or cleanup fails.
    fn release_unpublished_source(&self, _source: &serde_json::Value) -> Result<()> {
        Ok(())
    }
}

struct UnpublishedSource {
    retainer: Arc<dyn AudioSourceRetainer>,
    metadata: Option<serde_json::Value>,
}
impl Drop for UnpublishedSource {
    fn drop(&mut self) {
        if let Some(metadata) = self.metadata.take() {
            // Failure stays fail-closed. Never substitute a weaker identity or
            // delete a published asset merely because its consumer stopped.
            let _ = self.retainer.release_unpublished_source(&metadata);
        }
    }
}

pub(crate) struct RetainedTranscript {
    transcript: Transcript,
    source: Option<UnpublishedSource>,
}
impl std::ops::Deref for RetainedTranscript {
    type Target = Transcript;
    fn deref(&self) -> &Self::Target {
        &self.transcript
    }
}
impl RetainedTranscript {
    pub(crate) fn transcript(&self) -> &Transcript {
        &self.transcript
    }
    /// A successful pipeline push is an uncertain handoff, not an owner receipt.
    /// After this boundary only the existing owner's explicit policy may release.
    pub(crate) fn handoff(mut self) -> Transcript {
        if let Some(mut source) = self.source.take() {
            source.metadata.take();
        }
        self.transcript
    }
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
) -> Result<RetainedTranscript> {
    if cancel.is_cancelled() || shutdown.is_cancelled() {
        return Err(WhisperError::Cancelled);
    }
    let mut retained = None;
    if let Some(source) = source.filter(|_| !transcript.text.trim().is_empty()) {
        let metadata = source
            .retainer
            .retain_source(&source.pcm16, source.sequence)?;
        let lease = UnpublishedSource {
            retainer: source.retainer,
            metadata: Some(metadata.clone()),
        };
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
        retained = Some(lease);
    }
    Ok(RetainedTranscript {
        transcript,
        source: retained,
    })
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
