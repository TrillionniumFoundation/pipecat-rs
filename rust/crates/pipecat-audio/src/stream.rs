//! Byte-boundary-safe streaming PCM buffering.

use std::collections::VecDeque;

use crate::{decode_pcm16_le, AudioError, PcmFormat, Result};

/// Decode any byte-sized input chunks, retaining at most one incomplete frame.
#[derive(Debug, Clone)]
pub struct PcmStreamDecoder {
    format: PcmFormat,
    pending: Vec<u8>,
}

impl PcmStreamDecoder {
    pub fn new(format: PcmFormat) -> Self {
        Self {
            format,
            pending: Vec::new(),
        }
    }
    pub fn format(&self) -> PcmFormat {
        self.format
    }
    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    pub fn push(&mut self, bytes: &[u8]) -> Vec<i16> {
        self.pending.extend_from_slice(bytes);
        let complete = self.pending.len() - self.pending.len() % self.format.frame_bytes();
        // `complete` is a multiple of the validated PCM frame size.
        let samples = decode_pcm16_le(&self.pending[..complete]).expect("complete PCM frames");
        self.pending.drain(..complete);
        samples
    }

    /// Reject rather than drop a final incomplete frame. More bytes may follow an error.
    pub fn finish(&self) -> Result<()> {
        if !self.pending.is_empty() {
            return Err(AudioError::IncompleteFrame {
                length: self.pending.len(),
                frame_bytes: self.format.frame_bytes(),
            });
        }
        Ok(())
    }

    pub fn reset(&mut self) {
        self.pending.clear();
    }
}

/// Reblock an arbitrary stream into fixed-size, channel-aligned PCM chunks.
#[derive(Debug, Clone)]
pub struct PcmChunker {
    format: PcmFormat,
    chunk_bytes: usize,
    pending: VecDeque<u8>,
}

impl PcmChunker {
    pub fn new(format: PcmFormat, frames_per_chunk: usize) -> Result<Self> {
        if frames_per_chunk == 0 {
            return Err(AudioError::InvalidParameter(
                "frames_per_chunk must be positive",
            ));
        }
        let chunk_bytes = frames_per_chunk
            .checked_mul(format.frame_bytes())
            .ok_or(AudioError::SizeOverflow)?;
        Ok(Self {
            format,
            chunk_bytes,
            pending: VecDeque::new(),
        })
    }

    pub fn pending_bytes(&self) -> usize {
        self.pending.len()
    }

    pub fn push(&mut self, bytes: &[u8]) -> Vec<Vec<u8>> {
        self.pending.extend(bytes);
        let mut chunks = Vec::new();
        while self.pending.len() >= self.chunk_bytes {
            chunks.push(self.pending.drain(..self.chunk_bytes).collect());
        }
        chunks
    }

    /// Return a shorter final chunk only if it contains complete channel frames.
    /// On an incomplete frame, preserve all buffered bytes so the caller can retry.
    pub fn flush(&mut self) -> Result<Vec<u8>> {
        if !self.pending.len().is_multiple_of(self.format.frame_bytes()) {
            return Err(AudioError::IncompleteFrame {
                length: self.pending.len(),
                frame_bytes: self.format.frame_bytes(),
            });
        }
        Ok(self.pending.drain(..).collect())
    }

    pub fn reset(&mut self) {
        self.pending.clear();
    }
}
