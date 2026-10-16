//! Actor-owned bounded PCM16 suffix. Successful local flush is not a provider ACK.
use super::RealtimeError;
use std::collections::VecDeque;

pub(crate) fn bytes_for_duration(seconds: f64) -> Result<usize, RealtimeError> {
    if !seconds.is_finite() || seconds <= 0.0 || seconds > 5.0 {
        return Err(RealtimeError::Invalid(
            "audio preroll must be finite and within (0, 5] seconds",
        ));
    }
    let samples = (seconds * 24_000.0).floor() as usize;
    let bytes = samples.checked_mul(2).ok_or(RealtimeError::TooLarge)?;
    if bytes == 0 || bytes > 240_000 {
        return Err(RealtimeError::Invalid(
            "audio preroll requires at least one PCM16 sample",
        ));
    }
    Ok(bytes)
}

pub(super) struct Preroll {
    capacity: usize,
    bytes: VecDeque<u8>,
}
impl Preroll {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            capacity,
            bytes: VecDeque::new(),
        }
    }
    pub(super) fn resize(&mut self, capacity: usize) {
        self.capacity = capacity;
        let excess = self.bytes.len().saturating_sub(capacity);
        self.bytes.drain(..excess);
    }
    pub(super) fn append(&mut self, audio: &[u8]) {
        let suffix = &audio[audio.len().saturating_sub(self.capacity)..];
        let excess = self
            .bytes
            .len()
            .saturating_add(suffix.len())
            .saturating_sub(self.capacity);
        self.bytes.drain(..excess.min(self.bytes.len()));
        self.bytes.extend(suffix);
    }
    pub(super) fn snapshot(&self) -> Vec<u8> {
        self.bytes.iter().copied().collect()
    }
}
