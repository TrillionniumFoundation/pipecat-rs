//! Native bounded REST transcription and HTTP speech synthesis.
//!
//! Provider protocols have distinct typed requests; they are not interchangeable
//! OpenAI presets. See the crate README for endpoint coverage and limitations.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
pub mod cartesia;
mod common;
pub mod deepgram;
pub mod elevenlabs;
mod openai;
pub mod processor;
pub use openai::{SpeechClient, SynthesisRequest, Transcript, TranscriptionRequest};
use pipecat_services::CancellationToken;
pub use processor::{HttpTtsBackend, HttpTtsProcessor};
use std::{
    fmt,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{sync::mpsc, task::JoinHandle};

/// Shared transport policy. `base_url` is the provider root (OpenAI includes `/v1`).
/// Audio limits cover decoded PCM and upload WAV bytes; JSON limits cover a
/// transcription response or an individual ElevenLabs NDJSON record.
#[derive(Clone)]
pub struct SpeechConfig {
    pub base_url: String,
    pub api_key: String,
    pub timeout: Duration,
    pub max_audio_bytes: usize,
    pub max_json_bytes: usize,
    pub queue_capacity: usize,
}
impl fmt::Debug for SpeechConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SpeechConfig")
            .field("timeout", &self.timeout)
            .field("api_key", &"[redacted]")
            .finish_non_exhaustive()
    }
}
impl SpeechConfig {
    pub fn openai(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.openai.com/v1", api_key)
    }
    pub fn elevenlabs(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.elevenlabs.io", api_key)
    }
    pub fn deepgram(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.deepgram.com", api_key)
    }
    pub fn cartesia(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.cartesia.ai", api_key)
    }
    fn provider(base_url: &str, api_key: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            api_key: api_key.into(),
            timeout: Duration::from_secs(60),
            max_audio_bytes: 25 * 1024 * 1024,
            max_json_bytes: 1024 * 1024,
            queue_capacity: 16,
        }
    }
}
#[derive(thiserror::Error)]
pub enum SpeechError {
    #[error("invalid speech configuration or request: {0}")]
    Invalid(&'static str),
    #[error("audio format: {0}")]
    Audio(#[from] pipecat_audio::AudioError),
    #[error("HTTP {0}")]
    Http(u16),
    #[error("speech transport failed")]
    Transport,
    #[error("speech request timed out")]
    Timeout,
    #[error("speech request cancelled")]
    Cancelled,
    #[error("response exceeds configured limit")]
    TooLarge,
    #[error("invalid provider JSON response")]
    Json(#[from] serde_json::Error),
    #[error("invalid provider protocol: {0}")]
    Protocol(&'static str),
    #[error("PCM response ends in a partial sample")]
    PartialSample,
    #[error("empty audio response")]
    EmptyAudio,
    #[error("unexpected audio content type or format")]
    ContentType,
    #[error("consumer closed")]
    ConsumerClosed,
}
// Provider-controlled JSON errors may contain fragments of the response body.
impl fmt::Debug for SpeechError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpeechChunk {
    /// Signed 16-bit little-endian mono PCM, including when the wire codec is G.711.
    pub pcm: Vec<u8>,
    pub sample_rate: u32,
    pub num_channels: u16,
}
pub type SpeechStream = ResponseStream<SpeechChunk>;
/// Bounded stream with a terminal error, explicit cancellation and cancel-on-drop.
/// Cancellation discards buffered output; reaching EOF does not acknowledge playout.
pub struct ResponseStream<T> {
    rx: mpsc::Receiver<T>,
    cancel: CancellationToken,
    task: JoinHandle<Result<(), SpeechError>>,
    ended: bool,
    source_audio: Arc<AtomicBool>,
}
impl<T> ResponseStream<T> {
    pub(crate) fn source_audio_observed(&self) -> bool {
        self.source_audio.load(Ordering::Acquire)
    }
    pub async fn next(&mut self) -> Option<Result<T, SpeechError>> {
        if self.ended {
            return None;
        }
        tokio::select! { biased;
            _ = self.cancel.cancelled() => {
                self.rx.close(); while self.rx.try_recv().is_ok() {}
                self.task.abort(); self.ended = true;
                return Some(Err(SpeechError::Cancelled));
            }
            chunk = self.rx.recv() => if let Some(chunk) = chunk { return Some(Ok(chunk)); }
        }
        let result = (&mut self.task).await;
        self.ended = true;
        match result {
            Ok(Ok(())) => None,
            Ok(Err(error)) => Some(Err(error)),
            Err(_) => Some(Err(SpeechError::Transport)),
        }
    }
    pub fn cancel(&self) {
        self.cancel.cancel()
    }
}
impl<T> Drop for ResponseStream<T> {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}
