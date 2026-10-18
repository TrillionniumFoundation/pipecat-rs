//! Native bounded REST transcription and HTTP/opt-in WebSocket speech synthesis.
//!
//! Provider protocols have distinct typed requests; they are not interchangeable
//! OpenAI presets. See the crate README for endpoint coverage and limitations.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
pub mod cartesia;
mod common;
pub mod deepgram;
#[cfg(feature = "deepgram-ws")]
pub mod deepgram_ws;
pub mod elevenlabs;
#[cfg(feature = "minimax")]
pub mod minimax;
mod openai;
pub mod processor;
#[cfg(feature = "speechify")]
pub mod speechify;
#[cfg(feature = "speechmatics")]
pub mod speechmatics;
pub mod stt_processor;
pub use openai::{
    OpenAiTranscriptionOptions, OpenAiTranscriptionResult, SpeechClient, SynthesisRequest,
    Transcript, TranscriptionRequest,
};
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
pub use stt_processor::{HttpSttBackend, HttpSttOptions, HttpSttProcessor};
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
    #[cfg(feature = "minimax")]
    pub fn minimax(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.minimax.io", api_key)
    }
    pub fn cartesia(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.cartesia.ai", api_key)
    }
    #[cfg(feature = "speechify")]
    pub fn speechify(api_key: impl Into<String>) -> Self {
        Self::provider("https://api.speechify.ai", api_key)
    }
    #[cfg(feature = "speechmatics")]
    pub fn speechmatics(api_key: impl Into<String>) -> Self {
        Self::provider("https://preview.tts.speechmatics.com", api_key)
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
/// Use [`Self::cancel_and_join`] to await task destruction; dropping only aborts it.
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
                let _ = self.abort_and_join().await;
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

    /// Discard buffered output, cancel the request, and await its owned task.
    ///
    /// Expected cancellation is successful cleanup. A previously unreported
    /// provider or task failure is returned once; already joined streams return
    /// success. Cancelling this waiting future retains the task so a later call
    /// can resume the join. Task destruction may itself take time, so this does
    /// not impose a separate cleanup deadline.
    pub async fn cancel_and_join(&mut self) -> Result<(), SpeechError> {
        if self.ended {
            return Ok(());
        }
        self.cancel.cancel();
        self.abort_and_join().await
    }

    async fn abort_and_join(&mut self) -> Result<(), SpeechError> {
        self.rx.close();
        while self.rx.try_recv().is_ok() {}
        self.task.abort();
        let result = (&mut self.task).await;
        self.ended = true;
        // A sender holding a permit can finish its send after receiver.close().
        // Joining first ensures no producer can refill the drained queue.
        while self.rx.try_recv().is_ok() {}
        match result {
            Ok(Ok(())) | Ok(Err(SpeechError::Cancelled)) => Ok(()),
            Ok(Err(error)) => Err(error),
            Err(error) if error.is_cancelled() => Ok(()),
            Err(_) => Err(SpeechError::Transport),
        }
    }
}
impl<T> Drop for ResponseStream<T> {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.task.abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::poll;
    use std::sync::mpsc as blocking;
    use tokio::sync::oneshot;

    const WAIT: Duration = Duration::from_secs(5);

    struct DestructionGuard {
        started: Option<oneshot::Sender<()>>,
        release: blocking::Receiver<()>,
        destroyed: Arc<AtomicBool>,
    }
    impl Drop for DestructionGuard {
        fn drop(&mut self) {
            let _ = self.started.take().unwrap().send(());
            // The test uses a multi-thread runtime. Dropping its release sender
            // also releases this wait if an assertion fails.
            let _ = self.release.recv();
            self.destroyed.store(true, Ordering::Release);
        }
    }
    struct GatedStream {
        stream: ResponseStream<u8>,
        dropping: oneshot::Receiver<()>,
        release: blocking::Sender<()>,
        destroyed: Arc<AtomicBool>,
    }
    async fn gated_stream(buffered: bool, finish: bool) -> GatedStream {
        let (tx, rx) = mpsc::channel(2);
        let (started, dropping) = oneshot::channel();
        let (release, release_rx) = blocking::channel();
        let (ready_tx, ready_rx) = oneshot::channel();
        let destroyed = Arc::new(AtomicBool::new(false));
        let guard = DestructionGuard {
            started: Some(started),
            release: release_rx,
            destroyed: destroyed.clone(),
        };
        let task = tokio::spawn(async move {
            let _guard = guard;
            if buffered {
                tx.send(7).await.unwrap();
            }
            ready_tx.send(()).unwrap();
            if !finish {
                std::future::pending::<()>().await;
            }
            drop(tx);
            Ok(())
        });
        ready_rx.await.unwrap();
        GatedStream {
            stream: ResponseStream {
                rx,
                cancel: CancellationToken::new(),
                task,
                ended: false,
                source_audio: Arc::new(AtomicBool::new(false)),
            },
            dropping,
            release,
            destroyed,
        }
    }
    async fn finished_stream(result: Result<(), SpeechError>) -> ResponseStream<u8> {
        let (tx, rx) = mpsc::channel(1);
        let task = tokio::spawn(async move {
            drop(tx);
            result
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        ResponseStream {
            rx,
            cancel: CancellationToken::new(),
            task,
            ended: false,
            source_audio: Arc::new(AtomicBool::new(false)),
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_join_wait_retains_task_and_discards_buffered_output() {
        let GatedStream {
            mut stream,
            dropping,
            release,
            destroyed,
        } = gated_stream(true, false).await;
        let mut first = Box::pin(stream.cancel_and_join());
        assert!(poll!(first.as_mut()).is_pending());
        tokio::time::timeout(WAIT, dropping).await.unwrap().unwrap();
        assert!(!destroyed.load(Ordering::Acquire));
        assert!(poll!(first.as_mut()).is_pending());
        drop(first);
        assert!(!stream.ended);
        assert!(!stream.task.is_finished());
        assert!(stream.rx.try_recv().is_err());

        let mut resumed = Box::pin(stream.cancel_and_join());
        assert!(poll!(resumed.as_mut()).is_pending());
        release.send(()).unwrap();
        tokio::time::timeout(WAIT, resumed).await.unwrap().unwrap();
        assert!(destroyed.load(Ordering::Acquire));
        assert!(stream.ended);
        assert!(stream.task.is_finished());
        stream.cancel_and_join().await.unwrap();
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn next_cancellation_waits_for_destruction_and_resumes_after_dropped_wait() {
        let GatedStream {
            mut stream,
            dropping,
            release,
            destroyed,
        } = gated_stream(true, false).await;
        stream.cancel();
        let mut first = Box::pin(stream.next());
        assert!(poll!(first.as_mut()).is_pending());
        tokio::time::timeout(WAIT, dropping).await.unwrap().unwrap();
        assert!(!destroyed.load(Ordering::Acquire));
        assert!(poll!(first.as_mut()).is_pending());
        drop(first);
        assert!(!stream.ended);
        assert!(!stream.task.is_finished());
        release.send(()).unwrap();
        assert!(matches!(
            tokio::time::timeout(WAIT, stream.next()).await.unwrap(),
            Some(Err(SpeechError::Cancelled))
        ));
        assert!(destroyed.load(Ordering::Acquire));
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn normal_eof_join_is_resumable_and_keeps_successful_output() {
        let GatedStream {
            mut stream,
            dropping,
            release,
            destroyed,
        } = gated_stream(true, true).await;
        tokio::time::timeout(WAIT, dropping).await.unwrap().unwrap();
        assert_eq!(stream.next().await.unwrap().unwrap(), 7);
        let mut first = Box::pin(stream.next());
        assert!(poll!(first.as_mut()).is_pending());
        drop(first);
        assert!(!stream.ended);
        assert!(!destroyed.load(Ordering::Acquire));
        assert!(!stream.task.is_finished());
        release.send(()).unwrap();
        assert!(tokio::time::timeout(WAIT, stream.next())
            .await
            .unwrap()
            .is_none());
        assert!(destroyed.load(Ordering::Acquire));
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn drop_only_aborts_without_waiting_for_destruction() {
        let GatedStream {
            stream,
            dropping,
            release,
            destroyed,
        } = gated_stream(false, false).await;
        drop(stream);
        tokio::time::timeout(WAIT, dropping).await.unwrap().unwrap();
        assert!(!destroyed.load(Ordering::Acquire));
        release.send(()).unwrap();
        tokio::time::timeout(WAIT, async {
            while !destroyed.load(Ordering::Acquire) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn cancel_and_join_reports_unobserved_failure_only_once() {
        let mut stream = finished_stream(Err(SpeechError::Http(503))).await;
        assert!(matches!(
            stream.cancel_and_join().await,
            Err(SpeechError::Http(503))
        ));
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn cancel_and_join_reports_task_panic_only_once() {
        let (tx, rx) = mpsc::channel::<u8>(1);
        let task = tokio::spawn(async move {
            drop(tx);
            panic!("synthetic task failure");
        });
        while !task.is_finished() {
            tokio::task::yield_now().await;
        }
        let mut stream = ResponseStream {
            rx,
            cancel: CancellationToken::new(),
            task,
            ended: false,
            source_audio: Arc::new(AtomicBool::new(false)),
        };
        assert!(matches!(
            stream.cancel_and_join().await,
            Err(SpeechError::Transport)
        ));
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn joined_failure_and_expected_cancellation_are_cleanup_success() {
        let mut stream = finished_stream(Err(SpeechError::Timeout)).await;
        assert!(matches!(
            stream.next().await,
            Some(Err(SpeechError::Timeout))
        ));
        stream.cancel_and_join().await.unwrap();
        let mut stream = finished_stream(Err(SpeechError::Cancelled)).await;
        stream.cancel_and_join().await.unwrap();
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn next_preserves_cancelled_result_over_preexisting_provider_failure() {
        let mut stream = finished_stream(Err(SpeechError::Http(503))).await;
        stream.cancel();
        assert!(matches!(
            stream.next().await,
            Some(Err(SpeechError::Cancelled))
        ));
        stream.cancel_and_join().await.unwrap();
        assert!(stream.next().await.is_none());
    }
}
