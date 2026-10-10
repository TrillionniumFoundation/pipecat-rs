// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause

//! Clocked, bounded `BaseSmartTurn` audio policy and serial native inference.

use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};

use ort::session::RunOptions;
use tokio::sync::oneshot;

use crate::{Result, SmartTurnModel, SmartTurnPrediction, VadError};

/// Whether the captured utterance is complete. Maximum audio length is not a
/// completion condition; it only selects the tail passed to the model.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum EndOfTurnState {
    Complete,
    Incomplete,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SmartTurnParams {
    pub stop_secs: f64,
    pub pre_speech_ms: f64,
    pub max_duration_secs: f64,
}
impl Default for SmartTurnParams {
    fn default() -> Self {
        Self {
            stop_secs: 3.0,
            pre_speech_ms: 500.0,
            max_duration_secs: 8.0,
        }
    }
}
impl SmartTurnParams {
    fn validate(&self, sample_rate: u32) -> Result<usize> {
        if sample_rate == 0
            || sample_rate > 192_000
            || !self.stop_secs.is_finite()
            || !(0.0..=3600.0).contains(&self.stop_secs)
            || !self.pre_speech_ms.is_finite()
            || !(0.0..=3_600_000.0).contains(&self.pre_speech_ms)
            || !self.max_duration_secs.is_finite()
            || !(0.0..=600.0).contains(&self.max_duration_secs)
        {
            return Err(VadError::InvalidParameter(
                "invalid Smart Turn rate or timing parameters",
            ));
        }
        let samples = (self.max_duration_secs * f64::from(sample_rate)) as usize;
        if samples == 0 {
            return Err(VadError::InvalidParameter(
                "Smart Turn maximum duration must contain at least one sample",
            ));
        }
        Ok(samples)
    }
}

#[derive(Debug)]
struct Chunk {
    at: Duration,
    samples: Vec<i16>,
}

/// Sample-bounded audio history. Timestamps select whole chunks for pre-roll;
/// sample counts measure silence and select the last maximum-duration samples.
///
/// Retaining only the model's final window preserves prediction input while
/// bounding continuous speech, unpaced input, and pre-speech memory. The chunk
/// metadata limit is explicit: excessive fragmentation returns an error.
#[derive(Debug)]
pub struct SmartTurnBuffer {
    params: SmartTurnParams,
    sample_rate: u32,
    max_samples: usize,
    max_chunks: usize,
    chunks: VecDeque<Chunk>,
    sample_count: usize,
    speech_triggered: bool,
    speech_start: Option<Duration>,
    silence_ms: f64,
    vad_start_secs: f64,
    last_time: Option<Duration>,
    generation: u64,
}
impl SmartTurnBuffer {
    pub fn new(sample_rate: u32, params: SmartTurnParams) -> Result<Self> {
        Self::with_chunk_limit(sample_rate, params, 8192)
    }
    pub fn with_chunk_limit(
        sample_rate: u32,
        params: SmartTurnParams,
        max_chunks: usize,
    ) -> Result<Self> {
        let max_samples = params.validate(sample_rate)?;
        if max_chunks == 0 || max_chunks > 1_000_000 {
            return Err(VadError::InvalidParameter(
                "Smart Turn chunk limit must be in [1, 1000000]",
            ));
        }
        Ok(Self {
            params,
            sample_rate,
            max_samples,
            max_chunks,
            chunks: VecDeque::new(),
            sample_count: 0,
            speech_triggered: false,
            speech_start: None,
            silence_ms: 0.0,
            vad_start_secs: 0.0,
            last_time: None,
            generation: 0,
        })
    }
    pub fn params(&self) -> &SmartTurnParams {
        &self.params
    }
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }
    pub fn speech_triggered(&self) -> bool {
        self.speech_triggered
    }
    pub fn speech_start(&self) -> Option<Duration> {
        self.speech_start
    }
    pub fn silence_ms(&self) -> f64 {
        self.silence_ms
    }
    pub fn buffered_samples(&self) -> usize {
        self.sample_count
    }
    pub fn buffered_chunks(&self) -> usize {
        self.chunks.len()
    }
    pub fn generation(&self) -> u64 {
        self.generation
    }
    pub fn update_vad_start_secs(&mut self, seconds: f64) -> Result<()> {
        if !seconds.is_finite() || !(0.0..=3600.0).contains(&seconds) {
            return Err(VadError::InvalidParameter(
                "Smart Turn VAD start delay must be finite and in [0, 3600]",
            ));
        }
        self.vad_start_secs = seconds;
        Ok(())
    }
    /// Explicit external turn reset. VAD pre-roll configuration survives resets.
    pub fn clear(&mut self) {
        self.chunks.clear();
        self.sample_count = 0;
        self.speech_triggered = false;
        self.speech_start = None;
        self.silence_ms = 0.0;
        self.generation = self.generation.wrapping_add(1);
    }
    pub fn append_audio_at(
        &mut self,
        bytes: &[u8],
        is_speech: bool,
        now: Duration,
    ) -> Result<EndOfTurnState> {
        if !bytes.len().is_multiple_of(2) {
            return Err(VadError::InvalidParameter(
                "Smart Turn PCM16 frame has an odd byte count",
            ));
        }
        self.append_iter_at(
            bytes
                .as_chunks::<2>()
                .0
                .iter()
                .map(|b| i16::from_le_bytes([b[0], b[1]])),
            bytes.len() / 2,
            is_speech,
            now,
        )
    }
    pub fn append_pcm16_at(
        &mut self,
        samples: &[i16],
        is_speech: bool,
        now: Duration,
    ) -> Result<EndOfTurnState> {
        self.append_iter_at(samples.iter().copied(), samples.len(), is_speech, now)
    }
    fn append_iter_at(
        &mut self,
        samples: impl Iterator<Item = i16>,
        count: usize,
        is_speech: bool,
        now: Duration,
    ) -> Result<EndOfTurnState> {
        if self.last_time.is_some_and(|last| now < last) {
            return Err(VadError::InvalidParameter(
                "Smart Turn clock must be monotonic",
            ));
        }
        // Check projected metadata size before changing any state.
        let keep = count.min(self.max_samples);
        let limit = self.max_samples - keep;
        let mut excess = self.sample_count.saturating_sub(limit);
        let mut dropped = 0;
        for chunk in &self.chunks {
            if (excess == 0 && limit != 0) || chunk.samples.len() > excess {
                break;
            }
            excess -= chunk.samples.len();
            dropped += 1;
        }
        if self.chunks.len() - dropped >= self.max_chunks {
            return Err(VadError::InvalidParameter(
                "Smart Turn audio history exceeds its chunk limit",
            ));
        }
        self.trim_to(limit);
        self.chunks.push_back(Chunk {
            at: now,
            samples: samples.skip(count - keep).collect(),
        });
        self.sample_count += keep;
        self.last_time = Some(now);
        if is_speech {
            self.silence_ms = 0.0;
            self.speech_triggered = true;
            self.speech_start.get_or_insert(now);
            // Any new speech makes an older endpoint prediction stale.
            self.generation = self.generation.wrapping_add(1);
        } else if self.speech_triggered {
            self.silence_ms += count as f64 / (f64::from(self.sample_rate) / 1000.0);
            if self.silence_ms >= self.params.stop_secs * 1000.0 {
                self.clear();
                return Ok(EndOfTurnState::Complete);
            }
        } else {
            let age = self.params.pre_speech_ms / 1000.0
                + self.params.stop_secs
                + self.params.max_duration_secs;
            while self
                .chunks
                .front()
                .is_some_and(|chunk| chunk.at.as_secs_f64() < now.as_secs_f64() - age)
            {
                self.sample_count -= self
                    .chunks
                    .pop_front()
                    .expect("front was checked")
                    .samples
                    .len();
            }
        }
        Ok(EndOfTurnState::Incomplete)
    }
    fn trim_to(&mut self, limit: usize) {
        while self.sample_count > limit || (limit == 0 && !self.chunks.is_empty()) {
            let excess = self.sample_count.saturating_sub(limit);
            let front = self
                .chunks
                .front_mut()
                .expect("nonzero samples have a chunk");
            if front.samples.len() <= excess {
                self.sample_count -= self
                    .chunks
                    .pop_front()
                    .expect("front was checked")
                    .samples
                    .len();
            } else {
                front.samples.drain(..excess);
                self.sample_count -= excess;
            }
        }
    }
    /// The exact PCM16 model input, before model-specific front padding.
    pub fn analysis_samples(&self) -> Vec<i16> {
        let start = self.speech_start.map_or(0.0, |at| at.as_secs_f64())
            - (self.params.pre_speech_ms + self.vad_start_secs * 1000.0) / 1000.0;
        // Source behavior falls back to the whole buffer if no timestamp qualifies.
        let index = self
            .chunks
            .iter()
            .position(|chunk| chunk.at.as_secs_f64() >= start)
            .unwrap_or(0);
        self.chunks
            .iter()
            .skip(index)
            .flat_map(|chunk| chunk.samples.iter().copied())
            .collect()
    }
    /// Stale predictions cannot clear a resumed or externally reset turn.
    pub fn apply(&mut self, analysis: &SmartTurnAnalysis) -> bool {
        if analysis.generation != self.generation {
            return false;
        }
        if analysis.state == EndOfTurnState::Complete {
            self.clear();
        }
        true
    }
}

/// Injectable monotonic clock for buffer timestamps and model timing metrics.
pub trait SmartTurnClock: Send + Sync {
    fn now(&self) -> Duration;
}
#[derive(Debug)]
pub struct MonotonicSmartTurnClock {
    origin: Instant,
}
impl Default for MonotonicSmartTurnClock {
    fn default() -> Self {
        Self {
            origin: Instant::now(),
        }
    }
}
impl SmartTurnClock for MonotonicSmartTurnClock {
    fn now(&self) -> Duration {
        self.origin.elapsed()
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct SmartTurnMetrics {
    pub processor: &'static str,
    pub is_complete: bool,
    pub probability: f32,
    /// Feature extraction and inference; excludes queueing time.
    pub e2e_processing_time_ms: f64,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SmartTurnCompletionReason {
    Empty,
    Model,
    Timeout,
}
#[derive(Debug, Clone, PartialEq)]
pub struct SmartTurnAnalysis {
    pub state: EndOfTurnState,
    pub metrics: Option<SmartTurnMetrics>,
    pub reason: SmartTurnCompletionReason,
    pub generation: u64,
}

#[derive(Default)]
struct CancellationState {
    cancelled: AtomicBool,
    run: Mutex<Option<Arc<RunOptions>>>,
}
/// Cancellation is cooperative during preprocessing and terminates an active
/// ONNX run through its per-request RunOptions. It never terminates another job.
#[derive(Clone, Default)]
pub struct SmartTurnCancellation {
    inner: Arc<CancellationState>,
}
impl SmartTurnCancellation {
    pub fn is_cancelled(&self) -> bool {
        self.inner.cancelled.load(Ordering::SeqCst)
    }
    /// Whether per-request ONNX run options are registered for termination.
    pub fn is_inference_running(&self) -> bool {
        self.inner.run.lock().is_ok_and(|slot| slot.is_some())
    }
    pub fn cancel(&self) {
        self.inner.cancelled.store(true, Ordering::SeqCst);
        if let Ok(run) = self.inner.run.lock() {
            if let Some(run) = run.as_ref() {
                let _ = run.terminate();
            }
        }
    }
    pub(crate) fn install(&self, run: Arc<RunOptions>) -> Result<()> {
        let mut slot = self
            .inner
            .run
            .lock()
            .map_err(|_| VadError::Runtime("Smart Turn cancellation lock poisoned".into()))?;
        if self.is_cancelled() {
            return Err(VadError::Cancelled);
        }
        *slot = Some(run);
        Ok(())
    }
    pub(crate) fn uninstall(&self) {
        if let Ok(mut slot) = self.inner.run.lock() {
            slot.take();
        }
    }
}

/// Replaceable only for deterministic tests or alternative native models. The
/// production implementation is SmartTurnModel and invokes the actual ONNX graph.
pub trait SmartTurnPredictor: Send + 'static {
    fn predict(
        &mut self,
        samples: &[i16],
        sample_rate: u32,
        cancellation: &SmartTurnCancellation,
    ) -> Result<SmartTurnPrediction>;
}
impl SmartTurnPredictor for SmartTurnModel {
    fn predict(
        &mut self,
        samples: &[i16],
        sample_rate: u32,
        cancellation: &SmartTurnCancellation,
    ) -> Result<SmartTurnPrediction> {
        self.predict_pcm16_cancellable(samples, sample_rate, cancellation)
    }
}

struct Request {
    samples: Vec<i16>,
    sample_rate: u32,
    generation: u64,
    cancellation: SmartTurnCancellation,
    response: oneshot::Sender<Result<SmartTurnAnalysis>>,
}
/// A single snapshot awaiting its model result. Dropping it cancels the native
/// run; timeout concludes COMPLETE without fabricated probability or metrics.
pub struct PendingSmartTurnAnalysis {
    response: Option<oneshot::Receiver<Result<SmartTurnAnalysis>>>,
    cancellation: SmartTurnCancellation,
    generation: u64,
    deadline: Option<tokio::time::Instant>,
}
impl PendingSmartTurnAnalysis {
    pub fn cancellation(&self) -> SmartTurnCancellation {
        self.cancellation.clone()
    }
    pub async fn wait(mut self) -> Result<SmartTurnAnalysis> {
        let receiver = self
            .response
            .take()
            .expect("pending result is consumed once");
        let result = if let Some(deadline) = self.deadline {
            match tokio::time::timeout_at(deadline, receiver).await {
                Ok(result) => result,
                Err(_) => {
                    if self.cancellation.is_cancelled() {
                        return Err(VadError::Cancelled);
                    }
                    self.cancellation.cancel();
                    return Ok(SmartTurnAnalysis {
                        state: EndOfTurnState::Complete,
                        metrics: None,
                        reason: SmartTurnCompletionReason::Timeout,
                        generation: self.generation,
                    });
                }
            }
        } else {
            receiver.await
        };
        if self.cancellation.is_cancelled() {
            return Err(VadError::Cancelled);
        }
        result.map_err(|_| {
            VadError::Runtime("Smart Turn inference worker stopped without a result".into())
        })?
    }
}
impl Drop for PendingSmartTurnAnalysis {
    fn drop(&mut self) {
        self.cancellation.cancel();
    }
}

/// Owns one reusable native model and at most one active and one queued snapshot.
/// Cleanup cancels and joins the owned thread; the model can then be used again.
pub struct SmartTurnAnalyzer {
    buffer: SmartTurnBuffer,
    clock: Arc<dyn SmartTurnClock>,
    model: Arc<Mutex<Option<Box<dyn SmartTurnPredictor>>>>,
    sender: Option<std::sync::mpsc::SyncSender<Request>>,
    thread: Option<std::thread::JoinHandle<()>>,
    cancellations: Vec<std::sync::Weak<CancellationState>>,
    inference_timeout: Option<Duration>,
}
impl SmartTurnAnalyzer {
    pub fn new(model: SmartTurnModel, params: SmartTurnParams) -> Result<Self> {
        Self::with_predictor(
            model,
            16_000,
            params,
            Arc::new(MonotonicSmartTurnClock::default()),
            None,
        )
    }
    pub fn with_predictor(
        predictor: impl SmartTurnPredictor,
        sample_rate: u32,
        params: SmartTurnParams,
        clock: Arc<dyn SmartTurnClock>,
        inference_timeout: Option<Duration>,
    ) -> Result<Self> {
        if inference_timeout
            .is_some_and(|duration| duration.is_zero() || duration > Duration::from_secs(86400))
        {
            return Err(VadError::InvalidParameter(
                "Smart Turn inference timeout must be in (0, 86400] seconds",
            ));
        }
        Ok(Self {
            buffer: SmartTurnBuffer::new(sample_rate, params)?,
            clock,
            model: Arc::new(Mutex::new(Some(Box::new(predictor)))),
            sender: None,
            thread: None,
            cancellations: Vec::new(),
            inference_timeout,
        })
    }
    pub fn buffer(&self) -> &SmartTurnBuffer {
        &self.buffer
    }
    pub fn set_inference_timeout(&mut self, timeout: Option<Duration>) -> Result<()> {
        if timeout
            .is_some_and(|duration| duration.is_zero() || duration > Duration::from_secs(86400))
        {
            return Err(VadError::InvalidParameter(
                "Smart Turn inference timeout must be in (0, 86400] seconds",
            ));
        }
        self.inference_timeout = timeout;
        Ok(())
    }
    pub fn update_vad_start_secs(&mut self, seconds: f64) -> Result<()> {
        self.buffer.update_vad_start_secs(seconds)
    }
    pub fn append_audio(&mut self, bytes: &[u8], is_speech: bool) -> Result<EndOfTurnState> {
        let state = self
            .buffer
            .append_audio_at(bytes, is_speech, self.clock.now())?;
        if is_speech || state == EndOfTurnState::Complete {
            self.cancel_pending();
        }
        Ok(state)
    }
    pub fn append_pcm16(&mut self, samples: &[i16], is_speech: bool) -> Result<EndOfTurnState> {
        let state = self
            .buffer
            .append_pcm16_at(samples, is_speech, self.clock.now())?;
        if is_speech || state == EndOfTurnState::Complete {
            self.cancel_pending();
        }
        Ok(state)
    }
    pub fn clear(&mut self) {
        self.cancel_pending();
        self.buffer.clear();
    }
    fn cancel_pending(&mut self) {
        for state in self
            .cancellations
            .drain(..)
            .filter_map(|weak| weak.upgrade())
        {
            SmartTurnCancellation { inner: state }.cancel();
        }
    }
    fn start_worker(&mut self) -> Result<()> {
        if self.sender.is_some() {
            return Ok(());
        }
        let mut slot = self
            .model
            .lock()
            .map_err(|_| VadError::Runtime("Smart Turn model lock poisoned".into()))?;
        let model = slot.take().ok_or_else(|| {
            VadError::Runtime("Smart Turn cleanup is still joining its worker".into())
        })?;
        let model_slot = self.model.clone();
        let clock = self.clock.clone();
        let (sender, receiver) = std::sync::mpsc::sync_channel::<Request>(1);
        // Recover the model if creating the native thread fails.
        let transfer = Arc::new(Mutex::new(Some(model)));
        let thread_transfer = transfer.clone();
        let handle = std::thread::Builder::new()
            .name("pipecat-smart-turn".into())
            .spawn(move || {
                let mut model = thread_transfer
                    .lock()
                    .expect("uncontended model transfer")
                    .take()
                    .expect("owned model");
                while let Ok(request) = receiver.recv() {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        if request.cancellation.is_cancelled() {
                            return Err(VadError::Cancelled);
                        }
                        if request.samples.is_empty() {
                            return Ok(SmartTurnAnalysis {
                                state: EndOfTurnState::Incomplete,
                                metrics: None,
                                reason: SmartTurnCompletionReason::Empty,
                                generation: request.generation,
                            });
                        }
                        let start = clock.now();
                        let prediction = match model.predict(
                            &request.samples,
                            request.sample_rate,
                            &request.cancellation,
                        ) {
                            Ok(prediction) => prediction,
                            Err(VadError::SmartTurnTimeout)
                                if request.cancellation.is_cancelled() =>
                            {
                                return Err(VadError::Cancelled)
                            }
                            Err(VadError::SmartTurnTimeout) => {
                                return Ok(SmartTurnAnalysis {
                                    state: EndOfTurnState::Complete,
                                    metrics: None,
                                    reason: SmartTurnCompletionReason::Timeout,
                                    generation: request.generation,
                                })
                            }
                            Err(error) => return Err(error),
                        };
                        let elapsed = clock.now().saturating_sub(start);
                        if request.cancellation.is_cancelled() {
                            return Err(VadError::Cancelled);
                        }
                        Ok(SmartTurnAnalysis {
                            state: if prediction.complete {
                                EndOfTurnState::Complete
                            } else {
                                EndOfTurnState::Incomplete
                            },
                            metrics: Some(SmartTurnMetrics {
                                processor: "BaseSmartTurn",
                                is_complete: prediction.complete,
                                probability: prediction.probability,
                                e2e_processing_time_ms: elapsed.as_secs_f64() * 1000.0,
                            }),
                            reason: SmartTurnCompletionReason::Model,
                            generation: request.generation,
                        })
                    }))
                    .unwrap_or_else(|_| {
                        Err(VadError::Runtime("Smart Turn predictor panicked".into()))
                    });
                    let _ = request.response.send(result);
                }
                if let Ok(mut slot) = model_slot.lock() {
                    *slot = Some(model);
                }
            });
        match handle {
            Ok(handle) => {
                self.thread = Some(handle);
                self.sender = Some(sender);
                Ok(())
            }
            Err(error) => {
                *slot = transfer
                    .lock()
                    .map_err(|_| VadError::Runtime("Smart Turn model transfer poisoned".into()))?
                    .take();
                Err(VadError::Runtime(format!(
                    "cannot start Smart Turn thread: {error}"
                )))
            }
        }
    }
    pub fn begin_analysis(&mut self) -> Result<PendingSmartTurnAnalysis> {
        let samples = self.buffer.analysis_samples();
        let cancellation = SmartTurnCancellation::default();
        let (response, receiver) = oneshot::channel();
        let generation = self.buffer.generation;
        if samples.is_empty() {
            let _ = response.send(Ok(SmartTurnAnalysis {
                state: EndOfTurnState::Incomplete,
                metrics: None,
                reason: SmartTurnCompletionReason::Empty,
                generation,
            }));
            return Ok(PendingSmartTurnAnalysis {
                response: Some(receiver),
                cancellation,
                generation,
                deadline: None,
            });
        }
        self.start_worker()?;
        let request = Request {
            samples,
            sample_rate: self.buffer.sample_rate,
            generation,
            cancellation: cancellation.clone(),
            response,
        };
        self.sender
            .as_ref()
            .expect("worker started")
            .try_send(request)
            .map_err(|error| VadError::Runtime(format!("Smart Turn inference queue: {error}")))?;
        self.cancellations.retain(|weak| weak.strong_count() > 0);
        self.cancellations.push(Arc::downgrade(&cancellation.inner));
        Ok(PendingSmartTurnAnalysis {
            response: Some(receiver),
            cancellation,
            generation,
            deadline: self
                .inference_timeout
                .map(|timeout| tokio::time::Instant::now() + timeout),
        })
    }
    pub fn apply_analysis(&mut self, analysis: &SmartTurnAnalysis) -> bool {
        let accepted = self.buffer.apply(analysis);
        if accepted && analysis.state == EndOfTurnState::Complete {
            self.cancel_pending();
        }
        accepted
    }
    pub async fn analyze_end_of_turn(&mut self) -> Result<SmartTurnAnalysis> {
        let analysis = self.begin_analysis()?.wait().await?;
        self.apply_analysis(&analysis);
        Ok(analysis)
    }
    /// Cancel pending inference and join the actual native owner. Audio state is
    /// preserved, matching source cleanup; call clear for an external turn stop.
    /// Cancelling this async cleanup still lets the joining task finish and
    /// restore the model, so no new inference can overlap the old owner.
    pub async fn cleanup(&mut self) -> Result<()> {
        self.cancel_pending();
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            tokio::task::spawn_blocking(move || thread.join())
                .await
                .map_err(|error| VadError::Runtime(format!("joining Smart Turn worker: {error}")))?
                .map_err(|_| VadError::Runtime("Smart Turn worker panicked".into()))?;
        }
        Ok(())
    }
}
impl Drop for SmartTurnAnalyzer {
    fn drop(&mut self) {
        self.cancel_pending();
        self.sender.take();
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}
