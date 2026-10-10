use crate::{
    device, guard_callback, lock, native_error, AudioLimits, DeviceDirection, DeviceSelector,
    LocalAudioError, Signals,
};
use cpal::traits::{DeviceTrait, StreamTrait};
use pipecat_audio::PcmFormat;
use pipecat_core::{AudioRawFrame, Frame, FrameData};
use std::collections::VecDeque;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::oneshot;

/// Evidence that every sample was copied into CPAL's callback buffers.
///
/// This is **not** a native-write, DAC, speaker, or acoustic acknowledgement.
/// Even after this receipt, the native backend may reject or drop that buffer.
/// It must never be translated to `PlaybackReceipt::Played`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubmissionReceipt {
    pub frames: usize,
    pub sample_rate: u32,
    pub channels: u16,
    /// CPAL's final callback clock, relative to that stream's clock origin.
    pub callback_time: Duration,
    /// CPAL's prediction for the last sample, not proof it reached the device.
    pub predicted_playback_end: Duration,
}

/// A bounded native speaker writer. Does not implement `OutputBackend`, whose
/// stronger playback-acknowledgement contract cannot be guaranteed by CPAL.
pub struct LocalAudioOutput {
    selector: DeviceSelector,
    limits: AudioLimits,
    run: Mutex<Option<OutputRun>>,
}
struct OutputRun {
    stream: Option<cpal::Stream>,
    queue: Arc<Mutex<OutputQueue>>,
    signals: Arc<Signals>,
    format: PcmFormat,
}
impl OutputRun {
    fn close(&mut self, reason: LocalAudioError) {
        self.signals.fail(reason.clone());
        lock(&self.queue).cancel_all(reason);
        self.stream.take();
    }
}
impl Drop for OutputRun {
    fn drop(&mut self) {
        self.close(LocalAudioError::Stopped);
    }
}

impl LocalAudioOutput {
    pub fn new(selector: DeviceSelector, limits: AudioLimits) -> Result<Self, LocalAudioError> {
        limits.validate()?;
        Ok(Self {
            selector,
            limits,
            run: Mutex::new(None),
        })
    }
    /// Open and start the exact rate/channel format. Unsupported settings fail;
    /// no default-rate fallback or implicit channel remix is performed.
    pub fn open(&self, format: PcmFormat) -> Result<(), LocalAudioError> {
        let mut slot = lock(&self.run);
        if slot.is_some() {
            return Err(LocalAudioError::AlreadyStarted);
        }
        *slot = Some(self.open_run(format)?);
        Ok(())
    }
    fn open_run(&self, format: PcmFormat) -> Result<OutputRun, LocalAudioError> {
        let signals = Signals::new();
        let queue = Arc::new(Mutex::new(OutputQueue::new(format, self.limits.clone())));
        let device = device::select(&self.selector, DeviceDirection::Output)?;
        let (config, sample) = device::config(&device, DeviceDirection::Output, format)?;
        let callback = OutputCallback {
            queue: queue.clone(),
            signals: signals.clone(),
            format,
            max_callback_frames: self.limits.max_callback_frames,
        };
        macro_rules! build {
            ($ty:ty) => {
                build::<$ty>(
                    &device,
                    config,
                    callback,
                    signals.clone(),
                    self.limits.operation_timeout,
                )
            };
        }
        let stream = match sample {
            cpal::SampleFormat::I8 => build!(i8),
            cpal::SampleFormat::I16 => build!(i16),
            cpal::SampleFormat::I24 => build!(cpal::I24),
            cpal::SampleFormat::I32 => build!(i32),
            cpal::SampleFormat::I64 => build!(i64),
            cpal::SampleFormat::U8 => build!(u8),
            cpal::SampleFormat::U16 => build!(u16),
            cpal::SampleFormat::U24 => build!(cpal::U24),
            cpal::SampleFormat::U32 => build!(u32),
            cpal::SampleFormat::U64 => build!(u64),
            cpal::SampleFormat::F32 => build!(f32),
            cpal::SampleFormat::F64 => build!(f64),
            _ => {
                return Err(LocalAudioError::UnsupportedFormat {
                    rate: format.sample_rate(),
                    channels: format.channels(),
                })
            }
        }?;
        let run = OutputRun {
            stream: Some(stream),
            queue,
            signals,
            format,
        };
        run.stream.as_ref().unwrap().play().map_err(native_error)?;
        Ok(run)
    }
    /// Await full callback submission, bounded by `operation_timeout`. Dropping
    /// this future cancels remaining queued frames. Cancellation is observed
    /// between interleaved frames; one frame whose check already passed may
    /// finish copying concurrently with cancellation. Already copied device
    /// buffers cannot be retracted by cancelling a future; call `interrupt` to
    /// tear down the old stream before admitting newer audio.
    pub async fn write_audio(
        &self,
        audio: &AudioRawFrame,
    ) -> Result<SubmissionReceipt, LocalAudioError> {
        let (queue, signals) = {
            let slot = lock(&self.run);
            let run = slot.as_ref().ok_or(LocalAudioError::Stopped)?;
            (run.queue.clone(), run.signals.clone())
        };
        signals.check()?;
        let submission = lock(&queue).enqueue(audio)?;
        submission
            .wait(signals, self.limits.operation_timeout)
            .await
    }
    pub async fn write_frame(&self, frame: &Frame) -> Result<SubmissionReceipt, LocalAudioError> {
        if frame.transport_destination.is_some() {
            return Err(LocalAudioError::InvalidSettings(
                "local output supports only the default destination".into(),
            ));
        }
        match &frame.data {
            FrameData::OutputAudio(audio)
            | FrameData::TtsAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => self.write_audio(audio).await,
            _ => Err(LocalAudioError::InvalidAudio(
                "local output requires an output audio frame".into(),
            )),
        }
    }
    /// Cancel queued writes, destroy/join the old native stream, then reopen it.
    /// Sound already rendered by the device cannot be undone. If reopening
    /// fails, output remains closed and the error is returned.
    pub fn interrupt(&self) -> Result<(), LocalAudioError> {
        let mut slot = lock(&self.run);
        let Some(mut run) = slot.take() else {
            return Err(LocalAudioError::Stopped);
        };
        let format = run.format;
        run.close(LocalAudioError::Cancelled);
        drop(run);
        *slot = Some(self.open_run(format)?);
        Ok(())
    }
    /// Discard queued output and join native callback ownership. To retain data,
    /// await writes before closing; callback submission still is not playback.
    pub fn close(&self) {
        if let Some(mut run) = lock(&self.run).take() {
            run.close(LocalAudioError::Stopped);
        }
    }
    pub fn is_open(&self) -> bool {
        lock(&self.run)
            .as_ref()
            .is_some_and(|run| run.signals.check().is_ok())
    }
}
impl Drop for LocalAudioOutput {
    fn drop(&mut self) {
        self.close();
    }
}

fn build<T>(
    device: &cpal::Device,
    config: cpal::StreamConfig,
    mut callback: OutputCallback,
    signals: Arc<Signals>,
    timeout: Duration,
) -> Result<cpal::Stream, LocalAudioError>
where
    T: cpal::SizedSample + cpal::FromSample<i16>,
{
    let callback_signals = signals.clone();
    device
        .build_output_stream(
            config,
            move |data: &mut [T], info| {
                let timestamp = info.timestamp();
                if !guard_callback(&callback_signals, || {
                    callback.render(data, timestamp.callback, timestamp.playback)
                }) {
                    data.fill(T::EQUILIBRIUM);
                }
            },
            move |error| {
                guard_callback(&signals, || signals.fail(native_error(error)));
            },
            Some(timeout),
        )
        .map_err(native_error)
}

struct Write {
    audio: Vec<u8>,
    offset: usize,
    cancelled: Arc<AtomicBool>,
    receipt: oneshot::Sender<Result<SubmissionReceipt, LocalAudioError>>,
}
pub(crate) struct PendingSubmission {
    receiver: oneshot::Receiver<Result<SubmissionReceipt, LocalAudioError>>,
    cancelled: Arc<AtomicBool>,
}
impl Drop for PendingSubmission {
    fn drop(&mut self) {
        self.cancelled.store(true, Ordering::Release);
    }
}
impl PendingSubmission {
    pub(crate) async fn wait(
        mut self,
        signals: Arc<Signals>,
        timeout: Duration,
    ) -> Result<SubmissionReceipt, LocalAudioError> {
        let mut errors = signals.error.subscribe();
        signals.check()?;
        let wait = async {
            loop {
                tokio::select! {
                    biased;
                    _ = errors.changed() => { signals.check()?; }
                    receipt = &mut self.receiver => { return receipt.map_err(|_| LocalAudioError::Stopped)?; }
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| LocalAudioError::Timeout)?
    }
}

pub(crate) struct OutputQueue {
    writes: VecDeque<Write>,
    bytes: usize,
    format: PcmFormat,
    limits: AudioLimits,
    closed: bool,
}
impl OutputQueue {
    pub(crate) fn new(format: PcmFormat, limits: AudioLimits) -> Self {
        Self {
            writes: VecDeque::with_capacity(limits.queue_capacity),
            bytes: 0,
            format,
            limits,
            closed: false,
        }
    }
    pub(crate) fn enqueue(
        &mut self,
        audio: &AudioRawFrame,
    ) -> Result<PendingSubmission, LocalAudioError> {
        if self.closed {
            return Err(LocalAudioError::Stopped);
        }
        audio
            .validate()
            .map_err(|error| LocalAudioError::InvalidAudio(error.to_string()))?;
        if audio.sample_rate != self.format.sample_rate()
            || audio.num_channels != self.format.channels()
        {
            return Err(LocalAudioError::InvalidAudio(
                "audio rate/channels do not match the open device".into(),
            ));
        }
        if audio.audio.is_empty() {
            return Err(LocalAudioError::InvalidAudio(
                "empty PCM has no callback submission".into(),
            ));
        }
        // Cancelled entries need not wait for a callback to release their budget.
        self.writes.retain(|write| {
            if write.cancelled.load(Ordering::Acquire) {
                self.bytes -= write.audio.len();
                false
            } else {
                true
            }
        });
        if self.writes.len() >= self.limits.queue_capacity
            || audio.audio.len() > self.limits.queue_byte_capacity.saturating_sub(self.bytes)
        {
            return Err(LocalAudioError::QueueFull);
        }
        let (receipt, receiver) = oneshot::channel();
        let cancelled = Arc::new(AtomicBool::new(false));
        self.bytes += audio.audio.len();
        self.writes.push_back(Write {
            audio: audio.audio.clone(),
            offset: 0,
            cancelled: cancelled.clone(),
            receipt,
        });
        Ok(PendingSubmission {
            receiver,
            cancelled,
        })
    }
    pub(crate) fn cancel_all(&mut self, error: LocalAudioError) {
        self.closed = true;
        while let Some(write) = self.writes.pop_front() {
            let _ = write.receipt.send(Err(error.clone()));
        }
        self.bytes = 0;
    }
}

pub(crate) struct OutputCallback {
    pub(crate) queue: Arc<Mutex<OutputQueue>>,
    pub(crate) signals: Arc<Signals>,
    pub(crate) format: PcmFormat,
    pub(crate) max_callback_frames: usize,
}
impl OutputCallback {
    pub(crate) fn render<T>(
        &mut self,
        data: &mut [T],
        callback: cpal::StreamInstant,
        playback: cpal::StreamInstant,
    ) where
        T: cpal::Sample + cpal::FromSample<i16>,
    {
        data.fill(T::EQUILIBRIUM);
        if !self.signals.active.load(Ordering::Acquire) {
            return;
        }
        let channels = usize::from(self.format.channels());
        if !data.len().is_multiple_of(channels) || data.len() / channels > self.max_callback_frames
        {
            self.signals.fail(LocalAudioError::InvalidAudio(
                "malformed or oversized native output callback".into(),
            ));
            return;
        }
        // Device callbacks must not wait on an async producer copying a packet.
        // Contention adds silence while retaining all unsubmitted queued data.
        let mut queue = match self.queue.try_lock() {
            Ok(queue) => queue,
            Err(std::sync::TryLockError::WouldBlock) => return,
            Err(std::sync::TryLockError::Poisoned(_)) => {
                self.signals.fail(LocalAudioError::Device(
                    "native output callback state was poisoned".into(),
                ));
                return;
            }
        };
        if queue.closed {
            return;
        }
        let mut position = 0;
        while position < data.len() {
            if !self.signals.active.load(Ordering::Acquire) {
                return;
            }
            let Some(write) = queue.writes.front_mut() else {
                break;
            };
            if write.cancelled.load(Ordering::Acquire) {
                let write = queue.writes.pop_front().unwrap();
                queue.bytes -= write.audio.len();
                let _ = write.receipt.send(Err(LocalAudioError::Cancelled));
                continue;
            }
            let count = ((write.audio.len() - write.offset) / 2).min(data.len() - position);
            let mut copied = 0;
            for frame in data[position..position + count].chunks_exact_mut(channels) {
                // Keep channels together while bounding concurrent cancellation
                // to one in-flight interleaved frame, rather than a whole period.
                if write.cancelled.load(Ordering::Acquire)
                    || !self.signals.active.load(Ordering::Acquire)
                {
                    break;
                }
                for output in frame {
                    let sample = i16::from_le_bytes([
                        write.audio[write.offset],
                        write.audio[write.offset + 1],
                    ]);
                    *output = T::from_sample(sample);
                    write.offset += 2;
                }
                copied += channels;
            }
            position += copied;
            if !self.signals.active.load(Ordering::Acquire) {
                return;
            }
            if write.cancelled.load(Ordering::Acquire) {
                continue;
            }
            if write.offset == write.audio.len() {
                let write = queue.writes.pop_front().unwrap();
                queue.bytes -= write.audio.len();
                let duration = Duration::from_nanos(
                    ((position / channels) as u128 * 1_000_000_000
                        / u128::from(self.format.sample_rate())) as u64,
                );
                let Some(end) = playback.checked_add(duration) else {
                    let error =
                        LocalAudioError::InvalidAudio("native playback timestamp overflow".into());
                    let _ = write.receipt.send(Err(error.clone()));
                    queue.cancel_all(error.clone());
                    self.signals.fail(error);
                    return;
                };
                let receipt = SubmissionReceipt {
                    frames: write.audio.len() / self.format.frame_bytes(),
                    sample_rate: self.format.sample_rate(),
                    channels: self.format.channels(),
                    callback_time: callback.duration_since(cpal::StreamInstant::ZERO),
                    predicted_playback_end: end.duration_since(cpal::StreamInstant::ZERO),
                };
                let _ = write.receipt.send(Ok(receipt));
            }
        }
    }
}
