use crate::{
    device, guard_callback, lock, native_error, AudioLimits, DeviceDirection, DeviceSelector,
    LocalAudioError, Signals,
};
use async_trait::async_trait;
use cpal::traits::{DeviceTrait, StreamTrait};
use pipecat_audio::PcmFormat;
use pipecat_core::{AudioRawFrame, Frame, FrameData, ProcessorError};
use pipecat_media_transport::{InputBackend, InputIngress};
use std::sync::{atomic::Ordering, Arc, Mutex};
use tokio::sync::mpsc;

pub struct LocalAudioInput {
    selector: DeviceSelector,
    limits: AudioLimits,
    run: Mutex<Option<Arc<InputRun>>>,
}
struct InputRun {
    stream: Mutex<Option<cpal::Stream>>,
    receiver: Mutex<Option<mpsc::Receiver<Vec<u8>>>>,
    signals: Arc<Signals>,
    format: PcmFormat,
}
impl InputRun {
    fn close(&self) {
        self.signals.fail(LocalAudioError::Stopped);
        // CPAL owns and joins its platform callback worker on stream destruction.
        lock(&self.stream).take();
    }
}
impl Drop for InputRun {
    fn drop(&mut self) {
        self.close();
    }
}
struct CaptureOwner(Arc<InputRun>);
impl Drop for CaptureOwner {
    fn drop(&mut self) {
        self.0.close();
    }
}

impl LocalAudioInput {
    pub fn new(selector: DeviceSelector, limits: AudioLimits) -> Result<Self, LocalAudioError> {
        limits.validate()?;
        Ok(Self {
            selector,
            limits,
            run: Mutex::new(None),
        })
    }
    /// Open the exact rate/channel format. Capture starts when the pipeline
    /// invokes `InputBackend::capture`, so delayed streaming does not record.
    pub fn open(&self, format: PcmFormat) -> Result<(), LocalAudioError> {
        let mut slot = lock(&self.run);
        if slot.is_some() {
            return Err(LocalAudioError::AlreadyStarted);
        }
        let signals = Signals::new();
        let (capture, receiver) = CaptureCallback::new(format, &self.limits, signals.clone())?;
        let device = device::select(&self.selector, DeviceDirection::Input)?;
        let (config, sample) = device::config(&device, DeviceDirection::Input, format)?;
        macro_rules! build {
            ($ty:ty) => {
                build::<$ty>(
                    &device,
                    config,
                    capture,
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
        *slot = Some(Arc::new(InputRun {
            stream: Mutex::new(Some(stream)),
            receiver: Mutex::new(Some(receiver)),
            signals,
            format,
        }));
        Ok(())
    }
    /// Start one owned capture stream. Dropping the returned owner closes and
    /// joins its native stream, including when `next_frame` is cancelled.
    pub fn capture_stream(&self) -> Result<LocalAudioCapture, LocalAudioError> {
        let run = lock(&self.run).clone().ok_or(LocalAudioError::Stopped)?;
        run.signals.check()?;
        let receiver = lock(&run.receiver)
            .take()
            .ok_or(LocalAudioError::CaptureOwned)?;
        let owner = CaptureOwner(run.clone());
        {
            let stream = lock(&run.stream);
            stream
                .as_ref()
                .ok_or(LocalAudioError::Stopped)?
                .play()
                .map_err(native_error)?;
        }
        let errors = run.signals.error.subscribe();
        Ok(LocalAudioCapture {
            owner,
            receiver,
            errors,
        })
    }
    pub fn close(&self) {
        if let Some(run) = lock(&self.run).take() {
            run.close();
        }
    }
    pub fn is_open(&self) -> bool {
        lock(&self.run)
            .as_ref()
            .is_some_and(|run| run.signals.check().is_ok())
    }
}
impl Drop for LocalAudioInput {
    fn drop(&mut self) {
        self.close();
    }
}

/// Single-consumer capture ownership. `next_frame` is cancellation safe: a
/// cancelled read does not duplicate a chunk. Drop the owner to stop capture.
pub struct LocalAudioCapture {
    owner: CaptureOwner,
    receiver: mpsc::Receiver<Vec<u8>>,
    errors: tokio::sync::watch::Receiver<Option<LocalAudioError>>,
}
impl LocalAudioCapture {
    pub async fn next_frame(&mut self) -> Result<Frame, LocalAudioError> {
        loop {
            self.owner.0.signals.check()?;
            tokio::select! {
                biased;
                _ = self.errors.changed() => { self.owner.0.signals.check()?; }
                audio = self.receiver.recv() => {
                    let audio = audio.ok_or(LocalAudioError::Stopped)?;
                    self.owner.0.signals.check()?;
                    let format = self.owner.0.format;
                    let audio = AudioRawFrame::new(audio, format.sample_rate(), format.channels())
                        .map_err(|error| LocalAudioError::InvalidAudio(error.to_string()))?;
                    return Ok(Frame::new(FrameData::InputAudio(audio)));
                }
            }
        }
    }
}

#[async_trait]
impl InputBackend for LocalAudioInput {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        self.open(format).map_err(Into::into)
    }
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError> {
        let mut capture = self.capture_stream()?;
        loop {
            ingress.push(capture.next_frame().await?)?;
        }
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.close();
        Ok(())
    }
}

fn build<T>(
    device: &cpal::Device,
    config: cpal::StreamConfig,
    mut callback: CaptureCallback,
    signals: Arc<Signals>,
    timeout: std::time::Duration,
) -> Result<cpal::Stream, LocalAudioError>
where
    T: cpal::SizedSample,
    f64: cpal::FromSample<T>,
{
    let callback_signals = signals.clone();
    device
        .build_input_stream(
            config,
            move |data: &[T], _| {
                guard_callback(&callback_signals, || callback.receive(data));
            },
            move |error| {
                guard_callback(&signals, || signals.fail(native_error(error)));
            },
            Some(timeout),
        )
        .map_err(native_error)
}

/// The native callback and tests use exactly this bounded reblock/conversion path.
pub(crate) struct CaptureCallback {
    format: PcmFormat,
    chunk_bytes: usize,
    partial: Vec<u8>,
    sender: mpsc::Sender<Vec<u8>>,
    signals: Arc<Signals>,
    max_callback_frames: usize,
}
impl CaptureCallback {
    pub(crate) fn new(
        format: PcmFormat,
        limits: &AudioLimits,
        signals: Arc<Signals>,
    ) -> Result<(Self, mpsc::Receiver<Vec<u8>>), LocalAudioError> {
        // Match the source transport's 20-ms floor-to-10-ms-period sizing.
        let frames = (format.sample_rate() as usize / 100)
            .checked_mul(2)
            .ok_or_else(|| LocalAudioError::InvalidSettings("capture period overflow".into()))?;
        let chunk_bytes = frames
            .checked_mul(format.frame_bytes())
            .filter(|bytes| *bytes > 0)
            .ok_or_else(|| {
                LocalAudioError::InvalidSettings("capture requires at least 100 Hz".into())
            })?;
        // Reserve one partial callback chunk and one receiver-held chunk.
        let capacity = (limits.queue_byte_capacity / chunk_bytes)
            .saturating_sub(2)
            .min(limits.queue_capacity);
        if capacity == 0 {
            return Err(LocalAudioError::InvalidSettings(
                "capture byte budget must fit three 20-ms chunks".into(),
            ));
        }
        let (sender, receiver) = mpsc::channel(capacity);
        Ok((
            Self {
                format,
                chunk_bytes,
                partial: Vec::with_capacity(chunk_bytes),
                sender,
                signals,
                max_callback_frames: limits.max_callback_frames,
            },
            receiver,
        ))
    }
    pub(crate) fn receive<T>(&mut self, data: &[T])
    where
        T: cpal::Sample,
        f64: cpal::FromSample<T>,
    {
        if !self.signals.active.load(Ordering::Acquire) {
            return;
        }
        if !data
            .len()
            .is_multiple_of(usize::from(self.format.channels()))
            || data.len() / usize::from(self.format.channels()) > self.max_callback_frames
        {
            self.signals.fail(LocalAudioError::InvalidAudio(
                "malformed or oversized native capture callback".into(),
            ));
            return;
        }
        for sample in data {
            let value: f64 = sample.to_sample();
            if !value.is_finite() {
                self.signals.fail(LocalAudioError::InvalidAudio(
                    "non-finite native capture sample".into(),
                ));
                return;
            }
            let sample = (value.clamp(-1.0, 1.0) * 32768.0).clamp(-32768.0, 32767.0) as i16;
            self.partial.extend_from_slice(&sample.to_le_bytes());
            if self.partial.len() == self.chunk_bytes {
                let chunk = std::mem::take(&mut self.partial);
                match self.sender.try_send(chunk) {
                    Ok(()) => {
                        self.partial = Vec::with_capacity(self.chunk_bytes);
                    }
                    Err(mpsc::error::TrySendError::Full(_)) => {
                        self.signals.fail(LocalAudioError::QueueFull);
                        return;
                    }
                    Err(mpsc::error::TrySendError::Closed(_)) => {
                        self.signals.fail(LocalAudioError::Stopped);
                        return;
                    }
                }
            }
        }
    }
}
