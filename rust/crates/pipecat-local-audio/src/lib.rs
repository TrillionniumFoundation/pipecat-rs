//! Native local-device audio using CPAL, without a Python or PortAudio runtime.
//!
//! Capture implements the shared media input contract. Output deliberately uses
//! [`SubmissionReceipt`], not a `PlaybackReceipt`: CPAL callbacks precede native
//! writes and cannot establish audible completion. It therefore cannot be bound
//! to the media transport's receipt-backed spoken-history ledger.
#![doc = include_str!("../README.md")]

mod capture;
mod device;
mod playback;

pub use capture::{LocalAudioCapture, LocalAudioInput};
pub use device::{list_devices, AudioDevice, DeviceDirection, DeviceSelector};
pub use playback::{LocalAudioOutput, SubmissionReceipt};

use pipecat_media_transport::{InputTransport, InputTransportConfig};
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::watch;

/// Local CPAL indices refer to this host's all-device enumeration, not PyAudio
/// indices. Stable native IDs are preferred when persisting a selection.
#[derive(Debug, Clone)]
pub struct LocalAudioTransportParams {
    pub input_device: DeviceSelector,
    pub output_device: DeviceSelector,
    pub input: InputTransportConfig,
    pub limits: AudioLimits,
}
impl Default for LocalAudioTransportParams {
    fn default() -> Self {
        Self {
            input_device: DeviceSelector::Default,
            output_device: DeviceSelector::Default,
            input: InputTransportConfig::default(),
            limits: AudioLimits::default(),
        }
    }
}

/// Bounds include in-flight partial output, not just waiting queue entries.
#[derive(Debug, Clone)]
pub struct AudioLimits {
    pub queue_capacity: usize,
    pub queue_byte_capacity: usize,
    pub max_callback_frames: usize,
    pub operation_timeout: Duration,
}
impl Default for AudioLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 32,
            queue_byte_capacity: 1024 * 1024,
            max_callback_frames: 65_536,
            operation_timeout: Duration::from_secs(5),
        }
    }
}
impl AudioLimits {
    fn validate(&self) -> Result<(), LocalAudioError> {
        if self.queue_capacity == 0
            || self.queue_capacity > 65_536
            || self.queue_byte_capacity < 2
            || self.queue_byte_capacity > 64 * 1024 * 1024
            || self.max_callback_frames == 0
            || self.max_callback_frames > 1_048_576
            || self.operation_timeout.is_zero()
        {
            return Err(LocalAudioError::InvalidSettings(
                "invalid local audio buffer limits".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LocalAudioError {
    #[error("invalid local audio settings: {0}")]
    InvalidSettings(String),
    #[error("no {0:?} audio device matched the selection")]
    DeviceNotFound(DeviceDirection),
    #[error("native audio operation failed: {0}")]
    Device(String),
    #[error("device does not support {rate} Hz, {channels} channels with a supported native sample type")]
    UnsupportedFormat { rate: u32, channels: u16 },
    #[error("local audio is already started")]
    AlreadyStarted,
    #[error("local audio is not started or has stopped")]
    Stopped,
    #[error("local audio capture already has an owner")]
    CaptureOwned,
    #[error("local audio queue is full")]
    QueueFull,
    #[error("invalid audio data: {0}")]
    InvalidAudio(String),
    #[error("local audio callback submission timed out; device playback is unconfirmed")]
    Timeout,
    #[error("local audio submission was cancelled")]
    Cancelled,
}
impl From<LocalAudioError> for pipecat_core::ProcessorError {
    fn from(error: LocalAudioError) -> Self {
        Self::new(error.to_string())
    }
}

/// Opening either endpoint is explicit. The input endpoint is a pipeline
/// processor; output is the lower-level, honestly qualified device-write API.
pub struct LocalAudioTransport {
    input: Arc<InputTransport>,
    input_backend: Arc<LocalAudioInput>,
    output: Arc<LocalAudioOutput>,
}
impl LocalAudioTransport {
    pub fn new(params: LocalAudioTransportParams) -> Result<Self, pipecat_core::ProcessorError> {
        params.limits.validate()?;
        if params.input.video_enabled {
            return Err(LocalAudioError::InvalidSettings(
                "local audio does not capture video".into(),
            )
            .into());
        }
        let input_backend = Arc::new(LocalAudioInput::new(
            params.input_device,
            params.limits.clone(),
        )?);
        let input = Arc::new(InputTransport::new(
            "local-audio-input",
            params.input,
            input_backend.clone(),
            None,
        )?);
        let output = Arc::new(LocalAudioOutput::new(params.output_device, params.limits)?);
        Ok(Self {
            input,
            input_backend,
            output,
        })
    }
    pub fn input(&self) -> Arc<InputTransport> {
        self.input.clone()
    }
    pub fn input_backend(&self) -> Arc<LocalAudioInput> {
        self.input_backend.clone()
    }
    pub fn output(&self) -> Arc<LocalAudioOutput> {
        self.output.clone()
    }
}

struct Signals {
    active: AtomicBool,
    error: watch::Sender<Option<LocalAudioError>>,
}
impl Signals {
    fn new() -> Arc<Self> {
        let (error, _) = watch::channel(None);
        Arc::new(Self {
            active: AtomicBool::new(true),
            error,
        })
    }
    fn fail(&self, error: LocalAudioError) {
        self.error.send_if_modified(|slot| {
            if slot.is_some() {
                false
            } else {
                *slot = Some(error);
                true
            }
        });
        self.active.store(false, Ordering::Release);
    }
    fn check(&self) -> Result<(), LocalAudioError> {
        if let Some(error) = self.error.borrow().clone() {
            return Err(error);
        }
        if self.active.load(Ordering::Acquire) {
            Ok(())
        } else {
            Err(LocalAudioError::Stopped)
        }
    }
}

fn native_error(error: impl std::fmt::Display) -> LocalAudioError {
    LocalAudioError::Device(error.to_string())
}

#[cfg(test)]
mod tests;

/// Never unwind through a platform callback. Any panic invalidates the stream.
fn guard_callback(signals: &Signals, callback: impl FnOnce()) -> bool {
    if std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).is_err() {
        signals.fail(LocalAudioError::Device(
            "native audio callback panicked".into(),
        ));
        false
    } else {
        true
    }
}

fn lock<T>(mutex: &std::sync::Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    // A callback panic invalidates its stream before shutdown recovers the lock.
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}
