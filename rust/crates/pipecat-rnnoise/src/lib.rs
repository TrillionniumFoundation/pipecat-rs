//! In-process RNNoise with an explicitly supplied native library and bundled model.
//! No downloads, Python, fallback passthrough, or implicit resampling.
#![deny(unsafe_code)]
mod filter;
#[cfg(feature = "native")]
#[allow(unsafe_code)]
mod native;
mod stream;

pub use filter::RnNoiseFilter;
use std::path::PathBuf;

pub const SAMPLE_RATE: u32 = 48_000;
pub const FRAME_SAMPLES: usize = 480;

/// Resampling is an explicit compatibility choice, independent of RNNoise itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RateConversion {
    /// Require 48 kHz input; unsupported formats fail during start.
    #[default]
    Require48Khz,
    /// Native windowed-sinc conversion for mono 8–96 kHz input. This is not SOXR
    /// QQ numerical parity, and has no inactivity-based history clearing.
    NativeSinc,
}

/// Identity of trusted native code, including its built-in RNNoise model.
///
/// A hash verifies identity, not trust or ABI safety. The caller must trust the
/// library and its dependencies and keep the path immutable through filter use.
/// Hashing followed by loading has a filesystem race if another actor replaces it.
/// Only the built-in model (`rnnoise_create(NULL)`) is supported.
#[derive(Clone, Debug)]
pub struct RnNoiseConfig {
    pub library_path: PathBuf,
    pub library_sha256: String,
    pub rate_conversion: RateConversion,
    /// Per-call byte budget, including disabled passthrough. Maximum 192,000.
    pub max_input_bytes: usize,
}
impl RnNoiseConfig {
    pub fn new(library_path: impl Into<PathBuf>, library_sha256: impl Into<String>) -> Self {
        Self {
            library_path: library_path.into(),
            library_sha256: library_sha256.into(),
            rate_conversion: RateConversion::Require48Khz,
            max_input_bytes: 96_000,
        }
    }
    fn validate(&self) -> Result<()> {
        if !self.library_path.is_absolute() {
            return Err(RnNoiseError::Invalid("library path must be absolute"));
        }
        if self.library_sha256.len() != 64
            || !self.library_sha256.bytes().all(|b| b.is_ascii_hexdigit())
        {
            return Err(RnNoiseError::Invalid(
                "library SHA-256 must contain 64 hexadecimal digits",
            ));
        }
        if self.max_input_bytes == 0 || self.max_input_bytes > 192_000 {
            return Err(RnNoiseError::Invalid(
                "max_input_bytes must be within 1..=192000",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, thiserror::Error)]
pub enum RnNoiseError {
    #[error("invalid RNNoise configuration: {0}")]
    Invalid(&'static str),
    #[error("RNNoise native feature is disabled")]
    NativeDisabled,
    #[error("RNNoise only accepts mono PCM16LE")]
    Channels,
    #[error("RNNoise input rate requires explicit supported rate conversion")]
    SampleRate,
    #[error("RNNoise library IO failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("RNNoise library SHA-256 mismatch")]
    HashMismatch,
    #[error("RNNoise native library failed: {0}")]
    Native(String),
    #[error("RNNoise stream is not started; start is required after cancellation")]
    NotStarted,
    #[error("RNNoise input exceeds the configured byte budget")]
    TooLarge,
    #[error("RNNoise already has an admitted request or uncollected reply")]
    Backpressure,
    #[error("RNNoise lifecycle operation is already in progress")]
    LifecycleBusy,
    #[error("RNNoise request cancelled")]
    Cancelled,
    #[error("RNNoise worker is closed or failed")]
    Worker,
    #[error("invalid PCM: {0}")]
    Audio(#[from] pipecat_audio::AudioError),
}
pub type Result<T> = std::result::Result<T, RnNoiseError>;

trait Engine {
    fn process(&mut self, samples: &[i16; FRAME_SAMPLES]) -> Result<[i16; FRAME_SAMPLES]>;
}

fn create_engine(config: &RnNoiseConfig) -> Result<Box<dyn Engine>> {
    #[cfg(feature = "native")]
    {
        Ok(Box::new(native::NativeEngine::load(config)?))
    }
    #[cfg(not(feature = "native"))]
    {
        let _ = config;
        Err(RnNoiseError::NativeDisabled)
    }
}
