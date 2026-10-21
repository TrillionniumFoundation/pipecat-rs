//! Finite native NVIDIA Magpie HTTP TTS via signed SageMaker InvokeEndpoint.
//! Explicit credentials and caller-resolved HTTPS, owned HTTP1, raw mono PCM16.
//! This is not the source's bidirectional voice bot, playback, or spoken history.
mod app;
mod config;
mod processor;
mod protocol;
mod transport;
mod unicode15;
pub use app::{SageMakerCompletion, SageMakerReport, SageMakerRunningApp, SageMakerTtsApp};
pub use config::{SageMakerConfig, SageMakerLimits, SynthesisSettings, Utterance};
pub use processor::{SageMakerClient, SageMakerProcessor, SageMakerProgress};

/// Bounded redacted categories. Never contains credentials, text or peer bodies.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid SageMaker configuration/input: {0}")]
    Invalid(&'static str),
    #[error("unsupported SageMaker profile: {0}")]
    Unsupported(&'static str),
    #[error("SageMaker bounded budget exceeded")]
    TooLarge,
    #[error("invalid SageMaker protocol: {0}")]
    Protocol(&'static str),
    #[error("SageMaker HTTP status {0}")]
    HttpStatus(u16),
    #[error("SageMaker local observation cancelled; remote status unknown")]
    Cancelled,
    #[error("SageMaker deadline elapsed: {0}; remote status unknown")]
    Timeout(&'static str),
    #[error("SageMaker transport failed; remote status unknown")]
    Io,
    #[error("SageMaker caller output failed")]
    Output,
    #[error("SageMaker owned task failed")]
    Task,
}
