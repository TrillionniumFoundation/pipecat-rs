//! Finite multi-utterance NVIDIA Magpie synthesis over signed SageMaker H2 BiDi.
mod app;
mod config;
mod processor;
mod protocol;
mod transport;
mod unicode15;
pub use app::{Completion, Report, RunningApp, SageMakerBidiTtsApp, Terminal, UtteranceReport};
pub use config::{SageMakerBidiTtsConfig, SynthesisSettings, TtsLimits};
pub use processor::{Progress, SageMakerBidiTtsProcessor};
pub use transport::{TransportError, TransportLimits as DuplexLimits, TransportReport};
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid finite BiDi TTS configuration/input: {0}")]
    Invalid(&'static str),
    #[error("finite BiDi TTS budget exceeded")]
    TooLarge,
    #[error("invalid SageMaker BiDi TTS envelope or PCM samples")]
    Envelope,
    #[error("SageMaker BiDi TTS service reported failure")]
    Provider,
    #[error("local TTS cancelled; remote processing unknown")]
    Cancelled,
    #[error("TTS output failed or exceeded deadline")]
    Output,
    #[error("TTS owned task failed; join proof may be incomplete")]
    Task,
    #[error("TTS response ended without speech completion")]
    Incomplete,
    #[error("TTS absolute application deadline elapsed")]
    Deadline,
    #[error(transparent)]
    Transport(#[from] TransportError),
}
