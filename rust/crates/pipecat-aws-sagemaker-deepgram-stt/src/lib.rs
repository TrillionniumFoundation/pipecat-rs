//! Finite Deepgram PCM16 transcription over SageMaker HTTP/2 EventStream.
//! Explicit credentials, no credential discovery, reconnect, replay or microphone claim.
mod app;
mod config;
mod language;
mod processor;
mod protocol;
mod transport;
pub use app::{Completion, Report, RunningApp, SageMakerSttApp, Terminal};
pub use config::{
    RunManifest, SageMakerSttConfig, SttLimits, Timeline, TimelineMarker, TranscriptionSettings,
};
pub use processor::{Progress, SageMakerSttProcessor};
pub use transport::{TransportError, TransportLimits as DuplexLimits, TransportReport};

/// Static redacted error categories; peer text and credentials never become diagnostics.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum Error {
    #[error("invalid finite STT configuration/input: {0}")]
    Invalid(&'static str),
    #[error("finite STT budget exceeded")]
    TooLarge,
    #[error("invalid SageMaker STT envelope")]
    Envelope,
    #[error("SageMaker STT service reported failure")]
    Provider,
    #[error("local STT cancelled; remote processing unknown")]
    Cancelled,
    #[error("STT output failed or exceeded deadline")]
    Output,
    #[error("STT owned task failed; join proof may be incomplete")]
    Task,
    #[error(transparent)]
    Transport(#[from] TransportError),
}
