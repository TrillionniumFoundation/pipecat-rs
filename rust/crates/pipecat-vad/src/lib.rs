//! Native CPU ONNX voice activity detection. Runtime and model locations are
//! explicit; loading and inference never download assets or invoke Python.
mod analyzer;
mod model;
mod processor;
mod runtime;
mod volume;
pub use processor::{ProcessorOptions, SileroVadProcessor};
pub mod smart_turn;
pub mod smart_turn_analyzer;
pub use analyzer::{SileroVadAnalyzer, VadAnalysis, VadState, VadStateMachine};
pub use model::SileroModel;
pub use pipecat_core::VadParams;
pub use runtime::RuntimeOptions;
pub use smart_turn::{SmartTurnModel, SmartTurnPrediction, WhisperFeatureExtractor};
pub use smart_turn_analyzer::{
    EndOfTurnState, MonotonicSmartTurnClock, PendingSmartTurnAnalysis, SmartTurnAnalysis,
    SmartTurnAnalyzer, SmartTurnBuffer, SmartTurnCancellation, SmartTurnClock,
    SmartTurnCompletionReason, SmartTurnMetrics, SmartTurnParams, SmartTurnPredictor,
};
pub use volume::AudioVolumeTracker;

#[derive(Debug, thiserror::Error)]
pub enum VadError {
    #[error("Smart Turn analysis timed out")]
    SmartTurnTimeout,
    #[error("VAD analysis cancelled")]
    Cancelled,
    #[error("ONNX Runtime: {0}")]
    Runtime(String),
    #[error("model: {0}")]
    Model(String),
    #[error("Silero requires 8000 or 16000 Hz mono PCM, got {0} Hz")]
    SampleRate(u32),
    #[error("expected {expected} samples, got {actual}")]
    ChunkSize { expected: usize, actual: usize },
    #[error("{0}")]
    InvalidParameter(&'static str),
    #[error("{0}")]
    Inference(#[from] ort::Error),
}
pub type Result<T> = std::result::Result<T, VadError>;

#[cfg(feature = "http-smart-turn")]
pub mod http_smart_turn;
#[cfg(feature = "http-smart-turn")]
pub use http_smart_turn::{HttpSmartTurnConfig, HttpSmartTurnPredictor};
