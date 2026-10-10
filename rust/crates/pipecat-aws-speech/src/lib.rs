//! Native AWS Polly and Transcribe speech providers with explicit credentials.
//! Authentication never consults ambient AWS credentials or metadata endpoints.
mod auth;
mod common;
mod errors;
pub mod eventstream;
mod language;
pub mod polly;
mod settings;
mod stt_processor;
mod text;
pub mod transcribe;
mod tts_processor;
pub use auth::{AwsCredentials, PresignedUrl};
pub use language::{polly_language, transcribe_language};
pub use pipecat_services::CancellationToken;
pub use pipecat_speech::{SpeechChunk, SpeechError};
pub use pipecat_streaming_speech::{StreamingError, StreamingLimits};
pub use polly::{PollyClient, PollyConfig, PollySettings, PollyStream};
pub use stt_processor::{ReconnectPolicy, TranscribeSttProcessor};
pub use transcribe::{
    PartialResultsStability, TranscribeClient, TranscribeConfig, TranscribeEvent, TranscribeSender,
    TranscribeSession, TranscribeTranscript,
};
pub use tts_processor::PollyTtsProcessor;
