//! Native Deepgram Listen v1 STT and Cartesia streaming TTS.
//!
//! Each session owns bounded input/output queues and its WebSocket. Cancellation
//! bypasses those queues, discards pending events, and performs bounded cleanup.
//! Provider failures are surfaced without automatically replaying audio or text.

pub mod cartesia;
mod common;
pub mod deepgram;
pub mod processor;
mod settings;
mod stt_processor;

pub use cartesia::{
    CartesiaClient, CartesiaConfig, CartesiaEvent, CartesiaSender, CartesiaStream, WordTimestamp,
};
pub use common::{StreamingError, StreamingLimits};
pub use deepgram::{
    DeepgramClient, DeepgramConfig, DeepgramEvent, DeepgramSender, DeepgramSession,
    DeepgramTranscript, Endpointing,
};
pub use pipecat_services::CancellationToken;
pub use pipecat_text::AggregationType as TextAggregationMode;
pub use processor::CartesiaTtsProcessor;
pub use stt_processor::{DeepgramSttProcessor, ReconnectPolicy};
