//! Native Deepgram Listen v1 STT, Cartesia Live STT and Cartesia streaming TTS.
//!
//! Each session owns bounded input/output queues and its WebSocket. Cancellation
//! bypasses those queues, discards pending events, and performs bounded cleanup.
//! Provider failures are surfaced without automatically replaying audio or text.

pub mod cartesia;
pub mod cartesia_stt;
mod cartesia_stt_processor;
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

pub use cartesia_stt::{
    CartesiaKeyterms, CartesiaSttClient, CartesiaSttConfig, CartesiaSttEvent, CartesiaSttSender,
    CartesiaSttSession, CartesiaSttSettings, CartesiaSttTranscript, CARTESIA_STT_VERSION,
};
pub use cartesia_stt_processor::CartesiaSttProcessor;
