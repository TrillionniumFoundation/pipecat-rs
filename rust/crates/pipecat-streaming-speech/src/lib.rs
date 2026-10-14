//! Native Deepgram Listen v1 STT, Cartesia Live/Turns STT and Cartesia streaming TTS.
//!
//! Each session owns bounded input/output queues and its WebSocket. Cancellation
//! bypasses those queues, discards pending events, and performs bounded cleanup.
//! Provider failures are surfaced without automatically replaying audio or text.
//!
//! Cartesia Turns remains committed-only by default. The non-default
//! `cartesia-turns-eager` feature exposes `CartesiaTurnsProcessor::with_eager_turns`;
//! callers must also configure a source-aware eager user aggregator and a
//! supported native LLM processor. Opting in suppresses interim transcripts and
//! raw upstream proposed-speaking events; admitted consumer speaking events
//! remain available. Provider events have no authenticated turn identity.

pub mod cartesia;
pub mod cartesia_stt;
mod cartesia_stt_processor;
mod common;
pub mod deepgram;
#[cfg(feature = "openai-stt")]
pub mod openai_stt;
#[cfg(feature = "openai-stt")]
mod openai_stt_processor;
#[cfg(feature = "openai-stt")]
pub use openai_stt::{
    OpenAiSttClient, OpenAiSttConfig, OpenAiSttDelay, OpenAiSttEvent,
    OpenAiSttNoiseReduction, OpenAiSttSender, OpenAiSttSession,
};
#[cfg(feature = "openai-stt")]
pub use openai_stt_processor::{OpenAiRealtimeSttProcessor, OpenAiSttProcessor};
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

pub mod cartesia_turns;
#[cfg(feature = "cartesia-turns-eager")]
mod cartesia_turns_eager;
mod cartesia_turns_processor;
pub use cartesia_turns::{
    CartesiaTurnsClient, CartesiaTurnsConfig, CartesiaTurnsEvent, CartesiaTurnsSender,
    CartesiaTurnsSession, CartesiaTurnsSettings, CartesiaTurnsWatchdog, CARTESIA_TURNS_VERSION,
};
pub use cartesia_turns_processor::CartesiaTurnsProcessor;
