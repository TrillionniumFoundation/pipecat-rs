//! Native ElevenLabs realtime STT and single-context WebSocket TTS.
//!
//! Wire contracts follow Pipecat's pinned Python source. Provider multiplexing,
//! dialogue, pronunciation dictionaries and automatic reconnect/replay are not
//! supported. HTTP speech is implemented separately by `pipecat-speech`.
mod common;
mod settings;
pub mod stt;
mod stt_processor;
pub mod tts;
mod tts_processor;
pub use pipecat_services::CancellationToken;
pub use pipecat_streaming_speech::{StreamingError, StreamingLimits, WordTimestamp};
pub use pipecat_text::AggregationType as TextAggregationMode;
pub use stt::{
    CommitStrategy, ElevenLabsSttClient, ElevenLabsSttConfig, ElevenLabsSttEvent,
    ElevenLabsSttSender, ElevenLabsSttSession, ElevenLabsTranscript,
};
pub use stt_processor::ElevenLabsSttProcessor;
pub use tts::{
    ElevenLabsTtsClient, ElevenLabsTtsConfig, ElevenLabsTtsEvent, ElevenLabsTtsSender,
    ElevenLabsTtsStream, VoiceSettings,
};
pub use tts_processor::ElevenLabsTtsProcessor;
