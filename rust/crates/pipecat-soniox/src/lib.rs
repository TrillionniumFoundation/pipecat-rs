// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Native Soniox streaming recognition and multiplexed text-to-speech.
//!
//! A TTS client and its clones share one socket, with independently bounded,
//! cancellable streams. Accepted paid work is never automatically replayed.
mod common;
mod settings;
mod stt;
mod stt_processor;
mod tts;
mod tts_processor;
pub use common::{StreamingError, StreamingLimits};
pub use pipecat_services::CancellationToken;
pub use pipecat_text::AggregationType as TextAggregationMode;
pub use settings::{
    SonioxContext, SonioxContextItem, SonioxContextObject, SonioxSttSettings,
    SonioxTranslationTerm, SonioxTtsSettings,
};
pub use stt::{
    SonioxSttClient, SonioxSttConfig, SonioxSttEvent, SonioxSttSender, SonioxSttSession,
    SonioxTranscript,
};
pub use stt_processor::SonioxSttProcessor;
pub use tts::{
    SonioxTtsClient, SonioxTtsConfig, SonioxTtsEvent, SonioxTtsSender, SonioxTtsStream,
    WordTimestamp,
};
pub use tts_processor::SonioxTtsProcessor;
