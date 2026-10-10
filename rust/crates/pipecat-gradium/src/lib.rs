// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Bounded native Gradium WebSocket STT, TTS and provider-driven user turns.
//!
//! Disable `tungstenite`/`tokio_tungstenite` TRACE and DEBUG logging in real
//! sessions: dependency instrumentation can expose API keys, transcripts,
//! audio and close reasons. This crate redacts configuration Debug/errors only.
mod client;
mod config;
mod processor;
mod protocol;
mod security;
pub use client::{GradiumStatistics, GradiumSttClient, GradiumSttSender, GradiumSttSession};
pub use config::{
    GradiumError, GradiumLimits, GradiumSttConfig, GradiumSttSettings, GradiumTurnSettings,
};
pub use pipecat_services::CancellationToken;
pub use processor::GradiumSttProcessor;
pub use protocol::{
    GradiumSttEvent, GradiumTranscript, GradiumTurnPhase, TRANSCRIPT_AGGREGATION_DELAY,
};

mod tts;
mod tts_processor;
mod tts_settings;
pub use tts::{
    GradiumTtsClient, GradiumTtsConfig, GradiumTtsEvent, GradiumTtsSender, GradiumTtsStream,
    GradiumWordTimestamp,
};
pub use tts_processor::GradiumTtsProcessor;
pub use tts_settings::GradiumTtsSettings;
