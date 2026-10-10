// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Bounded native Gradium WebSocket STT and provider-driven user turn proposals.
//!
//! Disable `tungstenite`/`tokio_tungstenite` TRACE and DEBUG logging in real
//! sessions: dependency instrumentation can expose API keys, transcripts,
//! audio and close reasons. This crate redacts its own Debug/errors only.
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
