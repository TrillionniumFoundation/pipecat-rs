// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Native Gladia v2 REST-to-WebSocket recognition with bounded, non-replayed audio.
//!
//! # Credential-bearing debug instrumentation
//! Set both `tungstenite` and `tokio_tungstenite` logging targets to WARN or OFF
//! for real sessions. TRACE exposes temporary URL tokens and audio/transcript
//! frames; DEBUG exposes provider close reasons. This crate redacts its own
//! errors and config Debug output, not dependency wire instrumentation. A
//! sanitized dependency logging path remains a production-hardening item.
//! See the crate README for security limits and replay exclusions.
mod client;
mod config;
mod processor;
mod protocol;
mod security;
mod settings;
pub use client::{GladiaSttClient, GladiaSttSender, GladiaSttSession};
pub use config::{GladiaError, GladiaLimits, GladiaRegion, GladiaSttConfig};
pub use pipecat_services::CancellationToken;
pub use processor::GladiaSttProcessor;
pub use protocol::{GladiaAcknowledgment, GladiaSttEvent, GladiaTranscript, GladiaTranslation};
pub use settings::*;
