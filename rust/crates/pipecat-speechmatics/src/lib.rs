// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Native Speechmatics Agent STT. See SOURCE_MAP.md for compatibility limits.
mod client;
mod common;
mod processor;
mod protocol;
mod settings;
pub use client::*;
pub use common::{StreamingError, StreamingLimits};
pub use pipecat_services::CancellationToken;
pub use processor::SpeechmaticsSttProcessor;
pub use protocol::*;
pub use settings::*;
