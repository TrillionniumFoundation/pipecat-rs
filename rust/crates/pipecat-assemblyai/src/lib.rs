// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Native AssemblyAI streaming and segmented Sync recognition.
//! No provider SDK or Python runtime. Credentials are explicit configuration.
mod common;
mod processor;
mod settings;
mod streaming;
pub mod sync;
pub use common::{StreamingError, StreamingLimits};
pub use pipecat_services::CancellationToken;
pub use processor::AssemblyAiSttProcessor;
pub use settings::{is_u3_pro_model, prepare_language_codes, StreamingSettings};
pub use streaming::{
    AssemblyAiClient, AssemblyAiConfig, AssemblyAiEvent, AssemblyAiSender, AssemblyAiSession,
    Encoding, Transcript, UpdateOutcome,
};
