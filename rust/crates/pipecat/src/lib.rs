//! Native Pipecat components. See each crate's compatibility and validation scope.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
#[cfg(feature = "agent-workers")]
pub use pipecat_agent_workers as agent_workers;
#[cfg(feature = "assemblyai")]
pub use pipecat_assemblyai as assemblyai;
pub use pipecat_audio as audio;
#[cfg(feature = "client-server")]
pub use pipecat_client_server as client_server;
pub use pipecat_core as core;
#[cfg(feature = "elevenlabs")]
pub use pipecat_elevenlabs as elevenlabs;
#[cfg(feature = "evals")]
pub use pipecat_evals as evals;
pub use pipecat_flows as flows;
#[cfg(feature = "gemini-live")]
pub use pipecat_gemini_live as gemini_live;
#[cfg(feature = "livekit")]
pub use pipecat_livekit as livekit;
#[cfg(feature = "local-audio")]
pub use pipecat_local_audio as local_audio;
#[cfg(feature = "mcp")]
pub use pipecat_mcp as mcp;
pub use pipecat_media_transport as media_transport;
pub use pipecat_observers as observers;
pub use pipecat_protocol as protocol;
pub use pipecat_services as services;
pub use pipecat_transports as transports;
#[cfg(feature = "whisper")]
pub use pipecat_whisper as whisper;
#[cfg(feature = "worker-proxy")]
pub use pipecat_worker_proxy as worker_proxy;
pub use pipecat_workers as workers;
pub mod wire;

pub use pipecat_network_bus as network_bus;
pub use pipecat_pipeline as pipeline;
pub use pipecat_processors as processors;
#[cfg(feature = "realtime")]
pub use pipecat_realtime as realtime;
#[cfg(feature = "responses")]
pub use pipecat_responses as responses;
pub use pipecat_rtvi as rtvi;
pub use pipecat_speech as speech;
pub use pipecat_streaming_speech as streaming_speech;
pub use pipecat_text as text;
pub use pipecat_turns as turns;
pub use pipecat_vad as vad;
#[cfg(feature = "webrtc")]
pub use pipecat_webrtc as webrtc;
