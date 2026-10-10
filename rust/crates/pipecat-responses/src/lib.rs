// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Native OpenAI Responses HTTP/SSE and WebSocket clients.
//!
//! Requests never read credentials from the environment. All channels, wire
//! messages, response accumulation and waits are bounded. Ambiguous remote work
//! is never replayed automatically; only explicit WebSocket rejection is retried.

mod context;
mod http;
mod protocol;
mod settings;
mod stream;
mod websocket;

pub use context::{InputContent, InputItem, InputPart, ReasoningItem, ResponsesContext};
pub use http::ResponsesHttpClient;
pub use pipecat_services::{
    CancellationToken, ServiceError, ToolCall, ToolChoice, ToolDefinition, Usage,
};
pub use protocol::{ResponseEvent, ResponseStatus};
pub use settings::{
    default_reasoning_effort, model_supports_reasoning, model_supports_response_schema,
    ReasoningConfig, ResponsesRequest, ResponsesSettings,
};
pub use stream::{ResponsesConfig, ResponsesStream};
pub use websocket::ResponsesWebSocketClient;
mod processor;
pub use processor::{ResponsesClient, ResponsesProcessor};
