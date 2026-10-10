//! Native HTTP/SSE LLM clients with bounded streaming and explicit cancellation.
//!
//! These clients implement OpenAI Chat Completions, Anthropic Messages, and
//! Gemini streamGenerateContent wire protocols. They do not execute returned
//! tool calls or claim support for realtime APIs. Retries are explicit opt-in.

pub mod adapters;
pub mod azure;
pub use azure::{
    AzureApiSurface, AzureAuth, AzureConfig, AzureTokenProvider, AZURE_DATED_API_VERSION,
};
pub mod client;
pub mod context;
pub mod frame_context;
pub mod presets;
pub mod protocol;
pub mod settings;
pub mod sse;
pub mod tts_health;
pub mod types;
pub use tts_health::TtsAudioWatchdog;

pub use client::{ClientConfig, LlmClient, LlmStream, RetryPolicy};
pub use context::{Content, ContentPart, Context, Message, Role, ToolChoice, ToolDefinition};
pub use types::{
    CancellationToken, CompletionRequest, FinishReason, Provider, ServiceError, StreamEvent,
    ToolCall, Usage,
};

pub mod processor;
mod speculation;
pub use pipecat_core::{LlmSettings, ServiceSettings, Setting};
pub use processor::{InferenceOptions, LlmInference, LlmProcessor};
pub use speculation::{SpeculationLimits, SpeculationSnapshot};
