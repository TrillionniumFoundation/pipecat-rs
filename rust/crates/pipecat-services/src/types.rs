use crate::context::Context;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::fmt;
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    OpenAi,
    Anthropic,
    Gemini,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletionRequest {
    /// Sparse source-compatible settings; given fields override convenience fields.
    #[serde(default)]
    pub settings: pipecat_core::LlmSettings,
    pub model: String,
    pub context: Context,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_instruction: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_tokens: Option<u32>,
    /// Additional provider fields. Reserved protocol fields cannot be replaced.
    #[serde(default)]
    pub extra: Map<String, Value>,
}
impl CompletionRequest {
    pub fn new(model: impl Into<String>, context: Context) -> Self {
        Self {
            settings: pipecat_core::LlmSettings::default(),
            model: model.into(),
            context,
            system_instruction: None,
            temperature: None,
            max_tokens: None,
            extra: Map::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolCall {
    pub id: String,
    pub name: String,
    pub arguments: Value,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub prompt_tokens: u64,
    pub completion_tokens: u64,
    pub total_tokens: u64,
    pub cache_read_input_tokens: Option<u64>,
    pub cache_creation_input_tokens: Option<u64>,
    pub reasoning_tokens: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FinishReason {
    Stop,
    ToolCalls,
    Length,
    ContentFilter,
    Other(String),
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StreamEvent {
    Started,
    /// First accepted provider choice, Gemini candidate, or Anthropic content
    /// block start/delta, including events without visible text or buffered by a
    /// response adapter.
    ResponseProgress,
    /// Explicit provider thought boundaries; independent channels may overlap.
    ThoughtStart,
    ThoughtEnd,
    TextDelta(String),
    ThoughtDelta(String),
    AnthropicThoughtStart,
    AnthropicThoughtEnd,
    /// Closed provider prefix, containing only complete opaque signatures.
    AnthropicCheckpoint(Vec<crate::Message>),
    /// Complete provider-indexed history. Tools are not admitted by this event.
    AnthropicHistory(Vec<crate::Message>),
    GeminiHistory(Vec<crate::Message>),
    GeminiCheckpoint(Vec<crate::Message>),
    /// Partial arguments are untrusted, incomplete JSON until ToolCall arrives.
    ToolCallDelta {
        index: u32,
        id: Option<String>,
        name: Option<String>,
        arguments: String,
    },
    ToolCall(ToolCall),
    /// The latest cumulative usage is emitted once, including on failure/cancel.
    Usage(Usage),
    Finished {
        reason: Option<FinishReason>,
    },
}

#[derive(Debug, thiserror::Error)]
pub enum ServiceError {
    #[error("invalid request: {0}")]
    InvalidRequest(String),
    #[error("unsupported provider feature: {0}")]
    Unsupported(String),
    #[error("HTTP status {status}: {message}")]
    Http {
        status: u16,
        message: String,
        retry_after: Option<String>,
    },
    #[error("provider error ({kind}): {message}")]
    Provider { kind: String, message: String },
    #[error("transport error: {0}")]
    Transport(String),
    #[error("invalid stream: {0}")]
    Protocol(String),
    #[error("completion timed out")]
    Timeout,
    #[error("completion cancelled")]
    Cancelled,
    #[error("stream consumer closed")]
    ConsumerClosed,
}
impl ServiceError {
    pub fn retryable(&self) -> bool {
        match self {
            Self::Http { status, .. } => *status == 408 || *status == 429 || *status >= 500,
            Self::Timeout | Self::Transport(_) => true,
            _ => false,
        }
    }
}

/// Clones share a sticky cancellation state; cancellation cannot be reset.
#[derive(Clone)]
pub struct CancellationToken(watch::Sender<bool>);
impl Default for CancellationToken {
    fn default() -> Self {
        Self::new()
    }
}
impl fmt::Debug for CancellationToken {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CancellationToken")
            .field("cancelled", &self.is_cancelled())
            .finish()
    }
}
impl CancellationToken {
    pub fn new() -> Self {
        let (tx, _) = watch::channel(false);
        Self(tx)
    }
    pub fn cancel(&self) {
        self.0.send_replace(true);
    }
    pub fn is_cancelled(&self) -> bool {
        *self.0.borrow()
    }
    pub async fn cancelled(&self) {
        let mut rx = self.0.subscribe();
        while !*rx.borrow_and_update() {
            if rx.changed().await.is_err() {
                return;
            }
        }
    }
}
