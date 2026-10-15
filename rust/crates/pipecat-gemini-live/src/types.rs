use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{fmt, time::Duration};

pub const DEFAULT_ENDPOINT: &str = "wss://generativelanguage.googleapis.com/ws/google.ai.generativelanguage.v1beta.GenerativeService.BidiGenerateContent";

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum GeminiError {
    #[error("invalid Gemini Live configuration or input: {0}")]
    Invalid(&'static str),
    #[error("unsupported Gemini Live feature: {0}")]
    Unsupported(&'static str),
    #[error("Gemini Live message or state exceeds configured limit")]
    TooLarge,
    #[error("Gemini Live {0} queue is full")]
    Backpressure(&'static str),
    #[error("Gemini Live {0} timed out")]
    Timeout(&'static str),
    #[error("Gemini Live transport failed")]
    Transport,
    #[error("Gemini Live handshake failed (HTTP {0})")]
    Http(u16),
    #[error("Gemini Live protocol error: {0}")]
    Protocol(&'static str),
    #[error("Gemini Live provider error (code {code}): {message}")]
    Provider { code: i64, message: String },
    #[error("Gemini Live session closed")]
    Closed,
}

/// Explicit bounded receive-side session recovery. Sent actions are never replayed.
/// A saved handle need not cover the most recent input: this is not a guarantee
/// of lossless delivery or exactly-once provider output.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResumptionPolicy {
    /// Total successful or failed reconnect attempts per logical session.
    pub max_reconnects: usize,
    /// UTF-8 bytes retained for a single opaque provider handle.
    pub max_handle_bytes: usize,
}
impl Default for ResumptionPolicy {
    fn default() -> Self {
        Self {
            max_reconnects: 2,
            max_handle_bytes: 4096,
        }
    }
}
impl ResumptionPolicy {
    pub(crate) fn validate(&self) -> Result<(), GeminiError> {
        if !(1..=8).contains(&self.max_reconnects) || !(1..=65536).contains(&self.max_handle_bytes)
        {
            return Err(GeminiError::Invalid("session resumption limits"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct Limits {
    pub queue_capacity: usize,
    pub max_message_bytes: usize,
    pub max_audio_bytes: usize,
    pub max_pending_tools: usize,
    pub connect_timeout: Duration,
    pub setup_timeout: Duration,
    pub write_timeout: Duration,
    pub finish_timeout: Duration,
}
impl Default for Limits {
    fn default() -> Self {
        Self {
            queue_capacity: 64,
            max_message_bytes: 1 << 20,
            max_audio_bytes: 192_000,
            max_pending_tools: 32,
            connect_timeout: Duration::from_secs(10),
            setup_timeout: Duration::from_secs(10),
            write_timeout: Duration::from_secs(5),
            finish_timeout: Duration::from_secs(15),
        }
    }
}
impl Limits {
    pub(crate) fn validate(&self) -> Result<(), GeminiError> {
        if self.queue_capacity == 0
            || self.queue_capacity > 4096
            || self.max_message_bytes < 256
            || self.max_message_bytes > 16 * 1024 * 1024
            || self.max_audio_bytes == 0
            || self.max_audio_bytes > self.max_message_bytes
            || self.max_pending_tools == 0
            || self.max_pending_tools > 1024
            || [
                self.connect_timeout,
                self.setup_timeout,
                self.write_timeout,
                self.finish_timeout,
            ]
            .iter()
            .any(|duration| duration.is_zero() || *duration > Duration::from_secs(86_400))
        {
            return Err(GeminiError::Invalid("limits"));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum Modality {
    Audio,
    Text,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionMode {
    Conversation,
    Transcription,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StartSensitivity {
    #[serde(rename = "START_SENSITIVITY_LOW")]
    Low,
    #[serde(rename = "START_SENSITIVITY_HIGH")]
    High,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndSensitivity {
    #[serde(rename = "END_SENSITIVITY_LOW")]
    Low,
    #[serde(rename = "END_SENSITIVITY_HIGH")]
    High,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AutomaticActivityDetection {
    pub disabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub start_of_speech_sensitivity: Option<StartSensitivity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub end_of_speech_sensitivity: Option<EndSensitivity>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_padding_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silence_duration_ms: Option<u32>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FunctionDeclaration {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}

/// Connection-time configuration. No untyped pass-through for unsupported
/// advanced video settings or provider-managed background work is provided.
/// Bounded resumption is enabled separately on `GeminiClient`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionConfig {
    pub model: String,
    pub modality: Modality,
    pub system_instruction: Option<String>,
    pub voice: Option<String>,
    pub language_code: Option<String>,
    pub temperature: Option<f64>,
    pub max_output_tokens: Option<u32>,
    pub top_p: Option<f64>,
    pub top_k: Option<u32>,
    pub input_transcription: bool,
    pub output_transcription: bool,
    pub activity_detection: AutomaticActivityDetection,
    pub tools: Vec<FunctionDeclaration>,
}
impl SessionConfig {
    /// A model must be selected explicitly; availability is provider-dependent.
    pub fn new(model: impl Into<String>) -> Self {
        Self {
            model: model.into(),
            modality: Modality::Audio,
            system_instruction: None,
            voice: None,
            language_code: None,
            temperature: None,
            max_output_tokens: None,
            top_p: None,
            top_k: None,
            input_transcription: true,
            output_transcription: true,
            activity_detection: AutomaticActivityDetection::default(),
            tools: vec![],
        }
    }
    pub(crate) fn validate(&self, mode: SessionMode, limits: &Limits) -> Result<(), GeminiError> {
        let model = self.model.strip_prefix("models/").unwrap_or(&self.model);
        if model.is_empty()
            || model.len() > 256
            || !model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(GeminiError::Invalid("model resource name"));
        }
        if self
            .temperature
            .is_some_and(|x| !x.is_finite() || !(0.0..=2.0).contains(&x))
            || self
                .top_p
                .is_some_and(|x| !x.is_finite() || !(0.0..=1.0).contains(&x))
            || self.max_output_tokens == Some(0)
            || self.top_k == Some(0)
            || self.voice.as_ref().is_some_and(|x| x.is_empty())
        {
            return Err(GeminiError::Invalid("generation settings"));
        }
        if mode == SessionMode::Transcription
            && (self.modality != Modality::Text
                || !self.input_transcription
                || self.output_transcription
                || !self.tools.is_empty()
                || self.activity_detection.disabled)
        {
            return Err(GeminiError::Invalid("transcription mode needs TEXT, input transcription, server VAD, and no output transcription/tools"));
        }
        if self.tools.len() > limits.max_pending_tools {
            return Err(GeminiError::TooLarge);
        }
        let mut names = std::collections::HashSet::new();
        for tool in &self.tools {
            if tool.name.is_empty() || !names.insert(&tool.name) || !tool.parameters.is_object() {
                return Err(GeminiError::Invalid("function declarations"));
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct GeminiConfig {
    pub endpoint: String,
    pub api_key: String,
    pub session: SessionConfig,
    pub mode: SessionMode,
    pub input_sample_rate: u32,
    pub limits: Limits,
}
impl fmt::Debug for GeminiConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("GeminiConfig")
            .field("api_key", &"[REDACTED]")
            .field("session", &self.session)
            .field("mode", &self.mode)
            .field("input_sample_rate", &self.input_sample_rate)
            .field("limits", &self.limits)
            .finish_non_exhaustive()
    }
}
impl GeminiConfig {
    pub fn new(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            endpoint: DEFAULT_ENDPOINT.into(),
            api_key: api_key.into(),
            session: SessionConfig::new(model),
            mode: SessionMode::Conversation,
            input_sample_rate: 16_000,
            limits: Limits::default(),
        }
    }
    pub fn transcription(api_key: impl Into<String>, model: impl Into<String>) -> Self {
        let mut config = Self::new(api_key, model);
        config.mode = SessionMode::Transcription;
        config.session.modality = Modality::Text;
        config.session.output_transcription = false;
        config
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    User,
    Model,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TextTurn {
    pub role: Role,
    pub text: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionResponse {
    pub id: String,
    pub name: String,
    pub response: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    Audio(Vec<u8>),
    /// Bounded JPEG bytes, sent as realtimeInput.video in conversation mode.
    VideoJpeg(Vec<u8>),
    RealtimeText(String),
    ClientContent {
        turns: Vec<TextTurn>,
        turn_complete: bool,
    },
    /// Send activityStart and then the bounded pre-roll in the same queue item.
    ActivityStart {
        preroll: Vec<u8>,
    },
    ActivityEnd,
    AudioStreamEnd,
    ToolResponses(Vec<FunctionResponse>),
}
/// Local correlation, never claimed to be a provider-generated response ID.
/// Audio/transcription is asynchronous; this identifies the input active when
/// model output began, not sample-accurate causal alignment.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Origin {
    pub frame_id: Option<u64>,
    pub context_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCall {
    /// Application correlation ID, also echoed in the function response.
    /// Check `id_source` before treating this as provider-supplied identity.
    pub id: String,
    #[serde(default)]
    pub id_source: ToolCallIdSource,
    pub name: String,
    #[serde(default = "empty_arguments")]
    pub args: Value,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolCallIdSource {
    #[default]
    Provider,
    LocalGenerated,
}
fn empty_arguments() -> Value {
    Value::Object(Default::default())
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Transcript {
    #[serde(default)]
    pub text: String,
    #[serde(default)]
    pub language_code: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Usage {
    #[serde(default)]
    pub prompt_token_count: Option<u64>,
    #[serde(default)]
    pub response_token_count: Option<u64>,
    #[serde(default)]
    pub total_token_count: Option<u64>,
}
#[derive(Debug, Clone, PartialEq)]
pub enum Event {
    Text {
        text: String,
        thought: bool,
    },
    Audio {
        pcm: Vec<u8>,
        sample_rate: u32,
    },
    InputTranscription {
        transcript: Transcript,
        is_final: bool,
    },
    OutputTranscription(Transcript),
    GenerationComplete,
    TurnComplete,
    Interrupted,
    ToolCalls(Vec<FunctionCall>),
    ToolCallsCancelled(Vec<FunctionCall>),
    Usage(Usage),
    GoAway {
        time_left: String,
    },
}
impl Event {
    pub(crate) fn is_output(&self) -> bool {
        matches!(
            self,
            Self::Text { .. }
                | Self::Audio { .. }
                | Self::OutputTranscription(_)
                | Self::GenerationComplete
                | Self::TurnComplete
                | Self::ToolCalls(_)
        )
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct GeminiEvent {
    pub event: Event,
    pub origin: Origin,
    pub generation: u64,
}
