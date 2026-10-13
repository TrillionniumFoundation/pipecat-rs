//! Payloads for the complete source frame family. Local executable references
//! are intentionally rejected by serialization instead of losing behavior.
use crate::frames::*;
use crate::{FrameProcessor, ProcessorError};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::any::Any;
use std::collections::BTreeMap;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

type CallbackFuture = Pin<Box<dyn Future<Output = Result<(), ProcessorError>> + Send>>;
#[derive(Clone)]
pub struct LocalAsyncCallback(Arc<dyn Fn() -> CallbackFuture + Send + Sync>);
impl LocalAsyncCallback {
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        Self(Arc::new(move || Box::pin(callback())))
    }
    pub async fn call(&self) -> Result<(), ProcessorError> {
        (self.0)().await
    }
}
#[derive(Clone)]
pub struct LocalResultCallback(Arc<dyn Fn(Value) -> CallbackFuture + Send + Sync>);
impl LocalResultCallback {
    pub fn new<F, Fut>(callback: F) -> Self
    where
        F: Fn(Value) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        Self(Arc::new(move |value| Box::pin(callback(value))))
    }
    pub async fn call(&self, result: Value) -> Result<(), ProcessorError> {
        (self.0)(result).await
    }
}
type MessageTransformFn =
    dyn Fn(Vec<ContextMessage>) -> Result<Vec<ContextMessage>, ProcessorError> + Send + Sync;
#[derive(Clone)]
pub struct LocalMessageTransform(Arc<MessageTransformFn>);
impl LocalMessageTransform {
    pub fn new<F>(transform: F) -> Self
    where
        F: Fn(Vec<ContextMessage>) -> Result<Vec<ContextMessage>, ProcessorError>
            + Send
            + Sync
            + 'static,
    {
        Self(Arc::new(transform))
    }
    pub fn apply(
        &self,
        messages: Vec<ContextMessage>,
    ) -> Result<Vec<ContextMessage>, ProcessorError> {
        (self.0)(messages)
    }
}
#[derive(Clone)]
pub struct LocalObject(Arc<dyn Any + Send + Sync>);
impl LocalObject {
    pub fn new<T: Any + Send + Sync>(value: T) -> Self {
        Self(Arc::new(value))
    }
    pub fn downcast_ref<T: Any>(&self) -> Option<&T> {
        self.0.downcast_ref()
    }
}
#[derive(Clone)]
pub struct LocalProcessorReference(pub Arc<dyn FrameProcessor>);
macro_rules! local_reference {
    ($($ty:ident),+ $(,)?) => { $(
        impl fmt::Debug for $ty { fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result { f.write_str(stringify!($ty)) } }
        impl PartialEq for $ty { fn eq(&self, other: &Self) -> bool { Arc::ptr_eq(&self.0, &other.0) } }
        impl Serialize for $ty {
            fn serialize<S: Serializer>(&self, _: S) -> Result<S::Ok, S::Error> { Err(serde::ser::Error::custom(concat!(stringify!($ty), " is executable local state and cannot be serialized"))) }
        }
        impl<'de> Deserialize<'de> for $ty {
            fn deserialize<D: Deserializer<'de>>(_: D) -> Result<Self, D::Error> { Err(serde::de::Error::custom(concat!(stringify!($ty), " cannot be created by deserialization"))) }
        }
    )+ }
}
local_reference!(
    LocalAsyncCallback,
    LocalResultCallback,
    LocalMessageTransform,
    LocalObject,
    LocalProcessorReference
);

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum ProcessorReference {
    Name(String),
    Local(LocalProcessorReference),
}
impl ProcessorReference {
    pub fn name(&self) -> &str {
        match self {
            Self::Name(name) => name,
            Self::Local(processor) => processor.0.name(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OutputImageRawFrame {
    pub image: ImageRawFrame,
    pub sync_with_audio: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UrlImageRawFrame {
    pub image: OutputImageRawFrame,
    pub url: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpriteFrame {
    pub images: Vec<OutputImageRawFrame>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AggregatedTextProgressFrame {
    pub segment_id: u64,
    pub context_id: Option<String>,
    pub text: String,
    pub aggregated_by: String,
    pub accumulated_text: String,
    pub remaining_text: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmContextAssistantTurnFrame {
    pub text: String,
    pub timestamp: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedTranscriptionFrame {
    pub text: TextFrame,
    pub user_id: String,
    pub timestamp: String,
    pub language: Option<String>,
    pub result: Option<Value>,
    pub finalized: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserAudioRawFrame {
    pub audio: AudioRawFrame,
    pub user_id: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserImageRequestFrame {
    pub user_id: String,
    pub text: Option<String>,
    pub append_to_context: Option<bool>,
    pub video_source: Option<String>,
    pub function_name: Option<String>,
    pub tool_call_id: Option<String>,
    pub result_callback: Option<LocalResultCallback>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserImageRawFrame {
    pub image: ImageRawFrame,
    pub user_id: String,
    pub text: Option<String>,
    pub append_to_context: Option<bool>,
    pub request: Option<Box<UserImageRequestFrame>>,
    pub run_llm: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedUserFileRawFrame {
    pub file: FileRawFrame,
    pub user_id: String,
    pub text: String,
    pub append_to_context: Option<bool>,
    pub custom_options: Option<BTreeMap<String, Value>>,
    pub run_llm: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AssistantImageRawFrame {
    pub image: OutputImageRawFrame,
    pub original_data: Option<Vec<u8>>,
    pub original_mime_type: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct DtmfSequence {
    pub button: Option<KeypadEntry>,
    pub buttons: Vec<KeypadEntry>,
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DtmfError {
    #[error("DTMF needs at least one keypad entry")]
    Empty,
    #[error("invalid DTMF character: {0}")]
    Invalid(char),
}
impl DtmfSequence {
    /// An explicitly supplied sequence takes precedence over the single button.
    pub fn new(
        button: Option<KeypadEntry>,
        buttons: Option<Vec<KeypadEntry>>,
    ) -> Result<Self, DtmfError> {
        let buttons = buttons.unwrap_or_else(|| button.into_iter().collect());
        if buttons.is_empty() {
            return Err(DtmfError::Empty);
        }
        if let Some(button) = buttons.iter().find(|button| {
            matches!(
                button,
                KeypadEntry::A | KeypadEntry::B | KeypadEntry::C | KeypadEntry::D
            )
        }) {
            return Err(DtmfError::Invalid(button.as_char()));
        }
        Ok(Self { button, buttons })
    }
}
impl std::str::FromStr for DtmfSequence {
    type Err = DtmfError;
    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let buttons = value
            .chars()
            .map(KeypadEntry::try_from)
            .collect::<Result<Vec<_>, _>>()?;
        Self::new(None, Some(buttons))
    }
}
impl fmt::Display for DtmfSequence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for button in &self.buttons {
            write!(f, "{}", button.as_char())?;
        }
        Ok(())
    }
}
impl KeypadEntry {
    pub fn as_char(self) -> char {
        match self {
            Self::Zero => '0',
            Self::One => '1',
            Self::Two => '2',
            Self::Three => '3',
            Self::Four => '4',
            Self::Five => '5',
            Self::Six => '6',
            Self::Seven => '7',
            Self::Eight => '8',
            Self::Nine => '9',
            Self::Star => '*',
            Self::Pound => '#',
            Self::A => 'A',
            Self::B => 'B',
            Self::C => 'C',
            Self::D => 'D',
        }
    }
}
impl TryFrom<char> for KeypadEntry {
    type Error = DtmfError;
    fn try_from(value: char) -> Result<Self, Self::Error> {
        match value {
            '0' => Ok(Self::Zero),
            '1' => Ok(Self::One),
            '2' => Ok(Self::Two),
            '3' => Ok(Self::Three),
            '4' => Ok(Self::Four),
            '5' => Ok(Self::Five),
            '6' => Ok(Self::Six),
            '7' => Ok(Self::Seven),
            '8' => Ok(Self::Eight),
            '9' => Ok(Self::Nine),
            '*' => Ok(Self::Star),
            '#' => Ok(Self::Pound),
            'A' => Ok(Self::A),
            'B' => Ok(Self::B),
            'C' => Ok(Self::C),
            'D' => Ok(Self::D),
            other => Err(DtmfError::Invalid(other)),
        }
    }
}

/// Distinguishes an omitted setting from explicitly clearing its value.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(tag = "state", content = "value", rename_all = "snake_case")]
pub enum Setting<T> {
    #[default]
    NotGiven,
    Null,
    Value(T),
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ServiceSettings {
    pub model: Setting<String>,
    pub extra: BTreeMap<String, Value>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct LlmSettings {
    pub base: ServiceSettings,
    pub system_instruction: Setting<String>,
    pub temperature: Setting<f64>,
    pub max_tokens: Setting<u64>,
    pub top_p: Setting<f64>,
    pub top_k: Setting<u64>,
    pub frequency_penalty: Setting<f64>,
    pub presence_penalty: Setting<f64>,
    pub seed: Setting<i64>,
    pub filter_incomplete_user_turns: Setting<bool>,
    pub user_turn_completion_config: Setting<UserTurnCompletionConfig>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TtsSettings {
    pub base: ServiceSettings,
    pub voice: Setting<String>,
    pub language: Setting<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SttSettings {
    pub base: ServiceSettings,
    pub language: Setting<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceUpdateSettingsFrame<T> {
    pub settings: BTreeMap<String, Value>,
    pub delta: Option<T>,
    pub service: Option<ProcessorReference>,
    pub reach_inactive_services: bool,
}
impl<T> Default for ServiceUpdateSettingsFrame<T> {
    fn default() -> Self {
        Self {
            settings: BTreeMap::new(),
            delta: None,
            service: None,
            reach_inactive_services: false,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct VadParams {
    pub confidence: f64,
    pub start_secs: f64,
    pub stop_secs: f64,
    pub min_volume: f64,
}
impl Default for VadParams {
    fn default() -> Self {
        Self {
            confidence: 0.7,
            start_secs: 0.2,
            stop_secs: 0.2,
            min_volume: 0.6,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct BaseTurnParams {}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SpeechControlParamsFrame {
    pub vad_params: Option<VadParams>,
    pub turn_params: Option<BaseTurnParams>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UserTurnStrategy {
    Vad { enable_interruptions: bool },
    SpeechTimeout { timeout_seconds: f64 },
    External { enable_interruptions: bool },
    Incomplete { config: UserTurnCompletionConfig },
    Eager { speculation_timeout: f64 },
    Local { strategy: LocalObject },
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct UserTurnStrategies {
    pub start: Option<Vec<UserTurnStrategy>>,
    pub stop: Option<Vec<UserTurnStrategy>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServiceMetadataFrame {
    pub service_name: String,
    pub user_turn_strategies: Option<UserTurnStrategies>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SttMetadataFrame {
    pub base: ServiceMetadataFrame,
    pub ttfs_p99_latency: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmServiceMetadataFrame {
    pub base: ServiceMetadataFrame,
    pub is_realtime_service: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserTurnCompletionConfig {
    pub instructions: Option<String>,
    pub complete_marker: String,
    pub incomplete_short_marker: String,
    pub incomplete_long_marker: String,
    pub incomplete_short_timeout: f64,
    pub incomplete_long_timeout: f64,
    pub incomplete_short_prompt: Option<String>,
    pub incomplete_long_prompt: Option<String>,
}
impl Default for UserTurnCompletionConfig {
    fn default() -> Self {
        Self {
            instructions: None,
            complete_marker: "●".into(),
            incomplete_short_marker: "◐".into(),
            incomplete_long_marker: "○".into(),
            incomplete_short_timeout: 5.0,
            incomplete_long_timeout: 10.0,
            incomplete_short_prompt: None,
            incomplete_long_prompt: None,
        }
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessagePart {
    BedrockReasoning {
        text: String,
        signature: Option<String>,
    },
    BedrockRedacted {
        data: Vec<u8>,
    },
    GeminiText {
        text: String,
        thought: bool,
        signature: Option<String>,
        #[serde(default)]
        incomplete: bool,
    },
    GeminiToolMetadata {
        tool_call_id: String,
        signature: Option<String>,
    },
    Text {
        text: String,
    },
    Image {
        url: String,
        detail: Option<String>,
    },
    Audio {
        data: String,
        format: String,
    },
    File {
        data: String,
        mime_type: String,
        filename: Option<String>,
    },
    Thinking {
        text: String,
        signature: Option<String>,
    },
    Refusal {
        text: String,
    },
}
impl std::fmt::Debug for MessagePart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BedrockReasoning { text, signature } => f
                .debug_struct("BedrockReasoning")
                .field("text_bytes", &text.len())
                .field("signature_bytes", &signature.as_ref().map(String::len))
                .finish(),
            Self::BedrockRedacted { data } => f
                .debug_struct("BedrockRedacted")
                .field("bytes", &data.len())
                .finish(),
            Self::GeminiText {
                text,
                thought,
                signature,
                incomplete,
            } => f
                .debug_struct("GeminiText")
                .field("text_bytes", &text.len())
                .field("thought", thought)
                .field("signature", &signature.as_ref().map(|_| "[redacted]"))
                .field("incomplete", incomplete)
                .finish(),
            Self::GeminiToolMetadata { signature, .. } => f
                .debug_struct("GeminiToolMetadata")
                .field("signature", &signature.as_ref().map(|_| "[redacted]"))
                .finish_non_exhaustive(),
            Self::Text { text } => f.debug_struct("Text").field("text", text).finish(),
            Self::Image { url, detail } => f
                .debug_struct("Image")
                .field("url", url)
                .field("detail", detail)
                .finish(),
            Self::Audio { data, format } => f
                .debug_struct("Audio")
                .field("data", data)
                .field("format", format)
                .finish(),
            Self::File {
                data,
                mime_type,
                filename,
            } => f
                .debug_struct("File")
                .field("data", data)
                .field("mime_type", mime_type)
                .field("filename", filename)
                .finish(),
            Self::Thinking { text, signature } => f
                .debug_struct("Thinking")
                .field("text", text)
                .field("signature", &signature.as_ref().map(|_| "[redacted]"))
                .finish(),
            Self::Refusal { text } => f.debug_struct("Refusal").field("text", text).finish(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum MessageContent {
    Text(String),
    Parts(Vec<MessagePart>),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContextMessage {
    pub role: ChatRole,
    pub content: MessageContent,
    pub name: Option<String>,
    pub tool_call_id: Option<String>,
    pub tool_calls: Vec<FunctionCall>,
    pub extra: BTreeMap<String, Value>,
}
impl From<ChatMessage> for ContextMessage {
    fn from(message: ChatMessage) -> Self {
        Self {
            role: message.role,
            content: MessageContent::Text(message.content),
            name: message.name,
            tool_call_id: message.tool_call_id,
            tool_calls: message.tool_calls,
            extra: BTreeMap::new(),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum ToolSet {
    NotGiven,
    Functions(Vec<ToolDefinition>),
    Grouped(BTreeMap<String, Vec<ToolDefinition>>),
    Direct(Vec<DirectFunction>),
    Schema(ToolsSchema),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DirectFunction {
    pub schema: ToolDefinition,
    pub handler: LocalToolFunction,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmContextData {
    pub messages: Vec<ContextMessage>,
    pub tools: ToolSet,
    pub tool_choice: Option<ToolChoice>,
}
impl From<LlmContextFrame> for LlmContextData {
    fn from(context: LlmContextFrame) -> Self {
        Self {
            messages: context.messages.into_iter().map(Into::into).collect(),
            tools: ToolSet::Functions(context.tools),
            tool_choice: context.tool_choice,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedLlmContextFrame {
    pub context: LlmContextData,
    pub speculation: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedMessagesFrame {
    pub messages: Vec<ContextMessage>,
    pub run_llm: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmMessagesTransformFrame {
    pub transform: LocalMessageTransform,
    pub run_llm: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmThoughtStartFrame {
    pub append_to_context: bool,
    pub llm: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmThoughtEndFrame {
    pub signature: Option<Value>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmFullResponseFrame {
    pub skip_tts: Option<bool>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallResultProperties {
    pub run_llm: Option<bool>,
    pub on_context_updated: Option<LocalAsyncCallback>,
    pub is_final: bool,
}
impl Default for FunctionCallResultProperties {
    fn default() -> Self {
        Self {
            run_llm: None,
            on_context_updated: None,
            is_final: true,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedFunctionCallResultFrame {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Value,
    pub result: Value,
    pub run_llm: Option<bool>,
    pub properties: Option<FunctionCallResultProperties>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallFromLlm {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: BTreeMap<String, Value>,
    pub context: Option<LlmContextData>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExternalFunctionCall {
    pub function_name: String,
    pub tool_call_id: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExternalFunctionCallInProgressFrame {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExternalFunctionCallResultFrame {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Value,
    pub result: Value,
    pub is_final: bool,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ReasonFrame {
    pub reason: Option<Value>,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TracingContext {
    pub conversation_id: Option<String>,
    pub conversation_traceparent: Option<String>,
    pub turn_traceparent: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedStartFrame {
    pub configuration: StartFrame,
    pub enable_tracing: bool,
    pub tracing_context: Option<TracingContext>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DetailedErrorFrame {
    pub error: String,
    pub fatal: bool,
    pub processor: Option<ProcessorReference>,
    pub exception: Option<LocalObject>,
    pub category: Option<ErrorCategory>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmContextSummaryConfig {
    pub target_context_tokens: usize,
    pub min_messages_after_summary: usize,
    pub summarization_prompt: Option<String>,
    pub summary_message_template: String,
    pub llm: Option<ProcessorReference>,
    pub summarization_timeout: f64,
}
impl Default for LlmContextSummaryConfig {
    fn default() -> Self {
        Self {
            target_context_tokens: 6000,
            min_messages_after_summary: 4,
            summarization_prompt: None,
            summary_message_template: "Conversation summary: {summary}".into(),
            llm: None,
            summarization_timeout: 120.0,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct LlmContextSummaryRequestFrame {
    pub request_id: String,
    pub context: LlmContextData,
    pub min_messages_to_keep: usize,
    pub target_context_tokens: usize,
    pub summarization_prompt: String,
    pub summarization_timeout: Option<f64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LlmContextSummaryResultFrame {
    pub request_id: String,
    pub summary: String,
    pub last_summarized_index: usize,
    pub error: Option<String>,
}

#[derive(Clone)]
pub struct LocalFlushEvent(Arc<FlushEventState>);
struct FlushEventState {
    set: std::sync::atomic::AtomicBool,
    changed: tokio::sync::Notify,
}
impl Default for LocalFlushEvent {
    fn default() -> Self {
        Self(Arc::new(FlushEventState {
            set: std::sync::atomic::AtomicBool::new(false),
            changed: tokio::sync::Notify::new(),
        }))
    }
}
impl LocalFlushEvent {
    pub fn is_set(&self) -> bool {
        self.0.set.load(std::sync::atomic::Ordering::Acquire)
    }
    pub fn set(&self) {
        self.0.set.store(true, std::sync::atomic::Ordering::Release);
        self.0.changed.notify_waiters();
    }
    pub fn clear(&self) {
        self.0
            .set
            .store(false, std::sync::atomic::Ordering::Release);
    }
    pub async fn wait(&self) {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_set() {
                return;
            }
            notified.await;
        }
    }
}
local_reference!(LocalFlushEvent);
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct PipelineFlushFrame {
    pub event: Option<LocalFlushEvent>,
    pub returning: bool,
    pub origin: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct VadUserStartedSpeakingFrame {
    pub start_secs: f64,
    pub timestamp: f64,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct VadUserStoppedSpeakingFrame {
    pub stop_secs: f64,
    pub timestamp: f64,
}
fn wall_time_seconds() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
impl Default for VadUserStartedSpeakingFrame {
    fn default() -> Self {
        Self {
            start_secs: 0.0,
            timestamp: wall_time_seconds(),
        }
    }
}
impl Default for VadUserStoppedSpeakingFrame {
    fn default() -> Self {
        Self {
            stop_secs: 0.0,
            timestamp: wall_time_seconds(),
        }
    }
}

#[path = "frame_payloads/local_tool_function.rs"]
mod local_tool_function;
pub use local_tool_function::LocalToolOptions;

#[derive(Clone)]
pub struct LocalToolFunction(
    Arc<local_tool_function::ToolFunctionState>,
    LocalToolFunctionIdentity,
);
/// Retaining this token pins identity without retaining the callback or its captures.
#[derive(Clone)]
pub(crate) struct LocalToolFunctionIdentity(Arc<()>);
impl LocalToolFunctionIdentity {
    pub(crate) fn address(&self) -> usize {
        Arc::as_ptr(&self.0) as usize
    }
}
impl LocalToolFunction {
    pub(crate) fn local_identity(&self) -> &LocalToolFunctionIdentity {
        &self.1
    }
}
local_reference!(LocalToolFunction);
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AdapterType {
    Gemini,
    Openai,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum StandardTool {
    Function(ToolDefinition),
    Direct(DirectFunction),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolsSchema {
    pub standard_tools: Vec<StandardTool>,
    pub custom_tools: Option<BTreeMap<AdapterType, Vec<BTreeMap<String, Value>>>>,
}

/// Framework-level client request correlation, independent of the wire codec.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviClientMessageFrame {
    pub msg_id: String,
    pub message_type: String,
    pub data: Option<Value>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviServerResponseFrame {
    pub client_msg: RtviClientMessageFrame,
    pub data: Option<Value>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RtviFunctionCallReportLevel {
    Disabled,
    #[default]
    None,
    Name,
    Arguments,
    Full,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviConfigureObserverFrame {
    pub function_call_report_level: Option<BTreeMap<String, RtviFunctionCallReportLevel>>,
    pub vad_user_speaking_enabled: Option<bool>,
    pub bot_llm_marker_enabled: Option<bool>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviUiCommandFrame {
    pub command: String,
    pub payload: Option<Value>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviUiEventFrame {
    pub msg_id: String,
    pub event: String,
    pub payload: Option<Value>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviUiSnapshotFrame {
    pub msg_id: String,
    pub tree: Value,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct RtviUiCancelJobGroupFrame {
    pub msg_id: String,
    pub job_id: String,
    pub reason: Option<String>,
}

/// Native extension for already-prepared TTS input, not a Python frame alias.
/// The original channel includes source delimiters/markup; display text is what
/// users see; synthesis text is the exact provider request. A skipped input is
/// ordered with surrounding synthesis but is never sent to the provider.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsTextInputFrame {
    pub synthesis_text: String,
    pub display_text: String,
    pub original_text: String,
    pub append_to_context: bool,
    /// The aggregate supplies its own spacing. Provider declarations may also
    /// set this; false does not erase spacing present in provider word tokens.
    pub includes_inter_frame_spaces: bool,
    pub aggregated_by: String,
    pub skip_tts: bool,
}
impl TtsTextInputFrame {
    pub fn new(text: impl Into<String>) -> Self {
        let text = text.into();
        Self {
            synthesis_text: text.clone(),
            display_text: text.clone(),
            original_text: text,
            append_to_context: true,
            includes_inter_frame_spaces: true,
            aggregated_by: "sentence".into(),
            skip_tts: false,
        }
    }
}
/// Half-open Unicode-scalar range in original source, not UTF-8 bytes or sound.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsSourceSpan {
    pub start: usize,
    pub end: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtsAlignmentEvidence {
    Unreported,
    Unmatched,
    Resynchronized,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TtsSynthesisOutcome {
    Pending,
    Succeeded,
}
/// Native diagnostic only. Neither this frame nor successful synthesis proves
/// transport submission, Played receipts, audible progress or spoken history.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TtsAlignmentDiagnosticFrame {
    pub context_id: String,
    /// Exact emitted aggregate anchor frame ID, absent for unmatched words.
    pub segment_id: Option<u64>,
    pub evidence: TtsAlignmentEvidence,
    pub synthesis_text: String,
    pub original_text: Option<String>,
    pub source_span: Option<TtsSourceSpan>,
    pub display_position: Option<usize>,
    pub synthesis_outcome: TtsSynthesisOutcome,
}

#[cfg(test)]
mod review_handler_identity;

/// Typed Gateway command. The message is sent verbatim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenClawSendFrame {
    pub message: String,
    pub session_key: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenClawSteerFrame {
    pub message: String,
}
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenClawAbortFrame {
    pub reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenClawStartedFrame {
    pub run_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenClawTextFrame {
    pub text: String,
    pub run_id: String,
}
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenClawRunStatus {
    #[default]
    Completed,
    Cancelled,
    Failed,
}
/// Unknown means local observation ended without proof the remote run stopped.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OpenClawRemoteOutcome {
    Settled,
    #[default]
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OpenClawEndFrame {
    pub run_id: String,
    pub status: OpenClawRunStatus,
    pub text: String,
    #[serde(default)]
    pub remote_outcome: OpenClawRemoteOutcome,
}
