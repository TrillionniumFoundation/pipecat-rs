use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum AudioFormat {
    #[serde(rename = "audio/pcm")]
    Pcm { rate: u32 },
    #[serde(rename = "audio/pcmu")]
    Pcmu,
    #[serde(rename = "audio/pcma")]
    Pcma,
}
impl Default for AudioFormat {
    fn default() -> Self {
        Self::Pcm { rate: 24_000 }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum TurnDetection {
    #[serde(rename = "server_vad")]
    ServerVad {
        threshold: f32,
        prefix_padding_ms: u32,
        silence_duration_ms: u32,
        create_response: bool,
        interrupt_response: bool,
    },
    #[serde(rename = "semantic_vad")]
    SemanticVad {
        eagerness: Eagerness,
        create_response: bool,
        interrupt_response: bool,
    },
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Eagerness {
    Low,
    Medium,
    High,
    Auto,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionConfig {
    pub model: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prompt: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AudioInput {
    #[serde(default)]
    pub format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<TranscriptionConfig>,
    /// `None` explicitly disables server turn detection on the wire.
    pub turn_detection: Option<TurnDetection>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioOutput {
    #[serde(default)]
    pub format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f32>,
}
impl Default for AudioOutput {
    fn default() -> Self {
        Self {
            format: AudioFormat::default(),
            voice: Some("alloy".into()),
            speed: None,
        }
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AudioConfiguration {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input: Option<AudioInput>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<AudioOutput>,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modality {
    Text,
    Audio,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionTool {
    #[serde(rename = "type", default = "function_type")]
    kind: String,
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
fn function_type() -> String {
    "function".into()
}
impl FunctionTool {
    pub fn new(name: impl Into<String>, description: impl Into<String>, parameters: Value) -> Self {
        Self {
            kind: function_type(),
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionProperties {
    #[serde(rename = "type", default = "session_type")]
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_modalities: Option<Vec<Modality>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<FunctionTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<TokenLimit>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
}
fn session_type() -> String {
    "realtime".into()
}
impl Default for SessionProperties {
    fn default() -> Self {
        Self {
            kind: session_type(),
            id: None,
            model: None,
            instructions: None,
            output_modalities: None,
            audio: None,
            tools: None,
            tool_choice: None,
            max_output_tokens: None,
            reasoning: None,
        }
    }
}
impl SessionProperties {
    pub fn new() -> Self {
        Self {
            kind: session_type(),
            ..Default::default()
        }
    }
    pub fn pcm() -> Self {
        Self {
            audio: Some(AudioConfiguration {
                input: Some(AudioInput::default()),
                output: Some(AudioOutput::default()),
            }),
            output_modalities: Some(vec![Modality::Audio]),
            ..Self::new()
        }
    }
    pub(crate) fn normalize(&mut self) {
        self.kind = session_type();
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum TokenLimit {
    Tokens(u32),
    Unlimited(Infinity),
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum Infinity {
    #[serde(rename = "inf")]
    Infinite,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reasoning {
    pub effort: String,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResponseOptions {
    /// Local correlation ID; never transmitted as an API parameter.
    #[serde(skip)]
    pub context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_modalities: Option<Vec<Modality>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_output_tokens: Option<TokenLimit>,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    System,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ItemContent {
    InputText {
        text: String,
    },
    OutputText {
        text: String,
    },
    Text {
        text: String,
    },
    InputAudio {
        audio: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        transcript: Option<String>,
    },
    OutputAudio {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transcript: Option<String>,
    },
    Audio {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        transcript: Option<String>,
    },
    InputImage {
        image_url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConversationItem {
    Message {
        id: String,
        role: Role,
        content: Vec<ItemContent>,
    },
    FunctionCall {
        id: String,
        call_id: String,
        name: String,
        arguments: String,
    },
    FunctionCallOutput {
        id: String,
        call_id: String,
        output: String,
    },
}
impl ConversationItem {
    pub fn id(&self) -> &str {
        match self {
            Self::Message { id, .. }
            | Self::FunctionCall { id, .. }
            | Self::FunctionCallOutput { id, .. } => id,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Action {
    /// Drain preceding commands and the active response, then close the session.
    Finish,
    UpdateSession(SessionProperties),
    AppendAudio(Vec<u8>),
    CommitAudio,
    ClearAudio,
    CreateItem {
        item: ConversationItem,
        previous_item_id: Option<String>,
    },
    DeleteItem {
        item_id: String,
    },
    RetrieveItem {
        item_id: String,
    },
    TruncateItem(PlaybackPosition),
    CreateResponse(ResponseOptions),
    CancelResponse,
    /// Final tool outputs are synchronized across all outstanding calls. A
    /// continuation is created only after response.done and all final outputs.
    ToolResult {
        call_id: String,
        output: Value,
        is_final: bool,
        run_response: bool,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PlaybackPosition {
    pub item_id: String,
    pub content_index: u32,
    /// Audio actually rendered, relative to this content part (not wall time).
    pub audio_end_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Conversation {
    pub id: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseStatus {
    InProgress,
    Completed,
    Incomplete,
    Cancelled,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Response {
    pub id: String,
    pub status: ResponseStatus,
    #[serde(default)]
    pub conversation_id: Option<String>,
    #[serde(default)]
    pub output: Vec<ConversationItem>,
    #[serde(default)]
    pub usage: Option<Usage>,
    #[serde(default)]
    pub status_details: Option<ResponseStatusDetails>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseStatusDetails {
    #[serde(default)]
    pub reason: Option<String>,
    #[serde(default)]
    pub error: Option<ProviderError>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Usage {
    pub total_tokens: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    #[serde(default)]
    pub input_token_details: Option<TokenDetails>,
    #[serde(default)]
    pub output_token_details: Option<TokenDetails>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TokenDetails {
    #[serde(default)]
    pub text_tokens: u64,
    #[serde(default)]
    pub audio_tokens: u64,
    #[serde(default)]
    pub image_tokens: u64,
    #[serde(default)]
    pub cached_tokens: u64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderError {
    #[serde(rename = "type", default)]
    pub kind: String,
    #[serde(default)]
    pub code: Option<String>,
    pub message: String,
    #[serde(default)]
    pub param: Option<String>,
    #[serde(default)]
    pub event_id: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RateLimit {
    pub name: String,
    pub limit: u64,
    pub remaining: u64,
    pub reset_seconds: f64,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    #[serde(rename = "session.created")]
    SessionCreated { session: SessionProperties },
    #[serde(rename = "session.updated")]
    SessionUpdated { session: SessionProperties },
    #[serde(rename = "conversation.created")]
    ConversationCreated { conversation: Conversation },
    #[serde(rename = "conversation.item.added")]
    ItemAdded {
        item: ConversationItem,
        #[serde(default)]
        previous_item_id: Option<String>,
    },
    #[serde(rename = "conversation.item.done")]
    ItemDone { item: ConversationItem },
    #[serde(rename = "conversation.item.retrieved")]
    ItemRetrieved { item: ConversationItem },
    #[serde(rename = "conversation.item.deleted")]
    ItemDeleted { item_id: String },
    #[serde(rename = "conversation.item.truncated")]
    ItemTruncated {
        #[serde(flatten)]
        position: PlaybackPosition,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.delta")]
    InputTranscriptionDelta {
        item_id: String,
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.completed")]
    InputTranscriptionCompleted {
        item_id: String,
        content_index: u32,
        transcript: String,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.failed")]
    InputTranscriptionFailed {
        item_id: String,
        content_index: u32,
        error: ProviderError,
    },
    #[serde(rename = "input_audio_buffer.speech_started")]
    SpeechStarted {
        item_id: String,
        audio_start_ms: u64,
    },
    #[serde(rename = "input_audio_buffer.speech_stopped")]
    SpeechStopped { item_id: String, audio_end_ms: u64 },
    #[serde(rename = "input_audio_buffer.committed")]
    AudioCommitted {
        item_id: String,
        #[serde(default)]
        previous_item_id: Option<String>,
    },
    #[serde(rename = "input_audio_buffer.cleared")]
    AudioCleared,
    #[serde(rename = "response.created")]
    ResponseCreated { response: Response },
    #[serde(rename = "response.done")]
    ResponseDone { response: Response },
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded {
        response_id: String,
        output_index: u32,
        item: ConversationItem,
    },
    #[serde(rename = "response.output_item.done")]
    OutputItemDone {
        response_id: String,
        output_index: u32,
        item: ConversationItem,
    },
    #[serde(rename = "response.content_part.added")]
    ContentPartAdded {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ItemContent,
    },
    #[serde(rename = "response.content_part.done")]
    ContentPartDone {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        part: ItemContent,
    },
    #[serde(rename = "response.output_text.delta")]
    TextDelta {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "response.output_text.done")]
    TextDone {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        text: String,
    },
    #[serde(rename = "response.output_audio_transcript.delta")]
    AudioTranscriptDelta {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "response.output_audio_transcript.done")]
    AudioTranscriptDone {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        transcript: String,
    },
    #[serde(rename = "response.output_audio.delta")]
    AudioDelta {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        #[serde(rename = "delta", deserialize_with = "decode_audio")]
        audio: Vec<u8>,
    },
    #[serde(rename = "response.output_audio.done")]
    AudioDone {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
    },
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionArgumentsDelta {
        response_id: String,
        item_id: String,
        output_index: u32,
        call_id: String,
        delta: String,
    },
    #[serde(rename = "response.function_call_arguments.done")]
    FunctionArgumentsDone {
        response_id: String,
        item_id: String,
        output_index: u32,
        call_id: String,
        #[serde(default)]
        name: Option<String>,
        arguments: String,
    },
    #[serde(rename = "rate_limits.updated")]
    RateLimitsUpdated { rate_limits: Vec<RateLimit> },
    #[serde(rename = "error")]
    Error { error: ProviderError },
    /// New unknown protocol events are ignored, without retaining arbitrary payloads.
    #[serde(other)]
    Unknown,
}
fn decode_audio<'de, D: serde::Deserializer<'de>>(de: D) -> Result<Vec<u8>, D::Error> {
    STANDARD
        .decode(String::deserialize(de)?)
        .map_err(|_| serde::de::Error::custom("invalid base64 audio"))
}
impl Event {
    pub fn response_id(&self) -> Option<&str> {
        match self {
            Self::ResponseCreated { response } | Self::ResponseDone { response } => {
                Some(&response.id)
            }
            Self::OutputItemAdded { response_id, .. }
            | Self::OutputItemDone { response_id, .. }
            | Self::ContentPartAdded { response_id, .. }
            | Self::ContentPartDone { response_id, .. }
            | Self::TextDelta { response_id, .. }
            | Self::TextDone { response_id, .. }
            | Self::AudioTranscriptDelta { response_id, .. }
            | Self::AudioTranscriptDone { response_id, .. }
            | Self::AudioDelta { response_id, .. }
            | Self::AudioDone { response_id, .. }
            | Self::FunctionArgumentsDelta { response_id, .. }
            | Self::FunctionArgumentsDone { response_id, .. } => Some(response_id),
            _ => None,
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct RealtimeEvent {
    pub event_id: String,
    pub session_id: Option<String>,
    pub conversation_id: Option<String>,
    pub context_id: Option<String>,
    pub generation: u64,
    pub event: Event,
}
#[derive(Deserialize)]
pub(crate) struct WireEvent {
    pub event_id: String,
    #[serde(flatten)]
    pub event: Event,
}
