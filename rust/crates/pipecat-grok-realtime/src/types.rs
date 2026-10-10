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
/// Grok's root-level server VAD configuration. Manual mode is represented by
/// `SessionProperties::turn_detection = Some(None)`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TurnDetection {
    #[serde(rename = "type", default = "vad_type")]
    kind: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub silence_duration_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_padding_ms: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub idle_timeout_ms: Option<u32>,
}
fn vad_type() -> String {
    "server_vad".into()
}
impl Default for TurnDetection {
    fn default() -> Self {
        Self {
            kind: vad_type(),
            threshold: None,
            silence_duration_ms: None,
            prefix_padding_ms: None,
            idle_timeout_ms: None,
        }
    }
}
impl TurnDetection {
    pub fn server_vad() -> Self {
        Self {
            kind: vad_type(),
            ..Self::default()
        }
    }
    pub(crate) fn valid_kind(&self) -> bool {
        self.kind == "server_vad"
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub language_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub keyterms: Option<Vec<String>>,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AudioTransport {
    Json,
    Binary,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AudioInput {
    #[serde(default)]
    pub format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<TranscriptionConfig>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<AudioTransport>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct AudioOutput {
    #[serde(default)]
    pub format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transport: Option<AudioTransport>,
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
/// Provider-hosted tools are advertised only; this crate never executes MCP
/// requests or automatically sends credentials to tool servers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum GrokTool {
    Function {
        name: String,
        description: String,
        parameters: Value,
    },
    WebSearch,
    XSearch {
        #[serde(skip_serializing_if = "Option::is_none")]
        allowed_x_handles: Option<Vec<String>>,
    },
    FileSearch {
        vector_store_ids: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        max_num_results: Option<u32>,
    },
    Mcp {
        #[serde(flatten)]
        properties: serde_json::Map<String, Value>,
    },
}
impl From<FunctionTool> for GrokTool {
    fn from(f: FunctionTool) -> Self {
        Self::Function {
            name: f.name,
            description: f.description,
            parameters: f.parameters,
        }
    }
}
/// Partial session update. Absent fields are omitted, including turn detection;
/// `Some(None)` explicitly sends JSON null to disable server VAD.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct SessionProperties {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "double_option"
    )]
    pub turn_detection: Option<Option<TurnDetection>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<GrokTool>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reasoning: Option<Reasoning>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub resumption: Option<SessionResumption>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub replace: Option<std::collections::BTreeMap<String, String>>,
}
fn double_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(d).map(Some)
}
impl SessionProperties {
    pub fn new() -> Self {
        Self::default()
    }
    /// Merge a locally validated partial update. Server IDs and connection
    /// model are observations/connection parameters, never mutable settings.
    pub(crate) fn apply_patch(&mut self, patch: &Self) {
        macro_rules! field {
            ($name:ident) => {
                if patch.$name.is_some() {
                    self.$name.clone_from(&patch.$name);
                }
            };
        }
        field!(instructions);
        field!(voice);
        field!(turn_detection);
        field!(tools);
        field!(reasoning);
        field!(resumption);
        field!(replace);
        if let Some(audio) = &patch.audio {
            let current = self.audio.get_or_insert_with(AudioConfiguration::default);
            if audio.input.is_some() {
                current.input.clone_from(&audio.input);
            }
            if audio.output.is_some() {
                current.output.clone_from(&audio.output);
            }
        }
    }
    pub fn pcm() -> Self {
        Self {
            voice: Some("eve".into()),
            turn_detection: Some(Some(TurnDetection::server_vad())),
            audio: Some(AudioConfiguration {
                input: Some(AudioInput::default()),
                output: Some(AudioOutput::default()),
            }),
            ..Self::default()
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionResumption {
    pub enabled: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ReasoningEffort {
    High,
    None,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Reasoning {
    pub effort: ReasoningEffort,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResponseOptions {
    /// Local correlation ID; never transmitted as an API parameter.
    #[serde(skip)]
    pub context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<Modality>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
}
impl Default for ResponseOptions {
    fn default() -> Self {
        Self {
            context_id: None,
            modalities: Some(vec![Modality::Text, Modality::Audio]),
            instructions: None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    System,
    Tool,
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
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ConversationItem {
    Message {
        id: String,
        role: Role,
        content: Vec<ItemContent>,
    },
    ForceMessage {
        id: String,
        role: Role,
        content: Vec<ItemContent>,
    },
    FunctionCall {
        id: String,
        call_id: String,
        name: String,
        #[serde(default, deserialize_with = "default_on_null")]
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
            | Self::ForceMessage { id, .. }
            | Self::FunctionCall { id, .. }
            | Self::FunctionCallOutput { id, .. } => id,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Action {
    /// Drain preceding commands and the active response, then close the session.
    Finish,
    UpdateSession(Box<SessionProperties>),
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
    TruncateItem(PlaybackPosition),
    CreateResponse(ResponseOptions),
    /// Ask to consume delivered results, as the upstream context aggregator does.
    RequestInference(ResponseOptions),
    CancelResponse,
    /// A pipeline result bound to the provider-admitted name and arguments.
    CheckedToolResult {
        call: pipecat_core::FunctionCall,
        output: Value,
        is_final: bool,
    },
    CancelTool {
        call_id: String,
        function_name: String,
    },
    /// Deliver immediately, without starting inference. Only an explicit
    /// CreateResponse (normally the upstream aggregator) creates a response.
    ToolResult {
        call_id: String,
        output: Value,
        is_final: bool,
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
    #[serde(default)]
    pub total_tokens: Option<u64>,
    #[serde(default)]
    pub input_tokens: Option<u64>,
    #[serde(default)]
    pub output_tokens: Option<u64>,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ContentPart {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(default)]
    pub transcript: Option<String>,
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
        item_id: String,
        #[serde(default)]
        content_index: Option<u32>,
        #[serde(default)]
        audio_end_ms: Option<u64>,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.updated")]
    InputTranscriptionUpdated {
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        content_index: u32,
        #[serde(rename = "transcript")]
        transcript: String,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.completed")]
    InputTranscriptionCompleted {
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
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
        #[serde(default)]
        audio_start_ms: u64,
    },
    #[serde(rename = "input_audio_buffer.speech_stopped")]
    SpeechStopped {
        item_id: String,
        #[serde(default)]
        audio_end_ms: u64,
    },
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
    ResponseDone {
        response: Response,
        #[serde(default)]
        usage: Option<Usage>,
    },
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
        part: ContentPart,
    },
    #[serde(rename = "response.content_part.done")]
    ContentPartDone {
        response_id: String,
        item_id: String,
        output_index: u32,
        content_index: u32,
        #[serde(default)]
        part: Option<ContentPart>,
    },
    #[serde(rename = "response.output_text.delta", alias = "response.text.delta")]
    TextDelta {
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        output_index: u32,
        #[serde(default, deserialize_with = "default_on_null")]
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
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        output_index: u32,
        #[serde(default, deserialize_with = "default_on_null")]
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "response.output_audio_transcript.done")]
    AudioTranscriptDone {
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        output_index: u32,
        #[serde(default, deserialize_with = "default_on_null")]
        content_index: u32,
        #[serde(default)]
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
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        output_index: u32,
        #[serde(default, deserialize_with = "default_on_null")]
        content_index: u32,
    },
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionArgumentsDelta {
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        output_index: u32,
        call_id: String,
        delta: String,
    },
    #[serde(rename = "response.function_call_arguments.done")]
    FunctionArgumentsDone {
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        output_index: u32,
        call_id: String,
        #[serde(default)]
        name: Option<String>,
        arguments: String,
    },
    #[serde(rename = "ping")]
    Ping { timestamp: u64 },
    #[serde(rename = "input_audio_buffer.timeout_triggered")]
    IdleTimeout {
        #[serde(default)]
        item_id: Option<String>,
    },
    #[serde(rename = "input_audio_buffer.dtmf_event_received")]
    Dtmf {
        #[serde(default)]
        digit: Option<String>,
    },
    #[serde(
        rename = "mcp_list_tools.in_progress",
        alias = "mcp_list_tools.completed",
        alias = "mcp_list_tools.failed",
        alias = "response.mcp_call.in_progress",
        alias = "response.mcp_call.completed",
        alias = "response.mcp_call.failed",
        alias = "response.mcp_call_arguments.delta",
        alias = "response.mcp_call_arguments.done"
    )]
    Mcp {
        #[serde(flatten)]
        properties: serde_json::Map<String, Value>,
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
            Self::ResponseCreated { response } | Self::ResponseDone { response, .. } => {
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
    pub event_type: String,
    pub session_id: Option<String>,
    pub conversation_id: Option<String>,
    pub context_id: Option<String>,
    pub generation: u64,
    pub event: Event,
}
pub(crate) struct WireEvent {
    pub event_id: String,
    pub event_type: String,
    pub event: Event,
}
pub(crate) fn parse_wire(
    text: &str,
    max_audio_bytes: usize,
) -> Result<WireEvent, crate::RealtimeError> {
    let mut value: Value = serde_json::from_str(text)
        .map_err(|_| crate::RealtimeError::Protocol("invalid event JSON"))?;
    fn identifiers(value: &Value, depth: usize) -> Result<(), crate::RealtimeError> {
        if depth > 8 {
            return Ok(());
        }
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if matches!(
                        key.as_str(),
                        "id" | "event_id"
                            | "response_id"
                            | "item_id"
                            | "call_id"
                            | "previous_item_id"
                            | "conversation_id"
                    ) && value.as_str().is_some_and(|v| v.len() > 256)
                    {
                        return Err(crate::RealtimeError::TooLarge);
                    }
                    if matches!(
                        key.as_str(),
                        "session"
                            | "response"
                            | "conversation"
                            | "item"
                            | "output"
                            | "error"
                            | "status_details"
                    ) {
                        identifiers(value, depth + 1)?;
                    }
                }
            }
            Value::Array(values) => {
                for value in values {
                    identifiers(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    identifiers(&value, 0)?;
    if value.get("type").and_then(Value::as_str) == Some("response.output_audio.delta") {
        let encoded = value
            .get("delta")
            .and_then(Value::as_str)
            .ok_or(crate::RealtimeError::Protocol("missing audio delta"))?;
        let padding = encoded
            .as_bytes()
            .iter()
            .rev()
            .take_while(|c| **c == b'=')
            .count()
            .min(2);
        let decoded = encoded
            .len()
            .div_ceil(4)
            .saturating_mul(3)
            .saturating_sub(padding);
        if decoded > max_audio_bytes {
            return Err(crate::RealtimeError::TooLarge);
        }
    }
    let event_id = value
        .get("event_id")
        .and_then(Value::as_str)
        .filter(|v| !v.is_empty())
        .ok_or(crate::RealtimeError::Protocol("missing event ID"))?
        .to_owned();
    let event_type = value
        .get("type")
        .and_then(Value::as_str)
        .ok_or(crate::RealtimeError::Protocol("missing event type"))?
        .to_owned();
    if let Some(object) = value.as_object_mut() {
        object.remove("event_id");
    }
    if event_type.len() > 128 {
        return Err(crate::RealtimeError::TooLarge);
    }
    let event = serde_json::from_value(value)
        .map_err(|_| crate::RealtimeError::Protocol("invalid typed event"))?;
    Ok(WireEvent {
        event_id,
        event_type,
        event,
    })
}

fn default_on_null<'de, D: serde::Deserializer<'de>, T: Deserialize<'de> + Default>(
    d: D,
) -> Result<T, D::Error> {
    Ok(Option::<T>::deserialize(d)?.unwrap_or_default())
}
