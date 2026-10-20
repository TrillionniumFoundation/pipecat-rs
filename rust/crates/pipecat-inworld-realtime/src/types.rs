use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// The native pipeline supports mono signed PCM16 only.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(deny_unknown_fields)]
pub enum AudioFormat {
    #[serde(rename = "audio/pcm")]
    Pcm { rate: u32 },
}
impl Default for AudioFormat {
    fn default() -> Self {
        Self::Pcm { rate: 24_000 }
    }
}
impl AudioFormat {
    pub fn sample_rate(&self) -> u32 {
        match self {
            Self::Pcm { rate } => *rate,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VadKind {
    ServerVad,
    SemanticVad,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnDetection {
    #[serde(rename = "type")]
    pub kind: VadKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eagerness: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub create_response: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interrupt_response: Option<bool>,
}
impl Default for TurnDetection {
    fn default() -> Self {
        Self {
            kind: VadKind::SemanticVad,
            eagerness: Some("low".into()),
            create_response: Some(true),
            interrupt_response: Some(true),
        }
    }
}
impl TurnDetection {
    pub fn server_vad() -> Self {
        Self {
            kind: VadKind::ServerVad,
            ..Self::default()
        }
    }
    pub fn semantic_vad() -> Self {
        Self::default()
    }
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TranscriptionConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
}
pub type InputTranscription = TranscriptionConfig;
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudioInput {
    #[serde(default)]
    pub format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcription: Option<TranscriptionConfig>,
    /// `Some(None)` explicitly disables provider VAD; omission is not manual mode.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "double_option"
    )]
    pub turn_detection: Option<Option<TurnDetection>>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AudioOutput {
    #[serde(default)]
    pub format: AudioFormat,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub voice: Option<String>,
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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
#[serde(deny_unknown_fields)]
pub struct FunctionTool {
    #[serde(rename = "type", default = "function_type")]
    kind: FunctionKind,
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FunctionKind {
    Function,
}
fn function_type() -> FunctionKind {
    FunctionKind::Function
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
#[serde(tag = "type", rename_all = "snake_case")]
#[serde(deny_unknown_fields)]
pub enum InworldTool {
    Function {
        name: String,
        description: String,
        parameters: Value,
    },
}
impl From<FunctionTool> for InworldTool {
    fn from(f: FunctionTool) -> Self {
        Self::Function {
            name: f.name,
            description: f.description,
            parameters: f.parameters,
        }
    }
}
/// Full desired session snapshot, not a generic merge patch. Runtime updates
/// replace this snapshot atomically. Provider metadata is copied before reserved
/// client-owned values are added to outgoing requests.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionProperties {
    #[serde(default, skip_serializing)]
    pub id: Option<String>,
    #[serde(rename = "type", skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub temperature: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output_modalities: Option<Vec<Modality>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<AudioConfiguration>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<InworldTool>>,
    #[serde(rename = "providerData", skip_serializing_if = "Option::is_none")]
    pub provider_data: Option<Map<String, Value>>,
}
impl SessionProperties {
    pub fn new() -> Self {
        Self::pcm()
    }
    pub fn pcm() -> Self {
        Self {
            kind: Some("realtime".into()),
            model: Some("openai/gpt-4.1-mini".into()),
            output_modalities: Some(vec![Modality::Audio, Modality::Text]),
            audio: Some(AudioConfiguration {
                input: Some(AudioInput {
                    format: AudioFormat::default(),
                    transcription: Some(TranscriptionConfig {
                        model: Some("inworld/inworld-stt-1".into()),
                    }),
                    turn_detection: Some(Some(TurnDetection::default())),
                }),
                output: Some(AudioOutput {
                    format: AudioFormat::default(),
                    model: Some("inworld-tts-2".into()),
                    voice: Some("Clive".into()),
                }),
            }),
            ..Self::default()
        }
    }
    pub fn manual() -> Self {
        let mut session = Self::pcm();
        session
            .audio
            .as_mut()
            .unwrap()
            .input
            .as_mut()
            .unwrap()
            .turn_detection = Some(None);
        session
    }
    pub fn is_manual(&self) -> bool {
        self.audio
            .as_ref()
            .and_then(|a| a.input.as_ref())
            .is_some_and(|i| matches!(i.turn_detection, Some(None)))
    }
    pub fn input_sample_rate(&self) -> Option<u32> {
        self.audio
            .as_ref()?
            .input
            .as_ref()
            .map(|i| i.format.sample_rate())
    }
    pub fn output_sample_rate(&self) -> Option<u32> {
        self.audio
            .as_ref()?
            .output
            .as_ref()
            .map(|o| o.format.sample_rate())
    }
    pub fn validate(&self) -> Result<(), crate::RealtimeError> {
        crate::client::validate_session(self)
    }
    pub(crate) fn wire_snapshot(&self) -> Self {
        let mut snapshot = self.clone();
        let data = snapshot.provider_data.get_or_insert_with(Map::new);
        data.insert("auto_tool_response".into(), Value::Bool(false));
        let metadata = data
            .entry("metadata")
            .or_insert_with(|| Value::Object(Map::new()));
        if let Some(metadata) = metadata.as_object_mut() {
            metadata.insert("sdk".into(), Value::String("pipecat-realtime".into()));
        }
        snapshot
    }
}
fn double_option<'de, D: serde::Deserializer<'de>, T: Deserialize<'de>>(
    d: D,
) -> Result<Option<Option<T>>, D::Error> {
    Option::<T>::deserialize(d).map(Some)
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResponseOptions {
    /// Local correlation ID; not an Inworld request parameter.
    #[serde(skip)]
    pub context_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modalities: Option<Vec<Modality>>,
}
impl Default for ResponseOptions {
    fn default() -> Self {
        Self {
            context_id: None,
            modalities: Some(vec![Modality::Text, Modality::Audio]),
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
            | Self::FunctionCall { id, .. }
            | Self::FunctionCallOutput { id, .. } => id,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize)]
pub enum Action {
    /// Flush valid PCM tail, drain preceding commands/active response, then close.
    Finish,
    UpdateSession(Box<SessionProperties>),
    AppendAudio(Vec<u8>),
    CommitAudio,
    ClearAudio,
    /// Drop unsent partial PCM only, without a provider request.
    DiscardBufferedAudio,
    CreateItem {
        item: ConversationItem,
        previous_item_id: Option<String>,
    },
    CreateResponse(ResponseOptions),
    RequestInference(ResponseOptions),
    CancelResponse,
    CheckedToolResult {
        call: pipecat_core::FunctionCall,
        output: Value,
        is_final: bool,
    },
    CancelTool {
        call_id: String,
        function_name: String,
    },
    /// Only final results are supported. Delivery never implicitly starts inference.
    ToolResult {
        call_id: String,
        output: Value,
        is_final: bool,
    },
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
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProviderError {
    #[serde(rename = "type", default, deserialize_with = "default_on_null")]
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
    SessionCreated {
        #[serde(default)]
        session: Option<SessionProperties>,
    },
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
    #[serde(rename = "conversation.item.input_audio_transcription.delta")]
    InputTranscriptionDelta {
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.completed")]
    InputTranscriptionCompleted {
        item_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        content_index: u32,
        transcript: String,
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
    #[serde(rename = "response.output_audio_transcript.delta")]
    AudioTranscriptDelta {
        response_id: String,
        item_id: String,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
        content_index: u32,
        delta: String,
    },
    #[serde(rename = "response.output_audio_transcript.done")]
    AudioTranscriptDone {
        response_id: String,
        item_id: String,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
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
        response_id: String,
        item_id: String,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
        content_index: u32,
    },
    #[serde(rename = "response.function_call_arguments.delta")]
    FunctionArgumentsDelta {
        #[serde(default, deserialize_with = "default_on_null")]
        response_id: String,
        #[serde(default, deserialize_with = "default_on_null")]
        item_id: String,
        #[serde(default)]
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
        #[serde(default)]
        output_index: u32,
        call_id: String,
        #[serde(default)]
        name: Option<String>,
        arguments: String,
    },
    #[serde(rename = "error")]
    Error { error: ProviderError },
    /// Unknown bounded messages are ignored, without retaining their payload.
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
pub struct InworldEvent {
    pub event_id: String,
    pub event_type: String,
    pub session_id: Option<String>,
    pub conversation_id: Option<String>,
    pub context_id: Option<String>,
    pub generation: u64,
    pub event: Event,
}
pub type RealtimeEvent = InworldEvent;
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
                    ) && value
                        .as_str()
                        .is_some_and(|v| v.is_empty() || v.len() > 256)
                    {
                        return Err(crate::RealtimeError::Protocol("invalid event identifier"));
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
    if event_type.len() > 128 {
        return Err(crate::RealtimeError::TooLarge);
    }
    if let Some(object) = value.as_object_mut() {
        object.remove("event_id");
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
