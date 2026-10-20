//! Azure Voice Live's dated, flat wire dialect. No OpenAI Realtime GA aliases.
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::config::VoiceLiveError;

pub const SAMPLE_RATE: u32 = 24_000;
pub const API_VERSION: &str = "2026-07-15";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Modality {
    Text,
    Audio,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioFormat {
    #[serde(rename = "pcm16")]
    Pcm16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureStandardVoice {
    #[serde(rename = "type")]
    kind: AzureStandardVoiceKind,
    pub name: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum AzureStandardVoiceKind {
    #[serde(rename = "azure-standard")]
    AzureStandard,
}
impl AzureStandardVoice {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            kind: AzureStandardVoiceKind::AzureStandard,
            name: name.into(),
        }
    }
}

/// This profile intentionally supports only Azure speech transcription.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputAudioTranscription {
    model: AzureSpeechModel,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum AzureSpeechModel {
    #[serde(rename = "azure-speech")]
    AzureSpeech,
}
impl Default for InputAudioTranscription {
    fn default() -> Self {
        Self {
            model: AzureSpeechModel::AzureSpeech,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InputAudioNoiseReduction {
    #[serde(rename = "type")]
    kind: NoiseReductionKind,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum NoiseReductionKind {
    #[serde(rename = "azure_deep_noise_suppression")]
    AzureDeepNoiseSuppression,
}
impl Default for InputAudioNoiseReduction {
    fn default() -> Self {
        Self {
            kind: NoiseReductionKind::AzureDeepNoiseSuppression,
        }
    }
}

/// Explicit ownership settings are not optional: the provider creates and
/// interrupts semantic responses, but only actual native playback receipts may
/// authorize client truncation. Validation rejects any other combination.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AzureSemanticVad {
    #[serde(rename = "type")]
    kind: AzureSemanticVadKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub threshold: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub prefix_padding_ms: Option<u32>,
    pub silence_duration_ms: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speech_duration_ms: Option<u32>,
    pub create_response: bool,
    pub interrupt_response: bool,
    pub auto_truncate: bool,
    pub remove_filler_words: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
enum AzureSemanticVadKind {
    #[serde(rename = "azure_semantic_vad")]
    AzureSemanticVad,
}
impl Default for AzureSemanticVad {
    fn default() -> Self {
        Self {
            kind: AzureSemanticVadKind::AzureSemanticVad,
            threshold: None,
            prefix_padding_ms: None,
            silence_duration_ms: 500,
            speech_duration_ms: None,
            create_response: true,
            interrupt_response: true,
            auto_truncate: false,
            remove_filler_words: true,
        }
    }
}

/// Manual mode serializes as JSON null, never as false or an omitted field.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(untagged)]
pub enum TurnDetection {
    #[default]
    Manual,
    AzureSemanticVad(AzureSemanticVad),
}
impl TurnDetection {
    pub fn semantic_vad() -> Self {
        Self::AzureSemanticVad(AzureSemanticVad::default())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    #[default]
    Auto,
    None,
    Required,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FunctionTool {
    #[serde(rename = "type")]
    kind: FunctionKind,
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum FunctionKind {
    Function,
}
impl FunctionTool {
    pub fn new(name: impl Into<String>, description: impl Into<String>, parameters: Value) -> Self {
        Self {
            kind: FunctionKind::Function,
            name: name.into(),
            description: description.into(),
            parameters,
        }
    }
}

/// Immutable application profile. The connection model is deliberately absent.
/// Unsupported voice, format, transcription, VAD, and GA fields are rejected
/// rather than silently accepted and ignored.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionProperties {
    pub modalities: [Modality; 2],
    pub voice: AzureStandardVoice,
    pub input_audio_format: AudioFormat,
    pub input_audio_sampling_rate: u32,
    pub output_audio_format: AudioFormat,
    pub turn_detection: TurnDetection,
    pub input_audio_transcription: InputAudioTranscription,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub input_audio_noise_reduction: Option<InputAudioNoiseReduction>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instructions: Option<String>,
    #[serde(default)]
    pub tools: Vec<FunctionTool>,
    #[serde(default)]
    pub tool_choice: ToolChoice,
}
impl SessionProperties {
    pub fn new(voice: impl Into<String>) -> Self {
        Self {
            modalities: [Modality::Text, Modality::Audio],
            voice: AzureStandardVoice::new(voice),
            input_audio_format: AudioFormat::Pcm16,
            input_audio_sampling_rate: SAMPLE_RATE,
            output_audio_format: AudioFormat::Pcm16,
            turn_detection: TurnDetection::Manual,
            input_audio_transcription: InputAudioTranscription::default(),
            input_audio_noise_reduction: None,
            instructions: None,
            tools: Vec::new(),
            tool_choice: ToolChoice::Auto,
        }
    }
    pub fn is_manual(&self) -> bool {
        matches!(self.turn_detection, TurnDetection::Manual)
    }
    pub fn validate(&self) -> Result<(), VoiceLiveError> {
        if self.modalities != [Modality::Text, Modality::Audio]
            || self.input_audio_sampling_rate != SAMPLE_RATE
        {
            return Err(VoiceLiveError::Invalid(
                "fixed text/audio 24 kHz PCM profile",
            ));
        }
        valid_config_text(&self.voice.name, 256, false)?;
        if let Some(text) = &self.instructions {
            valid_config_text(text, 65_536, true)?;
        }
        if let TurnDetection::AzureSemanticVad(vad) = &self.turn_detection {
            if !vad.create_response
                || !vad.interrupt_response
                || vad.auto_truncate
                || vad
                    .threshold
                    .is_some_and(|v| !v.is_finite() || !(0.0..=1.0).contains(&v))
                || !(1..=10_000).contains(&vad.silence_duration_ms)
                || vad.prefix_padding_ms.is_some_and(|v| v > 5_000)
                || vad.speech_duration_ms.is_some_and(|v| v > 5_000)
            {
                return Err(VoiceLiveError::Invalid("semantic VAD bounds or ownership"));
            }
        }
        if self.tools.len() > 128 {
            return Err(VoiceLiveError::TooLarge);
        }
        let mut names = std::collections::HashSet::new();
        for tool in &self.tools {
            if tool.name.is_empty()
                || tool.name.len() > 64
                || !tool
                    .name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-'))
                || !names.insert(&tool.name)
                || !tool.parameters.is_object()
                || serde_json::to_vec(&tool.parameters)
                    .map_err(|_| VoiceLiveError::Invalid("tool schema"))?
                    .len()
                    > 65_536
            {
                return Err(VoiceLiveError::Invalid(
                    "function tool schema, name, or duplicate",
                ));
            }
            valid_config_text(&tool.description, 16_384, true)?;
        }
        if self.tool_choice == ToolChoice::Required && self.tools.is_empty() {
            return Err(VoiceLiveError::Invalid("required tool choice has no tools"));
        }
        Ok(())
    }
}
fn valid_config_text(value: &str, max: usize, multiline: bool) -> Result<(), VoiceLiveError> {
    if value.len() > max
        || (!multiline && value.trim().is_empty())
        || value
            .chars()
            .any(|c| c.is_control() && !(multiline && matches!(c, '\n' | '\r' | '\t')))
    {
        return Err(VoiceLiveError::Invalid(
            "session text bounds or control characters",
        ));
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    User,
    Assistant,
    System,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemKind {
    Message,
    FunctionCall,
    FunctionCallOutput,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ItemObject {
    #[serde(rename = "realtime.item")]
    RealtimeItem,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ItemStatus {
    Completed,
    InProgress,
    Incomplete,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ContentKind {
    InputText,
    InputAudio,
    Text,
    Audio,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ItemContent {
    #[serde(rename = "type")]
    pub kind: ContentKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audio: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub transcript: Option<String>,
}
impl ItemContent {
    pub fn input_text(text: impl Into<String>) -> Self {
        Self {
            kind: ContentKind::InputText,
            text: Some(text.into()),
            audio: None,
            transcript: None,
        }
    }
}
/// Optional wire IDs remain optional; only the actor can bind omissions to
/// previously announced and unambiguous response/item/call authority.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ConversationItem {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub object: Option<ItemObject>,
    #[serde(rename = "type")]
    pub kind: ItemKind,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<ItemStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub role: Option<Role>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<ItemContent>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub call_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub output: Option<String>,
}
impl ConversationItem {
    pub fn user_text(text: impl Into<String>) -> Self {
        Self {
            id: None,
            object: None,
            kind: ItemKind::Message,
            status: None,
            role: Some(Role::User),
            content: Some(vec![ItemContent::input_text(text)]),
            call_id: None,
            name: None,
            arguments: None,
            output: None,
        }
    }
    pub fn function_output(call_id: impl Into<String>, output: impl Into<String>) -> Self {
        Self {
            id: None,
            object: None,
            kind: ItemKind::FunctionCallOutput,
            status: None,
            role: None,
            content: None,
            call_id: Some(call_id.into()),
            name: None,
            arguments: None,
            output: Some(output.into()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type")]
pub enum ClientEvent {
    #[serde(rename = "session.update")]
    SessionUpdate {
        event_id: String,
        session: SessionProperties,
    },
    #[serde(rename = "input_audio_buffer.append")]
    InputAudioBufferAppend { event_id: String, audio: String },
    #[serde(rename = "input_audio_buffer.commit")]
    InputAudioBufferCommit { event_id: String },
    #[serde(rename = "input_audio_buffer.clear")]
    InputAudioBufferClear { event_id: String },
    #[serde(rename = "conversation.item.create")]
    ConversationItemCreate {
        event_id: String,
        item: ConversationItem,
        #[serde(skip_serializing_if = "Option::is_none")]
        previous_item_id: Option<String>,
    },
    #[serde(rename = "conversation.item.truncate")]
    ConversationItemTruncate {
        event_id: String,
        item_id: String,
        content_index: u32,
        audio_end_ms: u64,
    },
    #[serde(rename = "response.create")]
    ResponseCreate { event_id: String },
    #[serde(rename = "response.cancel")]
    ResponseCancel { event_id: String },
}

#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct SessionInfo {
    pub id: Option<String>,
    pub model: Option<String>,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResponseStatus {
    InProgress,
    Completed,
    Cancelled,
    Incomplete,
    Failed,
}
#[derive(Debug, Clone, PartialEq, Deserialize, Default)]
pub struct ResponseInfo {
    pub id: Option<String>,
    pub status: Option<ResponseStatus>,
    pub output: Option<Vec<ConversationItem>>,
    pub usage: Option<TokenUsage>,
    pub status_details: Option<ResponseStatusDetails>,
}
#[derive(Clone, PartialEq, Eq, Deserialize, Default)]
pub struct ResponseStatusDetails {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub reason: Option<String>,
    pub error: Option<ProviderError>,
}
impl std::fmt::Debug for ResponseStatusDetails {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ResponseStatusDetails([REDACTED])")
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct TokenUsageDetails {
    pub cached_tokens: Option<u64>,
    pub text_tokens: Option<u64>,
    pub audio_tokens: Option<u64>,
}
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Default)]
pub struct TokenUsage {
    pub total_tokens: Option<u64>,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub input_token_details: Option<TokenUsageDetails>,
    pub output_token_details: Option<TokenUsageDetails>,
}
/// Provider messages are untrusted diagnostics, not safe user-facing errors.
/// Never interpolate these fields into `VoiceLiveError` or logs.
#[derive(Clone, PartialEq, Eq, Deserialize, Default)]
pub struct ProviderError {
    #[serde(rename = "type")]
    pub kind: Option<String>,
    pub code: Option<String>,
    pub message: Option<String>,
    pub param: Option<String>,
    pub event_id: Option<String>,
}
impl std::fmt::Debug for ProviderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ProviderError([REDACTED])")
    }
}

/// Exact pinned Voice Live names. Unknown events retain no arbitrary payload.
/// GA spellings intentionally become Unknown, never output progress.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type")]
pub enum ServerEvent {
    #[serde(rename = "session.created")]
    SessionCreated {
        event_id: Option<String>,
        session: SessionInfo,
    },
    #[serde(rename = "session.updated")]
    SessionUpdated {
        event_id: Option<String>,
        session: SessionInfo,
    },
    #[serde(rename = "conversation.item.created")]
    ConversationItemCreated {
        event_id: Option<String>,
        item: ConversationItem,
        previous_item_id: Option<String>,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.delta")]
    InputAudioTranscriptionDelta {
        event_id: Option<String>,
        item_id: String,
        #[serde(default)]
        content_index: u32,
        delta: Option<String>,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.completed")]
    InputAudioTranscriptionCompleted {
        event_id: Option<String>,
        item_id: String,
        #[serde(default)]
        content_index: u32,
        #[serde(default)]
        transcript: String,
    },
    #[serde(rename = "conversation.item.input_audio_transcription.failed")]
    InputAudioTranscriptionFailed {
        event_id: Option<String>,
        item_id: String,
        #[serde(default)]
        content_index: u32,
        error: Option<ProviderError>,
    },
    #[serde(rename = "input_audio_buffer.speech_started")]
    SpeechStarted {
        event_id: Option<String>,
        audio_start_ms: Option<u64>,
        item_id: Option<String>,
    },
    #[serde(rename = "input_audio_buffer.speech_stopped")]
    SpeechStopped {
        event_id: Option<String>,
        audio_end_ms: Option<u64>,
        item_id: Option<String>,
    },
    #[serde(rename = "response.created")]
    ResponseCreated {
        event_id: Option<String>,
        response: ResponseInfo,
    },
    #[serde(rename = "response.done")]
    ResponseDone {
        event_id: Option<String>,
        response: ResponseInfo,
    },
    #[serde(rename = "response.output_item.added")]
    OutputItemAdded {
        event_id: Option<String>,
        response_id: Option<String>,
        #[serde(default)]
        output_index: u32,
        item: ConversationItem,
    },
    #[serde(rename = "response.text.delta")]
    TextDelta {
        event_id: Option<String>,
        response_id: Option<String>,
        item_id: Option<String>,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
        content_index: u32,
        #[serde(default)]
        delta: String,
    },
    #[serde(rename = "response.audio.delta")]
    AudioDelta {
        event_id: Option<String>,
        response_id: Option<String>,
        item_id: Option<String>,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
        content_index: u32,
        #[serde(default)]
        delta: String,
    },
    #[serde(rename = "response.audio.done")]
    AudioDone {
        event_id: Option<String>,
        response_id: Option<String>,
        item_id: Option<String>,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
        content_index: u32,
    },
    #[serde(rename = "response.audio_transcript.delta")]
    AudioTranscriptDelta {
        event_id: Option<String>,
        response_id: Option<String>,
        item_id: Option<String>,
        #[serde(default)]
        output_index: u32,
        #[serde(default)]
        content_index: u32,
        #[serde(default)]
        delta: String,
    },
    #[serde(rename = "response.function_call_arguments.done")]
    FunctionCallArgumentsDone {
        event_id: Option<String>,
        response_id: Option<String>,
        item_id: Option<String>,
        #[serde(default)]
        output_index: u32,
        call_id: Option<String>,
        name: Option<String>,
        #[serde(default)]
        arguments: String,
    },
    #[serde(rename = "error")]
    Error {
        event_id: Option<String>,
        error: ProviderError,
    },
    #[serde(other)]
    Unknown,
}

/// Parse bounded JSON without exposing parser input or parser error details.
/// Required fields and types follow the pinned models; omitted optional IDs
/// confer no authority and must still pass the actor's correlation checks.
pub fn parse_server_event(data: &[u8], max_bytes: usize) -> Result<ServerEvent, VoiceLiveError> {
    if data.len() > max_bytes {
        return Err(VoiceLiveError::TooLarge);
    }
    let event: ServerEvent = serde_json::from_slice(data)
        .map_err(|_| VoiceLiveError::Protocol("invalid Voice Live event"))?;
    event.validate_identifiers()?;
    Ok(event)
}
impl ServerEvent {
    fn validate_identifiers(&self) -> Result<(), VoiceLiveError> {
        fn id(value: Option<&str>) -> Result<(), VoiceLiveError> {
            if value.is_some_and(|s| {
                s.trim().is_empty() || s.len() > 256 || s.chars().any(char::is_control)
            }) {
                return Err(VoiceLiveError::Protocol(
                    "invalid event correlation identifier",
                ));
            }
            Ok(())
        }
        fn item(value: &ConversationItem) -> Result<(), VoiceLiveError> {
            id(value.id.as_deref())?;
            id(value.call_id.as_deref())?;
            id(value.name.as_deref())
        }
        let event_id = match self {
            Self::SessionCreated { event_id, session }
            | Self::SessionUpdated { event_id, session } => {
                id(session.id.as_deref())?;
                event_id
            }
            Self::ConversationItemCreated {
                event_id,
                item: value,
                previous_item_id,
            } => {
                item(value)?;
                // The source fixture explicitly allows "" for the first item.
                if previous_item_id.as_deref() != Some("") {
                    id(previous_item_id.as_deref())?;
                }
                event_id
            }
            Self::InputAudioTranscriptionDelta {
                event_id, item_id, ..
            }
            | Self::InputAudioTranscriptionCompleted {
                event_id, item_id, ..
            }
            | Self::InputAudioTranscriptionFailed {
                event_id, item_id, ..
            } => {
                id(Some(item_id))?;
                event_id
            }
            Self::SpeechStarted {
                event_id, item_id, ..
            }
            | Self::SpeechStopped {
                event_id, item_id, ..
            } => {
                id(item_id.as_deref())?;
                event_id
            }
            Self::ResponseCreated { event_id, response }
            | Self::ResponseDone { event_id, response } => {
                id(response.id.as_deref())?;
                if let Some(items) = &response.output {
                    for value in items {
                        item(value)?;
                    }
                }
                event_id
            }
            Self::OutputItemAdded {
                event_id,
                response_id,
                item: value,
                ..
            } => {
                id(response_id.as_deref())?;
                item(value)?;
                event_id
            }
            Self::TextDelta {
                event_id,
                response_id,
                item_id,
                ..
            }
            | Self::AudioDelta {
                event_id,
                response_id,
                item_id,
                ..
            }
            | Self::AudioDone {
                event_id,
                response_id,
                item_id,
                ..
            }
            | Self::AudioTranscriptDelta {
                event_id,
                response_id,
                item_id,
                ..
            } => {
                id(response_id.as_deref())?;
                id(item_id.as_deref())?;
                event_id
            }
            Self::FunctionCallArgumentsDone {
                event_id,
                response_id,
                item_id,
                call_id,
                name,
                ..
            } => {
                id(response_id.as_deref())?;
                id(item_id.as_deref())?;
                id(call_id.as_deref())?;
                id(name.as_deref())?;
                event_id
            }
            Self::Error { event_id, error } => {
                id(error.event_id.as_deref())?;
                event_id
            }
            Self::Unknown => return Ok(()),
        };
        id(event_id.as_deref())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn parse(value: Value) -> Result<ServerEvent, VoiceLiveError> {
        parse_server_event(&serde_json::to_vec(&value).unwrap(), 1024 * 1024)
    }

    #[test]
    fn manual_session_is_flat_and_has_explicit_null_vad() {
        let event = ClientEvent::SessionUpdate {
            event_id: "event-1".into(),
            session: SessionProperties::new("en-US-Synthetic:Voice&Name"),
        };
        let value = serde_json::to_value(event).unwrap();
        assert_eq!(
            value,
            json!({
                "type":"session.update", "event_id":"event-1", "session":{
                    "modalities":["text","audio"],
                    "voice":{"type":"azure-standard","name":"en-US-Synthetic:Voice&Name"},
                    "input_audio_format":"pcm16", "input_audio_sampling_rate":24000,
                    "output_audio_format":"pcm16", "turn_detection":null,
                    "input_audio_transcription":{"model":"azure-speech"},
                    "tools":[], "tool_choice":"auto"
                }
            })
        );
    }

    #[test]
    fn semantic_session_declares_server_response_but_native_truncation() {
        let mut session = SessionProperties::new("synthetic-voice");
        session.turn_detection = TurnDetection::semantic_vad();
        session.input_audio_noise_reduction = Some(InputAudioNoiseReduction::default());
        session.validate().unwrap();
        let value = serde_json::to_value(&session).unwrap();
        assert_eq!(
            value["turn_detection"],
            json!({
                "type":"azure_semantic_vad", "silence_duration_ms":500,
                "create_response":true, "interrupt_response":true,
                "auto_truncate":false, "remove_filler_words":true
            })
        );
        assert_eq!(
            value["input_audio_noise_reduction"],
            json!({"type":"azure_deep_noise_suppression"})
        );
        assert_eq!(
            serde_json::from_value::<SessionProperties>(value).unwrap(),
            session
        );
    }

    #[test]
    fn immutable_profile_rejects_ga_and_unsupported_settings() {
        let base = serde_json::to_value(SessionProperties::new("voice")).unwrap();
        for (key, value) in [
            ("model", json!("must-be-in-url")),
            ("type", json!("realtime")),
            ("audio", json!({})),
            ("output_modalities", json!(["audio"])),
            ("input_audio_echo_cancellation", json!({})),
            ("temperature", json!(0.5)),
        ] {
            let mut value_with_field = base.clone();
            value_with_field[key] = value;
            assert!(serde_json::from_value::<SessionProperties>(value_with_field).is_err());
        }
        for (key, value) in [
            ("voice", json!("alloy")),
            ("voice", json!({"type":"openai","name":"alloy"})),
            ("input_audio_format", json!("g711_ulaw")),
            ("output_audio_format", json!("pcm16_16000hz")),
            ("input_audio_transcription", json!({"model":"whisper-1"})),
            ("turn_detection", json!(false)),
            ("turn_detection", json!({"type":"server_vad"})),
        ] {
            let mut wrong = base.clone();
            wrong[key] = value;
            assert!(serde_json::from_value::<SessionProperties>(wrong).is_err());
        }
        let mut missing = base;
        missing.as_object_mut().unwrap().remove("turn_detection");
        assert!(serde_json::from_value::<SessionProperties>(missing).is_err());
    }

    #[test]
    fn profile_rate_modality_voice_and_vad_bounds_are_enforced() {
        let mut session = SessionProperties::new("voice");
        session.input_audio_sampling_rate = 16000;
        assert!(session.validate().is_err());
        session.input_audio_sampling_rate = SAMPLE_RATE;
        session.modalities = [Modality::Audio, Modality::Audio];
        assert!(session.validate().is_err());
        session.modalities = [Modality::Text, Modality::Audio];
        session.voice.name.clear();
        assert!(session.validate().is_err());
        session.voice.name = "voice".into();
        for vad in [
            AzureSemanticVad {
                threshold: Some(f64::NAN),
                ..Default::default()
            },
            AzureSemanticVad {
                threshold: Some(1.1),
                ..Default::default()
            },
            AzureSemanticVad {
                silence_duration_ms: 0,
                ..Default::default()
            },
            AzureSemanticVad {
                prefix_padding_ms: Some(u32::MAX),
                ..Default::default()
            },
            AzureSemanticVad {
                speech_duration_ms: Some(u32::MAX),
                ..Default::default()
            },
            AzureSemanticVad {
                create_response: false,
                ..Default::default()
            },
            AzureSemanticVad {
                interrupt_response: false,
                ..Default::default()
            },
            AzureSemanticVad {
                auto_truncate: true,
                ..Default::default()
            },
        ] {
            session.turn_detection = TurnDetection::AzureSemanticVad(vad);
            assert!(session.validate().is_err());
        }
    }

    #[test]
    fn functions_require_bounded_unique_names_and_object_schemas() {
        let mut session = SessionProperties::new("voice");
        session.tool_choice = ToolChoice::Required;
        assert!(session.validate().is_err());
        let tool = FunctionTool::new(
            "get_weather",
            "Weather",
            json!({"type":"object","properties":{}}),
        );
        session.tools.push(tool.clone());
        session.validate().unwrap();
        assert_eq!(serde_json::to_value(&tool).unwrap()["type"], "function");
        session.tools.push(tool);
        assert!(session.validate().is_err());
        session.tools.pop();
        session.tools[0].name = "not an identifier".into();
        assert!(session.validate().is_err());
        session.tools[0].name = "valid".into();
        session.tools[0].parameters = json!([]);
        assert!(session.validate().is_err());
    }

    #[test]
    fn ordinary_response_create_has_no_response_overrides() {
        assert_eq!(
            serde_json::to_value(ClientEvent::ResponseCreate {
                event_id: "request-1".into()
            })
            .unwrap(),
            json!({"type":"response.create","event_id":"request-1"})
        );
    }

    #[test]
    fn final_tool_output_keeps_preencoded_string_and_call_id() {
        let item = ConversationItem::function_output("call-1", "{\"answer\":42}");
        let event = ClientEvent::ConversationItemCreate {
            event_id: "event-2".into(),
            item,
            previous_item_id: None,
        };
        assert_eq!(
            serde_json::to_value(event).unwrap(),
            json!({
                "type":"conversation.item.create", "event_id":"event-2",
                "item":{"type":"function_call_output","call_id":"call-1","output":"{\"answer\":42}"}
            })
        );
    }

    #[test]
    fn text_item_has_no_fabricated_server_identity() {
        assert_eq!(
            serde_json::to_value(ConversationItem::user_text("hello")).unwrap(),
            json!({
                "type":"message","role":"user","content":[{"type":"input_text","text":"hello"}]
            })
        );
    }

    #[test]
    fn source_first_item_fixture_allows_empty_previous_id() {
        let event = parse(json!({"event_id":"event_2","type":"conversation.item.created","previous_item_id":"",
            "item":{"id":"item_1","object":"realtime.item","type":"message","status":"completed",
                "role":"user","content":[{"type":"input_text","text":"Say hello in five words."}]}})).unwrap();
        let ServerEvent::ConversationItemCreated {
            item,
            previous_item_id,
            ..
        } = event
        else {
            panic!()
        };
        assert_eq!(previous_item_id.as_deref(), Some(""));
        assert_eq!(item.role, Some(Role::User));
        assert_eq!(
            item.content.unwrap()[0].text.as_deref(),
            Some("Say hello in five words.")
        );
    }

    #[test]
    fn source_incomplete_announcement_does_not_require_content() {
        let event = parse(
            json!({"type":"response.output_item.added","response_id":"resp_1","output_index":0,
            "item":{"id":"msg_1","object":"realtime.item","type":"message","status":"incomplete",
                "role":"assistant","content":[]}}),
        )
        .unwrap();
        let ServerEvent::OutputItemAdded { item, .. } = event else {
            panic!()
        };
        assert_eq!(item.status, Some(ItemStatus::Incomplete));
        assert_eq!(item.content, Some(vec![]));
    }

    #[test]
    fn source_usage_retains_supported_breakdown_and_ignores_additions() {
        let event = parse(json!({"event_id":"event_6","type":"response.done","response":{
            "object":"realtime.response","id":"resp_1","status":"completed","status_details":null,
            "output":[{"id":"msg_1","object":"realtime.item","type":"message","status":"completed",
                "role":"assistant","content":[{"type":"audio","transcript":"Hello! How are you today?"}]}],
            "usage":{"total_tokens":65,"input_tokens":23,"output_tokens":42,
                "input_token_details":{"cached_tokens":0,"text_tokens":23,"audio_tokens":0,"image_tokens":0},
                "output_token_details":{"text_tokens":10,"audio_tokens":32}}
        }})).unwrap();
        let ServerEvent::ResponseDone { response, .. } = event else {
            panic!()
        };
        assert_eq!(response.status, Some(ResponseStatus::Completed));
        let usage = response.usage.unwrap();
        assert_eq!(
            (usage.total_tokens, usage.input_tokens, usage.output_tokens),
            (Some(65), Some(23), Some(42))
        );
        assert_eq!(usage.input_token_details.unwrap().cached_tokens, Some(0));
    }

    #[test]
    fn optional_correlation_fields_remain_absent_not_invented() {
        let event = parse(json!({"type":"response.function_call_arguments.done","arguments":"{}"}))
            .unwrap();
        let ServerEvent::FunctionCallArgumentsDone {
            event_id,
            response_id,
            item_id,
            call_id,
            name,
            ..
        } = event
        else {
            panic!()
        };
        assert_eq!(
            (event_id, response_id, item_id, call_id, name),
            (None, None, None, None, None)
        );
        let event = parse(json!({"type":"response.audio.delta","delta":"AAA="})).unwrap();
        let ServerEvent::AudioDelta {
            response_id,
            item_id,
            content_index,
            ..
        } = event
        else {
            panic!()
        };
        assert_eq!((response_id, item_id, content_index), (None, None, 0));
    }

    #[test]
    fn session_echo_does_not_require_optional_model_or_id() {
        for event_type in ["session.created", "session.updated"] {
            assert!(parse(json!({"type":event_type,"session":{}})).is_ok());
            assert!(parse(
                json!({"type":event_type,"session":{"model":"canonical-alias","extra":42}})
            )
            .is_ok());
            assert!(parse(json!({"type":event_type,"session":null})).is_err());
        }
    }

    #[test]
    fn unknown_and_ga_events_are_not_voice_live_progress() {
        for name in [
            "session.avatar.switch_to_speaking",
            "response.output_audio.delta",
            "response.output_audio.done",
            "response.output_audio_transcript.delta",
            "conversation.item.added",
        ] {
            assert_eq!(
                parse(json!({"type":name,"delta":"unretained","anything":{"secret":"discard"}}))
                    .unwrap(),
                ServerEvent::Unknown
            );
        }
    }

    #[test]
    fn recognized_malformed_events_fail_with_no_payload_echo() {
        for value in [
            json!({"type":"session.created"}),
            json!({"type":"response.audio.delta","delta":42}),
            json!({"type":"response.audio.done","content_index":-1}),
            json!({"type":"response.audio.delta","response_id":42}),
            json!({"type":"response.audio.delta","item_id":""}),
            json!({"type":"response.function_call_arguments.done","arguments":{}}),
            json!({"type":"conversation.item.input_audio_transcription.completed"}),
            json!({"type":"conversation.item.created","item":{"type":"bogus"}}),
            json!({"type":"conversation.item.created","item":{"type":"message","object":"wrong"}}),
            json!({"type":"response.done","response":{"usage":{"total_tokens":-1}}}),
            json!({"type":"error","error":"invented-secret"}),
            json!({"type":42}),
            json!({}),
            json!([]),
        ] {
            let error = parse(value).unwrap_err();
            assert!(!format!("{error:?} {error}").contains("invented-secret"));
        }
    }

    #[test]
    fn parser_enforces_wire_size_before_deserialization() {
        let raw = br#"{"type":"session.avatar.switch_to_speaking"}"#;
        assert_eq!(
            parse_server_event(raw, raw.len() - 1),
            Err(VoiceLiveError::TooLarge)
        );
        assert_eq!(
            parse_server_event(raw, raw.len()).unwrap(),
            ServerEvent::Unknown
        );
        assert!(parse_server_event(b"{invented-secret", 1024).is_err());
    }

    #[test]
    fn nested_error_request_identity_is_separate_and_diagnostics_are_redacted() {
        let event = parse(json!({"type":"error","event_id":"outer-1","error":{
            "type":"invalid_request_error","code":"response_cancel_not_active","event_id":"request-2",
            "message":"invented-secret","param":"invented-secret"}})).unwrap();
        assert!(!format!("{event:?}").contains("invented-secret"));
        let ServerEvent::Error { event_id, error } = event else {
            panic!()
        };
        assert_eq!(event_id.as_deref(), Some("outer-1"));
        assert_eq!(error.event_id.as_deref(), Some("request-2"));
        assert_eq!(error.code.as_deref(), Some("response_cancel_not_active"));
    }

    #[test]
    fn exact_remaining_source_event_names_parse() {
        for value in [
            json!({"type":"conversation.item.input_audio_transcription.delta","item_id":"user-1","delta":"Hi"}),
            json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user-1","transcript":"Hi"}),
            json!({"type":"conversation.item.input_audio_transcription.failed","item_id":"user-1","error":{}}),
            json!({"type":"input_audio_buffer.speech_started","audio_start_ms":12}),
            json!({"type":"input_audio_buffer.speech_stopped","audio_end_ms":34}),
            json!({"type":"response.created","response":{"id":"r-1","status":"in_progress"}}),
            json!({"type":"response.audio.done"}),
            json!({"type":"response.audio_transcript.delta","delta":"Hi"}),
            json!({"type":"response.text.delta","delta":"Hi"}),
        ] {
            assert!(!matches!(parse(value).unwrap(), ServerEvent::Unknown));
        }
    }
}
