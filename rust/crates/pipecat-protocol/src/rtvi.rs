//! RTVI 2.2.0 envelopes and payloads, matching the Python RTVI models.
use crate::{check_size, DtmfDigit, ProtocolError, DEFAULT_MAX_MESSAGE_BYTES};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Map, Value};
pub const MESSAGE_LABEL: &str = "rtvi-ai";
pub const PROTOCOL_VERSION: &str = "2.2.0";

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Envelope {
    pub label: String,
    #[serde(rename = "type")]
    pub message_type: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub data: Option<Value>,
}
impl Envelope {
    pub fn parse(bytes: &[u8]) -> Result<Self, ProtocolError> {
        Self::parse_with_limit(bytes, DEFAULT_MAX_MESSAGE_BYTES)
    }
    pub fn parse_with_limit(bytes: &[u8], limit: usize) -> Result<Self, ProtocolError> {
        check_size(bytes.len(), limit)?;
        let message: Self = serde_json::from_slice(bytes)?;
        message.validate()?;
        Ok(message)
    }
    pub fn validate(&self) -> Result<(), ProtocolError> {
        if self.label != MESSAGE_LABEL {
            return Err(ProtocolError::InvalidField("RTVI label"));
        }
        if self.message_type.is_empty() {
            return Err(ProtocolError::InvalidField("RTVI type"));
        }
        Ok(())
    }
    pub fn encode(&self) -> Result<String, ProtocolError> {
        self.validate()?;
        let text = serde_json::to_string(self)?;
        check_size(text.len(), DEFAULT_MAX_MESSAGE_BYTES)?;
        Ok(text)
    }
    pub fn client(id: impl Into<String>, command: &ClientCommand) -> Result<Self, ProtocolError> {
        let mut envelope = Self::from_tagged(command)?;
        envelope.id = Some(id.into());
        envelope.client_command()?;
        Ok(envelope)
    }
    pub fn server(event: &ServerEvent) -> Result<Self, ProtocolError> {
        event.validate()?;
        let envelope = Self::from_tagged(event)?;
        envelope.server_event()?;
        Ok(envelope)
    }
    pub fn response(id: impl Into<String>, event: &ServerEvent) -> Result<Self, ProtocolError> {
        event.validate()?;
        let mut envelope = Self::from_tagged(event)?;
        envelope.id = Some(id.into());
        envelope.server_event()?;
        Ok(envelope)
    }
    fn from_tagged<T: Serialize>(value: &T) -> Result<Self, ProtocolError> {
        let mut tagged = serde_json::to_value(value)?;
        let map = tagged
            .as_object_mut()
            .ok_or(ProtocolError::InvalidField("RTVI message"))?;
        map.insert("label".into(), Value::String(MESSAGE_LABEL.into()));
        Ok(serde_json::from_value(tagged)?)
    }
    fn tagged<T: DeserializeOwned>(&self) -> Result<T, ProtocolError> {
        self.validate()?;
        Ok(serde_json::from_value(
            json!({"type":self.message_type,"data":self.data}),
        )?)
    }
    pub fn client_command(&self) -> Result<ClientCommand, ProtocolError> {
        self.validate()?;
        self.id
            .as_ref()
            .ok_or(ProtocolError::MissingField("RTVI id"))?;
        if self.message_type == "client-ready" {
            let data = self.data.as_ref().and_then(Value::as_object);
            let version = data
                .and_then(|d| d.get("version"))
                .and_then(Value::as_str)
                .map(str::to_owned);
            let about = data
                .and_then(|d| d.get("about"))
                .and_then(|v| serde_json::from_value(v.clone()).ok());
            return Ok(ClientCommand::ClientReady(ClientReadyData {
                version,
                about,
            }));
        }
        let command: ClientCommand = self.tagged()?;
        command.validate()?;
        Ok(command)
    }
    pub fn server_event(&self) -> Result<ServerEvent, ProtocolError> {
        let event: ServerEvent = self.tagged()?;
        if matches!(
            event,
            ServerEvent::BotReady(_)
                | ServerEvent::ErrorResponse(_)
                | ServerEvent::ServerResponse(_)
        ) && self.id.is_none()
        {
            return Err(ProtocolError::MissingField("RTVI id"));
        }
        event.validate()?;
        Ok(event)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "kebab-case")]
pub enum ClientCommand {
    ClientReady(ClientReadyData),
    DisconnectBot,
    ClientMessage(CustomMessageData),
    SendText(SendTextData),
    SendFile(SendFileData),
    RawAudio(RawAudioData),
    RawAudioBatch(RawAudioBatchData),
    Dtmf(DtmfInputData),
    LlmFunctionCallResult(FunctionCallResultData),
    UiEvent(UiEventData),
    UiSnapshot(UiSnapshotData),
    UiCancelJobGroup(UiCancelJobGroupData),
}
impl ClientCommand {
    pub fn validate(&self) -> Result<(), ProtocolError> {
        match self {
            Self::RawAudio(data) => {
                data.decode()?;
            }
            Self::RawAudioBatch(data) => {
                data.decode()?;
            }
            Self::Dtmf(data) if data.buttons.is_empty() => {
                return Err(ProtocolError::InvalidField("buttons must not be empty"))
            }
            Self::SendFile(data) => data.file.validate()?,
            _ => {}
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
pub struct ClientReadyData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<AboutClient>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AboutClient {
    pub library: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub library_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_version: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub platform_details: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CustomMessageData {
    pub t: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub d: Option<Value>,
}
fn true_value() -> bool {
    true
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendTextOptions {
    #[serde(default = "true_value")]
    pub run_immediately: bool,
    #[serde(default = "true_value")]
    pub audio_response: bool,
}
impl Default for SendTextOptions {
    fn default() -> Self {
        Self {
            run_immediately: true,
            audio_response: true,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendTextData {
    pub content: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<SendTextOptions>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendFileData {
    pub content: String,
    pub file: File,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<SendFileOptions>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SendFileOptions {
    #[serde(default = "true_value")]
    pub run_immediately: bool,
    #[serde(default = "true_value")]
    pub audio_response: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub custom_options: Option<Map<String, Value>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct File {
    pub format: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    pub source: FileSource,
}
impl File {
    fn validate(&self) -> Result<(), ProtocolError> {
        if self.format.is_empty() {
            return Err(ProtocolError::InvalidField("file.format"));
        }
        match &self.source {
            FileSource::Bytes { bytes, .. } => {
                STANDARD.decode(bytes)?;
            }
            FileSource::Url { url } if url.is_empty() => {
                return Err(ProtocolError::InvalidField("file.url"))
            }
            _ => {}
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FileSource {
    Bytes {
        bytes: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        width: Option<u32>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        height: Option<u32>,
    },
    Url {
        url: String,
    },
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DtmfInputData {
    pub buttons: Vec<DtmfDigit>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawAudioData {
    pub base64_audio: String,
    pub sample_rate: u32,
    pub num_channels: u16,
}
impl RawAudioData {
    pub fn from_pcm16(
        bytes: &[u8],
        sample_rate: u32,
        num_channels: u16,
    ) -> Result<Self, ProtocolError> {
        validate_audio(bytes, sample_rate, num_channels)?;
        Ok(Self {
            base64_audio: STANDARD.encode(bytes),
            sample_rate,
            num_channels,
        })
    }
    pub fn decode(&self) -> Result<Vec<u8>, ProtocolError> {
        let bytes = STANDARD.decode(&self.base64_audio)?;
        validate_audio(&bytes, self.sample_rate, self.num_channels)?;
        Ok(bytes)
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RawAudioBatchData {
    pub base64_audio_batch: Vec<String>,
    pub sample_rate: u32,
    pub num_channels: u16,
}
impl RawAudioBatchData {
    pub fn decode(&self) -> Result<Vec<Vec<u8>>, ProtocolError> {
        validate_audio(&[], self.sample_rate, self.num_channels)?;
        self.base64_audio_batch
            .iter()
            .map(|data| {
                let bytes = STANDARD.decode(data)?;
                validate_audio(&bytes, self.sample_rate, self.num_channels)?;
                Ok(bytes)
            })
            .collect()
    }
}
fn validate_audio(data: &[u8], rate: u32, channels: u16) -> Result<(), ProtocolError> {
    if rate == 0 || channels == 0 || !data.len().is_multiple_of(usize::from(channels) * 2) {
        return Err(ProtocolError::InvalidField("PCM16 audio format"));
    }
    Ok(())
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data", rename_all = "kebab-case")]
pub enum ServerEvent {
    BotReady(BotReadyData),
    ErrorResponse(ErrorResponseData),
    Error(ErrorData),
    ServerResponse(CustomMessageData),
    ServerMessage(Value),
    BotLlmStarted,
    BotLlmStopped,
    BotTtsStarted,
    BotTtsStopped,
    BotLlmText(TextData),
    BotLlmMarker(MarkerData),
    BotTtsText(TextData),
    BotTranscription(TextData),
    BotOutput(BotOutputData),
    BotTtsAudio(AudioData),
    UserTranscription(TranscriptionData),
    UserLlmText(TextData),
    UserStartedSpeaking,
    UserStoppedSpeaking,
    VadUserStartedSpeaking,
    VadUserStoppedSpeaking,
    UserMuteStarted,
    UserMuteStopped,
    BotStartedSpeaking,
    BotStoppedSpeaking,
    BotInterrupted,
    LlmFunctionCallStarted(FunctionCallStartedData),
    LlmFunctionCallInProgress(FunctionCallInProgressData),
    LlmFunctionCallStopped(FunctionCallStoppedData),
    Metrics(Map<String, Value>),
    UserAudioLevel(AudioLevelData),
    BotAudioLevel(AudioLevelData),
    UiCommand(UiCommandData),
    UiJobGroup(UiJobGroupData),
}
impl ServerEvent {
    fn validate(&self) -> Result<(), ProtocolError> {
        if let Self::BotTtsAudio(audio) = self {
            validate_audio(
                &STANDARD.decode(&audio.audio)?,
                audio.sample_rate,
                audio.num_channels,
            )?;
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BotReadyData {
    pub version: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub about: Option<Map<String, Value>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub capabilities: Option<Map<String, Value>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorResponseData {
    pub error: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ErrorData {
    pub error: String,
    pub fatal: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TextData {
    pub text: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct MarkerData {
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub kind: Option<String>,
    #[serde(default)]
    pub raw: String,
    #[serde(default)]
    pub markers: Vec<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioData {
    pub audio: String,
    pub sample_rate: u32,
    pub num_channels: u16,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TranscriptionData {
    pub text: String,
    pub user_id: String,
    pub timestamp: String,
    #[serde(rename = "final")]
    pub is_final: bool,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallStartedData {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallInProgressData {
    pub tool_call_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Map<String, Value>>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallStoppedData {
    pub tool_call_id: String,
    pub cancelled: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FunctionCallResultData {
    pub function_name: String,
    pub tool_call_id: String,
    pub arguments: Map<String, Value>,
    pub result: Value,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AudioLevelData {
    pub value: f64,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SpokenStatus {
    New,
    InProgress,
    Completed,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SpokenProgress {
    pub accumulated_text: String,
    pub remaining_text: String,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BotOutputData {
    pub text: String,
    pub aggregated_by: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub segment_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spoken: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub will_be_spoken: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spoken_status: Option<SpokenStatus>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub spoken_progress: Option<SpokenProgress>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiEventData {
    pub event: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiCommandData {
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub payload: Option<Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiCancelJobGroupData {
    pub job_id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UiSnapshotData {
    pub tree: A11ySnapshot,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A11ySnapshot {
    pub root: A11yNode,
    pub captured_at: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub selection: Option<A11ySelection>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A11yNode {
    #[serde(rename = "ref")]
    pub reference: String,
    pub role: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub state: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub level: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub colcount: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rowcount: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub children: Option<Vec<A11yNode>>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct A11ySelection {
    #[serde(rename = "ref")]
    pub reference: String,
    pub text: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub start_offset: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_offset: Option<u64>,
    #[serde(flatten)]
    pub extra: Map<String, Value>,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UiJobGroupData {
    GroupStarted {
        job_id: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        workers: Option<Vec<String>>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        label: Option<String>,
        #[serde(default = "true_value")]
        cancellable: bool,
        #[serde(default)]
        at: u64,
    },
    JobUpdate {
        job_id: String,
        worker_name: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        data: Option<Value>,
        #[serde(default)]
        at: u64,
    },
    JobCompleted {
        job_id: String,
        worker_name: String,
        status: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        response: Option<Value>,
        #[serde(default)]
        at: u64,
    },
    GroupCompleted {
        job_id: String,
        #[serde(default)]
        at: u64,
    },
}
/// A warning is advisory: Python RTVI still completes client-ready.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NegotiatedVersion {
    pub advertised: String,
    pub legacy: bool,
    pub warning: Option<String>,
}
pub fn negotiate_version(version: Option<&str>) -> NegotiatedVersion {
    let parsed = version.and_then(|v| {
        let parts = v
            .split('.')
            .map(|s| {
                if !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit()) {
                    s.parse::<u32>().ok()
                } else {
                    None
                }
            })
            .collect::<Option<Vec<_>>>()?;
        (parts.len() == 3).then_some(parts)
    });
    match (version, parsed) {
        (Some(version), Some(parts)) if parts[0] == 1 => NegotiatedVersion {
            advertised: version.into(),
            legacy: true,
            warning: None,
        },
        (Some(_), Some(parts)) if parts[0] == 2 => NegotiatedVersion {
            advertised: PROTOCOL_VERSION.into(),
            legacy: false,
            warning: None,
        },
        (version, parsed) => {
            let warning = match (version, parsed) {
                (None, _) | (Some(""), _) => "Client version unknown.".into(),
                (Some(version), None) => format!("Invalid client version format ({version})."),
                (Some(version), Some(_)) => format!("RTVI version {version} is not compatible with server protocol {PROTOCOL_VERSION}."),
            };
            NegotiatedVersion {
                advertised: PROTOCOL_VERSION.into(),
                legacy: false,
                warning: Some(format!("{warning} Compatibility issues may occur.")),
            }
        }
    }
}
/// Client-side audio IDs increase for every encoded raw-audio message.
#[derive(Debug, Default)]
pub struct RtviClientCodec {
    next_id: u64,
}
impl RtviClientCodec {
    pub fn encode_audio(
        &mut self,
        pcm: &[u8],
        rate: u32,
        channels: u16,
    ) -> Result<String, ProtocolError> {
        let audio = RawAudioData::from_pcm16(pcm, rate, channels)?;
        let id = self
            .next_id
            .checked_add(1)
            .ok_or(ProtocolError::State("RTVI message ID exhausted"))?;
        let result = Envelope::client(id.to_string(), &ClientCommand::RawAudio(audio))?.encode()?;
        self.next_id = id;
        Ok(result)
    }
    pub fn deserialize(&self, bytes: &[u8]) -> Option<ServerEvent> {
        Envelope::parse(bytes).ok()?.server_event().ok()
    }
}
