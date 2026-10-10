//! Genesys AudioHook v2 control handshake and binary PCMU frame serializer.
//! L16 is rejected explicitly: the upstream serializer does not implement it.
use crate::{
    adapters::FrameSerializer, check_size, DtmfDigit, ProtocolError, WireMessage,
    DEFAULT_MAX_MESSAGE_BYTES,
};
use pipecat_audio::SincResampler;
use pipecat_core::{AudioRawFrame, Frame, FrameData};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

pub const PROTOCOL_VERSION: &str = "2";
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AudioHookChannel {
    #[default]
    External,
    Internal,
    Both,
}
impl AudioHookChannel {
    fn names(self) -> Vec<&'static str> {
        match self {
            Self::External => vec!["external"],
            Self::Internal => vec!["internal"],
            Self::Both => vec!["external", "internal"],
        }
    }
}
#[derive(Debug, Clone)]
pub struct AudioHookConfig {
    pub sample_rate: u32,
    pub pipeline_input_rate: u32,
    pub channel: AudioHookChannel,
    pub supported_languages: Vec<String>,
    pub selected_language: Option<String>,
    pub start_paused: bool,
    pub max_message_bytes: usize,
}
impl Default for AudioHookConfig {
    fn default() -> Self {
        Self {
            sample_rate: 8000,
            pipeline_input_rate: 16000,
            channel: AudioHookChannel::External,
            supported_languages: vec![],
            selected_language: None,
            start_paused: false,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ControlMessage {
    pub version: String,
    #[serde(rename = "type")]
    pub message_type: String,
    pub seq: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub clientseq: Option<u64>,
    pub id: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub position: Option<String>,
    #[serde(default)]
    pub parameters: Map<String, Value>,
}
impl ControlMessage {
    pub fn parse(text: &str, limit: usize) -> Result<Self, ProtocolError> {
        check_size(text.len(), limit)?;
        let message: Self = serde_json::from_str(text)?;
        if message.version != PROTOCOL_VERSION {
            return Err(ProtocolError::InvalidField("AudioHook version"));
        }
        if message.id.is_empty() || message.message_type.is_empty() || message.seq == 0 {
            return Err(ProtocolError::InvalidField("AudioHook envelope"));
        }
        if let Some(position) = &message.position {
            parse_position(position)?;
        }
        Ok(message)
    }
}
/// The caller supplies an initial session ID; a valid `open` replaces it with
/// the provider's session ID. Session identity is checked after opening.
pub struct AudioHookSerializer {
    config: AudioHookConfig,
    session_id: String,
    client_seq: u64,
    server_seq: u64,
    position_secs: f64,
    is_open: bool,
    is_paused: bool,
    input_resampler: SincResampler,
    output_resampler: Option<(u32, SincResampler)>,
    pub conversation_id: Option<String>,
    pub participant: Option<Map<String, Value>>,
    pub input_variables: Option<Map<String, Value>>,
    pub custom_config: Option<Map<String, Value>>,
    pub output_variables: Option<Map<String, Value>>,
    pub last_control: Option<ControlMessage>,
}
impl AudioHookSerializer {
    pub fn new(
        initial_session_id: impl Into<String>,
        config: AudioHookConfig,
    ) -> Result<Self, ProtocolError> {
        let session_id = initial_session_id.into();
        if session_id.is_empty() {
            return Err(ProtocolError::InvalidField("AudioHook id"));
        }
        let input_resampler =
            SincResampler::new(config.sample_rate, config.pipeline_input_rate, 1)?;
        Ok(Self {
            config,
            session_id,
            client_seq: 0,
            server_seq: 0,
            position_secs: 0.0,
            is_open: false,
            is_paused: false,
            input_resampler,
            output_resampler: None,
            conversation_id: None,
            participant: None,
            input_variables: None,
            custom_config: None,
            output_variables: None,
            last_control: None,
        })
    }
    pub fn session_id(&self) -> &str {
        &self.session_id
    }
    pub fn is_open(&self) -> bool {
        self.is_open
    }
    pub fn is_paused(&self) -> bool {
        self.is_paused
    }
    pub fn channel(&self) -> AudioHookChannel {
        self.config.channel
    }
    pub fn client_sequence(&self) -> u64 {
        self.client_seq
    }
    pub fn server_sequence(&self) -> u64 {
        self.server_seq
    }
    pub fn set_output_variables(&mut self, variables: Map<String, Value>) {
        self.output_variables = Some(variables)
    }
    fn response(
        &mut self,
        kind: &str,
        parameters: Value,
        include_position: bool,
    ) -> Result<ControlMessage, ProtocolError> {
        let seq = self
            .server_seq
            .checked_add(1)
            .ok_or(ProtocolError::State("AudioHook sequence exhausted"))?;
        let response = ControlMessage {
            version: PROTOCOL_VERSION.into(),
            message_type: kind.into(),
            seq,
            clientseq: Some(self.client_seq),
            id: self.session_id.clone(),
            position: include_position.then(|| format!("PT{:.3}S", self.position_secs)),
            parameters: parameters
                .as_object()
                .ok_or(ProtocolError::InvalidField("AudioHook parameters"))?
                .clone(),
        };
        check_size(
            serde_json::to_vec(&response)?.len(),
            self.config.max_message_bytes,
        )?;
        self.server_seq = seq;
        Ok(response)
    }
    pub fn opened(&mut self) -> Result<ControlMessage, ProtocolError> {
        let mut params = json!({"startPaused":self.config.start_paused,"media":[{"type":"audio","format":"PCMU","channels":self.config.channel.names(),"rate":self.config.sample_rate}]});
        if !self.config.supported_languages.is_empty() {
            params["supportedLanguages"] = json!(self.config.supported_languages)
        }
        if let Some(language) = &self.config.selected_language {
            params["selectedLanguage"] = json!(language)
        }
        let response = self.response("opened", params, false)?;
        self.is_open = true;
        self.is_paused = self.config.start_paused;
        Ok(response)
    }
    pub fn closed(&mut self) -> Result<ControlMessage, ProtocolError> {
        let params = match &self.output_variables {
            Some(vars) if !vars.is_empty() => json!({"outputVariables":vars}),
            _ => json!({}),
        };
        let response = self.response("closed", params, true)?;
        self.is_open = false;
        self.input_resampler.reset();
        if let Some((_, resampler)) = &mut self.output_resampler {
            resampler.reset()
        }
        Ok(response)
    }
    pub fn pong(&mut self) -> Result<ControlMessage, ProtocolError> {
        self.response("pong", json!({}), true)
    }
    pub fn resumed(&mut self) -> Result<ControlMessage, ProtocolError> {
        let response = self.response("resumed", json!({}), true)?;
        self.is_paused = false;
        Ok(response)
    }
    pub fn barge_in(&mut self) -> Result<ControlMessage, ProtocolError> {
        let response = self.response(
            "event",
            json!({"entities":[{"type":"barge_in","data":{}}]}),
            true,
        )?;
        if let Some((_, resampler)) = &mut self.output_resampler {
            resampler.reset()
        }
        Ok(response)
    }
    pub fn disconnect(
        &mut self,
        reason: &str,
        action: &str,
        info: Option<&str>,
    ) -> Result<ControlMessage, ProtocolError> {
        let mut vars = json!({"action":action});
        if let Some(values) = &self.output_variables {
            vars.as_object_mut().expect("object").extend(values.clone())
        }
        let mut params = json!({"reason":reason,"outputVariables":vars});
        if let Some(info) = info {
            params["info"] = json!(info)
        }
        self.response("disconnect", params, true)
    }
    pub fn error(
        &mut self,
        code: u32,
        message: &str,
        retryable: bool,
    ) -> Result<ControlMessage, ProtocolError> {
        self.response(
            "error",
            json!({"code":code,"message":message,"retryable":retryable}),
            true,
        )
    }
    pub fn try_deserialize(
        &mut self,
        message: &WireMessage,
    ) -> Result<Option<Frame>, ProtocolError> {
        match message {
            WireMessage::Binary(bytes) => {
                check_size(bytes.len(), self.config.max_message_bytes)?;
                if !self.is_open || self.is_paused || bytes.is_empty() {
                    return Ok(None);
                }
                let mono = if self.config.channel == AudioHookChannel::Both {
                    if !bytes.len().is_multiple_of(2) {
                        return Err(ProtocolError::InvalidField("AudioHook stereo alignment"));
                    }
                    bytes.iter().step_by(2).copied().collect::<Vec<_>>()
                } else {
                    bytes.clone()
                };
                let pcm = self
                    .input_resampler
                    .process(&pipecat_audio::ulaw_to_pcm(&mono))?;
                Ok(self.input_frame(pcm))
            }
            WireMessage::Text(text) => {
                let message = ControlMessage::parse(text, self.config.max_message_bytes)?;
                self.handle_control(message)
            }
        }
    }
    fn handle_control(&mut self, message: ControlMessage) -> Result<Option<Frame>, ProtocolError> {
        if self.is_open && message.id != self.session_id {
            return Err(ProtocolError::StreamMismatch);
        }
        if message.seq <= self.client_seq {
            return Err(ProtocolError::Sequence {
                previous: self.client_seq,
                received: message.seq,
            });
        }
        let parameters = &message.parameters;
        // Validate fields before committing sequence/session state.
        let participant = optional_map(parameters, "participant")?;
        let input_variables = optional_map(parameters, "inputVariables")?;
        let custom_config = optional_map(parameters, "customConfig")?;
        let mut channels = self.config.channel;
        if message.message_type == "open" {
            if self.is_open {
                return Err(ProtocolError::State("duplicate AudioHook open"));
            }
            if let Some(media) = parameters.get("media") {
                let items = media
                    .as_array()
                    .ok_or(ProtocolError::InvalidField("AudioHook media"))?;
                if let Some(media) = items.first() {
                    let media = media
                        .as_object()
                        .ok_or(ProtocolError::InvalidField("AudioHook media"))?;
                    if let Some(format) = media.get("format").and_then(Value::as_str) {
                        if format != "PCMU" {
                            return Err(ProtocolError::UnsupportedEncoding(format.into()));
                        }
                    }
                    if let Some(rate) = media.get("rate") {
                        if rate.as_u64() != Some(u64::from(self.config.sample_rate)) {
                            return Err(ProtocolError::InvalidField("AudioHook sample rate"));
                        }
                    }
                    if let Some(values) = media.get("channels") {
                        let values: Vec<String> = serde_json::from_value(values.clone())?;
                        let external = values.iter().any(|s| s == "external");
                        let internal = values.iter().any(|s| s == "internal");
                        channels = match (external, internal) {
                            (true, true) => AudioHookChannel::Both,
                            (true, false) => AudioHookChannel::External,
                            (false, true) => AudioHookChannel::Internal,
                            _ => return Err(ProtocolError::InvalidField("AudioHook channels")),
                        };
                    }
                }
            }
        }
        let digit = if message.message_type == "dtmf" {
            Some(
                parameters
                    .get("digit")
                    .and_then(Value::as_str)
                    .ok_or(ProtocolError::MissingField("DTMF digit"))?
                    .parse::<DtmfDigit>()?,
            )
        } else {
            None
        };
        let position = message
            .position
            .as_ref()
            .map(|p| parse_position(p))
            .transpose()?;
        self.client_seq = message.seq;
        if let Some(position) = position {
            self.position_secs = position
        }
        let response = match message.message_type.as_str() {
            "open" => {
                self.session_id = message.id.clone();
                self.config.channel = channels;
                self.conversation_id = parameters
                    .get("conversationId")
                    .and_then(Value::as_str)
                    .map(str::to_owned);
                self.participant = participant;
                self.input_variables = input_variables;
                self.custom_config = custom_config;
                Some(self.opened()?)
            }
            "close" => Some(self.closed()?),
            "ping" => Some(self.pong()?),
            "pause" => {
                self.is_paused = true;
                self.input_resampler.reset();
                if let Some((_, r)) = &mut self.output_resampler {
                    r.reset()
                }
                None
            }
            "update" => {
                if participant.is_some() {
                    self.participant = participant
                }
                None
            }
            _ => None,
        };
        self.last_control = Some(message);
        if let Some(response) = response {
            return Ok(Some(Frame::new(FrameData::OutputTransportMessageUrgent(
                serde_json::to_value(response)?,
            ))));
        }
        Ok(digit.map(|digit| Frame::new(FrameData::InputDtmf(digit.into()))))
    }
    fn input_frame(&self, bytes: Vec<u8>) -> Option<Frame> {
        if bytes.is_empty() {
            None
        } else {
            Some(Frame::new(FrameData::InputAudio(AudioRawFrame {
                audio: bytes,
                sample_rate: self.config.pipeline_input_rate,
                num_channels: 1,
                context_id: None,
            })))
        }
    }
    pub fn flush_input(&mut self) -> Result<Option<Frame>, ProtocolError> {
        let pcm = self.input_resampler.flush()?;
        Ok(self.input_frame(pcm))
    }
    pub fn flush_output(&mut self) -> Result<Option<WireMessage>, ProtocolError> {
        let Some((_, resampler)) = &mut self.output_resampler else {
            return Ok(None);
        };
        let bytes = pipecat_audio::pcm_to_ulaw(&resampler.flush()?)?;
        Ok((!bytes.is_empty()).then_some(WireMessage::Binary(bytes)))
    }
    fn control_wire(&self, message: &ControlMessage) -> Result<WireMessage, ProtocolError> {
        Ok(WireMessage::Text(serde_json::to_string(message)?))
    }
}
impl FrameSerializer for AudioHookSerializer {
    fn serialize(&mut self, frame: &Frame) -> Result<Option<WireMessage>, ProtocolError> {
        crate::reject_openclaw(frame)?;
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| ProtocolError::InvalidField("unconsumed native LLM continuation"))?;
        match &frame.data {
            FrameData::End(_) | FrameData::Cancel(_) => {
                let message = self.disconnect("completed", "transfer", None)?;
                self.control_wire(&message).map(Some)
            }
            FrameData::Interruption => {
                let message = self.barge_in()?;
                self.control_wire(&message).map(Some)
            }
            FrameData::InputAudio(audio)
            | FrameData::OutputAudio(audio)
            | FrameData::TtsAudio(audio) => {
                if !self.is_open || self.is_paused {
                    return Ok(None);
                }
                check_size(audio.audio.len(), self.config.max_message_bytes)?;
                if audio.num_channels != 1 || !audio.audio.len().is_multiple_of(2) {
                    return Err(ProtocolError::InvalidField("AudioHook PCM16 mono audio"));
                }
                if self.output_resampler.as_ref().map(|(rate, _)| *rate) != Some(audio.sample_rate)
                {
                    if self
                        .output_resampler
                        .as_ref()
                        .is_some_and(|(_, r)| r.buffered_frames() > 0)
                    {
                        return Err(ProtocolError::State(
                            "flush AudioHook output before rate change",
                        ));
                    }
                    self.output_resampler = Some((
                        audio.sample_rate,
                        SincResampler::new(audio.sample_rate, self.config.sample_rate, 1)?,
                    ));
                }
                let pcm = self
                    .output_resampler
                    .as_mut()
                    .expect("initialized")
                    .1
                    .process(&audio.audio)?;
                let bytes = pipecat_audio::pcm_to_ulaw(&pcm)?;
                Ok((!bytes.is_empty()).then_some(WireMessage::Binary(bytes)))
            }
            FrameData::OutputTransportMessage(message)
            | FrameData::OutputTransportMessageUrgent(message)
                if message.get("version").is_some()
                    && message.get("label").and_then(Value::as_str)
                        != Some(crate::rtvi::MESSAGE_LABEL) =>
            {
                let text = serde_json::to_string(message)?;
                check_size(text.len(), self.config.max_message_bytes)?;
                Ok(Some(WireMessage::Text(text)))
            }
            _ => Ok(None),
        }
    }
    fn deserialize(&mut self, message: &WireMessage) -> Result<Option<Frame>, ProtocolError> {
        match self.try_deserialize(message) {
            Ok(frame) => Ok(frame),
            Err(ProtocolError::Audio(error)) => Err(ProtocolError::Audio(error)),
            Err(_) => Ok(None),
        }
    }
}
fn optional_map(
    map: &Map<String, Value>,
    key: &'static str,
) -> Result<Option<Map<String, Value>>, ProtocolError> {
    map.get(key)
        .map(|v| {
            v.as_object()
                .cloned()
                .ok_or(ProtocolError::InvalidField(key))
        })
        .transpose()
}
pub fn parse_position(value: &str) -> Result<f64, ProtocolError> {
    let seconds = value
        .strip_prefix("PT")
        .and_then(|s| s.strip_suffix('S'))
        .and_then(|s| s.parse::<f64>().ok())
        .filter(|v| v.is_finite() && *v >= 0.0)
        .ok_or(ProtocolError::InvalidField("AudioHook position"))?;
    Ok(seconds)
}
