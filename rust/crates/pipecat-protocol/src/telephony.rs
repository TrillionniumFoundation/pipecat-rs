//! Provider-specific streaming messages. Audio payloads are raw codec bytes,
//! never WAV/MP3 containers. See the crate README for the source contracts.

use crate::{check_size, DtmfDigit, ProtocolError, WireMessage, DEFAULT_MAX_MESSAGE_BYTES};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Provider {
    Twilio,
    Plivo,
    Telnyx,
    Vonage,
    Exotel,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum AudioEncoding {
    Pcmu,
    Pcma,
    Pcm16Le,
}

impl AudioEncoding {
    pub fn from_wire(value: &str) -> Result<Self, ProtocolError> {
        match value {
            "PCMU" | "audio/x-mulaw" => Ok(Self::Pcmu),
            "PCMA" | "audio/x-alaw" => Ok(Self::Pcma),
            // L16 is intentionally not treated as little-endian PCM.
            _ => Err(ProtocolError::UnsupportedEncoding(value.into())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedAudio {
    pub data: Vec<u8>,
    pub encoding: AudioEncoding,
    pub sample_rate: u32,
    pub channels: u16,
}

impl EncodedAudio {
    pub fn to_pcm16(&self) -> Result<Vec<u8>, ProtocolError> {
        match self.encoding {
            AudioEncoding::Pcmu => Ok(pipecat_audio::ulaw_to_pcm(&self.data)),
            AudioEncoding::Pcma => Ok(pipecat_audio::alaw_to_pcm(&self.data)),
            AudioEncoding::Pcm16Le => {
                validate_pcm(&self.data, self.channels)?;
                Ok(self.data.clone())
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StartEvent {
    pub stream_id: String,
    pub call_id: Option<String>,
    pub account_id: Option<String>,
    pub encoding: AudioEncoding,
    pub sample_rate: u32,
    pub channels: u16,
    pub tracks: Vec<String>,
    pub custom_parameters: Map<String, Value>,
    pub metadata: Map<String, Value>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum EventKind {
    Connected {
        protocol: Option<String>,
        version: Option<String>,
        content_type: Option<String>,
    },
    Start(StartEvent),
    Media {
        audio: EncodedAudio,
        track: Option<String>,
        chunk: Option<u64>,
        timestamp_ms: Option<u64>,
    },
    Dtmf {
        digit: DtmfDigit,
        track: Option<String>,
        duration_ms: Option<u64>,
    },
    Mark {
        name: String,
    },
    Stop {
        metadata: Map<String, Value>,
    },
    Cleared,
    Notify {
        payload: Value,
    },
    Error {
        payload: Value,
    },
    Unknown {
        event: String,
        message: Value,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct TelephonyEvent {
    pub provider: Provider,
    pub stream_id: Option<String>,
    pub sequence: Option<u64>,
    pub kind: EventKind,
}

/// A configured codec. Telnyx's inbound encoding means data *sent to* Telnyx,
/// and outbound encoding means data *received from* Telnyx, as in Python.
#[derive(Debug, Clone)]
pub struct TelephonyCodec {
    provider: Provider,
    stream_id: Option<String>,
    sample_rate: u32,
    receive_encoding: AudioEncoding,
    send_encoding: AudioEncoding,
    max_message_bytes: usize,
    pub ignore_rtvi_messages: bool,
}

impl TelephonyCodec {
    pub fn twilio(stream_id: impl Into<String>) -> Result<Self, ProtocolError> {
        Self::new(
            Provider::Twilio,
            Some(stream_id.into()),
            8000,
            AudioEncoding::Pcmu,
            AudioEncoding::Pcmu,
        )
    }
    pub fn plivo(stream_id: impl Into<String>) -> Result<Self, ProtocolError> {
        Self::new(
            Provider::Plivo,
            Some(stream_id.into()),
            8000,
            AudioEncoding::Pcmu,
            AudioEncoding::Pcmu,
        )
    }
    pub fn telnyx(
        stream_id: impl Into<String>,
        outbound: &str,
        inbound: &str,
    ) -> Result<Self, ProtocolError> {
        Self::new(
            Provider::Telnyx,
            Some(stream_id.into()),
            8000,
            AudioEncoding::from_wire(outbound)?,
            AudioEncoding::from_wire(inbound)?,
        )
    }
    pub fn vonage(sample_rate: u32) -> Result<Self, ProtocolError> {
        Self::new(
            Provider::Vonage,
            None,
            sample_rate,
            AudioEncoding::Pcm16Le,
            AudioEncoding::Pcm16Le,
        )
    }
    pub fn exotel(stream_id: impl Into<String>, sample_rate: u32) -> Result<Self, ProtocolError> {
        Self::new(
            Provider::Exotel,
            Some(stream_id.into()),
            sample_rate,
            AudioEncoding::Pcm16Le,
            AudioEncoding::Pcm16Le,
        )
    }
    fn new(
        provider: Provider,
        stream_id: Option<String>,
        sample_rate: u32,
        receive_encoding: AudioEncoding,
        send_encoding: AudioEncoding,
    ) -> Result<Self, ProtocolError> {
        if sample_rate == 0 {
            return Err(ProtocolError::InvalidField("sample_rate"));
        }
        if stream_id.as_deref() == Some("") {
            return Err(ProtocolError::InvalidField("stream_id"));
        }
        Ok(Self {
            provider,
            stream_id,
            sample_rate,
            receive_encoding,
            send_encoding,
            max_message_bytes: DEFAULT_MAX_MESSAGE_BYTES,
            ignore_rtvi_messages: true,
        })
    }
    pub fn with_max_message_bytes(mut self, limit: usize) -> Self {
        self.max_message_bytes = limit;
        self
    }
    pub fn provider(&self) -> Provider {
        self.provider
    }
    pub fn sample_rate(&self) -> u32 {
        self.sample_rate
    }
    pub fn stream_id(&self) -> Option<&str> {
        self.stream_id.as_deref()
    }
    pub fn receive_encoding(&self) -> AudioEncoding {
        self.receive_encoding
    }
    pub fn send_encoding(&self) -> AudioEncoding {
        self.send_encoding
    }

    /// Strict, diagnostic parsing. Does not mutate stream state or do network I/O.
    pub fn parse(&self, data: &WireMessage) -> Result<TelephonyEvent, ProtocolError> {
        let text = match data {
            WireMessage::Binary(data) if self.provider == Provider::Vonage => {
                check_size(data.len(), self.max_message_bytes)?;
                validate_pcm(data, 1)?;
                return Ok(TelephonyEvent {
                    provider: self.provider,
                    stream_id: None,
                    sequence: None,
                    kind: EventKind::Media {
                        audio: EncodedAudio {
                            data: data.clone(),
                            encoding: AudioEncoding::Pcm16Le,
                            sample_rate: self.sample_rate,
                            channels: 1,
                        },
                        track: None,
                        chunk: None,
                        timestamp_ms: None,
                    },
                });
            }
            WireMessage::Binary(data) => {
                check_size(data.len(), self.max_message_bytes)?;
                std::str::from_utf8(data).map_err(|_| ProtocolError::InvalidField("UTF-8 JSON"))?
            }
            WireMessage::Text(text) => {
                check_size(text.len(), self.max_message_bytes)?;
                text
            }
        };
        let value: Value = serde_json::from_str(text)?;
        let message = object(&value, "message")?;
        let event = required_string(message, "event")?;
        let stream_key = match self.provider {
            Provider::Twilio => "streamSid",
            Provider::Plivo => "streamId",
            Provider::Exotel => "stream_sid",
            _ => "stream_id",
        };
        let sequence_key = if self.provider == Provider::Telnyx {
            "sequence_number"
        } else {
            "sequenceNumber"
        };
        let mut stream_id = optional_string(message, stream_key)?;
        let sequence = optional_number(message, sequence_key)?;
        if sequence == Some(0) {
            return Err(ProtocolError::InvalidField("sequence number"));
        }
        let recognized = match self.provider {
            Provider::Vonage => matches!(
                event,
                "websocket:connected" | "websocket:dtmf" | "websocket:cleared" | "websocket:notify"
            ),
            Provider::Plivo => matches!(
                event,
                "connected"
                    | "start"
                    | "media"
                    | "dtmf"
                    | "playedStream"
                    | "clearedAudio"
                    | "stop"
                    | "error"
            ),
            Provider::Twilio | Provider::Telnyx => matches!(
                event,
                "connected" | "start" | "media" | "dtmf" | "mark" | "stop" | "error"
            ),
            Provider::Exotel => matches!(
                event,
                "connected" | "start" | "media" | "dtmf" | "stop" | "error"
            ),
        };
        if !recognized {
            return Ok(TelephonyEvent {
                provider: self.provider,
                stream_id,
                sequence,
                kind: EventKind::Unknown {
                    event: event.into(),
                    message: value,
                },
            });
        }
        let kind = match event {
            "connected" | "websocket:connected" => EventKind::Connected {
                protocol: optional_string(message, "protocol")?,
                version: optional_string(message, "version")?,
                content_type: optional_string(message, "content-type")?,
            },
            "start" => {
                let start = required_object(message, "start")?;
                let nested_id = optional_string(start, stream_key)?;
                if let (Some(a), Some(b)) = (&stream_id, &nested_id) {
                    if a != b {
                        return Err(ProtocolError::StreamMismatch);
                    }
                }
                stream_id = stream_id.or(nested_id);
                let id = stream_id
                    .clone()
                    .ok_or(ProtocolError::MissingField("stream_id"))?;
                let (call_key, account_key, format_key, rate_key) = match self.provider {
                    Provider::Twilio => ("callSid", "accountSid", "mediaFormat", "sampleRate"),
                    Provider::Plivo => ("callId", "accountId", "mediaFormat", "sampleRate"),
                    Provider::Telnyx => {
                        ("call_control_id", "user_id", "media_format", "sample_rate")
                    }
                    Provider::Exotel => ("call_sid", "account_sid", "media_format", "sample_rate"),
                    Provider::Vonage => {
                        return Err(ProtocolError::UnsupportedMessage(event.into()))
                    }
                };
                let mut encoding = self.receive_encoding;
                let mut sample_rate = self.sample_rate;
                let mut channels = 1;
                if let Some(format) = start.get(format_key) {
                    let format = object(format, "media format")?;
                    if let Some(name) = optional_string(format, "encoding")? {
                        encoding = AudioEncoding::from_wire(&name)?;
                    }
                    if let Some(rate) = optional_number(format, rate_key)? {
                        sample_rate = u32::try_from(rate)
                            .map_err(|_| ProtocolError::InvalidField("sample_rate"))?;
                    }
                    if let Some(count) = optional_number(format, "channels")? {
                        channels = u16::try_from(count)
                            .map_err(|_| ProtocolError::InvalidField("channels"))?;
                    }
                }
                if sample_rate == 0 || channels != 1 {
                    return Err(ProtocolError::InvalidField("media format"));
                }
                if encoding != self.receive_encoding {
                    return Err(ProtocolError::UnsupportedEncoding(format!(
                        "negotiated {encoding:?}, configured {:?}",
                        self.receive_encoding
                    )));
                }
                if sample_rate != self.sample_rate {
                    return Err(ProtocolError::SampleRateMismatch {
                        expected: self.sample_rate,
                        actual: sample_rate,
                    });
                }
                let tracks = match start.get("tracks") {
                    None => vec![],
                    Some(v) => serde_json::from_value::<Vec<String>>(v.clone())?,
                };
                let custom_parameters = match start.get("customParameters") {
                    None => Map::new(),
                    Some(v) => object(v, "customParameters")?.clone(),
                };
                EventKind::Start(StartEvent {
                    stream_id: id,
                    call_id: optional_string(start, call_key)?,
                    account_id: optional_string(start, account_key)?,
                    encoding,
                    sample_rate,
                    channels,
                    tracks,
                    custom_parameters,
                    metadata: start.clone(),
                })
            }
            "media" => {
                let media = required_object(message, "media")?;
                let payload = required_string(media, "payload")?;
                if payload.is_empty() {
                    return Err(ProtocolError::InvalidField("payload"));
                }
                let bytes = STANDARD.decode(payload)?;
                if self.receive_encoding == AudioEncoding::Pcm16Le {
                    validate_pcm(&bytes, 1)?;
                }
                EventKind::Media {
                    audio: EncodedAudio {
                        data: bytes,
                        encoding: self.receive_encoding,
                        sample_rate: self.sample_rate,
                        channels: 1,
                    },
                    track: optional_string(media, "track")?,
                    chunk: optional_number(media, "chunk")?,
                    timestamp_ms: optional_number(media, "timestamp")?,
                }
            }
            "dtmf" | "websocket:dtmf" => {
                let dtmf = match message.get("dtmf") {
                    Some(v) => object(v, "dtmf")?,
                    None if self.provider == Provider::Vonage => message,
                    None => return Err(ProtocolError::MissingField("dtmf")),
                };
                let digit_value = if self.provider == Provider::Vonage {
                    message.get("digit").or_else(|| dtmf.get("digit"))
                } else {
                    dtmf.get("digit")
                }
                .ok_or(ProtocolError::MissingField("digit"))?;
                let digit = match digit_value {
                    Value::String(v) => v.parse()?,
                    Value::Number(v) if self.provider == Provider::Vonage => {
                        v.to_string().parse()?
                    }
                    _ => return Err(ProtocolError::InvalidField("digit")),
                };
                EventKind::Dtmf {
                    digit,
                    track: optional_string(dtmf, "track")?,
                    duration_ms: optional_number(message, "duration")?,
                }
            }
            "mark" => EventKind::Mark {
                name: required_string(required_object(message, "mark")?, "name")?.into(),
            },
            "playedStream" if self.provider == Provider::Plivo => EventKind::Mark {
                name: required_string(message, "name")?.into(),
            },
            "stop" => EventKind::Stop {
                metadata: match message.get("stop") {
                    None => Map::new(),
                    Some(v) => object(v, "stop")?.clone(),
                },
            },
            "clearedAudio" | "websocket:cleared" => EventKind::Cleared,
            "websocket:notify" => EventKind::Notify {
                payload: message.get("payload").cloned().unwrap_or(Value::Null),
            },
            "error" => EventKind::Error {
                payload: message.get("payload").cloned().unwrap_or(Value::Null),
            },
            _ => EventKind::Unknown {
                event: event.into(),
                message: value.clone(),
            },
        };
        if let (Some(configured), Some(actual)) = (&self.stream_id, &stream_id) {
            if configured != actual {
                return Err(ProtocolError::StreamMismatch);
            }
        }
        Ok(TelephonyEvent {
            provider: self.provider,
            stream_id,
            sequence,
            kind,
        })
    }

    /// Receive-loop compatibility: malformed packets are dropped, never panic.
    pub fn deserialize(&self, data: &WireMessage) -> Option<TelephonyEvent> {
        self.parse(data).ok()
    }

    /// Encode already converted provider audio. Encoding and rate are explicit.
    pub fn encode_audio(&self, audio: &EncodedAudio) -> Result<Option<WireMessage>, ProtocolError> {
        if audio.encoding != self.send_encoding || audio.channels != 1 {
            return Err(ProtocolError::InvalidField("output audio format"));
        }
        if audio.sample_rate != self.sample_rate {
            return Err(ProtocolError::SampleRateMismatch {
                expected: self.sample_rate,
                actual: audio.sample_rate,
            });
        }
        check_size(audio.data.len(), self.max_message_bytes)?;
        if audio.data.is_empty() {
            return Ok(None);
        }
        if audio.encoding == AudioEncoding::Pcm16Le {
            validate_pcm(&audio.data, audio.channels)?;
        }
        if self.provider == Provider::Vonage {
            return Ok(Some(WireMessage::Binary(audio.data.clone())));
        }
        let payload = STANDARD.encode(&audio.data);
        let message = match self.provider {
            Provider::Twilio => {
                json!({"event":"media","streamSid":self.stream_id,"media":{"payload":payload}})
            }
            Provider::Plivo => {
                json!({"event":"playAudio","streamId":self.stream_id,"media":{"contentType":"audio/x-mulaw","sampleRate":self.sample_rate,"payload":payload}})
            }
            Provider::Telnyx => json!({"event":"media","media":{"payload":payload}}),
            Provider::Exotel => {
                json!({"event":"media","stream_sid":self.stream_id,"media":{"payload":payload}})
            }
            Provider::Vonage => unreachable!(),
        };
        self.json_output(message).map(Some)
    }

    /// Convert PCM16 little-endian audio at the provider rate using pipecat-audio.
    /// Resampling is an explicit upstream operation rather than silently relabeling audio.
    pub fn encode_pcm16(
        &self,
        pcm: &[u8],
        sample_rate: u32,
    ) -> Result<Option<WireMessage>, ProtocolError> {
        validate_pcm(pcm, 1)?;
        let data = match self.send_encoding {
            AudioEncoding::Pcmu => pipecat_audio::pcm_to_ulaw(pcm)?,
            AudioEncoding::Pcma => pipecat_audio::pcm_to_alaw(pcm)?,
            AudioEncoding::Pcm16Le => pcm.to_vec(),
        };
        self.encode_audio(&EncodedAudio {
            data,
            encoding: self.send_encoding,
            sample_rate,
            channels: 1,
        })
    }

    pub fn clear(&self) -> Result<WireMessage, ProtocolError> {
        self.json_output(match self.provider {
            Provider::Twilio => json!({"event":"clear","streamSid":self.stream_id}),
            Provider::Plivo => json!({"event":"clearAudio","streamId":self.stream_id}),
            Provider::Telnyx => json!({"event":"clear"}),
            Provider::Vonage => json!({"action":"clear"}),
            Provider::Exotel => json!({"event":"clear","stream_sid":self.stream_id}),
        })
    }
    pub fn mark(&self, name: &str) -> Result<WireMessage, ProtocolError> {
        if name.is_empty() {
            return Err(ProtocolError::InvalidField("mark.name"));
        }
        self.json_output(match self.provider {
            Provider::Twilio => {
                json!({"event":"mark","streamSid":self.stream_id,"mark":{"name":name}})
            }
            Provider::Plivo => json!({"event":"checkpoint","streamId":self.stream_id,"name":name}),
            Provider::Telnyx => json!({"event":"mark","mark":{"name":name}}),
            Provider::Vonage => json!({"action":"notify","payload":{"name":name}}),
            Provider::Exotel => return Err(ProtocolError::UnsupportedMessage("mark".into())),
        })
    }
    pub fn transport_message(&self, message: &Value) -> Result<Option<WireMessage>, ProtocolError> {
        if self.ignore_rtvi_messages
            && message.get("label").and_then(Value::as_str) == Some(crate::rtvi::MESSAGE_LABEL)
        {
            return Ok(None);
        }
        self.json_output(message.clone()).map(Some)
    }
    fn json_output(&self, value: Value) -> Result<WireMessage, ProtocolError> {
        let text = serde_json::to_string(&value)?;
        check_size(text.len(), self.max_message_bytes)?;
        Ok(WireMessage::Text(text))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequencePolicy {
    Unchecked,
    Monotonic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamState {
    New,
    Connected,
    Started,
    Stopped,
}

/// Optional lifecycle validation. Telnyx defaults to unchecked sequence order:
/// its media and asynchronous DTMF events are explicitly allowed to arrive reordered.
pub struct TelephonySession {
    codec: TelephonyCodec,
    state: StreamState,
    last_sequence: Option<u64>,
    sequence_policy: SequencePolicy,
}
impl TelephonySession {
    pub fn new(codec: TelephonyCodec) -> Self {
        let sequence_policy = match codec.provider {
            Provider::Twilio | Provider::Plivo => SequencePolicy::Monotonic,
            _ => SequencePolicy::Unchecked,
        };
        Self {
            codec,
            state: StreamState::New,
            last_sequence: None,
            sequence_policy,
        }
    }
    pub fn with_sequence_policy(mut self, policy: SequencePolicy) -> Self {
        self.sequence_policy = policy;
        self
    }
    pub fn state(&self) -> StreamState {
        self.state
    }
    pub fn codec(&self) -> &TelephonyCodec {
        &self.codec
    }
    pub fn last_sequence(&self) -> Option<u64> {
        self.last_sequence
    }
    pub fn accept(&mut self, data: &WireMessage) -> Result<TelephonyEvent, ProtocolError> {
        let event = self.codec.parse(data)?;
        if self.state == StreamState::Stopped {
            return Err(ProtocolError::State("message after stop"));
        }
        if self.sequence_policy == SequencePolicy::Monotonic {
            if let (Some(previous), Some(received)) = (self.last_sequence, event.sequence) {
                if received <= previous {
                    return Err(ProtocolError::Sequence { previous, received });
                }
            }
        }
        let next = match &event.kind {
            EventKind::Connected { .. } if self.state == StreamState::New => StreamState::Connected,
            EventKind::Connected { .. } => return Err(ProtocolError::State("duplicate connected")),
            EventKind::Start(_)
                if matches!(self.state, StreamState::New | StreamState::Connected) =>
            {
                StreamState::Started
            }
            EventKind::Start(_) => return Err(ProtocolError::State("duplicate start")),
            EventKind::Media { .. }
                if self.codec.provider == Provider::Vonage
                    && self.state == StreamState::Connected =>
            {
                StreamState::Started
            }
            EventKind::Media { .. } | EventKind::Dtmf { .. } | EventKind::Mark { .. }
                if self.state != StreamState::Started =>
            {
                return Err(ProtocolError::State("media/control before start"))
            }
            EventKind::Stop { .. } if self.state == StreamState::Started => StreamState::Stopped,
            EventKind::Stop { .. } => return Err(ProtocolError::State("stop before start")),
            _ => self.state,
        };
        // Commit state only after the entire packet passes validation.
        self.state = next;
        if let Some(sequence) = event.sequence {
            self.last_sequence = Some(sequence);
        }
        Ok(event)
    }
}

fn object<'a>(
    value: &'a Value,
    field: &'static str,
) -> Result<&'a Map<String, Value>, ProtocolError> {
    value.as_object().ok_or(ProtocolError::InvalidField(field))
}
fn required_object<'a>(
    map: &'a Map<String, Value>,
    key: &'static str,
) -> Result<&'a Map<String, Value>, ProtocolError> {
    object(map.get(key).ok_or(ProtocolError::MissingField(key))?, key)
}
fn required_string<'a>(
    map: &'a Map<String, Value>,
    key: &'static str,
) -> Result<&'a str, ProtocolError> {
    map.get(key)
        .ok_or(ProtocolError::MissingField(key))?
        .as_str()
        .ok_or(ProtocolError::InvalidField(key))
}
fn optional_string(
    map: &Map<String, Value>,
    key: &'static str,
) -> Result<Option<String>, ProtocolError> {
    map.get(key)
        .map(|value| {
            value
                .as_str()
                .map(str::to_owned)
                .ok_or(ProtocolError::InvalidField(key))
        })
        .transpose()
}
fn optional_number(
    map: &Map<String, Value>,
    key: &'static str,
) -> Result<Option<u64>, ProtocolError> {
    map.get(key)
        .map(|value| {
            match value {
                Value::Number(n) => n.as_u64(),
                Value::String(s) if !s.is_empty() && s.bytes().all(|c| c.is_ascii_digit()) => {
                    s.parse().ok()
                }
                _ => None,
            }
            .ok_or(ProtocolError::InvalidField(key))
        })
        .transpose()
}
fn validate_pcm(data: &[u8], channels: u16) -> Result<(), ProtocolError> {
    if channels == 0 || !data.len().is_multiple_of(usize::from(channels) * 2) {
        Err(ProtocolError::InvalidField("PCM16 channel alignment"))
    } else {
        Ok(())
    }
}
