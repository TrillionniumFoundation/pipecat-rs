//! Synchronous frame/wire boundaries usable by asynchronous transports.
//! Malformed inbound packets are dropped by `deserialize`; diagnostic parsing
//! remains available through the wire codecs.
use crate::{
    rtvi,
    telephony::{EventKind, TelephonyCodec},
    DtmfDigit, ProtocolError, WireMessage,
};
use pipecat_audio::SincResampler;
use pipecat_core::{AudioRawFrame, Frame, FrameData, KeypadEntry, TextFrame, TranscriptionFrame};
use serde_json::{json, Value};

pub trait FrameSerializer {
    fn serialize(&mut self, frame: &Frame) -> Result<Option<WireMessage>, ProtocolError>;
    fn deserialize(&mut self, message: &WireMessage) -> Result<Option<Frame>, ProtocolError>;
}

pub struct TelephonyFrameSerializer {
    codec: TelephonyCodec,
    input_rate: u32,
    input_resampler: SincResampler,
    output_resampler: Option<(u32, SincResampler)>,
}
impl TelephonyFrameSerializer {
    pub fn new(codec: TelephonyCodec, pipeline_input_rate: u32) -> Result<Self, ProtocolError> {
        let input_resampler = SincResampler::new(codec.sample_rate(), pipeline_input_rate, 1)?;
        Ok(Self {
            codec,
            input_rate: pipeline_input_rate,
            input_resampler,
            output_resampler: None,
        })
    }
    pub fn codec(&self) -> &TelephonyCodec {
        &self.codec
    }
    pub fn codec_mut(&mut self) -> &mut TelephonyCodec {
        &mut self.codec
    }
    pub fn reset_audio(&mut self) {
        self.input_resampler.reset();
        if let Some((_, resampler)) = &mut self.output_resampler {
            resampler.reset();
        }
    }
    /// Flush the lookahead tail after the final inbound audio packet.
    pub fn flush_input(&mut self) -> Result<Option<Frame>, ProtocolError> {
        let bytes = self.input_resampler.flush()?;
        Ok(self.input_frame(bytes))
    }
    /// Flush the lookahead tail before gracefully ending playback. An interruption
    /// discards this tail instead, so interrupted audio cannot leak into later turns.
    pub fn flush_output(&mut self) -> Result<Option<WireMessage>, ProtocolError> {
        let Some((_, resampler)) = &mut self.output_resampler else {
            return Ok(None);
        };
        let pcm = resampler.flush()?;
        self.codec.encode_pcm16(&pcm, self.codec.sample_rate())
    }
    pub fn try_deserialize(
        &mut self,
        message: &WireMessage,
    ) -> Result<Option<Frame>, ProtocolError> {
        let event = self.codec.parse(message)?;
        match event.kind {
            EventKind::Media { audio, .. } => {
                let pcm = audio.to_pcm16()?;
                let pcm = self.input_resampler.process(&pcm)?;
                Ok(self.input_frame(pcm))
            }
            EventKind::Dtmf { digit, .. } => {
                Ok(Some(Frame::new(FrameData::InputDtmf(digit.into()))))
            }
            _ => Ok(None),
        }
    }
    fn input_frame(&self, bytes: Vec<u8>) -> Option<Frame> {
        if bytes.is_empty() {
            return None;
        }
        Some(Frame::new(FrameData::InputAudio(AudioRawFrame {
            audio: bytes,
            sample_rate: self.input_rate,
            num_channels: 1,
            context_id: None,
        })))
    }
}
impl FrameSerializer for TelephonyFrameSerializer {
    fn serialize(&mut self, frame: &Frame) -> Result<Option<WireMessage>, ProtocolError> {
        crate::reject_openclaw(frame)?;
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| ProtocolError::InvalidField("unconsumed native LLM continuation"))?;
        match &frame.data {
            FrameData::Interruption => {
                if let Some((_, resampler)) = &mut self.output_resampler {
                    resampler.reset();
                }
                self.codec.clear().map(Some)
            }
            FrameData::InputAudio(audio)
            | FrameData::OutputAudio(audio)
            | FrameData::TtsAudio(audio) => {
                if audio.num_channels != 1 || !audio.audio.len().is_multiple_of(2) {
                    return Err(ProtocolError::InvalidField("telephony PCM16 mono audio"));
                }
                if self.output_resampler.as_ref().map(|(rate, _)| *rate) != Some(audio.sample_rate)
                {
                    // A rate change with buffered samples would silently lose the tail.
                    if self
                        .output_resampler
                        .as_ref()
                        .is_some_and(|(_, r)| r.buffered_frames() > 0)
                    {
                        return Err(ProtocolError::State(
                            "flush output before changing sample rate",
                        ));
                    }
                    self.output_resampler = Some((
                        audio.sample_rate,
                        SincResampler::new(audio.sample_rate, self.codec.sample_rate(), 1)?,
                    ));
                }
                let (_, resampler) = self
                    .output_resampler
                    .as_mut()
                    .expect("resampler initialized");
                let pcm = resampler.process(&audio.audio)?;
                self.codec.encode_pcm16(&pcm, self.codec.sample_rate())
            }
            FrameData::OutputTransportMessage(message)
            | FrameData::OutputTransportMessageUrgent(message) => {
                self.codec.transport_message(message)
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
impl From<DtmfDigit> for KeypadEntry {
    fn from(d: DtmfDigit) -> Self {
        match d {
            DtmfDigit::Zero => Self::Zero,
            DtmfDigit::One => Self::One,
            DtmfDigit::Two => Self::Two,
            DtmfDigit::Three => Self::Three,
            DtmfDigit::Four => Self::Four,
            DtmfDigit::Five => Self::Five,
            DtmfDigit::Six => Self::Six,
            DtmfDigit::Seven => Self::Seven,
            DtmfDigit::Eight => Self::Eight,
            DtmfDigit::Nine => Self::Nine,
            DtmfDigit::Star => Self::Star,
            DtmfDigit::Pound => Self::Pound,
        }
    }
}

/// The same frame mappings as Python's RTVIClientSerializer. Handshake, metrics,
/// bot audio, and UI messages stay available through `rtvi::Envelope` separately.
#[derive(Default)]
pub struct RtviClientFrameSerializer {
    codec: rtvi::RtviClientCodec,
}
impl FrameSerializer for RtviClientFrameSerializer {
    fn serialize(&mut self, frame: &Frame) -> Result<Option<WireMessage>, ProtocolError> {
        crate::reject_openclaw(frame)?;
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| ProtocolError::InvalidField("unconsumed native LLM continuation"))?;
        match &frame.data {
            FrameData::OutputAudio(audio) | FrameData::TtsAudio(audio) => self
                .codec
                .encode_audio(&audio.audio, audio.sample_rate, audio.num_channels)
                .map(WireMessage::Text)
                .map(Some),
            FrameData::OutputTransportMessage(message)
            | FrameData::OutputTransportMessageUrgent(message)
                if message.get("label").and_then(Value::as_str) == Some(rtvi::MESSAGE_LABEL) =>
            {
                let text = serde_json::to_string(message)?;
                crate::check_size(text.len(), crate::DEFAULT_MAX_MESSAGE_BYTES)?;
                Ok(Some(WireMessage::Text(text)))
            }
            _ => Ok(None),
        }
    }
    fn deserialize(&mut self, message: &WireMessage) -> Result<Option<Frame>, ProtocolError> {
        let bytes = match message {
            WireMessage::Text(text) => text.as_bytes(),
            WireMessage::Binary(bytes) => bytes,
        };
        let Ok(envelope) = rtvi::Envelope::parse(bytes) else {
            return Ok(None);
        };
        let payload = match envelope.data {
            Some(Value::Object(map)) => Value::Object(map),
            None | Some(Value::Null) => json!({}),
            _ => return Ok(None),
        };
        let string = |key: &str| {
            payload
                .get(key)
                .and_then(Value::as_str)
                .unwrap_or_default()
                .to_owned()
        };
        let data = match envelope.message_type.as_str() {
            "bot-llm-started" => FrameData::LlmResponseStart,
            "bot-llm-stopped" => FrameData::LlmResponseEnd,
            "bot-llm-text" => FrameData::LlmText(TextFrame::llm(string("text"))),
            "bot-llm-marker" => {
                FrameData::LlmMarkerResponse(pipecat_core::LlmMarkerResponseFrame {
                    raw: string("raw"),
                    marker: Some(string("text")).filter(|s| !s.is_empty()),
                    kind: payload
                        .get("kind")
                        .and_then(Value::as_str)
                        .map(str::to_owned),
                    markers: payload
                        .get("markers")
                        .and_then(Value::as_array)
                        .map(|values| {
                            values
                                .iter()
                                .filter_map(Value::as_str)
                                .map(str::to_owned)
                                .collect()
                        })
                        .unwrap_or_default(),
                })
            }
            "bot-tts-text" => FrameData::TtsText(pipecat_core::AggregatedTextFrame {
                text: TextFrame::new(string("text")),
                aggregated_by: "sentence".into(),
                context_id: None,
                raw_text: None,
                will_be_spoken: true,
            }),
            "bot-started-speaking" => FrameData::BotStartedSpeaking,
            "bot-stopped-speaking" => FrameData::BotStoppedSpeaking,
            "bot-interrupted" => FrameData::Interruption,
            "llm-function-call-in-progress" => {
                FrameData::FunctionCallInProgress(pipecat_core::FunctionCallInProgressFrame {
                    function_name: string("function_name"),
                    tool_call_id: string("tool_call_id"),
                    arguments: payload
                        .get("arguments")
                        .filter(|v| v.is_object())
                        .cloned()
                        .unwrap_or_else(|| json!({})),
                    cancel_on_interruption: true,
                    group_id: None,
                })
            }
            "llm-function-call-stopped"
                if payload.get("cancelled").and_then(Value::as_bool) == Some(true) =>
            {
                FrameData::FunctionCallCancel(pipecat_core::FunctionCallCancelFrame {
                    function_name: string("function_name"),
                    tool_call_id: string("tool_call_id"),
                    run_llm: true,
                })
            }
            "llm-function-call-stopped" => {
                FrameData::FunctionCallResult(pipecat_core::FunctionCallResultFrame {
                    function_name: string("function_name"),
                    tool_call_id: string("tool_call_id"),
                    arguments: json!({}),
                    result: payload.get("result").cloned().unwrap_or(Value::Null),
                    run_llm: None,
                    is_final: true,
                    error: None,
                })
            }
            "user-transcription" => {
                let is_final = payload
                    .get("final")
                    .and_then(Value::as_bool)
                    .unwrap_or(true);
                let frame = TranscriptionFrame {
                    text: string("text"),
                    user_id: string("user_id"),
                    timestamp: string("timestamp"),
                    language: None,
                    result: None,
                    finalized: false,
                };
                if is_final {
                    FrameData::Transcription(frame)
                } else {
                    FrameData::InterimTranscription(frame)
                }
            }
            _ => return Ok(None),
        };
        Ok(Some(Frame::new(data)))
    }
}
