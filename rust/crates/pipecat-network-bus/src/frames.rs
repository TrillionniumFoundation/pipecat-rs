use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_core::*;
use serde_json::{json, Map, Value};

use crate::codec::{
    defaulted, invalid, object_tag, optional, put_optional, required, tag, CodecError,
};
#[path = "frame_context.rs"]
mod context;
#[path = "frame_extended.rs"]
mod extended;
#[path = "frame_settings.rs"]
mod settings;

const PREFIX: &str = "pipecat.frames.frames.";
const CONTEXT: &str = "pipecat.processors.aggregators.llm_context.LLMContext";

pub(crate) fn supported_frame(name: &str) -> bool {
    name.strip_prefix(PREFIX)
        .is_some_and(|name| extended::TYPES.contains(&name))
        || matches!(
            name.strip_prefix(PREFIX),
            Some(
                "TextFrame"
                    | "LLMTextFrame"
                    | "InputTextRawFrame"
                    | "VisionTextFrame"
                    | "LLMThoughtTextFrame"
                    | "InputAudioRawFrame"
                    | "OutputAudioRawFrame"
                    | "TTSAudioRawFrame"
                    | "TTSSpeakFrame"
                    | "TTSStartedFrame"
                    | "TTSStoppedFrame"
                    | "StartFrame"
                    | "EndFrame"
                    | "StopFrame"
                    | "CancelFrame"
                    | "InterruptionFrame"
                    | "LLMFullResponseStartFrame"
                    | "LLMFullResponseEndFrame"
                    | "LLMRunFrame"
                    | "LLMContextFrame"
                    | "UserStartedSpeakingFrame"
                    | "UserStoppedSpeakingFrame"
                    | "UserSpeakingFrame"
                    | "BotStartedSpeakingFrame"
                    | "BotStoppedSpeakingFrame"
                    | "BotSpeakingFrame"
                    | "UserMuteStartedFrame"
                    | "UserMuteStoppedFrame"
                    | "InputTransportMessageFrame"
                    | "OutputTransportMessageFrame"
                    | "OutputTransportMessageUrgentFrame"
                    | "BotConnectedFrame"
                    | "ClientConnectedFrame"
                    | "OutputTransportReadyFrame"
                    | "InputTransportStartAudioStreamingFrame"
                    | "HeartbeatFrame"
                    | "LLMThoughtStartFrame"
                    | "LLMThoughtEndFrame"
                    | "EagerEndOfTurnCancelFrame"
                    | "ProposedUserStartedSpeakingFrame"
                    | "ProposedUserStoppedSpeakingFrame"
                    | "AudioBufferStartRecordingFrame"
                    | "AudioBufferStopRecordingFrame"
            )
        )
}

pub(crate) fn decode_bytes(value: &Value) -> Result<Vec<u8>, CodecError> {
    if value.get("__type__").and_then(Value::as_str) != Some("bytes") {
        return Err(invalid("bytes.__type__"));
    }
    STANDARD
        .decode(
            value
                .get("__data__")
                .and_then(Value::as_str)
                .ok_or_else(|| invalid("bytes.__data__"))?,
        )
        .map_err(|_| invalid("base64"))
}

pub(crate) fn decode_frame(value: &Value) -> Result<Frame, CodecError> {
    let (qualified, f) = object_tag(value)?;
    let name = qualified
        .strip_prefix(PREFIX)
        .ok_or_else(|| CodecError::UnsupportedType(qualified.into()))?;
    let data = match name {
        "TextFrame"
        | "LLMTextFrame"
        | "InputTextRawFrame"
        | "VisionTextFrame"
        | "LLMThoughtTextFrame" => {
            let text = TextFrame {
                text: required(f, "text")?,
                skip_tts: optional(f, "skip_tts")?,
                includes_inter_frame_spaces: defaulted(
                    f,
                    "includes_inter_frame_spaces",
                    matches!(
                        name,
                        "LLMTextFrame" | "VisionTextFrame" | "LLMThoughtTextFrame"
                    ),
                )?,
                append_to_context: defaulted(f, "append_to_context", true)?,
            };
            match name {
                "LLMTextFrame" => FrameData::LlmText(text),
                "InputTextRawFrame" => FrameData::InputText(text),
                "VisionTextFrame" => FrameData::VisionText(text),
                "LLMThoughtTextFrame" => FrameData::LlmThoughtText(text),
                _ => FrameData::Text(text),
            }
        }
        "InputAudioRawFrame" | "OutputAudioRawFrame" | "TTSAudioRawFrame" => {
            let mut audio = AudioRawFrame::new(
                decode_bytes(f.get("audio").ok_or_else(|| invalid("audio"))?)?,
                required(f, "sample_rate")?,
                required(f, "num_channels")?,
            )
            .map_err(|_| invalid("audio format"))?;
            audio.context_id = optional(f, "context_id")?;
            if let Some(num_frames) = optional::<usize>(f, "num_frames")? {
                if num_frames != audio.audio.len() / (usize::from(audio.num_channels) * 2) {
                    return Err(invalid("num_frames"));
                }
            }
            match name {
                "InputAudioRawFrame" => FrameData::InputAudio(audio),
                "TTSAudioRawFrame" => FrameData::TtsAudio(audio),
                _ => FrameData::OutputAudio(audio),
            }
        }
        "TTSSpeakFrame" => FrameData::TtsSpeak(TtsSpeakFrame {
            text: required(f, "text")?,
            append_to_context: defaulted(f, "append_to_context", true)?,
        }),
        "TTSStartedFrame" => FrameData::TtsStarted(TtsStartedFrame {
            context_id: optional(f, "context_id")?,
            append_to_context: defaulted(f, "append_to_context", true)?,
        }),
        "TTSStoppedFrame" => FrameData::TtsStopped(TtsStoppedFrame {
            context_id: optional(f, "context_id")?,
        }),
        "StartFrame" => {
            extended::no_local(f, &["tracing_context"])?;
            let configuration = StartFrame {
                audio_in_sample_rate: defaulted(f, "audio_in_sample_rate", 16000)?,
                audio_out_sample_rate: defaulted(f, "audio_out_sample_rate", 24000)?,
                enable_metrics: defaulted(f, "enable_metrics", false)?,
                enable_usage_metrics: defaulted(f, "enable_usage_metrics", false)?,
                report_only_initial_ttfb: defaulted(f, "report_only_initial_ttfb", false)?,
            };
            if defaulted(f, "enable_tracing", false)? {
                FrameData::StartDetailed(DetailedStartFrame {
                    configuration,
                    enable_tracing: true,
                    tracing_context: None,
                })
            } else {
                FrameData::Start(configuration)
            }
        }
        "EndFrame"
            if f.get("reason")
                .is_some_and(|v| !v.is_null() && !v.is_string()) =>
        {
            FrameData::EndDetailed(ReasonFrame {
                reason: f.get("reason").cloned(),
            })
        }
        "EndFrame" => FrameData::End(EndFrame {
            reason: optional(f, "reason")?,
        }),
        "CancelFrame"
            if f.get("reason")
                .is_some_and(|v| !v.is_null() && !v.is_string()) =>
        {
            FrameData::CancelDetailed(ReasonFrame {
                reason: f.get("reason").cloned(),
            })
        }
        "CancelFrame" => FrameData::Cancel(CancelFrame {
            reason: optional(f, "reason")?,
        }),
        "StopFrame" => FrameData::Stop,
        "InterruptionFrame" => FrameData::Interruption,
        "LLMFullResponseStartFrame" | "LLMFullResponseEndFrame" => {
            let skip_tts: Option<bool> = optional(f, "skip_tts")?;
            match (name, skip_tts) {
                ("LLMFullResponseStartFrame", None) => FrameData::LlmResponseStart,
                ("LLMFullResponseEndFrame", None) => FrameData::LlmResponseEnd,
                ("LLMFullResponseStartFrame", value) => {
                    FrameData::LlmFullResponseStart(LlmFullResponseFrame { skip_tts: value })
                }
                (_, value) => {
                    FrameData::LlmFullResponseEnd(LlmFullResponseFrame { skip_tts: value })
                }
            }
        }
        "LLMRunFrame" => FrameData::LlmRun,
        "LLMThoughtStartFrame" => FrameData::LlmThoughtStartDetailed(extended::parse(
            f,
            json!({"append_to_context":false}),
        )?),
        "LLMThoughtEndFrame" => FrameData::LlmThoughtEndDetailed(extended::parse(f, json!({}))?),
        "LLMContextFrame" => FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: context::decode(f.get("context").ok_or_else(|| invalid("context"))?)?,
            speculation: defaulted(f, "speculation", false)?,
        }),
        "UserStartedSpeakingFrame" => FrameData::UserStartedSpeaking,
        "UserStoppedSpeakingFrame" => FrameData::UserStoppedSpeaking,
        "UserSpeakingFrame" => FrameData::UserSpeaking,
        "BotStartedSpeakingFrame" => FrameData::BotStartedSpeaking,
        "BotStoppedSpeakingFrame" => FrameData::BotStoppedSpeaking,
        "BotSpeakingFrame" => FrameData::BotSpeaking,
        "UserMuteStartedFrame" => FrameData::UserMuteStarted,
        "UserMuteStoppedFrame" => FrameData::UserMuteStopped,
        "EagerEndOfTurnCancelFrame" => FrameData::EagerEndOfTurnCancel,
        "ProposedUserStartedSpeakingFrame" => FrameData::ProposedUserStartedSpeaking,
        "ProposedUserStoppedSpeakingFrame" => FrameData::ProposedUserStoppedSpeaking,
        "InputTransportMessageFrame" => {
            FrameData::InputTransportMessage(required_value(f, "message")?)
        }
        "OutputTransportMessageFrame" => {
            FrameData::OutputTransportMessage(required_value(f, "message")?)
        }
        "OutputTransportMessageUrgentFrame" => {
            FrameData::OutputTransportMessageUrgent(required_value(f, "message")?)
        }
        "BotConnectedFrame" => FrameData::BotConnected,
        "ClientConnectedFrame" => FrameData::ClientConnected,
        "OutputTransportReadyFrame" => FrameData::OutputTransportReady,
        "InputTransportStartAudioStreamingFrame" => FrameData::InputTransportStartAudioStreaming,
        "HeartbeatFrame" => FrameData::Heartbeat {
            timestamp: required(f, "timestamp")?,
        },
        "AudioBufferStartRecordingFrame" => FrameData::AudioBufferStartRecording,
        "AudioBufferStopRecordingFrame" => FrameData::AudioBufferStopRecording,
        _ => extended::decode(name, f)?
            .ok_or_else(|| CodecError::UnsupportedType(qualified.into()))?,
    };
    let mut frame = Frame::new(data);
    if let Some(id) = optional(f, "id")? {
        frame.id = id;
    }
    frame.pts = optional(f, "pts")?;
    frame.broadcast_sibling_id = optional(f, "broadcast_sibling_id")?;
    frame.metadata = match f.get("metadata") {
        None => Default::default(),
        Some(Value::Object(metadata)) => metadata.clone().into_iter().collect(),
        Some(_) => return Err(invalid("metadata")),
    };
    frame.transport_source = optional(f, "transport_source")?;
    frame.transport_destination = optional(f, "transport_destination")?;
    frame.interruptible = defaulted(f, "interruptible", frame.interruptible)?;
    if let Some(name) = optional(f, "name")? {
        frame.name = name;
    }
    Ok(frame)
}

pub(crate) fn encode_frame(frame: &Frame) -> Result<Value, CodecError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| invalid("unconsumed native LLM continuation"))?;
    frame
        .require_wire_pts()
        .map_err(|_| invalid("pipeline-clock PTS must be rebased"))?;
    let (name, payload) = match &frame.data {
        FrameData::Text(text) => ("TextFrame", serde_json::to_value(text)?),
        FrameData::LlmText(text) => ("LLMTextFrame", serde_json::to_value(text)?),
        FrameData::InputText(text) => ("InputTextRawFrame", serde_json::to_value(text)?),
        FrameData::VisionText(text) => ("VisionTextFrame", serde_json::to_value(text)?),
        FrameData::LlmThoughtText(text) => ("LLMThoughtTextFrame", serde_json::to_value(text)?),
        FrameData::InputAudio(audio)
        | FrameData::OutputAudio(audio)
        | FrameData::TtsAudio(audio) => {
            audio.validate().map_err(|_| invalid("audio format"))?;
            (
                match &frame.data {
                    FrameData::InputAudio(_) => "InputAudioRawFrame",
                    FrameData::TtsAudio(_) => "TTSAudioRawFrame",
                    _ => "OutputAudioRawFrame",
                },
                json!({"audio":tag("bytes", STANDARD.encode(&audio.audio).into()), "sample_rate":audio.sample_rate,"num_channels":audio.num_channels,"num_frames":audio.audio.len() / (usize::from(audio.num_channels)*2), "context_id":audio.context_id}),
            )
        }
        FrameData::TtsSpeak(speak) => ("TTSSpeakFrame", serde_json::to_value(speak)?),
        FrameData::TtsStarted(started) => ("TTSStartedFrame", serde_json::to_value(started)?),
        FrameData::TtsStopped(stopped) => ("TTSStoppedFrame", serde_json::to_value(stopped)?),
        FrameData::Start(start) => ("StartFrame", serde_json::to_value(start)?),
        FrameData::StartDetailed(start) => {
            if start.tracing_context.is_some() {
                return Err(invalid("tracing_context has no source bus adapter"));
            }
            let mut payload = serde_json::to_value(&start.configuration)?;
            payload["enable_tracing"] = start.enable_tracing.into();
            ("StartFrame", payload)
        }
        FrameData::End(end) => ("EndFrame", serde_json::to_value(end)?),
        FrameData::EndDetailed(end) => ("EndFrame", serde_json::to_value(end)?),
        FrameData::Cancel(cancel) => ("CancelFrame", serde_json::to_value(cancel)?),
        FrameData::CancelDetailed(cancel) => ("CancelFrame", serde_json::to_value(cancel)?),
        FrameData::Stop => ("StopFrame", json!({})),
        FrameData::Interruption => ("InterruptionFrame", json!({})),
        FrameData::LlmResponseStart => ("LLMFullResponseStartFrame", json!({})),
        FrameData::LlmResponseEnd => ("LLMFullResponseEndFrame", json!({})),
        FrameData::LlmFullResponseStart(response) => {
            ("LLMFullResponseStartFrame", serde_json::to_value(response)?)
        }
        FrameData::LlmFullResponseEnd(response) => {
            ("LLMFullResponseEndFrame", serde_json::to_value(response)?)
        }
        FrameData::LlmRun => ("LLMRunFrame", json!({})),
        FrameData::LlmThoughtStart => ("LLMThoughtStartFrame", json!({})),
        FrameData::LlmThoughtEnd => ("LLMThoughtEndFrame", json!({})),
        FrameData::LlmThoughtStartDetailed(value) => {
            ("LLMThoughtStartFrame", extended::plain(value)?)
        }
        FrameData::LlmThoughtEndDetailed(value) => ("LLMThoughtEndFrame", extended::plain(value)?),
        FrameData::LlmContextDetailed(value) => (
            "LLMContextFrame",
            json!({"context":context::encode(&value.context)?,"speculation":value.speculation}),
        ),
        FrameData::LlmContext(context) => (
            "LLMContextFrame",
            json!({"context":context::encode(&context.clone().into())?}),
        ),
        FrameData::UserStartedSpeaking => ("UserStartedSpeakingFrame", json!({})),
        FrameData::UserStoppedSpeaking => ("UserStoppedSpeakingFrame", json!({})),
        FrameData::UserSpeaking => ("UserSpeakingFrame", json!({})),
        FrameData::BotStartedSpeaking => ("BotStartedSpeakingFrame", json!({})),
        FrameData::BotStoppedSpeaking => ("BotStoppedSpeakingFrame", json!({})),
        FrameData::BotSpeaking => ("BotSpeakingFrame", json!({})),
        FrameData::UserMuteStarted => ("UserMuteStartedFrame", json!({})),
        FrameData::UserMuteStopped => ("UserMuteStoppedFrame", json!({})),
        FrameData::EagerEndOfTurnCancel => ("EagerEndOfTurnCancelFrame", json!({})),
        FrameData::ProposedUserStartedSpeaking => ("ProposedUserStartedSpeakingFrame", json!({})),
        FrameData::ProposedUserStoppedSpeaking => ("ProposedUserStoppedSpeakingFrame", json!({})),
        FrameData::InputTransportMessage(message) => {
            ("InputTransportMessageFrame", json!({"message":message}))
        }
        FrameData::OutputTransportMessage(message) => {
            ("OutputTransportMessageFrame", json!({"message":message}))
        }
        FrameData::OutputTransportMessageUrgent(message) => (
            "OutputTransportMessageUrgentFrame",
            json!({"message":message}),
        ),
        FrameData::BotConnected => ("BotConnectedFrame", json!({})),
        FrameData::ClientConnected => ("ClientConnectedFrame", json!({})),
        FrameData::OutputTransportReady => ("OutputTransportReadyFrame", json!({})),
        FrameData::InputTransportStartAudioStreaming => {
            ("InputTransportStartAudioStreamingFrame", json!({}))
        }
        FrameData::Heartbeat { timestamp } => ("HeartbeatFrame", json!({"timestamp":timestamp})),
        FrameData::AudioBufferStartRecording => ("AudioBufferStartRecordingFrame", json!({})),
        FrameData::AudioBufferStopRecording => ("AudioBufferStopRecordingFrame", json!({})),
        // Gateway frames have a service-specific source module and native
        // outcome semantics. Reject rather than invent a Python wire type.
        FrameData::OpenClawSend(_)
        | FrameData::OpenClawSteer(_)
        | FrameData::OpenClawAbort(_)
        | FrameData::OpenClawStarted(_)
        | FrameData::OpenClawText(_)
        | FrameData::OpenClawEnd(_) => {
            return Err(CodecError::UnsupportedFrame(
                frame.data.source_class_name().into(),
            ));
        }
        // Diagnostics contain class names, never payloads.
        other => extended::encode(other)?
            .ok_or_else(|| CodecError::UnsupportedFrame(other.source_class_name().into()))?,
    };
    let mut f = payload
        .as_object()
        .cloned()
        .ok_or_else(|| invalid("frame payload"))?;
    f.retain(|_, value| !value.is_null());
    f.insert("id".into(), frame.id.into());
    f.insert("name".into(), frame.name.clone().into());
    f.insert("metadata".into(), serde_json::to_value(&frame.metadata)?);
    f.insert("interruptible".into(), frame.interruptible.into());
    put_optional(&mut f, "pts", &frame.pts)?;
    put_optional(&mut f, "broadcast_sibling_id", &frame.broadcast_sibling_id)?;
    put_optional(&mut f, "transport_source", &frame.transport_source)?;
    put_optional(
        &mut f,
        "transport_destination",
        &frame.transport_destination,
    )?;
    Ok(tag(&format!("{PREFIX}{name}"), f.into()))
}

fn required_value(fields: &Map<String, Value>, key: &str) -> Result<Value, CodecError> {
    fields.get(key).cloned().ok_or_else(|| invalid(key))
}

/// Validate auxiliary source tags against a static allowlist.
pub(crate) fn supported_aux_type(name: &str, data: &Value) -> Result<bool, CodecError> {
    extended::validate_aux(name, data)
}

#[cfg(test)]
mod timing_boundary_tests {
    use super::*;
    #[test]
    fn pipeline_clock_requires_explicit_rebase() {
        let mut frame = Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap(),
        ));
        frame.set_pipeline_pts(100);
        assert!(encode_frame(&frame).is_err());
        frame.rebase_pipeline_pts(75).unwrap();
        assert_eq!(frame.pts, Some(25));
        assert!(encode_frame(&frame).is_ok());
    }
}

#[cfg(test)]
mod native_history_boundary_tests {
    use super::*;
    use pipecat_core::{LlmContinuationProvider, NativeLlmContinuation};
    use std::sync::Arc;
    #[test]
    fn native_history_requires_consumption_even_if_the_payload_changes() {
        let mut frame = Frame::new(FrameData::LlmResponseEnd);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::new(1, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
            ))
            .unwrap();
        assert!(encode_frame(&frame).is_err());
        frame.data = FrameData::Text(TextFrame::new("changed"));
        assert!(encode_frame(&frame).is_err());
        frame.data = FrameData::LlmResponseEnd;
        frame.take_native_llm_continuation().unwrap();
        assert!(encode_frame(&frame).is_ok());
    }
}
