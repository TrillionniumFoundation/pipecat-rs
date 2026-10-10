//! Protobuf framing with a small, explicit client control protocol.
use crate::AgentError;
use pipecat_core::{AudioRawFrame, Frame, FrameData, TextFrame, VadSpeakingFrame};
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use pipecat_transports::Packet;
use serde::Deserialize;
use serde_json::Value;

#[derive(Debug)]
pub enum ClientInput {
    Frame(Box<Frame>),
    PlaybackComplete(String),
    End,
}
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "kebab-case", deny_unknown_fields)]
enum Control {
    UserStartedSpeaking,
    UserStoppedSpeaking {
        #[serde(default)]
        duration_secs: f64,
    },
    PlaybackComplete {
        context_id: String,
    },
    Interrupt,
    End,
}
pub fn decode(packet: Packet, input_rate: u32) -> Result<ClientInput, AgentError> {
    let kind = match packet {
        Packet::Binary(bytes) => pb::Frame::parse(&bytes)?
            .frame
            .ok_or(AgentError::Protocol("missing frame"))?,
        Packet::Text(text) => Kind::Message(pb::MessageFrame { data: text }),
    };
    let frame = match kind {
        Kind::Audio(audio) => {
            if audio.sample_rate != input_rate || audio.num_channels != 1 {
                return Err(AgentError::Protocol(
                    "input must use the configured mono PCM16 sample rate",
                ));
            }
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(audio.audio, audio.sample_rate, 1)
                    .map_err(|_| AgentError::Protocol("invalid PCM16 audio"))?,
            ))
        }
        Kind::Text(text) => Frame::new(FrameData::InputText(TextFrame::new(text.text))),
        Kind::Interruption(_) => Frame::interruption(),
        Kind::Transcription(_) => {
            return Err(AgentError::Protocol(
                "client transcription injection is not supported; use TextFrame",
            ))
        }
        Kind::Message(message) => {
            let control: Control = serde_json::from_str(&message.data)
                .map_err(|_| AgentError::Protocol("invalid client control message"))?;
            match control {
                Control::PlaybackComplete { context_id } => {
                    if context_id.is_empty() || context_id.len() > 256 {
                        return Err(AgentError::Protocol("invalid playback context ID"));
                    }
                    return Ok(ClientInput::PlaybackComplete(context_id));
                }
                Control::End => return Ok(ClientInput::End),
                Control::Interrupt => Frame::interruption(),
                Control::UserStartedSpeaking => {
                    Frame::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                        duration_secs: 0.0,
                        timestamp: 0.0,
                    }))
                }
                Control::UserStoppedSpeaking { duration_secs } => {
                    if !duration_secs.is_finite() || !(0.0..=60.0).contains(&duration_secs) {
                        return Err(AgentError::Protocol("invalid VAD stop duration"));
                    }
                    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                        duration_secs,
                        timestamp: 0.0,
                    }))
                }
            }
        }
    };
    Ok(ClientInput::Frame(Box::new(frame)))
}
pub fn message(value: Value) -> Result<Packet, AgentError> {
    packet(Kind::Message(pb::MessageFrame {
        data: value.to_string(),
    }))
}
pub fn packet(kind: Kind) -> Result<Packet, AgentError> {
    Ok(Packet::Binary(
        pb::Frame { frame: Some(kind) }.encode_checked()?.into(),
    ))
}
pub fn audio(frame: &Frame, audio: &AudioRawFrame) -> Result<Packet, AgentError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| AgentError::Protocol("unconsumed native LLM continuation"))?;
    frame
        .require_wire_pts()
        .map_err(|_| AgentError::Protocol("pipeline-clock PTS must be rebased"))?;
    packet(Kind::Audio(pb::AudioRawFrame {
        id: frame.id,
        name: format!("OutputAudioRawFrame#{}", frame.id),
        audio: audio.audio.clone(),
        sample_rate: audio.sample_rate,
        num_channels: u32::from(audio.num_channels),
        pts: frame.pts,
    }))
}
pub fn interruption(frame: &Frame) -> Result<Packet, AgentError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| AgentError::Protocol("unconsumed native LLM continuation"))?;
    packet(Kind::Interruption(pb::InterruptionFrame {
        id: frame.id,
        name: format!("InterruptionFrame#{}", frame.id),
    }))
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_untrusted_spoken_controls() {
        assert!(decode(
            Packet::Text(r#"{"type":"playback-complete","context_id":"x","spoken":true}"#.into()),
            16000
        )
        .is_err());
        assert!(decode(
            Packet::Text(r#"{"type":"user-stopped-speaking","duration_secs":-1}"#.into()),
            16000
        )
        .is_err());
    }
    #[test]
    fn audio_requires_exact_declared_format() {
        let p = packet(Kind::Audio(pb::AudioRawFrame {
            id: 1,
            name: String::new(),
            audio: vec![0, 1],
            sample_rate: 24000,
            num_channels: 1,
            pts: None,
        }))
        .unwrap();
        assert!(decode(p, 16000).is_err());
    }
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
        assert!(audio(
            &frame,
            match &frame.data {
                FrameData::OutputAudio(a) => a,
                _ => unreachable!(),
            }
        )
        .is_err());
        frame.rebase_pipeline_pts(75).unwrap();
        assert_eq!(frame.pts, Some(25));
        assert!(audio(
            &frame,
            match &frame.data {
                FrameData::OutputAudio(a) => a,
                _ => unreachable!(),
            }
        )
        .is_ok());
    }
}

#[cfg(test)]
mod native_history_boundary_tests {
    use super::*;
    use pipecat_core::{LlmContinuationProvider, NativeLlmContinuation};
    use std::sync::Arc;
    #[test]
    fn every_frame_packet_entrypoint_requires_consumed_native_history() {
        let mut frame = Frame::new(FrameData::LlmResponseEnd);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::new(1, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
            ))
            .unwrap();
        let pcm = AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap();
        assert!(audio(&frame, &pcm).is_err());
        assert!(interruption(&frame).is_err());
        frame.take_native_llm_continuation().unwrap();
        assert!(audio(&frame, &pcm).is_ok());
        assert!(interruption(&frame).is_ok());
    }
}
