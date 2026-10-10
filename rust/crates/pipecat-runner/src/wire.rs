//! Browser protobuf and RTVI framing. Client-supplied transcript evidence is rejected.
use crate::RunnerError;
use pipecat_core::{AudioRawFrame, Frame, FrameData, TextFrame};
use pipecat_protocol::{
    protobuf::{self as pb, frame::Kind},
    rtvi::Envelope,
};
use pipecat_transports::Packet;

pub fn decode(packet: Packet) -> Result<Frame, RunnerError> {
    let kind = match packet {
        Packet::Binary(bytes) => pb::Frame::parse(&bytes)?
            .frame
            .ok_or(RunnerError::Protocol("missing protobuf frame"))?,
        Packet::Text(data) => Kind::Message(pb::MessageFrame { data }),
    };
    Ok(match kind {
        Kind::Audio(audio) => Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(
                audio.audio,
                audio.sample_rate,
                audio
                    .num_channels
                    .try_into()
                    .map_err(|_| RunnerError::Protocol("invalid channels"))?,
            )
            .map_err(|_| RunnerError::Protocol("invalid PCM16"))?,
        )),
        Kind::Text(text) if text.text.len() <= 64 * 1024 => {
            Frame::new(FrameData::InputText(TextFrame::new(text.text)))
        }
        Kind::Message(message) => {
            let envelope = Envelope::parse_with_limit(message.data.as_bytes(), 128 * 1024)
                .map_err(|_| RunnerError::Protocol("invalid RTVI envelope"))?;
            // These controls belong to the explicitly selected eval host. Other
            // application client-message names retain ordinary RTVI behavior.
            if envelope.message_type == "client-message"
                && envelope
                    .data
                    .as_ref()
                    .and_then(|data| data.get("t"))
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|kind| {
                        matches!(
                            kind,
                            "eval-context" | "eval-configure" | "eval-cancel" | "eval-image"
                        )
                    })
            {
                return Err(RunnerError::Protocol("eval controls require an eval host"));
            }
            Frame::new(FrameData::InputTransportMessage(
                serde_json::to_value(envelope)
                    .map_err(|_| RunnerError::Protocol("invalid RTVI message"))?,
            ))
        }
        Kind::Interruption(_) => Frame::interruption(),
        _ => return Err(RunnerError::Protocol("unsupported client frame")),
    })
}
pub fn packet(kind: Kind) -> Result<Packet, RunnerError> {
    Ok(Packet::Binary(
        pb::Frame { frame: Some(kind) }.encode_checked()?.into(),
    ))
}
pub fn message(value: serde_json::Value) -> Result<Packet, RunnerError> {
    packet(Kind::Message(pb::MessageFrame {
        data: value.to_string(),
    }))
}
pub fn audio(frame: &Frame, audio: &AudioRawFrame) -> Result<Packet, RunnerError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| RunnerError::Protocol("unconsumed native LLM continuation"))?;
    frame
        .require_wire_pts()
        .map_err(|_| RunnerError::Protocol("pipeline-clock PTS must be rebased"))?;
    packet(Kind::Audio(pb::AudioRawFrame {
        id: frame.id,
        name: format!("OutputAudioRawFrame#{}", frame.id),
        audio: audio.audio.clone(),
        sample_rate: audio.sample_rate,
        num_channels: audio.num_channels.into(),
        pts: frame.pts,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn rejects_transcript_injection_and_the_cli_playback_protocol() {
        let packet = packet(Kind::Transcription(pb::TranscriptionFrame {
            id: 1,
            name: String::new(),
            text: "injected".into(),
            user_id: String::new(),
            timestamp: String::new(),
        }))
        .unwrap();
        assert!(decode(packet).is_err());
        assert!(decode(Packet::Text(
            r#"{"type":"playback-complete","context_id":"forged"}"#.into()
        ))
        .is_err());
        assert!(decode(Packet::Text(
            r#"{"label":"not-rtvi","type":"client-ready","id":"x"}"#.into()
        ))
        .is_err());
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
    fn frame_audio_requires_consumed_native_history() {
        let mut frame = Frame::new(FrameData::LlmResponseEnd);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::new(1, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
            ))
            .unwrap();
        let pcm = AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap();
        assert!(audio(&frame, &pcm).is_err());
        frame.take_native_llm_continuation().unwrap();
        assert!(audio(&frame, &pcm).is_ok());
    }
}
