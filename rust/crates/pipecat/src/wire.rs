//! Protobuf wire boundaries between native frames and WebSocket clients.
use crate::{
    core::{AudioRawFrame, Frame, FrameData, TranscriptionFrame},
    protocol::{protobuf as pb, ProtocolError},
};
use pb::frame::Kind;

pub fn encode(frame: &Frame) -> Result<Option<Vec<u8>>, ProtocolError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| ProtocolError::InvalidField("unconsumed native LLM continuation"))?;
    frame
        .require_wire_pts()
        .map_err(|_| ProtocolError::InvalidField("pipeline-clock PTS must be rebased"))?;
    let name = |kind: &str| {
        frame
            .metadata
            .get("wire_name")
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .unwrap_or_else(|| format!("{kind}#{}", frame.id))
    };
    let kind = match &frame.data {
        FrameData::Text(text) => Kind::Text(pb::TextFrame {
            id: frame.id,
            name: name("TextFrame"),
            text: text.text.clone(),
        }),
        FrameData::OutputAudio(audio) => Kind::Audio(pb::AudioRawFrame {
            id: frame.id,
            name: name("OutputAudioRawFrame"),
            audio: audio.audio.clone(),
            sample_rate: audio.sample_rate,
            num_channels: u32::from(audio.num_channels),
            pts: frame.pts,
        }),
        FrameData::Transcription(t) => Kind::Transcription(pb::TranscriptionFrame {
            id: frame.id,
            name: name("TranscriptionFrame"),
            text: t.text.clone(),
            user_id: t.user_id.clone(),
            timestamp: t.timestamp.clone(),
        }),
        FrameData::OutputTransportMessage(message)
        | FrameData::OutputTransportMessageUrgent(message) => Kind::Message(pb::MessageFrame {
            data: serde_json::to_string(message)?,
        }),
        FrameData::Interruption => Kind::Interruption(pb::InterruptionFrame {
            id: frame.id,
            name: name("InterruptionFrame"),
        }),
        _ => return Ok(None),
    };
    pb::Frame { frame: Some(kind) }.encode_checked().map(Some)
}
pub fn decode(bytes: &[u8]) -> Result<Frame, ProtocolError> {
    let proto = pb::Frame::parse(bytes)?;
    let (data, id, name, pts) = match proto.frame.ok_or(ProtocolError::MissingField("frame"))? {
        Kind::Text(t) => (Frame::text(t.text).data, Some(t.id), Some(t.name), None),
        Kind::Audio(a) => {
            let channels = u16::try_from(a.num_channels)
                .map_err(|_| ProtocolError::InvalidField("channel count"))?;
            let audio = AudioRawFrame::new(a.audio, a.sample_rate, channels)
                .map_err(|_| ProtocolError::InvalidField("PCM format"))?;
            (
                FrameData::InputAudio(audio),
                Some(a.id),
                Some(a.name),
                a.pts,
            )
        }
        Kind::Transcription(t) => (
            FrameData::Transcription(TranscriptionFrame {
                text: t.text,
                user_id: t.user_id,
                timestamp: t.timestamp,
                language: None,
                result: None,
                finalized: false,
            }),
            Some(t.id),
            Some(t.name),
            None,
        ),
        Kind::Message(m) => (
            FrameData::InputTransportMessage(serde_json::from_str(&m.data)?),
            None,
            None,
            None,
        ),
        Kind::Interruption(i) => (FrameData::Interruption, Some(i.id), Some(i.name), None),
    };
    let mut frame = Frame::new(data);
    if let Some(id) = id.filter(|id| *id != 0) {
        frame.id = id;
    }
    if let Some(name) = name.filter(|name| !name.is_empty()) {
        frame.metadata.insert("wire_name".into(), name.into());
    }
    frame.pts = pts;
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn audio_changes_direction_and_preserves_zero_timestamp() {
        let mut frame = Frame::new(FrameData::OutputAudio(
            AudioRawFrame::new(vec![0, 1, 2, 3], 16000, 1).unwrap(),
        ));
        frame.pts = Some(0);
        let recovered = decode(&encode(&frame).unwrap().unwrap()).unwrap();
        assert_eq!(recovered.id, frame.id);
        assert_eq!(recovered.pts, Some(0));
        match recovered.data {
            FrameData::InputAudio(audio) => assert_eq!(audio.audio, vec![0, 1, 2, 3]),
            _ => panic!("expected input audio"),
        }
    }
    #[test]
    fn rtvi_transport_messages_survive_wire_boundary() {
        let value = serde_json::json!({"label":"rtvi-ai","type":"bot-ready","id":"r1","data":{"version":"2.2.0"}});
        let frame = Frame::new(FrameData::OutputTransportMessage(value.clone()));
        assert_eq!(
            decode(&encode(&frame).unwrap().unwrap()).unwrap().data,
            FrameData::InputTransportMessage(value)
        );
    }
    #[test]
    fn control_frames_are_not_accidentally_sent_as_text() {
        assert_eq!(encode(&Frame::end()).unwrap(), None);
        assert!(decode(&[255, 255]).is_err());
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
        assert!(encode(&frame).is_err());
        frame.rebase_pipeline_pts(75).unwrap();
        assert_eq!(frame.pts, Some(25));
        assert!(encode(&frame).is_ok());
    }
}

#[cfg(test)]
mod native_history_boundary_tests {
    use super::*;
    use pipecat_core::{LlmContinuationProvider, NativeLlmContinuation};
    use std::sync::Arc;
    #[test]
    fn unsupported_frames_cannot_silently_discard_native_history() {
        let mut frame = Frame::new(FrameData::LlmResponseEnd);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::new(1, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
            ))
            .unwrap();
        assert!(encode(&frame).is_err());
        frame.data = FrameData::Text(crate::core::TextFrame::new("changed"));
        assert!(encode(&frame).is_err());
        frame.data = FrameData::LlmResponseEnd;
        frame.take_native_llm_continuation().unwrap();
        assert_eq!(encode(&frame).unwrap(), None);
    }
}
