//! Every manual codec must reject every unconsumed source role, including ignored kinds.
use pipecat_core::*;
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    genesys::{AudioHookConfig, AudioHookSerializer},
    telephony::TelephonyCodec,
};
#[test]
fn review_source_roles_fail_closed_at_all_manual_wire_boundaries() {
    let source = EagerSourceSessionOwner::new(4096).unwrap();
    let payload = |eager| {
        let data = TranscriptionFrame {
            text: "private".into(),
            finalized: true,
            ..Default::default()
        };
        if eager {
            FrameData::EagerTranscription(data)
        } else {
            FrameData::Transcription(data)
        }
    };
    let turn = source.begin_turn().unwrap();
    let prediction = source.predict(&turn, payload(true)).unwrap();
    let receipt = source.offer_final(&turn, payload(false)).unwrap();
    let mut serializers: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("review").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("review", AudioHookConfig::default()).unwrap()),
    ];
    let empty_source = EagerSourceSessionOwner::new(4096).unwrap();
    let empty_turn = empty_source.begin_turn().unwrap();
    empty_source.offer_empty_stop(&empty_turn).unwrap();
    for (data, carrier) in [
        (
            FrameData::ProposedUserStartedSpeaking,
            NativeEagerSource::Start(turn.clone()),
        ),
        (
            FrameData::UserStartedSpeaking,
            NativeEagerSource::Start(turn.clone()),
        ),
        (
            FrameData::ProposedUserStoppedSpeaking,
            NativeEagerSource::Stop {
                turn: empty_turn,
                commit: None,
            },
        ),
        (
            prediction.payload().clone(),
            NativeEagerSource::Prediction(prediction.clone()),
        ),
        (
            FrameData::EagerEndOfTurnCancel,
            NativeEagerSource::Cancel(prediction),
        ),
        (
            receipt.payload().clone(),
            NativeEagerSource::Final(receipt.clone()),
        ),
        (
            FrameData::ProposedUserStoppedSpeaking,
            NativeEagerSource::Stop {
                turn: turn.clone(),
                commit: Some(receipt),
            },
        ),
    ] {
        let mut frame = Frame::new(data);
        frame.set_native_eager_source(carrier).unwrap();
        for serializer in &mut serializers {
            assert!(serializer.serialize(&frame).is_err());
            assert!(serde_json::to_value(&frame).is_err());
        }
        let mut duplicate = frame.duplicate();
        duplicate
            .take_native_eager_source(Direction::Downstream)
            .unwrap();
        assert!(duplicate.require_no_native_llm_continuation().is_ok());
        assert!(frame.require_no_native_llm_continuation().is_err());
    }
}
