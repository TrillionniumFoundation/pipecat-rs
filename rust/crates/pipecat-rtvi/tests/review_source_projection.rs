//! Unconsumed source authority cannot be shed by ordinary RTVI projection.
use pipecat_core::*;
use pipecat_rtvi::{Observation, RtviObserver, RtviObserverParams};
fn count(frame: &Frame) -> usize {
    RtviObserver::new(RtviObserverParams::default(), None)
        .unwrap()
        .observe(
            frame,
            Observation::new("source-review", Direction::Downstream),
        )
        .unwrap()
        .len()
}
#[test]
fn review_projection_suppresses_source_carriers_but_preserves_consumed_payloads() {
    let source = EagerSourceSessionOwner::new(4096).unwrap();
    let payload = |eager| {
        let text = TranscriptionFrame {
            text: "private source".into(),
            finalized: true,
            ..Default::default()
        };
        if eager {
            FrameData::EagerTranscription(text)
        } else {
            FrameData::Transcription(text)
        }
    };
    let turn = source.begin_turn().unwrap();
    let prediction = source.predict(&turn, payload(true)).unwrap();
    let receipt = source.offer_final(&turn, payload(false)).unwrap();
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
            FrameData::UserStoppedSpeaking,
            NativeEagerSource::Stop {
                turn: turn.clone(),
                commit: Some(receipt.clone()),
            },
        ),
        (
            FrameData::ProposedUserStoppedSpeaking,
            NativeEagerSource::Stop {
                turn: turn.clone(),
                commit: Some(receipt),
            },
        ),
    ] {
        let ordinary = Frame::new(data.clone());
        let mut guarded = Frame::new(data);
        guarded.set_native_eager_source(carrier).unwrap();
        assert_eq!(
            count(&guarded),
            0,
            "observer shed unconsumed source authority"
        );
        guarded
            .take_native_eager_source(Direction::Downstream)
            .unwrap();
        assert_eq!(
            count(&guarded),
            count(&ordinary),
            "consumed payload lost ordinary observer behavior"
        );
        if matches!(
            ordinary.data,
            FrameData::Transcription(_)
                | FrameData::UserStoppedSpeaking
                | FrameData::UserStartedSpeaking
        ) {
            assert_ne!(count(&ordinary), 0, "projection control is ineffective");
        }
    }
}
