//! Unconsumed source authority cannot become an unscoped client envelope.
use pipecat_core::*;
use pipecat_rtvi::{Observation, RtviObserver};

#[test]
fn all_source_roles_are_suppressed_until_the_native_consumer_removes_authority() {
    for retired in [false, true] {
        let source = EagerSourceSessionOwner::new(4096).unwrap();
        let turn = source.begin_turn().unwrap();
        let prediction = source
            .predict(
                &turn,
                FrameData::EagerTranscription(TranscriptionFrame {
                    text: "private prediction".into(),
                    ..Default::default()
                }),
            )
            .unwrap();
        let receipt = source
            .offer_final(
                &turn,
                FrameData::Transcription(TranscriptionFrame {
                    text: "final transcript".into(),
                    finalized: true,
                    ..Default::default()
                }),
            )
            .unwrap();
        let empty_source = EagerSourceSessionOwner::new(4096).unwrap();
        let empty_turn = empty_source.begin_turn().unwrap();
        empty_source.offer_empty_stop(&empty_turn).unwrap();
        let roles = [
            (
                FrameData::ProposedUserStoppedSpeaking,
                NativeEagerSource::Stop {
                    turn: empty_turn,
                    commit: None,
                },
            ),
            (
                FrameData::UserStartedSpeaking,
                NativeEagerSource::Start(turn.clone()),
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
                    turn,
                    commit: Some(receipt),
                },
            ),
        ];
        if retired {
            source.revoke();
            empty_source.revoke();
        }
        for (payload, role) in roles {
            let mut frame = Frame::new(payload.clone());
            frame.set_native_eager_source(role).unwrap();
            for direction in [Direction::Downstream, Direction::Upstream] {
                let observer = RtviObserver::new(Default::default(), None).unwrap();
                assert!(observer
                    .observe(&frame, Observation::new("source", direction))
                    .unwrap()
                    .is_empty());
                assert!(observer
                    .observe(&frame.duplicate(), Observation::new("source", direction))
                    .unwrap()
                    .is_empty());
            }
            frame
                .take_native_eager_source(Direction::Downstream)
                .unwrap();
            let ordinary = Frame::new(payload.clone());
            let project = |frame: &Frame| {
                RtviObserver::new(Default::default(), None)
                    .unwrap()
                    .observe(frame, Observation::new("aggregator", Direction::Downstream))
                    .unwrap()
            };
            assert_eq!(project(&frame), project(&ordinary));
            if matches!(
                payload,
                FrameData::Transcription(_)
                    | FrameData::UserStartedSpeaking
                    | FrameData::UserStoppedSpeaking
            ) {
                assert!(
                    !project(&frame).is_empty(),
                    "consumed ordinary client projection must remain available"
                );
            }
        }
    }
}
