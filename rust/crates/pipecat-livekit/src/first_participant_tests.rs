//! Production normalized helpers and dispatch, not joined SDK rooms or Python callbacks.
use crate::{
    connection::{participant_connected, participant_disconnected},
    Error, LiveKitConfig, LiveKitEvent, ParticipantInfo,
};
use tokio::sync::mpsc;

fn participant(identity: &str) -> ParticipantInfo {
    ParticipantInfo {
        identity: identity.into(),
        sid: String::new(),
        name: String::new(),
        metadata: String::new(),
    }
}
fn raw_join(rx: &mut mpsc::Receiver<LiveKitEvent>, expected: &str) {
    assert!(
        matches!(rx.try_recv(), Ok(LiveKitEvent::ParticipantConnected(p)) if p.identity == expected)
    );
}
fn first(rx: &mut mpsc::Receiver<LiveKitEvent>, expected: &str) {
    assert!(
        matches!(rx.try_recv(), Ok(LiveKitEvent::FirstParticipantJoined { identity }) if identity == expected)
    );
}
fn empty(rx: &mut mpsc::Receiver<LiveKitEvent>) {
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[test]
fn first_join_raw_then_observation_duplicates_before_rearm_are_raw_only() {
    let (tx, mut rx) = mpsc::channel(2);
    let mut joined = false;
    for (identity, expect_first) in [("z", true), ("z", false), ("a", false)] {
        participant_connected(&tx, &mut joined, participant(identity)).unwrap();
        raw_join(&mut rx, identity);
        if expect_first {
            first(&mut rx, identity);
        }
        assert!(joined);
        empty(&mut rx);
    }
}

#[test]
fn normalized_initial_snapshot_delivered_order_empty_preservation_and_fresh_owner() {
    let (tx, mut rx) = mpsc::channel(2);
    let mut joined = false;
    // This is the normalized sequence used by Connected, not execution of the SDK arm.
    for snapshot in [vec![], vec!["z", "a"], vec![], vec!["z"]] {
        for identity in snapshot {
            let was_joined = joined;
            participant_connected(&tx, &mut joined, participant(identity)).unwrap();
            raw_join(&mut rx, identity);
            if !was_joined {
                first(&mut rx, identity);
            }
            empty(&mut rx);
        }
    }
    assert!(joined);
    // Actual reconnect preservation is source-inspected, not simulated by this fixture.
    let mut fresh_owner = false;
    participant_connected(&tx, &mut fresh_owner, participant("z")).unwrap();
    raw_join(&mut rx, "z");
    first(&mut rx, "z");
    assert!(fresh_owner);
    empty(&mut rx);
}

#[test]
fn successful_departure_samples_after_raw_and_only_empty_remotes_rearm_next_join() {
    let (tx, mut rx) = mpsc::channel(2);
    let mut joined = true;
    for remotes_empty in [false, true] {
        participant_disconnected(&tx, &mut joined, participant("gone"), || {
            assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::ParticipantDisconnected(p)) if p.identity == "gone"));
            remotes_empty
        }).unwrap();
        assert_eq!(joined, !remotes_empty);
        participant_connected(&tx, &mut joined, participant("next")).unwrap();
        raw_join(&mut rx, "next");
        if remotes_empty {
            first(&mut rx, "next");
        }
        assert!(joined);
        empty(&mut rx);
    }
}

#[test]
fn invalid_departure_with_empty_fact_does_not_sample_or_reset_true() {
    let (tx, mut rx) = mpsc::channel(2);
    let mut joined = true;
    let mut invalid = participant("gone");
    invalid.metadata = "x".repeat(65_536);
    let sampled = std::cell::Cell::new(false);
    assert!(matches!(
        participant_disconnected(&tx, &mut joined, invalid, || {
            sampled.set(true);
            true
        }),
        Err(Error::Limit("event metadata bytes"))
    ));
    assert!(joined);
    assert!(!sampled.get());
    empty(&mut rx);
}

#[test]
fn full_or_closed_departure_with_empty_fact_does_not_sample_or_reset_true() {
    for closed in [false, true] {
        let (tx, mut rx) = mpsc::channel(1);
        if closed {
            rx.close();
        } else {
            tx.try_send(LiveKitEvent::Reconnecting).unwrap();
        }
        let mut joined = true;
        let sampled = std::cell::Cell::new(false);
        let result = participant_disconnected(&tx, &mut joined, participant("gone"), || {
            sampled.set(true);
            true
        });
        assert!(if closed {
            matches!(result, Err(Error::Closed))
        } else {
            matches!(result, Err(Error::QueueFull))
        });
        assert!(joined);
        assert!(!sampled.get());
        if !closed {
            assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::Reconnecting)));
            empty(&mut rx);
        }
    }
}

#[test]
fn normalized_media_filter_excluded_remote_eligible_unfiltered_empty_fact_rearms() {
    let config = LiveKitConfig {
        participant_filter: vec!["selected".into()],
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(2);
    let mut joined = false;
    assert!(!config.accepts("excluded-remote"));
    participant_connected(&tx, &mut joined, participant("excluded-remote")).unwrap();
    raw_join(&mut rx, "excluded-remote");
    first(&mut rx, "excluded-remote");
    // No reader count or filter enters the production helper. No local SDK event is fabricated.
    participant_disconnected(&tx, &mut joined, participant("excluded-remote"), || true).unwrap();
    assert!(!joined);
    assert!(matches!(
        rx.try_recv(),
        Ok(LiveKitEvent::ParticipantDisconnected(_))
    ));
    empty(&mut rx);
}

#[test]
fn normalized_empty_and_exact_utf8_identity_boundaries_with_empty_other_fields() {
    for identity in [String::new(), "x".repeat(65_536), "é".repeat(32_768)] {
        let (tx, mut rx) = mpsc::channel(2);
        let mut joined = false;
        participant_connected(&tx, &mut joined, participant(&identity)).unwrap();
        raw_join(&mut rx, &identity);
        first(&mut rx, &identity);
        assert!(joined);
        empty(&mut rx);
    }
}

#[test]
fn invalid_join_identity_or_aggregate_metadata_prevents_both_observations() {
    let mut aggregate = participant(&"x".repeat(65_536));
    aggregate.sid = "s".into(); // Identity alone fits; full ParticipantInfo does not.
    let mut metadata = participant("remote");
    metadata.metadata = "é".repeat(32_768);
    for invalid in [
        participant(&"x".repeat(65_537)),
        participant(&"é".repeat(32_769)),
        aggregate,
        metadata,
    ] {
        for prior in [false, true] {
            let (tx, mut rx) = mpsc::channel(2);
            let mut joined = prior;
            assert!(matches!(
                participant_connected(&tx, &mut joined, invalid.clone()),
                Err(Error::Limit("event metadata bytes"))
            ));
            assert_eq!(joined, prior);
            empty(&mut rx);
        }
    }
}

#[test]
fn first_join_one_slot_partial_admission_leaves_false_and_raw_history_no_retry() {
    let (tx, mut rx) = mpsc::channel(1);
    let mut joined = false;
    assert!(matches!(
        participant_connected(&tx, &mut joined, participant("first")),
        Err(Error::QueueFull)
    ));
    assert!(!joined);
    raw_join(&mut rx, "first");
    empty(&mut rx);
    // Production propagates this terminal failure; this fixture intentionally does not resume.
}

#[test]
fn first_join_raw_full_or_closed_preserves_prior_bool() {
    for prior in [false, true] {
        for closed in [false, true] {
            let (tx, mut rx) = mpsc::channel(1);
            if closed {
                rx.close();
            } else {
                tx.try_send(LiveKitEvent::Reconnecting).unwrap();
            }
            let mut joined = prior;
            let result = participant_connected(&tx, &mut joined, participant("next"));
            assert!(if closed {
                matches!(result, Err(Error::Closed))
            } else {
                matches!(result, Err(Error::QueueFull))
            });
            assert_eq!(joined, prior);
            if !closed {
                assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::Reconnecting)));
                empty(&mut rx);
            }
        }
    }
}

#[test]
fn first_observation_direct_identity_validation_and_no_frame() {
    for identity in [String::new(), "é".repeat(32_768), "x".repeat(65_537)] {
        let fits = identity.len() <= 65_536;
        let event = LiveKitEvent::FirstParticipantJoined { identity };
        let result = crate::protocol::validate_event(&event);
        if fits {
            result.unwrap();
        } else {
            assert!(matches!(result, Err(Error::Limit("event metadata bytes"))));
        }
        assert!(event.into_frame().unwrap().is_none());
    }
}

struct DispatchEvents(std::sync::Mutex<Vec<LiveKitEvent>>);
#[async_trait::async_trait]
impl pipecat_core::FrameProcessor for DispatchEvents {
    fn name(&self) -> &str {
        "first-participant-production-dispatch"
    }
    async fn process(
        &self,
        frame: pipecat_core::Frame,
        direction: pipecat_core::Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> std::result::Result<(), pipecat_core::ProcessorError> {
        let start = frame.data.is_start();
        ctx.push(frame, direction).await?;
        if start {
            let events = std::mem::take(&mut *self.0.lock().unwrap());
            for event in events {
                crate::transport::dispatch_input(event, ctx).await?;
            }
        }
        Ok(())
    }
}

#[tokio::test]
async fn first_observation_dispatch_keeps_ordinary_client_connected_and_following_audio() {
    use pipecat_core::{AudioRawFrame, FrameData, Pipeline};
    let (tx, mut rx) = mpsc::channel(2);
    let mut info = participant("remote");
    info.sid = "PA_remote".into();
    participant_connected(&tx, &mut false, info).unwrap();
    let events = vec![
        rx.try_recv().unwrap(),
        rx.try_recv().unwrap(),
        LiveKitEvent::Audio {
            identity: "remote".into(),
            track_sid: "TR_audio".into(),
            audio: AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap(),
        },
    ];
    let mut worker = Pipeline::new(vec![std::sync::Arc::new(DispatchEvents(
        std::sync::Mutex::new(events),
    ))])
    .start(Default::default())
    .await
    .unwrap();
    let frames = tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let mut frames = Vec::new();
        for _ in 0..3 {
            frames.push(worker.recv().await.unwrap());
        }
        frames
    })
    .await
    .unwrap();
    assert!(frames[0].frame.data.is_start());
    assert!(matches!(frames[1].frame.data, FrameData::ClientConnected));
    assert_eq!(frames[1].frame.transport_source.as_deref(), Some("remote"));
    assert_eq!(
        frames[1].frame.metadata["livekit.participant_sid"],
        "PA_remote"
    );
    let FrameData::UserAudio(audio) = &frames[2].frame.data else {
        panic!("audio must follow without an observation frame")
    };
    assert_eq!(audio.user_id, "remote");
    assert_eq!(audio.audio.audio, vec![0; 4]);
    assert_eq!(frames[2].frame.metadata["livekit.track_sid"], "TR_audio");
    worker.end().await.unwrap();
    let (rest, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(rest.len(), 1);
    assert!(matches!(rest[0].frame.data, FrameData::End(_)));
}
