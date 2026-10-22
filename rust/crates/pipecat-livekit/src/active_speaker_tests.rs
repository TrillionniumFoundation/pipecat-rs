//! Production normalized admission and dispatch; no joined-room lifecycle claim.
use crate::{connection::active_speakers_changed, Error, LiveKitConfig, LiveKitEvent};
use tokio::sync::mpsc;

fn raw(rx: &mut mpsc::Receiver<LiveKitEvent>, expected: &[&str]) {
    let LiveKitEvent::ActiveSpeakers(actual) = rx.try_recv().unwrap() else {
        panic!("raw observation must be first")
    };
    assert_eq!(actual, expected);
}
fn changed(rx: &mut mpsc::Receiver<LiveKitEvent>, expected: &str) {
    let LiveKitEvent::ActiveSpeakerChanged { identity } = rx.try_recv().unwrap() else {
        panic!("expected effective change")
    };
    assert_eq!(identity, expected);
}
fn empty(rx: &mut mpsc::Receiver<LiveKitEvent>) {
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}
fn update(
    config: &LiveKitConfig,
    tx: &mpsc::Sender<LiveKitEvent>,
    last: &mut Option<String>,
    identities: &[&str],
) -> crate::Result<()> {
    active_speakers_changed(
        config,
        tx,
        last,
        identities.iter().map(|s| (*s).into()).collect(),
    )
}

#[test]
fn active_speaker_last_identity_preserves_raw_order_duplicates_and_repeats() {
    let config = LiveKitConfig::default();
    let (tx, mut rx) = mpsc::channel(2);
    let mut last = None;
    for (identities, change) in [
        (vec!["z", "a"], Some("a")),
        (vec!["a", "a"], None),
        (vec!["a", "a"], None),
        (vec!["a", "z"], Some("z")),
        (vec![""], Some("")),
    ] {
        update(&config, &tx, &mut last, &identities).unwrap();
        raw(&mut rx, &identities);
        if let Some(identity) = change {
            changed(&mut rx, identity);
        }
        empty(&mut rx);
    }
    assert_eq!(last.as_deref(), Some(""));
}

#[test]
fn active_speaker_empty_preserves_state_and_fresh_owner_reports_again() {
    let config = LiveKitConfig::default();
    let (tx, mut rx) = mpsc::channel(2);
    let mut last = None;
    for (identities, change) in [
        (vec![], false),
        (vec!["A"], true),
        (vec![], false),
        (vec!["A"], false),
    ] {
        update(&config, &tx, &mut last, &identities).unwrap();
        raw(&mut rx, &identities);
        if change {
            changed(&mut rx, "A");
        }
        empty(&mut rx);
    }
    assert_eq!(last.as_deref(), Some("A"));
    let mut fresh_owner = None;
    update(&config, &tx, &mut fresh_owner, &["A"]).unwrap();
    raw(&mut rx, &["A"]);
    changed(&mut rx, "A");
    empty(&mut rx);
}

#[test]
fn active_speaker_filter_does_not_exclude_local_or_unselected_identities() {
    let config = LiveKitConfig {
        participant_filter: vec!["selected".into()],
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(2);
    let mut last = None;
    for identity in ["local-identity-not-sid", "filter-excluded-identity-not-sid"] {
        assert!(!config.accepts(identity));
        update(&config, &tx, &mut last, &[identity]).unwrap();
        raw(&mut rx, &[identity]);
        changed(&mut rx, identity);
        empty(&mut rx);
    }
}

#[test]
fn active_speaker_invalid_count_and_aggregate_leave_queue_and_prior_state_unchanged() {
    let config = LiveKitConfig {
        max_audio_tracks: 2,
        ..Default::default()
    };
    let (tx, mut rx) = mpsc::channel(2);
    let mut last = Some("prior".into());
    for (identities, expected) in [
        (
            vec!["a".into(), "b".into(), "c".into()],
            "active speakers per event",
        ),
        (vec!["é".repeat(32_768), "x".into()], "event metadata bytes"),
        (vec!["x".repeat(65_537)], "event metadata bytes"),
    ] {
        assert!(
            matches!(active_speakers_changed(&config, &tx, &mut last, identities), Err(Error::Limit(reason)) if reason == expected)
        );
        assert_eq!(last.as_deref(), Some("prior"));
        empty(&mut rx);
    }
}

#[test]
fn active_speaker_exact_utf8_boundaries_and_compact_retained_copy() {
    let config = LiveKitConfig::default();
    let (tx, mut rx) = mpsc::channel(2);
    let mut last = None;
    for identities in [
        vec!["é".repeat(32_768)],
        vec!["é".repeat(32_767), "xy".into()],
    ] {
        let expected: Vec<&str> = identities.iter().map(String::as_str).collect();
        active_speakers_changed(&config, &tx, &mut last, identities.clone()).unwrap();
        raw(&mut rx, &expected);
        changed(&mut rx, expected.last().unwrap());
        empty(&mut rx);
    }
    let mut spare = String::with_capacity(131_072);
    spare.push_str("compact");
    let input_capacity = spare.capacity();
    active_speakers_changed(&config, &tx, &mut last, vec![spare]).unwrap();
    raw(&mut rx, &["compact"]);
    changed(&mut rx, "compact");
    assert_eq!(last.as_deref(), Some("compact"));
    assert!(last.as_ref().unwrap().capacity() < input_capacity);
    assert!(last.as_ref().unwrap().capacity() <= 65_536);
}

#[test]
fn active_speaker_one_slot_empty_unchanged_and_partial_changed_admission() {
    let config = LiveKitConfig::default();
    let (tx, mut rx) = mpsc::channel(1);
    let mut last = Some("A".into());
    for identities in [vec![], vec!["A"]] {
        update(&config, &tx, &mut last, &identities).unwrap();
        raw(&mut rx, &identities);
        empty(&mut rx);
    }
    assert!(matches!(
        update(&config, &tx, &mut last, &["B"]),
        Err(Error::QueueFull)
    ));
    assert_eq!(last.as_deref(), Some("A"));
    raw(&mut rx, &["B"]); // Non-atomic: history remains, no change or retry.
    empty(&mut rx);
}

#[test]
fn active_speaker_raw_full_and_closed_never_emit_change_or_commit() {
    let config = LiveKitConfig::default();
    let (tx, mut rx) = mpsc::channel(2);
    tx.try_send(LiveKitEvent::Reconnecting).unwrap();
    tx.try_send(LiveKitEvent::Reconnected).unwrap();
    let mut last = Some("A".into());
    assert!(matches!(
        update(&config, &tx, &mut last, &["B"]),
        Err(Error::QueueFull)
    ));
    assert_eq!(last.as_deref(), Some("A"));
    assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::Reconnecting)));
    assert!(matches!(rx.try_recv(), Ok(LiveKitEvent::Reconnected)));
    empty(&mut rx);
    rx.close();
    assert!(matches!(
        update(&config, &tx, &mut last, &["B"]),
        Err(Error::Closed)
    ));
    assert_eq!(last.as_deref(), Some("A"));
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Disconnected)
    ));
}

#[test]
fn active_speaker_direct_variant_metadata_limit_and_no_frame() {
    for bytes in [65_536, 65_537] {
        let event = LiveKitEvent::ActiveSpeakerChanged {
            identity: "x".repeat(bytes),
        };
        let result = crate::protocol::validate_event(&event);
        if bytes == 65_536 {
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
        "active-speaker-production-dispatch"
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
async fn active_speaker_production_dispatch_ignores_observations_and_keeps_media() {
    use pipecat_core::{AudioRawFrame, FrameData, ImageRawFrame, Pipeline};
    let (tx, mut rx) = mpsc::channel(2);
    active_speakers_changed(
        &LiveKitConfig::default(),
        &tx,
        &mut None,
        vec!["speaker".into()],
    )
    .unwrap();
    let events = vec![
        rx.try_recv().unwrap(),
        rx.try_recv().unwrap(),
        LiveKitEvent::Audio {
            identity: "audio-id".into(),
            track_sid: "audio-sid".into(),
            audio: AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap(),
        },
        LiveKitEvent::Data {
            payload: br#"{"message":"unchanged"}"#.to_vec(),
            identity: Some("data-id".into()),
            topic: Some("topic".into()),
            reliable: true,
        },
        LiveKitEvent::Video {
            identity: "video-id".into(),
            track_sid: "video-sid".into(),
            source: crate::VideoSource::ScreenVideo,
            image: ImageRawFrame {
                image: vec![1, 2, 3],
                size: (1, 1),
                format: Some("RGB".into()),
            },
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
        for _ in 0..4 {
            frames.push(worker.recv().await.unwrap());
        }
        frames
    })
    .await
    .unwrap();
    assert!(frames[0].frame.data.is_start());
    let FrameData::UserAudio(audio) = &frames[1].frame.data else {
        panic!("audio unchanged")
    };
    assert_eq!(audio.user_id, "audio-id");
    assert_eq!(audio.audio.audio, vec![0; 4]);
    assert_eq!(frames[1].frame.metadata["livekit.track_sid"], "audio-sid");
    let FrameData::InputTransportMessage(data) = &frames[2].frame.data else {
        panic!("data unchanged")
    };
    assert_eq!(data["message"], "unchanged");
    assert_eq!(frames[2].frame.transport_source.as_deref(), Some("data-id"));
    let FrameData::UserImage(image) = &frames[3].frame.data else {
        panic!("video unchanged")
    };
    assert_eq!(image.user_id, "video-id");
    assert_eq!(image.image.image, vec![1, 2, 3]);
    assert_eq!(
        frames[3].frame.transport_source.as_deref(),
        Some("screenVideo")
    );
    assert_eq!(frames[3].frame.metadata["livekit.track_sid"], "video-sid");
    worker.end().await.unwrap();
    let (rest, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(rest.len(), 1);
    assert!(matches!(rest[0].frame.data, FrameData::End(_)));
}
