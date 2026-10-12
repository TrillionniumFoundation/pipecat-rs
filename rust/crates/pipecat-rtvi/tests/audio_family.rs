use pipecat_core::*;
use pipecat_rtvi::*;

const MESSAGE_LIMIT: usize = 1024;

#[derive(Clone, Copy, Debug)]
enum AudioFamily {
    Input,
    User,
    Output,
    Tts,
    SpeechOutput,
}
fn audio(family: AudioFamily, bytes: usize) -> Frame {
    let audio = AudioRawFrame::new(vec![0; bytes], 16_000, 1).unwrap();
    Frame::new(match family {
        AudioFamily::Input => FrameData::InputAudio(audio),
        AudioFamily::User => FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "alice".into(),
        }),
        AudioFamily::Output => FrameData::OutputAudio(audio),
        AudioFamily::Tts => FrameData::TtsAudio(audio),
        AudioFamily::SpeechOutput => FrameData::SpeechOutputAudio(audio),
    })
}
fn observer() -> RtviObserver {
    RtviObserver::new(
        RtviObserverParams {
            max_message_bytes: MESSAGE_LIMIT,
            dedup_capacity: 1,
            ..Default::default()
        },
        None,
    )
    .unwrap()
}
fn assert_audio_bypasses_event_budget(family: AudioFamily) {
    // 20 ms of mono 16 kHz PCM16: valid media, but its JSON exceeds 1024 bytes.
    let frame = audio(family, 640);
    let json_bytes = serde_json::to_vec(&frame.data).unwrap().len();
    assert!(json_bytes > MESSAGE_LIMIT);
    let result = observer().observe(&frame, Observation::new("audio", Direction::Downstream));
    eprintln!("{family:?}: PCM bytes=640, JSON bytes={json_bytes}, observe={result:?}");
    assert!(
        result.is_ok(),
        "raw {family:?} must bypass RTVI event budgeting: {result:?}"
    );
    assert!(result.unwrap().is_empty());
}
fn assert_audio_preserves_event_dedup(family: AudioFamily) {
    let observer = observer();
    let speaking = Frame::new(FrameData::UserStartedSpeaking);
    let observation = Observation::new("audio", Direction::Downstream);
    assert_eq!(observer.observe(&speaking, observation).unwrap().len(), 1);
    let small_audio = audio(family, 2);
    assert!(serde_json::to_vec(&small_audio.data).unwrap().len() < MESSAGE_LIMIT);
    assert!(observer
        .observe(&small_audio, observation)
        .unwrap()
        .is_empty());
    let repeated = observer.observe(&speaking, observation).unwrap();
    eprintln!(
        "{family:?}: repeated speaking events after small PCM={}",
        repeated.len()
    );
    assert!(
        repeated.is_empty(),
        "raw {family:?} must not evict event identity"
    );
}

#[test]
fn user_audio_bypasses_rtvi_event_budget() {
    assert_audio_bypasses_event_budget(AudioFamily::User);
}
#[test]
fn speech_output_audio_bypasses_rtvi_event_budget() {
    assert_audio_bypasses_event_budget(AudioFamily::SpeechOutput);
}
#[test]
fn user_audio_does_not_evict_speaking_event_identity() {
    assert_audio_preserves_event_dedup(AudioFamily::User);
}
#[test]
fn speech_output_audio_does_not_evict_speaking_event_identity() {
    assert_audio_preserves_event_dedup(AudioFamily::SpeechOutput);
}
#[test]
fn existing_raw_audio_siblings_keep_budget_and_dedup_exemption() {
    for family in [AudioFamily::Input, AudioFamily::Output, AudioFamily::Tts] {
        assert_audio_bypasses_event_budget(family);
        assert_audio_preserves_event_dedup(family);
    }
}

#[test]
fn raw_audio_exemption_does_not_weaken_real_event_budget() {
    let observer = observer();
    for family in [AudioFamily::User, AudioFamily::SpeechOutput] {
        assert!(observer
            .observe(
                &audio(family, 640),
                Observation::new("audio", Direction::Downstream)
            )
            .unwrap()
            .is_empty());
    }
    let oversized = Frame::new(FrameData::RtviServerMessage(
        serde_json::json!({"text": "x".repeat(2048)}),
    ));
    let error = observer
        .observe(
            &oversized,
            Observation::new("application", Direction::Downstream),
        )
        .unwrap_err();
    assert_eq!(error.message, "RTVI payload too large");
    assert_eq!(error.category, ErrorCategory::Unknown);
    assert!(!error.fatal);
    let valid = Frame::new(FrameData::RtviServerMessage(
        serde_json::json!({"text": "ok"}),
    ));
    let events = observer
        .observe(
            &valid,
            Observation::new("application", Direction::Downstream),
        )
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(
        serde_json::to_value(&events[0]).unwrap()["type"],
        "server-message"
    );
}

#[test]
fn event_first_push_ignored_source_broadcast_and_dedup_gates_remain() {
    let observer = RtviObserver::new(
        RtviObserverParams {
            max_message_bytes: MESSAGE_LIMIT,
            ignored_sources: ["private".into()].into_iter().collect(),
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let speaking = Frame::new(FrameData::UserStartedSpeaking);
    let mut later_push = Observation::new("public", Direction::Downstream);
    later_push.first_push = false;
    assert!(observer.observe(&speaking, later_push).unwrap().is_empty());
    assert!(observer
        .observe(
            &speaking,
            Observation::new("private", Direction::Downstream)
        )
        .unwrap()
        .is_empty());
    assert_eq!(
        observer
            .observe(&speaking, Observation::new("public", Direction::Downstream))
            .unwrap()
            .len(),
        1
    );
    assert!(observer
        .observe(
            &speaking,
            Observation::new("second-tap", Direction::Downstream)
        )
        .unwrap()
        .is_empty());
    let (down, up) = Frame::new(FrameData::UserStoppedSpeaking).broadcast_pair();
    assert!(observer
        .observe(&up, Observation::new("public", Direction::Upstream))
        .unwrap()
        .is_empty());
    assert_eq!(
        observer
            .observe(&down, Observation::new("public", Direction::Downstream))
            .unwrap()
            .len(),
        1
    );
}

async fn audio_tap_bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl std::future::Future<Output = Result<T, E>>,
) -> Result<T, String> {
    tokio::time::timeout(std::time::Duration::from_secs(10), future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}

async fn audio_tap_finish(
    worker: PipelineWorker,
    graceful: bool,
) -> Result<(Vec<DirectedFrame>, WorkerReport), String> {
    use std::time::Duration;
    use tokio::time::timeout;
    let wait = Duration::from_secs(10);
    let sender = worker.sender();
    let terminal = if graceful {
        audio_tap_bounded("End", sender.end()).await
    } else {
        audio_tap_bounded("Cancel", sender.cancel("audio tap cancellation")).await
    };
    if terminal.is_err() {
        let _ = audio_tap_bounded(
            "Cancel failed terminal",
            sender.cancel("audio tap terminal failed"),
        )
        .await;
    }
    let mut collection = tokio::spawn(worker.collect());
    let collected = match timeout(wait, &mut collection).await {
        Ok(result) => result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string()),
        Err(_) => {
            let _ = audio_tap_bounded(
                "Cancel collection",
                sender.cancel("audio tap collection deadline"),
            )
            .await;
            match timeout(wait, &mut collection).await {
                Ok(result) => Err(format!(
                    "collection deadline; cancelled and joined: {result:?}"
                )),
                Err(_) => {
                    collection.abort();
                    let _ = collection.await;
                    Err("collection deadline after cancellation; aborted and joined".into())
                }
            }
        }
    };
    terminal.and(collected)
}

#[tokio::test]
async fn every_raw_audio_tap_variant_forwards_once_in_both_directions_and_terminals() {
    use std::{sync::Arc, time::Duration};
    use tokio::time::timeout;
    for direction in [Direction::Downstream, Direction::Upstream] {
        for graceful in [true, false] {
            let observer = Arc::new(observer());
            let mut expected = Vec::new();
            let tag = LocalFrameTag::new();
            for family in [
                AudioFamily::Input,
                AudioFamily::User,
                AudioFamily::Output,
                AudioFamily::Tts,
                AudioFamily::SpeechOutput,
            ] {
                let mut frame = audio(family, 640);
                frame.name = format!("raw-{family:?}");
                frame.set_pipeline_pts(12345);
                frame.transport_source = Some("source".into());
                frame.transport_destination = Some("destination".into());
                frame
                    .metadata
                    .insert("payload".into(), serde_json::json!({"kept": true}));
                frame.interruptible = false;
                tag.mark(&mut frame).unwrap();
                expected.push(frame);
            }
            let marker = Frame::new(FrameData::Text(TextFrame::new("raw-audio-marker")));
            let mut worker = Pipeline::new(vec![Arc::new(observer.tap("raw-audio-tap"))])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            let mut observed = Vec::new();
            let scenario: Result<(), String> = async {
                for frame in &expected {
                    audio_tap_bounded(
                        "raw audio send",
                        worker.sender().send(frame.clone(), direction),
                    )
                    .await?;
                }
                audio_tap_bounded(
                    "marker send",
                    worker.sender().send(marker.clone(), direction),
                )
                .await?;
                timeout(Duration::from_secs(10), async {
                    loop {
                        let frame = worker.recv().await.ok_or("closed before marker")?;
                        let found = frame.frame.id == marker.id;
                        observed.push(frame);
                        if found {
                            return Ok::<(), String>(());
                        }
                    }
                })
                .await
                .map_err(|_| "marker deadline".to_owned())?
            }
            .await;
            let finished = audio_tap_finish(worker, graceful && scenario.is_ok()).await;
            let (remaining, report) = finished.expect("join tap pipeline before assertions");
            observed.extend(remaining);
            assert!(scenario.is_ok(), "{scenario:?}");
            assert_eq!(
                report.state,
                if graceful {
                    WorkerState::Ended
                } else {
                    WorkerState::Cancelled
                }
            );
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            for expected in &expected {
                let matching: Vec<_> = observed
                    .iter()
                    .filter(|frame| frame.frame.id == expected.id)
                    .collect();
                assert_eq!(matching.len(), 1, "{}", expected.name);
                assert_eq!(matching[0].direction, direction);
                let actual = &matching[0].frame;
                assert_eq!(actual, expected);
                assert_eq!(actual.has_pipeline_pts(), expected.has_pipeline_pts());
                assert_eq!(actual.observation_token(), expected.observation_token());
                assert!(tag.contains(actual));
            }
            assert!(!observed.iter().any(|frame| matches!(
                frame.frame.data,
                FrameData::OutputTransportMessageUrgent(_)
            )));
        }
    }
}
