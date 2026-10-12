use pipecat_core::*;
use pipecat_rtvi::*;
use std::{future::Future, sync::Arc, time::Duration};
use tokio::time::timeout;

const MESSAGE_LIMIT: usize = 1024;
const WAIT: Duration = Duration::from_secs(10);
type TestResult<T> = Result<T, String>;

#[derive(Clone, Copy, Debug)]
enum ImageFamily {
    Input,
    User,
    Output,
    OutputDetailed,
    Url,
    Assistant,
}
const FAMILIES: [ImageFamily; 6] = [
    ImageFamily::Input,
    ImageFamily::User,
    ImageFamily::Output,
    ImageFamily::OutputDetailed,
    ImageFamily::Url,
    ImageFamily::Assistant,
];
fn image(family: ImageFamily, large: bool) -> Frame {
    let image = ImageRawFrame {
        image: vec![0; if large { 768 } else { 3 }],
        size: if large { (16, 16) } else { (1, 1) },
        format: Some("RGB".into()),
    };
    let output = OutputImageRawFrame {
        image: image.clone(),
        sync_with_audio: false,
    };
    Frame::new(match family {
        ImageFamily::Input => FrameData::InputImage(image),
        ImageFamily::User => FrameData::UserImage(UserImageRawFrame {
            image,
            user_id: "alice".into(),
            text: Some("image context".into()),
            append_to_context: Some(true),
            request: None,
            run_llm: Some(false),
        }),
        ImageFamily::Output => FrameData::OutputImage(image),
        ImageFamily::OutputDetailed => FrameData::OutputImageDetailed(output),
        ImageFamily::Url => FrameData::UrlImage(UrlImageRawFrame {
            image: output,
            url: Some("https://example.invalid/image.png".into()),
        }),
        ImageFamily::Assistant => FrameData::AssistantImage(AssistantImageRawFrame {
            image: output,
            original_data: Some(vec![137, 80, 78, 71]),
            original_mime_type: Some("image/png".into()),
        }),
    })
}
fn observer(capacity: usize) -> RtviObserver {
    RtviObserver::new(
        RtviObserverParams {
            max_message_bytes: MESSAGE_LIMIT,
            dedup_capacity: capacity,
            ..Default::default()
        },
        None,
    )
    .unwrap()
}
fn assert_large_image_bypasses_message_budget(family: ImageFamily) {
    let frame = image(family, true);
    let bytes = serde_json::to_vec(&frame.data).unwrap().len();
    assert!(bytes > MESSAGE_LIMIT);
    let result = observer(1).observe(&frame, Observation::new("images", Direction::Downstream));
    eprintln!("{family:?}: frame-data bytes={bytes}, observe={result:?}");
    assert!(result.is_ok(), "nonprojected {family:?}: {result:?}");
    assert!(result.unwrap().is_empty());
}

#[test]
fn input_image_bypasses_rtvi_message_budget() {
    assert_large_image_bypasses_message_budget(ImageFamily::Input);
}
#[test]
fn user_image_bypasses_rtvi_message_budget() {
    assert_large_image_bypasses_message_budget(ImageFamily::User);
}
#[test]
fn output_image_bypasses_rtvi_message_budget() {
    assert_large_image_bypasses_message_budget(ImageFamily::Output);
}
#[test]
fn detailed_output_image_bypasses_rtvi_message_budget() {
    assert_large_image_bypasses_message_budget(ImageFamily::OutputDetailed);
}
#[test]
fn url_image_bypasses_rtvi_message_budget() {
    assert_large_image_bypasses_message_budget(ImageFamily::Url);
}
#[test]
fn assistant_image_bypasses_rtvi_message_budget() {
    assert_large_image_bypasses_message_budget(ImageFamily::Assistant);
}

#[test]
fn small_images_keep_existing_seen_cache_occupancy() {
    for family in FAMILIES {
        let observer = observer(1);
        let speaking = Frame::new(FrameData::UserStartedSpeaking);
        let observation = Observation::new("images", Direction::Downstream);
        assert_eq!(observer.observe(&speaking, observation).unwrap().len(), 1);
        let small = image(family, false);
        assert!(serde_json::to_vec(&small.data).unwrap().len() < MESSAGE_LIMIT);
        assert!(observer.observe(&small, observation).unwrap().is_empty());
        assert_eq!(
            observer.observe(&speaking, observation).unwrap().len(),
            1,
            "small {family:?} must occupy and evict from the bounded seen cache"
        );
    }
}

async fn bounded<T, E: std::fmt::Display>(
    label: &str,
    future: impl Future<Output = Result<T, E>>,
) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label}: deadline"))?
        .map_err(|error| format!("{label}: {error}"))
}
async fn finish(worker: PipelineWorker) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let sender = worker.sender();
    let terminal = bounded("End", sender.end()).await;
    if terminal.is_err() {
        let _ = bounded("Cancel failed End", sender.cancel("image End failed")).await;
    }
    let mut collection = tokio::spawn(worker.collect());
    let collected = match timeout(WAIT, &mut collection).await {
        Ok(result) => result
            .map_err(|error| error.to_string())?
            .map_err(|error| error.to_string()),
        Err(_) => {
            let _ = bounded(
                "Cancel collection",
                sender.cancel("image collection deadline"),
            )
            .await;
            match timeout(WAIT, &mut collection).await {
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

async fn assert_shared_image_taps_preserve_both_lanes(direction: Direction) {
    let observer = Arc::new(observer(64));
    let tag = LocalFrameTag::new();
    let expected: Vec<_> = FAMILIES
        .into_iter()
        .map(|family| {
            let mut frame = image(family, true);
            frame.name = format!("nonprojected-{family:?}");
            frame.set_pipeline_pts(12345);
            frame.transport_source = Some("image-source".into());
            frame.transport_destination = Some("image-destination".into());
            frame
                .metadata
                .insert("kept".into(), serde_json::json!([true, null]));
            frame.interruptible = false;
            tag.mark(&mut frame).unwrap();
            frame
        })
        .collect();
    // Each lane has its own acknowledgement; neither waits for a lost image.
    let system_marker = Frame::new(FrameData::System);
    let normal_marker = Frame::new(FrameData::Data);
    assert!(system_marker.is_system());
    assert!(!normal_marker.is_system());
    let mut worker = bounded(
        "start taps",
        Pipeline::new(vec![
            Arc::new(observer.tap("first-image-tap")),
            Arc::new(observer.tap("second-image-tap")),
        ])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        }),
    )
    .await
    .unwrap();
    let mut observed = Vec::new();
    let operations: TestResult<()> = async {
        for frame in &expected {
            bounded("send image", worker.sender().send(frame.clone(), direction)).await?;
        }
        for marker in [&system_marker, &normal_marker] {
            bounded(
                "send lane marker",
                worker.sender().send(marker.clone(), direction),
            )
            .await?;
        }
        timeout(WAIT, async {
            let mut system_seen = false;
            let mut normal_seen = false;
            while !(system_seen && normal_seen) {
                let item = worker.recv().await.ok_or("closed before lane markers")?;
                system_seen |= item.frame.id == system_marker.id;
                normal_seen |= item.frame.id == normal_marker.id;
                observed.push(item);
            }
            Ok::<(), String>(())
        })
        .await
        .map_err(|_| "lane markers: deadline".to_owned())?
    }
    .await;
    let finished = finish(worker).await;
    let (remaining, report) = finished.expect("join before image assertions");
    observed.extend(remaining);
    for frame in &expected {
        let count = observed
            .iter()
            .filter(|item| item.frame.id == frame.id)
            .count();
        eprintln!("tap {direction:?} {}: original count={count}", frame.name);
    }
    eprintln!("tap {direction:?}: errors={:?}", report.errors);
    operations.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    for expected in &expected {
        let matching: Vec<_> = observed
            .iter()
            .filter(|item| item.frame.id == expected.id)
            .collect();
        assert_eq!(matching.len(), 1, "{}", expected.name);
        assert_eq!(matching[0].direction, direction);
        assert_eq!(matching[0].frame, *expected);
        assert_eq!(
            matching[0].frame.observation_token(),
            expected.observation_token()
        );
        assert_eq!(
            matching[0].frame.has_pipeline_pts(),
            expected.has_pipeline_pts()
        );
        assert!(tag.contains(&matching[0].frame));
    }
    assert!(!observed
        .iter()
        .any(|item| matches!(item.frame.data, FrameData::OutputTransportMessageUrgent(_))));
}

#[tokio::test]
async fn shared_image_taps_preserve_downstream_frames() {
    assert_shared_image_taps_preserve_both_lanes(Direction::Downstream).await;
}
#[tokio::test]
async fn shared_image_taps_preserve_upstream_frames() {
    assert_shared_image_taps_preserve_both_lanes(Direction::Upstream).await;
}

#[test]
fn large_images_consume_seen_identity_and_reset_releases_it() {
    for family in FAMILIES {
        let observer = observer(1);
        let observation = Observation::new("images", Direction::Downstream);
        let speaking = Frame::new(FrameData::UserStartedSpeaking);
        let image = image(family, true);
        assert_eq!(observer.observe(&speaking, observation).unwrap().len(), 1);
        assert!(observer.observe(&image, observation).unwrap().is_empty());
        assert_eq!(observer.observe(&speaking, observation).unwrap().len(), 1);
        assert!(observer.observe(&speaking, observation).unwrap().is_empty());
        observer.reset();
        assert_eq!(observer.observe(&speaking, observation).unwrap().len(), 1);
    }
}

#[test]
fn image_clone_hits_do_not_refresh_fifo_and_fresh_images_do() {
    for family in FAMILIES {
        for large in [false, true] {
            for fresh in [0, 1, 2] {
                let observer = observer(2);
                let observation = Observation::new("images", Direction::Downstream);
                let original = image(family, large);
                let repeated = match fresh {
                    0 => original.clone(),
                    1 => original.duplicate(),
                    _ => Frame::new(original.data.clone()),
                };
                assert_eq!(original.id == repeated.id, fresh == 0);
                assert_eq!(
                    original.observation_token() == repeated.observation_token(),
                    fresh == 0
                );
                let first = Frame::new(FrameData::UserStartedSpeaking);
                let second = Frame::new(FrameData::UserStoppedSpeaking);
                assert!(observer.observe(&original, observation).unwrap().is_empty());
                assert_eq!(observer.observe(&first, observation).unwrap().len(), 1);
                assert!(observer.observe(&repeated, observation).unwrap().is_empty());
                assert_eq!(observer.observe(&second, observation).unwrap().len(), 1);
                assert_eq!(
                    observer.observe(&first, observation).unwrap().len(),
                    usize::from(fresh != 0),
                    "{family:?}, large={large}, fresh={fresh}"
                );
            }
        }
    }
}

#[test]
fn image_filters_keep_their_existing_native_cache_order() {
    for family in FAMILIES {
        for large in [false, true] {
            for skipped in ["ignored", "later-push", "upstream-broadcast"] {
                let observer = RtviObserver::new(
                    RtviObserverParams {
                        max_message_bytes: MESSAGE_LIMIT,
                        dedup_capacity: 1,
                        ignored_sources: ["ignored".into()].into_iter().collect(),
                        ..Default::default()
                    },
                    None,
                )
                .unwrap();
                let allowed = Observation::new("allowed", Direction::Downstream);
                let speaking = Frame::new(FrameData::UserStartedSpeaking);
                assert_eq!(observer.observe(&speaking, allowed).unwrap().len(), 1);
                let original = image(family, large);
                let (frame, observation) = match skipped {
                    "ignored" => (original, Observation::new("ignored", Direction::Downstream)),
                    "later-push" => (
                        original,
                        Observation {
                            first_push: false,
                            ..allowed
                        },
                    ),
                    _ => {
                        let (_, up) = original.broadcast_pair();
                        (up, Observation::new("allowed", Direction::Upstream))
                    }
                };
                assert!(observer.observe(&frame, observation).unwrap().is_empty());
                assert!(
                    observer.observe(&speaking, allowed).unwrap().is_empty(),
                    "{family:?}, large={large}, skipped={skipped}"
                );
                // The native API admits a previously filtered ID on its first eligible push.
                assert!(observer.observe(&frame, allowed).unwrap().is_empty());
                assert_eq!(observer.observe(&speaking, allowed).unwrap().len(), 1);
            }
            for broadcast in [false, true] {
                let observer = observer(1);
                let speaking = Frame::new(FrameData::UserStartedSpeaking);
                let allowed = Observation::new("allowed", Direction::Downstream);
                assert_eq!(observer.observe(&speaking, allowed).unwrap().len(), 1);
                let original = image(family, large);
                let (frame, direction) = if broadcast {
                    let (down, _) = original.broadcast_pair();
                    (down, Direction::Downstream)
                } else {
                    (original, Direction::Upstream)
                };
                assert!(observer
                    .observe(&frame, Observation::new("allowed", direction))
                    .unwrap()
                    .is_empty());
                assert_eq!(observer.observe(&speaking, allowed).unwrap().len(), 1);
            }
        }
    }
}

#[test]
fn projected_event_budgets_keep_early_and_late_cache_timing() {
    let observation = Observation::new("application", Direction::Downstream);
    let observer = observer(1);
    let sentinel = Frame::new(FrameData::UserStartedSpeaking);
    assert_eq!(observer.observe(&sentinel, observation).unwrap().len(), 1);
    let early = Frame::new(FrameData::RtviServerMessage(
        serde_json::json!({"text": "x".repeat(2048)}),
    ));
    assert!(serde_json::to_vec(&early.data).unwrap().len() > MESSAGE_LIMIT);
    let error = observer.observe(&early, observation).unwrap_err();
    assert_eq!(error.message, "RTVI payload too large");
    assert!(!error.fatal);
    assert!(observer.observe(&sentinel, observation).unwrap().is_empty());
    assert!(observer.observe(&early, observation).is_err());

    let payload = serde_json::json!({"text": "x".repeat(970)});
    let late = Frame::new(FrameData::RtviServerMessage(payload.clone()));
    let envelope = wire::Envelope::server(&wire::ServerEvent::ServerMessage(payload)).unwrap();
    assert_eq!(serde_json::to_vec(&late.data).unwrap().len(), 1022);
    assert_eq!(serde_json::to_vec(&envelope).unwrap().len(), 1032);
    assert_eq!(
        observer.observe(&late, observation).unwrap_err().message,
        "RTVI payload too large"
    );
    assert!(observer.observe(&late, observation).unwrap().is_empty());
    assert_eq!(observer.observe(&sentinel, observation).unwrap().len(), 1);

    let valid = Frame::new(FrameData::RtviServerMessage(
        serde_json::json!({"text": "ok"}),
    ));
    let events = observer.observe(&valid, observation).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].message_type, "server-message");
    assert_eq!(events[0].data, Some(serde_json::json!({"text": "ok"})));
    assert!(observer.observe(&valid, observation).unwrap().is_empty());
}

#[test]
fn disabled_events_nonimage_data_and_sprite_keep_frame_budget() {
    let observation = Observation::new("application", Direction::Downstream);
    let observer = RtviObserver::new(
        RtviObserverParams {
            max_message_bytes: MESSAGE_LIMIT,
            bot_llm_enabled: false,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let FrameData::OutputImageDetailed(image) = image(ImageFamily::OutputDetailed, true).data
    else {
        panic!("wrong fixture variant")
    };
    for data in [
        FrameData::LlmText(TextFrame::new("x".repeat(2048))),
        FrameData::InputTransportMessage(serde_json::json!({"opaque": "x".repeat(2048)})),
        FrameData::Sprite(SpriteFrame {
            images: vec![image],
        }),
    ] {
        let frame = Frame::new(data);
        assert!(serde_json::to_vec(&frame.data).unwrap().len() > MESSAGE_LIMIT);
        assert_eq!(
            observer.observe(&frame, observation).unwrap_err().message,
            "RTVI payload too large"
        );
    }
    assert!(observer
        .observe(
            &Frame::new(FrameData::LlmText(TextFrame::new("disabled"))),
            observation
        )
        .unwrap()
        .is_empty());
}
