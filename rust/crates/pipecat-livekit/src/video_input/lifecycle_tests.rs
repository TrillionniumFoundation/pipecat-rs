//! Local SDK ownership observations, not remote subscription or physical capture evidence.
use super::*;
use crate::connection::{finish_video_input, video_subscribed, video_unsubscribed};
use livekit::{
    prelude::LocalVideoTrack,
    webrtc::{
        video_frame::{I420Buffer, VideoFrame, VideoRotation},
        video_source::{native::NativeVideoSource, RtcVideoSource, VideoResolution},
    },
};
use pipecat_core::{
    Direction, FrameData, FrameProcessor, Pipeline, ProcessorContext, ProcessorError,
    UserImageRequestFrame,
};
use tokio::{sync::oneshot, time::timeout};

const DEADLINE: Duration = Duration::from_secs(3);
fn key(source: VideoSource) -> Key {
    Key {
        identity: "identity-not-participant-sid".into(),
        source,
    }
}
fn config() -> crate::LiveKitConfig {
    crate::LiveKitConfig {
        video_input: Some(VideoInputConfig::default()),
        ..Default::default()
    }
}
fn source() -> (NativeVideoSource, LocalVideoTrack) {
    let source = NativeVideoSource::new(
        VideoResolution {
            width: 8,
            height: 4,
        },
        false,
    );
    let track =
        LocalVideoTrack::create_video_track("lifecycle", RtcVideoSource::Native(source.clone()));
    (source, track)
}
fn pixels() -> VideoFrame<I420Buffer> {
    VideoFrame {
        buffer: I420Buffer::with_strides(8, 4, 8, 4, 4),
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    }
}
fn observation(rx: &mut mpsc::Receiver<LiveKitEvent>, subscribed: bool, key: &Key, sid: &str) {
    let event = rx.try_recv().expect("one lifecycle observation");
    let (identity, track_sid, source) = match (subscribed, event) {
        (
            true,
            LiveKitEvent::VideoSubscribed {
                identity,
                track_sid,
                source,
            },
        )
        | (
            false,
            LiveKitEvent::VideoUnsubscribed {
                identity,
                track_sid,
                source,
            },
        ) => (identity, track_sid, source),
        other => panic!("wrong lifecycle observation: {other:?}"),
    };
    assert_eq!(identity, key.identity);
    assert_eq!(track_sid, sid);
    assert_eq!(source, key.source);
}
fn empty(rx: &mut mpsc::Receiver<LiveKitEvent>) {
    assert!(matches!(
        rx.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
}

#[tokio::test]
async fn selected_sources_filter_disabled_and_initially_muted_observations() {
    let (native, track) = source();
    let (mut readers, _images) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let (tx, mut rx) = mpsc::channel(8);
    let camera = key(VideoSource::Camera);
    video_subscribed(
        &crate::LiveKitConfig::default(),
        &tx,
        &mut readers,
        camera.clone(),
        "disabled".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    let mut filtered = config();
    filtered.participant_filter = vec!["someone-else".into()];
    video_subscribed(
        &filtered,
        &tx,
        &mut readers,
        camera.clone(),
        "filtered".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    assert!(readers.known.is_empty());
    empty(&mut rx);
    filtered.participant_filter = vec![camera.identity.clone()];
    // Source normalization is exactly that used by the RoomEvent arms.
    for (sdk_source, expected) in [
        (TrackSource::Camera, VideoSource::Camera),
        (TrackSource::Unknown, VideoSource::Camera),
        (TrackSource::Microphone, VideoSource::Camera),
        (TrackSource::ScreenshareAudio, VideoSource::Camera),
        (TrackSource::Screenshare, VideoSource::ScreenVideo),
    ] {
        let k = key(VideoSource::from_track(sdk_source));
        assert_eq!(k.source, expected);
        let sid = format!("{sdk_source:?}");
        video_subscribed(
            &filtered,
            &tx,
            &mut readers,
            k.clone(),
            sid.clone(),
            track.rtc_track(),
            true,
        )
        .await
        .unwrap();
        observation(&mut rx, true, &k, &sid);
        assert!(readers.readers.is_empty());
        assert!(readers.known.contains_key(&k));
        readers.unmute(&k, &sid).unwrap();
        assert!(readers.readers.contains_key(&k));
        // Capture is attempted only after the sink is installed.
        assert!(native.capture_frame(&pixels()));
        readers.mute(&k, &sid).await.unwrap();
        empty(&mut rx);
    }
    finish_video_input(Ok(()), &mut readers).await.unwrap();
    empty(&mut rx);
}

#[tokio::test]
async fn duplicate_replacement_stale_unpublish_and_reconnect_counts() {
    let (_, track) = source();
    let (mut readers, _images) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let (tx, mut rx) = mpsc::channel(8);
    let k = key(VideoSource::Camera);
    for sid in ["old", "old", "new"] {
        video_subscribed(
            &config(),
            &tx,
            &mut readers,
            k.clone(),
            sid.into(),
            track.rtc_track(),
            false,
        )
        .await
        .unwrap();
    }
    observation(&mut rx, true, &k, "old");
    observation(&mut rx, true, &k, "new");
    empty(&mut rx); // Replacement does not invent an unsubscribe.
    video_unsubscribed(&tx, &mut readers, k.clone(), "old".into())
        .await
        .unwrap();
    empty(&mut rx);
    readers.mute(&k, "new").await.unwrap();
    readers.unmute(&k, "new").unwrap();
    empty(&mut rx);
    // SDK normal ordering: unsubscribe then unpublish.
    video_unsubscribed(&tx, &mut readers, k.clone(), "new".into())
        .await
        .unwrap();
    observation(&mut rx, false, &k, "new");
    readers.stop(&k, Some("new")).await.unwrap();
    video_unsubscribed(&tx, &mut readers, k.clone(), "new".into())
        .await
        .unwrap();
    empty(&mut rx);
    video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "reversed".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    observation(&mut rx, true, &k, "reversed");
    // Synthetic reversed order invalidates before a stale unsubscribe arrives.
    readers.stop(&k, Some("reversed")).await.unwrap();
    video_unsubscribed(&tx, &mut readers, k.clone(), "reversed".into())
        .await
        .unwrap();
    empty(&mut rx);
    readers
        .start(k.clone(), "rebuild".into(), track.rtc_track(), false)
        .await
        .unwrap();
    readers.reset_readers().await.unwrap();
    readers
        .start(k.clone(), "rebuild".into(), track.rtc_track(), false)
        .await
        .unwrap();
    video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "rebuild".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    empty(&mut rx);
    video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "changed".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    observation(&mut rx, true, &k, "changed");
    readers.remove_participant(&k.identity).await.unwrap();
    finish_video_input(Ok(()), &mut readers).await.unwrap();
    empty(&mut rx);
}

#[tokio::test]
async fn production_pressure_and_receiver_drop_propagate_and_close_owned_sinks() {
    for dropped in [false, true] {
        let (native, track) = source();
        let (mut readers, mut images) =
            VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
        let (tx, rx) = mpsc::channel(1);
        let mut rx = Some(rx);
        let k = key(VideoSource::Camera);
        video_subscribed(
            &config(),
            &tx,
            &mut readers,
            k.clone(),
            "old".into(),
            track.rtc_track(),
            false,
        )
        .await
        .unwrap();
        assert!(native.capture_frame(&pixels()));
        if dropped {
            drop(rx.take());
        }
        let result = video_subscribed(
            &config(),
            &tx,
            &mut readers,
            k.clone(),
            "new".into(),
            track.rtc_track(),
            false,
        )
        .await;
        assert!(if dropped {
            matches!(&result, Err(Error::Closed))
        } else {
            matches!(&result, Err(Error::QueueFull))
        });
        // Completed ownership is not rolled back just to retry notification.
        assert_eq!(readers.known[&k].0, "new");
        let result = finish_video_input(result, &mut readers).await;
        assert!(if dropped {
            matches!(result, Err(Error::Closed))
        } else {
            matches!(result, Err(Error::QueueFull))
        });
        assert!(readers.known.is_empty() && readers.readers.is_empty());
        assert!(readers.gates.lock().unwrap().active.is_empty());
        images.close();
        assert_eq!(
            readers.lane.bytes.available_permits(),
            readers.config.max_retained_bytes
        );
        assert!(!native.capture_frame(&pixels()));
        if let Some(rx) = &mut rx {
            observation(rx, true, &k, "old"); // Queued history is not recalled.
            empty(rx);
        }
    }
}

#[tokio::test]
async fn start_and_join_failures_emit_no_success_and_terminal_cleanup_clears_ownership() {
    let (_, track) = source();
    let (mut readers, _images) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let (tx, mut rx) = mpsc::channel(4);
    let k = key(VideoSource::Camera);
    readers.next_generation = u64::MAX;
    let result = video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "overflow".into(),
        track.rtc_track(),
        false,
    )
    .await;
    assert!(matches!(&result, Err(Error::Limit("video generation"))));
    empty(&mut rx);
    assert!(finish_video_input(result, &mut readers).await.is_err());
    assert!(readers.known.is_empty() && readers.readers.is_empty());

    let (mut readers, _images) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "join".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    observation(&mut rx, true, &k, "join");
    readers.readers[&k].task.abort();
    let result = video_unsubscribed(&tx, &mut readers, k, "join".into()).await;
    assert!(matches!(&result, Err(Error::Sdk(_))));
    empty(&mut rx);
    assert!(finish_video_input(result, &mut readers).await.is_err());
    assert!(readers.known.is_empty() && readers.readers.is_empty());
    empty(&mut rx);
}

struct Admit {
    capture: CaptureHandle,
    admitted: mpsc::UnboundedSender<()>,
}
#[async_trait::async_trait]
impl FrameProcessor for Admit {
    fn name(&self) -> &str {
        "lifecycle-request-admission"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if self.capture.handles_request(&frame, direction) {
            self.capture.request(frame, ctx);
            self.admitted.send(()).unwrap();
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}

#[tokio::test]
async fn unsubscribe_observation_waits_for_join_and_invalidates_private_images_and_requests() {
    let (_, track) = source();
    let cfg = VideoInputConfig {
        requests: Some(ImageRequestConfig::default()),
        ..Default::default()
    };
    let (mut readers, mut images) = VideoReaders::new(cfg, CancellationToken::new());
    let (tx, mut rx) = mpsc::channel(4);
    let k = key(VideoSource::Camera);
    video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "joined".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    observation(&mut rx, true, &k, "joined");
    let old = readers.readers.remove(&k).unwrap();
    let stamp = old.stamp.clone();
    // Wrap the actual reader join: no observation may escape while it is parked.
    let (joined_tx, joined_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let stop = old.stop.clone();
    readers.readers.insert(
        k.clone(),
        Reader {
            stamp: stamp.clone(),
            stop,
            task: tokio::spawn(async move {
                old.task.await.map_err(|e| Error::Sdk(e.to_string()))??;
                joined_tx.send(()).unwrap();
                release_rx.await.unwrap();
                Ok(())
            }),
        },
    );
    let gates = readers.gates.clone();
    let capture = readers.capture_handle();
    let stats = readers.stats();
    let (admitted, mut notices) = mpsc::unbounded_channel();
    let worker = Pipeline::new(vec![Arc::new(Admit {
        capture: capture.clone(),
        admitted,
    })])
    .start(Default::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
                user_id: k.identity.clone(),
                video_source: Some("camera".into()),
                text: None,
                append_to_context: None,
                function_name: None,
                tool_call_id: None,
                result_callback: None,
            })),
            Direction::Upstream,
        )
        .await
        .unwrap();
    timeout(DEADLINE, notices.recv()).await.unwrap().unwrap();
    assert_eq!(stats.snapshot().failed_requests, 0);
    assert!(capture.request_cutoff(&k, &stamp) > 0);
    let frame = pixels();
    let queued = convert(
        VideoFrame {
            buffer: Box::new(frame.buffer),
            rotation: frame.rotation,
            timestamp_us: 0,
            frame_metadata: None,
        },
        k.clone(),
        stamp,
        &readers.lane,
        &readers.config,
    )
    .unwrap()
    .unwrap();
    readers
        .lane
        .tx
        .try_send(queued)
        .unwrap_or_else(|_| panic!("private queue available"));
    let operation = video_unsubscribed(&tx, &mut readers, k.clone(), "joined".into());
    tokio::pin!(operation);
    tokio::select! {
        result = &mut operation => panic!("unsubscribe returned before join barrier: {result:?}"),
        joined = timeout(DEADLINE, joined_rx) => joined.unwrap().unwrap(),
    }
    assert!(gates.lock().unwrap().active.is_empty());
    assert_eq!(stats.snapshot().failed_requests, 1);
    assert_eq!(
        stats.snapshot().last_rejection.as_deref(),
        Some("image request source invalidated")
    );
    empty(&mut rx);
    // The stale private envelope is discarded even before unsubscribe is observable.
    assert!(timeout(Duration::from_millis(20), images.recv())
        .await
        .is_err());
    release_tx.send(()).unwrap();
    timeout(DEADLINE, operation).await.unwrap().unwrap();
    observation(&mut rx, false, &k, "joined");
    empty(&mut rx);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
}

#[test]
fn lifecycle_metadata_boundary_and_no_frame_projection() {
    for subscribed in [false, true] {
        for bytes in [65_536, 65_537] {
            let event = if subscribed {
                LiveKitEvent::VideoSubscribed {
                    identity: "é".repeat(32_767),
                    track_sid: "x".repeat(bytes - 65_534),
                    source: VideoSource::Camera,
                }
            } else {
                LiveKitEvent::VideoUnsubscribed {
                    identity: "é".repeat(32_767),
                    track_sid: "x".repeat(bytes - 65_534),
                    source: VideoSource::ScreenVideo,
                }
            };
            assert_eq!(
                crate::protocol::validate_event(&event).is_ok(),
                bytes == 65_536
            );
            assert!(event.into_frame().unwrap().is_none());
        }
    }
}

#[tokio::test]
async fn unsubscribe_pressure_and_cancelled_or_oversize_start_have_no_false_success() {
    for dropped in [false, true] {
        let (_, track) = source();
        let (mut readers, _images) =
            VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
        let (tx, rx) = mpsc::channel(1);
        let mut rx = Some(rx);
        let k = key(VideoSource::Camera);
        video_subscribed(
            &config(),
            &tx,
            &mut readers,
            k.clone(),
            "TR".into(),
            track.rtc_track(),
            false,
        )
        .await
        .unwrap();
        if dropped {
            drop(rx.take());
        }
        let result = video_unsubscribed(&tx, &mut readers, k, "TR".into()).await;
        assert!(if dropped {
            matches!(&result, Err(Error::Closed))
        } else {
            matches!(&result, Err(Error::QueueFull))
        });
        assert!(readers.known.is_empty() && readers.readers.is_empty());
        assert!(finish_video_input(result, &mut readers).await.is_err());
        if let Some(rx) = &mut rx {
            observation(rx, true, &key(VideoSource::Camera), "TR");
            empty(rx);
        }
    }
    let (_, track) = source();
    let cancel = CancellationToken::new();
    let (mut readers, _images) = VideoReaders::new(VideoInputConfig::default(), cancel.clone());
    let (tx, mut rx) = mpsc::channel(2);
    let k = key(VideoSource::Camera);
    let result = video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "x".repeat(65_537),
        track.rtc_track(),
        false,
    )
    .await;
    assert!(matches!(result, Err(Error::Limit("video track metadata"))));
    assert!(readers.known.is_empty());
    empty(&mut rx);
    video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k.clone(),
        "owned".into(),
        track.rtc_track(),
        false,
    )
    .await
    .unwrap();
    observation(&mut rx, true, &k, "owned");
    cancel.cancel();
    let result = video_subscribed(
        &config(),
        &tx,
        &mut readers,
        k,
        "cancelled".into(),
        track.rtc_track(),
        false,
    )
    .await;
    assert!(matches!(&result, Err(Error::Closed)));
    assert!(matches!(
        finish_video_input(result, &mut readers).await,
        Err(Error::Closed)
    ));
    assert!(readers.known.is_empty() && readers.readers.is_empty());
    empty(&mut rx);
}

/// Exercises the actual active-speaker handler error through the production terminal
/// video cleanup seam, not a hosted room_loop or audio/publisher shutdown.
#[tokio::test]
async fn active_speaker_admission_failure_closes_and_joins_existing_video_reader() {
    for closed in [false, true] {
        let (native, track) = source();
        let (mut readers, mut images) =
            VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
        readers
            .start(
                key(VideoSource::Camera),
                "TR".into(),
                track.rtc_track(),
                false,
            )
            .await
            .unwrap();
        assert!(native.capture_frame(&pixels()));
        let (tx, rx) = mpsc::channel(1);
        let mut rx = Some(rx);
        if closed {
            drop(rx.take());
        }
        let mut last = Some("prior".into());
        let result = crate::connection::active_speakers_changed(
            &config(),
            &tx,
            &mut last,
            vec!["next".into()],
        );
        assert!(if closed {
            matches!(&result, Err(Error::Closed))
        } else {
            matches!(&result, Err(Error::QueueFull))
        });
        assert_eq!(last.as_deref(), Some("prior"));
        let result = timeout(DEADLINE, finish_video_input(result, &mut readers))
            .await
            .unwrap();
        assert!(if closed {
            matches!(result, Err(Error::Closed))
        } else {
            matches!(result, Err(Error::QueueFull))
        });
        assert!(readers.known.is_empty() && readers.readers.is_empty());
        assert!(readers.gates.lock().unwrap().active.is_empty());
        images.close();
        assert_eq!(
            readers.lane.bytes.available_permits(),
            readers.config.max_retained_bytes
        );
        assert!(!native.capture_frame(&pixels()));
        if let Some(rx) = &mut rx {
            assert!(
                matches!(rx.try_recv(), Ok(LiveKitEvent::ActiveSpeakers(ids)) if ids == ["next"])
            );
            empty(rx); // No retry, rollback or promised terminal Error observation.
        }
    }
}
