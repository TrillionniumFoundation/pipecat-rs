use super::*;
use livekit::{
    prelude::LocalVideoTrack,
    webrtc::{
        video_frame::{I420Buffer, NV12Buffer, VideoFrame, VideoRotation},
        video_source::{native::NativeVideoSource, RtcVideoSource, VideoResolution},
    },
};
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, Pipeline, PipelineConfig, ProcessorContext,
    ProcessorError,
};
use std::time::Duration;
use tokio::time::timeout;

fn key(source: VideoSource) -> Key {
    Key {
        identity: "alice".into(),
        source,
    }
}
fn stamp(sid: &str, generation: u64) -> Stamp {
    Stamp {
        sid: sid.into(),
        generation,
    }
}
fn fixture(width: u32, height: u32, padded: bool, yuv: [u8; 3]) -> VideoFrame<I420Buffer> {
    let pad = if padded { 7 } else { 0 };
    let mut buffer = I420Buffer::with_strides(
        width,
        height,
        width + pad,
        width.div_ceil(2) + pad,
        width.div_ceil(2) + pad,
    );
    let (y, u, v) = buffer.data_mut();
    y.fill(yuv[0]);
    u.fill(yuv[1]);
    v.fill(yuv[2]);
    VideoFrame {
        buffer,
        rotation: VideoRotation::VideoRotation90,
        timestamp_us: 0,
        frame_metadata: None,
    }
}
fn boxed(frame: VideoFrame<I420Buffer>) -> BoxVideoFrame {
    VideoFrame {
        buffer: Box::new(frame.buffer),
        rotation: frame.rotation,
        timestamp_us: frame.timestamp_us,
        frame_metadata: None,
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
        LocalVideoTrack::create_video_track("input-test", RtcVideoSource::Native(source.clone()));
    (source, track)
}
fn queued(readers: &VideoReaders, key: Key, stamp: Stamp, width: u32, height: u32) -> QueuedVideo {
    convert(
        boxed(fixture(width, height, true, [82, 90, 240])),
        key,
        stamp,
        &readers.lane,
        &readers.config,
    )
    .unwrap()
    .unwrap()
}
async fn next_red(receiver: &mut VideoReceiver) -> LiveKitEvent {
    timeout(Duration::from_secs(2), async {
        loop {
            let event = receiver.recv().await.expect("video receiver ended");
            if matches!(&event, LiveKitEvent::Video { image, .. } if image.image[0] > 200 && image.image[2] < 30) { return event; }
        }
    }).await.expect("red capture deadline")
}

#[test]
fn default_disabled_limits_and_all_source_mapping() {
    assert!(crate::LiveKitConfig::default().video_input.is_none());
    let c = VideoInputConfig::default();
    assert!(c.validate().is_ok());
    for (width, height) in [(0, 1), (1, 0), (u32::MAX, 1), (1, u32::MAX), (4096, 4096)] {
        assert!(c.layout(width, height).is_err());
    }
    for capacity in [0, 5, usize::MAX] {
        let c = VideoInputConfig {
            native_queue_capacity: capacity,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
    for tracks in [0, 17, usize::MAX] {
        let c = VideoInputConfig {
            max_tracks: tracks,
            ..Default::default()
        };
        assert!(c.validate().is_err());
    }
    for source in [
        TrackSource::Camera,
        TrackSource::Unknown,
        TrackSource::Microphone,
        TrackSource::ScreenshareAudio,
    ] {
        assert_eq!(VideoSource::from_track(source), VideoSource::Camera);
    }
    assert_eq!(
        VideoSource::from_track(TrackSource::Screenshare),
        VideoSource::ScreenVideo
    );
}

#[test]
fn concrete_i420_asymmetric_rgb_odd_padded_changing_sizes_and_capacity() {
    let (readers, _) = VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    // Limited-range BT.601 reference primaries. This catches ABGR/ARGB register-order mistakes.
    for (yuv, rgb) in [
        ([82, 90, 240], [255, 1, 0]),
        ([41, 240, 110], [0, 0, 255]),
        ([145, 54, 34], [0, 255, 1]),
    ] {
        for (width, height) in [(3, 3), (8, 4), (1, 5)] {
            let native = boxed(fixture(width, height, true, yuv));
            let event = convert(
                native,
                key(VideoSource::Camera),
                stamp("TR_red", 1),
                &readers.lane,
                &readers.config,
            )
            .unwrap()
            .unwrap();
            let LiveKitEvent::Video { image, .. } = &event.event else {
                panic!("video expected");
            };
            assert_eq!(image.size, (width, height));
            assert_eq!(image.image.len(), (width * height * 3) as usize);
            assert_eq!(image.image.capacity(), (width * height * 4) as usize);
            let (pixels, remainder) = image.image.as_chunks::<3>();
            assert!(remainder.is_empty(), "RGB must contain whole triplets");
            for pixel in pixels {
                for (actual, expected) in pixel.iter().zip(rgb) {
                    assert!(actual.abs_diff(expected) <= 3, "{pixel:?} versus {rgb:?}");
                }
            }
            assert_eq!(
                readers.lane.bytes.available_permits(),
                readers.config.max_retained_bytes - (width * height * 4) as usize
            );
        }
    }
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
}

#[tokio::test]
async fn byte_and_pending_permits_survive_queue_until_final_admission_and_stale_close_release() {
    let c = VideoInputConfig {
        max_frame_bytes: 128,
        max_retained_bytes: 128,
        queue_capacity: 2,
        ..Default::default()
    };
    let (readers, mut receiver) = VideoReaders::new(c, CancellationToken::new());
    let k = key(VideoSource::Camera);
    let s = stamp("TR_1", 1);
    readers
        .gates
        .lock()
        .unwrap()
        .active
        .insert(k.clone(), s.clone());
    let envelope = queued(&readers, k.clone(), s.clone(), 8, 4);
    assert_eq!(readers.lane.bytes.available_permits(), 0);
    readers
        .lane
        .tx
        .try_send(envelope)
        .unwrap_or_else(|_| panic!("queue available"));
    assert_eq!(readers.lane.bytes.available_permits(), 0);
    assert!(convert(
        boxed(fixture(1, 1, false, [82, 90, 240])),
        k.clone(),
        s.clone(),
        &readers.lane,
        &readers.config
    )
    .unwrap()
    .is_none());
    let public = receiver.recv().await.unwrap();
    assert_eq!(readers.lane.bytes.available_permits(), 128);
    let clone = public.clone(); // Public caller retention is intentionally outside adapter accounting.
    assert!(matches!(clone, LiveKitEvent::Video { .. }));
    let old = queued(&readers, k.clone(), s.clone(), 3, 3);
    readers
        .lane
        .tx
        .try_send(old)
        .unwrap_or_else(|_| panic!("queue available"));
    let new = stamp("TR_2", 2);
    readers
        .gates
        .lock()
        .unwrap()
        .active
        .insert(k.clone(), new.clone());
    let current = queued(&readers, k, new, 2, 2);
    readers
        .lane
        .tx
        .try_send(current)
        .unwrap_or_else(|_| panic!("queue available"));
    let admitted = receiver.recv().await.unwrap();
    assert!(matches!(admitted, LiveKitEvent::Video { track_sid, .. } if track_sid == "TR_2"));
    assert_eq!(readers.lane.bytes.available_permits(), 128);
    let envelope = queued(&readers, key(VideoSource::Camera), s, 4, 4);
    readers
        .lane
        .tx
        .try_send(envelope)
        .unwrap_or_else(|_| panic!("queue available"));
    receiver.close();
    assert_eq!(readers.lane.bytes.available_permits(), 128);
    assert_eq!(readers.lane.pending.available_permits(), 2);
}

#[test]
fn limits_and_unsupported_buffer_reject_without_converted_allocation() {
    let c = VideoInputConfig {
        max_pixels: 1,
        ..Default::default()
    };
    let (readers, _) = VideoReaders::new(c, CancellationToken::new());
    assert!(convert(
        boxed(fixture(2, 2, false, [82, 90, 240])),
        key(VideoSource::Camera),
        stamp("TR", 1),
        &readers.lane,
        &readers.config
    )
    .is_err());
    let native: BoxVideoFrame = VideoFrame {
        buffer: Box::new(NV12Buffer::new(1, 1)),
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    };
    assert!(matches!(
        convert(
            native,
            key(VideoSource::Camera),
            stamp("TR", 1),
            &readers.lane,
            &readers.config
        ),
        Err(Error::Video("incoming buffer must be I420"))
    ));
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
    assert_eq!(
        readers.lane.pending.available_permits(),
        readers.config.queue_capacity
    );
}

struct DispatchOnce(Mutex<Option<LiveKitEvent>>);
#[async_trait::async_trait]
impl FrameProcessor for DispatchOnce {
    fn name(&self) -> &str {
        "livekit-production-input-dispatch-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        let start = frame.data.is_start();
        ctx.push(frame, direction).await?;
        if start {
            let event = self.0.lock().unwrap().take();
            if let Some(event) = event {
                crate::transport::dispatch_input(event, ctx).await?;
            }
        }
        Ok(())
    }
}
/// Actual native source/track -> production reader/converter/admission -> production
/// input dispatch -> downstream pipeline UserImage. No SFU/remote subscription claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_reader_to_production_dispatch_downstream_user_image() {
    let (source, track) = source();
    let (mut readers, mut receiver) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    readers
        .start(
            key(VideoSource::ScreenVideo),
            "TR_screen".into(),
            track.rtc_track(),
            false,
        )
        .await
        .unwrap();
    for (width, height) in [(4, 4), (8, 4)] {
        let frame = fixture(width, height, true, [82, 90, 240]);
        assert!(source.capture_frame(&frame));
        drop(frame); // The production converter must not borrow source storage.
        let event = next_red(&mut receiver).await;
        let mut worker = Pipeline::new(vec![Arc::new(DispatchOnce(Mutex::new(Some(event))))])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let image = timeout(Duration::from_secs(2), async {
            loop {
                let directed = worker.recv().await.unwrap();
                if matches!(directed.frame.data, FrameData::UserImage(_)) {
                    break directed;
                }
            }
        })
        .await
        .unwrap();
        assert_eq!(image.direction, Direction::Downstream);
        assert_eq!(image.frame.transport_source.as_deref(), Some("screenVideo"));
        assert_eq!(image.frame.metadata["livekit.track_sid"], "TR_screen");
        assert_eq!(image.frame.pts, None);
        let FrameData::UserImage(user) = image.frame.data else {
            unreachable!()
        };
        assert_eq!(user.user_id, "alice");
        assert_eq!(user.image.size, (width, height));
        assert_eq!(user.image.format.as_deref(), Some("RGB"));
        assert!(
            user.text.is_none()
                && user.request.is_none()
                && user.run_llm.is_none()
                && user.append_to_context.is_none()
        );
        let (pixels, remainder) = user.image.image.as_chunks::<3>();
        assert!(remainder.is_empty(), "RGB must contain whole triplets");
        assert!(pixels.iter().all(|p| p[0] >= 250 && p[1] < 5 && p[2] < 5));
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
    }
    timeout(Duration::from_secs(2), readers.close())
        .await
        .unwrap()
        .unwrap();
    receiver.close();
    assert!(readers.readers.is_empty());
}

#[tokio::test]
async fn ownership_camera_screen_replacement_pressure_delayed_completion_and_unsubscribe() {
    let (_, track) = source();
    let c = VideoInputConfig {
        max_tracks: 2,
        ..Default::default()
    };
    let (mut readers, mut receiver) = VideoReaders::new(c, CancellationToken::new());
    let camera = key(VideoSource::Camera);
    let screen = key(VideoSource::ScreenVideo);
    readers
        .start(camera.clone(), "old".into(), track.rtc_track(), false)
        .await
        .unwrap();
    let old = readers.readers[&camera].stamp.clone();
    readers
        .start(screen.clone(), "screen".into(), track.rtc_track(), false)
        .await
        .unwrap();
    readers
        .start(camera.clone(), "new".into(), track.rtc_track(), false)
        .await
        .unwrap();
    assert_eq!(readers.readers.len(), 2);
    readers.finish_joined(camera.clone(), old, Ok(())).unwrap();
    readers.stop(&camera, Some("old")).await.unwrap();
    readers.mute(&camera, "old").await.unwrap();
    readers.unmute(&camera, "old").unwrap();
    assert_eq!(readers.readers[&camera].stamp.sid, "new");
    let other = Key {
        identity: "bob".into(),
        source: VideoSource::Camera,
    };
    assert!(matches!(
        readers
            .start(other, "third".into(), track.rtc_track(), false)
            .await,
        Err(Error::Limit(_))
    ));
    readers.remove_participant("bob").await.unwrap();
    assert_eq!(readers.readers.len(), 2);
    readers.remove_participant("alice").await.unwrap();
    assert!(readers.readers.is_empty());
    readers.close().await.unwrap();
    readers.close().await.unwrap();
    receiver.close();
}

#[tokio::test]
async fn muted_native_and_private_queues_are_not_retagged_on_unmute() {
    let (source, track) = source();
    let (mut readers, mut receiver) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let k = key(VideoSource::Camera);
    readers
        .start(k.clone(), "TR".into(), track.rtc_track(), false)
        .await
        .unwrap();
    let old = readers.readers[&k].stamp.clone();
    readers
        .lane
        .tx
        .try_send(queued(&readers, k.clone(), old.clone(), 3, 3))
        .unwrap_or_else(|_| panic!("queue available"));
    assert!(source.capture_frame(&fixture(8, 4, false, [41, 240, 110]))); // Still in the old sink.
    readers.mute(&k, "TR").await.unwrap();
    assert!(readers.readers.is_empty());
    assert!(readers.gates.lock().unwrap().active.is_empty());
    // Native AdaptFrame rejects captures when there are no interested sinks.
    // This is expected after mute closed and joined the only reader.
    assert!(!source.capture_frame(&fixture(8, 4, false, [145, 54, 34])));
    readers.unmute(&k, "TR").unwrap();
    assert_ne!(readers.readers[&k].stamp, old);
    assert!(source.capture_frame(&fixture(8, 4, false, [82, 90, 240])));
    // No generic color filter: it would hide leaked muted blue/green frames.
    // This current-thread fixture captured before yielding, so the source's
    // captured_frames guard suppresses its initial black keepalive.
    let event = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let LiveKitEvent::Video { image, .. } = event else {
        panic!("video expected")
    };
    assert_eq!(
        image.size,
        (8, 4),
        "old private 3x3 envelope must not be admitted"
    );
    let (pixels, remainder) = image.image.as_chunks::<3>();
    assert!(remainder.is_empty(), "RGB must contain whole triplets");
    assert!(
        pixels.iter().all(|p| p[0] >= 250 && p[1] < 5 && p[2] < 5),
        "stale muted blue/green must never be admitted"
    );
    readers.close().await.unwrap();
    receiver.close();
}

#[tokio::test]
async fn pending_reader_cancellation_latest_native_overflow_and_saturation_are_bounded() {
    let (source, track) = source();
    // Current-thread runtime: captures precede polling/keepalive so latest wins deterministically.
    let mut stream = NativeVideoStream::with_options(
        track.rtc_track(),
        NativeVideoStreamOptions {
            queue_size_frames: Some(1),
        },
    );
    for value in 20..30 {
        assert!(source.capture_frame(&fixture(8, 4, false, [value, 90, 240])));
    }
    let last = timeout(Duration::from_secs(1), stream.next())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(last.buffer.as_i420().unwrap().data().0, [29; 32]);
    stream.close();
    stream.close();
    assert!(stream.next().await.is_none());
    drop(stream);
    let c = VideoInputConfig {
        queue_capacity: 1,
        ..Default::default()
    };
    let (mut readers, mut receiver) = VideoReaders::new(c, CancellationToken::new());
    let k = key(VideoSource::Camera);
    readers
        .start(k.clone(), "TR".into(), track.rtc_track(), false)
        .await
        .unwrap();
    assert!(source.capture_frame(&fixture(8, 4, false, [82, 90, 240])));
    timeout(Duration::from_secs(2), async {
        while readers.lane.pending.available_permits() != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(source.capture_frame(&fixture(8, 4, false, [41, 240, 110])));
    timeout(Duration::from_secs(2), async {
        while readers.stats().snapshot().dropped_frames == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    next_red(&mut receiver).await;
    // Reader now blocks on the real native stream, not a synthetic cancellation future.
    timeout(Duration::from_secs(2), readers.close())
        .await
        .unwrap()
        .unwrap();
    receiver.close();
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
    // Joined close removed the last sink: native capture must report no forwarding.
    assert!(!source.capture_frame(&fixture(8, 4, false, [82, 90, 240])));
    assert!(receiver.recv().await.is_none());
}

#[tokio::test]
async fn initially_muted_slot_is_bounded_and_stale_resume_cannot_replace_new_sid() {
    let (_, track) = source();
    let (mut readers, mut receiver) = VideoReaders::new(
        VideoInputConfig {
            max_tracks: 1,
            ..Default::default()
        },
        CancellationToken::new(),
    );
    let k = key(VideoSource::Camera);
    readers
        .start(k.clone(), "old".into(), track.rtc_track(), true)
        .await
        .unwrap();
    assert!(readers.readers.is_empty());
    assert_eq!(readers.known.len(), 1);
    assert!(readers
        .start(
            key(VideoSource::ScreenVideo),
            "screen".into(),
            track.rtc_track(),
            false
        )
        .await
        .is_err());
    readers
        .start(k.clone(), "new".into(), track.rtc_track(), true)
        .await
        .unwrap();
    readers.unmute(&k, "old").unwrap();
    assert!(readers.readers.is_empty());
    readers.unmute(&k, "new").unwrap();
    assert_eq!(readers.readers.len(), 1);
    readers.close().await.unwrap();
    receiver.close();
}

#[tokio::test]
async fn production_rejection_is_observable_nonfatal_and_next_supported_frame_flows() {
    let (readers, mut receiver) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let k = key(VideoSource::Camera);
    let s = stamp("TR", 1);
    readers
        .gates
        .lock()
        .unwrap()
        .active
        .insert(k.clone(), s.clone());
    let native: BoxVideoFrame = VideoFrame {
        buffer: Box::new(NV12Buffer::new(1, 1)),
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    };
    deliver_frame(native, &k, &s, &readers.lane, &readers.config).unwrap();
    let stats = readers.stats().snapshot();
    assert_eq!(stats.rejected_frames, 1);
    assert!(stats.last_rejection.unwrap().contains("I420"));
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
    deliver_frame(
        boxed(fixture(3, 3, true, [82, 90, 240])),
        &k,
        &s,
        &readers.lane,
        &readers.config,
    )
    .unwrap();
    next_red(&mut receiver).await;
}

#[tokio::test]
async fn cancelled_admission_and_receiver_drop_release_all_private_permits() {
    let cancel = CancellationToken::new();
    let (readers, mut receiver) = VideoReaders::new(VideoInputConfig::default(), cancel.clone());
    let k = key(VideoSource::Camera);
    let s = stamp("TR", 1);
    readers
        .gates
        .lock()
        .unwrap()
        .active
        .insert(k.clone(), s.clone());
    readers
        .lane
        .tx
        .try_send(queued(&readers, k, s, 3, 3))
        .unwrap_or_else(|_| panic!("queue available"));
    cancel.cancel();
    // Poll once: it drains the stale envelope but cannot emit it, then waits.
    assert!(timeout(Duration::from_millis(20), receiver.recv())
        .await
        .is_err());
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
    readers
        .lane
        .tx
        .try_send(queued(
            &readers,
            key(VideoSource::Camera),
            stamp("TR", 1),
            3,
            3,
        ))
        .unwrap_or_else(|_| panic!("queue available"));
    drop(receiver);
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
}

#[tokio::test]
async fn failed_reader_is_joined_removed_and_other_sources_can_be_closed() {
    let (_, track) = source();
    let (mut readers, mut receiver) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let camera = key(VideoSource::Camera);
    let screen = key(VideoSource::ScreenVideo);
    readers
        .start(camera.clone(), "camera".into(), track.rtc_track(), false)
        .await
        .unwrap();
    readers
        .start(screen.clone(), "screen".into(), track.rtc_track(), false)
        .await
        .unwrap();
    readers.readers[&camera].task.abort();
    assert!(timeout(Duration::from_secs(2), readers.next_finished())
        .await
        .unwrap()
        .is_err());
    assert!(!readers.readers.contains_key(&camera));
    assert!(!readers.gates.lock().unwrap().active.contains_key(&camera));
    assert!(readers.readers.contains_key(&screen));
    readers.close().await.unwrap();
    receiver.close();
    assert!(readers.readers.is_empty());
}

struct DispatchBurst {
    events: Mutex<Vec<LiveKitEvent>>,
    result: Mutex<Option<tokio::sync::oneshot::Sender<(bool, bool)>>>,
}
#[async_trait::async_trait]
impl FrameProcessor for DispatchBurst {
    fn name(&self) -> &str {
        "livekit-input-dispatch-saturation"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let events = std::mem::take(&mut *self.events.lock().unwrap());
            let mut events = events.into_iter();
            // Both production pushes are immediately ready, so the consumer cannot
            // drain the first slot between these two admissions on this runtime.
            let first = crate::transport::dispatch_input(events.next().unwrap(), ctx)
                .await
                .is_ok();
            let second = crate::transport::dispatch_input(events.next().unwrap(), ctx)
                .await
                .is_err();
            self.result
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .send((first, second))
                .unwrap();
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn production_dispatch_reports_full_downstream_admission_without_waiting_for_consumption() {
    let (readers, _) = VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let events = (0..2)
        .map(|_| queued(&readers, key(VideoSource::Camera), stamp("TR", 1), 3, 3).event)
        .collect();
    let (tx, rx) = tokio::sync::oneshot::channel();
    let mut worker = Pipeline::new(vec![Arc::new(DispatchBurst {
        events: Mutex::new(events),
        result: Mutex::new(Some(tx)),
    })])
    .start(PipelineConfig {
        output_capacity: 1,
        ..Default::default()
    })
    .await
    .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    worker
        .sender()
        .send(Frame::text("dispatch"), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(
        timeout(Duration::from_secs(2), rx).await.unwrap().unwrap(),
        (true, true)
    );
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::UserImage(_)
    ));
    // Core output-lane saturation is fatal, even though this fixture observes
    // the returned dispatch error itself. Do not expect graceful End afterward.
    assert_eq!(worker.state(), pipecat_core::WorkerState::Failed);
    assert!(matches!(
        worker.end().await,
        Err(pipecat_core::PipelineError::Closed)
    ));
    assert!(
        matches!(worker.collect().await, Err(pipecat_core::PipelineError::Backpressure(message)) if message.contains("worker output queue is full"))
    );
}

fn sampling_time(readers: &VideoReaders, now: Duration) {
    readers.gates.lock().unwrap().now = Some(now);
}
fn sample_at(readers: &VideoReaders, key: &Key, stamp: &Stamp, millis: u64) -> bool {
    sampling_time(readers, Duration::from_millis(millis));
    readers.lane.capture.sample(key, stamp).unwrap()
}
fn activate(readers: &VideoReaders, key: &Key, stamp: &Stamp) {
    readers
        .gates
        .lock()
        .unwrap()
        .active
        .insert(key.clone(), stamp.clone());
}

#[test]
fn capture_modes_limits_and_default_all_frames_without_policy_retention() {
    for rate in [0, 1_000_000_001, u32::MAX] {
        assert!(VideoCaptureMode::FramesPerSecond(rate).params().is_err());
    }
    for rate in [1, 30, 1_000_000_000] {
        assert!(VideoCaptureMode::FramesPerSecond(rate).params().is_ok());
    }
    assert!(VideoInputConfig {
        sources: HashMap::from([(VideoSource::Camera, VideoCaptureMode::FramesPerSecond(0))]),
        ..Default::default()
    }
    .validate()
    .is_err());
    for count in [0, 129, usize::MAX] {
        assert!(VideoInputConfig {
            max_capture_policies: count,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    for bytes in [0, 65_537, usize::MAX] {
        assert!(VideoInputConfig {
            max_policy_identity_bytes: bytes,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    let (readers, _) = VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    for source in [VideoSource::Camera, VideoSource::ScreenVideo] {
        let k = key(source);
        let s = stamp("TR", 1);
        activate(&readers, &k, &s);
        assert!(sample_at(&readers, &k, &s, 0));
        assert!(sample_at(&readers, &k, &s, 0));
    }
    assert!(readers.gates.lock().unwrap().policies.is_empty());
}

#[test]
fn configured_allowlist_override_and_independent_participant_source_schedules() {
    let config = VideoInputConfig {
        sources: HashMap::from([(VideoSource::Camera, VideoCaptureMode::FramesPerSecond(2))]),
        ..Default::default()
    };
    let (readers, _) = VideoReaders::new(config, CancellationToken::new());
    let camera = key(VideoSource::Camera);
    let screen = key(VideoSource::ScreenVideo);
    let bob = Key {
        identity: "bob".into(),
        source: VideoSource::Camera,
    };
    let s = stamp("TR", 1);
    for k in [&camera, &screen, &bob] {
        activate(&readers, k, &s);
    }
    assert!(sample_at(&readers, &camera, &s, 0));
    assert!(!sample_at(&readers, &camera, &s, 449));
    assert!(sample_at(&readers, &bob, &s, 449));
    assert!(!sample_at(&readers, &screen, &s, 449));
    readers
        .capture_handle()
        .capture(
            "alice",
            VideoSource::ScreenVideo,
            VideoCaptureMode::AllFrames,
        )
        .unwrap();
    assert!(sample_at(&readers, &screen, &s, 449));
    assert!(sample_at(&readers, &camera, &s, 450));
    assert!(!sample_at(&readers, &bob, &s, 450));
    readers
        .capture_handle()
        .capture("alice", VideoSource::Camera, VideoCaptureMode::AllFrames)
        .unwrap();
    assert!(sample_at(&readers, &camera, &s, 450));
}

#[test]
fn strict_stall_boundary_no_catchup_and_rate_switch_preserves_schedule() {
    let (readers, _) = VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let k = key(VideoSource::Camera);
    let s = stamp("TR", 1);
    activate(&readers, &k, &s);
    let handle = readers.capture_handle();
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(2))
        .unwrap();
    assert!(sample_at(&readers, &k, &s, 0));
    handle
        .capture("alice", k.source, VideoCaptureMode::AllFrames)
        .unwrap();
    assert!(sample_at(&readers, &k, &s, 100));
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(2))
        .unwrap();
    assert!(!sample_at(&readers, &k, &s, 449));
    // Exactly one interval late uses max(next, now-tolerance)+interval = 1450ms.
    assert!(sample_at(&readers, &k, &s, 1000));
    assert!(sample_at(&readers, &k, &s, 1400));
    // Far past a stall resets to now+interval; no immediate catch-up burst.
    assert!(sample_at(&readers, &k, &s, 5000));
    assert!(!sample_at(&readers, &k, &s, 5000));
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(4))
        .unwrap();
    assert!(!sample_at(&readers, &k, &s, 5449));
    assert!(sample_at(&readers, &k, &s, 5450));
    let other = key(VideoSource::ScreenVideo);
    activate(&readers, &other, &s);
    handle
        .capture("alice", other.source, VideoCaptureMode::FramesPerSecond(2))
        .unwrap();
    assert!(sample_at(&readers, &other, &s, 6000));
    sampling_time(
        &readers,
        Duration::from_millis(7000) + Duration::from_nanos(1),
    );
    assert!(handle.sample(&other, &s).unwrap());
    assert!(!sample_at(&readers, &other, &s, 7400));
    sampling_time(
        &readers,
        Duration::from_millis(7450) + Duration::from_nanos(1),
    );
    assert!(handle.sample(&other, &s).unwrap());
}

#[tokio::test]
async fn policy_capacity_invalid_update_departure_and_closed_rejection_are_atomic() {
    let (mut readers, _) = VideoReaders::new(
        VideoInputConfig {
            max_capture_policies: 1,
            max_policy_identity_bytes: 5,
            ..Default::default()
        },
        CancellationToken::new(),
    );
    let handle = readers.capture_handle();
    let k = key(VideoSource::Camera);
    let s = stamp("TR", 1);
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(2))
        .unwrap();
    for (identity, source, mode) in [
        ("alice", k.source, VideoCaptureMode::FramesPerSecond(0)),
        ("bob", k.source, VideoCaptureMode::AllFrames),
        (
            "alice",
            VideoSource::ScreenVideo,
            VideoCaptureMode::AllFrames,
        ),
        ("", k.source, VideoCaptureMode::AllFrames),
    ] {
        assert!(handle.capture(identity, source, mode).is_err());
    }
    // Existing-key updates remain allowed at both full count and byte limits.
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(2))
        .unwrap();
    activate(&readers, &k, &s);
    assert!(sample_at(&readers, &k, &s, 0));
    assert!(!sample_at(&readers, &k, &s, 100));
    assert_eq!(readers.gates.lock().unwrap().identity_bytes, 5);
    readers.remove_participant("alice").await.unwrap(); // No owned track, only pre-subscription policy.
    assert!(readers.gates.lock().unwrap().policies.is_empty());
    assert!(handle
        .capture("longer", k.source, VideoCaptureMode::AllFrames)
        .is_err());
    handle
        .capture("bob", k.source, VideoCaptureMode::AllFrames)
        .unwrap();
    readers.close().await.unwrap();
    assert!(matches!(
        handle.capture("bob", k.source, VideoCaptureMode::AllFrames),
        Err(Error::Closed)
    ));
    assert_eq!(readers.gates.lock().unwrap().identity_bytes, 0);
}

#[test]
fn lazy_policy_exhaustion_rejects_without_unbounded_all_frame_fallback() {
    let (readers, _) = VideoReaders::new(
        VideoInputConfig {
            sources: HashMap::from([(VideoSource::Camera, VideoCaptureMode::FramesPerSecond(1))]),
            max_capture_policies: 1,
            ..Default::default()
        },
        CancellationToken::new(),
    );
    let a = key(VideoSource::Camera);
    let b = Key {
        identity: "bob".into(),
        source: a.source,
    };
    let s = stamp("TR", 1);
    activate(&readers, &a, &s);
    activate(&readers, &b, &s);
    assert!(sample_at(&readers, &a, &s, 0));
    assert!(matches!(
        readers.lane.capture.sample(&b, &s),
        Err(Error::Limit(_))
    ));
    assert_eq!(readers.gates.lock().unwrap().policies.len(), 1);
}

#[test]
fn stale_generation_cannot_consume_cadence_and_invalid_not_due_skips_conversion() {
    let (readers, mut receiver) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let k = key(VideoSource::Camera);
    let old = stamp("old", 1);
    let new = stamp("new", 2);
    readers
        .capture_handle()
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(1))
        .unwrap();
    activate(&readers, &k, &new);
    assert!(!sample_at(&readers, &k, &old, 0));
    assert!(sample_at(&readers, &k, &new, 0));
    sampling_time(&readers, Duration::from_millis(100));
    let invalid: BoxVideoFrame = VideoFrame {
        buffer: Box::new(NV12Buffer::new(1, 1)),
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 999999999,
        frame_metadata: None,
    };
    deliver_frame(invalid, &k, &new, &readers.lane, &readers.config).unwrap();
    let stats = readers.stats().snapshot();
    assert_eq!(
        (
            stats.sampled_out_frames,
            stats.rejected_frames,
            stats.dropped_frames
        ),
        (1, 0, 0)
    );
    assert!(receiver.rx.try_recv().is_err());
    assert_eq!(
        readers.lane.pending.available_permits(),
        readers.config.queue_capacity
    );
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
}

#[tokio::test]
async fn capture_policy_survives_mute_replacement_unsubscribe_reconnect_not_departure() {
    let (_, track) = source();
    let (mut readers, mut receiver) =
        VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let k = key(VideoSource::Camera);
    let handle = readers.capture_handle();
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(1))
        .unwrap();
    readers
        .start(k.clone(), "old".into(), track.rtc_track(), true)
        .await
        .unwrap();
    readers.unmute(&k, "old").unwrap();
    let old = readers.readers[&k].stamp.clone();
    assert!(sample_at(&readers, &k, &old, 0));
    readers.mute(&k, "old").await.unwrap();
    readers.unmute(&k, "old").unwrap();
    let fresh = readers.readers[&k].stamp.clone();
    assert_ne!(old, fresh);
    assert!(!sample_at(&readers, &k, &fresh, 100));
    readers
        .start(k.clone(), "new".into(), track.rtc_track(), true)
        .await
        .unwrap();
    readers.stop(&k, Some("old")).await.unwrap();
    assert_eq!(readers.gates.lock().unwrap().policies.len(), 1);
    readers.stop(&k, Some("new")).await.unwrap();
    readers.reset_readers().await.unwrap();
    assert_eq!(readers.gates.lock().unwrap().policies.len(), 1);
    readers
        .start(k.clone(), "reconnected".into(), track.rtc_track(), false)
        .await
        .unwrap();
    let current = readers.readers[&k].stamp.clone();
    assert!(!sample_at(&readers, &k, &old, 950));
    assert!(sample_at(&readers, &k, &current, 950));
    readers.remove_participant("alice").await.unwrap();
    assert!(readers.gates.lock().unwrap().policies.is_empty());
    readers.close().await.unwrap();
    receiver.close();
}

#[test]
fn concurrent_capture_and_sampling_share_one_bounded_schedule_without_deadlock() {
    let (readers, _) = VideoReaders::new(VideoInputConfig::default(), CancellationToken::new());
    let k = key(VideoSource::Camera);
    let s = stamp("TR", 1);
    activate(&readers, &k, &s);
    sampling_time(&readers, Duration::ZERO);
    readers
        .capture_handle()
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(1))
        .unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                for _ in 0..100 {
                    readers
                        .capture_handle()
                        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(1))
                        .unwrap();
                }
            });
        }
        let sampled = scope.spawn(|| {
            (0..400)
                .filter(|_| readers.lane.capture.sample(&k, &s).unwrap())
                .count()
        });
        assert_eq!(sampled.join().unwrap(), 1);
    });
    assert_eq!(readers.gates.lock().unwrap().policies.len(), 1);
    assert_eq!(readers.gates.lock().unwrap().identity_bytes, 5);
}

/// Local native source/stream and real pipeline, with deterministic sampler time.
/// This is not evidence of a remote RoomEvent, SFU, RTP or decoder path.
#[tokio::test]
async fn sampled_native_stream_admission_to_pipeline_preserves_rgb_and_source_metadata() {
    let (source, track) = source();
    let (mut readers, mut receiver) = VideoReaders::new(
        VideoInputConfig {
            sources: HashMap::from([(
                VideoSource::ScreenVideo,
                VideoCaptureMode::FramesPerSecond(1),
            )]),
            ..Default::default()
        },
        CancellationToken::new(),
    );
    let k = key(VideoSource::ScreenVideo);
    sampling_time(&readers, Duration::ZERO);
    readers
        .start(k, "TR_sample".into(), track.rtc_track(), false)
        .await
        .unwrap();
    // Capture before yielding avoids native startup black keepalive. Do not filter colors.
    assert!(source.capture_frame(&fixture(8, 4, true, [82, 90, 240])));
    let first = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(
        matches!(&first, LiveKitEvent::Video { image, .. } if image.image[0] >= 250 && image.image[2] < 5)
    );
    sampling_time(&readers, Duration::from_millis(100));
    assert!(source.capture_frame(&fixture(8, 4, true, [145, 54, 34])));
    timeout(Duration::from_secs(2), async {
        while readers.stats().snapshot().sampled_out_frames == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(receiver.rx.try_recv().is_err());
    assert_eq!(
        readers.lane.pending.available_permits(),
        readers.config.queue_capacity
    );
    sampling_time(&readers, Duration::from_millis(950));
    assert!(source.capture_frame(&fixture(8, 4, true, [41, 240, 110])));
    let event = timeout(Duration::from_secs(2), receiver.recv())
        .await
        .unwrap()
        .unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(DispatchOnce(Mutex::new(Some(event))))])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let image = timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.unwrap().frame;
            if matches!(frame.data, FrameData::UserImage(_)) {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(image.transport_source.as_deref(), Some("screenVideo"));
    assert_eq!(image.metadata["livekit.track_sid"], "TR_sample");
    let FrameData::UserImage(user) = image.data else {
        unreachable!()
    };
    assert_eq!(user.user_id, "alice");
    assert_eq!(user.image.size, (8, 4));
    let (pixels, remainder) = user.image.image.as_chunks::<3>();
    assert!(remainder.is_empty(), "RGB must contain whole triplets");
    assert!(pixels.iter().all(|p| p[0] < 5 && p[1] < 5 && p[2] >= 250));
    assert!(user.request.is_none());
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    readers.close().await.unwrap();
    receiver.close();
    assert_eq!(readers.stats().snapshot().dropped_frames, 0);
    assert_eq!(readers.stats().snapshot().rejected_frames, 0);
}

#[tokio::test]
async fn selected_pressure_drop_consumes_cadence_and_sampling_does_not_recall_queued_image() {
    let (readers, mut receiver) = VideoReaders::new(
        VideoInputConfig {
            queue_capacity: 1,
            ..Default::default()
        },
        CancellationToken::new(),
    );
    let k = key(VideoSource::Camera);
    let s = stamp("TR", 1);
    activate(&readers, &k, &s);
    let handle = readers.capture_handle();
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(1))
        .unwrap();
    let send = || {
        deliver_frame(
            boxed(fixture(3, 3, true, [82, 90, 240])),
            &k,
            &s,
            &readers.lane,
            &readers.config,
        )
        .unwrap()
    };
    sampling_time(&readers, Duration::ZERO);
    send();
    sampling_time(&readers, Duration::from_secs(1));
    send();
    assert_eq!(readers.stats().snapshot().dropped_frames, 1);
    // A policy update does not invalidate already queued output.
    handle
        .capture("alice", k.source, VideoCaptureMode::FramesPerSecond(2))
        .unwrap();
    assert!(receiver.recv().await.is_some());
    sampling_time(&readers, Duration::from_millis(1100));
    send();
    assert!(receiver.rx.try_recv().is_err());
    assert_eq!(readers.stats().snapshot().sampled_out_frames, 1);
    assert_eq!(readers.lane.pending.available_permits(), 1);
    assert_eq!(
        readers.lane.bytes.available_permits(),
        readers.config.max_retained_bytes
    );
}
