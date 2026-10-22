use super::*;
use pipecat_core::{
    async_trait, FrameProcessor, Pipeline, PipelineWorker, ProcessorError, UserImageRequestFrame,
};
use tokio::sync::mpsc;

struct Admit {
    handle: CaptureHandle,
    ack: mpsc::UnboundedSender<()>,
}
#[async_trait]
impl FrameProcessor for Admit {
    fn name(&self) -> &str {
        "request-ledger-fixture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if self.handle.handles_request(&frame, direction) {
            self.handle.request(frame, ctx);
            let _ = self.ack.send(());
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
fn key() -> Key {
    Key {
        identity: "alice".into(),
        source: VideoSource::Camera,
    }
}
fn stamp() -> Stamp {
    Stamp {
        sid: "TR_camera".into(),
        generation: 1,
    }
}
fn request() -> Frame {
    Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
        user_id: "alice".into(),
        text: Some("describe this observation".into()),
        append_to_context: Some(true),
        video_source: None,
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }))
}
async fn fixture(
    limits: ImageRequestConfig,
) -> (
    VideoReaders,
    VideoReceiver,
    PipelineWorker,
    mpsc::UnboundedReceiver<()>,
) {
    let (readers, rx) = VideoReaders::new(
        VideoInputConfig {
            requests: Some(limits),
            sources: HashMap::from([(VideoSource::Camera, VideoCaptureMode::RequestOnly)]),
            ..Default::default()
        },
        CancellationToken::new(),
    );
    readers.gates.lock().unwrap().active.insert(key(), stamp());
    let (ack, notices) = mpsc::unbounded_channel();
    let worker = Pipeline::new(vec![Arc::new(Admit {
        handle: readers.capture_handle(),
        ack,
    })])
    .start(Default::default())
    .await
    .unwrap();
    (readers, rx, worker, notices)
}
async fn submit(
    worker: &PipelineWorker,
    notices: &mut mpsc::UnboundedReceiver<()>,
    request: Frame,
) {
    worker
        .sender()
        .send(request, Direction::Upstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), notices.recv())
        .await
        .unwrap()
        .unwrap();
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
fn response() -> Frame {
    LiveKitEvent::Video {
        identity: "alice".into(),
        track_sid: "TR_camera".into(),
        source: VideoSource::Camera,
        image: ImageRawFrame {
            image: vec![255, 0, 0],
            size: (1, 1),
            format: Some("RGB".into()),
        },
    }
    .into_frame()
    .unwrap()
    .unwrap()
}
#[test]
fn plain_request_config_and_request_only_are_explicitly_bounded() {
    assert!(ImageRequestConfig::default().validate().is_ok());
    for bad in [0, 33, usize::MAX] {
        assert!(ImageRequestConfig {
            max_requests: bad,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    for timeout in [Duration::ZERO, Duration::from_secs(61)] {
        assert!(ImageRequestConfig {
            timeout,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    for max_rgb_bytes in [0, RGB_LIMIT - 1, 33 * RGB_LIMIT] {
        assert!(ImageRequestConfig {
            max_rgb_bytes,
            ..Default::default()
        }
        .validate()
        .is_err());
    }
    let mut config = VideoInputConfig {
        sources: HashMap::from([(VideoSource::Camera, VideoCaptureMode::RequestOnly)]),
        ..Default::default()
    };
    assert!(config.validate().is_err());
    config.requests = Some(Default::default());
    assert!(config.validate().is_ok());
}
#[tokio::test]
async fn cutoff_excludes_old_observations_and_never_rebinds_second_request() {
    let (readers, _rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let first_cutoff = handle.request_cutoff(&key(), &stamp());
    submit(&worker, &mut notices, request()).await;
    assert!(!handle.sample_requested(&key(), &stamp(), 0).unwrap().0);
    let first = handle
        .sample_requested(&key(), &stamp(), first_cutoff)
        .unwrap()
        .1
        .unwrap();
    assert_eq!(first.token, first_cutoff);
    assert!(
        !handle
            .sample_requested(&key(), &stamp(), first_cutoff)
            .unwrap()
            .0
    );
    let cutoff = handle.request_cutoff(&key(), &stamp());
    assert!(cutoff > first_cutoff);
    let second = handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .unwrap();
    assert_ne!(first.token, second.token);
    assert!(!handle.sample_requested(&key(), &stamp(), cutoff).unwrap().0);
    drop((first, second));
    finish(worker).await;
}
#[tokio::test]
async fn duplicate_ids_and_invalid_fields_do_not_reserve_or_consume_fifo() {
    let (readers, _rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    let original = request();
    submit(&worker, &mut notices, original.clone()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    submit(&worker, &mut notices, original).await;
    for variant in 0..7 {
        let mut bad = request();
        let FrameData::UserImageRequest(payload) = &mut bad.data else {
            unreachable!()
        };
        match variant {
            0 => payload.user_id = "alice\n".into(),
            1 => payload.user_id = "a".repeat(257),
            2 => payload.text = Some("x".repeat(8193)),
            3 => payload.video_source = Some("screen".into()),
            4 => payload.function_name = Some("tool".into()),
            5 => payload.tool_call_id = Some("call".into()),
            _ => {
                bad.metadata.insert("ignored".into(), true.into());
            }
        }
        submit(&worker, &mut notices, bad).await;
    }
    assert_eq!(handle.request_cutoff(&key(), &stamp()), cutoff);
    assert_eq!(readers.gates.lock().unwrap().requests.seen.len(), 1);
    assert_eq!(readers.stats().snapshot().rejected_requests, 8);
    assert!(handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .is_some());
    finish(worker).await;
}
#[tokio::test]
async fn exact_destination_direction_identity_and_source_not_sid() {
    let (readers, _rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    let mut frame = request();
    assert!(handle.handles_request(&frame, Direction::Upstream));
    assert!(!handle.handles_request(&frame, Direction::Downstream));
    frame.transport_destination = Some("elsewhere".into());
    assert!(!handle.handles_request(&frame, Direction::Upstream));
    for (identity, source) in [
        ("TR_camera", None),
        ("Alice", None),
        ("alice", Some("screenVideo")),
    ] {
        let mut frame = request();
        let FrameData::UserImageRequest(payload) = &mut frame.data else {
            unreachable!()
        };
        payload.user_id = identity.into();
        payload.video_source = source.map(str::to_owned);
        submit(&worker, &mut notices, frame).await;
    }
    assert_eq!(readers.gates.lock().unwrap().requests.seen.len(), 0);
    for source in [None, Some("")] {
        let mut frame = request();
        let FrameData::UserImageRequest(payload) = &mut frame.data else {
            unreachable!()
        };
        payload.video_source = source.map(str::to_owned);
        submit(&worker, &mut notices, frame).await;
    }
    assert_eq!(readers.gates.lock().unwrap().requests.seen.len(), 2);
    finish(worker).await;
}
#[tokio::test]
async fn retained_carriers_keep_slot_and_rgb_after_dispatch_and_expiry() {
    let config = ImageRequestConfig {
        max_requests: 1,
        max_requests_per_source: 1,
        max_rgb_bytes: RGB_LIMIT,
        ..Default::default()
    };
    let (readers, _rx, worker, mut notices) = fixture(config).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    let selected = handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .unwrap();
    let fence = selected.fence.clone();
    let mut frame = response();
    drop(selected.bind(&mut frame).unwrap());
    let clone = frame.clone();
    assert_eq!(handle.request_resources.slots.available_permits(), 0);
    assert_eq!(handle.request_resources.rgb.available_permits(), 0);
    submit(&worker, &mut notices, request()).await;
    assert_eq!(readers.stats().snapshot().rejected_requests, 1);
    readers.gates.lock().unwrap().requests.entries[0].deadline = Deadline::now();
    handle.maintain_requests();
    let constraint = frame
        .native_user_image_constraint(Direction::Downstream)
        .unwrap()
        .unwrap();
    assert!(!constraint.is_current());
    assert!(fence
        .with_current(|| constraint.begin_commit().unwrap().is_none())
        .unwrap());
    assert_eq!(handle.request_resources.slots.available_permits(), 0);
    drop(frame);
    assert_eq!(handle.request_resources.slots.available_permits(), 0);
    drop(clone);
    handle.maintain_requests();
    assert_eq!(handle.request_resources.slots.available_permits(), 1);
    assert_eq!(handle.request_resources.rgb.available_permits(), RGB_LIMIT);
    assert!(readers.gates.lock().unwrap().requests.entries.is_empty());
    finish(worker).await;
}
#[tokio::test]
async fn one_selected_binding_clones_share_consume_once_and_replay_ids_never_evict() {
    let (readers, _rx, worker, mut notices) = fixture(ImageRequestConfig {
        max_request_ids: 1,
        ..Default::default()
    })
    .await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    let selected = handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .unwrap();
    let fence = selected.fence.clone();
    let mut frame = response();
    drop(selected.bind(&mut frame).unwrap()); // consumes the sole selected owner
    assert!(handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .is_none());
    let duplicate = frame.duplicate();
    fence
        .with_current(|| {
            frame
                .native_user_image_constraint(Direction::Downstream)
                .unwrap()
                .unwrap()
                .begin_commit()
                .unwrap()
                .unwrap()
                .complete();
            assert!(duplicate
                .native_user_image_constraint(Direction::Downstream)
                .unwrap()
                .unwrap()
                .begin_commit()
                .unwrap()
                .is_none());
        })
        .unwrap();
    drop((frame, duplicate));
    handle.maintain_requests();
    submit(&worker, &mut notices, request()).await;
    assert_eq!(readers.gates.lock().unwrap().requests.seen.len(), 1);
    assert_eq!(readers.stats().snapshot().rejected_requests, 1);
    finish(worker).await;
}
#[tokio::test]
async fn expired_waiting_and_stale_generation_do_not_advance_cadence() {
    let (readers, _rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    handle
        .capture(
            "alice",
            VideoSource::Camera,
            VideoCaptureMode::FramesPerSecond(1),
        )
        .unwrap();
    readers.gates.lock().unwrap().now = Some(Duration::ZERO);
    assert!(handle.sample(&key(), &stamp()).unwrap());
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    readers.gates.lock().unwrap().requests.entries[0].deadline = Deadline::now();
    assert!(!handle.sample_requested(&key(), &stamp(), cutoff).unwrap().0);
    let stale = Stamp {
        generation: 0,
        ..stamp()
    };
    assert!(!handle.sample_requested(&key(), &stale, cutoff).unwrap().0);
    readers.gates.lock().unwrap().now = Some(Duration::from_millis(949));
    assert!(!handle.sample(&key(), &stamp()).unwrap());
    readers.gates.lock().unwrap().now = Some(Duration::from_millis(950));
    assert!(handle.sample(&key(), &stamp()).unwrap());
    finish(worker).await;
}
#[tokio::test]
async fn generation_invalidation_closes_selected_source_before_bind_and_close_rejects_new() {
    let (readers, _rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    let selected = handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .unwrap();
    readers.gates.lock().unwrap().active.insert(
        key(),
        Stamp {
            generation: 2,
            ..stamp()
        },
    );
    handle.maintain_requests();
    assert!(selected.bind(&mut response()).is_err());
    handle.close_requests();
    submit(&worker, &mut notices, request()).await;
    assert_eq!(readers.gates.lock().unwrap().requests.seen.len(), 1);
    finish(worker).await;
}
#[tokio::test]
async fn header_budget_exhaustion_is_local_without_accepting_id_or_callback() {
    let (readers, _rx, worker, mut notices) = fixture(ImageRequestConfig {
        max_header_bytes: 1,
        ..Default::default()
    })
    .await;
    submit(&worker, &mut notices, request()).await;
    assert!(readers.gates.lock().unwrap().requests.seen.is_empty());
    assert_eq!(readers.stats().snapshot().rejected_requests, 1);
    assert_eq!(
        readers
            .capture_handle()
            .request_resources
            .slots
            .available_permits(),
        32
    );
    finish(worker).await;
}

#[tokio::test]
async fn lifecycle_closes_already_marked_source_before_returning() {
    let (readers, _rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    let selected = handle
        .sample_requested(&key(), &stamp(), cutoff)
        .unwrap()
        .1
        .unwrap();
    let mut frame = response();
    drop(selected.bind(&mut frame).unwrap());
    // Deterministic interleaving: maintainer A marked the entry but was
    // descheduled before closing its cloned source outside the transport gate.
    let source_a = {
        let mut state = readers.gates.lock().unwrap();
        state.requests.entries[0].closed = true;
        state.requests.entries[0].source.clone()
    };
    let constraint = frame
        .native_user_image_constraint(Direction::Downstream)
        .unwrap()
        .unwrap();
    assert!(constraint.is_current());
    // Lifecycle B cannot mistake A's marker for completed invalidation.
    handle.close_requests();
    assert!(!constraint.is_current());
    source_a.close(); // A resumes; repeated closure is harmless.
    drop(frame);
    finish(worker).await;
}

#[tokio::test]
async fn spare_text_capacity_is_charged_before_retaining_original_payload() {
    let (readers, _rx, worker, mut notices) = fixture(ImageRequestConfig {
        max_header_bytes: 8192,
        ..Default::default()
    })
    .await;
    let mut frame = request();
    let FrameData::UserImageRequest(payload) = &mut frame.data else {
        unreachable!()
    };
    payload.text = Some(String::with_capacity(8192));
    submit(&worker, &mut notices, frame).await;
    assert!(readers.gates.lock().unwrap().requests.seen.is_empty());
    assert_eq!(
        readers
            .capture_handle()
            .request_resources
            .headers
            .available_permits(),
        8192
    );
    finish(worker).await;
}

fn native_frame(width: u32) -> BoxVideoFrame {
    use livekit::webrtc::video_frame::{I420Buffer, VideoFrame, VideoRotation};
    VideoFrame {
        buffer: Box::new(I420Buffer::new_black(width, 2)),
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    }
}
struct ScriptedIngress {
    step: usize,
    handle: CaptureHandle,
    invocation: ProcessorContext,
}
impl Stream for ScriptedIngress {
    type Item = BoxVideoFrame;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.step += 1;
        match self.step {
            1 => {
                // Request two arrives while draining request one's cutoff.
                // The returned oversize frame MUST be discarded, not selected.
                self.handle
                    .admit_request(request(), &self.invocation)
                    .unwrap();
                Poll::Ready(Some(native_frame(1025)))
            }
            2 | 4 => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            3 => Poll::Ready(Some(native_frame(1025))), // second request needs a new fence
            5 | 6 => Poll::Ready(Some(native_frame(2))),
            _ => Poll::Ready(None),
        }
    }
}
#[tokio::test]
async fn production_reader_drains_old_frames_and_repeats_fence_for_mid_drain_acceptance() {
    let (readers, mut rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let invocation = readers.gates.lock().unwrap().requests.entries[0]
        .output
        .as_ref()
        .unwrap()
        .clone();
    let mut stream = ScriptedIngress {
        step: 0,
        handle: handle.clone(),
        invocation,
    };
    read_observations(
        &mut stream,
        key(),
        stamp(),
        readers.lane.clone(),
        CancellationToken::new(),
        readers.config.clone(),
    )
    .await
    .unwrap();
    assert_eq!(stream.step, 7);
    assert_eq!(readers.stats().snapshot().failed_requests, 0);
    for _ in 0..2 {
        let InputEvent::Requested(image) = rx.recv_input().await.unwrap() else {
            panic!("request-only output")
        };
        let FrameData::UserImage(payload) = &image.frame.data else {
            unreachable!()
        };
        assert_eq!(payload.image.size, (2, 2));
        assert!(image
            .frame
            .native_user_image_constraint(Direction::Downstream)
            .unwrap()
            .is_some());
    }
    assert!(rx.rx.try_recv().is_err());
    finish(worker).await;
}
struct NeverEmpty {
    polls: usize,
}
impl Stream for NeverEmpty {
    type Item = BoxVideoFrame;
    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.polls += 1;
        Poll::Ready(Some(native_frame(2)))
    }
}
#[tokio::test]
async fn continuously_ready_ingress_yields_and_expires_without_false_empty_fence() {
    let (readers, _rx, worker, mut notices) = fixture(ImageRequestConfig {
        timeout: Duration::from_millis(10),
        ..Default::default()
    })
    .await;
    submit(&worker, &mut notices, request()).await;
    let mut stream = NeverEmpty { polls: 0 };
    let result = tokio::time::timeout(
        Duration::from_millis(20),
        drain_to_pending(&mut stream, &readers.lane),
    )
    .await;
    assert!(
        result.is_err(),
        "Ready frames must never manufacture an empty fence"
    );
    assert!(stream.polls >= 8);
    readers.capture_handle().maintain_requests();
    assert_eq!(readers.capture_handle().request_cutoff(&key(), &stamp()), 0);
    assert_eq!(readers.stats().snapshot().failed_requests, 1);
    assert_eq!(readers.stats().snapshot().request_dispatch_attempts, 0);
    finish(worker).await;
}

#[tokio::test]
async fn oversize_and_queue_pressure_fail_selected_request_not_later_fifo() {
    let (readers, mut rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    deliver_observation(
        native_frame(1025),
        &key(),
        &stamp(),
        &readers.lane,
        &readers.config,
        cutoff,
    )
    .unwrap();
    assert_eq!(readers.stats().snapshot().failed_requests, 1);
    assert!(rx.rx.try_recv().is_err());
    let held = readers
        .lane
        .pending
        .clone()
        .try_acquire_many_owned(readers.config.queue_capacity as u32)
        .unwrap();
    deliver_observation(
        native_frame(2),
        &key(),
        &stamp(),
        &readers.lane,
        &readers.config,
        cutoff,
    )
    .unwrap();
    assert_eq!(readers.stats().snapshot().failed_requests, 2);
    drop(held);
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    deliver_observation(
        native_frame(2),
        &key(),
        &stamp(),
        &readers.lane,
        &readers.config,
        cutoff,
    )
    .unwrap();
    let InputEvent::Requested(image) = rx.recv_input().await.unwrap() else {
        panic!("request-only image")
    };
    assert!(image
        .frame
        .native_user_image_constraint(Direction::Downstream)
        .unwrap()
        .is_some());
    drop(image);
    finish(worker).await;
}

#[tokio::test]
async fn full_core_rgb_ceiling_is_compacted_and_conversion_error_stays_request_local() {
    use livekit::webrtc::video_frame::{I420Buffer, NV12Buffer, VideoFrame, VideoRotation};
    let (readers, mut rx, worker, mut notices) = fixture(Default::default()).await;
    let handle = readers.capture_handle();
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    let unsupported = VideoFrame {
        buffer: Box::new(NV12Buffer::new(2, 2))
            as Box<dyn livekit::webrtc::video_frame::VideoBuffer>,
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    };
    deliver_observation(
        unsupported,
        &key(),
        &stamp(),
        &readers.lane,
        &readers.config,
        cutoff,
    )
    .unwrap();
    assert_eq!(readers.stats().snapshot().failed_requests, 1);
    submit(&worker, &mut notices, request()).await;
    let cutoff = handle.request_cutoff(&key(), &stamp());
    let maximum = VideoFrame {
        buffer: Box::new(I420Buffer::new_black(512, 512))
            as Box<dyn livekit::webrtc::video_frame::VideoBuffer>,
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    };
    deliver_observation(
        maximum,
        &key(),
        &stamp(),
        &readers.lane,
        &readers.config,
        cutoff,
    )
    .unwrap();
    let InputEvent::Requested(image) = rx.recv_input().await.unwrap() else {
        panic!("requested maximum RGB")
    };
    let FrameData::UserImage(payload) = &image.frame.data else {
        unreachable!()
    };
    assert_eq!(payload.image.image.len(), RGB_LIMIT);
    assert_eq!(payload.image.image.capacity(), RGB_LIMIT);
    assert!(image
        .frame
        .native_user_image_constraint(Direction::Downstream)
        .unwrap()
        .is_some());
    drop(image);
    finish(worker).await;
}
