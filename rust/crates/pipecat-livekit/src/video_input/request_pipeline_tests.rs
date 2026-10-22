//! Local SDK integration, not SFU or camera-acquisition freshness coverage.
//! Requests select a post-drain sink observation; SDK keepalive/replay may win.
use super::*;
use async_trait::async_trait;
use livekit::{
    prelude::LocalVideoTrack,
    webrtc::{
        video_frame::{I420Buffer, VideoFrame, VideoRotation},
        video_source::{native::NativeVideoSource, RtcVideoSource, VideoResolution},
    },
};
use pipecat_core::{
    AudioRawFrame, ChatRole, Direction, FrameData, FrameProcessor, LocalResultCallback,
    MessageContent, MessagePart, Pipeline, PipelineWorker, ProcessorContext, ProcessorError,
    StartFrame, UserImageRequestFrame,
};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use std::sync::atomic::AtomicUsize;
use tokio::time::{sleep, timeout};

const LIMIT: Duration = Duration::from_secs(3);

fn pixels() -> VideoFrame<I420Buffer> {
    let mut buffer = I420Buffer::with_strides(8, 4, 8, 4, 4);
    let (y, u, v) = buffer.data_mut();
    y.fill(82);
    u.fill(90);
    v.fill(240);
    VideoFrame {
        buffer,
        rotation: VideoRotation::VideoRotation0,
        timestamp_us: 0,
        frame_metadata: None,
    }
}
fn request(source: VideoSource, append: bool) -> Frame {
    Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
        user_id: "alice".into(),
        text: Some("Describe this exact observation: 红色".into()),
        append_to_context: Some(append),
        video_source: Some(source.as_str().into()),
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }))
}

/// Only observation/parking lives here: the ordinary assistant's actual
/// FrameProcessor handles the frame, including its real context commit gate.
struct AssistantProbe {
    inner: LLMAssistantAggregator,
    duplicate: bool,
    park: bool,
    release: Notify,
    received: AtomicUsize,
    processed: AtomicUsize,
    image: Mutex<Option<Frame>>,
}
#[async_trait]
impl FrameProcessor for AssistantProbe {
    fn name(&self) -> &str {
        "observed-real-assistant"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if direction == Direction::Downstream && matches!(frame.data, FrameData::UserImage(_)) {
            assert!(frame.native_user_image_constraint(direction)?.is_some());
            *self.image.lock().unwrap() = Some(frame.clone());
            self.received.fetch_add(1, Ordering::SeqCst);
            if self.park {
                self.release.notified().await;
            }
            let duplicate = self.duplicate.then(|| frame.duplicate());
            self.inner.process(frame, direction, ctx).await?;
            self.processed.fetch_add(1, Ordering::SeqCst);
            if let Some(duplicate) = duplicate {
                self.inner.process(duplicate, direction, ctx).await?;
                self.processed.fetch_add(1, Ordering::SeqCst);
            }
            Ok(())
        } else {
            self.inner.process(frame, direction, ctx).await
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
struct Fixture {
    readers: VideoReaders,
    capture: CaptureHandle,
    source: NativeVideoSource,
    _track: LocalVideoTrack,
    key: Key,
    stamp: Stamp,
    worker: PipelineWorker,
    probe: Arc<AssistantProbe>,
    context: SharedContext,
}
impl Fixture {
    async fn new(source_kind: VideoSource, duplicate: bool, park: bool) -> Self {
        let source = NativeVideoSource::new(
            VideoResolution {
                width: 8,
                height: 4,
            },
            false,
        );
        let track = LocalVideoTrack::create_video_track(
            "plain-request-test",
            RtcVideoSource::Native(source.clone()),
        );
        let config = VideoInputConfig {
            requests: Some(ImageRequestConfig::default()),
            sources: [(source_kind, VideoCaptureMode::RequestOnly)].into(),
            ..Default::default()
        };
        config.validate().unwrap();
        let (mut readers, receiver) = VideoReaders::new(config, CancellationToken::new());
        let key = Key {
            identity: "alice".into(),
            source: source_kind,
        };
        readers
            .start(key.clone(), "TR_plain".into(), track.rtc_track(), false)
            .await
            .unwrap();
        // NativeVideoStream installs the sink during start. Before that,
        // AdaptFrame legitimately rejects capture because no sink is interested.
        // Acceptance here is local source admission, never new-camera freshness.
        assert!(source.capture_frame(&pixels()));
        let capture = readers.capture_handle();
        let stamp = readers.gates.lock().unwrap().active[&key].clone();
        let context = SharedContext::default();
        let probe = Arc::new(AssistantProbe {
            inner: LLMAssistantAggregator::new(context.clone(), Default::default()),
            duplicate,
            park,
            release: Notify::new(),
            received: AtomicUsize::new(0),
            processed: AtomicUsize::new(0),
            image: Mutex::new(None),
        });
        let input = Arc::new(crate::transport::LiveKitInput::local_fixture(
            capture.clone(),
            receiver,
        ));
        let worker = Pipeline::new(vec![input, probe.clone()])
            .start(Default::default())
            .await
            .unwrap();
        Self {
            readers,
            capture,
            source,
            _track: track,
            key,
            stamp,
            worker,
            probe,
            context,
        }
    }
    async fn send_request(&self, frame: Frame) {
        let attempts = self.capture.stats.snapshot().request_dispatch_attempts;
        self.worker
            .sender()
            .send(frame, Direction::Upstream)
            .await
            .unwrap();
        // Actual transport admission acknowledgement, not a sleep or a claim
        // that the native reader has already finished its empty-queue fence.
        timeout(LIMIT, async {
            while self.capture.request_cutoff(&self.key, &self.stamp) == 0
                && self.capture.stats.snapshot().request_dispatch_attempts == attempts
            {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real input did not admit request");
    }
    async fn observe(&self) {
        timeout(LIMIT, async {
            while self.probe.received.load(Ordering::SeqCst) == 0 {
                assert!(self.source.capture_frame(&pixels()));
                sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("no post-drain native sink observation");
    }
    async fn processed(&self, count: usize) {
        timeout(LIMIT, async {
            while self.probe.processed.load(Ordering::SeqCst) != count {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("real assistant did not finish image");
    }
    async fn finish(mut self, expected_contexts: usize) {
        self.worker.end().await.unwrap();
        let (outputs, report) = timeout(LIMIT, self.worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let contexts = outputs
            .iter()
            .filter(|output| {
                matches!(
                    output.frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                )
            })
            .count();
        assert_eq!(contexts, expected_contexts);
        self.readers.close().await.unwrap();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_upstream_native_request_appends_exact_context_once_despite_clone() {
    for source in [VideoSource::Camera, VideoSource::ScreenVideo] {
        let fixture = Fixture::new(source, true, false).await;
        let request = request(source, true);
        let request_id = request.id;
        let FrameData::UserImageRequest(expected) = request.data.clone() else {
            unreachable!()
        };
        fixture.send_request(request).await;
        fixture.observe().await;
        fixture.processed(2).await;
        {
            let retained = fixture.probe.image.lock().unwrap();
            let frame = retained.as_ref().unwrap();
            assert_eq!(frame.transport_source.as_deref(), Some(source.as_str()));
            assert_eq!(frame.metadata["livekit.track_sid"], "TR_plain");
            assert_eq!(
                frame
                    .native_user_image_constraint(Direction::Downstream)
                    .unwrap()
                    .unwrap()
                    .request_id(),
                request_id
            );
            let FrameData::UserImage(image) = &frame.data else {
                unreachable!()
            };
            assert_eq!(image.user_id, "alice");
            assert_eq!(image.text, expected.text);
            assert_eq!(image.request.as_deref(), Some(&expected));
            assert_eq!(image.image.size, (8, 4));
            assert_eq!(image.image.format.as_deref(), Some("RGB"));
            assert_eq!(image.image.image.len(), 8 * 4 * 3);
            let (pixels, remainder) = image.image.image.as_chunks::<3>();
            assert!(remainder.is_empty());
            assert_eq!(pixels.len(), 32);
            // The SDK can emit black keepalive despite a red source fixture;
            // this contract validates RGB observation, not new/red capture.
            assert!(image.image.image.capacity() <= 786_432);
        }
        let context = fixture.context.snapshot_rich().unwrap();
        assert_eq!(context.messages.len(), 1);
        assert_eq!(context.messages[0].role, ChatRole::User);
        let MessageContent::Parts(parts) = &context.messages[0].content else {
            panic!("expected multimodal context")
        };
        assert_eq!(parts.len(), 2);
        assert!(
            matches!(&parts[0], MessagePart::Text { text } if Some(text) == expected.text.as_ref())
        );
        assert!(
            matches!(&parts[1], MessagePart::Image { url, .. } if url.starts_with("data:image/jpeg;base64,"))
        );
        fixture.finish(1).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn plain_passive_native_request_and_clone_never_append_or_infer() {
    let fixture = Fixture::new(VideoSource::Camera, true, false).await;
    fixture
        .send_request(request(VideoSource::Camera, false))
        .await;
    fixture.observe().await;
    fixture.processed(2).await;
    assert!(fixture.context.snapshot().unwrap().messages.is_empty());
    fixture.finish(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_request_mute_or_close_before_delayed_commit_keeps_context_empty() {
    for close in [false, true] {
        let mut fixture = Fixture::new(VideoSource::Camera, false, true).await;
        fixture
            .send_request(request(VideoSource::Camera, true))
            .await;
        fixture.observe().await;
        assert!(fixture.context.snapshot().unwrap().messages.is_empty());
        if close {
            fixture.readers.close().await.unwrap();
        } else {
            fixture
                .readers
                .mute(&fixture.key, "TR_plain")
                .await
                .unwrap();
        }
        assert!(!fixture
            .probe
            .image
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .native_user_image_constraint(Direction::Downstream)
            .unwrap()
            .unwrap()
            .is_current());
        fixture.probe.release.notify_one();
        fixture.processed(1).await;
        assert!(fixture.context.snapshot().unwrap().messages.is_empty());
        fixture.finish(0).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_request_delayed_commit_uses_original_invocation_after_interruption() {
    let fixture = Fixture::new(VideoSource::Camera, false, true).await;
    fixture
        .send_request(request(VideoSource::Camera, true))
        .await;
    fixture.observe().await;
    fixture
        .worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    timeout(LIMIT, async {
        loop {
            let current = fixture
                .probe
                .image
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .native_user_image_constraint(Direction::Downstream)
                .unwrap()
                .unwrap()
                .is_current();
            if !current {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("original invocation fence remained current");
    fixture.probe.release.notify_one();
    fixture.processed(1).await;
    assert!(fixture.context.snapshot().unwrap().messages.is_empty());
    fixture.finish(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn malformed_tool_and_callback_requests_are_local_and_later_audio_progresses() {
    let mut fixture = Fixture::new(VideoSource::Camera, false, false).await;
    let callbacks = Arc::new(AtomicUsize::new(0));
    for variant in 0..5 {
        let mut frame = request(VideoSource::Camera, true);
        let FrameData::UserImageRequest(request) = &mut frame.data else {
            unreachable!()
        };
        match variant {
            0 => request.user_id.clear(),
            1 => request.video_source = Some("unsupported-source".into()),
            2 => request.function_name = Some("inspect_camera".into()),
            3 => request.tool_call_id = Some("call-not-authorized".into()),
            _ => {
                let callbacks = callbacks.clone();
                request.result_callback = Some(LocalResultCallback::new(move |_| {
                    callbacks.fetch_add(1, Ordering::SeqCst);
                    async { Ok(()) }
                }));
            }
        }
        fixture
            .worker
            .sender()
            .send(frame, Direction::Upstream)
            .await
            .unwrap();
    }
    timeout(LIMIT, async {
        while fixture.capture.stats.snapshot().rejected_requests != 5 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("requests were not rejected locally by LiveKitInput");
    let audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![0; 320], 16_000, 1).unwrap(),
    ));
    let id = audio.id;
    fixture
        .worker
        .sender()
        .send(audio, Direction::Downstream)
        .await
        .unwrap();
    timeout(LIMIT, async {
        loop {
            let output = fixture
                .worker
                .recv()
                .await
                .expect("pipeline failed before later audio");
            assert!(!matches!(
                output.frame.data,
                FrameData::Error(_) | FrameData::ErrorDetailed(_)
            ));
            if output.frame.id == id {
                assert_eq!(output.direction, Direction::Downstream);
                assert!(matches!(output.frame.data, FrameData::InputAudio(_)));
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(callbacks.load(Ordering::SeqCst), 0);
    assert_eq!(fixture.probe.received.load(Ordering::SeqCst), 0);
    assert!(fixture.context.snapshot().unwrap().messages.is_empty());
    fixture.finish(0).await;
}
