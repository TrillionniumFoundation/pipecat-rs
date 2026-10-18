use super::*;
use pipecat_core::{
    ImageRawFrame, NativeUserImageSource, Pipeline, PipelineWorker, UserImageRawFrame,
    UserImageRequestFrame,
};
use std::sync::atomic::AtomicUsize;
use std::time::Duration;
use tokio::sync::Notify;
const LIMIT: Duration = Duration::from_secs(3);

struct Source {
    owner: NativeUserImageSource,
    produced: Notify,
    interrupted: Notify,
    contexts: Notify,
    duplicate: bool,
    retain_image: bool,
    retained: Mutex<Option<Frame>>,
    seen_contexts: AtomicUsize,
    lease: Arc<()>,
}
impl Source {
    fn new(duplicate: bool) -> Self {
        Self {
            owner: NativeUserImageSource::new(),
            produced: Notify::new(),
            interrupted: Notify::new(),
            contexts: Notify::new(),
            duplicate,
            retain_image: false,
            retained: Mutex::new(None),
            seen_contexts: AtomicUsize::new(0),
            lease: Arc::new(()),
        }
    }
}
#[async_trait]
impl FrameProcessor for Source {
    fn name(&self) -> &str {
        "SyntheticImageSource"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::UserImageRequest(request) = &frame.data {
            let mut image = Frame::new(FrameData::UserImage(UserImageRawFrame {
                image: ImageRawFrame {
                    image: vec![255, 0, 0],
                    size: (1, 1),
                    format: Some("RGB".into()),
                },
                user_id: request.user_id.clone(),
                text: request.text.clone(),
                append_to_context: request.append_to_context,
                request: Some(Box::new(request.clone())),
                run_llm: None,
            }));
            let scope = ctx.invocation_fence()?;
            self.owner
                .bind(&mut image, &frame, scope.clone(), self.lease.clone())?;
            if self.retain_image {
                *self.retained.lock().unwrap() = Some(image.clone());
            }
            let duplicate = self.duplicate.then(|| image.duplicate());
            ctx.try_push_with_invocation_fence(image, Direction::Downstream, &scope)?;
            if let Some(duplicate) = duplicate {
                ctx.try_push_with_invocation_fence(duplicate, Direction::Downstream, &scope)?;
            }
            self.produced.notify_one();
            return Ok(());
        }
        if matches!(frame.data, FrameData::Interruption) {
            self.interrupted.notify_one();
        }
        if matches!(
            frame.data,
            FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
        ) {
            let cause = frame
                .native_user_image_context(direction)?
                .expect("derived constraint");
            if cause.is_current() {
                self.seen_contexts.fetch_add(1, Ordering::SeqCst);
                self.contexts.notify_one();
            }
        }
        ctx.push(frame, direction).await
    }
}
struct ParkSystem {
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl FrameProcessor for ParkSystem {
    fn name(&self) -> &str {
        "ParkSystemLane"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::OutputTransportMessageUrgent(value) if value == "park")
        {
            self.entered.notify_one();
            self.release.notified().await;
            return Ok(());
        }
        // Deliberately ordinary forwarding: an original System image survives
        // runtime interruption and must be rejected by the final consumer gate.
        ctx.push(frame, direction).await
    }
}
fn request() -> Frame {
    Frame::new(FrameData::UserImageRequest(UserImageRequestFrame {
        user_id: "synthetic-user".into(),
        text: Some("red pixel".into()),
        append_to_context: Some(true),
        video_source: None,
        function_name: None,
        tool_call_id: None,
        result_callback: None,
    }))
}
async fn notify(notify: &Notify) {
    tokio::time::timeout(LIMIT, notify.notified())
        .await
        .unwrap();
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn barrier(worker: &mut PipelineWorker) {
    let frame = Frame::new(FrameData::Flush(pipecat_core::FlushFrame {
        origin: Some("image-test".into()),
        returning: false,
    }));
    let id = frame.id;
    send(worker, frame).await;
    tokio::time::timeout(LIMIT, async {
        while let Some(output) = worker.recv().await {
            if output.frame.id == id {
                return;
            }
        }
        panic!("pipeline ended before image barrier");
    })
    .await
    .unwrap();
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(LIMIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn actual_assistant_consumes_cloned_request_once_and_keeps_one_context() {
    let source = Arc::new(Source::new(true));
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![source.clone(), assistant])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, request()).await;
    notify(&source.produced).await;
    notify(&source.contexts).await;
    barrier(&mut worker).await;
    assert_eq!(shared.snapshot().unwrap().messages.len(), 1);
    assert_eq!(source.seen_contexts.load(Ordering::SeqCst), 1);
    finish(worker).await;
    assert_eq!(Arc::strong_count(&source.lease), 1);
}
#[tokio::test]
async fn already_queued_system_image_cannot_commit_after_original_source_interrupts() {
    let source = Arc::new(Source::new(false));
    let park = Arc::new(ParkSystem {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![source.clone(), park.clone(), assistant])
        .start(Default::default())
        .await
        .unwrap();
    send(
        &worker,
        Frame::new(FrameData::OutputTransportMessageUrgent("park".into())),
    )
    .await;
    notify(&park.entered).await;
    send(&worker, request()).await;
    notify(&source.produced).await;
    assert!(
        Arc::strong_count(&source.lease) > 1,
        "queued System image retains slot"
    );
    send(&worker, Frame::interruption()).await;
    notify(&source.interrupted).await;
    park.release.notify_one();
    // Wait for actual final carrier release, not a normal-lane Flush which can
    // run concurrently with the System lane and overtake its image.
    tokio::time::timeout(LIMIT, async {
        while Arc::strong_count(&source.lease) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    barrier(&mut worker).await;
    assert!(shared.snapshot().unwrap().messages.is_empty());
    assert_eq!(source.seen_contexts.load(Ordering::SeqCst), 0);
    finish(worker).await;
    assert_eq!(Arc::strong_count(&source.lease), 1);
}
#[tokio::test]
async fn interruption_before_commit_excludes_history_and_after_commit_suppresses_pending_context() {
    for before_commit in [true, false] {
        let source = Arc::new(Source::new(false));
        let shared = SharedContext::default();
        let assistant = Arc::new(LLMAssistantAggregator::new(
            shared.clone(),
            Default::default(),
        ));
        let pause = Arc::new(ImageTestPause {
            before_commit,
            entered: Notify::new(),
            release: Notify::new(),
        });
        *assistant.image_test_pause.lock().unwrap() = Some(pause.clone());
        let mut worker = Pipeline::new(vec![source.clone(), assistant])
            .start(Default::default())
            .await
            .unwrap();
        send(&worker, request()).await;
        notify(&pause.entered).await;
        assert_eq!(
            shared.snapshot().unwrap().messages.len(),
            usize::from(!before_commit)
        );
        send(&worker, Frame::interruption()).await;
        notify(&source.interrupted).await;
        pause.release.notify_one();
        barrier(&mut worker).await;
        assert_eq!(
            shared.snapshot().unwrap().messages.len(),
            usize::from(!before_commit)
        );
        assert_eq!(source.seen_contexts.load(Ordering::SeqCst), 0);
        finish(worker).await;
    }
}

#[tokio::test]
async fn failed_derived_context_sealing_rolls_back_history_and_same_carrier_can_retry() {
    let mut source = Source::new(false);
    source.retain_image = true;
    let source = Arc::new(source);
    let shared = SharedContext::default();
    shared
        .append(Message::text(Role::System, "x".repeat(1024 * 1024)))
        .unwrap();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![source.clone(), assistant])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, request()).await;
    tokio::time::timeout(LIMIT, async {
        while let Some(frame) = worker.recv().await {
            if matches!(
                frame.frame.data,
                FrameData::Error(_) | FrameData::ErrorDetailed(_)
            ) {
                return;
            }
        }
        panic!("failed context seal did not report rejection");
    })
    .await
    .unwrap();
    assert_eq!(
        shared.snapshot().unwrap().messages.len(),
        1,
        "image append rolled back"
    );
    assert_eq!(source.seen_contexts.load(Ordering::SeqCst), 0);
    shared.replace(Default::default()).unwrap();
    let retained = source.retained.lock().unwrap().take().unwrap();
    send(&worker, retained).await;
    notify(&source.contexts).await;
    barrier(&mut worker).await;
    assert_eq!(shared.snapshot().unwrap().messages.len(), 1);
    assert_eq!(source.seen_contexts.load(Ordering::SeqCst), 1);
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(LIMIT, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.errors.len(), 1);
}

#[tokio::test]
async fn passive_marked_duplicate_does_not_append_or_infer() {
    let source = Arc::new(Source::new(true));
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        Default::default(),
    ));
    let mut worker = Pipeline::new(vec![source.clone(), assistant])
        .start(Default::default())
        .await
        .unwrap();
    let mut passive = request();
    let FrameData::UserImageRequest(request) = &mut passive.data else {
        unreachable!()
    };
    request.append_to_context = Some(false);
    send(&worker, passive).await;
    notify(&source.produced).await;
    tokio::time::timeout(LIMIT, async {
        while Arc::strong_count(&source.lease) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    barrier(&mut worker).await;
    assert!(shared.snapshot().unwrap().messages.is_empty());
    assert_eq!(source.seen_contexts.load(Ordering::SeqCst), 0);
    finish(worker).await;
}
