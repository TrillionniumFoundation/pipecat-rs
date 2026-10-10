//! Included as a child module of the existing real spoken-history fixture.
use super::*;

struct ReviewCapture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for ReviewCapture {
    fn name(&self) -> &str {
        "review-extra-receipt-origin"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(ctx.clone());
        }
        ctx.push(frame, direction).await
    }
}
async fn review_origin() -> (PipelineWorker, ProcessorContext) {
    let capture = Arc::new(ReviewCapture(Mutex::new(None)));
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("capture original scope"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(frame.frame.data, FrameData::Text(_)) {
            break;
        }
    }
    let ctx = capture.0.lock().unwrap().take().unwrap();
    (worker, ctx)
}
async fn review_interrupt_origin(worker: &mut PipelineWorker) {
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if matches!(frame.frame.data, FrameData::Interruption) {
            break;
        }
    }
}
struct ReviewScopedOutput {
    output: Arc<OutputTransport>,
    extra_origin: ProcessorContext,
    scope_stop: bool,
    admitted: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for ReviewScopedOutput {
    fn name(&self) -> &str {
        self.output.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.output.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let finished_admission = matches!(frame.data, FrameData::LlmResponseEnd);
        let extra = if self.scope_stop {
            matches!(frame.data, FrameData::TtsStopped(_))
        } else {
            matches!(frame.data, FrameData::TtsText(_))
        };
        // A retained invocation can already be stale inside an owned surviving
        // media task. Preserve that origin rather than resampling its runtime.
        if extra {
            self.output
                .process(
                    frame,
                    direction,
                    &ctx.forwarding_context(&self.extra_origin),
                )
                .await?;
        } else {
            self.output.process(frame, direction, ctx).await?;
        }
        if finished_admission {
            self.admitted.add_permits(1);
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.output.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.output.cleanup().await
    }
}
async fn review_scope_fixture(
    backend: Arc<Receipts>,
    extra_origin: ProcessorContext,
    scope_stop: bool,
    assistant_adapter: bool,
) -> (Fixture, Arc<Semaphore>) {
    let shared = SharedContext::default();
    let assistant = Arc::new(LLMAssistantAggregator::new(
        shared.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    ));
    #[cfg(feature = "assistant-history")]
    let sink: Arc<dyn SpokenHistorySink> = if assistant_adapter {
        Arc::new(AssistantSpokenHistorySink::new(assistant.clone()))
    } else {
        Arc::new(AssistantSink(assistant.clone()))
    };
    #[cfg(not(feature = "assistant-history"))]
    let sink: Arc<dyn SpokenHistorySink> = {
        let _ = assistant_adapter;
        Arc::new(AssistantSink(assistant.clone()))
    };
    let history = SpokenHistory::new(SpokenHistoryConfig::default(), sink).unwrap();
    let source = Arc::new(history.source_boundary("review-source").unwrap());
    let output = Arc::new(
        OutputTransport::new("review-output", config(), backend, None, BTreeMap::new())
            .unwrap()
            .with_spoken_history(history.clone())
            .unwrap(),
    );
    let control = output.control();
    let admitted = Arc::new(Semaphore::new(0));
    let scoped = Arc::new(ReviewScopedOutput {
        output,
        extra_origin,
        scope_stop,
        admitted: admitted.clone(),
    });
    let worker = Pipeline::new(vec![source, scoped, assistant.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !control.is_ready() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    (
        Fixture {
            #[cfg(feature = "assistant-history")]
            assistant,
            worker,
            history,
            shared,
            control,
        },
        admitted,
    )
}
async fn review_collect_through_fence(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let marker = Frame::new(FrameData::Flush(FlushFrame {
        origin: Some("review-admission-fence".into()),
        returning: false,
    }));
    let id = marker.id;
    send(worker, marker).await;
    let mut observed = vec![];
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        let done = frame.frame.id == id;
        observed.push(frame);
        if done {
            return observed;
        }
    }
}
fn review_is_effect(frame: &DirectedFrame) -> bool {
    matches!(
        frame.frame.data,
        FrameData::LlmContext(_)
            | FrameData::LlmContextDetailed(_)
            | FrameData::LlmContextAssistantTimestamp { .. }
            | FrameData::LlmContextAssistantTurn(_)
    )
}

#[tokio::test]
async fn review_receipt_late_text_and_stop_origins_fence_feedback_without_rolling_back_history() {
    for scope_stop in [false, true] {
        for stale_before_admission in [false, true] {
            let (mut origin_worker, origin) = review_origin().await;
            if stale_before_admission {
                review_interrupt_origin(&mut origin_worker).await;
            }
            let backend = Arc::new(Receipts::new(true, None, None));
            let (mut f, admitted) =
                review_scope_fixture(backend.clone(), origin, scope_stop, false).await;
            begin(&f.worker, "mixed-origin").await;
            complete(&f.worker, "mixed-origin").await;
            tokio::time::timeout(Duration::from_secs(3), admitted.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            backend.wait_calls(1).await;
            assert!(contents(&f.shared).is_empty());
            if !stale_before_admission {
                review_interrupt_origin(&mut origin_worker).await;
            }
            backend.gate.as_ref().unwrap().add_permits(1);
            let mut frames = review_collect_through_fence(&mut f.worker).await;
            assert_eq!(
                contents(&f.shared),
                ["Heard"],
                "truthful receipt commits independently of notification admission"
            );
            assert_eq!(f.history.status().committed_contexts, 1);
            f.worker.end().await.unwrap();
            let (tail, report) = tokio::time::timeout(Duration::from_secs(3), f.worker.collect())
                .await
                .unwrap()
                .unwrap();
            frames.extend(tail);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(
                !frames.iter().any(review_is_effect),
                "late contributing origins must fence every derived effect"
            );
            origin_worker.end().await.unwrap();
            let (_, report) = origin_worker.collect().await.unwrap();
            assert!(report.errors.is_empty());
        }
    }
}

#[tokio::test]
async fn review_current_mixed_receipt_origins_keep_real_broadcast_identity() {
    let (origin_worker, origin) = review_origin().await;
    let backend = Arc::new(Receipts::new(false, None, None));
    let (mut f, _) = review_scope_fixture(backend, origin, true, false).await;
    begin(&f.worker, "current-mixed-origin").await;
    complete(&f.worker, "current-mixed-origin").await;
    let mut frames = review_collect_through_fence(&mut f.worker).await;
    assert_eq!(contents(&f.shared), ["Heard"]);
    f.worker.end().await.unwrap();
    let (tail, report) = f.worker.collect().await.unwrap();
    frames.extend(tail);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let turns: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))
        .collect();
    assert_eq!(turns.len(), 2);
    assert_ne!(turns[0].direction, turns[1].direction);
    assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
    assert_eq!(turns[1].frame.broadcast_sibling_id, Some(turns[0].frame.id));
    origin_worker.end().await.unwrap();
    origin_worker.collect().await.unwrap();
}

#[cfg(feature = "assistant-history")]
#[tokio::test]
async fn stopped_record_keeps_late_receipt_origins_without_undoing_history() {
    for scope_stop in [false, true] {
        let (mut origin_worker, origin) = review_origin().await;
        let backend = Arc::new(Receipts::new(true, None, None));
        let (mut f, admitted) =
            review_scope_fixture(backend.clone(), origin, scope_stop, true).await;
        let mut records = f.assistant.subscribe_stopped(Default::default()).unwrap();
        begin(&f.worker, "stopped-scope").await;
        complete(&f.worker, "stopped-scope").await;
        tokio::time::timeout(Duration::from_secs(3), admitted.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        backend.wait_calls(1).await;
        review_interrupt_origin(&mut origin_worker).await;
        backend.gate.as_ref().unwrap().add_permits(1);
        review_collect_through_fence(&mut f.worker).await;
        assert_eq!(contents(&f.shared), ["Heard"]);
        assert!(records.try_recv().is_none());
        assert_eq!(records.stats().stale, 1);
        f.worker.end().await.unwrap();
        assert!(f.worker.collect().await.unwrap().1.errors.is_empty());
        assert!(records.recv().await.is_none());
        origin_worker.end().await.unwrap();
        assert!(origin_worker.collect().await.unwrap().1.errors.is_empty());
    }
}
