use pipecat_core::*;
use std::sync::{Arc, Mutex};

struct Probe {
    context: Mutex<Option<ProcessorContext>>,
    health: ProcessorHealth,
}
impl Probe {
    fn new(health: ProcessorHealth) -> Arc<Self> {
        Arc::new(Self {
            context: Mutex::new(None),
            health,
        })
    }
    fn context(&self) -> ProcessorContext {
        self.context.lock().unwrap().as_ref().unwrap().clone()
    }
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "same"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.context.lock().unwrap().take();
        Ok(())
    }
}
#[tokio::test]
async fn local_processor_identity_matches_error_attribution_and_distinguishes_names() {
    let a = Probe::new(ProcessorHealth::default());
    let b = Probe::new(ProcessorHealth::default());
    let mut worker = Pipeline::new(vec![a.clone(), b.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert_ne!(
        a.context().processor_identity(),
        b.context().processor_identity()
    );
    b.context()
        .push_error(ErrorFrame::new("test"))
        .await
        .unwrap();
    loop {
        let output = worker.recv().await.unwrap();
        if output.frame.data.as_error_frame().is_some() {
            assert_eq!(
                output.frame.error_origin().unwrap().processor,
                b.context().processor_identity()
            );
            break;
        }
    }
    worker.sender().end().await.unwrap();
    worker.collect().await.unwrap();
}
#[tokio::test]
async fn supplied_interruption_rejects_wrong_type_and_stale_reuse_without_losing_future_input() {
    let p = Probe::new(ProcessorHealth::default());
    let worker = Pipeline::new(vec![p.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let ctx = p.context();
    assert!(ctx
        .broadcast_interruption_frame(&Frame::text("wrong"))
        .await
        .is_err());
    let old = Frame::interruption();
    assert!(ctx.broadcast_interruption_frame(&old).await.unwrap());
    assert!(!ctx.broadcast_interruption_frame(&old).await.unwrap());
    let newer = Frame::interruption();
    assert!(ctx.broadcast_interruption_frame(&newer).await.unwrap());
    assert!(!ctx.broadcast_interruption_frame(&old).await.unwrap());
    worker
        .sender()
        .send(Frame::text("future"), Direction::Downstream)
        .await
        .unwrap();
    worker.sender().end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::Interruption))
            .count(),
        4
    );
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data,FrameData::Text(t) if t.text=="future")));
}
#[tokio::test]
async fn read_only_health_can_only_be_changed_by_owner_even_when_error_is_permanent() {
    let (health, owner) = ProcessorHealth::read_only(true);
    let mut changes = health.watch();
    assert!(!health.set_usable(false));
    assert!(!changes.has_changed().unwrap());
    let p = Probe::new(health.clone());
    let worker = Pipeline::new(vec![p.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await
        .unwrap();
    p.context()
        .push_error(ErrorFrame {
            category: ErrorCategory::Authentication,
            ..ErrorFrame::new("member failed")
        })
        .await
        .unwrap();
    assert!(health.is_usable());
    assert_eq!(health.snapshot().revision, 0);
    assert!(owner.set_usable(false));
    changes.changed().await.unwrap();
    assert!(!changes.borrow_and_update().is_usable);
    assert!(!health.set_usable(true));
    assert!(owner.set_usable(true));
    assert_eq!(health.snapshot().revision, 2);
    worker.sender().end().await.unwrap();
    assert_eq!(worker.collect().await.unwrap().1.state, WorkerState::Ended);
}

struct ReturnedError {
    health: ProcessorHealth,
    error: ProcessorError,
}
#[async_trait]
impl FrameProcessor for ReturnedError {
    fn name(&self) -> &str {
        "returned"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            Err(self.error.clone())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn returned_permanence_override_preserves_category_and_never_restores_health() {
    for (initial, category, overridden, expected) in [
        (true, ErrorCategory::InvalidRequest, Some(false), true),
        (false, ErrorCategory::InvalidRequest, Some(false), false),
        (true, ErrorCategory::Connectivity, Some(true), false),
        (true, ErrorCategory::InvalidRequest, None, false),
        (true, ErrorCategory::Connectivity, None, true),
    ] {
        let health = ProcessorHealth::new(initial);
        let mut error = ProcessorError::new("proposal").with_category(category);
        if let Some(value) = overridden {
            error = error.with_permanence_override(value);
        }
        let mut worker = Pipeline::new(vec![Arc::new(ReturnedError {
            health: health.clone(),
            error,
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(Frame::text("report"), Direction::Downstream)
            .await
            .unwrap();
        loop {
            let out = worker.recv().await.unwrap();
            if let Some(error) = out.frame.data.as_error_frame() {
                assert_eq!(error.category, category);
                assert_eq!(
                    out.frame.error_origin().unwrap().reported_health.is_usable,
                    expected
                );
                break;
            }
        }
        assert_eq!(health.is_usable(), expected);
        worker.sender().end().await.unwrap();
        worker.collect().await.unwrap();
    }
}
#[tokio::test]
async fn false_permanence_override_never_softens_fatal_failure() {
    let health = ProcessorHealth::default();
    let error = ProcessorError::fatal("fatal")
        .with_category(ErrorCategory::InvalidRequest)
        .with_permanence_override(false);
    let mut worker = Pipeline::new(vec![Arc::new(ReturnedError {
        health: health.clone(),
        error,
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    worker
        .sender()
        .send(Frame::text("report"), Direction::Downstream)
        .await
        .unwrap();
    while worker.recv().await.is_some() {}
    assert!(worker.join().await.is_err());
    assert!(health.is_usable());
}

#[test]
fn local_frame_tags_are_scoped_bounded_and_never_cross_wire_or_change_equality() {
    let owner = LocalFrameTag::new();
    let other = LocalFrameTag::new();
    let mut frame = Frame::text("private");
    let original = frame.clone();
    owner.mark(&mut frame).unwrap();
    owner.mark(&mut frame).unwrap();
    assert!(owner.contains(&frame));
    assert!(!other.contains(&frame));
    assert_eq!(original, frame);
    assert!(owner.contains(&frame.clone()));
    assert!(owner.contains(&frame.duplicate()));
    let json = serde_json::to_value(&frame).unwrap();
    assert_eq!(json, serde_json::to_value(&original).unwrap());
    let decoded: Frame = serde_json::from_value(json).unwrap();
    assert!(!owner.contains(&decoded));
    for _ in 1..LocalFrameTag::MAX_TAGS_PER_FRAME {
        LocalFrameTag::new().mark(&mut frame).unwrap();
    }
    assert!(LocalFrameTag::new().mark(&mut frame).is_err());
    assert!(owner.contains(&frame));
    owner.mark(&mut frame).unwrap();
}
