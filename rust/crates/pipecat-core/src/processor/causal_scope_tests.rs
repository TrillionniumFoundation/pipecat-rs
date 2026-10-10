use super::*;
use crate::{Pipeline, PipelineConfig, PipelineWorker};
use std::sync::Mutex;

struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "capture"
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
async fn capture() -> (PipelineWorker, ProcessorContext, ProcessorContext) {
    let first = Arc::new(Capture(Mutex::new(None)));
    let second = Arc::new(Capture(Mutex::new(None)));
    let mut worker = Pipeline::new(vec![first.clone(), second.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("scope"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        let frame = worker.recv().await.unwrap();
        if matches!(frame.frame.data, FrameData::Text(_)) {
            break;
        }
    }
    let first = first.0.lock().unwrap().as_ref().unwrap().clone();
    let second = second.0.lock().unwrap().as_ref().unwrap().clone();
    (worker, first, second)
}
async fn finish(worker: PipelineWorker) {
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn scope_compares_destination_runtime_index_and_raw_invocation_generation() {
    let (worker, first, second) = capture().await;
    let (other_worker, other, _) = capture().await;
    assert!(first.same_causal_scope(&first.clone()));
    assert!(!first.same_causal_scope(&second));
    assert!(!first.same_causal_scope(&other));
    let mut changed = first.clone();
    changed.generation = Some(AtomicU64::new(1));
    assert!(!first.same_causal_scope(&changed));
    changed.generation = None;
    assert!(!first.same_causal_scope(&changed));
    let mut identity_only = first.clone();
    identity_only.system_frame_id = Some(u64::MAX);
    assert!(first.same_causal_scope(&identity_only));
    finish(worker).await;
    finish(other_worker).await;
}
#[tokio::test]
async fn scope_compares_all_inherited_origin_fields_length_and_order() {
    let (worker, destination, second) = capture().await;
    let (other_worker, source, _) = capture().await;
    let original = destination.forwarding_context(&source);
    assert!(original.same_causal_scope(&destination.forwarding_context(&source)));
    assert!(!original.same_causal_scope(&destination));
    for dimension in 0..3 {
        let mut changed = original.clone();
        match dimension {
            0 => changed.origins[0].generation += 1,
            1 => changed.origins[0].index += 1,
            _ => changed.origins[0].runtime = destination.runtime.clone(),
        }
        assert!(
            !original.same_causal_scope(&changed),
            "origin dimension {dimension} was ignored"
        );
    }
    let mut nested = original.clone();
    nested.origins.push(ForwardingOrigin {
        runtime: second.runtime.clone(),
        index: second.index,
        generation: 0,
    });
    assert!(!original.same_causal_scope(&nested));
    let mut reversed = nested.clone();
    reversed.origins.reverse();
    assert!(!nested.same_causal_scope(&reversed));
    assert!(nested.same_causal_scope(&nested.clone()));
    finish(worker).await;
    finish(other_worker).await;
}
#[tokio::test]
async fn scope_comparison_is_unmasked_in_surviving_background_work() {
    let (mut worker, old, _) = capture().await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ) {}
    assert!(!old.is_current());
    assert!(
        old.same_causal_scope(&old.clone()),
        "scope equality is not a freshness test"
    );
    let mut newer = old.clone();
    newer.generation = Some(AtomicU64::new(old.current_generation().unwrap() + 1));
    let (tx, rx) = tokio::sync::oneshot::channel();
    newer
        .clone()
        .spawn("scope-survivor", false, async move {
            assert!(old.is_current(), "fixture must exercise survival override");
            assert!(!old.same_causal_scope(&newer));
            assert!(old.same_causal_scope(&old.clone()));
            tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    rx.await.unwrap();
    finish(worker).await;
}
