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

#[tokio::test]
async fn synchronous_push_preserves_async_push_order_and_stale_drops() {
    let (mut worker, _, source) = capture().await;
    source
        .try_push(Frame::text("sync"), Direction::Downstream)
        .unwrap();
    source
        .push(Frame::text("async"), Direction::Downstream)
        .await
        .unwrap();
    for text in ["sync", "async"] {
        let frame = worker.recv().await.unwrap();
        assert!(matches!(frame.frame.data, FrameData::Text(ref value) if value.text == text));
    }
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ) {}
    source
        .try_push(Frame::text("stale sync"), Direction::Downstream)
        .unwrap();
    source
        .push(Frame::text("stale async"), Direction::Downstream)
        .await
        .unwrap();
    assert!(worker.try_recv().is_err());
    finish(worker).await;
}

#[tokio::test]
async fn invocation_push_preserves_current_order_inside_surviving_background() {
    let (mut worker, _, source) = capture().await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    source
        .clone()
        .spawn("current-invocation", false, async move {
            source.try_push_from_invocation(
                Frame::text("current"),
                Direction::Downstream,
                &source,
            )?;
            tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    rx.await.unwrap();
    assert!(
        matches!(worker.recv().await.unwrap().frame.data,FrameData::Text(ref value) if value.text == "current")
    );
    finish(worker).await;
}

#[tokio::test]
async fn invocation_push_fences_canceled_middle_origin_under_background_survival() {
    let (mut destination_worker, _, destination) = capture().await;
    let (mut middle_worker, _, middle) = capture().await;
    let (origin_worker, _, origin) = capture().await;
    let middle = middle.forwarding_context(&origin);
    let source = destination.forwarding_context(&middle);
    middle_worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(
        middle_worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ) {}
    assert!(origin.is_current());
    assert!(destination.is_current());
    assert!(!source.is_current());
    let (tx, rx) = tokio::sync::oneshot::channel();
    destination
        .clone()
        .spawn("retained-invocation", false, async move {
            assert!(
                source.is_current(),
                "fixture exercises ambient survival override"
            );
            destination.try_push_from_invocation(
                Frame::text("stale"),
                Direction::Downstream,
                &source,
            )?;
            let mut explicit = Frame::text("uninterruptible");
            explicit.interruptible = false;
            destination.try_push_from_invocation(explicit, Direction::Downstream, &source)?;
            tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    rx.await.unwrap();
    assert!(
        matches!(destination_worker.recv().await.unwrap().frame.data,FrameData::Text(ref value) if value.text == "uninterruptible")
    );
    assert!(destination_worker.try_recv().is_err());
    finish(destination_worker).await;
    finish(middle_worker).await;
    finish(origin_worker).await;
}
#[tokio::test]
async fn review_invocation_push_checks_inherited_origins_without_direct_generation() {
    let (mut destination_worker, _, destination) = capture().await;
    let (mut middle_worker, _, middle) = capture().await;
    let (origin_worker, _, origin) = capture().await;
    let middle = middle.forwarding_context(&origin);
    let source = destination.forwarding_context(&middle);
    // This is the public forwarding_context shape produced by a surviving task:
    // its direct epoch is absent, but its inherited origins remain meaningful.
    let source = BACKGROUND_INTERRUPTIBLE
        .scope(false, async { destination.forwarding_context(&source) })
        .await;
    assert_eq!(source.current_generation(), None);
    assert!(!source.origins.is_empty());
    middle_worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(
        middle_worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ) {}
    let (tx, rx) = tokio::sync::oneshot::channel();
    destination
        .clone()
        .spawn("review-inherited-fence", false, async move {
            destination.try_push_from_invocation(
                Frame::text("must-drop"),
                Direction::Downstream,
                &source,
            )?;
            let mut durable = Frame::text("durable");
            durable.interruptible = false;
            destination.try_push_from_invocation(durable, Direction::Downstream, &source)?;
            tx.send(()).unwrap();
            Ok(())
        })
        .unwrap();
    rx.await.unwrap();
    let observed = destination_worker.recv().await.unwrap();
    assert!(
        matches!(observed.frame.data, FrameData::Text(ref text) if text.text == "durable"),
        "stale inherited frame escaped: {:?}",
        observed.frame.data
    );
    assert!(destination_worker.try_recv().is_err());
    finish(destination_worker).await;
    finish(middle_worker).await;
    finish(origin_worker).await;
}

#[tokio::test]
async fn review_raw_invocation_fences_original_system_identity_but_normal_push_survives() {
    let (mut worker, _, mut source) = capture().await;
    let same_system = Frame::new(FrameData::BotStartedSpeaking);
    source.system_frame_id = Some(same_system.id);
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ) {}
    source
        .try_push_from_invocation(same_system.clone(), Direction::Downstream, &source)
        .unwrap();
    assert!(
        worker.try_recv().is_err(),
        "raw invocation cannot inherit same-system identity exemption"
    );
    source.try_push(same_system, Direction::Downstream).unwrap();
    assert!(matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::BotStartedSpeaking
    ));
    finish(worker).await;
}
