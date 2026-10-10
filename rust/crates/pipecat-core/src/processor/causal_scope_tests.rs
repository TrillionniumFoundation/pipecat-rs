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

#[tokio::test]
async fn realtime_fence_raw_none_still_checks_interrupted_inherited_origins() {
    let (worker, destination, _) = capture().await;
    let (other_worker, source, _) = capture().await;
    let mut inherited = destination.forwarding_context(&source);
    inherited.generation = None;
    let fence = inherited.invocation_fence().unwrap();
    assert!(fence.is_current());
    source.broadcast_interruption().await.unwrap();
    assert!(!fence.is_current());
    assert!(fence.with_current(|| panic!("stale admission")).is_none());
    finish(worker).await;
    finish(other_worker).await;
}
#[tokio::test]
async fn realtime_fence_ignores_background_survival_and_retains_no_runtime() {
    let (worker, source, _) = capture().await;
    let weak = Arc::downgrade(&source.runtime);
    let fence = BACKGROUND_INTERRUPTIBLE
        .scope(false, async { source.invocation_fence().unwrap() })
        .await;
    source.broadcast_interruption().await.unwrap();
    assert!(!fence.is_current());
    let after = source.invocation_fence().unwrap();
    assert!(after.is_current());
    let mut context = crate::Frame::new(FrameData::LlmContext(Default::default()));
    crate::NativeRealtimeContextProducer::default()
        .attach(&mut context, crate::RealtimeContextIntent::Run, true)
        .unwrap();
    context.bind_realtime_context_invocation(&source).unwrap();
    let retained = context.clone();
    finish(worker).await;
    assert!(!after.is_current());
    drop(source);
    assert!(!retained
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .inference_fence()
        .unwrap()
        .is_current());
    assert!(
        weak.upgrade().is_none(),
        "retained causes must not own runtime"
    );
    drop(retained);
    drop(context);
    drop(after);
    drop(fence);
    assert!(
        weak.upgrade().is_none(),
        "retained cause must not own runtime"
    );
}
#[tokio::test]
async fn realtime_fence_checks_all_nested_origins_in_either_order() {
    let (worker, destination, second) = capture().await;
    let (other_worker, source, _) = capture().await;
    let mut nested = destination.forwarding_context(&source);
    nested.origins.push(ForwardingOrigin {
        runtime: second.runtime.clone(),
        index: second.index,
        generation: 0,
    });
    let first = nested.invocation_fence().unwrap();
    nested.origins.reverse();
    let reversed = nested.invocation_fence().unwrap();
    assert!(first.is_current() && reversed.is_current());
    second.broadcast_interruption().await.unwrap();
    assert!(!first.is_current() && !reversed.is_current());
    finish(worker).await;
    finish(other_worker).await;
}

#[tokio::test]
async fn realtime_fence_stop_and_atomic_output_do_not_retain_runtime() {
    let (worker, source, _) = capture().await;
    let fence = source.invocation_fence().unwrap();
    source
        .try_push_with_invocation_fence(Frame::text("live"), Direction::Downstream, &fence)
        .unwrap();
    source.broadcast_interruption().await.unwrap();
    source
        .try_push_with_invocation_fence(Frame::text("stale"), Direction::Downstream, &fence)
        .unwrap();
    let new_fence = source.invocation_fence().unwrap();
    let weak = Arc::downgrade(&source.runtime);
    worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!frames
        .iter()
        .any(|frame| matches!(&frame.frame.data, FrameData::Text(text) if text.text=="stale")));
    assert!(!new_fence.is_current());
    drop(source);
    assert!(weak.upgrade().is_none());
    assert!(!new_fence.is_current());
}

#[tokio::test]
async fn realtime_fence_intersection_keeps_both_original_scopes_and_no_runtime_ownership() {
    let (left_worker, left, _) = capture().await;
    let (right_worker, right, _) = capture().await;
    let weak_left = Arc::downgrade(&left.runtime);
    let weak_right = Arc::downgrade(&right.runtime);
    let a = left.invocation_fence().unwrap();
    let b = right.invocation_fence().unwrap();
    let combined = a.intersection(&b).unwrap();
    let reversed = b.intersection(&a).unwrap();
    assert!(combined.is_current() && reversed.is_current());
    assert!(a.intersection(&a).unwrap().same_scope(&a));
    left.broadcast_interruption().await.unwrap();
    assert!(!combined.is_current() && !reversed.is_current());
    assert!(b.is_current());
    let fresh_left = left.invocation_fence().unwrap();
    let other = fresh_left.intersection(&b).unwrap();
    assert!(other.is_current());
    right.broadcast_interruption().await.unwrap();
    assert!(!other.is_current());
    assert!(fresh_left.is_current());
    assert!(other
        .with_current(|| panic!("intersection revived stale request"))
        .is_none());
    finish(left_worker).await;
    finish(right_worker).await;
    drop(left);
    drop(right);
    assert!(weak_left.upgrade().is_none() && weak_right.upgrade().is_none());
    assert!(!combined.is_current() && !other.is_current());
}

#[tokio::test]
async fn realtime_fence_intersection_checked_bound_preserves_distinct_raw_generations() {
    let (worker, source, _) = capture().await;
    let mut original = source.invocation_fence().unwrap();
    original.origins[0].2 = None;
    let mut full = original.clone();
    for generation in 0..63 {
        full.origins.push((
            Arc::downgrade(&source.runtime),
            source.index,
            Some(generation),
        ));
    }
    assert_eq!(full.intersection(&full).unwrap().origins.len(), 64);
    let mut extra = original.clone();
    extra.origins[0].2 = Some(63);
    assert!(full.intersection(&extra).is_err());
    assert_eq!(full.origins.len(), 64);
    assert!(original.is_current());
    source.broadcast_interruption().await.unwrap();
    // The raw None survives; the merged generation-0 restriction must not vanish.
    assert!(original.is_current());
    assert!(!full.is_current());
    finish(worker).await;
}

async fn native_scheduling_reason(
    worker: &mut PipelineWorker,
    source: &ProcessorContext,
    cancel_on_interruption: bool,
) -> crate::RealtimeNativeToolSchedulingReason {
    source
        .tool_outcome_publisher(
            crate::FunctionCall {
                function_name: "native".into(),
                tool_call_id: "one".into(),
                arguments: serde_json::json!({}),
            },
            cancel_on_interruption,
        )
        .unwrap()
        .complete(
            Ok(serde_json::json!(42)),
            crate::ToolCallEffectStatus::ExternalEffectsUnknown,
            crate::ToolCallEndReason::CallbackReturned,
        );
    loop {
        let frame = worker.recv().await.unwrap().frame;
        if let Some(outcome) = frame.native_tool_outcome().unwrap() {
            return crate::RealtimeNativeToolSchedulingReason::from_outcome(outcome);
        }
    }
}
fn native_context(reason: crate::RealtimeNativeToolSchedulingReason) -> Frame {
    let mut frame = Frame::new(FrameData::LlmContext(Default::default()));
    crate::NativeRealtimeContextProducer::default()
        .attach_assistant_continuation(&mut frame, Some(reason))
        .unwrap();
    frame
}
#[tokio::test]
async fn realtime_native_request_admission_renews_only_surviving_policy_and_never_bound_clone() {
    for cancel_on_interruption in [false, true] {
        let (mut worker, source, _) = capture().await;
        let reason = native_scheduling_reason(&mut worker, &source, cancel_on_interruption).await;
        let old = source.clone();
        source.broadcast_interruption().await.unwrap();
        assert!(!old.invocation_fence().unwrap().is_current());
        let frame = native_context(reason);
        let retained = frame.clone();
        old.try_push_realtime_context(frame, Direction::Upstream)
            .unwrap();
        let original = retained
            .native_realtime_context_cause()
            .unwrap()
            .unwrap()
            .inference_fence()
            .unwrap()
            .clone();
        assert_eq!(original.is_current(), !cancel_on_interruption);
        source.broadcast_interruption().await.unwrap();
        assert!(!original.is_current());
        source
            .try_push_realtime_context(retained.clone(), Direction::Upstream)
            .unwrap();
        assert!(retained
            .native_realtime_context_cause()
            .unwrap()
            .unwrap()
            .inference_fence()
            .unwrap()
            .same_scope(&original));
        assert!(
            !original.is_current(),
            "replayed bound cause renewed original scheduling scope"
        );
        finish(worker).await;
        assert!(source
            .try_push_realtime_context(native_context(reason), Direction::Upstream)
            .is_err());
    }
}
#[tokio::test]
async fn realtime_native_request_admission_raw_none_gets_local_epoch_and_keeps_inherited_scope() {
    let (mut worker, mut local, _) = capture().await;
    let reason = native_scheduling_reason(&mut worker, &local, false).await;
    local.generation = None;
    let frame = native_context(reason);
    let retained = frame.clone();
    local
        .try_push_realtime_context(frame, Direction::Upstream)
        .unwrap();
    let fence = retained
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .inference_fence()
        .unwrap();
    assert!(
        fence.origins[0].2.is_some(),
        "new scheduling cannot inherit a perpetual None exemption"
    );
    local.broadcast_interruption().await.unwrap();
    assert!(!fence.is_current());

    let (other_worker, origin, _) = capture().await;
    let inherited = local.forwarding_context(&origin);
    origin.broadcast_interruption().await.unwrap();
    let frame = native_context(reason);
    let retained = frame.clone();
    inherited
        .try_push_realtime_context(frame, Direction::Upstream)
        .unwrap();
    let fence = retained
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .inference_fence()
        .unwrap();
    assert!(
        !fence.is_current(),
        "fresh local admission discarded inherited stale origin"
    );
    assert_eq!(fence.origins.len(), 2);
    finish(other_worker).await;
    finish(worker).await;
}
