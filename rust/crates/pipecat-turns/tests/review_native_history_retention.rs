//! Native terminal retention is bounded independently of conversation history.
use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
#[derive(Default)]
struct Capture(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "IndependentRetentionCapture"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
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
}
async fn next(worker: &mut PipelineWorker) -> Frame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = worker.recv().await.unwrap();
            if item.frame.native_tool_outcome().unwrap().is_some() {
                return item.frame;
            }
        }
    })
    .await
    .unwrap()
}
fn publish(ctx: &ProcessorContext, id: usize, result: serde_json::Value) -> ToolCallReceipt {
    ctx.tool_outcome_publisher(
        FunctionCall {
            function_name: "result".into(),
            tool_call_id: format!("bounded-{id}"),
            arguments: json!({}),
        },
        true,
    )
    .unwrap()
    .complete(
        Ok(result),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    )
}
async fn bounded_history(count: usize, result_bytes: usize) {
    let capture = Arc::new(Capture::default());
    let weak_capture = Arc::downgrade(&capture);
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let context = SharedContext::new(Context::default());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    assistant
        .process_frame(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .unwrap();
    let mut first = None;
    for id in 0..count {
        let receipt = publish(&ctx, id, json!("x".repeat(result_bytes)));
        assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
        drop(receipt);
        let frame = next(&mut worker).await;
        if id == 0 {
            first = Some(Arc::downgrade(
                frame.native_tool_outcome().unwrap().unwrap(),
            ));
        }
        assistant
            .process_frame(frame, Direction::Downstream)
            .unwrap();
        if id == 0 {
            assert!(
                first.as_ref().unwrap().upgrade().is_some(),
                "terminal identity must initially be retained for duplicate validation"
            );
        }
    }
    assert!(
        first.unwrap().upgrade().is_none(),
        "old native outcome retained past count/byte tombstone limit"
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    assert_eq!(
        context
            .snapshot_rich()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m.role == ChatRole::Tool)
            .count(),
        count,
        "metadata eviction must not erase conversation results"
    );
    assistant
        .process_frame(Frame::new(FrameData::Stop), Direction::Downstream)
        .unwrap();
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert!(
        weak_capture.upgrade().is_none(),
        "assistant tombstones retained runtime"
    );
}
#[tokio::test]
async fn independent_terminal_tombstones_evict_by_count() {
    bounded_history(514, 1).await;
}
#[tokio::test]
async fn independent_terminal_tombstones_evict_by_result_bytes() {
    bounded_history(10, 1024 * 1024).await;
}
#[tokio::test]
async fn independent_oversized_result_is_explicit_failure_not_success() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let receipt = publish(
        &ctx,
        0,
        json!("x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES)),
    );
    assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Failed);
    assert_eq!(receipt.outcome.reason(), ToolCallEndReason::ResultTooLarge);
    assert_eq!(
        receipt.outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(receipt.outcome.retained_bytes() < 1024);
    let frame = next(&mut worker).await;
    assert!(frame.native_tool_outcome().is_ok());
    let FrameData::FunctionCallResult(result) = frame.data else {
        panic!("oversized result did not settle")
    };
    assert!(result.error.is_some());
    assert!(result.result.to_string().len() < 1024);
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}

#[tokio::test]
async fn independent_compact_terminal_marker_survives_payload_eviction_and_frame_id_forgery() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let context = SharedContext::new(Context::default());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    assistant
        .process_frame(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .unwrap();
    let call = |id| {
        Arc::new(FunctionCall {
            function_name: "result".into(),
            tool_call_id: format!("bounded-{id}"),
            arguments: json!({}),
        })
    };
    let batch =
        NativeToolCallBatch::new("held-group".into(), vec![(call(0), true), (call(1), true)])
            .unwrap();
    let original = ctx
        .tool_outcome_publisher(call(0), true)
        .unwrap()
        .with_batch(batch.clone())
        .unwrap()
        .complete(
            Ok(json!("x".repeat(7 * 1024 * 1024))),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    let sealed_id = original.outcome.id();
    drop(original);
    let mut first = next(&mut worker).await;
    assert_eq!(first.id, sealed_id);
    first.id = u64::MAX;
    assert_eq!(
        first.native_tool_outcome().unwrap().unwrap().id(),
        sealed_id,
        "mutable envelope changed native outcome identity"
    );
    assistant
        .process_frame(first.duplicate(), Direction::Downstream)
        .unwrap();
    publish(&ctx, 9999, json!("x".repeat(2 * 1024 * 1024)));
    assistant
        .process_frame(next(&mut worker).await, Direction::Downstream)
        .unwrap();
    // This completion exceeds the payload cache, but its compact marker cannot
    // be removed while the original group still has the second active member.
    let snapshot = context.snapshot_rich().unwrap();
    assistant
        .process_frame(first.duplicate(), Direction::Downstream)
        .unwrap();
    assert_eq!(context.snapshot_rich().unwrap(), snapshot);
    let forged = ctx
        .tool_outcome_publisher(call(0), true)
        .unwrap()
        .with_batch(batch.clone())
        .unwrap()
        .complete(
            Ok(json!("x".repeat(7 * 1024 * 1024))),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    assert_ne!(forged.outcome.id(), sealed_id);
    drop(forged);
    let mut forged = next(&mut worker).await;
    forged.id = u64::MAX;
    assert!(
        assistant
            .process_frame(forged, Direction::Downstream)
            .is_err(),
        "separate terminal reused mutable frame id as committed native identity"
    );
    assert_eq!(context.snapshot_rich().unwrap(), snapshot);
    ctx.tool_outcome_publisher(call(1), true)
        .unwrap()
        .with_batch(batch)
        .unwrap()
        .complete(
            Ok(json!("last sibling")),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    assistant
        .process_frame(next(&mut worker).await, Direction::Downstream)
        .unwrap();
    assert!(
        !assistant.has_function_calls_in_progress().unwrap(),
        "payload eviction prevented final sibling settlement"
    );
    let mut stopped = Frame::new(FrameData::UserStoppedSpeaking);
    stopped
        .metadata
        .insert("user_context_follows".into(), json!(false));
    let output = assistant
        .process_frame(stopped, Direction::Downstream)
        .unwrap();
    assert_eq!(output.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame) if frame.direction == Direction::Upstream && matches!(frame.frame.data, FrameData::LlmContext(_)))).count(), 1);
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}

#[tokio::test]
async fn independent_evicted_call_identity_requires_new_context_before_reuse() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let context = SharedContext::new(Context::default());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    assistant
        .process_frame(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .unwrap();
    publish(&ctx, 0, json!("old terminal"));
    let old = next(&mut worker).await;
    assistant
        .process_frame(old.duplicate(), Direction::Downstream)
        .unwrap();
    for id in 1..514 {
        publish(&ctx, id, json!(id));
        assistant
            .process_frame(next(&mut worker).await, Direction::Downstream)
            .unwrap();
    }
    publish(&ctx, 0, json!("different terminal"));
    assert!(
        assistant
            .process_frame(next(&mut worker).await, Direction::Downstream)
            .is_err(),
        "expired marker allowed overwriting an existing conversation call"
    );
    assistant
        .process_frame(
            Frame::new(FrameData::LlmMessagesUpdate(MessagesFrame {
                messages: vec![],
                run_llm: false,
            })),
            Direction::Downstream,
        )
        .unwrap();
    publish(&ctx, 0, json!("new context terminal"));
    assistant
        .process_frame(next(&mut worker).await, Direction::Downstream)
        .expect("expired, closed-group identity should be reusable in a new context");
    let snapshot = context.snapshot_rich().unwrap();
    assert_eq!(
        snapshot
            .messages
            .iter()
            .filter(|message| message.role == ChatRole::Tool)
            .count(),
        1
    );
    assert!(
        assistant.process_frame(old, Direction::Downstream).is_err(),
        "old receipt overwrote a new settlement after identity reuse"
    );
    assert_eq!(context.snapshot_rich().unwrap(), snapshot);
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
