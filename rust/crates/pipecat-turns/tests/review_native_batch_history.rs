//! Batch membership must remain immutable across independently valid receipts.
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
        "IndependentNativeBatchCapture"
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
fn call(id: &str) -> Arc<FunctionCall> {
    Arc::new(FunctionCall {
        function_name: "write".into(),
        tool_call_id: id.into(),
        arguments: json!({"id":id}),
    })
}
fn batch(ids: &[&str]) -> Arc<NativeToolCallBatch> {
    NativeToolCallBatch::new(
        "sealed-group".into(),
        ids.iter().map(|id| (call(id), true)).collect(),
    )
    .unwrap()
}
fn runs(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|action|matches!(action,AggregationAction::Push(frame) if frame.direction==Direction::Upstream && matches!(frame.frame.data,FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
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
async fn incompatible(second_batch: Option<Arc<NativeToolCallBatch>>) {
    incompatible_for("b", second_batch).await;
}
async fn incompatible_for(second_call: &str, second_batch: Option<Arc<NativeToolCallBatch>>) {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let original = batch(&["a", "b"]);
    ctx.tool_outcome_publisher(call("a"), true)
        .unwrap()
        .with_batch(original.clone())
        .unwrap()
        .complete(
            Ok(json!("first")),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    let first = next(&mut worker).await;
    let context = SharedContext::new(Context::default());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    assert_eq!(
        runs(
            &assistant
                .process_frame(first, Direction::Downstream)
                .unwrap()
        ),
        0
    );
    let before = context.snapshot_rich().unwrap();
    let publisher = ctx.tool_outcome_publisher(call(second_call), true).unwrap();
    let publisher = match second_batch {
        Some(batch) => publisher.with_batch(batch).unwrap(),
        None => publisher,
    };
    publisher.complete(
        Ok(json!("second")),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    );
    let changed = next(&mut worker).await;
    assert!(
        assistant
            .process_frame(changed, Direction::Downstream)
            .is_err(),
        "native sibling accepted a different or absent batch membership"
    );
    assert_eq!(
        context.snapshot_rich().unwrap(),
        before,
        "invalid batch partially changed history"
    );
    ctx.tool_outcome_publisher(call("b"), true)
        .unwrap()
        .with_batch(original)
        .unwrap()
        .complete(
            Ok(json!("second")),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    assert_eq!(
        runs(
            &assistant
                .process_frame(next(&mut worker).await, Direction::Downstream)
                .unwrap()
        ),
        1,
        "rejected batch must not contaminate valid completion"
    );
    assert!(!assistant.has_function_calls_in_progress().unwrap());
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
#[tokio::test]
async fn independent_native_batch_cannot_add_phantom_sibling() {
    incompatible(Some(batch(&["a", "b", "c"]))).await;
}
#[tokio::test]
async fn independent_native_batch_cannot_drop_original_sibling() {
    incompatible(Some(batch(&["b"]))).await;
}
#[tokio::test]
async fn independent_native_batch_cannot_replace_original_sibling() {
    incompatible(Some(batch(&["b", "c"]))).await;
}
#[tokio::test]
async fn independent_native_sibling_cannot_erase_batch_evidence() {
    incompatible(None).await;
}
#[tokio::test]
async fn independent_disjoint_batch_cannot_reuse_active_group_id() {
    incompatible_for("c", Some(batch(&["c", "d"]))).await;
}
#[test]
fn independent_native_batch_rejects_duplicate_and_oversized_membership() {
    assert!(
        NativeToolCallBatch::new("g".into(), vec![(call("a"), true), (call("a"), false)]).is_err()
    );
    assert!(NativeToolCallBatch::new(
        "g".into(),
        (0..=NativeToolCallBatch::MAX_MEMBERS)
            .map(|n| (call(&n.to_string()), true))
            .collect()
    )
    .is_err());
    assert!(NativeToolCallBatch::new(
        "g".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES),
        vec![(call("a"), true)]
    )
    .is_err());
}

#[tokio::test]
async fn independent_signed_declaration_survives_end_before_native_terminal() {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let context = SharedContext::new(Context::default());
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let attach = |data, history| {
        let mut frame = Frame::new(data);
        frame
            .set_native_llm_continuation(Arc::new(history))
            .unwrap();
        frame
    };
    assistant
        .process_frame(
            attach(
                FrameData::LlmResponseStart,
                NativeLlmContinuation::start(919, LlmContinuationProvider::Anthropic).unwrap(),
            ),
            Direction::Downstream,
        )
        .unwrap();
    let function = call("signed-a");
    let history = NativeLlmContinuation::new(
        919,
        LlmContinuationProvider::Anthropic,
        vec![
            LlmContinuationBlock::Thinking {
                text: "private reference".into(),
                signature: "opaque-exact+/==".into(),
            },
            LlmContinuationBlock::ToolCall(function.as_ref().clone()),
        ],
    )
    .unwrap();
    assistant
        .process_frame(
            attach(
                FrameData::FunctionCallsStarted(vec![function.as_ref().clone()]),
                history,
            ),
            Direction::Downstream,
        )
        .unwrap();
    // InProgress is missing; the signed response already materializes the call.
    assistant
        .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .unwrap();
    ctx.tool_outcome_publisher(function, true)
        .unwrap()
        .complete(
            Ok(json!("settled")),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
    let output = assistant
        .process_frame(next(&mut worker).await, Direction::Downstream)
        .expect("signed declaration authority must survive response end until native settlement");
    assert_eq!(runs(&output), 1);
    assert!(output.actions.iter().any(|action| matches!(action, AggregationAction::Push(frame)
        if frame.direction == Direction::Upstream && matches!(frame.frame.data, FrameData::LlmContextDetailed(_)))),
        "signed history must use the lossless detailed context representation");
    let snapshot = context.snapshot().unwrap();
    assert_eq!(
        snapshot
            .messages
            .iter()
            .flat_map(|m| &m.tool_calls)
            .filter(|call| call.id == "signed-a")
            .count(),
        1
    );
    let body = pipecat_services::adapters::request_body(
        pipecat_services::Provider::Anthropic,
        &pipecat_services::CompletionRequest::new("fixture", snapshot),
    )
    .unwrap();
    assert!(
        body.to_string().contains("opaque-exact+/=="),
        "native reconstruction erased signed reasoning"
    );
    assert!(body.to_string().contains("settled"));
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
