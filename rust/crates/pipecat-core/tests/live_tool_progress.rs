//! Nonterminal authority stays local and cannot be reused with changed public data.
use pipecat_core::*;
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
        "progress-context"
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
async fn owner(policy: bool) -> (PipelineWorker, ToolOutcomePublisher) {
    let capture = Arc::new(Capture::default());
    let worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let call = Arc::new(FunctionCall {
        function_name: "echo".into(),
        tool_call_id: "progress".into(),
        arguments: json!({"value":1}),
    });
    let batch = NativeToolCallBatch::new("group".into(), vec![(call.clone(), policy)]).unwrap();
    let publisher = ctx
        .tool_outcome_publisher(call, policy)
        .unwrap()
        .with_batch(batch)
        .unwrap();
    (worker, publisher)
}
async fn receive(worker: &mut PipelineWorker) -> Frame {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let frame = worker.recv().await.unwrap().frame;
            if matches!(frame.data, FrameData::FunctionCallResult(_)) {
                break frame;
            }
        }
    })
    .await
    .unwrap()
}
async fn finish(worker: PipelineWorker) {
    tokio::time::timeout(Duration::from_secs(5), async {
        worker.sender().end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn progress_requires_async_batch_and_rejects_oversize_before_publication() {
    let (worker, publisher) = owner(true).await;
    assert!(publisher.progress_publisher().is_err());
    finish(worker).await;
    let (mut worker, publisher) = owner(false).await;
    let progress = publisher.progress_publisher().unwrap();
    assert!(progress
        .publish_progress(LocalToolResult::new(json!({})), 0)
        .is_err());
    assert!(progress
        .publish_progress(
            LocalToolResult::new(json!("x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES))),
            1
        )
        .is_err());
    assert_eq!(
        progress
            .publish_progress(LocalToolResult::new(json!({"working":true})), 1)
            .unwrap(),
        ToolOutcomeAdmission::Admitted
    );
    let frame = receive(&mut worker).await;
    assert_eq!(frame.native_tool_progress().unwrap().unwrap().sequence(), 1);
    assert!(frame.native_tool_outcome().unwrap().is_none());
    let receipt = publisher.complete(
        Ok(json!({"done":true})),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    );
    assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
    assert!(receive(&mut worker)
        .await
        .native_tool_outcome()
        .unwrap()
        .is_some());
    finish(worker).await;
    assert_eq!(
        progress
            .publish_progress(LocalToolResult::new(json!({})), 2)
            .unwrap(),
        ToolOutcomeAdmission::Closed
    );
}

#[tokio::test]
async fn progress_payload_policy_identity_and_finalness_are_sealed() {
    let (mut worker, publisher) = owner(false).await;
    let progress = publisher.progress_publisher().unwrap();
    progress
        .publish_progress(
            LocalToolResult::new(json!({"working":true})).with_run_llm(false),
            1,
        )
        .unwrap();
    let frame = receive(&mut worker).await;
    let capability = frame.native_tool_progress().unwrap().unwrap().clone();
    assert_eq!(capability.call().arguments, json!({"value":1}));
    assert!(!capability.cancel_on_interruption());
    assert!(capability.retained_bytes() > 0);
    for mutation in 0..7 {
        let mut changed = frame.clone();
        let FrameData::FunctionCallResult(result) = &mut changed.data else {
            unreachable!()
        };
        match mutation {
            0 => result.result = json!({"working":false}),
            1 => result.run_llm = Some(true),
            2 => result.arguments = json!({"value":2}),
            3 => result.function_name = "other".into(),
            4 => result.tool_call_id = "other".into(),
            5 => result.is_final = true,
            _ => result.error = Some("forged".into()),
        }
        assert!(
            changed.native_tool_progress().is_err(),
            "mutation {mutation}"
        );
        assert!(
            changed.take_native_tool_progress().is_err(),
            "mutation {mutation}"
        );
    }
    let mut duplicate = frame.duplicate();
    assert_ne!(duplicate.id, frame.id);
    assert_eq!(
        duplicate.native_tool_progress().unwrap().unwrap().id(),
        capability.id()
    );
    assert!(serde_json::to_value(&duplicate).is_err());
    assert_eq!(
        duplicate.take_native_tool_progress().unwrap().unwrap(),
        capability
    );
    let wire = serde_json::to_value(&duplicate).unwrap();
    let decoded: Frame = serde_json::from_value(wire).unwrap();
    assert!(decoded.native_tool_progress().unwrap().is_none());
    assert!(decoded.native_tool_outcome().unwrap().is_none());
    publisher.cancel(
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::ExplicitCancel,
        false,
    );
    finish(worker).await;
}
