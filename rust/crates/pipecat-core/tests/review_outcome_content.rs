//! Independent receipt integrity probes, using only public native publication.
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
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    fn name(&self) -> &str {
        "IndependentReceiptCapture"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
}
async fn terminal(failed: bool) -> Frame {
    let capture = Arc::new(Capture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let receipt = ctx
        .tool_outcome_publisher(
            FunctionCall {
                function_name: "write".into(),
                tool_call_id: "receipt".into(),
                arguments: json!({"value":7}),
            },
            true,
        )
        .unwrap()
        .complete(
            if failed {
                Err("the original error".into())
            } else {
                Ok(json!({"saved":true}))
            },
            ToolCallEffectStatus::ExternalEffectsUnknown,
            if failed {
                ToolCallEndReason::CallbackError
            } else {
                ToolCallEndReason::CallbackReturned
            },
        );
    assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let item = worker.recv().await.unwrap();
            if matches!(item.frame.data, FrameData::FunctionCallResult(_)) {
                break item.frame;
            }
        }
    })
    .await
    .unwrap();
    drop(ctx);
    drop(capture);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    result
}
#[tokio::test]
async fn independent_completed_payload_mutation_cannot_reuse_receipt() {
    let mut frame = terminal(false).await;
    if let FrameData::FunctionCallResult(result) = &mut frame.data {
        result.result = json!({"saved":false,"forged":true});
    }
    assert!(
        frame.native_tool_outcome().is_err(),
        "receipt accepted altered success content"
    );
    assert!(frame.take_native_tool_outcome().is_err());
}
#[tokio::test]
async fn independent_failed_error_text_mutation_cannot_reuse_receipt() {
    let mut frame = terminal(true).await;
    if let FrameData::FunctionCallResult(result) = &mut frame.data {
        result.error = Some("different failure".into());
    }
    assert!(
        frame.native_tool_outcome().is_err(),
        "receipt accepted altered failure content"
    );
}
#[tokio::test]
async fn independent_failed_payload_mutation_cannot_reuse_receipt() {
    let mut frame = terminal(true).await;
    if let FrameData::FunctionCallResult(result) = &mut frame.data {
        result.result = json!({"error":"different failure"});
    }
    assert!(
        frame.native_tool_outcome().is_err(),
        "receipt accepted altered failure payload"
    );
}
#[tokio::test]
async fn independent_result_policy_mutation_requires_validated_override() {
    let mut frame = terminal(false).await;
    if let FrameData::FunctionCallResult(result) = &mut frame.data {
        result.run_llm = Some(false);
    }
    assert!(
        frame.native_tool_outcome().is_err(),
        "raw policy mutation accepted"
    );
}
#[tokio::test]
async fn independent_native_content_is_not_serializable_until_consumed() {
    let mut frame = terminal(false).await;
    let outcome = frame.native_tool_outcome().unwrap().unwrap().clone();
    assert!(serde_json::to_value(&frame).is_err());
    assert_eq!(frame.take_native_tool_outcome().unwrap().unwrap(), outcome);
    let wire = serde_json::to_value(&frame).unwrap();
    let decoded: Frame = serde_json::from_value(wire).unwrap();
    assert!(decoded.native_tool_outcome().unwrap().is_none());
}
