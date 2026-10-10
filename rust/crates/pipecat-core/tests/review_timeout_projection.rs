//! Independent validation of the sole admitted native outcome projection.
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
        "IndependentTimeoutCapture"
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
async fn terminal(failed: bool, reason: ToolCallEndReason) -> Frame {
    let capture = Arc::new(Capture::default());
    let weak = Arc::downgrade(&capture);
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = capture.0.lock().unwrap().take().unwrap();
    let receipt = ctx
        .tool_outcome_publisher(
            FunctionCall {
                function_name: "external".into(),
                tool_call_id: "timeout".into(),
                arguments: json!({"value":7}),
            },
            true,
        )
        .unwrap()
        .complete(
            if failed {
                Err("tool execution timed out".into())
            } else {
                Ok(json!("success"))
            },
            ToolCallEffectStatus::ExternalEffectsUnknown,
            reason,
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
    assert!(
        weak.upgrade().is_none(),
        "receipt retained pipeline processors/runtime after collect"
    );
    result
}
#[tokio::test]
async fn independent_timeout_projection_retains_original_arc_and_blocks_wire() {
    let mut frame = terminal(true, ToolCallEndReason::TimedOut).await;
    let original = frame.native_tool_outcome().unwrap().unwrap().clone();
    frame.project_native_tool_timeout_as_cancel().unwrap();
    let projected = frame.native_tool_outcome().unwrap().unwrap();
    assert!(
        Arc::ptr_eq(&original, projected),
        "projection replaced immutable native outcome"
    );
    assert_eq!(projected.state(), ToolCallTerminalState::Failed);
    assert_eq!(projected.reason(), ToolCallEndReason::TimedOut);
    assert_eq!(
        projected.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    let FrameData::FunctionCallCancel(cancel) = &frame.data else {
        panic!("timeout projection was not cancellation")
    };
    assert_eq!(cancel.tool_call_id, "timeout");
    assert_eq!(cancel.function_name, "external");
    assert!(cancel.run_llm);
    assert!(serde_json::to_value(&frame).is_err());
    assert!(serde_json::to_value(frame.duplicate()).is_err());
    assert!(Arc::ptr_eq(
        &frame.take_native_tool_outcome().unwrap().unwrap(),
        &original
    ));
    assert!(serde_json::to_value(frame).is_ok());
}
#[tokio::test]
async fn independent_timeout_projection_rejects_forged_identity_and_policy() {
    let mut valid = terminal(true, ToolCallEndReason::TimedOut).await;
    valid.project_native_tool_timeout_as_cancel().unwrap();
    for field in 0..3 {
        let mut altered = valid.duplicate();
        let FrameData::FunctionCallCancel(cancel) = &mut altered.data else {
            unreachable!()
        };
        match field {
            0 => cancel.function_name = "forged".into(),
            1 => cancel.tool_call_id = "forged".into(),
            _ => cancel.run_llm = false,
        }
        assert!(
            altered.native_tool_outcome().is_err(),
            "projected mutation {field} accepted"
        );
        assert!(altered.take_native_tool_outcome().is_err());
        assert!(serde_json::to_value(altered).is_err());
    }
}
#[tokio::test]
async fn independent_public_payload_cannot_forge_timeout_projection() {
    let mut raw = terminal(true, ToolCallEndReason::TimedOut).await;
    raw.data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
        function_name: "external".into(),
        tool_call_id: "timeout".into(),
        run_llm: true,
    });
    raw.metadata
        .insert("native_tool_timeout_projection".into(), json!(true));
    assert!(raw.native_tool_outcome().is_err());
    assert!(raw.project_native_tool_timeout_as_cancel().is_err());
}
#[tokio::test]
async fn independent_non_timeout_or_success_cannot_project() {
    for (failed, reason) in [
        (true, ToolCallEndReason::CallbackError),
        (false, ToolCallEndReason::TimedOut),
        (false, ToolCallEndReason::CallbackReturned),
    ] {
        let mut frame = terminal(failed, reason).await;
        assert!(
            frame.project_native_tool_timeout_as_cancel().is_err(),
            "incorrect state/reason accepted: {failed:?}/{reason:?}"
        );
        assert!(frame.native_tool_outcome().is_ok());
    }
}
#[tokio::test]
async fn independent_altered_error_cannot_be_laundered_through_projection() {
    let mut frame = terminal(true, ToolCallEndReason::TimedOut).await;
    if let FrameData::FunctionCallResult(result) = &mut frame.data {
        result.error = Some("forged".into());
    }
    assert!(frame.project_native_tool_timeout_as_cancel().is_err());
}
