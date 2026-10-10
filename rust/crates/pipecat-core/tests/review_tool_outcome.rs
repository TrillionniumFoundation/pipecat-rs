//! Independent outcome admission/provenance tests, separate from executor callbacks.
use pipecat_core::*;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;

fn call() -> FunctionCall {
    FunctionCall {
        function_name: "external".into(),
        tool_call_id: "identity".into(),
        arguments: json!({"account": "opaque"}),
    }
}
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("review outcome operation did not settle")
}
#[derive(Default)]
struct ContextCapture {
    context: Mutex<Option<ProcessorContext>>,
    captured: Notify,
}
impl ContextCapture {
    fn context(&self) -> ProcessorContext {
        self.context.lock().unwrap().as_ref().unwrap().clone()
    }
}
#[async_trait]
impl FrameProcessor for ContextCapture {
    fn name(&self) -> &str {
        "ReviewOutcomeContext"
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
        if matches!(frame.data, FrameData::Text(_)) {
            *self.context.lock().unwrap() = Some(ctx.clone());
            self.captured.notify_one();
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}

#[tokio::test]
async fn review_full_boundary_is_undelivered_and_fails_closed() {
    let capture = Arc::new(ContextCapture::default());
    let worker = Pipeline::new(vec![capture.clone()])
        .start(PipelineConfig {
            output_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    // Start occupies the sole output slot. Publishing must not call this delivery.
    let receipt = capture
        .context()
        .tool_outcome_publisher(call(), true)
        .unwrap()
        .cancel(
            ToolCallEffectStatus::NotStarted,
            ToolCallEndReason::TaskDropped,
            false,
        );
    assert!(
        matches!(receipt.admission, ToolOutcomeAdmission::Undelivered(_)),
        "{receipt:?}"
    );
    assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Cancelled);
    assert!(matches!(
        bounded(worker.collect()).await,
        Err(PipelineError::Backpressure(_))
    ));
}

struct SystemGate {
    entered: Notify,
    release: Notify,
}
#[async_trait]
impl FrameProcessor for SystemGate {
    fn name(&self) -> &str {
        "ReviewOutcomeSystemGate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::InputText(_)) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn review_full_adjacent_system_lane_is_undelivered_without_drop_spawn() {
    let capture = Arc::new(ContextCapture::default());
    let gate = Arc::new(SystemGate {
        entered: Notify::new(),
        release: Notify::new(),
    });
    let worker = Pipeline::new(vec![capture.clone(), gate.clone()])
        .start(PipelineConfig {
            system_queue_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    let ctx = capture.context();
    ctx.push(
        Frame::new(FrameData::InputText(TextFrame::new("held"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    bounded(gate.entered.notified()).await;
    ctx.push(
        Frame::new(FrameData::InputText(TextFrame::new("queued"))),
        Direction::Downstream,
    )
    .await
    .unwrap();
    let receipt = ctx.tool_outcome_publisher(call(), true).unwrap().cancel(
        ToolCallEffectStatus::NotStarted,
        ToolCallEndReason::TaskDropped,
        false,
    );
    assert!(
        matches!(receipt.admission, ToolOutcomeAdmission::Undelivered(_)),
        "{receipt:?}"
    );
    assert!(matches!(
        bounded(worker.collect()).await,
        Err(PipelineError::Backpressure(_))
    ));
}

#[tokio::test]
async fn review_stale_completed_result_is_cancelled_and_native_identity_is_sealed() {
    let capture = Arc::new(ContextCapture::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("capture invocation"), Direction::Downstream)
        .await
        .unwrap();
    bounded(capture.captured.notified()).await;
    let ctx = capture.context();
    let publisher = ctx.tool_outcome_publisher(call(), true).unwrap();
    ctx.broadcast_interruption().await.unwrap();
    let receipt = publisher.complete(
        Ok(json!("effect already returned")),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    );
    assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
    assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(receipt.outcome.reason(), ToolCallEndReason::StaleResult);
    assert_eq!(
        receipt.outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    let mut terminal = bounded(async {
        loop {
            let item = worker.recv().await.unwrap();
            if matches!(item.frame.data, FrameData::FunctionCallCancel(_)) {
                break item.frame;
            }
        }
    })
    .await;
    assert!(!terminal.interruptible);
    assert_eq!(
        terminal.native_tool_outcome().unwrap().unwrap(),
        &receipt.outcome
    );
    assert_eq!(
        terminal.duplicate().native_tool_outcome().unwrap().unwrap(),
        &receipt.outcome
    );
    assert!(
        serde_json::to_value(&terminal).is_err(),
        "wire encoding must not silently erase local outcome semantics"
    );
    let mut changed = terminal.clone();
    if let FrameData::FunctionCallCancel(cancel) = &mut changed.data {
        cancel.function_name = "different".into();
    }
    assert!(
        changed.native_tool_outcome().is_err(),
        "mutated payload must not inherit trusted identity"
    );
    terminal.take_native_tool_outcome().unwrap().unwrap();
    terminal.metadata.insert(
        "native_tool_outcome".into(),
        json!({"state":"Completed", "cancel_on_interruption":false}),
    );
    let decoded: Frame = serde_json::from_value(serde_json::to_value(&terminal).unwrap()).unwrap();
    assert!(
        decoded.native_tool_outcome().unwrap().is_none(),
        "metadata is not native outcome provenance"
    );
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
}

#[tokio::test]
async fn review_closed_runtime_retains_explicit_unadmitted_receipt() {
    let capture = Arc::new(ContextCapture::default());
    let worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let publisher = capture
        .context()
        .tool_outcome_publisher(call(), false)
        .unwrap();
    worker
        .cancel("closed before callback settlement")
        .await
        .unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    let receipt = publisher.cancel(
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::TaskDropped,
        false,
    );
    assert_eq!(receipt.admission, ToolOutcomeAdmission::Closed);
    assert_eq!(
        receipt.outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!receipt.outcome.cancel_on_interruption());
}
