//! Outcome scope and post-admission survival probes.
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
        tool_call_id: "scoped".into(),
        arguments: json!({}),
    }
}
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("review scope operation did not settle")
}
#[derive(Default)]
struct Capture {
    saved: Mutex<Option<ProcessorContext>>,
    entered: Notify,
}
impl Capture {
    fn context(&self) -> ProcessorContext {
        self.saved.lock().unwrap().as_ref().unwrap().clone()
    }
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "ReviewScopeCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.saved.lock().unwrap() = Some(ctx.clone());
            self.entered.notify_one();
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
async fn capture(worker: &PipelineWorker, processor: &Capture) {
    worker
        .sender()
        .send(Frame::text("capture"), Direction::Downstream)
        .await
        .unwrap();
    bounded(processor.entered.notified()).await;
}
#[tokio::test]
async fn review_inherited_origin_fences_interruptible_result_but_async_policy_survives() {
    for interruptible in [true, false] {
        let outer = Arc::new(Capture::default());
        let inner = Arc::new(Capture::default());
        let outer_worker = Pipeline::new(vec![outer.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let inner_worker = Pipeline::new(vec![inner.clone()])
            .start(Default::default())
            .await
            .unwrap();
        capture(&outer_worker, &outer).await;
        capture(&inner_worker, &inner).await;
        let origin = outer.context();
        let linked = inner.context().forwarding_context(&origin);
        let publisher = linked
            .tool_outcome_publisher(call(), interruptible)
            .unwrap();
        origin.broadcast_interruption().await.unwrap();
        let receipt = publisher.complete(
            Ok(json!("returned")),
            ToolCallEffectStatus::ExternalEffectsUnknown,
            ToolCallEndReason::CallbackReturned,
        );
        assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
        assert_eq!(
            receipt.outcome.state(),
            if interruptible {
                ToolCallTerminalState::Cancelled
            } else {
                ToolCallTerminalState::Completed
            }
        );
        assert_eq!(
            receipt.outcome.reason(),
            if interruptible {
                ToolCallEndReason::StaleResult
            } else {
                ToolCallEndReason::CallbackReturned
            }
        );
        outer_worker.end().await.unwrap();
        inner_worker.end().await.unwrap();
        bounded(outer_worker.collect()).await.unwrap();
        let (frames, _) = bounded(inner_worker.collect()).await.unwrap();
        let outcomes: Vec<_> = frames
            .iter()
            .filter_map(|frame| frame.frame.native_tool_outcome().unwrap())
            .collect();
        assert_eq!(outcomes, [&receipt.outcome]);
    }
}
struct NormalGate {
    entered: Notify,
}
#[async_trait]
impl FrameProcessor for NormalGate {
    fn name(&self) -> &str {
        "ReviewOutcomeNormalGate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            self.entered.notify_one();
            std::future::pending::<()>().await;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn review_admitted_result_survives_later_interruption_without_second_terminal() {
    let source = Arc::new(Capture::default());
    let sink = Arc::new(NormalGate {
        entered: Notify::new(),
    });
    let worker = Pipeline::new(vec![source.clone(), sink.clone()])
        .start(Default::default())
        .await
        .unwrap();
    capture(&worker, &source).await;
    let ctx = source.context();
    ctx.push(Frame::text("held normal lane"), Direction::Downstream)
        .await
        .unwrap();
    bounded(sink.entered.notified()).await;
    let receipt = ctx.tool_outcome_publisher(call(), true).unwrap().complete(
        Ok(json!("finished before interruption")),
        ToolCallEffectStatus::ExternalEffectsUnknown,
        ToolCallEndReason::CallbackReturned,
    );
    assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
    ctx.broadcast_interruption().await.unwrap();
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty());
    let terminals: Vec<_> = frames
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_)
            )
        })
        .collect();
    assert_eq!(terminals.len(), 1);
    assert!(matches!(
        terminals[0].frame.data,
        FrameData::FunctionCallResult(_)
    ));
    assert_eq!(
        terminals[0].frame.native_tool_outcome().unwrap().unwrap(),
        &receipt.outcome
    );
}
