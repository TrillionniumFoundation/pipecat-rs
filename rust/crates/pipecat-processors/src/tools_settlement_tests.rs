use super::*;
use pipecat_core::{Pipeline, StartFrame, ToolCallTerminalState};
use serde_json::json;
struct Capture(Arc<Mutex<Option<ProcessorContext>>>);
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "SettlementReservationCapture"
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
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "local".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
}
#[tokio::test]
async fn complete_batch_reservation_failure_has_no_partial_identity_and_releases_slots() {
    let captured = Arc::new(Mutex::new(None));
    let worker = Pipeline::new(vec![Arc::new(Capture(captured.clone()))])
        .start(Default::default())
        .await
        .unwrap();
    let ctx = captured.lock().unwrap().as_ref().unwrap().clone();
    let executor = ToolExecutor::new(2, 8, 128).unwrap();
    let first = executor
        .reserve(&[call("a"), call("b")], None, "fixture-batch", &ctx)
        .unwrap();
    let second = executor
        .reserve(&[call("c")], None, "fixture-batch", &ctx)
        .unwrap();
    assert!(executor
        .reserve(&[call("d"), call("e")], None, "fixture-batch", &ctx)
        .is_err());
    {
        let seen = executor.seen.lock().unwrap();
        assert_eq!(
            seen.active.keys().cloned().collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
        assert_eq!(
            seen.recent.iter().cloned().collect::<Vec<_>>(),
            ["a", "b", "c"]
        );
    }
    assert_eq!(executor.settlement_slots.available_permits(), 1);
    drop(first);
    assert_eq!(executor.settlement_slots.available_permits(), 3);
    let retried = executor
        .reserve(&[call("d"), call("e")], None, "fixture-batch", &ctx)
        .unwrap();
    drop(second);
    drop(retried);
    assert_eq!(executor.settlement_slots.available_permits(), 4);
    let snapshot = executor.outcomes().snapshot();
    assert!(snapshot.active.is_empty());
    assert_eq!(snapshot.total_terminal, 5);
    assert!(snapshot
        .terminal
        .iter()
        .all(
            |receipt| receipt.outcome.state() == ToolCallTerminalState::Cancelled
                && receipt.outcome.effects() == ToolCallEffectStatus::NotStarted
        ));
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
}
