use pipecat_core::*;
use pipecat_processors::tools::*;
use serde_json::json;
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::Notify;
fn call(id: &str, name: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
}
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("bounded tool settlement")
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
#[tokio::test]
async fn full_boundary_records_undelivered_cancellation_and_fails_closed() {
    bounded(async {
        let entered = Arc::new(Notify::new());
        let mut tools = ToolExecutor::new(1, 4, 128).unwrap();
        tools
            .register(
                "hang",
                RegisteredTool::new(Arc::new({
                    let entered = entered.clone();
                    move |_| {
                        entered.notify_one();
                        Box::pin(std::future::pending())
                    }
                })),
            )
            .unwrap();
        let journal = tools.outcomes();
        let mut worker = Pipeline::new(vec![Arc::new(tools)])
            .start(PipelineConfig {
                output_capacity: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        worker.recv().await.unwrap();
        send(
            &worker,
            FrameData::FunctionCallsStarted(vec![call("full", "hang")]),
        )
        .await;
        entered.notified().await;
        assert_eq!(journal.snapshot().active.len(), 1);
        // The declaration and placeholder fill the boundary. This command is
        // consumed, so the terminal acknowledgment itself exercises pressure.
        send(
            &worker,
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "hang".into(),
                tool_call_id: "full".into(),
                run_llm: false,
            }),
        )
        .await;
        while journal.snapshot().total_terminal == 0 {
            tokio::task::yield_now().await;
        }
        let snapshot = journal.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, 1);
        assert_eq!(snapshot.unadmitted_terminal, 1);
        assert!(matches!(
            snapshot.terminal[0].admission,
            ToolOutcomeAdmission::Undelivered(_)
        ));
        assert_eq!(
            snapshot.terminal[0].outcome.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
        assert_eq!(
            snapshot.terminal[0].outcome.state(),
            ToolCallTerminalState::Cancelled
        );
        assert!(worker.collect().await.is_err());
    })
    .await;
}
struct SavePublisher(Arc<Mutex<Option<ToolOutcomePublisher>>>);
#[async_trait]
impl FrameProcessor for SavePublisher {
    fn name(&self) -> &str {
        "SaveToolPublisher"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::InputText(_)) {
            *self.0.lock().unwrap() =
                Some(ctx.tool_outcome_publisher(call("saved", "local"), true)?);
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test]
async fn closed_runtime_is_explicitly_observable_without_a_detached_task() {
    bounded(async {
        let publisher = Arc::new(Mutex::new(None));
        let worker = Pipeline::new(vec![Arc::new(SavePublisher(publisher.clone()))])
            .start(Default::default())
            .await
            .unwrap();
        send(&worker, FrameData::InputText(TextFrame::new("save"))).await;
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
        let receipt = publisher.lock().unwrap().take().unwrap().cancel(
            ToolCallEffectStatus::NotStarted,
            ToolCallEndReason::TaskDropped,
            false,
        );
        assert_eq!(receipt.admission, ToolOutcomeAdmission::Closed);
        assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Cancelled);
        assert_eq!(receipt.outcome.effects(), ToolCallEffectStatus::NotStarted);
    })
    .await;
}
#[tokio::test]
async fn terminal_journal_eviction_is_counted_and_wire_metadata_cannot_forge_receipts() {
    bounded(async {
        let tools = ToolExecutor::new(1, 2, 128).unwrap();
        let journal = tools.outcomes();
        let mut worker = Pipeline::new(vec![Arc::new(tools)])
            .start(Default::default())
            .await
            .unwrap();
        let mut terminal = None;
        for id in ["one", "two", "three"] {
            send(
                &worker,
                FrameData::FunctionCallsStarted(vec![call(id, "missing")]),
            )
            .await;
            loop {
                let item = worker.recv().await.unwrap();
                if matches!(item.frame.data, FrameData::FunctionCallResult(_)) {
                    terminal = Some(item.frame);
                    break;
                }
            }
        }
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
        let snapshot = journal.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, 3);
        assert_eq!(snapshot.evicted_terminal, 1);
        assert_eq!(snapshot.terminal.len(), 2);
        assert_eq!(snapshot.unadmitted_terminal, 0);
        let mut frame = terminal.unwrap();
        assert!(frame.native_tool_outcome().unwrap().is_some());
        assert!(!frame.interruptible);
        assert!(serde_json::to_vec(&frame).is_err());
        let receipt = frame.take_native_tool_outcome().unwrap().unwrap();
        assert_eq!(receipt.effects(), ToolCallEffectStatus::NotStarted);
        frame
            .metadata
            .insert("native_tool_outcome".into(), json!({"state":"completed"}));
        let wire = serde_json::to_vec(&frame).unwrap();
        let decoded: Frame = serde_json::from_slice(&wire).unwrap();
        assert!(decoded.native_tool_outcome().unwrap().is_none());
    })
    .await;
}
#[tokio::test]
async fn timeout_panic_and_callback_error_are_failed_with_uncertain_external_effects() {
    bounded(async {
        let mut tools = ToolExecutor::new(3, 8, 128).unwrap();
        let mut slow = RegisteredTool::new(Arc::new(|_| Box::pin(std::future::pending())));
        slow.timeout = Duration::from_millis(5);
        tools.register("slow", slow).unwrap();
        tools
            .register(
                "panic",
                RegisteredTool::new(Arc::new(|_| panic!("callback construction panic"))),
            )
            .unwrap();
        tools
            .register(
                "error",
                RegisteredTool::new(Arc::new(|_| {
                    Box::pin(async { Err("external outcome unknown".into()) })
                })),
            )
            .unwrap();
        let journal = tools.outcomes();
        let worker = Pipeline::new(vec![Arc::new(tools)])
            .start(Default::default())
            .await
            .unwrap();
        send(
            &worker,
            FrameData::FunctionCallsStarted(vec![
                call("one", "slow"),
                call("two", "panic"),
                call("three", "error"),
            ]),
        )
        .await;
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(
            frames
                .iter()
                .filter(|item| matches!(item.frame.data, FrameData::FunctionCallResult(_)))
                .count(),
            3
        );
        let snapshot = journal.snapshot();
        assert_eq!(snapshot.total_terminal, 3);
        for receipt in snapshot.terminal {
            assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Failed);
            assert_eq!(
                receipt.outcome.effects(),
                ToolCallEffectStatus::ExternalEffectsUnknown
            );
            assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
        }
    })
    .await;
}
#[tokio::test]
async fn long_identity_is_rejected_before_journal_or_effect_admission() {
    let tools = ToolExecutor::new(1, 4, 16).unwrap();
    let journal = tools.outcomes();
    let worker = Pipeline::new(vec![Arc::new(tools)])
        .start(Default::default())
        .await
        .unwrap();
    send(
        &worker,
        FrameData::FunctionCallsStarted(vec![call(&"x".repeat(17), "missing")]),
    )
    .await;
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert!(journal.snapshot().active.is_empty());
    assert_eq!(journal.snapshot().total_terminal, 0);
}
