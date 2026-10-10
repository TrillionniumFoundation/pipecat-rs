//! Independent journal bounds, lifecycle and runtime-identity probes.
use pipecat_core::*;
use pipecat_processors::tools::*;
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
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
        .expect("review journal operation did not settle")
}
async fn send(worker: &PipelineWorker, calls: Vec<FunctionCall>) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(calls)),
            Direction::Downstream,
        )
        .await
        .unwrap();
}

#[tokio::test]
async fn review_closed_callback_has_one_inspectable_terminal_and_no_replay() {
    let entered = Arc::new(Notify::new());
    let effects = Arc::new(AtomicUsize::new(0));
    let (notify, count) = (entered.clone(), effects.clone());
    let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        count.fetch_add(1, Ordering::SeqCst);
        notify.notify_one();
        Box::pin(std::future::pending())
    }));
    tool.cancel_on_interruption = false;
    executor.register("external", tool).unwrap();
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, vec![call("closed", "external")]).await;
    bounded(entered.notified()).await;
    assert_eq!(journal.snapshot().active, [call("closed", "external")]);
    worker.cancel("shutdown owns callback").await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    let snapshot = journal.snapshot();
    assert!(snapshot.active.is_empty());
    assert_eq!(snapshot.total_terminal, 1);
    assert_eq!(snapshot.terminal.len(), 1);
    let receipt = &snapshot.terminal[0];
    assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(
        receipt.outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!receipt.outcome.cancel_on_interruption());
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(
        snapshot.unadmitted_terminal,
        u64::from(receipt.admission != ToolOutcomeAdmission::Admitted)
    );
}

#[tokio::test]
async fn review_terminal_retention_and_reservation_slots_are_bounded_and_reusable() {
    let executor = ToolExecutor::new(1, 2, 1024).unwrap();
    let journal = executor.outcomes();
    let mut worker = Pipeline::new(vec![Arc::new(executor)])
        .start(Default::default())
        .await
        .unwrap();
    for i in 0..16 {
        send(
            &worker,
            vec![call(&format!("unknown-{i}"), "not_registered")],
        )
        .await;
        bounded(async {
            loop {
                if matches!(
                    worker.recv().await.unwrap().frame.data,
                    FrameData::FunctionCallResult(_)
                ) {
                    break;
                }
            }
        })
        .await;
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty());
    let snapshot = journal.snapshot();
    assert!(snapshot.active.is_empty());
    assert_eq!(snapshot.total_terminal, 16);
    assert_eq!(snapshot.terminal.len(), 2);
    assert_eq!(snapshot.evicted_terminal, 14);
    assert_eq!(snapshot.unadmitted_terminal, 0);
    for receipt in snapshot.terminal {
        assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Failed);
        assert_eq!(receipt.outcome.reason(), ToolCallEndReason::NotRegistered);
        assert_eq!(receipt.outcome.effects(), ToolCallEffectStatus::NotStarted);
    }
}

#[tokio::test]
async fn review_invalid_batch_publishes_no_partial_identity_or_effect() {
    let effects = Arc::new(AtomicUsize::new(0));
    let counter = effects.clone();
    let mut executor = ToolExecutor::new(2, 4, 32).unwrap();
    executor
        .register(
            "external",
            RegisteredTool::new(Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!(true)) })
            })),
        )
        .unwrap();
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(Default::default())
        .await
        .unwrap();
    send(
        &worker,
        vec![call("valid", "external"), call(&"x".repeat(33), "external")],
    )
    .await;
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert!(!frames.iter().any(|frame| matches!(
        frame.frame.data,
        FrameData::FunctionCallsStarted(_)
            | FrameData::FunctionCallInProgress(_)
            | FrameData::FunctionCallResult(_)
            | FrameData::FunctionCallCancel(_)
    )));
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    let snapshot = journal.snapshot();
    assert!(snapshot.active.is_empty() && snapshot.terminal.is_empty());
    assert_eq!(snapshot.total_terminal, 0);
}

struct DropSignal(Arc<Notify>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
#[tokio::test]
async fn review_shared_executor_cancel_uses_origin_runtime_not_numeric_task_id() {
    let entered = Arc::new(Notify::new());
    let count = Arc::new(AtomicUsize::new(0));
    let dropped_a = Arc::new(Notify::new());
    let release_b = Arc::new(Notify::new());
    let (notify, effects, dropped, release) = (
        entered.clone(),
        count.clone(),
        dropped_a.clone(),
        release_b.clone(),
    );
    let mut executor = ToolExecutor::new(2, 8, 1024).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |call| {
        let (notify, effects, dropped, release) = (
            notify.clone(),
            effects.clone(),
            dropped.clone(),
            release.clone(),
        );
        Box::pin(async move {
            let _drop = (call.tool_call_id == "a").then(|| DropSignal(dropped));
            effects.fetch_add(1, Ordering::SeqCst);
            notify.notify_one();
            if call.tool_call_id == "a" {
                std::future::pending::<()>().await;
            } else {
                release.notified().await;
            }
            Ok(json!("finished"))
        })
    }));
    tool.cancel_on_interruption = false;
    executor.register("external", tool).unwrap();
    let journal = executor.outcomes();
    let shared = Arc::new(executor);
    let first = Pipeline::new(vec![shared.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let second = Pipeline::new(vec![shared])
        .start(Default::default())
        .await
        .unwrap();
    send(&first, vec![call("a", "external")]).await;
    send(&second, vec![call("b", "external")]).await;
    bounded(async {
        while count.load(Ordering::SeqCst) != 2 {
            entered.notified().await;
        }
    })
    .await;
    second
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "external".into(),
                tool_call_id: "a".into(),
                run_llm: false,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    bounded(dropped_a.notified()).await;
    release_b.notify_one();
    first.end().await.unwrap();
    second.end().await.unwrap();
    let (_, report_a) = bounded(first.collect()).await.unwrap();
    let (_, report_b) = bounded(second.collect()).await.unwrap();
    assert!(report_a.errors.is_empty() && report_b.errors.is_empty());
    let snapshot = journal.snapshot();
    assert_eq!(snapshot.total_terminal, 2);
    assert!(snapshot.active.is_empty());
    assert_eq!(
        snapshot
            .terminal
            .iter()
            .find(|r| r.outcome.call().tool_call_id == "a")
            .unwrap()
            .outcome
            .state(),
        ToolCallTerminalState::Cancelled
    );
    assert_eq!(
        snapshot
            .terminal
            .iter()
            .find(|r| r.outcome.call().tool_call_id == "b")
            .unwrap()
            .outcome
            .state(),
        ToolCallTerminalState::Completed
    );
}

#[tokio::test]
async fn review_end_and_stop_drain_real_completion_without_cancellation() {
    for terminal in [Frame::end(), Frame::new(FrameData::Stop)] {
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor
            .register(
                "external",
                RegisteredTool::new(Arc::new(|_| Box::pin(async { Ok(json!("done")) }))),
            )
            .unwrap();
        let journal = executor.outcomes();
        let worker = Pipeline::new(vec![Arc::new(executor)])
            .start(Default::default())
            .await
            .unwrap();
        send(&worker, vec![call("drain", "external")]).await;
        worker
            .sender()
            .send(terminal, Direction::Downstream)
            .await
            .unwrap();
        let (_, report) = bounded(worker.collect()).await.unwrap();
        assert!(matches!(
            report.state,
            WorkerState::Ended | WorkerState::Stopped
        ));
        let snapshot = journal.snapshot();
        assert_eq!(snapshot.total_terminal, 1);
        assert!(snapshot.active.is_empty());
        assert_eq!(
            snapshot.terminal[0].outcome.state(),
            ToolCallTerminalState::Completed
        );
        assert_eq!(
            snapshot.terminal[0].admission,
            ToolOutcomeAdmission::Admitted
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn review_result_cancel_race_has_one_terminal_decision_per_call() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (notify, finish) = (entered.clone(), release.clone());
    let effects = Arc::new(AtomicUsize::new(0));
    let counter = effects.clone();
    let mut executor = ToolExecutor::new(1, 64, 1024).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        let (notify, finish, counter) = (notify.clone(), finish.clone(), counter.clone());
        Box::pin(async move {
            counter.fetch_add(1, Ordering::SeqCst);
            notify.notify_one();
            finish.notified().await;
            Ok(json!("returned"))
        })
    }));
    tool.cancel_on_interruption = false;
    executor.register("external", tool).unwrap();
    let journal = executor.outcomes();
    let shared = Arc::new(executor);
    for i in 0..32 {
        let worker = Pipeline::new(vec![shared.clone()])
            .start(Default::default())
            .await
            .unwrap();
        let id = format!("race-{i}");
        send(&worker, vec![call(&id, "external")]).await;
        bounded(entered.notified()).await;
        // Waking the callback races a queued system cancel on another worker.
        release.notify_one();
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "external".into(),
                    tool_call_id: id.clone(),
                    run_llm: false,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker.end().await.unwrap();
        let (frames, report) = bounded(worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{report:?}");
        let terminals: Vec<_> = frames
            .iter()
            .filter(|frame| {
                matches!(
                    frame.frame.data,
                    FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_)
                )
            })
            .collect();
        assert_eq!(
            terminals.len(),
            1,
            "result and cancellation must linearize once for {id}"
        );
        let terminal = terminals[0]
            .frame
            .native_tool_outcome()
            .unwrap()
            .expect("native terminal identity");
        assert_eq!(terminal.call().tool_call_id, id);
        let snapshot = journal.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, i + 1);
        let receipts: Vec<_> = snapshot
            .terminal
            .iter()
            .filter(|receipt| receipt.outcome.call().tool_call_id == id)
            .collect();
        assert_eq!(receipts.len(), 1);
        assert_eq!(&receipts[0].outcome, terminal);
        assert!(matches!(
            terminal.state(),
            ToolCallTerminalState::Completed | ToolCallTerminalState::Cancelled
        ));
        assert_eq!(
            terminal.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
    }
    assert_eq!(effects.load(Ordering::SeqCst), 32);
}

struct NoopObserver;
#[async_trait]
impl PipelineObserver for NoopObserver {
    async fn on_event(&self, _: ObservationEvent) {}
}
#[tokio::test]
async fn review_retained_terminal_journal_and_frames_release_runtime_and_executor() {
    for cancelling in [false, true] {
        let entered = Arc::new(Notify::new());
        let notify = entered.clone();
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor
            .register(
                "external",
                RegisteredTool::new(Arc::new(move |_| {
                    let notify = notify.clone();
                    Box::pin(async move {
                        notify.notify_one();
                        if cancelling {
                            std::future::pending::<()>().await;
                        }
                        Ok(json!("done"))
                    })
                })),
            )
            .unwrap();
        let journal = executor.outcomes();
        let executor = Arc::new(executor);
        let weak_executor = Arc::downgrade(&executor);
        let observer = Arc::new(NoopObserver);
        let weak_observer = Arc::downgrade(&observer);
        let worker = Pipeline::new(vec![executor.clone()])
            .with_observer(observer.clone(), Default::default())
            .start(Default::default())
            .await
            .unwrap();
        drop(executor);
        drop(observer);
        send(&worker, vec![call("release", "external")]).await;
        bounded(entered.notified()).await;
        if cancelling {
            worker.cancel("release route ownership").await.unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let (frames, report) = bounded(worker.collect()).await.unwrap();
        assert!(report.errors.is_empty());
        let snapshot = journal.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, 1);
        assert!(
            weak_executor.upgrade().is_none(),
            "terminal journal retained executor ownership"
        );
        assert!(
            weak_observer.upgrade().is_none(),
            "terminal journal or frame retained runtime/observer ownership"
        );
        // Keep both the externally held journal and returned native frames live
        // through the weak-reference checks above.
        assert!(!frames.is_empty());
        assert_eq!(journal.snapshot().terminal.len(), 1);
    }
}
