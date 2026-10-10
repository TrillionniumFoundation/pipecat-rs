//! Independent aggregate journal budget probes through public processor APIs.
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
const MIB: usize = 1024 * 1024;
fn call(id: &str, bytes: usize) -> FunctionCall {
    FunctionCall {
        function_name: "external".into(),
        tool_call_id: id.into(),
        arguments: json!({"payload": "x".repeat(bytes)}),
    }
}
async fn bounded<F: std::future::Future>(future: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(8), future)
        .await
        .expect("review budget operation did not settle")
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
async fn review_terminal_payload_byte_eviction_is_visible_and_releases_budget() {
    let executor = ToolExecutor::new(1, 64, 2 * MIB).unwrap();
    let journal = executor.outcomes();
    let mut worker = Pipeline::new(vec![Arc::new(executor)])
        .start(Default::default())
        .await
        .unwrap();
    for n in 0..12 {
        send(&worker, vec![call(&format!("terminal-{n}"), MIB)]).await;
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
    assert!(report.errors.is_empty(), "{report:?}");
    let snapshot = journal.snapshot();
    assert!(snapshot.active.is_empty());
    assert_eq!(
        snapshot.payload_byte_limit,
        ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES
    );
    assert_eq!(snapshot.total_terminal, 12);
    assert!(
        snapshot.evicted_terminal > 0,
        "byte budget must evict before 64-entry count bound"
    );
    assert_eq!(
        snapshot.terminal.len() as u64 + snapshot.evicted_terminal,
        12
    );
    assert!(snapshot.retained_payload_bytes <= snapshot.payload_byte_limit);
    let exact_bytes: usize = snapshot
        .terminal
        .iter()
        .map(|receipt| receipt.outcome.retained_bytes())
        .sum();
    assert_eq!(snapshot.retained_payload_bytes, exact_bytes);
    assert_eq!(snapshot.unadmitted_terminal, 0);
}
struct RejectCapture {
    executor: ToolExecutor,
    rejected: Notify,
}
#[async_trait]
impl FrameProcessor for RejectCapture {
    fn name(&self) -> &str {
        "ReviewBudgetRejectCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let third = matches!(&frame.data, FrameData::FunctionCallsStarted(calls) if calls.iter().any(|call| call.tool_call_id == "rejected"));
        let result = self.executor.process(frame, direction, ctx).await;
        if third {
            assert!(
                result.is_err(),
                "active journal budget must reject before concurrency dispatch"
            );
            self.rejected.notify_one();
            Ok(())
        } else {
            result
        }
    }
}
#[tokio::test]
async fn review_active_payload_budget_rejects_atomically_without_evicting_active_calls() {
    let effects = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let (counter, notify) = (effects.clone(), entered.clone());
    let mut executor = ToolExecutor::new(2, 32, 4 * MIB).unwrap();
    executor
        .register(
            "external",
            RegisteredTool::new(Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                notify.notify_one();
                Box::pin(std::future::pending())
            })),
        )
        .unwrap();
    let journal = executor.outcomes();
    let wrapper = Arc::new(RejectCapture {
        executor,
        rejected: Notify::new(),
    });
    let worker = Pipeline::new(vec![wrapper.clone()])
        .start(Default::default())
        .await
        .unwrap();
    send(&worker, vec![call("active-a", MIB), call("active-b", MIB)]).await;
    bounded(async {
        while effects.load(Ordering::SeqCst) != 2 {
            entered.notified().await;
        }
    })
    .await;
    let before = journal.snapshot();
    assert_eq!(before.active.len(), 2);
    assert_eq!(before.total_terminal, 0);
    send(&worker, vec![call("rejected", 3 * MIB)]).await;
    bounded(wrapper.rejected.notified()).await;
    let after = journal.snapshot();
    assert_eq!(
        after.active, before.active,
        "an active receipt must never be evicted to admit another call"
    );
    assert_eq!(after.retained_payload_bytes, before.retained_payload_bytes);
    assert_eq!(
        after.total_terminal, 0,
        "rejected identity must not partially publish"
    );
    assert_eq!(effects.load(Ordering::SeqCst), 2);
    worker.cancel("release active budget").await.unwrap();
    bounded(worker.collect()).await.unwrap();
    let closed = journal.snapshot();
    assert!(closed.active.is_empty());
    assert_eq!(closed.total_terminal, 2);
    assert!(closed.retained_payload_bytes <= closed.payload_byte_limit);
}
