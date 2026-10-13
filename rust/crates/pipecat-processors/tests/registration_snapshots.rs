use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolRegistrationSnapshot};
use serde_json::json;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
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
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("snapshot regression did not settle")
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

struct PublishedRegistrations {
    executor: ToolExecutor,
    current: Arc<RwLock<ToolRegistrationSnapshot>>,
}
#[async_trait]
impl FrameProcessor for PublishedRegistrations {
    fn name(&self) -> &str {
        "PublishedRegistrations"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // A caller owns publication. The executor retains this immutable batch
        // choice without holding the publication lock across any await.
        let registrations = self.current.read().unwrap().clone();
        self.executor
            .process_with_registrations(frame, direction, ctx, &registrations)
            .await
    }
}

#[tokio::test(start_paused = true)]
async fn published_registry_keeps_admitted_callback_policy_and_duplicate_history() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let old_calls = Arc::new(AtomicUsize::new(0));
    let retired_calls = Arc::new(AtomicUsize::new(0));
    let new_calls = Arc::new(AtomicUsize::new(0));
    let added_calls = Arc::new(AtomicUsize::new(0));
    let mut old = ToolExecutor::new(4, 8, 1024).unwrap();
    let old_tool = {
        let entered = entered.clone();
        let release = release.clone();
        let count = old_calls.clone();
        let mut tool = RegisteredTool::new(Arc::new(move |_| {
            count.fetch_add(1, Ordering::SeqCst);
            entered.notify_one();
            let release = release.clone();
            Box::pin(async move {
                release.notified().await;
                Ok(json!("old callback"))
            })
        }));
        tool.cancel_on_interruption = false;
        tool.timeout = Duration::from_secs(10);
        tool
    };
    old.register("shared", old_tool).unwrap();
    let retired_count = retired_calls.clone();
    old.register(
        "retired",
        RegisteredTool::new(Arc::new(move |_| {
            retired_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(json!("retired effect")) })
        })),
    )
    .unwrap();
    let current = Arc::new(RwLock::new(old.registrations_snapshot()));

    let mut new = ToolExecutor::new(4, 8, 1024).unwrap();
    let new_count = new_calls.clone();
    let mut new_tool = RegisteredTool::new(Arc::new(move |_| {
        new_count.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(json!("new callback")) })
    }));
    new_tool.cancel_on_interruption = true;
    new_tool.timeout = Duration::from_millis(100);
    new.register("shared", new_tool).unwrap();
    let added_count = added_calls.clone();
    new.register(
        "added",
        RegisteredTool::new(Arc::new(move |_| {
            added_count.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(json!("added callback")) })
        })),
    )
    .unwrap();

    let executor = ToolExecutor::new(4, 8, 1024).unwrap();
    let journal = executor.outcomes();
    let mut worker = Pipeline::new(vec![Arc::new(PublishedRegistrations {
        executor,
        current: current.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(&worker, vec![call("old", "shared")]).await;
    bounded(entered.notified()).await;
    assert_eq!(old_calls.load(Ordering::SeqCst), 1);

    *current.write().unwrap() = new.registrations_snapshot();
    drop(old);
    drop(new);
    tokio::time::advance(Duration::from_secs(1)).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    let mut frames = vec![];
    bounded(async {
        loop {
            let frame = worker
                .recv()
                .await
                .expect("interruption must traverse the executor");
            let interrupted = matches!(frame.frame.data, FrameData::Interruption);
            frames.push(frame);
            if interrupted {
                break;
            }
        }
    })
    .await;
    assert_eq!(
        journal.snapshot().active.len(),
        1,
        "old policy must survive interruption"
    );
    assert_eq!(
        journal.snapshot().total_terminal,
        0,
        "new timeout must not alter old admission"
    );
    send(&worker, vec![call("old", "shared")]).await;
    send(
        &worker,
        vec![
            call("retired", "retired"),
            call("new", "shared"),
            call("added", "added"),
        ],
    )
    .await;
    release.notify_one();
    worker.end().await.unwrap();
    let (tail, report) = bounded(worker.collect()).await.unwrap();
    frames.extend(tail);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(new_calls.load(Ordering::SeqCst), 1);
    assert_eq!(retired_calls.load(Ordering::SeqCst), 0);
    assert_eq!(added_calls.load(Ordering::SeqCst), 1);
    assert!(!frames
        .iter()
        .any(|frame| matches!(frame.frame.data, FrameData::FunctionCallCancel(_))));
    let outcomes = journal.snapshot();
    assert!(outcomes.active.is_empty());
    assert_eq!(outcomes.total_terminal, 4);
    let outcome = |id: &str| {
        &outcomes
            .terminal
            .iter()
            .find(|receipt| receipt.outcome.call().tool_call_id == id)
            .unwrap()
            .outcome
    };
    assert_eq!(outcome("old").state(), ToolCallTerminalState::Completed);
    assert!(!outcome("old").cancel_on_interruption());
    assert_eq!(outcome("new").state(), ToolCallTerminalState::Completed);
    assert!(outcome("new").cancel_on_interruption());
    assert_eq!(outcome("retired").state(), ToolCallTerminalState::Failed);
    assert_eq!(
        outcome("retired").reason(),
        ToolCallEndReason::NotRegistered
    );
    assert_eq!(
        outcome("retired").effects(),
        ToolCallEffectStatus::NotStarted
    );
    let results: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::FunctionCallResult(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 4);
    assert_eq!(
        results
            .iter()
            .find(|result| result.tool_call_id == "old")
            .unwrap()
            .result,
        json!("old callback")
    );
    assert_eq!(
        results
            .iter()
            .find(|result| result.tool_call_id == "new")
            .unwrap()
            .result,
        json!("new callback")
    );
    assert_eq!(
        results
            .iter()
            .find(|result| result.tool_call_id == "added")
            .unwrap()
            .result,
        json!("added callback")
    );
    assert_eq!(
        results
            .iter()
            .find(|result| result.tool_call_id == "retired")
            .unwrap()
            .error
            .as_deref(),
        Some("tool is not registered")
    );
}
