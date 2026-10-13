use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor, ToolRegistrationSnapshot};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, RwLock,
    },
    time::Duration,
};
use tokio::sync::Semaphore;

fn call(id: &str, name: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
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
async fn result(worker: &mut PipelineWorker) -> FunctionCallResultFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(output) = worker.recv().await {
            if let FrameData::FunctionCallResult(result) = output.frame.data {
                return result;
            }
        }
        panic!("pipeline ended before direct tool result");
    })
    .await
    .expect("direct tool result did not settle")
}

#[tokio::test]
async fn direct_registration_uses_typed_cancellation_and_leaves_cleanup_to_its_owner() {
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let calls = cleanup_calls.clone();
    let null = LocalToolFunction::new_cancellable(|_| async { Some(Ok(Value::Null)) })
        .with_cleanup(LocalAsyncCallback::new(move || {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(())
            }
        }));
    let mut executor = ToolExecutor::new(4, 8, 1024).unwrap();
    for (name, handler) in [
        (
            "cancelled",
            LocalToolFunction::new_cancellable(|_| async { None }),
        ),
        ("null", null),
        (
            "error",
            LocalToolFunction::new_cancellable(|_| async {
                Some(Err(ProcessorError::new(
                    "direct tool invocation was cancelled",
                )))
            }),
        ),
        (
            "ordinary",
            LocalToolFunction::new(|value| async { Ok(value) }),
        ),
    ] {
        executor.register_direct(name, handler, None).unwrap();
    }
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        ["cancelled", "null", "error", "ordinary"]
            .into_iter()
            .map(|name| call(name, name))
            .collect(),
    )
    .await;
    worker.end().await.unwrap();
    let (outputs, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
    let outcomes = journal.snapshot();
    assert_eq!(outcomes.total_terminal, 4);
    for (name, state, reason) in [
        (
            "cancelled",
            ToolCallTerminalState::Cancelled,
            ToolCallEndReason::ExplicitCancel,
        ),
        (
            "null",
            ToolCallTerminalState::Completed,
            ToolCallEndReason::CallbackReturned,
        ),
        (
            "error",
            ToolCallTerminalState::Failed,
            ToolCallEndReason::CallbackError,
        ),
        (
            "ordinary",
            ToolCallTerminalState::Completed,
            ToolCallEndReason::CallbackReturned,
        ),
    ] {
        let outcome = &outcomes
            .terminal
            .iter()
            .find(|receipt| receipt.outcome.call().tool_call_id == name)
            .unwrap()
            .outcome;
        assert_eq!(outcome.state(), state);
        assert_eq!(outcome.reason(), reason);
        assert_eq!(
            outcome.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
    }
    let results: Vec<_> = outputs
        .iter()
        .filter_map(|output| match &output.frame.data {
            FrameData::FunctionCallResult(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 3);
    assert!(results.iter().any(|result| result.tool_call_id == "null"
        && result.result == Value::Null
        && result.error.is_none()));
    assert!(results
        .iter()
        .any(|result| result.tool_call_id == "ordinary"
            && result.result == json!({})
            && result.error.is_none()));
    assert!(results.iter().any(|result| result.tool_call_id == "error"
        && result.error.as_deref() == Some("direct tool invocation was cancelled")));
    assert_eq!(
        outputs
            .iter()
            .filter(|output| matches!(&output.frame.data,
                FrameData::FunctionCallCancel(cancel) if cancel.tool_call_id == "cancelled"
            ))
            .count(),
        1
    );
}

struct PublishedRegistrations {
    executor: ToolExecutor,
    current: Arc<RwLock<ToolRegistrationSnapshot>>,
}
#[async_trait]
impl FrameProcessor for PublishedRegistrations {
    fn name(&self) -> &str {
        "DirectRegistrations"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let registrations = self.current.read().unwrap().clone();
        self.executor
            .process_with_registrations(frame, direction, ctx, &registrations)
            .await
    }
}
fn pending_handler(entered: Arc<Semaphore>) -> LocalToolFunction {
    LocalToolFunction::new(move |_| {
        let entered = entered.clone();
        async move {
            entered.add_permits(1);
            std::future::pending::<Result<Value, ProcessorError>>().await
        }
    })
}

#[tokio::test(start_paused = true)]
async fn direct_deadlines_resolve_and_admitted_contract_survives_replacement() {
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let old_calls = Arc::new(AtomicUsize::new(0));
    let new_calls = Arc::new(AtomicUsize::new(0));
    let old = {
        let entered = entered.clone();
        let release = release.clone();
        let calls = old_calls.clone();
        LocalToolFunction::new(move |_| {
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                entered.add_permits(1);
                release.acquire().await.unwrap().forget();
                Ok(json!("old"))
            }
        })
        .with_options(LocalToolOptions {
            cancel_on_interruption: false,
            timeout: None,
        })
        .unwrap()
    };
    let mut initial = ToolExecutor::new(4, 8, 1024).unwrap();
    // Even a handler override cannot make an invalid binding default acceptable.
    let override_handler = pending_handler(entered.clone())
        .with_options(LocalToolOptions {
            timeout: Some(Duration::from_secs(5)),
            ..Default::default()
        })
        .unwrap();
    assert!(initial
        .register_direct("override", override_handler.clone(), Some(Duration::ZERO))
        .is_err());
    initial.register_direct("shared", old, None).unwrap();
    initial
        .register_direct(
            "inherited",
            pending_handler(entered.clone()),
            Some(Duration::from_secs(2)),
        )
        .unwrap();
    initial
        .register_direct("override", override_handler, Some(Duration::from_secs(2)))
        .unwrap();
    let legacy_entered = entered.clone();
    let mut legacy = RegisteredTool::new(Arc::new(move |_| {
        let entered = legacy_entered.clone();
        Box::pin(async move {
            entered.add_permits(1);
            std::future::pending::<Result<Value, String>>().await
        })
    }));
    legacy.timeout = Duration::from_secs(3);
    initial.register("legacy", legacy).unwrap();
    let current = Arc::new(RwLock::new(initial.registrations_snapshot()));
    drop(initial);
    let runtime = ToolExecutor::new(4, 8, 1024).unwrap();
    let journal = runtime.outcomes();
    let mut worker = Pipeline::new(vec![Arc::new(PublishedRegistrations {
        executor: runtime,
        current: current.clone(),
    })])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    send(
        &worker,
        vec![
            call("old", "shared"),
            call("inherited", "inherited"),
            call("override", "override"),
            call("legacy", "legacy"),
        ],
    )
    .await;
    tokio::time::timeout(Duration::from_secs(1), entered.acquire_many(4))
        .await
        .unwrap()
        .unwrap()
        .forget();
    tokio::time::advance(Duration::from_secs(3)).await;
    let a = result(&mut worker).await;
    let b = result(&mut worker).await;
    let mut ids = [a.tool_call_id, b.tool_call_id];
    ids.sort();
    assert_eq!(ids, ["inherited", "legacy"]);
    assert_eq!(journal.snapshot().active.len(), 2);
    tokio::time::advance(Duration::from_secs(3)).await;
    assert_eq!(result(&mut worker).await.tool_call_id, "override");
    tokio::time::advance(Duration::from_secs(60)).await;
    let active = journal.snapshot();
    assert_eq!(active.active.len(), 1);
    assert_eq!(active.active[0].tool_call_id, "old");
    let mut replacement = ToolExecutor::new(4, 8, 1024).unwrap();
    let calls = new_calls.clone();
    replacement
        .register_direct(
            "shared",
            LocalToolFunction::new(move |_| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(json!("new"))
                }
            })
            .with_options(LocalToolOptions {
                cancel_on_interruption: true,
                timeout: Some(Duration::from_millis(100)),
            })
            .unwrap(),
            None,
        )
        .unwrap();
    *current.write().unwrap() = replacement.registrations_snapshot();
    drop(replacement);
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    while !matches!(
        worker.recv().await.unwrap().frame.data,
        FrameData::Interruption
    ) {}
    assert_eq!(journal.snapshot().active.len(), 1);
    send(&worker, vec![call("old", "shared"), call("new", "shared")]).await;
    let new = result(&mut worker).await;
    assert_eq!(new.tool_call_id, "new");
    assert_eq!(new.result, json!("new"));
    release.add_permits(1);
    let old = result(&mut worker).await;
    assert_eq!(old.tool_call_id, "old");
    assert_eq!(old.result, json!("old"));
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(new_calls.load(Ordering::SeqCst), 1);
    let outcomes = journal.snapshot();
    assert_eq!(outcomes.total_terminal, 5);
    for id in ["inherited", "legacy", "override"] {
        let outcome = &outcomes
            .terminal
            .iter()
            .find(|receipt| receipt.outcome.call().tool_call_id == id)
            .unwrap()
            .outcome;
        assert_eq!(outcome.reason(), ToolCallEndReason::TimedOut);
    }
    for (id, policy) in [("old", false), ("new", true)] {
        let outcome = &outcomes
            .terminal
            .iter()
            .find(|receipt| receipt.outcome.call().tool_call_id == id)
            .unwrap()
            .outcome;
        assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
        assert_eq!(outcome.cancel_on_interruption(), policy);
    }
}
