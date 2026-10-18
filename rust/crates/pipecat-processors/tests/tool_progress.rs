//! Producer API and admission guards. Successful history receipts are exercised
//! by the actual Realtime/pair integration target, never fabricated in this crate.
use pipecat_core::{
    Direction, Frame, FrameData, FunctionCall, NativeToolProgressConfig, NativeToolProgressFailure,
    NativeToolProgressRoute, NativeToolProgressRouteState, Pipeline, PipelineConfig, WorkerState,
};
use pipecat_processors::tools::{RegisteredTool, ReportingTool, ToolCallback, ToolExecutor};
use serde_json::json;
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn route() -> NativeToolProgressRoute {
    NativeToolProgressRoute::new(NativeToolProgressConfig::default()).unwrap()
}
fn executor() -> ToolExecutor {
    ToolExecutor::new(2, 8, 1024)
        .unwrap()
        .with_progress_route(route())
        .unwrap()
}
fn reporting() -> ReportingTool {
    ReportingTool::new(Arc::new(|_, _| Box::pin(async { Ok(json!(true)) })))
}
fn final_only() -> RegisteredTool {
    RegisteredTool::new(Arc::new(|_| Box::pin(async { Ok(json!(true)) })))
}

#[test]
fn legacy_literal_and_callback_remain_source_compatible() {
    let callback: ToolCallback = Arc::new(|_| Box::pin(async { Ok(json!(true)) }));
    let tool = RegisteredTool {
        callback,
        timeout: Duration::from_secs(30),
        cancel_on_interruption: true,
    };
    let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
    executor.register("legacy", tool).unwrap();
}

#[test]
fn reporting_registration_requires_explicit_route_and_async_policy() {
    let mut legacy = ToolExecutor::new(2, 8, 1024).unwrap();
    assert!(legacy.register_reporting("work", reporting()).is_err());
    let mut selected = executor();
    let mut interruptible = reporting();
    interruptible.cancel_on_interruption = true;
    assert!(selected.register_reporting("work", interruptible).is_err());
    assert!(selected.register_reporting("", reporting()).is_err());
    selected.register_reporting("work", reporting()).unwrap();
    assert!(selected.register_reporting("work", reporting()).is_err());
    assert!(selected.register("work", final_only()).is_err());
    selected.register("ordinary", final_only()).unwrap();
    assert!(selected
        .register_reporting("ordinary", reporting())
        .is_err());
}

#[test]
fn reporting_timeouts_are_finite_without_changing_legacy_defaults() {
    for timeout in [
        Duration::ZERO,
        Duration::from_micros(999),
        Duration::from_secs(300) + Duration::from_nanos(1),
        Duration::MAX,
    ] {
        let mut selected = executor();
        let mut tool = reporting();
        tool.timeout = timeout;
        assert!(selected.register_reporting("work", tool).is_err());
    }
    for timeout in [Duration::from_millis(1), Duration::from_secs(300)] {
        let mut selected = executor();
        let mut tool = reporting();
        tool.timeout = timeout;
        selected.register_reporting("work", tool).unwrap();
    }
    let defaults = reporting();
    assert_eq!(defaults.timeout, Duration::from_secs(30));
    assert!(!defaults.cancel_on_interruption);
}

#[test]
fn selected_timeout_validation_includes_registered_ordinary_siblings() {
    let mut before = ToolExecutor::new(2, 8, 1024).unwrap();
    let mut long = final_only();
    long.timeout = Duration::from_secs(301);
    // Legacy standalone registration keeps its original contract.
    before.register("long", long.clone()).unwrap();
    assert!(before.with_progress_route(route()).is_err());
    let mut after = executor();
    assert!(after.register("long", long).is_err());
    let mut submillisecond = final_only();
    submillisecond.timeout = Duration::from_nanos(1);
    assert!(after.register("short", submillisecond).is_err());
}

#[test]
fn selected_capacity_and_route_rebinding_are_rejected() {
    assert!(ToolExecutor::new(17, 32, 1024)
        .unwrap()
        .with_progress_route(route())
        .is_err());
    assert!(executor().with_progress_route(route()).is_err());
}

#[tokio::test]
async fn an_executor_only_pipeline_cannot_issue_reporting_authority() {
    let entered = Arc::new(AtomicUsize::new(0));
    let counter = entered.clone();
    let selected_route = route();
    let mut executor = ToolExecutor::new(1, 4, 1024)
        .unwrap()
        .with_progress_route(selected_route.clone())
        .unwrap();
    executor
        .register_reporting(
            "work",
            ReportingTool::new(Arc::new(move |_, _| {
                counter.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!(true)) })
            })),
        )
        .unwrap();
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "work".into(),
                tool_call_id: "unadmitted".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .expect("unsupported selected composition did not fail closed");
    assert_eq!(entered.load(Ordering::SeqCst), 0);
    assert!(journal.snapshot().active.is_empty());
    assert_eq!(journal.snapshot().total_terminal, 0);
    assert!(!selected_route.is_open());
    assert!(result.is_err());
}

#[tokio::test]
async fn duplicate_selected_executor_setup_is_rejected_before_admission() {
    let selected_route = route();
    let first = ToolExecutor::new(1, 4, 1024)
        .unwrap()
        .with_progress_route(selected_route.clone())
        .unwrap();
    let second = ToolExecutor::new(1, 4, 1024)
        .unwrap()
        .with_progress_route(selected_route.clone())
        .unwrap();
    let journals = [first.outcomes(), second.outcomes()];
    let mut worker = Pipeline::new(vec![Arc::new(first), Arc::new(second)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    // Setup errors are reported after Start reaches Running. The duplicate
    // limits registration rejects this setup before its role registration.
    let errors = worker.errors();
    assert_eq!(errors.len(), 1);
    assert_eq!(
        errors[0].error,
        "selected executor limits already registered"
    );
    assert_eq!(errors[0].processor.as_deref(), Some("ToolExecutor"));
    assert!(!errors[0].fatal);
    assert_eq!(selected_route.state(), NativeToolProgressRouteState::Failed);
    assert_eq!(
        selected_route.failure(),
        Some(NativeToolProgressFailure::InvalidProvenance)
    );
    assert!(!selected_route.is_open());
    assert!(!selected_route.accepts_new_calls());
    for journal in &journals {
        let snapshot = journal.snapshot();
        assert!(snapshot.active.is_empty());
        assert_eq!(snapshot.total_terminal, 0);
    }
    worker
        .cancel("duplicate selected executor setup")
        .await
        .unwrap();
    let report = tokio::time::timeout(Duration::from_secs(2), worker.join())
        .await
        .expect("duplicate selected setup did not join after cancellation")
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(report.errors, errors);
}
