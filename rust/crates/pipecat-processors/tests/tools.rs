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
fn call(id: &str, name: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({"n":3}),
    }
}
async fn run(
    executor: ToolExecutor,
    calls: Vec<FunctionCall>,
) -> (Vec<DirectedFrame>, WorkerReport) {
    tokio::time::timeout(Duration::from_secs(2), async {
        let worker = Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallsStarted(calls)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker.end().await.unwrap();
        worker.collect().await.unwrap()
    })
    .await
    .unwrap()
}
fn results(out: &[DirectedFrame]) -> Vec<&FunctionCallResultFrame> {
    out.iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::FunctionCallResult(result) => Some(result),
            _ => None,
        })
        .collect()
}

#[tokio::test]
async fn callback_cancellation_settles_once_and_legacy_results_keep_their_behavior() {
    let mut executor = ToolExecutor::new(5, 8, 1024).unwrap();
    let cancelled = CancellableRegisteredTool::new(Arc::new(|call| {
        Box::pin(async move {
            match call.tool_call_id.as_str() {
                "cancel" => None,
                "null" => Some(Ok(serde_json::Value::Null)),
                _ => Some(Err("typed callback error".into())),
            }
        })
    }));
    executor
        .register_cancellable("cancelled", cancelled.clone())
        .unwrap();
    executor
        .register(
            "success",
            RegisteredTool {
                callback: Arc::new(|_| Box::pin(async { Ok(json!("legacy result")) })),
                timeout: Duration::from_secs(1),
                cancel_on_interruption: true,
            },
        )
        .unwrap();
    executor
        .register(
            "failed",
            RegisteredTool::new(Arc::new(|_| Box::pin(async { Err("legacy error".into()) }))),
        )
        .unwrap();
    assert!(executor.register_cancellable("success", cancelled).is_err());
    let journal = executor.outcomes();
    let (out, report) = run(
        executor,
        vec![
            call("cancel", "cancelled"),
            call("null", "cancelled"),
            call("typed-err", "cancelled"),
            call("ok", "success"),
            call("err", "failed"),
        ],
    )
    .await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let cancellation: Vec<_> = out
        .iter()
        .filter_map(|frame| {
            if let FrameData::FunctionCallCancel(call) = &frame.frame.data {
                Some(call)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(cancellation.len(), 1);
    assert_eq!(cancellation[0].function_name, "cancelled");
    assert_eq!(cancellation[0].tool_call_id, "cancel");
    assert!(!cancellation[0].run_llm);
    let results = results(&out);
    assert_eq!(results.len(), 4);
    let empty = results
        .iter()
        .find(|result| result.tool_call_id == "null")
        .unwrap();
    assert!(empty.result.is_null());
    assert!(empty.error.is_none());
    let typed_error = results
        .iter()
        .find(|result| result.tool_call_id == "typed-err")
        .unwrap();
    assert_eq!(typed_error.error.as_deref(), Some("typed callback error"));
    let succeeded = results
        .iter()
        .find(|result| result.tool_call_id == "ok")
        .unwrap();
    assert_eq!(succeeded.result, json!("legacy result"));
    assert!(succeeded.error.is_none());
    let failed = results
        .iter()
        .find(|result| result.tool_call_id == "err")
        .unwrap();
    assert_eq!(failed.error.as_deref(), Some("legacy error"));
    let snapshot = journal.snapshot();
    assert!(snapshot.active.is_empty());
    assert_eq!(snapshot.total_terminal, 5);
    assert_eq!(snapshot.terminal.len(), 5);
    let cancelled = snapshot
        .terminal
        .iter()
        .find(|receipt| receipt.outcome.call().tool_call_id == "cancel")
        .unwrap();
    assert_eq!(cancelled.outcome.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(
        cancelled.outcome.reason(),
        ToolCallEndReason::ExplicitCancel
    );
    assert_eq!(
        cancelled.outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!cancelled.outcome.run_llm());
    let failed = snapshot
        .terminal
        .iter()
        .find(|receipt| receipt.outcome.call().tool_call_id == "err")
        .unwrap();
    assert_eq!(failed.outcome.state(), ToolCallTerminalState::Failed);
    assert_eq!(failed.outcome.reason(), ToolCallEndReason::CallbackError);
}
#[tokio::test]
async fn parallel_callbacks_have_all_placeholders_before_results() {
    let mut executor = ToolExecutor::new(4, 16, 1024).unwrap();
    executor
        .register(
            "double",
            RegisteredTool::new(Arc::new(|call| {
                Box::pin(async move { Ok(json!(call.arguments["n"].as_i64().unwrap() * 2)) })
            })),
        )
        .unwrap();
    let (out, report) = run(executor, vec![call("a", "double"), call("b", "double")]).await;
    assert!(report.errors.is_empty());
    assert_eq!(results(&out).len(), 2);
    assert!(results(&out).iter().all(|r| r.result == 6));
    let last_start = out
        .iter()
        .rposition(|f| matches!(f.frame.data, FrameData::FunctionCallInProgress(_)))
        .unwrap();
    let first_result = out
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
        .unwrap();
    assert!(last_start < first_result);
}
#[tokio::test]
async fn unknown_tool_has_placeholder_and_terminal_error() {
    let (out, report) = run(
        ToolExecutor::new(1, 4, 1024).unwrap(),
        vec![call("a", "missing")],
    )
    .await;
    assert!(report.errors.is_empty());
    assert_eq!(results(&out).len(), 1);
    assert_eq!(
        results(&out)[0].error.as_deref(),
        Some("tool is not registered")
    );
    assert!(out
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::FunctionCallInProgress(_))));
}
#[tokio::test]
async fn duplicate_id_cannot_execute_twice_even_with_history_window_one() {
    let count = Arc::new(AtomicUsize::new(0));
    let copy = count.clone();
    let mut executor = ToolExecutor::new(3, 1, 1024).unwrap();
    executor
        .register(
            "f",
            RegisteredTool::new(Arc::new(move |_| {
                let count = copy.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    Ok(json!(true))
                })
            })),
        )
        .unwrap();
    let (out, report) = run(
        executor,
        vec![call("a", "f"), call("b", "f"), call("a", "f")],
    )
    .await;
    assert!(report.errors.is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 2);
    assert_eq!(results(&out).len(), 2);
}
#[tokio::test]
async fn timeout_and_panic_are_terminal_tool_results() {
    let mut executor = ToolExecutor::new(2, 8, 1024).unwrap();
    let mut slow = RegisteredTool::new(Arc::new(|_| {
        Box::pin(async { std::future::pending().await })
    }));
    slow.timeout = Duration::from_millis(10);
    executor.register("slow", slow).unwrap();
    executor
        .register(
            "panic",
            RegisteredTool::new(Arc::new(|_| Box::pin(async { panic!("synthetic panic") }))),
        )
        .unwrap();
    let (out, report) = run(executor, vec![call("a", "slow"), call("b", "panic")]).await;
    assert!(report.errors.is_empty());
    let result = results(&out);
    assert_eq!(result.len(), 2);
    assert!(result.iter().all(|r| r.error.is_some()));
}
#[tokio::test]
async fn malformed_or_excessive_calls_fail_before_callback_admission() {
    let (out, report) = run(
        ToolExecutor::new(1, 8, 1024).unwrap(),
        vec![call("a", "f"), call("b", "f")],
    )
    .await;
    assert_eq!(report.errors.len(), 1);
    assert!(results(&out).is_empty());
    let mut bad = call("bad", "f");
    bad.arguments = json!("not-object");
    let (_, report) = run(ToolExecutor::new(1, 8, 1024).unwrap(), vec![bad]).await;
    assert_eq!(report.errors.len(), 1);
}

#[tokio::test]
async fn explicit_function_cancel_stops_even_non_interruptible_callback() {
    struct Guard(Arc<tokio::sync::Notify>);
    impl Drop for Guard {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let stopped = Arc::new(tokio::sync::Notify::new());
    let (begin, stop) = (entered.clone(), stopped.clone());
    let mut executor = ToolExecutor::new(1, 8, 1024).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        let (begin, stop) = (begin.clone(), stop.clone());
        Box::pin(async move {
            let _guard = Guard(stop);
            begin.notify_one();
            std::future::pending().await
        })
    }));
    tool.cancel_on_interruption = false;
    executor.register("wait", tool).unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call("a", "wait")])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), entered.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "wait".into(),
                tool_call_id: "a".into(),
                run_llm: false,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), stopped.notified())
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (out, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert!(results(&out).is_empty());
}
