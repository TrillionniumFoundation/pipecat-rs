//! Behavior witness against the old executor after the typed API is declared.
use pipecat_core::*;
use pipecat_processors::tools::{ToolExecutor, ToolOutcomeSnapshot};
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[tokio::test]
async fn direct_success_preserves_explicit_final_result_policy() {
    let mut executor = ToolExecutor::new(2, 8, 1024).unwrap();
    executor
        .register_direct(
            "record",
            LocalToolFunction::new_with_result(|_| async {
                Ok(LocalToolResult::new(json!({"recorded":true})).with_run_llm(false))
            }),
            None,
        )
        .unwrap();
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    tokio::time::timeout(
        Duration::from_secs(5),
        sender.send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "record".into(),
                tool_call_id: "record-call".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), sender.end())
        .await
        .unwrap()
        .unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let terminal: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
        .collect();
    assert_eq!(terminal.len(), 1);
    let frame = &terminal[0].frame;
    let FrameData::FunctionCallResult(result) = &frame.data else {
        unreachable!()
    };
    assert_eq!(result.result, json!({"recorded":true}));
    assert!(result.is_final && result.error.is_none());
    let outcome = frame.native_tool_outcome().unwrap().unwrap();
    assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
    assert_eq!(journal.snapshot().total_terminal, 1);
    assert_eq!(
        result.run_llm,
        Some(false),
        "the registered typed success policy must reach the owned terminal result"
    );
}

async fn execute(
    executor: ToolExecutor,
    names: &[&str],
    expected_diagnostics: usize,
) -> (Vec<DirectedFrame>, ToolOutcomeSnapshot) {
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    let calls = names
        .iter()
        .map(|name| FunctionCall {
            function_name: (*name).into(),
            tool_call_id: (*name).into(),
            arguments: json!({}),
        })
        .collect();
    tokio::time::timeout(
        Duration::from_secs(5),
        sender.send(
            Frame::new(FrameData::FunctionCallsStarted(calls)),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), sender.end())
        .await
        .unwrap()
        .unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        report.errors.len(),
        expected_diagnostics,
        "{:?}",
        report.errors
    );
    assert!(report
        .errors
        .iter()
        .all(|error| error.category == ErrorCategory::Application && !error.fatal));
    (frames, journal.snapshot())
}

#[tokio::test]
async fn result_policy_is_sealed_and_user_json_and_default_policy_are_preserved() {
    let mut executor = ToolExecutor::new(3, 8, 1024).unwrap();
    for (name, flag) in [
        ("default", None),
        ("false", Some(false)),
        ("true", Some(true)),
    ] {
        executor
            .register_direct(
                name,
                LocalToolFunction::new_with_result(move |_| async move {
                    Ok(LocalToolResult {
                        value: json!({"run_llm":"user-data"}),
                        run_llm: flag,
                    })
                }),
                None,
            )
            .unwrap();
    }
    let (frames, journal) = execute(executor, &["default", "false", "true"], 0).await;
    assert_eq!(journal.total_terminal, 3);
    for (name, flag) in [
        ("default", None),
        ("false", Some(false)),
        ("true", Some(true)),
    ] {
        let frame = &frames.iter().find(|f| matches!(&f.frame.data, FrameData::FunctionCallResult(result) if result.tool_call_id == name)).unwrap().frame;
        let FrameData::FunctionCallResult(result) = &frame.data else {
            unreachable!()
        };
        assert_eq!(result.result, json!({"run_llm":"user-data"}));
        assert_eq!(result.run_llm, flag);
        assert!(frame.native_tool_outcome().unwrap().is_some());
        let mut altered = frame.clone();
        let FrameData::FunctionCallResult(result) = &mut altered.data else {
            unreachable!()
        };
        result.run_llm = Some(!flag.unwrap_or(false));
        assert!(
            altered.native_tool_outcome().is_err(),
            "a changed result policy must fail native receipt validation"
        );
    }
}

#[tokio::test]
async fn typed_errors_and_oversize_results_keep_default_failure_policy() {
    let mut executor = ToolExecutor::new(3, 8, 1024).unwrap();
    executor
        .register_direct(
            "error",
            LocalToolFunction::new_with_result(|_| async {
                Err(ProcessorError::new("direct tool invocation was cancelled"))
            }),
            None,
        )
        .unwrap();
    executor
        .register_direct(
            "oversize",
            LocalToolFunction::new_with_result(|_| async {
                Ok(LocalToolResult::new(serde_json::Value::String(
                    "x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES),
                ))
                .with_run_llm(false))
            }),
            None,
        )
        .unwrap();
    executor
        .register_direct(
            "cancelled",
            LocalToolFunction::new_cancellable_with_result(|_| async { None }),
            None,
        )
        .unwrap();
    let (frames, journal) = execute(executor, &["error", "oversize", "cancelled"], 1).await;
    assert_eq!(journal.total_terminal, 3);
    for (name, reason) in [
        ("error", ToolCallEndReason::CallbackError),
        ("oversize", ToolCallEndReason::ResultTooLarge),
    ] {
        let frame = &frames.iter().find(|f| matches!(&f.frame.data, FrameData::FunctionCallResult(result) if result.tool_call_id == name)).unwrap().frame;
        let FrameData::FunctionCallResult(result) = &frame.data else {
            unreachable!()
        };
        assert!(result.is_final && result.error.is_some());
        assert_eq!(result.run_llm, None);
        let outcome = frame.native_tool_outcome().unwrap().unwrap();
        assert_eq!(outcome.state(), ToolCallTerminalState::Failed);
        assert_eq!(outcome.reason(), reason);
    }
    assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::FunctionCallCancel(cancel) if cancel.tool_call_id == "cancelled" && !cancel.run_llm)));
    assert_eq!(
        journal
            .terminal
            .iter()
            .find(|r| r.outcome.call().tool_call_id == "cancelled")
            .unwrap()
            .outcome
            .state(),
        ToolCallTerminalState::Cancelled
    );
}

#[tokio::test]
async fn explicit_cancel_keeps_its_policy_and_drops_the_typed_success_future() {
    struct DropSignal(Arc<tokio::sync::Notify>);
    impl Drop for DropSignal {
        fn drop(&mut self) {
            self.0.notify_one();
        }
    }
    let entered = Arc::new(tokio::sync::Notify::new());
    let dropped = Arc::new(tokio::sync::Notify::new());
    let (begin, drop_signal) = (entered.clone(), dropped.clone());
    let handler = LocalToolFunction::new_with_result(move |_| {
        let (begin, drop_signal) = (begin.clone(), drop_signal.clone());
        async move {
            let _guard = DropSignal(drop_signal);
            begin.notify_one();
            std::future::pending::<()>().await;
            Ok(LocalToolResult::new(json!({"late":true})).with_run_llm(false))
        }
    })
    .with_options(LocalToolOptions {
        cancel_on_interruption: false,
        timeout: None,
    })
    .unwrap();
    let mut executor = ToolExecutor::new(1, 8, 1024).unwrap();
    executor.register_direct("held", handler, None).unwrap();
    let journal = executor.outcomes();
    let worker = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let sender = worker.sender();
    tokio::time::timeout(
        Duration::from_secs(5),
        sender.send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "held".into(),
                tool_call_id: "held".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.notified())
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(5),
        sender.send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "held".into(),
                tool_call_id: "held".into(),
                run_llm: true,
            })),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), dropped.notified())
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), sender.end())
        .await
        .unwrap()
        .unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_))));
    let snapshot = journal.snapshot();
    assert_eq!(snapshot.total_terminal, 1);
    let receipt = &snapshot.terminal[0];
    assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(receipt.outcome.reason(), ToolCallEndReason::ExplicitCancel);
    assert!(receipt.outcome.run_llm());
    assert!(frames.iter().any(
        |f| matches!(&f.frame.data, FrameData::FunctionCallCancel(cancel) if cancel.run_llm)
            && f.frame.native_tool_outcome().unwrap().is_some()
    ));
}
