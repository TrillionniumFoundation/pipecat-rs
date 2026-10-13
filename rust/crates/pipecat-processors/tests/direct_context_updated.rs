use pipecat_core::*;
use pipecat_processors::tools::ToolExecutor;
use serde_json::json;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("direct context-update completion did not settle")
}

fn call(name: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: name.into(),
        arguments: json!({"input":"original"}),
    }
}

async fn start(worker: &PipelineWorker, names: &[&str]) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(
                names.iter().map(|name| call(name)).collect(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
}

fn hook(resource: Arc<()>, invoked: Arc<AtomicUsize>) -> LocalAsyncCallback {
    LocalAsyncCallback::new(move || {
        let (resource, invoked) = (resource.clone(), invoked.clone());
        async move {
            invoked.fetch_add(1, Ordering::SeqCst);
            drop(resource);
            Ok(())
        }
    })
}

#[tokio::test]
async fn successful_completion_keeps_hook_only_on_its_final_frame_and_not_in_the_journal() {
    bounded(async {
        let invoked = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let callback = hook(resource, invoked.clone());
        let handler = LocalToolFunction::new_with_completion(move |arguments| {
            let callback = callback.clone();
            async move {
                assert_eq!(arguments, call("success").arguments);
                Ok(LocalToolCompletion::new(
                    LocalToolResult::new(json!({"saved":true})).with_run_llm(false),
                )
                .with_context_updated(callback))
            }
        });
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor.register_direct("success", handler, None).unwrap();
        let journal = executor.outcomes();
        let worker = Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        start(&worker, &["success"]).await;
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let finals: Vec<_> = frames
            .iter()
            .filter(|item| item.frame.native_tool_outcome().unwrap().is_some())
            .collect();
        assert_eq!(finals.len(), 1);
        let frame = &finals[0].frame;
        let outcome = frame.native_tool_outcome().unwrap().unwrap();
        assert_eq!(outcome.call(), &call("success"));
        assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
        assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
        assert_eq!(
            frame.native_tool_context_updated().unwrap(),
            Some(outcome.id())
        );
        let FrameData::FunctionCallResult(result) = &frame.data else {
            panic!("successful completion must carry its result");
        };
        assert_eq!(result.result, json!({"saved":true}));
        assert_eq!(result.run_llm, Some(false));
        assert!(result.is_final && result.error.is_none());
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            0,
            "executor must not run the hook"
        );
        assert!(
            weak_resource.upgrade().is_some(),
            "final frame owns the hook"
        );
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 1);
        assert_eq!(
            settled.terminal[0].admission,
            ToolOutcomeAdmission::Admitted
        );

        // Neither the immutable receipt snapshot nor the still-live journal may
        // retain the callback after its final frame and all handlers are gone.
        drop(finals);
        drop(frames);
        assert!(weak_resource.upgrade().is_none());
        assert_eq!(settled.terminal.len(), 1);
        assert_eq!(journal.snapshot().total_terminal, 1);
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
    })
    .await;
}

#[tokio::test]
async fn failure_oversize_and_cancellation_discard_hooks_without_running_them() {
    bounded(async {
        let invoked = Arc::new(AtomicUsize::new(0));
        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
        let callback = hook(resource, invoked.clone());
        let entered = Arc::new(Notify::new());
        let mut executor = ToolExecutor::new(4, 8, 1024).unwrap();
        let failed_callback = callback.clone();
        executor
            .register_direct(
                "error",
                LocalToolFunction::new_with_completion(move |_| {
                    let callback = failed_callback.clone();
                    async move {
                        let _retained_until_failure = callback;
                        Err(ProcessorError::new("callback failed"))
                    }
                }),
                None,
            )
            .unwrap();
        let oversized_callback = callback.clone();
        executor
            .register_direct(
                "oversize",
                LocalToolFunction::new_with_completion(move |_| {
                    let callback = oversized_callback.clone();
                    async move {
                        Ok(LocalToolCompletion::new(LocalToolResult::new(
                            serde_json::Value::String(
                                "x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES),
                            ),
                        ))
                        .with_context_updated(callback))
                    }
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
        let held = {
            let entered = entered.clone();
            LocalToolFunction::new_with_completion(move |_| {
                let (entered, callback) = (entered.clone(), callback.clone());
                async move {
                    // A valid completion is already retained when cancellation
                    // wins. Destruction must release its hook without execution.
                    let completion = LocalToolCompletion::new(
                        LocalToolResult::new(json!("late success")).with_run_llm(true),
                    )
                    .with_context_updated(callback);
                    entered.notify_one();
                    std::future::pending::<()>().await;
                    Ok(completion)
                }
            })
            .with_options(LocalToolOptions {
                cancel_on_interruption: false,
                timeout: None,
            })
            .unwrap()
        };
        executor.register_direct("held", held, None).unwrap();
        let journal = executor.outcomes();
        let worker = Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        start(&worker, &["error", "oversize", "cancelled", "held"]).await;
        entered.notified().await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "held".into(),
                    tool_call_id: "held".into(),
                    run_llm: false,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker.end().await.unwrap();
        let (frames, report) = worker.collect().await.unwrap();
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        assert_eq!(report.errors[0].category, ErrorCategory::Application);
        assert!(!report.errors[0].fatal);
        assert!(report.errors[0].error.contains("callback failed"));
        assert!(frames.iter().all(|item| item
            .frame
            .native_tool_context_updated()
            .unwrap()
            .is_none()));
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert!(
            weak_resource.upgrade().is_none(),
            "failed frames must not retain hooks"
        );
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 4);
        assert_eq!(settled.terminal.len(), 4);
        assert_eq!(
            frames
                .iter()
                .filter(|item| item.frame.native_tool_outcome().unwrap().is_some())
                .count(),
            4
        );
        for (name, state, reason) in [
            (
                "error",
                ToolCallTerminalState::Failed,
                ToolCallEndReason::CallbackError,
            ),
            (
                "oversize",
                ToolCallTerminalState::Failed,
                ToolCallEndReason::ResultTooLarge,
            ),
            (
                "cancelled",
                ToolCallTerminalState::Cancelled,
                ToolCallEndReason::ExplicitCancel,
            ),
            (
                "held",
                ToolCallTerminalState::Cancelled,
                ToolCallEndReason::ExplicitCancel,
            ),
        ] {
            let receipt = settled
                .terminal
                .iter()
                .find(|receipt| receipt.outcome.call().tool_call_id == name)
                .unwrap();
            assert_eq!(receipt.outcome.state(), state);
            assert_eq!(receipt.outcome.reason(), reason);
            assert!(!receipt.outcome.run_llm());
            assert_eq!(receipt.admission, ToolOutcomeAdmission::Admitted);
        }
    })
    .await;
}
