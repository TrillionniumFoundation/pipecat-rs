use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
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

struct HealthOwner {
    executor: ToolExecutor,
    health: ProcessorHealth,
}
#[async_trait]
impl FrameProcessor for HealthOwner {
    fn name(&self) -> &str {
        "DirectApplicationOwner"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.executor.process(frame, direction, ctx).await
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("application-error fixture did not settle")
}
fn call(id: &str, name: &str, mode: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({"mode":mode}),
    }
}
async fn send(worker: &PipelineWorker, call: FunctionCall) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call])),
            Direction::Downstream,
        )
        .await
        .unwrap();
}
async fn receive_until(
    worker: &mut PipelineWorker,
    frames: &mut Vec<DirectedFrame>,
    ready: impl Fn(&Frame) -> bool,
) {
    bounded(async {
        loop {
            let item = worker
                .recv()
                .await
                .expect("worker closed before fixture outcome");
            let done = ready(&item.frame);
            frames.push(item);
            if done {
                return;
            }
        }
    })
    .await;
}
fn is_terminal(frame: &Frame, id: &str) -> bool {
    frame
        .native_tool_outcome()
        .unwrap()
        .is_some_and(|outcome| outcome.call().tool_call_id == id)
}
fn terminal<'a>(frames: &'a [DirectedFrame], id: &str) -> &'a NativeToolCallOutcome {
    let outcomes: Vec<_> = frames
        .iter()
        .filter_map(|item| item.frame.native_tool_outcome().unwrap())
        .filter(|outcome| outcome.call().tool_call_id == id)
        .collect();
    assert_eq!(outcomes.len(), 1, "terminal settlement duplicated for {id}");
    outcomes[0].as_ref()
}
fn diagnostics(frames: &[DirectedFrame]) -> Vec<&DirectedFrame> {
    frames
        .iter()
        .filter(|item| matches!(&item.frame.data, FrameData::Error(_)))
        .collect()
}
fn assert_application(item: &DirectedFrame) -> &ErrorFrame {
    assert_eq!(item.direction, Direction::Upstream);
    let FrameData::Error(error) = &item.frame.data else {
        panic!("expected upstream application diagnostic");
    };
    assert_eq!(error.category, ErrorCategory::Application);
    assert!(!error.fatal);
    assert_eq!(error.processor.as_deref(), Some("DirectApplicationOwner"));
    assert!(item.frame.error_origin().unwrap().reported_health.is_usable);
    error
}

#[tokio::test]
async fn direct_auth_not_found_and_fatal_errors_report_once_without_poisoning_or_replay() {
    bounded(async {
        let invocations = Arc::new(AtomicUsize::new(0));
        let count = invocations.clone();
        let handler = LocalToolFunction::new(move |arguments| {
            let count = count.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                match arguments["mode"].as_str().unwrap() {
                    "401" => Err(ProcessorError::new("HTTP 401 fixture application failure")
                        .with_category(ErrorCategory::Authentication)),
                    "404" => Err(ProcessorError::new("HTTP 404 fixture application failure")
                        .with_category(ErrorCategory::InvalidRequest)),
                    "fatal" => Err(ProcessorError::fatal("fatal fixture application failure")
                        .with_category(ErrorCategory::Authorization)),
                    _ => Ok(json!("next call succeeded")),
                }
            }
        });
        let mut executor = ToolExecutor::new(1, 32, 1024).unwrap();
        executor.register_direct("job", handler, None).unwrap();
        let journal = executor.outcomes();
        let health = ProcessorHealth::default();
        let original_health = health.snapshot();
        let mut worker = Pipeline::new(vec![Arc::new(HealthOwner {
            executor,
            health: health.clone(),
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let mut frames = vec![];
        for mode in ["401", "404", "fatal"] {
            send(&worker, call(mode, "job", mode)).await;
            receive_until(&mut worker, &mut frames, |frame| {
                matches!(&frame.data, FrameData::Error(error) if error.error.contains(&format!("{mode} fixture")))
            })
            .await;
            // The diagnostic is emitted after the original settlement ledger
            // is updated. It must not consume a new terminal reservation.
            assert!(journal.snapshot().active.is_empty());
            assert_eq!(health.snapshot(), original_health);
            send(&worker, call(mode, "job", mode)).await;
            let next = format!("after-{mode}");
            send(&worker, call(&next, "job", "success")).await;
            receive_until(&mut worker, &mut frames, |frame| is_terminal(frame, &next)).await;
            assert_eq!(health.snapshot(), original_health);
        }
        worker.end().await.unwrap();
        let (tail, report) = worker.collect().await.unwrap();
        frames.extend(tail);
        assert_eq!(invocations.load(Ordering::SeqCst), 6);
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 3);
        assert_eq!(report.errors.len(), 3);
        let errors = diagnostics(&frames);
        assert_eq!(errors.len(), 3);
        for item in errors {
            assert_application(item);
        }
        for mode in ["401", "404", "fatal"] {
            let outcome = terminal(&frames, mode);
            assert_eq!(outcome.state(), ToolCallTerminalState::Failed);
            assert_eq!(outcome.reason(), ToolCallEndReason::CallbackError);
            assert_eq!(outcome.effects(), ToolCallEffectStatus::ExternalEffectsUnknown);
            assert_eq!(terminal(&frames, &format!("after-{mode}")).state(), ToolCallTerminalState::Completed);
        }
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 6);
        assert_eq!(settled.unadmitted_terminal, 0);
        assert_eq!(health.snapshot(), original_health);
    })
    .await;
}

#[tokio::test(start_paused = true)]
async fn cancellation_legacy_timeout_capacity_unknown_and_panic_do_not_become_application_errors() {
    bounded(async {
        let entered = Arc::new(Notify::new());
        let called = Arc::new(AtomicUsize::new(0));
        let (signal, count) = (entered.clone(), called.clone());
        let mut executor = ToolExecutor::new(1, 32, 1024).unwrap();
        executor
            .register_direct(
                "typed-cancel",
                LocalToolFunction::new_cancellable(|_| async { None }),
                None,
            )
            .unwrap();
        executor
            .register(
                "legacy",
                RegisteredTool::new(Arc::new(|_| {
                    Box::pin(async { Err("HTTP 401 legacy application-looking string".into()) })
                })),
            )
            .unwrap();
        executor
            .register_direct(
                "panic",
                LocalToolFunction::new(|_| async {
                    panic!("application-looking panic remains a panic settlement");
                }),
                None,
            )
            .unwrap();
        executor
            .register_direct(
                "wait",
                LocalToolFunction::new(move |_| {
                    let (signal, count) = (signal.clone(), count.clone());
                    async move {
                        count.fetch_add(1, Ordering::SeqCst);
                        signal.notify_one();
                        std::future::pending::<Result<serde_json::Value, ProcessorError>>().await
                    }
                }),
                Some(Duration::from_secs(1)),
            )
            .unwrap();
        let journal = executor.outcomes();
        let health = ProcessorHealth::default();
        let mut worker = Pipeline::new(vec![Arc::new(HealthOwner {
            executor,
            health: health.clone(),
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let mut frames = vec![];
        for (id, name, expected) in [
            ("cancel", "typed-cancel", ToolCallEndReason::ExplicitCancel),
            ("legacy", "legacy", ToolCallEndReason::CallbackError),
            ("unknown", "missing", ToolCallEndReason::NotRegistered),
            ("panic", "panic", ToolCallEndReason::CallbackPanicked),
        ] {
            send(&worker, call(id, name, "ignored")).await;
            receive_until(&mut worker, &mut frames, |frame| is_terminal(frame, id)).await;
            assert_eq!(terminal(&frames, id).reason(), expected);
        }
        send(&worker, call("timeout", "wait", "ignored")).await;
        entered.notified().await;
        send(&worker, call("capacity", "wait", "ignored")).await;
        receive_until(&mut worker, &mut frames, |frame| {
            is_terminal(frame, "capacity")
        })
        .await;
        assert_eq!(
            terminal(&frames, "capacity").reason(),
            ToolCallEndReason::ConcurrencyLimit
        );
        tokio::time::advance(Duration::from_secs(1)).await;
        receive_until(&mut worker, &mut frames, |frame| {
            is_terminal(frame, "timeout")
        })
        .await;
        assert_eq!(
            terminal(&frames, "timeout").reason(),
            ToolCallEndReason::TimedOut
        );
        assert_eq!(called.load(Ordering::SeqCst), 1);
        worker.end().await.unwrap();
        let (tail, report) = worker.collect().await.unwrap();
        frames.extend(tail);
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(diagnostics(&frames).is_empty());
        assert!(journal.snapshot().active.is_empty());
        assert_eq!(journal.snapshot().total_terminal, 6);
        assert!(health.is_usable());
    })
    .await;
}

#[tokio::test]
async fn explicit_cancel_wins_and_unicode_diagnostic_is_bounded_without_changing_raw_terminal() {
    bounded(async {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let returned = Arc::new(AtomicUsize::new(0));
        let (signal, gate, count) = (entered.clone(), release.clone(), returned.clone());
        let raw_error = "工具故障🦀".repeat(2000);
        let error = raw_error.clone();
        let mut executor = ToolExecutor::new(1, 16, 1024).unwrap();
        executor
            .register_direct(
                "cancel-error",
                LocalToolFunction::new(move |_| {
                    let (signal, gate, count) = (signal.clone(), gate.clone(), count.clone());
                    async move {
                        signal.notify_one();
                        gate.notified().await;
                        count.fetch_add(1, Ordering::SeqCst);
                        Err(ProcessorError::new("cancelled error must not be diagnosed"))
                    }
                }),
                None,
            )
            .unwrap();
        executor
            .register_direct(
                "unicode",
                LocalToolFunction::new(move |_| {
                    let error = error.clone();
                    async move { Err(ProcessorError::new(error)) }
                }),
                None,
            )
            .unwrap();
        let journal = executor.outcomes();
        let health = ProcessorHealth::default();
        let mut worker = Pipeline::new(vec![Arc::new(HealthOwner {
            executor,
            health: health.clone(),
        })])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        let mut frames = vec![];
        send(&worker, call("cancel-winner", "cancel-error", "ignored")).await;
        entered.notified().await;
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: "cancel-error".into(),
                    tool_call_id: "cancel-winner".into(),
                    run_llm: false,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        receive_until(&mut worker, &mut frames, |frame| {
            is_terminal(frame, "cancel-winner")
        })
        .await;
        assert_eq!(
            terminal(&frames, "cancel-winner").reason(),
            ToolCallEndReason::ExplicitCancel
        );
        release.notify_one();
        send(&worker, call("unicode-error", "unicode", "ignored")).await;
        receive_until(&mut worker, &mut frames, |frame| {
            matches!(&frame.data, FrameData::Error(_))
        })
        .await;
        worker.end().await.unwrap();
        let (tail, report) = worker.collect().await.unwrap();
        frames.extend(tail);
        assert_eq!(returned.load(Ordering::SeqCst), 0);
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(report.total_errors, 1);
        let errors = diagnostics(&frames);
        assert_eq!(errors.len(), 1);
        let error = assert_application(errors[0]);
        assert!(error.error.len() <= 4096);
        let body = error
            .error
            .strip_prefix("Error executing function call [unicode]: ")
            .unwrap();
        let retained = body.strip_suffix("... [truncated]").unwrap();
        assert!(!retained.is_empty());
        assert!(
            raw_error.starts_with(retained),
            "diagnostic changed the retained UTF8 prefix"
        );
        let outcome = terminal(&frames, "unicode-error");
        assert_eq!(outcome.state(), ToolCallTerminalState::Failed);
        assert_eq!(outcome.reason(), ToolCallEndReason::CallbackError);
        let result = frames
            .iter()
            .find_map(|item| match &item.frame.data {
                FrameData::FunctionCallResult(result) if result.tool_call_id == "unicode-error" => {
                    Some(result)
                }
                _ => None,
            })
            .unwrap();
        assert_eq!(result.error.as_deref(), Some(raw_error.as_str()));
        assert!(result.is_final);
        assert!(journal.snapshot().active.is_empty());
        assert_eq!(journal.snapshot().total_terminal, 2);
        assert_eq!(journal.snapshot().unadmitted_terminal, 0);
        assert!(health.is_usable());
    })
    .await;
}
