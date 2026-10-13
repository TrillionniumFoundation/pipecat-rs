use pipecat_core::*;
use pipecat_processors::tools::ToolExecutor;
use serde_json::json;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Weak,
    },
    time::Duration,
};
use tokio::sync::{mpsc, Notify};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("progress context-update ownership did not settle")
}

fn call() -> FunctionCall {
    FunctionCall {
        function_name: "reporting".into(),
        tool_call_id: "progress-hooks".into(),
        arguments: json!({"input":"original"}),
    }
}

fn tracked_hook() -> (LocalAsyncCallback, Weak<()>, Arc<AtomicUsize>) {
    let resource = Arc::new(());
    let weak = Arc::downgrade(&resource);
    let invoked = Arc::new(AtomicUsize::new(0));
    let callback = {
        let invoked = invoked.clone();
        LocalAsyncCallback::new(move || {
            let (resource, invoked) = (resource.clone(), invoked.clone());
            async move {
                invoked.fetch_add(1, Ordering::SeqCst);
                drop(resource);
                Ok(())
            }
        })
    };
    (callback, weak, invoked)
}

async fn start(worker: &PipelineWorker) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call()])),
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
    loop {
        let item = worker.recv().await.expect("worker closed before receipt");
        let done = ready(&item.frame);
        frames.push(item);
        if done {
            return;
        }
    }
}

#[tokio::test]
async fn progress_hook_is_frame_only_and_rejected_reports_do_not_consume_sequence() {
    bounded(async {
        let (callback, weak_callback, invoked) = tracked_hook();
        let release = Arc::new(Notify::new());
        let (ready, mut entered) = mpsc::channel(1);
        let handler = {
            let release = release.clone();
            LocalToolFunction::new_with_reporter(move |arguments, reporter| {
                let (callback, release, ready) = (callback.clone(), release.clone(), ready.clone());
                async move {
                    assert_eq!(arguments, call().arguments);
                    let (oversized_hook, weak_oversized, oversized_invoked) = tracked_hook();
                    let rejected = reporter
                        .report_with_context_updated(
                            LocalToolCompletion::new(LocalToolResult::new(
                                serde_json::Value::String(
                                    "x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES),
                                ),
                            ))
                            .with_context_updated(oversized_hook),
                        )
                        .await
                        .is_err();
                    reporter
                        .report_with_context_updated(
                            LocalToolCompletion::new(
                                LocalToolResult::new(json!("working")).with_run_llm(false),
                            )
                            .with_context_updated(callback),
                        )
                        .await?;
                    ready
                        .send((reporter, rejected, weak_oversized, oversized_invoked))
                        .await
                        .map_err(|_| ProcessorError::new("test receiver closed"))?;
                    release.notified().await;
                    Ok(LocalToolResult::new(json!("done")).with_run_llm(false))
                }
            })
        };
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor
            .register_direct("reporting", handler, None)
            .unwrap();
        let journal = executor.outcomes();
        let mut worker = Pipeline::new(vec![Arc::new(executor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        start(&worker).await;
        let (escaped, rejected, weak_oversized, oversized_invoked) = entered.recv().await.unwrap();
        assert!(rejected);
        assert!(weak_oversized.upgrade().is_none());
        assert_eq!(oversized_invoked.load(Ordering::SeqCst), 0);
        let mut frames = vec![];
        receive_until(&mut worker, &mut frames, |frame| {
            frame.native_tool_progress().unwrap().is_some()
        })
        .await;
        let frame = &frames.last().unwrap().frame;
        let retained_progress = frame.native_tool_progress().unwrap().unwrap().clone();
        assert_eq!(retained_progress.sequence(), 1);
        assert_eq!(retained_progress.call(), &call());
        assert_eq!(
            frame.native_tool_progress_context_updated().unwrap(),
            Some(retained_progress.id())
        );
        assert!(frame.native_tool_context_updated().unwrap().is_none());
        assert!(frame.native_tool_outcome().unwrap().is_none());
        let FrameData::FunctionCallResult(result) = &frame.data else {
            panic!("owned progress must carry its result");
        };
        assert_eq!(result.result, json!("working"));
        assert_eq!(result.run_llm, Some(false));
        assert!(!result.is_final && result.error.is_none());
        assert_eq!(journal.snapshot().active, vec![call()]);
        assert_eq!(journal.snapshot().total_terminal, 0);
        assert_eq!(invoked.load(Ordering::SeqCst), 0);

        release.notify_one();
        receive_until(&mut worker, &mut frames, |frame| {
            frame.native_tool_outcome().unwrap().is_some()
        })
        .await;
        let final_frame = &frames.last().unwrap().frame;
        assert!(final_frame
            .native_tool_progress_context_updated()
            .unwrap()
            .is_none());
        assert!(final_frame.native_tool_context_updated().unwrap().is_none());
        assert_eq!(
            final_frame.native_tool_outcome().unwrap().unwrap().reason(),
            ToolCallEndReason::CallbackReturned
        );
        let (late_hook, weak_late, late_invoked) = tracked_hook();
        assert!(escaped
            .report_with_context_updated(
                LocalToolCompletion::new(LocalToolResult::new(json!("late")))
                    .with_context_updated(late_hook),
            )
            .await
            .is_err());
        assert!(weak_late.upgrade().is_none());
        assert_eq!(late_invoked.load(Ordering::SeqCst), 0);
        worker.end().await.unwrap();
        let (tail, report) = worker.collect().await.unwrap();
        frames.extend(tail);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            frames
                .iter()
                .filter(|item| item
                    .frame
                    .native_tool_progress_context_updated()
                    .unwrap()
                    .is_some())
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|item| item.frame.native_tool_outcome().unwrap().is_some())
                .count(),
            1
        );
        assert_eq!(
            frames
                .iter()
                .filter(|item| matches!(item.frame.data, FrameData::FunctionCallResult(_)))
                .count(),
            2
        );
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 1);
        assert!(
            weak_callback.upgrade().is_some(),
            "only the progress frame owns the hook"
        );
        drop(frames);
        assert!(weak_callback.upgrade().is_none());
        assert_eq!(
            invoked.load(Ordering::SeqCst),
            0,
            "executor must not execute hooks"
        );
        // Retaining provenance, the terminal journal and an escaped endpoint
        // after frame destruction must not retain the progress callback.
        assert_eq!(retained_progress.sequence(), 1);
        assert_eq!(settled.terminal.len(), 1);
        assert_eq!(journal.snapshot().total_terminal, 1);
        drop(escaped);
    })
    .await;
}

#[tokio::test]
async fn failed_progress_admission_releases_hook_and_revokes_the_escaped_endpoint() {
    bounded(async {
        let (ready, mut entered) = mpsc::channel(1);
        let handler = LocalToolFunction::new_with_reporter(move |_, reporter| {
            let ready = ready.clone();
            async move {
                let (callback, weak_callback, invoked) = tracked_hook();
                let rejected = reporter
                    .report_with_context_updated(
                        LocalToolCompletion::new(LocalToolResult::new(json!("working")))
                            .with_context_updated(callback),
                    )
                    .await
                    .is_err();
                ready
                    .send((reporter, rejected, weak_callback, invoked))
                    .await
                    .map_err(|_| ProcessorError::new("test receiver closed"))?;
                std::future::pending::<Result<LocalToolResult, ProcessorError>>().await
            }
        });
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor
            .register_direct("reporting", handler, None)
            .unwrap();
        let journal = executor.outcomes();
        let clock = Arc::new(MonotonicObserverClock::default());
        let weak_clock = Arc::downgrade(&clock);
        let mut worker = Pipeline::new(vec![Arc::new(executor)])
            .with_observer_clock(clock.clone())
            .start(PipelineConfig {
                output_capacity: 2,
                ..Default::default()
            })
            .await
            .unwrap();
        drop(clock);
        assert!(worker.recv().await.unwrap().frame.data.is_start());
        // The declaration and placeholder occupy both output slots before the
        // reporting handler starts. Its real first-hop admission must fail.
        start(&worker).await;
        let (escaped, rejected, weak_callback, invoked) = entered.recv().await.unwrap();
        assert!(rejected, "a full output boundary must reject admission");
        assert!(weak_callback.upgrade().is_none());
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert!(matches!(
            worker.collect().await,
            Err(PipelineError::Backpressure(_))
        ));
        let (late_hook, weak_late, late_invoked) = tracked_hook();
        assert!(escaped
            .report_with_context_updated(
                LocalToolCompletion::new(LocalToolResult::new(json!("after shutdown")))
                    .with_context_updated(late_hook),
            )
            .await
            .is_err());
        assert!(weak_late.upgrade().is_none());
        assert_eq!(late_invoked.load(Ordering::SeqCst), 0);
        assert!(
            weak_clock.upgrade().is_none(),
            "escaped endpoint must not retain runtime"
        );
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 1);
        assert_eq!(settled.terminal.len(), 1);
        let receipt = &settled.terminal[0];
        assert_eq!(receipt.outcome.call(), &call());
        assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Cancelled);
        assert_eq!(receipt.outcome.reason(), ToolCallEndReason::TaskDropped);
        assert_eq!(
            receipt.outcome.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
        assert!(!receipt.outcome.run_llm());
        assert_eq!(receipt.admission, ToolOutcomeAdmission::Closed);
        assert_eq!(settled.unadmitted_terminal, 1);
    })
    .await;
}
