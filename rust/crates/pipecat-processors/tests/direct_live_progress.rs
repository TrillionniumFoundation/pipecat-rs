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
use tokio::sync::{mpsc, Notify};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("live tool progress did not settle")
}

fn call() -> FunctionCall {
    FunctionCall {
        function_name: "reporting".into(),
        tool_call_id: "live-call".into(),
        arguments: json!({"input":"original"}),
    }
}

async fn start_call(worker: &PipelineWorker) {
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
async fn rejected_report_keeps_sequence_and_progress_holds_the_call_until_one_final_receipt() {
    bounded(async {
        let release = Arc::new(Notify::new());
        let invocations = Arc::new(AtomicUsize::new(0));
        let (ready, mut entered) = mpsc::channel(1);
        let handler = {
            let release = release.clone();
            let invocations = invocations.clone();
            LocalToolFunction::new_with_reporter(move |arguments, reporter| {
                let (release, invocations, ready) =
                    (release.clone(), invocations.clone(), ready.clone());
                async move {
                    invocations.fetch_add(1, Ordering::SeqCst);
                    assert_eq!(arguments, call().arguments);
                    let rejected = reporter
                        .report(LocalToolResult::new(serde_json::Value::String(
                            "x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES),
                        )))
                        .await
                        .is_err();
                    reporter
                        .report(LocalToolResult::new(json!("working")).with_run_llm(false))
                        .await?;
                    ready
                        .send((reporter, rejected))
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
        start_call(&worker).await;
        let (escaped, rejected) = entered.recv().await.unwrap();
        assert!(rejected, "the oversized report must fail admission");
        let mut frames = vec![];
        receive_until(&mut worker, &mut frames, |frame| {
            frame.native_tool_progress().unwrap().is_some()
        })
        .await;
        let progress_frame = &frames.last().unwrap().frame;
        let progress = progress_frame.native_tool_progress().unwrap().unwrap();
        assert_eq!(
            progress.sequence(),
            1,
            "rejection must not consume a sequence"
        );
        assert_eq!(progress.call(), &call());
        assert!(!progress.cancel_on_interruption());
        assert_eq!(progress.batch().unwrap().members().len(), 1);
        assert!(progress_frame.native_tool_outcome().unwrap().is_none());
        let FrameData::FunctionCallResult(result) = &progress_frame.data else {
            panic!("progress must carry the actual result");
        };
        assert_eq!(result.result, json!("working"));
        assert_eq!(result.run_llm, Some(false));
        assert!(!result.is_final && result.error.is_none());
        let active = journal.snapshot();
        assert_eq!(active.active, vec![call()]);
        assert_eq!(active.total_terminal, 0);
        assert!(active.terminal.is_empty());

        release.notify_one();
        receive_until(&mut worker, &mut frames, |frame| {
            frame.native_tool_outcome().unwrap().is_some()
        })
        .await;
        let final_frame = &frames.last().unwrap().frame;
        assert!(final_frame.native_tool_progress().unwrap().is_none());
        let final_outcome = final_frame.native_tool_outcome().unwrap().unwrap();
        assert_eq!(final_outcome.call(), &call());
        assert_eq!(final_outcome.state(), ToolCallTerminalState::Completed);
        assert_eq!(final_outcome.reason(), ToolCallEndReason::CallbackReturned);
        assert_eq!(
            final_outcome.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
        let FrameData::FunctionCallResult(result) = &final_frame.data else {
            panic!("final completion must carry the actual result");
        };
        assert_eq!(result.result, json!("done"));
        assert_eq!(result.run_llm, Some(false));
        assert!(result.is_final && result.error.is_none());
        assert!(escaped
            .report(LocalToolResult::new(json!("too late")))
            .await
            .is_err());

        // This normal-lane traversal is an output barrier after the rejected
        // synchronous report; no detached publication can appear after it.
        worker
            .sender()
            .send(
                Frame::new(FrameData::InputText(TextFrame::new("after-final"))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        receive_until(
            &mut worker,
            &mut frames,
            |frame| matches!(&frame.data, FrameData::InputText(text) if text.text == "after-final"),
        )
        .await;
        worker.end().await.unwrap();
        let (tail, report) = worker.collect().await.unwrap();
        frames.extend(tail);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(
            frames
                .iter()
                .filter(|item| item.frame.native_tool_progress().unwrap().is_some())
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
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 1);
        assert_eq!(settled.terminal.len(), 1);
        assert_eq!(
            settled.terminal[0].admission,
            ToolOutcomeAdmission::Admitted
        );
    })
    .await;
}

struct DropSignal(Arc<Notify>);
impl Drop for DropSignal {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn dropping_worker_revokes_escaped_reporter_and_releases_its_runtime() {
    bounded(async {
        let dropped = Arc::new(Notify::new());
        let (ready, mut entered) = mpsc::channel(1);
        let handler = {
            let dropped = dropped.clone();
            LocalToolFunction::new_with_reporter(move |_, reporter| {
                let (dropped, ready) = (dropped.clone(), ready.clone());
                async move {
                    let _drop_signal = DropSignal(dropped);
                    reporter
                        .report(LocalToolResult::new(json!("working")))
                        .await?;
                    ready
                        .send(reporter)
                        .await
                        .map_err(|_| ProcessorError::new("test receiver closed"))?;
                    std::future::pending::<Result<LocalToolResult, ProcessorError>>().await
                }
            })
        };
        let mut executor = ToolExecutor::new(1, 4, 1024).unwrap();
        executor
            .register_direct("reporting", handler, None)
            .unwrap();
        let journal = executor.outcomes();
        let executor = Arc::new(executor);
        let weak_executor = Arc::downgrade(&executor);
        // ObserverHub keeps this clock for the complete lifetime of Runtime,
        // including after shutdown. Its weak release proves runtime release.
        let clock = Arc::new(MonotonicObserverClock::default());
        let weak_clock = Arc::downgrade(&clock);
        let mut worker = Pipeline::new(vec![executor.clone()])
            .with_observer_clock(clock.clone())
            .start(PipelineConfig::default())
            .await
            .unwrap();
        drop(executor);
        drop(clock);
        start_call(&worker).await;
        let escaped = entered.recv().await.unwrap();
        let mut frames = vec![];
        receive_until(&mut worker, &mut frames, |frame| {
            frame.native_tool_progress().unwrap().is_some()
        })
        .await;
        assert_eq!(journal.snapshot().active, vec![call()]);
        assert_eq!(journal.snapshot().total_terminal, 0);
        assert!(weak_clock.upgrade().is_some());

        // Dropping the owner closes the runtime and aborts its pending task;
        // this is task destruction, rather than an explicit tool-cancel command.
        drop(worker);
        dropped.notified().await;
        assert!(escaped
            .report(LocalToolResult::new(json!("after shutdown")))
            .await
            .is_err());
        while journal.snapshot().total_terminal == 0 || weak_clock.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
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
        assert!(weak_executor.upgrade().is_none());
        assert!(weak_clock.upgrade().is_none());
        // Keep both the endpoint and the native progress frame through these
        // ownership checks, then confirm the revoked endpoint remains closed.
        assert!(frames
            .last()
            .unwrap()
            .frame
            .native_tool_progress()
            .unwrap()
            .is_some());
        assert!(escaped
            .report(LocalToolResult::new(json!("still closed")))
            .await
            .is_err());
        assert_eq!(journal.snapshot().total_terminal, 1);
    })
    .await;
}
