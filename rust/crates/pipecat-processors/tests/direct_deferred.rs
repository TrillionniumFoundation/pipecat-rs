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

#[test]
fn deferred_registration_requires_a_representable_effective_deadline_before_effects() {
    let effects = Arc::new(AtomicUsize::new(0));
    let deferred = || {
        let effects = effects.clone();
        LocalToolFunction::new_deferred(move |_, _| {
            effects.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        })
    };
    let options = |timeout| LocalToolOptions {
        cancel_on_interruption: false,
        timeout,
    };
    let mut executor = ToolExecutor::new(2, 8, 1024).unwrap();
    assert!(executor
        .register_direct("missing", deferred(), None)
        .is_err());
    // A rejected registration must not occupy the name or publish a callback.
    executor
        .register_direct("missing", deferred(), Some(Duration::from_secs(1)))
        .unwrap();
    assert!(executor
        .register_direct("default-max", deferred(), Some(Duration::MAX))
        .is_err());
    assert!(executor
        .register_direct(
            "handler-max",
            deferred()
                .with_options(options(Some(Duration::MAX)))
                .unwrap(),
            None,
        )
        .is_err());
    executor
        .register_direct(
            "handler-finite",
            deferred()
                .with_options(options(Some(Duration::from_secs(1))))
                .unwrap(),
            None,
        )
        .unwrap();
    // Only the effective value is checked: a finite handler deadline overrides
    // an unrepresentable binding default just as it overrides any other default.
    executor
        .register_direct(
            "override-default-max",
            deferred()
                .with_options(options(Some(Duration::from_secs(1))))
                .unwrap(),
            Some(Duration::MAX),
        )
        .unwrap();
    executor
        .register_direct(
            "ordinary",
            LocalToolFunction::new(|_| async { Ok(json!(null)) }),
            None,
        )
        .unwrap();
    executor
        .register_direct(
            "live",
            LocalToolFunction::new_with_reporter(|_, _| async {
                Ok(LocalToolResult::new(json!(null)))
            }),
            None,
        )
        .unwrap();
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(executor.outcomes().snapshot().active.is_empty());
    assert_eq!(executor.outcomes().snapshot().total_terminal, 0);
}

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("owned deferred call did not settle")
}

fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "job".into(),
        tool_call_id: id.into(),
        arguments: json!({"input":"original"}),
    }
}

async fn start(worker: &PipelineWorker, id: &str) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call(id)])),
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

struct DispatchReturned(Arc<Notify>);
impl Drop for DispatchReturned {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}

#[tokio::test]
async fn returned_dispatcher_retains_lease_capacity_and_budget_retry_until_one_final() {
    bounded(async {
        let returned = Arc::new(Notify::new());
        let invocations = Arc::new(AtomicUsize::new(0));
        let (ready, mut endpoints) = mpsc::channel(1);
        let handler = {
            let returned = returned.clone();
            let invocations = invocations.clone();
            LocalToolFunction::new_deferred(move |arguments, responder| {
                let (returned, invocations, ready) =
                    (returned.clone(), invocations.clone(), ready.clone());
                async move {
                    let _returned = DispatchReturned(returned);
                    assert_eq!(arguments, call("held").arguments);
                    invocations.fetch_add(1, Ordering::SeqCst);
                    ready
                        .send(responder)
                        .await
                        .map_err(|_| ProcessorError::new("test receiver closed"))?;
                    Ok(())
                }
            })
        };
        let mut executor = ToolExecutor::new(1, 8, 1024).unwrap();
        executor
            .register_direct("job", handler, Some(Duration::from_secs(5)))
            .unwrap();
        let journal = executor.outcomes();
        let clock = Arc::new(MonotonicObserverClock::default());
        let weak_clock = Arc::downgrade(&clock);
        let mut worker = Pipeline::new(vec![Arc::new(executor)])
            .with_observer_clock(clock.clone())
            .start(PipelineConfig::default())
            .await
            .unwrap();
        drop(clock);
        start(&worker, "held").await;
        let responder = endpoints.recv().await.unwrap();
        returned.notified().await;
        assert_eq!(journal.snapshot().active, vec![call("held")]);
        assert_eq!(journal.snapshot().total_terminal, 0);

        // Dispatch acknowledgment must not release the actual execution permit.
        start(&worker, "busy").await;
        let mut frames = vec![];
        receive_until(&mut worker, &mut frames, |frame| {
            frame
                .native_tool_outcome()
                .unwrap()
                .is_some_and(|outcome| outcome.call().tool_call_id == "busy")
        })
        .await;
        let busy = frames
            .last()
            .unwrap()
            .frame
            .native_tool_outcome()
            .unwrap()
            .unwrap();
        assert_eq!(busy.reason(), ToolCallEndReason::ConcurrencyLimit);
        assert_eq!(busy.effects(), ToolCallEffectStatus::NotStarted);
        assert_eq!(invocations.load(Ordering::SeqCst), 1);
        assert_eq!(journal.snapshot().active, vec![call("held")]);

        assert!(responder
            .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(
                serde_json::Value::String("x".repeat(NativeToolCallBatch::MAX_RETAINED_BYTES)),
            ))))
            .is_err());
        // A rejected final must leave the endpoint open, without consuming an
        // intermediate sequence or settling its original reservation.
        responder
            .report(LocalToolResult::new(json!("working")).with_run_llm(false))
            .await
            .unwrap();
        receive_until(&mut worker, &mut frames, |frame| {
            frame.native_tool_progress().unwrap().is_some()
        })
        .await;
        let retained_progress = frames
            .last()
            .unwrap()
            .frame
            .native_tool_progress()
            .unwrap()
            .unwrap()
            .clone();
        assert_eq!(retained_progress.call(), &call("held"));
        assert_eq!(retained_progress.sequence(), 1);
        assert_eq!(journal.snapshot().active, vec![call("held")]);
        assert_eq!(journal.snapshot().total_terminal, 1);

        let resource = Arc::new(());
        let weak_resource = Arc::downgrade(&resource);
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
        responder
            .finish(Ok(LocalToolCompletion::new(
                LocalToolResult::new(json!("done")).with_run_llm(false),
            )
            .with_context_updated(callback)))
            .unwrap();
        assert!(responder
            .report(LocalToolResult::new(json!("late")))
            .await
            .is_err());
        assert!(responder
            .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(json!(
                "duplicate"
            )))))
            .is_err());
        assert!(responder.cancel().is_err());
        receive_until(&mut worker, &mut frames, |frame| {
            frame
                .native_tool_outcome()
                .unwrap()
                .is_some_and(|outcome| outcome.call().tool_call_id == "held")
        })
        .await;
        let frame = &frames.last().unwrap().frame;
        let outcome = frame.native_tool_outcome().unwrap().unwrap();
        assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
        assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
        assert_eq!(
            frame.native_tool_context_updated().unwrap(),
            Some(outcome.id())
        );
        let FrameData::FunctionCallResult(result) = &frame.data else {
            panic!("deferred final must carry its actual result");
        };
        assert_eq!(result.result, json!("done"));
        assert_eq!(result.run_llm, Some(false));
        assert!(result.is_final && result.error.is_none());
        worker.end().await.unwrap();
        let (tail, report) = worker.collect().await.unwrap();
        frames.extend(tail);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let settled = journal.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 2);
        assert_eq!(settled.terminal.len(), 2);
        assert_eq!(
            frames
                .iter()
                .filter(|item| item.frame.native_tool_outcome().unwrap().is_some())
                .count(),
            2
        );
        assert_eq!(
            frames
                .iter()
                .filter(|item| item.frame.native_tool_progress().unwrap().is_some())
                .count(),
            1
        );
        assert_eq!(invoked.load(Ordering::SeqCst), 0);
        assert!(weak_resource.upgrade().is_some());
        drop(frames);
        assert!(weak_resource.upgrade().is_none());
        assert!(
            weak_clock.upgrade().is_none(),
            "escaped responder must not retain runtime"
        );
        assert_eq!(retained_progress.sequence(), 1);
        assert_eq!(journal.snapshot().total_terminal, 2);
        assert!(responder
            .finish(Err(ProcessorError::new("after close")))
            .is_err());
    })
    .await;
}
