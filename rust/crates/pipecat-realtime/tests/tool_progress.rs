//! Full native reporting qualification through a real local WebSocket session.
//! Every progress carrier/receipt below originates a real registered callback.
#[path = "support/tool_progress_fixture.rs"]
mod fixture;

use fixture::*;
use pipecat_core::*;
use pipecat_processors::tools::{
    RegisteredTool, ReportingTool, ReportingToolCallback, ToolExecutor,
};
use pipecat_realtime::{
    OpenAiRealtimeProcessor, OpenAiRealtimeToolProgressGate, OpenAiRealtimeToolProgressRelay,
};
use pipecat_services::Context;
use pipecat_turns::{LLMContextAggregatorPair, RealtimeServiceMode};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use tokio::{
    sync::{mpsc, Semaphore},
    time::timeout,
};

struct Harness {
    worker: PipelineWorker,
    peer: Peer,
    pair: LLMContextAggregatorPair,
    route: NativeToolProgressRoute,
    before: Arc<Boundary>,
    before_held: mpsc::UnboundedReceiver<HeldFrame>,
    after: Arc<Boundary>,
    after_held: mpsc::UnboundedReceiver<HeldFrame>,
    terminals: mpsc::UnboundedReceiver<Frame>,
    executor: Arc<ToolExecutor>,
    executor_node: Arc<ExecutorWitness>,
    provider: Arc<OpenAiRealtimeProcessor>,
    provider_events: mpsc::UnboundedReceiver<AssistantObservation>,
    assistant_events: mpsc::UnboundedReceiver<AssistantObservation>,
}
impl Harness {
    async fn new(callback: ReportingToolCallback, hold_before: bool, hold_after: bool) -> Self {
        Self::with_response_timeout(callback, hold_before, hold_after, RESPONSE_TIMEOUT).await
    }
    async fn with_response_timeout(
        callback: ReportingToolCallback,
        hold_before: bool,
        hold_after: bool,
        response_timeout: std::time::Duration,
    ) -> Self {
        Self::with_tools(callback, hold_before, hold_after, response_timeout, None).await
    }
    async fn with_tools(
        callback: ReportingToolCallback,
        hold_before: bool,
        hold_after: bool,
        response_timeout: std::time::Duration,
        ordinary: Option<RegisteredTool>,
    ) -> Self {
        let route = NativeToolProgressRoute::new(NativeToolProgressConfig {
            end_drain_timeout: END_DRAIN,
        })
        .unwrap();
        let (peer, client) = Peer::with_response_timeout(response_timeout).await;
        let provider = Arc::new(
            OpenAiRealtimeProcessor::new(client)
                .unwrap()
                .with_progress_route(route.clone()),
        );
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap();
        pair.bind_tool_progress(route.clone()).unwrap();
        let mut executor = ToolExecutor::new(2, 32, 4096)
            .unwrap()
            .with_progress_route(route.clone())
            .unwrap();
        let mut tool = ReportingTool::new(callback);
        tool.timeout = CALLBACK_TIMEOUT;
        executor.register_reporting("work", tool).unwrap();
        if let Some(tool) = ordinary {
            executor.register("ordinary", tool).unwrap();
        }
        let executor = Arc::new(executor);
        let executor_node = ExecutorWitness::new(executor.clone());
        let (before, before_held, _) = Boundary::new(hold_before);
        let (after, after_held, terminals) = Boundary::new(hold_after);
        let (assistant, assistant_events) = AssistantWitness::new(pair.assistant());
        let (provider_node, provider_events) = AssistantWitness::new(provider.clone());
        let mut pipeline_config = pipeline_config();
        pipeline_config.shutdown_timeout = END_DRAIN
            + response_timeout
            + OPERATION_TIMEOUT * 2
            + CLOSE_TIMEOUT
            + std::time::Duration::from_secs(5);
        let worker = Pipeline::new(vec![
            Arc::new(OpenAiRealtimeToolProgressGate::new(route.clone())),
            pair.user(),
            provider_node,
            executor_node.clone(),
            before.clone(),
            Arc::new(OpenAiRealtimeToolProgressRelay::new(route.clone())),
            after.clone(),
            assistant,
        ])
        .start(pipeline_config)
        .await
        .unwrap();
        assert!(
            !provider.selected_session_joined(),
            "a connected live actor must not already claim its JoinHandle was awaited"
        );
        Self {
            worker,
            peer,
            pair,
            route,
            before,
            before_held,
            after,
            after_held,
            terminals,
            executor,
            executor_node,
            provider,
            provider_events,
            assistant_events,
        }
    }
    async fn call(&mut self, id: &str) {
        barrier(&mut self.worker).await;
        self.peer.response_start("initial-tool-response");
        assistant_observed(
            &mut self.assistant_events,
            AssistantObservation::ResponseStarted,
        )
        .await;
        self.peer.call(
            "initial-tool-response",
            id,
            "work",
            0,
            json!({"genuine":true}),
        );
    }
    async fn close_initial_response(&mut self) {
        self.peer.response_end("initial-tool-response");
        assistant_observed(
            &mut self.assistant_events,
            AssistantObservation::ResponseEnded,
        )
        .await;
    }
    async fn finish_response(&mut self, id: &str) {
        self.peer.response_start(id);
        self.peer.response_end(id);
        assistant_observed(
            &mut self.assistant_events,
            AssistantObservation::ResponseEnded,
        )
        .await;
        barrier(&mut self.worker).await;
    }
    async fn finish(self) {
        let provider = self.provider;
        ended(self.worker, self.peer, &self.route).await;
        assert!(
            provider.selected_session_joined(),
            "selected actor JoinHandle must actually be awaited, including graceful End"
        );
    }
}

/// Polling and dropping a genuine receipt waiter proves it is still pending;
/// neither a sleep nor a fixture callback can complete history consumption.
async fn assert_history_pending(receipt: &ToolProgressReceipt) {
    assert!(receipt.history().is_none());
    let wait = receipt.await_history();
    tokio::pin!(wait);
    assert!(futures_util::poll!(&mut wait).is_pending());
}

#[tokio::test]
async fn real_callback_progress_waits_for_actual_history_and_responds_before_final() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (committed_tx, mut committed) = mpsc::unbounded_channel();
    let next_report = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let returned = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let next_report = next_report.clone();
        let finish = finish.clone();
        let returned = returned.clone();
        Arc::new(move |call, reporter| {
            assert_eq!(call.arguments, json!({"genuine":true}));
            let receipt_tx = receipt_tx.clone();
            let committed_tx = committed_tx.clone();
            let next_report = next_report.clone();
            let finish = finish.clone();
            let returned = returned.clone();
            Box::pin(async move {
                for (index, result) in [json!({"part":1}), json!({"part":2})]
                    .into_iter()
                    .enumerate()
                {
                    if index != 0 {
                        next_report.acquire().await.unwrap().forget();
                    }
                    let receipt = reporter
                        .report(result, Some(true))
                        .map_err(|error| error.to_string())?;
                    receipt_tx.send(receipt.clone()).unwrap();
                    let history = receipt
                        .await_history()
                        .await
                        .map_err(|error| format!("{error:?}"))?;
                    committed_tx.send(history).unwrap();
                }
                finish.acquire().await.unwrap().forget();
                returned.store(true, Ordering::SeqCst);
                Ok(json!({"complete":true}))
            })
        })
    };
    let mut h = Harness::new(callback, true, true).await;
    h.call("progress-call").await;
    let first = channel(&mut receipts).await;
    assert_eq!(first.ordinal(), 1);
    let before_first = channel(&mut h.before_held).await;
    before_first.deliver().await;
    let held_first = channel(&mut h.after_held).await;
    assert_eq!(
        held_first
            .frame
            .native_tool_progress()
            .unwrap()
            .unwrap()
            .id(),
        first.event_id()
    );
    assert_progress_command(
        &h.peer.substantive().await,
        "progress-call",
        &json!({"part":1}),
    );
    assert_eq!(h.peer.formal_outputs("progress-call"), 0);
    assert!(result_envelopes(&h.pair, "progress-call").is_empty());
    assert_history_pending(&first).await;
    assert!(committed.try_recv().is_err());
    h.close_initial_response().await;
    h.peer.quiet().await;

    held_first.deliver().await;
    let first_commit = channel(&mut committed).await;
    assert_eq!(first.await_history().await.unwrap(), first_commit);
    assert_eq!(
        first.await_history().await.unwrap(),
        first_commit,
        "receipt is repeatably observable after a dropped pending waiter"
    );
    assert_eq!(result_envelopes(&h.pair, "progress-call").len(), 1);
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    assert!(
        !returned.load(Ordering::SeqCst),
        "first progress must answer while the registered callback still owns its final result"
    );
    assert_eq!(finish.available_permits(), 0);
    h.finish_response("progress-answer-one").await;

    next_report.add_permits(1);
    let second = channel(&mut receipts).await;
    let before_second = channel(&mut h.before_held).await;
    before_second.deliver().await;
    let held_second = channel(&mut h.after_held).await;
    assert_eq!(second.ordinal(), 2);
    assert_ne!(first.event_id(), second.event_id());
    assert_progress_command(
        &h.peer.substantive().await,
        "progress-call",
        &json!({"part":2}),
    );
    assert_history_pending(&second).await;
    held_second.deliver().await;
    let second_commit = channel(&mut committed).await;
    assert!(second_commit.revision > first_commit.revision);
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    assert!(!returned.load(Ordering::SeqCst));
    h.finish_response("progress-answer-two").await;

    // Re-submit the exact actual callback event through the real relay and both
    // consumers. No public fake frame or private test mint is involved.
    h.after.hold_progress.store(false, Ordering::SeqCst);
    before_second.deliver().await;
    barrier(&mut h.worker).await;
    assert_eq!(second.await_history().await.unwrap(), second_commit);
    assert_eq!(result_envelopes(&h.pair, "progress-call").len(), 2);
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("progress-call"), 0);

    finish.add_permits(1);
    let terminal = channel(&mut h.terminals).await;
    assert_eq!(
        terminal
            .native_tool_outcome()
            .unwrap()
            .unwrap()
            .progress_terminal()
            .unwrap()
            .predecessor_ordinal(),
        2
    );
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], "progress-call");
    assert_eq!(output["item"]["output"], "{\"complete\":true}");
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    h.finish_response("final-answer").await;
    assert!(returned.load(Ordering::SeqCst));
    let history = envelopes(&h.pair, "progress-call");
    assert_eq!(
        history.len(),
        4,
        "started, two real progress commits, and a deferred final"
    );
    assert_eq!(history[0]["status"], "running");
    assert!(
        history[0].get("result").is_none(),
        "deferred completion must not rewrite the started placeholder"
    );
    assert_eq!(history[3]["status"], "finished");
    assert_eq!(history[3]["result"], "{\"complete\":true}");
    assert_eq!(h.peer.formal_outputs("progress-call"), 1);
    let journal = h.executor.outcomes().snapshot();
    assert_eq!(journal.total_terminal, 1);
    assert!(journal.terminal[0].outcome.is_journal_projection());
    assert!(journal.terminal[0].outcome.progress_terminal().is_none());
    assert_eq!(
        journal.terminal[0]
            .outcome
            .progress_journal_summary()
            .unwrap()
            .predecessor_ordinal,
        2
    );
    h.peer.quiet().await;
    drop(held_first);
    drop(held_second);
    drop(before_first);
    drop(before_second);
    h.finish().await;
}

#[tokio::test]
async fn no_run_report_receipt_commits_without_any_inference_and_survives_waiter_drop() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            Box::pin(async move {
                let receipt = reporter
                    .report(json!(false), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    h.call("silent-call").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.after_held).await;
    assert_progress_command(&h.peer.substantive().await, "silent-call", &json!(false));
    assert_history_pending(&receipt).await;
    h.close_initial_response().await;
    held.deliver().await;
    let commit = timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    assert!(commit.end > commit.start);
    barrier(&mut h.worker).await;
    assert_eq!(
        result_envelopes(&h.pair, "silent-call")[0]["result"],
        "false"
    );
    h.peer.quiet().await;
    finish.add_permits(1);
    channel(&mut h.terminals).await;
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["output"], "COMPLETED");
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(receipt.await_history().await.unwrap(), commit);
    assert_eq!(h.peer.formal_outputs("silent-call"), 1);
    drop(held);
    h.finish().await;
}

struct CallbackDrop(Arc<AtomicBool>);
impl Drop for CallbackDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

async fn await_route_state(route: &NativeToolProgressRoute, wanted: NativeToolProgressRouteState) {
    timeout(DEADLINE, async {
        loop {
            let changed = route.changed();
            tokio::pin!(changed);
            // Register the actual route notification before inspecting state.
            let ready = futures_util::poll!(&mut changed).is_ready();
            if route.state() == wanted {
                return;
            }
            if !ready {
                changed.await;
            }
        }
    })
    .await
    .expect("selected route state transition deadline");
}

#[tokio::test]
async fn real_end_drains_live_reports_and_final_but_suppresses_all_new_inference() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (reporter_tx, mut reporters) = mpsc::unbounded_channel();
    let next_report = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let next_report = next_report.clone();
        let finish = finish.clone();
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let reporter_tx = reporter_tx.clone();
            let next_report = next_report.clone();
            let finish = finish.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _lifetime = CallbackDrop(dropped);
                reporter_tx.send(reporter.clone()).unwrap();
                for index in 1..=2 {
                    if index == 2 {
                        next_report.acquire().await.unwrap().forget();
                    }
                    let receipt = reporter
                        .report(json!({"part":index}), Some(true))
                        .map_err(|error| error.to_string())?;
                    receipt_tx.send(receipt.clone()).unwrap();
                    receipt
                        .await_history()
                        .await
                        .map_err(|error| format!("{error:?}"))?;
                }
                finish.acquire().await.unwrap().forget();
                Ok(json!("finished during End"))
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    h.call("end-drain-call").await;
    let reporter = channel(&mut reporters).await;
    let first = channel(&mut receipts).await;
    let first_held = channel(&mut h.after_held).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "end-drain-call",
        &json!({"part":1}),
    );
    h.close_initial_response().await;
    h.worker.end().await.unwrap();
    await_route_state(&h.route, NativeToolProgressRouteState::Draining).await;
    assert!(!h.route.accepts_new_calls());
    assert!(!h.route.inference_allowed());
    assert!(
        !h.worker.has_finished(),
        "a live reporting lease cannot be mistaken for a drained route"
    );
    assert_history_pending(&first).await;
    assert!(!dropped.load(Ordering::SeqCst));
    first_held.deliver().await;
    let first_commit = timeout(DEADLINE, first.await_history())
        .await
        .unwrap()
        .unwrap();
    next_report.add_permits(1);
    let second = channel(&mut receipts).await;
    let second_held = channel(&mut h.after_held).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "end-drain-call",
        &json!({"part":2}),
    );
    second_held.deliver().await;
    timeout(DEADLINE, second.await_history())
        .await
        .unwrap()
        .unwrap();
    assert!(!h.worker.has_finished());
    h.peer.quiet().await;
    finish.add_permits(1);
    let terminal = channel(&mut h.terminals).await;
    assert_eq!(
        terminal
            .native_tool_outcome()
            .unwrap()
            .unwrap()
            .progress_terminal()
            .unwrap()
            .predecessor_ordinal(),
        2
    );
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["output"], "\"finished during End\"");
    drop(first_held);
    drop(second_held);
    let report = timeout(DEADLINE, h.worker.join())
        .await
        .expect("End must actually join all pipeline tasks")
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(h.route.is_closed());
    assert!(
        h.provider.selected_session_joined(),
        "pipeline join must include the actual selected actor JoinHandle await"
    );
    h.peer.join().await;
    assert_eq!(h.peer.formal_outputs("end-drain-call"), 1);
    assert!(!h
        .peer
        .observed
        .iter()
        .any(|command| command["type"] == "response.create"));
    assert_eq!(first.await_history().await.unwrap(), first_commit);
    assert!(reporter.report(json!("too late"), Some(true)).is_err());
}

#[tokio::test]
async fn selected_request_stop_supersedes_held_end_with_real_stop_and_joins_callback() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _lifetime = CallbackDrop(dropped);
                let receipt = reporter
                    .report(json!("wire-before-stop"), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                std::future::pending::<Result<Value, String>>().await
            })
        })
    };
    let mut h =
        Harness::with_response_timeout(callback, false, true, std::time::Duration::from_secs(120))
            .await;
    h.call("stop-call").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.after_held).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "stop-call",
        &json!("wire-before-stop"),
    );
    // Keep the real provider response active: selected Stop must use immediate
    // close, rather than waiting for graceful response.done/response watchdog.
    h.worker.end().await.unwrap();
    await_route_state(&h.route, NativeToolProgressRouteState::Draining).await;
    assert_eq!(
        h.route.request_stop().unwrap(),
        NativeToolProgressStopResult::Requested
    );
    assert_eq!(
        h.route.request_stop().unwrap(),
        NativeToolProgressStopResult::AlreadyRequested
    );
    assert!(timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .is_err());
    let report = timeout(DEADLINE, h.worker.join())
        .await
        .expect("active-response selected Stop must not wait for the response watchdog")
        .unwrap();
    assert_eq!(report.state, WorkerState::Stopped);
    assert!(
        dropped.load(Ordering::SeqCst),
        "PipelineWorker join must include the owned callback cancellation"
    );
    assert!(h.route.is_closed());
    let mut ends = 0;
    let mut stops = 0;
    while let Ok(frame) = h.worker.try_recv() {
        match frame.frame.data {
            FrameData::End(_) | FrameData::EndDetailed(_) => ends += 1,
            FrameData::Stop => stops += 1,
            _ => (),
        }
    }
    assert_eq!(
        (stops, ends),
        (1, 0),
        "held End must be replaced by an actual Stop, never a synthetic success or Cancel"
    );
    assert!(result_envelopes(&h.pair, "stop-call").is_empty());
    assert!(
        h.provider.selected_session_joined(),
        "pipeline join must include the actual selected actor JoinHandle await"
    );
    h.peer.join().await;
    assert!(!h
        .peer
        .observed
        .iter()
        .any(|command| command["type"] == "response.create"));
    // A late real carrier is still authentic, but closure must not create a new
    // append, a new socket command, or a success-shaped history receipt.
    let _ = held.context.push(held.frame.clone(), held.direction).await;
    assert!(receipt.history().unwrap().is_err());
    assert!(result_envelopes(&h.pair, "stop-call").is_empty());
}

#[tokio::test]
async fn real_cancel_fails_pending_receipt_and_does_not_claim_assistant_commit() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _lifetime = CallbackDrop(dropped);
                let receipt = reporter
                    .report(json!("only provider consumed"), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                std::future::pending::<Result<Value, String>>().await
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    h.call("cancel-route-call").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.after_held).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "cancel-route-call",
        &json!("only provider consumed"),
    );
    assert_history_pending(&receipt).await;
    h.worker.cancel("real selected cancellation").await.unwrap();
    assert!(timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .is_err());
    let report = timeout(DEADLINE, h.worker.join())
        .await
        .expect("actual Cancel task join")
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(dropped.load(Ordering::SeqCst));
    assert!(h.route.is_closed());
    assert!(result_envelopes(&h.pair, "cancel-route-call").is_empty());
    assert!(
        h.provider.selected_session_joined(),
        "pipeline join must include the actual selected actor JoinHandle await"
    );
    h.peer.join().await;
    assert_eq!(
        h.peer.formal_outputs("cancel-route-call"),
        0,
        "pipeline cancellation is not a remote terminal settlement receipt"
    );
    let _ = held.context.push(held.frame.clone(), held.direction).await;
    assert!(receipt.history().unwrap().is_err());
    assert!(result_envelopes(&h.pair, "cancel-route-call").is_empty());
}

#[tokio::test]
async fn interruption_preserves_old_progress_history_but_only_new_report_can_request_inference() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let next_report = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let next_report = next_report.clone();
        let finish = finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let next_report = next_report.clone();
            let finish = finish.clone();
            Box::pin(async move {
                for index in 1..=2 {
                    if index == 2 {
                        next_report.acquire().await.unwrap().forget();
                    }
                    let receipt = reporter
                        .report(json!(index), Some(true))
                        .map_err(|error| error.to_string())?;
                    receipt_tx.send(receipt.clone()).unwrap();
                    receipt
                        .await_history()
                        .await
                        .map_err(|error| format!("{error:?}"))?;
                }
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    h.call("scope-call").await;
    let old = channel(&mut receipts).await;
    let old_held = channel(&mut h.after_held).await;
    assert_progress_command(&h.peer.substantive().await, "scope-call", &json!(1));
    h.close_initial_response().await;
    inject(&h.worker, FrameData::Interruption).await;
    assistant_observed(&mut h.assistant_events, AssistantObservation::Interrupted).await;
    assert_eq!(
        h.peer.substantive().await["type"],
        "input_audio_buffer.clear"
    );
    assert!(!old_held
        .frame
        .native_tool_progress()
        .unwrap()
        .unwrap()
        .publication_fence()
        .is_current());
    old_held.deliver().await;
    let old_commit = timeout(DEADLINE, old.await_history())
        .await
        .unwrap()
        .unwrap();
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(result_envelopes(&h.pair, "scope-call")[0]["result"], "1");

    next_report.add_permits(1);
    let fresh = channel(&mut receipts).await;
    let fresh_held = channel(&mut h.after_held).await;
    assert_eq!(fresh.ordinal(), 2);
    assert!(fresh_held
        .frame
        .native_tool_progress()
        .unwrap()
        .unwrap()
        .publication_fence()
        .is_current());
    assert_progress_command(&h.peer.substantive().await, "scope-call", &json!(2));
    fresh_held.deliver().await;
    let fresh_commit = timeout(DEADLINE, fresh.await_history())
        .await
        .unwrap()
        .unwrap();
    assert!(fresh_commit.revision > old_commit.revision);
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    assert_eq!(finish.available_permits(), 0);
    h.finish_response("fresh-progress-answer").await;
    finish.add_permits(1);
    channel(&mut h.terminals).await;
    assert_eq!(
        h.peer.substantive().await["item"]["type"],
        "function_call_output"
    );
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    drop(old_held);
    drop(fresh_held);
    h.finish().await;
}

#[tokio::test]
async fn explicit_tool_cancel_after_commit_revokes_held_real_continuation_without_erasing_receipt()
{
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _lifetime = CallbackDrop(dropped);
                let receipt = reporter
                    .report(json!("committed before cancel"), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                std::future::pending::<Result<Value, String>>().await
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.after.hold_continuation.store(true, Ordering::SeqCst);
    h.call("cancel-permission-call").await;
    let receipt = channel(&mut receipts).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "cancel-permission-call",
        &json!("committed before cancel"),
    );
    let commit = timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    h.close_initial_response().await;
    let held_continuation = channel(&mut h.after_held).await;
    assert_eq!(held_continuation.direction, Direction::Upstream);
    inject(
        &h.worker,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "work".into(),
            tool_call_id: "cancel-permission-call".into(),
            run_llm: false,
        }),
    )
    .await;
    channel(&mut h.terminals).await;
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["output"], "CANCELLED");
    assert!(!held_continuation
        .frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .progress_permission()
        .unwrap()
        .is_allowed());
    held_continuation.deliver().await;
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(receipt.await_history().await.unwrap(), commit);
    let history = envelopes(&h.pair, "cancel-permission-call");
    assert_eq!(history.len(), 3);
    assert_eq!(history[1]["result"], "\"committed before cancel\"");
    assert_eq!(history[2]["status"], "finished");
    assert!(history[2]["description"]
        .as_str()
        .unwrap()
        .contains("cancelled"));
    drop(held_continuation);
    h.finish().await;
}

#[tokio::test]
async fn system_cancel_overtakes_two_reports_but_both_consumers_wait_for_contiguous_watermark() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _lifetime = CallbackDrop(dropped);
                for index in 1..=2 {
                    receipt_tx
                        .send(
                            reporter
                                .report(json!({"ordered":index}), Some(true))
                                .map_err(|error| error.to_string())?,
                        )
                        .unwrap();
                }
                std::future::pending::<Result<Value, String>>().await
            })
        })
    };
    let mut h = Harness::new(callback, true, true).await;
    h.call("watermark-call").await;
    let first = channel(&mut receipts).await;
    let second = channel(&mut receipts).await;
    let before_first = channel(&mut h.before_held).await;
    let before_second = channel(&mut h.before_held).await;
    h.close_initial_response().await;
    inject(
        &h.worker,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "work".into(),
            tool_call_id: "watermark-call".into(),
            run_llm: false,
        }),
    )
    .await;
    let terminal = channel(&mut h.terminals).await;
    let outcome = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(
        outcome.progress_terminal().unwrap().predecessor_ordinal(),
        2
    );
    assert!(matches!(terminal.data, FrameData::FunctionCallCancel(_)));
    assert!(dropped.load(Ordering::SeqCst));
    assert_history_pending(&first).await;
    assert_history_pending(&second).await;
    assert_eq!(h.peer.formal_outputs("watermark-call"), 0);
    assert!(result_envelopes(&h.pair, "watermark-call").is_empty());
    h.peer.quiet().await;

    // Let ordinal 2 reach both consumers first. The genuine provider has no
    // progress/terminal command to emit until ordinal 1 arrives.
    before_second.deliver().await;
    let after_second = channel(&mut h.after_held).await;
    h.peer.quiet().await;
    before_first.deliver().await;
    let after_first = channel(&mut h.after_held).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "watermark-call",
        &json!({"ordered":1}),
    );
    assert_progress_command(
        &h.peer.substantive().await,
        "watermark-call",
        &json!({"ordered":2}),
    );
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["output"], "CANCELLED");
    assert!(result_envelopes(&h.pair, "watermark-call").is_empty());
    assert_history_pending(&first).await;
    assert_history_pending(&second).await;

    after_second.deliver().await;
    barrier(&mut h.worker).await;
    assert_history_pending(&second).await;
    after_first.deliver().await;
    let first_commit = timeout(DEADLINE, first.await_history())
        .await
        .unwrap()
        .unwrap();
    let second_commit = timeout(DEADLINE, second.await_history())
        .await
        .unwrap()
        .unwrap();
    assert!(second_commit.revision > first_commit.revision);
    barrier(&mut h.worker).await;
    let history = envelopes(&h.pair, "watermark-call");
    assert_eq!(history.len(), 4);
    assert_eq!(history[1]["result"], "{\"ordered\":1}");
    assert_eq!(history[2]["result"], "{\"ordered\":2}");
    assert_eq!(history[3]["status"], "finished");
    assert_eq!(h.peer.formal_outputs("watermark-call"), 1);
    h.peer.quiet().await;
    drop((before_first, before_second, after_first, after_second));
    h.finish().await;
}

#[tokio::test]
async fn report_saturation_and_oversize_rejection_consume_no_ordinal_or_terminal_reservation() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (rejected_tx, mut rejected) = mpsc::unbounded_channel();
    let retry = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let retry = retry.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let rejected_tx = rejected_tx.clone();
            let retry = retry.clone();
            Box::pin(async move {
                assert!(reporter
                    .report(json!("x".repeat(17 * 1024)), Some(false))
                    .is_err());
                for index in 1..=4 {
                    receipt_tx
                        .send(
                            reporter
                                .report(json!(index), Some(false))
                                .map_err(|error| error.to_string())?,
                        )
                        .unwrap();
                }
                assert!(reporter
                    .report(json!("pending fifth"), Some(false))
                    .is_err());
                rejected_tx.send(()).unwrap();
                retry.acquire().await.unwrap().forget();
                receipt_tx
                    .send(
                        reporter
                            .report(json!(5), Some(false))
                            .map_err(|error| error.to_string())?,
                    )
                    .unwrap();
                // Four reports remain uncommitted. Their permits cannot consume
                // the independently reserved terminal result/watermark slot.
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    h.call("capacity-call").await;
    let mut admitted = Vec::new();
    let mut held = Vec::new();
    for ordinal in 1..=4 {
        let receipt = channel(&mut receipts).await;
        assert_eq!(receipt.ordinal(), ordinal);
        admitted.push(receipt);
        held.push(channel(&mut h.after_held).await);
        assert_progress_command(
            &h.peer.substantive().await,
            "capacity-call",
            &json!(ordinal),
        );
    }
    channel(&mut rejected).await;
    h.close_initial_response().await;
    held[0].deliver().await;
    timeout(DEADLINE, admitted[0].await_history())
        .await
        .unwrap()
        .unwrap();
    retry.add_permits(1);
    let fifth = channel(&mut receipts).await;
    assert_eq!(
        fifth.ordinal(),
        5,
        "neither an oversized value nor a saturated attempt allocates an ordinal"
    );
    held.push(channel(&mut h.after_held).await);
    assert_progress_command(&h.peer.substantive().await, "capacity-call", &json!(5));
    admitted.push(fifth);
    let terminal = channel(&mut h.terminals).await;
    assert_eq!(
        terminal
            .native_tool_outcome()
            .unwrap()
            .unwrap()
            .progress_terminal()
            .unwrap()
            .predecessor_ordinal(),
        5
    );
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["output"], "COMPLETED");
    assert_eq!(h.executor.outcomes().snapshot().total_terminal, 1);
    for receipt in &admitted[1..] {
        assert_history_pending(receipt).await;
    }
    for event in &held[1..] {
        event.deliver().await;
    }
    for receipt in &admitted {
        timeout(DEADLINE, receipt.await_history())
            .await
            .unwrap()
            .unwrap();
    }
    barrier(&mut h.worker).await;
    assert_eq!(envelopes(&h.pair, "capacity-call").len(), 7);
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("capacity-call"), 1);
    drop(held);
    h.finish().await;
}

#[tokio::test]
async fn native_null_and_falsy_progress_values_commit_their_exact_no_run_semantics() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let values = [
        Value::Null,
        json!(false),
        json!(0),
        json!(""),
        json!([]),
        json!({}),
    ];
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        let values = values.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            let values = values.clone();
            Box::pin(async move {
                for value in values {
                    let receipt = reporter
                        .report(value, Some(false))
                        .map_err(|error| error.to_string())?;
                    receipt_tx.send(receipt.clone()).unwrap();
                    receipt
                        .await_history()
                        .await
                        .map_err(|error| format!("{error:?}"))?;
                }
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.call("value-call").await;
    for (index, value) in values.iter().enumerate() {
        let receipt = channel(&mut receipts).await;
        assert_eq!(receipt.ordinal(), index as u64 + 1);
        assert_progress_command(&h.peer.substantive().await, "value-call", value);
        timeout(DEADLINE, receipt.await_history())
            .await
            .unwrap()
            .unwrap();
        let expected = if value.is_null() {
            "COMPLETED".into()
        } else {
            value.to_string()
        };
        assert_eq!(
            result_envelopes(&h.pair, "value-call")[index]["result"],
            expected
        );
    }
    h.close_initial_response().await;
    h.peer.quiet().await;
    finish.add_permits(1);
    channel(&mut h.terminals).await;
    assert_eq!(h.peer.substantive().await["item"]["output"], "COMPLETED");
    barrier(&mut h.worker).await;
    assert_eq!(envelopes(&h.pair, "value-call").len(), 8);
    h.peer.quiet().await;
    h.finish().await;
}

#[tokio::test]
async fn inference_delivery_failure_after_real_commit_preserves_history_receipt() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _lifetime = CallbackDrop(dropped);
                let receipt = reporter
                    .report(json!("committed fact"), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                std::future::pending::<Result<Value, String>>().await
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    h.call("failure-after-commit").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.after_held).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "failure-after-commit",
        &json!("committed fact"),
    );
    h.close_initial_response().await;
    h.after.fail_continuation.store(true, Ordering::SeqCst);
    held.deliver().await;
    let commit = timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    let joined = timeout(DEADLINE, h.worker.join())
        .await
        .expect("failed real pipeline must join");
    assert!(
        joined.is_err()
            || joined
                .as_ref()
                .is_ok_and(|report| report.state == WorkerState::Failed)
    );
    assert_eq!(h.worker.state(), WorkerState::Failed);
    assert!(h.route.is_closed());
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(
        receipt.history(),
        Some(Ok(commit)),
        "inference failure cannot replace the actual history-committed fact"
    );
    assert_eq!(receipt.await_history().await.unwrap(), commit);
    assert_eq!(
        result_envelopes(&h.pair, "failure-after-commit")[0]["result"],
        "\"committed fact\""
    );
    assert!(
        h.provider.selected_session_joined(),
        "pipeline join must include the actual selected actor JoinHandle await"
    );
    h.peer.join().await;
    assert!(!h
        .peer
        .observed
        .iter()
        .any(|command| command["type"] == "response.create"));
}

#[tokio::test]
async fn permanently_missing_predecessor_expires_without_waiter_or_false_terminal_settlement() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let callback: ReportingToolCallback = Arc::new(move |_, reporter| {
        let receipt_tx = receipt_tx.clone();
        Box::pin(async move {
            receipt_tx
                .send(
                    reporter
                        .report(json!("never delivered"), Some(false))
                        .map_err(|error| error.to_string())?,
                )
                .unwrap();
            Ok(Value::Null)
        })
    });
    // The owner disposition deadline (5 s) must win before the original provider
    // response/settlement watchdog (8 s). No receipt waiter owns this deadline.
    let mut h =
        Harness::with_response_timeout(callback, true, false, std::time::Duration::from_secs(8))
            .await;
    h.call("missing-predecessor").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.before_held).await;
    let terminal = channel(&mut h.terminals).await;
    assert_eq!(
        terminal
            .native_tool_outcome()
            .unwrap()
            .unwrap()
            .progress_terminal()
            .unwrap()
            .predecessor_ordinal(),
        1
    );
    h.close_initial_response().await;
    h.peer.quiet().await;
    assert!(receipt.history().is_none());
    timeout(std::time::Duration::from_secs(6), async {
        loop {
            let changed = h.route.changed();
            tokio::pin!(changed);
            let ready = futures_util::poll!(&mut changed).is_ready();
            if h.route.is_closed() {
                break;
            }
            if !ready {
                changed.await;
            }
        }
    })
    .await
    .expect("one absolute owner disposition deadline must close the gap");
    assert_eq!(
        receipt.history(),
        Some(Err(NativeToolProgressFailure::Deadline))
    );
    assert!(result_envelopes(&h.pair, "missing-predecessor").is_empty());
    let joined = timeout(DEADLINE, h.worker.join())
        .await
        .expect("gap failure must actually join the pipeline");
    assert!(
        joined.is_err()
            || joined
                .as_ref()
                .is_ok_and(|report| report.state == WorkerState::Failed)
    );
    assert!(
        h.provider.selected_session_joined(),
        "pipeline join must include the actual selected actor JoinHandle await"
    );
    h.peer.join().await;
    assert_eq!(h.peer.formal_outputs("missing-predecessor"), 0);
    assert!(!h
        .peer
        .observed
        .iter()
        .any(|command| command["type"] == "response.create"));
    drop(held);
}

#[tokio::test]
async fn ordinary_open_sibling_blocks_progress_response_until_its_real_terminal() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (ordinary_tx, mut ordinary_entered) = mpsc::unbounded_channel();
    let report_now = Arc::new(Semaphore::new(0));
    let reporting_finish = Arc::new(Semaphore::new(0));
    let ordinary_finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let report_now = report_now.clone();
        let reporting_finish = reporting_finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let report_now = report_now.clone();
            let reporting_finish = reporting_finish.clone();
            Box::pin(async move {
                report_now.acquire().await.unwrap().forget();
                let receipt = reporter
                    .report(json!("asynchronous partial"), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                reporting_finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut ordinary = {
        let ordinary_finish = ordinary_finish.clone();
        RegisteredTool::new(Arc::new(move |_| {
            let ordinary_tx = ordinary_tx.clone();
            let ordinary_finish = ordinary_finish.clone();
            Box::pin(async move {
                ordinary_tx.send(()).unwrap();
                ordinary_finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        }))
    };
    ordinary.timeout = CALLBACK_TIMEOUT;
    let mut h = Harness::with_tools(callback, false, false, RESPONSE_TIMEOUT, Some(ordinary)).await;
    h.call("async-sibling").await;
    h.peer.call(
        "initial-tool-response",
        "blocking-sibling",
        "ordinary",
        1,
        json!({}),
    );
    channel(&mut ordinary_entered).await;
    h.close_initial_response().await;
    report_now.add_permits(1);
    let receipt = channel(&mut receipts).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "async-sibling",
        &json!("asynchronous partial"),
    );
    timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(h.executor.outcomes().snapshot().active.len(), 2);
    ordinary_finish.add_permits(1);
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], "blocking-sibling");
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    assert_eq!(h.executor.outcomes().snapshot().active.len(), 1);
    assert_eq!(reporting_finish.available_permits(), 0);
    h.finish_response("after-blocking-sibling").await;
    reporting_finish.add_permits(1);
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["call_id"], "async-sibling");
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("blocking-sibling"), 1);
    assert_eq!(h.peer.formal_outputs("async-sibling"), 1);
    h.finish().await;
}

#[tokio::test]
async fn actual_assistant_playback_gate_defers_progress_response_but_not_history_receipt() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            Box::pin(async move {
                let receipt = reporter
                    .report(json!("ready during playback"), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    inject(&h.worker, FrameData::BotStartedSpeaking).await;
    barrier(&mut h.worker).await;
    h.call("playback-call").await;
    let receipt = channel(&mut receipts).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "playback-call",
        &json!("ready during playback"),
    );
    let commit = timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    h.close_initial_response().await;
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("playback-call"), 0);
    inject(&h.worker, FrameData::BotStoppedSpeaking).await;
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    assert_eq!(finish.available_permits(), 0);
    assert_eq!(receipt.await_history().await.unwrap(), commit);
    h.finish_response("after-playback").await;
    finish.add_permits(1);
    channel(&mut h.terminals).await;
    assert_eq!(h.peer.substantive().await["item"]["output"], "COMPLETED");
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    h.finish().await;
}

#[tokio::test]
async fn ordinary_user_json_cannot_impersonate_an_attributed_sibling_history_message() {
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        Arc::new(move |_, _| {
            let entered_tx = entered_tx.clone();
            let finish = finish.clone();
            Box::pin(async move {
                entered_tx.send(()).unwrap();
                finish.acquire().await.unwrap().forget();
                Ok(json!("real final"))
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.call("own-call").await;
    channel(&mut entered).await;
    h.close_initial_response().await;
    barrier(&mut h.worker).await;
    let masquerade = json!({"type":"async_tool","tool_call_id":"not-a-real-sibling","status":"running","result":"looks native"}).to_string();
    inject(
        &h.worker,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, masquerade.clone())],
            run_llm: false,
        }),
    )
    .await;
    let ordinary = h.peer.substantive().await;
    assert_eq!(ordinary["item"]["role"], "user");
    assert_eq!(ordinary["item"]["content"][0]["text"], masquerade);
    barrier(&mut h.worker).await;
    finish.add_permits(1);
    channel(&mut h.terminals).await;
    let final_output = h.peer.substantive().await;
    assert_eq!(final_output["item"]["type"], "function_call_output");
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    h.finish_response("after-masquerade").await;
    let history = envelopes(&h.pair, "own-call");
    assert_eq!(
        history.len(),
        2,
        "ordinary user text after the started placeholder must force a deferred final"
    );
    assert_eq!(history[0]["status"], "running");
    assert!(history[0].get("result").is_none());
    assert_eq!(history[1]["status"], "finished");
    assert_eq!(history[1]["result"], "\"real final\"");
    assert!(h.pair.context().snapshot().unwrap().messages.iter().any(|message|
        message.role == pipecat_services::Role::User
            && matches!(&message.content, Some(pipecat_services::Content::Text(text)) if text == &masquerade)));
    h.finish().await;
}

#[tokio::test]
async fn mutated_real_progress_carrier_cannot_fall_back_to_raw_provider_or_history_acceptance() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _guard = CallbackDrop(dropped);
                let receipt = reporter
                    .report(json!("authentic payload"), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, true, false).await;
    h.call("mutated-call").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.before_held).await;
    h.close_initial_response().await;
    let generation = h.provider.sender().unwrap().generation();
    let mut changed = held.frame.clone();
    let FrameData::FunctionCallResult(result) = &mut changed.data else {
        panic!("native progress must be a result");
    };
    result.result = json!("forged replacement");
    let expected_rejection = changed.native_tool_progress().unwrap_err();
    // Actual routing admission rejects the inconsistent private carrier before
    // it can mutate any queue, callback, history, or invocation epoch.
    let rejection = held
        .context
        .push(changed, Direction::Downstream)
        .await
        .expect_err("mutated report must be rejected at actual route admission");
    assert_eq!(rejection, expected_rejection);
    barrier(&mut h.worker).await;
    assert_eq!(h.worker.state(), WorkerState::Running);
    assert_eq!(h.provider.sender().unwrap().generation(), generation);
    assert!(held
        .frame
        .native_tool_progress()
        .unwrap()
        .unwrap()
        .publication_fence()
        .is_current());
    assert!(!dropped.load(Ordering::SeqCst));
    assert_eq!(h.executor.outcomes().snapshot().active.len(), 1);
    assert_eq!(h.executor.outcomes().snapshot().total_terminal, 0);
    assert_history_pending(&receipt).await;
    assert!(result_envelopes(&h.pair, "mutated-call").is_empty());
    h.peer.quiet().await;

    held.deliver().await;
    assert_progress_command(
        &h.peer.substantive().await,
        "mutated-call",
        &json!("authentic payload"),
    );
    timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    assert!(!dropped.load(Ordering::SeqCst));
    finish.add_permits(1);
    let terminal = h.peer.substantive().await;
    assert_eq!(terminal["item"]["call_id"], "mutated-call");
    assert_eq!(terminal["item"]["output"], "COMPLETED");
    assistant_observed(&mut h.assistant_events, AssistantObservation::ToolTerminal).await;
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(h.peer.formal_outputs("mutated-call"), 1);
    assert_eq!(result_envelopes(&h.pair, "mutated-call").len(), 2);
    h.peer.quiet().await;
    h.finish().await;
}

#[tokio::test]
async fn cancellation_cannot_rewrite_an_already_frozen_success_with_held_terminal_delivery() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            Box::pin(async move {
                let receipt = reporter
                    .report(json!("earlier progress"), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                finish.acquire().await.unwrap().forget();
                Ok(json!("frozen success"))
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.before.hold_terminal.store(true, Ordering::SeqCst);
    h.call("frozen-call").await;
    let receipt = channel(&mut receipts).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "frozen-call",
        &json!("earlier progress"),
    );
    timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    h.close_initial_response().await;
    finish.add_permits(1);
    let held_terminal = channel(&mut h.before_held).await;
    let frozen_id = held_terminal
        .frame
        .native_tool_outcome()
        .unwrap()
        .unwrap()
        .id();
    assert_eq!(
        held_terminal
            .frame
            .native_tool_outcome()
            .unwrap()
            .unwrap()
            .reason(),
        ToolCallEndReason::CallbackReturned
    );
    inject(
        &h.worker,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "work".into(),
            tool_call_id: "frozen-call".into(),
            run_llm: false,
        }),
    )
    .await;
    barrier(&mut h.worker).await;
    let journal = h.executor.outcomes().snapshot();
    assert_eq!(journal.total_terminal, 1);
    assert_eq!(journal.terminal[0].outcome.id(), frozen_id);
    assert_eq!(
        journal.terminal[0].outcome.reason(),
        ToolCallEndReason::CallbackReturned
    );
    h.peer.quiet().await;
    held_terminal.deliver().await;
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["output"], "\"frozen success\"");
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    h.finish_response("frozen-final-answer").await;
    assert_eq!(h.peer.formal_outputs("frozen-call"), 1);
    assert_eq!(
        envelopes(&h.pair, "frozen-call").last().unwrap()["result"],
        "\"frozen success\""
    );
    drop(held_terminal);
    h.finish().await;
}

#[tokio::test]
async fn new_provider_call_after_end_freeze_is_fatal_before_callback_entry() {
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let executions = Arc::new(AtomicUsize::new(0));
    let callback: ReportingToolCallback = {
        let executions = executions.clone();
        Arc::new(move |_, _| {
            let entered_tx = entered_tx.clone();
            let executions = executions.clone();
            Box::pin(async move {
                executions.fetch_add(1, Ordering::SeqCst);
                entered_tx.send(()).unwrap();
                std::future::pending::<Result<Value, String>>().await
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.call("accepted-before-End").await;
    channel(&mut entered).await;
    h.close_initial_response().await;
    h.worker.end().await.unwrap();
    await_route_state(&h.route, NativeToolProgressRouteState::Draining).await;
    h.peer.response_start("unrequested-after-End");
    h.peer.call(
        "unrequested-after-End",
        "forbidden-after-End",
        "work",
        0,
        json!({"too_late":true}),
    );
    let joined = timeout(DEADLINE, h.worker.join())
        .await
        .expect("post-End provider announcement must fail the selected session promptly");
    assert!(
        joined.is_err()
            || joined
                .as_ref()
                .is_ok_and(|report| report.state == WorkerState::Failed)
    );
    assert_eq!(h.worker.state(), WorkerState::Failed);
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    assert!(entered.try_recv().is_err());
    assert!(h.route.is_closed());
    assert!(
        h.executor
            .outcomes()
            .snapshot()
            .terminal
            .iter()
            .all(|receipt| receipt.outcome.call().tool_call_id != "forbidden-after-End"),
        "never-admitted provider work must not acquire a fabricated executor settlement"
    );
    assert!(
        h.provider.selected_session_joined(),
        "pipeline join must include the actual selected actor JoinHandle await"
    );
    h.peer.join().await;
    assert_eq!(h.peer.formal_outputs("forbidden-after-End"), 0);
    let mut success_end = false;
    while let Ok(frame) = h.worker.try_recv() {
        success_end |= matches!(
            frame.frame.data,
            FrameData::End(_) | FrameData::EndDetailed(_)
        );
    }
    assert!(
        !success_end,
        "a failed held drain cannot later forward a success-shaped End"
    );
}

#[tokio::test]
async fn insufficient_selected_shutdown_budget_is_rejected_before_connect_or_dispatch() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = pipecat_realtime::RealtimeConfig::new("unused-selected-test-key");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.response_timeout = RESPONSE_TIMEOUT;
    config.limits.operation_timeout = OPERATION_TIMEOUT;
    config.limits.close_timeout = CLOSE_TIMEOUT;
    let route = NativeToolProgressRoute::new(NativeToolProgressConfig {
        end_drain_timeout: END_DRAIN,
    })
    .unwrap();
    let provider = Arc::new(
        OpenAiRealtimeProcessor::new(pipecat_realtime::RealtimeClient::new(config).unwrap())
            .unwrap()
            .with_progress_route(route.clone()),
    );
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    pair.bind_tool_progress(route.clone()).unwrap();
    let entered = Arc::new(AtomicBool::new(false));
    let flag = entered.clone();
    let mut tool = ReportingTool::new(Arc::new(move |_, _| {
        flag.store(true, Ordering::SeqCst);
        Box::pin(async { Ok(Value::Null) })
    }));
    tool.timeout = CALLBACK_TIMEOUT;
    let mut executor = ToolExecutor::new(2, 32, 4096)
        .unwrap()
        .with_progress_route(route.clone())
        .unwrap();
    executor.register_reporting("work", tool).unwrap();
    let mut pipeline_config = pipeline_config();
    pipeline_config.shutdown_timeout = SHUTDOWN_TIMEOUT - std::time::Duration::from_millis(1);
    let started = Pipeline::new(vec![
        Arc::new(OpenAiRealtimeToolProgressGate::new(route.clone())),
        pair.user(),
        provider,
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolProgressRelay::new(route.clone())),
        pair.assistant(),
    ])
    .start(pipeline_config)
    .await;
    assert!(
        started.is_err(),
        "the exact 25-second formula must not be silently weakened"
    );
    assert!(!entered.load(Ordering::SeqCst));
    assert!(route.is_closed());
    assert!(
        timeout(std::time::Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "selected setup validation must precede socket creation"
    );
}

#[tokio::test]
async fn a_fresh_report_never_refreshes_an_expired_inherited_foreign_origin() {
    let (source_tx, mut source_rx) = mpsc::unbounded_channel();
    let mut source = Pipeline::new(vec![Arc::new(CaptureInvocation(source_tx))])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    inject(
        &source,
        FrameData::Text(TextFrame::new("actual independent origin")),
    )
    .await;
    let source_context = channel(&mut source_rx).await;
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let next_report = Arc::new(Semaphore::new(0));
    let finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let next_report = next_report.clone();
        let finish = finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let next_report = next_report.clone();
            let finish = finish.clone();
            Box::pin(async move {
                for index in 1..=2 {
                    if index == 2 {
                        next_report.acquire().await.unwrap().forget();
                    }
                    let receipt = reporter
                        .report(json!(index), Some(true))
                        .map_err(|error| error.to_string())?;
                    receipt_tx.send(receipt.clone()).unwrap();
                    receipt
                        .await_history()
                        .await
                        .map_err(|error| format!("{error:?}"))?;
                }
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, true).await;
    *h.executor_node.foreign.lock().unwrap() = Some(source_context.clone());
    let provider_generation = h.provider.sender().unwrap().generation();
    h.call("foreign-origin-call").await;
    let first = channel(&mut receipts).await;
    let first_held = channel(&mut h.after_held).await;
    assert!(first_held
        .frame
        .native_tool_progress()
        .unwrap()
        .unwrap()
        .publication_fence()
        .is_current());
    assert_progress_command(
        &h.peer.substantive().await,
        "foreign-origin-call",
        &json!(1),
    );
    h.close_initial_response().await;
    inject(&source, FrameData::Interruption).await;
    barrier(&mut source).await;
    assert_eq!(
        h.provider.sender().unwrap().generation(),
        provider_generation
    );
    assert!(!first_held
        .frame
        .native_tool_progress()
        .unwrap()
        .unwrap()
        .publication_fence()
        .is_current());
    first_held.deliver().await;
    timeout(DEADLINE, first.await_history())
        .await
        .unwrap()
        .unwrap();
    next_report.add_permits(1);
    let second = channel(&mut receipts).await;
    let second_held = channel(&mut h.after_held).await;
    assert_eq!(second.ordinal(), 2);
    assert!(
        !second_held
            .frame
            .native_tool_progress()
            .unwrap()
            .unwrap()
            .publication_fence()
            .is_current(),
        "new local publication must preserve the original independently stale source"
    );
    assert_progress_command(
        &h.peer.substantive().await,
        "foreign-origin-call",
        &json!(2),
    );
    second_held.deliver().await;
    timeout(DEADLINE, second.await_history())
        .await
        .unwrap()
        .unwrap();
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(result_envelopes(&h.pair, "foreign-origin-call").len(), 2);
    finish.add_permits(1);
    channel(&mut h.terminals).await;
    assert_eq!(h.peer.substantive().await["item"]["output"], "COMPLETED");
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    drop((first_held, second_held, source_context));
    h.finish().await;
    source.end().await.unwrap();
    let report = timeout(DEADLINE, source.join()).await.unwrap().unwrap();
    assert_eq!(report.state, WorkerState::Ended);
}

#[tokio::test]
async fn progress_clone_changed_to_cancel_is_revalidated_before_executor_cancellation() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _guard = CallbackDrop(dropped);
                let receipt = reporter
                    .report(json!("not cancellation authority"), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, true, false).await;
    h.call("not-cancel-call").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.before_held).await;
    h.close_initial_response().await;
    let generation = h.provider.sender().unwrap().generation();
    let mut forged_cancel = held.frame.clone();
    forged_cancel.data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
        function_name: "work".into(),
        tool_call_id: "not-cancel-call".into(),
        run_llm: false,
    });
    // Exercise the real executor's self-queue admission. Reject before a
    // progress clone can become an actual cancellation or bump an epoch.
    let rejection = h
        .executor_node
        .queue(forged_cancel)
        .await
        .expect_err("conflicting carrier must be rejected at actual queue admission");
    assert_eq!(rejection, ProcessorError::from(QueueError::Closed));
    barrier(&mut h.worker).await;
    assert_eq!(h.worker.state(), WorkerState::Running);
    assert_eq!(h.provider.sender().unwrap().generation(), generation);
    assert!(held
        .frame
        .native_tool_progress()
        .unwrap()
        .unwrap()
        .publication_fence()
        .is_current());
    assert!(!dropped.load(Ordering::SeqCst));
    assert_eq!(h.executor.outcomes().snapshot().active.len(), 1);
    assert_eq!(h.executor.outcomes().snapshot().total_terminal, 0);
    assert_history_pending(&receipt).await;
    assert!(result_envelopes(&h.pair, "not-cancel-call").is_empty());
    h.peer.quiet().await;

    held.deliver().await;
    assert_progress_command(
        &h.peer.substantive().await,
        "not-cancel-call",
        &json!("not cancellation authority"),
    );
    timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    assert!(!dropped.load(Ordering::SeqCst));
    finish.add_permits(1);
    let terminal = h.peer.substantive().await;
    assert_eq!(terminal["item"]["call_id"], "not-cancel-call");
    assert_eq!(terminal["item"]["output"], "COMPLETED");
    assistant_observed(&mut h.assistant_events, AssistantObservation::ToolTerminal).await;
    assert!(dropped.load(Ordering::SeqCst));
    assert_eq!(
        h.executor.outcomes().snapshot().terminal[0]
            .outcome
            .reason(),
        ToolCallEndReason::CallbackReturned
    );
    assert_eq!(h.peer.formal_outputs("not-cancel-call"), 1);
    assert_eq!(result_envelopes(&h.pair, "not-cancel-call").len(), 2);
    h.peer.quiet().await;
    h.finish().await;
}

#[tokio::test]
async fn actual_no_run_history_capacity_fails_after_provider_delivery_and_preserves_prior_commit() {
    let route = NativeToolProgressRoute::new(NativeToolProgressConfig {
        end_drain_timeout: END_DRAIN,
    })
    .unwrap();
    let (mut peer, client) = Peer::with_command_capacity(RESPONSE_TIMEOUT, 2048).await;
    let initial: Vec<_> = (0..1021)
        .map(|index| ChatMessage::new(ChatRole::User, format!("old-{index}")))
        .collect();
    let seed = LlmContextFrame {
        messages: initial.clone(),
        ..Default::default()
    };
    let provider = Arc::new(
        OpenAiRealtimeProcessor::new(client)
            .unwrap()
            .with_initial_context(seed.into())
            .unwrap()
            .with_progress_route(route.clone()),
    );
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::new(
            initial
                .iter()
                .map(|message| {
                    pipecat_services::Message::text(
                        pipecat_services::Role::User,
                        message.content.clone(),
                    )
                })
                .collect(),
        ),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    pair.bind_tool_progress(route.clone()).unwrap();
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let second_report = Arc::new(Semaphore::new(0));
    let next = second_report.clone();
    let mut tool = ReportingTool::new(Arc::new(move |_, reporter| {
        let receipt_tx = receipt_tx.clone();
        let next = next.clone();
        Box::pin(async move {
            for index in 1..=2 {
                if index == 2 {
                    next.acquire().await.unwrap().forget();
                }
                let receipt = reporter
                    .report(json!(index), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
            }
            std::future::pending::<Result<Value, String>>().await
        })
    }));
    tool.timeout = CALLBACK_TIMEOUT;
    let mut executor = ToolExecutor::new(2, 32, 4096)
        .unwrap()
        .with_progress_route(route.clone())
        .unwrap();
    executor.register_reporting("work", tool).unwrap();
    let (boundary, mut held, _) = Boundary::new(true);
    let (assistant, mut assistant_events) = AssistantWitness::new(pair.assistant());
    let mut worker = Pipeline::new(vec![
        Arc::new(OpenAiRealtimeToolProgressGate::new(route.clone())),
        pair.user(),
        provider.clone(),
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolProgressRelay::new(route.clone())),
        boundary,
        assistant,
    ])
    .start(pipeline_config())
    .await
    .unwrap();
    // Existing provider seeding deliberately folds a saved conversation into
    // one user item. Validate the entire canonical 1021-message JSON payload,
    // while the real shared pair retains 1021 distinct history entries.
    let seed_command = peer.substantive().await;
    assert_eq!(seed_command["type"], "conversation.item.create");
    assert_eq!(seed_command["item"]["role"], "user");
    let seed_text = seed_command["item"]["content"][0]["text"].as_str().unwrap();
    let prefix = "\n        This is a previously saved conversation. Please treat this conversation history as a\n        starting point for the current conversation.\n\n";
    let suffix = "\n\n\n        This is the end of the previously saved conversation. Please continue the conversation\n        from here. If the last message is a user instruction or question, act on that instruction\n        or answer the question. If the last message is an assistant response, simple say that you\n        are ready to continue the conversation.";
    let saved: Value = serde_json::from_str(
        seed_text
            .strip_prefix(prefix)
            .unwrap()
            .strip_suffix(suffix)
            .unwrap(),
    )
    .unwrap();
    let expected = Value::Array(
        (0..1021)
            .map(|index| json!({"role":"user", "content": format!("old-{index}")}))
            .collect(),
    );
    assert_eq!(saved, expected);
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1021);
    barrier(&mut worker).await;
    peer.response_start("capacity-origin");
    assistant_observed(&mut assistant_events, AssistantObservation::ResponseStarted).await;
    peer.call("capacity-origin", "history-capacity", "work", 0, json!({}));
    let first = channel(&mut receipts).await;
    let first_held = channel(&mut held).await;
    assert_progress_command(&peer.substantive().await, "history-capacity", &json!(1));
    first_held.deliver().await;
    let commit = timeout(DEADLINE, first.await_history())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1024);
    peer.response_end("capacity-origin");
    assistant_observed(&mut assistant_events, AssistantObservation::ResponseEnded).await;
    second_report.add_permits(1);
    let second = channel(&mut receipts).await;
    let second_held = channel(&mut held).await;
    assert_progress_command(&peer.substantive().await, "history-capacity", &json!(2));
    assert_history_pending(&second).await;
    second_held.deliver().await;
    assert!(timeout(DEADLINE, second.await_history())
        .await
        .unwrap()
        .is_err());
    let joined = timeout(DEADLINE, worker.join())
        .await
        .expect("real history capacity failure must close and join");
    assert!(
        joined.is_err()
            || joined
                .as_ref()
                .is_ok_and(|report| report.state == WorkerState::Failed)
    );
    assert!(route.is_closed());
    assert_eq!(
        pair.context().snapshot().unwrap().messages.len(),
        1024,
        "the failed second append must be atomic even though the provider already received it"
    );
    assert_eq!(first.await_history().await.unwrap(), commit);
    assert_eq!(result_envelopes(&pair, "history-capacity").len(), 1);
    assert!(
        provider.selected_session_joined(),
        "history failure must await the actual selected actor JoinHandle"
    );
    peer.join().await;
    assert_eq!(peer.formal_outputs("history-capacity"), 0);
    assert!(!peer
        .observed
        .iter()
        .any(|command| command["type"] == "response.create"));
}

#[tokio::test]
async fn authenticated_nonreporting_async_sibling_allows_before_final_progress_response() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (ordinary_tx, mut ordinary_entered) = mpsc::unbounded_channel();
    let report_now = Arc::new(Semaphore::new(0));
    let reporting_finish = Arc::new(Semaphore::new(0));
    let ordinary_finish = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let report_now = report_now.clone();
        let reporting_finish = reporting_finish.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let report_now = report_now.clone();
            let reporting_finish = reporting_finish.clone();
            Box::pin(async move {
                report_now.acquire().await.unwrap().forget();
                let receipt = reporter
                    .report(json!("both calls asynchronous"), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                reporting_finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut ordinary = {
        let ordinary_finish = ordinary_finish.clone();
        RegisteredTool::new(Arc::new(move |_| {
            let ordinary_tx = ordinary_tx.clone();
            let ordinary_finish = ordinary_finish.clone();
            Box::pin(async move {
                ordinary_tx.send(()).unwrap();
                ordinary_finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        }))
    };
    ordinary.timeout = CALLBACK_TIMEOUT;
    ordinary.cancel_on_interruption = false;
    let mut h = Harness::with_tools(callback, false, false, RESPONSE_TIMEOUT, Some(ordinary)).await;
    h.call("reporting-async-sibling").await;
    h.peer.call(
        "initial-tool-response",
        "ordinary-async-sibling",
        "ordinary",
        1,
        json!({}),
    );
    channel(&mut ordinary_entered).await;
    h.close_initial_response().await;
    report_now.add_permits(1);
    let receipt = channel(&mut receipts).await;
    assert_progress_command(
        &h.peer.substantive().await,
        "reporting-async-sibling",
        &json!("both calls asynchronous"),
    );
    timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(h.peer.substantive().await["type"], "response.create",
        "actual admitted asynchronous policy, not reporting-registration presence, controls the open-call exemption");
    assert_eq!(h.executor.outcomes().snapshot().active.len(), 2);
    assert_eq!(h.peer.formal_outputs("ordinary-async-sibling"), 0);
    assert_eq!(h.peer.formal_outputs("reporting-async-sibling"), 0);
    assert_eq!(ordinary_finish.available_permits(), 0);
    assert_eq!(reporting_finish.available_permits(), 0);
    h.finish_response("both-asynchronous-answer").await;
    ordinary_finish.add_permits(1);
    assert_eq!(
        h.peer.substantive().await["item"]["call_id"],
        "ordinary-async-sibling"
    );
    reporting_finish.add_permits(1);
    assert_eq!(
        h.peer.substantive().await["item"]["call_id"],
        "reporting-async-sibling"
    );
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("ordinary-async-sibling"), 1);
    assert_eq!(h.peer.formal_outputs("reporting-async-sibling"), 1);
    h.finish().await;
}

#[tokio::test]
async fn cancelling_newer_lease_debt_preserves_earlier_live_progress_response_permission() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let report_a = Arc::new(Semaphore::new(0));
    let report_b = Arc::new(Semaphore::new(0));
    let finish_a = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let report_a = report_a.clone();
        let report_b = report_b.clone();
        let finish_a = finish_a.clone();
        Arc::new(move |call, reporter| {
            let receipt_tx = receipt_tx.clone();
            let entered_tx = entered_tx.clone();
            let is_a = call.tool_call_id == "debt-A";
            let report_now = if is_a {
                report_a.clone()
            } else {
                report_b.clone()
            };
            let finish_a = finish_a.clone();
            Box::pin(async move {
                entered_tx.send(call.tool_call_id.clone()).unwrap();
                report_now.acquire().await.unwrap().forget();
                let receipt = reporter
                    .report(json!(call.tool_call_id), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                if is_a {
                    finish_a.acquire().await.unwrap().forget();
                    Ok(Value::Null)
                } else {
                    std::future::pending::<Result<Value, String>>().await
                }
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.after.hold_continuation.store(true, Ordering::SeqCst);
    h.call("debt-A").await;
    assert_eq!(channel(&mut entered).await, "debt-A");
    h.peer
        .call("initial-tool-response", "debt-B", "work", 1, json!({}));
    assert_eq!(channel(&mut entered).await, "debt-B");
    h.close_initial_response().await;
    let mut continuations = Vec::new();
    let mut commits = Vec::new();
    for (id, report_now) in [("debt-A", report_a), ("debt-B", report_b)] {
        report_now.add_permits(1);
        let receipt = channel(&mut receipts).await;
        assert_progress_command(&h.peer.substantive().await, id, &json!(id));
        let commit = timeout(DEADLINE, receipt.await_history())
            .await
            .unwrap()
            .unwrap();
        let continuation = channel(&mut h.after_held).await;
        assert_eq!(continuation.direction, Direction::Upstream);
        continuations.push(continuation);
        commits.push((receipt, commit));
    }
    // Real response.done made the assistant eligible to transfer both debts.
    // Start a genuine new provider response before admitting those retained
    // contexts, so the actor must hold both alternatives while it is busy.
    h.peer.response_start("debt-busy-response");
    assistant_observed(
        &mut h.assistant_events,
        AssistantObservation::ResponseStarted,
    )
    .await;
    for continuation in &continuations {
        continuation.deliver().await;
        assistant_observed(&mut h.provider_events, AssistantObservation::Continuation).await;
        // This real actor command is FIFO after actual provider consumption of
        // the sealed continuation. The new provider response remains active.
        h.provider
            .sender()
            .unwrap()
            .send(pipecat_realtime::Action::UpdateSession(
                pipecat_realtime::SessionProperties::default(),
            ))
            .await
            .unwrap();
        assert_eq!(h.peer.command().await["type"], "session.update");
        h.peer.quiet().await;
    }
    inject(
        &h.worker,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "work".into(),
            tool_call_id: "debt-B".into(),
            run_llm: false,
        }),
    )
    .await;
    channel(&mut h.terminals).await;
    let cancelled = h.peer.substantive().await;
    assert_eq!(cancelled["item"]["call_id"], "debt-B");
    assert_eq!(cancelled["item"]["output"], "CANCELLED");
    assert!(continuations[0]
        .frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .progress_permission()
        .unwrap()
        .is_allowed());
    assert!(!continuations[1]
        .frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .progress_permission()
        .unwrap()
        .is_allowed());
    h.peer.response_end("debt-busy-response");
    assistant_observed(&mut h.assistant_events, AssistantObservation::ResponseEnded).await;
    assert_eq!(
        h.peer.substantive().await["type"],
        "response.create",
        "retiring newer B must not erase independently valid owed work from A"
    );
    assert_eq!(finish_a.available_permits(), 0);
    h.finish_response("remaining-A-answer").await;
    h.peer.quiet().await;
    for (receipt, commit) in commits {
        assert_eq!(receipt.await_history().await.unwrap(), commit);
    }
    finish_a.add_permits(1);
    assert_eq!(h.peer.substantive().await["item"]["call_id"], "debt-A");
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(
        h.peer
            .observed
            .iter()
            .filter(|command| command["type"] == "response.create")
            .count(),
        1
    );
    assert_eq!(h.peer.formal_outputs("debt-A"), 1);
    assert_eq!(h.peer.formal_outputs("debt-B"), 1);
    drop(continuations);
    h.finish().await;
}

#[tokio::test]
async fn cancelling_latest_debt_after_playback_coalescing_keeps_earlier_transferred_permission() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let report_a = Arc::new(Semaphore::new(0));
    let report_b = Arc::new(Semaphore::new(0));
    let finish_a = Arc::new(Semaphore::new(0));
    let callback: ReportingToolCallback = {
        let report_a = report_a.clone();
        let report_b = report_b.clone();
        let finish_a = finish_a.clone();
        Arc::new(move |call, reporter| {
            let receipt_tx = receipt_tx.clone();
            let entered_tx = entered_tx.clone();
            let is_a = call.tool_call_id == "coalesced-A";
            let report_now = if is_a {
                report_a.clone()
            } else {
                report_b.clone()
            };
            let finish_a = finish_a.clone();
            Box::pin(async move {
                entered_tx.send(call.tool_call_id.clone()).unwrap();
                report_now.acquire().await.unwrap().forget();
                let receipt = reporter
                    .report(json!(call.tool_call_id), Some(true))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                if is_a {
                    finish_a.acquire().await.unwrap().forget();
                    Ok(Value::Null)
                } else {
                    std::future::pending::<Result<Value, String>>().await
                }
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.after.hold_continuation.store(true, Ordering::SeqCst);
    inject(&h.worker, FrameData::BotStartedSpeaking).await;
    barrier(&mut h.worker).await;
    h.call("coalesced-A").await;
    assert_eq!(channel(&mut entered).await, "coalesced-A");
    h.peer
        .call("initial-tool-response", "coalesced-B", "work", 1, json!({}));
    assert_eq!(channel(&mut entered).await, "coalesced-B");
    let mut committed = Vec::new();
    for (id, gate) in [("coalesced-A", report_a), ("coalesced-B", report_b)] {
        gate.add_permits(1);
        let receipt = channel(&mut receipts).await;
        assert_progress_command(&h.peer.substantive().await, id, &json!(id));
        let commit = timeout(DEADLINE, receipt.await_history())
            .await
            .unwrap()
            .unwrap();
        committed.push((receipt, commit));
    }
    h.close_initial_response().await;
    barrier(&mut h.worker).await;
    assert!(
        h.after_held.try_recv().is_err(),
        "actual playback must hold inference debt at the assistant"
    );
    inject(&h.worker, FrameData::BotStoppedSpeaking).await;
    assistant_observed(
        &mut h.assistant_events,
        AssistantObservation::PlaybackStopped,
    )
    .await;
    barrier(&mut h.worker).await;
    let mut transferred = Vec::new();
    while let Ok(context) = h.after_held.try_recv() {
        transferred.push(context);
    }
    assert!(
        !transferred.is_empty(),
        "playback release must enqueue an actual assistant continuation before cancellation"
    );
    assert!(transferred
        .iter()
        .all(|frame| frame.direction == Direction::Upstream));
    h.peer.quiet().await;

    // Both actual history commits were coalesced while still assistant-owned.
    // The newest lease is cancelled only after that context was enqueued, while
    // the provider has received none of the transferred continuation authority.
    inject(
        &h.worker,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "work".into(),
            tool_call_id: "coalesced-B".into(),
            run_llm: false,
        }),
    )
    .await;
    assistant_observed(&mut h.assistant_events, AssistantObservation::ToolTerminal).await;
    let cancelled = h.peer.substantive().await;
    assert_eq!(cancelled["item"]["call_id"], "coalesced-B");
    assert_eq!(cancelled["item"]["output"], "CANCELLED");
    barrier(&mut h.worker).await;
    while let Ok(context) = h.after_held.try_recv() {
        transferred.push(context);
    }
    for context in &transferred {
        context.deliver().await;
    }
    assert_eq!(
        h.peer.substantive().await["type"],
        "response.create",
        "a coalesced context must retain independently valid A after latest B is cancelled"
    );
    assert_eq!(finish_a.available_permits(), 0);
    h.finish_response("coalesced-A-answer").await;
    h.peer.quiet().await;
    for (receipt, commit) in committed {
        assert_eq!(receipt.await_history().await.unwrap(), commit);
    }
    finish_a.add_permits(1);
    assert_eq!(h.peer.substantive().await["item"]["call_id"], "coalesced-A");
    barrier(&mut h.worker).await;
    h.peer.quiet().await;
    assert_eq!(
        h.peer
            .observed
            .iter()
            .filter(|command| command["type"] == "response.create")
            .count(),
        1
    );
    assert_eq!(h.peer.formal_outputs("coalesced-A"), 1);
    assert_eq!(h.peer.formal_outputs("coalesced-B"), 1);
    drop(transferred);
    h.finish().await;
}

#[tokio::test]
async fn independent_user_run_waits_for_committed_progress_to_reach_the_provider() {
    let (receipt_tx, mut receipts) = mpsc::unbounded_channel();
    let finish = Arc::new(Semaphore::new(0));
    let dropped = Arc::new(AtomicBool::new(false));
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        let dropped = dropped.clone();
        Arc::new(move |_, reporter| {
            let receipt_tx = receipt_tx.clone();
            let finish = finish.clone();
            let dropped = dropped.clone();
            Box::pin(async move {
                let _guard = CallbackDrop(dropped);
                // Only the later, independent user context requests inference.
                let receipt = reporter
                    .report(json!("committed before provider delivery"), Some(false))
                    .map_err(|error| error.to_string())?;
                receipt_tx.send(receipt.clone()).unwrap();
                receipt
                    .await_history()
                    .await
                    .map_err(|error| format!("{error:?}"))?;
                finish.acquire().await.unwrap().forget();
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.before
        .hold_upstream_progress
        .store(true, Ordering::SeqCst);
    h.call("user-overtake-call").await;
    let receipt = channel(&mut receipts).await;
    let held = channel(&mut h.before_held).await;
    assert_eq!(held.direction, Direction::Upstream);
    let commit = timeout(DEADLINE, receipt.await_history())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result_envelopes(&h.pair, "user-overtake-call").len(), 1);
    h.close_initial_response().await;
    h.peer.quiet().await;

    // The genuine pair.user producer snapshots the shared, already committed
    // history while the selected provider has not received the progress frame.
    inject(
        &h.worker,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(
                ChatRole::User,
                "continue from that real progress",
            )],
            run_llm: true,
        }),
    )
    .await;
    assistant_observed(&mut h.provider_events, AssistantObservation::UserContext).await;
    let mut probe = pipecat_realtime::SessionProperties::default();
    probe.instructions = Some("independent-user-progress-fifo-probe".into());
    h.provider
        .sender()
        .unwrap()
        .send(pipecat_realtime::Action::UpdateSession(probe))
        .await
        .unwrap();
    loop {
        let command = h.peer.command().await;
        assert_ne!(
            command["type"], "response.create",
            "user inference must not overtake its committed but undelivered progress"
        );
        assert_ne!(
            command["item"]["role"], "system",
            "only the retained real progress carrier can send its provider item"
        );
        if command["type"] == "session.update"
            && command["session"]["instructions"] == "independent-user-progress-fifo-probe"
        {
            break;
        }
    }
    assert_eq!(h.worker.state(), WorkerState::Running);
    assert!(!dropped.load(Ordering::SeqCst));
    assert_eq!(h.peer.formal_outputs("user-overtake-call"), 0);
    assert_eq!(receipt.await_history().await.unwrap(), commit);
    h.peer.quiet().await;

    held.deliver().await;
    let mut saw_progress = false;
    loop {
        let command = h.peer.substantive().await;
        if command["item"]["role"] == "system" {
            assert!(
                !saw_progress,
                "the held provider progress must be sent exactly once"
            );
            assert_progress_command(
                &command,
                "user-overtake-call",
                &json!("committed before provider delivery"),
            );
            saw_progress = true;
        }
        if command["type"] == "response.create" {
            assert!(
                saw_progress,
                "provider progress must precede the user-requested inference"
            );
            break;
        }
    }
    assert!(!dropped.load(Ordering::SeqCst));
    assert_eq!(h.peer.formal_outputs("user-overtake-call"), 0);
    h.finish_response("after-user-overtake").await;
    finish.add_permits(1);
    let terminal = h.peer.substantive().await;
    assert_eq!(terminal["item"]["call_id"], "user-overtake-call");
    assert_eq!(terminal["item"]["output"], "COMPLETED");
    assistant_observed(&mut h.assistant_events, AssistantObservation::ToolTerminal).await;
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("user-overtake-call"), 1);
    assert_eq!(
        h.peer
            .observed
            .iter()
            .filter(|command| command["type"] == "response.create")
            .count(),
        1
    );
    assert_eq!(receipt.await_history().await.unwrap(), commit);
    h.finish().await;
}

#[tokio::test]
async fn selected_terminal_context_cannot_be_laundered_into_control_or_fresh_public_authority() {
    let finish = Arc::new(Semaphore::new(0));
    let returned = Arc::new(AtomicBool::new(false));
    let (entered_tx, mut entered) = mpsc::unbounded_channel();
    let callback: ReportingToolCallback = {
        let finish = finish.clone();
        let returned = returned.clone();
        Arc::new(move |_, _| {
            let finish = finish.clone();
            let returned = returned.clone();
            let entered_tx = entered_tx.clone();
            Box::pin(async move {
                entered_tx.send(()).unwrap();
                finish.acquire().await.unwrap().forget();
                returned.store(true, Ordering::SeqCst);
                Ok(json!({"actual":"terminal"}))
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    h.after.hold_continuation.store(true, Ordering::SeqCst);
    h.call("sealed-terminal-call").await;
    channel(&mut entered).await;
    h.close_initial_response().await;
    finish.add_permits(1);
    let terminal = channel(&mut h.terminals).await;
    let held = channel(&mut h.after_held).await;
    assert_eq!(held.direction, Direction::Upstream);
    assert!(returned.load(Ordering::SeqCst));
    let outcome = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
    let reason = RealtimeNativeToolSchedulingReason::from_outcome(outcome);
    let cause = held
        .frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .clone();
    assert_eq!(cause.intent(), RealtimeContextIntent::AssistantContinuation);
    assert_eq!(
        cause.assistant_selected_native_terminal_route_id(),
        Some(h.route.id())
    );
    assert!(held.frame.has_native_tool_progress_restriction());
    assert!(cause.inference_fence().unwrap().is_current());
    let generation = h.provider.sender().unwrap().generation();
    let output = h.peer.substantive().await;
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], "sealed-terminal-call");
    assert_eq!(output["item"]["output"], "{\"actual\":\"terminal\"}");
    h.peer.quiet().await;

    for control in [
        FrameData::Interruption,
        FrameData::Cancel(CancelFrame {
            reason: Some("changed selected context".into()),
        }),
    ] {
        let mut changed = held.frame.clone();
        changed.data = control;
        assert!(changed.native_realtime_context_cause().is_err());
        h.worker
            .sender()
            .send(changed, Direction::Downstream)
            .await
            .expect_err("selected terminal context must be rejected before control effects");
        assert_eq!(h.worker.state(), WorkerState::Running);
        assert_eq!(h.provider.sender().unwrap().generation(), generation);
        assert_eq!(h.route.state(), NativeToolProgressRouteState::Open);
        assert!(cause.inference_fence().unwrap().is_current());
    }

    // Consuming the real private cause never clears the sticky selected origin.
    // Neither public producer path may turn that clone into new raw authority.
    let mut stripped = held.frame.clone();
    let taken = stripped
        .take_native_realtime_context_cause()
        .unwrap()
        .unwrap();
    assert_eq!(
        taken.assistant_selected_native_terminal_route_id(),
        Some(h.route.id())
    );
    assert!(stripped.has_native_tool_progress_restriction());
    assert!(stripped.native_realtime_context_cause().is_err());
    assert!(stripped.require_no_native_llm_continuation().is_err());
    assert!(serde_json::to_value(&stripped).is_err());
    let producer = NativeRealtimeContextProducer::default();
    assert!(producer
        .attach(&mut stripped, RealtimeContextIntent::Run, true)
        .is_err());
    assert!(producer
        .attach_assistant_continuation(&mut stripped, None)
        .is_err());
    assert!(producer
        .attach_assistant_continuation(&mut stripped, Some(reason))
        .is_err());
    h.worker
        .sender()
        .send(stripped, Direction::Downstream)
        .await
        .expect_err("stripped selected terminal context must not enter the real pipeline");
    barrier(&mut h.worker).await;
    assert_eq!(h.worker.state(), WorkerState::Running);
    assert_eq!(h.provider.sender().unwrap().generation(), generation);
    assert!(cause.inference_fence().unwrap().is_current());
    assert_eq!(h.peer.formal_outputs("sealed-terminal-call"), 1);
    h.peer.quiet().await;

    // The untouched frame still has its actual assistant-produced capability.
    // Its normal upstream provider consumption must succeed exactly once.
    held.deliver().await;
    assert_eq!(h.peer.substantive().await["type"], "response.create");
    h.finish_response("sealed-terminal-answer").await;
    h.peer.quiet().await;
    assert_eq!(h.peer.formal_outputs("sealed-terminal-call"), 1);
    assert_eq!(
        h.peer
            .observed
            .iter()
            .filter(|command| command["type"] == "response.create")
            .count(),
        1
    );
    assert_eq!(h.executor.outcomes().snapshot().total_terminal, 1);
    h.finish().await;
}

#[tokio::test]
async fn idle_peer_close_retires_selected_route_and_joins_the_actual_actor_as_failure() {
    let executions = Arc::new(AtomicUsize::new(0));
    let callback: ReportingToolCallback = {
        let executions = executions.clone();
        Arc::new(move |_, _| {
            let executions = executions.clone();
            Box::pin(async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            })
        })
    };
    let mut h = Harness::new(callback, false, false).await;
    barrier(&mut h.worker).await;
    assert_eq!(h.route.state(), NativeToolProgressRouteState::Open);
    assert_eq!(h.worker.state(), WorkerState::Running);
    assert!(!h.provider.selected_session_joined());
    assert_eq!(h.executor.outcomes().snapshot().total_terminal, 0);

    // A real idle WebSocket peer closes without any tool call, response, or
    // synthetic local error. Open selected ownership must not survive its actor.
    h.peer.close();
    let joined = timeout(DEADLINE, h.worker.join())
        .await
        .expect("unexpected idle provider exit must fail and actually join the pipeline");
    assert!(
        joined.is_err()
            || joined
                .as_ref()
                .is_ok_and(|report| report.state == WorkerState::Failed)
    );
    assert_eq!(h.worker.state(), WorkerState::Failed);
    assert!(matches!(
        h.route.state(),
        NativeToolProgressRouteState::Failed | NativeToolProgressRouteState::Closed
    ));
    assert_eq!(
        h.route.failure(),
        Some(NativeToolProgressFailure::ProviderRejected)
    );
    assert!(
        h.provider.selected_session_joined(),
        "actual actor JoinHandle await must finish before the failed pipeline joins"
    );
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert_eq!(h.executor.outcomes().snapshot().total_terminal, 0);
    h.peer.join().await;
    assert!(h
        .peer
        .observed
        .iter()
        .all(|command| command["type"] == "session.update"));
}

#[tokio::test]
async fn duplicate_selected_executor_setup_after_complete_composition_never_connects() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = pipecat_realtime::RealtimeConfig::new("unused-duplicate-executor-test-key");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.response_timeout = RESPONSE_TIMEOUT;
    config.limits.operation_timeout = OPERATION_TIMEOUT;
    config.limits.close_timeout = CLOSE_TIMEOUT;
    let (stop_listener, stopped) = tokio::sync::oneshot::channel::<()>();
    let listener_task = tokio::spawn(async move {
        // At termination a queued connection wins over the stop signal. Even a
        // failed WebSocket handshake therefore cannot hide a TCP connection.
        tokio::select! {
            biased;
            accepted = listener.accept() => {
                let (stream, _) = accepted.unwrap();
                drop(stream);
                1_usize
            }
            _ = stopped => 0_usize,
        }
    });
    let route = NativeToolProgressRoute::new(NativeToolProgressConfig {
        end_drain_timeout: END_DRAIN,
    })
    .unwrap();
    let provider = Arc::new(
        OpenAiRealtimeProcessor::new(pipecat_realtime::RealtimeClient::new(config).unwrap())
            .unwrap()
            .with_progress_route(route.clone()),
    );
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    pair.bind_tool_progress(route.clone()).unwrap();
    let executions = Arc::new(AtomicUsize::new(0));
    let callback: ReportingToolCallback = {
        let executions = executions.clone();
        Arc::new(move |_, _| {
            let executions = executions.clone();
            Box::pin(async move {
                executions.fetch_add(1, Ordering::SeqCst);
                Ok(Value::Null)
            })
        })
    };
    let make_executor = || {
        let mut executor = ToolExecutor::new(2, 32, 4096)
            .unwrap()
            .with_progress_route(route.clone())
            .unwrap();
        let mut tool = ReportingTool::new(callback.clone());
        tool.timeout = CALLBACK_TIMEOUT;
        executor.register_reporting("work", tool).unwrap();
        Arc::new(executor)
    };
    let first = make_executor();
    let duplicate = make_executor();
    let (observation_tx, mut observations) = mpsc::unbounded_channel();
    // Every setting and all six required roles are valid. Only the additional
    // selected executor is invalid, after the complete real composition setup.
    let processors: Vec<Arc<dyn FrameProcessor>> = vec![
        Arc::new(OpenAiRealtimeToolProgressGate::new(route.clone())),
        pair.user(),
        provider.clone(),
        first.clone(),
        Arc::new(OpenAiRealtimeToolProgressRelay::new(route.clone())),
        pair.assistant(),
        duplicate.clone(),
    ];
    let processors = processors
        .into_iter()
        .enumerate()
        .map(|(index, processor)| {
            SetupCleanupWitness::new(processor, index, observation_tx.clone())
                as Arc<dyn FrameProcessor>
        })
        .collect();
    let started = timeout(DEADLINE, Pipeline::new(processors).start(pipeline_config()))
        .await
        .expect("invalid selected setup must reach its actual startup terminal state");
    match started {
        // The real Pipeline.start failure path already awaits its worker join.
        Err(error) => assert!(matches!(error, PipelineError::Execution(_)), "{error:?}"),
        Ok(mut worker) => {
            // Core may report collected setup failures after Start. Do not
            // manufacture a Cancel to make this naturally failed lifecycle end.
            let joined = timeout(DEADLINE, worker.join())
                .await
                .expect("returned worker must naturally fail and actually join");
            assert!(
                joined.is_err()
                    || joined
                        .as_ref()
                        .is_ok_and(|report| report.state == WorkerState::Failed)
            );
            assert_eq!(worker.state(), WorkerState::Failed);
        }
    }
    let _ = stop_listener.send(());
    let connections = timeout(DEADLINE, listener_task)
        .await
        .expect("real preconnect listener task must join")
        .unwrap();
    let mut setup = Vec::new();
    let mut cleanup = Vec::new();
    while let Ok(observation) = observations.try_recv() {
        match observation {
            SetupLifecycleObservation::Setup(index, outcome) => setup.push((index, outcome)),
            SetupLifecycleObservation::Cleanup(index, outcome) => cleanup.push((index, outcome)),
        }
    }
    assert_eq!(setup.len(), 7);
    for (index, (actual_index, outcome)) in setup.iter().take(6).enumerate() {
        assert_eq!(*actual_index, index);
        assert!(
            outcome.is_ok(),
            "required role {index} setup failed: {outcome:?}"
        );
    }
    assert_eq!(setup[6].0, 6);
    assert_eq!(
        setup[6].1.as_ref().unwrap_err().message,
        "selected executor limits already registered"
    );
    cleanup.sort_by_key(|(index, _)| *index);
    assert_eq!(
        cleanup.len(),
        7,
        "every attempted real component must finish cleanup before startup/join returns"
    );
    for (index, (actual_index, outcome)) in cleanup.iter().enumerate() {
        assert_eq!(*actual_index, index);
        assert!(
            outcome.is_ok(),
            "component {index} cleanup failed: {outcome:?}"
        );
    }
    assert_eq!(
        connections, 0,
        "a failed duplicate selected setup must never reach TCP connect"
    );
    assert!(provider.sender().is_err());
    assert!(
        !provider.selected_session_joined(),
        "no actor was created to await"
    );
    assert!(route.is_closed());
    assert_eq!(
        route.failure(),
        Some(NativeToolProgressFailure::InvalidProvenance)
    );
    assert_eq!(executions.load(Ordering::SeqCst), 0);
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    for executor in [first, duplicate] {
        let journal = executor.outcomes().snapshot();
        assert!(journal.active.is_empty());
        assert_eq!(journal.total_terminal, 0);
    }
}
