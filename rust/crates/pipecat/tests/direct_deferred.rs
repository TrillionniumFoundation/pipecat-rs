//! Deferred application dispatch through actual loopback HTTP, not a cloud qualification.
use pipecat::{
    core::{self, *},
    pipeline::DirectToolBinding,
    processors::tools::ToolOutcomeJournal,
    services::{ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider},
    turns::LLMContextAggregatorPair,
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);

async fn server() -> (String, oneshot::Sender<()>, JoinHandle<Vec<Value>>) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/completion", listener.local_addr().unwrap());
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        timeout(WAIT, async {
            let mut requests = vec![];
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let mut bytes = vec![];
                let mut chunk = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    assert!(bytes.len() <= 64 * 1024);
                    if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") { break end + 4; }
                };
                let length: usize = String::from_utf8_lossy(&bytes[..end]).lines().find_map(|line| {
                    line.to_ascii_lowercase().strip_prefix("content-length:")
                        .map(|value| value.trim().parse().unwrap())
                }).unwrap();
                assert!(length <= 64 * 1024);
                while bytes.len() < end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                requests.push(serde_json::from_slice(&bytes[end..end + length]).unwrap());
                assert!(requests.len() <= 2, "unexpected deferred result replay");
                let packet = if requests.len() == 1 {
                    json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"deferred-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"native\"}"}}]},"finish_reason":"tool_calls"}]})
                } else {
                    json!({"choices":[{"index":0,"delta":{"content":"deferred result explained"},"finish_reason":"stop"}]})
                };
                let body = format!("data: {packet}\n\ndata: [DONE]\n\n");
                let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
            requests
        }).await.unwrap()
    });
    (url, stop, task)
}

struct DispatcherExited(Arc<AtomicUsize>);
impl Drop for DispatcherExited {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Witness(Arc<Mutex<Vec<Frame>>>);
#[async_trait]
impl FrameProcessor for Witness {
    fn name(&self) -> &str {
        "deferred-native-result-witness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.native_tool_progress()?.is_some() || frame.native_tool_outcome()?.is_some() {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}

fn async_values(context: &LlmContextData, status: &str) -> Vec<Value> {
    context
        .messages
        .iter()
        .filter_map(|message| {
            if message.role != ChatRole::Developer {
                return None;
            }
            let MessageContent::Text(text) = &message.content else {
                return None;
            };
            let outer: Value = serde_json::from_str(text).ok()?;
            if outer["type"] != "async_tool"
                || outer["tool_call_id"] != "deferred-call"
                || outer["status"] != status
            {
                return None;
            }
            serde_json::from_str(outer["result"].as_str()?).ok()
        })
        .collect()
}

#[tokio::test]
async fn dispatcher_return_keeps_native_lease_for_later_progress_and_final_response() {
    let (url, stop, peer) = server().await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let exited = Arc::new(AtomicUsize::new(0));
    let on_exit = exited.clone();
    let (escaped, mut responders) = mpsc::channel(1);
    let handler = LocalToolFunction::new_deferred(move |arguments, responder| {
        let escaped = escaped.clone();
        let count = count.clone();
        let on_exit = on_exit.clone();
        async move {
            let _exit = DispatcherExited(on_exit);
            assert_eq!(arguments, json!({"value":"native"}));
            count.fetch_add(1, Ordering::SeqCst);
            assert!(escaped.try_send(responder).is_ok());
            // No await follows the handoff. The exit witness records this
            // successful dispatcher scope returning, not a suspended handler.
            Ok(())
        }
    })
    .with_options(LocalToolOptions {
        cancel_on_interruption: false,
        timeout: Some(Duration::from_secs(5)),
    })
    .unwrap();
    assert!(!handler.options().cancel_on_interruption);
    let advertised = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(DirectFunction {
            schema: core::ToolDefinition {
                name: "echo".into(),
                description: "Dispatch work for later completion".into(),
                parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
            },
            handler,
        })],
        custom_tools: None,
    });
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(
                ChatRole::User,
                "Dispatch echo and explain its final result",
            )
            .into()],
            tools: advertised.clone(),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    config.api_key = None;
    config.request_timeout = WAIT;
    config.idle_timeout = WAIT;
    let binding = DirectToolBinding::new(Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    )))
    .unwrap();
    let outcomes = binding.outcomes();
    let recorded = Arc::new(Mutex::new(vec![]));
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(Witness(recorded.clone())));
    stages.push(pair.assistant());
    let mut worker = timeout(WAIT, Pipeline::new(stages).start(PipelineConfig::default()))
        .await
        .unwrap()
        .unwrap();
    timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream),
    )
    .await
    .unwrap()
    .unwrap();
    let responder = timeout(WAIT, responders.recv()).await.unwrap().unwrap();
    timeout(WAIT, async {
        while exited.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let marker = Frame::new(FrameData::OutputTransportMessage(
        json!({"fixture":"dispatcher returned"}),
    ));
    let marker_id = marker.id;
    timeout(WAIT, worker.sender().send(marker, Direction::Downstream))
        .await
        .unwrap()
        .unwrap();
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if frame.frame.id == marker_id {
                break;
            }
            if let FrameData::Error(error) = frame.frame.data {
                panic!("unexpected dispatch error: {error:?}");
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(exited.load(Ordering::SeqCst), 1);
    let mut returned = outcomes.snapshot();
    let working = json!({"phase":"working","value":"native"});
    let done = json!({"phase":"done","value":"native"});
    let reported = timeout(
        WAIT,
        responder.report(LocalToolResult::new(working.clone()).with_run_llm(false)),
    )
    .await
    .unwrap();
    let finished = responder.finish(Ok(LocalToolCompletion::new(
        LocalToolResult::new(done.clone()).with_run_llm(true),
    )));
    if finished.is_err() {
        // Dispatcher exit alone does not fence its owner's settlement. Wait for
        // the baseline's actual null terminal instead of assuming a snapshot or
        // using a sleep; a successful deferred handoff never takes this branch.
        timeout(WAIT, async {
            loop {
                assert!(worker.errors().is_empty(), "{:?}", worker.errors());
                let observed_null = recorded.lock().unwrap().iter().any(|frame| {
                    frame.native_tool_outcome().unwrap().is_some()
                        && matches!(&frame.data, FrameData::FunctionCallResult(result)
                            if result.is_final && result.error.is_none() && result.result.is_null())
                });
                if outcomes.snapshot().total_terminal == 1 && observed_null {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        returned = outcomes.snapshot();
        assert!(returned.active.is_empty());
        let receipt = &returned.terminal[0].outcome;
        assert_eq!(receipt.state(), ToolCallTerminalState::Completed);
        assert_eq!(receipt.reason(), ToolCallEndReason::CallbackReturned);
        assert_eq!(receipt.call().tool_call_id, "deferred-call");
        assert_eq!(receipt.call().arguments, json!({"value":"native"}));
    }
    // A closed baseline endpoint is a concrete, finite result. Only admitted
    // progress and completion wait for their expected second native response.
    if reported.is_ok() && finished.is_ok() {
        timeout(WAIT, async {
            loop {
                let frame = worker.recv().await.unwrap();
                if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                    if context.context.messages.last().is_some_and(|message|
                        message.role == ChatRole::Assistant && message.content == MessageContent::Text("deferred result explained".into()))) { break; }
                if let FrameData::Error(error) = frame.frame.data { panic!("unexpected deferred result error: {error:?}"); }
            }
        }).await.unwrap();
    }
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(requests[0]["tools"][0]["function"]["name"], "echo");
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(context.tools, advertised);
    let frames = recorded.lock().unwrap();
    let progress = frames
        .iter()
        .filter(|frame| frame.native_tool_progress().unwrap().is_some())
        .count();
    let terminal = frames
        .iter()
        .filter(|frame| frame.native_tool_outcome().unwrap().is_some())
        .count();
    let observed = (
        returned.active.len(),
        returned.total_terminal,
        reported.is_ok(),
        finished.is_ok(),
        requests.len(),
        async_values(&context, "running"),
        async_values(&context, "finished"),
        progress,
        terminal,
    );
    assert_eq!(
        observed,
        (1, 0, true, true, 2, vec![working], vec![done], 1, 1),
        "dispatcher return prematurely settled native work; report_error={:?}; finish_error={:?}",
        reported.err(),
        finished.err()
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
    assert_eq!(
        settled.terminal[0].outcome.reason(),
        ToolCallEndReason::CallbackReturned
    );
}

type DispatchedTool = (
    LocalToolFunction,
    mpsc::Receiver<(LocalToolResponder, Instant)>,
    Arc<AtomicUsize>,
    Arc<AtomicUsize>,
);

fn dispatcher(deadline: Duration) -> DispatchedTool {
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let exited = Arc::new(AtomicUsize::new(0));
    let on_exit = exited.clone();
    let (escaped, received) = mpsc::channel(1);
    let handler = LocalToolFunction::new_deferred(move |arguments, responder| {
        let escaped = escaped.clone();
        let count = count.clone();
        let on_exit = on_exit.clone();
        async move {
            let entered = Instant::now();
            let _exit = DispatcherExited(on_exit);
            assert_eq!(arguments, json!({"value":"native"}));
            count.fetch_add(1, Ordering::SeqCst);
            assert!(escaped.try_send((responder, entered)).is_ok());
            Ok(())
        }
    })
    .with_options(LocalToolOptions {
        cancel_on_interruption: false,
        timeout: Some(deadline),
    })
    .unwrap();
    (handler, received, called, exited)
}

async fn start(
    url: String,
    handler: LocalToolFunction,
) -> (
    PipelineWorker,
    LLMContextAggregatorPair,
    ToolOutcomeJournal,
    Arc<Mutex<Vec<Frame>>>,
) {
    let tools = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(DirectFunction {
            schema: core::ToolDefinition {
                name: "echo".into(),
                description: "Dispatch work for later completion".into(),
                parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
            },
            handler,
        })],
        custom_tools: None,
    });
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(
                ChatRole::User,
                "Dispatch echo and explain its final result",
            )
            .into()],
            tools,
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    config.api_key = None;
    config.request_timeout = WAIT;
    config.idle_timeout = WAIT;
    let binding = DirectToolBinding::new(Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    )))
    .unwrap();
    let outcomes = binding.outcomes();
    let recorded = Arc::new(Mutex::new(vec![]));
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(Witness(recorded.clone())));
    stages.push(pair.assistant());
    let worker = timeout(
        WAIT,
        Pipeline::new(stages).start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .unwrap();
    send(&worker, FrameData::LlmRun).await;
    (worker, pair, outcomes, recorded)
}

async fn send(worker: &PipelineWorker, data: FrameData) {
    timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn wait_values(pair: &LLMContextAggregatorPair, status: &str, expected: Vec<Value>) {
    timeout(WAIT, async {
        loop {
            if async_values(&pair.context().snapshot_rich().unwrap(), status) == expected {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_response(worker: &mut PipelineWorker) {
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant && message.content == MessageContent::Text("deferred result explained".into()))) { break; }
            if let FrameData::Error(error) = frame.frame.data { panic!("unexpected deferred response error: {error:?}"); }
        }
    }).await.unwrap();
}

async fn joined(
    worker: PipelineWorker,
    stop: oneshot::Sender<()>,
    peer: JoinHandle<Vec<Value>>,
) -> (WorkerReport, Vec<Value>) {
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    (report, requests)
}

#[tokio::test]
async fn external_final_hook_observes_committed_context_once_and_closed_endpoint_rejects_replay() {
    let (url, stop, peer) = server().await;
    let (handler, mut received, called, exited) = dispatcher(Duration::from_secs(5));
    let (worker, pair, outcomes, recorded) = start(url, handler).await;
    let (responder, _) = timeout(WAIT, received.recv()).await.unwrap().unwrap();
    assert_eq!(exited.load(Ordering::SeqCst), 1);
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    let working = json!({"phase":"working"});
    timeout(
        WAIT,
        responder.report(LocalToolResult::new(working.clone()).with_run_llm(false)),
    )
    .await
    .unwrap()
    .unwrap();
    wait_values(&pair, "running", vec![working.clone()]).await;
    let done = json!({"phase":"done"});
    let context = pair.context();
    let hooks = Arc::new(AtomicUsize::new(0));
    let hook_count = hooks.clone();
    let (notified, mut notification) = mpsc::channel(1);
    let callback = LocalAsyncCallback::new(move || {
        let context = context.clone();
        let hook_count = hook_count.clone();
        let notified = notified.clone();
        async move {
            hook_count.fetch_add(1, Ordering::SeqCst);
            assert!(notified
                .try_send(async_values(&context.snapshot_rich()?, "finished"))
                .is_ok());
            Ok(())
        }
    });
    responder
        .finish(Ok(LocalToolCompletion::new(
            LocalToolResult::new(done.clone()).with_run_llm(false),
        )
        .with_context_updated(callback.clone())))
        .unwrap();
    assert!(responder
        .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(json!(
            "duplicate"
        )))
        .with_context_updated(callback)))
        .is_err());
    assert!(
        timeout(WAIT, responder.report(LocalToolResult::new(json!("late"))))
            .await
            .unwrap()
            .is_err()
    );
    assert_eq!(
        timeout(WAIT, notification.recv()).await.unwrap().unwrap(),
        vec![done.clone()]
    );
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "finished"),
        vec![done]
    );
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (report, requests) = joined(worker, stop, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(requests.len(), 1);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(hooks.load(Ordering::SeqCst), 1);
    assert!(notification.try_recv().is_err());
    assert_eq!(recorded.lock().unwrap().len(), 2);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
}

#[tokio::test]
async fn deferred_tool_or_pipeline_cancellation_revokes_escaped_endpoints_and_settles_once() {
    for mode in ["tool-command", "responder", "pipeline"] {
        let (url, stop, peer) = server().await;
        let (handler, mut received, called, exited) = dispatcher(Duration::from_secs(5));
        let (worker, pair, outcomes, recorded) = start(url, handler).await;
        let (responder, _) = timeout(WAIT, received.recv()).await.unwrap().unwrap();
        let cloned = responder.clone();
        assert_eq!(exited.load(Ordering::SeqCst), 1);
        assert_eq!(outcomes.snapshot().active.len(), 1);
        assert_eq!(outcomes.snapshot().total_terminal, 0);
        if mode == "pipeline" {
            timeout(WAIT, worker.cancel("deferred owner fixture"))
                .await
                .unwrap()
                .unwrap();
        } else {
            if mode == "responder" {
                responder.cancel().unwrap();
            } else {
                send(
                    &worker,
                    FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                        function_name: "echo".into(),
                        tool_call_id: "deferred-call".into(),
                        run_llm: false,
                    }),
                )
                .await;
            }
            timeout(WAIT, async {
                loop {
                    assert!(worker.errors().is_empty(), "{:?}", worker.errors());
                    if outcomes.snapshot().total_terminal == 1
                        && recorded.lock().unwrap().len() == 1
                        && !pair.assistant().has_function_calls_in_progress().unwrap()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap();
            timeout(WAIT, worker.end()).await.unwrap().unwrap();
        }
        let (report, requests) = joined(worker, stop, peer).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            requests.len(),
            1,
            "cancellation must not rerun the native model"
        );
        assert_eq!(called.load(Ordering::SeqCst), 1);
        for endpoint in [responder, cloned] {
            assert!(
                timeout(WAIT, endpoint.report(LocalToolResult::new(json!("late"))))
                    .await
                    .unwrap()
                    .is_err()
            );
            assert!(endpoint
                .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(json!(
                    "late final"
                )))))
                .is_err());
            assert!(endpoint.cancel().is_err());
        }
        let settled = outcomes.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 1);
        assert_eq!(settled.terminal.len(), 1);
        assert_eq!(
            settled.terminal[0].outcome.state(),
            ToolCallTerminalState::Cancelled
        );
        assert_eq!(
            settled.terminal[0].outcome.reason(),
            if mode == "pipeline" {
                ToolCallEndReason::TaskDropped
            } else {
                ToolCallEndReason::ExplicitCancel
            }
        );
        // Revoking this endpoint cancels the owned lease, not an external app's
        // independently launched work or any side effects it already performed.
    }
}

#[tokio::test]
async fn reports_after_dispatcher_return_do_not_reset_the_original_finite_deadline() {
    let (url, stop, peer) = server().await;
    let (handler, mut received, called, exited) = dispatcher(Duration::from_secs(1));
    let (mut worker, pair, outcomes, recorded) = start(url, handler).await;
    let (responder, entered) = timeout(WAIT, received.recv()).await.unwrap().unwrap();
    assert_eq!(exited.load(Ordering::SeqCst), 1);
    let first = json!({"phase":"first"});
    let second = json!({"phase":"second"});
    timeout(
        WAIT,
        responder.report(LocalToolResult::new(first.clone()).with_run_llm(false)),
    )
    .await
    .unwrap()
    .unwrap();
    wait_values(&pair, "running", vec![first.clone()]).await;
    timeout(
        WAIT,
        tokio::time::sleep_until(tokio::time::Instant::from_std(
            entered + Duration::from_millis(700),
        )),
    )
    .await
    .unwrap();
    timeout(
        WAIT,
        responder.report(LocalToolResult::new(second.clone()).with_run_llm(false)),
    )
    .await
    .unwrap()
    .unwrap();
    wait_values(&pair, "running", vec![first.clone(), second.clone()]).await;
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    timeout(WAIT, async {
        while outcomes.snapshot().total_terminal == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let elapsed = entered.elapsed();
    assert!(
        elapsed < Duration::from_millis(1450),
        "a report extended the dispatch deadline: {elapsed:?}"
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Failed
    );
    assert_eq!(
        settled.terminal[0].outcome.reason(),
        ToolCallEndReason::TimedOut
    );
    assert!(
        timeout(WAIT, responder.report(LocalToolResult::new(json!("late"))))
            .await
            .unwrap()
            .is_err()
    );
    assert!(responder
        .finish(Ok(LocalToolCompletion::new(LocalToolResult::new(json!(
            "late final"
        )))))
        .is_err());
    // Ordinary timeout is Failed/TimedOut with default continuation; provide a
    // healthy second HTTP response instead of changing it into cancellation.
    wait_response(&mut worker).await;
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (report, requests) = joined(worker, stop, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(requests.len(), 2);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        vec![first, second]
    );
    assert_eq!(recorded.lock().unwrap().len(), 3);
    assert_eq!(outcomes.snapshot().total_terminal, 1);
}

#[tokio::test]
async fn external_final_during_end_drain_completes_within_the_shutdown_budget() {
    let (url, stop, peer) = server().await;
    let (handler, mut received, called, exited) = dispatcher(Duration::from_secs(5));
    let (worker, pair, outcomes, recorded) = start(url, handler).await;
    let (responder, _) = timeout(WAIT, received.recv()).await.unwrap().unwrap();
    assert_eq!(exited.load(Ordering::SeqCst), 1);
    let working = json!({"phase":"working"});
    let done = json!({"phase":"done during drain"});
    timeout(
        WAIT,
        responder.report(LocalToolResult::new(working.clone()).with_run_llm(false)),
    )
    .await
    .unwrap()
    .unwrap();
    wait_values(&pair, "running", vec![working]).await;
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    assert!(timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::text("input is closed"), Direction::Downstream)
    )
    .await
    .unwrap()
    .is_err());
    assert!(
        !worker.has_finished(),
        "the active owned lease must still be draining"
    );
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    responder
        .finish(Ok(LocalToolCompletion::new(
            LocalToolResult::new(done.clone()).with_run_llm(false),
        )))
        .unwrap();
    let (report, requests) = joined(worker, stop, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(requests.len(), 1);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "finished"),
        vec![done]
    );
    assert_eq!(recorded.lock().unwrap().len(), 2);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
    assert_eq!(
        settled.terminal[0].outcome.reason(),
        ToolCallEndReason::CallbackReturned
    );
}
