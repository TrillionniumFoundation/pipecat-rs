//! Actual loopback admission and reducer scheduling, not hosted-provider qualification.
use pipecat::{
    core::{self, *},
    pipeline::DirectToolBinding,
    processors::tools::ToolOutcomeJournal,
    services::{ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider},
    turns::{AggregationAction, LLMContextAggregatorPair, SharedContext},
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, Semaphore},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);

async fn server() -> (String, JoinHandle<Value>) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/completion", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        timeout(WAIT, async {
            let (mut socket, _) = listener.accept().await.unwrap();
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
            let request = serde_json::from_slice(&bytes[end..end + length]).unwrap();
            let packet = json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"progress-context-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"native\"}"}}]},"finish_reason":"tool_calls"}]});
            let body = format!("data: {packet}\n\ndata: [DONE]\n\n");
            let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
            socket.write_all(response.as_bytes()).await.unwrap();
            socket.shutdown().await.unwrap();
            request
        }).await.unwrap()
    });
    (url, task)
}

struct BeforeAssistant {
    recorded: mpsc::Sender<Frame>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for BeforeAssistant {
    fn name(&self) -> &str {
        "progress-context-updated-before-assistant"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let progress = frame.native_tool_progress()?.is_some();
        if progress || frame.native_tool_outcome()?.is_some() {
            self.recorded
                .try_send(frame.clone())
                .map_err(|error| ProcessorError::new(error.to_string()))?;
        }
        if progress {
            timeout(WAIT, self.release.acquire())
                .await
                .map_err(|_| ProcessorError::new("test progress barrier timed out"))?
                .map_err(|_| ProcessorError::new("test progress barrier closed"))?
                .forget();
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
                || outer["tool_call_id"] != "progress-context-call"
                || outer["status"] != status
            {
                return None;
            }
            serde_json::from_str(outer["result"].as_str()?).ok()
        })
        .collect()
}

#[tokio::test]
async fn owned_live_progress_schedules_context_updated_after_commit_without_settlement() {
    let (url, peer) = server().await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let callback = LocalAsyncCallback::new(move || {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let expected_callback = callback.clone();
    let working = json!({"phase":"working","value":"native"});
    let done = json!({"phase":"done","value":"native"});
    let finish = Arc::new(Semaphore::new(0));
    let gate = finish.clone();
    let function = DirectFunction {
        schema: core::ToolDefinition {
            name: "echo".into(),
            description: "Report live work before completing".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        },
        handler: LocalToolFunction::new_with_reporter(move |arguments, reporter| {
            let callback = callback.clone();
            let gate = gate.clone();
            async move {
                assert_eq!(arguments, json!({"value":"native"}));
                reporter
                    .report_with_context_updated(
                        LocalToolCompletion::new(
                            LocalToolResult::new(json!({"phase":"working","value":"native"}))
                                .with_run_llm(false),
                        )
                        .with_context_updated(callback),
                    )
                    .await?;
                gate.acquire().await.unwrap().forget();
                Ok(
                    LocalToolResult::new(json!({"phase":"done","value":"native"}))
                        .with_run_llm(false),
                )
            }
        }),
    };
    assert!(!function.handler.options().cancel_on_interruption);
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![
                ChatMessage::new(ChatRole::User, "Report live work, then finish").into(),
            ],
            tools: ToolSet::Schema(ToolsSchema {
                standard_tools: vec![StandardTool::Direct(function)],
                custom_tools: None,
            }),
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
    let release = Arc::new(Semaphore::new(0));
    let (recorded, mut observed) = mpsc::channel(4);
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(BeforeAssistant {
        recorded,
        release: release.clone(),
    }));
    stages.push(pair.assistant());
    let worker = timeout(WAIT, Pipeline::new(stages).start(PipelineConfig::default()))
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
    let progress = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let authority = progress
        .native_tool_progress()
        .unwrap()
        .expect("admitted reporter must mint valid progress authority");
    assert!(progress.native_tool_outcome().unwrap().is_none());
    assert_eq!(authority.sequence(), 1);
    assert!(!authority.cancel_on_interruption());
    assert_eq!(authority.call().tool_call_id, "progress-context-call");
    assert_eq!(authority.call().arguments, json!({"value":"native"}));
    assert_eq!(authority.batch().unwrap().members().len(), 1);
    let FrameData::FunctionCallResult(result) = &progress.data else {
        panic!("native progress result required");
    };
    assert!(!result.is_final && result.error.is_none());
    assert_eq!(result.run_llm, Some(false));
    assert_eq!(result.result, working);
    timeout(WAIT, async {
        loop {
            assert!(worker.errors().is_empty(), "{:?}", worker.errors());
            if pair.assistant().has_function_calls_in_progress().unwrap() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(async_values(&pair.context().snapshot_rich().unwrap(), "running").is_empty());
    assert_eq!(called.load(Ordering::SeqCst), 0);

    // This is a finite scheduling proof using the actual public reducer. The
    // callback action is inspected, not executed; actor task ownership is separate.
    let output = pair
        .assistant()
        .process_frame(progress, Direction::Downstream)
        .unwrap();
    let callbacks: Vec<_> = output
        .actions
        .into_iter()
        .filter_map(|action| {
            if let AggregationAction::ContextUpdated(callback) = action {
                Some(callback)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        vec![working.clone()]
    );
    assert!(pair.assistant().has_function_calls_in_progress().unwrap());
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.active[0].tool_call_id, "progress-context-call");
    assert_eq!(held.total_terminal, 0);
    assert_eq!(called.load(Ordering::SeqCst), 0);
    release.add_permits(1);
    finish.add_permits(1);
    let terminal = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let receipt = terminal
        .native_tool_outcome()
        .unwrap()
        .expect("normal return must settle once");
    assert_eq!(receipt.state(), ToolCallTerminalState::Completed);
    assert_eq!(receipt.reason(), ToolCallEndReason::CallbackReturned);
    timeout(WAIT, async {
        loop {
            assert!(worker.errors().is_empty(), "{:?}", worker.errors());
            if async_values(&pair.context().snapshot_rich().unwrap(), "finished")
                == vec![done.clone()]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(request["tools"][0]["function"]["name"], "echo");
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(async_values(&context, "running"), vec![working]);
    assert_eq!(async_values(&context, "finished"), vec![done]);
    assert_eq!(
        called.load(Ordering::SeqCst),
        0,
        "manual action inspection must not execute user code"
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        callbacks.len(),
        1,
        "owned intermediate context commit lost its context-updated action"
    );
    assert_eq!(callbacks[0], expected_callback);
}

fn reporting_handler(callback: LocalAsyncCallback, gate: Arc<Semaphore>) -> LocalToolFunction {
    LocalToolFunction::new_with_reporter(move |arguments, reporter| {
        let callback = callback.clone();
        let gate = gate.clone();
        async move {
            assert_eq!(arguments, json!({"value":"native"}));
            reporter
                .report_with_context_updated(
                    LocalToolCompletion::new(
                        LocalToolResult::new(json!({"phase":"working","value":"native"}))
                            .with_run_llm(false),
                    )
                    .with_context_updated(callback),
                )
                .await?;
            gate.acquire().await.unwrap().forget();
            Ok(LocalToolResult::new(json!({"phase":"done","value":"native"})).with_run_llm(false))
        }
    })
}

async fn start(
    url: String,
    handler: LocalToolFunction,
) -> (PipelineWorker, LLMContextAggregatorPair, ToolOutcomeJournal) {
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![
                ChatMessage::new(ChatRole::User, "Report live work, then finish").into(),
            ],
            tools: ToolSet::Schema(ToolsSchema {
                standard_tools: vec![StandardTool::Direct(DirectFunction {
                    schema: core::ToolDefinition {
                        name: "echo".into(),
                        description: "Report live work before completing".into(),
                        parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
                    },
                    handler,
                })],
                custom_tools: None,
            }),
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
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(pair.assistant());
    let worker = timeout(WAIT, Pipeline::new(stages).start(PipelineConfig::default()))
        .await
        .unwrap()
        .unwrap();
    (worker, pair, outcomes)
}

async fn run(worker: &PipelineWorker) {
    timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream),
    )
    .await
    .unwrap()
    .unwrap();
}

async fn wait_finished(pair: &LLMContextAggregatorPair) {
    timeout(WAIT, async {
        loop {
            if async_values(&pair.context().snapshot_rich().unwrap(), "finished")
                == vec![json!({"phase":"done","value":"native"})]
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn joined(worker: PipelineWorker, peer: JoinHandle<Value>) -> WorkerReport {
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(request["tools"][0]["function"]["name"], "echo");
    report
}

#[tokio::test]
async fn actual_progress_callback_observes_committed_history_while_tool_remains_active() {
    let (url, peer) = server().await;
    let context_slot = Arc::new(Mutex::new(None::<SharedContext>));
    let captured = context_slot.clone();
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let (finished, mut observed) = mpsc::channel(2);
    let callback = LocalAsyncCallback::new(move || {
        let context = captured.lock().unwrap().take().unwrap();
        let count = count.clone();
        let finished = finished.clone();
        async move {
            let values = async_values(&context.snapshot_rich()?, "running");
            count.fetch_add(1, Ordering::SeqCst);
            finished
                .try_send(values)
                .map_err(|_| ProcessorError::new("test callback receiver closed"))?;
            Ok(())
        }
    });
    let gate = Arc::new(Semaphore::new(0));
    let (worker, pair, outcomes) = start(url, reporting_handler(callback, gate.clone())).await;
    *context_slot.lock().unwrap() = Some(pair.context());
    run(&worker).await;
    let values = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    assert_eq!(values, vec![json!({"phase":"working","value":"native"})]);
    assert!(context_slot.lock().unwrap().is_none());
    assert!(pair.assistant().has_function_calls_in_progress().unwrap());
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.active[0].tool_call_id, "progress-context-call");
    assert_eq!(held.total_terminal, 0);
    gate.add_permits(1);
    wait_finished(&pair).await;
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let report = joined(worker, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert!(observed.try_recv().is_err());
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        values
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
}

struct FutureDropped(Arc<AtomicUsize>);
impl Drop for FutureDropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

async fn held_hook_outlives_tool_cancel_until_pipeline_terminal(cancel: bool) {
    let (url, peer) = server().await;
    let context_slot = Arc::new(Mutex::new(None::<SharedContext>));
    let captured = context_slot.clone();
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let dropped = Arc::new(AtomicUsize::new(0));
    let on_drop = dropped.clone();
    let (entered, mut observed) = mpsc::channel(1);
    let callback = LocalAsyncCallback::new(move || {
        let context = captured.lock().unwrap().take().unwrap();
        let entered = entered.clone();
        let count = count.clone();
        let on_drop = on_drop.clone();
        async move {
            let _drop = FutureDropped(on_drop);
            let resource = Arc::new(());
            count.fetch_add(1, Ordering::SeqCst);
            assert!(entered
                .try_send((
                    async_values(&context.snapshot_rich()?, "running"),
                    Arc::downgrade(&resource)
                ))
                .is_ok());
            std::future::pending::<()>().await;
            drop(resource);
            drop(context);
            Ok(())
        }
    });
    let gate = Arc::new(Semaphore::new(0));
    let (mut worker, pair, outcomes) = start(url, reporting_handler(callback, gate)).await;
    *context_slot.lock().unwrap() = Some(pair.context());
    run(&worker).await;
    let (values, weak) = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    assert_eq!(values, vec![json!({"phase":"working","value":"native"})]);
    assert!(context_slot.lock().unwrap().is_none());
    assert!(weak.upgrade().is_some());
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    let interruption = Frame::interruption();
    let interruption_id = interruption.id;
    timeout(
        WAIT,
        worker.sender().send(interruption, Direction::Downstream),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if frame.frame.id == interruption_id {
                break;
            }
            if let FrameData::Error(error) = frame.frame.data {
                panic!("unexpected interruption error: {error:?}");
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    assert!(pair.assistant().has_function_calls_in_progress().unwrap());
    timeout(
        WAIT,
        worker.sender().send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "echo".into(),
                tool_call_id: "progress-context-call".into(),
                run_llm: false,
            })),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(WAIT, async {
        loop {
            assert!(worker.errors().is_empty(), "{:?}", worker.errors());
            let settled = outcomes.snapshot();
            if settled.active.is_empty()
                && settled.total_terminal == 1
                && !pair.assistant().has_function_calls_in_progress().unwrap()
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Cancelled
    );
    assert_eq!(
        settled.terminal[0].outcome.reason(),
        ToolCallEndReason::ExplicitCancel
    );
    let marker = json!({"fixture":"tool cancel does not own callback task"});
    timeout(
        WAIT,
        worker.sender().send(
            Frame::new(FrameData::OutputTransportMessage(marker.clone())),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if matches!(&frame.frame.data, FrameData::OutputTransportMessage(value) if value == &marker) { break; }
            if let FrameData::Error(error) = frame.frame.data { panic!("unexpected traffic error: {error:?}"); }
        }
    }).await.unwrap();
    assert_eq!(
        dropped.load(Ordering::SeqCst),
        0,
        "tool cancellation must not cancel the independent callback task"
    );
    assert!(weak.upgrade().is_some());
    if cancel {
        timeout(WAIT, worker.cancel("progress callback fixture"))
            .await
            .unwrap()
            .unwrap();
    } else {
        timeout(WAIT, worker.end()).await.unwrap().unwrap();
    }
    let report = joined(worker, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(weak.upgrade().is_none());
    assert_eq!(outcomes.snapshot().total_terminal, 1);
}

#[tokio::test]
async fn hanging_progress_hook_survives_interruption_and_tool_cancel_until_end_or_cancel() {
    for cancel in [false, true] {
        held_hook_outlives_tool_cancel_until_pipeline_terminal(cancel).await;
    }
}

#[tokio::test]
async fn progress_hook_error_does_not_settle_or_resettle_the_live_tool() {
    let (url, peer) = server().await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let callback = LocalAsyncCallback::new(move || {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(ProcessorError::new(
                "progress context-updated fixture error",
            ))
        }
    });
    let gate = Arc::new(Semaphore::new(0));
    let (mut worker, pair, outcomes) = start(url, reporting_handler(callback, gate.clone())).await;
    run(&worker).await;
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if let FrameData::Error(error) = frame.frame.data {
                assert_eq!(error.error, "progress context-updated fixture error");
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        vec![json!({"phase":"working","value":"native"})]
    );
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.total_terminal, 0);
    gate.add_permits(1);
    wait_finished(&pair).await;
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let report = joined(worker, peer).await;
    assert_eq!(report.errors.len(), 1);
    assert_eq!(
        report.errors[0].error,
        "progress context-updated fixture error"
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

#[tokio::test]
async fn reusing_one_progress_callback_executes_it_for_each_report_before_final_settlement() {
    let (url, peer) = server().await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let (entered, mut observed) = mpsc::channel(2);
    let callback = LocalAsyncCallback::new(move || {
        let count = count.clone();
        let entered = entered.clone();
        async move {
            let invocation = count.fetch_add(1, Ordering::SeqCst) + 1;
            entered
                .try_send(invocation)
                .map_err(|_| ProcessorError::new("test callback receiver closed"))?;
            Ok(())
        }
    });
    let next = Arc::new(Semaphore::new(0));
    let next_report = next.clone();
    let finish = Arc::new(Semaphore::new(0));
    let final_gate = finish.clone();
    let handler = LocalToolFunction::new_with_reporter(move |arguments, reporter| {
        let callback = callback.clone();
        let next_report = next_report.clone();
        let final_gate = final_gate.clone();
        async move {
            assert_eq!(arguments, json!({"value":"native"}));
            reporter
                .report_with_context_updated(
                    LocalToolCompletion::new(
                        LocalToolResult::new(json!({"phase":"first"})).with_run_llm(false),
                    )
                    .with_context_updated(callback.clone()),
                )
                .await?;
            next_report.acquire().await.unwrap().forget();
            reporter
                .report_with_context_updated(
                    LocalToolCompletion::new(
                        LocalToolResult::new(json!({"phase":"second"})).with_run_llm(false),
                    )
                    .with_context_updated(callback),
                )
                .await?;
            final_gate.acquire().await.unwrap().forget();
            Ok(LocalToolResult::new(json!({"phase":"done","value":"native"})).with_run_llm(false))
        }
    });
    let (worker, pair, outcomes) = start(url, handler).await;
    run(&worker).await;
    assert_eq!(timeout(WAIT, observed.recv()).await.unwrap().unwrap(), 1);
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        vec![json!({"phase":"first"})]
    );
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    next.add_permits(1);
    assert_eq!(timeout(WAIT, observed.recv()).await.unwrap().unwrap(), 2);
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        vec![json!({"phase":"first"}), json!({"phase":"second"})]
    );
    assert!(pair.assistant().has_function_calls_in_progress().unwrap());
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    finish.add_permits(1);
    wait_finished(&pair).await;
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let report = joined(worker, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(called.load(Ordering::SeqCst), 2);
    assert!(observed.try_recv().is_err());
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
}
