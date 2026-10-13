//! Native loopback coverage; this does not qualify a hosted provider.
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

async fn server(calls: usize) -> (String, JoinHandle<Value>) {
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
                if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
                    break end + 4;
                }
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
            let tool_calls: Vec<_> = (0..calls).map(|index| {
                let (id, value) = if index == 0 { ("context-call", "native") } else { ("context-sibling", "sibling") };
                json!({"index":index,"id":id,"type":"function","function":{"name":"echo","arguments":json!({"value":value}).to_string()}})
            }).collect();
            let packet = json!({"choices":[{"index":0,"delta":{"tool_calls":tool_calls},"finish_reason":"tool_calls"}]});
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
        "context-updated-before-assistant"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.native_tool_outcome()?.is_some() {
            self.recorded
                .try_send(frame.clone())
                .map_err(|error| ProcessorError::new(error.to_string()))?;
            timeout(WAIT, self.release.acquire())
                .await
                .map_err(|_| ProcessorError::new("test final barrier timed out"))?
                .map_err(|_| ProcessorError::new("test final barrier closed"))?
                .forget();
        }
        ctx.push(frame, direction).await
    }
}

fn result_values(context: &LlmContextData) -> Vec<Value> {
    result_values_for(context, "context-call")
}

fn result_values_for(context: &LlmContextData, id: &str) -> Vec<Value> {
    context
        .messages
        .iter()
        .filter_map(|message| {
            if message.role != ChatRole::Tool || message.tool_call_id.as_deref() != Some(id) {
                return None;
            }
            let MessageContent::Text(text) = &message.content else {
                return None;
            };
            serde_json::from_str(text).ok()
        })
        .collect()
}

#[tokio::test]
async fn successful_native_final_schedules_context_updated_after_its_context_commit() {
    let (url, peer) = server(1).await;
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
    let value = json!({"value":"native","completed":true});
    let function = DirectFunction {
        schema: core::ToolDefinition {
            name: "echo".into(),
            description: "Return a native completion".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        },
        handler: LocalToolFunction::new_with_completion(move |arguments| {
            let callback = callback.clone();
            async move {
                assert_eq!(arguments, json!({"value":"native"}));
                Ok(LocalToolCompletion::new(
                    LocalToolResult::new(json!({"value":"native","completed":true}))
                        .with_run_llm(false),
                )
                .with_context_updated(callback))
            }
        }),
    };
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "Complete the native call").into()],
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
    let (recorded, mut observed) = mpsc::channel(2);
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
    let terminal = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let receipt = terminal
        .native_tool_outcome()
        .unwrap()
        .expect("real executor must admit a sealed terminal");
    assert_eq!(receipt.state(), ToolCallTerminalState::Completed);
    assert_eq!(receipt.reason(), ToolCallEndReason::CallbackReturned);
    assert_eq!(receipt.call().tool_call_id, "context-call");
    assert_eq!(receipt.call().arguments, json!({"value":"native"}));
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
    assert!(result_values(&pair.context().snapshot_rich().unwrap()).is_empty());
    assert_eq!(
        called.load(Ordering::SeqCst),
        0,
        "admission must not run the context callback"
    );

    // Inspect the actual public reducer's finite scheduling decision. It writes
    // this pair's shared history synchronously and does not execute user code.
    // Callback task ownership is deliberately left to separate pipeline tests.
    let output = pair
        .assistant()
        .process_frame(terminal, Direction::Downstream)
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
    let applied = pair.context().snapshot_rich().unwrap();
    assert_eq!(result_values(&applied), vec![value]);
    assert!(!pair.assistant().has_function_calls_in_progress().unwrap());
    assert_eq!(
        called.load(Ordering::SeqCst),
        0,
        "public reducer schedules without calling user code"
    );
    release.add_permits(1);
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(request["tools"][0]["function"]["name"], "echo");
    assert_eq!(
        pair.context().snapshot_rich().unwrap(),
        applied,
        "original receipt replay must not mutate history"
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        callbacks.len(),
        1,
        "the admitted successful final lost its context-updated scheduling action"
    );
    assert_eq!(callbacks[0], expected_callback);
}

fn completion_handler(callback: LocalAsyncCallback) -> LocalToolFunction {
    LocalToolFunction::new_with_completion(move |arguments| {
        let callback = callback.clone();
        async move {
            assert!(matches!(
                arguments["value"].as_str(),
                Some("native" | "sibling")
            ));
            Ok(LocalToolCompletion::new(
                LocalToolResult::new(json!({"value":arguments["value"],"completed":true}))
                    .with_run_llm(false),
            )
            .with_context_updated(callback))
        }
    })
}

async fn start(
    url: String,
    handler: LocalToolFunction,
) -> (PipelineWorker, LLMContextAggregatorPair, ToolOutcomeJournal) {
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "Complete the native call").into()],
            tools: ToolSet::Schema(ToolsSchema {
                standard_tools: vec![StandardTool::Direct(DirectFunction {
                    schema: core::ToolDefinition {
                        name: "echo".into(),
                        description: "Return a native completion".into(),
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

async fn joined(worker: PipelineWorker, peer: JoinHandle<Value>) -> WorkerReport {
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    let request = timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(request["tools"][0]["function"]["name"], "echo");
    report
}

#[tokio::test]
async fn actual_assistant_callback_observes_committed_result_once() {
    let (url, peer) = server(1).await;
    let context_slot = Arc::new(Mutex::new(None::<SharedContext>));
    let captured = context_slot.clone();
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let (finished, mut observed) = mpsc::channel(2);
    let callback = LocalAsyncCallback::new(move || {
        // Taking this reference breaks the temporary construction cycle before
        // user code runs; the callback future now owns the strong context clone.
        let context = captured.lock().unwrap().take().unwrap();
        let count = count.clone();
        let finished = finished.clone();
        async move {
            let values = result_values(&context.snapshot_rich()?);
            count.fetch_add(1, Ordering::SeqCst);
            finished
                .try_send(values)
                .map_err(|_| ProcessorError::new("test callback receiver closed"))?;
            Ok(())
        }
    });
    let (worker, pair, outcomes) = start(url, completion_handler(callback)).await;
    *context_slot.lock().unwrap() = Some(pair.context());
    run(&worker).await;
    let committed = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let value = json!({"value":"native","completed":true});
    assert_eq!(committed, vec![value.clone()]);
    assert!(context_slot.lock().unwrap().is_none());
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let report = joined(worker, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert!(observed.try_recv().is_err());
    assert_eq!(
        result_values(&pair.context().snapshot_rich().unwrap()),
        vec![value]
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

async fn held_callback_is_owned_until_terminal(cancel: bool) {
    let (url, peer) = server(1).await;
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
                    result_values(&context.snapshot_rich()?),
                    Arc::downgrade(&resource)
                ))
                .is_ok());
            std::future::pending::<()>().await;
            // Both values are deliberately retained across the pending await.
            drop(resource);
            drop(context);
            Ok(())
        }
    });
    let (mut worker, pair, outcomes) = start(url, completion_handler(callback)).await;
    *context_slot.lock().unwrap() = Some(pair.context());
    run(&worker).await;
    let (values, weak) = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    assert_eq!(values, vec![json!({"value":"native","completed":true})]);
    assert!(context_slot.lock().unwrap().is_none());
    assert!(weak.upgrade().is_some());
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
                panic!("unexpected native error: {error:?}");
            }
        }
    })
    .await
    .unwrap();
    let marker = json!({"fixture":"callback does not block traffic"});
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
        "interruption must preserve the callback task"
    );
    assert!(weak.upgrade().is_some());
    if cancel {
        timeout(WAIT, worker.cancel("context-updated callback fixture"))
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
    assert!(
        weak.upgrade().is_none(),
        "terminal cleanup must release the callback future's owned resource"
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(
        settled.terminal[0].outcome.state(),
        ToolCallTerminalState::Completed
    );
}

#[tokio::test]
async fn hanging_callback_survives_interruption_and_end_releases_its_captures() {
    held_callback_is_owned_until_terminal(false).await;
}

#[tokio::test]
async fn hanging_callback_survives_interruption_and_cancel_releases_its_captures() {
    held_callback_is_owned_until_terminal(true).await;
}

#[tokio::test]
async fn callback_error_is_a_pipeline_error_and_does_not_resettle_the_completed_tool() {
    let (url, peer) = server(1).await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let callback = LocalAsyncCallback::new(move || {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Err(ProcessorError::new("context-updated fixture error"))
        }
    });
    let (mut worker, pair, outcomes) = start(url, completion_handler(callback)).await;
    run(&worker).await;
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if let FrameData::Error(error) = frame.frame.data {
                assert_eq!(error.error, "context-updated fixture error");
                break;
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(
        result_values(&pair.context().snapshot_rich().unwrap()),
        vec![json!({"value":"native","completed":true})]
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
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let report = joined(worker, peer).await;
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].error, "context-updated fixture error");
    assert_eq!(outcomes.snapshot().total_terminal, 1);
}

#[tokio::test]
async fn reusing_one_callback_executes_it_once_for_each_distinct_publication() {
    let (url, peer) = server(2).await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let (finished, mut observed) = mpsc::channel(2);
    let callback = LocalAsyncCallback::new(move || {
        let count = count.clone();
        let finished = finished.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            finished
                .try_send(())
                .map_err(|_| ProcessorError::new("test callback receiver closed"))?;
            Ok(())
        }
    });
    let (worker, pair, outcomes) = start(url, completion_handler(callback)).await;
    run(&worker).await;
    for _ in 0..2 {
        timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    }
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let report = joined(worker, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(called.load(Ordering::SeqCst), 2);
    assert!(observed.try_recv().is_err());
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(
        result_values(&context),
        vec![json!({"value":"native","completed":true})]
    );
    assert_eq!(
        result_values_for(&context, "context-sibling"),
        vec![json!({"value":"sibling","completed":true})]
    );
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 2);
    assert_eq!(settled.terminal.len(), 2);
    assert!(settled
        .terminal
        .iter()
        .all(|receipt| receipt.outcome.state() == ToolCallTerminalState::Completed));
}
