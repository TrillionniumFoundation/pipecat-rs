//! Ordinary live Direct progress through real loopback HTTP, not a cloud qualification.
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
        Arc,
    },
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, oneshot, Semaphore},
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
        timeout(WAIT, async move {
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
            requests.push(serde_json::from_slice(&bytes[end..end + length]).unwrap());
            assert!(requests.len() <= 2, "unexpected additional inference");
            let packet = if requests.len() == 1 {
                json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"progress-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"native\"}"}}]},"finish_reason":"tool_calls"}]})
            } else {
                json!({"choices":[{"index":0,"delta":{"content":"continued native response"},"finish_reason":"stop"}]})
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

struct Witness(mpsc::Sender<Frame>);
#[async_trait]
impl FrameProcessor for Witness {
    fn name(&self) -> &str {
        "live-progress-before-assistant"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let captured = (frame.native_tool_progress()?.is_some()
            || frame.native_tool_outcome()?.is_some())
        .then(|| frame.clone());
        ctx.push(frame, direction).await?;
        if let Some(frame) = captured {
            self.0
                .try_send(frame)
                .map_err(|error| ProcessorError::new(error.to_string()))?;
        }
        Ok(())
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
                || outer["tool_call_id"] != "progress-call"
                || outer["status"] != status
            {
                return None;
            }
            serde_json::from_str(outer["result"].as_str()?).ok()
        })
        .collect()
}

async fn start(
    url: String,
    handler: LocalToolFunction,
) -> (
    PipelineWorker,
    LLMContextAggregatorPair,
    ToolOutcomeJournal,
    mpsc::Receiver<Frame>,
) {
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "Report live work").into()],
            tools: ToolSet::Schema(ToolsSchema {
                standard_tools: vec![StandardTool::Direct(DirectFunction {
                    schema: core::ToolDefinition {
                        name: "echo".into(),
                        description: "Report ordinary live work".into(),
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
    let (recorded, observed) = mpsc::channel(8);
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(Witness(recorded)));
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
    (worker, pair, outcomes, observed)
}

async fn wait_running(
    pair: &LLMContextAggregatorPair,
    worker: &PipelineWorker,
    values: Vec<Value>,
) {
    timeout(WAIT, async {
        loop {
            assert!(worker.errors().is_empty(), "{:?}", worker.errors());
            if async_values(&pair.context().snapshot_rich().unwrap(), "running") == values {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_idle(pair: &LLMContextAggregatorPair, worker: &PipelineWorker) {
    timeout(WAIT, async {
        loop {
            assert!(worker.errors().is_empty(), "{:?}", worker.errors());
            if !pair.assistant().has_function_calls_in_progress().unwrap() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

async fn wait_continuation(worker: &mut PipelineWorker) {
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.expect("native continuation must produce a response");
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant
                        && message.content == MessageContent::Text("continued native response".into()))) {
                break;
            }
            if let FrameData::Error(error) = frame.frame.data { panic!("unexpected native error: {error:?}"); }
        }
    }).await.unwrap();
}

async fn finish(
    worker: PipelineWorker,
    stop: oneshot::Sender<()>,
    peer: JoinHandle<Vec<Value>>,
) -> Vec<Value> {
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    requests
}

struct FutureDropped(Arc<AtomicUsize>);
impl Drop for FutureDropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn live_owned_progress_reaches_shared_history_without_settling_the_call() {
    let (url, stop, peer) = server().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let reports = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Semaphore::new(0));
    let entered = calls.clone();
    let reported = reports.clone();
    let gate = release.clone();
    let working = json!({"phase":"working","value":"native"});
    let done = json!({"phase":"done","value":"native"});
    let function = DirectFunction {
        schema: core::ToolDefinition {
            name: "echo".into(),
            description: "Report ordinary live work".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        },
        handler: LocalToolFunction::new_with_reporter(move |arguments, reporter| {
            let entered = entered.clone();
            let reported = reported.clone();
            let gate = gate.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                assert_eq!(arguments, json!({"value":"native"}));
                reporter
                    .report(
                        LocalToolResult::new(json!({"phase":"working","value":"native"}))
                            .with_run_llm(false),
                    )
                    .await?;
                reported.fetch_add(1, Ordering::SeqCst);
                gate.acquire().await.unwrap().forget();
                Ok(
                    LocalToolResult::new(json!({"phase":"done","value":"native"}))
                        .with_run_llm(false),
                )
            }
        }),
    };
    assert!(!function.handler.options().cancel_on_interruption);
    let advertised = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(function)],
        custom_tools: None,
    });
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "Report working, then done").into()],
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
    let (recorded, mut observed) = mpsc::channel(4);
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(Witness(recorded)));
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
        .expect("endpoint must mint valid sealed progress authority");
    assert!(progress.native_tool_outcome().unwrap().is_none());
    assert_eq!(authority.call().tool_call_id, "progress-call");
    assert_eq!(authority.call().function_name, "echo");
    assert_eq!(authority.call().arguments, json!({"value":"native"}));
    assert_eq!(authority.id(), progress.id);
    assert!(!authority.cancel_on_interruption());
    assert!(authority.sequence() > 0);
    let batch = authority
        .batch()
        .expect("active Direct progress must retain its admitted batch");
    assert_eq!(batch.members().len(), 1);
    assert!(!batch.members()[0].1);
    let FrameData::FunctionCallResult(value) = &progress.data else {
        panic!("owned progress must contain a result");
    };
    assert!(!value.is_final && value.error.is_none());
    assert_eq!(value.run_llm, Some(false));
    assert_eq!(value.result, working);
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.active[0].tool_call_id, "progress-call");
    assert_eq!(held.total_terminal, 0);

    let (accepted, consumer_error) = timeout(WAIT, async {
        loop {
            if async_values(&pair.context().snapshot_rich().unwrap(), "running")
                == vec![working.clone()]
            {
                break (true, None);
            }
            if let Some(error) = worker.errors().first() {
                break (false, Some(error.error.clone()));
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    release.add_permits(1);
    let final_frame = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let final_authority = final_frame
        .native_tool_outcome()
        .unwrap()
        .expect("final must have its terminal receipt");
    assert!(final_frame.native_tool_progress().unwrap().is_none());
    assert_eq!(final_authority.state(), ToolCallTerminalState::Completed);
    assert_eq!(
        final_authority.reason(),
        ToolCallEndReason::CallbackReturned
    );
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["tools"][0]["function"]["name"], "echo");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(reports.load(Ordering::SeqCst), 1);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert!(accepted, "valid owned progress must be consumed as intermediate history; consumer_error={consumer_error:?}");
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(context.tools, advertised);
    assert_eq!(async_values(&context, "running"), vec![working]);
    assert_eq!(async_values(&context, "finished"), vec![done]);
}

#[tokio::test]
async fn progress_does_not_extend_the_original_deadline_and_escaped_reporter_is_revoked() {
    let (url, stop, peer) = server().await;
    let (escaped, mut reporters) = mpsc::channel(1);
    let dropped = Arc::new(AtomicUsize::new(0));
    let on_drop = dropped.clone();
    let first = json!({"phase":"first"});
    let second = json!({"phase":"second"});
    let handler = LocalToolFunction::new_with_reporter(move |arguments, reporter| {
        let escaped = escaped.clone();
        let on_drop = on_drop.clone();
        async move {
            let entered = Instant::now();
            let _drop = FutureDropped(on_drop);
            assert_eq!(arguments, json!({"value":"native"}));
            reporter
                .report(LocalToolResult::new(json!({"phase":"first"})).with_run_llm(false))
                .await?;
            assert!(escaped.try_send((reporter, entered)).is_ok());
            std::future::pending::<Result<LocalToolResult, ProcessorError>>().await
        }
    })
    .with_options(LocalToolOptions {
        cancel_on_interruption: false,
        timeout: Some(Duration::from_secs(1)),
    })
    .unwrap();
    let (mut worker, pair, outcomes, mut observed) = start(url, handler).await;
    let first_frame = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let first_authority = first_frame.native_tool_progress().unwrap().unwrap();
    assert_eq!(first_authority.sequence(), 1);
    assert!(first_frame.native_tool_outcome().unwrap().is_none());
    wait_running(&pair, &worker, vec![first.clone()]).await;
    let (reporter, entered) = timeout(WAIT, reporters.recv()).await.unwrap().unwrap();
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
        reporter.report(LocalToolResult::new(second.clone()).with_run_llm(false)),
    )
    .await
    .unwrap()
    .unwrap();
    let second_frame = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let second_authority = second_frame.native_tool_progress().unwrap().unwrap();
    assert_eq!(second_authority.sequence(), 2);
    assert_eq!(second_authority.call(), first_authority.call());
    wait_running(&pair, &worker, vec![first.clone(), second.clone()]).await;
    assert_eq!(outcomes.snapshot().active.len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 0);
    let terminal = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let elapsed = entered.elapsed();
    let authority = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(authority.state(), ToolCallTerminalState::Failed);
    assert_eq!(authority.reason(), ToolCallEndReason::TimedOut);
    assert!(
        elapsed < Duration::from_millis(1450),
        "progress reset the deadline: {elapsed:?}"
    );
    let FrameData::FunctionCallResult(result) = &terminal.data else {
        panic!("timeout must publish a result");
    };
    assert!(result.is_final && result.error.is_some());
    assert_eq!(result.run_llm, None);
    let late = LocalToolResult::new(json!({"phase":"too late"})).with_run_llm(false);
    assert!(timeout(WAIT, reporter.report(late)).await.unwrap().is_err());
    wait_continuation(&mut worker).await;
    wait_idle(&pair, &worker).await;
    assert_eq!(
        async_values(&pair.context().snapshot_rich().unwrap(), "running"),
        vec![first, second]
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        observed.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(settled.terminal.len(), 1);
    let requests = finish(worker, stop, peer).await;
    assert_eq!(
        requests.len(),
        2,
        "timeout's default continuation must receive a healthy native response"
    );
}

#[tokio::test]
async fn explicit_cancel_drops_the_live_future_and_revokes_all_escaped_reporters() {
    let (url, stop, peer) = server().await;
    let (escaped, mut reporters) = mpsc::channel(1);
    let dropped = Arc::new(AtomicUsize::new(0));
    let on_drop = dropped.clone();
    let working = json!({"phase":"working"});
    let handler = LocalToolFunction::new_with_reporter(move |arguments, reporter| {
        let escaped = escaped.clone();
        let on_drop = on_drop.clone();
        async move {
            let _drop = FutureDropped(on_drop);
            assert_eq!(arguments, json!({"value":"native"}));
            reporter
                .report(LocalToolResult::new(json!({"phase":"working"})).with_run_llm(false))
                .await?;
            assert!(escaped.try_send(reporter).is_ok());
            std::future::pending::<Result<LocalToolResult, ProcessorError>>().await
        }
    });
    let (worker, pair, outcomes, mut observed) = start(url, handler).await;
    let progress = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    assert!(progress.native_tool_progress().unwrap().is_some());
    wait_running(&pair, &worker, vec![working.clone()]).await;
    let reporter = timeout(WAIT, reporters.recv()).await.unwrap().unwrap();
    let cloned = reporter.clone();
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.total_terminal, 0);
    timeout(
        WAIT,
        worker.sender().send(
            Frame::new(FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "echo".into(),
                tool_call_id: "progress-call".into(),
                run_llm: false,
            })),
            Direction::Downstream,
        ),
    )
    .await
    .unwrap()
    .unwrap();
    let terminal = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let authority = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(authority.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(authority.reason(), ToolCallEndReason::ExplicitCancel);
    assert!(terminal.native_tool_progress().unwrap().is_none());
    wait_idle(&pair, &worker).await;
    let before = pair.context().snapshot_rich().unwrap();
    for endpoint in [reporter, cloned] {
        assert!(timeout(
            WAIT,
            endpoint.report(LocalToolResult::new(json!({"phase":"late"})).with_run_llm(false))
        )
        .await
        .unwrap()
        .is_err());
    }
    assert_eq!(pair.context().snapshot_rich().unwrap(), before);
    assert_eq!(async_values(&before, "running"), vec![working]);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(matches!(
        observed.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(settled.terminal.len(), 1);
    assert_eq!(finish(worker, stop, peer).await.len(), 1);
}

#[tokio::test]
async fn async_false_progress_survives_interruption_then_finishes_once() {
    let (url, stop, peer) = server().await;
    let (escaped, mut reporters) = mpsc::channel(1);
    let release = Arc::new(Semaphore::new(0));
    let gate = release.clone();
    let first = json!({"phase":"before interruption"});
    let second = json!({"phase":"after interruption"});
    let done = json!({"phase":"done"});
    let handler = LocalToolFunction::new_with_reporter(move |arguments, reporter| {
        let escaped = escaped.clone();
        let gate = gate.clone();
        async move {
            assert_eq!(arguments, json!({"value":"native"}));
            reporter
                .report(
                    LocalToolResult::new(json!({"phase":"before interruption"}))
                        .with_run_llm(false),
                )
                .await?;
            assert!(escaped.try_send(reporter).is_ok());
            gate.acquire().await.unwrap().forget();
            Ok(LocalToolResult::new(json!({"phase":"done"})).with_run_llm(false))
        }
    });
    let (mut worker, pair, outcomes, mut observed) = start(url, handler).await;
    let first_frame = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let first_authority = first_frame.native_tool_progress().unwrap().unwrap();
    assert!(!first_authority.cancel_on_interruption());
    wait_running(&pair, &worker, vec![first.clone()]).await;
    let reporter = timeout(WAIT, reporters.recv()).await.unwrap().unwrap();
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
    timeout(
        WAIT,
        reporter.report(LocalToolResult::new(second.clone()).with_run_llm(false)),
    )
    .await
    .unwrap()
    .unwrap();
    let second_frame = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let second_authority = second_frame.native_tool_progress().unwrap().unwrap();
    assert_eq!(second_authority.call(), first_authority.call());
    assert_eq!(second_authority.sequence(), first_authority.sequence() + 1);
    assert!(!second_authority.cancel_on_interruption());
    wait_running(&pair, &worker, vec![first.clone(), second.clone()]).await;
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.total_terminal, 0);
    release.add_permits(1);
    let terminal = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let authority = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(authority.state(), ToolCallTerminalState::Completed);
    assert_eq!(authority.reason(), ToolCallEndReason::CallbackReturned);
    wait_idle(&pair, &worker).await;
    assert!(matches!(
        observed.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    let requests = finish(worker, stop, peer).await;
    assert_eq!(requests.len(), 1);
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(async_values(&context, "running"), vec![first, second]);
    assert_eq!(async_values(&context, "finished"), vec![done]);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
}

#[tokio::test]
async fn live_progress_true_runs_native_inference_while_the_same_handler_is_held() {
    let (url, stop, peer) = server().await;
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    let release = Arc::new(Semaphore::new(0));
    let gate = release.clone();
    let working = json!({"phase":"working","value":"native"});
    let done = json!({"phase":"done"});
    let handler = LocalToolFunction::new_with_reporter(move |arguments, reporter| {
        let entered = entered.clone();
        let gate = gate.clone();
        async move {
            entered.fetch_add(1, Ordering::SeqCst);
            assert_eq!(arguments, json!({"value":"native"}));
            reporter
                .report(
                    LocalToolResult::new(json!({"phase":"working","value":"native"}))
                        .with_run_llm(true),
                )
                .await?;
            gate.acquire().await.unwrap().forget();
            Ok(LocalToolResult::new(json!({"phase":"done"})).with_run_llm(false))
        }
    });
    let (mut worker, pair, outcomes, mut observed) = start(url, handler).await;
    let progress = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    assert!(progress.native_tool_progress().unwrap().is_some());
    assert!(progress.native_tool_outcome().unwrap().is_none());
    let FrameData::FunctionCallResult(result) = &progress.data else {
        panic!("progress result required");
    };
    assert!(!result.is_final);
    assert_eq!(result.run_llm, Some(true));
    assert_eq!(result.result, working);
    // This proves native loopback continuation with an active async call, not
    // acceptance of the pending context by a hosted provider.
    wait_continuation(&mut worker).await;
    let held = outcomes.snapshot();
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.active[0].tool_call_id, "progress-call");
    assert_eq!(held.total_terminal, 0);
    release.add_permits(1);
    let terminal = timeout(WAIT, observed.recv()).await.unwrap().unwrap();
    let authority = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(authority.state(), ToolCallTerminalState::Completed);
    assert_eq!(authority.reason(), ToolCallEndReason::CallbackReturned);
    let FrameData::FunctionCallResult(result) = &terminal.data else {
        panic!("terminal result required");
    };
    assert!(result.is_final);
    assert_eq!(result.run_llm, Some(false));
    wait_idle(&pair, &worker).await;
    let requests = finish(worker, stop, peer).await;
    assert_eq!(
        requests.len(),
        2,
        "the final false result must add no third request"
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| {
            message["content"]
                .as_str()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                .is_some_and(|outer| {
                    outer["type"] == "async_tool"
                        && outer["status"] == "running"
                        && outer["tool_call_id"] == "progress-call"
                        && outer["result"]
                            .as_str()
                            .and_then(|text| serde_json::from_str::<Value>(text).ok())
                            == Some(working.clone())
                })
        }));
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(async_values(&context, "running"), vec![working]);
    assert_eq!(async_values(&context, "finished"), vec![done]);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
}
