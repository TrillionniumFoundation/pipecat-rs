//! Real Responses HTTP on loopback; no hosted-provider qualification.
#![cfg(feature = "responses")]
use pipecat::{
    core::{self, *},
    pipeline::DirectToolBinding,
    responses::{
        InputItem, ResponseEvent, ResponseStatus, ResponsesConfig, ResponsesContext,
        ResponsesHttpClient, ResponsesProcessor, ResponsesRequest,
        ToolDefinition as ResponseToolDefinition,
    },
    services::Role,
    turns::LLMContextAggregatorPair,
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
    sync::{oneshot, Semaphore},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);

fn records(tool_scenario: bool, request: usize) -> Vec<Value> {
    let id = format!("response-{request}");
    let mut events = vec![json!({"type":"response.created","response":{"id":id}})];
    if tool_scenario && request == 1 {
        let call = json!({"type":"function_call","id":"function-item","call_id":"responses-direct-call","name":"echo","arguments":""});
        let done = json!({"type":"function_call","id":"function-item","call_id":"responses-direct-call","name":"echo","arguments":"{\"value\":\"native\"}"});
        events.extend([
            json!({"type":"response.output_item.added","item":call}),
            json!({"type":"response.function_call_arguments.delta","item_id":"function-item","delta":"{\"value\":"}),
            json!({"type":"response.function_call_arguments.delta","item_id":"function-item","delta":"\"native\"}"}),
            json!({"type":"response.function_call_arguments.done","item_id":"function-item","arguments":"{\"value\":\"native\"}"}),
            json!({"type":"response.output_item.done","item":done.clone()}),
            json!({"type":"response.completed","response":{"id":id,"model":"gpt-4.1","output":[done]}}),
        ]);
    } else {
        let text = if tool_scenario {
            "Responses tool loop complete"
        } else {
            "healthy Responses control"
        };
        events.extend([
            json!({"type":"response.output_text.delta","delta":text}),
            json!({"type":"response.completed","response":{"id":id,"model":"gpt-4.1","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}]}}),
        ]);
    }
    events
}

async fn server(
    tool_scenario: bool,
) -> (
    String,
    oneshot::Sender<()>,
    JoinHandle<Vec<Value>>,
    Arc<AtomicUsize>,
) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let (stop, mut stopped) = oneshot::channel();
    let io_count = Arc::new(AtomicUsize::new(0));
    let count = io_count.clone();
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
                count.fetch_add(1, Ordering::SeqCst);
                assert!(requests.len() <= if tool_scenario { 2 } else { 1 }, "unexpected Responses replay");
                let body: String = records(tool_scenario, requests.len()).into_iter().map(|event|
                    format!("event: {}\ndata: {event}\n\n", event["type"].as_str().unwrap())).collect();
                let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
            }
            requests
        }).await.unwrap()
    });
    (url, stop, task, io_count)
}

fn config(url: String) -> ResponsesConfig {
    ResponsesConfig {
        http_url: url,
        request_timeout: WAIT,
        idle_timeout: WAIT,
        connect_timeout: WAIT,
        drain_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

fn schema() -> core::ToolDefinition {
    core::ToolDefinition {
        name: "echo".into(),
        description: "Echo an application value".into(),
        parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
    }
}

async fn healthy_plain_function_control() {
    let (url, stop, peer, _) = server(false).await;
    let client = ResponsesHttpClient::new(config(url)).unwrap();
    let definition = schema();
    let mut context =
        ResponsesContext::new(vec![InputItem::text(Role::User, "Check native Responses")]);
    context.tools = Some(vec![ResponseToolDefinition {
        name: definition.name,
        description: definition.description,
        parameters: definition.parameters,
    }]);
    let mut stream = client
        .stream(ResponsesRequest::new("gpt-4.1", context))
        .unwrap();
    let events = timeout(WAIT, async {
        let mut events = vec![];
        while let Some(event) = stream.recv().await {
            events.push(event.unwrap());
        }
        events
    })
    .await
    .unwrap();
    timeout(WAIT, stream.close()).await.unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(
        requests.len(),
        1,
        "the health control has its own request count"
    );
    assert_eq!(requests[0]["tools"][0]["type"], "function");
    assert_eq!(requests[0]["tools"][0]["name"], "echo");
    assert!(events.iter().any(|event| matches!(event, ResponseEvent::TextDelta(text) if text == "healthy Responses control")));
    assert!(events.iter().any(|event| matches!(
        event,
        ResponseEvent::Finished {
            status: ResponseStatus::Completed,
            ..
        }
    )));
}

struct TerminalWitness(Arc<Mutex<Vec<Frame>>>);
#[async_trait]
impl FrameProcessor for TerminalWitness {
    fn name(&self) -> &str {
        "responses-direct-terminal-witness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.native_tool_outcome()?.is_some() {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn schema_direct_handler_completes_native_responses_tool_result_and_second_request() {
    healthy_plain_function_control().await;
    let (url, stop, peer, _) = server(true).await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let value = json!({"value":"native","source":"application"});
    let function = DirectFunction {
        schema: schema(),
        handler: LocalToolFunction::new_with_result(move |arguments| {
            let count = count.clone();
            async move {
                assert_eq!(arguments, json!({"value":"native"}));
                count.fetch_add(1, Ordering::SeqCst);
                Ok(
                    LocalToolResult::new(json!({"value":"native","source":"application"}))
                        .with_run_llm(true),
                )
            }
        }),
    };
    let advertised = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(function)],
        custom_tools: None,
    });
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![
                ChatMessage::new(ChatRole::User, "Call echo and explain its result").into(),
            ],
            tools: advertised.clone(),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let llm = Arc::new(
        ResponsesProcessor::http(
            config(url),
            ResponsesRequest::new("gpt-4.1", ResponsesContext::default()),
        )
        .unwrap(),
    );
    let binding = DirectToolBinding::new_responses(llm).unwrap();
    let outcomes = binding.outcomes();
    let terminal = Arc::new(Mutex::new(vec![]));
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(TerminalWitness(terminal.clone())));
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
    let rejected = timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.expect("Responses pipeline must produce a concrete outcome");
            if let FrameData::Error(error) = frame.frame.data { break Some(error); }
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant && message.content == MessageContent::Text("Responses tool loop complete".into()))) { break None; }
        }
    }).await.unwrap();
    if let Some(error) = &rejected {
        assert!(
            error
                .error
                .contains("direct tool handlers must be registered with a tool executor"),
            "unexpected error: {error:?}"
        );
    }
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(report.errors.len(), usize::from(rejected.is_some()));
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(context.tools, advertised);
    let results: Vec<Value> = context
        .messages
        .iter()
        .filter_map(|message| {
            if message.role != ChatRole::Tool
                || message.tool_call_id.as_deref() != Some("responses-direct-call")
            {
                return None;
            }
            let MessageContent::Text(text) = &message.content else {
                return None;
            };
            serde_json::from_str(text).ok()
        })
        .collect();
    let completed = context
        .messages
        .iter()
        .filter(|message| {
            message.role == ChatRole::Assistant
                && message.content == MessageContent::Text("Responses tool loop complete".into())
        })
        .count();
    let owned = terminal.lock().unwrap().len();
    // Health-control HTTP1 is deliberately excluded from this scenario tuple.
    assert_eq!((called.load(Ordering::SeqCst), requests.len(), results, completed, owned),
        (1, 2, vec![value.clone()], 1, 1), "Responses Schema Direct has no executable tool-result continuation; concrete_rejection={rejected:?}");
    assert_eq!(requests[0]["tools"][0]["type"], "function");
    assert_eq!(requests[0]["tools"][0]["name"], "echo");
    assert!(requests[1]["input"]
        .as_array()
        .unwrap()
        .iter()
        .any(|item| item["type"] == "function_call_output"
            && item["call_id"] == "responses-direct-call"
            && item["output"]
                .as_str()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                == Some(value.clone())));
    let frame = terminal.lock().unwrap()[0].clone();
    let receipt = frame.native_tool_outcome().unwrap().unwrap();
    assert_eq!(receipt.state(), ToolCallTerminalState::Completed);
    assert_eq!(receipt.reason(), ToolCallEndReason::CallbackReturned);
    assert_eq!(receipt.call().tool_call_id, "responses-direct-call");
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
}

fn advertised(handler: LocalToolFunction) -> ToolSet {
    ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(DirectFunction {
            schema: schema(),
            handler,
        })],
        custom_tools: None,
    })
}

fn rich(tools: ToolSet) -> LlmContextData {
    LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, "Call echo and explain its result").into()],
        tools,
        tool_choice: None,
    }
}

async fn start(
    pair: &LLMContextAggregatorPair,
    binding: &DirectToolBinding,
    terminal: Arc<Mutex<Vec<Frame>>>,
    user: bool,
) -> PipelineWorker {
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![];
    if user {
        stages.push(pair.user());
    }
    stages.extend(binding.stages());
    stages.push(Arc::new(TerminalWitness(terminal)));
    stages.push(pair.assistant());
    timeout(WAIT, Pipeline::new(stages).start(PipelineConfig::default()))
        .await
        .unwrap()
        .unwrap()
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

async fn completed(worker: &mut PipelineWorker) {
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant && message.content == MessageContent::Text("Responses tool loop complete".into()))) { break; }
            if let FrameData::Error(error) = frame.frame.data { panic!("unexpected Responses error: {error:?}"); }
        }
    }).await.unwrap();
}

async fn rejection(worker: &mut PipelineWorker) -> ErrorFrame {
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.unwrap();
            if let FrameData::Error(error) = frame.frame.data {
                break error;
            }
        }
    })
    .await
    .unwrap()
}

async fn joined(
    worker: PipelineWorker,
    stop: oneshot::Sender<()>,
    peer: JoinHandle<Vec<Value>>,
) -> (WorkerReport, Vec<Value>) {
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    (report, requests)
}

struct FutureDropped(Arc<AtomicUsize>);
impl Drop for FutureDropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn explicit_cancel_settles_a_held_responses_handler_without_another_request() {
    for cancel_on_interruption in [false, true] {
        let (url, stop, peer, io_count) = server(true).await;
        let called = Arc::new(AtomicUsize::new(0));
        let count = called.clone();
        let dropped = Arc::new(AtomicUsize::new(0));
        let on_drop = dropped.clone();
        let entered = Arc::new(Semaphore::new(0));
        let started = entered.clone();
        let handler = LocalToolFunction::new_with_result(move |arguments| {
            let count = count.clone();
            let on_drop = on_drop.clone();
            let started = started.clone();
            async move {
                let _drop = FutureDropped(on_drop);
                assert_eq!(arguments, json!({"value":"native"}));
                count.fetch_add(1, Ordering::SeqCst);
                started.add_permits(1);
                std::future::pending::<Result<LocalToolResult, ProcessorError>>().await
            }
        })
        .with_options(LocalToolOptions {
            cancel_on_interruption,
            timeout: None,
        })
        .unwrap();
        let pair = LLMContextAggregatorPair::new_rich(
            rich(advertised(handler)),
            Default::default(),
            Default::default(),
        );
        let llm = Arc::new(
            ResponsesProcessor::http(
                config(url),
                ResponsesRequest::new("gpt-4.1", ResponsesContext::default()),
            )
            .unwrap(),
        );
        let binding = DirectToolBinding::new_responses(llm).unwrap();
        let outcomes = binding.outcomes();
        let terminal = Arc::new(Mutex::new(vec![]));
        let worker = start(&pair, &binding, terminal.clone(), true).await;
        send(&worker, FrameData::LlmRun).await;
        timeout(WAIT, entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert_eq!(io_count.load(Ordering::SeqCst), 1);
        assert_eq!(outcomes.snapshot().active.len(), 1);
        assert_eq!(outcomes.snapshot().total_terminal, 0);
        send(
            &worker,
            FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                function_name: "echo".into(),
                tool_call_id: "responses-direct-call".into(),
                run_llm: false,
            }),
        )
        .await;
        timeout(WAIT, async {
            loop {
                assert!(worker.errors().is_empty(), "{:?}", worker.errors());
                if terminal.lock().unwrap().len() == 1
                    && !pair.assistant().has_function_calls_in_progress().unwrap()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let frame = terminal.lock().unwrap()[0].clone();
        let receipt = frame.native_tool_outcome().unwrap().unwrap();
        assert_eq!(receipt.state(), ToolCallTerminalState::Cancelled);
        assert_eq!(receipt.reason(), ToolCallEndReason::ExplicitCancel);
        assert_eq!(receipt.call().tool_call_id, "responses-direct-call");
        let (report, requests) = joined(worker, stop, peer).await;
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(
            requests.len(),
            1,
            "cancellation must not fabricate a second Responses request"
        );
        assert_eq!(called.load(Ordering::SeqCst), 1);
        assert_eq!(dropped.load(Ordering::SeqCst), 1);
        let settled = outcomes.snapshot();
        assert!(settled.active.is_empty());
        assert_eq!(settled.total_terminal, 1);
        assert_eq!(settled.terminal.len(), 1);
    }
}

#[tokio::test]
async fn failed_full_responses_preflight_preserves_old_handlers_and_cleanup_inventory() {
    let (url, stop, peer, io_count) = server(true).await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let old_cleanup = Arc::new(AtomicUsize::new(0));
    let cleanup = old_cleanup.clone();
    let handler = LocalToolFunction::new_with_result(move |arguments| {
        let count = count.clone();
        async move {
            assert_eq!(arguments, json!({"value":"native"}));
            count.fetch_add(1, Ordering::SeqCst);
            Ok(
                LocalToolResult::new(json!({"value":"native","source":"application"}))
                    .with_run_llm(true),
            )
        }
    })
    .with_cleanup(LocalAsyncCallback::new(move || {
        let cleanup = cleanup.clone();
        async move {
            cleanup.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }));
    let old_tools = advertised(handler);
    let mut context = rich(old_tools.clone());
    context.tool_choice = Some(core::ToolChoice::Function("echo".into()));
    let pair = LLMContextAggregatorPair::new_rich(context, Default::default(), Default::default());
    let definition = schema();
    let mut base = ResponsesContext::new(vec![InputItem::text(
        Role::User,
        "Call echo and explain its result",
    )]);
    base.tools = Some(vec![ResponseToolDefinition {
        name: definition.name,
        description: definition.description,
        parameters: definition.parameters,
    }]);
    base.tool_choice = Some(pipecat::responses::ToolChoice::Function("echo".into()));
    let llm = Arc::new(
        ResponsesProcessor::http(config(url), ResponsesRequest::new("gpt-4.1", base)).unwrap(),
    );
    let binding = DirectToolBinding::new_responses(llm.clone()).unwrap();
    let outcomes = binding.outcomes();
    let terminal = Arc::new(Mutex::new(vec![]));
    // Send context/setting boundaries directly into the owner, so this test
    // measures owner publication rather than unrelated upstream reducer state.
    let mut worker = start(&pair, &binding, terminal.clone(), false).await;
    send(&worker, FrameData::LlmSetTools(old_tools)).await;
    let marker = Frame::new(FrameData::OutputTransportMessage(
        json!({"fixture":"old registry admitted"}),
    ));
    let marker_id = marker.id;
    timeout(WAIT, worker.sender().send(marker, Direction::Downstream))
        .await
        .unwrap()
        .unwrap();
    timeout(WAIT, async {
        loop {
            if worker.recv().await.unwrap().frame.id == marker_id {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        worker.errors().is_empty(),
        "old registry must be admitted: {:?}",
        worker.errors()
    );
    let original_request = llm.request_snapshot().unwrap();
    let new_called = Arc::new(AtomicUsize::new(0));
    let count = new_called.clone();
    let new_cleanup = Arc::new(AtomicUsize::new(0));
    let cleanup = new_cleanup.clone();
    let replacement = LocalToolFunction::new_with_result(move |_| {
        let count = count.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(LocalToolResult::new(json!("replacement")).with_run_llm(false))
        }
    })
    .with_cleanup(LocalAsyncCallback::new(move || {
        let cleanup = cleanup.clone();
        async move {
            cleanup.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }));
    let mut replacement_schema = schema();
    replacement_schema.name = "replacement".into();
    let replacement_tools = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(DirectFunction {
            schema: replacement_schema,
            handler: replacement,
        })],
        custom_tools: None,
    });
    let mut bad_message = ChatMessage::new(ChatRole::User, "Responses forbids this name");
    bad_message.name = Some("unsupported-name".into());
    let bad_detailed = LlmContextData {
        messages: vec![bad_message.clone().into()],
        tools: replacement_tools.clone(),
        tool_choice: Some(core::ToolChoice::Function("replacement".into())),
    };
    for (data, expected) in [
        (
            FrameData::LlmContextDetailed(DetailedLlmContextFrame {
                context: bad_detailed,
                speculation: false,
            }),
            "Responses message name",
        ),
        (
            FrameData::LlmContext(LlmContextFrame {
                messages: vec![bad_message],
                tools: vec![],
                tool_choice: None,
            }),
            "Responses message name",
        ),
        (
            FrameData::LlmSetTools(replacement_tools),
            "tool choice names an undeclared function",
        ),
    ] {
        send(&worker, data).await;
        let error = rejection(&mut worker).await;
        assert!(
            error.error.contains(expected),
            "unexpected preflight error: {error:?}"
        );
        assert_eq!(io_count.load(Ordering::SeqCst), 0);
        assert_eq!(llm.request_snapshot().unwrap(), original_request);
        assert_eq!(called.load(Ordering::SeqCst), 0);
        assert_eq!(new_called.load(Ordering::SeqCst), 0);
        assert_eq!(old_cleanup.load(Ordering::SeqCst), 0);
        assert_eq!(new_cleanup.load(Ordering::SeqCst), 0);
    }
    // Run the retained request without republishing its schema: a fresh valid
    // context here would conceal an erroneous prune by a rejected boundary.
    send(&worker, FrameData::LlmRun).await;
    completed(&mut worker).await;
    let (report, requests) = joined(worker, stop, peer).await;
    assert_eq!(report.errors.len(), 3);
    assert_eq!(requests.len(), 2);
    assert_eq!(
        called.load(Ordering::SeqCst),
        1,
        "the old selected handler must remain executable"
    );
    assert_eq!(new_called.load(Ordering::SeqCst), 0);
    assert_eq!(old_cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(
        new_cleanup.load(Ordering::SeqCst),
        0,
        "rejected context must not register its cleanup hook"
    );
    assert_eq!(terminal.lock().unwrap().len(), 1);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
}

#[tokio::test]
async fn responses_owner_preserves_encrypted_reasoning_through_real_tool_continuation() {
    let (url, stop, peer, _) = server(true).await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let handler = LocalToolFunction::new_with_result(move |arguments| {
        let count = count.clone();
        async move {
            assert_eq!(arguments, json!({"value":"native"}));
            count.fetch_add(1, Ordering::SeqCst);
            Ok(
                LocalToolResult::new(json!({"value":"native","source":"application"}))
                    .with_run_llm(true),
            )
        }
    });
    let mut context = rich(advertised(handler));
    let mut reasoning: ContextMessage = ChatMessage::new(ChatRole::Assistant, "").into();
    reasoning.extra.insert("openai_responses_reasoning".into(), json!({
        "id":"reasoning-input","summary":["Preserved provider reasoning"],"encrypted_content":"synthetic-opaque-content",
    }));
    context.messages.push(reasoning);
    context
        .messages
        .push(ChatMessage::new(ChatRole::User, "Now call echo").into());
    let pair = LLMContextAggregatorPair::new_rich(context, Default::default(), Default::default());
    let llm = Arc::new(
        ResponsesProcessor::http(
            config(url),
            ResponsesRequest::new("gpt-4.1", ResponsesContext::default()),
        )
        .unwrap(),
    );
    let binding = DirectToolBinding::new_responses(llm.clone()).unwrap();
    let outcomes = binding.outcomes();
    let terminal = Arc::new(Mutex::new(vec![]));
    let mut worker = start(&pair, &binding, terminal.clone(), true).await;
    send(&worker, FrameData::LlmRun).await;
    completed(&mut worker).await;
    let snapshot = llm.request_snapshot().unwrap();
    assert!(snapshot.context.items.iter().any(|item| matches!(item, InputItem::Reasoning(reasoning)
        if reasoning.id == "reasoning-input" && reasoning.encrypted_content.as_deref() == Some("synthetic-opaque-content"))));
    let (report, requests) = joined(worker, stop, peer).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(requests.len(), 2);
    for request in &requests {
        assert!(request["input"]
            .as_array()
            .unwrap()
            .iter()
            .any(|item| item["type"] == "reasoning"
                && item["id"] == "reasoning-input"
                && item["encrypted_content"] == "synthetic-opaque-content"
                && item["summary"]
                    == json!([{"type":"summary_text","text":"Preserved provider reasoning"}])));
    }
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert_eq!(terminal.lock().unwrap().len(), 1);
    assert_eq!(outcomes.snapshot().total_terminal, 1);
}
