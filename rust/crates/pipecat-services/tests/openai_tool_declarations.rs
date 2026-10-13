use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig};
use pipecat_services::{
    frame_context, ClientConfig, CompletionRequest, Context, InferenceOptions, LlmClient,
    LlmInference, LlmProcessor, Provider, ServiceError,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};

struct Server {
    url: String,
    requests: Arc<Mutex<Vec<Value>>>,
    task: JoinHandle<()>,
}
impl Server {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/completion", listener.local_addr().unwrap());
        let requests = Arc::new(Mutex::new(vec![]));
        let shared = requests.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let (socket, _) = accepted.unwrap();
                        let requests = shared.clone();
                        connections.spawn(async move { respond(socket, requests).await; });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {},
                }
            }
        });
        Self {
            url,
            requests,
            task,
        }
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn respond(mut socket: TcpStream, requests: Arc<Mutex<Vec<Value>>>) {
    let mut bytes = vec![];
    let end = loop {
        let mut chunk = [0; 4096];
        let Ok(n) = socket.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break end + 4;
        }
        assert!(bytes.len() < 64 * 1024);
    };
    let headers = String::from_utf8_lossy(&bytes[..end]);
    let length: usize = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    assert!(length < 64 * 1024);
    while bytes.len() < end + length {
        let mut chunk = [0; 4096];
        let Ok(n) = socket.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    let request: Value = serde_json::from_slice(&bytes[end..end + length]).unwrap();
    requests.lock().unwrap().push(request);
    let body = format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":{"content":"declared","tool_calls":[{"index":0,"id":"declared-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"native\"}"}}]}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]})
    );
    let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}
fn definition() -> core::ToolDefinition {
    core::ToolDefinition {
        name: "echo".into(),
        description: "Echo value".into(),
        parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
    }
}
fn schema() -> core::ToolsSchema {
    core::ToolsSchema {
        standard_tools: vec![core::StandardTool::Function(definition())],
        custom_tools: Some(BTreeMap::from([(
            core::AdapterType::Gemini,
            vec![BTreeMap::from([("google_search".into(), json!({}))])],
        )])),
    }
}
fn context(schema: core::ToolsSchema) -> core::LlmContextData {
    core::LlmContextData {
        messages: vec![core::ChatMessage::new(core::ChatRole::User, "declare echo").into()],
        tools: core::ToolSet::Schema(schema),
        tool_choice: Some(core::ToolChoice::Function("echo".into())),
    }
}
fn processor(server: &Server, provider: Provider) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(provider, "owned-native-fixture");
    config.endpoint = server.url.clone();
    config.api_key = None;
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
fn assert_wire(request: &Value) {
    assert_eq!(
        request["tools"],
        json!([{"type":"function","function":{"name":"echo","description":"Echo value","parameters":definition().parameters}}])
    );
    assert_eq!(
        request["tool_choice"],
        json!({"type":"function","function":{"name":"echo"}})
    );
    assert!(!request.to_string().contains("google_search"));
}
async fn pipeline_round_trip(setter: bool) {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let schema = schema();
    let input = context(schema.clone());
    if setter {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetTools(input.tools.clone())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetToolChoice(core::ToolChoice::Function(
                    "echo".into(),
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
    } else {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContextDetailed(
                    core::DetailedLlmContextFrame {
                        context: input.clone(),
                        speculation: false,
                    },
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    let (calls, failure) = tokio::time::timeout(Duration::from_secs(3), async {
        let mut calls = vec![];
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::FunctionCallsStarted(batch) => calls.extend(batch),
                FrameData::LlmFullResponseEnd(_) | FrameData::LlmResponseEnd => {
                    return (calls, None)
                }
                FrameData::Error(error) => return (calls, Some(error.error)),
                _ => {}
            }
        }
        (calls, None)
    })
    .await
    .unwrap();
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "foreign adapter declarations must allow a real OpenAI request; failure={failure:?}, report={:?}", report.errors);
    assert_eq!(
        calls,
        vec![core::FunctionCall {
            function_name: "echo".into(),
            tool_call_id: "declared-call".into(),
            arguments: json!({"value":"native"})
        }]
    );
    assert!(failure.is_none());
    assert!(report.errors.is_empty());
    assert_wire(&requests[0]);
    assert_eq!(input.tools, core::ToolSet::Schema(schema));
}

#[tokio::test]
async fn openai_context_ignores_gemini_tools_and_returns_a_real_native_function_call() {
    pipeline_round_trip(false).await;
}
#[tokio::test]
async fn openai_setter_ignores_gemini_tools_and_returns_a_real_native_function_call() {
    pipeline_round_trip(true).await;
}
#[tokio::test]
async fn openai_out_of_band_inference_ignores_gemini_tools_without_mutating_input() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let input = context(schema());
    let result = processor
        .run_inference(input.clone(), InferenceOptions::default())
        .await;
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "foreign adapter declarations must allow real inference; result={result:?}"
    );
    assert_eq!(result.unwrap(), Some("declared".into()));
    assert_wire(&requests[0]);
    assert_eq!(input, context(schema()));
    assert!(processor
        .request_snapshot()
        .unwrap()
        .context
        .tools
        .is_none());
}

fn own_custom() -> core::ToolsSchema {
    let mut schema = schema();
    schema.custom_tools.as_mut().unwrap().insert(
        core::AdapterType::Openai,
        vec![BTreeMap::from([("type".into(), json!("tool_search"))])],
    );
    schema
}
fn own_declared_schema() -> core::ToolsSchema {
    let mut schema = own_custom();
    let declaration = &mut schema
        .custom_tools
        .as_mut()
        .unwrap()
        .get_mut(&core::AdapterType::Openai)
        .unwrap()[0];
    declaration.insert(
        "options".into(),
        json!({"limit":2,"filters":["public","docs"]}),
    );
    schema
}
fn assert_own_declared_wire(request: &Value, schema: &core::ToolsSchema) {
    let own = &schema.custom_tools.as_ref().unwrap()[&core::AdapterType::Openai];
    assert_eq!(
        request["tools"],
        json!([
            {"type":"function","function":{"name":"echo","description":"Echo value","parameters":definition().parameters}},
            own[0],
        ])
    );
    assert!(!request.to_string().contains("google_search"));
}
async fn own_declared_round_trip(setter: bool) {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let schema = own_declared_schema();
    let input = context(schema.clone());
    let mut failure = None;
    if setter {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetTools(input.tools.clone())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        failure = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if processor
                    .request_snapshot()
                    .unwrap()
                    .context
                    .tools
                    .as_ref()
                    .is_some_and(|tools| tools.len() == 1 && tools[0].name == "echo")
                {
                    return None;
                }
                tokio::select! {
                    frame = worker.recv() => {
                        if let FrameData::Error(error) = frame.unwrap().frame.data {
                            return Some(error.error);
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_millis(5)) => {},
                }
            }
        })
        .await
        .unwrap();
        if failure.is_none() {
            worker
                .sender()
                .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
                .await
                .unwrap();
        }
    } else {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmContextDetailed(
                    core::DetailedLlmContextFrame {
                        context: input.clone(),
                        speculation: false,
                    },
                )),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    let mut calls = vec![];
    if failure.is_none() {
        failure = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(frame) = worker.recv().await {
                match frame.frame.data {
                    FrameData::FunctionCallsStarted(batch) => calls.extend(batch),
                    FrameData::LlmFullResponseEnd(_) | FrameData::LlmResponseEnd => return None,
                    FrameData::Error(error) => return Some(error.error),
                    _ => {}
                }
            }
            Some("pipeline stopped before declared response".into())
        })
        .await
        .unwrap();
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "own declarations must reach real OpenAI request intact; setter={setter}, failure={failure:?}, errors={:?}", report.errors);
    assert!(failure.is_none());
    assert!(report.errors.is_empty());
    assert_own_declared_wire(&requests[0], &schema);
    assert_eq!(
        calls,
        vec![core::FunctionCall {
            function_name: "echo".into(),
            tool_call_id: "declared-call".into(),
            arguments: json!({"value":"native"})
        }]
    );
    assert_eq!(input, context(schema));
}

#[tokio::test]
async fn own_declared_complete_context_preserves_the_exact_openai_request() {
    own_declared_round_trip(false).await;
}
#[tokio::test]
async fn own_declared_tool_setter_preserves_the_exact_openai_request() {
    own_declared_round_trip(true).await;
}
#[tokio::test]
async fn own_declared_inference_advertises_direct_without_executing_it() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let executed = Arc::new(AtomicUsize::new(0));
    let mut schema = own_declared_schema();
    schema.standard_tools = vec![core::StandardTool::Direct(direct(executed.clone()))];
    let input = context(schema.clone());
    let result = processor
        .run_inference(input.clone(), InferenceOptions::default())
        .await;
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "own declarations must reach real inference; result={result:?}"
    );
    assert_eq!(result.unwrap(), Some("declared".into()));
    assert_own_declared_wire(&requests[0], &schema);
    assert_eq!(
        executed.load(Ordering::SeqCst),
        0,
        "advertisement grants no local callback execution"
    );
    assert_eq!(input, context(schema));
    assert!(processor
        .request_snapshot()
        .unwrap()
        .context
        .tools
        .is_none());
}

#[test]
fn declared_request_api_keeps_legacy_bodies_and_enforces_own_bounds() {
    use pipecat_services::{DeclaredRequest, OpenAiToolDeclarations, ToolDefinition};
    let own = OpenAiToolDeclarations::new(vec![BTreeMap::from([
        ("type".into(), json!("tool_search")),
        ("marker".into(), json!("opaque-body-marker")),
    ])])
    .unwrap();
    assert!(!format!("{own:?}").contains("opaque-body-marker"));
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let mut config = ClientConfig::new(provider, "fixture");
        config.api_key = None;
        let client = LlmClient::new(config).unwrap();
        let mut request = CompletionRequest::new("fixture", Context::default());
        request.context.tools = Some(
            (0..129)
                .map(|i| ToolDefinition {
                    name: format!("fn-{i}"),
                    description: "fixture".into(),
                    parameters: json!({"type":"object"}),
                })
                .collect(),
        );
        assert_eq!(
            client
                .request_body_declared(&request.clone().into())
                .unwrap(),
            client.request_body(&request).unwrap(),
            "empty wrapper must not change legacy validation or tool count"
        );
        let declared = DeclaredRequest::new(request, own.clone());
        assert!(matches!(
            client.request_body_declared(&declared),
            Err(ServiceError::InvalidRequest(_)) | Err(ServiceError::Unsupported(_))
        ));
    }
    let valid = BTreeMap::from([("type".into(), json!("tool_search"))]);
    let mut deep = Value::Null;
    for _ in 0..=core::NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
        deep = json!([deep]);
    }
    for tools in [
        vec![BTreeMap::new()],
        vec![BTreeMap::from([("type".into(), json!(false))])],
        vec![BTreeMap::from([("type".into(), json!(" "))])],
        vec![valid.clone(); 129],
        vec![BTreeMap::from([
            ("type".into(), json!("tool_search")),
            ("nested".into(), deep),
        ])],
        vec![BTreeMap::from([
            ("type".into(), json!("tool_search")),
            ("large".into(), json!("x".repeat(64 * 1024))),
        ])],
    ] {
        assert!(matches!(
            OpenAiToolDeclarations::from_slice(&tools),
            Err(ServiceError::InvalidRequest(_))
        ));
        assert!(matches!(
            OpenAiToolDeclarations::new(tools),
            Err(ServiceError::InvalidRequest(_))
        ));
    }
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.api_key = None;
    let client = LlmClient::new(config).unwrap();
    let mut request = CompletionRequest::new("fixture", Context::default());
    request.extra.insert("tools".into(), json!([]));
    assert!(
        matches!(
            client.request_body_declared(&DeclaredRequest::new(request, own)),
            Err(ServiceError::InvalidRequest(_))
        ),
        "declared tools must not bypass reserved extra validation"
    );
}

#[tokio::test]
async fn declared_request_presets_and_precancelled_stream_do_not_contact_fixture() {
    use pipecat_services::{
        CancellationToken, DeclaredRequest, OpenAiToolDeclarations, StreamEvent,
    };
    let server = Server::start().await;
    let own = OpenAiToolDeclarations::new(vec![BTreeMap::from([(
        "type".into(),
        json!("tool_search"),
    )])])
    .unwrap();
    let request = CompletionRequest::new("fixture", Context::default());
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = server.url.clone();
    config.api_key = None;
    config.openai_preset = Some(pipecat_services::presets::OpenAiPresetConfig::new(
        pipecat_services::presets::OpenAiPreset::OpenRouter,
    ));
    let preset = LlmClient::new(config.clone()).unwrap();
    assert!(matches!(
        preset.stream_declared(DeclaredRequest::new(request.clone(), own.clone())),
        Err(ServiceError::Unsupported(_))
    ));
    config.openai_preset = None;
    let client = LlmClient::new(config).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut stream = client
        .stream_declared_with_cancel(DeclaredRequest::new(request, own), cancel)
        .unwrap();
    let mut cancelled = false;
    while let Some(event) = stream.next().await {
        match event {
            Err(ServiceError::Cancelled) => cancelled = true,
            Ok(StreamEvent::Started | StreamEvent::Finished { .. }) => {}
            other => panic!("unexpected pre-cancelled event: {other:?}"),
        }
    }
    assert!(cancelled);
    assert!(server.requests.lock().unwrap().is_empty());
}

async fn declared_setter_applied(
    worker: &core::PipelineWorker,
    processor: &LlmProcessor,
    schema: core::ToolsSchema,
) {
    let expected = schema
        .custom_tools
        .as_ref()
        .and_then(|map| map.get(&core::AdapterType::Openai))
        .cloned()
        .unwrap_or_default();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(core::ToolSet::Schema(schema))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if processor
                .declared_request_snapshot()
                .unwrap()
                .openai_tools()
                .as_slice()
                == expected
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("own declaration setter stalled");
}

#[tokio::test]
async fn declared_state_preserves_rejected_updates_and_clears_removed_own_entries() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let original = own_declared_schema();
    declared_setter_applied(&worker, &processor, original.clone()).await;
    let before = processor.declared_request_snapshot().unwrap();
    let mut delta = core::LlmSettings::default();
    delta.base.model = core::Setting::Value("updated-model".into());
    processor.update_settings(&delta).unwrap();
    assert_eq!(
        processor
            .declared_request_snapshot()
            .unwrap()
            .openai_tools()
            .as_slice(),
        before.openai_tools().as_slice()
    );
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmMessagesUpdateDetailed(
                core::DetailedMessagesFrame {
                    messages: vec![core::ChatMessage::new(
                        core::ChatRole::User,
                        "carry own declarations",
                    )
                    .into()],
                    run_llm: Some(false),
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    // Wait for this normal message update itself; system setters can preempt it.
    tokio::time::timeout(Duration::from_secs(3), async {
        while !processor
            .request_snapshot()
            .unwrap()
            .context
            .messages
            .first()
            .is_some_and(|message| {
                message.content
                    == Some(pipecat_services::Content::Text(
                        "carry own declarations".into(),
                    ))
            })
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        processor
            .declared_request_snapshot()
            .unwrap()
            .openai_tools()
            .as_slice(),
        before.openai_tools().as_slice()
    );
    assert_eq!(
        processor.request_snapshot().unwrap().context.messages[0].content,
        Some(pipecat_services::Content::Text(
            "carry own declarations".into()
        ))
    );
    // InvalidRequest retains the existing permanent health gate. Verify normal
    // message updates while healthy, then compare the whole rejected snapshot.
    let before = processor.declared_request_snapshot().unwrap();
    let mut invalid = original;
    invalid
        .custom_tools
        .as_mut()
        .unwrap()
        .get_mut(&core::AdapterType::Openai)
        .unwrap()[0]
        .insert("type".into(), json!(""));
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(core::ToolSet::Schema(invalid))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = frame.frame.data {
                assert!(error.error.contains("nonempty string type"));
                return;
            }
        }
        panic!("invalid declaration did not produce error");
    })
    .await
    .unwrap();
    let after = processor.declared_request_snapshot().unwrap();
    assert_eq!(
        serde_json::to_value(before.request()).unwrap(),
        serde_json::to_value(after.request()).unwrap()
    );
    assert_eq!(
        before.openai_tools().as_slice(),
        after.openai_tools().as_slice()
    );
    declared_setter_applied(&worker, &processor, schema()).await;
    assert!(processor
        .declared_request_snapshot()
        .unwrap()
        .openai_tools()
        .is_empty());
    assert!(server.requests.lock().unwrap().is_empty());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 1);
}
fn direct(executed: Arc<AtomicUsize>) -> core::DirectFunction {
    core::DirectFunction {
        schema: definition(),
        handler: core::LocalToolFunction::new(move |_| {
            let executed = executed.clone();
            async move {
                executed.fetch_add(1, Ordering::SeqCst);
                Ok(json!("unexpected"))
            }
        }),
    }
}

#[tokio::test]
async fn invalid_openai_declarations_and_gemini_owned_tools_reject_before_io() {
    let server = Server::start().await;
    let mut invalid = own_custom();
    invalid
        .custom_tools
        .as_mut()
        .unwrap()
        .get_mut(&core::AdapterType::Openai)
        .unwrap()[0]
        .insert("type".into(), json!(7));
    for (provider, schema) in [(Provider::OpenAi, invalid), (Provider::Gemini, schema())] {
        let processor = processor(&server, provider);
        let error = processor
            .run_inference(context(schema), InferenceOptions::default())
            .await
            .unwrap_err();
        assert!(match provider {
            Provider::OpenAi => matches!(error, ServiceError::InvalidRequest(_)),
            _ => matches!(error, ServiceError::Unsupported(_)),
        });
        assert!(processor
            .request_snapshot()
            .unwrap()
            .context
            .tools
            .is_none());
    }
    assert!(server.requests.lock().unwrap().is_empty());
    // Provider-agnostic callers include MCP and Direct owner admission; they
    // still reject custom declarations before publishing executable authority.
    assert!(matches!(
        frame_context::context(&context(schema())),
        Err(ServiceError::Unsupported(_))
    ));
}

#[tokio::test]
async fn invalid_openai_custom_setter_keeps_previous_tool_snapshot() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(core::ToolSet::Functions(vec![
                definition(),
            ]))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if processor
                .request_snapshot()
                .unwrap()
                .context
                .tools
                .is_some()
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before = processor.request_snapshot().unwrap();
    let mut invalid = own_custom();
    invalid
        .custom_tools
        .as_mut()
        .unwrap()
        .get_mut(&core::AdapterType::Openai)
        .unwrap()[0]
        .insert("type".into(), json!(""));
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(core::ToolSet::Schema(invalid))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = frame.frame.data {
                assert!(error.error.contains("nonempty string type"));
                return;
            }
        }
        panic!("invalid owning declaration must be rejected");
    })
    .await
    .unwrap();
    assert_eq!(
        processor.request_snapshot().unwrap().context,
        before.context
    );
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn openai_foreign_custom_projection_preserves_inference_only_direct_advertisement() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let executed = Arc::new(AtomicUsize::new(0));
    let mut schema = schema();
    schema.standard_tools = vec![core::StandardTool::Direct(direct(executed.clone()))];
    schema
        .custom_tools
        .as_mut()
        .unwrap()
        .insert(core::AdapterType::Openai, vec![]);
    let input = context(schema);
    let result = processor
        .run_inference(input, InferenceOptions::default())
        .await
        .unwrap();
    assert_eq!(result, Some("declared".into()));
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_wire(&requests[0]);
}

#[tokio::test]
async fn openai_foreign_custom_projection_still_validates_standard_functions_and_direct_ownership()
{
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let mut schema = schema();
    let core::StandardTool::Function(function) = &mut schema.standard_tools[0] else {
        unreachable!()
    };
    function.parameters = Value::Null;
    assert!(matches!(
        processor
            .run_inference(context(schema), InferenceOptions::default())
            .await,
        Err(ServiceError::InvalidRequest(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
    let executed = Arc::new(AtomicUsize::new(0));
    let mut schema = own_declared_schema();
    schema.standard_tools = vec![core::StandardTool::Direct(direct(executed.clone()))];
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContextDetailed(
                core::DetailedLlmContextFrame {
                    context: context(schema),
                    speculation: false,
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = frame.frame.data {
                assert!(error
                    .error
                    .contains("direct tool handlers must be registered with a tool executor"));
                return;
            }
        }
        panic!("ordinary Direct must retain its executable owner");
    })
    .await
    .unwrap();
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert!(server.requests.lock().unwrap().is_empty());
}
