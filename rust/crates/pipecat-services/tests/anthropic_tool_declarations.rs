use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, InferenceOptions, LlmClient, LlmInference,
    LlmProcessor, Message, Provider, Role, ServiceError,
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
    let events = [
        json!({"type":"message_start","message":{"id":"message-fixture","type":"message","role":"assistant","model":"fixture","content":[],"usage":{"input_tokens":1,"output_tokens":0}}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":""}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"text_delta","text":"declared"}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"tool_use","id":"declared-call","name":"echo","input":{}}}),
        json!({"type":"content_block_delta","index":1,"delta":{"type":"input_json_delta","partial_json":"{\"value\":\"native\"}"}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"tool_use"},"usage":{"output_tokens":2}}),
        json!({"type":"message_stop"}),
    ];
    let body: String = events
        .iter()
        .map(|event| {
            format!(
                "event: {}\ndata: {event}\n\n",
                event["type"].as_str().unwrap()
            )
        })
        .collect();
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
        custom_tools: Some(BTreeMap::from([
            (
                core::AdapterType::Gemini,
                vec![BTreeMap::from([("google_search".into(), json!({}))])],
            ),
            (
                core::AdapterType::Openai,
                vec![BTreeMap::from([("type".into(), json!("tool_search"))])],
            ),
        ])),
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
        CompletionRequest::new(
            "fixture",
            Context::new(vec![Message::text(Role::User, "declare echo")]),
        ),
    ))
}
fn assert_wire(request: &Value) {
    assert_eq!(
        request["tools"],
        json!([{"name":"echo","description":"Echo value","input_schema":definition().parameters}])
    );
    assert_eq!(request["tool_choice"], json!({"type":"tool","name":"echo"}));
    assert!(!request.to_string().contains("google_search"));
    assert!(!request.to_string().contains("tool_search"));
}
async fn pipeline_round_trip(setter: bool) {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Anthropic);
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
    assert_eq!(requests.len(), 1, "foreign adapter declarations must allow a real Anthropic request; failure={failure:?}, report={:?}", report.errors);
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
async fn anthropic_context_ignores_external_declarations_and_returns_native_tool_use() {
    pipeline_round_trip(false).await;
}
#[tokio::test]
async fn anthropic_setter_ignores_external_declarations_and_returns_native_tool_use() {
    pipeline_round_trip(true).await;
}
#[tokio::test]
async fn anthropic_inference_ignores_external_declarations_without_mutating_input() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Anthropic);
    let input = context(schema());
    let before = processor.request_snapshot().unwrap().context;
    let result = processor
        .run_inference(input.clone(), InferenceOptions::default())
        .await;
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "foreign declarations must allow real Anthropic inference; result={result:?}"
    );
    assert_eq!(result.unwrap(), Some("declared".into()));
    assert_wire(&requests[0]);
    assert_eq!(input, context(schema()));
    assert_eq!(processor.request_snapshot().unwrap().context, before);
}

#[tokio::test]
async fn anthropic_external_declarations_preserve_validation_and_direct_execution_boundaries() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Anthropic);
    let before = processor.request_snapshot().unwrap().context;
    let mut invalid = schema();
    let core::StandardTool::Function(function) = &mut invalid.standard_tools[0] else {
        unreachable!()
    };
    function.parameters = Value::Null;
    assert!(matches!(
        processor
            .run_inference(context(invalid), InferenceOptions::default())
            .await,
        Err(ServiceError::InvalidRequest(_))
    ));
    assert!(server.requests.lock().unwrap().is_empty());
    assert_eq!(processor.request_snapshot().unwrap().context, before);

    let executed = Arc::new(AtomicUsize::new(0));
    let callback_executed = executed.clone();
    let mut direct = schema();
    direct.standard_tools = vec![core::StandardTool::Direct(core::DirectFunction {
        schema: definition(),
        handler: core::LocalToolFunction::new(move |_| {
            let executed = callback_executed.clone();
            async move {
                executed.fetch_add(1, Ordering::SeqCst);
                Ok(json!("unexpected"))
            }
        }),
    })];
    assert_eq!(
        processor
            .run_inference(context(direct.clone()), InferenceOptions::default())
            .await
            .unwrap(),
        Some("declared".into())
    );
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_wire(&requests[0]);
    assert_eq!(processor.request_snapshot().unwrap().context, before);

    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContextDetailed(
                core::DetailedLlmContextFrame {
                    context: context(direct),
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
        panic!("ordinary Anthropic Direct requires execution ownership");
    })
    .await
    .unwrap();
    assert_eq!(processor.request_snapshot().unwrap().context, before);
    worker.end().await.unwrap();
    worker.collect().await.unwrap();
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    assert_eq!(
        server.requests.lock().unwrap().len(),
        1,
        "ordinary Direct must issue no additional request"
    );
}
