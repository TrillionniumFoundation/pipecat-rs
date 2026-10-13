use pipecat_core::{self as core, Direction, Frame, FrameData, Pipeline, PipelineConfig};
use pipecat_services::{
    frame_context, ClientConfig, CompletionRequest, Context, InferenceOptions, LlmClient,
    LlmInference, LlmProcessor, Message, Provider, Role, ServiceError,
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
    let packet = json!({"candidates":[{"index":0,"content":{"role":"model","parts":[{"text":"declared"},{"functionCall":{"id":"declared-call","name":"echo","args":{"value":"native"}}}]},"finishReason":"STOP"}],"usageMetadata":{"promptTokenCount":1,"candidatesTokenCount":2,"totalTokenCount":3}});
    let body = format!("data: {packet}\n\n");
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
            core::AdapterType::Openai,
            vec![BTreeMap::from([("type".into(), json!("tool_search"))])],
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
        CompletionRequest::new(
            "fixture",
            Context::new(vec![Message::text(Role::User, "declare echo")]),
        ),
    ))
}
fn assert_wire(request: &Value, owning: bool) {
    let mut tools = json!([{"functionDeclarations":[{"name":"echo","description":"Echo value","parameters":{"type":"OBJECT","properties":{"value":{"type":"STRING"}},"required":["value"]}}]}]);
    if owning {
        // The locked SDK normalizes tools by moving the merged function group
        // after the built-in declarations, then maps google_search to googleSearch.
        tools
            .as_array_mut()
            .unwrap()
            .insert(0, json!({"googleSearch":{}}));
    }
    assert_eq!(request["tools"], tools);
    assert_eq!(
        request["toolConfig"],
        json!({"functionCallingConfig":{"mode":"ANY","allowedFunctionNames":["echo"]}})
    );
    assert!(!request.to_string().contains("tool_search"));
    assert!(!request.to_string().contains("google_search"));
    assert!(request.get("tool_choice").is_none());
}

#[tokio::test]
async fn gemini_function_schema_types_match_locked_sdk_native_serialization() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let mut schema = owning_schema();
    let parameters = json!({
        "type":"object", "description":"object array string",
        "default":{"type":"object","marker":"string"},
        "properties":{
            "type":{"type":"string","enum":["object","array","null"]},
            "names":{"type":"array","items":{"type":["string","null"]}},
            "nested":{"type":"object","properties":{
                "flag":{"type":"boolean"},"number":{"type":"number"},"integer":{"type":"integer"}
            }}
        },"required":["type"]
    });
    let core::StandardTool::Function(function) = &mut schema.standard_tools[0] else {
        unreachable!()
    };
    function.parameters = parameters.clone();
    // The public adapter mirrors Pipecat's SDK input; native serialization is
    // a separate step performed by the locked SDK's Schema/Type model.
    let adapted = pipecat_services::adapters::adapt_gemini_tool_schema(&parameters).0;
    assert_eq!(adapted["type"], "object");
    assert_eq!(
        adapted["properties"]["names"]["items"]["anyOf"][0]["type"],
        "string"
    );
    let input = context(schema);
    assert_eq!(
        processor
            .run_inference(input.clone(), InferenceOptions::default())
            .await
            .unwrap(),
        Some("declared".into())
    );
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["tools"][0], json!({"googleSearch":{}}));
    assert_eq!(
        requests[0]["tools"][1]["functionDeclarations"][0]["parameters"],
        json!({
            "type":"OBJECT", "description":"object array string",
            "default":{"type":"object","marker":"string"},
            "properties":{
                "type":{"type":"STRING","enum":["object","array","null"]},
            "names":{"type":"ARRAY","items":{"any_of":[{"type":"STRING"},{"type":"NULL"}]}},
                "nested":{"type":"OBJECT","properties":{
                    "flag":{"type":"BOOLEAN"},"number":{"type":"NUMBER"},"integer":{"type":"INTEGER"}
                }}
            },"required":["type"]
        }),
        "SDK Schema Type values must be canonical only at native schema nodes"
    );
    let core::ToolSet::Schema(retained) = input.tools else {
        unreachable!()
    };
    let core::StandardTool::Function(retained) = &retained.standard_tools[0] else {
        unreachable!()
    };
    assert_eq!(retained.parameters, parameters);
    assert!(processor
        .request_snapshot()
        .unwrap()
        .context
        .tools
        .is_none());
}

#[test]
fn gemini_schema_native_type_normalization_preserves_source_and_other_providers() {
    let parameters = json!({"type":"object","properties":{
        "case":{"type":"sTrInG","description":"string","enum":["object","STRING"],"default":{"type":"object"}},
        "unspecified":{"type":"type_unspecified"},
        "already":{"type":"OBJECT"},
        "unknown":{"type":"vendor-type"},
        "literal":{"type":"object","default":{
            "properties":{"kind":{"type":"string"}},"items":{"type":"array"},"anyOf":[{"type":"number"}]
        }}
    }});
    let mut request = CompletionRequest::new("fixture", Context::default());
    request.context.tools = Some(vec![pipecat_services::ToolDefinition {
        name: "echo".into(),
        description: "fixture".into(),
        parameters: parameters.clone(),
    }]);
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let body = pipecat_services::adapters::request_body(provider, &request).unwrap();
        let native = match provider {
            Provider::OpenAi => &body["tools"][0]["function"]["parameters"],
            Provider::Anthropic => &body["tools"][0]["input_schema"],
            Provider::Gemini => &body["tools"][0]["functionDeclarations"][0]["parameters"],
        };
        if provider == Provider::Gemini {
            assert_eq!(native["type"], "OBJECT");
            assert_eq!(native["properties"]["case"]["type"], "STRING");
            assert_eq!(
                native["properties"]["unspecified"]["type"],
                "TYPE_UNSPECIFIED"
            );
            assert_eq!(native["properties"]["already"]["type"], "OBJECT");
            assert_eq!(native["properties"]["unknown"]["type"], "vendor-type");
            assert_eq!(
                native["properties"]["case"]["enum"],
                parameters["properties"]["case"]["enum"]
            );
            assert_eq!(native["properties"]["case"]["description"], "string");
            assert_eq!(
                native["properties"]["case"]["default"],
                parameters["properties"]["case"]["default"]
            );
            assert_eq!(
                native["properties"]["literal"]["default"],
                parameters["properties"]["literal"]["default"]
            );
        } else {
            assert_eq!(native, &parameters);
        }
    }
    assert_eq!(request.context.tools.unwrap()[0].parameters, parameters);
    assert_eq!(
        pipecat_services::adapters::adapt_gemini_tool_schema(&parameters),
        (parameters, vec![])
    );
    let (adapted, notes) = pipecat_services::adapters::adapt_gemini_tool_schema(&json!({
        "type":["string","null"],"enum":["a",1],"additionalProperties":false,"x-note":true
    }));
    assert_eq!(
        adapted,
        json!({"anyOf":[{"type":"string"},{"type":"null"}]})
    );
    assert_eq!(
        notes,
        vec![
            "converted a union 'type' to 'anyOf'",
            "dropped a non-string 'enum', losing its constraint",
            "dropped unsupported key 'additionalProperties'",
            "dropped unsupported key 'x-note'"
        ]
    );
}

#[tokio::test]
async fn gemini_schema_aliases_match_locked_sdk_native_serialization() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let mut schema = owning_schema();
    let parameters = json!({"type":"object","properties":{
        "ordered":{"type":"object","properties":{"b":{"type":"string"},"a":{"type":"number"}},"propertyOrdering":["b","a"]},
        "choice":{"anyOf":[{"type":"string"},{"type":"null"}]},
        "snake":{"any_of":[{"type":"boolean"},{"type":"null"}]},
        "anyOf":{"type":"string"},"propertyOrdering":{"type":"string"},
        "literal":{"type":"object","default":{"anyOf":[{"type":"object"}],"propertyOrdering":["raw"]}}
    }});
    let core::StandardTool::Function(function) = &mut schema.standard_tools[0] else {
        unreachable!()
    };
    function.parameters = parameters.clone();
    assert_eq!(
        pipecat_services::adapters::adapt_gemini_tool_schema(&parameters),
        (parameters.clone(), vec![])
    );
    let input = context(schema);
    assert_eq!(
        processor
            .run_inference(input.clone(), InferenceOptions::default())
            .await
            .unwrap(),
        Some("declared".into())
    );
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["tools"][0], json!({"googleSearch":{}}));
    assert_eq!(requests[0]["tools"][1]["functionDeclarations"][0]["parameters"],json!({"type":"OBJECT","properties":{
        "ordered":{"type":"OBJECT","properties":{"b":{"type":"STRING"},"a":{"type":"NUMBER"}},"property_ordering":["b","a"]},
        "choice":{"any_of":[{"type":"STRING"},{"type":"NULL"}]},
        "snake":{"any_of":[{"type":"BOOLEAN"},{"type":"NULL"}]},
        "anyOf":{"type":"STRING"},"propertyOrdering":{"type":"STRING"},
        "literal":{"type":"OBJECT","default":{"anyOf":[{"type":"object"}],"propertyOrdering":["raw"]}}
    }}),"typed Schema fields use exact SDK native spelling; property names and default data remain literal");
    let core::ToolSet::Schema(retained) = input.tools else {
        unreachable!()
    };
    let core::StandardTool::Function(retained) = &retained.standard_tools[0] else {
        unreachable!()
    };
    assert_eq!(retained.parameters, parameters);
    assert!(processor
        .request_snapshot()
        .unwrap()
        .context
        .tools
        .is_none());
}

#[tokio::test]
async fn gemini_schema_alias_conflicts_reject_before_io_without_changing_state() {
    schema_alias_conflicts("properties").await;
}

#[tokio::test]
async fn gemini_defs_alias_conflicts_reject_before_io_without_changing_state() {
    schema_alias_conflicts("defs").await;
}

async fn schema_alias_conflicts(node_map: &str) {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let before = processor.gemini_declared_request_snapshot().unwrap();
    for (camel, snake, first, second) in [
        (
            "anyOf",
            "any_of",
            json!([{"type":"string"}]),
            json!([{"type":"number"}]),
        ),
        (
            "propertyOrdering",
            "property_ordering",
            json!(["b", "a"]),
            json!(["a", "b"]),
        ),
    ] {
        for equal in [false, true] {
            let mut invalid = owning_schema();
            let core::StandardTool::Function(function) = &mut invalid.standard_tools[0] else {
                unreachable!()
            };
            function.parameters = json!({"type":"object"});
            function.parameters.as_object_mut().unwrap().insert(node_map.into(), json!({"nested":{"type":"object","properties":{"a":{"type":"string"},"b":{"type":"number"}}}}));
            let node = function.parameters[node_map]["nested"]
                .as_object_mut()
                .unwrap();
            node.insert(camel.into(), first.clone());
            node.insert(
                snake.into(),
                if equal { first.clone() } else { second.clone() },
            );
            let input = context(invalid);
            let error = processor
                .run_inference(input.clone(), InferenceOptions::default())
                .await
                .unwrap_err();
            assert!(
                matches!(error,ServiceError::InvalidRequest(ref message) if message.contains("conflicting") && message.contains("aliases")),
                "mapped alias collisions must reject before HTTP: {error:?}"
            );
            assert!(matches!(
                processor.preflight_context(&input),
                Err(ServiceError::InvalidRequest(_))
            ));
            assert!(matches!(
                processor.preflight_tools(&input.tools),
                Err(ServiceError::InvalidRequest(_))
            ));
            let after = processor.gemini_declared_request_snapshot().unwrap();
            assert_eq!(
                serde_json::to_value(before.request()).unwrap(),
                serde_json::to_value(after.request()).unwrap()
            );
            assert_eq!(
                before.gemini_tools().as_slice(),
                after.gemini_tools().as_slice()
            );
        }
    }
    assert!(server.requests.lock().unwrap().is_empty());
}

#[tokio::test]
async fn gemini_defs_schema_nodes_match_locked_sdk_native_http() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let parameters = json!({
        "type":"object",
        "defs":{
            "Pet":{"type":"object","properties":{
                "name":{"type":"string"},
                "tag":{"anyOf":[{"type":"string"},{"type":"null"}]}
            },"propertyOrdering":["tag","name"]},
            "type":{"type":"array","items":{"type":"boolean"}}
        },
        "properties":{"pet":{"ref":"#/defs/Pet"},"flags":{"ref":"#/defs/type"}},
        "default":{"defs":{"Pet":{"type":"object","anyOf":[{"type":"string"}]}}}
    });
    let (adapted, notes) = pipecat_services::adapters::adapt_gemini_tool_schema(&parameters);
    assert_eq!(adapted, parameters);
    assert!(notes.is_empty());
    let mut schema = owning_schema();
    let core::StandardTool::Function(function) = &mut schema.standard_tools[0] else {
        unreachable!()
    };
    function.parameters = parameters.clone();
    let input = context(schema);
    let before = processor.gemini_declared_request_snapshot().unwrap();
    assert_eq!(
        processor
            .run_inference(input.clone(), InferenceOptions::default())
            .await
            .unwrap(),
        Some("declared".into())
    );
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0]["tools"][1]["functionDeclarations"][0]["parameters"], json!({
        "type":"OBJECT",
        "defs":{
            "Pet":{"type":"OBJECT","properties":{
                "name":{"type":"STRING"},
                "tag":{"any_of":[{"type":"STRING"},{"type":"NULL"}]}
            },"property_ordering":["tag","name"]},
            "type":{"type":"ARRAY","items":{"type":"BOOLEAN"}}
        },
        "properties":{"pet":{"ref":"#/defs/Pet"},"flags":{"ref":"#/defs/type"}},
        "default":{"defs":{"Pet":{"type":"object","anyOf":[{"type":"string"}]}}}
    }), "SDK typed defs values must receive Schema conversion; definition names, refs and default data stay literal");
    let core::ToolSet::Schema(retained) = input.tools else {
        unreachable!()
    };
    let core::StandardTool::Function(retained) = &retained.standard_tools[0] else {
        unreachable!()
    };
    assert_eq!(retained.parameters, parameters);
    let after = processor.gemini_declared_request_snapshot().unwrap();
    assert_eq!(
        serde_json::to_value(before.request()).unwrap(),
        serde_json::to_value(after.request()).unwrap()
    );
    assert_eq!(
        before.gemini_tools().as_slice(),
        after.gemini_tools().as_slice()
    );
}
fn owning_schema() -> core::ToolsSchema {
    owning_schema_with_key("google_search")
}
fn owning_schema_with_key(key: &str) -> core::ToolsSchema {
    let mut schema = schema();
    schema.custom_tools.as_mut().unwrap().insert(
        core::AdapterType::Gemini,
        vec![BTreeMap::from([(key.into(), json!({}))])],
    );
    schema
}
async fn pipeline_round_trip(setter: bool, owning: bool) {
    pipeline_round_trip_with_key(setter, owning, "google_search").await;
}
async fn pipeline_round_trip_with_key(setter: bool, owning: bool, key: &str) {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let schema = if owning {
        owning_schema_with_key(key)
    } else {
        schema()
    };
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
    assert_eq!(requests.len(), 1, "Gemini declaration selection must allow a real request; owning={owning}, failure={failure:?}, report={:?}", report.errors);
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
    assert_wire(&requests[0], owning);
    assert_eq!(input.tools, core::ToolSet::Schema(schema));
}

#[tokio::test]
async fn gemini_context_ignores_openai_tools_and_returns_a_real_native_function_call() {
    pipeline_round_trip(false, false).await;
}
#[tokio::test]
async fn gemini_setter_ignores_openai_tools_and_returns_a_real_native_function_call() {
    pipeline_round_trip(true, false).await;
}
#[tokio::test]
async fn gemini_owning_context_matches_locked_sdk_wire_order() {
    pipeline_round_trip(false, true).await;
}
#[tokio::test]
async fn gemini_owning_setter_matches_locked_sdk_wire_order() {
    pipeline_round_trip(true, true).await;
}
#[tokio::test]
async fn gemini_camel_search_context_reaches_native_http_with_the_original_dictionary() {
    pipeline_round_trip_with_key(false, true, "googleSearch").await;
}
#[tokio::test]
async fn gemini_camel_search_setter_reaches_native_http_with_the_original_dictionary() {
    pipeline_round_trip_with_key(true, true, "googleSearch").await;
}
#[tokio::test]
async fn gemini_camel_search_inference_reaches_native_http_without_mutation() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let input = context(owning_schema_with_key("googleSearch"));
    let before = processor.request_snapshot().unwrap().context;
    let result = processor
        .run_inference(input.clone(), InferenceOptions::default())
        .await;
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "valid SDK camel Search alias must reach HTTP; result={result:?}"
    );
    assert_eq!(result.unwrap(), Some("declared".into()));
    assert_wire(&requests[0], true);
    assert_eq!(input, context(owning_schema_with_key("googleSearch")));
    assert_eq!(processor.request_snapshot().unwrap().context, before);
}
#[tokio::test]
async fn gemini_owning_inference_keeps_sdk_declaration_order_without_mutating_input() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let input = context(owning_schema());
    let before = processor.request_snapshot().unwrap().context;
    let result = processor
        .run_inference(input.clone(), InferenceOptions::default())
        .await;
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "owning Gemini declarations must reach native HTTP; result={result:?}"
    );
    assert_eq!(result.unwrap(), Some("declared".into()));
    assert_wire(&requests[0], true);
    assert_eq!(input, context(owning_schema()));
    assert_eq!(processor.request_snapshot().unwrap().context, before);
}

#[test]
fn gemini_declaration_wrapper_keeps_legacy_bodies_and_exact_supported_shape() {
    use pipecat_services::{GeminiDeclaredRequest, GeminiToolDeclarations, ToolDefinition};
    let search = BTreeMap::from([("google_search".into(), json!({}))]);
    let own = GeminiToolDeclarations::from_slice(std::slice::from_ref(&search)).unwrap();
    assert_eq!(own.as_slice(), std::slice::from_ref(&search));
    for provider in [Provider::OpenAi, Provider::Anthropic, Provider::Gemini] {
        let mut config = ClientConfig::new(provider, "fixture");
        config.api_key = None;
        let client = LlmClient::new(config).unwrap();
        let mut request = CompletionRequest::new("fixture", Context::default());
        request.context.tools = Some(
            (0..129)
                .map(|i| ToolDefinition {
                    name: format!("echo-{i}"),
                    description: "fixture".into(),
                    parameters: json!({"type":"object"}),
                })
                .collect(),
        );
        assert_eq!(
            client
                .request_body_gemini_declared(&request.clone().into())
                .unwrap(),
            client.request_body(&request).unwrap(),
            "empty wrapper retains existing provider contracts and tool limits"
        );
        assert!(matches!(
            client.request_body_gemini_declared(&GeminiDeclaredRequest::new(request, own.clone())),
            Err(ServiceError::InvalidRequest(_)) | Err(ServiceError::Unsupported(_))
        ));
    }
    assert!(matches!(
        GeminiToolDeclarations::from_slice(&vec![search; 129]),
        Err(ServiceError::InvalidRequest(_))
    ));
    for unsupported in [
        BTreeMap::new(),
        BTreeMap::from([("GoogleSearch".into(), json!({}))]),
        BTreeMap::from([("code_execution".into(), json!({}))]),
        BTreeMap::from([("google_search".into(), Value::Null)]),
        BTreeMap::from([("googleSearch".into(), Value::Null)]),
        BTreeMap::from([(
            "googleSearch".into(),
            json!({"excludeDomains":["example.com"]}),
        )]),
        BTreeMap::from([(
            "google_search".into(),
            json!({"exclude_domains":["example.com"]}),
        )]),
        BTreeMap::from([
            ("google_search".into(), json!({})),
            ("marker".into(), json!("do not retain")),
        ]),
        BTreeMap::from([
            ("google_search".into(), json!({})),
            ("googleSearch".into(), json!({})),
        ]),
    ] {
        assert!(matches!(
            GeminiToolDeclarations::new(vec![unsupported]),
            Err(ServiceError::Unsupported(_))
        ));
    }
    let mut config = ClientConfig::new(Provider::Gemini, "fixture");
    config.api_key = None;
    let client = LlmClient::new(config).unwrap();
    let request = CompletionRequest::new("fixture", Context::default());
    for key in ["google_search", "googleSearch"] {
        let source = BTreeMap::from([(key.into(), json!({}))]);
        let copied = GeminiToolDeclarations::from_slice(std::slice::from_ref(&source)).unwrap();
        let owned = GeminiToolDeclarations::new(vec![source.clone()]).unwrap();
        assert_eq!(copied.as_slice(), owned.as_slice());
        assert_eq!(copied.as_slice(), std::slice::from_ref(&source));
        assert!(matches!(
            GeminiToolDeclarations::from_slice(&vec![source.clone(); 129]),
            Err(ServiceError::InvalidRequest(_))
        ));
        let declared = GeminiDeclaredRequest::new(request.clone(), copied);
        assert_eq!(
            client.request_body_gemini_declared(&declared).unwrap()["tools"],
            json!([{"googleSearch":{}}])
        );
        let (base, retained) = declared.into_parts();
        assert_eq!(
            serde_json::to_value(base).unwrap(),
            serde_json::to_value(&request).unwrap()
        );
        assert_eq!(retained.as_slice(), std::slice::from_ref(&source));
    }
    let declared = GeminiDeclaredRequest::new(request.clone(), own.clone());
    assert_eq!(
        client.request_body_gemini_declared(&declared).unwrap()["tools"],
        json!([{"googleSearch":{}}])
    );
    let (base, retained) = declared.into_parts();
    assert_eq!(
        serde_json::to_value(base).unwrap(),
        serde_json::to_value(&request).unwrap()
    );
    assert_eq!(retained.as_slice(), own.as_slice());
    let mut reserved = request;
    reserved.extra.insert("tools".into(), json!([]));
    assert!(matches!(
        client.request_body_gemini_declared(&GeminiDeclaredRequest::new(reserved, own)),
        Err(ServiceError::InvalidRequest(_))
    ));
}

#[tokio::test]
async fn gemini_declaration_stream_precancel_and_foreign_provider_make_no_request() {
    use pipecat_services::{
        CancellationToken, GeminiDeclaredRequest, GeminiToolDeclarations, StreamEvent,
    };
    let server = Server::start().await;
    let own =
        GeminiToolDeclarations::new(vec![BTreeMap::from([("google_search".into(), json!({}))])])
            .unwrap();
    let request = CompletionRequest::new("fixture", Context::default());
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = server.url.clone();
    config.api_key = None;
    let foreign = LlmClient::new(config.clone()).unwrap();
    assert!(matches!(
        foreign.stream_gemini_declared(GeminiDeclaredRequest::new(request.clone(), own.clone())),
        Err(ServiceError::Unsupported(_))
    ));
    config.provider = Provider::Gemini;
    let native = LlmClient::new(config).unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    let mut stream = native
        .stream_gemini_declared_with_cancel(GeminiDeclaredRequest::new(request, own), cancel)
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

#[tokio::test]
async fn gemini_owning_inference_advertises_direct_without_callback_execution() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let executed = Arc::new(AtomicUsize::new(0));
    let counter = executed.clone();
    let mut schema = owning_schema();
    schema.standard_tools = vec![core::StandardTool::Direct(core::DirectFunction {
        schema: definition(),
        handler: core::LocalToolFunction::new(move |_| {
            let counter = counter.clone();
            async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(json!("unexpected"))
            }
        }),
    })];
    let input = context(schema);
    assert_eq!(
        processor
            .run_inference(input.clone(), InferenceOptions::default())
            .await
            .unwrap(),
        Some("declared".into())
    );
    assert_eq!(executed.load(Ordering::SeqCst), 0);
    let requests = server.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_wire(&requests[0], true);
    assert!(processor
        .gemini_declared_request_snapshot()
        .unwrap()
        .gemini_tools()
        .is_empty());
    assert!(
        matches!(
            processor.preflight_context(&input),
            Err(ServiceError::Unsupported(_))
        ),
        "ordinary contexts still require an execution owner for Direct entries"
    );
}

async fn gemini_setter_applied(
    worker: &core::PipelineWorker,
    processor: &LlmProcessor,
    schema: core::ToolsSchema,
) {
    let expected = schema
        .custom_tools
        .as_ref()
        .and_then(|m| m.get(&core::AdapterType::Gemini))
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
                .gemini_declared_request_snapshot()
                .unwrap()
                .gemini_tools()
                .as_slice()
                == expected
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("Gemini declaration setter stalled");
}

#[tokio::test]
async fn gemini_owning_state_retains_metadata_and_rejects_before_publication() {
    owning_state_retains_metadata("google_search").await;
}

#[tokio::test]
async fn gemini_camel_search_state_retains_the_original_key_and_rejects_before_publication() {
    owning_state_retains_metadata("googleSearch").await;
}

async fn owning_state_retains_metadata(key: &str) {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    gemini_setter_applied(&worker, &processor, owning_schema_with_key(key)).await;
    assert!(
        matches!(
            processor.declared_request_snapshot(),
            Err(ServiceError::Unsupported(_))
        ),
        "OpenAI-specific snapshots must not silently drop Gemini metadata"
    );
    let mut delta = core::LlmSettings::default();
    delta.base.model = core::Setting::Value("updated-model".into());
    processor.update_settings(&delta).unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmMessagesUpdateDetailed(
                core::DetailedMessagesFrame {
                    messages: vec![
                        core::ChatMessage::new(core::ChatRole::User, "keep search").into()
                    ],
                    run_llm: Some(false),
                },
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if processor.request_snapshot().unwrap().context.messages[0].content
                == Some(pipecat_services::Content::Text("keep search".into()))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let before = processor.gemini_declared_request_snapshot().unwrap();
    assert_eq!(before.gemini_tools().len(), 1);
    assert_eq!(
        before.gemini_tools().as_slice(),
        &[BTreeMap::from([(key.into(), json!({}))])]
    );
    processor
        .preflight_context(&context(owning_schema_with_key(key)))
        .unwrap();
    processor
        .preflight_tools(&core::ToolSet::Schema(owning_schema_with_key(key)))
        .unwrap();
    let preflight_after = processor.gemini_declared_request_snapshot().unwrap();
    assert_eq!(
        serde_json::to_value(before.request()).unwrap(),
        serde_json::to_value(preflight_after.request()).unwrap()
    );
    assert_eq!(
        before.gemini_tools().as_slice(),
        preflight_after.gemini_tools().as_slice()
    );
    assert!(server.requests.lock().unwrap().is_empty());
    let mut invalid = owning_schema_with_key(key);
    invalid.standard_tools = (0..128)
        .map(|i| {
            let mut tool = definition();
            tool.name = format!("echo-{i}");
            core::StandardTool::Function(tool)
        })
        .collect();
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
                assert!(error.error.contains("tool count limit"));
                return;
            }
        }
        panic!("invalid combined count produced no error");
    })
    .await
    .unwrap();
    let after = processor.gemini_declared_request_snapshot().unwrap();
    assert_eq!(
        serde_json::to_value(before.request()).unwrap(),
        serde_json::to_value(after.request()).unwrap()
    );
    assert_eq!(
        before.gemini_tools().as_slice(),
        after.gemini_tools().as_slice()
    );
    gemini_setter_applied(&worker, &processor, schema()).await;
    assert!(processor
        .gemini_declared_request_snapshot()
        .unwrap()
        .gemini_tools()
        .is_empty());
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

#[tokio::test]
async fn gemini_snapshot_rejects_nonempty_openai_metadata_without_losing_it() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::OpenAi);
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(core::ToolSet::Schema(schema()))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while processor
            .declared_request_snapshot()
            .unwrap()
            .openai_tools()
            .is_empty()
        {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(matches!(
        processor.gemini_declared_request_snapshot(),
        Err(ServiceError::Unsupported(_))
    ));
    assert_eq!(
        processor
            .declared_request_snapshot()
            .unwrap()
            .openai_tools()
            .as_slice(),
        schema().custom_tools.unwrap()[&core::AdapterType::Openai]
    );
    assert!(server.requests.lock().unwrap().is_empty());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
}
#[tokio::test]
async fn gemini_out_of_band_inference_ignores_openai_tools_without_mutating_input() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
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
    assert_wire(&requests[0], false);
    assert_eq!(input, context(schema()));
    assert!(processor
        .request_snapshot()
        .unwrap()
        .context
        .tools
        .is_none());
}

#[tokio::test]
async fn gemini_selection_keeps_owned_opaque_validation_and_direct_execution_boundaries() {
    let server = Server::start().await;
    let processor = processor(&server, Provider::Gemini);
    let before = processor.request_snapshot().unwrap().context;
    let mut owned = schema();
    owned.custom_tools.as_mut().unwrap().insert(
        core::AdapterType::Gemini,
        vec![BTreeMap::from([("code_execution".into(), json!({}))])],
    );
    assert!(matches!(
        processor
            .run_inference(context(owned), InferenceOptions::default())
            .await,
        Err(ServiceError::Unsupported(_))
    ));
    assert!(matches!(
        frame_context::context(&context(schema())),
        Err(ServiceError::Unsupported(_))
    ));
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
    direct
        .custom_tools
        .as_mut()
        .unwrap()
        .insert(core::AdapterType::Gemini, vec![]);
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
    assert_wire(&requests[0], false);
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
        panic!("ordinary Gemini Direct requires execution ownership");
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
        "ordinary Direct must issue no extra request"
    );
}
