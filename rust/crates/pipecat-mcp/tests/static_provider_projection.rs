mod support;

use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor, LlmContextData,
    MessageContent, Pipeline, PipelineConfig, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, ServerConfig};
use pipecat_pipeline::DirectToolBinding;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::Arc,
    time::Duration,
};
use support::Server;

async fn round_trip(mode: &str, owning: bool) {
    provider_round_trip(mode, owning, Provider::OpenAi).await;
}
async fn provider_round_trip(mode: &str, owning: bool, native_provider: Provider) {
    provider_round_trip_with_search_key(mode, owning, native_provider, "google_search").await;
}

async fn provider_round_trip_with_search_key(
    mode: &str,
    owning: bool,
    native_provider: Provider,
    search_key: &str,
) {
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    config.tools_arguments.insert(
        "echo".into(),
        serde_json::Map::from_iter([("hidden".into(), json!("static-native"))]),
    );
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let mut schema = tools.schema();
    let (adapter, declaration) = if native_provider == Provider::Gemini {
        (
            pipecat_core::AdapterType::Gemini,
            BTreeMap::from([(search_key.into(), json!({}))]),
        )
    } else if owning {
        (
            pipecat_core::AdapterType::Openai,
            BTreeMap::from([
                ("type".into(), json!("tool_search")),
                ("options".into(), json!({"limit":2})),
            ]),
        )
    } else {
        (
            pipecat_core::AdapterType::Gemini,
            BTreeMap::from([("google_search".into(), json!({}))]),
        )
    };
    schema.custom_tools = Some(BTreeMap::from([(adapter, vec![declaration.clone()])]));
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke declared echo").into()],
            tools: ToolSet::Schema(schema.clone()),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let provider = Server::start(if native_provider == Provider::Gemini {
        "gemini-llm"
    } else {
        "llm"
    })
    .await;
    let mut config = ClientConfig::new(native_provider, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let native = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let stages = match mode {
        "direct" => DirectToolBinding::new(native).unwrap().stages(),
        "fixed" => tools.llm_stages(native).unwrap(),
        "explicit" => tools.bind_llm(native).unwrap().stages(),
        "auto" => tools.bind_llm_auto(native).unwrap().stages(),
        _ => unreachable!(),
    };
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    processors.extend(stages);
    processors.push(pair.assistant());
    let mut worker = Pipeline::new(processors)
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    let failure = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::Error(error) => return Some(error.error),
                FrameData::LlmContextDetailed(context)
                    if context.context.messages.last().is_some_and(|m| {
                        m.role == ChatRole::Assistant
                            && m.content == MessageContent::Text("tool completed".into())
                    }) =>
                {
                    return None
                }
                _ => {}
            }
        }
        Some("pipeline stopped before native continuation".into())
    })
    .await
    .unwrap();
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    let requests = provider.state.lock().unwrap().completions.clone();
    let calls = server.state.lock().unwrap().calls.clone();
    assert_eq!(requests.len(),2,"{mode}: static provider selection must preserve real MCP continuation; failure={failure:?}, MCP calls={}, errors={:?}",calls.len(),report.errors);
    assert_eq!(calls.len(), 1);
    assert!(failure.is_none());
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    if native_provider == Provider::Gemini {
        let response = requests[1]["contents"]
            .as_array()
            .unwrap()
            .iter()
            .flat_map(|m| m["parts"].as_array().unwrap())
            .find_map(|p| p.get("functionResponse"))
            .unwrap();
        assert_eq!(response["name"], "echo");
        assert_eq!(response["id"], "agent-call");
        assert_eq!(
            serde_json::from_str::<Value>(response["response"]["value"].as_str().unwrap()).unwrap(),
            json!({"value":"agent","hidden":"static-native"})
        );
        for request in &requests {
            assert_eq!(request["tools"][0], json!({"googleSearch":{}}));
            assert_eq!(
                request["tools"][1]["functionDeclarations"][0]["name"],
                "echo"
            );
            assert_eq!(request["tools"].as_array().unwrap().len(), 2);
            assert!(!request.to_string().contains("google_search"));
        }
    } else {
        let result = requests[1]["messages"]
            .as_array()
            .unwrap()
            .iter()
            .find(|m| m["role"] == "tool")
            .unwrap();
        assert_eq!(result["tool_call_id"], "agent-call");
        let text: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(text.as_str().unwrap()).unwrap(),
            json!({"value":"agent","hidden":"static-native"})
        );
        assert!(!requests[0].to_string().contains("google_search"));
        for request in &requests {
            assert_eq!(request["tools"][0]["function"]["name"], "echo");
            if owning {
                assert_eq!(request["tools"].as_array().unwrap().len(), 2);
                assert_eq!(
                    request["tools"][1],
                    serde_json::to_value(&declaration).unwrap()
                );
            } else {
                assert_eq!(request["tools"].as_array().unwrap().len(), 1);
            }
        }
    }
    assert_eq!(
        pair.context().snapshot_rich().unwrap().tools,
        ToolSet::Schema(schema)
    );
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|s| s.completed_deletes == 1).await;
    client.close().await.unwrap();
}

#[tokio::test]
async fn gemini_owning_static_auto_bridge_returns_real_mcp_result_and_cleanup() {
    provider_round_trip("auto", true, Provider::Gemini).await;
}

#[tokio::test]
async fn gemini_camel_search_static_auto_bridge_returns_real_mcp_result_and_cleanup() {
    provider_round_trip_with_search_key("auto", true, Provider::Gemini, "googleSearch").await;
}
#[tokio::test]
async fn gemini_owning_static_direct_bridge_returns_real_mcp_result_and_cleanup() {
    provider_round_trip("direct", true, Provider::Gemini).await;
}

#[tokio::test]
async fn generic_direct_static_provider_retains_mcp_callbacks_with_foreign_declarations() {
    round_trip("direct", false).await;
}
#[tokio::test]
async fn fixed_mcp_static_provider_retains_callbacks_with_foreign_declarations() {
    round_trip("fixed", false).await;
}
#[tokio::test]
async fn explicit_mcp_static_provider_retains_callbacks_with_foreign_declarations() {
    round_trip("explicit", false).await;
}
#[tokio::test]
async fn automatic_mcp_static_provider_retains_callbacks_with_foreign_declarations() {
    round_trip("auto", false).await;
}

#[tokio::test]
async fn owning_direct_static_bridge_keeps_real_mcp_continuation() {
    round_trip("direct", true).await;
}
#[tokio::test]
async fn owning_fixed_mcp_static_bridge_keeps_real_continuation() {
    round_trip("fixed", true).await;
}
#[tokio::test]
async fn owning_explicit_mcp_static_bridge_keeps_real_continuation() {
    round_trip("explicit", true).await;
}
#[tokio::test]
async fn owning_automatic_mcp_static_bridge_keeps_real_continuation() {
    round_trip("auto", true).await;
}

fn native(provider: &Server) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
fn with_custom(
    mut schema: pipecat_core::ToolsSchema,
    adapter: pipecat_core::AdapterType,
) -> ToolSet {
    schema.custom_tools = Some(BTreeMap::from([(
        adapter,
        vec![BTreeMap::from([("opaque".into(), json!({}))])],
    )]));
    ToolSet::Schema(schema)
}
async fn outcome(
    worker: &mut pipecat_core::PipelineWorker,
) -> Result<pipecat_core::FunctionCallResultFrame, String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::FunctionCallResult(result) => return Ok(result),
                FrameData::Error(error) => return Err(error.error),
                _ => {}
            }
        }
        panic!("pipeline ended before expected outcome");
    })
    .await
    .expect("static provider outcome stalled")
}
async fn set(
    worker: &pipecat_core::PipelineWorker,
    llm: &LlmProcessor,
    tools: ToolSet,
    description: &str,
) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(tools)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if llm
                .request_snapshot()
                .unwrap()
                .context
                .tools
                .as_ref()
                .is_some_and(|tools| tools.len() == 1 && tools[0].description == description)
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("static provider setter stalled");
}
async fn call(worker: &pipecat_core::PipelineWorker, id: &str) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![
                pipecat_core::FunctionCall {
                    function_name: "echo".into(),
                    tool_call_id: id.into(),
                    arguments: json!({"value":id}),
                },
            ])),
            Direction::Downstream,
        )
        .await
        .unwrap();
}
fn invalid_context(tools: ToolSet) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(
        pipecat_core::DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![ChatMessage::new(ChatRole::Tool, "missing result id").into()],
                tools,
                tool_choice: None,
            },
            speculation: false,
        },
    ))
}

#[tokio::test]
async fn refreshed_static_mcp_owner_keeps_provider_and_rejects_foreign_authority_atomically() {
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    let client = McpClient::new(config.clone()).unwrap();
    let first = client.tools().await.unwrap();
    let provider = Server::start("llm").await;
    let llm = native(&provider);
    let binding = first.bind_llm_auto(llm.clone()).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    set(
        &worker,
        &llm,
        with_custom(first.schema(), pipecat_core::AdapterType::Gemini),
        &first.definitions()[0].description,
    )
    .await;
    call(&worker, "before-refresh").await;
    assert!(outcome(&mut worker).await.unwrap().error.is_none());
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::NotGiven)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while llm.request_snapshot().unwrap().context.tools.is_some() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("pruning setter stalled");
    let fresh = client.tools().await.unwrap();
    binding.update(&fresh).unwrap();
    // Wait for None -> Some before sending the system-priority tool batch.
    // The refreshed definition has the same name but a new callback identity.
    set(
        &worker,
        &llm,
        with_custom(fresh.schema(), pipecat_core::AdapterType::Gemini),
        &fresh.definitions()[0].description,
    )
    .await;
    call(&worker, "after-refresh").await;
    assert!(outcome(&mut worker).await.unwrap().error.is_none());

    let foreign_client = McpClient::new(config).unwrap();
    let foreign = foreign_client.tools().await.unwrap();
    assert_eq!(foreign.definitions(), fresh.definitions());
    for (tools, expected) in [
        (
            with_custom(foreign.schema(), pipecat_core::AdapterType::Gemini),
            "bound snapshot",
        ),
        (
            with_custom(fresh.schema(), pipecat_core::AdapterType::Openai),
            "nonempty string type",
        ),
    ] {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetTools(tools)),
                Direction::Downstream,
            )
            .await
            .unwrap();
        assert!(outcome(&mut worker).await.unwrap_err().contains(expected));
    }
    worker
        .sender()
        .send(
            invalid_context(with_custom(
                pipecat_core::ToolsSchema {
                    standard_tools: vec![],
                    custom_tools: None,
                },
                pipecat_core::AdapterType::Gemini,
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(outcome(&mut worker)
        .await
        .unwrap_err()
        .contains("tool result requires tool_call_id"));
    call(&worker, "after-rejected-contexts").await;
    assert!(outcome(&mut worker).await.unwrap().error.is_none());
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    assert!(provider.state.lock().unwrap().completions.is_empty());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 3);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    client.close().await.unwrap();
    foreign_client.close().await.unwrap();
}

#[tokio::test]
async fn static_direct_rejected_context_cannot_publish_handler_or_cleanup() {
    use pipecat_core::{
        DirectFunction, LocalAsyncCallback, LocalToolFunction, StandardTool, ToolDefinition,
        ToolsSchema,
    };
    use std::sync::atomic::{AtomicUsize, Ordering};
    let old_calls = Arc::new(AtomicUsize::new(0));
    let old_cleanup = Arc::new(AtomicUsize::new(0));
    let rejected_calls = Arc::new(AtomicUsize::new(0));
    let rejected_cleanup = Arc::new(AtomicUsize::new(0));
    let function =
        |calls: Arc<AtomicUsize>, cleanup: Arc<AtomicUsize>, description: &str| DirectFunction {
            schema: ToolDefinition {
                name: "echo".into(),
                description: description.into(),
                parameters: json!({"type":"object"}),
            },
            handler: LocalToolFunction::new(move |_| {
                let calls = calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(json!("old callback"))
                }
            })
            .with_cleanup(LocalAsyncCallback::new(move || {
                let cleanup = cleanup.clone();
                async move {
                    cleanup.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })),
        };
    let old = function(old_calls.clone(), old_cleanup.clone(), "original");
    let rejected = function(rejected_calls.clone(), rejected_cleanup.clone(), "rejected");
    let schema = |function| ToolsSchema {
        standard_tools: vec![StandardTool::Direct(function)],
        custom_tools: None,
    };
    let provider = Server::start("llm").await;
    let llm = native(&provider);
    let binding = DirectToolBinding::with_config(
        llm.clone(),
        pipecat_pipeline::DirectToolBindingConfig {
            max_cleanup_hooks: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    set(
        &worker,
        &llm,
        with_custom(schema(old), pipecat_core::AdapterType::Gemini),
        "original",
    )
    .await;
    worker
        .sender()
        .send(
            invalid_context(with_custom(
                schema(rejected.clone()),
                pipecat_core::AdapterType::Gemini,
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(outcome(&mut worker)
        .await
        .unwrap_err()
        .contains("tool result requires tool_call_id"));
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(with_custom(
                schema(rejected),
                pipecat_core::AdapterType::Openai,
            ))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(outcome(&mut worker)
        .await
        .unwrap_err()
        .contains("nonempty string type"));
    call(&worker, "retained-old").await;
    let result = outcome(&mut worker).await.unwrap();
    assert_eq!(result.result, json!("old callback"));
    assert!(result.error.is_none());
    assert_eq!(old_calls.load(Ordering::SeqCst), 1);
    assert_eq!(rejected_calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        llm.request_snapshot().unwrap().context.tools.unwrap()[0].description,
        "original"
    );
    assert!(provider.state.lock().unwrap().completions.is_empty());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 2);
    assert_eq!(old_cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(rejected_cleanup.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn switcher_owners_keep_agnostic_custom_validation_without_io() {
    let server = Server::start("").await;
    let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
        HttpConfig::new(&server.url),
    )))
    .unwrap();
    let tools = client.tools().await.unwrap();
    let provider = Server::start("llm").await;
    for generic in [false, true] {
        let switcher =
            Arc::new(pipecat_pipeline::LlmSwitcher::new(vec![native(&provider)]).unwrap());
        let stages = if generic {
            DirectToolBinding::new_switcher(switcher).unwrap().stages()
        } else {
            tools.bind_llm_switcher_auto(switcher).unwrap().stages()
        };
        let mut worker = Pipeline::new(stages)
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetTools(with_custom(
                    tools.schema(),
                    pipecat_core::AdapterType::Gemini,
                ))),
                Direction::Downstream,
            )
            .await
            .unwrap();
        assert!(outcome(&mut worker)
            .await
            .unwrap_err()
            .contains("provider-specific custom"));
        assert!(provider.state.lock().unwrap().completions.is_empty());
        assert!(server.state.lock().unwrap().calls.is_empty());
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert_eq!(report.errors.len(), 1);
    }
    client.close().await.unwrap();
}

fn owning_tools(mut schema: pipecat_core::ToolsSchema) -> ToolSet {
    schema.custom_tools = Some(BTreeMap::from([(
        pipecat_core::AdapterType::Openai,
        vec![BTreeMap::from([
            ("type".into(), json!("tool_search")),
            ("name".into(), json!("opaque-only")),
        ])],
    )]));
    ToolSet::Schema(schema)
}

#[tokio::test]
async fn actual_client_preflight_rejects_preset_and_reserved_fields_before_admission() {
    for generic in [false, true] {
        for reserved in [false, true] {
            let server = Server::start("").await;
            let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
                HttpConfig::new(&server.url),
            )))
            .unwrap();
            let tools = client.tools().await.unwrap();
            let provider = Server::start("llm").await;
            let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
            config.endpoint = provider.url.clone();
            config.api_key = None;
            if !reserved {
                config.openai_preset = Some(pipecat_services::presets::OpenAiPresetConfig::new(
                    pipecat_services::presets::OpenAiPreset::OpenRouter,
                ));
            }
            let mut request = CompletionRequest::new("fixture", Context::default());
            if reserved {
                request.extra.insert("tools".into(), json!([]));
            }
            let llm = Arc::new(LlmProcessor::new(LlmClient::new(config).unwrap(), request));
            let before = llm.declared_request_snapshot().unwrap();
            let stages = if generic {
                DirectToolBinding::new(llm.clone()).unwrap().stages()
            } else {
                tools.bind_llm_auto(llm.clone()).unwrap().stages()
            };
            let mut worker = Pipeline::new(stages)
                .start(PipelineConfig::default())
                .await
                .unwrap();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::LlmSetTools(owning_tools(tools.schema()))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            let error = outcome(&mut worker).await.unwrap_err();
            assert!(
                error.contains(if reserved {
                    "extra field tools"
                } else {
                    "ordinary OpenAI Chat Completions"
                }),
                "{error}"
            );
            let after = llm.declared_request_snapshot().unwrap();
            assert_eq!(
                serde_json::to_value(before.request()).unwrap(),
                serde_json::to_value(after.request()).unwrap()
            );
            assert_eq!(
                before.openai_tools().as_slice(),
                after.openai_tools().as_slice()
            );
            call(&worker, "after-client-rejection").await;
            assert_eq!(
                outcome(&mut worker).await.unwrap().error.as_deref(),
                Some("tool is not registered")
            );
            assert!(server.state.lock().unwrap().calls.is_empty());
            assert!(provider.state.lock().unwrap().completions.is_empty());
            worker.end().await.unwrap();
            let (_, report) = worker.collect().await.unwrap();
            assert_eq!(report.errors.len(), 1);
            // A rejected generic schema cannot publish its attached MCP cleanup.
            assert_eq!(server.state.lock().unwrap().deletes, usize::from(!generic));
            client.close().await.unwrap();
        }
    }
}

#[tokio::test]
async fn owning_auto_preflight_retains_old_handler_and_grants_no_opaque_callback() {
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let provider = Server::start("llm").await;
    let llm = native(&provider);
    let binding = tools.bind_llm_auto(llm.clone()).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    set(
        &worker,
        &llm,
        owning_tools(tools.schema()),
        &tools.definitions()[0].description,
    )
    .await;
    call(&worker, "before-invalid-own-context").await;
    assert!(outcome(&mut worker).await.unwrap().error.is_none());
    worker
        .sender()
        .send(
            invalid_context(owning_tools(pipecat_core::ToolsSchema {
                standard_tools: vec![],
                custom_tools: None,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(outcome(&mut worker)
        .await
        .unwrap_err()
        .contains("tool result requires tool_call_id"));
    let mut too_many = tools.schema();
    too_many.standard_tools.extend((0..127).map(|i| {
        pipecat_core::StandardTool::Function(pipecat_core::ToolDefinition {
            name: format!("advertisement-{i}"),
            description: "no callback".into(),
            parameters: json!({"type":"object"}),
        })
    }));
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(owning_tools(too_many))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(outcome(&mut worker)
        .await
        .unwrap_err()
        .contains("standard tools and declarations exceed tool count limit"));
    call(&worker, "after-invalid-own-context").await;
    assert!(outcome(&mut worker).await.unwrap().error.is_none());
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(owning_tools(
                pipecat_core::ToolsSchema {
                    standard_tools: vec![],
                    custom_tools: None,
                },
            ))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = llm.declared_request_snapshot().unwrap();
            if snapshot
                .request()
                .context
                .tools
                .as_ref()
                .is_some_and(Vec::is_empty)
                && snapshot.openai_tools().len() == 1
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![
                pipecat_core::FunctionCall {
                    function_name: "opaque-only".into(),
                    tool_call_id: "opaque-is-advertisement".into(),
                    arguments: json!({}),
                },
            ])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert_eq!(
        outcome(&mut worker).await.unwrap().error.as_deref(),
        Some("tool is not registered")
    );
    call(&worker, "pruned-old-echo").await;
    assert_eq!(
        outcome(&mut worker).await.unwrap().error.as_deref(),
        Some("tool is not registered")
    );
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert!(provider.state.lock().unwrap().completions.is_empty());
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 2);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|s| s.completed_deletes == 1).await;
    client.close().await.unwrap();
}
