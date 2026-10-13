mod support;

use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor, LlmContextData,
    MessageContent, Pipeline, PipelineConfig, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, McpTools, ServerConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use support::Server;

fn bound_stages(tools: &McpTools, llm: Arc<LlmProcessor>) -> Vec<Arc<dyn FrameProcessor>> {
    tools.llm_stages(llm).unwrap()
}

#[tokio::test]
async fn ordinary_bound_schema_runs_mcp_and_one_upstream_native_llm_continuation() {
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    config.tools_arguments.insert(
        "echo".into(),
        json!({"hidden":"bound-pipeline-fixed-value"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke echo once").into()],
            tools: ToolSet::Schema(schema.clone()),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let provider = Server::start("llm").await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    processors.extend(bound_stages(&tools, llm));
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
    let observed = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = &frame.frame.data {
                return Err(error.error.clone());
            }
            if let FrameData::LlmContextDetailed(context) = &frame.frame.data {
                if context.context.messages.last().is_some_and(|message| {
                    message.role == ChatRole::Assistant
                        && message.content == MessageContent::Text("tool completed".into())
                }) {
                    return Ok(());
                }
            }
        }
        Err("pipeline stopped before the committed final assistant response".into())
    })
    .await
    .expect("ordinary MCP/native LLM round trip stalled");
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    let completions = provider.state.lock().unwrap().completions.clone();
    assert_eq!(
        completions.len(),
        2,
        "executable schema must reach native inference and its upstream continuation: {observed:?}"
    );
    observed.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    let call = server.state.lock().unwrap().calls[0].clone();
    assert_eq!(call["name"], "echo");
    assert_eq!(
        call["arguments"],
        json!({"value":"agent","hidden":"bound-pipeline-fixed-value"})
    );
    for completion in &completions {
        let definition = &completion["tools"][0]["function"];
        assert_eq!(definition["name"], tools.definitions()[0].name);
        assert_eq!(
            definition["description"],
            tools.definitions()[0].description
        );
        assert_eq!(definition["parameters"], tools.definitions()[0].parameters);
        assert!(!completion["tools"]
            .to_string()
            .contains("bound-pipeline-fixed-value"));
    }
    let history = completions[1]["messages"].as_array().unwrap();
    let result = history
        .iter()
        .find(|message| message["role"] == "tool")
        .unwrap();
    assert_eq!(result["tool_call_id"], "agent-call");
    // The native assistant serializes the callback's Value::String as JSON.
    let text: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
    let value: Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert_eq!(value["hidden"], "bound-pipeline-fixed-value");
    let snapshot = pair.context().snapshot_rich().unwrap();
    assert_eq!(snapshot.tools, ToolSet::Schema(schema));
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|state| state.completed_deletes == 1).await;
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        pipecat_mcp::McpError::NotConnected,
    );
}

#[tokio::test]
async fn snapshot_schema_handlers_keep_identity_until_a_new_discovery() {
    let server = Server::start("").await;
    let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
        HttpConfig::new(&server.url),
    )))
    .unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.schema(), tools.schema());
    assert_eq!(tools.schema(), tools.clone().schema());
    assert_ne!(tools.schema(), client.tools().await.unwrap().schema());
    assert!(server.state.lock().unwrap().calls.is_empty());
    client.close().await.unwrap();
    server.wait_for(|state| state.deletes == 1).await;
}

#[tokio::test]
async fn bound_factory_projects_tool_updates_in_both_directions_without_inference() {
    use pipecat_core::StandardTool;
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let direct = tools
        .schema()
        .standard_tools
        .into_iter()
        .map(|tool| match tool {
            StandardTool::Direct(function) => function,
            _ => unreachable!(),
        })
        .collect::<Vec<_>>();
    let provider = Server::start("llm").await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let worker = Pipeline::new(bound_stages(&tools, llm.clone()))
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for direction in [Direction::Downstream, Direction::Upstream] {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetTools(ToolSet::Direct(direct.clone()))),
                direction,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while llm
                .request_snapshot()
                .unwrap()
                .context
                .tools
                .as_ref()
                .map(Vec::len)
                != Some(1)
            {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("bound tool update reached the native service");
        assert_eq!(
            llm.request_snapshot().unwrap().context.tools.unwrap()[0].parameters,
            tools.definitions()[0].parameters,
        );
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmSetTools(ToolSet::Direct(vec![]))),
                direction,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            while llm.request_snapshot().unwrap().context.tools != Some(vec![]) {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("explicit empty tool update reached the native service");
    }
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(provider.state.lock().unwrap().completions.is_empty());
    assert!(server.state.lock().unwrap().calls.is_empty());
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn bound_factory_rejects_foreign_or_modified_handlers_and_sealed_contexts() {
    use pipecat_core::{
        DetailedLlmContextFrame, LocalToolFunction, NativeRealtimeContextProducer,
        RealtimeContextIntent, SpeculationOwner, StandardTool,
    };
    let server = Server::start("").await;
    let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
        HttpConfig::new(&server.url),
    )))
    .unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let StandardTool::Direct(function) = schema.standard_tools[0].clone() else {
        unreachable!()
    };
    let provider = Server::start("llm").await;
    let owner = SpeculationOwner::new();
    let foreign_invocations = Arc::new(AtomicUsize::new(0));
    for scenario in [
        "foreign",
        "modified",
        "duplicate",
        "count",
        "sealed",
        "capability",
        "speculation",
        "custom",
        "grouped",
    ] {
        let mut functions = vec![function.clone()];
        match scenario {
            "foreign" => {
                let observed = foreign_invocations.clone();
                functions[0].handler = LocalToolFunction::new(move |_| {
                    observed.fetch_add(1, Ordering::SeqCst);
                    async { Ok(json!("foreign result")) }
                })
            }
            "modified" => {
                functions[0].schema.parameters = json!({"type":"object","title":"altered"})
            }
            "duplicate" => functions.push(function.clone()),
            "count" => {
                functions = vec![function.clone(); pipecat_mcp::Limits::default().max_tools + 1]
            }
            _ => {}
        }
        let context_tools = match scenario {
            "custom" => ToolSet::Schema(pipecat_core::ToolsSchema {
                standard_tools: functions.into_iter().map(StandardTool::Direct).collect(),
                custom_tools: Some(
                    [(
                        pipecat_core::AdapterType::Openai,
                        vec![[("type".into(), json!(""))].into_iter().collect()],
                    )]
                    .into_iter()
                    .collect(),
                ),
            }),
            "grouped" => ToolSet::Grouped(Default::default()),
            _ => ToolSet::Direct(functions),
        };
        let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![ChatMessage::new(ChatRole::User, "rejected context").into()],
                tools: context_tools,
                tool_choice: None,
            },
            speculation: scenario == "speculation",
        }));
        if scenario == "sealed" {
            NativeRealtimeContextProducer::default()
                .attach(&mut frame, RealtimeContextIntent::Run, true)
                .unwrap();
        }
        if scenario == "capability" {
            frame.set_native_speculation(owner.capability()).unwrap();
        }
        let retained = frame.clone();
        let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
        config.endpoint = provider.url.clone();
        config.api_key = None;
        let llm = Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("fixture", Context::default()),
        ));
        let mut worker = Pipeline::new(bound_stages(&tools, llm))
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        let error = tokio::time::timeout(Duration::from_secs(3), async {
            while let Some(frame) = worker.recv().await {
                if let FrameData::Error(error) = frame.frame.data {
                    return error.error;
                }
            }
            panic!("rejected executable context must produce an explicit error");
        })
        .await
        .unwrap();
        let expected = match scenario {
            "foreign" | "modified" => "executable MCP tool does not match its bound snapshot",
            "duplicate" => "duplicate bound MCP tool name",
            "count" => "bound MCP tool count exceeded",
            "sealed" | "capability" | "speculation" => {
                "bound MCP executable schema cannot project sealed or speculative context"
            }
            "custom" => "invalid request: OpenAI declarations require a nonempty string type",
            "grouped" => "unsupported provider feature: grouped provider-specific tool schemas",
            _ => unreachable!(),
        };
        assert_eq!(error, expected, "{scenario}");
        worker.end().await.unwrap();
        worker.collect().await.unwrap();
        if scenario == "sealed" {
            assert!(retained.native_realtime_context_cause().unwrap().is_some());
        }
        if scenario == "capability" {
            assert_eq!(retained.native_speculation(), Some(&owner.capability()));
        }
        assert!(provider.state.lock().unwrap().completions.is_empty());
        assert!(server.state.lock().unwrap().calls.is_empty());
        assert_eq!(foreign_invocations.load(Ordering::SeqCst), 0);
    }
}
