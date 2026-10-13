mod support;

use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor, FunctionCall,
    LlmContextData, MessageContent, NativeToolCallOutcome, Pipeline, PipelineConfig,
    PipelineWorker, ToolCallEffectStatus, ToolCallEndReason, ToolCallTerminalState, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, ServerConfig};
use pipecat_pipeline::{DirectToolBinding, DirectToolBindingConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::Server;

fn llm(provider: &Server) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
async fn setter_applied(llm: &LlmProcessor, expected: &[pipecat_core::ToolDefinition]) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if llm
                .request_snapshot()
                .unwrap()
                .context
                .tools
                .as_ref()
                .is_some_and(|actual| {
                    actual.len() == expected.len()
                        && actual.iter().zip(expected).all(|(actual, expected)| {
                            actual.name == expected.name
                                && actual.description == expected.description
                                && actual.parameters == expected.parameters
                        })
                })
            {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("schema setter did not reach native LLM");
}
async fn terminal(worker: &mut PipelineWorker) -> (FrameData, Arc<NativeToolCallOutcome>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            let native = frame.frame.native_tool_outcome().unwrap().cloned();
            match &frame.frame.data {
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_) => {
                    return (
                        frame.frame.data,
                        native.expect("owned native terminal outcome"),
                    );
                }
                FrameData::Error(error) => panic!("schema binding failed: {}", error.error),
                _ => {}
            }
        }
        panic!("pipeline stopped before the native tool outcome");
    })
    .await
    .expect("schema tool outcome stalled")
}
fn no_remaining_outcomes(frames: &[pipecat_core::DirectedFrame]) {
    assert!(
        !frames.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
                | FrameData::FunctionCallCancel(_)
        )),
        "all exact outcomes were consumed; duplicate identities cannot settle again"
    );
}
fn config(server: &Server, names: &[&str]) -> McpConfig {
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(
        names
            .iter()
            .map(|name| (*name).to_string())
            .collect::<BTreeSet<_>>(),
    );
    config
}
fn handler<'a>(
    schema: &'a pipecat_core::ToolsSchema,
    name: &str,
) -> &'a pipecat_core::LocalToolFunction {
    let tool = schema
        .standard_tools
        .iter()
        .find(|tool| {
            matches!(tool,
                pipecat_core::StandardTool::Direct(function) if function.schema.name == name
            )
        })
        .expect("discovered native handler");
    let pipecat_core::StandardTool::Direct(function) = tool else {
        unreachable!();
    };
    &function.handler
}

#[tokio::test]
async fn direct_schema_round_trip_refresh_prune_and_teardown_owns_one_mcp_cleanup() {
    let server = Server::start("").await;
    let client = McpClient::new(config(&server, &["echo", "other"])).unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke discovered echo").into()],
            tools: ToolSet::Schema(schema.clone()),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let provider = Server::start("llm").await;
    let native_llm = llm(&provider);
    let binding = DirectToolBinding::with_config(
        native_llm.clone(),
        DirectToolBindingConfig {
            max_cleanup_hooks: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    processors.extend(binding.stages());
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
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("native round trip failed: {}", error.error);
            }
            if let FrameData::LlmContextDetailed(context) = &frame.frame.data {
                if context.context.messages.last().is_some_and(|message| {
                    message.role == ChatRole::Assistant
                        && message.content == MessageContent::Text("tool completed".into())
                }) {
                    return;
                }
            }
        }
        panic!("no committed assistant response");
    })
    .await
    .expect("real MCP/schema/native LLM continuation stalled");
    let requests = provider.state.lock().unwrap().completions.clone();
    assert_eq!(requests.len(), 2);
    let result = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .unwrap();
    assert_eq!(result["tool_call_id"], "agent-call");
    let text: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
    let actual: Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert_eq!(actual, json!({"value":"agent"}));
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(
        pair.context().snapshot_rich().unwrap().tools,
        ToolSet::Schema(schema.clone())
    );

    server.state.lock().unwrap().mode = "live-other".into();
    let fresh = client.clone().tools().await.unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Schema(fresh.schema()))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    setter_applied(&native_llm, fresh.definitions()).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Direct(vec![]))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    setter_applied(&native_llm, &[]).await;
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(server.state.lock().unwrap().deletes, 0);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    // Retain client, both discoveries and cached handlers so Drop cannot hide missing cleanup.
    assert_eq!(tools.definitions().len(), 2);
    assert_eq!(fresh.definitions().len(), 1);
    assert_eq!(schema.standard_tools.len(), 2);
    assert_eq!(
        server.state.lock().unwrap().deletes,
        1,
        "generic schema owner must retain one MCP cleanup across refresh and pruning"
    );
    server.wait_for(|state| state.completed_deletes == 1).await;
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        pipecat_mcp::McpError::NotConnected
    );
    // Metadata assertions follow the real teardown observation so a missing hook cannot mask it.
    let echo_hook = handler(&schema, "echo")
        .cleanup_callback()
        .expect("owned MCP cleanup");
    assert_eq!(
        handler(&schema, "other").cleanup_callback().unwrap(),
        echo_hook
    );
    assert_eq!(
        handler(&tools.clone().schema(), "echo")
            .cleanup_callback()
            .unwrap(),
        echo_hook
    );
    assert_eq!(
        handler(&fresh.schema(), "other")
            .cleanup_callback()
            .unwrap(),
        echo_hook
    );
    let limits = config(&server, &["echo", "other"]).limits;
    let expected_deadline = limits
        .request_timeout
        .checked_add(limits.close_timeout)
        .unwrap();
    assert_eq!(
        handler(&schema, "echo").options().timeout,
        Some(expected_deadline)
    );
    assert!(handler(&schema, "echo").options().cancel_on_interruption);
    assert_eq!(
        handler(&schema, "other").options(),
        handler(&schema, "echo").options()
    );
    let foreign_client = McpClient::new(config(&server, &["echo", "other"])).unwrap();
    let foreign = foreign_client.tools().await.unwrap();
    assert_ne!(
        handler(&foreign.schema(), "other")
            .cleanup_callback()
            .unwrap(),
        echo_hook,
        "same endpoint must not merge independent client cleanup identities"
    );
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    foreign_client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 2);
    echo_hook.call().await.unwrap();
    assert_eq!(
        server.state.lock().unwrap().deletes,
        2,
        "the original shared hook stays idempotent"
    );
}

#[tokio::test]
async fn direct_schema_explicit_session_close_preserves_typed_native_cancellation() {
    let server = Server::start("blocked-slow").await;
    let mut config = config(&server, &["slow"]);
    config.limits.request_timeout = Duration::from_secs(30);
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let provider = Server::start("llm").await;
    let native_llm = llm(&provider);
    let binding = DirectToolBinding::new(native_llm.clone()).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Schema(tools.schema()))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    setter_applied(&native_llm, tools.definitions()).await;
    let call = FunctionCall {
        function_name: "slow".into(),
        tool_call_id: "schema-closed-call".into(),
        arguments: json!({}),
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    server.wait_for(|state| state.calls.len() == 1).await;
    client.close().await.unwrap();
    let (frame, outcome) = terminal(&mut worker).await;
    assert_eq!(
        outcome.state(),
        ToolCallTerminalState::Cancelled,
        "remote session cancellation must not turn into a schema callback error"
    );
    assert_eq!(outcome.reason(), ToolCallEndReason::ExplicitCancel);
    assert_eq!(
        outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert_eq!(outcome.call(), &call);
    assert!(!outcome.run_llm());
    let FrameData::FunctionCallCancel(cancelled) = frame else {
        panic!("typed cancellation receipt expected");
    };
    assert_eq!(cancelled.function_name, call.function_name);
    assert_eq!(cancelled.tool_call_id, call.tool_call_id);
    assert!(!cancelled.run_llm);
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    no_remaining_outcomes(&remaining);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    assert!(provider.state.lock().unwrap().completions.is_empty());
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn direct_schema_deadline_overrides_short_generic_default_and_retains_cleanup_after_pruning()
{
    let server = Server::start("").await;
    let mut config = config(&server, &["slow"]);
    config.limits.request_timeout = Duration::from_secs(1);
    config.limits.close_timeout = Duration::from_millis(500);
    let expected_deadline = config
        .limits
        .request_timeout
        .checked_add(config.limits.close_timeout)
        .unwrap();
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let provider = Server::start("llm").await;
    let native_llm = llm(&provider);
    let binding = DirectToolBinding::with_config(
        native_llm.clone(),
        DirectToolBindingConfig {
            max_cleanup_hooks: 1,
            default_timeout: Some(Duration::from_millis(10)),
            ..Default::default()
        },
    )
    .unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Schema(schema.clone()))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    setter_applied(&native_llm, tools.definitions()).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "slow".into(),
                tool_call_id: "schema-deadline".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    server.wait_for(|state| state.calls.len() == 1).await;
    let (frame, outcome) = terminal(&mut worker).await;
    assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned,
        "the actual 250ms MCP invocation must use its own deadline, overriding the 10ms generic default");
    assert_eq!(outcome.state(), ToolCallTerminalState::Completed);
    let FrameData::FunctionCallResult(result) = frame else {
        panic!("successful slow MCP result");
    };
    assert_eq!(result.tool_call_id, "schema-deadline");
    assert_eq!(result.result, json!("done"));
    assert!(result.error.is_none());
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Direct(vec![]))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    setter_applied(&native_llm, &[]).await;
    assert_eq!(server.state.lock().unwrap().deletes, 0);
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    no_remaining_outcomes(&remaining);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|state| state.completed_deletes == 1).await;
    assert!(provider.state.lock().unwrap().completions.is_empty());
    assert_eq!(
        handler(&schema, "slow").options().timeout,
        Some(expected_deadline)
    );
    assert!(handler(&schema, "slow").options().cancel_on_interruption);
}
