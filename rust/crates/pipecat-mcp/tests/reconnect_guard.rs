mod support;

use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor, LlmContextData,
    MessageContent, Pipeline, PipelineConfig, ToolCallEndReason, ToolCallTerminalState, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, McpError, ServerConfig};
use pipecat_pipeline::DirectToolBinding;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::Server;

#[tokio::test]
async fn retained_schema_reconnect_and_real_tool_result_fit_the_native_guard() {
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into(), "failure".into()]));
    config.limits.connect_timeout = Duration::from_secs(5);
    config.limits.request_timeout = Duration::from_secs(1);
    config.limits.close_timeout = Duration::from_millis(500);
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    // Invalidate this actual generation without replaying the rejected invocation.
    server.state.lock().unwrap().mode = "connection-closed-code".into();
    assert_eq!(
        client.call("failure", Value::Null).await.unwrap_err(),
        McpError::Remote(-32000)
    );
    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.initializes, 1);
        assert_eq!(state.calls.len(), 1);
        assert_eq!(state.calls[0]["name"], "failure");
        assert_eq!(state.deletes, 1);
        assert_eq!(state.completed_deletes, 1);
    }
    server.state.lock().unwrap().mode = "slow-second-initialize".into();

    let provider = Server::start("llm").await;
    let mut llm_config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    llm_config.endpoint = provider.url.clone();
    llm_config.api_key = None;
    let native_llm = Arc::new(LlmProcessor::new(
        LlmClient::new(llm_config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let binding = DirectToolBinding::new(native_llm).unwrap();
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke retained echo").into()],
            tools: ToolSet::Schema(schema.clone()),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
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
    let receipt = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let snapshot = binding.outcomes().snapshot();
            if !snapshot.terminal.is_empty() {
                assert!(snapshot.active.is_empty());
                assert_eq!(snapshot.total_terminal, 1);
                return snapshot.terminal[0].clone();
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("native reconnect invocation did not settle");
    {
        let state = server.state.lock().unwrap();
        eprintln!(
            "reconnect receipt={:?}; initializes={}; initialized={}; MCP calls={}; provider requests={}",
            receipt.outcome.reason(),
            state.initializes,
            state.initialized,
            state.calls.len(),
            provider.state.lock().unwrap().completions.len(),
        );
    }
    assert_eq!(
        receipt.outcome.reason(),
        ToolCallEndReason::CallbackReturned,
        "a separately admitted reconnect and tool request must retain their complete MCP budget"
    );
    assert_eq!(receipt.outcome.state(), ToolCallTerminalState::Completed);
    assert_eq!(receipt.outcome.call().function_name, "echo");
    assert_eq!(receipt.outcome.call().tool_call_id, "agent-call");
    tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("native reconnect round trip failed: {}", error.error);
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
        panic!("no committed assistant response after reconnect");
    })
    .await
    .expect("real MCP result did not resume inference");
    let requests = provider.state.lock().unwrap().completions.clone();
    assert_eq!(requests.len(), 2);
    let tool_result = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .unwrap();
    assert_eq!(tool_result["tool_call_id"], "agent-call");
    let text: Value = serde_json::from_str(tool_result["content"].as_str().unwrap()).unwrap();
    let actual: Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert_eq!(actual, json!({"value":"agent"}));
    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.initializes, 2);
        assert_eq!(state.initialized, 2);
        assert_eq!(state.calls.len(), 2);
        assert_eq!(state.calls[1]["name"], "echo");
        assert_eq!(state.deletes, 1);
    }
    assert_eq!(
        pair.context().snapshot_rich().unwrap().tools,
        ToolSet::Schema(schema.clone())
    );
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.wait_for(|state| state.completed_deletes == 2).await;
    {
        let state = server.state.lock().unwrap();
        assert_eq!(state.deletes, 2);
        assert_eq!(state.completed_deletes, 2);
        assert_eq!(
            state.calls.len(),
            2,
            "neither generation may replay its call"
        );
    }
    // Runtime success and teardown precede the precise metadata assertion.
    let echo = schema
        .standard_tools
        .iter()
        .find_map(|tool| match tool {
            pipecat_core::StandardTool::Direct(function) if function.schema.name == "echo" => {
                Some(&function.handler)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(echo.options().timeout, Some(Duration::from_millis(6500)));
    assert_eq!(tools.definitions().len(), 2);
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
}
