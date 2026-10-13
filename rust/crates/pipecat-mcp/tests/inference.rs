mod support;

use pipecat_core::{ChatMessage, ChatRole, LlmContextData, StandardTool, ToolSet};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, ServerConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, InferenceOptions, LlmClient, LlmInference,
    LlmProcessor, Provider,
};
use serde_json::{json, Value};
use std::{collections::BTreeSet, time::Duration};
use support::Server;

#[tokio::test]
async fn out_of_band_inference_advertises_mcp_schema_without_executing_tools() {
    let server = Server::start("").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    config.tools_arguments.insert(
        "echo".into(),
        json!({"hidden":"fixed-inference-secret"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let provider = Server::start("llm").await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let processor = LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    );
    let before = serde_json::to_value(processor.request_snapshot().unwrap()).unwrap();
    for as_direct in [false, true] {
        let schema = tools.schema();
        let advertised = if as_direct {
            ToolSet::Direct(
                schema
                    .standard_tools
                    .into_iter()
                    .map(|tool| match tool {
                        StandardTool::Direct(tool) => tool,
                        _ => panic!("MCP schema must carry its handler"),
                    })
                    .collect(),
            )
        } else {
            ToolSet::Schema(schema)
        };
        let context = LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "infer without side effects").into()],
            tools: advertised,
            tool_choice: Some(pipecat_core::ToolChoice::Required),
        };
        // This peer returns a real tool call. Out-of-band inference advertises
        // schemas but must leave execution and result-driven reruns to a pipeline.
        assert_eq!(
            tokio::time::timeout(
                Duration::from_secs(3),
                processor.run_inference(context, InferenceOptions::default()),
            )
            .await
            .unwrap()
            .unwrap(),
            None,
        );
    }
    let completions = provider.state.lock().unwrap().completions.clone();
    assert_eq!(completions.len(), 2, "no tool-driven inference rerun");
    for request in completions {
        let advertised = &request["tools"][0]["function"];
        assert_eq!(advertised["name"], "echo");
        assert_eq!(advertised["parameters"], tools.definitions()[0].parameters);
        assert_eq!(request["tool_choice"], "required");
        assert!(!request.to_string().contains("fixed-inference-secret"));
    }
    assert!(server.state.lock().unwrap().calls.is_empty());
    assert_eq!(
        serde_json::to_value(processor.request_snapshot().unwrap()).unwrap(),
        before,
    );
    tools.close().await.unwrap();
    server.wait_for(|state| state.deletes == 1).await;
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        pipecat_mcp::McpError::NotConnected,
    );
}
