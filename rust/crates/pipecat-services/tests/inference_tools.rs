use pipecat_core::{self as core, ChatMessage, ChatRole, LlmContextData, StandardTool, ToolSet};
use pipecat_services::{
    frame_context, ClientConfig, CompletionRequest, Context, InferenceOptions, LlmClient,
    LlmInference, LlmProcessor, Provider, ServiceError,
};
use serde_json::json;

fn direct() -> core::DirectFunction {
    core::DirectFunction {
        schema: core::ToolDefinition {
            name: "lookup".into(),
            description: "Lookup an explicit value".into(),
            parameters: json!({"type":"object"}),
        },
        handler: core::LocalToolFunction::new(|_| async {
            panic!("out-of-band inference must never execute a handler")
        }),
    }
}
fn context(tools: ToolSet) -> LlmContextData {
    LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, "infer").into()],
        tools,
        tool_choice: None,
    }
}
fn processor() -> LlmProcessor {
    let mut config = ClientConfig::new(Provider::OpenAi, "unused-native-fixture");
    config.endpoint = "http://127.0.0.1:1/unused".into();
    config.api_key = None;
    LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    )
}

#[test]
fn pipeline_context_still_rejects_handler_loss() {
    for tools in [
        ToolSet::Direct(vec![direct()]),
        ToolSet::Schema(core::ToolsSchema {
            standard_tools: vec![StandardTool::Direct(direct())],
            custom_tools: None,
        }),
    ] {
        assert!(matches!(
            frame_context::context(&context(tools)),
            Err(ServiceError::Unsupported(message))
                if message == "direct tool handlers must be registered with a tool executor"
        ));
    }
}

#[tokio::test]
async fn inference_schema_projection_preserves_message_validation() {
    let mut context = context(ToolSet::Direct(vec![direct()]));
    context.messages[0].role = ChatRole::Tool;
    assert!(matches!(
        processor()
            .run_inference(context, InferenceOptions::default())
            .await,
        Err(ServiceError::InvalidRequest(message))
            if message == "tool result requires tool_call_id"
    ));
}

#[tokio::test]
async fn inference_still_rejects_custom_and_grouped_tool_schemas() {
    for tools in [
        ToolSet::Schema(core::ToolsSchema {
            standard_tools: vec![StandardTool::Direct(direct())],
            custom_tools: Some(
                [(
                    core::AdapterType::Openai,
                    vec![[("type".into(), json!("web_search"))].into_iter().collect()],
                )]
                .into_iter()
                .collect(),
            ),
        }),
        ToolSet::Grouped(Default::default()),
    ] {
        assert!(matches!(
            processor()
                .run_inference(context(tools), InferenceOptions::default())
                .await,
            Err(ServiceError::Unsupported(_))
        ));
    }
}
