mod support;

use pipecat_core::{
    ChatMessage, ChatRole, DetailedLlmContextFrame, Direction, Frame, FrameData, FunctionCall,
    FunctionCallResultFrame, LlmContextData, NativeToolCallOutcome, Pipeline, PipelineConfig,
    PipelineWorker, ToolCallEffectStatus, ToolCallEndReason, ToolCallTerminalState, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, McpTools, ServerConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use serde_json::json;
use std::{sync::Arc, time::Duration};
use support::Server;

fn context(tools: &McpTools) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke advertised tool").into()],
            tools: ToolSet::Schema(tools.schema()),
            tool_choice: None,
        },
        speculation: false,
    }))
}
async fn result(
    worker: &mut PipelineWorker,
) -> Result<(FunctionCallResultFrame, Arc<NativeToolCallOutcome>), String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            let native = frame.frame.native_tool_outcome().unwrap().cloned();
            match frame.frame.data {
                FrameData::FunctionCallResult(result) => {
                    return Ok((result, native.expect("owned native outcome")))
                }
                FrameData::FunctionCallCancel(cancelled) => {
                    return Err(format!(
                        "unexpected cancellation: {}",
                        cancelled.tool_call_id
                    ))
                }
                FrameData::Error(error) => return Err(error.error),
                _ => {}
            }
        }
        Err("pipeline stopped before tool outcome".into())
    })
    .await
    .expect("native tool outcome stalled")
}

#[tokio::test]
async fn running_native_pipeline_accepts_same_client_tool_refresh() {
    let server = Server::start("live-echo").await;
    let client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
        HttpConfig::new(&server.url),
    )))
    .unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.definitions().len(), 1);
    assert_eq!(tools.definitions()[0].name, "echo");
    let provider = Server::start("llm-live-tools").await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let binding = tools.bind_llm(llm).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(context(&tools), Direction::Downstream)
        .await
        .unwrap();
    let (first, _) = result(&mut worker).await.unwrap();
    assert_eq!(first.function_name, "echo");
    assert_eq!(first.tool_call_id, "live-call-1");
    assert!(first.error.is_none());
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    server.state.lock().unwrap().mode = "live-other".into();
    let fresh = client.tools().await.unwrap();
    assert_eq!(fresh.definitions().len(), 1);
    assert_eq!(fresh.definitions()[0].name, "other");
    binding.update(&fresh).unwrap();
    worker
        .sender()
        .send(context(&fresh), Direction::Downstream)
        .await
        .unwrap();
    let second = result(&mut worker).await;
    assert_eq!(
        provider.state.lock().unwrap().completions.len(),
        2,
        "refreshed context must reach native provider: {second:?}"
    );
    assert!(
        second.is_ok(),
        "fresh same-client context must run: {second:?}"
    );
    let (second, _) = second.unwrap();
    assert_eq!(second.function_name, "other");
    assert_eq!(second.tool_call_id, "live-call-2");
    assert!(second.error.is_none());
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(provider.state.lock().unwrap().completions.len(), 2);
    let completions = provider.state.lock().unwrap().completions.clone();
    assert_eq!(completions[0]["tools"][0]["function"]["name"], "echo");
    assert_eq!(completions[1]["tools"][0]["function"]["name"], "other");
    assert_eq!(
        completions[1]["tools"][0]["function"]["description"],
        "refreshed other"
    );
    assert_eq!(
        completions[1]["tools"][0]["function"]["parameters"],
        fresh.definitions()[0].parameters
    );
    // The old executable identity is no longer eligible for provider inference.
    worker
        .sender()
        .send(context(&tools), Direction::Downstream)
        .await
        .unwrap();
    assert!(result(&mut worker)
        .await
        .unwrap_err()
        .contains("bound snapshot"));
    assert_eq!(provider.state.lock().unwrap().completions.len(), 2);
    // A duplicate from the old generation cannot execute under the new callback map.
    for call in [
        FunctionCall {
            function_name: "other".into(),
            tool_call_id: first.tool_call_id,
            arguments: json!({}),
        },
        FunctionCall {
            function_name: "echo".into(),
            tool_call_id: "retired-future".into(),
            arguments: json!({}),
        },
    ] {
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallsStarted(vec![call])),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    let (retired, retired_outcome) = result(&mut worker).await.unwrap();
    assert_eq!(retired.tool_call_id, "retired-future");
    assert_eq!(retired.function_name, "echo");
    assert_eq!(retired.error.as_deref(), Some("tool is not registered"));
    assert_eq!(retired_outcome.state(), ToolCallTerminalState::Failed);
    assert_eq!(retired_outcome.reason(), ToolCallEndReason::NotRegistered);
    assert_eq!(retired_outcome.effects(), ToolCallEffectStatus::NotStarted);
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    let foreign_server = Server::start("live-other").await;
    let foreign_client = McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
        HttpConfig::new(&foreign_server.url),
    )))
    .unwrap();
    let foreign = foreign_client.tools().await.unwrap();
    assert_eq!(foreign.definitions(), fresh.definitions());
    assert!(binding
        .update(&foreign)
        .unwrap_err()
        .to_string()
        .contains("another client"));
    // Rejected publication leaves the original resource and metadata selected.
    worker
        .sender()
        .send(context(&fresh), Direction::Downstream)
        .await
        .unwrap();
    let (retained, _) = result(&mut worker).await.unwrap();
    assert_eq!(retained.tool_call_id, "live-call-3");
    assert_eq!(retained.function_name, "other");
    assert!(retained.error.is_none());
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    assert!(foreign_server.state.lock().unwrap().calls.is_empty());
    assert_eq!(provider.state.lock().unwrap().completions.len(), 3);
    foreign_client.close().await.unwrap();
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert!(
        !remaining.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
                | FrameData::FunctionCallCancel(_)
        )),
        "all four expected outcomes were consumed; a duplicate must not settle again"
    );
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0].error.contains("bound snapshot"));
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|state| state.completed_deletes == 1).await;
    let reopened = client.tools().await.unwrap();
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    assert!(binding
        .update(&reopened)
        .unwrap_err()
        .to_string()
        .contains("closed"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 2);
}
