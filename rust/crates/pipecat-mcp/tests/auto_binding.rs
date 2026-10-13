mod support;

use pipecat_core::{
    ChatMessage, ChatRole, DetailedLlmContextFrame, Direction, Frame, FrameData, FunctionCall,
    FunctionCallResultFrame, LlmContextData, NativeToolCallOutcome, Pipeline, PipelineConfig,
    PipelineWorker, StandardTool, ToolCallEffectStatus, ToolCallEndReason, ToolCallTerminalState,
    ToolSet, ToolsSchema,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, McpTools, ServerConfig};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use serde_json::json;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::Server;

fn context(tools: ToolSet) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke other once").into()],
            tools,
            tool_choice: None,
        },
        speculation: false,
    }))
}
fn subset(tools: &McpTools, name: &str) -> ToolSet {
    let mut schema = tools.schema();
    schema.standard_tools.retain(|tool| match tool {
        StandardTool::Direct(function) => function.schema.name == name,
        StandardTool::Function(definition) => definition.name == name,
    });
    ToolSet::Schema(schema)
}
async fn result(
    worker: &mut PipelineWorker,
) -> Result<(FunctionCallResultFrame, Arc<NativeToolCallOutcome>), String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            let native = frame.frame.native_tool_outcome().unwrap().cloned();
            match frame.frame.data {
                FrameData::FunctionCallResult(result) => {
                    return Ok((result, native.expect("owned native outcome")));
                }
                FrameData::Error(error) => return Err(error.error),
                FrameData::FunctionCallCancel(cancelled) => {
                    return Err(format!(
                        "unexpected tool cancellation: {}",
                        cancelled.tool_call_id
                    ));
                }
                _ => {}
            }
        }
        Err("pipeline stopped before the native outcome".into())
    })
    .await
    .expect("native tool outcome stalled")
}
async fn send_call(worker: &PipelineWorker, name: &str, id: &str) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: name.into(),
                tool_call_id: id.into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
}
async fn setter_applied(llm: &LlmProcessor, expected: &[pipecat_core::ToolDefinition]) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let request = llm.request_snapshot().unwrap();
            if request.context.tools.as_ref().is_some_and(|selected| {
                selected.len() == expected.len()
                    && selected.iter().zip(expected).all(|(selected, expected)| {
                        selected.name == expected.name
                            && selected.description == expected.description
                            && selected.parameters == expected.parameters
                    })
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("tool setter did not update the native request");
}
fn assert_not_registered(result: &FunctionCallResultFrame, outcome: &NativeToolCallOutcome) {
    assert_eq!(outcome.reason(), ToolCallEndReason::NotRegistered);
    assert_eq!(outcome.state(), ToolCallTerminalState::Failed);
    assert_eq!(outcome.effects(), ToolCallEffectStatus::NotStarted);
    assert_eq!(result.error.as_deref(), Some("tool is not registered"));
}
fn assert_completed(result: &FunctionCallResultFrame, outcome: &NativeToolCallOutcome) {
    assert!(result.error.is_none());
    assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
}
fn llm(provider: &Server) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
async fn discover(server: &Server) -> (McpClient, McpTools) {
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into(), "other".into()]));
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.definitions().len(), 2);
    (client, tools)
}

#[tokio::test]
async fn advertised_context_subset_retires_a_stale_provider_call() {
    let server = Server::start("").await;
    let (client, tools) = discover(&server).await;
    let provider = Server::start("llm-force-other").await;
    let binding = tools.bind_llm_auto(llm(&provider)).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    // Discovery authorizes origin/name, but auto mode starts with no selected handlers.
    send_call(&worker, "other", "before-advertising").await;
    let (before, before_outcome) = result(&mut worker).await.unwrap();
    assert_not_registered(&before, &before_outcome);
    assert!(server.state.lock().unwrap().calls.is_empty());
    assert!(provider.state.lock().unwrap().completions.is_empty());
    worker
        .sender()
        .send(
            context(ToolSet::Schema(tools.schema())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (initial, initial_outcome) = result(&mut worker).await.unwrap();
    assert_eq!(initial.function_name, "other");
    assert_completed(&initial, &initial_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);

    worker
        .sender()
        .send(context(subset(&tools, "echo")), Direction::Downstream)
        .await
        .unwrap();
    let (retired, retired_outcome) = result(&mut worker).await.unwrap();
    assert_eq!(retired.function_name, "other");
    assert_eq!(provider.state.lock().unwrap().completions.len(), 2);
    let request = provider.state.lock().unwrap().completions[1].clone();
    assert_eq!(request["tools"].as_array().unwrap().len(), 1);
    assert_eq!(request["tools"][0]["function"]["name"], "echo");
    assert_not_registered(&retired, &retired_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);

    let other = tools
        .definitions()
        .iter()
        .find(|tool| tool.name == "other")
        .unwrap()
        .clone();
    for (sequence, advertised) in [
        ToolSet::Schema(ToolsSchema {
            standard_tools: vec![],
            custom_tools: None,
        }),
        ToolSet::NotGiven,
        ToolSet::Schema(ToolsSchema {
            standard_tools: vec![StandardTool::Function(other)],
            custom_tools: None,
        }),
    ]
    .into_iter()
    .enumerate()
    {
        worker
            .sender()
            .send(context(advertised), Direction::Downstream)
            .await
            .unwrap();
        let (pruned, outcome) = result(&mut worker).await.unwrap();
        assert_eq!(pruned.tool_call_id, format!("forced-call-{}", sequence + 3));
        assert_not_registered(&pruned, &outcome);
        assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    }
    assert_eq!(
        provider.state.lock().unwrap().completions[4]["tools"][0]["function"]["name"],
        "other"
    );
    // Re-advertising the original cached identities restores the original callbacks.
    worker
        .sender()
        .send(
            context(ToolSet::Schema(tools.schema())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (restored, restored_outcome) = result(&mut worker).await.unwrap();
    assert_eq!(restored.tool_call_id, "forced-call-6");
    assert_completed(&restored, &restored_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().initializes, 1);

    let foreign_server = Server::start("").await;
    let (foreign_client, foreign) = discover(&foreign_server).await;
    assert_eq!(foreign.definitions(), tools.definitions());
    let mut mismatched = tools.schema();
    let foreign_other = foreign.schema().standard_tools.into_iter().find(|tool| {
        matches!(tool, StandardTool::Direct(function) if function.schema.name == "other")
    }).unwrap();
    *mismatched.standard_tools.last_mut().unwrap() = foreign_other;
    // A valid prefix followed by a foreign handler must not partly publish a subset.
    worker
        .sender()
        .send(context(ToolSet::Schema(mismatched)), Direction::Downstream)
        .await
        .unwrap();
    let rejected = result(&mut worker).await.unwrap_err();
    assert!(rejected.contains("bound snapshot"), "{rejected}");
    assert_eq!(provider.state.lock().unwrap().completions.len(), 6);
    send_call(&worker, "other", "after-rejected-foreign").await;
    let (retained, retained_outcome) = result(&mut worker).await.unwrap();
    assert_completed(&retained, &retained_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    assert!(foreign_server.state.lock().unwrap().calls.is_empty());
    foreign_client.close().await.unwrap();

    let fresh = client.tools().await.unwrap();
    assert_ne!(fresh.schema(), tools.schema());
    binding.update(&fresh).unwrap();
    // A refresh publishes a new origin-bound inventory, without selecting handlers.
    send_call(&worker, "other", "after-refresh-before-advertising").await;
    let (unselected, unselected_outcome) = result(&mut worker).await.unwrap();
    assert_not_registered(&unselected, &unselected_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    worker
        .sender()
        .send(
            context(ToolSet::Schema(tools.schema())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(result(&mut worker)
        .await
        .unwrap_err()
        .contains("bound snapshot"));
    assert_eq!(provider.state.lock().unwrap().completions.len(), 6);
    send_call(&worker, "other", "after-rejected-old-snapshot").await;
    let (still_unselected, still_unselected_outcome) = result(&mut worker).await.unwrap();
    assert_not_registered(&still_unselected, &still_unselected_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    worker
        .sender()
        .send(
            context(ToolSet::Schema(fresh.schema())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (refreshed, refreshed_outcome) = result(&mut worker).await.unwrap();
    assert_completed(&refreshed, &refreshed_outcome);
    assert_eq!(refreshed.tool_call_id, "forced-call-7");
    assert_eq!(server.state.lock().unwrap().calls.len(), 4);
    assert_eq!(server.state.lock().unwrap().initializes, 1);

    let mut invalid_other = fresh
        .definitions()
        .iter()
        .find(|tool| tool.name == "other")
        .unwrap()
        .clone();
    invalid_other.parameters = serde_json::Value::Null;
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Schema(ToolsSchema {
                standard_tools: vec![StandardTool::Function(invalid_other)],
                custom_tools: None,
            }))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(result(&mut worker)
        .await
        .unwrap_err()
        .contains("tools require unique names and object schemas"));
    assert_eq!(provider.state.lock().unwrap().completions.len(), 7);
    // Native tool-definition validation must precede automatic handler pruning.
    send_call(&worker, "other", "after-invalid-tool-setter").await;
    let (after_invalid, after_invalid_outcome) = result(&mut worker).await.unwrap();
    assert_completed(&after_invalid, &after_invalid_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 5);

    let mut custom = fresh.schema();
    custom.custom_tools = Some(
        [(
            pipecat_core::AdapterType::Openai,
            vec![[("type".into(), json!("web_search"))].into_iter().collect()],
        )]
        .into_iter()
        .collect(),
    );
    worker
        .sender()
        .send(context(ToolSet::Schema(custom)), Direction::Downstream)
        .await
        .unwrap();
    assert!(result(&mut worker)
        .await
        .unwrap_err()
        .contains("provider-specific custom tool schemas"));
    let mut speculative_empty = context(ToolSet::NotGiven);
    let FrameData::LlmContextDetailed(speculative_context) = &mut speculative_empty.data else {
        unreachable!();
    };
    speculative_context.speculation = true;
    worker
        .sender()
        .send(speculative_empty, Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(
        result(&mut worker).await.unwrap_err(),
        "automatic MCP handler sync cannot accept sealed or speculative context"
    );
    assert_eq!(provider.state.lock().unwrap().completions.len(), 7);
    // Unsupported input cannot prune the current valid selection, even without handlers.
    send_call(&worker, "other", "after-rejected-authority-changes").await;
    let (guarded, guarded_outcome) = result(&mut worker).await.unwrap();
    assert_completed(&guarded, &guarded_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 6);

    worker
        .sender()
        .send(context(ToolSet::NotGiven), Direction::Downstream)
        .await
        .unwrap();
    let (empty, empty_outcome) = result(&mut worker).await.unwrap();
    assert_not_registered(&empty, &empty_outcome);
    assert_eq!(server.state.lock().unwrap().calls.len(), 6);
    assert_eq!(provider.state.lock().unwrap().completions.len(), 8);
    // Removing every selected handler retains the session's cleanup owner.
    assert_eq!(server.state.lock().unwrap().deletes, 0);
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert!(
        !remaining.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
                | FrameData::FunctionCallCancel(_)
        )),
        "all exact terminal outcomes were consumed"
    );
    assert_eq!(report.errors.len(), 5, "{:?}", report.errors);
    assert!(report.errors[0].error.contains("bound snapshot"));
    assert!(report.errors[1].error.contains("bound snapshot"));
    assert!(report.errors[2]
        .error
        .contains("tools require unique names and object schemas"));
    assert!(report.errors[3]
        .error
        .contains("provider-specific custom tool schemas"));
    assert_eq!(
        report.errors[4].error,
        "automatic MCP handler sync cannot accept sealed or speculative context"
    );
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|state| state.completed_deletes == 1).await;
}

#[tokio::test]
async fn automatic_tool_updates_sync_once_in_both_directions_and_explicit_mode_stays_registered() {
    for automatic in [true, false] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            let server = Server::start("").await;
            let (_client, tools) = discover(&server).await;
            let provider = Server::start("llm-force-other").await;
            let native_llm = llm(&provider);
            let binding = if automatic {
                tools.bind_llm_auto(native_llm.clone()).unwrap()
            } else {
                tools.bind_llm(native_llm.clone()).unwrap()
            };
            let mut worker = Pipeline::new(binding.stages())
                .start(PipelineConfig::default())
                .await
                .unwrap();
            let schemas = [
                (
                    ToolSet::Schema(tools.schema()),
                    true,
                    tools.definitions().to_vec(),
                ),
                (
                    subset(&tools, "echo"),
                    !automatic,
                    tools
                        .definitions()
                        .iter()
                        .filter(|tool| tool.name == "echo")
                        .cloned()
                        .collect(),
                ),
                (ToolSet::Direct(vec![]), !automatic, vec![]),
            ];
            for (sequence, (advertised, registered, expected)) in schemas.into_iter().enumerate() {
                worker
                    .sender()
                    .send(Frame::new(FrameData::LlmSetTools(advertised)), direction)
                    .await
                    .unwrap();
                // LLM consumes the setter. Each value differs from the previous native snapshot.
                // Entry synchronization completes before this native schema update.
                setter_applied(&native_llm, &expected).await;
                send_call(&worker, "other", &format!("setter-call-{sequence}")).await;
                let (settled, outcome) = result(&mut worker).await.unwrap();
                if registered {
                    assert_completed(&settled, &outcome);
                } else {
                    assert_not_registered(&settled, &outcome);
                }
            }
            assert!(provider.state.lock().unwrap().completions.is_empty());
            assert_eq!(
                server.state.lock().unwrap().calls.len(),
                if automatic { 1 } else { 3 }
            );
            assert_eq!(server.state.lock().unwrap().initializes, 1);
            worker.end().await.unwrap();
            let (remaining, report) = worker.collect().await.unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(!remaining.iter().any(|frame| matches!(
                frame.frame.data,
                FrameData::FunctionCallResult(_)
                    | FrameData::FunctionCallResultDetailed(_)
                    | FrameData::FunctionCallCancel(_)
            )));
            assert_eq!(server.state.lock().unwrap().deletes, 1);
            server.wait_for(|state| state.completed_deletes == 1).await;
        }
    }
}
