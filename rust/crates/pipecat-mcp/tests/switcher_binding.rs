mod support;

use pipecat_core::{
    ChatMessage, ChatRole, DetailedLlmContextFrame, Direction, Frame, FrameData, FunctionCall,
    FunctionCallCancelFrame, FunctionCallResultFrame, LlmContextData, NativeToolCallOutcome,
    Pipeline, PipelineConfig, PipelineWorker, StandardTool, ToolCallEffectStatus,
    ToolCallEndReason, ToolCallTerminalState, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, ServerConfig};
use pipecat_pipeline::LlmSwitcher;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use serde_json::json;
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::Server;

fn context(tools: ToolSet) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke this member's tool").into()],
            tools,
            tool_choice: None,
        },
        speculation: false,
    }))
}
fn llm(provider: &Server, name: &str) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    Arc::new(
        LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new("fixture", Context::default()),
        )
        .with_name(name),
    )
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
                FrameData::Error(error) => return Err(error.error),
                FrameData::FunctionCallCancel(cancelled) => {
                    return Err(format!(
                        "unexpected cancellation: {}",
                        cancelled.tool_call_id
                    ))
                }
                _ => {}
            }
        }
        Err("pipeline stopped before the native outcome".into())
    })
    .await
    .expect("native switcher tool outcome stalled")
}
async fn selected(worker: &mut PipelineWorker, switcher: &LlmSwitcher) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            match frame.frame.data {
                FrameData::LlmServiceMetadata(metadata)
                    if metadata.base.service_name == "secondary" =>
                {
                    assert_eq!(switcher.service_switcher().active_index(), 1);
                    return;
                }
                FrameData::Error(error) => panic!("switch failed: {}", error.error),
                _ => {}
            }
        }
        panic!("pipeline stopped before the selected member's metadata");
    })
    .await
    .expect("manual switch stalled");
}
async fn propagated(llm: &LlmProcessor, expected: &[pipecat_core::ToolDefinition]) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let snapshot = llm.request_snapshot().unwrap();
            if snapshot.context.tools.as_ref().is_some_and(|definitions| {
                definitions.len() == expected.len()
                    && definitions.iter().zip(expected).all(|(actual, expected)| {
                        actual.name == expected.name
                            && actual.description == expected.description
                            && actual.parameters == expected.parameters
                    })
            }) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("inactive member did not receive tool schemas");
}
async fn switched_cancel(
    worker: &mut PipelineWorker,
    switcher: &LlmSwitcher,
) -> (FunctionCallCancelFrame, Arc<NativeToolCallOutcome>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut selected = false;
        let mut cancellation = None;
        while let Some(frame) = worker.recv().await {
            let native = frame.frame.native_tool_outcome().unwrap().cloned();
            match frame.frame.data {
                FrameData::LlmServiceMetadata(metadata)
                    if metadata.base.service_name == "secondary" =>
                {
                    assert_eq!(switcher.service_switcher().active_index(), 1);
                    selected = true;
                }
                FrameData::FunctionCallCancel(cancelled) => {
                    assert!(
                        cancellation.is_none(),
                        "only one cancellation can settle the admitted call"
                    );
                    cancellation = Some((cancelled, native.expect("owned native cancellation")));
                }
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_) => {
                    panic!("blocked MCP call must settle as cancellation")
                }
                FrameData::Error(error) => panic!("switch cancellation failed: {}", error.error),
                _ => {}
            }
            if selected {
                if let Some(cancelled) = cancellation.take() {
                    return cancelled;
                }
            }
        }
        panic!("pipeline stopped before switch metadata and cancellation");
    })
    .await
    .expect("switch cancellation stalled")
}

#[tokio::test]
async fn existing_native_switcher_executes_mcp_on_both_members_with_one_journal_and_cleanup() {
    for automatic in [false, true] {
        let server = Server::start("").await;
        let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
        config.tools_filter = Some(BTreeSet::from(["echo".into(), "other".into()]));
        let client = McpClient::new(config).unwrap();
        let tools = client.tools().await.unwrap();
        let primary = Server::start("llm").await;
        let secondary = Server::start("llm-force-other").await;
        let primary_llm = llm(&primary, "primary");
        let secondary_llm = llm(&secondary, "secondary");
        let switcher =
            Arc::new(LlmSwitcher::new(vec![primary_llm.clone(), secondary_llm.clone()]).unwrap());
        let binding = if automatic {
            tools.bind_llm_switcher_auto(switcher.clone()).unwrap()
        } else {
            tools.bind_llm_switcher(switcher.clone()).unwrap()
        };
        let mut worker = Pipeline::new(binding.stages())
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                context(ToolSet::Schema(tools.schema())),
                Direction::Downstream,
            )
            .await
            .unwrap();
        let (first, first_outcome) = match result(&mut worker).await {
            Ok(outcome) => outcome,
            Err(error) => {
                worker.end().await.unwrap();
                let (_, report) = worker.collect().await.unwrap();
                let primary_requests = primary.state.lock().unwrap().completions.len();
                let secondary_requests = secondary.state.lock().unwrap().completions.len();
                let (calls, deletes) = {
                    let state = server.state.lock().unwrap();
                    (state.calls.len(), state.deletes)
                };
                panic!("bound MCP context must produce a native outcome: {error}; primary requests={}, secondary requests={}, MCP calls={}, cleanup={}, errors={:?}",
                    primary_requests, secondary_requests, calls, deletes, report.errors);
            }
        };
        assert_eq!(first.function_name, "echo");
        assert_eq!(first.tool_call_id, "agent-call");
        assert!(first.error.is_none());
        assert_eq!(first_outcome.reason(), ToolCallEndReason::CallbackReturned);
        assert_eq!(server.state.lock().unwrap().calls.len(), 1);
        propagated(&secondary_llm, tools.definitions()).await;
        assert_eq!(primary.state.lock().unwrap().completions.len(), 1);
        assert!(secondary.state.lock().unwrap().completions.is_empty());

        let mut next_schema = tools.schema();
        let next_definitions = if automatic {
            next_schema.standard_tools.retain(|tool| matches!(tool, StandardTool::Direct(function) if function.schema.name == "other"));
            let expected: Vec<_> = tools
                .definitions()
                .iter()
                .filter(|tool| tool.name == "other")
                .cloned()
                .collect();
            worker
                .sender()
                .send(
                    Frame::new(FrameData::LlmSetTools(ToolSet::Schema(next_schema.clone()))),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            // Both members consume the shared tool update without provider inference.
            propagated(&primary_llm, &expected).await;
            propagated(&secondary_llm, &expected).await;
            expected
        } else {
            tools.definitions().to_vec()
        };

        worker
            .sender()
            .send(
                switcher.service_switcher().switch_frame(1).unwrap(),
                Direction::Downstream,
            )
            .await
            .unwrap();
        selected(&mut worker, &switcher).await;
        assert_eq!(primary.state.lock().unwrap().completions.len(), 1);
        assert!(secondary.state.lock().unwrap().completions.is_empty());
        worker
            .sender()
            .send(context(ToolSet::Schema(next_schema)), Direction::Downstream)
            .await
            .unwrap();
        let (second, second_outcome) = result(&mut worker).await.unwrap();
        assert_eq!(second.function_name, "other");
        assert_eq!(second.tool_call_id, "forced-call-1");
        assert!(second.error.is_none());
        assert_eq!(second_outcome.reason(), ToolCallEndReason::CallbackReturned);
        assert_eq!(server.state.lock().unwrap().calls.len(), 2);
        assert_eq!(primary.state.lock().unwrap().completions.len(), 1);
        assert_eq!(secondary.state.lock().unwrap().completions.len(), 1);
        for (provider, expected) in [
            (&primary, tools.definitions()),
            (&secondary, next_definitions.as_slice()),
        ] {
            let request = provider.state.lock().unwrap().completions[0].clone();
            let advertised = request["tools"].as_array().unwrap();
            assert_eq!(advertised.len(), expected.len());
            for (definition, original) in advertised.iter().zip(expected) {
                assert_eq!(definition["function"]["name"], original.name);
                assert_eq!(definition["function"]["parameters"], original.parameters);
            }
        }
        if automatic {
            worker
                .sender()
                .send(
                    Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                        function_name: "echo".into(),
                        tool_call_id: "retired-after-switch".into(),
                        arguments: json!({}),
                    }])),
                    Direction::Downstream,
                )
                .await
                .unwrap();
            let (retired, retired_outcome) = result(&mut worker).await.unwrap();
            assert_eq!(retired.tool_call_id, "retired-after-switch");
            assert_eq!(retired.error.as_deref(), Some("tool is not registered"));
            assert_eq!(retired_outcome.reason(), ToolCallEndReason::NotRegistered);
            assert_eq!(retired_outcome.state(), ToolCallTerminalState::Failed);
            assert_eq!(retired_outcome.effects(), ToolCallEffectStatus::NotStarted);
            assert_eq!(server.state.lock().unwrap().calls.len(), 2);
        }
        // The first member's old ID is still suppressed after selecting another member.
        for (name, id) in [
            ("other", first.tool_call_id.as_str()),
            (
                if automatic { "other" } else { "echo" },
                "after-switch-duplicate",
            ),
        ] {
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
        let (barrier, barrier_outcome) = result(&mut worker).await.unwrap();
        assert_eq!(barrier.tool_call_id, "after-switch-duplicate");
        assert_eq!(
            barrier_outcome.reason(),
            ToolCallEndReason::CallbackReturned
        );
        assert!(barrier.error.is_none());
        assert_eq!(server.state.lock().unwrap().calls.len(), 3);
        assert_eq!(server.state.lock().unwrap().initializes, 1);
        assert_eq!(server.state.lock().unwrap().deletes, 0);
        worker.end().await.unwrap();
        let (remaining, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            !remaining.iter().any(|frame| matches!(
                frame.frame.data,
                FrameData::FunctionCallResult(_)
                    | FrameData::FunctionCallResultDetailed(_)
                    | FrameData::FunctionCallCancel(_)
            )),
            "the duplicate must not emit another terminal outcome"
        );
        assert_eq!(server.state.lock().unwrap().calls.len(), 3);
        assert_eq!(server.state.lock().unwrap().deletes, 1);
        server.wait_for(|state| state.completed_deletes == 1).await;
        assert_eq!(
            client
                .call("echo", serde_json::Value::Null)
                .await
                .unwrap_err(),
            pipecat_mcp::McpError::NotConnected
        );
    }
}

#[tokio::test]
async fn switching_cancels_an_admitted_mcp_call_without_closing_its_session() {
    let server = Server::start("blocked-slow").await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.tools_filter = Some(BTreeSet::from(["slow".into(), "other".into()]));
    config.limits.request_timeout = Duration::from_secs(30);
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let primary = Server::start("llm-slow").await;
    let secondary = Server::start("llm-force-other").await;
    let switcher = Arc::new(
        LlmSwitcher::new(vec![llm(&primary, "primary"), llm(&secondary, "secondary")]).unwrap(),
    );
    let binding = tools.bind_llm_switcher_auto(switcher.clone()).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            context(ToolSet::Schema(tools.schema())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    server.wait_for(|state| state.calls.len() == 1).await;
    assert_eq!(server.state.lock().unwrap().calls[0]["name"], "slow");
    worker
        .sender()
        .send(
            switcher.service_switcher().switch_frame(1).unwrap(),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (cancelled, outcome) = switched_cancel(&mut worker, &switcher).await;
    assert_eq!(cancelled.function_name, "slow");
    assert_eq!(cancelled.tool_call_id, "agent-call");
    assert!(!cancelled.run_llm);
    assert_eq!(outcome.call().function_name, "slow");
    assert_eq!(outcome.call().tool_call_id, "agent-call");
    assert_eq!(outcome.state(), ToolCallTerminalState::Cancelled);
    // Switching broadcasts a runtime interruption that aborts interruptible tasks;
    // the owned lease's Drop records TaskDropped rather than an explicit cancel command.
    assert_eq!(outcome.reason(), ToolCallEndReason::TaskDropped);
    assert_eq!(
        outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!outcome.run_llm());
    server.wait_for(|state| state.cancellations == 1).await;
    assert_eq!(primary.state.lock().unwrap().completions.len(), 1);
    assert!(secondary.state.lock().unwrap().completions.is_empty());
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(server.state.lock().unwrap().deletes, 0);

    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "slow".into(),
                tool_call_id: "agent-call".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            context(ToolSet::Schema(tools.schema())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (later, later_outcome) = result(&mut worker).await.unwrap();
    assert_eq!(later.function_name, "other");
    assert_eq!(later.tool_call_id, "forced-call-1");
    assert!(later.error.is_none());
    assert_eq!(later_outcome.reason(), ToolCallEndReason::CallbackReturned);
    assert_eq!(primary.state.lock().unwrap().completions.len(), 1);
    assert_eq!(secondary.state.lock().unwrap().completions.len(), 1);
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(server.state.lock().unwrap().deletes, 0);
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(
        !remaining.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
                | FrameData::FunctionCallCancel(_)
        )),
        "the cancelled duplicate must not settle again"
    );
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.wait_for(|state| state.completed_deletes == 1).await;
}
