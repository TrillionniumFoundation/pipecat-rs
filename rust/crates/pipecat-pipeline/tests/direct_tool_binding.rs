#[path = "../../pipecat-mcp/tests/support/mod.rs"]
mod support;

use pipecat_core::{
    ChatMessage, ChatRole, DetailedLlmContextFrame, DirectFunction, Direction, Frame, FrameData,
    FrameProcessor, FunctionCall, LlmContextData, LlmContextFrame, LocalAsyncCallback,
    LocalToolFunction, LocalToolOptions, MessageContent, NativeToolCallOutcome, Pipeline,
    PipelineConfig, PipelineWorker, ProcessorError, ToolCallEffectStatus, ToolCallEndReason,
    ToolCallTerminalState, ToolDefinition, ToolSet,
};
use pipecat_pipeline::{DirectToolBinding, DirectToolBindingConfig, LlmSwitcher};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use support::Server;
use tokio::sync::Notify;

fn llm(provider: &Server) -> Arc<LlmProcessor> {
    named_llm(provider, "local-tools")
}
fn named_llm(provider: &Server, name: &str) -> Arc<LlmProcessor> {
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
fn direct(name: &str, handler: LocalToolFunction) -> DirectFunction {
    DirectFunction {
        schema: ToolDefinition {
            name: name.into(),
            description: name.into(),
            parameters: json!({"type":"object"}),
        },
        handler,
    }
}
fn context(tools: ToolSet) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke local tool").into()],
            tools,
            tool_choice: None,
        },
        speculation: false,
    }))
}
async fn settle(
    worker: &mut PipelineWorker,
) -> Result<(FrameData, Arc<NativeToolCallOutcome>), String> {
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            let outcome = frame.frame.native_tool_outcome().unwrap().cloned();
            match &frame.frame.data {
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_) => {
                    return Ok((
                        frame.frame.data,
                        outcome.expect("owned native tool outcome"),
                    ));
                }
                FrameData::Error(error) => return Err(error.error.clone()),
                _ => {}
            }
        }
        Err("pipeline stopped before the native tool outcome".into())
    })
    .await
    .expect("local tool outcome stalled")
}
async fn send_call(worker: &PipelineWorker, name: &str, id: &str, arguments: Value) {
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: name.into(),
                tool_call_id: id.into(),
                arguments,
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
}
async fn selected(llm: &LlmProcessor, expected: Option<&[ToolDefinition]>) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let actual = llm.request_snapshot().unwrap().context.tools;
            let matches = match (actual.as_ref(), expected) {
                (None, None) => true,
                (Some(actual), Some(expected)) => {
                    actual.len() == expected.len()
                        && actual.iter().zip(expected).all(|(actual, expected)| {
                            actual.name == expected.name
                                && actual.description == expected.description
                                && actual.parameters == expected.parameters
                        })
                }
                _ => false,
            };
            if matches {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("local tool setter did not reach native LLM");
}
async fn set_tools(worker: &PipelineWorker, llm: &LlmProcessor, functions: Vec<DirectFunction>) {
    let definitions: Vec<_> = functions
        .iter()
        .map(|function| function.schema.clone())
        .collect();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Direct(functions))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    selected(llm, Some(&definitions)).await;
}
fn assert_no_remaining_outcomes(frames: &[pipecat_core::DirectedFrame]) {
    assert!(
        !frames.iter().any(|frame| matches!(
            frame.frame.data,
            FrameData::FunctionCallResult(_)
                | FrameData::FunctionCallResultDetailed(_)
                | FrameData::FunctionCallCancel(_)
        )),
        "every expected outcome was consumed; duplicate IDs must not settle again"
    );
}

#[tokio::test]
async fn advertised_application_handler_runs_and_automatically_continues_with_its_actual_result() {
    let calls = Arc::new(AtomicUsize::new(0));
    let callback_calls = calls.clone();
    let function = DirectFunction {
        schema: ToolDefinition {
            name: "echo".into(),
            description: "Application echo".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"],"additionalProperties":false}),
        },
        handler: LocalToolFunction::new(move |arguments| {
            let calls = callback_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!({"source":"application","value":arguments["value"]}))
            }
        }),
    };
    let advertised = ToolSet::Direct(vec![function.clone()]);
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke application echo").into()],
            tools: advertised.clone(),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let provider = Server::start("llm").await;
    let binding = DirectToolBinding::new(llm(&provider)).unwrap();
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
    .expect("native application tool round trip stalled");
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    let requests = provider.state.lock().unwrap().completions.clone();
    assert_eq!(requests.len(), 2,
        "executable application schema must run its handler and automatically continue: observed={observed:?}, callbacks={}, errors={:?}", calls.load(Ordering::SeqCst), report.errors);
    observed.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for request in &requests {
        assert_eq!(
            request["tools"][0]["function"]["name"],
            function.schema.name
        );
        assert_eq!(
            request["tools"][0]["function"]["parameters"],
            function.schema.parameters
        );
    }
    let result = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|message| message["role"] == "tool")
        .unwrap();
    assert_eq!(result["tool_call_id"], "agent-call");
    let actual: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
    assert_eq!(actual, json!({"source":"application","value":"agent"}));
    assert_eq!(pair.context().snapshot_rich().unwrap().tools, advertised);
    provider
        .wait_for(|state| state.completions.len() == 2)
        .await;
}

fn cleanup(counter: Arc<AtomicUsize>, fail: bool) -> LocalAsyncCallback {
    LocalAsyncCallback::new(move || {
        let counter = counter.clone();
        async move {
            counter.fetch_add(1, Ordering::SeqCst);
            if fail {
                Err(ProcessorError::new("expected direct cleanup failure"))
            } else {
                Ok(())
            }
        }
    })
}

#[tokio::test]
async fn typed_dispositions_prune_handlers_but_retain_deduplicated_cleanup_and_continue_after_failure(
) {
    let provider = Server::start("llm").await;
    let native_llm = llm(&provider);
    let shared_count = Arc::new(AtomicUsize::new(0));
    let failed_count = Arc::new(AtomicUsize::new(0));
    let final_count = Arc::new(AtomicUsize::new(0));
    let shared = cleanup(shared_count.clone(), false);
    let functions = vec![
        direct(
            "null",
            LocalToolFunction::new(|_| async { Ok(Value::Null) }).with_cleanup(shared.clone()),
        ),
        direct(
            "error",
            LocalToolFunction::new(|_| async {
                Err(ProcessorError::new("direct tool invocation was cancelled"))
            })
            .with_cleanup(shared.clone()),
        ),
        direct(
            "cancel",
            LocalToolFunction::new_cancellable(|_| async { None }).with_cleanup(shared),
        ),
        direct(
            "bad_cleanup",
            LocalToolFunction::new(|_| async { Ok(Value::Null) })
                .with_cleanup(cleanup(failed_count.clone(), true)),
        ),
        direct(
            "final_cleanup",
            LocalToolFunction::new(|_| async { Ok(Value::Null) })
                .with_cleanup(cleanup(final_count.clone(), false)),
        ),
    ];
    let binding = DirectToolBinding::new(native_llm.clone()).unwrap();
    let stages = binding.stages();
    let owner = stages[2].clone();
    let mut worker = Pipeline::new(stages)
        .start(PipelineConfig::default())
        .await
        .unwrap();
    set_tools(&worker, &native_llm, functions.clone()).await;
    for (name, reason, state) in [
        (
            "null",
            ToolCallEndReason::CallbackReturned,
            ToolCallTerminalState::Completed,
        ),
        (
            "error",
            ToolCallEndReason::CallbackError,
            ToolCallTerminalState::Failed,
        ),
        (
            "cancel",
            ToolCallEndReason::ExplicitCancel,
            ToolCallTerminalState::Cancelled,
        ),
    ] {
        send_call(&worker, name, name, json!({})).await;
        let (frame, outcome) = settle(&mut worker).await.unwrap();
        assert_eq!(outcome.call().function_name, name);
        assert_eq!(outcome.reason(), reason);
        assert_eq!(outcome.state(), state);
        assert_eq!(
            outcome.effects(),
            ToolCallEffectStatus::ExternalEffectsUnknown
        );
        match (name, frame) {
            ("null", FrameData::FunctionCallResult(result)) => {
                assert_eq!(result.result, Value::Null);
                assert!(result.error.is_none());
            }
            ("error", FrameData::FunctionCallResult(result)) => {
                assert_eq!(
                    result.error.as_deref(),
                    Some("direct tool invocation was cancelled")
                );
            }
            ("cancel", FrameData::FunctionCallCancel(cancelled)) => {
                assert!(!cancelled.run_llm);
                assert!(!outcome.run_llm());
            }
            _ => panic!("typed cancellation, successful null and legacy error must stay distinct"),
        }
    }
    // The handler-free legacy frame must prune before its actual provider request.
    let legacy = LlmContextFrame {
        messages: vec![ChatMessage::new(
            ChatRole::User,
            "legacy has no advertised tools",
        )],
        ..Default::default()
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(legacy)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (_, stale_provider_call) = settle(&mut worker).await.unwrap();
    assert_eq!(stale_provider_call.call().function_name, "echo");
    assert_eq!(
        stale_provider_call.reason(),
        ToolCallEndReason::NotRegistered
    );
    send_call(&worker, "null", "retired-null", json!({})).await;
    let (_, retired) = settle(&mut worker).await.unwrap();
    assert_eq!(retired.reason(), ToolCallEndReason::NotRegistered);
    assert_eq!(retired.effects(), ToolCallEffectStatus::NotStarted);
    set_tools(&worker, &native_llm, functions).await;
    let invalid_legacy = LlmContextFrame {
        messages: vec![ChatMessage::new(
            ChatRole::Tool,
            "invalid legacy tool result",
        )],
        ..Default::default()
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(invalid_legacy)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(settle(&mut worker)
        .await
        .unwrap_err()
        .contains("tool result requires tool_call_id"));
    send_call(&worker, "null", "invalid-legacy-keeps-old", json!({})).await;
    let (_, retained) = settle(&mut worker).await.unwrap();
    assert_eq!(retained.reason(), ToolCallEndReason::CallbackReturned);
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::NotGiven)),
            Direction::Downstream,
        )
        .await
        .unwrap();
    selected(&native_llm, None).await;
    assert_eq!(shared_count.load(Ordering::SeqCst), 0);
    assert_eq!(failed_count.load(Ordering::SeqCst), 0);
    assert_eq!(final_count.load(Ordering::SeqCst), 0);
    worker.end().await.unwrap();
    let failure = worker.collect().await.unwrap_err();
    assert!(
        matches!(failure, pipecat_core::PipelineError::Execution(message)
        if message == "DirectToolExecutor cleanup failed: expected direct cleanup failure")
    );
    assert_eq!(shared_count.load(Ordering::SeqCst), 1);
    assert_eq!(failed_count.load(Ordering::SeqCst), 1);
    assert_eq!(final_count.load(Ordering::SeqCst), 1);
    owner.cleanup().await.unwrap();
    assert_eq!(shared_count.load(Ordering::SeqCst), 1);
    assert_eq!(failed_count.load(Ordering::SeqCst), 1);
    assert_eq!(final_count.load(Ordering::SeqCst), 1);
    assert_eq!(provider.state.lock().unwrap().completions.len(), 1);
    assert!(binding.outcomes().snapshot().active.is_empty());
}

#[tokio::test]
async fn complete_context_validation_is_atomic_and_rebinding_preserves_admitted_contracts_and_history(
) {
    let provider = Server::start("llm").await;
    let native_llm = llm(&provider);
    let old_cleanup = Arc::new(AtomicUsize::new(0));
    let rejected_cleanup = Arc::new(AtomicUsize::new(0));
    let new_cleanup = Arc::new(AtomicUsize::new(0));
    let old_calls = Arc::new(AtomicUsize::new(0));
    let new_calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let calls = old_calls.clone();
    let callback_entered = entered.clone();
    let callback_release = release.clone();
    let old = direct(
        "echo",
        LocalToolFunction::new(move |arguments| {
            let calls = calls.clone();
            let entered = callback_entered.clone();
            let release = callback_release.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                if arguments["hold"] == true {
                    entered.notify_one();
                    release.notified().await;
                }
                Ok(json!("old"))
            }
        })
        .with_cleanup(cleanup(old_cleanup.clone(), false))
        .with_options(LocalToolOptions {
            cancel_on_interruption: false,
            timeout: None,
        })
        .unwrap(),
    );
    let rejected = direct(
        "echo",
        LocalToolFunction::new(|_| async { Ok(json!("rejected")) })
            .with_cleanup(cleanup(rejected_cleanup.clone(), false)),
    );
    let binding = DirectToolBinding::with_config(
        native_llm.clone(),
        DirectToolBindingConfig {
            default_timeout: None,
            ..Default::default()
        },
    )
    .unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    set_tools(&worker, &native_llm, vec![old.clone()]).await;
    for (id, functions) in [
        ("after-invalid-prune", vec![]),
        ("after-invalid-new-hook", vec![rejected]),
    ] {
        let mut invalid = context(ToolSet::Direct(functions));
        let FrameData::LlmContextDetailed(context) = &mut invalid.data else {
            unreachable!();
        };
        context.context.messages[0].role = ChatRole::Tool;
        worker
            .sender()
            .send(invalid, Direction::Downstream)
            .await
            .unwrap();
        assert!(settle(&mut worker)
            .await
            .unwrap_err()
            .contains("tool result requires tool_call_id"));
        send_call(&worker, "echo", id, json!({})).await;
        let (frame, outcome) = settle(&mut worker).await.unwrap();
        let FrameData::FunctionCallResult(result) = frame else {
            panic!("valid old handler must remain executable");
        };
        assert_eq!(result.result, json!("old"));
        assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
    }
    assert_eq!(old_calls.load(Ordering::SeqCst), 2);
    assert!(provider.state.lock().unwrap().completions.is_empty());
    send_call(&worker, "echo", "admitted-old", json!({"hold":true})).await;
    tokio::time::timeout(Duration::from_secs(3), entered.notified())
        .await
        .expect("old callback admitted");
    let calls = new_calls.clone();
    let mut new = direct(
        "echo",
        LocalToolFunction::new(move |_| {
            let calls = calls.clone();
            async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(json!("new"))
            }
        })
        .with_cleanup(cleanup(new_cleanup.clone(), false)),
    );
    new.schema.description = "rebound application contract".into();
    let deadline = direct(
        "deadline",
        LocalToolFunction::new(|_| async {
            std::future::pending::<Result<Value, ProcessorError>>().await
        })
        .with_options(LocalToolOptions {
            cancel_on_interruption: true,
            timeout: Some(Duration::from_millis(20)),
        })
        .unwrap(),
    );
    set_tools(&worker, &native_llm, vec![new, deadline]).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    send_call(&worker, "echo", "admitted-old", json!({})).await;
    send_call(&worker, "echo", "new-contract", json!({})).await;
    let (frame, outcome) = settle(&mut worker).await.unwrap();
    let FrameData::FunctionCallResult(result) = frame else {
        panic!("rebound call must return");
    };
    assert_eq!(result.tool_call_id, "new-contract");
    assert_eq!(result.result, json!("new"));
    assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
    assert_eq!(old_calls.load(Ordering::SeqCst), 3);
    assert_eq!(new_calls.load(Ordering::SeqCst), 1);
    assert!(binding
        .outcomes()
        .snapshot()
        .active
        .iter()
        .any(|call| call.tool_call_id == "admitted-old"));
    release.notify_one();
    let (frame, outcome) = settle(&mut worker).await.unwrap();
    let FrameData::FunctionCallResult(result) = frame else {
        panic!("old admitted callback must survive interruption");
    };
    assert_eq!(result.tool_call_id, "admitted-old");
    assert_eq!(result.result, json!("old"));
    assert_eq!(outcome.reason(), ToolCallEndReason::CallbackReturned);
    send_call(&worker, "deadline", "per-handler-deadline", json!({})).await;
    let (_, deadline) = settle(&mut worker).await.unwrap();
    assert_eq!(deadline.reason(), ToolCallEndReason::TimedOut);
    assert_eq!(deadline.state(), ToolCallTerminalState::Failed);
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert_no_remaining_outcomes(&remaining);
    assert_eq!(report.errors.len(), 2, "{:?}", report.errors);
    assert!(report
        .errors
        .iter()
        .all(|error| error.error.contains("tool result requires tool_call_id")));
    assert_eq!(old_cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(rejected_cleanup.load(Ordering::SeqCst), 0);
    assert_eq!(new_cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(old_calls.load(Ordering::SeqCst), 3);
    assert_eq!(new_calls.load(Ordering::SeqCst), 1);
    assert!(provider.state.lock().unwrap().completions.is_empty());
}

#[tokio::test]
async fn direct_switcher_factory_propagates_schemas_and_shares_execution_history_and_cleanup() {
    let primary = Server::start("llm").await;
    let secondary = Server::start("llm-force-other").await;
    let secondary_llm = named_llm(&secondary, "secondary");
    let switcher = Arc::new(
        LlmSwitcher::new(vec![named_llm(&primary, "primary"), secondary_llm.clone()]).unwrap(),
    );
    let cleanup_count = Arc::new(AtomicUsize::new(0));
    let hook = cleanup(cleanup_count.clone(), false);
    let calls = Arc::new(AtomicUsize::new(0));
    let functions: Vec<_> = ["echo", "other"]
        .into_iter()
        .map(|name| {
            let calls = calls.clone();
            direct(
                name,
                LocalToolFunction::new(move |_| {
                    let calls = calls.clone();
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        Ok(json!({"application":true}))
                    }
                })
                .with_cleanup(hook.clone()),
            )
        })
        .collect();
    let definitions: Vec<_> = functions
        .iter()
        .map(|function| function.schema.clone())
        .collect();
    let binding = DirectToolBinding::new_switcher(switcher.clone()).unwrap();
    let mut worker = Pipeline::new(binding.stages())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            context(ToolSet::Direct(functions.clone())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let (frame, first) = settle(&mut worker).await.unwrap();
    let FrameData::FunctionCallResult(first_result) = frame else {
        panic!("primary application result");
    };
    assert_eq!(first_result.function_name, "echo");
    assert_eq!(first.reason(), ToolCallEndReason::CallbackReturned);
    selected(&secondary_llm, Some(&definitions)).await;
    assert!(secondary.state.lock().unwrap().completions.is_empty());
    worker
        .sender()
        .send(
            switcher.service_switcher().switch_frame(1).unwrap(),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if matches!(frame.frame.data, FrameData::LlmServiceMetadata(metadata) if metadata.base.service_name == "secondary") { return; }
        }
        panic!("switch metadata missing");
    }).await.expect("switch stalled");
    assert_eq!(switcher.service_switcher().active_index(), 1);
    assert!(secondary.state.lock().unwrap().completions.is_empty());
    worker
        .sender()
        .send(context(ToolSet::Direct(functions)), Direction::Downstream)
        .await
        .unwrap();
    let (frame, second) = settle(&mut worker).await.unwrap();
    let FrameData::FunctionCallResult(result) = frame else {
        panic!("secondary application result");
    };
    assert_eq!(result.function_name, "other");
    assert_eq!(second.reason(), ToolCallEndReason::CallbackReturned);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    send_call(&worker, "other", &first_result.tool_call_id, json!({})).await;
    send_call(&worker, "other", "after-switch-duplicate", json!({})).await;
    let (_, barrier) = settle(&mut worker).await.unwrap();
    assert_eq!(barrier.call().tool_call_id, "after-switch-duplicate");
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert_no_remaining_outcomes(&remaining);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(calls.load(Ordering::SeqCst), 3);
    assert_eq!(cleanup_count.load(Ordering::SeqCst), 1);
    assert_eq!(primary.state.lock().unwrap().completions.len(), 1);
    assert_eq!(secondary.state.lock().unwrap().completions.len(), 1);
}
