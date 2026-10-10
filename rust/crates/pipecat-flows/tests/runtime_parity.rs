use async_trait::async_trait;
use pipecat_core::*;
use pipecat_flows::*;
use pipecat_services::{CancellationToken, Context};
use pipecat_turns::{AssistantAggregatorConfig, LLMAssistantAggregator, SharedContext};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

struct TestSink {
    frames: Mutex<Vec<Frame>>,
    context: SharedContext,
    assistant: LLMAssistantAggregator,
    drains: AtomicUsize,
    block_drain: AtomicBool,
    gate: Semaphore,
}
impl TestSink {
    fn new() -> Arc<Self> {
        let context = SharedContext::new(Context::default());
        Arc::new(Self {
            assistant: LLMAssistantAggregator::new(
                context.clone(),
                AssistantAggregatorConfig::default(),
            ),
            context,
            frames: Mutex::new(vec![]),
            drains: AtomicUsize::new(0),
            block_drain: AtomicBool::new(false),
            gate: Semaphore::new(0),
        })
    }
    fn frames(&self) -> Vec<Frame> {
        self.frames.lock().unwrap().clone()
    }
    fn runs(&self) -> usize {
        self.frames()
            .iter()
            .filter(|f| matches!(f.data, FrameData::LlmRun))
            .count()
    }
}
#[async_trait]
impl FlowSink for TestSink {
    async fn queue_frame(&self, frame: Frame) -> Result<(), FlowError> {
        if let FrameData::TtsSpeak(speech) = &frame.data {
            if speech.append_to_context {
                self.context.append(pipecat_services::Message::text(
                    pipecat_services::Role::Assistant,
                    &speech.text,
                ))?;
            }
        } else {
            self.assistant
                .process_frame(frame.clone(), Direction::Downstream)?;
        }
        self.frames.lock().unwrap().push(frame);
        Ok(())
    }
    async fn drain(&self) -> Result<(), FlowError> {
        self.drains.fetch_add(1, Ordering::SeqCst);
        if self.block_drain.load(Ordering::SeqCst) {
            self.gate
                .acquire()
                .await
                .map_err(|_| FlowError::Closed)?
                .forget();
        }
        Ok(())
    }
}
fn config(value: Value) -> FlowConfig {
    FlowConfig::from_json(&value.to_string()).unwrap()
}
fn schema(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        description: format!("Call {name}"),
        parameters: json!({"type":"object","properties":{}}),
    }
}
fn call(name: &str, id: &str) -> FunctionCall {
    FunctionCall {
        function_name: name.into(),
        tool_call_id: id.into(),
        arguments: json!({}),
    }
}
fn manager(config: FlowConfig, registry: HandlerRegistry) -> (FlowManager, Arc<TestSink>) {
    let sink = TestSink::new();
    let manager = FlowManager::new(
        Flow::new(config, registry).unwrap(),
        sink.clone(),
        sink.context.clone(),
    );
    (manager, sink)
}
fn simple() -> FlowConfig {
    config(
        json!({"initial_node":"a","nodes":{"a":{"task_messages":[{"role":"developer","content":"first"}],"respond_immediately":false},"b":{"task_messages":[{"role":"developer","content":"second"}],"respond_immediately":false}}}),
    )
}
fn text(context: &SharedContext) -> Vec<String> {
    context
        .snapshot_rich()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| match m.content {
            MessageContent::Text(text) => text,
            _ => panic!("expected text"),
        })
        .collect()
}

#[tokio::test]
async fn initialization_append_reset_role_persistence_and_uninterruptible_updates() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().role_message = Some("Persistent role".into());
    config.nodes.get_mut("a").unwrap().pre_actions = vec![ActionConfig::say("hello")];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    assert!(matches!(
        manager.set_node("b").await,
        Err(FlowError::NotInitialized)
    ));
    manager.initialize().await.unwrap();
    manager.initialize().await.unwrap();
    assert_eq!(text(&sink.context), vec!["hello", "first"]);
    manager.set_node("b").await.unwrap();
    assert_eq!(text(&sink.context), vec!["hello", "first", "second"]);
    assert_eq!(
        sink.frames()
            .iter()
            .filter(|f| matches!(f.data, FrameData::LlmUpdateSettings(_)))
            .count(),
        1
    );
    assert!(sink
        .frames()
        .iter()
        .filter(|f| matches!(
            f.data,
            FrameData::LlmMessagesAppendDetailed(_) | FrameData::LlmSetTools(_)
        ))
        .all(|f| !f.interruptible));
    assert_eq!(sink.runs(), 0);
    let mut config = simple();
    config.nodes.get_mut("b").unwrap().context_strategy = Some(ContextStrategy::Reset);
    let (manager, sink) = self::manager(config, HandlerRegistry::new());
    manager.initialize().await.unwrap();
    manager.set_state("preserved", json!(true)).unwrap();
    manager.set_node("b").await.unwrap();
    assert_eq!(text(&sink.context), vec!["second"]);
    assert_eq!(manager.state().unwrap()["preserved"], true);
}
#[tokio::test]
async fn render_from_state_on_every_entry_without_mutating_the_config() {
    let mut config = simple();
    config.nodes.get_mut("b").unwrap().task_messages[0].content = "For {{ user.name }}".into();
    let (manager, sink) = manager(config, HandlerRegistry::new());
    manager.initialize().await.unwrap();
    assert!(manager.set_node("b").await.is_err());
    assert_eq!(manager.current_node().as_deref(), Some("a"));
    manager.set_state("user", json!({"name":"Ada"})).unwrap();
    manager.set_node("b").await.unwrap();
    manager.set_state("user", json!({"name":"Grace"})).unwrap();
    manager.set_node("b").await.unwrap();
    assert_eq!(text(&sink.context), vec!["first", "For Ada", "For Grace"]);
}
#[tokio::test]
async fn deferred_post_actions_once_and_clear_on_transition() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().post_actions = vec![ActionConfig::say("after a")];
    config.nodes.get_mut("b").unwrap().post_actions = vec![ActionConfig::say("after b")];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    manager.initialize().await.unwrap();
    assert_eq!(text(&sink.context), vec!["first"]);
    manager.bot_stopped_speaking().await.unwrap();
    manager.bot_stopped_speaking().await.unwrap();
    assert_eq!(text(&sink.context), vec!["first", "after a"]);
    manager.set_node("a").await.unwrap();
    manager.set_node("b").await.unwrap();
    manager.bot_stopped_speaking().await.unwrap();
    assert_eq!(text(&sink.context).last().unwrap(), "after b");
    assert_eq!(
        text(&sink.context)
            .iter()
            .filter(|s| s.as_str() == "after a")
            .count(),
        1
    );
}
#[tokio::test]
async fn no_missing_text_deadlock_and_end_short_circuits_remaining_actions() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().pre_actions = vec![
        ActionConfig::new("tts_say"),
        ActionConfig::new("end_conversation"),
        ActionConfig::say("must not run"),
    ];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    manager.initialize().await.unwrap();
    assert_eq!(manager.status(), FlowStatus::Ending);
    assert!(text(&sink.context).is_empty());
    assert_eq!(manager.action_manager().ongoing_actions(), 0);
    assert_eq!(sink.frames().len(), 1);
    assert!(matches!(sink.frames()[0].data, FrameData::End(_)));
    assert!(matches!(
        manager.set_node("b").await,
        Err(FlowError::Closed)
    ));
}
#[tokio::test]
async fn custom_and_function_actions_receive_state_and_acknowledge_order() {
    let mut registry = HandlerRegistry::new();
    registry
        .register_action(
            "record",
            ActionHandler::new(|_, handle| async move { handle.set_state("ran", json!(true)) }),
        )
        .unwrap();
    let mut config = simple();
    let mut action = ActionConfig::new("function");
    action.handler = Some("record".into());
    config.nodes.get_mut("a").unwrap().pre_actions = vec![ActionConfig::say("hi"), action];
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    assert_eq!(manager.state().unwrap()["ran"], true);
    assert_eq!(sink.drains.load(Ordering::SeqCst), 2);
}
#[tokio::test]
async fn cancel_releases_action_barriers_without_corrupting_action_count() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().pre_actions = vec![ActionConfig::say("hi")];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    sink.block_drain.store(true, Ordering::SeqCst);
    let clone = manager.clone();
    let task = tokio::spawn(async move { clone.initialize().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.drains.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.cancel();
    assert!(matches!(task.await.unwrap(), Err(FlowError::Cancelled)));
    assert_eq!(manager.action_manager().ongoing_actions(), 0);
    assert_eq!(manager.status(), FlowStatus::Cancelled);
}
#[tokio::test]
async fn configured_tool_transition_updates_context_before_entering_next_node() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().functions =
        vec![FunctionConfig::edge("go", "Continue", "b")];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    manager.initialize().await.unwrap();
    let outcome = manager.execute_tools(vec![call("go", "t1")]).await.unwrap();
    assert_eq!(outcome.transitioned_to.as_deref(), Some("b"));
    assert_eq!(outcome.tools[0].result, json!({"status":"acknowledged"}));
    assert_eq!(manager.current_node().as_deref(), Some("b"));
    let messages = sink.context.snapshot_rich().unwrap().messages;
    assert_eq!(messages[1].tool_calls[0].tool_call_id, "t1");
    assert_eq!(messages[2].tool_call_id.as_deref(), Some("t1"));
    assert_eq!(
        messages.last().unwrap().content,
        MessageContent::Text("second".into())
    );
    assert!(matches!(
        manager.execute_tools(vec![call("go", "t2")]).await,
        Err(FlowError::Tool(_))
    ));
}
#[tokio::test]
async fn branch_no_response_and_contract_failures() {
    let mut config = simple();
    let mut function = FunctionConfig::tool("choose");
    function.transition_to = Some(Transition::Branch(Branch {
        field: "ok".into(),
        cases: [("True".into(), "b".into())].into(),
        default: None,
    }));
    config.nodes.get_mut("a").unwrap().functions = vec![function];
    let mut registry = HandlerRegistry::new();
    registry
        .register_tool(
            "choose",
            ToolHandler::new(schema("different"), |args, handle| async move {
                handle.set_state("name", json!("Ada"))?;
                Ok(ToolResult::configured(args))
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    let mut invocation = call("choose", "t1");
    invocation.arguments = json!({"ok":true});
    manager.execute_tools(vec![invocation]).await.unwrap();
    assert_eq!(manager.current_node().as_deref(), Some("b"));
    assert_eq!(manager.state().unwrap()["name"], "Ada");
    assert!(matches!(
        &sink.context.snapshot_rich().unwrap().tools,
        ToolSet::NotGiven
    ));
    for (result, expect_runs) in [
        (ToolResult::no_response(json!(false)), 0),
        (ToolResult::stay(json!({})), 1),
    ] {
        let mut config = simple();
        config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("choose")];
        let mut registry = HandlerRegistry::new();
        registry
            .register_tool(
                "choose",
                ToolHandler::new(schema("choose"), move |_, _| {
                    let result = result.clone();
                    async move { Ok(result) }
                }),
            )
            .unwrap();
        let (manager, sink) = self::manager(config, registry);
        manager.initialize().await.unwrap();
        let batch = manager
            .execute_tools(vec![call("choose", "t1")])
            .await
            .unwrap();
        assert_eq!(sink.runs(), expect_runs);
        if expect_runs == 1 {
            assert!(batch.tools[0]
                .error
                .as_ref()
                .unwrap()
                .contains("must return Configured"));
        } else {
            assert_eq!(batch.tools[0].result, false);
        }
    }
}
#[tokio::test]
async fn parallel_tools_last_completion_transition_wins_after_all_results() {
    let mut config = simple();
    config.nodes.insert(
        "c".into(),
        NodeConfig {
            respond_immediately: false,
            ..NodeConfig::default()
        },
    );
    for (tool, node) in [("fast", "b"), ("slow", "c")] {
        let mut function = FunctionConfig::tool(tool);
        function.transition_to = Some(Transition::Node(node.into()));
        config.nodes.get_mut("a").unwrap().functions.push(function);
    }
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let mut registry = HandlerRegistry::new();
    registry
        .register_tool(
            "fast",
            ToolHandler::new(schema("fast"), |_, handle| async move {
                handle.set_state("fast", json!(true))?;
                Ok(ToolResult::configured(json!({"fast":true})))
            }),
        )
        .unwrap();
    let (signal, gate) = (started.clone(), release.clone());
    registry
        .register_tool(
            "slow",
            ToolHandler::new(schema("slow"), move |_, handle| {
                let (signal, gate) = (signal.clone(), gate.clone());
                async move {
                    signal.notify_one();
                    gate.acquire().await.unwrap().forget();
                    assert_eq!(handle.current_node()?.as_deref(), Some("a"));
                    Ok(ToolResult::configured(json!({"slow":true})))
                }
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    let clone = manager.clone();
    let task = tokio::spawn(async move {
        clone
            .execute_tools(vec![call("fast", "1"), call("slow", "2")])
            .await
    });
    started.notified().await;
    assert_eq!(manager.current_node().as_deref(), Some("a"));
    assert!(matches!(
        manager.execute_tools(vec![call("fast", "3")]).await,
        Err(FlowError::Busy)
    ));
    release.add_permits(1);
    let batch = task.await.unwrap().unwrap();
    assert_eq!(batch.transitioned_to.as_deref(), Some("c"));
    assert_eq!(batch.tools.len(), 2);
    assert_eq!(sink.runs(), 0);
    assert!(!sink.assistant.has_function_calls_in_progress().unwrap());
}
#[tokio::test]
async fn interruption_only_cancels_opted_in_tools() {
    for cancel_on_interruption in [true, false] {
        let mut config = simple();
        config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("wait")];
        let gate = Arc::new(Semaphore::new(0));
        let started = Arc::new(Notify::new());
        let mut registry = HandlerRegistry::new();
        let (g, n) = (gate.clone(), started.clone());
        registry
            .register_tool(
                "wait",
                ToolHandler::new(schema("wait"), move |_, _| {
                    let (g, n) = (g.clone(), n.clone());
                    async move {
                        n.notify_one();
                        g.acquire().await.unwrap().forget();
                        Ok(ToolResult::configured(json!("done")))
                    }
                })
                .with_options(ToolOptions {
                    cancel_on_interruption,
                    timeout: None,
                }),
            )
            .unwrap();
        let (manager, sink) = manager(config, registry);
        manager.initialize().await.unwrap();
        let clone = manager.clone();
        let task = tokio::spawn(async move { clone.execute_tools(vec![call("wait", "1")]).await });
        started.notified().await;
        manager.interrupt();
        if !cancel_on_interruption {
            gate.add_permits(1);
        }
        let batch = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(batch.tools[0].cancelled, cancel_on_interruption);
        assert_eq!(sink.runs(), usize::from(!cancel_on_interruption));
        assert!(!sink.assistant.has_function_calls_in_progress().unwrap());
    }
}
#[tokio::test]
async fn timeout_errors_reach_context_and_allow_next_batch() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("wait")];
    let mut registry = HandlerRegistry::new();
    registry
        .register_tool(
            "wait",
            ToolHandler::new(schema("wait"), |_, _| async move {
                std::future::pending().await
            })
            .with_options(ToolOptions {
                cancel_on_interruption: false,
                timeout: Some(Duration::from_millis(5)),
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    for id in ["1", "2"] {
        let batch = manager.execute_tools(vec![call("wait", id)]).await.unwrap();
        assert!(batch.tools[0].error.as_ref().unwrap().contains("timed out"));
    }
    assert_eq!(sink.runs(), 2);
    assert!(!sink.assistant.has_function_calls_in_progress().unwrap());
}
#[tokio::test]
async fn manual_transition_cancels_old_tools_and_blocks_stale_writes() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("wait")];
    let started = Arc::new(Notify::new());
    let mut registry = HandlerRegistry::new();
    let signal = started.clone();
    registry
        .register_tool(
            "wait",
            ToolHandler::new(schema("wait"), move |_, handle| {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    handle.cancellation_token().cancelled().await;
                    handle.set_state("stale", json!(true))?;
                    Ok(ToolResult::configured(json!({})))
                }
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    let clone = manager.clone();
    let task = tokio::spawn(async move { clone.execute_tools(vec![call("wait", "1")]).await });
    started.notified().await;
    manager.set_node("b").await.unwrap();
    assert!(task.await.unwrap().is_err());
    assert_eq!(manager.current_node().as_deref(), Some("b"));
    assert!(!manager.state().unwrap().contains_key("stale"));
    assert!(!sink.assistant.has_function_calls_in_progress().unwrap());
}
struct Summary(Option<String>);
#[async_trait]
impl SummaryProvider for Summary {
    async fn generate_summary(
        &self,
        _: &str,
        _: LlmContextData,
        _: CancellationToken,
    ) -> Result<Option<String>, FlowError> {
        Ok(self.0.clone())
    }
}
#[tokio::test]
async fn real_summary_provider_contract_and_fallback_to_append() {
    for summary in [Some("Asked for pizza".into()), None] {
        let sink = TestSink::new();
        let options = ManagerOptions {
            summary_provider: Some(Arc::new(Summary(summary.clone()))),
            ..Default::default()
        };
        let manager = FlowManager::with_options(
            Flow::new(simple(), HandlerRegistry::new()).unwrap(),
            sink.clone(),
            sink.context.clone(),
            options,
        )
        .unwrap();
        manager.initialize().await.unwrap();
        manager
            .set_node_with_summary("b", "Summarize")
            .await
            .unwrap();
        if summary.is_some() {
            assert_eq!(
                text(&sink.context),
                vec![
                    "Here's a summary of the conversation:\nAsked for pizza",
                    "second"
                ]
            );
        } else {
            assert_eq!(text(&sink.context), vec!["first", "second"]);
        }
    }
}

#[tokio::test]
async fn dropped_initialization_is_retryable_and_cleans_action_counter() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().pre_actions = vec![ActionConfig::say("hi")];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    sink.block_drain.store(true, Ordering::SeqCst);
    let clone = manager.clone();
    let task = tokio::spawn(async move { clone.initialize().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.drains.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    task.abort();
    let _ = task.await;
    assert_eq!(manager.status(), FlowStatus::New);
    assert_eq!(manager.action_manager().ongoing_actions(), 0);
    sink.block_drain.store(false, Ordering::SeqCst);
    manager.initialize().await.unwrap();
    assert_eq!(manager.current_node().as_deref(), Some("a"));
}
#[tokio::test]
async fn dropped_tool_batch_is_reconciled_before_next_operation() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("wait")];
    let started = Arc::new(Notify::new());
    let signal = started.clone();
    let mut registry = HandlerRegistry::new();
    registry
        .register_tool(
            "wait",
            ToolHandler::new(schema("wait"), move |_, _| {
                let signal = signal.clone();
                async move {
                    signal.notify_one();
                    std::future::pending().await
                }
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    let clone = manager.clone();
    let task = tokio::spawn(async move { clone.execute_tools(vec![call("wait", "1")]).await });
    started.notified().await;
    task.abort();
    let _ = task.await;
    manager.set_node("b").await.unwrap();
    assert!(!sink.assistant.has_function_calls_in_progress().unwrap());
}
#[tokio::test]
async fn handler_panic_is_an_error_result_and_other_tools_finish() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().functions =
        vec![FunctionConfig::tool("panic"), FunctionConfig::tool("ok")];
    let mut registry = HandlerRegistry::new();
    registry
        .register_tool(
            "panic",
            ToolHandler::new(schema("panic"), |_, _| async {
                panic!("application handler panic");
                #[allow(unreachable_code)]
                Ok(ToolResult::configured(Value::Null))
            }),
        )
        .unwrap();
    registry
        .register_tool(
            "ok",
            ToolHandler::new(schema("ok"), |_, _| async {
                Ok(ToolResult::configured(json!(42)))
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    let outcome = manager
        .execute_tools(vec![call("panic", "1"), call("ok", "2")])
        .await
        .unwrap();
    assert_eq!(outcome.tools.len(), 2);
    assert!(outcome.tools.iter().any(|result| result
        .error
        .as_ref()
        .is_some_and(|error| error.contains("task failed"))));
    assert!(!sink.assistant.has_function_calls_in_progress().unwrap());
}
#[tokio::test]
async fn typed_arguments_reject_missing_fields_as_llm_visible_errors() {
    #[derive(serde::Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Args {
        amount: u32,
    }
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("typed")];
    let mut registry = HandlerRegistry::new();
    registry
        .register_tool(
            "typed",
            ToolHandler::typed(schema("typed"), |args: Args, _| async move {
                Ok(ToolResult::configured(json!(args.amount)))
            }),
        )
        .unwrap();
    let (manager, _) = manager(config, registry);
    manager.initialize().await.unwrap();
    let batch = manager
        .execute_tools(vec![call("typed", "1")])
        .await
        .unwrap();
    assert!(batch.tools[0]
        .error
        .as_ref()
        .unwrap()
        .contains("invalid arguments"));
    let mut invocation = call("typed", "2");
    invocation.arguments = json!({"amount":7});
    assert_eq!(
        manager.execute_tools(vec![invocation]).await.unwrap().tools[0].result,
        7
    );
}
#[tokio::test]
async fn global_tools_keep_source_order_alias_and_options() {
    let mut config = simple();
    config.global_functions = vec![FunctionConfig::tool("z_global")];
    config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("a_local")];
    let mut registry = HandlerRegistry::new();
    for name in ["z_global", "a_local"] {
        registry
            .register_tool(
                name,
                ToolHandler::new(schema("original_handler_name"), |_, _| async {
                    Ok(ToolResult::no_response(json!("ok")))
                }),
            )
            .unwrap();
    }
    let (manager, sink) = manager(config, registry);
    manager.initialize().await.unwrap();
    let context = sink.context.snapshot_rich().unwrap();
    let ToolSet::Functions(tools) = context.tools else {
        panic!("tools")
    };
    assert_eq!(
        tools.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
        ["z_global", "a_local"]
    );
    manager.set_node("b").await.unwrap();
    manager
        .execute_tools(vec![call("z_global", "1")])
        .await
        .unwrap();
    assert!(manager
        .execute_tools(vec![call("z_global", "1")])
        .await
        .is_err());
}
#[tokio::test]
async fn native_handlers_choose_nodes_and_reject_configured_sentinel() {
    for next in [NextStep::Node("b".into()), NextStep::Configured] {
        let mut config = simple();
        config.nodes.get_mut("a").unwrap().functions = vec![FunctionConfig::tool("go")];
        let mut registry = HandlerRegistry::new();
        registry
            .register_tool(
                "go",
                ToolHandler::new(schema("go"), move |_, _| {
                    let next = next.clone();
                    async move {
                        Ok(ToolResult {
                            result: json!("ok"),
                            next,
                        })
                    }
                }),
            )
            .unwrap();
        let sink = TestSink::new();
        let manager = FlowManager::new(
            Flow::native(config, registry).unwrap(),
            sink.clone(),
            sink.context.clone(),
        );
        manager.initialize().await.unwrap();
        let result = manager.execute_tools(vec![call("go", "1")]).await.unwrap();
        if result.tools[0].error.is_some() {
            assert_eq!(manager.current_node().as_deref(), Some("a"));
        } else {
            assert_eq!(manager.current_node().as_deref(), Some("b"));
        }
    }
}

#[tokio::test]
async fn surviving_post_action_fence_cannot_execute_after_cancel() {
    let mut config = simple();
    config.nodes.get_mut("a").unwrap().respond_immediately = true;
    let mut action = ActionConfig::new("function");
    action.handler = Some("after".into());
    config.nodes.get_mut("a").unwrap().post_actions = vec![action];
    let mut registry = HandlerRegistry::new();
    registry
        .register_action(
            "after",
            ActionHandler::new(|_, handle| async move {
                handle.set_state("should_not_run", json!(true))
            }),
        )
        .unwrap();
    let (manager, sink) = manager(config, registry);
    sink.block_drain.store(true, Ordering::SeqCst);
    sink.gate.add_permits(1);
    let clone = manager.clone();
    let task = tokio::spawn(async move { clone.initialize().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.drains.load(Ordering::SeqCst) < 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.cancel();
    sink.gate.add_permits(1);
    assert!(task.await.unwrap().is_err());
    assert!(!manager.state().unwrap().contains_key("should_not_run"));
    assert_eq!(manager.action_manager().ongoing_actions(), 0);
}
#[tokio::test]
async fn configured_custom_actions_register_once_and_keep_explicit_overrides() {
    let mut config = simple();
    let mut named = ActionConfig::new("audit");
    named.handler = Some("first".into());
    config.nodes.get_mut("a").unwrap().pre_actions = vec![named.clone()];
    config.nodes.get_mut("a").unwrap().post_actions = vec![ActionConfig::new("audit")];
    named.handler = Some("second".into());
    config.nodes.get_mut("b").unwrap().pre_actions = vec![named];
    let mut registry = HandlerRegistry::new();
    for name in ["first", "second"] {
        registry
            .register_action(
                name,
                ActionHandler::new(move |_, handle| async move {
                    handle.set_state("handler", json!(name))
                }),
            )
            .unwrap();
    }
    let (manager, _) = manager(config, registry);
    manager.initialize().await.unwrap();
    manager.set_node("b").await.unwrap();
    assert_eq!(manager.state().unwrap()["handler"], "first");
    manager
        .register_action(
            "audit",
            ActionHandler::new(|_, handle| async move {
                handle.set_state("handler", json!("override"))
            }),
        )
        .unwrap();
    manager.set_node("a").await.unwrap();
    assert_eq!(manager.state().unwrap()["handler"], "override");
}
#[tokio::test]
async fn end_goodbye_can_opt_out_of_context_and_failure_unwinds_actions() {
    let mut config = simple();
    let mut end = ActionConfig::new("end_conversation");
    end.parameters.insert("text".into(), json!("Goodbye"));
    end.parameters
        .insert("append_text_to_context".into(), json!(false));
    config.nodes.get_mut("a").unwrap().pre_actions = vec![end];
    let (manager, sink) = manager(config, HandlerRegistry::new());
    manager.initialize().await.unwrap();
    assert!(text(&sink.context).is_empty());
    assert!(matches!(sink.frames()[0].data, FrameData::TtsSpeak(_)));
    assert!(matches!(sink.frames()[1].data, FrameData::End(_)));
    let mut config = simple();
    let mut action = ActionConfig::new("function");
    action.handler = Some("fail".into());
    config.nodes.get_mut("a").unwrap().pre_actions = vec![action];
    let mut registry = HandlerRegistry::new();
    registry
        .register_action(
            "fail",
            ActionHandler::new(|_, _| async { Err(FlowError::Action("deliberate".into())) }),
        )
        .unwrap();
    let (manager, _) = self::manager(config, registry);
    assert!(manager.initialize().await.is_err());
    assert_eq!(manager.status(), FlowStatus::New);
    assert_eq!(manager.action_manager().ongoing_actions(), 0);
}
struct SlowSummary;
#[async_trait]
impl SummaryProvider for SlowSummary {
    async fn generate_summary(
        &self,
        _: &str,
        _: LlmContextData,
        _: CancellationToken,
    ) -> Result<Option<String>, FlowError> {
        std::future::pending().await
    }
}
#[tokio::test]
async fn summary_timeout_falls_back_without_mutating_default_strategy() {
    let sink = TestSink::new();
    let options = ManagerOptions {
        summary_provider: Some(Arc::new(SlowSummary)),
        summary_timeout: Duration::from_millis(5),
        context_strategy: ContextStrategy::Reset,
        ..Default::default()
    };
    let manager = FlowManager::with_options(
        Flow::new(simple(), HandlerRegistry::new()).unwrap(),
        sink.clone(),
        sink.context.clone(),
        options,
    )
    .unwrap();
    manager.initialize().await.unwrap();
    manager
        .set_node_with_summary("b", "Summarize")
        .await
        .unwrap();
    assert_eq!(text(&sink.context), vec!["first", "second"]);
    manager.set_node("a").await.unwrap();
    assert_eq!(text(&sink.context), vec!["first"]);
}

#[tokio::test]
async fn post_speech_and_end_are_not_queued_ahead_of_the_response_fence() {
    for action in [
        ActionConfig::say("after response"),
        ActionConfig::new("end_conversation"),
    ] {
        let mut config = simple();
        let node = config.nodes.get_mut("a").unwrap();
        node.respond_immediately = true;
        node.post_actions = vec![action];
        let (manager, sink) = manager(config, HandlerRegistry::new());
        sink.block_drain.store(true, Ordering::SeqCst);
        sink.gate.add_permits(1);
        let clone = manager.clone();
        let task = tokio::spawn(async move { clone.initialize().await });
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.drains.load(Ordering::SeqCst) < 2 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        assert_eq!(sink.runs(), 1);
        assert!(!sink
            .frames()
            .iter()
            .any(|frame| matches!(frame.data, FrameData::TtsSpeak(_) | FrameData::End(_))));
        sink.gate.add_permits(2);
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(sink
            .frames()
            .iter()
            .any(|frame| matches!(frame.data, FrameData::TtsSpeak(_) | FrameData::End(_))));
    }
}
