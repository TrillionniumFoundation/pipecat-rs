use pipecat_core::*;
use pipecat_flows::*;
use pipecat_services::Context;
use pipecat_turns::{AssistantAggregatorConfig, LLMContextAggregatorPair, UserAggregatorConfig};
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn flow() -> Flow {
    Flow::new(FlowConfig::from_json(r#"{"initial_node":"a","nodes":{"a":{"task_messages":[{"role":"developer","content":"choose"}],"functions":[{"name":"go","description":"Continue","transition_only":true,"transition_to":"b"}],"respond_immediately":false},"b":{"task_messages":[{"role":"developer","content":"complete"}],"respond_immediately":false}}}"#).unwrap(),HandlerRegistry::new()).unwrap()
}
#[tokio::test]
async fn pipeline_executes_declared_tools_and_keeps_context_without_duplicate_updates() {
    let pair = LLMContextAggregatorPair::new(
        Context::default(),
        UserAggregatorConfig::default(),
        AssistantAggregatorConfig::default(),
    );
    let sink = Arc::new(PipelineFlowSink::new());
    let manager = FlowManager::new(flow(), sink.clone(), pair.context());
    let mut worker = Pipeline::new(vec![
        pair.user(),
        Arc::new(FlowProcessor::new(manager.clone())),
        pair.assistant(),
        Arc::new(FlowActionBoundary::new(manager.clone())),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    sink.bind(worker.sender()).unwrap();
    tokio::time::timeout(Duration::from_secs(2), manager.initialize())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(pair.context().snapshot_rich().unwrap().messages.len(), 1);
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "go".into(),
                tool_call_id: "call1".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while manager.current_node().as_deref() != Some("b") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(context.messages.len(), 4);
    assert_eq!(context.messages[2].tool_call_id.as_deref(), Some("call1"));
    assert!(!pair.assistant().has_function_calls_in_progress().unwrap());
    assert_eq!(sink.pending_barriers(), 0);
    worker.end().await.unwrap();
    while worker.recv().await.is_some() {}
    let report = worker.join().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(manager.status(), FlowStatus::Ended);
}
#[tokio::test]
async fn public_tool_dispatch_is_not_executed_twice_by_the_processor() {
    let pair =
        LLMContextAggregatorPair::new(Context::default(), Default::default(), Default::default());
    let sink = Arc::new(PipelineFlowSink::new());
    let manager = FlowManager::new(flow(), sink.clone(), pair.context());
    let worker = Pipeline::new(vec![
        pair.user(),
        Arc::new(FlowProcessor::new(manager.clone())),
        pair.assistant(),
        Arc::new(FlowActionBoundary::new(manager.clone())),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    sink.bind(worker.sender()).unwrap();
    manager.initialize().await.unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        manager.execute_tools(vec![FunctionCall {
            function_name: "go".into(),
            tool_call_id: "1".into(),
            arguments: json!({}),
        }]),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.tools.len(), 1);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test]
async fn cancelled_pipeline_releases_pending_drain_without_an_ack() {
    let sink = Arc::new(PipelineFlowSink::new());
    let mut worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .start(Default::default())
        .await
        .unwrap();
    sink.bind(worker.sender()).unwrap();
    let clone = sink.clone();
    let wait = tokio::spawn(async move { clone.drain().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.pending_barriers() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker.cancel("stop").await.unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(1), wait)
        .await
        .unwrap()
        .unwrap()
        .is_err());
    assert_eq!(sink.pending_barriers(), 0);
    while worker.recv().await.is_some() {}
    worker.join().await.unwrap();
}
#[tokio::test]
async fn dropping_drain_cleans_pending_receipt_and_sink_cannot_rebind() {
    let sink = Arc::new(PipelineFlowSink::new());
    let worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .start(Default::default())
        .await
        .unwrap();
    sink.bind(worker.sender()).unwrap();
    assert!(sink.bind(worker.sender()).is_err());
    let clone = sink.clone();
    let wait = tokio::spawn(async move { clone.drain().await });
    tokio::time::timeout(Duration::from_secs(1), async {
        while sink.pending_barriers() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    wait.abort();
    let _ = wait.await;
    assert_eq!(sink.pending_barriers(), 0);
    worker.cancel("cleanup").await.unwrap();
    worker.collect().await.unwrap();
}

#[tokio::test]
async fn post_function_waits_for_actual_native_llm_completion() {
    use pipecat_services::{ClientConfig, CompletionRequest, LlmClient, LlmProcessor, Provider};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
        sync::{Notify, Semaphore},
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let (signal, gate) = (started.clone(), release.clone());
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buffer = [0u8; 4096];
        loop {
            let count = socket.read(&mut buffer).await.unwrap();
            assert!(count > 0);
            request.extend_from_slice(&buffer[..count]);
            if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
                let headers = String::from_utf8_lossy(&request[..end]);
                let len = headers
                    .lines()
                    .find_map(|line| {
                        line.to_ascii_lowercase()
                            .strip_prefix("content-length:")
                            .and_then(|value| value.trim().parse::<usize>().ok())
                    })
                    .unwrap_or(0);
                if request.len() >= end + 4 + len {
                    break;
                }
            }
        }
        signal.notify_one();
        gate.acquire().await.unwrap().forget();
        let body="data: {\"choices\":[{\"delta\":{\"content\":\"Hello\"},\"finish_reason\":null}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body).as_bytes()).await.unwrap();
    });
    let mut client = ClientConfig::new(Provider::OpenAi, "test-only");
    client.endpoint = format!("http://{address}/v1/chat/completions");
    let pair =
        LLMContextAggregatorPair::new(Context::default(), Default::default(), Default::default());
    let sink = Arc::new(PipelineFlowSink::new());
    let mut handlers = HandlerRegistry::new();
    handlers
        .register_action(
            "after",
            ActionHandler::new(|_, handle| async move { handle.set_state("after", json!(true)) }),
        )
        .unwrap();
    let config=FlowConfig::from_json(r#"{"initial_node":"a","nodes":{"a":{"task_messages":[{"role":"developer","content":"Say hello"}],"post_actions":[{"type":"function","handler":"after"}]}}}"#).unwrap();
    let manager = FlowManager::new(
        Flow::new(config, handlers).unwrap(),
        sink.clone(),
        pair.context(),
    );
    let worker = Pipeline::new(vec![
        pair.user(),
        Arc::new(LlmProcessor::new(
            LlmClient::new(client).unwrap(),
            CompletionRequest::new("test", Context::default()),
        )),
        Arc::new(FlowProcessor::new(manager.clone())),
        pair.assistant(),
        Arc::new(FlowActionBoundary::new(manager.clone())),
    ])
    .start(Default::default())
    .await
    .unwrap();
    sink.bind(worker.sender()).unwrap();
    let clone = manager.clone();
    let initialize = tokio::spawn(async move { clone.initialize().await });
    tokio::time::timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert!(
        !manager.state().unwrap().contains_key("after"),
        "post action overtook native inference"
    );
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(2), initialize)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(manager.state().unwrap()["after"], true);
    assert!(pair
        .context()
        .snapshot()
        .unwrap()
        .messages
        .iter()
        .any(|message| message.content == Some(pipecat_services::Content::Text("Hello".into()))));
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

#[tokio::test]
async fn repeated_live_declaration_does_not_execute_or_cancel_the_original_call() {
    use tokio::sync::{Notify, Semaphore};
    let pair =
        LLMContextAggregatorPair::new(Context::default(), Default::default(), Default::default());
    let sink = Arc::new(PipelineFlowSink::new());
    let started = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let mut handlers = HandlerRegistry::new();
    let (signal, gate) = (started.clone(), release.clone());
    handlers
        .register_tool(
            "slow",
            ToolHandler::new(
                ToolDefinition {
                    name: "slow".into(),
                    description: "Wait".into(),
                    parameters: json!({"type":"object"}),
                },
                move |_, handle| {
                    let (signal, gate) = (signal.clone(), gate.clone());
                    async move {
                        signal.notify_one();
                        gate.acquire().await.unwrap().forget();
                        handle.set_state("calls", json!(1))?;
                        Ok(ToolResult::no_response(json!({"done":true})))
                    }
                },
            ),
        )
        .unwrap();
    let config=FlowConfig::from_json(r#"{"initial_node":"a","nodes":{"a":{"task_messages":[],"functions":[{"name":"slow"}],"respond_immediately":false}}}"#).unwrap();
    let manager = FlowManager::new(
        Flow::new(config, handlers).unwrap(),
        sink.clone(),
        pair.context(),
    );
    let worker = Pipeline::new(vec![
        pair.user(),
        Arc::new(FlowProcessor::new(manager.clone())),
        pair.assistant(),
        Arc::new(FlowActionBoundary::new(manager.clone())),
    ])
    .start(Default::default())
    .await
    .unwrap();
    sink.bind(worker.sender()).unwrap();
    manager.initialize().await.unwrap();
    let frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "slow".into(),
        tool_call_id: "same".into(),
        arguments: json!({}),
    }]));
    worker
        .sender()
        .send(frame.clone(), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), started.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(frame.data), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(10)).await;
    release.add_permits(1);
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if pair
                .context()
                .snapshot_rich()
                .unwrap()
                .messages
                .iter()
                .any(|message| {
                    message.tool_call_id.as_deref() == Some("same")
                        && message.content == MessageContent::Text("{\"done\":true}".into())
                })
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(manager.state().unwrap()["calls"], 1);
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
