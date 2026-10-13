#[path = "support/legacy_sse.rs"]
mod legacy;
mod support;

use pipecat_core::{
    ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor, LlmContextData,
    MessageContent, Pipeline, PipelineConfig, ToolCallEndReason, ToolCallTerminalState, ToolSet,
};
use pipecat_mcp::{LegacySseConfig, McpClient};
use pipecat_pipeline::DirectToolBinding;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc, time::Duration};

fn config(server: &legacy::Server, names: &[&str]) -> LegacySseConfig {
    let mut config = LegacySseConfig::new(&server.url);
    config.headers.insert(
        "authorization".into(),
        "Bearer explicit-fixture-token".into(),
    );
    config.limits.request_timeout = Duration::from_millis(250);
    config.sse_read_timeout = Duration::from_secs(2);
    config.tools_filter = Some(
        names
            .iter()
            .map(|s| (*s).to_string())
            .collect::<BTreeSet<_>>(),
    );
    config.tools_arguments.insert(
        "echo".into(),
        serde_json::Map::from_iter([("hidden".into(), json!("native-fixed"))]),
    );
    config
}
fn llm(provider: &support::Server) -> Arc<LlmProcessor> {
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ))
}
fn pair(tools: &pipecat_mcp::McpTools) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke discovered tool").into()],
            tools: ToolSet::Schema(tools.schema()),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    )
}
async fn worker(
    pair: &LLMContextAggregatorPair,
    binding: &DirectToolBinding,
) -> pipecat_core::PipelineWorker {
    let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    processors.extend(binding.stages());
    processors.push(pair.assistant());
    Pipeline::new(processors)
        .start(PipelineConfig::default())
        .await
        .unwrap()
}

#[tokio::test]
async fn standalone_sse_real_result_reaches_native_llm_and_schema_cleanup_closes_get() {
    let server = legacy::Server::start(None).await;
    let client = McpClient::new_legacy_sse(config(&server, &["echo"])).unwrap();
    let tools = client
        .tools()
        .await
        .expect("native standalone SSE discovery");
    assert_eq!(tools.definitions().len(), 1);
    // An idle event stream must outlive the separate POST request deadline.
    tokio::time::sleep(Duration::from_millis(350)).await;
    let pair = pair(&tools);
    let provider = support::Server::start("llm").await;
    let binding = DirectToolBinding::new(llm(&provider)).unwrap();
    let mut worker = worker(&pair, &binding).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("native SSE round trip failed: {}", error.error);
            }
            if let FrameData::LlmContextDetailed(context) = frame.frame.data {
                if context.context.messages.last().is_some_and(|m| {
                    m.role == ChatRole::Assistant
                        && m.content == MessageContent::Text("tool completed".into())
                }) {
                    return;
                }
            }
        }
        panic!("SSE result did not reach second native inference");
    })
    .await
    .unwrap();
    provider.wait_for(|s| s.completions.len() == 2).await;
    let requests = provider.state.lock().unwrap().completions.clone();
    assert_eq!(requests.len(), 2);
    let result = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .find(|m| m["role"] == "tool")
        .unwrap();
    assert_eq!(result["tool_call_id"], "agent-call");
    let text: Value = serde_json::from_str(result["content"].as_str().unwrap()).unwrap();
    let actual: Value = serde_json::from_str(text.as_str().unwrap()).unwrap();
    assert_eq!(actual, json!({"value":"agent","hidden":"native-fixed"}));
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.wait_for(|s| s.closed_streams == 1).await;
    {
        let state = server.state.lock().unwrap();
        assert_eq!(
            state
                .requests
                .iter()
                .filter(|(method, _, _)| method == "GET")
                .count(),
            1
        );
        assert!(!state
            .requests
            .iter()
            .any(|(method, _, _)| method == "DELETE"));
        assert!(state.requests.iter().all(|(_, _, headers)| headers
            .get("authorization")
            .is_some_and(|v| v == "Bearer explicit-fixture-token")));
        assert_eq!(
            state
                .messages
                .iter()
                .filter(|m| m["method"] == "initialize")
                .count(),
            1
        );
        assert_eq!(
            state
                .messages
                .iter()
                .filter(|m| m["method"] == "notifications/initialized")
                .count(),
            1
        );
        assert_eq!(
            state
                .messages
                .iter()
                .filter(|m| m["method"] == "tools/call")
                .count(),
            1
        );
    }
    assert_eq!(
        client
            .call("echo", json!({"value":"closed"}))
            .await
            .unwrap_err(),
        pipecat_mcp::McpError::NotConnected
    );
    client.close().await.unwrap();
}

#[tokio::test]
async fn standalone_sse_close_preserves_native_cancellation_without_rerun() {
    let server = legacy::Server::start(None).await;
    let client = McpClient::new_legacy_sse(config(&server, &["slow"])).unwrap();
    let tools = client.tools().await.unwrap();
    let pair = pair(&tools);
    let provider = support::Server::start("llm-slow").await;
    let binding = DirectToolBinding::new(llm(&provider)).unwrap();
    let worker = worker(&pair, &binding).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    server
        .wait_for(|s| s.messages.iter().any(|m| m["method"] == "tools/call"))
        .await;
    client.close().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let outcomes = binding.outcomes().snapshot();
            if let Some(receipt) = outcomes.terminal.first() {
                let outcome = &receipt.outcome;
                assert_eq!(outcome.state(), ToolCallTerminalState::Cancelled);
                assert_eq!(outcome.reason(), ToolCallEndReason::ExplicitCancel);
                assert_eq!(outcome.call().tool_call_id, "agent-call");
                assert!(!outcome.run_llm());
                assert_eq!(outcomes.total_terminal, 1);
                assert!(outcomes.active.is_empty());
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    worker.end().await.unwrap();
    let (remaining, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!remaining.iter().any(|f| matches!(
        f.frame.data,
        FrameData::FunctionCallResult(_) | FrameData::FunctionCallCancel(_)
    )));
    server.wait_for(|s| s.closed_streams == 1).await;
    assert_eq!(provider.state.lock().unwrap().completions.len(), 1);
    let state = server.state.lock().unwrap();
    assert_eq!(
        state
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call")
            .count(),
        1
    );
    assert!(!state
        .requests
        .iter()
        .any(|(method, _, _)| method == "DELETE"));
}

#[tokio::test]
async fn standalone_sse_eof_does_not_replay_and_next_call_can_reconnect() {
    let server = legacy::Server::start(None).await;
    let client = McpClient::new_legacy_sse(config(&server, &["side_effect", "echo"])).unwrap();
    client.tools().await.unwrap();
    assert_eq!(
        client.call("side_effect", json!({})).await.unwrap_err(),
        pipecat_mcp::McpError::Transport
    );
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call")
            .count(),
        1
    );
    let response = client
        .call("echo", json!({"value":"recovered"}))
        .await
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&response.text).unwrap(),
        json!({"value":"recovered","hidden":"native-fixed"})
    );
    client.close().await.unwrap();
    server.wait_for(|s| s.closed_streams == 2).await;
    let state = server.state.lock().unwrap();
    assert_eq!(
        state
            .requests
            .iter()
            .filter(|(method, _, _)| method == "GET")
            .count(),
        2
    );
    assert_eq!(
        state
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call")
            .count(),
        2
    );
    assert_eq!(
        state
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call" && m["params"]["name"] == "side_effect")
            .count(),
        1
    );
}

#[tokio::test]
async fn standalone_sse_rejects_foreign_endpoint_before_forwarding_headers_and_bounds_raw_events() {
    let foreign = legacy::Server::start(None).await;
    let server = legacy::Server::start(Some(
        foreign
            .url
            .replace("/sse", "/messages?session=owned-native"),
    ))
    .await;
    let client = McpClient::new_legacy_sse(config(&server, &["echo"])).unwrap();
    assert!(client.tools().await.is_err());
    server.wait_for(|s| s.closed_streams == 1).await;
    assert!(foreign.state.lock().unwrap().requests.is_empty());
    assert!(server.state.lock().unwrap().messages.is_empty());
    client.close().await.unwrap();

    let server = legacy::Server::start(None).await;
    let mut config = config(&server, &["oversized"]);
    config.limits.max_wire_bytes = 2048;
    let client = McpClient::new_legacy_sse(config).unwrap();
    client.tools().await.unwrap();
    assert_eq!(
        client.call("oversized", json!({})).await.unwrap_err(),
        pipecat_mcp::McpError::Transport
    );
    client.close().await.unwrap();
    server.wait_for(|s| s.closed_streams == 1).await;
    assert_eq!(
        server
            .state
            .lock()
            .unwrap()
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call")
            .count(),
        1
    );
}

#[test]
fn standalone_sse_configuration_validation_is_redacted_and_performs_no_io() {
    let mut config = LegacySseConfig::new("http://127.0.0.1:1/sse?secret-token=hidden");
    config
        .headers
        .insert("authorization".into(), "secret-token".into());
    assert!(!format!("{config:?}").contains("secret-token"));
    config.sse_read_timeout = Duration::ZERO;
    assert!(McpClient::new_legacy_sse(config.clone()).is_err());
    config.sse_read_timeout = Duration::from_secs(1);
    config.headers.insert("accept".into(), "overridden".into());
    assert!(McpClient::new_legacy_sse(config).is_err());
}

#[tokio::test]
async fn standalone_sse_post_deadline_is_independent_of_get_idle_with_keepalives() {
    let server = legacy::Server::start(None).await;
    {
        let mut state = server.state.lock().unwrap();
        state.keepalive = true;
        state.post_delay = Duration::from_millis(150);
    }
    let mut config = config(&server, &["echo"]);
    config.limits.request_timeout = Duration::from_secs(1);
    config.sse_read_timeout = Duration::from_millis(50);
    let client = McpClient::new_legacy_sse(config).unwrap();
    client.tools().await.unwrap();
    let result = client
        .call("echo", json!({"value":"delayed-ack"}))
        .await
        .expect("a legal POST must outlive the shorter GET idle timer");
    assert_eq!(
        serde_json::from_str::<Value>(&result.text).unwrap(),
        json!({"value":"delayed-ack","hidden":"native-fixed"})
    );
    client.close().await.unwrap();
    server.wait_for(|s| s.closed_streams == 1).await;
    let state = server.state.lock().unwrap();
    assert_eq!(
        state
            .messages
            .iter()
            .filter(|m| m["method"] == "tools/call")
            .count(),
        1
    );
    assert_eq!(
        state
            .requests
            .iter()
            .filter(|(method, _, _)| method == "GET")
            .count(),
        1
    );
}

#[tokio::test]
async fn standalone_sse_pending_post_is_bounded_and_close_awaits_writes_after_caller_drop() {
    for drop_caller in [false, true] {
        let server = legacy::Server::start(None).await;
        server.state.lock().unwrap().hold_posts = true;
        let mut config = config(&server, &["other"]);
        config.limits.close_timeout = Duration::from_millis(500);
        config.limits.request_timeout = if drop_caller {
            Duration::from_secs(30)
        } else {
            Duration::from_millis(100)
        };
        let client = McpClient::new_legacy_sse(config).unwrap();
        client.tools().await.unwrap();
        let call_client = client.clone();
        let call = tokio::spawn(async move { call_client.call("other", json!({})).await });
        server
            .wait_for(|s| s.messages.iter().any(|m| m["method"] == "tools/call"))
            .await;
        if drop_caller {
            call.abort();
            assert!(call.await.unwrap_err().is_cancelled());
            server
                .wait_for(|s| {
                    s.messages
                        .iter()
                        .any(|m| m["method"] == "notifications/cancelled")
                })
                .await;
        } else {
            let error = tokio::time::timeout(Duration::from_millis(800), call)
                .await
                .expect("POST request deadline")
                .unwrap()
                .unwrap_err();
            // The SDK write and response deadlines can settle the same request.
            assert!(
                matches!(
                    error,
                    pipecat_mcp::McpError::Timeout | pipecat_mcp::McpError::Transport
                ),
                "{error:?}"
            );
        }
        tokio::time::timeout(Duration::from_millis(800), client.close())
            .await
            .expect("close must not wait for the 30s request deadline")
            .unwrap();
        server
            .wait_for(|s| s.closed_streams == 1 && s.closed_posts == 1)
            .await;
        let state = server.state.lock().unwrap();
        assert_eq!(
            state
                .messages
                .iter()
                .filter(|m| m["method"] == "tools/call")
                .count(),
            1
        );
        assert!(!state
            .requests
            .iter()
            .any(|(method, _, _)| method == "DELETE"));
    }
}

#[tokio::test]
async fn standalone_sse_initializer_drop_and_connect_deadline_release_get_before_reopen() {
    for abort in [true, false] {
        let server = legacy::Server::start(None).await;
        server.state.lock().unwrap().hold_endpoint = true;
        let mut config = config(&server, &["echo"]);
        config.limits.connect_timeout = Duration::from_millis(150);
        let client = McpClient::new_legacy_sse(config).unwrap();
        let init_client = client.clone();
        let initialize = tokio::spawn(async move { init_client.tools().await });
        server.wait_for(|s| !s.requests.is_empty()).await;
        if abort {
            initialize.abort();
            assert!(initialize.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(
                initialize.await.unwrap().unwrap_err(),
                pipecat_mcp::McpError::Timeout
            );
        }
        server.wait_for(|s| s.closed_streams == 1).await;
        server.state.lock().unwrap().hold_endpoint = false;
        client.tools().await.unwrap();
        client.close().await.unwrap();
        server.wait_for(|s| s.closed_streams == 2).await;
        let state = server.state.lock().unwrap();
        assert_eq!(
            state
                .requests
                .iter()
                .filter(|(method, _, _)| method == "GET")
                .count(),
            2
        );
        assert_eq!(
            state
                .messages
                .iter()
                .filter(|m| m["method"] == "initialize")
                .count(),
            1
        );
    }
}
