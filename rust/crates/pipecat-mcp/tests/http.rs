mod support;
use pipecat_agent_workers::{ToolHandler, ToolRegistry, ToolResult};
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, FunctionCall, Pipeline, PipelineConfig,
    StandardTool,
};
use pipecat_mcp::*;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc, time::Duration};
use support::Server;
fn config_for(server: &Server) -> McpConfig {
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&server.url)));
    config.limits.request_timeout = Duration::from_secs(2);
    config
}
fn client_for(server: &Server) -> McpClient {
    McpClient::new(config_for(server)).unwrap()
}

#[tokio::test]
async fn discovery_readiness_owned_lifecycle_and_explicit_reopen() {
    let server = Server::start("").await;
    let client = client_for(&server);
    assert_eq!(server.state.lock().unwrap().initializes, 0);
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    client.close().await.unwrap();
    let tools = client.tools().await.unwrap();
    server.wait_for(|s| s.initialized == 1).await;
    assert_eq!(tools.definitions().len(), 9);
    assert!(server.state.lock().unwrap().calls.is_empty());
    assert_eq!(
        client.tools().await.unwrap().definitions(),
        tools.definitions()
    );
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    let other = client.clone();
    tokio::spawn(async move { other.close().await })
        .await
        .unwrap()
        .unwrap();
    client.close().await.unwrap();
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    client.tools().await.unwrap();
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 2);
}
#[tokio::test]
async fn concurrent_starts_share_one_initialized_session() {
    let server = Server::start("").await;
    let client = client_for(&server);
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..16 {
        let client = client.clone();
        tasks.spawn(async move { client.start().await });
    }
    while let Some(result) = tasks.join_next().await {
        result.unwrap().unwrap();
    }
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    client.close().await.unwrap();
}

#[tokio::test]
async fn initialization_rejects_unsupported_protocols_and_accepts_known_legacy_fallback() {
    for mode in ["unsupported-protocol", "modern-protocol", "no-tools"] {
        let server = Server::start(mode).await;
        let client = client_for(&server);
        assert_eq!(client.start().await.unwrap_err(), McpError::Protocol);
        server.wait_for(|state| state.deletes == 1).await;
        assert!(server.state.lock().unwrap().calls.is_empty());
        client.close().await.unwrap();
    }
    let server = Server::start("older-protocol").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    client.call("other", Value::Null).await.unwrap();
    client.close().await.unwrap();
}
#[tokio::test]
async fn filtering_fixed_arguments_and_native_schema_handlers() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    config.tools_arguments.insert(
        "echo".into(),
        json!({"hidden":"fixed-secret","injected":7})
            .as_object()
            .unwrap()
            .clone(),
    );
    config.tools_output_filters.insert(
        "echo".into(),
        Arc::new(|text| Ok(format!("filtered:{text}"))),
    );
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let definition = &tools.definitions()[0];
    assert_eq!(definition.parameters["required"], json!(["value"]));
    assert!(definition.parameters["properties"].get("hidden").is_none());
    assert_eq!(definition.parameters["additionalProperties"], false);
    assert_eq!(definition.parameters["$defs"]["Item"]["type"], "string");
    assert!(!definition.parameters.to_string().contains("fixed-secret"));
    let schema = tools.schema();
    let StandardTool::Direct(tool) = &schema.standard_tools[0] else {
        panic!("native executable handler missing")
    };
    assert!(tool
        .handler
        .call(json!({"value":"hello","hidden":"model"}))
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .starts_with("filtered:"));
    assert_eq!(
        server.state.lock().unwrap().calls[0]["arguments"],
        json!({"value":"hello","hidden":"fixed-secret","injected":7})
    );
    assert_eq!(
        client.call("other", Value::Null).await.unwrap_err(),
        McpError::UnknownTool
    );
    tools.lifecycle().cleanup().await.unwrap();
}
#[tokio::test]
async fn empty_allowlist_is_source_compatible_and_null_arguments_get_fixed_values() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.tools_filter = Some(BTreeSet::new());
    config.tools_arguments.insert(
        "echo".into(),
        json!({"hidden":"fixed"}).as_object().unwrap().clone(),
    );
    let client = McpClient::new(config).unwrap();
    assert_eq!(client.tools().await.unwrap().definitions().len(), 9);
    client.call("echo", Value::Null).await.unwrap();
    assert_eq!(
        server.state.lock().unwrap().calls[0]["arguments"],
        json!({"hidden":"fixed"})
    );
    client.close().await.unwrap();
}
#[tokio::test]
async fn typed_content_text_fallback_remote_errors_and_redaction() {
    let server = Server::start("").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    let response = client.call("blocks", Value::Null).await.unwrap();
    assert_eq!(response.text, "onetwo");
    assert!(response.is_error);
    assert_eq!(response.content.len(), 3);
    assert_eq!(response.structured_content, Some(json!({"answer":42})));
    assert!(!format!("{response:?}").contains("onetwo"));
    assert_eq!(
        client.call_text("empty", Value::Null).await.unwrap(),
        "Sorry, could not call the mcp tool"
    );
    let error = client.call("failure", Value::Null).await.unwrap_err();
    assert_eq!(error, McpError::Remote(-32602));
    assert!(!format!("{error:?} {error}").contains("secret-token"));
    let text = client.call_text("failure", Value::Null).await.unwrap();
    assert!(text.contains("-32602"));
    assert!(!text.contains("secret-token"));
    client.call("other", Value::Null).await.unwrap();
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    client.close().await.unwrap();
}
#[tokio::test]
async fn streamable_http_sse_and_stateless_json_both_work() {
    for mode in ["sse", "stateless"] {
        let server = Server::start(mode).await;
        let client = client_for(&server);
        client.tools().await.unwrap();
        assert_eq!(
            client
                .call("echo", json!({"value":"你好"}))
                .await
                .unwrap()
                .structured_content
                .unwrap()["echo"]["value"],
            "你好"
        );
        client.close().await.unwrap();
    }
}
#[tokio::test]
async fn side_effect_then_404_or_disconnect_is_never_replayed() {
    for mode in ["expire", "disconnect"] {
        let server = Server::start(mode).await;
        let client = client_for(&server);
        client.tools().await.unwrap();
        let error = client
            .call("side_effect", json!({"operation":"record-once"}))
            .await
            .unwrap_err();
        assert_eq!(error, McpError::Transport);
        assert_eq!(server.state.lock().unwrap().calls.len(), 1);
        assert_eq!(server.state.lock().unwrap().initializes, 1);
        client.call("other", Value::Null).await.unwrap();
        assert_eq!(server.state.lock().unwrap().initializes, 2);
        assert_eq!(server.state.lock().unwrap().calls.len(), 2);
        client.close().await.unwrap();
    }
}
#[tokio::test]
async fn cancellation_emits_wire_notification_and_preserves_other_calls() {
    let server = Server::start("").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    let cancel = CancellationToken::new();
    let c = client.clone();
    let token = cancel.clone();
    let task = tokio::spawn(async move { c.call_with_cancel("slow", Value::Null, token).await });
    server.wait_for(|s| s.calls.len() == 1).await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err(), McpError::Cancelled);
    server.wait_for(|s| s.cancellations == 1).await;
    assert_eq!(
        client.call("other", Value::Null).await.unwrap().text,
        "done"
    );
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    client.close().await.unwrap();
}
#[tokio::test]
async fn dropping_a_call_future_notifies_server_and_releases_capacity() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.limits.max_in_flight = 1;
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    let c = client.clone();
    let task = tokio::spawn(async move { c.call("slow", Value::Null).await });
    server.wait_for(|s| s.calls.len() == 1).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    server.wait_for(|s| s.cancellations == 1).await;
    let result = tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            match client.call("other", Value::Null).await {
                Err(McpError::Limit(_)) => tokio::task::yield_now().await,
                other => break other,
            }
        }
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.text, "done");
    client.close().await.unwrap();
}
#[tokio::test]
async fn call_timeout_and_admission_are_bounded() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.limits.max_in_flight = 1;
    config.limits.request_timeout = Duration::from_millis(60);
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    let c = client.clone();
    let task = tokio::spawn(async move { c.call("slow", Value::Null).await });
    server.wait_for(|s| s.calls.len() == 1).await;
    assert_eq!(
        client.call("other", Value::Null).await.unwrap_err(),
        McpError::Limit("in-flight requests")
    );
    // The HTTP read deadline can win the request deadline; both terminate.
    let error = task.await.unwrap().unwrap_err();
    assert!(matches!(error, McpError::Timeout | McpError::Transport));
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    client.close().await.unwrap();
}
#[tokio::test]
async fn handshake_timeout_failure_and_cancel_allow_retry() {
    for mode in ["hang", "bad-initialize"] {
        let server = Server::start(mode).await;
        let mut config = config_for(&server);
        config.limits.connect_timeout = Duration::from_millis(80);
        let client = McpClient::new(config).unwrap();
        assert!(client.start().await.is_err());
        server.state.lock().unwrap().mode.clear();
        client.tools().await.unwrap();
        client.close().await.unwrap();
    }
    let server = Server::start("hang").await;
    let client = client_for(&server);
    let c = client.clone();
    let task = tokio::spawn(async move { c.start().await });
    server.wait_for(|s| s.initializes == 1).await;
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    server.state.lock().unwrap().mode.clear();
    client.tools().await.unwrap();
    client.close().await.unwrap();
}
#[tokio::test]
async fn close_during_call_then_new_session_ignores_old_failure() {
    let server = Server::start("delayed-expire").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    let c = client.clone();
    let old = tokio::spawn(async move { c.call("side_effect", Value::Null).await });
    server.wait_for(|s| s.calls.len() == 1).await;
    client.close().await.unwrap();
    client.tools().await.unwrap();
    assert!(matches!(
        old.await.unwrap(),
        Err(McpError::Cancelled | McpError::Transport)
    ));
    tokio::time::sleep(Duration::from_millis(180)).await;
    client.call("other", Value::Null).await.unwrap();
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    client.close().await.unwrap();
}
#[tokio::test]
async fn pagination_duplicates_invalid_schema_and_discovery_limits() {
    let server = Server::start("pages").await;
    let client = client_for(&server);
    assert_eq!(client.tools().await.unwrap().definitions().len(), 9);
    client.close().await.unwrap();
    for mode in ["cursor-loop", "duplicate", "bad-schema"] {
        let server = Server::start(mode).await;
        let client = client_for(&server);
        assert_eq!(client.tools().await.unwrap_err(), McpError::Protocol);
        assert_eq!(
            client.call("echo", Value::Null).await.unwrap_err(),
            McpError::UnknownTool
        );
        client.close().await.unwrap();
    }
    for (max_tools, max_pages, max_schema_bytes) in [(1, 32, 10000), (128, 1, 10000), (128, 32, 10)]
    {
        let server = Server::start("pages").await;
        let mut config = config_for(&server);
        config.limits.max_tools = max_tools;
        config.limits.max_pages = max_pages;
        config.limits.max_schema_bytes = max_schema_bytes;
        let client = McpClient::new(config).unwrap();
        assert!(matches!(client.tools().await, Err(McpError::Limit(_))));
        client.close().await.unwrap();
    }
}
#[tokio::test]
async fn argument_output_json_wire_and_unterminated_sse_limits() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.limits.max_argument_bytes = 16;
    config.limits.max_output_bytes = 128;
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    assert_eq!(
        client.call("echo", json!([])).await.unwrap_err(),
        McpError::InvalidArguments
    );
    assert_eq!(
        client
            .call("echo", json!({"value":"long argument"}))
            .await
            .unwrap_err(),
        McpError::Limit("tool arguments")
    );
    assert!(server.state.lock().unwrap().calls.is_empty());
    assert_eq!(
        client.call("oversized", Value::Null).await.unwrap_err(),
        McpError::Limit("tool output")
    );
    client.close().await.unwrap();
    for mode in ["", "large-sse"] {
        let server = Server::start(mode).await;
        let mut config = config_for(&server);
        config.limits.max_wire_bytes = 2048;
        let client = McpClient::new(config).unwrap();
        client.tools().await.unwrap();
        assert!(matches!(
            client.call("oversized", Value::Null).await,
            Err(McpError::Transport | McpError::Timeout)
        ));
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn chunked_json_and_fragmented_empty_sse_data_are_bounded_before_parsing() {
    for mode in ["chunked-json", "fragmented-sse"] {
        let server = Server::start(mode).await;
        let mut config = config_for(&server);
        config.limits.max_wire_bytes = 2048;
        let client = McpClient::new(config).unwrap();
        client.tools().await.unwrap();
        assert_eq!(
            client.call("oversized", Value::Null).await.unwrap_err(),
            McpError::Transport
        );
        assert_eq!(server.state.lock().unwrap().calls.len(), 1);
        client.call("other", Value::Null).await.unwrap();
        assert_eq!(server.state.lock().unwrap().initializes, 2);
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn cleanup_deadline_reports_failure_instead_of_completion() {
    let server = Server::start("slow-delete").await;
    let mut config = config_for(&server);
    config.limits.close_timeout = Duration::from_millis(20);
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    assert_eq!(client.close().await.unwrap_err(), McpError::Cleanup);
    assert_eq!(server.state.lock().unwrap().completed_deletes, 0);
    assert_eq!(
        client.call("other", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    client.close().await.unwrap();
}
#[tokio::test]
async fn output_filter_errors_panics_and_oversize_are_contained() {
    for filter in [
        Arc::new(|_: String| Err("secret-token".into())) as OutputFilter,
        Arc::new(|_: String| panic!("fixture filter panic")),
        Arc::new(|_: String| Ok("x".repeat(2048))),
    ] {
        let server = Server::start("").await;
        let mut config = config_for(&server);
        config.limits.max_output_bytes = 1024;
        config.tools_output_filters.insert("other".into(), filter);
        let client = McpClient::new(config).unwrap();
        client.tools().await.unwrap();
        let error = client.call("other", Value::Null).await.unwrap_err();
        assert!(!format!("{error:?}").contains("secret-token"));
        assert!(matches!(error, McpError::OutputFilter | McpError::Limit(_)));
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn model_facing_error_filters_receive_empty_text_and_can_supply_a_fallback() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.tools_output_filters.insert(
        "failure".into(),
        Arc::new(|text| {
            assert!(text.is_empty());
            Ok("configured fallback".into())
        }),
    );
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    assert_eq!(
        client.call_text("failure", Value::Null).await.unwrap(),
        "configured fallback"
    );
    assert_eq!(
        client.call("failure", Value::Null).await.unwrap_err(),
        McpError::Remote(-32602)
    );
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    client.close().await.unwrap();
}
#[tokio::test]
async fn registry_registration_is_atomic_and_pipeline_executor_closes_session() {
    let server = Server::start("").await;
    let client = client_for(&server);
    let tools = client.tools().await.unwrap();
    let mut registry = ToolRegistry::new();
    let lifecycle = tools.register_into(&mut registry).unwrap();
    assert_eq!(registry.schemas().len(), 9);
    assert!(tools.register_into(&mut registry).is_err());
    assert_eq!(registry.schemas().len(), 9);
    let mut duplicate = ToolRegistry::new();
    duplicate
        .register(ToolHandler::new(
            pipecat_services::ToolDefinition {
                name: "other".into(),
                description: "existing".into(),
                parameters: json!({"type":"object"}),
            },
            |_, _| async { Ok(ToolResult::new(Value::Null)) },
        ))
        .unwrap();
    assert!(tools.register_into(&mut duplicate).is_err());
    assert_eq!(duplicate.schemas().len(), 1);
    let worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let call = FunctionCall {
        function_name: "echo".into(),
        tool_call_id: "owned-call".into(),
        arguments: json!({"value":"frame"}),
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call.clone(), call])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let results: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::FunctionCallResult(r) = &f.frame.data {
                Some(r)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert!(results[0].result.as_str().unwrap().contains("frame"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    lifecycle.cleanup().await.unwrap();
}

#[tokio::test]
async fn pipeline_executor_accepts_null_arguments_with_fixed_values() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.tools_arguments.insert(
        "echo".into(),
        json!({"value":"fixed", "hidden":"private"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "echo".into(),
                tool_call_id: "null-arguments".into(),
                arguments: Value::Null,
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        server.state.lock().unwrap().calls[0]["arguments"],
        json!({"value":"fixed", "hidden":"private"})
    );
}

#[tokio::test]
async fn pipeline_interruption_cancels_mcp_then_allows_a_later_tool_call() {
    let server = Server::start("").await;
    let client = client_for(&server);
    let tools = client.tools().await.unwrap();
    let worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for (name, id) in [("slow", "interrupted"), ("other", "survivor")] {
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
        if name == "slow" {
            server.wait_for(|state| state.calls.len() == 1).await;
            worker
                .sender()
                .send(Frame::interruption(), Direction::Downstream)
                .await
                .unwrap();
            server.wait_for(|state| state.cancellations == 1).await;
        }
    }
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(frames.iter().any(|frame| matches!(
        &frame.frame.data,
        FrameData::FunctionCallResult(result) if result.tool_call_id == "survivor"
    )));
    assert!(!frames.iter().any(|frame| matches!(
        &frame.frame.data,
        FrameData::FunctionCallResult(result) if result.tool_call_id == "interrupted"
    )));
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn pipeline_cleanup_surfaces_mcp_delete_failure() {
    let server = Server::start("delete-fails").await;
    let client = client_for(&server);
    let tools = client.tools().await.unwrap();
    let worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.end().await.unwrap();
    let error = worker.collect().await.unwrap_err();
    assert!(error.to_string().contains("MCP transport cleanup failed"));
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    assert!(!format!("{error:?}").contains("secret-token"));
}
#[tokio::test]
async fn explicit_headers_are_sent_only_to_same_endpoint_and_redirects_fail() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    let ServerConfig::StreamableHttp(http) = &mut config.server else {
        unreachable!()
    };
    http.headers
        .insert("authorization".into(), "Bearer secret-token".into());
    let client = McpClient::new(config).unwrap();
    assert!(!format!("{client:?}").contains("secret-token"));
    client.tools().await.unwrap();
    client.call("other", Value::Null).await.unwrap();
    client.close().await.unwrap();
    assert!(server
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .all(|(_, h)| h.get("authorization").unwrap() == "Bearer secret-token"));
    assert!(server
        .state
        .lock()
        .unwrap()
        .requests
        .iter()
        .filter(|(m, _)| m == "POST")
        .skip(1)
        .all(|(_, h)| h.get("mcp-protocol-version").unwrap() == "2025-11-25"));
    let server = Server::start("redirect").await;
    let client = client_for(&server);
    assert_eq!(client.start().await.unwrap_err(), McpError::Initialization);
    client.close().await.unwrap();
}
#[tokio::test]
async fn terminate_on_close_can_be_disabled_explicitly() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    let ServerConfig::StreamableHttp(http) = &mut config.server else {
        unreachable!()
    };
    http.terminate_on_close = false;
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 0);
}
#[test]
fn config_validation_and_debug_do_not_discover_or_expose_secrets() {
    for url in [
        "http://example.com/mcp",
        "https://user:secret@example.com/mcp",
        "file:///tmp/mcp",
        "https://example.com/mcp#fragment",
    ] {
        assert!(McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(
            HttpConfig::new(url)
        )))
        .is_err());
    }
    for name in ["host", "Mcp-Session-Id", "content-length", "accept"] {
        let mut http = HttpConfig::new("https://example.com/mcp");
        http.headers.insert(name.into(), "secret-token".into());
        assert!(McpClient::new(McpConfig::new(ServerConfig::StreamableHttp(http))).is_err());
    }
    let mut http = HttpConfig::new("https://example.com/private?token=secret-token");
    http.headers
        .insert("authorization".into(), "Bearer secret-token".into());
    let config = McpConfig::new(ServerConfig::StreamableHttp(http));
    assert!(!format!("{config:?}").contains("secret-token"));
    assert!(McpClient::new(config).is_ok());
    let mut config = McpConfig::new(ServerConfig::Stdio(StdioConfig::new("relative")));
    assert!(McpClient::new(config.clone()).is_err());
    config.server = ServerConfig::Stdio(StdioConfig::new("/explicit/native"));
    config.limits.max_in_flight = 0;
    assert!(McpClient::new(config).is_err());
}

#[tokio::test]
async fn failed_delete_is_reported_even_when_sdk_shutdown_returns_normally() {
    let server = Server::start("delete-fails").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    assert_eq!(client.close().await.unwrap_err(), McpError::Cleanup);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    assert_eq!(
        client.call("other", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    client.close().await.unwrap();
}
#[tokio::test]
async fn lifecycle_transitions_are_serialized_in_both_orders() {
    let server = Server::start("slow-delete").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    let c = client.clone();
    let closing = tokio::spawn(async move { c.close().await });
    server.wait_for(|s| s.deletes == 1).await;
    client.tools().await.unwrap();
    closing.await.unwrap().unwrap();
    client.call("other", Value::Null).await.unwrap();
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    client.close().await.unwrap();
    let server = Server::start("slow-initialize").await;
    let client = client_for(&server);
    let c = client.clone();
    let starting = tokio::spawn(async move { c.start().await });
    server.wait_for(|s| s.initializes == 1).await;
    client.close().await.unwrap();
    starting.await.unwrap().unwrap();
    assert_eq!(
        client.call("other", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
}

#[tokio::test]
async fn aborted_close_is_still_awaited_before_close_reports_success() {
    let server = Server::start("slow-delete").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    let c = client.clone();
    let closing = tokio::spawn(async move { c.close().await });
    server.wait_for(|s| s.deletes == 1).await;
    closing.abort();
    assert!(closing.await.unwrap_err().is_cancelled());
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().completed_deletes, 1);
}

#[tokio::test]
async fn aborted_close_preserves_cleanup_errors_and_serializes_reopen() {
    for (mode, reopen) in [("slow-delete-fails", false), ("slow-delete", true)] {
        let server = Server::start(mode).await;
        let client = client_for(&server);
        client.tools().await.unwrap();
        let c = client.clone();
        let closing = tokio::spawn(async move { c.close().await });
        server.wait_for(|s| s.deletes == 1).await;
        closing.abort();
        assert!(closing.await.unwrap_err().is_cancelled());
        if reopen {
            client.start().await.unwrap();
            assert_eq!(server.state.lock().unwrap().completed_deletes, 1);
            assert_eq!(server.state.lock().unwrap().initializes, 2);
        } else {
            assert_eq!(client.close().await.unwrap_err(), McpError::Cleanup);
        }
        client.close().await.unwrap();
    }
}

#[tokio::test]
async fn explicit_cancellation_interrupts_waiting_for_a_lifecycle_transition() {
    let server = Server::start("slow-delete").await;
    let client = client_for(&server);
    client.tools().await.unwrap();
    let c = client.clone();
    let closing = tokio::spawn(async move { c.close().await });
    server.wait_for(|s| s.deletes == 1).await;
    let token = CancellationToken::new();
    let mut call = Box::pin(client.call_with_cancel("other", Value::Null, token.clone()));
    assert!(futures_util::poll!(call.as_mut()).is_pending());
    token.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_millis(30), call)
            .await
            .expect("cancellation does not wait for DELETE")
            .unwrap_err(),
        McpError::Cancelled
    );
    closing.await.unwrap().unwrap();
}
#[tokio::test]
async fn native_llm_worker_executes_mcp_registry_handler_and_owns_cleanup() {
    use pipecat_agent_workers::{LlmWorker, LlmWorkerOptions, IDLE};
    use pipecat_services::{
        ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role,
    };
    let server = Server::start("").await;
    let client = client_for(&server);
    let tools = client.tools().await.unwrap();
    let mut registry = ToolRegistry::new();
    let lifecycle = tools.register_into(&mut registry).unwrap();
    let provider = Server::start("llm").await;
    let mut llm_config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    llm_config.endpoint = provider.url.clone();
    llm_config.api_key = None;
    let llm = LlmClient::new(llm_config).unwrap();
    let mut worker = LlmWorker::start_with_output(
        llm,
        CompletionRequest::new("fixture", Context::default()),
        registry,
        LlmWorkerOptions::default(),
        vec![Arc::new(lifecycle)],
    )
    .await
    .unwrap();
    worker
        .handle()
        .append(
            vec![Message::text(Role::User, "invoke the test tool")],
            true,
        )
        .await
        .unwrap();
    let frames = tokio::time::timeout(Duration::from_secs(3), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            let idle = frame.frame.metadata.get(IDLE) == Some(&Value::Bool(true));
            frames.push(frame.frame);
            if idle {
                break;
            }
        }
        frames
    })
    .await
    .unwrap();
    assert!(frames.iter().any(|f| matches!(&f.data, FrameData::FunctionCallResult(r) if r.function_name == "echo" && r.result.as_str().is_some_and(|s| s.contains("agent")))));
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data, FrameData::LlmText(t) if t.text == "tool completed")));
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(provider.state.lock().unwrap().completions.len(), 2);
    worker.handle().end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn native_llm_worker_interruption_and_drop_own_mcp_teardown() {
    use pipecat_agent_workers::{LlmWorker, LlmWorkerOptions};
    use pipecat_services::{
        ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role,
    };
    for drop_worker in [false, true] {
        let server = Server::start("").await;
        let client = client_for(&server);
        let tools = client.tools().await.unwrap();
        let mut registry = ToolRegistry::new();
        let lifecycle = tools.register_into(&mut registry).unwrap();
        let provider = Server::start("llm-slow").await;
        let mut llm_config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
        llm_config.endpoint = provider.url.clone();
        llm_config.api_key = None;
        let mut worker = LlmWorker::start_with_output(
            LlmClient::new(llm_config).unwrap(),
            CompletionRequest::new("fixture", Context::default()),
            registry,
            LlmWorkerOptions::default(),
            vec![Arc::new(lifecycle)],
        )
        .await
        .unwrap();
        worker
            .handle()
            .append(
                vec![Message::text(Role::User, "invoke the test tool")],
                true,
            )
            .await
            .unwrap();
        server.wait_for(|state| state.calls.len() == 1).await;
        if drop_worker {
            drop(worker);
            server.wait_for(|state| state.deletes == 1).await;
        } else {
            worker.handle().interrupt();
            server.wait_for(|state| state.cancellations == 1).await;
            worker.handle().end().await.unwrap();
            tokio::time::timeout(Duration::from_secs(3), worker.join())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            client.call("other", Value::Null).await.unwrap_err(),
            McpError::NotConnected
        );
        assert_eq!(server.state.lock().unwrap().deletes, 1);
        assert_eq!(provider.state.lock().unwrap().completions.len(), 1);
    }
}

#[tokio::test]
async fn closing_session_preserves_text_call_cancellation_without_running_filter() {
    use std::sync::atomic::{AtomicUsize, Ordering};
    let server = Server::start("blocked-slow").await;
    let mut config = config_for(&server);
    config.limits.request_timeout = Duration::from_secs(30);
    let filtered = Arc::new(AtomicUsize::new(0));
    let observed = filtered.clone();
    config.tools_output_filters.insert(
        "slow".into(),
        Arc::new(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Ok("filter success fallback".into())
        }),
    );
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    let caller = client.clone();
    let pending = tokio::spawn(async move { caller.call_text("slow", json!({})).await });
    server.wait_for(|state| state.calls.len() == 1).await;
    client.close().await.unwrap();
    assert_eq!(pending.await.unwrap(), Err(McpError::Cancelled));
    assert_eq!(filtered.load(Ordering::SeqCst), 0);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn closing_mcp_session_cancels_native_llm_tool_without_a_model_rerun() {
    use pipecat_agent_workers::{LlmWorker, LlmWorkerOptions, IDLE};
    use pipecat_services::{
        ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role,
    };
    let server = Server::start("blocked-slow").await;
    let mut mcp_config = config_for(&server);
    mcp_config.limits.request_timeout = Duration::from_secs(30);
    let client = McpClient::new(mcp_config).unwrap();
    let tools = client.tools().await.unwrap();
    let mut registry = ToolRegistry::new();
    let lifecycle = tools.register_into(&mut registry).unwrap();
    let provider = Server::start("llm-slow").await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let mut worker = LlmWorker::start_with_output(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
        registry,
        LlmWorkerOptions::default(),
        vec![Arc::new(lifecycle)],
    )
    .await
    .unwrap();
    worker
        .handle()
        .append(
            vec![Message::text(Role::User, "invoke the test tool")],
            true,
        )
        .await
        .unwrap();
    server.wait_for(|state| state.calls.len() == 1).await;
    client.close().await.unwrap();
    let frames = tokio::time::timeout(Duration::from_secs(3), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            let idle = frame.frame.metadata.get(IDLE) == Some(&Value::Bool(true));
            frames.push(frame.frame);
            if idle {
                break;
            }
        }
        frames
    })
    .await
    .unwrap();
    let cancelled: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.data {
            FrameData::FunctionCallCancel(call) => Some(call),
            _ => None,
        })
        .collect();
    assert_eq!(cancelled.len(), 1);
    assert_eq!(cancelled[0].tool_call_id, "agent-call");
    assert_eq!(cancelled[0].function_name, "slow");
    assert!(!cancelled[0].run_llm);
    assert!(!frames.iter().any(|frame| matches!(
        frame.data,
        FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
    )));
    assert_eq!(provider.state.lock().unwrap().completions.len(), 1);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    worker.handle().end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn closing_mcp_session_settles_pipeline_call_as_one_native_cancellation() {
    use pipecat_core::{ToolCallEffectStatus, ToolCallEndReason, ToolCallTerminalState};
    let server = Server::start("blocked-slow").await;
    let mut mcp_config = config_for(&server);
    mcp_config.limits.request_timeout = Duration::from_secs(30);
    let client = McpClient::new(mcp_config).unwrap();
    let tools = client.tools().await.unwrap();
    let worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let call = FunctionCall {
        function_name: "slow".into(),
        tool_call_id: "closed-pipeline-call".into(),
        arguments: json!({}),
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![
                call.clone(),
                call.clone(),
            ])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    server.wait_for(|state| state.calls.len() == 1).await;
    client.close().await.unwrap();
    // The retained identity cannot settle again, even after the session closes.
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let terminal: Vec<_> = frames
        .iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::FunctionCallCancel(_)
                    | FrameData::FunctionCallResult(_)
                    | FrameData::FunctionCallResultDetailed(_)
            )
        })
        .collect();
    assert_eq!(terminal.len(), 1);
    let FrameData::FunctionCallCancel(cancelled) = &terminal[0].frame.data else {
        panic!("MCP cancellation must remain a native cancellation");
    };
    assert_eq!(cancelled.function_name, call.function_name);
    assert_eq!(cancelled.tool_call_id, call.tool_call_id);
    assert!(!cancelled.run_llm);
    let outcome = terminal[0]
        .frame
        .native_tool_outcome()
        .unwrap()
        .expect("owned native receipt");
    assert_eq!(outcome.call(), &call);
    assert_eq!(outcome.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(outcome.reason(), ToolCallEndReason::ExplicitCancel);
    assert_eq!(
        outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!outcome.run_llm());
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn reference_connection_closed_code_invalidates_only_failed_tool_session_without_replay() {
    let server = Server::start("connection-closed-code").await;
    let client = client_for(&server);
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let handler = |name| {
        schema
            .standard_tools
            .iter()
            .find_map(|tool| match tool {
                StandardTool::Direct(tool) if tool.schema.name == name => {
                    Some(tool.handler.clone())
                }
                _ => None,
            })
            .unwrap()
    };
    let failure = handler("failure");
    let echo = handler("echo");
    let result = failure.call(Value::Null).await.unwrap();
    assert_eq!(
        result,
        Value::String(format!(
            "Error calling mcp tool: {}",
            McpError::Remote(-32000)
        ))
    );
    assert!(!result.to_string().contains("secret-token"));
    assert!(result.as_str().unwrap().len() <= 256);
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    // Recovery is a separate invocation through the original native handler.
    assert!(echo
        .call(json!({"value":"recovered"}))
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .contains("recovered"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    server.state.lock().unwrap().mode.clear();
    let error = client.call("failure", Value::Null).await.unwrap_err();
    assert_eq!(error, McpError::Remote(-32602));
    assert!(!format!("{error:?} {error}").contains("secret-token"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 3);
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    assert!(echo
        .call(json!({"value":"same-session"}))
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .contains("same-session"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 4);
    assert_eq!(server.state.lock().unwrap().initializes, 2);
    client.close().await.unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 2);
}

#[tokio::test]
async fn retained_snapshot_handlers_dispatch_after_refresh_but_close_revokes_them() {
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let StandardTool::Direct(echo) = &schema.standard_tools[0] else {
        panic!("discovery must attach an executable handler");
    };
    let mut worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    server.state.lock().unwrap().mode = "without-echo".into();
    let refreshed = client.tools().await.unwrap();
    assert!(refreshed.definitions().is_empty());
    for name in ["echo", "other", "never-discovered"] {
        assert_eq!(
            client.call(name, Value::Null).await.unwrap_err(),
            McpError::UnknownTool
        );
    }
    assert!(server.state.lock().unwrap().calls.is_empty());
    let result = echo
        .handler
        .call(json!({"value":"retained-schema"}))
        .await
        .unwrap();
    assert!(result.as_str().unwrap().contains("retained-schema"));
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "echo".into(),
                tool_call_id: "retained-executor".into(),
                arguments: json!({"value":"retained-executor"}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if let FrameData::FunctionCallResult(result) = frame.frame.data {
                return result;
            }
        }
        panic!("retained executor must produce a result");
    })
    .await
    .unwrap();
    assert_eq!(result.tool_call_id, "retained-executor");
    assert!(result.error.is_none());
    assert!(result
        .result
        .as_str()
        .unwrap()
        .contains("retained-executor"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    client.close().await.unwrap();
    let error = echo.handler.call(Value::Null).await.unwrap_err();
    assert!(error.to_string().contains("MCP client is closed"));
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                function_name: "echo".into(),
                tool_call_id: "closed-executor".into(),
                arguments: json!({}),
            }])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (frames, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert_eq!(
        report.errors[0].category,
        pipecat_core::ErrorCategory::Application
    );
    assert!(!report.errors[0].fatal);
    assert!(report.errors[0].error.contains("MCP client is closed"));
    let results: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::FunctionCallResult(result) => Some(result),
            _ => None,
        })
        .collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].tool_call_id, "closed-executor");
    assert!(results[0]
        .error
        .as_ref()
        .unwrap()
        .contains("MCP client is closed"));
    assert_eq!(server.state.lock().unwrap().calls.len(), 2);
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(server.state.lock().unwrap().deletes, 1);
}

#[tokio::test]
async fn retained_worker_registry_handler_runs_after_discovery_removes_its_name() {
    use pipecat_agent_workers::{LlmWorker, LlmWorkerOptions, IDLE};
    use pipecat_services::{
        ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role,
    };
    let server = Server::start("").await;
    let mut config = config_for(&server);
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let mut registry = ToolRegistry::new();
    let lifecycle = tools.register_into(&mut registry).unwrap();
    server.state.lock().unwrap().mode = "without-echo".into();
    assert!(client.tools().await.unwrap().definitions().is_empty());
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::UnknownTool
    );
    let provider = Server::start("llm").await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider.url.clone();
    config.api_key = None;
    let mut worker = LlmWorker::start_with_output(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
        registry,
        LlmWorkerOptions::default(),
        vec![Arc::new(lifecycle)],
    )
    .await
    .unwrap();
    worker
        .handle()
        .append(
            vec![Message::text(Role::User, "invoke retained echo")],
            true,
        )
        .await
        .unwrap();
    let frames = tokio::time::timeout(Duration::from_secs(3), async {
        let mut frames = vec![];
        while let Some(frame) = worker.recv().await {
            let idle = frame.frame.metadata.get(IDLE) == Some(&Value::Bool(true));
            frames.push(frame.frame);
            if idle {
                break;
            }
        }
        frames
    })
    .await
    .unwrap();
    assert!(frames.iter().any(|frame| matches!(
        &frame.data, FrameData::FunctionCallResult(result)
            if result.function_name == "echo" && result.result.as_str().is_some_and(|text| text.contains("agent"))
    )));
    assert!(frames.iter().any(|frame| matches!(
        &frame.data, FrameData::LlmText(text) if text.text == "tool completed"
    )));
    assert_eq!(server.state.lock().unwrap().calls.len(), 1);
    assert_eq!(server.state.lock().unwrap().initializes, 1);
    assert_eq!(provider.state.lock().unwrap().completions.len(), 2);
    worker.handle().end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.join())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(server.state.lock().unwrap().deletes, 1);
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
}
