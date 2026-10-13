use pipecat_core::StandardTool;
use pipecat_mcp::*;
use serde_json::{json, Value};
use std::{path::PathBuf, time::Duration};
fn config() -> McpConfig {
    let mut stdio = StdioConfig::new(env!("CARGO_BIN_EXE_pipecat-mcp-fixture"));
    stdio.arguments.push("--native-test-fixture".into());
    let mut config = McpConfig::new(ServerConfig::Stdio(stdio));
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.request_timeout = Duration::from_secs(2);
    config
}
async fn gone(pid: u64) {
    #[cfg(target_os = "linux")]
    tokio::time::timeout(Duration::from_secs(3), async {
        while PathBuf::from(format!("/proc/{pid}")).exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("owned native fixture reaped");
    #[cfg(not(target_os = "linux"))]
    let _ = pid;
}
#[tokio::test]
async fn owned_native_stdio_initializes_discovers_calls_and_reaps() {
    let client = McpClient::new(config()).unwrap();
    let tools = client.tools().await.unwrap();
    assert_eq!(tools.definitions().len(), 9);
    let result = client
        .call("echo", json!({"value":"native"}))
        .await
        .unwrap();
    let pid = result.structured_content.unwrap()["pid"].as_u64().unwrap();
    let other = client.clone();
    tokio::spawn(async move { other.close().await })
        .await
        .unwrap()
        .unwrap();
    gone(pid).await;
    assert_eq!(
        client.call("echo", Value::Null).await.unwrap_err(),
        McpError::NotConnected
    );
    client.tools().await.unwrap();
    let new_pid = client
        .call("echo", Value::Null)
        .await
        .unwrap()
        .structured_content
        .unwrap()["pid"]
        .as_u64()
        .unwrap();
    assert_ne!(new_pid, pid);
    client.close().await.unwrap();
    gone(new_pid).await;
}
#[tokio::test]
async fn stdio_environment_is_explicit_and_stderr_never_becomes_tool_content() {
    let mut config = config();
    let ServerConfig::Stdio(stdio) = &mut config.server else {
        unreachable!()
    };
    stdio
        .environment
        .insert("MCP_FIXTURE_EXPLICIT".into(), "only-this-value".into());
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    let result = client.call("environment", Value::Null).await.unwrap();
    assert_eq!(result.text, "only-this-value");
    assert_eq!(result.structured_content.unwrap()["path_present"], false);
    let error = client.call("failure", Value::Null).await.unwrap_err();
    assert_eq!(error, McpError::Remote(-32602));
    assert!(!format!("{error:?}").contains("secret-token"));
    client.close().await.unwrap();
}
#[tokio::test]
async fn stdio_disconnect_drops_only_failed_session_and_next_call_reconnects() {
    let client = McpClient::new(config()).unwrap();
    client.tools().await.unwrap();
    let first = client
        .call("echo", Value::Null)
        .await
        .unwrap()
        .structured_content
        .unwrap()["pid"]
        .as_u64()
        .unwrap();
    assert_eq!(
        client.call("side_effect", Value::Null).await.unwrap_err(),
        McpError::Transport
    );
    gone(first).await;
    let next = client
        .call("echo", Value::Null)
        .await
        .unwrap()
        .structured_content
        .unwrap()["pid"]
        .as_u64()
        .unwrap();
    assert_ne!(first, next);
    client.close().await.unwrap();
    gone(next).await;
}
#[tokio::test]
async fn dropping_last_owner_releases_stdio_but_schema_handlers_keep_it_alive() {
    let client = McpClient::new(config()).unwrap();
    let tools = client.tools().await.unwrap();
    let schema = tools.schema();
    let pid = client
        .call("echo", Value::Null)
        .await
        .unwrap()
        .structured_content
        .unwrap()["pid"]
        .as_u64()
        .unwrap();
    drop(client);
    drop(tools);
    let StandardTool::Direct(tool) = &schema.standard_tools[0] else {
        panic!("handler")
    };
    assert!(tool
        .handler
        .call(json!({"value":"alive"}))
        .await
        .unwrap()
        .as_str()
        .unwrap()
        .contains("alive"));
    drop(schema);
    gone(pid).await;
}
#[tokio::test]
async fn stdio_cancelled_and_timed_out_handshake_reaps_native_process() {
    for cancelled in [true, false] {
        let path = std::env::temp_dir().join(format!(
            "pipecat-mcp-owned-fixture-{}-{}.pid",
            std::process::id(),
            cancelled
        ));
        let mut config = config();
        config.limits.connect_timeout = Duration::from_millis(120);
        let ServerConfig::Stdio(stdio) = &mut config.server else {
            unreachable!()
        };
        stdio.arguments.push(path.to_str().unwrap().into());
        stdio
            .environment
            .insert("MCP_FIXTURE_MODE".into(), "hang-initialize".into());
        let client = McpClient::new(config).unwrap();
        let c = client.clone();
        let task = tokio::spawn(async move { c.start().await });
        let pid = tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if let Ok(text) = std::fs::read_to_string(&path) {
                    if let Ok(pid) = text.parse::<u64>() {
                        break pid;
                    }
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        if cancelled {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            assert_eq!(task.await.unwrap().unwrap_err(), McpError::Timeout);
        }
        client.close().await.unwrap();
        gone(pid).await;
        std::fs::remove_file(path).unwrap();
    }
}
#[tokio::test]
async fn oversized_unterminated_stdio_message_is_bounded_and_child_reaped() {
    let mut config = config();
    config.limits.max_wire_bytes = 2048;
    let ServerConfig::Stdio(stdio) = &mut config.server else {
        unreachable!()
    };
    stdio
        .environment
        .insert("MCP_FIXTURE_MODE".into(), "unterminated".into());
    let client = McpClient::new(config).unwrap();
    client.tools().await.unwrap();
    let pid = client
        .call("echo", Value::Null)
        .await
        .unwrap()
        .structured_content
        .unwrap()["pid"]
        .as_u64()
        .unwrap();
    assert_eq!(
        client.call("oversized", Value::Null).await.unwrap_err(),
        McpError::Transport
    );
    gone(pid).await;
    client.close().await.unwrap();
}
#[tokio::test]
async fn cancelling_native_call_keeps_session_available() {
    let client = McpClient::new(config()).unwrap();
    client.tools().await.unwrap();
    let cancel = CancellationToken::new();
    let c = client.clone();
    let token = cancel.clone();
    let task = tokio::spawn(async move { c.call_with_cancel("slow", Value::Null, token).await });
    tokio::time::sleep(Duration::from_millis(30)).await;
    cancel.cancel();
    assert_eq!(task.await.unwrap().unwrap_err(), McpError::Cancelled);
    assert_eq!(
        client.call("other", Value::Null).await.unwrap().text,
        "done"
    );
    client.close().await.unwrap();
}
#[tokio::test]
async fn nonexistent_native_executable_reports_redacted_initialization_failure() {
    let config = McpConfig::new(ServerConfig::Stdio(StdioConfig::new(
        "/nonexistent/secret-token-native-fixture",
    )));
    let client = McpClient::new(config).unwrap();
    let error = client.start().await.unwrap_err();
    assert_eq!(error, McpError::Initialization);
    assert!(!format!("{client:?} {error:?}").contains("secret-token"));
    client.close().await.unwrap();
}

#[tokio::test]
async fn closing_native_stdio_executor_cancels_dispatched_call_and_reaps_child() {
    use pipecat_core::{
        Direction, Frame, FrameData, FunctionCall, Pipeline, ToolCallEffectStatus,
        ToolCallTerminalState,
    };
    use std::sync::Arc;
    let directory = std::env::temp_dir().join(format!(
        "pipecat-mcp-executor-cancel-{}-{}",
        std::process::id(),
        Frame::new(FrameData::LlmRun).id,
    ));
    std::fs::create_dir(&directory).unwrap();
    let marker = directory.join("dispatched");
    let pid_file = directory.join("pid");
    let mut config = config();
    let ServerConfig::Stdio(stdio) = &mut config.server else {
        unreachable!()
    };
    stdio.arguments.push(pid_file.to_str().unwrap().into());
    stdio.environment.insert(
        "MCP_FIXTURE_SLOW_MARKER".into(),
        marker.to_str().unwrap().into(),
    );
    let client = McpClient::new(config).unwrap();
    let tools = client.tools().await.unwrap();
    let pid: u64 = std::fs::read_to_string(&pid_file).unwrap().parse().unwrap();
    let worker = Pipeline::new(vec![Arc::new(tools.executor().unwrap())])
        .start(Default::default())
        .await
        .unwrap();
    let call = FunctionCall {
        function_name: "slow".into(),
        tool_call_id: "stdio-closed-call".into(),
        arguments: json!({}),
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::FunctionCallsStarted(vec![call.clone()])),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while !marker.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("native fixture received tools/call");
    client.close().await.unwrap();
    gone(pid).await;
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
        panic!("stdio cancellation must remain a native cancellation");
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
    assert_eq!(
        outcome.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(!outcome.run_llm());
    client.close().await.unwrap();
    std::fs::remove_dir_all(directory).unwrap();
}
