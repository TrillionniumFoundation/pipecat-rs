//! Existing MCP factory failure routing over loopback, not live-provider qualification.
#[path = "support/fixture.rs"]
mod fixture;

use pipecat_core::{
    async_trait, ChatMessage, ChatRole, Direction, ErrorCategory, ErrorFrame, Frame, FrameData,
    FrameProcessor, LlmContextData, MessageContent, Pipeline, PipelineConfig, ProcessorContext,
    ProcessorError, ToolCallEffectStatus, ToolCallEndReason, ToolCallTerminalState, ToolSet,
};
use pipecat_mcp::{HttpConfig, McpClient, McpConfig, McpError, ServerConfig};
use pipecat_pipeline::LlmSwitcher;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    collections::BTreeSet,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);
type Requests = Vec<(String, Value)>;

// The ordinary MCP fixture responses are shared with the existing HTTP suite;
// this small owner additionally permits an observed stop and join of each peer.
async fn server(mcp: bool) -> (String, oneshot::Sender<()>, JoinHandle<Requests>) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/mcp", listener.local_addr().unwrap());
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        timeout(WAIT, async {
            let mut requests: Requests = vec![];
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                let mut bytes = vec![];
                let mut chunk = [0; 4096];
                let end = loop {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                    assert!(bytes.len() <= 64 * 1024);
                    if let Some(end) = bytes.windows(4).position(|s| s == b"\r\n\r\n") { break end + 4; }
                };
                let header = String::from_utf8_lossy(&bytes[..end]);
                let method = header.split_whitespace().next().unwrap().to_string();
                let length: usize = header.lines().find_map(|line| {
                    line.to_ascii_lowercase().strip_prefix("content-length:")
                        .map(|value| value.trim().parse().unwrap())
                }).unwrap_or(0);
                assert!(length <= 64 * 1024);
                while bytes.len() < end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                let message: Value = if length == 0 { Value::Null } else {
                    serde_json::from_slice(&bytes[end..end + length]).unwrap()
                };
                requests.push((method.clone(), message.clone()));
                let (status, content_type, body, session) = if !mcp {
                    assert!(requests.len() <= 2, "unexpected native request replay");
                    let delta = if requests.len() == 1 {
                        json!({"tool_calls":[{"index":0,"id":"agent-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"agent\"}"}}]})
                    } else { json!({"content":"tool completed"}) };
                    let finish = if requests.len() == 1 { "tool_calls" } else { "stop" };
                    let packet = json!({"choices":[{"index":0,"delta":delta,"finish_reason":finish}]});
                    (200, "text/event-stream", format!("data: {packet}\n\ndata: [DONE]\n\n"), "")
                } else if method == "GET" { (405, "application/json", String::new(), "")
                } else if method == "DELETE" { (204, "application/json", String::new(), "")
                } else if message.get("id").is_none() { (202, "application/json", String::new(), "")
                } else {
                    let operation = message["method"].as_str().unwrap();
                    let result = match operation {
                        "initialize" => fixture::initialize(),
                        "tools/list" => fixture::tools(),
                        "tools/call" => fixture::call(message["params"]["name"].as_str().unwrap(), &message["params"]["arguments"]),
                        _ => panic!("unexpected MCP fixture operation: {operation}"),
                    };
                    let body = json!({"jsonrpc":"2.0","id":message["id"],"result":result}).to_string();
                    let session = if operation == "initialize" { "Mcp-Session-Id: native-session\r\n" } else { "" };
                    (200, "application/json", body, session)
                };
                let response = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{session}Connection: close\r\n\r\n{body}", body.len());
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
            requests
        }).await.unwrap()
    });
    (url, stop, task)
}

struct UpstreamDiagnostics(Arc<Mutex<Vec<ErrorFrame>>>);
#[async_trait]
impl FrameProcessor for UpstreamDiagnostics {
    fn name(&self) -> &str {
        "mcp-application-upstream-diagnostics"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Upstream {
            if let FrameData::Error(error) = &frame.data {
                self.0.lock().unwrap().push(error.clone());
            }
        }
        ctx.push(frame, direction).await
    }
}

struct TerminalWitness(Arc<Mutex<Vec<Frame>>>);
#[async_trait]
impl FrameProcessor for TerminalWitness {
    fn name(&self) -> &str {
        "mcp-application-terminal-witness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.native_tool_outcome()?.is_some() {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}

#[derive(Clone, Copy, Debug)]
enum Factory {
    Explicit,
    Fixed,
    Automatic,
    Switcher,
    AutomaticSwitcher,
}

async fn closed_factory(factory: Factory) {
    let (mcp_url, stop_mcp, mcp_peer) = server(true).await;
    let mut config = McpConfig::new(ServerConfig::StreamableHttp(HttpConfig::new(&mcp_url)));
    config.tools_filter = Some(BTreeSet::from(["echo".into()]));
    config.tools_arguments.insert(
        "echo".into(),
        json!({"hidden":"native-fixed"})
            .as_object()
            .unwrap()
            .clone(),
    );
    let client = McpClient::new(config).unwrap();
    let tools = timeout(WAIT, client.tools()).await.unwrap().unwrap();
    assert_eq!(tools.definitions().len(), 1);
    assert_eq!(tools.definitions()[0].name, "echo");
    let schema = tools.schema();
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "invoke cached echo once").into()],
            tools: ToolSet::Schema(schema.clone()),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let (provider_url, stop_provider, provider_peer) = server(false).await;
    let mut config = ClientConfig::new(Provider::OpenAi, "owned-native-fixture");
    config.endpoint = provider_url;
    config.api_key = None;
    config.request_timeout = WAIT;
    config.idle_timeout = WAIT;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let health = llm.health().unwrap();
    assert!(health.is_usable());
    let factory_stages = match factory {
        Factory::Explicit => tools.bind_llm(llm).unwrap().stages(),
        Factory::Fixed => tools.llm_stages(llm).unwrap(),
        Factory::Automatic => tools.bind_llm_auto(llm).unwrap().stages(),
        Factory::Switcher | Factory::AutomaticSwitcher => {
            let switcher = Arc::new(LlmSwitcher::new(vec![llm]).unwrap());
            if matches!(factory, Factory::AutomaticSwitcher) {
                tools.bind_llm_switcher_auto(switcher).unwrap().stages()
            } else {
                tools.bind_llm_switcher(switcher).unwrap().stages()
            }
        }
    };
    // Explicit close revokes SDK execution, while cached schema identity remains
    // valid for this existing factory's projection and admitted native callback.
    timeout(WAIT, client.close()).await.unwrap().unwrap();
    assert_eq!(
        timeout(WAIT, client.call("echo", json!({"value":"closed"})))
            .await
            .unwrap()
            .unwrap_err(),
        McpError::NotConnected
    );
    let diagnostics = Arc::new(Mutex::new(vec![]));
    let terminal = Arc::new(Mutex::new(vec![]));
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![
        pair.user(),
        Arc::new(UpstreamDiagnostics(diagnostics.clone())),
    ];
    stages.extend(factory_stages);
    stages.push(Arc::new(TerminalWitness(terminal.clone())));
    stages.push(pair.assistant());
    let mut worker = timeout(WAIT, Pipeline::new(stages).start(PipelineConfig::default()))
        .await
        .unwrap()
        .unwrap();
    timeout(
        WAIT,
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream),
    )
    .await
    .unwrap()
    .unwrap();
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.expect("native continuation stopped early");
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant && message.content == MessageContent::Text("tool completed".into()))) { break; }
            if let FrameData::Error(error) = &frame.frame.data {
                assert!(!error.fatal, "unexpected fatal error: {error:?}");
            }
        }
    }).await.unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (remaining, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    // Both actual servers exit normally before the finite behavioral RED.
    stop_mcp.send(()).unwrap();
    stop_provider.send(()).unwrap();
    let mcp_requests = timeout(WAIT, mcp_peer).await.unwrap().unwrap();
    let completions = timeout(WAIT, provider_peer).await.unwrap().unwrap();

    assert_eq!(
        completions.len(),
        2,
        "the cached failure must reach a healthy native continuation"
    );
    assert!(
        health.is_usable(),
        "an MCP application failure must not invalidate the LLM"
    );
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|(_, message)| message["method"] == "initialize")
            .count(),
        1
    );
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|(_, message)| message["method"] == "tools/list")
            .count(),
        1
    );
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|(_, message)| message["method"] == "tools/call")
            .count(),
        0,
        "close must prevent actual MCP execution or replay"
    );
    assert_eq!(
        mcp_requests
            .iter()
            .filter(|(method, _)| method == "DELETE")
            .count(),
        1,
        "factory teardown must not repeat explicit close"
    );
    for (_, request) in &completions {
        assert_eq!(request["tools"][0]["function"]["name"], "echo");
        assert_eq!(
            request["tools"][0]["function"]["parameters"],
            tools.definitions()[0].parameters
        );
    }
    let frames = terminal.lock().unwrap();
    assert_eq!(
        frames.len(),
        1,
        "one real callback attempt must settle exactly once"
    );
    let receipt = frames[0].native_tool_outcome().unwrap().unwrap();
    assert_eq!(receipt.state(), ToolCallTerminalState::Failed);
    assert_eq!(receipt.reason(), ToolCallEndReason::CallbackError);
    assert_eq!(
        receipt.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert_eq!(receipt.call().function_name, "echo");
    assert_eq!(receipt.call().tool_call_id, "agent-call");
    assert_eq!(receipt.call().arguments, json!({"value":"agent"}));
    let FrameData::FunctionCallResult(result) = &frames[0].data else {
        panic!("owned failure result required");
    };
    assert!(result.is_final);
    assert_eq!(result.run_llm, None);
    let failure = result
        .error
        .as_ref()
        .expect("the cached SDK failure must remain a typed native error");
    assert!(!failure.is_empty());
    assert_eq!(result.result, json!({"error":failure}));
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(context.tools, ToolSet::Schema(schema));
    let model_results: Vec<Value> = context
        .messages
        .iter()
        .filter_map(|message| {
            if message.role != ChatRole::Tool
                || message.tool_call_id.as_deref() != Some("agent-call")
            {
                return None;
            }
            let MessageContent::Text(text) = &message.content else {
                return None;
            };
            Some(serde_json::from_str(text).unwrap())
        })
        .collect();
    assert_eq!(model_results, vec![json!({"error":failure})]);
    let sent_results: Vec<Value> = completions[1].1["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool" && message["tool_call_id"] == "agent-call")
        .map(|message| serde_json::from_str(message["content"].as_str().unwrap()).unwrap())
        .collect();
    assert_eq!(sent_results, model_results);
    assert!(
        !remaining
            .iter()
            .any(|frame| frame.frame.native_tool_outcome().unwrap().is_some()),
        "a consumed native failure must not be published again during teardown"
    );
    let upstream = diagnostics.lock().unwrap();
    assert!(
        upstream
            .iter()
            .all(|error| !error.fatal && error.error.contains(failure)),
        "{upstream:?}"
    );
    assert!(
        report
            .errors
            .iter()
            .all(|error| !error.fatal && error.error.contains(failure)),
        "{:?}",
        report.errors
    );
    // Existing public APIs, real SDK close, actual provider requests, owned
    // settlement and all normal joins precede this missing-diagnostic RED.
    assert_eq!(upstream.iter().filter(|error| error.category == ErrorCategory::Application).count(), 1,
        "{factory:?} MCP factory lost the upstream Application diagnostic for its admitted SDK failure: {upstream:?}; native_error={failure}");
}

#[tokio::test]
async fn closed_mcp_factory_reports_application_error_and_keeps_native_continuation() {
    closed_factory(Factory::Explicit).await;
}

#[tokio::test]
async fn other_owned_mcp_factories_keep_the_same_application_diagnostic_and_settlement() {
    for factory in [
        Factory::Fixed,
        Factory::Automatic,
        Factory::Switcher,
        Factory::AutomaticSwitcher,
    ] {
        closed_factory(factory).await;
    }
}
