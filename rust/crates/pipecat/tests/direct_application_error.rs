//! Actual application-tool error routing on loopback, not provider qualification.
use pipecat::{
    core::{self, *},
    pipeline::DirectToolBinding,
    services::{ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider},
    turns::LLMContextAggregatorPair,
};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
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
const FAILURE: &str = "application handler fixture failure";

async fn server() -> (String, oneshot::Sender<()>, JoinHandle<Vec<Value>>) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/completion", listener.local_addr().unwrap());
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        timeout(WAIT, async {
            let mut requests = vec![];
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
                let length: usize = String::from_utf8_lossy(&bytes[..end]).lines().find_map(|line| {
                    line.to_ascii_lowercase().strip_prefix("content-length:")
                        .map(|value| value.trim().parse().unwrap())
                }).unwrap();
                assert!(length <= 64 * 1024);
                while bytes.len() < end + length {
                    let n = socket.read(&mut chunk).await.unwrap();
                    assert!(n > 0);
                    bytes.extend_from_slice(&chunk[..n]);
                }
                requests.push(serde_json::from_slice(&bytes[end..end + length]).unwrap());
                assert!(requests.len() <= 2, "unexpected application-error replay");
                let packet = if requests.len() == 1 {
                    json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"application-error-call","type":"function","function":{"name":"explode","arguments":"{\"value\":\"native\"}"}}]},"finish_reason":"tool_calls"}]})
                } else {
                    json!({"choices":[{"index":0,"delta":{"content":"application failure explained"},"finish_reason":"stop"}]})
                };
                let body = format!("data: {packet}\n\ndata: [DONE]\n\n");
                let response = format!("HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                socket.write_all(response.as_bytes()).await.unwrap();
                socket.shutdown().await.unwrap();
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
        "direct-application-upstream-diagnostics"
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
        "direct-application-terminal-witness"
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

#[tokio::test]
async fn direct_application_failure_reports_upstream_and_keeps_its_owned_tool_continuation() {
    let (url, stop, peer) = server().await;
    let called = Arc::new(AtomicUsize::new(0));
    let count = called.clone();
    let function = DirectFunction {
        schema: core::ToolDefinition {
            name: "explode".into(),
            description: "Fail in the application".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        },
        handler: LocalToolFunction::new(move |arguments| {
            let count = count.clone();
            async move {
                assert_eq!(arguments, json!({"value":"native"}));
                count.fetch_add(1, Ordering::SeqCst);
                Err(ProcessorError::new(FAILURE))
            }
        }),
    };
    let advertised = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(function)],
        custom_tools: None,
    });
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![ChatMessage::new(
                ChatRole::User,
                "Run the application tool and explain its result",
            )
            .into()],
            tools: advertised.clone(),
            tool_choice: None,
        },
        Default::default(),
        Default::default(),
    );
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = url;
    config.api_key = None;
    config.request_timeout = WAIT;
    config.idle_timeout = WAIT;
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    ));
    let health = llm.health().unwrap();
    assert!(health.is_usable());
    let binding = DirectToolBinding::new(llm).unwrap();
    let outcomes = binding.outcomes();
    let diagnostics = Arc::new(Mutex::new(vec![]));
    let terminal = Arc::new(Mutex::new(vec![]));
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![
        pair.user(),
        Arc::new(UpstreamDiagnostics(diagnostics.clone())),
    ];
    stages.extend(binding.stages());
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
            let frame = worker.recv().await.unwrap();
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant && message.content == MessageContent::Text("application failure explained".into()))) { break; }
            if let FrameData::Error(error) = &frame.frame.data {
                assert!(error.error.contains(FAILURE), "unexpected pipeline error: {error:?}");
                assert!(!error.fatal);
            }
        }
    }).await.unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "the actual tool error must reach a healthy native continuation"
    );
    assert_eq!(called.load(Ordering::SeqCst), 1);
    assert!(
        health.is_usable(),
        "an application error must not invalidate the LLM"
    );
    let context = pair.context().snapshot_rich().unwrap();
    assert_eq!(context.tools, advertised);
    let results: Vec<Value> = context
        .messages
        .iter()
        .filter_map(|message| {
            if message.role != ChatRole::Tool
                || message.tool_call_id.as_deref() != Some("application-error-call")
            {
                return None;
            }
            let MessageContent::Text(text) = &message.content else {
                return None;
            };
            serde_json::from_str(text).ok()
        })
        .collect();
    // This finite slice preserves the existing Rust model-context error string.
    // Python's separate generic-failure context policy is not claimed here.
    assert_eq!(results, vec![json!({"error":FAILURE})]);
    assert!(requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["role"] == "tool"
            && message["tool_call_id"] == "application-error-call"
            && message["content"]
                .as_str()
                .and_then(|text| serde_json::from_str::<Value>(text).ok())
                == Some(json!({"error":FAILURE}))));
    let frames = terminal.lock().unwrap();
    assert_eq!(frames.len(), 1);
    let receipt = frames[0].native_tool_outcome().unwrap().unwrap();
    assert_eq!(receipt.state(), ToolCallTerminalState::Failed);
    assert_eq!(receipt.reason(), ToolCallEndReason::CallbackError);
    assert_eq!(receipt.call().tool_call_id, "application-error-call");
    assert_eq!(receipt.call().arguments, json!({"value":"native"}));
    let FrameData::FunctionCallResult(result) = &frames[0].data else {
        panic!("a failed tool result is required");
    };
    assert!(result.is_final);
    assert_eq!(result.error.as_deref(), Some(FAILURE));
    assert_eq!(result.run_llm, None);
    let settled = outcomes.snapshot();
    assert!(settled.active.is_empty());
    assert_eq!(settled.total_terminal, 1);
    assert_eq!(settled.terminal.len(), 1);
    let upstream = diagnostics.lock().unwrap();
    assert!(
        upstream
            .iter()
            .all(|error| error.error.contains(FAILURE) && !error.fatal),
        "{upstream:?}"
    );
    assert!(
        report
            .errors
            .iter()
            .all(|error| error.error.contains(FAILURE) && !error.fatal),
        "{:?}",
        report.errors
    );
    // All real-I/O, owned-settlement, history and normal-join checks precede the
    // old-routing RED: it currently publishes the result but no ErrorFrame.
    assert_eq!(upstream.iter().filter(|error| error.category == ErrorCategory::Application).count(), 1,
        "the application handler failure lost its upstream Application diagnostic; actual_upstream={upstream:?}");
}
