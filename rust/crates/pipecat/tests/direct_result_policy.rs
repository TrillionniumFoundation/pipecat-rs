//! Real native HTTP verifies a Direct handler's final continuation policy.
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
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Semaphore},
    task::JoinHandle,
    time::timeout,
};

const WAIT: Duration = Duration::from_secs(10);

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
        assert!(bytes.len() <= 64 * 1024);
        if let Some(end) = bytes.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break end + 4;
        }
    };
    let length: usize = String::from_utf8_lossy(&bytes[..end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse().unwrap())
        })
        .unwrap();
    assert!(length <= 64 * 1024);
    while bytes.len() < end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[end..end + length]).unwrap()
}

async fn server(tool_calls: Value) -> (String, oneshot::Sender<()>, JoinHandle<Vec<Value>>) {
    let listener = timeout(WAIT, TcpListener::bind("127.0.0.1:0"))
        .await
        .unwrap()
        .unwrap();
    let url = format!("http://{}/completion", listener.local_addr().unwrap());
    let (stop, mut stopped) = oneshot::channel();
    let task = tokio::spawn(async move {
        timeout(WAIT, async move {
            let mut requests = vec![];
            loop {
                let (mut socket, _) = tokio::select! {
                    _ = &mut stopped => break,
                    accepted = listener.accept() => accepted.unwrap(),
                };
                requests.push(read_request(&mut socket).await);
                assert!(requests.len() <= 2, "unexpected repeated continuation");
                let packet = if requests.len() == 1 {
                    json!({"choices":[{"index":0,"delta":{"tool_calls":tool_calls.clone()},"finish_reason":"tool_calls"}]})
                } else {
                    json!({"choices":[{"index":0,"delta":{"content":"unexpected automatic continuation"},"finish_reason":"stop"}]})
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

struct ResultTap(mpsc::Sender<(FunctionCallResultFrame, bool)>);
#[async_trait]
impl FrameProcessor for ResultTap {
    fn name(&self) -> &str {
        "direct-result-policy-witness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let result = match &frame.data {
            FrameData::FunctionCallResult(result) => {
                Some((result.clone(), frame.native_tool_outcome()?.is_some()))
            }
            _ => None,
        };
        ctx.push(frame, direction).await?;
        if let Some(result) = result {
            self.0
                .try_send(result)
                .map_err(|error| ProcessorError::new(error.to_string()))?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn direct_final_false_is_published_and_suppresses_automatic_native_continuation() {
    let (url, stop, peer) = server(json!([{"index":0,"id":"policy-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"native\"}"}}])).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = calls.clone();
    let function = DirectFunction {
        schema: core::ToolDefinition {
            name: "echo".into(),
            description: "Echo native value".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        },
        handler: LocalToolFunction::new_with_result(move |arguments| {
            let entered = entered.clone();
            async move {
                entered.fetch_add(1, Ordering::SeqCst);
                assert_eq!(arguments, json!({"value":"native"}));
                Ok(
                    LocalToolResult::new(json!({"value":"native","source":"application"}))
                        .with_run_llm(false),
                )
            }
        }),
    };
    let advertised = ToolSet::Schema(ToolsSchema {
        standard_tools: vec![StandardTool::Direct(function.clone())],
        custom_tools: None,
    });
    let pair = LLMContextAggregatorPair::new_rich(
        LlmContextData {
            messages: vec![
                ChatMessage::new(ChatRole::User, "Invoke echo without continuing").into(),
            ],
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
    let binding = DirectToolBinding::new(Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    )))
    .unwrap();
    let (recorded, mut results) = mpsc::channel(1);
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(ResultTap(recorded)));
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
    let (published, owned) = timeout(WAIT, results.recv()).await.unwrap().unwrap();
    timeout(WAIT, async {
        loop {
            let context = pair.context().snapshot_rich().unwrap();
            if context.messages.iter().any(|message| message.role == ChatRole::Tool
                && message.tool_call_id.as_deref() == Some("policy-call")
                && matches!(&message.content, MessageContent::Text(text)
                    if serde_json::from_str::<Value>(text).ok() == Some(published.result.clone()))) {
                break;
            }
            tokio::task::yield_now().await;
        }
        // Finish the known old route normally, rather than treating a missing
        // second response or a deadline as the functional failure witness.
        if published.run_llm.is_none() {
            loop {
                let frame = worker.recv().await.expect("old automatic continuation must finish");
                if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                    if context.context.messages.last().is_some_and(|message|
                        message.role == ChatRole::Assistant
                            && message.content == MessageContent::Text("unexpected automatic continuation".into()))) {
                    break;
                }
                if let FrameData::Error(error) = frame.frame.data { panic!("unexpected native error: {error:?}"); }
            }
        }
    }).await.unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(owned, "result must have the native settlement receipt");
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(published.tool_call_id, "policy-call");
    assert_eq!(
        published.result,
        json!({"value":"native","source":"application"})
    );
    assert!(published.error.is_none() && published.is_final);
    assert_eq!(pair.context().snapshot_rich().unwrap().tools, advertised);
    assert_eq!(requests[0]["tools"][0]["function"]["name"], "echo");
    assert_eq!(
        (requests.len(), published.run_llm),
        (1, Some(false)),
        "explicit false must survive owned publication and suppress native continuation",
    );
}

#[tokio::test]
async fn direct_final_true_continues_while_a_sibling_is_held_and_false_sibling_adds_no_run() {
    let (url, stop, peer) = server(json!([
        {"index":0,"id":"policy-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"native\"}"}},
        {"index":1,"id":"sibling-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"sibling\"}"}}
    ])).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let count = calls.clone();
    let started = entered.clone();
    let gate = release.clone();
    let function = DirectFunction {
        schema: core::ToolDefinition {
            name: "echo".into(),
            description: "Echo native value".into(),
            parameters: json!({"type":"object","properties":{"value":{"type":"string"}},"required":["value"]}),
        },
        handler: LocalToolFunction::new_with_result(move |arguments| {
            let count = count.clone();
            let started = started.clone();
            let gate = gate.clone();
            async move {
                count.fetch_add(1, Ordering::SeqCst);
                let run_llm = arguments["value"] != "sibling";
                if !run_llm {
                    started.add_permits(1);
                    gate.acquire().await.unwrap().forget();
                }
                Ok(
                    LocalToolResult::new(
                        json!({"value":arguments["value"],"source":"application"}),
                    )
                    .with_run_llm(run_llm),
                )
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
                "Invoke echo with an explicit continuation",
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
    let binding = DirectToolBinding::new(Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new("fixture", Context::default()),
    )))
    .unwrap();
    let outcomes = binding.outcomes();
    let (recorded, mut results) = mpsc::channel(2);
    let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![pair.user()];
    stages.extend(binding.stages());
    stages.push(Arc::new(ResultTap(recorded)));
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
    timeout(WAIT, entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let first = timeout(WAIT, results.recv()).await.unwrap().unwrap();
    // The second request is a native loopback contract, including its pending
    // placeholder; it does not establish acceptance by a hosted provider.
    timeout(WAIT, async {
        loop {
            let frame = worker.recv().await.expect("explicit true must reach the second native response");
            if matches!(&frame.frame.data, FrameData::LlmContextDetailed(context)
                if context.context.messages.last().is_some_and(|message|
                    message.role == ChatRole::Assistant
                        && message.content == MessageContent::Text("unexpected automatic continuation".into()))) {
                break;
            }
            if let FrameData::Error(error) = frame.frame.data { panic!("unexpected native error: {error:?}"); }
        }
    }).await.unwrap();
    let held = outcomes.snapshot();
    release.add_permits(1);
    let second = timeout(WAIT, results.recv()).await.unwrap().unwrap();
    timeout(WAIT, async {
        loop {
            let context = pair.context().snapshot_rich().unwrap();
            if context.messages.iter().any(|message| {
                message.role == ChatRole::Tool
                    && message.tool_call_id.as_deref() == Some("sibling-call")
                    && matches!(&message.content, MessageContent::Text(text)
                    if serde_json::from_str::<Value>(text).ok() == Some(second.0.result.clone()))
            }) {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    timeout(WAIT, worker.end()).await.unwrap().unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    stop.send(()).unwrap();
    let requests = timeout(WAIT, peer).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(
        first.1 && second.1,
        "both results must have native settlement receipts"
    );
    assert_eq!(
        (first.0.tool_call_id.as_str(), first.0.run_llm),
        ("policy-call", Some(true))
    );
    assert_eq!(
        (second.0.tool_call_id.as_str(), second.0.run_llm),
        ("sibling-call", Some(false))
    );
    assert_eq!(held.active.len(), 1);
    assert_eq!(held.active[0].tool_call_id, "sibling-call");
    assert_eq!(held.total_terminal, 1);
    assert_eq!(
        requests.len(),
        2,
        "false sibling must add no third native request"
    );
    let tool_messages: Vec<_> = requests[1]["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|message| message["role"] == "tool")
        .collect();
    assert_eq!(tool_messages.len(), 2);
    assert!(tool_messages
        .iter()
        .any(|message| message["tool_call_id"] == "policy-call"
            && serde_json::from_str::<Value>(message["content"].as_str().unwrap()).unwrap()
                == first.0.result));
    assert!(tool_messages
        .iter()
        .any(|message| message["tool_call_id"] == "sibling-call"
            && message["content"] == "IN_PROGRESS"));
    assert_eq!(pair.context().snapshot_rich().unwrap().tools, advertised);
    let final_outcomes = outcomes.snapshot();
    assert!(final_outcomes.active.is_empty());
    assert_eq!(final_outcomes.total_terminal, 2);
}
