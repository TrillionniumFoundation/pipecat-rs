//! Real localhost HTTP → pipeline aggregation → existing native job RPC.
use pipecat_agent_workers::AssistantJobReply;
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Content, Context, LlmClient, LlmProcessor, Provider,
    ToolDefinition,
};
use pipecat_turns::*;
use pipecat_workers::{
    JobClient, JobParams, JobResponse, JobStatus, LocalBus, NativeWorker, WorkerRegistry,
};
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::mpsc,
    task::JoinSet,
};
const LIMIT: Duration = Duration::from_secs(5);

fn text_reply(text: &str) -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":{"content":text}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
    )
}
fn tool_reply() -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":{"tool_calls":[{"index":0,"id":"sensor-read","type":"function","function":{"name":"read_sensor","arguments":"{}"}}]}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]})
    )
}
async fn provider(replies: Vec<String>) -> (LlmClient, mpsc::Receiver<Value>, JoinSet<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (send, receive) = mpsc::channel(replies.len());
    let mut tasks = JoinSet::new();
    tasks.spawn(async move {
        for reply in replies {
            let (mut socket, _) = tokio::time::timeout(LIMIT, listener.accept()).await.unwrap().unwrap();
            let mut bytes = Vec::new();
            let boundary = loop {
                let mut buffer = [0; 1024];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0 && bytes.len() < 1024 * 1024);
                bytes.extend_from_slice(&buffer[..count]);
                if let Some(offset) = bytes.windows(4).position(|part| part == b"\r\n\r\n") { break offset + 4; }
            };
            let length = String::from_utf8_lossy(&bytes[..boundary]).lines().find_map(|line| line.to_ascii_lowercase().strip_prefix("content-length:").and_then(|length| length.trim().parse::<usize>().ok())).unwrap();
            assert!(length < 1024 * 1024);
            while bytes.len() < boundary + length {
                let mut buffer = [0; 4096];
                let count = socket.read(&mut buffer).await.unwrap();
                assert!(count > 0);
                bytes.extend_from_slice(&buffer[..count]);
            }
            send.send(serde_json::from_slice(&bytes[boundary..boundary+length]).unwrap()).await.unwrap();
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", reply.len(), reply).as_bytes()).await.unwrap();
        }
    });
    let mut config = ClientConfig::new(Provider::OpenAi, "local-assistant-job");
    config.endpoint = format!("http://{address}/v1/chat/completions");
    config.api_key = None;
    (LlmClient::new(config).unwrap(), receive, tasks)
}

async fn native_job(sensor: bool) {
    let replies = if sensor {
        vec![tool_reply(), text_reply("Temperature is 21 C")]
    } else {
        vec![text_reply("Reusable launchers lower material cost")]
    };
    let (client, mut requests, mut provider_tasks) = provider(replies).await;
    let completions = Arc::new(Mutex::new(Vec::<Arc<AssistantTurnStopped>>::new()));
    let records = completions.clone();
    let bus = LocalBus::new(32).unwrap();
    let registry = WorkerRegistry::new("assistant-completion-test");
    let worker = NativeWorker::new("native-assistant").active(true).max_jobs(1)
        .on_job("respond", true, move |job| {
            let client = client.clone(); let records = records.clone();
            async move {
                let run = async {
                    let mut context = Context::default();
                    if sensor { context.tools = Some(vec![ToolDefinition { name: "read_sensor".into(), description: "Read the temperature".into(), parameters: json!({"type":"object","properties":{}}) }]); }
                    let pair = LLMContextAggregatorPair::new(context, Default::default(), Default::default());
                    let assistant = pair.assistant();
                    let shared = pair.context();
                    let mut receiver = assistant.subscribe_stopped(Default::default())?;
                    let mut response = AssistantJobReply::capture(&job, &assistant, 7)?;
                    let mut executor = ToolExecutor::new(1, 16, 4096)?;
                    executor.register("read_sensor", RegisteredTool::new(Arc::new(|_| Box::pin(async { Ok(json!({"temperature":21})) }))))?;
                    let mut pipeline = Pipeline::new(vec![pair.user(), Arc::new(LlmProcessor::new(client, CompletionRequest::new("local", Context::default()))), Arc::new(executor), assistant])
                        .start(Default::default()).await?;
                    pipeline.sender().send(Frame::new(FrameData::InputText(TextFrame::new("Explain the result"))), Direction::Downstream).await?;
                    let result = loop {
                        tokio::select! {
                            record = receiver.recv() => {
                                let record = record.ok_or("completion stream closed")?;
                                let completion = record.completion();
                                assert_eq!(completion.job_generation(), Some(7));
                                if !completion.text().is_empty() {
                                    assert!(shared.snapshot()?.messages.iter().any(|message| message.content == Some(Content::Text(completion.text().into()))), "context is committed before receipt consumption");
                                }
                                records.lock().unwrap().push(completion.clone());
                                if let Some(result) = response.try_response(&job, completion)? {
                                    assert!(response.try_response(&job, completion)?.is_none(), "one binding answers only once");
                                    break result;
                                }
                            }
                            output = pipeline.recv() => {
                                let output = output.ok_or("pipeline closed")?;
                                if let FrameData::Error(error) = output.frame.data { return Err(error.error.into()); }
                            }
                        }
                    };
                    pipeline.end().await?;
                    let (_, report) = pipeline.collect().await?;
                    assert!(report.errors.is_empty(), "{:?}", report.errors);
                    assert!(receiver.recv().await.is_none());
                    assert_eq!(receiver.stats().retained_records, 0);
                    Ok::<_, Box<dyn std::error::Error + Send + Sync>>(result)
                };
                match tokio::time::timeout(LIMIT, run).await {
                    Ok(Ok(response)) => response,
                    Ok(Err(error)) => JobResponse::error(error.to_string()),
                    Err(_) => JobResponse::error("native pipeline timed out"),
                }
            }
        }).unwrap().start(bus.clone(), registry.clone()).unwrap();
    let mut job = JobClient::new("caller", bus, registry)
        .request(
            "native-assistant",
            JobParams {
                name: Some("respond".into()),
                timeout: Some(Duration::from_secs(7)),
                ..Default::default()
            },
        )
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(8), job.result())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.status, JobStatus::Completed, "{:?}", result);
    assert_eq!(
        result.response,
        Some(json!(if sensor {
            "Temperature is 21 C"
        } else {
            "Reusable launchers lower material cost"
        }))
    );
    let first = requests.recv().await.unwrap();
    assert!(first["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|message| message["content"] == "Explain the result"));
    if sensor {
        let next = requests.recv().await.unwrap();
        assert!(next["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|message| message["role"] == "tool"
                && message["content"]
                    .as_str()
                    .is_some_and(|text| text.contains("21"))));
    }
    {
        let records = completions.lock().unwrap();
        assert_eq!(records.len(), if sensor { 2 } else { 1 });
        if sensor {
            assert!(records[0].text().is_empty());
            assert!(!records[0].interrupted());
            assert_ne!(records[0].turn_id(), records[1].turn_id());
        }
    }
    worker.end(None).await.unwrap();
    tokio::time::timeout(LIMIT, worker.wait())
        .await
        .unwrap()
        .unwrap();
    while let Some(result) = provider_tasks.join_next().await {
        result.unwrap();
    }
}
#[tokio::test]
async fn debate_job_responds_from_real_http_assistant_commit() {
    native_job(false).await;
}
#[tokio::test]
async fn sensor_job_skips_tool_only_round_then_returns_real_final_response() {
    native_job(true).await;
}

#[tokio::test]
async fn captured_binding_rejects_replacement_job_identity_and_late_generation() {
    let assistant = Arc::new(LLMAssistantAggregator::new(
        SharedContext::default(),
        Default::default(),
    ));
    let receiver = Arc::new(Mutex::new(Some(
        assistant.subscribe_stopped(Default::default()).unwrap(),
    )));
    let previous = Arc::new(Mutex::new(None::<AssistantJobReply>));
    let bus = LocalBus::new(8).unwrap();
    let registry = WorkerRegistry::new("job-identity");
    let worker = NativeWorker::new("owner")
        .active(true)
        .on_job("", true, move |job| {
            let a = assistant.clone();
            let receiver = receiver.clone();
            let previous = previous.clone();
            async move {
                let generation = job.payload.as_ref().and_then(Value::as_u64).unwrap();
                if generation == 2 {
                    let replacement =
                        LLMAssistantAggregator::new(SharedContext::default(), Default::default());
                    let mut replacement_records =
                        replacement.subscribe_stopped(Default::default()).unwrap();
                    let mut binding = AssistantJobReply::capture(&job, &replacement, 1).unwrap();
                    let mut receiver = receiver.lock().unwrap().take().unwrap();
                    let late = receiver.recv().await.unwrap();
                    assert_eq!(late.completion().job_generation(), Some(1));
                    assert!(binding
                        .try_response(&job, late.completion())
                        .unwrap()
                        .is_none());
                    assert!(previous
                        .lock()
                        .unwrap()
                        .as_mut()
                        .unwrap()
                        .try_response(&job, late.completion())
                        .is_err());
                    drop(late);
                    stopped_turn(&replacement, "new answer");
                    let current = replacement_records.recv().await.unwrap();
                    return binding
                        .try_response(&job, current.completion())
                        .unwrap()
                        .unwrap();
                }
                let binding = AssistantJobReply::capture(&job, &a, 1).unwrap();
                stopped_turn(&a, "old answer");
                *previous.lock().unwrap() = Some(binding);
                // The immutable first record remains queued after its real job
                // finishes cancelled; the next real native job has another identity.
                JobResponse::cancelled()
            }
        })
        .unwrap()
        .start(bus.clone(), registry.clone())
        .unwrap();
    let client = JobClient::new("caller", bus, registry);
    for generation in [1, 2] {
        let mut job = client
            .request(
                "owner",
                JobParams {
                    payload: Some(json!(generation)),
                    timeout: Some(LIMIT),
                    ..Default::default()
                },
            )
            .unwrap();
        let response = job.result().await.unwrap();
        if generation == 1 {
            assert_eq!(response.status, JobStatus::Cancelled);
        } else {
            assert_eq!(response.status, JobStatus::Completed);
            assert_eq!(response.response, Some(json!("new answer")));
        }
    }
    worker.end(None).await.unwrap();
    worker.wait().await.unwrap();
}
fn stopped_turn(assistant: &LLMAssistantAggregator, text: &str) {
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm(text)),
        FrameData::LlmResponseEnd,
    ] {
        let effects = assistant
            .process_frame(Frame::new(data), Direction::Downstream)
            .unwrap();
        for action in effects.actions {
            if let AggregationAction::AssistantStopped(action) = action {
                action.admit().unwrap();
            }
        }
    }
}
