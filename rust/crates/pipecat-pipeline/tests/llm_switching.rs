use pipecat_core::{self as core, *};
use pipecat_pipeline::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, InferenceOptions, LlmClient, LlmProcessor, Message,
    Provider, Role,
};
use serde_json::{json, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

struct Request {
    body: Value,
    respond: oneshot::Sender<(u16, String)>,
}
struct Server {
    address: String,
    requests: mpsc::Receiver<Request>,
    task: JoinHandle<()>,
    count: Arc<AtomicUsize>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Server {
    async fn new() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = format!("http://{}/stream", listener.local_addr().unwrap());
        let (send, requests) = mpsc::channel(8);
        let count = Arc::new(AtomicUsize::new(0));
        let calls = count.clone();
        let task = tokio::spawn(async move {
            loop {
                let Ok((mut socket, _)) = listener.accept().await else {
                    return;
                };
                let Some(body) = read(&mut socket).await else {
                    return;
                };
                calls.fetch_add(1, Ordering::SeqCst);
                let (respond, wait) = oneshot::channel();
                if send.send(Request { body, respond }).await.is_err() {
                    return;
                }
                if let Ok((status, body)) = wait.await {
                    let kind = if status == 200 {
                        "text/event-stream"
                    } else {
                        "application/json"
                    };
                    let _=socket.write_all(format!("HTTP/1.1 {status} Response\r\nContent-Type: {kind}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await;
                }
            }
        });
        Self {
            address,
            requests,
            task,
            count,
        }
    }
    async fn next(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(3), self.requests.recv())
            .await
            .unwrap()
            .unwrap()
    }
    fn llm(&self, name: &str) -> Arc<LlmProcessor> {
        let mut config = ClientConfig::new(Provider::OpenAi, "");
        config.api_key = None;
        config.endpoint = self.address.clone();
        config.request_timeout = Duration::from_secs(5);
        Arc::new(
            LlmProcessor::new(
                LlmClient::new(config).unwrap(),
                CompletionRequest::new(
                    name,
                    Context::new(vec![Message::text(Role::User, "initial")]),
                ),
            )
            .with_name(name),
        )
    }
}
async fn read(socket: &mut TcpStream) -> Option<Value> {
    let mut bytes = vec![];
    let mut buf = [0; 4096];
    let header_end = loop {
        let n = socket.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        bytes.extend_from_slice(&buf[..n]);
        if let Some(i) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break i + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let length: usize = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|l| l.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < header_end + length {
        let n = socket.read(&mut buf).await.ok()?;
        if n == 0 {
            return None;
        }
        bytes.extend_from_slice(&buf[..n]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).ok()
}
fn text_response(text: &str) -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})
    )
}
fn tool_response() -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"same-call","type":"function","function":{"name":"write","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})
    )
}
fn context(text: &str) -> core::LlmContextFrame {
    core::LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, text)],
        ..Default::default()
    }
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn next(worker: &mut PipelineWorker, pred: impl Fn(&DirectedFrame) -> bool) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let out = worker.recv().await.unwrap();
            if pred(&out) {
                return out;
            }
        }
    })
    .await
    .unwrap()
}
async fn metadata(worker: &mut PipelineWorker, name: &str) {
    next(worker,|out|matches!(&out.frame.data,FrameData::LlmServiceMetadata(m) if m.base.service_name==name)).await;
}
async fn finish(worker: PipelineWorker) -> WorkerReport {
    worker.sender().end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
        .1
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn actual_http_failover_refreshes_metadata_but_never_replays_failed_context() {
    let mut primary = Server::new().await;
    let backup = Server::new().await;
    let a = primary.llm("primary");
    let b = backup.llm("backup");
    let switcher = Arc::new(
        LlmSwitcher::with_strategy(
            vec![a.clone(), b.clone()],
            ServiceSwitcherStrategy::Failover,
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    metadata(&mut worker, "primary").await;
    send(
        &worker,
        Frame::new(FrameData::LlmContext(context("one attempt"))),
    )
    .await;
    let request = primary.next().await;
    assert_eq!(request.body["messages"][0]["content"], "one attempt");
    request
        .respond
        .send((401, json!({"error":{"message":"invalid"}}).to_string()))
        .unwrap();
    metadata(&mut worker, "backup").await;
    assert_eq!(switcher.service_switcher().active_index(), 1);
    let report = finish(worker).await;
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
    assert_eq!(primary.count.load(Ordering::SeqCst), 1);
    assert_eq!(backup.count.load(Ordering::SeqCst), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inactive_llm_settings_and_tools_are_ready_before_explicit_post_switch_context() {
    let primary = Server::new().await;
    let mut backup = Server::new().await;
    let a = primary.llm("primary");
    let b = backup.llm("backup");
    let switcher = Arc::new(LlmSwitcher::new(vec![a.clone(), b.clone()]).unwrap());
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut delta = LlmSettings::default();
    delta.base.model = Setting::Value("configured-backup".into());
    send(
        &worker,
        Frame::new(FrameData::LlmUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                delta: Some(delta),
                service: Some(ProcessorReference::Local(LocalProcessorReference(
                    b.clone(),
                ))),
                ..Default::default()
            },
        ))),
    )
    .await;
    let tools = vec![core::ToolDefinition {
        name: "write".into(),
        description: "write".into(),
        parameters: json!({"type":"object"}),
    }];
    send(
        &worker,
        Frame::new(FrameData::LlmSetTools(ToolSet::Functions(tools.clone()))),
    )
    .await;
    send(
        &worker,
        switcher.service_switcher().switch_frame(1).unwrap(),
    )
    .await;
    metadata(&mut worker, "backup").await;
    assert_eq!(
        b.request_snapshot().unwrap().settings.base.model,
        Setting::Value("configured-backup".into())
    );
    assert_eq!(
        b.request_snapshot().unwrap().context.tools.unwrap()[0].name,
        "write"
    );
    let mut ctx = context("explicit fresh context");
    ctx.tools = tools;
    send(&worker, Frame::new(FrameData::LlmContext(ctx))).await;
    let request = backup.next().await;
    assert_eq!(request.body["model"], "configured-backup");
    assert_eq!(request.body["tools"][0]["function"]["name"], "write");
    request
        .respond
        .send((200, text_response("backup result")))
        .unwrap();
    next(
        &mut worker,
        |out| matches!(&out.frame.data,FrameData::LlmText(t) if t.text=="backup result"),
    )
    .await;
    assert!(finish(worker).await.errors.is_empty());
    assert_eq!(primary.count.load(Ordering::SeqCst), 0);
    assert_eq!(backup.count.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn shared_tool_executor_retains_callback_options_and_deduplicates_across_llms() {
    let mut primary = Server::new().await;
    let mut backup = Server::new().await;
    let switcher =
        Arc::new(LlmSwitcher::new(vec![primary.llm("primary"), backup.llm("backup")]).unwrap());
    let count = Arc::new(AtomicUsize::new(0));
    let calls = count.clone();
    let mut executor = ToolExecutor::new(2, 32, 1024).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(move |_| {
        let calls = calls.clone();
        Box::pin(async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(json!({"done":true}))
        })
    }));
    tool.cancel_on_interruption = false;
    tool.timeout = Duration::from_secs(1);
    executor.register("write", tool).unwrap();
    let mut worker = Pipeline::new(vec![switcher.clone(), Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::new(FrameData::LlmContext(context("first")))).await;
    primary
        .next()
        .await
        .respond
        .send((200, tool_response()))
        .unwrap();
    let result = next(&mut worker, |out| {
        matches!(out.frame.data, FrameData::FunctionCallResult(_))
    })
    .await;
    assert!(matches!(result.frame.data,FrameData::FunctionCallResult(r) if r.result["done"]==true));
    send(
        &worker,
        switcher.service_switcher().switch_frame(1).unwrap(),
    )
    .await;
    metadata(&mut worker, "backup").await;
    send(
        &worker,
        Frame::new(FrameData::LlmContext(context("second explicit request"))),
    )
    .await;
    backup
        .next()
        .await
        .respond
        .send((200, tool_response()))
        .unwrap();
    next(&mut worker, |out| {
        matches!(out.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    assert!(finish(worker).await.errors.is_empty());
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(primary.count.load(Ordering::SeqCst), 1);
    assert_eq!(backup.count.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inference_keeps_selected_member_and_settings_snapshot_without_mutating_context() {
    let mut primary = Server::new().await;
    let mut backup = Server::new().await;
    let a = primary.llm("primary");
    let b = backup.llm("backup");
    let switcher = Arc::new(LlmSwitcher::new(vec![a.clone(), b.clone()]).unwrap());
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let selected = switcher.clone();
    let mut options = InferenceOptions::default();
    options.settings.base.model = Setting::Value("one-shot-model".into());
    let first = tokio::spawn(async move {
        selected
            .run_inference(context("out of band").into(), options)
            .await
    });
    let request = primary.next().await;
    assert_eq!(request.body["model"], "one-shot-model");
    assert_eq!(request.body["messages"][0]["content"], "out of band");
    send(
        &worker,
        switcher.service_switcher().switch_frame(1).unwrap(),
    )
    .await;
    metadata(&mut worker, "backup").await;
    request.respond.send((200, text_response("first"))).unwrap();
    assert_eq!(first.await.unwrap().unwrap(), Some("first".into()));
    let selected = switcher.clone();
    let second = tokio::spawn(async move {
        selected
            .run_inference(context("new selection").into(), InferenceOptions::default())
            .await
    });
    let request = backup.next().await;
    assert_eq!(request.body["model"], "backup");
    request
        .respond
        .send((200, text_response("second")))
        .unwrap();
    assert_eq!(second.await.unwrap().unwrap(), Some("second".into()));
    assert_eq!(a.request_snapshot().unwrap().model, "primary");
    assert_eq!(
        a.request_snapshot().unwrap().context.messages[0].content,
        Some(pipecat_services::Content::Text("initial".into()))
    );
    assert!(finish(worker).await.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inference_bounds_accumulation_and_does_not_execute_tool_calls() {
    let mut server = Server::new().await;
    let switcher = Arc::new(LlmSwitcher::new(vec![server.llm("a")]).unwrap());
    let selected = switcher.clone();
    let inference = tokio::spawn(async move {
        selected
            .run_inference(context("tools").into(), InferenceOptions::default())
            .await
    });
    server
        .next()
        .await
        .respond
        .send((200, tool_response()))
        .unwrap();
    assert!(inference.await.unwrap().unwrap().is_none());
    let selected = switcher.clone();
    let inference = tokio::spawn(async move {
        selected
            .run_inference(
                context("bounded").into(),
                InferenceOptions {
                    max_output_bytes: 4,
                    ..Default::default()
                },
            )
            .await
    });
    server
        .next()
        .await
        .respond
        .send((200, text_response("too long")))
        .unwrap();
    assert!(inference.await.unwrap().is_err());
    assert!(switcher
        .run_inference(
            context("invalid").into(),
            InferenceOptions {
                max_output_bytes: 0,
                ..Default::default()
            }
        )
        .await
        .is_err());
    assert_eq!(server.count.load(Ordering::SeqCst), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancelling_out_of_band_inference_closes_its_http_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ClientConfig::new(Provider::OpenAi, "");
    config.api_key = None;
    config.endpoint = format!("http://{}/completion", listener.local_addr().unwrap());
    let llm = Arc::new(LlmProcessor::new(
        LlmClient::new(config).unwrap(),
        CompletionRequest::new(
            "model",
            Context::new(vec![Message::text(Role::User, "initial")]),
        ),
    ));
    let switcher = Arc::new(LlmSwitcher::new(vec![llm]).unwrap());
    let (admitted, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read(&mut socket).await.unwrap();
        admitted.send(()).unwrap();
        let mut bytes = [0; 16];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(3), socket.read(&mut bytes))
                .await
                .unwrap()
                .unwrap(),
            0
        );
    });
    let inference = tokio::spawn(async move {
        switcher
            .run_inference(context("cancel me").into(), InferenceOptions::default())
            .await
    });
    tokio::time::timeout(Duration::from_secs(3), wait)
        .await
        .unwrap()
        .unwrap();
    inference.abort();
    assert!(inference.await.unwrap_err().is_cancelled());
    server.await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn rejected_llm_settings_keep_last_good_configuration_and_do_not_fail_over() {
    let primary = Server::new().await;
    let backup = Server::new().await;
    let a = primary.llm("primary");
    let switcher = Arc::new(
        LlmSwitcher::with_strategy(
            vec![a.clone(), backup.llm("backup")],
            ServiceSwitcherStrategy::Failover,
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut delta = LlmSettings::default();
    delta.base.model = Setting::Null;
    send(
        &worker,
        Frame::new(FrameData::LlmUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                delta: Some(delta),
                ..Default::default()
            },
        ))),
    )
    .await;
    let error = next(&mut worker, |out| out.frame.data.as_error_frame().is_some()).await;
    assert_eq!(
        error.frame.data.as_error_frame().unwrap().category,
        ErrorCategory::InvalidRequest
    );
    assert!(a.health().unwrap().is_usable());
    assert_eq!(a.request_snapshot().unwrap().model, "primary");
    assert_eq!(switcher.service_switcher().active_index(), 0);
    finish(worker).await;
    assert_eq!(primary.count.load(Ordering::SeqCst), 0);
    assert_eq!(backup.count.load(Ordering::SeqCst), 0);
}
