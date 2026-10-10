use pipecat_agent_workers::*;
use pipecat_core::{Direction, Frame, FrameData, TextFrame};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role, ToolDefinition,
};
use pipecat_workers::{JobClient, JobParams, JobStatus, LocalBus, WorkerRegistry};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpListener,
    sync::{mpsc, Notify},
    task::JoinHandle,
    time::timeout,
};

const LIMIT: Duration = Duration::from_secs(5);
struct ProviderFixture {
    client: LlmClient,
    requests: mpsc::Receiver<Value>,
    task: JoinHandle<()>,
}
impl Drop for ProviderFixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn fixture(replies: Vec<String>) -> ProviderFixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (tx, requests) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        for body in replies {
            let (mut socket, _) = listener.accept().await.unwrap();
            let mut bytes = vec![];
            let boundary = loop {
                let mut buf = [0; 1024];
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&buf[..n]);
                if let Some(pos) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
                    break pos + 4;
                }
            };
            let header = String::from_utf8_lossy(&bytes[..boundary]);
            let len = header
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|v| v.trim().parse::<usize>().ok())
                })
                .unwrap();
            while bytes.len() < boundary + len {
                let mut buf = [0; 4096];
                let n = socket.read(&mut buf).await.unwrap();
                if n == 0 {
                    return;
                }
                bytes.extend_from_slice(&buf[..n]);
            }
            let request = serde_json::from_slice(&bytes[boundary..boundary + len]).unwrap();
            tx.send(request).await.unwrap();
            let response=format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",body.len(),body);
            let _ = socket.write_all(response.as_bytes()).await;
        }
    });
    let mut config = ClientConfig::new(Provider::OpenAi, "fixture");
    config.endpoint = format!("http://{addr}/v1/chat/completions");
    config.api_key = None;
    ProviderFixture {
        client: LlmClient::new(config).unwrap(),
        requests,
        task,
    }
}
fn text_reply(text: &str) -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":{"content":text}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"stop"}]})
    )
}
fn tools_reply(calls: Vec<(&str, &str, Value)>) -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"index":0,"delta":{"tool_calls":calls.into_iter().enumerate().map(|(i,(id,name,args))|json!({"index":i,"id":id,"type":"function","function":{"name":name,"arguments":args.to_string()}})).collect::<Vec<_>>()}}]}),
        json!({"choices":[{"index":0,"delta":{},"finish_reason":"tool_calls"}]})
    )
}
fn request() -> CompletionRequest {
    CompletionRequest::new("local", Context::default())
}
fn schema(name: &str) -> ToolDefinition {
    ToolDefinition {
        name: name.into(),
        description: "fixture tool".into(),
        parameters: json!({"type":"object","properties":{}}),
    }
}
async fn until_idle(worker: &mut LlmWorker) -> Vec<Frame> {
    timeout(LIMIT, async {
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
    .unwrap()
}
async fn stop(worker: &mut LlmWorker) {
    worker.handle().end().await.unwrap();
    timeout(LIMIT, worker.join()).await.unwrap().unwrap();
}
fn job(name: &str, payload: Value) -> JobParams {
    JobParams {
        name: Some(name.into()),
        payload: Some(payload),
        timeout: Some(LIMIT),
        ..Default::default()
    }
}
fn options(name: &str) -> LlmWorkerOptions {
    LlmWorkerOptions {
        name: name.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn actual_pipeline_stream_owns_and_accumulates_context() {
    let mut provider = fixture(vec![text_reply("one"), text_reply("two")]).await;
    let mut worker = LlmWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        options("llm"),
    )
    .await
    .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "first")], true)
        .await
        .unwrap();
    let frames = until_idle(&mut worker).await;
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data,FrameData::LlmText(t) if t.text=="one")));
    worker
        .handle()
        .append(vec![Message::text(Role::User, "second")], true)
        .await
        .unwrap();
    until_idle(&mut worker).await;
    let first = provider.requests.recv().await.unwrap();
    let second = provider.requests.recv().await.unwrap();
    assert_eq!(first["messages"].as_array().unwrap().len(), 1);
    assert_eq!(second["messages"][1]["content"], "one");
    assert_eq!(second["messages"][2]["content"], "second");
    assert_eq!(worker.handle().snapshot().context.messages.len(), 4);
    stop(&mut worker).await;
}
#[tokio::test]
async fn typed_tool_dispatch_streams_updates_and_reruns_model() {
    let mut provider = fixture(vec![
        tools_reply(vec![("t1", "lookup", json!({"number":7}))]),
        text_reply("result"),
    ])
    .await;
    let mut tools = ToolRegistry::new();
    #[derive(serde::Deserialize)]
    struct Args {
        number: i64,
    }
    tools
        .register(ToolHandler::typed(
            schema("lookup"),
            |args: Args, ctx| async move {
                ctx.update(json!({"progress":1})).await?;
                Ok(ToolResult::new(json!({"number":args.number+1})))
            },
        ))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("llm"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "do it")], true)
        .await
        .unwrap();
    let frames = until_idle(&mut worker).await;
    assert!(frames.iter().any(|f|matches!(&f.data,FrameData::FunctionCallResult(r) if !r.is_final&&r.result["progress"]==1)));
    assert!(frames.iter().any(
        |f| matches!(&f.data,FrameData::FunctionCallResult(r) if r.is_final&&r.result["number"]==8)
    ));
    timeout(LIMIT, provider.requests.recv())
        .await
        .unwrap()
        .unwrap();
    let next = timeout(LIMIT, provider.requests.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        next["messages"][1]["tool_calls"][0]["function"]["name"],
        "lookup"
    );
    assert_eq!(next["messages"][2]["role"], "tool");
    stop(&mut worker).await;
}
#[tokio::test]
async fn tool_frames_wait_for_all_results_but_external_frames_flow() {
    let provider = fixture(vec![tools_reply(vec![("1", "hold", json!({}))])]).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut tools = ToolRegistry::new();
    let e = entered.clone();
    let r = release.clone();
    tools
        .register(ToolHandler::new(schema("hold"), move |_, ctx| {
            let e = e.clone();
            let r = r.clone();
            async move {
                ctx.queue_frame(
                    Frame::new(FrameData::Text(TextFrame::new("deferred"))),
                    Direction::Downstream,
                )?;
                e.notify_one();
                r.notified().await;
                Ok(ToolResult::silent(json!({"ok":true})))
            }
        }))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("llm"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    timeout(LIMIT, entered.notified()).await.unwrap();
    worker
        .handle()
        .queue_frame(
            Frame::new(FrameData::Text(TextFrame::new("external"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let mut before = vec![];
    timeout(LIMIT, async {
        loop {
            let frame = worker.recv().await.unwrap().frame;
            let external = matches!(&frame.data,FrameData::Text(t)if t.text=="external");
            before.push(frame);
            if external {
                break;
            }
        }
    })
    .await
    .unwrap();
    assert!(!before
        .iter()
        .any(|f| matches!(&f.data,FrameData::Text(t)if t.text=="deferred")));
    release.notify_one();
    let after = until_idle(&mut worker).await;
    let result = after
        .iter()
        .position(|f| matches!(f.data, FrameData::FunctionCallResult(_)))
        .unwrap();
    let deferred = after
        .iter()
        .position(|f| matches!(&f.data,FrameData::Text(t)if t.text=="deferred"))
        .unwrap();
    assert!(result < deferred);
    stop(&mut worker).await;
}
#[tokio::test]
async fn selective_interruption_preserves_opted_out_tool_and_cancels_other() {
    let provider = fixture(vec![tools_reply(vec![
        ("1", "cancel", json!({})),
        ("2", "keep", json!({})),
    ])])
    .await;
    let entered = Arc::new(tokio::sync::Barrier::new(3));
    let release = Arc::new(Notify::new());
    let kept = Arc::new(AtomicBool::new(false));
    let mut tools = ToolRegistry::new();
    for (name, cancellable) in [("cancel", true), ("keep", false)] {
        let entered = entered.clone();
        let release = release.clone();
        let kept = kept.clone();
        tools
            .register(
                ToolHandler::new(schema(name), move |_, _| {
                    let entered = entered.clone();
                    let release = release.clone();
                    let kept = kept.clone();
                    async move {
                        entered.wait().await;
                        if cancellable {
                            std::future::pending::<()>().await;
                        } else {
                            release.notified().await;
                            kept.store(true, Ordering::SeqCst);
                        }
                        Ok(ToolResult::silent(json!({"ok":true})))
                    }
                })
                .with_options(ToolOptions {
                    cancel_on_interruption: cancellable,
                    timeout: Some(LIMIT),
                }),
            )
            .unwrap();
    }
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("llm"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    timeout(LIMIT, entered.wait()).await.unwrap();
    worker.handle().interrupt();
    release.notify_one();
    let frames = until_idle(&mut worker).await;
    assert!(kept.load(Ordering::SeqCst));
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data,FrameData::FunctionCallCancel(c)if c.function_name=="cancel")));
    assert!(frames
        .iter()
        .any(|f| matches!(&f.data,FrameData::FunctionCallResult(c)if c.function_name=="keep")));
    assert_eq!(
        worker
            .handle()
            .snapshot()
            .context
            .messages
            .iter()
            .filter(|m| m.role == Role::Tool)
            .count(),
        2
    );
    stop(&mut worker).await;
}
#[tokio::test]
async fn cancel_bypasses_full_pipeline_output() {
    let provider = fixture(vec![text_reply("blocked")]).await;
    let mut config = options("llm");
    config.pipeline.output_capacity = 1;
    config.pipeline.queue_capacity = 1;
    let mut worker = LlmWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        config,
    )
    .await
    .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    worker.handle().cancel();
    let result = timeout(LIMIT, worker.join()).await.unwrap();
    assert!(result.is_ok() || result.unwrap_err().to_string().contains("backpressure"));
    assert!(worker.handle().snapshot().closed);
}
#[tokio::test]
async fn backend_attach_message_ack_outputs_and_duplicate_refusal() {
    let provider = fixture(vec![text_reply("private note\n >> Spoken result\n>> More")]).await;
    let bus = LocalBus::new(128).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut backend = BackendLlmWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        options("backend"),
        bus,
        registry,
        None,
    )
    .await
    .unwrap();
    let mut session = BackendSession::attach(client.clone(), "backend", LIMIT)
        .await
        .unwrap();
    let mut duplicate = client
        .request("backend", job("attach", Value::Null))
        .unwrap();
    assert_eq!(duplicate.result().await.unwrap().status, JobStatus::Error);
    assert!(!session.send("question").await.unwrap());
    let first = timeout(LIMIT, session.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let second = session.next().await.unwrap().unwrap();
    assert_eq!(
        first,
        BackendEvent::Output(BackendOutput {
            text: "private note".into(),
            is_thought: false,
            prefers_spoken: false
        })
    );
    assert_eq!(
        second,
        BackendEvent::Output(BackendOutput {
            text: "Spoken result\nMore".into(),
            is_thought: false,
            prefers_spoken: true
        })
    );
    assert_eq!(session.next().await.unwrap(), Some(BackendEvent::Idle));
    session.close().await.unwrap();
    backend.worker().cancel("done");
    timeout(LIMIT, backend.wait()).await.unwrap().unwrap();
}
#[tokio::test]
async fn backend_refuses_messages_without_attachment() {
    let provider = fixture(vec![]).await;
    let bus = LocalBus::new(64).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut backend = BackendLlmWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        options("backend"),
        bus,
        registry,
        None,
    )
    .await
    .unwrap();
    let mut job = client
        .request("backend", job("message", json!({"request":"hello"})))
        .unwrap();
    assert_eq!(job.result().await.unwrap().status, JobStatus::Error);
    backend.worker().cancel("done");
    timeout(LIMIT, backend.wait()).await.unwrap().unwrap();
}
#[tokio::test]
async fn ui_respond_is_answer_data_and_stateless_context_resets() {
    let mut provider = fixture(vec![
        text_reply("first answer"),
        text_reply("second answer"),
    ])
    .await;
    let bus = LocalBus::new(128).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut seeded = request();
    seeded
        .context
        .messages
        .push(Message::text(Role::User, "seed cleared"));
    let mut ui = UiWorker::start(
        provider.client.clone(),
        seeded,
        ToolRegistry::new(),
        UiEventRegistry::new(),
        UiWorkerOptions::default(),
        bus,
        registry,
    )
    .await
    .unwrap();
    ui.handle()
        .set_snapshot(json!({"root":{"role":"button","ref":"e1","name":"Go"}}))
        .unwrap();
    for query in ["first", "second"] {
        let mut job = client
            .request("ui", job("respond", json!({"query":query})))
            .unwrap();
        let response = timeout(LIMIT, job.result()).await.unwrap().unwrap();
        assert_eq!(response.status, JobStatus::Completed);
        assert_eq!(
            response.response.unwrap()["answer"],
            format!("{query} answer")
        );
    }
    for query in ["first", "second"] {
        let body = provider.requests.recv().await.unwrap();
        let messages = body["messages"].as_array().unwrap();
        assert_eq!(messages.iter().filter(|m| m["role"] == "user").count(), 1);
        assert_eq!(
            messages.iter().find(|m| m["role"] == "user").unwrap()["content"],
            query
        );
        assert_eq!(
            messages.iter().filter(|m| m["role"] == "developer").count(),
            1
        );
    }
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}
#[tokio::test]
async fn ui_screen_lists_selection_and_dispatches_real_command() {
    let provider = fixture(vec![text_reply(r#"{"ref":"e2","confidence":0.9}"#)]).await;
    let bus = LocalBus::new(128).unwrap();
    let observer = bus.subscribe("observer").unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        UiEventRegistry::new(),
        UiWorkerOptions::default(),
        bus,
        registry,
    )
    .await
    .unwrap();
    ui.handle().set_snapshot(json!({"root":{"role":"group","children":[{"ref":"e1","role":"textbox","name":"Name","value":"Ada"},{"ref":"e2","role":"button","name":"Submit"}]},"selection":{"ref":"e1","text":"Ada"}})).unwrap();
    let mut list = client
        .request(
            "ui",
            job("screen", json!({"action":"list","target":"textbox"})),
        )
        .unwrap();
    let response = list.result().await.unwrap().response.unwrap();
    assert_eq!(response["elements"][0]["name"], "Name");
    assert!(response["elements"][0].get("ref").is_none());
    let mut selection = client
        .request("ui", job("screen", json!({"action":"selection"})))
        .unwrap();
    assert_eq!(
        selection.result().await.unwrap().response.unwrap()["text"],
        "Ada"
    );
    let mut click = client
        .request(
            "ui",
            job("screen", json!({"action":"click","target":"Submit"})),
        )
        .unwrap();
    assert_eq!(
        click.result().await.unwrap().response.unwrap(),
        json!({"done":true,"label":"Submit"})
    );
    timeout(LIMIT, async {
        loop {
            if let Some(message) = observer.recv().await.unwrap() {
                if let pipecat_workers::MessageData::Frame { frame, .. } = message.data {
                    if let FrameData::RtviUiCommand(command) = frame.data {
                        assert_eq!(command.command, "click");
                        assert_eq!(command.payload.unwrap()["ref"], "e2");
                        break;
                    }
                }
            }
        }
    })
    .await
    .unwrap();
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}
#[test]
fn text_protocol_helpers_preserve_source_separation() {
    assert_eq!(
        split_spoken("note >> remains\n\t>> hear this\n>> next"),
        ("note >> remains".into(), "hear this\nnext".into())
    );
    let messages = vec![
        Message::text(Role::System, "secret"),
        Message::text(Role::User, "hello"),
        Message::text(Role::Assistant, "frontend speech"),
        Message::tool_result("1", "not forwarded"),
    ];
    assert_eq!(render_transcript_request(&messages,false,"Act."),"Voice conversation since the previous delegation:\nUSER: hello\nASSISTANT: frontend speech\n\nAct.");
    assert_eq!(render_ui_state(&json!({"root":{"role":"heading","name":"Hi","level":2,"ref":"e1"},"selection":{"ref":"e1","text":"Hi"}})),"<ui_state>\n- heading \"Hi\" [level=2] [ref=e1]\n<selection ref=\"e1\">\nHi\n</selection>\n</ui_state>");
}

#[tokio::test]
async fn tool_timeout_settles_error_and_runs_one_followup() {
    let mut provider = fixture(vec![
        tools_reply(vec![("slow-1", "slow", json!({}))]),
        text_reply("recovered"),
    ])
    .await;
    let mut tools = ToolRegistry::new();
    tools
        .register(
            ToolHandler::new(schema("slow"), |_, _| async {
                std::future::pending::<Result<ToolResult, AgentError>>().await
            })
            .with_options(ToolOptions {
                cancel_on_interruption: true,
                timeout: Some(Duration::from_millis(10)),
            }),
        )
        .unwrap();
    let mut worker = LlmWorker::start(
        provider.client.clone(),
        request(),
        tools,
        options("timeout"),
    )
    .await
    .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "run")], true)
        .await
        .unwrap();
    let frames = until_idle(&mut worker).await;
    assert!(frames.iter().any(|f|matches!(&f.data,FrameData::FunctionCallResult(c) if c.is_final && c.error.as_deref().is_some_and(|e|e.contains("timed out")))));
    provider.requests.recv().await.unwrap();
    let next = provider.requests.recv().await.unwrap();
    assert!(next["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["role"] == "tool" && m["content"].as_str().unwrap().contains("timed out")));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::LlmResponseStart))
            .count(),
        2
    );
    stop(&mut worker).await;
}

#[tokio::test]
async fn steering_during_tool_execution_is_seen_once_after_tool_result() {
    let mut provider = fixture(vec![
        tools_reply(vec![("1", "gate", json!({}))]),
        text_reply("new request answered"),
    ])
    .await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut tools = ToolRegistry::new();
    let enter = entered.clone();
    let gate = release.clone();
    tools
        .register(ToolHandler::new(schema("gate"), move |_, _| {
            let enter = enter.clone();
            let gate = gate.clone();
            async move {
                enter.notify_one();
                gate.notified().await;
                Ok(ToolResult::new(json!({"done":true})))
            }
        }))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("steer"))
        .await
        .unwrap();
    assert!(!worker
        .handle()
        .append(vec![Message::text(Role::User, "old")], true)
        .await
        .unwrap());
    timeout(LIMIT, entered.notified()).await.unwrap();
    assert!(worker
        .handle()
        .append(vec![Message::text(Role::User, "new")], true)
        .await
        .unwrap());
    release.notify_one();
    let frames = until_idle(&mut worker).await;
    provider.requests.recv().await.unwrap();
    let second = provider.requests.recv().await.unwrap();
    let messages = second["messages"].as_array().unwrap();
    assert_eq!(
        messages
            .iter()
            .filter(|m| m["role"] == "user" && m["content"] == "new")
            .count(),
        1
    );
    assert_eq!(messages.iter().filter(|m| m["role"] == "tool").count(), 1);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.data, FrameData::LlmResponseStart))
            .count(),
        2
    );
    stop(&mut worker).await;
}

#[tokio::test]
async fn backend_detach_fences_a_transform_and_buffered_output_before_reattach() {
    let provider = fixture(vec![
        text_reply(">> old output"),
        text_reply(">> new output"),
    ])
    .await;
    let entered = Arc::new(Notify::new());
    let gate = entered.clone();
    let transform = OutputTransform::new(move |output| {
        let gate = gate.clone();
        async move {
            if output.text == "old output" {
                gate.notify_one();
                std::future::pending::<()>().await;
            }
            Ok(Some(output))
        }
    });
    let bus = LocalBus::new(64).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut backend = BackendLlmWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        options("backend"),
        bus,
        registry,
        Some(transform),
    )
    .await
    .unwrap();
    let first = BackendSession::attach(client.clone(), "backend", LIMIT)
        .await
        .unwrap();
    first.send("old").await.unwrap();
    timeout(LIMIT, entered.notified()).await.unwrap();
    first.close().await.unwrap();
    timeout(LIMIT, async {
        while backend.engine().generation() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let mut second = BackendSession::attach(client, "backend", LIMIT)
        .await
        .unwrap();
    second.send("new").await.unwrap();
    loop {
        match timeout(LIMIT, second.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            BackendEvent::Output(output) => {
                assert_eq!(output.text, "new output");
                break;
            }
            BackendEvent::Idle => {}
            event => panic!("unexpected event: {event:?}"),
        }
    }
    assert_eq!(
        timeout(LIMIT, second.next()).await.unwrap().unwrap(),
        Some(BackendEvent::Idle)
    );
    second.close().await.unwrap();
    backend.worker().cancel("done");
    timeout(LIMIT, backend.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn ui_event_injects_context_before_handler_and_snapshot_stays_context_only() {
    use pipecat_core::{RtviUiEventFrame, RtviUiSnapshotFrame};
    use pipecat_workers::{BusMessage, MessageData};
    let provider = fixture(vec![]).await;
    let bus = LocalBus::new(64).unwrap();
    let registry = WorkerRegistry::new("test");
    let (tx, mut rx) = mpsc::channel(1);
    let mut events = UiEventRegistry::new();
    events
        .register("clicked", move |event, handle| {
            let tx = tx.clone();
            async move {
                let context = serde_json::to_string(&handle.engine().snapshot().context).unwrap();
                tx.send((event, context, handle.snapshot())).await.unwrap();
                handle.send_command("toast", json!({"ok":true})).await
            }
        })
        .unwrap();
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        events,
        UiWorkerOptions::default(),
        bus.clone(),
        registry,
    )
    .await
    .unwrap();
    let snapshot = json!({"root":{"role":"button","name":"Save","ref":"e1"}});
    for data in [
        FrameData::RtviUiSnapshot(RtviUiSnapshotFrame {
            msg_id: "s".into(),
            tree: snapshot.clone(),
        }),
        FrameData::RtviUiEvent(RtviUiEventFrame {
            msg_id: "e".into(),
            event: "clicked".into(),
            payload: Some(json!({"ref":"e1"})),
        }),
    ] {
        bus.publish_wait(BusMessage {
            source: "client".into(),
            target: Some("ui".into()),
            data: MessageData::Frame {
                frame: Box::new(Frame::new(data)),
                direction: Direction::Downstream,
                bridge: None,
            },
        })
        .await
        .unwrap();
    }
    let (event, context, seen) = timeout(LIMIT, rx.recv()).await.unwrap().unwrap();
    assert_eq!(event.name, "clicked");
    assert!(context.contains("<ui_event"));
    assert!(!context.contains("<ui_state>"));
    assert_eq!(seen, Some(snapshot));
    assert!(!ui.handle().engine().snapshot().working);
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn ui_reply_tool_acknowledges_silently_without_extra_inference() {
    let mut provider = fixture(vec![tools_reply(vec![(
        "reply-1",
        "reply",
        json!({"answer":null}),
    )])])
    .await;
    let bus = LocalBus::new(64).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        UiEventRegistry::new(),
        UiWorkerOptions {
            reply_tool: true,
            ..Default::default()
        },
        bus,
        registry,
    )
    .await
    .unwrap();
    let mut request = client
        .request("ui", job("respond", json!({"query":"do this quietly"})))
        .unwrap();
    let response = timeout(LIMIT, request.result()).await.unwrap().unwrap();
    assert_eq!(response.status, JobStatus::Completed);
    assert_eq!(response.response, None);
    provider.requests.recv().await.unwrap();
    assert!(provider.requests.try_recv().is_err());
    assert!(ui.handle().current_job().is_none());
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn cancelled_ui_job_cannot_answer_the_next_job() {
    let mut provider = fixture(vec![
        tools_reply(vec![("hold-1", "hold", json!({}))]),
        text_reply("second answer"),
    ])
    .await;
    let entered = Arc::new(Notify::new());
    let enter = entered.clone();
    let mut tools = ToolRegistry::new();
    tools
        .register(ToolHandler::new(schema("hold"), move |_, _| {
            let enter = enter.clone();
            async move {
                enter.notify_one();
                std::future::pending::<Result<ToolResult, AgentError>>().await
            }
        }))
        .unwrap();
    let bus = LocalBus::new(64).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        tools,
        UiEventRegistry::new(),
        UiWorkerOptions::default(),
        bus,
        registry,
    )
    .await
    .unwrap();
    let mut first = client
        .request("ui", job("respond", json!({"query":"first"})))
        .unwrap();
    timeout(LIMIT, entered.notified()).await.unwrap();
    first.cancel(None).await.unwrap();
    assert_eq!(first.result().await.unwrap().status, JobStatus::Cancelled);
    let mut second = client
        .request("ui", job("respond", json!({"query":"second"})))
        .unwrap();
    let result = timeout(LIMIT, second.result()).await.unwrap().unwrap();
    assert_eq!(result.response, Some(json!({"answer":"second answer"})));
    provider.requests.recv().await.unwrap();
    let sent = provider.requests.recv().await.unwrap();
    assert!(!sent["messages"].to_string().contains("first"));
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn malformed_screen_classifier_cannot_publish_a_command() {
    let provider = fixture(vec![text_reply(r#"{"ref":"made-up","confidence":0.9}"#)]).await;
    let bus = LocalBus::new(64).unwrap();
    let tap = bus.subscribe("observer").unwrap();
    let registry = WorkerRegistry::new("test");
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        UiEventRegistry::new(),
        UiWorkerOptions::default(),
        bus,
        registry,
    )
    .await
    .unwrap();
    ui.handle()
        .set_snapshot(json!({"root":{"name":"real","ref":"e1","role":"button"}}))
        .unwrap();
    assert!(ui
        .handle()
        .screen("click", "real", None)
        .await
        .unwrap_err()
        .to_string()
        .contains("unknown ref"));
    while let Some(message) = tap.try_recv().unwrap() {
        assert!(
            !matches!(message.data,pipecat_workers::MessageData::Frame{frame,..}if matches!(frame.data,FrameData::RtviUiCommand(_)))
        );
    }
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn ui_event_handler_limit_fails_explicitly_and_cancels_running_handlers() {
    use pipecat_core::RtviUiEventFrame;
    use pipecat_workers::{BusMessage, MessageData};
    let provider = fixture(vec![]).await;
    let bus = LocalBus::new(8).unwrap();
    let registry = WorkerRegistry::new("test");
    let entered = Arc::new(Notify::new());
    let enter = entered.clone();
    let mut events = UiEventRegistry::new();
    events
        .register("slow", move |_, _| {
            let enter = enter.clone();
            async move {
                enter.notify_one();
                std::future::pending::<Result<(), AgentError>>().await
            }
        })
        .unwrap();
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        events,
        UiWorkerOptions {
            max_event_handlers: 1,
            ..Default::default()
        },
        bus.clone(),
        registry,
    )
    .await
    .unwrap();
    let event = BusMessage {
        source: "client".into(),
        target: Some("ui".into()),
        data: MessageData::Frame {
            frame: Box::new(Frame::new(FrameData::RtviUiEvent(RtviUiEventFrame {
                msg_id: "e".into(),
                event: "slow".into(),
                payload: None,
            }))),
            direction: Direction::Downstream,
            bridge: None,
        },
    };
    bus.publish_wait(event.clone()).await.unwrap();
    timeout(LIMIT, entered.notified()).await.unwrap();
    bus.publish_wait(event).await.unwrap();
    let result = timeout(LIMIT, ui.wait()).await.unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("UI event handlers"));
    assert!(ui.handle().engine().snapshot().closed);
}

#[tokio::test]
async fn ui_verbatim_speech_is_separate_from_null_job_acknowledgment() {
    let provider = fixture(vec![tools_reply(vec![("hold-1", "hold", json!({}))])]).await;
    let entered = Arc::new(Notify::new());
    let enter = entered.clone();
    let release = Arc::new(Notify::new());
    let gate = release.clone();
    let mut tools = ToolRegistry::new();
    tools
        .register(ToolHandler::new(schema("hold"), move |_, _| {
            let enter = enter.clone();
            let gate = gate.clone();
            async move {
                enter.notify_one();
                gate.notified().await;
                Ok(ToolResult::silent(Value::Null))
            }
        }))
        .unwrap();
    let bus = LocalBus::new(64).unwrap();
    let observer = bus.subscribe("observer").unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        tools,
        UiEventRegistry::new(),
        UiWorkerOptions::default(),
        bus,
        registry,
    )
    .await
    .unwrap();
    let mut response = client
        .request("ui", job("respond", json!({"query":"speak"})))
        .unwrap();
    timeout(LIMIT, entered.notified()).await.unwrap();
    let id = ui.handle().current_job().unwrap();
    assert!(ui
        .handle()
        .respond_to_job(&id, Some("Exact words".into()), true, JobStatus::Completed)
        .await
        .unwrap());
    assert!(!ui
        .handle()
        .respond_to_job(&id, Some("duplicate".into()), true, JobStatus::Completed)
        .await
        .unwrap());
    release.notify_one();
    let result = timeout(LIMIT, response.result()).await.unwrap().unwrap();
    assert_eq!(result.response, None);
    let mut spoken = vec![];
    while let Some(message) = observer.try_recv().unwrap() {
        if message.target.as_deref() != Some("voice") {
            continue;
        }
        if let pipecat_workers::MessageData::Speak { text, .. } = message.data {
            spoken.push(text);
        }
    }
    assert_eq!(spoken, vec!["Exact words"]);
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn backend_remains_attached_after_explicit_engine_interruption() {
    let provider = fixture(vec![text_reply(">> before"), text_reply(">> after")]).await;
    let bus = LocalBus::new(64).unwrap();
    let registry = WorkerRegistry::new("test");
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut backend = BackendLlmWorker::start(
        provider.client.clone(),
        request(),
        ToolRegistry::new(),
        options("backend"),
        bus,
        registry,
        None,
    )
    .await
    .unwrap();
    let mut session = BackendSession::attach(client, "backend", LIMIT)
        .await
        .unwrap();
    session.send("before").await.unwrap();
    loop {
        if session.next().await.unwrap() == Some(BackendEvent::Idle) {
            break;
        }
    }
    backend.engine().interrupt();
    session.send("after").await.unwrap();
    loop {
        if let BackendEvent::Output(output) = timeout(LIMIT, session.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
        {
            assert_eq!(output.text, "after");
            break;
        }
    }
    session.close().await.unwrap();
    backend.worker().cancel("done");
    timeout(LIMIT, backend.wait()).await.unwrap().unwrap();
}
