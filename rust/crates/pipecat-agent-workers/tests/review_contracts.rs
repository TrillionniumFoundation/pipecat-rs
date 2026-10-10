use pipecat_agent_workers::*;
use pipecat_core::{Direction, Frame, FrameData};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, Message, Provider, Role, ToolDefinition,
};
use pipecat_workers::{JobClient, LocalBus, WorkerRegistry};
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
fn options(name: &str) -> LlmWorkerOptions {
    LlmWorkerOptions {
        name: name.into(),
        ..Default::default()
    }
}

#[tokio::test]
async fn full_interruption_cannot_be_downgraded_by_a_later_normal_interruption() {
    let provider = fixture(vec![tools_reply(vec![("1", "hold", json!({}))])]).await;
    let (entered_tx, mut entered_rx) = mpsc::channel(1);
    let released = Arc::new(Notify::new());
    let ran_after_cancel = Arc::new(AtomicBool::new(false));
    let mut tools = ToolRegistry::new();
    let gate = released.clone();
    let ran = ran_after_cancel.clone();
    tools
        .register(
            ToolHandler::new(schema("hold"), move |_, _ctx| {
                let entered = entered_tx.clone();
                let gate = gate.clone();
                let ran = ran.clone();
                async move {
                    entered.send(()).await.unwrap();
                    gate.notified().await;
                    ran.store(true, Ordering::SeqCst);
                    Ok(ToolResult::silent(json!({"changed_external_state":true})))
                }
            })
            .with_options(ToolOptions {
                cancel_on_interruption: false,
                timeout: None,
            }),
        )
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    timeout(LIMIT, entered_rx.recv()).await.unwrap().unwrap();
    // Current-thread runtime guarantees both writes happen before the driver polls.
    worker.handle().interrupt_all();
    worker.handle().interrupt();
    tokio::time::sleep(Duration::from_millis(25)).await;
    released.notify_one();
    until_idle(&mut worker).await;
    stop(&mut worker).await;
    assert!(
        !ran_after_cancel.load(Ordering::SeqCst),
        "an interruption-exempt tool survived interrupt_all when immediately followed by interrupt"
    );
}

#[tokio::test]
async fn cancelling_worker_notifies_retained_tool_context() {
    let provider = fixture(vec![tools_reply(vec![("1", "hold", json!({}))])]).await;
    let (entered_tx, mut entered_rx) = mpsc::channel(1);
    let mut tools = ToolRegistry::new();
    tools
        .register(
            ToolHandler::new(schema("hold"), move |_, ctx| {
                let entered = entered_tx.clone();
                async move {
                    entered.send(ctx).await.unwrap();
                    std::future::pending::<()>().await;
                    Ok(ToolResult::silent(Value::Null))
                }
            })
            .with_options(ToolOptions {
                cancel_on_interruption: false,
                timeout: None,
            }),
        )
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    let ctx = timeout(LIMIT, entered_rx.recv()).await.unwrap().unwrap();
    worker.handle().cancel();
    timeout(LIMIT, worker.join()).await.unwrap().unwrap();
    assert!(
        timeout(Duration::from_millis(100), ctx.cancelled())
            .await
            .is_ok(),
        "ToolContext::cancelled remains pending after the worker has completely stopped"
    );
    assert!(matches!(
        ctx.queue_frame(Frame::new(FrameData::Data), Direction::Downstream),
        Err(AgentError::Cancelled)
    ));
}

#[tokio::test]
async fn malformed_typed_arguments_never_enter_application_handler() {
    #[derive(serde::Deserialize)]
    struct Args {
        value: u64,
    }
    let mut provider = fixture(vec![
        tools_reply(vec![("bad", "typed", json!({"value":"wrong type"}))]),
        text_reply("request rejected"),
    ])
    .await;
    let called = Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let mut tools = ToolRegistry::new();
    tools
        .register(ToolHandler::typed(schema("typed"), move |args: Args, _| {
            observed.store(true, Ordering::SeqCst);
            async move { Ok(ToolResult::new(json!({"value": args.value}))) }
        }))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    let frames = until_idle(&mut worker).await;
    assert!(!called.load(Ordering::SeqCst));
    assert!(frames.iter().any(|f| matches!(&f.data, FrameData::FunctionCallResult(r) if r.is_final && r.error.as_ref().is_some_and(|e| e.contains("invalid tool arguments")))));
    provider.requests.recv().await.unwrap();
    let retry = provider.requests.recv().await.unwrap();
    assert!(retry["messages"]
        .as_array()
        .unwrap()
        .iter()
        .any(|m| m["role"] == "tool"
            && m["content"]
                .as_str()
                .unwrap()
                .contains("invalid tool arguments")));
    stop(&mut worker).await;
}

#[tokio::test]
async fn duplicate_model_call_ids_fail_the_whole_batch_before_any_handler_runs() {
    let provider = fixture(vec![tools_reply(vec![
        ("same", "safe", json!({})),
        ("same", "safe", json!({})),
    ])])
    .await;
    let called = Arc::new(AtomicBool::new(false));
    let observed = called.clone();
    let mut tools = ToolRegistry::new();
    tools
        .register(ToolHandler::new(schema("safe"), move |_, _| {
            observed.store(true, Ordering::SeqCst);
            async { Ok(ToolResult::silent(Value::Null)) }
        }))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    let result = timeout(LIMIT, worker.join()).await.unwrap();
    assert!(result
        .unwrap_err()
        .to_string()
        .contains("duplicate tool call id"));
    assert!(!called.load(Ordering::SeqCst));
}

#[tokio::test]
async fn attachment_timeout_cancels_handler_when_ack_never_arrives() {
    struct OnDrop(Arc<AtomicBool>);
    impl Drop for OnDrop {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let bus = LocalBus::new(32).unwrap();
    let registry = WorkerRegistry::new("review");
    let dropped = Arc::new(AtomicBool::new(false));
    let observed = dropped.clone();
    let worker = pipecat_workers::NativeWorker::new("no-ack")
        .active(true)
        .on_job("attach", false, move |_| {
            let observed = observed.clone();
            async move {
                let _guard = OnDrop(observed);
                std::future::pending::<pipecat_workers::JobResponse>().await
            }
        })
        .unwrap()
        .start(bus.clone(), registry.clone())
        .unwrap();
    let client = JobClient::new("voice", bus, registry);
    let result = BackendSession::attach(client, "no-ack", Duration::from_millis(50)).await;
    assert!(
        matches!(result, Err(AgentError::Failed(error)) if error.contains("attachment timed out"))
    );
    timeout(LIMIT, async {
        while !dropped.load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    worker.cancel("done");
    timeout(LIMIT, worker.wait()).await.unwrap().unwrap();
}

#[tokio::test]
async fn interrupt_drops_buffered_progress_from_cancelled_tools() {
    let provider = fixture(vec![tools_reply(vec![("1", "hold", json!({}))])]).await;
    let (entered_tx, mut entered_rx) = mpsc::channel(1);
    let mut tools = ToolRegistry::new();
    tools
        .register(ToolHandler::new(schema("hold"), move |_, ctx| {
            let entered = entered_tx.clone();
            async move {
                ctx.update(json!({"stale_progress":true})).await?;
                entered.send(()).await.unwrap();
                std::future::pending::<Result<ToolResult, AgentError>>().await
            }
        }))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    timeout(LIMIT, entered_rx.recv()).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    worker.handle().interrupt();
    let frames = until_idle(&mut worker).await;
    stop(&mut worker).await;
    assert!(!frames.iter().any(|f| matches!(&f.data, FrameData::FunctionCallResult(r) if r.result["stale_progress"]==true)), "progress from an opted-in cancelled call escaped the interrupted generation");
    assert!(frames
        .iter()
        .any(|f| matches!(f.data, FrameData::FunctionCallCancel(_))));
}

#[tokio::test]
async fn panicking_peer_tool_cancels_retained_contexts_for_the_entire_batch() {
    let provider = fixture(vec![tools_reply(vec![
        ("hold", "hold", json!({})),
        ("panic", "panic", json!({})),
    ])])
    .await;
    let (entered_tx, mut entered_rx) = mpsc::channel(1);
    let fail = Arc::new(Notify::new());
    let mut tools = ToolRegistry::new();
    tools
        .register(ToolHandler::new(schema("hold"), move |_, ctx| {
            let entered = entered_tx.clone();
            async move {
                entered.send(ctx).await.unwrap();
                std::future::pending::<Result<ToolResult, AgentError>>().await
            }
        }))
        .unwrap();
    let gate = fail.clone();
    tools
        .register(ToolHandler::new(schema("panic"), move |_, _| {
            let gate = gate.clone();
            async move {
                gate.notified().await;
                panic!("deliberate peer failure");
            }
        }))
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "start")], true)
        .await
        .unwrap();
    let ctx = timeout(LIMIT, entered_rx.recv()).await.unwrap().unwrap();
    fail.notify_one();
    let error = timeout(LIMIT, worker.join()).await.unwrap().unwrap_err();
    assert!(error.to_string().contains("tool task failed"));
    assert!(
        timeout(Duration::from_millis(100), ctx.cancelled())
            .await
            .is_ok(),
        "the sibling ToolContext still reports an active invocation after its batch was aborted"
    );
    assert!(matches!(
        ctx.queue_frame(Frame::new(FrameData::Data), Direction::Downstream),
        Err(AgentError::Cancelled)
    ));
}

#[tokio::test]
async fn full_interrupt_discards_buffered_exempt_progress_but_future_normal_interrupt_preserves_it()
{
    let provider = fixture(vec![
        tools_reply(vec![("first", "hold", json!({}))]),
        tools_reply(vec![("second", "hold", json!({}))]),
    ])
    .await;
    let (entered_tx, mut entered_rx) = mpsc::channel(2);
    let release = Arc::new(Notify::new());
    let gate = release.clone();
    let mut tools = ToolRegistry::new();
    tools
        .register(
            ToolHandler::new(schema("hold"), move |_, ctx| {
                let entered = entered_tx.clone();
                let gate = gate.clone();
                async move {
                    ctx.update(json!({"id":ctx.call.id})).await?;
                    entered.send(()).await.unwrap();
                    gate.notified().await;
                    Ok(ToolResult::silent(json!({"id":ctx.call.id})))
                }
            })
            .with_options(ToolOptions {
                cancel_on_interruption: false,
                timeout: None,
            }),
        )
        .unwrap();
    let mut worker = LlmWorker::start(provider.client.clone(), request(), tools, options("review"))
        .await
        .unwrap();
    worker
        .handle()
        .append(vec![Message::text(Role::User, "first")], true)
        .await
        .unwrap();
    timeout(LIMIT, entered_rx.recv()).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    worker.handle().interrupt_all();
    let first = until_idle(&mut worker).await;
    assert!(!first
        .iter()
        .any(|f| matches!(f.data, FrameData::FunctionCallResult(_))));
    worker
        .handle()
        .append(vec![Message::text(Role::User, "second")], true)
        .await
        .unwrap();
    timeout(LIMIT, entered_rx.recv()).await.unwrap().unwrap();
    tokio::time::sleep(Duration::from_millis(25)).await;
    worker.handle().interrupt();
    release.notify_one();
    let second = until_idle(&mut worker).await;
    assert!(second.iter().any(|f|matches!(&f.data,FrameData::FunctionCallResult(r) if r.is_final && r.result["id"]=="second")));
    assert!(second.iter().any(|f|matches!(&f.data,FrameData::FunctionCallResult(r) if !r.is_final && r.result["id"]=="second")));
    stop(&mut worker).await;
}

#[tokio::test]
async fn cancellation_after_success_fences_results_already_queued_for_the_consumer() {
    struct BufferedIdle(Arc<Notify>);
    #[async_trait::async_trait]
    impl pipecat_core::FrameProcessor for BufferedIdle {
        fn name(&self) -> &str {
            "review-output-barrier"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &pipecat_core::ProcessorContext,
        ) -> Result<(), pipecat_core::ProcessorError> {
            let idle = frame.metadata.get(IDLE) == Some(&Value::Bool(true));
            ctx.push(frame, direction).await?;
            if idle {
                self.0.notify_one();
            }
            Ok(())
        }
    }
    for mode in 0..3 {
        for exempt in [false, true] {
            let provider =
                fixture(vec![tools_reply(vec![("completed", "finish", json!({}))])]).await;
            let buffered = Arc::new(Notify::new());
            let mut tools = ToolRegistry::new();
            tools
                .register(
                    ToolHandler::new(schema("finish"), |_, _| async {
                        Ok(ToolResult::silent(json!({"ok":true})))
                    })
                    .with_options(ToolOptions {
                        cancel_on_interruption: !exempt,
                        timeout: None,
                    }),
                )
                .unwrap();
            let mut worker = LlmWorker::start_with_output(
                provider.client.clone(),
                request(),
                tools,
                options("review"),
                vec![Arc::new(BufferedIdle(buffered.clone()))],
            )
            .await
            .unwrap();
            worker
                .handle()
                .append(vec![Message::text(Role::User, "start")], true)
                .await
                .unwrap();
            timeout(LIMIT, buffered.notified()).await.unwrap();
            // The barrier runs after output admission; no consumer has dequeued any frame.
            match mode {
                0 => worker.handle().interrupt(),
                1 => worker.handle().interrupt_all(),
                _ => worker.handle().cancel(),
            }
            if mode != 2 {
                worker.handle().end().await.unwrap();
            }
            let frames = timeout(LIMIT, async {
                let mut frames = vec![];
                while let Some(frame) = worker.recv().await {
                    frames.push(frame.frame);
                }
                frames
            })
            .await
            .unwrap();
            timeout(LIMIT, worker.join()).await.unwrap().unwrap();
            let results = frames
                .iter()
                .filter(|f| matches!(f.data, FrameData::FunctionCallResult(_)))
                .count();
            assert_eq!(
                results,
                usize::from(mode == 0 && exempt),
                "wrong buffered result visibility: mode={mode}, exempt={exempt}"
            );
        }
    }
}

#[tokio::test]
async fn verbatim_speech_reaches_requesting_worker_not_its_private_rpc_inbox() {
    use pipecat_workers::{
        BusMessage, JobParams, JobStatus, MessageData, PipelineBusInput, PipelineInputOptions,
    };
    let provider = fixture(vec![tools_reply(vec![("hold", "hold", json!({}))])]).await;
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let mut tools = ToolRegistry::new();
    let enter = entered.clone();
    let gate = release.clone();
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
    let observer = bus.subscribe("audit-observer").unwrap();
    let registry = WorkerRegistry::new("review");
    let mut voice = pipecat_core::Pipeline::new(vec![])
        .start(Default::default())
        .await
        .unwrap();
    let mut voice_input = PipelineBusInput::attach(
        "voice",
        bus.clone(),
        registry.clone(),
        voice.sender(),
        PipelineInputOptions {
            active: true,
            ..Default::default()
        },
    )
    .unwrap();
    let client = JobClient::new("voice", bus.clone(), registry.clone());
    let mut ui = UiWorker::start(
        provider.client.clone(),
        request(),
        tools,
        UiEventRegistry::new(),
        UiWorkerOptions::default(),
        bus.clone(),
        registry,
    )
    .await
    .unwrap();
    let mut job = client
        .request(
            "ui",
            JobParams {
                name: Some("respond".into()),
                payload: Some(json!({"query":"answer aloud"})),
                timeout: Some(LIMIT),
                ..Default::default()
            },
        )
        .unwrap();
    timeout(LIMIT, entered.notified()).await.unwrap();
    let id = ui.handle().current_job().unwrap();
    ui.handle()
        .respond_to_job(
            &id,
            Some("Exact spoken answer".into()),
            true,
            JobStatus::Completed,
        )
        .await
        .unwrap();
    release.notify_one();
    assert_eq!(
        timeout(LIMIT, job.result())
            .await
            .unwrap()
            .unwrap()
            .response,
        None
    );
    let mut delivered = vec![];
    bus.publish_wait(BusMessage {
        source: "review".into(),
        target: Some("voice".into()),
        data: MessageData::EndWorker { reason: None },
    })
    .await
    .unwrap();
    while let Some(frame) = timeout(LIMIT, voice.recv()).await.unwrap() {
        if let FrameData::TtsSpeak(speech) = frame.frame.data {
            assert!(speech.append_to_context);
            delivered.push(speech.text);
        }
    }
    timeout(LIMIT, voice.join()).await.unwrap().unwrap();
    timeout(LIMIT, voice_input.wait()).await.unwrap().unwrap();
    let mut targets = vec![];
    while let Some(message) = observer.try_recv().unwrap() {
        if matches!(message.data, MessageData::Speak { .. }) {
            targets.push(message.target);
        }
    }
    ui.worker().cancel("done");
    timeout(LIMIT, ui.wait()).await.unwrap().unwrap();
    assert_eq!(
        delivered,
        vec!["Exact spoken answer"],
        "speech was published to these destinations instead: {targets:?}"
    );
}
