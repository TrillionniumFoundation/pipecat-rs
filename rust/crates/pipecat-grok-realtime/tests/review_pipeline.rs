//! Real native ToolExecutor/assistant-aggregator integration with a synthetic Grok peer.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_grok_realtime::{FunctionTool, GrokRealtimeProcessor, RealtimeClient, RealtimeConfig};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{Context, Message as ContextMessage, Role};
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(5);
async fn read(ws: &mut Ws) -> Value {
    let message = timeout(WAIT, ws.next())
        .await
        .expect("peer read deadline")
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn send(ws: &mut Ws, mut value: Value) {
    value["event_id"] = json!("review-pipeline");
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(Duration::from_millis(80), ws.next()).await.is_err(),
        "inference ran without an upstream request"
    );
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    send(ws, json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
// The real executor completes once. This application boundary explicitly consumes
// its native receipt before issuing caller-controlled legacy progress/results.
// Declaration ownership and terminal proof are explicitly consumed here.
// Those downstream frames no longer carry native terminal integrity guarantees.
struct ResultPolicy {
    run: bool,
}
#[async_trait]
impl FrameProcessor for ResultPolicy {
    fn name(&self) -> &str {
        "ReviewResultPolicy"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            frame.take_native_tool_batch()?;
            if matches!(frame.data, FrameData::FunctionCallResult(_)) {
                frame.take_native_tool_outcome()?;
            }
            if let FrameData::FunctionCallResult(result) = &mut frame.data {
                let mut progress = result.clone();
                progress.is_final = false;
                progress.run_llm = Some(false);
                progress.result = json!({"progress":1});
                ctx.push(
                    Frame::new(FrameData::FunctionCallResult(progress)),
                    direction,
                )
                .await?;
                result.run_llm = Some(self.run);
            }
        }
        ctx.push(frame, direction).await
    }
}
struct AnswerProbe(Arc<AtomicUsize>);
#[async_trait]
impl FrameProcessor for AnswerProbe {
    fn name(&self) -> &str {
        "ReviewAnswerProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(&frame.data, FrameData::LlmText(text) if text.text=="answer")
        {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}
async fn tool_cycle(run: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.session.tools = Some(vec![FunctionTool::new(
        "echo",
        "Echo",
        json!({"type":"object"}),
    )
    .into()]);
    config.limits.connect_timeout = WAIT;
    config.limits.operation_timeout = WAIT;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let relay = processor.tool_result_relay();
    let (settled, settled_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"s"}}),
        )
        .await;
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        let mut seeds = 0;
        loop {
            let action = read(&mut ws).await;
            match action["type"].as_str().unwrap() {
                "session.update" => {}
                "conversation.item.create" => {
                    seeds += 1;
                    assert_eq!(action["item"]["content"][0]["text"], "question");
                }
                "response.create" => break,
                other => panic!("unexpected initial action {other}"),
            }
        }
        assert_eq!(seeds, 1);
        response(&mut ws, "r1", false).await;
        send(&mut ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"i1","call_id":"c1","name":"echo","arguments":""}})).await;
        for _ in 0..2 {
            send(&mut ws,json!({"type":"response.function_call_arguments.done","call_id":"c1","name":"echo","arguments":"{\"n\":7}"})).await;
        }
        let progress = read(&mut ws).await;
        assert_eq!(progress["type"], "conversation.item.create");
        assert_eq!(progress["item"]["type"], "message");
        let final_result = read(&mut ws).await;
        assert_eq!(final_result["item"]["type"], "function_call_output");
        assert_eq!(final_result["item"]["call_id"], "c1");
        assert_eq!(final_result["item"]["output"], "{\"n\":7}");
        // Even run=true is still deferred by the actual assistant aggregator.
        quiet(&mut ws).await;
        response(&mut ws, "r1", true).await;
        if !run {
            quiet(&mut ws).await;
        }
        settled.send(()).unwrap();
        if !run {
            let append = read(&mut ws).await;
            assert_eq!(append["type"], "conversation.item.create");
            assert_eq!(append["item"]["content"][0]["text"], "continue");
        }
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r2", false).await;
        send(&mut ws,json!({"type":"response.output_text.delta","response_id":"r2","item_id":"i2","output_index":0,"content_index":0,"delta":"answer"})).await;
        response(&mut ws, "r2", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    });
    let count = Arc::new(AtomicUsize::new(0));
    let effect = count.clone();
    let mut executor = ToolExecutor::new(2, 32, 4096).unwrap();
    executor
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let effect = effect.clone();
                Box::pin(async move {
                    effect.fetch_add(1, Ordering::SeqCst);
                    Ok(call.arguments)
                })
            })),
        )
        .unwrap();
    let context = SharedContext::new(Context::new(vec![ContextMessage::text(
        Role::User,
        "question",
    )]));
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = 24000;
    pipeline_config.start.audio_out_sample_rate = 24000;
    let answers = Arc::new(AtomicUsize::new(0));
    let worker = Pipeline::new(vec![
        processor,
        Arc::new(executor),
        Arc::new(ResultPolicy { run }),
        Arc::new(relay),
        Arc::new(AnswerProbe(answers.clone())),
        Arc::new(LLMAssistantAggregator::new(
            context.clone(),
            Default::default(),
        )),
    ])
    .start(pipeline_config)
    .await
    .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, settled_rx).await.unwrap().unwrap();
    if !run {
        worker
            .sender()
            .send(
                Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
                    messages: vec![ChatMessage::new(ChatRole::User, "continue")],
                    run_llm: true,
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
    }
    timeout(WAIT, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(answers.load(Ordering::SeqCst), 1);
    let snapshot = context.snapshot().unwrap();
    assert_eq!(
        snapshot
            .messages
            .iter()
            .filter(|m| m.role == Role::Assistant
                && m.content == Some(pipecat_services::Content::Text("answer".into())))
            .count(),
        1
    );
}
#[tokio::test]
async fn review_native_handler_progress_and_run_false_wait_for_aggregator_append() {
    tool_cycle(false).await;
}
#[tokio::test]
async fn review_native_handler_run_true_waits_for_response_end_and_runs_once() {
    tool_cycle(true).await;
}

#[tokio::test]
async fn review_processor_start_waits_for_readiness_before_context_and_appends() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.limits.connect_timeout = WAIT;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let (configured, configured_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"s"}}),
        )
        .await;
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        configured.send(()).unwrap();
        release_rx.await.unwrap();
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        let mut items = Vec::new();
        let mut creates = 0;
        while items.len() < 2 || creates == 0 {
            let action = read(&mut ws).await;
            match action["type"].as_str().unwrap() {
                "session.update" => {}
                "conversation.item.create" => items.push(
                    action["item"]["content"][0]["text"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                ),
                "response.create" => creates += 1,
                other => panic!("unexpected initial action {other}"),
            }
        }
        assert_eq!(items, ["question", "late append"]);
        assert_eq!(creates, 1);
        response(&mut ws, "r", false).await;
        response(&mut ws, "r", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    });
    let context = SharedContext::new(Context::new(vec![ContextMessage::text(
        Role::User,
        "question",
    )]));
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = 24000;
    pipeline_config.start.audio_out_sample_rate = 24000;
    let context_for_start = context.clone();
    let mut start = tokio::spawn(async move {
        Pipeline::new(vec![
            processor,
            Arc::new(LLMAssistantAggregator::new(
                context_for_start,
                Default::default(),
            )),
        ])
        .start(pipeline_config)
        .await
    });
    timeout(WAIT, configured_rx).await.unwrap().unwrap();
    assert!(
        timeout(Duration::from_millis(80), &mut start)
            .await
            .is_err(),
        "processor accepted frames before provider readiness"
    );
    release.send(()).unwrap();
    let worker = timeout(WAIT, start).await.unwrap().unwrap().unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
                messages: vec![ChatMessage::new(ChatRole::User, "late append")],
                run_llm: true,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[tokio::test]
async fn review_health_restore_retires_session_and_suppresses_late_output_without_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let health = processor.health_handle();
    let (armed, armed_rx) = tokio::sync::oneshot::channel();
    let (release, release_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"s"}}),
        )
        .await;
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r", false).await;
        armed.send(()).unwrap();
        release_rx.await.unwrap();
        let _=ws.send(Message::Text(json!({"event_id":"late","type":"response.text.delta","response_id":"r","delta":"stale after restore"}).to_string().into())).await;
        let closed = timeout(WAIT, ws.next()).await.unwrap();
        assert!(closed.is_none() || matches!(closed, Some(Ok(Message::Close(_)))));
        assert!(
            timeout(Duration::from_millis(80), listener.accept())
                .await
                .is_err(),
            "health restore reconnected automatically"
        );
    });
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = 24000;
    pipeline_config.start.audio_out_sample_rate = 24000;
    let worker = Pipeline::new(vec![processor.clone()])
        .start(pipeline_config)
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    timeout(WAIT, armed_rx).await.unwrap().unwrap();
    health.set_usable(false);
    health.set_usable(true);
    assert!(
        processor.sender().is_err(),
        "restoration reused prior session"
    );
    release.send(()).unwrap();
    timeout(WAIT, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (frames, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(!frames.iter().any(
        |f| matches!(&f.frame.data,FrameData::LlmText(t) if t.text.contains("stale after restore"))
    ));
}

#[tokio::test]
async fn review_runtime_instructions_and_hosted_tools_survive_first_context_and_function_update() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.session.tools = Some(vec![pipecat_grok_realtime::GrokTool::WebSearch]);
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let (updated, updated_rx) = tokio::sync::oneshot::channel();
    let (tools_changed, tools_changed_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        send(
            &mut ws,
            json!({"type":"session.created","session":{"id":"s"}}),
        )
        .await;
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        let update = read(&mut ws).await;
        assert_eq!(update["session"]["instructions"], "runtime instruction");
        assert_eq!(update["session"]["tools"][0]["type"], "x_search");
        updated.send(()).unwrap();
        let tools = read(&mut ws).await;
        let tools = tools["session"]["tools"].as_array().unwrap();
        assert!(tools
            .iter()
            .any(|t| t["type"] == "function" && t["name"] == "echo"));
        assert!(
            tools.iter().any(|t| t["type"] == "x_search"),
            "current hosted tool was discarded"
        );
        assert!(
            !tools.iter().any(|t| t["type"] == "web_search"),
            "stale initial hosted tool was restored"
        );
        tools_changed.send(()).unwrap();
        let initial = read(&mut ws).await;
        assert_eq!(initial["type"], "session.update");
        assert_ne!(
            initial["session"]["instructions"], "context instruction must lose",
            "context overwrote runtime service instruction"
        );
        assert_eq!(read(&mut ws).await["type"], "conversation.item.create");
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r", false).await;
        response(&mut ws, "r", true).await;
        quiet(&mut ws).await;
        ws.close(None).await.unwrap();
    });
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = 24000;
    pipeline_config.start.audio_out_sample_rate = 24000;
    let worker = Pipeline::new(vec![processor])
        .start(pipeline_config)
        .await
        .unwrap();
    let settings = ServiceUpdateSettingsFrame {
        settings: [
            ("system_instruction".into(), json!("runtime instruction")),
            (
                "tools".into(),
                json!([{"type":"x_search","allowed_x_handles":["changed"]}]),
            ),
        ]
        .into(),
        ..Default::default()
    };
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmUpdateSettings(Box::new(settings))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, updated_rx).await.unwrap().unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmSetTools(ToolSet::Functions(vec![
                ToolDefinition {
                    name: "echo".into(),
                    description: "Echo".into(),
                    parameters: json!({"type":"object"}),
                },
            ]))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, tools_changed_rx).await.unwrap().unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(LlmContextFrame {
                messages: vec![
                    ChatMessage::new(ChatRole::System, "context instruction must lose"),
                    ChatMessage::new(ChatRole::User, "question"),
                ],
                ..Default::default()
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, server).await.unwrap().unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[derive(Clone, Copy, Debug)]
enum UnsupportedEdit {
    Append,
    Update,
    Transform,
}
struct EditProbe(Arc<AtomicUsize>);
#[async_trait]
impl FrameProcessor for EditProbe {
    fn name(&self) -> &str {
        "ReviewEditProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            &frame.data,
            FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
        ) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}

// Inject the raw edit at the provider before any aggregator can consume it.
// This does not claim to reject mutations already converted into a context by an
// earlier aggregator; arbitrary context reconciliation is outside this slice.
async fn unsupported_edit(kind: UnsupportedEdit, direction: Direction) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = RealtimeConfig::new("review-only-key");
    config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    config.limits.connect_timeout = WAIT;
    let processor =
        Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
    let (seeded, seeded_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
        send(
            &mut ws,
            json!({"type":"conversation.created","conversation":{"id":"c"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        send(
            &mut ws,
            json!({"type":"session.updated","session":{"id":"s"}}),
        )
        .await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        let item = read(&mut ws).await;
        assert_eq!(item["type"], "conversation.item.create");
        assert_eq!(item["item"]["content"][0]["text"], "original");
        assert_eq!(read(&mut ws).await["type"], "response.create");
        response(&mut ws, "r", false).await;
        response(&mut ws, "r", true).await;
        seeded.send(()).unwrap();
        loop {
            match timeout(WAIT, ws.next()).await.unwrap() {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                other => panic!("unsupported edit caused a provider operation: {other:?}"),
            }
        }
    });
    let context = SharedContext::new(Context::new(vec![ContextMessage::text(
        Role::User,
        "original",
    )]));
    let original = context.snapshot().unwrap().messages;
    let forwarded = Arc::new(AtomicUsize::new(0));
    let probe: Arc<dyn FrameProcessor> = Arc::new(EditProbe(forwarded.clone()));
    let aggregator: Arc<dyn FrameProcessor> = Arc::new(LLMAssistantAggregator::new(
        context.clone(),
        Default::default(),
    ));
    let stages: Vec<Arc<dyn FrameProcessor>> = match direction {
        Direction::Downstream => vec![processor, probe, aggregator],
        Direction::Upstream => vec![aggregator, probe, processor],
    };
    let mut pipeline_config = PipelineConfig::default();
    pipeline_config.start.audio_in_sample_rate = 24000;
    pipeline_config.start.audio_out_sample_rate = 24000;
    let worker = Pipeline::new(stages).start(pipeline_config).await.unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmContext(context.frame().unwrap())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    timeout(WAIT, seeded_rx).await.unwrap().unwrap();
    let transform_effect = Arc::new(AtomicUsize::new(0));
    let messages = DetailedMessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "forbidden mutation").into()],
        run_llm: Some(true),
    };
    let edit = match kind {
        UnsupportedEdit::Append => FrameData::LlmMessagesAppendDetailed(messages),
        UnsupportedEdit::Update => FrameData::LlmMessagesUpdateDetailed(messages),
        UnsupportedEdit::Transform => {
            let effect = transform_effect.clone();
            FrameData::LlmMessagesTransform(LlmMessagesTransformFrame {
                transform: LocalMessageTransform::new(move |_| {
                    effect.fetch_add(1, Ordering::SeqCst);
                    Ok(Vec::new())
                }),
                run_llm: Some(true),
            })
        }
    };
    worker
        .sender()
        .send(Frame::new(edit), direction)
        .await
        .unwrap();
    worker.end().await.unwrap();
    let (_, report) = timeout(WAIT, worker.collect()).await.unwrap().unwrap();
    timeout(WAIT, server).await.unwrap().unwrap();
    assert_eq!(
        forwarded.load(Ordering::SeqCst),
        0,
        "{kind:?} {direction:?}"
    );
    assert_eq!(transform_effect.load(Ordering::SeqCst), 0);
    assert_eq!(context.snapshot().unwrap().messages, original);
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(report.errors[0].error.contains("unsupported"));
    assert_eq!(
        report.errors[0].processor.as_deref(),
        Some("GrokRealtimeService")
    );
}

#[tokio::test]
async fn review_detailed_append_is_rejected_before_forwarding_or_provider_effects() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        unsupported_edit(UnsupportedEdit::Append, direction).await;
    }
}
#[tokio::test]
async fn review_detailed_update_is_rejected_before_forwarding_or_provider_effects() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        unsupported_edit(UnsupportedEdit::Update, direction).await;
    }
}
#[tokio::test]
async fn review_transform_is_rejected_without_executing_local_callback() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        unsupported_edit(UnsupportedEdit::Transform, direction).await;
    }
}
