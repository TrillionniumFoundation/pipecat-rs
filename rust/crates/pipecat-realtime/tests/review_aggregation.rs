//! Independent real-pipeline and loopback tests for shared realtime aggregation.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::{OpenAiRealtimeProcessor, RealtimeClient, RealtimeConfig, TurnDetection};
use pipecat_services::{Content, Context};
use pipecat_turns::{LLMContextAggregatorPair, RealtimeServiceMode};
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle, time::timeout};
use tokio_tungstenite::tungstenite::Message;

struct Peer {
    events: mpsc::UnboundedSender<Value>,
    commands: mpsc::UnboundedReceiver<Value>,
    task: JoinHandle<()>,
}
struct Tap(mpsc::UnboundedSender<DirectedFrame>);
#[async_trait]
impl FrameProcessor for Tap {
    fn name(&self) -> &str {
        "independent-provider-tap"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let _ = self.0.send(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
async fn tapped(
    rx: &mut mpsc::UnboundedReceiver<DirectedFrame>,
    wanted: impl Fn(&FrameData) -> bool,
) {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = rx.recv().await.expect("tap closed");
            if wanted(&frame.frame.data) {
                break;
            }
        }
    })
    .await
    .expect("provider frame deadline");
}
impl Peer {
    async fn new(server_vad: bool) -> (Self, Arc<OpenAiRealtimeProcessor>) {
        Self::with_readiness(server_vad, None).await
    }
    async fn with_readiness(
        server_vad: bool,
        ready: Option<Arc<tokio::sync::Semaphore>>,
    ) -> (Self, Arc<OpenAiRealtimeProcessor>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("independent-loopback-key");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits.response_timeout = Duration::from_secs(3);
        if server_vad {
            config
                .session
                .audio
                .as_mut()
                .unwrap()
                .input
                .as_mut()
                .unwrap()
                .turn_detection = Some(TurnDetection::ServerVad {
                threshold: 0.5,
                prefix_padding_ms: 300,
                silence_duration_ms: 500,
                create_response: true,
                interrupt_response: true,
            });
        }
        let (event_tx, mut event_rx) = mpsc::unbounded_channel::<Value>();
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let initial = ws.next().await.unwrap().unwrap();
            let initial: Value = serde_json::from_str(initial.to_text().unwrap()).unwrap();
            assert_eq!(initial["type"], "session.update");
            if let Some(ready) = ready {
                ready.acquire().await.unwrap().forget();
            }
            ws.send(Message::Text(json!({"type":"session.updated","event_id":"ready","session":{"id":"session-review","type":"realtime"}}).to_string().into())).await.unwrap();
            loop {
                tokio::select! {
                    event = event_rx.recv() => {
                        let Some(mut event) = event else { break };
                        event["event_id"] = json!("review-event");
                        if ws.send(Message::Text(event.to_string().into())).await.is_err() { break; }
                    }
                    message = ws.next() => {
                        match message {
                            Some(Ok(Message::Text(text))) => {
                                let command = serde_json::from_str(&text).unwrap();
                                if command_tx.send(command).is_err() { break; }
                            }
                            Some(Ok(Message::Close(_))) | None => { let _ = ws.close(None).await; break; }
                            Some(Err(_)) => break,
                            _ => {}
                        }
                    }
                }
            }
        });
        let processor =
            Arc::new(OpenAiRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        (
            Self {
                events: event_tx,
                commands: command_rx,
                task,
            },
            processor,
        )
    }
    fn event(&self, event: Value) {
        self.events.send(event).unwrap();
    }
    async fn command(&mut self) -> Value {
        timeout(Duration::from_secs(2), self.commands.recv())
            .await
            .expect("provider command deadline")
            .expect("peer closed")
    }
    async fn quiet(&mut self) {
        match timeout(Duration::from_millis(120), self.commands.recv()).await {
            Err(_) => {}
            Ok(value) => panic!("unexpected provider action: {value:?}"),
        }
    }
    fn response_start(&self, id: &str) {
        self.event(json!({"type":"response.created","response":{"id":id,"status":"in_progress","output":[]}}));
    }
    fn response_end(&self, id: &str) {
        self.event(
            json!({"type":"response.done","response":{"id":id,"status":"completed","output":[]}}),
        );
    }
    async fn closed(self) {
        timeout(Duration::from_secs(3), self.task)
            .await
            .expect("socket closure deadline")
            .unwrap();
    }
}
async fn start(processors: Vec<Arc<dyn FrameProcessor>>) -> PipelineWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    Pipeline::new(processors).start(config).await.unwrap()
}
async fn inject(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn receive(
    worker: &mut PipelineWorker,
    wanted: impl Fn(&DirectedFrame) -> bool,
) -> DirectedFrame {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.expect("pipeline closed");
            if let FrameData::Error(error) = &frame.frame.data {
                panic!("pipeline error: {error:?}");
            }
            if wanted(&frame) {
                return frame;
            }
        }
    })
    .await
    .expect("pipeline frame deadline")
}
async fn barrier(worker: &mut PipelineWorker) {
    inject(
        worker,
        FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("review-barrier".into()),
        }),
    )
    .await;
    receive(worker, |f| matches!(&f.frame.data, FrameData::Flush(v) if v.origin.as_deref() == Some("review-barrier"))).await;
}
fn pair(mode: RealtimeServiceMode) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        mode,
    )
    .unwrap()
}
fn texts(pair: &LLMContextAggregatorPair) -> Vec<String> {
    pair.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|message| match message.content {
            Some(Content::Text(text)) => text,
            _ => String::new(),
        })
        .collect()
}
async fn transcript_received(pair: &LLMContextAggregatorPair, text: &str) {
    timeout(Duration::from_secs(2), async {
        loop {
            if pair.user().transcript_frames().unwrap().iter().any(|frame| matches!(&frame.data, FrameData::Transcription(value) if value.text == text)) { return; }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    }).await.expect("provider transcript must reach the paired user");
}
async fn stop(mut worker: PipelineWorker, peer: Peer) {
    worker.end().await.unwrap();
    let report = worker.join().await.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    peer.closed().await;
}

#[tokio::test]
async fn review_first_marked_plain_and_detailed_contexts_are_wire_silent() {
    let (mut peer, processor) = Peer::new(false).await;
    let mut worker = start(vec![processor]).await;
    let plain = LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "already heard over audio")],
        ..Default::default()
    };
    for data in [
        FrameData::LlmContext(plain.clone()),
        FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: plain.into(),
            speculation: false,
        }),
    ] {
        let mut frame = Frame::new(data);
        frame
            .metadata
            .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        peer.quiet().await;
    }
    inject(
        &worker,
        FrameData::InputText(TextFrame::new("explicit next turn")),
    )
    .await;
    let item = peer.command().await;
    assert_eq!(item["type"], "conversation.item.create");
    assert_eq!(item["item"]["content"][0]["text"], "explicit next turn");
    assert_eq!(peer.command().await["type"], "response.create");
    peer.response_start("explicit");
    peer.response_end("explicit");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_metadata_broadcast_recommends_external_only_for_server_turn_detection() {
    for server_vad in [false, true] {
        let (mut peer, processor) = Peer::new(server_vad).await;
        let mut worker = start(vec![processor]).await;
        let mut directions = vec![];
        for _ in 0..2 {
            let frame = receive(&mut worker, |f| {
                matches!(f.frame.data, FrameData::LlmServiceMetadata(_))
            })
            .await;
            let FrameData::LlmServiceMetadata(metadata) = frame.frame.data else {
                unreachable!()
            };
            assert!(metadata.is_realtime_service);
            assert!(!metadata.base.service_name.is_empty());
            if server_vad {
                let strategies = metadata
                    .base
                    .user_turn_strategies
                    .expect("source recommends external strategies for server VAD");
                for values in [strategies.start.unwrap(), strategies.stop.unwrap()] {
                    assert_eq!(values.len(), 1);
                    assert!(matches!(values[0], UserTurnStrategy::External { .. }));
                }
            } else {
                assert!(
                    metadata.base.user_turn_strategies.is_none(),
                    "manual turns cannot be driven by absent server VAD"
                );
            }
            directions.push(frame.direction);
        }
        assert!(directions.contains(&Direction::Upstream));
        assert!(directions.contains(&Direction::Downstream));
        peer.quiet().await;
        stop(worker, peer).await;
    }
}

#[tokio::test]
async fn review_audio_only_server_transcript_reaches_user_before_assistant_history() {
    let (mut peer, processor) = Peer::new(true).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    assert!(
        pair.user().realtime_service_enabled().unwrap(),
        "provider metadata must auto-enable user"
    );
    assert!(
        pair.assistant().realtime_service_enabled().unwrap(),
        "provider metadata must auto-enable assistant"
    );
    inject(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![0; 480], 24_000, 1).unwrap()),
    )
    .await;
    assert_eq!(peer.command().await["type"], "input_audio_buffer.append");
    peer.response_start("audio-only");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"server-user","content_index":0,"transcript":"spoken question"}));
    transcript_received(&pair, "spoken question").await;
    peer.event(json!({"type":"response.output_text.delta","response_id":"audio-only","item_id":"server-answer","output_index":0,"content_index":0,"delta":"spoken answer"}));
    peer.response_end("audio-only");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    assert_eq!(texts(&pair), ["spoken question", "spoken answer"]);
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_native_tool_result_waits_for_bot_playback_then_continues_once() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let executions = Arc::new(AtomicUsize::new(0));
    let effect = executions.clone();
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
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let (terminal_tx, mut terminal_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(executor),
        Arc::new(CaptureNativeTerminal(terminal_tx)),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    inject(&worker, FrameData::BotStartedSpeaking).await;
    barrier(&mut worker).await;
    peer.response_start("tools");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    barrier(&mut worker).await;
    peer.event(json!({"type":"response.output_item.added","response_id":"tools","output_index":0,"item":{"type":"function_call","id":"call-item","call_id":"call-review","name":"echo","arguments":""}}));
    peer.event(json!({"type":"response.function_call_arguments.done","response_id":"tools","item_id":"call-item","output_index":0,"call_id":"call-review","arguments":"{\"x\":7}"}));
    peer.response_end("tools");
    let output = peer.command().await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["call_id"], "call-review");
    assert_eq!(output["item"]["output"], "{\"x\":7}");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    timeout(Duration::from_secs(2), async {
        while !texts(&pair).iter().any(|text| text == "{\"x\":7}") {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
    })
    .await
    .expect("assistant must accept the final tool result before playback releases inference");
    peer.quiet().await;
    assert_eq!(executions.load(Ordering::SeqCst), 1);

    let duplicate = timeout(Duration::from_secs(2), terminal_rx.recv())
        .await
        .expect("actual terminal receipt")
        .unwrap();
    worker
        .sender()
        .send(duplicate, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    peer.quiet().await;
    inject(&worker, FrameData::BotStoppedSpeaking).await;
    let mut continuation = peer.command().await;
    while continuation["type"] == "session.update" {
        continuation = peer.command().await;
    }
    assert_eq!(continuation["type"], "response.create", "tool output must precede the assistant-authorized continuation and must not be recreated from history");
    peer.response_start("continued");
    peer.response_end("continued");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_generated_audio_and_transcript_do_not_prove_spoken_delivery() {
    use pipecat_turns::{AssistantAggregatorConfig, AssistantTextMode};
    let (mut peer, processor) = Peer::new(true).await;
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
        RealtimeServiceMode::Auto,
    )
    .unwrap();
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("not-played");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"user-delivery","content_index":0,"transcript":"question"}));
    transcript_received(&pair, "question").await;
    peer.event(json!({"type":"response.output_audio.delta","response_id":"not-played","item_id":"unheard","output_index":0,"content_index":0,"delta":"AAAAAA=="}));
    peer.event(json!({"type":"response.output_audio_transcript.delta","response_id":"not-played","item_id":"unheard","output_index":0,"content_index":0,"delta":"unheard answer"}));
    peer.response_end("not-played");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    assert_eq!(
        texts(&pair),
        ["question"],
        "generated PCM, transcript and response.done are not playback acknowledgements"
    );
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_parallel_response_tools_wait_for_the_slow_final_result() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    use tokio::sync::Semaphore;
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let slow = Arc::new(Semaphore::new(0));
    let slow_callback = slow.clone();
    let mut executor = ToolExecutor::new(2, 32, 4096).unwrap();
    executor
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let slow = slow_callback.clone();
                Box::pin(async move {
                    if call.arguments["x"] == 2 {
                        slow.acquire().await.unwrap().forget();
                    }
                    Ok(call.arguments)
                })
            })),
        )
        .unwrap();
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("tools");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    barrier(&mut worker).await;
    for (index, id) in ["fast", "slow"].into_iter().enumerate() {
        peer.event(json!({"type":"response.output_item.added","response_id":"tools","output_index":index,"item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":"echo","arguments":""}}));
        peer.event(json!({"type":"response.function_call_arguments.done","response_id":"tools","item_id":format!("item-{id}"),"output_index":index,"call_id":id,"arguments":format!("{{\"x\":{}}}",index+1)}));
    }
    peer.response_end("tools");
    let fast = peer.command().await;
    assert_eq!(fast["item"]["type"], "function_call_output");
    assert_eq!(fast["item"]["call_id"], "fast");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    slow.add_permits(1);
    let slow = peer.command().await;
    assert_eq!(slow["item"]["type"], "function_call_output");
    assert_eq!(slow["item"]["call_id"], "slow");
    let mut next = peer.command().await;
    while next["type"] == "session.update" {
        next = peer.command().await;
    }
    assert_eq!(next["type"], "response.create");
    peer.response_start("continued");
    peer.response_end("continued");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_unheard_old_response_cannot_steal_new_response_text_ownership() {
    let (mut peer, processor) = Peer::new(false).await;
    let mut worker = start(vec![processor]).await;
    peer.response_start("old");
    peer.event(json!({"type":"response.output_text.delta","response_id":"old","item_id":"old-item","output_index":0,"content_index":0,"delta":"unheard old text"}));
    peer.response_end("old");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    peer.response_start("new");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseStart)
    })
    .await;
    let plain = LlmContextFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "question")],
        ..Default::default()
    };
    let mut marked = Frame::new(FrameData::LlmContext(plain.clone()));
    marked
        .metadata
        .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
    worker
        .sender()
        .send(marked, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    peer.quiet().await;
    peer.event(json!({"type":"response.output_text.delta","response_id":"new","item_id":"new-item","output_index":0,"content_index":0,"delta":"new reply"}));
    peer.response_end("new");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    let mut full = plain;
    full.messages
        .push(ChatMessage::new(ChatRole::Assistant, "new reply"));
    inject(&worker, FrameData::LlmContext(full)).await;
    barrier(&mut worker).await;
    let mut next = peer.command().await;
    while next["type"] == "session.update" {
        next = peer.command().await;
    }
    assert_eq!(next["type"], "response.create", "the new provider reply is already owned and must not be reimported or confused with old unheard text");
    peer.response_start("explicit");
    peer.response_end("explicit");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_readiness_interruption_preserves_seed_but_cancels_its_inference() {
    let ready = Arc::new(tokio::sync::Semaphore::new(0));
    let (mut peer, processor) = Peer::with_readiness(false, Some(ready.clone())).await;
    let mut worker = start(vec![processor]).await;
    inject(
        &worker,
        FrameData::LlmContext(LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "durable seed")],
            ..Default::default()
        }),
    )
    .await;
    barrier(&mut worker).await;
    inject(&worker, FrameData::Interruption).await;
    barrier(&mut worker).await;
    ready.add_permits(1);
    let mut accepted = vec![];
    let deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    loop {
        let command = tokio::time::timeout_at(deadline, peer.commands.recv())
            .await
            .expect("durable item was lost while waiting for readiness")
            .expect("peer closed");
        assert_ne!(
            command["type"], "response.create",
            "interrupted inference must not be revived when readiness arrives"
        );
        let item = command["type"] == "conversation.item.create";
        accepted.push(command);
        if item {
            break;
        }
    }
    while let Ok(Some(command)) = timeout(Duration::from_millis(120), peer.commands.recv()).await {
        assert_ne!(
            command["type"], "response.create",
            "interrupted inference must not be revived when readiness arrives"
        );
        accepted.push(command);
    }
    let items: Vec<_> = accepted
        .iter()
        .filter(|v| v["type"] == "conversation.item.create")
        .collect();
    assert_eq!(
        items.len(),
        1,
        "durable admission must not retry or duplicate items"
    );
    assert_eq!(items[0]["item"]["content"][0]["text"], "durable seed");
    inject(&worker, FrameData::LlmRun).await;
    assert_eq!(peer.command().await["type"], "response.create");
    peer.response_start("requested-later");
    peer.response_end("requested-later");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_rejected_context_item_closes_instead_of_reusing_unconfirmed_history() {
    let (mut peer, processor) = Peer::new(false).await;
    let mut worker = start(vec![processor.clone()]).await;
    let sender = processor.sender().unwrap();
    inject(
        &worker,
        FrameData::LlmContext(LlmContextFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "rejected seed")],
            ..Default::default()
        }),
    )
    .await;
    let mut item = peer.command().await;
    while item["type"] == "session.update" {
        item = peer.command().await;
    }
    assert_eq!(item["type"], "conversation.item.create");
    peer.event(json!({"type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"context item rejected","event_id":item["event_id"]}}));
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker
                .recv()
                .await
                .expect("an explicit provider error must be reported");
            if let FrameData::Error(error) = frame.frame.data {
                assert!(
                    error.error.contains("rejected") || error.error.contains("provider"),
                    "{error:?}"
                );
                break;
            }
        }
    })
    .await
    .expect("provider rejection must not be swallowed");
    peer.closed().await;
    assert!(
        sender
            .send(pipecat_realtime::Action::CreateResponse(Default::default()))
            .await
            .is_err(),
        "failed history cannot support later inference on the same socket"
    );
    let _ = worker.cancel("rejection test complete").await;
    let failure = timeout(Duration::from_secs(3), worker.join())
        .await
        .expect("terminal owner cleanup")
        .expect_err("fatal provider rejection must remain visible in the terminal join result");
    assert!(
        matches!(failure, PipelineError::Execution(message) if message.contains("context item rejected"))
    );
}

#[tokio::test]
async fn review_arguments_done_executes_and_delivers_result_before_response_done() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    use std::sync::atomic::{AtomicUsize, Ordering};
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let executions = Arc::new(AtomicUsize::new(0));
    let effect = executions.clone();
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
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("no-done-yet");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    barrier(&mut worker).await;
    peer.event(json!({"type":"response.output_item.added","response_id":"no-done-yet","output_index":0,"item":{"type":"function_call","id":"item-no-done","call_id":"no-done-call","name":"echo","arguments":""}}));
    peer.event(json!({"type":"response.function_call_arguments.done","response_id":"no-done-yet","item_id":"item-no-done","output_index":0,"call_id":"no-done-call","arguments":"{\"x\":9}"}));
    let result = peer.command().await;
    assert_eq!(result["type"], "conversation.item.create");
    assert_eq!(result["item"]["type"], "function_call_output", "argument completion must execute the callback and deliver its output without requiring response.done");
    assert_eq!(result["item"]["call_id"], "no-done-call");
    assert_eq!(result["item"]["output"], "{\"x\":9}");
    assert_eq!(executions.load(Ordering::SeqCst), 1);
    peer.quiet().await;
    // Only inference is allowed to wait behind the originating active response.
    peer.response_end("no-done-yet");
    let mut next = peer.command().await;
    while next["type"] == "session.update" {
        next = peer.command().await;
    }
    assert_eq!(next["type"], "response.create");
    peer.response_start("continuation");
    peer.response_end("continuation");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    inject(&worker, FrameData::LlmRun).await;
    let mut explicit = peer.command().await;
    while explicit["type"] == "session.update" {
        explicit = peer.command().await;
    }
    assert_eq!(
        explicit["type"], "response.create",
        "an explicit downstream run must not be suppressed by an unchanged tool-result revision"
    );
    peer.response_start("explicit-after-tool");
    peer.response_end("explicit-after-tool");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    inject(
        &worker,
        FrameData::InputText(TextFrame::new("new question after tool")),
    )
    .await;
    let mut item = peer.command().await;
    while item["type"] == "session.update" {
        item = peer.command().await;
    }
    assert_eq!(item["type"], "conversation.item.create");
    assert_eq!(
        item["item"]["content"][0]["text"],
        "new question after tool"
    );
    assert_eq!(
        peer.command().await["type"],
        "response.create",
        "an explicit new input must retain inference intent after tool continuation"
    );
    peer.response_start("text-after-tool");
    peer.response_end("text-after-tool");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_explicit_repeated_input_text_is_a_new_item_not_transcript_reconciliation() {
    let (mut peer, processor) = Peer::new(false).await;
    let mut worker = start(vec![processor]).await;
    peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"earlier-user-item","content_index":0,"transcript":"repeat"}));
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::Transcription(_))
    })
    .await;
    inject(&worker, FrameData::InputText(TextFrame::new("repeat"))).await;
    let item = peer.command().await;
    assert_eq!(
        item["type"], "conversation.item.create",
        "explicit input text owns a new item even if earlier provider audio had identical words"
    );
    assert_eq!(item["item"]["role"], "user");
    assert_eq!(item["item"]["content"][0]["text"], "repeat");
    assert_eq!(peer.command().await["type"], "response.create");
    peer.response_start("new-repeat");
    peer.response_end("new-repeat");
    receive(&mut worker, |f| {
        matches!(f.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_no_run_append_survives_trailing_audio_snapshot_until_explicit_run() {
    let (mut peer, processor) = Peer::new(true).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    inject(
        &worker,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(
                ChatRole::User,
                "remember this explicit note",
            )],
            run_llm: false,
        }),
    )
    .await;
    let mut append = peer.command().await;
    while append["type"] == "session.update" {
        append = peer.command().await;
    }
    assert_eq!(append["type"], "conversation.item.create");
    assert_eq!(
        append["item"]["content"][0]["text"],
        "remember this explicit note"
    );
    barrier(&mut worker).await;
    peer.quiet().await;
    peer.response_start("audio-after-edit");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"audio-user-after-edit","content_index":0,"transcript":"spoken question"}));
    transcript_received(&pair, "spoken question").await;
    peer.event(json!({"type":"response.output_text.delta","response_id":"audio-after-edit","item_id":"answer-after-edit","output_index":0,"content_index":0,"delta":"spoken answer"}));
    peer.response_end("audio-after-edit");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    assert_eq!(
        texts(&pair),
        [
            "remember this explicit note",
            "spoken question",
            "spoken answer"
        ]
    );
    peer.quiet().await;
    inject(&worker, FrameData::LlmRun).await;
    let mut next = peer.command().await;
    while next["type"] == "session.update" {
        next = peer.command().await;
    }
    assert_eq!(next["type"], "response.create", "the explicit note and provider-owned audio history must not be dropped or replayed by trailing snapshots");
    peer.response_start("explicit-after-edit");
    peer.response_end("explicit-after-edit");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

struct HoldFirstExplicitContext {
    pending: mpsc::UnboundedSender<Frame>,
    hold: std::sync::atomic::AtomicBool,
}
#[async_trait]
impl FrameProcessor for HoldFirstExplicitContext {
    fn name(&self) -> &str {
        "hold-explicit-context-before-provider"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
            && !frame.is_realtime_context_update()
            && self.hold.swap(false, std::sync::atomic::Ordering::AcqRel)
        {
            self.pending.send(frame).unwrap();
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_trailing_snapshot_cannot_absorb_an_explicit_append_not_yet_admitted() {
    let (mut peer, processor) = Peer::new(true).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let (pending_tx, mut pending_rx) = mpsc::unbounded_channel();
    let hold = Arc::new(HoldFirstExplicitContext {
        pending: pending_tx,
        hold: true.into(),
    });
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        hold,
        processor,
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    inject(
        &worker,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "delayed explicit note")],
            run_llm: false,
        }),
    )
    .await;
    let held = timeout(Duration::from_secs(2), pending_rx.recv())
        .await
        .expect("the paired user must produce a no-run explicit context effect")
        .unwrap();
    assert!(!held.is_realtime_context_update());
    peer.quiet().await;
    peer.response_start("audio-before-edit-admission");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    peer.event(json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"later-user","content_index":0,"transcript":"later audio question"}));
    transcript_received(&pair, "later audio question").await;
    peer.event(json!({"type":"response.output_text.delta","response_id":"audio-before-edit-admission","item_id":"later-answer","output_index":0,"content_index":0,"delta":"later audio answer"}));
    peer.response_end("audio-before-edit-admission");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    assert_eq!(
        texts(&pair),
        [
            "delayed explicit note",
            "later audio question",
            "later audio answer"
        ]
    );
    peer.quiet().await;
    worker
        .sender()
        .send(held, Direction::Downstream)
        .await
        .unwrap();
    let mut append = peer.command().await;
    while append["type"] == "session.update" {
        append = peer.command().await;
    }
    assert_eq!(
        append["type"], "conversation.item.create",
        "trailing local state must not impersonate admission of a delayed explicit edit"
    );
    assert_eq!(
        append["item"]["content"][0]["text"],
        "delayed explicit note"
    );
    barrier(&mut worker).await;
    peer.quiet().await;
    inject(&worker, FrameData::LlmRun).await;
    let mut run = peer.command().await;
    while run["type"] == "session.update" {
        run = peer.command().await;
    }
    assert_eq!(run["type"], "response.create");
    peer.response_start("run-after-delayed-edit");
    peer.response_end("run-after-delayed-edit");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

async fn expect_context_rejection(worker: &mut PipelineWorker, peer: &mut Peer) {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker
                .recv()
                .await
                .expect("invalid native context must report an error");
            if let FrameData::Error(error) = frame.frame.data {
                assert!(
                    error.error.contains("context") || error.error.contains("realtime"),
                    "{error:?}"
                );
                break;
            }
        }
    })
    .await
    .expect("invalid native context was silently accepted");
    match timeout(Duration::from_millis(120), peer.commands.recv()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(command)) => panic!("invalid context caused a provider mutation: {command:?}"),
    }
}
async fn cancel_after_rejection(mut worker: PipelineWorker, peer: Peer) {
    let _ = worker.cancel("intent rejection checked").await;
    timeout(Duration::from_secs(3), worker.join())
        .await
        .expect("rejected context cleanup")
        .unwrap();
    peer.closed().await;
}

#[tokio::test]
async fn review_exact_context_replay_is_idempotent_but_forged_payload_or_marker_is_rejected() {
    for forge_marker in [false, true] {
        let (mut peer, processor) = Peer::new(false).await;
        let pair = pair(RealtimeServiceMode::Auto);
        let (pending_tx, mut pending_rx) = mpsc::unbounded_channel();
        let hold = Arc::new(HoldFirstExplicitContext {
            pending: pending_tx,
            hold: true.into(),
        });
        let mut worker = start(vec![pair.user(), hold, processor, pair.assistant()]).await;
        barrier(&mut worker).await;
        inject(
            &worker,
            FrameData::LlmMessagesAppend(MessagesFrame {
                messages: vec![ChatMessage::new(ChatRole::User, "sealed explicit note")],
                run_llm: false,
            }),
        )
        .await;
        let held = timeout(Duration::from_secs(2), pending_rx.recv())
            .await
            .expect("explicit context producer")
            .unwrap();
        worker
            .sender()
            .send(held.clone(), Direction::Downstream)
            .await
            .unwrap();
        let mut append = peer.command().await;
        while append["type"] == "session.update" {
            append = peer.command().await;
        }
        assert_eq!(append["type"], "conversation.item.create");
        assert_eq!(append["item"]["content"][0]["text"], "sealed explicit note");
        barrier(&mut worker).await;
        worker
            .sender()
            .send(held.clone(), Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut worker).await;
        peer.quiet().await;
        let mut forged = held;
        if forge_marker {
            forged
                .metadata
                .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
        } else {
            match &mut forged.data {
                FrameData::LlmContext(context) => context
                    .messages
                    .push(ChatMessage::new(ChatRole::User, "forged extra text")),
                FrameData::LlmContextDetailed(context) => context
                    .context
                    .messages
                    .push(ChatMessage::new(ChatRole::User, "forged extra text").into()),
                _ => panic!("expected context"),
            }
        }
        worker
            .sender()
            .send(forged, Direction::Downstream)
            .await
            .unwrap();
        expect_context_rejection(&mut worker, &mut peer).await;
        cancel_after_rejection(worker, peer).await;
    }
}

#[tokio::test]
async fn review_missing_explicit_context_sequence_is_rejected_before_wire_mutation() {
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let (pending_tx, mut pending_rx) = mpsc::unbounded_channel();
    let hold = Arc::new(HoldFirstExplicitContext {
        pending: pending_tx,
        hold: true.into(),
    });
    let mut worker = start(vec![pair.user(), hold, processor, pair.assistant()]).await;
    barrier(&mut worker).await;
    inject(
        &worker,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "first withheld")],
            run_llm: false,
        }),
    )
    .await;
    let _first = timeout(Duration::from_secs(2), pending_rx.recv())
        .await
        .expect("first explicit context")
        .unwrap();
    inject(
        &worker,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "second arrives first")],
            run_llm: false,
        }),
    )
    .await;
    expect_context_rejection(&mut worker, &mut peer).await;
    assert_eq!(
        texts(&pair),
        ["first withheld", "second arrives first"],
        "the local edit remains truthful even though remote admission rejects its sequence gap"
    );
    cancel_after_rejection(worker, peer).await;
}

#[tokio::test]
async fn review_reinjected_old_run_cannot_acquire_the_new_interruption_epoch() {
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let (pending_tx, mut pending_rx) = mpsc::unbounded_channel();
    let hold = Arc::new(HoldFirstExplicitContext {
        pending: pending_tx,
        hold: true.into(),
    });
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        hold,
        processor,
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    inject(&worker, FrameData::LlmRun).await;
    let stale = timeout(Duration::from_secs(2), pending_rx.recv())
        .await
        .expect("held explicit run")
        .unwrap();
    inject(&worker, FrameData::Interruption).await;
    barrier(&mut worker).await;
    worker
        .sender()
        .send(stale, Direction::Downstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    while let Ok(Some(command)) = timeout(Duration::from_millis(120), peer.commands.recv()).await {
        assert_ne!(
            command["type"], "response.create",
            "a retained old inference fence cannot become current on reinjection"
        );
        assert_eq!(
            command["type"], "session.update",
            "only durable context settings are permissible for this stale empty run"
        );
    }
    inject(&worker, FrameData::LlmRun).await;
    let mut fresh = peer.command().await;
    while fresh["type"] == "session.update" {
        fresh = peer.command().await;
    }
    assert_eq!(
        fresh["type"], "response.create",
        "the next current operation must remain admissible after stale inference is suppressed"
    );
    peer.response_start("fresh-after-reinjection");
    peer.response_end("fresh-after-reinjection");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_deferred_inference_keeps_foreign_source_fence_when_provider_epoch_does_not_change()
{
    let source_pair = pair(RealtimeServiceMode::Enabled);
    let mut source = start(vec![source_pair.user()]).await;
    barrier(&mut source).await;
    let (mut peer, processor) = Peer::new(false).await;
    let mut destination = start(vec![processor.clone()]).await;
    barrier(&mut destination).await;
    let provider_sender = processor.sender().unwrap();
    let provider_epoch = provider_sender.generation();
    for first in [true, false] {
        inject(&source, FrameData::LlmRun).await;
        let context = receive(&mut source, |frame| {
            matches!(
                frame.frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .await
        .frame;
        assert!(context
            .native_realtime_context_cause()
            .unwrap()
            .unwrap()
            .inference_fence()
            .unwrap()
            .is_current());
        destination
            .sender()
            .send(context, Direction::Downstream)
            .await
            .unwrap();
        barrier(&mut destination).await;
        if first {
            let mut command = peer.command().await;
            while command["type"] == "session.update" {
                command = peer.command().await;
            }
            assert_eq!(command["type"], "response.create");
            peer.response_start("foreign-active");
            receive(&mut destination, |frame| {
                matches!(frame.frame.data, FrameData::LlmResponseStart)
            })
            .await;
        } else {
            peer.quiet().await;
        }
    }
    inject(&source, FrameData::Interruption).await;
    barrier(&mut source).await;
    assert_eq!(
        provider_sender.generation(),
        provider_epoch,
        "this control must not rely on a provider-side interruption"
    );
    peer.response_end("foreign-active");
    peer.quiet().await;
    inject(&destination, FrameData::LlmRun).await;
    assert_eq!(
        peer.command().await["type"],
        "response.create",
        "a fresh provider invocation must remain live after the foreign source expires"
    );
    peer.response_start("fresh-local");
    peer.response_end("fresh-local");
    receive(&mut destination, |frame| {
        matches!(frame.frame.data, FrameData::LlmResponseEnd)
    })
    .await;
    source.end().await.unwrap();
    let (_, report) = timeout(Duration::from_secs(3), source.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    stop(destination, peer).await;
}

struct HoldRawCancel {
    entered: Arc<tokio::sync::Semaphore>,
    release: Arc<tokio::sync::Semaphore>,
}
#[async_trait]
impl FrameProcessor for HoldRawCancel {
    fn name(&self) -> &str {
        "independent-raw-cancel-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::FunctionCallCancel(_))
            && frame.native_tool_outcome()?.is_none()
        {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
async fn tapped_terminal(rx: &mut mpsc::UnboundedReceiver<DirectedFrame>) -> Frame {
    timeout(Duration::from_secs(2), async {
        loop {
            let item = rx.recv().await.unwrap();
            if item.frame.native_tool_outcome().unwrap().is_some() {
                return item.frame;
            }
        }
    })
    .await
    .expect("native terminal receipt")
}
fn declare_tool(peer: &Peer, response: &str, id: &str) {
    peer.event(json!({"type":"response.output_item.added","response_id":response,"output_index":0,"item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":"work","arguments":""}}));
    peer.event(json!({"type":"response.function_call_arguments.done","response_id":response,"item_id":format!("item-{id}"),"output_index":0,"call_id":id,"arguments":"{}"}));
}
#[tokio::test]
async fn review_raw_cancellation_request_has_no_provider_effect_until_executor_acknowledges() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let callback_entered = Arc::new(tokio::sync::Semaphore::new(0));
    let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
    let called = callback_entered.clone();
    executor
        .register(
            "work",
            RegisteredTool::new(Arc::new(move |_| {
                let called = called.clone();
                Box::pin(async move {
                    called.add_permits(1);
                    std::future::pending::<Result<Value, String>>().await
                })
            })),
        )
        .unwrap();
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(HoldRawCancel {
            entered: entered.clone(),
            release: release.clone(),
        }),
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("cancel-race");
    declare_tool(&peer, "cancel-race", "cancel-call");
    timeout(Duration::from_secs(2), callback_entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    peer.response_end("cancel-race");
    tapped(&mut tap_rx, |frame| {
        matches!(frame, FrameData::LlmResponseEnd)
    })
    .await;
    inject(
        &worker,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "work".into(),
            tool_call_id: "cancel-call".into(),
            run_llm: false,
        }),
    )
    .await;
    timeout(Duration::from_secs(2), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    peer.quiet().await;
    assert!(pair.assistant().has_function_calls_in_progress().unwrap());
    release.add_permits(1);
    let output = peer.command().await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["call_id"], "cancel-call");
    assert_eq!(output["item"]["output"], "CANCELLED");
    let terminal = tapped_terminal(&mut tap_rx).await;
    let receipt = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(receipt.state(), ToolCallTerminalState::Cancelled);
    assert_eq!(
        receipt.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    barrier(&mut worker).await;
    assert!(!pair.assistant().has_function_calls_in_progress().unwrap());
    assert_eq!(
        texts(&pair)
            .iter()
            .filter(|text| text.as_str() == "CANCELLED")
            .count(),
        1
    );
    peer.quiet().await;
    stop(worker, peer).await;
}
struct HoldNativeTerminal(mpsc::UnboundedSender<Frame>, std::sync::atomic::AtomicBool);
#[async_trait]
impl FrameProcessor for HoldNativeTerminal {
    fn name(&self) -> &str {
        "independent-native-terminal-hold"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && frame.native_tool_outcome()?.is_some()
            && self.1.swap(false, std::sync::atomic::Ordering::SeqCst)
        {
            self.0.send(frame).unwrap();
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test]
async fn review_forged_native_tool_payload_is_rejected_before_provider_or_assistant_settlement() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
    executor
        .register(
            "work",
            RegisteredTool::new(Arc::new(|_| Box::pin(async { Ok(json!("actual result")) }))),
        )
        .unwrap();
    let (terminal_tx, mut terminal_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(executor),
        Arc::new(HoldNativeTerminal(terminal_tx, true.into())),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("forgery");
    declare_tool(&peer, "forgery", "forged-call");
    let mut terminal = timeout(Duration::from_secs(2), terminal_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(terminal.native_tool_outcome().unwrap().is_some());
    peer.response_end("forgery");
    barrier(&mut worker).await;
    peer.quiet().await;
    let FrameData::FunctionCallResult(result) = &mut terminal.data else {
        panic!("expected native plain result")
    };
    result.result = json!("forged result");
    worker
        .sender()
        .send(terminal, Direction::Downstream)
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let item = worker
                .recv()
                .await
                .expect("rejection should produce an error");
            if let FrameData::Error(_) = item.frame.data {
                break;
            }
        }
    })
    .await
    .expect("forged native outcome must fail before any provider action");
    assert!(!texts(&pair)
        .iter()
        .any(|text| text.contains("forged result")));
    match timeout(Duration::from_millis(120), peer.commands.recv()).await {
        Err(_) | Ok(None) => {}
        Ok(Some(command)) => panic!("forgery mutated provider: {command}"),
    }
    cancel_after_rejection(worker, peer).await;
}
#[tokio::test]
async fn review_native_timeout_projects_cancelled_for_provider_and_keeps_original_failed_receipt() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
    let mut tool = RegisteredTool::new(Arc::new(|_| {
        Box::pin(std::future::pending::<Result<Value, String>>())
    }));
    tool.timeout = Duration::from_millis(80);
    executor.register("work", tool).unwrap();
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    inject(&worker, FrameData::BotStartedSpeaking).await;
    barrier(&mut worker).await;
    peer.response_start("timeout");
    declare_tool(&peer, "timeout", "timeout-call");
    peer.response_end("timeout");
    let output = peer.command().await;
    assert_eq!(output["item"]["call_id"], "timeout-call");
    assert_eq!(output["item"]["output"], "CANCELLED");
    let terminal = tapped_terminal(&mut tap_rx).await;
    let receipt = terminal.native_tool_outcome().unwrap().unwrap();
    assert_eq!(receipt.state(), ToolCallTerminalState::Failed);
    assert_eq!(receipt.reason(), ToolCallEndReason::TimedOut);
    assert_eq!(
        receipt.effects(),
        ToolCallEffectStatus::ExternalEffectsUnknown
    );
    assert!(
        matches!(terminal.data, FrameData::FunctionCallCancel(_)),
        "relay must preserve the permitted source cancellation projection toward assistant"
    );
    barrier(&mut worker).await;
    assert_eq!(
        texts(&pair)
            .iter()
            .filter(|text| text.as_str() == "CANCELLED")
            .count(),
        1
    );
    peer.quiet().await;
    inject(&worker, FrameData::BotStoppedSpeaking).await;
    let mut continuation = peer.command().await;
    while continuation["type"] == "session.update" {
        continuation = peer.command().await;
    }
    assert_eq!(continuation["type"], "response.create");
    peer.response_start("after-timeout");
    peer.response_end("after-timeout");
    tapped(&mut tap_rx, |frame| {
        matches!(frame, FrameData::LlmResponseEnd)
    })
    .await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

struct CaptureNativeTerminal(mpsc::UnboundedSender<Frame>);
#[async_trait]
impl FrameProcessor for CaptureNativeTerminal {
    fn name(&self) -> &str {
        "independent-native-terminal-capture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream && frame.native_tool_outcome()?.is_some() {
            self.0.send(frame.clone()).unwrap();
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_interruption_cannot_replace_already_admitted_held_success_with_synthetic_cancel() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
    executor
        .register(
            "work",
            RegisteredTool::new(Arc::new(|_| {
                Box::pin(async { Ok(json!("admitted success")) })
            })),
        )
        .unwrap();
    let (terminal_tx, mut terminal_rx) = mpsc::unbounded_channel();
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor,
        Arc::new(executor),
        Arc::new(HoldNativeTerminal(terminal_tx, true.into())),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("held-success");
    declare_tool(&peer, "held-success", "held-success-call");
    let terminal = timeout(Duration::from_secs(2), terminal_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        terminal.native_tool_outcome().unwrap().unwrap().state(),
        ToolCallTerminalState::Completed
    );
    peer.response_end("held-success");
    tapped(&mut tap_rx, |frame| {
        matches!(frame, FrameData::LlmResponseEnd)
    })
    .await;
    inject(&worker, FrameData::Interruption).await;
    barrier(&mut worker).await;
    while let Ok(Some(command)) = timeout(Duration::from_millis(120), peer.commands.recv()).await {
        assert!(
            matches!(
                command["type"].as_str(),
                Some("response.cancel" | "input_audio_buffer.clear")
            ),
            "interruption invented provider settlement: {command}"
        );
    }
    // Current delivery policy gates any new inference after durable settlement.
    inject(&worker, FrameData::BotStartedSpeaking).await;
    barrier(&mut worker).await;
    worker
        .sender()
        .send(terminal, Direction::Downstream)
        .await
        .unwrap();
    let output = peer.command().await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["call_id"], "held-success-call");
    assert_eq!(output["item"]["output"], "\"admitted success\"");
    let terminal = tapped_terminal(&mut tap_rx).await;
    assert_eq!(
        terminal.native_tool_outcome().unwrap().unwrap().state(),
        ToolCallTerminalState::Completed
    );
    barrier(&mut worker).await;
    assert_eq!(
        texts(&pair)
            .iter()
            .filter(|text| text.as_str() == "\"admitted success\"")
            .count(),
        1
    );
    assert!(!texts(&pair).iter().any(|text| text == "CANCELLED"));
    peer.quiet().await;
    inject(&worker, FrameData::BotStoppedSpeaking).await;
    let mut continuation = peer.command().await;
    while continuation["type"] == "session.update" {
        continuation = peer.command().await;
    }
    assert_eq!(continuation["type"], "response.create");
    peer.response_start("after-held-success");
    peer.response_end("after-held-success");
    tapped(&mut tap_rx, |frame| {
        matches!(frame, FrameData::LlmResponseEnd)
    })
    .await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_unreceipted_success_error_and_context_cannot_settle_native_tool_calls() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    for declared in [false, true] {
        for injection in ["success", "error", "context"] {
            if !declared && injection == "context" {
                // A deliberately supplied initial saved history is a seed,
                // not settlement of a live provider call.
                continue;
            }
            let (mut peer, processor) = Peer::new(false).await;
            let pair = pair(RealtimeServiceMode::Auto);
            let entered = Arc::new(tokio::sync::Semaphore::new(0));
            let called = entered.clone();
            let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
            executor
                .register(
                    "work",
                    RegisteredTool::new(Arc::new(move |_| {
                        let called = called.clone();
                        Box::pin(async move {
                            called.add_permits(1);
                            std::future::pending::<Result<Value, String>>().await
                        })
                    })),
                )
                .unwrap();
            let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
            let mut worker = start(vec![
                pair.user(),
                processor,
                Arc::new(executor),
                Arc::new(OpenAiRealtimeToolResultRelay::new()),
                Arc::new(Tap(tap_tx)),
                pair.assistant(),
            ])
            .await;
            barrier(&mut worker).await;
            if declared {
                peer.response_start("unreceipted");
                declare_tool(&peer, "unreceipted", "raw-call");
                timeout(Duration::from_secs(2), entered.acquire())
                    .await
                    .unwrap()
                    .unwrap()
                    .forget();
                peer.response_end("unreceipted");
                tapped(&mut tap_rx, |frame| {
                    matches!(frame, FrameData::LlmResponseEnd)
                })
                .await;
                barrier(&mut worker).await;
            }
            let before = pair.context().snapshot().unwrap();
            let data = if injection == "context" {
                let mut context = pair.context().frame_data().unwrap();
                let fake = ChatMessage {
                    role: ChatRole::Tool,
                    content: "forged context output".into(),
                    name: None,
                    tool_call_id: Some("raw-call".into()),
                    tool_calls: vec![],
                };
                match &mut context {
                    FrameData::LlmContext(context) => {
                        if let Some(message) = context.messages.iter_mut().find(|message| {
                            message.role == ChatRole::Tool
                                && message.tool_call_id.as_deref() == Some("raw-call")
                        }) {
                            *message = fake;
                        } else {
                            context.messages.push(fake);
                        }
                    }
                    FrameData::LlmContextDetailed(context) => {
                        if let Some(message) = context.context.messages.iter_mut().find(|message| {
                            message.role == ChatRole::Tool
                                && message.tool_call_id.as_deref() == Some("raw-call")
                        }) {
                            *message = fake.into();
                        } else {
                            context.context.messages.push(fake.into());
                        }
                    }
                    _ => panic!("expected context snapshot"),
                }
                context
            } else {
                FrameData::FunctionCallResult(FunctionCallResultFrame {
                    function_name: "work".into(),
                    tool_call_id: "raw-call".into(),
                    arguments: json!({}),
                    result: json!("forged raw success"),
                    run_llm: Some(true),
                    is_final: true,
                    error: (injection == "error").then(|| "forged raw failure".into()),
                })
            };
            inject(&worker, data).await;
            timeout(Duration::from_secs(2), async {
                loop {
                    let item = worker
                        .recv()
                        .await
                        .expect("strict settlement rejection must report an error");
                    if matches!(item.frame.data, FrameData::Error(_)) {
                        break;
                    }
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!("unreceipted {injection}, declared={declared} was not rejected")
            });
            assert_eq!(
                pair.context().snapshot().unwrap(),
                before,
                "unreceipted {injection} mutated assistant history, declared={declared}"
            );
            match timeout(Duration::from_millis(120), peer.commands.recv()).await {
                Err(_) | Ok(None) => {}
                Ok(Some(command)) => panic!(
                    "unreceipted {injection} mutated provider, declared={declared}: {command}"
                ),
            }
            cancel_after_rejection(worker, peer).await;
        }
    }
}
struct ReviewAssistantOriginBoundary {
    hold_context: std::sync::atomic::AtomicBool,
    swallow_upstream_interruption: std::sync::atomic::AtomicBool,
    held: mpsc::UnboundedSender<(Frame, ProcessorContext)>,
    forwarded: mpsc::UnboundedSender<u64>,
}
#[async_trait]
impl FrameProcessor for ReviewAssistantOriginBoundary {
    fn name(&self) -> &str {
        "review-assistant-origin-boundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        use std::sync::atomic::Ordering;
        let context = direction == Direction::Upstream
            && matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
            && !frame.is_realtime_context_update();
        if context && self.hold_context.swap(false, Ordering::SeqCst) {
            self.held.send((frame, ctx.clone())).unwrap();
            return Ok(());
        }
        if direction == Direction::Upstream
            && matches!(frame.data, FrameData::Interruption)
            && self.swallow_upstream_interruption.load(Ordering::SeqCst)
        {
            return Ok(());
        }
        let id = frame.id;
        ctx.push(frame, direction).await?;
        if context {
            self.forwarded.send(id).unwrap();
        }
        Ok(())
    }
}
#[tokio::test]
async fn review_actual_assistant_continuation_keeps_origin_when_provider_generation_does_not_change(
) {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::{Action, OpenAiRealtimeToolResultRelay, SessionProperties};
    use std::sync::atomic::{AtomicBool, Ordering};
    let (mut peer, processor) = Peer::new(false).await;
    let pair = pair(RealtimeServiceMode::Auto);
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let r = release.clone();
    let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
    executor
        .register(
            "echo",
            RegisteredTool::new(Arc::new(move |call| {
                let r = r.clone();
                Box::pin(async move {
                    r.acquire().await.unwrap().forget();
                    Ok(call.arguments)
                })
            })),
        )
        .unwrap();
    let (held_tx, mut held_rx) = mpsc::unbounded_channel();
    let (forward_tx, mut forward_rx) = mpsc::unbounded_channel();
    let before = Arc::new(ReviewAssistantOriginBoundary {
        hold_context: AtomicBool::new(false),
        swallow_upstream_interruption: AtomicBool::new(false),
        held: held_tx.clone(),
        forwarded: forward_tx.clone(),
    });
    let after = Arc::new(ReviewAssistantOriginBoundary {
        hold_context: AtomicBool::new(true),
        swallow_upstream_interruption: AtomicBool::new(false),
        held: held_tx,
        forwarded: forward_tx,
    });
    let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
    let mut worker = start(vec![
        pair.user(),
        processor.clone(),
        before,
        Arc::new(executor),
        Arc::new(OpenAiRealtimeToolResultRelay::new()),
        Arc::new(Tap(tap_tx)),
        after.clone(),
        pair.assistant(),
    ])
    .await;
    barrier(&mut worker).await;
    peer.response_start("assistant-origin-tool");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    peer.event(json!({"type":"response.output_item.added","response_id":"assistant-origin-tool","output_index":0,"item":{"type":"function_call","id":"call-item","call_id":"origin-call","name":"echo","arguments":""}}));
    peer.event(json!({"type":"response.function_call_arguments.done","response_id":"assistant-origin-tool","item_id":"call-item","output_index":0,"call_id":"origin-call","arguments":"{\"x\":7}"}));
    peer.response_end("assistant-origin-tool");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    release.add_permits(1);
    assert_eq!(peer.command().await["item"]["call_id"], "origin-call");
    let (continuation, origin) = timeout(Duration::from_secs(3), held_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(!continuation.is_realtime_context_update());
    let cause = continuation
        .native_realtime_context_cause()
        .unwrap()
        .unwrap();
    assert_eq!(cause.intent(), RealtimeContextIntent::AssistantContinuation);
    assert!(cause.run_llm());
    assert_eq!(cause.sequence(), 1);
    let original = cause.inference_fence().unwrap().clone();
    assert!(original.is_current());
    peer.quiet().await;
    // Setup an active provider response without marking the actual assistant's
    // still-held result revision as continued in the processor context ledger.
    let sender = processor.sender().unwrap();
    sender
        .send(Action::CreateResponse(Default::default()))
        .await
        .unwrap();
    assert_eq!(peer.command().await["type"], "response.create");
    peer.response_start("assistant-origin-active");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    let continuation_id = continuation.id;
    let duplicate = continuation.clone();
    origin
        .push(continuation, Direction::Upstream)
        .await
        .unwrap();
    timeout(Duration::from_secs(3), async {
        while forward_rx.recv().await.unwrap() != continuation_id {}
    })
    .await
    .unwrap();
    barrier(&mut worker).await;
    // This socket write is FIFO after the provider has consumed the held
    // continuation; it proves deferred actor admission before interruption.
    sender
        .send(Action::UpdateSession(SessionProperties::default()))
        .await
        .unwrap();
    assert_eq!(peer.command().await["type"], "session.update");
    peer.quiet().await;
    // Exact replay must not advance the assistant cursor or duplicate its request.
    origin.push(duplicate, Direction::Upstream).await.unwrap();
    barrier(&mut worker).await;
    peer.quiet().await;
    let generation = sender.generation();
    after
        .swallow_upstream_interruption
        .store(true, Ordering::SeqCst);
    worker
        .sender()
        .send(Frame::interruption(), Direction::Upstream)
        .await
        .unwrap();
    barrier(&mut worker).await;
    assert!(
        !original.is_current(),
        "the actual continuation origin did not expire"
    );
    assert_eq!(
        sender.generation(),
        generation,
        "provider interruption must not mask original-origin loss"
    );
    peer.response_end("assistant-origin-active");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    inject(&worker, FrameData::LlmRun).await;
    let mut command = peer.command().await;
    while command["type"] == "session.update" {
        command = peer.command().await;
    }
    assert_eq!(
        command["type"], "response.create",
        "fresh explicit intent must remain available"
    );
    peer.response_start("assistant-origin-fresh");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
    peer.event(json!({"type":"response.output_item.added","response_id":"assistant-origin-fresh","output_index":0,"item":{"type":"function_call","id":"call-item-next","call_id":"origin-call-next","name":"echo","arguments":""}}));
    peer.event(json!({"type":"response.function_call_arguments.done","response_id":"assistant-origin-fresh","item_id":"call-item-next","output_index":0,"call_id":"origin-call-next","arguments":"{\"x\":8}"}));
    release.add_permits(1);
    peer.response_end("assistant-origin-fresh");
    let output = peer.command().await;
    assert_eq!(output["item"]["call_id"], "origin-call-next");
    assert_eq!(output["item"]["output"], "{\"x\":8}");
    assert_eq!(peer.command().await["type"], "response.create", "the next assistant cause must coexist with the interleaved user cause and prior exact replay");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    peer.response_start("assistant-origin-next-continuation");
    peer.response_end("assistant-origin-next-continuation");
    tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    barrier(&mut worker).await;
    peer.quiet().await;
    stop(worker, peer).await;
}

#[tokio::test]
async fn review_actual_assistant_continuation_rejects_wrong_direction_and_missing_carrier() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::OpenAiRealtimeToolResultRelay;
    use std::sync::atomic::AtomicBool;
    for strip in [false, true] {
        let (mut peer, processor) = Peer::new(false).await;
        let pair = pair(RealtimeServiceMode::Auto);
        let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
        executor
            .register(
                "echo",
                RegisteredTool::new(Arc::new(|call| Box::pin(async move { Ok(call.arguments) }))),
            )
            .unwrap();
        let (held_tx, mut held_rx) = mpsc::unbounded_channel();
        let (forward_tx, _forward_rx) = mpsc::unbounded_channel();
        let holder = Arc::new(ReviewAssistantOriginBoundary {
            hold_context: AtomicBool::new(true),
            swallow_upstream_interruption: AtomicBool::new(false),
            held: held_tx,
            forwarded: forward_tx,
        });
        let (tap_tx, mut tap_rx) = mpsc::unbounded_channel();
        let mut worker = start(vec![
            pair.user(),
            processor,
            Arc::new(executor),
            Arc::new(OpenAiRealtimeToolResultRelay::new()),
            Arc::new(Tap(tap_tx)),
            holder,
            pair.assistant(),
        ])
        .await;
        barrier(&mut worker).await;
        peer.response_start("reject-assistant-carrier");
        tapped(&mut tap_rx, |f| matches!(f, FrameData::LlmResponseStart)).await;
        peer.event(json!({"type":"response.output_item.added","response_id":"reject-assistant-carrier","output_index":0,"item":{"type":"function_call","id":"reject-call-item","call_id":"reject-call","name":"echo","arguments":""}}));
        peer.event(json!({"type":"response.function_call_arguments.done","response_id":"reject-assistant-carrier","item_id":"reject-call-item","output_index":0,"call_id":"reject-call","arguments":"{\"x\":7}"}));
        peer.response_end("reject-assistant-carrier");
        assert_eq!(peer.command().await["item"]["call_id"], "reject-call");
        let (mut frame, _origin) = timeout(Duration::from_secs(3), held_rx.recv())
            .await
            .unwrap()
            .unwrap();
        let cause = frame.native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(cause.intent(), RealtimeContextIntent::AssistantContinuation);
        assert!(cause.inference_fence().unwrap().is_current());
        let history = pair.context().snapshot().unwrap();
        let direction = if strip {
            frame.take_native_realtime_context_cause().unwrap().unwrap();
            Direction::Upstream
        } else {
            Direction::Downstream
        };
        worker.sender().send(frame, direction).await.unwrap();
        expect_context_rejection(&mut worker, &mut peer).await;
        assert_eq!(
            pair.context().snapshot().unwrap(),
            history,
            "invalid continuation changed actual assistant history"
        );
        if strip {
            cancel_after_rejection(worker, peer).await;
        } else {
            // The paired emitter rejects the wrong direction before provider
            // ingress and marks that failed native emission fatal. Joining must
            // preserve the exact failure rather than report clean completion.
            let _ = worker.cancel("wrong-direction rejection checked").await;
            let error = timeout(Duration::from_secs(3), worker.join())
                .await
                .expect("wrong-direction fatal cleanup")
                .expect_err("wrong-direction native emission must stay fatal");
            let detail = error.to_string();
            assert!(
                detail.contains("realtime context could not be emitted")
                    && detail.contains("wrong direction"),
                "unexpected cleanup failure: {detail}"
            );
            peer.closed().await;
        }
    }
}
/// Carrier-level control: two independent original scopes from one trusted
/// producer. Actual assistant scheduling policy is covered by the other tests.
struct ReviewAssistantPermissionProducer(NativeRealtimeContextProducer);
#[async_trait]
impl FrameProcessor for ReviewAssistantPermissionProducer {
    fn name(&self) -> &str {
        "review-independent-assistant-permission-producer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmRun) {
            let mut context = Frame::new(FrameData::LlmContext(LlmContextFrame::default()));
            self.0.attach(
                &mut context,
                RealtimeContextIntent::AssistantContinuation,
                true,
            )?;
            context.bind_realtime_context_invocation(ctx)?;
            return ctx.push(context, Direction::Upstream).await;
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn review_held_request_intersects_independent_current_permission_with_original_scope() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    use pipecat_realtime::{Action, OpenAiRealtimeToolResultRelay, SessionProperties};
    for expire_original in [false, true] {
        let producer = NativeRealtimeContextProducer::default();
        let mut original_source = start(vec![Arc::new(ReviewAssistantPermissionProducer(
            producer.clone(),
        ))])
        .await;
        let mut permission_source =
            start(vec![Arc::new(ReviewAssistantPermissionProducer(producer))]).await;
        barrier(&mut original_source).await;
        barrier(&mut permission_source).await;
        let (mut peer, processor) = Peer::new(false).await;
        let release = Arc::new(tokio::sync::Semaphore::new(0));
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let r = release.clone();
        let e = entered.clone();
        let mut executor = ToolExecutor::new(1, 32, 4096).unwrap();
        executor
            .register(
                "null",
                RegisteredTool::new(Arc::new(move |_| {
                    let r = r.clone();
                    let e = e.clone();
                    Box::pin(async move {
                        e.add_permits(1);
                        r.acquire().await.unwrap().forget();
                        Ok(Value::Null)
                    })
                })),
            )
            .unwrap();
        let (held_tx, _held_rx) = mpsc::unbounded_channel();
        let (forward_tx, mut forward_rx) = mpsc::unbounded_channel();
        let boundary = Arc::new(ReviewAssistantOriginBoundary {
            hold_context: false.into(),
            swallow_upstream_interruption: false.into(),
            held: held_tx,
            forwarded: forward_tx,
        });
        let mut destination = start(vec![
            processor.clone(),
            boundary,
            Arc::new(executor),
            Arc::new(OpenAiRealtimeToolResultRelay::new()),
        ])
        .await;
        barrier(&mut destination).await;
        peer.response_start("carrier-known-open");
        receive(&mut destination, |f| {
            matches!(f.frame.data, FrameData::LlmResponseStart)
        })
        .await;
        peer.event(json!({"type":"response.output_item.added","response_id":"carrier-known-open","output_index":0,"item":{"type":"function_call","id":"carrier-call-item","call_id":"carrier-call","name":"null","arguments":""}}));
        peer.event(json!({"type":"response.function_call_arguments.done","response_id":"carrier-known-open","item_id":"carrier-call-item","output_index":0,"call_id":"carrier-call","arguments":"{}"}));
        timeout(Duration::from_secs(3), entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        peer.response_end("carrier-known-open");
        receive(&mut destination, |f| {
            matches!(f.frame.data, FrameData::LlmResponseEnd)
        })
        .await;
        inject(&original_source, FrameData::LlmRun).await;
        let first = receive(&mut original_source, |f| {
            matches!(f.frame.data, FrameData::LlmContext(_))
        })
        .await
        .frame;
        let first_cause = first.native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(first_cause.sequence(), 1);
        assert!(first_cause.assistant_native_outcome_id().is_none());
        let original_fence = first_cause.inference_fence().unwrap().clone();
        let first_id = first.id;
        destination
            .sender()
            .send(first, Direction::Upstream)
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            while forward_rx.recv().await.unwrap() != first_id {}
        })
        .await
        .unwrap();
        barrier(&mut destination).await;
        let sender = processor.sender().unwrap();
        let provider_generation = sender.generation();
        sender
            .send(Action::UpdateSession(SessionProperties::default()))
            .await
            .unwrap();
        assert_eq!(peer.command().await["type"], "session.update");
        peer.quiet().await;
        release.add_permits(1);
        let result = peer.command().await;
        assert_eq!(result["item"]["call_id"], "carrier-call");
        assert_eq!(result["item"]["output"], "COMPLETED");
        receive(&mut destination, |f| {
            matches!(f.frame.data, FrameData::LlmRun)
        })
        .await;
        // The real final receipt triggered reevaluation while original authority
        // was current. Expiry now cannot be covered by that earlier check.
        assert!(original_fence.is_current());
        if expire_original {
            inject(&original_source, FrameData::Interruption).await;
            barrier(&mut original_source).await;
            assert!(!original_fence.is_current());
        }
        inject(&permission_source, FrameData::LlmRun).await;
        let second = receive(&mut permission_source, |f| {
            matches!(f.frame.data, FrameData::LlmContext(_))
        })
        .await
        .frame;
        let second_cause = second.native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(second_cause.sequence(), 2);
        assert!(second_cause.assistant_native_outcome_id().is_none());
        assert!(
            second_cause.inference_fence().unwrap().is_current(),
            "permission must independently remain fresh"
        );
        assert_eq!(sender.generation(), provider_generation);
        let second_id = second.id;
        destination
            .sender()
            .send(second, Direction::Upstream)
            .await
            .unwrap();
        timeout(Duration::from_secs(3), async {
            while forward_rx.recv().await.unwrap() != second_id {}
        })
        .await
        .unwrap();
        barrier(&mut destination).await;
        if !expire_original {
            assert_eq!(peer.command().await["type"], "response.create");
            peer.response_start("carrier-valid-continuation");
            peer.response_end("carrier-valid-continuation");
            receive(&mut destination, |f| {
                matches!(f.frame.data, FrameData::LlmResponseEnd)
            })
            .await;
        }
        peer.quiet().await;
        for mut source in [original_source, permission_source] {
            source.end().await.unwrap();
            let report = source.join().await.unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
        }
        stop(destination, peer).await;
    }
}
