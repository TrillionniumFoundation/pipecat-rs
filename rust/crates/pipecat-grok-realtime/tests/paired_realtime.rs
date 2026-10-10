//! Actual paired user → Grok → executor → relay → output → assistant loopback tests.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_grok_realtime::*;
use pipecat_media_transport::{
    OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt, SpokenContext,
    SpokenHistory, SpokenHistoryConfig, SpokenHistorySink,
};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{Context, Role as ContextRole};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::sync::{
    atomic::{AtomicBool, AtomicU8, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
type Ws = WebSocketStream<TcpStream>;
const WAIT: Duration = Duration::from_secs(5);
async fn wire(ws: &mut Ws, mut value: Value) {
    static EVENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    value["event_id"] = json!(format!(
        "paired-event-{}",
        EVENT.fetch_add(1, Ordering::Relaxed)
    ));
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn read(ws: &mut Ws) -> Value {
    let message = timeout(WAIT, ws.next()).await.unwrap().unwrap().unwrap();
    assert!(
        message.is_text(),
        "expected provider command, got {message:?}"
    );
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(Duration::from_millis(50), ws.next()).await.is_err(),
        "unexpected provider command"
    );
}
async fn response(ws: &mut Ws, id: &str, done: bool) {
    wire(ws,json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
#[derive(Default)]
struct Probe {
    frames: Mutex<Vec<DirectedFrame>>,
    hold_next: AtomicBool,
    held: Mutex<Option<Frame>>,
    corrupt_batch: AtomicU8,
    hold_raw_result: AtomicBool,
    hold_invalid_batch: AtomicBool,
    inject: Mutex<Option<Frame>>,
    inject_direction: Mutex<Option<Direction>>,
    inject_assistant: AtomicBool,
    inject_strip_cause: AtomicBool,
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "PairedBoundary"
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let inject = if matches!(&frame.data, FrameData::Flush(flush) if flush.origin.as_deref() == Some("paired-inject"))
        {
            self.inject.lock().unwrap().take()
        } else {
            None
        };
        if let Some(mut inject) = inject {
            if self.inject_strip_cause.swap(false, Ordering::SeqCst) {
                inject.take_native_realtime_context_cause()?;
            }
            if self.inject_assistant.swap(false, Ordering::SeqCst) {
                inject.take_native_realtime_context_cause()?;
                NativeRealtimeContextProducer::default().attach(
                    &mut inject,
                    RealtimeContextIntent::AssistantContinuation,
                    true,
                )?;
                inject.bind_realtime_context_invocation(ctx)?;
            }
            let direction = self
                .inject_direction
                .lock()
                .unwrap()
                .take()
                .unwrap_or(direction);
            return ctx.push(inject, direction).await;
        }
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        if self.hold_next.load(Ordering::SeqCst)
            && frame.native_realtime_context_cause()?.is_some()
            && self.hold_next.swap(false, Ordering::SeqCst)
        {
            *self.held.lock().unwrap() = Some(frame);
            return Ok(());
        }
        if self.hold_invalid_batch.load(Ordering::SeqCst) && frame.native_tool_batch().is_err() {
            *self.held.lock().unwrap() = Some(frame);
            return Ok(());
        }
        if self.hold_raw_result.load(Ordering::SeqCst)
            && matches!(
                frame.data,
                FrameData::FunctionCallResult(_) | FrameData::FunctionCallResultDetailed(_)
            )
            && frame.native_tool_outcome()?.is_none()
        {
            *self.held.lock().unwrap() = Some(frame);
            return Ok(());
        }
        if self.corrupt_batch.load(Ordering::SeqCst) != 0 && frame.native_tool_batch()?.is_some() {
            let attack = self.corrupt_batch.swap(0, Ordering::SeqCst);
            if attack != 0 {
                frame.data = forged_batch_payload(attack);
            }
        }
        ctx.push(frame, direction).await
    }
}
fn forged_batch_payload(attack: u8) -> FrameData {
    if attack == 1 {
        FrameData::InputAudio(AudioRawFrame {
            audio: vec![0, 0],
            sample_rate: 24000,
            num_channels: 1,
            context_id: None,
        })
    } else {
        FrameData::LlmSetTools(ToolSet::Functions(vec![]))
    }
}
struct Harness {
    worker: PipelineWorker,
    ws: Ws,
    pair: LLMContextAggregatorPair,
    before: Arc<Probe>,
    after: Arc<Probe>,
    at_relay: Arc<Probe>,
    after_provider: Arc<Probe>,
    after_cursor: usize,
}
impl Harness {
    async fn new(context: Context, spoken: bool) -> Self {
        Self::with_turns(context, spoken, true, false).await
    }
    async fn with_turns(
        context: Context,
        spoken: bool,
        server_vad: bool,
        interruptions: bool,
    ) -> Self {
        Self::with_output(context, spoken, server_vad, interruptions, None).await
    }
    async fn with_output(
        context: Context,
        spoken: bool,
        server_vad: bool,
        interruptions: bool,
        backend: Option<Arc<dyn OutputBackend>>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("offline-paired-fixture");
        config.endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
        config.limits.connect_timeout = WAIT;
        config.session.turn_detection = Some(if server_vad {
            Some(TurnDetection::server_vad())
        } else {
            None
        });
        config.session.tools = Some(vec![FunctionTool::new(
            "echo",
            "Echo",
            json!({"type":"object"}),
        )
        .into()]);
        let provider =
            Arc::new(GrokRealtimeProcessor::new(RealtimeClient::new(config).unwrap()).unwrap());
        let relay = provider.tool_result_relay();
        let mut user = UserAggregatorConfig::default();
        user.turn.enable_interruptions = interruptions;
        let pair = LLMContextAggregatorPair::new(
            context,
            user,
            AssistantAggregatorConfig {
                text_mode: if spoken {
                    AssistantTextMode::Spoken
                } else {
                    AssistantTextMode::Text
                },
            },
        );
        let before = Arc::new(Probe::default());
        let after = Arc::new(Probe::default());
        let at_relay = Arc::new(Probe::default());
        let after_provider = Arc::new(Probe::default());
        let mut pipeline = PipelineConfig::default();
        pipeline.start.audio_in_sample_rate = 24000;
        pipeline.start.audio_out_sample_rate = 24000;
        let mut executor = ToolExecutor::new(2, 32, 4096).unwrap();
        executor
            .register(
                "echo",
                RegisteredTool::new(Arc::new(|call| {
                    Box::pin(async move {
                        if call.arguments.get("hold").and_then(Value::as_bool) == Some(true) {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                        }
                        Ok(call.arguments)
                    })
                })),
            )
            .unwrap();
        let mut stages: Vec<Arc<dyn FrameProcessor>> = vec![
            pair.user(),
            before.clone(),
            provider,
            after_provider.clone(),
            Arc::new(executor),
            at_relay.clone(),
            Arc::new(relay),
        ];
        if let Some(backend) = backend {
            let history = SpokenHistory::new(
                SpokenHistoryConfig::default(),
                Arc::new(AssistantReceiptSink(pair.assistant())),
            )
            .unwrap();
            stages.push(Arc::new(
                history.source_boundary("paired-spoken-source").unwrap(),
            ));
            stages.push(Arc::new(
                OutputTransport::new(
                    "paired-output",
                    OutputTransportConfig {
                        sample_rate: Some(24000),
                        chunk_duration: Duration::from_millis(10),
                        auto_silence: false,
                        end_silence: Duration::ZERO,
                        write_timeout: WAIT,
                        ..Default::default()
                    },
                    backend,
                    None,
                    BTreeMap::new(),
                )
                .unwrap()
                .with_spoken_history(history)
                .unwrap(),
            ));
        }
        stages.push(after.clone());
        stages.push(pair.assistant());
        let handshake = async {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            wire(
                &mut ws,
                json!({"type":"conversation.created","conversation":{"id":"c"}}),
            )
            .await;
            assert_eq!(read(&mut ws).await["type"], "session.update");
            wire(
                &mut ws,
                json!({"type":"session.updated","session":{"id":"s"}}),
            )
            .await;
            ws
        };
        let (worker, ws) = tokio::join!(Pipeline::new(stages).start(pipeline), handshake);
        let mut result = Self {
            worker: worker.unwrap(),
            ws,
            pair,
            before,
            after,
            at_relay,
            after_provider,
            after_cursor: 0,
        };
        result
            .receive(|frame| matches!(frame, FrameData::LlmServiceMetadata(_)))
            .await;
        result.barrier().await;
        assert!(result.pair.user().realtime_service_enabled().unwrap());
        assert!(result.pair.assistant().realtime_service_enabled().unwrap());
        result
    }
    async fn send(&self, data: FrameData) {
        self.worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    async fn receive(&mut self, wanted: impl Fn(&FrameData) -> bool) -> DirectedFrame {
        timeout(WAIT, async {
            loop {
                let frame = self.worker.recv().await.unwrap();
                if let FrameData::Error(error) = &frame.frame.data {
                    panic!("{error:?}");
                }
                if wanted(&frame.frame.data) {
                    return frame;
                }
            }
        })
        .await
        .unwrap()
    }
    async fn observe(&mut self, wanted: impl Fn(&FrameData) -> bool) {
        timeout(WAIT, async {
            loop {
                {
                    let frames = self.after.frames.lock().unwrap();
                    while self.after_cursor < frames.len() {
                        let frame = &frames[self.after_cursor];
                        self.after_cursor += 1;
                        if let FrameData::Error(error) = &frame.frame.data {
                            panic!("{error:?}");
                        }
                        if wanted(&frame.frame.data) {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .unwrap();
        self.barrier().await;
    }
    async fn barrier(&mut self) {
        self.send(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("paired-barrier".into()),
        }))
        .await;
        self.receive(|frame| matches!(frame, FrameData::Flush(_)))
            .await;
    }
    async fn finish(self) {
        self.finish_with_errors(0).await;
    }
    async fn finish_with_errors(mut self, expected_errors: usize) {
        self.worker.end().await.unwrap();
        let close = async {
            loop {
                match timeout(WAIT, self.ws.next()).await.unwrap() {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    other => panic!("unexpected shutdown {other:?}"),
                }
            }
            let _ = self.ws.close(None).await;
        };
        let (result, ()) = tokio::join!(self.worker.collect(), close);
        let (_, report) = result.unwrap();
        assert_eq!(report.errors.len(), expected_errors, "{:?}", report.errors);
        assert!(report.errors.iter().all(|error| !error.fatal));
    }
}
#[tokio::test]
async fn paired_repeat_identical_input_and_run_are_explicit_once_each() {
    let mut h = Harness::new(Context::default(), false).await;
    for index in 0..2 {
        h.send(FrameData::InputText(TextFrame::new("same"))).await;
        if index == 0 {
            assert_eq!(read(&mut h.ws).await["type"], "session.update");
        }
        let item = read(&mut h.ws).await;
        assert_eq!(item["type"], "conversation.item.create");
        assert_eq!(item["item"]["content"][0]["text"], "same");
        assert_eq!(read(&mut h.ws).await["type"], "response.create");
        let id = format!("r{index}");
        response(&mut h.ws, &id, false).await;
        response(&mut h.ws, &id, true).await;
        h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
            .await;
        quiet(&mut h.ws).await;
    }
    for index in 2..4 {
        h.send(FrameData::LlmRun).await;
        assert_eq!(read(&mut h.ws).await["type"], "response.create");
        let id = format!("r{index}");
        response(&mut h.ws, &id, false).await;
        response(&mut h.ws, &id, true).await;
        h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
            .await;
        quiet(&mut h.ws).await;
    }
    assert_eq!(
        h.pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.role == ContextRole::User)
            .count(),
        2
    );
    h.finish().await;
}
#[tokio::test]
async fn paired_audio_only_marked_first_context_never_seeds_or_infers() {
    let mut h = Harness::new(Context::default(), true).await;
    response(&mut h.ws, "audio", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.updated","item_id":"u","transcript":"interim"})).await;
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u","transcript":"heard user"})).await;
    wire(&mut h.ws,json!({"type":"response.output_audio.delta","response_id":"audio","item_id":"a","output_index":0,"content_index":0,"delta":"AQI="})).await;
    wire(&mut h.ws,json!({"type":"response.output_audio_transcript.delta","response_id":"audio","item_id":"a","delta":"unplayed assistant"})).await;
    response(&mut h.ws, "audio", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    let history = h.pair.context().snapshot().unwrap();
    assert_eq!(history.messages.len(), 1);
    assert_eq!(history.messages[0].role, ContextRole::User);
    {
        let observed = h.before.frames.lock().unwrap();
        assert_eq!(
            observed
                .iter()
                .filter(|frame| frame.frame.is_realtime_context_update())
                .count(),
            1
        );
    }
    h.pair
        .assistant()
        .confirm_playback(
            "audio",
            &[AggregatedTextFrame {
                context_id: Some("audio".into()),
                text: TextFrame::new("unplayed assistant"),
                aggregated_by: "delivered-output-fixture".into(),
                raw_text: None,
                will_be_spoken: true,
            }],
        )
        .unwrap();
    assert_eq!(
        h.pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.role == ContextRole::Assistant)
            .count(),
        1
    );
    quiet(&mut h.ws).await;
    h.send(FrameData::InputText(TextFrame::new("heard user")))
        .await;
    let item = read(&mut h.ws).await;
    assert_eq!(item["item"]["content"][0]["text"], "heard user");
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "typed", false).await;
    response(&mut h.ws, "typed", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.finish().await;
}
#[tokio::test]
async fn no_run_append_survives_trailing_snapshot_then_run_without_duplicate() {
    let mut h = Harness::new(Context::default(), false).await;
    h.send(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "explicit append")],
        run_llm: false,
    }))
    .await;
    assert_eq!(read(&mut h.ws).await["type"], "session.update");
    assert_eq!(
        read(&mut h.ws).await["item"]["content"][0]["text"],
        "explicit append"
    );
    quiet(&mut h.ws).await;
    response(&mut h.ws, "server", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u","transcript":"audio transcript"})).await;
    response(&mut h.ws, "server", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "run", false).await;
    response(&mut h.ws, "run", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    quiet(&mut h.ws).await;
    h.finish().await;
}

#[tokio::test]
async fn audio_only_native_tool_continuation_has_no_history_import() {
    let mut h = Harness::new(Context::default(), false).await;
    response(&mut h.ws, "tool", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":""}})).await;
    wire(&mut h.ws,json!({"type":"response.function_call_arguments.done","call_id":"call","name":"echo","arguments":"{\"n\":7}"})).await;
    let output = read(&mut h.ws).await;
    assert_eq!(output["type"], "conversation.item.create");
    assert_eq!(output["item"]["type"], "function_call_output");
    assert_eq!(output["item"]["output"], "{\"n\":7}");
    quiet(&mut h.ws).await;
    response(&mut h.ws, "tool", true).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "continuation", false).await;
    response(&mut h.ws, "continuation", true).await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    assert_eq!(
        h.pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .filter(|message| message.role == ContextRole::Tool)
            .count(),
        1
    );
    h.finish().await;
}
#[tokio::test]
async fn server_vad_pair_owns_one_interruption_and_no_local_commit_or_create() {
    let mut h = Harness::with_turns(Context::default(), false, true, true).await;
    response(&mut h.ws, "speaking", false).await;
    wire(
        &mut h.ws,
        json!({"type":"input_audio_buffer.speech_started","item_id":"u","audio_start_ms":0}),
    )
    .await;
    assert_eq!(read(&mut h.ws).await["type"], "response.cancel");
    response(&mut h.ws, "speaking", true).await;
    wire(
        &mut h.ws,
        json!({"type":"input_audio_buffer.speech_stopped","item_id":"u","audio_end_ms":500}),
    )
    .await;
    quiet(&mut h.ws).await;
    h.barrier().await;
    let starts = h
        .before
        .frames
        .lock()
        .unwrap()
        .iter()
        .filter(|frame| {
            frame.direction == Direction::Downstream
                && matches!(frame.frame.data, FrameData::UserStartedSpeaking)
        })
        .count();
    assert_eq!(starts, 1);
    h.finish().await;
}
#[tokio::test]
async fn manual_turn_real_stop_commits_once_and_typed_turn_does_not_commit_audio() {
    let mut h = Harness::with_turns(Context::default(), false, false, false).await;
    h.send(FrameData::UserStartedSpeaking).await;
    h.send(FrameData::UserStoppedSpeaking).await;
    assert_eq!(read(&mut h.ws).await["type"], "input_audio_buffer.commit");
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "manual", false).await;
    response(&mut h.ws, "manual", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.send(FrameData::InputText(TextFrame::new("typed"))).await;
    assert_eq!(read(&mut h.ws).await["item"]["content"][0]["text"], "typed");
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "typed", false).await;
    response(&mut h.ws, "typed", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    quiet(&mut h.ws).await;
    h.finish().await;
}
#[tokio::test]
async fn oversized_local_edit_fails_session_before_queued_audio_or_inference() {
    let mut h = Harness::new(Context::default(), false).await;
    h.send(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "x".repeat(1024 * 1024))],
        run_llm: false,
    }))
    .await;
    let _ = h
        .worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(AudioRawFrame {
                audio: vec![0, 0],
                sample_rate: 24000,
                num_channels: 1,
                context_id: None,
            })),
            Direction::Downstream,
        )
        .await;
    let _ = h
        .worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await;
    let peer = async {
        loop {
            match timeout(WAIT, h.ws.next()).await.unwrap() {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                other => panic!("divergent session emitted {other:?}"),
            }
        }
        let _ = h.ws.close(None).await;
    };
    let (result, ()) = tokio::join!(h.worker.collect(), peer);
    assert!(result.is_err(), "fatal divergence must end runtime");
    assert_eq!(h.pair.context().snapshot().unwrap().messages.len(), 1);
}

#[tokio::test]
async fn paired_ttfs_deadline_and_response_end_flush_only_state_snapshots() {
    let mut h = Harness::new(Context::default(), false).await;
    h.send(FrameData::SttMetadata(SttMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "test-stt".into(),
            user_turn_strategies: None,
        },
        ttfs_p99_latency: 0.02,
    }))
    .await;
    h.barrier().await;
    response(&mut h.ws, "late", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u1","transcript":"first final"})).await;
    timeout(WAIT, async {
        loop {
            if h.pair.context().snapshot().unwrap().messages.len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    quiet(&mut h.ws).await;
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u2","transcript":"second final"})).await;
    response(&mut h.ws, "late", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    assert_eq!(h.pair.context().snapshot().unwrap().messages.len(), 2);
    assert_eq!(
        h.before
            .frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| frame.frame.is_realtime_context_update())
            .count(),
        2
    );
    h.finish().await;
}

#[tokio::test]
async fn held_no_run_append_is_not_swallowed_by_newer_marked_snapshot() {
    let mut h = Harness::new(Context::default(), false).await;
    h.before.hold_next.store(true, Ordering::SeqCst);
    h.send(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "held explicit")],
        run_llm: false,
    }))
    .await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    response(&mut h.ws, "audio", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u","transcript":"later audio"})).await;
    response(&mut h.ws, "audio", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    let held = h.before.held.lock().unwrap().take().unwrap();
    h.worker
        .sender()
        .send(held.clone(), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(
        read(&mut h.ws).await["item"]["content"][0]["text"],
        "held explicit"
    );
    quiet(&mut h.ws).await;
    h.worker
        .sender()
        .send(held, Direction::Downstream)
        .await
        .unwrap();
    h.barrier().await;
    quiet(&mut h.ws).await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "run", false).await;
    response(&mut h.ws, "run", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.finish().await;
}
#[tokio::test]
async fn delayed_original_cause_keeps_history_but_never_revives_stale_inference() {
    let mut h = Harness::new(Context::default(), false).await;
    h.before.hold_next.store(true, Ordering::SeqCst);
    h.send(FrameData::InputText(TextFrame::new("durable")))
        .await;
    h.barrier().await;
    let held = h.before.held.lock().unwrap().take().unwrap();
    assert!(held
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .inference_fence()
        .unwrap()
        .is_current());
    h.send(FrameData::Interruption).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.cancel");
    assert!(!held
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .inference_fence()
        .unwrap()
        .is_current());
    h.worker
        .sender()
        .send(held, Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(read(&mut h.ws).await["type"], "session.update");
    assert_eq!(
        read(&mut h.ws).await["item"]["content"][0]["text"],
        "durable"
    );
    quiet(&mut h.ws).await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "fresh", false).await;
    response(&mut h.ws, "fresh", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    h.finish().await;
}

#[tokio::test]
async fn raw_matching_success_cannot_settle_a_native_admitted_tool() {
    let mut h = Harness::new(Context::default(), false).await;
    // Keep a missing-guard mutation from being masked by downstream assistant rejection.
    h.after.hold_raw_result.store(true, Ordering::SeqCst);
    response(&mut h.ws, "native", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":""}})).await;
    wire(&mut h.ws,json!({"type":"response.function_call_arguments.done","call_id":"call","name":"echo","arguments":"{\"hold\":true}"})).await;
    h.observe(|frame| matches!(frame, FrameData::FunctionCallInProgress(_)))
        .await;
    h.send(FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: "echo".into(),
        tool_call_id: "call".into(),
        arguments: json!({"hold":true}),
        result: json!("forged"),
        run_llm: Some(true),
        is_final: true,
        error: None,
    }))
    .await;
    let peer = async {
        loop {
            match timeout(WAIT, h.ws.next()).await.unwrap() {
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Text(text))) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    assert_ne!(value["type"], "response.create");
                    assert!(!value.to_string().contains("forged"));
                }
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                other => panic!("unexpected close {other:?}"),
            }
        }
        let _ = h.ws.close(None).await;
    };
    let (result, ()) = tokio::join!(h.worker.collect(), peer);
    assert!(result.is_err());
    assert!(!format!("{:?}", h.pair.context().snapshot().unwrap()).contains("forged"));
}

#[tokio::test]
async fn explicit_initial_context_seeds_once_and_live_replacement_fails_before_wire_mutation() {
    for run_llm in [false, true] {
        let initial = Context::new(vec![
            pipecat_services::Message::text(ContextRole::System, "instructions"),
            pipecat_services::Message::text(ContextRole::User, "initial history"),
        ]);
        let mut h = Harness::new(initial, false).await;
        h.send(FrameData::LlmRun).await;
        let update = read(&mut h.ws).await;
        assert_eq!(update["type"], "session.update");
        assert_eq!(update["session"]["instructions"], "instructions");
        assert_eq!(
            read(&mut h.ws).await["item"]["content"][0]["text"],
            "initial history"
        );
        assert_eq!(read(&mut h.ws).await["type"], "response.create");
        response(&mut h.ws, "seed", false).await;
        response(&mut h.ws, "seed", true).await;
        h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
            .await;
        quiet(&mut h.ws).await;
        h.send(FrameData::LlmMessagesUpdateDetailed(
            DetailedMessagesFrame {
                messages: vec![ChatMessage::new(ChatRole::User, "local replacement").into()],
                run_llm: Some(run_llm),
            },
        ))
        .await;
        let peer = async {
            loop {
                match timeout(WAIT, h.ws.next()).await.unwrap() {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    other => panic!("rejected replacement partially mutated provider: {other:?}"),
                }
            }
            let _ = h.ws.close(None).await;
        };
        let (result, ()) = tokio::join!(h.worker.collect(), peer);
        assert!(result.is_err());
        let history = h.pair.context().snapshot().unwrap();
        assert_eq!(history.messages.len(), 1);
        assert!(format!("{history:?}").contains("local replacement"));
    }
}

#[tokio::test]
async fn forged_native_batch_rejects_at_provider_and_relay_before_wire_effects() {
    for relay in [false, true] {
        for attack in [1, 2] {
            let mut h = Harness::new(Context::default(), false).await;
            // Downstream rejection must not mask a missing ingress guard.
            h.after_provider
                .hold_invalid_batch
                .store(!relay, Ordering::SeqCst);
            h.after.hold_invalid_batch.store(relay, Ordering::SeqCst);
            if relay {
                h.at_relay.corrupt_batch.store(attack, Ordering::SeqCst);
            }
            response(&mut h.ws, "native", false).await;
            wire(&mut h.ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"item","call_id":"call","name":"echo","arguments":""}})).await;
            wire(&mut h.ws,json!({"type":"response.function_call_arguments.done","call_id":"call","name":"echo","arguments":"{\"hold\":true}"})).await;
            if !relay {
                h.observe(|frame| matches!(frame, FrameData::FunctionCallInProgress(_)))
                    .await;
                let mut forged = h
                    .at_relay
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .find(|frame| frame.frame.native_tool_batch().unwrap().is_some())
                    .unwrap()
                    .frame
                    .clone();
                forged.data = forged_batch_payload(attack);
                h.worker
                    .sender()
                    .send(forged, Direction::Downstream)
                    .await
                    .unwrap();
            }
            let peer = async {
                loop {
                    match timeout(WAIT, h.ws.next()).await.unwrap() {
                        Some(Ok(Message::Close(_))) | None => break,
                        Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                        other => panic!("forged batch reached provider effects: {other:?}"),
                    }
                }
                let _ = h.ws.close(None).await;
            };
            let (result, ()) = tokio::join!(h.worker.collect(), peer);
            assert!(result.is_err());
        }
    }
}

#[tokio::test]
async fn conflicting_or_gapped_native_context_fails_before_session_history_or_inference() {
    for attack in [0, 1, 2, 3, 4, 5] {
        let mut h = Harness::new(Context::default(), false).await;
        h.before.hold_next.store(true, Ordering::SeqCst);
        h.send(FrameData::InputText(TextFrame::new("sealed"))).await;
        h.barrier().await;
        let mut held = h.before.held.lock().unwrap().take().unwrap();
        quiet(&mut h.ws).await;
        if attack == 2 {
            h.send(FrameData::LlmRun).await; // sequence 2 cannot overtake undelivered sequence 1
        } else {
            if attack == 0 {
                held.metadata
                    .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), json!(true));
            } else if attack == 1 {
                match &mut held.data {
                    FrameData::LlmContext(context) => context.messages[0].content = "forged".into(),
                    FrameData::LlmContextDetailed(context) => {
                        context.context.messages[0].content = MessageContent::Text("forged".into())
                    }
                    _ => unreachable!(),
                }
            }
            if attack == 3 || attack == 5 {
                h.after_provider
                    .inject_strip_cause
                    .store(attack == 5, Ordering::SeqCst);
                *h.after_provider.inject.lock().unwrap() = Some(held);
                *h.after_provider.inject_direction.lock().unwrap() = Some(Direction::Upstream);
            } else {
                *h.before.inject.lock().unwrap() = Some(held);
                h.before
                    .inject_assistant
                    .store(attack == 4, Ordering::SeqCst);
            }
            h.send(FrameData::Flush(FlushFrame {
                returning: false,
                origin: Some("paired-inject".into()),
            }))
            .await;
        }
        let peer = async {
            loop {
                match timeout(WAIT, h.ws.next()).await.unwrap() {
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    other => panic!("conflicting cause partially mutated provider: {other:?}"),
                }
            }
            let _ = h.ws.close(None).await;
        };
        let (result, ()) = tokio::join!(h.worker.collect(), peer);
        assert!(result.is_err());
        assert!(format!("{:?}", h.pair.context().snapshot().unwrap()).contains("sealed"));
    }
}

struct AssistantReceiptSink(Arc<LLMAssistantAggregator>);
impl SpokenHistorySink for AssistantReceiptSink {
    fn commit(&self, context: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        self.0
            .confirm_playback(&context.context_id, &context.texts)?
            .actions
            .into_iter()
            .map(|action| match action {
                AggregationAction::Push(frame) => Ok(frame),
                _ => Err(ProcessorError::new("unexpected playback effect")),
            })
            .collect()
    }
}
struct HeldPlayback {
    played: bool,
    entered: tokio::sync::Notify,
    release: tokio::sync::Semaphore,
}
#[async_trait]
impl OutputBackend for HeldPlayback {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.entered.notify_one();
        self.release.acquire().await.unwrap().forget();
        Ok(if self.played {
            PlaybackReceipt::Played
        } else {
            PlaybackReceipt::Discarded
        })
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
#[tokio::test]
async fn actual_output_playback_receipts_are_the_only_spoken_history_authority() {
    for played in [false, true] {
        let backend = Arc::new(HeldPlayback {
            played,
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Semaphore::new(0),
        });
        let mut h =
            Harness::with_output(Context::default(), true, true, false, Some(backend.clone()))
                .await;
        response(&mut h.ws, "spoken", false).await;
        wire(&mut h.ws,json!({"type":"response.output_audio.delta","response_id":"spoken","item_id":"a","output_index":0,"content_index":0,"delta":STANDARD.encode([1_u8,0].repeat(240))})).await;
        wire(&mut h.ws,json!({"type":"response.output_audio_transcript.delta","response_id":"spoken","item_id":"a","delta":"only delivered speech"})).await;
        response(&mut h.ws, "spoken", true).await;
        timeout(WAIT, backend.entered.notified()).await.unwrap();
        assert!(h.pair.context().snapshot().unwrap().messages.is_empty());
        quiet(&mut h.ws).await;
        backend.release.add_permits(1);
        h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
            .await;
        h.barrier().await;
        let history = h.pair.context().snapshot().unwrap();
        assert_eq!(history.messages.len(), usize::from(played), "{history:?}");
        if played {
            assert!(format!("{history:?}").contains("only delivered speech"));
        }
        quiet(&mut h.ws).await;
        h.finish().await;
    }
}

#[tokio::test]
async fn malformed_external_cause_reinjection_is_rejected_before_user_mutation_or_wire_effect() {
    let mut h = Harness::new(Context::default(), false).await;
    h.before.hold_next.store(true, Ordering::SeqCst);
    h.send(FrameData::InputText(TextFrame::new("retained original")))
        .await;
    h.barrier().await;
    let original = h.before.held.lock().unwrap().take().unwrap();
    let mut forged = original.clone();
    forged
        .metadata
        .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), json!(true));
    h.worker
        .sender()
        .send(forged, Direction::Downstream)
        .await
        .unwrap();
    let error = timeout(WAIT, async {
        loop {
            let frame = h.worker.recv().await.unwrap();
            if let FrameData::Error(error) = frame.frame.data {
                break error;
            }
        }
    })
    .await
    .unwrap();
    assert!(
        !error.fatal,
        "malformed incoming carrier is rejected before any new local edit"
    );
    assert!(error.error.contains("sealed frame"));
    h.barrier().await;
    quiet(&mut h.ws).await;
    assert_eq!(h.pair.context().snapshot().unwrap().messages.len(), 1);
    // Validation did not consume the original intent or reapply its local append.
    h.worker
        .sender()
        .send(original, Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(read(&mut h.ws).await["type"], "session.update");
    assert_eq!(
        read(&mut h.ws).await["item"]["content"][0]["text"],
        "retained original"
    );
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "valid-original", false).await;
    response(&mut h.ws, "valid-original", true).await;
    h.observe(|frame| matches!(frame, FrameData::LlmResponseEnd))
        .await;
    assert_eq!(h.pair.context().snapshot().unwrap().messages.len(), 1);
    h.finish_with_errors(1).await;
}
