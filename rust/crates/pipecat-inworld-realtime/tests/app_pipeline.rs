//! The public runnable application's exact constructor, real paired context,
//! real ToolExecutor and a synthetic TCP/WebSocket Inworld peer.
mod common;
use base64::{engine::general_purpose::STANDARD, Engine};
use common::*;
use pipecat_core::*;
use pipecat_inworld_realtime::*;
use pipecat_processors::tools::{RegisteredTool, ToolOutcomeJournal};
use pipecat_services::{Content, Context, Message as ContextMessage, Role as ContextRole};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{net::TcpListener, sync::Notify, time::timeout};

#[derive(Default)]
struct Probe(Mutex<Vec<DirectedFrame>>);
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "InworldAppOutputProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
struct Harness {
    worker: PipelineWorker,
    pair: LLMContextAggregatorPair,
    provider: Arc<InworldRealtimeProcessor>,
    outcomes: ToolOutcomeJournal,
    ws: Ws,
    probe: Arc<Probe>,
    cursor: usize,
    seeded: Option<Vec<Value>>,
    shutdown_budget: Duration,
}
impl Harness {
    async fn new(manual: bool, context: Context, tools: Vec<InworldLocalTool>) -> Self {
        let limits = RealtimeLimits {
            connect_timeout: WAIT,
            operation_timeout: WAIT,
            close_timeout: Duration::from_millis(250),
            ..Default::default()
        };
        Self::with_limits(manual, context, tools, limits).await
    }
    async fn with_limits(
        manual: bool,
        context: Context,
        tools: Vec<InworldLocalTool>,
        limits: RealtimeLimits,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = InworldConfig::new("synthetic-shared-app");
        config.endpoint = format!(
            "ws://{}/api/v1/realtime/session",
            listener.local_addr().unwrap()
        );
        config.session = if manual {
            SessionProperties::manual()
        } else {
            SessionProperties::pcm()
        };
        config.limits = limits;
        let probe = Arc::new(Probe::default());
        let app = InworldApp::new(config, context, tools, probe.clone()).unwrap();
        let shutdown_budget = app.pipeline_config.shutdown_timeout;
        let peer = async {
            let (stream, _) = bounded(listener.accept()).await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            let update = handshake(&mut ws).await;
            assert_eq!(
                update["session"]["audio"]["input"]["turn_detection"].is_null(),
                manual
            );
            // The runnable startup path seeds automatically, with no caller's
            // fixture-only LlmContext bootstrap and no speculative response.
            let seed_update = read(&mut ws).await;
            assert_eq!(seed_update["type"], "session.update");
            wire(
                &mut ws,
                json!({"type":"session.updated","session":seed_update["session"]}),
            )
            .await;
            let mut seeded = vec![seed_update];
            while let Ok(Some(Ok(message))) = timeout(
                Duration::from_millis(60),
                futures_util::StreamExt::next(&mut ws),
            )
            .await
            {
                assert!(message.is_text());
                let value: Value = serde_json::from_str(message.to_text().unwrap()).unwrap();
                assert_ne!(value["type"], "response.create", "seeding must not infer");
                assert_eq!(value["type"], "conversation.item.create");
                seeded.push(value);
            }
            (ws, seeded)
        };
        let (running, (ws, seeded)) = tokio::join!(app.start(), peer);
        let running = running.unwrap();
        let mut h = Self {
            worker: running.worker,
            pair: running.pair,
            provider: running.provider,
            outcomes: running.outcomes,
            ws,
            probe,
            cursor: 0,
            seeded: Some(seeded),
            shutdown_budget,
        };
        h.observe(|f| matches!(f, FrameData::LlmServiceMetadata(_)))
            .await;
        h.barrier().await;
        assert!(h.pair.user().realtime_service_enabled().unwrap());
        assert!(h.pair.assistant().realtime_service_enabled().unwrap());
        h
    }
    async fn send(&self, data: FrameData) {
        self.send_frame(Frame::new(data)).await;
    }
    async fn send_frame(&self, frame: Frame) {
        self.worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    async fn observe(&mut self, wanted: impl Fn(&FrameData) -> bool) {
        bounded(async {
            loop {
                {
                    let frames = self.probe.0.lock().unwrap();
                    while self.cursor < frames.len() {
                        let frame = &frames[self.cursor];
                        self.cursor += 1;
                        if let FrameData::Error(e) = &frame.frame.data {
                            panic!("unexpected app error: {e:?}");
                        }
                        if wanted(&frame.frame.data) {
                            return;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
    }
    async fn barrier(&mut self) {
        self.send(FrameData::Flush(FlushFrame {
            returning: false,
            origin: Some("inworld-test-barrier".into()),
        }))
        .await;
        bounded(async {
            loop {
                let frame=self.worker.recv().await.expect("pipeline closed before barrier");
                if matches!(&frame.frame.data,FrameData::Flush(f) if f.origin.as_deref()==Some("inworld-test-barrier")) {break;}
                if let FrameData::Error(e)=&frame.frame.data {panic!("unexpected app error: {e:?}");}
            }
        }).await;
    }
    async fn seed(&mut self) -> Vec<Value> {
        self.seeded.take().expect("startup is seeded exactly once")
    }
    async fn finish(self) {
        self.finish_with_errors(0).await;
    }
    async fn finish_with_errors(mut self, expected: usize) {
        self.worker.end().await.unwrap();
        let (report, ()) = tokio::join!(bounded(self.worker.collect()), closed(&mut self.ws));
        let (_, report) = report.unwrap();
        assert_eq!(report.errors.len(), expected, "{:?}", report.errors);
        assert!(self.outcomes.snapshot().active.is_empty());
        assert!(!self.provider.sender().is_ok_and(|sender| sender.is_ready()));
    }
}
fn history() -> Context {
    Context::new(vec![
        ContextMessage::text(ContextRole::System, "synthetic instructions"),
        ContextMessage::text(ContextRole::User, "old question"),
        ContextMessage::text(ContextRole::Assistant, "old answer"),
    ])
}
fn texts(h: &Harness, role: ContextRole) -> Vec<String> {
    h.pair
        .context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter(|m| m.role == role)
        .filter_map(|m| match m.content {
            Some(Content::Text(text)) => Some(text),
            _ => None,
        })
        .collect()
}
async fn spoken(ws: &mut Ws, response_id: &str, answer: &str) {
    wire(ws, audio_delta(response_id, "audio", &[1, 2, 3])).await;
    wire(ws, audio_delta(response_id, "audio", &[4, 5, 6])).await;
    wire(ws,json!({"type":"response.output_audio_transcript.delta","response_id":response_id,"item_id":"audio","delta":answer})).await;
    wire(
        ws,
        json!({"type":"response.output_audio.done","response_id":response_id,"item_id":"audio"}),
    )
    .await;
    response(ws, response_id, true).await;
}

#[tokio::test]
async fn shared_constructor_two_sessions_two_server_vad_turns_seed_once_no_duplicate_inference() {
    for session_index in 0..2 {
        let mut h = Harness::new(false, history(), vec![]).await;
        let seeded = h.seed().await;
        let items: Vec<_> = seeded
            .iter()
            .filter(|v| v["type"] == "conversation.item.create")
            .collect();
        assert_eq!(items.len(), 1);
        assert_eq!(items[0]["item"]["role"], "user");
        let packed = items[0]["item"]["content"][0]["text"].as_str().unwrap();
        assert!(packed.contains("old question") && packed.contains("old answer"));
        assert!(!packed.contains("synthetic instructions"));
        for turn in 0..2 {
            let pcm: Vec<u8> = (0..2880).map(|i| ((i + turn) % 251) as u8).collect();
            h.send_frame(pcm_frame(pcm[..960].to_vec())).await;
            h.barrier().await;
            quiet(&mut h.ws).await;
            let mut other = Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                audio: AudioRawFrame {
                    audio: pcm[960..].to_vec(),
                    sample_rate: 24000,
                    num_channels: 1,
                    context_id: Some("capture".into()),
                },
                user_id: "local-microphone-only".into(),
            }));
            other
                .metadata
                .insert("route".into(), json!("synthetic-route"));
            h.send_frame(other).await;
            let append = read(&mut h.ws).await;
            assert_eq!(append["type"], "input_audio_buffer.append");
            assert_eq!(
                STANDARD.decode(append["audio"].as_str().unwrap()).unwrap(),
                pcm
            );
            assert!(append.get("user_id").is_none());
            let user = format!("user-{session_index}-{turn}");
            let response_id = format!("r-{turn}");
            wire(
                &mut h.ws,
                json!({"type":"input_audio_buffer.speech_started","item_id":user}),
            )
            .await;
            h.observe(|f| matches!(f, FrameData::UserStartedSpeaking))
                .await;
            h.barrier().await;
            wire(
                &mut h.ws,
                json!({"type":"input_audio_buffer.speech_stopped","item_id":user}),
            )
            .await;
            h.observe(|f| matches!(f, FrameData::UserStoppedSpeaking))
                .await;
            h.barrier().await;
            response(&mut h.ws, &response_id, false).await;
            wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.delta","item_id":user,"delta":"heard "})).await;
            wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":user,"transcript":format!("heard {turn}")})).await;
            spoken(&mut h.ws, &response_id, &format!("answer {turn}")).await;
            h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
            h.barrier().await;
            quiet(&mut h.ws).await;
        }
        assert_eq!(
            texts(&h, ContextRole::User),
            ["old question", "heard 0", "heard 1"]
        );
        assert_eq!(
            texts(&h, ContextRole::Assistant),
            ["old answer", "answer 0", "answer 1"]
        );
        {
            let frames = h.probe.0.lock().unwrap();
            let pcm: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.frame.data {
                    FrameData::TtsAudio(a) => {
                        assert_eq!(a.sample_rate, 24000);
                        Some(a.audio.as_slice())
                    }
                    _ => None,
                })
                .flatten()
                .copied()
                .collect();
            assert_eq!(pcm, [1, 2, 3, 4, 5, 6, 1, 2, 3, 4, 5, 6]);
            assert_eq!(
                frames
                    .iter()
                    .filter(
                        |f| matches!(&f.frame.data,FrameData::LlmText(t) if !t.append_to_context)
                    )
                    .count(),
                2
            );
            assert!(frames.iter().any(|f|matches!(&f.frame.data,FrameData::UserAudio(a) if a.user_id=="local-microphone-only") && f.frame.metadata.get("route")==Some(&json!("synthetic-route"))));
        }
        h.finish().await;
    }
}

#[tokio::test]
async fn shared_constructor_two_sessions_two_manual_turns_flush_tail_and_ignore_server_vad() {
    for _ in 0..2 {
        let mut h = Harness::new(true, history(), vec![]).await;
        h.seed().await;
        for turn in 0..2 {
            h.send(FrameData::UserStartedSpeaking).await;
            h.send_frame(pcm_frame(vec![7; 2880])).await;
            let chunk = read(&mut h.ws).await;
            assert_eq!(
                STANDARD.decode(chunk["audio"].as_str().unwrap()).unwrap(),
                vec![7; 2880]
            );
            h.send_frame(pcm_frame(vec![9; 8])).await;
            h.send(FrameData::UserStoppedSpeaking).await;
            let tail = read(&mut h.ws).await;
            assert_eq!(
                STANDARD.decode(tail["audio"].as_str().unwrap()).unwrap(),
                vec![9; 8]
            );
            assert_eq!(read(&mut h.ws).await["type"], "input_audio_buffer.commit");
            assert_eq!(read(&mut h.ws).await["type"], "response.create");
            wire(
                &mut h.ws,
                json!({"type":"input_audio_buffer.speech_started","item_id":"late-provider-vad"}),
            )
            .await;
            wire(
                &mut h.ws,
                json!({"type":"input_audio_buffer.speech_stopped","item_id":"late-provider-vad"}),
            )
            .await;
            let id = format!("manual-{turn}");
            response(&mut h.ws, &id, false).await;
            wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":format!("user-{turn}"),"transcript":format!("manual question {turn}")})).await;
            spoken(&mut h.ws, &id, &format!("manual answer {turn}")).await;
            h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
            h.barrier().await;
            quiet(&mut h.ws).await;
        }
        assert_eq!(
            texts(&h, ContextRole::User),
            ["old question", "manual question 0", "manual question 1"]
        );
        assert_eq!(
            texts(&h, ContextRole::Assistant),
            ["old answer", "manual answer 0", "manual answer 1"]
        );
        assert!(!h.probe.0.lock().unwrap().iter().any(|f| matches!(
            f.frame.data,
            FrameData::ProposedUserStartedSpeaking | FrameData::ProposedUserStoppedSpeaking
        )));
        h.finish().await;
    }
}

fn echo(counter: Arc<AtomicUsize>) -> InworldLocalTool {
    InworldLocalTool {
        schema: FunctionTool::new(
            "echo",
            "Synthetic echo",
            json!({"type":"object","properties":{"n":{"type":"number"}},"required":["n"]}),
        ),
        handler: RegisteredTool::new(Arc::new(move |call| {
            let counter = counter.clone();
            Box::pin(async move {
                counter.fetch_add(1, Ordering::SeqCst);
                Ok(call.arguments)
            })
        })),
    }
}
#[tokio::test]
async fn real_executor_final_receipt_precedes_one_continuation_and_deferred_vad_transcript() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::new(false, Context::default(), vec![echo(count.clone())]).await;
    h.seed().await;
    wire(
        &mut h.ws,
        json!({"type":"input_audio_buffer.speech_started","item_id":"u"}),
    )
    .await;
    h.observe(|f| matches!(f, FrameData::UserStartedSpeaking))
        .await;
    h.barrier().await;
    wire(
        &mut h.ws,
        json!({"type":"input_audio_buffer.speech_stopped","item_id":"u"}),
    )
    .await;
    h.observe(|f| matches!(f, FrameData::UserStoppedSpeaking))
        .await;
    h.barrier().await;
    response(&mut h.ws, "tool-turn", false).await;
    call(&mut h.ws, "echo-call", "echo", "{\"n\":7}", true).await;
    // A repeated provider completion must not execute or settle twice.
    wire(&mut h.ws,json!({"type":"response.function_call_arguments.done","call_id":"echo-call","arguments":"{\"n\":7}"})).await;
    let result = read(&mut h.ws).await;
    assert_eq!(result["type"], "conversation.item.create");
    assert_eq!(result["item"]["type"], "function_call_output");
    assert_eq!(result["item"]["call_id"], "echo-call");
    assert_eq!(result["item"]["output"], "{\"n\":7}");
    h.observe(|f| matches!(f, FrameData::FunctionCallResult(_)))
        .await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    // The assistant/tool context update occurs before the deferred final user
    // transcript, reproducing the pinned Inworld regression without fake causes.
    wire(&mut h.ws,json!({"type":"conversation.item.input_audio_transcription.completed","item_id":"u","transcript":"tool question"})).await;
    response(&mut h.ws, "tool-turn", true).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "answer-turn", false).await;
    spoken(&mut h.ws, "answer-turn", "tool answer").await;
    h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
    h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(texts(&h, ContextRole::User), ["tool question"]);
    assert_eq!(
        texts(&h, ContextRole::Assistant)
            .iter()
            .filter(|s| s.as_str() == "tool answer")
            .count(),
        1
    );
    assert_eq!(h.outcomes.snapshot().total_terminal, 1);
    assert!(h
        .probe
        .0
        .lock()
        .unwrap()
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
        .all(|f| f.frame.native_tool_outcome().unwrap().is_some()));
    h.finish().await;
}

#[tokio::test]
async fn repeated_identical_typed_input_is_explicit_but_transcript_snapshots_are_not() {
    let mut h = Harness::new(false, Context::default(), vec![]).await;
    h.seed().await;
    for index in 0..2 {
        h.send(FrameData::InputText(TextFrame::new("same text")))
            .await;
        let item = read(&mut h.ws).await;
        assert_eq!(item["item"]["content"][0]["text"], "same text");
        assert_eq!(read(&mut h.ws).await["type"], "response.create");
        let id = format!("typed-{index}");
        response(&mut h.ws, &id, false).await;
        response(&mut h.ws, &id, true).await;
        h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
        h.barrier().await;
        quiet(&mut h.ws).await;
    }
    assert_eq!(texts(&h, ContextRole::User), ["same text", "same text"]);
    h.finish().await;
}

#[tokio::test]
async fn paused_input_and_interruption_discard_buffered_audio_without_truncate() {
    let mut h = Harness::new(true, Context::default(), vec![]).await;
    h.seed().await;
    h.send_frame(pcm_frame(vec![1; 100])).await;
    h.barrier().await;
    h.provider.set_audio_input_paused(true).unwrap();
    h.send_frame(pcm_frame(vec![2; 2880])).await;
    h.barrier().await;
    h.provider.set_audio_input_paused(false).unwrap();
    h.send(FrameData::UserStoppedSpeaking).await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    h.send_frame(pcm_frame(vec![3; 2880])).await;
    let chunk = read(&mut h.ws).await;
    assert_eq!(
        STANDARD.decode(chunk["audio"].as_str().unwrap()).unwrap(),
        vec![3; 2880]
    );
    h.send_frame(pcm_frame(vec![4; 6])).await;
    h.barrier().await;
    h.send(FrameData::Interruption).await;
    assert_eq!(read(&mut h.ws).await["type"], "input_audio_buffer.clear");
    assert_eq!(read(&mut h.ws).await["type"], "response.cancel");
    h.barrier().await;
    quiet(&mut h.ws).await;
    h.send_frame(pcm_frame(vec![5; 2880])).await;
    let after = read(&mut h.ws).await;
    assert_eq!(
        STANDARD.decode(after["audio"].as_str().unwrap()).unwrap(),
        vec![5; 2880]
    );
    h.barrier().await;
    quiet(&mut h.ws).await;
    h.finish().await;
}

#[test]
fn shared_constructor_rejects_noninterruptible_tools_before_connecting() {
    let mut tool = echo(Arc::new(AtomicUsize::new(0)));
    tool.handler.cancel_on_interruption = false;
    assert!(InworldApp::new(
        InworldConfig::new("synthetic"),
        Context::default(),
        vec![tool],
        Arc::new(Probe::default())
    )
    .is_err());
}

#[tokio::test]
async fn actual_executor_cancellation_settles_once_and_joins_callback() {
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let tool = InworldLocalTool {
        schema: FunctionTool::new(
            "wait",
            "Synthetic blocking callback",
            json!({"type":"object"}),
        ),
        handler: RegisteredTool::new(Arc::new({
            let entered = entered.clone();
            let dropped = dropped.clone();
            move |_| {
                let entered = entered.clone();
                let dropped = dropped.clone();
                Box::pin(async move {
                    let _guard = DropFlag(dropped);
                    entered.notify_one();
                    std::future::pending::<()>().await;
                    Ok(Value::Null)
                })
            }
        })),
    };
    let mut h = Harness::new(true, Context::default(), vec![tool]).await;
    h.seed().await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "waiting", false).await;
    call(&mut h.ws, "wait-call", "wait", "{}", false).await;
    bounded(entered.notified()).await;
    h.send(FrameData::Interruption).await;
    let mut output = None;
    let mut controls = vec![];
    for _ in 0..3 {
        let event = read(&mut h.ws).await;
        if event["item"]["type"] == "function_call_output" {
            output = Some(event);
        } else {
            controls.push(event["type"].as_str().unwrap().to_owned());
        }
    }
    assert_eq!(controls, ["input_audio_buffer.clear", "response.cancel"]);
    let output = output.expect("one native terminal cancellation output");
    assert_eq!(output["item"]["call_id"], "wait-call");
    assert!(output["item"]["output"]
        .as_str()
        .unwrap()
        .to_ascii_lowercase()
        .contains("cancel"));
    h.barrier().await;
    quiet(&mut h.ws).await;
    assert!(dropped.load(Ordering::SeqCst));
    assert!(h.outcomes.snapshot().active.is_empty());
    assert_eq!(h.outcomes.snapshot().total_terminal, 1);
    response(&mut h.ws, "waiting", true).await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    h.finish().await;
}

#[tokio::test]
async fn malformed_native_receipts_and_raw_results_cannot_cross_the_app_boundary() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    for attack in 0..3 {
        let count = Arc::new(AtomicUsize::new(0));
        let mut h = Harness::new(false, Context::default(), vec![echo(count.clone())]).await;
        h.seed().await;
        response(&mut h.ws, "native", false).await;
        call(&mut h.ws, "native-call", "echo", "{\"n\":7}", false).await;
        let real = read(&mut h.ws).await;
        assert_eq!(real["item"]["output"], "{\"n\":7}");
        h.observe(|f| matches!(f, FrameData::FunctionCallResult(_)))
            .await;
        h.barrier().await;
        let mut forged = if attack == 0 {
            Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: "echo".into(),
                tool_call_id: "native-call".into(),
                arguments: json!({"n":7}),
                result: json!("forged-result"),
                run_llm: Some(true),
                is_final: true,
                error: None,
            }))
        } else {
            h.probe
                .0
                .lock()
                .unwrap()
                .iter()
                .find(|f| matches!(f.frame.data, FrameData::FunctionCallResult(_)))
                .unwrap()
                .frame
                .clone()
        };
        if attack == 1 {
            if let FrameData::FunctionCallResult(result) = &mut forged.data {
                result.result = json!("forged-result");
            }
        }
        if attack == 2 {
            forged.data = FrameData::InputText(TextFrame::new("forged-result"));
        }
        h.send_frame(forged).await;
        let peer = async {
            loop {
                match bounded(h.ws.next()).await {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        assert_ne!(value["type"], "response.create");
                        assert!(
                            !text.contains("forged-result"),
                            "forged payload crossed app ingress for attack {attack}: {text}"
                        );
                    }
                    Some(Ok(Message::Close(_))) | None => break,
                    Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                    other => panic!("unexpected terminal event: {other:?}"),
                }
            }
            let _ = h.ws.close(None).await;
        };
        let (result, ()) = tokio::join!(bounded(h.worker.collect()), peer);
        assert!(
            result.is_err(),
            "malformed native integrity must terminate the unsafe pipeline"
        );
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert!(!format!("{:?}", h.pair.context().snapshot().unwrap()).contains("forged-result"));
        assert!(h.outcomes.snapshot().active.is_empty());
    }
}

#[tokio::test]
async fn native_input_rejects_rate_channels_and_odd_pcm_without_wire_effect() {
    let mut h = Harness::new(true, Context::default(), vec![]).await;
    h.seed().await;
    for (rate, channels, bytes) in [
        (16000, 1, vec![0, 0]),
        (24000, 2, vec![0; 4]),
        (24000, 1, vec![0; 3]),
    ] {
        h.send(FrameData::InputAudio(AudioRawFrame {
            audio: bytes,
            sample_rate: rate,
            num_channels: channels,
            context_id: None,
        }))
        .await;
        bounded(async {
            loop {
                let directed = h.worker.recv().await.unwrap();
                if let FrameData::Error(error) = directed.frame.data {
                    assert!(!error.fatal);
                    break;
                }
            }
        })
        .await;
        quiet(&mut h.ws).await;
    }
    h.finish_with_errors(3).await;
}

#[tokio::test]
async fn ordered_app_end_flushes_valid_tail_without_committing_or_inference() {
    let mut h = Harness::new(true, Context::default(), vec![]).await;
    h.seed().await;
    h.send_frame(pcm_frame(vec![9, 8, 7, 6])).await;
    h.barrier().await;
    h.worker.end().await.unwrap();
    let tail = read(&mut h.ws).await;
    assert_eq!(tail["type"], "input_audio_buffer.append");
    assert_eq!(
        STANDARD.decode(tail["audio"].as_str().unwrap()).unwrap(),
        [9, 8, 7, 6]
    );
    let (result, ()) = tokio::join!(bounded(h.worker.collect()), closed(&mut h.ws));
    assert!(result.unwrap().1.errors.is_empty());
    assert!(h.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn app_cancel_discards_tail_and_joins_an_active_response() {
    let mut h = Harness::new(true, Context::default(), vec![]).await;
    h.seed().await;
    h.send(FrameData::LlmRun).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "active", false).await;
    h.observe(|f| matches!(f, FrameData::LlmResponseStart))
        .await;
    h.send_frame(pcm_frame(vec![7, 8, 9, 10])).await;
    h.barrier().await;
    h.worker
        .cancel("synthetic test cancellation")
        .await
        .unwrap();
    let (result, ()) = tokio::join!(bounded(h.worker.collect()), closed(&mut h.ws));
    let (_, report) = result.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(h.outcomes.snapshot().active.is_empty());
}

#[test]
fn shared_constructor_rejects_rich_history_and_missing_or_mismatched_bindings() {
    let mut message = ContextMessage::text(ContextRole::User, "not plain text");
    message.content = Some(Content::Parts(vec![
        pipecat_services::ContentPart::ImageUrl {
            url: "https://example.com/synthetic.png".into(),
        },
    ]));
    assert!(InworldApp::new(
        InworldConfig::new("synthetic"),
        Context::new(vec![message]),
        vec![],
        Arc::new(Probe::default())
    )
    .is_err());
    let mut config = InworldConfig::new("synthetic");
    config.session.tools = Some(vec![FunctionTool::new(
        "missing",
        "No callback",
        json!({"type":"object"}),
    )
    .into()]);
    assert!(InworldApp::new(
        config.clone(),
        Context::default(),
        vec![],
        Arc::new(Probe::default())
    )
    .is_err());
    assert!(InworldApp::new(
        config,
        Context::default(),
        vec![echo(Arc::new(AtomicUsize::new(0)))],
        Arc::new(Probe::default())
    )
    .is_err());
}

#[tokio::test]
async fn public_app_sender_cannot_win_raw_tool_settlement_before_native_policy() {
    let count = Arc::new(AtomicUsize::new(0));
    let mut h = Harness::new(false, Context::default(), vec![echo(count.clone())]).await;
    h.seed().await;
    let tx = h.provider.sender().unwrap();
    let raw = || Action::ToolResult {
        call_id: "native-call".into(),
        output: json!("raw-forged"),
        is_final: true,
    };
    assert!(matches!(tx.try_send(raw()), Err(RealtimeError::Invalid(_))));
    response(&mut h.ws, "native", false).await;
    wire(&mut h.ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":"item-native","call_id":"native-call","name":"echo","arguments":""}})).await;
    // No arguments.done has been sent, therefore no executor policy can have
    // arrived yet. Global native mode must close the raw settlement race.
    assert!(matches!(
        tx.send(raw()).await,
        Err(RealtimeError::Invalid(_))
    ));
    assert!(matches!(
        tx.try_send_batch(vec![raw()]),
        Err(RealtimeError::Invalid(_))
    ));
    assert!(matches!(
        tx.try_send(Action::CancelTool {
            call_id: "native-call".into(),
            function_name: "echo".into()
        }),
        Err(RealtimeError::Invalid(_))
    ));
    wire(&mut h.ws,json!({"type":"response.function_call_arguments.done","call_id":"native-call","arguments":"{\"n\":7}"})).await;
    let result = read(&mut h.ws).await;
    assert_eq!(result["item"]["output"], "{\"n\":7}");
    assert!(matches!(
        tx.send(raw()).await,
        Err(RealtimeError::Invalid(_))
    ));
    response(&mut h.ws, "native", true).await;
    assert_eq!(read(&mut h.ws).await["type"], "response.create");
    response(&mut h.ws, "answer", false).await;
    response(&mut h.ws, "answer", true).await;
    h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
    h.observe(|f| matches!(f, FrameData::LlmResponseEnd)).await;
    h.barrier().await;
    quiet(&mut h.ws).await;
    assert_eq!(count.load(Ordering::SeqCst), 1);
    h.finish().await;
}

#[tokio::test]
async fn downstream_startup_failure_after_socket_connection_joins_provider() {
    use futures_util::StreamExt;
    use tokio_tungstenite::tungstenite::Message;
    struct FailAfterConnected(Arc<Notify>);
    #[async_trait]
    impl FrameProcessor for FailAfterConnected {
        fn name(&self) -> &str {
            "SyntheticFailAfterConnected"
        }
        async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
            self.0.notified().await;
            Err(ProcessorError::new("synthetic downstream setup failure"))
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            ctx.push(frame, direction).await
        }
    }
    let connected = Arc::new(Notify::new());
    let signal = connected.clone();
    let (config, server) = peer(move |mut ws| async move {
        let initial = handshake(&mut ws).await;
        signal.notify_one();
        // Core reports a nonfatal setup error after constructing the worker.
        // App startup may already have admitted its one no-run seed update.
        // This is not permission for inference, user text, retry or replay.
        let mut seed_updates = 0;
        loop {
            match bounded(ws.next()).await {
                Some(Ok(Message::Text(text))) => {
                    let command: Value = serde_json::from_str(&text).unwrap();
                    assert_eq!(command["type"], "session.update");
                    assert_eq!(command["session"], initial["session"]);
                    seed_updates += 1;
                    assert!(seed_updates <= 1, "startup seed must not replay");
                }
                Some(Ok(Message::Close(_))) | None => break,
                Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
                other => panic!("unexpected startup cleanup event: {other:?}"),
            }
        }
        let _ = ws.close(None).await;
    })
    .await;
    let app = InworldApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(FailAfterConnected(connected)),
    )
    .unwrap();
    let provider = app.provider.clone();
    assert!(bounded(app.start()).await.is_err());
    bounded(server).await.unwrap();
    assert!(!provider.sender().is_ok_and(|sender| sender.is_ready()));
}

#[tokio::test]
async fn cancelling_app_start_while_waiting_for_ready_closes_and_joins_peer() {
    let (configured, wait) = tokio::sync::oneshot::channel();
    let (config, server) = peer(move |mut ws| async move {
        wire(&mut ws, json!({"type":"session.created"})).await;
        assert_eq!(read(&mut ws).await["type"], "session.update");
        configured.send(()).unwrap();
        // Never acknowledge readiness: the start task is proven pending.
        closed(&mut ws).await;
    })
    .await;
    let app = InworldApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(Probe::default()),
    )
    .unwrap();
    let start = tokio::spawn(app.start());
    bounded(wait).await.unwrap();
    assert!(!start.is_finished());
    start.abort();
    assert!(bounded(start)
        .await
        .is_err_and(|error| error.is_cancelled()));
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn saturated_native_output_cancels_and_joins_a_blocked_output_owner() {
    use futures_util::SinkExt;
    use tokio_tungstenite::tungstenite::Message;
    struct BlockOutput {
        entered: Arc<Notify>,
        dropped: Arc<AtomicBool>,
    }
    struct PendingOutput(Arc<AtomicBool>);
    impl Drop for PendingOutput {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    #[async_trait]
    impl FrameProcessor for BlockOutput {
        fn name(&self) -> &str {
            "SyntheticBlockedOutput"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::TtsAudio(_)) {
                let _owner = PendingOutput(self.dropped.clone());
                self.entered.notify_one();
                std::future::pending::<()>().await;
            }
            ctx.push(frame, direction).await
        }
    }
    let entered = Arc::new(Notify::new());
    let dropped = Arc::new(AtomicBool::new(false));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = InworldConfig::new("synthetic-output-pressure");
    config.endpoint = format!(
        "ws://{}/api/v1/realtime/session",
        listener.local_addr().unwrap()
    );
    config.limits.queue_capacity = 4;
    config.limits.close_timeout = Duration::from_millis(100);
    let mut app = InworldApp::new(
        config,
        Context::default(),
        vec![],
        Arc::new(BlockOutput {
            entered: entered.clone(),
            dropped: dropped.clone(),
        }),
    )
    .unwrap();
    app.pipeline_config.queue_capacity = 4;
    app.pipeline_config.output_capacity = 4;
    app.pipeline_config.shutdown_timeout = Duration::from_secs(2);
    let accept = async {
        let (stream, _) = bounded(listener.accept()).await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        handshake(&mut ws).await;
        let seed = read(&mut ws).await;
        assert_eq!(seed["type"], "session.update");
        wire(
            &mut ws,
            json!({"type":"session.updated","session":seed["session"]}),
        )
        .await;
        ws
    };
    let (running, mut ws) = tokio::join!(app.start(), accept);
    let running = running.unwrap();
    response(&mut ws, "pressure", false).await;
    wire(&mut ws, audio_delta("pressure", "a", &[1, 2])).await;
    bounded(entered.notified()).await;
    for index in 0..128 {
        let mut event = audio_delta("pressure", "a", &[3, 4]);
        event["event_id"] = json!(format!("pressure-{index}"));
        if ws
            .send(Message::Text(event.to_string().into()))
            .await
            .is_err()
        {
            break;
        }
    }
    let _ = running.worker.cancel("synthetic output saturation").await;
    let (result, ()) = tokio::join!(bounded(running.worker.collect()), closed(&mut ws));
    assert!(!matches!(result, Err(PipelineError::ShutdownTimeout)));
    assert!(
        dropped.load(Ordering::SeqCst),
        "blocked output future must be dropped and joined"
    );
    assert!(!running
        .provider
        .sender()
        .is_ok_and(|sender| sender.is_ready()));
}

#[tokio::test]
async fn scaled_app_end_drains_active_tool_without_creating_new_inference() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let completed = Arc::new(AtomicBool::new(false));
    let mut handler = RegisteredTool::new(Arc::new({
        let entered = entered.clone();
        let release = release.clone();
        let completed = completed.clone();
        move |_| {
            let entered = entered.clone();
            let release = release.clone();
            let completed = completed.clone();
            Box::pin(async move {
                entered.notify_one();
                release.notified().await;
                completed.store(true, Ordering::SeqCst);
                Ok(json!("end-final"))
            })
        }
    }));
    handler.timeout = Duration::from_millis(600);
    let tool = InworldLocalTool {
        schema: FunctionTool::new(
            "wait",
            "Synthetic bounded End callback",
            json!({"type":"object"}),
        ),
        handler,
    };
    let limits = RealtimeLimits {
        connect_timeout: WAIT,
        operation_timeout: Duration::from_millis(100),
        close_timeout: Duration::from_millis(100),
        response_timeout: Duration::from_millis(600),
        ..Default::default()
    };
    let mut h = Harness::with_limits(false, Context::default(), vec![tool], limits).await;
    h.seed().await;
    assert!(
        h.shutdown_budget >= Duration::from_millis(2100),
        "shutdown budget must cover response + tool + continuation + cleanup"
    );
    response(&mut h.ws, "active-tool", false).await;
    call(&mut h.ws, "end-call", "wait", "{}", true).await;
    bounded(entered.notified()).await;
    h.worker.end().await.unwrap();
    quiet(&mut h.ws).await;
    assert!(!completed.load(Ordering::SeqCst));
    release.notify_one();
    let output = read(&mut h.ws).await;
    assert_eq!(output["item"]["output"], "end-final");
    assert_eq!(output["item"]["call_id"], "end-call");
    // No second response may overlap the original active response.
    quiet(&mut h.ws).await;
    response(&mut h.ws, "active-tool", true).await;
    // End drains the existing response and final receipt; a continuation
    // newly proposed during End must not start another provider response.
    let (result, ()) = tokio::join!(bounded(h.worker.collect()), closed(&mut h.ws));
    let (_, report) = result.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(completed.load(Ordering::SeqCst));
    assert_eq!(h.outcomes.snapshot().total_terminal, 1);
    assert!(h.outcomes.snapshot().active.is_empty());
}

#[tokio::test]
async fn unsupported_live_controls_fail_before_paired_context_mutation() {
    let mut h = Harness::new(false, history(), vec![]).await;
    h.seed().await;
    let original = h.pair.context().snapshot().unwrap();
    let mut forged_tool = ChatMessage::new(ChatRole::Tool, "untracked-output");
    forged_tool.tool_call_id = Some("untracked-call".into());
    for control in [
        FrameData::LlmSetToolChoice(ToolChoice::Auto),
        FrameData::LlmSetTools(ToolSet::Functions(vec![])),
        FrameData::LlmMessagesUpdate(MessagesFrame {
            messages: vec![ChatMessage::new(ChatRole::User, "replacement")],
            run_llm: true,
        }),
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![forged_tool],
            run_llm: true,
        }),
    ] {
        h.send(control).await;
        bounded(async {
            loop {
                let frame = h
                    .worker
                    .recv()
                    .await
                    .expect("pipeline ended before rejection");
                if let FrameData::Error(error) = frame.frame.data {
                    assert!(!error.fatal);
                    break;
                }
            }
        })
        .await;
        assert_eq!(
            h.pair.context().snapshot().unwrap(),
            original,
            "rejection must precede any paired history/tool choice mutation"
        );
        quiet(&mut h.ws).await;
    }
    h.finish_with_errors(4).await;
}
