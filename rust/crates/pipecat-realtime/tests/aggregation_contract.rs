//! Real loopback qualification of adapter ownership and failure boundaries.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_realtime::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, time::timeout};
use tokio_tungstenite::tungstenite::Message;

struct Peer {
    send: mpsc::UnboundedSender<Value>,
    recv: mpsc::UnboundedReceiver<Value>,
    task: tokio::task::JoinHandle<()>,
}
impl Peer {
    async fn new() -> (Self, RealtimeClient) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = RealtimeConfig::new("local-test-key");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        let (send, mut input) = mpsc::unbounded_channel::<Value>();
        let (output, recv) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            let init = ws.next().await.unwrap().unwrap();
            assert_eq!(
                serde_json::from_str::<Value>(init.to_text().unwrap()).unwrap()["type"],
                "session.update"
            );
            ws.send(Message::Text(json!({"type":"session.updated","event_id":"ready","session":{"id":"s","type":"realtime"}}).to_string().into())).await.unwrap();
            loop {
                tokio::select! {
                    event = input.recv() => {
                        let Some(mut event) = event else { break; };
                        event["event_id"] = json!("server-event");
                        if ws.send(Message::Text(event.to_string().into())).await.is_err() { break; }
                    }
                    message = ws.next() => match message {
                        Some(Ok(Message::Text(text))) => { let _ = output.send(serde_json::from_str(&text).unwrap()); }
                        Some(Ok(Message::Close(_))) | None | Some(Err(_)) => { let _ = ws.close(None).await; break; }
                        _ => {}
                    }
                }
            }
        });
        (
            Self { send, recv, task },
            RealtimeClient::new(config).unwrap(),
        )
    }
    fn event(&self, value: Value) {
        self.send.send(value).unwrap();
    }
    async fn command(&mut self) -> Value {
        timeout(Duration::from_secs(2), self.recv.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn quiet(&mut self) {
        assert!(timeout(Duration::from_millis(80), self.recv.recv())
            .await
            .is_err());
    }
    async fn close(self) {
        timeout(Duration::from_secs(2), self.task)
            .await
            .unwrap()
            .unwrap();
    }
}
async fn start(processor: Arc<OpenAiRealtimeProcessor>) -> PipelineWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    Pipeline::new(vec![processor]).start(config).await.unwrap()
}
async fn inject(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn seen(worker: &mut PipelineWorker, kind: impl Fn(&FrameData) -> bool) {
    timeout(Duration::from_secs(2), async {
        loop {
            let frame = worker.recv().await.unwrap();
            if kind(&frame.frame.data) {
                break;
            }
        }
    })
    .await
    .unwrap();
}
async fn end(mut worker: PipelineWorker, peer: Peer) {
    worker.end().await.unwrap();
    let report = worker.join().await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    peer.close().await;
}

#[tokio::test]
async fn explicit_seed_sets_instructions_without_inference_and_does_not_reimport() {
    let (mut peer, client) = Peer::new().await;
    let seed = LlmContextFrame {
        messages: vec![
            ChatMessage::new(ChatRole::System, "be concise"),
            ChatMessage::new(ChatRole::User, "history"),
        ],
        ..Default::default()
    };
    let processor = Arc::new(
        OpenAiRealtimeProcessor::new(client)
            .unwrap()
            .with_initial_context(seed.clone().into())
            .unwrap(),
    );
    let mut worker = start(processor).await;
    let update = peer.command().await;
    assert_eq!(update["type"], "session.update");
    assert_eq!(update["session"]["instructions"], "be concise");
    let item = peer.command().await;
    assert_eq!(item["item"]["content"][0]["text"], "history");
    peer.quiet().await;
    inject(
        &worker,
        FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: seed.into(),
            speculation: false,
        }),
    )
    .await;
    assert_eq!(peer.command().await["type"], "response.create");
    peer.event(
        json!({"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
    );
    peer.event(
        json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
    );
    seen(&mut worker, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    peer.quiet().await;
    end(worker, peer).await;
}
#[tokio::test]
async fn unsupported_detailed_history_has_no_partial_wire_effects() {
    let (mut peer, client) = Peer::new().await;
    let processor = Arc::new(OpenAiRealtimeProcessor::new(client).unwrap());
    let mut worker = start(processor).await;
    let mut bad: ContextMessage = ChatMessage::new(ChatRole::User, "opaque").into();
    bad.content = MessageContent::Parts(vec![MessagePart::Image {
        url: "https://example.invalid/image".into(),
        detail: None,
    }]);
    inject(
        &worker,
        FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![
                    ChatMessage::new(ChatRole::User, "must not leak partial prefix").into(),
                    bad,
                ],
                tools: ToolSet::Functions(vec![ToolDefinition {
                    name: "must_not_install".into(),
                    description: String::new(),
                    parameters: json!({"type":"object"}),
                }]),
                tool_choice: None,
            },
            speculation: false,
        }),
    )
    .await;
    seen(&mut worker, |f| matches!(f, FrameData::Error(_))).await;
    peer.quiet().await;
    worker.cancel("invalid context inspected").await.unwrap();
    worker.join().await.unwrap();
    peer.close().await;
}
#[tokio::test]
async fn playback_receipt_validates_item_epoch_and_exact_truncation() {
    use base64::Engine;
    let (mut peer, client) = Peer::new().await;
    let processor = Arc::new(OpenAiRealtimeProcessor::new(client).unwrap());
    let mut worker = start(processor.clone()).await;
    assert!(processor
        .acknowledge_playback(PlaybackPosition {
            item_id: "unknown".into(),
            content_index: 0,
            audio_end_ms: 0
        })
        .is_err());
    inject(&worker, FrameData::LlmRun).await;
    assert_eq!(peer.command().await["type"], "response.create");
    peer.event(
        json!({"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
    );
    peer.event(json!({"type":"response.output_audio.delta","response_id":"r","item_id":"audio","output_index":0,"content_index":0,"delta":base64::engine::general_purpose::STANDARD.encode(vec![0; 4800])}));
    seen(&mut worker, |f| matches!(f, FrameData::TtsAudio(_))).await;
    let mut receipt = PlaybackPosition {
        item_id: "audio".into(),
        content_index: 0,
        audio_end_ms: 101,
    };
    assert!(processor.acknowledge_playback(receipt.clone()).is_err());
    receipt.audio_end_ms = 40;
    processor.acknowledge_playback(receipt.clone()).unwrap();
    let mut older = receipt.clone();
    older.audio_end_ms = 39;
    assert!(processor.acknowledge_playback(older).is_err());
    inject(&worker, FrameData::Interruption).await;
    assert_eq!(peer.command().await["type"], "response.cancel");
    let truncate = peer.command().await;
    assert_eq!(truncate["type"], "conversation.item.truncate");
    assert_eq!(truncate["item_id"], "audio");
    assert_eq!(truncate["audio_end_ms"], 40);
    assert!(processor.acknowledge_playback(receipt).is_err());
    peer.event(
        json!({"type":"response.done","response":{"id":"r","status":"cancelled","output":[]}}),
    );
    end(worker, peer).await;
}
#[tokio::test]
async fn stop_releases_sender_and_rejects_processor_reuse() {
    let (peer, client) = Peer::new().await;
    let processor = Arc::new(OpenAiRealtimeProcessor::new(client).unwrap());
    let mut worker = start(processor.clone()).await;
    inject(&worker, FrameData::Stop).await;
    worker.join().await.unwrap();
    peer.close().await;
    assert!(processor.sender().is_err());
    let mut config = PipelineConfig::default();
    config.start.audio_in_sample_rate = 24_000;
    config.start.audio_out_sample_rate = 24_000;
    let outcome = Pipeline::new(vec![processor]).start(config).await;
    match outcome {
        Err(_) => {}
        Ok(mut worker) => {
            seen(&mut worker, |f| matches!(f, FrameData::Error(_))).await;
            let _ = worker.cancel("reuse rejected").await;
            worker.join().await.unwrap();
        }
    }
}

#[tokio::test]
async fn direct_tool_update_invalidates_context_settings_cache() {
    let (mut peer, client) = Peer::new().await;
    let tool = |name: &str| ToolDefinition {
        name: name.into(),
        description: String::new(),
        parameters: json!({"type":"object"}),
    };
    let seed = LlmContextFrame {
        tools: vec![tool("a")],
        ..Default::default()
    };
    let processor = Arc::new(
        OpenAiRealtimeProcessor::new(client)
            .unwrap()
            .with_initial_context(seed.clone().into())
            .unwrap(),
    );
    let mut worker = start(processor).await;
    assert_eq!(peer.command().await["session"]["tools"][0]["name"], "a");
    inject(
        &worker,
        FrameData::LlmSetTools(ToolSet::Functions(vec![tool("b")])),
    )
    .await;
    assert_eq!(peer.command().await["session"]["tools"][0]["name"], "b");
    inject(&worker, FrameData::LlmContext(seed)).await;
    assert_eq!(peer.command().await["session"]["tools"][0]["name"], "a");
    assert_eq!(peer.command().await["type"], "response.create");
    peer.event(
        json!({"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
    );
    peer.event(
        json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
    );
    seen(&mut worker, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    end(worker, peer).await;
}

#[tokio::test]
async fn source_seed_packs_unicode_history_and_leaves_later_append_separate() {
    let (mut peer, client) = Peer::new().await;
    let seed = LlmContextFrame {
        messages: vec![
            ChatMessage::new(ChatRole::System, "instructions"),
            ChatMessage::new(ChatRole::User, "你好\n\"quoted\""),
            ChatMessage::new(ChatRole::Assistant, "réponse"),
            ChatMessage::new(ChatRole::Developer, "follow-up"),
        ],
        ..Default::default()
    };
    let processor = Arc::new(
        OpenAiRealtimeProcessor::new(client)
            .unwrap()
            .with_initial_context(seed.into())
            .unwrap(),
    );
    let mut worker = start(processor).await;
    assert_eq!(
        peer.command().await["session"]["instructions"],
        "instructions"
    );
    let item = peer.command().await;
    assert_eq!(item["item"]["role"], "user");
    let expected = concat!(
        "\n        This is a previously saved conversation. Please treat this conversation history as a\n        starting point for the current conversation.\n\n",
        "[\n  {\n    \"role\": \"user\",\n    \"content\": \"你好\\n\\\"quoted\\\"\"\n  },\n  {\n    \"role\": \"assistant\",\n    \"content\": \"réponse\"\n  },\n  {\n    \"role\": \"user\",\n    \"content\": \"follow-up\"\n  }\n]",
        "\n\n\n        This is the end of the previously saved conversation. Please continue the conversation\n        from here. If the last message is a user instruction or question, act on that instruction\n        or answer the question. If the last message is an assistant response, simple say that you\n        are ready to continue the conversation."
    );
    assert_eq!(item["item"]["content"][0]["text"], expected);
    peer.quiet().await;
    inject(
        &worker,
        FrameData::InputText(TextFrame::new("你好\n\"quoted\"")),
    )
    .await;
    let item = peer.command().await;
    assert_eq!(item["item"]["content"][0]["text"], "你好\n\"quoted\"");
    assert_eq!(peer.command().await["type"], "response.create");
    peer.event(
        json!({"type":"response.created","response":{"id":"r","status":"in_progress","output":[]}}),
    );
    peer.event(
        json!({"type":"response.done","response":{"id":"r","status":"completed","output":[]}}),
    );
    seen(&mut worker, |f| matches!(f, FrameData::LlmResponseEnd)).await;
    end(worker, peer).await;
}

#[tokio::test]
async fn unsupported_provider_controls_fail_without_wire_effects() {
    for control in [
        FrameData::LlmConfigureOutput { skip_tts: true },
        FrameData::LlmEnablePromptCaching { enable: true },
        FrameData::LlmSummarizeContext(None),
    ] {
        let (mut peer, client) = Peer::new().await;
        let mut worker = start(Arc::new(OpenAiRealtimeProcessor::new(client).unwrap())).await;
        inject(&worker, control).await;
        seen(&mut worker, |f| matches!(f, FrameData::Error(_))).await;
        peer.quiet().await;
        worker
            .cancel("unsupported control inspected")
            .await
            .unwrap();
        worker.join().await.unwrap();
        peer.close().await;
    }
}

#[tokio::test]
async fn mutated_native_batch_is_rejected_at_provider_and_relay_ingress() {
    use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
    let mut executor = ToolExecutor::new(1, 16, 4096).unwrap();
    executor
        .register(
            "batch_probe",
            RegisteredTool::new(Arc::new(|_| Box::pin(async { Ok(json!("done")) }))),
        )
        .unwrap();
    let mut source = Pipeline::new(vec![Arc::new(executor)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    inject(
        &source,
        FrameData::FunctionCallsStarted(vec![FunctionCall {
            function_name: "batch_probe".into(),
            tool_call_id: "native-batch-call".into(),
            arguments: json!({}),
        }]),
    )
    .await;
    let original = timeout(Duration::from_secs(2), async {
        loop {
            let frame = source.recv().await.unwrap().frame;
            if frame.native_tool_batch().unwrap().is_some() {
                break frame;
            }
        }
    })
    .await
    .unwrap();
    assert!(original.native_tool_batch().unwrap().is_some());
    for relay_first in [false, true] {
        for audio in [false, true] {
            let (mut peer, client) = Peer::new().await;
            let processor = Arc::new(OpenAiRealtimeProcessor::new(client).unwrap());
            let mut config = PipelineConfig::default();
            config.start.audio_in_sample_rate = 24_000;
            config.start.audio_out_sample_rate = 24_000;
            let processors: Vec<Arc<dyn FrameProcessor>> = if relay_first {
                vec![Arc::new(OpenAiRealtimeToolResultRelay::new()), processor]
            } else {
                vec![processor]
            };
            let mut worker = Pipeline::new(processors).start(config).await.unwrap();
            let mut forged = original.clone();
            forged.data = if audio {
                FrameData::InputAudio(AudioRawFrame::new(vec![0; 480], 24_000, 1).unwrap())
            } else {
                FrameData::InputText(TextFrame::new("must not be sent"))
            };
            worker
                .sender()
                .send(forged, Direction::Downstream)
                .await
                .unwrap();
            seen(&mut worker, |data| matches!(data, FrameData::Error(_))).await;
            peer.quiet().await;
            worker.cancel("forged batch rejected").await.unwrap();
            worker.join().await.unwrap();
            peer.close().await;
        }
    }
    // Validation borrows, preserving the valid original for its actual consumer.
    assert!(original.native_tool_batch().unwrap().is_some());
    let pair = pipecat_turns::LLMContextAggregatorPair::new(
        pipecat_services::context::Context::default(),
        Default::default(),
        Default::default(),
    );
    pair.assistant()
        .process_frame(original, Direction::Downstream)
        .unwrap();
    assert!(pair.assistant().has_function_calls_in_progress().unwrap());
    source.end().await.unwrap();
    source.join().await.unwrap();
}

#[path = "support/continuation_gate.rs"]
mod continuation_gate;
