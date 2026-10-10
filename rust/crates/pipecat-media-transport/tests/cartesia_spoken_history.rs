//! Cross-crate composition with the native Cartesia processor, a real loopback
//! WebSocket carrying synthetic provider events, and synthetic Played receipts.
//! This does not exercise a live provider or a physical playback device.
use async_trait::async_trait;
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::{encode_pcm16_le, PcmFormat};
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_services::{Content, Context};
use pipecat_streaming_speech::{CartesiaClient, CartesiaConfig, CartesiaTtsProcessor};
use pipecat_turns::{
    AggregationAction, AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator,
    SharedContext,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("Cartesia composition stalled")
}
async fn permit(gate: &Semaphore) {
    bounded(gate.acquire()).await.unwrap().forget();
}

struct Playback {
    entered: Semaphore,
    release: Semaphore,
    played: Semaphore,
    interrupted: Semaphore,
    frames: Mutex<Vec<Frame>>,
}
#[async_trait]
impl OutputBackend for Playback {
    async fn start(
        &self,
        format: PcmFormat,
        destinations: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        assert_eq!(format, PcmFormat::new(8000, 1).unwrap());
        assert_eq!(destinations, &[None]);
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.frames.lock().unwrap().push(frame.clone());
        self.played.add_permits(1);
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupted.add_permits(1);
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}
struct AssistantSink(Arc<LLMAssistantAggregator>);
impl SpokenHistorySink for AssistantSink {
    fn commit(&self, context: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        self.0
            .confirm_playback(&context.context_id, &context.texts)?
            .actions
            .into_iter()
            .map(|action| match action {
                AggregationAction::Push(frame) => Ok(frame),
                _ => Err(ProcessorError::new("unexpected native confirmation effect")),
            })
            .collect()
    }
}

/// Observe native TTS output without altering its identities or synthesizing
/// source completion. Holding the real end separates the two proof boundaries.
struct TtsProbe {
    frames: Mutex<Vec<Frame>>,
    end_ready: Semaphore,
    release_end: Semaphore,
    end_forwarded: Semaphore,
}
#[async_trait]
impl FrameProcessor for TtsProbe {
    fn name(&self) -> &str {
        "CartesiaProofProbe"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let end =
            direction == Direction::Downstream && matches!(frame.data, FrameData::LlmResponseEnd);
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
        }
        if end {
            self.end_ready.add_permits(1);
            self.release_end.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await?;
        if end {
            self.end_forwarded.add_permits(1);
        }
        Ok(())
    }
}
async fn receive_json(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let message = bounded(socket.next()).await.unwrap().unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn send_json(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}

struct Harness {
    worker: PipelineWorker,
    history: SpokenHistory,
    shared: SharedContext,
    playback: Arc<Playback>,
    probe: Arc<TtsProbe>,
    finish_provider: Arc<Semaphore>,
    server: JoinHandle<()>,
    start_id: u64,
    end_id: u64,
}
impl Harness {
    async fn start(fail_provider: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = CartesiaConfig::new("synthetic-loopback-key", "synthetic-voice");
        config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
        config.sample_rate = 8000;
        config.limits.response_timeout = Duration::from_secs(5);
        let finish_provider = Arc::new(Semaphore::new(0));
        let finish = finish_provider.clone();
        let server = tokio::spawn(async move {
            let mut socket = bounded(accept_async(bounded(listener.accept()).await.unwrap().0))
                .await
                .unwrap();
            let request = receive_json(&mut socket).await;
            assert_eq!(request["transcript"], "Hello.");
            assert_eq!(request["voice"]["id"], "synthetic-voice");
            assert_eq!(
                request["output_format"],
                json!({"container":"raw", "encoding":"pcm_s16le", "sample_rate":8000})
            );
            assert_eq!(request["continue"], true);
            let ending = receive_json(&mut socket).await;
            assert_eq!(ending["context_id"], request["context_id"]);
            assert_eq!(ending["continue"], false);
            let context = &request["context_id"];
            send_json(
                &mut socket,
                json!({"type":"chunk", "context_id":context,
                "data":STANDARD.encode(encode_pcm16_le(&[1000; 200]))}),
            )
            .await;
            send_json(
                &mut socket,
                json!({"type":"timestamps", "context_id":context,
                "word_timestamps":{"words":["Hello."], "start":[0.0], "end":[0.025]}}),
            )
            .await;
            permit(&finish).await;
            if fail_provider {
                send_json(
                    &mut socket,
                    json!({"type":"error", "context_id":context, "status_code":429}),
                )
                .await;
            } else {
                send_json(&mut socket, json!({"type":"done", "context_id":context})).await;
            }
            // Native client cleanup owns cancellation/close. Drain that real
            // exchange instead of manufacturing a successful server completion.
            bounded(async {
                while let Some(message) = socket.next().await {
                    match message {
                        Ok(Message::Close(_)) | Err(_) => break,
                        Ok(_) => {}
                    }
                }
            })
            .await;
        });
        let shared = SharedContext::new(Context::default());
        let assistant = Arc::new(LLMAssistantAggregator::new(
            shared.clone(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        ));
        let history = SpokenHistory::new(
            SpokenHistoryConfig::default(),
            Arc::new(AssistantSink(assistant.clone())),
        )
        .unwrap();
        let source = Arc::new(history.source_boundary("source-before-cartesia").unwrap());
        let playback = Arc::new(Playback {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            played: Semaphore::new(0),
            interrupted: Semaphore::new(0),
            frames: Mutex::new(vec![]),
        });
        let probe = Arc::new(TtsProbe {
            frames: Mutex::new(vec![]),
            end_ready: Semaphore::new(0),
            release_end: Semaphore::new(0),
            end_forwarded: Semaphore::new(0),
        });
        let output = Arc::new(
            OutputTransport::new(
                "native-output",
                OutputTransportConfig {
                    sample_rate: Some(8000),
                    chunk_duration: Duration::from_millis(10),
                    auto_silence: false,
                    end_silence: Duration::ZERO,
                    write_timeout: Duration::from_secs(5),
                    ..Default::default()
                },
                playback.clone(),
                None,
                BTreeMap::new(),
            )
            .unwrap()
            .with_spoken_history(history.clone())
            .unwrap(),
        );
        let control = output.control();
        let worker = Pipeline::new(vec![
            source,
            Arc::new(CartesiaTtsProcessor::new(
                CartesiaClient::new(config).unwrap(),
            )),
            probe.clone(),
            output,
            assistant,
        ])
        .start(PipelineConfig {
            start: StartFrame {
                audio_out_sample_rate: 8000,
                ..Default::default()
            },
            ..Default::default()
        })
        .await
        .unwrap();
        bounded(async {
            while !control.is_ready() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let mut start = Frame::new(FrameData::LlmResponseStart);
        start
            .metadata
            .insert("test.original-start".into(), "preserved".into());
        let start_id = start.id;
        let mut end = Frame::new(FrameData::LlmResponseEnd);
        end.metadata
            .insert("test.original-end".into(), "preserved".into());
        let end_id = end.id;
        for frame in [
            start,
            Frame::new(FrameData::LlmText(TextFrame::llm("Hello."))),
            end,
        ] {
            worker
                .sender()
                .send(frame, Direction::Downstream)
                .await
                .unwrap();
        }
        Self {
            worker,
            history,
            shared,
            playback,
            probe,
            finish_provider,
            server,
            start_id,
            end_id,
        }
    }
    fn text(&self) -> Vec<String> {
        self.shared
            .snapshot()
            .unwrap()
            .messages
            .into_iter()
            .filter_map(|message| match message.content {
                Some(Content::Text(text)) => Some(text),
                _ => None,
            })
            .collect()
    }
    fn assert_native_boundaries(&self, failed: bool) {
        let frames = self.probe.frames.lock().unwrap();
        let start = frames
            .iter()
            .find(|frame| matches!(frame.data, FrameData::LlmResponseStart))
            .unwrap();
        let end = frames
            .iter()
            .find(|frame| matches!(frame.data, FrameData::LlmResponseEnd))
            .unwrap();
        assert_eq!(start.id, self.start_id);
        assert_eq!(end.id, self.end_id);
        assert_eq!(start.metadata["test.original-start"], "preserved");
        assert_eq!(end.metadata["test.original-end"], "preserved");
        assert_eq!(
            start.metadata["pipecat.media.spoken.source"],
            end.metadata["pipecat.media.spoken.source"]
        );
        assert_eq!(
            end.metadata
                .get("failed")
                .and_then(Value::as_bool)
                .unwrap_or(false),
            failed
        );
        let started = frames
            .iter()
            .position(|frame| matches!(frame.data, FrameData::TtsStarted(_)))
            .unwrap();
        let stopped = frames
            .iter()
            .position(|frame| matches!(frame.data, FrameData::TtsStopped(_)))
            .unwrap();
        let ended = frames
            .iter()
            .position(|frame| matches!(frame.data, FrameData::LlmResponseEnd))
            .unwrap();
        assert!(started < stopped && stopped < ended);
        let FrameData::TtsStarted(begin) = &frames[started].data else {
            unreachable!()
        };
        let FrameData::TtsStopped(stop) = &frames[stopped].data else {
            unreachable!()
        };
        assert!(begin.context_id.is_some());
        assert_eq!(begin.context_id, stop.context_id);
        assert!(frames.iter().any(|frame| matches!(&frame.data, FrameData::TtsText(text) if text.context_id == begin.context_id && text.text.text == "Hello." && text.will_be_spoken)));
        assert!(frames.iter().any(|frame| matches!(&frame.data, FrameData::TtsAudio(audio) if audio.context_id == begin.context_id && audio.num_frames() == 200)));
    }
    async fn finish(self) -> WorkerReport {
        self.worker.end().await.unwrap();
        let (_, report) = bounded(self.worker.collect()).await.unwrap();
        bounded(self.server).await.unwrap();
        report
    }
}

#[tokio::test]
async fn native_cartesia_success_requires_every_receipt_and_its_original_source_end() {
    let h = Harness::start(false).await;
    permit(&h.playback.entered).await;
    assert!(h.text().is_empty());
    h.finish_provider.add_permits(1);
    permit(&h.probe.end_ready).await;
    h.assert_native_boundaries(false);
    h.playback.release.add_permits(2);
    permit(&h.playback.entered).await;
    permit(&h.playback.entered).await;
    assert_eq!(h.playback.frames.lock().unwrap().len(), 2);
    assert!(
        h.text().is_empty(),
        "native provider Done is not a playback receipt"
    );
    h.playback.release.add_permits(1);
    for _ in 0..3 {
        permit(&h.playback.played).await;
    }
    assert!(
        h.text().is_empty(),
        "all Played receipts still require the original successful source end"
    );
    h.probe.release_end.add_permits(1);
    bounded(async {
        while h.history.status().committed_contexts == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await;
    assert_eq!(h.text(), ["Hello."]);
    assert_eq!(h.history.status().committed_contexts, 1);
    assert!(h.finish().await.errors.is_empty());
}

#[tokio::test]
async fn native_cartesia_failure_after_partial_pcm_and_tts_stop_withholds_history() {
    let h = Harness::start(true).await;
    permit(&h.playback.entered).await;
    h.playback.release.add_permits(1);
    permit(&h.playback.played).await;
    assert_eq!(h.playback.frames.lock().unwrap().len(), 1);
    h.finish_provider.add_permits(1);
    permit(&h.probe.end_ready).await;
    h.assert_native_boundaries(true);
    h.playback.release.add_permits(10);
    h.probe.release_end.add_permits(1);
    permit(&h.probe.end_forwarded).await;
    let shared = h.shared.clone();
    let history = h.history.clone();
    let report = h.finish().await;
    assert!(report
        .errors
        .iter()
        .any(|error| error.category == ErrorCategory::RateLimit));
    assert!(shared.snapshot().unwrap().messages.is_empty());
    assert_eq!(history.status().committed_contexts, 0);
}

#[tokio::test]
async fn native_cartesia_interruption_before_last_receipt_withholds_the_context() {
    let h = Harness::start(false).await;
    permit(&h.playback.entered).await;
    h.finish_provider.add_permits(1);
    permit(&h.probe.end_ready).await;
    h.assert_native_boundaries(false);
    h.probe.release_end.add_permits(1);
    permit(&h.probe.end_forwarded).await;
    h.playback.release.add_permits(2);
    permit(&h.playback.entered).await;
    permit(&h.playback.entered).await;
    assert_eq!(h.playback.frames.lock().unwrap().len(), 2);
    assert!(h.text().is_empty());
    h.history.invalidate();
    h.worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    permit(&h.playback.interrupted).await;
    let shared = h.shared.clone();
    let history = h.history.clone();
    let playback = h.playback.clone();
    assert!(h.finish().await.errors.is_empty());
    assert_eq!(
        playback.frames.lock().unwrap().len(),
        2,
        "cancelled final write must not finish later"
    );
    assert!(shared.snapshot().unwrap().messages.is_empty());
    assert_eq!(history.status().committed_contexts, 0);
}
