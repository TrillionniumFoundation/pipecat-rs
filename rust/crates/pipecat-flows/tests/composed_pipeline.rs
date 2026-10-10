//! Post-action ordering through native inference, synthesis, and synthetic Played receipts.
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_flows::*;
use pipecat_media_transport::{
    OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt,
};
use pipecat_services::{
    ClientConfig, CompletionRequest, Content, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_streaming_speech::{CartesiaClient, CartesiaConfig, CartesiaTtsProcessor};
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Semaphore,
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("composed pipeline stalled")
}

async fn permit(gate: &Semaphore) {
    bounded(gate.acquire()).await.unwrap().forget();
}

struct PlaybackSink {
    entered: Semaphore,
    release: Semaphore,
    played: Mutex<Vec<Frame>>,
}

#[async_trait]
impl OutputBackend for PlaybackSink {
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
        self.played.lock().unwrap().push(frame.clone());
        Ok(PlaybackReceipt::Played)
    }

    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        Ok(())
    }

    async fn stop(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}

/// Observe the real returning marker without manufacturing an acknowledgement.
struct ReceiptProbe {
    returned: Mutex<Vec<Frame>>,
    changed: Semaphore,
}

impl ReceiptProbe {
    async fn wait_for(&self, count: usize) {
        bounded(async {
            loop {
                if self.returned.lock().unwrap().len() >= count {
                    return;
                }
                self.changed.acquire().await.unwrap().forget();
            }
        })
        .await;
    }

    fn assert_receipts(&self) {
        let frames = self.returned.lock().unwrap();
        assert_eq!(
            frames.len(),
            2,
            "one setup receipt and one post-action receipt"
        );
        assert_ne!(frames[0].id, frames[1].id);
        assert_ne!(
            frames[0].metadata["pipecat.flows.barrier"],
            frames[1].metadata["pipecat.flows.barrier"]
        );
        for frame in frames.iter() {
            assert!(!frame.interruptible);
            assert!(frame.metadata["pipecat.flows.barrier"].as_u64().is_some());
        }
    }
}

#[async_trait]
impl FrameProcessor for ReceiptProbe {
    fn name(&self) -> &str {
        "ReceiptProbe"
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let receipt = (direction == Direction::Downstream
            && matches!(&frame.data, FrameData::FlushDetailed(flush)
                if flush.returning && flush.origin.as_deref() == Some("pipecat.flows")))
        .then(|| frame.clone());
        ctx.push(frame, direction).await?;
        if let Some(frame) = receipt {
            self.returned.lock().unwrap().push(frame);
            self.changed.add_permits(1);
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

async fn receive_http(socket: &mut TcpStream) -> Value {
    let mut request = Vec::new();
    let mut buffer = [0u8; 4096];
    loop {
        let count = bounded(socket.read(&mut buffer)).await.unwrap();
        assert!(count > 0, "HTTP request ended before its body");
        request.extend_from_slice(&buffer[..count]);
        assert!(request.len() <= 64 * 1024, "unexpected HTTP request size");
        if let Some(end) = request.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            let headers = String::from_utf8_lossy(&request[..end]);
            assert!(headers.starts_with("POST /v1/chat/completions HTTP/1.1"));
            let length = headers
                .lines()
                .find_map(|line| {
                    line.to_ascii_lowercase()
                        .strip_prefix("content-length:")
                        .and_then(|value| value.trim().parse::<usize>().ok())
                })
                .expect("HTTP request has a content length");
            if request.len() >= end + 4 + length {
                return serde_json::from_slice(&request[end + 4..end + 4 + length]).unwrap();
            }
        }
    }
}

struct Harness {
    worker: PipelineWorker,
    manager: FlowManager,
    flow_sink: Arc<PipelineFlowSink>,
    pair: LLMContextAggregatorPair,
    initialize: JoinHandle<Result<(), FlowError>>,
    llm_requested: Arc<Semaphore>,
    release_llm: Arc<Semaphore>,
    synthesis_requested: Arc<Semaphore>,
    release_synthesis: Arc<Semaphore>,
    playback: Arc<PlaybackSink>,
    receipts: Arc<ReceiptProbe>,
    actions: Arc<AtomicUsize>,
    servers: [JoinHandle<()>; 2],
}

impl Harness {
    async fn start() -> Self {
        let http_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let ws_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut llm_config = ClientConfig::new(Provider::OpenAi, "loopback-only");
        llm_config.endpoint = format!(
            "http://{}/v1/chat/completions",
            http_listener.local_addr().unwrap()
        );
        let mut tts_config = CartesiaConfig::new("loopback-only", "test-voice");
        tts_config.endpoint = format!("ws://{}/tts/websocket", ws_listener.local_addr().unwrap());
        tts_config.sample_rate = 8000;

        let llm_requested = Arc::new(Semaphore::new(0));
        let release_llm = Arc::new(Semaphore::new(0));
        let synthesis_requested = Arc::new(Semaphore::new(0));
        let release_synthesis = Arc::new(Semaphore::new(0));
        let (requested, release) = (llm_requested.clone(), release_llm.clone());
        let http_server = tokio::spawn(async move {
            let (mut socket, _) = bounded(http_listener.accept()).await.unwrap();
            let request = receive_http(&mut socket).await;
            assert_eq!(request["stream"], true);
            assert!(request["messages"]
                .as_array()
                .unwrap()
                .iter()
                .any(|message| message["content"] == "Say hello"));
            requested.add_permits(1);
            permit(&release).await;
            let body = concat!(
                "data: {\"choices\":[{\"delta\":{\"content\":\"Hello.\"},\"finish_reason\":null}]}\n\n",
                "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\n",
                "data: [DONE]\n\n"
            );
            socket.write_all(format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body
            ).as_bytes()).await.unwrap();
        });
        let (requested, release) = (synthesis_requested.clone(), release_synthesis.clone());
        let ws_server = tokio::spawn(async move {
            let mut socket = bounded(accept_async(bounded(ws_listener.accept()).await.unwrap().0))
                .await
                .unwrap();
            let request = receive_json(&mut socket).await;
            assert_eq!(request["transcript"], "Hello.");
            assert_eq!(request["voice"]["id"], "test-voice");
            assert_eq!(
                request["output_format"],
                json!({"container":"raw", "encoding":"pcm_s16le", "sample_rate":8000})
            );
            assert_eq!(request["continue"], true);
            let finish = receive_json(&mut socket).await;
            assert_eq!(finish["context_id"], request["context_id"]);
            assert_eq!(finish["continue"], false);
            requested.add_permits(1);
            permit(&release).await;
            send_json(
                &mut socket,
                json!({"type":"chunk", "context_id":request["context_id"], "data":"AQACAA=="}),
            )
            .await;
            send_json(&mut socket, json!({"type":"timestamps", "context_id":request["context_id"], "word_timestamps":{"words":["Hello."], "start":[0.0], "end":[0.00025]}})).await;
            send_json(
                &mut socket,
                json!({"type":"done", "context_id":request["context_id"]}),
            )
            .await;
            // Keep the socket alive until the native client closes its completed stream.
            let _ = bounded(socket.next()).await;
        });

        let playback = Arc::new(PlaybackSink {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            played: Mutex::new(vec![]),
        });
        let receipts = Arc::new(ReceiptProbe {
            returned: Mutex::new(vec![]),
            changed: Semaphore::new(0),
        });
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        let flow_sink = Arc::new(PipelineFlowSink::new());
        let actions = Arc::new(AtomicUsize::new(0));
        let mut handlers = HandlerRegistry::new();
        let (calls, sink) = (actions.clone(), playback.clone());
        handlers
            .register_action(
                "after",
                ActionHandler::new(move |_, handle| {
                    let (calls, sink) = (calls.clone(), sink.clone());
                    async move {
                        calls.fetch_add(1, Ordering::SeqCst);
                        assert_eq!(
                            sink.played.lock().unwrap().len(),
                            1,
                            "post action preceded playback"
                        );
                        assert_eq!(
                            handle
                                .context()
                                .snapshot()
                                .unwrap()
                                .messages
                                .iter()
                                .filter(|message| message.content
                                    == Some(Content::Text("Hello.".into())))
                                .count(),
                            1,
                            "post action must see exactly one committed assistant response"
                        );
                        handle.set_state("after", json!(true))
                    }
                }),
            )
            .unwrap();
        let config = FlowConfig::from_json(r#"{"initial_node":"hello","nodes":{"hello":{"task_messages":[{"role":"developer","content":"Say hello"}],"post_actions":[{"type":"function","handler":"after"}]}}}"#).unwrap();
        let manager = FlowManager::new(
            Flow::new(config, handlers).unwrap(),
            flow_sink.clone(),
            pair.context(),
        );
        let output = OutputTransport::new(
            "playback",
            OutputTransportConfig {
                sample_rate: Some(8000),
                chunk_duration: Duration::from_millis(10),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_secs(3),
                ..Default::default()
            },
            playback.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap();
        let worker = Pipeline::new(vec![
            pair.user(),
            Arc::new(LlmProcessor::new(
                LlmClient::new(llm_config).unwrap(),
                CompletionRequest::new("test-model", Context::default()),
            )),
            Arc::new(FlowProcessor::new(manager.clone())),
            Arc::new(CartesiaTtsProcessor::new(
                CartesiaClient::new(tts_config).unwrap(),
            )),
            Arc::new(output),
            pair.assistant(),
            receipts.clone(),
            Arc::new(FlowActionBoundary::new(manager.clone())),
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
        flow_sink.bind(worker.sender()).unwrap();
        let clone = manager.clone();
        let initialize = tokio::spawn(async move { clone.initialize().await });
        Self {
            worker,
            manager,
            flow_sink,
            pair,
            initialize,
            llm_requested,
            release_llm,
            synthesis_requested,
            release_synthesis,
            playback,
            receipts,
            actions,
            servers: [http_server, ws_server],
        }
    }

    async fn assert_post_waits(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(30), &mut self.initialize)
                .await
                .is_err(),
            "post action completed while its prerequisite was gated"
        );
        assert_eq!(self.actions.load(Ordering::SeqCst), 0);
        assert!(!self.manager.state().unwrap().contains_key("after"));
        assert_eq!(self.flow_sink.pending_barriers(), 1);
        assert_eq!(self.receipts.returned.lock().unwrap().len(), 1);
        assert!(!self
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .any(|message| message.content == Some(Content::Text("Hello.".into()))));
    }

    async fn finish(self) {
        self.receipts.wait_for(2).await;
        self.receipts.assert_receipts();
        let played = self.playback.played.lock().unwrap().clone();
        assert_eq!(played.len(), 1);
        let FrameData::TtsAudio(audio) = &played[0].data else {
            panic!("expected native TTS PCM")
        };
        assert_eq!(audio.sample_rate, 8000);
        assert!(audio.context_id.is_some());
        assert_eq!(&audio.audio[..4], &[1, 0, 2, 0]);
        assert_eq!(
            audio.audio.len(),
            160,
            "partial PCM tail must reach playback"
        );
        assert!(audio.audio[4..].iter().all(|byte| *byte == 0));
        bounded(self.worker.end()).await.unwrap();
        let (frames, report) = bounded(self.worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(report.state, WorkerState::Ended);
        assert!(!frames.iter().any(|frame| matches!(&frame.frame.data, FrameData::FlushDetailed(flush) if flush.origin.as_deref() == Some("pipecat.flows"))), "private flow receipts must be consumed by the real action boundary");
        assert_eq!(self.flow_sink.pending_barriers(), 0);
        for server in self.servers {
            bounded(server).await.unwrap();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn post_function_waits_for_native_llm_synthesis_and_gated_synthetic_played_receipt() {
    let mut harness = Harness::start().await;
    permit(&harness.llm_requested).await;
    harness.assert_post_waits().await;
    harness.release_llm.add_permits(1);
    permit(&harness.synthesis_requested).await;
    harness.assert_post_waits().await;
    harness.release_synthesis.add_permits(1);
    permit(&harness.playback.entered).await;
    harness.assert_post_waits().await;
    assert!(harness.playback.played.lock().unwrap().is_empty());
    harness.playback.release.add_permits(1);
    bounded(&mut harness.initialize).await.unwrap().unwrap();
    assert_eq!(harness.actions.load(Ordering::SeqCst), 1);
    assert_eq!(harness.manager.state().unwrap()["after"], true);
    assert!(harness
        .pair
        .context()
        .snapshot()
        .unwrap()
        .messages
        .iter()
        .any(|message| message.content == Some(Content::Text("Hello.".into()))));
    harness.finish().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_at_each_native_stage_rejects_late_uninterruptible_receipts() {
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
    enum Stage {
        Llm,
        Synthesis,
        Playback,
    }

    for cancel_at in [Stage::Llm, Stage::Synthesis, Stage::Playback] {
        let mut harness = Harness::start().await;
        permit(&harness.llm_requested).await;
        if cancel_at >= Stage::Synthesis {
            harness.release_llm.add_permits(1);
            permit(&harness.synthesis_requested).await;
        }
        if cancel_at == Stage::Playback {
            harness.release_synthesis.add_permits(1);
            permit(&harness.playback.entered).await;
        }
        harness.assert_post_waits().await;
        harness.manager.cancel();
        assert!(matches!(
            bounded(&mut harness.initialize).await.unwrap(),
            Err(FlowError::Cancelled)
        ));
        assert_eq!(harness.manager.status(), FlowStatus::Cancelled);
        assert_eq!(harness.flow_sink.pending_barriers(), 0);
        // Let every native operation and its preserved fence finish after cancellation.
        if cancel_at == Stage::Llm {
            harness.release_llm.add_permits(1);
            permit(&harness.synthesis_requested).await;
        }
        if cancel_at <= Stage::Synthesis {
            harness.release_synthesis.add_permits(1);
            permit(&harness.playback.entered).await;
        }
        harness.playback.release.add_permits(1);
        harness.receipts.wait_for(2).await;
        let (manager, actions) = (harness.manager.clone(), harness.actions.clone());
        harness.finish().await;
        assert_eq!(
            actions.load(Ordering::SeqCst),
            0,
            "late receipt revived a cancelled post action at stage {cancel_at:?}"
        );
        assert!(!manager.state().unwrap().contains_key("after"));
    }
}
