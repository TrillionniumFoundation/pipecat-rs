//! Independent native socket → STT → turn aggregator → HTTP LLM composition.
//! All provider transcript and completion responses are synthetic.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_soniox::*;
use pipecat_turns::LLMContextAggregatorPair;
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("native Soniox composition stalled")
}
async fn wait(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
}
#[derive(Default)]
struct Recorder(Mutex<Vec<Frame>>);
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "SonioxReviewRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.0.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
}
impl Recorder {
    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|f| predicate(&f.data))
            .count()
    }
}
struct Peer {
    client: SonioxSttClient,
    send: mpsc::Sender<Value>,
    wire: mpsc::Receiver<Message>,
    task: tokio::task::JoinHandle<()>,
}
async fn peer(local_vad: bool) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SonioxSttConfig::new("synthetic-soniox-secret");
    config.endpoint = format!(
        "ws://{}/transcribe-websocket",
        listener.local_addr().unwrap()
    );
    config.vad_force_turn_endpoint = local_vad;
    config.limits.close_timeout = Duration::from_millis(100);
    let (send, mut incoming) = mpsc::channel::<Value>(16);
    let (wire, outgoing) = mpsc::channel(32);
    let task = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        loop {
            tokio::select! {
                value=incoming.recv()=> { let Some(value)=value else {break}; if socket.send(Message::Text(value.to_string().into())).await.is_err(){break} },
                value=socket.next()=> match value {
                    Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,
                    Some(Ok(value))=>{if wire.send(value).await.is_err(){break}},
                }
            }
        }
    });
    Peer {
        client: SonioxSttClient::new(config).unwrap(),
        send,
        wire: outgoing,
        task,
    }
}
async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        if let Some(p) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..end]);
    let len: usize = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + len {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
    }
    serde_json::from_slice(&bytes[end..end + len]).unwrap()
}
#[tokio::test]
async fn provider_endpoint_keeps_final_tokens_interim_and_drives_exactly_one_native_inference() {
    let mut peer = peer(false).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut llm_config = ClientConfig::new(Provider::OpenAi, "synthetic");
    llm_config.endpoint = format!("http://{}/completions", listener.local_addr().unwrap());
    let (req_tx, req_rx) = oneshot::channel();
    let llm_task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        req_tx.send(request).unwrap();
        let body = format!(
            "data: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"delta":{"content":"Native reply."},"finish_reason":"stop"}]})
        );
        socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err(),
            "duplicate inference"
        );
    });
    let stt = Arc::new(
        SonioxSttProcessor::new(peer.client)
            .with_user_id("speaker-7")
            .unwrap(),
    );
    let mut user_config = stt.recommended_user_config();
    user_config.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
    let raw = Arc::new(Recorder::default());
    let downstream = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt,
        raw.clone(),
        pair.user(),
        Arc::new(LlmProcessor::new(
            LlmClient::new(llm_config).unwrap(),
            CompletionRequest::new("synthetic", Context::default()),
        )),
        downstream.clone(),
        pair.assistant(),
    ])
    .start(PipelineConfig {
        start: StartFrame {
            enable_usage_metrics: true,
            ..Default::default()
        },
        ..Default::default()
    })
    .await
    .unwrap();
    let config = bounded(peer.wire.recv()).await.unwrap();
    let config: Value = serde_json::from_str(config.to_text().unwrap()).unwrap();
    assert_eq!(config["api_key"], "synthetic-soniox-secret");
    assert_eq!(config["enable_endpoint_detection"], true);
    worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert_eq!(
        bounded(peer.wire.recv()).await.unwrap(),
        Message::Binary(vec![0; 640].into())
    );
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStartedSpeakingDetailed(Default::default())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    wait(|| raw.count(|f| matches!(f, FrameData::ProposedUserStartedSpeaking)) == 1).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeakingDetailed(Default::default())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    peer.send.send(json!({"tokens":[{"text":"Hola","is_final":true,"language":"es","speaker":"one"},{"text":" borrador","is_final":false,"language":"en"}]})).await.unwrap();
    wait(|| raw.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    assert_eq!(
        raw.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        0
    );
    assert!(pair.context().snapshot().unwrap().messages.is_empty());
    peer.send.send(json!({"tokens":[{"text":" mundo!","is_final":true,"language":"en"},{"text":"<end>","is_final":true}]})).await.unwrap();
    let request = bounded(req_rx).await.unwrap();
    assert_eq!(request["messages"][0]["content"], "Hola mundo!");
    wait(|| downstream.count(|f| matches!(f, FrameData::LlmResponseEnd)) == 1).await;
    bounded(llm_task).await.unwrap();
    {
        let frames = raw.0.lock().unwrap();
        let start = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::ProposedUserStartedSpeaking))
            .unwrap();
        let usage = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::Metrics(_)))
            .unwrap();
        let final_index = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::TranscriptionDetailed(_)))
            .unwrap();
        let stop = frames
            .iter()
            .position(|f| matches!(f.data, FrameData::ProposedUserStoppedSpeaking))
            .unwrap();
        assert!(start < final_index && usage < final_index && final_index < stop);
        let FrameData::TranscriptionDetailed(t) = &frames[final_index].data else {
            unreachable!()
        };
        assert_eq!(t.user_id, "speaker-7");
        assert!(!t.timestamp.is_empty());
        assert_eq!(t.language.as_deref(), Some("es"));
        assert!(t.finalized);
        assert_eq!(t.result.as_ref().unwrap()[0]["speaker"], "one");
        assert_eq!(t.result.as_ref().unwrap().as_array().unwrap().len(), 2);
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.data, FrameData::ProposedUserStartedSpeaking))
                .count(),
            1
        );
    }
    assert_eq!(
        downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
        1
    );
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        bounded(peer.wire.recv()).await.unwrap(),
        Message::Text("".into())
    );
    bounded(peer.task).await.unwrap();
}

#[tokio::test]
async fn local_vad_finalizes_without_provider_turns_and_cancel_never_sends_end_audio() {
    let mut peer = peer(true).await;
    let stt = Arc::new(SonioxSttProcessor::new(peer.client));
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), recorder.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let config = bounded(peer.wire.recv()).await.unwrap();
    let config: Value = serde_json::from_str(config.to_text().unwrap()).unwrap();
    assert_eq!(config["enable_endpoint_detection"], false);
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStartedSpeakingDetailed(Default::default())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::VadUserStoppedSpeakingDetailed(Default::default())),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let finalize = bounded(peer.wire.recv()).await.unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(finalize.to_text().unwrap()).unwrap(),
        json!({"type":"finalize"})
    );
    peer.send.send(json!({"tokens":[{"text":"kept","is_final":true},{"text":" discarded","is_final":false}],"finished":true})).await.unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))) == 1).await;
    assert_eq!(
        recorder.count(|f| matches!(
            f,
            FrameData::ProposedUserStartedSpeaking
                | FrameData::ProposedUserStoppedSpeaking
                | FrameData::Interruption
        )),
        0
    );
    assert!(recorder
        .0
        .lock()
        .unwrap()
        .iter()
        .any(|f| matches!(&f.data,FrameData::TranscriptionDetailed(t)if t.text.text=="kept")));
    worker.cancel("review cleanup").await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    bounded(peer.task).await.unwrap();
    while let Some(message) = peer.wire.recv().await {
        assert_ne!(message, Message::Text("".into()));
    }
}

#[tokio::test]
async fn health_restore_fences_old_socket_tokens_and_retains_boundary_identity() {
    let mut peer = peer(false).await;
    let stt = Arc::new(SonioxSttProcessor::new(peer.client));
    let recorder = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt, recorder.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let _ = bounded(peer.wire.recv()).await;
    peer.send
        .send(json!({"tokens":[{"text":"old","is_final":true}]}))
        .await
        .unwrap();
    wait(|| recorder.count(|f| matches!(f, FrameData::InterimTranscriptionDetailed(_))) == 1).await;
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    let _ = peer
        .send
        .send(json!({"tokens":[{"text":"<end>","is_final":true}]}))
        .await;
    let boundary = Frame::new(FrameData::UserStoppedSpeaking);
    let id = boundary.id;
    worker
        .sender()
        .send(boundary, Direction::Downstream)
        .await
        .unwrap();
    wait(|| recorder.0.lock().unwrap().iter().any(|f| f.id == id)).await;
    bounded(peer.task).await.unwrap();
    assert_eq!(
        recorder.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        0
    );
    worker.cancel("review cleanup").await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
}

#[tokio::test]
async fn direct_sender_settings_survive_processor_cleanup_and_next_setup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SonioxSttConfig::new("synthetic");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(50);
    let (tx, mut configured) = mpsc::channel(3);
    let server = tokio::spawn(async move {
        for expected in ["stt-rt-v5", "retained-model", "retained-model"] {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let config = bounded(socket.next()).await.unwrap().unwrap();
            let config: Value = serde_json::from_str(config.to_text().unwrap()).unwrap();
            assert_eq!(config["model"], expected);
            tx.send(()).await.unwrap();
            while let Some(Ok(message)) = socket.next().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        }
    });
    let processor = Arc::new(SonioxSttProcessor::new(
        SonioxSttClient::new(config).unwrap(),
    ));
    let first = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    bounded(configured.recv()).await.unwrap();
    processor
        .sender()
        .unwrap()
        .merge_settings(json!({"model":"retained-model"}).as_object().unwrap())
        .unwrap();
    bounded(configured.recv()).await.unwrap();
    assert_eq!(
        processor.config_snapshot().unwrap().settings.model,
        "retained-model"
    );
    first.end().await.unwrap();
    let (_, report) = bounded(first.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        processor.config_snapshot().unwrap().settings.model,
        "retained-model"
    );
    let second = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    bounded(configured.recv()).await.unwrap();
    second.end().await.unwrap();
    let (_, report) = bounded(second.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    bounded(server).await.unwrap();
}

/// One provider message contains start evidence, final text, and its endpoint.
/// Native queue priority must not lose the normal transcript/stop when the
/// proposed start causes the paired aggregator's interruption.
#[tokio::test]
async fn coalesced_provider_start_final_and_stop_survive_their_own_start_interruption() {
    let mut peer = peer(false).await;
    let stt = Arc::new(SonioxSttProcessor::new(peer.client));
    let mut user = stt.recommended_user_config();
    user.empty_user_turn = None;
    let pair = LLMContextAggregatorPair::new(Context::default(), user, Default::default());
    let raw = Arc::new(Recorder::default());
    let downstream = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        stt,
        raw.clone(),
        pair.user(),
        downstream.clone(),
        pair.assistant(),
    ])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    bounded(peer.wire.recv()).await.unwrap();
    for (index, text) in ["First complete utterance.", "Second complete utterance."]
        .iter()
        .enumerate()
    {
        peer.send
            .send(
                json!({"tokens":[{"text":text,"is_final":true},{"text":"<end>","is_final":true}]}),
            )
            .await
            .unwrap();
        // A genuine context frame is the boundary outcome. Do not send an
        // artificial interim, inject another turn, or refresh an old scope.
        let completed = tokio::time::timeout(Duration::from_secs(2), async {
            while downstream.count(|frame| {
                matches!(
                    frame,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                )
            }) <= index
            {
                tokio::task::yield_now().await;
            }
        })
        .await;
        if completed.is_err() {
            let raw_names: Vec<_> = raw
                .0
                .lock()
                .unwrap()
                .iter()
                .map(|frame| frame.data.source_class_name())
                .collect();
            let snapshot = pair.context().snapshot().unwrap();
            worker
                .cancel("coalesced regression diagnostic")
                .await
                .unwrap();
            let (_, report) = worker.collect().await.unwrap();
            panic!(
                "coalesced turn {index} lost: raw={raw_names:?}, context={snapshot:?}, dropped={}",
                report.dropped_frames
            );
        }
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), index + 1);
        assert_eq!(
            raw.count(|frame| matches!(frame, FrameData::TranscriptionDetailed(_))),
            index + 1
        );
        assert_eq!(
            downstream.count(|frame| matches!(frame, FrameData::UserStartedSpeaking)),
            index + 1
        );
        assert!(raw
            .0
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| matches!(frame.data, FrameData::TranscriptionDetailed(_)))
            .all(|frame| frame.interruptible));
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        bounded(peer.wire.recv()).await.unwrap(),
        Message::Text("".into())
    );
    bounded(peer.task).await.unwrap();
}

struct HoldTurnStartMarker {
    entered: tokio::sync::Semaphore,
    events: Mutex<Vec<LocalFlushEvent>>,
}
impl Default for HoldTurnStartMarker {
    fn default() -> Self {
        Self {
            entered: tokio::sync::Semaphore::new(0),
            events: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl FrameProcessor for HoldTurnStartMarker {
    fn name(&self) -> &str {
        "HoldSonioxTurnStartMarker"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::FlushDetailed(marker) = &frame.data {
            if marker.origin.as_deref() == Some("pipecat.soniox.turn-start") {
                self.events
                    .lock()
                    .unwrap()
                    .push(marker.event.clone().unwrap());
                self.entered.add_permits(1);
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test]
async fn blocked_turn_start_barrier_end_cancel_drop_and_health_settle_without_transcript_revival() {
    for mode in ["end", "cancel", "drop", "health"] {
        let mut peer = peer(false).await;
        let stt = Arc::new(SonioxSttProcessor::new(peer.client));
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            stt.recommended_user_config(),
            Default::default(),
        );
        let hold = Arc::new(HoldTurnStartMarker::default());
        let after = Arc::new(Recorder::default());
        let worker = Pipeline::new(vec![
            stt,
            hold.clone(),
            pair.user(),
            after.clone(),
            pair.assistant(),
        ])
        .start(Default::default())
        .await
        .unwrap();
        bounded(peer.wire.recv()).await.unwrap();
        peer.send.send(json!({"tokens":[{"text":"must not revive","is_final":true},{"text":"<end>","is_final":true}]})).await.unwrap();
        bounded(hold.entered.acquire()).await.unwrap().forget();
        match mode {
            "end" => {
                worker.end().await.unwrap();
                let (_, report) = bounded(worker.collect()).await.unwrap();
                assert!(report.errors.is_empty(), "{:?}", report.errors);
            }
            "cancel" => {
                worker.cancel("barrier blocked").await.unwrap();
                let _ = bounded(worker.collect()).await.unwrap();
            }
            "health" => {
                let health = worker.processor_health(0).unwrap();
                health.set_usable(false);
                health.set_usable(true);
                bounded(peer.task).await.unwrap();
                worker.cancel("health retired").await.unwrap();
                let _ = bounded(worker.collect()).await.unwrap();
                for event in hold.events.lock().unwrap().iter() {
                    event.set();
                }
                assert!(pair.context().snapshot().unwrap().messages.is_empty());
                continue;
            }
            _ => drop(worker),
        }
        bounded(peer.task).await.unwrap();
        for event in hold.events.lock().unwrap().iter() {
            event.set();
        }
        assert!(pair.context().snapshot().unwrap().messages.is_empty());
        assert_eq!(
            after.count(|frame| matches!(
                frame,
                FrameData::TranscriptionDetailed(_)
                    | FrameData::LlmContext(_)
                    | FrameData::LlmContextDetailed(_)
            )),
            0
        );
    }
}

#[tokio::test]
async fn uncompleted_turn_start_barrier_times_out_visibly_without_admitting_normal_transcript() {
    let mut peer = peer(false).await;
    let mut config = peer.client.config().clone();
    config.limits.response_timeout = Duration::from_millis(50);
    let stt = Arc::new(SonioxSttProcessor::new(
        SonioxSttClient::new(config).unwrap(),
    ));
    let hold = Arc::new(HoldTurnStartMarker::default());
    let mut worker = Pipeline::new(vec![stt, hold])
        .start(Default::default())
        .await
        .unwrap();
    bounded(peer.wire.recv()).await.unwrap();
    peer.send.send(json!({"tokens":[{"text":"must time out","is_final":true},{"text":"<end>","is_final":true}]})).await.unwrap();
    bounded(async {
        loop {
            let frame = worker.recv().await.unwrap();
            assert!(!matches!(
                frame.frame.data,
                FrameData::TranscriptionDetailed(_)
            ));
            if let Some(error) = frame.frame.data.as_error_frame() {
                assert!(error.error.contains("STT turn-start barrier"));
                break;
            }
        }
    })
    .await;
    worker.cancel("timeout observed").await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.total_errors > 0);
    bounded(peer.task).await.unwrap();
}
