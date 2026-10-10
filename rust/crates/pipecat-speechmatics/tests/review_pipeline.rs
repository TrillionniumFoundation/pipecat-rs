//! Independent native Agent STT -> user aggregator -> HTTP LLM composition.
//! Provider transcripts and HTTP completions are synthetic loopback fixtures.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_speechmatics::*;
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
    sync::mpsc,
};
use tokio_tungstenite::{accept_async, tungstenite::Message};

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("Speechmatics composition stalled")
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
struct Recorder(Mutex<Vec<(Frame, Direction)>>);
#[async_trait]
impl FrameProcessor for Recorder {
    fn name(&self) -> &str {
        "SpeechmaticsReviewRecorder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.0.lock().unwrap().push((frame.clone(), direction));
        ctx.push(frame, direction).await
    }
}
impl Recorder {
    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|(frame, direction)| {
                *direction == Direction::Downstream && predicate(&frame.data)
            })
            .count()
    }
}
struct Peer {
    config: SpeechmaticsSttConfig,
    send: mpsc::Sender<Value>,
    wire: mpsc::Receiver<Message>,
    task: tokio::task::JoinHandle<()>,
}
async fn peer() -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("synthetic-speechmatics-secret");
    config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(1);
    let (send, mut incoming) = mpsc::channel::<Value>(16);
    let (wire, outgoing) = mpsc::channel(32);
    let task = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let start = socket.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(start.to_text().unwrap()).unwrap()["message"],
            "StartRecognition"
        );
        wire.send(start).await.unwrap();
        socket
            .send(Message::Text(
                json!({"message":"RecognitionStarted","id":"fixture-session"})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        loop {
            tokio::select! {
                value=incoming.recv()=> { let Some(value)=value else {break}; if socket.send(Message::Text(value.to_string().into())).await.is_err(){break} },
                value=socket.next()=> match value {
                    Some(Ok(Message::Close(_)))|None|Some(Err(_))=>break,
                    Some(Ok(value))=>{
                        let eos=value.to_text().ok().and_then(|s|serde_json::from_str::<Value>(s).ok()).is_some_and(|v|v["message"]=="EndOfStream");
                        if wire.send(value).await.is_err(){break}
                        if eos { let _=socket.send(Message::Text(json!({"message":"EndOfTranscript"}).to_string().into())).await; }
                    },
                }
            }
        }
    });
    Peer {
        config,
        send,
        wire: outgoing,
        task,
    }
}
fn segment(kind: &str, text: &str) -> Value {
    json!({"message":kind,"segment":{"transcript":text,"speaker":"provider-speaker"},"metadata":{"start_time":0.0,"end_time":1.0}})
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
        assert!(bytes.len() < 1024 * 1024);
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
async fn review_service_vad_multiple_final_segments_commit_one_complete_native_llm_request() {
    for should_interrupt in [false, true] {
        let mut peer = peer().await;
        peer.config.settings.turn_detection_mode = TurnDetectionMode::Vad;
        peer.config.should_interrupt = should_interrupt;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut llm_config = ClientConfig::new(Provider::OpenAi, "synthetic");
        llm_config.endpoint = format!("http://{}/completions", listener.local_addr().unwrap());
        let (req_tx, mut req_rx) = mpsc::channel(2);
        let llm_task = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            req_tx.send(read_request(&mut socket).await).await.unwrap();
            let body = format!(
                "data: {}\n\ndata: [DONE]\n\n",
                json!({"choices":[{"delta":{"content":"Native reply."},"finish_reason":"stop"}]})
            );
            socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
            assert!(
                tokio::time::timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "duplicate native inference"
            );
        });
        let stt = Arc::new(SpeechmaticsSttProcessor::new(
            SpeechmaticsSttClient::new(peer.config).unwrap(),
        ));
        let mut user_config = stt.recommended_user_config();
        user_config.empty_user_turn = None;
        let pair =
            LLMContextAggregatorPair::new(Context::default(), user_config, Default::default());
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
        let start = bounded(peer.wire.recv()).await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(start.to_text().unwrap()).unwrap()["turn_config"]
                ["turn_detection_mode"],
            "vad"
        );
        worker
            .sender()
            .send(
                Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                    audio: AudioRawFrame::new(vec![0; 640], 16000, 1).unwrap(),
                    user_id: "unrelated-input-user".into(),
                })),
                Direction::Downstream,
            )
            .await
            .unwrap();
        assert_eq!(
            bounded(peer.wire.recv()).await.unwrap(),
            Message::Binary(vec![0; 640].into())
        );
        peer.send
            .send(json!({"message":"StartOfTurn"}))
            .await
            .unwrap();
        wait(|| downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)) == 1).await;
        peer.send
            .send(segment("AddPartialSegment", "old preview"))
            .await
            .unwrap();
        peer.send
            .send(segment("AddPartialSegment", "replacement preview"))
            .await
            .unwrap();
        peer.send
            .send(segment("AddSegment", "Hello "))
            .await
            .unwrap();
        peer.send
            .send(json!({"message":"SpeechEnded"}))
            .await
            .unwrap();
        peer.send
            .send(segment("AddSegment", "world."))
            .await
            .unwrap();
        wait(||raw.count(|f|matches!(f,FrameData::TranscriptionDetailed(t)|FrameData::InterimTranscriptionDetailed(t) if t.text.text=="world."))==1).await;
        assert_eq!(raw.count(|f|matches!(f,FrameData::TranscriptionDetailed(_))),2,"both AddSegment messages must remain final variants even without an explicit finalize confirmation");
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(
            req_rx.try_recv().is_err(),
            "segment boundary or SpeechEnded cannot close provider turn"
        );
        assert!(pair.context().snapshot().unwrap().messages.is_empty());
        peer.send
            .send(json!({"message":"EndOfTurn"}))
            .await
            .unwrap();
        let request = bounded(req_rx.recv()).await.unwrap();
        // The pinned default TextFrame has includes_inter_frame_spaces=false:
        // preserve provider whitespace, and let the shared aggregator insert its separator.
        assert_eq!(request["messages"][0]["content"], "Hello  world.");
        wait(|| downstream.count(|f| matches!(f, FrameData::LlmResponseEnd)) == 1).await;
        bounded(llm_task).await.unwrap();
        {
            let frames = raw.0.lock().unwrap();
            let finals: Vec<_> = frames
                .iter()
                .enumerate()
                .filter_map(|(i, (f, d))| match (&f.data, d) {
                    (FrameData::TranscriptionDetailed(t), Direction::Downstream) => Some((i, t)),
                    _ => None,
                })
                .collect();
            let stop = frames
                .iter()
                .position(|(f, d)| {
                    *d == Direction::Downstream
                        && matches!(f.data, FrameData::ProposedUserStoppedSpeaking)
                })
                .unwrap();
            assert_eq!(finals.len(), 2);
            assert!(finals[0].0 < finals[1].0 && finals[1].0 < stop);
            let usage: Vec<_> = frames
                .iter()
                .enumerate()
                .flat_map(|(i, (f, _))| match &f.data {
                    FrameData::Metrics(items) => items
                        .iter()
                        .filter_map(move |m| match m {
                            Metric::SttUsage { audio_seconds, .. } => Some((i, *audio_seconds)),
                            _ => None,
                        })
                        .collect::<Vec<_>>(),
                    _ => vec![],
                })
                .collect();
            assert_eq!(usage.len(), 1);
            assert!(usage[0].0 < finals[0].0);
            assert!((usage[0].1 - 0.02).abs() < 1e-9);
            for (_, t) in finals {
                assert!(
                    !t.finalized,
                    "ordinary final segment is not explicit-finalize confirmation"
                );
                assert_eq!(t.user_id, "provider-speaker");
                assert_eq!(t.language.as_deref(), Some("en"));
                assert!(t.result.is_none());
                assert!(!t.timestamp.is_empty());
            }
        }
        assert_eq!(
            downstream.count(|f| matches!(f, FrameData::UserStartedSpeaking)),
            1
        );
        assert_eq!(
            downstream.count(|f| matches!(f, FrameData::UserStoppedSpeaking)),
            1
        );
        assert_eq!(
            downstream.count(|f| matches!(f, FrameData::Interruption)),
            usize::from(should_interrupt)
        );
        worker.end().await.unwrap();
        let (_, report) = bounded(worker.collect()).await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        bounded(peer.task).await.unwrap();
    }
}

#[tokio::test]
async fn review_conversation_interruption_keeps_stt_but_health_restore_revokes_old_socket() {
    let mut peer = peer().await;
    let stt = Arc::new(SpeechmaticsSttProcessor::new(
        SpeechmaticsSttClient::new(peer.config).unwrap(),
    ));
    let health = stt.health().unwrap();
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![stt.clone(), record.clone()])
        .start(Default::default())
        .await
        .unwrap();
    bounded(peer.wire.recv()).await.unwrap();
    peer.send
        .send(segment("AddSegment", "before interruption"))
        .await
        .unwrap();
    wait(||record.count(|f|matches!(f,FrameData::TranscriptionDetailed(t)if t.text.text=="before interruption"))==1).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    wait(|| record.count(|f| matches!(f, FrameData::Interruption)) == 1).await;
    peer.send
        .send(segment("AddSegment", "after interruption"))
        .await
        .unwrap();
    wait(|| {
        record.count(
            |f| matches!(f,FrameData::TranscriptionDetailed(t)if t.text.text=="after interruption"),
        ) == 1
    })
    .await;
    health.set_usable(false);
    health.set_usable(true);
    bounded(peer.task).await.unwrap();
    assert!(stt.sender().unwrap().is_closed());
    assert_eq!(
        record.count(|f| matches!(f, FrameData::TranscriptionDetailed(_))),
        2
    );
    worker.cancel("fixture complete").await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
}

#[tokio::test]
async fn review_pipeline_end_suppresses_drain_transcript_and_turn_callbacks() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechmaticsSttConfig::new("fixture");
    config.endpoint = format!("ws://{}/v2/agent", listener.local_addr().unwrap());
    config.settings.turn_detection_mode = TurnDetectionMode::Vad;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(1);
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        socket.next().await.unwrap().unwrap();
        socket
            .send(Message::Text(
                json!({"message":"RecognitionStarted"}).to_string().into(),
            ))
            .await
            .unwrap();
        let eos = socket.next().await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(eos.to_text().unwrap()).unwrap()["message"],
            "EndOfStream"
        );
        for value in [
            json!({"message":"StartOfTurn"}),
            segment("AddSegment", "drain must stay silent"),
            json!({"message":"EndOfTurn"}),
            json!({"message":"EndOfTranscript"}),
        ] {
            if socket
                .send(Message::Text(value.to_string().into()))
                .await
                .is_err()
            {
                break;
            }
        }
        while let Some(Ok(value)) = socket.next().await {
            if value.is_close() {
                break;
            }
        }
    });
    let record = Arc::new(Recorder::default());
    let worker = Pipeline::new(vec![
        Arc::new(SpeechmaticsSttProcessor::new(
            SpeechmaticsSttClient::new(config).unwrap(),
        )),
        record.clone(),
    ])
    .start(Default::default())
    .await
    .unwrap();
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        record.count(|f| matches!(
            f,
            FrameData::TranscriptionDetailed(_)
                | FrameData::ProposedUserStartedSpeaking
                | FrameData::ProposedUserStoppedSpeaking
        )),
        0,
        "End must fence provider drain output"
    );
}
