//! Source context outcomes over the actual local Cartesia WebSocket protocol.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

#[derive(Clone, Copy)]
enum Reply {
    Silent,
    Empty,
    Audio,
    Error,
    Timeout,
}
async fn read(socket: &mut WebSocketStream<TcpStream>) -> Value {
    serde_json::from_str(socket.next().await.unwrap().unwrap().to_text().unwrap()).unwrap()
}
async fn write(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn fixture(replies: Vec<Reply>) -> (CartesiaConfig, tokio::task::JoinHandle<()>) {
    fixture_with_response_timeout(replies, Duration::from_secs(1)).await
}
async fn fixture_with_response_timeout(
    replies: Vec<Reply>,
    response_timeout: Duration,
) -> (CartesiaConfig, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    config.limits.response_timeout = response_timeout;
    config.limits.close_timeout = Duration::from_millis(20);
    let server = tokio::spawn(async move {
        for reply in replies {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = read(&mut socket).await;
            let id = request["context_id"].clone();
            assert_eq!(read(&mut socket).await["continue"], false);
            match reply {
                Reply::Audio | Reply::Empty => write(&mut socket, json!({"type":"chunk", "context_id":id, "data": if matches!(reply, Reply::Audio) {"AQI="} else {""}})).await,
                Reply::Error => write(&mut socket, json!({"type":"error", "context_id":id, "status_code":503})).await,
                _ => {},
            }
            if !matches!(reply, Reply::Error | Reply::Timeout) {
                write(&mut socket, json!({"type":"done", "context_id":id})).await;
            }
            while let Some(Ok(message)) = socket.next().await {
                if matches!(message, Message::Close(_)) {
                    break;
                }
            }
        }
    });
    (config, server)
}
async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(5), future)
        .await
        .expect("operation stalled")
}
async fn utterance(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    for frame in [
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::LlmText(TextFrame::llm("Hello."))),
        end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    let mut frames = vec![];
    loop {
        let frame = bounded(worker.recv()).await.unwrap();
        let done = frame.frame.id == end_id;
        frames.push(frame);
        if done {
            return frames;
        }
    }
}
fn errors(frames: &[DirectedFrame]) -> Vec<&Frame> {
    frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
        .map(|f| &f.frame)
        .collect()
}
fn assert_silent(frames: &[DirectedFrame]) {
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::TtsAudio(_)
            | FrameData::TtsText(_)
            | FrameData::BotStartedSpeaking
            | FrameData::BotStoppedSpeaking
    )));
    assert!(!frames.iter().any(|f| matches!(&f.frame.data, FrameData::TtsAlignmentDiagnostic(d) if d.evidence != TtsAlignmentEvidence::Unreported)));
}

#[tokio::test]
async fn default_three_silent_contexts_report_then_skip_and_recover_with_fresh_context() {
    let (config, server) = fixture(vec![Reply::Silent; 4]).await;
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let health = worker.processor_health(0).unwrap();
    let mut ids = std::collections::HashSet::new();
    for i in 0..3 {
        let frames = utterance(&mut worker).await;
        assert_silent(&frames);
        assert_eq!(errors(&frames).len(), 1);
        assert_eq!(
            errors(&frames)[0]
                .error_origin()
                .unwrap()
                .reported_health
                .is_usable,
            i < 2
        );
        assert_eq!(health.is_usable(), i < 2);
        assert_eq!(
            frames.last().unwrap().frame.metadata.get("failed"),
            Some(&json!(true))
        );
        for frame in &frames {
            if let FrameData::TtsStarted(start) = &frame.frame.data {
                assert!(ids.insert(start.context_id.clone().unwrap()));
            }
        }
    }
    assert!(errors(&utterance(&mut worker).await).is_empty());
    health.set_usable(true);
    assert_eq!(errors(&utterance(&mut worker).await).len(), 1);
    assert!(health.is_usable());
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn zero_cutoff_and_real_audio_reset_preserve_each_silent_error() {
    for maximum in [0, 2] {
        let (config, server) = fixture(vec![
            Reply::Silent,
            Reply::Audio,
            Reply::Silent,
            Reply::Silent,
        ])
        .await;
        let mut worker = Pipeline::new(vec![Arc::new(
            CartesiaTtsProcessor::new(CartesiaClient::new(config).unwrap())
                .with_max_consecutive_zero_audio_contexts(maximum),
        )])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        for i in 0..4 {
            let frames = utterance(&mut worker).await;
            assert_eq!(
                errors(&frames).len(),
                usize::from(i != 1),
                "maximum={maximum} utterance={i} errors={:?} audio_frames={} health={:?}",
                errors(&frames)
                    .iter()
                    .map(|frame| &frame.data)
                    .collect::<Vec<_>>(),
                frames
                    .iter()
                    .filter(|frame| matches!(frame.frame.data, FrameData::TtsAudio(_)))
                    .count(),
                worker.processor_health(0).unwrap().snapshot(),
            );
            assert_eq!(
                worker.processor_health(0).unwrap().is_usable(),
                maximum == 0 || i < 3
            );
        }
        worker.end().await.unwrap();
        bounded(worker.collect()).await.unwrap();
        bounded(server).await.unwrap();
    }
}

#[tokio::test]
async fn empty_chunk_counts_source_presence_without_pcm_or_clock_and_resets_streak() {
    let (config, server) = fixture(vec![Reply::Silent, Reply::Empty, Reply::Silent]).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        CartesiaTtsProcessor::new(CartesiaClient::new(config).unwrap())
            .with_max_consecutive_zero_audio_contexts(2),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    for i in 0..3 {
        let frames = utterance(&mut worker).await;
        assert_silent(&frames);
        assert_eq!(
            errors(&frames).len(),
            usize::from(i != 1),
            "empty-presence utterance={i} errors={:?} audio_frames={} health={:?}",
            errors(&frames)
                .iter()
                .map(|frame| &frame.data)
                .collect::<Vec<_>>(),
            frames
                .iter()
                .filter(|frame| matches!(frame.frame.data, FrameData::TtsAudio(_)))
                .count(),
            worker.processor_health(0).unwrap().snapshot(),
        );
        assert!(worker.processor_health(0).unwrap().is_usable());
    }
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn errors_and_timeouts_count_but_explicit_usable_true_resets_the_streak() {
    let (config, server) = fixture(vec![Reply::Error, Reply::Timeout, Reply::Silent]).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        CartesiaTtsProcessor::new(CartesiaClient::new(config).unwrap())
            .with_max_consecutive_zero_audio_contexts(2),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let health = worker.processor_health(0).unwrap();
    assert_eq!(errors(&utterance(&mut worker).await).len(), 2);
    assert!(!health.set_usable(true));
    assert_eq!(errors(&utterance(&mut worker).await).len(), 2);
    assert!(health.is_usable());
    assert_eq!(errors(&utterance(&mut worker).await).len(), 1);
    assert!(!health.is_usable());
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn unanswered_context_enforces_100ms_response_deadline_without_inventing_audio() {
    let (config, server) =
        fixture_with_response_timeout(vec![Reply::Timeout], Duration::from_millis(100)).await;
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    let frames = utterance(&mut worker).await;
    let usable = worker.processor_health(0).unwrap().is_usable();
    worker.end().await.unwrap();
    let (tail, report) = bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();

    let reported: Vec<_> = frames
        .iter()
        .filter_map(|frame| match &frame.frame.data {
            FrameData::Error(error) => Some(error),
            _ => None,
        })
        .collect();
    assert_eq!(reported.len(), 2, "{reported:?}");
    assert_eq!(
        reported
            .iter()
            .filter(|error| error.category == ErrorCategory::Connectivity
                && error.error == "Cartesia response timed out")
            .count(),
        1,
        "{reported:?}"
    );
    assert_eq!(
        reported
            .iter()
            .filter(|error| error.category == ErrorCategory::Unknown
                && error.error.contains("completed with no audio"))
            .count(),
        1,
        "{reported:?}"
    );
    assert_silent(&frames);
    assert_silent(&tail);
    assert_eq!(
        frames.last().unwrap().frame.metadata.get("failed"),
        Some(&json!(true))
    );
    assert!(usable);
    assert_eq!(report.errors.len(), 2);
    assert_eq!(report.state, WorkerState::Ended);
}
