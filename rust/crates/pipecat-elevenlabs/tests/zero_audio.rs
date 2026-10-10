//! Source context outcomes over the actual local ElevenLabs WebSocket protocol.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_elevenlabs::*;
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
async fn fixture(replies: Vec<Reply>) -> (ElevenLabsTtsConfig, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.response_timeout = Duration::from_millis(100);
    config.limits.close_timeout = Duration::from_millis(20);
    let server = tokio::spawn(async move {
        for reply in replies {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = read(&mut socket).await;
            let id = request["context_id"].clone();
            assert_eq!(read(&mut socket).await["text"], "Hello.");
            assert_eq!(read(&mut socket).await["flush"], true);
            assert_eq!(read(&mut socket).await["close_context"], true);
            match reply {
                Reply::Audio | Reply::Empty => write(&mut socket, json!({"contextId":id, "audio": if matches!(reply, Reply::Audio) {"AQI="} else {""}})).await,
                Reply::Error => write(&mut socket, json!({"contextId":id, "error":"failed", "code":503})).await,
                _ => {},
            }
            if !matches!(reply, Reply::Error | Reply::Timeout) {
                write(&mut socket, json!({"contextId":id, "isFinal":true})).await;
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
    let mut worker = Pipeline::new(vec![Arc::new(ElevenLabsTtsProcessor::new(
        ElevenLabsTtsClient::new(config).unwrap(),
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
            ElevenLabsTtsProcessor::new(ElevenLabsTtsClient::new(config).unwrap())
                .with_max_consecutive_zero_audio_contexts(maximum),
        )])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        for i in 0..4 {
            assert_eq!(
                errors(&utterance(&mut worker).await).len(),
                usize::from(i != 1)
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
async fn empty_audio_field_does_not_count_source_presence_or_make_playout() {
    let (config, server) = fixture(vec![Reply::Silent, Reply::Empty]).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        ElevenLabsTtsProcessor::new(ElevenLabsTtsClient::new(config).unwrap())
            .with_max_consecutive_zero_audio_contexts(2),
    )])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    for i in 0..2 {
        let frames = utterance(&mut worker).await;
        assert_silent(&frames);
        assert_eq!(errors(&frames).len(), 1);
        assert_eq!(worker.processor_health(0).unwrap().is_usable(), i == 0);
    }
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn errors_and_timeouts_count_but_explicit_usable_true_resets_the_streak() {
    let (config, server) = fixture(vec![Reply::Error, Reply::Timeout, Reply::Silent]).await;
    let mut worker = Pipeline::new(vec![Arc::new(
        ElevenLabsTtsProcessor::new(ElevenLabsTtsClient::new(config).unwrap())
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
