//! Independent processor admission, empty-stream and health watchdog regressions.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_soniox::*;
use serde_json::{json, Value};
use std::{future::Future, sync::Arc, time::Duration};
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("Soniox lifecycle stalled")
}
async fn send(w: &PipelineWorker, data: FrameData) {
    w.sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn until(w: &mut PipelineWorker, id: u64) -> Vec<DirectedFrame> {
    let mut out = vec![];
    loop {
        let f = bounded(w.recv()).await.unwrap();
        let done = f.frame.id == id;
        out.push(f);
        if done {
            return out;
        }
    }
}
async fn utterance(w: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let end = Frame::new(FrameData::LlmResponseEnd);
    let id = end.id;
    send(w, FrameData::LlmResponseStart).await;
    send(w, FrameData::LlmText(TextFrame::llm("Hello."))).await;
    w.sender().send(end, Direction::Downstream).await.unwrap();
    until(w, id).await
}
async fn fixture() -> (
    Arc<SonioxTtsProcessor>,
    mpsc::Receiver<Value>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SonioxTtsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/tts-websocket", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let (tx, rx) = mpsc::channel(64);
    let task = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        while let Some(Ok(message)) = ws.next().await {
            match message {
                Message::Text(text) => {
                    let value: Value = serde_json::from_str(&text).unwrap();
                    tx.send(value.clone()).await.unwrap();
                    if value["text_end"] == true {
                        ws.send(Message::Text(
                            json!({"stream_id":value["stream_id"],"audio":""})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                        ws.send(Message::Text(
                            json!({"stream_id":value["stream_id"],"terminated":true})
                                .to_string()
                                .into(),
                        ))
                        .await
                        .unwrap();
                    }
                }
                Message::Close(_) => break,
                _ => (),
            }
        }
    });
    (
        Arc::new(
            SonioxTtsProcessor::new(SonioxTtsClient::new(config).unwrap())
                .with_max_consecutive_zero_audio_contexts(2),
        ),
        rx,
        task,
    )
}
#[tokio::test]
async fn empty_tool_only_turn_cancels_configured_stream_without_silent_error_or_failed_end() {
    let (processor, mut wire, server) = fixture().await;
    let mut worker = Pipeline::new(vec![processor])
        .start(Default::default())
        .await
        .unwrap();
    let start = Frame::new(FrameData::LlmResponseStart);
    let start_id = start.id;
    worker
        .sender()
        .send(start, Direction::Downstream)
        .await
        .unwrap();
    let config = bounded(wire.recv()).await.unwrap();
    assert!(config.get("api_key").is_some());
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    let frames = until(&mut worker, end_id).await;
    let cancel = bounded(wire.recv()).await.unwrap();
    assert_eq!(
        cancel,
        json!({"stream_id":config["stream_id"],"cancel":true})
    );
    assert_eq!(frames.iter().filter(|f| f.frame.id == start_id).count(), 1);
    assert_eq!(frames.last().unwrap().frame.metadata.get("failed"), None);
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::Error(_)
            | FrameData::TtsAudio(_)
            | FrameData::TtsText(_)
            | FrameData::TtsStarted(_)
            | FrameData::TtsStopped(_)
    )));
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert!(report.errors.is_empty());
    bounded(server).await.unwrap();
}
#[tokio::test]
async fn empty_audio_fails_response_and_disabled_health_has_no_provider_admission_until_restore() {
    let (processor, mut wire, server) = fixture().await;
    let mut worker = Pipeline::new(vec![processor])
        .start(Default::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    let mut ids = std::collections::HashSet::new();
    for i in 0..2 {
        let frames = utterance(&mut worker).await;
        assert_eq!(
            frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
                .count(),
            1
        );
        assert!(frames.last().unwrap().frame.metadata.get("failed") == Some(&json!(true)));
        assert!(!frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::TtsAudio(_) | FrameData::TtsText(_) | FrameData::BotStartedSpeaking
        )));
        let config = bounded(wire.recv()).await.unwrap();
        assert!(ids.insert(config["stream_id"].clone().as_str().unwrap().to_owned()));
        assert_eq!(bounded(wire.recv()).await.unwrap()["text"], "Hello.");
        assert_eq!(bounded(wire.recv()).await.unwrap()["text_end"], true);
        assert_eq!(health.is_usable(), i == 0);
    }
    let frames = utterance(&mut worker).await;
    assert!(frames.last().unwrap().frame.metadata.get("failed") == Some(&json!(true)));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
            .count(),
        0
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(80), wire.recv())
            .await
            .is_err(),
        "unusable processor opened a provider stream"
    );
    health.set_usable(true);
    let frames = utterance(&mut worker).await;
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::Error(_)))
            .count(),
        1
    );
    assert!(health.is_usable());
    let config = bounded(wire.recv()).await.unwrap();
    assert!(ids.insert(config["stream_id"].as_str().unwrap().to_owned()));
    assert_eq!(bounded(wire.recv()).await.unwrap()["text"], "Hello.");
    assert_eq!(bounded(wire.recv()).await.unwrap()["text_end"], true);
    worker.end().await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}
#[tokio::test]
async fn rejected_and_noop_settings_do_not_flush_pending_text_or_rotate_stream() {
    let (processor, mut wire, server) = fixture().await;
    let before = processor.config_snapshot().unwrap();
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    let config = bounded(wire.recv()).await.unwrap();
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello"))).await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [("speed".into(), json!(9.0))].into(),
            ..Default::default()
        })),
    )
    .await;
    loop {
        let f = bounded(worker.recv()).await.unwrap();
        if let FrameData::Error(error) = f.frame.data {
            assert_eq!(error.category, ErrorCategory::InvalidRequest);
            break;
        }
    }
    assert_eq!(processor.config_snapshot().unwrap(), before);
    assert!(health.is_usable());
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value("Bryce".into()),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::LlmText(TextFrame::llm("."))).await;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let end_id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    let frames = until(&mut worker, end_id).await;
    let text = bounded(wire.recv()).await.unwrap();
    assert_eq!(text["stream_id"], config["stream_id"]);
    assert_eq!(text["text"], "Hello.");
    let end = bounded(wire.recv()).await.unwrap();
    assert_eq!(end["stream_id"], config["stream_id"]);
    assert_eq!(end["text_end"], true);
    assert_eq!(processor.config_snapshot().unwrap(), before);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStarted(_)))
            .count(),
        1
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(60), wire.recv())
            .await
            .is_err(),
        "no-op or rejected update configured an extra stream"
    );
    worker.end().await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn timed_out_eager_stream_reports_failure_without_unpaired_tts_stop() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SonioxTtsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.limits.response_timeout = Duration::from_millis(80);
    config.limits.close_timeout = Duration::from_millis(50);
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let init = bounded(ws.next()).await.unwrap().unwrap();
        let init: Value = serde_json::from_str(init.to_text().unwrap()).unwrap();
        let cancel = bounded(ws.next()).await.unwrap().unwrap();
        let cancel: Value = serde_json::from_str(cancel.to_text().unwrap()).unwrap();
        assert_eq!(cancel, json!({"stream_id":init["stream_id"],"cancel":true}));
        while let Some(Ok(message)) = ws.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });
    let mut worker = Pipeline::new(vec![Arc::new(SonioxTtsProcessor::new(
        SonioxTtsClient::new(config).unwrap(),
    ))])
    .start(Default::default())
    .await
    .unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    let mut frames = vec![];
    loop {
        let frame = bounded(worker.recv()).await.unwrap();
        let error = matches!(frame.frame.data, FrameData::Error(_));
        frames.push(frame);
        if error {
            break;
        }
    }
    let end = Frame::new(FrameData::LlmResponseEnd);
    let id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    frames.extend(until(&mut worker, id).await);
    assert!(!frames.iter().any(|f| matches!(
        f.frame.data,
        FrameData::TtsStarted(_) | FrameData::TtsStopped(_)
    )));
    assert_eq!(
        frames.last().unwrap().frame.metadata.get("failed"),
        Some(&json!(true))
    );
    worker.end().await.unwrap();
    let _ = bounded(worker.collect()).await.unwrap();
    bounded(server).await.unwrap();
}
