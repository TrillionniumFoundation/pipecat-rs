//! Adversarial provider-health retirement and boundary regression tests.
use futures_util::StreamExt;
use pipecat_core::*;
use pipecat_streaming_speech::*;
use std::{sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tokio_tungstenite::accept_async;

async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .expect("pipeline event stalled")
        .expect("pipeline output closed")
}

#[tokio::test]
async fn deepgram_coalesced_recovery_preserves_user_stopped_boundary() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("test-key");
    config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
    let (connected_tx, connected_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        connected_tx.send(()).unwrap();
        while let Some(Ok(_)) = socket.next().await {}
    });
    let mut worker = Pipeline::new(vec![Arc::new(DeepgramSttProcessor::new(
        DeepgramClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    tokio::time::timeout(Duration::from_secs(3), connected_rx)
        .await
        .unwrap()
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    let boundary = Frame::new(FrameData::UserStoppedSpeaking);
    let id = boundary.id;
    worker
        .sender()
        .send(boundary, Direction::Downstream)
        .await
        .unwrap();
    let output = next(&mut worker).await;
    let result =
        matches!(output.frame.data, FrameData::UserStoppedSpeaking) && output.frame.id == id;
    worker.cancel("test cleanup").await.unwrap();
    while worker.recv().await.is_some() {}
    let _ = worker.join().await;
    server.abort();
    assert!(result, "boundary was replaced by {:?}", output.frame.data);
}

async fn read_json(
    socket: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
) -> serde_json::Value {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}

#[tokio::test]
async fn cartesia_coalesced_retirement_drops_old_audio_and_keeps_response_boundaries() {
    use futures_util::SinkExt;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}/tts/websocket", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let (captured_tx, captured_rx) = tokio::sync::oneshot::channel();
    let (stale_tx, stale_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read_json(&mut old).await;
        assert_eq!(request["transcript"], "old");
        captured_tx.send(()).unwrap();
        stale_rx.await.unwrap();
        // Deliberately race delayed provider results against generation retirement.
        let _ = old
            .send(Message::Text(
                json!({"type":"chunk","context_id":request["context_id"],"data":"AQIDBA=="})
                    .to_string()
                    .into(),
            ))
            .await;
        let _ = old.send(Message::Text(json!({"type":"timestamps","context_id":request["context_id"],"word_timestamps":{"words":["stale"],"start":[0.0],"end":[0.1]}}).to_string().into())).await;
        let cancel = read_json(&mut old).await;
        assert_eq!(cancel["cancel"], true);
        let _ = old.close(None).await;
        let mut fresh = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read_json(&mut fresh).await;
        assert_eq!(request["transcript"], "fresh");
        assert_eq!(read_json(&mut fresh).await["continue"], false);
        fresh
            .send(Message::Text(
                json!({"type":"chunk","context_id":request["context_id"],"data":"BQYHCA=="})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        fresh
            .send(Message::Text(
                json!({"type":"done","context_id":request["context_id"]})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
        let _ = fresh.next().await;
    });
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    let old_start = Frame::new(FrameData::LlmResponseStart);
    let old_start_id = old_start.id;
    worker
        .sender()
        .send(old_start, Direction::Downstream)
        .await
        .unwrap();
    worker
        .sender()
        .send(
            Frame::new(FrameData::LlmText(TextFrame::llm("old"))),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), captured_rx)
        .await
        .unwrap()
        .unwrap();
    let mut frames = vec![];
    loop {
        let frame = next(&mut worker).await;
        let started = matches!(frame.frame.data, FrameData::TtsStarted(_));
        frames.push(frame);
        if started {
            break;
        }
    }
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    stale_tx.send(()).unwrap();
    let old_end = Frame::new(FrameData::LlmResponseEnd);
    let old_end_id = old_end.id;
    let fresh_start = Frame::new(FrameData::LlmResponseStart);
    let fresh_start_id = fresh_start.id;
    let fresh_end = Frame::new(FrameData::LlmResponseEnd);
    let fresh_end_id = fresh_end.id;
    let flush = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        ..Default::default()
    }));
    let flush_id = flush.id;
    for frame in [
        old_end,
        flush,
        fresh_start,
        Frame::new(FrameData::LlmText(TextFrame::llm("fresh"))),
        fresh_end,
    ] {
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }
    worker.end().await.unwrap();
    let (tail, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    frames.extend(tail);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(health.is_usable());
    let flushes: Vec<_> = frames.iter().filter(|f| f.frame.id == flush_id).collect();
    assert_eq!(flushes.len(), 1);
    assert!(matches!(&flushes[0].frame.data, FrameData::FlushDetailed(f) if f.returning));
    for id in [old_start_id, old_end_id, fresh_start_id, fresh_end_id] {
        assert_eq!(frames.iter().filter(|f| f.frame.id == id).count(), 1);
    }
    assert_eq!(
        frames
            .iter()
            .find(|f| f.frame.id == old_end_id)
            .unwrap()
            .frame
            .metadata
            .get("failed"),
        Some(&json!(true))
    );
    assert!(!frames
        .iter()
        .find(|f| f.frame.id == fresh_end_id)
        .unwrap()
        .frame
        .metadata
        .contains_key("failed"));
    let stops: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .collect();
    assert_eq!(stops.len(), 2);
    assert_eq!(stops[0].frame.metadata.get("failed"), Some(&json!(true)));
    assert!(!stops[1].frame.metadata.contains_key("failed"));
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .collect();
    assert_eq!(audio, [vec![5, 6, 7, 8]]);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

fn review_audio(bytes: [u8; 2]) -> Frame {
    Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(bytes.to_vec(), 16000, 1).unwrap(),
    ))
}
async fn review_send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn review_barrier(worker: &mut PipelineWorker, frames: &mut Vec<DirectedFrame>) {
    let marker = Frame::text("admitted review barrier");
    let id = marker.id;
    review_send(worker, marker).await;
    loop {
        let frame = next(worker).await;
        let found = frame.frame.id == id;
        frames.push(frame);
        if found {
            return;
        }
    }
}

#[tokio::test]
async fn exhausted_deepgram_budget_requires_effective_settings_and_never_replays_audio() {
    use futures_util::SinkExt;
    use serde_json::json;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::{
        accept_hdr_async,
        tungstenite::{
            handshake::server::{Request, Response},
            Message,
        },
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("test-key");
    config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let original_model = config.model.clone();
    let (retry_tx, retry_rx) = tokio::sync::oneshot::channel();
    let (fail_tx, fail_rx) = tokio::sync::oneshot::channel();
    let (check_tx, check_rx) = tokio::sync::oneshot::channel();
    let (checked_tx, checked_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(
            first.next().await.unwrap().unwrap().into_data().as_ref(),
            &[1, 2]
        );
        first.close(None).await.unwrap();
        let (mut retry, _) = listener.accept().await.unwrap();
        let mut headers = vec![];
        while !headers.ends_with(b"\r\n\r\n") {
            headers.push(retry.read_u8().await.unwrap());
        }
        retry_tx.send(()).unwrap();
        fail_rx.await.unwrap();
        retry.write_all(b"HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").await.unwrap();
        drop(retry);
        check_rx.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(75), listener.accept())
                .await
                .is_err(),
            "an exhausted provider reopened before recovery"
        );
        checked_tx.send(()).unwrap();
        #[allow(clippy::result_large_err)]
        let mut fresh = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &Request, response: Response| {
                assert!(request.uri().query().unwrap().contains("model=nova-fresh"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            fresh.next().await.unwrap().unwrap().into_data().as_ref(),
            &[3, 4],
            "old admitted or skipped audio was replayed"
        );
        assert_eq!(read_json(&mut fresh).await, json!({"type":"CloseStream"}));
        fresh.send(Message::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"channel":{"alternatives":[{"transcript":"fresh","languages":["en"]}]}}).to_string().into())).await.unwrap();
        fresh.close(None).await.unwrap();
    });
    let processor = Arc::new(
        DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap()).with_reconnect_policy(
            ReconnectPolicy {
                max_attempts: 1,
                initial_delay: Duration::from_millis(1),
                max_delay: Duration::from_millis(1),
                stable_after: Duration::from_secs(60),
            },
        ),
    );
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    let mut frames = vec![];
    review_send(&worker, review_audio([1, 2])).await;
    tokio::time::timeout(Duration::from_secs(3), retry_rx)
        .await
        .unwrap()
        .unwrap();
    review_send(&worker, review_audio([9, 10])).await;
    review_barrier(&mut worker, &mut frames).await;
    assert!(
        health.is_usable(),
        "a single transient failure changed usability before exhaustion"
    );
    fail_tx.send(()).unwrap();
    loop {
        let frame = next(&mut worker).await;
        let exhausted = matches!(&frame.frame.data, FrameData::Error(e) if e.error.contains("recovery exhausted after 1 attempts"));
        if exhausted {
            assert_eq!(
                frame.frame.data.as_error_frame().unwrap().category,
                ErrorCategory::Connectivity
            );
        }
        frames.push(frame);
        if exhausted {
            break;
        }
    }
    assert!(!health.is_usable());
    let revision = health.snapshot().revision;
    let invalid = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(SttSettings {
                base: ServiceSettings {
                    model: Setting::Null,
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        },
    )));
    let no_op = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(SttSettings {
                base: ServiceSettings {
                    model: Setting::Value(original_model.clone()),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        },
    )));
    review_send(&worker, review_audio([11, 12])).await;
    review_send(&worker, invalid).await;
    review_send(&worker, no_op).await;
    review_barrier(&mut worker, &mut frames).await;
    assert!(!health.is_usable());
    assert_eq!(health.snapshot().revision, revision);
    assert_eq!(processor.config_snapshot().unwrap().model, original_model);
    assert!(frames.iter().any(|f| matches!(&f.frame.data, FrameData::Error(e) if e.category == ErrorCategory::InvalidRequest)));
    check_tx.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(3), checked_rx)
        .await
        .unwrap()
        .unwrap();
    let recovery = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(SttSettings {
                base: ServiceSettings {
                    model: Setting::Value("nova-fresh".into()),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        },
    )));
    review_send(&worker, recovery).await;
    review_barrier(&mut worker, &mut frames).await;
    assert!(health.is_usable());
    assert_eq!(health.snapshot().revision, revision + 1);
    review_send(&worker, review_audio([3, 4])).await;
    worker.end().await.unwrap();
    let (tail, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    frames.extend(tail);
    assert_eq!(report.state, WorkerState::Ended);
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text == "fresh")));
    assert!(health.is_usable());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn zero_deepgram_reconnect_budget_remains_usable_for_new_input_session() {
    use futures_util::SinkExt;
    use serde_json::json;
    use tokio_tungstenite::tungstenite::Message;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramConfig::new("test-key");
    config.endpoint = format!("ws://{}/v1/listen", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let (idle_tx, idle_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(
            old.next().await.unwrap().unwrap().into_data().as_ref(),
            &[1, 2]
        );
        old.close(None).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(75), listener.accept())
                .await
                .is_err(),
            "zero budget must disable automatic retries"
        );
        idle_tx.send(()).unwrap();
        let mut fresh = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(
            fresh.next().await.unwrap().unwrap().into_data().as_ref(),
            &[3, 4]
        );
        assert_eq!(read_json(&mut fresh).await, json!({"type":"CloseStream"}));
        fresh.send(Message::Text(json!({"type":"Results","is_final":true,"from_finalize":true,"channel":{"alternatives":[{"transcript":"fresh","languages":["en"]}]}}).to_string().into())).await.unwrap();
        fresh.close(None).await.unwrap();
    });
    let processor = Arc::new(
        DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap()).with_reconnect_policy(
            ReconnectPolicy {
                max_attempts: 0,
                ..Default::default()
            },
        ),
    );
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    review_send(&worker, review_audio([1, 2])).await;
    loop {
        let frame = next(&mut worker).await;
        if let FrameData::Error(error) = frame.frame.data {
            assert_eq!(error.category, ErrorCategory::Connectivity);
            assert!(!error.error.contains("recovery exhausted"));
            break;
        }
    }
    tokio::time::timeout(Duration::from_secs(3), idle_rx)
        .await
        .unwrap()
        .unwrap();
    assert!(health.is_usable());
    assert_eq!(health.snapshot().revision, 0);
    review_send(&worker, review_audio([3, 4])).await;
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text == "fresh")));
    assert!(health.is_usable());
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
