//! Coalesced usability transitions retire complete provider generations.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_elevenlabs::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

async fn read(socket: &mut WebSocketStream<TcpStream>) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn write(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn coalesced_tts_retirement_discards_audio_words_and_sentence_tail() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("test-key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let old_context = read(&mut old).await["context_id"].clone();
        assert_eq!(read(&mut old).await["text"], "Old.");
        ready_tx.send(()).unwrap();
        assert_eq!(read(&mut old).await["close_context"], true);
        // Race a delayed provider result against retirement cleanup.
        write(&mut old, json!({"contextId":old_context,"audio":"AQI=","alignment":{"chars":["o","l","d"," "],"charStartTimesMs":[0,1,2,3],"charDurationsMs":[1,1,1,1]}})).await;
        assert_eq!(read(&mut old).await["close_socket"], true);
        old.close(None).await.unwrap();
        let mut fresh = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut fresh).await["context_id"].clone();
        assert_ne!(old_context, context);
        assert_eq!(read(&mut fresh).await["text"], "Fresh.");
        assert_eq!(read(&mut fresh).await["flush"], true);
        assert_eq!(read(&mut fresh).await["close_context"], true);
        write(&mut fresh, json!({"contextId":context,"audio":"AwQ=","alignment":{"chars":["F","r","e","s","h","."],"charStartTimesMs":[0,1,2,3,4,5],"charDurationsMs":[1,1,1,1,1,1]},"isFinal":true})).await;
        assert_eq!(read(&mut fresh).await["close_socket"], true);
        fresh.close(None).await.unwrap();
    });
    let mut worker = Pipeline::new(vec![Arc::new(ElevenLabsTtsProcessor::new(
        ElevenLabsTtsClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    let old_start = Frame::new(FrameData::LlmResponseStart);
    let old_start_id = old_start.id;
    send(&worker, old_start).await;
    send(
        &worker,
        Frame::new(FrameData::LlmText(TextFrame::llm("Old. Leaked tail"))),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), ready_rx)
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
        Frame::new(FrameData::LlmText(TextFrame::llm("Fresh."))),
        fresh_end,
    ] {
        send(&worker, frame).await;
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
    assert_eq!(audio, [vec![3, 4]]);
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsText(t) => Some(t.text.text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(words, ["Fresh."]);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn coalesced_stt_retirement_drops_old_transcript_and_starts_fresh_audio() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("test-key");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (late_tx, late_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut old).await["audio_base_64"], "AQI=");
        ready_tx.send(()).unwrap();
        late_rx.await.unwrap();
        let _ = old
            .send(Message::Text(
                json!({"message_type":"partial_transcript","text":"stale"})
                    .to_string()
                    .into(),
            ))
            .await;
        while let Some(Ok(message)) = old.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
        let _ = old.close(None).await;
        let mut fresh = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut fresh).await["audio_base_64"], "AwQ=");
        assert_eq!(read(&mut fresh).await["commit"], true);
        write(
            &mut fresh,
            json!({"message_type":"committed_transcript","text":"fresh"}),
        )
        .await;
        while let Some(Ok(message)) = fresh.next().await {
            if matches!(message, Message::Close(_)) {
                break;
            }
        }
    });
    let processor = Arc::new(ElevenLabsSttProcessor::new(
        ElevenLabsSttClient::new(config).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    let old_audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![1, 2], 16000, 1).unwrap(),
    ));
    let old_audio_id = old_audio.id;
    send(&worker, old_audio).await;
    tokio::time::timeout(Duration::from_secs(3), ready_rx)
        .await
        .unwrap()
        .unwrap();
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    late_tx.send(()).unwrap();
    let fresh_audio = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![3, 4], 16000, 1).unwrap(),
    ));
    let fresh_audio_id = fresh_audio.id;
    let boundary = Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
        duration_secs: 0.01,
        timestamp: 1.0,
    }));
    let boundary_id = boundary.id;
    send(&worker, fresh_audio).await;
    send(&worker, boundary).await;
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    for id in [old_audio_id, fresh_audio_id, boundary_id] {
        assert_eq!(frames.iter().filter(|f| f.frame.id == id).count(), 1);
    }
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::Transcription(t) | FrameData::InterimTranscription(t) => {
                Some(t.text.as_str())
            }
            _ => None,
        })
        .collect();
    assert_eq!(transcripts, ["fresh"]);
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}
