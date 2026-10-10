use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_elevenlabs::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};
type Socket = WebSocketStream<TcpStream>;
async fn read(ws: &mut Socket) -> Value {
    let m = tokio::time::timeout(Duration::from_secs(3), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(m.to_text().unwrap()).unwrap()
}
async fn write(ws: &mut Socket, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn setup() -> (TcpListener, Arc<ElevenLabsTtsProcessor>, PipelineWorker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("secret", "old-voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_secs(2);
    let processor = Arc::new(ElevenLabsTtsProcessor::new(
        ElevenLabsTtsClient::new(config).unwrap(),
    ));
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    (listener, processor, worker)
}
async fn respond(ws: &mut Socket, context: &Value, word: &str) {
    let chars: Vec<_> = word.chars().map(|c| c.to_string()).collect();
    write(ws,json!({"contextId":context,"audio":"AQIDBA==","alignment":{"chars":chars,"charStartTimesMs":vec![0;chars.len()],"charDurationsMs":vec![1;chars.len()]}})).await;
    write(ws, json!({"contextId":context,"isFinal":true})).await;
    assert_eq!(read(ws).await, json!({"close_socket":true}));
    ws.close(None).await.unwrap();
}
#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite callback has a fixed HTTP error type.
async fn settings_rotate_snapshots_and_preserve_original_source_boundaries() {
    let (listener, processor, worker) = setup().await;
    let server = tokio::spawn(async move {
        for (voice, text, stability) in [
            ("old-voice", "Hello.", None),
            ("new-voice", "你好", Some(0.7)),
        ] {
            let mut ws = accept_hdr_async(
                listener.accept().await.unwrap().0,
                |request: &Request, response: Response| {
                    assert!(request.uri().path().contains(voice));
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let init = read(&mut ws).await;
            assert_eq!(init["text"], " ");
            assert_eq!(
                init.get("voice_settings")
                    .and_then(|v| v["stability"].as_f64()),
                stability
            );
            assert_eq!(read(&mut ws).await["text"], text);
            assert_eq!(read(&mut ws).await["flush"], true);
            assert_eq!(read(&mut ws).await["close_context"], true);
            respond(&mut ws, &init["context_id"], text).await;
        }
    });
    let mut start = Frame::new(FrameData::LlmResponseStart);
    start
        .metadata
        .insert("boundary".into(), json!("original-start"));
    let start_id = start.id;
    worker
        .sender()
        .send(start, Direction::Downstream)
        .await
        .unwrap();
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello."))).await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value("new-voice".into()),
                language: Setting::Value("zh".into()),
                base: ServiceSettings {
                    extra: [("stability".into(), json!(0.7))].into(),
                    ..Default::default()
                },
            }),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::LlmText(TextFrame::llm("你好"))).await;
    let mut end = Frame::new(FrameData::LlmResponseEnd);
    end.metadata
        .insert("boundary".into(), json!("original-end"));
    let end_id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let starts: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::LlmResponseStart))
        .collect();
    let ends: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .collect();
    assert_eq!(starts.len(), 1);
    assert_eq!(ends.len(), 1);
    assert_eq!(starts[0].frame.id, start_id);
    assert_eq!(ends[0].frame.id, end_id);
    assert_eq!(ends[0].frame.metadata["boundary"], "original-end");
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::TtsText(t) = &f.frame.data {
                Some(t)
            } else {
                None
            }
        })
        .collect();
    assert_eq!(words.len(), 2);
    assert_eq!(words[0].text.text, "Hello.");
    assert_eq!(words[1].text.text, "你好");
    assert!(words[1].text.includes_inter_frame_spaces);
    assert_eq!(processor.config_snapshot().unwrap().voice_id, "new-voice");
    let last_stop = frames
        .iter()
        .rposition(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .unwrap();
    let response_end = frames.iter().position(|f| f.frame.id == end_id).unwrap();
    assert!(last_stop < response_end);
    server.await.unwrap();
}
#[tokio::test]
async fn failure_preserves_end_with_failure_metadata_and_no_invented_audio() {
    let (listener, _, worker) = setup().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        read(&mut ws).await;
        read(&mut ws).await;
        write(&mut ws, json!({"error":"rejected","code":429})).await;
        loop {
            if read(&mut ws).await.get("close_socket") == Some(&json!(true)) {
                break;
            }
        }
        ws.close(None).await.unwrap();
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello."))).await;
    let end = Frame::new(FrameData::LlmResponseEnd);
    let id = end.id;
    worker
        .sender()
        .send(end, Direction::Downstream)
        .await
        .unwrap();
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.errors.len(), 2);
    assert!(report.errors[1].error.contains("completed with no audio"));
    let end = frames.iter().find(|f| f.frame.id == id).unwrap();
    assert_eq!(end.frame.metadata["failed"], true);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_) | FrameData::TtsText(_))));
    server.await.unwrap();
}
#[tokio::test]
async fn stt_pipeline_vad_commit_survives_interruption_and_drains_final() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("secret");
    config.endpoint = format!(
        "ws://{}/v1/speech-to-text/realtime",
        listener.local_addr().unwrap()
    );
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(read(&mut ws).await["commit"], false);
        write(
            &mut ws,
            json!({"message_type":"partial_transcript","text":"hello"}),
        )
        .await;
        assert_eq!(read(&mut ws).await["commit"], true);
        write(
            &mut ws,
            json!({"message_type":"committed_transcript","text":"hello!"}),
        )
        .await;
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
    });
    let processor = Arc::new(
        ElevenLabsSttProcessor::new(ElevenLabsSttClient::new(config).unwrap())
            .with_user_id("speaker"),
    );
    let worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, FrameData::Interruption).await;
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![1, 2, 3, 4], 16000, 1).unwrap()),
    )
    .await;
    send(
        &worker,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
    )
    .await;
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert!(frames.iter().any(|f|matches!(&f.frame.data,FrameData::Transcription(t)if t.text=="hello!"&&t.user_id=="speaker"&&t.finalized)));
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::InterimTranscription(_))));
    server.await.unwrap();
}
#[tokio::test]
async fn settings_reject_unsupported_fields_without_mutation() {
    let (_listener, processor, worker) = setup().await;
    let before = processor.config_snapshot().unwrap();
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            settings: [
                ("dialogue".into(), json!(true)),
                ("voice".into(), json!("changed")),
            ]
            .into(),
            ..Default::default()
        })),
    )
    .await;
    worker.sender().end().await.unwrap();
    let (_, report) = worker.collect().await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(processor.config_snapshot().unwrap(), before);
}

#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite callback has a fixed HTTP error type.
async fn stt_settings_drain_old_audio_before_reconnecting_with_new_snapshot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("key");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.language = Some("en-US".into());
    let (ready, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ready = Some(ready);
        for (language, text) in [("eng", "old"), ("spa", "new")] {
            let mut ws = accept_hdr_async(
                listener.accept().await.unwrap().0,
                |req: &Request, res: Response| {
                    assert!(req
                        .uri()
                        .query()
                        .unwrap()
                        .contains(&format!("language_code={language}")));
                    Ok(res)
                },
            )
            .await
            .unwrap();
            assert_eq!(read(&mut ws).await["commit"], false);
            if let Some(ready) = ready.take() {
                ready.send(()).unwrap();
            }
            assert_eq!(read(&mut ws).await["commit"], true);
            write(
                &mut ws,
                json!({"message_type":"committed_transcript","text":text}),
            )
            .await;
            assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
        }
    });
    let processor = Arc::new(ElevenLabsSttProcessor::new(
        ElevenLabsSttClient::new(config).unwrap(),
    ));
    let worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![1, 2], 16000, 1).unwrap()),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), wait)
        .await
        .unwrap()
        .unwrap();
    send(
        &worker,
        FrameData::SttUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(SttSettings {
                language: Setting::Value("es-MX".into()),
                ..Default::default()
            }),
            ..Default::default()
        })),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while processor.config_snapshot().unwrap().language.as_deref() != Some("es-MX") {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    send(
        &worker,
        FrameData::InputAudio(AudioRawFrame::new(vec![3, 4], 16000, 1).unwrap()),
    )
    .await;
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let transcripts: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::Transcription(t) = &f.frame.data {
                Some(t.text.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(transcripts, ["old", "new"]);
    assert_eq!(
        processor.config_snapshot().unwrap().language.as_deref(),
        Some("es-MX")
    );
    server.await.unwrap();
}
#[tokio::test]
async fn tts_interruption_cancels_owned_context_and_suppresses_stale_audio() {
    let (listener, _, mut worker) = setup().await;
    let (ready, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let context = read(&mut ws).await["context_id"].clone();
        read(&mut ws).await;
        ready.send(()).unwrap();
        assert_eq!(
            read(&mut ws).await,
            json!({"context_id":context,"close_context":true})
        );
        assert_eq!(read(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello. Next"))).await;
    tokio::time::timeout(Duration::from_secs(3), wait)
        .await
        .unwrap()
        .unwrap();
    send(&worker, FrameData::Interruption).await;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!matches!(frame.frame.data, FrameData::TtsAudio(_)));
        if matches!(frame.frame.data, FrameData::Interruption) {
            break;
        }
    }
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_) | FrameData::TtsText(_))));
    server.await.unwrap();
}

#[tokio::test]
async fn flow_fence_preserves_failed_response_end_after_closing_the_active_context() {
    for (standalone, wait_for_fence) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let (listener, _, mut worker) = setup().await;
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            read(&mut ws).await;
            assert_eq!(read(&mut ws).await["text"], "Hello.");
            assert_eq!(read(&mut ws).await["flush"], true);
            assert_eq!(read(&mut ws).await["close_context"], true);
            write(&mut ws, json!({"error":"rejected","code":429})).await;
            assert_eq!(read(&mut ws).await["close_context"], true);
            assert_eq!(read(&mut ws).await["close_socket"], true);
            ws.close(None).await.unwrap();
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let context = read(&mut ws).await["context_id"].clone();
            assert_eq!(read(&mut ws).await["text"], "Clean.");
            assert_eq!(read(&mut ws).await["flush"], true);
            assert_eq!(read(&mut ws).await["close_context"], true);
            respond(&mut ws, &context, "Clean.").await;
        });
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            if standalone {
                FrameData::TtsSpeak(TtsSpeakFrame::new("Hello."))
            } else {
                FrameData::LlmText(TextFrame::llm("Hello."))
            },
        )
        .await;
        let fence = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            ..Default::default()
        }));
        let fence_id = fence.id;
        worker
            .sender()
            .send(fence, Direction::Downstream)
            .await
            .unwrap();
        let mut frames = vec![];
        if wait_for_fence {
            loop {
                let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
                    .await
                    .unwrap()
                    .unwrap();
                let done = frame.frame.id == fence_id;
                frames.push(frame);
                if done {
                    break;
                }
            }
        }
        let mut end = Frame::new(FrameData::LlmResponseEnd);
        end.metadata.insert("source".into(), json!("original"));
        let end_id = end.id;
        worker
            .sender()
            .send(end, Direction::Downstream)
            .await
            .unwrap();
        send(&worker, FrameData::LlmResponseStart).await;
        send(&worker, FrameData::LlmText(TextFrame::llm("Clean."))).await;
        let clean_end = Frame::new(FrameData::LlmResponseEnd);
        let clean_end_id = clean_end.id;
        worker
            .sender()
            .send(clean_end, Direction::Downstream)
            .await
            .unwrap();
        worker.sender().end().await.unwrap();
        let (remaining, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
            .await
            .unwrap()
            .unwrap();
        frames.extend(remaining);
        assert_eq!(report.errors.len(), 2);
        assert!(report.errors[1].error.contains("completed with no audio"));
        let end = frames.iter().find(|f| f.frame.id == end_id).unwrap();
        assert_eq!(end.frame.metadata["source"], "original");
        assert_eq!(end.frame.metadata.get("failed"), Some(&json!(true)));
        assert!(!frames
            .iter()
            .find(|f| f.frame.id == clean_end_id)
            .unwrap()
            .frame
            .metadata
            .contains_key("failed"));
        let fence_index = frames.iter().position(|f| f.frame.id == fence_id).unwrap();
        let end_index = frames.iter().position(|f| f.frame.id == end_id).unwrap();
        let stopped_index = frames
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .unwrap();
        // Core returns this fence upstream, so its externally observed arrival can
        // follow the response end even though downstream dispatch is FIFO.
        assert!(stopped_index < fence_index && stopped_index < end_index);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn interrupted_context_discards_late_audio_and_partial_text_before_a_fresh_turn() {
    let (listener, _, mut worker) = setup().await;
    let (ready, wait_ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut old = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let old_context = read(&mut old).await["context_id"].clone();
        assert_eq!(read(&mut old).await["text"], "Old.");
        ready.send(()).unwrap();
        assert_eq!(read(&mut old).await["close_context"], true);
        // The provider can race cancellation. These bytes and timestamps must
        // be consumed only by cleanup, never forwarded into the next response.
        write(&mut old, json!({"contextId":old_context,"audio":"AQI=","alignment":{"chars":["o","l","d"," "],"charStartTimesMs":[0,1,2,3],"charDurationsMs":[1,1,1,1]}})).await;
        assert_eq!(read(&mut old).await["close_socket"], true);
        old.close(None).await.unwrap();
        let mut fresh = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let new_context = read(&mut fresh).await["context_id"].clone();
        assert_ne!(old_context, new_context);
        assert_eq!(read(&mut fresh).await["text"], "Fresh.");
        assert_eq!(read(&mut fresh).await["flush"], true);
        assert_eq!(read(&mut fresh).await["close_context"], true);
        write(&mut fresh, json!({"contextId":new_context,"audio":"AwQ=","alignment":{"chars":["F","r","e","s","h","."],"charStartTimesMs":[0,1,2,3,4,5],"charDurationsMs":[1,1,1,1,1,1]},"isFinal":true})).await;
        assert_eq!(read(&mut fresh).await["close_socket"], true);
        fresh.close(None).await.unwrap();
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::LlmText(TextFrame::llm("Old. Leaked tail")),
    )
    .await;
    tokio::time::timeout(Duration::from_secs(3), wait_ready)
        .await
        .unwrap()
        .unwrap();
    send(&worker, FrameData::Interruption).await;
    loop {
        let frame = tokio::time::timeout(Duration::from_secs(3), worker.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(!matches!(
            frame.frame.data,
            FrameData::TtsAudio(_) | FrameData::TtsText(_)
        ));
        if matches!(frame.frame.data, FrameData::Interruption) {
            break;
        }
    }
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Fresh."))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(5), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let audio: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::TtsAudio(a) = &f.frame.data {
                Some(a.audio.clone())
            } else {
                None
            }
        })
        .flatten()
        .collect();
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| {
            if let FrameData::TtsText(t) = &f.frame.data {
                Some(t.text.text.as_str())
            } else {
                None
            }
        })
        .collect();
    assert_eq!(audio, [3, 4]);
    assert_eq!(words, ["Fresh."]);
    server.await.unwrap();
}
