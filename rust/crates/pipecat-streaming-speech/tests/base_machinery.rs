//! Source parity: STTService reconnect guards, typed settings, Cartesia context
//! rotation and TTSService aggregation/lifecycle, exercised over real loopback WS.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{accept_async, accept_hdr_async, tungstenite::Message, WebSocketStream};

type Socket = WebSocketStream<TcpStream>;
async fn read(socket: &mut Socket) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn write(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn end(worker: PipelineWorker) -> (Vec<DirectedFrame>, WorkerReport) {
    worker.sender().end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap()
}
fn audio(byte: u8) -> FrameData {
    FrameData::InputAudio(AudioRawFrame::new(vec![byte; 4], 16000, 1).unwrap())
}
async fn tts_setup(
    mode: TextAggregationMode,
) -> (TcpListener, Arc<CartesiaTtsProcessor>, PipelineWorker) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("secret", "old-voice");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    let processor = Arc::new(
        CartesiaTtsProcessor::new(CartesiaClient::new(config).unwrap())
            .with_text_aggregation_mode(mode),
    );
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    (listener, processor, worker)
}
fn tts_update(delta: TtsSettings) -> FrameData {
    FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
        delta: Some(delta),
        ..Default::default()
    }))
}
fn stt_update(delta: SttSettings) -> FrameData {
    FrameData::SttUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
        delta: Some(delta),
        ..Default::default()
    }))
}
async fn done(socket: &mut Socket, request: &Value, word: &str) {
    write(
        socket,
        json!({"type":"chunk","context_id":request["context_id"],"data":"AQACAA=="}),
    )
    .await;
    write(socket, json!({"type":"timestamps","context_id":request["context_id"],"word_timestamps":{"words":[word],"start":[0.0],"end":[0.2]}})).await;
    write(
        socket,
        json!({"type":"done","context_id":request["context_id"]}),
    )
    .await;
    let _ = socket.next().await;
}

#[tokio::test]
async fn settings_rotate_immutable_tts_contexts_and_flush_pending_sentences() {
    let (listener, processor, worker) = tts_setup(TextAggregationMode::Sentence).await;
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut first).await;
        assert_eq!(request["transcript"], "Hello.");
        assert_eq!(request["voice"]["id"], "old-voice");
        assert_eq!(request["language"], "en");
        let finish = read(&mut first).await;
        assert_eq!(finish["continue"], false);
        assert_eq!(finish["voice"], request["voice"]);
        assert_eq!(finish["language"], request["language"]);
        done(&mut first, &request, "Hello.").await;
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let changed = read(&mut second).await;
        assert_ne!(changed["context_id"], request["context_id"]);
        assert_eq!(changed["transcript"], "你好");
        assert_eq!(changed["voice"]["id"], "new-voice");
        assert_eq!(changed["language"], "zh");
        assert_eq!(changed["generation_config"], json!({"speed":0.9}));
        assert_eq!(read(&mut second).await["continue"], false);
        done(&mut second, &changed, "你好").await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Hello."))).await;
    send(
        &worker,
        tts_update(TtsSettings {
            voice: Setting::Value("new-voice".into()),
            language: Setting::Value("zh".into()),
            base: ServiceSettings {
                extra: [("generation_config".into(), json!({"speed":0.9}))].into(),
                ..Default::default()
            },
        }),
    )
    .await;
    send(&worker, FrameData::LlmText(TextFrame::llm("你好"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let (frames, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(processor.config_snapshot().unwrap().voice_id, "new-voice");
    let kinds: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::LlmResponseStart => Some("start"),
            FrameData::TtsStarted(_) => Some("tts-start"),
            FrameData::TtsStopped(_) => Some("tts-stop"),
            FrameData::LlmResponseEnd => Some("end"),
            _ => None,
        })
        .collect();
    assert_eq!(
        kinds,
        [
            "start",
            "tts-start",
            "tts-stop",
            "tts-start",
            "tts-stop",
            "end"
        ]
    );
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
    assert!(!words[0].text.includes_inter_frame_spaces);
    assert!(words[1].text.includes_inter_frame_spaces);
    server.await.unwrap();
}

#[tokio::test]
async fn settings_after_last_text_keep_empty_successor_response_barrier_and_failure() {
    let (listener, _, worker) = tts_setup(TextAggregationMode::Token).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(read(&mut socket).await["continue"], false);
        write(
            &mut socket,
            json!({"type":"error","context_id":request["context_id"],"status_code":429}),
        )
        .await;
        assert_eq!(read(&mut socket).await["cancel"], true);
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("hello"))).await;
    send(
        &worker,
        tts_update(TtsSettings {
            voice: Setting::Value("second-voice".into()),
            ..Default::default()
        }),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let (frames, report) = end(worker).await;
    assert_eq!(report.errors.len(), 1);
    let stopped = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .unwrap();
    let ended = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .unwrap();
    assert!(stopped < ended);
    assert_eq!(frames[ended].frame.metadata["failed"], true);
    server.await.unwrap();
}

#[tokio::test]
async fn settings_validate_atomically_and_respect_service_target() {
    let (listener, processor, worker) = tts_setup(TextAggregationMode::Token).await;
    let health = worker.processor_health(0).unwrap();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request["voice"]["id"], "old-voice");
        assert_eq!(request["model_id"], "sonic-3.6");
        assert_eq!(read(&mut socket).await["continue"], false);
        done(&mut socket, &request, "ok").await;
    });
    send(
        &worker,
        tts_update(TtsSettings {
            base: ServiceSettings {
                model: Setting::Value("invalid-change".into()),
                ..Default::default()
            },
            voice: Setting::Null,
            ..Default::default()
        }),
    )
    .await;
    send(
        &worker,
        FrameData::TtsUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value("other-voice".into()),
                ..Default::default()
            }),
            service: Some(ProcessorReference::Name("OtherService".into())),
            ..Default::default()
        })),
    )
    .await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("ok"))).await;
    let (frames, report) = end(worker).await;
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.errors[0].category, ErrorCategory::InvalidRequest);
    assert!(health.is_usable());
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsUpdateSettings(_))));
    assert_eq!(processor.config_snapshot().unwrap().model, "sonic-3.6");
    server.await.unwrap();
}

#[tokio::test]
async fn sentence_aggregation_preserves_spell_tags_and_flushes_on_end() {
    let (listener, _, worker) = tts_setup(TextAggregationMode::Sentence).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let mut texts = vec![];
        let mut first = None;
        loop {
            let request = read(&mut socket).await;
            if request["continue"] == false {
                break;
            }
            first.get_or_insert_with(|| request.clone());
            texts.push(request["transcript"].as_str().unwrap().to_owned());
        }
        assert_eq!(texts, ["Call <spell>A.B.</spell> now.", "Next sentence."]);
        done(&mut socket, &first.unwrap(), "Call").await;
    });
    send(&worker, FrameData::LlmResponseStart).await;
    for text in [
        "Call <sp",
        "ell>A.",
        "B.</spell> now.",
        " N",
        "ext sentence.",
    ] {
        send(&worker, FrameData::LlmText(TextFrame::llm(text))).await;
    }
    let (_, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

#[allow(clippy::result_large_err)]
async fn accept_language(listener: &TcpListener, language: &'static str) -> Socket {
    accept_hdr_async(
        listener.accept().await.unwrap().0,
        move |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
            let uri = url::Url::parse(&format!("ws://localhost{}", request.uri())).unwrap();
            assert!(uri
                .query_pairs()
                .any(|(key, value)| key == "language" && value == language));
            Ok(response)
        },
    )
    .await
    .unwrap()
}
async fn stt_setup(
    address: std::net::SocketAddr,
    policy: Option<ReconnectPolicy>,
) -> (Arc<DeepgramSttProcessor>, PipelineWorker) {
    let mut config = DeepgramConfig::new("secret");
    config.endpoint = format!("ws://{address}/listen");
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.connect_timeout = Duration::from_secs(1);
    let processor = DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap())
        .with_audio_passthrough(false)
        .with_reconnect_policy(policy.unwrap_or_default());
    let processor = Arc::new(processor);
    let mut worker = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    (processor, worker)
}
async fn pcm(socket: &mut Socket, expected: u8) {
    assert!(
        matches!(socket.next().await, Some(Ok(Message::Binary(bytes))) if bytes.as_ref() == [expected;4])
    );
}
#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite fixes the handshake callback error type.
async fn stt_settings_defer_until_full_user_stop_and_buffer_during_reconnect() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_language(&listener, "en").await;
        pcm(&mut first, 1).await;
        pcm(&mut first, 2).await;
        assert_eq!(read(&mut first).await, json!({"type":"Finalize"}));
        write(&mut first, json!({"type":"Results","is_final":true,"from_finalize":true,"channel":{"alternatives":[{"transcript":"old final"}]}})).await;
        assert_eq!(read(&mut first).await, json!({"type":"CloseStream"}));
        first.close(None).await.unwrap();
        let incoming = listener.accept().await.unwrap().0;
        handshake_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let mut second = accept_hdr_async(
            incoming,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert!(request.uri().query().unwrap().contains("language=es"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        pcm(&mut second, 3).await;
        pcm(&mut second, 4).await;
        assert_eq!(read(&mut second).await, json!({"type":"CloseStream"}));
        second.close(None).await.unwrap();
    });
    let (processor, mut worker) = stt_setup(address, None).await;
    send(
        &worker,
        FrameData::VadUserStartedSpeakingDetailed(VadUserStartedSpeakingFrame {
            start_secs: 0.1,
            timestamp: 1.0,
        }),
    )
    .await;
    send(&worker, audio(1)).await;
    send(
        &worker,
        stt_update(SttSettings {
            language: Setting::Value("es".into()),
            ..Default::default()
        }),
    )
    .await;
    send(&worker, audio(2)).await;
    send(
        &worker,
        FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
            stop_secs: 0.1,
            timestamp: 2.0,
        }),
    )
    .await;
    while !matches!(
        next(&mut worker).await.frame.data,
        FrameData::Transcription(_)
    ) {}
    send(&worker, FrameData::UserStoppedSpeaking).await;
    tokio::time::timeout(Duration::from_secs(3), handshake_rx)
        .await
        .unwrap()
        .unwrap();
    send(&worker, audio(3)).await;
    send(&worker, audio(4)).await;
    release_tx.send(()).unwrap();
    let (_, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        processor.config_snapshot().unwrap().language.as_deref(),
        Some("es")
    );
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn stt_recovery_keeps_unsent_audio_without_replaying_old_connection_audio() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (handshake_tx, handshake_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_language(&listener, "en").await;
        pcm(&mut first, 1).await;
        first.close(None).await.unwrap();
        let incoming = listener.accept().await.unwrap().0;
        handshake_tx.send(()).unwrap();
        release_rx.await.unwrap();
        let mut second = accept_async(incoming).await.unwrap();
        pcm(&mut second, 2).await;
        assert_eq!(read(&mut second).await, json!({"type":"CloseStream"}));
        write(&mut second, json!({"type":"Results","is_final":true,"channel":{"alternatives":[{"transcript":"new audio only"}]}})).await;
        second.close(None).await.unwrap();
    });
    let (_, mut worker) = stt_setup(
        address,
        Some(ReconnectPolicy {
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        }),
    )
    .await;
    send(&worker, audio(1)).await;
    while !matches!(next(&mut worker).await.frame.data, FrameData::Error(_)) {}
    tokio::time::timeout(Duration::from_secs(2), handshake_rx)
        .await
        .unwrap()
        .unwrap();
    send(&worker, audio(2)).await;
    release_tx.send(()).unwrap();
    let (frames, report) = end(worker).await;
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
    assert!(frames.iter().any(
        |f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text == "new audio only")
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn rapid_stt_disconnects_exhaust_budget_and_report_buffered_audio() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (retry_tx, retry_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        pcm(&mut first, 1).await;
        first.close(None).await.unwrap();
        let incoming = listener.accept().await.unwrap().0;
        retry_tx.send(()).unwrap();
        release_rx.await.unwrap();
        // A permanent 401 must terminate recovery instead of looping.
        use tokio::io::AsyncWriteExt;
        let mut incoming = incoming;
        incoming
            .write_all(b"HTTP/1.1 401 Unauthorized\r\nContent-Length: 0\r\n\r\n")
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
    });
    let (_, mut worker) = stt_setup(
        address,
        Some(ReconnectPolicy {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        }),
    )
    .await;
    send(&worker, audio(1)).await;
    tokio::time::timeout(Duration::from_secs(2), retry_rx)
        .await
        .unwrap()
        .unwrap();
    send(&worker, audio(2)).await;
    // Synchronize admission into the supervisor before allowing failure.
    send(&worker, FrameData::Text(TextFrame::new("queued"))).await;
    while !matches!(&next(&mut worker).await.frame.data, FrameData::Text(t) if t.text == "queued") {
    }
    release_tx.send(()).unwrap();
    let mut found = false;
    while !found {
        if let FrameData::Error(error) = next(&mut worker).await.frame.data {
            found = error
                .error
                .contains("4 buffered PCM bytes were not submitted");
        }
    }
    worker.sender().cancel("finished test").await.unwrap();
    let _ = worker.collect().await.unwrap();
    server.await.unwrap();
}

#[test]
fn typed_stt_settings_reject_invalid_delta_without_partial_mutation() {
    let processor =
        DeepgramSttProcessor::new(DeepgramClient::new(DeepgramConfig::new("secret")).unwrap());
    let original = processor.config_snapshot().unwrap();
    let bad = SttSettings {
        language: Setting::Value("es".into()),
        base: ServiceSettings {
            extra: [("sample_rate".into(), json!(8000))].into(),
            ..Default::default()
        },
    };
    assert!(processor.update_settings(&bad).is_err());
    assert_eq!(processor.config_snapshot().unwrap(), original);
    processor
        .update_settings(&SttSettings {
            language: Setting::Null,
            base: ServiceSettings {
                extra: [
                    ("keyterm".into(), json!(["one", "two"])),
                    ("endpointing".into(), json!(false)),
                ]
                .into(),
                ..Default::default()
            },
        })
        .unwrap();
    let updated = processor.config_snapshot().unwrap();
    assert_eq!(updated.language, None);
    assert_eq!(updated.endpointing, Some(Endpointing::Disabled));
    assert_eq!(
        updated.extra_query,
        [
            ("keyterm".into(), "one".into()),
            ("keyterm".into(), "two".into())
        ]
    );
}

#[tokio::test]
async fn stt_rapid_successful_handshakes_do_not_reset_reconnect_budget() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        for index in 0..3 {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            if index == 0 {
                pcm(&mut socket, 1).await;
            }
            socket.close(None).await.unwrap();
        }
        assert!(
            tokio::time::timeout(Duration::from_millis(150), listener.accept())
                .await
                .is_err()
        );
    });
    let (_, mut worker) = stt_setup(
        address,
        Some(ReconnectPolicy {
            max_attempts: 2,
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        }),
    )
    .await;
    let health = worker.processor_health(0).unwrap();
    send(&worker, audio(1)).await;
    let mut errors = 0;
    while errors != 3 {
        if matches!(next(&mut worker).await.frame.data, FrameData::Error(_)) {
            errors += 1;
        }
    }
    loop {
        if let FrameData::Error(error) = next(&mut worker).await.frame.data {
            if error.error.contains("recovery exhausted") {
                assert_eq!(error.category, ErrorCategory::Connectivity);
                break;
            }
        }
    }
    assert!(!health.is_usable());
    // New audio must not silently start another complete reconnect budget.
    send(&worker, audio(2)).await;
    send(&worker, FrameData::Text(TextFrame::new("gated"))).await;
    while !matches!(next(&mut worker).await.frame.data, FrameData::Text(_)) {}
    tokio::time::timeout(Duration::from_secs(2), server)
        .await
        .unwrap()
        .unwrap();
    worker.sender().cancel("finished test").await.unwrap();
    let _ = worker.collect().await.unwrap();
}

#[tokio::test]
async fn cancelling_stt_during_reconnect_aborts_pending_handshake() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (pending_tx, pending_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        pcm(&mut first, 1).await;
        first.close(None).await.unwrap();
        let (mut pending, _) = listener.accept().await.unwrap();
        pending_tx.send(()).unwrap();
        use tokio::io::AsyncReadExt;
        let mut bytes = Vec::new();
        tokio::time::timeout(Duration::from_secs(2), pending.read_to_end(&mut bytes))
            .await
            .unwrap()
            .unwrap();
    });
    let (_, worker) = stt_setup(
        address,
        Some(ReconnectPolicy {
            initial_delay: Duration::from_millis(1),
            ..Default::default()
        }),
    )
    .await;
    send(&worker, audio(1)).await;
    tokio::time::timeout(Duration::from_secs(2), pending_rx)
        .await
        .unwrap()
        .unwrap();
    worker.sender().cancel("stop reconnect").await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    server.await.unwrap();
}

#[tokio::test]
async fn sentence_interruption_discards_unsubmitted_text_and_context() {
    let (listener, _, mut worker) = tts_setup(TextAggregationMode::Sentence).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request["transcript"], "fresh");
        assert_eq!(read(&mut socket).await["continue"], false);
        done(&mut socket, &request, "fresh").await;
    });
    send(
        &worker,
        FrameData::LlmText(TextFrame::llm("stale unfinished sentence")),
    )
    .await;
    send(&worker, FrameData::Interruption).await;
    while !matches!(next(&mut worker).await.frame.data, FrameData::Interruption) {}
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))).await;
    let (_, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

#[tokio::test]
async fn tts_finish_bypasses_saturated_single_slot_text_queue() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("secret", "voice");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.limits.queue_capacity = 1;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request["transcript"], "hello");
        assert_eq!(read(&mut socket).await["continue"], false);
        done(&mut socket, &request, "hello").await;
    });
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("hello"))).await;
    let (frames, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .count(),
        1
    );
    server.await.unwrap();
}

#[tokio::test]
async fn tts_context_admission_is_bounded_and_accepted_contexts_remain_fifo() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = CartesiaConfig::new("secret", "voice");
    config.endpoint = format!("ws://{}/tts", listener.local_addr().unwrap());
    config.limits.queue_capacity = 2;
    let (started_tx, started_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut first).await;
        assert_eq!(request["transcript"], "first");
        assert_eq!(read(&mut first).await["continue"], false);
        started_tx.send(()).unwrap();
        release_rx.await.unwrap();
        done(&mut first, &request, "first").await;
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut second).await;
        assert_eq!(request["transcript"], "second");
        assert_eq!(read(&mut second).await["continue"], false);
        done(&mut second, &request, "second").await;
        assert!(
            tokio::time::timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let mut worker = Pipeline::new(vec![Arc::new(CartesiaTtsProcessor::new(
        CartesiaClient::new(config).unwrap(),
    ))])
    .start(PipelineConfig::default())
    .await
    .unwrap();
    next(&mut worker).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("first"))).await;
    started_rx.await.unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("second"))).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("rejected"))).await;
    while !matches!(next(&mut worker).await.frame.data, FrameData::Error(_)) {}
    release_tx.send(()).unwrap();
    let (frames, report) = end(worker).await;
    assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
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
    assert_eq!(words, ["first", "second"]);
    server.await.unwrap();
}

fn fence_frame(id: u64) -> Frame {
    let mut frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("pipecat.flows".into()),
        returning: false,
        event: None,
    }));
    frame
        .metadata
        .insert("pipecat.flows.barrier".into(), id.into());
    frame
}

#[tokio::test]
async fn flows_fence_finishes_partial_tts_and_forwards_only_after_synthesis() {
    let (listener, _, worker) = tts_setup(TextAggregationMode::Sentence).await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        assert_eq!(request["transcript"], "unfinished sentence");
        assert_eq!(read(&mut socket).await["continue"], false);
        done(&mut socket, &request, "unfinished sentence").await;
    });
    send(
        &worker,
        FrameData::LlmText(TextFrame::llm("unfinished sentence")),
    )
    .await;
    let fence = fence_frame(23);
    worker
        .sender()
        .send(fence.clone(), Direction::Downstream)
        .await
        .unwrap();
    let (frames, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    let stopped = frames
        .iter()
        .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .unwrap();
    let markers: Vec<_> = frames
        .iter()
        .enumerate()
        .filter(|(_, f)| f.frame.id == fence.id)
        .collect();
    assert_eq!(markers.len(), 1);
    assert!(stopped < markers[0].0);
    // Core may turn the marker around, but service preserves all routing data.
    assert_eq!(markers[0].1.frame.metadata, fence.metadata);
    assert!(!markers[0].1.frame.interruptible);
    server.await.unwrap();
}

#[tokio::test]
async fn flows_fence_does_not_block_interruption_and_releases_after_context_cancel() {
    let (listener, _, mut worker) = tts_setup(TextAggregationMode::Token).await;
    let (request_tx, request_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut socket).await;
        request_tx.send(()).unwrap();
        loop {
            let message = read(&mut socket).await;
            if message["cancel"] == true {
                assert_eq!(message["context_id"], request["context_id"]);
                break;
            }
        }
    });
    send(&worker, FrameData::LlmText(TextFrame::llm("stale"))).await;
    request_rx.await.unwrap();
    let fence = fence_frame(24);
    worker
        .sender()
        .send(fence.clone(), Direction::Downstream)
        .await
        .unwrap();
    // Ensure the fence was admitted before interrupting; this passthrough
    // marker also proves process is not awaiting provider completion.
    let mut marker = TextFrame::new("after fence");
    marker.skip_tts = Some(true);
    send(&worker, FrameData::Text(marker)).await;
    while !matches!(&next(&mut worker).await.frame.data, FrameData::Text(t) if t.text == "after fence")
    {
    }
    send(&worker, FrameData::Interruption).await;
    let mut saw_interruption = false;
    let mut saw_fence = false;
    while !saw_interruption || !saw_fence {
        let frame = next(&mut worker).await.frame;
        saw_interruption |= matches!(frame.data, FrameData::Interruption);
        if frame.id == fence.id {
            assert!(!saw_fence);
            saw_fence = true;
        }
        assert!(!matches!(frame.data, FrameData::TtsAudio(_)));
    }
    let (_, report) = end(worker).await;
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    server.await.unwrap();
}

#[tokio::test]
async fn closed_context_failure_follows_original_response_end_across_flow_fences() {
    for (standalone, wait_for_fence) in [(false, false), (false, true), (true, false), (true, true)]
    {
        let (listener, _, mut worker) = tts_setup(TextAggregationMode::Sentence).await;
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = read(&mut socket).await;
            assert_eq!(request["transcript"], "Rejected.");
            assert_eq!(read(&mut socket).await["continue"], false);
            write(&mut socket, json!({"type":"error","context_id":request["context_id"],"status_code":429,"error":"rejected"})).await;
            assert_eq!(read(&mut socket).await["cancel"], true);
            assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            let request = read(&mut socket).await;
            assert_eq!(request["transcript"], "Clean.");
            assert_eq!(read(&mut socket).await["continue"], false);
            done(&mut socket, &request, "Clean.").await;
        });
        let start = Frame::new(FrameData::LlmResponseStart);
        let start_id = start.id;
        worker
            .sender()
            .send(start, Direction::Downstream)
            .await
            .unwrap();
        send(
            &worker,
            if standalone {
                FrameData::TtsSpeak(TtsSpeakFrame::new("Rejected."))
            } else {
                FrameData::LlmText(TextFrame::llm("Rejected."))
            },
        )
        .await;
        let fence = fence_frame(111);
        let fence_id = fence.id;
        worker
            .sender()
            .send(fence, Direction::Downstream)
            .await
            .unwrap();
        let mut frames = vec![];
        if wait_for_fence {
            loop {
                let frame = next(&mut worker).await;
                let done = frame.frame.id == fence_id;
                frames.push(frame);
                if done {
                    break;
                }
            }
        }
        let mut boundary = Frame::new(FrameData::LlmResponseEnd);
        boundary.metadata.insert("source".into(), json!("original"));
        let boundary_id = boundary.id;
        worker
            .sender()
            .send(boundary, Direction::Downstream)
            .await
            .unwrap();
        send(&worker, FrameData::LlmResponseStart).await;
        send(&worker, FrameData::LlmText(TextFrame::llm("Clean."))).await;
        let clean = Frame::new(FrameData::LlmResponseEnd);
        let clean_id = clean.id;
        worker
            .sender()
            .send(clean, Direction::Downstream)
            .await
            .unwrap();
        let (remaining, report) = end(worker).await;
        frames.extend(remaining);
        assert_eq!(report.errors.len(), 2);
        assert!(report.errors[1].error.contains("completed with no audio"));
        assert_eq!(frames.iter().filter(|f| f.frame.id == start_id).count(), 1);
        let boundary = frames.iter().find(|f| f.frame.id == boundary_id).unwrap();
        assert_eq!(
            boundary.frame.metadata.get("failed"),
            Some(&json!(true)),
            "standalone={standalone}, wait_for_fence={wait_for_fence}"
        );
        assert_eq!(boundary.frame.metadata["source"], "original");
        assert!(!frames
            .iter()
            .find(|f| f.frame.id == clean_id)
            .unwrap()
            .frame
            .metadata
            .contains_key("failed"));
        let stop = frames
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .unwrap();
        let boundary = frames
            .iter()
            .position(|f| f.frame.id == boundary_id)
            .unwrap();
        assert!(stop < boundary);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn permanent_tts_failure_gates_work_and_effective_settings_recover_fresh() {
    let (listener, processor, mut worker) = tts_setup(TextAggregationMode::Sentence).await;
    let server = tokio::spawn(async move {
        let mut first = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let request = read(&mut first).await;
        assert_eq!(request["transcript"], "Rejected.");
        assert_eq!(read(&mut first).await["continue"], false);
        write(
            &mut first,
            json!({"type":"error","context_id":request["context_id"],"status_code":401}),
        )
        .await;
        let _ = first.next().await;
        let mut second = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let fresh = read(&mut second).await;
        assert_ne!(fresh["context_id"], request["context_id"]);
        assert_eq!(fresh["transcript"], "Fresh.");
        assert_eq!(fresh["voice"]["id"], "recovered-voice");
        assert_eq!(read(&mut second).await["continue"], false);
        done(&mut second, &fresh, "Fresh.").await;
    });
    let health = worker.processor_health(0).unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Rejected."))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let mut seen = vec![];
    loop {
        let frame = next(&mut worker).await;
        let ended = matches!(frame.frame.data, FrameData::LlmResponseEnd);
        if ended {
            assert_eq!(frame.frame.metadata["failed"], true);
        }
        seen.push(frame);
        if ended {
            break;
        }
    }
    assert!(!health.is_usable());
    assert!(seen.iter().any(|f| matches!(&f.frame.data, FrameData::Error(error) if error.category == ErrorCategory::Authentication)));
    assert!(seen
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
        .all(|f| f.frame.metadata.get("failed") == Some(&json!(true))));
    send(&worker, tts_update(TtsSettings::default())).await;
    send(
        &worker,
        tts_update(TtsSettings {
            voice: Setting::Null,
            ..Default::default()
        }),
    )
    .await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::LlmText(TextFrame::llm("Discarded partial")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    loop {
        let frame = next(&mut worker).await;
        assert!(!matches!(
            frame.frame.data,
            FrameData::TtsAudio(_) | FrameData::TtsText(_) | FrameData::TtsStarted(_)
        ));
        if matches!(frame.frame.data, FrameData::LlmResponseEnd) {
            assert_eq!(frame.frame.metadata["failed"], true);
            break;
        }
    }
    assert!(!health.is_usable());
    assert_eq!(processor.config_snapshot().unwrap().voice_id, "old-voice");
    send(
        &worker,
        tts_update(TtsSettings {
            voice: Setting::Value("recovered-voice".into()),
            ..Default::default()
        }),
    )
    .await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::LlmText(TextFrame::llm("Fresh."))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let (frames, report) = end(worker).await;
    assert!(health.is_usable());
    assert_eq!(report.errors.len(), 2);
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_))));
    assert!(frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
        .all(|f| !f.frame.metadata.contains_key("failed")));
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn permanent_stt_failure_retires_socket_and_settings_open_fresh_audio_generation() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let mut first = accept_language(&listener, "en").await;
        pcm(&mut first, 1).await;
        write(&mut first, json!({"type":"Error", "status_code":401})).await;
        let _ = first.next().await;
        let mut second = accept_language(&listener, "es").await;
        pcm(&mut second, 3).await;
        write(&mut second, json!({"type":"Results","is_final":true,"speech_final":true,"channel":{"alternatives":[{"transcript":"fresh audio"}]}})).await;
        assert_eq!(read(&mut second).await, json!({"type":"CloseStream"}));
        second.close(None).await.unwrap();
    });
    let (processor, mut worker) = stt_setup(address, None).await;
    let health = worker.processor_health(0).unwrap();
    send(&worker, audio(1)).await;
    loop {
        if let FrameData::Error(error) = next(&mut worker).await.frame.data {
            assert_eq!(error.category, ErrorCategory::Authentication);
            break;
        }
    }
    assert!(!health.is_usable());
    processor.update_settings(&SttSettings::default()).unwrap();
    assert!(!health.is_usable());
    assert!(processor
        .update_settings(&SttSettings {
            language: Setting::Value(String::new()),
            ..Default::default()
        })
        .is_err());
    assert!(!health.is_usable());
    send(&worker, audio(2)).await;
    // A normal passthrough frame confirms the disabled audio was processed.
    send(&worker, FrameData::Text(TextFrame::new("barrier"))).await;
    while !matches!(next(&mut worker).await.frame.data, FrameData::Text(_)) {}
    processor
        .update_settings(&SttSettings {
            language: Setting::Value("es".into()),
            ..Default::default()
        })
        .unwrap();
    assert!(health.is_usable());
    send(&worker, audio(3)).await;
    let (frames, report) = end(worker).await;
    assert_eq!(report.errors.len(), 1);
    assert!(frames
        .iter()
        .any(|f| matches!(&f.frame.data, FrameData::Transcription(t) if t.text == "fresh audio")));
    tokio::time::timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[test]
fn equivalent_query_setting_order_does_not_restore_unusable_stt() {
    let mut config = DeepgramConfig::new("secret");
    config.extra_query = vec![
        ("tag".into(), "same".into()),
        ("keyterm".into(), "word".into()),
    ];
    let processor = DeepgramSttProcessor::new(DeepgramClient::new(config).unwrap());
    let health = processor.health().unwrap();
    health.set_usable(false);
    let before = processor.config_snapshot().unwrap();
    processor
        .update_settings(&SttSettings {
            base: ServiceSettings {
                extra: [("tag".into(), json!("same"))].into(),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
    assert!(!health.is_usable());
    assert_eq!(health.snapshot().revision, 1);
    assert_eq!(processor.config_snapshot().unwrap(), before);
    processor
        .update_settings(&SttSettings {
            base: ServiceSettings {
                extra: [("tag".into(), json!("changed"))].into(),
                ..Default::default()
            },
            ..Default::default()
        })
        .unwrap();
    assert!(health.is_usable());
}
