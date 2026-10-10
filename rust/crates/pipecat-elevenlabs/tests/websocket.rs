use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_elevenlabs::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};
async fn recv(ws: &mut WebSocketStream<TcpStream>) -> Value {
    serde_json::from_str(ws.next().await.unwrap().unwrap().to_text().unwrap()).unwrap()
}
async fn send(ws: &mut WebSocketStream<TcpStream>, v: Value) {
    ws.send(Message::Text(v.to_string().into())).await.unwrap();
}
fn pcm() -> AudioRawFrame {
    AudioRawFrame {
        audio: vec![1, 2, 3, 4],
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    }
}
fn limits() -> StreamingLimits {
    StreamingLimits {
        response_timeout: Duration::from_secs(2),
        close_timeout: Duration::from_millis(100),
        ..Default::default()
    }
}
#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite callback has a fixed HTTP error type.
async fn stt_authenticated_query_audio_manual_commits_and_drain() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("test-secret");
    config.endpoint = format!(
        "ws://{}/v1/speech-to-text/realtime",
        listener.local_addr().unwrap()
    );
    config.language = Some("en-US".into());
    config.keyterms = vec!["Pipecat".into(), "Scribe V2&?".into()];
    config.filter_background_audio = Some(false);
    config.include_timestamps = true;
    config.limits = limits();
    let server = tokio::spawn(async move {
        let mut ws = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |req: &Request, res: Response| {
                assert_eq!(req.headers()["xi-api-key"], "test-secret");
                assert!(!req.uri().to_string().contains("test-secret"));
                let url = url::Url::parse(&format!("ws://localhost{}", req.uri())).unwrap();
                let q: Vec<_> = url.query_pairs().collect();
                assert!(q.contains(&("language_code".into(), "eng".into())));
                assert!(q.contains(&("keyterms".into(), "Scribe V2&?".into())));
                assert!(q.contains(&("filter_background_audio".into(), "false".into())));
                Ok(res)
            },
        )
        .await
        .unwrap();
        send(
            &mut ws,
            json!({"message_type":"session_started","session_id":"s"}),
        )
        .await;
        assert_eq!(
            recv(&mut ws).await,
            json!({"message_type":"input_audio_chunk","audio_base_64":"AQIDBA==","commit":false,"sample_rate":16000})
        );
        send(
            &mut ws,
            json!({"message_type":"partial_transcript","text":" Hel "}),
        )
        .await;
        assert_eq!(
            recv(&mut ws).await,
            json!({"message_type":"input_audio_chunk","audio_base_64":"","commit":true,"sample_rate":16000})
        );
        send(
            &mut ws,
            json!({"message_type":"committed_transcript","text":"Hello."}),
        )
        .await;
        send(&mut ws,json!({"message_type":"committed_transcript_with_timestamps","text":"Hello.","language_code":"en","words":[]})).await;
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
        let _ = ws.close(None).await;
    });
    let mut session = ElevenLabsSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.send_audio(pcm()).await.unwrap();
    sender.finish().await.unwrap();
    assert_eq!(
        sender.send_audio(pcm()).await.unwrap_err(),
        StreamingError::Closed
    );
    let mut transcripts = vec![];
    while let Some(event) = session.next().await {
        if let ElevenLabsSttEvent::Transcript(t) = event.unwrap() {
            transcripts.push(t);
        }
    }
    assert_eq!(transcripts.len(), 2);
    assert!(!transcripts[0].is_final);
    assert_eq!(transcripts[0].text, "Hel");
    assert!(transcripts[1].finalized);
    server.await.unwrap();
}
#[tokio::test]
#[allow(clippy::result_large_err)] // Tungstenite callback has a fixed HTTP error type.
async fn tts_init_text_flush_close_alignment_pcm_and_owned_socket_cleanup() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("test-secret", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    config.voice_settings.stability = Some(0.4);
    let server = tokio::spawn(async move {
        let mut ws = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |req: &Request, res: Response| {
                assert_eq!(req.headers()["xi-api-key"], "test-secret");
                assert!(req.uri().path().ends_with("/voice/multi-stream-input"));
                assert!(req
                    .uri()
                    .query()
                    .unwrap()
                    .contains("output_format=pcm_24000"));
                Ok(res)
            },
        )
        .await
        .unwrap();
        assert_eq!(
            recv(&mut ws).await,
            json!({"text":" ","context_id":"ctx","voice_settings":{"stability":0.4}})
        );
        assert_eq!(
            recv(&mut ws).await,
            json!({"text":"Hello world","context_id":"ctx"})
        );
        assert_eq!(
            recv(&mut ws).await,
            json!({"context_id":"ctx","flush":true})
        );
        assert_eq!(
            recv(&mut ws).await,
            json!({"context_id":"ctx","close_context":true})
        );
        send(&mut ws,json!({"contextId":"ctx","audio":"AQ==","alignment":{"chars":[" ","H","i"],"charStartTimesMs":[0,1,2],"charDurationsMs":[1,1,1]}})).await;
        send(&mut ws,json!({"contextId":"ctx","audio":"AgME","alignment":{"chars":[" ","y","o","u"],"charStartTimesMs":[0,1,2,3],"charDurationsMs":[1,1,1,1]}})).await;
        send(&mut ws, json!({"contextId":"ctx","isFinal":true})).await;
        assert_eq!(recv(&mut ws).await, json!({"close_socket":true}));
        ws.close(None).await.unwrap();
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    stream.sender().send_text("Hello world").await.unwrap();
    stream.sender().finish().await.unwrap();
    let mut audio = vec![];
    let mut words = vec![];
    let mut done = 0;
    while let Some(event) = stream.next().await {
        match event.unwrap() {
            ElevenLabsTtsEvent::Audio(a) => audio.extend(a.audio),
            ElevenLabsTtsEvent::WordTimestamps { words: w, .. } => words.extend(w),
            ElevenLabsTtsEvent::Done { .. } => done += 1,
        }
    }
    assert_eq!(audio, [1, 2, 3, 4]);
    assert_eq!(
        words.iter().map(|w| w.word.as_str()).collect::<Vec<_>>(),
        ["Hi", "you"]
    );
    assert_eq!(done, 1);
    server.await.unwrap();
}
#[tokio::test]
async fn cancellation_bypasses_full_output_and_closes_context_without_replay() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    config.limits.queue_capacity = 1;
    let (ready, wait) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        recv(&mut ws).await;
        for _ in 0..3 {
            send(&mut ws, json!({"contextId":"ctx","audio":"AQI="})).await;
        }
        ready.send(()).unwrap();
        assert_eq!(
            recv(&mut ws).await,
            json!({"context_id":"ctx","close_context":true})
        );
        assert_eq!(recv(&mut ws).await, json!({"close_socket":true}));
        ws.close(None).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    stream.sender().send_text("hello").await.unwrap();
    wait.await.unwrap();
    stream.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        StreamingError::Cancelled
    );
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn provider_error_is_terminal_and_never_replayed() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("secret", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        recv(&mut ws).await;
        send(&mut ws, json!({"error":"secret rejected","code":429})).await;
        assert_eq!(recv(&mut ws).await["close_context"], true);
        assert_eq!(recv(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), listener.accept())
                .await
                .is_err()
        );
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    stream.sender().send_text("hello").await.unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::Provider { status: Some(429) }
    );
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
#[test]
fn configuration_security_limits_and_unsupported_features_are_explicit() {
    for endpoint in [
        "ws://example.com",
        "wss://user:pass@example.com",
        "wss://example.com?token=key",
        "wss://example.com#fragment",
    ] {
        let mut c = ElevenLabsTtsConfig::new("secret", "voice");
        c.endpoint = endpoint.into();
        assert!(ElevenLabsTtsClient::new(c).is_err());
    }
    let mut c = ElevenLabsTtsConfig::new("secret", "voice");
    assert!(!format!("{c:?}").contains("secret"));
    c.voice_settings.speed = Some(f64::NAN);
    assert!(ElevenLabsTtsClient::new(c).is_err());
    let mut c = ElevenLabsSttConfig::new("key\r\ninject:yes");
    assert!(ElevenLabsSttClient::new(c.clone()).is_err());
    c.api_key = "key".into();
    c.sample_rate = 12345;
    assert!(ElevenLabsSttClient::new(c).is_err());
}

#[tokio::test]
async fn input_backpressure_is_explicit_and_cancel_has_priority() {
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = "ws://127.0.0.1:9".into();
    config.limits.queue_capacity = 1;
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    let sender = stream.sender();
    sender.try_send_text("first").unwrap();
    assert_eq!(
        sender.try_send_text("rejected").unwrap_err(),
        StreamingError::Backpressure
    );
    sender.cancel();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::Cancelled
    );
}
#[tokio::test]
async fn audio_context_limit_rejects_provider_bytes() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.max_audio_bytes = 2;
    config.limits = limits();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        send(&mut ws, json!({"contextId":"ctx","audio":"AQIDBA=="})).await;
        assert_eq!(recv(&mut ws).await["close_context"], true);
        assert_eq!(recv(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::TooLarge
    );
    server.await.unwrap();
}
#[tokio::test]
async fn missing_final_times_out_and_closes_without_done() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    config.limits.response_timeout = Duration::from_millis(30);
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        assert_eq!(recv(&mut ws).await["flush"], true);
        assert_eq!(recv(&mut ws).await["close_context"], true);
        assert_eq!(recv(&mut ws).await["close_context"], true);
        assert_eq!(recv(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    stream.sender().finish().await.unwrap();
    assert!(matches!(
        stream.next().await.unwrap(),
        Err(StreamingError::Timeout(_))
    ));
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn vad_mode_never_sends_manual_commit_and_preserves_finality() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("key");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.commit_strategy = CommitStrategy::Vad;
    config.limits = limits();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(recv(&mut ws).await["commit"], false);
        write_final(&mut ws).await;
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
    });
    let mut session = ElevenLabsSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session.sender().send_audio(pcm()).await.unwrap();
    assert!(matches!(
        session.sender().commit().await,
        Err(StreamingError::Invalid(_))
    ));
    session.sender().finish().await.unwrap();
    let mut count = 0;
    while let Some(event) = session.next().await {
        if let ElevenLabsSttEvent::Transcript(t) = event.unwrap() {
            assert!(t.is_final);
            assert!(!t.finalized);
            count += 1;
        }
    }
    assert_eq!(count, 1);
    server.await.unwrap();
}
async fn write_final(ws: &mut WebSocketStream<TcpStream>) {
    send(
        ws,
        json!({"message_type":"committed_transcript","text":"vad final"}),
    )
    .await;
}

#[tokio::test]
async fn oversized_provider_message_is_rejected_before_decoding() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    config.limits.max_message_bytes = 256;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        send(&mut ws, json!({"contextId":"ctx","audio":"A".repeat(1024)})).await;
        // The protocol may already be closing after its frame-size rejection.
        let _ = tokio::time::timeout(Duration::from_secs(1), async {
            while let Some(message) = ws.next().await {
                match message {
                    Ok(Message::Text(text)) if text.contains("close_socket") => {
                        let _ = ws.close(None).await;
                        break;
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    _ => {}
                }
            }
        })
        .await;
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::TooLarge
    );
    server.await.unwrap();
}

#[tokio::test]
async fn cancelled_next_can_be_retried_after_its_future_is_dropped() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    config.limits.close_timeout = Duration::from_secs(2);
    let (ready, wait_ready) = tokio::sync::oneshot::channel();
    let (release, wait_release) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        ready.send(()).unwrap();
        assert_eq!(recv(&mut ws).await["close_context"], true);
        assert_eq!(recv(&mut ws).await["close_socket"], true);
        wait_release.await.unwrap();
        ws.close(None).await.unwrap();
    });
    let mut stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    wait_ready.await.unwrap();
    stream.cancel();
    // Simulate a caller using next() in a timeout or select, while protocol
    // cleanup still owns the socket and the task's terminal result.
    assert!(
        tokio::time::timeout(Duration::from_millis(30), stream.next())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap(),
        Some(Err(StreamingError::Cancelled))
    );
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn stt_22050_hz_keepalive_contains_complete_pcm16_samples() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("key");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.sample_rate = 22050;
    config.keepalive_interval = Duration::from_millis(10);
    config.limits = limits();
    let server = tokio::spawn(async move {
        use base64::Engine;
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        let keepalive = recv(&mut ws).await;
        assert_eq!(keepalive["sample_rate"], 22050);
        assert_eq!(keepalive["commit"], false);
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(keepalive["audio_base_64"].as_str().unwrap())
            .unwrap();
        assert!(!bytes.is_empty());
        assert!(bytes.iter().all(|byte| *byte == 0));
        assert_eq!(bytes.len() % 2, 0, "keepalive split a PCM16 sample");
    });
    let session = ElevenLabsSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap();
    session.cancel();
    result.unwrap();
}

#[tokio::test]
async fn repeated_stt_finals_are_deduplicated_by_source_pair_not_transcript_text() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("key");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.include_language_detection = true;
    config.limits = limits();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        for reversed in [true, false] {
            assert_eq!(recv(&mut ws).await["commit"], false);
            assert_eq!(recv(&mut ws).await["commit"], true);
            let plain = json!({"message_type":"committed_transcript","text":"again"});
            let with_language = json!({"message_type":"committed_transcript_with_timestamps","text":"again","language_code":"en","words":null});
            for event in if reversed {
                [with_language, plain]
            } else {
                [plain, with_language]
            } {
                send(&mut ws, event).await;
            }
        }
        assert!(matches!(ws.next().await, Some(Ok(Message::Close(_)))));
    });
    let mut session = ElevenLabsSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    for _ in 0..2 {
        session.sender().send_audio(pcm()).await.unwrap();
        session.sender().commit().await.unwrap();
    }
    session.sender().finish().await.unwrap();
    let mut transcripts = vec![];
    let mut commits = 0;
    while let Some(event) = session.next().await {
        match event.unwrap() {
            ElevenLabsSttEvent::Transcript(transcript) => transcripts.push(transcript),
            ElevenLabsSttEvent::Committed => commits += 1,
            _ => {}
        }
    }
    assert_eq!(commits, 2);
    assert_eq!(transcripts.len(), 2);
    assert!(transcripts
        .iter()
        .all(|t| t.text == "again" && t.language.as_deref() == Some("en") && t.finalized));
    server.await.unwrap();
}

#[tokio::test]
async fn malformed_tts_payloads_fail_the_socket_without_audio_or_word_leakage() {
    let cases = [
        ("not json".to_owned(), StreamingError::Protocol("ElevenLabs TTS JSON")),
        (json!({"contextId":"ctx","audio":"not-base64"}).to_string(), StreamingError::Protocol("ElevenLabs base64 audio")),
        (json!({"contextId":"ctx","audio":"AQ==","isFinal":true}).to_string(), StreamingError::PartialSample),
        (json!({"contextId":"ctx","audio":"AQI=","alignment":{"chars":["a"],"charStartTimesMs":[],"charDurationsMs":[]}}).to_string(), StreamingError::Protocol("alignment array lengths")),
        (json!({"contextId":"old-context","audio":"AQI="}).to_string(), StreamingError::Protocol("unexpected multiplexed TTS context")),
        (json!({"contextId":"ctx","audio":"AQI=","isFinal":"true"}).to_string(), StreamingError::Protocol("ElevenLabs isFinal boolean")),
    ];
    for (payload, expected) in cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = ElevenLabsTtsConfig::new("secret", "voice");
        config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
        config.limits = limits();
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            recv(&mut ws).await;
            assert_eq!(recv(&mut ws).await["flush"], true);
            assert_eq!(recv(&mut ws).await["close_context"], true);
            ws.send(Message::Text(payload.into())).await.unwrap();
            assert_eq!(recv(&mut ws).await["close_context"], true);
            assert_eq!(recv(&mut ws).await["close_socket"], true);
            ws.close(None).await.unwrap();
        });
        let mut stream = ElevenLabsTtsClient::new(config)
            .unwrap()
            .stream("ctx")
            .unwrap();
        stream.sender().finish().await.unwrap();
        assert_eq!(stream.next().await, Some(Err(expected)));
        assert!(stream.next().await.is_none());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn dropping_tts_stream_closes_its_socket_even_with_a_surviving_sender() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsTtsConfig::new("key", "voice");
    config.endpoint = format!("ws://{}", listener.local_addr().unwrap());
    config.limits = limits();
    config.limits.queue_capacity = 1;
    let (ready, wait_ready) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        recv(&mut ws).await;
        for _ in 0..3 {
            send(&mut ws, json!({"contextId":"ctx","audio":"AQI="})).await;
        }
        ready.send(()).unwrap();
        assert_eq!(recv(&mut ws).await["close_context"], true);
        assert_eq!(recv(&mut ws).await["close_socket"], true);
        ws.close(None).await.unwrap();
    });
    let stream = ElevenLabsTtsClient::new(config)
        .unwrap()
        .stream("ctx")
        .unwrap();
    let sender = stream.sender();
    wait_ready.await.unwrap();
    drop(stream);
    assert_eq!(
        sender.send_text("stale").await,
        Err(StreamingError::Cancelled)
    );
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn dropping_stt_connect_future_releases_an_unfinished_handshake_socket() {
    use tokio::io::AsyncReadExt;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = ElevenLabsSttConfig::new("key");
    config.endpoint = format!("ws://{}/stt", listener.local_addr().unwrap());
    config.limits = limits();
    let server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let mut request = vec![];
        socket.read_to_end(&mut request).await.unwrap();
        assert!(request.starts_with(b"GET /stt?"));
    });
    let client = ElevenLabsSttClient::new(config).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(30), client.connect())
            .await
            .is_err()
    );
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
