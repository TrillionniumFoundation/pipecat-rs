mod support;
use pipecat_gladia::*;
use serde_json::json;
use std::time::Duration;
use support::*;
use tokio::{io::AsyncWriteExt, net::TcpListener};
use tokio_tungstenite::tungstenite::Message;
#[tokio::test]
async fn rest_nested_settings_auth_region_real_audio_and_inclusive_ack() {
    let mut peer = support::peer(1).await;
    peer.config.region = Some(GladiaRegion::UsWest);
    peer.config.settings=serde_json::from_value(json!({"model":"solaria-1","enable_vad":true,"language":"ignored-local-language","language_config":{"languages":["en","fr"],"code_switching":true},"custom_metadata":{"pipecat":"override-me","application":"fixture"},"endpointing":0.2,"pre_processing":{"audio_enhancer":true},"realtime_processing":{"custom_vocabulary":true,"custom_vocabulary_config":{"vocabulary":["Pipecat",{"value":"Gladia","intensity":0.4,"pronunciations":["glad-ee-ah"],"language":"en"}],"default_intensity":0.3},"custom_spelling":true,"custom_spelling_config":{"spelling_dictionary":{"Pipecat":["pipe cat"]}},"translation":true,"translation_config":{"target_languages":["fr"],"model":"base","informal":true}},"messages_config":{"receive_acknowledgments":true,"receive_errors":true}})).unwrap();
    let mut session = GladiaSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let (headers, body) = bounded(peer.requests.recv()).await.unwrap();
    assert!(headers.starts_with("POST /v2/live?region=us-west HTTP/1.1"));
    assert!(headers
        .to_ascii_lowercase()
        .contains("x-gladia-key: synthetic-rest-key"));
    assert_eq!(body, peer.config.session_request().unwrap());
    assert_eq!(body["encoding"], "wav/pcm");
    assert_eq!(body["bit_depth"], 16);
    assert_eq!(body["channels"], 1);
    assert_eq!(body["sample_rate"], 16000);
    assert_eq!(
        body["custom_metadata"]["pipecat"],
        env!("CARGO_PKG_VERSION")
    );
    assert!(body.get("enable_vad").is_none());
    assert!(body.get("language").is_none());
    assert!(body["pre_processing"].get("speech_threshold").is_none());
    {
        let headers = peer.headers.lock().unwrap();
        assert!(headers[0].headers().get("x-gladia-key").is_none());
        assert!(headers[0].headers().get("authorization").is_none());
        assert_eq!(
            headers[0].uri().query(),
            Some("token=synthetic-session-token")
        );
    }
    let sender = session.sender();
    sender.send_audio(pcm(4096)).await.unwrap();
    let value = next_wire(&mut peer).await;
    assert_eq!(value["type"], "audio_chunk");
    use base64::Engine;
    assert_eq!(
        base64::engine::general_purpose::STANDARD
            .decode(value["data"]["chunk"].as_str().unwrap())
            .unwrap(),
        vec![42; 4096]
    );
    emit(&peer,json!({"type":"audio_chunk","session_id":"fixture-0","acknowledged":true,"data":{"byte_range":[0,4095]}})).await;
    let GladiaSttEvent::Acknowledgment(ack) = next(&mut session).await else {
        panic!()
    };
    assert_eq!(ack.acknowledged_bytes, 4096);
    assert_eq!(ack.buffered_bytes, 0);
    assert_eq!(ack.generation, session.generation());
    sender.finish().await.unwrap();
    assert_eq!(next_wire(&mut peer).await, json!({"type":"stop_recording"}));
    bounded(peer.task).await.unwrap();
    assert!(bounded(session.next()).await.is_none());
}
#[tokio::test]
async fn translations_errors_raw_transcripts_and_deduplicated_turns() {
    let peer = support::peer(1).await;
    let mut config = peer.config.clone();
    config.settings.enable_vad = true;
    let mut session = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    for kind in ["speech_start", "speech_start", "speech_end", "speech_end"] {
        emit(&peer, json!({"type":kind})).await;
    }
    assert_eq!(next(&mut session).await, GladiaSttEvent::TurnStarted);
    assert_eq!(next(&mut session).await, GladiaSttEvent::TurnStopped);
    emit(
        &peer,
        json!({"type":"translation","data":null,"error":{"message":"synthetic secret"}}),
    )
    .await;
    assert_eq!(
        next(&mut session).await,
        GladiaSttEvent::Error(GladiaError::Provider("translation addon"))
    );
    emit(&peer,json!({"type":"translation","data":{"original_language":"en","translated_utterance":{"language":"en","text":"same"}}})).await;
    let value = transcript("hello", false);
    emit(&peer, value.clone()).await;
    let GladiaSttEvent::Transcript(t) = next(&mut session).await else {
        panic!()
    };
    assert_eq!(t.result, value);
    assert!(!t.is_final);
    assert_eq!(t.language.as_deref(), Some("en"));
    emit(&peer,json!({"type":"translation","data":{"original_language":"en","translated_utterance":{"language":"fr","text":"bonjour"}}})).await;
    let GladiaSttEvent::Translation(t) = next(&mut session).await else {
        panic!()
    };
    assert_eq!(t.text, "bonjour");
    assert_eq!(t.original_language, "en");
    emit(
        &peer,
        json!({"type":"post_summarization","data":{"results":"ignored addon"}}),
    )
    .await;
    assert_eq!(
        next(&mut session).await,
        GladiaSttEvent::Observed {
            kind: "post_summarization".into()
        }
    );
    emit(&peer, transcript("still works", true)).await;
    assert!(matches!(next(&mut session).await,GladiaSttEvent::Transcript(t)if t.is_final));
    session.cancel();
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn ack_generation_malformed_partial_budget_and_no_replay() {
    let mut peer = support::peer(1).await;
    peer.config.limits.max_audio_chunk_bytes = 4096;
    peer.config.limits.max_buffered_audio_bytes = 4096;
    let mut session = GladiaSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    sender.try_send_audio(pcm(4096)).unwrap();
    next_wire(&mut peer).await;
    assert_eq!(
        sender.try_send_audio(pcm(2)),
        Err(GladiaError::Backpressure)
    );
    for value in [
        json!({"type":"audio_chunk","acknowledged":true,"data":{"byte_range":[0,4096]}}),
        json!({"type":"audio_chunk","acknowledged":true,"data":{"byte_range":[0]}}),
        json!({"type":"audio_chunk","session_id":"old-session","acknowledged":true,"data":{"byte_range":[0,4095]}}),
    ] {
        emit(&peer, value).await;
        assert!(matches!(next(&mut session).await, GladiaSttEvent::Error(_)));
        assert_eq!(
            sender.try_send_audio(pcm(2)),
            Err(GladiaError::Backpressure)
        );
    }
    emit(
        &peer,
        json!({"type":"audio_chunk","acknowledged":true,"data":{"byte_range":[0,2047]}}),
    )
    .await;
    assert!(
        matches!(next(&mut session).await,GladiaSttEvent::Acknowledgment(a)if a.acknowledged_bytes==2048&&a.buffered_bytes==4096)
    );
    emit(
        &peer,
        json!({"type":"audio_chunk","acknowledged":true,"data":{"byte_range":[2048,4095]}}),
    )
    .await;
    next(&mut session).await;
    sender.try_send_audio(pcm(2)).unwrap();
    next_wire(&mut peer).await;
    peer.send.send(Message::Close(None)).await.unwrap();
    assert_eq!(
        bounded(session.next()).await,
        Some(Err(GladiaError::Closed))
    );
    assert!(sender.is_closed());
    bounded(peer.task).await.unwrap();
}
#[tokio::test]
async fn future_settings_are_atomic_and_keepalive_is_empty() {
    let mut peer = support::peer(1).await;
    peer.config.limits.keepalive_interval = Duration::from_millis(20);
    let mut session = GladiaSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    assert_eq!(
        next_wire(&mut peer).await,
        json!({"type":"audio_chunk","data":{"chunk":""}})
    );
    sender
        .merge_settings(
            json!({"model":"future-model","enable_vad":true})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(
        sender.config_snapshot().unwrap().settings.model,
        "future-model"
    );
    assert_eq!(sender.active_settings().model, "solaria-1");
    assert!(!sender.active_settings().enable_vad);
    assert!(sender
        .merge_settings(json!({"model":null}).as_object().unwrap())
        .is_err());
    assert!(sender
        .merge_settings(json!({"endpointing":-1}).as_object().unwrap())
        .is_err());
    assert!(sender
        .merge_settings(json!({"unknown":true}).as_object().unwrap())
        .is_err());
    assert_eq!(
        sender.config_snapshot().unwrap().settings.model,
        "future-model"
    );
    emit(&peer, json!({"type":"speech_start"})).await;
    emit(&peer, transcript("no invented audio", true)).await;
    let GladiaSttEvent::Transcript(t) = next(&mut session).await else {
        panic!()
    };
    assert_eq!(t.audio_seconds, 0.0);
    sender.cancel();
    bounded(peer.task).await.unwrap();
    while let Some(message) = peer.wire.recv().await {
        if let Message::Text(s) = message {
            assert!(!s.contains("stop_recording"));
        }
    }
}
#[tokio::test]
async fn full_event_output_drop_and_read_deadlines_are_bounded() {
    let peer = support::peer(1).await;
    let mut config = peer.config.clone();
    config.limits.queue_capacity = 1;
    let mut session = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    emit(&peer, transcript("first", false)).await;
    emit(&peer, transcript("second", false)).await;
    bounded(peer.task).await.unwrap();
    assert!(matches!(
        next(&mut session).await,
        GladiaSttEvent::Transcript(_)
    ));
    assert_eq!(
        bounded(session.next()).await,
        Some(Err(GladiaError::Backpressure))
    );
    let peer = support::peer(1).await;
    let mut config = peer.config.clone();
    config.limits.read_timeout = Duration::from_millis(50);
    let mut session = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    assert_eq!(
        bounded(session.next()).await,
        Some(Err(GladiaError::Timeout("read")))
    );
    bounded(peer.task).await.unwrap();
    let peer = support::peer(1).await;
    let session = GladiaSttClient::new(peer.config.clone())
        .unwrap()
        .connect()
        .await
        .unwrap();
    let sender = session.sender();
    drop(session);
    bounded(peer.task).await.unwrap();
    assert!(sender.is_closed());
}
#[tokio::test]
async fn returned_endpoint_is_untrusted_and_rest_redirect_is_not_followed() {
    for returned in [
        "ws://127.0.0.1:9/leak?token=private",
        "wss://attacker.invalid/leak?token=private",
        "wss://user:pass@api.gladia.io/leak",
        "ws://localhost:9/leak",
    ] {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GladiaSttConfig::new("synthetic");
        config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
        config.allow_loopback = true;
        let body = json!({"id":"fixture","url":returned}).to_string();
        let server = tokio::spawn(async move {
            let (mut socket, _) = http.accept().await.unwrap();
            request(&mut socket).await;
            respond(&mut socket, 201, &body).await;
        });
        let error = GladiaSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .err()
            .unwrap();
        assert_eq!(error, GladiaError::Endpoint);
        assert!(!format!("{error:?}").contains("private"));
        bounded(server).await.unwrap();
    }
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = http.local_addr().unwrap();
    let mut config = GladiaSttConfig::new("synthetic");
    config.endpoint = format!("http://{address}/v2/live");
    config.allow_loopback = true;
    let server = tokio::spawn(async move {
        let (mut socket, _) = http.accept().await.unwrap();
        request(&mut socket).await;
        socket.write_all(format!("HTTP/1.1 307 Redirect\r\nLocation: http://{address}/leak\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), http.accept())
                .await
                .is_err()
        );
    });
    assert_eq!(
        GladiaSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .err()
            .unwrap(),
        GladiaError::Http(307)
    );
    bounded(server).await.unwrap();
}
#[tokio::test]
async fn http_errors_body_limit_and_setup_cancellation_do_not_claim_connected() {
    for (status, body, limit, expected) in [
        (
            401,
            "private provider error".into(),
            65536,
            GladiaError::Http(401),
        ),
        (201, "x".repeat(1024), 128, GladiaError::TooLarge),
        (
            201,
            "{}".into(),
            65536,
            GladiaError::Protocol("session identity"),
        ),
    ] {
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = GladiaSttConfig::new("synthetic");
        config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
        config.allow_loopback = true;
        config.limits.max_http_body_bytes = limit;
        // Request itself must fit the bound; use a minimal valid response bound.
        if limit == 128 {
            config.limits.max_http_body_bytes = 256;
        }
        let server = tokio::spawn(async move {
            let (mut socket, _) = http.accept().await.unwrap();
            request(&mut socket).await;
            respond(&mut socket, status, &body).await;
        });
        assert_eq!(
            GladiaSttClient::new(config)
                .unwrap()
                .connect()
                .await
                .err()
                .unwrap(),
            expected
        );
        bounded(server).await.unwrap();
    }
    let token = CancellationToken::new();
    token.cancel();
    let client = GladiaSttClient::new(GladiaSttConfig::new("synthetic")).unwrap();
    assert_eq!(
        client.connect_with_cancel(token).await.err().unwrap(),
        GladiaError::Cancelled
    );
}
#[test]
fn configuration_validation_redacts_and_preserves_old_values() {
    let config = GladiaSttConfig::new("private-key");
    assert!(!format!("{config:?}").contains("private-key"));
    let settings = GladiaSttSettings::default();
    let mut changed = settings.clone();
    changed.pre_processing = Some(GladiaPreProcessingConfig {
        speech_threshold: Some(f64::NAN),
        ..Default::default()
    });
    assert!(changed.validate().is_err());
    assert!(settings
        .merge(
            json!({"realtime_processing":{"translation_config":{"unexpected":true}}})
                .as_object()
                .unwrap()
        )
        .is_err());
    assert_eq!(settings.model, "solaria-1");
    assert!(settings
        .merge(
            json!({"messages_config":{"receive_acknowledgments":false}})
                .as_object()
                .unwrap()
        )
        .is_err());
    let mut config = config;
    config.allowed_session_origins = vec!["wss://api.gladia.io/path".into()];
    assert!(config.validate().is_err());
}

#[tokio::test]
async fn websocket_redirect_is_not_followed_or_given_rest_credentials() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws.local_addr().unwrap();
    let mut config = GladiaSttConfig::new("private-rest-key");
    config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
    config.allowed_session_origins = vec![format!("ws://{ws_addr}")];
    config.allow_loopback = true;
    let server = tokio::spawn(async move {
        let (mut socket, _) = http.accept().await.unwrap();
        request(&mut socket).await;
        respond(
            &mut socket,
            201,
            &json!({"id":"fixture","url":format!("ws://{ws_addr}/session?token=private")})
                .to_string(),
        )
        .await;
        let (mut socket, _) = ws.accept().await.unwrap();
        let (headers, _) = request(&mut socket).await;
        assert!(!headers.contains("private-rest-key"));
        socket.write_all(format!("HTTP/1.1 302 Redirect\r\nLocation: ws://{ws_addr}/leak?token=private\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(100), ws.accept())
                .await
                .is_err()
        );
    });
    assert_eq!(
        GladiaSttClient::new(config)
            .unwrap()
            .connect()
            .await
            .err()
            .unwrap(),
        GladiaError::Http(302)
    );
    bounded(server).await.unwrap();
}

#[tokio::test]
async fn cancellation_preempts_a_busy_native_writer_without_stop_or_replay() {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws_addr = ws.local_addr().unwrap();
    let mut config = GladiaSttConfig::new("synthetic");
    config.endpoint = format!("http://{}/v2/live", http.local_addr().unwrap());
    config.allowed_session_origins = vec![format!("ws://{ws_addr}")];
    config.allow_loopback = true;
    config.limits.max_audio_chunk_bytes = 512 * 1024;
    config.limits.max_buffered_audio_bytes = 32 * 1024 * 1024;
    config.limits.queue_capacity = 64;
    config.limits.close_timeout = Duration::from_millis(50);
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (drop_tx, drop_rx) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let (mut socket, _) = http.accept().await.unwrap();
        request(&mut socket).await;
        respond(
            &mut socket,
            201,
            &json!({"id":"fixture","url":format!("ws://{ws_addr}/session")}).to_string(),
        )
        .await;
        let socket = tokio_tungstenite::accept_async(ws.accept().await.unwrap().0)
            .await
            .unwrap();
        ready_tx.send(()).unwrap();
        let _ = drop_rx.await;
        drop(socket);
    });
    let mut session = GladiaSttClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    bounded(ready_rx).await.unwrap();
    let sender = session.sender();
    for _ in 0..64 {
        sender.try_send_audio(pcm(512 * 1024)).unwrap();
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    sender.cancel();
    assert_eq!(
        bounded(session.next()).await,
        Some(Err(GladiaError::Cancelled))
    );
    assert!(sender.is_closed());
    let _ = drop_tx.send(());
    bounded(server).await.unwrap();
}
