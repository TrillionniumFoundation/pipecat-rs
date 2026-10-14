#![cfg(feature = "deepgram-ws")]
//! Real loopback WebSockets and synthetic credentials only; not live qualification.
use futures_util::{SinkExt, StreamExt};
use pipecat_speech::{
    deepgram::{DeepgramEncoding, DeepgramSynthesisRequest},
    deepgram_ws::*,
    SpeechChunk, SpeechError,
};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_tungstenite::{
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};

type Server = WebSocketStream<TcpStream>;
// Tungstenite requires its unboxed HTTP error response in this handshake callback.
#[allow(clippy::result_large_err)]
async fn server<F, Fut>(
    edit: impl FnOnce(&mut DeepgramWsConfig),
    handler: F,
) -> (DeepgramWsClient, JoinHandle<()>)
where
    F: FnOnce(Server, Request) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramWsConfig::new("synthetic-secret");
    config.endpoint = format!("ws://{}/v1/speak", listener.local_addr().unwrap());
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.response_timeout = Duration::from_secs(3);
    config.limits.close_timeout = Duration::from_millis(150);
    edit(&mut config);
    let task = tokio::spawn(async move {
        let (stream, _) = tokio::time::timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let capture = Arc::new(Mutex::new(None));
        let captured = capture.clone();
        let ws = tokio_tungstenite::accept_hdr_async(
            stream,
            move |request: &Request, response: Response| {
                *captured.lock().unwrap() = Some(request.clone());
                Ok(response)
            },
        )
        .await
        .unwrap();
        let request = capture.lock().unwrap().take().unwrap();
        handler(ws, request).await;
    });
    (DeepgramWsClient::new(config).unwrap(), task)
}
async fn read_json(ws: &mut Server) -> Value {
    let message = tokio::time::timeout(Duration::from_secs(2), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
async fn start(ws: &mut Server) -> Value {
    let speak = read_json(ws).await;
    assert_eq!(speak["type"], "Speak");
    assert_eq!(read_json(ws).await, json!({"type":"Flush"}));
    speak
}
async fn text(ws: &mut Server, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn binary(ws: &mut Server, bytes: &[u8]) {
    ws.send(Message::Binary(bytes.to_vec().into()))
        .await
        .unwrap();
}
async fn close(ws: &mut Server, cancelled: bool) {
    if cancelled {
        assert_eq!(read_json(ws).await, json!({"type":"Clear"}));
    }
    assert_eq!(read_json(ws).await, json!({"type":"Close"}));
    assert!(matches!(
        ws.next().await.unwrap().unwrap(),
        Message::Close(_)
    ));
    ws.flush().await.unwrap();
}
async fn observe(mut stream: DeepgramWsStream) -> (Vec<SpeechChunk>, Result<(), SpeechError>) {
    let mut chunks = Vec::new();
    while let Some(result) = stream.next().await {
        match result {
            Ok(chunk) => chunks.push(chunk),
            Err(error) => {
                assert!(stream.next().await.is_none());
                return (chunks, Err(error));
            }
        }
    }
    assert!(stream.next().await.is_none());
    (chunks, Ok(()))
}
fn request() -> DeepgramSynthesisRequest {
    DeepgramSynthesisRequest::new("你好\nhello")
}

fn source_case(name: &str) -> Value {
    let source: Value =
        serde_json::from_str(include_str!("fixtures/deepgram_ws_source.json")).unwrap();
    assert_eq!(
        source["baseline"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    source["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["name"] == name)
        .unwrap()
        .clone()
}

#[tokio::test]
async fn exact_default_wire_and_fragmented_pcm() {
    let (client, task) = server(
        |config| config.api_key = "fake-offline-key".into(),
        |mut ws, upgrade| async move {
            let original_connect = source_case("connect_default_query_token_header");
            assert_eq!(
                upgrade.headers()["authorization"],
                original_connect["trace"][0]["headers"]["Authorization"]
                    .as_str()
                    .unwrap()
            );
            let url = reqwest::Url::parse(&format!("http://localhost{}", upgrade.uri())).unwrap();
            assert_eq!(url.path(), "/v1/speak");
            let source_url =
                reqwest::Url::parse(original_connect["trace"][0]["url"].as_str().unwrap()).unwrap();
            assert_eq!(
                url.query_pairs().collect::<Vec<_>>(),
                source_url.query_pairs().collect::<Vec<_>>()
            );
            assert_eq!(
                url.query_pairs().collect::<Vec<_>>(),
                [
                    ("model".into(), "aura-2-helena-en".into()),
                    ("encoding".into(), "linear16".into()),
                    ("sample_rate".into(), "24000".into())
                ]
            );
            assert_eq!(
                read_json(&mut ws).await,
                source_case("run_speak_only_no_flush_or_context_id")["trace"][0]["message"]
            );
            assert_eq!(
                read_json(&mut ws).await,
                source_case("flush_connected")["trace"][0]["message"]
            );
            text(
                &mut ws,
                json!({"type":"Metadata", "request_id":"synthetic"}),
            )
            .await;
            binary(&mut ws, &[1]).await;
            binary(&mut ws, &[2, 3, 4, 5]).await;
            binary(&mut ws, &[6]).await;
            text(&mut ws, json!({"type":"Flushed", "sequence_id":0})).await;
            close(&mut ws, false).await;
        },
    )
    .await;
    let original = source_case("run_speak_only_no_flush_or_context_id");
    let source_request =
        DeepgramSynthesisRequest::new(original["trace"][0]["message"]["text"].as_str().unwrap());
    let (chunks, result) = observe(client.synthesize(source_request).unwrap()).await;
    result.unwrap();
    task.await.unwrap();
    assert_eq!(
        chunks
            .iter()
            .flat_map(|c| c.pcm.clone())
            .collect::<Vec<_>>(),
        [1, 2, 3, 4, 5, 6]
    );
    assert!(chunks
        .iter()
        .all(|c| c.sample_rate == 24000 && c.num_channels == 1 && c.pcm.len() % 2 == 0));
    assert_eq!(client.active_sessions(), 0);
    client.cleanup().await.unwrap();
}

#[tokio::test]
async fn query_encoding_optional_false_and_sample_rates() {
    for rate in [8000, 16000, 24000, 32000, 48000] {
        let (client, task) = server(
            |_| {},
            move |mut ws, upgrade| async move {
                let url =
                    reqwest::Url::parse(&format!("http://localhost{}", upgrade.uri())).unwrap();
                let pairs = url
                    .query_pairs()
                    .collect::<std::collections::HashMap<_, _>>();
                assert_eq!(pairs["model"], "voice +&?/测试");
                assert_eq!(pairs["sample_rate"], rate.to_string());
                assert_eq!(pairs["mip_opt_out"], "false");
                assert_eq!(pairs["speed"], "0.7");
                assert_eq!(pairs.len(), 5);
                start(&mut ws).await;
                binary(&mut ws, &[1, 2]).await;
                text(&mut ws, json!({"type":"Flushed"})).await;
                close(&mut ws, false).await;
            },
        )
        .await;
        let mut req = request();
        req.voice = "voice +&?/测试".into();
        req.sample_rate = rate;
        req.speed = Some(0.7);
        req.mip_opt_out = Some(false);
        let (chunks, result) = observe(client.synthesize(req).unwrap()).await;
        result.unwrap();
        assert_eq!(chunks[0].sample_rate, rate);
        task.await.unwrap();
    }
}

#[test]
fn strict_configuration_validation_and_redacted_errors() {
    for endpoint in [
        "http://127.0.0.1/v1/speak",
        "ws://api.deepgram.com/v1/speak",
        "wss://example.com/v1/speak",
        "wss://api.deepgram.com/other",
        "wss://api.deepgram.com:8443/v1/speak",
        "wss://user:secret@api.deepgram.com/v1/speak",
        "wss://api.deepgram.com/v1/speak?api_key=secret",
        "wss://api.deepgram.com/v1/speak#secret",
    ] {
        let mut config = DeepgramWsConfig::new("synthetic-secret");
        config.endpoint = endpoint.into();
        assert!(
            DeepgramWsClient::new(config).is_err(),
            "accepted {endpoint}"
        );
    }
    for key in ["", "secret\r\nheader: value"] {
        assert!(DeepgramWsClient::new(DeepgramWsConfig::new(key)).is_err());
    }
    let config = DeepgramWsConfig::new("synthetic-secret");
    assert!(!format!("{config:?}").contains("synthetic-secret"));
    let mut config = config.clone();
    config.limits.max_sessions = 0;
    assert!(DeepgramWsClient::new(config).is_err());
    let mut config = DeepgramWsConfig::new("synthetic");
    config.limits.queue_capacity = usize::MAX;
    assert!(DeepgramWsClient::new(config).is_err());
    let mut config = DeepgramWsConfig::new("synthetic");
    config.limits.close_timeout = Duration::MAX;
    assert!(DeepgramWsClient::new(config).is_err());
    let client = DeepgramWsClient::new(DeepgramWsConfig::new("synthetic")).unwrap();
    // Even a valid request outside a runtime fails before admission/spawn.
    assert!(matches!(
        client.synthesize(request()),
        Err(SpeechError::Invalid(_))
    ));
    assert_eq!(client.active_sessions(), 0);
}

#[tokio::test]
async fn request_limits_fail_before_spawn() {
    let mut config = DeepgramWsConfig::new("synthetic");
    config.limits.max_text_bytes = 32;
    let client = DeepgramWsClient::new(config).unwrap();
    for speed in [f64::NAN, f64::INFINITY, 0.69, 1.51] {
        let mut req = request();
        req.speed = Some(speed);
        assert!(client.synthesize(req).is_err());
    }
    for encoding in [DeepgramEncoding::Mulaw, DeepgramEncoding::Alaw] {
        let mut req = request();
        req.encoding = encoding;
        assert!(client.synthesize(req).is_err());
    }
    for rate in [0, 22050, 44100] {
        let mut req = request();
        req.sample_rate = rate;
        assert!(client.synthesize(req).is_err());
    }
    let mut req = request();
    req.text = "x".repeat(33);
    assert!(matches!(client.synthesize(req), Err(SpeechError::TooLarge)));
    let mut req = request();
    req.voice = "x".repeat(257);
    assert!(matches!(client.synthesize(req), Err(SpeechError::TooLarge)));
    assert_eq!(client.active_sessions(), 0);
}

#[tokio::test]
async fn malformed_unknown_out_of_order_and_provider_controls_fail_closed() {
    for message in [
        "{",
        "[]",
        r#"{"type":"Unknown"}"#,
        r#"{"type":"Cleared"}"#,
        r#"{"type":"Error","description":"synthetic-secret echo"}"#,
        r#"{"type":"Flushed","type":"Metadata"}"#,
    ] {
        let (client, task) = server(
            |_| {},
            move |mut ws, _| async move {
                start(&mut ws).await;
                ws.send(Message::Text(message.into())).await.unwrap();
                close(&mut ws, true).await;
            },
        )
        .await;
        let (chunks, result) = observe(client.synthesize(request()).unwrap()).await;
        assert!(chunks.is_empty());
        let error = result.unwrap_err();
        assert!(matches!(error, SpeechError::Protocol(_)));
        assert!(!format!("{error:?} {error}").contains("synthetic-secret"));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn early_flushed_empty_and_partial_samples_never_succeed() {
    for bytes in [vec![], vec![1]] {
        let odd = !bytes.is_empty();
        let (client, task) = server(
            |_| {},
            move |mut ws, _| async move {
                start(&mut ws).await;
                if !bytes.is_empty() {
                    binary(&mut ws, &bytes).await;
                }
                text(&mut ws, json!({"type":"Flushed"})).await;
                close(&mut ws, true).await;
            },
        )
        .await;
        let (chunks, result) = observe(client.synthesize(request()).unwrap()).await;
        assert!(chunks.is_empty());
        if odd {
            assert!(matches!(result, Err(SpeechError::PartialSample)));
        } else {
            assert!(matches!(result, Err(SpeechError::EmptyAudio)));
        }
        task.await.unwrap();
    }
}

#[tokio::test]
async fn duplicate_metadata_and_metadata_after_pcm_fail_closed() {
    for after_pcm in [false, true] {
        let (client, task) = server(
            |_| {},
            move |mut ws, _| async move {
                start(&mut ws).await;
                if after_pcm {
                    binary(&mut ws, &[1, 2]).await;
                } else {
                    text(&mut ws, json!({"type":"Metadata"})).await;
                }
                text(&mut ws, json!({"type":"Metadata"})).await;
                close(&mut ws, true).await;
            },
        )
        .await;
        let (_, result) = observe(client.synthesize(request()).unwrap()).await;
        assert!(matches!(result, Err(SpeechError::Protocol(_))));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn late_pcm_and_duplicate_flushed_are_terminal_failures() {
    for late_pcm in [false, true] {
        let (client, task) = server(
            |_| {},
            move |mut ws, _| async move {
                start(&mut ws).await;
                binary(&mut ws, &[1, 2]).await;
                // Feed both frames before flushing to make the adverse order
                // deterministic even if the client immediately starts WS close.
                ws.feed(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                    .await
                    .unwrap();
                if late_pcm {
                    ws.feed(Message::Binary(vec![3, 4].into())).await.unwrap();
                } else {
                    ws.feed(Message::Text(json!({"type":"Flushed"}).to_string().into()))
                        .await
                        .unwrap();
                }
                ws.flush().await.unwrap();
                while let Some(Ok(message)) = ws.next().await {
                    if matches!(message, Message::Close(_)) {
                        let _ = ws.flush().await;
                        break;
                    }
                }
            },
        )
        .await;
        let (chunks, result) = observe(client.synthesize(request()).unwrap()).await;
        assert_eq!(
            chunks
                .iter()
                .flat_map(|c| c.pcm.clone())
                .collect::<Vec<_>>(),
            [1, 2]
        );
        assert!(result.is_err());
        task.await.unwrap();
    }
}

#[tokio::test]
async fn peer_close_before_flushed_is_error() {
    let (client, task) = server(
        |_| {},
        |mut ws, _| async move {
            start(&mut ws).await;
            binary(&mut ws, &[1, 2]).await;
            ws.close(None).await.unwrap();
            while ws.next().await.is_some() {}
        },
    )
    .await;
    let (_, result) = observe(client.synthesize(request()).unwrap()).await;
    assert!(result.is_err());
    task.await.unwrap();
}

#[tokio::test]
async fn cancellation_under_backpressure_sends_clear_then_close_and_joins() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (client, task) = server(
        |c| c.limits.queue_capacity = 1,
        move |mut ws, _| async move {
            start(&mut ws).await;
            for _ in 0..8 {
                binary(&mut ws, &[1, 2]).await;
            }
            ready_tx.send(()).unwrap();
            close(&mut ws, true).await;
        },
    )
    .await;
    let mut stream = client.synthesize(request()).unwrap();
    ready_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(15)).await;
    stream.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
    client.cleanup().await.unwrap();
    assert_eq!(client.active_sessions(), 0);
    task.await.unwrap();
}

#[tokio::test]
async fn drop_only_signals_and_cancelling_cleanup_keeps_capacity_and_join_handle() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (close_seen_tx, close_seen_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (client, task) = server(
        |c| {
            c.limits.max_sessions = 1;
            c.limits.close_timeout = Duration::from_secs(2);
        },
        move |mut ws, _| async move {
            start(&mut ws).await;
            ready_tx.send(()).unwrap();
            assert_eq!(read_json(&mut ws).await, json!({"type":"Clear"}));
            assert_eq!(read_json(&mut ws).await, json!({"type":"Close"}));
            // Do not read/acknowledge WS close until the test releases it.
            close_seen_tx.send(()).unwrap();
            release_rx.await.unwrap();
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            ws.flush().await.unwrap();
        },
    )
    .await;
    let stream = client.synthesize(request()).unwrap();
    ready_rx.await.unwrap();
    drop(stream);
    close_seen_rx.await.unwrap();
    assert_eq!(client.active_sessions(), 1);
    assert!(client.synthesize(request()).is_err());
    assert!(
        tokio::time::timeout(Duration::from_millis(10), client.cleanup())
            .await
            .is_err()
    );
    assert_eq!(client.active_sessions(), 1);
    release_tx.send(()).unwrap();
    client.cleanup().await.unwrap();
    assert_eq!(client.active_sessions(), 0);
    assert!(
        client.synthesize(request()).is_err(),
        "sealed clones cannot restart"
    );
    task.await.unwrap();
}

#[tokio::test]
async fn cancelled_stream_shutdown_can_be_joined_again() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (client, task) = server(
        |c| c.limits.close_timeout = Duration::from_secs(2),
        move |mut ws, _| async move {
            start(&mut ws).await;
            ready_tx.send(()).unwrap();
            assert_eq!(read_json(&mut ws).await, json!({"type":"Clear"}));
            assert_eq!(read_json(&mut ws).await, json!({"type":"Close"}));
            release_rx.await.unwrap();
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            ws.flush().await.unwrap();
        },
    )
    .await;
    let mut stream = client.synthesize(request()).unwrap();
    ready_rx.await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(10), stream.shutdown())
            .await
            .is_err()
    );
    assert_eq!(client.active_sessions(), 1);
    release_tx.send(()).unwrap();
    assert!(matches!(
        stream.shutdown().await,
        Err(SpeechError::Cancelled)
    ));
    client.cleanup().await.unwrap();
    assert_eq!(client.active_sessions(), 0);
    task.await.unwrap();
}

#[tokio::test]
async fn finished_actors_are_reaped_and_every_utterance_gets_a_new_socket() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramWsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/v1/speak", listener.local_addr().unwrap());
    config.limits.max_sessions = 1;
    let client = DeepgramWsClient::new(config).unwrap();
    let task = tokio::spawn(async move {
        for _ in 0..5 {
            let (tcp, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
            start(&mut ws).await;
            binary(&mut ws, &[1, 2]).await;
            text(&mut ws, json!({"type":"Flushed"})).await;
            close(&mut ws, false).await;
        }
    });
    for _ in 0..5 {
        observe(client.synthesize(request()).unwrap())
            .await
            .1
            .unwrap();
    }
    assert_eq!(client.active_sessions(), 0);
    task.await.unwrap();
    client.cleanup().await.unwrap();
    assert_eq!(client.fork_owner().active_sessions(), 0);
}

#[tokio::test]
async fn independent_owner_cleanup_cannot_cancel_another_owner() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (client, task) = server(
        |_| {},
        move |mut ws, _| async move {
            start(&mut ws).await;
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            binary(&mut ws, &[1, 2]).await;
            text(&mut ws, json!({"type":"Flushed"})).await;
            close(&mut ws, false).await;
        },
    )
    .await;
    let independent = client.fork_owner();
    let stream = independent.synthesize(request()).unwrap();
    ready_rx.await.unwrap();
    client.cleanup().await.unwrap();
    assert_eq!(independent.active_sessions(), 1);
    release_tx.send(()).unwrap();
    observe(stream).await.1.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn audio_and_wire_budgets_fail_closed() {
    for wire in [false, true] {
        let (client, task) = server(
            move |c| {
                if wire {
                    c.limits.max_wire_bytes = 1;
                } else {
                    c.limits.max_audio_bytes = 1;
                }
            },
            |mut ws, _| async move {
                start(&mut ws).await;
                binary(&mut ws, &[1, 2]).await;
                close(&mut ws, true).await;
            },
        )
        .await;
        let (chunks, result) = observe(client.synthesize(request()).unwrap()).await;
        assert!(chunks.is_empty());
        assert!(matches!(result, Err(SpeechError::TooLarge)));
        task.await.unwrap();
    }
}

#[tokio::test]
async fn whole_response_deadline_includes_backpressure() {
    let (client, task) = server(
        |c| {
            c.limits.queue_capacity = 1;
            c.limits.response_timeout = Duration::from_millis(40);
        },
        |mut ws, _| async move {
            start(&mut ws).await;
            for _ in 0..5 {
                binary(&mut ws, &[1, 2]).await;
            }
            close(&mut ws, true).await;
        },
    )
    .await;
    let stream = client.synthesize(request()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let (_, result) = observe(stream).await;
    assert!(matches!(result, Err(SpeechError::Timeout)));
    task.await.unwrap();
}

#[tokio::test]
async fn redirects_do_not_forward_authorization() {
    let trap = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramWsConfig::new("synthetic-secret");
    config.endpoint = format!("ws://{}/v1/speak", listener.local_addr().unwrap());
    let destination = trap.local_addr().unwrap();
    let task = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        let mut request = vec![0; 8192];
        let _ = tcp.read(&mut request).await.unwrap();
        tcp.write_all(format!("HTTP/1.1 302 Found\r\nLocation: ws://{destination}/steal\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
    });
    let client = DeepgramWsClient::new(config).unwrap();
    let (_, result) = observe(client.synthesize(request()).unwrap()).await;
    assert!(matches!(result, Err(SpeechError::Http(302))));
    assert!(
        tokio::time::timeout(Duration::from_millis(25), trap.accept())
            .await
            .is_err()
    );
    task.await.unwrap();
}

#[tokio::test]
async fn stalled_handshake_cancels_and_releases_registry_slot() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = DeepgramWsConfig::new("synthetic");
    config.endpoint = format!("ws://{}/v1/speak", listener.local_addr().unwrap());
    config.limits.max_sessions = 1;
    let client = DeepgramWsClient::new(config).unwrap();
    let (ready_tx, ready_rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut tcp, _) = listener.accept().await.unwrap();
        ready_tx.send(()).unwrap();
        let mut bytes = vec![0; 8192];
        while tcp.read(&mut bytes).await.unwrap() != 0 {}
    });
    let stream = client.synthesize(request()).unwrap();
    ready_rx.await.unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_millis(200), client.cleanup())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.active_sessions(), 0);
    task.await.unwrap();
}

#[tokio::test]
async fn successful_eof_waits_for_close_ack_and_actor_join() {
    let (closing_tx, closing_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (client, task) = server(
        |config| config.limits.close_timeout = Duration::from_secs(2),
        move |mut ws, _| async move {
            start(&mut ws).await;
            binary(&mut ws, &[1, 2]).await;
            text(&mut ws, json!({"type":"Flushed"})).await;
            assert_eq!(read_json(&mut ws).await, json!({"type":"Close"}));
            closing_tx.send(()).unwrap();
            release_rx.await.unwrap();
            assert!(matches!(
                ws.next().await.unwrap().unwrap(),
                Message::Close(_)
            ));
            ws.flush().await.unwrap();
        },
    )
    .await;
    let mut stream = client.synthesize(request()).unwrap();
    closing_rx.await.unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().pcm, [1, 2]);
    assert!(
        tokio::time::timeout(Duration::from_millis(10), stream.next())
            .await
            .is_err()
    );
    assert_eq!(client.active_sessions(), 1);
    release_tx.send(()).unwrap();
    assert!(stream.next().await.is_none());
    assert_eq!(client.active_sessions(), 0);
    task.await.unwrap();
}

#[tokio::test]
async fn cancellation_bounds_large_write_to_peer_that_stops_reading() {
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let (client, task) = server(
        |config| {
            config.limits.max_sessions = 1;
            config.limits.queue_capacity = 1;
            config.limits.max_text_bytes = 1024 * 1024;
            config.limits.max_message_bytes = 8 * 1024 * 1024;
            config.limits.write_timeout = Duration::from_secs(5);
            config.limits.close_timeout = Duration::from_millis(40);
        },
        move |ws, _| async move {
            // Leave the upgraded TCP peer unread while the escaped Speak payload
            // exceeds ordinary socket buffers. Cleanup must not await peer progress.
            ready_tx.send(()).unwrap();
            release_rx.await.unwrap();
            drop(ws);
        },
    )
    .await;
    let stream = client
        .synthesize(DeepgramSynthesisRequest::new("\0".repeat(1024 * 1024)))
        .unwrap();
    ready_rx.await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    drop(stream);
    tokio::time::timeout(Duration::from_millis(300), client.cleanup())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(client.active_sessions(), 0);
    release_tx.send(()).unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn cancelling_completed_actor_with_unread_pcm_is_not_success() {
    for shutdown in [false, true] {
        let (client, task) = server(
            |_| {},
            |mut ws, _| async move {
                start(&mut ws).await;
                binary(&mut ws, &[1, 2]).await;
                text(&mut ws, json!({"type":"Flushed"})).await;
                close(&mut ws, false).await;
            },
        )
        .await;
        let mut stream = client.synthesize(request()).unwrap();
        task.await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), async {
            while client.active_sessions() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        // The actor has completed and been joined/reaped, but its PCM remains
        // unread. Explicitly dropping that output must produce cancellation.
        if shutdown {
            assert!(matches!(
                stream.shutdown().await,
                Err(SpeechError::Cancelled)
            ));
            assert!(stream.shutdown().await.is_ok());
        } else {
            stream.cancel();
            assert!(matches!(
                stream.next().await,
                Some(Err(SpeechError::Cancelled))
            ));
        }
        assert!(stream.next().await.is_none());
        client.cleanup().await.unwrap();
    }
}
