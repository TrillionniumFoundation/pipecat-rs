// The server handshake callback requires tungstenite's unboxed HTTP error type.
#![allow(clippy::result_large_err)]

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_streaming_speech::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{
    accept_async, accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
    WebSocketStream,
};

async fn serve_on_loopback() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", listener.local_addr().unwrap());
    (listener, url)
}
fn deepgram(url: String) -> DeepgramClient {
    let mut config = DeepgramConfig::new("test-secret");
    config.endpoint = url;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_millis(300);
    DeepgramClient::new(config).unwrap()
}
fn cartesia(url: String) -> CartesiaClient {
    let mut config = CartesiaConfig::new("test-secret", "voice-id");
    config.endpoint = url;
    config.limits.close_timeout = Duration::from_millis(100);
    config.limits.response_timeout = Duration::from_millis(300);
    CartesiaClient::new(config).unwrap()
}
fn transcript(text: &str, is_final: bool) -> Value {
    json!({"type":"Results","is_final":is_final,"speech_final":is_final,
        "channel":{"alternatives":[{"transcript":text,"languages":["en"],"words":[{"word":text,"speaker":0}]}]}})
}
async fn send(socket: &mut WebSocketStream<TcpStream>, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn receive(socket: &mut WebSocketStream<TcpStream>) -> Value {
    match tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
    {
        Message::Text(text) => serde_json::from_str(&text).unwrap(),
        other => panic!("expected JSON, got {other:?}"),
    }
}

#[tokio::test]
async fn deepgram_auth_repeated_options_pcm_finalize_and_close_stream() {
    let (listener, url) = serve_on_loopback().await;
    let mut config = DeepgramConfig::new("test-secret");
    config.endpoint = format!("{url}v1/listen");
    config.endpointing = Some(Endpointing::Disabled);
    config.extra_query = vec![
        ("keyterm".into(), "Rust & C++".into()),
        ("keyterm".into(), "音声".into()),
    ];
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(stream, |request: &Request, response: Response| {
            assert_eq!(request.headers()["authorization"], "Token test-secret");
            let url = url::Url::parse(&format!("ws://localhost{}", request.uri())).unwrap();
            assert_eq!(url.path(), "/v1/listen");
            let query: Vec<_> = url.query_pairs().collect();
            let values: Vec<_> = query
                .iter()
                .filter(|(k, _)| k == "keyterm")
                .map(|(_, v)| v.as_ref())
                .collect();
            assert_eq!(values, ["Rust & C++", "音声"]);
            assert!(query
                .iter()
                .any(|(k, v)| k == "encoding" && v == "linear16"));
            assert!(query
                .iter()
                .any(|(k, v)| k == "endpointing" && v == "false"));
            Ok(response)
        })
        .await
        .unwrap();
        assert_eq!(
            socket.next().await.unwrap().unwrap(),
            Message::Binary(vec![1, 2, 3, 4].into())
        );
        send(&mut socket, transcript("hel", false)).await;
        send(&mut socket, transcript("hello", true)).await;
        assert_eq!(receive(&mut socket).await, json!({"type":"Finalize"}));
        let mut final_response = transcript("", true);
        final_response["from_finalize"] = true.into();
        send(&mut socket, final_response).await;
        assert_eq!(receive(&mut socket).await, json!({"type":"CloseStream"}));
        send(
            &mut socket,
            json!({"type":"Metadata","request_id":"example"}),
        )
        .await;
        socket.close(None).await.unwrap();
    });
    let mut session = DeepgramClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    session
        .sender()
        .send_audio(AudioRawFrame::new(vec![1, 2, 3, 4], 16000, 1).unwrap())
        .await
        .unwrap();
    assert!(
        matches!(session.next().await.unwrap().unwrap(), DeepgramEvent::Transcript(t) if !t.is_final && t.text == "hel")
    );
    assert!(
        matches!(session.next().await.unwrap().unwrap(), DeepgramEvent::Transcript(t) if t.is_final && t.speech_final && t.language.as_deref() == Some("en") && t.result.pointer("/channel/alternatives/0/words/0/speaker") == Some(&json!(0)))
    );
    session.sender().finalize().await.unwrap();
    assert_eq!(
        session.next().await.unwrap().unwrap(),
        DeepgramEvent::Finalized
    );
    session.sender().finish().await.unwrap();
    assert_eq!(
        session.sender().finalize().await.unwrap_err(),
        StreamingError::Closed
    );
    assert!(matches!(
        session.next().await.unwrap().unwrap(),
        DeepgramEvent::Metadata(_)
    ));
    assert!(session.next().await.is_none());
    assert!(session.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn deepgram_keepalive_and_cancel_discard_buffered_transcripts() {
    let (listener, url) = serve_on_loopback().await;
    let (sent, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(receive(&mut socket).await, json!({"type":"KeepAlive"}));
        for _ in 0..4 {
            send(&mut socket, transcript("stale", false)).await;
        }
        sent.send(()).unwrap();
        loop {
            if matches!(socket.next().await, Some(Ok(Message::Close(_))) | None) {
                break;
            }
        }
    });
    let mut config = DeepgramConfig::new("test-secret");
    config.endpoint = url;
    config.keepalive_interval = Duration::from_millis(20);
    config.limits.queue_capacity = 1;
    let mut session = DeepgramClient::new(config)
        .unwrap()
        .connect()
        .await
        .unwrap();
    ready.await.unwrap();
    session.cancel();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), session.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err(),
        StreamingError::Cancelled
    );
    assert!(session.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn deepgram_finish_deadline_and_audio_validation() {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(receive(&mut socket).await, json!({"type":"CloseStream"}));
        let _ = socket.next().await;
    });
    let mut session = deepgram(url).connect().await.unwrap();
    assert!(matches!(
        session
            .sender()
            .send_audio(AudioRawFrame::new(vec![0, 0], 24000, 1).unwrap())
            .await,
        Err(StreamingError::Invalid(_))
    ));
    session.sender().finish().await.unwrap();
    assert_eq!(
        session.next().await.unwrap().unwrap_err(),
        StreamingError::Timeout("Deepgram final response")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn deepgram_provider_events_and_unexpected_disconnect() {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        send(
            &mut socket,
            json!({"type":"SpeechStarted","timestamp":1.25}),
        )
        .await;
        send(
            &mut socket,
            json!({"type":"UtteranceEnd","last_word_end":2.0}),
        )
        .await;
        send(&mut socket, json!({"type":"FutureOptionalEvent"})).await;
        socket.close(None).await.unwrap();
    });
    let mut session = deepgram(url).connect().await.unwrap();
    assert_eq!(
        session.next().await.unwrap().unwrap(),
        DeepgramEvent::SpeechStarted { timestamp: 1.25 }
    );
    assert_eq!(
        session.next().await.unwrap().unwrap(),
        DeepgramEvent::UtteranceEnd { last_word_end: 2.0 }
    );
    assert_eq!(
        session.next().await.unwrap().unwrap_err(),
        StreamingError::Closed
    );
    server.await.unwrap();
}

#[tokio::test]
async fn cartesia_auth_continuation_audio_timestamps_flush_done_and_end() {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &Request, response: Response| {
                assert_eq!(request.headers()["x-api-key"], "test-secret");
                assert_eq!(request.headers()["cartesia-version"], "2026-03-01");
                assert!(request.uri().query().is_none());
                Ok(response)
            },
        )
        .await
        .unwrap();
        let first = receive(&mut socket).await;
        assert_eq!(first["context_id"], "ctx-1");
        assert_eq!(first["continue"], true);
        assert_eq!(first["transcript"], "Hello ");
        assert_eq!(first["model_id"], "sonic-3.6");
        assert_eq!(first["voice"], json!({"mode":"id","id":"voice-id"}));
        assert_eq!(
            first["output_format"],
            json!({"container":"raw","encoding":"pcm_s16le","sample_rate":24000})
        );
        assert_eq!(first["use_normalized_timestamps"], false);
        assert_eq!(receive(&mut socket).await["transcript"], "world.");
        let last = receive(&mut socket).await;
        assert_eq!(last["continue"], false);
        assert_eq!(last["transcript"], "");
        send(
            &mut socket,
            json!({"type":"chunk","context_id":"old","data":"ignored-invalid-base64"}),
        )
        .await;
        for audio in [vec![1], vec![2, 3, 4]] {
            send(
                &mut socket,
                json!({"type":"chunk","context_id":"ctx-1","data":STANDARD.encode(audio)}),
            )
            .await;
        }
        send(&mut socket,json!({"type":"timestamps","context_id":"ctx-1","word_timestamps":{"words":["<spell>Hello</spell>","world."],"start":[0.0,0.5],"end":[0.4,0.9]}})).await;
        send(
            &mut socket,
            json!({"type":"flush_done","context_id":"ctx-1","flush_id":2}),
        )
        .await;
        send(&mut socket, json!({"type":"done","context_id":"ctx-1"})).await;
        let _ = socket.next().await;
    });
    let mut stream = cartesia(url).stream("ctx-1").unwrap();
    stream.sender().send_text("Hello ").await.unwrap();
    stream.sender().send_text("world.").await.unwrap();
    stream.sender().finish().await.unwrap();
    assert_eq!(
        stream.sender().send_text("too late").await.unwrap_err(),
        StreamingError::Closed
    );
    assert!(
        matches!(stream.next().await.unwrap().unwrap(), CartesiaEvent::Audio(a) if a.audio == vec![1,2,3,4] && a.context_id.as_deref() == Some("ctx-1"))
    );
    assert!(
        matches!(stream.next().await.unwrap().unwrap(), CartesiaEvent::WordTimestamps { words,.. } if words.len() == 2 && words[0].word == "Hello" && words[1].start == 0.5 && words[1].end == Some(0.9))
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        CartesiaEvent::FlushDone {
            context_id: "ctx-1".into(),
            flush_id: Some(2)
        }
    );
    assert_eq!(
        stream.next().await.unwrap().unwrap(),
        CartesiaEvent::Done {
            context_id: "ctx-1".into()
        }
    );
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn cartesia_cancel_bypasses_full_output_and_sends_context_cancel() {
    let (listener, url) = serve_on_loopback().await;
    let (sent, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        receive(&mut socket).await;
        for _ in 0..4 {
            send(
                &mut socket,
                json!({"type":"chunk","context_id":"interrupt","data":"AAA="}),
            )
            .await;
        }
        sent.send(()).unwrap();
        assert_eq!(
            receive(&mut socket).await,
            json!({"context_id":"interrupt","cancel":true})
        );
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    });
    let mut config = CartesiaConfig::new("key", "voice");
    config.endpoint = url;
    config.limits.queue_capacity = 1;
    let mut stream = CartesiaClient::new(config)
        .unwrap()
        .stream("interrupt")
        .unwrap();
    stream.sender().send_text("speak").await.unwrap();
    ready.await.unwrap();
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
async fn cartesia_drop_sends_context_cancel_without_explicit_next() {
    let (listener, url) = serve_on_loopback().await;
    let (sent, ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        receive(&mut socket).await;
        sent.send(()).unwrap();
        assert_eq!(
            receive(&mut socket).await,
            json!({"context_id":"drop","cancel":true})
        );
    });
    let stream = cartesia(url).stream("drop").unwrap();
    stream.sender().send_text("speak").await.unwrap();
    ready.await.unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}

async fn cartesia_error_case(messages: Vec<Value>) -> StreamingError {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        receive(&mut socket).await;
        for message in messages {
            send(&mut socket, message).await;
        }
        let _ = socket.next().await;
    });
    let mut stream = cartesia(url).stream("ctx").unwrap();
    stream.sender().send_text("speak").await.unwrap();
    let error = loop {
        if let Err(error) = stream.next().await.expect("terminal error expected") {
            break error;
        }
    };
    server.await.unwrap();
    error
}
#[tokio::test]
async fn cartesia_malformed_base64_and_timestamps_are_errors() {
    assert_eq!(
        cartesia_error_case(vec![json!({"type":"chunk","context_id":"ctx","data":"!"})]).await,
        StreamingError::Protocol("Cartesia base64 audio")
    );
    assert_eq!(cartesia_error_case(vec![json!({"type":"timestamps","context_id":"ctx","word_timestamps":{"words":["x"],"start":[]}})]).await,StreamingError::Protocol("Cartesia timestamp lengths"));
    assert_eq!(cartesia_error_case(vec![json!({"type":"timestamps","context_id":"ctx","word_timestamps":{"words":["x"],"start":[1],"end":[0]}})]).await,StreamingError::Protocol("Cartesia word end precedes start"));
}
#[tokio::test]
async fn cartesia_truncated_pcm_and_provider_failure_are_errors() {
    assert_eq!(
        cartesia_error_case(vec![
            json!({"type":"chunk","context_id":"ctx","data":"AA=="}),
            json!({"type":"done","context_id":"ctx"})
        ])
        .await,
        StreamingError::PartialSample
    );
    assert_eq!(
        cartesia_error_case(vec![
            json!({"type":"error","status_code":401,"message":"redacted"})
        ])
        .await,
        StreamingError::Provider { status: Some(401) }
    );
}

#[tokio::test]
async fn cartesia_response_deadline_includes_full_output_queue() {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        receive(&mut socket).await;
        for _ in 0..4 {
            send(
                &mut socket,
                json!({"type":"chunk","context_id":"ctx","data":"AAA="}),
            )
            .await;
        }
        assert_eq!(
            receive(&mut socket).await,
            json!({"context_id":"ctx","cancel":true})
        );
    });
    let mut config = CartesiaConfig::new("key", "voice");
    config.endpoint = url;
    config.limits.queue_capacity = 1;
    config.limits.response_timeout = Duration::from_millis(40);
    let mut stream = CartesiaClient::new(config).unwrap().stream("ctx").unwrap();
    stream.sender().send_text("speak").await.unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(matches!(
        stream.next().await,
        Some(Ok(CartesiaEvent::Audio(_)))
    ));
    assert!(matches!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::Timeout(_)
    ));
    server.await.unwrap();
}

#[tokio::test]
async fn cartesia_audio_and_text_context_limits_are_enforced() {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        receive(&mut socket).await;
        send(
            &mut socket,
            json!({"type":"chunk","context_id":"ctx","data":"AAAAAAAA"}),
        )
        .await;
        let _ = socket.next().await;
    });
    let mut config = CartesiaConfig::new("key", "voice");
    config.endpoint = url;
    config.max_audio_bytes = 4;
    let mut stream = CartesiaClient::new(config).unwrap().stream("ctx").unwrap();
    stream.sender().send_text("speak").await.unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::TooLarge
    );
    server.await.unwrap();

    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert_eq!(receive(&mut socket).await["transcript"], "one");
        assert_eq!(
            receive(&mut socket).await,
            json!({"context_id":"ctx","cancel":true})
        );
    });
    let mut config = CartesiaConfig::new("key", "voice");
    config.endpoint = url;
    config.max_text_bytes = 4;
    let mut stream = CartesiaClient::new(config).unwrap().stream("ctx").unwrap();
    stream.sender().send_text("one").await.unwrap();
    stream.sender().send_text("two").await.unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::TooLarge
    );
    server.await.unwrap();
}

#[tokio::test]
async fn handshake_failure_and_timeout_never_include_credentials() {
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let _ = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |_request: &Request, _response: Response| {
                Err(tokio_tungstenite::tungstenite::http::Response::builder()
                    .status(401)
                    .body(Some("sensitive upstream body".into()))
                    .unwrap())
            },
        )
        .await;
    });
    assert!(matches!(
        deepgram(url).connect().await,
        Err(StreamingError::Http(401))
    ));
    server.await.unwrap();
    let (listener, url) = serve_on_loopback().await;
    let server = tokio::spawn(async move {
        let _socket = listener.accept().await.unwrap();
        tokio::time::sleep(Duration::from_millis(100)).await;
    });
    let mut config = DeepgramConfig::new("test-secret");
    config.endpoint = url;
    config.limits.connect_timeout = Duration::from_millis(20);
    assert!(!format!("{config:?}").contains("test-secret"));
    assert!(matches!(
        DeepgramClient::new(config).unwrap().connect().await,
        Err(StreamingError::Timeout("connect"))
    ));
    server.await.unwrap();
}

#[test]
fn invalid_configs_fail_before_any_network_access() {
    for endpoint in [
        "http://localhost/",
        "ws://example.com/",
        "wss://name:key@example.com/",
        "wss://example.com/?api_key=secret",
    ] {
        let mut config = DeepgramConfig::new("key");
        config.endpoint = endpoint.into();
        assert!(DeepgramClient::new(config).is_err());
        let mut config = CartesiaConfig::new("key", "voice");
        config.endpoint = endpoint.into();
        assert!(CartesiaClient::new(config).is_err());
    }
    let mut config = CartesiaConfig::new("secret", "voice");
    config.limits.queue_capacity = 0;
    assert!(CartesiaClient::new(config).is_err());
    let mut config = DeepgramConfig::new("key");
    config
        .extra_query
        .push(("sample_rate".into(), "8000".into()));
    assert!(DeepgramClient::new(config).is_err());
    assert!(!format!("{:?}", CartesiaConfig::new("test-secret", "voice")).contains("test-secret"));
}

#[tokio::test]
async fn upgrade_redirects_are_rejected_without_forwarding_credentials() {
    for use_cartesia in [false, true] {
        let (redirect_target, target_url) = serve_on_loopback().await;
        let (listener, url) = serve_on_loopback().await;
        let server = tokio::spawn(async move {
            let _ = accept_hdr_async(
                listener.accept().await.unwrap().0,
                move |_request: &Request, _response: Response| {
                    Err(tokio_tungstenite::tungstenite::http::Response::builder()
                        .status(307)
                        .header("Location", &target_url)
                        .body(Some(String::new()))
                        .unwrap())
                },
            )
            .await;
        });
        if use_cartesia {
            let mut stream = cartesia(url).stream("redirect").unwrap();
            assert_eq!(
                stream.next().await.unwrap().unwrap_err(),
                StreamingError::Http(307)
            );
        } else {
            assert!(matches!(
                deepgram(url).connect().await,
                Err(StreamingError::Http(307))
            ));
        }
        server.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(50), redirect_target.accept())
                .await
                .is_err()
        );
    }
}

#[tokio::test]
async fn deepgram_rejects_malformed_and_oversized_provider_messages() {
    for (payload, limit, expected) in [
        (
            "{not-json".to_string(),
            1024,
            StreamingError::Protocol("Deepgram JSON"),
        ),
        (
            transcript(&"x".repeat(512), true).to_string(),
            128,
            StreamingError::TooLarge,
        ),
        (
            json!({"type":"Error","status_code":429,"description":"redacted"}).to_string(),
            1024,
            StreamingError::Provider { status: Some(429) },
        ),
    ] {
        let (listener, url) = serve_on_loopback().await;
        let server = tokio::spawn(async move {
            let mut socket = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            socket.send(Message::Text(payload.into())).await.unwrap();
            let _ = socket.next().await;
        });
        let mut config = DeepgramConfig::new("secret");
        config.endpoint = url;
        config.limits.max_message_bytes = limit;
        let mut session = DeepgramClient::new(config)
            .unwrap()
            .connect()
            .await
            .unwrap();
        assert_eq!(session.next().await.unwrap().unwrap_err(), expected);
        server.await.unwrap();
    }
}

#[tokio::test]
async fn cancellation_during_connect_and_precancelled_requests_stop_immediately() {
    let (listener, url) = serve_on_loopback().await;
    let token = CancellationToken::new();
    token.cancel();
    assert!(matches!(
        deepgram(url.clone())
            .connect_with_cancel(token.clone())
            .await,
        Err(StreamingError::Cancelled)
    ));
    let mut stream = cartesia(url).stream_with_cancel("cancel", token).unwrap();
    assert_eq!(
        stream.next().await.unwrap().unwrap_err(),
        StreamingError::Cancelled
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(40), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn cartesia_cancelled_next_is_retryable_after_dropping_its_pending_future() {
    let (listener, url) = serve_on_loopback().await;
    let (ready, wait_ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        receive(&mut socket).await;
        ready.send(()).unwrap();
        assert_eq!(
            receive(&mut socket).await,
            json!({"context_id":"ctx","cancel":true})
        );
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    });
    let mut stream = cartesia(url).stream("ctx").unwrap();
    stream.sender().send_text("speak").await.unwrap();
    wait_ready.await.unwrap();
    stream.cancel();
    // On this current-thread runtime the actor cannot run cleanup until we
    // yield. Poll just far enough to wait on its join, as select() can do.
    let mut next = Box::pin(stream.next());
    assert!(futures_util::poll!(&mut next).is_pending());
    drop(next);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), stream.next())
            .await
            .unwrap(),
        Some(Err(StreamingError::Cancelled))
    );
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    server.await.unwrap();
}

#[tokio::test]
async fn deepgram_cancelled_next_is_retryable_after_dropping_its_pending_future() {
    let (listener, url) = serve_on_loopback().await;
    let (ready, wait_ready) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut socket = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        assert!(matches!(socket.next().await, Some(Ok(Message::Binary(_)))));
        ready.send(()).unwrap();
        assert!(matches!(socket.next().await, Some(Ok(Message::Close(_)))));
    });
    let mut session = deepgram(url).connect().await.unwrap();
    session
        .sender()
        .send_audio(AudioRawFrame::new(vec![0, 0], 16000, 1).unwrap())
        .await
        .unwrap();
    wait_ready.await.unwrap();
    session.cancel();
    let mut next = Box::pin(session.next());
    assert!(futures_util::poll!(&mut next).is_pending());
    drop(next);
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), session.next())
            .await
            .unwrap(),
        Some(Err(StreamingError::Cancelled))
    );
    assert!(session.next().await.is_none());
    assert!(session.next().await.is_none());
    server.await.unwrap();
}
