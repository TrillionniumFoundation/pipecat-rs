use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_deepgram_flux::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
};
use tokio_tungstenite::{accept_async, accept_hdr_async, tungstenite::Message, WebSocketStream};
type Socket = WebSocketStream<TcpStream>;
async fn bind() -> (TcpListener, FluxConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = FluxConfig::new("secret-test-key");
    config.endpoint = format!("ws://{}/v2/listen", listener.local_addr().unwrap());
    config.limits.connect_timeout = Duration::from_secs(2);
    config.ready_timeout = Duration::from_millis(150);
    config.limits.response_timeout = Duration::from_millis(200);
    config.limits.close_timeout = Duration::from_millis(100);
    (listener, config)
}
async fn send(socket: &mut Socket, value: Value) {
    socket
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
async fn ready(listener: TcpListener) -> Socket {
    let mut socket = accept_async(listener.accept().await.unwrap().0)
        .await
        .unwrap();
    send(
        &mut socket,
        json!({"type":"Connected","request_id":"fixture"}),
    )
    .await;
    socket
}
async fn msg(socket: &mut Socket) -> Message {
    tokio::time::timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
}
fn pcm() -> AudioRawFrame {
    AudioRawFrame::new(vec![1u8; 320], 16000, 1).unwrap()
}
fn turn(event: &str, index: u64, text: &str) -> Value {
    json!({"type":"TurnInfo","event":event,"turn_index":index,"transcript":text,"words":[{"word":text,"confidence":0.9}]})
}
async fn event(session: &mut FluxSession) -> Result<FluxEvent, StreamingError> {
    tokio::time::timeout(Duration::from_secs(3), session.next())
        .await
        .unwrap()
        .unwrap()
}

#[test]
fn settings_are_bounded_atomic_and_redacted() {
    let mut config = FluxConfig::new("secret-test-key");
    config.eager_end_of_turn = true;
    config.settings.model = FluxModel::Multilingual;
    config.settings.keyterm = vec!["a & b".into(), "hello".into()];
    config.tags = vec!["tag/?".into()];
    config.settings.language_hints =
        Some(vec![LanguageHint::Es, LanguageHint::En, LanguageHint::Es]);
    let client = FluxClient::new(config).unwrap();
    let url = url::Url::parse(&client.config().url().unwrap()).unwrap();
    let pairs: Vec<_> = url.query_pairs().collect();
    assert_eq!(
        pairs.iter().filter(|(k, _)| k == "language_hint").count(),
        2
    );
    assert!(pairs
        .iter()
        .any(|(k, v)| k == "eager_eot_threshold" && v == "0.5"));
    assert!(pairs.iter().any(|(k, v)| k == "keyterm" && v == "a & b"));
    assert!(!format!("{client:?}").contains("secret-test-key"));
    let old = client.config().settings.clone();
    assert!(old
        .merge(
            json!({"keyterm":["ok"],"min_confidence":2.0})
                .as_object()
                .unwrap()
        )
        .is_err());
    assert_eq!(old, client.config().settings);
    for endpoint in [
        "ws://example.com/v2/listen",
        "wss://user:pass@example.com/v2/listen",
        "wss://example.com/v2/listen?token=secret",
        "https://example.com/v2/listen",
    ] {
        let mut c = FluxConfig::new("secret");
        c.endpoint = endpoint.into();
        assert!(FluxClient::new(c).is_err());
    }
    for value in [f64::NAN, f64::INFINITY, -0.01, 1.01] {
        let s = FluxSettings {
            eot_threshold: Some(value),
            ..Default::default()
        };
        assert!(s.validate().is_err());
    }
    assert!(old
        .merge(json!({"unknown":true}).as_object().unwrap())
        .is_err());
    assert_eq!(
        old.merge(json!({"min_confidence":null}).as_object().unwrap())
            .unwrap()
            .min_confidence,
        None
    );
}
#[tokio::test]
// Tungstenite fixes the callback error type to its HTTP ErrorResponse.
#[allow(clippy::result_large_err)]
async fn authentication_readiness_audio_and_close_stream_are_real_wire() {
    let (listener, config) = bind().await;
    let (gate, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let socket = listener.accept().await.unwrap().0;
        let mut ws = accept_hdr_async(
            socket,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert_eq!(request.headers()["authorization"], "Token secret-test-key");
                assert!(request.uri().to_string().starts_with(
                    "/v2/listen?model=flux-general-en&sample_rate=16000&encoding=linear16"
                ));
                Ok(response)
            },
        )
        .await
        .unwrap();
        wait.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(30), ws.next())
            .await
            .is_err());
        send(&mut ws, json!({"type":"Connected"})).await;
        assert_eq!(msg(&mut ws).await, Message::Binary(vec![1u8; 320].into()));
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap(),
            json!({"type":"CloseStream"})
        );
        ws.close(None).await.unwrap();
    });
    let client = FluxClient::new(config).unwrap();
    let connect = tokio::spawn(async move { client.connect().await });
    gate.send(()).unwrap();
    let mut session = connect.await.unwrap().unwrap();
    let sender = session.sender();
    sender.send_audio(pcm()).await.unwrap();
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::AudioSent { .. }
    ));
    sender.finish().await.unwrap();
    assert!(matches!(
        sender.try_send_audio(pcm()),
        Err(StreamingError::Closed)
    ));
    assert!(tokio::time::timeout(Duration::from_secs(2), session.next())
        .await
        .unwrap()
        .is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn readiness_timeout_and_cancel_are_bounded() {
    let (listener, config) = bind().await;
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        while ws.next().await.is_some() {}
    });
    assert!(matches!(
        FluxClient::new(config).unwrap().connect().await,
        Err(StreamingError::Timeout("Flux Connected"))
    ));
    server.await.unwrap();
    let (listener, mut config) = bind().await;
    config.ready_timeout = Duration::from_secs(60);
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        while ws.next().await.is_some() {}
    });
    let cancel = CancellationToken::new();
    let other = cancel.clone();
    let client = FluxClient::new(config).unwrap();
    let task = tokio::spawn(async move { client.connect_with_cancel(other).await });
    tokio::time::sleep(Duration::from_millis(20)).await;
    cancel.cancel();
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap(),
        Err(StreamingError::Cancelled)
    ));
    server.await.unwrap();
}
#[tokio::test]
async fn turn_protocol_keeps_updates_predictions_and_confidence_separate() {
    let (listener, config) = bind().await;
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        for (kind, text) in [
            ("StartOfTurn", "hello"),
            ("Update", "hello w"),
            ("EagerEndOfTurn", "hello world"),
            ("TurnResumed", "hello world"),
            ("EndOfTurn", "Hello world!"),
        ] {
            send(&mut ws, turn(kind, 0, text)).await;
        }
        send(&mut ws, turn("EndOfTurn", 0, "duplicate")).await;
        send(&mut ws, turn("StartOfTurn", 1, "second")).await;
        while ws.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    for expected in [
        TurnEvent::StartOfTurn,
        TurnEvent::Update,
        TurnEvent::EagerEndOfTurn,
        TurnEvent::TurnResumed,
        TurnEvent::EndOfTurn,
        TurnEvent::StartOfTurn,
    ] {
        let FluxEvent::Turn { turn, model, .. } = event(&mut session).await.unwrap() else {
            panic!()
        };
        assert_eq!(turn.event, expected);
        assert_eq!(turn.language(model).as_deref(), Some("en"));
        assert!(turn.accepts(Some(0.89)));
        assert!(!turn.accepts(Some(0.9)));
        assert!(turn.result.is_object());
    }
    session.cancel();
    assert_eq!(
        event(&mut session).await.unwrap_err(),
        StreamingError::Cancelled
    );
    assert!(session.next().await.is_none());
    server.await.unwrap();
}
#[tokio::test]
async fn malformed_binary_oversized_and_out_of_order_are_terminal() {
    for message in [Message::Text("not json".into()),Message::Binary(vec![0,0].into()),Message::Text(turn("EndOfTurn",0,"orphan").to_string().into()),Message::Text(json!({"type":"TurnInfo","event":"StartOfTurn","turn_index":0,"words":[{"confidence":1.1}]}).to_string().into()),Message::Text("x".repeat(2048).into())]{let (listener,mut config)=bind().await;config.limits.max_message_bytes=1024;let server=tokio::spawn(async move{let mut ws=ready(listener).await;ws.send(message).await.unwrap();while ws.next().await.is_some(){}});let mut session=FluxClient::new(config).unwrap().connect().await.unwrap();assert!(event(&mut session).await.is_err());assert!(session.next().await.is_none());server.await.unwrap();}
}
#[tokio::test]
async fn configure_serializes_and_coalesces_latest_values() {
    let (listener, mut config) = bind().await;
    config.settings.model = FluxModel::Multilingual;
    let (updates, mut receive) = mpsc::channel(4);
    let (ack, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        let first = serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap();
        updates.send(first).await.unwrap();
        wait.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(30), ws.next())
            .await
            .is_err());
        send(&mut ws, json!({"type":"ConfigureSuccess"})).await;
        let second = serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap();
        updates.send(second).await.unwrap();
        send(
            &mut ws,
            json!({"type":"ConfigureFailure","description":"do not leak"}),
        )
        .await;
        while ws.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    sender
        .merge_settings(
            json!({"eot_threshold":0.8,"keyterm":["first"]})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    let first = receive.recv().await.unwrap();
    assert_eq!(
        first,
        json!({"type":"Configure","thresholds":{"eot_threshold":0.8},"keyterms":["first"]})
    );
    sender
        .merge_settings(
            json!({"eot_threshold":0.7,"language_hints":["es"]})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    sender
        .merge_settings(
            json!({"eot_threshold":0.9,"language_hints":[],"profanity_filter":true})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    ack.send(()).unwrap();
    assert_eq!(event(&mut session).await.unwrap(), FluxEvent::Configured);
    let second = receive.recv().await.unwrap();
    assert_eq!(
        second,
        json!({"type":"Configure","thresholds":{"eot_threshold":0.9},"language_hints":[],"profanity_filter":true})
    );
    assert_eq!(
        event(&mut session).await.unwrap(),
        FluxEvent::ConfigureRejected
    );
    session.cancel();
    server.await.unwrap();
}
#[tokio::test]
async fn watchdog_sends_real_bounded_silence_only_for_active_audio() {
    let (listener, mut config) = bind().await;
    config.watchdog_min_timeout = Duration::from_millis(20);
    config.limits.max_message_bytes = 512;
    let (silence, got) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        send(&mut ws, turn("StartOfTurn", 0, "")).await;
        assert!(tokio::time::timeout(Duration::from_millis(120), ws.next())
            .await
            .is_err());
        let data = msg(&mut ws).await;
        assert_eq!(data.into_data().len(), 320);
        let mut bytes = 0;
        while bytes < 16000 {
            let message = msg(&mut ws).await;
            let bytes_in = message.into_data();
            assert!(bytes_in.len() <= 512);
            assert!(bytes_in.iter().all(|v| *v == 0));
            bytes += bytes_in.len();
        }
        silence.send(bytes).unwrap();
        while ws.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    event(&mut session).await.unwrap();
    tokio::time::sleep(Duration::from_millis(150)).await;
    session.sender().send_audio(pcm()).await.unwrap();
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::AudioSent { .. }
    ));
    assert_eq!(
        event(&mut session).await.unwrap(),
        FluxEvent::WatchdogAudio { seconds: 0.5 }
    );
    assert_eq!(got.await.unwrap(), 16000);
    session.cancel();
    server.await.unwrap();
}
#[tokio::test]
async fn bounded_output_backpressure_cancellation_and_drop_close_socket() {
    for cancel in [true, false] {
        let (listener, mut config) = bind().await;
        config.limits.queue_capacity = 1;
        config.limits.write_timeout = Duration::from_millis(100);
        let server = tokio::spawn(async move {
            let mut ws = ready(listener).await;
            send(&mut ws, turn("StartOfTurn", 0, "")).await;
            for _ in 0..20 {
                send(&mut ws, turn("Update", 0, "held")).await;
            }
            while let Some(Ok(message)) = ws.next().await {
                if message.is_close() {
                    return;
                }
            }
        });
        let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        if cancel {
            session.cancel();
            assert_eq!(
                event(&mut session).await.unwrap_err(),
                StreamingError::Cancelled
            );
        } else {
            drop(session);
        }
        tokio::time::timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn input_queue_full_does_not_mutate_settings_or_accept_partial_pcm() {
    let (listener, mut config) = bind().await;
    config.limits.queue_capacity = 1;
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        while ws.next().await.is_some() {}
    });
    let session = FluxClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    let original = sender.settings().unwrap();
    let invalid = AudioRawFrame {
        audio: vec![0],
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    };
    assert_eq!(
        sender.try_send_audio(invalid),
        Err(StreamingError::PartialSample)
    );
    sender.try_send_audio(pcm()).unwrap();
    let mut updated = original.clone();
    updated.eot_threshold = Some(0.8);
    assert_eq!(
        sender.update_settings(updated),
        Err(StreamingError::Backpressure)
    );
    assert_eq!(sender.settings().unwrap(), original);
    session.cancel();
    drop(session);
    server.await.unwrap();
}

#[tokio::test]
// Tungstenite fixes the callback error type to its HTTP ErrorResponse.
#[allow(clippy::result_large_err)]
async fn connection_setting_waits_for_final_turn_and_reopens_without_audio_replay() {
    let (listener, config) = bind().await;
    let (changed, change) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        send(&mut ws, json!({"type":"Connected"})).await;
        assert_eq!(msg(&mut ws).await.into_data().len(), 320);
        send(&mut ws, turn("StartOfTurn", 0, "hello")).await;
        change.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
        send(&mut ws, turn("EndOfTurn", 0, "hello")).await;
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap(),
            json!({"type":"CloseStream"})
        );
        ws.close(None).await.unwrap();
        let mut second = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert!(request
                    .uri()
                    .to_string()
                    .contains("model=flux-general-multi"));
                assert!(request.uri().to_string().contains("numerals=true"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        send(&mut second, json!({"type":"Connected"})).await;
        assert!(
            tokio::time::timeout(Duration::from_millis(60), second.next())
                .await
                .is_err()
        );
        send(&mut second, turn("StartOfTurn", 0, "hola")).await;
        while second.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    session.sender().send_audio(pcm()).await.unwrap();
    event(&mut session).await.unwrap();
    event(&mut session).await.unwrap();
    session
        .sender()
        .merge_settings(
            json!({"model":"flux-general-multi","numerals":true})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    changed.send(()).unwrap();
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::Turn {
            turn: FluxTurn {
                event: TurnEvent::EndOfTurn,
                ..
            },
            ..
        }
    ));
    assert_eq!(
        event(&mut session).await.unwrap(),
        FluxEvent::Reset {
            interrupted_turn: false
        }
    );
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::Turn {
            model: FluxModel::Multilingual,
            ..
        }
    ));
    session.cancel();
    server.await.unwrap();
}
#[tokio::test]
// Tungstenite fixes the callback error type to its HTTP ErrorResponse.
#[allow(clippy::result_large_err)]
async fn missing_configure_ack_rotates_connection_and_bounds_stale_ack_ambiguity() {
    let (listener, mut config) = bind().await;
    config.configure_ack_timeout = Duration::from_millis(20);
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        send(&mut ws, json!({"type":"Connected"})).await;
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap()["type"],
            "Configure"
        );
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap()["type"],
            "CloseStream"
        );
        ws.close(None).await.unwrap();
        let mut second = accept_hdr_async(
            listener.accept().await.unwrap().0,
            |request: &tokio_tungstenite::tungstenite::handshake::server::Request, response| {
                assert!(request.uri().to_string().contains("eot_threshold=0.8"));
                Ok(response)
            },
        )
        .await
        .unwrap();
        send(&mut second, json!({"type":"Connected"})).await;
        send(&mut second, turn("StartOfTurn", 0, "fresh")).await;
        while second.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    session
        .sender()
        .merge_settings(json!({"eot_threshold":0.8}).as_object().unwrap())
        .unwrap();
    assert_eq!(
        event(&mut session).await.unwrap(),
        FluxEvent::Reset {
            interrupted_turn: false
        }
    );
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::Turn { .. }
    ));
    session.cancel();
    server.await.unwrap();
}
#[tokio::test]
async fn local_confidence_update_does_not_configure_and_drain_timeout_is_reported() {
    let (listener, config) = bind().await;
    let (server_ready, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        wait.await.unwrap();
        assert!(tokio::time::timeout(Duration::from_millis(30), ws.next())
            .await
            .is_err());
        send(&mut ws, turn("StartOfTurn", 0, "")).await;
        send(&mut ws,json!({"type":"TurnInfo","event":"EndOfTurn","turn_index":0,"transcript":"no confidence"})).await;
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap()["type"],
            "CloseStream"
        );
        while ws.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    session
        .sender()
        .merge_settings(json!({"min_confidence":0.8}).as_object().unwrap())
        .unwrap();
    server_ready.send(()).unwrap();
    event(&mut session).await.unwrap();
    let FluxEvent::Turn {
        turn,
        min_confidence,
        ..
    } = event(&mut session).await.unwrap()
    else {
        panic!()
    };
    assert_eq!(min_confidence, Some(0.8));
    assert!(!turn.accepts(min_confidence));
    assert!(turn.accepts(Some(0.0)));
    session.sender().finish().await.unwrap();
    assert_eq!(
        event(&mut session).await.unwrap_err(),
        StreamingError::Timeout("Flux drain")
    );
    server.await.unwrap();
}

#[tokio::test]
async fn stray_ack_does_not_invent_acceptance_or_clear_a_future_request() {
    let (listener, config) = bind().await;
    let (acked, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        send(&mut ws, json!({"type":"ConfigureSuccess"})).await;
        send(&mut ws, json!({"type":"ConfigureFailure"})).await;
        send(&mut ws, turn("StartOfTurn", 0, "")).await;
        wait.await.unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap()["type"],
            "Configure"
        );
        send(&mut ws, json!({"type":"ConfigureSuccess"})).await;
        while ws.next().await.is_some() {}
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::Turn { .. }
    ));
    session
        .sender()
        .merge_settings(json!({"eot_threshold":0.8}).as_object().unwrap())
        .unwrap();
    acked.send(()).unwrap();
    assert_eq!(event(&mut session).await.unwrap(), FluxEvent::Configured);
    session.cancel();
    server.await.unwrap();
}
#[test]
fn oversized_or_deep_delta_is_rejected_before_cloning() {
    let settings = FluxSettings::default();
    assert_eq!(
        settings.merge(
            json!({"keyterm":["a".repeat(1024*1024)]})
                .as_object()
                .unwrap()
        ),
        Err(StreamingError::TooLarge)
    );
    let mut deep = json!(true);
    for _ in 0..20 {
        deep = json!([deep]);
    }
    assert_eq!(
        settings.merge(json!({"keyterm":deep}).as_object().unwrap()),
        Err(StreamingError::TooLarge)
    );
}
