use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_deepgram_flux::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
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
fn turn(event: &str, index: u64, text: &str) -> Value {
    json!({"type":"TurnInfo","event":event,"turn_index":index,"transcript":text,"words":[{"word":text,"confidence":0.9}]})
}
async fn event(session: &mut FluxSession) -> Result<FluxEvent, StreamingError> {
    tokio::time::timeout(Duration::from_secs(3), session.next())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test]
async fn review_provider_fatal_before_and_after_connected_is_redacted_and_terminal() {
    for confirmed in [false, true] {
        let (listener, config) = bind().await;
        let server = tokio::spawn(async move {
            let mut ws = accept_async(listener.accept().await.unwrap().0)
                .await
                .unwrap();
            if confirmed {
                send(&mut ws, json!({"type":"Connected"})).await;
            }
            send(&mut ws,json!({"type":"Error","code":"UNPARSABLE_CLIENT_MESSAGE","description":"secret-provider-description"})).await;
            while let Some(Ok(message)) = ws.next().await {
                if message.is_close() {
                    break;
                }
            }
        });
        let result = FluxClient::new(config).unwrap().connect().await;
        let error = if confirmed {
            let mut session = result.unwrap();
            let error = event(&mut session).await.unwrap_err();
            assert!(session.next().await.is_none());
            error
        } else {
            match result {
                Err(error) => error,
                Ok(_) => panic!("fatal before readiness cannot return a session"),
            }
        };
        assert!(matches!(error, StreamingError::Invalid(_)));
        assert!(!error.to_string().contains("secret-provider-description"));
        tokio::time::timeout(Duration::from_secs(1), server)
            .await
            .unwrap()
            .unwrap();
    }
}
#[tokio::test]
async fn review_dropping_connect_future_owns_unready_socket() {
    let (listener, mut config) = bind().await;
    config.ready_timeout = Duration::from_secs(60);
    let (ready, received) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap();
        ready.send(()).unwrap();
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let client = FluxClient::new(config).unwrap();
    let connecting = tokio::spawn(async move { client.connect().await });
    received.await.unwrap();
    connecting.abort();
    let _ = connecting.await;
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn review_invalid_audio_and_settings_are_rejected_before_wire_or_snapshot_mutation() {
    let (listener, mut config) = bind().await;
    config.limits.max_message_bytes = 512;
    let (checked, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        wait.await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(60), ws.next())
                .await
                .is_err(),
            "invalid inputs must not reach network"
        );
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    let sender = session.sender();
    for audio in [
        AudioRawFrame::new(vec![0u8; 320], 8000, 1).unwrap(),
        AudioRawFrame::new(vec![0u8; 320], 16000, 2).unwrap(),
        AudioRawFrame::new(vec![0u8; 514], 16000, 1).unwrap(),
    ] {
        assert!(sender.try_send_audio(audio).is_err());
    }
    let settings = sender.settings().unwrap();
    assert!(sender
        .merge_settings(
            json!({"keyterm":["not committed"],"min_confidence":9})
                .as_object()
                .unwrap()
        )
        .is_err());
    assert_eq!(sender.settings().unwrap(), settings);
    checked.send(()).unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    session.cancel();
    assert_eq!(
        event(&mut session).await.unwrap_err(),
        StreamingError::Cancelled
    );
    server.await.unwrap();
}
#[tokio::test]
async fn review_stray_configure_ack_does_not_ack_a_future_update() {
    let (listener, config) = bind().await;
    let (stray, seen) = oneshot::channel();
    let (release, wait) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut ws = ready(listener).await;
        send(&mut ws, json!({"type":"ConfigureSuccess"})).await;
        send(&mut ws, turn("StartOfTurn", 0, "")).await;
        stray.send(()).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(msg(&mut ws).await.to_text().unwrap()).unwrap()["type"],
            "Configure"
        );
        wait.await.unwrap();
        send(&mut ws, json!({"type":"ConfigureSuccess"})).await;
        while let Some(Ok(message)) = ws.next().await {
            if message.is_close() {
                break;
            }
        }
    });
    let mut session = FluxClient::new(config).unwrap().connect().await.unwrap();
    seen.await.unwrap();
    assert!(matches!(
        event(&mut session).await.unwrap(),
        FluxEvent::Turn { .. }
    ));
    session
        .sender()
        .merge_settings(json!({"eot_threshold":0.8}).as_object().unwrap())
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), session.next())
            .await
            .is_err()
    );
    release.send(()).unwrap();
    assert_eq!(event(&mut session).await.unwrap(), FluxEvent::Configured);
    session.cancel();
    server.await.unwrap();
}
