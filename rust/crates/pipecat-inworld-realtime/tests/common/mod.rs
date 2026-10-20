#![allow(dead_code)]
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat_inworld_realtime::*;
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};

pub type Ws = WebSocketStream<TcpStream>;
pub const WAIT: Duration = Duration::from_secs(5);
pub async fn bounded<F: Future>(future: F) -> F::Output {
    timeout(WAIT, future)
        .await
        .expect("loopback operation exceeded its deadline")
}
pub async fn wire(ws: &mut Ws, mut event: Value) {
    static EVENT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    event["event_id"] = json!(format!(
        "inworld-test-{}",
        EVENT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ));
    bounded(ws.send(Message::Text(event.to_string().into())))
        .await
        .unwrap();
}
pub async fn read(ws: &mut Ws) -> Value {
    let message = bounded(ws.next())
        .await
        .expect("peer closed")
        .expect("peer read failed");
    assert!(
        message.is_text(),
        "expected JSON command, received {message:?}"
    );
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
pub async fn quiet(ws: &mut Ws) {
    assert!(
        timeout(Duration::from_millis(60), ws.next()).await.is_err(),
        "unexpected provider command or closure"
    );
}
pub async fn closed(ws: &mut Ws) {
    loop {
        match bounded(ws.next()).await {
            Some(Ok(Message::Close(_))) | None => {
                let _ = ws.close(None).await;
                return;
            }
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
            other => panic!("unexpected command while closing: {other:?}"),
        }
    }
}
/// Initiate a clean peer close but keep the TCP owner alive until the client's
/// close reply arrives. Dropping immediately after a close write is not a
/// completed WebSocket close handshake and can legitimately report Transport.
pub async fn peer_close(ws: &mut Ws) {
    bounded(ws.close(None)).await.unwrap();
    loop {
        match bounded(ws.next()).await {
            Some(Ok(Message::Close(_))) | None => return,
            Some(Ok(Message::Ping(_))) | Some(Ok(Message::Pong(_))) => {}
            Some(Err(
                tokio_tungstenite::tungstenite::Error::ConnectionClosed
                | tokio_tungstenite::tungstenite::Error::AlreadyClosed,
            )) => return,
            other => panic!("unexpected peer-initiated close result: {other:?}"),
        }
    }
}
pub async fn handshake(ws: &mut Ws) -> Value {
    wire(
        ws,
        json!({"type":"session.created","session":{"id":"synthetic-session"}}),
    )
    .await;
    let update = read(ws).await;
    assert_eq!(update["type"], "session.update");
    assert_eq!(update["session"]["type"], "realtime");
    assert_eq!(
        update["session"]["providerData"]["auto_tool_response"],
        false
    );
    assert!(update["session"].get("voice").is_none());
    assert!(update["session"].get("turn_detection").is_none());
    // Readiness is acknowledgment of the exact supplied configuration, not an
    // unrelated session.created or a Grok conversation.created event.
    wire(
        ws,
        json!({"type":"session.updated","session":update["session"]}),
    )
    .await;
    update
}
pub async fn peer<F, Fut>(handler: F) -> (InworldConfig, JoinHandle<()>)
where
    F: FnOnce(Ws) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = InworldConfig::new("synthetic-inworld-credential");
    config.endpoint = format!(
        "ws://{}/api/v1/realtime/session",
        listener.local_addr().unwrap()
    );
    config.limits.connect_timeout = WAIT;
    config.limits.operation_timeout = WAIT;
    config.limits.close_timeout = Duration::from_millis(250);
    let task = tokio::spawn(async move {
        let (stream, _) = bounded(listener.accept()).await.unwrap();
        let ws = bounded(tokio_tungstenite::accept_async(stream))
            .await
            .unwrap();
        handler(ws).await;
    });
    (config, task)
}
pub async fn next(session: &mut InworldSession) -> InworldEvent {
    bounded(session.next_event())
        .await
        .expect("session closed before event")
        .expect("session failed")
}
pub async fn ready(config: InworldConfig) -> InworldSession {
    let mut session = bounded(InworldClient::new(config).unwrap().connect())
        .await
        .unwrap();
    loop {
        let event = next(&mut session).await;
        if matches!(event.event, Event::SessionUpdated { .. }) {
            break;
        }
    }
    assert!(session.sender().is_ready());
    session
}
pub async fn response(ws: &mut Ws, id: &str, done: bool) {
    wire(ws, json!({"type":if done {"response.done"} else {"response.created"},"response":{"id":id,"status":if done {"completed"} else {"in_progress"},"output":[]}})).await;
}
pub fn audio_delta(response: &str, item: &str, bytes: &[u8]) -> Value {
    json!({"type":"response.output_audio.delta","response_id":response,"item_id":item,"output_index":0,"content_index":0,"delta":STANDARD.encode(bytes)})
}
pub async fn call(ws: &mut Ws, id: &str, name: &str, arguments: &str, omit_name: bool) {
    wire(ws,json!({"type":"conversation.item.added","item":{"type":"function_call","id":format!("item-{id}"),"call_id":id,"name":name,"arguments":""}})).await;
    let mut done =
        json!({"type":"response.function_call_arguments.done","call_id":id,"arguments":arguments});
    if !omit_name {
        done["name"] = json!(name);
    }
    wire(ws, done).await;
}
pub async fn admitted_call(session: &mut InworldSession) {
    loop {
        if matches!(
            next(session).await.event,
            Event::FunctionArgumentsDone { .. }
        ) {
            return;
        }
    }
}
pub fn pcm_frame(bytes: Vec<u8>) -> pipecat_core::Frame {
    pipecat_core::Frame::new(pipecat_core::FrameData::InputAudio(
        pipecat_core::AudioRawFrame {
            audio: bytes,
            sample_rate: 24_000,
            num_channels: 1,
            context_id: None,
        },
    ))
}
