#![allow(dead_code)]
use futures_util::{SinkExt, StreamExt};
use pipecat_openclaw::*;
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};
pub type Peer = WebSocketStream<TcpStream>;
pub async fn recv(peer: &mut Peer) -> Value {
    let data = tokio::time::timeout(Duration::from_secs(3), peer.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    serde_json::from_str(data.to_text().unwrap()).unwrap()
}
pub async fn send(peer: &mut Peer, v: Value) {
    peer.send(Message::Text(v.to_string().into()))
        .await
        .unwrap();
}
pub async fn response(peer: &mut Peer, request: &Value, payload: Value) {
    send(
        peer,
        json!({"type":"res","id":request["id"],"ok":true,"payload":payload}),
    )
    .await;
}
pub async fn challenge(peer: &mut Peer) -> Value {
    send(peer, json!({"type":"event","event":"connect.challenge"})).await;
    let req = recv(peer).await;
    assert_eq!(req["method"], "connect");
    response(peer, &req, json!({"type":"hello-ok","protocol":4})).await;
    req
}
pub async fn chat(peer: &mut Peer, id: &str, state: &str, text: &str) {
    let mut payload = json!({"runId":id,"state":state});
    if state == "delta" {
        payload["deltaText"] = text.into();
        payload["message"] = json!({"content":[{"text":"cumulative is not a delta"}]});
    } else {
        payload["message"] = json!({"content":[{"type":"text","text":text}]});
    }
    send(
        peer,
        json!({"type":"event","event":"chat","payload":payload}),
    )
    .await;
}
pub async fn server<F, Fut>(script: F) -> (GatewayConfig, tokio::task::JoinHandle<()>)
where
    F: FnOnce(Peer) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let config = GatewayConfig {
        url: format!("ws://{}", listener.local_addr().unwrap()),
        allow_loopback_cleartext: true,
        token: Some("test-secret-token".into()),
        password: Some("test-secret-password".into()),
        limits: GatewayLimits {
            connect_timeout: Duration::from_millis(600),
            write_timeout: Duration::from_millis(200),
            request_timeout: Duration::from_millis(300),
            run_timeout: Duration::from_secs(2),
            close_timeout: Duration::from_millis(200),
            ..Default::default()
        },
        ..Default::default()
    };
    let task = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        script(accept_async(stream).await.unwrap()).await;
    });
    (config, task)
}
pub async fn event(run: &mut GatewayRun) -> RunEvent {
    tokio::time::timeout(Duration::from_secs(3), run.next())
        .await
        .unwrap()
        .unwrap()
}
pub async fn end(run: &mut GatewayRun) -> RunResult {
    loop {
        if let RunEvent::End(e) = event(run).await {
            return e;
        }
    }
}
pub async fn wait_close(peer: &mut Peer) {
    while let Some(Ok(message)) = peer.next().await {
        if matches!(message, Message::Close(_)) {
            break;
        }
    }
}
