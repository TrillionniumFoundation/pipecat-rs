#![allow(dead_code)]
use futures_util::{SinkExt, StreamExt};
use pipecat_core::AudioRawFrame;
use pipecat_gradium::*;
use serde_json::{json, Value};
use std::{future::Future, time::Duration};
use tokio::{net::TcpListener, sync::mpsc, task::JoinHandle};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
};
pub struct Peer {
    pub config: GradiumSttConfig,
    pub outgoing: mpsc::Sender<Message>,
    pub wire: mpsc::Receiver<Value>,
    pub task: JoinHandle<()>,
}
pub async fn peer() -> Peer {
    peer_ready(Some(json!({"type":"ready"}))).await
}
pub async fn peer_ready(ready: Option<Value>) -> Peer {
    peer_mode(ready, true).await
}
// Tungstenite requires the fixed, unboxed HTTP ErrorResponse callback type.
#[allow(clippy::result_large_err)]
pub async fn peer_mode(ready: Option<Value>, auto_eos: bool) -> Peer {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (tx, mut rx) = mpsc::channel(128);
    let (wire_tx, wire) = mpsc::channel(128);
    let task = tokio::spawn(async move {
        let (tcp, _) = listener.accept().await.unwrap();
        let mut socket = accept_hdr_async(tcp, |request: &Request, response: Response| {
            assert_eq!(request.headers()["x-api-key"], "synthetic-not-secret");
            assert_eq!(request.headers()["x-api-source"], "pipecat");
            assert_eq!(request.uri().path(), "/asr");
            Ok(response)
        })
        .await
        .unwrap();
        let Some(Ok(Message::Text(text))) = socket.next().await else {
            return;
        };
        let setup: Value = serde_json::from_str(&text).unwrap();
        wire_tx.send(setup).await.unwrap();
        if let Some(ready) = ready {
            if socket
                .send(Message::Text(ready.to_string().into()))
                .await
                .is_err()
            {
                return;
            }
        }
        loop {
            tokio::select! {
                message=rx.recv()=>{let Some(message)=message else{return};if socket.send(message).await.is_err(){return}},
                message=socket.next()=>match message{
                    Some(Ok(Message::Text(text))) => {
                        let value: Value = serde_json::from_str(&text).unwrap();
                        let eos = value["type"] == "end_of_stream";
                        if wire_tx.send(value).await.is_err() { return; }
                        if eos && auto_eos && socket.send(
                            Message::Text(json!({"type":"end_of_stream"}).to_string().into())
                        ).await.is_err() { return; }
                    },
                    Some(Ok(Message::Close(_)))|None|Some(Err(_))=>return,
                    _=>(),
                }
            }
        }
    });
    let mut config = GradiumSttConfig::new("synthetic-not-secret");
    config.endpoint = format!("ws://{address}/asr");
    config.allow_loopback = true;
    config.limits.connect_timeout = Duration::from_secs(1);
    config.limits.read_timeout = Duration::from_secs(2);
    config.limits.finish_timeout = Duration::from_secs(1);
    config.limits.flush_timeout = Duration::from_millis(350);
    config.limits.close_timeout = Duration::from_millis(100);
    Peer {
        config,
        outgoing: tx,
        wire,
        task,
    }
}
pub async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), future)
        .await
        .expect("fixture deadline")
}
pub async fn emit(peer: &Peer, value: Value) {
    peer.outgoing
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap()
}
pub async fn wire(peer: &mut Peer) -> Value {
    bounded(peer.wire.recv()).await.expect("wire closed")
}
pub async fn next(session: &mut GradiumSttSession) -> GradiumSttEvent {
    bounded(session.next())
        .await
        .expect("event ended")
        .expect("event error")
}
pub fn pcm(bytes: usize) -> AudioRawFrame {
    AudioRawFrame {
        audio: vec![0; bytes],
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    }
}
pub fn step(prob: f64) -> Value {
    json!({"type":"step","vad":[{"horizon_s":3.0,"inactivity_prob":prob}]})
}
pub async fn finish(session: &mut GradiumSttSession) -> Vec<GradiumSttEvent> {
    let sender = session.sender();
    let task = tokio::spawn(async move { sender.finish().await });
    let mut events = Vec::new();
    while let Some(event) = bounded(session.next()).await {
        events.push(event.unwrap())
    }
    bounded(task).await.unwrap().unwrap();
    events
}
pub async fn wait(mut predicate: impl FnMut() -> bool) {
    bounded(async {
        while !predicate() {
            tokio::time::sleep(Duration::from_millis(2)).await
        }
    })
    .await
}
