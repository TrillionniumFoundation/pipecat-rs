//! Independent deterministic wire fixture. No Python or live service is used.
use futures_util::{SinkExt, StreamExt};
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_tungstenite::{accept_async, tungstenite::Message, WebSocketStream};

pub type Socket = WebSocketStream<TcpStream>;
pub const DEADLINE: Duration = Duration::from_secs(3);

pub async fn listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/", listener.local_addr().unwrap());
    (listener, endpoint)
}

pub async fn accept(listener: TcpListener) -> Socket {
    tokio::time::timeout(DEADLINE, async {
        accept_async(listener.accept().await.unwrap().0)
            .await
            .unwrap()
    })
    .await
    .expect("fixture accept deadline")
}

pub async fn send(ws: &mut Socket, value: Value) {
    tokio::time::timeout(DEADLINE, ws.send(Message::Text(value.to_string().into())))
        .await
        .expect("fixture write deadline")
        .expect("fixture write");
}

pub async fn request(ws: &mut Socket) -> Value {
    tokio::time::timeout(DEADLINE, async {
        loop {
            match ws
                .next()
                .await
                .expect("fixture peer closed")
                .expect("fixture read")
            {
                Message::Text(text) => return serde_json::from_str(&text).expect("request JSON"),
                Message::Ping(_) => {
                    ws.flush().await.unwrap();
                }
                other => panic!("unexpected request message: {other:?}"),
            }
        }
    })
    .await
    .expect("fixture request deadline")
}

pub async fn hello(listener: TcpListener) -> Socket {
    let mut ws = accept(listener).await;
    send(
        &mut ws,
        json!({"type":"event","event":"connect.challenge","payload":{"nonce":"review-challenge"}}),
    )
    .await;
    let connect = request(&mut ws).await;
    assert_eq!(connect["type"], "req");
    assert_eq!(connect["method"], "connect");
    assert_eq!(connect["params"]["minProtocol"], 4);
    assert_eq!(connect["params"]["maxProtocol"], 4);
    ok(&mut ws, &connect, json!({"type":"hello-ok","protocol":4})).await;
    ws
}

pub async fn ok(ws: &mut Socket, request: &Value, payload: Value) {
    send(
        ws,
        json!({"type":"res","id":request["id"],"ok":true,"payload":payload}),
    )
    .await;
}

pub async fn reject(ws: &mut Socket, request: &Value) {
    send(ws, json!({"type":"res","id":request["id"],"ok":false,"error":{"code":"REVIEW_REJECTED","message":"private-rejected-content"}})).await;
}

pub fn chat(run_id: &str, state: &str, text: &str) -> Value {
    json!({"type":"event","event":"chat","payload":{"runId":run_id,"state":state,"deltaText":text,"message":{"role":"assistant","content":[{"type":"text","text":text}]}}})
}

pub async fn done(task: JoinHandle<()>) {
    tokio::time::timeout(DEADLINE, task)
        .await
        .expect("fixture task deadline")
        .expect("fixture task panicked");
}

pub async fn receive<T>(rx: oneshot::Receiver<T>) -> T {
    tokio::time::timeout(DEADLINE, rx)
        .await
        .expect("fixture signal deadline")
        .expect("fixture signal dropped")
}
