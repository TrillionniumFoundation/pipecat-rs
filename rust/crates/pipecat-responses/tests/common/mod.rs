#![allow(dead_code)]
use futures_util::{SinkExt, StreamExt};
use pipecat_responses::*;
use serde_json::{json, Value};
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};
use tokio_tungstenite::{tungstenite::Message, WebSocketStream};
pub fn request() -> ResponsesRequest {
    ResponsesRequest::new(
        "gpt-4.1",
        ResponsesContext::new(vec![InputItem::text(pipecat_services::Role::User, "hello")]),
    )
}
pub fn config() -> ResponsesConfig {
    ResponsesConfig {
        request_timeout: Duration::from_secs(3),
        idle_timeout: Duration::from_secs(1),
        drain_timeout: Duration::from_millis(300),
        ..ResponsesConfig::new("synthetic-secret")
    }
}
pub fn created(id: &str) -> Value {
    json!({"type":"response.created","response":{"id":id}})
}
pub fn delta(text: &str) -> Value {
    json!({"type":"response.output_text.delta","delta":text})
}
pub fn completed(id: &str, text: &str) -> Value {
    json!({"type":"response.completed","response":{"id":id,"model":"gpt-4.1-snapshot","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":text}]}],"usage":{"input_tokens":10,"output_tokens":4,"total_tokens":14,"input_tokens_details":{"cached_tokens":3,"cache_write_tokens":2},"output_tokens_details":{"reasoning_tokens":1}}}})
}
pub fn record(value: Value) -> Vec<u8> {
    format!(
        "event: {}\r\ndata: {value}\r\n\r\n",
        value["type"].as_str().unwrap()
    )
    .into_bytes()
}
pub async fn collect(mut stream: ResponsesStream) -> Vec<Result<ResponseEvent, ServiceError>> {
    tokio::time::timeout(Duration::from_secs(6), async move {
        let mut events = vec![];
        while let Some(event) = stream.recv().await {
            events.push(event);
        }
        events
    })
    .await
    .unwrap()
}
pub async fn read_http(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut chunk = [0; 4096];
    let end = loop {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(pos) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length: usize = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|s| s.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&chunk[..n]);
    }
    (
        headers,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
pub async fn reply(socket: &mut TcpStream, status: &str, mime: &str, body: &[u8]) {
    let _=socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\nRetry-After: 3\r\n\r\n",body.len()).as_bytes()).await;
    let _ = socket.write_all(body).await;
}
pub async fn http_fixture(
    events: Vec<Value>,
) -> (
    ResponsesHttpClient,
    oneshot::Receiver<(String, Value)>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.http_url = format!("http://{}/v1/responses", listener.local_addr().unwrap());
    let (tx, rx) = oneshot::channel();
    let task = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let _ = tx.send(read_http(&mut socket).await);
        let body: Vec<u8> = events.into_iter().flat_map(record).collect();
        reply(&mut socket, "200 OK", "text/event-stream", &body).await;
    });
    (ResponsesHttpClient::new(cfg).unwrap(), rx, task)
}
pub async fn ws_read(ws: &mut WebSocketStream<TcpStream>) -> Value {
    loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(text) => return serde_json::from_str(&text).unwrap(),
            Message::Ping(payload) => ws.send(Message::Pong(payload)).await.unwrap(),
            other => panic!("unexpected {other:?}"),
        }
    }
}
pub async fn ws_send(ws: &mut WebSocketStream<TcpStream>, value: Value) {
    ws.send(Message::Text(value.to_string().into()))
        .await
        .unwrap();
}
pub async fn ws_listener() -> (TcpListener, ResponsesConfig) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut cfg = config();
    cfg.websocket_url = format!("ws://{}/v1/responses", listener.local_addr().unwrap());
    (listener, cfg)
}
