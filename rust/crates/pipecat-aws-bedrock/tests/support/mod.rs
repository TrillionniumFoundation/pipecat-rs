#![allow(dead_code)]
use pipecat_aws_bedrock::*;
use pipecat_services::{Context, Message, Role};
use serde_json::{json, Value};
use std::{collections::BTreeMap, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
pub fn credentials() -> AwsCredentials {
    AwsCredentials::new("BEDROCKEXAMPLE", "FAKESECRET", Some("FAKETOKEN/+==".into())).unwrap()
}
pub fn config(listener: &TcpListener) -> BedrockConfig {
    let mut c = BedrockConfig::new(credentials(), "us-east-1");
    c.endpoint = format!("http://{}/", listener.local_addr().unwrap());
    c.limits.connect_timeout = Duration::from_secs(2);
    c.limits.idle_timeout = Duration::from_millis(500);
    c.limits.request_timeout = Duration::from_secs(3);
    c
}
pub fn request() -> CompletionRequest {
    CompletionRequest::new(
        "us.amazon.nova-lite-v1:0",
        Context::new(vec![Message::text(Role::User, "Hello")]),
    )
}
pub fn crc(data: &[u8]) -> u32 {
    let mut c = !0u32;
    for b in data {
        c ^= u32::from(*b);
        for _ in 0..8 {
            c = (c >> 1) ^ if c & 1 == 1 { 0xedb88320 } else { 0 };
        }
    }
    !c
}
pub fn record(kind: &str, value: Value) -> Vec<u8> {
    raw_record(
        "event",
        ":event-type",
        kind,
        serde_json::to_vec(&value).unwrap(),
    )
}
pub fn raw_record(message_type: &str, name: &str, kind: &str, payload: Vec<u8>) -> Vec<u8> {
    let mut headers = vec![];
    for (n, v) in [
        (":message-type", message_type),
        (name, kind),
        (":content-type", "application/json"),
    ] {
        headers.push(n.len() as u8);
        headers.extend_from_slice(n.as_bytes());
        headers.push(7);
        headers.extend_from_slice(&(v.len() as u16).to_be_bytes());
        headers.extend_from_slice(v.as_bytes());
    }
    let mut all = vec![];
    all.extend_from_slice(&((16 + headers.len() + payload.len()) as u32).to_be_bytes());
    all.extend_from_slice(&(headers.len() as u32).to_be_bytes());
    all.extend_from_slice(&crc(&all).to_be_bytes());
    all.extend(headers);
    all.extend(payload);
    all.extend_from_slice(&crc(&all).to_be_bytes());
    all
}
pub fn text_response(text: &str) -> Vec<u8> {
    [
record("messageStart",json!({"role":"assistant"})),
record("contentBlockDelta",json!({"contentBlockIndex":0,"delta":{"text":text}})),
record("contentBlockStop",json!({"contentBlockIndex":0})),
record("messageStop",json!({"stopReason":"end_turn"})),
record("metadata",json!({"usage":{"inputTokens":5,"outputTokens":2,"totalTokens":10,"cacheReadInputTokens":3}})),
].concat()
}
#[derive(Debug)]
pub struct Request {
    pub route: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
impl Request {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap()
    }
}
pub async fn read_request(socket: &mut TcpStream) -> Request {
    let mut all = vec![];
    let end = loop {
        let mut one = [0u8; 1];
        socket.read_exact(&mut one).await.unwrap();
        all.push(one[0]);
        if all.ends_with(b"\r\n\r\n") {
            break all.len();
        }
        assert!(all.len() < 64 * 1024);
    };
    let head = String::from_utf8_lossy(&all[..end]);
    let mut lines = head.lines();
    let route = lines.next().unwrap().to_string();
    let headers: BTreeMap<String, String> = lines
        .filter_map(|l| {
            l.split_once(':')
                .map(|(k, v)| (k.to_ascii_lowercase(), v.trim().into()))
        })
        .collect();
    let n: usize = headers["content-length"].parse().unwrap();
    assert!(n < 32 * 1024 * 1024);
    let mut body = vec![0; n];
    socket.read_exact(&mut body).await.unwrap();
    Request {
        route,
        headers,
        body,
    }
}
pub async fn respond(socket: &mut TcpStream, content: &str, body: &[u8], split: usize) {
    socket.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {content}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",body.len()).as_bytes()).await.unwrap();
    for chunk in body.chunks(split.max(1)) {
        socket.write_all(chunk).await.unwrap();
    }
}
