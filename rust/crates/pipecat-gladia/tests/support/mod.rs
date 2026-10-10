#![allow(dead_code)]
use futures_util::{SinkExt, StreamExt};
use pipecat_gladia::*;
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::JoinHandle,
};
use tokio_tungstenite::{
    accept_hdr_async,
    tungstenite::{
        handshake::server::{Request, Response},
        Message,
    },
};
pub async fn bounded<T>(f: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(4), f)
        .await
        .expect("native fixture stalled")
}
pub async fn wait(mut f: impl FnMut() -> bool) {
    bounded(async {
        while !f() {
            tokio::task::yield_now().await
        }
    })
    .await
}
pub async fn request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut buf = [0; 4096];
    let end = loop {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
        assert!(bytes.len() < 128 * 1024);
        if let Some(p) = bytes.windows(4).position(|w| w == b"\r\n\r\n") {
            break p + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let len = headers
        .lines()
        .find_map(|l| {
            l.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>().unwrap())
        })
        .unwrap_or(0);
    while bytes.len() < end + len {
        let n = socket.read(&mut buf).await.unwrap();
        assert!(n > 0);
        bytes.extend_from_slice(&buf[..n]);
    }
    (
        headers,
        if len == 0 {
            Value::Null
        } else {
            serde_json::from_slice(&bytes[end..end + len]).unwrap()
        },
    )
}
pub async fn respond(socket: &mut TcpStream, status: u16, body: &str) {
    socket.write_all(format!("HTTP/1.1 {status} Test\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PeerTermination {
    Close,
    Eof,
    ReadError,
    WriteError,
    CommandClosed,
    ObserverClosed,
}
pub struct Peer {
    pub config: GladiaSttConfig,
    pub send: mpsc::Sender<Message>,
    pub wire: mpsc::Receiver<Message>,
    pub requests: mpsc::Receiver<(String, Value)>,
    pub headers: Arc<Mutex<Vec<Request>>>,
    pub terminations: Arc<Mutex<Vec<PeerTermination>>>,
    pub task: JoinHandle<()>,
}
// tungstenite fixes this callback's error type to an unboxed HTTP response.
#[allow(clippy::result_large_err)]
pub async fn peer(sessions: usize) -> Peer {
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_addr = http.local_addr().unwrap();
    let ws_addr = ws.local_addr().unwrap();
    let mut config = GladiaSttConfig::new("synthetic-rest-key");
    config.endpoint = format!("http://{http_addr}/v2/live");
    config.allowed_session_origins = vec![format!("ws://{ws_addr}")];
    config.allow_loopback = true;
    config.limits.close_timeout = Duration::from_millis(100);
    let (request_tx, requests) = mpsc::channel(8);
    let (wire_tx, wire) = mpsc::channel(64);
    let (send, mut outgoing) = mpsc::channel::<Message>(64);
    let headers = Arc::new(Mutex::new(vec![]));
    let observed = headers.clone();
    let terminations = Arc::new(Mutex::new(vec![]));
    let ended = terminations.clone();
    let task = tokio::spawn(async move {
        for n in 0..sessions {
            let (mut socket, _) = http.accept().await.unwrap();
            let req = request(&mut socket).await;
            request_tx.send(req).await.unwrap();
            respond(&mut socket,201,&json!({"id":format!("fixture-{n}"),"url":format!("ws://{ws_addr}/v2/live?token=synthetic-session-token")}).to_string()).await;
            let observed = observed.clone();
            let mut socket = accept_hdr_async(
                ws.accept().await.unwrap().0,
                move |request: &Request, response: Response| {
                    observed.lock().unwrap().push(request.clone());
                    Ok(response)
                },
            )
            .await
            .unwrap();
            let termination = loop {
                tokio::select! {
                    out = outgoing.recv() => {
                        let Some(out) = out else { break PeerTermination::CommandClosed };
                        if socket.send(out).await.is_err() {
                            break PeerTermination::WriteError;
                        }
                    },
                    incoming = socket.next() => match incoming {
                        Some(Ok(message)) => {
                            let close = matches!(message, Message::Close(_));
                            if wire_tx.send(message).await.is_err() {
                                break PeerTermination::ObserverClosed;
                            }
                            if close {
                                break PeerTermination::Close;
                            }
                        },
                        Some(Err(_)) => break PeerTermination::ReadError,
                        None => break PeerTermination::Eof,
                    },
                }
            };
            ended.lock().unwrap().push(termination);
        }
    });
    Peer {
        config,
        send,
        wire,
        requests,
        headers,
        terminations,
        task,
    }
}
pub async fn emit(peer: &Peer, value: Value) {
    peer.send
        .send(Message::Text(value.to_string().into()))
        .await
        .unwrap()
}
pub async fn next(session: &mut GladiaSttSession) -> GladiaSttEvent {
    bounded(session.next()).await.unwrap().unwrap()
}
pub async fn next_wire(peer: &mut Peer) -> Value {
    let message = bounded(peer.wire.recv()).await.unwrap();
    serde_json::from_str(message.to_text().unwrap()).unwrap()
}
pub fn transcript(text: &str, is_final: bool) -> Value {
    json!({"type":"transcript","data":{"is_final":is_final,"utterance":{"text":text,"language":"en","words":[{"word":text,"start":0,"end":0.1}]}}})
}
pub fn pcm(bytes: usize) -> pipecat_core::AudioRawFrame {
    pipecat_core::AudioRawFrame::new(vec![42; bytes], 16000, 1).unwrap()
}
