use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
    task::{JoinHandle, JoinSet},
};

#[derive(Default)]
pub struct Recorded {
    pub requests: Vec<(String, String, BTreeMap<String, String>)>,
    pub messages: Vec<Value>,
    pub closed_streams: usize,
    pub closed_posts: usize,
    pub hold_endpoint: bool,
    pub hold_posts: bool,
    pub keepalive: bool,
    pub post_delay: Duration,
    events: Option<mpsc::Sender<String>>,
}
pub struct Server {
    pub url: String,
    pub state: Arc<Mutex<Recorded>>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn start(endpoint: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/sse", listener.local_addr().unwrap());
        let endpoint = endpoint.unwrap_or_else(|| "/messages?session=owned-native".into());
        let state = Arc::new(Mutex::new(Recorded::default()));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let (socket, _) = result.unwrap();
                        let state = shared.clone(); let endpoint = endpoint.clone();
                        connections.spawn(async move { handle(socket, state, endpoint).await; });
                    }
                    _ = connections.join_next(), if !connections.is_empty() => {},
                }
            }
        });
        Self { url, state, task }
    }
    pub async fn wait_for(&self, check: impl Fn(&Recorded) -> bool) {
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if check(&self.state.lock().unwrap()) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("legacy SSE fixture condition");
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn handle(mut socket: TcpStream, shared: Arc<Mutex<Recorded>>, endpoint: String) {
    let mut bytes = vec![];
    let header_end = loop {
        let mut chunk = [0; 1024];
        let Ok(n) = socket.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
        if let Some(i) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break i + 4;
        }
        if bytes.len() > 128 * 1024 {
            return;
        }
    };
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let mut line = header.split_whitespace();
    let method = line.next().unwrap().to_string();
    let path = line.next().unwrap().to_string();
    let headers: BTreeMap<_, _> = header
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(k, v)| (k.to_lowercase(), v.trim().to_string()))
        .collect();
    let size: usize = headers
        .get("content-length")
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    if size > 128 * 1024 {
        return;
    }
    while bytes.len() - header_end < size {
        let mut chunk = [0; 1024];
        let Ok(n) = socket.read(&mut chunk).await else {
            return;
        };
        if n == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..n]);
    }
    shared
        .lock()
        .unwrap()
        .requests
        .push((method.clone(), path.clone(), headers));
    if method == "GET" && path == "/sse" {
        let (tx, mut rx) = mpsc::channel::<String>(16);
        shared.lock().unwrap().events = Some(tx);
        let head = "HTTP/1.1 200 Fixture\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n";
        if socket.write_all(head.as_bytes()).await.is_err() {
            return;
        }
        let event = format!("event: endpoint\r\ndata: {endpoint}\r\n\r\n");
        // Split CRLF and data over real network chunks to exercise the byte budget.
        let (hold_endpoint, keepalive) = {
            let state = shared.lock().unwrap();
            (state.hold_endpoint, state.keepalive)
        };
        if !hold_endpoint && !chunks(&mut socket, &event).await {
            return;
        }
        let mut probe = [0; 1];
        let mut heartbeat = tokio::time::interval(Duration::from_millis(10));
        loop {
            tokio::select! {
                event = rx.recv() => {
                    match event {
                        Some(event) if chunks(&mut socket, &event).await => {},
                        _ => break,
                    }
                }
                _ = socket.read(&mut probe) => break,
                _ = heartbeat.tick(), if keepalive => {
                    if !chunks(&mut socket, ": heartbeat\r\n\r\n").await { break; }
                }
            }
        }
        shared.lock().unwrap().closed_streams += 1;
        return;
    }
    if method != "POST" || path != "/messages?session=owned-native" {
        respond(&mut socket, 405).await;
        return;
    }
    let message: Value = serde_json::from_slice(&bytes[header_end..header_end + size]).unwrap();
    let events = {
        let mut state = shared.lock().unwrap();
        state.messages.push(message.clone());
        state.events.clone()
    };
    if message["method"] == "tools/call" {
        let (hold_posts, delay) = {
            let state = shared.lock().unwrap();
            (state.hold_posts, state.post_delay)
        };
        if hold_posts {
            let mut probe = [0; 1];
            let _ = socket.read(&mut probe).await;
            shared.lock().unwrap().closed_posts += 1;
            return;
        }
        tokio::time::sleep(delay).await;
    }
    respond(&mut socket, 202).await;
    let Some(id) = message.get("id") else {
        return;
    };
    let name = message["params"]["name"].as_str().unwrap_or_default();
    if name == "slow" {
        return;
    }
    if name == "side_effect" {
        // EOF after accepting a side effect; the client must not replay it.
        shared.lock().unwrap().events = None;
        drop(events);
        return;
    }
    let result = match message["method"].as_str() {
        Some("initialize") => crate::support::fixture::initialize(),
        Some("tools/list") => crate::support::fixture::tools(),
        Some("tools/call") => crate::support::fixture::call(name, &message["params"]["arguments"]),
        _ => json!({}),
    };
    let event = if name == "oversized" {
        format!("event: message\r\ndata: {}", "x".repeat(8192))
    } else {
        format!(
            "event: message\r\ndata: {}\r\n\r\n",
            json!({"jsonrpc":"2.0","id":id,"result":result})
        )
    };
    if let Some(events) = events {
        let _ = events.send(event).await;
    }
}
async fn chunks(socket: &mut TcpStream, event: &str) -> bool {
    for bytes in event.as_bytes().chunks(7) {
        let head = format!("{:x}\r\n", bytes.len());
        if socket.write_all(head.as_bytes()).await.is_err()
            || socket.write_all(bytes).await.is_err()
            || socket.write_all(b"\r\n").await.is_err()
        {
            return false;
        }
    }
    true
}
async fn respond(socket: &mut TcpStream, status: u16) {
    let response =
        format!("HTTP/1.1 {status} Fixture\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
    let _ = socket.write_all(response.as_bytes()).await;
    let _ = socket.shutdown().await;
}
