pub mod fixture;
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::{JoinHandle, JoinSet},
};
#[derive(Default)]
pub struct Recorded {
    pub initializes: usize,
    pub initialized: usize,
    pub deletes: usize,
    pub completed_deletes: usize,
    pub calls: Vec<Value>,
    pub cancellations: usize,
    pub requests: Vec<(String, BTreeMap<String, String>)>,
    pub mode: String,
    pub completions: Vec<Value>,
}
pub struct Server {
    pub url: String,
    pub state: Arc<Mutex<Recorded>>,
    task: JoinHandle<()>,
}
impl Server {
    pub async fn start(mode: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}/mcp", listener.local_addr().unwrap());
        let state = Arc::new(Mutex::new(Recorded {
            mode: mode.into(),
            ..Default::default()
        }));
        let shared = state.clone();
        let task = tokio::spawn(async move {
            let mut connections = JoinSet::new();
            loop {
                tokio::select! {
                    result = listener.accept() => { let (socket, _) = result.unwrap(); let shared = shared.clone(); connections.spawn(async move { handle(socket, shared).await; }); },
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
        .expect("fixture condition reached");
    }
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn handle(mut socket: TcpStream, shared: Arc<Mutex<Recorded>>) {
    let mut bytes = vec![];
    let header_end = loop {
        let mut chunk = [0; 1024];
        let Ok(size) = socket.read(&mut chunk).await else {
            return;
        };
        if size == 0 {
            return;
        }
        bytes.extend_from_slice(&chunk[..size]);
        if let Some(index) = bytes.windows(4).position(|s| s == b"\r\n\r\n") {
            break index + 4;
        }
        if bytes.len() > 128 * 1024 {
            return;
        }
    };
    let header = String::from_utf8_lossy(&bytes[..header_end]);
    let method = header.split_whitespace().next().unwrap().to_string();
    let headers: BTreeMap<_, _> = header
        .lines()
        .skip(1)
        .filter_map(|line| line.split_once(':'))
        .map(|(key, value)| (key.to_lowercase(), value.trim().to_string()))
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
    let message: Value =
        serde_json::from_slice(&bytes[header_end..header_end + size]).unwrap_or(Value::Null);
    let mode = {
        let mut state = shared.lock().unwrap();
        state.requests.push((method.clone(), headers));
        if message.get("messages").is_some() {
            state.completions.push(message.clone());
        }
        if method == "DELETE" {
            state.deletes += 1;
        }
        match message["method"].as_str() {
            Some("initialize") => state.initializes += 1,
            Some("notifications/initialized") => state.initialized += 1,
            Some("notifications/cancelled") => state.cancellations += 1,
            Some("tools/call") => state.calls.push(message["params"].clone()),
            _ => {}
        }
        state.mode.clone()
    };
    if method == "GET" {
        respond(&mut socket, 405, "application/json", "", "").await;
        return;
    }
    if method == "DELETE" {
        if mode == "slow-delete" || mode == "slow-delete-fails" {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        if mode == "delete-fails" || mode == "slow-delete-fails" {
            respond(&mut socket, 503, "text/plain", "secret-token", "").await;
            return;
        }
        shared.lock().unwrap().completed_deletes += 1;
        respond(&mut socket, 204, "application/json", "", "").await;
        return;
    }
    if mode == "llm" || mode == "llm-slow" {
        let saw_tool = message["messages"]
            .as_array()
            .unwrap()
            .iter()
            .any(|m| m["role"] == "tool");
        let delta = if saw_tool {
            json!({"content":"tool completed"})
        } else if mode == "llm-slow" {
            json!({"tool_calls":[{"index":0,"id":"agent-call","type":"function","function":{"name":"slow","arguments":"{}"}}]})
        } else {
            json!({"tool_calls":[{"index":0,"id":"agent-call","type":"function","function":{"name":"echo","arguments":"{\"value\":\"agent\"}"}}]})
        };
        let finish = if saw_tool { "stop" } else { "tool_calls" };
        let body = format!(
            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
            json!({"choices":[{"index":0,"delta":delta}]}),
            json!({"choices":[{"index":0,"delta":{},"finish_reason":finish}]})
        );
        respond(&mut socket, 200, "text/event-stream", &body, "").await;
        return;
    }
    let operation = message["method"].as_str().unwrap_or_default();
    if operation == "initialize" && mode == "slow-initialize" {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    if operation == "initialize" && mode == "hang" {
        tokio::time::sleep(Duration::from_secs(30)).await;
        return;
    }
    if operation == "initialize" && mode == "bad-initialize" {
        respond(&mut socket, 503, "text/plain", "secret-token", "").await;
        return;
    }
    if message.get("id").is_none() {
        respond(&mut socket, 202, "application/json", "", "").await;
        return;
    }
    let name = message["params"]["name"].as_str().unwrap_or_default();
    if name == "slow" {
        if mode == "blocked-slow" {
            // This call stays pending until the owning fixture aborts the
            // connection task. Close tests must win by cancellation, not time.
            std::future::pending::<()>().await;
        } else {
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    }
    if name == "side_effect" {
        if mode == "disconnect" {
            return;
        }
        if mode == "expire" {
            respond(&mut socket, 404, "text/plain", "secret-token", "").await;
            return;
        }
        if mode == "delayed-expire" {
            tokio::time::sleep(Duration::from_millis(150)).await;
            respond(&mut socket, 404, "text/plain", "secret-token", "").await;
            return;
        }
    }
    if mode == "redirect" {
        respond(
            &mut socket,
            302,
            "text/plain",
            "",
            "Location: http://127.0.0.1:1/stolen\r\n",
        )
        .await;
        return;
    }
    let mut result = match operation {
        "initialize" => fixture::initialize(),
        "tools/list" => fixture::tools(),
        "tools/call" => fixture::call(name, &message["params"]["arguments"]),
        _ => json!({}),
    };
    if operation == "initialize" {
        match mode.as_str() {
            "unsupported-protocol" => result["protocolVersion"] = "2099-01-01".into(),
            "modern-protocol" => result["protocolVersion"] = "2026-07-28".into(),
            "older-protocol" => result["protocolVersion"] = "2025-06-18".into(),
            "no-tools" => result["capabilities"] = json!({}),
            _ => {}
        }
    }
    if operation == "tools/list" {
        if mode == "without-echo" {
            result["tools"]
                .as_array_mut()
                .unwrap()
                .retain(|tool| tool["name"] != "echo");
        }
        if mode == "pages" {
            if message["params"]["cursor"].is_null() {
                result["tools"].as_array_mut().unwrap().truncate(1);
                result["nextCursor"] = "page-two".into();
            } else {
                result["tools"].as_array_mut().unwrap().remove(0);
            }
        }
        if mode == "cursor-loop" {
            result["nextCursor"] = "same".into();
        }
        if mode == "duplicate" {
            result["tools"]
                .as_array_mut()
                .unwrap()
                .push(fixture::tools()["tools"][0].clone());
        }
        if mode == "bad-schema" {
            result["tools"][0]["inputSchema"]["properties"] = json!([]);
        }
    }
    let response = if name == "failure" {
        let code = if mode == "connection-closed-code" {
            -32000
        } else {
            -32602
        };
        json!({"jsonrpc":"2.0","id":message["id"],"error":{"code":code,"message":"secret-token","data":{"password":"secret-token"}}})
    } else {
        json!({"jsonrpc":"2.0","id":message["id"],"result":result})
    };
    let session_header = if operation == "initialize" && mode != "stateless" {
        "Mcp-Session-Id: native-session\r\n"
    } else {
        ""
    };
    if name == "oversized" && matches!(mode.as_str(), "chunked-json" | "fragmented-sse") {
        let (content_type, body) = if mode == "fragmented-sse" {
            ("text/event-stream", "data:\r\n".repeat(512))
        } else {
            ("application/json", response.to_string())
        };
        respond_chunked(&mut socket, content_type, body.as_bytes()).await;
        return;
    }
    if mode == "sse" || mode == "large-sse" && operation == "tools/call" {
        let body = if mode == "large-sse" {
            format!("data: {}", "x".repeat(8192))
        } else {
            format!("event: message\r\ndata: {}\r\n\r\n", response)
        };
        respond(&mut socket, 200, "text/event-stream", &body, session_header).await;
    } else {
        respond(
            &mut socket,
            200,
            "application/json",
            &response.to_string(),
            session_header,
        )
        .await;
    }
}
async fn respond_chunked(socket: &mut TcpStream, content_type: &str, body: &[u8]) {
    let headers = format!("HTTP/1.1 200 Fixture\r\nContent-Type: {content_type}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n");
    if socket.write_all(headers.as_bytes()).await.is_err() {
        return;
    }
    for chunk in body.chunks(127) {
        if socket
            .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
            .await
            .is_err()
            || socket.write_all(chunk).await.is_err()
            || socket.write_all(b"\r\n").await.is_err()
        {
            return;
        }
        tokio::time::sleep(Duration::from_millis(1)).await;
    }
    let _ = socket.write_all(b"0\r\n\r\n").await;
    let _ = socket.shutdown().await;
}
async fn respond(
    socket: &mut TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
    headers: &str,
) {
    let text = format!("HTTP/1.1 {status} Fixture\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\n{headers}Connection: close\r\n\r\n{body}",body.len());
    let _ = socket.write_all(text.as_bytes()).await;
    let _ = socket.shutdown().await;
}
