//! Shared real loopback HTTP/SSE harness. This module contains no tests.
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, FinishReason, LlmClient, Message, Provider, Role,
    Usage,
};
use serde_json::{json, Value};
use std::{fmt::Write as _, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
    time::timeout,
};

pub const WAIT: Duration = Duration::from_secs(10);
pub const MODEL: &str = "gemini-2.5-usage-fixture";
pub const MARKER: &str = "latest-usage-decoded";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Protocol,
    // Constructed by the services target; RTVI exercises ordinary EOF/errors.
    #[allow(dead_code)]
    Cancelled,
    Timeout,
}
impl Outcome {
    pub fn holds_socket(self) -> bool {
        matches!(self, Self::Cancelled | Self::Timeout)
    }
}
#[derive(Clone, Debug)]
pub struct Case {
    pub label: String,
    pub records: Vec<Value>,
    pub usage: Option<Usage>,
    pub outcome: Outcome,
    pub reason: Option<FinishReason>,
}
pub fn snapshot(
    prompt: u64,
    completion: u64,
    total: u64,
    cache: Option<u64>,
    reasoning: Option<u64>,
) -> Usage {
    Usage {
        prompt_tokens: prompt,
        completion_tokens: completion,
        total_tokens: total,
        cache_read_input_tokens: cache,
        cache_creation_input_tokens: None,
        reasoning_tokens: reasoning,
    }
}
pub fn full() -> Value {
    json!({"promptTokenCount":40,"toolUsePromptTokenCount":3,"candidatesTokenCount":10,
        "thoughtsTokenCount":120,"cachedContentTokenCount":15,"totalTokenCount":173})
}
pub fn full_snapshot() -> Usage {
    snapshot(43, 130, 173, Some(15), Some(120))
}
pub fn usage(metadata: Value) -> Value {
    json!({"candidates":null,"usageMetadata":metadata})
}
pub fn finish() -> Value {
    json!({"candidates":[{"index":0,"finishReason":"STOP"}]})
}
pub fn marker() -> Value {
    json!({"candidates":[{"index":0,"content":{"parts":[{"text":MARKER}]}}]})
}
pub fn complete(
    label: impl Into<String>,
    mut records: Vec<Value>,
    expected: Option<Usage>,
) -> Case {
    records.push(finish());
    Case {
        label: label.into(),
        records,
        usage: expected,
        outcome: Outcome::Success,
        reason: Some(FinishReason::Stop),
    }
}
pub fn request() -> CompletionRequest {
    CompletionRequest::new(
        MODEL,
        Context::new(vec![Message::text(Role::User, "synthetic usage request")]),
    )
}
async fn read_request(socket: &mut TcpStream) -> Result<(String, Value), String> {
    const MAX: usize = 32 * 1024;
    let mut bytes = vec![];
    let mut chunk = [0; 2048];
    let end = loop {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed before HTTP headers".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX {
            return Err("HTTP request budget".into());
        }
        if let Some(pos) = bytes.windows(4).position(|b| b == b"\r\n\r\n") {
            break pos + 4;
        }
    };
    let headers = String::from_utf8(bytes[..end].to_vec()).map_err(|e| e.to_string())?;
    let length = headers
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|n| n.trim().parse::<usize>())
        })
        .ok_or("missing HTTP content length")?
        .map_err(|e| e.to_string())?;
    if length > MAX - end {
        return Err("HTTP body budget".into());
    }
    while bytes.len() < end + length {
        let n = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if n == 0 {
            return Err("closed before HTTP body".into());
        }
        bytes.extend_from_slice(&chunk[..n]);
        if bytes.len() > MAX {
            return Err("HTTP body budget".into());
        }
    }
    let body = serde_json::from_slice(&bytes[end..end + length]).map_err(|e| e.to_string())?;
    Ok((headers, body))
}
#[derive(Debug, Default)]
pub struct Trace {
    requests: Vec<(String, Value)>,
    closed_connections: usize,
}
pub struct Peer {
    stop: oneshot::Sender<()>,
    task: JoinHandle<Result<Trace, String>>,
}
impl Peer {
    pub async fn open(cases: &[Case]) -> Result<(Self, LlmClient), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let mut config = ClientConfig::new(Provider::Gemini, "synthetic-usage-key");
        config.endpoint = format!(
            "http://{}/v1beta/models/{{model}}:streamGenerateContent?alt=sse",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        config.connect_timeout = Duration::from_secs(2);
        config.idle_timeout = Duration::from_millis(500);
        config.request_timeout = Duration::from_secs(5);
        let client = LlmClient::new(config).map_err(|e| e.to_string())?;
        let cases = cases.to_vec();
        let (stop, mut stopped) = oneshot::channel();
        let task = tokio::spawn(async move {
            let mut trace = Trace::default();
            for case in cases {
                let accepted = tokio::select! {
                    _ = &mut stopped => return Err("peer stopped before all requests".into()),
                    accepted = timeout(WAIT, listener.accept()) => accepted
                        .map_err(|_| "HTTP accept deadline")?.map_err(|e| e.to_string())?,
                };
                let mut socket = accepted.0;
                trace.requests.push(
                    timeout(WAIT, read_request(&mut socket))
                        .await
                        .map_err(|_| "HTTP request deadline")??,
                );
                let mut data = String::new();
                for value in &case.records {
                    write!(&mut data, "data: {value}\n\n").map_err(|e| e.to_string())?;
                }
                let response = if case.outcome.holds_socket() {
                    // A nonterminated chunked response remains open until the
                    // consumer's cancellation or actual idle timeout closes it.
                    format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n{:x}\r\n{data}\r\n", data.len())
                } else {
                    format!("HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{data}", data.len())
                };
                timeout(WAIT, socket.write_all(response.as_bytes()))
                    .await
                    .map_err(|_| "HTTP response deadline")?
                    .map_err(|e| e.to_string())?;
                if case.outcome.holds_socket() {
                    let mut byte = [0];
                    let closed = tokio::select! {
                        _ = &mut stopped => return Err("peer stopped before client socket close".into()),
                        closed = timeout(WAIT, socket.read(&mut byte)) => closed
                            .map_err(|_| "HTTP close deadline")?,
                    };
                    match closed {
                        Ok(0) => trace.closed_connections += 1,
                        Err(error)
                            if matches!(
                                error.kind(),
                                std::io::ErrorKind::ConnectionReset
                                    | std::io::ErrorKind::BrokenPipe
                                    | std::io::ErrorKind::UnexpectedEof
                            ) =>
                        {
                            trace.closed_connections += 1;
                        }
                        other => return Err(format!("expected client socket close: {other:?}")),
                    }
                } else {
                    socket.shutdown().await.map_err(|e| e.to_string())?;
                }
            }
            Ok(trace)
        });
        Ok((Self { stop, task }, client))
    }
    pub async fn join(mut self) -> Result<Trace, String> {
        // Success waits for actual peer completion. A timeout is a failed test,
        // followed by explicit abort+join cleanup, never a termination witness.
        match timeout(WAIT, &mut self.task).await {
            Ok(result) => result.map_err(|e| e.to_string()).and_then(|r| r),
            Err(error) => {
                let _ = self.stop.send(());
                self.task.abort();
                let joined = self.task.await;
                Err(format!("HTTP peer aborted and joined: {error}; {joined:?}"))
            }
        }
    }
}
pub fn check_trace(trace: &Trace, cases: &[Case]) {
    assert_eq!(trace.requests.len(), cases.len());
    assert_eq!(
        trace.closed_connections,
        cases
            .iter()
            .filter(|case| case.outcome.holds_socket())
            .count()
    );
    for (headers, body) in &trace.requests {
        let headers = headers.to_ascii_lowercase();
        assert!(headers.starts_with(&format!(
            "post /v1beta/models/{MODEL}:streamgeneratecontent?alt=sse http/1.1\r\n"
        )));
        assert!(headers.contains("x-goog-api-key: synthetic-usage-key\r\n"));
        assert_eq!(
            body["contents"][0]["parts"][0]["text"],
            "synthetic usage request"
        );
    }
}
