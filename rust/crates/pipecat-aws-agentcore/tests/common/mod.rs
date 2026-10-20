//! Independent literal-loopback wire fixture. This module does not use provider
//! signing, request mapping, response parsing, or transport implementation helpers.
#![allow(dead_code)]
use hmac::{Hmac, Mac};
use pipecat_aws_agentcore::{AgentCoreConfig, AgentCoreLimits};
use pipecat_aws_common::AwsCredentials;
use pipecat_core::{
    async_trait, ChatMessage, ChatRole, Direction, Frame, FrameData, FrameProcessor,
    LlmContextData, ProcessorContext, ProcessorError, ToolSet,
};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::Notify,
    time::timeout,
};

pub const ARN: &str =
    "arn:aws:bedrock-agentcore:us-east-1:123456789012:runtime/TestAgent-Ab01234567";
pub const ROUTE: &str = "/runtimes/arn%3Aaws%3Abedrock-agentcore%3Aus-east-1%3A123456789012%3Aruntime%2FTestAgent-Ab01234567/invocations";
pub const ACCESS: &str = "AGENTCOREFIXTUREKEY";
pub const SECRET: &str = "synthetic-agentcore-test-secret-not-real";
pub const TOKEN: &str = "synthetic-agentcore-test-token-not-real";
pub const SESSION: &str = "x-amzn-bedrock-agentcore-runtime-session-id";
pub const WAIT: Duration = Duration::from_secs(8);

pub async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
pub fn origin(listener: &TcpListener) -> String {
    format!("http://{}", listener.local_addr().unwrap())
}
pub fn limits() -> AgentCoreLimits {
    AgentCoreLimits {
        connect_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(3),
        idle_timeout: Duration::from_secs(3),
        output_timeout: Duration::from_millis(500),
        total_timeout: Duration::from_secs(6),
        shutdown_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}
pub fn config(listener: &TcpListener) -> AgentCoreConfig {
    config_with_credentials(listener, ACCESS, SECRET, Some(TOKEN))
}
pub fn config_with_credentials(
    listener: &TcpListener,
    access: &str,
    secret: &str,
    token: Option<&str>,
) -> AgentCoreConfig {
    AgentCoreConfig::new(
        "us-east-1",
        ARN,
        AwsCredentials::new(access, secret, token.map(str::to_owned)).unwrap(),
    )
    .unwrap()
    .with_loopback_endpoint(&origin(listener))
    .unwrap()
    .with_limits(limits())
    .unwrap()
}
pub fn context(prompt: &str) -> LlmContextData {
    LlmContextData {
        messages: vec![ChatMessage::new(ChatRole::User, prompt).into()],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    }
}
#[derive(Clone, Debug)]
pub struct Request {
    pub route: String,
    pub headers: BTreeMap<String, String>,
    pub body: Vec<u8>,
}
pub async fn read_request(stream: &mut TcpStream) -> Request {
    timeout(WAIT, async {
        let mut bytes = Vec::new();
        while !bytes.ends_with(b"\r\n\r\n") {
            assert!(bytes.len() < 64 * 1024, "unbounded request head");
            bytes.push(stream.read_u8().await.unwrap());
        }
        let head = std::str::from_utf8(&bytes).unwrap();
        let mut lines = head.split("\r\n");
        let route = lines.next().unwrap().to_owned();
        let mut headers = BTreeMap::new();
        for line in lines.filter(|s| !s.is_empty()) {
            let (name, value) = line.split_once(':').unwrap();
            assert!(headers
                .insert(name.to_ascii_lowercase(), value.trim().to_owned())
                .is_none());
        }
        let size: usize = headers["content-length"].parse().unwrap();
        assert!(size <= 1024 * 1024);
        assert!(!headers.contains_key("transfer-encoding"));
        let mut body = vec![0; size];
        stream.read_exact(&mut body).await.unwrap();
        Request {
            route,
            headers,
            body,
        }
    })
    .await
    .expect("request timed out")
}
pub async fn head(stream: &mut TcpStream, mime: &str) {
    stream.write_all(format!("HTTP/1.1 200 OK\r\nContent-Type: {mime}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").as_bytes()).await.unwrap();
}
pub async fn chunk(stream: &mut TcpStream, bytes: &[u8]) {
    assert!(!bytes.is_empty());
    let mut frame = format!("{:x}\r\n", bytes.len()).into_bytes();
    frame.extend_from_slice(bytes);
    frame.extend_from_slice(b"\r\n");
    stream.write_all(&frame).await.unwrap();
    stream.flush().await.unwrap();
}
pub async fn end_chunks(stream: &mut TcpStream) {
    stream.write_all(b"0\r\n\r\n").await.unwrap();
}
pub async fn response(stream: &mut TcpStream, mime: &str, body: &[u8], split: usize) {
    head(stream, mime).await;
    for part in body.chunks(split) {
        chunk(stream, part).await;
        tokio::task::yield_now().await;
    }
    end_chunks(stream).await;
}
pub async fn peer_eof(stream: &mut TcpStream) {
    let mut byte = [0];
    let count = timeout(WAIT, stream.read(&mut byte))
        .await
        .expect("peer socket did not close")
        .unwrap();
    assert_eq!(count, 0, "client unexpectedly sent more bytes");
}
pub async fn peer_closed(stream: &mut TcpStream) {
    let mut byte = [0];
    match timeout(WAIT, stream.read(&mut byte))
        .await
        .expect("peer socket did not close")
    {
        Ok(0) => {}
        Err(e)
            if matches!(
                e.kind(),
                std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::BrokenPipe
            ) => {}
        result => panic!("client socket was not closed: {result:?}"),
    }
}
pub async fn no_retry(listener: &TcpListener) {
    assert!(
        timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err(),
        "unexpected retry/new request"
    );
}
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn hash(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
fn hmac(key: &[u8], value: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(value);
    mac.finalize().into_bytes().to_vec()
}
/// Independent reconstruction from captured bytes, not a production signer round trip.
pub fn signature_valid(request: &Request, secret: &str) -> bool {
    let authorization = &request.headers["authorization"];
    let fields: BTreeMap<_, _> = authorization
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .unwrap()
        .split(", ")
        .map(|s| s.split_once('=').unwrap())
        .collect();
    let credential: Vec<_> = fields["Credential"].split('/').collect();
    assert_eq!(credential.len(), 5);
    let signed = fields["SignedHeaders"];
    let names: Vec<_> = signed.split(';').collect();
    assert!(names.windows(2).all(|p| p[0] < p[1]));
    let canonical_headers: String = names
        .iter()
        .map(|name| {
            format!(
                "{name}:{}\n",
                request.headers[*name]
                    .split_whitespace()
                    .collect::<Vec<_>>()
                    .join(" ")
            )
        })
        .collect();
    let route = request
        .route
        .split_whitespace()
        .nth(1)
        .unwrap()
        .replace('%', "%25");
    let canonical = format!(
        "POST\n{route}\n\n{canonical_headers}\n{signed}\n{}",
        hash(&request.body)
    );
    let scope = credential[1..].join("/");
    let to_sign = format!(
        "AWS4-HMAC-SHA256\n{}\n{scope}\n{}",
        request.headers["x-amz-date"],
        hash(canonical.as_bytes())
    );
    let day_key = hmac(format!("AWS4{secret}").as_bytes(), credential[1].as_bytes());
    let region_key = hmac(&day_key, credential[2].as_bytes());
    let service_key = hmac(&region_key, credential[3].as_bytes());
    let key = hmac(&service_key, credential[4].as_bytes());
    fields["Signature"] == hex(&hmac(&key, to_sign.as_bytes()))
}
pub fn verify_request(
    request: &Request,
    host: &str,
    prompt: &str,
    access: &str,
    secret: &str,
    token: Option<&str>,
) {
    assert_eq!(request.route, format!("POST {ROUTE} HTTP/1.1"));
    assert_eq!(request.headers["host"], host);
    assert_eq!(request.headers["content-type"], "application/json");
    assert_eq!(
        request.headers["accept"],
        "application/json, text/event-stream"
    );
    assert_eq!(request.headers["connection"], "close");
    assert_eq!(
        request.body,
        serde_json::to_vec(&serde_json::json!({"prompt":prompt})).unwrap()
    );
    assert_eq!(
        request
            .headers
            .get("x-amz-security-token")
            .map(String::as_str),
        token
    );
    let session = uuid::Uuid::parse_str(&request.headers[SESSION]).unwrap();
    assert_eq!(session.get_version_num(), 4);
    assert_eq!(session.to_string(), request.headers[SESSION]);
    let auth = &request.headers["authorization"];
    assert!(auth.contains(&format!("Credential={access}/")));
    assert!(auth.contains("/us-east-1/bedrock-agentcore/aws4_request"));
    assert!(auth.contains(SESSION));
    assert!(signature_valid(request, secret));
    for edit in 0..4 {
        let mut tampered = request.clone();
        match edit {
            0 => tampered.body.push(b' '),
            1 => tampered.route = tampered.route.replace("/invocations", "/commands"),
            2 => {
                tampered
                    .headers
                    .insert(SESSION.into(), uuid::Uuid::new_v4().to_string());
            }
            _ => {
                tampered
                    .headers
                    .insert("x-amz-date".into(), "20200101T000000Z".into());
            }
        }
        assert!(
            !signature_valid(&tampered, secret),
            "tampered field accepted"
        );
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Start,
    Text(String),
    End,
}
#[derive(Default)]
pub struct Recording {
    pub events: Mutex<Vec<Event>>,
    pub changed: Notify,
}
impl Recording {
    pub fn snapshot(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
    pub async fn wait_len(&self, count: usize) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if self.events.lock().unwrap().len() >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("output was not observed");
    }
}
#[async_trait]
impl FrameProcessor for Recording {
    fn name(&self) -> &str {
        "IndependentAgentCoreRecording"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            let event = match &frame.data {
                FrameData::LlmResponseStart => Some(Event::Start),
                FrameData::LlmText(t) => Some(Event::Text(t.text.clone())),
                FrameData::LlmResponseEnd => Some(Event::End),
                _ => None,
            };
            if let Some(event) = event {
                self.events.lock().unwrap().push(event);
                self.changed.notify_waiters();
            }
        }
        ctx.push(frame, direction).await
    }
}
pub fn recording() -> Arc<Recording> {
    Arc::new(Recording::default())
}
