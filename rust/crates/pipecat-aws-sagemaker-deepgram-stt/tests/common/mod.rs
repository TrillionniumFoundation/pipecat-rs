#![allow(dead_code)]
use bytes::Bytes;
use chrono::{DateTime, Utc};
use h2::{server, RecvStream, SendStream};
use hmac::{Hmac, Mac};
use http::{Request, Response};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    future::{poll_fn, Future},
    sync::Mutex,
    time::Duration,
};
use tokio::{net::TcpListener, task::JoinHandle};
pub const ACCESS: &str = "SAGEMAKERSTTFIXTURE";
pub const SECRET: &str = "synthetic-stt-secret-not-real";
pub const TOKEN: &str = "synthetic-stt-token";
pub const QUERY: &str = "detect_entities=false&diarize=false&dictation=false&interim_results=true&numerals=false&punctuate=true&smart_format=false&model=nova-3&language=en&encoding=linear16&channels=1&multichannel=false&sample_rate=16000";
pub const MIME: &str = "application/vnd.amazon.eventstream";
pub const PATH: &str = "/endpoints/test-endpoint/invocations-bidirectional-stream";
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn sha(bytes: &[u8]) -> String {
    hex(&Sha256::digest(bytes))
}
fn hmac(key: &[u8], data: &[u8]) -> Vec<u8> {
    let mut mac = Hmac::<Sha256>::new_from_slice(key).unwrap();
    mac.update(data);
    mac.finalize().into_bytes().to_vec()
}
fn signing_key(day: &str) -> Vec<u8> {
    let day = hmac(format!("AWS4{SECRET}").as_bytes(), day.as_bytes());
    let region = hmac(&day, b"us-east-1");
    let service = hmac(&region, b"sagemaker");
    hmac(&service, b"aws4_request")
}
fn crc(bytes: &[u8]) -> u32 {
    let mut result = !0_u32;
    for byte in bytes {
        result ^= u32::from(*byte);
        for _ in 0..8 {
            result = (result >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(result & 1));
        }
    }
    !result
}
fn u32_be(bytes: &[u8]) -> u32 {
    u32::from_be_bytes(bytes.try_into().unwrap())
}
#[derive(Clone, Debug, PartialEq)]
enum Header {
    Text(String),
    Binary(Vec<u8>),
    Timestamp(i64),
}
struct Parsed {
    headers: BTreeMap<String, Header>,
    payload: Vec<u8>,
}
fn parse_frame(bytes: &[u8]) -> Parsed {
    assert!((16..=1024 * 1024).contains(&bytes.len()));
    assert_eq!(u32_be(&bytes[..4]) as usize, bytes.len());
    assert_eq!(u32_be(&bytes[8..12]), crc(&bytes[..8]), "prelude CRC");
    assert_eq!(
        u32_be(&bytes[bytes.len() - 4..]),
        crc(&bytes[..bytes.len() - 4]),
        "message CRC"
    );
    let header_end = 12 + u32_be(&bytes[4..8]) as usize;
    assert!(header_end <= bytes.len() - 4);
    let mut cursor = 12;
    let mut headers = BTreeMap::new();
    while cursor < header_end {
        let count = usize::from(bytes[cursor]);
        cursor += 1;
        assert!(count > 0 && cursor + count < header_end);
        let name = std::str::from_utf8(&bytes[cursor..cursor + count])
            .unwrap()
            .to_owned();
        cursor += count;
        let kind = bytes[cursor];
        cursor += 1;
        let value = match kind {
            6 | 7 => {
                assert!(cursor + 2 <= header_end);
                let count =
                    u16::from_be_bytes(bytes[cursor..cursor + 2].try_into().unwrap()) as usize;
                cursor += 2;
                assert!(cursor + count <= header_end);
                let data = bytes[cursor..cursor + count].to_vec();
                cursor += count;
                if kind == 6 {
                    Header::Binary(data)
                } else {
                    Header::Text(String::from_utf8(data).unwrap())
                }
            }
            8 => {
                assert!(cursor + 8 <= header_end);
                let value = i64::from_be_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
                cursor += 8;
                Header::Timestamp(value)
            }
            _ => panic!("unexpected fixture header type {kind}"),
        };
        assert!(headers.insert(name, value).is_none(), "duplicate header");
    }
    assert_eq!(cursor, header_end);
    Parsed {
        headers,
        payload: bytes[header_end..bytes.len() - 4].to_vec(),
    }
}
fn text_header(name: &str, value: &str, output: &mut Vec<u8>) {
    output.push(u8::try_from(name.len()).unwrap());
    output.extend_from_slice(name.as_bytes());
    output.push(7);
    output.extend_from_slice(&u16::try_from(value.len()).unwrap().to_be_bytes());
    output.extend_from_slice(value.as_bytes());
}
pub fn frame(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for (name, value) in headers {
        text_header(name, value, &mut encoded);
    }
    let mut wire = Vec::new();
    wire.extend_from_slice(
        &u32::try_from(16 + encoded.len() + payload.len())
            .unwrap()
            .to_be_bytes(),
    );
    wire.extend_from_slice(&u32::try_from(encoded.len()).unwrap().to_be_bytes());
    wire.extend_from_slice(&crc(&wire).to_be_bytes());
    wire.extend_from_slice(&encoded);
    wire.extend_from_slice(payload);
    wire.extend_from_slice(&crc(&wire).to_be_bytes());
    wire
}
pub struct Verify {
    previous: String,
    signatures: Vec<String>,
    pub terminal: bool,
}
impl Verify {
    pub fn request<B>(request: &Request<B>, token: bool) -> Self {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.version(), http::Version::HTTP_2);
        assert_eq!(request.uri().path(), PATH);
        assert!(request.uri().query().is_none());
        let headers = request.headers();
        assert_eq!(headers["content-type"], MIME);
        for forbidden in ["connection", "transfer-encoding", "x-amz-content-sha256"] {
            assert!(!headers.contains_key(forbidden));
        }
        let host = request.uri().authority().unwrap().as_str();
        assert_eq!(headers["host"], host);
        let date = headers["x-amz-date"].to_str().unwrap();
        assert_eq!(date.len(), 16);
        let signed_headers = if token {
            "content-type;host;x-amz-date;x-amz-security-token;x-amzn-sagemaker-model-invocation-path;x-amzn-sagemaker-model-query-string"
        } else {
            "content-type;host;x-amz-date;x-amzn-sagemaker-model-invocation-path;x-amzn-sagemaker-model-query-string"
        };
        let mut canonical_headers =
            format!("content-type:{MIME}\nhost:{host}\nx-amz-date:{date}\n");
        if token {
            assert_eq!(headers["x-amz-security-token"], TOKEN);
            canonical_headers.push_str(&format!("x-amz-security-token:{TOKEN}\n"));
        } else {
            assert!(!headers.contains_key("x-amz-security-token"));
        }
        assert_eq!(
            headers["x-amzn-sagemaker-model-invocation-path"],
            "v1/listen"
        );
        assert_eq!(headers["x-amzn-sagemaker-model-query-string"], QUERY);
        canonical_headers.push_str(&format!("x-amzn-sagemaker-model-invocation-path:v1/listen\nx-amzn-sagemaker-model-query-string:{QUERY}\n"));
        // Fixed expected route has one wire encoding and a second canonical encoding.
        let canonical = format!("POST\n/endpoints/test-endpoint/invocations-bidirectional-stream\n\n{canonical_headers}\n{signed_headers}\n{}", sha(b""));
        let scope = format!("{}/us-east-1/sagemaker/aws4_request", &date[..8]);
        let to_sign = format!(
            "AWS4-HMAC-SHA256\n{date}\n{scope}\n{}",
            sha(canonical.as_bytes())
        );
        let expected = hex(&hmac(&signing_key(&date[..8]), to_sign.as_bytes()));
        assert_eq!(headers["authorization"], format!("AWS4-HMAC-SHA256 Credential={ACCESS}/{scope}, SignedHeaders={signed_headers}, Signature={expected}"));
        Self {
            previous: expected,
            signatures: Vec::new(),
            terminal: false,
        }
    }
    pub fn outer(&mut self, wire: &[u8]) -> Option<Value> {
        assert!(!self.terminal, "wire data after signed empty terminal");
        let outer = parse_frame(wire);
        assert_eq!(outer.headers.len(), 2);
        let Header::Timestamp(millis) = outer.headers[":date"] else {
            panic!("date must be a typed timestamp")
        };
        assert_eq!(
            millis % 1000,
            0,
            "SDK stream timestamps truncate subseconds"
        );
        let timestamp = DateTime::<Utc>::from_timestamp_millis(millis)
            .unwrap()
            .format("%Y%m%dT%H%M%SZ")
            .to_string();
        let Header::Binary(signature) = &outer.headers[":chunk-signature"] else {
            panic!("signature must be binary")
        };
        assert_eq!(signature.len(), 32);
        let mut date_header = vec![5];
        date_header.extend_from_slice(b":date");
        date_header.push(8);
        date_header.extend_from_slice(&millis.to_be_bytes());
        let to_sign = format!(
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{}/us-east-1/sagemaker/aws4_request\n{}\n{}\n{}",
            &timestamp[..8],
            self.previous,
            sha(&date_header),
            sha(&outer.payload)
        );
        assert_eq!(
            *signature,
            hmac(&signing_key(&timestamp[..8]), to_sign.as_bytes()),
            "independent chained signature"
        );
        self.previous = hex(signature);
        self.signatures.push(self.previous.clone());
        if outer.payload.is_empty() {
            self.terminal = true;
            return None;
        }
        let inner = parse_frame(&outer.payload);
        assert_eq!(
            inner.headers,
            BTreeMap::from([
                ("DataType".into(), inner.headers["DataType"].clone()),
                (":message-type".into(), Header::Text("event".into())),
                (":event-type".into(), Header::Text("PayloadPart".into())),
                (
                    ":content-type".into(),
                    Header::Text("application/octet-stream".into())
                ),
            ])
        );
        if inner.headers["DataType"] == Header::Text("BINARY".into()) {
            assert!(!inner.payload.is_empty());
            return Some(serde_json::json!({"binary": inner.payload}));
        }
        assert_eq!(inner.headers["DataType"], Header::Text("UTF8".into()));
        let value: Value = serde_json::from_slice(&inner.payload).unwrap();
        assert!(value.get("bytes").is_none());
        assert!(value.get("event").is_none());
        Some(value)
    }
}
pub fn event(value: Value) -> Vec<u8> {
    frame(
        &[
            (":message-type", "event"),
            (":event-type", "PayloadPart"),
            (":content-type", "application/octet-stream"),
            ("DataType", "UTF8"),
        ],
        &serde_json::to_vec(&value).unwrap(),
    )
}
pub struct Upload {
    body: RecvStream,
    pending: Vec<u8>,
    verify: Verify,
    pub events: Vec<Value>,
}
impl Upload {
    pub fn new(request: Request<RecvStream>) -> Self {
        let verify = Verify::request(&request, true);
        Self {
            body: request.into_body(),
            pending: vec![],
            verify,
            events: vec![],
        }
    }
    pub async fn next(&mut self) -> Option<Value> {
        loop {
            if self.pending.len() >= 12 {
                let size = u32_be(&self.pending[..4]) as usize;
                assert!((16..=1024 * 1024).contains(&size));
                if self.pending.len() >= size {
                    let wire: Vec<_> = self.pending.drain(..size).collect();
                    let value = self.verify.outer(&wire);
                    if let Some(v) = &value {
                        self.events.push(v.clone());
                    }
                    return value;
                }
            }
            let bytes = self
                .body
                .data()
                .await
                .expect("upload before terminal")
                .expect("valid upload");
            assert!(self.pending.len() + bytes.len() <= 2 * 1024 * 1024);
            self.pending.extend_from_slice(&bytes);
            self.body
                .flow_control()
                .release_capacity(bytes.len())
                .unwrap();
        }
    }
    pub async fn finish(&mut self) {
        while self.next().await.is_some() {}
        assert!(self.verify.terminal);
        assert!(self.pending.is_empty());
        assert!(self.body.data().await.is_none());
    }
}
pub async fn send(stream: &mut SendStream<Bytes>, wire: &[u8], end: bool, fragment: usize) {
    if wire.is_empty() {
        stream.send_data(Bytes::new(), end).unwrap();
        return;
    }
    for (index, chunk) in wire.chunks(fragment).enumerate() {
        let mut bytes = Bytes::copy_from_slice(chunk);
        while !bytes.is_empty() {
            stream.reserve_capacity(bytes.len());
            let capacity = poll_fn(|cx| {
                if stream.capacity() > 0 {
                    std::task::Poll::Ready(Some(Ok(stream.capacity())))
                } else {
                    stream.poll_capacity(cx)
                }
            })
            .await
            .unwrap()
            .unwrap();
            let count = capacity.min(bytes.len());
            let part = bytes.split_to(count);
            stream
                .send_data(
                    part,
                    end && bytes.is_empty() && (index + 1) * fragment >= wire.len(),
                )
                .unwrap();
        }
    }
}
pub async fn peer<F, Fut>(action: F) -> (String, JoinHandle<()>)
where
    F: FnOnce(Request<RecvStream>, server::SendResponse<Bytes>) -> Fut + Send + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        drop(listener);
        let mut builder = server::Builder::new();
        builder
            .initial_window_size(1024)
            .initial_connection_window_size(1024);
        let mut connection = builder.handshake(socket).await.unwrap();
        let (request, response) = connection.accept().await.unwrap().unwrap();
        let action = action(request, response);
        tokio::pin!(action);
        let (mut done, mut closed) = (false, false);
        while !done || !closed {
            tokio::select! {_= &mut action,if !done=>done=true,next=connection.accept(),if !closed=>match next{Some(Ok(_))=>panic!("no retry or second request"),_=>closed=true}}
        }
    });
    (origin, task)
}
pub fn response() -> Response<()> {
    Response::builder()
        .status(200)
        .header("content-type", MIME)
        .body(())
        .unwrap()
}
pub fn config(origin: &str) -> pipecat_aws_sagemaker_deepgram_stt::SageMakerSttConfig {
    use pipecat_aws_sagemaker_deepgram_stt::{DuplexLimits, SageMakerSttConfig, SttLimits};
    SageMakerSttConfig::new(
        "us-east-1",
        "test-endpoint",
        pipecat_aws_common::AwsCredentials::new(ACCESS, SECRET, Some(TOKEN.into())).unwrap(),
    )
    .unwrap()
    .with_loopback_origin(origin)
    .unwrap()
    .with_limits(SttLimits {
        duplex: DuplexLimits::default(),
        output_timeout: Duration::from_secs(1),
        ..Default::default()
    })
    .unwrap()
}
#[derive(Default)]
pub struct Output {
    pub frames: Mutex<Vec<pipecat_core::FrameData>>,
    pub seen: tokio::sync::Notify,
    pub block: bool,
    pub panic: bool,
    pub cleanup_error: bool,
    pub setup_error: bool,
    pub setup_block: bool,
}
#[pipecat_core::async_trait]
impl pipecat_core::FrameProcessor for Output {
    fn name(&self) -> &str {
        "FixtureDetailedOutput"
    }
    async fn setup(
        &self,
        _config: &pipecat_core::StartFrame,
        _ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        if self.setup_block {
            std::future::pending::<()>().await;
        }
        if self.setup_error {
            return Err(pipecat_core::ProcessorError::new("injected setup failure"));
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: pipecat_core::Frame,
        direction: pipecat_core::Direction,
        ctx: &pipecat_core::ProcessorContext,
    ) -> Result<(), pipecat_core::ProcessorError> {
        if matches!(
            &frame.data,
            pipecat_core::FrameData::TranscriptionDetailed(_)
                | pipecat_core::FrameData::InterimTranscriptionDetailed(_)
        ) {
            assert!(!self.panic, "injected output panic");
            if self.block {
                std::future::pending::<()>().await;
            }
            self.frames.lock().unwrap().push(frame.data.clone());
            self.seen.notify_one();
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), pipecat_core::ProcessorError> {
        if self.cleanup_error {
            Err(pipecat_core::ProcessorError::new(
                "injected cleanup failure",
            ))
        } else {
            Ok(())
        }
    }
}
pub fn assert_joined(report: &pipecat_aws_sagemaker_deepgram_stt::Report) {
    assert!(report.pipeline_joined, "{report:?}");
    let t = report.transport.as_ref().unwrap();
    assert!(
        t.reader_joined && t.writer_joined && t.connection_joined,
        "{report:?}"
    );
}
pub fn assert_pcm(events: &[Value], pcm: &[u8]) {
    assert_eq!(events.last().unwrap()["type"], "CloseStream");
    let mut received = vec![];
    for value in events {
        if let Some(bytes) = value.get("binary") {
            received.extend(
                bytes
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|v| v.as_u64().unwrap() as u8),
            );
        } else {
            assert!(matches!(
                value["type"].as_str(),
                Some("KeepAlive" | "Finalize" | "CloseStream")
            ));
        }
    }
    assert_eq!(received, pcm);
}
pub fn result(text: &str, is_final: bool, from_finalize: bool) -> Value {
    serde_json::json!({"channel":{"alternatives":[{"transcript":text,"languages":["en-US"]}]},"is_final":is_final,"from_finalize":from_finalize})
}

pub fn invalid_datatype_event() -> Vec<u8> {
    let mut wire = event(serde_json::json!({"type":"unknown"}));
    let pos = wire
        .windows(8)
        .position(|part| part == b"DataType")
        .unwrap();
    assert_eq!(wire[pos + 8], 7);
    wire[pos + 8] = 6; // Correct CRC, wrong typed header.
    let end = wire.len() - 4;
    let checksum = crc(&wire[..end]);
    wire[end..].copy_from_slice(&checksum.to_be_bytes());
    wire
}
