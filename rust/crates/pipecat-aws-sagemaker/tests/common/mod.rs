//! Independent literal-loopback fixture. No provider request, signature,
//! transport, PCM decoder or app-internal helper is used here.
#![allow(dead_code)]
use hmac::{Hmac, Mac};
use pipecat_aws_common::AwsCredentials;
use pipecat_aws_sagemaker::{SageMakerConfig, SageMakerLimits, SynthesisSettings, Utterance};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
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

pub const ENDPOINT: &str = "Test-Endpoint";
pub const ROUTE: &str = "/endpoints/Test-Endpoint/invocations";
pub const ACCESS: &str = "SAGEMAKERFIXTUREKEY";
pub const SECRET: &str = "synthetic-sagemaker-test-secret-not-real";
pub const TOKEN: &str = "synthetic-sagemaker-test-token-not-real";
pub const WAIT: Duration = Duration::from_secs(8);

pub async fn listener() -> TcpListener {
    // Keep this listener until the actual peer runs; never release and rebind.
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
pub fn origin(listener: &TcpListener) -> String {
    format!("http://{}", listener.local_addr().unwrap())
}
pub fn credentials() -> AwsCredentials {
    AwsCredentials::new(ACCESS, SECRET, Some(TOKEN.into())).unwrap()
}
pub fn limits() -> SageMakerLimits {
    SageMakerLimits {
        connect_timeout: Duration::from_secs(2),
        write_timeout: Duration::from_secs(3),
        idle_timeout: Duration::from_secs(3),
        output_timeout: Duration::from_millis(700),
        total_timeout: Duration::from_secs(6),
        shutdown_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}
pub fn config(listener: &TcpListener) -> SageMakerConfig {
    config_with_credentials(listener, ACCESS, SECRET, Some(TOKEN))
}
pub fn config_with_credentials(
    listener: &TcpListener,
    access: &str,
    secret: &str,
    token: Option<&str>,
) -> SageMakerConfig {
    SageMakerConfig::new(
        "us-east-1",
        ENDPOINT,
        AwsCredentials::new(access, secret, token.map(str::to_owned)).unwrap(),
    )
    .unwrap()
    .with_loopback_endpoint(&origin(listener))
    .unwrap()
    .with_limits(limits())
    .unwrap()
}
pub fn utterance(text: &str) -> Utterance {
    Utterance::new(text)
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
        assert!(size <= 6_291_456, "AWS BodyBlob bound exceeded");
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
pub async fn content_length_response(stream: &mut TcpStream, body: &[u8]) {
    let mut wire = format!("HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).into_bytes();
    wire.extend_from_slice(body);
    stream.write_all(&wire).await.unwrap();
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
        "unexpected retry or overlapping invocation"
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
/// Reconstruct SigV4 from captured wire bytes, independently of the shared signer.
pub fn signature_valid(request: &Request, secret: &str) -> bool {
    let fields: BTreeMap<_, _> = request.headers["authorization"]
        .strip_prefix("AWS4-HMAC-SHA256 ")
        .unwrap()
        .split(", ")
        .map(|s| s.split_once('=').unwrap())
        .collect();
    let credential: Vec<_> = fields["Credential"].split('/').collect();
    assert_eq!(credential.len(), 5);
    let signed = fields["SignedHeaders"];
    let names: Vec<_> = signed.split(';').collect();
    assert!(names.windows(2).all(|pair| pair[0] < pair[1]));
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
    let route = request.route.split_whitespace().nth(1).unwrap();
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
    text: &str,
    settings: &SynthesisSettings,
    access: &str,
    secret: &str,
    token: Option<&str>,
) {
    assert_eq!(request.route, format!("POST {ROUTE} HTTP/1.1"));
    assert_eq!(request.headers["host"], host);
    assert_eq!(request.headers["content-type"], "application/json");
    assert_eq!(request.headers["accept"], "application/octet-stream");
    assert_eq!(request.headers["connection"], "close");
    assert!(!request.headers.contains_key("x-amzn-sagemaker-accept"));
    assert!(!request.headers.contains_key("x-amzn-sagemaker-session-id"));
    assert_eq!(
        serde_json::from_slice::<serde_json::Value>(&request.body).unwrap(),
        serde_json::json!({
            "text": text, "voice_name": settings.voice_name, "language_code": settings.language_code, "sample_rate_hz": settings.sample_rate_hz
        })
    );
    assert_eq!(
        request
            .headers
            .get("x-amz-security-token")
            .map(String::as_str),
        token
    );
    let auth = &request.headers["authorization"];
    assert!(auth.contains(&format!("Credential={access}/")));
    assert!(auth.contains("/us-east-1/sagemaker/aws4_request"));
    assert!(!auth.contains("sagemaker-runtime"));
    let signed = auth
        .split("SignedHeaders=")
        .nth(1)
        .unwrap()
        .split(',')
        .next()
        .unwrap();
    for required in ["accept", "content-type", "host", "x-amz-date"] {
        assert!(signed.split(';').any(|name| name == required));
    }
    if token.is_some() {
        assert!(signed.split(';').any(|name| name == "x-amz-security-token"));
    }
    let date = &request.headers["x-amz-date"];
    assert_eq!(date.len(), 16);
    assert_eq!(&date[8..9], "T");
    assert_eq!(&date[15..], "Z");
    assert!(signature_valid(request, secret));
    for field in ["body", "route", "accept", "host", "x-amz-date"] {
        let mut edited = request.clone();
        match field {
            "body" => edited.body.push(b' '),
            "route" => edited.route = edited.route.replace("/invocations", "/other"),
            "accept" => {
                edited
                    .headers
                    .insert(field.into(), "application/json".into());
            }
            "host" => {
                edited
                    .headers
                    .insert(field.into(), "different.invalid".into());
            }
            _ => {
                edited
                    .headers
                    .insert(field.into(), "20200101T000000Z".into());
            }
        }
        assert!(
            !signature_valid(&edited, secret),
            "tampering {field} went undetected"
        );
    }
    if token.is_some() {
        let mut edited = request.clone();
        edited
            .headers
            .insert("x-amz-security-token".into(), "different-token".into());
        assert!(!signature_valid(&edited, secret));
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Start(Option<String>),
    Audio {
        context: Option<String>,
        rate: u32,
        channels: u16,
        bytes: Vec<u8>,
    },
    Stop(Option<String>),
    Text(String),
}
#[derive(Default)]
pub struct Recording {
    pub events: Mutex<Vec<Event>>,
    pub append_flags: Mutex<Vec<bool>>,
    pub changed: Notify,
}
impl Recording {
    pub fn snapshot(&self) -> Vec<Event> {
        self.events.lock().unwrap().clone()
    }
    pub fn pcm(&self) -> Vec<u8> {
        self.snapshot()
            .into_iter()
            .filter_map(|event| match event {
                Event::Audio { bytes, .. } => Some(bytes),
                _ => None,
            })
            .flatten()
            .collect()
    }
    pub async fn wait_pcm(&self, count: usize) {
        timeout(WAIT, async {
            loop {
                let changed = self.changed.notified();
                if self.pcm().len() >= count {
                    return;
                }
                changed.await;
            }
        })
        .await
        .expect("PCM output was not observed");
    }
    pub fn assert_complete(&self, expected: &[u8], rate: u32, utterances: usize) {
        assert_eq!(self.pcm(), expected);
        let mut active = None;
        let mut completed = Vec::new();
        for event in self.snapshot() {
            match event {
                Event::Start(context) => {
                    assert!(active.is_none(), "overlapping TTS contexts");
                    let context = context.expect("TTS start has no context");
                    assert!(!completed.contains(&context), "reused completed context");
                    active = Some(context);
                }
                Event::Audio {
                    context,
                    rate: actual,
                    channels,
                    bytes,
                } => {
                    assert!(active.is_some(), "audio outside started context");
                    assert_eq!(context, active);
                    assert_eq!(actual, rate);
                    assert_eq!(channels, 1);
                    assert!(!bytes.is_empty());
                    assert_eq!(bytes.len() % 2, 0);
                }
                Event::Stop(context) => {
                    assert!(active.is_some(), "stop without start");
                    assert_eq!(context, active);
                    completed.push(active.take().unwrap());
                }
                Event::Text(_) => {}
            }
        }
        assert!(active.is_none(), "context missing final stop");
        assert_eq!(completed.len(), utterances);
    }
}
#[async_trait]
impl FrameProcessor for Recording {
    fn name(&self) -> &str {
        "IndependentSageMakerRecording"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            let event = match &frame.data {
                FrameData::TtsStarted(start) => {
                    self.append_flags
                        .lock()
                        .unwrap()
                        .push(start.append_to_context);
                    Some(Event::Start(start.context_id.clone()))
                }
                FrameData::TtsAudio(audio) => Some(Event::Audio {
                    context: audio.context_id.clone(),
                    rate: audio.sample_rate,
                    channels: audio.num_channels,
                    bytes: audio.audio.clone(),
                }),
                FrameData::TtsStopped(stop) => Some(Event::Stop(stop.context_id.clone())),
                FrameData::Text(text) => Some(Event::Text(text.text.clone())),
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
