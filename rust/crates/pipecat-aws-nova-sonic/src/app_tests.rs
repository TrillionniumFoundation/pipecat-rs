//! Acceptance through the production app, an independently authenticated h2 peer,
//! the real standard history pair, and (for interruption) real OutputTransport.
//! The fixture implements its own EventStream CRC/header codec and HMAC chain.
//! No production signing/framing/mapper helper is used to accept input or emit output.
use super::*;
use bytes::Bytes;
use chrono::{DateTime, Utc};
use h2::{server, RecvStream, SendStream};
use hmac::{Hmac, Mac};
use http::{Request, Response};
use pipecat_audio::PcmFormat;
use pipecat_aws_common::AwsCredentials;
use pipecat_core::{AudioRawFrame, LlmContextFrame, ProcessorHealth, TextFrame, WorkerState};
use pipecat_media_transport::{
    OutputBackend, OutputTransport, OutputTransportConfig, PlaybackReceipt,
};
use pipecat_services::{Message, Role, ToolChoice, ToolDefinition};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, future::poll_fn, path::PathBuf, process::Command};
use tokio::{
    net::TcpListener,
    sync::{mpsc, oneshot, Notify, Semaphore},
    task::JoinHandle,
    time::timeout,
};

const ACCESS: &str = "NOVAFIXTUREKEY";
const SECRET: &str = "synthetic-nova-acceptance-secret-not-a-real-key";
const TOKEN: &str = "synthetic-nova-acceptance-token";
const MIME: &str = "application/vnd.amazon.eventstream";
const PATH: &str = "/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream";
const LIMIT: Duration = Duration::from_secs(12);

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
    let service = hmac(&region, b"bedrock");
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
#[derive(Debug, PartialEq)]
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
fn frame(headers: &[(&str, &str)], payload: &[u8]) -> Vec<u8> {
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
fn chunk(value: &Value) -> Vec<u8> {
    let blob =
        serde_json::to_vec(&json!({"bytes":STANDARD.encode(serde_json::to_vec(value).unwrap())}))
            .unwrap();
    frame(
        &[
            (":message-type", "event"),
            (":event-type", "chunk"),
            (":content-type", "application/json"),
        ],
        &blob,
    )
}
fn canonical_base64(value: &str) -> Vec<u8> {
    let decoded = STANDARD.decode(value).unwrap();
    assert_eq!(STANDARD.encode(&decoded), value, "noncanonical blob");
    decoded
}
struct Verify {
    previous: String,
    signatures: Vec<String>,
    terminal: bool,
}
impl Verify {
    fn request(request: &Request<RecvStream>, token: bool) -> Self {
        assert_eq!(request.method(), "POST");
        assert_eq!(request.version(), http::Version::HTTP_2);
        assert_eq!(request.uri().path(), PATH);
        assert!(request.uri().query().is_none());
        assert!(
            !request.body().is_end_stream(),
            "headers prematurely closed upload"
        );
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
            "content-type;host;x-amz-date;x-amz-security-token"
        } else {
            "content-type;host;x-amz-date"
        };
        let mut canonical_headers =
            format!("content-type:{MIME}\nhost:{host}\nx-amz-date:{date}\n");
        if token {
            assert_eq!(headers["x-amz-security-token"], TOKEN);
            canonical_headers.push_str(&format!("x-amz-security-token:{TOKEN}\n"));
        } else {
            assert!(!headers.contains_key("x-amz-security-token"));
        }
        // Fixed expected route has one wire encoding and a second canonical encoding.
        let canonical = format!("POST\n/model/amazon.nova-2-sonic-v1%253A0/invoke-with-bidirectional-stream\n\n{canonical_headers}\n{signed_headers}\n{}", sha(b""));
        let scope = format!("{}/us-east-1/bedrock/aws4_request", &date[..8]);
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
    fn outer(&mut self, wire: &[u8]) -> Option<Value> {
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
            "AWS4-HMAC-SHA256-PAYLOAD\n{timestamp}\n{}/us-east-1/bedrock/aws4_request\n{}\n{}\n{}",
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
                (":message-type".into(), Header::Text("event".into())),
                (":event-type".into(), Header::Text("chunk".into())),
                (
                    ":content-type".into(),
                    Header::Text("application/json".into())
                ),
            ])
        );
        let blob: Value = serde_json::from_slice(&inner.payload).unwrap();
        assert_eq!(blob.as_object().unwrap().len(), 1);
        let value: Value =
            serde_json::from_slice(&canonical_base64(blob["bytes"].as_str().unwrap())).unwrap();
        assert_eq!(value.as_object().unwrap().len(), 1);
        assert_eq!(value["event"].as_object().unwrap().len(), 1);
        if let Some(audio) = value["event"].get("audioInput") {
            let pcm = canonical_base64(audio["content"].as_str().unwrap());
            assert!(!pcm.is_empty() && pcm.len().is_multiple_of(2));
        }
        Some(value)
    }
}
struct Upload {
    body: RecvStream,
    pending: Vec<u8>,
    verify: Verify,
    events: Vec<Value>,
    stage: &'static str,
    trace: Arc<Mutex<Vec<String>>>,
}
fn record_upload_trace(trace: &Mutex<Vec<String>>, entry: String) {
    let mut trace = trace.lock().unwrap();
    // Synthetic fixture diagnostics retain counts/credit/stages only, never
    // request headers, credentials, content identifiers, transcripts or PCM.
    if trace.len() < 512 {
        trace.push(entry);
    }
}
enum UploadItem {
    Event(Value),
    Terminal,
    Eof,
    Reset,
}
impl Upload {
    async fn next(&mut self) -> UploadItem {
        loop {
            if self.pending.len() >= 12 {
                assert_eq!(u32_be(&self.pending[8..12]), crc(&self.pending[..8]));
                let count = u32_be(&self.pending[..4]) as usize;
                assert!((16..=1024 * 1024).contains(&count));
                if self.pending.len() >= count {
                    let wire: Vec<u8> = self.pending.drain(..count).collect();
                    return match self.verify.outer(&wire) {
                        Some(value) => {
                            self.events.push(value.clone());
                            let kind = match value["event"]
                                .as_object()
                                .unwrap()
                                .keys()
                                .next()
                                .unwrap()
                                .as_str()
                            {
                                "sessionStart" => "sessionStart",
                                "promptStart" => "promptStart",
                                "contentStart" => "contentStart",
                                "textInput" => "textInput",
                                "audioInput" => "audioInput",
                                "contentEnd" => "contentEnd",
                                "promptEnd" => "promptEnd",
                                "sessionEnd" => "sessionEnd",
                                _ => "unknown",
                            };
                            record_upload_trace(
                                &self.trace,
                                format!(
                                    "stage={} decoded={} event_count={} pending_bytes={}",
                                    self.stage,
                                    kind,
                                    self.events.len(),
                                    self.pending.len(),
                                ),
                            );
                            UploadItem::Event(value)
                        }
                        None => {
                            record_upload_trace(
                                &self.trace,
                                format!(
                                    "stage={} signed_terminal pending_bytes={}",
                                    self.stage,
                                    self.pending.len()
                                ),
                            );
                            UploadItem::Terminal
                        }
                    };
                }
            }
            let flow = self.body.flow_control();
            record_upload_trace(&self.trace, format!(
                "stage={} awaiting_DATA available={} used={} pending_bytes={} decoded_events={}",
                self.stage, flow.available_capacity(), flow.used_capacity(), self.pending.len(), self.events.len(),
            ));
            match self.body.data().await {
                Some(Ok(bytes)) => {
                    assert!(self.pending.len() + bytes.len() <= 2 * 1024 * 1024);
                    self.pending.extend_from_slice(&bytes);
                    let flow = self.body.flow_control();
                    let before = (flow.available_capacity(), flow.used_capacity());
                    flow.release_capacity(bytes.len()).unwrap();
                    let after = (flow.available_capacity(), flow.used_capacity());
                    record_upload_trace(&self.trace, format!(
                        "stage={} DATA bytes={} release_bytes={} available_before={} used_before={} available_after={} used_after={} pending_bytes={} decoded_events={}",
                        self.stage, bytes.len(), bytes.len(), before.0, before.1, after.0, after.1, self.pending.len(), self.events.len(),
                    ));
                }
                Some(Err(_)) => {
                    let flow = self.body.flow_control();
                    record_upload_trace(&self.trace, format!("stage={} RESET available={} used={} pending_bytes={} decoded_events={}", self.stage, flow.available_capacity(), flow.used_capacity(), self.pending.len(), self.events.len()));
                    return UploadItem::Reset;
                }
                None => {
                    record_upload_trace(
                        &self.trace,
                        format!(
                            "stage={} EOF pending_bytes={} decoded_events={}",
                            self.stage,
                            self.pending.len(),
                            self.events.len()
                        ),
                    );
                    assert!(self.pending.is_empty(), "truncated request body");
                    return UploadItem::Eof;
                }
            }
        }
    }
}
async fn send_wire(stream: &mut SendStream<Bytes>, wire: &[u8], end: bool) {
    if wire.is_empty() {
        stream.send_data(Bytes::new(), end).unwrap();
        return;
    }
    // Deliberate segmentation across prelude, header, payload, and CRC, with later
    // chunks coalescing several complete frames. h2 flow control remains real.
    let mut remaining = Bytes::copy_from_slice(wire);
    let mut segment = 0;
    let cuts = [1, 3, 4, 1, 3, 7, 19, 4096];
    while !remaining.is_empty() {
        let wanted = remaining.len().min(cuts[segment.min(cuts.len() - 1)]);
        stream.reserve_capacity(wanted);
        // poll_capacity reports increases, not all already available capacity.
        // Consume an existing positive grant before waiting for a notification.
        let available = stream.capacity();
        let capacity = if available > 0 {
            available
        } else {
            poll_fn(|cx| stream.poll_capacity(cx))
                .await
                .unwrap()
                .unwrap()
        };
        // A capacity notification may report zero after previously reserved
        // capacity was consumed. Keep the same unsent bytes and wait again.
        if capacity == 0 {
            continue;
        }
        let part = remaining.split_to(wanted.min(capacity));
        stream.send_data(part, end && remaining.is_empty()).unwrap();
        segment += 1;
    }
}
#[derive(Clone)]
struct Nova {
    prompt: String,
    session: String,
    completion: String,
}
impl Nova {
    fn new(prompt: String) -> Self {
        Self {
            session: format!("session-{prompt}"),
            completion: format!("completion-{prompt}"),
            prompt,
        }
    }
    fn event(&self, name: &str, extra: Value) -> Value {
        let mut body = json!({"sessionId":self.session,"promptName":self.prompt,"completionId":self.completion});
        body.as_object_mut()
            .unwrap()
            .extend(extra.as_object().unwrap().clone());
        json!({"event":{name:body}})
    }
    fn text(&self, id: &str, role: &str, stage: &str, text: &str, reason: &str) -> Vec<Value> {
        vec![
            self.event("contentStart",json!({"contentId":id,"type":"TEXT","role":role,"additionalModelFields":format!("{{\"generationStage\":\"{stage}\"}}"),"textOutputConfiguration":{"mediaType":"text/plain"}})),
            self.event("textOutput",json!({"contentId":id,"content":text})),
            self.event("contentEnd",json!({"contentId":id,"type":"TEXT","stopReason":reason})),
        ]
    }
    fn audio(&self, id: &str, pcm: &[u8], reason: &str) -> Vec<Value> {
        vec![
            self.event("contentStart",json!({"contentId":id,"type":"AUDIO","role":"ASSISTANT","audioOutputConfiguration":{"mediaType":"audio/lpcm","encoding":"base64","sampleRateHertz":24000,"sampleSizeBits":16,"channelCount":1,"audioType":"SPEECH"}})),
            self.event("audioOutput",json!({"contentId":id,"content":STANDARD.encode(pcm)})),
            self.event("contentEnd",json!({"contentId":id,"type":"AUDIO","stopReason":reason})),
        ]
    }
    fn turn(&self, number: usize, user: &[&str], generated: &str, pcm: &[u8]) -> Vec<Value> {
        let mut events = Vec::new();
        for (index, text) in user.iter().enumerate() {
            events.extend(self.text(
                &format!("user-{number}-{index}"),
                "USER",
                "FINAL",
                text,
                "END_TURN",
            ));
        }
        events.extend(self.text(
            &format!("spec-{number}"),
            "ASSISTANT",
            "SPECULATIVE",
            generated,
            "PARTIAL_TURN",
        ));
        events.extend(self.audio(&format!("audio-{number}"), pcm, "END_TURN"));
        events.extend(self.text(
            &format!("final-{number}"),
            "ASSISTANT",
            "FINAL",
            "provider FINAL is not generated history",
            "END_TURN",
        ));
        events
    }
}
enum PeerCommand {
    Send(Vec<u8>, oneshot::Sender<()>),
    Finish(Vec<Value>),
}
struct PeerReady {
    nova: Nova,
    startup: Vec<Value>,
}
struct PeerReport {
    events: Vec<Value>,
    signatures: Vec<String>,
    terminal: bool,
    eof: bool,
    reset: bool,
}
struct Peer {
    endpoint: String,
    ready: Option<oneshot::Receiver<PeerReady>>,
    commands: mpsc::Sender<PeerCommand>,
    uploaded: mpsc::Receiver<Value>,
    trace: Arc<Mutex<Vec<String>>>,
    task: JoinHandle<PeerReport>,
}
impl Peer {
    async fn new(token: bool) -> Self {
        Self::status(token, 200).await
    }
    async fn status(token: bool, status: u16) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}{PATH}", listener.local_addr().unwrap());
        let (ready_tx, ready_rx) = oneshot::channel();
        let (commands, mut control) = mpsc::channel(16);
        let (observed, uploaded) = mpsc::channel(128);
        let trace = Arc::new(Mutex::new(Vec::new()));
        let upload_trace = trace.clone();
        let task = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            drop(listener);
            let mut builder = server::Builder::new();
            builder
                .initial_window_size(4096)
                .initial_connection_window_size(8192);
            let mut connection = builder.handshake(socket).await.unwrap();
            let (request, mut respond) = connection.accept().await.unwrap().unwrap();
            let verify = Verify::request(&request, token);
            let mut input = Upload {
                body: request.into_body(),
                pending: Vec::new(),
                verify,
                events: Vec::new(),
                stage: "startup",
                trace: upload_trace,
            };
            let action = async move {
                // Crucial duplex gate: no HTTP response head until signed startup
                // AUDIO contentStart has actually arrived and been authenticated.
                let prompt = loop {
                    match input.next().await {
                        UploadItem::Event(event)
                            if event["event"]["contentStart"]["type"] == "AUDIO" =>
                        {
                            break event["event"]["contentStart"]["promptName"]
                                .as_str()
                                .unwrap()
                                .to_owned()
                        }
                        UploadItem::Event(_) => {}
                        _ => panic!("startup did not reach input AUDIO content"),
                    }
                };
                record_upload_trace(
                    &input.trace,
                    format!(
                        "startup-complete decoded_events={} pending_bytes={}",
                        input.events.len(),
                        input.pending.len()
                    ),
                );
                input.stage = "live";
                let nova = Nova::new(prompt);
                let response = Response::builder()
                    .status(status)
                    .header("content-type", MIME)
                    .body(())
                    .unwrap();
                let mut output = respond.send_response(response, false).unwrap();
                let _ = ready_tx.send(PeerReady {
                    nova: nova.clone(),
                    startup: input.events.clone(),
                });
                let mut finish = None;
                let mut eof = false;
                let mut reset = false;
                loop {
                    if eof && finish.is_some() {
                        let mut events: Vec<Value> = finish.take().unwrap();
                        events.push(nova.event("completionEnd", json!({"stopReason":"END_TURN"})));
                        let wire: Vec<u8> = events.iter().flat_map(chunk).collect();
                        send_wire(&mut output, &wire, true).await;
                        break;
                    }
                    tokio::select! {
                        message = control.recv() => match message {
                            Some(PeerCommand::Send(wire, ack)) => { send_wire(&mut output,&wire,false).await; let _=ack.send(()); },
                            Some(PeerCommand::Finish(events)) => { assert!(finish.replace(events).is_none()); },
                            None => { output.send_reset(h2::Reason::CANCEL); reset=true; break; },
                        },
                        event = input.next(), if !eof => match event {
                            UploadItem::Event(value) => observed.send(value).await.unwrap(),
                            UploadItem::Terminal => {},
                            UploadItem::Eof => { assert!(input.verify.terminal, "EOF requires signed empty terminal"); eof=true; },
                            UploadItem::Reset => { reset=true; break; },
                        }
                    }
                }
                drop(output);
                PeerReport {
                    events: input.events,
                    signatures: input.verify.signatures,
                    terminal: input.verify.terminal,
                    eof,
                    reset,
                }
            };
            tokio::pin!(action);
            let mut result = None;
            let mut connection_done = false;
            while result.is_none() || !connection_done {
                tokio::select! {
                    report = &mut action, if result.is_none() => result=Some(report),
                    request = connection.accept(), if !connection_done => match request {
                        Some(Ok(_)) => panic!("unexpected second request/retry"),
                        Some(Err(_)) | None => connection_done=true,
                    },
                }
            }
            result.unwrap()
        });
        Self {
            endpoint,
            ready: Some(ready_rx),
            commands,
            uploaded,
            trace,
            task,
        }
    }
    async fn ready(&mut self) -> PeerReady {
        timeout(LIMIT, self.ready.take().unwrap())
            .await
            .unwrap()
            .unwrap()
    }
    async fn send(&self, events: Vec<Value>) {
        self.raw(events.iter().flat_map(chunk).collect()).await;
    }
    async fn raw(&self, wire: Vec<u8>) {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(PeerCommand::Send(wire, tx))
            .await
            .unwrap();
        timeout(LIMIT, rx).await.unwrap().unwrap();
    }
    async fn finish(&self, trailing: Vec<Value>) {
        self.commands
            .send(PeerCommand::Finish(trailing))
            .await
            .unwrap();
    }
    async fn audio(&mut self) -> Vec<u8> {
        let event = timeout(LIMIT, self.uploaded.recv()).await.unwrap().unwrap();
        let body = &event["event"]["audioInput"];
        assert!(body.is_object(), "unexpected post-startup event before PCM");
        canonical_base64(body["content"].as_str().unwrap())
    }
    async fn audio_from_app(&mut self, app: &NovaSonicRunningApp, stage: &str) -> Vec<u8> {
        match timeout(LIMIT, self.uploaded.recv()).await.unwrap() {
            Some(event) => {
                let body = &event["event"]["audioInput"];
                assert!(body.is_object(), "unexpected post-startup event at {stage}");
                canonical_base64(body["content"].as_str().unwrap())
            }
            None => {
                let provider = timeout(LIMIT, app.provider.wait_joined()).await;
                panic!(
                    "peer upload closed at {stage}: worker={:?}, errors={:?}, provider={provider:?}; synthetic_upload_flow={:?}",
                    app.worker.state(), app.worker.errors(), self.trace.lock().unwrap(),
                );
            }
        }
    }
    async fn join(self) -> PeerReport {
        // Keep command sender alive until the peer's actual connection future
        // has returned, otherwise dropping it would itself reset the fixture.
        let result = timeout(LIMIT, self.task).await.unwrap().unwrap();
        drop(self.commands);
        result
    }
}
fn config(endpoint: &str, token: bool) -> NovaSonicConfig {
    let mut config = NovaSonicConfig::new(
        "us-east-1",
        "amazon.nova-2-sonic-v1:0",
        "tiffany",
        AwsCredentials::new(ACCESS, SECRET, token.then(|| TOKEN.to_owned())).unwrap(),
    )
    .unwrap();
    config.endpoint = Some(endpoint.to_owned());
    config.limits.session_timeout = Duration::from_secs(30);
    config.limits.connect_timeout = Duration::from_secs(3);
    config.limits.operation_timeout = Duration::from_secs(3);
    config.limits.response_timeout = Duration::from_secs(8);
    config.limits.consumer_timeout = Duration::from_secs(3);
    config.limits.drain_timeout = Duration::from_secs(5);
    config.limits.close_timeout = Duration::from_secs(2);
    config
}
#[derive(Default)]
struct Sink {
    frames: Mutex<Vec<FrameData>>,
    changed: Notify,
    fail_audio: bool,
    fail_cleanup: bool,
}
#[async_trait]
impl FrameProcessor for Sink {
    fn name(&self) -> &str {
        "AcceptancePcmSink"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            if self.fail_audio && matches!(frame.data, FrameData::TtsAudio(_)) {
                return Err(ProcessorError::fatal("fixture output write failure"));
            }
            self.frames.lock().unwrap().push(frame.data.clone());
            self.changed.notify_waiters();
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.fail_cleanup {
            Err(ProcessorError::fatal("fixture cleanup failure"))
        } else {
            Ok(())
        }
    }
}
impl Sink {
    fn pcm(&self) -> Vec<u8> {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter_map(|f| {
                if let FrameData::TtsAudio(a) = f {
                    assert_eq!((a.sample_rate, a.num_channels), (24000, 1));
                    Some(a.audio.clone())
                } else {
                    None
                }
            })
            .flatten()
            .collect()
    }
    async fn wait_pcm(&self, count: usize) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                if self.pcm().len() >= count {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
    async fn wait_responses(&self, count: usize) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                if self
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|f| matches!(f, FrameData::LlmResponseEnd))
                    .count()
                    >= count
                {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
    async fn wait_usage(&self, count: usize) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                let observed = self
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .filter(|frame| {
                        matches!(frame, FrameData::Metrics(metrics) if metrics.iter().any(
                            |metric| matches!(metric, pipecat_core::Metric::LlmUsage { processor, .. } if processor == "NovaSonic")
                        ))
                    })
                    .count();
                if observed >= count {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
async fn pcm(app: &NovaSonicRunningApp, bytes: &[u8]) {
    app.worker
        .sender()
        .send(
            Frame::new(FrameData::InputAudio(
                AudioRawFrame::new(bytes.to_vec(), 16000, 1).unwrap(),
            )),
            Direction::Downstream,
        )
        .await
        .unwrap();
}
async fn end_clean(app: &mut NovaSonicRunningApp) {
    let sender = app.worker.sender();
    let (end, report) = timeout(LIMIT, async {
        tokio::join!(sender.end(), async {
            while app.worker.recv().await.is_some() {}
            app.worker.join().await
        })
    })
    .await
    .unwrap();
    end.unwrap();
    let report = report.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty(), "{report:?}");
    assert_eq!(report.total_errors, 0);
    assert_eq!(report.evicted_errors, 0);
    assert_eq!(report.dropped_frames, 0);
    timeout(LIMIT, app.provider.wait_joined())
        .await
        .unwrap()
        .unwrap();
    assert!(app.provider.session_joined());
}
async fn cancel_clean(app: &mut NovaSonicRunningApp) {
    let sender = app.worker.sender();
    let (_, report) = timeout(LIMIT, async {
        tokio::join!(sender.cancel("fixture cancellation"), async {
            while app.worker.recv().await.is_some() {}
            app.worker.join().await
        })
    })
    .await
    .unwrap();
    let report = report.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(report.total_errors, 0, "{report:?}");
    timeout(LIMIT, app.provider.wait_joined())
        .await
        .unwrap()
        .unwrap();
    assert!(app.provider.session_joined());
}
async fn expect_failure(app: &mut NovaSonicRunningApp) -> String {
    let result = timeout(LIMIT, async {
        while app.worker.recv().await.is_some() {}
        app.worker.join().await
    })
    .await
    .unwrap();
    let errors = app.worker.errors();
    assert!(
        result.is_err() || !errors.is_empty() || result.as_ref().is_ok_and(|r| r.total_errors > 0),
        "failure reported as clean"
    );
    let provider = timeout(LIMIT, app.provider.wait_joined()).await.unwrap();
    assert!(
        app.provider.session_joined(),
        "failed provider tasks must actually join"
    );
    format!("{result:?}; {errors:?}; {provider:?}")
}
fn history(history: &NovaSonicHistory) -> Vec<(Role, String)> {
    history
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| {
            let Some(Content::Text(text)) = m.content else {
                panic!("nontext fixture history")
            };
            (m.role, text)
        })
        .collect()
}
fn assert_startup(startup: &[Value], seed: &[(Role, &str)]) {
    assert_eq!(startup.len(), 3 + 3 * seed.len());
    assert!(startup[0]["event"]["sessionStart"].is_object());
    assert!(startup[1]["event"]["promptStart"].is_object());
    let prompt = &startup[1]["event"]["promptStart"]["promptName"];
    for (index, (role, text)) in seed.iter().enumerate() {
        let start = &startup[2 + index * 3]["event"]["contentStart"];
        let input = &startup[3 + index * 3]["event"]["textInput"];
        let end = &startup[4 + index * 3]["event"]["contentEnd"];
        assert_eq!(start["interactive"], false);
        assert_eq!(start["type"], "TEXT");
        assert_eq!(
            start["role"],
            match role {
                Role::System => "SYSTEM",
                Role::User => "USER",
                Role::Assistant => "ASSISTANT",
                _ => panic!("invalid fixture seed"),
            }
        );
        assert_eq!(input["content"], *text);
        assert_eq!(input["contentName"], start["contentName"]);
        assert_eq!(end["contentName"], start["contentName"]);
        assert_eq!(&start["promptName"], prompt);
    }
    let audio = &startup.last().unwrap()["event"]["contentStart"];
    assert_eq!(audio["type"], "AUDIO");
    assert_eq!(audio["role"], "USER");
    assert_eq!(audio["interactive"], true);
    assert_eq!(
        audio["audioInputConfiguration"],
        json!({"mediaType":"audio/lpcm","sampleRateHertz":16000,"sampleSizeBits":16,"channelCount":1,"audioType":"SPEECH","encoding":"base64"})
    );
}

#[tokio::test]
async fn production_app_two_sessions_two_turns_one_completion_seed_pcm_history_and_eof_prefix() {
    let mut previous_prompt = None;
    let mut previous_signatures = None;
    for token in [false, true] {
        let mut peer = Peer::new(token).await;
        let seed = [
            (Role::System, "system policy"),
            (Role::User, "old user"),
            (Role::Assistant, "old assistant"),
            (Role::User, "last user remains history"),
        ];
        let context = Context::new(
            seed.iter()
                .map(|(role, text)| Message::text(*role, *text))
                .collect(),
        );
        let sink = Arc::new(Sink::default());
        let mut app = timeout(
            LIMIT,
            NovaSonicApp::new(config(&peer.endpoint, token), context, sink.clone())
                .unwrap()
                .start(),
        )
        .await
        .unwrap()
        .unwrap();
        let ready = peer.ready().await;
        assert_startup(&ready.startup, &seed);
        assert_ne!(previous_prompt.as_ref(), Some(&ready.nova.prompt));
        previous_prompt = Some(ready.nova.prompt.clone());
        peer.send(vec![ready.nova.event("completionStart", json!({}))])
            .await;
        let first = [0, 1, 2, 3, 4, 5, 254, 255];
        pcm(&app, &first).await;
        assert_eq!(
            peer.audio_from_app(&app, "first live PCM after nonempty seed")
                .await,
            first
        );
        peer.send(
            ready
                .nova
                .turn(1, &["first", "user"], "Generated first.", &[7, 8, 9, 10]),
        )
        .await;
        sink.wait_responses(1).await;
        sink.wait_pcm(4).await;
        // Input content is still open after a complete generated response.
        pcm(&app, &[11, 12, 13, 14]).await;
        assert_eq!(
            peer.audio_from_app(&app, "second live PCM in same completion")
                .await,
            [11, 12, 13, 14]
        );
        peer.send(ready.nova.turn(
            2,
            &["second user", "   "],
            "Generated second.",
            &[15, 16, 17, 18, 19, 20],
        ))
        .await;
        sink.wait_responses(2).await;
        peer.finish(ready.nova.text(
            "trailing-user",
            "USER",
            "FINAL",
            "final trailing prefix",
            "END_TURN",
        ))
        .await;
        // End must retain the immediately preceding input before the registered fence.
        pcm(&app, &[21, 22]).await;
        end_clean(&mut app).await;
        assert_eq!(app.provider.responses_completed(), 2);
        assert_eq!(sink.pcm(), [7, 8, 9, 10, 15, 16, 17, 18, 19, 20]);
        let mut expected: Vec<_> = seed.iter().map(|(r, t)| (*r, (*t).to_owned())).collect();
        expected.extend([
            (Role::User, "first user".into()),
            (Role::Assistant, "Generated first.".into()),
            (Role::User, "second user".into()),
            (Role::Assistant, "Generated second.".into()),
            (Role::User, "final trailing prefix".into()),
        ]);
        assert_eq!(history(&app.history), expected);
        for data in sink.frames.lock().unwrap().iter() {
            if let FrameData::LlmText(text) = data {
                assert!(!text.append_to_context);
            }
        }
        let report = peer.join().await;
        assert!(report.terminal && report.eof && !report.reset);
        assert_eq!(
            report
                .events
                .iter()
                .filter(|v| v["event"].get("sessionStart").is_some())
                .count(),
            1
        );
        assert_eq!(
            report
                .events
                .iter()
                .filter(|v| v["event"].get("textInput").is_some())
                .count(),
            seed.len(),
            "authentic local snapshots must not be replayed"
        );
        let uploaded: Vec<u8> = report
            .events
            .iter()
            .filter_map(|v| v["event"].get("audioInput"))
            .flat_map(|v| canonical_base64(v["content"].as_str().unwrap()))
            .collect();
        assert_eq!(
            uploaded,
            [0, 1, 2, 3, 4, 5, 254, 255, 11, 12, 13, 14, 21, 22]
        );
        let tail = &report.events[report.events.len() - 3..];
        assert!(tail[0]["event"]["contentEnd"].is_object());
        assert!(tail[1]["event"]["promptEnd"].is_object());
        assert!(tail[2]["event"]["sessionEnd"].is_object());
        assert_ne!(previous_signatures.as_ref(), Some(&report.signatures));
        previous_signatures = Some(report.signatures);
    }
}

struct Hold {
    entered: AtomicU64,
    changed: Notify,
    release: Semaphore,
}
impl Hold {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: AtomicU64::new(0),
            changed: Notify::new(),
            release: Semaphore::new(0),
        })
    }
    async fn wait(&self) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                if self.entered.load(Ordering::SeqCst) > 0 {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
struct HeldRealUser {
    inner: Arc<dyn FrameProcessor>,
    hold: Arc<Hold>,
}
#[async_trait]
impl FrameProcessor for HeldRealUser {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Upstream
            && matches!(&frame.data,FrameData::Transcription(t) if t.finalized)
        {
            self.hold.entered.fetch_add(1, Ordering::SeqCst);
            self.hold.changed.notify_waiters();
            self.hold
                .release
                .acquire()
                .await
                .map_err(|_| ProcessorError::fatal("fixture hold closed"))?
                .forget();
        }
        // Unchanged context, real user implementation, same exact graph slot.
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
async fn wait_responses(provider: &NovaSonicHandle, count: u64) {
    timeout(LIMIT, async {
        while provider.responses_completed() < count {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn actual_user_receipt_holds_dependent_output_and_end_until_real_history_commit() {
    let mut peer = Peer::new(true).await;
    let sink = Arc::new(Sink::default());
    let hold = Hold::new();
    let wrapped = hold.clone();
    let mut app = NovaSonicApp::new_with_user_wrapper(
        config(&peer.endpoint, true),
        Context::default(),
        sink.clone(),
        move |inner| {
            Arc::new(HeldRealUser {
                inner,
                hold: wrapped,
            })
        },
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let ready = peer.ready().await;
    let mut events = vec![ready.nova.event("completionStart", json!({}))];
    // All user finals and dependent assistant events share one coalesced DATA send.
    events.extend(ready.nova.turn(
        1,
        &["one", "two"],
        "held generated answer",
        &[41, 42, 43, 44],
    ));
    peer.send(events).await;
    hold.wait().await;
    wait_responses(&app.provider, 1).await;
    assert!(
        history(&app.history).is_empty(),
        "the held real consumer has not mutated history"
    );
    assert!(sink.pcm().is_empty());
    assert!(!sink.frames.lock().unwrap().iter().any(|f| matches!(
        f,
        FrameData::LlmResponseStart
            | FrameData::LlmText(_)
            | FrameData::TtsText(_)
            | FrameData::LlmResponseEnd
    )));
    peer.finish(Vec::new()).await;
    let sender = app.worker.sender();
    sender.end().await.unwrap();
    assert!(
        timeout(Duration::from_millis(35), app.provider.wait_joined())
            .await
            .is_err(),
        "End must not overtake the held registered prefix"
    );
    assert!(history(&app.history).is_empty());
    hold.release.add_permits(1);
    let report = timeout(LIMIT, async {
        while app.worker.recv().await.is_some() {}
        app.worker.join().await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0);
    timeout(LIMIT, app.provider.wait_joined())
        .await
        .unwrap()
        .unwrap();
    assert!(app.provider.session_joined());
    assert_eq!(
        history(&app.history),
        [
            (Role::User, "one two".into()),
            (Role::Assistant, "held generated answer".into())
        ]
    );
    assert_eq!(sink.pcm(), [41, 42, 43, 44]);
    assert_eq!(hold.entered.load(Ordering::SeqCst), 1);
    assert!(peer.join().await.eof);
}

struct PlayGuard<'a>(&'a AtomicU64);
impl Drop for PlayGuard<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[derive(Default)]
struct ControlledBackend {
    entered_old: AtomicU64,
    abandoned_old: AtomicU64,
    interrupted: AtomicU64,
    stopped: AtomicU64,
    played: Mutex<Vec<u8>>,
    changed: Notify,
    release_old: Option<Arc<Semaphore>>,
}
#[async_trait]
impl OutputBackend for ControlledBackend {
    async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        let audio = match &frame.data {
            FrameData::OutputAudio(a)
            | FrameData::TtsAudio(a)
            | FrameData::SpeechOutputAudio(a) => a,
            _ => panic!("backend received non-audio"),
        };
        assert_eq!((audio.sample_rate, audio.num_channels), (24000, 1));
        if audio.audio.iter().all(|b| *b == 7) {
            self.entered_old.fetch_add(1, Ordering::SeqCst);
            self.changed.notify_waiters();
            let _guard = PlayGuard(&self.abandoned_old);
            // Dropping this exact future discards unacknowledged local audio.
            if let Some(release) = &self.release_old {
                release.acquire().await.unwrap().forget();
            } else {
                std::future::pending::<()>().await;
            }
        }
        self.played.lock().unwrap().extend_from_slice(&audio.audio);
        self.changed.notify_waiters();
        Ok(PlaybackReceipt::Played)
    }
    async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
        self.interrupted.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_waiters();
        Ok(())
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stopped.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
impl ControlledBackend {
    async fn wait_for(&self, predicate: impl Fn(&Self) -> bool) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                if predicate(self) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
async fn output_interruption(late: bool) {
    let mut peer = Peer::new(true).await;
    let backend = Arc::new(ControlledBackend::default());
    let output = Arc::new(
        OutputTransport::new(
            "NovaAcceptanceOutput",
            OutputTransportConfig {
                sample_rate: Some(24000),
                channels: 1,
                chunk_duration: Duration::from_millis(20),
                auto_silence: false,
                end_silence: Duration::ZERO,
                queue_capacity: 32,
                queue_byte_capacity: 256 * 1024,
                write_timeout: Duration::from_secs(5),
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), output)
        .unwrap()
        .start()
        .await
        .unwrap();
    let ready = peer.ready().await;
    let mut events = vec![ready.nova.event("completionStart", json!({}))];
    events.extend(
        ready
            .nova
            .text("old-user", "USER", "FINAL", "old question", "END_TURN"),
    );
    events.extend(ready.nova.text(
        "old-spec",
        "ASSISTANT",
        "SPECULATIVE",
        "old generated answer",
        "PARTIAL_TURN",
    ));
    let mut audio = ready.nova.audio(
        "old-audio",
        &vec![7; 1920],
        if late { "END_TURN" } else { "INTERRUPTED" },
    );
    let audio_end = audio.pop().unwrap();
    events.extend(audio);
    if late {
        events.push(audio_end.clone());
    }
    peer.send(events).await;
    backend
        .wait_for(|b| b.entered_old.load(Ordering::SeqCst) == 1)
        .await;
    assert!(backend.played.lock().unwrap().is_empty());
    if late {
        wait_responses(&app.provider, 1).await;
        // No active AUDIO response remains, yet this canonical late FINAL must flush.
        peer.send(ready.nova.text(
            "old-final",
            "ASSISTANT",
            "FINAL",
            "interrupted final observation",
            "INTERRUPTED",
        ))
        .await;
    } else {
        peer.send(vec![audio_end]).await;
        peer.send(ready.nova.text(
            "old-final",
            "ASSISTANT",
            "FINAL",
            "interrupted final observation",
            "END_TURN",
        ))
        .await;
    }
    backend
        .wait_for(|b| {
            b.interrupted.load(Ordering::SeqCst) > 0 && b.abandoned_old.load(Ordering::SeqCst) == 1
        })
        .await;
    assert!(backend.played.lock().unwrap().is_empty());
    peer.send(ready.nova.turn(
        2,
        &["fresh question"],
        "fresh generated answer",
        &vec![9; 960],
    ))
    .await;
    backend
        .wait_for(|b| b.played.lock().unwrap().len() == 960)
        .await;
    peer.finish(Vec::new()).await;
    // Interruption intentionally retires stale queued frames; drop accounting is
    // not required to be zero in this scenario.
    let sender = app.worker.sender();
    let (end, report) = timeout(LIMIT, async {
        tokio::join!(sender.end(), async {
            while app.worker.recv().await.is_some() {}
            app.worker.join().await
        })
    })
    .await
    .unwrap();
    end.unwrap();
    let report = report.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0, "{report:?}");
    timeout(LIMIT, app.provider.wait_joined())
        .await
        .unwrap()
        .unwrap();
    assert!(app.provider.session_joined());
    assert_eq!(
        backend.entered_old.load(Ordering::SeqCst),
        1,
        "queued second old chunk must not enter backend"
    );
    assert_eq!(*backend.played.lock().unwrap(), vec![9; 960]);
    assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
    let stored = history(&app.history);
    assert!(stored
        .iter()
        .any(|(r, t)| *r == Role::User && t.contains("old question")));
    assert!(stored
        .iter()
        .any(|(r, t)| *r == Role::User && t.contains("fresh question")));
    assert_eq!(
        stored
            .iter()
            .filter(|(r, t)| *r == Role::Assistant && t == "fresh generated answer")
            .count(),
        1
    );
    assert!(!stored.iter().any(|(_, t)| t.contains("final observation")));
    assert!(peer.join().await.eof);
}

#[derive(Default)]
struct OutputObservation {
    admitted_audio: AtomicU64,
    end_prepared: AtomicBool,
    changed: Notify,
}
impl OutputObservation {
    async fn wait_for(&self, predicate: impl Fn(&Self) -> bool) {
        timeout(LIMIT, async {
            loop {
                let changed = self.changed.notified();
                if predicate(self) {
                    break;
                }
                changed.await;
            }
        })
        .await
        .unwrap();
    }
}
struct ObservedRealOutput {
    inner: Arc<OutputTransport>,
    observed: Arc<OutputObservation>,
}
#[async_trait]
impl FrameProcessor for ObservedRealOutput {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let audio =
            direction == Direction::Downstream && matches!(frame.data, FrameData::TtsAudio(_));
        self.inner.process(frame, direction, ctx).await?;
        if audio {
            self.observed.admitted_audio.fetch_add(1, Ordering::SeqCst);
            self.observed.changed.notify_waiters();
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await?;
        self.observed.end_prepared.store(true, Ordering::SeqCst);
        self.observed.changed.notify_waiters();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

#[tokio::test]
async fn cancel_during_real_output_playback_and_drain_discards_stale_pcm_and_joins() {
    for draining in [false, true] {
        let mut peer = Peer::new(true).await;
        let release = Arc::new(Semaphore::new(0));
        let backend = Arc::new(ControlledBackend {
            release_old: Some(release.clone()),
            ..Default::default()
        });
        let output = Arc::new(
            OutputTransport::new(
                "CancelledNovaOutput",
                OutputTransportConfig {
                    sample_rate: Some(24000),
                    channels: 1,
                    chunk_duration: Duration::from_millis(20),
                    auto_silence: false,
                    end_silence: Duration::ZERO,
                    queue_capacity: 32,
                    queue_byte_capacity: 256 * 1024,
                    write_timeout: Duration::from_secs(5),
                    ..Default::default()
                },
                backend.clone(),
                None,
                BTreeMap::new(),
            )
            .unwrap(),
        );
        let control = output.control();
        let observed = Arc::new(OutputObservation::default());
        let mut app = NovaSonicApp::new(
            config(&peer.endpoint, true),
            Context::default(),
            Arc::new(ObservedRealOutput {
                inner: output,
                observed: observed.clone(),
            }),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let ready = peer.ready().await;
        pcm(&app, &[2, 2]).await;
        assert_eq!(peer.audio().await, [2, 2]);
        let mut events = vec![ready.nova.event("completionStart", json!({}))];
        let mut turn = ready.nova.turn(
            1,
            &["cancelled output question"],
            "unplayed generated answer",
            &vec![7; 1920],
        );
        let audio_index = turn
            .iter()
            .position(|event| event["event"]["audioOutput"].is_object())
            .unwrap();
        turn.insert(
            audio_index + 1,
            ready.nova.event(
                "audioOutput",
                json!({"contentId":"audio-1","content":STANDARD.encode(vec![9;960])}),
            ),
        );
        events.extend(turn);
        peer.send(events).await;
        backend
            .wait_for(|b| b.entered_old.load(Ordering::SeqCst) == 1)
            .await;
        observed
            .wait_for(|o| o.admitted_audio.load(Ordering::SeqCst) == 2)
            .await;
        wait_responses(&app.provider, 1).await;
        assert!(backend.played.lock().unwrap().is_empty());
        assert_eq!(backend.abandoned_old.load(Ordering::SeqCst), 0);
        assert_eq!(
            history(&app.history),
            [(Role::User, "cancelled output question".into())]
        );
        if draining {
            peer.finish(Vec::new()).await;
            app.worker.sender().end().await.unwrap();
            // This is the exact output's real prepare_end, after provider EOF,
            // with its pipeline-owned media task still blocked in play_audio.
            observed
                .wait_for(|o| o.end_prepared.load(Ordering::SeqCst))
                .await;
            assert!(app.provider.session_joined());
            assert_eq!(backend.abandoned_old.load(Ordering::SeqCst), 0);
            assert_eq!(backend.stopped.load(Ordering::SeqCst), 0);
            assert!(!app.worker.has_finished());
        } else {
            assert!(!observed.end_prepared.load(Ordering::SeqCst));
            assert!(control.is_ready());
        }

        let sender = app.worker.sender();
        let (cancelled, report) = timeout(LIMIT, async {
            tokio::join!(sender.cancel("cancel blocked real output"), async {
                while app.worker.recv().await.is_some() {}
                app.worker.join().await
            })
        })
        .await
        .unwrap();
        cancelled.unwrap();
        let report = report.unwrap();
        assert_eq!(report.state, WorkerState::Cancelled, "{report:?}");
        assert_eq!(report.total_errors, 0, "{report:?}");
        assert!(report.errors.is_empty(), "{report:?}");
        timeout(LIMIT, app.provider.wait_joined())
            .await
            .unwrap()
            .unwrap();
        assert!(app.provider.session_joined());
        assert!(!control.is_ready());
        assert_eq!(backend.entered_old.load(Ordering::SeqCst), 1);
        assert_eq!(backend.abandoned_old.load(Ordering::SeqCst), 1);
        assert!(backend.interrupted.load(Ordering::SeqCst) > 0);
        assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
        assert!(backend.played.lock().unwrap().is_empty());
        assert_eq!(
            history(&app.history),
            [(Role::User, "cancelled output question".into())]
        );

        // Releasing the original backend operation after every real task joined
        // cannot resurrect either its remainder or the separately queued PCM.
        release.add_permits(1);
        assert!(sender
            .send(
                Frame::new(FrameData::InputAudio(
                    AudioRawFrame::new(vec![4, 4], 16000, 1).unwrap(),
                )),
                Direction::Downstream,
            )
            .await
            .is_err());
        assert_eq!(release.available_permits(), 1);
        assert_eq!(backend.entered_old.load(Ordering::SeqCst), 1);
        assert_eq!(backend.abandoned_old.load(Ordering::SeqCst), 1);
        assert!(backend.played.lock().unwrap().is_empty());
        let report = peer.join().await;
        if draining {
            assert!(report.eof && report.terminal && !report.reset);
        } else {
            assert!(report.reset && !report.eof && !report.terminal);
        }
    }
}

#[tokio::test]
async fn cancelled_completion_waiter_and_drop_with_retained_sender_preserve_real_join() {
    for drop_app in [false, true] {
        let mut peer = Peer::new(true).await;
        let sink = Arc::new(Sink::default());
        let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), sink)
            .unwrap()
            .start()
            .await
            .unwrap();
        let ready = peer.ready().await;
        peer.send(vec![ready.nova.event("completionStart", json!({}))])
            .await;
        let retained = app.worker.sender();
        let provider = app.provider.clone();
        assert!(timeout(Duration::from_millis(20), provider.wait_joined())
            .await
            .is_err());
        if drop_app {
            drop(app);
            timeout(LIMIT, provider.wait_joined())
                .await
                .unwrap()
                .unwrap();
            assert!(provider.session_joined());
            assert!(retained
                .send(
                    Frame::new(FrameData::InputAudio(
                        AudioRawFrame::new(vec![1, 2], 16000, 1).unwrap()
                    )),
                    Direction::Downstream
                )
                .await
                .is_err());
        } else {
            cancel_clean(&mut app).await;
            let (a, b) = tokio::join!(provider.wait_joined(), provider.wait_joined());
            a.unwrap();
            b.unwrap();
        }
        drop(retained);
        let report = peer.join().await;
        assert!(
            report.reset && !report.terminal,
            "cancellation is not graceful empty-terminal completion"
        );
    }
}

#[tokio::test]
async fn provider_tools_bad_identity_crc_and_exception_fail_without_tool_history_or_leaks() {
    for scenario in ["tool", "identity", "crc", "exception"] {
        let mut peer = Peer::new(true).await;
        let sink = Arc::new(Sink::default());
        let mut app = NovaSonicApp::new(
            config(&peer.endpoint, true),
            Context::default(),
            sink.clone(),
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let ready = peer.ready().await;
        let start = ready.nova.event("completionStart", json!({}));
        match scenario {
            "tool"=>peer.send(vec![start,ready.nova.event("contentStart",json!({"contentId":"tool-1","type":"TOOL","role":"ASSISTANT","toolUseOutputConfiguration":{"toolName":"must-never-execute"}}))]).await,
            "identity"=>peer.send(vec![start,ready.nova.event("contentStart",json!({"sessionId":"forged-session","contentId":"bad","type":"TEXT","role":"USER","additionalModelFields":"{\"generationStage\":\"FINAL\"}","textOutputConfiguration":{"mediaType":"text/plain"}}))]).await,
            "crc"=>{let mut wire=chunk(&start);let last=wire.len()-1;wire[last]^=1;peer.raw(wire).await;},
            "exception"=>peer.raw(frame(&[(":message-type","exception"),(":exception-type","validationException"),(":content-type","application/json")],br#"{"message":"sensitive-provider-text-must-not-leak"}"#)).await,
            _=>unreachable!(),
        }
        let failure = expect_failure(&mut app).await;
        assert!(!failure.contains("sensitive-provider-text-must-not-leak"));
        assert!(!failure.contains(SECRET));
        assert!(!failure.contains(TOKEN));
        assert!(history(&app.history).is_empty());
        assert!(sink.pcm().is_empty());
        let report = peer.join().await;
        assert!(report.reset);
        assert!(!report
            .events
            .iter()
            .any(|v| v["event"].get("toolResult").is_some()));
    }
}

#[tokio::test]
async fn live_text_forged_context_and_reserved_fences_reject_at_both_public_boundaries() {
    for upstream in [false, true] {
        for case in [0, 1, 2, 3] {
            let mut peer = Peer::new(true).await;
            let sink = Arc::new(Sink::default());
            let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), sink)
                .unwrap()
                .start()
                .await
                .unwrap();
            let _ = peer.ready().await;
            let data = match case {
                0 => FrameData::LlmRun,
                1 => FrameData::Text(TextFrame::llm("unrequested live text")),
                2 => FrameData::LlmContext(LlmContextFrame::default()),
                3 => FrameData::Flush(FlushFrame {
                    returning: false,
                    origin: Some("gemini_history_end_input".into()),
                }),
                _ => unreachable!(),
            };
            assert!(app
                .worker
                .sender()
                .send(
                    Frame::new(data),
                    if upstream {
                        Direction::Upstream
                    } else {
                        Direction::Downstream
                    }
                )
                .await
                .is_err());
            assert!(history(&app.history).is_empty());
            cancel_clean(&mut app).await;
            let report = peer.join().await;
            assert!(report.reset);
            assert_eq!(
                report.events.len(),
                3,
                "no live request or context replay may escape"
            );
        }
    }
}

#[test]
fn invalid_initial_profiles_reject_before_any_connection_or_output_setup() {
    let base = config(
        "http://127.0.0.1:1/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream",
        true,
    );
    let sink = Arc::new(Sink::default());
    for context in [
        Context {
            tools: Some(vec![ToolDefinition {
                name: "no-tools".into(),
                description: String::new(),
                parameters: json!({}),
            }]),
            ..Default::default()
        },
        Context {
            tool_choice: Some(ToolChoice::None),
            ..Default::default()
        },
        Context::new(vec![Message::tool_result("unowned-tool", "result")]),
        Context::new(vec![Message::text(
            Role::Developer,
            "unsupported developer",
        )]),
        Context::new(vec![
            Message::text(Role::System, "one"),
            Message::text(Role::System, "two"),
        ]),
    ] {
        assert!(NovaSonicApp::new(base.clone(), context, sink.clone()).is_err());
    }
    let mut conflict = base.clone();
    conflict.system_instruction = Some("one".into());
    assert!(NovaSonicApp::new(
        conflict,
        Context::new(vec![Message::text(Role::System, "two")]),
        sink.clone()
    )
    .is_err());
    for endpoint in ["http://example.com/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream","http://127.0.0.1:1/wrong","http://user:secret@127.0.0.1:1/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream"] {
        let mut bad=base.clone();bad.endpoint=Some(endpoint.into());assert!(NovaSonicApp::new(bad,Context::default(),sink.clone()).is_err());
    }
    assert!(sink.frames.lock().unwrap().is_empty());
}

#[tokio::test]
async fn startup_failure_retains_output_cleanup_failure_and_joins_peer() {
    let mut peer = Peer::status(true, 403).await;
    let sink = Arc::new(Sink {
        fail_cleanup: true,
        ..Default::default()
    });
    let app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), sink).unwrap();
    let provider = NovaSonicHandle(app.state.clone());
    let result = timeout(LIMIT, app.start()).await.unwrap();
    let failure = match result {
        Err(error) => error.to_string(),
        Ok(_) => panic!("HTTP 403 accepted"),
    };
    assert!(failure.contains("403"), "{failure}");
    assert!(
        failure.contains("fixture cleanup failure"),
        "lost output cleanup failure: {failure}"
    );
    assert!(!failure.contains(SECRET));
    timeout(LIMIT, provider.wait_joined())
        .await
        .unwrap()
        .unwrap_err();
    assert!(provider.session_joined());
    let _ = peer.ready().await;
    assert!(peer.join().await.reset);
}

#[tokio::test]
async fn actual_output_failure_terminates_session_and_preserves_primary_diagnostic() {
    let mut peer = Peer::new(true).await;
    let sink = Arc::new(Sink {
        fail_audio: true,
        ..Default::default()
    });
    let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), sink)
        .unwrap()
        .start()
        .await
        .unwrap();
    let ready = peer.ready().await;
    let mut events = vec![ready.nova.event("completionStart", json!({}))];
    events.extend(ready.nova.turn(1, &["user"], "answer", &[1, 2]));
    peer.send(events).await;
    let failure = expect_failure(&mut app).await;
    assert!(
        failure.contains("fixture output write failure"),
        "{failure}"
    );
    assert!(peer.join().await.reset);
}

struct FixtureDir(PathBuf);
impl FixtureDir {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "nova-app-acceptance-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for FixtureDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
fn example_binary() -> PathBuf {
    let executable = std::env::current_exe().unwrap();
    let path = executable
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("examples")
        .join(format!("pcm_session{}", std::env::consts::EXE_SUFFIX));
    assert!(path.is_file(),"acceptance requires the parent build owner to compile the actual pcm_session executable first");
    path
}
async fn binary(arguments: Vec<std::ffi::OsString>, credentials: bool) -> std::process::Output {
    let executable = example_binary();
    tokio::task::spawn_blocking(move || {
        let mut command = Command::new(executable);
        command
            .args(arguments)
            .env_clear()
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped());
        if credentials {
            command
                .env("NOVA_FIXTURE_ACCESS_KEY_ID", ACCESS)
                .env("NOVA_FIXTURE_SECRET_ACCESS_KEY", SECRET)
                .env("NOVA_FIXTURE_SESSION_TOKEN", TOKEN);
        }
        let mut child = command.spawn().unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(15);
        loop {
            if child.try_wait().unwrap().is_some() {
                break;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                let output = child.wait_with_output().unwrap();
                panic!(
                    "compiled fixture timed out; process killed and reaped: {:?}",
                    output.status
                );
            }
            std::thread::sleep(Duration::from_millis(5));
        }
        let output = child.wait_with_output().unwrap();
        assert!(output.stdout.len() + output.stderr.len() < 64 * 1024);
        for secret in [SECRET, TOKEN] {
            assert!(!String::from_utf8_lossy(&output.stdout).contains(secret));
            assert!(!String::from_utf8_lossy(&output.stderr).contains(secret));
        }
        output
    })
    .await
    .unwrap()
}
fn options(endpoint: &str) -> Vec<std::ffi::OsString> {
    [
        "--region",
        "us-east-1",
        "--model",
        "amazon.nova-2-sonic-v1:0",
        "--voice",
        "tiffany",
        "--credentials-env",
        "NOVA_FIXTURE",
        "--endpoint",
        endpoint,
    ]
    .into_iter()
    .map(Into::into)
    .collect()
}
#[tokio::test]
async fn compiled_pcm_example_help_check_and_invalid_files_are_no_network_no_overwrite() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("http://{}{PATH}", listener.local_addr().unwrap());
    let help = binary(vec!["--help".into()], false).await;
    assert!(help.status.success());
    assert!(String::from_utf8_lossy(&help.stdout).contains("Recorded AWS Nova Sonic PCM session"));
    let mut check = vec!["--check-config".into()];
    check.extend(options(&endpoint));
    assert!(binary(check.clone(), true).await.status.success());
    assert!(!binary(check, false).await.status.success());
    let dir = FixtureDir::new();
    let input = dir.0.join("input.pcm");
    let output = dir.0.join("output.pcm");
    std::fs::write(&input, [1, 2, 3, 4]).unwrap();
    std::fs::write(&output, b"existing output must survive").unwrap();
    let mut session = vec![
        "--session".into(),
        input.as_os_str().to_owned(),
        output.as_os_str().to_owned(),
    ];
    session.extend(options(&endpoint));
    assert!(!binary(session, true).await.status.success());
    assert_eq!(
        std::fs::read(&output).unwrap(),
        b"existing output must survive"
    );
    let missing = dir.0.join("missing.pcm");
    let new_output = dir.0.join("new-output.pcm");
    let mut session = vec![
        "--session".into(),
        missing.as_os_str().to_owned(),
        new_output.as_os_str().to_owned(),
    ];
    session.extend(options(&endpoint));
    assert!(!binary(session, true).await.status.success());
    assert!(!new_output.exists());
    std::fs::write(&input, [1, 2, 3]).unwrap();
    let mut session = vec![
        "--session".into(),
        input.as_os_str().to_owned(),
        new_output.as_os_str().to_owned(),
    ];
    session.extend(options(&endpoint));
    assert!(!binary(session, true).await.status.success());
    assert!(!new_output.exists());
    assert!(
        timeout(Duration::from_millis(50), listener.accept())
            .await
            .is_err(),
        "help, config and invalid paths must not connect"
    );
}

#[tokio::test]
async fn compiled_pcm_example_records_exact_output_and_reaps_success_and_failure_processes() {
    for fail in [false, true] {
        let dir = FixtureDir::new();
        let input = dir.0.join("input.pcm");
        let output = dir.0.join("output.pcm");
        let input_pcm: Vec<u8> = (0..2048).map(|n| (n % 251) as u8).collect();
        std::fs::write(&input, &input_pcm).unwrap();
        let mut peer = Peer::new(true).await;
        let mut args = vec![
            "--session".into(),
            input.as_os_str().to_owned(),
            output.as_os_str().to_owned(),
        ];
        args.extend(options(&peer.endpoint));
        let process = tokio::spawn(binary(args, true));
        let ready = peer.ready().await;
        assert_startup(&ready.startup, &[]);
        assert_eq!(peer.audio().await, input_pcm[..1024]);
        if fail {
            peer.raw(frame(
                &[
                    (":message-type", "exception"),
                    (":exception-type", "modelStreamErrorException"),
                    (":content-type", "application/json"),
                ],
                br#"{"message":"private synthetic provider failure"}"#,
            ))
            .await;
        } else {
            let mut events = vec![ready.nova.event("completionStart", json!({}))];
            events.extend(ready.nova.turn(
                1,
                &["recorded user"],
                "recorded generated answer",
                &[51, 52, 53, 54, 55, 56],
            ));
            peer.send(events).await;
            assert_eq!(peer.audio().await, input_pcm[1024..]);
            peer.finish(Vec::new()).await;
        }
        let result = timeout(LIMIT, process).await.unwrap().unwrap();
        assert_eq!(
            result.status.success(),
            !fail,
            "stdout={} stderr={}",
            String::from_utf8_lossy(&result.stdout),
            String::from_utf8_lossy(&result.stderr)
        );
        let report = peer.join().await;
        if fail {
            assert!(report.reset);
            assert!(!String::from_utf8_lossy(&result.stderr)
                .contains("private synthetic provider failure"));
            assert!(String::from_utf8_lossy(&result.stderr).contains("partial"));
        } else {
            assert!(report.terminal && report.eof);
            let actual = std::fs::read(&output).unwrap();
            assert_eq!(actual, [51, 52, 53, 54, 55, 56]);
            assert_eq!(sha(&actual), sha(&[51, 52, 53, 54, 55, 56]));
            assert!(String::from_utf8_lossy(&result.stdout)
                .contains("pipeline report clean and provider session joined"));
            let uploaded: Vec<u8> = report
                .events
                .iter()
                .filter_map(|v| v["event"].get("audioInput"))
                .flat_map(|v| canonical_base64(v["content"].as_str().unwrap()))
                .collect();
            assert_eq!(uploaded, input_pcm);
        }
        assert_eq!(std::fs::read(&input).unwrap(), input_pcm);
    }
}

#[tokio::test]
async fn interruption_while_real_user_receipt_pending_retires_only_old_dependent_output() {
    let mut peer = Peer::new(true).await;
    let sink = Arc::new(Sink::default());
    let hold = Hold::new();
    let wrapped = hold.clone();
    let mut app = NovaSonicApp::new_with_user_wrapper(
        config(&peer.endpoint, true),
        Context::default(),
        sink.clone(),
        move |inner| {
            Arc::new(HeldRealUser {
                inner,
                hold: wrapped,
            })
        },
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let ready = peer.ready().await;
    let mut events = vec![ready.nova.event("completionStart", json!({}))];
    events.extend(ready.nova.text(
        "held-user",
        "USER",
        "FINAL",
        "genuine held user",
        "END_TURN",
    ));
    events.extend(ready.nova.text(
        "held-spec",
        "ASSISTANT",
        "SPECULATIVE",
        "retired old answer",
        "PARTIAL_TURN",
    ));
    events.extend(ready.nova.audio("held-audio", &[7, 7, 7, 7], "END_TURN"));
    peer.send(events).await;
    hold.wait().await;
    wait_responses(&app.provider, 1).await;
    assert!(sink.pcm().is_empty());
    assert!(history(&app.history).is_empty());
    peer.send(ready.nova.text(
        "held-final",
        "ASSISTANT",
        "FINAL",
        "late interruption",
        "INTERRUPTED",
    ))
    .await;
    timeout(LIMIT, async {
        loop {
            let changed = sink.changed.notified();
            if sink
                .frames
                .lock()
                .unwrap()
                .iter()
                .any(|f| matches!(f, FrameData::Interruption))
            {
                break;
            }
            changed.await;
        }
    })
    .await
    .unwrap();
    assert!(sink.pcm().is_empty());
    hold.release.add_permits(2);
    timeout(LIMIT, async {
        while history(&app.history).is_empty() {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    peer.send(
        ready
            .nova
            .turn(2, &["fresh held user"], "fresh held answer", &[9, 9, 9, 9]),
    )
    .await;
    sink.wait_responses(1).await;
    peer.finish(Vec::new()).await;
    end_clean(&mut app).await;
    assert_eq!(sink.pcm(), [9, 9, 9, 9]);
    let stored = history(&app.history);
    assert!(stored
        .iter()
        .any(|(r, t)| *r == Role::User && t == "genuine held user"));
    assert!(stored
        .iter()
        .any(|(r, t)| *r == Role::User && t == "fresh held user"));
    assert_eq!(
        stored
            .iter()
            .filter(|(r, _)| *r == Role::Assistant)
            .map(|(_, t)| t.as_str())
            .collect::<Vec<_>>(),
        ["fresh held answer"]
    );
    assert!(peer.join().await.eof);
}

#[tokio::test]
async fn real_user_receipt_timeout_fails_and_joins_without_releasing_dependent_audio() {
    let mut peer = Peer::new(true).await;
    let sink = Arc::new(Sink::default());
    let hold = Hold::new();
    let wrapped = hold.clone();
    let mut cfg = config(&peer.endpoint, true);
    cfg.limits.consumer_timeout = Duration::from_millis(250);
    let mut app =
        NovaSonicApp::new_with_user_wrapper(cfg, Context::default(), sink.clone(), move |inner| {
            Arc::new(HeldRealUser {
                inner,
                hold: wrapped,
            })
        })
        .unwrap()
        .start()
        .await
        .unwrap();
    let ready = peer.ready().await;
    let mut events = vec![ready.nova.event("completionStart", json!({}))];
    events.extend(ready.nova.turn(
        1,
        &["held forever until failure"],
        "must not release",
        &[1, 2],
    ));
    peer.send(events).await;
    hold.wait().await;
    let failed = timeout(LIMIT, app.provider.wait_joined())
        .await
        .unwrap()
        .unwrap_err();
    assert!(failed.message.contains("receipt deadline"), "{failed:?}");
    hold.release.add_permits(1);
    expect_failure(&mut app).await;
    assert!(sink.pcm().is_empty());
    assert!(!history(&app.history)
        .iter()
        .any(|(r, _)| *r == Role::Assistant));
    assert!(peer.join().await.reset);
}

#[tokio::test]
async fn real_user_receipt_held_output_count_and_byte_overflow_fail_closed_and_join() {
    for byte_limit in [false, true] {
        let mut peer = Peer::new(true).await;
        let sink = Arc::new(Sink::default());
        let hold = Hold::new();
        let wrapped = hold.clone();
        let mut cfg = config(&peer.endpoint, true);
        let audio = vec![7; 1026];
        let encoded = STANDARD.encode(&audio);
        let audio_capacity = canonical_base64(&encoded).capacity();
        // The exact real queue charges two frame-sized allocation slots plus
        // small-field slack per held frame. Eight bytes of speculative text
        // leave no String growth ambiguity. The byte case fits exactly two PCM
        // frames; the count case fits exactly five, after three start/text frames.
        let overhead = 2 * std::mem::size_of::<super::Held>() + 512;
        let allowed_audio = if byte_limit {
            cfg.limits.max_held_bytes = 3 * overhead + 8 + 2 * (overhead + audio_capacity);
            2
        } else {
            cfg.limits.max_held_events = 8;
            5
        };
        assert!(cfg.limits.max_held_bytes < 4 * 1024 * 1024);
        let mut app = NovaSonicApp::new_with_user_wrapper(
            cfg,
            Context::default(),
            sink.clone(),
            move |inner| {
                Arc::new(HeldRealUser {
                    inner,
                    hold: wrapped,
                })
            },
        )
        .unwrap()
        .start()
        .await
        .unwrap();
        let ready = peer.ready().await;
        let usage = ready.nova.event(
            "usageEvent",
            json!({"details":{"delta":{"input":{"speechTokens":0,"textTokens":0},"output":{"speechTokens":1,"textTokens":0}}}}),
        );
        let mut events = vec![ready.nova.event("completionStart", json!({}))];
        events.extend(ready.nova.text(
            "budget-user",
            "USER",
            "FINAL",
            "real user awaiting its exact receipt",
            "END_TURN",
        ));
        events.extend(ready.nova.text(
            "budget-spec",
            "ASSISTANT",
            "SPECULATIVE",
            "retained",
            "PARTIAL_TURN",
        ));
        events.push(
            ready
                .nova
                .audio("budget-audio", &audio, "END_TURN")
                .remove(0),
        );
        events.push(usage.clone());
        peer.send(events).await;
        hold.wait().await;
        sink.wait_usage(1).await;
        assert_eq!(hold.entered.load(Ordering::SeqCst), 1);
        assert!(history(&app.history).is_empty());

        let audio_event = ready.nova.event(
            "audioOutput",
            json!({"contentId":"budget-audio","content":encoded}),
        );
        for index in 0..allowed_audio {
            peer.send(vec![audio_event.clone(), usage.clone()]).await;
            // Usage is a real decoded event after the PCM and bypasses the
            // dependent-output queue. Its actual sink observation proves that
            // this PCM was processed, not merely written into an h2 buffer.
            sink.wait_usage(index + 2).await;
            assert_eq!(app.worker.state(), WorkerState::Running);
            assert!(history(&app.history).is_empty());
            assert!(sink.pcm().is_empty());
        }
        peer.send(vec![audio_event]).await;
        let failed = timeout(LIMIT, app.provider.wait_joined())
            .await
            .unwrap()
            .unwrap_err();
        assert!(
            failed
                .message
                .contains("Nova dependent output exceeds retained budget"),
            "byte_limit={byte_limit}: {failed:?}"
        );
        assert!(!failed.message.contains("mapped event batch"));
        assert!(!failed.message.contains("receipt deadline"));
        // Never grant the real consumer a permit, even during failure cleanup.
        // Actual core joining must cancel that exact suspended invocation.
        assert_eq!(hold.release.available_permits(), 0);
        let joined = expect_failure(&mut app).await;
        assert!(
            joined.contains("dependent output exceeds retained budget"),
            "{joined}"
        );
        assert_eq!(app.worker.state(), WorkerState::Failed);
        assert!(app.provider.session_joined());
        assert_eq!(hold.entered.load(Ordering::SeqCst), 1);
        assert_eq!(hold.release.available_permits(), 0);
        assert!(
            history(&app.history).is_empty(),
            "no fabricated user or assistant history"
        );
        assert!(sink.pcm().is_empty());
        assert!(!sink.frames.lock().unwrap().iter().any(|frame| matches!(
            frame,
            FrameData::LlmResponseStart
                | FrameData::LlmText(_)
                | FrameData::TtsStarted(_)
                | FrameData::TtsAudio(_)
                | FrameData::TtsText(_)
                | FrameData::LlmResponseEnd
                | FrameData::TtsStopped(_)
        )));
        let report = peer.join().await;
        assert!(report.reset && !report.terminal && !report.eof);
    }
}

#[tokio::test]
async fn invalid_pcm_and_excess_public_frame_metadata_reject_before_owned_queue_or_wire() {
    let mut peer = Peer::new(true).await;
    let sink = Arc::new(Sink::default());
    let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), sink)
        .unwrap()
        .start()
        .await
        .unwrap();
    let _ = peer.ready().await;
    for audio in [
        AudioRawFrame {
            audio: vec![1],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![1, 2],
            sample_rate: 24000,
            num_channels: 1,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![1, 2, 3, 4],
            sample_rate: 16000,
            num_channels: 2,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![0; 64 * 1024],
            sample_rate: 16000,
            num_channels: 1,
            context_id: None,
        },
    ] {
        assert!(app
            .worker
            .sender()
            .send(
                Frame::new(FrameData::InputAudio(audio)),
                Direction::Downstream
            )
            .await
            .is_err());
    }
    let mut frame = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![1, 2], 16000, 1).unwrap(),
    ));
    frame.metadata.insert(
        "bounded-metadata".into(),
        Value::String("x".repeat(300 * 1024)),
    );
    assert!(app
        .worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .is_err());
    assert!(history(&app.history).is_empty());
    cancel_clean(&mut app).await;
    let report = peer.join().await;
    assert_eq!(report.events.len(), 3);
    assert!(report.reset);
}

#[tokio::test]
async fn audio_and_late_final_interruption_flush_real_output_queue_and_fresh_turn_survives() {
    output_interruption(false).await;
    output_interruption(true).await;
}

#[tokio::test]
async fn native_tool_carriers_foreign_context_seals_and_retags_reject_before_user_mutation() {
    let mut peer = Peer::new(true).await;
    let sink = Arc::new(Sink::default());
    let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), sink)
        .unwrap()
        .start()
        .await
        .unwrap();
    let _ = peer.ready().await;
    let producer = pipecat_core::NativeRealtimeContextProducer::default();
    let mut sealed = Frame::new(FrameData::LlmContext(LlmContextFrame::default()));
    producer
        .attach(&mut sealed, pipecat_core::RealtimeContextIntent::Run, true)
        .unwrap();
    let mut retagged = sealed.clone();
    retagged.data = FrameData::InputAudio(AudioRawFrame::new(vec![1, 2], 16000, 1).unwrap());
    let call = Arc::new(pipecat_core::FunctionCall {
        function_name: "never-execute".into(),
        tool_call_id: "native-unowned".into(),
        arguments: json!({}),
    });
    let batch =
        pipecat_core::NativeToolCallBatch::new("unowned-batch".into(), vec![(call.clone(), true)])
            .unwrap();
    let mut declaration = Frame::new(FrameData::FunctionCallsStarted(vec![(*call).clone()]));
    declaration.set_native_tool_batch(batch).unwrap();
    let mut tool_retag = declaration.clone();
    tool_retag.data = FrameData::InputAudio(AudioRawFrame::new(vec![3, 4], 16000, 1).unwrap());
    let owner = pipecat_core::SpeculationOwner::new();
    let mut speculative = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(vec![5, 6], 16000, 1).unwrap(),
    ));
    speculative
        .set_native_speculation(owner.capability())
        .unwrap();
    for frame in [sealed, retagged, declaration, tool_retag, speculative] {
        for direction in [Direction::Downstream, Direction::Upstream] {
            assert!(app
                .worker
                .sender()
                .send(frame.clone(), direction)
                .await
                .is_err());
        }
    }
    assert!(history(&app.history).is_empty());
    cancel_clean(&mut app).await;
    let report = peer.join().await;
    assert_eq!(report.events.len(), 3);
    assert!(report.reset);
}

#[tokio::test]
async fn stop_cancels_provider_without_fabricating_graceful_terminal() {
    let mut peer = Peer::new(true).await;
    let mut app = NovaSonicApp::new(
        config(&peer.endpoint, true),
        Context::default(),
        Arc::new(Sink::default()),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let _ = peer.ready().await;
    app.worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    let report = timeout(LIMIT, async {
        while app.worker.recv().await.is_some() {}
        app.worker.join().await
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(report.state, WorkerState::Stopped);
    assert_eq!(report.total_errors, 0);
    timeout(LIMIT, app.provider.wait_joined())
        .await
        .unwrap()
        .unwrap();
    assert!(app.provider.session_joined());
    let report = peer.join().await;
    assert!(report.reset && !report.terminal);
}

struct HeldSystemOutput {
    inner: Arc<dyn FrameProcessor>,
    hold: Arc<Hold>,
}
#[async_trait]
impl FrameProcessor for HeldSystemOutput {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(frame.data, FrameData::BotStartedSpeaking)
            && self.hold.entered.fetch_add(1, Ordering::SeqCst) == 0
        {
            self.hold.changed.notify_waiters();
            self.hold
                .release
                .acquire()
                .await
                .map_err(|_| ProcessorError::fatal("fixture system hold closed"))?
                .forget();
        }
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
#[tokio::test]
async fn saturated_real_output_system_lane_fails_broadcast_immediately_and_joins_cleanup() {
    let mut peer = Peer::new(true).await;
    let backend = Arc::new(ControlledBackend::default());
    let output = Arc::new(
        OutputTransport::new(
            "BlockedNovaSystemOutput",
            OutputTransportConfig {
                sample_rate: Some(24000),
                auto_silence: false,
                end_silence: Duration::ZERO,
                ..Default::default()
            },
            backend,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let hold = Hold::new();
    let output = Arc::new(HeldSystemOutput {
        inner: output,
        hold: hold.clone(),
    });
    let mut app = NovaSonicApp::new(config(&peer.endpoint, true), Context::default(), output)
        .unwrap()
        .start()
        .await
        .unwrap();
    let _ = peer.ready().await;
    let state = app.provider.0.clone();
    let ctx = state.setup.lock().unwrap().clone().unwrap();
    ctx.try_push(
        Frame::new(FrameData::BotStartedSpeaking).with_interruptible(false),
        Direction::Downstream,
    )
    .unwrap();
    hold.wait().await;
    // Exactly the configured 64 free slots. Do not probe with a 65th push:
    // that would itself fail the runtime before exercising broadcast.
    for _ in 0..64 {
        ctx.try_push(
            Frame::new(FrameData::BotStartedSpeaking).with_interruptible(false),
            Direction::Downstream,
        )
        .unwrap();
    }
    // ProcessorContext::push deliberately delegates to synchronous try_push.
    // A full lane is an immediate fatal admission error, not a blocked future.
    let failure = timeout(Duration::from_millis(250), super::interrupt(&state, &ctx))
        .await
        .unwrap()
        .unwrap_err();
    assert_eq!(failure.message, "frame queue is full");
    assert!(!failure.message.contains(SECRET));
    assert!(!failure.message.contains(TOKEN));
    assert!(!failure.message.contains(&peer.endpoint));
    hold.release.add_permits(1);
    let joined = expect_failure(&mut app).await;
    assert!(
        joined.contains("Backpressure") || joined.contains("full"),
        "{joined}"
    );
    assert!(peer.join().await.reset);
}

#[test]
fn public_normalization_discards_spare_capacity_and_rejects_metadata_depth_and_nodes() {
    let cfg = config(
        "http://127.0.0.1:1/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream",
        true,
    );
    let mut pcm = Vec::with_capacity(1024 * 1024);
    pcm.extend_from_slice(&[1, 2, 3, 4]);
    let mut value = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(pcm, 16000, 1).unwrap(),
    ));
    let mut name = String::with_capacity(1024 * 1024);
    name.push_str("small name");
    value.name = name;
    let mut key = String::with_capacity(1024 * 1024);
    key.push_str("small-key");
    let mut text = String::with_capacity(1024 * 1024);
    text.push_str("small value");
    value.metadata.insert(key, Value::String(text));
    let mut array = Vec::with_capacity(64 * 1024);
    array.push(Value::Null);
    value.metadata.insert("array".into(), Value::Array(array));
    let id = value.id;
    let normalized = super::normalize_input(value, Direction::Downstream, &cfg).unwrap();
    assert_eq!(normalized.id, id);
    assert_eq!(normalized.name, "small name");
    assert!(normalized.name.capacity() < 256);
    let FrameData::InputAudio(audio) = normalized.data else {
        panic!("input kind changed")
    };
    assert_eq!(audio.audio, [1, 2, 3, 4]);
    assert!(audio.audio.capacity() < 128);
    let (key, text) = normalized.metadata.get_key_value("small-key").unwrap();
    assert!(key.capacity() < 128);
    let Value::String(text) = text else {
        panic!("metadata kind changed")
    };
    assert_eq!(text, "small value");
    assert!(text.capacity() < 128);
    let Value::Array(array) = &normalized.metadata["array"] else {
        panic!("metadata array changed")
    };
    assert_eq!(array.as_slice(), [Value::Null]);
    assert!(array.capacity() < 128);
    for metadata in [
        (0..10).fold(Value::Null, |value, _| Value::Array(vec![value])),
        Value::Array(vec![Value::Null; 256]),
    ] {
        let mut frame = Frame::new(FrameData::InputAudio(
            AudioRawFrame::new(vec![1, 2], 16000, 1).unwrap(),
        ));
        frame.metadata.insert("bounded".into(), metadata);
        assert!(super::normalize_input(frame, Direction::Downstream, &cfg).is_err());
    }
}

#[test]
fn hosted_address_is_explicit_bounded_and_rejected_before_application_output_setup() {
    let base = NovaSonicConfig::new(
        "us-east-1",
        "amazon.nova-2-sonic-v1:0",
        "tiffany",
        AwsCredentials::new(ACCESS, SECRET, Some(TOKEN.into())).unwrap(),
    )
    .unwrap();
    // new() can validate syntax before callers provide the pre-resolved address.
    assert!(base.connect_address.is_none());
    assert!(base.validate().is_err());
    let sink = Arc::new(Sink::default());
    assert!(NovaSonicApp::new(base.clone(), Context::default(), sink.clone()).is_err());
    for invalid in [
        "0.0.0.0:443",
        "224.0.0.1:443",
        "[::]:443",
        "[ff02::1]:443",
        "127.0.0.1:80",
    ] {
        let mut config = base.clone();
        config.connect_address = Some(invalid.parse().unwrap());
        assert!(
            config.validate().is_err(),
            "invalid address accepted: {invalid}"
        );
        assert!(NovaSonicApp::new(config, Context::default(), sink.clone()).is_err());
    }
    // Constructor-only checks make no connection and claim no TLS success.
    for valid in ["198.51.100.7:443", "127.0.0.1:443", "[::1]:443"] {
        let mut config = base.clone();
        config.connect_address = Some(valid.parse().unwrap());
        config.validate().unwrap();
        assert!(NovaSonicApp::new(config, Context::default(), sink.clone()).is_ok());
    }
    let mut local = config(
        "http://127.0.0.1:1/model/amazon.nova-2-sonic-v1%3A0/invoke-with-bidirectional-stream",
        true,
    );
    local.validate().unwrap();
    local.connect_address = Some("127.0.0.1:443".parse().unwrap());
    assert!(
        local.validate().is_err(),
        "literal loopback endpoints must not accept a connect-address override"
    );
    assert!(NovaSonicApp::new(local, Context::default(), sink.clone()).is_err());
    assert!(sink.frames.lock().unwrap().is_empty());
}

#[tokio::test]
async fn cancelled_receiver_abort_recovery_keeps_cached_failure_and_retained_transport_join() {
    let mut peer = Peer::new(true).await;
    let mut app = NovaSonicApp::new(
        config(&peer.endpoint, true),
        Context::default(),
        Arc::new(Sink::default()),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let _ = peer.ready().await;
    let state = app.provider.0.clone();
    let pause = Arc::new(Semaphore::new(0));
    *state.recovery_pause.lock().unwrap() = Some(pause);
    // Abort bypasses catch_unwind. This exact observer owns the resumable
    // transport recovery, while the primary cancellation remains an error.
    let receiver = state.task.lock().await.as_ref().unwrap().abort_handle();
    receiver.abort();
    let provider = app.provider.clone();
    let waiter = tokio::spawn(async move { provider.wait_joined().await });
    timeout(LIMIT, async {
        loop {
            if state.receiver_joined.load(Ordering::SeqCst)
                && matches!(state.result.lock().unwrap().as_ref(), Some(Err(_)))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .unwrap();
    let cached = state.result.lock().unwrap().clone().unwrap().unwrap_err();
    assert!(cached.message.contains("receiver task failed"));
    assert!(
        !waiter.is_finished(),
        "first waiter must be paused in recovery"
    );
    assert!(!app.provider.session_joined());
    assert!(state.transport_completion.lock().unwrap().is_some());

    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    assert_eq!(
        state
            .result
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .as_ref()
            .unwrap_err()
            .message,
        cached.message,
        "cancelling recovery must not erase or convert the primary failure"
    );
    assert!(!app.provider.session_joined());
    assert!(state.transport_completion.lock().unwrap().is_some());
    state.recovery_pause.lock().unwrap().take();

    let (first, second) = timeout(LIMIT, async {
        tokio::join!(app.provider.wait_joined(), app.provider.wait_joined())
    })
    .await
    .unwrap();
    let first = first.unwrap_err();
    let second = second.unwrap_err();
    assert!(first.message.contains(&cached.message));
    assert_eq!(first.message, second.message);
    assert!(app.provider.session_joined());
    assert!(state.transport_completion.lock().unwrap().is_none());
    let repeated = app.provider.wait_joined().await.unwrap_err();
    assert_eq!(first.message, repeated.message);
    assert!(state.transport_completion.lock().unwrap().is_none());

    let _ = app
        .worker
        .sender()
        .cancel("fixture receiver abort cleanup")
        .await;
    let failure = expect_failure(&mut app).await;
    assert!(failure.contains("receiver task failed"));
    assert!(peer.join().await.reset);
}

#[tokio::test]
async fn post_ready_receiver_panic_fails_pipeline_and_joins_without_prior_provider_observer() {
    let mut peer = Peer::new(true).await;
    let mut app = NovaSonicApp::new(
        config(&peer.endpoint, true),
        Context::default(),
        Arc::new(Sink::default()),
    )
    .unwrap()
    .start()
    .await
    .unwrap();
    let ready = peer.ready().await;
    app.provider.0.panic_receiver.store(true, Ordering::SeqCst);
    peer.send(vec![ready.nova.event("completionStart", json!({}))])
        .await;
    // expect_failure drains and joins the actual pipeline BEFORE it ever calls
    // provider.wait_joined(). An external observer must not be necessary to
    // turn an unhandled receiver panic into a terminal application failure.
    let failure = expect_failure(&mut app).await;
    assert!(failure.contains("receiver task failed"), "{failure}");
    assert!(!failure.contains(SECRET));
    assert!(!failure.contains(TOKEN));
    assert!(app
        .provider
        .0
        .transport_completion
        .lock()
        .unwrap()
        .is_none());
    assert!(peer.join().await.reset);
}

#[test]
fn transport_diagnostics_preserve_primary_cleanup_forced_abort_and_actual_join_flags() {
    use crate::transport::{TransportError, TransportReport};
    let report = TransportReport {
        primary: Some(TransportError::WriteTimeout),
        cleanup: Some(TransportError::ForcedAbort),
        forced_abort: true,
        writer_joined: true,
        reader_joined: true,
        connection_joined: true,
    };
    let failure = super::transport_outcome(&report).unwrap_err();
    for expected in [
        "write deadline",
        "forced task abort",
        "forced_abort=true",
        "writer_joined=true",
        "reader_joined=true",
        "connection_joined=true",
    ] {
        assert!(
            failure.message.contains(expected),
            "lost {expected}: {failure:?}"
        );
    }
    assert!(!failure.message.contains(SECRET));
    assert!(!failure.message.contains(TOKEN));
    assert!(super::transport_outcome(&TransportReport::default()).is_err());
    let clean = TransportReport {
        writer_joined: true,
        reader_joined: true,
        connection_joined: true,
        ..Default::default()
    };
    assert!(super::transport_outcome(&clean).is_ok());
    let cleanup_only = TransportReport {
        cleanup: Some(TransportError::ForcedAbort),
        forced_abort: true,
        ..Default::default()
    };
    let failure = super::transport_outcome(&cleanup_only).unwrap_err();
    assert!(failure.message.contains("forced task abort"));
    for expected in [
        "writer_joined=false",
        "reader_joined=false",
        "connection_joined=false",
    ] {
        assert!(
            failure.message.contains(expected),
            "lost {expected}: {failure:?}"
        );
    }
}
