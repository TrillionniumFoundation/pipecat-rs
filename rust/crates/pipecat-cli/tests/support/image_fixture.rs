//! Native API fixture; all media uses the production ReceiptBackend and shared output.
#![allow(dead_code)]
#[path = "chunk_fixture.rs"]
pub(crate) mod chunk;
pub use chunk::{bounded, control, next, send, TestResult, CHUNK_BYTES};
use futures_util::{SinkExt, StreamExt};
use pipecat_cli::receipt_output::*;
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use pipecat_transports::WebSocketSession;
use serde_json::{json, Value};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::oneshot,
    task::JoinHandle,
};
use tokio_tungstenite::tungstenite::Message;

pub fn images_config() -> ImageChannelConfig {
    ImageChannelConfig {
        pairing_timeout: Duration::from_millis(500),
        operation_timeout: Duration::from_secs(2),
        disposal_timeout: Duration::from_millis(200),
        ..Default::default()
    }
}
struct Feed(Mutex<Option<oneshot::Sender<ProcessorContext>>>);
#[async_trait]
impl FrameProcessor for Feed {
    fn name(&self) -> &str {
        "native-image-fixture-feed"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(ready) = self.0.lock().unwrap().take() {
            let _ = ready.send(ctx.clone());
        }
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
struct FailStart;
#[async_trait]
impl FrameProcessor for FailStart {
    fn name(&self) -> &str {
        "injected-image-start-failure"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            return Err(ProcessorError::new("injected image start failure"));
        }
        ctx.push(frame, direction).await
    }
}
pub struct Native {
    pub client: Option<WebSocketSession>,
    pub backend: Arc<ReceiptBackend>,
    pub offer: Value,
    pub feed: ProcessorContext,
    pub output: OutputControl,
    task: Option<JoinHandle<TestResult<ReceiptRunReport>>>,
    fail_setup: bool,
}
impl Native {
    pub async fn start(images: ImageChannelConfig) -> TestResult<Self> {
        Self::start_inner(images, false).await
    }
    pub async fn start_partial_failure(images: ImageChannelConfig) -> TestResult<Self> {
        Self::start_inner(images, true).await
    }
    async fn start_inner(images: ImageChannelConfig, fail_setup: bool) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
        let image_listener = TcpListener::bind("127.0.0.1:0").await.map_err(err)?;
        let url = format!("ws://{}", listener.local_addr().map_err(err)?);
        let (ready, waiting) = oneshot::channel();
        let (feed_ready, feed_wait) = oneshot::channel();
        let task = tokio::spawn(async move {
            let stream = listener.accept().await.map_err(err)?.0;
            let socket = WebSocketSession::accept(stream, chunk::config())
                .await
                .map_err(err)?;
            let owner = ReceiptSession::negotiate_with_images(
                socket,
                ReceiptConfig {
                    handshake_timeout: Duration::from_secs(1),
                    chunk_timeout: Duration::from_secs(2),
                    cutoff_timeout: Duration::from_secs(1),
                    enqueue_timeout: Duration::from_secs(1),
                    drain_timeout: Duration::from_secs(3),
                },
                24000,
                image_listener,
                images,
            )
            .await
            .map_err(err)?;
            let backend = owner.backend();
            let output = ReceiptOutput::new_with_images(
                "real-native-image-output",
                OutputTransportConfig {
                    sample_rate: Some(24000),
                    channels: 1,
                    chunk_duration: Duration::from_millis(40),
                    auto_silence: false,
                    end_silence: Duration::ZERO,
                    write_timeout: Duration::from_secs(3),
                    queue_capacity: 8,
                    queue_byte_capacity: 20 * 1024 * 1024,
                    ..Default::default()
                },
                backend.clone(),
                None,
                LiveVideoOutputConfig {
                    queue_capacity: 4,
                    queue_byte_capacity: 20 * 1024 * 1024,
                    framerate: None,
                },
            )
            .map_err(err)?;
            let output_control = output.control();
            let _ = ready.send((backend, output_control));
            let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![
                Arc::new(Feed(Mutex::new(Some(feed_ready)))),
                Arc::new(output),
            ];
            if fail_setup {
                processors.push(Arc::new(FailStart));
            }
            owner
                .run(
                    Pipeline::new(processors),
                    PipelineConfig {
                        queue_capacity: 16,
                        system_queue_capacity: 16,
                        shutdown_timeout: Duration::from_secs(2),
                        ..Default::default()
                    },
                    ReceiptRunConfig {
                        input_rate: 16000,
                        idle_timeout: Duration::from_secs(10),
                        history: None,
                        initial_frames: vec![],
                    },
                )
                .await
                .map_err(|error| format!("{error}; typed={error:?}"))
        });
        let mut client = match WebSocketSession::connect(&url, chunk::config()).await {
            Ok(c) => c,
            Err(e) => {
                task.abort();
                let _ = task.await;
                return Err(err(e));
            }
        };
        let setup:TestResult<_>=async{
            let offer=control(next(&mut client).await?)?;
            if offer["image_channel"]["type"]!="image-channel-offer"{return Err("missing native image offer".into());}
            send(&client,json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],
                "chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true,"independent_images":true})).await?;
            let (backend,output)=bounded(async{waiting.await.map_err(err)}).await?;
            let feed=bounded(async{feed_wait.await.map_err(err)}).await?;
            bounded(async{
                loop {
                    let done=if fail_setup { backend.image_status().is_some_and(|s|s.closed&&!s.owner_alive&&s.open_streams==0) } else { output.is_ready() };
                    if done { return Ok(()); } tokio::task::yield_now().await;
                }
            }).await?;
            Ok((backend,output,feed,offer["image_channel"].clone()))
        }.await;
        match setup {
            Ok((backend, output, feed, offer)) => Ok(Self {
                client: Some(client),
                backend,
                output,
                feed,
                offer,
                task: Some(task),
                fail_setup,
            }),
            Err(e) => {
                let close = client.cancel().await;
                task.abort();
                let join = task.await;
                Err(format!("{e}; setup cleanup {close:?}/{:?}", join.err()))
            }
        }
    }
    pub fn status(&self) -> ImageChannelStatus {
        self.backend.image_status().expect("enabled image status")
    }
    pub async fn push(&self, frame: Frame) -> TestResult<()> {
        self.feed
            .push(frame, Direction::Downstream)
            .await
            .map_err(err)
    }
    pub async fn wait_status(
        &self,
        predicate: impl Fn(&ImageChannelStatus) -> bool,
    ) -> TestResult<ImageChannelStatus> {
        bounded(async {
            loop {
                let s = self.status();
                if predicate(&s) {
                    return Ok(s);
                }
                tokio::task::yield_now().await;
            }
        })
        .await
    }
    pub async fn pair(&self) -> TestResult<TcpStream> {
        pair(&self.offer).await
    }
    pub async fn replacement(&mut self) -> TestResult<Value> {
        let value = control(next(self.client.as_mut().unwrap()).await?)?;
        if value["type"] != "image-channel-offer"
            || value["incarnation"].as_u64() <= self.offer["incarnation"].as_u64()
            || value["pairing_nonce"] == self.offer["pairing_nonce"]
        {
            return Err(format!("invalid replacement {value}"));
        }
        self.offer = value.clone();
        Ok(value)
    }
    pub async fn pcm(&mut self) -> TestResult<Value> {
        let Kind::Audio(audio) = next(self.client.as_mut().unwrap()).await? else {
            return Err("missing actual PCM".into());
        };
        if audio.audio != vec![7; CHUNK_BYTES]
            || audio.sample_rate != 24000
            || audio.num_channels != 1
        {
            return Err("PCM mismatch".into());
        }
        serde_json::from_str(&audio.name).map_err(err)
    }
    pub async fn ack(&self, meta: &Value) -> TestResult<()> {
        send(
            self.client.as_ref().unwrap(),
            json!({
        "type":"chunk-complete","version":2,"session_id":meta["session_id"],"epoch":meta["epoch"],
        "chunk_id":meta["chunk_id"],"receipt_nonce":meta["receipt_nonce"]}),
        )
        .await
    }
    pub async fn cutoff(&mut self) -> TestResult<Value> {
        let mut value = control(next(self.client.as_mut().unwrap()).await?)?;
        if value["type"] != "playback-cutoff" {
            return Err(format!("expected cutoff, got {value}"));
        }
        value["type"] = json!("cutoff-complete");
        send(self.client.as_ref().unwrap(), value.clone()).await?;
        Ok(value)
    }
    pub async fn end(&mut self) -> TestResult<()> {
        send(self.client.as_ref().unwrap(), json!({"type":"end"})).await?;
        bounded(async {
            let mut n = 0;
            while let Some(packet) = self.client.as_mut().unwrap().recv().await {
                let pipecat_transports::Packet::Binary(bytes) = packet else {
                    return Err("terminal non-binary".into());
                };
                let value = control(
                    pb::Frame::parse(&bytes)
                        .map_err(err)?
                        .frame
                        .ok_or("empty terminal packet")?,
                )?;
                if value["type"] == "image-channel-offer" {
                    self.offer = value;
                } else if value["type"] == "playback-cutoff" {
                    let mut ack = value;
                    ack["type"] = json!("cutoff-complete");
                    send(self.client.as_ref().unwrap(), ack).await?;
                } else {
                    return Err(format!("unexpected terminal packet {value}"));
                }
                n += 1;
                if n > 8 {
                    return Err("terminal control budget".into());
                }
            }
            Ok(())
        })
        .await
    }
    /// Joins every caller-owned task even after an observation error. A fallback
    /// abort is joined and reported as failure, never accepted as normal cleanup.
    pub async fn close(mut self, expected_failure: bool) -> TestResult<()> {
        let mut errors = vec![];
        if let Some(client) = self.client.take() {
            if let Err(e) = client.cancel().await {
                let outcome = chunk::peer_exit(e);
                eprintln!(
                    "raw audio-client cleanup (expected_failure={expected_failure}): {outcome}"
                );
                if !(expected_failure && outcome.starts_with("expected peer shutdown: ")) {
                    errors.push(format!("client join: {outcome}"));
                }
            }
        }
        let mut task = self.task.take().unwrap();
        let result = match tokio::time::timeout(Duration::from_secs(5), &mut task).await {
            Ok(Ok(r)) => r,
            Ok(Err(e)) => Err(err(e)),
            Err(_) => {
                task.abort();
                let _ = task.await;
                errors.push("owner abort required and joined".into());
                Err("owner deadline".into())
            }
        };
        let status = self.status();
        eprintln!(
            "native owner error={:?}; image status={status:?}; cleanup={errors:?}",
            result.as_ref().err()
        );
        if status.owner_alive
            || status.pairing
            || status.idle
            || status.request_id.is_some()
            || status.open_streams != 0
        {
            errors.push("image ownership not settled".into());
        }
        match result {
            Ok(r) if expected_failure => errors.push(format!(
                "unexpected successful owner ({} errors)",
                r.pipeline.total_errors
            )),
            Ok(r) if r.pipeline.total_errors != 0 => {
                errors.push(format!("pipeline errors {:?}", r.pipeline.errors))
            }
            Ok(_) => {}
            Err(e)
                if expected_failure
                    && !e.contains("unverified")
                    && !e.contains("disposal deadline")
                    && (if self.fail_setup {
                        e.contains("Pipeline(Execution(\"injected-image-start-failure failed processing Start\"))")
                    } else {
                        e.contains("v2 client cancelled") || e.contains("v2 client disconnected")
                    }) => {}
            Err(e) => errors.push(e),
        }
        if errors.is_empty() {
            Ok(())
        } else {
            Err(format!("{errors:?}"))
        }
    }
}
impl Drop for Native {
    fn drop(&mut self) {
        if let Some(task) = &self.task {
            task.abort();
        }
        self.backend.fail_closed();
    }
}
pub fn err(e: impl std::fmt::Display) -> String {
    e.to_string()
}
pub fn pcm() -> Frame {
    Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![7; CHUNK_BYTES], 24000, 1).unwrap(),
    ))
}
pub fn image(value: u8, large: bool) -> Frame {
    Frame::new(FrameData::OutputImage(ImageRawFrame {
        image: vec![value; if large { IMAGE_MAX_PIXELS_BYTES } else { 12 }],
        size: if large { (1024, 2048) } else { (2, 2) },
        format: Some(if large { "RGBA" } else { "RGB" }.into()),
    }))
}
pub fn pair_message(offer: &Value) -> Value {
    json!({"type":"image-channel-pair","version":1,"session_id":offer["session_id"],
    "destination":null,"incarnation":offer["incarnation"],"pairing_nonce":offer["pairing_nonce"]})
}
pub async fn pair(offer: &Value) -> TestResult<TcpStream> {
    pair_request(offer, pair_message(offer)).await
}
pub async fn pair_request(offer: &Value, request: Value) -> TestResult<TcpStream> {
    let address = offer["address"].as_str().ok_or("missing image address")?;
    let stream = TcpStream::connect(address).await.map_err(err)?;
    let url = format!("ws://{address}");
    let (mut socket, _) = bounded(async {
        tokio_tungstenite::client_async(url, stream)
            .await
            .map_err(err)
    })
    .await?;
    socket
        .send(Message::Text(request.to_string().into()))
        .await
        .map_err(err)?;
    let paired =
        bounded(async { socket.next().await.ok_or("no pairing reply")?.map_err(err) }).await?;
    let Message::Text(text) = paired else {
        return Err("non-text pairing reply".into());
    };
    let value: Value = serde_json::from_str(&text).map_err(err)?;
    if value["type"] != "image-channel-paired" || value["incarnation"] != offer["incarnation"] {
        return Err("wrong pair confirmation".into());
    }
    Ok(socket.into_inner())
}
pub async fn read_message(peer: &mut TcpStream) -> TestResult<(Value, Vec<u8>)> {
    bounded(async {
        let mut header = [0; 2];
        peer.read_exact(&mut header).await.map_err(err)?;
        if header[0] != 0x82 || header[1] & 0x80 != 0 {
            return Err(format!("unexpected image framing {header:?}"));
        }
        let len = match header[1] {
            126 => peer.read_u16().await.map_err(err)? as u64,
            127 => peer.read_u64().await.map_err(err)?,
            n => n as u64,
        };
        if len > (IMAGE_MAX_PIXELS_BYTES + IMAGE_HEADER_BYTES + 12) as u64 {
            return Err("wire image too large".into());
        }
        let mut message = vec![0; len as usize];
        peer.read_exact(&mut message).await.map_err(err)?;
        decode(&message)
    })
    .await
}
pub fn decode(message: &[u8]) -> TestResult<(Value, Vec<u8>)> {
    if message.len() < 12 || &message[..8] != b"PCIMG001" {
        return Err("invalid image magic".into());
    }
    let n = u32::from_be_bytes(message[8..12].try_into().unwrap()) as usize;
    if n > IMAGE_HEADER_BYTES || 12 + n > message.len() {
        return Err("invalid image header length".into());
    }
    let header = serde_json::from_slice(&message[12..12 + n]).map_err(err)?;
    Ok((header, message[12 + n..].to_vec()))
}
pub async fn drain(peer: &mut TcpStream) -> TestResult<Vec<u8>> {
    bounded(async {
        let mut bytes = vec![];
        let mut buffer = [0; 16384];
        loop {
            match peer.read(&mut buffer).await {
                Ok(0) => return Ok(bytes),
                Ok(n) => {
                    bytes.extend_from_slice(&buffer[..n]);
                    if bytes.len() > IMAGE_MAX_PIXELS_BYTES + 2048 {
                        return Err("peer drain budget".into());
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::ConnectionReset => return Ok(bytes),
                Err(e) => return Err(err(e)),
            }
        }
    })
    .await
}
pub fn partial(bytes: &[u8], name: &str, value: u8) -> TestResult<()> {
    if bytes.len() < 22 || bytes[0] != 0x82 || bytes[1] != 127 {
        return Err(format!(
            "missing physical binary prefix: {} bytes",
            bytes.len()
        ));
    }
    let total = u64::from_be_bytes(bytes[2..10].try_into().unwrap()) as usize;
    if bytes.len() >= total + 10 {
        return Err(format!(
            "cancelled frame already completed: {} of {}",
            bytes.len(),
            total + 10
        ));
    }
    let payload = &bytes[10..];
    if &payload[..8] != b"PCIMG001" {
        return Err("partial image magic".into());
    }
    let n = u32::from_be_bytes(payload[8..12].try_into().unwrap()) as usize;
    if n > IMAGE_HEADER_BYTES || payload.len() <= 12 + n {
        return Err("partial frame lacks actual pixels".into());
    }
    let header: Value = serde_json::from_slice(&payload[12..12 + n]).map_err(err)?;
    if header["pixel_bytes"] != IMAGE_MAX_PIXELS_BYTES
        || payload[12 + n..].iter().any(|b| *b != value)
    {
        return Err("partial pixel prefix mismatch".into());
    }
    eprintln!(
        "PHYSICAL_PARTIAL {name}: retained={} expected={} header={header}",
        bytes.len(),
        total + 10
    );
    if let Ok(directory) = std::env::var("PIPECAT_IMAGE_EVIDENCE_DIR") {
        std::fs::create_dir_all(&directory).map_err(err)?;
        let path = std::path::Path::new(&directory).join(format!("{name}.bin"));
        // No accidental overwrite can hide an earlier failed observation.
        use std::io::Write;
        let mut file = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .map_err(err)?;
        file.write_all(bytes).map_err(err)?;
    }
    Ok(())
}
pub async fn prefix(peer: &mut TcpStream) -> TestResult<Vec<u8>> {
    let mut bytes = vec![0; 512];
    bounded(async {
        peer.read_exact(&mut bytes).await.map_err(err)?;
        Ok(())
    })
    .await?;
    Ok(bytes)
}
pub fn exact(
    header: &Value,
    pixels: &[u8],
    offer: &Value,
    value: u8,
    length: usize,
) -> TestResult<()> {
    if header["version"] != 1
        || header["session_id"] != offer["session_id"]
        || header["incarnation"] != offer["incarnation"]
        || header["destination"] != Value::Null
        || header["request_id"].as_u64().unwrap_or(0) == 0
        || header["pixel_bytes"] != length
        || pixels != vec![value; length]
    {
        return Err(format!("image mismatch: {header}, bytes={}", pixels.len()));
    }
    Ok(())
}
