//! Bounded synthetic assets and real socket/HTTP ownership fixtures.
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
use serde_json::json;
#[cfg(feature = "user-images")]
use serde_json::Value;
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
pub type Error = Box<dyn std::error::Error + Send + Sync>;
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
pub const WAIT: std::time::Duration = std::time::Duration::from_secs(6);
pub const TEXT: &str = " Inspect synthetic RGB exactly once ";
pub const ANSWER: &str = "native image accepted";
static NEXT: AtomicUsize = AtomicUsize::new(0);
pub struct Bundle(pub PathBuf);
impl Bundle {
    pub fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "pipecat-user-images-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::SeqCst)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
    pub fn put(&self, name: &str, bytes: impl AsRef<[u8]>) -> PathBuf {
        let path = self.0.join(name);
        std::fs::write(&path, bytes).unwrap();
        path
    }
    #[cfg(feature = "user-images")]
    pub fn json(&self, name: &str, value: &Value) -> PathBuf {
        self.put(name, serde_json::to_vec(value).unwrap())
    }
    pub fn source(&self, image: &str, expected: &str) -> PathBuf {
        // Source-shaped YAML, not a normalized ScriptScenario literal.
        self.put("scenario.yaml", format!("name: synthetic image route\nuser:\n  modality: text\nturns:\n  - image: {image}\n    user: {TEXT:?}\n    expect:\n      - event: response\n        text_contains: {expected:?}\n        within_ms: 2500\n"))
    }
}
impl Drop for Bundle {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
pub fn rgb(width: u32, height: u32, color: [u8; 3]) -> Vec<u8> {
    color.repeat((width * height) as usize)
}
pub fn png_image(
    width: u32,
    height: u32,
    color: png::ColorType,
    pixels: &[u8],
    palette: Option<&[u8]>,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, width, height);
        encoder.set_color(color);
        encoder.set_depth(png::BitDepth::Eight);
        if let Some(palette) = palette {
            encoder.set_palette(palette);
        }
        let mut writer = encoder.write_header().unwrap();
        writer.write_image_data(pixels).unwrap();
    }
    bytes
}
pub fn png_rgb(width: u32, height: u32, color: [u8; 3]) -> Vec<u8> {
    png_image(
        width,
        height,
        png::ColorType::Rgb,
        &rgb(width, height, color),
        None,
    )
}
#[cfg(feature = "user-images")]
pub fn jpeg_rgb(width: u16, height: u16, color: [u8; 3]) -> Vec<u8> {
    let mut bytes = vec![];
    jpeg_encoder::Encoder::new(&mut bytes, 95)
        .encode(
            &color.repeat(usize::from(width) * usize::from(height)),
            width,
            height,
            jpeg_encoder::ColorType::Rgb,
        )
        .unwrap();
    bytes
}
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
pub fn jpeg_pixels(bytes: &[u8]) -> ((u32, u32), Vec<u8>) {
    let mut decoder = jpeg_decoder::Decoder::new(std::io::Cursor::new(bytes));
    let pixels = decoder.decode().unwrap();
    let info = decoder.info().unwrap();
    assert_eq!(info.pixel_format, jpeg_decoder::PixelFormat::RGB24);
    ((u32::from(info.width), u32::from(info.height)), pixels)
}
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
pub fn assert_http_image(
    body: &Value,
    text: &str,
    size: (u32, u32),
    pixels: &[u8],
    expected_users: usize,
) {
    use base64::{engine::general_purpose::STANDARD, Engine};
    let messages = body["messages"]
        .as_array()
        .expect("native OpenAI request messages");
    let users = messages
        .iter()
        .filter(|message| message["role"] == "user")
        .collect::<Vec<_>>();
    assert_eq!(
        users.len(),
        expected_users,
        "duplicate original text append/inference"
    );
    let parts = users.last().unwrap()["content"].as_array().unwrap();
    assert_eq!(parts.len(), 2);
    assert_eq!(parts[0], json!({"type":"text", "text":text}));
    assert_eq!(parts[1]["type"], "image_url");
    let url = parts[1]["image_url"]["url"].as_str().unwrap();
    let bytes = STANDARD
        .decode(
            url.strip_prefix("data:image/jpeg;base64,")
                .expect("raw RGB must be encoded by the real standard consumer"),
        )
        .unwrap();
    let (actual_size, actual) = jpeg_pixels(&bytes);
    assert_eq!(actual_size, size);
    assert_eq!(actual.len(), pixels.len());
    // All fixtures have constant color. One JPEG re-encode may introduce small
    // quantization/YCbCr roundoff; a 4-level per-channel bound is justified here.
    assert!(
        actual.iter().zip(pixels).all(|(a, b)| a.abs_diff(*b) <= 4),
        "HTTP JPEG pixels differ from the requested RGB fixture"
    );
    assert!(body.get("tools").is_none_or(Value::is_null));
}

#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
mod native {
    use super::*;
    use crate::image_app::{ImageBot, Trace};
    use pipecat_evals::{bot_host::*, serializer, Cancellation, ImageInputLimits, SessionOptions};
    use pipecat_protocol::rtvi::Envelope;
    use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
    use std::sync::{atomic::AtomicBool, Arc, Mutex};
    use std::time::Duration;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Notify, Semaphore},
        task::JoinHandle,
    };

    pub fn transport() -> SessionConfig {
        SessionConfig {
            connect_timeout: Duration::from_secs(1),
            write_timeout: Duration::from_millis(500),
            close_timeout: Duration::from_millis(80),
            ..Default::default()
        }
    }
    pub fn options() -> SessionOptions {
        SessionOptions {
            transport: transport(),
            handshake_timeout: Duration::from_secs(2),
            default_timeout: Duration::from_secs(2),
            send_after_timeout: Duration::from_millis(200),
            max_duration: WAIT,
            ..Default::default()
        }
    }
    pub fn config() -> EvalBotHostConfig {
        EvalBotHostConfig {
            transport: SessionConfig {
                max_message_bytes: 128 * 1024,
                ..transport()
            },
            startup_timeout: Duration::from_secs(2),
            session_timeout: WAIT,
            host_timeout: WAIT + WAIT,
            shutdown_timeout: Duration::from_secs(2),
            ..Default::default()
        }
    }
    pub struct Host {
        pub url: String,
        pub trace: Arc<Trace>,
        pub cancel: Cancellation,
        task: JoinHandle<Result<EvalBotHostReport, BotHostError>>,
    }
    impl Host {
        pub async fn start(bot: ImageBot) -> Self {
            Self::start_with(bot, config(), Some(ImageInputLimits::default())).await
        }
        pub async fn start_with(
            bot: ImageBot,
            config: EvalBotHostConfig,
            limits: Option<ImageInputLimits>,
        ) -> Self {
            let host = EvalBotHost::bind("127.0.0.1:0".parse().unwrap(), config)
                .await
                .unwrap();
            let host = if let Some(limits) = limits {
                host.with_user_images(limits).unwrap()
            } else {
                host
            };
            let url = format!("ws://{}", host.local_addr().unwrap());
            let trace = bot.trace.clone();
            let cancel = Cancellation::new();
            let owned_cancel = cancel.clone();
            let task = tokio::spawn(async move { host.serve(&bot, owned_cancel).await });
            Self {
                url,
                trace,
                cancel,
                task,
            }
        }
        pub async fn finish(mut self) -> (Result<EvalBotHostReport, String>, Arc<Trace>) {
            self.cancel.cancel();
            let result = match tokio::time::timeout(WAIT, &mut self.task).await {
                Ok(join) => join
                    .map_err(|e| e.to_string())
                    .and_then(|result| result.map_err(|e| e.to_string())),
                Err(_) => {
                    self.task.abort();
                    let joined = self.task.await;
                    Err(format!("host did not cooperatively join: {joined:?}"))
                }
            };
            (result, self.trace)
        }
    }
    pub struct HttpTrace {
        pub requests: Mutex<Vec<Value>>,
        pub arrived: Notify,
        pub gate: Semaphore,
        pub gated: AtomicBool,
        pub joined: AtomicUsize,
        pub active: AtomicUsize,
        pub peer_closed: Notify,
        pub closed_connections: AtomicUsize,
    }
    impl Default for HttpTrace {
        fn default() -> Self {
            Self {
                requests: Mutex::new(vec![]),
                arrived: Notify::new(),
                gate: Semaphore::new(0),
                gated: AtomicBool::new(false),
                joined: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                peer_closed: Notify::new(),
                closed_connections: AtomicUsize::new(0),
            }
        }
    }
    pub struct Http {
        pub url: String,
        pub trace: Arc<HttpTrace>,
        cancel: Cancellation,
        task: JoinHandle<Result<(), String>>,
    }
    async fn request(socket: &mut TcpStream) -> Result<Value, String> {
        let mut data = Vec::new();
        let mut buf = [0; 4096];
        let end = loop {
            let n = socket.read(&mut buf).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("HTTP closed before headers".into());
            }
            data.extend_from_slice(&buf[..n]);
            if data.len() > 2 * 1024 * 1024 {
                return Err("HTTP fixture request bound".into());
            }
            if let Some(index) = data.windows(4).position(|v| v == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let length: usize = std::str::from_utf8(&data[..end])
            .map_err(|e| e.to_string())?
            .lines()
            .find_map(|line| {
                line.to_ascii_lowercase()
                    .strip_prefix("content-length:")
                    .map(|s| s.trim().parse::<usize>())
            })
            .ok_or("missing HTTP content length")?
            .map_err(|e| e.to_string())?;
        if length > 2 * 1024 * 1024 {
            return Err("HTTP fixture body bound".into());
        }
        while data.len() < end + length {
            let n = socket.read(&mut buf).await.map_err(|e| e.to_string())?;
            if n == 0 {
                return Err("HTTP closed before body".into());
            }
            data.extend_from_slice(&buf[..n]);
        }
        serde_json::from_slice(&data[end..end + length]).map_err(|e| e.to_string())
    }
    impl Http {
        pub async fn start() -> Self {
            Self::start_answer(ANSWER).await
        }
        pub async fn start_answer(answer: &str) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!(
                "http://{}/v1/chat/completions",
                listener.local_addr().unwrap()
            );
            let trace = Arc::new(HttpTrace::default());
            let task_trace = trace.clone();
            let cancel = Cancellation::new();
            let task_cancel = cancel.clone();
            let answer = answer.to_owned();
            let task = tokio::spawn(async move {
                let operation = async {
                    loop {
                        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
                        task_trace.active.fetch_add(1, Ordering::SeqCst);
                        let body = request(&mut socket).await?;
                        {
                            let mut requests = task_trace.requests.lock().unwrap();
                            if requests.len() >= 16 {
                                return Err("unexpected HTTP inference count".into());
                            }
                            requests.push(body);
                        }
                        task_trace.arrived.notify_one();
                        if task_trace.gated.load(Ordering::SeqCst) {
                            let mut byte = [0_u8; 1];
                            tokio::select! {
                                permit=task_trace.gate.acquire() => permit.map_err(|e|e.to_string())?.forget(),
                                read=socket.read(&mut byte) => {
                                    let read=read.map_err(|e|e.to_string())?;
                                    if read!=0 { return Err("unexpected HTTP bytes after complete request".into()); }
                                    task_trace.closed_connections.fetch_add(1,Ordering::SeqCst);
                                    task_trace.peer_closed.notify_one();
                                    task_trace.active.fetch_sub(1,Ordering::SeqCst);
                                    continue;
                                }
                            }
                        }
                        let body = format!(
                            "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
                            json!({"choices":[{"delta":{"content":answer},"index":0}]}),
                            json!({"choices":[{"delta":{},"finish_reason":"stop","index":0}]})
                        );
                        socket.write_all(format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.map_err(|e| e.to_string())?;
                        socket.shutdown().await.map_err(|e| e.to_string())?;
                        task_trace.active.fetch_sub(1, Ordering::SeqCst);
                    }
                };
                let result = tokio::select! { _ = task_cancel.cancelled() => Ok(()), result = operation => result };
                task_trace.active.store(0, Ordering::SeqCst);
                task_trace.joined.fetch_add(1, Ordering::SeqCst);
                result
            });
            Self {
                url,
                trace,
                cancel,
                task,
            }
        }
        pub async fn finish(mut self) -> Arc<HttpTrace> {
            self.cancel.cancel();
            let result = tokio::time::timeout(WAIT, &mut self.task).await;
            if result.is_err() {
                self.task.abort();
                let _ = self.task.await;
                panic!("HTTP owner did not join");
            }
            result.unwrap().unwrap().unwrap();
            self.trace
        }
    }
    pub async fn send(socket: &WebSocketSession, envelope: Envelope) -> Result<(), Error> {
        socket
            .sender()
            .send(Packet::Text(envelope.encode()?))
            .await?;
        Ok(())
    }
    pub async fn next(socket: &mut WebSocketSession) -> Result<Envelope, Error> {
        match tokio::time::timeout(WAIT, socket.recv())
            .await?
            .ok_or("socket closed")?
        {
            Packet::Text(text) => Ok(Envelope::parse(text.as_bytes())?),
            Packet::Binary(_) => Err("unexpected binary output".into()),
        }
    }
    pub async fn connect(url: &str) -> Result<WebSocketSession, Error> {
        let mut socket = WebSocketSession::connect(url, transport()).await?;
        send(
            &socket,
            serializer::message(1, "client-ready", json!({"version":"2.2.0"})),
        )
        .await?;
        let ready = next(&mut socket).await?;
        if ready.message_type != "bot-ready" {
            return Err(format!("unexpected readiness {ready:?}").into());
        }
        Ok(socket)
    }
    pub async fn register(
        socket: &WebSocketSession,
        bytes: &[u8],
        mime: &str,
    ) -> Result<(), Error> {
        use base64::{engine::general_purpose::STANDARD, Engine};
        send(
            socket,
            serializer::control(
                2,
                serializer::EVAL_IMAGE,
                json!({"image":STANDARD.encode(bytes), "format":mime}),
            ),
        )
        .await
    }
    pub async fn text(socket: &WebSocketSession, value: &str) -> Result<(), Error> {
        send(
            socket,
            serializer::message(
                3,
                "send-text",
                json!({"content":value,"options":{"audio_response":false,"run_immediately":true}}),
            ),
        )
        .await
    }
    pub async fn response(socket: &mut WebSocketSession) -> Result<String, Error> {
        let mut text = String::new();
        for _ in 0..128 {
            let event = next(socket).await?;
            if event.message_type == "bot-llm-text" {
                text.push_str(
                    event
                        .data
                        .as_ref()
                        .and_then(|v| v["text"].as_str())
                        .ok_or("bad LLM text event")?,
                );
            }
            if event.message_type == "bot-llm-stopped" {
                return Ok(text);
            }
            if event.message_type == "error" {
                return Err(format!("native error: {event:?}").into());
            }
        }
        Err("response event count bound".into())
    }
    pub async fn close(socket: WebSocketSession) {
        let _ = tokio::time::timeout(WAIT, socket.finish())
            .await
            .expect("socket owner join deadline");
    }
    pub fn assert_joined(trace: &Trace, sessions: usize, http: &HttpTrace) {
        assert_eq!(
            trace.cleaned.load(Ordering::SeqCst),
            sessions,
            "application cleanup count"
        );
        assert_eq!(
            trace.dropped.load(Ordering::SeqCst),
            sessions,
            "pipeline owner still retained"
        );
        assert_eq!(http.joined.load(Ordering::SeqCst), 1);
        assert_eq!(http.active.load(Ordering::SeqCst), 0);
    }
}
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
pub use native::*;
