//! Bounded synthetic assets and real socket/HTTP ownership fixtures.
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
use serde_json::{json, Value};
use std::{
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};
pub type Error = Box<dyn std::error::Error + Send + Sync>;
pub const WAIT: Duration = Duration::from_secs(6);
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
mod http {
    use super::*;
    use pipecat_evals::Cancellation;
    use std::sync::{atomic::AtomicBool, Arc, Mutex};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::{TcpListener, TcpStream},
        sync::{Notify, Semaphore},
        task::JoinHandle,
    };
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
}
#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
pub use http::*;
