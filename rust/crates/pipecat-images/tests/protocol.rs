use base64::Engine;
use pipecat_audio::PcmFormat;
use pipecat_core::*;
use pipecat_images::*;
use pipecat_media_transport::*;
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
};

#[derive(Debug)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}
#[derive(Clone)]
struct Reply {
    status: u16,
    body: Vec<u8>,
    chunked: bool,
    delay: Duration,
    extra: String,
}
impl Reply {
    fn json(status: u16, value: Value) -> Self {
        Self {
            status,
            body: value.to_string().into_bytes(),
            chunked: false,
            delay: Duration::ZERO,
            extra: String::new(),
        }
    }
}
async fn read_request(socket: &mut TcpStream) -> Request {
    let mut bytes = Vec::new();
    let split = loop {
        let mut block = [0u8; 4096];
        let count = socket.read(&mut block).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&block[..count]);
        assert!(bytes.len() < 25 * 1024 * 1024);
        if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break index;
        }
    };
    let head = std::str::from_utf8(&bytes[..split]).unwrap();
    let mut lines = head.split("\r\n");
    let first: Vec<_> = lines.next().unwrap().split_whitespace().collect();
    let method = first[0].into();
    let path = first[1].into();
    let headers: BTreeMap<_, _> = lines
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_ascii_lowercase(), value.trim().to_string())
        })
        .collect();
    assert!(!headers.contains_key("transfer-encoding"));
    let length = headers
        .get("content-length")
        .map(|s| s.parse::<usize>().unwrap())
        .unwrap_or(0);
    let mut body = bytes[split + 4..].to_vec();
    while body.len() < length {
        let mut block = [0u8; 4096];
        let count = socket.read(&mut block).await.unwrap();
        assert!(count > 0);
        body.extend_from_slice(&block[..count]);
    }
    assert_eq!(body.len(), length);
    Request {
        method,
        path,
        headers,
        body,
    }
}
async fn write_reply(socket: &mut TcpStream, reply: Reply, keepalive: bool) {
    tokio::time::sleep(reply.delay).await;
    let connection = if keepalive { "keep-alive" } else { "close" };
    let framing = if reply.chunked {
        "Transfer-Encoding: chunked\r\n".into()
    } else {
        format!("Content-Length: {}\r\n", reply.body.len())
    };
    let header = format!(
        "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nConnection: {}\r\n{}{}\r\n",
        reply.status, connection, framing, reply.extra
    );
    if socket.write_all(header.as_bytes()).await.is_err() {
        return;
    }
    if reply.chunked {
        for chunk in reply.body.chunks(13) {
            if socket
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .is_err()
            {
                return;
            }
            if socket.write_all(chunk).await.is_err() {
                return;
            }
            if socket.write_all(b"\r\n").await.is_err() {
                return;
            }
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    } else {
        let _ = socket.write_all(&reply.body).await;
    }
}
async fn server(
    replies: Vec<Reply>,
) -> (
    String,
    mpsc::UnboundedReceiver<Request>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let (sender, receiver) = mpsc::unbounded_channel();
    let handle = tokio::spawn(async move {
        for reply in replies {
            let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let request = tokio::time::timeout(Duration::from_secs(5), read_request(&mut socket))
                .await
                .unwrap();
            let _ = sender.send(request);
            write_reply(&mut socket, reply, false).await;
        }
    });
    (url, receiver, handle)
}
fn config(url: String) -> ImageGenerationConfig {
    let mut config = ImageGenerationConfig::new("test-secret");
    config.base_url = format!("{url}/v1");
    config.allow_loopback_api = true;
    config.limits.request_timeout = Duration::from_secs(3);
    config
}
fn png(mode: png::ColorType) -> Vec<u8> {
    let mut bytes = Vec::new();
    {
        let mut encoder = png::Encoder::new(&mut bytes, 2, 1);
        encoder.set_color(mode);
        encoder.set_depth(png::BitDepth::Eight);
        let data: &[u8] = match mode {
            png::ColorType::Rgb => &[255, 0, 0, 3, 4, 5],
            png::ColorType::Rgba => &[255, 0, 0, 0, 3, 4, 5, 127],
            png::ColorType::Grayscale => &[42, 97],
            png::ColorType::GrayscaleAlpha => &[42, 0, 97, 127],
            _ => panic!(),
        };
        encoder
            .write_header()
            .unwrap()
            .write_image_data(data)
            .unwrap();
    }
    bytes
}
fn inline(bytes: &[u8]) -> Reply {
    Reply::json(
        200,
        json!({"created":0,"data":[{"b64_json": base64::engine::general_purpose::STANDARD.encode(bytes)}]}),
    )
}

#[tokio::test]
async fn exact_request_inline_rgb_alpha_and_source_defaults() {
    for mode in [
        png::ColorType::Rgb,
        png::ColorType::Rgba,
        png::ColorType::Grayscale,
        png::ColorType::GrayscaleAlpha,
    ] {
        let (url, mut requests, task) = server(vec![inline(&png(mode))]).await;
        let image = OpenAiImageClient::new(config(url))
            .unwrap()
            .generate("a red square", &CancellationToken::new())
            .await
            .unwrap();
        let request = requests.recv().await.unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(request.path, "/v1/images/generations");
        assert_eq!(request.headers["authorization"], "Bearer test-secret");
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap(),
            json!({"prompt":"a red square","model":"gpt-image-2.5-flare","n":1,"size":null})
        );
        assert_eq!(image.image.size, (2, 1));
        assert_eq!(image.image.format.as_deref(), Some("RGB"));
        assert_eq!(image.url, None);
        assert_eq!(
            image.image.image,
            if matches!(mode, png::ColorType::Rgb | png::ColorType::Rgba) {
                vec![255, 0, 0, 3, 4, 5]
            } else {
                vec![42, 42, 42, 97, 97, 97]
            }
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn jpeg_is_really_decoded() {
    let (url, _, task) = server(vec![inline(include_bytes!("fixtures/source-cat.jpg"))]).await;
    let image = OpenAiImageClient::new(config(url))
        .unwrap()
        .generate("cat", &CancellationToken::new())
        .await
        .unwrap();
    assert!(image.image.size.0 > 100);
    assert!(image.image.size.1 > 100);
    assert_eq!(
        image.image.image.len(),
        image.image.size.0 as usize * image.image.size.1 as usize * 3
    );
    assert!(image.image.image.windows(3).any(|p| p != [0, 0, 0]));
    task.await.unwrap();
}
#[tokio::test]
async fn hosted_url_wins_and_never_receives_auth() {
    let bytes = png(png::ColorType::Rgba);
    let (image_origin, mut downloads, download_task) = server(vec![Reply {
        body: bytes,
        ..Reply::json(200, json!(null))
    }])
    .await;
    let hosted = format!("{image_origin}/image.png?signature=private");
    let (api, _, api_task) = server(vec![Reply::json(
        200,
        json!({"data":[{"url":hosted,"b64_json":"invalid!"}]}),
    )])
    .await;
    let mut config = config(api);
    config.hosted_images = HostedImagePolicy::loopback_fixtures(&[&image_origin]).unwrap();
    let image = OpenAiImageClient::new(config)
        .unwrap()
        .generate("test", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(image.url.as_deref(), Some(hosted.as_str()));
    assert_eq!(image.image.image, vec![255, 0, 0, 3, 4, 5]);
    let download = downloads.recv().await.unwrap();
    assert_eq!(download.method, "GET");
    assert!(!download.headers.contains_key("authorization"));
    assert!(!download.headers.contains_key("cookie"));
    api_task.await.unwrap();
    download_task.await.unwrap();
}
#[tokio::test]
async fn missing_malformed_and_oversize_responses() {
    for (reply, expected) in [
        (Reply::json(200, json!({"data":[]})), ImageError::NoImage),
        (Reply::json(200, json!({"data":[{}]})), ImageError::NoImage),
        (
            Reply::json(200, json!({"data":[{"b64_json":"!!!!"}]})),
            ImageError::Protocol,
        ),
        (
            Reply::json(200, json!({"data":[{"b64_json":"a"}]})),
            ImageError::Protocol,
        ),
        (inline(b"GIF89a"), ImageError::Decode),
        (
            Reply {
                body: b"{bad".to_vec(),
                ..Reply::json(200, json!(null))
            },
            ImageError::Protocol,
        ),
        (
            Reply::json(401, json!({"error":"secret-provider-body"})),
            ImageError::Http(401),
        ),
        (
            Reply {
                extra: "Content-Encoding: gzip\r\n".into(),
                ..inline(&png(png::ColorType::Rgb))
            },
            ImageError::Protocol,
        ),
    ] {
        let (url, _, task) = server(vec![reply]).await;
        let result = OpenAiImageClient::new(config(url))
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await;
        assert_eq!(result, Err(expected));
        task.await.unwrap();
    }
    for chunked in [false, true] {
        let (url, _, task) = server(vec![Reply {
            chunked,
            ..inline(&png(png::ColorType::Rgb))
        }])
        .await;
        let mut c = config(url);
        c.limits.max_response_bytes = 16;
        assert_eq!(
            OpenAiImageClient::new(c)
                .unwrap()
                .generate("x", &CancellationToken::new())
                .await,
            Err(ImageError::TooLarge)
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn compressed_and_pixel_limits_and_truncation() {
    let original = png(png::ColorType::Rgb);
    for kind in 0..4 {
        let mut bytes = original.clone();
        if kind == 2 {
            bytes.truncate(bytes.len() - 2);
        }
        if kind == 3 {
            bytes[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
        }
        let (url, _, task) = server(vec![inline(&bytes)]).await;
        let mut c = config(url);
        if kind == 0 {
            c.limits.max_encoded_image_bytes = 8;
        }
        if kind == 1 {
            c.limits.max_pixels = 1;
        }
        let result = OpenAiImageClient::new(c)
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await;
        assert_eq!(
            result,
            Err(if kind == 2 {
                ImageError::Decode
            } else {
                ImageError::TooLarge
            })
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn address_allowlist_and_redirects_fail_closed() {
    for value in [
        "http://169.254.169.254/latest/meta-data",
        "https://127.0.0.1/test",
        "https://10.0.0.1/x",
        "https://[::1]/x",
        "https://example.com/x",
        "https://user:pass@example.com/x",
        "https://example.com/x#frag",
        "file:///etc/passwd",
    ] {
        let (url,_,task)=server(vec![Reply::json(200,json!({"data":[{"url":value,"b64_json":base64::engine::general_purpose::STANDARD.encode(png(png::ColorType::Rgb))}]}))]).await;
        assert_eq!(
            OpenAiImageClient::new(config(url))
                .unwrap()
                .generate("x", &CancellationToken::new())
                .await,
            Err(ImageError::AddressDenied)
        );
        task.await.unwrap();
    }
    let forbidden = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let location = format!("http://{}/stolen", forbidden.local_addr().unwrap());
    for hosted in [false, true] {
        let reply = Reply {
            status: 302,
            extra: format!("Location: {location}\r\n"),
            ..Reply::json(200, json!(null))
        };
        let (origin, _, task) = server(vec![reply]).await;
        let (c, api_task) = if hosted {
            let (api, _, api_task) = server(vec![Reply::json(
                200,
                json!({"data":[{"url":format!("{origin}/image")}]}),
            )])
            .await;
            let mut c = config(api);
            c.hosted_images = HostedImagePolicy::loopback_fixtures(&[&origin]).unwrap();
            (c, Some(api_task))
        } else {
            (config(origin), None)
        };
        assert_eq!(
            OpenAiImageClient::new(c)
                .unwrap()
                .generate("x", &CancellationToken::new())
                .await,
            Err(ImageError::Http(302))
        );
        task.await.unwrap();
        if let Some(task) = api_task {
            task.await.unwrap();
        }
    }
    assert!(
        tokio::time::timeout(Duration::from_millis(40), forbidden.accept())
            .await
            .is_err()
    );
}
#[tokio::test]
async fn trusted_origin_does_not_authorize_private_network() {
    for origin in [
        "https://127.0.0.1",
        "https://169.254.169.254",
        "https://[::ffff:127.0.0.1]",
        "https://localhost",
    ] {
        let (api, _, task) = server(vec![Reply::json(
            200,
            json!({"data":[{"url":format!("{origin}/image")}]}),
        )])
        .await;
        let mut c = config(api);
        c.hosted_images = HostedImagePolicy::https_origins(&[origin]).unwrap();
        assert_eq!(
            OpenAiImageClient::new(c)
                .unwrap()
                .generate("x", &CancellationToken::new())
                .await,
            Err(ImageError::AddressDenied)
        );
        task.await.unwrap();
    }
    assert!(HostedImagePolicy::loopback_fixtures(&["http://localhost"]).is_err());
    assert!(HostedImagePolicy::loopback_fixtures(&["http://10.0.0.1"]).is_err());
}
#[tokio::test]
async fn cancellation_timeout_and_direct_request_backpressure() {
    for timeout in [false, true] {
        let (url, mut requests, task) = server(vec![Reply {
            delay: Duration::from_millis(150),
            ..inline(&png(png::ColorType::Rgb))
        }])
        .await;
        let mut c = config(url);
        c.limits.max_pending = 1;
        if timeout {
            c.limits.request_timeout = Duration::from_millis(40);
        }
        let client = OpenAiImageClient::new(c).unwrap();
        let token = CancellationToken::new();
        let copy = client.clone();
        let other = token.clone();
        let pending = tokio::spawn(async move { copy.generate("x", &other).await });
        requests.recv().await.unwrap();
        assert_eq!(
            client.generate("y", &CancellationToken::new()).await,
            Err(ImageError::Backpressure)
        );
        if !timeout {
            token.cancel();
        }
        assert_eq!(
            pending.await.unwrap(),
            Err(if timeout {
                ImageError::Timeout
            } else {
                ImageError::Cancelled
            })
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn drop_request_closes_its_download() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let (api, _, api_task) = server(vec![Reply::json(
        200,
        json!({"data":[{"url":format!("{origin}/image")}]}),
    )])
    .await;
    let mut c = config(api);
    c.hosted_images = HostedImagePolicy::loopback_fixtures(&[&origin]).unwrap();
    let client = OpenAiImageClient::new(c).unwrap();
    let pending =
        tokio::spawn(async move { client.generate("x", &CancellationToken::new()).await });
    let (mut socket, _) = listener.accept().await.unwrap();
    read_request(&mut socket).await;
    pending.abort();
    assert!(pending.await.unwrap_err().is_cancelled());
    let mut b = [0u8; 1];
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), socket.read(&mut b))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    api_task.await.unwrap();
}
#[derive(Default)]
struct Pixels(Mutex<Vec<Frame>>);
#[async_trait]
impl OutputBackend for Pixels {
    async fn start(
        &self,
        _: PcmFormat,
        _: &[Option<String>],
    ) -> std::result::Result<(), ProcessorError> {
        Ok(())
    }
    async fn play_audio(&self, _: &Frame) -> std::result::Result<PlaybackReceipt, ProcessorError> {
        Ok(PlaybackReceipt::Discarded)
    }
    async fn interrupt(&self, _: Option<&str>) -> std::result::Result<(), ProcessorError> {
        Ok(())
    }
    async fn stop(&self) -> std::result::Result<(), ProcessorError> {
        Ok(())
    }
    async fn write_image(&self, frame: &Frame) -> std::result::Result<(), ProcessorError> {
        self.0.lock().unwrap().push(frame.clone());
        Ok(())
    }
}
fn output(sink: Arc<Pixels>, capacity: usize) -> Arc<OutputTransport> {
    Arc::new(
        OutputTransport::new(
            "pixels",
            OutputTransportConfig {
                auto_silence: false,
                end_silence: Duration::ZERO,
                queue_byte_capacity: capacity,
                ..Default::default()
            },
            sink,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    )
}
#[tokio::test]
async fn real_pipeline_end_and_stop_deliver_ordered_rgb_and_identity() {
    for stop in [false, true] {
        let (url, mut requests, task) = server(vec![
            inline(&png(png::ColorType::Rgba)),
            inline(&png(png::ColorType::Rgb)),
        ])
        .await;
        let service = Arc::new(ImageGenProcessor::new(
            OpenAiImageClient::new(config(url)).unwrap(),
        ));
        let sink = Arc::new(Pixels::default());
        let pipeline = Pipeline::new(vec![service, output(sink.clone(), 1024 * 1024)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let frame = Frame::text("first");
        let id = frame.id;
        pipeline.queue(frame).await.unwrap();
        pipeline.queue(Frame::text("second")).await.unwrap();
        if stop {
            pipeline.queue(Frame::new(FrameData::Stop)).await.unwrap();
        } else {
            pipeline.queue(Frame::end()).await.unwrap();
        }
        let (events, report) = pipeline.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let images = sink.0.lock().unwrap().clone();
        assert_eq!(images.len(), 2);
        for image in images.iter() {
            let FrameData::UrlImage(image) = &image.data else {
                panic!()
            };
            assert_eq!(image.image.image.image, vec![255, 0, 0, 3, 4, 5]);
            assert_eq!(image.image.image.size, (2, 1));
        }
        assert_eq!(
            images[0].metadata["image_generation.source_frame_id"],
            json!(id)
        );
        let mut order = vec![];
        for event in events {
            if event.direction == Direction::Downstream {
                match event.frame.data {
                    FrameData::Text(t) => order.push(t.text),
                    FrameData::UrlImage(_) => order.push("image".into()),
                    _ => (),
                }
            }
        }
        assert_eq!(order, vec!["first", "image", "second", "image"]);
        assert_eq!(
            serde_json::from_slice::<Value>(&requests.recv().await.unwrap().body).unwrap()
                ["prompt"],
            "first"
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn settings_are_atomic_snapshots_and_future_only() {
    let (url, mut requests, task) = server(vec![
        Reply {
            delay: Duration::from_millis(60),
            ..inline(&png(png::ColorType::Rgb))
        },
        inline(&png(png::ColorType::Rgb)),
    ])
    .await;
    let processor = Arc::new(ImageGenProcessor::new(
        OpenAiImageClient::new(config(url)).unwrap(),
    ));
    let pipeline = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline.queue(Frame::text("before")).await.unwrap();
    let first = requests.recv().await.unwrap();
    let invalid = ImageGenerationSettingsDelta {
        model: Setting::Value("new".into()),
        image_size: Setting::Value("x".repeat(129)),
    };
    assert!(processor.update_settings(&invalid).is_err());
    assert_eq!(
        processor.settings().unwrap().model.as_deref(),
        Some(SOURCE_DEFAULT_MODEL)
    );
    processor
        .update_settings(&ImageGenerationSettingsDelta {
            model: Setting::Value("explicit-model".into()),
            image_size: Setting::Value("future-size".into()),
        })
        .unwrap();
    pipeline.queue(Frame::text("after")).await.unwrap();
    pipeline.queue(Frame::end()).await.unwrap();
    assert!(pipeline.collect().await.unwrap().1.errors.is_empty());
    let second = requests.recv().await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&first.body).unwrap()["model"],
        SOURCE_DEFAULT_MODEL
    );
    let body: Value = serde_json::from_slice(&second.body).unwrap();
    assert_eq!(body["model"], "explicit-model");
    assert_eq!(body["size"], "future-size");
    assert!(body.get("response_format").is_none());
    task.await.unwrap();
}
#[tokio::test]
async fn interruption_cancel_and_health_fence_late_results() {
    for mode in 0..3 {
        let (url, mut requests, task) = server(vec![Reply {
            delay: Duration::from_millis(100),
            ..inline(&png(png::ColorType::Rgb))
        }])
        .await;
        let processor = Arc::new(ImageGenProcessor::new(
            OpenAiImageClient::new(config(url)).unwrap(),
        ));
        let health = processor.health().unwrap();
        let sink = Arc::new(Pixels::default());
        let pipeline = Pipeline::new(vec![processor, output(sink.clone(), 1024 * 1024)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        pipeline.queue(Frame::text("cancel me")).await.unwrap();
        requests.recv().await.unwrap();
        match mode {
            0 => pipeline.queue(Frame::interruption()).await.unwrap(),
            1 => {
                health.set_usable(false);
                health.set_usable(true);
            }
            _ => pipeline.queue(Frame::cancel("test")).await.unwrap(),
        }
        if mode != 2 {
            pipeline.queue(Frame::end()).await.unwrap();
        }
        let _ = pipeline.collect().await.unwrap();
        assert!(sink.0.lock().unwrap().is_empty());
        task.await.unwrap();
    }
}
#[test]
fn config_and_debug_never_leak_secret_urls_or_credentials() {
    let mut c = ImageGenerationConfig::new("test-secret");
    c.base_url = "https://user:private@example.com/v1".into();
    assert!(!format!("{c:?}").contains("test-secret"));
    assert!(!format!("{c:?}").contains("private"));
    assert!(OpenAiImageClient::new(c).is_err());
    let mut c = ImageGenerationConfig::new("secret");
    c.base_url = "http://example.com".into();
    c.allow_loopback_api = true;
    assert!(OpenAiImageClient::new(c).is_err());
}

#[async_trait]
trait Queue {
    async fn queue(&self, frame: Frame) -> std::result::Result<(), PipelineError>;
}
#[async_trait]
impl Queue for PipelineWorker {
    async fn queue(&self, frame: Frame) -> std::result::Result<(), PipelineError> {
        self.sender().send(frame, Direction::Downstream).await
    }
}

#[tokio::test]
async fn source_text_inheritance_includes_system_input_but_excludes_thought() {
    let (url, mut requests, task) = server(vec![inline(&png(png::ColorType::Rgb))]).await;
    let service = Arc::new(ImageGenProcessor::new(
        OpenAiImageClient::new(config(url)).unwrap(),
    ));
    let pipeline = Pipeline::new(vec![service])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline
        .queue(Frame::new(FrameData::InputText(TextFrame::new(
            "typed input",
        ))))
        .await
        .unwrap();
    let request = requests.recv().await.unwrap();
    assert_eq!(
        serde_json::from_slice::<Value>(&request.body).unwrap()["prompt"],
        "typed input"
    );
    pipeline
        .queue(Frame::new(FrameData::LlmThoughtText(TextFrame::new(
            "private thought",
        ))))
        .await
        .unwrap();
    pipeline.end().await.unwrap();
    let (frames, report) = pipeline.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::UrlImage(_)))
            .count(),
        1
    );
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::LlmThoughtText(_))));
    task.await.unwrap();
}
#[tokio::test]
async fn compressed_png_text_is_ignored_and_jpeg_scan_bombs_rejected() {
    let mut bytes = vec![];
    {
        let mut encoder = png::Encoder::new(&mut bytes, 1, 1);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        let mut writer = encoder.write_header().unwrap();
        writer
            .write_text_chunk(&png::text_metadata::ZTXtChunk::new(
                "comment",
                "x".repeat(2 * 1024 * 1024),
            ))
            .unwrap();
        writer.write_image_data(&[1, 2, 3]).unwrap();
    }
    assert!(bytes.len() < 16384);
    let (url, _, task) = server(vec![inline(&bytes)]).await;
    let mut c = config(url);
    c.limits.max_decode_bytes = 128 * 1024;
    let image = OpenAiImageClient::new(c)
        .unwrap()
        .generate("x", &CancellationToken::new())
        .await
        .unwrap();
    assert_eq!(image.image.image, vec![1, 2, 3]);
    task.await.unwrap();
    let jpeg = include_bytes!("fixtures/source-cat.jpg");
    let start = jpeg.windows(2).position(|b| b == [0xff, 0xda]).unwrap();
    let length = u16::from_be_bytes([jpeg[start + 2], jpeg[start + 3]]) as usize;
    let mut bomb = jpeg[..start].to_vec();
    for _ in 0..65 {
        bomb.extend_from_slice(&jpeg[start..start + 2 + length]);
    }
    bomb.extend_from_slice(&[0xff, 0xd9]);
    let (url, _, task) = server(vec![inline(&bomb)]).await;
    assert_eq!(
        OpenAiImageClient::new(config(url))
            .unwrap()
            .generate("x", &CancellationToken::new())
            .await,
        Err(ImageError::TooLarge)
    );
    task.await.unwrap();
}
#[tokio::test]
async fn output_capacity_counts_actual_decoded_bytes_not_url_alone() {
    let mut bytes = vec![];
    {
        let mut encoder = png::Encoder::new(&mut bytes, 64, 64);
        encoder.set_color(png::ColorType::Rgb);
        encoder.set_depth(png::BitDepth::Eight);
        encoder
            .write_header()
            .unwrap()
            .write_image_data(&vec![7; 64 * 64 * 3])
            .unwrap();
    }
    let (url, _, task) = server(vec![inline(&bytes)]).await;
    let service = Arc::new(ImageGenProcessor::new(
        OpenAiImageClient::new(config(url)).unwrap(),
    ));
    let sink = Arc::new(Pixels::default());
    let pipeline = Pipeline::new(vec![service, output(sink.clone(), 4096)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline.queue(Frame::text("x")).await.unwrap();
    pipeline.end().await.unwrap();
    let (_, report) = pipeline.collect().await.unwrap();
    assert!(!report.errors.is_empty());
    assert!(sink.0.lock().unwrap().is_empty());
    task.await.unwrap();
}
#[tokio::test]
async fn pipeline_queue_is_fail_fast_and_later_interruption_can_recover() {
    let (url, mut requests, task) = server(vec![
        Reply {
            delay: Duration::from_millis(100),
            ..inline(&png(png::ColorType::Rgb))
        },
        inline(&png(png::ColorType::Rgb)),
    ])
    .await;
    let mut c = config(url);
    c.limits.max_pending = 1;
    let service = Arc::new(ImageGenProcessor::new(OpenAiImageClient::new(c).unwrap()));
    let pipeline = Pipeline::new(vec![service])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline.queue(Frame::text("first")).await.unwrap();
    requests.recv().await.unwrap();
    pipeline.queue(Frame::text("overflow")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while pipeline.total_errors() == 0 {
            tokio::task::yield_now().await
        }
    })
    .await
    .unwrap();
    pipeline.queue(Frame::interruption()).await.unwrap();
    tokio::time::sleep(Duration::from_millis(120)).await;
    pipeline.queue(Frame::text("new epoch")).await.unwrap();
    pipeline.end().await.unwrap();
    let (frames, report) = pipeline.collect().await.unwrap();
    assert!(report
        .errors
        .iter()
        .any(|e| e.error.contains("queue is full")));
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::UrlImage(_)))
            .count(),
        1
    );
    task.await.unwrap();
}

#[path = "review/mod.rs"]
mod independent_review;
