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
fn config(url: String) -> GeminiImageConfig {
    let mut config = GeminiImageConfig::new("test-key");
    config.base_url = format!("{url}/v1beta");
    config.allow_loopback_api = true;
    config.limits.image.request_timeout = Duration::from_secs(3);
    config.limits.generation_timeout = Duration::from_secs(5);
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
fn part(bytes: &[u8], mime: &str) -> Value {
    json!({"inlineData":{"mimeType":mime,"data":base64::engine::general_purpose::STANDARD.encode(bytes)}})
}
fn inline(bytes: &[u8]) -> Reply {
    Reply::json(
        200,
        json!({"candidates":[{"content":{"role":"model","parts":[part(bytes,"image/png")]},"finishReason":"STOP"}]}),
    )
}
async fn first(client: &GeminiImageClient) -> pipecat_images::Result<GeneratedImage> {
    client
        .generate("a red square", &CancellationToken::new())?
        .next_image()
        .await?
        .ok_or(ImageError::NoImage)
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
async fn exact_rest_wire_scan_order_and_real_rgb_alpha() {
    for mode in [
        png::ColorType::Rgb,
        png::ColorType::Rgba,
        png::ColorType::Grayscale,
        png::ColorType::GrayscaleAlpha,
    ] {
        let reply = Reply::json(
            200,
            json!({"candidates":[
                {"content":null}, {"content":{"parts":[{"text":"ignored"},{"inlineData":{"mimeType":"image/png","data":""}}]}},
                {"content":{"parts":[part(&png(mode),"image/png"),part(b"bad second image", "image/png")]}}
            ]}),
        );
        let (url, mut requests, task) = server(vec![reply]).await;
        let mut c = config(url);
        c.settings.negative_prompt = Some("do not forward this".into());
        let client = GeminiImageClient::new(c).unwrap();
        let image = first(&client).await.unwrap();
        let request = requests.recv().await.unwrap();
        assert_eq!(request.method, "POST");
        assert_eq!(
            request.path,
            "/v1beta/models/gemini-3.1-flash-image:generateContent"
        );
        assert_eq!(request.headers["x-goog-api-key"], "test-key");
        assert!(!request.headers.contains_key("authorization"));
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap(),
            json!({
                "contents":[{"role":"user","parts":[{"text":"a red square"}]}],
                "generationConfig":{"responseModalities":["IMAGE"],"imageConfig":{"aspectRatio":"1:1"}}
            })
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
async fn jpeg_is_native_and_mime_must_match_codec() {
    let (url, _, task) = server(vec![Reply::json(200,json!({"candidates":[{"content":{"parts":[part(include_bytes!("fixtures/source-cat.jpg"),"image/jpeg")]}}]}))]).await;
    let image = first(&GeminiImageClient::new(config(url)).unwrap())
        .await
        .unwrap();
    assert!(image.image.size.0 > 100 && image.image.size.1 > 100);
    assert_eq!(
        image.image.image.len(),
        image.image.size.0 as usize * image.image.size.1 as usize * 3
    );
    assert!(image.image.image.windows(3).any(|p| p != [0, 0, 0]));
    task.await.unwrap();
}
#[tokio::test]
async fn count_is_sequential_lazy_and_null_aspect_is_omitted() {
    let (url, mut requests, task) = server(vec![inline(&png(png::ColorType::Rgb)); 3]).await;
    let mut c = config(url);
    c.settings.number_of_images = 3;
    c.settings.aspect_ratio = None;
    let client = GeminiImageClient::new(c).unwrap();
    let mut images = client.generate("three", &CancellationToken::new()).unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), requests.recv())
            .await
            .is_err()
    );
    for count in 1..=3 {
        assert!(images.next_image().await.unwrap().is_some());
        assert_eq!(images.delivered(), count);
        let request = requests.recv().await.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&request.body).unwrap(),
            json!({"contents":[{"role":"user","parts":[{"text":"three"}]}],"generationConfig":{"responseModalities":["IMAGE"],"imageConfig":{}}})
        );
        if count < 3 {
            assert!(
                tokio::time::timeout(Duration::from_millis(20), requests.recv())
                    .await
                    .is_err()
            );
        }
    }
    assert!(images.next_image().await.unwrap().is_none());
    task.await.unwrap();
}
#[tokio::test]
async fn first_absent_or_error_preserves_partial_delivery_and_fuses() {
    for reply in [
        Reply::json(200, json!({"candidates":[]})),
        Reply::json(503, json!({"error":"private response"})),
    ] {
        let expected = if reply.status == 200 {
            ImageError::NoImage
        } else {
            ImageError::Http(503)
        };
        let (url, mut requests, task) =
            server(vec![inline(&png(png::ColorType::Rgb)), reply]).await;
        let mut c = config(url);
        c.settings.number_of_images = 3;
        let client = GeminiImageClient::new(c).unwrap();
        let mut images = client.generate("x", &CancellationToken::new()).unwrap();
        let first = images.next_image().await.unwrap().unwrap();
        assert_eq!(images.next_image().await, Err(expected));
        assert_eq!(images.next_image().await, Ok(None));
        assert_eq!(images.delivered(), 1);
        assert_eq!(first.image.image, vec![255, 0, 0, 3, 4, 5]);
        task.await.unwrap();
        assert!(requests.recv().await.is_some());
        assert!(requests.recv().await.is_some());
        assert!(requests.recv().await.is_none());
    }
}
#[tokio::test]
async fn malformed_safety_mime_base64_and_status_fail_explicitly() {
    for (reply, expected) in [
        (
            Reply::json(200, json!({"candidates":null})),
            ImageError::NoImage,
        ),
        (
            Reply::json(200, json!({"promptFeedback":{"blockReason":"SAFETY"}})),
            ImageError::NoImage,
        ),
        (
            Reply::json(
                200,
                json!({"candidates":[{"finishReason":"IMAGE_SAFETY","content":{"parts":[part(&png(png::ColorType::Rgb),"image/png")]}}]}),
            ),
            ImageError::NoImage,
        ),
        (
            Reply::json(
                200,
                json!({"candidates":[{"content":{"parts":[{"inlineData":{"data":"AAAA"}}]}}]}),
            ),
            ImageError::Protocol,
        ),
        (
            Reply::json(
                200,
                json!({"candidates":[{"content":{"parts":[{"inlineData":{"mimeType":"image/png","data":"!!!!"}}]}}]}),
            ),
            ImageError::Protocol,
        ),
        (
            Reply::json(
                200,
                json!({"candidates":[{"content":{"parts":[{"inlineData":{"mimeType":"image/png","data":"a"}}]}}]}),
            ),
            ImageError::Protocol,
        ),
        (
            Reply::json(
                200,
                json!({"candidates":[{"content":{"parts":[part(&png(png::ColorType::Rgb),"image/jpeg")]}}]}),
            ),
            ImageError::Decode,
        ),
        (
            Reply::json(
                200,
                json!({"candidates":[{"content":{"parts":[part(b"GIF89a","image/gif")]}}]}),
            ),
            ImageError::Decode,
        ),
        (
            Reply {
                body: b"{bad".to_vec(),
                ..Reply::json(200, json!(null))
            },
            ImageError::Protocol,
        ),
        (
            Reply {
                body: b"{\"candidates\":[],\"candidates\":[]}".to_vec(),
                ..Reply::json(200, json!(null))
            },
            ImageError::Protocol,
        ),
        (
            Reply::json(401, json!({"error":"private details"})),
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
        assert_eq!(
            first(&GeminiImageClient::new(config(url)).unwrap()).await,
            Err(expected)
        );
        task.await.unwrap();
    }
}
#[tokio::test]
async fn response_candidate_part_encoded_pixel_and_aggregate_bounds() {
    for kind in 0..11 {
        let mut reply = inline(&png(png::ColorType::Rgb));
        if kind == 1 {
            reply.chunked = true;
        }
        if kind == 2 {
            reply = Reply::json(200, json!({"candidates":[{},{}]}));
        }
        if kind == 3 {
            reply = Reply::json(
                200,
                json!({"candidates":[{"content":{"parts":[{}]}},{"content":{"parts":[{}]}}]}),
            );
        }
        if kind == 6 {
            let mut bytes = png(png::ColorType::Rgb);
            bytes[16..20].copy_from_slice(&u32::MAX.to_be_bytes());
            reply = inline(&bytes);
        }
        let length = reply.body.len();
        let (url, _, task) = server(vec![reply.clone(), reply]).await;
        let mut c = config(url);
        c.settings.number_of_images = 2;
        match kind {
            0 | 1 => c.limits.image.max_response_bytes = 16,
            2 => c.limits.max_candidates = 1,
            3 => c.limits.max_parts = 1,
            4 => c.limits.image.max_encoded_image_bytes = 8,
            5 => c.limits.image.max_pixels = 1,
            6 => (),
            7 => c.limits.max_total_response_bytes = length + 16,
            8 => c.limits.max_total_encoded_bytes = png(png::ColorType::Rgb).len() + 8,
            9 => c.limits.max_total_output_bytes = 9,
            10 => c.limits.image.max_decode_bytes = 1,
            _ => unreachable!(),
        }
        let client = GeminiImageClient::new(c).unwrap();
        let mut images = client.generate("x", &CancellationToken::new()).unwrap();
        if (7..=9).contains(&kind) {
            assert!(images.next_image().await.unwrap().is_some());
        }
        assert_eq!(
            images.next_image().await,
            Err(ImageError::TooLarge),
            "kind {kind}"
        );
        assert_eq!(images.next_image().await, Ok(None));
        task.abort();
        let _ = task.await;
    }
}
#[tokio::test]
async fn cancellation_between_requests_and_whole_prompt_deadline() {
    for expired in [false, true] {
        let (url, mut requests, task) = server(vec![inline(&png(png::ColorType::Rgb))]).await;
        let mut c = config(url);
        c.settings.number_of_images = 2;
        c.limits.generation_timeout = Duration::from_millis(100);
        let client = GeminiImageClient::new(c).unwrap();
        let token = CancellationToken::new();
        let mut images = client.generate("x", &token).unwrap();
        assert!(images.next_image().await.unwrap().is_some());
        if expired {
            tokio::time::sleep(Duration::from_millis(110)).await;
        } else {
            token.cancel();
        }
        assert_eq!(
            images.next_image().await,
            Err(if expired {
                ImageError::Timeout
            } else {
                ImageError::Cancelled
            })
        );
        assert_eq!(images.delivered(), 1);
        task.await.unwrap();
        assert!(requests.recv().await.is_some());
        assert!(requests.recv().await.is_none());
    }
}
#[tokio::test]
async fn request_deadline_cancellation_backpressure_and_drop_are_owned() {
    for mode in 0..3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut c = config(format!("http://{}", listener.local_addr().unwrap()));
        c.limits.image.max_pending = 1;
        if mode == 0 {
            c.limits.image.request_timeout = Duration::from_millis(80);
        }
        let client = GeminiImageClient::new(c).unwrap();
        let token = CancellationToken::new();
        let mut images = client.generate("x", &token).unwrap();
        assert!(matches!(
            client.generate("blocked", &token),
            Err(ImageError::Backpressure)
        ));
        let mut pending = Box::pin(images.next_image());
        let (mut socket, _) = tokio::select! { result=listener.accept()=>result.unwrap(), _=&mut pending=>panic!("response must wait") };
        let _request = tokio::select! { request=read_request(&mut socket)=>request, _=&mut pending=>panic!("response must wait") };
        if mode == 0 {
            assert_eq!(pending.as_mut().await, Err(ImageError::Timeout));
        }
        if mode == 1 {
            token.cancel();
            assert_eq!(pending.as_mut().await, Err(ImageError::Cancelled));
        }
        drop(pending);
        if mode == 2 {
            assert_eq!(images.next_image().await, Err(ImageError::Cancelled));
        }
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), socket.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(images.next_image().await, Ok(None));
        assert!(client.generate("fresh", &CancellationToken::new()).is_ok());
        assert!(
            tokio::time::timeout(Duration::from_millis(30), listener.accept())
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn redirects_and_ambiguous_disconnect_never_retry() {
    for redirect in [false, true] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let client = GeminiImageClient::new(config(origin.clone())).unwrap();
        let pending = tokio::spawn(async move { first(&client).await });
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        if redirect {
            write_reply(
                &mut socket,
                Reply {
                    status: 307,
                    extra: format!("Location: {origin}/stolen\r\n"),
                    ..Reply::json(200, json!(null))
                },
                false,
            )
            .await;
        }
        drop(socket);
        assert_eq!(
            pending.await.unwrap(),
            Err(if redirect {
                ImageError::Http(307)
            } else {
                ImageError::Transport
            })
        );
        assert!(
            tokio::time::timeout(Duration::from_millis(40), listener.accept())
                .await
                .is_err()
        );
    }
}
#[test]
fn bounded_configuration_atomic_settings_and_redacted_debug() {
    for model in ["", "..", "models/unsafe", "unsafe?key=x", "bad\nmodel"] {
        let mut c = GeminiImageConfig::new("secret");
        c.settings.model = model.into();
        assert!(GeminiImageClient::new(c).is_err());
    }
    for n in [0, 9, 255] {
        let mut c = GeminiImageConfig::new("secret");
        c.settings.number_of_images = n;
        assert!(GeminiImageClient::new(c).is_err());
    }
    for url in [
        "http://example.com",
        "http://localhost:1",
        "https://user:password@example.com",
        "https://example.com?key=secret",
        "https://example.com#fragment",
    ] {
        let mut c = GeminiImageConfig::new("test-secret");
        c.base_url = url.into();
        c.allow_loopback_api = true;
        assert!(!format!("{c:?}").contains("test-secret"));
        assert!(!format!("{c:?}").contains(url));
        assert!(GeminiImageClient::new(c).is_err());
    }
    let settings = GeminiImageSettings::default();
    assert!(settings
        .apply(&GeminiImageSettingsDelta {
            model: Setting::Value("new-model".into()),
            number_of_images: Setting::Value(0),
            ..Default::default()
        })
        .is_err());
    assert_eq!(settings.model, GEMINI_SOURCE_DEFAULT_MODEL);
    assert!(settings
        .apply(&GeminiImageSettingsDelta {
            model: Setting::Null,
            ..Default::default()
        })
        .is_err());
    assert!(settings
        .apply(&GeminiImageSettingsDelta {
            aspect_ratio: Setting::Value("100:1".into()),
            ..Default::default()
        })
        .is_err());
}

#[tokio::test]
async fn real_pipeline_end_stop_deliver_every_rgb_in_order() {
    for stop in [false, true] {
        let (url, mut requests, task) = server(vec![inline(&png(png::ColorType::Rgba)); 4]).await;
        let mut c = config(url);
        c.settings.number_of_images = 2;
        let service = Arc::new(GeminiImageGenProcessor::new(
            GeminiImageClient::new(c).unwrap(),
        ));
        let sink = Arc::new(Pixels::default());
        let pipeline = Pipeline::new(vec![service, output(sink.clone(), 1024 * 1024)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let mut frame = Frame::text("first");
        frame.transport_source = Some("source".into());
        let id = frame.id;
        pipeline.queue(frame).await.unwrap();
        pipeline.queue(Frame::text("second")).await.unwrap();
        pipeline
            .queue(if stop {
                Frame::new(FrameData::Stop)
            } else {
                Frame::end()
            })
            .await
            .unwrap();
        let (events, report) = pipeline.collect().await.unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        let images = sink.0.lock().unwrap().clone();
        assert_eq!(images.len(), 4);
        for frame in images.iter() {
            let FrameData::UrlImage(image) = &frame.data else {
                panic!()
            };
            assert_eq!(image.image.image.image, vec![255, 0, 0, 3, 4, 5]);
            assert_eq!(image.image.image.size, (2, 1));
            assert_eq!(image.url, None);
        }
        assert_eq!(
            images[0].metadata["image_generation.source_frame_id"],
            json!(id)
        );
        assert_eq!(
            images[1].metadata["image_generation.source_frame_id"],
            json!(id)
        );
        assert_eq!(images[0].transport_source.as_deref(), Some("source"));
        drop(images);
        let order: Vec<_> = events
            .into_iter()
            .filter(|e| e.direction == Direction::Downstream)
            .filter_map(|e| match e.frame.data {
                FrameData::Text(t) => Some(t.text),
                FrameData::UrlImage(_) => Some("image".into()),
                _ => None,
            })
            .collect();
        assert_eq!(
            order,
            vec!["first", "image", "image", "second", "image", "image"]
        );
        task.await.unwrap();
        for text in ["first", "first", "second", "second"] {
            assert_eq!(
                serde_json::from_slice::<Value>(&requests.recv().await.unwrap().body).unwrap()
                    ["contents"][0]["parts"][0]["text"],
                text
            );
        }
        assert!(requests.recv().await.is_none());
    }
}
#[tokio::test]
async fn processor_partial_delivery_survives_no_image_and_errors() {
    for status in [200, 401, 503] {
        let (url, mut requests, task) = server(vec![
            inline(&png(png::ColorType::Rgb)),
            Reply::json(status, json!({"candidates":[]})),
        ])
        .await;
        let mut c = config(url);
        c.settings.number_of_images = 3;
        let processor = Arc::new(GeminiImageGenProcessor::new(
            GeminiImageClient::new(c).unwrap(),
        ));
        let health = processor.health().unwrap();
        let sink = Arc::new(Pixels::default());
        let pipeline = Pipeline::new(vec![processor, output(sink.clone(), 1024 * 1024)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        pipeline.queue(Frame::text("partial")).await.unwrap();
        pipeline.end().await.unwrap();
        let (_, report) = pipeline.collect().await.unwrap();
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        assert_eq!(report.errors.len(), 1);
        assert_eq!(health.is_usable(), status != 401);
        task.await.unwrap();
        assert!(requests.recv().await.is_some());
        assert!(requests.recv().await.is_some());
        assert!(requests.recv().await.is_none());
    }
}
#[tokio::test]
async fn processor_settings_are_atomic_future_only_across_a_sequence() {
    let (url, mut requests, task) = server(vec![
        Reply {
            delay: Duration::from_millis(60),
            ..inline(&png(png::ColorType::Rgb))
        },
        inline(&png(png::ColorType::Rgb)),
        inline(&png(png::ColorType::Rgb)),
    ])
    .await;
    let mut c = config(url);
    c.settings.number_of_images = 2;
    let processor = Arc::new(GeminiImageGenProcessor::new(
        GeminiImageClient::new(c).unwrap(),
    ));
    let pipeline = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline.queue(Frame::text("before")).await.unwrap();
    let first = requests.recv().await.unwrap();
    assert!(processor
        .update_settings(&GeminiImageSettingsDelta {
            model: Setting::Value("new-model".into()),
            number_of_images: Setting::Value(0),
            ..Default::default()
        })
        .is_err());
    assert_eq!(
        processor.settings().unwrap().model,
        GEMINI_SOURCE_DEFAULT_MODEL
    );
    processor
        .update_settings(&GeminiImageSettingsDelta {
            model: Setting::Value("explicit-model".into()),
            number_of_images: Setting::Value(1),
            aspect_ratio: Setting::Value("16:9".into()),
            ..Default::default()
        })
        .unwrap();
    pipeline.queue(Frame::text("after")).await.unwrap();
    pipeline.end().await.unwrap();
    assert!(pipeline.collect().await.unwrap().1.errors.is_empty());
    let second = requests.recv().await.unwrap();
    let third = requests.recv().await.unwrap();
    assert_eq!(first.path, second.path);
    assert!(third.path.contains("/explicit-model:generateContent"));
    assert_eq!(
        serde_json::from_slice::<Value>(&second.body).unwrap()["generationConfig"]["imageConfig"]
            ["aspectRatio"],
        "1:1"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&third.body).unwrap()["generationConfig"]["imageConfig"]
            ["aspectRatio"],
        "16:9"
    );
    task.await.unwrap();
}
#[tokio::test]
async fn targeted_settings_frames_validate_borrowed_values_and_recover_health() {
    let (url, mut requests, task) = server(vec![inline(&png(png::ColorType::Rgb))]).await;
    let processor = Arc::new(GeminiImageGenProcessor::new(
        GeminiImageClient::new(config(url)).unwrap(),
    ));
    let pipeline = Pipeline::new(vec![processor.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let update = ServiceUpdateSettingsFrame {
        settings: BTreeMap::from([
            ("model".into(), json!("alternate")),
            ("aspect_ratio".into(), Value::Null),
            ("number_of_images".into(), json!(1)),
            ("negative_prompt".into(), json!("ignored")),
        ]),
        service: Some(ProcessorReference::Name("GoogleImageGenService".into())),
        ..Default::default()
    };
    pipeline
        .queue(Frame::new(FrameData::ServiceUpdateSettings(Box::new(
            update,
        ))))
        .await
        .unwrap();
    pipeline.queue(Frame::text("x")).await.unwrap();
    pipeline.end().await.unwrap();
    assert!(pipeline.collect().await.unwrap().1.errors.is_empty());
    let request = requests.recv().await.unwrap();
    assert!(request.path.contains("/alternate:generateContent"));
    assert_eq!(
        serde_json::from_slice::<Value>(&request.body).unwrap()["generationConfig"]["imageConfig"],
        json!({})
    );
    task.await.unwrap();
    let processor = GeminiImageGenProcessor::new(
        GeminiImageClient::new(GeminiImageConfig::new("test-key")).unwrap(),
    );
    let health = processor.health().unwrap();
    health.set_usable(false);
    assert!(processor
        .update_settings(&GeminiImageSettingsDelta {
            aspect_ratio: Setting::Value("bad".into()),
            ..Default::default()
        })
        .is_err());
    assert!(!health.is_usable());
    processor
        .update_settings(&GeminiImageSettingsDelta {
            model: Setting::Value("different".into()),
            ..Default::default()
        })
        .unwrap();
    assert!(health.is_usable());
}
#[tokio::test]
async fn interruption_cancel_health_and_drop_fence_second_response() {
    for mode in 0..4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut c = config(format!("http://{}", listener.local_addr().unwrap()));
        c.settings.number_of_images = 3;
        let processor = Arc::new(GeminiImageGenProcessor::new(
            GeminiImageClient::new(c).unwrap(),
        ));
        let health = processor.health().unwrap();
        let sink = Arc::new(Pixels::default());
        let pipeline = Pipeline::new(vec![processor, output(sink.clone(), 1024 * 1024)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        pipeline
            .queue(Frame::text("cancel after first"))
            .await
            .unwrap();
        let (mut first, _) = listener.accept().await.unwrap();
        read_request(&mut first).await;
        write_reply(&mut first, inline(&png(png::ColorType::Rgb)), false).await;
        drop(first);
        let (mut second, _) = listener.accept().await.unwrap();
        read_request(&mut second).await;
        tokio::time::timeout(Duration::from_secs(1), async {
            while sink.0.lock().unwrap().len() != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match mode {
            0 => pipeline.queue(Frame::interruption()).await.unwrap(),
            1 => {
                health.set_usable(false);
                health.set_usable(true);
            }
            2 => pipeline.queue(Frame::cancel("test")).await.unwrap(),
            _ => (),
        }
        if mode == 3 {
            drop(pipeline);
        } else {
            if mode < 2 {
                pipeline.end().await.unwrap();
            }
            let _ = pipeline.collect().await.unwrap();
        }
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(1), second.read(&mut byte))
                .await
                .unwrap()
                .unwrap(),
            0
        );
        assert_eq!(sink.0.lock().unwrap().len(), 1);
        assert!(
            tokio::time::timeout(Duration::from_millis(20), listener.accept())
                .await
                .is_err()
        );
    }
}
#[tokio::test]
async fn processor_backpressure_and_source_text_inheritance() {
    let (url, mut requests, task) = server(vec![Reply {
        delay: Duration::from_millis(80),
        ..inline(&png(png::ColorType::Rgb))
    }])
    .await;
    let mut c = config(url);
    c.limits.image.max_pending = 1;
    let service = Arc::new(GeminiImageGenProcessor::new(
        GeminiImageClient::new(c).unwrap(),
    ));
    let pipeline = Pipeline::new(vec![service])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    pipeline
        .queue(Frame::new(FrameData::InputText(TextFrame::new("typed"))))
        .await
        .unwrap();
    requests.recv().await.unwrap();
    pipeline.queue(Frame::text("overflow")).await.unwrap();
    tokio::time::timeout(Duration::from_secs(1), async {
        while pipeline.total_errors() == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    pipeline
        .queue(Frame::new(FrameData::LlmThoughtText(TextFrame::new(
            "private thought",
        ))))
        .await
        .unwrap();
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
    assert!(frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::LlmThoughtText(_))));
    task.await.unwrap();
}

#[tokio::test]
async fn mixed_candidate_safety_veto_is_explicitly_response_wide() {
    let valid = json!({"content":{"parts":[part(&png(png::ColorType::Rgb),"image/png")]},"finishReason":"STOP"});
    let blocked = json!({"content":{"parts":[{"text":"blocked"}]},"finishReason":"IMAGE_SAFETY"});
    for candidates in [vec![valid.clone(), blocked.clone()], vec![blocked, valid]] {
        let (url, _, task) = server(vec![Reply::json(200, json!({"candidates":candidates}))]).await;
        let client = GeminiImageClient::new(config(url)).unwrap();
        assert_eq!(first(&client).await, Err(ImageError::NoImage));
        task.await.unwrap();
    }
}

#[path = "gemini_review/mod.rs"]
mod independent_review;
