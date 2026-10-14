//! Synthetic, real-socket Azure REST fixtures. No real credentials or external service calls.
use base64::Engine;
use pipecat_images::{
    AzureImageClient, AzureImageConfig, CancellationToken, GeneratedImage, HostedImagePolicy,
    ImageError, ImageGenerationSettings,
};
use serde_json::{json, Value};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};

#[derive(Debug, Clone)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
    received: Instant,
}
#[derive(Clone)]
struct Reply {
    status: u16,
    body: Vec<u8>,
    headers: String,
    delay: Duration,
    chunked: bool,
}
impl Reply {
    fn json(value: Value) -> Self {
        Self {
            status: 200,
            body: value.to_string().into_bytes(),
            headers: String::new(),
            delay: Duration::ZERO,
            chunked: false,
        }
    }
    fn submitted(origin: &str) -> Self {
        Self {
            status: 202,
            headers: format!(
                "operation-location: {origin}/operations/id?token=operation-secret\r\n"
            ),
            ..Self::json(json!({"ignored": true}))
        }
    }
    fn success(url: &str) -> Self {
        Self::json(json!({"status":"succeeded","result":{"data":[{"url":url}]}}))
    }
    fn image(bytes: Vec<u8>) -> Self {
        Self {
            body: bytes,
            ..Self::json(Value::Null)
        }
    }
}
struct Server {
    origin: String,
    requests: Arc<Mutex<Vec<Request>>>,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
impl Server {
    async fn start(replies: impl FnOnce(&str) -> Vec<Reply>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        let mut replies: VecDeque<_> = replies(&origin).into();
        let requests = Arc::new(Mutex::new(Vec::new()));
        let log = requests.clone();
        let stop = CancellationToken::new();
        let stopping = stop.clone();
        let task = tokio::spawn(async move {
            loop {
                let accepted = tokio::select! { biased;
                    _ = stopping.cancelled() => break,
                    result = listener.accept() => result,
                };
                let Ok((mut socket, _)) = accepted else {
                    break;
                };
                let request = tokio::select! { biased;
                    _ = stopping.cancelled() => break,
                    result = read_request(&mut socket) => result,
                };
                let Some(request) = request else {
                    continue;
                };
                log.lock().unwrap().push(request);
                let reply = replies.pop_front().unwrap_or_else(|| Reply {
                    status: 500,
                    ..Reply::json(json!({"unexpected_request":true}))
                });
                tokio::select! { biased;
                    _ = stopping.cancelled() => break,
                    _ = write_reply(&mut socket, reply) => (),
                }
            }
        });
        Self {
            origin,
            requests,
            stop,
            task,
        }
    }
    async fn finish_observed(self) -> (Vec<Request>, bool) {
        self.stop.cancel();
        let joined = self.task.await.is_ok();
        let requests = std::mem::take(
            &mut *self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner),
        );
        (requests, joined)
    }
    async fn finish(self) -> Vec<Request> {
        let (requests, joined) = self.finish_observed().await;
        assert!(joined, "fixture owner panicked");
        requests
    }
    async fn wait_for(&self, count: usize) -> bool {
        tokio::time::timeout(Duration::from_secs(3), async {
            while self
                .requests
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .len()
                < count
            {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .is_ok()
    }
}
async fn read_request(socket: &mut TcpStream) -> Option<Request> {
    let mut bytes = Vec::new();
    let split = loop {
        let mut block = [0u8; 4096];
        let count = socket.read(&mut block).await.ok()?;
        if count == 0 {
            return None;
        }
        bytes.extend_from_slice(&block[..count]);
        assert!(bytes.len() <= 2 * 1024 * 1024);
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
            (name.to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect();
    let length: usize = headers
        .get("content-length")
        .map(|s| s.parse().unwrap())
        .unwrap_or(0);
    let mut body = bytes[split + 4..].to_vec();
    while body.len() < length {
        let mut block = [0u8; 4096];
        let count = socket.read(&mut block).await.ok()?;
        if count == 0 {
            return None;
        }
        body.extend_from_slice(&block[..count]);
    }
    assert_eq!(body.len(), length);
    Some(Request {
        method,
        path,
        headers,
        body,
        received: Instant::now(),
    })
}
async fn write_reply(socket: &mut TcpStream, reply: Reply) {
    tokio::time::sleep(reply.delay).await;
    let framing = if reply.chunked {
        "Transfer-Encoding: chunked\r\n".to_owned()
    } else {
        format!("Content-Length: {}\r\n", reply.body.len())
    };
    let head = format!(
        "HTTP/1.1 {} Test\r\nContent-Type: application/json\r\nConnection: close\r\n{}{}\r\n",
        reply.status, framing, reply.headers,
    );
    if socket.write_all(head.as_bytes()).await.is_err() {
        return;
    }
    if reply.chunked {
        for chunk in reply.body.chunks(13) {
            if socket
                .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                .await
                .is_err()
                || socket.write_all(chunk).await.is_err()
                || socket.write_all(b"\r\n").await.is_err()
            {
                return;
            }
        }
        let _ = socket.write_all(b"0\r\n\r\n").await;
    } else {
        let _ = socket.write_all(&reply.body).await;
    }
}
fn config(origin: &str) -> AzureImageConfig {
    let mut config = AzureImageConfig::new("synthetic-azure-secret", origin);
    config.allow_loopback_api = true;
    config.poll_interval = Duration::from_millis(1);
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
            _ => unreachable!(),
        };
        encoder
            .write_header()
            .unwrap()
            .write_image_data(data)
            .unwrap();
    }
    bytes
}
async fn image_servers(
    poll: impl FnOnce(&str) -> Reply,
    bytes: Vec<u8>,
) -> (Server, Server, AzureImageConfig) {
    let download = Server::start(|_| vec![Reply::image(bytes)]).await;
    let url = format!("{}/image.png?signature=hosted-secret", download.origin);
    let api = Server::start(|origin| vec![Reply::submitted(origin), poll(&url)]).await;
    let mut settings = config(&api.origin);
    settings.hosted_images = HostedImagePolicy::loopback_fixtures(&[&download.origin]).unwrap();
    (api, download, settings)
}
async fn joined(
    task: &mut JoinHandle<Result<GeneratedImage, ImageError>>,
) -> Result<Result<GeneratedImage, ImageError>, &'static str> {
    let result = tokio::time::timeout(Duration::from_secs(5), &mut *task).await;
    match result {
        Ok(result) => result.map_err(|_| "request owner panicked"),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("request owner exceeded its bounded deadline and was aborted/joined")
        }
    }
}

#[test]
fn defaults_validation_and_debug_redaction() {
    let defaults =
        AzureImageConfig::new("synthetic-key", "https://fixture.example/private-endpoint");
    assert_eq!(defaults.api_version, "2023-06-01-preview");
    assert_eq!(defaults.settings.model, None);
    assert_eq!(defaults.settings.image_size, None);
    assert_eq!(defaults.poll_interval, Duration::from_secs(1));
    assert_eq!(defaults.max_polls, 119);
    assert!(!defaults.allow_loopback_api);
    let client = AzureImageClient::new(defaults.clone()).unwrap();
    let debug = format!("{defaults:?} {client:?}");
    for secret in ["synthetic-key", "fixture.example", "private-endpoint"] {
        assert!(!debug.contains(secret));
    }
    for change in [
        |c: &mut AzureImageConfig| c.max_polls = 0,
        |c: &mut AzureImageConfig| c.max_polls = 120,
        |c: &mut AzureImageConfig| c.poll_interval = Duration::ZERO,
        |c: &mut AzureImageConfig| c.poll_interval = Duration::from_secs(61),
        |c: &mut AzureImageConfig| c.api_version.clear(),
        |c: &mut AzureImageConfig| c.api_version = "x".repeat(129),
        |c: &mut AzureImageConfig| c.settings.model = Some("x".repeat(1025)),
        |c: &mut AzureImageConfig| c.settings.image_size = Some("x".repeat(129)),
        |c: &mut AzureImageConfig| c.limits.max_pending = 0,
        |c: &mut AzureImageConfig| c.limits.request_timeout = Duration::ZERO,
    ] {
        let mut invalid = defaults.clone();
        change(&mut invalid);
        assert!(matches!(
            AzureImageClient::new(invalid),
            Err(ImageError::Invalid(_))
        ));
    }
    for key in [
        "".into(),
        "x".repeat(4097),
        "secret\r\ninjected: value".into(),
    ] {
        assert!(matches!(
            AzureImageClient::new(AzureImageConfig::new(key, "https://fixture.example")),
            Err(ImageError::Invalid(_))
        ));
    }
    for endpoint in [
        "http://127.0.0.1:1234",
        "http://fixture.example",
        "https://user:pass@fixture.example",
        "https://@fixture.example",
        "https://fixture.example?query=secret",
        "https://fixture.example#secret",
    ] {
        assert_eq!(
            AzureImageClient::new(AzureImageConfig::new("key", endpoint)).unwrap_err(),
            ImageError::AddressDenied
        );
    }
}

#[tokio::test]
async fn exact_submit_poll_download_order_headers_body_and_overrides() {
    let (api, download, mut cfg) = image_servers(Reply::success, png(png::ColorType::Rgba)).await;
    cfg.base_url = format!("{}/resource///", api.origin);
    cfg.api_version = "preview &other=value".into();
    cfg.poll_interval = Duration::from_millis(15);
    cfg.settings.model = Some("local-only-model".into());
    let client = AzureImageClient::new(cfg).unwrap();
    let settings = ImageGenerationSettings {
        model: Some("override-metadata".into()),
        image_size: Some("256x256".into()),
    };
    let result = client
        .generate_with_settings("synthetic prompt", &settings, &CancellationToken::new())
        .await;
    let calls = api.finish().await;
    let downloads = download.finish().await;
    let image = result.unwrap();
    assert_eq!(calls.len(), 2);
    assert_eq!(downloads.len(), 1);
    assert_eq!(calls[0].method, "POST");
    assert_eq!(
        calls[0].path,
        "/resource/openai/images/generations:submit?api-version=preview+%26other%3Dvalue"
    );
    assert_eq!(
        serde_json::from_slice::<Value>(&calls[0].body).unwrap(),
        json!({"prompt":"synthetic prompt","n":1,"size":"256x256"})
    );
    assert_eq!(calls[1].method, "GET");
    assert_eq!(calls[1].path, "/operations/id?token=operation-secret");
    assert!(calls[1].body.is_empty());
    for request in &calls {
        assert_eq!(request.headers["api-key"], "synthetic-azure-secret");
        assert_eq!(request.headers["content-type"], "application/json");
        assert!(!request.headers.contains_key("authorization"));
    }
    assert!(calls[1].received.duration_since(calls[0].received) >= Duration::from_millis(15));
    assert!(downloads[0].received >= calls[1].received);
    assert_eq!(downloads[0].method, "GET");
    for name in ["api-key", "authorization", "cookie"] {
        assert!(!downloads[0].headers.contains_key(name));
    }
    assert_eq!(image.image.image, [255, 0, 0, 3, 4, 5]);
    assert_eq!(image.image.format.as_deref(), Some("RGB"));
    assert!(image.url.unwrap().ends_with("?signature=hosted-secret"));
    assert_eq!(client.config().settings.image_size, None);
}

#[tokio::test]
async fn none_size_is_omitted_and_png_modes_normalize_to_rgb() {
    for mode in [
        png::ColorType::Rgb,
        png::ColorType::Rgba,
        png::ColorType::Grayscale,
        png::ColorType::GrayscaleAlpha,
    ] {
        let (api, download, cfg) = image_servers(Reply::success, png(mode)).await;
        let result = AzureImageClient::new(cfg)
            .unwrap()
            .generate("colors", &CancellationToken::new())
            .await;
        let calls = api.finish().await;
        let downloads = download.finish().await;
        let image = result.unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&calls[0].body).unwrap(),
            json!({"prompt":"colors","n":1})
        );
        assert_eq!(downloads.len(), 1);
        assert_eq!(image.image.size, (2, 1));
        assert_eq!(image.image.format.as_deref(), Some("RGB"));
        assert_eq!(
            image.image.image,
            if matches!(mode, png::ColorType::Rgb | png::ColorType::Rgba) {
                vec![255, 0, 0, 3, 4, 5]
            } else {
                vec![42, 42, 42, 97, 97, 97]
            }
        );
    }
}

#[tokio::test]
async fn synthetic_grayscale_jpeg_is_decoded_to_rgb() {
    // Pillow-generated constant 2x1 gray fixture. It contains no real image data.
    let encoded = "/9j/4AAQSkZJRgABAQAAAQABAAD/2wBDAAgGBgcGBQgHBwcJCQgKDBQNDAsLDBkSEw8UHRofHh0aHBwgJC4nICIsIxwcKDcpLDAxNDQ0Hyc5PTgyPC4zNDL/wAALCAABAAIBAREA/8QAHwAAAQUBAQEBAQEAAAAAAAAAAAECAwQFBgcICQoL/8QAtRAAAgEDAwIEAwUFBAQAAAF9AQIDAAQRBRIhMUEGE1FhByJxFDKBkaEII0KxwRVS0fAkM2JyggkKFhcYGRolJicoKSo0NTY3ODk6Q0RFRkdISUpTVFVWV1hZWmNkZWZnaGlqc3R1dnd4eXqDhIWGh4iJipKTlJWWl5iZmqKjpKWmp6ipqrKztLW2t7i5usLDxMXGx8jJytLT1NXW19jZ2uHi4+Tl5ufo6erx8vP09fb3+Pn6/9oACAEBAAA/APNq/9k=";
    let (api, download, cfg) = image_servers(
        Reply::success,
        base64::engine::general_purpose::STANDARD
            .decode(encoded)
            .unwrap(),
    )
    .await;
    let result = AzureImageClient::new(cfg)
        .unwrap()
        .generate("gray", &CancellationToken::new())
        .await;
    api.finish().await;
    download.finish().await;
    let image = result.unwrap();
    assert_eq!(image.image.size, (2, 1));
    assert_eq!(image.image.format.as_deref(), Some("RGB"));
    assert_eq!(image.image.image, [55; 6]);
}

#[tokio::test]
async fn failed_canceled_unknown_and_case_mismatch_continue_until_exact_success() {
    let download = Server::start(|_| vec![Reply::image(png(png::ColorType::Rgb))]).await;
    let image_url = format!("{}/image", download.origin);
    let api = Server::start(|origin| {
        let mut replies = vec![Reply::submitted(origin)];
        for status in ["failed", "canceled", "running", "Succeeded", ""] {
            replies.push(Reply::json(json!({"status":status,"result":123})));
        }
        replies.push(Reply::success(&image_url));
        replies
    })
    .await;
    let mut cfg = config(&api.origin);
    cfg.hosted_images = HostedImagePolicy::loopback_fixtures(&[&download.origin]).unwrap();
    let result = AzureImageClient::new(cfg)
        .unwrap()
        .generate("test", &CancellationToken::new())
        .await;
    let requests = api.finish().await;
    download.finish().await;
    assert!(result.is_ok());
    assert_eq!(requests.len(), 7);
    assert!(requests[1..].iter().all(|r| r.method == "GET"));
}

#[tokio::test]
async fn source_budget_is_119_polls_and_small_budget_is_exact_without_resubmit() {
    for budget in [1, 3, 119] {
        let api = Server::start(|origin| {
            let mut replies = vec![Reply::submitted(origin)];
            replies.extend((0..budget).map(|_| Reply::json(json!({"status":"failed"}))));
            replies
        })
        .await;
        let mut cfg = config(&api.origin);
        cfg.max_polls = budget;
        let result = AzureImageClient::new(cfg)
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        let calls = api.finish().await;
        assert_eq!(result, Err(ImageError::Timeout));
        assert_eq!(calls.len(), budget + 1);
        assert_eq!(calls.iter().filter(|r| r.method == "POST").count(), 1);
    }
}

#[tokio::test]
async fn operation_location_rejects_unsafe_targets_before_touching_them() {
    let malicious = Server::start(|_| vec![]).await;
    let changed_port = malicious.origin.clone();
    let changed_scheme = malicious.origin.replacen("http:", "https:", 1);
    for target in [
        changed_port,
        changed_scheme,
        "/relative".into(),
        "ftp://127.0.0.1/image".into(),
        "http://user:secret@127.0.0.1/image".into(),
        "http://169.254.169.254/latest/meta-data".into(),
    ] {
        let api = Server::start(|_| {
            vec![Reply {
                headers: format!("operation-location: {target}\r\n"),
                ..Reply::json(Value::Null)
            }]
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        let calls = api.finish().await;
        assert_eq!(result, Err(ImageError::AddressDenied));
        assert_eq!(calls.len(), 1);
    }
    for suffix in ["#fragment", ""] {
        let api = Server::start(|origin| {
            let target = if suffix.is_empty() {
                origin.replacen("://", "://@", 1)
            } else {
                format!("{origin}/operation{suffix}")
            };
            vec![Reply {
                headers: format!("operation-location: {target}\r\n"),
                ..Reply::json(Value::Null)
            }]
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        assert_eq!(api.finish().await.len(), 1);
        assert_eq!(result, Err(ImageError::AddressDenied));
    }
    assert!(malicious.finish().await.is_empty());
}

#[tokio::test]
async fn operation_header_must_be_unique_nonempty_and_bounded() {
    for (headers, expected) in [
        (String::new(), ImageError::Protocol),
        ("operation-location: \r\n".into(), ImageError::Protocol),
        (
            "operation-location: http://127.0.0.1/a\r\noperation-location: http://127.0.0.1/b\r\n"
                .into(),
            ImageError::Protocol,
        ),
        (
            format!(
                "operation-location: http://127.0.0.1/{}\r\n",
                "x".repeat(8192)
            ),
            ImageError::TooLarge,
        ),
    ] {
        let api = Server::start(|_| {
            vec![Reply {
                headers,
                ..Reply::json(Value::Null)
            }]
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        assert_eq!(api.finish().await.len(), 1);
        assert_eq!(result, Err(expected));
    }
    for extra in [
        format!("x-big: {}\r\n", "x".repeat(16 * 1024)),
        (0..65).map(|n| format!("x-{n}: x\r\n")).collect(),
    ] {
        let api = Server::start(|origin| {
            vec![Reply {
                headers: format!("{}{}", Reply::submitted(origin).headers, extra),
                ..Reply::json(Value::Null)
            }]
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        assert_eq!(api.finish().await.len(), 1);
        assert_eq!(result, Err(ImageError::TooLarge));
    }
}

#[tokio::test]
async fn redirect_and_http_errors_never_retry_or_forward_key() {
    let malicious = Server::start(|_| vec![]).await;
    for status in [301, 302, 307, 308, 400, 401, 429, 500] {
        for poll_error in [false, true] {
            let api = Server::start(|origin| {
                let error = Reply {
                    status,
                    headers: format!("Location: {}/steal\r\n", malicious.origin),
                    ..Reply::json(json!({"secret":"provider-body"}))
                };
                if poll_error {
                    vec![Reply::submitted(origin), error]
                } else {
                    vec![error]
                }
            })
            .await;
            let result = AzureImageClient::new(config(&api.origin))
                .unwrap()
                .generate("private-prompt", &CancellationToken::new())
                .await;
            let calls = api.finish().await;
            assert_eq!(result, Err(ImageError::Http(status)));
            assert_eq!(calls.len(), if poll_error { 2 } else { 1 });
            let debug = format!("{result:?}");
            for secret in [
                "provider-body",
                "private-prompt",
                "synthetic-azure-secret",
                "operation-secret",
            ] {
                assert!(!debug.contains(secret));
            }
        }
    }
    assert!(malicious.finish().await.is_empty());
}

#[tokio::test]
async fn malformed_status_and_success_shapes_fail_without_retry() {
    for (body, expected) in [
        (b"not json".to_vec(), ImageError::Protocol),
        (br#"{"status":true}"#.to_vec(), ImageError::Protocol),
        (br#"{"status":null}"#.to_vec(), ImageError::Protocol),
        (br#"{}"#.to_vec(), ImageError::Protocol),
        (
            br#"{"status":"running","status":"succeeded"}"#.to_vec(),
            ImageError::Protocol,
        ),
        (
            br#"{"status":"succeeded"} trailing"#.to_vec(),
            ImageError::Protocol,
        ),
        (
            br#"{"status":"succeeded","result":123}"#.to_vec(),
            ImageError::Protocol,
        ),
        (
            br#"{"status":"succeeded","result":{"data":{}}}"#.to_vec(),
            ImageError::Protocol,
        ),
        (
            br#"{"status":"succeeded","result":{"data":[{"url":23}]}}"#.to_vec(),
            ImageError::Protocol,
        ),
        (
            br#"{"status":"succeeded","result":{"data":[]}}"#.to_vec(),
            ImageError::NoImage,
        ),
        (
            br#"{"status":"succeeded","result":{"data":[{"url":""}]}}"#.to_vec(),
            ImageError::NoImage,
        ),
        (br#"{"status":"succeeded"}"#.to_vec(), ImageError::NoImage),
    ] {
        let api = Server::start(|origin| {
            vec![
                Reply::submitted(origin),
                Reply {
                    body,
                    ..Reply::json(Value::Null)
                },
            ]
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        assert_eq!(api.finish().await.len(), 2);
        assert_eq!(result, Err(expected));
    }
}

#[tokio::test]
async fn first_url_only_and_no_base64_or_later_result_fallback() {
    let (api, download, cfg) = image_servers(|url| Reply::json(json!({"status":"succeeded","result":{"data":[{"url":url,"b64_json":"ignored"},123,{"url":"http://evil.invalid"}]}})), png(png::ColorType::Rgb)).await;
    let result = AzureImageClient::new(cfg)
        .unwrap()
        .generate("test", &CancellationToken::new())
        .await;
    api.finish().await;
    assert_eq!(download.finish().await.len(), 1);
    assert!(result.is_ok());
    for first in [
        json!({}),
        json!({"url":null}),
        json!({"url":""}),
        json!({"b64_json":base64::engine::general_purpose::STANDARD.encode(png(png::ColorType::Rgb))}),
    ] {
        let (api, download, cfg) = image_servers(
            |url| Reply::json(json!({"status":"succeeded","result":{"data":[first,{"url":url}]}})),
            png(png::ColorType::Rgb),
        )
        .await;
        let result = AzureImageClient::new(cfg)
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        api.finish().await;
        assert!(download.finish().await.is_empty());
        assert_eq!(result, Err(ImageError::NoImage));
    }
}

#[tokio::test]
async fn hosted_policy_is_deny_default_and_rejects_private_resolution_and_credentials() {
    let malicious = Server::start(|_| vec![]).await;
    for hosted in [
        format!("{}/image", malicious.origin),
        "https://127.0.0.1/image".into(),
        "https://localhost/image".into(),
        "https://169.254.169.254/image".into(),
        "https://user:secret@fixture.example/image".into(),
        "https://fixture.example/image#secret".into(),
    ] {
        let api =
            Server::start(|origin| vec![Reply::submitted(origin), Reply::success(&hosted)]).await;
        let mut cfg = config(&api.origin);
        cfg.hosted_images = HostedImagePolicy::https_origins(&[
            "https://127.0.0.1",
            "https://localhost",
            "https://169.254.169.254",
            "https://fixture.example",
        ])
        .unwrap();
        let result = AzureImageClient::new(cfg)
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        api.finish().await;
        assert_eq!(result, Err(ImageError::AddressDenied));
    }
    let api = Server::start(|origin| {
        vec![
            Reply::submitted(origin),
            Reply::success(&format!("{}/image", malicious.origin)),
        ]
    })
    .await;
    let result = AzureImageClient::new(config(&api.origin))
        .unwrap()
        .generate("test", &CancellationToken::new())
        .await;
    api.finish().await;
    assert_eq!(result, Err(ImageError::AddressDenied));
    assert!(malicious.finish().await.is_empty());
}

#[tokio::test]
async fn hosted_redirect_stays_denied_and_never_reaches_redirect_target() {
    let malicious = Server::start(|_| vec![]).await;
    let download = Server::start(|_| {
        vec![Reply {
            status: 302,
            headers: format!("Location: {}/stolen\r\n", malicious.origin),
            ..Reply::json(Value::Null)
        }]
    })
    .await;
    let api = Server::start(|origin| {
        vec![
            Reply::submitted(origin),
            Reply::success(&format!("{}/image", download.origin)),
        ]
    })
    .await;
    let mut cfg = config(&api.origin);
    cfg.hosted_images =
        HostedImagePolicy::loopback_fixtures(&[&download.origin, &malicious.origin]).unwrap();
    let result = AzureImageClient::new(cfg)
        .unwrap()
        .generate("test", &CancellationToken::new())
        .await;
    api.finish().await;
    let calls = download.finish().await;
    assert!(malicious.finish().await.is_empty());
    assert_eq!(result, Err(ImageError::Http(302)));
    assert_eq!(calls.len(), 1);
    assert!(!calls[0].headers.contains_key("api-key"));
    assert!(!calls[0].headers.contains_key("authorization"));
}

#[tokio::test]
async fn input_response_download_dimension_and_decode_budgets_are_enforced() {
    let unused = Server::start(|_| vec![]).await;
    let mut cfg = config(&unused.origin);
    cfg.limits.max_prompt_bytes = 3;
    let result = AzureImageClient::new(cfg)
        .unwrap()
        .generate("four", &CancellationToken::new())
        .await;
    assert!(unused.finish().await.is_empty());
    assert_eq!(result, Err(ImageError::TooLarge));
    for chunked in [false, true] {
        for oversized_submit in [false, true] {
            let api = Server::start(|origin| {
                let large = Reply {
                    body: vec![b'x'; 129],
                    chunked,
                    ..Reply::submitted(origin)
                };
                if oversized_submit {
                    vec![large]
                } else {
                    vec![Reply::submitted(origin), large]
                }
            })
            .await;
            let mut cfg = config(&api.origin);
            cfg.limits.max_response_bytes = 128;
            let result = AzureImageClient::new(cfg)
                .unwrap()
                .generate("test", &CancellationToken::new())
                .await;
            assert_eq!(
                api.finish().await.len(),
                if oversized_submit { 1 } else { 2 }
            );
            assert_eq!(result, Err(ImageError::TooLarge));
        }
    }
    for mode in 0..4 {
        let (api, download, mut cfg) = image_servers(
            Reply::success,
            if mode == 3 {
                b"GIF89a".to_vec()
            } else {
                png(png::ColorType::Rgb)
            },
        )
        .await;
        match mode {
            0 => cfg.limits.max_encoded_image_bytes = 8,
            1 => cfg.limits.max_pixels = 1,
            2 => cfg.limits.max_decode_bytes = 1024,
            _ => (),
        }
        let result = AzureImageClient::new(cfg)
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        api.finish().await;
        download.finish().await;
        assert_eq!(
            result,
            Err(if mode == 3 {
                ImageError::Decode
            } else {
                ImageError::TooLarge
            })
        );
    }
}

#[tokio::test]
async fn compressed_submit_and_poll_responses_are_rejected() {
    for compressed_submit in [false, true] {
        let api = Server::start(|origin| {
            let mut compressed = if compressed_submit {
                Reply::submitted(origin)
            } else {
                Reply::json(json!({"status":"running"}))
            };
            compressed.headers.push_str("Content-Encoding: gzip\r\n");
            if compressed_submit {
                vec![compressed]
            } else {
                vec![Reply::submitted(origin), compressed]
            }
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        api.finish().await;
        assert_eq!(result, Err(ImageError::Protocol));
    }
}

#[tokio::test]
async fn total_deadline_covers_submit_poll_sleep_and_download() {
    for phase in 0..4 {
        let download = Server::start(|_| {
            vec![Reply {
                delay: Duration::from_secs(30),
                ..Reply::image(png(png::ColorType::Rgb))
            }]
        })
        .await;
        let api = Server::start(|origin| {
            let mut submission = Reply::submitted(origin);
            let mut poll = Reply::success(&format!("{}/image", download.origin));
            if phase == 0 {
                submission.delay = Duration::from_secs(30);
            }
            if phase == 2 {
                poll.delay = Duration::from_secs(30);
            }
            vec![submission, poll]
        })
        .await;
        let mut cfg = config(&api.origin);
        cfg.limits.request_timeout = Duration::from_millis(60);
        if phase == 1 {
            cfg.poll_interval = Duration::from_secs(1);
        }
        cfg.hosted_images = HostedImagePolicy::loopback_fixtures(&[&download.origin]).unwrap();
        let client = AzureImageClient::new(cfg).unwrap();
        let mut task =
            tokio::spawn(async move { client.generate("test", &CancellationToken::new()).await });
        let result = joined(&mut task).await;
        let (calls, api_joined) = api.finish_observed().await;
        let (downloads, download_joined) = download.finish_observed().await;
        assert!(api_joined && download_joined);
        assert_eq!(result.unwrap(), Err(ImageError::Timeout));
        assert_eq!(calls.len(), if phase <= 1 { 1 } else { 2 });
        assert_eq!(downloads.len(), usize::from(phase == 3));
    }
}

#[tokio::test]
async fn cancellation_and_task_drop_stop_network_and_release_admission() {
    for abort in [false, true] {
        let api = Server::start(|origin| vec![Reply::submitted(origin)]).await;
        let mut cfg = config(&api.origin);
        cfg.limits.max_pending = 1;
        cfg.poll_interval = Duration::from_secs(1);
        let client = AzureImageClient::new(cfg).unwrap();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let owner = client.clone();
        let mut task = tokio::spawn(async move { owner.generate("private-prompt", &token).await });
        let observed = api.wait_for(1).await;
        let overflow = client.generate("second", &CancellationToken::new()).await;
        let mut abort_observed = false;
        let result = if abort {
            task.abort();
            abort_observed = matches!(task.await, Err(error) if error.is_cancelled());
            None
        } else {
            cancel.cancel();
            Some(joined(&mut task).await)
        };
        // A fresh request reaches the fixture's fallback 500, proving admission
        // is free after cancellation/drop rather than merely testing early cancel.
        let after = client.generate("after", &CancellationToken::new()).await;
        // A canceled caller cannot issue a request even when admission is now free.
        let already_canceled = CancellationToken::new();
        already_canceled.cancel();
        let canceled = client.generate("third", &already_canceled).await;
        drop(client); // last-owner drop synchronously joins the owned decoder lane
        let (calls, api_joined) = api.finish_observed().await;
        assert!(observed && api_joined);
        assert!(!abort || abort_observed);
        assert_eq!(overflow, Err(ImageError::Backpressure));
        if let Some(result) = result {
            assert_eq!(result.unwrap(), Err(ImageError::Cancelled));
        }
        assert_eq!(after, Err(ImageError::Http(500)));
        assert_eq!(canceled, Err(ImageError::Cancelled));
        assert_eq!(calls.len(), 2);
    }
}

#[tokio::test]
async fn cancellation_during_poll_sleep_and_download_has_no_late_delivery() {
    for during_download in [false, true] {
        let download = Server::start(|_| {
            vec![Reply {
                delay: Duration::from_secs(30),
                ..Reply::image(png(png::ColorType::Rgb))
            }]
        })
        .await;
        let api = Server::start(|origin| {
            vec![
                Reply::submitted(origin),
                Reply::success(&format!("{}/image", download.origin)),
            ]
        })
        .await;
        let mut cfg = config(&api.origin);
        cfg.poll_interval = if during_download {
            Duration::from_millis(1)
        } else {
            Duration::from_secs(1)
        };
        cfg.hosted_images = HostedImagePolicy::loopback_fixtures(&[&download.origin]).unwrap();
        let client = AzureImageClient::new(cfg).unwrap();
        let cancel = CancellationToken::new();
        let token = cancel.clone();
        let mut task = tokio::spawn(async move { client.generate("test", &token).await });
        let observed = if during_download {
            download.wait_for(1).await
        } else {
            api.wait_for(1).await
        };
        cancel.cancel();
        let result = joined(&mut task).await;
        let (calls, api_joined) = api.finish_observed().await;
        let (downloads, download_joined) = download.finish_observed().await;
        assert!(observed && api_joined && download_joined);
        assert_eq!(result.unwrap(), Err(ImageError::Cancelled));
        assert_eq!(calls.len(), if during_download { 2 } else { 1 });
        assert_eq!(downloads.len(), usize::from(during_download));
    }
}

#[tokio::test]
async fn same_origin_redirects_are_also_rejected_without_another_request() {
    for redirect_poll in [false, true] {
        let api = Server::start(|origin| {
            let redirect = Reply {
                status: 307,
                headers: format!("Location: {origin}/redirected\r\n"),
                ..Reply::json(Value::Null)
            };
            if redirect_poll {
                vec![Reply::submitted(origin), redirect]
            } else {
                vec![redirect]
            }
        })
        .await;
        let result = AzureImageClient::new(config(&api.origin))
            .unwrap()
            .generate("test", &CancellationToken::new())
            .await;
        let calls = api.finish().await;
        assert_eq!(result, Err(ImageError::Http(307)));
        assert_eq!(calls.len(), if redirect_poll { 2 } else { 1 });
        assert!(calls.iter().all(|call| call.path != "/redirected"));
    }
}
