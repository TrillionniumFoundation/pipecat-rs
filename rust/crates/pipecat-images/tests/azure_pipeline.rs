//! Real loopback Azure submit/poll/download through the shared native processor.
//! No Azure credentials, provider SDKs, mock pixels, or provider availability claim.
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
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

const WAIT: Duration = Duration::from_secs(5);
type TestResult<T> = std::result::Result<T, String>;

#[derive(Clone, Debug)]
struct Request {
    method: String,
    path: String,
    headers: BTreeMap<String, String>,
    body: Vec<u8>,
}
struct Reply {
    status: u16,
    body: Vec<u8>,
    headers: String,
    release: Option<oneshot::Receiver<()>>,
    closed: Option<oneshot::Sender<bool>>,
}
impl Reply {
    fn json(status: u16, body: Value) -> Self {
        Self {
            status,
            body: body.to_string().into_bytes(),
            headers: String::new(),
            release: None,
            closed: None,
        }
    }
    fn png(mode: png::ColorType) -> Self {
        let mut bytes = Vec::new();
        {
            let mut encoder = png::Encoder::new(&mut bytes, 2, 1);
            encoder.set_color(mode);
            encoder.set_depth(png::BitDepth::Eight);
            let pixels: &[u8] = match mode {
                png::ColorType::Rgba => &[255, 0, 3, 0, 4, 5, 6, 127],
                png::ColorType::GrayscaleAlpha => &[42, 0, 97, 127],
                _ => panic!("fixture uses grayscale-alpha or RGBA PNG"),
            };
            encoder
                .write_header()
                .unwrap()
                .write_image_data(pixels)
                .unwrap();
        }
        Self {
            body: bytes,
            ..Self::json(200, Value::Null)
        }
    }
    fn submitted(origin: &str, index: usize) -> Self {
        Self {
            headers: format!("Operation-Location: {origin}/operations/{index}\r\n"),
            ..Self::json(202, json!({}))
        }
    }
    fn succeeded(url: &str) -> Self {
        Self::json(
            200,
            json!({"status":"succeeded","result":{"data":[{"url":url}]}}),
        )
    }
    fn held(self, release: oneshot::Receiver<()>) -> Self {
        Self {
            release: Some(release),
            ..self
        }
    }
    fn until_closed(closed: oneshot::Sender<bool>) -> Self {
        Self {
            closed: Some(closed),
            ..Self::json(200, Value::Null)
        }
    }
}
async fn read_request(socket: &mut TcpStream) -> TestResult<Request> {
    let mut bytes = Vec::new();
    let split = loop {
        let mut chunk = [0u8; 4096];
        let count = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if count == 0 || bytes.len() + count > 64 * 1024 {
            return Err("missing or oversized fixture request headers".into());
        }
        bytes.extend_from_slice(&chunk[..count]);
        if let Some(index) = bytes.windows(4).position(|part| part == b"\r\n\r\n") {
            break index;
        }
    };
    let head = std::str::from_utf8(&bytes[..split]).map_err(|e| e.to_string())?;
    let mut lines = head.split("\r\n");
    let mut first = lines
        .next()
        .ok_or("missing request line")?
        .split_whitespace();
    let method = first.next().ok_or("missing method")?.to_owned();
    let path = first.next().ok_or("missing path")?.to_owned();
    let headers: BTreeMap<String, String> = lines
        .map(|line| {
            let (key, value) = line.split_once(':').ok_or("invalid header")?;
            Ok((key.to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect::<TestResult<_>>()?;
    let length = headers
        .get("content-length")
        .map(|value| value.parse::<usize>().map_err(|e| e.to_string()))
        .transpose()?
        .unwrap_or(0);
    if length > 64 * 1024 || headers.contains_key("transfer-encoding") {
        return Err("unexpected fixture request framing".into());
    }
    let mut body = bytes[split + 4..].to_vec();
    while body.len() < length {
        let mut chunk = [0u8; 4096];
        let count = socket.read(&mut chunk).await.map_err(|e| e.to_string())?;
        if count == 0 {
            return Err("truncated fixture request".into());
        }
        body.extend_from_slice(&chunk[..count]);
    }
    if body.len() != length {
        return Err("fixture request exceeded content length".into());
    }
    Ok(Request {
        method,
        path,
        headers,
        body,
    })
}

struct Peer {
    requests: mpsc::UnboundedReceiver<Request>,
    shutdown: CancellationToken,
    task: Option<JoinHandle<TestResult<Vec<Request>>>>,
}
impl Peer {
    async fn bind() -> (TcpListener, String) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", listener.local_addr().unwrap());
        (listener, origin)
    }
    fn start(listener: TcpListener, replies: Vec<Reply>) -> Self {
        let (tx, requests) = mpsc::unbounded_channel();
        let shutdown = CancellationToken::new();
        let cancelled = shutdown.clone();
        let task = tokio::spawn(async move {
            let mut recorded = Vec::new();
            for reply in replies {
                let step = async {
                    let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
                    let request = read_request(&mut socket).await?;
                    recorded.push(request.clone());
                    let _ = tx.send(request);
                    if let Some(release) = reply.release {
                        release
                            .await
                            .map_err(|_| "fixture release dropped".to_string())?;
                    }
                    if let Some(closed) = reply.closed {
                        let mut byte = [0];
                        let eof = socket.read(&mut byte).await.map_err(|e| e.to_string())? == 0;
                        let _ = closed.send(eof);
                        return Ok::<(), String>(());
                    }
                    let head = format!(
                        "HTTP/1.1 {} Test\r\nConnection: close\r\nContent-Length: {}\r\n{}\r\n",
                        reply.status,
                        reply.body.len(),
                        reply.headers
                    );
                    socket
                        .write_all(head.as_bytes())
                        .await
                        .map_err(|e| e.to_string())?;
                    socket
                        .write_all(&reply.body)
                        .await
                        .map_err(|e| e.to_string())?;
                    Ok(())
                };
                tokio::select! {
                    _ = cancelled.cancelled() => return Ok(recorded),
                    result = tokio::time::timeout(WAIT, step) => {
                        result.map_err(|_| "fixture step timed out".to_string())??;
                    }
                }
            }
            Ok(recorded)
        });
        Self {
            requests,
            shutdown,
            task: Some(task),
        }
    }
    async fn request(&mut self) -> TestResult<Request> {
        self.requests
            .recv()
            .await
            .ok_or_else(|| "fixture request channel closed".into())
    }
    async fn finish(mut self) -> TestResult<Vec<Request>> {
        self.shutdown.cancel();
        let mut task = self.task.take().ok_or("fixture already joined")?;
        match tokio::time::timeout(WAIT, &mut task).await {
            Ok(joined) => joined.map_err(|e| e.to_string())?,
            Err(_) => {
                task.abort();
                let joined = task.await;
                Err(format!("fixture cleanup timed out; abort/join={joined:?}"))
            }
        }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.shutdown.cancel();
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

fn config(api: &str, download: &str) -> AzureImageConfig {
    let mut config = AzureImageConfig::new("azure-fixture-key", api);
    config.allow_loopback_api = true;
    config.poll_interval = Duration::from_millis(1);
    config.limits.request_timeout = Duration::from_secs(3);
    config.limits.max_pending = 8;
    config.hosted_images = HostedImagePolicy::loopback_fixtures(&[download]).unwrap();
    config
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
async fn pipeline(service: Arc<ImageGenProcessor>, pixels: Arc<Pixels>) -> PipelineWorker {
    let output = Arc::new(
        OutputTransport::new(
            "azure-pixels",
            OutputTransportConfig {
                auto_silence: false,
                end_silence: Duration::ZERO,
                queue_byte_capacity: 1024 * 1024,
                destinations: vec!["canvas".into()],
                ..Default::default()
            },
            pixels,
            None,
            BTreeMap::new(),
        )
        .unwrap(),
    );
    let mut config = PipelineConfig {
        shutdown_timeout: Duration::from_secs(2),
        ..Default::default()
    };
    config.start.enable_metrics = true;
    tokio::time::timeout(WAIT, Pipeline::new(vec![service, output]).start(config))
        .await
        .unwrap()
        .unwrap()
}
async fn send(pipeline: &PipelineWorker, frame: Frame) -> TestResult<()> {
    pipeline
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .map_err(|e| e.to_string())
}
async fn finish_pipeline(
    pipeline: PipelineWorker,
    terminal: Option<Frame>,
    failed: bool,
) -> TestResult<(Vec<DirectedFrame>, WorkerReport)> {
    let sender = pipeline.sender();
    let mut task = tokio::spawn(async move {
        let sent = if failed {
            pipeline.cancel("Azure fixture exercise failed").await
        } else if let Some(terminal) = terminal {
            pipeline
                .sender()
                .send(terminal, Direction::Downstream)
                .await
        } else {
            Ok(())
        };
        let result = pipeline.collect().await;
        // Always collect before reporting a terminal-send error.
        sent.map_err(|e| e.to_string())?;
        result.map_err(|e| e.to_string())
    });
    match tokio::time::timeout(WAIT, &mut task).await {
        Ok(result) => result.map_err(|e| e.to_string())?,
        Err(_) => {
            let cancellation =
                tokio::time::timeout(WAIT, sender.cancel("Azure fixture completion timeout")).await;
            let cleanup = tokio::time::timeout(WAIT, &mut task).await;
            if cleanup.is_err() {
                task.abort();
                let _ = task.await;
            }
            Err(format!(
                "pipeline completion timed out; cancellation={cancellation:?}, cleanup={cleanup:?}"
            ))
        }
    }
}
fn update(target: ProcessorReference, model: Value, size: Value) -> Frame {
    Frame::new(FrameData::ServiceUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            service: Some(target),
            settings: BTreeMap::from([("model".into(), model), ("image_size".into(), size)]),
            ..Default::default()
        },
    )))
}
fn assert_wire(requests: &[Request]) {
    for request in requests {
        assert_eq!(
            request.headers.get("api-key").map(String::as_str),
            Some("azure-fixture-key")
        );
        assert!(!request.headers.contains_key("authorization"));
        if request.method == "POST" {
            assert_eq!(
                request.path,
                "/openai/images/generations:submit?api-version=2023-06-01-preview"
            );
            let body: Value = serde_json::from_slice(&request.body).unwrap();
            assert_eq!(body["n"], 1);
            assert!(
                body.get("model").is_none(),
                "metadata model leaked onto wire"
            );
        } else {
            assert_eq!(request.method, "GET");
            assert!(request.path.starts_with("/operations/"));
            assert!(request.body.is_empty());
        }
    }
}

#[tokio::test]
async fn end_and_stop_drain_real_azure_rgb_to_native_output_in_order() {
    for stop in [false, true] {
        let (download_listener, download_origin) = Peer::bind().await;
        let image_urls = [
            format!("{download_origin}/gray.png?signature=fixture"),
            format!("{download_origin}/rgba.png"),
        ];
        let download = Peer::start(
            download_listener,
            vec![
                Reply::png(png::ColorType::GrayscaleAlpha),
                Reply::png(png::ColorType::Rgba),
            ],
        );
        let (listener, origin) = Peer::bind().await;
        let mut api = Peer::start(
            listener,
            vec![
                Reply::submitted(&origin, 0),
                Reply::json(200, json!({"status":"running"})),
                Reply::succeeded(&image_urls[0]),
                Reply::submitted(&origin, 1),
                Reply::succeeded(&image_urls[1]),
            ],
        );
        let service = Arc::new(ImageGenProcessor::azure(
            AzureImageClient::new(config(&origin, &download_origin)).unwrap(),
        ));
        let pixels = Arc::new(Pixels::default());
        let pipeline = pipeline(service.clone(), pixels.clone()).await;
        let mut first = Frame::text("gray with alpha");
        first.transport_source = Some("user-camera".into());
        first.transport_destination = Some("canvas".into());
        first.metadata.insert("fixture".into(), json!("preserved"));
        let second = Frame::text("rgba with alpha");
        let thought = Frame::new(FrameData::LlmThoughtText(TextFrame::new(
            "passthrough only",
        )));
        let exercised = tokio::time::timeout(WAIT, async {
            send(&pipeline, first.clone()).await?;
            api.request().await?;
            send(&pipeline, second.clone()).await?;
            send(&pipeline, thought.clone()).await
        })
        .await;
        let finished = finish_pipeline(
            pipeline,
            Some(if stop {
                Frame::new(FrameData::Stop)
            } else {
                Frame::end()
            }),
            !matches!(&exercised, Ok(Ok(()))),
        )
        .await;
        let api_requests = api.finish().await;
        let downloads = download.finish().await;

        exercised.unwrap().unwrap();
        let (frames, report) = finished.unwrap();
        assert_eq!(
            report.state,
            if stop {
                WorkerState::Stopped
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(report.total_errors, 0, "{report:#?}");
        let api_requests = api_requests.unwrap();
        assert_eq!(api_requests.len(), 5);
        assert_wire(&api_requests);
        let downloads = downloads.unwrap();
        assert_eq!(downloads.len(), 2);
        for request in &downloads {
            assert_eq!(request.method, "GET");
            assert!(!request.headers.contains_key("api-key"));
            assert!(!request.headers.contains_key("authorization"));
            assert!(!request.headers.contains_key("cookie"));
        }
        assert_eq!(downloads[0].path, "/gray.png?signature=fixture");
        let captured = pixels.0.lock().unwrap().clone();
        assert_eq!(captured.len(), 2);
        for (index, expected) in [vec![42, 42, 42, 97, 97, 97], vec![255, 0, 3, 4, 5, 6]]
            .iter()
            .enumerate()
        {
            let FrameData::UrlImage(image) = &captured[index].data else {
                panic!("missing native URL image")
            };
            assert_eq!(&image.image.image.image, expected);
            assert_eq!(image.image.image.size, (2, 1));
            assert_eq!(image.image.image.format.as_deref(), Some("RGB"));
            assert_eq!(image.url.as_deref(), Some(image_urls[index].as_str()));
            assert!(!image.image.sync_with_audio);
        }
        assert_eq!(captured[0].transport_source, first.transport_source);
        assert_eq!(
            captured[0].transport_destination,
            first.transport_destination
        );
        assert_eq!(
            captured[0].metadata["image_generation.source_frame_id"],
            json!(first.id)
        );
        assert_eq!(
            captured[1].metadata["image_generation.source_frame_id"],
            json!(second.id)
        );
        let relevant: Vec<_> = frames
            .iter()
            .filter(|event| {
                matches!(
                    event.frame.data,
                    FrameData::Text(_) | FrameData::LlmThoughtText(_) | FrameData::UrlImage(_)
                )
            })
            .collect();
        assert_eq!(relevant.len(), 5, "{frames:#?}");
        assert_eq!(
            relevant[0],
            &DirectedFrame::new(first, Direction::Downstream)
        );
        assert!(matches!(relevant[1].frame.data, FrameData::UrlImage(_)));
        assert_eq!(
            relevant[2],
            &DirectedFrame::new(second, Direction::Downstream)
        );
        assert!(matches!(relevant[3].frame.data, FrameData::UrlImage(_)));
        assert_eq!(
            relevant[4],
            &DirectedFrame::new(thought, Direction::Downstream)
        );
        let metrics: Vec<_> = frames
            .iter()
            .flat_map(|event| match &event.frame.data {
                FrameData::Metrics(metrics) => metrics.as_slice(),
                _ => &[],
            })
            .collect();
        assert_eq!(metrics.iter().filter(|metric| matches!(metric, Metric::Processing { processor, .. } if processor == "AzureImageGenServiceREST")).count(), 2);
        assert_eq!(service.name(), "AzureImageGenServiceREST");
        assert_eq!(
            service.update_settings(&ImageGenerationSettingsDelta::default()),
            Err(ImageError::Closed)
        );
    }
}

#[tokio::test]
async fn upstream_inherited_text_and_nontext_preserve_identity_and_direction() {
    let (download_listener, download_origin) = Peer::bind().await;
    let url = format!("{download_origin}/image.png");
    let download = Peer::start(download_listener, vec![Reply::png(png::ColorType::Rgba)]);
    let (listener, origin) = Peer::bind().await;
    let mut api = Peer::start(
        listener,
        vec![Reply::submitted(&origin, 0), Reply::succeeded(&url)],
    );
    let service = Arc::new(ImageGenProcessor::azure(
        AzureImageClient::new(config(&origin, &download_origin)).unwrap(),
    ));
    let pixels = Arc::new(Pixels::default());
    let pipeline = pipeline(service, pixels.clone()).await;
    let mut input = Frame::new(FrameData::InputText(TextFrame::new(
        "upstream inherited input",
    )));
    input.transport_source = Some("typed-source".into());
    input.transport_destination = Some("canvas".into());
    let thought = Frame::new(FrameData::LlmThoughtText(TextFrame::new(
        "upstream non-prompt",
    )));
    let exercised = tokio::time::timeout(WAIT, async {
        pipeline
            .sender()
            .send(input.clone(), Direction::Upstream)
            .await
            .map_err(|e| e.to_string())?;
        api.request().await?;
        pipeline
            .sender()
            .send(thought.clone(), Direction::Upstream)
            .await
            .map_err(|e| e.to_string())
    })
    .await;
    let finished = finish_pipeline(
        pipeline,
        Some(Frame::end()),
        !matches!(&exercised, Ok(Ok(()))),
    )
    .await;
    let api_requests = api.finish().await;
    let downloads = download.finish().await;

    exercised.unwrap().unwrap();
    let (frames, report) = finished.unwrap();
    assert_eq!(report.total_errors, 0, "{report:#?}");
    assert_eq!(api_requests.unwrap().len(), 2);
    assert_eq!(downloads.unwrap().len(), 1);
    assert!(frames.contains(&DirectedFrame::new(input.clone(), Direction::Upstream)));
    assert!(frames.contains(&DirectedFrame::new(thought, Direction::Upstream)));
    let image_index = frames
        .iter()
        .position(|event| matches!(event.frame.data, FrameData::UrlImage(_)))
        .unwrap();
    let text_index = frames
        .iter()
        .position(|event| event.frame.id == input.id)
        .unwrap();
    assert!(text_index < image_index);
    assert_eq!(frames[image_index].direction, Direction::Downstream);
    let images = pixels.0.lock().unwrap();
    assert_eq!(images.len(), 1);
    assert_eq!(
        images[0].metadata["image_generation.source_frame_id"],
        json!(input.id)
    );
    assert_eq!(images[0].transport_source, input.transport_source);
    assert_eq!(images[0].transport_destination, input.transport_destination);
}

#[tokio::test]
async fn azure_settings_are_atomic_targeted_and_snapshotted_at_admission() {
    let (download_listener, download_origin) = Peer::bind().await;
    let url = format!("{download_origin}/image.png");
    let download = Peer::start(
        download_listener,
        (0..4).map(|_| Reply::png(png::ColorType::Rgba)).collect(),
    );
    let (listener, origin) = Peer::bind().await;
    let (release_tx, release_rx) = oneshot::channel();
    let mut replies = vec![
        Reply::submitted(&origin, 0).held(release_rx),
        Reply::succeeded(&url),
    ];
    for index in 1..4 {
        replies.extend([Reply::submitted(&origin, index), Reply::succeeded(&url)]);
    }
    let mut api = Peer::start(listener, replies);
    let service = Arc::new(ImageGenProcessor::azure(
        AzureImageClient::new(config(&origin, &download_origin)).unwrap(),
    ));
    let pixels = Arc::new(Pixels::default());
    let pipeline = pipeline(service.clone(), pixels.clone()).await;
    let exercised = tokio::time::timeout(WAIT, async {
        send(&pipeline, Frame::text("first")).await?;
        api.request().await?;
        // Text and settings share the normal lane. The named update is a
        // deterministic admission barrier while the first HTTP request is held.
        send(&pipeline, Frame::text("admitted-before-update")).await?;
        let ignored = update(
            ProcessorReference::Name("OpenAIImageGenService".into()),
            json!("wrong-target"),
            json!("wrong-size"),
        );
        let ignored_id = ignored.id;
        send(&pipeline, ignored).await?;
        send(
            &pipeline,
            update(
                ProcessorReference::Name(service.name().into()),
                json!("must-not-apply"),
                json!("x".repeat(129)),
            ),
        )
        .await?;
        while pipeline.total_errors() == 0 {
            tokio::task::yield_now().await;
        }
        let after_invalid = service.settings().map_err(|e| e.to_string())?;
        send(
            &pipeline,
            update(
                ProcessorReference::Name(service.name().into()),
                json!("name-metadata"),
                json!("future-size"),
            ),
        )
        .await?;
        while service
            .settings()
            .map_err(|e| e.to_string())?
            .image_size
            .as_deref()
            != Some("future-size")
        {
            tokio::task::yield_now().await;
        }
        let after_name = service.settings().map_err(|e| e.to_string())?;
        send(&pipeline, Frame::text("admitted-with-size")).await?;
        // Local-target typed settings exercise the same native identity path.
        let local = Frame::new(FrameData::ServiceUpdateSettings(Box::new(
            ServiceUpdateSettingsFrame {
                service: Some(ProcessorReference::Local(LocalProcessorReference(
                    service.clone(),
                ))),
                delta: Some(ServiceSettings {
                    model: Setting::Value("local-metadata".into()),
                    extra: BTreeMap::from([("image_size".into(), Value::Null)]),
                }),
                ..Default::default()
            },
        )));
        send(&pipeline, local).await?;
        while service
            .settings()
            .map_err(|e| e.to_string())?
            .model
            .as_deref()
            != Some("local-metadata")
        {
            tokio::task::yield_now().await;
        }
        let after_local = service.settings().map_err(|e| e.to_string())?;
        send(&pipeline, Frame::text("admitted-after-clear")).await?;
        Ok::<_, String>((after_invalid, after_name, after_local, ignored_id))
    })
    .await;
    let released = release_tx.send(());
    let finished = finish_pipeline(
        pipeline,
        Some(Frame::end()),
        !matches!(&exercised, Ok(Ok(_))) || released.is_err(),
    )
    .await;
    let api_requests = api.finish().await;
    let downloads = download.finish().await;

    let (after_invalid, after_name, after_local, ignored_id) = exercised.unwrap().unwrap();
    released.unwrap();
    let (frames, report) = finished.unwrap();
    assert_eq!(report.total_errors, 1, "{report:#?}");
    assert!(service.health().unwrap().is_usable());
    assert_eq!(
        after_invalid,
        ImageGenerationSettings {
            model: None,
            image_size: None
        }
    );
    assert_eq!(
        after_name,
        ImageGenerationSettings {
            model: Some("name-metadata".into()),
            image_size: Some("future-size".into())
        }
    );
    assert_eq!(
        after_local,
        ImageGenerationSettings {
            model: Some("local-metadata".into()),
            image_size: None
        }
    );
    assert!(frames.iter().any(|event| event.frame.id == ignored_id));
    let api_requests = api_requests.unwrap();
    assert_wire(&api_requests);
    let bodies: Vec<Value> = api_requests
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| serde_json::from_slice(&request.body).unwrap())
        .collect();
    assert_eq!(
        bodies,
        vec![
            json!({"prompt":"first","n":1}),
            json!({"prompt":"admitted-before-update","n":1}),
            json!({"prompt":"admitted-with-size","n":1,"size":"future-size"}),
            json!({"prompt":"admitted-after-clear","n":1}),
        ]
    );
    assert_eq!(downloads.unwrap().len(), 4);
    assert_eq!(pixels.0.lock().unwrap().len(), 4);
}

#[tokio::test]
async fn azure_no_image_and_http_errors_never_emit_success_and_preserve_health_categories() {
    for case in 0..3 {
        let (listener, origin) = Peer::bind().await;
        let replies = match case {
            0 => vec![
                Reply::submitted(&origin, 0),
                Reply::json(200, json!({"status":"succeeded","result":{"data":[]}})),
            ],
            1 => vec![
                Reply::submitted(&origin, 0),
                Reply::json(401, json!({"error":"private-provider-body"})),
            ],
            _ => vec![Reply::json(429, json!({"error":"private-provider-body"}))],
        };
        let api = Peer::start(listener, replies);
        let service = Arc::new(ImageGenProcessor::azure(
            AzureImageClient::new(config(&origin, &origin)).unwrap(),
        ));
        let pixels = Arc::new(Pixels::default());
        let pipeline = pipeline(service.clone(), pixels.clone()).await;
        let text = Frame::text("error case");
        let exercised = tokio::time::timeout(WAIT, send(&pipeline, text.clone())).await;
        let finished = finish_pipeline(
            pipeline,
            Some(Frame::end()),
            !matches!(&exercised, Ok(Ok(()))),
        )
        .await;
        let api_requests = api.finish().await;

        exercised.unwrap().unwrap();
        let (frames, report) = finished.unwrap();
        assert_eq!(report.total_errors, 1, "{report:#?}");
        assert_eq!(report.errors.len(), 1);
        assert_eq!(
            report.errors[0].category,
            match case {
                0 => ErrorCategory::Application,
                1 => ErrorCategory::from_http_status(401),
                _ => ErrorCategory::from_http_status(429),
            }
        );
        assert_eq!(service.health().unwrap().is_usable(), case != 1);
        assert!(!report.errors[0].error.contains("private-provider-body"));
        assert!(frames.contains(&DirectedFrame::new(text, Direction::Downstream)));
        assert!(!frames
            .iter()
            .any(|event| matches!(event.frame.data, FrameData::UrlImage(_))));
        assert!(pixels.0.lock().unwrap().is_empty());
        assert_eq!(api_requests.unwrap().len(), if case == 2 { 1 } else { 2 });
    }
}

#[tokio::test]
async fn interruption_cancel_and_health_change_close_submit_poll_and_download_sockets() {
    for stage in 0..3 {
        for control in 0..3 {
            let (download_listener, download_origin) = Peer::bind().await;
            let url = format!("{download_origin}/held-image.png");
            let (closed_tx, closed_rx) = oneshot::channel();
            let held = Reply::until_closed(closed_tx);
            let (listener, origin) = Peer::bind().await;
            let (api_replies, download_replies) = match stage {
                0 => (vec![held], vec![]),
                1 => (vec![Reply::submitted(&origin, 0), held], vec![]),
                _ => (
                    vec![Reply::submitted(&origin, 0), Reply::succeeded(&url)],
                    vec![held],
                ),
            };
            let mut api = Peer::start(listener, api_replies);
            let mut download = Peer::start(download_listener, download_replies);
            let service = Arc::new(ImageGenProcessor::azure(
                AzureImageClient::new(config(&origin, &download_origin)).unwrap(),
            ));
            let health = service.health().unwrap();
            let pixels = Arc::new(Pixels::default());
            let pipeline = pipeline(service, pixels.clone()).await;
            let exercised = tokio::time::timeout(WAIT, async {
                send(&pipeline, Frame::text("must be cancelled")).await?;
                api.request().await?;
                if stage > 0 {
                    api.request().await?;
                }
                if stage == 2 {
                    download.request().await?;
                }
                match control {
                    0 => send(&pipeline, Frame::interruption()).await?,
                    1 => {
                        health.set_usable(false);
                        health.set_usable(true);
                    }
                    _ => pipeline
                        .cancel("fixture cancellation")
                        .await
                        .map_err(|e| e.to_string())?,
                }
                closed_rx
                    .await
                    .map_err(|_| "socket-close observer dropped".to_string())
            })
            .await;
            let finished = finish_pipeline(
                pipeline,
                if control == 2 {
                    None
                } else {
                    Some(Frame::end())
                },
                !matches!(&exercised, Ok(Ok(_))),
            )
            .await;
            let api_requests = api.finish().await;
            let downloads = download.finish().await;

            assert!(
                exercised.unwrap().unwrap(),
                "socket did not close: stage={stage}, control={control}"
            );
            let (frames, report) = finished.unwrap();
            assert_eq!(
                report.state,
                if control == 2 {
                    WorkerState::Cancelled
                } else {
                    WorkerState::Ended
                }
            );
            assert_eq!(
                report.total_errors, 0,
                "stage={stage}, control={control}: {report:#?}"
            );
            assert!(!frames
                .iter()
                .any(|event| matches!(event.frame.data, FrameData::UrlImage(_))));
            assert!(pixels.0.lock().unwrap().is_empty());
            assert_eq!(api_requests.unwrap().len(), if stage == 0 { 1 } else { 2 });
            assert_eq!(downloads.unwrap().len(), usize::from(stage == 2));
        }
    }
}

#[tokio::test]
async fn azure_backpressure_is_fail_fast_and_interruption_allows_fresh_admission() {
    let (download_listener, download_origin) = Peer::bind().await;
    let url = format!("{download_origin}/fresh.png");
    let download = Peer::start(download_listener, vec![Reply::png(png::ColorType::Rgba)]);
    let (listener, origin) = Peer::bind().await;
    let (closed_tx, closed_rx) = oneshot::channel();
    let mut api = Peer::start(
        listener,
        vec![
            Reply::until_closed(closed_tx),
            Reply::submitted(&origin, 1),
            Reply::succeeded(&url),
        ],
    );
    let mut config = config(&origin, &download_origin);
    config.limits.max_pending = 1;
    let service = Arc::new(ImageGenProcessor::azure(
        AzureImageClient::new(config).unwrap(),
    ));
    let pixels = Arc::new(Pixels::default());
    let pipeline = pipeline(service, pixels.clone()).await;
    let overflow = Frame::new(FrameData::InputText(TextFrame::new("must not submit")));
    let fresh = Frame::new(FrameData::InputText(TextFrame::new(
        "fresh after interruption",
    )));
    let exercised = tokio::time::timeout(WAIT, async {
        send(&pipeline, Frame::text("held submission")).await?;
        api.request().await?;
        send(&pipeline, overflow.clone()).await?;
        while pipeline.total_errors() == 0 {
            tokio::task::yield_now().await;
        }
        send(&pipeline, Frame::interruption()).await?;
        let closed = closed_rx
            .await
            .map_err(|_| "socket-close observer dropped".to_string())?;
        send(&pipeline, fresh.clone()).await?;
        api.request().await?;
        Ok::<_, String>(closed)
    })
    .await;
    let finished = finish_pipeline(
        pipeline,
        Some(Frame::end()),
        !matches!(&exercised, Ok(Ok(_))),
    )
    .await;
    let api_requests = api.finish().await;
    let downloads = download.finish().await;

    assert!(exercised.unwrap().unwrap());
    let (frames, report) = finished.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 1, "{report:#?}");
    assert!(report.errors[0].error.contains("queue is full"));
    assert!(!frames.iter().any(|event| event.frame.id == overflow.id));
    assert!(frames.contains(&DirectedFrame::new(fresh.clone(), Direction::Downstream)));
    let images = pixels.0.lock().unwrap();
    assert_eq!(images.len(), 1);
    assert_eq!(
        images[0].metadata["image_generation.source_frame_id"],
        json!(fresh.id)
    );
    let api_requests = api_requests.unwrap();
    assert_eq!(api_requests.len(), 3);
    let prompts: Vec<Value> = api_requests
        .iter()
        .filter(|request| request.method == "POST")
        .map(|request| serde_json::from_slice::<Value>(&request.body).unwrap()["prompt"].clone())
        .collect();
    assert_eq!(
        prompts,
        vec![json!("held submission"), json!("fresh after interruption")]
    );
    assert_eq!(downloads.unwrap().len(), 1);
}
