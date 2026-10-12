//! Independent loopback regressions for the pinned Speechmatics HTTP TTS contract.
#![cfg(feature = "speechmatics")]
use pipecat_core::*;
use pipecat_speech::{speechmatics::*, *};
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot},
    task::JoinHandle,
};

#[derive(Debug)]
struct Captured {
    head: String,
    body: Value,
}
impl Captured {
    fn url(&self) -> reqwest::Url {
        reqwest::Url::parse(&format!(
            "http://localhost{}",
            self.head
                .lines()
                .next()
                .unwrap()
                .split_whitespace()
                .nth(1)
                .unwrap()
        ))
        .unwrap()
    }
    fn header(&self, wanted: &str) -> Option<&str> {
        self.head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(key, value)| key.eq_ignore_ascii_case(wanted).then_some(value.trim()))
    }
}
async fn capture(socket: &mut TcpStream) -> Captured {
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut raw = Vec::new();
        let mut buf = [0u8; 1024];
        let end = loop {
            let n = socket.read(&mut buf).await.unwrap();
            assert_ne!(n, 0, "request closed before headers");
            raw.extend_from_slice(&buf[..n]);
            assert!(raw.len() < 1024 * 1024);
            if let Some(index) = raw.windows(4).position(|b| b == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let head = String::from_utf8(raw[..end].to_vec()).unwrap();
        let len = head
            .lines()
            .filter_map(|line| line.split_once(':'))
            .find_map(|(key, value)| {
                key.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().unwrap())
            })
            .unwrap();
        assert!(len < 1024 * 1024);
        while raw.len() < end + len {
            let n = socket.read(&mut buf).await.unwrap();
            assert_ne!(n, 0, "request closed before body");
            raw.extend_from_slice(&buf[..n]);
        }
        Captured {
            head,
            body: serde_json::from_slice(&raw[end..end + len]).unwrap(),
        }
    })
    .await
    .unwrap()
}
struct Reply {
    status: &'static str,
    mime: &'static str,
    extra_headers: String,
    chunks: Vec<Vec<u8>>,
    terminate: bool,
    hold: bool,
    gate: Option<oneshot::Receiver<()>>,
}
impl Reply {
    fn pcm(bytes: &[u8]) -> Self {
        Self {
            status: "200 OK",
            mime: "application/octet-stream",
            extra_headers: String::new(),
            chunks: bytes.iter().map(|v| vec![*v]).collect(),
            terminate: true,
            hold: false,
            gate: None,
        }
    }
    fn status(status: &'static str) -> Self {
        Self {
            status,
            ..Self::pcm(&[])
        }
    }
    fn stalled() -> Self {
        Self {
            hold: true,
            terminate: false,
            ..Self::pcm(&[])
        }
    }
}
struct Fixture {
    config: SpeechConfig,
    received: mpsc::UnboundedReceiver<Captured>,
    task: JoinHandle<()>,
}
impl Drop for Fixture {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Fixture {
    async fn request(&mut self) -> Captured {
        tokio::time::timeout(Duration::from_secs(3), self.received.recv())
            .await
            .unwrap()
            .unwrap()
    }
    async fn assert_no_request(&mut self) {
        assert!(
            tokio::time::timeout(Duration::from_millis(350), self.received.recv())
                .await
                .is_err()
        );
    }
}
async fn fixture(replies: Vec<Reply>) -> Fixture {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = SpeechConfig::openai("review-secret-do-not-disclose");
    config.base_url = format!("http://{}/proxy", listener.local_addr().unwrap());
    config.timeout = Duration::from_secs(3);
    config.queue_capacity = 2;
    let (tx, rx) = mpsc::unbounded_channel();
    let task = tokio::spawn(async move {
        let mut replies = replies.into_iter();
        loop {
            let (mut socket, _) = listener.accept().await.unwrap();
            let request = capture(&mut socket).await;
            tx.send(request).unwrap();
            let Some(mut reply) = replies.next() else {
                continue;
            };
            if let Some(gate) = reply.gate.take() {
                let _ = gate.await;
            }
            let head = format!("HTTP/1.1 {}\r\nContent-Type: {}\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n{}\r\n", reply.status, reply.mime, reply.extra_headers);
            if socket.write_all(head.as_bytes()).await.is_err() {
                continue;
            }
            for chunk in reply.chunks {
                if socket
                    .write_all(format!("{:x}\r\n", chunk.len()).as_bytes())
                    .await
                    .is_err()
                    || socket.write_all(&chunk).await.is_err()
                    || socket.write_all(b"\r\n").await.is_err()
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
            if reply.terminate {
                let _ = socket.write_all(b"0\r\n\r\n").await;
            }
            if reply.hold {
                let mut byte = [0];
                assert_eq!(
                    tokio::time::timeout(Duration::from_secs(3), socket.read(&mut byte))
                        .await
                        .unwrap()
                        .unwrap(),
                    0,
                    "client did not close retired socket"
                );
            }
            let _ = socket.shutdown().await;
        }
    });
    Fixture {
        config,
        received: rx,
        task,
    }
}
async fn collect(mut stream: SpeechStream) -> (Vec<u8>, Option<SpeechError>) {
    tokio::time::timeout(Duration::from_secs(4), async {
        let mut pcm = vec![];
        while let Some(result) = stream.next().await {
            match result {
                Ok(chunk) => {
                    assert_eq!(chunk.num_channels, 1);
                    assert_eq!(chunk.pcm.len() % 2, 0);
                    pcm.extend(chunk.pcm);
                }
                Err(error) => {
                    assert!(stream.next().await.is_none());
                    return (pcm, Some(error));
                }
            }
        }
        (pcm, None)
    })
    .await
    .unwrap()
}
fn synthesize(config: SpeechConfig, request: SpeechmaticsSynthesisRequest) -> SpeechStream {
    SpeechmaticsHttpClient::new(config)
        .unwrap()
        .synthesize(request)
        .unwrap()
}

#[tokio::test]
async fn review_exact_wire_fragmentation_and_nondefault_rate() {
    let mut f = fixture(vec![Reply::pcm(&[1, 2, 3, 4, 5, 6])]).await;
    let mut request = SpeechmaticsSynthesisRequest::new("你好 & \"speech\"");
    assert_eq!(request.voice, "sarah");
    assert_eq!(request.sample_rate, 16000);
    assert_eq!(request.max_retries, 5);
    request.voice = "voice /?#%中文".into();
    request.sample_rate = 22050;
    let mut stream = synthesize(f.config.clone(), request);
    let mut pcm = vec![];
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.unwrap();
        assert_eq!(chunk.sample_rate, 22050);
        assert_eq!(chunk.num_channels, 1);
        assert_eq!(chunk.pcm.len() % 2, 0);
        pcm.extend(chunk.pcm);
    }
    assert_eq!(pcm, [1, 2, 3, 4, 5, 6]);
    let request = f.request().await;
    assert!(request.head.starts_with("POST "));
    assert_eq!(
        request.header("authorization"),
        Some("Bearer review-secret-do-not-disclose")
    );
    assert_eq!(request.header("content-type"), Some("application/json"));
    assert_eq!(
        request.url().path(),
        "/proxy/generate/voice%20%2F%3F%23%25%E4%B8%AD%E6%96%87"
    );
    let query: BTreeMap<_, _> = request.url().query_pairs().into_owned().collect();
    assert_eq!(query.len(), 2);
    assert_eq!(query["output_format"], "pcm_22050");
    assert_eq!(query["sm-app"], SPEECHMATICS_APP);
    assert_eq!(request.body, json!({"text":"你好 & \"speech\""}));
    f.assert_no_request().await;
}

#[tokio::test]
async fn review_503_total_attempt_edges_and_successful_retry() {
    for (max_retries, expected) in [(0, 1), (1, 1), (2, 2)] {
        let mut f = fixture(
            (0..4)
                .map(|_| Reply::status("503 Service Unavailable"))
                .collect(),
        )
        .await;
        let mut request = SpeechmaticsSynthesisRequest::new("bounded");
        request.max_retries = max_retries;
        let (pcm, error) = collect(synthesize(f.config.clone(), request)).await;
        assert!(pcm.is_empty());
        assert!(matches!(error, Some(SpeechError::Http(503))), "{error:?}");
        for _ in 0..expected {
            assert_eq!(f.request().await.body, json!({"text":"bounded"}));
        }
        f.assert_no_request().await;
    }
    let mut f = fixture(vec![
        Reply {
            status: "503 Service Unavailable",
            ..Reply::stalled()
        },
        Reply::pcm(&[7, 8]),
    ])
    .await;
    let mut request = SpeechmaticsSynthesisRequest::new("retry");
    request.max_retries = 2;
    let (pcm, error) = collect(synthesize(f.config.clone(), request)).await;
    assert_eq!(pcm, [7, 8]);
    assert!(error.is_none(), "{error:?}");
    let first = f.request().await;
    let second = f.request().await;
    assert_eq!(first.body, second.body);
    assert_eq!(first.url(), second.url());
    f.assert_no_request().await;
}

#[tokio::test]
async fn review_deadline_includes_retry_backoff() {
    let mut f = fixture(vec![
        Reply::status("503 Service Unavailable"),
        Reply::pcm(&[7, 8]),
    ])
    .await;
    f.config.timeout = Duration::from_millis(90);
    let started = tokio::time::Instant::now();
    let (pcm, error) = collect(synthesize(
        f.config.clone(),
        SpeechmaticsSynthesisRequest::new("deadline"),
    ))
    .await;
    assert!(pcm.is_empty());
    assert!(matches!(error, Some(SpeechError::Timeout)), "{error:?}");
    assert!(started.elapsed() < Duration::from_secs(1));
    f.request().await;
    f.assert_no_request().await;
}

#[tokio::test]
async fn review_only_200_and_503_policy_errors_are_redacted() {
    for status in [
        "201 Created",
        "204 No Content",
        "301 Moved Permanently",
        "400 Bad Request",
        "429 Too Many Requests",
        "500 Internal Server Error",
    ] {
        let mut f = fixture(vec![Reply {
            status,
            chunks: vec![b"private-provider-body-do-not-disclose".to_vec()],
            extra_headers: "Location: http://127.0.0.1:9/private-redirect\r\n".into(),
            ..Reply::pcm(&[])
        }])
        .await;
        let (pcm, error) = collect(synthesize(
            f.config.clone(),
            SpeechmaticsSynthesisRequest::new("private-input"),
        ))
        .await;
        assert!(pcm.is_empty());
        let error = error.expect("non-200 must fail");
        assert!(matches!(error, SpeechError::Http(_)), "{error:?}");
        let rendered = format!("{error:?} {error}");
        for secret in [
            "private-provider",
            "private-input",
            "review-secret",
            "/proxy",
            "private-redirect",
        ] {
            assert!(!rendered.contains(secret));
        }
        f.request().await;
        f.assert_no_request().await;
    }
}

#[tokio::test]
async fn review_terminal_shape_and_body_bounds() {
    for (reply, limit, expected) in [
        (Reply::pcm(&[]), 8, "empty"),
        (Reply::pcm(&[1]), 8, "partial"),
        (Reply::pcm(&[1, 2, 3]), 8, "partial"),
        (Reply::pcm(&[1, 2, 3, 4]), 3, "large"),
        (
            Reply {
                mime: "audio/pcm;rate=24000;channels=1",
                ..Reply::pcm(&[1, 2])
            },
            8,
            "mime",
        ),
        (
            Reply {
                mime: "audio/pcm;rate=16000;channels=2",
                ..Reply::pcm(&[1, 2])
            },
            8,
            "mime",
        ),
        (
            Reply {
                mime: "application/json",
                ..Reply::pcm(&[1, 2])
            },
            8,
            "mime",
        ),
        (
            Reply {
                terminate: false,
                ..Reply::pcm(&[1, 2])
            },
            8,
            "transport",
        ),
    ] {
        let mut f = fixture(vec![reply]).await;
        f.config.max_audio_bytes = limit;
        let (_, error) = collect(synthesize(
            f.config.clone(),
            SpeechmaticsSynthesisRequest::new("shape"),
        ))
        .await;
        assert!(
            matches!(
                (expected, error.as_ref()),
                ("empty", Some(SpeechError::Protocol("empty PCM response")))
                    | ("partial", Some(SpeechError::PartialSample))
                    | ("large", Some(SpeechError::TooLarge))
                    | ("mime", Some(SpeechError::ContentType))
                    | ("transport", Some(SpeechError::Transport))
            ),
            "expected {expected}, got {error:?}"
        );
        f.request().await;
        f.assert_no_request().await;
    }
}

#[tokio::test]
async fn review_request_and_response_header_limits() {
    let mut f = fixture(vec![]).await;
    f.config.max_json_bytes = 8;
    assert!(matches!(
        SpeechmaticsHttpClient::new(f.config.clone())
            .unwrap()
            .synthesize(SpeechmaticsSynthesisRequest::new("too much")),
        Err(SpeechError::TooLarge)
    ));
    f.assert_no_request().await;
    for headers in [
        format!("x-large: {}\r\n", "a".repeat(17000)),
        (0..80)
            .map(|_| format!("x-repeat: {}\r\n", "b".repeat(220)))
            .collect::<String>(),
    ] {
        let mut f = fixture(vec![Reply {
            extra_headers: headers,
            ..Reply::pcm(&[1, 2])
        }])
        .await;
        let (pcm, error) = collect(synthesize(
            f.config.clone(),
            SpeechmaticsSynthesisRequest::new("header"),
        ))
        .await;
        assert!(pcm.is_empty());
        assert!(
            matches!(error, Some(SpeechError::TooLarge | SpeechError::Transport)),
            "{error:?}"
        );
        f.request().await;
        f.assert_no_request().await;
    }
}

#[tokio::test]
async fn review_cancel_and_drop_close_socket_and_cancel_discards_buffer() {
    for drop_stream in [false, true] {
        let mut f = fixture(vec![Reply::stalled(), Reply::pcm(&[9, 10])]).await;
        let mut stream = synthesize(
            f.config.clone(),
            SpeechmaticsSynthesisRequest::new("retire"),
        );
        f.request().await;
        if drop_stream {
            drop(stream);
        } else {
            stream.cancel();
            assert!(matches!(
                stream.next().await,
                Some(Err(SpeechError::Cancelled))
            ));
            assert!(stream.next().await.is_none());
        }
        let (pcm, error) = collect(synthesize(
            f.config.clone(),
            SpeechmaticsSynthesisRequest::new("fresh"),
        ))
        .await;
        assert_eq!(pcm, [9, 10]);
        assert!(error.is_none());
        assert_eq!(f.request().await.body, json!({"text":"fresh"}));
    }
    let mut f = fixture(vec![Reply::pcm(&[1, 2, 3, 4, 5, 6, 7, 8])]).await;
    let mut stream = synthesize(
        f.config.clone(),
        SpeechmaticsSynthesisRequest::new("buffer"),
    );
    f.request().await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    stream.cancel();
    assert!(matches!(
        stream.next().await,
        Some(Err(SpeechError::Cancelled))
    ));
    assert!(stream.next().await.is_none());
}

fn processor(config: SpeechConfig, retries: u8) -> Arc<HttpTtsProcessor> {
    let mut request = SpeechmaticsSynthesisRequest::new("");
    request.voice = "old-voice".into();
    request.max_retries = retries;
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechmatics {
                client: SpeechmaticsHttpClient::new(config).unwrap(),
                request,
            },
            8,
        )
        .unwrap(),
    )
}
async fn start(processor: Arc<HttpTtsProcessor>) -> PipelineWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = 16000;
    Pipeline::new(vec![processor]).start(config).await.unwrap()
}
async fn send(worker: &PipelineWorker, data: FrameData) {
    worker
        .sender()
        .send(Frame::new(data), Direction::Downstream)
        .await
        .unwrap();
}
async fn marker(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
    let mut text = TextFrame::new("review-admission-marker");
    text.skip_tts = Some(true);
    let marker = Frame::new(FrameData::Text(text));
    let id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        let mut out = vec![];
        loop {
            let frame = worker.recv().await.unwrap();
            let found = frame.frame.id == id;
            out.push(frame);
            if found {
                return out;
            }
        }
    })
    .await
    .unwrap()
}
async fn finished(worker: PipelineWorker) -> Vec<DirectedFrame> {
    let (frames, report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
    frames
}

#[tokio::test]
async fn review_settings_are_atomic_and_queued_requests_keep_snapshots() {
    let (release, gate) = oneshot::channel();
    let mut f = fixture(vec![
        Reply {
            gate: Some(gate),
            ..Reply::pcm(&[1, 2])
        },
        Reply::pcm(&[3, 4]),
        Reply::pcm(&[7, 8]),
        Reply::status("503 Service Unavailable"),
        Reply::pcm(&[5, 6]),
    ])
    .await;
    let p = processor(f.config.clone(), 1);
    let mut worker = start(p.clone()).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("first"))).await;
    let first = f.request().await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("queued"))).await;
    let mut frames = marker(&mut worker).await;
    let mut delta = TtsSettings {
        voice: Setting::Value("must-not-commit".into()),
        ..Default::default()
    };
    delta.base.extra.insert("max_retries".into(), json!(33));
    assert!(p.update_settings(&delta).is_err());
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("after-invalid")),
    )
    .await;
    frames.extend(marker(&mut worker).await);
    delta.voice = Setting::Value("new-voice".into());
    delta.base.extra.insert("max_retries".into(), json!(2));
    assert!(p.update_settings(&delta).unwrap());
    assert!(!p.update_settings(&delta).unwrap());
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("new"))).await;
    send(&worker, FrameData::LlmResponseEnd).await;
    let flush = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
        origin: Some("review".into()),
        ..Default::default()
    }));
    let flush_id = flush.id;
    worker
        .sender()
        .send(flush, Direction::Downstream)
        .await
        .unwrap();
    release.send(()).unwrap();
    worker.end().await.unwrap();
    frames.extend(finished(worker).await);
    let queued = f.request().await;
    let after_invalid = f.request().await;
    let fresh = f.request().await;
    let retry = f.request().await;
    assert_eq!(first.url().path(), "/proxy/generate/old-voice");
    assert_eq!(queued.url().path(), "/proxy/generate/old-voice");
    assert_eq!(after_invalid.url().path(), "/proxy/generate/old-voice");
    assert_eq!(after_invalid.body, json!({"text":"after-invalid"}));
    assert_eq!(fresh.url().path(), "/proxy/generate/new-voice");
    assert_eq!(fresh.body, json!({"text":"new"}));
    assert_eq!(fresh.url(), retry.url());
    assert_eq!(fresh.body, retry.body);
    let order: Vec<_> = frames
        .iter()
        .filter_map(|f| match f.frame.data {
            FrameData::TtsStarted(_) => Some("start"),
            FrameData::TtsAudio(_) => Some("audio"),
            FrameData::TtsStopped(_) => Some("stop"),
            FrameData::LlmResponseEnd => Some("response-end"),
            FrameData::End(_) => Some("end"),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        [
            "start",
            "audio",
            "stop",
            "start",
            "audio",
            "stop",
            "start",
            "audio",
            "stop",
            "start",
            "audio",
            "stop",
            "response-end",
            "end"
        ]
    );
    assert_eq!(frames.iter().filter(|f| f.frame.id == flush_id).count(), 1);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_) | FrameData::Error(_))));
    f.assert_no_request().await;
}

#[tokio::test]
async fn review_interruption_retires_active_and_queued_voice_authority() {
    let mut f = fixture(vec![Reply::stalled(), Reply::pcm(&[11, 12])]).await;
    let mut worker = start(processor(f.config.clone(), 2)).await;
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("old"))).await;
    f.request().await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("queued-must-not-run")),
    )
    .await;
    let _ = marker(&mut worker).await;
    send(&worker, FrameData::Interruption).await;
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if matches!(
                worker.recv().await.unwrap().frame.data,
                FrameData::Interruption
            ) {
                break;
            }
        }
    })
    .await
    .unwrap();
    send(&worker, FrameData::TtsSpeak(TtsSpeakFrame::new("fresh"))).await;
    worker.end().await.unwrap();
    let frames = finished(worker).await;
    assert_eq!(f.request().await.body, json!({"text":"fresh"}));
    let pcm: Vec<u8> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(pcm, [11, 12]);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    f.assert_no_request().await;
}

#[tokio::test]
async fn review_health_retirement_has_no_replay_after_restore() {
    let (release, gate) = oneshot::channel();
    let mut f = fixture(vec![
        Reply {
            gate: Some(gate),
            ..Reply::pcm(&[1, 2])
        },
        Reply::pcm(&[13, 14]),
    ])
    .await;
    let mut worker = start(processor(f.config.clone(), 2)).await;
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("old-health")),
    )
    .await;
    f.request().await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("queued-health-must-not-run")),
    )
    .await;
    let old_end = Frame::new(FrameData::LlmResponseEnd);
    let old_id = old_end.id;
    worker
        .sender()
        .send(old_end, Direction::Downstream)
        .await
        .unwrap();
    let mut frames = marker(&mut worker).await;
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    release.send(()).unwrap();
    send(&worker, FrameData::LlmResponseStart).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("fresh-health")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    worker.end().await.unwrap();
    frames.extend(finished(worker).await);
    assert_eq!(f.request().await.body, json!({"text":"fresh-health"}));
    assert!(health.is_usable());
    assert_eq!(
        frames
            .iter()
            .find(|f| f.frame.id == old_id)
            .unwrap()
            .frame
            .metadata
            .get("failed"),
        Some(&json!(true))
    );
    let pcm: Vec<u8> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(pcm, [13, 14]);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    f.assert_no_request().await;
}

#[tokio::test]
async fn review_pipeline_cancel_and_drop_release_http_owner() {
    for drop_worker in [false, true] {
        let mut f = fixture(vec![Reply::stalled(), Reply::pcm(&[15, 16])]).await;
        let worker = start(processor(f.config.clone(), 2)).await;
        send(
            &worker,
            FrameData::TtsSpeak(TtsSpeakFrame::new("cancel-owner")),
        )
        .await;
        f.request().await;
        if drop_worker {
            drop(worker);
        } else {
            worker.cancel("review-owner-cancel").await.unwrap();
            let frames = finished(worker).await;
            assert!(!frames
                .iter()
                .any(|f| matches!(f.frame.data, FrameData::TtsAudio(_) | FrameData::TtsText(_))));
        }
        let (pcm, error) = collect(synthesize(
            f.config.clone(),
            SpeechmaticsSynthesisRequest::new("independent-fresh"),
        ))
        .await;
        assert_eq!(pcm, [15, 16]);
        assert!(error.is_none());
        assert_eq!(f.request().await.body, json!({"text":"independent-fresh"}));
    }
}

#[tokio::test]
async fn review_stop_drains_successful_audio_and_response() {
    let mut f = fixture(vec![Reply::pcm(&[17, 18])]).await;
    let worker = start(processor(f.config.clone(), 1)).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("stop-drain")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    send(&worker, FrameData::Stop).await;
    let frames = finished(worker).await;
    assert_eq!(f.request().await.body, json!({"text":"stop-drain"}));
    let order: Vec<_> = frames
        .iter()
        .filter_map(|f| match f.frame.data {
            FrameData::TtsStarted(_) => Some("start"),
            FrameData::TtsAudio(_) => Some("audio"),
            FrameData::TtsStopped(_) => Some("stop"),
            FrameData::LlmResponseEnd => Some("response-end"),
            FrameData::Stop => Some("terminal"),
            _ => None,
        })
        .collect();
    assert_eq!(
        order,
        ["start", "audio", "stop", "response-end", "terminal"]
    );
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
}

#[tokio::test]
async fn review_invalid_settings_leave_health_disabled_and_do_not_recover() {
    let f = fixture(vec![]).await;
    let p = processor(f.config.clone(), 1);
    let health = p.health().unwrap();
    health.set_usable(false);
    for value in [
        Value::Null,
        json!(-1),
        json!(1.5),
        json!(true),
        json!("2"),
        json!(33),
        json!(256),
    ] {
        let mut delta = TtsSettings {
            voice: Setting::Value("invalid-combined-update".into()),
            ..Default::default()
        };
        delta.base.extra.insert("max_retries".into(), value);
        assert!(p.update_settings(&delta).is_err());
        assert!(!health.is_usable());
    }
    let mut same = TtsSettings {
        voice: Setting::Value("old-voice".into()),
        ..Default::default()
    };
    same.base.extra.insert("max_retries".into(), json!(1));
    assert!(!p.update_settings(&same).unwrap());
    assert!(!health.is_usable());
    same.voice = Setting::Value("valid-new-voice".into());
    assert!(p.update_settings(&same).unwrap());
    assert!(health.is_usable());
}

#[tokio::test]
async fn review_empty_and_partial_audio_cannot_report_synthesis_success() {
    for bytes in [vec![], vec![1], vec![1, 2, 3]] {
        let mut f = fixture(vec![Reply::pcm(&bytes)]).await;
        let worker = start(processor(f.config.clone(), 1)).await;
        send(&worker, FrameData::LlmResponseStart).await;
        send(
            &worker,
            FrameData::TtsSpeak(TtsSpeakFrame::new("must-not-succeed")),
        )
        .await;
        send(&worker, FrameData::LlmResponseEnd).await;
        worker.end().await.unwrap();
        let (frames, _report) = tokio::time::timeout(Duration::from_secs(4), worker.collect())
            .await
            .unwrap()
            .unwrap();
        f.request().await;
        assert!(frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::Error(_))));
        let stopped: Vec<_> = frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
            .collect();
        assert_eq!(stopped.len(), 1);
        assert_eq!(
            stopped[0].frame.metadata.get("failed"),
            Some(&json!(true)),
            "empty/partial audio reported successful TtsStopped"
        );
        let ended = frames
            .iter()
            .find(|f| matches!(f.frame.data, FrameData::LlmResponseEnd))
            .unwrap();
        assert_eq!(ended.frame.metadata.get("failed"), Some(&json!(true)));
        assert!(!frames.iter().any(|f|matches!(&f.frame.data,FrameData::TtsAlignmentDiagnostic(d) if d.synthesis_outcome==TtsSynthesisOutcome::Succeeded)),"empty/partial audio generated successful synthesis diagnostic");
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    }
}

#[tokio::test]
async fn review_active_retry_keeps_original_voice_and_retry_snapshot() {
    let (release, gate) = oneshot::channel();
    let mut f = fixture(vec![
        Reply {
            gate: Some(gate),
            ..Reply::status("503 Service Unavailable")
        },
        Reply::pcm(&[21, 22]),
        Reply::pcm(&[23, 24]),
    ])
    .await;
    let p = processor(f.config.clone(), 2);
    let worker = start(p.clone()).await;
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("old-retry-settings")),
    )
    .await;
    let first = f.request().await;
    let mut delta = TtsSettings {
        voice: Setting::Value("new-voice".into()),
        ..Default::default()
    };
    delta.base.extra.insert("max_retries".into(), json!(0));
    assert!(p.update_settings(&delta).unwrap());
    send(
        &worker,
        FrameData::TtsSpeak(TtsSpeakFrame::new("new-retry-settings")),
    )
    .await;
    send(&worker, FrameData::LlmResponseEnd).await;
    release.send(()).unwrap();
    worker.end().await.unwrap();
    let frames = finished(worker).await;
    let retry = f.request().await;
    let fresh = f.request().await;
    assert_eq!(first.url().path(), "/proxy/generate/old-voice");
    assert_eq!(first.url(), retry.url());
    assert_eq!(first.body, retry.body);
    assert_eq!(fresh.url().path(), "/proxy/generate/new-voice");
    assert_eq!(fresh.body, json!({"text":"new-retry-settings"}));
    let pcm: Vec<u8> = frames
        .iter()
        .filter_map(|f| match &f.frame.data {
            FrameData::TtsAudio(a) => Some(a.audio.clone()),
            _ => None,
        })
        .flatten()
        .collect();
    assert_eq!(pcm, [21, 22, 23, 24]);
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::TtsText(_))));
    f.assert_no_request().await;
}
