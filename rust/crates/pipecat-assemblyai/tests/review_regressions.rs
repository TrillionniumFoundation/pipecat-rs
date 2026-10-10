// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Independent review regressions on a frozen implementation snapshot.
//! Helpers copied from the authored protocol tests; regression assertions independently authored.
#![allow(dead_code, unused_imports)]
use pipecat_assemblyai::sync::*;
use pipecat_audio::{pcm_to_wav, read_pcm16_wav};
use pipecat_core::{
    AudioRawFrame, Direction, Frame, FrameData, LlmContextAssistantTurnFrame, Pipeline,
    PipelineConfig, UserAudioRawFrame, VadSpeakingFrame,
};
use pipecat_services::CancellationToken;
use serde_json::{json, Value};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
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
    fn text(text: &str) -> Self {
        Self::json(200, json!({"text":text,"words":[]}))
    }
}
fn find(bytes: &[u8], needle: &[u8]) -> usize {
    bytes
        .windows(needle.len())
        .position(|v| v == needle)
        .unwrap()
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
            sender.send(request).unwrap();
            write_reply(&mut socket, reply, false).await;
        }
    });
    (url, receiver, handle)
}
fn config(url: String) -> AssemblyAiSyncConfig {
    let mut c = AssemblyAiSyncConfig::new("test-key");
    c.base_url = url;
    c.request_timeout = Duration::from_secs(2);
    c
}
fn wav() -> Vec<u8> {
    pcm_to_wav(&[1, 0, 2, 0, 255, 255, 0, 0], 16_000, 1).unwrap()
}
fn token() -> CancellationToken {
    CancellationToken::new()
}
#[derive(Debug)]
struct Part {
    headers: String,
    body: Vec<u8>,
}
fn multipart(request: &Request) -> BTreeMap<String, Part> {
    let content_type = &request.headers["content-type"];
    assert!(content_type.starts_with("multipart/form-data; boundary="));
    let boundary = content_type
        .split_once("boundary=")
        .unwrap()
        .1
        .trim_matches('"');
    let prefix = format!("--{boundary}\r\n");
    let delimiter = format!("\r\n--{boundary}");
    let mut data = request.body.as_slice();
    let mut parts = BTreeMap::new();
    loop {
        assert!(data.starts_with(prefix.as_bytes()));
        data = &data[prefix.len()..];
        let index = find(data, b"\r\n\r\n");
        let headers = std::str::from_utf8(&data[..index]).unwrap().to_string();
        data = &data[index + 4..];
        let end = find(data, delimiter.as_bytes());
        let body = data[..end].to_vec();
        data = &data[end + 2..];
        let name = headers
            .split("name=\"")
            .nth(1)
            .unwrap()
            .split('"')
            .next()
            .unwrap()
            .to_string();
        assert!(parts.insert(name, Part { headers, body }).is_none());
        if data == format!("--{boundary}--\r\n").as_bytes() {
            break;
        }
    }
    parts
}
fn request_config(request: &Request) -> Value {
    serde_json::from_slice(&multipart(request)["config"].body).unwrap()
}

async fn next(worker: &mut pipecat_core::PipelineWorker) -> pipecat_core::DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}
async fn observed(worker: &mut pipecat_core::PipelineWorker, frame: Frame, direction: Direction) {
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    loop {
        if next(worker).await.frame.id == id {
            break;
        }
    }
}
async fn started(processor: Arc<AssemblyAiSyncProcessor>) -> pipecat_core::PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    loop {
        if let FrameData::SttMetadata(metadata) = next(&mut worker).await.frame.data {
            assert_eq!(metadata.base.service_name, "AssemblyAISyncSTTService");
            assert_eq!(metadata.ttfs_p99_latency, SYNC_TTFS_P99);
            break;
        }
    }
    worker
}
fn audio(bytes: Vec<u8>, user_id: &str) -> Frame {
    Frame::new(FrameData::UserAudio(UserAudioRawFrame {
        audio: AudioRawFrame::new(bytes, 16_000, 1).unwrap(),
        user_id: user_id.into(),
    }))
}
fn vad(start: bool) -> Frame {
    let vad = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    Frame::new(if start {
        FrameData::VadUserStartedSpeaking(vad)
    } else {
        FrameData::VadUserStoppedSpeaking(vad)
    })
}
async fn end(worker: &mut pipecat_core::PipelineWorker) {
    worker.end().await.unwrap();
    let mut count = 0;
    while let Some(out) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        if matches!(out.frame.data, FrameData::End(_)) {
            count += 1;
        }
    }
    assert_eq!(count, 1);
    worker.join().await.unwrap();
}

#[test]
fn review_streaming_numeric_region_codes_are_supported() {
    let result =
        pipecat_assemblyai::prepare_language_codes(&["es-419".into(), "ar-001".into()], Some(10));
    assert_eq!(result, Ok(vec!["es".into(), "ar".into()]));
}
#[test]
fn review_sync_numeric_region_codes_are_supported() {
    let mut c = AssemblyAiSyncConfig::new("synthetic-key");
    c.settings.language_codes = Some(vec!["es-419".into(), "ar-001".into()]);
    let service =
        AssemblyAiSyncStt::new(c).expect("pinned Language enum numeric regions must be accepted");
    assert_eq!(
        service.build_config().unwrap()["language_codes"],
        json!(["es", "ar"])
    );
}
#[test]
fn review_streaming_empty_language_base_is_rejected() {
    assert!(pipecat_assemblyai::prepare_language_codes(&["-US".into()], Some(10)).is_err());
}
#[tokio::test]
async fn review_idle_health_recovery_resets_context() {
    let (url, mut requests, handle) = server(vec![
        Reply::text("prior conversation"),
        Reply::text("new conversation"),
    ])
    .await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                segmentation: SyncSegmentation::AudioFrames,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    worker
        .sender()
        .send(audio(vec![1; 400], "old"), Direction::Downstream)
        .await
        .unwrap();
    let first = requests.recv().await.unwrap();
    assert!(request_config(&first).get("conversation_context").is_none());
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::Transcription(_)
        ) {
            break;
        }
    }
    // Deliberately transition while the worker is idle, after it committed context.
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    worker
        .sender()
        .send(audio(vec![2; 400], "new"), Direction::Downstream)
        .await
        .unwrap();
    let second = requests.recv().await.unwrap();
    let config = request_config(&second);
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::Transcription(_)
        ) {
            break;
        }
    }
    end(&mut worker).await;
    handle.await.unwrap();
    assert!(
        config.get("conversation_context").is_none(),
        "old context crossed recovery: {config}"
    );
}
#[tokio::test]
async fn review_local_settings_target_does_not_mutate_other_instance() {
    use pipecat_core::{LocalProcessorReference, ProcessorReference, ServiceUpdateSettingsFrame};
    let (url, mut requests, handle) =
        server(vec![Reply::text("first"), Reply::text("second")]).await;
    let options = SyncProcessorOptions {
        enable_prewarming: false,
        segmentation: SyncSegmentation::AudioFrames,
        ..Default::default()
    };
    let first =
        Arc::new(AssemblyAiSyncProcessor::new(config(url.clone()), options.clone()).unwrap());
    let second = Arc::new(AssemblyAiSyncProcessor::new(config(url), options).unwrap());
    let mut a = started(first).await;
    let mut b = started(second.clone()).await;
    let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            settings: [("prompt".into(), json!("only second should receive this"))]
                .into_iter()
                .collect(),
            service: Some(ProcessorReference::Local(LocalProcessorReference(second))),
            ..Default::default()
        },
    )));
    observed(&mut a, update.clone(), Direction::Downstream).await;
    b.sender()
        .send(update, Direction::Downstream)
        .await
        .unwrap();
    observed(
        &mut b,
        Frame::text("settings barrier"),
        Direction::Downstream,
    )
    .await;
    a.sender()
        .send(audio(vec![1; 400], "a"), Direction::Downstream)
        .await
        .unwrap();
    let first_config = request_config(&requests.recv().await.unwrap());
    loop {
        if matches!(next(&mut a).await.frame.data, FrameData::Transcription(_)) {
            break;
        }
    }
    b.sender()
        .send(audio(vec![2; 400], "b"), Direction::Downstream)
        .await
        .unwrap();
    let second_config = request_config(&requests.recv().await.unwrap());
    loop {
        if matches!(next(&mut b).await.frame.data, FrameData::Transcription(_)) {
            break;
        }
    }
    end(&mut a).await;
    end(&mut b).await;
    handle.await.unwrap();
    assert_eq!(second_config["prompt"], "only second should receive this");
    assert!(
        first_config.get("prompt").is_none(),
        "other instance got targeted prompt: {first_config}"
    );
}

#[tokio::test]
async fn review_settings_delta_takes_precedence_over_legacy_map() {
    use pipecat_core::{ServiceSettings, ServiceUpdateSettingsFrame, SttSettings};
    let (url, mut requests, handle) = server(vec![Reply::text("test")]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                segmentation: SyncSegmentation::AudioFrames,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            settings: [("prompt".into(), json!("stale legacy prompt"))]
                .into_iter()
                .collect(),
            delta: Some(SttSettings {
                base: ServiceSettings {
                    extra: [("timestamps".into(), json!(true))].into_iter().collect(),
                    ..Default::default()
                },
                ..Default::default()
            }),
            ..Default::default()
        },
    )));
    worker
        .sender()
        .send(update, Direction::Downstream)
        .await
        .unwrap();
    observed(
        &mut worker,
        Frame::text("settings barrier"),
        Direction::Downstream,
    )
    .await;
    worker
        .sender()
        .send(audio(vec![1; 400], "u"), Direction::Downstream)
        .await
        .unwrap();
    let config = request_config(&requests.recv().await.unwrap());
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::Transcription(_)
        ) {
            break;
        }
    }
    end(&mut worker).await;
    handle.await.unwrap();
    assert_eq!(config["timestamps"], true);
    assert!(
        config.get("prompt").is_none(),
        "legacy map merged despite typed delta: {config}"
    );
}
#[tokio::test]
async fn review_handled_settings_frame_is_consumed() {
    use pipecat_core::ServiceUpdateSettingsFrame;
    let (url, mut requests, handle) = server(vec![Reply::text("test")]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                segmentation: SyncSegmentation::AudioFrames,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    let update = Frame::new(FrameData::SttUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            settings: [("timestamps".into(), json!(true))].into_iter().collect(),
            ..Default::default()
        },
    )));
    let id = update.id;
    worker
        .sender()
        .send(update, Direction::Downstream)
        .await
        .unwrap();
    let barrier = Frame::text("settings barrier");
    let barrier_id = barrier.id;
    worker
        .sender()
        .send(barrier, Direction::Downstream)
        .await
        .unwrap();
    let mut leaked = false;
    loop {
        let out = next(&mut worker).await;
        leaked |= out.frame.id == id;
        if out.frame.id == barrier_id {
            break;
        }
    }
    worker
        .sender()
        .send(audio(vec![1; 400], "u"), Direction::Downstream)
        .await
        .unwrap();
    let config = request_config(&requests.recv().await.unwrap());
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::Transcription(_)
        ) {
            break;
        }
    }
    end(&mut worker).await;
    handle.await.unwrap();
    assert_eq!(config["timestamps"], true);
    assert!(
        !leaked,
        "handled targeted settings frame escaped to downstream"
    );
}
#[test]
fn review_unrepresentable_streaming_queue_is_rejected() {
    let mut config = pipecat_assemblyai::AssemblyAiConfig::new("synthetic-key");
    config.limits.queue_capacity = usize::MAX;
    assert!(
        pipecat_assemblyai::AssemblyAiClient::new(config).is_err(),
        "channel capacity must be rejected before connect can panic in tokio semaphore"
    );
}
#[tokio::test]
async fn review_graceful_end_waits_for_capacity_to_flush_buffered_speech() {
    let mut first = Reply::text("one");
    first.delay = Duration::from_millis(200);
    let (url, mut requests, handle) =
        server(vec![first, Reply::text("two"), Reply::text("three")]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                queue_capacity: 1,
                trailing_silence: Duration::ZERO,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![1; 400], "u"), Direction::Downstream).await;
    observed(&mut worker, vad(false), Direction::Upstream).await;
    requests.recv().await.unwrap(); // First HTTP has started and is now delayed.
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![2; 400], "u"), Direction::Downstream).await;
    observed(&mut worker, vad(false), Direction::Upstream).await; // Second segment fills queue.
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![3; 400], "u"), Direction::Downstream).await;
    // Third utterance is still buffered. End must drain then enqueue its flush,
    // rather than drop accepted speech just because the ordinary queue is full.
    end(&mut worker).await;
    let mut remaining = Vec::new();
    while let Ok(request) = requests.try_recv() {
        remaining.push(request);
    }
    if handle.is_finished() {
        handle.await.unwrap();
    } else {
        handle.abort();
        let _ = handle.await;
    }
    assert_eq!(
        remaining.len(),
        2,
        "End lost the buffered third utterance while its queue was full"
    );
}
#[tokio::test]
async fn review_graceful_end_waits_for_audio_byte_budget() {
    let mut first = Reply::text("one");
    first.delay = Duration::from_millis(200);
    let (url, mut requests, handle) = server(vec![first, Reply::text("two")]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                queue_capacity: 4,
                trailing_silence: Duration::ZERO,
                max_pending_audio_bytes: 400,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![1; 400], "u"), Direction::Downstream).await;
    observed(&mut worker, vad(false), Direction::Upstream).await;
    requests.recv().await.unwrap();
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![2; 400], "u"), Direction::Downstream).await;
    end(&mut worker).await;
    let second = requests
        .try_recv()
        .expect("End must wait for the in-flight byte budget to free");
    assert_eq!(
        read_pcm16_wav(&multipart(&second)["audio"].body)
            .unwrap()
            .pcm,
        vec![2; 400]
    );
    handle.await.unwrap();
}
#[tokio::test]
async fn review_cancel_aborts_end_while_waiting_for_byte_budget() {
    let mut first = Reply::text("late");
    first.delay = Duration::from_secs(2);
    let (url, mut requests, handle) = server(vec![first]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                queue_capacity: 4,
                trailing_silence: Duration::ZERO,
                max_pending_audio_bytes: 400,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![1; 400], "u"), Direction::Downstream).await;
    observed(&mut worker, vad(false), Direction::Upstream).await;
    requests.recv().await.unwrap();
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(&mut worker, audio(vec![2; 400], "u"), Direction::Downstream).await;
    worker.end().await.unwrap();
    tokio::time::sleep(Duration::from_millis(20)).await;
    worker.cancel("review cancel during End").await.unwrap();
    let mut transcriptions = 0;
    let mut ends = 0;
    tokio::time::timeout(Duration::from_millis(500), async {
        while let Some(out) = worker.recv().await {
            transcriptions += usize::from(matches!(out.frame.data, FrameData::Transcription(_)));
            ends += usize::from(matches!(out.frame.data, FrameData::End(_)));
        }
        worker.join().await.unwrap();
    })
    .await
    .expect("cancel must bypass full byte budget without waiting for HTTP");
    handle.abort();
    let _ = handle.await;
    assert_eq!(transcriptions, 0);
    assert_eq!(ends, 0);
    assert!(requests.try_recv().is_err());
}
