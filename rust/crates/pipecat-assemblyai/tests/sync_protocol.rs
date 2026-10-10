// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Protocol-exact, credential-free loopback tests. No external provider traffic.
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

#[tokio::test]
async fn exact_default_multipart_and_final_payload() {
    let (url, mut requests, handle) = server(vec![Reply::text("  Hello there  ")]).await;
    let mut service = AssemblyAiSyncStt::new(config(url)).unwrap();
    let transcript = service
        .transcribe_wav(&wav(), &token())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(transcript.text, "Hello there");
    assert_eq!(transcript.language.as_deref(), Some("en"));
    assert!(transcript.finalized);
    assert_eq!(
        transcript.result.unwrap(),
        json!({"text":"  Hello there  ","words":[]})
    );
    chrono::DateTime::parse_from_rfc3339(&transcript.timestamp).unwrap();
    let request = requests.recv().await.unwrap();
    assert_eq!(request.method, "POST");
    assert_eq!(request.path, "/v1/transcribe");
    assert_eq!(request.headers["authorization"], "test-key");
    assert_eq!(request.headers["x-aai-model"], SYNC_MODEL);
    let parts = multipart(&request);
    assert_eq!(parts.len(), 2);
    assert_eq!(parts["audio"].body, wav());
    assert!(parts["audio"].headers.contains("filename=\"audio.wav\""));
    assert!(parts["audio"]
        .headers
        .to_ascii_lowercase()
        .contains("content-type: audio/wav"));
    assert!(parts["config"]
        .headers
        .to_ascii_lowercase()
        .contains("content-type: application/json"));
    assert_eq!(request_config(&request), json!({"language_codes":["en"]}));
    handle.await.unwrap();
}
#[tokio::test]
async fn config_languages_prompt_keyterms_timestamps_and_precedence() {
    let (url, mut requests, handle) = server(vec![Reply::text("hello")]).await;
    let mut c = config(url);
    c.settings.language = Some("fr".into());
    c.settings.language_codes = Some(
        [
            "es-MX", "en-US", "es", "fr", "de", "it", "pt", "nl", "sv", "da", "fi", "hi", "ja",
        ]
        .map(String::from)
        .to_vec(),
    );
    c.settings.prompt = Some("Transcribe this call.".into());
    c.settings.keyterms_prompt = Some(vec!["Pipecat".into(), "AssemblyAI".into()]);
    c.settings.timestamps = Some(true);
    let mut service = AssemblyAiSyncStt::new(c).unwrap();
    service.transcribe_wav(&wav(), &token()).await.unwrap();
    let result = request_config(&requests.recv().await.unwrap());
    assert_eq!(
        result,
        json!({"language_codes":["es","en","fr","de","it","pt","nl","sv","da","fi","hi","ja"],"prompt":"Transcribe this call.","keyterms_prompt":["Pipecat","AssemblyAI"],"timestamps":true})
    );
    handle.await.unwrap();
}
#[tokio::test]
async fn no_config_part_when_all_optional_settings_absent() {
    let (url, mut requests, handle) = server(vec![Reply::text("")]).await;
    let mut c = config(url);
    c.settings.language = None;
    let mut service = AssemblyAiSyncStt::new(c).unwrap();
    assert!(service
        .transcribe_wav(&wav(), &token())
        .await
        .unwrap()
        .is_none());
    assert_eq!(multipart(&requests.recv().await.unwrap()).len(), 1);
    handle.await.unwrap();
}
#[test]
fn empty_language_list_falls_back_and_false_timestamps_are_sent() {
    let mut c = AssemblyAiSyncConfig::new("key");
    c.settings.language_codes = Some(vec![]);
    c.settings.language = Some("fr-CA".into());
    c.settings.timestamps = Some(false);
    assert_eq!(
        AssemblyAiSyncStt::new(c).unwrap().build_config().unwrap(),
        json!({"language_codes":["fr"],"timestamps":false})
    );
}
#[tokio::test]
async fn rolling_context_excludes_current_turn_preserves_assistant_and_resets() {
    let (url, mut requests, handle) = server(vec![
        Reply::text("first"),
        Reply::text("second"),
        Reply::text("third"),
    ])
    .await;
    let mut service = AssemblyAiSyncStt::new(config(url)).unwrap();
    service.transcribe_wav(&wav(), &token()).await.unwrap();
    assert!(request_config(&requests.recv().await.unwrap())
        .get("conversation_context")
        .is_none());
    service.append_assistant_turn("How can I help?");
    service.transcribe_wav(&wav(), &token()).await.unwrap();
    assert_eq!(
        request_config(&requests.recv().await.unwrap())["conversation_context"],
        json!(["first", "How can I help?"])
    );
    service.reset_context();
    service.transcribe_wav(&wav(), &token()).await.unwrap();
    assert!(request_config(&requests.recv().await.unwrap())
        .get("conversation_context")
        .is_none());
    handle.await.unwrap();
}
#[test]
fn strict_unicode_context_budget_eviction_and_disabled_modes() {
    let mut c = AssemblyAiSyncConfig::new("key");
    c.max_context_turns = 2;
    c.max_context_chars = 10;
    let mut service = AssemblyAiSyncStt::new(c.clone()).unwrap();
    for text in ["aaaaa", "bbbbb", "ccccc"] {
        service.append_assistant_turn(text);
    }
    assert_eq!(
        service.build_config().unwrap()["conversation_context"],
        json!(["bbbbb", "ccccc"])
    );
    service.append_assistant_turn("界".repeat(30).as_str());
    assert_eq!(
        service.build_config().unwrap()["conversation_context"],
        json!(["界".repeat(10)])
    );
    service.append_assistant_turn("  ");
    assert_eq!(service.context_turns().len(), 1);
    for (turns, chars) in [(0, 10), (2, 0)] {
        c.max_context_turns = turns;
        c.max_context_chars = chars;
        let mut s = AssemblyAiSyncStt::new(c.clone()).unwrap();
        s.append_assistant_turn("hello");
        assert!(s.context_turns().is_empty());
    }
}
#[tokio::test]
async fn manual_context_string_and_list_are_exact_and_disable_buffering() {
    for context in [
        ConversationContext::Text("Booking a flight.".into()),
        ConversationContext::Turns(vec!["Booking a flight.".into()]),
    ] {
        let (url, mut requests, handle) = server(vec![Reply::text("Lisbon")]).await;
        let mut c = config(url);
        c.settings.conversation_context = Some(context.clone());
        let mut service = AssemblyAiSyncStt::new(c).unwrap();
        service.append_assistant_turn("ignored");
        service.transcribe_wav(&wav(), &token()).await.unwrap();
        assert_eq!(
            request_config(&requests.recv().await.unwrap())["conversation_context"],
            serde_json::to_value(context).unwrap()
        );
        assert!(service.context_turns().is_empty());
        handle.await.unwrap();
    }
}
#[tokio::test]
async fn warming_has_model_no_auth_and_reuses_pool_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let warm = read_request(&mut socket).await;
        assert_eq!(warm.method, "GET");
        assert_eq!(warm.path, "/v1/warm");
        assert_eq!(warm.headers["x-aai-model"], SYNC_MODEL);
        assert!(!warm.headers.contains_key("authorization"));
        assert!(warm.body.is_empty());
        write_reply(&mut socket, Reply::json(500, json!({"ignored":true})), true).await;
        let transcription = tokio::time::timeout(Duration::from_secs(2), read_request(&mut socket))
            .await
            .unwrap();
        assert_eq!(transcription.path, "/v1/transcribe");
        assert_eq!(transcription.headers["authorization"], "test-key");
        write_reply(&mut socket, Reply::text("ok"), false).await;
    });
    let mut service = AssemblyAiSyncStt::new(config(url)).unwrap();
    service.warm(&token()).await;
    service.transcribe_wav(&wav(), &token()).await.unwrap();
    handle.await.unwrap();
}
#[tokio::test]
async fn status_classification_usable_policy_and_private_error_bodies() {
    for status in [400, 401, 403, 413, 429, 503] {
        let (url,mut requests,handle)=server(vec![Reply::json(status,json!({"error_code":"test-key","title":"secret transcript","detail":"private prompt","message":"sensitive"}))]).await;
        let mut service = AssemblyAiSyncStt::new(config(url)).unwrap();
        let error = service.transcribe_wav(&wav(), &token()).await.unwrap_err();
        assert_eq!(error, AssemblyAiSyncError::Http { status });
        assert_eq!(
            error.category(),
            pipecat_core::ErrorCategory::from_http_status(status)
        );
        assert_eq!(service.is_usable(), ![400, 401, 403].contains(&status));
        let display = format!("{error:?} {error}");
        for private in [
            "test-key",
            "secret transcript",
            "private prompt",
            "sensitive",
        ] {
            assert!(!display.contains(private));
        }
        assert!(service.context_turns().is_empty());
        requests.recv().await.unwrap();
        handle.await.unwrap();
    }
}
#[tokio::test]
async fn oversized_declared_and_chunked_response_and_invalid_json_are_bounded() {
    for chunked in [false, true] {
        let mut reply = Reply::text(&"x".repeat(300));
        reply.chunked = chunked;
        let (url, mut requests, handle) = server(vec![reply]).await;
        let mut c = config(url);
        c.max_response_bytes = 64;
        let mut service = AssemblyAiSyncStt::new(c).unwrap();
        assert_eq!(
            service.transcribe_wav(&wav(), &token()).await.unwrap_err(),
            AssemblyAiSyncError::ResponseTooLarge
        );
        requests.recv().await.unwrap();
        handle.await.unwrap();
        assert!(service.is_usable());
    }
    for body in [
        b"private broken JSON".to_vec(),
        b"[]".to_vec(),
        br#"{"text":4}"#.to_vec(),
    ] {
        let mut reply = Reply::text("");
        reply.body = body;
        let (url, mut requests, handle) = server(vec![reply]).await;
        let mut service = AssemblyAiSyncStt::new(config(url)).unwrap();
        assert_eq!(
            service.transcribe_wav(&wav(), &token()).await.unwrap_err(),
            AssemblyAiSyncError::Protocol
        );
        requests.recv().await.unwrap();
        handle.await.unwrap();
    }
}
#[tokio::test]
async fn cancellation_and_timeout_drop_http_without_context_or_retry() {
    for cancel in [false, true] {
        let mut reply = Reply::text("late");
        reply.delay = Duration::from_secs(1);
        let (url, mut requests, handle) = server(vec![reply]).await;
        let mut c = config(url);
        c.request_timeout = Duration::from_millis(100);
        let mut service = AssemblyAiSyncStt::new(c).unwrap();
        let cancellation = token();
        let cancelling = cancellation.clone();
        let cancel_task = tokio::spawn(async move {
            requests.recv().await.unwrap();
            if cancel {
                cancelling.cancel();
            }
        });
        let error = service
            .transcribe_wav(&wav(), &cancellation)
            .await
            .unwrap_err();
        assert_eq!(
            error,
            if cancel {
                AssemblyAiSyncError::Cancelled
            } else {
                AssemblyAiSyncError::Timeout
            }
        );
        assert!(service.context_turns().is_empty());
        assert!(service.is_usable());
        cancel_task.await.unwrap();
        handle.abort();
    }
    let mut service = AssemblyAiSyncStt::new(config("http://127.0.0.1:1".into())).unwrap();
    let cancellation = token();
    cancellation.cancel();
    assert_eq!(
        service
            .transcribe_wav(&wav(), &cancellation)
            .await
            .unwrap_err(),
        AssemblyAiSyncError::Cancelled
    );
}
#[tokio::test]
async fn redirects_never_forward_authentication() {
    let mut reply = Reply::json(307, json!({}));
    reply.extra = "Location: http://127.0.0.1:1/stolen\r\n".into();
    let (url, mut requests, handle) = server(vec![reply]).await;
    let mut service = AssemblyAiSyncStt::new(config(url)).unwrap();
    assert_eq!(
        service.transcribe_wav(&wav(), &token()).await.unwrap_err(),
        AssemblyAiSyncError::Http { status: 307 }
    );
    requests.recv().await.unwrap();
    handle.await.unwrap();
}
#[tokio::test]
async fn strict_pcm_wav_and_120_second_limit_reject_before_network() {
    let mut service = AssemblyAiSyncStt::new(config("http://127.0.0.1:1".into())).unwrap();
    for audio in [
        b"RIFF....WAVEfmt ".to_vec(),
        pcm_to_wav(&[], 16000, 1).unwrap(),
        pcm_to_wav(&vec![0; 16000 * 120 * 2 + 2], 16000, 1).unwrap(),
        [wav(), vec![0]].concat(),
    ] {
        assert!(matches!(
            service.transcribe_wav(&audio, &token()).await,
            Err(AssemblyAiSyncError::Invalid(_))
        ));
    }
    let bad = AudioRawFrame {
        audio: vec![0; 3],
        sample_rate: 16000,
        num_channels: 1,
        context_id: None,
    };
    assert!(matches!(
        service.transcribe_pcm(&bad, &token()).await,
        Err(AssemblyAiSyncError::Invalid(_))
    ));
}
#[test]
fn explicit_config_validation_and_debug_redaction() {
    for url in [
        "http://example.com",
        "http://localhost:80",
        "https://key@example.com",
        "https://example.com/?key=secret",
        "https://example.com/#secret",
        "file:///tmp/audio",
        "https://example.com/v1",
    ] {
        assert!(AssemblyAiSyncStt::new(config(url.into())).is_err());
    }
    let mut c = AssemblyAiSyncConfig::new("private-key");
    c.settings.prompt = Some("private-transcript".into());
    let debug = format!("{c:?}");
    assert!(!debug.contains("private-key"));
    assert!(!debug.contains("private-transcript"));
    c.settings.conversation_context = Some(ConversationContext::Text("x".repeat(1501)));
    assert!(AssemblyAiSyncStt::new(c).is_err());
    let mut c = AssemblyAiSyncConfig::new("key\r\nInjected: bad");
    assert!(AssemblyAiSyncStt::new(c.clone()).is_err());
    c = AssemblyAiSyncConfig::new("key");
    c.settings.model = "bad\r\n".into();
    assert!(AssemblyAiSyncStt::new(c).is_err());
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
#[tokio::test]
async fn pipeline_upstream_vad_assistant_context_preroll_metadata_and_identity() {
    let (url, mut requests, handle) = server(vec![Reply::text("recognised")]).await;
    let options = SyncProcessorOptions {
        enable_prewarming: false,
        pre_roll: Duration::from_millis(100),
        ..Default::default()
    };
    let processor = Arc::new(AssemblyAiSyncProcessor::new(config(url), options).unwrap());
    let mut worker = started(processor).await;
    observed(
        &mut worker,
        Frame::new(FrameData::LlmContextAssistantTurn(
            LlmContextAssistantTurnFrame {
                text: "What is your name?".into(),
                timestamp: "now".into(),
            },
        )),
        Direction::Upstream,
    )
    .await;
    observed(
        &mut worker,
        audio(vec![2; 4000], "speaker"),
        Direction::Downstream,
    )
    .await;
    observed(&mut worker, vad(true), Direction::Upstream).await;
    observed(
        &mut worker,
        audio(vec![3; 1000], "speaker"),
        Direction::Downstream,
    )
    .await;
    worker
        .sender()
        .send(vad(false), Direction::Upstream)
        .await
        .unwrap();
    loop {
        if let FrameData::Transcription(t) = next(&mut worker).await.frame.data {
            assert_eq!(t.text, "recognised");
            assert_eq!(t.user_id, "speaker");
            break;
        }
    }
    let request = requests.recv().await.unwrap();
    let parts = multipart(&request);
    let wav = read_pcm16_wav(&parts["audio"].body).unwrap();
    assert_eq!(
        wav.pcm,
        [vec![2; 3200], vec![3; 1000], vec![0; 16000]].concat()
    );
    assert_eq!(
        request_config(&request)["conversation_context"],
        json!(["What is your name?"])
    );
    end(&mut worker).await;
    handle.await.unwrap();
}
#[tokio::test]
async fn pipeline_graceful_end_flushes_active_speech_and_drains_before_terminal() {
    let (url, mut requests, handle) = server(vec![Reply::text("flushed")]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Downstream).await;
    observed(
        &mut worker,
        audio(vec![4; 800], "user"),
        Direction::Downstream,
    )
    .await;
    worker.end().await.unwrap();
    let mut transcript = false;
    let mut terminal = 0;
    while let Some(out) = tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
    {
        match out.frame.data {
            FrameData::Transcription(t) => {
                assert_eq!(t.text, "flushed");
                assert_eq!(terminal, 0);
                transcript = true;
            }
            FrameData::End(_) => {
                assert!(transcript);
                terminal += 1;
            }
            _ => {}
        }
    }
    assert_eq!(terminal, 1);
    worker.join().await.unwrap();
    assert_eq!(
        read_pcm16_wav(&multipart(&requests.recv().await.unwrap())["audio"].body)
            .unwrap()
            .pcm,
        [vec![4; 800], vec![0; 16000]].concat()
    );
    handle.await.unwrap();
}
#[tokio::test]
async fn pipeline_audioframes_are_ordered_and_survive_bot_interruption() {
    let mut reply = Reply::text("first");
    reply.delay = Duration::from_millis(40);
    let (url, mut requests, handle) = server(vec![reply, Reply::text("second")]).await;
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
        .send(audio(vec![1; 400], "u1"), Direction::Downstream)
        .await
        .unwrap();
    requests.recv().await.unwrap();
    observed(&mut worker, Frame::interruption(), Direction::Downstream).await;
    worker
        .sender()
        .send(audio(vec![2; 400], "u2"), Direction::Downstream)
        .await
        .unwrap();
    let mut texts = vec![];
    while texts.len() < 2 {
        if let FrameData::Transcription(t) = next(&mut worker).await.frame.data {
            texts.push((t.text, t.user_id));
        }
    }
    assert_eq!(
        texts,
        vec![
            ("first".into(), "u1".into()),
            ("second".into(), "u2".into())
        ]
    );
    assert_eq!(
        request_config(&requests.recv().await.unwrap())["conversation_context"],
        json!(["first"])
    );
    end(&mut worker).await;
    handle.await.unwrap();
}
#[tokio::test]
async fn pipeline_cancel_aborts_pending_http_and_emits_no_late_transcript() {
    let mut reply = Reply::text("late private result");
    reply.delay = Duration::from_secs(1);
    let (url, mut requests, handle) = server(vec![reply]).await;
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
        .send(audio(vec![0; 400], "u"), Direction::Downstream)
        .await
        .unwrap();
    requests.recv().await.unwrap();
    worker.cancel("test cancel").await.unwrap();
    while let Some(out) = tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
    {
        assert!(!matches!(out.frame.data, FrameData::Transcription(_)));
    }
    worker.join().await.unwrap();
    handle.abort();
}
#[tokio::test]
async fn pipeline_authentication_poisoning_and_recoverable_413_503() {
    for status in [401, 413, 503] {
        let (url, mut requests, handle) = server(vec![Reply::json(
            status,
            json!({"message":"do not expose me"}),
        )])
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
            .send(audio(vec![0; 400], "u"), Direction::Downstream)
            .await
            .unwrap();
        loop {
            if let FrameData::Error(error) = next(&mut worker).await.frame.data {
                assert_eq!(
                    error.category,
                    pipecat_core::ErrorCategory::from_http_status(status)
                );
                assert!(!error.error.contains("expose"));
                break;
            }
        }
        assert_eq!(
            worker.processor_health(0).unwrap().is_usable(),
            status != 401
        );
        requests.recv().await.unwrap();
        end(&mut worker).await;
        handle.await.unwrap();
    }
}
#[tokio::test]
async fn pipeline_health_change_fences_late_context_and_allows_new_audio() {
    let mut reply = Reply::text("stale");
    reply.delay = Duration::from_millis(200);
    let (url, mut requests, handle) = server(vec![reply, Reply::text("fresh")]).await;
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
    requests.recv().await.unwrap();
    let health = worker.processor_health(0).unwrap();
    health.set_usable(false);
    health.set_usable(true);
    worker
        .sender()
        .send(audio(vec![2; 400], "new"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        if let FrameData::Transcription(t) = next(&mut worker).await.frame.data {
            assert_eq!(t.text, "fresh");
            assert_eq!(t.user_id, "new");
            break;
        }
    }
    assert!(request_config(&requests.recv().await.unwrap())
        .get("conversation_context")
        .is_none());
    end(&mut worker).await;
    handle.await.unwrap();
}
#[tokio::test]
async fn pipeline_prewarm_is_scheduled_from_upstream_speech_and_cancel_is_bounded() {
    let mut reply = Reply::json(200, json!({"warm":true}));
    reply.delay = Duration::from_secs(1);
    let (url, mut requests, handle) = server(vec![reply]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(config(url), SyncProcessorOptions::default()).unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Upstream).await;
    let request = requests.recv().await.unwrap();
    assert_eq!(request.path, "/v1/warm");
    assert!(!request.headers.contains_key("authorization"));
    worker.cancel("cancel warm").await.unwrap();
    while tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .unwrap()
        .is_some()
    {}
    worker.join().await.unwrap();
    handle.abort();
}
#[tokio::test]
async fn pipeline_byte_backpressure_is_nonfatal_and_does_not_contact_provider() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                segmentation: SyncSegmentation::AudioFrames,
                max_pending_audio_bytes: 100,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    worker
        .sender()
        .send(audio(vec![0; 400], "u"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        if let FrameData::Error(error) = next(&mut worker).await.frame.data {
            assert!(error.error.contains("queue is full"));
            break;
        }
    }
    assert!(worker.processor_health(0).unwrap().is_usable());
    end(&mut worker).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}
#[test]
fn settings_updates_preserve_context_and_unlisted_base_languages() {
    let mut service = AssemblyAiSyncStt::new(AssemblyAiSyncConfig::new("key")).unwrap();
    service.append_assistant_turn("keep this");
    let mut settings = service.settings().clone();
    settings.language_codes = Some(vec!["pl-PL".into(), "pl".into()]);
    settings.timestamps = Some(true);
    service.update_settings(settings).unwrap();
    assert_eq!(
        service.build_config().unwrap(),
        json!({"language_codes":["pl"],"timestamps":true,"conversation_context":["keep this"]})
    );
}

#[tokio::test]
async fn pipeline_trailing_silence_is_configurable_and_counts_toward_segment_cap() {
    let (url, mut requests, handle) = server(vec![Reply::text("unpadded")]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                trailing_silence: Duration::ZERO,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Downstream).await;
    observed(&mut worker, audio(vec![4; 100], "u"), Direction::Downstream).await;
    worker
        .sender()
        .send(vad(false), Direction::Downstream)
        .await
        .unwrap();
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::Transcription(_)
        ) {
            break;
        }
    }
    assert_eq!(
        read_pcm16_wav(&multipart(&requests.recv().await.unwrap())["audio"].body)
            .unwrap()
            .pcm,
        vec![4; 100]
    );
    end(&mut worker).await;
    handle.await.unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", listener.local_addr().unwrap());
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = started(processor).await;
    observed(&mut worker, vad(true), Direction::Downstream).await;
    worker
        .sender()
        .send(audio(vec![0; 16_000 * 120 * 2], "u"), Direction::Downstream)
        .await
        .unwrap();
    loop {
        if let FrameData::Error(error) = next(&mut worker).await.frame.data {
            assert!(error.error.contains("120 seconds"));
            break;
        }
    }
    observed(&mut worker, vad(false), Direction::Downstream).await;
    end(&mut worker).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn pipeline_usage_counts_padding_before_provider_outcome_even_on_error() {
    let (url, mut requests, handle) = server(vec![Reply::json(503, json!({}))]).await;
    let processor = Arc::new(
        AssemblyAiSyncProcessor::new(
            config(url),
            SyncProcessorOptions {
                enable_prewarming: false,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut pipeline = PipelineConfig::default();
    pipeline.start.enable_usage_metrics = true;
    let mut worker = Pipeline::new(vec![processor])
        .start(pipeline)
        .await
        .unwrap();
    loop {
        if matches!(
            next(&mut worker).await.frame.data,
            FrameData::SttMetadata(_)
        ) {
            break;
        }
    }
    observed(&mut worker, vad(true), Direction::Downstream).await;
    observed(
        &mut worker,
        audio(vec![0; 3200], "u"),
        Direction::Downstream,
    )
    .await;
    worker
        .sender()
        .send(vad(false), Direction::Downstream)
        .await
        .unwrap();
    let mut usage = false;
    loop {
        match next(&mut worker).await.frame.data {
            FrameData::Metrics(metrics) => {
                for metric in metrics {
                    if let pipecat_core::Metric::SttUsage {
                        processor,
                        audio_seconds,
                    } = metric
                    {
                        assert_eq!(processor, "AssemblyAISyncSTTService");
                        assert!((audio_seconds - 0.6).abs() < 1e-9);
                        assert!(!usage);
                        usage = true;
                    }
                }
            }
            FrameData::Error(_) => {
                assert!(usage);
                break;
            }
            _ => {}
        }
    }
    requests.recv().await.unwrap();
    end(&mut worker).await;
    handle.await.unwrap();
}
