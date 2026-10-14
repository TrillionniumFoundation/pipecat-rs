#![cfg(feature = "speechify")]
//! Real loopback HTTP/SSE through the owned native processor. PCM and server
//! responses are synthetic: these checks do not attest acoustic playback.
use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::FutureExt;
use pipecat::{
    core::*,
    speech::{speechify::*, HttpTtsBackend, HttpTtsProcessor, SpeechConfig},
};
use serde_json::{json, Value};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
};

const DEADLINE: Duration = Duration::from_secs(5);
const BASE: u64 = 100_000_000;
struct Clock;
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        BASE
    }
}

// Keep every owner outside timeout/catch_unwind. Even a failed assertion during
// admission must cancel and join the pipeline, then reap the HTTP server.
struct OwnedWorker {
    worker: PipelineWorker,
    joined: bool,
}
impl Deref for OwnedWorker {
    type Target = PipelineWorker;
    fn deref(&self) -> &Self::Target {
        &self.worker
    }
}
impl DerefMut for OwnedWorker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.worker
    }
}
impl OwnedWorker {
    async fn collect(&mut self) -> Result<(Vec<DirectedFrame>, WorkerReport), PipelineError> {
        let mut frames = vec![];
        while let Some(frame) = self.worker.recv().await {
            frames.push(frame);
        }
        let report = self.worker.join().await;
        // join consumes its handle on both success and terminal worker error.
        self.joined = true;
        report.map(|report| (frames, report))
    }
}
#[derive(Default)]
struct TestOwners {
    workers: Vec<OwnedWorker>,
    servers: Vec<Option<tokio::task::JoinHandle<()>>>,
}
impl TestOwners {
    fn spawn(&mut self, future: impl Future<Output = ()> + Send + 'static) -> usize {
        let index = self.servers.len();
        self.servers.push(Some(tokio::spawn(future)));
        index
    }
    async fn join_server(&mut self, index: usize) {
        let result = tokio::time::timeout(DEADLINE, self.servers[index].as_mut().unwrap()).await;
        match result {
            Ok(result) => {
                self.servers[index].take();
                result.unwrap();
            }
            Err(_) => {
                self.servers[index].as_ref().unwrap().abort();
                let _ = self.servers[index].as_mut().unwrap().await;
                self.servers[index].take();
                panic!("HTTP server did not finish before deadline; aborted and joined");
            }
        }
    }
    async fn cleanup(&mut self) -> Vec<String> {
        let mut errors = vec![];
        for owned in &mut self.workers {
            if owned.joined {
                continue;
            }
            if !owned.worker.has_finished() {
                let _ = tokio::time::timeout(DEADLINE, owned.worker.cancel("test cleanup")).await;
            }
            match tokio::time::timeout(DEADLINE, owned.collect()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => errors.push(format!("pipeline join failed: {error:?}")),
                Err(_) => errors.push("pipeline did not join after cancellation".into()),
            }
        }
        for server in &mut self.servers {
            if let Some(mut task) = server.take() {
                match tokio::time::timeout(DEADLINE, &mut task).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(format!("HTTP server failed: {error:?}")),
                    Err(_) => {
                        task.abort();
                        let _ = task.await;
                        errors.push("HTTP server timed out; aborted and joined".into());
                    }
                }
            }
        }
        errors
    }
}
async fn with_owners(
    run: impl for<'a> FnOnce(&'a mut TestOwners) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
) {
    let mut owners = TestOwners::default();
    let result = tokio::time::timeout(
        DEADLINE * 4,
        AssertUnwindSafe(run(&mut owners)).catch_unwind(),
    )
    .await;
    let cleanup_errors = owners.cleanup().await;
    // Check cleanup before resuming the original test failure: do not silently
    // claim that a failed cancellation or unreaped worker was successful.
    assert!(cleanup_errors.is_empty(), "{cleanup_errors:?}");
    match result {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("test body deadline elapsed; owners cleaned up"),
    }
}

async fn request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 1_048_576);
        if let Some(index) = bytes.windows(4).position(|v| v == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let header = String::from_utf8(bytes[..end].to_vec()).unwrap();
    let length = header
        .lines()
        .filter_map(|line| line.split_once(':'))
        .find_map(|(key, value)| {
            key.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().unwrap())
        })
        .unwrap();
    while bytes.len() < end + length {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0);
        bytes.extend_from_slice(&buffer[..count]);
    }
    (
        header,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
fn event(name: &str, value: Value) -> String {
    format!("event: {name}\r\ndata: {value}\r\n\r\n")
}
fn mark(start: usize, end: usize, time: f64, until: f64, value: &str) -> Value {
    json!({"start":start,"end":end,"start_time":time,"end_time":until,"value":value})
}
fn simple_body(text: &str, duration_ms: f64) -> String {
    event(
        "speech.chunk",
        json!({"audio": STANDARD.encode([1, 0]), "speech_marks":[mark(0,text.chars().count(),0.0,100.0,text)]}),
    ) + &event("speech.done", json!({"audio_duration_ms":duration_ms}))
}
async fn headers(socket: &mut TcpStream) {
    socket.write_all(b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await.unwrap();
}
async fn chunk(socket: &mut TcpStream, bytes: &[u8]) {
    socket
        .write_all(format!("{:x}\r\n", bytes.len()).as_bytes())
        .await
        .unwrap();
    socket.write_all(bytes).await.unwrap();
    socket.write_all(b"\r\n").await.unwrap();
}
async fn reply(socket: &mut TcpStream, body: &str, truncated: bool) {
    headers(socket).await;
    // Deliberately split UTF-8, JSON, base64 and CRLF over real HTTP chunks.
    for part in body.as_bytes().chunks(1) {
        chunk(socket, part).await;
    }
    if !truncated {
        socket.write_all(b"0\r\n\r\n").await.unwrap();
    }
}
fn processor(listener: &TcpListener, rate: u32) -> Arc<HttpTtsProcessor> {
    let mut config = SpeechConfig::speechify("synthetic-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = DEADLINE;
    let mut request = SpeechifySynthesisRequest::new("");
    request.sample_rate = rate;
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::Speechify {
                client: SpeechifyHttpClient::new(config).unwrap(),
                request,
            },
            8,
        )
        .unwrap(),
    )
}
async fn pipeline(
    owners: &mut TestOwners,
    processor: Arc<HttpTtsProcessor>,
    rate: u32,
) -> &mut OwnedWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = rate;
    let worker = Pipeline::new(vec![processor])
        .with_observer_clock(Arc::new(Clock))
        .start(config)
        .await
        .unwrap();
    owners.workers.push(OwnedWorker {
        worker,
        joined: false,
    });
    owners.workers.last_mut().unwrap()
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn finish(worker: &mut OwnedWorker) -> Vec<Frame> {
    worker.sender().end().await.unwrap();
    let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !report.errors.iter().any(|error| error.fatal),
        "{:?}",
        report.errors
    );
    frames.into_iter().map(|output| output.frame).collect()
}
fn input(text: &str, original: &str, destination: &str, append: bool) -> Frame {
    let mut input = TtsTextInputFrame::new(text);
    input.original_text = original.into();
    input.append_to_context = append;
    input.aggregated_by = "paragraph".into();
    input.includes_inter_frame_spaces = true;
    let mut frame = Frame::new(FrameData::TtsTextInput(input));
    frame.transport_destination = Some(destination.into());
    frame
}

#[tokio::test]
async fn typed_sse_preserves_unicode_word_assembly_pcm_provenance_and_continuous_duration() {
    with_owners(|owners| Box::pin(async move {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let tts = processor(&listener, 24000);
    let server = owners.spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let (header, body) = request(&mut first).await;
        assert!(header.starts_with("POST /v1/audio/stream/with-timestamps HTTP/1.1"));
        assert!(header
            .to_ascii_lowercase()
            .contains("speechify-version: 2026-09-30"));
        assert!(header
            .to_ascii_lowercase()
            .contains("authorization: bearer synthetic-key"));
        assert_eq!(body["input"], "😀 you’d text-to-speech");
        assert_eq!(body["output_format"], "pcm_24000");
        let body = event("speech.chunk", json!({"audio":STANDARD.encode([1, 0]), "note":"😀"}))
            + &event(
                "speech.chunk",
                json!({"audio":STANDARD.encode([2, 0]), "speech_marks":[mark(2,7,0.0,100.0,"you'd"),mark(8,12,200.0,300.0,"text")]}),
            )
            + &event(
                "speech.chunk",
                json!({"speech_marks":[mark(12,13,300.0,400.0,"-"),mark(13,15,400.0,500.0,"to")]}),
            )
            + &event(
                "speech.chunk",
                json!({"speech_marks":[mark(15,16,500.0,600.0,"-")]}),
            )
            + &event("speech.done", json!({"audio_duration_ms":1500}))
            + &event(
                "speech.chunk",
                json!({"speech_marks":[mark(16,22,600.0,900.0,"speech")]}),
            );
        reply(&mut first, &body, false).await;
        let (mut second, _) = listener.accept().await.unwrap();
        assert_eq!(request(&mut second).await.1["input"], "next");
        reply(&mut second, &simple_body("next", 500.0), false).await;
    });
    let worker = pipeline(owners, tts, 24000).await;
    let first = input(
        "😀 you’d text-to-speech",
        "😀 you’d <b>text-to-speech</b>",
        "left",
        false,
    );
    let first_id = first.id;
    let second = input("next", "**next**", "right", true);
    let second_id = second.id;
    send(worker, first).await;
    send(worker, second).await;
    send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
    let frames = finish(worker).await;
    owners.join_server(server).await;
    assert!(!frames
        .iter()
        .any(|f| matches!(f.data, FrameData::Error(_) | FrameData::BotStartedSpeaking)));
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsText(t) => Some((f, t)),
            _ => None,
        })
        .collect();
    assert_eq!(
        words
            .iter()
            .map(|(_, t)| t.text.text.as_str())
            .collect::<Vec<_>>(),
        ["you’d", "text-to-speech", "next"]
    );
    assert_eq!(
        words
            .iter()
            .map(|(f, _)| f.pts.unwrap())
            .collect::<Vec<_>>(),
        [BASE, BASE + 200_000_000, BASE + 1_500_000_001]
    );
    let stops: Vec<_> = frames
        .iter()
        .filter(|f| matches!(f.data, FrameData::TtsStopped(_)))
        .collect();
    assert_eq!(
        stops.iter().map(|f| f.pts.unwrap()).collect::<Vec<_>>(),
        [BASE + 1_500_000_001, BASE + 2_000_000_002]
    );
    let anchors: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::AggregatedText(t) => Some((f, t)),
            _ => None,
        })
        .collect();
    assert_eq!(anchors.len(), 2);
    assert_eq!(
        anchors[0].1.raw_text.as_deref(),
        Some("😀 you’d <b>text-to-speech</b>")
    );
    assert_eq!(anchors[1].1.raw_text.as_deref(), Some("**next**"));
    assert_ne!(anchors[0].1.context_id, anchors[1].1.context_id);
    let mut pcm = vec![];
    for (f, t) in &words {
        let index = usize::from(t.text.text == "next");
        assert_eq!(t.context_id, anchors[index].1.context_id);
        assert_eq!(f.metadata["segment_id"], json!(anchors[index].0.id));
        assert_eq!(
            f.metadata["source_frame_id"],
            json!([first_id, second_id][index])
        );
        assert_eq!(
            f.transport_destination.as_deref(),
            Some(["left", "right"][index])
        );
        assert_eq!(t.text.append_to_context, index == 1);
        assert!(f.has_pipeline_pts());
    }
    for f in &frames {
        if let FrameData::TtsAudio(a) = &f.data {
            let index = usize::from(f.transport_destination.as_deref() == Some("right"));
            assert_eq!(a.context_id, anchors[index].1.context_id);
            assert_eq!((a.sample_rate, a.num_channels), (24000, 1));
            assert_eq!(
                f.metadata["source_frame_id"],
                json!([first_id, second_id][index])
            );
            pcm.extend_from_slice(&a.audio);
        }
    }
    assert_eq!(pcm, [1, 0, 2, 0, 1, 0]);
    let first_audio = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsAudio(_)))
        .unwrap();
    let first_word = frames
        .iter()
        .position(|f| matches!(f.data, FrameData::TtsText(_)))
        .unwrap();
    assert!(first_audio < first_word);
    assert_eq!(
        frames
            .iter()
            .find(|f| matches!(f.data, FrameData::LlmResponseEnd))
            .unwrap()
            .pts,
        stops[1].pts
    );

    })).await;
}

#[tokio::test]
async fn settings_are_atomic_and_snapshot_per_request_and_fallback_pcm_is_not_relabelled() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener, 32000);
            let (captured, ready) = oneshot::channel();
            let (release, released) = oneshot::channel();
            let server = owners.spawn(async move {
                let (mut first, _) = listener.accept().await.unwrap();
                let body = request(&mut first).await.1;
                assert_eq!(body["voice_id"], "geffen_32");
                assert_eq!(body["model"], "simba-3.2");
                assert_eq!(body["output_format"], "pcm_24000");
                captured.send(()).unwrap();
                released.await.unwrap();
                reply(&mut first, &simple_body("first", 100.0), false).await;
                let (mut second, _) = listener.accept().await.unwrap();
                let body = request(&mut second).await.1;
                assert_eq!(body["voice_id"], "next-voice");
                assert_eq!(body["model"], "next-model");
                assert_eq!(body["language"], "fr-FR");
                reply(&mut second, &simple_body("second", 100.0), false).await;
            });
            let worker = pipeline(owners, tts.clone(), 32000).await;
            send(worker, input("first", "first", "first", false)).await;
            tokio::time::timeout(DEADLINE, ready)
                .await
                .unwrap()
                .unwrap();
            let delta = TtsSettings {
                voice: Setting::Value("next-voice".into()),
                language: Setting::Value("fr".into()),
                base: ServiceSettings {
                    model: Setting::Value("next-model".into()),
                    ..Default::default()
                },
            };
            assert!(tts.update_settings(&delta).unwrap());
            let invalid = TtsSettings {
                voice: Setting::Value("must-not-leak".into()),
                base: ServiceSettings {
                    model: Setting::Null,
                    ..Default::default()
                },
                ..Default::default()
            };
            assert!(tts.update_settings(&invalid).is_err());
            for key in [
                "speed",
                "sample_rate",
                "loudness_normalization",
                "text_normalization",
            ] {
                let mut unsupported = TtsSettings::default();
                unsupported.base.extra.insert(key.into(), json!(false));
                assert!(tts.update_settings(&unsupported).is_err());
            }
            send(worker, input("second", "second", "second", true)).await;
            release.send(()).unwrap();
            let frames = finish(worker).await;
            owners.join_server(server).await;
            let audio: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.data {
                    FrameData::TtsAudio(a) => Some(a),
                    _ => None,
                })
                .collect();
            assert_eq!(audio.len(), 2);
            assert!(audio.iter().all(|a| a.sample_rate == 24000));
            assert!(!frames.iter().any(|f| matches!(f.data, FrameData::Error(_))));
        })
    })
    .await;
}

#[tokio::test]
async fn missing_audio_provider_error_and_truncated_http_are_failed_contexts() {
    with_owners(|owners| {
        Box::pin(async move {
            let cases = [
                (
                    event(
                        "speech.chunk",
                        json!({"speech_marks":[mark(0,3,0.0,100.0,"one")]}),
                    ) + &event("speech.done", json!({"audio_duration_ms":500})),
                    false,
                    false,
                ),
                (
                    event("speech.chunk", json!({"audio":STANDARD.encode([1,0])}))
                        + &event(
                            "speech.error",
                            json!({"error":{"code":"secret","message":"private-input"}}),
                        ),
                    false,
                    true,
                ),
                (simple_body("one", 500.0), true, true),
            ];
            for (body, truncated, has_audio) in cases {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let tts = processor(&listener, 24000);
                let server = owners.spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    request(&mut socket).await;
                    reply(&mut socket, &body, truncated).await;
                });
                let worker = pipeline(owners, tts, 24000).await;
                send(
                    worker,
                    Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("one"))),
                )
                .await;
                send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
                let frames = finish(worker).await;
                owners.join_server(server).await;
                assert_eq!(
                    frames
                        .iter()
                        .any(|f| matches!(f.data, FrameData::TtsAudio(_))),
                    has_audio
                );
                assert!(frames.iter().any(|f| matches!(f.data, FrameData::Error(_))));
                assert!(frames
                    .iter()
                    .filter(|f| matches!(
                        f.data,
                        FrameData::TtsStopped(_) | FrameData::LlmResponseEnd
                    ))
                    .all(|f| f.metadata.get("failed") == Some(&json!(true))));
                assert!(!frames
                    .iter()
                    .any(|f| matches!(f.data, FrameData::BotStartedSpeaking)));
                if !has_audio {
                    assert!(!frames
                        .iter()
                        .any(|f| matches!(f.data, FrameData::TtsText(_))));
                }
                for frame in &frames {
                    if let FrameData::Error(e) = &frame.data {
                        assert!(!e.error.contains("secret") && !e.error.contains("private-input"));
                    }
                }
            }
        })
    })
    .await;
}

#[tokio::test]
async fn interruption_closes_stalled_request_and_resets_timeline_without_late_replay() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener, 24000);
            let (held, ready) = oneshot::channel();
            let server = owners.spawn(async move {
                let (mut first, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut first).await.1["input"], "first");
                reply(&mut first, &simple_body("first", 10_000.0), false).await;
                let (mut stalled, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut stalled).await.1["input"], "stalled");
                headers(&mut stalled).await;
                held.send(()).unwrap();
                let mut byte = [0];
                assert_eq!(
                    tokio::time::timeout(DEADLINE, stalled.read(&mut byte))
                        .await
                        .unwrap()
                        .unwrap(),
                    0
                );
                let (mut new, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut new).await.1["input"], "new");
                reply(&mut new, &simple_body("new", 250.0), false).await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(40), listener.accept())
                        .await
                        .is_err()
                );
            });
            let worker = pipeline(owners, tts, 24000).await;
            send(
                worker,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("first"))),
            )
            .await;
            send(
                worker,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("stalled"))),
            )
            .await;
            tokio::time::timeout(DEADLINE, ready)
                .await
                .unwrap()
                .unwrap();
            send(
                worker,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("queued-old"))),
            )
            .await;
            send(worker, Frame::new(FrameData::Interruption)).await;
            tokio::time::timeout(DEADLINE, async {
                while let Some(output) = worker.recv().await {
                    if matches!(output.frame.data, FrameData::Interruption) {
                        return;
                    }
                }
                panic!("missing interruption");
            })
            .await
            .unwrap();
            send(
                worker,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("new"))),
            )
            .await;
            let frames = finish(worker).await;
            owners.join_server(server).await;
            let words: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.data {
                    FrameData::TtsText(t) => Some((f, t)),
                    _ => None,
                })
                .collect();
            assert_eq!(words.len(), 1);
            assert_eq!(words[0].1.text.text, "new");
            assert_eq!(words[0].0.pts, Some(BASE));
            assert_eq!(
                frames
                    .iter()
                    .find(|f| matches!(f.data, FrameData::TtsStopped(_)))
                    .unwrap()
                    .pts,
                Some(BASE + 250_000_001)
            );
        })
    })
    .await;
}

#[tokio::test]
async fn cancel_closes_stalled_http_and_joins_owned_work() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener, 24000);
            let (held, ready) = oneshot::channel();
            let server = owners.spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                request(&mut socket).await;
                headers(&mut socket).await;
                held.send(()).unwrap();
                let mut byte = [0];
                assert_eq!(
                    tokio::time::timeout(DEADLINE, socket.read(&mut byte))
                        .await
                        .unwrap()
                        .unwrap(),
                    0
                );
            });
            let worker = pipeline(owners, tts, 24000).await;
            send(
                worker,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("held"))),
            )
            .await;
            tokio::time::timeout(DEADLINE, ready)
                .await
                .unwrap()
                .unwrap();
            worker.sender().cancel("test cancellation").await.unwrap();
            let (frames, _) = tokio::time::timeout(DEADLINE, worker.collect())
                .await
                .unwrap()
                .unwrap();
            owners.join_server(server).await;
            assert!(!frames.iter().any(|f| matches!(
                f.frame.data,
                FrameData::TtsText(_) | FrameData::TtsAudio(_) | FrameData::BotStartedSpeaking
            )));
        })
    })
    .await;
}

#[tokio::test]
async fn graceful_stop_drains_then_reusing_processor_starts_a_fresh_timeline() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener, 24000);
            let server = owners.spawn(async move {
                for (text, duration) in [("first", 10_000.0), ("second", 250.0)] {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    assert_eq!(request(&mut socket).await.1["input"], text);
                    reply(&mut socket, &simple_body(text, duration), false).await;
                }
            });
            let first = pipeline(owners, tts.clone(), 24000).await;
            send(
                first,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("first"))),
            )
            .await;
            send(first, Frame::new(FrameData::Stop)).await;
            let (frames, report) = tokio::time::timeout(DEADLINE, first.collect())
                .await
                .unwrap()
                .unwrap();
            assert!(report.errors.is_empty());
            let stopped = frames
                .iter()
                .position(|f| matches!(f.frame.data, FrameData::TtsStopped(_)))
                .unwrap();
            let terminal = frames
                .iter()
                .position(|f| matches!(f.frame.data, FrameData::Stop))
                .unwrap();
            assert!(stopped < terminal);
            assert_eq!(frames[stopped].frame.pts, Some(BASE + 10_000_000_001));
            let second = pipeline(owners, tts, 24000).await;
            send(
                second,
                Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new("second"))),
            )
            .await;
            let frames = finish(second).await;
            assert_eq!(
                frames
                    .iter()
                    .find(|f| matches!(f.data, FrameData::TtsText(_)))
                    .unwrap()
                    .pts,
                Some(BASE)
            );
            assert_eq!(
                frames
                    .iter()
                    .find(|f| matches!(f.data, FrameData::TtsStopped(_)))
                    .unwrap()
                    .pts,
                Some(BASE + 250_000_001)
            );
            owners.join_server(server).await;
        })
    })
    .await;
}
