#![cfg(feature = "minimax")]
//! Synthetic MiniMax HTTP/SSE crosses the public native client and shared TTS
//! processor. Controlled Played receipts do not attest a speaker or live service.
use futures_util::FutureExt;
use pipecat::{
    core::*,
    speech::{
        minimax::{MiniMaxHttpClient, MiniMaxSynthesisRequest},
        HttpTtsBackend, HttpTtsProcessor, SpeechConfig,
    },
};
use serde_json::{json, Value};
use std::{
    future::Future,
    ops::{Deref, DerefMut},
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{oneshot, Semaphore},
};

const DEADLINE: Duration = Duration::from_secs(5);
const RATE: u32 = 24000;
const BASE: u64 = 100_000_000;
const GROUP: &str = "fixture+group/&= space";
struct Clock(tokio::time::Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        BASE + self.0.elapsed().as_nanos() as u64
    }
}

// Keep worker and server handles outside every timeout and unwind future. A
// failed assertion still cancels and joins all retained owners. The Drop test
// separately observes native cleanup and socket EOF before processor reuse.
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
    async fn collect(&mut self) -> Result<(Vec<Frame>, WorkerReport), PipelineError> {
        let mut frames = vec![];
        while let Some(output) = self.worker.recv().await {
            frames.push(output.frame);
        }
        let report = self.worker.join().await;
        self.joined = true;
        report.map(|report| (frames, report))
    }
}
#[derive(Default)]
struct Owners {
    workers: Vec<OwnedWorker>,
    servers: Vec<Option<tokio::task::JoinHandle<()>>>,
}
impl Owners {
    fn spawn(&mut self, future: impl Future<Output = ()> + Send + 'static) -> usize {
        let index = self.servers.len();
        self.servers.push(Some(tokio::spawn(future)));
        index
    }
    async fn join_server(&mut self, index: usize) {
        // Borrow the handle: timing out a consumed handle would detach the peer.
        match tokio::time::timeout(DEADLINE, self.servers[index].as_mut().unwrap()).await {
            Ok(result) => {
                self.servers[index].take();
                result.unwrap();
            }
            Err(_) => {
                self.servers[index].as_ref().unwrap().abort();
                let _ = self.servers[index].as_mut().unwrap().await;
                self.servers[index].take();
                panic!("MiniMax peer timed out; aborted and joined");
            }
        }
    }
    async fn cleanup(&mut self) -> Vec<String> {
        let mut errors = vec![];
        for owner in &mut self.workers {
            if owner.joined {
                continue;
            }
            if !owner.has_finished() {
                let _ = tokio::time::timeout(DEADLINE, owner.cancel("MiniMax test cleanup")).await;
            }
            match tokio::time::timeout(DEADLINE, owner.collect()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => errors.push(format!("pipeline cleanup: {error:?}")),
                Err(_) => errors.push("pipeline did not join after cancellation".into()),
            }
        }
        for server in &mut self.servers {
            if let Some(mut task) = server.take() {
                match tokio::time::timeout(DEADLINE, &mut task).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => errors.push(format!("MiniMax peer: {error:?}")),
                    Err(_) => {
                        task.abort();
                        let _ = task.await;
                        errors.push("MiniMax peer timed out; aborted and joined".into());
                    }
                }
            }
        }
        errors
    }
}
async fn with_owners(
    run: impl for<'a> FnOnce(&'a mut Owners) -> Pin<Box<dyn Future<Output = ()> + 'a>>,
) {
    let mut owners = Owners::default();
    let result = tokio::time::timeout(
        DEADLINE * 4,
        AssertUnwindSafe(run(&mut owners)).catch_unwind(),
    )
    .await;
    let cleanup = owners.cleanup().await;
    assert!(cleanup.is_empty(), "{cleanup:?}");
    match result {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("MiniMax scenario timed out; retained owners cleaned up"),
    }
}
async fn permit(gate: &Semaphore) {
    tokio::time::timeout(DEADLINE, gate.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
async fn request(socket: &mut TcpStream) -> (String, Value) {
    let mut bytes = vec![];
    let mut buffer = [0; 4096];
    let end = loop {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0, "EOF before request headers");
        bytes.extend_from_slice(&buffer[..count]);
        assert!(bytes.len() < 64 * 1024);
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
    assert!(length < 64 * 1024);
    while bytes.len() < end + length {
        let count = socket.read(&mut buffer).await.unwrap();
        assert!(count > 0, "EOF before request body");
        bytes.extend_from_slice(&buffer[..count]);
    }
    (
        header,
        serde_json::from_slice(&bytes[end..end + length]).unwrap(),
    )
}
fn event(value: Value) -> String {
    format!("data: {value}\r\n\r\n")
}
fn audio(hex: &str) -> String {
    event(json!({"data":{"audio":hex,"status":1},"base_resp":{"status_code":0}}))
}
fn complete(aggregate: &str) -> String {
    // MiniMax status 2 audio is the entire aggregation, not another delta. It
    // must never duplicate the incrementally delivered PCM.
    event(
        json!({"data":{"audio":aggregate,"status":2},"extra_info":{},"base_resp":{"status_code":0}}),
    )
}
fn body(hex: &str) -> String {
    audio(hex) + &complete(hex)
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
async fn reply(socket: &mut TcpStream, body: &str) {
    headers(socket).await;
    // Deliberately split SSE delimiters, JSON and hex PCM across HTTP chunks.
    for part in body.as_bytes().chunks(17) {
        chunk(socket, part).await;
    }
    socket.write_all(b"0\r\n\r\n").await.unwrap();
}
async fn peer_closed(socket: &mut TcpStream) {
    let mut byte = [0];
    assert_eq!(
        tokio::time::timeout(DEADLINE, socket.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0,
        "cancelled request still owns its HTTP connection"
    );
}
fn processor(listener: &TcpListener) -> Arc<HttpTtsProcessor> {
    let mut config = SpeechConfig::minimax("synthetic-key");
    config.base_url = format!("http://{}", listener.local_addr().unwrap());
    config.timeout = DEADLINE;
    let mut request = MiniMaxSynthesisRequest::new("");
    request.sample_rate = RATE;
    request.language = Some("en-US".into());
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::MiniMax {
                client: MiniMaxHttpClient::new(config, GROUP).unwrap(),
                request,
            },
            8,
        )
        .unwrap(),
    )
}
async fn pipeline(
    owners: &mut Owners,
    processors: Vec<Arc<dyn FrameProcessor>>,
) -> &mut OwnedWorker {
    let mut config = PipelineConfig::default();
    config.start.audio_out_sample_rate = RATE;
    config.shutdown_timeout = Duration::from_secs(1);
    let worker = Pipeline::new(processors)
        .with_observer_clock(Arc::new(Clock(tokio::time::Instant::now())))
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
async fn speak(worker: &PipelineWorker, text: &str) {
    send(
        worker,
        Frame::new(FrameData::TtsSpeak(TtsSpeakFrame::new(text))),
    )
    .await;
}
async fn finish(worker: &mut OwnedWorker) -> Vec<Frame> {
    worker.end().await.unwrap();
    let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(
        !report.errors.iter().any(|error| error.fatal),
        "{:?}",
        report.errors
    );
    frames
}
async fn admission_marker(worker: &mut OwnedWorker) -> Vec<Frame> {
    let mut marker = TextFrame::new("admission barrier");
    marker.skip_tts = Some(true);
    let marker = Frame::new(FrameData::Text(marker));
    let id = marker.id;
    send(worker, marker).await;
    tokio::time::timeout(DEADLINE, async {
        let mut frames = vec![];
        loop {
            let frame = worker.recv().await.expect("missing admission marker").frame;
            let finished = frame.id == id;
            frames.push(frame);
            if finished {
                return frames;
            }
        }
    })
    .await
    .unwrap()
}
fn pcm(frames: &[Frame]) -> Vec<u8> {
    frames
        .iter()
        .filter_map(|frame| match &frame.data {
            FrameData::TtsAudio(audio) => Some(audio.audio.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}
fn no_reported_words(frames: &[Frame]) {
    assert!(!frames.iter().any(|frame| matches!(
        frame.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
    )));
}

#[tokio::test]
async fn exact_request_and_three_text_channels_stay_synthesis_only() {
    with_owners(|owners| Box::pin(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = processor(&listener);
        let server = owners.spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            let (head, value) = request(&mut socket).await;
            assert!(head.starts_with("POST /v1/t2a_v2?GroupId=fixture%2Bgroup%2F%26%3D+space HTTP/1.1\r\n"), "{head}");
            assert!(head.to_ascii_lowercase().contains("authorization: bearer synthetic-key\r\n"));
            assert!(head.to_ascii_lowercase().contains("content-type: application/json\r\n"));
            assert_eq!(value, json!({
                "stream":true,"model":"speech-2.8-turbo","text":"你好 one hundred",
                "voice_setting":{"voice_id":"Calm_Woman","speed":1.0,"vol":1.0,"pitch":0},
                "audio_setting":{"sample_rate":RATE,"bitrate":128000,"format":"pcm","channel":1},
                "language_boost":"English"
            }));
            let response = ": synthetic Unicode 注释\r\n\r\n".to_owned()
                + &audio("01000200")
                + &audio("03000400")
                + &complete("0100020003000400");
            reply(&mut socket, &response).await;
        });
        let worker = pipeline(owners, vec![tts]).await;
        let mut input = TtsTextInputFrame::new("你好 one hundred");
        input.display_text = "你好 100".into();
        input.original_text = "**你好 100**".into();
        input.append_to_context = false;
        input.aggregated_by = "paragraph".into();
        let mut input = Frame::new(FrameData::TtsTextInput(input));
        input.transport_destination = Some("secondary".into());
        let source_id = input.id;
        send(worker, input).await;
        send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
        let frames = finish(worker).await;
        owners.join_server(server).await;
        assert_eq!(pcm(&frames), [1, 0, 2, 0, 3, 0, 4, 0]);
        no_reported_words(&frames);
        assert!(!frames.iter().any(|frame| matches!(frame.data, FrameData::Error(_) | FrameData::BotStartedSpeaking)));
        let anchors: Vec<_> = frames.iter().filter_map(|frame| match &frame.data {
            FrameData::AggregatedText(text) => Some((frame, text)), _ => None,
        }).collect();
        assert_eq!(anchors.len(), 1);
        let (anchor, text) = anchors[0];
        assert_eq!(text.text.text, "你好 100");
        assert_eq!(text.raw_text.as_deref(), Some("**你好 100**"));
        assert_eq!(text.aggregated_by, "paragraph");
        assert!(!text.text.append_to_context);
        assert!(text.text.includes_inter_frame_spaces);
        let diagnostics: Vec<_> = frames.iter().filter_map(|frame| match &frame.data {
            FrameData::TtsAlignmentDiagnostic(diagnostic) => Some((frame, diagnostic)), _ => None,
        }).collect();
        assert_eq!(diagnostics.len(), 1);
        let (frame, diagnostic) = diagnostics[0];
        assert_eq!(diagnostic.evidence, TtsAlignmentEvidence::Unreported);
        assert_eq!(diagnostic.synthesis_outcome, TtsSynthesisOutcome::Succeeded);
        assert_eq!(diagnostic.synthesis_text, "你好 one hundred");
        assert_eq!(diagnostic.original_text.as_deref(), Some("**你好 100**"));
        assert_eq!(diagnostic.segment_id, Some(anchor.id));
        assert_eq!(Some(&diagnostic.context_id), text.context_id.as_ref());
        assert_eq!(frame.metadata["source_frame_id"], json!(source_id));
        for frame in frames.iter().filter(|frame| matches!(frame.data,
            FrameData::TtsAudio(_) | FrameData::AggregatedText(_) | FrameData::TtsAlignmentDiagnostic(_))) {
            assert_eq!(frame.transport_destination.as_deref(), Some("secondary"));
            assert_eq!(frame.metadata["source_frame_id"], json!(source_id));
            if let FrameData::TtsAudio(audio) = &frame.data {
                assert_eq!((audio.sample_rate, audio.num_channels), (RATE, 1));
                assert_eq!(audio.context_id, text.context_id);
            }
        }
        assert_eq!(frames.iter().filter(|frame| matches!(frame.data, FrameData::TtsStarted(_))).count(), 1);
        assert_eq!(frames.iter().filter(|frame| matches!(frame.data, FrameData::TtsStopped(_))).count(), 1);
    })).await;
}

#[tokio::test]
async fn settings_are_atomic_and_snapshot_active_and_already_admitted_requests() {
    with_owners(|owners| Box::pin(async move {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let tts = processor(&listener);
        let (ready_tx, ready) = oneshot::channel();
        let (release, gate) = oneshot::channel();
        let server = owners.spawn(async move {
            let (mut first, _) = listener.accept().await.unwrap();
            let initial = request(&mut first).await.1;
            assert_eq!(initial["text"], "active");
            assert_eq!(initial["voice_setting"]["voice_id"], "Calm_Woman");
            assert_eq!(initial["model"], "speech-2.8-turbo");
            assert_eq!(initial["language_boost"], "English");
            ready_tx.send(()).unwrap();
            gate.await.unwrap();
            reply(&mut first, &body("0100")).await;
            for (text, voice, model, language) in [
                ("queued", "Calm_Woman", "speech-2.8-turbo", "English"),
                ("fresh", "new-voice", "speech-2.8-hd", "French"),
            ] {
                let (mut socket, _) = listener.accept().await.unwrap();
                let value = request(&mut socket).await.1;
                assert_eq!(value["text"], text);
                assert_eq!(value["voice_setting"]["voice_id"], voice);
                assert_eq!(value["model"], model);
                assert_eq!(value["language_boost"], language);
                reply(&mut socket, &body("0200")).await;
            }
        });
        let worker = pipeline(owners, vec![tts.clone()]).await;
        speak(worker, "active").await;
        tokio::time::timeout(DEADLINE, ready).await.unwrap().unwrap();
        speak(worker, "queued").await;
        let mut frames = admission_marker(worker).await;
        let delta = TtsSettings {
            voice: Setting::Value("new-voice".into()),
            language: Setting::Value("fr-CA".into()),
            base: ServiceSettings { model: Setting::Value("speech-2.8-hd".into()), ..Default::default() },
        };
        assert!(tts.update_settings(&delta).unwrap());
        assert!(!tts.update_settings(&delta).unwrap());
        for invalid in [
            TtsSettings { voice: Setting::Value("must-not-leak".into()), base: ServiceSettings {model: Setting::Null, ..Default::default()}, ..Default::default() },
            TtsSettings { voice: Setting::Value("must-not-leak".into()), language: Setting::Value("x".repeat(129)), ..Default::default() },
        ] {
            assert!(tts.update_settings(&invalid).is_err());
        }
        let mut unsupported = TtsSettings::default();
        unsupported.base.extra.insert("sample_rate".into(), json!(8000));
        assert!(tts.update_settings(&unsupported).is_err());
        speak(worker, "fresh").await;
        release.send(()).unwrap();
        frames.extend(finish(worker).await);
        owners.join_server(server).await;
        assert_eq!(pcm(&frames), [1, 0, 2, 0, 2, 0]);
        no_reported_words(&frames);
        assert!(!frames.iter().any(|frame| matches!(frame.data, FrameData::Error(_))));
        assert_eq!(frames.iter().filter(|frame| matches!(&frame.data,
            FrameData::TtsAlignmentDiagnostic(d) if d.evidence == TtsAlignmentEvidence::Unreported && d.synthesis_outcome == TtsSynthesisOutcome::Succeeded)).count(), 3);
    })).await;
}

#[tokio::test]
async fn provider_malformed_hex_and_missing_final_fail_without_success_diagnostics() {
    with_owners(|owners| Box::pin(async move {
        let cases = [
            (event(json!({"base_resp":{"status_code":1000,"status_msg":"private-input synthetic-key"}})), false),
            (audio("0100") + &event(json!({"base_resp":{"status_code":1001,"status_msg":"private-input synthetic-key"}})), true),
            (audio("not-hex") + &complete(""), false),
            (audio("0100"), true),
            ("data: {malformed private-input synthetic-key}\r\n\r\n".into(), false),
        ];
        for (response, has_audio) in cases {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener);
            let server = owners.spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                request(&mut socket).await;
                // One HTTP chunk avoids a deliberate parser rejection racing
                // with dozens of fixture writes to the now-closed connection.
                headers(&mut socket).await;
                chunk(&mut socket, response.as_bytes()).await;
                let _ = socket.write_all(b"0\r\n\r\n").await;
            });
            let worker = pipeline(owners, vec![tts]).await;
            speak(worker, "private-input").await;
            send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
            let frames = finish(worker).await;
            owners.join_server(server).await;
            assert_eq!(!pcm(&frames).is_empty(), has_audio);
            no_reported_words(&frames);
            let errors: Vec<_> = frames.iter().filter_map(|frame| match &frame.data {
                FrameData::Error(error) => Some(error), _ => None,
            }).collect();
            assert!(!errors.is_empty());
            assert!(errors.iter().all(|error| !error.error.contains("private-input") && !error.error.contains("synthetic-key")));
            assert!(!frames.iter().any(|frame| matches!(&frame.data,
                FrameData::TtsAlignmentDiagnostic(d) if d.synthesis_outcome == TtsSynthesisOutcome::Succeeded)));
            let terminals: Vec<_> = frames.iter().filter(|frame| matches!(frame.data,
                FrameData::TtsStopped(_) | FrameData::LlmResponseEnd)).collect();
            assert_eq!(terminals.len(), 2);
            assert!(terminals.iter().all(|frame| frame.metadata.get("failed") == Some(&json!(true))));
        }
    })).await;
}

#[tokio::test]
async fn interruption_closes_held_stream_and_fresh_request_cannot_replay_old_queue() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener);
            let (held, ready) = oneshot::channel();
            let server = owners.spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut socket).await.1["text"], "old-active");
                headers(&mut socket).await;
                held.send(()).unwrap();
                peer_closed(&mut socket).await;
                let (mut fresh, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut fresh).await.1["text"], "fresh");
                reply(&mut fresh, &body("09000a00")).await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(40), listener.accept())
                        .await
                        .is_err()
                );
            });
            let worker = pipeline(owners, vec![tts]).await;
            speak(worker, "old-active").await;
            tokio::time::timeout(DEADLINE, ready)
                .await
                .unwrap()
                .unwrap();
            speak(worker, "queued-must-not-run").await;
            admission_marker(worker).await;
            send(worker, Frame::new(FrameData::Interruption)).await;
            tokio::time::timeout(DEADLINE, async {
                loop {
                    if matches!(
                        worker
                            .recv()
                            .await
                            .expect("missing interruption")
                            .frame
                            .data,
                        FrameData::Interruption
                    ) {
                        break;
                    }
                }
            })
            .await
            .unwrap();
            speak(worker, "fresh").await;
            let frames = finish(worker).await;
            owners.join_server(server).await;
            assert_eq!(pcm(&frames), [9, 0, 10, 0]);
            no_reported_words(&frames);
            assert!(!frames
                .iter()
                .any(|frame| matches!(frame.data, FrameData::Error(_))));
            let diagnostics: Vec<_> = frames
                .iter()
                .filter_map(|frame| match &frame.data {
                    FrameData::TtsAlignmentDiagnostic(d) => Some(d),
                    _ => None,
                })
                .collect();
            assert_eq!(diagnostics.len(), 1);
            assert_eq!(diagnostics[0].synthesis_text, "fresh");
        })
    })
    .await;
}

#[tokio::test]
async fn cancel_closes_held_stream_and_joins_without_late_audio() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener);
            let (held, ready) = oneshot::channel();
            let server = owners.spawn(async move {
                let (mut socket, _) = listener.accept().await.unwrap();
                request(&mut socket).await;
                headers(&mut socket).await;
                held.send(()).unwrap();
                peer_closed(&mut socket).await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(40), listener.accept())
                        .await
                        .is_err()
                );
            });
            let worker = pipeline(owners, vec![tts]).await;
            speak(worker, "cancel-active").await;
            tokio::time::timeout(DEADLINE, ready)
                .await
                .unwrap()
                .unwrap();
            speak(worker, "cancel-queued").await;
            admission_marker(worker).await;
            worker.cancel("MiniMax cancellation proof").await.unwrap();
            let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert!(!report.errors.iter().any(|error| error.fatal));
            owners.join_server(server).await;
            assert!(pcm(&frames).is_empty());
            no_reported_words(&frames);
            assert!(!frames.iter().any(|frame| matches!(
                frame.data,
                FrameData::TtsAlignmentDiagnostic(_) | FrameData::BotStartedSpeaking
            )));
        })
    })
    .await;
}

struct Probe {
    frames: Mutex<Vec<Frame>>,
    cleaned: Semaphore,
}
impl Default for Probe {
    fn default() -> Self {
        Self {
            frames: Mutex::new(vec![]),
            cleaned: Semaphore::new(0),
        }
    }
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "MiniMax-frame-proof"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.frames.lock().unwrap().push(frame.clone());
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.add_permits(1);
        Ok(())
    }
}

#[tokio::test]
async fn owner_drop_closes_http_and_cleanup_precedes_fresh_processor_reuse() {
    with_owners(|owners| {
        Box::pin(async move {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let tts = processor(&listener);
            let probe = Arc::new(Probe::default());
            let (held, ready) = oneshot::channel();
            let (closed, closed_rx) = oneshot::channel();
            let server = owners.spawn(async move {
                let (mut old, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut old).await.1["text"], "drop-active");
                headers(&mut old).await;
                held.send(()).unwrap();
                peer_closed(&mut old).await;
                closed.send(()).unwrap();
                let (mut fresh, _) = listener.accept().await.unwrap();
                assert_eq!(request(&mut fresh).await.1["text"], "after-drop");
                reply(&mut fresh, &body("0b000c00")).await;
                assert!(
                    tokio::time::timeout(Duration::from_millis(40), listener.accept())
                        .await
                        .is_err()
                );
            });
            let worker = pipeline(owners, vec![tts.clone(), probe.clone()]).await;
            speak(worker, "drop-active").await;
            tokio::time::timeout(DEADLINE, ready)
                .await
                .unwrap()
                .unwrap();
            speak(worker, "drop-queued-must-not-run").await;
            admission_marker(worker).await;
            // Exercise PipelineWorker::drop itself rather than substituting Cancel.
            drop(owners.workers.pop().unwrap());
            tokio::time::timeout(DEADLINE, closed_rx)
                .await
                .unwrap()
                .unwrap();
            permit(&probe.cleaned).await;
            assert!(pcm(&probe.frames.lock().unwrap()).is_empty());
            let fresh = pipeline(owners, vec![tts]).await;
            speak(fresh, "after-drop").await;
            let frames = finish(fresh).await;
            owners.join_server(server).await;
            assert_eq!(pcm(&frames), [11, 0, 12, 0]);
            no_reported_words(&frames);
            assert!(!frames
                .iter()
                .any(|frame| matches!(frame.data, FrameData::Error(_))));
            let diagnostics: Vec<_> = frames
                .iter()
                .filter_map(|frame| match &frame.data {
                    FrameData::TtsAlignmentDiagnostic(d) => Some(d),
                    _ => None,
                })
                .collect();
            assert_eq!(diagnostics.len(), 1);
            assert_eq!(diagnostics[0].synthesis_text, "after-drop");
        })
    })
    .await;
}

#[cfg(feature = "assistant-history")]
mod playback_history {
    use super::*;
    use pipecat::{
        audio::{encode_pcm16_le, PcmFormat},
        media_transport::*,
        services::Context,
        turns::{
            AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator, SharedContext,
        },
    };
    use std::{
        collections::BTreeMap,
        sync::atomic::{AtomicUsize, Ordering},
    };

    struct Playback {
        entered: Semaphore,
        release: Semaphore,
        starts: Mutex<Vec<(PcmFormat, Vec<Option<String>>)>>,
        submitted: Mutex<Vec<Frame>>,
        receipts: AtomicUsize,
        stops: AtomicUsize,
    }
    #[async_trait]
    impl OutputBackend for Playback {
        async fn start(
            &self,
            format: PcmFormat,
            destinations: &[Option<String>],
        ) -> Result<(), ProcessorError> {
            self.starts
                .lock()
                .unwrap()
                .push((format, destinations.to_vec()));
            Ok(())
        }
        async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
            self.submitted.lock().unwrap().push(frame.clone());
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|error| ProcessorError::new(error.to_string()))?
                .forget();
            self.receipts.fetch_add(1, Ordering::SeqCst);
            Ok(PlaybackReceipt::Played)
        }
        async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
            Ok(())
        }
        async fn stop(&self) -> Result<(), ProcessorError> {
            self.stops.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[tokio::test]
    async fn real_output_played_receipts_cannot_turn_unreported_synthesis_into_heard_history() {
        with_owners(|owners| {
            Box::pin(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
                let tts = processor(&listener);
                let samples: Vec<_> = (0..RATE * 30 / 1000)
                    .map(|index| index as i16 - 300)
                    .collect();
                let expected_pcm = encode_pcm16_le(&samples);
                let hex: String = expected_pcm
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect();
                let server = owners.spawn(async move {
                    let (mut socket, _) = listener.accept().await.unwrap();
                    assert_eq!(
                        request(&mut socket).await.1["text"],
                        "No invented heard words."
                    );
                    reply(&mut socket, &body(&hex)).await;
                });
                let shared = SharedContext::new(Context::default());
                let assistant = Arc::new(LLMAssistantAggregator::new(
                    shared.clone(),
                    AssistantAggregatorConfig {
                        text_mode: AssistantTextMode::Spoken,
                    },
                ));
                let history = SpokenHistory::new(
                    SpokenHistoryConfig::default(),
                    Arc::new(AssistantSpokenHistorySink::new(assistant.clone())),
                )
                .unwrap();
                let playback = Arc::new(Playback {
                    entered: Semaphore::new(0),
                    release: Semaphore::new(0),
                    starts: Mutex::new(vec![]),
                    submitted: Mutex::new(vec![]),
                    receipts: AtomicUsize::new(0),
                    stops: AtomicUsize::new(0),
                });
                let before = Arc::new(Probe::default());
                let after = Arc::new(Probe::default());
                let output = Arc::new(
                    OutputTransport::new(
                        "MiniMax-controlled-playback",
                        OutputTransportConfig {
                            sample_rate: Some(RATE),
                            chunk_duration: Duration::from_millis(10),
                            auto_silence: false,
                            end_silence: Duration::ZERO,
                            write_timeout: DEADLINE,
                            ..Default::default()
                        },
                        playback.clone(),
                        None,
                        BTreeMap::new(),
                    )
                    .unwrap()
                    .with_spoken_history(history.clone())
                    .unwrap(),
                );
                let control = output.control();
                let worker = pipeline(
                    owners,
                    vec![
                        Arc::new(history.source_boundary("source-before-MiniMax").unwrap()),
                        tts,
                        before.clone(),
                        output,
                        after.clone(),
                        assistant,
                    ],
                )
                .await;
                tokio::time::timeout(DEADLINE, async {
                    while !control.is_ready() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let start = Frame::new(FrameData::LlmResponseStart);
                let start_id = start.id;
                let input = Frame::new(FrameData::TtsTextInput(TtsTextInputFrame::new(
                    "No invented heard words.",
                )));
                let input_id = input.id;
                let end = Frame::new(FrameData::LlmResponseEnd);
                let end_id = end.id;
                for frame in [start, input, end] {
                    send(worker, frame).await;
                }
                permit(&playback.entered).await;
                assert_eq!(playback.receipts.load(Ordering::SeqCst), 0);
                assert!(shared.snapshot().unwrap().messages.is_empty());
                assert_eq!(history.status().committed_contexts, 0);
                playback.release.add_permits(3);
                let frames = finish(worker).await;
                owners.join_server(server).await;
                assert!(!frames
                    .iter()
                    .any(|frame| matches!(frame.data, FrameData::Error(_))));
                assert_eq!(playback.receipts.load(Ordering::SeqCst), 3);
                assert_eq!(playback.stops.load(Ordering::SeqCst), 1);
                assert_eq!(
                    *playback.starts.lock().unwrap(),
                    vec![(PcmFormat::new(RATE, 1).unwrap(), vec![None])]
                );
                let submitted = playback.submitted.lock().unwrap();
                assert_eq!(submitted.len(), 3);
                let mut played_pcm = vec![];
                for frame in submitted.iter() {
                    let audio = match &frame.data {
                        FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => audio,
                        _ => panic!("output backend received non-PCM frame"),
                    };
                    assert_eq!((audio.sample_rate, audio.num_channels), (RATE, 1));
                    assert_eq!(audio.num_frames(), (RATE / 100) as usize);
                    played_pcm.extend_from_slice(&audio.audio);
                }
                assert_eq!(
                    played_pcm, expected_pcm,
                    "final aggregate must not replay the PCM"
                );
                assert_eq!(pcm(&before.frames.lock().unwrap()), expected_pcm);
                for probe in [&before, &after] {
                    let observed = probe.frames.lock().unwrap();
                    no_reported_words(&observed);
                    let diagnostics: Vec<_> = observed
                        .iter()
                        .filter_map(|frame| match &frame.data {
                            FrameData::TtsAlignmentDiagnostic(diagnostic) => {
                                Some((frame, diagnostic))
                            }
                            _ => None,
                        })
                        .collect();
                    assert_eq!(diagnostics.len(), 1);
                    let (frame, diagnostic) = diagnostics[0];
                    assert_eq!(diagnostic.evidence, TtsAlignmentEvidence::Unreported);
                    assert_eq!(diagnostic.synthesis_outcome, TtsSynthesisOutcome::Succeeded);
                    assert_eq!(diagnostic.synthesis_text, "No invented heard words.");
                    assert_eq!(frame.metadata["source_frame_id"], json!(input_id));
                    assert_ne!(frame.metadata.get("spoken"), Some(&json!(true)));
                    for id in [start_id, end_id] {
                        let source: Vec<_> =
                            observed.iter().filter(|frame| frame.id == id).collect();
                        assert_eq!(
                            source.len(),
                            1,
                            "original source boundary must cross exactly once"
                        );
                        assert_ne!(source[0].metadata.get("failed"), Some(&json!(true)));
                    }
                }
                // Successful synthesis and three Played receipts prove PCM delivery
                // only. No provider words means no audible text or assistant commit.
                assert!(shared.snapshot().unwrap().messages.is_empty());
                assert_eq!(history.status().committed_contexts, 0);
                assert_eq!(history.status().pending_responses, 0);
                assert_eq!(history.status().pending_source_responses, 0);
            })
        })
        .await;
    }
}
