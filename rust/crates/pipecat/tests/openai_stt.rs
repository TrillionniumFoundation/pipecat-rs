//! Transcription-only OpenAI STT through real loopback WebSockets and native pipelines.
//! Synthetic PCM and provider events only: no API key, live service, remote LLM,
//! tools, responses, assistant audio, or conversation/history authority is claimed.
//! The selected profile is static, local-VAD, PCM16LE/24 kHz/mono. InputTransport
//! performs explicit resampling; this does not claim Python hot-settings parity.
//! Source anchor: python-af9f9d/services/openai/stt.py and the independently
//! executed slice68 source-golden oracle (default session and per-item events).
#![cfg(feature = "openai-stt")]

use base64::{engine::general_purpose::STANDARD, Engine};
use futures_util::{SinkExt, StreamExt};
use pipecat::{
    audio::{encode_pcm16_le, resample_pcm16, PcmFormat},
    core::*,
    media_transport::{InputBackend, InputIngress, InputTransport, InputTransportConfig},
    streaming_speech::{OpenAiRealtimeSttProcessor, OpenAiSttClient, OpenAiSttConfig},
};
use serde_json::{json, Value};
use std::{
    future::Future,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    sync::mpsc,
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::tungstenite::{self, Message};

const WAIT: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(80);
const KEY: &str = "synthetic-openai-stt-fixture";
type TestResult<T = ()> = Result<T, String>;

fn require(condition: bool, message: impl Into<String>) -> TestResult {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
async fn bounded<T>(label: &str, future: impl Future<Output = TestResult<T>>) -> TestResult<T> {
    timeout(WAIT, future)
        .await
        .map_err(|_| format!("{label} deadline"))?
}
async fn join_owned<T>(handle: &mut JoinHandle<TestResult<T>>) -> TestResult<T> {
    match timeout(WAIT, &mut *handle).await {
        Ok(result) => result.map_err(|error| error.to_string())?,
        Err(_) => {
            handle.abort();
            let _ = handle.await;
            Err("fixture task deadline; aborted and joined".into())
        }
    }
}

#[derive(Default, Debug, Clone)]
struct WireLog {
    requests: Vec<(String, String, bool)>,
    updates: Vec<Value>,
    commands: Vec<(usize, Value)>,
    closed: usize,
}
enum PeerCommand {
    Event(Value),
    Ready,
}
struct Peer {
    endpoint: String,
    commands: mpsc::UnboundedSender<PeerCommand>,
    log: Arc<Mutex<WireLog>>,
    server: JoinHandle<TestResult>,
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite's handshake callback signature.
    async fn new(sessions: usize, hold_ready: bool) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let endpoint = format!(
            "ws://{}/v1/realtime",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        let (commands, mut receiver) = mpsc::unbounded_channel();
        let log = Arc::new(Mutex::new(WireLog::default()));
        let observed = log.clone();
        let server = tokio::spawn(async move {
            for connection in 0..sessions {
                let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
                let headers = observed.clone();
                let mut socket = tokio_tungstenite::accept_hdr_async(
                    tcp,
                    move |request: &tungstenite::handshake::server::Request, response| {
                        headers.lock().unwrap().requests.push((
                            request.uri().to_string(),
                            request
                                .headers()
                                .get("authorization")
                                .and_then(|v| v.to_str().ok())
                                .unwrap_or_default()
                                .into(),
                            request.headers().contains_key("openai-beta"),
                        ));
                        Ok(response)
                    },
                )
                .await
                .map_err(|e| e.to_string())?;
                socket.send(Message::Text(json!({"type":"session.created","session":{"id":"synthetic-session","type":"transcription"}}).to_string().into())).await.map_err(|e| e.to_string())?;
                let mut update: Option<Value> = None;
                loop {
                    tokio::select! {
                        input = receiver.recv() => {
                            let event = match input {
                                Some(PeerCommand::Event(event)) => event,
                                Some(PeerCommand::Ready) => ready(update.as_ref().ok_or("ready before session.update")?),
                                None => return Err("fixture command owner dropped before client close".into()),
                            };
                            if socket.send(Message::Text(event.to_string().into())).await.is_err() {
                                // A late provider event may race genuine client cancellation.
                                break;
                            }
                        }
                        message = socket.next() => match message {
                            Some(Ok(Message::Text(text))) => {
                                let message: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                                if message["type"] == "session.update" {
                                    require(update.is_none(), "unexpected live settings update")?;
                                    observed.lock().unwrap().updates.push(message.clone());
                                    if !hold_ready {
                                        socket.send(Message::Text(ready(&message).to_string().into())).await.map_err(|e| e.to_string())?;
                                    }
                                    update = Some(message);
                                } else {
                                    observed.lock().unwrap().commands.push((connection, message));
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                // Reading Close queued tungstenite's automatic
                                // reply. Flush it; attempting another close()
                                // can fail before the queued reply is written.
                                let _ = socket.flush().await;
                                break;
                            }
                            None => break,
                            Some(Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed)) => break,
                            Some(Err(tungstenite::Error::Protocol(tungstenite::error::ProtocolError::ResetWithoutClosingHandshake))) => break,
                            Some(Err(tungstenite::Error::Io(error))) if matches!(error.kind(), std::io::ErrorKind::ConnectionReset | std::io::ErrorKind::ConnectionAborted | std::io::ErrorKind::BrokenPipe) => break,
                            Some(Err(error)) => return Err(error.to_string()),
                            Some(Ok(_)) => {},
                        }
                    }
                }
                observed.lock().unwrap().closed += 1;
            }
            Ok(())
        });
        Ok(Self {
            endpoint,
            commands,
            log,
            server,
        })
    }
    fn snapshot(&self) -> WireLog {
        self.log.lock().unwrap().clone()
    }
    fn event(&self, event: Value) -> TestResult {
        self.commands
            .send(PeerCommand::Event(event))
            .map_err(|_| "loopback peer closed".into())
    }
    async fn wait(&self, predicate: impl Fn(&WireLog) -> bool) -> TestResult {
        bounded("wire observation", async {
            while !predicate(&self.snapshot()) {
                sleep(Duration::from_millis(2)).await;
            }
            Ok(())
        })
        .await
    }
    async fn close(&mut self) -> TestResult {
        join_owned(&mut self.server).await
    }
}
fn ready(update: &Value) -> Value {
    let mut session = update["session"].clone();
    session["id"] = json!("synthetic-session");
    json!({"type":"session.updated","session":session})
}
fn config(peer: &Peer) -> OpenAiSttConfig {
    let mut config = OpenAiSttConfig::new(KEY);
    config.endpoint = peer.endpoint.clone();
    config.ready_timeout = Duration::from_secs(2);
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(1);
    config.limits.response_timeout = Duration::from_millis(700);
    config.limits.close_timeout = Duration::from_millis(200);
    config
}
fn pipeline_config() -> PipelineConfig {
    let mut config = PipelineConfig {
        startup_timeout: Duration::from_secs(3),
        shutdown_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    config.start.audio_in_sample_rate = 24_000;
    config
}

#[derive(Default)]
struct Witness {
    cleanup: AtomicUsize,
    frames: Mutex<Vec<DirectedFrame>>,
}
#[async_trait]
impl FrameProcessor for Witness {
    fn name(&self) -> &str {
        "OpenAiSttDownstreamWitness"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames
            .lock()
            .unwrap()
            .push(DirectedFrame::new(frame.clone(), direction));
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        // Core invokes this after the STT processor's real cleanup. Tests never
        // call cleanup themselves or substitute socket EOF for this witness.
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct Case {
    peer: Peer,
    stt: Arc<OpenAiRealtimeSttProcessor>,
    witness: Arc<Witness>,
    worker: PipelineWorker,
    observed: Vec<DirectedFrame>,
}
impl Case {
    async fn new() -> TestResult<Self> {
        Self::configured(|_| {}, vec![]).await
    }
    async fn configured(
        change: impl FnOnce(&mut OpenAiSttConfig),
        mut before: Vec<Arc<dyn FrameProcessor>>,
    ) -> TestResult<Self> {
        let mut peer = Peer::new(1, false).await?;
        let mut settings = config(&peer);
        change(&mut settings);
        let client = match OpenAiSttClient::new(settings) {
            Ok(client) => client,
            Err(error) => {
                let cleanup = peer.close().await;
                return Err(format!(
                    "client configuration: {error}; peer teardown: {cleanup:?}"
                ));
            }
        };
        let stt = Arc::new(
            OpenAiRealtimeSttProcessor::new(client)
                .with_user_id("configured-speaker")
                .expect("bounded fixture speaker"),
        );
        let witness = Arc::new(Witness::default());
        before.push(stt.clone());
        before.push(witness.clone());
        let started = Pipeline::new(before).start(pipeline_config()).await;
        let worker = match started {
            Ok(worker) => worker,
            Err(error) => {
                let cleanup = peer.close().await;
                return Err(format!(
                    "pipeline setup: {error}; peer teardown: {cleanup:?}"
                ));
            }
        };
        Ok(Self {
            peer,
            stt,
            witness,
            worker,
            observed: vec![],
        })
    }
    async fn send(&self, frame: Frame, direction: Direction) -> TestResult {
        self.worker
            .sender()
            .send(frame, direction)
            .await
            .map_err(|e| e.to_string())
    }
    async fn receive(&mut self, predicate: impl Fn(&DirectedFrame) -> bool) -> TestResult {
        bounded("pipeline output", async {
            loop {
                let frame = self
                    .worker
                    .recv()
                    .await
                    .ok_or("pipeline closed before expected output")?;
                let found = predicate(&frame);
                self.observed.push(frame);
                if found {
                    return Ok(());
                }
            }
        })
        .await
    }
    async fn pass(&mut self, frame: Frame, direction: Direction) -> TestResult {
        let id = frame.id;
        self.send(frame, direction).await?;
        self.receive(|output| output.frame.id == id && output.direction == direction)
            .await
    }
    async fn audio_commit(&mut self, item: &str) -> TestResult {
        let count = self.peer.snapshot().commands.len();
        self.pass(
            audio(false, &[0, 1000, -1000, i16::MIN, i16::MAX]),
            Direction::Downstream,
        )
        .await?;
        self.peer
            .wait(|log| log.commands.len() == count + 1)
            .await?;
        self.pass(vad_stop(), Direction::Downstream).await?;
        self.peer
            .wait(|log| log.commands.len() == count + 2)
            .await?;
        self.peer.event(committed(item))
    }
    async fn finish(
        mut self,
        scenario: TestResult,
        terminal: FrameData,
    ) -> (Vec<DirectedFrame>, WireLog, WorkerReport) {
        // Keep the real worker in this scope across every deadline. Failed
        // observations trigger cancellation before joining, never detachment.
        let cancel = scenario.is_err();
        let admitted = timeout(WAIT, async {
            if self.worker.has_finished() {
                Ok(())
            } else if cancel {
                self.worker.cancel("OpenAI STT fixture failure").await
            } else {
                self.worker
                    .sender()
                    .send(Frame::new(terminal), Direction::Downstream)
                    .await
            }
        })
        .await;
        if !matches!(admitted, Ok(Ok(()))) {
            let _ = self
                .worker
                .cancel("OpenAI STT terminal admission failed")
                .await;
        }
        let joined = timeout(WAIT, async {
            while let Some(frame) = self.worker.recv().await {
                self.observed.push(frame);
            }
            self.worker.join().await
        })
        .await;
        let report = match joined {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => {
                let _ = self.worker.cancel("OpenAI STT join deadline").await;
                let joined = self.worker.join().await;
                Err(format!("worker deadline; cancelled and joined: {joined:?}"))
            }
        };
        let closed = self.peer.close().await;
        let log = self.peer.snapshot();
        scenario.expect("OpenAI STT scenario");
        assert!(
            matches!(admitted, Ok(Ok(()))),
            "terminal admission: {admitted:?}"
        );
        closed.expect("peer must close and join");
        (self.observed, log, report.expect("real worker must join"))
    }
}
fn audio(user: bool, samples: &[i16]) -> Frame {
    let audio = AudioRawFrame::new(encode_pcm16_le(samples), 24_000, 1).unwrap();
    Frame::new(if user {
        FrameData::UserAudio(UserAudioRawFrame {
            audio,
            user_id: "audio-speaker-must-not-override-config".into(),
        })
    } else {
        FrameData::InputAudio(audio)
    })
}
fn vad_stop() -> Frame {
    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    }))
}
fn committed(item: &str) -> Value {
    json!({"type":"input_audio_buffer.committed","item_id":item,"previous_item_id":null})
}
fn delta(item: &str, text: &str) -> Value {
    json!({"type":"conversation.item.input_audio_transcription.delta","item_id":item,"content_index":0,"delta":text,"provider_extension":{"untouched":true}})
}
fn completed(item: &str, text: &str) -> Value {
    json!({"type":"conversation.item.input_audio_transcription.completed","item_id":item,"content_index":0,"transcript":text,"usage":{"type":"duration","seconds":0.2}})
}
fn transcript(frame: &DirectedFrame) -> Option<(&DetailedTranscriptionFrame, bool)> {
    match &frame.frame.data {
        FrameData::InterimTranscriptionDetailed(value) => Some((value, false)),
        FrameData::TranscriptionDetailed(value) => Some((value, true)),
        _ => None,
    }
}
fn finals(frames: &[DirectedFrame]) -> Vec<&str> {
    frames
        .iter()
        .filter_map(|frame| {
            transcript(frame)
                .filter(|(_, final_)| *final_)
                .map(|(value, _)| value.text.text.as_str())
        })
        .collect()
}
fn client_audio(log: &WireLog) -> Vec<Vec<u8>> {
    log.commands
        .iter()
        .filter(|(_, value)| value["type"] == "input_audio_buffer.append")
        .map(|(_, value)| STANDARD.decode(value["audio"].as_str().unwrap()).unwrap())
        .collect()
}
fn commit_count(log: &WireLog) -> usize {
    log.commands
        .iter()
        .filter(|(_, value)| value["type"] == "input_audio_buffer.commit")
        .count()
}
fn timestamp_seconds(value: &str) -> u64 {
    assert!(
        value.ends_with("+00:00") || value.ends_with('Z'),
        "UTC timestamp required: {value}"
    );
    let fields: Vec<_> = value[..19]
        .split(['-', 'T', ':'])
        .map(|part| part.parse::<u64>().unwrap())
        .collect();
    assert_eq!(fields.len(), 6);
    let [year, month, day, hour, minute, second] = <[u64; 6]>::try_from(fields).unwrap();
    let leap = |year: u64| {
        year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400))
    };
    assert!((1970..=9999).contains(&year) && (1..=12).contains(&month));
    let months = [
        31,
        if leap(year) { 29 } else { 28 },
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    assert!((1..=months[month as usize - 1]).contains(&day));
    assert!(hour < 24 && minute < 60 && second < 60);
    let days: u64 = (1970..year)
        .map(|year| if leap(year) { 366 } else { 365 })
        .sum::<u64>()
        + months[..month as usize - 1].iter().sum::<u64>()
        + day
        - 1;
    days * 86_400 + hour * 3600 + minute * 60 + second
}
fn assert_clean(report: &WorkerReport, state: WorkerState) {
    assert_eq!(report.state, state);
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}

#[tokio::test]
async fn input_only_handshake_exact_pcm_and_detailed_transcripts_preserve_local_envelopes() {
    let mut case = Case::new().await.unwrap();
    let tag = LocalFrameTag::new();
    let mut input = audio(false, &[i16::MIN, -1, 0, 1, i16::MAX]);
    input.pts = Some(831);
    input
        .metadata
        .insert("private-envelope".into(), json!("never-send-to-provider"));
    input.transport_source = Some("local-microphone".into());
    input.transport_destination = Some("local-stt".into());
    if let FrameData::InputAudio(audio) = &mut input.data {
        audio.context_id = Some("local-audio-context".into());
    }
    tag.mark(&mut input).unwrap();
    let user = audio(true, &[2000, -2000]);
    let untouched = Frame::text("local-non-audio-passthrough");
    let partial = delta("A", "partial only");
    let final_event = completed("A", "authoritative final");
    let earliest = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let scenario = bounded("exact audio and result mapping", async {
        case.pass(input.clone(), Direction::Downstream).await?;
        case.pass(user.clone(), Direction::Downstream).await?;
        case.pass(untouched.clone(), Direction::Upstream).await?;
        case.peer.wait(|log| client_audio(log).len() == 2).await?;
        case.pass(vad_stop(), Direction::Downstream).await?;
        case.peer.wait(|log| commit_count(log) == 1).await?;
        case.peer.event(committed("A"))?;
        case.peer.event(partial.clone())?;
        case.peer.event(final_event.clone())?;
        case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
            .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(
        log.requests,
        vec![(
            "/v1/realtime?intent=transcription".into(),
            format!("Bearer {KEY}"),
            false
        )]
    );
    assert_eq!(log.updates.len(), 1);
    assert_eq!(
        log.updates[0],
        json!({"type":"session.update","session":{"type":"transcription","audio":{"input":{"format":{"type":"audio/pcm","rate":24000},"transcription":{"model":"gpt-realtime-whisper","language":"en"},"turn_detection":null}}}})
    );
    let profile = &log.updates[0]["session"];
    assert_eq!(profile["type"], "transcription");
    assert_eq!(
        profile["audio"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>(),
        vec!["input"]
    );
    assert_eq!(
        profile["audio"]["input"]["format"],
        json!({"type":"audio/pcm","rate":24000})
    );
    assert!(profile["audio"]["input"]
        .as_object()
        .unwrap()
        .contains_key("turn_detection"));
    assert!(profile["audio"]["input"]["turn_detection"].is_null());
    for name in [
        "tools",
        "instructions",
        "output_modalities",
        "response",
        "conversation",
        "voice",
    ] {
        assert!(profile.get(name).is_none(), "unexpected authority: {name}");
    }
    assert_eq!(
        client_audio(&log),
        vec![
            encode_pcm16_le(&[i16::MIN, -1, 0, 1, i16::MAX]),
            encode_pcm16_le(&[2000, -2000])
        ]
    );
    assert_eq!(commit_count(&log), 1);
    for (_, event) in &log.commands {
        let keys = event.as_object().unwrap();
        assert!(keys.keys().all(|key| key == "type" || key == "audio"));
        assert!(!event.to_string().contains("local-"));
        assert!(!event.to_string().contains("private-envelope"));
    }
    let forwarded = frames
        .iter()
        .find(|frame| frame.frame.id == input.id)
        .unwrap();
    assert_eq!(forwarded.direction, Direction::Downstream);
    assert_eq!(forwarded.frame.pts, input.pts);
    assert_eq!(forwarded.frame.metadata, input.metadata);
    assert_eq!(forwarded.frame.transport_source, input.transport_source);
    assert_eq!(
        forwarded.frame.transport_destination,
        input.transport_destination
    );
    assert_eq!(forwarded.frame.data, input.data);
    assert!(tag.contains(&forwarded.frame));
    assert!(frames
        .iter()
        .any(|frame| frame.frame.id == user.id && frame.frame.data == user.data));
    assert!(frames
        .iter()
        .any(|frame| frame.frame.id == untouched.id && frame.direction == Direction::Upstream));
    let transcripts: Vec<_> = frames.iter().filter_map(transcript).collect();
    assert_eq!(transcripts.len(), 2);
    for ((value, is_final), expected) in transcripts.iter().zip([&partial, &final_event]) {
        assert_eq!(value.result.as_ref(), Some(expected));
        assert_eq!(value.user_id, "configured-speaker");
        assert!(!value.finalized);
        assert!(value.language.is_none());
        assert_eq!(
            value.text.text,
            if *is_final {
                "authoritative final"
            } else {
                "partial only"
            }
        );
        let stamped = timestamp_seconds(&value.timestamp);
        let latest = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(
            (earliest..=latest).contains(&stamped),
            "timestamp must reflect event receipt: {}",
            value.timestamp
        );
    }
    assert!(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
            - earliest
            < 10
    );
}

#[tokio::test]
async fn upstream_audio_is_passthrough_and_only_vad_stop_commits_in_either_direction() {
    let mut case = Case::new().await.unwrap();
    let upstream = audio(true, &[9, 8, 7]);
    let scenario = bounded("VAD direction and upstream audio", async {
        case.pass(upstream.clone(), Direction::Upstream).await?;
        case.pass(audio(false, &[1, 2]), Direction::Downstream)
            .await?;
        case.peer.wait(|log| client_audio(log).len() == 1).await?;
        case.pass(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Downstream,
        )
        .await?;
        case.pass(
            Frame::new(FrameData::UserStoppedSpeaking),
            Direction::Upstream,
        )
        .await?;
        sleep(QUIET).await;
        require(
            commit_count(&case.peer.snapshot()) == 0,
            "ordinary turn stop committed audio",
        )?;
        case.pass(vad_stop(), Direction::Upstream).await?;
        case.peer.wait(|log| commit_count(log) == 1).await?;
        case.peer.event(committed("upstream-vad"))?;
        case.peer.event(completed("upstream-vad", "first"))?;
        case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
            .await?;
        case.pass(audio(true, &[3, 4]), Direction::Downstream)
            .await?;
        case.peer.wait(|log| client_audio(log).len() == 2).await?;
        case.pass(
            Frame::new(FrameData::VadUserStoppedSpeakingDetailed(
                VadUserStoppedSpeakingFrame::default(),
            )),
            Direction::Downstream,
        )
        .await?;
        case.peer.wait(|log| commit_count(log) == 2).await?;
        case.peer.event(committed("downstream-vad"))?;
        case.peer.event(completed("downstream-vad", "second"))?;
        case.receive(|frame| {
            transcript(frame).is_some_and(|(value, final_)| final_ && value.text.text == "second")
        })
        .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(
        client_audio(&log),
        vec![encode_pcm16_le(&[1, 2]), encode_pcm16_le(&[3, 4])]
    );
    assert_eq!(commit_count(&log), 2);
    assert!(frames
        .iter()
        .any(|frame| frame.frame.id == upstream.id && frame.direction == Direction::Upstream));
}

#[tokio::test]
async fn reverse_order_finals_follow_wire_order_without_delta_concatenation() {
    let mut case = Case::new().await.unwrap();
    let scenario = bounded("independent committed items", async {
        case.audio_commit("A").await?;
        case.audio_commit("B").await?;
        case.peer.event(delta("A", "A-part-1"))?;
        case.peer.event(delta("B", "B-part"))?;
        case.peer.event(delta("A", "A-part-2"))?;
        case.peer.event(completed("B", "B-final"))?;
        case.peer.event(completed("A", "A-final"))?;
        case.receive(|frame| {
            transcript(frame).is_some_and(|(value, final_)| final_ && value.text.text == "A-final")
        })
        .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(commit_count(&log), 2);
    assert_eq!(finals(&frames), vec!["B-final", "A-final"]);
    let interims: Vec<_> = frames
        .iter()
        .filter_map(transcript)
        .filter(|(_, final_)| !final_)
        .map(|(value, _)| value.text.text.as_str())
        .collect();
    assert_eq!(interims, vec!["A-part-1", "B-part", "A-part-2"]);
}

#[derive(Default)]
struct CaptureBackend {
    starts: AtomicUsize,
    stops: AtomicUsize,
}
#[async_trait]
impl InputBackend for CaptureBackend {
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError> {
        if format.sample_rate() != 24_000 || format.channels() != 1 {
            return Err(ProcessorError::new(
                "fixture expected explicit 24 kHz output format",
            ));
        }
        self.starts.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn capture(&self, _: InputIngress) -> Result<(), ProcessorError> {
        std::future::pending().await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        self.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn real_input_transport_resamples_captured_user_pcm_16k_to_24k_including_tail() {
    let backend = Arc::new(CaptureBackend::default());
    let input = Arc::new(
        InputTransport::new(
            "captured-stt-input",
            InputTransportConfig::default(),
            backend.clone(),
            None,
        )
        .unwrap(),
    );
    let ingress = input.ingress();
    let mut case = Case::configured(|_| {}, vec![input]).await.unwrap();
    let samples: Vec<i16> = (0..501).map(|index| (index * 40 - 10_000) as i16).collect();
    let expected = resample_pcm16(&encode_pcm16_le(&samples), 16_000, 24_000, 1).unwrap();
    let scenario = bounded("captured resampling", async {
        while !ingress.is_ready() {
            sleep(Duration::from_millis(2)).await;
        }
        for chunk in samples.chunks(37) {
            let audio =
                AudioRawFrame::new(encode_pcm16_le(chunk), 16_000, 1).map_err(|e| e.to_string())?;
            ingress
                .push(Frame::new(FrameData::UserAudio(UserAudioRawFrame {
                    audio,
                    user_id: "capture-speaker".into(),
                })))
                .map_err(|e| e.to_string())?;
        }
        // This queued control flushes the actual InputTransport resampler tail.
        case.pass(Frame::new(FrameData::FilterControl), Direction::Downstream)
            .await?;
        case.peer
            .wait(|log| client_audio(log).concat().len() == expected.len())
            .await?;
        require(
            client_audio(&case.peer.snapshot()).concat() == expected,
            "wire PCM differs from real resampler output",
        )?;
        case.pass(vad_stop(), Direction::Downstream).await?;
        case.peer.wait(|log| commit_count(log) == 1).await?;
        case.peer.event(committed("capture"))?;
        case.peer
            .event(completed("capture", "captured transcript"))?;
        case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
            .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(client_audio(&log).concat(), expected);
    assert_eq!(backend.starts.load(Ordering::SeqCst), 1);
    assert_eq!(backend.stops.load(Ordering::SeqCst), 1);
    assert!(frames.iter().any(|frame| matches!(&frame.frame.data, FrameData::UserAudio(user) if user.audio.sample_rate == 24_000 && user.user_id == "capture-speaker")));
    assert_eq!(finals(&frames), vec!["captured transcript"]);
}

#[tokio::test]
async fn wrong_rate_stereo_odd_pcm_and_oversized_audio_never_reach_provider() {
    let mut case = Case::configured(|config| config.max_audio_bytes = 64, vec![])
        .await
        .unwrap();
    let invalid = [
        AudioRawFrame {
            audio: vec![0; 8],
            sample_rate: 16_000,
            num_channels: 1,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![0; 8],
            sample_rate: 24_000,
            num_channels: 2,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![0; 3],
            sample_rate: 24_000,
            num_channels: 1,
            context_id: None,
        },
        AudioRawFrame {
            audio: vec![0; 66],
            sample_rate: 24_000,
            num_channels: 1,
            context_id: None,
        },
    ];
    let mut original = Frame::new(FrameData::LlmResponseStart);
    original
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(686801, LlmContinuationProvider::Gemini).unwrap(),
        ))
        .unwrap();
    let scenario = bounded("invalid input rejection", async {
        // The untouched native non-audio carrier remains local and transparent.
        case.pass(original.clone(), Direction::Downstream).await?;
        // Mutating a carrier's public payload must not turn sealed native
        // semantics into ordinary outbound audio or a new remote commit.
        for data in [
            audio(false, &[1, 2]).data,
            audio(true, &[1, 2]).data,
            vad_stop().data,
        ] {
            let mut mutated = original.clone();
            mutated.data = data;
            case.send(mutated, Direction::Downstream).await?;
            case.receive(|frame| matches!(frame.frame.data, FrameData::Error(_)))
                .await?;
        }
        for audio in invalid {
            let count = case.worker.total_errors();
            case.send(
                Frame::new(FrameData::InputAudio(audio)),
                Direction::Downstream,
            )
            .await?;
            case.receive(|frame| matches!(frame.frame.data, FrameData::Error(_)))
                .await?;
            require(
                case.worker.total_errors() > count,
                "invalid PCM missing pipeline error",
            )?;
        }
        case.pass(audio(false, &[5, -5]), Direction::Downstream)
            .await?;
        case.peer.wait(|log| client_audio(log).len() == 1).await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 7);
    let forwarded = frames
        .iter()
        .find(|frame| {
            frame.frame.id == original.id && matches!(frame.frame.data, FrameData::LlmResponseStart)
        })
        .unwrap();
    assert!(forwarded.frame.native_llm_continuation().unwrap().is_some());
    assert_eq!(client_audio(&log), vec![encode_pcm16_le(&[5, -5])]);
    assert_eq!(commit_count(&log), 0);
}

#[tokio::test]
async fn interruption_preserves_the_input_service_session_and_pending_user_segment() {
    let mut case = Case::new().await.unwrap();
    let scenario = bounded("interruption and same user segment", async {
        case.audio_commit("same-user").await?;
        case.pass(Frame::interruption(), Direction::Downstream)
            .await?;
        case.peer.event(delta("same-user", "after interruption"))?;
        case.peer
            .event(completed("same-user", "same segment completed"))?;
        case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
            .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(log.requests.len(), 1);
    assert_eq!(finals(&frames), vec!["same segment completed"]);
}

#[tokio::test]
async fn cancel_closes_pending_socket_without_emitting_late_transcription() {
    let mut case = Case::new().await.unwrap();
    let scenario = bounded("cancel pending segment", async {
        case.audio_commit("cancelled").await?;
        case.pass(
            Frame::cancel("synthetic cancellation"),
            Direction::Downstream,
        )
        .await?;
        // Cancel has traversed the service before this late provider event.
        let _ = case
            .peer
            .event(completed("cancelled", "late cancelled final"));
        case.peer.wait(|log| log.closed == 1).await
    })
    .await;
    let witness = case.witness.clone();
    let (frames, log, report) = case
        .finish(
            scenario,
            FrameData::Cancel(CancelFrame {
                reason: Some("synthetic cancellation".into()),
            }),
        )
        .await;
    assert_clean(&report, WorkerState::Cancelled);
    assert_eq!(log.closed, 1);
    assert!(finals(&frames).is_empty());
    assert_eq!(witness.cleanup.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn health_generation_retirement_closes_old_socket_and_fences_late_results() {
    let mut case = Case::new().await.unwrap();
    let scenario = bounded("health retirement", async {
        case.audio_commit("retired").await?;
        case.stt
            .health()
            .ok_or("missing stable STT health")?
            .set_usable(false);
        // Send immediately after the public retirement boundary. Whether the
        // peer writes first or observes Close first, this must never be output.
        let _ = case
            .peer
            .event(completed("retired", "must not escape retired generation"));
        case.peer.wait(|log| log.closed == 1).await?;
        case.pass(
            Frame::text("retirement downstream barrier"),
            Direction::Downstream,
        )
        .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(log.requests.len(), 1);
    assert!(finals(&frames).is_empty());
}

#[tokio::test]
async fn end_and_stop_drain_committed_items_without_committing_unfinished_audio() {
    for terminal in [Frame::end().data, FrameData::Stop] {
        let mut case = Case::new().await.unwrap();
        let state = if matches!(terminal, FrameData::End(_)) {
            WorkerState::Ended
        } else {
            WorkerState::Stopped
        };
        let scenario = bounded("graceful committed drain", async {
            case.audio_commit("committed").await?;
            case.pass(audio(false, &[77, -77]), Direction::Downstream)
                .await?;
            case.peer.wait(|log| client_audio(log).len() == 2).await?;
            // Admit the terminal first and hold the provider final. The real
            // worker must remain alive until the committed result arrives.
            case.send(Frame::new(terminal.clone()), Direction::Downstream)
                .await?;
            sleep(QUIET).await;
            require(
                !case.worker.has_finished(),
                "graceful shutdown discarded committed work",
            )?;
            case.peer
                .event(completed("committed", "drained committed result"))?;
            case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
                .await
        })
        .await;
        // Terminal is already admitted. Avoid a second terminal by using the
        // common finish path's still-owned worker join directly below.
        let joined = timeout(WAIT, async {
            while let Some(frame) = case.worker.recv().await {
                case.observed.push(frame);
            }
            case.worker.join().await
        })
        .await;
        let report = match joined {
            Ok(result) => result.map_err(|e| e.to_string()),
            Err(_) => {
                let _ = case.worker.cancel("graceful drain deadline").await;
                let result = case.worker.join().await;
                Err(format!("drain deadline; joined: {result:?}"))
            }
        };
        let peer = case.peer.close().await;
        scenario.unwrap();
        peer.unwrap();
        assert_clean(&report.unwrap(), state);
        assert_eq!(commit_count(&case.peer.snapshot()), 1);
        assert_eq!(finals(&case.observed), vec!["drained committed result"]);
    }
}

#[tokio::test]
async fn end_without_vad_commit_closes_without_synthetic_commit_or_transcript() {
    let mut case = Case::new().await.unwrap();
    let scenario = bounded("uncommitted end", async {
        case.pass(audio(false, &[123, -123]), Direction::Downstream)
            .await?;
        case.peer.wait(|log| client_audio(log).len() == 1).await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(commit_count(&log), 0);
    assert!(finals(&frames).is_empty());
}

#[tokio::test]
async fn held_session_updated_gates_admitted_pipeline_audio_until_ready() {
    let mut peer = Peer::new(1, true).await.unwrap();
    let stt = Arc::new(OpenAiRealtimeSttProcessor::new(
        OpenAiSttClient::new(config(&peer)).unwrap(),
    ));
    let witness = Arc::new(Witness::default());
    let worker = match Pipeline::new(vec![stt.clone(), witness.clone()])
        .start(pipeline_config())
        .await
    {
        Ok(worker) => worker,
        Err(error) => {
            let closed = peer.close().await;
            panic!("start: {error}; teardown: {closed:?}");
        }
    };
    let mut case = Case {
        peer,
        stt,
        witness,
        worker,
        observed: vec![],
    };
    let scenario = bounded("held ready gate", async {
        case.peer.wait(|log| log.updates.len() == 1).await?;
        case.pass(audio(false, &[7, 6, 5]), Direction::Downstream)
            .await?;
        sleep(QUIET).await;
        require(
            case.peer.snapshot().commands.is_empty(),
            "audio escaped before session.updated",
        )?;
        case.peer
            .commands
            .send(PeerCommand::Ready)
            .map_err(|_| "peer closed before ready".to_string())?;
        case.peer.wait(|log| client_audio(log).len() == 1).await
    })
    .await;
    let (_, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(client_audio(&log), vec![encode_pcm16_le(&[7, 6, 5])]);
}

#[tokio::test]
async fn owner_drop_awaits_real_core_cleanup_before_reusing_the_same_processor() {
    let mut peer = Peer::new(2, false).await.unwrap();
    let stt = Arc::new(
        OpenAiRealtimeSttProcessor::new(OpenAiSttClient::new(config(&peer)).unwrap())
            .with_user_id("reused-speaker")
            .unwrap(),
    );
    let witness = Arc::new(Witness::default());
    let worker = match Pipeline::new(vec![stt.clone(), witness.clone()])
        .start(pipeline_config())
        .await
    {
        Ok(worker) => worker,
        Err(error) => {
            let closed = peer.close().await;
            panic!("start: {error}; teardown: {closed:?}");
        }
    };
    let mut case = Case {
        peer,
        stt: stt.clone(),
        witness: witness.clone(),
        worker,
        observed: vec![],
    };
    let scenario = bounded("owner-drop setup", case.audio_commit("old-owner")).await;
    let Case {
        mut peer, worker, ..
    } = case;
    drop(worker);
    let cleaned = bounded("real core cleanup witness", async {
        while witness.cleanup.load(Ordering::SeqCst) != 1 {
            sleep(Duration::from_millis(2)).await;
        }
        peer.wait(|log| log.closed == 1).await
    })
    .await;
    if scenario.is_err() || cleaned.is_err() {
        let closed = peer.close().await;
        panic!("owner-drop: {scenario:?}; real cleanup: {cleaned:?}; peer: {closed:?}");
    }
    let old_frames = witness.frames.lock().unwrap().clone();
    let next_witness = Arc::new(Witness::default());
    let worker = match Pipeline::new(vec![stt.clone(), next_witness.clone()])
        .start(pipeline_config())
        .await
    {
        Ok(worker) => worker,
        Err(error) => {
            let closed = peer.close().await;
            panic!("reuse: {error}; teardown: {closed:?}");
        }
    };
    let mut case = Case {
        peer,
        stt,
        witness: next_witness,
        worker,
        observed: vec![],
    };
    let scenario = bounded("new owner only", async {
        case.audio_commit("new-owner").await?;
        case.peer
            .event(completed("new-owner", "new owner result"))?;
        case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
            .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(log.requests.len(), 2);
    assert_eq!(log.closed, 2);
    assert_eq!(finals(&frames), vec!["new owner result"]);
    assert!(finals(&old_frames).is_empty());
    assert_eq!(
        log.commands
            .iter()
            .filter(|(connection, _)| *connection == 1)
            .count(),
        2,
        "new socket must not replay the old segment"
    );
}

// Fault injection is local to a single ProcessorContext: background capacity is
// per node, so a separate preceding processor cannot exhaust the STT reader's
// slot. The wrapper never performs cleanup outside the core lifecycle.
struct SetupProbe {
    inner: Arc<OpenAiRealtimeSttProcessor>,
    occupy_slot: bool,
    occupied_dropped: Arc<AtomicUsize>,
    setup_error: Mutex<Option<ProcessorError>>,
    cleanup_error: Mutex<Option<ProcessorError>>,
    cleaned: AtomicUsize,
}
impl SetupProbe {
    fn new(inner: Arc<OpenAiRealtimeSttProcessor>, occupy_slot: bool) -> Self {
        Self {
            inner,
            occupy_slot,
            occupied_dropped: Arc::new(AtomicUsize::new(0)),
            setup_error: Mutex::new(None),
            cleanup_error: Mutex::new(None),
            cleaned: AtomicUsize::new(0),
        }
    }
    async fn cleaned_by_core(&self) -> TestResult {
        bounded("actual startup cleanup", async {
            while self.cleaned.load(Ordering::SeqCst) == 0 {
                sleep(Duration::from_millis(2)).await;
            }
            let error = self.cleanup_error.lock().unwrap().clone();
            require(error.is_none(), format!("STT cleanup failed: {error:?}"))
        })
        .await
    }
}
struct OccupiedSlot(Arc<AtomicUsize>);
impl Drop for OccupiedSlot {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for SetupProbe {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<ProcessorHealth> {
        self.inner.health()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.occupy_slot {
            // Construct before spawn so even an unpolled task's destruction is
            // observable. Only core cancellation is allowed to drop this task.
            let occupied = OccupiedSlot(self.occupied_dropped.clone());
            ctx.spawn("synthetic occupied STT reader slot", false, async move {
                let _occupied = occupied;
                std::future::pending::<()>().await;
                Ok(())
            })?;
        }
        let result = self.inner.setup(start, ctx).await;
        *self.setup_error.lock().unwrap() = result.as_ref().err().cloned();
        result
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.inner.cleanup().await;
        *self.cleanup_error.lock().unwrap() = result.as_ref().err().cloned();
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        result
    }
}

#[tokio::test]
async fn aborting_start_during_http_upgrade_waits_for_real_startup_cleanup_and_peer_eof() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let endpoint = format!("ws://{}/v1/realtime", listener.local_addr().unwrap());
    let headers_received = Arc::new(AtomicUsize::new(0));
    let peer_eof = Arc::new(AtomicUsize::new(0));
    let headers = headers_received.clone();
    let eof = peer_eof.clone();
    let mut server = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let mut request = Vec::new();
        let mut buffer = [0; 1024];
        loop {
            let count = socket.read(&mut buffer).await.map_err(|e| e.to_string())?;
            require(
                count != 0,
                "client closed before complete HTTP upgrade request",
            )?;
            request.extend_from_slice(&buffer[..count]);
            require(
                request.len() <= 16_384,
                "upgrade request exceeded fixture bound",
            )?;
            if request.windows(4).any(|part| part == b"\r\n\r\n") {
                break;
            }
        }
        let request = String::from_utf8(request).map_err(|e| e.to_string())?;
        require(
            request.starts_with("GET /v1/realtime?intent=transcription HTTP/1.1\r\n"),
            "unexpected upgrade target",
        )?;
        headers.store(1, Ordering::SeqCst);
        // Never return HTTP 101. The client remains inside its actual upgrade
        // await until Pipeline::start cancellation drops that owned operation.
        let count = socket.read(&mut buffer).await.map_err(|e| e.to_string())?;
        require(
            count == 0,
            "client sent data instead of closing held HTTP upgrade",
        )?;
        eof.store(1, Ordering::SeqCst);
        Ok(())
    });
    let mut settings = OpenAiSttConfig::new(KEY);
    settings.endpoint = endpoint;
    settings.limits.connect_timeout = Duration::from_secs(30);
    let stt = Arc::new(OpenAiRealtimeSttProcessor::new(
        OpenAiSttClient::new(settings).unwrap(),
    ));
    let probe = Arc::new(SetupProbe::new(stt, false));
    let wrapper = probe.clone();
    let mut config = pipeline_config();
    config.startup_timeout = Duration::from_secs(30);
    let mut start = tokio::spawn(async move { Pipeline::new(vec![wrapper]).start(config).await });
    let observed = bounded("held HTTP upgrade", async {
        while headers_received.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(2)).await;
        }
        require(!start.is_finished(), "startup escaped a held HTTP upgrade")
    })
    .await;
    start.abort();
    let aborted = match timeout(WAIT, &mut start).await {
        Ok(Err(error)) if error.is_cancelled() => Ok(()),
        Ok(Ok(Ok(mut worker))) => {
            let _ = worker
                .cancel("unexpected completed startup fixture cleanup")
                .await;
            let _ = worker.join().await;
            Err("startup unexpectedly returned a live worker".to_owned())
        }
        Ok(Ok(Err(error))) => Err(format!("startup returned before cancellation: {error}")),
        Ok(Err(error)) => Err(format!("startup join failed: {error}")),
        Err(_) => {
            start.abort();
            let _ = start.await;
            Err("startup abort join deadline".into())
        }
    };
    // abort(start) is only the first boundary. StartupCleanupGuard must finish
    // the attempted STT processor's real cleanup and the socket must reach EOF.
    let cleaned = probe.cleaned_by_core().await;
    let closed = join_owned(&mut server).await;
    observed.unwrap();
    aborted.unwrap();
    cleaned.unwrap();
    closed.unwrap();
    assert_eq!(peer_eof.load(Ordering::SeqCst), 1);
    assert_eq!(probe.cleaned.load(Ordering::SeqCst), 1);
    assert!(
        probe.setup_error.lock().unwrap().is_none(),
        "setup was aborted, not completed with an unrelated error"
    );
}

#[tokio::test]
async fn reader_spawn_capacity_rejection_joins_connected_session_and_rolls_back_for_reuse() {
    let mut peer = Peer::new(2, false).await.unwrap();
    let stt = Arc::new(OpenAiRealtimeSttProcessor::new(
        OpenAiSttClient::new(config(&peer)).unwrap(),
    ));
    let probe = Arc::new(SetupProbe::new(stt.clone(), true));
    let wrapper = probe.clone();
    let mut settings = pipeline_config();
    settings.max_background_tasks = 1;
    let mut start =
        tokio::spawn(async move { Ok(Pipeline::new(vec![wrapper]).start(settings).await) });
    let started = join_owned(&mut start).await;
    let failed = match started {
        Ok(Ok(mut worker)) => {
            let joined = timeout(WAIT, worker.join()).await;
            match joined {
                Ok(result) => result,
                Err(_) => {
                    let _ = worker.cancel("spawn rejection join deadline").await;
                    let joined = worker.join().await;
                    let closed = peer.close().await;
                    panic!("failed worker deadline; cancelled and joined: {joined:?}; peer: {closed:?}");
                }
            }
        }
        Ok(Err(error)) => Err(error),
        Err(error) => {
            let cleaned = probe.cleaned_by_core().await;
            let closed = peer.close().await;
            panic!("startup task: {error}; cleanup: {cleaned:?}; peer: {closed:?}");
        }
    };
    let observed = bounded("reader spawn rejection cleanup", async {
        probe.cleaned_by_core().await?;
        peer.wait(|log| log.closed == 1).await?;
        require(matches!(&failed, Err(PipelineError::Execution(message)) if message == "background task limit reached"), format!("unexpected failure: {failed:?}"))?;
        let error = probe.setup_error.lock().unwrap().clone().ok_or("missing typed setup error")?;
        require(error == ProcessorError::fatal("background task limit reached"), format!("unexpected typed setup error: {error:?}"))?;
        require(probe.occupied_dropped.load(Ordering::SeqCst) == 1, "core did not cancel and join occupied background slot")?;
        require(peer.snapshot().requests.len() == 1, "failed setup did not establish exactly one socket")?;
        require(stt.health().is_some_and(|health| health.is_usable()), "local capacity failure unexpectedly changed service health")
    }).await;
    if let Err(error) = observed {
        let closed = peer.close().await;
        panic!("spawn rejection: {error}; peer: {closed:?}");
    }
    // The prior worker has joined and core has completed real cleanup. Reuse
    // the same service without the capacity-injecting wrapper or manual cleanup.
    let witness = Arc::new(Witness::default());
    let worker = match Pipeline::new(vec![stt.clone(), witness.clone()])
        .start(pipeline_config())
        .await
    {
        Ok(worker) => worker,
        Err(error) => {
            let closed = peer.close().await;
            panic!("rollback reuse: {error}; peer: {closed:?}");
        }
    };
    let mut case = Case {
        peer,
        stt,
        witness,
        worker,
        observed: vec![],
    };
    let scenario = bounded("post-rollback transcription", async {
        case.audio_commit("after-rejected-spawn").await?;
        case.peer.event(completed(
            "after-rejected-spawn",
            "fresh reader after rollback",
        ))?;
        case.receive(|frame| transcript(frame).is_some_and(|(_, final_)| final_))
            .await
    })
    .await;
    let (frames, log, report) = case.finish(scenario, Frame::end().data).await;
    assert_clean(&report, WorkerState::Ended);
    assert_eq!(finals(&frames), vec!["fresh reader after rollback"]);
    assert_eq!(log.requests.len(), 2);
    assert_eq!(log.closed, 2);
    assert!(
        log.commands.iter().all(|(connection, _)| *connection == 1),
        "failed setup replayed or admitted audio"
    );
}
