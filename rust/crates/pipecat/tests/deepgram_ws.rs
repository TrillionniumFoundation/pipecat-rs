//! Synthetic Deepgram WebSocket TTS through the public facade and real Pipeline.
//! Each complete utterance owns a socket. These tests qualify PCM, lifecycle,
//! settings ownership and the absence of word evidence, not a live provider or
//! audible playback. Controlled Played receipts cannot invent heard history.
#![cfg(feature = "deepgram-ws")]

use futures_util::{FutureExt, SinkExt, StreamExt};
use pipecat::{
    core::*,
    speech::{
        deepgram::{DeepgramHttpClient, DeepgramSynthesisRequest},
        deepgram_ws::{DeepgramWsClient, DeepgramWsConfig},
        HttpTtsBackend, HttpTtsProcessor, SpeechConfig,
    },
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    future::Future,
    ops::{Deref, DerefMut},
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{mpsc, Semaphore},
    task::JoinHandle,
    time::{sleep, timeout},
};
use tokio_tungstenite::tungstenite::{self, Message};

const WAIT: Duration = Duration::from_secs(5);
const QUIET: Duration = Duration::from_millis(50);
const RATE: u32 = 24_000;
const KEY: &str = "synthetic-deepgram-ws-fixture";
type TestResult = Result<(), String>;

struct Clock(tokio::time::Instant);
impl ObserverClock for Clock {
    fn now_ns(&self) -> u64 {
        100_000_000 + self.0.elapsed().as_nanos() as u64
    }
}
struct OwnedWorker {
    inner: PipelineWorker,
    joined: bool,
}
impl Deref for OwnedWorker {
    type Target = PipelineWorker;
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}
impl DerefMut for OwnedWorker {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}
impl OwnedWorker {
    async fn collect_result(&mut self) -> Result<(Vec<Frame>, WorkerReport), PipelineError> {
        let mut frames = vec![];
        while let Some(output) = self.recv().await {
            frames.push(output.frame);
        }
        let result = self.join().await;
        self.joined = true;
        result.map(|report| (frames, report))
    }
    async fn collect(&mut self) -> (Vec<Frame>, WorkerReport) {
        self.collect_result()
            .await
            .expect("real pipeline supervisor must join")
    }
}
#[derive(Default)]
struct Owners {
    workers: Vec<OwnedWorker>,
    peers: Vec<Option<JoinHandle<TestResult>>>,
    gates: Vec<Arc<Semaphore>>,
    dropped: Vec<Arc<Witness>>,
    clients: Vec<DeepgramWsClient>,
}
impl Owners {
    async fn join_peer(&mut self, index: usize) {
        let task = self.peers[index].as_mut().expect("peer already joined");
        let result = match timeout(WAIT, &mut *task).await {
            Ok(result) => result,
            Err(_) => {
                task.abort();
                let _ = task.await;
                self.peers[index].take();
                panic!("loopback peer deadline; aborted and joined");
            }
        };
        self.peers[index].take();
        result
            .expect("loopback task panicked")
            .expect("loopback protocol");
    }
    async fn cleanup(&mut self) -> Vec<String> {
        let mut errors = vec![];
        // Release test-owned close gates before cancel/join, including unwind.
        // Never panic in teardown while another retained owner still needs joining.
        for gate in &self.gates {
            gate.add_permits(1024);
        }
        for worker in &mut self.workers {
            if worker.joined {
                continue;
            }
            if !worker.has_finished()
                && timeout(WAIT, worker.cancel("Deepgram WS fixture cleanup"))
                    .await
                    .is_err()
            {
                errors.push("pipeline cancellation admission deadline".into());
            }
            match timeout(WAIT, worker.collect_result()).await {
                Ok(Ok(_)) => {}
                Ok(Err(error)) => errors.push(format!("pipeline cleanup: {error}")),
                Err(_) => {
                    // Keep the supervisor in scope. Core has its own bounded
                    // shutdown; timeout must never consume its JoinHandle.
                    let _ = worker.cancel("Deepgram WS fixture join deadline").await;
                    let result = worker.join().await;
                    worker.joined = true;
                    errors.push(format!(
                        "pipeline deadline; cancelled and joined: {result:?}"
                    ));
                }
            }
        }
        for witness in &self.dropped {
            if timeout(WAIT, async {
                while witness.cleaned.load(Ordering::SeqCst) == 0 {
                    sleep(Duration::from_millis(2)).await;
                }
            })
            .await
            .is_err()
            {
                errors.push("dropped owner did not reach real core cleanup".into());
            }
        }
        for client in &self.clients {
            match timeout(WAIT, client.cleanup()).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => errors.push(format!("test-owned client cleanup: {error}")),
                Err(_) => {
                    // Actor cancellation and close are bounded by the client;
                    // retry only joining the same retained registry on timeout.
                    client.cancel_all();
                    let joined = client.cleanup().await;
                    errors.push(format!(
                        "test-owned client cleanup deadline; joined: {joined:?}"
                    ));
                }
            }
        }
        for handle in &mut self.peers {
            if let Some(mut task) = handle.take() {
                match timeout(WAIT, &mut task).await {
                    Ok(Ok(Ok(()))) => {}
                    Ok(Ok(Err(error))) => errors.push(format!("loopback teardown: {error}")),
                    Ok(Err(error)) => errors.push(format!("loopback task: {error}")),
                    Err(_) => {
                        task.abort();
                        let _ = task.await;
                        errors.push("loopback teardown deadline; aborted and joined".into());
                    }
                }
            }
        }
        errors
    }
}
async fn owned(run: impl for<'a> FnOnce(&'a mut Owners) -> Pin<Box<dyn Future<Output = ()> + 'a>>) {
    let mut owners = Owners::default();
    let result = timeout(WAIT * 6, AssertUnwindSafe(run(&mut owners)).catch_unwind()).await;
    let cleanup = owners.cleanup().await;
    assert!(
        cleanup.is_empty(),
        "fixture cleanup errors after all owners joined: {cleanup:?}"
    );
    match result {
        Ok(Ok(())) => {}
        Ok(Err(panic)) => std::panic::resume_unwind(panic),
        Err(_) => panic!("scenario deadline; retained peers and workers cleaned up"),
    }
}
#[cfg(feature = "assistant-history")]
async fn permit(gate: &Semaphore) {
    timeout(WAIT, gate.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}

#[derive(Clone)]
struct Plan {
    text: &'static str,
    pcm: Option<Vec<u8>>,
    late_on_clear: bool,
    close_gate: Option<Arc<Semaphore>>,
}
impl Plan {
    fn automatic(text: &'static str, pcm: &[u8]) -> Self {
        Self {
            text,
            pcm: Some(pcm.to_vec()),
            late_on_clear: false,
            close_gate: None,
        }
    }
    fn held(text: &'static str) -> Self {
        Self {
            text,
            pcm: None,
            late_on_clear: false,
            close_gate: None,
        }
    }
}
#[derive(Clone, Debug, Default)]
struct SessionLog {
    uri: String,
    authorization: String,
    commands: Vec<Value>,
    ws_close: bool,
    close_flushed: bool,
    late_writes: usize,
}
impl SessionLog {
    fn text(&self) -> Option<&str> {
        self.commands
            .iter()
            .find(|v| v["type"] == "Speak")
            .and_then(|v| v["text"].as_str())
    }
    fn count(&self, kind: &str) -> usize {
        self.commands.iter().filter(|v| v["type"] == kind).count()
    }
}
#[derive(Clone)]
struct Peer {
    endpoint: String,
    sessions: Arc<Mutex<Vec<SessionLog>>>,
    controls: Arc<Mutex<BTreeMap<String, mpsc::UnboundedSender<Vec<u8>>>>>,
    handles: Vec<usize>,
}
impl Peer {
    #[allow(clippy::result_large_err)] // tungstenite's handshake callback API.
    async fn new(owners: &mut Owners, plans: Vec<Plan>) -> Self {
        let listener = Arc::new(TcpListener::bind("127.0.0.1:0").await.unwrap());
        let endpoint = format!("ws://{}/v1/speak", listener.local_addr().unwrap());
        let sessions = Arc::new(Mutex::new(Vec::<SessionLog>::new()));
        let controls = Arc::new(Mutex::new(
            BTreeMap::<String, mpsc::UnboundedSender<Vec<u8>>>::new(),
        ));
        let count = plans.len();
        for plan in &plans {
            if let Some(gate) = &plan.close_gate {
                owners.gates.push(gate.clone());
            }
        }
        let plans = Arc::new(
            plans
                .into_iter()
                .map(|p| (p.text, p))
                .collect::<BTreeMap<_, _>>(),
        );
        let mut handles = vec![];
        for _ in 0..count {
            let listener = listener.clone();
            let observed = sessions.clone();
            let controls = controls.clone();
            let plans = plans.clone();
            handles.push(owners.peers.len());
            owners.peers.push(Some(tokio::spawn(async move {
                let (tcp, _) = listener.accept().await.map_err(|e| e.to_string())?;
                let index = {
                    let mut log = observed.lock().unwrap();
                    let index = log.len();
                    log.push(SessionLog::default());
                    index
                };
                let headers = observed.clone();
                let mut socket = tokio_tungstenite::accept_hdr_async(tcp,
                    move |request: &tungstenite::handshake::server::Request, response| {
                        let mut log = headers.lock().unwrap();
                        log[index].uri = request.uri().to_string();
                        log[index].authorization = request.headers().get("authorization")
                            .and_then(|v| v.to_str().ok()).unwrap_or_default().into();
                        Ok(response)
                    }).await.map_err(|e| e.to_string())?;
                let (tx, mut rx) = mpsc::unbounded_channel::<Vec<u8>>();
                let mut plan: Option<Plan> = None;
                loop {
                    tokio::select! {
                        pcm = rx.recv() => {
                            let pcm = pcm.ok_or("fixture control unexpectedly closed")?;
                            complete(&mut socket, &pcm).await?;
                        }
                        event = socket.next() => match event {
                            Some(Ok(Message::Text(text))) => {
                                let value: Value = serde_json::from_str(&text).map_err(|e| e.to_string())?;
                                observed.lock().unwrap()[index].commands.push(value.clone());
                                match value["type"].as_str() {
                                    Some("Speak") => {
                                        let text = value["text"].as_str().ok_or("Speak missing text")?;
                                        let chosen = plans.get(text).ok_or_else(|| format!("unexpected synthesis: {text}"))?.clone();
                                        if plan.is_some() { return Err("second Speak on one socket".into()); }
                                        controls.lock().unwrap().insert(text.into(), tx.clone());
                                        plan = Some(chosen);
                                    }
                                    Some("Flush") => {
                                        if let Some(pcm) = &plan.as_ref().ok_or("Flush before Speak")?.pcm {
                                            complete(&mut socket, pcm).await?;
                                        }
                                    }
                                    Some("Clear") => {
                                        if plan.as_ref().is_some_and(|p| p.late_on_clear) {
                                            // Deliberately inject old untagged PCM and Flushed
                                            // after the client's retirement is on the wire.
                                            for event in [Message::Binary(vec![0xee, 0x7f].into()),
                                                Message::Text(json!({"type":"Flushed"}).to_string().into())] {
                                                if socket.send(event).await.is_ok() {
                                                    observed.lock().unwrap()[index].late_writes += 1;
                                                }
                                            }
                                        }
                                    }
                                    Some("Close") => {},
                                    _ => return Err("unexpected client protocol command".into()),
                                }
                            }
                            Some(Ok(Message::Close(_))) => {
                                observed.lock().unwrap()[index].ws_close = true;
                                if let Some(gate) = plan.as_ref().and_then(|p| p.close_gate.as_ref()) {
                                    gate.acquire().await.map_err(|e| e.to_string())?.forget();
                                }
                                // Reading Close already queued the automatic reply.
                                // Flush that reply; a second close() can skip it.
                                socket.flush().await.map_err(|e| e.to_string())?;
                                observed.lock().unwrap()[index].close_flushed = true;
                                return Ok(());
                            }
                            Some(Ok(Message::Ping(_))) => socket.flush().await.map_err(|e| e.to_string())?,
                            Some(Ok(_)) => {},
                            Some(Err(error)) => return Err(format!("peer before close reply: {error}")),
                            None => return Err("peer EOF without owned close handshake".into()),
                        }
                    }
                }
            })));
        }
        Self {
            endpoint,
            sessions,
            controls,
            handles,
        }
    }
    fn snapshot(&self) -> Vec<SessionLog> {
        self.sessions.lock().unwrap().clone()
    }
    fn session(&self, text: &str) -> SessionLog {
        self.snapshot()
            .into_iter()
            .find(|v| v.text() == Some(text))
            .expect("missing wire session")
    }
    async fn wait(&self, predicate: impl Fn(&[SessionLog]) -> bool) {
        timeout(WAIT, async {
            while !predicate(&self.snapshot()) {
                sleep(Duration::from_millis(2)).await;
            }
        })
        .await
        .expect("loopback observation deadline");
    }
    async fn flushed(&self, text: &str) {
        self.wait(|v| {
            v.iter()
                .any(|s| s.text() == Some(text) && s.count("Flush") == 1)
        })
        .await;
    }
    async fn closed(&self, text: &str) {
        self.wait(|v| v.iter().any(|s| s.text() == Some(text) && s.close_flushed))
            .await;
    }
    fn complete(&self, text: &str, pcm: &[u8]) {
        self.controls
            .lock()
            .unwrap()
            .get(text)
            .expect("session control missing")
            .send(pcm.to_vec())
            .unwrap();
    }
    async fn join(&self, owners: &mut Owners) {
        for &index in &self.handles {
            owners.join_peer(index).await;
        }
    }
}
async fn complete<S>(socket: &mut tokio_tungstenite::WebSocketStream<S>, pcm: &[u8]) -> TestResult
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    // Odd wire boundaries exercise PCM16 carry without changing exact output.
    for chunk in pcm.chunks(3) {
        socket
            .send(Message::Binary(chunk.to_vec().into()))
            .await
            .map_err(|e| e.to_string())?;
    }
    socket
        .send(Message::Text(json!({"type":"Flushed"}).to_string().into()))
        .await
        .map_err(|e| e.to_string())
}
fn client(peer: &Peer) -> DeepgramWsClient {
    let mut config = DeepgramWsConfig::new(KEY);
    config.endpoint = peer.endpoint.clone();
    config.limits.connect_timeout = Duration::from_secs(2);
    config.limits.write_timeout = Duration::from_secs(2);
    config.limits.response_timeout = Duration::from_secs(10);
    config.limits.close_timeout = Duration::from_secs(2);
    DeepgramWsClient::new(config).unwrap()
}
fn processor(client: DeepgramWsClient) -> Arc<HttpTtsProcessor> {
    Arc::new(
        HttpTtsProcessor::new(
            HttpTtsBackend::DeepgramWebSocket {
                client,
                request: DeepgramSynthesisRequest::new(""),
            },
            16,
        )
        .unwrap(),
    )
}
fn pipeline_config() -> PipelineConfig {
    let mut config = PipelineConfig {
        startup_timeout: Duration::from_secs(2),
        shutdown_timeout: Duration::from_secs(3),
        ..Default::default()
    };
    config.start.audio_out_sample_rate = RATE;
    config
}
async fn pipeline(owners: &mut Owners, nodes: Vec<Arc<dyn FrameProcessor>>) -> usize {
    let inner = Pipeline::new(nodes)
        .with_observer_clock(Arc::new(Clock(tokio::time::Instant::now())))
        .start(pipeline_config())
        .await
        .unwrap();
    let index = owners.workers.len();
    owners.workers.push(OwnedWorker {
        inner,
        joined: false,
    });
    index
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
async fn receive(worker: &mut PipelineWorker, predicate: impl Fn(&Frame) -> bool) -> Vec<Frame> {
    timeout(WAIT, async {
        let mut frames = vec![];
        loop {
            let frame = worker
                .recv()
                .await
                .expect("pipeline closed before barrier")
                .frame;
            let done = predicate(&frame);
            frames.push(frame);
            if done {
                return frames;
            }
        }
    })
    .await
    .expect("pipeline observation deadline")
}
async fn marker(worker: &mut PipelineWorker) -> Vec<Frame> {
    let mut text = TextFrame::new("synthetic admission barrier");
    text.skip_tts = Some(true);
    let frame = Frame::new(FrameData::Text(text));
    let id = frame.id;
    send(worker, frame).await;
    receive(worker, |frame| frame.id == id).await
}
async fn finish(worker: &mut OwnedWorker, terminal: FrameData) -> (Vec<Frame>, WorkerReport) {
    send(worker, Frame::new(terminal)).await;
    timeout(WAIT, worker.collect())
        .await
        .expect("pipeline must close and join")
}
fn clean(report: &WorkerReport, state: WorkerState) {
    assert_eq!(report.state, state);
    assert!(
        !report.errors.iter().any(|e| e.fatal),
        "{:?}",
        report.errors
    );
}
fn pcm(frames: &[Frame]) -> Vec<u8> {
    frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsAudio(v) => Some(v.audio.clone()),
            _ => None,
        })
        .flatten()
        .collect()
}
fn unreported(frames: &[Frame], texts: &[&str]) {
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
    )));
    let diagnostics: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsAlignmentDiagnostic(v) => Some(v),
            _ => None,
        })
        .collect();
    assert_eq!(diagnostics.len(), texts.len());
    for (diagnostic, text) in diagnostics.iter().zip(texts) {
        assert_eq!(diagnostic.evidence, TtsAlignmentEvidence::Unreported);
        assert_eq!(diagnostic.synthesis_outcome, TtsSynthesisOutcome::Succeeded);
        assert_eq!(&diagnostic.synthesis_text, text);
    }
}
fn wire_closed(session: &SessionLog, cleared: bool) {
    assert_eq!(session.count("Speak"), 1);
    assert_eq!(session.count("Flush"), 1);
    assert_eq!(session.count("Close"), 1);
    assert_eq!(session.count("Clear"), usize::from(cleared));
    assert!(session.ws_close && session.close_flushed);
}
#[derive(Default)]
struct Witness {
    frames: Mutex<Vec<Frame>>,
    cleaned: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for Witness {
    fn name(&self) -> &str {
        "DeepgramWsDownstreamWitness"
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
        // Core calls this after TTS cleanup. Tests never manually invoke cleanup.
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
async fn cleaned(witness: &Witness) {
    timeout(WAIT, async {
        while witness.cleaned.load(Ordering::SeqCst) == 0 {
            sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("real core cleanup witness");
}

#[tokio::test]
async fn three_text_channels_exact_pcm_and_lifecycle_remain_synthesis_only() {
    owned(|owners| {
        Box::pin(async move {
            let peer = Peer::new(
                owners,
                vec![Plan::automatic(
                    "你好 one hundred",
                    &[1, 0, 2, 0, 3, 0, 4, 0],
                )],
            )
            .await;
            let tts = processor(client(&peer));
            let witness = Arc::new(Witness::default());
            let index = pipeline(owners, vec![tts, witness.clone()]).await;
            let worker = &mut owners.workers[index];
            let mut input = TtsTextInputFrame::new("你好 one hundred");
            input.display_text = "你好 100".into();
            input.original_text = "**你好 100**".into();
            input.append_to_context = false;
            input.aggregated_by = "paragraph".into();
            let mut source = Frame::new(FrameData::TtsTextInput(input));
            source.transport_destination = Some("secondary".into());
            source.transport_source = Some("private-local-source".into());
            source
                .metadata
                .insert("private-input-envelope".into(), json!(true));
            let source_id = source.id;
            send(worker, source).await;
            send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
            let (frames, report) = finish(worker, Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(witness.cleaned.load(Ordering::SeqCst), 1);
            assert_eq!(pcm(&frames), [1, 0, 2, 0, 3, 0, 4, 0]);
            unreported(&frames, &["你好 one hundred"]);
            let session = peer.session("你好 one hundred");
            wire_closed(&session, false);
            assert_eq!(session.authorization, format!("Token {KEY}"));
            assert_eq!(
                session.uri,
                "/v1/speak?model=aura-2-helena-en&encoding=linear16&sample_rate=24000"
            );
            assert_eq!(
                session.commands[0],
                json!({"type":"Speak","text":"你好 one hundred"})
            );
            assert!(!serde_json::to_string(&session.commands)
                .unwrap()
                .contains("private-"));
            let anchors: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.data {
                    FrameData::AggregatedText(v) => Some((f, v)),
                    _ => None,
                })
                .collect();
            assert_eq!(anchors.len(), 1);
            let (anchor, text) = anchors[0];
            assert_eq!(text.text.text, "你好 100");
            assert_eq!(text.raw_text.as_deref(), Some("**你好 100**"));
            assert_eq!(text.aggregated_by, "paragraph");
            assert!(!text.text.append_to_context);
            for frame in frames.iter().filter(|f| {
                matches!(
                    f.data,
                    FrameData::TtsAudio(_)
                        | FrameData::AggregatedText(_)
                        | FrameData::TtsAlignmentDiagnostic(_)
                )
            }) {
                assert_eq!(frame.transport_destination.as_deref(), Some("secondary"));
                assert_eq!(frame.metadata["source_frame_id"], json!(source_id));
                if let FrameData::TtsAudio(audio) = &frame.data {
                    assert_eq!((audio.sample_rate, audio.num_channels), (RATE, 1));
                    assert_eq!(audio.context_id, text.context_id);
                }
                if let FrameData::TtsAlignmentDiagnostic(diagnostic) = &frame.data {
                    assert_eq!(diagnostic.segment_id, Some(anchor.id));
                    assert_eq!(Some(&diagnostic.context_id), text.context_id.as_ref());
                    assert_eq!(diagnostic.original_text.as_deref(), Some("**你好 100**"));
                }
            }
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.data, FrameData::TtsStarted(_)))
                    .count(),
                1
            );
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.data, FrameData::TtsStopped(_)))
                    .count(),
                1
            );
            assert!(!frames
                .iter()
                .any(|f| matches!(f.data, FrameData::Error(_) | FrameData::BotStartedSpeaking)));
        })
    })
    .await;
}

#[tokio::test]
async fn sequential_utterances_end_waits_for_close_reply_and_actor_join() {
    owned(|owners| {
        Box::pin(async move {
            let gate = Arc::new(Semaphore::new(0));
            let mut first = Plan::automatic("first", &[1, 0]);
            first.close_gate = Some(gate.clone());
            let peer = Peer::new(owners, vec![first, Plan::automatic("second", &[2, 0])]).await;
            let witness = Arc::new(Witness::default());
            let tts = processor(client(&peer));
            let index = pipeline(owners, vec![tts, witness.clone()]).await;
            let worker = &mut owners.workers[index];
            speak(worker, "first").await;
            speak(worker, "second").await;
            send(worker, Frame::new(FrameData::LlmResponseEnd)).await;
            worker.end().await.unwrap();
            peer.wait(|log| log.iter().any(|s| s.text() == Some("first") && s.ws_close))
                .await;
            sleep(QUIET).await;
            assert!(!worker.has_finished());
            assert_eq!(
                peer.snapshot().len(),
                1,
                "next utterance must await previous owned close"
            );
            assert_eq!(witness.cleaned.load(Ordering::SeqCst), 0);
            assert!(!witness.frames.lock().unwrap().iter().any(|f| matches!(
                f.data,
                FrameData::TtsStopped(_) | FrameData::TtsAlignmentDiagnostic(_)
            )));
            gate.add_permits(1);
            let (frames, report) = timeout(WAIT, worker.collect()).await.unwrap();
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [1, 0, 2, 0]);
            unreported(&frames, &["first", "second"]);
            for text in ["first", "second"] {
                wire_closed(&peer.session(text), false);
            }
            let contexts: Vec<_> = frames
                .iter()
                .filter_map(|f| match &f.data {
                    FrameData::TtsAudio(v) => v.context_id.clone(),
                    _ => None,
                })
                .collect();
            assert_eq!(contexts.len(), 2);
            assert_ne!(contexts[0], contexts[1]);
        })
    })
    .await;
}

#[tokio::test]
async fn stop_joins_without_core_cleanup_and_same_arc_starts_a_fresh_run() {
    owned(|owners| {
        Box::pin(async move {
            let gate = Arc::new(Semaphore::new(0));
            let mut stopped = Plan::automatic("before-stop", &[3, 0]);
            stopped.close_gate = Some(gate.clone());
            let peer = Peer::new(
                owners,
                vec![stopped, Plan::automatic("after-stop", &[4, 0])],
            )
            .await;
            let tts = processor(client(&peer));
            let witness = Arc::new(Witness::default());
            let index = pipeline(owners, vec![tts.clone(), witness.clone()]).await;
            let worker = &mut owners.workers[index];
            speak(worker, "before-stop").await;
            send(worker, Frame::new(FrameData::Stop)).await;
            peer.wait(|log| {
                log.iter()
                    .any(|s| s.text() == Some("before-stop") && s.ws_close)
            })
            .await;
            sleep(QUIET).await;
            assert!(
                !worker.has_finished(),
                "Stop must await WS terminal ownership"
            );
            gate.add_permits(1);
            let (frames, report) = timeout(WAIT, worker.collect()).await.unwrap();
            clean(&report, WorkerState::Stopped);
            assert_eq!(
                witness.cleaned.load(Ordering::SeqCst),
                0,
                "core deliberately skips cleanup on Stop"
            );
            assert_eq!(pcm(&frames), [3, 0]);
            unreported(&frames, &["before-stop"]);
            let index = pipeline(owners, vec![tts.clone(), witness.clone()]).await;
            let worker = &mut owners.workers[index];
            speak(worker, "after-stop").await;
            let (frames, report) = finish(worker, Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [4, 0]);
            unreported(&frames, &["after-stop"]);
            assert_eq!(witness.cleaned.load(Ordering::SeqCst), 1);
            for text in ["before-stop", "after-stop"] {
                wire_closed(&peer.session(text), false);
            }
        })
    })
    .await;
}

#[tokio::test]
async fn interruption_retires_held_socket_and_late_pcm_cannot_enter_fresh_context() {
    owned(|owners| {
        Box::pin(async move {
            let mut old = Plan::held("old-active");
            old.late_on_clear = true;
            let peer = Peer::new(owners, vec![old, Plan::automatic("fresh", &[9, 0, 10, 0])]).await;
            let tts = processor(client(&peer));
            let index = pipeline(owners, vec![tts]).await;
            let worker = &mut owners.workers[index];
            speak(worker, "old-active").await;
            peer.flushed("old-active").await;
            speak(worker, "queued-must-not-run").await;
            marker(worker).await;
            send(worker, Frame::new(FrameData::Interruption)).await;
            receive(worker, |f| matches!(f.data, FrameData::Interruption)).await;
            speak(worker, "fresh").await;
            let (frames, report) = finish(worker, Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [9, 0, 10, 0]);
            unreported(&frames, &["fresh"]);
            let old = peer.session("old-active");
            wire_closed(&old, true);
            assert_eq!(
                old.late_writes, 2,
                "late old provider data must actually be sent"
            );
            wire_closed(&peer.session("fresh"), false);
            assert_eq!(peer.snapshot().len(), 2);
            assert!(!frames.iter().any(|f| matches!(f.data, FrameData::Error(_))));
        })
    })
    .await;
}

#[tokio::test]
async fn cancel_closes_active_socket_and_core_cleanup_joins_before_same_arc_reuse() {
    owned(|owners| {
        Box::pin(async move {
            let peer = Peer::new(
                owners,
                vec![
                    Plan::held("cancel-active"),
                    Plan::automatic("after-cancel", &[5, 0]),
                ],
            )
            .await;
            let tts = processor(client(&peer));
            let witness = Arc::new(Witness::default());
            let index = pipeline(owners, vec![tts.clone(), witness.clone()]).await;
            let worker = &mut owners.workers[index];
            speak(worker, "cancel-active").await;
            peer.flushed("cancel-active").await;
            speak(worker, "cancel-queued-must-not-run").await;
            marker(worker).await;
            worker.cancel("synthetic cancellation").await.unwrap();
            let (frames, report) = timeout(WAIT, worker.collect()).await.unwrap();
            clean(&report, WorkerState::Cancelled);
            cleaned(&witness).await;
            peer.closed("cancel-active").await;
            assert!(pcm(&frames).is_empty());
            unreported(&frames, &[]);
            let index = pipeline(owners, vec![tts]).await;
            speak(&owners.workers[index], "after-cancel").await;
            let (frames, report) = finish(&mut owners.workers[index], Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [5, 0]);
            unreported(&frames, &["after-cancel"]);
            wire_closed(&peer.session("cancel-active"), true);
        })
    })
    .await;
}

#[tokio::test]
async fn owner_drop_witnesses_real_core_cleanup_before_processor_reuse() {
    owned(|owners| {
        Box::pin(async move {
            let peer = Peer::new(
                owners,
                vec![
                    Plan::held("drop-active"),
                    Plan::automatic("after-drop", &[6, 0]),
                ],
            )
            .await;
            let tts = processor(client(&peer));
            let witness = Arc::new(Witness::default());
            let index = pipeline(owners, vec![tts.clone(), witness.clone()]).await;
            speak(&owners.workers[index], "drop-active").await;
            peer.flushed("drop-active").await;
            speak(&owners.workers[index], "drop-queued-must-not-run").await;
            marker(&mut owners.workers[index]).await;
            owners.dropped.push(witness.clone());
            // Specifically exercise PipelineWorker::drop. The downstream witness
            // observes core cleanup after all TTS-owned actors are joined.
            drop(owners.workers.pop().unwrap());
            cleaned(&witness).await;
            peer.closed("drop-active").await;
            assert!(pcm(&witness.frames.lock().unwrap()).is_empty());
            let index = pipeline(owners, vec![tts]).await;
            speak(&owners.workers[index], "after-drop").await;
            let (frames, report) = finish(&mut owners.workers[index], Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [6, 0]);
            unreported(&frames, &["after-drop"]);
            wire_closed(&peer.session("drop-active"), true);
        })
    })
    .await;
}

#[tokio::test]
async fn health_retirement_closes_and_joins_old_generation_without_late_pcm() {
    owned(|owners| {
        Box::pin(async move {
            let mut retired = Plan::held("retired");
            retired.late_on_clear = true;
            let peer = Peer::new(owners, vec![retired]).await;
            let tts = processor(client(&peer));
            let witness = Arc::new(Witness::default());
            let index = pipeline(owners, vec![tts.clone(), witness.clone()]).await;
            speak(&owners.workers[index], "retired").await;
            peer.flushed("retired").await;
            tts.health().unwrap().set_usable(false);
            peer.closed("retired").await;
            let (frames, report) = finish(&mut owners.workers[index], Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            cleaned(&witness).await;
            assert!(pcm(&frames).is_empty());
            unreported(&frames, &[]);
            let session = peer.session("retired");
            wire_closed(&session, true);
            assert_eq!(session.late_writes, 2);
        })
    })
    .await;
}

#[tokio::test]
async fn shared_external_client_two_processor_owners_do_not_cancel_each_other() {
    owned(|owners| {
        Box::pin(async move {
            let peer = Peer::new(
                owners,
                vec![
                    Plan::held("owner-a"),
                    Plan::held("owner-b"),
                    Plan::automatic("external-survives", &[8, 0]),
                ],
            )
            .await;
            let shared = client(&peer);
            owners.clients.push(shared.clone());
            let first = processor(shared.clone());
            let second = processor(shared.clone());
            let witness_a = Arc::new(Witness::default());
            let witness_b = Arc::new(Witness::default());
            let a = pipeline(owners, vec![first, witness_a.clone()]).await;
            let b = pipeline(owners, vec![second, witness_b.clone()]).await;
            speak(&owners.workers[a], "owner-a").await;
            speak(&owners.workers[b], "owner-b").await;
            peer.flushed("owner-a").await;
            peer.flushed("owner-b").await;
            owners.workers[a]
                .cancel("cancel only owner a")
                .await
                .unwrap();
            let (frames, report) = timeout(WAIT, owners.workers[a].collect()).await.unwrap();
            clean(&report, WorkerState::Cancelled);
            cleaned(&witness_a).await;
            assert!(pcm(&frames).is_empty());
            sleep(QUIET).await;
            let survivor = peer.session("owner-b");
            assert_eq!(survivor.count("Clear"), 0);
            assert!(!survivor.ws_close);
            assert!(!owners.workers[b].has_finished());
            peer.complete("owner-b", &[7, 0]);
            let (frames, report) = finish(&mut owners.workers[b], Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            cleaned(&witness_b).await;
            assert_eq!(pcm(&frames), [7, 0]);
            unreported(&frames, &["owner-b"]);
            // Owner cleanup also must not seal the caller's original client registry.
            let mut stream = shared
                .synthesize(DeepgramSynthesisRequest::new("external-survives"))
                .unwrap();
            let mut audio = vec![];
            while let Some(chunk) = timeout(WAIT, stream.next()).await.unwrap() {
                audio.extend(chunk.unwrap().pcm);
            }
            assert_eq!(audio, [8, 0]);
            peer.join(owners).await;
            wire_closed(&peer.session("owner-a"), true);
            wire_closed(&peer.session("owner-b"), false);
            wire_closed(&peer.session("external-survives"), false);
        })
    })
    .await;
}

fn settings(target: ProcessorReference, voice: &str) -> Frame {
    Frame::new(FrameData::TtsUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(TtsSettings {
                voice: Setting::Value(voice.into()),
                ..Default::default()
            }),
            service: Some(target),
            ..Default::default()
        },
    )))
}
#[tokio::test]
async fn local_and_name_targets_hit_real_instance_and_old_http_name_passes_through() {
    owned(|owners| {
        Box::pin(async move {
            let peer = Peer::new(
                owners,
                vec![
                    Plan::automatic("local", &[1, 0]),
                    Plan::automatic("named", &[2, 0]),
                ],
            )
            .await;
            let tts = processor(client(&peer));
            assert_eq!(tts.name(), "DeepgramWebSocketTtsService");
            let http = HttpTtsProcessor::new(
                HttpTtsBackend::Deepgram {
                    client: DeepgramHttpClient::new(SpeechConfig::deepgram("synthetic-http-key"))
                        .unwrap(),
                    request: DeepgramSynthesisRequest::new(""),
                },
                4,
            )
            .unwrap();
            assert_eq!(http.name(), "HttpTtsService");
            let index = pipeline(owners, vec![tts.clone()]).await;
            let worker = &mut owners.workers[index];
            let local = settings(
                ProcessorReference::Local(LocalProcessorReference(tts.clone())),
                "aura-2-asteria-en",
            );
            send(worker, local).await;
            speak(worker, "local").await;
            let named = settings(
                ProcessorReference::Name("DeepgramWebSocketTtsService".into()),
                "aura-2-luna-en",
            );
            send(worker, named).await;
            let old = settings(
                ProcessorReference::Name("HttpTtsService".into()),
                "must-not-be-applied",
            );
            let unrelated = settings(
                ProcessorReference::Name("UnrelatedService".into()),
                "also-must-not-be-applied",
            );
            for frame in [&old, &unrelated] {
                send(worker, frame.clone()).await;
            }
            speak(worker, "named").await;
            let (frames, report) = finish(worker, Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [1, 0, 2, 0]);
            unreported(&frames, &["local", "named"]);
            assert!(peer
                .session("local")
                .uri
                .contains("model=aura-2-asteria-en&"));
            assert!(peer.session("named").uri.contains("model=aura-2-luna-en&"));
            for expected in [&old, &unrelated] {
                let matches: Vec<_> = frames.iter().filter(|f| f.id == expected.id).collect();
                assert_eq!(matches.len(), 1);
                assert_eq!(matches[0].data, expected.data);
            }
            assert!(!frames.iter().any(|f| matches!(f.data, FrameData::Error(_))));
        })
    })
    .await;
}

#[tokio::test]
async fn concurrent_pipeline_setup_rejection_cannot_cleanup_the_live_primary_run() {
    owned(|owners| {
        Box::pin(async move {
            let peer = Peer::new(
                owners,
                vec![Plan::held("primary"), Plan::automatic("reused", &[12, 0])],
            )
            .await;
            let tts = processor(client(&peer));
            let first_witness = Arc::new(Witness::default());
            let second_witness = Arc::new(Witness::default());
            let index = pipeline(owners, vec![tts.clone(), first_witness.clone()]).await;
            speak(&owners.workers[index], "primary").await;
            peer.flushed("primary").await;
            let secondary = Pipeline::new(vec![tts.clone(), second_witness.clone()])
                .start(pipeline_config())
                .await;
            // Core can return a worker after reporting a fatal setup error.
            // Retain that supervisor and establish its terminal result and cleanup.
            if let Ok(inner) = secondary {
                let rejected = owners.workers.len();
                owners.workers.push(OwnedWorker {
                    inner,
                    joined: false,
                });
                let outcome = timeout(WAIT, owners.workers[rejected].collect_result())
                    .await
                    .expect("rejected secondary worker must reach terminal cleanup");
                assert!(
                    outcome.is_err(),
                    "parallel setup must reject the secondary run"
                );
                assert_eq!(owners.workers[rejected].state(), WorkerState::Failed);
            }
            cleaned(&second_witness).await;
            assert!(
                tts.health().unwrap().is_usable(),
                "secondary setup poisoned the primary health"
            );
            assert_eq!(first_witness.cleaned.load(Ordering::SeqCst), 0);
            sleep(QUIET).await;
            let session = peer.session("primary");
            assert_eq!(
                session.count("Clear"),
                0,
                "failed secondary cleanup retired primary owner"
            );
            assert!(!session.ws_close);
            assert!(!owners.workers[index].has_finished());
            peer.complete("primary", &[11, 0]);
            let (frames, report) = finish(&mut owners.workers[index], Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            assert_eq!(pcm(&frames), [11, 0]);
            unreported(&frames, &["primary"]);
            cleaned(&first_witness).await;
            let index = pipeline(owners, vec![tts]).await;
            speak(&owners.workers[index], "reused").await;
            let (frames, report) = finish(&mut owners.workers[index], Frame::end().data).await;
            clean(&report, WorkerState::Ended);
            peer.join(owners).await;
            assert_eq!(pcm(&frames), [12, 0]);
            unreported(&frames, &["reused"]);
            wire_closed(&peer.session("primary"), false);
            wire_closed(&peer.session("reused"), false);
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

    struct Playback {
        entered: Semaphore,
        release: Semaphore,
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
            assert_eq!(format, PcmFormat::new(RATE, 1).unwrap());
            assert_eq!(destinations, &[None]);
            Ok(())
        }
        async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
            self.submitted.lock().unwrap().push(frame.clone());
            self.entered.add_permits(1);
            self.release
                .acquire()
                .await
                .map_err(|e| ProcessorError::new(e.to_string()))?
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
    async fn output_write_and_controlled_played_do_not_turn_unreported_synthesis_into_heard_words()
    {
        owned(|owners| {
            Box::pin(async move {
                let samples: Vec<_> = (0..RATE * 30 / 1000).map(|i| i as i16 - 300).collect();
                let expected = encode_pcm16_le(&samples);
                let peer = Peer::new(
                    owners,
                    vec![Plan::automatic("one hundred exactly", &expected)],
                )
                .await;
                let tts = processor(client(&peer));
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
                    submitted: Mutex::new(vec![]),
                    receipts: AtomicUsize::new(0),
                    stops: AtomicUsize::new(0),
                });
                let before = Arc::new(Witness::default());
                let after = Arc::new(Witness::default());
                let output = Arc::new(
                    OutputTransport::new(
                        "Deepgram-WS-controlled-playback",
                        OutputTransportConfig {
                            sample_rate: Some(RATE),
                            chunk_duration: Duration::from_millis(10),
                            auto_silence: false,
                            end_silence: Duration::ZERO,
                            write_timeout: WAIT,
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
                let index = pipeline(
                    owners,
                    vec![
                        Arc::new(
                            history
                                .source_boundary("source-before-Deepgram-WS")
                                .unwrap(),
                        ),
                        tts,
                        before.clone(),
                        output,
                        after.clone(),
                        assistant,
                    ],
                )
                .await;
                let worker = &mut owners.workers[index];
                timeout(WAIT, async {
                    while !control.is_ready() {
                        tokio::task::yield_now().await;
                    }
                })
                .await
                .unwrap();
                let start = Frame::new(FrameData::LlmResponseStart);
                let start_id = start.id;
                let mut input = TtsTextInputFrame::new("one hundred exactly");
                input.display_text = "100 exactly".into();
                input.original_text = "**100 exactly**".into();
                let input = Frame::new(FrameData::TtsTextInput(input));
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
                let (frames, report) = finish(worker, Frame::end().data).await;
                clean(&report, WorkerState::Ended);
                peer.join(owners).await;
                assert_eq!(playback.receipts.load(Ordering::SeqCst), 3);
                assert_eq!(playback.stops.load(Ordering::SeqCst), 1);
                let submitted = playback.submitted.lock().unwrap();
                assert_eq!(submitted.len(), 3);
                let mut actual = vec![];
                for frame in submitted.iter() {
                    let audio = match &frame.data {
                        FrameData::TtsAudio(v) | FrameData::OutputAudio(v) => v,
                        _ => panic!("output backend received non-PCM"),
                    };
                    assert_eq!((audio.sample_rate, audio.num_channels), (RATE, 1));
                    assert_eq!(audio.num_frames(), (RATE / 100) as usize);
                    actual.extend_from_slice(&audio.audio);
                }
                assert_eq!(actual, expected);
                assert_eq!(pcm(&before.frames.lock().unwrap()), expected);
                for witness in [&before, &after] {
                    let observed = witness.frames.lock().unwrap();
                    unreported(&observed, &["one hundred exactly"]);
                    for source_id in [start_id, end_id] {
                        let boundary: Vec<_> =
                            observed.iter().filter(|f| f.id == source_id).collect();
                        assert_eq!(
                            boundary.len(),
                            1,
                            "original source boundary crosses exactly once"
                        );
                        assert_ne!(boundary[0].metadata.get("failed"), Some(&json!(true)));
                    }
                    let anchor = observed
                        .iter()
                        .find_map(|f| match &f.data {
                            FrameData::AggregatedText(v) => Some(v),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(anchor.text.text, "100 exactly");
                    assert_eq!(anchor.raw_text.as_deref(), Some("**100 exactly**"));
                    let (frame, diagnostic) = observed
                        .iter()
                        .find_map(|f| match &f.data {
                            FrameData::TtsAlignmentDiagnostic(v) => Some((f, v)),
                            _ => None,
                        })
                        .unwrap();
                    assert_eq!(frame.metadata["source_frame_id"], json!(input_id));
                    assert_eq!(diagnostic.original_text.as_deref(), Some("**100 exactly**"));
                    assert_ne!(frame.metadata.get("spoken"), Some(&json!(true)));
                }
                assert!(!frames.iter().any(|f| matches!(f.data, FrameData::Error(_))));
                assert!(shared.snapshot().unwrap().messages.is_empty());
                assert_eq!(history.status().committed_contexts, 0);
                assert_eq!(history.status().pending_responses, 0);
                assert_eq!(history.status().pending_source_responses, 0);
                wire_closed(&peer.session("one hundred exactly"), false);
            })
        })
        .await;
    }
}
