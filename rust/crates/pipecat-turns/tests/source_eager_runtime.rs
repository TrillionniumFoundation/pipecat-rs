//! Source-session ownership through real aggregation and loopback HTTP inference.
//! The synthetic source exercises native receipts; Cartesia eager stays disabled.
use pipecat_core::*;
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use pipecat_services::{
    ClientConfig, CompletionRequest, Context, LlmClient, LlmProcessor, Provider,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{mpsc, oneshot, Semaphore},
};

type Request = (Value, oneshot::Sender<String>);

struct Peer {
    endpoint: String,
    requests: mpsc::Receiver<Request>,
    count: Arc<AtomicUsize>,
    task: tokio::task::JoinHandle<()>,
}

impl Peer {
    async fn new(automatic: Option<String>) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}/stream", listener.local_addr().unwrap());
        let (tx, requests) = mpsc::channel(32);
        let count = Arc::new(AtomicUsize::new(0));
        let counted = count.clone();
        let task = tokio::spawn(async move {
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let tx = tx.clone();
                let count = counted.clone();
                let automatic = automatic.clone();
                connections.spawn(async move {
                    let request = read_request(&mut socket).await;
                    count.fetch_add(1, Ordering::SeqCst);
                    let (send, receive) = oneshot::channel();
                    if tx.send((request, send)).await.is_err() {
                        return;
                    }
                    let body = match automatic {
                        Some(body) => body,
                        None => match receive.await {
                            Ok(body) => body,
                            Err(_) => return,
                        },
                    };
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = socket.write_all(response.as_bytes()).await;
                });
            }
        });
        Self {
            endpoint,
            requests,
            count,
            task,
        }
    }

    async fn request(&mut self) -> Request {
        tokio::time::timeout(Duration::from_secs(5), self.requests.recv())
            .await
            .expect("HTTP request deadline")
            .expect("loopback peer")
    }

    fn request_count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

impl Drop for Peer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn read_request(socket: &mut TcpStream) -> Value {
    let mut bytes = Vec::new();
    let mut buffer = [0; 4096];
    let header_end = loop {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
        if let Some(position) = bytes.windows(4).position(|value| value == b"\r\n\r\n") {
            break position + 4;
        }
    };
    let length: usize = String::from_utf8_lossy(&bytes[..header_end])
        .lines()
        .find_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|value| value.trim().parse().unwrap())
        })
        .unwrap();
    while bytes.len() < header_end + length {
        let n = socket.read(&mut buffer).await.unwrap();
        assert_ne!(n, 0);
        bytes.extend_from_slice(&buffer[..n]);
    }
    serde_json::from_slice(&bytes[header_end..header_end + length]).unwrap()
}

fn answer(text: &str) -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"finish_reason":"stop"}]})
    )
}

fn tool_response(id: &str) -> String {
    format!(
        "data: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":id,"type":"function","function":{"name":"book","arguments":"{}"}}]},"finish_reason":"tool_calls"}]})
    )
}

fn transcript(kind: fn(TranscriptionFrame) -> FrameData, text: &str) -> Frame {
    Frame::new(kind(TranscriptionFrame {
        text: text.into(),
        user_id: "synthetic-source".into(),
        timestamp: "0".into(),
        finalized: true,
        ..Default::default()
    }))
}

fn unbound(mut frame: Frame) -> Frame {
    frame
        .metadata
        .insert("synthetic_unbound".into(), true.into());
    frame
}

fn source_start(turn: NativeEagerTurn) -> Frame {
    let mut frame = Frame::new(FrameData::ProposedUserStartedSpeaking);
    frame
        .set_native_eager_source(NativeEagerSource::Start(turn))
        .unwrap();
    frame
}

#[derive(Default)]
struct SourceState {
    owner: Option<EagerSourceSessionOwner>,
    turn: Option<NativeEagerTurn>,
    prediction: Option<NativeEagerPrediction>,
    final_receipt: Option<NativeEagerCommit>,
}

#[derive(Default)]
struct NativeSource {
    state: Mutex<SourceState>,
    health: ProcessorHealth,
}

impl NativeSource {
    fn revoke(&self) {
        self.state.lock().unwrap().owner.as_ref().unwrap().revoke();
    }

    fn prediction(&self) -> NativeEagerPrediction {
        self.state.lock().unwrap().prediction.clone().unwrap()
    }

    fn final_receipt(&self) -> NativeEagerCommit {
        self.state.lock().unwrap().final_receipt.clone().unwrap()
    }

    fn turn(&self) -> NativeEagerTurn {
        self.state.lock().unwrap().turn.clone().unwrap()
    }

    fn restart(&self) {
        let previous = {
            let mut state = self.state.lock().unwrap();
            state.turn = None;
            state.prediction = None;
            state.final_receipt = None;
            state
                .owner
                .replace(EagerSourceSessionOwner::new(4096).unwrap())
        };
        if let Some(previous) = previous {
            previous.revoke();
        }
    }

    fn held_final(&self, text: &str) -> (Frame, Frame) {
        let mut state = self.state.lock().unwrap();
        let mut final_frame = transcript(FrameData::Transcription, text);
        let turn = state.turn.clone().unwrap();
        let commit = state
            .owner
            .as_ref()
            .unwrap()
            .offer_final(&turn, final_frame.data.clone())
            .unwrap();
        state.final_receipt = Some(commit.clone());
        final_frame
            .set_native_eager_source(NativeEagerSource::Final(commit.clone()))
            .unwrap();
        let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
        stop.set_native_eager_source(NativeEagerSource::Stop {
            turn,
            commit: Some(commit),
        })
        .unwrap();
        (final_frame, stop)
    }

    fn held_empty_stop(&self) -> Frame {
        let state = self.state.lock().unwrap();
        let turn = state.turn.clone().unwrap();
        state
            .owner
            .as_ref()
            .unwrap()
            .offer_empty_stop(&turn)
            .unwrap();
        let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
        stop.set_native_eager_source(NativeEagerSource::Stop { turn, commit: None })
            .unwrap();
        stop
    }

    fn cancel_frame(&self, prediction: &NativeEagerPrediction) -> Frame {
        self.state
            .lock()
            .unwrap()
            .owner
            .as_ref()
            .unwrap()
            .resume(prediction);
        let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
        frame
            .set_native_eager_source(NativeEagerSource::Cancel(prediction.clone()))
            .unwrap();
        frame
    }
}

#[async_trait]
impl FrameProcessor for NativeSource {
    fn name(&self) -> &str {
        "SyntheticEagerSource"
    }

    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }

    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        // A listening session owns the lifecycle context, as the committed
        // Cartesia processor does. A Start invocation has one queue epoch.
        self.state.lock().unwrap().owner = Some(EagerSourceSessionOwner::from_context(4096, ctx)?);
        Ok(())
    }

    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            let mut state = self.state.lock().unwrap();
            if frame.native_eager_source().is_none()
                && frame.metadata.get("synthetic_unbound") != Some(&Value::Bool(true))
            {
                match &frame.data {
                    FrameData::ProposedUserStartedSpeaking => {
                        let turn = state.owner.as_ref().unwrap().begin_turn()?;
                        state.turn = Some(turn.clone());
                        state.prediction = None;
                        state.final_receipt = None;
                        frame.set_native_eager_source(NativeEagerSource::Start(turn))?;
                    }
                    FrameData::EagerTranscription(_) => {
                        let prediction = state
                            .owner
                            .as_ref()
                            .unwrap()
                            .predict(state.turn.as_ref().unwrap(), frame.data.clone())?;
                        state.prediction = Some(prediction.clone());
                        frame.set_native_eager_source(NativeEagerSource::Prediction(prediction))?;
                    }
                    FrameData::Transcription(_) => {
                        let receipt = state
                            .owner
                            .as_ref()
                            .unwrap()
                            .offer_final(state.turn.as_ref().unwrap(), frame.data.clone())?;
                        state.final_receipt = Some(receipt.clone());
                        frame.set_native_eager_source(NativeEagerSource::Final(receipt))?;
                    }
                    FrameData::ProposedUserStoppedSpeaking => {
                        let turn = state.turn.clone().unwrap();
                        let commit = state.final_receipt.clone();
                        if commit.is_none() {
                            state.owner.as_ref().unwrap().offer_empty_stop(&turn)?;
                        }
                        frame.set_native_eager_source(NativeEagerSource::Stop { turn, commit })?;
                    }
                    _ => {}
                }
            }
        }
        ctx.push(frame, direction).await
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PauseAt {
    Prediction,
    Context,
    Boundary,
}

struct Probe {
    name: &'static str,
    pause: Option<PauseAt>,
    armed: AtomicBool,
    arrived: Semaphore,
    proceed: Semaphore,
    frames: Mutex<Vec<DirectedFrame>>,
}

impl Probe {
    fn new(name: &'static str, pause: Option<PauseAt>) -> Self {
        Self {
            name,
            pause,
            armed: AtomicBool::new(pause.is_some()),
            arrived: Semaphore::new(0),
            proceed: Semaphore::new(0),
            frames: Mutex::new(Vec::new()),
        }
    }

    async fn paused(&self) {
        tokio::time::timeout(Duration::from_secs(5), self.arrived.acquire())
            .await
            .expect("pause arrival")
            .unwrap()
            .forget();
    }

    fn release(&self) {
        self.proceed.add_permits(1);
    }

    fn capability(&self) -> NativeSpeculation {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .rev()
            .find_map(|frame| {
                matches!(frame.frame.data, FrameData::LlmContextDetailed(_))
                    .then(|| frame.frame.native_speculation().cloned())
                    .flatten()
            })
            .expect("native speculative context")
    }

    fn has(&self, predicate: impl Fn(&FrameData) -> bool) -> bool {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .any(|frame| predicate(&frame.frame.data))
    }

    fn count(&self, predicate: impl Fn(&FrameData) -> bool) -> usize {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|frame| predicate(&frame.frame.data))
            .count()
    }
}

#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        self.name
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
        let wanted = direction == Direction::Downstream
            && match self.pause {
                Some(PauseAt::Prediction) => matches!(frame.data, FrameData::EagerTranscription(_)),
                Some(PauseAt::Context) => matches!(
                    &frame.data,
                    FrameData::LlmContextDetailed(context) if context.speculation
                ),
                Some(PauseAt::Boundary) => matches!(frame.data, FrameData::UserStoppedSpeaking),
                None => false,
            };
        if wanted && self.armed.swap(false, Ordering::SeqCst) {
            self.arrived.add_permits(1);
            self.proceed.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}

struct Harness {
    source: Arc<NativeSource>,
    before: Arc<Probe>,
    after: Arc<Probe>,
    output: Arc<Probe>,
    settled: Arc<Probe>,
    pair: LLMContextAggregatorPair,
    user: Arc<ClockedUser>,
    llm: Arc<LlmProcessor>,
    worker: PipelineWorker,
}

/// Advances the real aggregator using controlled state-machine time and records
/// completed input processing. It does not replace aggregation or inference.
struct ClockedUser {
    user: Arc<LLMUserAggregator>,
    applied: Mutex<Vec<u64>>,
}

#[async_trait]
impl FrameProcessor for ClockedUser {
    fn name(&self) -> &str {
        self.user.name()
    }

    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        context: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let id = frame.id;
        if direction == Direction::Downstream
            && matches!(&frame.data, FrameData::FlushDetailed(flush)
                if flush.origin.as_deref() == Some("source-test-clock"))
        {
            let millis = frame.metadata["advance_ms"].as_u64().unwrap();
            self.user
                .advance(Duration::from_millis(millis))?
                .emit(context)
                .await?;
            context.push(frame, direction).await?;
        } else {
            self.user.process(frame, direction, context).await?;
        }
        self.applied.lock().unwrap().push(id);
        Ok(())
    }

    async fn prepare_end(&self, context: &ProcessorContext) -> Result<(), ProcessorError> {
        self.user.prepare_end(context).await
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.user.cleanup().await
    }
}

impl Harness {
    async fn new(endpoint: String, pause: Option<PauseAt>) -> Self {
        Self::with_provider(endpoint, pause, Provider::OpenAi, None).await
    }

    async fn with_provider(
        endpoint: String,
        pause: Option<PauseAt>,
        provider: Provider,
        tools: Option<Arc<dyn FrameProcessor>>,
    ) -> Self {
        let source = Arc::new(NativeSource::default());
        let before = Arc::new(Probe::new(
            "BeforeSourceAdoption",
            pause.filter(|pause| *pause == PauseAt::Prediction),
        ));
        let after = Arc::new(Probe::new(
            "AfterSourceAdoption",
            pause.filter(|pause| *pause != PauseAt::Prediction),
        ));
        let output = Arc::new(Probe::new("SourceCompletionOutput", None));
        let settled = Arc::new(Probe::new("SettledSourceHistory", None));
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            UserAggregatorConfig {
                eager: Some(Default::default()),
                // Keep default interrupted recovery enabled. A retired source
                // must not create a developer-prompt replacement request.
                ..Default::default()
            },
            Default::default(),
        );
        let user = Arc::new(ClockedUser {
            user: pair.user(),
            applied: Mutex::new(Vec::new()),
        });
        let mut config = ClientConfig::new(provider, "synthetic");
        config.endpoint = endpoint;
        let llm = Arc::new(LlmProcessor::new(
            LlmClient::new(config).unwrap(),
            CompletionRequest::new(
                if provider == Provider::Gemini {
                    "gemini-3-pro"
                } else {
                    "test"
                },
                Context::default(),
            ),
        ));
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![
            source.clone(),
            before.clone(),
            user.clone(),
            after.clone(),
            llm.clone(),
        ];
        if let Some(tools) = tools {
            processors.push(tools);
        }
        processors.push(output.clone());
        processors.push(pair.assistant());
        processors.push(settled.clone());
        let worker = Pipeline::new(processors)
            .start(Default::default())
            .await
            .unwrap();
        let result = Self {
            source,
            before,
            after,
            output,
            settled,
            pair,
            user,
            llm,
            worker,
        };
        result.barrier(false).await;
        result
    }

    async fn send(&self, frame: Frame) {
        self.worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
    }

    async fn start(&self) {
        self.accept_start(Frame::new(FrameData::ProposedUserStartedSpeaking))
            .await;
    }

    async fn accept_start(&self, frame: Frame) {
        let prior = self
            .output
            .count(|data| matches!(data, FrameData::Interruption));
        self.send_applied(frame).await;
        wait(|| {
            self.output
                .count(|data| matches!(data, FrameData::Interruption))
                > prior
        })
        .await;
        self.barrier(false).await;
    }

    async fn send_applied(&self, frame: Frame) {
        let id = frame.id;
        let prior = self
            .user
            .applied
            .lock()
            .unwrap()
            .iter()
            .filter(|&&seen| seen == id)
            .count();
        self.send(frame).await;
        wait(|| {
            self.user
                .applied
                .lock()
                .unwrap()
                .iter()
                .filter(|&&seen| seen == id)
                .count()
                > prior
        })
        .await;
    }

    async fn advance(&self, millis: u64) {
        let mut frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: None,
            returning: false,
            origin: Some("source-test-clock".into()),
        }));
        frame.metadata.insert("advance_ms".into(), millis.into());
        self.send_applied(frame).await;
        self.barrier(true).await;
    }

    async fn non_speaking(&self) {
        self.send_applied(Frame::new(FrameData::VadUserStoppedSpeaking(
            VadSpeakingFrame {
                duration_secs: 0.0,
                timestamp: 0.0,
            },
        )))
        .await;
    }

    async fn predict(&self, text: &str) {
        self.send(transcript(FrameData::EagerTranscription, text))
            .await;
    }

    async fn final_text(&self, text: &str) {
        self.send(transcript(FrameData::Transcription, text)).await;
        self.barrier(false).await;
    }

    async fn stop(&self) {
        self.send(Frame::new(FrameData::ProposedUserStoppedSpeaking))
            .await;
    }

    async fn barrier(&self, completions: bool) {
        let frame = Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            event: None,
            returning: false,
            origin: Some(if completions { "pipecat.flows" } else { "test" }.into()),
        }));
        let id = frame.id;
        self.send(frame).await;
        let observed = if completions {
            &self.settled
        } else {
            &self.after
        };
        wait(|| {
            observed
                .frames
                .lock()
                .unwrap()
                .iter()
                .any(|frame| frame.direction == Direction::Downstream && frame.frame.id == id)
        })
        .await;
    }

    async fn buffered(&self, minimum: usize) {
        tokio::time::timeout(Duration::from_secs(5), async {
            while self.llm.speculation_snapshot().await.buffered_frames < minimum {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("held completion");
    }

    fn assert_no_history(&self) {
        let messages = self.pair.context().snapshot().unwrap().messages;
        assert!(
            messages.is_empty(),
            "retired user/developer history: {messages:?}"
        );
    }

    async fn finish(self, allow_provider_error: bool) {
        self.worker.end().await.unwrap();
        let (_, report) = tokio::time::timeout(Duration::from_secs(5), self.worker.collect())
            .await
            .expect("pipeline completion")
            .unwrap();
        if !allow_provider_error {
            assert!(report.errors.is_empty(), "{:?}", report.errors);
        }
        for directed in self.after.frames.lock().unwrap().iter() {
            assert!(directed.frame.native_eager_source().is_none());
        }
    }
}

async fn wait(mut predicate: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !predicate() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("state transition deadline");
}

#[tokio::test]
async fn source_retirement_before_adoption_or_request_never_contacts_http() {
    for pause in [PauseAt::Prediction, PauseAt::Context] {
        let peer = Peer::new(Some(answer("must not run"))).await;
        let harness = Harness::new(peer.endpoint.clone(), Some(pause)).await;
        harness.start().await;
        harness.predict("retired").await;
        let blocked = if pause == PauseAt::Prediction {
            &harness.before
        } else {
            &harness.after
        };
        blocked.paused().await;
        let prediction = harness.source.prediction();
        assert!(prediction.is_live());
        harness.source.revoke();
        assert!(!prediction.is_live());
        if pause == PauseAt::Context {
            assert_eq!(
                harness.after.capability().status(),
                SpeculationStatus::Cancelled
            );
        }
        blocked.release();
        harness.barrier(true).await;
        assert_eq!(peer.request_count(), 0, "retired source contacted HTTP");
        harness.assert_no_history();
        harness.finish(false).await;
    }
}

#[tokio::test]
async fn retired_staged_final_has_no_user_or_default_developer_fallback() {
    let mut peer = Peer::new(Some(answer("uncommitted"))).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("old text").await;
    let _ = peer.request().await;
    harness.buffered(3).await;
    let capability = harness.after.capability();
    harness.final_text("old text").await;
    let receipt = harness.source.final_receipt();
    assert!(receipt.is_live());
    harness.source.revoke();
    assert!(!receipt.is_live());
    harness.stop().await;
    harness.barrier(true).await;
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
    assert_eq!(
        peer.request_count(),
        1,
        "retired final triggered another request"
    );
    harness.assert_no_history();
    assert!(!harness
        .output
        .has(|data| matches!(data, FrameData::LlmText(_))));
    harness.finish(false).await;
}

#[tokio::test]
async fn transfer_before_boundary_release_survives_source_revoke() {
    let mut peer = Peer::new(None).await;
    let harness = Harness::new(peer.endpoint.clone(), Some(PauseAt::Boundary)).await;
    harness.start().await;
    harness.predict("its ready").await;
    let (_, reply) = peer.request().await;
    reply.send(answer("Ready.")).unwrap();
    harness.buffered(3).await;
    let capability = harness.after.capability();
    harness.final_text("It's ready!").await;
    harness.stop().await;
    harness.after.paused().await;
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert!(!harness
        .output
        .has(|data| matches!(data, FrameData::LlmText(_))));
    harness.source.revoke();
    assert!(capability.source_is_current());
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    harness.after.release();
    harness.barrier(true).await;
    assert_eq!(capability.status(), SpeculationStatus::Released);
    assert_eq!(peer.request_count(), 1);
    assert_eq!(
        harness
            .output
            .count(|data| matches!(data, FrameData::LlmText(text) if text.text == "Ready.")),
        1
    );
    assert_eq!(
        harness.llm.request_snapshot().unwrap().context.messages[0].content,
        Some(pipecat_services::Content::Text("It's ready!".into()))
    );
    {
        let frames = harness.output.frames.lock().unwrap();
        let stop = frames
            .iter()
            .position(|frame| {
                frame.direction == Direction::Downstream
                    && matches!(frame.frame.data, FrameData::UserStoppedSpeaking)
            })
            .unwrap();
        let text = frames
            .iter()
            .position(|frame| matches!(frame.frame.data, FrameData::LlmText(_)))
            .unwrap();
        assert!(stop < text);
    }
    harness.finish(false).await;
}

#[tokio::test]
async fn old_source_cancel_and_duplicate_prediction_do_not_cancel_replacement() {
    let mut peer = Peer::new(None).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("old").await;
    let (_, first_reply) = peer.request().await;
    let old_prediction = harness.source.prediction();
    let old_capability = harness.after.capability();
    let old_cancel = harness.source.cancel_frame(&old_prediction);
    harness.predict("new").await;
    let (_, second_reply) = peer.request().await;
    let replacement = harness.after.capability();
    assert_ne!(old_capability, replacement);
    assert_eq!(old_capability.status(), SpeculationStatus::Cancelled);
    assert_eq!(replacement.status(), SpeculationStatus::Pending);
    let mut replay = Frame::new(old_prediction.payload().clone());
    replay
        .set_native_eager_source(NativeEagerSource::Prediction(old_prediction))
        .unwrap();
    harness.send(old_cancel).await;
    harness.send(replay).await;
    harness.barrier(false).await;
    assert_eq!(replacement.status(), SpeculationStatus::Pending);
    drop(first_reply);
    second_reply.send(answer("new answer")).unwrap();
    harness.buffered(3).await;
    harness.final_text("new").await;
    harness.stop().await;
    harness.barrier(true).await;
    assert_eq!(replacement.status(), SpeculationStatus::Released);
    assert_eq!(peer.request_count(), 2);
    assert_eq!(
        harness
            .output
            .count(|data| matches!(data, FrameData::LlmText(_))),
        1
    );
    harness.finish(false).await;
}

#[tokio::test]
async fn source_retirement_suppresses_pending_http_error_and_held_text() {
    let mut peer = Peer::new(None).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("pending").await;
    let (_, reply) = peer.request().await;
    let capability = harness.after.capability();
    harness.source.revoke();
    assert!(!capability.source_is_current());
    reply.send("data: malformed-json\n\n".into()).unwrap();
    harness.barrier(true).await;
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
    assert!(!harness
        .after
        .has(|data| matches!(data, FrameData::Error(_))));
    assert!(!harness
        .output
        .has(|data| matches!(data, FrameData::LlmText(_))));
    harness.assert_no_history();
    assert_eq!(peer.request_count(), 1);
    harness.finish(false).await;
}

#[tokio::test]
async fn current_source_provider_error_remains_visible() {
    let mut peer = Peer::new(None).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("current").await;
    let (_, reply) = peer.request().await;
    let capability = harness.after.capability();
    reply.send("data: malformed-json\n\n".into()).unwrap();
    wait(|| {
        harness
            .after
            .has(|data| matches!(data, FrameData::Error(_)))
    })
    .await;
    assert!(capability.source_is_current());
    capability.cancel();
    harness.barrier(true).await;
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
    assert_eq!(peer.request_count(), 1);
    harness.assert_no_history();
    harness.finish(true).await;
}

#[tokio::test]
async fn revoked_source_waiting_in_llm_fifo_never_starts_second_request() {
    let mut peer = Peer::new(None).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.send(Frame::new(FrameData::LlmRun)).await;
    let (_, ordinary_reply) = peer.request().await;
    harness.predict("must stay queued").await;
    tokio::time::timeout(Duration::from_secs(5), async {
        while harness.llm.speculation_snapshot().await.status != Some(SpeculationStatus::Pending) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let capability = harness.after.capability();
    assert_eq!(peer.request_count(), 1);
    harness.source.revoke();
    assert!(!capability.admits_completion());
    ordinary_reply.send(answer("ordinary completion")).unwrap();
    harness.barrier(true).await;
    assert_eq!(peer.request_count(), 1, "stale FIFO waiter contacted HTTP");
    assert!(!harness
        .pair
        .context()
        .snapshot()
        .unwrap()
        .messages
        .iter()
        .any(|message| {
            matches!(
                message.role,
                pipecat_services::Role::User | pipecat_services::Role::Developer
            )
        }));
    harness.finish(false).await;
}

#[tokio::test]
async fn retired_final_watchdog_cannot_create_default_empty_turn_recovery() {
    let mut peer = Peer::new(Some(answer("retired"))).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("retired").await;
    let _ = peer.request().await;
    harness.buffered(3).await;
    harness.final_text("").await;
    harness.source.revoke();
    harness.non_speaking().await;
    harness.advance(6_000).await;
    assert_eq!(
        peer.request_count(),
        1,
        "watchdog admitted a developer fallback"
    );
    harness.assert_no_history();
    harness.finish(false).await;
}

#[tokio::test]
async fn equal_text_from_another_source_does_not_confirm_old_prediction() {
    let mut peer = Peer::new(Some(answer("response"))).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("same text").await;
    let _ = peer.request().await;
    harness.buffered(3).await;
    let old = harness.after.capability();
    let foreign_source = EagerSourceSessionOwner::new(4096).unwrap();
    let foreign_turn = foreign_source.begin_turn().unwrap();
    harness
        .accept_start(source_start(foreign_turn.clone()))
        .await;
    let mut final_frame = transcript(FrameData::Transcription, "same text");
    let receipt = foreign_source
        .offer_final(&foreign_turn, final_frame.data.clone())
        .unwrap();
    final_frame
        .set_native_eager_source(NativeEagerSource::Final(receipt.clone()))
        .unwrap();
    harness.send(final_frame).await;
    harness.barrier(false).await;
    let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
    stop.set_native_eager_source(NativeEagerSource::Stop {
        turn: foreign_turn,
        commit: Some(receipt),
    })
    .unwrap();
    harness.send(stop).await;
    harness.barrier(true).await;
    assert_eq!(old.status(), SpeculationStatus::Cancelled);
    assert_eq!(
        peer.request_count(),
        2,
        "foreign equal text reused old inference"
    );
    let messages = harness.pair.context().snapshot().unwrap().messages;
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == pipecat_services::Role::User)
            .count(),
        1
    );
    harness.finish(false).await;
}

#[tokio::test]
async fn quiet_source_health_restoration_invalidates_queued_request() {
    for go_unusable_first in [false, true] {
        let peer = Peer::new(Some(answer("stale health"))).await;
        let harness = Harness::new(peer.endpoint.clone(), Some(PauseAt::Context)).await;
        harness.start().await;
        harness.predict("old generation").await;
        harness.after.paused().await;
        if go_unusable_first {
            harness.source.health.set_usable(false);
        }
        harness.source.health.set_usable(true);
        assert!(!harness.after.capability().admits_completion());
        harness.after.release();
        harness.barrier(true).await;
        assert_eq!(peer.request_count(), 0);
        harness.assert_no_history();
        harness.finish(false).await;
    }
}

#[tokio::test]
async fn normal_source_completion_keeps_offered_receipt_after_owner_drop() {
    let mut peer = Peer::new(Some(answer("preserved"))).await;
    let harness = Harness::new(peer.endpoint.clone(), None).await;
    harness.start().await;
    harness.predict("valid final").await;
    let _ = peer.request().await;
    harness.buffered(3).await;
    let capability = harness.after.capability();
    harness.final_text("valid final").await;
    let source = harness.source.state.lock().unwrap().owner.take().unwrap();
    assert!(source.begin_drain());
    assert!(source.complete_drain());
    drop(source);
    // Mechanical health changes after successful normal drain no longer
    // invalidate the already offered receipt.
    harness.source.health.set_usable(true);
    harness.stop().await;
    harness.barrier(true).await;
    assert_eq!(capability.status(), SpeculationStatus::Released);
    assert_eq!(peer.request_count(), 1);
    assert!(harness
        .output
        .has(|data| matches!(data, FrameData::LlmText(text) if text.text == "preserved")));
    harness.finish(false).await;
}

#[tokio::test]
async fn transferred_source_revocation_preserves_tool_callback_after_release() {
    let mut peer = Peer::new(None).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    tools
        .register(
            "book",
            RegisteredTool::new(Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({"booked": true})) })
            })),
        )
        .unwrap();
    let harness = Harness::with_provider(
        peer.endpoint.clone(),
        Some(PauseAt::Boundary),
        Provider::OpenAi,
        Some(Arc::new(tools)),
    )
    .await;
    harness.start().await;
    harness.predict("book").await;
    let (_, reply) = peer.request().await;
    harness.final_text("book").await;
    harness.stop().await;
    harness.after.paused().await;
    let capability = harness.after.capability();
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    harness.source.revoke();
    reply.send(tool_response("book1")).unwrap();
    harness.buffered(3).await;
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    harness.after.release();
    wait(|| effects.load(Ordering::SeqCst) == 1).await;
    peer.request().await.1.send(answer("Booked.")).unwrap();
    harness.barrier(true).await;
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    assert_eq!(peer.request_count(), 2);
    harness.finish(false).await;
}

#[tokio::test]
async fn bound_gemini_signed_bytes_survive_transfer_and_source_revoke() {
    let mut peer = Peer::new(None).await;
    let harness = Harness::with_provider(
        peer.endpoint.clone(),
        Some(PauseAt::Boundary),
        Provider::Gemini,
        None,
    )
    .await;
    harness.start().await;
    harness.predict("hello").await;
    let (_, reply) = peer.request().await;
    let parts = json!([
        {"text":"● exact ","thoughtSignature":"opaque+/=="},
        {"text":"text"},
        {"text":"","thoughtSignature":"tail+/=="}
    ]);
    reply
        .send(format!(
            "data: {}\n\n",
            json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]})
        ))
        .unwrap();
    harness.buffered(5).await;
    harness.final_text("Hello!").await;
    harness.stop().await;
    harness.after.paused().await;
    harness.source.revoke();
    harness.after.release();
    harness.barrier(true).await;
    wait(|| harness.pair.context().snapshot().unwrap().messages.len() > 1).await;
    let body = pipecat_services::adapters::request_body(
        Provider::Gemini,
        &CompletionRequest::new("gemini-3-pro", harness.pair.context().snapshot().unwrap()),
    )
    .unwrap();
    assert_eq!(body["contents"][1]["parts"], parts);
    assert_eq!(peer.request_count(), 1);
    harness.finish(false).await;
}

#[tokio::test]
async fn pending_bound_tool_withdraws_without_effect_and_current_final_retries_once() {
    let mut peer = Peer::new(None).await;
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    tools
        .register(
            "book",
            RegisteredTool::new(Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({"booked": true})) })
            })),
        )
        .unwrap();
    let harness = Harness::with_provider(
        peer.endpoint.clone(),
        None,
        Provider::OpenAi,
        Some(Arc::new(tools)),
    )
    .await;
    harness.start().await;
    harness.predict("book").await;
    peer.request()
        .await
        .1
        .send(tool_response("withdrawn"))
        .unwrap();
    let capability = harness.after.capability();
    wait(|| capability.status() == SpeculationStatus::Cancelled).await;
    harness.barrier(true).await;
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    assert!(capability.source_is_current());
    harness.assert_no_history();
    harness.final_text("book").await;
    harness.stop().await;
    peer.request()
        .await
        .1
        .send(tool_response("committed"))
        .unwrap();
    wait(|| effects.load(Ordering::SeqCst) == 1).await;
    peer.request().await.1.send(answer("Booked.")).unwrap();
    harness.barrier(true).await;
    assert_eq!(peer.request_count(), 3);
    assert_eq!(effects.load(Ordering::SeqCst), 1);
    let messages = harness.pair.context().snapshot().unwrap().messages;
    assert_eq!(
        messages
            .iter()
            .filter(|message| message.role == pipecat_services::Role::User)
            .count(),
        1
    );
    harness.finish(false).await;
}

#[tokio::test]
async fn bound_anthropic_exact_signature_transfers_or_is_discarded_with_retired_source() {
    let events = [
        json!({"type":"message_start","message":{}}),
        json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","thinking":"closed\n思考"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"opaque+/"}}),
        json!({"type":"content_block_delta","index":0,"delta":{"type":"signature_delta","signature":"bytes=="}}),
        json!({"type":"content_block_stop","index":0}),
        json!({"type":"content_block_start","index":1,"content_block":{"type":"text","text":"Ready."}}),
        json!({"type":"content_block_stop","index":1}),
        json!({"type":"message_delta","delta":{"stop_reason":"end_turn"}}),
        json!({"type":"message_stop"}),
    ];
    let response: String = events
        .iter()
        .map(|event| format!("data: {event}\n\n"))
        .collect();
    let expected = json!([
        {"type":"thinking","thinking":"closed\n思考","signature":"opaque+/bytes=="},
        {"type":"text","text":"Ready."}
    ]);
    for transfer in [false, true] {
        let mut peer = Peer::new(Some(response.clone())).await;
        let harness = Harness::with_provider(
            peer.endpoint.clone(),
            transfer.then_some(PauseAt::Boundary),
            Provider::Anthropic,
            None,
        )
        .await;
        harness.start().await;
        harness.predict("its ready").await;
        let _ = peer.request().await;
        harness.buffered(7).await;
        let capability = harness.after.capability();
        harness.assert_no_history();
        assert!(!harness.output.has(|data| matches!(
            data,
            FrameData::LlmThoughtText(_)
                | FrameData::LlmText(_)
                | FrameData::LlmContinuationCheckpoint
        )));
        harness.final_text("It's ready!").await;
        if transfer {
            harness.stop().await;
            harness.after.paused().await;
            assert_eq!(capability.status(), SpeculationStatus::Confirmed);
            harness.source.revoke();
            assert_eq!(capability.status(), SpeculationStatus::Confirmed);
            harness.after.release();
            harness.barrier(true).await;
            wait(|| harness.pair.context().snapshot().unwrap().messages.len() > 1).await;
            assert_eq!(capability.status(), SpeculationStatus::Released);
            // Verify the actual next provider request, not just an in-memory
            // projection of the retained signed continuation.
            harness.send(Frame::new(FrameData::LlmRun)).await;
            let (body, _) = peer.request().await;
            assert_eq!(
                body["messages"][0]["content"],
                json!([{ "type": "text", "text": "It's ready!" }])
            );
            assert_eq!(body["messages"][1]["content"], expected);
            harness.barrier(true).await;
            assert_eq!(peer.request_count(), 2);
        } else {
            harness.source.revoke();
            harness.stop().await;
            harness.barrier(true).await;
            assert_eq!(capability.status(), SpeculationStatus::Cancelled);
            assert_eq!(harness.llm.speculation_snapshot().await.buffered_frames, 0);
            assert_eq!(
                peer.request_count(),
                1,
                "retired signed final triggered fallback"
            );
            harness.assert_no_history();
            assert!(!harness.output.has(|data| matches!(
                data,
                FrameData::LlmThoughtText(_)
                    | FrameData::LlmText(_)
                    | FrameData::LlmContinuationCheckpoint
            )));
            assert!(harness
                .output
                .frames
                .lock()
                .unwrap()
                .iter()
                .all(|frame| { frame.frame.native_llm_continuation().unwrap().is_none() }));
        }
        harness.finish(false).await;
    }
}

async fn effect_harness(endpoint: String) -> (Harness, Arc<AtomicUsize>) {
    let effects = Arc::new(AtomicUsize::new(0));
    let counted = effects.clone();
    let mut tools = ToolExecutor::new(1, 8, 4096).unwrap();
    tools
        .register(
            "book",
            RegisteredTool::new(Arc::new(move |_| {
                counted.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(json!({"booked": true})) })
            })),
        )
        .unwrap();
    let harness =
        Harness::with_provider(endpoint, None, Provider::OpenAi, Some(Arc::new(tools))).await;
    (harness, effects)
}

#[derive(Clone, Copy, Debug)]
enum Replacement {
    RestartedSource,
    SameSource,
    Unbound,
}

async fn replace_active_turn(harness: &Harness, replacement: Replacement) {
    match replacement {
        Replacement::RestartedSource => {
            harness.source.restart();
            harness.start().await;
        }
        Replacement::SameSource => harness.start().await,
        Replacement::Unbound => {
            harness.source.revoke();
            harness
                .accept_start(unbound(Frame::new(FrameData::ProposedUserStartedSpeaking)))
                .await;
        }
    }
}

async fn deliver_ordered(harness: &Harness, final_frame: Frame, stop: Frame, stop_first: bool) {
    // Input completion witnesses force the requested order at the real user
    // aggregator despite the normal/system lane priority difference.
    if stop_first {
        harness.send_applied(stop).await;
        harness.send_applied(final_frame).await;
    } else {
        harness.send_applied(final_frame).await;
        harness.send_applied(stop).await;
    }
}

#[tokio::test]
async fn never_predicted_old_turn_cannot_close_replacement_or_commit_its_buffered_text() {
    for replacement in [
        Replacement::RestartedSource,
        Replacement::SameSource,
        Replacement::Unbound,
    ] {
        for stop_first in [false, true] {
            for buffered in [false, true] {
                let mut peer = Peer::new(Some(answer("B answer"))).await;
                let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
                harness.start().await;
                let old_start = source_start(harness.source.turn());
                let (old_final, old_stop) = harness.source.held_final("A stale");
                assert_eq!(peer.request_count(), 0, "A has never adopted a prediction");
                replace_active_turn(&harness, replacement).await;
                if buffered {
                    harness
                        .send_applied(unbound(transcript(FrameData::Transcription, "B buffered")))
                        .await;
                }
                let (new_final, new_stop) = if matches!(replacement, Replacement::Unbound) {
                    (
                        unbound(transcript(FrameData::Transcription, "B final")),
                        unbound(Frame::new(FrameData::ProposedUserStoppedSpeaking)),
                    )
                } else {
                    harness.source.held_final("B final")
                };
                let deadline = harness.pair.user().deadline().unwrap();
                let transcript_count = harness.pair.user().transcript_frames().unwrap().len();
                let stops = harness
                    .output
                    .count(|data| matches!(data, FrameData::UserStoppedSpeaking));
                deliver_ordered(&harness, old_final.clone(), old_stop.clone(), stop_first).await;
                harness.send_applied(old_start.clone()).await;
                assert_eq!(
                    harness.pair.user().deadline().unwrap(),
                    deadline,
                    "stale A changed B timer: {replacement:?}"
                );
                assert_eq!(
                    harness.pair.user().transcript_frames().unwrap().len(),
                    transcript_count
                );
                harness.barrier(true).await;
                assert_eq!(
                    harness
                        .output
                        .count(|data| matches!(data, FrameData::UserStoppedSpeaking)),
                    stops
                );
                assert_eq!(peer.request_count(), 0, "A prematurely completed B: {replacement:?}, stop_first={stop_first}, buffered={buffered}");
                assert_eq!(effects.load(Ordering::SeqCst), 0);
                harness.assert_no_history();
                deliver_ordered(&harness, new_final, new_stop, false).await;
                harness.barrier(true).await;
                let (request, _) = peer.request().await;
                let expected = if buffered {
                    "B buffered B final"
                } else {
                    "B final"
                };
                assert_eq!(request["messages"][0]["content"], expected);
                assert!(!request.to_string().contains("A stale"));
                assert_eq!(peer.request_count(), 1);
                assert_eq!(effects.load(Ordering::SeqCst), 0);
                let history = harness.pair.context().snapshot().unwrap();
                let stops = harness
                    .output
                    .count(|data| matches!(data, FrameData::UserStoppedSpeaking));
                deliver_ordered(&harness, old_final, old_stop, !stop_first).await;
                harness.send_applied(old_start).await;
                harness.advance(12_000).await;
                assert_eq!(
                    harness.pair.context().snapshot().unwrap(),
                    history,
                    "old A reopened completed B"
                );
                assert_eq!(
                    harness
                        .output
                        .count(|data| matches!(data, FrameData::UserStoppedSpeaking)),
                    stops
                );
                assert_eq!(peer.request_count(), 1);
                harness.finish(false).await;
            }
        }
    }
}

#[tokio::test]
async fn never_predicted_old_receipts_cannot_suppress_replacement_recovery() {
    for replacement in [Replacement::RestartedSource, Replacement::Unbound] {
        for stop_first in [false, true] {
            let mut peer = Peer::new(Some(answer("Please repeat"))).await;
            let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
            harness.start().await;
            let (old_final, old_stop) = harness.source.held_final("A stale");
            replace_active_turn(&harness, replacement).await;
            deliver_ordered(&harness, old_final, old_stop, stop_first).await;
            harness.barrier(true).await;
            assert_eq!(peer.request_count(), 0);
            harness.assert_no_history();
            harness.non_speaking().await;
            harness.advance(6_000).await;
            let (request, _) = peer.request().await;
            assert_eq!(request["messages"][0]["role"], "developer");
            assert_eq!(
                request["messages"][0]["content"],
                DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
            );
            assert_eq!(
                peer.request_count(),
                1,
                "B recovery was changed by A: {replacement:?}"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            assert!(!harness
                .pair
                .context()
                .snapshot()
                .unwrap()
                .messages
                .iter()
                .any(|message| { message.role == pipecat_services::Role::User }));
            harness.finish(false).await;
        }
    }
}

#[tokio::test]
async fn revoked_started_turn_without_adopted_prediction_never_recovers_with_any_receipt_delivery()
{
    for disposition in ["none", "held", "dropped", "delivered", "normal_eof"] {
        let peer = Peer::new(Some(answer("must not recover"))).await;
        let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
        harness.start().await;
        let mut held = (!matches!(disposition, "none" | "normal_eof"))
            .then(|| harness.source.held_final("A stale"));
        if disposition == "dropped" {
            drop(held.take());
            harness.source.state.lock().unwrap().final_receipt = None;
        }
        if disposition == "normal_eof" {
            let owner = harness.source.state.lock().unwrap().owner.take().unwrap();
            assert!(owner.begin_drain());
            assert!(owner.complete_drain());
            drop(owner);
        } else {
            harness.source.revoke();
        }
        if disposition == "delivered" {
            let (final_frame, stop) = held.take().unwrap();
            deliver_ordered(&harness, final_frame, stop, true).await;
        }
        harness.non_speaking().await;
        harness.advance(6_000).await;
        assert_eq!(
            peer.request_count(),
            0,
            "revoked {disposition} turn recovered without a prediction"
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        harness.assert_no_history();
        assert_eq!(
            harness
                .output
                .count(|data| matches!(data, FrameData::UserStoppedSpeaking)),
            0
        );
        drop(held);
        harness.finish(false).await;
    }
}

#[tokio::test]
async fn healthy_started_turn_without_prediction_or_final_really_exercises_default_recovery() {
    let mut peer = Peer::new(Some(answer("Please repeat"))).await;
    let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
    harness.start().await;
    harness.non_speaking().await;
    harness.advance(6_000).await;
    let (request, _) = peer.request().await;
    assert_eq!(request["messages"][0]["role"], "developer");
    assert_eq!(
        request["messages"][0]["content"],
        DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT
    );
    assert_eq!(peer.request_count(), 1);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    harness.finish(false).await;
}

#[tokio::test]
async fn old_empty_stop_cannot_close_replacement_or_suppress_its_recovery() {
    for replacement in [
        Replacement::RestartedSource,
        Replacement::SameSource,
        Replacement::Unbound,
    ] {
        let mut peer = Peer::new(Some(answer("Please repeat"))).await;
        let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
        harness.start().await;
        let old_empty = harness.source.held_empty_stop();
        replace_active_turn(&harness, replacement).await;
        let deadline = harness.pair.user().deadline().unwrap();
        harness.send_applied(old_empty.clone()).await;
        assert_eq!(harness.pair.user().deadline().unwrap(), deadline);
        harness.barrier(true).await;
        assert_eq!(peer.request_count(), 0);
        harness.assert_no_history();
        harness.non_speaking().await;
        harness.advance(6_000).await;
        let (request, _) = peer.request().await;
        assert_eq!(request["messages"][0]["role"], "developer");
        assert_eq!(peer.request_count(), 1);
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        harness.send_applied(old_empty).await;
        harness.advance(12_000).await;
        assert_eq!(peer.request_count(), 1);
        harness.finish(false).await;
    }
}

#[tokio::test]
async fn source_authorized_empty_stop_allows_one_healthy_recovery() {
    let mut peer = Peer::new(Some(answer("Please repeat"))).await;
    let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
    harness.start().await;
    let stop = harness.source.held_empty_stop();
    harness.send_applied(stop.clone()).await;
    harness.advance(6_000).await;
    let (request, _) = peer.request().await;
    assert_eq!(request["messages"][0]["role"], "developer");
    assert_eq!(peer.request_count(), 1);
    assert_eq!(effects.load(Ordering::SeqCst), 0);
    harness.send_applied(stop).await;
    harness.advance(12_000).await;
    assert_eq!(peer.request_count(), 1);
    harness.finish(false).await;
}

#[tokio::test]
async fn typed_start_internal_events_and_duplicate_start_preserve_own_final_admission() {
    for stop_first in [false, true] {
        let mut peer = Peer::new(Some(answer("A answer"))).await;
        let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
        harness.start().await;
        let duplicate_start = source_start(harness.source.turn());
        harness.send_applied(duplicate_start.clone()).await;
        let (final_frame, stop) = harness.source.held_final("A own final");
        deliver_ordered(&harness, final_frame, stop, stop_first).await;
        harness.barrier(true).await;
        assert_eq!(
            peer.request_count(),
            1,
            "internal Started/reset discarded the typed binding"
        );
        let (request, _) = peer.request().await;
        assert_eq!(request["messages"][0]["content"], "A own final");
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        let history = harness.pair.context().snapshot().unwrap();
        harness.send_applied(duplicate_start).await;
        harness.non_speaking().await;
        harness.advance(12_000).await;
        assert_eq!(peer.request_count(), 1);
        assert_eq!(harness.pair.context().snapshot().unwrap(), history);
        harness.finish(false).await;
    }
}

#[tokio::test]
async fn live_old_source_receipt_cannot_close_an_independent_current_source_turn() {
    for stop_first in [false, true] {
        let mut peer = Peer::new(Some(answer("B answer"))).await;
        let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
        harness.start().await;
        let old_turn = harness.source.turn();
        let (old_final, old_stop) = harness.source.held_final("A still live");
        let next_source = EagerSourceSessionOwner::new(4096).unwrap();
        let next_turn = next_source.begin_turn().unwrap();
        harness.accept_start(source_start(next_turn.clone())).await;
        // Isolate exact current-turn equality from source-liveness guards.
        // Removing equality alone must admit A and fail the HTTP assertion.
        assert!(old_turn.is_live());
        assert!(next_turn.is_live());
        let mut final_frame = transcript(FrameData::Transcription, "B own final");
        let receipt = next_source
            .offer_final(&next_turn, final_frame.data.clone())
            .unwrap();
        final_frame
            .set_native_eager_source(NativeEagerSource::Final(receipt.clone()))
            .unwrap();
        let mut stop = Frame::new(FrameData::ProposedUserStoppedSpeaking);
        stop.set_native_eager_source(NativeEagerSource::Stop {
            turn: next_turn,
            commit: Some(receipt),
        })
        .unwrap();
        deliver_ordered(&harness, old_final, old_stop, stop_first).await;
        harness.barrier(true).await;
        assert_eq!(
            peer.request_count(),
            0,
            "a live non-current source closed B"
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        harness.assert_no_history();
        assert_eq!(
            harness
                .output
                .count(|data| matches!(data, FrameData::UserStoppedSpeaking)),
            0
        );
        deliver_ordered(&harness, final_frame, stop, false).await;
        harness.barrier(true).await;
        assert_eq!(peer.request_count(), 1);
        let (request, _) = peer.request().await;
        assert_eq!(request["messages"][0]["content"], "B own final");
        harness.finish(false).await;
    }
}

fn user_history(context: &Context) -> Vec<String> {
    context
        .messages
        .iter()
        .filter(|message| message.role == pipecat_services::Role::User)
        .map(|message| match &message.content {
            Some(pipecat_services::Content::Text(text)) => text.clone(),
            other => panic!("expected ordinary user text, got {other:?}"),
        })
        .collect()
}

#[tokio::test]
async fn typed_input_after_retired_watchdog_uses_preserved_ordinary_text_without_resurrecting_source(
) {
    for keep in [false, true] {
        let mut peer = Peer::new(Some(answer("typed answer"))).await;
        let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
        harness.start().await;
        harness.final_text("discard").await;
        if keep {
            harness
                .send_applied(unbound(transcript(FrameData::Transcription, "keep")))
                .await;
        }
        harness.source.revoke();
        harness.non_speaking().await;
        harness.advance(6_000).await;
        harness.assert_no_history();
        assert_eq!(
            peer.request_count(),
            0,
            "retired watchdog emitted inference"
        );
        assert_eq!(effects.load(Ordering::SeqCst), 0);

        let interruptions = harness
            .output
            .count(|data| matches!(data, FrameData::Interruption));
        harness
            .send_applied(Frame::new(FrameData::InputText(TextFrame::new(
                "typed next",
            ))))
            .await;
        wait(|| {
            harness
                .output
                .count(|data| matches!(data, FrameData::Interruption))
                > interruptions
        })
        .await;
        harness.barrier(true).await;
        assert_eq!(
            peer.request_count(),
            1,
            "typed input should produce one explicit request"
        );
        let (request, _) = peer.request().await;
        let expected = if keep {
            vec!["keep".to_owned(), "typed next".to_owned()]
        } else {
            vec!["typed next".to_owned()]
        };
        let requested: Vec<_> = request["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|message| {
                assert_eq!(message["role"], "user");
                message["content"].as_str().unwrap().to_owned()
            })
            .collect();
        assert_eq!(requested, expected);
        assert!(!request.to_string().contains("discard"));
        assert_eq!(
            user_history(&harness.pair.context().snapshot().unwrap()),
            expected
        );
        assert!(!harness
            .pair
            .context()
            .snapshot()
            .unwrap()
            .messages
            .iter()
            .any(|message| { message.role == pipecat_services::Role::Developer }));
        assert_eq!(effects.load(Ordering::SeqCst), 0);
        harness.finish(false).await;
    }
}

#[tokio::test]
async fn global_end_independently_flushes_only_ordinary_text_before_or_after_source_watchdog() {
    for watchdog_first in [false, true] {
        for keep in [false, true] {
            let mut peer = Peer::new(Some(answer("ordinary terminal answer"))).await;
            let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
            harness.start().await;
            harness.final_text("discard").await;
            if keep {
                harness
                    .send_applied(unbound(transcript(FrameData::Transcription, "keep")))
                    .await;
            }
            harness.source.revoke();
            if watchdog_first {
                harness.non_speaking().await;
                harness.advance(6_000).await;
            }
            harness.assert_no_history();
            assert_eq!(peer.request_count(), 0);
            let context = harness.pair.context();
            // A real pipeline End reaches the aggregator and drains the real
            // LLM completion before shutdown. No terminal frames are withheld.
            harness.finish(false).await;
            assert_eq!(
                peer.request_count(),
                usize::from(keep),
                "terminal HTTP admission, watchdog_first={watchdog_first}, keep={keep}"
            );
            let snapshot = context.snapshot().unwrap();
            let expected = if keep {
                vec!["keep".to_owned()]
            } else {
                vec![]
            };
            assert_eq!(user_history(&snapshot), expected);
            assert!(!snapshot
                .messages
                .iter()
                .any(|message| message.role == pipecat_services::Role::Developer));
            assert!(!serde_json::to_string(&snapshot)
                .unwrap()
                .contains("discard"));
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            if keep {
                let (request, _) = peer.request().await;
                assert_eq!(
                    request["messages"],
                    json!([{"role":"user","content":"keep"}])
                );
            } else {
                assert!(snapshot.messages.is_empty());
            }
        }
    }
}
// Append only to the frozen review tree's tests/source_eager_runtime.rs.
// Reuses the native source/actual aggregator/LLM/output fixture, not manufactured completion.
#[cfg(test)]
mod independent_signed_source_review {
    use super::*;

    #[tokio::test]
    async fn review_gemini_source_retirement_or_transfer_preserves_exact_next_wire_request() {
        let parts = json!([
            {"text":"review ● ","thoughtSignature":"opaque+/review=="},
            {"text":"suffix"},
            {"text":"","thoughtSignature":"trailing+/=="}
        ]);
        let response = format!(
            "data: {}\n\n",
            json!({
                "candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]
            })
        );
        for transfer in [false, true] {
            let mut peer = Peer::new(Some(response.clone())).await;
            let harness = Harness::with_provider(
                peer.endpoint.clone(),
                transfer.then_some(PauseAt::Boundary),
                Provider::Gemini,
                None,
            )
            .await;
            harness.start().await;
            harness.predict("review ready").await;
            let _ = peer.request().await;
            harness.buffered(5).await;
            let capability = harness.after.capability();
            harness.assert_no_history();
            harness.final_text("Review ready!").await;
            if transfer {
                harness.stop().await;
                harness.after.paused().await;
                assert_eq!(capability.status(), SpeculationStatus::Confirmed);
                harness.source.revoke();
                assert_eq!(capability.status(), SpeculationStatus::Confirmed);
                harness.after.release();
                harness.barrier(true).await;
                wait(|| harness.pair.context().snapshot().unwrap().messages.len() > 1).await;
                assert_eq!(capability.status(), SpeculationStatus::Released);
                harness.send(Frame::new(FrameData::LlmRun)).await;
                let (body, _) = peer.request().await;
                assert_eq!(
                    body["contents"][0]["parts"],
                    json!([{"text":"Review ready!"}])
                );
                assert_eq!(
                    body["contents"][1]["parts"], parts,
                    "actual next Gemini HTTP request lost exact signed/trailing parts"
                );
                harness.barrier(true).await;
                assert_eq!(peer.request_count(), 2);
            } else {
                harness.source.revoke();
                harness.stop().await;
                harness.barrier(true).await;
                assert_eq!(capability.status(), SpeculationStatus::Cancelled);
                assert_eq!(harness.llm.speculation_snapshot().await.buffered_frames, 0);
                harness.assert_no_history();
                assert_eq!(
                    peer.request_count(),
                    1,
                    "retired signed final created fallback inference"
                );
                assert!(!harness.output.has(|data| matches!(
                    data,
                    FrameData::LlmText(_)
                        | FrameData::LlmThoughtText(_)
                        | FrameData::LlmContinuationCheckpoint
                )));
                assert!(harness
                    .output
                    .frames
                    .lock()
                    .unwrap()
                    .iter()
                    .all(|frame| frame.frame.native_llm_continuation().unwrap().is_none()));
            }
            harness.finish(false).await;
        }
    }
}

#[cfg(test)]
mod independent_live_turn_replacement_review {
    use super::*;

    #[tokio::test]
    async fn review_live_old_receipt_after_unbound_start_never_reaches_http_or_output() {
        for stop_first in [false, true] {
            let mut peer = Peer::new(Some(answer("B answer"))).await;
            let (harness, effects) = effect_harness(peer.endpoint.clone()).await;
            harness.start().await;
            let (a_final, a_stop) = harness.source.held_final("old live A");
            let a_receipt = harness.source.final_receipt();
            // Deliberately do not revoke/restart the source. Only the consumer
            // sees a genuine unbound replacement start; core A remains live.
            harness
                .accept_start(unbound(Frame::new(FrameData::ProposedUserStartedSpeaking)))
                .await;
            assert!(
                a_receipt.is_live(),
                "source liveness would mask current-turn gate control"
            );
            harness
                .send_applied(unbound(transcript(FrameData::Transcription, "B buffered")))
                .await;
            let deadline = harness.pair.user().deadline().unwrap();
            let stops = harness
                .output
                .count(|data| matches!(data, FrameData::UserStoppedSpeaking));
            deliver_ordered(&harness, a_final, a_stop, stop_first).await;
            harness.barrier(true).await;
            assert_eq!(
                peer.request_count(),
                0,
                "live foreign old turn reached actual HTTP inference"
            );
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            harness.assert_no_history();
            assert_eq!(harness.pair.user().deadline().unwrap(), deadline);
            assert_eq!(
                harness
                    .output
                    .count(|data| matches!(data, FrameData::UserStoppedSpeaking)),
                stops
            );
            deliver_ordered(
                &harness,
                unbound(transcript(FrameData::Transcription, "B final")),
                unbound(Frame::new(FrameData::ProposedUserStoppedSpeaking)),
                false,
            )
            .await;
            harness.barrier(true).await;
            let (request, _) = peer.request().await;
            assert_eq!(request["messages"][0]["content"], "B buffered B final");
            assert!(!request.to_string().contains("old live A"));
            assert_eq!(peer.request_count(), 1);
            assert_eq!(effects.load(Ordering::SeqCst), 0);
            harness.finish(false).await;
        }
    }
}
