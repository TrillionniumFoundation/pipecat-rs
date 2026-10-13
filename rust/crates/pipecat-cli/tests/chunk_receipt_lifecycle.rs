//! Public ReceiptSession/ReceiptBackend lifecycle tests over real loopback WS.
//! These complement the real-provider CLI tests without private-ledger access.
#[path = "support/chunk_fixture.rs"]
mod fixture;
use fixture::{bounded, config, control, next, send, start_user, TestResult, CHUNK_BYTES};
use pipecat_cli::receipt_output::{
    ReceiptBackend, ReceiptConfig, ReceiptOutput, ReceiptRunConfig, ReceiptRunReport,
    ReceiptSession,
};
use pipecat_core::*;
use pipecat_media_transport::*;
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use pipecat_services::{Content, Context, Role};
use pipecat_transports::{Packet, WebSocketSession};
use pipecat_turns::{
    AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, SharedContext,
    UserAggregatorConfig,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpListener,
    sync::{oneshot, Notify, Semaphore},
    task::JoinHandle,
};

fn require(condition: bool, message: impl Into<String>) -> TestResult<()> {
    if condition {
        Ok(())
    } else {
        Err(message.into())
    }
}
fn receipt(meta: &Value) -> Value {
    json!({"type":"chunk-complete","version":2,"session_id":meta["session_id"],"epoch":meta["epoch"],"chunk_id":meta["chunk_id"],"receipt_nonce":meta["receipt_nonce"]})
}
fn cutoff_receipt(mut value: Value) -> Value {
    value["type"] = json!("cutoff-complete");
    value
}
fn pcm_frame(value: u8, interruptible: bool) -> Frame {
    let mut frame = Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![value; CHUNK_BYTES], 24000, 1).unwrap(),
    ));
    frame.interruptible = interruptible;
    frame
}
fn speech(id: &str, text: &str, value: u8) -> Vec<Frame> {
    let mut pcm = AudioRawFrame::new(vec![value; CHUNK_BYTES], 24000, 1).unwrap();
    pcm.context_id = Some(id.into());
    vec![
        Frame::new(FrameData::LlmResponseStart),
        Frame::new(FrameData::TtsStarted(TtsStartedFrame {
            context_id: Some(id.into()),
            append_to_context: true,
        })),
        Frame::new(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new(text),
            context_id: Some(id.into()),
            aggregated_by: "word".into(),
            raw_text: None,
            will_be_spoken: true,
        })),
        Frame::new(FrameData::TtsAudio(pcm)),
        Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some(id.into()),
        })),
        Frame::new(FrameData::LlmResponseEnd),
    ]
}
struct Script;
#[async_trait]
impl FrameProcessor for Script {
    fn name(&self) -> &str {
        "new-generation-source"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::InputText(text) if text.text == "new speech") {
            for generated in speech("new-context", "New utterance", 9) {
                ctx.push(generated, direction).await?;
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct BlockInput {
    started: Arc<Notify>,
    gate: Arc<Semaphore>,
    completed: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for BlockInput {
    fn name(&self) -> &str {
        "bounded-input-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let is_audio = matches!(frame.data, FrameData::InputAudio(_));
        if is_audio {
            self.started.notify_one();
            let permit = self
                .gate
                .acquire()
                .await
                .map_err(|_| ProcessorError::new("test gate closed"))?;
            permit.forget();
        }
        if is_audio {
            // This synthetic ingress sink isolates input-pump admission. Burst
            // forwarding after release would test unrelated downstream System
            // queues, whose ctx.push intentionally fails instead of waiting.
            self.completed.fetch_add(1, Ordering::SeqCst);
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
struct LowFixture {
    client: Option<WebSocketSession>,
    owner: JoinHandle<TestResult<ReceiptRunReport>>,
    backend: Arc<ReceiptBackend>,
    shared: SharedContext,
}
impl LowFixture {
    async fn start(
        initial: Vec<Frame>,
        processors: Vec<Arc<dyn FrameProcessor>>,
        queue_capacity: usize,
    ) -> TestResult<Self> {
        Self::start_custom(
            initial,
            processors,
            vec![],
            queue_capacity,
            Duration::from_secs(2),
            Duration::from_secs(2),
            true,
        )
        .await
    }
    async fn start_custom(
        initial: Vec<Frame>,
        processors: Vec<Arc<dyn FrameProcessor>>,
        tail: Vec<Arc<dyn FrameProcessor>>,
        queue_capacity: usize,
        shutdown_timeout: Duration,
        drain_timeout: Duration,
        wait_ready: bool,
    ) -> TestResult<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let url = format!("ws://{}", listener.local_addr().map_err(|e| e.to_string())?);
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            UserAggregatorConfig::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        );
        let shared = pair.context();
        let (ready, waiting) = oneshot::channel();
        let owner = tokio::spawn(async move {
            let stream = listener.accept().await.map_err(|e| e.to_string())?.0;
            let session = WebSocketSession::accept(stream, config())
                .await
                .map_err(|e| e.to_string())?;
            let receipt = ReceiptSession::negotiate(
                session,
                ReceiptConfig {
                    handshake_timeout: Duration::from_millis(500),
                    chunk_timeout: Duration::from_secs(2),
                    enqueue_timeout: Duration::from_millis(500),
                    cutoff_timeout: Duration::from_millis(500),
                    drain_timeout,
                },
                24000,
            )
            .await
            .map_err(|e| e.to_string())?;
            let backend = receipt.backend();
            let history = SpokenHistory::new(
                SpokenHistoryConfig::default(),
                Arc::new(AssistantSpokenHistorySink::new(pair.assistant())),
            )
            .map_err(|e| e.to_string())?;
            let output = Arc::new(
                ReceiptOutput::new(
                    "real-shared-output",
                    OutputTransportConfig {
                        sample_rate: Some(24000),
                        chunk_duration: Duration::from_millis(40),
                        queue_capacity: 8,
                        queue_byte_capacity: 256 * 1024,
                        auto_silence: false,
                        end_silence: Duration::ZERO,
                        write_timeout: Duration::from_secs(3),
                        ..Default::default()
                    },
                    backend.clone(),
                    Some(history.clone()),
                )
                .map_err(|e| e.to_string())?,
            );
            let control = output.control();
            let mut chain = processors;
            chain.push(Arc::new(
                history
                    .source_boundary("test-source")
                    .map_err(|e| e.to_string())?,
            ));
            chain.push(output);
            chain.extend(tail);
            chain.push(pair.assistant());
            let _ = ready.send((backend, control));
            receipt
                .run(
                    Pipeline::new(chain),
                    PipelineConfig {
                        queue_capacity,
                        // Wire InputAudio/InputText use the core System lane.
                        system_queue_capacity: queue_capacity,
                        shutdown_timeout,
                        ..Default::default()
                    },
                    ReceiptRunConfig {
                        input_rate: 16000,
                        idle_timeout: Duration::from_secs(10),
                        history: Some(history),
                        initial_frames: initial,
                    },
                )
                .await
                .map_err(|e| e.to_string())
        });
        let connected = WebSocketSession::connect(&url, config()).await;
        let mut client = match connected {
            Ok(client) => client,
            Err(e) => {
                owner.abort();
                let _ = owner.await;
                return Err(e.to_string());
            }
        };
        let setup: TestResult<(Arc<ReceiptBackend>, OutputControl)> = async {
            let offer = control(next(&mut client).await?)?;
            require(offer["type"] == "playback-v2-offer", "lower session omitted negotiation")?;
            send(&client, json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],"chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true})).await?;
            bounded(async { waiting.await.map_err(|e| e.to_string()) }).await
        }.await;
        match setup {
            Ok((backend, output_control)) => {
                let fixture = Self {
                    client: Some(client),
                    owner,
                    backend,
                    shared,
                };
                let ready = bounded(async {
                    while wait_ready && !output_control.is_ready() {
                        tokio::task::yield_now().await;
                    }
                    Ok(())
                })
                .await;
                if let Err(e) = ready {
                    let closed = fixture.close().await;
                    return Err(format!("{e}; cleanup={:?}", closed.1));
                }
                Ok(fixture)
            }
            Err(e) => {
                let _ = client.cancel().await;
                owner.abort();
                let _ = owner.await;
                Err(e)
            }
        }
    }
    fn client(&mut self) -> TestResult<&mut WebSocketSession> {
        self.client.as_mut().ok_or_else(|| "client closed".into())
    }
    async fn chunk(&mut self) -> TestResult<(pb::AudioRawFrame, Value)> {
        let kind = next(self.client()?).await?;
        let Kind::Audio(audio) = kind else {
            return Err(format!("expected PCM, got {kind:?}"));
        };
        let meta = serde_json::from_str(&audio.name).map_err(|e| e.to_string())?;
        Ok((audio, meta))
    }
    async fn quiet(&mut self) -> TestResult<()> {
        match tokio::time::timeout(Duration::from_millis(50), self.client()?.recv()).await {
            Err(_) => Ok(()),
            other => Err(format!("progress before exact receipt: {other:?}")),
        }
    }
    async fn finish(&mut self) -> TestResult<()> {
        send(self.client()?, json!({"type":"end"})).await?;
        bounded(async {
            while let Some(packet) = self.client()?.recv().await {
                let Packet::Binary(bytes) = packet else {
                    return Err("non-protobuf terminal packet".into());
                };
                let kind = pb::Frame::parse(&bytes)
                    .map_err(|e| e.to_string())?
                    .frame
                    .ok_or("missing terminal frame")?;
                let value = control(kind)?;
                if value["type"] != "playback-cutoff" {
                    return Err(format!("unexpected terminal control {value}"));
                }
                send(self.client()?, cutoff_receipt(value)).await?;
            }
            Ok(())
        })
        .await
    }
    async fn close(
        self,
    ) -> (
        TestResult<ReceiptRunReport>,
        Vec<String>,
        TestResult<Context>,
    ) {
        self.close_mode(false, false).await
    }
    async fn close_after_abort(
        self,
    ) -> (
        TestResult<ReceiptRunReport>,
        Vec<String>,
        TestResult<Context>,
    ) {
        self.close_mode(false, true).await
    }
    // Only the dedicated controlled-cleanup test may use this exception.
    async fn close_allowing_unverified_cleanup(
        self,
    ) -> (
        TestResult<ReceiptRunReport>,
        Vec<String>,
        TestResult<Context>,
    ) {
        self.close_mode(true, true).await
    }
    async fn close_mode(
        mut self,
        allow_unverified_cleanup: bool,
        expected_peer_abort: bool,
    ) -> (
        TestResult<ReceiptRunReport>,
        Vec<String>,
        TestResult<Context>,
    ) {
        let mut errors = Vec::new();
        if let Some(client) = self.client.take() {
            match client.cancel().await {
                Ok(()) => eprintln!("client terminal outcome: joined normally"),
                Err(error) => {
                    let outcome = fixture::peer_exit(error);
                    eprintln!("client terminal outcome: joined with {outcome}");
                    if !(expected_peer_abort && outcome.starts_with("expected peer shutdown: ")) {
                        errors.push(outcome);
                    }
                }
            }
        }
        let result = match tokio::time::timeout(Duration::from_secs(5), &mut self.owner).await {
            Ok(Ok(result)) => result,
            Ok(Err(e)) => Err(e.to_string()),
            Err(_) => {
                self.owner.abort();
                let _ = self.owner.await;
                errors.push("owner cleanup required abort; abort joined".into());
                Err("owner cleanup deadline".into())
            }
        };
        if !allow_unverified_cleanup
            && result
                .as_ref()
                .is_err_and(|error| error.contains(fixture::JOIN_UNVERIFIED))
        {
            errors.push("ordinary session returned an unverified pipeline join".into());
        }
        eprintln!(
            "low-fixture owner error={:?}; cleanup={errors:?}",
            result.as_ref().err()
        );
        (
            result,
            errors,
            self.shared.snapshot().map_err(|e| e.to_string()),
        )
    }
}
async fn join_play(
    mut task: JoinHandle<Result<PlaybackReceipt, ProcessorError>>,
) -> TestResult<PlaybackReceipt> {
    match tokio::time::timeout(Duration::from_secs(1), &mut task).await {
        Ok(Ok(result)) => result.map_err(|e| e.to_string()),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("play future needed forced abort; abort joined".into())
        }
    }
}

#[tokio::test]
async fn actual_negotiated_backend_keeps_independent_video_disabled() {
    let mut fixture = LowFixture::start(vec![], vec![], 1).await.expect("fixture");
    let result: TestResult<()> = async {
        require(
            !fixture.backend.supports_independent_video(),
            "shared websocket backend cannot claim independent image I/O",
        )?;
        let output = OutputTransport::new(
            "capability",
            OutputTransportConfig::default(),
            fixture.backend.clone(),
            None,
            BTreeMap::new(),
        )
        .map_err(|e| e.to_string())?;
        require(
            output
                .with_live_video(LiveVideoOutputConfig {
                    queue_capacity: 1,
                    queue_byte_capacity: 16,
                    framerate: None,
                })
                .is_err(),
            "with_live_video accepted the actual receipt backend",
        )?;
        fixture.finish().await
    }
    .await;
    let (owner, errors, _) = fixture.close().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(owner.is_ok(), "{:?}", owner.err());
}

#[tokio::test]
async fn dropping_an_admitted_play_future_requires_cutoff_before_new_pcm() {
    for interruptible in [true, false] {
        let mut fixture = LowFixture::start(vec![], vec![], 1).await.expect("fixture");
        let backend = fixture.backend.clone();
        let mut first = Some(tokio::spawn(async move {
            backend.play_audio(&pcm_frame(3, interruptible)).await
        }));
        let mut interruption = None;
        let result: TestResult<()> = async {
            let (_, old) = fixture.chunk().await?;
            if let Some(task) = first.take() {
                let prematurely_finished = task.is_finished();
                task.abort();
                let dropped = task.await;
                require(
                    !prematurely_finished && dropped.as_ref().is_err_and(|e| e.is_cancelled()),
                    "public play future completed before a client receipt",
                )?;
            }
            let cutoff = control(next(fixture.client()?).await?)?;
            require(
                cutoff["type"] == "playback-cutoff" && cutoff["all"] == !interruptible,
                "dropped future did not request the matching selective/all-audio cutoff",
            )?;
            send(fixture.client()?, receipt(&old)).await?;
            fixture.quiet().await?;
            // This is a pure request Drop, with no external socket/media gate.
            // interrupt waits for the exact network flush. Socket-origin gates
            // are tested separately through the actual wrapper's double marker.
            let backend = fixture.backend.clone();
            interruption = Some(tokio::spawn(async move { backend.interrupt(None).await }));
            send(fixture.client()?, cutoff_receipt(cutoff)).await?;
            let mut interrupt = interruption.take().ok_or("missing interrupt owner")?;
            let barrier = tokio::time::timeout(Duration::from_secs(1), &mut interrupt).await;
            let barrier = match barrier {
                Ok(result) => result
                    .map_err(|e| e.to_string())?
                    .map_err(|e| e.to_string()),
                Err(_) => {
                    interrupt.abort();
                    let _ = interrupt.await;
                    Err("cutoff barrier did not settle".into())
                }
            };
            barrier?;
            let backend = fixture.backend.clone();
            let current =
                tokio::spawn(async move { backend.play_audio(&pcm_frame(7, true)).await });
            let observed: TestResult<()> = async {
                let (pcm, new) = fixture.chunk().await?;
                require(
                    pcm.audio == vec![7; CHUNK_BYTES]
                        && new["epoch"].as_u64() > old["epoch"].as_u64()
                        && new["chunk_id"].as_u64() > old["chunk_id"].as_u64(),
                    "new generation identity/PCM differs",
                )?;
                send(fixture.client()?, receipt(&old)).await?;
                fixture.quiet().await?;
                send(fixture.client()?, receipt(&new)).await
            }
            .await;
            let played = join_play(current).await;
            observed?;
            require(
                matches!(played, Ok(PlaybackReceipt::Played)),
                format!("new PCM lacks exact Played: {played:?}"),
            )?;
            fixture.finish().await
        }
        .await;
        if let Some(task) = first.take() {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = interruption.take() {
            task.abort();
            let _ = task.await;
        }
        let (owner, errors, _) = fixture.close().await;
        assert!(errors.is_empty(), "{errors:?}");
        assert!(result.is_ok(), "{result:?}");
        assert!(owner.is_ok(), "{:?}", owner.err());
    }
}

#[tokio::test]
async fn ordinary_cutoff_preserves_an_inflight_uninterruptible_chunk() {
    let mut fixture = LowFixture::start(vec![], vec![], 1).await.expect("fixture");
    let backend = fixture.backend.clone();
    let play = tokio::spawn(async move { backend.play_audio(&pcm_frame(5, false)).await });
    let result: TestResult<()> = async {
        let (_, old) = fixture.chunk().await?;
        fixture
            .backend
            .interrupt(None)
            .await
            .map_err(|e| e.to_string())?;
        fixture.quiet().await?;
        require(
            !play.is_finished(),
            "uninterruptible play completed before client report",
        )?;
        send(fixture.client()?, receipt(&old)).await
    }
    .await;
    let played = join_play(play).await;
    let finished = fixture.finish().await;
    let (owner, errors, _) = fixture.close().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(matches!(played, Ok(PlaybackReceipt::Played)), "{played:?}");
    assert!(finished.is_ok(), "{finished:?}");
    assert!(owner.is_ok(), "{:?}", owner.err());
}

#[tokio::test]
async fn ordinary_ingress_saturation_does_not_block_exact_chunk_receipts() {
    let started = Arc::new(Notify::new());
    let gate = Arc::new(Semaphore::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let blocker = Arc::new(BlockInput {
        started: started.clone(),
        gate: gate.clone(),
        completed: completed.clone(),
    });
    let mut fixture = LowFixture::start(vec![], vec![blocker], 1)
        .await
        .expect("fixture");
    let backend = fixture.backend.clone();
    let play = tokio::spawn(async move { backend.play_audio(&pcm_frame(6, true)).await });
    let result: TestResult<()> = async {
        let (_, chunk) = fixture.chunk().await?;
        start_user(fixture.client()?).await?;
        bounded(async {
            started.notified().await;
            Ok(())
        })
        .await?;
        // First System callback is held; system_queue_capacity=1 plus four
        // ordered socket inputs necessarily leaves input-pump admission pending
        // before the following ACK can be read from this same WebSocket.
        for _ in 0..4 {
            start_user(fixture.client()?).await?;
        }
        fixture.quiet().await?;
        require(
            completed.load(Ordering::SeqCst) == 0,
            "the System input callback was not held before ACK",
        )?;
        require(
            !play.is_finished(),
            "play completed without an exact client report",
        )?;
        send(fixture.client()?, receipt(&chunk)).await
    }
    .await;
    let played = join_play(play).await; // Must resolve while the ordinary gate is held.
    gate.add_permits(16);
    let finished = fixture.finish().await;
    let (owner, errors, _) = fixture.close().await;
    eprintln!("saturation observations: input={result:?}, playback={played:?}, finish={finished:?}, consumed={}", completed.load(Ordering::SeqCst));
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(
        matches!(played, Ok(PlaybackReceipt::Played)),
        "ACK blocked behind ordinary admission: {played:?}"
    );
    assert!(finished.is_ok(), "{finished:?}");
    assert!(owner.is_ok(), "{:?}", owner.err());
    assert_eq!(completed.load(Ordering::SeqCst), 5, "every input ordered before ACK must survive bounded pending admission and reach the blocked processor");
}

#[tokio::test]
async fn overflowing_the_bounded_input_pump_fails_closed_instead_of_buffering_forever() {
    let started = Arc::new(Notify::new());
    let gate = Arc::new(Semaphore::new(0));
    let completed = Arc::new(AtomicUsize::new(0));
    let blocker = Arc::new(BlockInput {
        started: started.clone(),
        gate: gate.clone(),
        completed: completed.clone(),
    });
    let mut fixture = LowFixture::start(vec![], vec![blocker], 1)
        .await
        .expect("fixture");
    let result: TestResult<()> = async {
        start_user(fixture.client()?).await?;
        bounded(async {
            started.notified().await;
            Ok(())
        })
        .await?;
        for _ in 0..32 {
            if start_user(fixture.client()?).await.is_err() {
                break;
            }
        }
        bounded(async {
            require(
                fixture.client()?.recv().await.is_none(),
                "overflow emitted unexpected output",
            )
        })
        .await
    }
    .await;
    gate.add_permits(64);
    let (owner, errors, _) = fixture.close_after_abort().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(
        owner
            .as_ref()
            .is_err_and(|error| error.contains("v2 input backpressure limit reached")),
        "overflow must fail at the bounded input pump: {:?}",
        owner.as_ref().err()
    );
}

#[tokio::test]
async fn cutoff_ack_reopens_real_shared_output_for_only_the_new_speech_generation() {
    let mut fixture = LowFixture::start(
        speech("old-context", "Old utterance", 3),
        vec![Arc::new(Script)],
        8,
    )
    .await
    .expect("fixture");
    let result: TestResult<()> = async {
        let (_, old) = fixture.chunk().await?;
        let packet = pipecat_cli::wire::packet(Kind::Text(pb::TextFrame {
            id: 99,
            name: "new input".into(),
            text: "new speech".into(),
        }))
        .map_err(|e| e.to_string())?;
        fixture
            .client()?
            .sender()
            .send(packet)
            .await
            .map_err(|e| e.to_string())?;
        let cutoff = control(next(fixture.client()?).await?)?;
        require(
            cutoff["type"] == "playback-cutoff" && cutoff["all"] == false,
            "new user input did not fence old interruptible PCM",
        )?;
        send(fixture.client()?, receipt(&old)).await?;
        let mut wrong = cutoff_receipt(cutoff.clone());
        wrong["cutoff_id"] = json!(cutoff["cutoff_id"].as_u64().ok_or("cutoff id")? + 1);
        send(fixture.client()?, wrong).await?;
        fixture.quiet().await?;
        require(
            fixture
                .shared
                .snapshot()
                .map_err(|e| e.to_string())?
                .messages
                .is_empty(),
            "old speech committed while cutoff pending",
        )?;
        send(fixture.client()?, cutoff_receipt(cutoff)).await?;
        let (pcm, new) = fixture.chunk().await?;
        require(
            pcm.audio == vec![9; CHUNK_BYTES] && new["epoch"].as_u64() > old["epoch"].as_u64(),
            "new-generation PCM was not admitted after exact cutoff",
        )?;
        send(fixture.client()?, receipt(&old)).await?;
        fixture.quiet().await?;
        send(fixture.client()?, receipt(&new)).await?;
        let committed = control(next(fixture.client()?).await?)?;
        require(
            committed["type"] == "playback-v2-context-committed"
                && committed["committed_contexts"] == 1,
            "new generation did not commit through real shared history",
        )?;
        fixture.finish().await
    }
    .await;
    let (owner, errors, context) = fixture.close().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(owner.is_ok(), "{:?}", owner.err());
    let messages = context.unwrap().messages;
    assert_eq!(messages.len(), 1);
    assert_eq!(messages[0].role, Role::Assistant);
    assert_eq!(
        messages[0].content,
        Some(Content::Text("New utterance".into()))
    );
}

// Delay the actual driver, rather than mutating its private ledger, to make
// Registered-but-unsent public play cancellation deterministic.
async fn registered_drop_fixture() -> TestResult<(LowFixture, bool)> {
    use std::{future::Future, task::Poll};
    let listener = TcpListener::bind("127.0.0.1:0")
        .await
        .map_err(|e| e.to_string())?;
    let url = format!("ws://{}", listener.local_addr().map_err(|e| e.to_string())?);
    let mut negotiation = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.map_err(|e| e.to_string())?;
        let session = WebSocketSession::accept(socket, config())
            .await
            .map_err(|e| e.to_string())?;
        ReceiptSession::negotiate(session, ReceiptConfig::default(), 24000)
            .await
            .map_err(|e| e.to_string())
    });
    let mut client = match WebSocketSession::connect(&url, config()).await {
        Ok(client) => client,
        Err(e) => {
            negotiation.abort();
            let _ = negotiation.await;
            return Err(e.to_string());
        }
    };
    let exchange: TestResult<()> = async {
        let offer = control(next(&mut client).await?)?;
        send(&client, json!({"type":"playback-v2-accept","version":2,"session_id":offer["session_id"],"chunk_receipts":true,"cutoff_receipts":true,"flush_on_disconnect":true})).await
    }.await;
    if let Err(e) = exchange {
        let _ = client.cancel().await;
        negotiation.abort();
        let _ = negotiation.await;
        return Err(e);
    }
    let receipt = match tokio::time::timeout(Duration::from_secs(3), &mut negotiation).await {
        Ok(Ok(Ok(receipt))) => receipt,
        Ok(Ok(Err(e))) => {
            let _ = client.cancel().await;
            return Err(e);
        }
        Ok(Err(e)) => {
            let _ = client.cancel().await;
            return Err(e.to_string());
        }
        Err(_) => {
            let _ = client.cancel().await;
            negotiation.abort();
            let _ = negotiation.await;
            return Err("negotiation deadline; abort joined".into());
        }
    };
    let backend = receipt.backend();
    if let Err(e) = backend
        .start(
            pipecat_audio::PcmFormat::new(24000, 1).map_err(|e| e.to_string())?,
            &[None],
        )
        .await
    {
        backend.fail_closed();
        let _ = client.cancel().await;
        let _ = receipt.close().await;
        return Err(e.to_string());
    }
    let frame = pcm_frame(3, true);
    let mut playing = Box::pin(backend.play_audio(&frame));
    let registered_pending =
        std::future::poll_fn(|cx| Poll::Ready(matches!(playing.as_mut().poll(cx), Poll::Pending)))
            .await;
    drop(playing); // Command was queued, but no driver could enqueue any packet.
    let owner = tokio::spawn(async move {
        receipt
            .run(
                Pipeline::new(vec![]),
                PipelineConfig::default(),
                ReceiptRunConfig {
                    input_rate: 16000,
                    idle_timeout: Duration::from_secs(3),
                    history: None,
                    initial_frames: vec![],
                },
            )
            .await
            .map_err(|e| e.to_string())
    });
    Ok((
        LowFixture {
            client: Some(client),
            owner,
            backend,
            shared: SharedContext::new(Context::default()),
        },
        registered_pending,
    ))
}

#[tokio::test]
async fn dropping_registered_unsent_audio_never_publishes_it_and_stop_is_idempotent() {
    let (mut fixture, registered_pending) = registered_drop_fixture().await.expect("fixture");
    let result: TestResult<()> = async {
        require(
            registered_pending,
            "play did not reach pending registration",
        )?;
        fixture.quiet().await?; // No stale PCM and no unnecessary remote cutoff.
        fixture
            .backend
            .interrupt(None)
            .await
            .map_err(|e| e.to_string())?;
        let backend = fixture.backend.clone();
        let current = tokio::spawn(async move { backend.play_audio(&pcm_frame(11, true)).await });
        let observed: TestResult<()> = async {
            let (pcm, meta) = fixture.chunk().await?;
            require(
                pcm.audio == vec![11; CHUNK_BYTES],
                "unsent cancelled PCM leaked onto the socket",
            )?;
            send(fixture.client()?, receipt(&meta)).await
        }
        .await;
        let played = join_play(current).await;
        observed?;
        require(
            matches!(played, Ok(PlaybackReceipt::Played)),
            format!("fresh play failed: {played:?}"),
        )?;
        fixture.finish().await
    }
    .await;
    let backend = fixture.backend.clone();
    let (owner, errors, _) = fixture.close().await;
    let first_stop = backend.stop().await;
    let second_stop = backend.stop().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(owner.is_ok(), "{:?}", owner.err());
    assert!(
        first_stop.is_ok() && second_stop.is_ok(),
        "closed-owner stop must be idempotent"
    );
}

struct FailSetup {
    release: Arc<Semaphore>,
    entered: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for FailSetup {
    fn name(&self) -> &str {
        "controlled-partial-start-failure"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.entered.fetch_add(1, Ordering::SeqCst);
        self.release
            .acquire()
            .await
            .map_err(|_| ProcessorError::new("setup release lost"))?
            .forget();
        Err(ProcessorError::new(
            "controlled setup failure after real output start",
        ))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
struct HoldCleanup {
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    finished: Arc<AtomicUsize>,
    changed: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for HoldCleanup {
    fn name(&self) -> &str {
        "controlled-cleanup-hold"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.entered.notify_one();
        self.release
            .acquire()
            .await
            .map_err(|_| ProcessorError::new("cleanup release lost"))?
            .forget();
        self.finished.fetch_add(1, Ordering::SeqCst);
        self.changed.notify_one();
        Ok(())
    }
}

#[tokio::test]
async fn partial_pipeline_start_failure_closes_the_negotiated_socket_and_joins() {
    let release = Arc::new(Semaphore::new(0));
    let entered = Arc::new(AtomicUsize::new(0));
    let failing = Arc::new(FailSetup {
        release: release.clone(),
        entered: entered.clone(),
    });
    // Setup is sequential: this tail processor enters only after the actual
    // ReceiptOutput/backend setup. Waiting for Start/is_ready would deadlock
    // because Start cannot run until this held setup callback has returned.
    let setup = LowFixture::start_custom(
        vec![],
        vec![],
        vec![failing],
        1,
        Duration::from_secs(2),
        Duration::from_secs(2),
        false,
    )
    .await;
    if setup.is_err() {
        release.add_permits(1);
    }
    let mut fixture = setup.expect("partially started real output");
    let reached = bounded(async {
        while entered.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        Ok(())
    })
    .await;
    release.add_permits(1);
    let result: TestResult<()> = async {
        reached?;
        bounded(async {
            require(
                fixture.client()?.recv().await.is_none(),
                "partial startup emitted PCM or success receipt",
            )
        })
        .await
    }
    .await;
    let backend = fixture.backend.clone();
    let (owner, errors, context) = fixture.close().await;
    let reused = backend.play_audio(&pcm_frame(3, true)).await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(owner.is_err(), "partial startup returned success");
    assert_eq!(entered.load(Ordering::SeqCst), 1);
    assert!(reused.is_err(), "partially started session was reused");
    assert!(context.unwrap().messages.is_empty());
}

#[tokio::test]
async fn cleanup_budget_returns_join_unverified_then_external_release_observes_completion() {
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Semaphore::new(0));
    let finished = Arc::new(AtomicUsize::new(0));
    let changed = Arc::new(Notify::new());
    let held = Arc::new(HoldCleanup {
        entered: entered.clone(),
        release: release.clone(),
        finished: finished.clone(),
        changed: changed.clone(),
    });
    // Core's longer deadline permits this externally owned test callback to
    // finish after the public receipt owner truthfully gives up its join proof.
    let setup = LowFixture::start_custom(
        vec![],
        vec![held],
        vec![],
        1,
        Duration::from_secs(5),
        Duration::from_millis(150),
        true,
    )
    .await;
    if setup.is_err() {
        release.add_permits(1);
    }
    let mut fixture = setup.expect("fixture");
    let backend = fixture.backend.clone();
    let play = tokio::spawn(async move { backend.play_audio(&pcm_frame(4, true)).await });
    let result: TestResult<()> = async {
        let _ = fixture.chunk().await?;
        send(fixture.client()?, json!({"type":"cancel"})).await?;
        bounded(async {
            entered.notified().await;
            Ok(())
        })
        .await?;
        bounded(async {
            require(
                fixture.client()?.recv().await.is_none(),
                "cleanup timeout emitted PCM or success receipt",
            )
        })
        .await
    }
    .await;
    let played = join_play(play).await;
    let backend = fixture.backend.clone();
    let (owner, errors, _) = fixture.close_allowing_unverified_cleanup().await;
    let finished_before_release = finished.load(Ordering::SeqCst);
    let reused = backend.play_audio(&pcm_frame(8, true)).await;
    release.add_permits(1);
    let external_completion = bounded(async {
        while finished.load(Ordering::SeqCst) == 0 {
            changed.notified().await;
        }
        Ok(())
    })
    .await;
    // The signal proves only this externally held callback finished later.
    // It does not retrospectively turn the API's unverified join into success.
    eprintln!(
        "cleanup owner returned {:?}; external held callback finished later={:?}",
        owner.as_ref().err(),
        external_completion
    );
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(played.is_err(), "unreported PCM became Played: {played:?}");
    assert!(
        owner
            .as_ref()
            .is_err_and(|e| e.contains(fixture::JOIN_UNVERIFIED)),
        "public cleanup boundary differs: {:?}",
        owner.err()
    );
    assert_eq!(finished_before_release, 0);
    assert!(reused.is_err(), "timed-out session was reusable");
    assert!(external_completion.is_ok(), "{external_completion:?}");
}

struct HoldInterruption {
    release: Arc<Semaphore>,
    entered: Arc<AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for HoldInterruption {
    fn name(&self) -> &str {
        "hold-media-cutoff-mark"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Interruption) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            self.release
                .acquire()
                .await
                .map_err(|_| ProcessorError::new("interrupt release lost"))?
                .forget();
        }
        ctx.push(frame, direction).await
    }
}
struct ObserveText(Arc<std::sync::Mutex<Vec<String>>>);
#[async_trait]
impl FrameProcessor for ObserveText {
    fn name(&self) -> &str {
        "observe-retained-user-text"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::InputText(text) = &frame.data {
            self.0.lock().unwrap().push(text.text.clone());
        }
        ctx.push(frame, direction).await
    }
}
async fn input_text(client: &WebSocketSession, text: &str) -> TestResult<()> {
    let packet = pipecat_cli::wire::packet(Kind::Text(pb::TextFrame {
        id: 99,
        name: "controlled user input".into(),
        text: text.into(),
    }))
    .map_err(|e| e.to_string())?;
    client
        .sender()
        .send(packet)
        .await
        .map_err(|e| e.to_string())
}

#[tokio::test]
async fn overlapping_socket_cutoffs_revoke_pump_pending_text_before_the_latest_media_release() {
    let cutoff_release = Arc::new(Semaphore::new(0));
    let cutoff_entered = Arc::new(AtomicUsize::new(0));
    let seen_text = Arc::new(std::sync::Mutex::new(Vec::new()));
    let mut fixture = LowFixture::start(
        speech("old-context", "Old utterance", 3),
        vec![
            Arc::new(HoldInterruption {
                release: cutoff_release.clone(),
                entered: cutoff_entered.clone(),
            }),
            Arc::new(ObserveText(seen_text.clone())),
            Arc::new(Script),
        ],
        8,
    )
    .await
    .expect("fixture");
    let result: TestResult<()> = async {
        let (_, old) = fixture.chunk().await?;
        // Each InputText is System once core-admitted, so this scenario holds
        // the first Interruption before the media mark instead of incorrectly
        // expecting a blocked InputAudio System callback to be preempted.
        // Text stays in the CLI input pump behind its closed media gate; the
        // second socket cutoff must discard the first still-pending text.
        input_text(fixture.client()?, "stale queued speech").await?;
        let cutoff = control(next(fixture.client()?).await?)?;
        require(
            cutoff["type"] == "playback-cutoff",
            "missing physical flush fence",
        )?;
        bounded(async {
            while cutoff_entered.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
            Ok(())
        })
        .await?;
        input_text(fixture.client()?, "new speech").await?;
        send(fixture.client()?, receipt(&old)).await?;
        fixture.quiet().await?;
        require(
            seen_text.lock().unwrap().is_empty(),
            "ordinary input escaped while media mark/flush was pending",
        )?;
        cutoff_release.add_permits(8);
        send(fixture.client()?, cutoff_receipt(cutoff)).await?;
        let (pcm, current) = fixture.chunk().await?;
        require(
            pcm.audio == vec![9; CHUNK_BYTES] && current["epoch"].as_u64() > old["epoch"].as_u64(),
            "latest cutoff failed to reopen only new-generation media",
        )?;
        send(fixture.client()?, receipt(&current)).await?;
        let committed = control(next(fixture.client()?).await?)?;
        require(
            committed["type"] == "playback-v2-context-committed"
                && committed["committed_contexts"] == 1,
            "latest source did not commit",
        )?;
        fixture.finish().await
    }
    .await;
    // Release the externally held System callback even on an observation error.
    cutoff_release.add_permits(32);
    let (owner, errors, context) = fixture.close().await;
    assert!(errors.is_empty(), "{errors:?}");
    assert!(result.is_ok(), "{result:?}");
    assert!(owner.is_ok(), "{:?}", owner.err());
    assert!(
        cutoff_entered.load(Ordering::SeqCst) >= 2,
        "both socket cutoffs must reach the real media path"
    );
    assert_eq!(
        *seen_text.lock().unwrap(),
        vec!["new speech"],
        "old queued normal text survived the newer socket cutoff"
    );
    let context = context.unwrap();
    assert_eq!(context.messages.len(), 1);
    assert_eq!(
        context.messages[0].content,
        Some(Content::Text("New utterance".into()))
    );
}
