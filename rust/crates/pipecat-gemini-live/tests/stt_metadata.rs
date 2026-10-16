//! Dedicated-STT metadata comes from the real Gemini processor, then reaches the
//! standard user aggregator. Local WebSocket fixtures make no hosted-provider or
//! measured-latency claim: 0.60 is the pinned Python service benchmark default.
//! Python pin: af9f9da9d7afddaa8811c0e0251b32566c980bdf, stt_latency.py
//! SHA256 cd1b1969ceedb228965d88ecc2a9e10bb9c615e3e7c8b332fe6b89d7c2e12227.
//! Native Name/Local filtering is deliberate; Python broadcasts for any target.
use futures_util::{SinkExt, StreamExt};
use pipecat_core::*;
use pipecat_gemini_live::*;
use pipecat_turns::{LLMUserAggregator, SharedContext, UserAggregatorConfig, UserTurnConfig};
use serde_json::{json, Value};
use std::{
    cell::Cell,
    collections::HashSet,
    future::Future,
    sync::{Arc, Mutex},
    task::Poll,
    time::Duration,
};
use tokio::{
    net::TcpListener,
    task::JoinHandle,
    time::{timeout, Instant},
};
use tokio_tungstenite::tungstenite::Message;

// The complete affected old graph is retained once: speaker labels (5), audio
// pause (5), live (28), STT mute (4), and input audio families (13): 55 tests.
#[path = "stt_speaker_labels.rs"]
mod speaker_label_regressions;

const WAIT: Duration = Duration::from_secs(5);
const NAME: &str = "GeminiLiveProcessor";
macro_rules! require {
    ($condition:expr, $($message:tt)+) => {
        if !$condition { return Err(format!($($message)+)); }
    };
}

#[derive(Clone, Copy)]
enum Latency {
    Default,
    Explicit(Option<f64>),
}
impl Latency {
    fn advertised(self) -> f64 {
        match self {
            Self::Default => 0.60,
            Self::Explicit(value) => value.unwrap_or(1.0),
        }
    }
    fn apply(self, processor: GeminiLiveProcessor) -> Result<GeminiLiveProcessor, GeminiError> {
        match self {
            Self::Default => Ok(processor),
            Self::Explicit(value) => processor.with_ttfs_p99_latency(value),
        }
    }
}
#[derive(Clone, Copy)]
enum Case {
    Starts,
    Requests,
    Conversation,
    Timing { floor_ms: u64, deadline_ms: u64 },
}
#[derive(Debug)]
struct Trace {
    setup: Value,
    wire: Vec<Value>,
    closed: bool,
}
struct Peer(JoinHandle<Result<Trace, String>>);
impl Peer {
    async fn open(mode: SessionMode) -> Result<(Self, GeminiConfig), String> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(|e| e.to_string())?;
        let mut config = if mode == SessionMode::Transcription {
            GeminiConfig::transcription("metadata-key", "metadata-model")
        } else {
            GeminiConfig::new("metadata-key", "metadata-model")
        };
        config.endpoint = format!(
            "ws://{}/live",
            listener.local_addr().map_err(|e| e.to_string())?
        );
        config.limits.finish_timeout = Duration::from_secs(2);
        let peer = tokio::spawn(async move {
            let (tcp, _) = timeout(WAIT, listener.accept())
                .await
                .map_err(|_| "accept deadline")?
                .map_err(|e| e.to_string())?;
            let mut ws = timeout(WAIT, tokio_tungstenite::accept_async(tcp))
                .await
                .map_err(|_| "upgrade deadline")?
                .map_err(|e| e.to_string())?;
            let first = timeout(WAIT, ws.next())
                .await
                .map_err(|_| "setup deadline")?
                .ok_or("no setup")?
                .map_err(|e| e.to_string())?;
            let setup: Value = serde_json::from_str(first.to_text().map_err(|e| e.to_string())?)
                .map_err(|e| e.to_string())?;
            let mut trace = Trace {
                setup,
                wire: vec![],
                closed: false,
            };
            timeout(
                WAIT,
                ws.send(Message::Text(
                    json!({"setupComplete":{}}).to_string().into(),
                )),
            )
            .await
            .map_err(|_| "ready deadline")?
            .map_err(|e| e.to_string())?;
            loop {
                let message = timeout(WAIT, ws.next())
                    .await
                    .map_err(|_| "peer read deadline")?;
                match message {
                    Some(Ok(Message::Text(text))) => {
                        let value: Value =
                            serde_json::from_str(&text).map_err(|e| e.to_string())?;
                        // A real VAD stop reaches the real transport. This reply
                        // deliberately has NO final transcript: finalization
                        // would release the STT safety gate ahead of its budget.
                        let end = value["realtimeInput"].get("audioStreamEnd").is_some();
                        trace.wire.push(value);
                        if end {
                            timeout(
                                WAIT,
                                ws.send(Message::Text(
                                    json!({"serverContent":{"turnComplete":true}})
                                        .to_string()
                                        .into(),
                                )),
                            )
                            .await
                            .map_err(|_| "completion deadline")?
                            .map_err(|e| e.to_string())?;
                        }
                    }
                    Some(Ok(Message::Ping(bytes))) => {
                        // Protocol housekeeping is never a pipeline-consumption witness.
                        timeout(WAIT, ws.send(Message::Pong(bytes)))
                            .await
                            .map_err(|_| "pong deadline")?
                            .map_err(|e| e.to_string())?;
                    }
                    Some(Ok(Message::Pong(_))) => {}
                    Some(Ok(Message::Close(_))) => {
                        trace.closed = true;
                        timeout(WAIT, ws.flush())
                            .await
                            .map_err(|_| "close flush deadline")?
                            .map_err(|e| e.to_string())?;
                        return Ok(trace);
                    }
                    other => return Err(format!("expected actual WS Close: {other:?}")),
                }
            }
        });
        Ok((Self(peer), config))
    }
    async fn join(mut self) -> Result<Trace, String> {
        match timeout(WAIT, &mut self.0).await {
            Ok(result) => result.map_err(|e| e.to_string()).and_then(|r| r),
            Err(error) => {
                self.0.abort();
                let _ = self.0.await;
                Err(format!("peer deadline; aborted and joined: {error}"))
            }
        }
    }
}

struct Tagged {
    frame: Frame,
    tag: LocalFrameTag,
    admission: Option<Frame>,
}
impl Tagged {
    fn new(data: FrameData) -> Result<Self, String> {
        let mut frame = Frame::new(data);
        frame.name = "metadata-original-envelope".into();
        frame.set_pipeline_pts(1234);
        frame.transport_source = Some("source".into());
        frame.transport_destination = Some("destination".into());
        frame.broadcast_sibling_id = Some(4567);
        frame.interruptible = true;
        frame
            .metadata
            .insert("metadata-test-original".into(), json!({"preserved":true}));
        let tag = LocalFrameTag::new();
        tag.mark(&mut frame).map_err(|e| e.to_string())?;
        Ok(Self {
            frame,
            tag,
            admission: None,
        })
    }
}
// Additional Start variants/directions are processor-routing probes, not a
// public PipelineSender API. Pipeline::start still owns the genuine initial
// lifecycle. These sources only translate a test control into the original
// tagged Start; metadata is always produced by the real Gemini processor.
#[derive(Default)]
struct StartSource(Mutex<Option<(u64, Frame)>>);
impl StartSource {
    fn prepare(&self, original: &mut Tagged) -> Result<(), String> {
        let mut pending = self.0.lock().map_err(|_| "start source lock poisoned")?;
        require!(pending.is_none(), "start source already has pending input");
        let trigger = Frame::new(FrameData::TaskSystem);
        *pending = Some((trigger.id, original.frame.clone()));
        original.admission = Some(trigger);
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for StartSource {
    fn name(&self) -> &str {
        "MetadataStartSource"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let replacement = {
            let mut pending = self
                .0
                .lock()
                .map_err(|_| ProcessorError::fatal("start source lock poisoned"))?;
            if pending.as_ref().is_some_and(|(id, _)| *id == frame.id) {
                pending.take().map(|(_, original)| original)
            } else {
                None
            }
        };
        ctx.push(replacement.unwrap_or(frame), direction).await
    }
}
async fn next(worker: &mut PipelineWorker, seen: &mut Vec<DirectedFrame>) -> Result<(), String> {
    let output = worker
        .recv()
        .await
        .ok_or("pipeline closed before witness")?;
    let error = if let FrameData::Error(error) = &output.frame.data {
        Some(format!("unexpected pipeline error: {error:?}"))
    } else {
        None
    };
    seen.push(output);
    if let Some(error) = error {
        return Err(error);
    }
    Ok(())
}
async fn send_original(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    original: &Tagged,
    direction: Direction,
) -> Result<(), String> {
    let begin = seen.len();
    worker
        .sender()
        .send(
            original
                .admission
                .as_ref()
                .unwrap_or(&original.frame)
                .clone(),
            direction,
        )
        .await
        .map_err(|e| e.to_string())?;
    loop {
        if let Some(actual) = seen[begin..]
            .iter()
            .find(|f| f.frame.id == original.frame.id && f.direction == direction)
        {
            require!(
                actual.frame == original.frame && original.tag.contains(&actual.frame),
                "original frame/envelope/local identity changed: {actual:?}"
            );
            return Ok(());
        }
        next(worker, seen).await?;
    }
}
async fn fence(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    direction: Direction,
) -> Result<(), String> {
    // Two same-System-lane witnesses also settle effects emitted after the
    // preceding original frame by the real user processor. They are not pings.
    for _ in 0..2 {
        send_original(
            worker,
            seen,
            &Tagged::new(FrameData::TaskSystem)?,
            direction,
        )
        .await?;
    }
    Ok(())
}
fn metadata(frames: &[DirectedFrame]) -> Vec<&DirectedFrame> {
    frames
        .iter()
        .filter(|f| matches!(f.frame.data, FrameData::SttMetadata(_)))
        .collect()
}
fn check_pair(
    frames: &[DirectedFrame],
    latency: f64,
    used: &mut HashSet<u64>,
) -> Result<(), String> {
    let pair = metadata(frames);
    require!(pair.len() == 2, "expected one metadata pair, got {pair:?}");
    let down = pair
        .iter()
        .find(|f| f.direction == Direction::Downstream)
        .ok_or("no downstream metadata")?;
    let up = pair
        .iter()
        .find(|f| f.direction == Direction::Upstream)
        .ok_or("no upstream metadata")?;
    require!(
        down.frame.id != up.frame.id
            && down.frame.broadcast_sibling_id == Some(up.frame.id)
            && up.frame.broadcast_sibling_id == Some(down.frame.id),
        "not fresh reciprocal siblings: {pair:?}"
    );
    require!(down.frame.data == up.frame.data, "sibling payload differs");
    for actual in pair {
        require!(
            used.insert(actual.frame.id),
            "metadata ID was reused: {}",
            actual.frame.id
        );
        let FrameData::SttMetadata(payload) = &actual.frame.data else {
            unreachable!()
        };
        require!(
            payload.base.service_name == NAME
                && payload.base.user_turn_strategies.is_none()
                && payload.ttfs_p99_latency == latency,
            "unexpected metadata payload: {payload:?}"
        );
        require!(
            actual.frame.metadata.is_empty()
                && actual.frame.transport_source.is_none()
                && actual.frame.transport_destination.is_none()
                && actual.frame.name != "metadata-original-envelope",
            "metadata inherited original envelope: {:?}",
            actual.frame
        );
    }
    Ok(())
}
async fn probe(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    original: &Tagged,
    direction: Direction,
    emits: bool,
    latency: f64,
    used: &mut HashSet<u64>,
) -> Result<(), String> {
    let begin = seen.len();
    send_original(worker, seen, original, direction).await?;
    fence(worker, seen, Direction::Downstream).await?;
    fence(worker, seen, Direction::Upstream).await?;
    let stage = &seen[begin..];
    if emits {
        check_pair(stage, latency, used)?;
        // Each adjacent queue has FIFO; no cross-outlet total-order claim.
        let lane: Vec<_> = stage.iter().filter(|f| f.direction == direction).collect();
        let original_at = lane
            .iter()
            .position(|f| f.frame.id == original.frame.id)
            .ok_or("lost original")?;
        let metadata_at = lane
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::SttMetadata(_)))
            .ok_or("lost same-direction metadata")?;
        require!(
            if original.frame.data.is_start() {
                original_at < metadata_at
            } else {
                metadata_at < original_at
            },
            "wrong local Start/request FIFO: {lane:?}"
        );
        require!(
            metadata(stage)
                .iter()
                .all(|f| f.frame.id != original.frame.id && !original.tag.contains(&f.frame)),
            "metadata reused original/private identity"
        );
    } else {
        require!(
            metadata(stage).is_empty(),
            "metadata emitted for non-target/conversation: {stage:?}"
        );
    }
    Ok(())
}
fn start_frame(detailed: bool) -> FrameData {
    if detailed {
        FrameData::StartDetailed(DetailedStartFrame {
            configuration: StartFrame::default(),
            enable_tracing: true,
            tracing_context: Some(TracingContext {
                conversation_id: Some("metadata-conversation".into()),
                ..Default::default()
            }),
        })
    } else {
        FrameData::Start(StartFrame::default())
    }
}
fn request(target: ProcessorReference) -> Result<Tagged, String> {
    Tagged::new(FrameData::ServiceSwitcherRequestMetadata(target))
}
async fn initial(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    dedicated: bool,
    latency: f64,
    used: &mut HashSet<u64>,
) -> Result<(), String> {
    while !seen.iter().any(|f| f.frame.data.is_start()) {
        next(worker, seen).await?;
    }
    fence(worker, seen, Direction::Downstream).await?;
    fence(worker, seen, Direction::Upstream).await?;
    if dedicated {
        check_pair(seen, latency, used)?;
        let lane: Vec<_> = seen
            .iter()
            .filter(|f| f.direction == Direction::Downstream)
            .collect();
        let start = lane
            .iter()
            .position(|f| f.frame.data.is_start())
            .ok_or("lost automatic Start")?;
        let meta = lane
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::SttMetadata(_)))
            .ok_or("lost startup metadata")?;
        require!(
            start < meta,
            "automatic Start must precede metadata on its lane"
        );
    } else {
        require!(
            metadata(seen).is_empty(),
            "conversation startup emitted STT metadata"
        );
    }
    Ok(())
}

// Keeping this future runnable prevents paused Tokio's idle auto-advance. A
// genuine wall-clock bound still terminates failed observations; no timeout,
// sleep, polling iteration count or unjoined spinner stands in for consumption.
async fn bounded_frozen<T>(future: impl Future<Output = Result<T, String>>) -> Result<T, String> {
    let wall_deadline = std::time::Instant::now() + WAIT;
    tokio::pin!(future);
    std::future::poll_fn(|cx| {
        if std::time::Instant::now() >= wall_deadline {
            return Poll::Ready(Err("frozen observation wall-clock deadline".into()));
        }
        match future.as_mut().poll(cx) {
            Poll::Ready(result) => Poll::Ready(result),
            Poll::Pending => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
        }
    })
    .await
}
struct FrozenClock;
impl FrozenClock {
    fn pause() -> Self {
        tokio::time::pause();
        Self
    }
}
impl Drop for FrozenClock {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}
fn stop_count(seen: &[DirectedFrame]) -> usize {
    seen.iter()
        .filter(|f| matches!(f.frame.data, FrameData::UserStoppedSpeaking))
        .count()
}
async fn timing(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
    latency: f64,
    floor_ms: u64,
    deadline_ms: u64,
    used: &mut HashSet<u64>,
) -> Result<(), String> {
    // initial() already observed genuine service metadata AFTER the standard
    // user consumer plus two same-System-lane outlets. Never inject metadata.
    let clock = FrozenClock::pause();
    let origin = Instant::now();
    let phase = Cell::new("construct VAD start");
    let result = bounded_frozen(async {
        let started = Tagged::new(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.1,
            timestamp: 0.0,
        }))?;
        phase.set("VAD start original outlet");
        send_original(worker, seen, &started, Direction::Downstream).await?;
        phase.set("VAD start consumer fence");
        fence(worker, seen, Direction::Downstream).await?;
        require!(
            seen.iter()
                .any(|f| matches!(f.frame.data, FrameData::UserStartedSpeaking)),
            "user never started a real turn"
        );
        let stopped = Tagged::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        }))?;
        phase.set("VAD stop original outlet");
        send_original(worker, seen, &stopped, Direction::Downstream).await?;
        phase.set("VAD stop consumer fence");
        fence(worker, seen, Direction::Downstream).await?;
        require!(
            Instant::now() == origin,
            "clock auto-advanced before VAD stop was consumed"
        );
        require!(stop_count(seen) == 0, "user stopped before its budget");
        phase.set("advance 100ms");
        tokio::time::advance(Duration::from_millis(100)).await;
        phase.set("100ms consumer fence");
        fence(worker, seen, Direction::Downstream).await?;
        require!(
            stop_count(seen) == 0,
            "STT metadata was not consumed: stopped at 100ms (floor={floor_ms})"
        );
        // A refresh is an observation, not a new VAD boundary or a deadline reset.
        let refresh = request(ProcessorReference::Name(NAME.into()))?;
        for repetition in 0..2 {
            phase.set(if repetition == 0 {
                "first metadata refresh"
            } else {
                "repeated metadata refresh"
            });
            probe(
                worker,
                seen,
                &refresh,
                Direction::Downstream,
                true,
                latency,
                used,
            )
            .await?;
        }
        require!(
            Instant::now().duration_since(origin) == Duration::from_millis(100),
            "metadata refresh moved frozen clock"
        );
        let mut elapsed_ms = 100;
        if floor_ms == 600 {
            phase.set("advance to 400ms floor witness");
            tokio::time::advance(Duration::from_millis(300)).await;
            phase.set("400ms floor consumer fence");
            fence(worker, seen, Direction::Downstream).await?;
            require!(
                stop_count(seen) == 0,
                "default 600ms floor failed to mask 400ms STT remainder"
            );
            require!(
                Instant::now().duration_since(origin) == Duration::from_millis(400),
                "floor witness advanced clock"
            );
            elapsed_ms = 400;
        }
        phase.set("advance to one ms before deadline");
        tokio::time::advance(Duration::from_millis(deadline_ms - elapsed_ms - 1)).await;
        phase.set("one ms before deadline consumer fence");
        fence(worker, seen, Direction::Downstream).await?;
        require!(
            Instant::now().duration_since(origin) == Duration::from_millis(deadline_ms - 1),
            "clock auto-advanced before deadline"
        );
        require!(stop_count(seen) == 0, "user stopped before {deadline_ms}ms");
        phase.set("advance to exact deadline");
        tokio::time::advance(Duration::from_millis(1)).await;
        // Tokio 1.53's timer wheel rounds deadlines up to milliseconds while
        // its current tick truncates. Startup happens on a real socket before
        // pause(), so this clock anchor can have a fractional-ms offset. Keep
        // the exact logical deadline and allow only the documented one-ms
        // wheel quantum for actual output, never a wall-clock timeout increase.
        phase.set("advance one ms timer-wheel observation quantum");
        tokio::time::advance(Duration::from_millis(1)).await;
        phase.set("actual timer bidirectional stop within one ms of deadline");
        // No synthetic marker at this boundary: the owned production timer must
        // produce the actual stop, with both real broadcast branches observed.
        while stop_count(seen) < 2 {
            next(worker, seen).await?;
        }
        require!(
            (Duration::from_millis(deadline_ms)..=Duration::from_millis(deadline_ms + 1))
                .contains(&Instant::now().duration_since(origin)),
            "actual stop exceeded the one-ms timer-wheel observation quantum"
        );
        let stops: Vec<_> = seen
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::UserStoppedSpeaking))
            .collect();
        require!(
            stops.len() == 2
                && stops.iter().any(|f| f.direction == Direction::Downstream)
                && stops.iter().any(|f| f.direction == Direction::Upstream),
            "not one real bidirectional stop: {stops:?}"
        );
        require!(
            seen.iter().all(|f| !matches!(
                f.frame.data,
                FrameData::Transcription(_)
                    | FrameData::InterimTranscription(_)
                    | FrameData::TranscriptionDetailed(_)
                    | FrameData::InterimTranscriptionDetailed(_)
            )),
            "a transcript contaminated the safety-budget test"
        );
        Ok(())
    })
    .await;
    let result = result.map_err(|error| {
        let tail: Vec<_> = seen.iter().rev().take(16).map(|f| {
            (f.frame.id, f.direction, f.frame.data.source_class_name())
        }).collect();
        format!("{error}; phase={}; frozen_elapsed={:?}; stop_count={}; seen_count={}; seen_tail_newest_first={tail:?}",
            phase.get(), origin.elapsed(), stop_count(seen), seen.len())
    });
    // Restore the clock on success AND observation error before owner cleanup.
    drop(clock);
    result
}

async fn drain_join(
    worker: &mut PipelineWorker,
    seen: &mut Vec<DirectedFrame>,
) -> Result<WorkerReport, String> {
    while let Some(frame) = worker.recv().await {
        seen.push(frame);
    }
    worker.join().await.map_err(|e| e.to_string())
}
async fn execute(mode: SessionMode, latency: Latency, case: Case) {
    let (peer, config) = Peer::open(mode).await.unwrap();
    let processor = Arc::new(
        latency
            .apply(GeminiLiveProcessor::new(GeminiClient::new(config).unwrap()))
            .unwrap(),
    );
    let start_sources = if matches!(case, Case::Starts | Case::Conversation) {
        Some((
            Arc::new(StartSource::default()),
            Arc::new(StartSource::default()),
        ))
    } else {
        None
    };
    let mut nodes: Vec<Arc<dyn FrameProcessor>> =
        if let Some((downstream_source, upstream_source)) = &start_sources {
            vec![
                downstream_source.clone(),
                processor.clone(),
                upstream_source.clone(),
            ]
        } else {
            vec![processor.clone()]
        };
    if let Case::Timing { floor_ms, .. } = case {
        let config = UserAggregatorConfig {
            turn: UserTurnConfig {
                speech_timeout: Duration::from_millis(floor_ms),
                wait_for_transcript: false,
                enable_interruptions: false,
                ..Default::default()
            },
            ..Default::default()
        };
        nodes.push(Arc::new(LLMUserAggregator::new(
            SharedContext::default(),
            config,
        )));
    }
    // Core owns startup timeout and joins setup work. Do not drop an outer
    // timeout future which could lose a newly returned Pipeline owner.
    let created = Pipeline::new(nodes)
        .start(PipelineConfig {
            startup_timeout: WAIT,
            shutdown_timeout: Duration::from_secs(3),
            ..Default::default()
        })
        .await;
    let mut worker = match created {
        Ok(worker) => worker,
        Err(error) => {
            let peer = peer.join().await;
            panic!("pipeline setup: {error}; peer={peer:?}");
        }
    };
    let mut seen = vec![];
    let mut used = HashSet::new();
    let observed = timeout(WAIT, async {
        initial(
            &mut worker,
            &mut seen,
            mode == SessionMode::Transcription,
            latency.advertised(),
            &mut used,
        )
        .await?;
        match case {
            Case::Starts | Case::Conversation => {
                for detailed in [false, true] {
                    for direction in [Direction::Downstream, Direction::Upstream] {
                        let mut original = Tagged::new(start_frame(detailed))?;
                        let (downstream_source, upstream_source) = start_sources
                            .as_ref()
                            .ok_or("missing Start routing source")?;
                        match direction {
                            Direction::Downstream => downstream_source.prepare(&mut original)?,
                            Direction::Upstream => upstream_source.prepare(&mut original)?,
                        }
                        probe(
                            &mut worker,
                            &mut seen,
                            &original,
                            direction,
                            mode == SessionMode::Transcription,
                            latency.advertised(),
                            &mut used,
                        )
                        .await?;
                    }
                }
            }
            _ => {}
        }
        if matches!(case, Case::Requests | Case::Conversation) {
            // Same display name but a different Arc must NOT match Local identity.
            let other = Arc::new(GeminiLiveProcessor::new(
                GeminiClient::new(GeminiConfig::transcription("other-key", "metadata-model"))
                    .map_err(|e| e.to_string())?,
            ));
            let targets = [
                (ProcessorReference::Name(NAME.into()), true),
                (
                    ProcessorReference::Local(LocalProcessorReference(processor.clone())),
                    true,
                ),
                (ProcessorReference::Name("another-service".into()), false),
                (
                    ProcessorReference::Local(LocalProcessorReference(other)),
                    false,
                ),
            ];
            for (target, matched) in targets {
                for direction in [Direction::Downstream, Direction::Upstream] {
                    let original = request(target.clone())?;
                    // Reuse this exact original ID: every matching request still
                    // advertises a new sibling pair, rather than being deduped.
                    for _ in 0..2 {
                        probe(
                            &mut worker,
                            &mut seen,
                            &original,
                            direction,
                            mode == SessionMode::Transcription && matched,
                            latency.advertised(),
                            &mut used,
                        )
                        .await?;
                    }
                }
            }
        }
        if let Case::Timing {
            floor_ms,
            deadline_ms,
        } = case
        {
            timing(
                &mut worker,
                &mut seen,
                latency.advertised(),
                floor_ms,
                deadline_ms,
                &mut used,
            )
            .await?;
        }
        Ok::<_, String>(())
    })
    .await
    .map_err(|_| "pipeline observation deadline".to_owned())
    .and_then(|r| r);
    let normal = observed.is_ok();
    let mut failures = observed.err().into_iter().collect::<Vec<_>>();
    let admitted = timeout(WAIT, async {
        if normal {
            worker.end().await
        } else {
            worker.cancel("metadata observation failed").await
        }
    })
    .await;
    if !matches!(admitted, Ok(Ok(()))) {
        failures.push(format!("terminal admission: {admitted:?}"));
        let _ = timeout(WAIT, worker.cancel("metadata terminal cleanup")).await;
    }
    let joined = match timeout(WAIT, drain_join(&mut worker, &mut seen)).await {
        Ok(report) => report,
        Err(error) => {
            failures.push(format!("owner join deadline: {error}"));
            let _ = timeout(WAIT, worker.cancel("metadata join cleanup")).await;
            // Join the actual owner even after a timed-out observation.
            drain_join(&mut worker, &mut seen).await
        }
    };
    let trace = peer.join().await;
    // All runtime and peer owners have been joined before assertions can unwind.
    assert!(
        failures.is_empty(),
        "{failures:?}; owner={joined:?}; peer={trace:?}"
    );
    let report = joined.unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.total_errors, 0);
    assert!(report.errors.is_empty());
    assert_eq!(report.evicted_errors, 0);
    assert!(seen
        .iter()
        .all(|f| !matches!(f.frame.data, FrameData::Error(_))));
    let trace = trace.unwrap();
    assert!(trace.closed, "normal End must produce actual peer WS Close");
    assert_eq!(trace.setup["setup"]["model"], "models/metadata-model");
    if matches!(case, Case::Timing { .. }) {
        assert_eq!(
            trace.wire,
            vec![json!({"realtimeInput":{"audioStreamEnd":true}})]
        );
        assert_eq!(stop_count(&seen), 2, "End must not fabricate a second stop");
    } else {
        assert!(
            trace.wire.is_empty(),
            "metadata/requests must create no provider work: {trace:?}"
        );
    }
}

#[tokio::test]
async fn dedicated_start_preserves_envelope_before_fresh_bidirectional_metadata() {
    for invalid in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.001, 300.001] {
        let processor = GeminiLiveProcessor::new(
            GeminiClient::new(GeminiConfig::transcription("key", "model")).unwrap(),
        );
        assert!(processor.with_ttfs_p99_latency(Some(invalid)).is_err());
    }
    for latency in [
        Latency::Default,
        Latency::Explicit(None),
        Latency::Explicit(Some(0.0)),
        Latency::Explicit(Some(300.0)),
    ] {
        execute(SessionMode::Transcription, latency, Case::Starts).await;
    }
}
#[tokio::test]
async fn targeted_requests_repeat_fresh_pairs_before_unchanged_request() {
    execute(SessionMode::Transcription, Latency::Default, Case::Requests).await;
}
#[tokio::test]
async fn conversation_never_emits_stt_metadata() {
    execute(
        SessionMode::Conversation,
        Latency::Explicit(None),
        Case::Conversation,
    )
    .await;
}
#[tokio::test]
async fn real_user_stop_waits_for_default_ttfs_minus_vad() {
    execute(
        SessionMode::Transcription,
        Latency::Default,
        Case::Timing {
            floor_ms: 100,
            deadline_ms: 400,
        },
    )
    .await;
}
#[tokio::test]
async fn real_user_stop_uses_none_one_second_fallback() {
    execute(
        SessionMode::Transcription,
        Latency::Explicit(None),
        Case::Timing {
            floor_ms: 100,
            deadline_ms: 800,
        },
    )
    .await;
}
#[tokio::test]
async fn default_speech_floor_masks_default_ttfs_remainder() {
    assert_eq!(
        UserTurnConfig::default().speech_timeout,
        Duration::from_millis(600)
    );
    execute(
        SessionMode::Transcription,
        Latency::Default,
        Case::Timing {
            floor_ms: 600,
            deadline_ms: 600,
        },
    )
    .await;
}
