//! Playback-confirmed output. Network delivery never counts as spoken audio.
use crate::{wire, AgentError};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, InvocationFence, ProcessorContext,
    ProcessorError, StartFrame,
};
use pipecat_transports::{Packet, SessionSender};
use pipecat_turns::{AggregationAction, AggregationOutput, LLMAssistantAggregator};
use serde_json::json;
use std::{
    collections::VecDeque,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};
const MAX_PENDING_CONTEXTS: usize = 16;
const MAX_PENDING_TEXT: usize = 1024 * 1024;

struct Pending {
    id: String,
    texts: Vec<Frame>,
    ready: bool,
    created: Instant,
    context: ProcessorContext,
    cause: InvocationFence,
}
#[derive(Default)]
struct State {
    pending: VecDeque<Pending>,
    context: Option<ProcessorContext>,
    text_bytes: usize,
    source_failed: bool,
}
pub struct Outbound {
    packet: Packet,
    generation: u64,
}
pub struct OutputChannels {
    ordinary: mpsc::Receiver<Outbound>,
    urgent: mpsc::Receiver<Packet>,
    generation: Arc<AtomicU64>,
}
pub struct PlaybackOutput {
    assistant: Arc<LLMAssistantAggregator>,
    state: Mutex<State>,
    ordinary: mpsc::Sender<Outbound>,
    urgent: mpsc::Sender<Packet>,
    generation: Arc<AtomicU64>,
}
impl PlaybackOutput {
    pub fn new(assistant: Arc<LLMAssistantAggregator>) -> (Arc<Self>, OutputChannels) {
        let (ordinary, ordinary_rx) = mpsc::channel(128);
        let (urgent, urgent_rx) = mpsc::channel(16);
        let generation = Arc::new(AtomicU64::new(0));
        (
            Arc::new(Self {
                assistant,
                state: Mutex::new(State::default()),
                ordinary,
                urgent,
                generation: generation.clone(),
            }),
            OutputChannels {
                ordinary: ordinary_rx,
                urgent: urgent_rx,
                generation,
            },
        )
    }
    /// Verify and atomically commit a complete remote playback acknowledgement.
    /// The state lock orders acknowledgement against interruption, while the
    /// aggregator commits synchronously instead of through overtaken queues.
    pub async fn acknowledge(&self, id: String) -> Result<(), AgentError> {
        let (ctx, cause, effects) = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| AgentError::Protocol("output state poisoned"))?;
            if !state.pending.front().is_some_and(|p| p.id == id && p.ready) {
                return Ok(());
            }
            let pending = state.pending.pop_front().expect("front checked");
            let texts: Vec<_> = pending
                .texts
                .iter()
                .filter_map(|frame| match &frame.data {
                    FrameData::TtsText(text) => Some(text.clone()),
                    _ => None,
                })
                .collect();
            let effects = self.assistant.confirm_playback(&id, &texts)?;
            for text in &texts {
                state.text_bytes = state.text_bytes.saturating_sub(text.text.text.len());
            }
            // Retain the acknowledged synthesis invocation, including nested
            // origins. The setup context cannot authorize delayed turn feedback.
            let ctx = pending.context;
            (ctx, pending.cause, effects)
        };
        for action in effects.actions {
            match action {
                AggregationAction::AssistantStopped(action) => action.admit_with_fence(&cause)?,
                AggregationAction::Push(frame)
                    if matches!(
                        frame.frame.data,
                        FrameData::LlmContextAssistantTimestamp { .. }
                            | FrameData::LlmContextAssistantTurn(_)
                    ) =>
                {
                    ctx.try_push_with_invocation_fence(frame.frame, frame.direction, &cause)?
                }
                action => {
                    AggregationOutput {
                        actions: vec![action],
                        events: vec![],
                    }
                    .emit(&ctx)
                    .await?
                }
            }
        }
        ctx.push(
            Frame::new(FrameData::BotStoppedSpeaking),
            Direction::Downstream,
        )
        .await?;
        self.control(json!({"type":"playback-acknowledged","context_id":id}))?;
        Ok(())
    }
    /// Invalidate pending playback immediately when an input interrupt is
    /// received, before asynchronous pipeline propagation can be overtaken.
    pub fn invalidate(&self) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
        state.pending.clear();
        state.text_bytes = 0;
        self.generation.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    pub fn playback_timed_out(&self, timeout: Duration) -> bool {
        self.state
            .lock()
            .map(|s| {
                s.pending
                    .front()
                    .is_some_and(|p| p.created.elapsed() > timeout)
            })
            .unwrap_or(true)
    }
    fn enqueue(&self, packet: Packet) -> Result<(), ProcessorError> {
        self.enqueue_at(packet, self.generation.load(Ordering::SeqCst))
    }
    fn enqueue_at(&self, packet: Packet, generation: u64) -> Result<(), ProcessorError> {
        self.ordinary
            .try_send(Outbound { packet, generation })
            .map_err(|_| ProcessorError::fatal("websocket output queue is full or closed"))
    }
    fn control(&self, value: serde_json::Value) -> Result<(), ProcessorError> {
        self.enqueue(
            wire::message(value)
                .map_err(|_| ProcessorError::fatal("cannot encode output control"))?,
        )
    }
}
#[async_trait]
impl FrameProcessor for PlaybackOutput {
    fn name(&self) -> &str {
        "WebSocketPlaybackOutput"
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("output state poisoned"))?
            .context = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        if matches!(
            frame.data,
            FrameData::LlmResponseStart
                | FrameData::LlmFullResponseStart(_)
                | FrameData::VisionFullResponseStart(_)
        ) {
            self.state
                .lock()
                .map_err(|_| ProcessorError::fatal("output state poisoned"))?
                .source_failed = true;
            let ready = self
                .assistant
                .playback_readiness(&frame, ctx.invocation_fence()?)?
                .ok_or_else(|| ProcessorError::fatal("assistant source readiness unavailable"))?;
            ctx.push(frame, direction).await?;
            if let Err(error) = tokio::time::timeout(Duration::from_secs(10), ready.wait())
                .await
                .map_err(|_| ProcessorError::fatal("assistant source readiness timed out"))
                .and_then(|result| result)
            {
                self.invalidate()?;
                if !ctx.invocation_fence()?.is_current() {
                    return Ok(());
                }
                return Err(ProcessorError::fatal(error.message));
            }
            self.state
                .lock()
                .map_err(|_| ProcessorError::fatal("output state poisoned"))?
                .source_failed = false;
            return Ok(());
        }
        match &frame.data {
            FrameData::TtsStarted(start) => {
                let id = start.context_id.as_ref().ok_or_else(|| {
                    ProcessorError::fatal(
                        "TTS context ID is required for playback acknowledgements",
                    )
                })?;
                let generation = {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
                    if state.source_failed {
                        return Err(ProcessorError::fatal(
                            "assistant source boundary was not ready",
                        ));
                    }
                    if state.pending.len() >= MAX_PENDING_CONTEXTS
                        || state.pending.iter().any(|p| &p.id == id)
                    {
                        return Err(ProcessorError::fatal(
                            "too many or duplicate pending playback contexts",
                        ));
                    }
                    self.assistant.begin_playback(id)?;
                    state.pending.push_back(Pending {
                        id: id.clone(),
                        texts: vec![],
                        ready: false,
                        created: Instant::now(),
                        context: ctx.clone(),
                        cause: ctx.invocation_fence()?,
                    });
                    self.generation.load(Ordering::SeqCst)
                };
                self.enqueue_at(wire::message(json!({"type":"playback-start","context_id":id,"sample_rate":ctx.configuration().audio_out_sample_rate,"channels":1,"encoding":"pcm_s16le"})).map_err(|_|ProcessorError::fatal("cannot encode playback start"))?,generation)?;
                // Only acknowledged contexts enter the assistant aggregator;
                // confirming one context must not invalidate later queued audio.
                return Ok(());
            }
            FrameData::TtsText(text) => {
                let id = text
                    .context_id
                    .as_ref()
                    .ok_or_else(|| ProcessorError::fatal("TTS text context ID missing"))?;
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
                if state.text_bytes.saturating_add(text.text.text.len()) > MAX_PENDING_TEXT {
                    return Err(ProcessorError::fatal(
                        "unacknowledged TTS text limit exceeded",
                    ));
                }
                state.text_bytes += text.text.text.len();
                let Some(pending) = state.pending.iter_mut().find(|p| &p.id == id) else {
                    state.text_bytes = state.text_bytes.saturating_sub(text.text.text.len());
                    return Ok(());
                };
                pending.cause = pending.cause.intersection(&ctx.invocation_fence()?)?;
                pending.texts.push(frame);
                return Ok(());
            }
            FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => {
                let generation = {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
                    let Some(pending) = state
                        .pending
                        .iter_mut()
                        .find(|p| Some(p.id.as_str()) == audio.context_id.as_deref())
                    else {
                        return Ok(());
                    };
                    pending.cause = pending.cause.intersection(&ctx.invocation_fence()?)?;
                    self.generation.load(Ordering::SeqCst)
                };
                self.enqueue_at(
                    wire::audio(&frame, audio)
                        .map_err(|_| ProcessorError::fatal("cannot encode output audio"))?,
                    generation,
                )?;
            }
            FrameData::TtsStopped(stop) => {
                let id = stop
                    .context_id
                    .as_ref()
                    .ok_or_else(|| ProcessorError::fatal("TTS stop context ID missing"))?;
                let generation = {
                    let mut state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
                    let Some(pending) = state.pending.iter_mut().find(|p| &p.id == id) else {
                        return Ok(());
                    };
                    pending.cause = pending.cause.intersection(&ctx.invocation_fence()?)?;
                    pending.ready = true;
                    self.generation.load(Ordering::SeqCst)
                };
                self.enqueue_at(
                    wire::message(json!({"type":"playback-ready","context_id":id}))
                        .map_err(|_| ProcessorError::fatal("cannot encode playback completion"))?,
                    generation,
                )?;
            }
            FrameData::Interruption => {
                self.invalidate()?;
                self.urgent
                    .try_send(
                        wire::interruption(&frame)
                            .map_err(|_| ProcessorError::fatal("cannot encode interruption"))?,
                    )
                    .map_err(|_| {
                        ProcessorError::fatal("websocket interruption queue is full or closed")
                    })?;
            }
            FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
            | FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
                state.pending.clear();
                state.text_bytes = 0;
            }
            _ => {}
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
        state.context = None;
        state.pending.clear();
        state.text_bytes = 0;
        Ok(())
    }
}
impl OutputChannels {
    /// The packet pump is separate from processor calls. Interruptions bypass
    /// unsent audio; packets already sent still require the client to flush its
    /// playback buffer when it receives the interruption frame.
    pub async fn run(mut self, sender: SessionSender) -> Result<(), AgentError> {
        loop {
            let packet = tokio::select! {
                biased;
                packet=self.urgent.recv()=>match packet {Some(p)=>p,None=>return Ok(())},
                outbound=self.ordinary.recv()=>match outbound {
                    Some(o) if o.generation==self.generation.load(Ordering::SeqCst)=>o.packet,
                    Some(_)=>continue,
                    None=>return Ok(()),
                }
            };
            tokio::select! {
                biased;
                urgent=self.urgent.recv()=> {
                    if let Some(urgent)=urgent {
                        tokio::time::timeout(Duration::from_secs(5),sender.send(urgent)).await.map_err(|_|AgentError::Protocol("output backpressure timeout"))??;
                    } else {return Ok(());}
                }
                sent=tokio::time::timeout(Duration::from_secs(5),sender.send(packet))=>sent.map_err(|_|AgentError::Protocol("output backpressure timeout"))??,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{
        AggregatedTextFrame, FlushFrame, Pipeline, PipelineConfig, PipelineWorker, TextFrame,
        TtsStartedFrame, TtsStoppedFrame,
    };
    use pipecat_services::{Content, Context, Role};
    use pipecat_turns::{
        AssistantAggregatorConfig, AssistantTextMode, LLMContextAggregatorPair, SharedContext,
        UserAggregatorConfig,
    };
    async fn fixture() -> (
        Arc<PlaybackOutput>,
        OutputChannels,
        PipelineWorker,
        SharedContext,
    ) {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            UserAggregatorConfig::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        );
        let (output, channels) = PlaybackOutput::new(pair.assistant());
        let shared = pair.context();
        let worker = Pipeline::new(vec![output.clone(), pair.assistant()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        (output, channels, worker, shared)
    }
    async fn send(worker: &PipelineWorker, data: FrameData) {
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
    }
    async fn fence(worker: &mut PipelineWorker) {
        let marker = Frame::new(FrameData::Flush(FlushFrame {
            origin: Some("test-fence".into()),
            returning: false,
        }));
        let id = marker.id;
        worker
            .sender()
            .send(marker, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while let Some(frame) = worker.recv().await {
                if frame.frame.id == id {
                    return;
                }
            }
            panic!("pipeline ended before fence");
        })
        .await
        .unwrap();
    }
    async fn start_text(worker: &PipelineWorker, id: &str) {
        send(worker, FrameData::LlmResponseStart).await;
        send(
            worker,
            FrameData::TtsStarted(TtsStartedFrame {
                context_id: Some(id.into()),
                append_to_context: true,
            }),
        )
        .await;
        send(
            worker,
            FrameData::TtsText(AggregatedTextFrame {
                text: TextFrame::new("Actually heard"),
                aggregated_by: "test".into(),
                context_id: Some(id.into()),
                raw_text: None,
                will_be_spoken: true,
            }),
        )
        .await;
    }
    #[tokio::test]
    async fn sent_audio_and_provider_done_never_commit_without_playback_ack() {
        let (output, _channels, mut worker, shared) = fixture().await;
        start_text(&worker, "one").await;
        send(
            &worker,
            FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some("one".into()),
            }),
        )
        .await;
        fence(&mut worker).await;
        assert!(shared.snapshot().unwrap().messages.is_empty());
        output.acknowledge("one".into()).await.unwrap();
        fence(&mut worker).await;
        let messages = shared.snapshot().unwrap().messages;
        assert_eq!(messages.len(), 1);
        assert_eq!(messages[0].role, Role::Assistant);
        assert_eq!(
            messages[0].content,
            Some(Content::Text("Actually heard".into()))
        );
        output.acknowledge("one".into()).await.unwrap();
        fence(&mut worker).await;
        assert_eq!(
            shared.snapshot().unwrap().messages.len(),
            1,
            "duplicate acks must not duplicate history"
        );
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }
    #[tokio::test]
    async fn acknowledgement_commits_before_a_following_interruption() {
        let (output, _channels, mut worker, shared) = fixture().await;
        start_text(&worker, "one").await;
        send(
            &worker,
            FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some("one".into()),
            }),
        )
        .await;
        fence(&mut worker).await;
        output.acknowledge("one".into()).await.unwrap();
        assert_eq!(
            shared.snapshot().unwrap().messages.len(),
            1,
            "confirmation must commit synchronously"
        );
        output.invalidate().unwrap();
        send(&worker, FrameData::Interruption).await;
        fence(&mut worker).await;
        assert_eq!(
            shared.snapshot().unwrap().messages.len(),
            1,
            "later system interruption must preserve confirmed speech"
        );
        worker.cancel("test done").await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }
    #[tokio::test]
    async fn multiple_pending_contexts_commit_in_playback_order() {
        let (output, _channels, mut worker, shared) = fixture().await;
        for id in ["one", "two"] {
            start_text(&worker, id).await;
            send(
                &worker,
                FrameData::TtsStopped(TtsStoppedFrame {
                    context_id: Some(id.into()),
                }),
            )
            .await;
        }
        fence(&mut worker).await;
        output.acknowledge("two".into()).await.unwrap();
        assert!(shared.snapshot().unwrap().messages.is_empty());
        output.acknowledge("one".into()).await.unwrap();
        output.acknowledge("two".into()).await.unwrap();
        assert_eq!(shared.snapshot().unwrap().messages.len(), 2);
        worker.cancel("test done").await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }
    #[tokio::test]
    async fn immediate_input_invalidation_rejects_late_ack_without_queue_wait() {
        let (output, _channels, mut worker, shared) = fixture().await;
        start_text(&worker, "one").await;
        send(
            &worker,
            FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some("one".into()),
            }),
        )
        .await;
        fence(&mut worker).await;
        output.invalidate().unwrap();
        output.acknowledge("one".into()).await.unwrap();
        assert!(shared.snapshot().unwrap().messages.is_empty());
        worker.cancel("test done").await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }
    #[tokio::test]
    async fn premature_and_interrupted_context_acks_cannot_invent_spoken_history() {
        let (output, _channels, mut worker, shared) = fixture().await;
        start_text(&worker, "one").await;
        fence(&mut worker).await;
        output.acknowledge("one".into()).await.unwrap();
        fence(&mut worker).await;
        assert!(shared.snapshot().unwrap().messages.is_empty());
        send(
            &worker,
            FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some("one".into()),
            }),
        )
        .await;
        fence(&mut worker).await;
        send(&worker, FrameData::Interruption).await;
        fence(&mut worker).await;
        output.acknowledge("one".into()).await.unwrap();
        fence(&mut worker).await;
        assert!(shared.snapshot().unwrap().messages.is_empty());
        worker.cancel("test done").await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }
    async fn receipt_effects_fence(
        worker: &mut PipelineWorker,
    ) -> Vec<pipecat_core::DirectedFrame> {
        tokio::time::timeout(Duration::from_secs(2), async {
            let mut effects = vec![];
            for direction in [Direction::Downstream, Direction::Upstream] {
                let marker = Frame::new(FrameData::Flush(FlushFrame {
                    origin: Some("feedback-fence".into()),
                    returning: false,
                }));
                let id = marker.id;
                worker.sender().send(marker, direction).await.unwrap();
                loop {
                    let frame = worker.recv().await.expect("pipeline feedback fence");
                    if frame.frame.id == id {
                        break;
                    }
                    if matches!(
                        frame.frame.data,
                        FrameData::LlmContext(_)
                            | FrameData::LlmContextDetailed(_)
                            | FrameData::LlmContextAssistantTurn(_)
                            | FrameData::LlmContextAssistantTimestamp { .. }
                    ) {
                        effects.push(frame);
                    }
                }
            }
            effects
        })
        .await
        .unwrap()
    }
    async fn feedback_fence(worker: &mut PipelineWorker) -> Vec<pipecat_core::DirectedFrame> {
        receipt_effects_fence(worker)
            .await
            .into_iter()
            .filter(|f| {
                matches!(
                    f.frame.data,
                    FrameData::LlmContextAssistantTurn(_)
                        | FrameData::LlmContextAssistantTimestamp { .. }
                )
            })
            .collect()
    }
    #[tokio::test]
    async fn playback_feedback_is_receipt_owned_linked_and_repeat_safe() {
        let (output, _channels, mut worker, shared) = fixture().await;
        for id in ["first", "second"] {
            start_text(&worker, id).await;
            send(
                &worker,
                FrameData::TtsStopped(TtsStoppedFrame {
                    context_id: Some(id.into()),
                }),
            )
            .await;
            assert!(feedback_fence(&mut worker).await.is_empty());
            output.acknowledge("wrong-context".into()).await.unwrap();
            assert!(feedback_fence(&mut worker).await.is_empty());
            output.acknowledge(id.into()).await.unwrap();
            let frames = feedback_fence(&mut worker).await;
            assert_eq!(frames.len(), 3);
            let turns: Vec<_> = frames
                .iter()
                .filter(|f| matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))
                .collect();
            assert_eq!(turns.len(), 2);
            assert_ne!(turns[0].direction, turns[1].direction);
            assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
            assert_eq!(turns[1].frame.broadcast_sibling_id, Some(turns[0].frame.id));
            assert!(turns.iter().all(|f| matches!(&f.frame.data, FrameData::LlmContextAssistantTurn(t) if t.text == "Actually heard")));
            output.acknowledge(id.into()).await.unwrap();
            assert!(feedback_fence(&mut worker).await.is_empty());
            // The next receipt must use its own generation, never setup's epoch.
            send(&worker, FrameData::Interruption).await;
            assert!(feedback_fence(&mut worker).await.is_empty());
        }
        assert_eq!(shared.snapshot().unwrap().messages.len(), 2);
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }

    #[derive(Default)]
    struct InvocationCapture(Mutex<Option<ProcessorContext>>);
    #[async_trait]
    impl FrameProcessor for InvocationCapture {
        fn name(&self) -> &str {
            "playback-feedback-origin"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::Text(_)) {
                *self.0.lock().unwrap() = Some(ctx.clone());
            }
            ctx.push(frame, direction).await
        }
    }
    #[tokio::test]
    async fn late_receipt_material_keeps_its_nested_origin_fence() {
        for late_text in [true, false] {
            let capture = Arc::new(InvocationCapture::default());
            let mut origin_worker = Pipeline::new(vec![capture.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            send(&origin_worker, FrameData::Text(TextFrame::new("origin"))).await;
            fence(&mut origin_worker).await;
            let source = capture.0.lock().unwrap().clone().unwrap();
            let (output, _channels, mut worker, shared) = fixture().await;
            let mut stopped_records = output
                .assistant
                .subscribe_stopped(Default::default())
                .unwrap();
            start_text(&worker, "scoped").await;
            assert!(feedback_fence(&mut worker).await.is_empty());
            let destination = output
                .state
                .lock()
                .unwrap()
                .pending
                .front()
                .unwrap()
                .context
                .clone();
            let nested = destination.forwarding_context(&source);
            if late_text {
                output
                    .process(
                        Frame::new(FrameData::TtsText(AggregatedTextFrame {
                            text: TextFrame::new("late text"),
                            aggregated_by: "test".into(),
                            context_id: Some("scoped".into()),
                            raw_text: None,
                            will_be_spoken: true,
                        })),
                        Direction::Downstream,
                        &nested,
                    )
                    .await
                    .unwrap();
                send(
                    &worker,
                    FrameData::TtsStopped(TtsStoppedFrame {
                        context_id: Some("scoped".into()),
                    }),
                )
                .await;
            } else {
                output
                    .process(
                        Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
                            context_id: Some("scoped".into()),
                        })),
                        Direction::Downstream,
                        &nested,
                    )
                    .await
                    .unwrap();
            }
            assert!(feedback_fence(&mut worker).await.is_empty());
            send(&origin_worker, FrameData::Interruption).await;
            fence(&mut origin_worker).await;
            assert!(!source.invocation_fence().unwrap().is_current());
            assert!(destination.invocation_fence().unwrap().is_current());
            output.acknowledge("scoped".into()).await.unwrap();
            // Notification staleness never rolls back the synchronous receipt commit.
            assert_eq!(shared.snapshot().unwrap().messages.len(), 1);
            assert!(stopped_records.try_recv().is_none());
            assert_eq!(stopped_records.stats().stale, 1);
            let effects = receipt_effects_fence(&mut worker).await;
            assert_eq!(
                effects
                    .iter()
                    .filter(|f| matches!(
                        f.frame.data,
                        FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                    ))
                    .count(),
                1,
                "existing CLI context effects retain the acknowledged start scope"
            );
            assert!(!effects.iter().any(|f| matches!(
                f.frame.data,
                FrameData::LlmContextAssistantTurn(_)
                    | FrameData::LlmContextAssistantTimestamp { .. }
            )));
            worker.end().await.unwrap();
            assert!(worker.collect().await.unwrap().1.errors.is_empty());
            origin_worker.end().await.unwrap();
            assert!(origin_worker.collect().await.unwrap().1.errors.is_empty());
        }
    }
    #[tokio::test]
    async fn stopped_record_survives_cli_receipt_adapter_exactly_once() {
        let (output, _channels, mut worker, shared) = fixture().await;
        let mut records = output
            .assistant
            .subscribe_stopped(Default::default())
            .unwrap();
        for id in ["record-one", "record-two"] {
            start_text(&worker, id).await;
            send(
                &worker,
                FrameData::TtsStopped(TtsStoppedFrame {
                    context_id: Some(id.into()),
                }),
            )
            .await;
            fence(&mut worker).await;
            assert!(records.try_recv().is_none());
            output.acknowledge("wrong".into()).await.unwrap();
            assert!(records.try_recv().is_none());
            output.acknowledge(id.into()).await.unwrap();
            let record = records.try_recv().expect("CLI admitted completion");
            assert_eq!(record.completion().text(), "Actually heard");
            assert!(!record.completion().interrupted());
            assert!(shared
                .snapshot()
                .unwrap()
                .messages
                .iter()
                .any(|message| message.content == Some(Content::Text("Actually heard".into()))));
            output.acknowledge(id.into()).await.unwrap();
            assert!(records.try_recv().is_none());
            fence(&mut worker).await;
        }
        worker.end().await.unwrap();
        assert!(worker.collect().await.unwrap().1.errors.is_empty());
        assert!(records.recv().await.is_none());
    }
}

#[cfg(test)]
mod readiness_tests {
    use super::*;
    use pipecat_core::{
        AggregatedTextFrame, Pipeline, PipelineConfig, TextFrame, TtsStartedFrame, TtsStoppedFrame,
    };
    use pipecat_turns::{AssistantAggregatorConfig, AssistantTextMode, SharedContext};
    use tokio::sync::{Notify, Semaphore};
    struct HoldStart {
        entered: Notify,
        release: Semaphore,
    }
    #[async_trait]
    impl FrameProcessor for HoldStart {
        fn name(&self) -> &str {
            "cli-readiness-gate"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if direction == Direction::Downstream
                && matches!(frame.data, FrameData::LlmResponseStart)
            {
                self.entered.notify_one();
                self.release.acquire().await.unwrap().forget();
            }
            ctx.push(frame, direction).await
        }
    }
    #[tokio::test]
    async fn source_start_is_accepted_before_remote_playback_is_admitted() {
        let shared = SharedContext::default();
        let assistant = Arc::new(LLMAssistantAggregator::new(
            shared.clone(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        ));
        let (output, mut channels) = PlaybackOutput::new(assistant.clone());
        let hold = Arc::new(HoldStart {
            entered: Notify::new(),
            release: Semaphore::new(0),
        });
        let worker = Pipeline::new(vec![output.clone(), hold.clone(), assistant])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        for data in [
            FrameData::LlmResponseStart,
            FrameData::TtsStarted(TtsStartedFrame {
                context_id: Some("held".into()),
                append_to_context: true,
            }),
            FrameData::TtsText(AggregatedTextFrame {
                text: TextFrame::new("Heard"),
                context_id: Some("held".into()),
                raw_text: None,
                aggregated_by: "test".into(),
                will_be_spoken: true,
            }),
            FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some("held".into()),
            }),
            FrameData::LlmResponseEnd,
        ] {
            worker
                .sender()
                .send(Frame::new(data), Direction::Downstream)
                .await
                .unwrap();
        }
        tokio::time::timeout(Duration::from_secs(2), hold.entered.notified())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), channels.ordinary.recv())
                .await
                .is_err()
        );
        assert!(shared.snapshot().unwrap().messages.is_empty());
        hold.release.add_permits(1);
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                if output
                    .state
                    .lock()
                    .unwrap()
                    .pending
                    .front()
                    .is_some_and(|pending| pending.ready)
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        output.acknowledge("held".into()).await.unwrap();
        assert_eq!(shared.snapshot().unwrap().messages.len(), 1);
        worker.end().await.unwrap();
        let (_, report) = worker.collect().await.unwrap();
        assert!(report.errors.is_empty());
    }
}
