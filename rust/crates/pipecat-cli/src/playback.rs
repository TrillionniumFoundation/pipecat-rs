//! Playback-confirmed output. Network delivery never counts as spoken audio.
use crate::{wire, AgentError};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    StartFrame,
};
use pipecat_transports::{Packet, SessionSender};
use pipecat_turns::LLMAssistantAggregator;
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
}
#[derive(Default)]
struct State {
    pending: VecDeque<Pending>,
    context: Option<ProcessorContext>,
    text_bytes: usize,
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
        let (ctx, effects) = {
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
            let ctx = state
                .context
                .clone()
                .ok_or(AgentError::Protocol("output not initialized"))?;
            (ctx, effects)
        };
        effects.emit(&ctx).await?;
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
                pending.texts.push(frame);
                return Ok(());
            }
            FrameData::TtsAudio(audio) | FrameData::OutputAudio(audio) => {
                let generation = {
                    let state = self
                        .state
                        .lock()
                        .map_err(|_| ProcessorError::fatal("output state poisoned"))?;
                    if !state
                        .pending
                        .iter()
                        .any(|p| Some(p.id.as_str()) == audio.context_id.as_deref())
                    {
                        return Ok(());
                    }
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
}
