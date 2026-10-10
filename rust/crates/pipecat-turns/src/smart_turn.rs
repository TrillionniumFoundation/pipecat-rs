//! Native Smart Turn audio analysis integrated with STT-gated user aggregation.
use crate::{
    AggregationEvent, AggregationOutput, LLMUserAggregator, SharedContext, UserAggregatorConfig,
};
use pipecat_core::{
    async_trait, BaseTurnParams, Direction, ErrorFrame, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, SpeechControlParamsFrame, StartFrame,
};
use pipecat_vad::{
    EndOfTurnState, SmartTurnAnalysis, SmartTurnAnalyzer, SmartTurnCancellation, VadError,
};
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::sync::{Mutex, Notify};

struct State {
    analyzer: Option<SmartTurnAnalyzer>,
    vad_speaking: bool,
    generation: u64,
    analyze_requested: bool,
    cancellation: Option<SmartTurnCancellation>,
    task_id: Option<u64>,
    closing: bool,
}
impl State {
    fn invalidate(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.analyze_requested = false;
        if let Some(cancel) = self.cancellation.take() {
            cancel.cancel();
        }
    }
    fn clear(&mut self) {
        self.invalidate();
        if let Some(analyzer) = self.analyzer.as_mut() {
            analyzer.clear();
        }
    }
    fn observe(&mut self, output: &AggregationOutput) {
        if output
            .events
            .iter()
            .any(|event| matches!(event, AggregationEvent::UserMuteChanged { muted: true }))
        {
            self.vad_speaking = false;
            self.clear();
        }
        if output
            .events
            .iter()
            .any(|event| matches!(event, AggregationEvent::UserTurnStopped { .. }))
        {
            self.clear();
        }
    }
}

type AnalysisFuture = Pin<Box<dyn Future<Output = pipecat_vad::Result<SmartTurnAnalysis>> + Send>>;

/// A model-backed user aggregator with one managed async supervisor and one
/// bounded native inference worker. Put it after STT and before the LLM.
///
/// VAD frames label the input audio. A model COMPLETE verdict opens the turn
/// gate; final transcription or the STT P99 deadline releases the context. The
/// deadline is anchored to physical speech stop, so model latency never starts
/// a fresh STT wait. Transcript-only input retains the no-VAD fallback.
///
/// Speech resume invalidates pending results without discarding the utterance
/// buffer. External stop, mute, End, and Cancel clear it. The normal controller
/// watchdog remains available when the model reports INCOMPLETE.
///
/// End cancels incomplete model work and flushes accepted final transcripts;
/// Cancel never starts an LLM response. Cleanup joins the owned native worker.
/// Never place the wrapped `LLMUserAggregator` separately in the same pipeline.
pub struct SmartTurnUserAggregator {
    inner: Arc<LLMUserAggregator>,
    state: Arc<Mutex<State>>,
    changed: Arc<Notify>,
    sample_rate: u32,
    params: pipecat_vad::SmartTurnParams,
}
impl SmartTurnUserAggregator {
    pub fn new(
        context: SharedContext,
        mut config: UserAggregatorConfig,
        analyzer: SmartTurnAnalyzer,
    ) -> Self {
        config.turn.require_model_completion = true;
        config.turn.speech_timeout = Duration::ZERO;
        Self::wrap(Arc::new(LLMUserAggregator::new(context, config)), analyzer)
    }
    /// Wrap an existing aggregator, retaining its shared context and assistant
    /// pairing. This must happen before the aggregator starts a conversation.
    pub fn with_aggregator(
        inner: Arc<LLMUserAggregator>,
        analyzer: SmartTurnAnalyzer,
    ) -> Result<Self, ProcessorError> {
        inner.configure_model_gate()?;
        Ok(Self::wrap(inner, analyzer))
    }
    fn wrap(inner: Arc<LLMUserAggregator>, analyzer: SmartTurnAnalyzer) -> Self {
        let sample_rate = analyzer.buffer().sample_rate();
        let params = analyzer.buffer().params().clone();
        Self {
            inner,
            state: Arc::new(Mutex::new(State {
                analyzer: Some(analyzer),
                vad_speaking: false,
                generation: 0,
                analyze_requested: false,
                cancellation: None,
                task_id: None,
                closing: false,
            })),
            changed: Arc::new(Notify::new()),
            sample_rate,
            params,
        }
    }
    pub fn context(&self) -> SharedContext {
        self.inner.context()
    }
    pub fn user(&self) -> Arc<LLMUserAggregator> {
        self.inner.clone()
    }
    pub async fn buffered_samples(&self) -> usize {
        self.state
            .lock()
            .await
            .analyzer
            .as_ref()
            .map_or(0, |analyzer| analyzer.buffer().buffered_samples())
    }
    async fn supervise(
        shared: Arc<Mutex<State>>,
        changed: Arc<Notify>,
        inner: Arc<LLMUserAggregator>,
        ctx: ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut pending: Option<(u64, AnalysisFuture)> = None;
        loop {
            let deadline = {
                let mut state = shared.lock().await;
                if state.closing {
                    return Ok(());
                }
                // A canceled predictor is allowed to finish cooperatively. Only
                // the latest requested snapshot is started once it has exited.
                if pending.is_none() && state.analyze_requested {
                    state.analyze_requested = false;
                    let analysis = state
                        .analyzer
                        .as_mut()
                        .ok_or_else(|| ProcessorError::fatal("Smart Turn analyzer is closed"))?
                        .begin_analysis();
                    match analysis {
                        Ok(analysis) => {
                            state.cancellation = Some(analysis.cancellation());
                            pending = Some((state.generation, Box::pin(analysis.wait())));
                        }
                        Err(error) => {
                            ctx.push_error(ErrorFrame::new(format!("Smart Turn: {error}")))
                                .await?;
                        }
                    }
                }
                inner.deadline()?
            };
            enum Wake {
                Changed,
                Deadline,
                Analysis(u64, pipecat_vad::Result<SmartTurnAnalysis>),
            }
            let wake = tokio::select! {
                _ = changed.notified() => Wake::Changed,
                _ = async {
                    match deadline {
                        Some(deadline) => tokio::time::sleep(deadline.saturating_sub(inner.now())).await,
                        None => std::future::pending::<()>().await,
                    }
                } => Wake::Deadline,
                result = async {
                    match pending.as_mut() {
                        Some((generation, future)) => (*generation, future.await),
                        None => std::future::pending().await,
                    }
                } => Wake::Analysis(result.0, result.1),
            };
            let mut state = shared.lock().await;
            if state.closing {
                return Ok(());
            }
            match wake {
                Wake::Changed => {}
                Wake::Deadline => {
                    let output = inner.advance_turn_at(inner.now())?;
                    state.observe(&output);
                    inner.notify_inference_requests(&output, &ctx).await?;
                    output.emit(&ctx).await?;
                    inner.schedule_idle_timer(&ctx)?;
                }
                Wake::Analysis(generation, result) => {
                    pending.take();
                    if state.generation != generation || state.vad_speaking {
                        continue;
                    }
                    state.cancellation = None;
                    match result {
                        Ok(analysis) => {
                            if !state
                                .analyzer
                                .as_mut()
                                .is_some_and(|analyzer| analyzer.apply_analysis(&analysis))
                            {
                                continue;
                            }
                            if let Some(metrics) = analysis.metrics {
                                let mut frame =
                                    Frame::new(FrameData::Metrics(vec![Metric::Processing {
                                        processor: metrics.processor.into(),
                                        seconds: metrics.e2e_processing_time_ms / 1000.0,
                                    }]));
                                frame.metadata.insert(
                                    "smart_turn".into(),
                                    serde_json::json!({
                                        "is_complete": metrics.is_complete,
                                        "probability": metrics.probability,
                                        "e2e_processing_time_ms": metrics.e2e_processing_time_ms,
                                    }),
                                );
                                ctx.push(frame, Direction::Downstream).await?;
                            }
                            if analysis.state == EndOfTurnState::Complete {
                                let output = inner.mark_model_complete_at(inner.now())?;
                                state.observe(&output);
                                inner.notify_inference_requests(&output, &ctx).await?;
                                output.emit(&ctx).await?;
                                inner.schedule_idle_timer(&ctx)?;
                            }
                        }
                        Err(VadError::Cancelled) => {}
                        Err(error) => {
                            ctx.push_error(ErrorFrame::new(format!("Smart Turn: {error}")))
                                .await?;
                        }
                    }
                }
            }
        }
    }
}
fn model_error(error: VadError) -> ProcessorError {
    ProcessorError::new(format!("Smart Turn: {error}"))
}

#[async_trait]
impl FrameProcessor for SmartTurnUserAggregator {
    fn name(&self) -> &str {
        "SmartTurnUserAggregator"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if config.audio_in_sample_rate != self.sample_rate {
            return Err(ProcessorError::fatal(
                "Smart Turn analyzer sample rate must match pipeline input rate",
            ));
        }
        self.inner.configure_model_gate()?;
        let mut state = self.state.lock().await;
        if state.closing || state.task_id.is_some() {
            return Err(ProcessorError::fatal(
                "Smart Turn aggregator cannot be started twice",
            ));
        }
        state.task_id = Some(ctx.spawn(
            "smart-turn-supervisor",
            false,
            Self::supervise(
                self.state.clone(),
                self.changed.clone(),
                self.inner.clone(),
                ctx.clone(),
            ),
        )?);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        crate::user::validate_control(&frame.data)?;
        let mut state = self.state.lock().await;
        let terminal = matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
        );
        let started = frame.data.is_start();
        let muted = self.inner.is_muted()?;
        let mut audio_complete = false;
        if terminal {
            state.closing = true;
            state.clear();
        } else if !state.closing && !muted {
            match &frame.data {
                FrameData::VadUserStartedSpeaking(vad) => {
                    state
                        .analyzer
                        .as_mut()
                        .expect("open analyzer")
                        .update_vad_start_secs(vad.duration_secs)
                        .map_err(model_error)?;
                    state.invalidate();
                    state.vad_speaking = true;
                }
                FrameData::VadUserStartedSpeakingDetailed(vad) => {
                    state
                        .analyzer
                        .as_mut()
                        .expect("open analyzer")
                        .update_vad_start_secs(vad.start_secs)
                        .map_err(model_error)?;
                    state.invalidate();
                    state.vad_speaking = true;
                }
                FrameData::VadUserStoppedSpeaking(_)
                | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                    state.invalidate();
                    state.vad_speaking = false;
                    state.analyze_requested = true;
                }
                FrameData::UserStartedSpeaking => state.invalidate(),
                FrameData::UserStoppedSpeaking if !self.inner.has_explicit_stop_strategies()? => {
                    state.clear()
                }
                FrameData::UserMuteStarted | FrameData::SttMute(true) => {
                    state.vad_speaking = false;
                    state.clear();
                }
                FrameData::InputAudio(audio)
                | FrameData::UserAudio(pipecat_core::UserAudioRawFrame { audio, .. })
                    if direction == Direction::Downstream =>
                {
                    if audio.sample_rate != self.sample_rate || audio.num_channels != 1 {
                        return Err(ProcessorError::new(
                            "Smart Turn requires mono PCM at its configured sample rate",
                        ));
                    }
                    let speaking = state.vad_speaking;
                    audio_complete = state
                        .analyzer
                        .as_mut()
                        .expect("open analyzer")
                        .append_audio(&audio.audio, speaking)
                        .map_err(model_error)?
                        == EndOfTurnState::Complete;
                    if audio_complete {
                        state.invalidate();
                    }
                }
                _ => {}
            }
        }
        let output = self.inner.process_at(frame, direction, self.inner.now())?;
        state.observe(&output);
        self.inner.notify_inference_requests(&output, ctx).await?;
        output.emit(ctx).await?;
        if audio_complete {
            let output = self.inner.mark_model_complete_at(self.inner.now())?;
            state.observe(&output);
            self.inner.notify_inference_requests(&output, ctx).await?;
            output.emit(ctx).await?;
        }
        if started {
            let mut control =
                Frame::new(FrameData::SpeechControlParams(SpeechControlParamsFrame {
                    vad_params: None,
                    turn_params: Some(BaseTurnParams::default()),
                }));
            control.metadata.insert(
                "smart_turn_params".into(),
                serde_json::json!({
                    "stop_secs": self.params.stop_secs,
                    "pre_speech_ms": self.params.pre_speech_ms,
                    "max_duration_secs": self.params.max_duration_secs,
                }),
            );
            ctx.broadcast(control).await?;
        }
        self.changed.notify_one();
        self.inner.schedule_idle_timer(ctx)?;
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        state.closing = true;
        state.clear();
        if let Some(id) = state.task_id.take() {
            ctx.cancel_task(id);
        }
        self.changed.notify_one();
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let analyzer = {
            let mut state = self.state.lock().await;
            state.closing = true;
            state.clear();
            state.analyzer.take()
        };
        if let Some(mut analyzer) = analyzer {
            analyzer.cleanup().await.map_err(model_error)?;
        }
        self.inner.cleanup().await
    }
}
