//! Nonblocking TTS with bounded, ordered, independently cancellable contexts.
pub use crate::stt_processor::DeepgramSttProcessor;
use crate::{
    settings, CancellationToken, CartesiaClient, CartesiaConfig, CartesiaEvent, CartesiaStream,
    StreamingError,
};
use pipecat_core::{
    async_trait, Direction, ErrorCategory, ErrorFrame, Frame, FrameData, FrameProcessor, Metric,
    ProcessorContext, ProcessorError, ProcessorHealthGeneration, StartFrame, TtsSettings,
    TtsStartedFrame, TtsStoppedFrame,
};
use pipecat_services::TtsAudioWatchdog;
use pipecat_text::{
    AggregationType, SkipTagsAggregator, TextAggregator, TokenReporting, TtsAlignment,
    TtsAlignmentError, TtsAlignmentEvent, TtsAlignmentFrames, TtsAlignmentLimits, TtsTextChannels,
};
use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::{mpsc, oneshot};

static NEXT_CONTEXT: AtomicU64 = AtomicU64::new(1);
struct TextCommand {
    text: TtsTextChannels,
    skipped: bool,
    destination: Option<String>,
    append_to_context: bool,
    source_id: u64,
    aggregate: bool,
    aggregated_by: String,
    includes_inter_frame_spaces: bool,
}
struct Turn {
    tx: mpsc::Sender<TextCommand>,
    finish: oneshot::Sender<Option<Frame>>,
    cancel: CancellationToken,
    response_failed: Arc<AtomicBool>,
    append_to_context: Option<bool>,
    destination: Option<Option<String>>,
}
struct CancelOnDrop(CancellationToken);
impl Drop for CancelOnDrop {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
struct State {
    client: CartesiaClient,
    active: Option<Turn>,
    // A response can outlive its active context (for example a flows fence or
    // TtsSpeak closes that context before the original LLM end arrives).
    response_failed: Arc<AtomicBool>,
    turns: Vec<CancellationToken>,
    fences: Vec<CancellationToken>,
    // Completion dependencies remain until the owned task actually drops. A
    // cancelled middle task must not release a successor past a live survivor.
    completions: Vec<CancellationToken>,
    health_revision: u64,
    timeline: Arc<AtomicU64>,
}
/// Context settings are immutable. Updates flush pending sentences and rotate
/// contexts; response ends wait for every preceding context, including failures.
pub struct CartesiaTtsProcessor {
    state: Mutex<State>,
    aggregation_mode: AggregationType,
    audio_watchdog: TtsAudioWatchdog,
}
impl CartesiaTtsProcessor {
    pub fn new(client: CartesiaClient) -> Self {
        Self {
            state: Mutex::new(State {
                client,
                active: None,
                response_failed: Arc::default(),
                turns: vec![],
                fences: vec![],
                completions: vec![],
                health_revision: 0,
                timeline: Arc::default(),
            }),
            aggregation_mode: AggregationType::Token,
            audio_watchdog: TtsAudioWatchdog::default(),
        }
    }
    /// Sentence mode matches the source service's default; token mode preserves
    /// this native adapter's incremental API. Both protect complete <spell> tags.
    pub fn with_text_aggregation_mode(mut self, mode: AggregationType) -> Self {
        self.aggregation_mode = mode;
        self
    }
    /// Zero disables only the permanent cutoff; each silent context is reported.
    pub fn with_max_consecutive_zero_audio_contexts(mut self, maximum: usize) -> Self {
        self.audio_watchdog = TtsAudioWatchdog::new(maximum);
        self
    }
    pub fn config_snapshot(&self) -> Result<CartesiaConfig, StreamingError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| StreamingError::Closed)?
            .client
            .config()
            .clone())
    }
    fn reserve_turn(state: &mut State) -> Result<(), ProcessorError> {
        state.turns.retain(|token| !token.is_cancelled());
        if state.turns.len() >= state.client.config().limits.queue_capacity {
            return Err(processor_error(StreamingError::Backpressure));
        }
        Ok(())
    }
    fn start_turn(
        &self,
        state: &mut State,
        start: Option<Frame>,
        response_failed: Arc<AtomicBool>,
        ctx: &ProcessorContext,
    ) -> Result<Turn, ProcessorError> {
        Self::reserve_turn(state)?;
        let (tx, rx) = mpsc::channel(state.client.config().limits.queue_capacity);
        // Finalization cannot be blocked by a saturated text queue.
        let (finish, finish_rx) = oneshot::channel();
        let cancel = CancellationToken::new();
        let worker_cancel = cancel.clone();
        let client = state.client.clone();
        let output = ctx.clone();
        let serial = NEXT_CONTEXT
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| ProcessorError::fatal("TTS context identity exhausted"))?;
        let context_id = format!("pipecat-tts-{serial}");
        let completed = CancellationToken::new();
        state.completions.retain(|done| !done.is_cancelled());
        let predecessors = state.completions.clone();
        // Capture guards before spawning so even an unpolled aborted task
        // releases its successor and counts as no longer live.
        let completion_guard = CancelOnDrop(completed.clone());
        let cancel_guard = CancelOnDrop(worker_cancel.clone());
        let worker_failed = response_failed.clone();
        let mode = self.aggregation_mode;
        let generation = ctx.health().generation();
        let audio_watchdog = self.audio_watchdog.clone();
        let timeline = state.timeline.clone();
        ctx.spawn("cartesia-turn", true, async move {
            let (_completion_guard, _cancel_guard) = (completion_guard, cancel_guard);
            tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => Ok(()),
                result = async {
                    for predecessor in predecessors { predecessor.cancelled().await; }
                    run_turn(client, context_id, start, rx, finish_rx, worker_failed, mode, generation, timeline, audio_watchdog, output).await
                } => result,
            }
        })?;
        if !completed.is_cancelled() {
            state.turns.push(cancel.clone());
            state.completions.push(completed);
        }
        Ok(Turn {
            tx,
            finish,
            cancel,
            response_failed,
            append_to_context: None,
            destination: None,
        })
    }
    fn finish_locked(state: &mut State, frame: Option<Frame>) -> Option<Arc<AtomicBool>> {
        let turn = state.active.take()?;
        if turn.cancel.is_cancelled() {
            return None;
        }
        if turn.finish.send(frame).is_err() {
            return None;
        }
        Some(turn.response_failed)
        // Dropping the last producer lets the worker drain all admitted text
        // before observing the separate finish barrier.
    }
    fn enqueue_text(
        &self,
        command: TextCommand,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.is_usable() {
            self.state
                .lock()
                .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?
                .response_failed
                .store(true, Ordering::Release);
            return Ok(());
        }
        if command.text.synthesis.is_empty()
            && command.text.display.is_empty()
            && command.text.original.is_empty()
        {
            return Ok(());
        }
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        if command
            .destination
            .as_ref()
            .is_some_and(|destination| destination.len() > 1024)
            || command.aggregated_by.len() > 128
        {
            return Err(processor_error(StreamingError::TooLarge));
        }
        if [
            &command.text.synthesis,
            &command.text.display,
            &command.text.original,
        ]
        .iter()
        .any(|text| {
            text.len() > state.client.config().max_text_bytes
                || text.len() > state.client.config().limits.max_message_bytes
        }) {
            return Err(processor_error(StreamingError::TooLarge));
        }
        if state
            .active
            .as_ref()
            .is_some_and(|turn| turn.cancel.is_cancelled())
        {
            state.active = None;
        }
        if state.active.as_ref().is_some_and(|turn| {
            turn.append_to_context
                .is_some_and(|append| append != command.append_to_context)
                || turn
                    .destination
                    .as_ref()
                    .is_some_and(|destination| destination != &command.destination)
        }) {
            Self::reserve_turn(&mut state)?;
            let failed = Self::finish_locked(&mut state, None).unwrap_or_default();
            state.active = Some(self.start_turn(&mut state, None, failed, ctx)?);
        }
        if state.active.is_none() {
            let failed = state.response_failed.clone();
            state.active = Some(self.start_turn(&mut state, None, failed, ctx)?);
        }
        let turn = state.active.as_mut().unwrap();
        turn.append_to_context = Some(command.append_to_context);
        turn.destination = Some(command.destination.clone());
        turn.tx.try_send(command).map_err(|error| {
            // Rejected text remains explicitly rejected; accepted text and its
            // response barrier continue draining without cancelling the turn.
            processor_error(if matches!(error, mpsc::error::TrySendError::Full(_)) {
                StreamingError::Backpressure
            } else {
                StreamingError::Closed
            })
        })
    }
    fn update_settings(
        &self,
        delta: &TtsSettings,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        let candidate = settings::cartesia(&state.client, delta).map_err(settings_error)?;
        if candidate.config() == state.client.config() {
            return Ok(());
        }
        let failed = if state.active.is_some() {
            Self::reserve_turn(&mut state)?;
            Self::finish_locked(&mut state, None)
        } else {
            None
        };
        state.client = candidate;
        ctx.set_usable(true);
        state.health_revision = ctx.health().snapshot().revision;
        // Even an empty successor carries the LLM response barrier after a
        // settings change that follows the final text token.
        if let Some(failed) = failed {
            state.active = Some(self.start_turn(&mut state, None, failed, ctx)?);
        }
        Ok(())
    }
    fn finish_turn(&self, frame: Option<Frame>) -> Result<bool, ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        Ok(Self::finish_locked(&mut state, frame).is_some())
    }
    fn fence(&self, frame: Frame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.enqueue_frame(frame, true, ctx)
    }
    /// Queue normal frames behind owned synthesis without finalizing the active
    /// provider context. Subsequent text may still enter that same context.
    fn enqueue_frame(
        &self,
        frame: Frame,
        finish_context: bool,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        state.fences.retain(|done| !done.is_cancelled());
        if state.fences.len() >= state.client.config().limits.queue_capacity {
            state.response_failed.store(true, Ordering::Release);
            return Err(processor_error(StreamingError::Backpressure));
        }
        if finish_context {
            Self::finish_locked(&mut state, None);
        }
        state.completions.retain(|done| !done.is_cancelled());
        let predecessors = state.completions.clone();
        let done = CancellationToken::new();
        let completion_guard = CancelOnDrop(done.clone());
        let output = ctx.clone();
        let failed = state.response_failed.clone();
        // The existing flows fence survives interruption. An ordinary frame
        // instead retains its own interruption policy and opaque envelope.
        let interruptible = !finish_context && frame.interruptible;
        let admitted = ctx.spawn("cartesia-fence", interruptible, async move {
            let _completion_guard = completion_guard;
            for predecessor in predecessors {
                predecessor.cancelled().await;
            }
            let result = output.push(frame, Direction::Downstream).await;
            if result.is_err() {
                // This cannot rewrite a response end already emitted by the
                // preceding context; the runtime still records the push error.
                failed.store(true, Ordering::Release);
            }
            result
        });
        if let Err(error) = admitted {
            state.response_failed.store(true, Ordering::Release);
            return Err(error);
        }
        if !done.is_cancelled() {
            state.fences.push(done.clone());
            state.completions.push(done);
        }
        Ok(())
    }
    // Every health revision retires the previous generation, even when a
    // disable/restore pair coalesces before the next input frame arrives.
    fn sync_health(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let snapshot = ctx.health().snapshot();
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        if state.health_revision != snapshot.revision {
            state.response_failed.store(true, Ordering::Release);
            Self::finish_locked(&mut state, None);
            state.health_revision = snapshot.revision;
            state.timeline = Arc::default();
        }
        Ok(())
    }
    fn cancel_turns(&self) -> Result<(), ProcessorError> {
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        for token in state.turns.drain(..) {
            token.cancel();
        }
        state.active = None;
        state.response_failed = Arc::default();
        state.completions.retain(|done| !done.is_cancelled());
        state.timeline = Arc::default();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for CartesiaTtsProcessor {
    fn name(&self) -> &str {
        "CartesiaTtsService"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if config.audio_out_sample_rate
            != self.config_snapshot().map_err(processor_error)?.sample_rate
        {
            return Err(ProcessorError::new(
                "pipeline output rate differs from Cartesia configuration",
            )
            .with_category(ErrorCategory::InvalidRequest));
        }
        Ok(())
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.sync_health(ctx)?;
        if matches!(
            &frame.data,
            FrameData::Interruption | FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.cancel_turns()?;
        }
        match &frame.data {
            FrameData::TtsUpdateSettings(update) if settings::targets(self, &update.service) => {
                let delta = match &update.delta {
                    Some(delta) => delta.clone(),
                    None => settings::tts_mapping(&update.settings).map_err(settings_error)?,
                };
                self.update_settings(&delta, ctx)?;
                return Ok(());
            }
            FrameData::ServiceUpdateSettings(update)
                if settings::targets(self, &update.service) =>
            {
                let delta = match &update.delta {
                    Some(delta) => TtsSettings {
                        base: delta.clone(),
                        ..Default::default()
                    },
                    None => settings::tts_mapping(&update.settings).map_err(settings_error)?,
                };
                self.update_settings(&delta, ctx)?;
                return Ok(());
            }
            _ => {}
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        match &frame.data {
            FrameData::FlushDetailed(flush)
                if !flush.returning && flush.origin.as_deref() == Some("pipecat.flows") =>
            {
                self.fence(frame, ctx)?;
                return Ok(());
            }
            FrameData::LlmResponseStart => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
                Self::reserve_turn(&mut state)?;
                Self::finish_locked(&mut state, None);
                state.response_failed = Arc::new(AtomicBool::new(!ctx.is_usable()));
                let failed = state.response_failed.clone();
                state.active = Some(self.start_turn(&mut state, Some(frame), failed, ctx)?);
                return Ok(());
            }
            FrameData::LlmText(text) | FrameData::Text(text) if text.skip_tts != Some(true) => {
                self.enqueue_text(
                    TextCommand {
                        text: TtsTextChannels::identical(text.text.clone()),
                        skipped: text.skip_tts == Some(true),
                        destination: frame.transport_destination.clone(),
                        append_to_context: text.append_to_context,
                        source_id: frame.id,
                        aggregate: true,
                        aggregated_by: "sentence".into(),
                        includes_inter_frame_spaces: false,
                    },
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::AggregatedText(text) => {
                self.enqueue_text(
                    TextCommand {
                        text: TtsTextChannels {
                            synthesis: text.text.text.clone(),
                            display: text.text.text.clone(),
                            original: text
                                .raw_text
                                .clone()
                                .unwrap_or_else(|| text.text.text.clone()),
                        },
                        skipped: text.text.skip_tts == Some(true),
                        destination: frame.transport_destination.clone(),
                        append_to_context: text.text.append_to_context,
                        source_id: frame.id,
                        aggregate: false,
                        aggregated_by: text.aggregated_by.clone(),
                        includes_inter_frame_spaces: text.text.includes_inter_frame_spaces,
                    },
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::TtsTextInput(text) => {
                self.enqueue_text(
                    TextCommand {
                        text: TtsTextChannels {
                            synthesis: text.synthesis_text.clone(),
                            display: text.display_text.clone(),
                            original: text.original_text.clone(),
                        },
                        skipped: text.skip_tts,
                        destination: frame.transport_destination.clone(),
                        append_to_context: text.append_to_context,
                        source_id: frame.id,
                        aggregate: false,
                        aggregated_by: text.aggregated_by.clone(),
                        includes_inter_frame_spaces: text.includes_inter_frame_spaces,
                    },
                    ctx,
                )?;
                return Ok(());
            }
            FrameData::TtsSpeak(speak) => {
                self.finish_turn(None)?;
                self.enqueue_text(
                    TextCommand {
                        text: TtsTextChannels::identical(speak.text.clone()),
                        skipped: false,
                        destination: frame.transport_destination.clone(),
                        append_to_context: speak.append_to_context,
                        source_id: frame.id,
                        aggregate: false,
                        aggregated_by: "sentence".into(),
                        includes_inter_frame_spaces: false,
                    },
                    ctx,
                )?;
                self.finish_turn(None)?;
                return Ok(());
            }
            FrameData::LlmResponseEnd => {
                let mut state = self
                    .state
                    .lock()
                    .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
                if Self::finish_locked(&mut state, Some(frame.clone())).is_some() {
                    state.response_failed = Arc::default();
                    return Ok(());
                }
                // A fence or standalone speak may have closed the context.
                // Retain the response's failure flag across its empty barrier.
                if state.completions.iter().any(|done| !done.is_cancelled()) {
                    let failed = state.response_failed.clone();
                    state.active = Some(self.start_turn(&mut state, None, failed, ctx)?);
                    Self::finish_locked(&mut state, Some(frame));
                    state.response_failed = Arc::default();
                    return Ok(());
                }
                if state.response_failed.load(Ordering::Acquire) {
                    frame.metadata.insert("failed".into(), true.into());
                }
                state.response_failed = Arc::default();
            }
            _ => {}
        }
        let skip_tts = matches!(
            &frame.data,
            FrameData::Text(text) | FrameData::LlmText(text) if text.skip_tts == Some(true)
        );
        if !frame.is_system() && !frame.is_terminal() && !skip_tts {
            return self.enqueue_frame(frame, false, ctx);
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.sync_health(ctx)?;
        self.finish_turn(None)?;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_turns()
    }
}
struct Synthesis {
    client: CartesiaClient,
    context_id: String,
    stream: Option<CartesiaStream>,
    ctx: ProcessorContext,
    response_failed: Arc<AtomicBool>,
    failed: bool,
    revision: u64,
    append_to_context: bool,
    source_id: u64,
    characters: u64,
    first_audio: bool,
    had_context: bool,
    source_audio: bool,
    finalizing: bool,
    started: tokio::time::Instant,
    destination: Option<String>,
    alignment: Option<TtsAlignment>,
    alignment_frames: Option<TtsAlignmentFrames>,
    timeline: Arc<AtomicU64>,
}
impl Synthesis {
    fn usable(&self) -> bool {
        let health = self.ctx.health().snapshot();
        self.ctx.is_current() && health.is_usable && health.revision == self.revision
    }
    async fn retire(&mut self) -> Result<(), ProcessorError> {
        self.failed = true;
        if let Some(alignment) = self.alignment.as_mut() {
            alignment.cancel();
        }
        self.response_failed.store(true, Ordering::Release);
        if let Some(stream) = self.stream.take() {
            self.source_audio |= stream.source_audio_observed();
            self.stopped().await?;
        }
        Ok(())
    }

    async fn fail(&mut self, error: StreamingError) -> Result<(), ProcessorError> {
        if !self.usable() {
            return self.retire().await;
        }
        self.failed = true;
        if let Some(alignment) = self.alignment.as_mut() {
            alignment.cancel();
        }
        self.response_failed.store(true, Ordering::Release);
        self.ctx.push_error(error_frame(error)).await?;
        if let Some(stream) = self.stream.take() {
            self.source_audio |= stream.source_audio_observed();
            self.stopped().await?;
        }
        Ok(())
    }
    async fn stopped(&self) -> Result<(), ProcessorError> {
        let mut frame = Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
            context_id: Some(self.context_id.clone()),
        }));
        frame.transport_destination = self.destination.clone();
        if self.failed || !self.usable() {
            frame.metadata.insert("failed".into(), true.into());
        }
        if !self.failed {
            if let Some(alignment) = &self.alignment {
                frame.set_pipeline_pts(
                    alignment
                        .boundary_pts()
                        .map_err(|_| ProcessorError::new("TTS timestamp boundary overflow"))?,
                );
                self.timeline
                    .fetch_max(frame.pts.unwrap(), Ordering::AcqRel);
            }
        }
        self.ctx.push(frame, Direction::Downstream).await
    }
    async fn text(
        &mut self,
        text: TtsTextChannels,
        complete: bool,
        aggregated_by: &str,
        includes_inter_frame_spaces: bool,
    ) -> Result<(), ProcessorError> {
        if !self.usable() {
            self.retire().await?;
        }
        if self.failed || text.synthesis.is_empty() {
            return Ok(());
        }
        if self.stream.is_none() {
            self.stream = Some(
                self.client
                    .stream(self.context_id.clone())
                    .map_err(processor_error)?,
            );
            self.had_context = true;
            self.started = tokio::time::Instant::now();
            let mut started = Frame::new(FrameData::TtsStarted(TtsStartedFrame {
                context_id: Some(self.context_id.clone()),
                append_to_context: self.append_to_context,
            }));
            started.transport_destination = self.destination.clone();
            self.ctx.push(started, Direction::Downstream).await?;
        }
        if !self.ensure_alignment().await? {
            return Ok(());
        }
        let events = self.alignment.as_mut().unwrap().register_prepared(
            &text,
            complete,
            aggregated_by,
            includes_inter_frame_spaces,
        );
        if !self.aligned(events).await? {
            return Ok(());
        }
        let characters = text.synthesis.chars().count() as u64;
        if self.submit(Some(text.synthesis)).await? {
            self.characters = self.characters.saturating_add(characters);
        }
        Ok(())
    }
    async fn ensure_alignment(&mut self) -> Result<bool, ProcessorError> {
        if self.alignment.is_none() {
            let limits = TtsAlignmentLimits::default();
            let reporting = if self.client.config().add_timestamps {
                TokenReporting::WordTimestamps
            } else {
                TokenReporting::SynthesisCompletion
            };
            let alignment = TtsAlignment::new(
                self.revision,
                self.append_to_context,
                reporting,
                self.client.config().language.as_deref(),
                limits,
            );
            let mapper = alignment
                .as_ref()
                .map_err(|error| error.clone())
                .and_then(|alignment| {
                    TtsAlignmentFrames::new(
                        self.context_id.clone(),
                        alignment.context(),
                        self.append_to_context,
                        limits.sequencer.sequencer.max_slots,
                    )
                });
            match (alignment, mapper) {
                (Ok(alignment), Ok(mapper)) => {
                    self.alignment = Some(alignment);
                    self.alignment_frames = Some(mapper);
                }
                _ => {
                    self.fail(StreamingError::Protocol(
                        "TTS alignment initialization failed",
                    ))
                    .await?;
                    return Ok(false);
                }
            }
        }
        Ok(!self.failed)
    }
    async fn aligned(
        &mut self,
        events: Result<Vec<TtsAlignmentEvent>, TtsAlignmentError>,
    ) -> Result<bool, ProcessorError> {
        let frames =
            events.and_then(|events| self.alignment_frames.as_mut().unwrap().convert(events));
        let frames = match frames {
            Ok(frames) => frames,
            Err(_) => {
                self.fail(StreamingError::Protocol(
                    "TTS alignment or timestamp limit violated",
                ))
                .await?;
                return Ok(false);
            }
        };
        for mut frame in frames {
            if frame.transport_destination.is_none() {
                frame.transport_destination = self.destination.clone();
            }
            frame
                .metadata
                .insert("source_frame_id".into(), self.source_id.into());
            if !self.usable() {
                self.retire().await?;
                return Ok(false);
            }
            if let Some(pts) = frame.pts {
                self.timeline.fetch_max(pts, Ordering::AcqRel);
            }
            self.ctx.push(frame, Direction::Downstream).await?;
        }
        Ok(true)
    }
    /// Drain output while awaiting bounded producer admission. Waiting on a
    /// full input queue alone can deadlock a provider with a full output queue.
    async fn submit(&mut self, text: Option<String>) -> Result<bool, ProcessorError> {
        let mut health = self.ctx.health().watch();
        if !self.usable() {
            self.retire().await?;
            return Ok(false);
        }
        if text.is_none() {
            self.finalizing = true;
        }
        let sender = self.stream.as_ref().unwrap().sender();
        let send = async {
            match text {
                Some(text) => sender.send_text(text).await,
                None => sender.finish().await,
            }
        };
        tokio::pin!(send);
        loop {
            // A provider event may retire the stream after a concurrent health
            // transition. Its sender must not keep the retired context alive.
            if self.failed || self.stream.is_none() {
                return Ok(false);
            }
            tokio::select! {
                biased;
                _ = health.changed() => { self.retire().await?; return Ok(false); },
                result = &mut send => match result {
                    Ok(()) => return Ok(true),
                    Err(error) => { self.fail(error).await?; return Ok(false); },
                },
                event = self.stream.as_mut().unwrap().next() => match event {
                    Some(Ok(event)) => {
                        if self.event(event).await? { self.fail(StreamingError::Protocol("Cartesia completed before finalization")).await?; return Ok(false); }
                    },
                    Some(Err(error)) => { self.fail(error).await?; return Ok(false); },
                    None => { self.fail(StreamingError::Protocol("Cartesia stream closed without done")).await?; return Ok(false); },
                },
            }
        }
    }
    async fn event(&mut self, event: CartesiaEvent) -> Result<bool, ProcessorError> {
        if !self.usable() {
            self.retire().await?;
            return Ok(false);
        }
        match event {
            CartesiaEvent::Audio(audio) => {
                if self.first_audio && self.ctx.configuration().enable_metrics {
                    self.ctx
                        .push(
                            Frame::new(FrameData::Metrics(vec![Metric::Ttfb {
                                processor: self.ctx.name().into(),
                                seconds: self.started.elapsed().as_secs_f64(),
                            }])),
                            Direction::Downstream,
                        )
                        .await?;
                }
                if !self.usable() {
                    self.retire().await?;
                    return Ok(false);
                }
                if self.first_audio {
                    let events = self.alignment.as_mut().unwrap().start_audio(
                        self.ctx.observer_clock().now_ns(),
                        self.timeline.load(Ordering::Acquire),
                    );
                    if !self.aligned(events).await? {
                        return Ok(false);
                    }
                }
                self.first_audio = false;
                let mut frame = Frame::new(FrameData::TtsAudio(audio));
                frame.transport_destination = self.destination.clone();
                self.ctx.push(frame, Direction::Downstream).await?;
            }
            CartesiaEvent::WordTimestamps { words, .. } => {
                let spaces = self
                    .client
                    .config()
                    .language
                    .as_deref()
                    .is_some_and(|language| {
                        language.starts_with("zh") || language.starts_with("ja")
                    });
                for word in words {
                    if !self.usable() {
                        self.retire().await?;
                        return Ok(false);
                    }
                    let events = self
                        .alignment
                        .as_mut()
                        .unwrap()
                        .word(&word.word, word.start, spaces);
                    if !self.aligned(events).await? {
                        return Ok(false);
                    }
                }
            }
            CartesiaEvent::FlushDone { .. } => {}
            CartesiaEvent::Done { .. } => {
                if !self.finalizing {
                    self.fail(StreamingError::Protocol(
                        "Cartesia completed before finalization",
                    ))
                    .await?;
                    return Ok(false);
                }
                let events = self.alignment.as_mut().unwrap().finish_success(
                    self.ctx.observer_clock().now_ns(),
                    self.timeline.load(Ordering::Acquire),
                );
                if !self.aligned(events).await? {
                    return Ok(true);
                }
                self.stopped().await?;
                if self.ctx.configuration().enable_usage_metrics {
                    self.ctx
                        .push(
                            Frame::new(FrameData::Metrics(vec![Metric::TtsUsage {
                                processor: self.ctx.name().into(),
                                characters: self.characters,
                            }])),
                            Direction::Downstream,
                        )
                        .await?;
                }
                return Ok(true);
            }
        }
        Ok(false)
    }
}
#[allow(clippy::too_many_arguments)]
async fn run_turn(
    client: CartesiaClient,
    context_id: String,
    start: Option<Frame>,
    mut commands: mpsc::Receiver<TextCommand>,
    finish: oneshot::Receiver<Option<Frame>>,
    response_failed: Arc<AtomicBool>,
    mode: AggregationType,
    generation: ProcessorHealthGeneration,
    timeline: Arc<AtomicU64>,
    audio_watchdog: TtsAudioWatchdog,
    ctx: ProcessorContext,
) -> Result<(), ProcessorError> {
    let revision = generation.snapshot.revision;
    if let Some(start) = start {
        ctx.push(start, Direction::Downstream).await?;
    }
    let mut health = ctx.health().watch();
    let mut aggregator = SkipTagsAggregator::new(
        vec![("<spell>".into(), "</spell>".into())],
        mode,
        client.config().language.as_deref(),
    );
    let mut total_text = 0usize;
    let mut synthesis = Synthesis {
        client,
        context_id,
        stream: None,
        ctx,
        response_failed,
        failed: false,
        revision,
        append_to_context: true,
        source_id: 0,
        characters: 0,
        first_audio: true,
        had_context: false,
        source_audio: false,
        finalizing: false,
        started: tokio::time::Instant::now(),
        destination: None,
        alignment: None,
        alignment_frames: None,
        timeline,
    };
    let mut finish = Some(finish);
    let mut response_end = None;
    let mut finished = false;
    loop {
        if !synthesis.usable() && !synthesis.failed {
            synthesis.retire().await?;
            aggregator.reset();
        }
        if finished && synthesis.failed {
            break;
        }
        let event = async {
            match synthesis.stream.as_mut() {
                Some(stream) => stream.next().await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            _ = health.changed() => continue,
            event = event => match event {
                Some(Ok(event)) => { if synthesis.event(event).await? { break; } },
                Some(Err(error)) => { synthesis.fail(error).await?; if finished { break; } },
                None => { synthesis.fail(StreamingError::Protocol("Cartesia stream closed without done")).await?; if finished { break; } },
            },
            command = commands.recv(), if !finished => match command {
                Some(command) => {
                    if synthesis.failed { continue; }
                    if synthesis.source_id == 0 { synthesis.source_id = command.source_id; synthesis.append_to_context = command.append_to_context; synthesis.destination = command.destination.clone(); }
                    total_text = total_text.saturating_add(command.text.synthesis.len()).saturating_add(command.text.display.len()).saturating_add(command.text.original.len());
                    if total_text > synthesis.client.config().max_text_bytes.saturating_mul(3) { synthesis.fail(StreamingError::TooLarge).await?; aggregator.reset(); continue; }
                    if command.skipped {
                        if let Some(text) = aggregator.flush() { synthesis.text(TtsTextChannels::identical(text.text), true, "sentence", false).await?; }
                        if synthesis.ensure_alignment().await? {
                            let events = synthesis.alignment.as_mut().unwrap().skipped_prepared(&command.text.display, &command.text.original, command.destination.as_deref(), &command.aggregated_by, command.includes_inter_frame_spaces);
                            synthesis.aligned(events).await?;
                        }
                    } else if command.aggregate {
                        for text in aggregator.aggregate(&command.text.synthesis) { synthesis.text(TtsTextChannels::identical(text.text), mode != AggregationType::Token, "sentence", false).await?; }
                    } else {
                        if let Some(text) = aggregator.flush() { synthesis.text(TtsTextChannels::identical(text.text), true, "sentence", false).await?; }
                        synthesis.text(command.text, true, &command.aggregated_by, command.includes_inter_frame_spaces).await?;
                    }
                },
                None => {
                    response_end = finish.take().unwrap().await.unwrap_or(None);
                    finished = true;
                    if !synthesis.failed {
                        if let Some(text) = aggregator.flush() { synthesis.text(TtsTextChannels::identical(text.text), true, "sentence", false).await?; }
                        if let Some(alignment) = synthesis.alignment.as_mut() {
                            let events = alignment.finalize(); synthesis.aligned(events).await?;
                        }
                    }
                    if synthesis.stream.is_some() {
                        if !synthesis.submit(None).await? { break; }
                    } else {
                        if !synthesis.failed {
                            if let Some(alignment) = synthesis.alignment.as_mut() {
                                let events = alignment.finish_success(synthesis.ctx.observer_clock().now_ns(), synthesis.timeline.load(Ordering::Acquire));
                                synthesis.aligned(events).await?;
                            }
                        }
                        break;
                    }
                },
            },
        }
    }
    let source_audio = synthesis.source_audio
        || synthesis
            .stream
            .as_ref()
            .is_some_and(CartesiaStream::source_audio_observed);
    if synthesis.had_context
        && audio_watchdog
            .complete(
                &synthesis.ctx,
                generation,
                &synthesis.context_id,
                !synthesis.first_audio || source_audio,
            )
            .await?
    {
        synthesis.response_failed.store(true, Ordering::Release);
    }
    if let Some(mut end) = response_end {
        if !synthesis.usable() || synthesis.response_failed.load(Ordering::Acquire) {
            end.metadata.insert("failed".into(), true.into());
        }
        if let Some(alignment) = &synthesis.alignment {
            if let Ok(pts) = alignment.boundary_pts() {
                end.set_pipeline_pts(pts);
            }
        }
        synthesis.ctx.push(end, Direction::Downstream).await?;
    }
    Ok(())
}
pub(crate) fn error_frame(error: StreamingError) -> ErrorFrame {
    let category = match &error {
        StreamingError::Http(status) => ErrorCategory::from_http_status(*status),
        StreamingError::Provider {
            status: Some(status),
        } => ErrorCategory::from_http_status(*status),
        StreamingError::Invalid(_) => ErrorCategory::InvalidRequest,
        StreamingError::Transport | StreamingError::Timeout(_) | StreamingError::Closed => {
            ErrorCategory::Connectivity
        }
        _ => ErrorCategory::Application,
    };
    ErrorFrame {
        error: error.to_string(),
        category,
        ..ErrorFrame::new("")
    }
}
pub(crate) fn processor_error(error: StreamingError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error).with_category(error.category)
}
pub(crate) fn timestamp() -> String {
    iso_timestamp(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default(),
    )
}
fn iso_timestamp(time: std::time::Duration) -> String {
    let days = (time.as_secs() / 86400) as i64;
    // Gregorian civil date from days since the Unix epoch.
    let z = days + 719468;
    let era = z / 146097;
    let day_of_era = z - era * 146097;
    let year_of_era =
        (day_of_era - day_of_era / 1460 + day_of_era / 36524 - day_of_era / 146096) / 365;
    let mut year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_position = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_position + 2) / 5 + 1;
    let month = month_position + if month_position < 10 { 3 } else { -9 };
    year += i64::from(month <= 2);
    let seconds = time.as_secs() % 86400;
    format!(
        "{year:04}-{month:02}-{day:02}T{:02}:{:02}:{:02}.{:06}+00:00",
        seconds / 3600,
        (seconds / 60) % 60,
        seconds % 60,
        time.subsec_micros()
    )
}
// A rejected proposal leaves the validated active configuration intact.
// Retain its validation category without changing the previous health state.
fn settings_error(error: StreamingError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn transcription_timestamp_is_iso8601_utc() {
        assert_eq!(
            iso_timestamp(std::time::Duration::ZERO),
            "1970-01-01T00:00:00.000000+00:00"
        );
        assert_eq!(
            iso_timestamp(std::time::Duration::from_micros(951827696123456)),
            "2000-02-29T12:34:56.123456+00:00"
        );
    }
}

#[cfg(test)]
mod ordering_survivor_tests {
    use super::*;
    use pipecat_core::{
        DirectedFrame, Pipeline, PipelineConfig, PipelineError, PipelineWorker, WorkerState,
    };
    use std::time::Duration;

    const DEADLINE: Duration = Duration::from_secs(3);
    const HELD_WINDOW: Duration = Duration::from_millis(50);

    // Candidate-only internal controls, not public-API baseline regressions.
    // An ordinary push is nonblocking, so install one real owned task and its
    // matching completion/fence records to hold A. All subsequent turn/fence
    // admission, interruption, terminal draining and task reaping are real.
    struct HeldPredecessor {
        tts: Arc<CartesiaTtsProcessor>,
        held_id: u64,
        forward_held: bool,
        release: Mutex<Option<oneshot::Receiver<()>>>,
        observed: mpsc::Sender<Admission>,
    }

    #[derive(Debug)]
    struct Admission {
        id: u64,
        result: Result<(), ProcessorError>,
        fences: usize,
        completions: usize,
        active: bool,
        response_failed: bool,
    }

    impl HeldPredecessor {
        fn hold(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            assert!(!frame.interruptible);
            let release = self.release.lock().unwrap().take().unwrap();
            let done = CancellationToken::new();
            let guard = CancelOnDrop(done.clone());
            let output = ctx.clone();
            let forward_held = self.forward_held;
            ctx.spawn("held-noninterruptible-predecessor", false, async move {
                let _guard = guard;
                release.await.unwrap();
                if forward_held {
                    output.push(frame, direction).await
                } else {
                    Ok(())
                }
            })?;
            let mut state = self.tts.state.lock().unwrap();
            state.fences.push(done.clone());
            state.completions.push(done);
            Ok(())
        }
    }

    #[async_trait]
    impl FrameProcessor for HeldPredecessor {
        fn name(&self) -> &str {
            self.tts.name()
        }
        async fn setup(
            &self,
            config: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.tts.setup(config, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let id = frame.id;
            let result = if id == self.held_id {
                self.hold(frame, direction, ctx)
            } else {
                self.tts.process(frame, direction, ctx).await
            };
            let state = self.tts.state.lock().unwrap();
            self.observed
                .try_send(Admission {
                    id,
                    result: result.clone(),
                    fences: state.fences.len(),
                    completions: state.completions.len(),
                    active: state.active.is_some(),
                    response_failed: state.response_failed.load(Ordering::Acquire),
                })
                .unwrap();
            result
        }
        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.tts.prepare_end(ctx).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.tts.cleanup().await
        }
    }

    struct Fixture {
        worker: PipelineWorker,
        observed: mpsc::Receiver<Admission>,
        release: oneshot::Sender<()>,
        tts: Arc<CartesiaTtsProcessor>,
        first: Frame,
    }

    async fn fixture(queue_capacity: usize, max_background_tasks: usize) -> Fixture {
        fixture_with_output(queue_capacity, max_background_tasks, 1024, true).await
    }

    async fn fixture_with_output(
        queue_capacity: usize,
        max_background_tasks: usize,
        output_capacity: usize,
        forward_held: bool,
    ) -> Fixture {
        let mut config = CartesiaConfig::new("synthetic-key", "voice");
        // These tests admit only empty turns and opaque Data frames. No provider
        // connection or synthesis request is needed or expected.
        config.endpoint = "ws://127.0.0.1:1/tts".into();
        config.limits.queue_capacity = queue_capacity;
        let tts = Arc::new(CartesiaTtsProcessor::new(
            CartesiaClient::new(config).unwrap(),
        ));
        let first = marker(false);
        let (release, receiver) = oneshot::channel();
        let (observed, receiver_observed) = mpsc::channel(32);
        let processor = HeldPredecessor {
            tts: tts.clone(),
            held_id: first.id,
            forward_held,
            release: Mutex::new(Some(receiver)),
            observed,
        };
        let worker = Pipeline::new(vec![Arc::new(processor)])
            .start(PipelineConfig {
                max_background_tasks,
                output_capacity,
                ..Default::default()
            })
            .await
            .unwrap();
        Fixture {
            worker,
            observed: receiver_observed,
            release,
            tts,
            first,
        }
    }

    fn marker(interruptible: bool) -> Frame {
        Frame::new(FrameData::Data).with_interruptible(interruptible)
    }

    async fn attempt(
        worker: &PipelineWorker,
        observed: &mut mpsc::Receiver<Admission>,
        frame: Frame,
    ) -> Admission {
        let id = frame.id;
        worker
            .sender()
            .send(frame, Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(DEADLINE, async {
            loop {
                let admission = observed.recv().await.unwrap();
                if admission.id == id {
                    return admission;
                }
            }
        })
        .await
        .expect("processor did not report the frame attempt")
    }

    async fn admit(
        worker: &PipelineWorker,
        observed: &mut mpsc::Receiver<Admission>,
        frame: Frame,
    ) -> Admission {
        let admission = attempt(worker, observed, frame).await;
        assert!(admission.result.is_ok(), "{:?}", admission.result);
        admission
    }

    fn completion_tokens(tts: &CartesiaTtsProcessor) -> Vec<CancellationToken> {
        tts.state.lock().unwrap().completions.clone()
    }

    async fn collect_held_window(worker: &mut PipelineWorker) -> Vec<DirectedFrame> {
        let mut frames = vec![];
        let result = tokio::time::timeout(HELD_WINDOW, async {
            while let Some(frame) = worker.recv().await {
                frames.push(frame);
            }
        })
        .await;
        assert!(result.is_err(), "pipeline terminated while A was held");
        frames
    }

    fn assert_no_synthesis(frames: &[DirectedFrame]) {
        assert!(!frames.iter().any(|f| matches!(
            f.frame.data,
            FrameData::TtsStarted(_) | FrameData::TtsAudio(_) | FrameData::TtsStopped(_)
        )));
    }

    async fn interrupted_middle_preserves_survivor(middle_is_turn: bool) {
        let Fixture {
            mut worker,
            mut observed,
            release,
            tts,
            first,
        } = fixture(8, 8).await;
        let middle = if middle_is_turn {
            Frame::new(FrameData::LlmResponseStart)
        } else {
            marker(true)
        };
        let last = marker(false);
        admit(&worker, &mut observed, first.clone()).await;
        admit(&worker, &mut observed, middle.clone()).await;
        let admission = admit(&worker, &mut observed, last.clone()).await;
        assert_eq!(
            admission.active, middle_is_turn,
            "normal enqueue must leave an active provider context open"
        );
        assert_eq!(admission.completions, 3);
        let completions = completion_tokens(&tts);
        admit(&worker, &mut observed, Frame::interruption()).await;
        // Wait for B's actual task drop, not merely interruption admission.
        tokio::time::timeout(DEADLINE, completions[1].cancelled())
            .await
            .unwrap();
        assert!(!completions[0].is_cancelled());
        worker.end().await.unwrap();
        let mut frames = collect_held_window(&mut worker).await;
        let overtook = frames.iter().any(|f| f.frame.id == last.id);
        let premature_end = frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::End(_)));
        release.send(()).unwrap();
        let (rest, report) = tokio::time::timeout(DEADLINE, worker.collect())
            .await
            .unwrap()
            .unwrap();
        frames.extend(rest);
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            !overtook,
            "cancelled B released C while uninterruptible A was held"
        );
        assert!(
            !premature_end,
            "End escaped before surviving owned work drained"
        );
        let ids: Vec<_> = frames
            .iter()
            .filter_map(|f| {
                [first.id, middle.id, last.id]
                    .contains(&f.frame.id)
                    .then_some(f.frame.id)
            })
            .collect();
        assert_eq!(ids, [first.id, last.id]);
        let last_index = frames.iter().position(|f| f.frame.id == last.id).unwrap();
        let end_index = frames
            .iter()
            .position(|f| matches!(f.frame.data, FrameData::End(_)))
            .unwrap();
        assert!(last_index < end_index);
        assert!(completions.iter().all(CancellationToken::is_cancelled));
        assert_no_synthesis(&frames);
    }

    #[tokio::test]
    async fn interrupted_middle_turn_cannot_release_fence_past_held_survivor() {
        interrupted_middle_preserves_survivor(true).await;
    }

    #[tokio::test]
    async fn interrupted_middle_normal_frame_cannot_release_fence_past_held_survivor() {
        interrupted_middle_preserves_survivor(false).await;
    }

    #[tokio::test]
    async fn cancel_reaps_all_held_surviving_dependencies_without_release() {
        let Fixture {
            worker,
            mut observed,
            release,
            tts,
            first,
        } = fixture(8, 8).await;
        let middle = Frame::new(FrameData::LlmResponseStart);
        let last = marker(false);
        admit(&worker, &mut observed, first.clone()).await;
        admit(&worker, &mut observed, middle.clone()).await;
        let admission = admit(&worker, &mut observed, last.clone()).await;
        assert!(
            admission.active,
            "normal enqueue finalized the open context"
        );
        assert_eq!(admission.completions, 3);
        let completions = completion_tokens(&tts);
        worker.cancel("cancel all dependency owners").await.unwrap();
        let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Cancelled);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert!(
            release.send(()).is_err(),
            "owned held predecessor was not reaped"
        );
        assert!(completions.iter().all(CancellationToken::is_cancelled));
        assert!(!frames
            .iter()
            .any(|f| [first.id, middle.id, last.id].contains(&f.frame.id)));
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::End(_))));
        assert_no_synthesis(&frames);
    }

    #[tokio::test]
    async fn exact_fence_queue_cap_rejects_only_overflow_and_preserves_failure_barrier() {
        let Fixture {
            worker,
            mut observed,
            release,
            tts,
            first,
        } = fixture(2, 8).await;
        let second = marker(false);
        let rejected = marker(false);
        let end = Frame::new(FrameData::LlmResponseEnd);
        let first_admission = admit(&worker, &mut observed, first.clone()).await;
        assert_eq!(
            (first_admission.fences, first_admission.completions),
            (1, 1)
        );
        let second_admission = admit(&worker, &mut observed, second.clone()).await;
        assert_eq!(
            (second_admission.fences, second_admission.completions),
            (2, 2)
        );
        let overflow = attempt(&worker, &mut observed, rejected.clone()).await;
        assert_eq!(
            overflow.result,
            Err(processor_error(StreamingError::Backpressure))
        );
        assert_eq!((overflow.fences, overflow.completions), (2, 2));
        assert!(overflow.response_failed);
        assert!(!overflow.active);
        // The failure barrier is a turn, so its separate admission cap permits
        // it at the exact fence limit, while the core's total task cap still applies.
        let barrier = admit(&worker, &mut observed, end.clone()).await;
        assert_eq!((barrier.fences, barrier.completions), (2, 3));
        assert!(!barrier.active);
        let completions = completion_tokens(&tts);
        release.send(()).unwrap();
        worker.end().await.unwrap();
        let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(report.total_errors, 1);
        assert_eq!(report.errors.len(), 1, "{:?}", report.errors);
        let error = &report.errors[0];
        assert_eq!(error.error, "streaming speech queue is full");
        assert_eq!(error.category, ErrorCategory::Application);
        assert!(!error.fatal);
        assert_eq!(error.processor.as_deref(), Some("CartesiaTtsService"));
        let ids: Vec<_> = frames
            .iter()
            .filter_map(|f| {
                [first.id, second.id, rejected.id, end.id]
                    .contains(&f.frame.id)
                    .then_some(f.frame.id)
            })
            .collect();
        assert_eq!(ids, [first.id, second.id, end.id]);
        let response_end = frames.iter().find(|f| f.frame.id == end.id).unwrap();
        assert_eq!(
            response_end.frame.metadata.get("failed"),
            Some(&serde_json::json!(true))
        );
        assert!(completions.iter().all(CancellationToken::is_cancelled));
        assert_no_synthesis(&frames);
    }

    #[tokio::test]
    async fn exact_core_task_budget_fails_explicitly_and_reaps_without_release() {
        let Fixture {
            mut worker,
            mut observed,
            release,
            tts,
            first,
        } = fixture(8, 2).await;
        let second = marker(false);
        let rejected = marker(false);
        admit(&worker, &mut observed, first.clone()).await;
        let second_admission = admit(&worker, &mut observed, second.clone()).await;
        assert_eq!(
            (second_admission.fences, second_admission.completions),
            (2, 2)
        );
        let completions = completion_tokens(&tts);
        let overflow = attempt(&worker, &mut observed, rejected.clone()).await;
        assert_eq!(
            overflow.result,
            Err(ProcessorError::fatal("background task limit reached"))
        );
        assert_eq!((overflow.fences, overflow.completions), (2, 2));
        assert!(overflow.response_failed);
        let frames = tokio::time::timeout(DEADLINE, async {
            let mut frames = vec![];
            while let Some(frame) = worker.recv().await {
                frames.push(frame);
            }
            frames
        })
        .await
        .unwrap();
        let error = tokio::time::timeout(DEADLINE, worker.join())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error,
            PipelineError::Execution("background task limit reached".into())
        );
        assert_eq!(worker.state(), WorkerState::Failed);
        let errors = worker.errors();
        assert_eq!(worker.total_errors(), 1);
        assert_eq!(errors.len(), 1, "{errors:?}");
        assert_eq!(errors[0].error, "background task limit reached");
        assert_eq!(errors[0].category, ErrorCategory::Unknown);
        assert!(errors[0].fatal);
        assert_eq!(errors[0].processor.as_deref(), Some("CartesiaTtsService"));
        assert_eq!(frames.iter().filter(|f| matches!(&f.frame.data,
            FrameData::Error(error) if error.fatal && error.error == "background task limit reached"
        ) && f.direction == Direction::Upstream).count(), 1);
        assert!(
            release.send(()).is_err(),
            "fatal admission did not reap held A"
        );
        assert!(completions.iter().all(CancellationToken::is_cancelled));
        assert!(!frames
            .iter()
            .any(|f| [first.id, second.id, rejected.id].contains(&f.frame.id)));
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::End(_))));
        assert_no_synthesis(&frames);
    }

    #[tokio::test]
    async fn full_output_fence_push_fails_explicitly_without_successful_end() {
        // Here A only holds a completion dependency. Its release must not do
        // the failing push: that push belongs to the real queued fence B.
        let Fixture {
            mut worker,
            mut observed,
            release,
            tts,
            first,
        } = fixture_with_output(8, 8, 1, false).await;
        let start = tokio::time::timeout(DEADLINE, worker.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(start.frame.data, FrameData::Start(_)));
        let marker = marker(false);
        let bypass = Frame::new(FrameData::System);
        admit(&worker, &mut observed, first.clone()).await;
        let admission = admit(&worker, &mut observed, marker.clone()).await;
        assert_eq!((admission.fences, admission.completions), (2, 2));
        let completions = completion_tokens(&tts);
        let failed = tts.state.lock().unwrap().response_failed.clone();
        // System frames bypass the ordinary FIFO and fill the sole output slot.
        // Do not consume it until the real queued push has failed.
        admit(&worker, &mut observed, bypass.clone()).await;
        worker.end().await.unwrap();
        release.send(()).unwrap();
        let error = tokio::time::timeout(DEADLINE, worker.join())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(
            error,
            PipelineError::Backpressure(
                "worker output queue is full; consume output concurrently".into()
            )
        );
        assert_eq!(worker.state(), WorkerState::Failed);
        assert!(
            failed.load(Ordering::Acquire),
            "fence push failure was not recorded"
        );
        assert!(completions.iter().all(CancellationToken::is_cancelled));
        let mut frames = vec![start];
        while let Some(frame) = tokio::time::timeout(DEADLINE, worker.recv()).await.unwrap() {
            frames.push(frame);
        }
        assert_eq!(frames.iter().filter(|f| f.frame.id == bypass.id).count(), 1);
        assert!(!frames
            .iter()
            .any(|f| [first.id, marker.id].contains(&f.frame.id)));
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::End(_))));
        assert_no_synthesis(&frames);
        // The authoritative fatal result is join's Backpressure error. A full
        // output lane cannot promise delivery of an additional ErrorFrame, nor
        // can this failure rewrite a response end already emitted upstream.
    }
}
