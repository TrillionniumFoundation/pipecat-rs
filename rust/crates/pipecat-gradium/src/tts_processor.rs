//! Nonblocking TTS with bounded, ordered, independently cancellable contexts.
use crate::{
    tts_settings as settings, CancellationToken, GradiumError, GradiumTtsClient, GradiumTtsConfig,
    GradiumTtsEvent, GradiumTtsStream,
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
    client: GradiumTtsClient,
    active: Option<Turn>,
    // A response can outlive its active context (for example a flows fence or
    // TtsSpeak closes that context before the original LLM end arrives).
    response_failed: Arc<AtomicBool>,
    turns: Vec<CancellationToken>,
    fences: Vec<CancellationToken>,
    tail: Option<CancellationToken>,
    health_revision: u64,
    timeline: Arc<AtomicU64>,
}
/// Context settings are immutable. Updates flush pending sentences and rotate
/// contexts; response ends wait for every preceding context, including failures.
pub struct GradiumTtsProcessor {
    state: Mutex<State>,
    aggregation_mode: AggregationType,
    audio_watchdog: TtsAudioWatchdog,
}
impl GradiumTtsProcessor {
    pub fn new(client: GradiumTtsClient) -> Self {
        Self {
            state: Mutex::new(State {
                client,
                active: None,
                response_failed: Arc::default(),
                turns: vec![],
                fences: vec![],
                tail: None,
                health_revision: 0,
                timeline: Arc::default(),
            }),
            aggregation_mode: AggregationType::Sentence,
            audio_watchdog: TtsAudioWatchdog::default(),
        }
    }
    /// Sentence mode matches the source default. Token mode also preserves
    /// incremental synthesis. Both protect complete <spell> tags.
    pub fn with_text_aggregation_mode(mut self, mode: AggregationType) -> Self {
        self.aggregation_mode = mode;
        self
    }
    /// Zero disables only the permanent cutoff; each silent context is reported.
    pub fn with_max_consecutive_zero_audio_contexts(mut self, maximum: usize) -> Self {
        self.audio_watchdog = TtsAudioWatchdog::new(maximum);
        self
    }
    pub fn config_snapshot(&self) -> Result<GradiumTtsConfig, GradiumError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| GradiumError::Closed)?
            .client
            .config()
            .clone())
    }
    fn validate_frame(&self, frame: &Frame) -> Result<(), ProcessorError> {
        let max = {
            let state = self
                .state
                .lock()
                .map_err(|_| processor_error(GradiumError::Closed))?;
            state
                .client
                .config()
                .max_text_bytes
                .min(state.client.config().limits.max_message_bytes)
        };
        let text = |value: &str| {
            if value.len() <= max {
                Ok(())
            } else {
                Err(processor_error(GradiumError::TooLarge))
            }
        };
        if frame
            .transport_destination
            .as_ref()
            .is_some_and(|v| v.len() > 1024)
        {
            return Err(processor_error(GradiumError::TooLarge));
        }
        match &frame.data {
            FrameData::Text(v) | FrameData::LlmText(v) => text(&v.text)?,
            FrameData::TtsSpeak(v) => text(&v.text)?,
            FrameData::AggregatedText(v) => {
                text(&v.text.text)?;
                if let Some(raw) = &v.raw_text {
                    text(raw)?;
                }
                if v.aggregated_by.len() > 128 {
                    return Err(processor_error(GradiumError::TooLarge));
                }
            }
            FrameData::TtsTextInput(v) => {
                text(&v.synthesis_text)?;
                text(&v.display_text)?;
                text(&v.original_text)?;
                if v.aggregated_by.len() > 128 {
                    return Err(processor_error(GradiumError::TooLarge));
                }
            }
            FrameData::TtsUpdateSettings(v) if settings::targets(self, &v.service) => {
                settings::validate_values(&v.settings).map_err(settings_error)?;
                if let Some(d) = &v.delta {
                    settings::validate_values(&d.base.extra).map_err(settings_error)?;
                    for field in [&d.base.model, &d.voice, &d.language] {
                        settings::validate_setting(field).map_err(settings_error)?;
                    }
                }
            }
            FrameData::ServiceUpdateSettings(v) if settings::targets(self, &v.service) => {
                settings::validate_values(&v.settings).map_err(settings_error)?;
                if let Some(d) = &v.delta {
                    settings::validate_values(&d.extra).map_err(settings_error)?;
                    settings::validate_setting(&d.model).map_err(settings_error)?;
                }
            }
            _ => (),
        }
        Ok(())
    }
    fn reserve_turn(state: &mut State) -> Result<(), ProcessorError> {
        state.turns.retain(|token| !token.is_cancelled());
        if state.turns.len() >= state.client.config().limits.queue_capacity {
            return Err(processor_error(GradiumError::Backpressure));
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
        let predecessor = state.tail.replace(completed.clone());
        // Capture guards before spawning so even an unpolled aborted task
        // releases its successor and counts as no longer live.
        let completion_guard = CancelOnDrop(completed);
        let cancel_guard = CancelOnDrop(worker_cancel.clone());
        state.turns.push(cancel.clone());
        let worker_failed = response_failed.clone();
        let mode = self.aggregation_mode;
        let generation = ctx.health().generation();
        let audio_watchdog = self.audio_watchdog.clone();
        let timeline = state.timeline.clone();
        ctx.spawn("gradium-turn", true, async move {
            let (_completion_guard, _cancel_guard) = (completion_guard, cancel_guard);
            tokio::select! {
                biased;
                _ = worker_cancel.cancelled() => Ok(()),
                result = async {
                    if let Some(predecessor) = predecessor { predecessor.cancelled().await; }
                    run_turn(client, context_id, None, start, rx, finish_rx, worker_failed, mode, generation, timeline, audio_watchdog, output).await
                } => result,
            }
        })?;
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
            return Err(processor_error(GradiumError::TooLarge));
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
            return Err(processor_error(GradiumError::TooLarge));
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
                GradiumError::Backpressure
            } else {
                GradiumError::Closed
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
        let candidate = settings::tts(&state.client, delta).map_err(settings_error)?;
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
        let mut state = self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("TTS state lock poisoned"))?;
        state.fences.retain(|done| !done.is_cancelled());
        if state.fences.len() >= state.client.config().limits.queue_capacity {
            return Err(processor_error(GradiumError::Backpressure));
        }
        Self::finish_locked(&mut state, None);
        let done = CancellationToken::new();
        let predecessor = state.tail.replace(done.clone());
        state.fences.push(done.clone());
        let completion_guard = CancelOnDrop(done);
        let output = ctx.clone();
        // Interruption cancels synthesis, but its completion guard still
        // releases the fence. This acknowledges synthesis, never playout.
        ctx.spawn("gradium-fence", false, async move {
            let _completion_guard = completion_guard;
            if let Some(predecessor) = predecessor {
                predecessor.cancelled().await;
            }
            output.push(frame, Direction::Downstream).await
        })?;
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
        state.tail = None;
        state.timeline = Arc::default();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for GradiumTtsProcessor {
    fn name(&self) -> &str {
        "GradiumTTSService"
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
                "pipeline output rate differs from GradiumTts configuration",
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
        self.validate_frame(&frame)?;
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
                if state.tail.as_ref().is_some_and(|tail| !tail.is_cancelled()) {
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
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.sync_health(ctx)?;
        self.finish_turn(None)?;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cancel_turns()?;
        let client = self
            .state
            .lock()
            .map_err(|_| processor_error(GradiumError::Closed))?
            .client
            .clone();
        client.shutdown().await;
        Ok(())
    }
}
struct Synthesis {
    client: GradiumTtsClient,
    context_id: String,
    stream: Option<GradiumTtsStream>,
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
            if self.had_context {
                self.stopped().await?;
            }
        }
        Ok(())
    }

    async fn fail(&mut self, error: GradiumError) -> Result<(), ProcessorError> {
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
            if self.had_context {
                self.stopped().await?;
            }
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
        // Empty/tool-only responses allocate no provider-side state.
        if self.stream.is_none() {
            match self.client.stream(self.context_id.clone()) {
                Ok(stream) => self.stream = Some(stream),
                Err(error) => {
                    self.fail(error).await?;
                    return Ok(());
                }
            }
        }
        if !self.had_context {
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
            let reporting = TokenReporting::WordTimestamps;
            let alignment = TtsAlignment::new(
                self.revision,
                self.append_to_context,
                reporting,
                None,
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
                    self.fail(GradiumError::Protocol(
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
                self.fail(GradiumError::Protocol(
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
                        if self.event(event).await? { self.fail(GradiumError::Protocol("GradiumTts completed before finalization")).await?; return Ok(false); }
                    },
                    Some(Err(error)) => { self.fail(error).await?; return Ok(false); },
                    None => { self.fail(GradiumError::Protocol("GradiumTts stream closed without done")).await?; return Ok(false); },
                },
            }
        }
    }
    async fn event(&mut self, event: GradiumTtsEvent) -> Result<bool, ProcessorError> {
        if !self.usable() {
            self.retire().await?;
            return Ok(false);
        }
        if !self.had_context {
            self.fail(GradiumError::Protocol("TTS output before admitted text"))
                .await?;
            return Ok(false);
        }
        match event {
            GradiumTtsEvent::Audio(audio) => {
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
            GradiumTtsEvent::WordTimestamps { words, .. } => {
                let spaces = false;
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
            GradiumTtsEvent::Done { .. } => {
                if !self.finalizing {
                    self.fail(GradiumError::Protocol(
                        "GradiumTts completed before finalization",
                    ))
                    .await?;
                    return Ok(false);
                }
                if self.first_audio {
                    // The watchdog reports the empty synthesis below. Do not
                    // first publish successful alignment/stopped diagnostics.
                    self.retire().await?;
                    return Ok(true);
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
    client: GradiumTtsClient,
    context_id: String,
    stream: Option<GradiumTtsStream>,
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
    let mut aggregator =
        SkipTagsAggregator::new(vec![("<spell>".into(), "</spell>".into())], mode, None);
    let mut total_text = 0usize;
    let mut synthesis = Synthesis {
        client,
        context_id,
        stream,
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
                None => { synthesis.fail(GradiumError::Protocol("GradiumTts stream closed without done")).await?; if finished { break; } },
            },
            command = commands.recv(), if !finished => match command {
                Some(command) => {
                    if synthesis.failed { continue; }
                    if synthesis.source_id == 0 { synthesis.source_id = command.source_id; synthesis.append_to_context = command.append_to_context; synthesis.destination = command.destination.clone(); }
                    total_text = total_text.saturating_add(command.text.synthesis.len()).saturating_add(command.text.display.len()).saturating_add(command.text.original.len());
                    if total_text > synthesis.client.config().max_text_bytes.saturating_mul(3) { synthesis.fail(GradiumError::TooLarge).await?; aggregator.reset(); continue; }
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
                    if synthesis.stream.is_some() && synthesis.had_context {
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
            .is_some_and(GradiumTtsStream::source_audio_observed);
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
pub(crate) fn error_frame(error: GradiumError) -> ErrorFrame {
    let category = match &error {
        GradiumError::Http(status) => ErrorCategory::from_http_status(*status),
        GradiumError::Invalid(_) => ErrorCategory::InvalidRequest,
        GradiumError::Transport | GradiumError::Timeout(_) | GradiumError::Closed => {
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
pub(crate) fn processor_error(error: GradiumError) -> ProcessorError {
    let error = error_frame(error);
    ProcessorError::new(error.error).with_category(error.category)
}
// A rejected proposal leaves the validated active configuration intact.
// Retain its validation category without changing the previous health state.
fn settings_error(error: GradiumError) -> ProcessorError {
    processor_error(error).with_permanence_override(false)
}
