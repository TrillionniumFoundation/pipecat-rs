use crate::{
    AggregationAction, AggregationEvent, AggregationOutput, RecentFrames, SharedContext,
    TextBuffer, TurnStopReason, UserTurnConfig, UserTurnController, UserTurnEvent,
};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
};
use pipecat_services::{Message, Role};
use std::collections::VecDeque;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct UserAggregatorConfig {
    pub turn: UserTurnConfig,
    pub initially_muted: bool,
    /// Silence after the bot finishes responding; zero disables idle events.
    pub user_idle_timeout: Duration,
    /// Policies are evaluated in parallel (OR) on each non-lifecycle frame.
    pub mute_strategies: Vec<crate::UserMuteStrategy>,
    /// By default, recover one empty interrupted turn; idle turns stay silent.
    /// Set to None to disable recovery entirely.
    pub empty_user_turn: Option<crate::EmptyUserTurnConfig>,
}
impl Default for UserAggregatorConfig {
    fn default() -> Self {
        Self {
            turn: UserTurnConfig::default(),
            initially_muted: false,
            user_idle_timeout: Duration::ZERO,
            mute_strategies: vec![],
            empty_user_turn: Some(crate::EmptyUserTurnConfig::default()),
        }
    }
}
struct UserState {
    controller: UserTurnController,
    idle: Arc<crate::idle::UserIdleTracker>,
    now: Duration,
    paired_assistant: Option<std::sync::Weak<crate::LLMAssistantAggregator>>,
    aggregation: TextBuffer,
    held_external_stop: Option<pipecat_core::DirectedFrame>,
    interim: String,
    muted: bool,
    manual_muted: bool,
    policy_muted: bool,
    mute_strategies: crate::UserMuteStrategies,
    empty_user_turn: Option<crate::EmptyUserTurnConfig>,
    interrupted_bot: bool,
    consecutive_empty_recoveries: usize,
    seen: RecentFrames,
    transcript_ids: VecDeque<String>,
    transcript_frames: VecDeque<Frame>,
    ended: bool,
    timer_disabled: bool,
    timer_id: Option<u64>,
    timer_running: bool,
}
/// Final transcripts are appended once; interim hypotheses replace only the
/// preview and are never committed as final user speech.
pub struct LLMUserAggregator {
    context: SharedContext,
    state: Arc<Mutex<UserState>>,
    changed: Arc<Notify>,
    origin: Instant,
    idle: Arc<crate::idle::UserIdleTracker>,
    inference_handlers: Arc<Mutex<Vec<InferenceHandler>>>,
}
type InferenceHandler =
    Arc<dyn Fn(crate::UserTurnInferenceRequest) -> Result<(), ProcessorError> + Send + Sync>;
impl LLMUserAggregator {
    pub fn new(context: SharedContext, config: UserAggregatorConfig) -> Self {
        let origin = Instant::now();
        let idle = Arc::new(crate::idle::UserIdleTracker::new(
            config.user_idle_timeout,
            origin,
        ));
        Self {
            context,
            state: Arc::new(Mutex::new(UserState {
                controller: UserTurnController::new(config.turn),
                idle: idle.clone(),
                now: Duration::ZERO,
                paired_assistant: None,
                aggregation: TextBuffer::default(),
                held_external_stop: None,
                interim: String::new(),
                muted: config.initially_muted,
                manual_muted: config.initially_muted,
                policy_muted: false,
                mute_strategies: crate::UserMuteStrategies::new(config.mute_strategies),
                empty_user_turn: config.empty_user_turn,
                interrupted_bot: false,
                consecutive_empty_recoveries: 0,
                seen: RecentFrames::default(),
                transcript_ids: VecDeque::new(),
                transcript_frames: VecDeque::new(),
                ended: false,
                timer_disabled: false,
                timer_id: None,
                timer_running: false,
            })),
            changed: Arc::new(Notify::new()),
            origin,
            idle,
            inference_handlers: Arc::new(Mutex::new(vec![])),
        }
    }
    /// Observe external-verdict requests in the runtime. Handlers execute
    /// outside conversation locks and must only enqueue/notify, never block on
    /// inference. Return a correlated completion through the pipeline input.
    /// Deterministic `process_at`/`advance` return events without invoking these.
    pub fn on_user_turn_inference_requested<F>(&self, handler: F) -> Result<(), ProcessorError>
    where
        F: Fn(crate::UserTurnInferenceRequest) -> Result<(), ProcessorError>
            + Send
            + Sync
            + 'static,
    {
        let mut handlers = self
            .inference_handlers
            .lock()
            .map_err(|_| ProcessorError::fatal("inference notification lock poisoned"))?;
        if handlers.len() >= 64 {
            return Err(ProcessorError::new(
                "at most 64 inference notification handlers are supported",
            ));
        }
        handlers.push(Arc::new(handler));
        Ok(())
    }
    pub(crate) async fn notify_inference_requests(
        &self,
        output: &AggregationOutput,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        Self::notify_handlers(&self.inference_handlers, output, ctx).await
    }
    async fn notify_handlers(
        handlers: &Mutex<Vec<InferenceHandler>>,
        output: &AggregationOutput,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let handlers = handlers
            .lock()
            .map_err(|_| ProcessorError::fatal("inference notification lock poisoned"))?
            .clone();
        for event in &output.events {
            if let AggregationEvent::UserTurnInferenceRequested(request) = event {
                for handler in &handlers {
                    if let Err(error) = handler(request.clone()) {
                        if error.fatal {
                            return Err(error);
                        }
                        ctx.push_error(pipecat_core::ErrorFrame {
                            error: error.message,
                            fatal: false,
                            processor: None,
                            category: error.category,
                        })
                        .await?;
                    }
                }
            }
        }
        Ok(())
    }
    pub(crate) fn with_assistant(
        mut self,
        assistant: std::sync::Weak<crate::LLMAssistantAggregator>,
    ) -> Self {
        // Construction precedes any sharing or processing, so this lock is not poisoned.
        Arc::get_mut(&mut self.state)
            .expect("unshared construction state")
            .get_mut()
            .expect("new state")
            .paired_assistant = Some(assistant);
        self
    }
    pub fn context(&self) -> SharedContext {
        self.context.clone()
    }
    pub fn interim_text(&self) -> Result<String, ProcessorError> {
        Ok(self.lock()?.interim.clone())
    }
    /// Latest 512 accepted final/interim frames, retaining provider payloads and metadata.
    pub fn transcript_frames(&self) -> Result<Vec<Frame>, ProcessorError> {
        Ok(self.lock()?.transcript_frames.iter().cloned().collect())
    }
    pub fn is_muted(&self) -> Result<bool, ProcessorError> {
        Ok(self.lock()?.muted)
    }
    pub fn deadline(&self) -> Result<Option<Duration>, ProcessorError> {
        Ok(self.lock()?.controller.deadline())
    }
    /// Deadline for user idleness, independent of the turn-stop watchdog.
    pub fn idle_deadline(&self) -> Result<Option<Duration>, ProcessorError> {
        self.idle.deadline()
    }
    pub fn waiting_for_user(&self) -> Result<bool, ProcessorError> {
        self.idle.waiting_for_user()
    }
    pub fn function_calls_in_progress(&self) -> Result<bool, ProcessorError> {
        self.idle.function_calls_in_progress()
    }
    /// Register an idle event handler. Handlers run in registration order on
    /// an owned background task and may push reminders using the supplied
    /// context. Keep handlers short; End, Stop and Cancel cancel pending work.
    pub fn on_user_turn_idle<F, Fut>(&self, handler: F) -> Result<(), ProcessorError>
    where
        F: Fn(ProcessorContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        self.idle.on_idle(handler)
    }
    /// Deterministic equivalent of explicitly waiting after an unanswered turn.
    pub fn wait_for_user_at(&self, now: Duration) -> Result<(), ProcessorError> {
        self.idle.wait_for_user(now)
    }
    /// Re-arm detection without waiting for another bot speaking cycle.
    pub fn wait_for_user(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.wait_for_user_at(self.origin.elapsed())?;
        self.schedule_idle_timer(ctx)
    }
    fn lock(&self) -> Result<MutexGuard<'_, UserState>, ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))
    }
    /// Synchronous state-machine entry point. Effects are returned, not emitted.
    pub fn process_at(
        &self,
        frame: Frame,
        direction: Direction,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut state = self.lock()?;
        let mut output = AggregationOutput::default();
        if matches!(
            frame.data,
            FrameData::Transcription(_)
                | FrameData::InterimTranscription(_)
                | FrameData::TranscriptionDetailed(_)
                | FrameData::InterimTranscriptionDetailed(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
                | FrameData::InputText(_)
                | FrameData::LlmMessagesAppend(_)
                | FrameData::LlmMessagesUpdate(_)
        ) && !state.seen.insert(frame.id)
        {
            return Ok(output);
        }
        if state.ended {
            if matches!(
                frame.data,
                FrameData::End(_)
                    | FrameData::EndDetailed(_)
                    | FrameData::Cancel(_)
                    | FrameData::CancelDetailed(_)
                    | FrameData::Stop
            ) {
                output.push(frame, direction);
            }
            return Ok(output);
        }
        validate_control(&frame.data)?;
        if matches!(frame.data, FrameData::UserTurnInferenceCompleted) {
            if let Some(value) = frame.metadata.get("user_turn_inference_id") {
                let id = value.as_u64().ok_or_else(|| {
                    ProcessorError::new("user_turn_inference_id must be an unsigned integer")
                })?;
                if state.controller.pending_inference_request() != Some(id) {
                    return Ok(output);
                }
            }
        }
        state.idle.validate(&frame.data)?;
        let previous_now = state.now;
        state.now = state.now.max(now);
        match &frame.data {
            FrameData::Start(_) | FrameData::StartDetailed(_) => {
                output.push(frame, direction);
                if state.muted {
                    output.broadcast(FrameData::UserMuteStarted);
                }
                return Ok(output);
            }
            FrameData::UserMuteStarted | FrameData::SttMute(true) => {
                state.manual_muted = true;
                if !state.muted {
                    state.muted = true;
                    state.interim.clear();
                    let events = state.controller.finish(true, TurnStopReason::Muted);
                    Self::apply_events(&mut state, &self.context, events, false, &mut output)?;
                }
                output.push(frame, direction);
                return Ok(output);
            }
            FrameData::UserMuteStopped | FrameData::SttMute(false) => {
                state.manual_muted = false;
                state.muted = state.policy_muted;
                output.push(frame, direction);
                if state.muted {
                    // The explicit unmute clears only its manual layer.
                    output.broadcast(FrameData::UserMuteStarted);
                }
                return Ok(output);
            }
            FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_) => {
                let run_llm = !matches!(
                    frame.data,
                    FrameData::Cancel(_) | FrameData::CancelDetailed(_)
                );
                let events = state.controller.finish(false, TurnStopReason::SessionEnd);
                Self::apply_events(&mut state, &self.context, events, run_llm, &mut output)?;
                // Text may be pending even if an externally controlled turn did not start.
                Self::commit(&mut state, &self.context, run_llm, &mut output)?;
                state.ended = true;
                state.timer_disabled = true;
                state.idle.stop(None)?;
                state.interim.clear();
                state.controller.cleanup();
                state.mute_strategies.cleanup();
                output.push(frame, direction);
                return Ok(output);
            }
            _ => {}
        }
        // Source policy admission uses the mute state at frame arrival; policy
        // transitions are broadcast before forwarding that frame. Every policy
        // still observes suppressed frames, without OR short-circuiting.
        let muted_at_arrival = state.muted;
        let policy_muted = match state.mute_strategies.process(&frame.data) {
            Ok(muted) => muted,
            Err(error) => {
                state.now = previous_now;
                return Err(error);
            }
        };
        state.policy_muted = policy_muted;
        let muted = state.manual_muted || policy_muted;
        if muted != state.muted {
            state.muted = muted;
            if muted {
                state.interim.clear();
                let events = state.controller.finish(true, TurnStopReason::Muted);
                Self::apply_events(&mut state, &self.context, events, false, &mut output)?;
            }
            output
                .events
                .push(AggregationEvent::UserMuteChanged { muted });
            output.broadcast(if muted {
                FrameData::UserMuteStarted
            } else {
                FrameData::UserMuteStopped
            });
        }
        if muted_at_arrival
            && matches!(
                frame.data,
                FrameData::InputAudio(_)
                    | FrameData::UserAudio(_)
                    | FrameData::Transcription(_)
                    | FrameData::InterimTranscription(_)
                    | FrameData::EagerTranscription(_)
                    | FrameData::TranscriptionDetailed(_)
                    | FrameData::InterimTranscriptionDetailed(_)
                    | FrameData::EagerTranscriptionDetailed(_)
                    | FrameData::Interruption
                    | FrameData::VadUserStartedSpeaking(_)
                    | FrameData::VadUserStoppedSpeaking(_)
                    | FrameData::VadUserStartedSpeakingDetailed(_)
                    | FrameData::VadUserStoppedSpeakingDetailed(_)
                    | FrameData::UserStartedSpeaking
                    | FrameData::UserStoppedSpeaking
                    | FrameData::ProposedUserStartedSpeaking
                    | FrameData::ProposedUserStoppedSpeaking
            )
        {
            return Ok(output);
        }
        // Adopted external starts also reach the idle tracker below. Capture
        // its waiting window before that frame closes it.
        let waiting_before_frame = state.idle.waiting_for_user()?;
        let hold_external_stop = matches!(frame.data, FrameData::UserStoppedSpeaking)
            && state.controller.config().stop_strategies.is_some()
            && state.controller.is_active();
        if let Err(error) = if hold_external_stop {
            Ok(())
        } else {
            state.idle.process(&frame.data, state.now)
        } {
            state.now = previous_now;
            return Err(error);
        }
        if matches!(
            frame.data,
            FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_)
                | FrameData::UserStartedSpeaking
                | FrameData::ProposedUserStartedSpeaking
        ) {
            state.held_external_stop = None;
        }
        match &frame.data {
            FrameData::UserStoppedSpeaking if hold_external_stop => {
                state.held_external_stop =
                    Some(pipecat_core::DirectedFrame::new(frame.clone(), direction));
            }
            FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_) => {
                let (text, append, spaces) = match &frame.data {
                    FrameData::Transcription(text) => (&text.text, true, false),
                    FrameData::TranscriptionDetailed(text) => (
                        &text.text.text,
                        text.text.append_to_context,
                        text.text.includes_inter_frame_spaces,
                    ),
                    _ => unreachable!(),
                };
                if text.trim().is_empty() {
                    return Ok(output);
                }
                if !state.controller.is_active()
                    && !state.controller.config().start_on_transcription
                    && state.controller.config().start_strategies.is_none()
                {
                    return Ok(output);
                }
                if let Some(id) = frame
                    .metadata
                    .get("transcript_id")
                    .and_then(|id| id.as_str())
                {
                    if state.transcript_ids.iter().any(|old| old == id) {
                        return Ok(output);
                    }
                    state.transcript_ids.push_back(id.into());
                    if state.transcript_ids.len() > 512 {
                        state.transcript_ids.pop_front();
                    }
                }
                if append {
                    state.aggregation.append(text, spaces);
                }
                Self::remember_transcript(&mut state, frame.clone());
                state.interim.clear();
            }
            FrameData::InterimTranscription(text) => {
                state.interim.clone_from(&text.text);
                Self::remember_transcript(&mut state, frame.clone());
            }
            FrameData::InterimTranscriptionDetailed(text) => {
                state.interim.clone_from(&text.text.text);
                Self::remember_transcript(&mut state, frame.clone());
            }
            FrameData::EagerTranscription(_)
            | FrameData::Translation(_)
            | FrameData::EagerTranscriptionDetailed(_)
            | FrameData::TranslationDetailed(_) => return Ok(output),
            FrameData::InputText(text) => {
                if text.append_to_context && !text.text.trim().is_empty() {
                    // Typed input is already final and does not need VAD/STT timers.
                    let events = state.controller.finish(true, TurnStopReason::External);
                    Self::apply_events(&mut state, &self.context, events, false, &mut output)?;
                    Self::commit(&mut state, &self.context, false, &mut output)?;
                    // Typed input is an immediate completed user turn; its
                    // inference must close any existing idle waiting window.
                    state
                        .idle
                        .process(&FrameData::UserStartedSpeaking, state.now)?;
                    state
                        .idle
                        .process(&FrameData::UserStoppedSpeaking, state.now)?;
                    if state.controller.config().enable_interruptions {
                        if let Some(assistant) = state
                            .paired_assistant
                            .as_ref()
                            .and_then(std::sync::Weak::upgrade)
                        {
                            output
                                .events
                                .extend(assistant.prepare_user_interrupt(false)?);
                        }
                        output.actions.push(AggregationAction::Interrupt);
                    }
                    self.context
                        .append(Message::text(Role::User, text.text.trim()))?;
                    Self::user_context(&state, &self.context, &mut output)?;
                }
                return Ok(output);
            }
            FrameData::LlmRun => {
                Self::user_context(&state, &self.context, &mut output)?;
                return Ok(output);
            }
            FrameData::LlmMessagesAppend(messages) | FrameData::LlmMessagesUpdate(messages) => {
                let mut context = self.context.lock()?;
                context.append_legacy(
                    &messages.messages,
                    matches!(frame.data, FrameData::LlmMessagesUpdate(_)),
                );
                drop(context);
                if messages.run_llm {
                    Self::user_context(&state, &self.context, &mut output)?;
                }
                return Ok(output);
            }
            FrameData::LlmMessagesAppendDetailed(messages)
            | FrameData::LlmMessagesUpdateDetailed(messages) => {
                let mut context = self.context.lock()?;
                if matches!(frame.data, FrameData::LlmMessagesUpdateDetailed(_)) {
                    context.set_rich(messages.messages.clone());
                } else {
                    context.append_rich(messages.messages.clone());
                }
                drop(context);
                if messages.run_llm == Some(true) {
                    Self::user_context(&state, &self.context, &mut output)?;
                }
                return Ok(output);
            }
            FrameData::LlmMessagesTransform(transform) => {
                self.context.lock()?.transform(&transform.transform)?;
                if transform.run_llm == Some(true) {
                    Self::user_context(&state, &self.context, &mut output)?;
                }
                return Ok(output);
            }
            FrameData::LlmSetTools(tools) => {
                self.context.lock()?.tools = tools.clone();
                output.push(frame, direction);
                return Ok(output);
            }
            FrameData::LlmSetToolChoice(choice) => {
                self.context.lock()?.tool_choice = Some(choice.clone());
                return Ok(output);
            }
            FrameData::ProposedUserStartedSpeaking
                if state.controller.resolves_proposed_turn_start_frames() => {}
            FrameData::ProposedUserStoppedSpeaking
                if state.controller.resolves_proposed_turn_stop_frames() => {}
            _ => output.push(frame.clone(), direction),
        }
        let events = state.controller.process(&frame.data, now);
        for event in &events {
            if let UserTurnEvent::Started { interrupt, .. } = event {
                state.interrupted_bot = *interrupt && !waiting_before_frame;
            }
        }
        Self::apply_events(&mut state, &self.context, events, true, &mut output)?;
        Ok(output)
    }
    fn remember_transcript(state: &mut UserState, frame: Frame) {
        state.transcript_frames.push_back(frame);
        if state.transcript_frames.len() > 512 {
            state.transcript_frames.pop_front();
        }
    }
    pub(crate) fn has_explicit_stop_strategies(&self) -> Result<bool, ProcessorError> {
        Ok(self.lock()?.controller.config().stop_strategies.is_some())
    }
    pub(crate) fn configure_model_gate(&self) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.ended || state.timer_running {
            return Err(ProcessorError::new(
                "configure the model gate before pipeline processing",
            ));
        }
        state.controller.configure_model_gate()
    }
    pub(crate) fn now(&self) -> Duration {
        self.origin.elapsed()
    }
    pub(crate) fn mark_model_complete_at(
        &self,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut state = self.lock()?;
        let mut output = AggregationOutput::default();
        if !state.ended && !state.timer_disabled {
            state.now = state.now.max(now);
            let events = state.controller.mark_model_complete(now);
            Self::apply_events(&mut state, &self.context, events, true, &mut output)?;
        }
        Ok(output)
    }
    /// Resolve an out-of-band request. Stale IDs, resumed speech, and revised
    /// transcripts produce no effects. Only accepted final text is committed.
    pub fn complete_inference_at(
        &self,
        request_id: u64,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut state = self.lock()?;
        let mut output = AggregationOutput::default();
        if !state.ended && !state.timer_disabled {
            let events = state.controller.complete_inference(request_id, now);
            if !events.is_empty() {
                state.now = state.now.max(now);
            }
            Self::apply_events(&mut state, &self.context, events, true, &mut output)?;
        }
        Ok(output)
    }
    pub fn pending_inference_request(&self) -> Result<Option<u64>, ProcessorError> {
        Ok(self.lock()?.controller.pending_inference_request())
    }
    pub fn advance(&self, now: Duration) -> Result<AggregationOutput, ProcessorError> {
        let mut output = self.advance_turn_at(now)?;
        if self.idle.advance(now)? {
            output.events.push(AggregationEvent::UserTurnIdle);
        }
        Ok(output)
    }
    pub(crate) fn advance_turn_at(
        &self,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        Self::advance_state(&self.state, &self.context, now)
    }
    fn advance_state(
        state: &Mutex<UserState>,
        context: &SharedContext,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut state = state
            .lock()
            .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))?;
        let mut output = AggregationOutput::default();
        if !state.ended && !state.timer_disabled {
            state.now = state.now.max(now);
            let events = state.controller.advance(now);
            Self::apply_events(&mut state, context, events, true, &mut output)?;
        }
        Ok(output)
    }
    fn apply_events(
        state: &mut UserState,
        context: &SharedContext,
        events: Vec<UserTurnEvent>,
        run_llm: bool,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        for event in events {
            match event {
                UserTurnEvent::InferenceRequested {
                    request_id,
                    strategy_index,
                    reason,
                } => {
                    output
                        .events
                        .push(AggregationEvent::UserTurnInferenceRequested(
                            crate::UserTurnInferenceRequest {
                                request_id,
                                strategy_index,
                                reason,
                                text: state.aggregation.text.trim().to_owned(),
                            },
                        ));
                }
                UserTurnEvent::ResetAggregation => {
                    state.aggregation.take();
                    state.interim.clear();
                }
                UserTurnEvent::StartStrategy(event) => {
                    output
                        .events
                        .push(AggregationEvent::UserStartStrategy(event));
                }
                UserTurnEvent::Started {
                    emit_speaking,
                    interrupt,
                } => {
                    state
                        .idle
                        .process(&FrameData::UserStartedSpeaking, state.now)?;
                    if let Some(assistant) = state
                        .paired_assistant
                        .as_ref()
                        .and_then(std::sync::Weak::upgrade)
                    {
                        assistant.prepare_user_start()?;
                    }
                    if emit_speaking {
                        output.broadcast(FrameData::UserStartedSpeaking);
                    }
                    if interrupt {
                        if let Some(assistant) = state
                            .paired_assistant
                            .as_ref()
                            .and_then(std::sync::Weak::upgrade)
                        {
                            output
                                .events
                                .extend(assistant.prepare_user_interrupt(emit_speaking)?);
                        }
                        output.actions.push(AggregationAction::Interrupt);
                    }
                    output.events.push(AggregationEvent::UserTurnStarted);
                }
                UserTurnEvent::Stopped {
                    emit_speaking,
                    reason,
                } => {
                    state
                        .idle
                        .process(&FrameData::UserStoppedSpeaking, state.now)?;
                    let held = state.held_external_stop.take();
                    if !emit_speaking && !matches!(reason, TurnStopReason::SessionEnd) {
                        if let Some(held) = held {
                            output.push(held.frame, held.direction);
                        }
                    }
                    // Stop is sent before inference so the assistant half sees the
                    // complete speaking boundary before results can request a rerun.
                    if emit_speaking {
                        let mut boundary = Frame::new(FrameData::UserStoppedSpeaking);
                        boundary
                            .metadata
                            .insert("user_context_follows".into(), false.into());
                        output.actions.push(AggregationAction::Broadcast(boundary));
                    }
                    let interrupted_bot = std::mem::take(&mut state.interrupted_bot);
                    let text = Self::commit(state, context, run_llm, output)?;
                    if text.is_empty()
                        && !state.timer_disabled
                        && !matches!(reason, TurnStopReason::SessionEnd | TurnStopReason::Muted)
                        && !(run_llm
                            && Self::recover_empty_turn(state, context, interrupted_bot, output)?)
                    {
                        state.idle.wait_for_user(state.now)?;
                    }
                    state.interim.clear();
                    output
                        .events
                        .push(AggregationEvent::UserTurnStopped { text, reason });
                }
            }
        }
        Ok(())
    }
    fn commit(
        state: &mut UserState,
        context: &SharedContext,
        run_llm: bool,
        output: &mut AggregationOutput,
    ) -> Result<String, ProcessorError> {
        let text = state.aggregation.take();
        if !text.is_empty() {
            context.append(Message::text(Role::User, text.clone()))?;
            state.consecutive_empty_recoveries = 0;
            if run_llm {
                Self::user_context(state, context, output)?;
            }
        }
        Ok(text)
    }
    fn recover_empty_turn(
        state: &mut UserState,
        context: &SharedContext,
        interrupted_bot: bool,
        output: &mut AggregationOutput,
    ) -> Result<bool, ProcessorError> {
        let Some(config) = &state.empty_user_turn else {
            return Ok(false);
        };
        let prompt = if interrupted_bot {
            &config.interrupted_prompt
        } else {
            &config.idle_prompt
        };
        let Some(prompt) = prompt.as_ref().filter(|prompt| !prompt.is_empty()) else {
            return Ok(false);
        };
        if state.consecutive_empty_recoveries >= config.max_consecutive_recoveries
            || state.muted
            || state.idle.function_calls_in_progress()?
        {
            return Ok(false);
        }
        state.consecutive_empty_recoveries += 1;
        context.append(Message::text(Role::Developer, prompt.clone()))?;
        Self::user_context(state, context, output)?;
        Ok(true)
    }
    fn user_context(
        state: &UserState,
        context: &SharedContext,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let snapshot = match state
            .paired_assistant
            .as_ref()
            .and_then(std::sync::Weak::upgrade)
        {
            Some(assistant) => assistant.prepare_user_context()?,
            None => context.frame_data()?,
        };
        output.context_frame(snapshot, Direction::Downstream);
        Ok(())
    }
    pub(crate) fn schedule_timer(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.schedule_idle_timer(ctx)?;
        self.changed.notify_one();
        let mut state = self.lock()?;
        if state.timer_disabled
            || state.ended
            || state.timer_running
            || state.controller.deadline().is_none()
        {
            return Ok(());
        }
        state.timer_running = true;
        let shared = self.state.clone();
        let context = self.context.clone();
        let changed = self.changed.clone();
        let origin = self.origin;
        let output = ctx.clone();
        let idle = self.idle.clone();
        let handlers = self.inference_handlers.clone();
        match ctx.spawn("user-turn-deadline", false, async move {
            let _guard = UserTimerGuard(shared.clone());
            loop {
                let deadline = {
                    let state = shared
                        .lock()
                        .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))?;
                    if state.timer_disabled || state.ended {
                        return Ok(());
                    }
                    state.controller.deadline()
                };
                tokio::select! {
                    _ = async {
                        match deadline.and_then(|deadline| origin.checked_add(deadline)) {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => {
                        let effects = Self::advance_state(&shared, &context, origin.elapsed())?;
                        Self::notify_handlers(&handlers, &effects, &output).await?;
                        effects.emit(&output).await?;
                        idle.schedule(&output)?;
                    }
                    _ = changed.notified() => {}
                }
            }
        }) {
            Ok(id) => {
                state.timer_id = Some(id);
                Ok(())
            }
            Err(error) => {
                state.timer_running = false;
                Err(error)
            }
        }
    }
    pub(crate) fn schedule_idle_timer(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.idle.schedule(ctx)
    }
}
struct UserTimerGuard(Arc<Mutex<UserState>>);
impl Drop for UserTimerGuard {
    fn drop(&mut self) {
        if let Ok(mut state) = self.0.lock() {
            state.timer_running = false;
            state.timer_id = None;
        }
    }
}
#[async_trait]
impl FrameProcessor for LLMUserAggregator {
    fn name(&self) -> &str {
        "LLMUserAggregator"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let output = self.process_at(frame, direction, self.origin.elapsed())?;
        self.notify_inference_requests(&output, ctx).await?;
        output.emit(ctx).await?;
        self.schedule_timer(ctx)
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.idle.stop(Some(ctx))?;
        let mut state = self.lock()?;
        state.timer_disabled = true;
        if let Some(id) = state.timer_id.take() {
            ctx.cancel_task(id);
        }
        state.timer_running = false;
        self.changed.notify_one();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.idle.stop(None)?;
        self.inference_handlers
            .lock()
            .map_err(|_| ProcessorError::fatal("inference notification lock poisoned"))?
            .clear();
        let mut state = self.lock()?;
        state.ended = true;
        state.timer_disabled = true;
        state.controller.cleanup();
        state.mute_strategies.cleanup();
        state.interim.clear();
        self.changed.notify_one();
        Ok(())
    }
}

pub(crate) fn validate_control(data: &FrameData) -> Result<(), ProcessorError> {
    if matches!(data, FrameData::SttMetadata(metadata)
        if !metadata.ttfs_p99_latency.is_finite()
            || !(0.0..=86_400.0).contains(&metadata.ttfs_p99_latency))
    {
        return Err(ProcessorError::new(
            "STT P99 latency must be finite and within 0..86400 seconds",
        ));
    }
    if matches!(data, FrameData::LlmServiceMetadata(metadata) if metadata.is_realtime_service) {
        return Err(ProcessorError::new(
            "realtime-service turn strategy replacement is not supported by the cascade user aggregator",
        ));
    }
    if let FrameData::UserIdleTimeoutUpdate { timeout } = data {
        crate::idle::timeout_duration(*timeout)?;
    }
    let vad = match data {
        FrameData::VadUserStartedSpeaking(vad) | FrameData::VadUserStoppedSpeaking(vad) => {
            Some((vad.duration_secs, vad.timestamp))
        }
        FrameData::VadUserStartedSpeakingDetailed(vad) => Some((vad.start_secs, vad.timestamp)),
        FrameData::VadUserStoppedSpeakingDetailed(vad) => Some((vad.stop_secs, vad.timestamp)),
        _ => None,
    };
    if vad.is_some_and(|(duration, timestamp)| {
        !duration.is_finite()
            || !(0.0..=86_400.0).contains(&duration)
            || !timestamp.is_finite()
            || timestamp < 0.0
    }) {
        return Err(ProcessorError::new(
            "VAD timing requires finite nonnegative timestamps and durations within 0..86400 seconds",
        ));
    }
    let strategies = match data {
        FrameData::ServiceMetadata(metadata) => metadata.user_turn_strategies.as_ref(),
        FrameData::SttMetadata(metadata) => metadata.base.user_turn_strategies.as_ref(),
        FrameData::LlmServiceMetadata(metadata) => metadata.base.user_turn_strategies.as_ref(),
        _ => None,
    };
    if strategies.is_some_and(|strategies| strategies.start.is_some() || strategies.stop.is_some())
    {
        return Err(ProcessorError::new(
            "automatic service-recommended strategy replacement is not supported; configure the user aggregator explicitly",
        ));
    }
    Ok(())
}
