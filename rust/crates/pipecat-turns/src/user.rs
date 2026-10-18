use crate::{
    AggregationAction, AggregationEvent, AggregationOutput, RecentFrames, SharedContext,
    TurnStopReason, UserTurnConfig, UserTurnController, UserTurnEvent,
};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, NativeEagerSource, NativeEagerTurn,
    ProcessorContext, ProcessorError,
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
    /// Opt in to provider-owned eager turn prediction and bounded speculation.
    pub eager: Option<crate::EagerUserTurnConfig>,
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
            eager: None,
            initially_muted: false,
            user_idle_timeout: Duration::ZERO,
            mute_strategies: vec![],
            empty_user_turn: Some(crate::EmptyUserTurnConfig::default()),
        }
    }
}
enum ControllerBinding {
    Inactive,
    Unbound,
    Scoped(NativeEagerTurn),
}
struct UserState {
    controller: UserTurnController,
    binding: ControllerBinding,
    preserve_start_text: bool,
    context_causes: pipecat_core::NativeRealtimeContextProducer,
    context_cause_failed: bool,
    eager: Option<crate::eager::EagerState>,
    idle: Arc<crate::idle::UserIdleTracker>,
    now: Duration,
    paired_assistant: Option<std::sync::Weak<crate::LLMAssistantAggregator>>,
    aggregation: Arc<crate::realtime::RealtimeState>,
    explicit_strategies: bool,
    realtime_configured: bool,
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
    input_controls_closed: bool,
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
impl UserState {
    fn accepts_external_start(&self, data: &FrameData, direction: Direction) -> bool {
        direction == Direction::Downstream
            && (matches!(data, FrameData::UserStartedSpeaking)
                || (matches!(data, FrameData::ProposedUserStartedSpeaking)
                    && self.controller.resolves_proposed_turn_start_frames()))
    }
    fn owns_source_turn(&self, turn: &NativeEagerTurn) -> bool {
        self.controller.is_active()
            && matches!(&self.binding, ControllerBinding::Scoped(current) if current == turn)
    }
    fn replace_binding(
        &mut self,
        binding: ControllerBinding,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        if matches!(self.binding, ControllerBinding::Scoped(_)) {
            // Supersession discards controller events without committing or
            // recovering A. Unrelated ordinary text and its metadata stay put.
            let _ = self.controller.finish(false, TurnStopReason::SessionEnd);
            self.held_external_stop = None;
            self.interim.clear();
            self.interrupted_bot = false;
            if let Some(eager) = &mut self.eager {
                eager.abandon_source_turn(output)?;
            }
            self.preserve_start_text = true;
        }
        self.binding = binding;
        Ok(())
    }
    fn discard_scoped_turn(
        &mut self,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        if let Some(eager) = &mut self.eager {
            eager.abandon_source_turn(output)?;
        }
        self.binding = ControllerBinding::Inactive;
        self.held_external_stop = None;
        self.interim.clear();
        self.interrupted_bot = false;
        self.preserve_start_text = true;
        Ok(())
    }
    fn deadline(&self) -> Result<Option<Duration>, ProcessorError> {
        Ok([
            if self.input_controls_closed {
                None
            } else {
                self.controller.deadline()
            },
            self.aggregation.deadline()?,
            self.eager
                .as_ref()
                .and_then(crate::eager::EagerState::deadline),
        ]
        .into_iter()
        .flatten()
        .min())
    }
    fn cancel_eager(&mut self, output: &mut AggregationOutput) -> Result<(), ProcessorError> {
        if let Some(eager) = &mut self.eager {
            eager.cancel(output)?;
        }
        Ok(())
    }
}
impl LLMUserAggregator {
    pub fn new(context: SharedContext, config: UserAggregatorConfig) -> Self {
        let realtime = Arc::new(crate::realtime::RealtimeState::new(
            crate::RealtimeServiceMode::Auto,
            false,
            config.eager.is_some(),
        ));
        Self::new_paired(context, config, realtime)
    }
    pub(crate) fn new_paired(
        context: SharedContext,
        config: UserAggregatorConfig,
        realtime: Arc<crate::realtime::RealtimeState>,
    ) -> Self {
        let mut config = config;
        if let Some(eager) = &config.eager {
            eager.configure_turn(&mut config.turn);
        }
        let origin = realtime.origin;
        let explicit_strategies = config.turn.start_strategies.is_some()
            || config.turn.stop_strategies.is_some()
            || config.turn.require_model_completion;
        realtime
            .strategy_policy(explicit_strategies, config.turn.enable_interruptions)
            .expect("new realtime state");
        let mut controller = UserTurnController::new(config.turn);
        if realtime.enabled().expect("new realtime state") {
            controller.apply_realtime_mode();
        }
        realtime
            .resolvers(
                controller.resolves_proposed_turn_start_frames(),
                controller.resolves_proposed_turn_stop_frames(),
            )
            .expect("new realtime state");
        let idle = Arc::new(crate::idle::UserIdleTracker::new(
            config.user_idle_timeout,
            origin,
        ));
        Self {
            context,
            state: Arc::new(Mutex::new(UserState {
                controller,
                binding: ControllerBinding::Inactive,
                preserve_start_text: false,
                context_causes: Default::default(),
                context_cause_failed: false,
                eager: config.eager.map(crate::eager::EagerState::new),
                idle: idle.clone(),
                now: Duration::ZERO,
                paired_assistant: None,
                aggregation: realtime.clone(),
                explicit_strategies,
                realtime_configured: realtime.enabled().expect("new realtime state"),
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
                input_controls_closed: false,
            })),
            changed: realtime.changed.clone(),
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
    pub fn realtime_service_enabled(&self) -> Result<bool, ProcessorError> {
        self.lock()?.aggregation.enabled()
    }
    fn sync_realtime(state: &mut UserState, force: bool) -> Result<(), ProcessorError> {
        if state.aggregation.enabled()? {
            if !state.realtime_configured || force {
                state.controller.apply_realtime_mode();
                state.realtime_configured = true;
            }
            state.empty_user_turn = None;
        }
        state.aggregation.resolvers(
            state.controller.resolves_proposed_turn_start_frames(),
            state.controller.resolves_proposed_turn_stop_frames(),
        )
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
        self.lock()?.deadline()
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
    #[cfg(test)]
    pub(crate) fn history_controls_closed(&self) -> bool {
        self.lock().unwrap().input_controls_closed
    }
    pub(crate) fn reserve_history(
        &self,
    ) -> Result<crate::RealtimeUserHistoryEndpoint, ProcessorError> {
        let state = self.lock()?;
        if state.ended || state.timer_running || state.context_cause_failed {
            return Err(ProcessorError::new(
                "history requires an unused healthy user aggregator",
            ));
        }
        state.aggregation.reserve_history(&self.context)
    }
    /// Synchronous state-machine entry point. Effects are returned, not emitted.
    pub fn process_at(
        &self,
        mut frame: Frame,
        direction: Direction,
        now: Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        let aggregation = self.lock()?.aggregation.clone();
        let registered_history = aggregation.history_validate_reducer(&frame, direction)?;
        aggregation.mark_used()?;
        if aggregation.history_bound()? {
            crate::user_history::validate_live_control(&frame.data)?;
        }
        // Consume and validate private authority before deduplication, policy,
        // history retention or forwarding. Upstream/private or mutated roles
        // fail closed here rather than reaching a wire adapter.
        let source = frame.take_native_eager_source(direction)?;
        let mut state = self.lock()?;
        let mut output = AggregationOutput::default();
        if source.is_some() && state.eager.is_none() {
            return Err(ProcessorError::new(
                "native eager source frames require an eager user aggregator",
            ));
        }
        let mut source_prediction_owner = None;
        // Reject ownership before deduplication, clocks, mute policy, idle state
        // or cancellation. Start claim and binding share this UserState lock.
        match &source {
            Some(NativeEagerSource::Start(turn)) => {
                if state.ended
                    || state.muted
                    || !state.accepts_external_start(&frame.data, direction)
                {
                    return Ok(output);
                }
                let mut policies = state.mute_strategies.clone();
                if state.manual_muted || policies.process(&frame.data)? {
                    return Ok(output);
                }
                if !state
                    .eager
                    .as_ref()
                    .expect("source consumer checked")
                    .claim_source_start(turn)
                {
                    return Ok(output);
                }
                state.replace_binding(ControllerBinding::Scoped(turn.clone()), &mut output)?;
            }
            Some(NativeEagerSource::Prediction(prediction)) => {
                if !state.owns_source_turn(prediction.turn())
                    || !prediction.is_live()
                    || state.muted
                {
                    return Ok(output);
                }
                let mut policies = state.mute_strategies.clone();
                if state.manual_muted || policies.process(&frame.data)? {
                    return Ok(output);
                }
                source_prediction_owner = state
                    .eager
                    .as_ref()
                    .expect("source consumer checked")
                    .claim_source_prediction(prediction)?;
                if source_prediction_owner.is_none() {
                    return Ok(output);
                }
            }
            Some(NativeEagerSource::Final(receipt)) => {
                if !state.owns_source_turn(receipt.turn()) || !receipt.turn().is_live() {
                    return Ok(output);
                }
            }
            Some(NativeEagerSource::Stop { turn, .. }) => {
                if !state.owns_source_turn(turn) || !turn.is_live() {
                    return Ok(output);
                }
            }
            Some(NativeEagerSource::Cancel(prediction)) => {
                state
                    .eager
                    .as_mut()
                    .expect("source consumer checked")
                    .cancel_source(prediction, &mut output)?;
                return Ok(output);
            }
            None => {}
        }
        let mut deferred_source_stop = None;
        match &source {
            Some(NativeEagerSource::Final(receipt)) => {
                let ordinary_offset = state.aggregation.text()?.len();
                let eager = state.eager.as_mut().expect("source consumer checked");
                if !eager.stage_source_final(receipt, ordinary_offset)? {
                    return Ok(output);
                }
                deferred_source_stop = eager.take_ready_source_stop();
            }
            Some(NativeEagerSource::Stop {
                turn,
                commit: Some(receipt),
            }) => {
                if receipt.turn() != turn
                    || !state
                        .eager
                        .as_mut()
                        .expect("source consumer checked")
                        .source_stop(receipt, frame.clone(), direction)
                {
                    return Ok(output);
                }
            }
            Some(NativeEagerSource::Stop { turn, commit: None })
                if !state
                    .eager
                    .as_ref()
                    .expect("source consumer checked")
                    .claim_empty_stop(turn) =>
            {
                return Ok(output);
            }
            _ => {}
        }
        if state.context_cause_failed
            && matches!(
                frame.data,
                FrameData::InputText(_)
                    | FrameData::LlmRun
                    | FrameData::LlmMessagesAppend(_)
                    | FrameData::LlmMessagesUpdate(_)
                    | FrameData::LlmMessagesAppendDetailed(_)
                    | FrameData::LlmMessagesUpdateDetailed(_)
                    | FrameData::LlmMessagesTransform(_)
            )
        {
            return Err(ProcessorError::fatal("a realtime context edit was retained locally but not delivered; create a new pair and provider session from repaired context"));
        }
        if matches!(
            frame.data,
            FrameData::Transcription(_)
                | FrameData::InterimTranscription(_)
                | FrameData::TranscriptionDetailed(_)
                | FrameData::InterimTranscriptionDetailed(_)
                | FrameData::EagerTranscription(_)
                | FrameData::EagerTranscriptionDetailed(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
                | FrameData::InputText(_)
                | FrameData::LlmMessagesAppend(_)
                | FrameData::LlmMessagesUpdate(_)
        ) && source.is_none()
            && !registered_history
            && !state.seen.insert(frame.id)
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
        let recommended = crate::realtime::recommended_strategies(
            &frame.data,
            state.explicit_strategies,
            state.controller.config().enable_interruptions,
        )?;
        state.aggregation.observe(&frame.data)?;
        if let Some((start, stop)) = recommended {
            state.controller.replace_strategies(start, stop);
        }
        Self::sync_realtime(
            &mut state,
            matches!(&frame.data, FrameData::LlmServiceMetadata(metadata) if metadata.is_realtime_service),
        )?;
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
                state.cancel_eager(&mut output)?;
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
                state.cancel_eager(&mut output)?;
                let run_llm = !matches!(
                    frame.data,
                    FrameData::Cancel(_) | FrameData::CancelDetailed(_)
                );
                let events = state.controller.finish(false, TurnStopReason::SessionEnd);
                Self::apply_events(&mut state, &self.context, events, run_llm, &mut output)?;
                // This global terminal independently flushes ordinary input.
                // A retired scoped finish leaves that buffer untouched and
                // drops its separate source_final stage without merging it.
                // Realtime sessions write trailing speech even on cancellation.
                if state.aggregation.enabled()? {
                    state.aggregation.stop_timer()?;
                    state.aggregation.flush(&self.context, None, &mut output)?;
                } else {
                    Self::commit(&mut state, &self.context, run_llm, &mut output)?;
                }
                state.ended = true;
                state.binding = ControllerBinding::Inactive;
                state.timer_disabled = true;
                state.idle.stop(None)?;
                state.interim.clear();
                state.controller.cleanup();
                state.mute_strategies.cleanup();
                if let Some(eager) = &mut state.eager {
                    eager.reset_turn();
                }
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
                state.cancel_eager(&mut output)?;
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
        if source.is_none() && state.accepts_external_start(&frame.data, direction) && !state.muted
        {
            state.replace_binding(ControllerBinding::Unbound, &mut output)?;
        }
        if matches!(frame.data, FrameData::EagerEndOfTurnCancel) {
            let before = output.actions.len();
            if let Some(eager) = &mut state.eager {
                eager.cancel_matching(frame.native_speculation(), &mut output)?;
            }
            // The incoming cancellation already travels through the pipeline.
            output.actions.truncate(before);
        }
        if matches!(
            frame.data,
            FrameData::Interruption
                | FrameData::UserStartedSpeaking
                | FrameData::ProposedUserStartedSpeaking
                | FrameData::InputText(_)
                | FrameData::LlmMessagesUpdate(_)
                | FrameData::LlmMessagesAppend(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesTransform(_)
        ) {
            state.cancel_eager(&mut output)?;
        }
        // Adopted external starts also reach the idle tracker below. Capture
        // its waiting window before that frame closes it.
        let waiting_before_frame = state.idle.waiting_for_user()?;
        let hold_external_stop = !state.input_controls_closed
            && matches!(frame.data, FrameData::UserStoppedSpeaking)
            && state.controller.config().stop_strategies.is_some()
            && state.controller.is_active();
        if let Err(error) = if hold_external_stop || state.input_controls_closed {
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
                let completion_only = text.trim().is_empty()
                    && match &frame.data {
                        FrameData::Transcription(text) => text.finalized,
                        FrameData::TranscriptionDetailed(text) => text.finalized,
                        _ => false,
                    }
                    && source.is_none()
                    && state.eager.is_none()
                    && !state.aggregation.enabled()?
                    && !state.aggregation.text()?.trim().is_empty()
                    && state.controller.accepts_empty_final();
                if text.trim().is_empty()
                    && !completion_only
                    && !state
                        .controller
                        .config()
                        .stop_strategies
                        .as_ref()
                        .is_some_and(
                            crate::UserTurnStopStrategies::resolves_proposed_turn_stop_frames,
                        )
                {
                    return Ok(output);
                }
                if !state.aggregation.enabled()?
                    && !state.controller.is_active()
                    && !state.controller.config().start_on_transcription
                    && state.controller.config().start_strategies.is_none()
                {
                    return Ok(output);
                }
                if let Some(id) = frame
                    .metadata
                    .get("transcript_id")
                    .and_then(|id| id.as_str())
                    .filter(|_| source.is_none() && !registered_history)
                {
                    if state.transcript_ids.iter().any(|old| old == id) {
                        return Ok(output);
                    }
                    state.transcript_ids.push_back(id.into());
                    if state.transcript_ids.len() > 512 {
                        state.transcript_ids.pop_front();
                    }
                }
                if completion_only {
                    // Private/source, mute, terminal, Frame.id and transcript_id
                    // admission already ran under this lock. Do not expose this
                    // control to eager text matching or ordinary transcript starts.
                    state.interim.clear();
                    let events = state.controller.process(&frame.data, now);
                    Self::apply_events(&mut state, &self.context, events, true, &mut output)?;
                    return Ok(output);
                }
                if source.is_none() {
                    if let Some(eager) = &mut state.eager {
                        eager.record_transcript(text, append, spaces, &mut output)?;
                    }
                    if append && !text.trim().is_empty() {
                        state.aggregation.append_registered(
                            text,
                            spaces,
                            registered_history.then_some(&frame),
                        )?;
                    }
                }
                Self::remember_transcript(&mut state, frame.clone());
                if registered_history {
                    while state.transcript_frames.len() > 16 {
                        state.transcript_frames.pop_front();
                    }
                }
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
            FrameData::EagerTranscription(_) | FrameData::EagerTranscriptionDetailed(_) => {
                if state.aggregation.enabled()? {
                    return Ok(output);
                }
                if direction != Direction::Downstream
                    || !state.controller.is_active()
                    || state.eager.is_none()
                {
                    return Ok(output);
                }
                let text = match &frame.data {
                    FrameData::EagerTranscription(text) => &text.text,
                    FrameData::EagerTranscriptionDetailed(text) => &text.text.text,
                    _ => unreachable!(),
                };
                if let Some(assistant) = state
                    .paired_assistant
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                {
                    assistant.prepare_user_context()?;
                }
                let now = state.now;
                let eager = state.eager.as_mut().expect("enabled");
                if let Some(NativeEagerSource::Prediction(prediction)) = &source {
                    eager.begin_source(
                        prediction,
                        source_prediction_owner
                            .take()
                            .expect("source adoption reserved before side effects"),
                        &self.context,
                        now,
                        &mut output,
                    )?;
                } else {
                    eager.begin(text, &self.context, now, &mut output)?;
                }
                return Ok(output);
            }
            FrameData::Translation(_) | FrameData::TranslationDetailed(_) => return Ok(output),
            FrameData::InputText(text) => {
                if text.append_to_context && !text.text.trim().is_empty() {
                    // Typed input is already final and does not need VAD/STT timers.
                    let events = state.controller.finish(true, TurnStopReason::External);
                    Self::apply_events(&mut state, &self.context, events, false, &mut output)?;
                    // Preserve ordinary input before this independent typed
                    // request; a discarded scoped final never enters this buffer.
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
                            let effects = assistant.prepare_user_interrupt(false)?;
                            Self::assistant_interrupt_effects(effects, &mut output);
                        } else {
                            output.actions.push(AggregationAction::Interrupt);
                        }
                    }
                    let mut context = self.context.lock()?;
                    let start = context.messages.len();
                    context.append_native(Message::text(Role::User, text.text.trim()));
                    let end = context.messages.len();
                    let lineage = context.lineage;
                    drop(context);
                    Self::explicit_user_context(
                        &mut state,
                        &self.context,
                        &mut output,
                        pipecat_core::RealtimeContextIntent::Append { start, end },
                        Some(lineage),
                        true,
                    )?;
                }
                return Ok(output);
            }
            FrameData::LlmRun => {
                Self::explicit_user_context(
                    &mut state,
                    &self.context,
                    &mut output,
                    pipecat_core::RealtimeContextIntent::Run,
                    None,
                    true,
                )?;
                return Ok(output);
            }
            FrameData::LlmMessagesAppend(messages) | FrameData::LlmMessagesUpdate(messages) => {
                let mut context = self.context.lock()?;
                let start = context.messages.len();
                context.append_legacy(
                    &messages.messages,
                    matches!(frame.data, FrameData::LlmMessagesUpdate(_)),
                );
                let intent = if matches!(frame.data, FrameData::LlmMessagesUpdate(_)) {
                    pipecat_core::RealtimeContextIntent::Replace
                } else {
                    pipecat_core::RealtimeContextIntent::Append {
                        start,
                        end: context.messages.len(),
                    }
                };
                let lineage = context.lineage;
                drop(context);
                Self::explicit_user_context(
                    &mut state,
                    &self.context,
                    &mut output,
                    intent,
                    Some(lineage),
                    messages.run_llm,
                )?;
                return Ok(output);
            }
            FrameData::LlmMessagesAppendDetailed(messages)
            | FrameData::LlmMessagesUpdateDetailed(messages) => {
                let mut context = self.context.lock()?;
                let start = context.messages.len();
                if matches!(frame.data, FrameData::LlmMessagesUpdateDetailed(_)) {
                    context.set_rich(messages.messages.clone());
                } else {
                    context.append_rich(messages.messages.clone());
                }
                let intent = if matches!(frame.data, FrameData::LlmMessagesUpdateDetailed(_)) {
                    pipecat_core::RealtimeContextIntent::Replace
                } else {
                    pipecat_core::RealtimeContextIntent::Append {
                        start,
                        end: context.messages.len(),
                    }
                };
                let lineage = context.lineage;
                drop(context);
                Self::explicit_user_context(
                    &mut state,
                    &self.context,
                    &mut output,
                    intent,
                    Some(lineage),
                    messages.run_llm == Some(true),
                )?;
                return Ok(output);
            }
            FrameData::LlmMessagesTransform(transform) => {
                let lineage = {
                    let mut context = self.context.lock()?;
                    context.transform(&transform.transform)?;
                    context.lineage
                };
                Self::explicit_user_context(
                    &mut state,
                    &self.context,
                    &mut output,
                    pipecat_core::RealtimeContextIntent::Replace,
                    Some(lineage),
                    transform.run_llm == Some(true),
                )?;
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
        if state.input_controls_closed {
            return Ok(output);
        }
        let events = state.controller.process(&frame.data, now);
        for event in &events {
            if let UserTurnEvent::Started { interrupt, .. } = event {
                state.interrupted_bot = *interrupt && !waiting_before_frame;
            }
        }
        Self::apply_events(&mut state, &self.context, events, true, &mut output)?;
        if let Some(stop) = deferred_source_stop.filter(|_| state.controller.is_active()) {
            // Preserve the same state-lock decision as final admission. An
            // unlocked recursive process_at would let a newer turn interleave
            // before this already-consumed stop and accidentally close it.
            let hold = matches!(stop.frame.data, FrameData::UserStoppedSpeaking)
                && state.controller.config().stop_strategies.is_some();
            if hold {
                state.held_external_stop = Some(stop.clone());
            } else {
                state.idle.process(&stop.frame.data, state.now)?;
                if !matches!(stop.frame.data, FrameData::ProposedUserStoppedSpeaking)
                    || !state.controller.resolves_proposed_turn_stop_frames()
                {
                    output.push(stop.frame.clone(), stop.direction);
                }
            }
            let events = state.controller.process(&stop.frame.data, now);
            Self::apply_events(&mut state, &self.context, events, true, &mut output)?;
        }
        if state.accepts_external_start(&frame.data, direction) {
            state.preserve_start_text = false;
        }
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
        let aggregation = state.aggregation.clone();
        aggregation.configure_model_wrapper(&mut state.controller)?;
        state.explicit_strategies = true;
        Ok(())
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
            Self::sync_realtime(&mut state, false)?;
            state.now = state.now.max(now);
            state.aggregation.service_history(context, &mut output)?;
            state.aggregation.flush(context, Some(now), &mut output)?;
            if state.input_controls_closed {
                return Ok(output);
            }
            if let Some(eager) = &mut state.eager {
                eager.advance(now, &mut output)?;
            }
            let events = state.controller.advance(now);
            Self::apply_events(&mut state, context, events, true, &mut output)?;
        }
        Ok(output)
    }
    fn assistant_interrupt_effects(effects: AggregationOutput, output: &mut AggregationOutput) {
        output.events.extend(effects.events);
        let mut feedback = Vec::new();
        for action in effects.actions {
            match action {
                AggregationAction::RealtimeUserContext(_) => output.actions.push(action),
                AggregationAction::AssistantStopped(_) => feedback.push(action),
                AggregationAction::Push(ref directed)
                    if matches!(
                        directed.frame.data,
                        FrameData::LlmContextAssistantTimestamp { .. }
                            | FrameData::LlmContextAssistantTurn(_)
                    ) =>
                {
                    feedback.push(action)
                }
                _ => {}
            }
        }
        output.actions.push(AggregationAction::Interrupt);
        output.actions.extend(feedback);
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
                                text: state.aggregation.text()?.trim().to_owned(),
                            },
                        ));
                }
                UserTurnEvent::ResetAggregation => {
                    state.aggregation.take()?;
                    if let Some(eager) = &mut state.eager {
                        eager.reset_turn();
                    }
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
                    if !state.preserve_start_text {
                        if let Some(eager) = &mut state.eager {
                            eager.reset_turn();
                        }
                    }
                    if matches!(state.binding, ControllerBinding::Inactive) {
                        state.binding = ControllerBinding::Unbound;
                    }
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
                            let effects = assistant.prepare_user_interrupt(emit_speaking)?;
                            Self::assistant_interrupt_effects(effects, output);
                        } else {
                            output.actions.push(AggregationAction::Interrupt);
                        }
                    }
                    output.events.push(AggregationEvent::UserTurnStarted);
                }
                UserTurnEvent::Stopped {
                    emit_speaking,
                    reason,
                } => {
                    let no_source_final = state
                        .eager
                        .as_ref()
                        .is_none_or(|eager| !eager.has_source_final());
                    if no_source_final
                        && matches!(&state.binding, ControllerBinding::Scoped(turn) if !turn.is_active())
                    {
                        state.discard_scoped_turn(output)?;
                        continue;
                    }
                    let realtime = state.aggregation.enabled()?;
                    let pending_text = state.aggregation.text()?;
                    let bound = match &mut state.eager {
                        Some(eager) => eager.finish_source(
                            &pending_text,
                            reason,
                            state.now,
                            context,
                            output,
                        )?,
                        None => crate::eager::BoundFinish::NoBoundDecision,
                    };
                    let confirmation = match bound {
                        crate::eager::BoundFinish::Confirmed { capability, text } => {
                            state.aggregation.take()?;
                            state.aggregation.append(&text, true)?;
                            Some(capability)
                        }
                        crate::eager::BoundFinish::CurrentFinalFallback { text } => {
                            state.aggregation.take()?;
                            state.aggregation.append(&text, true)?;
                            None
                        }
                        crate::eager::BoundFinish::RetiredScopedDiscard => {
                            state.discard_scoped_turn(output)?;
                            continue;
                        }
                        crate::eager::BoundFinish::NoBoundDecision => match &mut state.eager {
                            Some(eager) => {
                                eager.finish(&pending_text, reason, state.now, context, output)?
                            }
                            None => None,
                        },
                    };
                    // Receipt admission has already won its source race. Never
                    // re-veto a successful confirmation or ordinary consume.
                    state
                        .idle
                        .process(&FrameData::UserStoppedSpeaking, state.now)?;
                    state.binding = ControllerBinding::Inactive;
                    let held = state.held_external_stop.take();
                    if !emit_speaking && !matches!(reason, TurnStopReason::SessionEnd) {
                        if let Some(mut held) = held {
                            if let Some(capability) = &confirmation {
                                held.frame.set_native_speculation(capability.clone())?;
                            }
                            output.push(held.frame, held.direction);
                        }
                    }
                    // Stop is sent before inference so the assistant half sees the
                    // complete speaking boundary before results can request a rerun.
                    if emit_speaking {
                        let mut boundary = Frame::new(FrameData::UserStoppedSpeaking);
                        if let Some(capability) = &confirmation {
                            boundary.set_native_speculation(capability.clone())?;
                        }
                        boundary
                            .metadata
                            .insert("user_context_follows".into(), false.into());
                        output.actions.push(AggregationAction::Broadcast(boundary));
                    }
                    if realtime {
                        state.interrupted_bot = false;
                        state.interim.clear();
                        if !matches!(reason, TurnStopReason::SessionEnd) {
                            output
                                .events
                                .push(AggregationEvent::RealtimeUserTurnStopped { reason });
                        }
                        continue;
                    }
                    let interrupted_bot = std::mem::take(&mut state.interrupted_bot);
                    let text =
                        Self::commit(state, context, run_llm && confirmation.is_none(), output)?;
                    if text.is_empty()
                        && confirmation.is_none()
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
        let text = state.aggregation.take()?;
        if let Some(eager) = &mut state.eager {
            eager.reset_transcript();
        }
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
    fn explicit_user_context(
        state: &mut UserState,
        context: &SharedContext,
        output: &mut AggregationOutput,
        intent: pipecat_core::RealtimeContextIntent,
        expected_lineage: Option<u64>,
        run_llm: bool,
    ) -> Result<(), ProcessorError> {
        if !state.aggregation.enabled()? {
            if run_llm {
                Self::user_context(state, context, output)?;
            }
            return Ok(());
        }
        let result = (|| {
            let snapshot = if run_llm {
                match state
                    .paired_assistant
                    .as_ref()
                    .and_then(std::sync::Weak::upgrade)
                {
                    Some(assistant) => {
                        assistant.prepare_user_context_at_lineage(expected_lineage)?
                    }
                    None => context.frame_data_at_lineage(expected_lineage)?,
                }
            } else {
                context.frame_data_at_lineage(expected_lineage)?
            };
            output.context_frame(snapshot, Direction::Downstream);
            let Some(AggregationAction::Push(context)) = output.actions.last_mut() else {
                return Err(ProcessorError::fatal("explicit user context is missing"));
            };
            state
                .context_causes
                .attach(&mut context.frame, intent, run_llm)
        })();
        if let Err(error) = result {
            state.context_cause_failed = true;
            return Err(ProcessorError::fatal(format!("realtime local history retained as-is but edit not delivered: {}; create a new pair and provider session from repaired context", error.message)));
        }
        Ok(())
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
            || (!state.aggregation.paired() && state.deadline()?.is_none())
        {
            return Ok(());
        }
        state.timer_running = true;
        drop(state);
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
                let notified = changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                let effects = Self::advance_state(&shared, &context, origin.elapsed())?;
                #[cfg(test)]
                if !effects.actions.is_empty() {
                    let aggregation = shared
                        .lock()
                        .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))?
                        .aggregation
                        .clone();
                    aggregation.pause_history_effects(false).await;
                }
                Self::notify_handlers(&handlers, &effects, &output).await?;
                effects.emit(&output).await?;
                let aggregation = shared
                    .lock()
                    .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))?
                    .aggregation
                    .clone();
                aggregation.resolve_history(&context)?;
                let controls_closed = shared
                    .lock()
                    .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))?
                    .input_controls_closed;
                if !controls_closed {
                    idle.schedule(&output)?;
                }
                let deadline = {
                    let state = shared
                        .lock()
                        .map_err(|_| ProcessorError::fatal("user aggregator lock poisoned"))?;
                    if state.timer_disabled || state.ended {
                        return Ok(());
                    }
                    state.deadline()?
                };
                tokio::select! {
                    _ = async {
                        match deadline.and_then(|deadline| origin.checked_add(deadline)) {
                            Some(deadline) => tokio::time::sleep_until(deadline).await,
                            None => std::future::pending::<()>().await,
                        }
                    } => {}
                    _ = &mut notified => {}
                }
            }
        }) {
            Ok(id) => {
                let cancel = {
                    let mut state = self.lock()?;
                    if state.timer_disabled || state.ended || !state.timer_running {
                        true
                    } else {
                        state.timer_id = Some(id);
                        false
                    }
                };
                if cancel {
                    ctx.cancel_task(id);
                }
                Ok(())
            }
            Err(error) => {
                self.lock()?.timer_running = false;
                Err(error)
            }
        }
    }
    pub(crate) fn schedule_idle_timer(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.lock()?.input_controls_closed {
            return Ok(());
        }
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
    async fn setup(
        &self,
        _config: &pipecat_core::StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let aggregation = {
            let state = self.lock()?;
            if state.ended {
                return Err(ProcessorError::fatal(
                    "ended user aggregator cannot be set up again",
                ));
            }
            state.aggregation.clone()
        };
        aggregation.setup_user(ctx)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let terminal = matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
        );
        let aggregation = self.lock()?.aggregation.clone();
        aggregation.bind_user(ctx)?;
        let processing = aggregation.history_processing(&frame, direction, ctx)?;
        if processing
            .as_ref()
            .is_some_and(|guard| guard.skip_processing())
        {
            processing
                .expect("non-processing disposition exists")
                .finish()?;
            aggregation.resolve_history(&self.context)?;
            return Ok(());
        }
        #[cfg(test)]
        if processing.is_some() {
            aggregation.pause_history_consumer().await;
        }
        if aggregation.history_marker(&frame, direction, true)? {
            {
                let mut state = self.lock()?;
                state.input_controls_closed = true;
                state.held_external_stop = None;
                state.controller.cleanup();
            }
            self.idle.stop(Some(ctx))?;
        }
        let output = self.process_at(frame, direction, self.origin.elapsed())?;
        #[cfg(test)]
        if processing.is_some() {
            aggregation.pause_history_staged().await;
        }
        let emitted = async {
            self.notify_inference_requests(&output, ctx).await?;
            output.emit(ctx).await
        }
        .await;
        let released = if terminal {
            aggregation.terminal_emitted()
        } else {
            Ok(())
        };
        if let Err(error) = emitted {
            if error.fatal {
                self.lock()?.context_cause_failed = true;
            }
            return Err(error);
        }
        released?;
        if let Some(processing) = processing {
            processing.finish()?;
        }
        let mut effects = AggregationOutput::default();
        {
            let state = self.lock()?;
            state
                .aggregation
                .service_history(&self.context, &mut effects)?;
        }
        effects.emit(ctx).await?;
        aggregation.resolve_history(&self.context)?;
        self.schedule_timer(ctx)
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.idle.stop(Some(ctx))?;
        let mut state = self.lock()?;
        state.cancel_eager(&mut AggregationOutput::default())?;
        state.timer_disabled = true;
        state.aggregation.stop_timer()?;
        let task = state.timer_id.take();
        state.timer_running = false;
        drop(state);
        if let Some(id) = task {
            ctx.cancel_task(id);
        }
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
        state.cancel_eager(&mut AggregationOutput::default())?;
        state.ended = true;
        state.binding = ControllerBinding::Inactive;
        state.timer_disabled = true;
        let aggregation = state.aggregation.clone();
        state.controller.cleanup();
        state.mute_strategies.cleanup();
        if let Some(eager) = &mut state.eager {
            eager.reset_turn();
        }
        state.interim.clear();
        drop(state);
        aggregation.close()?;
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
    Ok(())
}

#[cfg(test)]
mod realtime_lineage_tests {
    use super::*;
    use crate::{LLMContextAggregatorPair, RealtimeServiceMode};
    use pipecat_core::{ChatMessage, ChatRole, LocalMessageTransform, RealtimeContextIntent};
    use pipecat_services::Context;

    #[test]
    fn realtime_snapshot_lineage_no_run_and_transform_fail_before_seal_or_sequence() {
        for transform in [false, true] {
            for run_llm in [false, true] {
                let pair = LLMContextAggregatorPair::with_realtime_mode(
                    Context::default(),
                    Default::default(),
                    Default::default(),
                    RealtimeServiceMode::Enabled,
                )
                .unwrap();
                let user = pair.user();
                let shared = pair.context();
                let mut state = user.lock().unwrap();
                // Exercise the exact mutation/snapshot phase boundary without a
                // scheduling hook or holding context while acquiring assistant.
                let lineage = {
                    let mut context = shared.lock().unwrap();
                    if transform {
                        context
                            .transform(&LocalMessageTransform::new(|_| {
                                Ok(vec![ChatMessage::new(
                                    ChatRole::User,
                                    "original transformed",
                                )
                                .into()])
                            }))
                            .unwrap();
                    } else {
                        context.append_native(Message::text(Role::User, "original append"));
                    }
                    context.lineage
                };
                let producer = state.context_causes.clone();
                shared
                    .replace(Context::new(vec![Message::text(
                        Role::User,
                        "concurrent replacement",
                    )]))
                    .unwrap();
                let retained = shared.snapshot().unwrap();
                let mut output = AggregationOutput::default();
                let error = LLMUserAggregator::explicit_user_context(
                    &mut state,
                    &shared,
                    &mut output,
                    if transform {
                        RealtimeContextIntent::Replace
                    } else {
                        RealtimeContextIntent::Append { start: 0, end: 1 }
                    },
                    Some(lineage),
                    run_llm,
                )
                .unwrap_err();
                assert!(error.fatal);
                assert!(error.message.contains("lineage changed"));
                assert!(state.context_cause_failed);
                assert!(output.actions.is_empty());
                assert_eq!(shared.snapshot().unwrap(), retained);
                let mut probe = Frame::new(shared.frame_data().unwrap());
                producer
                    .attach(&mut probe, RealtimeContextIntent::Run, true)
                    .unwrap();
                assert_eq!(
                    probe
                        .native_realtime_context_cause()
                        .unwrap()
                        .unwrap()
                        .sequence(),
                    1
                );
                drop(state);
                assert!(
                    user.process_at(
                        Frame::new(FrameData::LlmRun),
                        Direction::Downstream,
                        Duration::ZERO
                    )
                    .unwrap_err()
                    .fatal
                );
                assert_eq!(shared.snapshot().unwrap(), retained);
            }
        }
    }

    #[test]
    fn realtime_snapshot_lineage_no_run_preserves_range_during_ordinary_append() {
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap();
        let user = pair.user();
        let shared = pair.context();
        let mut state = user.lock().unwrap();
        let lineage = {
            let mut context = shared.lock().unwrap();
            context.append_native(Message::text(Role::User, "explicit"));
            context.lineage
        };
        shared
            .append(Message::text(Role::Assistant, "ordinary concurrent append"))
            .unwrap();
        let mut output = AggregationOutput::default();
        LLMUserAggregator::explicit_user_context(
            &mut state,
            &shared,
            &mut output,
            RealtimeContextIntent::Append { start: 0, end: 1 },
            Some(lineage),
            false,
        )
        .unwrap();
        let AggregationAction::Push(frame) = &output.actions[0] else {
            panic!("context expected")
        };
        let cause = frame
            .frame
            .native_realtime_context_cause()
            .unwrap()
            .unwrap();
        assert_eq!(cause.sequence(), 1);
        assert_eq!(
            cause.intent(),
            RealtimeContextIntent::Append { start: 0, end: 1 }
        );
        assert!(!cause.run_llm());
        assert_eq!(shared.snapshot().unwrap().messages.len(), 2);
    }
}
