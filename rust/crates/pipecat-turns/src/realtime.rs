//! Shared realtime handoff state. Neither half locks the other through this state.
use crate::{AggregationAction, AggregationEvent, AggregationOutput, SharedContext, TextBuffer};
use pipecat_core::{Direction, Frame, FrameData, ProcessorContext, ProcessorError};
use pipecat_services::{Message, Role};
use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::time::Instant;

const MAX_PENDING_TRANSCRIPT_BYTES: usize = 1024 * 1024;

/// Realtime trailing writes are configured once for both halves of a pair.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum RealtimeServiceMode {
    /// Keep cascade behavior until a realtime service announces itself.
    #[default]
    Auto,
    Enabled,
    /// Never auto-enable, including after a service switch.
    Disabled,
}

pub(crate) struct State {
    enabled: bool,
    text: TextBuffer,
    latency: Duration,
    deadline: Option<Duration>,
    generation: u64,
    cause: Option<ProcessorContext>,
    effect_cause: Option<(u64, ProcessorContext)>,
    pub(crate) user_route: Option<ProcessorContext>,
    pub(crate) user_identity: Option<pipecat_core::ObservedProcessor>,
    pub(crate) history: Option<crate::user_history::HistoryState>,
    tool_progress: Option<(pipecat_core::NativeToolProgressRoute, u64, usize)>,
    used: bool,
    timers_closed: bool,
    closed: bool,
    resolves_start: bool,
    resolves_stop: bool,
    explicit_strategies: bool,
    interruptions: bool,
    model_wrapper: bool,
}
pub(crate) struct RealtimeState {
    mode: RealtimeServiceMode,
    paired: bool,
    eager: bool,
    state: Mutex<State>,
    pub(crate) changed: Arc<Notify>,
    pub(crate) origin: Instant,
}
impl RealtimeState {
    pub(crate) fn new(mode: RealtimeServiceMode, paired: bool, eager: bool) -> Self {
        Self {
            mode,
            paired,
            eager,
            state: Mutex::new(State {
                enabled: mode == RealtimeServiceMode::Enabled,
                text: TextBuffer::default(),
                latency: Duration::from_secs(1),
                deadline: None,
                generation: 0,
                cause: None,
                effect_cause: None,
                user_route: None,
                user_identity: None,
                history: None,
                tool_progress: None,
                used: false,
                timers_closed: false,
                closed: false,
                resolves_start: false,
                resolves_stop: false,
                explicit_strategies: false,
                interruptions: true,
                model_wrapper: false,
            }),
            changed: Arc::new(Notify::new()),
            origin: Instant::now(),
        }
    }
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, State>, ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("realtime aggregation lock poisoned"))
    }
    pub(crate) fn mark_used(&self) -> Result<(), ProcessorError> {
        self.lock()?.used = true;
        Ok(())
    }
    pub(crate) fn reserve_history(
        self: &Arc<Self>,
        context: &SharedContext,
    ) -> Result<crate::RealtimeUserHistoryEndpoint, ProcessorError> {
        let mut state = self.lock()?;
        if self.mode != RealtimeServiceMode::Enabled
            || !self.paired
            || self.eager
            || state.model_wrapper
            || state.used
            || state.closed
            || state.history.is_some()
            || state.tool_progress.is_some()
            || state.user_identity.is_some()
            || !state.text.text.is_empty()
        {
            return Err(ProcessorError::new(
                "history binding requires one unused explicitly Enabled standard non-eager pair",
            ));
        }
        let lineage = context.lock()?.lineage;
        let gate = Arc::new(Mutex::new(true));
        state.history = Some(crate::user_history::HistoryState::new(
            lineage,
            gate.clone(),
        )?);
        Ok(crate::RealtimeUserHistoryEndpoint {
            realtime: self.clone(),
            context: context.clone(),
            gate,
        })
    }
    pub(crate) fn bind_tool_progress(
        &self,
        context: &SharedContext,
        route: pipecat_core::NativeToolProgressRoute,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if self.mode != RealtimeServiceMode::Enabled
            || !self.paired
            || self.eager
            || state.model_wrapper
            || state.used
            || state.closed
            || state.history.is_some()
            || state.tool_progress.is_some()
            || state.user_identity.is_some()
            || !state.text.text.is_empty()
        {
            return Err(ProcessorError::new("tool progress requires one unused explicitly Enabled standard unsigned non-eager pair"));
        }
        let mut history = context.lock()?;
        if history.tool_progress.is_some() {
            return Err(ProcessorError::new(
                "shared context already has a tool progress binding",
            ));
        }
        let bound = crate::tool_progress::History::new(route.clone(), &history)?;
        history.tool_progress = Some(bound);
        state.tool_progress = Some((route, history.lineage, context.identity()));
        Ok(())
    }
    pub(crate) fn tool_progress_route(
        &self,
    ) -> Result<Option<pipecat_core::NativeToolProgressRoute>, ProcessorError> {
        Ok(self
            .lock()?
            .tool_progress
            .as_ref()
            .map(|(route, _, _)| route.clone()))
    }
    pub(crate) fn validate_tool_progress(
        &self,
        context: &SharedContext,
        frame: Option<&Frame>,
    ) -> Result<(), ProcessorError> {
        let binding = self.lock()?.tool_progress.clone();
        if let Some((_route, lineage, identity)) = binding {
            if let Some(frame) = frame {
                crate::tool_progress::validate_control(frame)?;
            }
            let history = context.lock()?;
            if history.lineage != lineage
                || context.identity() != identity
                || history.tool_progress.is_none()
            {
                return Err(ProcessorError::fatal(
                    "selected tool progress context was replaced",
                ));
            }
            history.preflight_tool_progress(&[])?;
        }
        Ok(())
    }
    pub(crate) fn setup_tool_progress(
        &self,
        context: &SharedContext,
        role: pipecat_core::NativeToolProgressRole,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.validate_tool_progress(context, None)?;
        if let Some(route) = self.tool_progress_route()? {
            route.register_role(role, ctx, Some(context.identity()))?;
        }
        Ok(())
    }
    pub(crate) fn history_bound(&self) -> Result<bool, ProcessorError> {
        Ok(self.lock()?.history.is_some())
    }
    pub(crate) fn setup_user(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let duplicate = {
            let mut state = self.lock()?;
            state.used = true;
            let duplicate = state.history.is_some() && state.user_identity.is_some();
            if let Some(history) = &mut state.history {
                history.require_open()?;
                if duplicate {
                    history.poison_setup("selected user setup cannot repeat");
                } else {
                    state.user_identity = Some(ctx.processor_identity());
                    state.user_route = Some(ctx.clone());
                }
            }
            duplicate
        };
        if duplicate {
            self.fail_history("selected user setup cannot repeat", true)?;
            return Err(ProcessorError::fatal("selected user setup cannot repeat"));
        }
        Ok(())
    }
    pub(crate) fn setup_assistant(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let duplicate = {
            let mut state = self.lock()?;
            state.used = true;
            if let Some(history) = &mut state.history {
                history.require_open()?;
                if history.assistant_setup.is_some() {
                    history.poison_setup("selected assistant setup cannot repeat");
                    true
                } else {
                    history.assistant_setup = Some(ctx.processor_identity());
                    false
                }
            } else {
                false
            }
        };
        if duplicate {
            self.fail_history("selected assistant setup cannot repeat", true)?;
            return Err(ProcessorError::fatal(
                "selected assistant setup cannot repeat",
            ));
        }
        Ok(())
    }
    pub(crate) fn service_history(
        self: &Arc<Self>,
        context: &SharedContext,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let ready = {
            let state = self.lock()?;
            match &state.history {
                Some(history)
                    if matches!(
                        history.phase,
                        crate::user_history::Phase::Open | crate::user_history::Phase::DrainingEnd
                    ) =>
                {
                    history.command_ready()?
                }
                _ => false,
            }
        };
        if ready {
            self.flush(context, None, output)?;
        }
        Ok(())
    }
    pub(crate) fn paired(&self) -> bool {
        self.paired
    }
    pub(crate) fn enabled(&self) -> Result<bool, ProcessorError> {
        Ok(self.lock()?.enabled)
    }
    pub(crate) fn observe(&self, data: &FrameData) -> Result<(), ProcessorError> {
        crate::user::validate_control(data)?;
        let mut state = self.lock()?;
        if state.closed {
            return Ok(());
        }
        self.validate_model_wrapper(&state, data)?;
        recommended_strategies(data, state.explicit_strategies, state.interruptions)?;
        if matches!(data, FrameData::LlmServiceMetadata(metadata) if metadata.is_realtime_service) {
            if !self.paired && self.mode != RealtimeServiceMode::Disabled {
                return Err(ProcessorError::new(
                    "realtime metadata requires LLMContextAggregatorPair",
                ));
            }
            if self.mode != RealtimeServiceMode::Disabled {
                if self.eager {
                    return Err(ProcessorError::new(
                        "realtime aggregation and eager speculation cannot share a pair",
                    ));
                }
                if state.text.text.len() > MAX_PENDING_TRANSCRIPT_BYTES {
                    return Err(ProcessorError::new(
                        "pending realtime transcript exceeds 1048576 bytes",
                    ));
                }
                state.enabled = true;
            }
        }
        if let FrameData::SttMetadata(metadata) = data {
            state.latency = Duration::from_secs_f64(metadata.ttfs_p99_latency);
        }
        Ok(())
    }
    pub(crate) fn bind_user(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        let identity = ctx.processor_identity();
        if state.history.is_some() {
            if state.user_identity.as_ref() != Some(&identity) {
                return Err(ProcessorError::fatal(
                    "selected user runtime does not match successful setup",
                ));
            }
            return Ok(());
        }
        if let Some(previous) = &state.user_identity {
            if previous != &identity {
                return Err(ProcessorError::fatal(
                    "an aggregator belongs to one pipeline session and cannot be restarted",
                ));
            }
        } else if state.closed {
            return Err(ProcessorError::fatal(
                "a closed aggregator cannot be started",
            ));
        }
        if !state.closed {
            state.user_identity = Some(identity);
            if self.paired {
                state.user_route = Some(ctx.clone());
            }
        }
        Ok(())
    }
    pub(crate) fn validate_assistant(
        &self,
        ctx: &ProcessorContext,
        data: &FrameData,
    ) -> Result<(), ProcessorError> {
        let state = self.lock()?;
        self.validate_model_wrapper(&state, data)?;
        let activating = self.mode != RealtimeServiceMode::Disabled
            && matches!(data, FrameData::LlmServiceMetadata(metadata) if metadata.is_realtime_service);
        if !state.enabled && !activating {
            return Ok(());
        }
        if let Some(user) = &state.user_identity {
            let assistant = ctx.processor_identity();
            if user.runtime_id != assistant.runtime_id || user.index >= assistant.index {
                return Err(ProcessorError::fatal(
                    "realtime pair requires user before assistant in the same pipeline",
                ));
            }
        } else {
            return Err(ProcessorError::fatal(
                "realtime assistant requires its paired user in the pipeline",
            ));
        }
        Ok(())
    }
    fn validate_model_wrapper(
        &self,
        state: &State,
        data: &FrameData,
    ) -> Result<(), ProcessorError> {
        if state.model_wrapper
            && self.mode != RealtimeServiceMode::Disabled
            && matches!(data, FrameData::LlmServiceMetadata(metadata) if metadata.is_realtime_service)
        {
            return Err(ProcessorError::new(
                "Smart Turn wrapper does not support realtime aggregation; use Disabled mode",
            ));
        }
        Ok(())
    }
    pub(crate) fn configure_model_wrapper(
        &self,
        controller: &mut crate::UserTurnController,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.enabled {
            return Err(ProcessorError::new(
                "Smart Turn wrapper does not support realtime aggregation; use Disabled mode",
            ));
        }
        // Configure before publishing policy ownership; a rejected wrapper must
        // leave both the caller's controller and shared activation state intact.
        controller.configure_model_gate()?;
        state.model_wrapper = true;
        state.explicit_strategies = true;
        Ok(())
    }
    pub(crate) fn strategy_policy(
        &self,
        explicit: bool,
        interruptions: bool,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.explicit_strategies = explicit;
        state.interruptions = interruptions;
        Ok(())
    }
    pub(crate) fn resolvers(&self, start: bool, stop: bool) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.resolves_start = start;
        state.resolves_stop = stop;
        Ok(())
    }
    pub(crate) fn resolves(&self, start: bool) -> Result<bool, ProcessorError> {
        let state = self.lock()?;
        Ok(if start {
            state.resolves_start
        } else {
            state.resolves_stop
        })
    }
    pub(crate) fn append(&self, text: &str, spaces: bool) -> Result<(), ProcessorError> {
        self.append_registered(text, spaces, None)
    }
    pub(crate) fn append_registered(
        &self,
        text: &str,
        spaces: bool,
        frame: Option<&Frame>,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.closed {
            return Ok(());
        }
        if state.enabled
            && state
                .text
                .text
                .len()
                .saturating_add(text.len())
                .saturating_add(usize::from(!state.text.text.is_empty()))
                > MAX_PENDING_TRANSCRIPT_BYTES
        {
            return Err(ProcessorError::new(
                "pending realtime transcript exceeds 1048576 bytes",
            ));
        }
        if let Some(history) = &mut state.history {
            let frame = frame.ok_or_else(|| {
                ProcessorError::fatal("selected append requires an actual registered final")
            })?;
            history.stage(frame)?;
        }
        state.text.append(text, spaces);
        Ok(())
    }
    pub(crate) fn text(&self) -> Result<String, ProcessorError> {
        Ok(self.lock()?.text.text.clone())
    }
    pub(crate) fn take(&self) -> Result<String, ProcessorError> {
        let mut state = self.lock()?;
        if state.history.is_some() && !state.text.text.trim().is_empty() {
            return Err(ProcessorError::fatal(
                "strategy reset would discard registered user history",
            ));
        }
        Ok(state.text.take())
    }
    pub(crate) fn deadline(&self) -> Result<Option<Duration>, ProcessorError> {
        Ok(self.lock()?.deadline)
    }
    pub(crate) fn handoff(
        &self,
        now: Duration,
        cause: Option<&ProcessorContext>,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if state.enabled && !state.closed && !state.timers_closed {
            state.generation = state
                .generation
                .checked_add(1)
                .ok_or_else(|| ProcessorError::fatal("realtime handoff generation exhausted"))?;
            state.deadline = Some(now.saturating_add(state.latency));
            state.cause = cause.cloned();
            state.effect_cause = None;
            self.changed.notify_one();
        }
        Ok(())
    }
    /// Commit before assistant content or on the user's timer. Context effects
    /// retain the user origin and the handoff's causal scope, never a context ID.
    pub(crate) fn flush(
        self: &Arc<Self>,
        context: &SharedContext,
        now: Option<Duration>,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        if !state.enabled || state.closed {
            return Ok(());
        }
        if let Some(now) = now {
            if state.timers_closed || state.deadline.is_none_or(|deadline| now < deadline) {
                return Ok(());
            }
        }
        let text = state.text.text.trim().to_owned();
        if text.is_empty() {
            state.deadline = None;
            state.cause = None;
            return Ok(());
        }
        let generation = state
            .generation
            .checked_add(1)
            .ok_or_else(|| ProcessorError::fatal("realtime handoff generation exhausted"))?;
        let mut selected_sequence = None;
        let terminal_only = state
            .history
            .as_ref()
            .is_some_and(|history| history.phase == crate::user_history::Phase::ClosingTerminal);
        let frame = if let Some(history) = &mut state.history {
            if history.phase == crate::user_history::Phase::Closed {
                return Err(ProcessorError::fatal("history session is closed"));
            }
            let through = history.staged_through()?;
            if through <= history.appended_through {
                return Err(ProcessorError::fatal(
                    "selected user text has no staged registered prefix",
                ));
            }
            let lineage = history.lineage;
            let (frame, ledger) = context.append_with_prepared_frame_at_lineage(
                Message::text(Role::User, text.clone()),
                Some(lineage),
                |data| {
                    let mut frame = Frame::new(data);
                    frame.interruptible = false;
                    frame.metadata.insert(
                        pipecat_core::REALTIME_CONTEXT_UPDATE_METADATA.into(),
                        true.into(),
                    );
                    let ledger = if terminal_only {
                        None
                    } else {
                        Some(history.prepare_ledger(&frame, through)?)
                    };
                    Ok((frame, ledger))
                },
            )?;
            history.appended_through = through;
            if let Some(ledger) = ledger {
                history.sequence = ledger.receipt.sequence();
                selected_sequence = Some(history.sequence);
                history.ledger.push_back(ledger);
            }
            frame
        } else {
            let mut frame =
                Frame::new(context.append_with_frame(Message::text(Role::User, text.clone()))?);
            frame.metadata.insert(
                pipecat_core::REALTIME_CONTEXT_UPDATE_METADATA.into(),
                true.into(),
            );
            frame
        };
        state.text.take();
        state.generation = generation;
        state.deadline = None;
        let cause = if now.is_some() {
            state.cause.take()
        } else {
            state.cause = None;
            None
        };
        let captured_cause = cause.is_some();
        state.effect_cause = cause.map(|cause| (generation, cause));
        output
            .events
            .push(AggregationEvent::UserTurnMessageAdded { text });
        if !terminal_only {
            output.actions.push(AggregationAction::RealtimeUserContext(
                RealtimeUserContext {
                    frame,
                    route: self.clone(),
                    captured_cause,
                    generation: state.generation,
                    selected_sequence,
                    context: context.clone(),
                },
            ));
        }
        self.changed.notify_one();
        Ok(())
    }
    pub(crate) fn stop_timer(&self) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.timers_closed = true;
        state.deadline = None;
        state.cause = None;
        state.effect_cause = None;
        let next = state.generation.checked_add(1);
        if let Some(next) = next {
            state.generation = next;
        } else {
            // Fail terminally rather than leaving a formerly captured action
            // valid after its raw causal fence has been revoked.
            state.closed = true;
            state.user_route = None;
        }
        self.changed.notify_one();
        next.map(|_| ())
            .ok_or_else(|| ProcessorError::fatal("realtime handoff generation exhausted"))
    }
    pub(crate) fn close(&self) -> Result<(), ProcessorError> {
        self.fail_history("user history cleanup closed", false)?;
        self.stop_timer()?;
        let mut state = self.lock()?;
        state.closed = true;
        state.text.take();
        state.user_route = None;
        Self::release_history_routes(&mut state);
        Ok(())
    }
    fn release_history_routes(state: &mut State) {
        if let Some(history) = &mut state.history {
            history.phase = crate::user_history::Phase::Closed;
            history.gate_setup = None;
            history.service_setup = None;
            history.registrations.clear();
            // Retain bounded weak snapshot identities so already-admitted
            // notifications can be consumed after terminal revocation. They
            // retain neither a runtime nor successful release authority.
        }
    }
    /// Stop deliberately skips processor cleanup in core. Drop owning runtime
    /// handles only after attempting the terminal's context effects. Admission
    /// failure remains an error; truthful committed history is not replayed.
    /// Retain identity (not Runtime) to validate downstream terminal frames and
    /// reject attempts to reuse this already-ended session in another runtime.
    pub(crate) fn terminal_emitted(&self) -> Result<(), ProcessorError> {
        self.fail_history("user history terminal completed", false)?;
        let mut state = self.lock()?;
        state.closed = true;
        state.timers_closed = true;
        state.deadline = None;
        state.cause = None;
        state.effect_cause = None;
        state.user_route = None;
        Self::release_history_routes(&mut state);
        self.changed.notify_one();
        Ok(())
    }
}

/// A context snapshot emitted from the paired user's downstream boundary.
/// `AggregationOutput::emit` resolves the route; deterministic callers may
/// inspect `frame`. It is never an upstream inference request.
#[derive(Clone)]
pub struct RealtimeUserContext {
    pub frame: Frame,
    route: Arc<RealtimeState>,
    captured_cause: bool,
    generation: u64,
    selected_sequence: Option<u64>,
    context: SharedContext,
}
impl fmt::Debug for RealtimeUserContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RealtimeUserContext")
            .field("frame", &self.frame)
            .field("generation", &self.generation)
            .finish_non_exhaustive()
    }
}
impl PartialEq for RealtimeUserContext {
    fn eq(&self, other: &Self) -> bool {
        self.frame == other.frame
            && self.generation == other.generation
            && Arc::ptr_eq(&self.route, &other.route)
    }
}
pub(crate) enum RealtimeEffectAdmission {
    Admitted,
    Closed,
    Ordinary,
}
impl RealtimeUserContext {
    #[cfg(test)]
    pub(crate) async fn pause_history_effect(&self) {
        if self.selected_sequence.is_some() {
            self.route.pause_history_effects(true).await;
        }
    }
    pub(crate) fn emit(
        self,
        current: &ProcessorContext,
    ) -> Result<RealtimeEffectAdmission, ProcessorError> {
        if let Some(sequence) = self.selected_sequence {
            let (route, gate) = {
                let state = self.route.lock()?;
                let history = state
                    .history
                    .as_ref()
                    .ok_or_else(|| ProcessorError::fatal("history effect lost its binding"))?;
                history.require_healthy()?;
                if matches!(
                    history.phase,
                    crate::user_history::Phase::ClosingTerminal
                        | crate::user_history::Phase::Closed
                ) {
                    return Ok(RealtimeEffectAdmission::Closed);
                }
                history.require_open()?;
                if self.context.lock()?.lineage != history.lineage {
                    return Err(ProcessorError::fatal(
                        "history context lineage changed before effect admission",
                    ));
                }
                let record = history
                    .ledger
                    .iter()
                    .find(|record| record.receipt.sequence() == sequence)
                    .ok_or_else(|| {
                        ProcessorError::fatal("history effect has no actual commit ledger")
                    })?;
                if !record.seal.matches(&self.frame, Direction::Downstream)? {
                    return Err(ProcessorError::fatal("history effect seal mismatch"));
                }
                (
                    state.user_route.clone().ok_or_else(|| {
                        ProcessorError::fatal("history user setup route is unavailable")
                    })?,
                    history.gate.clone(),
                )
            };
            let admitted = {
                let open = gate
                    .lock()
                    .map_err(|_| ProcessorError::fatal("history release gate poisoned"))?;
                if !*open {
                    return Ok(RealtimeEffectAdmission::Closed);
                } else {
                    route.try_push(self.frame, Direction::Downstream)
                }
            };
            if let Err(error) = admitted {
                self.route.fail_history(
                    "CommittedButEffectsFailed: actual user history is retained",
                    true,
                )?;
                return Err(ProcessorError::fatal(format!(
                    "CommittedButEffectsFailed: {}",
                    error.message
                )));
            }
            {
                let mut state = self.route.lock()?;
                let history = state
                    .history
                    .as_mut()
                    .ok_or_else(|| ProcessorError::fatal("history effect binding disappeared"))?;
                if let Some(record) = history
                    .ledger
                    .iter_mut()
                    .find(|record| record.receipt.sequence() == sequence)
                {
                    record.admitted = true;
                }
            }
            self.route.resolve_history(&self.context)?;
            return Ok(RealtimeEffectAdmission::Admitted);
        }
        let state = self.route.lock()?;
        if state.closed || state.generation != self.generation {
            return Ok(RealtimeEffectAdmission::Ordinary);
        }
        let route = state.user_route.as_ref().ok_or_else(|| {
            ProcessorError::new("realtime context emission requires the paired user runtime")
        })?;
        let source = if self.captured_cause {
            let Some((generation, cause)) = &state.effect_cause else {
                return Ok(RealtimeEffectAdmission::Ordinary);
            };
            if *generation != self.generation {
                return Ok(RealtimeEffectAdmission::Ordinary);
            }
            cause
        } else {
            current
        };
        route.try_push_from_invocation(self.frame, Direction::Downstream, source)?;
        Ok(RealtimeEffectAdmission::Ordinary)
    }
}

type RecommendedStrategies = (
    Option<crate::UserTurnStartStrategies>,
    Option<crate::UserTurnStopStrategies>,
);

pub(crate) fn recommended_strategies(
    data: &FrameData,
    explicit: bool,
    interruptions: bool,
) -> Result<Option<RecommendedStrategies>, ProcessorError> {
    use pipecat_core::UserTurnStrategy as Descriptor;
    let strategies = match data {
        FrameData::ServiceMetadata(metadata) => &metadata.user_turn_strategies,
        FrameData::SttMetadata(metadata) => &metadata.base.user_turn_strategies,
        FrameData::LlmServiceMetadata(metadata) => &metadata.base.user_turn_strategies,
        _ => return Ok(None),
    };
    if explicit {
        return Ok(None);
    }
    let Some(strategies) = strategies else {
        return Ok(None);
    };
    if strategies
        .start
        .as_ref()
        .is_some_and(|list| list.len() > 64)
        || strategies.stop.as_ref().is_some_and(|list| list.len() > 64)
    {
        return Err(ProcessorError::new(
            "at most 64 recommended start/stop strategies are supported",
        ));
    }
    let start = strategies.start.as_ref().map(|descriptors| {
        descriptors.iter().map(|descriptor| match descriptor {
            Descriptor::Vad { enable_interruptions } => Ok(crate::UserTurnStartStrategy::vad().with_interruptions(*enable_interruptions && interruptions)),
            Descriptor::External { enable_interruptions } => Ok(crate::UserTurnStartStrategy::external().with_interruptions(*enable_interruptions && interruptions)),
            _ => Err(ProcessorError::new("unsupported service-recommended start strategy; configure native strategies explicitly")),
        }).collect::<Result<Vec<_>, _>>().and_then(crate::UserTurnStartStrategies::new)
    }).transpose()?;
    let stop = strategies.stop.as_ref().map(|descriptors| {
        descriptors.iter().map(|descriptor| match descriptor {
            Descriptor::External { .. } => Ok(crate::UserTurnStopStrategy::external(Duration::from_millis(500), true)),
            Descriptor::SpeechTimeout { timeout_seconds } if timeout_seconds.is_finite() && (0.0..=86_400.0).contains(timeout_seconds) => Ok(crate::UserTurnStopStrategy::speech_timeout(Duration::from_secs_f64(*timeout_seconds), true)),
            _ => Err(ProcessorError::new("unsupported service-recommended stop strategy; configure native strategies explicitly")),
        }).collect::<Result<Vec<_>, _>>().and_then(crate::UserTurnStopStrategies::new)
    }).transpose()?;
    Ok(Some((start, stop)))
}
#[cfg(test)]
mod review_generation_tests {
    use super::*;

    #[test]
    fn exhausted_effect_generation_preserves_pending_text_and_context() {
        let realtime = Arc::new(RealtimeState::new(
            RealtimeServiceMode::Enabled,
            true,
            false,
        ));
        realtime.append("accepted", false).unwrap();
        {
            let mut state = realtime.lock().unwrap();
            state.generation = u64::MAX;
            state.deadline = Some(Duration::from_secs(1));
        }
        let context = SharedContext::default();
        let mut output = AggregationOutput::default();
        let error = realtime
            .flush(&context, Some(Duration::from_secs(1)), &mut output)
            .unwrap_err();
        assert!(error.fatal);
        assert!(error.message.contains("generation exhausted"));
        assert!(context.snapshot().unwrap().messages.is_empty());
        assert_eq!(realtime.text().unwrap(), "accepted");
        assert_eq!(realtime.deadline().unwrap(), Some(Duration::from_secs(1)));
        assert!(output.actions.is_empty());
        assert!(output.events.is_empty());
    }

    #[test]
    fn exhausted_stop_generation_closes_instead_of_revalidating_revoked_effects() {
        let realtime = RealtimeState::new(RealtimeServiceMode::Enabled, true, false);
        realtime.append("accepted", false).unwrap();
        realtime.lock().unwrap().generation = u64::MAX;
        assert!(realtime.stop_timer().unwrap_err().fatal);
        let state = realtime.lock().unwrap();
        assert!(state.closed);
        assert!(state.timers_closed);
        assert!(state.cause.is_none());
        assert!(state.effect_cause.is_none());
        assert!(state.user_route.is_none());
        assert_eq!(state.text.text, "accepted");
    }
}
