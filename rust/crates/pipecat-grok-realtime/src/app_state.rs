//! Private, bounded application provenance and causal drain bookkeeping.
//!
//! These records never manufacture native outcomes or inference authority.
//! Missing observations keep the frontier closed to successful completion.
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameObservationToken, FrameProcessor, FunctionCall,
    LocalFlushEvent, LocalFrameTag, PipelineFlushFrame, ProcessorContext, ProcessorError,
    StartFrame,
};
use pipecat_processors::tools::{RegisteredTool, ToolExecutor};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::{watch, Mutex as AsyncMutex};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ActorCheckpoint {
    pub revision: u64,
    pub emitted: u64,
    pub idle: bool,
}
/// Bounded local failure classification. No provider or callback payload is retained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GrokAppFailureStage {
    Receive,
    Mapping,
    OutputSetup,
    OutputProcess,
    OutputPrepareEnd,
    OutputCleanup,
}
/// Observed causal blockers, not a replacement for either native aggregator.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct GrokAppDrainGates {
    pub user: bool,
    pub awaiting: bool,
    pub bot: bool,
    pub llm: bool,
    pub active: bool,
    pub pending_contexts: usize,
    pub pending_media: usize,
    pub pending_cuts: usize,
}
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct AppSnapshot {
    pub emitted: u64,
    pub mapped: u64,
    pub speech_responses: u64,
    pub mapped_calls: usize,
    pub settled_calls: usize,
    pub pending_frames: usize,
    pub pending_invocations: usize,
    pub failed: bool,
    pub cancelled: bool,
    pub first_failure_stage: Option<GrokAppFailureStage>,
    pub cleanup_output_failed: bool,
    pub drain_gates: GrokAppDrainGates,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AggregatorSide {
    User,
    Assistant,
}
#[derive(Clone, Copy)]
enum MarkerKind {
    Begin(u64, AggregatorSide),
    End(u64, AggregatorSide),
    CutNormal(u64),
    CutSystem(u64),
    InputUser(u64),
    InputUserSystem(u64),
    InputProvider(u64),
    InputProviderSystem(u64),
    VadUser(u64),
    VadProvider(u64),
}
struct Marker {
    frame_id: u64,
    kind: MarkerKind,
}
struct Invocation {
    source: Option<ProcessorContext>,
    finished: bool,
}
struct FrameDebt {
    source: ProcessorContext,
    epoch: u64,
    frame_id: u64,
    sequence: u64,
}
struct UnenteredCall {
    epoch: u64,
    source: ProcessorContext,
}
struct InputAdmission {
    frame_id: u64,
    data: FrameData,
    direction: Direction,
    charge: usize,
    visits: u8,
}
struct CallDebt {
    call: FunctionCall,
    declaration_token: FrameObservationToken,
    source: ProcessorContext,
    epoch: u64,
    delegated: bool,
    callback_checked: bool,
    wire: bool,
    assistant: bool,
    assistant_declared: bool,
    outcome: Option<u64>,
    discarded: bool,
}
#[derive(Default)]
struct Gates {
    user: bool,
    awaiting: bool,
    bot: bool,
    llm: bool,
    active: bool,
}
struct Cut {
    normal: bool,
    system: bool,
}
struct HistoryBudget {
    bytes: usize,
    messages: usize,
    output: usize,
    max_bytes: usize,
    max_messages: usize,
    max_output: usize,
    max_argument: usize,
    max_audio_chunk: usize,
    max_text: usize,
    max_input_admission_bytes: usize,
}
#[derive(Default)]
struct Ledger {
    revision: u64,
    emitted: u64,
    mapped: u64,
    next: u64,
    speech_responses: u64,
    retired_snapshot: Option<AppSnapshot>,
    first_failure_stage: Option<GrokAppFailureStage>,
    cleanup_output_failed: bool,
    cancelled: bool,
    failed: bool,
    closing: bool,
    provider_epoch: Option<u64>,
    provider_health: Option<(
        pipecat_core::ProcessorHealth,
        pipecat_core::ProcessorHealthGeneration,
    )>,
    scopes: BTreeMap<u64, ProcessorContext>,
    session: Option<Arc<crate::client::SessionCompletion>>,
    frames: HashMap<FrameObservationToken, FrameDebt>,
    media: HashMap<FrameObservationToken, u64>,
    history: Option<HistoryBudget>,
    calls: BTreeMap<String, CallDebt>,
    unentered_calls: BTreeMap<String, UnenteredCall>,
    inputs: HashMap<FrameObservationToken, InputAdmission>,
    input_admission_bytes: usize,
    markers: HashMap<FrameObservationToken, Marker>,
    invocations: BTreeMap<u64, Invocation>,
    context_sources: HashMap<FrameObservationToken, ProcessorContext>,
    cuts: BTreeMap<u64, Cut>,
    input_cuts: BTreeMap<u64, LocalFlushEvent>,
    gates: Gates,
}
pub(crate) struct AppState {
    ledger: Mutex<Ledger>,
    changed: watch::Sender<u64>,
    marker_tag: LocalFrameTag,
    mapped_tag: LocalFrameTag,
    input_tag: LocalFrameTag,
    max_records: usize,
    max_value_bytes: usize,
    #[cfg(test)]
    callback_gate: Mutex<Option<Arc<CallbackGate>>>,
    #[cfg(test)]
    test_hooks: Mutex<Option<Arc<crate::app_tests::TestHooks>>>,
}
impl std::fmt::Debug for AppState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AppState")
            .field("snapshot", &self.snapshot())
            .finish_non_exhaustive()
    }
}
impl AppState {
    pub(crate) fn new(max_records: usize, max_value_bytes: usize) -> Arc<Self> {
        Arc::new(Self {
            ledger: Mutex::new(Ledger::default()),
            changed: watch::channel(0).0,
            marker_tag: LocalFrameTag::new(),
            mapped_tag: LocalFrameTag::new(),
            input_tag: LocalFrameTag::new(),
            max_records,
            max_value_bytes,
            #[cfg(test)]
            callback_gate: Mutex::new(None),
            #[cfg(test)]
            test_hooks: Mutex::new(None),
        })
    }
    #[cfg(test)]
    pub(crate) fn install_test_hooks(&self, hooks: Arc<crate::app_tests::TestHooks>) {
        *self.test_hooks.lock().unwrap() = Some(hooks);
    }
    #[cfg(test)]
    pub(crate) fn test_hooks(&self) -> Option<Arc<crate::app_tests::TestHooks>> {
        self.test_hooks.lock().unwrap().clone()
    }
    fn source_current(source: &ProcessorContext) -> bool {
        source.is_usable()
            && source
                .invocation_fence()
                .is_ok_and(|fence| fence.is_current())
    }
    fn lock(&self) -> MutexGuard<'_, Ledger> {
        self.ledger.lock().unwrap_or_else(|error| {
            let mut state = error.into_inner();
            state.failed = true;
            state
        })
    }
    fn bump(&self, state: &mut Ledger) {
        match state.revision.checked_add(1) {
            Some(value) => {
                state.revision = value;
                self.changed.send_replace(value);
            }
            None => {
                state.failed = true;
                self.changed.send_replace(u64::MAX);
            }
        }
    }
    fn next(&self, state: &mut Ledger) -> Result<u64, ProcessorError> {
        state.next = state
            .next
            .checked_add(1)
            .ok_or_else(|| ProcessorError::fatal("application identity exhausted"))?;
        Ok(state.next)
    }
    fn available(&self, state: &Ledger) -> Result<(), ProcessorError> {
        if state.cancelled || state.failed || state.closing {
            return Err(ProcessorError::fatal("application admission closed"));
        }
        if self.max_records == 0
            || state
                .frames
                .len()
                .saturating_add(state.media.len())
                .saturating_add(state.calls.len())
                .saturating_add(state.unentered_calls.len())
                .saturating_add(state.inputs.len())
                .saturating_add(state.invocations.len())
                .saturating_add(state.markers.len())
                .saturating_add(state.context_sources.len())
                .saturating_add(state.cuts.len())
                .saturating_add(state.input_cuts.len())
                >= self.max_records
        {
            return Err(ProcessorError::fatal(
                "application provenance capacity exceeded",
            ));
        }
        Ok(())
    }
    pub(crate) fn subscribe(&self) -> watch::Receiver<u64> {
        self.changed.subscribe()
    }
    pub(crate) async fn cancelled(&self) {
        let mut changed = self.subscribe();
        loop {
            if self.lock().cancelled {
                return;
            }
            if changed.changed().await.is_err() {
                return;
            }
        }
    }
    pub(crate) fn cancel(&self) {
        let mut state = self.lock();
        state.cancelled = true;
        self.bump(&mut state);
    }
    pub(crate) fn fail(&self) {
        let mut state = self.lock();
        state.failed = true;
        self.bump(&mut state);
    }
    /// First operational cause wins; later cleanup records remain separate.
    pub(crate) fn note_failure_stage(&self, stage: GrokAppFailureStage) {
        let mut state = self.lock();
        state.first_failure_stage.get_or_insert(stage);
        state.failed = true;
        self.bump(&mut state);
    }
    pub(crate) fn note_cleanup_failure(&self, stage: GrokAppFailureStage) {
        let mut state = self.lock();
        if stage == GrokAppFailureStage::OutputCleanup {
            state.cleanup_output_failed = true;
        }
        state.failed = true;
        self.bump(&mut state);
    }
    pub(crate) fn retain_session(&self, session: Arc<crate::client::SessionCompletion>) {
        let mut state = self.lock();
        if state.session.is_some() {
            state.failed = true;
        } else {
            state.session = Some(session);
        }
        self.bump(&mut state);
    }
    pub(crate) fn session(&self) -> Option<Arc<crate::client::SessionCompletion>> {
        self.lock().session.clone()
    }
    pub(crate) fn bind_provider_health(
        &self,
        health: pipecat_core::ProcessorHealth,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        if state.provider_health.is_some() || !health.is_usable() {
            return Err(ProcessorError::fatal(
                "application provider health binding rejected",
            ));
        }
        let generation = health.generation();
        state.provider_health = Some((health, generation));
        self.bump(&mut state);
        Ok(())
    }
    fn provider_current(state: &Ledger) -> bool {
        state
            .provider_health
            .as_ref()
            .is_some_and(|(health, generation)| {
                generation.snapshot.is_usable && health.generation() == *generation
            })
    }
    pub(crate) fn register_provider_scope(&self, epoch: u64, source: ProcessorContext) {
        let mut state = self.lock();
        if state.provider_epoch.is_some_and(|old| epoch <= old)
            || state.scopes.len() >= self.max_records
        {
            state.failed = true;
        } else {
            state.provider_epoch = Some(epoch);
            state.scopes.insert(epoch, source);
        }
        self.bump(&mut state);
    }
    pub(crate) fn provider_scope(&self, epoch: u64) -> Option<ProcessorContext> {
        self.lock().scopes.get(&epoch).cloned()
    }
    pub(crate) fn note_actor_change(&self) {
        let mut state = self.lock();
        self.bump(&mut state);
    }
    pub(crate) fn actor_revision(&self) -> u64 {
        self.lock().revision
    }
    pub(crate) fn emitted(&self) -> u64 {
        self.lock().emitted
    }
    pub(crate) fn note_emitted(&self) -> Result<u64, ProcessorError> {
        let mut state = self.lock();
        self.available(&state)?;
        state.emitted = state
            .emitted
            .checked_add(1)
            .ok_or_else(|| ProcessorError::fatal("application event identity exhausted"))?;
        self.bump(&mut state);
        Ok(state.emitted)
    }
    pub(crate) fn mapped_through(&self, sequence: u64) {
        let mut state = self.lock();
        if sequence > state.emitted || sequence < state.mapped {
            state.failed = true;
        } else {
            state.mapped = sequence;
        }
        self.bump(&mut state);
    }
    pub(crate) async fn wait_mapped(&self, sequence: u64) -> Result<(), ProcessorError> {
        let mut changed = self.subscribe();
        loop {
            {
                let state = self.lock();
                if state.failed || state.cancelled {
                    return Err(ProcessorError::fatal(
                        "application mapping did not complete",
                    ));
                }
                if state.mapped >= sequence {
                    return Ok(());
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| ProcessorError::fatal("application mapping observer closed"))?;
        }
    }
    /// Lock order is ledger -> original runtime admission, matching stale-debt
    /// reconciliation. The fence closure only mutates the already-held ledger;
    /// notify observers after releasing runtime admission, never route or spawn.
    pub(crate) fn note_speech_complete_fenced(
        &self,
        fence: &pipecat_core::InvocationFence,
    ) -> bool {
        let mut state = self.lock();
        if state.failed || state.cancelled || state.closing {
            return false;
        }
        let accepted = fence
            .with_current(|| match state.speech_responses.checked_add(1) {
                Some(count) => state.speech_responses = count,
                None => state.failed = true,
            })
            .is_some();
        if accepted {
            self.bump(&mut state);
        }
        accepted
    }
    pub(crate) fn snapshot(&self) -> AppSnapshot {
        let state = self.lock();
        if let Some(snapshot) = state.retired_snapshot {
            return snapshot;
        }
        AppSnapshot {
            emitted: state.emitted,
            mapped: state.mapped,
            speech_responses: state.speech_responses,
            mapped_calls: state.calls.len(),
            settled_calls: state
                .calls
                .values()
                .filter(|call| call.outcome.is_some() && call.wire && call.assistant)
                .count(),
            pending_frames: state.frames.len().saturating_add(state.media.len()),
            pending_invocations: state.invocations.len(),
            failed: state.failed,
            cancelled: state.cancelled,
            first_failure_stage: state.first_failure_stage,
            cleanup_output_failed: state.cleanup_output_failed,
            drain_gates: GrokAppDrainGates {
                user: state.gates.user,
                awaiting: state.gates.awaiting,
                bot: state.gates.bot,
                llm: state.gates.llm,
                active: state.gates.active,
                pending_contexts: state.context_sources.len(),
                pending_media: state.media.len(),
                pending_cuts: state.cuts.len().saturating_add(state.input_cuts.len()),
            },
        }
    }
    /// After the actual pipeline and session owners have joined, release strong
    /// runtime provenance while retaining the final bounded diagnostic snapshot.
    /// This never claims that clearing a record has completed unfinished work.
    pub(crate) fn release_runtime_contexts(&self) {
        let snapshot = self.snapshot();
        let mut state = self.lock();
        state.retired_snapshot.get_or_insert(snapshot);
        state.closing = true;
        state.scopes.clear();
        state.frames.clear();
        state.media.clear();
        state.calls.clear();
        state.unentered_calls.clear();
        state.inputs.clear();
        state.input_admission_bytes = 0;
        state.invocations.clear();
        state.context_sources.clear();
        state.markers.clear();
        state.cuts.clear();
        state.input_cuts.clear();
        state.session.take();
        self.bump(&mut state);
    }
    fn clear(state: &Ledger) -> bool {
        !state.failed
            && !state.cancelled
            && !state.closing
            && Self::provider_current(state)
            && state.mapped == state.emitted
            && state.frames.is_empty()
            && state.media.is_empty()
            && state.invocations.is_empty()
            && state.markers.is_empty()
            && state.inputs.is_empty()
            && state.context_sources.is_empty()
            && state.cuts.is_empty()
            && state.input_cuts.is_empty()
            && state.calls.values().all(|call| {
                call.discarded
                    || (call.outcome.is_some()
                        && call.wire
                        && call.assistant
                        && call.assistant_declared)
            })
            && !state.gates.user
            && !state.gates.awaiting
            && !state.gates.bot
            && !state.gates.llm
            && !state.gates.active
    }
    pub(crate) fn frontier_clear(&self) -> bool {
        let mut state = self.lock();
        self.sweep_inputs(&mut state);
        Self::clear(&state)
    }
    /// The actor must check its own idle/deferred/write state before this call.
    /// The closure may only set actor-local finishing state: no I/O or routing.
    pub(crate) fn try_commit_close(
        &self,
        revision: u64,
        emitted: u64,
        commit: impl FnOnce(),
    ) -> bool {
        let mut state = self.lock();
        self.sweep_inputs(&mut state);
        if state.revision != revision || state.emitted != emitted || !Self::clear(&state) {
            return false;
        }
        state.closing = true;
        commit();
        self.bump(&mut state);
        true
    }
    pub(crate) fn register_mapped_frame(
        &self,
        event_sequence: u64,
        frame: &mut Frame,
        source: ProcessorContext,
        epoch: u64,
    ) -> Result<(), ProcessorError> {
        self.mapped_tag.mark(frame)?;
        let mut state = self.lock();
        self.available(&state)?;
        let token = frame.observation_token();
        if event_sequence == 0 || event_sequence > state.emitted {
            return Err(ProcessorError::fatal(
                "unreserved application event identity",
            ));
        }
        if let FrameData::TtsAudio(audio)
        | FrameData::OutputAudio(audio)
        | FrameData::SpeechOutputAudio(audio) = &frame.data
        {
            let history = state.history.as_mut().ok_or_else(|| {
                ProcessorError::fatal("application history limits not configured")
            })?;
            let total = history
                .output
                .checked_add(audio.audio.len())
                .ok_or_else(|| ProcessorError::fatal("application output capacity exceeded"))?;
            if total > history.max_output {
                return Err(ProcessorError::fatal(
                    "application output capacity exceeded",
                ));
            }
            history.output = total;
        }
        if state.frames.contains_key(&token) {
            return Err(ProcessorError::fatal(
                "duplicate application frame identity",
            ));
        }
        if let FrameData::FunctionCallsStarted(calls) = &frame.data {
            let mut unique = HashSet::new();
            for call in calls {
                if !unique.insert(call.tool_call_id.as_str())
                    || state.calls.contains_key(&call.tool_call_id)
                    || state.calls.len().saturating_add(calls.len()) > self.max_records
                {
                    return Err(ProcessorError::fatal(
                        "duplicate or excessive application tool identity",
                    ));
                }
            }
            for call in calls {
                if let Some(record) = state.unentered_calls.get(&call.tool_call_id) {
                    if record.epoch != epoch || !record.source.same_causal_scope(&source) {
                        return Err(ProcessorError::fatal(
                            "late declaration source conflicts with no-entry disposition",
                        ));
                    }
                }
            }
            for call in calls {
                let discarded = state.unentered_calls.contains_key(&call.tool_call_id);
                state.calls.insert(
                    call.tool_call_id.clone(),
                    CallDebt {
                        call: call.clone(),
                        declaration_token: token.clone(),
                        source: source.clone(),
                        epoch,
                        delegated: false,
                        callback_checked: false,
                        wire: false,
                        assistant: false,
                        assistant_declared: false,
                        outcome: None,
                        discarded,
                    },
                );
            }
        }
        let discarded = matches!(&frame.data, FrameData::FunctionCallsStarted(calls) if !calls.is_empty() && calls.iter().all(|call| state.calls.get(&call.tool_call_id).is_some_and(|record| record.discarded)));
        if !discarded {
            state.frames.insert(
                token,
                FrameDebt {
                    source,
                    epoch,
                    frame_id: frame.id,
                    sequence: event_sequence,
                },
            );
        }
        self.bump(&mut state);
        Ok(())
    }
    /// Configure before setup. Charges are conservative cumulative reservations,
    /// not snapshots of mutable standard-aggregator history.
    pub(crate) fn configure_history_limits(
        &self,
        limits: &crate::app_config::GrokAppLimits,
        seed_bytes: usize,
        seed_messages: usize,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        if state.history.is_some()
            || seed_bytes > limits.max_history_bytes
            || seed_messages > limits.max_history_messages
        {
            return Err(ProcessorError::fatal("application history budget rejected"));
        }
        state.history = Some(HistoryBudget {
            bytes: seed_bytes,
            messages: seed_messages,
            output: 0,
            max_bytes: limits.max_history_bytes,
            max_messages: limits.max_history_messages,
            max_output: limits.max_output_bytes,
            max_argument: limits.max_tool_argument_bytes,
            max_audio_chunk: limits.max_audio_chunk_bytes,
            max_text: limits.max_text_bytes,
            max_input_admission_bytes: limits.max_retained_bytes,
        });
        self.bump(&mut state);
        Ok(())
    }
    /// A definitely unqueued frame has no downstream obligation. Never use this
    /// to erase a declaration which has entered the actual executor.
    fn reject_user_carriers(frame: &Frame, direction: Direction) -> Result<(), ProcessorError> {
        // Validate every capability before dispatch on mutable public data. A
        // relabeled receipt/context must fail here before standard user effects.
        let authority = (|| -> Result<bool, ProcessorError> {
            Ok(frame.native_tool_batch()?.is_some()
                || frame.native_tool_outcome()?.is_some()
                || frame.native_realtime_context_cause()?.is_some()
                || frame.native_tool_progress_admission()?.is_some()
                || frame.native_tool_progress()?.is_some()
                || frame.native_speculation().is_some()
                || frame.native_eager_source().is_some()
                || frame
                    .native_llm_continuation()
                    .map_err(|_| ProcessorError::fatal("invalid application continuation carrier"))?
                    .is_some()
                || frame.native_user_image_constraint(direction)?.is_some()
                || frame.native_user_image_context(direction)?.is_some())
        })()
        .map_err(|_| ProcessorError::fatal("invalid application user capability carrier"))?;
        if authority {
            return Err(ProcessorError::fatal(
                "native authority is not application user input",
            ));
        }
        Ok(())
    }
    fn plain_input_headers(frame: &Frame) -> bool {
        frame.metadata.is_empty()
            && frame.pts.is_none()
            && frame.transport_source.is_none()
            && frame.transport_destination.is_none()
            && frame.broadcast_sibling_id.is_none()
    }
    fn sweep_inputs(&self, state: &mut Ledger) {
        let before = state.inputs.len();
        let mut released = 0usize;
        state.inputs.retain(|token, admission| {
            if token.is_alive() {
                true
            } else {
                released = released.saturating_add(admission.charge);
                false
            }
        });
        state.input_admission_bytes = state.input_admission_bytes.saturating_sub(released);
        if before != state.inputs.len() {
            self.bump(state);
        }
    }
    /// Private typed admission binds exact payload as well as opaque identity.
    /// The additional compact payload copies have their own aggregate live-byte
    /// cap (max_retained_bytes), charged before cloning. No raw sender escapes.
    pub(crate) fn admit_input(&self, mut frame: Frame) -> Result<Frame, ProcessorError> {
        Self::reject_user_carriers(&frame, Direction::Downstream)?;
        if !Self::plain_input_headers(&frame) {
            return Err(ProcessorError::fatal("application input headers rejected"));
        }
        let mut state = self.lock();
        self.sweep_inputs(&mut state);
        self.available(&state)?;
        let history = state
            .history
            .as_ref()
            .ok_or_else(|| ProcessorError::fatal("application input limits not configured"))?;
        let payload = match &frame.data {
            FrameData::InputAudio(audio)
                if audio.sample_rate == 24_000
                    && audio.num_channels == 1
                    && audio.context_id.is_none()
                    && !audio.audio.is_empty()
                    && audio.audio.len().is_multiple_of(2)
                    && audio.audio.len() <= history.max_audio_chunk =>
            {
                audio.audio.len()
            }
            FrameData::LlmMessagesAppend(messages)
                if (messages.messages.is_empty() && !messages.run_llm)
                    || (messages.messages.len() == 1
                        && messages.messages.iter().all(|message| {
                            message.role == pipecat_core::ChatRole::User
                                && !message.content.is_empty()
                                && message.content.len() <= history.max_text
                                && message.name.is_none()
                                && message.tool_call_id.is_none()
                                && message.tool_calls.is_empty()
                        })) =>
            {
                encoded_len(messages, history.max_input_admission_bytes)?
            }
            FrameData::LlmRun | FrameData::Interruption => 0,
            FrameData::FlushDetailed(flush)
                if flush.event.is_some()
                    && !flush.returning
                    && flush.origin.as_deref() == Some("pipecat.flows") =>
            {
                128
            }
            _ => {
                return Err(ProcessorError::fatal(
                    "application input type or payload rejected",
                ));
            }
        };
        let charge = payload
            .checked_add(std::mem::size_of::<InputAdmission>())
            .ok_or_else(|| ProcessorError::fatal("application input reservation overflow"))?;
        if state.input_admission_bytes.saturating_add(charge) > history.max_input_admission_bytes {
            return Err(ProcessorError::fatal(
                "application input reservation capacity exceeded",
            ));
        }
        let token = frame.observation_token();
        if state.inputs.contains_key(&token) {
            return Err(ProcessorError::fatal(
                "duplicate application input admission",
            ));
        }
        self.input_tag.mark(&mut frame)?;
        state.inputs.insert(
            token,
            InputAdmission {
                frame_id: frame.id,
                data: frame.data.clone(),
                direction: Direction::Downstream,
                charge,
                visits: 0,
            },
        );
        state.input_admission_bytes += charge;
        self.bump(&mut state);
        Ok(frame)
    }
    /// Authenticate the upstream half after the provider creates broadcast_pair.
    pub(crate) fn admit_provider_metadata(
        &self,
        mut frame: Frame,
    ) -> Result<Frame, ProcessorError> {
        Self::reject_user_carriers(&frame, Direction::Upstream)?;
        if !matches!(&frame.data, FrameData::LlmServiceMetadata(_))
            || !frame.metadata.is_empty()
            || frame.pts.is_some()
            || frame.transport_source.is_some()
            || frame.transport_destination.is_some()
        {
            return Err(ProcessorError::fatal(
                "application provider metadata rejected",
            ));
        }
        let mut state = self.lock();
        self.sweep_inputs(&mut state);
        self.available(&state)?;
        let max = state
            .history
            .as_ref()
            .ok_or_else(|| ProcessorError::fatal("application input limits not configured"))?
            .max_input_admission_bytes;
        let charge =
            encoded_len(&frame.data, max)?.saturating_add(std::mem::size_of::<InputAdmission>());
        if state.input_admission_bytes.saturating_add(charge) > max
            || state.inputs.contains_key(&frame.observation_token())
        {
            return Err(ProcessorError::fatal(
                "application metadata reservation rejected",
            ));
        }
        self.input_tag.mark(&mut frame)?;
        state.inputs.insert(
            frame.observation_token(),
            InputAdmission {
                frame_id: frame.id,
                data: frame.data.clone(),
                direction: Direction::Upstream,
                charge,
                visits: 0,
            },
        );
        state.input_admission_bytes += charge;
        self.bump(&mut state);
        Ok(frame)
    }
    fn validate_user_input(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        Self::reject_user_carriers(frame, direction)?;
        if self.input_tag.contains(frame) {
            let mut state = self.lock();
            let token = frame.observation_token();
            let record = state.inputs.get_mut(&token).ok_or_else(|| {
                ProcessorError::fatal("missing or replayed private input admission")
            })?;
            let flow = matches!(&record.data, FrameData::FlushDetailed(_));
            let expected_direction = if flow && record.visits == 1 {
                Direction::Upstream
            } else {
                record.direction
            };
            let payload_matches = match (&record.data, &frame.data) {
                (FrameData::FlushDetailed(expected), FrameData::FlushDetailed(actual)) => {
                    actual.event == expected.event
                        && actual.origin == expected.origin
                        && actual.returning == (record.visits == 2)
                }
                (expected, actual) => expected == actual,
            };
            if record.frame_id != frame.id
                || !payload_matches
                || direction != expected_direction
                || !frame.metadata.is_empty()
                || frame.pts.is_some()
                || frame.transport_source.is_some()
                || frame.transport_destination.is_some()
            {
                return Err(ProcessorError::fatal(
                    "changed application input or direction",
                ));
            }
            if flow && record.visits < 2 {
                record.visits += 1;
            } else {
                let charge = record.charge;
                state.inputs.remove(&token);
                state.input_admission_bytes -= charge;
            }
            self.bump(&mut state);
            return Ok(());
        }
        if frame.data.is_start() || frame.is_terminal() {
            return if direction == Direction::Downstream {
                Ok(())
            } else {
                Err(ProcessorError::fatal(
                    "application lifecycle direction rejected",
                ))
            };
        }
        if direction != Direction::Upstream {
            return Err(ProcessorError::fatal(
                "unauthenticated downstream application input",
            ));
        }
        match &frame.data {
            FrameData::Transcription(_)
            | FrameData::InterimTranscription(_)
            | FrameData::ProposedUserStartedSpeaking
            | FrameData::ProposedUserStoppedSpeaking
            | FrameData::InputDtmf(_) => {
                let state = self.lock();
                if !self.mapped_tag.contains(frame)
                    || state
                        .frames
                        .get(&frame.observation_token())
                        .is_none_or(|record| record.frame_id != frame.id)
                {
                    return Err(ProcessorError::fatal(
                        "user event lacks exact mapped provider identity",
                    ));
                }
                Ok(())
            }
            FrameData::BotStartedSpeaking
            | FrameData::BotStoppedSpeaking
            | FrameData::BotSpeaking
            | FrameData::OutputTransportReady
            | FrameData::Metrics(_)
            | FrameData::Error(_)
            | FrameData::LlmContextAssistantTimestamp { .. }
            | FrameData::LlmContextAssistantTurn(_) => Ok(()),
            _ => Err(ProcessorError::fatal(
                "unsupported upstream application user input",
            )),
        }
    }
    pub(crate) fn discard_mapped_token(
        &self,
        token: &FrameObservationToken,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        let Some(debt) = state.frames.get(token) else {
            if state
                .calls
                .values()
                .any(|call| call.discarded && call.declaration_token == *token)
            {
                return Ok(());
            }
            return Err(ProcessorError::fatal("unknown discarded mapped frame"));
        };
        let _ = debt;
        let ids: Vec<_> = state
            .calls
            .iter()
            .filter(|(_, call)| call.declaration_token == *token)
            .map(|(id, _)| id.clone())
            .collect();
        if ids.iter().any(|id| state.calls[id].delegated) {
            return Err(ProcessorError::fatal(
                "cannot discard an entered application declaration",
            ));
        }
        for id in ids {
            state
                .calls
                .get_mut(&id)
                .expect("retained declaration")
                .discarded = true;
        }
        state.frames.remove(token);
        self.bump(&mut state);
        Ok(())
    }
    pub(crate) fn argument_limit(&self) -> usize {
        self.lock()
            .history
            .as_ref()
            .map_or(self.max_value_bytes, |history| history.max_argument)
    }
    /// Called only after actual output acceptance. Its private media debt is
    /// retired separately by an authenticated real output flush receipt.
    pub(crate) fn output_consumed_token(
        &self,
        token: FrameObservationToken,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        if let Some(debt) = state.frames.remove(&token) {
            state.media.insert(token, debt.sequence);
            self.bump(&mut state);
        }
        Ok(())
    }
    pub(crate) fn note_output_flushed(&self, through: u64) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        if through > state.mapped {
            return Err(ProcessorError::fatal(
                "output flush exceeds mapped watermark",
            ));
        }
        state.media.retain(|_, sequence| *sequence > through);
        self.bump(&mut state);
        Ok(())
    }
    fn admit_history(&self, frame: &Frame, side: AggregatorSide) -> Result<(), ProcessorError> {
        // Count before the real aggregator can clone, buffer or append. The
        // restricted app never admits rich/history-replacement control frames.
        let (bytes, messages) = match (&frame.data, side) {
            (FrameData::InputText(text), AggregatorSide::User) if text.append_to_context => {
                (text.text.len().saturating_add(1), 1)
            }
            (
                FrameData::Transcription(text) | FrameData::InterimTranscription(text),
                AggregatorSide::User,
            ) => (
                text.text.len().saturating_add(1),
                usize::from(text.finalized),
            ),
            (FrameData::LlmMessagesAppend(value), AggregatorSide::User) => (
                encoded_len(&value.messages, self.max_value_bytes)?,
                value.messages.len(),
            ),
            (FrameData::LlmResponseStart, AggregatorSide::Assistant) => (0, 1),
            (
                FrameData::LlmText(text) | FrameData::Text(text) | FrameData::VisionText(text),
                AggregatorSide::Assistant,
            ) if text.append_to_context => (text.text.len().saturating_add(1), 0),
            (
                FrameData::TtsText(text) | FrameData::AggregatedText(text),
                AggregatorSide::Assistant,
            ) if text.text.append_to_context => (
                text.raw_text
                    .as_deref()
                    .unwrap_or(&text.text.text)
                    .len()
                    .saturating_add(1),
                0,
            ),
            (FrameData::FunctionCallsStarted(calls), AggregatorSide::Assistant) => (
                encoded_len(calls, self.max_value_bytes)?,
                calls.len().saturating_mul(2),
            ),
            (FrameData::FunctionCallResult(result), AggregatorSide::Assistant) => (
                encoded_len(result, self.max_value_bytes.saturating_mul(2))?,
                0,
            ),
            (FrameData::FunctionCallCancel(cancel), AggregatorSide::Assistant) => (
                encoded_len(cancel, self.max_value_bytes)?.saturating_add(32),
                0,
            ),
            _ => (0, 0),
        };
        if bytes == 0 && messages == 0 {
            return Ok(());
        }
        let mut state = self.lock();
        let budget = state
            .history
            .as_mut()
            .ok_or_else(|| ProcessorError::fatal("application history limits not configured"))?;
        let next_bytes = budget
            .bytes
            .checked_add(bytes)
            .ok_or_else(|| ProcessorError::fatal("application history capacity exceeded"))?;
        let next_messages = budget
            .messages
            .checked_add(messages)
            .ok_or_else(|| ProcessorError::fatal("application history capacity exceeded"))?;
        if next_bytes > budget.max_bytes || next_messages > budget.max_messages {
            state.failed = true;
            self.bump(&mut state);
            return Err(ProcessorError::fatal(
                "application history capacity exceeded",
            ));
        }
        budget.bytes = next_bytes;
        budget.messages = next_messages;
        self.bump(&mut state);
        Ok(())
    }
    /// Reconcile one actor-owned immutable call identity. True is an irrevocable
    /// local no-entry disposition, not a native outcome. The same mutex guards
    /// this transition, late mapper registration, and actual executor delegation.
    pub(crate) fn reconcile_unentered_call(
        &self,
        call_id: &str,
        epoch: u64,
        original: &ProcessorContext,
    ) -> Result<bool, ProcessorError> {
        let mut state = self.lock();
        if let Some(record) = state.unentered_calls.get(call_id) {
            if record.epoch != epoch || !record.source.same_causal_scope(original) {
                return Err(ProcessorError::fatal(
                    "application discarded call identity conflict",
                ));
            }
            return Ok(true);
        }
        let stale = state.provider_epoch != Some(epoch)
            || !Self::provider_current(&state)
            || !Self::source_current(original);
        if let Some(record) = state.calls.get_mut(call_id) {
            if record.epoch != epoch || !record.source.same_causal_scope(original) {
                return Err(ProcessorError::fatal(
                    "application call source identity conflict",
                ));
            }
            if record.delegated {
                return Ok(false);
            }
            if record.discarded {
                return Ok(true);
            }
            if !stale {
                return Ok(false);
            }
            record.discarded = true;
            let token = record.declaration_token.clone();
            state.frames.remove(&token);
            self.bump(&mut state);
            return Ok(true);
        }
        if !stale {
            return Ok(false);
        }
        self.available(&state)?;
        let max_identity = state
            .history
            .as_ref()
            .map_or(self.max_value_bytes, |history| history.max_argument);
        if call_id.is_empty()
            || call_id.len() > max_identity
            || state.unentered_calls.len() >= self.max_records
        {
            return Err(ProcessorError::fatal(
                "application discarded identity capacity exceeded",
            ));
        }
        state.unentered_calls.insert(
            call_id.to_owned(),
            UnenteredCall {
                epoch,
                source: original.clone(),
            },
        );
        self.bump(&mut state);
        Ok(true)
    }
    pub(crate) fn discard_stale_unentered(&self) {
        let mut state = self.lock();
        let tokens: Vec<_> = state
            .frames
            .iter()
            .filter_map(|(token, debt)| {
                if Self::source_current(&debt.source)
                    && state.provider_epoch == Some(debt.epoch)
                    && Self::provider_current(&state)
                {
                    return None;
                }
                let calls: Vec<_> = state
                    .calls
                    .values()
                    .filter(|call| call.declaration_token == *token)
                    .collect();
                if calls.iter().any(|call| call.delegated) {
                    return None;
                }
                if calls.is_empty() && token.is_alive() {
                    return None;
                }
                Some(token.clone())
            })
            .collect();
        if tokens.is_empty() {
            return;
        }
        for token in tokens {
            for call in state
                .calls
                .values_mut()
                .filter(|call| call.declaration_token == token)
            {
                call.discarded = true;
            }
            state.frames.remove(&token);
        }
        self.bump(&mut state);
    }
    pub(crate) fn discarded_calls(&self) -> Vec<String> {
        self.discard_stale_unentered();
        self.lock()
            .calls
            .iter()
            .filter(|(_, call)| call.discarded)
            .map(|(id, _)| id.clone())
            .collect()
    }
    pub(crate) async fn wait_declarations(&self) -> Result<(), ProcessorError> {
        let mut changed = self.subscribe();
        loop {
            self.discard_stale_unentered();
            {
                let state = self.lock();
                if state.failed || state.cancelled {
                    return Err(ProcessorError::fatal(
                        "application declaration drain failed",
                    ));
                }
                if state
                    .calls
                    .values()
                    .all(|call| call.discarded || call.assistant_declared)
                {
                    return Ok(());
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| ProcessorError::fatal("application declaration observer closed"))?;
        }
    }
    pub(crate) fn note_wire_outcome(
        &self,
        outcome: &pipecat_core::NativeToolCallOutcome,
    ) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        let call = state
            .calls
            .get_mut(&outcome.call().tool_call_id)
            .ok_or_else(|| ProcessorError::fatal("unknown application native outcome"))?;
        if call.call != *outcome.call()
            || call.discarded
            || call.outcome.is_some_and(|id| id != outcome.id())
        {
            return Err(ProcessorError::fatal(
                "conflicting application native outcome",
            ));
        }
        call.outcome = Some(outcome.id());
        call.wire = true;
        self.bump(&mut state);
        Ok(())
    }
    /// Obtain the exact source without retiring its actor-admission debt.
    /// The provider must acknowledge only after every real action was enqueued.
    pub(crate) fn take_context_source(
        &self,
        frame: &Frame,
    ) -> Result<ProcessorContext, ProcessorError> {
        frame
            .native_realtime_context_cause()?
            .ok_or_else(|| ProcessorError::fatal("application context has no native cause"))?;
        self.lock()
            .context_sources
            .get(&frame.observation_token())
            .cloned()
            .ok_or_else(|| {
                ProcessorError::fatal("application context lacks its exact originating invocation")
            })
    }
    pub(crate) fn retire_context_source(&self, frame: &Frame) -> Result<(), ProcessorError> {
        frame
            .native_realtime_context_cause()?
            .ok_or_else(|| ProcessorError::fatal("application context has no native cause"))?;
        let mut state = self.lock();
        if state
            .context_sources
            .remove(&frame.observation_token())
            .is_none()
        {
            state.failed = true;
            self.bump(&mut state);
            return Err(ProcessorError::fatal(
                "application context admission acknowledgment is missing or replayed",
            ));
        }
        self.bump(&mut state);
        Ok(())
    }
    fn marker(&self, kind: MarkerKind, system: bool) -> Result<Frame, ProcessorError> {
        let mut frame = if system {
            Frame::new(FrameData::System)
        } else {
            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame::default()))
        }
        .with_interruptible(false);
        self.marker_tag.mark(&mut frame)?;
        let mut state = self.lock();
        self.available(&state)?;
        state.markers.insert(
            frame.observation_token(),
            Marker {
                frame_id: frame.id,
                kind,
            },
        );
        self.bump(&mut state);
        Ok(frame)
    }
    fn peek_marker(&self, frame: &Frame) -> Result<Option<MarkerKind>, ProcessorError> {
        if !self.marker_tag.contains(frame) {
            return Ok(None);
        }
        let state = self.lock();
        let marker = state
            .markers
            .get(&frame.observation_token())
            .ok_or_else(|| ProcessorError::fatal("unknown private application marker"))?;
        let system = matches!(
            marker.kind,
            MarkerKind::CutSystem(_)
                | MarkerKind::InputUserSystem(_)
                | MarkerKind::InputProviderSystem(_)
                | MarkerKind::VadUser(_)
                | MarkerKind::VadProvider(_)
        );
        let shape = if system {
            matches!(&frame.data, FrameData::System)
        } else {
            matches!(&frame.data, FrameData::FlushDetailed(flush) if *flush == PipelineFlushFrame::default())
        };
        let vad_headers_changed = matches!(
            marker.kind,
            MarkerKind::VadUser(_) | MarkerKind::VadProvider(_)
        ) && (frame.interruptible
            || frame.pts.is_some()
            || frame.has_pipeline_pts()
            || frame.transport_source.is_some()
            || frame.transport_destination.is_some()
            || frame.broadcast_sibling_id.is_some());
        if marker.frame_id != frame.id
            || !shape
            || !frame.metadata.is_empty()
            || vad_headers_changed
        {
            return Err(ProcessorError::fatal("changed private application marker"));
        }
        Ok(Some(marker.kind))
    }
    fn input_marker(kind: MarkerKind) -> bool {
        matches!(
            kind,
            MarkerKind::InputUser(_)
                | MarkerKind::InputUserSystem(_)
                | MarkerKind::InputProvider(_)
                | MarkerKind::InputProviderSystem(_)
                | MarkerKind::VadUser(_)
                | MarkerKind::VadProvider(_)
        )
    }
    /// This scheduling cut carries only the already-reserved provider event
    /// sequence. It cannot authorize inference, refresh a source, or clear a gate.
    pub(crate) fn make_vad_cut(&self, sequence: u64) -> Result<Frame, ProcessorError> {
        {
            let state = self.lock();
            self.available(&state)?;
            if sequence == 0
                || sequence > state.emitted
                || state.markers.values().any(|marker| {
                    matches!(
                        marker.kind,
                        MarkerKind::VadUser(_) | MarkerKind::VadProvider(_)
                    )
                })
            {
                return Err(ProcessorError::fatal(
                    "invalid or overlapping application VAD cut",
                ));
            }
        }
        self.marker(MarkerKind::VadUser(sequence), true)
    }
    /// The provider intercepts only the exact downstream roundtrip before its
    /// ordinary boundary dispatch. The actor still owns sequence reconciliation.
    pub(crate) fn take_vad_provider_ack(
        &self,
        frame: &Frame,
        direction: Direction,
    ) -> Result<Option<u64>, ProcessorError> {
        match self.peek_marker(frame)? {
            Some(MarkerKind::VadProvider(sequence)) => {
                Self::reject_user_carriers(frame, direction)?;
                if direction != Direction::Downstream {
                    return Err(ProcessorError::fatal(
                        "application VAD acknowledgment direction",
                    ));
                }
                self.take_marker(frame)?;
                Ok(Some(sequence))
            }
            Some(MarkerKind::VadUser(_)) => Err(ProcessorError::fatal(
                "application VAD cut reached provider before user",
            )),
            _ => Ok(None),
        }
    }
    pub(crate) fn make_input_cut(&self) -> Result<(Frame, LocalFlushEvent), ProcessorError> {
        let event = LocalFlushEvent::default();
        let id = {
            let mut state = self.lock();
            self.available(&state)?;
            let id = self.next(&mut state)?;
            state.input_cuts.insert(id, event.clone());
            self.bump(&mut state);
            id
        };
        Ok((self.marker(MarkerKind::InputUser(id), false)?, event))
    }
    /// Call at the provider before interpreting frame data. The Normal cut
    /// waits nonblockingly for earlier System commands via a private sentinel.
    pub(crate) async fn provider_boundary(
        &self,
        frame: &Frame,
        ctx: &ProcessorContext,
    ) -> Result<bool, ProcessorError> {
        match self.peek_marker(frame)? {
            Some(MarkerKind::InputProvider(id)) => {
                self.take_marker(frame)?;
                ctx.queue(
                    self.marker(MarkerKind::InputProviderSystem(id), true)?,
                    Direction::Downstream,
                )
                .await?;
                Ok(true)
            }
            Some(MarkerKind::InputProviderSystem(id)) => {
                self.take_marker(frame)?;
                let event = {
                    let mut state = self.lock();
                    let event = state
                        .input_cuts
                        .remove(&id)
                        .ok_or_else(|| ProcessorError::fatal("unknown application input cut"))?;
                    self.bump(&mut state);
                    event
                };
                event.set();
                Ok(true)
            }
            Some(_) => Err(ProcessorError::fatal(
                "unexpected private marker at provider",
            )),
            None => Ok(false),
        }
    }
    fn take_marker(&self, frame: &Frame) -> Result<Option<MarkerKind>, ProcessorError> {
        self.peek_marker(frame)?;
        if !self.marker_tag.contains(frame) {
            return Ok(None);
        }
        let mut state = self.lock();
        let marker = state
            .markers
            .remove(&frame.observation_token())
            .ok_or_else(|| ProcessorError::fatal("replayed private application marker"))?;
        if marker.frame_id != frame.id {
            state.failed = true;
            return Err(ProcessorError::fatal("changed private application marker"));
        }
        self.bump(&mut state);
        Ok(Some(marker.kind))
    }
    fn begin(&self) -> Result<u64, ProcessorError> {
        let mut state = self.lock();
        self.available(&state)?;
        let id = self.next(&mut state)?;
        state.invocations.insert(
            id,
            Invocation {
                source: None,
                finished: false,
            },
        );
        self.bump(&mut state);
        Ok(id)
    }
    fn finish_invocation(&self, id: u64, source: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        let invocation = state
            .invocations
            .get_mut(&id)
            .ok_or_else(|| ProcessorError::fatal("missing application invocation"))?;
        invocation.source = Some(source.clone());
        invocation.finished = true;
        self.bump(&mut state);
        Ok(())
    }
    fn consumed(&self, frame: &Frame, side: AggregatorSide) -> Result<(), ProcessorError> {
        let mut state = self.lock();
        state.frames.remove(&frame.observation_token());
        if side == AggregatorSide::Assistant {
            if let FrameData::FunctionCallsStarted(calls) = &frame.data {
                if frame.native_tool_batch()?.is_none() {
                    return Err(ProcessorError::fatal(
                        "assistant declaration lacks native batch",
                    ));
                }
                for call in calls {
                    let record = state
                        .calls
                        .get_mut(&call.tool_call_id)
                        .ok_or_else(|| ProcessorError::fatal("assistant declaration unknown"))?;
                    if record.call != *call
                        || record.declaration_token != frame.observation_token()
                        || !record.delegated
                        || record.discarded
                    {
                        return Err(ProcessorError::fatal(
                            "assistant declaration identity conflict",
                        ));
                    }
                    record.assistant_declared = true;
                }
            }
            if let Some(outcome) = frame.native_tool_outcome()? {
                let call = state
                    .calls
                    .get_mut(&outcome.call().tool_call_id)
                    .ok_or_else(|| {
                        ProcessorError::fatal("assistant consumed unknown native outcome")
                    })?;
                if call.call != *outcome.call()
                    || call.discarded
                    || call.outcome.is_some_and(|id| id != outcome.id())
                {
                    return Err(ProcessorError::fatal(
                        "assistant native outcome identity conflict",
                    ));
                }
                call.outcome = Some(outcome.id());
                call.assistant = true;
            }
            match &frame.data {
                FrameData::LlmResponseStart => {
                    state.gates.llm = true;
                    state.gates.active = true;
                    state.gates.awaiting = false;
                }
                FrameData::LlmResponseEnd => {
                    state.gates.llm = false;
                    state.gates.active = false;
                }
                FrameData::TtsStarted(value) if value.append_to_context => {
                    state.gates.active = true
                }
                FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallsStartedDetailed(_) => {
                    state.gates.llm = true
                }
                FrameData::UserStartedSpeaking => {
                    state.gates.user = true;
                    if frame
                        .metadata
                        .get("user_context_follows")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    {
                        state.gates.awaiting = false;
                    }
                }
                FrameData::UserStoppedSpeaking => {
                    state.gates.user = false;
                    if let Some(value) = frame
                        .metadata
                        .get("user_context_follows")
                        .and_then(serde_json::Value::as_bool)
                    {
                        state.gates.awaiting = value;
                    }
                }
                FrameData::BotStartedSpeaking => state.gates.bot = true,
                FrameData::BotStoppedSpeaking => state.gates.bot = false,
                FrameData::Interruption => {
                    state.gates.bot = false;
                    state.gates.llm = false;
                    state.gates.active = false;
                    if frame
                        .metadata
                        .get("user_context_follows")
                        .and_then(serde_json::Value::as_bool)
                        != Some(true)
                    {
                        state.gates.awaiting = false;
                    }
                }
                _ => {}
            }
        } else {
            // The actual paired user can set this gate synchronously before its
            // outgoing context reaches the assistant. Only real later boundaries release it.
            if matches!(&frame.data, FrameData::InputText(text) if text.append_to_context && !text.text.trim().is_empty())
            {
                state.gates.bot = false;
                state.gates.llm = false;
                state.gates.active = false;
            }
            if matches!(frame.data, FrameData::ProposedUserStartedSpeaking) {
                state.gates.user = true;
                state.gates.awaiting = false;
                state.gates.bot = false;
                state.gates.llm = false;
                state.gates.active = false;
            }
        }
        self.bump(&mut state);
        Ok(())
    }
    /// Start a two-lane cut at one actual boundary. Queue both markers on this
    /// processor; do not wait while occupying its normal or system process call.
    pub(crate) async fn start_cut(&self, ctx: &ProcessorContext) -> Result<u64, ProcessorError> {
        let id = {
            let mut state = self.lock();
            self.available(&state)?;
            let id = self.next(&mut state)?;
            state.cuts.insert(
                id,
                Cut {
                    normal: false,
                    system: false,
                },
            );
            self.bump(&mut state);
            id
        };
        ctx.queue(
            self.marker(MarkerKind::CutNormal(id), false)?,
            Direction::Downstream,
        )
        .await?;
        ctx.queue(
            self.marker(MarkerKind::CutSystem(id), true)?,
            Direction::Downstream,
        )
        .await?;
        Ok(id)
    }
    pub(crate) async fn wait_cut(&self, id: u64) -> Result<(), ProcessorError> {
        let mut changed = self.subscribe();
        loop {
            {
                let state = self.lock();
                if state.failed || state.cancelled {
                    return Err(ProcessorError::fatal("application cut failed"));
                }
                if !state.cuts.contains_key(&id) {
                    return Ok(());
                }
            }
            changed
                .changed()
                .await
                .map_err(|_| ProcessorError::fatal("application cut observer closed"))?;
        }
    }
    fn consume_cut(&self, kind: MarkerKind) -> Result<bool, ProcessorError> {
        let (id, normal) = match kind {
            MarkerKind::CutNormal(id) => (id, true),
            MarkerKind::CutSystem(id) => (id, false),
            _ => return Ok(false),
        };
        let mut state = self.lock();
        let cut = state
            .cuts
            .get_mut(&id)
            .ok_or_else(|| ProcessorError::fatal("unknown application cut"))?;
        if normal {
            cut.normal = true;
        } else {
            cut.system = true;
        }
        if cut.normal && cut.system {
            state.cuts.remove(&id);
        }
        self.bump(&mut state);
        Ok(true)
    }
}

/// Calls the actual standard aggregator, never its reducer as a replacement.
pub(crate) struct AggregatorBoundary {
    inner: Arc<dyn FrameProcessor>,
    state: Arc<AppState>,
    side: AggregatorSide,
    serial: AsyncMutex<()>,
    context: Mutex<Option<ProcessorContext>>,
}
impl AggregatorBoundary {
    pub(crate) fn user(inner: Arc<dyn FrameProcessor>, state: Arc<AppState>) -> Self {
        Self::new(inner, state, AggregatorSide::User)
    }
    pub(crate) fn assistant(inner: Arc<dyn FrameProcessor>, state: Arc<AppState>) -> Self {
        Self::new(inner, state, AggregatorSide::Assistant)
    }
    fn new(inner: Arc<dyn FrameProcessor>, state: Arc<AppState>, side: AggregatorSide) -> Self {
        Self {
            inner,
            state,
            side,
            serial: AsyncMutex::new(()),
            context: Mutex::new(None),
        }
    }
    /// Setup routing for explicit public input only, never inference provenance.
    pub(crate) fn input_context(&self) -> Option<ProcessorContext> {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
    }
    pub(crate) async fn begin_cut(&self) -> Result<u64, ProcessorError> {
        let ctx = self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or_else(|| ProcessorError::fatal("aggregator boundary is not ready"))?;
        self.state.start_cut(&ctx).await
    }
}
#[async_trait]
impl FrameProcessor for AggregatorBoundary {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await?;
        *self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        #[cfg(test)]
        if self.side == AggregatorSide::Assistant
            && matches!(&frame.data, FrameData::BotStoppedSpeaking)
        {
            if let Some(hooks) = self.state.test_hooks() {
                hooks.bot_stopped.pause(&self.state).await;
            }
        }
        let _serial = self.serial.lock().await;
        if self.side == AggregatorSide::User {
            AppState::reject_user_carriers(&frame, direction)?;
        }
        if let Some(kind) = self.state.peek_marker(&frame)? {
            if AppState::input_marker(kind) {
                match kind {
                    MarkerKind::VadUser(sequence)
                        if self.side == AggregatorSide::User
                            && direction == Direction::Upstream =>
                    {
                        // Both marker hops are System FIFO. This runs only after
                        // the prior actual ProposedStart invocation returned,
                        // including its genuine interruption broadcast.
                        let acknowledgment =
                            self.state.marker(MarkerKind::VadProvider(sequence), true)?;
                        self.state.take_marker(&frame)?;
                        return ctx.push(acknowledgment, Direction::Downstream).await;
                    }
                    MarkerKind::InputUser(id) if self.side == AggregatorSide::User => {
                        self.state.take_marker(&frame)?;
                        return ctx
                            .queue(
                                self.state.marker(MarkerKind::InputUserSystem(id), true)?,
                                Direction::Downstream,
                            )
                            .await;
                    }
                    MarkerKind::InputUserSystem(id) if self.side == AggregatorSide::User => {
                        self.state.take_marker(&frame)?;
                        return ctx
                            .push(
                                self.state.marker(MarkerKind::InputProvider(id), false)?,
                                Direction::Downstream,
                            )
                            .await;
                    }
                    _ => {
                        return Err(ProcessorError::fatal(
                            "input cut reached wrong aggregator boundary",
                        ));
                    }
                }
            }
        }
        if let Some(kind) = self.state.take_marker(&frame)? {
            if self.state.consume_cut(kind)? {
                return Ok(());
            }
            return Err(ProcessorError::fatal("bracket marker reached aggregator"));
        }
        if self.side == AggregatorSide::User {
            self.state.validate_user_input(&frame, direction)?;
        }
        // Lifecycle forwarding does not schedule app inference and must remain
        // possible after the actor has atomically closed admissions.
        if frame.data.is_start() || frame.is_terminal() {
            return self.inner.process(frame, direction, ctx).await;
        }
        // Real terminal user flushes may still deliver history-only snapshots
        // after the actor closes. They carry no inference capability.
        if self.state.lock().closing
            && matches!(
                &frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
            && frame.native_realtime_context_cause()?.is_none()
        {
            return self.inner.process(frame, direction, ctx).await;
        }
        let id = self.state.begin()?;
        let route = if self.side == AggregatorSide::User {
            Direction::Downstream
        } else {
            Direction::Upstream
        };
        ctx.push(
            self.state.marker(MarkerKind::Begin(id, self.side), false)?,
            route,
        )
        .await?;
        self.state.admit_history(&frame, self.side)?;
        let observed = frame.clone();
        let result = self.inner.process(frame, direction, ctx).await;
        if let Err(error) = result {
            self.state.fail();
            return Err(error);
        }
        self.state.consumed(&observed, self.side)?;
        self.state.finish_invocation(id, ctx)?;
        ctx.push(
            self.state.marker(MarkerKind::End(id, self.side), false)?,
            route,
        )
        .await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        self.inner.cleanup().await
    }
}
struct TapInvocation {
    id: u64,
    contexts: Vec<Frame>,
}
pub(crate) struct ContextTap {
    state: Arc<AppState>,
    side: AggregatorSide,
    active: AsyncMutex<Option<TapInvocation>>,
    context: Mutex<Option<ProcessorContext>>,
}
impl ContextTap {
    pub(crate) fn user(state: Arc<AppState>) -> Self {
        Self::new(state, AggregatorSide::User)
    }
    pub(crate) fn assistant(state: Arc<AppState>) -> Self {
        Self::new(state, AggregatorSide::Assistant)
    }
    fn new(state: Arc<AppState>, side: AggregatorSide) -> Self {
        Self {
            state,
            side,
            active: AsyncMutex::new(None),
            context: Mutex::new(None),
        }
    }
    pub(crate) async fn begin_cut(&self) -> Result<u64, ProcessorError> {
        let ctx = self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
            .ok_or_else(|| ProcessorError::fatal("context tap is not ready"))?;
        self.state.start_cut(&ctx).await
    }
}
#[async_trait]
impl FrameProcessor for ContextTap {
    fn name(&self) -> &str {
        if self.side == AggregatorSide::User {
            "GrokAppUserContextTap"
        } else {
            "GrokAppAssistantContextTap"
        }
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self
            .context
            .lock()
            .unwrap_or_else(|error| error.into_inner()) = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut active = self.active.lock().await;
        if self
            .state
            .peek_marker(&frame)?
            .is_some_and(AppState::input_marker)
        {
            return ctx.push(frame, direction).await;
        }
        if let Some(kind) = self.state.take_marker(&frame)? {
            if self.state.consume_cut(kind)? {
                return Ok(());
            }
            match kind {
                MarkerKind::Begin(id, side) if side == self.side && active.is_none() => {
                    *active = Some(TapInvocation {
                        id,
                        contexts: Vec::new(),
                    });
                    return Ok(());
                }
                MarkerKind::End(id, side)
                    if side == self.side && active.as_ref().is_some_and(|value| value.id == id) =>
                {
                    let invocation = active.take().expect("matching invocation");
                    let source = {
                        let state = self.state.lock();
                        let value = state
                            .invocations
                            .get(&id)
                            .ok_or_else(|| ProcessorError::fatal("missing bracket source"))?;
                        if !value.finished {
                            return Err(ProcessorError::fatal("unfinished bracket source"));
                        }
                        value
                            .source
                            .clone()
                            .ok_or_else(|| ProcessorError::fatal("missing bracket context"))?
                    };
                    for context in invocation.contexts {
                        if self.side == AggregatorSide::User
                            && context
                                .native_realtime_context_cause()?
                                .is_some_and(|cause| cause.run_llm())
                        {
                            let mut state = self.state.lock();
                            state.gates.awaiting = true;
                            self.state.bump(&mut state);
                        }
                        {
                            let mut state = self.state.lock();
                            self.state.available(&state)?;
                            if state
                                .context_sources
                                .insert(context.observation_token(), source.clone())
                                .is_some()
                            {
                                return Err(ProcessorError::fatal("duplicate bracket context"));
                            }
                            self.state.bump(&mut state);
                        }
                        ctx.push(context, direction).await?;
                    }
                    {
                        let mut state = self.state.lock();
                        state.invocations.remove(&id);
                        self.state.bump(&mut state);
                    }
                    return Ok(());
                }
                _ => {
                    self.state.fail();
                    return Err(ProcessorError::fatal(
                        "crossed application invocation brackets",
                    ));
                }
            }
        }
        let route = if self.side == AggregatorSide::User {
            Direction::Downstream
        } else {
            Direction::Upstream
        };
        if direction == route && frame.native_realtime_context_cause()?.is_some() {
            let invocation = active
                .as_mut()
                .ok_or_else(|| ProcessorError::fatal("unbracketed native application context"))?;
            if !invocation.contexts.is_empty() {
                return Err(ProcessorError::fatal(
                    "too many contexts in one application invocation",
                ));
            }
            invocation.contexts.push(frame);
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.context
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .take();
        self.active.lock().await.take();
        Ok(())
    }
}

pub(crate) struct GuardedExecutor {
    inner: ToolExecutor,
    state: Arc<AppState>,
}
impl GuardedExecutor {
    pub(crate) fn new(inner: ToolExecutor, state: Arc<AppState>) -> Self {
        Self { inner, state }
    }
}
#[async_trait]
impl FrameProcessor for GuardedExecutor {
    fn name(&self) -> &str {
        "GrokAppGuardedToolExecutor"
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let FrameData::FunctionCallsStarted(calls) = &frame.data else {
            return self.inner.process(frame, direction, ctx).await;
        };
        #[cfg(test)]
        if let Some(hooks) = self.state.test_hooks() {
            hooks.guard.pause(&self.state).await;
        }
        if direction != Direction::Downstream || !self.state.mapped_tag.contains(&frame) {
            return Err(ProcessorError::fatal(
                "application declaration direction or private origin",
            ));
        }
        if frame.native_tool_batch()?.is_some() || frame.native_tool_outcome()?.is_some() {
            return Err(ProcessorError::fatal(
                "application declaration already has executor authority",
            ));
        }
        let (source, epoch) = {
            let state = self.state.lock();
            let token = frame.observation_token();
            if !calls.is_empty()
                && calls.iter().all(|call| {
                    state
                        .calls
                        .get(&call.tool_call_id)
                        .is_some_and(|registered| {
                            registered.discarded
                                && registered.call == *call
                                && registered.declaration_token == token
                        })
                })
            {
                #[cfg(test)]
                if let Some(hooks) = self.state.test_hooks() {
                    let original = &state.calls[&calls[0].tool_call_id].source;
                    hooks.note_guard_recheck(
                        ctx.is_current() && ctx.is_usable(),
                        AppState::source_current(original),
                    );
                }
                return Ok(());
            }
            let debt = state.frames.get(&token).ok_or_else(|| {
                ProcessorError::fatal("tool declaration lacks private mapped identity")
            })?;
            if debt.frame_id != frame.id || calls.is_empty() {
                return Err(ProcessorError::fatal("changed mapped declaration identity"));
            }
            for call in calls {
                let registered = state
                    .calls
                    .get(&call.tool_call_id)
                    .ok_or_else(|| ProcessorError::fatal("unknown mapped application call"))?;
                if registered.call != *call
                    || registered.delegated
                    || registered.discarded
                    || registered.epoch != debt.epoch
                {
                    return Err(ProcessorError::fatal(
                        "replayed or changed mapped application call",
                    ));
                }
            }
            (debt.source.clone(), debt.epoch)
        };
        let source_current = AppState::source_current(&source);
        let destination_current = ctx.is_current() && ctx.is_usable();
        #[cfg(test)]
        if let Some(hooks) = self.state.test_hooks() {
            hooks.note_guard_recheck(destination_current, source_current);
        }
        let current = source_current && destination_current;
        {
            let mut state = self.state.lock();
            if !current
                || !AppState::provider_current(&state)
                || state.provider_epoch != Some(epoch)
            {
                for call in calls {
                    if let Some(record) = state.calls.get_mut(&call.tool_call_id) {
                        record.discarded = true;
                    }
                }
                state.frames.remove(&frame.observation_token());
                self.state.bump(&mut state);
                return Ok(());
            }
            self.state.available(&state)?;
            for call in calls {
                state
                    .calls
                    .get_mut(&call.tool_call_id)
                    .expect("validated call")
                    .delegated = true;
            }
            self.state.bump(&mut state);
        }
        // The real executor publishes native batches, owns callback tasks and
        // settles outcomes. Its spawn sees the retained original causal source.
        let original = ctx.forwarding_context(&source);
        self.inner.process(frame, direction, &original).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}

/// Bounds final return data and checks the original scope on the first poll,
/// before constructing the caller's future. Native executor entry/settlement
/// still belongs solely to ToolExecutor; this is not a fabricated NotStarted receipt.
pub(crate) fn wrap_final_tool(tool: RegisteredTool, state: Arc<AppState>) -> RegisteredTool {
    let callback = tool.callback;
    RegisteredTool {
        timeout: tool.timeout,
        cancel_on_interruption: tool.cancel_on_interruption,
        callback: Arc::new(move |call| {
            let callback = callback.clone();
            let state = state.clone();
            Box::pin(async move {
                #[cfg(test)]
                if let Some(hooks) = state.test_hooks() {
                    hooks.callback.pause(&state).await;
                }
                #[cfg(test)]
                {
                    let gate = state.callback_gate.lock().unwrap().clone();
                    if let Some(gate) = gate {
                        gate.entered.notify_one();
                        gate.release.notified().await;
                    }
                }
                let (source, epoch) = {
                    let ledger = state.lock();
                    let record = ledger
                        .calls
                        .get(&call.tool_call_id)
                        .ok_or_else(|| "application tool provenance missing".to_owned())?;
                    if record.call != call
                        || !record.delegated
                        || record.discarded
                        || record.callback_checked
                    {
                        return Err("application tool provenance rejected".into());
                    }
                    (record.source.clone(), record.epoch)
                };
                if !AppState::source_current(&source) {
                    return Err("application tool source expired".into());
                }
                {
                    let mut ledger = state.lock();
                    if ledger.failed
                        || ledger.cancelled
                        || ledger.closing
                        || !AppState::provider_current(&ledger)
                        || ledger.provider_epoch != Some(epoch)
                    {
                        return Err("application tool admission closed".into());
                    }
                    ledger
                        .calls
                        .get_mut(&call.tool_call_id)
                        .ok_or_else(|| "application tool provenance missing".to_owned())?
                        .callback_checked = true;
                    state.bump(&mut ledger);
                }
                let mut value = callback(call)
                    .await
                    .map_err(|_| "application final callback failed".to_owned())?;
                crate::app_config::compact_json(&mut value, state.max_value_bytes).map_err(
                    |_| "application final result exceeds bounded representation".to_owned(),
                )?;
                Ok(value)
            })
        }),
    }
}

/// Serialization length without allocating a serialized payload or a deep clone.
fn encoded_len(value: &impl serde::Serialize, limit: usize) -> Result<usize, ProcessorError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            let next = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("bounded serialization"))?;
            self.bytes = next;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|_| ProcessorError::fatal("application history payload exceeds limit"))?;
    Ok(counter.bytes)
}

/// Observes acceptance by the real output, without replacing its drain,
/// generated text forwarding, speaking signals, or playback semantics.
pub(crate) struct OutputBoundary {
    inner: Arc<dyn FrameProcessor>,
    state: Arc<AppState>,
}
impl OutputBoundary {
    pub(crate) fn new(inner: Arc<dyn FrameProcessor>, state: Arc<AppState>) -> Self {
        Self { inner, state }
    }
}
#[async_trait]
impl FrameProcessor for OutputBoundary {
    fn name(&self) -> &str {
        self.inner.name()
    }
    fn health(&self) -> Option<pipecat_core::ProcessorHealth> {
        self.inner.health()
    }
    fn process_when_unusable(&self, frame: &Frame, direction: Direction) -> bool {
        self.inner.process_when_unusable(frame, direction)
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let result = self.inner.setup(config, ctx).await;
        if result.is_err() {
            self.state
                .note_failure_stage(GrokAppFailureStage::OutputSetup);
        }
        result
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let audio = matches!(
            &frame.data,
            FrameData::TtsAudio(_) | FrameData::OutputAudio(_) | FrameData::SpeechOutputAudio(_)
        );
        let token = frame.observation_token();
        match self.inner.process(frame, direction, ctx).await {
            Ok(()) => {
                if audio {
                    self.state.output_consumed_token(token)?;
                }
                Ok(())
            }
            Err(error) => {
                self.state
                    .note_failure_stage(GrokAppFailureStage::OutputProcess);
                Err(error)
            }
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let result = self.inner.prepare_end(ctx).await;
        if result.is_err() {
            self.state
                .note_failure_stage(GrokAppFailureStage::OutputPrepareEnd);
        }
        result
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        let result = self.inner.cleanup().await;
        if result.is_err() {
            self.state
                .note_cleanup_failure(GrokAppFailureStage::OutputCleanup);
        }
        result
    }
}

#[cfg(test)]
#[derive(Default)]
struct CallbackGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{Pipeline, PipelineConfig};
    use std::{
        sync::atomic::{AtomicUsize, Ordering},
        time::Duration,
    };

    struct Source {
        state: Arc<AppState>,
        context: Mutex<Option<ProcessorContext>>,
        original: Option<ProcessorContext>,
        mapped_gate: Option<Arc<CallbackGate>>,
    }
    #[async_trait]
    impl FrameProcessor for Source {
        fn name(&self) -> &str {
            "ApplicationOriginalSource"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::LlmRun) {
                *self.context.lock().unwrap() = Some(ctx.clone());
                let original = self.original.as_ref().unwrap_or(ctx);
                self.state.register_provider_scope(0, original.clone());
                let sequence = self.state.note_emitted()?;
                let mut declaration =
                    Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                        function_name: "final".into(),
                        tool_call_id: "call".into(),
                        arguments: serde_json::json!({}),
                    }]));
                self.state.register_mapped_frame(
                    sequence,
                    &mut declaration,
                    original.clone(),
                    0,
                )?;
                if let Some(gate) = &self.mapped_gate {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                }
                ctx.push(declaration, Direction::Downstream).await?;
                self.state.mapped_through(sequence);
                return Ok(());
            }
            ctx.push(frame, direction).await
        }
    }
    #[derive(Default)]
    struct Terminal {
        outcomes: Mutex<Vec<Arc<pipecat_core::NativeToolCallOutcome>>>,
        carriers: Mutex<Vec<Frame>>,
        ready: tokio::sync::Notify,
    }
    #[async_trait]
    impl FrameProcessor for Terminal {
        fn name(&self) -> &str {
            "ApplicationNativeOutcomeObserver"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if frame.native_tool_batch()?.is_some() || frame.native_tool_outcome()?.is_some() {
                self.carriers.lock().unwrap().push(frame.clone());
            }
            if let Some(outcome) = frame.native_tool_outcome()? {
                self.outcomes.lock().unwrap().push(outcome.clone());
                self.ready.notify_one();
            }
            ctx.push(frame, direction).await
        }
    }
    async fn first_poll_retirement(provider: bool) {
        let state = AppState::new(128, 4096);
        state
            .configure_history_limits(&crate::app_config::GrokAppLimits::default(), 0, 0)
            .unwrap();
        let health = pipecat_core::ProcessorHealth::default();
        state.bind_provider_health(health.clone()).unwrap();
        let gate = Arc::new(CallbackGate::default());
        *state.callback_gate.lock().unwrap() = Some(gate.clone());
        let source = Arc::new(Source {
            state: state.clone(),
            context: Mutex::new(None),
            original: None,
            mapped_gate: None,
        });
        let terminal = Arc::new(Terminal::default());
        let entered = Arc::new(AtomicUsize::new(0));
        let counter = entered.clone();
        let tool = RegisteredTool {
            timeout: Duration::from_secs(2),
            cancel_on_interruption: true,
            callback: Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(serde_json::Value::Null) })
            }),
        };
        let mut executor = ToolExecutor::new(4, 32, 4096).unwrap();
        executor
            .register("final", wrap_final_tool(tool, state.clone()))
            .unwrap();
        let mut worker = Pipeline::new(vec![
            source.clone(),
            Arc::new(GuardedExecutor::new(executor, state.clone())),
            terminal.clone(),
        ])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        if provider {
            health.set_usable(false);
        } else {
            source
                .context
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .set_usable(false);
        }
        gate.release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), terminal.ready.notified())
            .await
            .unwrap();
        assert_eq!(
            entered.load(Ordering::SeqCst),
            0,
            "caller future must not be constructed after source or provider retirement"
        );
        assert_eq!(
            terminal.outcomes.lock().unwrap().len(),
            1,
            "only the real native executor settles this entered task"
        );
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.recv().await.is_some() {}
            worker.join().await.unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn original_source_retirement_before_callback_first_poll_rejects_user_future() {
        first_poll_retirement(false).await;
    }
    #[tokio::test]
    async fn provider_retirement_before_callback_first_poll_rejects_user_future() {
        first_poll_retirement(true).await;
    }
    #[derive(Default)]
    struct OriginalCapture {
        context: Mutex<Option<ProcessorContext>>,
        captured: tokio::sync::Notify,
        interrupted: tokio::sync::Notify,
    }
    #[async_trait]
    impl FrameProcessor for OriginalCapture {
        fn name(&self) -> &str {
            "SeparateOriginalRuntime"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(frame.data, FrameData::LlmRun) {
                *self.context.lock().unwrap() = Some(ctx.clone());
                self.captured.notify_one();
            }
            if matches!(frame.data, FrameData::Interruption) {
                self.interrupted.notify_one();
            }
            ctx.push(frame, direction).await
        }
    }
    async fn separate_source_interruption(before_guard: bool) {
        let original = Arc::new(OriginalCapture::default());
        let mut origin_worker = Pipeline::new(vec![original.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        origin_worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), original.captured.notified())
            .await
            .unwrap();
        let original_context = original.context.lock().unwrap().clone().unwrap();
        let state = AppState::new(128, 4096);
        state
            .configure_history_limits(&crate::app_config::GrokAppLimits::default(), 0, 0)
            .unwrap();
        state
            .bind_provider_health(pipecat_core::ProcessorHealth::default())
            .unwrap();
        let gate = Arc::new(CallbackGate::default());
        if !before_guard {
            *state.callback_gate.lock().unwrap() = Some(gate.clone());
        }
        let source = Arc::new(Source {
            state: state.clone(),
            context: Mutex::new(None),
            original: Some(original_context.clone()),
            mapped_gate: before_guard.then(|| gate.clone()),
        });
        let terminal = Arc::new(Terminal::default());
        let entered = Arc::new(AtomicUsize::new(0));
        let counter = entered.clone();
        let tool = RegisteredTool {
            timeout: Duration::from_secs(2),
            cancel_on_interruption: true,
            callback: Arc::new(move |_| {
                counter.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(serde_json::Value::Null) })
            }),
        };
        let mut executor = ToolExecutor::new(4, 32, 4096).unwrap();
        executor
            .register("final", wrap_final_tool(tool, state.clone()))
            .unwrap();
        let outcomes = executor.outcomes();
        let mut worker = Pipeline::new(vec![
            source.clone(),
            Arc::new(GuardedExecutor::new(executor, state.clone())),
            terminal.clone(),
        ])
        .start(PipelineConfig::default())
        .await
        .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
            .await
            .unwrap();
        origin_worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), original.interrupted.notified())
            .await
            .unwrap();
        assert!(!original_context.is_current());
        assert!(
            source
                .context
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .is_current(),
            "destination runtime must remain fresh"
        );
        gate.release.notify_one();
        if before_guard {
            tokio::time::timeout(Duration::from_secs(2), async {
                let mut changed = state.subscribe();
                loop {
                    if state
                        .lock()
                        .calls
                        .get("call")
                        .is_some_and(|call| call.discarded)
                    {
                        break;
                    }
                    changed.changed().await.unwrap();
                }
            })
            .await
            .unwrap();
            assert_eq!(
                outcomes.snapshot().total_terminal,
                0,
                "pre-entry rejection must not invent native settlement"
            );
            assert!(terminal.outcomes.lock().unwrap().is_empty());
        } else {
            tokio::time::timeout(Duration::from_secs(2), terminal.ready.notified())
                .await
                .unwrap();
            let snapshot = outcomes.snapshot();
            assert_eq!(snapshot.total_terminal, 1);
            let outcome = &snapshot.terminal[0].outcome;
            assert_eq!(
                outcome.reason(),
                pipecat_core::ToolCallEndReason::StaleResult
            );
            assert_eq!(
                outcome.state(),
                pipecat_core::ToolCallTerminalState::Cancelled
            );
            assert_eq!(
                outcome.effects(),
                pipecat_core::ToolCallEffectStatus::ExternalEffectsUnknown
            );
        }
        assert_eq!(entered.load(Ordering::SeqCst), 0);
        worker.end().await.unwrap();
        origin_worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.recv().await.is_some() {}
            worker.join().await.unwrap();
            while origin_worker.recv().await.is_some() {}
            origin_worker.join().await.unwrap();
        })
        .await
        .unwrap();
    }
    #[tokio::test]
    async fn original_runtime_interruption_before_callback_yields_real_stale_receipt() {
        separate_source_interruption(false).await;
    }
    #[tokio::test]
    async fn original_runtime_interruption_before_guard_is_unentered_without_receipt() {
        separate_source_interruption(true).await;
    }
    #[test]
    fn private_input_identity_rejects_relabel_direction_duplicate_and_replay() {
        let state = AppState::new(128, 4096);
        state
            .configure_history_limits(&crate::app_config::GrokAppLimits::default(), 0, 0)
            .unwrap();
        let raw = Frame::new(FrameData::LlmRun);
        assert!(state
            .validate_user_input(&raw, Direction::Downstream)
            .is_err());
        let admitted = state.admit_input(raw).unwrap();
        let mut changed = admitted.clone();
        changed.data = FrameData::Interruption;
        assert!(state
            .validate_user_input(&changed, Direction::Downstream)
            .is_err());
        assert!(state
            .validate_user_input(&admitted, Direction::Upstream)
            .is_err());
        assert!(state
            .validate_user_input(&admitted.duplicate(), Direction::Downstream)
            .is_err());
        state
            .validate_user_input(&admitted, Direction::Downstream)
            .unwrap();
        assert!(state
            .validate_user_input(&admitted, Direction::Downstream)
            .is_err());
        assert!(state
            .admit_input(Frame::new(FrameData::LlmContext(Default::default())))
            .is_err());
        assert!(state
            .validate_user_input(
                &Frame::new(FrameData::ProposedUserStartedSpeaking),
                Direction::Upstream
            )
            .is_err());
        assert_eq!(state.lock().history.as_ref().unwrap().bytes, 0);
    }

    #[test]
    fn private_media_fence_requires_exact_roundtrip_and_dead_admission_is_swept() {
        let state = AppState::new(128, 4096);
        state
            .configure_history_limits(&crate::app_config::GrokAppLimits::default(), 0, 0)
            .unwrap();
        let mut frame = state
            .admit_input(Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: Some(LocalFlushEvent::default()),
                returning: false,
                origin: Some("pipecat.flows".into()),
            })))
            .unwrap();
        state
            .validate_user_input(&frame, Direction::Downstream)
            .unwrap();
        assert!(state
            .validate_user_input(&frame, Direction::Downstream)
            .is_err());
        state
            .validate_user_input(&frame, Direction::Upstream)
            .unwrap();
        if let FrameData::FlushDetailed(flush) = &mut frame.data {
            flush.returning = true;
        }
        state
            .validate_user_input(&frame, Direction::Downstream)
            .unwrap();
        assert!(state.lock().inputs.is_empty());
        let abandoned = state.admit_input(Frame::new(FrameData::LlmRun)).unwrap();
        drop(abandoned);
        state.frontier_clear();
        assert!(state.lock().inputs.is_empty());
        assert_eq!(state.lock().input_admission_bytes, 0);
    }
    fn boundary_state() -> Arc<AppState> {
        let state = AppState::new(256, 4096);
        state
            .configure_history_limits(&crate::app_config::GrokAppLimits::default(), 0, 0)
            .unwrap();
        state
            .bind_provider_health(pipecat_core::ProcessorHealth::default())
            .unwrap();
        state
    }
    fn user_append() -> FrameData {
        FrameData::LlmMessagesAppend(pipecat_core::MessagesFrame {
            messages: vec![pipecat_core::ChatMessage::new(
                pipecat_core::ChatRole::User,
                "must not enter history",
            )],
            run_llm: true,
        })
    }
    fn standard_pair() -> pipecat_turns::LLMContextAggregatorPair {
        pipecat_turns::LLMContextAggregatorPair::with_realtime_mode(
            pipecat_services::Context::default(),
            Default::default(),
            Default::default(),
            pipecat_turns::RealtimeServiceMode::Enabled,
        )
        .unwrap()
    }
    async fn reject_at_actual_user(
        boundary: &AggregatorBoundary,
        pair: &pipecat_turns::LLMContextAggregatorPair,
        state: &AppState,
        ctx: &ProcessorContext,
        frame: Frame,
        direction: Direction,
    ) {
        let history = pair.context().frame().unwrap();
        let revision = state.actor_revision();
        assert!(boundary.process(frame, direction, ctx).await.is_err());
        assert_eq!(
            pair.context().frame().unwrap(),
            history,
            "actual standard history mutated"
        );
        assert_eq!(
            state.actor_revision(),
            revision,
            "rejection admitted a bracket/history obligation"
        );
    }
    async fn join_fixture(mut worker: pipecat_core::PipelineWorker) {
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.recv().await.is_some() {}
            worker.join().await.unwrap();
        })
        .await
        .unwrap();
    }
    #[derive(Default)]
    struct MappedHold {
        retained: Mutex<Option<(Frame, ProcessorContext)>>,
        gate: Option<Arc<CallbackGate>>,
    }
    #[async_trait]
    impl FrameProcessor for MappedHold {
        fn name(&self) -> &str {
            "ActualMappedDeclarationHold"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            if matches!(&frame.data, FrameData::FunctionCallsStarted(_)) {
                *self.retained.lock().unwrap() = Some((frame.clone(), ctx.clone()));
                if let Some(gate) = &self.gate {
                    gate.entered.notify_one();
                    gate.release.notified().await;
                }
            }
            ctx.push(frame, direction).await
        }
    }
    async fn native_fixture(
        held: bool,
    ) -> (
        pipecat_core::PipelineWorker,
        Arc<AppState>,
        Arc<MappedHold>,
        Arc<GuardedExecutor>,
        Arc<Terminal>,
        pipecat_processors::tools::ToolOutcomeJournal,
        Arc<AtomicUsize>,
    ) {
        let state = boundary_state();
        let source = Arc::new(Source {
            state: state.clone(),
            context: Mutex::new(None),
            original: None,
            mapped_gate: None,
        });
        let hold = Arc::new(MappedHold {
            retained: Mutex::new(None),
            gate: held.then(|| Arc::new(CallbackGate::default())),
        });
        let count = Arc::new(AtomicUsize::new(0));
        let counter = count.clone();
        let mut executor = ToolExecutor::new(4, 32, 4096).unwrap();
        executor
            .register(
                "final",
                RegisteredTool {
                    timeout: Duration::from_secs(2),
                    cancel_on_interruption: true,
                    callback: Arc::new(move |_| {
                        counter.fetch_add(1, Ordering::SeqCst);
                        Box::pin(async { Ok(serde_json::Value::Null) })
                    }),
                },
            )
            .unwrap();
        let journal = executor.outcomes();
        let guard = Arc::new(GuardedExecutor::new(executor, state.clone()));
        let terminal = Arc::new(Terminal::default());
        let worker = Pipeline::new(vec![source, hold.clone(), guard.clone(), terminal.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        if let Some(gate) = &hold.gate {
            tokio::time::timeout(Duration::from_secs(2), gate.entered.notified())
                .await
                .unwrap();
        } else {
            tokio::time::timeout(Duration::from_secs(2), terminal.ready.notified())
                .await
                .unwrap();
        }
        (worker, state, hold, guard, terminal, journal, count)
    }

    #[tokio::test]
    async fn actual_user_wrapper_rejects_native_relabels_raw_edits_and_speculation() {
        let (worker, _, hold, _, terminal, journal, count) = native_fixture(false).await;
        let ctx = hold.retained.lock().unwrap().as_ref().unwrap().1.clone();
        let state = boundary_state();
        let pair = standard_pair();
        let boundary = AggregatorBoundary::user(pair.user(), state.clone());
        let before = journal.snapshot();
        assert_eq!(before.total_terminal, 1);
        let native = terminal.carriers.lock().unwrap().clone();
        assert!(native
            .iter()
            .any(|f| f.native_tool_batch().unwrap().is_some()));
        assert!(native
            .iter()
            .any(|f| f.native_tool_outcome().unwrap().is_some()));
        for original in native {
            for direction in [Direction::Downstream, Direction::Upstream] {
                reject_at_actual_user(&boundary, &pair, &state, &ctx, original.clone(), direction)
                    .await;
                let mut relabeled = original.clone();
                relabeled.data = user_append();
                reject_at_actual_user(&boundary, &pair, &state, &ctx, relabeled, direction).await;
            }
        }
        let raw = vec![
            user_append(),
            FrameData::LlmContext(Default::default()),
            FrameData::LlmMessagesUpdate(pipecat_core::MessagesFrame {
                messages: vec![],
                run_llm: true,
            }),
            FrameData::LlmSetTools(pipecat_core::ToolSet::Direct(vec![])),
            FrameData::LlmSetToolChoice(pipecat_core::ToolChoice::Required),
            FrameData::FunctionCallResult(pipecat_core::FunctionCallResultFrame {
                function_name: "final".into(),
                tool_call_id: "forged".into(),
                arguments: serde_json::json!({}),
                result: serde_json::Value::Null,
                run_llm: Some(true),
                is_final: true,
                error: None,
            }),
        ];
        for data in raw {
            for direction in [Direction::Downstream, Direction::Upstream] {
                reject_at_actual_user(
                    &boundary,
                    &pair,
                    &state,
                    &ctx,
                    Frame::new(data.clone()),
                    direction,
                )
                .await;
            }
        }
        let owner = pipecat_core::SpeculationOwner::new();
        let mut speculative = state.admit_input(Frame::new(user_append())).unwrap();
        speculative
            .set_native_speculation(owner.capability())
            .unwrap();
        reject_at_actual_user(
            &boundary,
            &pair,
            &state,
            &ctx,
            speculative,
            Direction::Downstream,
        )
        .await;
        let admitted = state.admit_input(Frame::new(user_append())).unwrap();
        reject_at_actual_user(
            &boundary,
            &pair,
            &state,
            &ctx,
            admitted.clone(),
            Direction::Upstream,
        )
        .await;
        let mut changed = admitted.clone();
        changed.data = FrameData::LlmSetTools(pipecat_core::ToolSet::Direct(vec![]));
        reject_at_actual_user(
            &boundary,
            &pair,
            &state,
            &ctx,
            changed,
            Direction::Downstream,
        )
        .await;
        assert_eq!(journal.snapshot().total_terminal, before.total_terminal);
        assert_eq!(journal.snapshot().terminal, before.terminal);
        assert_eq!(journal.snapshot().active, before.active);
        assert_eq!(count.load(Ordering::SeqCst), 1);
        join_fixture(worker).await;
    }

    #[tokio::test]
    async fn actual_guard_rejects_private_identity_payload_direction_and_replay() {
        let (worker, state, hold, guard, terminal, journal, count) = native_fixture(true).await;
        let (original, ctx) = hold.retained.lock().unwrap().clone().unwrap();
        let mut wrong_identity = Frame::new(original.data.clone());
        state.mapped_tag.mark(&mut wrong_identity).unwrap();
        let mut negatives = vec![
            Frame::new(original.data.clone()),
            original.duplicate(),
            wrong_identity,
        ];
        for field in 0..3 {
            let mut changed = original.clone();
            let FrameData::FunctionCallsStarted(calls) = &mut changed.data else {
                unreachable!()
            };
            match field {
                0 => calls[0].function_name = "substituted".into(),
                1 => calls[0].tool_call_id = "substituted".into(),
                _ => calls[0].arguments = serde_json::json!({"changed":true}),
            }
            negatives.push(changed);
        }
        for frame in negatives {
            let revision = state.actor_revision();
            assert!(guard
                .process(frame, Direction::Downstream, &ctx)
                .await
                .is_err());
            assert_eq!(state.actor_revision(), revision);
            assert!(journal.snapshot().active.is_empty());
            assert_eq!(journal.snapshot().total_terminal, 0);
            assert_eq!(count.load(Ordering::SeqCst), 0);
        }
        assert!(guard
            .process(original.clone(), Direction::Upstream, &ctx)
            .await
            .is_err());
        hold.gate.as_ref().unwrap().release.notify_one();
        tokio::time::timeout(Duration::from_secs(2), terminal.ready.notified())
            .await
            .unwrap();
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(journal.snapshot().total_terminal, 1);
        let settled = journal.snapshot();
        assert!(guard
            .process(original, Direction::Downstream, &ctx)
            .await
            .is_err());
        assert_eq!(count.load(Ordering::SeqCst), 1);
        let native = terminal
            .carriers
            .lock()
            .unwrap()
            .iter()
            .find(|frame| matches!(&frame.data, FrameData::FunctionCallsStarted(_)))
            .unwrap()
            .clone();
        assert!(guard
            .process(native, Direction::Downstream, &ctx)
            .await
            .is_err());
        assert_eq!(journal.snapshot().total_terminal, 1);
        assert_eq!(journal.snapshot().terminal, settled.terminal);
        assert!(journal.snapshot().active.is_empty());
        join_fixture(worker).await;
    }

    struct ProgressCaptureRole {
        role: pipecat_core::NativeToolProgressRole,
        route: pipecat_core::NativeToolProgressRoute,
        shared: Arc<Mutex<Vec<Frame>>>,
        ready: Arc<tokio::sync::Notify>,
        history_identity: Arc<()>,
    }
    #[async_trait]
    impl FrameProcessor for ProgressCaptureRole {
        fn name(&self) -> &str {
            "ActualProgressCarrierCapture"
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            use pipecat_core::NativeToolProgressRole as Role;
            self.route.register_role(
                self.role,
                ctx,
                matches!(self.role, Role::User | Role::Assistant)
                    .then_some(Arc::as_ptr(&self.history_identity) as usize),
            )?;
            if self.role == Role::Gate {
                self.route.register_gate_context(ctx)?;
            }
            Ok(())
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            use pipecat_core::NativeToolProgressRole as Role;
            if frame.data.is_start() && self.role == Role::Provider {
                self.route.validate_and_open(
                    ctx,
                    pipecat_core::NativeToolProgressProviderLimits {
                        response_timeout: Duration::from_secs(1),
                        operation_timeout: Duration::from_secs(1),
                        close_timeout: Duration::from_secs(1),
                        max_message_bytes: 1024 * 1024,
                        max_in_flight: 1,
                    },
                )?;
            }
            if self.role == Role::Relay && direction == Direction::Downstream {
                let progress = frame.native_tool_progress()?.is_some();
                if progress || frame.native_tool_progress_admission()?.is_some() {
                    self.shared.lock().unwrap().push(frame.clone());
                    if progress {
                        self.ready.notify_one();
                    }
                }
            }
            ctx.push(frame, direction).await
        }
    }
    #[tokio::test]
    async fn actual_user_wrapper_rejects_real_progress_admission_and_report_carriers() {
        use pipecat_core::{
            NativeToolProgressConfig, NativeToolProgressFailure, NativeToolProgressRole as Role,
            NativeToolProgressRoute,
        };
        let route = NativeToolProgressRoute::new(NativeToolProgressConfig {
            end_drain_timeout: Duration::from_secs(7),
        })
        .unwrap();
        let captured = Arc::new(Mutex::new(vec![]));
        let ready = Arc::new(tokio::sync::Notify::new());
        let history_identity = Arc::new(());
        let role = |role| {
            Arc::new(ProgressCaptureRole {
                role,
                route: route.clone(),
                shared: captured.clone(),
                ready: ready.clone(),
                history_identity: history_identity.clone(),
            }) as Arc<dyn FrameProcessor>
        };
        let original = Arc::new(OriginalCapture::default());
        let release = Arc::new(tokio::sync::Notify::new());
        let count = Arc::new(AtomicUsize::new(0));
        let mut executor = ToolExecutor::new(1, 8, 4096)
            .unwrap()
            .with_progress_route(route.clone())
            .unwrap();
        executor
            .register_reporting(
                "report",
                pipecat_processors::tools::ReportingTool {
                    timeout: Duration::from_secs(2),
                    cancel_on_interruption: false,
                    callback: Arc::new({
                        let release = release.clone();
                        let count = count.clone();
                        move |_, reporter| {
                            let release = release.clone();
                            let count = count.clone();
                            Box::pin(async move {
                                count.fetch_add(1, Ordering::SeqCst);
                                reporter
                                    .report(serde_json::json!({"partial":true}), Some(false))
                                    .map_err(|_| "actual report failed".to_owned())?;
                                release.notified().await;
                                Ok(serde_json::Value::Null)
                            })
                        }
                    }),
                },
            )
            .unwrap();
        let journal = executor.outcomes();
        let mut worker = Pipeline::new(vec![
            original.clone(),
            role(Role::Gate),
            role(Role::User),
            role(Role::Provider),
            Arc::new(executor),
            role(Role::Relay),
            role(Role::Assistant),
        ])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_secs(20),
            ..Default::default()
        })
        .await
        .unwrap();
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), original.captured.notified())
            .await
            .unwrap();
        worker
            .sender()
            .send(
                Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
                    function_name: "report".into(),
                    tool_call_id: "real-progress".into(),
                    arguments: serde_json::json!({}),
                }])),
                Direction::Downstream,
            )
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), ready.notified())
            .await
            .unwrap();
        let ctx = original.context.lock().unwrap().clone().unwrap();
        let state = boundary_state();
        let pair = standard_pair();
        let boundary = AggregatorBoundary::user(pair.user(), state.clone());
        let carriers = captured.lock().unwrap().clone();
        assert!(carriers
            .iter()
            .any(|f| f.native_tool_progress_admission().unwrap().is_some()));
        assert!(carriers
            .iter()
            .any(|f| f.native_tool_progress().unwrap().is_some()));
        let before = journal.snapshot();
        assert_eq!(before.active.len(), 1);
        assert_eq!(before.total_terminal, 0);
        for frame in carriers {
            for direction in [Direction::Downstream, Direction::Upstream] {
                reject_at_actual_user(&boundary, &pair, &state, &ctx, frame.clone(), direction)
                    .await;
                let mut changed = frame.clone();
                changed.data = user_append();
                reject_at_actual_user(&boundary, &pair, &state, &ctx, changed, direction).await;
            }
        }
        assert_eq!(count.load(Ordering::SeqCst), 1);
        assert_eq!(journal.snapshot().active, before.active);
        assert_eq!(journal.snapshot().terminal, before.terminal);
        assert_eq!(
            journal.snapshot().total_terminal,
            0,
            "rejection must not fabricate progress settlement"
        );
        route.close(NativeToolProgressFailure::Cancelled);
        release.notify_one();
        worker
            .cancel("private carrier fixture complete")
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(2), async {
            while worker.recv().await.is_some() {}
            worker.join().await.unwrap();
        })
        .await
        .unwrap();
    }
    #[test]
    fn failure_stage_and_drain_diagnostics_survive_retirement_without_primary_overwrite() {
        let state = boundary_state();
        {
            let mut ledger = state.lock();
            ledger.gates.user = true;
            ledger.gates.awaiting = true;
            ledger.cuts.insert(
                1,
                Cut {
                    normal: true,
                    system: false,
                },
            );
        }
        state.note_failure_stage(GrokAppFailureStage::Mapping);
        state.note_failure_stage(GrokAppFailureStage::Receive);
        state.note_cleanup_failure(GrokAppFailureStage::OutputCleanup);
        let before = state.snapshot();
        assert_eq!(
            before.first_failure_stage,
            Some(GrokAppFailureStage::Mapping)
        );
        assert!(before.failed && before.cleanup_output_failed);
        assert!(before.drain_gates.user && before.drain_gates.awaiting);
        assert_eq!(before.drain_gates.pending_cuts, 1);
        state.release_runtime_contexts();
        let after = state.snapshot();
        assert_eq!(after.first_failure_stage, before.first_failure_stage);
        assert_eq!(after.cleanup_output_failed, before.cleanup_output_failed);
        assert_eq!(after.drain_gates, before.drain_gates);
    }
    #[test]
    fn private_vad_cut_rejects_shape_identity_direction_and_replay() {
        let state = boundary_state();
        assert!(state.make_vad_cut(0).is_err());
        assert!(state.make_vad_cut(1).is_err());
        let sequence = state.note_emitted().unwrap();
        let user = state.make_vad_cut(sequence).unwrap();
        assert!(state.make_vad_cut(sequence).is_err());
        assert!(
            matches!(state.peek_marker(&user).unwrap(), Some(MarkerKind::VadUser(value)) if value == sequence)
        );
        assert!(state
            .take_vad_provider_ack(&user, Direction::Downstream)
            .is_err());
        let mut changed = user.clone();
        changed.data = FrameData::BotSpeaking;
        assert!(state.peek_marker(&changed).is_err());
        let mut changed = user.clone();
        changed.interruptible = true;
        assert!(state.peek_marker(&changed).is_err());
        assert!(state.peek_marker(&user.duplicate()).is_err());
        let acknowledgment = state
            .marker(MarkerKind::VadProvider(sequence), true)
            .unwrap();
        state.take_marker(&user).unwrap();
        assert!(state
            .take_vad_provider_ack(&acknowledgment, Direction::Upstream)
            .is_err());
        let mut changed = acknowledgment.clone();
        changed
            .metadata
            .insert("sequence".into(), serde_json::json!(sequence));
        assert!(state
            .take_vad_provider_ack(&changed, Direction::Downstream)
            .is_err());
        assert_eq!(
            state
                .take_vad_provider_ack(&Frame::new(FrameData::System), Direction::Downstream)
                .unwrap(),
            None
        );
        assert_eq!(
            state
                .take_vad_provider_ack(&acknowledgment, Direction::Downstream)
                .unwrap(),
            Some(sequence)
        );
        assert!(state
            .take_vad_provider_ack(&acknowledgment, Direction::Downstream)
            .is_err());
        assert!(state.lock().markers.is_empty());
        assert_eq!(state.snapshot().drain_gates, GrokAppDrainGates::default());
        assert_eq!(state.snapshot().mapped_calls, 0);
    }
}
