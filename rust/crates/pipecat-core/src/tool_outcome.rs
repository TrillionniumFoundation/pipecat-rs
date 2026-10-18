//! Process-local tool settlement receipts. Admission is not downstream consumption.
use crate::{
    Direction, Frame, FrameData, FunctionCall, FunctionCallCancelFrame, FunctionCallResultFrame,
    ProcessorContext, ProcessorError,
};
use serde_json::{json, Value};
use std::sync::Arc;

/// Complete, immutable membership of one admitted executor batch. Every member
/// shares this descriptor, so terminal delivery can reconstruct overtaken policy.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeToolCallBatch {
    id: u64,
    group_id: String,
    members: Vec<(Arc<FunctionCall>, bool)>,
    retained_bytes: usize,
}
impl NativeToolCallBatch {
    pub const MAX_MEMBERS: usize = 1024;
    pub const MAX_RETAINED_BYTES: usize = 8 * 1024 * 1024;
    /// Validate and measure borrowed membership before allocating owned calls.
    pub fn measure(
        group_id: &str,
        members: &[(&FunctionCall, bool)],
    ) -> Result<usize, ProcessorError> {
        if group_id.is_empty()
            || group_id.len() > Self::MAX_RETAINED_BYTES
            || members.is_empty()
            || members.len() > Self::MAX_MEMBERS
        {
            return Err(ProcessorError::new("invalid native tool batch"));
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut bytes = group_id.len();
        for (call, _) in members {
            if call.function_name.is_empty()
                || call.tool_call_id.is_empty()
                || !call.arguments.is_object()
                || !ids.insert(&call.tool_call_id)
            {
                return Err(ProcessorError::new(
                    "invalid or duplicate native tool batch member",
                ));
            }
            bytes = bytes
                .checked_add(encoded_bytes(*call, Self::MAX_RETAINED_BYTES)?)
                .filter(|bytes| *bytes <= Self::MAX_RETAINED_BYTES)
                .ok_or_else(|| ProcessorError::new("native tool batch exceeds byte limit"))?;
        }
        Ok(bytes)
    }
    /// Encoded common-consumer policy cost, measured without cloning arguments.
    pub fn policy_bytes(
        call: &FunctionCall,
        cancel_on_interruption: bool,
        group_id: Option<&str>,
    ) -> Result<usize, ProcessorError> {
        #[derive(serde::Serialize)]
        struct Policy<'a> {
            function_name: &'a str,
            tool_call_id: &'a str,
            arguments: &'a Value,
            cancel_on_interruption: bool,
            group_id: Option<&'a str>,
        }
        encoded_bytes(
            &Policy {
                function_name: &call.function_name,
                tool_call_id: &call.tool_call_id,
                arguments: &call.arguments,
                cancel_on_interruption,
                group_id,
            },
            Self::MAX_RETAINED_BYTES,
        )
    }
    pub fn new(
        group_id: String,
        members: Vec<(Arc<FunctionCall>, bool)>,
    ) -> Result<Arc<Self>, ProcessorError> {
        if group_id.is_empty() || members.is_empty() || members.len() > Self::MAX_MEMBERS {
            return Err(ProcessorError::new("invalid native tool batch"));
        }
        let mut ids = std::collections::BTreeSet::new();
        let mut retained_bytes = group_id.len();
        for (call, _) in &members {
            if call.function_name.is_empty()
                || call.tool_call_id.is_empty()
                || !call.arguments.is_object()
                || !ids.insert(&call.tool_call_id)
            {
                return Err(ProcessorError::new(
                    "invalid or duplicate native tool batch member",
                ));
            }
            retained_bytes = retained_bytes
                .checked_add(encoded_bytes(call.as_ref(), Self::MAX_RETAINED_BYTES)?)
                .filter(|bytes| *bytes <= Self::MAX_RETAINED_BYTES)
                .ok_or_else(|| ProcessorError::new("native tool batch exceeds byte limit"))?;
        }
        Ok(Arc::new(Self {
            id: Frame::new(FrameData::LlmRun).id,
            group_id,
            members,
            retained_bytes,
        }))
    }
    /// Internal batch identity, independent of its human-readable group label.
    pub(crate) fn validate(&self, data: &FrameData) -> Result<(), ProcessorError> {
        let valid = match data {
            FrameData::FunctionCallsStarted(calls) => {
                calls.len() == self.members.len()
                    && calls
                        .iter()
                        .zip(&self.members)
                        .all(|(call, (member, _))| call == member.as_ref())
            }
            FrameData::FunctionCallInProgress(call) => {
                self.members.iter().any(|(member, policy)| {
                    call.function_name == member.function_name
                        && call.tool_call_id == member.tool_call_id
                        && call.arguments == member.arguments
                        && call.cancel_on_interruption == *policy
                        && call.group_id.as_deref() == Some(self.group_id())
                })
            }
            _ => false,
        };
        if valid {
            Ok(())
        } else {
            Err(ProcessorError::new(
                "native tool batch does not match declaration",
            ))
        }
    }
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn group_id(&self) -> &str {
        &self.group_id
    }
    pub fn members(&self) -> &[(Arc<FunctionCall>, bool)] {
        &self.members
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
}

fn encoded_bytes(value: &impl serde::Serialize, limit: usize) -> Result<usize, ProcessorError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= self.limit)
                .ok_or_else(|| std::io::Error::other("native tool payload exceeds byte limit"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut counter = Counter { bytes: 0, limit };
    serde_json::to_writer(&mut counter, value)
        .map_err(|error| ProcessorError::new(error.to_string()))?;
    Ok(counter.bytes)
}

#[derive(Debug, Clone, PartialEq)]
struct TerminalResult {
    result: Value,
    error: Option<String>,
    run_llm: Option<bool>,
    bytes: usize,
    selected_retained: usize,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallTerminalState {
    Completed,
    Failed,
    Cancelled,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallEndReason {
    CallbackReturned,
    CallbackError,
    CallbackPanicked,
    TimedOut,
    ExplicitCancel,
    SpeculationDenied,
    TaskDropped,
    StaleResult,
    AdmissionRejected,
    NotRegistered,
    ConcurrencyLimit,
    ResultTooLarge,
}
/// Dropping a callback never proves that a remote side effect was rolled back.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ToolCallEffectStatus {
    NotStarted,
    ExternalEffectsUnknown,
}

/// Immutable native provenance. Public metadata and wire decoding cannot create it.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeToolCallOutcome {
    id: u64,
    call: Arc<FunctionCall>,
    cancel_on_interruption: bool,
    state: ToolCallTerminalState,
    reason: ToolCallEndReason,
    effects: ToolCallEffectStatus,
    run_llm: bool,
    batch: Option<Arc<NativeToolCallBatch>>,
    result: Option<TerminalResult>,
    progress_terminal: Option<crate::NativeToolProgressTerminal>,
    journal_only: bool,
    progress_summary: Option<crate::NativeToolProgressTerminalSummary>,
}
impl NativeToolCallOutcome {
    pub fn progress_journal_summary(&self) -> Option<crate::NativeToolProgressTerminalSummary> {
        self.progress_summary
    }
    pub fn is_journal_projection(&self) -> bool {
        self.journal_only
    }
    pub fn progress_terminal(&self) -> Option<&crate::NativeToolProgressTerminal> {
        self.progress_terminal.as_ref()
    }
    /// Immutable identity of the original terminal publication.
    pub fn id(&self) -> u64 {
        self.id
    }
    pub fn call(&self) -> &FunctionCall {
        &self.call
    }
    pub fn cancel_on_interruption(&self) -> bool {
        self.cancel_on_interruption
    }
    pub fn state(&self) -> ToolCallTerminalState {
        self.state
    }
    pub fn reason(&self) -> ToolCallEndReason {
        self.reason
    }
    pub fn run_llm(&self) -> bool {
        self.run_llm
    }
    pub fn effects(&self) -> ToolCallEffectStatus {
        self.effects
    }
    pub fn batch(&self) -> Option<&Arc<NativeToolCallBatch>> {
        self.batch.as_ref()
    }
    /// Conservative encoded bytes reachable from this receipt, including the
    /// entire shared batch. This is a retention budget, not an RSS measurement.
    pub fn retained_bytes(&self) -> usize {
        self.batch
            .as_ref()
            .map_or_else(
                || encoded_bytes(self.call.as_ref(), usize::MAX).unwrap_or(usize::MAX),
                |batch| batch.retained_bytes(),
            )
            .saturating_add(self.result.as_ref().map_or(0, |result| result.bytes))
    }
    pub(crate) fn validate(
        &self,
        data: &FrameData,
        timeout_projection: bool,
    ) -> Result<(), ProcessorError> {
        if self.journal_only {
            return Err(ProcessorError::fatal(
                "compact selected journal is not frame authority",
            ));
        }
        if self.progress_terminal.is_some() {
            match data {
                FrameData::FunctionCallCancel(cancel) => {
                    crate::tool_progress::validate_selected_call_copy(
                        &cancel.function_name,
                        &cancel.tool_call_id,
                        None,
                        &self.call,
                    )?;
                }
                FrameData::FunctionCallResult(result) => {
                    crate::tool_progress::validate_selected_call_copy(
                        &result.function_name,
                        &result.tool_call_id,
                        Some(&result.arguments),
                        &self.call,
                    )?;
                    let retained = crate::tool_progress::measured_json_retention(
                        &result.result,
                        crate::NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES,
                    )?
                    .saturating_add(result.error.as_ref().map_or(0, String::capacity));
                    if self
                        .result
                        .as_ref()
                        .is_none_or(|sealed| retained > sealed.selected_retained)
                    {
                        return Err(ProcessorError::fatal(
                            "selected terminal retained representation changed",
                        ));
                    }
                }
                _ => {
                    return Err(ProcessorError::fatal(
                        "selected terminal payload family changed",
                    ))
                }
            }
        }
        let matches = match data {
            FrameData::FunctionCallCancel(cancel) => {
                (self.state == ToolCallTerminalState::Cancelled && cancel.run_llm == self.run_llm
                    || timeout_projection
                        && self.state == ToolCallTerminalState::Failed
                        && self.reason == ToolCallEndReason::TimedOut
                        && cancel.run_llm)
                    && cancel.function_name == self.call.function_name
                    && cancel.tool_call_id == self.call.tool_call_id
            }
            FrameData::FunctionCallResult(result) => {
                !timeout_projection
                    && self.state != ToolCallTerminalState::Cancelled
                    && result.function_name == self.call.function_name
                    && result.tool_call_id == self.call.tool_call_id
                    && result.arguments == self.call.arguments
                    && result.is_final
                    && result.error.is_some() == (self.state == ToolCallTerminalState::Failed)
                    && self.result.as_ref().is_some_and(|sealed| {
                        sealed.result == result.result
                            && sealed.error == result.error
                            && sealed.run_llm == result.run_llm
                    })
            }
            _ => false,
        };
        if matches {
            Ok(())
        } else {
            Err(ProcessorError::new(
                "native tool outcome does not match frame",
            ))
        }
    }
}

/// Describes only the first pipeline hop. Even Admitted may later be discarded
/// by shutdown or an application processor; it is never a provider acknowledgment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ToolOutcomeAdmission {
    Admitted,
    Closed,
    Undelivered(String),
    /// Routing succeeded while terminal shutdown raced the observation. The
    /// receiver may or may not have observed the frame; never retry its effect.
    ShutdownRace,
}
#[derive(Debug, Clone, PartialEq)]
pub struct ToolCallReceipt {
    pub outcome: Arc<NativeToolCallOutcome>,
    pub admission: ToolOutcomeAdmission,
}

/// One native publisher, owned by a bounded executor settlement lease. It has
/// no destructor work and launches no tasks. Consuming it publishes one terminal
/// outcome, with stale results replaced by a cancellation acknowledgment.
pub struct ToolOutcomePublisher {
    context: ProcessorContext,
    call: Arc<FunctionCall>,
    cancel_on_interruption: bool,
    batch: Option<Arc<NativeToolCallBatch>>,
    progress_terminal: Option<crate::NativeToolProgressTerminal>,
}
impl ProcessorContext {
    pub fn tool_outcome_publisher(
        &self,
        call: impl Into<Arc<FunctionCall>>,
        cancel_on_interruption: bool,
    ) -> Result<ToolOutcomePublisher, ProcessorError> {
        let call = call.into();
        if call.function_name.is_empty()
            || call.tool_call_id.is_empty()
            || !call.arguments.is_object()
        {
            return Err(ProcessorError::new("invalid tool outcome identity"));
        }
        Ok(ToolOutcomePublisher {
            context: self.clone(),
            call,
            cancel_on_interruption,
            batch: None,
            progress_terminal: None,
        })
    }
}
impl ToolOutcomePublisher {
    pub fn with_progress_terminal(
        mut self,
        terminal: crate::NativeToolProgressTerminal,
    ) -> Result<Self, ProcessorError> {
        if terminal.admission().call() != self.call.as_ref()
            || self
                .batch
                .as_ref()
                .is_none_or(|batch| batch.id() != terminal.admission().batch().id())
            || self.progress_terminal.is_some()
        {
            return Err(ProcessorError::fatal(
                "selected terminal does not match publisher lease",
            ));
        }
        self.progress_terminal = Some(terminal);
        Ok(self)
    }
    pub fn batch(&self) -> Option<&Arc<NativeToolCallBatch>> {
        self.batch.as_ref()
    }
    pub fn with_batch(mut self, batch: Arc<NativeToolCallBatch>) -> Result<Self, ProcessorError> {
        if !batch
            .members
            .iter()
            .any(|(call, policy)| call == &self.call && *policy == self.cancel_on_interruption)
        {
            return Err(ProcessorError::new(
                "native tool publisher does not match batch policy",
            ));
        }
        self.batch = Some(batch);
        Ok(self)
    }
    pub fn cancel(
        self,
        effects: ToolCallEffectStatus,
        reason: ToolCallEndReason,
        run_llm: bool,
    ) -> ToolCallReceipt {
        let data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: self.call.function_name.clone(),
            tool_call_id: self.call.tool_call_id.clone(),
            run_llm,
        });
        self.publish(
            data,
            ToolCallTerminalState::Cancelled,
            effects,
            reason,
            run_llm,
        )
    }
    pub fn complete(
        self,
        mut result: Result<Value, String>,
        effects: ToolCallEffectStatus,
        mut reason: ToolCallEndReason,
    ) -> ToolCallReceipt {
        if self.progress_terminal.is_some() {
            let valid = match &result {
                Ok(value) => crate::tool_progress::validate_json(
                    value,
                    crate::NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES,
                ),
                Err(error) => crate::tool_progress::encoded_size(
                    error,
                    crate::NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES / 2,
                )
                .map(|_| ()),
            };
            if valid.is_err() {
                result = Err("selected native tool result exceeds byte limit".into());
                reason = ToolCallEndReason::ResultTooLarge;
            }
        }
        // Count without an encoded allocation before making the immutable copy.
        let size = match &result {
            Ok(value) => encoded_bytes(value, NativeToolCallBatch::MAX_RETAINED_BYTES),
            Err(error) => encoded_bytes(error, NativeToolCallBatch::MAX_RETAINED_BYTES / 2),
        };
        if size.is_err() {
            result = Err("native tool result exceeds byte limit".into());
            reason = ToolCallEndReason::ResultTooLarge;
        }
        let (state, result, error) = match result {
            Ok(result) => (ToolCallTerminalState::Completed, result, None),
            Err(error) => (
                ToolCallTerminalState::Failed,
                json!({"error":error}),
                Some(error),
            ),
        };
        let data = FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: self.call.function_name.clone(),
            tool_call_id: self.call.tool_call_id.clone(),
            arguments: self.call.arguments.clone(),
            result,
            run_llm: None,
            is_final: true,
            error,
        });
        self.publish(data, state, effects, reason, false)
    }
    fn publish(
        self,
        mut data: FrameData,
        mut state: ToolCallTerminalState,
        effects: ToolCallEffectStatus,
        mut reason: ToolCallEndReason,
        mut run_llm: bool,
    ) -> ToolCallReceipt {
        if self.progress_terminal.is_some() {
            return self.publish_selected(data, state, effects, reason, run_llm);
        }
        let ctx = &self.context;
        ctx.with_admission(&ctx.runtime, || {
            let generation = self
                .cancel_on_interruption
                .then(|| ctx.current_generation())
                .flatten();
            let stale = self.cancel_on_interruption
                && (ctx.source_is_stale(generation)
                    || ctx.origins.iter().any(|origin| origin.is_stale())
                    || generation.is_some_and(|generation| {
                        ctx.has_downstream_processor()
                            && ctx.runtime.queues[ctx.index + 1].generation() > generation
                    }));
            if state != ToolCallTerminalState::Cancelled && stale {
                data = FrameData::FunctionCallCancel(FunctionCallCancelFrame {
                    function_name: self.call.function_name.clone(),
                    tool_call_id: self.call.tool_call_id.clone(),
                    run_llm: false,
                });
                state = ToolCallTerminalState::Cancelled;
                reason = ToolCallEndReason::StaleResult;
                run_llm = false;
            }
            let mut frame = Frame::new(data).with_interruptible(false);
            let outcome = Arc::new(NativeToolCallOutcome {
                id: frame.id,
                call: self.call.clone(),
                cancel_on_interruption: self.cancel_on_interruption,
                state,
                reason,
                effects,
                run_llm,
                batch: self.batch.clone(),
                progress_terminal: None,
                journal_only: false,
                progress_summary: None,
                result: match &frame.data {
                    FrameData::FunctionCallResult(result) => Some(TerminalResult {
                        result: result.result.clone(),
                        error: result.error.clone(),
                        run_llm: result.run_llm,
                        selected_retained: 0,
                        bytes: encoded_bytes(
                            &(&result.result, &result.error, result.run_llm),
                            usize::MAX,
                        )
                        .unwrap_or(usize::MAX),
                    }),
                    _ => None,
                },
            });
            if ctx.runtime.is_finished() {
                return ToolCallReceipt {
                    outcome,
                    admission: ToolOutcomeAdmission::Closed,
                };
            }
            // Terminal tool receipts survive the next interruption after admission.
            // Admission of a result is nevertheless fenced by its original scope.
            frame.attach_tool_outcome(outcome.clone());
            let admission = match ctx.runtime.route_unlocked(
                ctx.index,
                generation,
                frame,
                Direction::Downstream,
            ) {
                Ok(()) if ctx.runtime.is_finished() => ToolOutcomeAdmission::ShutdownRace,
                Ok(()) => ToolOutcomeAdmission::Admitted,
                Err(error) => {
                    // A closed route must not leave a live session waiting forever.
                    if !ctx.runtime.is_finished() {
                        ctx.runtime.fail(crate::PipelineError::Execution(format!(
                            "tool outcome was not admitted: {error}"
                        )));
                    }
                    ToolOutcomeAdmission::Undelivered(error.to_string())
                }
            };
            ToolCallReceipt { outcome, admission }
        })
    }
}

impl ToolOutcomePublisher {
    fn publish_selected(
        self,
        data: FrameData,
        state: ToolCallTerminalState,
        effects: ToolCallEffectStatus,
        reason: ToolCallEndReason,
        run_llm: bool,
    ) -> ToolCallReceipt {
        let mut frame = Frame::new(data).with_interruptible(false);
        let outcome = Arc::new(NativeToolCallOutcome {
            id: frame.id,
            call: self.call,
            cancel_on_interruption: self.cancel_on_interruption,
            state,
            reason,
            effects,
            run_llm,
            batch: self.batch,
            progress_summary: self
                .progress_terminal
                .as_ref()
                .map(crate::NativeToolProgressTerminal::summary),
            progress_terminal: self.progress_terminal,
            journal_only: false,
            result: match &frame.data {
                FrameData::FunctionCallResult(result) => Some(TerminalResult {
                    result: result.result.clone(),
                    error: result.error.clone(),
                    run_llm: result.run_llm,
                    selected_retained: crate::tool_progress::measured_json_retention(
                        &result.result,
                        crate::NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES,
                    )
                    .unwrap_or(usize::MAX)
                    .saturating_add(result.error.as_ref().map_or(0, String::capacity)),
                    bytes: encoded_bytes(
                        &(&result.result, &result.error, result.run_llm),
                        usize::MAX,
                    )
                    .unwrap_or(usize::MAX),
                }),
                _ => None,
            },
        });
        frame.attach_tool_outcome(outcome.clone());
        // Last owners of frame payload and seal remain outside the admission lock.
        let retained = frame.clone();
        let deferred = self.context.with_admission(&self.context.runtime, || {
            self.context.runtime.route_tool_progress_unlocked(
                self.context.index,
                frame,
                Direction::Downstream,
            )
        });
        let admission = if deferred.accepted() {
            ToolOutcomeAdmission::Admitted
        } else if self.context.runtime.is_finished() {
            ToolOutcomeAdmission::Closed
        } else {
            ToolOutcomeAdmission::Undelivered("selected terminal queue rejected".into())
        };
        let _ = deferred.finish();
        drop(retained);
        ToolCallReceipt { outcome, admission }
    }
}

impl ToolCallReceipt {
    /// Journal-only selected projection. It preserves immutable call identity,
    /// final choice and publication status, but owns neither result, full batch,
    /// progress authority nor the terminal payload reservation. It cannot be
    /// attached or accepted as native frame authority.
    pub fn compact_selected_journal(&self) -> Self {
        if self.outcome.progress_terminal.is_none() {
            return self.clone();
        }
        let outcome = &self.outcome;
        Self {
            admission: self.admission.clone(),
            outcome: Arc::new(NativeToolCallOutcome {
                id: outcome.id,
                call: outcome.call.clone(),
                cancel_on_interruption: outcome.cancel_on_interruption,
                state: outcome.state,
                reason: outcome.reason,
                effects: outcome.effects,
                run_llm: outcome.run_llm,
                batch: None,
                result: None,
                progress_terminal: None,
                journal_only: true,
                progress_summary: outcome
                    .progress_terminal
                    .as_ref()
                    .map(crate::NativeToolProgressTerminal::summary),
            }),
        }
    }
}

impl NativeToolCallBatch {
    pub(crate) fn validate_progress_bounds(&self) -> Result<(), ProcessorError> {
        let mut retained = self.group_id.capacity().saturating_add(
            self.members
                .capacity()
                .saturating_mul(std::mem::size_of::<(Arc<FunctionCall>, bool)>()),
        );
        for (call, _) in &self.members {
            crate::NativeToolProgressRoute::validate_call(call)?;
            retained = retained
                .checked_add(call.function_name.capacity())
                .and_then(|bytes| bytes.checked_add(call.tool_call_id.capacity()))
                .and_then(|bytes| {
                    crate::tool_progress::measured_json_retention(&call.arguments, 32 * 1024)
                        .ok()
                        .and_then(|count| bytes.checked_add(count))
                })
                .filter(|bytes| *bytes <= Self::MAX_RETAINED_BYTES)
                .ok_or_else(|| {
                    ProcessorError::new("selected shared batch retained capacity exceeded")
                })?;
        }
        if retained > Self::MAX_RETAINED_BYTES {
            return Err(ProcessorError::new(
                "selected batch retained capacity exceeded",
            ));
        }
        Ok(())
    }
}
