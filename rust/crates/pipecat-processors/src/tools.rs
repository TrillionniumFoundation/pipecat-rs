//! Registered native function callbacks with bounded admission and typed settlement.
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, FunctionCall,
    FunctionCallInProgressFrame, LlmContinuationBlock, NativeLlmContinuation, NativeToolCallBatch,
    NativeToolProgressFailure, NativeToolProgressLease, NativeToolProgressRole,
    NativeToolProgressRoute, NativeToolProgressTerminalChoice, ProcessorContext, ProcessorError,
    StartFrame, ToolCallEffectStatus, ToolCallEndReason, ToolCallReceipt, ToolOutcomeAdmission,
    ToolOutcomePublisher,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

pub type ToolFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;
pub type ToolCallback = Arc<dyn Fn(FunctionCall) -> ToolFuture + Send + Sync>;
pub use pipecat_core::{ToolProgressReceipt, ToolProgressReporter};
/// A reporting callback runs in the same owned task as its final return. Waiting
/// for an actual history receipt spends, rather than renews, its execution budget.
pub type ReportingToolCallback =
    Arc<dyn Fn(FunctionCall, ToolProgressReporter) -> ToolFuture + Send + Sync>;

/// Opt-in native asynchronous tool. The existing `RegisteredTool` literal and
/// callback signature deliberately remain unchanged.
#[derive(Clone)]
pub struct ReportingTool {
    pub callback: ReportingToolCallback,
    pub timeout: Duration,
    /// Reporting is only supported for non-interruptible asynchronous tools.
    /// Registration rejects `true`; explicit cancellation still stops the task.
    pub cancel_on_interruption: bool,
}
impl ReportingTool {
    pub fn new(callback: ReportingToolCallback) -> Self {
        Self {
            callback,
            timeout: Duration::from_secs(30),
            cancel_on_interruption: false,
        }
    }
}

#[derive(Clone)]
pub struct RegisteredTool {
    pub callback: ToolCallback,
    pub timeout: Duration,
    pub cancel_on_interruption: bool,
}
impl RegisteredTool {
    pub fn new(callback: ToolCallback) -> Self {
        Self {
            callback,
            timeout: Duration::from_secs(30),
            cancel_on_interruption: true,
        }
    }
}
/// Active settlements cannot be evicted. Terminal receipts retain the latest
/// `duplicate_window` entries; counters include entries no longer retained.
#[derive(Debug, Clone)]
pub struct ToolOutcomeSnapshot {
    pub active: Vec<FunctionCall>,
    pub terminal: Vec<ToolCallReceipt>,
    pub total_terminal: u64,
    pub evicted_terminal: u64,
    pub unadmitted_terminal: u64,
    pub retained_payload_bytes: usize,
    pub payload_byte_limit: usize,
    pub recent_identity_bytes: usize,
    pub evicted_identities: u64,
}
#[derive(Clone)]
pub struct ToolOutcomeJournal(Arc<Mutex<History>>);
impl ToolOutcomeJournal {
    pub fn snapshot(&self) -> ToolOutcomeSnapshot {
        let history = self.0.lock().unwrap_or_else(|error| error.into_inner());
        ToolOutcomeSnapshot {
            active: history
                .active
                .values()
                .map(|active| active.call.as_ref().clone())
                .collect(),
            terminal: history
                .terminal
                .iter()
                .map(|(receipt, _)| receipt.clone())
                .collect(),
            total_terminal: history.total_terminal,
            evicted_terminal: history.evicted_terminal,
            unadmitted_terminal: history.unadmitted_terminal,
            retained_payload_bytes: history.active_bytes + history.terminal_bytes,
            payload_byte_limit: ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES,
            recent_identity_bytes: history.recent_bytes,
            evicted_identities: history.evicted_identities,
        }
    }
}
/// Duplicate prevention retains at most `duplicate_window` call IDs and
/// `MAX_RECENT_IDENTITY_BYTES`. IDs can execute again after eviction. Exactly-once
/// settlement is per admitted lease, not durable or external exactly-once execution.
/// Callbacks must be cancellation-safe. Dropping them cannot undo external effects.
/// Every admitted identity first reserves a finite journal slot. Full pipeline
/// lanes fail the session closed; inspect `outcomes()` for undelivered receipts.
pub struct ToolExecutor {
    tools: BTreeMap<String, RegisteredTool>,
    reporting_tools: BTreeMap<String, ReportingTool>,
    progress_route: Option<NativeToolProgressRoute>,
    slots: Arc<Semaphore>,
    settlement_slots: Arc<Semaphore>,
    seen: Arc<Mutex<History>>,
    duplicate_window: usize,
    max_argument_bytes: usize,
    max_batch: usize,
}
impl ToolExecutor {
    /// Aggregate encoded identity/argument bytes retained by active and terminal receipts.
    pub const MAX_OUTCOME_PAYLOAD_BYTES: usize = 8 * 1024 * 1024;
    /// Separate finite byte bound for recent duplicate identities.
    pub const MAX_RECENT_IDENTITY_BYTES: usize = 8 * 1024 * 1024;
    pub fn new(
        max_in_flight: usize,
        duplicate_window: usize,
        max_argument_bytes: usize,
    ) -> Result<Self, ProcessorError> {
        let settlement_capacity = max_in_flight
            .checked_mul(2)
            .filter(|value| *value <= u32::MAX as usize && *value <= Semaphore::MAX_PERMITS);
        if max_in_flight == 0
            || duplicate_window == 0
            || max_argument_bytes == 0
            || settlement_capacity.is_none()
        {
            return Err(ProcessorError::new(
                "tool limits must be positive and bounded",
            ));
        }
        Ok(Self {
            tools: BTreeMap::new(),
            reporting_tools: BTreeMap::new(),
            progress_route: None,
            slots: Arc::new(Semaphore::new(max_in_flight)),
            settlement_slots: Arc::new(Semaphore::new(settlement_capacity.unwrap())),
            seen: Arc::new(Mutex::new(History::default())),
            duplicate_window,
            max_argument_bytes,
            max_batch: max_in_flight,
        })
    }
    /// Select the complete same-runtime native progress route. The provider's
    /// Start validates every actual role, ordering and shutdown budget before
    /// any registered callback can run.
    pub fn with_progress_route(
        mut self,
        route: NativeToolProgressRoute,
    ) -> Result<Self, ProcessorError> {
        if self.progress_route.is_some() || self.max_batch > 16 {
            return Err(ProcessorError::new(
                "invalid selected tool progress capacity or rebinding",
            ));
        }
        for tool in self.tools.values() {
            validate_selected_timeout(tool.timeout)?;
        }
        self.progress_route = Some(route);
        Ok(self)
    }
    pub fn register_reporting(
        &mut self,
        name: impl Into<String>,
        tool: ReportingTool,
    ) -> Result<(), ProcessorError> {
        let name = name.into();
        validate_selected_timeout(tool.timeout)?;
        if self.progress_route.is_none()
            || name.is_empty()
            || tool.cancel_on_interruption
            || self.tools.contains_key(&name)
            || self.reporting_tools.contains_key(&name)
        {
            return Err(ProcessorError::new(
                "invalid or duplicate reporting tool registration",
            ));
        }
        self.reporting_tools.insert(name, tool);
        Ok(())
    }
    fn cancel_on_interruption(&self, name: &str) -> bool {
        !self.reporting_tools.contains_key(name)
            && self
                .tools
                .get(name)
                .is_none_or(|tool| tool.cancel_on_interruption)
    }
    fn admission_error(&self, message: impl Into<String>) -> ProcessorError {
        if self.progress_route.is_some() {
            ProcessorError::fatal(message)
        } else {
            ProcessorError::new(message)
        }
    }
    fn timeout(&self, name: &str) -> Duration {
        self.reporting_tools.get(name).map_or_else(
            || {
                self.tools
                    .get(name)
                    .map_or(Duration::from_millis(1), |tool| tool.timeout)
            },
            |tool| tool.timeout,
        )
    }
    pub fn outcomes(&self) -> ToolOutcomeJournal {
        ToolOutcomeJournal(self.seen.clone())
    }
    pub fn register(
        &mut self,
        name: impl Into<String>,
        tool: RegisteredTool,
    ) -> Result<(), ProcessorError> {
        let name = name.into();
        if self.progress_route.is_some() {
            validate_selected_timeout(tool.timeout)?;
        }
        if name.is_empty()
            || tool.timeout.is_zero()
            || self.tools.contains_key(&name)
            || self.reporting_tools.contains_key(&name)
        {
            return Err(ProcessorError::new(
                "invalid or duplicate tool registration",
            ));
        }
        self.tools.insert(name, tool);
        Ok(())
    }
    fn reserve(
        &self,
        calls: &[FunctionCall],
        signed: Option<Arc<NativeLlmContinuation>>,
        group_id: &str,
        ctx: &ProcessorContext,
    ) -> Result<Vec<ActiveLease>, ProcessorError> {
        let mut seen = self
            .seen
            .lock()
            .map_err(|_| ProcessorError::fatal("tool history poisoned"))?;
        let retained: BTreeSet<&str> = seen
            .active
            .keys()
            .chain(seen.recent.iter())
            .map(String::as_str)
            .collect();
        let live: BTreeMap<_, _> = seen.signed.iter().filter(|(_, history)| history.blocks().iter().any(|block|
            matches!(block, LlmContinuationBlock::ToolCall(call) if retained.contains(call.tool_call_id.as_str()))))
            .map(|(id, history)| (*id, history.clone())).collect();
        let fresh: Vec<&FunctionCall> = if let Some(history) = &signed {
            if let Some(previous) = live.get(&history.response_id()) {
                return if previous.as_ref() == history.as_ref() {
                    Ok(vec![])
                } else {
                    Err(ProcessorError::new("conflicting signed response identity"))
                };
            }
            if calls
                .iter()
                .any(|call| retained.contains(call.tool_call_id.as_str()))
            {
                return Err(ProcessorError::new(
                    "partially duplicate or conflicting signed tool batch",
                ));
            }
            if live
                .values()
                .try_fold(history.retained_bytes(), |bytes, entry| {
                    bytes.checked_add(entry.retained_bytes())
                })
                .is_none_or(|bytes| bytes > NativeLlmContinuation::MAX_RETAINED_BYTES)
            {
                return Err(ProcessorError::new(
                    "aggregate signed tool history exceeds byte limit",
                ));
            }
            calls.iter().collect()
        } else {
            let mut unique = retained.clone();
            calls
                .iter()
                .filter(|call| unique.insert(call.tool_call_id.as_str()))
                .collect()
        };
        drop(retained);
        if fresh.is_empty() {
            return Ok(vec![]);
        }
        // Reserve every reachable shared batch conservatively for each lease.
        // A sibling can retain that batch after any other member has settled.
        let members: Vec<_> = fresh
            .iter()
            .map(|call| (*call, self.cancel_on_interruption(&call.function_name)))
            .collect();
        let batch_bytes = NativeToolCallBatch::measure(group_id, &members)?;
        let sizes: Vec<_> = members
            .iter()
            .map(|(call, policy)| {
                NativeToolCallBatch::policy_bytes(call, *policy, Some(group_id)).and_then(|bytes| {
                    batch_bytes
                        .checked_add(bytes)
                        .and_then(|bytes| {
                            bytes.checked_add(call.tool_call_id.len().saturating_mul(2))
                        })
                        .ok_or_else(|| ProcessorError::new("tool tracking byte overflow"))
                })
            })
            .collect::<Result<_, _>>()?;
        let needed = sizes
            .iter()
            .try_fold(0_usize, |total, bytes| total.checked_add(*bytes))
            .ok_or_else(|| ProcessorError::fatal("tool outcome byte budget overflow"))?;
        let active_bytes = seen
            .active_bytes
            .checked_add(needed)
            .filter(|bytes| *bytes <= Self::MAX_OUTCOME_PAYLOAD_BYTES)
            .ok_or_else(|| {
                ProcessorError::fatal("active tool outcomes exceed aggregate byte limit")
            })?;
        // Both count and byte reservations precede identity publication and payload cloning.
        let mut reservations = self
            .settlement_slots
            .clone()
            .try_acquire_many_owned(fresh.len() as u32)
            .map_err(|_| ProcessorError::fatal("tool settlement capacity exceeded"))?;
        let batch = NativeToolCallBatch::new(
            group_id.into(),
            members
                .into_iter()
                .map(|(call, policy)| (Arc::new(call.clone()), policy))
                .collect(),
        )?;
        let mut publishers = Vec::with_capacity(fresh.len());
        for (call, interruptible) in batch.members() {
            let publisher = ctx
                .tool_outcome_publisher(call.clone(), *interruptible)?
                .with_batch(batch.clone())?;
            publishers.push((call.clone(), publisher));
        }
        // Core reserves the entire selected batch atomically, including ordinary
        // siblings and each separate terminal permit. No partial route identity
        // survives a failed batch reservation.
        let mut selected_leases = if let Some(route) = &self.progress_route {
            let policies = batch
                .members()
                .iter()
                .map(|(call, _)| {
                    (
                        self.reporting_tools.contains_key(&call.function_name),
                        self.timeout(&call.function_name),
                    )
                })
                .collect();
            Some(route.admit_batch(ctx, batch.clone(), policies)?.into_iter())
        } else {
            None
        };
        while active_bytes + seen.terminal_bytes > Self::MAX_OUTCOME_PAYLOAD_BYTES {
            seen.evict_terminal();
        }
        seen.active_bytes = active_bytes;
        seen.signed = live;
        if let Some(history) = signed {
            seen.signed.insert(history.response_id(), history);
        }
        let mut leases = Vec::with_capacity(publishers.len());
        for ((call, publisher), bytes) in publishers.into_iter().zip(sizes) {
            let (publisher, selected) = if let Some(leases) = &mut selected_leases {
                let lease = leases.next().expect("atomically reserved selected batch");
                (
                    None,
                    Some(Arc::new(SelectedSettlement {
                        lease,
                        route: self
                            .progress_route
                            .as_ref()
                            .expect("selected route")
                            .clone(),
                        history: Arc::downgrade(&self.seen),
                        retention: self.duplicate_window,
                        control: Mutex::new(SelectedControl {
                            publisher: Some(publisher),
                            entered: false,
                            task: None,
                        }),
                    })),
                )
            } else {
                (Some(publisher), None)
            };
            let control = Arc::new(Mutex::new(Control::default()));
            let active = ActiveCall {
                call: call.clone(),
                bytes,
                control: control.clone(),
                context: ctx.clone(),
                selected: selected.clone(),
            };
            while seen.recent.len() >= self.duplicate_window
                || seen.recent_bytes + call.tool_call_id.len() > Self::MAX_RECENT_IDENTITY_BYTES
            {
                let Some(old) = seen.recent.pop_front() else {
                    break;
                };
                seen.recent_bytes -= old.len();
                seen.evicted_identities = seen.evicted_identities.saturating_add(1);
            }
            seen.recent_bytes += call.tool_call_id.len();
            seen.recent.push_back(call.tool_call_id.clone());
            seen.active.insert(call.tool_call_id.clone(), active);
            leases.push(ActiveLease {
                call,
                publisher,
                selected,
                control,
                history: self.seen.clone(),
                retention: self.duplicate_window,
                _reservation: reservations.split(1).expect("reserved complete batch"),
            });
        }
        Ok(leases)
    }
    fn dispatch(
        &self,
        mut lease: ActiveLease,
        ctx: &ProcessorContext,
        admission: Option<pipecat_core::NativeSpeculation>,
    ) -> Result<(), ProcessorError> {
        if lease.selected.is_some() {
            return self.dispatch_selected(lease, ctx);
        }
        let Some(tool) = self.tools.get(&lease.call.function_name).cloned() else {
            lease.complete(
                Err("tool is not registered".into()),
                ToolCallEndReason::NotRegistered,
            );
            return Ok(());
        };
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                lease.complete(
                    Err("tool concurrency capacity exceeded".into()),
                    ToolCallEndReason::ConcurrencyLimit,
                );
                return Ok(());
            }
        };
        let control = lease.control.clone();
        // The settlement lease is handed over only AFTER spawn releases runtime
        // admission locks. Rejected futures therefore cannot deadlock in Drop.
        let (handoff, receive) = oneshot::channel::<ActiveLease>();
        let task = ctx.spawn("tool-call", tool.cancel_on_interruption, async move {
            let _permit = permit;
            let Ok(mut lease) = receive.await else {
                return Ok(());
            };
            let call = lease.call.as_ref().clone();
            let callback = std::panic::AssertUnwindSafe(async {
                if !lease.enter() {
                    return None;
                }
                Some((tool.callback)(call).await)
            })
            .catch_unwind();
            let guarded = async {
                match &admission {
                    Some(capability) => capability.run_effect(callback).await,
                    None => Some(callback.await),
                }
            };
            match tokio::time::timeout(tool.timeout, guarded).await {
                Ok(Some(Ok(Some(result)))) => {
                    let reason = if result.is_ok() {
                        ToolCallEndReason::CallbackReturned
                    } else {
                        ToolCallEndReason::CallbackError
                    };
                    lease.complete(result, reason);
                }
                Ok(Some(Err(_))) => lease.complete(
                    Err("tool callback panicked".into()),
                    ToolCallEndReason::CallbackPanicked,
                ),
                Err(_) => lease.complete(
                    Err("tool call timed out".into()),
                    ToolCallEndReason::TimedOut,
                ),
                Ok(None) => lease.cancel(ToolCallEndReason::SpeculationDenied),
                Ok(Some(Ok(None))) => lease.cancel(ToolCallEndReason::ExplicitCancel),
            }
            Ok(())
        });
        match task {
            Ok(id) => {
                let cancel = {
                    let mut state = control.lock().unwrap_or_else(|error| error.into_inner());
                    state.task = Some(id);
                    state.cancel.is_some()
                };
                if cancel {
                    ctx.cancel_task(id);
                }
                // A dropped receiver returns the still-owned lease to this stack.
                if let Err(mut lease) = handoff.send(lease) {
                    lease.cancel(ToolCallEndReason::TaskDropped);
                }
                Ok(())
            }
            Err(error) => {
                lease.cancel(ToolCallEndReason::AdmissionRejected);
                Err(error)
            }
        }
    }
    fn dispatch_selected(
        &self,
        mut lease: ActiveLease,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let callback = if let Some(tool) = self.reporting_tools.get(&lease.call.function_name) {
            SelectedCallback::Reporting(tool.clone())
        } else if let Some(tool) = self.tools.get(&lease.call.function_name) {
            SelectedCallback::Final(tool.clone())
        } else {
            lease.complete(
                Err("tool is not registered".into()),
                ToolCallEndReason::NotRegistered,
            );
            return Ok(());
        };
        let permit = match self.slots.clone().try_acquire_owned() {
            Ok(permit) => permit,
            Err(_) => {
                lease.complete(
                    Err("tool concurrency capacity exceeded".into()),
                    ToolCallEndReason::ConcurrencyLimit,
                );
                return Ok(());
            }
        };
        let selected = lease
            .selected
            .as_ref()
            .expect("selected settlement")
            .clone();
        let interruptible = callback.cancel_on_interruption();
        let (handoff, receive) = oneshot::channel::<ActiveLease>();
        let task = ctx.spawn("tool-call", interruptible, async move {
            let _permit = permit;
            let Ok(mut lease) = receive.await else {
                return Ok(());
            };
            // Capture exactly once after receiving the owned lease and before
            // constructing or polling user code. Receipt waits share this timer.
            let deadline = match lease
                .selected
                .as_ref()
                .expect("selected settlement")
                .lease
                .start_execution()
            {
                Ok(deadline) => deadline,
                Err(_) => {
                    lease.cancel(ToolCallEndReason::AdmissionRejected);
                    return Ok(());
                }
            };
            let call = lease.call.as_ref().clone();
            let reporter = lease
                .selected
                .as_ref()
                .expect("selected settlement")
                .lease
                .reporter();
            let callback = std::panic::AssertUnwindSafe(async {
                if !lease.enter() {
                    return None;
                }
                Some(match callback {
                    SelectedCallback::Final(tool) => (tool.callback)(call).await,
                    SelectedCallback::Reporting(tool) => (tool.callback)(call, reporter).await,
                })
            })
            .catch_unwind();
            match tokio::time::timeout_at(deadline, callback).await {
                Ok(Ok(Some(result))) => {
                    let reason = if result.is_ok() {
                        ToolCallEndReason::CallbackReturned
                    } else {
                        ToolCallEndReason::CallbackError
                    };
                    lease.complete(result, reason);
                }
                Ok(Err(_)) => lease.complete(
                    Err("tool callback panicked".into()),
                    ToolCallEndReason::CallbackPanicked,
                ),
                Err(_) => lease.complete(
                    Err("tool call timed out".into()),
                    ToolCallEndReason::TimedOut,
                ),
                Ok(Ok(None)) => lease.cancel(ToolCallEndReason::ExplicitCancel),
            }
            Ok(())
        });
        match task {
            Ok(id) => {
                let cancelled = selected.set_task(id);
                if let Err(error) = selected.lease.set_task(id) {
                    lease.cancel(ToolCallEndReason::AdmissionRejected);
                    ctx.cancel_task(id);
                    return Err(error);
                }
                if cancelled {
                    ctx.cancel_task(id);
                }
                if let Err(mut lease) = handoff.send(lease) {
                    lease.cancel(ToolCallEndReason::TaskDropped);
                }
                Ok(())
            }
            Err(error) => {
                lease.cancel(ToolCallEndReason::AdmissionRejected);
                Err(error)
            }
        }
    }
}

fn validate_selected_timeout(timeout: Duration) -> Result<(), ProcessorError> {
    if !(Duration::from_millis(1)..=Duration::from_secs(300)).contains(&timeout) {
        return Err(ProcessorError::new(
            "selected tool timeout must be 1 ms to 300 s",
        ));
    }
    Ok(())
}

// The selected borrowed guard has already bounded recursion and retained shape.
// Count encoded bytes without allocating a serialized argument buffer.
fn selected_arguments_fit(value: &Value, limit: usize) -> Result<(), std::io::Error> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> Result<usize, std::io::Error> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= self.limit)
                .ok_or_else(|| {
                    std::io::Error::other("selected arguments exceed configured limit")
                })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> Result<(), std::io::Error> {
            Ok(())
        }
    }
    serde_json::to_writer(&mut Counter { bytes: 0, limit }, value).map_err(std::io::Error::other)
}

enum SelectedCallback {
    Final(RegisteredTool),
    Reporting(ReportingTool),
}
impl SelectedCallback {
    fn cancel_on_interruption(&self) -> bool {
        match self {
            Self::Final(tool) => tool.cancel_on_interruption,
            Self::Reporting(tool) => tool.cancel_on_interruption,
        }
    }
}

#[async_trait]
impl FrameProcessor for ToolExecutor {
    fn name(&self) -> &str {
        "ToolExecutor"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(route) = &self.progress_route {
            let largest_timeout = self
                .tools
                .values()
                .map(|tool| tool.timeout)
                .chain(self.reporting_tools.values().map(|tool| tool.timeout))
                .max()
                .unwrap_or(Duration::from_millis(1));
            validate_selected_timeout(largest_timeout)?;
            route.register_executor_limits(self.max_batch, largest_timeout)?;
            route.register_role(NativeToolProgressRole::Executor, ctx, None)?;
        }
        Ok(())
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        // Public variants cannot be used to evade a retained private carrier.
        // Validate before the batch shortcut, cancellation or callback admission.
        frame.native_tool_progress()?;
        frame.native_tool_progress_admission()?;
        frame.native_tool_outcome()?;
        if frame.native_tool_batch()?.is_some() {
            return ctx.push(frame, direction).await;
        }
        let admission = frame.native_speculation().cloned();
        let signed = frame
            .native_llm_continuation()
            .map_err(|error| ProcessorError::new(error.to_string()))?
            .cloned();
        if let FrameData::FunctionCallCancel(cancel) = &frame.data {
            // A verified acknowledgment is data, never a fresh cancellation command.
            if frame.native_tool_outcome()?.is_some() {
                return ctx.push(frame, direction).await;
            }
            if self.progress_route.is_some() {
                // History is only an index. Never hold it while freezing a lease,
                // publishing a terminal, recording the journal or cancelling a task.
                let target = {
                    let history = self
                        .seen
                        .lock()
                        .map_err(|_| ProcessorError::fatal("tool history poisoned"))?;
                    match history.active.get(&cancel.tool_call_id) {
                        Some(active) if active.call.function_name != cancel.function_name => {
                            return Err(ProcessorError::new("tool cancellation identity mismatch"));
                        }
                        Some(active) => Some((
                            active.context.clone(),
                            active
                                .selected
                                .as_ref()
                                .expect("selected active settlement")
                                .clone(),
                        )),
                        None => None,
                    }
                };
                if let Some((origin, selected)) = target {
                    // The winner is frozen and published, then recorded, before
                    // aborting the owned callback. A later cancel cannot rewrite it.
                    if let Some(task) =
                        selected.cancel(ToolCallEndReason::ExplicitCancel, cancel.run_llm)
                    {
                        origin.cancel_task(task);
                    }
                }
                return Ok(());
            }
            let target = {
                let history = self
                    .seen
                    .lock()
                    .map_err(|_| ProcessorError::fatal("tool history poisoned"))?;
                if history
                    .active
                    .get(&cancel.tool_call_id)
                    .is_some_and(|active| active.call.function_name != cancel.function_name)
                {
                    return Err(ProcessorError::new("tool cancellation identity mismatch"));
                }
                history.active.get(&cancel.tool_call_id).map(|active| {
                    let mut control = active
                        .control
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    control.cancel.get_or_insert(cancel.run_llm);
                    (active.context.clone(), control.task)
                })
            };
            if let Some((origin, task)) = target {
                if let Some(task) = task {
                    origin.cancel_task(task);
                }
                return Ok(());
            }
            // Unknown or already-settled identities cannot produce a new receipt.
            return Ok(());
        }
        let calls = match (&frame.data, direction) {
            (FrameData::FunctionCallsStarted(calls), Direction::Downstream) => calls,
            _ => return ctx.push(frame, direction).await,
        };
        if let Some(route) = &self.progress_route {
            if !route.accepts_new_calls() {
                return Err(ProcessorError::fatal(
                    "selected tool call arrived outside open admission",
                ));
            }
            if signed.is_some() || admission.is_some() {
                return Err(ProcessorError::fatal(
                    "selected reporting route requires unsigned non-speculative calls",
                ));
            }
        }
        if admission
            .as_ref()
            .is_some_and(|capability| !capability.admits_effect())
        {
            return Ok(());
        }
        if calls.len() > self.max_batch {
            return Err(self.admission_error("tool batch exceeds configured limit"));
        }
        for call in calls {
            if call.tool_call_id.is_empty()
                || call.function_name.is_empty()
                || !call.arguments.is_object()
            {
                return Err(self.admission_error("invalid tool call identity or arguments"));
            }
            if call.tool_call_id.len() > self.max_argument_bytes
                || call.function_name.len() > self.max_argument_bytes
            {
                return Err(
                    self.admission_error("tool arguments or identity exceed configured limit")
                );
            }
            let arguments_too_large = if self.progress_route.is_some() {
                // Validate borrowed structure before legacy serialization or
                // any batch/call clone. This also checks selected retained capacity.
                NativeToolProgressRoute::validate_call(call)
                    .map_err(|error| ProcessorError::fatal(error.to_string()))?;
                selected_arguments_fit(&call.arguments, self.max_argument_bytes).is_err()
            } else {
                serde_json::to_vec(&call.arguments)
                    .map_err(|error| ProcessorError::new(error.to_string()))?
                    .len()
                    > self.max_argument_bytes
            };
            if arguments_too_large {
                return Err(
                    self.admission_error("tool arguments or identity exceed configured limit")
                );
            }
        }
        let signed_duplicate = signed.is_some();
        let group_id = format!("tool-batch-{}", Frame::new(FrameData::LlmRun).id);
        let leases = self
            .reserve(calls, signed, &group_id, ctx)
            .map_err(|error| {
                if self.progress_route.is_some() {
                    ProcessorError::fatal(error.to_string())
                } else {
                    error
                }
            })?;
        frame.take_native_speculation();
        if leases.is_empty() {
            return if signed_duplicate {
                ctx.push(frame, direction).await
            } else {
                Ok(())
            };
        }
        let batch = leases[0].batch().clone();
        frame.data = FrameData::FunctionCallsStarted(
            leases
                .iter()
                .map(|lease| lease.call.as_ref().clone())
                .collect(),
        );
        frame.set_native_tool_batch(batch.clone())?;
        ctx.push(frame, direction).await?;
        for lease in &leases {
            let mut frame = Frame::new(FrameData::FunctionCallInProgress(
                FunctionCallInProgressFrame {
                    function_name: lease.call.function_name.clone(),
                    tool_call_id: lease.call.tool_call_id.clone(),
                    arguments: lease.call.arguments.clone(),
                    cancel_on_interruption: self.cancel_on_interruption(&lease.call.function_name),
                    group_id: Some(group_id.clone()),
                },
            ));
            frame.set_native_tool_batch(batch.clone())?;
            if let Some(selected) = &lease.selected {
                frame.set_native_tool_progress_admission(selected.lease.admission())?;
            }
            ctx.push(frame, Direction::Downstream).await?;
        }
        for lease in leases {
            self.dispatch(lease, ctx, admission.clone())?;
        }
        Ok(())
    }
}
#[derive(Default)]
struct History {
    recent: VecDeque<String>,
    active: BTreeMap<String, ActiveCall>,
    signed: BTreeMap<u64, Arc<NativeLlmContinuation>>,
    terminal: VecDeque<(ToolCallReceipt, usize)>,
    active_bytes: usize,
    terminal_bytes: usize,
    recent_bytes: usize,
    evicted_identities: u64,
    total_terminal: u64,
    evicted_terminal: u64,
    unadmitted_terminal: u64,
}
impl History {
    fn evict_terminal(&mut self) {
        if let Some((_, bytes)) = self.terminal.pop_front() {
            self.terminal_bytes -= bytes;
            self.evicted_terminal = self.evicted_terminal.saturating_add(1);
        }
    }
}
struct ActiveCall {
    call: Arc<FunctionCall>,
    bytes: usize,
    control: Arc<Mutex<Control>>,
    context: ProcessorContext,
    selected: Option<Arc<SelectedSettlement>>,
}
#[derive(Default)]
struct Control {
    task: Option<u64>,
    entered: bool,
    cancel: Option<bool>,
}
struct ActiveLease {
    call: Arc<FunctionCall>,
    publisher: Option<ToolOutcomePublisher>,
    selected: Option<Arc<SelectedSettlement>>,
    control: Arc<Mutex<Control>>,
    history: Arc<Mutex<History>>,
    retention: usize,
    _reservation: OwnedSemaphorePermit,
}
impl ActiveLease {
    fn batch(&self) -> &Arc<NativeToolCallBatch> {
        if let Some(selected) = &self.selected {
            selected.lease.batch()
        } else {
            self.publisher
                .as_ref()
                .and_then(ToolOutcomePublisher::batch)
                .expect("reserved native batch")
        }
    }
    fn enter(&self) -> bool {
        if let Some(selected) = &self.selected {
            return selected.enter();
        }
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if control.cancel.is_some() {
            return false;
        }
        control.entered = true;
        true
    }
    fn complete(&mut self, result: Result<Value, String>, reason: ToolCallEndReason) {
        if let Some(selected) = &self.selected {
            selected.complete(result, reason);
            return;
        }
        let Some(publisher) = self.publisher.take() else {
            return;
        };
        let control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let effects = if control.entered {
            ToolCallEffectStatus::ExternalEffectsUnknown
        } else {
            ToolCallEffectStatus::NotStarted
        };
        let receipt = if let Some(run_llm) = control.cancel {
            publisher.cancel(effects, ToolCallEndReason::ExplicitCancel, run_llm)
        } else {
            publisher.complete(result, effects, reason)
        };
        drop(control);
        self.record(receipt);
    }
    fn cancel(&mut self, reason: ToolCallEndReason) {
        if let Some(selected) = &self.selected {
            let _ = selected.cancel(reason, false);
            return;
        }
        let Some(publisher) = self.publisher.take() else {
            return;
        };
        let control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let effects = if control.entered {
            ToolCallEffectStatus::ExternalEffectsUnknown
        } else {
            ToolCallEffectStatus::NotStarted
        };
        let reason = if control.cancel.is_some() {
            ToolCallEndReason::ExplicitCancel
        } else {
            reason
        };
        let receipt = publisher.cancel(effects, reason, control.cancel.unwrap_or(false));
        drop(control);
        self.record(receipt);
    }
    fn record(&self, receipt: ToolCallReceipt) {
        record_receipt(
            &self.history,
            &self.call.tool_call_id,
            self.retention,
            receipt,
        );
    }
}
fn record_receipt(
    history: &Mutex<History>,
    call_id: &str,
    retention: usize,
    receipt: ToolCallReceipt,
) {
    let mut history = history.lock().unwrap_or_else(|error| error.into_inner());
    let bytes = history
        .active
        .remove(call_id)
        .expect("owned active settlement")
        .bytes;
    history.active_bytes -= bytes;
    history.total_terminal = history.total_terminal.saturating_add(1);
    if receipt.admission != ToolOutcomeAdmission::Admitted {
        history.unadmitted_terminal = history.unadmitted_terminal.saturating_add(1);
    }
    let bytes = receipt.outcome.retained_bytes();
    while history.terminal.len() >= retention
        || history
            .active_bytes
            .saturating_add(history.terminal_bytes)
            .saturating_add(bytes)
            > ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES
    {
        if history.terminal.is_empty() {
            break;
        }
        history.evict_terminal();
    }
    if history.active_bytes.saturating_add(bytes) > ToolExecutor::MAX_OUTCOME_PAYLOAD_BYTES {
        history.evicted_terminal = history.evicted_terminal.saturating_add(1);
        return;
    }
    history.terminal_bytes += bytes;
    history.terminal.push_back((receipt, bytes));
}

impl Drop for ActiveLease {
    fn drop(&mut self) {
        self.cancel(ToolCallEndReason::TaskDropped);
    }
}

/// Only bookkeeping shares this outer lock. The core lease owns the one
/// authoritative report/freeze gate. No publisher, journal update, notification
/// or task cancellation runs while this bookkeeping lock is held.
struct SelectedSettlement {
    lease: NativeToolProgressLease,
    route: NativeToolProgressRoute,
    history: Weak<Mutex<History>>,
    retention: usize,
    control: Mutex<SelectedControl>,
}
struct SelectedControl {
    publisher: Option<ToolOutcomePublisher>,
    entered: bool,
    task: Option<u64>,
}
impl SelectedSettlement {
    fn enter(&self) -> bool {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if control.publisher.is_none() {
            return false;
        }
        control.entered = true;
        true
    }
    fn set_task(&self, task: u64) -> bool {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        control.task = Some(task);
        control.publisher.is_none()
    }
    fn complete(&self, result: Result<Value, String>, reason: ToolCallEndReason) {
        // The selected publisher validates before cloning, with the same bounded
        // terminal guard. Prevalidation here also freezes the correct error
        // reason, so an oversized success cannot retain progress permission.
        let (result, reason) = if self.lease.validate_terminal_result(&result).is_err() {
            (
                Err("native tool result exceeds selected byte limit".into()),
                ToolCallEndReason::ResultTooLarge,
            )
        } else {
            (result, reason)
        };
        let _ = self.settle(
            NativeToolProgressTerminalChoice::Complete { reason },
            Some(result),
        );
    }
    fn cancel(&self, reason: ToolCallEndReason, run_llm: bool) -> Option<u64> {
        self.settle(
            NativeToolProgressTerminalChoice::Cancel { reason, run_llm },
            None,
        )
    }
    fn settle(
        &self,
        choice: NativeToolProgressTerminalChoice,
        result: Option<Result<Value, String>>,
    ) -> Option<u64> {
        let (terminal, publisher, effects, task) = {
            let mut control = self
                .control
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            control.publisher.as_ref()?;
            let terminal = self.lease.freeze_terminal(choice)?;
            let publisher = control
                .publisher
                .take()
                .expect("winning selected publisher");
            let effects = if control.entered {
                ToolCallEffectStatus::ExternalEffectsUnknown
            } else {
                ToolCallEffectStatus::NotStarted
            };
            (terminal, publisher, effects, control.task)
        };
        let choice = terminal.choice();
        let receipt = match publisher.with_progress_terminal(terminal) {
            Ok(publisher) => match choice {
                NativeToolProgressTerminalChoice::Complete { reason } => publisher.complete(
                    result.expect("only the completing winner owns its returned value"),
                    effects,
                    reason,
                ),
                NativeToolProgressTerminalChoice::Cancel { reason, run_llm } => {
                    publisher.cancel(effects, reason, run_llm)
                }
            },
            Err(_) => {
                // A mismatch here is an internal provenance failure, never a
                // reason to retry the callback or silently lose its journal.
                self.route
                    .close(NativeToolProgressFailure::InvalidProvenance);
                let history = self.history.upgrade().expect("owned active journal");
                let (context, call) = {
                    let history = history.lock().unwrap_or_else(|error| error.into_inner());
                    let active = history
                        .active
                        .get(&self.lease.call().tool_call_id)
                        .expect("unrecorded selected winner remains active");
                    (active.context.clone(), active.call.clone())
                };
                let policy = self
                    .lease
                    .batch()
                    .members()
                    .iter()
                    .find(|(member, _)| member.as_ref() == call.as_ref())
                    .map(|(_, policy)| *policy)
                    .expect("admitted call has batch policy");
                context
                    .tool_outcome_publisher(call, policy)
                    .and_then(|publisher| publisher.with_batch(self.lease.batch().clone()))
                    .expect("original executor identity is validated")
                    .complete(
                        Err("selected terminal provenance could not be published".into()),
                        effects,
                        ToolCallEndReason::AdmissionRejected,
                    )
            }
        };
        if let Some(history) = self.history.upgrade() {
            record_receipt(
                &history,
                &self.lease.call().tool_call_id,
                self.retention,
                receipt.compact_selected_journal(),
            );
        }
        // Actual consumer dispositions, rather than this first-hop receipt or
        // an empty callback journal, decide when the route can forward End.
        if self.route.note_terminal_publication(&receipt).is_err() {
            self.route
                .close(NativeToolProgressFailure::InvalidProvenance);
        }
        task
    }
}

#[cfg(test)]
#[path = "tools_settlement_tests.rs"]
mod settlement_tests;
