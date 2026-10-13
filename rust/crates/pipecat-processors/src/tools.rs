//! Registered native function callbacks with bounded admission and typed settlement.
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, FunctionCall,
    FunctionCallInProgressFrame, LlmContinuationBlock, LocalToolCompletion, LocalToolFunction,
    LocalToolReporter, LocalToolResult, NativeLlmContinuation, NativeToolCallBatch,
    ProcessorContext, ProcessorError, ToolCallEffectStatus, ToolCallEndReason, ToolCallReceipt,
    ToolOutcomeAdmission, ToolOutcomePublisher, ToolProgressPublisher,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, BTreeSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::sync::{oneshot, OwnedSemaphorePermit, Semaphore};

pub type ToolFuture = Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>;
pub type ToolCallback = Arc<dyn Fn(FunctionCall) -> ToolFuture + Send + Sync>;
/// `None` explicitly cancels this invocation; it never means an empty result.
pub type CancellableToolFuture =
    Pin<Box<dyn Future<Output = Option<Result<Value, String>>> + Send>>;
pub type CancellableToolCallback = Arc<dyn Fn(FunctionCall) -> CancellableToolFuture + Send + Sync>;
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
/// A callback that can return a cancellation acknowledgment without inventing
/// an error string. `Some(Ok(value))` and `Some(Err(error))` retain the ordinary
/// callback behavior. `None` settles as Cancelled / ExplicitCancel and does not
/// request another LLM run, unless a racing explicit cancellation command has
/// already selected its own `run_llm` policy. Once entered, every outcome still
/// reports ExternalEffectsUnknown; cancellation cannot undo remote side effects.
#[derive(Clone)]
pub struct CancellableRegisteredTool {
    pub callback: CancellableToolCallback,
    pub timeout: Duration,
    pub cancel_on_interruption: bool,
}
impl CancellableRegisteredTool {
    pub fn new(callback: CancellableToolCallback) -> Self {
        Self {
            callback,
            timeout: Duration::from_secs(30),
            cancel_on_interruption: true,
        }
    }
}
type NormalizedToolFuture =
    Pin<Box<dyn Future<Output = Option<Result<LocalToolCompletion, String>>> + Send>>;
type NormalizedToolCallback =
    Arc<dyn Fn(FunctionCall, Option<LocalToolReporter>) -> NormalizedToolFuture + Send + Sync>;
#[derive(Clone)]
struct RegistrationRow {
    callback: NormalizedToolCallback,
    timeout: Option<Duration>,
    cancel_on_interruption: bool,
    requires_reporter: bool,
}
/// An immutable callback inventory captured from validated registrations.
/// It retains callback/options only; admission limits and outcome history remain
/// owned by the executor that processes a batch with this snapshot.
#[derive(Clone)]
pub struct ToolRegistrationSnapshot(Arc<BTreeMap<String, RegistrationRow>>);
impl ToolRegistrationSnapshot {
    /// Select only these validated registrations, preserving their callbacks
    /// and options. Unknown names reject the selection without changing either
    /// snapshot or any executor's admission state and outcome history.
    pub fn select_names(&self, names: &BTreeSet<String>) -> Result<Self, ProcessorError> {
        let selected = names
            .iter()
            .map(|name| {
                self.0
                    .get(name)
                    .cloned()
                    .map(|tool| (name.clone(), tool))
                    .ok_or_else(|| ProcessorError::new("selected tool is not registered"))
            })
            .collect::<Result<BTreeMap<_, _>, _>>()?;
        Ok(Self(Arc::new(selected)))
    }
}
impl std::fmt::Debug for ToolRegistrationSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolRegistrationSnapshot")
            .field("tool_count", &self.0.len())
            .finish_non_exhaustive()
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
    tools: BTreeMap<String, RegistrationRow>,
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
            slots: Arc::new(Semaphore::new(max_in_flight)),
            settlement_slots: Arc::new(Semaphore::new(settlement_capacity.unwrap())),
            seen: Arc::new(Mutex::new(History::default())),
            duplicate_window,
            max_argument_bytes,
            max_batch: max_in_flight,
        })
    }
    pub fn outcomes(&self) -> ToolOutcomeJournal {
        ToolOutcomeJournal(self.seen.clone())
    }
    pub fn register(
        &mut self,
        name: impl Into<String>,
        tool: RegisteredTool,
    ) -> Result<(), ProcessorError> {
        let callback = tool.callback;
        self.register_cancellable(
            name,
            CancellableRegisteredTool {
                callback: Arc::new(move |call| {
                    let callback = callback.clone();
                    Box::pin(async move { Some(callback(call).await) })
                }),
                timeout: tool.timeout,
                cancel_on_interruption: tool.cancel_on_interruption,
            },
        )
    }
    /// Register an explicitly cancellation-aware callback. Admission, limits,
    /// duplicate protection and settlement ownership match `register`.
    pub fn register_cancellable(
        &mut self,
        name: impl Into<String>,
        tool: CancellableRegisteredTool,
    ) -> Result<(), ProcessorError> {
        let name = name.into();
        if name.is_empty() || tool.timeout.is_zero() || self.tools.contains_key(&name) {
            return Err(ProcessorError::new(
                "invalid or duplicate tool registration",
            ));
        }
        let callback = tool.callback;
        self.tools.insert(
            name,
            RegistrationRow {
                callback: Arc::new(move |call, _reporter| {
                    let callback = callback.clone();
                    Box::pin(async move {
                        callback(call).await.map(|result| {
                            result.map(|value| LocalToolCompletion {
                                result: LocalToolResult {
                                    value,
                                    run_llm: None,
                                },
                                on_context_updated: None,
                            })
                        })
                    })
                }),
                timeout: Some(tool.timeout),
                cancel_on_interruption: tool.cancel_on_interruption,
                requires_reporter: false,
            },
        );
        Ok(())
    }
    /// Register a schema handler's immutable call contract. Handler deadlines
    /// override this default; two absent deadlines mean no execution timeout.
    /// Cleanup remains the owning pipeline's responsibility and is never invoked
    /// or recorded by this method.
    pub fn register_direct(
        &mut self,
        name: impl Into<String>,
        handler: LocalToolFunction,
        default_timeout: Option<Duration>,
    ) -> Result<(), ProcessorError> {
        let name = name.into();
        let options = handler.options();
        let requires_reporter = handler.requires_reporter();
        if name.is_empty()
            || self.tools.contains_key(&name)
            || default_timeout.is_some_and(|timeout| timeout.is_zero())
            || options.timeout.is_some_and(|timeout| timeout.is_zero())
            || (requires_reporter && options.cancel_on_interruption)
        {
            return Err(ProcessorError::new(
                "invalid or duplicate direct tool registration",
            ));
        }
        self.tools.insert(
            name,
            RegistrationRow {
                callback: Arc::new(move |call, reporter| {
                    let handler = handler.clone();
                    Box::pin(async move {
                        handler
                            .call_completion_with_reporter(call.arguments, reporter)
                            .await
                            .map(|result| result.map_err(|error| error.to_string()))
                    })
                }),
                timeout: options.timeout.or(default_timeout),
                cancel_on_interruption: options.cancel_on_interruption,
                requires_reporter,
            },
        );
        Ok(())
    }
    /// Capture the current validated inventory without copying executor history.
    pub fn registrations_snapshot(&self) -> ToolRegistrationSnapshot {
        ToolRegistrationSnapshot(Arc::new(self.tools.clone()))
    }
    /// Admit this batch through one immutable inventory. The same callback and
    /// interruption policy are used for reservation, progress and dispatch even
    /// if the caller publishes a different snapshot while this call is pending.
    pub async fn process_with_registrations(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
        registrations: &ToolRegistrationSnapshot,
    ) -> Result<(), ProcessorError> {
        self.process_registered(frame, direction, ctx, &registrations.0)
            .await
    }
    fn reserve(
        &self,
        calls: &[FunctionCall],
        signed: Option<Arc<NativeLlmContinuation>>,
        group_id: &str,
        ctx: &ProcessorContext,
        registrations: &BTreeMap<String, RegistrationRow>,
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
            .map(|call| {
                (
                    *call,
                    registrations
                        .get(&call.function_name)
                        .is_none_or(|tool| tool.cancel_on_interruption),
                )
            })
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
            let control = Arc::new(Mutex::new(Control::default()));
            let active = ActiveCall {
                call: call.clone(),
                bytes,
                control: control.clone(),
                context: ctx.clone(),
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
                publisher: Some(publisher),
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
        registrations: &BTreeMap<String, RegistrationRow>,
    ) -> Result<(), ProcessorError> {
        let Some(tool) = registrations.get(&lease.call.function_name).cloned() else {
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
                let reporter = if tool.requires_reporter {
                    match lease.reporter() {
                        Ok(reporter) => Some(reporter),
                        Err(error) => return Some(Err(error.to_string())),
                    }
                } else {
                    None
                };
                if !lease.enter() {
                    return None;
                }
                (tool.callback)(call, reporter).await
            })
            .catch_unwind();
            let guarded = async {
                match &admission {
                    Some(capability) => capability.run_effect(callback).await,
                    None => Some(callback.await),
                }
            };
            let outcome = match tool.timeout {
                Some(timeout) => tokio::time::timeout(timeout, guarded).await.map_err(|_| ()),
                None => Ok(guarded.await),
            };
            match outcome {
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
}
#[async_trait]
impl FrameProcessor for ToolExecutor {
    fn name(&self) -> &str {
        "ToolExecutor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.process_registered(frame, direction, ctx, &self.tools)
            .await
    }
}
impl ToolExecutor {
    async fn process_registered(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
        registrations: &BTreeMap<String, RegistrationRow>,
    ) -> Result<(), ProcessorError> {
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
                history.active.get(&cancel.tool_call_id).and_then(|active| {
                    let mut control = active
                        .control
                        .lock()
                        .unwrap_or_else(|error| error.into_inner());
                    if control.settled {
                        return None;
                    }
                    control.cancel.get_or_insert(cancel.run_llm);
                    let revoked = control.close_reporting();
                    Some((active.context.clone(), control.task, revoked))
                })
            };
            if let Some((origin, task, revoked)) = target {
                drop(revoked);
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
        if admission
            .as_ref()
            .is_some_and(|capability| !capability.admits_effect())
        {
            return Ok(());
        }
        if calls.len() > self.max_batch {
            return Err(ProcessorError::new("tool batch exceeds configured limit"));
        }
        for call in calls {
            if call.tool_call_id.is_empty()
                || call.function_name.is_empty()
                || !call.arguments.is_object()
            {
                return Err(ProcessorError::new(
                    "invalid tool call identity or arguments",
                ));
            }
            if call.tool_call_id.len() > self.max_argument_bytes
                || call.function_name.len() > self.max_argument_bytes
                || serde_json::to_vec(&call.arguments)
                    .map_err(|error| ProcessorError::new(error.to_string()))?
                    .len()
                    > self.max_argument_bytes
            {
                return Err(ProcessorError::new(
                    "tool arguments or identity exceed configured limit",
                ));
            }
        }
        let signed_duplicate = signed.is_some();
        let group_id = format!("tool-batch-{}", Frame::new(FrameData::LlmRun).id);
        let leases = self.reserve(calls, signed, &group_id, ctx, registrations)?;
        frame.take_native_speculation();
        if leases.is_empty() {
            return if signed_duplicate {
                ctx.push(frame, direction).await
            } else {
                Ok(())
            };
        }
        let batch = leases[0]
            .publisher
            .as_ref()
            .and_then(ToolOutcomePublisher::batch)
            .expect("reserved native batch")
            .clone();
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
                    cancel_on_interruption: registrations
                        .get(&lease.call.function_name)
                        .is_none_or(|tool| tool.cancel_on_interruption),
                    group_id: Some(group_id.clone()),
                },
            ));
            frame.set_native_tool_batch(batch.clone())?;
            ctx.push(frame, Direction::Downstream).await?;
        }
        for lease in leases {
            self.dispatch(lease, ctx, admission.clone(), registrations)?;
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
}
#[derive(Default)]
struct Control {
    task: Option<u64>,
    entered: bool,
    cancel: Option<bool>,
    settled: bool,
    progress_sequence: u64,
    progress: Option<ToolProgressPublisher>,
}
impl Control {
    fn close_reporting(&mut self) -> Option<ToolProgressPublisher> {
        self.settled = true;
        self.progress.take()
    }
}
struct ActiveLease {
    call: Arc<FunctionCall>,
    publisher: Option<ToolOutcomePublisher>,
    control: Arc<Mutex<Control>>,
    history: Arc<Mutex<History>>,
    retention: usize,
    _reservation: OwnedSemaphorePermit,
}
impl ActiveLease {
    fn enter(&self) -> bool {
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if control.cancel.is_some() || control.settled {
            return false;
        }
        control.entered = true;
        true
    }
    fn reporter(&self) -> Result<LocalToolReporter, ProcessorError> {
        let progress = self
            .publisher
            .as_ref()
            .ok_or_else(|| ProcessorError::new("tool reporter is no longer active"))?
            .progress_publisher()?;
        {
            let mut control = self
                .control
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if control.cancel.is_some() || control.settled {
                return Err(ProcessorError::new("tool reporter is no longer active"));
            }
            control.progress = Some(progress);
        }
        let weak = Arc::downgrade(&self.control);
        // Retaining an endpoint must not retain the call's runtime or publisher.
        Ok(LocalToolReporter::new_completion_sink(move |completion| {
            let state = weak
                .upgrade()
                .ok_or_else(|| ProcessorError::new("tool reporter is no longer active"))?;
            let mut control = state.lock().unwrap_or_else(|error| error.into_inner());
            if !control.entered || control.cancel.is_some() || control.settled {
                return Err(ProcessorError::new("tool reporter is no longer active"));
            }
            let sequence = control
                .progress_sequence
                .checked_add(1)
                .ok_or_else(|| ProcessorError::new("tool progress sequence exhausted"))?;
            // Keep this lock through queue admission so settlement cannot overtake
            // an accepted update. Rejected attempts do not consume a sequence.
            let admission = control
                .progress
                .as_ref()
                .ok_or_else(|| ProcessorError::new("tool reporter is no longer active"))?
                .publish_progress_with_context_updated(
                    completion.result,
                    sequence,
                    completion.on_context_updated,
                )?;
            if admission != ToolOutcomeAdmission::Admitted {
                return Err(ProcessorError::new("tool progress was not admitted"));
            }
            control.progress_sequence = sequence;
            Ok(())
        }))
    }
    fn complete(&mut self, result: Result<LocalToolCompletion, String>, reason: ToolCallEndReason) {
        let Some(publisher) = self.publisher.take() else {
            return;
        };
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let revoked = control.close_reporting();
        let effects = if control.entered {
            ToolCallEffectStatus::ExternalEffectsUnknown
        } else {
            ToolCallEffectStatus::NotStarted
        };
        let receipt = if let Some(run_llm) = control.cancel {
            publisher.cancel(effects, ToolCallEndReason::ExplicitCancel, run_llm)
        } else {
            let (result, run_llm, on_context_updated) = match result {
                Ok(completion) => (
                    Ok(completion.result.value),
                    completion.result.run_llm,
                    completion.on_context_updated,
                ),
                Err(error) => (Err(error), None, None),
            };
            publisher.complete_with_context_updated(
                result,
                run_llm,
                on_context_updated,
                effects,
                reason,
            )
        };
        drop(control);
        drop(revoked);
        self.record(receipt);
    }
    fn cancel(&mut self, reason: ToolCallEndReason) {
        let Some(publisher) = self.publisher.take() else {
            return;
        };
        let mut control = self
            .control
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let revoked = control.close_reporting();
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
        drop(revoked);
        self.record(receipt);
    }
    fn record(&self, receipt: ToolCallReceipt) {
        let mut history = self
            .history
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let bytes = history
            .active
            .remove(&self.call.tool_call_id)
            .expect("owned active settlement")
            .bytes;
        history.active_bytes -= bytes;
        history.total_terminal = history.total_terminal.saturating_add(1);
        if receipt.admission != ToolOutcomeAdmission::Admitted {
            history.unadmitted_terminal = history.unadmitted_terminal.saturating_add(1);
        }
        let bytes = receipt.outcome.retained_bytes();
        while history.terminal.len() >= self.retention
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
}
impl Drop for ActiveLease {
    fn drop(&mut self) {
        self.cancel(ToolCallEndReason::TaskDropped);
    }
}

#[cfg(test)]
#[path = "tools_settlement_tests.rs"]
mod settlement_tests;
