//! Bounded process-local asynchronous tool progress authority.
//!
//! This module never spawns a task. The selected relay owns deadline servicing
//! and terminal forwarding. Public receipts contain compact facts and weak
//! capabilities, never a processor context or a result payload.
use crate::{
    Direction, Frame, FrameData, FrameObservationToken, FunctionCall, FunctionCallResultFrame,
    InvocationFence, NativeToolCallBatch, NativeToolCallOutcome, ProcessorContext, ProcessorError,
    ToolCallEndReason, ToolCallReceipt, ToolOutcomeAdmission,
};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::{sync::Notify, time::Instant};

const KIB: usize = 1024;
#[derive(Debug, Clone)]
pub struct NativeToolProgressConfig {
    pub end_drain_timeout: Duration,
}
impl Default for NativeToolProgressConfig {
    fn default() -> Self {
        Self {
            end_drain_timeout: Duration::from_secs(35),
        }
    }
}
#[derive(Debug, Clone, Copy)]
pub struct NativeToolProgressProviderLimits {
    pub response_timeout: Duration,
    pub operation_timeout: Duration,
    pub close_timeout: Duration,
    pub max_message_bytes: usize,
    pub max_in_flight: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeToolProgressRole {
    Gate,
    User,
    Provider,
    Executor,
    Relay,
    Assistant,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeToolProgressRouteState {
    SettingUp,
    Open,
    Draining,
    Stopping,
    Cancelled,
    Failed,
    EndForwarded,
    Closed,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeToolProgressStopResult {
    Requested,
    AlreadyRequested,
    AlreadyEnding,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum NativeToolProgressFailure {
    #[error("tool progress route closed")]
    Closed,
    #[error("tool progress cancelled")]
    Cancelled,
    #[error("tool progress stopped")]
    Stopped,
    #[error("tool progress disposition deadline expired")]
    Deadline,
    #[error("tool progress queue closed")]
    QueueClosed,
    #[error("tool progress queue full")]
    QueueFull,
    #[error("tool progress disposition mailbox full")]
    MailboxFull,
    #[error("tool progress history lineage changed")]
    InvalidHistory,
    #[error("tool progress history capacity exceeded")]
    HistoryCapacity,
    #[error("tool progress provenance mismatch")]
    InvalidProvenance,
    #[error("tool progress provider rejected delivery")]
    ProviderRejected,
    #[error("tool progress inference admission failed")]
    InferenceFailed,
    #[error("tool progress owner dropped")]
    OwnerDropped,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeToolProgressHistoryCommit {
    pub digest: [u8; 32],
    pub message_digests: [[u8; 32]; 2],
    pub revision: u64,
    pub start: usize,
    pub end: usize,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeToolProgressDisposition {
    Admitted,
    ProviderAdmitted,
    HistoryCommitted(NativeToolProgressHistoryCommit),
    TerminalPublished,
    ProviderTerminal,
    AssistantTerminal,
    Failed(NativeToolProgressFailure),
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeToolProgressDispositionRecord {
    pub lease_id: u64,
    pub event_id: u64,
    pub ordinal: u64,
    pub disposition: NativeToolProgressDisposition,
}

pub struct NativeToolProgressRoute(Arc<RouteInner>, Mutex<tokio::sync::watch::Receiver<u64>>);
impl Clone for NativeToolProgressRoute {
    fn clone(&self) -> Self {
        Self(self.0.clone(), Mutex::new(self.1.lock().unwrap().clone()))
    }
}
impl std::fmt::Debug for NativeToolProgressRoute {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeToolProgressRoute")
            .field("id", &self.0.id)
            .field("state", &self.state())
            .finish()
    }
}
struct RouteInner {
    id: u64,
    config: NativeToolProgressConfig,
    state: Mutex<RouteData>,
    changed: tokio::sync::watch::Sender<u64>,
    inference: Arc<Mutex<bool>>,
    history_admission: Mutex<bool>,
    publication_open: std::sync::atomic::AtomicBool,
    released_payload: std::sync::atomic::AtomicUsize,
    released_terminal: std::sync::atomic::AtomicUsize,
    released_metadata: std::sync::atomic::AtomicUsize,
    gate: Mutex<GateOwner>,
}
struct GateOwner {
    context: Option<ProcessorContext>,
    held: Option<Frame>,
}
struct RoleRegistration {
    runtime: Weak<crate::pipeline::Runtime>,
    runtime_id: u64,
    index: usize,
    context: Option<usize>,
}
struct RouteData {
    phase: NativeToolProgressRouteState,
    terminal_forwarded: bool,
    roles: BTreeMap<NativeToolProgressRole, RoleRegistration>,
    executor_limits: Option<(usize, Duration)>,
    history_bytes: usize,
    drain_deadline: Option<Instant>,
    leases: BTreeMap<u64, LeaseRecord>,
    events: BTreeMap<u64, EventRecord>,
    records: VecDeque<NativeToolProgressDispositionRecord>,
    total_reports: usize,
    pending_reports: usize,
    payload_bytes: usize,
    metadata_bytes: usize,
    core_metadata_bytes: usize,
    terminal_bytes: usize,
    active_reporting: usize,
    failure: Option<NativeToolProgressFailure>,
    admission_history: BTreeMap<u64, NativeToolProgressHistoryCommit>,
    terminal_history: BTreeMap<u64, NativeToolProgressHistoryCommit>,
}
struct LeaseRecord {
    lease: Weak<LeaseInner>,
    batch_id: u64,
    call_id_digest: [u8; 32],
    reporting: bool,
    published: bool,
    terminal_outcome: Option<u64>,
    provider_staged: bool,
    provider: bool,
    assistant: bool,
    terminal_deadline: Option<Instant>,
}
struct EventRecord {
    receipt: Arc<ReceiptInner>,
    provider: bool,
    history: bool,
    deadline: Option<Instant>,
    lease: Weak<LeaseInner>,
}
impl NativeToolProgressRoute {
    pub const MAX_ACTIVE_REPORTING: usize = 16;
    pub const MAX_REPORTS_PER_LEASE: u64 = 64;
    pub const MAX_REPORTS: usize = 1024;
    pub const MAX_PENDING_PER_LEASE: usize = 4;
    pub const MAX_PENDING: usize = 32;
    pub const MAX_PAYLOAD_BYTES: usize = 4 * 1024 * 1024;
    pub const MAX_METADATA_BYTES: usize = 1024 * 1024;
    pub const MAX_RESULT_BYTES: usize = 16 * 1024;
    pub const MAX_TERMINAL_RESULT_BYTES: usize = 64 * 1024;
    pub const MAX_EVENT_BYTES: usize = 256 * 1024;
    pub const MAX_HISTORY_MESSAGES: usize = 1024;
    const CORE_LEASE_BYTES: usize = 640;
    const CORE_EVENT_BYTES: usize = 384;
    const CORE_METADATA_BYTES: usize = 768 * KIB;
    pub const DISPOSITION_TIMEOUT: Duration = Duration::from_secs(5);
    pub fn new(config: NativeToolProgressConfig) -> Result<Self, ProcessorError> {
        if config.end_drain_timeout.is_zero() || config.end_drain_timeout > Duration::from_secs(305)
        {
            return Err(ProcessorError::new(
                "invalid selected tool progress drain timeout",
            ));
        }
        let (changes, receiver) = tokio::sync::watch::channel(0u64);
        Ok(Self(
            Arc::new(RouteInner {
                id: Frame::new(FrameData::LlmRun).id,
                config,
                state: Mutex::new(RouteData {
                    phase: NativeToolProgressRouteState::SettingUp,
                    terminal_forwarded: false,
                    roles: BTreeMap::new(),
                    executor_limits: None,
                    history_bytes: 1024 * KIB,
                    drain_deadline: None,
                    leases: BTreeMap::new(),
                    events: BTreeMap::new(),
                    records: VecDeque::new(),
                    total_reports: 0,
                    pending_reports: 0,
                    payload_bytes: 0,
                    metadata_bytes: 0,
                    core_metadata_bytes: 136 * KIB,
                    terminal_bytes: 0,
                    active_reporting: 0,
                    failure: None,
                    admission_history: BTreeMap::new(),
                    terminal_history: BTreeMap::new(),
                }),
                changed: changes,
                inference: Arc::new(Mutex::new(false)),
                history_admission: Mutex::new(true),
                publication_open: std::sync::atomic::AtomicBool::new(false),
                released_payload: std::sync::atomic::AtomicUsize::new(0),
                released_terminal: std::sync::atomic::AtomicUsize::new(0),
                released_metadata: std::sync::atomic::AtomicUsize::new(0),
                gate: Mutex::new(GateOwner {
                    context: None,
                    held: None,
                }),
            }),
            Mutex::new(receiver),
        ))
    }
    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<u64> {
        self.0.changed.subscribe()
    }
    pub fn with_history_admission<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        let allowed = self.0.history_admission.lock().unwrap();
        if !*allowed {
            return None;
        }
        Some(operation())
    }
    pub fn validate_call(call: &FunctionCall) -> Result<(), ProcessorError> {
        if call.function_name.capacity() > 32 * KIB || call.tool_call_id.capacity() > 32 * KIB {
            return Err(ProcessorError::new(
                "selected call retained identity capacity",
            ));
        }
        validate_json(&call.arguments, 32 * KIB)?;
        encoded_size(call, 32 * KIB).map(|_| ())
    }
    pub fn id(&self) -> u64 {
        self.0.id
    }
    pub fn config(&self) -> &NativeToolProgressConfig {
        &self.0.config
    }
    pub fn state(&self) -> NativeToolProgressRouteState {
        self.0.state.lock().unwrap().phase
    }
    pub fn is_open(&self) -> bool {
        self.state() == NativeToolProgressRouteState::Open
    }
    pub fn accepts_new_calls(&self) -> bool {
        self.is_open()
    }
    pub fn is_closed(&self) -> bool {
        matches!(
            self.state(),
            NativeToolProgressRouteState::Stopping
                | NativeToolProgressRouteState::Cancelled
                | NativeToolProgressRouteState::Failed
                | NativeToolProgressRouteState::Closed
        )
    }
    pub fn inference_permission(&self) -> NativeToolProgressPermission {
        NativeToolProgressPermission {
            owner: Arc::downgrade(&self.0),
            route: Arc::downgrade(&self.0.inference),
            lease: None,
            lease_id: None,
            changes: self.0.changed.subscribe(),
        }
    }
    pub fn inference_allowed(&self) -> bool {
        *self.0.inference.lock().unwrap()
    }
    pub fn history_max_bytes(&self) -> usize {
        self.0.state.lock().unwrap().history_bytes
    }
    pub fn failure(&self) -> Option<NativeToolProgressFailure> {
        self.0.state.lock().unwrap().failure
    }
    pub async fn changed(&self) {
        let (mut receiver, unseen) = {
            let mut receiver = self.1.lock().unwrap();
            let unseen = receiver.has_changed().unwrap_or(true);
            receiver.borrow_and_update();
            (receiver.clone(), unseen)
        };
        if !unseen {
            let _ = receiver.changed().await;
        }
    }
    fn from_inner(inner: Arc<RouteInner>) -> Self {
        let receiver = inner.changed.subscribe();
        Self(inner, Mutex::new(receiver))
    }
    pub fn register_role(
        &self,
        role: NativeToolProgressRole,
        ctx: &ProcessorContext,
        shared_context: Option<usize>,
    ) -> Result<(), ProcessorError> {
        let identity = ctx.processor_identity();
        let mut data = self.0.state.lock().unwrap();
        if data.phase != NativeToolProgressRouteState::SettingUp || data.roles.contains_key(&role) {
            let failed_setup = data.phase == NativeToolProgressRouteState::SettingUp;
            if failed_setup {
                data.phase = NativeToolProgressRouteState::Failed;
                data.failure
                    .get_or_insert(NativeToolProgressFailure::InvalidProvenance);
            }
            drop(data);
            if failed_setup {
                self.close(NativeToolProgressFailure::InvalidProvenance);
            }
            return Err(ProcessorError::fatal(
                "duplicate or late tool progress role registration",
            ));
        }
        data.roles.insert(
            role,
            RoleRegistration {
                runtime: Arc::downgrade(&ctx.runtime),
                runtime_id: identity.runtime_id,
                index: identity.index,
                context: shared_context,
            },
        );
        Ok(())
    }
    pub fn register_executor_limits(
        &self,
        max_in_flight: usize,
        largest_timeout: Duration,
    ) -> Result<(), ProcessorError> {
        if max_in_flight == 0
            || max_in_flight > Self::MAX_ACTIVE_REPORTING
            || largest_timeout < Duration::from_millis(1)
            || largest_timeout > Duration::from_secs(300)
        {
            return Err(ProcessorError::new("incompatible selected executor limits"));
        }
        let mut data = self.0.state.lock().unwrap();
        if data.phase != NativeToolProgressRouteState::SettingUp || data.executor_limits.is_some() {
            let failed_setup = data.phase == NativeToolProgressRouteState::SettingUp;
            if failed_setup {
                data.phase = NativeToolProgressRouteState::Failed;
                data.failure
                    .get_or_insert(NativeToolProgressFailure::InvalidProvenance);
            }
            drop(data);
            if failed_setup {
                self.close(NativeToolProgressFailure::InvalidProvenance);
            }
            return Err(ProcessorError::new(
                "selected executor limits already registered",
            ));
        }
        data.executor_limits = Some((max_in_flight, largest_timeout));
        Ok(())
    }
    pub fn validate_configuration(
        &self,
        ctx: &ProcessorContext,
        provider: NativeToolProgressProviderLimits,
    ) -> Result<(), ProcessorError> {
        self.validate_configuration_inner(ctx, provider, false)
    }
    pub fn validate_and_open(
        &self,
        ctx: &ProcessorContext,
        provider: NativeToolProgressProviderLimits,
    ) -> Result<(), ProcessorError> {
        self.validate_configuration_inner(ctx, provider, true)
    }
    fn validate_configuration_inner(
        &self,
        ctx: &ProcessorContext,
        provider: NativeToolProgressProviderLimits,
        open: bool,
    ) -> Result<(), ProcessorError> {
        let mut permit = self.0.inference.lock().unwrap();
        let mut data = self.0.state.lock().unwrap();
        if data.phase != NativeToolProgressRouteState::SettingUp {
            return Err(ProcessorError::fatal("tool progress route already started"));
        }
        let roles = [
            NativeToolProgressRole::Gate,
            NativeToolProgressRole::User,
            NativeToolProgressRole::Provider,
            NativeToolProgressRole::Executor,
            NativeToolProgressRole::Relay,
            NativeToolProgressRole::Assistant,
        ];
        let id = ctx.processor_identity();
        let mut previous = None;
        for role in roles {
            let registration = data.roles.get(&role).ok_or_else(|| {
                ProcessorError::fatal("tool progress composition is missing a role")
            })?;
            if registration.runtime_id != id.runtime_id
                || !Weak::ptr_eq(&registration.runtime, &Arc::downgrade(&ctx.runtime))
                || previous.is_some_and(|index| index >= registration.index)
            {
                return Err(ProcessorError::fatal(
                    "tool progress roles have incompatible runtime or order",
                ));
            }
            if role == NativeToolProgressRole::Provider && registration.index != id.index {
                return Err(ProcessorError::fatal(
                    "tool progress Start was not admitted by provider",
                ));
            }
            previous = Some(registration.index);
        }
        if data.roles[&NativeToolProgressRole::User].context.is_none()
            || data.roles[&NativeToolProgressRole::User].context
                != data.roles[&NativeToolProgressRole::Assistant].context
        {
            return Err(ProcessorError::fatal(
                "tool progress halves do not share one history",
            ));
        }
        let (capacity, timeout) = data
            .executor_limits
            .ok_or_else(|| ProcessorError::fatal("tool progress executor limits missing"))?;
        if provider.max_in_flight < capacity
            || provider.max_message_bytes < 64 * KIB
            || provider.response_timeout.is_zero()
            || provider.operation_timeout.is_zero()
            || provider.close_timeout.is_zero()
        {
            return Err(ProcessorError::fatal(
                "incompatible selected provider limits",
            ));
        }
        let minimum_drain = timeout
            .checked_add(Self::DISPOSITION_TIMEOUT)
            .ok_or_else(|| ProcessorError::fatal("tool progress duration overflow"))?;
        let minimum_shutdown = self
            .0
            .config
            .end_drain_timeout
            .checked_add(provider.response_timeout)
            .and_then(|d| d.checked_add(provider.operation_timeout))
            .and_then(|d| d.checked_add(provider.operation_timeout))
            .and_then(|d| d.checked_add(provider.close_timeout))
            .and_then(|d| d.checked_add(Duration::from_secs(5)))
            .ok_or_else(|| ProcessorError::fatal("tool progress shutdown duration overflow"))?;
        if self.0.config.end_drain_timeout < minimum_drain
            || ctx.shutdown_timeout() < minimum_shutdown
        {
            return Err(ProcessorError::fatal(
                "pipeline shutdown budget is insufficient for selected tool progress",
            ));
        }
        data.history_bytes = (1024 * KIB).min(provider.max_message_bytes);
        if open {
            data.phase = NativeToolProgressRouteState::Open;
            *permit = true;
            self.0
                .publication_open
                .store(true, std::sync::atomic::Ordering::Release);
        }
        drop(data);
        drop(permit);
        self.0.notify();
        Ok(())
    }
    pub fn begin_drain(&self) -> Result<Instant, ProcessorError> {
        let mut permit = self.0.inference.lock().unwrap();
        let mut data = self.0.state.lock().unwrap();
        if data.phase == NativeToolProgressRouteState::Draining {
            return Ok(data.drain_deadline.unwrap());
        }
        if data.phase != NativeToolProgressRouteState::Open {
            return Err(ProcessorError::fatal(
                "tool progress route cannot start End drain",
            ));
        }
        let deadline = Instant::now()
            .checked_add(self.0.config.end_drain_timeout)
            .ok_or_else(|| ProcessorError::fatal("tool progress drain deadline overflow"))?;
        *permit = false;
        data.phase = NativeToolProgressRouteState::Draining;
        data.drain_deadline = Some(deadline);
        drop(data);
        drop(permit);
        self.0.notify();
        Ok(deadline)
    }
    pub fn request_stop(&self) -> Result<NativeToolProgressStopResult, ProcessorError> {
        let gate = self.0.gate.lock().unwrap();
        let mut data = self.0.state.lock().unwrap();
        let result = match data.phase {
            NativeToolProgressRouteState::EndForwarded
            | NativeToolProgressRouteState::Closed
            | NativeToolProgressRouteState::Cancelled
            | NativeToolProgressRouteState::Failed => NativeToolProgressStopResult::AlreadyEnding,
            NativeToolProgressRouteState::Stopping => {
                NativeToolProgressStopResult::AlreadyRequested
            }
            _ => {
                data.phase = NativeToolProgressRouteState::Stopping;
                data.failure = Some(NativeToolProgressFailure::Stopped);
                NativeToolProgressStopResult::Requested
            }
        };
        drop(data);
        drop(gate);
        if result == NativeToolProgressStopResult::Requested {
            self.close(NativeToolProgressFailure::Stopped);
        }
        Ok(result)
    }
    /// Confirm an End already admitted by forward_held_terminal. This cannot
    /// manufacture terminal admission or advance lifecycle state.
    pub fn mark_end_forwarded(&self) -> Result<(), ProcessorError> {
        let data = self.0.state.lock().unwrap();
        if data.phase == NativeToolProgressRouteState::EndForwarded && data.terminal_forwarded {
            Ok(())
        } else {
            Err(ProcessorError::fatal("selected End has not been forwarded"))
        }
    }
    pub fn mark_closed(&self) {
        self.close(NativeToolProgressFailure::Closed);
        let mut data = self.0.state.lock().unwrap();
        data.phase = NativeToolProgressRouteState::Closed;
    }
    pub fn close(&self, reason: NativeToolProgressFailure) {
        let mut history = self.0.history_admission.lock().unwrap();
        *history = false;
        let runtime = {
            self.0
                .state
                .lock()
                .unwrap()
                .roles
                .values()
                .next()
                .map(|role| role.runtime.clone())
        };
        let runtime = runtime.and_then(|runtime| runtime.upgrade());
        if let Some(runtime) = &runtime {
            let _admission = runtime.admission.lock().unwrap();
            self.0
                .publication_open
                .store(false, std::sync::atomic::Ordering::Release);
        } else {
            self.0
                .publication_open
                .store(false, std::sync::atomic::Ordering::Release);
        }
        let mut permission = self.0.inference.lock().unwrap();
        *permission = false;
        let (leases, receipts, clear_gate, effective_reason) = {
            let mut data = self.0.state.lock().unwrap();
            let next = match reason {
                NativeToolProgressFailure::Cancelled
                    if data.phase != NativeToolProgressRouteState::Closed =>
                {
                    NativeToolProgressRouteState::Cancelled
                }
                NativeToolProgressFailure::Stopped
                    if matches!(
                        data.phase,
                        NativeToolProgressRouteState::SettingUp
                            | NativeToolProgressRouteState::Open
                            | NativeToolProgressRouteState::Draining
                    ) =>
                {
                    NativeToolProgressRouteState::Stopping
                }
                NativeToolProgressFailure::Closed
                    if !matches!(
                        data.phase,
                        NativeToolProgressRouteState::Cancelled
                            | NativeToolProgressRouteState::Failed
                    ) =>
                {
                    NativeToolProgressRouteState::Closed
                }
                NativeToolProgressFailure::Stopped
                | NativeToolProgressFailure::Closed
                | NativeToolProgressFailure::Cancelled => data.phase,
                _ if !matches!(
                    data.phase,
                    NativeToolProgressRouteState::Closed | NativeToolProgressRouteState::Cancelled
                ) =>
                {
                    NativeToolProgressRouteState::Failed
                }
                _ => data.phase,
            };
            if next != data.phase || data.failure.is_none() {
                data.failure = Some(reason);
            }
            data.phase = next;
            (
                data.leases
                    .values()
                    .filter_map(|entry| entry.lease.upgrade())
                    .collect::<Vec<_>>(),
                data.events
                    .values()
                    .map(|entry| entry.receipt.clone())
                    .collect::<Vec<_>>(),
                data.phase != NativeToolProgressRouteState::Stopping,
                data.failure.unwrap_or(reason),
            )
        };
        drop(permission);
        drop(history);
        let abandoned = if clear_gate {
            let mut gate = self.0.gate.lock().unwrap();
            (gate.context.take(), gate.held.take())
        } else {
            (None, None)
        };
        drop(abandoned);
        // Never cancel tasks or notify receipt waiters under route/lease locks.
        for lease in leases {
            lease.revoke_and_cancel();
        }
        for receipt in receipts {
            receipt.fail(effective_reason);
        }
        self.0.notify();
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        let data = self.0.state.lock().unwrap();
        data.events
            .values()
            .filter(|entry| !entry.provider || !entry.history)
            .filter_map(|entry| entry.deadline)
            .chain(
                data.leases
                    .values()
                    .filter(|entry| !entry.provider || !entry.assistant)
                    .filter_map(|entry| entry.terminal_deadline),
            )
            .chain(data.drain_deadline)
            .min()
    }
    pub fn expire(&self, now: Instant) {
        if self.next_deadline().is_some_and(|deadline| deadline <= now) {
            self.close(NativeToolProgressFailure::Deadline);
        }
    }
    pub fn is_drained(&self) -> bool {
        let data = self.0.state.lock().unwrap();
        data.pending_reports == 0
            && data
                .leases
                .values()
                .all(|entry| entry.published && entry.provider && entry.assistant)
    }
    pub fn take_dispositions(&self) -> Vec<NativeToolProgressDispositionRecord> {
        self.0.state.lock().unwrap().records.drain(..).collect()
    }
    fn check_role(
        &self,
        ctx: &ProcessorContext,
        expected: NativeToolProgressRole,
    ) -> Result<(), ProcessorError> {
        let data = self.0.state.lock().unwrap();
        let id = ctx.processor_identity();
        match data.roles.get(&expected) {
            Some(role)
                if role.runtime_id == id.runtime_id
                    && role.index == id.index
                    && Weak::ptr_eq(&role.runtime, &Arc::downgrade(&ctx.runtime)) =>
            {
                Ok(())
            }
            _ => Err(ProcessorError::fatal(
                "tool progress consumer role mismatch",
            )),
        }
    }
    pub fn validate_admission(
        &self,
        ctx: &ProcessorContext,
        admission: &NativeToolProgressAdmission,
    ) -> Result<(), ProcessorError> {
        let data = self.0.state.lock().unwrap();
        let id = ctx.processor_identity();
        if admission.route_id() != self.id()
            || !Weak::ptr_eq(&admission.0.route, &Arc::downgrade(&self.0))
            || !data.leases.contains_key(&admission.lease_id())
            || !data
                .roles
                .values()
                .any(|role| role.runtime_id == id.runtime_id && role.index == id.index)
        {
            return Err(ProcessorError::fatal("foreign tool progress admission"));
        }
        Ok(())
    }
    pub fn admit_call(
        &self,
        ctx: &ProcessorContext,
        call: Arc<FunctionCall>,
        batch: Arc<NativeToolCallBatch>,
        reporting: bool,
        timeout: Duration,
    ) -> Result<NativeToolProgressLease, ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Executor)?;
        ctx.invocation_fence()?;
        if timeout < Duration::from_millis(1) || timeout > Duration::from_secs(300) {
            return Err(ProcessorError::new(
                "selected tool timeout outside 1ms..300s",
            ));
        }
        let policy = batch
            .members()
            .iter()
            .find(|(member, _)| member.as_ref() == call.as_ref())
            .map(|(_, policy)| *policy)
            .ok_or_else(|| ProcessorError::new("selected tool missing batch membership"))?;
        if reporting && policy {
            return Err(ProcessorError::new(
                "reporting tool must survive interruption",
            ));
        }
        // Selected terminal copies must fit its independently reserved capacity.
        Self::validate_call(&call)?;
        batch.validate_progress_bounds()?;
        let call_bytes = encoded_size(call.as_ref(), 32 * KIB)?;
        if call_bytes > 32 * KIB {
            return Err(ProcessorError::new(
                "selected tool identity exceeds terminal reservation",
            ));
        }
        let id = Frame::new(FrameData::LlmRun).id;
        let permission = Arc::new(Mutex::new(true));
        let admission = NativeToolProgressAdmission(Arc::new(AdmissionInner {
            id,
            route_id: self.id(),
            route: Arc::downgrade(&self.0),
            call,
            batch,
            reporting,
            permission: NativeToolProgressPermission {
                owner: Arc::downgrade(&self.0),
                route: Arc::downgrade(&self.0.inference),
                lease: Some(permission.clone()),
                lease_id: Some(id),
                changes: self.0.changed.subscribe(),
            },
        }));
        let lease = Arc::new(LeaseInner {
            terminal_charge: Arc::new(TerminalCharge {
                route: Arc::downgrade(&self.0),
                active: std::sync::atomic::AtomicBool::new(false),
            }),
            admission,
            context: ctx.downgrade_tool_progress(),
            route: Arc::downgrade(&self.0),
            timeout,
            permission,
            control: Mutex::new(LeaseControl {
                open: true,
                ordinal: 0,
                pending: 0,
                reserved: 0,
                terminal: None,
                execution_deadline: None,
                task: None,
            }),
        });
        let mut data = self.0.state.lock().unwrap();
        self.0.reconcile_released(&mut data);
        if data.phase != NativeToolProgressRouteState::Open
            || data.leases.len() >= Self::MAX_REPORTS
            || data
                .leases
                .values()
                .filter(|lease| !(lease.published && lease.provider && lease.assistant))
                .count()
                >= data.executor_limits.map_or(0, |limits| limits.0)
            || (reporting && data.active_reporting >= Self::MAX_ACTIVE_REPORTING)
            || data
                .core_metadata_bytes
                .saturating_add(Self::CORE_LEASE_BYTES)
                > Self::CORE_METADATA_BYTES
            || data
                .terminal_bytes
                .checked_add(Self::MAX_EVENT_BYTES)
                .is_none_or(|bytes| bytes > Self::MAX_PAYLOAD_BYTES)
        {
            return Err(ProcessorError::fatal(
                "selected tool admission closed or exhausted",
            ));
        }
        lease
            .terminal_charge
            .active
            .store(true, std::sync::atomic::Ordering::Release);
        data.terminal_bytes += Self::MAX_EVENT_BYTES;
        data.core_metadata_bytes += Self::CORE_LEASE_BYTES;
        if reporting {
            data.active_reporting += 1;
        }
        data.leases.insert(
            id,
            LeaseRecord {
                lease: Arc::downgrade(&lease),
                batch_id: lease.admission.batch().id(),
                call_id_digest: Sha256::digest(lease.admission.call().tool_call_id.as_bytes())
                    .into(),
                reporting,
                published: false,
                terminal_outcome: None,
                provider_staged: false,
                provider: false,
                assistant: false,
                terminal_deadline: None,
            },
        );
        Ok(NativeToolProgressLease(lease))
    }
    pub fn provider_admitted(
        &self,
        ctx: &ProcessorContext,
        progress: &NativeToolProgress,
    ) -> Result<(), ProcessorError> {
        self.stage_provider_admitted(ctx, progress)?.notify()
    }
    pub fn stage_provider_admitted(
        &self,
        ctx: &ProcessorContext,
        progress: &NativeToolProgress,
    ) -> Result<NativeToolProgressProviderNotification, ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Provider)?;
        self.validate_admission(ctx, progress.admission())?;
        {
            let data = self.0.state.lock().unwrap();
            let event = data
                .events
                .get(&progress.id())
                .ok_or_else(|| ProcessorError::fatal("unknown staged provider progress"))?;
            if !Arc::ptr_eq(&event.receipt, &progress.receipt.0) {
                return Err(ProcessorError::fatal("conflicting staged provider event"));
            }
        }
        progress.receipt.0.state.lock().unwrap().provider_admitted = true;
        Ok(NativeToolProgressProviderNotification {
            route: Arc::downgrade(&self.0),
            receipt: progress.receipt.0.clone(),
            event_id: progress.id(),
            lease_id: progress.lease_id(),
            ordinal: progress.ordinal(),
        })
    }
    pub fn history_committed(
        &self,
        ctx: &ProcessorContext,
        progress: &NativeToolProgress,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<(), ProcessorError> {
        self.stage_history_committed(ctx, progress, commit)?
            .notify()
    }
    /// Record the actual canonical commit while the paired history-admission
    /// guard is held. This stages immutable truth only: no notification, user
    /// waker, task cancellation, runtime operation, or lease Control acquisition.
    pub fn stage_history_committed(
        &self,
        ctx: &ProcessorContext,
        progress: &NativeToolProgress,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<NativeToolProgressHistoryNotification, ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        if commit.start >= commit.end
            || commit.end > Self::MAX_HISTORY_MESSAGES
            || progress.admission.route_id() != self.id()
        {
            return Err(ProcessorError::fatal(
                "invalid tool progress history attribution",
            ));
        }
        {
            let data = self.0.state.lock().unwrap();
            let event = data
                .events
                .get(&progress.id())
                .ok_or_else(|| ProcessorError::fatal("unknown tool progress commit"))?;
            if !Arc::ptr_eq(&event.receipt, &progress.receipt.0) {
                return Err(ProcessorError::fatal(
                    "conflicting tool progress commit identity",
                ));
            }
        }
        progress.receipt.0.stage_commit(commit)?;
        Ok(NativeToolProgressHistoryNotification {
            route: Arc::downgrade(&self.0),
            receipt: progress.receipt.0.clone(),
            event_id: progress.id(),
            lease_id: progress.lease_id(),
            ordinal: progress.ordinal(),
            commit,
        })
    }
    fn consume_identified_event(
        &self,
        event_id: u64,
        lease_id: u64,
        ordinal: u64,
        identity: &Arc<ReceiptInner>,
        history: Option<NativeToolProgressHistoryCommit>,
    ) -> Result<(), ProcessorError> {
        let (receipt, finished, overflow) = {
            let mut data = self.0.state.lock().unwrap();
            let event = data
                .events
                .get_mut(&event_id)
                .ok_or_else(|| ProcessorError::fatal("unknown tool progress event"))?;
            if !Arc::ptr_eq(&event.receipt, identity) {
                return Err(ProcessorError::fatal(
                    "conflicting tool progress event identity",
                ));
            }
            if history.is_none() && event.provider {
                return Ok(());
            }
            if let Some(commit) = history {
                if event.history {
                    let receipt = event.receipt.clone();
                    drop(data);
                    return receipt.commit(commit);
                }
            }
            let was_finished = event.provider && event.history;
            if history.is_some() {
                event.history = true;
            } else {
                event.provider = true;
            }
            let finished = !was_finished && event.provider && event.history;
            let receipt = event.receipt.clone();
            let lease = if finished {
                event.lease.upgrade()
            } else {
                None
            };
            if finished {
                data.pending_reports = data.pending_reports.saturating_sub(1);
            }
            let overflow = data.records.len() >= 256;
            if !overflow {
                data.records.push_back(NativeToolProgressDispositionRecord {
                    lease_id,
                    event_id,
                    ordinal,
                    disposition: history.map_or(
                        NativeToolProgressDisposition::ProviderAdmitted,
                        NativeToolProgressDisposition::HistoryCommitted,
                    ),
                });
            }
            (receipt, lease, overflow)
        };
        if let Some(lease) = finished {
            let mut control = lease.control.lock().unwrap();
            control.pending = control.pending.saturating_sub(1);
        }
        if let Some(commit) = history {
            receipt.commit(commit)?;
        }
        if overflow {
            self.close(NativeToolProgressFailure::MailboxFull);
            return Err(ProcessorError::fatal("tool progress mailbox overflow"));
        }
        self.0.notify();
        Ok(())
    }
    pub fn consumer_failed(
        &self,
        _ctx: &ProcessorContext,
        progress: &NativeToolProgress,
        failure: NativeToolProgressFailure,
    ) -> Result<(), ProcessorError> {
        if progress.admission.route_id() != self.id() {
            return Err(ProcessorError::fatal("foreign failed progress"));
        }
        self.close(failure);
        Ok(())
    }
    pub fn note_terminal_publication(
        &self,
        receipt: &ToolCallReceipt,
    ) -> Result<(), ProcessorError> {
        let terminal = receipt.outcome.progress_terminal().ok_or_else(|| {
            ProcessorError::fatal("selected publication lacks terminal watermark")
        })?;
        if terminal.admission().route_id() != self.id() {
            return Err(ProcessorError::fatal("foreign terminal publication"));
        }
        let mut data = self.0.state.lock().unwrap();
        let lease = data
            .leases
            .get_mut(&terminal.lease_id())
            .ok_or_else(|| ProcessorError::fatal("unknown selected terminal lease"))?;
        if lease.published {
            return Ok(());
        }
        if lease
            .terminal_outcome
            .is_some_and(|id| id != receipt.outcome.id())
        {
            return Err(ProcessorError::fatal(
                "conflicting selected terminal identity",
            ));
        }
        lease.terminal_outcome = Some(receipt.outcome.id());
        lease.published = true;
        lease.terminal_deadline = Some(Instant::now() + Self::DISPOSITION_TIMEOUT);
        drop(data);
        if !matches!(
            receipt.admission,
            ToolOutcomeAdmission::Admitted | ToolOutcomeAdmission::ShutdownRace
        ) {
            self.close(NativeToolProgressFailure::QueueClosed);
        }
        self.0.notify();
        Ok(())
    }
    pub fn terminal_consumed(
        &self,
        ctx: &ProcessorContext,
        outcome: &NativeToolCallOutcome,
    ) -> Result<(), ProcessorError> {
        let role = if self
            .check_role(ctx, NativeToolProgressRole::Provider)
            .is_ok()
        {
            NativeToolProgressRole::Provider
        } else {
            self.check_role(ctx, NativeToolProgressRole::Assistant)?;
            NativeToolProgressRole::Assistant
        };
        let terminal = outcome.progress_terminal().ok_or_else(|| {
            ProcessorError::fatal("selected terminal lacks predecessor watermark")
        })?;
        self.validate_admission(ctx, terminal.admission())?;
        let mut data = self.0.state.lock().unwrap();
        let lease = data
            .leases
            .get_mut(&terminal.lease_id())
            .ok_or_else(|| ProcessorError::fatal("unknown selected terminal"))?;
        let was_complete = lease.provider && lease.assistant;
        if role == NativeToolProgressRole::Provider {
            lease.provider = true;
        } else {
            lease.assistant = true;
        }
        let finished = !was_complete && lease.provider && lease.assistant;
        let reporting = lease.reporting;
        if finished && reporting {
            data.active_reporting = data.active_reporting.saturating_sub(1);
        }
        drop(data);
        self.0.notify();
        Ok(())
    }
}

#[derive(Clone)]
pub struct NativeToolProgressPermission {
    owner: Weak<RouteInner>,
    route: Weak<Mutex<bool>>,
    lease: Option<Arc<Mutex<bool>>>,
    lease_id: Option<u64>,
    changes: tokio::sync::watch::Receiver<u64>,
}
impl std::fmt::Debug for NativeToolProgressPermission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("NativeToolProgressPermission(<weak>)")
    }
}
impl NativeToolProgressPermission {
    pub(crate) fn same_route(&self, other: &Self) -> bool {
        self.owner.ptr_eq(&other.owner)
    }
    pub fn reserve_metadata(
        &self,
        bytes: usize,
    ) -> Result<NativeToolProgressMetadataCharge, ProcessorError> {
        NativeToolProgressRoute::from_inner(
            self.owner
                .upgrade()
                .ok_or_else(|| ProcessorError::new("selected metadata owner expired"))?,
        )
        .reserve_metadata(bytes)
    }
    pub fn lease_id(&self) -> Option<u64> {
        self.lease_id
    }
    pub fn is_allowed(&self) -> bool {
        self.with_allowed(|| ()).is_some()
    }
    pub async fn revoked(&self) {
        let mut changes = self.changes.clone();
        loop {
            if !self.is_allowed() {
                return;
            }
            if changes.changed().await.is_err() {
                return;
            }
        }
    }
    /// Permission locks precede canonical runtime admission. Do not await,
    /// route frames, or call a revocation operation from this closure.
    pub fn with_allowed<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        let route = self.route.upgrade()?;
        let route_guard = route.lock().unwrap();
        let lease_guard = self.lease.as_ref().map(|lease| lease.lock().unwrap());
        if !*route_guard || lease_guard.as_ref().is_some_and(|guard| !**guard) {
            return None;
        }
        Some(operation())
    }
}
struct AdmissionInner {
    id: u64,
    route_id: u64,
    route: Weak<RouteInner>,
    call: Arc<FunctionCall>,
    batch: Arc<NativeToolCallBatch>,
    reporting: bool,
    permission: NativeToolProgressPermission,
}
#[derive(Clone)]
pub struct NativeToolProgressAdmission(Arc<AdmissionInner>);
impl std::fmt::Debug for NativeToolProgressAdmission {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeToolProgressAdmission")
            .field("lease_id", &self.lease_id())
            .field("reporting", &self.reporting())
            .finish()
    }
}
impl PartialEq for NativeToolProgressAdmission {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl NativeToolProgressAdmission {
    pub fn lease_id(&self) -> u64 {
        self.0.id
    }
    pub fn route_id(&self) -> u64 {
        self.0.route_id
    }
    pub fn call(&self) -> &FunctionCall {
        &self.0.call
    }
    pub fn batch(&self) -> &Arc<NativeToolCallBatch> {
        &self.0.batch
    }
    pub fn reporting(&self) -> bool {
        self.0.reporting
    }
    pub fn permission(&self) -> NativeToolProgressPermission {
        self.0.permission.clone()
    }
    fn validate_frame(&self, frame: &Frame) -> Result<(), ProcessorError> {
        frame.require_selected_tool_authority(true)?;
        if frame
            .native_tool_batch()?
            .is_none_or(|batch| batch.id() != self.batch().id())
        {
            return Err(ProcessorError::fatal(
                "selected admission batch identity changed",
            ));
        }
        let call = self.call();
        if let FrameData::FunctionCallInProgress(progress) = &frame.data {
            validate_selected_call_copy(
                &progress.function_name,
                &progress.tool_call_id,
                Some(&progress.arguments),
                call,
            )?;
            if progress
                .group_id
                .as_ref()
                .is_some_and(|group| group.capacity() > self.batch().group_id().len())
            {
                return Err(ProcessorError::fatal(
                    "selected declaration retained group changed",
                ));
            }
        }
        let policy = self
            .batch()
            .members()
            .iter()
            .find(|(member, _)| member.as_ref() == call)
            .map(|(_, policy)| *policy);
        match &frame.data {
            FrameData::FunctionCallInProgress(progress)
                if progress.function_name == call.function_name
                    && progress.tool_call_id == call.tool_call_id
                    && progress.arguments == call.arguments
                    && Some(progress.cancel_on_interruption) == policy
                    && progress.group_id.as_deref() == Some(self.batch().group_id()) =>
            {
                Ok(())
            }
            _ => Err(ProcessorError::fatal(
                "selected tool admission does not match declaration",
            )),
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeToolProgressTerminalChoice {
    Complete {
        reason: ToolCallEndReason,
    },
    Cancel {
        reason: ToolCallEndReason,
        run_llm: bool,
    },
}
#[derive(Clone)]
pub struct NativeToolProgressTerminal {
    _charge: Arc<TerminalCharge>,
    admission: NativeToolProgressAdmission,
    predecessor: u64,
    choice: NativeToolProgressTerminalChoice,
}
impl NativeToolProgressTerminal {
    pub fn lease_id(&self) -> u64 {
        self.admission.lease_id()
    }
    pub fn predecessor_ordinal(&self) -> u64 {
        self.predecessor
    }
    pub fn choice(&self) -> NativeToolProgressTerminalChoice {
        self.choice
    }
    pub fn admission(&self) -> &NativeToolProgressAdmission {
        &self.admission
    }
}
struct LeaseControl {
    open: bool,
    ordinal: u64,
    pending: usize,
    reserved: usize,
    terminal: Option<NativeToolProgressTerminal>,
    execution_deadline: Option<Instant>,
    task: Option<u64>,
}
struct LeaseInner {
    terminal_charge: Arc<TerminalCharge>,
    admission: NativeToolProgressAdmission,
    context: crate::processor::WeakToolProgressContext,
    route: Weak<RouteInner>,
    timeout: Duration,
    permission: Arc<Mutex<bool>>,
    control: Mutex<LeaseControl>,
}
impl LeaseInner {
    fn revoke_and_cancel(&self) {
        let task = {
            let mut control = self.control.lock().unwrap();
            control.open = false;
            *self.permission.lock().unwrap() = false;
            control.task
        };
        if let (Some(task), Some(context)) = (task, self.context.upgrade()) {
            context.cancel_task(task);
        }
    }
}
#[derive(Clone)]
pub struct NativeToolProgressLease(Arc<LeaseInner>);
impl std::fmt::Debug for NativeToolProgressLease {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.admission.fmt(f)
    }
}
impl NativeToolProgressLease {
    pub fn id(&self) -> u64 {
        self.0.admission.lease_id()
    }
    pub fn call(&self) -> &FunctionCall {
        self.0.admission.call()
    }
    pub fn batch(&self) -> &Arc<NativeToolCallBatch> {
        self.0.admission.batch()
    }
    pub fn admission(&self) -> NativeToolProgressAdmission {
        self.0.admission.clone()
    }
    pub fn reporting(&self) -> bool {
        self.0.admission.reporting()
    }
    pub fn inference_permission(&self) -> NativeToolProgressPermission {
        self.0.admission.permission()
    }
    pub fn reporter(&self) -> ToolProgressReporter {
        ToolProgressReporter(Arc::downgrade(&self.0))
    }
    pub fn start_execution(&self) -> Result<Instant, ProcessorError> {
        let mut control = self.0.control.lock().unwrap();
        if !control.open {
            return Err(ProcessorError::new(
                "selected tool lease closed before callback",
            ));
        }
        if let Some(deadline) = control.execution_deadline {
            return Ok(deadline);
        }
        let deadline = Instant::now()
            .checked_add(self.0.timeout)
            .ok_or_else(|| ProcessorError::new("selected execution deadline overflow"))?;
        control.execution_deadline = Some(deadline);
        Ok(deadline)
    }
    pub fn execution_deadline(&self) -> Option<Instant> {
        self.0.control.lock().unwrap().execution_deadline
    }
    pub fn set_task(&self, task: u64) -> Result<(), ProcessorError> {
        let mut control = self.0.control.lock().unwrap();
        if control.task.is_some() {
            return Err(ProcessorError::new("selected task already bound"));
        }
        control.task = Some(task);
        let closed = !control.open;
        drop(control);
        if closed {
            if let Some(context) = self.0.context.upgrade() {
                context.cancel_task(task);
            }
        }
        Ok(())
    }
    pub fn freeze_terminal(
        &self,
        choice: NativeToolProgressTerminalChoice,
    ) -> Option<NativeToolProgressTerminal> {
        let context = self.0.context.upgrade();
        let cancel_on_interruption = self
            .batch()
            .members()
            .iter()
            .find(|(call, _)| call.as_ref() == self.call())
            .is_some_and(|(_, policy)| *policy);
        let mut control = self.0.control.lock().unwrap();
        if control.terminal.is_some() {
            return None;
        }
        // Permission precedes canonical runtime admission, matching socket send.
        let mut permission = self.0.permission.lock().unwrap();
        let mut freeze = || {
            let stale = cancel_on_interruption
                && context.as_ref().is_some_and(|context| {
                    let generation = context.current_generation();
                    context.source_is_stale(generation)
                        || context.origins.iter().any(|origin| origin.is_stale())
                        || generation.is_some_and(|generation| {
                            context.has_downstream_processor()
                                && context.runtime.queues[context.index + 1].generation()
                                    > generation
                        })
                });
            let choice =
                if stale && matches!(choice, NativeToolProgressTerminalChoice::Complete { .. }) {
                    NativeToolProgressTerminalChoice::Cancel {
                        reason: ToolCallEndReason::StaleResult,
                        run_llm: false,
                    }
                } else {
                    choice
                };
            control.open = false;
            if !matches!(
                choice,
                NativeToolProgressTerminalChoice::Complete {
                    reason: ToolCallEndReason::CallbackReturned
                }
            ) {
                *permission = false;
            }
            let terminal = NativeToolProgressTerminal {
                _charge: self.0.terminal_charge.clone(),
                admission: self.admission(),
                predecessor: control.ordinal,
                choice,
            };
            control.terminal = Some(terminal.clone());
            terminal
        };
        let terminal = if let Some(context) = &context {
            context.with_admission(&context.runtime, &mut freeze)
        } else {
            freeze()
        };
        drop(permission);
        drop(control);
        Some(terminal)
    }
    pub fn report(
        &self,
        value: serde_json::Value,
        run_llm: Option<bool>,
    ) -> Result<ToolProgressReceipt, ProcessorError> {
        self.reporter().report(value, run_llm)
    }
}
#[derive(Clone)]
pub struct ToolProgressReporter(Weak<LeaseInner>);
pub type NativeToolProgressReporter = ToolProgressReporter;
impl std::fmt::Debug for ToolProgressReporter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ToolProgressReporter(<weak>)")
    }
}
impl ToolProgressReporter {
    pub fn report(
        &self,
        value: serde_json::Value,
        run_llm: Option<bool>,
    ) -> Result<ToolProgressReceipt, ProcessorError> {
        let lease = self
            .0
            .upgrade()
            .ok_or_else(|| ProcessorError::new("tool progress lease expired"))?;
        if !lease.admission.reporting() {
            return Err(ProcessorError::new(
                "tool does not have reporting authority",
            ));
        }
        let value_retained =
            measured_json_retention(&value, NativeToolProgressRoute::MAX_RESULT_BYTES)?;
        let value_bytes = encoded_size(&value, NativeToolProgressRoute::MAX_RESULT_BYTES)?;
        // JSON-encoded JSON strings charge escaping before any owned clone.
        let encoded = serde_json::to_string(&value)
            .map_err(|error| ProcessorError::new(error.to_string()))?;
        let escaped = encoded_size(&encoded, 64 * KIB)?;
        let call_bytes = encoded_size(lease.admission.call(), 32 * KIB)?;
        let argument_retained =
            measured_json_retention(&lease.admission.call().arguments, 32 * KIB)?;
        let bytes = value_bytes
            .checked_mul(5)
            .and_then(|n| n.checked_add(escaped * 2))
            .and_then(|n| n.checked_add(call_bytes * 3))
            .and_then(|n| n.checked_add(value_retained * 5))
            .and_then(|n| n.checked_add(argument_retained * 3))
            .and_then(|n| n.checked_add(4096))
            .filter(|n| *n <= NativeToolProgressRoute::MAX_EVENT_BYTES)
            .ok_or_else(|| ProcessorError::new("tool progress retained representation limit"))?;
        let route = lease
            .route
            .upgrade()
            .ok_or_else(|| ProcessorError::new("tool progress route expired"))?;
        let context = lease
            .context
            .upgrade()
            .ok_or_else(|| ProcessorError::new("tool progress runtime expired"))?;
        let metadata_charge =
            NativeToolProgressRoute::from_inner(route.clone()).reserve_metadata(4096)?;
        // Reserve a pending slot and all logical copies before Control. The
        // reservation has an outside-lock owner through the entire admission.
        let charge = Arc::new(PayloadCharge {
            route: Arc::downgrade(&route),
            bytes,
            active: std::sync::atomic::AtomicBool::new(false),
        });
        let receipt = ToolProgressReceipt(Arc::new(ReceiptInner {
            identity: Mutex::new(ReceiptIdentity {
                event_id: 0,
                lease_id: lease.admission.lease_id(),
                ordinal: 0,
                digest: [0; 32],
            }),
            state: Mutex::new(ReceiptState {
                admitted: false,
                provider_admitted: false,
                history: None,
            }),
            changed: Notify::new(),
        }));
        let mut frame = Frame::new(FrameData::LlmRun).with_interruptible(false);
        let event_id = frame.id;
        {
            let mut data = route.state.lock().unwrap();
            route.reconcile_released(&mut data);
            if !matches!(
                data.phase,
                NativeToolProgressRouteState::Open | NativeToolProgressRouteState::Draining
            ) || data.pending_reports >= NativeToolProgressRoute::MAX_PENDING
                || data.events.len() >= NativeToolProgressRoute::MAX_REPORTS
                || data
                    .core_metadata_bytes
                    .saturating_add(NativeToolProgressRoute::CORE_EVENT_BYTES)
                    > NativeToolProgressRoute::CORE_METADATA_BYTES
                || data
                    .payload_bytes
                    .checked_add(bytes)
                    .is_none_or(|n| n > NativeToolProgressRoute::MAX_PAYLOAD_BYTES)
            {
                return Err(ProcessorError::new(
                    "tool progress route capacity unavailable",
                ));
            }
            data.pending_reports += 1;
            data.core_metadata_bytes += NativeToolProgressRoute::CORE_EVENT_BYTES;
            data.payload_bytes += bytes;
            charge
                .active
                .store(true, std::sync::atomic::Ordering::Release);
            data.events.insert(
                event_id,
                EventRecord {
                    receipt: receipt.0.clone(),
                    provider: false,
                    history: false,
                    deadline: None,
                    lease: Arc::downgrade(&lease),
                },
            );
        }
        frame.data = FrameData::FunctionCallResult(FunctionCallResultFrame {
            function_name: lease.admission.call().function_name.clone(),
            tool_call_id: lease.admission.call().tool_call_id.clone(),
            arguments: lease.admission.call().arguments.clone(),
            result: value,
            run_llm,
            is_final: false,
            error: None,
        });
        let digest =
            progress_digest(&frame.data).expect("premeasured selected progress frame fits 64KiB");
        let mut candidate = frame.clone();
        let result_for_seal = match &frame.data {
            FrameData::FunctionCallResult(result) => result.result.clone(),
            _ => unreachable!(),
        };
        let publication = {
            let mut control = lease.control.lock().unwrap();
            if !control.open
                || control
                    .execution_deadline
                    .is_none_or(|deadline| deadline <= Instant::now())
                || control.ordinal >= NativeToolProgressRoute::MAX_REPORTS_PER_LEASE
                || control.pending + control.reserved
                    >= NativeToolProgressRoute::MAX_PENDING_PER_LEASE
            {
                None
            } else {
                let ordinal = control.ordinal.checked_add(1).expect("bounded ordinal");
                let admitted_at = Instant::now();
                let result = context.with_admission(&context.runtime, || {
                    if !route
                        .publication_open
                        .load(std::sync::atomic::Ordering::Acquire)
                    {
                        return Err(ProcessorError::new(
                            "tool progress route publication closed",
                        ));
                    }
                    let fence = context.fresh_tool_progress_fence()?;
                    let progress = Arc::new(NativeToolProgress {
                        event_id,
                        frame_name: Arc::from(candidate.name.as_str()),
                        admission: lease.admission.clone(),
                        ordinal,
                        result: result_for_seal,
                        run_llm,
                        frame_identity: frame.observation_token(),
                        digest,
                        fence: Arc::new(fence),
                        metadata_charge: metadata_charge.clone(),
                        receipt: receipt.clone(),
                        _charge: charge.clone(),
                        retained_bytes: bytes,
                        result_retained: value_retained,
                    });
                    frame.native_tool_progress = Some(progress.clone());
                    frame.native_tool_progress_restricted = true;
                    candidate.native_tool_progress = Some(progress.clone());
                    candidate.native_tool_progress_restricted = true;
                    {
                        let mut identity = receipt.0.identity.lock().unwrap();
                        identity.event_id = event_id;
                        identity.ordinal = ordinal;
                        identity.digest = digest;
                    }
                    let deferred = context.runtime.route_tool_progress_unlocked(
                        context.index,
                        frame,
                        Direction::Downstream,
                    );
                    Ok::<_, ProcessorError>((deferred, progress))
                });
                Some(result.map(|(deferred, progress)| {
                    if deferred.accepted() {
                        control.ordinal = ordinal;
                        control.pending += 1;
                    }
                    (deferred, progress, admitted_at)
                }))
            }
        };
        let Some(publication) = publication else {
            {
                let mut data = route.state.lock().unwrap();
                data.pending_reports -= 1;
                data.events.remove(&event_id);
                data.core_metadata_bytes -= NativeToolProgressRoute::CORE_EVENT_BYTES;
            }
            drop(candidate);
            drop(charge);
            return Err(ProcessorError::new(
                "tool progress lease closed or report limit reached",
            ));
        };
        let (deferred, progress, admitted_at) = match publication {
            Ok(value) => value,
            Err(error) => {
                {
                    let mut data = route.state.lock().unwrap();
                    data.pending_reports -= 1;
                    data.events.remove(&event_id);
                    data.core_metadata_bytes -= NativeToolProgressRoute::CORE_EVENT_BYTES;
                }
                drop(candidate);
                drop(charge);
                return Err(error);
            }
        };
        let accepted = deferred.accepted();
        if !accepted {
            {
                let mut data = route.state.lock().unwrap();
                data.pending_reports -= 1;
                data.events.remove(&event_id);
                data.core_metadata_bytes -= NativeToolProgressRoute::CORE_EVENT_BYTES;
            }
            let error = deferred
                .finish()
                .err()
                .unwrap_or_else(|| ProcessorError::fatal("tool progress enqueue rejected"));
            NativeToolProgressRoute::from_inner(route)
                .close(NativeToolProgressFailure::QueueClosed);
            return Err(error);
        }
        // Publication is irrevocably admitted. A concurrently closed route may
        // fail its consumer result, but never turn this into a retryable reject.
        let overflow = {
            let mut data = route.state.lock().unwrap();
            data.total_reports += 1;
            if let Some(event) = data.events.get_mut(&event_id) {
                event.deadline = Some(admitted_at + NativeToolProgressRoute::DISPOSITION_TIMEOUT);
            }
            let overflow = data.records.len() >= 256;
            if !overflow {
                data.records.push_back(NativeToolProgressDispositionRecord {
                    lease_id: progress.lease_id(),
                    event_id,
                    ordinal: progress.ordinal(),
                    disposition: NativeToolProgressDisposition::Admitted,
                });
            }
            overflow
        };
        receipt.0.admit();
        let finish = deferred.finish();
        if overflow {
            NativeToolProgressRoute::from_inner(route.clone())
                .close(NativeToolProgressFailure::MailboxFull);
        }
        if finish.is_err() {
            NativeToolProgressRoute::from_inner(route.clone())
                .close(NativeToolProgressFailure::QueueClosed);
        }
        let failure = route.state.lock().unwrap().failure;
        if let Some(failure) = failure {
            receipt.0.fail(failure);
        }
        drop(candidate);
        drop(charge);
        route.notify();
        Ok(receipt)
    }
}
struct PayloadCharge {
    route: Weak<RouteInner>,
    bytes: usize,
    active: std::sync::atomic::AtomicBool,
}
impl Drop for PayloadCharge {
    fn drop(&mut self) {
        if self.active.load(std::sync::atomic::Ordering::Acquire) {
            if let Some(route) = self.route.upgrade() {
                route
                    .released_payload
                    .fetch_add(self.bytes, std::sync::atomic::Ordering::Release);
            }
        }
    }
}
struct ReceiptIdentity {
    event_id: u64,
    lease_id: u64,
    ordinal: u64,
    digest: [u8; 32],
}
struct ReceiptState {
    admitted: bool,
    provider_admitted: bool,
    history: Option<Result<NativeToolProgressHistoryCommit, NativeToolProgressFailure>>,
}
struct ReceiptInner {
    identity: Mutex<ReceiptIdentity>,
    state: Mutex<ReceiptState>,
    changed: Notify,
}
impl ReceiptInner {
    fn admit(&self) {
        self.state.lock().unwrap().admitted = true;
        self.changed.notify_waiters();
    }
    fn fail(&self, error: NativeToolProgressFailure) {
        let mut state = self.state.lock().unwrap();
        if state.history.is_none() {
            state.history = Some(Err(error));
        }
        drop(state);
        self.changed.notify_waiters();
    }
    fn stage_commit(&self, commit: NativeToolProgressHistoryCommit) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().unwrap();
        match state.history {
            Some(Ok(previous)) if previous == commit => Ok(()),
            None => {
                state.history = Some(Ok(commit));
                Ok(())
            }
            _ => Err(ProcessorError::fatal(
                "conflicting or closed tool history receipt",
            )),
        }
    }
    fn commit(&self, commit: NativeToolProgressHistoryCommit) -> Result<(), ProcessorError> {
        self.stage_commit(commit)?;
        self.changed.notify_waiters();
        Ok(())
    }
}
#[derive(Clone)]
pub struct ToolProgressReceipt(Arc<ReceiptInner>);
impl std::fmt::Debug for ToolProgressReceipt {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ToolProgressReceipt")
            .field("event_id", &self.event_id())
            .field("ordinal", &self.ordinal())
            .finish()
    }
}
impl ToolProgressReceipt {
    pub fn event_id(&self) -> u64 {
        self.0.identity.lock().unwrap().event_id
    }
    pub fn lease_id(&self) -> u64 {
        self.0.identity.lock().unwrap().lease_id
    }
    pub fn ordinal(&self) -> u64 {
        self.0.identity.lock().unwrap().ordinal
    }
    pub fn provider_admitted(&self) -> bool {
        let state = self.0.state.lock().unwrap();
        state.admitted && state.provider_admitted
    }
    pub fn history(
        &self,
    ) -> Option<Result<NativeToolProgressHistoryCommit, NativeToolProgressFailure>> {
        let state = self.0.state.lock().unwrap();
        if state.admitted {
            state.history
        } else {
            None
        }
    }
    pub async fn await_history(
        &self,
    ) -> Result<NativeToolProgressHistoryCommit, NativeToolProgressFailure> {
        loop {
            let notified = self.0.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.history() {
                return result;
            }
            notified.await;
        }
    }
}
#[derive(Clone)]
pub struct NativeToolProgress {
    event_id: u64,
    frame_name: Arc<str>,
    admission: NativeToolProgressAdmission,
    ordinal: u64,
    result: serde_json::Value,
    run_llm: Option<bool>,
    frame_identity: FrameObservationToken,
    digest: [u8; 32],
    fence: Arc<InvocationFence>,
    metadata_charge: NativeToolProgressMetadataCharge,
    receipt: ToolProgressReceipt,
    _charge: Arc<PayloadCharge>,
    retained_bytes: usize,
    result_retained: usize,
}
impl std::fmt::Debug for NativeToolProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeToolProgress")
            .field("id", &self.id())
            .field("lease_id", &self.lease_id())
            .field("ordinal", &self.ordinal())
            .finish()
    }
}
impl PartialEq for NativeToolProgress {
    fn eq(&self, other: &Self) -> bool {
        self.event_id == other.event_id
            && self.admission == other.admission
            && self.frame_identity == other.frame_identity
            && self.digest == other.digest
    }
}
impl NativeToolProgress {
    pub fn id(&self) -> u64 {
        self.event_id
    }
    pub fn lease_id(&self) -> u64 {
        self.admission.lease_id()
    }
    pub fn ordinal(&self) -> u64 {
        self.ordinal
    }
    pub fn call(&self) -> &FunctionCall {
        self.admission.call()
    }
    pub fn batch(&self) -> &Arc<NativeToolCallBatch> {
        self.admission.batch()
    }
    pub fn result(&self) -> &serde_json::Value {
        &self.result
    }
    pub fn run_llm(&self) -> Option<bool> {
        self.run_llm
    }
    pub fn publication_fence(&self) -> &InvocationFence {
        &self.fence
    }
    pub(crate) fn shared_publication_fence(&self) -> Arc<InvocationFence> {
        self.fence.clone()
    }
    pub(crate) fn publication_metadata_charge(&self) -> NativeToolProgressMetadataCharge {
        self.metadata_charge.clone()
    }
    pub fn admission(&self) -> &NativeToolProgressAdmission {
        &self.admission
    }
    pub fn receipt(&self) -> ToolProgressReceipt {
        self.receipt.clone()
    }
    pub fn retained_bytes(&self) -> usize {
        self.retained_bytes
    }
    fn validate(&self, frame: &Frame) -> Result<(), ProcessorError> {
        frame.require_selected_tool_authority(false)?;
        let FrameData::FunctionCallResult(result) = &frame.data else {
            return Err(ProcessorError::fatal(
                "selected progress payload family changed",
            ));
        };
        validate_selected_call_copy(
            &result.function_name,
            &result.tool_call_id,
            Some(&result.arguments),
            self.call(),
        )?;
        if measured_json_retention(&result.result, NativeToolProgressRoute::MAX_RESULT_BYTES)?
            > self.result_retained
        {
            return Err(ProcessorError::fatal(
                "selected progress retained payload changed",
            ));
        }
        if frame.id != self.event_id
            || frame.name != self.frame_name.as_ref()
            || frame.observation_token() != self.frame_identity
            || frame.interruptible
            || frame.native_tool_outcome()?.is_some()
            || frame.native_tool_batch()?.is_some()
            || frame.native_realtime_context_cause()?.is_some()
            || self.digest != progress_digest(&frame.data)?
        {
            return Err(ProcessorError::fatal(
                "tool progress does not match its sealed event",
            ));
        }
        match &frame.data {
            FrameData::FunctionCallResult(result)
                if !result.is_final
                    && result.error.is_none()
                    && result.function_name == self.call().function_name
                    && result.tool_call_id == self.call().tool_call_id
                    && result.arguments == self.call().arguments
                    && result.result == self.result
                    && result.run_llm == self.run_llm =>
            {
                Ok(())
            }
            _ => Err(ProcessorError::fatal(
                "tool progress payload or policy changed",
            )),
        }
    }
}
impl Frame {
    pub fn set_native_tool_progress_admission(
        &mut self,
        admission: NativeToolProgressAdmission,
    ) -> Result<(), ProcessorError> {
        admission.validate_frame(self)?;
        if self.native_tool_progress_admission.is_some()
            || self.native_tool_progress.is_some()
            || self.native_tool_outcome()?.is_some()
        {
            return Err(ProcessorError::new(
                "conflicting tool progress declaration authority",
            ));
        }
        self.native_tool_progress_admission = Some(admission);
        self.native_tool_progress_restricted = true;
        Ok(())
    }
    pub fn native_tool_progress_admission(
        &self,
    ) -> Result<Option<&NativeToolProgressAdmission>, ProcessorError> {
        if let Some(admission) = &self.native_tool_progress_admission {
            admission.validate_frame(self)?;
        }
        Ok(self.native_tool_progress_admission.as_ref())
    }
    pub fn take_native_tool_progress_admission(
        &mut self,
    ) -> Result<Option<NativeToolProgressAdmission>, ProcessorError> {
        self.native_tool_progress_admission()?;
        Ok(self.native_tool_progress_admission.take())
    }
    pub fn native_tool_progress(&self) -> Result<Option<&Arc<NativeToolProgress>>, ProcessorError> {
        if let Some(progress) = &self.native_tool_progress {
            progress.validate(self)?;
        } else if self.native_tool_progress_restricted
            && self.native_tool_progress_admission.is_none()
            && self.native_tool_outcome()?.is_none()
        {
            return Err(ProcessorError::fatal("selected tool carrier was stripped"));
        }
        Ok(self.native_tool_progress.as_ref())
    }
    pub fn take_native_tool_progress(
        &mut self,
    ) -> Result<Option<Arc<NativeToolProgress>>, ProcessorError> {
        self.native_tool_progress()?;
        Ok(self.native_tool_progress.take())
    }
    pub fn has_native_tool_progress_restriction(&self) -> bool {
        self.native_tool_progress_restricted || self.native_tool_progress_context_restricted
    }
}

pub(crate) fn validate_json(
    value: &serde_json::Value,
    byte_limit: usize,
) -> Result<(), ProcessorError> {
    measured_json_retention(value, byte_limit).map(|_| ())
}
pub(crate) fn measured_json_retention(
    value: &serde_json::Value,
    byte_limit: usize,
) -> Result<usize, ProcessorError> {
    let mut stack = vec![(value, 0usize)];
    let mut nodes = 0usize;
    let mut strings = 0usize;
    let mut retained = 0usize;
    while let Some((value, depth)) = stack.pop() {
        nodes += 1;
        if nodes > 4096 || depth > 32 {
            return Err(ProcessorError::new(
                "tool JSON depth or node limit exceeded",
            ));
        }
        match value {
            serde_json::Value::String(value) => {
                retained = retained.saturating_add(value.capacity());
                strings = strings
                    .checked_add(value.len())
                    .ok_or_else(|| ProcessorError::new("tool JSON strings overflow"))?;
                if value.capacity() > byte_limit {
                    return Err(ProcessorError::new(
                        "tool JSON retained string capacity exceeded",
                    ));
                }
            }
            serde_json::Value::Array(values) => {
                retained = retained.saturating_add(
                    values
                        .capacity()
                        .saturating_mul(std::mem::size_of::<serde_json::Value>()),
                );
                if values
                    .capacity()
                    .saturating_mul(std::mem::size_of::<serde_json::Value>())
                    > NativeToolProgressRoute::MAX_EVENT_BYTES
                    || stack.len().saturating_add(values.len()) > 4096
                {
                    return Err(ProcessorError::new(
                        "tool JSON retained array capacity exceeded",
                    ));
                }
                stack.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                if stack.len().saturating_add(values.len()) > 4096 {
                    return Err(ProcessorError::new("tool JSON object limit exceeded"));
                }
                for (key, value) in values {
                    retained = retained
                        .saturating_add(key.capacity())
                        .saturating_add(std::mem::size_of::<serde_json::Value>() + 64);
                    strings = strings
                        .checked_add(key.len())
                        .ok_or_else(|| ProcessorError::new("tool JSON keys overflow"))?;
                    if key.capacity() > byte_limit {
                        return Err(ProcessorError::new(
                            "tool JSON retained key capacity exceeded",
                        ));
                    }
                    stack.push((value, depth + 1));
                }
            }
            _ => (),
        }
        if retained > NativeToolProgressRoute::MAX_EVENT_BYTES / 4 {
            return Err(ProcessorError::new(
                "tool JSON cumulative retained capacity exceeded",
            ));
        }
        if strings > byte_limit {
            return Err(ProcessorError::new(
                "tool JSON cumulative string limit exceeded",
            ));
        }
    }
    encoded_size(value, byte_limit)?;
    Ok(retained.saturating_add(std::mem::size_of::<serde_json::Value>()))
}
pub(crate) fn encoded_size(
    value: &impl serde::Serialize,
    limit: usize,
) -> Result<usize, ProcessorError> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl std::io::Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|n| *n <= self.limit)
                .ok_or_else(|| std::io::Error::other("tool progress encoding limit"))?;
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
fn progress_digest(data: &FrameData) -> Result<[u8; 32], ProcessorError> {
    let FrameData::FunctionCallResult(result) = data else {
        return Err(ProcessorError::fatal(
            "selected progress payload family changed",
        ));
    };
    validate_json(&result.result, NativeToolProgressRoute::MAX_RESULT_BYTES)?;
    validate_json(&result.arguments, 32 * KIB)?;

    struct DigestWriter(Sha256, usize);
    impl std::io::Write for DigestWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.1 = self
                .1
                .checked_add(bytes.len())
                .filter(|n| *n <= 64 * KIB)
                .ok_or_else(|| std::io::Error::other("progress frame bound"))?;
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = DigestWriter(Sha256::new(), 0);
    serde_json::to_writer(&mut writer, data)
        .map_err(|error| ProcessorError::new(error.to_string()))?;
    Ok(writer.0.finalize().into())
}

/// Exact synchronous enqueue result plus outside-lock cleanup. Accepted means
/// local queue admission only; never socket delivery, terminal completion or join.
#[must_use = "finish deferred failure outside all owner/admission locks"]
pub struct NativeToolProgressEnqueue {
    pub(crate) runtime: Arc<crate::pipeline::Runtime>,
    pub(crate) error: Option<ProcessorError>,
    pub(crate) dropped: Option<Box<crate::DirectedFrame>>,
    pub(crate) failure: Option<crate::PipelineError>,
}
impl NativeToolProgressEnqueue {
    pub fn accepted(&self) -> bool {
        self.error.is_none()
    }
    pub(crate) fn rejected(
        runtime: Arc<crate::pipeline::Runtime>,
        frame: Frame,
        direction: Direction,
        error: ProcessorError,
    ) -> Self {
        Self {
            runtime,
            error: Some(error),
            dropped: Some(Box::new(crate::DirectedFrame::new(frame, direction))),
            failure: None,
        }
    }
    pub fn finish(self) -> Result<(), ProcessorError> {
        if let Some(failure) = self.failure {
            self.runtime.fail(failure);
        }
        drop(self.dropped);
        match self.error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl NativeToolProgressRoute {
    pub fn register_gate_context(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Gate)?;
        let mut gate = self.0.gate.lock().unwrap();
        if gate.context.is_some() {
            return Err(ProcessorError::fatal(
                "selected gate context already registered",
            ));
        }
        gate.context = Some(ctx.clone());
        Ok(())
    }
    pub fn hold_end(&self, frame: Frame) -> Result<(), ProcessorError> {
        let reason_bytes = match &frame.data {
            FrameData::End(end) => end.reason.as_ref().map_or(0, String::capacity),
            FrameData::EndDetailed(end) => end
                .reason
                .as_ref()
                .map(|value| measured_json_retention(value, 64 * KIB))
                .transpose()?
                .unwrap_or(0),
            _ => return Err(ProcessorError::new("selected gate can only hold End")),
        };
        let mut retained = reason_bytes
            .saturating_add(frame.name.capacity())
            .saturating_add(frame.transport_source.as_ref().map_or(0, String::capacity))
            .saturating_add(
                frame
                    .transport_destination
                    .as_ref()
                    .map_or(0, String::capacity),
            );
        for (key, value) in &frame.metadata {
            retained = retained
                .saturating_add(key.capacity())
                .saturating_add(64)
                .saturating_add(measured_json_retention(value, 16 * KIB)?);
        }
        if retained > 64 * KIB {
            return Err(ProcessorError::new("selected held End retained byte limit"));
        }
        encoded_size(
            &(
                &frame.metadata,
                &frame.data,
                &frame.name,
                &frame.transport_source,
                &frame.transport_destination,
            ),
            64 * KIB,
        )?;
        if !matches!(frame.data, FrameData::End(_) | FrameData::EndDetailed(_)) {
            return Err(ProcessorError::new("selected gate can only hold End"));
        }
        let mut gate = self.0.gate.lock().unwrap();
        let mut permit = self.0.inference.lock().unwrap();
        let mut data = self.0.state.lock().unwrap();
        if gate.context.is_none()
            || gate.held.is_some()
            || !matches!(
                data.phase,
                NativeToolProgressRouteState::Open | NativeToolProgressRouteState::Draining
            )
        {
            return Err(ProcessorError::fatal("selected gate End cannot be held"));
        }
        let deadline = data.drain_deadline.unwrap_or(
            Instant::now()
                .checked_add(self.0.config.end_drain_timeout)
                .ok_or_else(|| ProcessorError::fatal("tool progress drain deadline overflow"))?,
        );
        gate.held = Some(frame);
        *permit = false;
        data.phase = NativeToolProgressRouteState::Draining;
        data.drain_deadline = Some(deadline);
        drop(data);
        drop(permit);
        drop(gate);
        self.0.notify();
        Ok(())
    }
    /// Linearize terminal choice with synchronous gate-origin enqueue. Call
    /// finish() on the returned work only after releasing any outer owner lock.
    pub fn forward_held_terminal(
        &self,
    ) -> Result<Option<NativeToolProgressEnqueue>, ProcessorError> {
        let mut gate = self.0.gate.lock().unwrap();
        let mut data = self.0.state.lock().unwrap();
        if data.terminal_forwarded {
            return Ok(None);
        }
        let stop = data.phase == NativeToolProgressRouteState::Stopping;
        if !stop
            && (data.phase != NativeToolProgressRouteState::Draining
                || data.pending_reports != 0
                || !data
                    .leases
                    .values()
                    .all(|lease| lease.published && lease.provider && lease.assistant))
        {
            return Ok(None);
        }
        if !stop && gate.held.is_none() {
            return Ok(None);
        }
        let context = gate
            .context
            .as_ref()
            .ok_or_else(|| ProcessorError::fatal("selected terminal lost its gate route"))?
            .clone();
        let mut superseded = None;
        let frame = if stop {
            superseded = gate.held.take();
            Frame::new(FrameData::Stop)
        } else {
            gate.held
                .take()
                .ok_or_else(|| ProcessorError::fatal("selected End missing held frame"))?
        };
        let deferred = context.prepare_selected_tool_terminal(frame, Direction::Downstream);
        if deferred.accepted() {
            data.terminal_forwarded = true;
            data.phase = if stop {
                NativeToolProgressRouteState::Stopping
            } else {
                NativeToolProgressRouteState::EndForwarded
            };
        } else {
            data.phase = NativeToolProgressRouteState::Failed;
            data.failure = Some(NativeToolProgressFailure::QueueClosed);
        }
        let retained_context = gate.context.take();
        drop(data);
        drop(gate);
        drop(superseded);
        drop(retained_context);
        drop(context);
        self.0.notify();
        Ok(Some(deferred))
    }
    pub fn owner_guard(&self) -> NativeToolProgressOwnerGuard {
        NativeToolProgressOwnerGuard(self.clone())
    }
    pub fn history_commit(&self, event_id: u64) -> Option<NativeToolProgressHistoryCommit> {
        self.0
            .state
            .lock()
            .unwrap()
            .events
            .get(&event_id)
            .and_then(|event| {
                let state = event.receipt.state.lock().unwrap();
                if state.admitted {
                    state.history.and_then(Result::ok)
                } else {
                    None
                }
            })
    }
    pub fn admission_history_commit(
        &self,
        lease_id: u64,
    ) -> Option<NativeToolProgressHistoryCommit> {
        self.0
            .state
            .lock()
            .unwrap()
            .admission_history
            .get(&lease_id)
            .copied()
    }
    pub fn terminal_history_commit(
        &self,
        outcome_id: u64,
    ) -> Option<NativeToolProgressHistoryCommit> {
        self.0
            .state
            .lock()
            .unwrap()
            .terminal_history
            .get(&outcome_id)
            .copied()
    }
    pub fn admission_history_committed(
        &self,
        ctx: &ProcessorContext,
        admission: &NativeToolProgressAdmission,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<(), ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        self.validate_admission(ctx, admission)?;
        if commit.start >= commit.end || commit.end > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::fatal(
                "invalid selected admission history range",
            ));
        }
        let mut data = self.0.state.lock().unwrap();
        if data
            .admission_history
            .get(&admission.lease_id())
            .is_some_and(|old| old != &commit)
        {
            return Err(ProcessorError::fatal(
                "conflicting selected admission history attribution",
            ));
        }
        data.admission_history.insert(admission.lease_id(), commit);
        drop(data);
        self.0.notify();
        Ok(())
    }
    pub fn terminal_history_committed(
        &self,
        ctx: &ProcessorContext,
        outcome: &NativeToolCallOutcome,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<(), ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        if commit.start >= commit.end || commit.end > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::fatal(
                "invalid selected terminal history range",
            ));
        }
        let terminal = outcome
            .progress_terminal()
            .ok_or_else(|| ProcessorError::fatal("selected terminal authority missing"))?;
        self.validate_admission(ctx, terminal.admission())?;
        let mut data = self.0.state.lock().unwrap();
        if data
            .terminal_history
            .get(&outcome.id())
            .is_some_and(|old| old != &commit)
        {
            return Err(ProcessorError::fatal(
                "conflicting selected terminal history attribution",
            ));
        }
        let lease = data
            .leases
            .get_mut(&terminal.lease_id())
            .ok_or_else(|| ProcessorError::fatal("unknown selected terminal"))?;
        if lease.terminal_outcome.is_some_and(|id| id != outcome.id()) {
            return Err(ProcessorError::fatal(
                "conflicting selected terminal identity",
            ));
        }
        lease.terminal_outcome = Some(outcome.id());
        data.terminal_history.insert(outcome.id(), commit);
        drop(data);
        self.terminal_consumed(ctx, outcome)
    }
    /// Admit a bounded dispatch batch; rollback is permitted only before any
    /// declaration or callback escapes to another owner.
    pub fn admit_batch(
        &self,
        ctx: &ProcessorContext,
        batch: Arc<NativeToolCallBatch>,
        policies: Vec<(bool, Duration)>,
    ) -> Result<Vec<NativeToolProgressLease>, ProcessorError> {
        if policies.len() != batch.members().len() {
            return Err(ProcessorError::new(
                "selected admission batch policy length mismatch",
            ));
        }
        let mut leases: Vec<NativeToolProgressLease> = Vec::with_capacity(policies.len());
        for ((call, _), (reporting, timeout)) in batch.members().iter().zip(policies) {
            match self.admit_call(ctx, call.clone(), batch.clone(), reporting, timeout) {
                Ok(lease) => leases.push(lease),
                Err(error) => {
                    let removed = {
                        let mut data = self.0.state.lock().unwrap();
                        let mut removed = Vec::new();
                        for lease in &leases {
                            if let Some(entry) = data.leases.remove(&lease.id()) {
                                data.core_metadata_bytes -= Self::CORE_LEASE_BYTES;
                                lease
                                    .0
                                    .terminal_charge
                                    .active
                                    .store(false, std::sync::atomic::Ordering::Release);
                                data.terminal_bytes =
                                    data.terminal_bytes.saturating_sub(Self::MAX_EVENT_BYTES);
                                if entry.reporting {
                                    data.active_reporting = data.active_reporting.saturating_sub(1);
                                }
                                removed.push(entry);
                            }
                        }
                        removed
                    };
                    drop(removed);
                    return Err(error);
                }
            }
        }
        Ok(leases)
    }
}
pub struct NativeToolProgressOwnerGuard(NativeToolProgressRoute);
impl Drop for NativeToolProgressOwnerGuard {
    fn drop(&mut self) {
        let complete = {
            let data = self.0 .0.state.lock().unwrap();
            data.terminal_forwarded || data.phase == NativeToolProgressRouteState::Closed
        };
        if !complete {
            self.0.close(NativeToolProgressFailure::OwnerDropped);
        }
    }
}
impl NativeToolProgressLease {
    pub fn validate_terminal_result(
        &self,
        result: &Result<serde_json::Value, String>,
    ) -> Result<(), ProcessorError> {
        let retained = match result {
            Ok(value) => {
                measured_json_retention(value, NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES)?
            }
            Err(error) => error.capacity(),
        };
        let result_bytes = match result {
            Ok(value) => {
                validate_json(value, NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES)?;
                encoded_size(value, NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES)?
            }
            Err(error) => {
                if error.capacity() > NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES {
                    return Err(ProcessorError::new(
                        "selected terminal retained error capacity",
                    ));
                }
                encoded_size(
                    error,
                    NativeToolProgressRoute::MAX_TERMINAL_RESULT_BYTES / 2,
                )?
            }
        };
        let identity = encoded_size(self.call(), 32 * KIB)?;
        let identity_retained = measured_json_retention(&self.call().arguments, 32 * KIB)?;
        if result_bytes
            .checked_mul(3)
            .and_then(|n| n.checked_add(retained * 3))
            .and_then(|n| n.checked_add(identity * 2))
            .and_then(|n| n.checked_add(identity_retained * 3))
            .and_then(|n| n.checked_add(4096))
            .is_none_or(|n| n > NativeToolProgressRoute::MAX_EVENT_BYTES)
        {
            return Err(ProcessorError::new(
                "selected terminal representations exceed reservation",
            ));
        }
        Ok(())
    }
}
impl RouteInner {
    fn notify(&self) {
        self.changed
            .send_modify(|generation| *generation = generation.wrapping_add(1));
    }
}
struct TerminalCharge {
    route: Weak<RouteInner>,
    active: std::sync::atomic::AtomicBool,
}
impl Drop for TerminalCharge {
    fn drop(&mut self) {
        if self.active.load(std::sync::atomic::Ordering::Acquire) {
            if let Some(route) = self.route.upgrade() {
                route.released_terminal.fetch_add(
                    NativeToolProgressRoute::MAX_EVENT_BYTES,
                    std::sync::atomic::Ordering::Release,
                );
            }
        }
    }
}

/// A compact staged notification. Finish outside assistant/context/history locks.
#[must_use = "notify the staged history fact after releasing history locks"]
pub struct NativeToolProgressHistoryNotification {
    route: Weak<RouteInner>,
    receipt: Arc<ReceiptInner>,
    event_id: u64,
    lease_id: u64,
    ordinal: u64,
    commit: NativeToolProgressHistoryCommit,
}
impl NativeToolProgressHistoryNotification {
    pub fn notify(self) -> Result<(), ProcessorError> {
        self.receipt.changed.notify_waiters();
        if let Some(route) = self.route.upgrade() {
            NativeToolProgressRoute::from_inner(route).consume_identified_event(
                self.event_id,
                self.lease_id,
                self.ordinal,
                &self.receipt,
                Some(self.commit),
            )
        } else {
            Ok(())
        }
    }
}

/// Private metadata reservation for selected consumer/actor scheduling copies.
/// This is a logical owner charge; it is not an RSS estimate.
#[derive(Clone)]
pub struct NativeToolProgressMetadataCharge(Arc<MetadataCharge>);
struct MetadataCharge {
    route: Weak<RouteInner>,
    bytes: usize,
}
impl Drop for MetadataCharge {
    fn drop(&mut self) {
        if let Some(route) = self.route.upgrade() {
            route
                .released_metadata
                .fetch_add(self.bytes, std::sync::atomic::Ordering::Release);
        }
    }
}
impl NativeToolProgressRoute {
    /// Fixed compact route maps/mailbox reserve 768KiB of the 1MiB pool. The
    /// remaining 256KiB bounds all externally held selected scheduling copies.
    pub fn reserve_metadata(
        &self,
        bytes: usize,
    ) -> Result<NativeToolProgressMetadataCharge, ProcessorError> {
        let mut data = self.0.state.lock().unwrap();
        self.0.reconcile_released(&mut data);
        let next = data
            .metadata_bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= 256 * KIB)
            .ok_or_else(|| ProcessorError::new("selected progress metadata budget exhausted"))?;
        data.metadata_bytes = next;
        Ok(NativeToolProgressMetadataCharge(Arc::new(MetadataCharge {
            route: Arc::downgrade(&self.0),
            bytes,
        })))
    }
    pub fn stage_admission_history_committed(
        &self,
        ctx: &ProcessorContext,
        admission: &NativeToolProgressAdmission,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<NativeToolProgressContextNotification, ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        self.validate_admission(ctx, admission)?;
        if commit.start >= commit.end || commit.end > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::fatal(
                "invalid selected admission history range",
            ));
        }
        let mut data = self.0.state.lock().unwrap();
        if data
            .admission_history
            .get(&admission.lease_id())
            .is_some_and(|old| old != &commit)
        {
            return Err(ProcessorError::fatal(
                "conflicting selected admission history attribution",
            ));
        }
        data.admission_history.insert(admission.lease_id(), commit);
        Ok(NativeToolProgressContextNotification {
            route: Arc::downgrade(&self.0),
            terminal: None,
        })
    }
    pub fn stage_terminal_history_committed(
        &self,
        ctx: &ProcessorContext,
        outcome: &NativeToolCallOutcome,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<NativeToolProgressContextNotification, ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        if commit.start >= commit.end || commit.end > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::fatal(
                "invalid selected terminal history range",
            ));
        }
        let terminal = outcome
            .progress_terminal()
            .ok_or_else(|| ProcessorError::fatal("selected terminal authority missing"))?;
        self.validate_admission(ctx, terminal.admission())?;
        let mut data = self.0.state.lock().unwrap();
        if data
            .terminal_history
            .get(&outcome.id())
            .is_some_and(|old| old != &commit)
        {
            return Err(ProcessorError::fatal(
                "conflicting selected terminal history attribution",
            ));
        }
        let lease = data
            .leases
            .get_mut(&terminal.lease_id())
            .ok_or_else(|| ProcessorError::fatal("unknown selected terminal"))?;
        if lease.terminal_outcome.is_some_and(|id| id != outcome.id()) {
            return Err(ProcessorError::fatal(
                "conflicting selected terminal identity",
            ));
        }
        lease.terminal_outcome = Some(outcome.id());
        data.terminal_history.insert(outcome.id(), commit);
        Ok(NativeToolProgressContextNotification {
            route: Arc::downgrade(&self.0),
            terminal: Some((terminal.lease_id(), false)),
        })
    }
}
/// Actual history attribution is already staged; only bookkeeping/notification
/// remains. There is deliberately no destructor with notification side effects.
#[must_use]
pub struct NativeToolProgressContextNotification {
    route: Weak<RouteInner>,
    terminal: Option<(u64, bool)>,
}
impl NativeToolProgressContextNotification {
    pub fn notify(self) -> Result<(), ProcessorError> {
        if let Some(route) = self.route.upgrade() {
            if let Some((id, provider)) = self.terminal {
                let mut data = route.state.lock().unwrap();
                let lease = data
                    .leases
                    .get_mut(&id)
                    .ok_or_else(|| ProcessorError::fatal("lost staged terminal identity"))?;
                let was_finished = lease.assistant && lease.provider;
                if provider {
                    lease.provider = true;
                } else {
                    lease.assistant = true;
                }
                let finished = !was_finished && lease.assistant && lease.provider;
                let reporting = lease.reporting;
                if finished && reporting {
                    data.active_reporting = data.active_reporting.saturating_sub(1);
                }
            }
            route.notify();
        }
        Ok(())
    }
}

impl NativeToolProgressHistoryCommit {
    pub fn new(
        revision: u64,
        start: usize,
        end: usize,
        messages: &[crate::ContextMessage],
    ) -> Result<Self, ProcessorError> {
        if revision == 0
            || start >= end
            || end > messages.len()
            || messages.len() > NativeToolProgressRoute::MAX_HISTORY_MESSAGES
            || end - start > 2
        {
            return Err(ProcessorError::new(
                "invalid selected history attribution range",
            ));
        }
        let mut message_digests = [[0; 32]; 2];
        for (offset, message) in messages[start..end].iter().enumerate() {
            message_digests[offset] = history_range_digest(std::slice::from_ref(message))?;
        }
        Ok(Self {
            revision,
            start,
            end,
            message_digests,
            digest: history_range_digest(&messages[start..end])?,
        })
    }
    pub fn matches_index(
        &self,
        messages: &[crate::ContextMessage],
        index: usize,
    ) -> Result<bool, ProcessorError> {
        if index < self.start
            || index >= self.end
            || index >= messages.len()
            || index - self.start >= 2
        {
            return Ok(false);
        }
        Ok(self.message_digests[index - self.start]
            == history_range_digest(std::slice::from_ref(&messages[index]))?)
    }
    pub fn matches(&self, messages: &[crate::ContextMessage]) -> Result<bool, ProcessorError> {
        if self.start >= self.end
            || self.end > messages.len()
            || messages.len() > NativeToolProgressRoute::MAX_HISTORY_MESSAGES
        {
            return Ok(false);
        }
        Ok(self.digest == history_range_digest(&messages[self.start..self.end])?)
    }
}
fn history_range_digest(messages: &[crate::ContextMessage]) -> Result<[u8; 32], ProcessorError> {
    for message in messages {
        if message.tool_calls.len() > 1024 || message.extra.len() > 4096 {
            return Err(ProcessorError::new(
                "selected history nested collection limit",
            ));
        }
        for call in &message.tool_calls {
            validate_json(&call.arguments, 1024 * KIB)?;
        }
        for value in message.extra.values() {
            validate_json(value, 1024 * KIB)?;
        }
    }
    struct BoundedHistoryDigest(Sha256, usize);
    impl std::io::Write for BoundedHistoryDigest {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.1 = self
                .1
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= 1024 * KIB)
                .ok_or_else(|| {
                    std::io::Error::other("selected history range exceeds byte bound")
                })?;
            self.0.update(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut writer = BoundedHistoryDigest(Sha256::new(), 0);
    serde_json::to_writer(&mut writer, messages)
        .map_err(|error| ProcessorError::new(error.to_string()))?;
    Ok(writer.0.finalize().into())
}

#[cfg(test)]
#[path = "tool_progress_tests.rs"]
mod tests;

impl std::fmt::Debug for NativeToolProgressTerminal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeToolProgressTerminal")
            .field("lease_id", &self.lease_id())
            .field("predecessor", &self.predecessor)
            .field("choice", &self.choice)
            .finish()
    }
}
impl PartialEq for NativeToolProgressTerminal {
    fn eq(&self, other: &Self) -> bool {
        self.admission == other.admission
            && self.predecessor == other.predecessor
            && self.choice == other.choice
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NativeToolProgressTerminalSummary {
    pub route_id: u64,
    pub lease_id: u64,
    pub predecessor_ordinal: u64,
    pub choice: NativeToolProgressTerminalChoice,
}
impl NativeToolProgressTerminal {
    pub fn summary(&self) -> NativeToolProgressTerminalSummary {
        NativeToolProgressTerminalSummary {
            route_id: self.admission.route_id(),
            lease_id: self.lease_id(),
            predecessor_ordinal: self.predecessor,
            choice: self.choice,
        }
    }
}
impl std::fmt::Debug for NativeToolProgressMetadataCharge {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NativeToolProgressMetadataCharge")
            .field("bytes", &self.0.bytes)
            .finish()
    }
}

impl RouteInner {
    fn reconcile_released(&self, data: &mut RouteData) {
        use std::sync::atomic::Ordering;
        data.payload_bytes = data
            .payload_bytes
            .saturating_sub(self.released_payload.swap(0, Ordering::AcqRel));
        data.terminal_bytes = data
            .terminal_bytes
            .saturating_sub(self.released_terminal.swap(0, Ordering::AcqRel));
        data.metadata_bytes = data
            .metadata_bytes
            .saturating_sub(self.released_metadata.swap(0, Ordering::AcqRel));
    }
}

#[must_use]
pub struct NativeToolProgressProviderNotification {
    route: Weak<RouteInner>,
    receipt: Arc<ReceiptInner>,
    event_id: u64,
    lease_id: u64,
    ordinal: u64,
}
impl NativeToolProgressProviderNotification {
    pub fn notify(self) -> Result<(), ProcessorError> {
        if let Some(route) = self.route.upgrade() {
            NativeToolProgressRoute::from_inner(route).consume_identified_event(
                self.event_id,
                self.lease_id,
                self.ordinal,
                &self.receipt,
                None,
            )
        } else {
            Ok(())
        }
    }
}
impl NativeToolProgressRoute {
    pub fn stage_terminal_consumed(
        &self,
        ctx: &ProcessorContext,
        outcome: &NativeToolCallOutcome,
    ) -> Result<NativeToolProgressContextNotification, ProcessorError> {
        let provider = self
            .check_role(ctx, NativeToolProgressRole::Provider)
            .is_ok();
        if !provider {
            self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        }
        let terminal = outcome
            .progress_terminal()
            .ok_or_else(|| ProcessorError::fatal("selected terminal missing watermark"))?;
        self.validate_admission(ctx, terminal.admission())?;
        {
            let mut data = self.0.state.lock().unwrap();
            let lease = data
                .leases
                .get_mut(&terminal.lease_id())
                .ok_or_else(|| ProcessorError::fatal("unknown selected terminal"))?;
            if lease.terminal_outcome.is_some_and(|id| id != outcome.id()) {
                return Err(ProcessorError::fatal(
                    "conflicting selected terminal identity",
                ));
            }
            lease.terminal_outcome = Some(outcome.id());
            if provider {
                lease.provider_staged = true;
            }
        }
        Ok(NativeToolProgressContextNotification {
            route: Arc::downgrade(&self.0),
            terminal: Some((terminal.lease_id(), provider)),
        })
    }
}

impl NativeToolProgressRoute {
    pub fn stage_batch_admission_history_committed(
        &self,
        ctx: &ProcessorContext,
        admission: &NativeToolProgressAdmission,
        sibling_call_id: &str,
        commit: NativeToolProgressHistoryCommit,
    ) -> Result<NativeToolProgressContextNotification, ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Assistant)?;
        self.validate_admission(ctx, admission)?;
        if !admission
            .batch()
            .members()
            .iter()
            .any(|(call, _)| call.tool_call_id == sibling_call_id)
            || commit.start >= commit.end
            || commit.end > Self::MAX_HISTORY_MESSAGES
        {
            return Err(ProcessorError::fatal(
                "invalid selected sibling history attribution",
            ));
        }
        let digest: [u8; 32] = Sha256::digest(sibling_call_id.as_bytes()).into();
        let mut data = self.0.state.lock().unwrap();
        let id = data
            .leases
            .iter()
            .find_map(|(id, lease)| {
                (lease.batch_id == admission.batch().id() && lease.call_id_digest == digest)
                    .then_some(*id)
            })
            .ok_or_else(|| ProcessorError::fatal("sibling has no real executor lease admission"))?;
        if data
            .admission_history
            .get(&id)
            .is_some_and(|old| old != &commit)
        {
            return Err(ProcessorError::fatal(
                "conflicting selected sibling history attribution",
            ));
        }
        data.admission_history.insert(id, commit);
        Ok(NativeToolProgressContextNotification {
            route: Arc::downgrade(&self.0),
            terminal: None,
        })
    }
}

/// Retained public copies must fit the immutable identity's original budget.
pub(crate) fn validate_selected_call_copy(
    function_name: &String,
    tool_call_id: &String,
    arguments: Option<&serde_json::Value>,
    call: &FunctionCall,
) -> Result<(), ProcessorError> {
    if function_name.capacity() > call.function_name.capacity()
        || tool_call_id.capacity() > call.tool_call_id.capacity()
        || arguments
            .map(|value| measured_json_retention(value, 32 * KIB))
            .transpose()?
            .is_some_and(|retained| {
                retained > measured_json_retention(&call.arguments, 32 * KIB).unwrap_or(0)
            })
    {
        return Err(ProcessorError::fatal(
            "selected call retained representation changed",
        ));
    }
    Ok(())
}

/// Compact real-delivery prerequisites for an already authenticated history snapshot.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum NativeToolProgressProviderPrerequisite {
    Progress(u64),
    Terminal(u64),
}
impl NativeToolProgressRoute {
    pub fn authenticated_history_indexes(
        &self,
        messages: &[crate::ContextMessage],
    ) -> Result<Vec<usize>, ProcessorError> {
        if messages.len() > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::new("selected history message limit"));
        }
        let mut authenticated = [false; Self::MAX_HISTORY_MESSAGES];
        let mut mark = |commit: NativeToolProgressHistoryCommit| -> Result<(), ProcessorError> {
            let range = authenticated
                .get_mut(commit.start..commit.end)
                .ok_or_else(|| {
                    ProcessorError::fatal("invalid selected history attribution range")
                })?;
            for (offset, authenticated) in range.iter_mut().enumerate() {
                if commit.matches_index(messages, commit.start + offset)? {
                    *authenticated = true;
                }
            }
            Ok(())
        };
        let data = self.0.state.lock().unwrap();
        for commit in data
            .admission_history
            .values()
            .chain(data.terminal_history.values())
        {
            mark(*commit)?;
        }
        for event in data.events.values() {
            let commit = event
                .receipt
                .state
                .lock()
                .unwrap()
                .history
                .and_then(Result::ok);
            if let Some(commit) = commit {
                mark(commit)?;
            }
        }
        Ok(authenticated
            .iter()
            .enumerate()
            .filter_map(|(index, valid)| valid.then_some(index))
            .collect())
    }
    pub fn pending_provider_history(
        &self,
        messages: &[crate::ContextMessage],
    ) -> Result<Vec<NativeToolProgressProviderPrerequisite>, ProcessorError> {
        if messages.len() > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::new("selected history message limit"));
        }
        let data = self.0.state.lock().unwrap();
        let mut pending = Vec::new();
        for (id, event) in &data.events {
            let state = event.receipt.state.lock().unwrap();
            let commit = state.history.and_then(Result::ok);
            let ready = state.provider_admitted;
            drop(state);
            if !ready {
                if let Some(commit) = commit {
                    if commit.matches(messages)? {
                        pending.push(NativeToolProgressProviderPrerequisite::Progress(*id));
                    }
                }
            }
        }
        for (id, commit) in &data.terminal_history {
            let ready = data.leases.values().any(|lease| {
                lease.terminal_outcome == Some(*id) && (lease.provider_staged || lease.provider)
            });
            if !ready && commit.matches(messages)? {
                pending.push(NativeToolProgressProviderPrerequisite::Terminal(*id));
            }
        }
        if pending.len() > Self::MAX_HISTORY_MESSAGES {
            return Err(ProcessorError::new("selected provider prerequisite limit"));
        }
        Ok(pending)
    }
    /// Pure compact-state query. No callbacks, notifications or runtime effects.
    pub fn provider_history_ready(
        &self,
        prerequisites: &[NativeToolProgressProviderPrerequisite],
    ) -> bool {
        if prerequisites.len() > Self::MAX_HISTORY_MESSAGES {
            return false;
        }
        let data = self.0.state.lock().unwrap();
        prerequisites.iter().all(|prerequisite| match prerequisite {
            NativeToolProgressProviderPrerequisite::Progress(id) => data
                .events
                .get(id)
                .is_some_and(|event| event.receipt.state.lock().unwrap().provider_admitted),
            NativeToolProgressProviderPrerequisite::Terminal(id) => {
                data.leases.values().any(|lease| {
                    lease.terminal_outcome == Some(*id) && (lease.provider_staged || lease.provider)
                })
            }
        })
    }
}

impl NativeToolProgressRoute {
    /// Validate a selected context after this route's actual provider consumed
    /// its sealed cause. This permits local provider projection only; the sticky
    /// frame remains unsuitable for raw forwarding or wire serialization.
    pub fn validate_consumed_progress_context(
        &self,
        ctx: &ProcessorContext,
        frame: &Frame,
        cause: &crate::NativeRealtimeContextCause,
    ) -> Result<(), ProcessorError> {
        self.check_role(ctx, NativeToolProgressRole::Provider)?;
        if !frame.native_tool_progress_context_restricted
            || frame.native_realtime_context.is_some()
            || (cause.assistant_native_progress_reasons().is_empty()
                && cause
                    .assistant_selected_native_terminal_route_id()
                    .is_none())
        {
            return Err(ProcessorError::fatal(
                "selected provider context was not consumed",
            ));
        }
        cause.validate(frame)?;
        if let Some(route_id) = cause.assistant_selected_native_terminal_route_id() {
            if route_id != self.id() {
                return Err(ProcessorError::fatal(
                    "selected terminal context belongs to another route",
                ));
            }
            self.validate_terminal_scheduling_identity(
                cause.assistant_native_outcome_id().ok_or_else(|| {
                    ProcessorError::fatal("selected terminal context lacks outcome")
                })?,
            )?;
        }
        let owner = Arc::downgrade(&self.0);
        let data = self.0.state.lock().unwrap();
        for reason in cause.assistant_native_progress_reasons() {
            if !reason.permission().owner.ptr_eq(&owner)
                || data.events.get(&reason.event_id()).is_none_or(|event| {
                    event.receipt.identity.lock().unwrap().lease_id != reason.lease_id()
                })
            {
                return Err(ProcessorError::fatal(
                    "selected context carries a foreign or unknown event",
                ));
            }
        }
        Ok(())
    }
}

impl NativeToolProgressRoute {
    pub(crate) fn validate_terminal_scheduling_identity(
        &self,
        outcome_id: u64,
    ) -> Result<(), ProcessorError> {
        let data = self.0.state.lock().unwrap();
        if !data
            .leases
            .values()
            .any(|lease| lease.terminal_outcome == Some(outcome_id))
        {
            return Err(ProcessorError::fatal(
                "selected terminal context has no recorded outcome",
            ));
        }
        Ok(())
    }
}
