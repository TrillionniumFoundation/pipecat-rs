//! Ordered, bounded delivery of committed assistant-turn records.
use futures_util::FutureExt;
use pipecat_core::{InvocationFence, ProcessorError};
use std::{
    collections::VecDeque,
    future::Future,
    sync::{Arc, Mutex, Weak},
    time::Duration,
};
use tokio::sync::Notify;

/// Immutable committed-turn data. This record confers no pipeline or job authority.
/// Its timestamp is captured when the turn starts, including empty/tool-only turns.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssistantTurnStopped {
    text: Arc<str>,
    started_at: Arc<str>,
    interrupted: bool,
    session_id: u64,
    turn_id: u64,
    job_generation: Option<u64>,
}
impl AssistantTurnStopped {
    pub fn text(&self) -> &str {
        &self.text
    }
    pub fn started_at(&self) -> &str {
        &self.started_at
    }
    pub fn interrupted(&self) -> bool {
        self.interrupted
    }
    pub fn session_id(&self) -> u64 {
        self.session_id
    }
    pub fn turn_id(&self) -> u64 {
        self.turn_id
    }
    pub fn job_generation(&self) -> Option<u64> {
        self.job_generation
    }
}

/// Capture once before launching a job's inference. A replacement job uses a
/// fresh assistant session. Compare both fields on delivery; never answer
/// whichever job happens to be current when a record is consumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AssistantJobCorrelation {
    pub session_id: u64,
    pub generation: u64,
}
impl AssistantJobCorrelation {
    pub fn matches(&self, completion: &AssistantTurnStopped) -> bool {
        completion.session_id == self.session_id
            && completion.job_generation == Some(self.generation)
    }
}

#[derive(Debug, Clone)]
pub struct AssistantStoppedConfig {
    /// Shared count for reserved actions, queued records, and borrowed records.
    pub max_records: usize,
    /// Shared UTF-8 byte budget for text plus start timestamps in those records.
    pub max_bytes: usize,
    /// Deadline for `handle_next`, beginning when the handler is invoked.
    pub handler_timeout: Duration,
}
impl Default for AssistantStoppedConfig {
    fn default() -> Self {
        Self {
            max_records: 16,
            max_bytes: 1024 * 1024,
            handler_timeout: Duration::from_secs(10),
        }
    }
}
impl AssistantStoppedConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if !(1..=1024).contains(&self.max_records)
            || !(1..=64 * 1024 * 1024).contains(&self.max_bytes)
            || self.handler_timeout < Duration::from_millis(1)
            || self.handler_timeout > Duration::from_secs(60)
        {
            return Err(ProcessorError::new(
                "invalid assistant-stopped receiver bounds",
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AssistantStoppedClose {
    End,
    Stop,
    Cancel,
    Cleanup,
    OwnerDropped,
    ReceiverDropped,
}

/// Payload-free, saturating outcome counters. Failed notification never rolls
/// back committed context; inspect these counters even when output is saturated.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AssistantStoppedStats {
    pub retained_records: usize,
    pub retained_bytes: usize,
    pub queued_records: usize,
    pub in_flight_handlers: usize,
    pub reserved: u64,
    pub delivered: u64,
    pub rejected: u64,
    pub stale: u64,
    pub abandoned: u64,
    pub handled: u64,
    pub handler_errors: u64,
    pub handler_panics: u64,
    pub handler_timeouts: u64,
    pub handler_cancelled: u64,
}
struct State {
    queue: VecDeque<AssistantStoppedRecord>,
    stats: AssistantStoppedStats,
    sealed: bool,
    close: Option<AssistantStoppedClose>,
    receiver_alive: bool,
}
pub(crate) struct StoppedTurn<'a> {
    pub text: &'a str,
    pub started_at: &'a str,
    pub interrupted: bool,
    pub session_id: u64,
    pub turn_id: u64,
    pub job_generation: Option<u64>,
    pub cause: Option<InvocationFence>,
}
/// Closing belongs to this field so consuming aggregator builders can still
/// move their unrelated owned components without cloning/resetting them.
#[derive(Default)]
pub(crate) struct StoppedOwner(pub Mutex<Option<Arc<StoppedHub>>>);
impl Drop for StoppedOwner {
    fn drop(&mut self) {
        if let Some(hub) = self.0.get_mut().unwrap_or_else(|error| error.into_inner()) {
            hub.close(AssistantStoppedClose::OwnerDropped);
        }
    }
}
pub(crate) struct StoppedHub {
    config: AssistantStoppedConfig,
    state: Mutex<State>,
    ready: Notify,
}
impl StoppedHub {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|error| error.into_inner())
    }
    pub(crate) fn new(
        config: AssistantStoppedConfig,
    ) -> Result<(Arc<Self>, AssistantStoppedReceiver), ProcessorError> {
        config.validate()?;
        let hub = Arc::new(Self {
            config,
            state: Mutex::new(State {
                queue: VecDeque::new(),
                stats: AssistantStoppedStats::default(),
                sealed: false,
                close: None,
                receiver_alive: true,
            }),
            ready: Notify::new(),
        });
        Ok((hub.clone(), AssistantStoppedReceiver { hub }))
    }
    pub(crate) fn reserve(
        self: &Arc<Self>,
        turn: StoppedTurn<'_>,
    ) -> Result<AssistantStoppedAction, ProcessorError> {
        let StoppedTurn {
            text,
            started_at,
            interrupted,
            session_id,
            turn_id,
            job_generation,
            cause,
        } = turn;
        let bytes = text.len().saturating_add(started_at.len());
        let mut state = self.lock();
        if state.sealed
            || !state.receiver_alive
            || state.close.is_some()
            || state.stats.retained_records >= self.config.max_records
            || bytes
                > self
                    .config
                    .max_bytes
                    .saturating_sub(state.stats.retained_bytes)
        {
            state.stats.rejected = state.stats.rejected.saturating_add(1);
            return Err(ProcessorError::new(
                "assistant-stopped receiver capacity exceeded or closed",
            ));
        }
        state.stats.retained_records += 1;
        state.stats.retained_bytes += bytes;
        state.stats.reserved = state.stats.reserved.saturating_add(1);
        drop(state);
        let record = AssistantStoppedRecord {
            completion: Arc::new(AssistantTurnStopped {
                text: text.into(),
                started_at: started_at.into(),
                interrupted,
                session_id,
                turn_id,
                job_generation,
            }),
            _lease: RecordLease {
                hub: Arc::downgrade(self),
                bytes,
            },
        };
        Ok(AssistantStoppedAction(Arc::new(Ticket {
            pending: Mutex::new(Some(Pending::Record {
                hub: self.clone(),
                record,
                cause,
            })),
        })))
    }
    pub(crate) fn terminal(
        self: &Arc<Self>,
        reason: AssistantStoppedClose,
    ) -> AssistantStoppedAction {
        self.lock().sealed = true;
        AssistantStoppedAction(Arc::new(Ticket {
            pending: Mutex::new(Some(Pending::Close {
                hub: self.clone(),
                reason,
            })),
        }))
    }
    pub(crate) fn close(&self, reason: AssistantStoppedClose) {
        let mut state = self.lock();
        state.sealed = true;
        state.close.get_or_insert(reason);
        drop(state);
        self.ready.notify_waiters();
    }
    pub(crate) fn stats(&self) -> AssistantStoppedStats {
        self.lock().stats.clone()
    }
    fn publish(&self, record: AssistantStoppedRecord) {
        let mut state = self.lock();
        if state.close.is_some() || !state.receiver_alive {
            state.stats.rejected = state.stats.rejected.saturating_add(1);
            drop(state);
            drop(record);
            return;
        }
        state.stats.queued_records += 1;
        state.stats.delivered = state.stats.delivered.saturating_add(1);
        state.queue.push_back(record);
        drop(state);
        self.ready.notify_one();
    }
}

struct RecordLease {
    hub: Weak<StoppedHub>,
    bytes: usize,
}
impl Drop for RecordLease {
    fn drop(&mut self) {
        if let Some(hub) = self.hub.upgrade() {
            let mut state = hub.lock();
            state.stats.retained_records -= 1;
            state.stats.retained_bytes -= self.bytes;
        }
    }
}
/// A non-cloneable budget lease. Dropping it releases its record reservation.
/// Copies of `completion()` are application-owned and outside this receiver's
/// budget, as are external side effects performed by the application.
pub struct AssistantStoppedRecord {
    completion: Arc<AssistantTurnStopped>,
    _lease: RecordLease,
}
impl AssistantStoppedRecord {
    pub fn completion(&self) -> &Arc<AssistantTurnStopped> {
        &self.completion
    }
}
impl std::fmt::Debug for AssistantStoppedRecord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AssistantStoppedRecord")
            .field("completion", &self.completion)
            .finish()
    }
}

enum Pending {
    Record {
        hub: Arc<StoppedHub>,
        record: AssistantStoppedRecord,
        cause: Option<InvocationFence>,
    },
    Close {
        hub: Arc<StoppedHub>,
        reason: AssistantStoppedClose,
    },
}
struct Ticket {
    pending: Mutex<Option<Pending>>,
}
impl Drop for Ticket {
    fn drop(&mut self) {
        if let Some(pending) = self
            .pending
            .get_mut()
            .unwrap_or_else(|error| error.into_inner())
            .take()
        {
            match pending {
                Pending::Record { hub, record, .. } => {
                    {
                        let mut state = hub.lock();
                        state.stats.abandoned = state.stats.abandoned.saturating_add(1);
                    }
                    drop(record);
                }
                Pending::Close { hub, reason } => hub.close(reason),
            }
        }
    }
}
/// One ordered local admission, shared by every clone of reducer output.
/// Calling admission twice cannot create a second notification. No handler or
/// other user code is executed here. Receipt adapters must supply their original
/// invocation fence, rather than sampling a new scope when playback finishes.
#[derive(Clone)]
pub struct AssistantStoppedAction(Arc<Ticket>);
impl std::fmt::Debug for AssistantStoppedAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AssistantStoppedAction")
    }
}
impl PartialEq for AssistantStoppedAction {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl AssistantStoppedAction {
    /// Direct reducers may admit without a runtime. Any captured source scope is
    /// still checked. This gives access to data, never fresh execution authority.
    pub fn admit(&self) -> Result<(), ProcessorError> {
        self.admit_inner(None)
    }
    pub fn admit_with_fence(&self, receipt: &InvocationFence) -> Result<(), ProcessorError> {
        self.admit_inner(Some(receipt))
    }
    fn admit_inner(&self, receipt: Option<&InvocationFence>) -> Result<(), ProcessorError> {
        // Serialize admission through its synchronous publication. A cloned
        // ordered emitter must not skip an in-progress record and close the
        // stream before that record reaches its queue. No user code runs here.
        let mut ticket = self
            .0
            .pending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let Some(pending) = ticket.take() else {
            return Ok(());
        };
        match pending {
            Pending::Close { hub, reason } => hub.close(reason),
            Pending::Record { hub, record, cause } => {
                // Interrupted history remains observable even after its cause
                // expires. It carries no authority to launch replacement work.
                if record.completion.interrupted {
                    hub.publish(record);
                    return Ok(());
                }
                let fence = match (cause, receipt) {
                    (Some(cause), Some(receipt)) => match cause.intersection(receipt) {
                        Ok(fence) => Some(fence),
                        Err(error) => {
                            {
                                let mut state = hub.lock();
                                state.stats.rejected = state.stats.rejected.saturating_add(1);
                            }
                            return Err(error);
                        }
                    },
                    (Some(cause), None) => Some(cause),
                    (None, Some(receipt)) => Some(receipt.clone()),
                    (None, None) => None,
                };
                if let Some(fence) = fence {
                    if fence.with_current(|| hub.publish(record)).is_none() {
                        let mut state = hub.lock();
                        state.stats.stale = state.stats.stale.saturating_add(1);
                    }
                } else {
                    hub.publish(record);
                }
            }
        }
        Ok(())
    }
}

/// Single-consumer completion stream. No waiter, callback or dispatcher task is
/// spawned. End/Stop/Cancel admit a final record before closing; unread records
/// remain available to this caller. Cancel does not run an async callback.
/// Dropping the receiver closes admission and releases queued records.
pub struct AssistantStoppedReceiver {
    hub: Arc<StoppedHub>,
}
impl AssistantStoppedReceiver {
    pub fn stats(&self) -> AssistantStoppedStats {
        self.hub.lock().stats.clone()
    }
    pub fn closed_reason(&self) -> Option<AssistantStoppedClose> {
        self.hub.lock().close
    }
    pub fn try_recv(&mut self) -> Option<AssistantStoppedRecord> {
        let mut state = self.hub.lock();
        let record = state.queue.pop_front();
        if record.is_some() {
            state.stats.queued_records -= 1;
        }
        record
    }
    pub async fn recv(&mut self) -> Option<AssistantStoppedRecord> {
        loop {
            let ready = self.hub.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            {
                let mut state = self.hub.lock();
                if let Some(record) = state.queue.pop_front() {
                    state.stats.queued_records -= 1;
                    return Some(record);
                }
                if state.close.is_some() {
                    return None;
                }
            }
            ready.await;
        }
    }
    /// Invoke one handler in the caller's owned future. The mutable receiver
    /// permits at most one handler at a time. Deadline and cancellation require
    /// cooperative yielding; blocking code and caller-detached work cannot be
    /// controlled. Returned errors and counters omit handler error/panic payloads.
    /// Catching a panic does not suppress the process's configured panic hook.
    /// If the caller drops this entire helper future, Rust runs destructors in
    /// that caller; a destructor panic during external cancellation is not caught.
    pub async fn handle_next<F, Fut>(&mut self, handler: F) -> Result<bool, ProcessorError>
    where
        F: FnOnce(Arc<AssistantTurnStopped>) -> Fut,
        Fut: Future<Output = Result<(), ProcessorError>>,
    {
        let Some(record) = self.recv().await else {
            return Ok(false);
        };
        self.hub.lock().stats.in_flight_handlers += 1;
        let mut lease = HandlerLease {
            hub: self.hub.clone(),
            settled: false,
        };
        let completion = record.completion.clone();
        let timeout = self.hub.config.handler_timeout;
        // The timeout future is created and destroyed inside the unwind guard,
        // covering a handler destructor that panics during timeout cancellation.
        let result = std::panic::AssertUnwindSafe(async {
            tokio::time::timeout(timeout, async { handler(completion).await }).await
        })
        .catch_unwind()
        .await;
        let error = {
            let mut state = self.hub.lock();
            let (counter, error) = match result {
                Ok(Ok(Ok(()))) => (&mut state.stats.handled, None),
                Ok(Ok(Err(_))) => (
                    &mut state.stats.handler_errors,
                    Some("assistant-stopped handler failed"),
                ),
                Err(_) => (
                    &mut state.stats.handler_panics,
                    Some("assistant-stopped handler panicked"),
                ),
                Ok(Err(_)) => (
                    &mut state.stats.handler_timeouts,
                    Some("assistant-stopped handler timed out"),
                ),
            };
            *counter = counter.saturating_add(1);
            error
        };
        lease.settled = true;
        drop(record);
        error.map_or(Ok(true), |error| Err(ProcessorError::new(error)))
    }
}
struct HandlerLease {
    hub: Arc<StoppedHub>,
    settled: bool,
}
impl Drop for HandlerLease {
    fn drop(&mut self) {
        let mut state = self.hub.lock();
        state.stats.in_flight_handlers -= 1;
        if !self.settled {
            state.stats.handler_cancelled = state.stats.handler_cancelled.saturating_add(1);
        }
    }
}
impl Drop for AssistantStoppedReceiver {
    fn drop(&mut self) {
        let records = {
            let mut state = self.hub.lock();
            state.receiver_alive = false;
            state.stats.queued_records = 0;
            std::mem::take(&mut state.queue)
        };
        self.hub.close(AssistantStoppedClose::ReceiverDropped);
        drop(records);
    }
}

#[cfg(test)]
mod concurrency_tests;
