use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Completed,
    Cancelled,
    Failed,
    Error,
}

/// Native Rust message schema, not the Python bus JSON wire schema.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum MessageData {
    Activate {
        args: Option<Value>,
    },
    Deactivate,
    End {
        reason: Option<String>,
    },
    EndWorker {
        reason: Option<String>,
    },
    Cancel {
        reason: Option<String>,
    },
    CancelWorker {
        reason: Option<String>,
    },
    Ready {
        runner: String,
        parent: Option<String>,
        active: bool,
        bridged: bool,
    },
    WorkerError {
        error: String,
    },
    Speak {
        text: String,
        append_to_context: bool,
    },
    Frame {
        frame: Box<pipecat_core::Frame>,
        direction: pipecat_core::Direction,
        bridge: Option<String>,
    },
    JobRequest {
        job_id: String,
        name: Option<String>,
        payload: Option<Value>,
    },
    JobResponse {
        job_id: String,
        status: JobStatus,
        response: Option<Value>,
        urgent: bool,
    },
    JobUpdate {
        job_id: String,
        update: Option<Value>,
        urgent: bool,
    },
    JobCancel {
        job_id: String,
        reason: Option<String>,
    },
    JobUpdateRequest {
        job_id: String,
    },
    StreamStart {
        job_id: String,
    },
    StreamData {
        job_id: String,
        data: Value,
    },
    StreamEnd {
        job_id: String,
    },
}
impl MessageData {
    pub fn is_system(&self) -> bool {
        matches!(
            self,
            Self::Cancel { .. }
                | Self::CancelWorker { .. }
                | Self::WorkerError { .. }
                | Self::JobCancel { .. }
                | Self::JobUpdateRequest { .. }
                | Self::JobResponse { urgent: true, .. }
                | Self::JobUpdate { urgent: true, .. }
        )
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct BusMessage {
    pub source: String,
    pub target: Option<String>,
    pub data: MessageData,
}

#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum BusError {
    #[error("bus is closed")]
    Closed,
    #[error("subscriber already exists: {0}")]
    Duplicate(String),
    #[error("subscriber queue is full: {0}")]
    Full(String),
    #[error("capacity must be nonzero")]
    InvalidCapacity,
    #[error("bus state lock poisoned")]
    Poisoned,
}
/// Atomic bounded pub/sub. Each priority class has its own finite capacity.
///
/// `subscribe` receives all publications; `subscribe_target` only receives
/// broadcasts and messages for its name. Unlike Python's unbounded local bus,
/// publication can fail with `Full`, or wait with `publish_wait`.
#[derive(Clone)]
pub struct LocalBus {
    inner: Arc<Mutex<BusState>>,
    capacity: usize,
    space: Arc<Notify>,
}
struct BusState {
    closed: bool,
    next_id: u64,
    subscribers: BTreeMap<String, Queue>,
}
struct Queue {
    generation: u64,
    targeted: bool,
    outbound_only: bool,
    system: VecDeque<BusMessage>,
    data: VecDeque<BusMessage>,
    ready: Arc<Notify>,
}
pub struct Subscription {
    bus: LocalBus,
    name: String,
    generation: u64,
    ready: Arc<Notify>,
}
impl LocalBus {
    pub fn new(capacity_per_priority: usize) -> Result<Self, BusError> {
        if capacity_per_priority == 0 {
            return Err(BusError::InvalidCapacity);
        }
        Ok(Self {
            inner: Arc::new(Mutex::new(BusState {
                closed: false,
                next_id: 0,
                subscribers: BTreeMap::new(),
            })),
            capacity: capacity_per_priority,
            space: Arc::new(Notify::new()),
        })
    }
    pub fn capacity_per_priority(&self) -> usize {
        self.capacity
    }
    fn lock(&self) -> Result<MutexGuard<'_, BusState>, BusError> {
        self.inner.lock().map_err(|_| BusError::Poisoned)
    }
    pub fn subscribe(&self, name: impl Into<String>) -> Result<Subscription, BusError> {
        self.subscribe_inner(name.into(), false, false)
    }
    pub fn subscribe_target(&self, name: impl Into<String>) -> Result<Subscription, BusError> {
        self.subscribe_inner(name.into(), true, false)
    }
    /// Bounded forwarding lane for locally published messages only. Imported
    /// messages bypass this lane, preventing network forwarding cycles.
    pub fn subscribe_outbound(&self, name: impl Into<String>) -> Result<Subscription, BusError> {
        self.subscribe_inner(name.into(), false, true)
    }
    fn subscribe_inner(
        &self,
        name: String,
        targeted: bool,
        outbound_only: bool,
    ) -> Result<Subscription, BusError> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(BusError::Closed);
        }
        if state.subscribers.contains_key(&name) {
            return Err(BusError::Duplicate(name));
        }
        let generation = state.next_id.checked_add(1).ok_or(BusError::Closed)?;
        state.next_id = generation;
        let ready = Arc::new(Notify::new());
        state.subscribers.insert(
            name.clone(),
            Queue {
                generation,
                targeted,
                outbound_only,
                system: VecDeque::new(),
                data: VecDeque::new(),
                ready: ready.clone(),
            },
        );
        Ok(Subscription {
            bus: self.clone(),
            name,
            generation,
            ready,
        })
    }
    /// Whether an addressed local consumer exists. Forwarding adapters use this
    /// to retain local lookup precedence without guessing recipient name prefixes.
    pub fn has_target_subscriber(&self, target: &str) -> Result<bool, BusError> {
        Ok(self
            .lock()?
            .subscribers
            .get(target)
            .is_some_and(|queue| queue.targeted))
    }
    /// If any eligible subscriber is full, no subscriber receives the message.
    pub fn publish(&self, message: BusMessage) -> Result<usize, BusError> {
        self.publish_inner(message, false)
    }
    /// Atomically deliver a message received from a network adapter. This never
    /// enters outbound subscriptions, but otherwise has normal admission rules.
    pub fn inject(&self, message: BusMessage) -> Result<usize, BusError> {
        self.publish_inner(message, true)
    }
    fn publish_inner(&self, message: BusMessage, imported: bool) -> Result<usize, BusError> {
        let mut state = self.lock()?;
        if state.closed {
            return Err(BusError::Closed);
        }
        let accepts = |name: &String, q: &Queue| {
            !(imported && q.outbound_only)
                && (!q.targeted || message.target.as_ref().is_none_or(|target| target == name))
        };
        let system = message.data.is_system();
        for (name, q) in &state.subscribers {
            if !accepts(name, q) {
                continue;
            }
            let len = if system { q.system.len() } else { q.data.len() };
            if len >= self.capacity {
                return Err(BusError::Full(name.clone()));
            }
        }
        let mut count = 0;
        for (name, q) in &mut state.subscribers {
            if !accepts(name, q) {
                continue;
            }
            (if system { &mut q.system } else { &mut q.data }).push_back(message.clone());
            q.ready.notify_waiters();
            count += 1;
        }
        Ok(count)
    }
    /// Cancellation-safe admission. Dropping this future never partially publishes.
    pub async fn publish_wait(&self, message: BusMessage) -> Result<usize, BusError> {
        loop {
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.publish(message.clone()) {
                Err(BusError::Full(_)) => notified.await,
                result => return result,
            }
        }
    }
    /// Cancellation-safe inbound admission with the same bounded atomicity as
    /// `publish_wait`, excluding network egress subscriptions.
    pub async fn inject_wait(&self, message: BusMessage) -> Result<usize, BusError> {
        loop {
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.inject(message.clone()) {
                Err(BusError::Full(_)) => notified.await,
                result => return result,
            }
        }
    }
    /// Wait for admission shutdown without consuming a subscriber's messages.
    pub async fn closed(&self) -> Result<(), BusError> {
        loop {
            let notified = self.space.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.lock()?.closed {
                return Ok(());
            }
            notified.await;
        }
    }
    /// Close admission and let subscribers drain admitted messages.
    pub fn close(&self) -> Result<(), BusError> {
        let mut state = self.lock()?;
        state.closed = true;
        for q in state.subscribers.values() {
            q.ready.notify_waiters();
        }
        self.space.notify_waiters();
        Ok(())
    }
}
impl Subscription {
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn try_recv(&self) -> Result<Option<BusMessage>, BusError> {
        let mut state = self.bus.lock()?;
        let Some(q) = state.subscribers.get_mut(&self.name) else {
            return Ok(None);
        };
        let message = q.system.pop_front().or_else(|| q.data.pop_front());
        if message.is_some() {
            self.bus.space.notify_waiters();
        }
        Ok(message)
    }
    pub async fn recv(&self) -> Result<Option<BusMessage>, BusError> {
        self.recv_class(None).await
    }
    pub async fn recv_system(&self) -> Result<Option<BusMessage>, BusError> {
        self.recv_class(Some(true)).await
    }
    pub async fn recv_data(&self) -> Result<Option<BusMessage>, BusError> {
        self.recv_class(Some(false)).await
    }
    async fn recv_class(&self, system: Option<bool>) -> Result<Option<BusMessage>, BusError> {
        loop {
            let notified = self.ready.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            {
                let mut state = self.bus.lock()?;
                let closed = state.closed;
                let Some(q) = state.subscribers.get_mut(&self.name) else {
                    return Ok(None);
                };
                if q.generation != self.generation {
                    return Ok(None);
                }
                let message = match system {
                    None => q.system.pop_front().or_else(|| q.data.pop_front()),
                    Some(true) => q.system.pop_front(),
                    Some(false) => q.data.pop_front(),
                };
                if let Some(message) = message {
                    self.bus.space.notify_waiters();
                    return Ok(Some(message));
                }
                if closed {
                    return Ok(None);
                }
            }
            notified.await;
        }
    }
    pub(crate) fn queued_job_keys(
        &self,
    ) -> Result<std::collections::BTreeSet<(String, String)>, BusError> {
        let state = self.bus.lock()?;
        Ok(state
            .subscribers
            .get(&self.name)
            .into_iter()
            .flat_map(|queue| queue.data.iter())
            .filter_map(|message| match &message.data {
                MessageData::JobRequest { job_id, .. } => {
                    Some((message.source.clone(), job_id.clone()))
                }
                _ => None,
            })
            .collect())
    }
    /// Remove requests overtaken by a system-priority cancellation.
    pub(crate) fn remove_requests(
        &self,
        key: Option<(&str, &str)>,
    ) -> Result<Vec<BusMessage>, BusError> {
        let mut state = self.bus.lock()?;
        let Some(q) = state.subscribers.get_mut(&self.name) else {
            return Ok(vec![]);
        };
        let mut removed = vec![];
        q.data.retain(|message| {
            let matches=matches!(&message.data, MessageData::JobRequest {job_id,..} if key.is_none_or(|(source,id)| message.source==source && job_id==id));
            if matches {removed.push(message.clone());} !matches
        });
        if !removed.is_empty() {
            self.bus.space.notify_waiters();
        }
        Ok(removed)
    }
}
impl Drop for Subscription {
    fn drop(&mut self) {
        if let Ok(mut state) = self.bus.lock() {
            if state
                .subscribers
                .get(&self.name)
                .is_some_and(|q| q.generation == self.generation)
            {
                state.subscribers.remove(&self.name);
                self.bus.space.notify_waiters();
            }
        }
    }
}
