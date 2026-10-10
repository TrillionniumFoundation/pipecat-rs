use crate::{
    BusError, BusMessage, JobContext, JobResponse, LocalBus, MessageData, Subscription,
    WorkerReady, WorkerRegistration, WorkerRegistry,
};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use tokio::sync::{watch, Mutex};
use tokio::task::{AbortHandle, Id, JoinSet};

type Handler =
    Arc<dyn Fn(JobContext) -> Pin<Box<dyn Future<Output = JobResponse> + Send>> + Send + Sync>;
type JobKey = (String, String);
type JobCompletion = Option<Result<(Id, (JobKey, JobResponse)), tokio::task::JoinError>>;
#[derive(Clone)]
struct HandlerEntry {
    handler: Handler,
    sequential: bool,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Inactive,
    Active,
    Ending,
    Cancelling,
    Ended,
    Cancelled,
    Failed,
}
impl WorkerState {
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Ended | Self::Cancelled | Self::Failed)
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct WorkerSnapshot {
    pub state: WorkerState,
    pub activation_args: Option<Value>,
    pub error: Option<String>,
}
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum WorkerError {
    #[error(transparent)]
    Bus(#[from] BusError),
    #[error("worker capacity must be nonzero")]
    InvalidCapacity,
    #[error("duplicate job handler: {0}")]
    DuplicateHandler(String),
    #[error("worker failed: {0}")]
    Failed(String),
    #[error("worker task stopped unexpectedly")]
    TaskStopped,
    #[error("worker already registered: {0}")]
    DuplicateWorker(String),
    #[error("worker runner has stopped accepting workers")]
    RunnerStopped,
}
/// Async job worker. Handler futures run outside the bus router; system messages
/// continue to run while handlers or data publication are blocked.
///
/// At most `max_jobs` requests run or await normal completion publication. Further
/// requests stay in the bounded bus data queue. Sequential handlers are FIFO per
/// name. Shutdown cancels futures, so blocking/non-yielding work is unsupported.
pub struct NativeWorker {
    name: String,
    active: bool,
    max_jobs: usize,
    handlers: BTreeMap<String, HandlerEntry>,
    parent: Option<String>,
    children: Vec<NativeWorker>,
}
impl NativeWorker {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            active: false,
            max_jobs: 64,
            handlers: BTreeMap::new(),
            parent: None,
            children: vec![],
        }
    }
    /// Add an owned child. The parent propagates shutdown and does not become
    /// terminal until every descendant has released its subscription and jobs.
    pub fn child(mut self, mut child: NativeWorker) -> Self {
        child.parent = Some(self.name.clone());
        self.children.push(child);
        self
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    fn validate_tree(&self, names: &mut BTreeSet<String>) -> Result<(), WorkerError> {
        if self.max_jobs == 0 {
            return Err(WorkerError::InvalidCapacity);
        }
        if !names.insert(self.name.clone()) {
            return Err(WorkerError::DuplicateWorker(self.name.clone()));
        }
        for child in &self.children {
            child.validate_tree(names)?;
        }
        Ok(())
    }
    pub fn active(mut self, active: bool) -> Self {
        self.active = active;
        self
    }
    pub fn max_jobs(mut self, max_jobs: usize) -> Self {
        self.max_jobs = max_jobs;
        self
    }
    /// Empty name is the default handler for unnamed or unmatched requests.
    pub fn on_job<F, Fut>(
        mut self,
        name: impl Into<String>,
        sequential: bool,
        handler: F,
    ) -> Result<Self, WorkerError>
    where
        F: Fn(JobContext) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = JobResponse> + Send + 'static,
    {
        let name = name.into();
        if self.handlers.contains_key(&name) {
            return Err(WorkerError::DuplicateHandler(name));
        }
        self.handlers.insert(
            name,
            HandlerEntry {
                handler: Arc::new(move |context| Box::pin(handler(context))),
                sequential,
            },
        );
        Ok(self)
    }
    pub fn start(
        self,
        bus: LocalBus,
        registry: WorkerRegistry,
    ) -> Result<WorkerHandle, WorkerError> {
        self.validate_tree(&mut BTreeSet::new())?;
        self.start_inner(bus, registry)
    }
    fn start_inner(
        mut self,
        bus: LocalBus,
        registry: WorkerRegistry,
    ) -> Result<WorkerHandle, WorkerError> {
        if self.max_jobs == 0 {
            return Err(WorkerError::InvalidCapacity);
        }
        let sub = bus.subscribe_target(self.name.clone())?;
        if registry
            .get(&self.name)
            .is_some_and(|ready| ready.runner == registry.runner_name())
        {
            return Err(WorkerError::DuplicateWorker(self.name));
        }
        let (state_tx, state) = watch::channel(WorkerSnapshot {
            state: if self.active {
                WorkerState::Active
            } else {
                WorkerState::Inactive
            },
            activation_args: None,
            error: None,
        });
        let (control_tx, control) = watch::channel(None);
        let mut children: Vec<WorkerHandle> = vec![];
        for child in std::mem::take(&mut self.children) {
            match child.start_inner(bus.clone(), registry.clone()) {
                Ok(handle) => children.push(handle),
                Err(error) => {
                    for child in &children {
                        child.cancel("parent startup failed");
                    }
                    return Err(error);
                }
            }
        }
        let handle = WorkerHandle {
            name: self.name.clone(),
            parent: self.parent.clone(),
            children: children.clone(),
            bus: bus.clone(),
            state,
            control: control_tx,
        };
        let ready = WorkerReady {
            worker_name: self.name.clone(),
            runner: registry.runner_name().into(),
        };
        if !registry.register_entry(WorkerRegistration {
            ready,
            parent: self.parent.clone(),
            active: self.active,
            bridged: false,
        }) {
            for child in &children {
                child.cancel("parent startup failed");
            }
            return Err(WorkerError::DuplicateWorker(self.name));
        }
        tokio::spawn(async move {
            let name = self.name.clone();
            let child_state = state_tx.subscribe();
            let child_control = control.clone();
            let run = async {
                let result = self.run(&bus, &sub, &state_tx, control, &registry).await;
                if result.is_err() {
                    state_tx.send_modify(|s| s.state = WorkerState::Cancelling);
                }
                result
            };
            let (result, children_result) = tokio::join!(
                run,
                finish_children(&name, &children, child_state, child_control)
            );
            let result = result.and_then(|terminal| children_result.map(|()| terminal));
            // Remove subscription before discovery, so a new same-name worker can start.
            drop(sub);
            registry.unregister(&name, registry.runner_name());
            match result {
                Ok(terminal) => {
                    state_tx.send_modify(|snapshot| snapshot.state = terminal);
                }
                Err(error) => {
                    state_tx.send_modify(|snapshot| {
                        snapshot.state = WorkerState::Failed;
                        snapshot.error = Some(error.to_string());
                    });
                }
            }
        });
        Ok(handle)
    }
    async fn run(
        self,
        bus: &LocalBus,
        sub: &Subscription,
        state: &watch::Sender<WorkerSnapshot>,
        mut control: watch::Receiver<Option<String>>,
        registry: &WorkerRegistry,
    ) -> Result<WorkerState, WorkerError> {
        let mut jobs: BTreeMap<JobKey, RunningJob> = BTreeMap::new();
        let mut pending_updates: BTreeSet<JobKey> = BTreeSet::new();
        // Network control lanes may deliver cancellation before its request.
        // Retain only a finite recent window, never an unbounded execution ledger.
        let mut cancelled_requests: VecDeque<JobKey> = VecDeque::new();
        let mut tasks = JoinSet::new();
        let mut task_keys: HashMap<Id, JobKey> = HashMap::new();
        let mut normal: VecDeque<Reply> = VecDeque::new();
        let mut urgent: VecDeque<Reply> = VecDeque::new();
        let mut locks: BTreeMap<String, Arc<Mutex<()>>> = BTreeMap::new();
        let mut ending = false;
        let mut cancelling = false;
        let mut data_closed = false;
        let mut system_closed = false;
        let result=async { loop {
            if !pending_updates.is_empty() {
                let queued = sub.queued_job_keys()?;
                pending_updates.retain(|key| queued.contains(key));
            }
            if (ending || cancelling)
                && jobs.is_empty()
                && normal.is_empty()
                && urgent.is_empty()
                && tasks.is_empty()
            {
                return Ok(if cancelling {
                    WorkerState::Cancelled
                } else {
                    WorkerState::Ended
                });
            }
            let out_normal = normal.front().map(|r| r.message.clone());
            let out_urgent = urgent.front().map(|r| r.message.clone());
            enum Event {
                Cancel,
                Message(Option<BusMessage>, bool),
                Done(JobCompletion),
                Published(bool, Result<usize, BusError>),
            }
            let event = tokio::select! {
                biased;
                _=control.changed(),if !cancelling=>Event::Cancel,
                message=sub.recv_system(),if !system_closed && urgent.len() < self.max_jobs.saturating_add(bus.capacity_per_priority())=>Event::Message(message?,true),
                done=tasks.join_next_with_id(),if !tasks.is_empty()=>Event::Done(done),
                result=async {bus.publish_wait(out_urgent.unwrap()).await},if out_urgent.is_some()=>Event::Published(true,result),
                result=async {bus.publish_wait(out_normal.unwrap()).await},if out_normal.is_some()=>Event::Published(false,result),
                message=sub.recv_data(),if !data_closed && !ending && !cancelling && jobs.len()<self.max_jobs=>Event::Message(message?,false),
            };
            match event {
                Event::Cancel => {
                    cancelling = true;
                    state.send_modify(|s| {
                        s.state = WorkerState::Cancelling;
                        s.activation_args = None;
                    });
                    for (key, job) in &mut jobs {
                        if let Some(abort) = job.abort.take() {
                            abort.abort();
                        }
                        normal.retain(|reply| reply.key != *key);
                        if !urgent.iter().any(|reply| reply.key == *key) {
                            urgent.push_back(reply(
                                &self.name,
                                key,
                                JobResponse::cancelled(),
                                true,
                            ));
                        }
                    }
                    for request in sub.remove_requests(None)? {
                        if let MessageData::JobRequest { job_id, .. } = request.data {
                            let key = (request.source, job_id);
                            if !jobs.contains_key(&key) {
                                urgent.push_back(reply(
                                    &self.name,
                                    &key,
                                    JobResponse::cancelled(),
                                    true,
                                ));
                            }
                        }
                    }
                }
                Event::Message(None, system) => {
                    if system {
                        system_closed = true;
                    } else {
                        data_closed = true;
                    }
                    if data_closed || system_closed {
                        // A closed bus cannot carry terminal responses; fail explicitly.
                        return Err(WorkerError::Bus(BusError::Closed));
                    }
                }
                Event::Message(Some(message), _) => match message.data {
                    MessageData::Cancel { .. } if self.parent.is_some() => {},
                    MessageData::End { .. } if self.parent.is_some() => {},
                    MessageData::CancelWorker { .. } | MessageData::Cancel { .. } => {
                        cancelling = true;
                        state.send_modify(|s| {
                            s.state = WorkerState::Cancelling;
                            s.activation_args = None;
                        });
                        for (key, job) in &mut jobs {
                            if let Some(abort) = job.abort.take() {
                                abort.abort();
                            }
                            normal.retain(|r| r.key != *key);
                            if !urgent.iter().any(|r| r.key == *key) {
                                urgent.push_back(reply(
                                    &self.name,
                                    key,
                                    JobResponse::cancelled(),
                                    true,
                                ));
                            }
                        }
                        for request in sub.remove_requests(None)? {
                            if let MessageData::JobRequest { job_id, .. } = request.data {
                                let key = (request.source, job_id);
                                if !jobs.contains_key(&key) {
                                    urgent.push_back(reply(
                                        &self.name,
                                        &key,
                                        JobResponse::cancelled(),
                                        true,
                                    ));
                                }
                            }
                        }
                    }
                    MessageData::EndWorker { .. } | MessageData::End { .. } => {
                        ending = true;
                        state.send_modify(|s| {
                            s.state = WorkerState::Ending;
                            s.activation_args = None;
                        });
                    }
                    MessageData::Activate { args } if !ending && !cancelling => {
                        registry.set_active(&self.name, true);
                        state.send_modify(|s| {
                            s.state = WorkerState::Active;
                            s.activation_args = args;
                        })
                    }
                    MessageData::Deactivate if !ending && !cancelling => {
                        registry.set_active(&self.name, false);
                        state.send_modify(|s| {
                            s.state = WorkerState::Inactive;
                            s.activation_args = None;
                        });
                    },
                    MessageData::JobCancel { job_id, .. } => {
                        let key = (message.source, job_id);
                        if let Some(job) = jobs.get_mut(&key) {
                            if let Some(abort) = job.abort.take() {
                                abort.abort();
                            }
                            normal.retain(|r| r.key != key);
                            if !urgent.iter().any(|r| r.key == key) {
                                urgent.push_back(reply(
                                    &self.name,
                                    &key,
                                    JobResponse::cancelled(),
                                    true,
                                ));
                            }
                        } else if !sub.remove_requests(Some((&key.0, &key.1)))?.is_empty() {
                            urgent.push_back(reply(&self.name, &key, JobResponse::cancelled(), true));
                        } else if !cancelled_requests.contains(&key) {
                            if cancelled_requests.len() >= self.max_jobs.saturating_add(bus.capacity_per_priority().saturating_mul(2)) {
                                cancelled_requests.pop_front();
                            }
                            cancelled_requests.push_back(key);
                        }
                    }
                    MessageData::JobUpdateRequest { job_id } => {
                        let key = (message.source, job_id);
                        if let Some(job) = jobs.get(&key) {
                            job.updates.send_modify(|count| *count = count.wrapping_add(1));
                        } else if sub.queued_job_keys()?.contains(&key) {
                            pending_updates.insert(key);
                        }
                    }
                    MessageData::JobRequest {
                        job_id,
                        name,
                        payload,
                    } if state.borrow().state == WorkerState::Active => {
                        let key = (message.source.clone(), job_id.clone());
                        if jobs.contains_key(&key) {
                            continue;
                        }
                        if let Some(index) = cancelled_requests.iter().position(|cancelled| cancelled == &key) {
                            cancelled_requests.remove(index);
                            urgent.push_back(reply(&self.name, &key, JobResponse::cancelled(), true));
                            continue;
                        }
                        let name = name.unwrap_or_default();
                        let handler = self.handlers.get_key_value(&name).or_else(|| self.handlers.get_key_value(""));
                        let (updates, updates_rx) = watch::channel(0);
                        if pending_updates.remove(&key) { updates.send_replace(1); }
                        let Some((handler_name, handler)) = handler else {
                            jobs.insert(
                                key.clone(),
                                RunningJob {
                                    abort: None,
                                    updates,
                                },
                            );
                            normal.push_back(reply(
                                &self.name,
                                &key,
                                JobResponse::error(format!("no handler for {name}")),
                                false,
                            ));
                            continue;
                        };
                        let context = JobContext {
                            job_id,
                            requester: message.source,
                            worker: self.name.clone(),
                            payload,
                            bus: bus.clone(),
                            updates: updates_rx,
                            registry: registry.clone(),
                        };
                        let handler = handler.clone();
                        // Poll each acquisition once in dispatch order, reserving its
                        // FIFO position before the task can run on another executor thread.
                        let lock = handler.sequential.then(|| {
                            locks
                                .entry(handler_name.clone())
                                .or_insert_with(|| Arc::new(Mutex::new(())))
                                .clone()
                        });
                        let key_copy = key.clone();
                        let mut guard = lock.map(|lock| Box::pin(lock.lock_owned()));
                        let acquired = match guard.as_mut() {
                            Some(guard) => {
                                std::future::poll_fn(|cx| {
                                    std::task::Poll::Ready(guard.as_mut().poll(cx))
                                })
                                .await
                            }
                            None => std::task::Poll::Pending,
                        };
                        let abort = tasks.spawn(async move {
                            let _guard = match acquired {
                                std::task::Poll::Ready(guard) => Some(guard),
                                std::task::Poll::Pending => match guard {
                                    Some(guard) => Some(guard.await),
                                    None => None,
                                },
                            };
                            let response = (handler.handler)(context).await;
                            (key_copy, response)
                        });
                        task_keys.insert(abort.id(), key.clone());
                        jobs.insert(
                            key,
                            RunningJob {
                                abort: Some(abort),
                                updates,
                            },
                        );
                    }
                    _ => {}
                },
                Event::Done(Some(Ok((id, (key, result))))) => {
                    task_keys.remove(&id);
                    if let Some(job) = jobs.get_mut(&key) {
                        if job.abort.as_ref().is_some_and(|abort| abort.id() == id) {
                            job.abort.take();
                            normal.push_back(reply(&self.name, &key, result, false));
                        }
                    }
                }
                Event::Done(Some(Err(error))) => {
                    if let Some(key) = task_keys.remove(&error.id()) {
                        if let Some(job) = jobs.get_mut(&key) {
                            if job
                                .abort
                                .as_ref()
                                .is_some_and(|abort| abort.id() == error.id())
                            {
                                job.abort.take();
                                normal.push_back(reply(
                                    &self.name,
                                    &key,
                                    JobResponse::error(error.to_string()),
                                    false,
                                ));
                            }
                        }
                    }
                }
                Event::Published(system, result) => {
                    result?;
                    let sent = if system {
                        urgent.pop_front()
                    } else {
                        normal.pop_front()
                    };
                    if let Some(sent) = sent {
                        jobs.remove(&sent.key);
                    }
                }
                Event::Done(None) => {}
            }
        }}.await;
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
        result
    }
}
struct RunningJob {
    abort: Option<AbortHandle>,
    updates: watch::Sender<u64>,
}
struct Reply {
    key: JobKey,
    message: BusMessage,
}
fn reply(worker: &str, key: &JobKey, result: JobResponse, urgent: bool) -> Reply {
    Reply {
        key: key.clone(),
        message: BusMessage {
            source: worker.into(),
            target: Some(key.0.clone()),
            data: MessageData::JobResponse {
                job_id: key.1.clone(),
                status: result.status,
                response: result.response,
                urgent,
            },
        },
    }
}

#[derive(Clone)]
pub struct WorkerHandle {
    name: String,
    parent: Option<String>,
    children: Vec<WorkerHandle>,
    bus: LocalBus,
    state: watch::Receiver<WorkerSnapshot>,
    control: watch::Sender<Option<String>>,
}
impl WorkerHandle {
    pub fn parent(&self) -> Option<&str> {
        self.parent.as_deref()
    }
    pub fn children(&self) -> &[WorkerHandle] {
        &self.children
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn snapshot(&self) -> WorkerSnapshot {
        self.state.borrow().clone()
    }
    pub fn watch(&self) -> watch::Receiver<WorkerSnapshot> {
        self.state.clone()
    }
    async fn send(&self, data: MessageData) -> Result<(), BusError> {
        self.bus
            .publish_wait(BusMessage {
                source: "worker_handle".into(),
                target: Some(self.name.clone()),
                data,
            })
            .await
            .map(|_| ())
    }
    pub async fn activate(&self, args: Option<Value>) -> Result<(), BusError> {
        self.send(MessageData::Activate { args }).await
    }
    pub async fn deactivate(&self) -> Result<(), BusError> {
        self.send(MessageData::Deactivate).await
    }
    /// FIFO graceful shutdown. Requests admitted before End drain to terminal responses.
    pub async fn end(&self, reason: Option<String>) -> Result<(), BusError> {
        self.send(MessageData::EndWorker { reason }).await
    }
    /// Out-of-band cancellation remains available even when bus queues are full.
    pub fn cancel(&self, reason: impl Into<String>) {
        self.control.send_replace(Some(reason.into()));
    }
    async fn end_from(&self, parent: &str) -> Result<(), BusError> {
        if self.snapshot().state.is_terminal() {
            return Ok(());
        }
        self.bus
            .publish_wait(BusMessage {
                source: parent.into(),
                target: Some(self.name.clone()),
                data: MessageData::EndWorker { reason: None },
            })
            .await
            .map(|_| ())
    }
    pub async fn wait(&self) -> Result<WorkerState, WorkerError> {
        let mut state = self.state.clone();
        loop {
            let snapshot = state.borrow_and_update().clone();
            if snapshot.state == WorkerState::Failed {
                return Err(WorkerError::Failed(snapshot.error.unwrap_or_default()));
            }
            if snapshot.state.is_terminal() {
                return Ok(snapshot.state);
            }
            if state.changed().await.is_err() {
                return Err(WorkerError::TaskStopped);
            }
        }
    }
}
/// Owns root workers and shared local admission/discovery. Children belong to
/// their parent; runner shutdown signals roots only and awaits the whole tree.
/// Connect a network forwarding adapter to `bus()` and `registry()` for remote RPC.
pub struct WorkerRunner {
    bus: LocalBus,
    registry: WorkerRegistry,
    state: std::sync::Mutex<RunnerState>,
    shutdown: watch::Sender<bool>,
    changed: watch::Sender<u64>,
}
struct RunnerState {
    workers: BTreeMap<String, WorkerHandle>,
    closing: bool,
}
impl WorkerRunner {
    pub fn new(name: impl Into<String>, capacity_per_priority: usize) -> Result<Self, BusError> {
        Ok(Self {
            bus: LocalBus::new(capacity_per_priority)?,
            registry: WorkerRegistry::new(name),
            state: std::sync::Mutex::new(RunnerState {
                workers: BTreeMap::new(),
                closing: false,
            }),
            shutdown: watch::channel(false).0,
            changed: watch::channel(0).0,
        })
    }
    pub fn bus(&self) -> LocalBus {
        self.bus.clone()
    }
    pub fn registry(&self) -> WorkerRegistry {
        self.registry.clone()
    }
    pub fn client(&self, name: impl Into<String>) -> crate::JobClient {
        crate::JobClient::new(name, self.bus(), self.registry())
    }
    /// Admission is serialized with shutdown and auto-end. A completed worker's
    /// name may be reused while the host remains open; terminal shutdown rejects
    /// every new root rather than leaving it outside runner ownership.
    pub fn add(&self, worker: NativeWorker) -> Result<WorkerHandle, WorkerError> {
        let mut state = self.state.lock().expect("runner lock poisoned");
        if state.closing {
            return Err(WorkerError::RunnerStopped);
        }
        if state
            .workers
            .get(&worker.name)
            .is_some_and(|w| !w.snapshot().state.is_terminal())
        {
            return Err(WorkerError::DuplicateWorker(worker.name));
        }
        let handle = worker.start(self.bus(), self.registry())?;
        state.workers.insert(handle.name.clone(), handle.clone());
        let completed = handle.clone();
        let changed = self.changed.clone();
        tokio::spawn(async move {
            let _ = completed.wait().await;
            changed.send_modify(|revision| *revision = revision.wrapping_add(1));
        });
        self.changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        Ok(handle)
    }
    /// Returns a retained handle, including for a descendant, without borrowing
    /// the runner's admission lock across user code or an async wait.
    pub fn get(&self, name: &str) -> Option<WorkerHandle> {
        fn find(handle: &WorkerHandle, name: &str) -> Option<WorkerHandle> {
            if handle.name() == name {
                return Some(handle.clone());
            }
            handle.children.iter().find_map(|child| find(child, name))
        }
        self.state
            .lock()
            .expect("runner lock poisoned")
            .workers
            .values()
            .find_map(|handle| find(handle, name))
    }
    fn close_admission(&self) -> Vec<WorkerHandle> {
        let mut state = self.state.lock().expect("runner lock poisoned");
        state.closing = true;
        self.shutdown.send_replace(true);
        state.workers.values().cloned().collect()
    }
    pub async fn end(&self) -> Result<(), WorkerError> {
        let workers = self.close_admission();
        for worker in workers {
            if !worker.snapshot().state.is_terminal() {
                worker.end(None).await?;
            }
        }
        self.wait().await
    }
    pub async fn cancel(&self) -> Result<(), WorkerError> {
        for worker in self.close_admission() {
            worker.cancel("runner cancelled");
        }
        self.wait().await
    }
    /// Auto-end closes admission atomically once all current roots are terminal.
    /// With false, an empty host remains live and accepts `add(&self)` calls from
    /// another task until explicit end/cancel or a bus-wide termination message.
    pub async fn run(&self, auto_end: bool) -> Result<(), WorkerError> {
        let subscription = self
            .bus
            .subscribe_target(format!("{}/runner-control", self.registry.runner_name()))?;
        let mut changed = self.changed.subscribe();
        let mut shutdown = self.shutdown.subscribe();
        loop {
            let done = {
                let mut state = self.state.lock().expect("runner lock poisoned");
                if auto_end
                    && state
                        .workers
                        .values()
                        .all(|worker| worker.snapshot().state.is_terminal())
                {
                    state.closing = true;
                    self.shutdown.send_replace(true);
                }
                state.closing
            };
            if done {
                return self.wait().await;
            }
            tokio::select! {
                biased;
                _ = shutdown.changed() => {},
                _ = changed.changed() => {},
                message = subscription.recv() => match message? {
                    Some(BusMessage { data: MessageData::End { .. }, .. }) => return self.end().await,
                    Some(BusMessage { data: MessageData::Cancel { .. }, .. }) => return self.cancel().await,
                    None => { let _ = self.cancel().await; return Err(WorkerError::Bus(BusError::Closed)); },
                    _ => {},
                }
            }
        }
    }
    pub async fn wait(&self) -> Result<(), WorkerError> {
        let mut changed = self.changed.subscribe();
        loop {
            let workers: Vec<_> = self
                .state
                .lock()
                .expect("runner lock poisoned")
                .workers
                .values()
                .cloned()
                .collect();
            let mut first = None;
            for worker in workers {
                if let Err(error) = worker.wait().await {
                    first.get_or_insert(error);
                }
            }
            let done = {
                let state = self.state.lock().expect("runner lock poisoned");
                state
                    .workers
                    .values()
                    .all(|worker| worker.snapshot().state.is_terminal())
            };
            if done {
                return first.map_or(Ok(()), Err);
            }
            let _ = changed.changed().await;
        }
    }
}
impl Drop for WorkerRunner {
    fn drop(&mut self) {
        for worker in self.close_admission() {
            if !worker.snapshot().state.is_terminal() {
                worker.cancel("runner dropped");
            }
        }
    }
}

async fn finish_children(
    parent: &str,
    children: &[WorkerHandle],
    mut state: watch::Receiver<WorkerSnapshot>,
    mut control: watch::Receiver<Option<String>>,
) -> Result<(), WorkerError> {
    if children.is_empty() {
        return Ok(());
    }
    let mut tasks = JoinSet::new();
    let mut ending = false;
    let mut cancelling = false;
    let mut first_error = None;
    loop {
        let status = state.borrow_and_update().state;
        let cancel = status == WorkerState::Cancelling || control.borrow_and_update().is_some();
        if cancel && !cancelling {
            cancelling = true;
            for child in children {
                child.cancel(format!("parent {parent} cancelled"));
            }
            if ending {
                tasks.abort_all();
                while tasks.join_next().await.is_some() {}
                ending = false;
            }
        }
        if !ending && (cancel || status == WorkerState::Ending) {
            ending = true;
            for child in children {
                let child = child.clone();
                let parent = parent.to_owned();
                tasks.spawn(async move {
                    if !cancel {
                        child.end_from(&parent).await?;
                    }
                    child.wait().await.map(|_| ())
                });
            }
        }
        if ending && tasks.is_empty() {
            return first_error.map_or(Ok(()), Err);
        }
        tokio::select! {
            _ = state.changed() => {},
            _ = control.changed(), if !cancelling => {},
            result = tasks.join_next(), if !tasks.is_empty() => match result {
                Some(Ok(Err(error))) => { first_error.get_or_insert(error); },
                Some(Err(error)) => { first_error.get_or_insert(WorkerError::Failed(error.to_string())); },
                _ => {},
            }
        }
    }
}
