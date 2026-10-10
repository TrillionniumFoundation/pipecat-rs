use crate::{BusError, BusMessage, JobStatus, LocalBus, MessageData, WorkerRegistry};
use serde_json::Value;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, watch};
use tokio::time::Instant;

static NEXT_JOB: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, PartialEq)]
pub struct JobResponse {
    pub status: JobStatus,
    pub response: Option<Value>,
}
impl JobResponse {
    pub fn completed(response: impl Into<Value>) -> Self {
        Self {
            status: JobStatus::Completed,
            response: Some(response.into()),
        }
    }
    pub fn failed(response: impl Into<Value>) -> Self {
        Self {
            status: JobStatus::Failed,
            response: Some(response.into()),
        }
    }
    pub fn cancelled() -> Self {
        Self {
            status: JobStatus::Cancelled,
            response: None,
        }
    }
    pub fn error(error: impl Into<String>) -> Self {
        Self {
            status: JobStatus::Error,
            response: Some(serde_json::json!({"error":error.into()})),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum JobEvent {
    Update(Option<Value>),
    StreamStart,
    StreamData(Value),
    StreamEnd,
}
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum JobError {
    #[error(transparent)]
    Bus(#[from] BusError),
    #[error("job timed out (cancellation admission error: {cancellation_error:?})")]
    Timeout {
        cancellation_error: Option<BusError>,
    },
    #[error("job event receiver lagged by {0} events")]
    EventLagged(u64),
    #[error("job driver stopped before a response")]
    DriverStopped,
    #[error("event capacity must be nonzero")]
    InvalidEventCapacity,
    #[error("timeout exceeds the clock range")]
    InvalidTimeout,
    #[error("a job group requires distinct targets and at most its configured worker limit")]
    InvalidGroup,
    #[error("job target stopped: {0}")]
    WorkerStopped(String),
}
#[derive(Debug, Clone)]
pub struct JobParams {
    pub name: Option<String>,
    pub payload: Option<Value>,
    /// Covers readiness, admission, and execution. The timer runs even when the handle is not polled.
    pub timeout: Option<Duration>,
    pub event_capacity: usize,
}
impl Default for JobParams {
    fn default() -> Self {
        Self {
            name: None,
            payload: None,
            timeout: None,
            event_capacity: 128,
        }
    }
}
#[derive(Clone)]
pub struct JobClient {
    name: String,
    bus: LocalBus,
    registry: WorkerRegistry,
}
impl JobClient {
    pub fn new(name: impl Into<String>, bus: LocalBus, registry: WorkerRegistry) -> Self {
        Self {
            name: name.into(),
            bus,
            registry,
        }
    }
    pub fn request(
        &self,
        target: impl Into<String>,
        params: JobParams,
    ) -> Result<JobHandle, JobError> {
        let id = format!(
            "{}:{}-{}",
            self.registry.runner_name(),
            self.name,
            NEXT_JOB.fetch_add(1, Ordering::Relaxed)
        );
        self.request_with_id(target.into(), params, id, None)
    }
    fn request_with_id(
        &self,
        target: String,
        params: JobParams,
        id: String,
        member: Option<usize>,
    ) -> Result<JobHandle, JobError> {
        if params.event_capacity == 0 {
            return Err(JobError::InvalidEventCapacity);
        }
        let deadline = params
            .timeout
            .map(|timeout| {
                Instant::now()
                    .checked_add(timeout)
                    .ok_or(JobError::InvalidTimeout)
            })
            .transpose()?;
        let source = match member {
            Some(member) => format!("{}/group/{id}/{member}", self.name),
            None => format!("{}/job/{id}", self.name),
        };
        let subscription = self.bus.subscribe_target(source.clone())?;
        let (result_tx, result) = watch::channel(None);
        let (events_tx, events) = broadcast::channel(params.event_capacity);
        let (dropped_tx, mut dropped) = watch::channel(false);
        let (cancelled_tx, mut cancelled) = watch::channel(false);
        let handle = JobHandle {
            id: id.clone(),
            source: source.clone(),
            target: target.clone(),
            bus: self.bus.clone(),
            result,
            events,
            dropped: dropped_tx,
            cancelled: cancelled_tx,
        };
        let bus = self.bus.clone();
        let registry = self.registry.clone();
        let requester = self.name.clone();
        tokio::spawn(async move {
            let cancel = BusMessage {
                source: source.clone(),
                target: Some(target.clone()),
                data: MessageData::JobCancel {
                    job_id: id.clone(),
                    reason: Some("caller cancelled or deadline expired".into()),
                },
            };
            let mut admitted = false;
            let execute = async {
                let local_target = tokio::select! {
                    biased;
                    closed = bus.closed() => { closed?; return Err(JobError::Bus(BusError::Closed)); }
                    ready = registry.wait_ready(&target) => ready.runner == registry.runner_name(),
                };
                bus.publish_wait(BusMessage {
                    source: source.clone(),
                    target: Some(target.clone()),
                    data: MessageData::JobRequest {
                        job_id: id.clone(),
                        name: params.name,
                        payload: params.payload,
                    },
                })
                .await?;
                admitted = true;
                let mut readiness = registry.watch(&target);
                loop {
                    let stopped = readiness.borrow_and_update().is_none();
                    // A remote registry snapshot can overtake a normal response
                    // already admitted to network egress. Only local disappearance
                    // proves no response remains outside this subscription.
                    let message = if stopped && local_target {
                        match subscription.try_recv()? {
                            Some(message) => Some(message),
                            None => return Err(JobError::WorkerStopped(target.clone())),
                        }
                    } else {
                        tokio::select! {
                            biased;
                            value=subscription.recv()=>value?,
                            _=readiness.changed()=>continue,
                        }
                    };
                    let Some(message) = message else {
                        return Err(JobError::Bus(BusError::Closed));
                    };
                    if message.source != target
                        || message.target.as_deref() != Some(source.as_str())
                    {
                        continue;
                    }
                    let event = match message.data {
                        data @ MessageData::Speak { .. } => {
                            // The private reply inbox owns this request. Only its
                            // validated peer may send speech to the logical caller.
                            // Await admission before consuming a terminal response;
                            // the outer cancellation/deadline drops blocked relays.
                            bus.publish_wait(BusMessage {
                                source: target.clone(),
                                target: Some(requester.clone()),
                                data,
                            })
                            .await?;
                            None
                        }
                        MessageData::JobResponse {
                            job_id,
                            status,
                            response,
                            ..
                        } if job_id == id => return Ok(JobResponse { status, response }),
                        MessageData::JobUpdate { job_id, update, .. } if job_id == id => {
                            Some(JobEvent::Update(update))
                        }
                        MessageData::StreamStart { job_id } if job_id == id => {
                            Some(JobEvent::StreamStart)
                        }
                        MessageData::StreamData { job_id, data } if job_id == id => {
                            Some(JobEvent::StreamData(data))
                        }
                        MessageData::StreamEnd { job_id } if job_id == id => {
                            Some(JobEvent::StreamEnd)
                        }
                        _ => None,
                    };
                    if let Some(event) = event {
                        let _ = events_tx.send(event);
                    }
                }
            };
            let outcome = tokio::select! {
                biased;
                _=async {match deadline {Some(deadline)=>tokio::time::sleep_until(deadline).await,None=>std::future::pending().await}}=>None,
                _=dropped.changed()=>Some(Err(JobError::DriverStopped)),
                _=async { if !*cancelled.borrow_and_update() { let _ = cancelled.changed().await; } }=>Some(Ok(JobResponse::cancelled())),
                outcome=execute=>Some(outcome),
            };
            let outcome = match outcome {
                None => Err(JobError::Timeout {
                    cancellation_error: if admitted {
                        bus.publish(cancel).err()
                    } else {
                        None
                    },
                }),
                Some(Err(JobError::DriverStopped)) => {
                    if admitted {
                        let _ = bus.publish(cancel);
                    }
                    Err(JobError::DriverStopped)
                }
                Some(Ok(response)) if response.status == JobStatus::Cancelled => {
                    if admitted {
                        let _ = bus.publish(cancel);
                    }
                    Ok(response)
                }
                Some(outcome) => outcome,
            };
            result_tx.send_replace(Some(outcome));
        });
        Ok(handle)
    }
}
/// Owns one request. Dropping it cancels the local driver and attempts a remote
/// cancellation without blocking; use `cancel().await` to check admission.
/// Slow event readers receive `EventLagged`; terminal results never use the event ring.
/// Speech from the validated job peer is relayed from this unique private inbox
/// to the logical caller before a following normal terminal response is observed.
/// The bus is a trusted in-process transport; source names are not authentication.
pub struct JobHandle {
    id: String,
    source: String,
    target: String,
    bus: LocalBus,
    result: watch::Receiver<Option<Result<JobResponse, JobError>>>,
    events: broadcast::Receiver<JobEvent>,
    dropped: watch::Sender<bool>,
    cancelled: watch::Sender<bool>,
}
impl JobHandle {
    pub fn id(&self) -> &str {
        &self.id
    }
    pub fn target(&self) -> &str {
        &self.target
    }
    pub async fn cancel(&self, reason: Option<String>) -> Result<(), BusError> {
        if self.result.borrow().is_some() {
            return Ok(());
        }
        self.cancelled.send_replace(true);
        self.bus
            .publish_wait(BusMessage {
                source: self.source.clone(),
                target: Some(self.target.clone()),
                data: MessageData::JobCancel {
                    job_id: self.id.clone(),
                    reason,
                },
            })
            .await
            .map(|_| ())
    }
    pub async fn request_update(&self) -> Result<(), BusError> {
        self.bus
            .publish_wait(BusMessage {
                source: self.source.clone(),
                target: Some(self.target.clone()),
                data: MessageData::JobUpdateRequest {
                    job_id: self.id.clone(),
                },
            })
            .await
            .map(|_| ())
    }
    pub async fn result(&mut self) -> Result<JobResponse, JobError> {
        loop {
            if let Some(result) = self.result.borrow_and_update().clone() {
                return result;
            }
            if self.result.changed().await.is_err() {
                return Err(JobError::DriverStopped);
            }
        }
    }
    pub async fn next_event(&mut self) -> Result<Option<JobEvent>, JobError> {
        loop {
            match self.events.try_recv() {
                Ok(event) => return Ok(Some(event)),
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    return Err(JobError::EventLagged(n))
                }
                Err(_) => {}
            }
            if let Some(result) = self.result.borrow_and_update().clone() {
                return result.map(|_| None);
            }
            tokio::select! {
                event=self.events.recv()=>match event {Ok(event)=>return Ok(Some(event)),Err(broadcast::error::RecvError::Lagged(n))=>return Err(JobError::EventLagged(n)),Err(_)=>return self.result().await.map(|_|None)},
                changed=self.result.changed()=>if changed.is_err() {return Err(JobError::DriverStopped);},
            }
        }
    }
}
impl Drop for JobHandle {
    fn drop(&mut self) {
        self.dropped.send_replace(true);
    }
}

/// Handler-owned job context. Updates and stream messages preserve data FIFO.
/// Completion is the handler's returned `JobResponse`, including after `stream_end`.
#[derive(Clone)]
pub struct JobContext {
    pub job_id: String,
    pub requester: String,
    pub worker: String,
    pub payload: Option<Value>,
    pub(crate) bus: LocalBus,
    pub(crate) updates: watch::Receiver<u64>,
    pub(crate) registry: WorkerRegistry,
}
impl JobContext {
    /// Create nested worker RPC. Keeping its JobHandle or JobGroupHandle inside
    /// this handler makes Rust ownership propagate handler cancellation on drop.
    pub fn client(&self) -> JobClient {
        JobClient::new(self.worker.clone(), self.bus.clone(), self.registry.clone())
    }
    async fn send(&self, data: MessageData) -> Result<(), BusError> {
        self.bus
            .publish_wait(BusMessage {
                source: self.worker.clone(),
                target: Some(self.requester.clone()),
                data,
            })
            .await
            .map(|_| ())
    }
    pub async fn update(&self, update: Option<Value>, urgent: bool) -> Result<(), BusError> {
        self.send(MessageData::JobUpdate {
            job_id: self.job_id.clone(),
            update,
            urgent,
        })
        .await
    }
    pub async fn stream_start(&self) -> Result<(), BusError> {
        self.send(MessageData::StreamStart {
            job_id: self.job_id.clone(),
        })
        .await
    }
    pub async fn stream_data(&self, data: Value) -> Result<(), BusError> {
        self.send(MessageData::StreamData {
            job_id: self.job_id.clone(),
            data,
        })
        .await
    }
    pub async fn stream_end(&self) -> Result<(), BusError> {
        self.send(MessageData::StreamEnd {
            job_id: self.job_id.clone(),
        })
        .await
    }
    pub async fn update_requested(&mut self) -> bool {
        self.updates.changed().await.is_ok()
    }
}

/// A group shares one job ID across distinct worker targets. Each member has a
/// separate bounded reply subscription so an urgent response cannot consume
/// another member's readiness or completion notification.
#[derive(Debug, Clone)]
pub struct JobGroupParams {
    pub job: JobParams,
    pub fail_fast: bool,
    pub max_workers: usize,
}
impl Default for JobGroupParams {
    fn default() -> Self {
        Self {
            job: JobParams::default(),
            fail_fast: true,
            max_workers: 64,
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct JobGroupEvent {
    pub worker: String,
    pub event: JobEvent,
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct JobGroupResult {
    /// Includes completed, failed, errored and cancelled worker responses.
    pub responses: std::collections::BTreeMap<String, JobResponse>,
    /// Transport, deadline, stopped-worker and local-driver failures.
    pub errors: std::collections::BTreeMap<String, JobError>,
}
impl JobGroupResult {
    pub fn is_success(&self) -> bool {
        self.errors.is_empty()
            && self
                .responses
                .values()
                .all(|r| r.status == JobStatus::Completed)
    }
}
#[derive(Debug, Clone, thiserror::Error, PartialEq)]
#[error("one or more group members failed; partial responses are retained")]
pub struct JobGroupError {
    pub partial: JobGroupResult,
}
struct GroupControl {
    source: String,
    target: String,
    cancelled: watch::Sender<bool>,
    result: watch::Receiver<Option<Result<JobResponse, JobError>>>,
}
impl GroupControl {
    fn cancel_message(&self, id: &str, reason: Option<String>) -> Option<BusMessage> {
        if self.result.borrow().is_some() {
            return None;
        }
        self.cancelled.send_replace(true);
        Some(BusMessage {
            source: self.source.clone(),
            target: Some(self.target.clone()),
            data: MessageData::JobCancel {
                job_id: id.into(),
                reason,
            },
        })
    }
}
pub struct JobGroupHandle {
    id: String,
    controls: std::sync::Arc<Vec<GroupControl>>,
    bus: LocalBus,
    result: watch::Receiver<Option<JobGroupResult>>,
    events: broadcast::Receiver<JobGroupEvent>,
    member_lagged: std::sync::Arc<AtomicU64>,
    dropped: watch::Sender<bool>,
}
impl JobClient {
    pub fn request_group<I, S>(
        &self,
        targets: I,
        params: JobGroupParams,
    ) -> Result<JobGroupHandle, JobError>
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        // Bound iteration even if a caller supplies an unbounded iterator.
        let targets: Vec<String> = targets
            .into_iter()
            .take(params.max_workers.saturating_add(1))
            .map(Into::into)
            .collect();
        let distinct: std::collections::BTreeSet<_> = targets.iter().collect();
        if targets.is_empty()
            || targets.len() > params.max_workers
            || distinct.len() != targets.len()
        {
            return Err(JobError::InvalidGroup);
        }
        if params.job.event_capacity == 0 {
            return Err(JobError::InvalidEventCapacity);
        }
        let id = format!(
            "{}:{}-group-{}",
            self.registry.runner_name(),
            self.name,
            NEXT_JOB.fetch_add(1, Ordering::Relaxed)
        );
        let mut handles = Vec::with_capacity(targets.len());
        let mut controls = Vec::with_capacity(targets.len());
        for (index, target) in targets.into_iter().enumerate() {
            let handle =
                self.request_with_id(target, params.job.clone(), id.clone(), Some(index))?;
            controls.push(GroupControl {
                source: handle.source.clone(),
                target: handle.target.clone(),
                cancelled: handle.cancelled.clone(),
                result: handle.result.clone(),
            });
            handles.push(handle);
        }
        let controls = std::sync::Arc::new(controls);
        let member_lagged = std::sync::Arc::new(AtomicU64::new(0));
        let (events_tx, events) = broadcast::channel(params.job.event_capacity);
        let (result_tx, result) = watch::channel(None);
        let (dropped_tx, mut dropped) = watch::channel(false);
        let handle = JobGroupHandle {
            id: id.clone(),
            controls: controls.clone(),
            bus: self.bus.clone(),
            result,
            events,
            member_lagged: member_lagged.clone(),
            dropped: dropped_tx,
        };
        let bus = self.bus.clone();
        tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            let mut task_workers = std::collections::HashMap::new();
            for mut member in handles {
                let events_tx = events_tx.clone();
                let worker_name = member.target.clone();
                let member_lagged = member_lagged.clone();
                let task = tasks.spawn(async move {
                    let worker = member.target.clone();
                    let result = loop {
                        match member.next_event().await {
                            Ok(Some(event)) => {
                                let _ = events_tx.send(JobGroupEvent {
                                    worker: worker.clone(),
                                    event,
                                });
                            }
                            Ok(None) => break member.result().await,
                            Err(JobError::EventLagged(count)) => {
                                let _ = member_lagged.try_update(
                                    Ordering::SeqCst,
                                    Ordering::SeqCst,
                                    |lost| Some(lost.saturating_add(count)),
                                );
                            }
                            Err(error) => break Err(error),
                        }
                    };
                    (worker, result)
                });
                task_workers.insert(task.id(), worker_name);
            }
            drop(events_tx);
            let mut result = JobGroupResult::default();
            let mut failed = false;
            while !tasks.is_empty() {
                let next = tokio::select! { biased;
                    _ = dropped.changed() => { tasks.abort_all(); break; },
                    next = tasks.join_next_with_id() => next,
                };
                let completed = match next {
                    Some(Ok((id, value))) => {
                        task_workers.remove(&id);
                        Some(value)
                    }
                    Some(Err(error)) => task_workers
                        .remove(&error.id())
                        .map(|worker| (worker, Err(JobError::DriverStopped))),
                    None => None,
                };
                if let Some((worker, response)) = completed {
                    let unsuccessful = !matches!(&response, Ok(response) if response.status == JobStatus::Completed);
                    match response {
                        Ok(response) => {
                            result.responses.insert(worker, response);
                        }
                        Err(error) => {
                            result.errors.insert(worker, error);
                        }
                    }
                    if unsuccessful && params.fail_fast && !failed {
                        failed = true;
                        for control in controls.iter() {
                            if let Some(message) = control
                                .cancel_message(&id, Some("another group member failed".into()))
                            {
                                let _ = bus.publish(message);
                            }
                        }
                    }
                }
            }
            // Dropping the JoinSet and member handles cancels any unfinished jobs.
            result_tx.send_replace(Some(result));
        });
        Ok(handle)
    }
}
impl JobGroupHandle {
    pub fn id(&self) -> &str {
        &self.id
    }
    /// Signal every local member before awaiting any bounded bus admission.
    /// A successful return confirms cancellation admission, not remote execution termination.
    pub async fn cancel(&self, reason: Option<String>) -> Result<(), BusError> {
        let messages: Vec<_> = self
            .controls
            .iter()
            .filter_map(|control| control.cancel_message(&self.id, reason.clone()))
            .collect();
        let mut error = None;
        for message in messages {
            if let Err(err) = self.bus.publish_wait(message).await {
                error.get_or_insert(err);
            }
        }
        error.map_or(Ok(()), Err)
    }
    pub async fn request_updates(&self) -> Result<(), BusError> {
        for control in self
            .controls
            .iter()
            .filter(|control| control.result.borrow().is_none())
        {
            self.bus
                .publish_wait(BusMessage {
                    source: control.source.clone(),
                    target: Some(control.target.clone()),
                    data: MessageData::JobUpdateRequest {
                        job_id: self.id.clone(),
                    },
                })
                .await?;
        }
        Ok(())
    }
    pub async fn result(&mut self) -> Result<JobGroupResult, JobGroupError> {
        loop {
            if let Some(result) = self.result.borrow_and_update().clone() {
                return if result.is_success() {
                    Ok(result)
                } else {
                    Err(JobGroupError { partial: result })
                };
            }
            if self.result.changed().await.is_err() {
                let mut partial = JobGroupResult::default();
                partial
                    .errors
                    .insert("group".into(), JobError::DriverStopped);
                return Err(JobGroupError { partial });
            }
        }
    }
    pub async fn next_event(&mut self) -> Result<Option<JobGroupEvent>, JobError> {
        loop {
            let lost = self.member_lagged.swap(0, Ordering::SeqCst);
            if lost != 0 {
                return Err(JobError::EventLagged(lost));
            }
            match self.events.try_recv() {
                Ok(event) => return Ok(Some(event)),
                Err(broadcast::error::TryRecvError::Lagged(n)) => {
                    return Err(JobError::EventLagged(n))
                }
                Err(_) => {}
            }
            if self.result.borrow_and_update().is_some() {
                return Ok(None);
            }
            tokio::select! {
                event = self.events.recv() => match event {
                    Ok(event) => return Ok(Some(event)),
                    Err(broadcast::error::RecvError::Lagged(n)) => return Err(JobError::EventLagged(n)),
                    Err(_) => { let _ = self.result.changed().await; },
                },
                changed = self.result.changed() => if changed.is_err() { return Err(JobError::DriverStopped); },
            }
        }
    }
}
impl Drop for JobGroupHandle {
    fn drop(&mut self) {
        for control in self.controls.iter() {
            if let Some(message) =
                control.cancel_message(&self.id, Some("job group dropped".into()))
            {
                let _ = self.bus.publish(message);
            }
        }
        self.dropped.send_replace(true);
    }
}
