//! Linear pipelines with two concurrent processing lanes per processor.
#[path = "pipeline_lifecycle.rs"]
mod lifecycle;
#[path = "pipeline_stream.rs"]
pub(crate) mod stream;
use lifecycle::{LifecycleGroup, LifecycleOwner};

use crate::*;
use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{mpsc, watch, Notify};
use tokio::task::{AbortHandle, JoinHandle, JoinSet};
use tokio::time::Instant;

#[cfg(test)]
#[path = "pipeline_flush_tests.rs"]
mod flush_tests;

static NEXT_RUNTIME_ID: AtomicU64 = AtomicU64::new(1);

/// Applied once per attributed processor, when an error from an unusable
/// processor reaches the worker's upstream boundary. Health changes alone do
/// not request shutdown. Recovery before that boundary suppresses the policy.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ProcessorUnusablePolicy {
    #[default]
    Continue,
    End,
    Cancel,
}

#[derive(Debug, Clone)]
pub struct PipelineConfig {
    pub queue_capacity: usize,
    pub system_queue_capacity: usize,
    pub output_capacity: usize,
    pub max_background_tasks: usize,
    pub error_history_capacity: usize,
    pub startup_timeout: Duration,
    pub shutdown_timeout: Duration,
    pub start: StartFrame,
    pub start_metadata: BTreeMap<String, serde_json::Value>,
    pub processor_unusable_policy: ProcessorUnusablePolicy,
}
impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            queue_capacity: 128,
            system_queue_capacity: 64,
            output_capacity: 1024,
            max_background_tasks: 128,
            error_history_capacity: 256,
            startup_timeout: Duration::from_secs(10),
            shutdown_timeout: Duration::from_secs(30),
            start: StartFrame::default(),
            start_metadata: BTreeMap::new(),
            processor_unusable_policy: ProcessorUnusablePolicy::Continue,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    Starting,
    Running,
    Ended,
    Stopped,
    Cancelled,
    Failed,
}
impl WorkerState {
    pub fn is_finished(self) -> bool {
        !matches!(self, Self::Starting | Self::Running)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PipelineError {
    #[error("invalid pipeline configuration: {0}")]
    InvalidConfiguration(String),
    #[error("pipeline startup timed out")]
    StartupTimeout,
    #[error("pipeline shutdown timed out")]
    ShutdownTimeout,
    #[error("pipeline is closed")]
    Closed,
    #[error("Start is managed by Pipeline::start")]
    DuplicateStart,
    #[error("terminal frames must be sent downstream")]
    TerminalUpstream,
    #[error("backpressure: {0}")]
    Backpressure(String),
    #[error("processor task panicked: {0}")]
    ProcessorPanic(String),
    #[error("pipeline execution failed: {0}")]
    Execution(String),
    #[error("worker has already been joined")]
    AlreadyJoined,
}
impl From<QueueError> for PipelineError {
    fn from(error: QueueError) -> Self {
        match error {
            QueueError::Closed => Self::Closed,
            QueueError::Full => Self::Backpressure("input queue is full".into()),
        }
    }
}
#[derive(Debug, Clone)]
pub struct WorkerReport {
    pub state: WorkerState,
    pub errors: Vec<ErrorFrame>,
    pub dropped_frames: u64,
    pub total_errors: u64,
    pub evicted_errors: u64,
}

pub struct Pipeline {
    idle: Option<crate::WorkerIdleConfig>,
    heartbeats: Option<crate::HeartbeatConfig>,
    processors: Vec<Arc<dyn FrameProcessor>>,
    observers: Vec<crate::observer::ObserverRegistration>,
    observer_clock: Arc<dyn ObserverClock>,
    inherited_observers: Option<Arc<crate::observer::ObserverHub>>,
    lifecycle_owner: Option<LifecycleOwner>,
}
impl Pipeline {
    pub fn new(processors: Vec<Arc<dyn FrameProcessor>>) -> Self {
        Self {
            processors,
            heartbeats: None,
            idle: None,
            observers: vec![],
            observer_clock: Arc::new(MonotonicObserverClock::default()),
            inherited_observers: None,
            lifecycle_owner: None,
        }
    }
    /// Enable an explicit root-wide idle policy. Existing pipelines remain off.
    /// The configured default cancels this root after 300 seconds without a
    /// selected first-push activity; use NotifyOnly for non-cancelling monitoring.
    pub fn with_idle_timeout(mut self, config: crate::WorkerIdleConfig) -> Self {
        self.idle = Some(config);
        self
    }
    /// Enable bounded traversal diagnostics for this root pipeline only.
    /// Configuration validates at start; defaults remain disabled. This does not
    /// automatically cancel, repair or mark processors unusable on a timeout.
    pub fn with_heartbeats(mut self, config: crate::HeartbeatConfig) -> Self {
        self.heartbeats = Some(config);
        self
    }
    /// Register a bounded, independently dispatched observer before starting.
    /// Register a lifecycle-owning instance once per root, and let nested
    /// pipelines inherit it. Reusing the instance across registrations couples
    /// their cancellation and allows their callbacks to run concurrently.
    pub fn add_observer(
        &mut self,
        observer: Arc<dyn PipelineObserver>,
        options: ObserverOptions,
    ) -> ObserverHandle {
        let stats = ObserverHandle::default();
        self.observers.push(crate::observer::ObserverRegistration {
            observer,
            options,
            stats: stats.clone(),
        });
        stats
    }
    pub fn with_observer(
        mut self,
        observer: Arc<dyn PipelineObserver>,
        options: ObserverOptions,
    ) -> Self {
        self.add_observer(observer, options);
        self
    }
    pub fn with_observer_clock(mut self, clock: Arc<dyn ObserverClock>) -> Self {
        self.observer_clock = clock;
        self
    }
    /// Nested runtimes share dispatch, clock and global observation. The parent
    /// owns shutdown. Local registrations cannot be combined with inheritance.
    pub fn inherit_observers(mut self, context: &ProcessorContext) -> Self {
        self.inherited_observers = Some(context.runtime.observers.clone());
        self
    }
    /// Join a processor-owned composition's graceful shutdown. Child terminals
    /// stage at their boundaries; the root drains the entire composition before
    /// closing children, and waits for their cleanup before publishing its End.
    /// At most 4096 live runtimes may belong to one composition. Consumers of
    /// each child output stream remain responsible for reading it; untracked
    /// external forwarding tasks are not implicitly owned by this lifecycle.
    pub fn inherit_lifecycle(mut self, context: &ProcessorContext) -> Self {
        self.lifecycle_owner = Some(LifecycleOwner {
            runtime: Arc::downgrade(&context.runtime),
            index: context.index,
        });
        self
    }
    pub async fn start(mut self, config: PipelineConfig) -> Result<PipelineWorker, PipelineError> {
        if let Some(idle) = self.idle {
            idle.validate()?;
            if self.lifecycle_owner.is_some() || self.inherited_observers.is_some() {
                return Err(PipelineError::InvalidConfiguration(
                    "idle policy must be configured on the root pipeline".into(),
                ));
            }
        }
        if let Some(heartbeats) = self.heartbeats {
            heartbeats.validate()?;
            if self.lifecycle_owner.is_some() || self.inherited_observers.is_some() {
                return Err(PipelineError::InvalidConfiguration(
                    "heartbeats must be configured on the root pipeline".into(),
                ));
            }
        }
        if config.queue_capacity == 0
            || config.system_queue_capacity == 0
            || config.output_capacity == 0
            || config.max_background_tasks == 0
        {
            return Err(PipelineError::InvalidConfiguration(
                "all queue capacities must be positive".into(),
            ));
        }
        if config.start.audio_in_sample_rate == 0 || config.start.audio_out_sample_rate == 0 {
            return Err(PipelineError::InvalidConfiguration(
                "sample rates must be positive".into(),
            ));
        }
        if self.processors.is_empty() {
            self.processors.push(Arc::new(IdentityProcessor));
        }
        if self
            .observers
            .iter()
            .any(|observer| observer.options.capacity == 0)
        {
            return Err(PipelineError::InvalidConfiguration(
                "observer capacities must be positive".into(),
            ));
        }
        if self.inherited_observers.is_some() && !self.observers.is_empty() {
            return Err(PipelineError::InvalidConfiguration(
                "inherited observers cannot be combined with local registrations".into(),
            ));
        }
        let observer_owner = self.inherited_observers.is_none();
        let observers = self.inherited_observers.take().unwrap_or_else(|| {
            Arc::new(crate::observer::ObserverHub::new(
                self.observers,
                self.observer_clock,
            ))
        });
        let runtime_id = NEXT_RUNTIME_ID.fetch_add(1, Ordering::Relaxed);
        let processor_info = self
            .processors
            .iter()
            .enumerate()
            .map(|(index, processor)| ObservedProcessor {
                runtime_id,
                index,
                name: Arc::from(processor.name()),
            })
            .collect();
        let (output_tx, output_rx) = mpsc::channel(config.output_capacity);
        let (state_tx, _) = watch::channel(WorkerState::Starting);
        let owner = self
            .lifecycle_owner
            .as_ref()
            .map(|owner| owner.runtime.upgrade().ok_or(PipelineError::Closed))
            .transpose()?;
        let lifecycle = owner.as_ref().map_or_else(
            || {
                Arc::new(LifecycleGroup::new(
                    runtime_id,
                    self.heartbeats,
                    self.idle
                        .map(|config| (config, crate::observer::next_observation_domain_id())),
                ))
            },
            |parent| parent.lifecycle.clone(),
        );
        let admission = lifecycle.admission.clone();
        let interruptions = Arc::new(Mutex::new(crate::queue::InterruptionRegistry::default()));
        let dropped = Arc::new(AtomicU64::new(0));
        let runtime = Arc::new(Runtime {
            id: runtime_id,
            observers,
            observer_owner,
            processor_info,
            admission: admission.clone(),
            lifecycle: lifecycle.clone(),
            lifecycle_owner: self.lifecycle_owner.clone(),
            draining_children: Mutex::new(None),
            cleanup_complete: AtomicBool::new(false),
            terminal_ready: Notify::new(),
            queues: self
                .processors
                .iter()
                .map(|_| {
                    Arc::new(FrameQueue::with_admission(
                        config.queue_capacity,
                        config.system_queue_capacity,
                        admission.clone(),
                        interruptions.clone(),
                        dropped.clone(),
                    ))
                })
                .collect(),
            nodes: self
                .processors
                .iter()
                .map(|processor| {
                    Arc::new(NodeState::new(
                        config.max_background_tasks,
                        processor.health().unwrap_or_default(),
                    ))
                })
                .collect(),
            config,
            output: Mutex::new(Some(output_tx)),
            state: state_tx,
            failure: Mutex::new(None),
            errors: Mutex::new(BTreeMap::new()),
            total_errors: AtomicU64::new(0),
            evicted_errors: AtomicU64::new(0),
            dropped,
            accepting: AtomicBool::new(true),
            input_lock: tokio::sync::Mutex::new(()),
            ending: Mutex::new(None),
            graceful_terminal: Mutex::new(None),
            unusable_processors: Mutex::new(BTreeSet::new()),
            pending_policy: Mutex::new(None),
            policy_terminal_requested: AtomicBool::new(false),
            policy_changed: Notify::new(),
            health_observations: Mutex::new(vec![]),
        });
        {
            let _admission = admission.lock().unwrap();
            if lifecycle.sealed.load(Ordering::SeqCst)
                || owner.as_ref().is_some_and(|parent| parent.is_finished())
            {
                return Err(PipelineError::Closed);
            }
            let mut members = lifecycle.members.lock().unwrap();
            members.retain(|member| {
                member
                    .upgrade()
                    .is_some_and(|runtime| !runtime.cleanup_complete.load(Ordering::SeqCst))
            });
            if members.len() >= 4096 {
                return Err(PipelineError::Backpressure(
                    "composition runtime limit reached".into(),
                ));
            }
            members.push(Arc::downgrade(&runtime));
        }
        if runtime.observers.enabled() {
            runtime.observers.emit(PipelineObservation::RuntimeStarted {
                runtime_id,
                timestamp_ns: runtime.observers.clock.now_ns(),
            });
            for (node, processor) in runtime.nodes.iter().zip(&runtime.processor_info) {
                runtime.health_observations.lock().unwrap().push(
                    node.health
                        .observe(processor.clone(), runtime.observers.clone()),
                );
            }
        }
        let contexts: Vec<_> = self
            .processors
            .iter()
            .enumerate()
            .map(|(index, processor)| ProcessorContext {
                runtime: runtime.clone(),
                node: runtime.nodes[index].clone(),
                index,
                name: processor.name().into(),
                generation: None,
                system_frame_id: None,
                origins: vec![],
            })
            .collect();
        let mut startup_guard = StartupCleanupGuard {
            runtime: runtime.clone(),
            processors: Some(self.processors.clone()),
            contexts: contexts.clone(),
            attempted: 0,
            current_completed: None,
        };
        let mut setup_errors = Vec::new();
        for (processor, ctx) in self.processors.iter().zip(&contexts) {
            startup_guard.attempted += 1;
            let completed = Arc::new(Notify::new());
            startup_guard.current_completed = Some(completed.clone());
            let completion_guard = CompletionGuard(completed);
            let owned_processor = processor.clone();
            let owned_ctx = ctx.clone();
            let mut task = AbortOnDrop(tokio::spawn(async move {
                let _completion_guard = completion_guard;
                let mut observation = SetupObservationGuard::new(&owned_ctx);
                let result = owned_processor
                    .setup(&owned_ctx.runtime.config.start, &owned_ctx)
                    .await;
                observation.outcome = if result.is_ok() {
                    SetupOutcome::Completed
                } else {
                    SetupOutcome::Failed
                };
                result
            }));
            match tokio::time::timeout(runtime.config.startup_timeout, &mut task.0).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(error))) => setup_errors.push((ctx.clone(), error)),
                Ok(Err(error)) => setup_errors.push((
                    ctx.clone(),
                    ProcessorError::new(format!("setup task panicked: {error}")),
                )),
                Err(_) => {
                    task.0.abort();
                    let _ = (&mut task.0).await;
                    setup_errors.push((
                        ctx.clone(),
                        ProcessorError::new("processor setup timed out"),
                    ));
                }
            }
        }
        let supervisor_runtime = runtime.clone();
        let supervisor_contexts = contexts.clone();
        let supervisor = tokio::spawn(async move {
            supervise(supervisor_runtime, self.processors, supervisor_contexts).await
        });
        startup_guard.processors = None;
        let mut worker = PipelineWorker {
            runtime: runtime.clone(),
            output: output_rx,
            supervisor: Some(supervisor),
        };
        let mut start = Frame::new(FrameData::Start(runtime.config.start.clone()));
        start.metadata = runtime.config.start_metadata.clone();
        runtime.queues[0]
            .send(DirectedFrame::new(start, Direction::Downstream))
            .await?;
        let mut state = runtime.state.subscribe();
        let startup = async {
            loop {
                let current = *state.borrow_and_update();
                if current == WorkerState::Running {
                    return Ok(());
                }
                if current.is_finished() {
                    return Err(runtime
                        .failure
                        .lock()
                        .unwrap()
                        .clone()
                        .unwrap_or(PipelineError::Closed));
                }
                state.changed().await.map_err(|_| PipelineError::Closed)?;
            }
        };
        match tokio::time::timeout(runtime.config.startup_timeout, startup).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                let _ = worker.join().await;
                return Err(error);
            }
            Err(_) => {
                runtime.fail(PipelineError::StartupTimeout);
                let _ = worker.join().await;
                return Err(PipelineError::StartupTimeout);
            }
        }
        for (ctx, error) in setup_errors {
            report_processor_error(&ctx, error).await;
        }
        Ok(worker)
    }
}

#[derive(Clone)]
pub struct PipelineSender {
    runtime: Arc<Runtime>,
}
impl PipelineSender {
    /// Wait for input capacity. Cancellation bypasses the normal-input mutex
    /// and remains possible while another producer is backpressured.
    pub async fn send(&self, frame: Frame, direction: Direction) -> Result<(), PipelineError> {
        frame
            .validate_selected_tool_authority()
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        if frame.data.is_start() {
            return Err(PipelineError::DuplicateStart);
        }
        if frame.is_terminal() && direction == Direction::Upstream {
            return Err(PipelineError::TerminalUpstream);
        }
        let index = match direction {
            Direction::Downstream => 0,
            Direction::Upstream => self.runtime.queues.len() - 1,
        };
        let cancelling = frame.data.is_cancel();
        let terminal = frame.is_terminal();
        if cancelling {
            {
                let _admission = self.runtime.admission.lock().unwrap();
                if self.state().is_finished() {
                    return Err(PipelineError::Closed);
                }
                self.runtime.close_input();
            }
            return self.runtime.queues[index]
                .send_checked(
                    DirectedFrame::new(frame, direction),
                    || !self.state().is_finished(),
                    || {},
                )
                .await
                .map_err(Into::into);
        }
        if frame.is_system() {
            let administrative = matches!(
                frame.data,
                FrameData::Interruption
                    | FrameData::InterruptionWorker
                    | FrameData::ProcessorResume { .. }
                    | FrameData::ProcessorResumeUrgent { .. }
                    | FrameData::ProcessorResumeTarget(_)
                    | FrameData::ProcessorPauseUrgent { .. }
            );
            return self.runtime.queues[index]
                .send_checked(
                    DirectedFrame::new(frame, direction),
                    || {
                        !self.state().is_finished()
                            && (administrative || self.runtime.accepting.load(Ordering::SeqCst))
                    },
                    || {},
                )
                .await
                .map_err(Into::into);
        }
        let _guard = self.runtime.input_lock.lock().await;
        self.runtime.queues[index]
            .send_checked(
                DirectedFrame::new(frame, direction),
                || self.runtime.accepting.load(Ordering::SeqCst) && !self.state().is_finished(),
                || {
                    if terminal {
                        self.runtime.close_input();
                    }
                },
            )
            .await
            .map_err(Into::into)
    }
    /// Nonblocking child-pipeline admission with the source invocation's epoch.
    /// Stale interruptible emissions are discarded; full lanes return explicit
    /// backpressure. This lifecycle's owned heartbeat diagnostics alone may be
    /// softly dropped under pressure. Source/destination cutoffs remain atomic.
    pub fn try_send_from(
        &self,
        frame: Frame,
        direction: Direction,
        source: &ProcessorContext,
    ) -> Result<(), PipelineError> {
        frame
            .validate_selected_tool_authority()
            .map_err(|error| PipelineError::Execution(error.to_string()))?;
        if frame.data.is_start() {
            return Err(PipelineError::DuplicateStart);
        }
        if frame.is_terminal() && direction == Direction::Upstream {
            return Err(PipelineError::TerminalUpstream);
        }
        source.with_admission(&self.runtime, || {
            let index = match direction {
                Direction::Downstream => 0,
                Direction::Upstream => self.runtime.queues.len() - 1,
            };
            let generation = source.frame_generation(&frame);
            if frame.interruptible
                && (source.source_is_stale(generation)
                    || generation
                        .is_some_and(|epoch| self.runtime.queues[index].generation() > epoch))
            {
                self.runtime.drop_frame();
                return Ok(());
            }
            let terminal = frame.is_terminal();
            let administrative = frame.data.is_cancel()
                || matches!(
                    frame.data,
                    FrameData::Interruption
                        | FrameData::InterruptionWorker
                        | FrameData::ProcessorResume { .. }
                        | FrameData::ProcessorResumeUrgent { .. }
                        | FrameData::ProcessorResumeTarget(_)
                        | FrameData::ProcessorPauseUrgent { .. }
                );
            if self.state().is_finished()
                || (!administrative
                    && !self.runtime.accepting.load(Ordering::SeqCst)
                    && (terminal || !self.runtime.owned_admission(source)))
            {
                return Err(PipelineError::Closed);
            }
            let _input = if frame.is_system() {
                None
            } else {
                match self.runtime.input_lock.try_lock() {
                    Ok(guard) => Some(guard),
                    Err(_) if self.runtime.heartbeat_pressure_drop(&frame) => return Ok(()),
                    Err(_) => {
                        return Err(PipelineError::Backpressure(
                            "input admission is busy".into(),
                        ))
                    }
                }
            };
            match self.runtime.queues[index]
                .try_send_unlocked(DirectedFrame::new(frame, direction), generation)
            {
                Ok(()) => {}
                Err((QueueError::Full, dropped))
                    if self.runtime.heartbeat_pressure_drop(&dropped.frame) =>
                {
                    return Ok(())
                }
                Err((error, _)) => return Err(PipelineError::from(error)),
            }
            if terminal {
                self.runtime.close_input();
            }
            Ok(())
        })
    }
    /// Await arrival of a graceful terminal at this runtime's output boundary.
    /// An inherited child remains live until the composition's root commits it.
    pub async fn wait_terminal_staged(&self) -> Result<(), PipelineError> {
        loop {
            let ready = self.runtime.terminal_ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if let Some(error) = self.runtime.failure.lock().unwrap().clone() {
                return Err(error);
            }
            if self.runtime.graceful_terminal.lock().unwrap().is_some()
                || matches!(self.state(), WorkerState::Ended | WorkerState::Stopped)
            {
                return Ok(());
            }
            if self.state().is_finished() {
                return Err(PipelineError::Closed);
            }
            ready.await;
        }
    }
    pub async fn end(&self) -> Result<(), PipelineError> {
        self.send(Frame::end(), Direction::Downstream).await
    }
    pub async fn cancel(&self, reason: impl Into<String>) -> Result<(), PipelineError> {
        self.send(Frame::cancel(reason), Direction::Downstream)
            .await
    }
    pub fn watch(&self) -> watch::Receiver<WorkerState> {
        self.runtime.state.subscribe()
    }
    pub fn state(&self) -> WorkerState {
        *self.runtime.state.borrow()
    }
}

pub struct PipelineWorker {
    runtime: Arc<Runtime>,
    output: mpsc::Receiver<DirectedFrame>,
    supervisor: Option<JoinHandle<Result<WorkerReport, PipelineError>>>,
}
impl PipelineWorker {
    /// Index-based lookup avoids ambiguity when several processors share a name.
    pub fn processor_health(&self, index: usize) -> Option<ProcessorHealth> {
        self.runtime
            .nodes
            .get(index)
            .map(|node| node.health.clone())
    }
    /// Processors whose unusability policy was applied in this worker. Entries
    /// remain after restoration, matching the source once-per-processor rule.
    pub fn unusable_processors(&self) -> Vec<ObservedProcessor> {
        self.runtime
            .unusable_processors
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect()
    }
    /// Bounded idle-policy snapshots, available only on the configured root.
    pub fn idle_monitor(&self) -> Option<crate::WorkerIdleMonitor> {
        if self.runtime.lifecycle_owner.is_some() {
            return None;
        }
        self.runtime
            .lifecycle
            .idle
            .as_ref()
            .map(|idle| idle.lock().unwrap().monitor())
    }
    /// Latest-value diagnostics; only a configured root exposes this handle.
    pub fn heartbeat_monitor(&self) -> Option<crate::HeartbeatMonitor> {
        if self.runtime.lifecycle_owner.is_some() {
            return None;
        }
        self.runtime
            .lifecycle
            .heartbeats
            .as_ref()
            .map(|state| state.lock().unwrap().monitor())
    }
    pub fn sender(&self) -> PipelineSender {
        PipelineSender {
            runtime: self.runtime.clone(),
        }
    }
    pub async fn recv(&mut self) -> Option<DirectedFrame> {
        self.output.recv().await
    }
    pub fn try_recv(&mut self) -> Result<DirectedFrame, mpsc::error::TryRecvError> {
        self.output.try_recv()
    }
    pub async fn end(&self) -> Result<(), PipelineError> {
        self.sender().end().await
    }
    pub async fn cancel(&self, reason: impl Into<String>) -> Result<(), PipelineError> {
        self.sender().cancel(reason).await
    }
    pub fn state(&self) -> WorkerState {
        *self.runtime.state.borrow()
    }
    pub fn has_finished(&self) -> bool {
        self.state().is_finished()
    }
    pub fn errors(&self) -> Vec<ErrorFrame> {
        self.runtime
            .errors
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect()
    }
    pub fn total_errors(&self) -> u64 {
        self.runtime.total_errors.load(Ordering::Relaxed)
    }
    pub fn evicted_errors(&self) -> u64 {
        self.runtime.evicted_errors.load(Ordering::Relaxed)
    }
    pub async fn join(&mut self) -> Result<WorkerReport, PipelineError> {
        let supervisor = self
            .supervisor
            .as_mut()
            .ok_or(PipelineError::AlreadyJoined)?;
        let result = supervisor
            .await
            .map_err(|error| PipelineError::ProcessorPanic(error.to_string()));
        self.supervisor.take();
        result?
    }
    pub async fn collect(mut self) -> Result<(Vec<DirectedFrame>, WorkerReport), PipelineError> {
        let mut frames = vec![];
        while let Some(frame) = self.recv().await {
            frames.push(frame);
        }
        let report = self.join().await?;
        Ok((frames, report))
    }
}
impl Drop for PipelineWorker {
    fn drop(&mut self) {
        if !self.state().is_finished() {
            self.runtime.finish(WorkerState::Cancelled);
        }
    }
}

pub(crate) struct Runtime {
    id: u64,
    pub(crate) observers: Arc<crate::observer::ObserverHub>,
    observer_owner: bool,
    pub(crate) processor_info: Vec<ObservedProcessor>,
    pub(crate) admission: Arc<Mutex<()>>,
    lifecycle: Arc<LifecycleGroup>,
    lifecycle_owner: Option<LifecycleOwner>,
    draining_children: Mutex<Option<Vec<Arc<Runtime>>>>,
    cleanup_complete: AtomicBool,
    terminal_ready: Notify,
    pub(crate) queues: Vec<Arc<FrameQueue>>,
    nodes: Vec<Arc<NodeState>>,
    pub(crate) config: PipelineConfig,
    output: Mutex<Option<mpsc::Sender<DirectedFrame>>>,
    state: watch::Sender<WorkerState>,
    failure: Mutex<Option<PipelineError>>,
    errors: Mutex<BTreeMap<u64, ErrorFrame>>,
    total_errors: AtomicU64,
    evicted_errors: AtomicU64,
    dropped: Arc<AtomicU64>,
    accepting: AtomicBool,
    input_lock: tokio::sync::Mutex<()>,
    ending: Mutex<Option<Instant>>,
    graceful_terminal: Mutex<Option<(DirectedFrame, WorkerState)>>,
    unusable_processors: Mutex<BTreeSet<ObservedProcessor>>,
    pending_policy: Mutex<Option<DirectedFrame>>,
    policy_terminal_requested: AtomicBool,
    policy_changed: Notify,
    health_observations: Mutex<Vec<Arc<crate::health::HealthObservation>>>,
}
impl Runtime {
    fn start_idle(&self) {
        if self.lifecycle_owner.is_none() {
            let _admission = self.admission.lock().unwrap();
            if let Some(idle) = &self.lifecycle.idle {
                idle.lock().unwrap().start(Instant::now());
            }
        }
    }
    fn drive_idle(&self) {
        if self.lifecycle_owner.is_some() || self.lifecycle.idle.is_none() {
            return;
        }
        let _admission = self.admission.lock().unwrap();
        let mut idle = self.lifecycle.idle.as_ref().unwrap().lock().unwrap();
        if self.state.borrow().is_finished()
            || !self.accepting.load(Ordering::SeqCst)
            || self.lifecycle.sealed.load(Ordering::SeqCst)
        {
            idle.close();
            return;
        }
        let cancel = idle.tick(Instant::now());
        drop(idle);
        if cancel && !self.policy_terminal_requested.swap(true, Ordering::SeqCst) {
            self.close_input();
            let mut pending = self.pending_policy.lock().unwrap();
            if pending.is_none() {
                *pending = Some(DirectedFrame::new(
                    Frame::cancel("idle timeout"),
                    Direction::Downstream,
                ));
                self.policy_changed.notify_one();
            }
        }
    }
    fn drive_heartbeats(&self) {
        if self.lifecycle_owner.is_some() || self.lifecycle.heartbeats.is_none() {
            return;
        }
        let _admission = self.admission.lock().unwrap();
        if *self.state.borrow() != WorkerState::Running {
            return;
        }
        if let Some(heartbeats) = &self.lifecycle.heartbeats {
            heartbeats.lock().unwrap().tick(
                Instant::now(),
                self.accepting.load(Ordering::SeqCst)
                    && !self.lifecycle.sealed.load(Ordering::SeqCst),
                |frame| {
                    self.queues[0]
                        .try_send_unlocked(DirectedFrame::new(frame, Direction::Downstream), None)
                        .is_ok()
                },
            );
        }
    }
    pub(crate) fn is_heartbeat_probe(&self, frame: &Frame) -> bool {
        self.lifecycle
            .heartbeats
            .as_ref()
            .is_some_and(|state| state.lock().unwrap().owns(frame))
    }
    fn heartbeat_pressure_drop(&self, frame: &Frame) -> bool {
        self.lifecycle
            .heartbeats
            .as_ref()
            .is_some_and(|state| state.lock().unwrap().pressure_drop(frame))
    }
    pub(crate) fn is_finished(&self) -> bool {
        self.state.borrow().is_finished() || self.lifecycle.sealed.load(Ordering::SeqCst)
    }
    pub(crate) fn drop_frame(&self) {
        self.dropped.fetch_add(1, Ordering::Relaxed);
    }
    pub(crate) fn route_unlocked(
        &self,
        from: usize,
        generation: Option<u64>,
        frame: Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        frame.validate_selected_tool_authority()?;
        if frame.interruptible
            && generation.is_some_and(|epoch| self.queues[from].generation() > epoch)
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if self.state.borrow().is_finished()
            || (self.lifecycle.sealed.load(Ordering::SeqCst) && !Self::sealed_control(&frame))
        {
            return Err(ProcessorError::new("pipeline is closed"));
        }
        let destination = match direction {
            Direction::Downstream if from + 1 < self.queues.len() => Some(from + 1),
            Direction::Upstream if from > 0 => Some(from - 1),
            _ => None,
        };
        if destination.is_some_and(|index| {
            frame.interruptible
                && generation.is_some_and(|epoch| self.queues[index].generation() > epoch)
        }) {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        let push_domains = (self.observers.enabled() || self.lifecycle.idle.is_some())
            .then(|| frame.observed_runtimes.clone());
        let idle_activity =
            self.lifecycle.idle.is_some() && crate::idle::IdleState::selected(&frame.data);
        let observation = self.observers.enabled().then(|| {
            (
                ObservedFrame::capture(&frame),
                self.observers.clock.now_ns(),
                self.observers.capture_push(
                    &frame,
                    &self.processor_info[from],
                    self.nodes[from].health.snapshot(),
                ),
            )
        });
        let is_start =
            frame.data.is_start() && direction == Direction::Downstream && destination.is_none();
        let result = if let Some(index) = destination {
            match self.queues[index]
                .try_send_unlocked(DirectedFrame::new(frame, direction), generation)
            {
                Ok(()) => Ok(()),
                Err((QueueError::Full, dropped))
                    if self.heartbeat_pressure_drop(&dropped.frame) =>
                {
                    Ok(())
                }
                Err((error, _)) => {
                    if error == QueueError::Full {
                        self.fail(PipelineError::Backpressure(format!(
                            "processor {index} input lane is full"
                        )));
                    }
                    Err(ProcessorError::from(error))
                }
            }
        } else {
            self.deliver(frame, direction, generation)
        };
        if result.is_ok() {
            let first_push = push_domains.is_some_and(|domains| {
                if let Some(idle) = &self.lifecycle.idle {
                    idle.lock().unwrap().observe_push(
                        &domains,
                        idle_activity,
                        self.observers.id,
                        self.observers.enabled(),
                    )
                } else {
                    domains
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap()
                        .insert(self.observers.id)
                }
            });
            if let Some((frame, timestamp_ns, captures)) = observation {
                self.observers.emit_captured(
                    PipelineObservation::Push {
                        source: self.processor_info[from].clone(),
                        destination: destination.map(|index| self.processor_info[index].clone()),
                        frame,
                        direction,
                        first_push,
                        timestamp_ns,
                    },
                    captures,
                );
                if is_start {
                    self.observers.emit(PipelineObservation::PipelineStarted {
                        runtime_id: self.id,
                        timestamp_ns: self.observers.clock.now_ns(),
                    });
                }
            }
        }
        result
    }
    pub(crate) fn route_tool_progress_unlocked(
        self: &Arc<Self>,
        from: usize,
        frame: Frame,
        direction: Direction,
    ) -> crate::NativeToolProgressEnqueue {
        if let Err(error) = frame.validate_selected_tool_authority() {
            return crate::NativeToolProgressEnqueue::rejected(
                self.clone(),
                frame,
                direction,
                error,
            );
        }
        let generation = None;
        if frame.interruptible
            || self.state.borrow().is_finished()
            || (self.lifecycle.sealed.load(Ordering::SeqCst) && !Self::sealed_control(&frame))
        {
            return crate::NativeToolProgressEnqueue::rejected(
                self.clone(),
                frame,
                direction,
                ProcessorError::new("selected route is closed or interruptible"),
            );
        }
        let destination = match direction {
            Direction::Downstream if from + 1 < self.queues.len() => Some(from + 1),
            Direction::Upstream if from > 0 => Some(from - 1),
            _ => None,
        };
        let Some(destination_index) = destination else {
            return crate::NativeToolProgressEnqueue::rejected(
                self.clone(),
                frame,
                direction,
                ProcessorError::new("selected progress requires an adjacent owned consumer"),
            );
        };
        let push_domains = (self.observers.enabled() || self.lifecycle.idle.is_some())
            .then(|| frame.observed_runtimes.clone());
        let idle_activity =
            self.lifecycle.idle.is_some() && crate::idle::IdleState::selected(&frame.data);
        let observation = self.observers.enabled().then(|| {
            (
                ObservedFrame::capture(&frame),
                self.observers.clock.now_ns(),
                self.observers.capture_push(
                    &frame,
                    &self.processor_info[from],
                    self.nodes[from].health.snapshot(),
                ),
            )
        });
        let is_start =
            frame.data.is_start() && direction == Direction::Downstream && destination.is_none();
        let mut dropped = None;
        let mut failure = None;
        let result = match self.queues[destination_index]
            .try_send_unlocked(DirectedFrame::new(frame, direction), generation)
        {
            Ok(()) => Ok(()),
            Err((error, frame)) => {
                dropped = Some(frame);
                if error == QueueError::Full {
                    failure = Some(PipelineError::Backpressure(format!(
                        "processor {destination_index} selected input lane is full"
                    )));
                }
                Err(ProcessorError::from(error))
            }
        };
        if result.is_ok() {
            let first_push = push_domains.is_some_and(|domains| {
                if let Some(idle) = &self.lifecycle.idle {
                    idle.lock().unwrap().observe_push(
                        &domains,
                        idle_activity,
                        self.observers.id,
                        self.observers.enabled(),
                    )
                } else {
                    domains
                        .get_or_init(Default::default)
                        .lock()
                        .unwrap()
                        .insert(self.observers.id)
                }
            });
            if let Some((frame, timestamp_ns, captures)) = observation {
                self.observers.emit_captured(
                    PipelineObservation::Push {
                        source: self.processor_info[from].clone(),
                        destination: destination.map(|index| self.processor_info[index].clone()),
                        frame,
                        direction,
                        first_push,
                        timestamp_ns,
                    },
                    captures,
                );
                if is_start {
                    self.observers.emit(PipelineObservation::PipelineStarted {
                        runtime_id: self.id,
                        timestamp_ns: self.observers.clock.now_ns(),
                    });
                }
            }
        }
        crate::NativeToolProgressEnqueue {
            runtime: self.clone(),
            error: result.err(),
            dropped,
            failure,
        }
    }
    fn observe_process(&self, index: usize, frame: &Frame, direction: Direction) {
        if !self.observers.enabled() {
            return;
        }
        // Publish an admitted push before a destination observes process entry.
        let _admission = self.admission.lock().unwrap();
        self.observers.emit(PipelineObservation::Process {
            processor: self.processor_info[index].clone(),
            frame: ObservedFrame::capture(frame),
            direction,
            timestamp_ns: self.observers.clock.now_ns(),
        });
    }
    fn deliver(
        &self,
        mut frame: Frame,
        direction: Direction,
        generation: Option<u64>,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && self.lifecycle_owner.is_none()
            && self
                .lifecycle
                .heartbeats
                .as_ref()
                .is_some_and(|state| state.lock().unwrap().receive(&frame, Instant::now()))
        {
            return Ok(());
        }
        let diagnostic_probe = self
            .lifecycle
            .heartbeats
            .as_ref()
            .is_some_and(|state| state.lock().unwrap().owns(&frame));
        let diagnostic_copy = diagnostic_probe.then(|| frame.clone());
        let requested = match &frame.data {
            FrameData::EndWorker(reason) => Some(FrameData::EndDetailed(reason.clone())),
            FrameData::StopWorker => Some(FrameData::Stop),
            FrameData::CancelWorker(reason) => Some(FrameData::CancelDetailed(reason.clone())),
            FrameData::InterruptionWorker => Some(FrameData::Interruption),
            _ => None,
        };
        if let Some(requested) = requested {
            let (index, next) = if direction == Direction::Downstream {
                (
                    self.queues.len() - 1,
                    DirectedFrame::new(frame, Direction::Upstream),
                )
            } else {
                let next = Frame::new(requested);
                if next.is_terminal() {
                    self.close_input();
                }
                (0, DirectedFrame::new(next, Direction::Downstream))
            };
            return self.queues[index]
                .try_send_unlocked(next, generation)
                .map_err(|(error, _)| {
                    self.fail(PipelineError::Backpressure(format!(
                        "worker request could not enter queue: {error}"
                    )));
                    ProcessorError::from(error)
                });
        }
        let flush = match &frame.data {
            FrameData::Flush(probe) => Some((probe.returning, None)),
            FrameData::FlushDetailed(probe) => Some((probe.returning, probe.event.clone())),
            _ => None,
        };
        let mut completed_flush_event = None;
        if let Some((returning, event)) = flush {
            match (direction, returning) {
                (Direction::Downstream, false) => {
                    let index = self.queues.len() - 1;
                    return self.queues[index]
                        .try_send_unlocked(
                            DirectedFrame::new(frame, Direction::Upstream),
                            generation,
                        )
                        .map_err(|(error, _)| ProcessorError::from(error));
                }
                (Direction::Upstream, _) => {
                    let mut frame = frame;
                    match &mut frame.data {
                        FrameData::Flush(probe) => probe.returning = true,
                        FrameData::FlushDetailed(probe) => probe.returning = true,
                        _ => unreachable!(),
                    }
                    return self.queues[0]
                        .try_send_unlocked(
                            DirectedFrame::new(frame, Direction::Downstream),
                            generation,
                        )
                        .map_err(|(error, _)| ProcessorError::from(error));
                }
                _ => completed_flush_event = event,
            }
        }
        let fatal_error = self
            .record_frame_error(&mut frame)
            .filter(|error| error.fatal);
        let error_origin = (direction == Direction::Upstream
            && frame.data.as_error_frame().is_some()
            && fatal_error.is_none())
        .then(|| frame.error_origin.clone())
        .flatten();
        let terminal = if direction == Direction::Downstream {
            match frame.data {
                FrameData::End(_) | FrameData::EndDetailed(_) => Some(WorkerState::Ended),
                FrameData::Stop => Some(WorkerState::Stopped),
                FrameData::Cancel(_) | FrameData::CancelDetailed(_) => Some(WorkerState::Cancelled),
                _ => None,
            }
        } else {
            None
        };
        if matches!(terminal, Some(WorkerState::Ended | WorkerState::Stopped)) {
            self.close_input();
            self.graceful_terminal
                .lock()
                .unwrap()
                .get_or_insert((DirectedFrame::new(frame, direction), terminal.unwrap()));
            self.terminal_ready.notify_waiters();
            return Ok(());
        }
        let start = direction == Direction::Downstream && frame.data.is_start();
        let send_result = self
            .output
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| ProcessorError::new("output is closed"))?
            .try_reserve()
            .map(|permit| {
                if let Some(origin) = error_origin {
                    self.apply_unusable_policy_unlocked(&origin);
                }
                // Reservation admits the receipt even if the receiver closes
                // afterward. Publish completion before waking its consumer;
                // the event attests admission, not receiver consumption.
                if let Some(event) = completed_flush_event {
                    event.set();
                }
                permit.send(DirectedFrame::new(frame, direction));
            });
        if let Err(error) = send_result {
            if matches!(error, mpsc::error::TrySendError::Full(_))
                && diagnostic_copy
                    .as_ref()
                    .is_some_and(|frame| self.heartbeat_pressure_drop(frame))
            {
                return Ok(());
            }
            let failure = match error {
                mpsc::error::TrySendError::Full(_) => PipelineError::Backpressure(
                    "worker output queue is full; consume output concurrently".into(),
                ),
                mpsc::error::TrySendError::Closed(_) => PipelineError::Closed,
            };
            self.fail(failure.clone());
            return Err(ProcessorError::fatal(failure.to_string()));
        }
        if start {
            self.state.send_modify(|state| {
                if *state == WorkerState::Starting {
                    *state = WorkerState::Running;
                }
            });
        }
        if let Some(state) = terminal {
            self.finish(state);
        }
        if let Some(error) = fatal_error {
            self.fail(PipelineError::Execution(error.error));
        }
        Ok(())
    }
    /// Called under admission. A single pending terminal is owned by the
    /// supervisor, so reporting never waits on a full lane or spawns a task.
    fn apply_unusable_policy_unlocked(&self, origin: &ProcessorErrorOrigin) {
        if origin.health.is_usable()
            || !self
                .unusable_processors
                .lock()
                .unwrap()
                .insert(origin.processor.clone())
        {
            return;
        }
        let terminal = match self.config.processor_unusable_policy {
            ProcessorUnusablePolicy::Continue => return,
            ProcessorUnusablePolicy::End if !self.accepting.load(Ordering::SeqCst) => return,
            ProcessorUnusablePolicy::End => Frame::end(),
            ProcessorUnusablePolicy::Cancel => Frame::cancel(format!(
                "{} can no longer do its job",
                origin.processor.name
            )),
        };
        if self.policy_terminal_requested.swap(true, Ordering::SeqCst) {
            return;
        }
        self.close_input();
        let mut pending = self.pending_policy.lock().unwrap();
        if pending.is_none() {
            *pending = Some(DirectedFrame::new(terminal, Direction::Downstream));
            self.policy_changed.notify_one();
        }
    }
    fn admit_pending_policy(&self) {
        let _admission = self.admission.lock().unwrap();
        if self.state.borrow().is_finished() {
            self.pending_policy.lock().unwrap().take();
            return;
        }
        let mut pending = self.pending_policy.lock().unwrap();
        let Some(frame) = pending.take() else {
            return;
        };
        match self.queues[0].try_send_unlocked(frame, None) {
            Ok(()) => {}
            Err((QueueError::Full, frame)) => *pending = Some(*frame),
            Err((QueueError::Closed, _)) => self.fail(PipelineError::Closed),
        }
    }
    pub(crate) fn record_frame_error(&self, frame: &mut Frame) -> Option<ErrorFrame> {
        let error = frame.data.as_error_frame()?;
        let reported = frame.reported_runtimes.get_or_insert_with(Default::default);
        if reported.lock().unwrap().insert(self.id) {
            self.record_error(frame.id, error.clone());
        }
        Some(error)
    }
    pub(crate) fn record_error(&self, id: u64, error: ErrorFrame) {
        let mut errors = self.errors.lock().unwrap();
        if errors.contains_key(&id) {
            return;
        }
        self.total_errors.fetch_add(1, Ordering::Relaxed);
        if self.config.error_history_capacity == 0 {
            self.evicted_errors.fetch_add(1, Ordering::Relaxed);
            return;
        }
        errors.insert(id, error);
        while errors.len() > self.config.error_history_capacity {
            errors.pop_first();
            self.evicted_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
    /// Caller holds admission so no external capacity retry can cross this fence.
    fn close_input(&self) {
        if let Some(idle) = self
            .lifecycle
            .idle
            .as_ref()
            .filter(|_| self.lifecycle_owner.is_none())
        {
            idle.lock().unwrap().close();
        }
        self.accepting.store(false, Ordering::SeqCst);
        self.ending.lock().unwrap().get_or_insert_with(Instant::now);
        for queue in &self.queues {
            queue.wake_senders();
        }
    }
    pub(crate) fn interrupt_unlocked(
        &self,
        index: usize,
        generation: Option<u64>,
        identity: u64,
        token: Option<&AtomicU64>,
    ) -> Option<u64> {
        if generation.is_some_and(|epoch| self.queues[index].generation() > epoch) {
            return None;
        }
        let (epoch, dropped) = self.queues[index].interrupt_now(Some(identity), token);
        self.apply_interruption(index, epoch, dropped);
        Some(epoch)
    }
    fn interrupt_at(&self, index: usize, epoch: u64) {
        let _admission = self.admission.lock().unwrap();
        let dropped = self.queues[index].reset_before_generation(epoch);
        self.apply_interruption(index, epoch, dropped);
    }
    pub(crate) fn queue_from_unlocked(
        &self,
        index: usize,
        generation: Option<u64>,
        frame: Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        if self.lifecycle.sealed.load(Ordering::SeqCst) && !Self::sealed_control(&frame) {
            return Err(ProcessorError::new("pipeline is closed"));
        }
        if frame.interruptible
            && generation.is_some_and(|epoch| self.queues[index].generation() > epoch)
        {
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        self.queues[index]
            .try_send_unlocked(DirectedFrame::new(frame, direction), generation)
            .map_err(|(error, _)| ProcessorError::from(error))
    }
    fn apply_interruption(&self, index: usize, epoch: u64, dropped: usize) {
        self.dropped.fetch_add(dropped as u64, Ordering::Relaxed);
        self.nodes[index]
            .signal
            .send_modify(|signal| signal.epoch = signal.epoch.max(epoch));
        self.nodes[index].interrupt_background_before(epoch);
    }
    fn cancel_node(&self, index: usize) {
        let dropped = self.queues[index].discard_normal();
        self.dropped.fetch_add(dropped as u64, Ordering::Relaxed);
        self.nodes[index]
            .signal
            .send_modify(|signal| signal.cancelled = true);
        self.nodes[index].cancel_background();
    }
    fn finish(&self, new_state: WorkerState) {
        self.state.send_modify(|state| {
            if !state.is_finished() {
                *state = new_state;
            }
        });
        self.accepting.store(false, Ordering::SeqCst);
        self.terminal_ready.notify_waiters();
        for queue in &self.queues {
            queue.close();
        }
    }
    pub(crate) fn fail(&self, error: PipelineError) {
        self.lifecycle
            .failure
            .lock()
            .unwrap()
            .get_or_insert(error.clone());
        self.failure.lock().unwrap().get_or_insert(error);
        self.state.send_replace(WorkerState::Failed);
        self.accepting.store(false, Ordering::SeqCst);
        self.terminal_ready.notify_waiters();
        for queue in &self.queues {
            queue.close();
        }
    }
}

#[derive(Clone, Copy, Default)]
struct NodeSignal {
    started: bool,
    paused: bool,
    cancelled: bool,
    epoch: u64,
}
struct BackgroundTask {
    id: u64,
    name: String,
    handle: JoinHandle<Result<(), ProcessorError>>,
    controls: Arc<Mutex<BTreeMap<u64, TaskControl>>>,
}
struct TaskControl {
    interruptible: bool,
    generation: u64,
    abort: AbortHandle,
}
impl Drop for BackgroundTask {
    fn drop(&mut self) {
        self.handle.abort();
        self.controls.lock().unwrap().remove(&self.id);
    }
}
pub(crate) struct NodeState {
    pub(crate) health: ProcessorHealth,
    signal: watch::Sender<NodeSignal>,
    background: Mutex<Vec<BackgroundTask>>,
    controls: Arc<Mutex<BTreeMap<u64, TaskControl>>>,
    max_tasks: usize,
    next_task: AtomicU64,
    closed: AtomicBool,
}
impl NodeState {
    fn new(max_tasks: usize, health: ProcessorHealth) -> Self {
        let (signal, _) = watch::channel(NodeSignal::default());
        Self {
            health,
            signal,
            background: Mutex::new(vec![]),
            controls: Arc::new(Mutex::new(BTreeMap::new())),
            max_tasks,
            next_task: AtomicU64::new(1),
            closed: AtomicBool::new(false),
        }
    }
    pub(crate) fn spawn<F>(
        &self,
        name: String,
        interruptible: bool,
        generation: Option<u64>,
        future: F,
    ) -> Result<u64, ProcessorError>
    where
        F: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        let mut tasks = self.background.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(ProcessorError::new("processor is shutting down"));
        }
        let mut controls = self.controls.lock().unwrap();
        if self.closed.load(Ordering::SeqCst) {
            return Err(ProcessorError::new("processor is shutting down"));
        }
        if interruptible
            && generation.is_some_and(|generation| self.signal.borrow().epoch > generation)
        {
            return Ok(self.next_task.fetch_add(1, Ordering::Relaxed));
        }
        if controls.len() >= self.max_tasks {
            return Err(ProcessorError::fatal("background task limit reached"));
        }
        let id = self.next_task.fetch_add(1, Ordering::Relaxed);
        let handle =
            tokio::spawn(crate::processor::BACKGROUND_INTERRUPTIBLE.scope(interruptible, future));
        controls.insert(
            id,
            TaskControl {
                interruptible,
                generation: generation.unwrap_or_else(|| self.signal.borrow().epoch),
                abort: handle.abort_handle(),
            },
        );
        tasks.push(BackgroundTask {
            id,
            name,
            handle,
            controls: self.controls.clone(),
        });
        Ok(id)
    }
    pub(crate) fn cancel_task(&self, id: u64) -> bool {
        if let Some(task) = self.controls.lock().unwrap().get(&id) {
            task.abort.abort();
            true
        } else {
            false
        }
    }
    fn interrupt_background_before(&self, epoch: u64) {
        for task in self.controls.lock().unwrap().values() {
            if task.interruptible && task.generation < epoch {
                task.abort.abort();
            }
        }
    }
    fn cancel_background(&self) {
        self.closed.store(true, Ordering::SeqCst);
        for task in self.controls.lock().unwrap().values() {
            task.abort.abort();
        }
    }
    fn take_finished(&self) -> Vec<BackgroundTask> {
        let mut tasks = self.background.lock().unwrap();
        let mut finished = vec![];
        let mut i = 0;
        while i < tasks.len() {
            if tasks[i].handle.is_finished() {
                finished.push(tasks.swap_remove(i));
            } else {
                i += 1;
            }
        }
        finished
    }
    async fn drain(&self, ctx: &ProcessorContext) {
        loop {
            let tasks = std::mem::take(&mut *self.background.lock().unwrap());
            if tasks.is_empty() {
                return;
            }
            for task in tasks {
                complete_background(task, ctx).await;
            }
        }
    }
}
async fn complete_background(mut task: BackgroundTask, ctx: &ProcessorContext) {
    match (&mut task.handle).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => report_processor_error(ctx, error).await,
        Err(error) if error.is_cancelled() => {}
        Err(error) => ctx.runtime.fail(PipelineError::ProcessorPanic(format!(
            "{} task {} ({}): {error}",
            ctx.name, task.id, task.name
        ))),
    }
}
async fn report_processor_error(ctx: &ProcessorContext, error: ProcessorError) {
    let frame = ErrorFrame {
        error: error.message,
        fatal: error.fatal,
        processor: Some(ctx.name.clone()),
        category: error.category,
    };
    if let Err(delivery_error) = ctx
        .push_error_with_permanence_override(frame, error.permanence_override)
        .await
    {
        if !ctx.runtime.state.borrow().is_finished() {
            ctx.runtime.fail(PipelineError::Execution(format!(
                "could not deliver processor error: {delivery_error}"
            )));
        }
    }
}
struct SystemProcessingGuard(Arc<FrameQueue>);
impl Drop for SystemProcessingGuard {
    fn drop(&mut self) {
        self.0.finish_system();
    }
}
async fn system_loop(processor: Arc<dyn FrameProcessor>, ctx: ProcessorContext) {
    let queue = ctx.runtime.queues[ctx.index].clone();
    let mut global = ctx.runtime.state.subscribe();
    loop {
        if global.borrow().is_finished() {
            return;
        }
        let started = ctx.node.signal.borrow().started;
        let frame = tokio::select! {
            biased; _ = global.changed() => continue,
            frame = async { if started { queue.recv_system().await } else { queue.recv_start().await } } => frame,
        };
        let Some((epoch, mut item)) = frame else {
            return;
        };
        let _system_guard = SystemProcessingGuard(queue.clone());
        if let Err(error) = item.frame.validate_selected_tool_authority() {
            report_processor_error(&ctx, error).await;
            continue;
        }
        ctx.runtime.record_frame_error(&mut item.frame);
        let is_start = item.frame.data.is_start();
        match &item.frame.data {
            FrameData::Interruption => ctx.runtime.interrupt_at(ctx.index, epoch),
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                ctx.runtime.cancel_node(ctx.index)
            }
            FrameData::ProcessorResume { processor }
            | FrameData::ProcessorResumeUrgent { processor }
                if processor == &ctx.name =>
            {
                queue.set_normal_paused(false);
                ctx.node.signal.send_modify(|signal| signal.paused = false)
            }
            FrameData::ProcessorPauseUrgent { processor } if processor == &ctx.name => {
                queue.set_normal_paused(true);
                ctx.node.signal.send_modify(|signal| signal.paused = true)
            }
            FrameData::ProcessorResumeTarget(processor) if processor.name() == ctx.name => {
                queue.set_normal_paused(false);
                ctx.node.signal.send_modify(|signal| signal.paused = false);
            }
            FrameData::ProcessorPauseUrgentTarget(processor) if processor.name() == ctx.name => {
                queue.set_normal_paused(true);
                ctx.node.signal.send_modify(|signal| signal.paused = true);
            }
            _ => {}
        }
        let mut invocation_ctx = ctx.clone();
        invocation_ctx.generation = Some(AtomicU64::new(epoch));
        invocation_ctx.system_frame_id = Some(item.frame.id);
        let processing = async {
            ctx.runtime
                .observe_process(ctx.index, &item.frame, item.direction);
            processor
                .process(item.frame, item.direction, &invocation_ctx)
                .await
        };
        tokio::pin!(processing);
        let result = loop {
            tokio::select! {
                biased;
                _ = global.changed() => { if global.borrow().is_finished() { return; } },
                result = &mut processing => break result,
            }
        };
        if let Err(error) = result {
            report_processor_error(&ctx, error).await;
            if is_start {
                ctx.runtime.fail(PipelineError::Execution(format!(
                    "{} failed processing Start",
                    ctx.name
                )));
                return;
            }
        }
        if is_start {
            ctx.node.signal.send_modify(|signal| signal.started = true);
        }
    }
}
struct NormalProcessingGuard(Arc<FrameQueue>);
impl Drop for NormalProcessingGuard {
    fn drop(&mut self) {
        self.0.finish_normal();
    }
}
async fn normal_loop(processor: Arc<dyn FrameProcessor>, ctx: ProcessorContext) {
    let mut signal = ctx.node.signal.subscribe();
    let mut global = ctx.runtime.state.subscribe();
    loop {
        let current = *signal.borrow_and_update();
        if global.borrow().is_finished() || current.cancelled {
            return;
        }
        if !current.started || current.paused {
            tokio::select! { _ = signal.changed() => {}, _ = global.changed() => {} }
            continue;
        }
        let item = tokio::select! {
            biased; _ = global.changed() => continue, _ = signal.changed() => continue,
            item = ctx.runtime.queues[ctx.index].recv_normal() => item,
        };
        let Some((epoch, mut item)) = item else {
            return;
        };
        let _normal_guard = NormalProcessingGuard(ctx.runtime.queues[ctx.index].clone());
        let interruptible = item.frame.interruptible;
        if signal.borrow().epoch > epoch && interruptible {
            ctx.runtime.dropped.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        if let Err(error) = item.frame.validate_selected_tool_authority() {
            report_processor_error(&ctx, error).await;
            continue;
        }
        ctx.runtime.record_frame_error(&mut item.frame);
        let pauses = match &item.frame.data {
            FrameData::ProcessorPause { processor } => processor == &ctx.name,
            FrameData::ProcessorPauseTarget(processor) => processor.name() == ctx.name,
            _ => false,
        };
        let mut invocation_ctx = ctx.clone();
        invocation_ctx.generation = Some(AtomicU64::new(epoch));
        let processing = async {
            if item.frame.is_terminal() {
                if let Err(error) = processor.prepare_end(&invocation_ctx).await {
                    report_processor_error(&ctx, error).await;
                }
                ctx.node.drain(&ctx).await;
            }
            // Health is checked at invocation entry, not queue admission. A
            // later transition does not cancel effects already in progress.
            if !item.frame.is_terminal()
                && !ctx.is_usable()
                && !processor.process_when_unusable(&item.frame, item.direction)
            {
                ctx.runtime.drop_frame();
                return Ok(());
            }
            ctx.runtime
                .observe_process(ctx.index, &item.frame, item.direction);
            processor
                .process(item.frame, item.direction, &invocation_ctx)
                .await
        };
        tokio::pin!(processing);
        loop {
            tokio::select! {
                biased;
                _ = global.changed() => { if global.borrow().is_finished() { return; } },
                _ = signal.changed() => {
                    let current = *signal.borrow_and_update(); if current.cancelled { return; }
                    if current.epoch > invocation_ctx.current_generation().unwrap_or(epoch) && interruptible { ctx.runtime.dropped.fetch_add(1, Ordering::Relaxed); break; }
                },
                result = &mut processing => {
                    if let Err(error) = result { report_processor_error(&ctx, error).await; }
                    if pauses { ctx.runtime.queues[ctx.index].set_normal_paused(true); ctx.node.signal.send_modify(|signal| signal.paused = true); } break;
                },
            }
        }
    }
}
struct CompletionGuard(Arc<Notify>);
impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.notify_one();
    }
}
struct StartupCleanupGuard {
    runtime: Arc<Runtime>,
    processors: Option<Vec<Arc<dyn FrameProcessor>>>,
    contexts: Vec<ProcessorContext>,
    attempted: usize,
    current_completed: Option<Arc<Notify>>,
}
impl Drop for StartupCleanupGuard {
    fn drop(&mut self) {
        let Some(mut processors) = self.processors.take() else {
            return;
        };
        let runtime = self.runtime.clone();
        runtime.finish(WorkerState::Cancelled);
        for node in &runtime.nodes {
            node.cancel_background();
        }
        processors.truncate(self.attempted);
        let mut contexts = self.contexts.clone();
        contexts.truncate(self.attempted);
        let completed = self.current_completed.take();
        tokio::spawn(async move {
            let _output_guard = OutputCloseGuard(runtime.clone());
            if let Some(completed) = completed {
                let _ = tokio::time::timeout(runtime.config.shutdown_timeout, completed.notified())
                    .await;
            }
            for ctx in &contexts {
                ctx.node.drain(ctx).await;
            }
            cleanup_processors(&runtime, &processors, &contexts).await;
            if runtime.observer_owner {
                runtime.observers.shutdown(false).await;
            }
        });
    }
}
struct AbortOnDrop<T>(JoinHandle<T>);
impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct OutputCloseGuard(Arc<Runtime>);
impl Drop for OutputCloseGuard {
    fn drop(&mut self) {
        if self.0.lifecycle_owner.is_none() {
            if let Some(idle) = &self.0.lifecycle.idle {
                idle.lock().unwrap().close();
            }
            if let Some(heartbeats) = &self.0.lifecycle.heartbeats {
                heartbeats.lock().unwrap().close();
            }
        }
        self.0.health_observations.lock().unwrap().clear();
        if self.0.observer_owner {
            self.0.observers.abort();
        }
        self.0.output.lock().unwrap().take();
        if !self.0.state.borrow().is_finished() {
            self.0.fail(PipelineError::Execution(
                "supervisor terminated unexpectedly".into(),
            ));
        }
        self.0.cleanup_complete.store(true, Ordering::SeqCst);
    }
}
async fn supervise(
    runtime: Arc<Runtime>,
    processors: Vec<Arc<dyn FrameProcessor>>,
    contexts: Vec<ProcessorContext>,
) -> Result<WorkerReport, PipelineError> {
    let _output_guard = OutputCloseGuard(runtime.clone());
    runtime.start_idle();
    let mut actors = JoinSet::new();
    for (processor, ctx) in processors.iter().zip(&contexts) {
        let processor = processor.clone();
        let ctx = ctx.clone();
        actors.spawn(async move {
            tokio::join!(
                system_loop(processor.clone(), ctx.clone()),
                normal_loop(processor, ctx)
            );
        });
    }
    let mut state = runtime.state.subscribe();
    let mut tick = tokio::time::interval(Duration::from_millis(10));
    while !state.borrow().is_finished() {
        tokio::select! {
            _ = state.changed() => {},
            _ = runtime.policy_changed.notified() => runtime.admit_pending_policy(),
            result = actors.join_next(), if !actors.is_empty() => {
                if let Some(Err(error)) = result { runtime.fail(PipelineError::ProcessorPanic(error.to_string())); }
            },
            _ = tick.tick() => {
                runtime.admit_pending_policy();
                for ctx in &contexts { for task in ctx.node.take_finished() { complete_background(task, ctx).await; } }
                runtime.enforce_shutdown_deadline();
                runtime.finish_if_drained();
            },
        }
        runtime.drive_heartbeats();
        runtime.drive_idle();
    }
    for node in &runtime.nodes {
        node.cancel_background();
    }
    actors.abort_all();
    while let Some(result) = actors.join_next().await {
        if let Err(error) = result {
            if !error.is_cancelled() {
                runtime.fail(PipelineError::ProcessorPanic(error.to_string()));
            }
        }
    }
    for ctx in &contexts {
        ctx.node.drain(ctx).await;
    }
    let stopped = *runtime.state.borrow() == WorkerState::Stopped;
    if !stopped {
        cleanup_processors(&runtime, &processors, &contexts).await;
    }
    if runtime.observer_owner {
        let graceful = matches!(
            *runtime.state.borrow(),
            WorkerState::Ended | WorkerState::Stopped
        );
        runtime.observers.shutdown(graceful).await;
    }
    runtime.output.lock().unwrap().take();
    if let Some(error) = runtime.failure.lock().unwrap().clone() {
        return Err(error);
    }
    let report = WorkerReport {
        state: *runtime.state.borrow(),
        errors: runtime.errors.lock().unwrap().values().cloned().collect(),
        dropped_frames: runtime.dropped.load(Ordering::Relaxed),
        total_errors: runtime.total_errors.load(Ordering::Relaxed),
        evicted_errors: runtime.evicted_errors.load(Ordering::Relaxed),
    };
    Ok(report)
}

async fn cleanup_processors(
    runtime: &Arc<Runtime>,
    processors: &[Arc<dyn FrameProcessor>],
    contexts: &[ProcessorContext],
) {
    for (processor, ctx) in processors.iter().zip(contexts) {
        let owned_processor = processor.clone();
        let mut task = AbortOnDrop(tokio::spawn(async move { owned_processor.cleanup().await }));
        let failure = match tokio::time::timeout(runtime.config.shutdown_timeout, &mut task.0).await
        {
            Ok(Ok(Ok(()))) => None,
            Ok(Ok(Err(error))) => Some(PipelineError::Execution(format!(
                "{} cleanup failed: {}",
                ctx.name, error.message
            ))),
            Ok(Err(error)) => Some(PipelineError::ProcessorPanic(format!(
                "{} cleanup panicked: {error}",
                ctx.name
            ))),
            Err(_) => {
                task.0.abort();
                let _ = (&mut task.0).await;
                Some(PipelineError::ShutdownTimeout)
            }
        };
        if let Some(failure) = failure {
            let mut error = ErrorFrame::new(failure.to_string());
            error.processor = Some(ctx.name.clone());
            runtime.record_error(Frame::text("").id, error);
            runtime.fail(failure);
        }
    }
}

struct SetupObservationGuard<'a> {
    context: &'a ProcessorContext,
    started_ns: Option<u64>,
    outcome: SetupOutcome,
}
impl<'a> SetupObservationGuard<'a> {
    fn new(context: &'a ProcessorContext) -> Self {
        Self {
            context,
            started_ns: context
                .runtime
                .observers
                .enabled()
                .then(|| context.runtime.observers.clock.now_ns()),
            outcome: SetupOutcome::Aborted,
        }
    }
}
impl Drop for SetupObservationGuard<'_> {
    fn drop(&mut self) {
        if let Some(started_ns) = self.started_ns {
            self.context
                .runtime
                .observers
                .emit(PipelineObservation::ProcessorSetup {
                    processor: self.context.runtime.processor_info[self.context.index].clone(),
                    started_ns,
                    finished_ns: self.context.runtime.observers.clock.now_ns(),
                    outcome: self.outcome,
                });
        }
    }
}
