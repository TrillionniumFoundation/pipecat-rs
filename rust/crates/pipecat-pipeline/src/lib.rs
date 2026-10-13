#![doc = include_str!("../README.md")]
//! Real, bounded parallel composition of native [`pipecat_core::Pipeline`] workers.
//! Branches run independently; synchronization occurs at their directional bookends.
use pipecat_core::{
    async_trait, DirectedFrame, Direction, ErrorFrame, Frame, FrameData, FrameProcessor, Pipeline,
    PipelineConfig, PipelineError, PipelineSender, PipelineWorker, ProcessorContext,
    ProcessorError, StartFrame, WorkerReport,
};
use std::collections::{HashSet, VecDeque};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::task::{JoinHandle, JoinSet};

mod direct_tools;
mod switching;
pub use direct_tools::{DirectToolBinding, DirectToolBindingConfig};
pub use switching::{
    LlmSwitcher, ServiceSelection, ServiceSwitcher, ServiceSwitcherStrategy, SwitchReason,
};

pub type Branch = Vec<Arc<dyn FrameProcessor>>;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum FrameOrder {
    #[default]
    Arrival,
    Pipeline,
}

/// Finite limits are intentional: overflows fail rather than deadlock a branch.
#[derive(Debug, Clone)]
pub struct ParallelPipelineConfig {
    pub branch: PipelineConfig,
    pub max_branches: usize,
    pub buffered_frames: usize,
    pub deduplication_capacity: usize,
    pub synchronization_timeout: Duration,
}
impl Default for ParallelPipelineConfig {
    fn default() -> Self {
        Self {
            branch: PipelineConfig::default(),
            max_branches: 64,
            buffered_frames: 4096,
            deduplication_capacity: 16384,
            synchronization_timeout: Duration::from_secs(30),
        }
    }
}

/// A private-identity FIFO control sentinel. Branches must forward control frames.
/// Like Python's SyncFrame, this does not await detached background computation.
#[derive(Debug, Clone)]
pub struct SyncFrame(Frame);
impl Default for SyncFrame {
    fn default() -> Self {
        let mut frame = Frame::new(FrameData::Control);
        frame.name = format!("SyncFrame#{}", frame.id);
        Self(frame)
    }
}
impl SyncFrame {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn into_frame(self) -> Frame {
        self.0
    }
}

/// Fan out each frame to concurrently running native pipelines; merge unique IDs.
/// Start precedes all branch output, and End/Cancel await every branch's cleanup.
pub struct ParallelPipeline {
    engine: Arc<Engine>,
}
impl ParallelPipeline {
    pub fn new(branches: Vec<Branch>) -> Result<Self, ProcessorError> {
        Self::with_config(branches, ParallelPipelineConfig::default())
    }
    pub fn with_config(
        branches: Vec<Branch>,
        config: ParallelPipelineConfig,
    ) -> Result<Self, ProcessorError> {
        Ok(Self {
            engine: Engine::new(branches, config, None, None)?,
        })
    }
    pub fn branch_count(&self) -> usize {
        self.engine.branches.len()
    }
}

/// Buffer each normal input's output until every branch returns its FIFO sentinel.
/// System frames bypass the per-input barrier. Lifecycle barriers still apply.
pub struct SyncParallelPipeline {
    engine: Arc<Engine>,
}
impl SyncParallelPipeline {
    pub fn new(branches: Vec<Branch>) -> Result<Self, ProcessorError> {
        Self::with_config(
            branches,
            FrameOrder::Arrival,
            ParallelPipelineConfig::default(),
        )
    }
    pub fn with_order(branches: Vec<Branch>, order: FrameOrder) -> Result<Self, ProcessorError> {
        Self::with_config(branches, order, ParallelPipelineConfig::default())
    }
    pub fn with_config(
        branches: Vec<Branch>,
        order: FrameOrder,
        config: ParallelPipelineConfig,
    ) -> Result<Self, ProcessorError> {
        Ok(Self {
            engine: Engine::new(branches, config, Some(order), None)?,
        })
    }
    pub fn branch_count(&self) -> usize {
        self.engine.branches.len()
    }
}

macro_rules! implement_processor {
    ($name:ident) => {
        #[async_trait]
        impl FrameProcessor for $name {
            fn name(&self) -> &str {
                stringify!($name)
            }
            async fn setup(
                &self,
                _: &StartFrame,
                ctx: &ProcessorContext,
            ) -> Result<(), ProcessorError> {
                self.engine.state.lock().unwrap().context = Some(ctx.clone());
                Ok(())
            }
            async fn process(
                &self,
                frame: Frame,
                direction: Direction,
                ctx: &ProcessorContext,
            ) -> Result<(), ProcessorError> {
                self.engine.process(frame, direction, ctx).await
            }
            async fn cleanup(&self) -> Result<(), ProcessorError> {
                self.engine.cleanup().await
            }
        }
    };
}
implement_processor!(ParallelPipeline);
implement_processor!(SyncParallelPipeline);

struct RecentIds {
    capacity: usize,
    ids: HashSet<u64>,
    order: VecDeque<u64>,
}
impl RecentIds {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            ids: HashSet::new(),
            order: VecDeque::new(),
        }
    }
    fn insert(&mut self, id: u64) -> bool {
        if !self.ids.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.ids.remove(&old);
            }
        }
        true
    }
    fn contains(&self, id: u64) -> bool {
        self.ids.contains(&id)
    }
}
struct Output {
    branch: usize,
    item: DirectedFrame,
    context: ProcessorContext,
}
struct Batch {
    id: u64,
    sentinels: Vec<u64>,
    direction: Direction,
    completed: Vec<bool>,
    output: Vec<Output>,
}
struct State {
    context: Option<ProcessorContext>,
    initial_start: Option<Frame>,
    started: bool,
    closed: bool,
    senders: Vec<PipelineSender>,
    completed: Vec<bool>,
    stopped: Vec<bool>,
    terminal: Option<u64>,
    terminal_forwarded: bool,
    buffered: Vec<Output>,
    batch: Option<Batch>,
    seen: RecentIds,
    sentinels: RecentIds,
    interruptions: RecentIds,
    failure: Option<String>,
}
struct OwnedTask(JoinHandle<()>);
impl Drop for OwnedTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct Engine {
    branches: Vec<Branch>,
    switching: Option<Arc<switching::SwitchRouting>>,
    config: ParallelPipelineConfig,
    order: Option<FrameOrder>,
    state: Mutex<State>,
    emission: tokio::sync::Mutex<()>,
    tasks: Mutex<Vec<OwnedTask>>,
    shutdown: watch::Sender<bool>,
    changed: Notify,
}
impl Engine {
    fn new(
        branches: Vec<Branch>,
        config: ParallelPipelineConfig,
        order: Option<FrameOrder>,
        switching: Option<Arc<switching::SwitchRouting>>,
    ) -> Result<Arc<Self>, ProcessorError> {
        if branches.is_empty() || branches.len() > config.max_branches {
            return Err(ProcessorError::new(
                "parallel branch count must be within 1..=max_branches",
            ));
        }
        if config.buffered_frames == 0
            || config.deduplication_capacity == 0
            || config.synchronization_timeout.is_zero()
        {
            return Err(ProcessorError::new(
                "parallel capacities and timeout must be positive",
            ));
        }
        let count = branches.len();
        let (shutdown, _) = watch::channel(false);
        Ok(Arc::new(Self {
            branches,
            switching,
            state: Mutex::new(State {
                context: None,
                initial_start: None,
                started: false,
                closed: false,
                senders: vec![],
                completed: vec![false; count],
                stopped: vec![false; count],
                terminal: None,
                terminal_forwarded: false,
                buffered: vec![],
                batch: None,
                seen: RecentIds::new(config.deduplication_capacity),
                sentinels: RecentIds::new(config.deduplication_capacity),
                interruptions: RecentIds::new(config.deduplication_capacity),
                failure: None,
            }),
            config,
            order,
            emission: tokio::sync::Mutex::new(()),
            tasks: Mutex::new(vec![]),
            shutdown,
            changed: Notify::new(),
        }))
    }

    async fn start(
        self: &Arc<Self>,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        {
            let mut state = self.state.lock().unwrap();
            if state.initial_start.is_some() {
                return Err(ProcessorError::fatal(
                    "parallel pipeline cannot be started twice",
                ));
            }
            state.initial_start = Some(frame.clone());
        }
        let mut starts = JoinSet::new();
        for (index, branch) in self.branches.iter().enumerate() {
            let mut processors: Branch = Vec::with_capacity(branch.len() + 2);
            processors.push(Arc::new(Bookend {
                engine: Arc::downgrade(self),
                index,
                source: true,
            }));
            processors.extend(branch.iter().cloned());
            processors.push(Arc::new(Bookend {
                engine: Arc::downgrade(self),
                index,
                source: false,
            }));
            let mut config = self.config.branch.clone();
            config.start = ctx.configuration().clone();
            config.start_metadata = frame.metadata.clone();
            let parent_context = ctx.clone();
            starts.spawn(async move {
                (
                    index,
                    Pipeline::new(processors)
                        .inherit_observers(&parent_context)
                        .inherit_lifecycle(&parent_context)
                        .start(config)
                        .await,
                )
            });
        }
        let mut workers: Vec<Option<PipelineWorker>> =
            (0..self.branches.len()).map(|_| None).collect();
        while let Some(result) = starts.join_next().await {
            let (index, worker) = result.map_err(|error| {
                ProcessorError::fatal(format!("branch startup panicked: {error}"))
            })?;
            workers[index] = Some(worker.map_err(|error| {
                ProcessorError::fatal(format!("branch {index} startup: {error}"))
            })?);
        }
        {
            let mut state = self.state.lock().unwrap();
            state.senders = workers
                .iter()
                .map(|worker| worker.as_ref().unwrap().sender())
                .collect();
        }
        if let Some(switching) = &self.switching {
            switching.flush_pending_metadata(self, ctx)?;
        }
        for (index, worker) in workers.into_iter().enumerate() {
            let weak = Arc::downgrade(self);
            let shutdown = self.shutdown.subscribe();
            self.tasks
                .lock()
                .unwrap()
                .push(OwnedTask(tokio::spawn(monitor_branch(
                    weak,
                    index,
                    worker.unwrap(),
                    shutdown,
                ))));
        }
        let _emission = self.emission.lock().await;
        let outputs = {
            let mut state = self.state.lock().unwrap();
            check(&state)?;
            state.started = true;
            state.seen.insert(frame.id);
            std::mem::take(&mut state.buffered)
        };
        ctx.push(frame, direction).await?;
        if let Some(switching) = &self.switching {
            switching.report_pending_errors(ctx).await?;
        }
        self.emit(outputs).await
    }

    async fn process(
        self: &Arc<Self>,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            return self.start(frame, direction, ctx).await;
        }
        if let Some(switching) = &self.switching {
            if matches!(frame.data, FrameData::ManuallySwitchService(_)) {
                let _emission = self.emission.lock().await;
                if switching.manual(self, &frame, ctx).await? {
                    return Ok(());
                }
            }
            if let Some(tools) = switching.context_tools(&frame)? {
                self.fanout(&tools, direction, ctx)?;
            }
        }
        if frame.is_terminal() {
            {
                // Selection and terminal admission share one linearization
                // point. Release before awaiting any branch completion.
                let _emission = self.emission.lock().await;
                let mut state = self.state.lock().unwrap();
                check(&state)?;
                state.terminal = Some(frame.id);
                if frame.data.is_cancel() {
                    state.batch = None;
                    state
                        .buffered
                        .retain(|output| !output.item.frame.interruptible);
                }
            }
            self.fanout(&frame, direction, ctx)?;
            if frame.data.is_cancel() {
                self.wait_for(None).await?;
            } else {
                let senders = self.state.lock().unwrap().senders.clone();
                tokio::time::timeout(self.config.synchronization_timeout, async {
                    for sender in senders {
                        sender
                            .wait_terminal_staged()
                            .await
                            .map_err(branch_admission)?;
                    }
                    Ok::<_, ProcessorError>(())
                })
                .await
                .map_err(|_| ProcessorError::fatal("parallel synchronization timed out"))??;
            }
            let _emission = self.emission.lock().await;
            let outputs = {
                let mut state = self.state.lock().unwrap();
                check(&state)?;
                if state.terminal != Some(frame.id) {
                    return Ok(());
                }
                state.terminal_forwarded = true;
                std::mem::take(&mut state.buffered)
            };
            self.emit(outputs).await?;
            return ctx.push(frame, direction).await;
        }
        let interruption = matches!(frame.data, FrameData::Interruption);
        if interruption || (self.order.is_some() && frame.is_system()) {
            let should_emit = {
                let mut state = self.state.lock().unwrap();
                check(&state)?;
                if interruption {
                    state.interruptions.insert(frame.id);
                }
                state.seen.insert(frame.id)
            };
            self.fanout(&frame, direction, ctx)?;
            let _emission = self.emission.lock().await;
            if should_emit {
                ctx.push(frame, direction).await?;
            }
            if interruption {
                let outputs = {
                    let mut state = self.state.lock().unwrap();
                    let mut outputs = state
                        .batch
                        .take()
                        .map(|batch| batch.output)
                        .unwrap_or_default();
                    if state.terminal.is_none() && state.started {
                        outputs.append(&mut state.buffered);
                        outputs
                    } else {
                        state.buffered.extend(outputs);
                        vec![]
                    }
                };
                self.emit(outputs).await?;
                self.changed.notify_waiters();
            }
            return Ok(());
        }
        if self.order.is_none() || frame.is_system() {
            return self.fanout(&frame, direction, ctx);
        }
        let sentinels: Vec<_> = self
            .branches
            .iter()
            .map(|_| SyncFrame::new().into_frame())
            .collect();
        let id = sentinels[0].id;
        {
            let mut state = self.state.lock().unwrap();
            check(&state)?;
            for sentinel in &sentinels {
                state.sentinels.insert(sentinel.id);
            }
            state.batch = Some(Batch {
                id,
                sentinels: sentinels.iter().map(|frame| frame.id).collect(),
                direction,
                completed: vec![false; self.branches.len()],
                output: vec![],
            });
        }
        let _batch_guard = BatchGuard {
            engine: Arc::downgrade(self),
            id,
        };
        let senders = self.state.lock().unwrap().senders.clone();
        for (sender, sentinel) in senders.iter().zip(sentinels) {
            sender
                .try_send_from(frame.clone(), direction, ctx)
                .map_err(branch_admission)?;
            sender
                .try_send_from(sentinel, direction, ctx)
                .map_err(branch_admission)?;
        }
        self.wait_for(Some(id)).await?;
        let _emission = self.emission.lock().await;
        let mut outputs = {
            let mut state = self.state.lock().unwrap();
            check(&state)?;
            match state.batch.take() {
                Some(batch) if batch.id == id => batch.output,
                _ => return Ok(()),
            }
        };
        if self.order == Some(FrameOrder::Pipeline) {
            outputs.sort_by_key(|output| output.branch);
        }
        let mut seen = HashSet::new();
        outputs.retain(|output| seen.insert(output.item.frame.id));
        self.emit(outputs).await
    }

    fn fanout(
        &self,
        frame: &Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let (senders, draining) = {
            let state = self.state.lock().unwrap();
            check(&state)?;
            (state.senders.clone(), state.terminal.is_some())
        };
        let targets = if let Some(switching) = &self.switching {
            if !switching::lifecycle(frame) {
                switching.input_targets(frame)?
            } else {
                (0..senders.len())
                    .map(|index| (index, frame.clone()))
                    .collect()
            }
        } else {
            (0..senders.len())
                .map(|index| (index, frame.clone()))
                .collect()
        };
        for (index, frame) in targets {
            let Some(sender) = senders.get(index) else {
                continue;
            };
            match sender.try_send_from(frame, direction, ctx) {
                Err(PipelineError::Closed) if draining && sender.state().is_finished() => {}
                result => result.map_err(branch_admission)?,
            }
        }
        Ok(())
    }

    async fn capture(
        &self,
        branch: usize,
        frame: Frame,
        direction: Direction,
        source: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream && (frame.data.is_start() || frame.is_terminal()) {
            return Ok(());
        }
        let _emission = self.emission.lock().await;
        if self.switching.is_some() && switch_fenced(&frame) && !source.is_current() {
            return Ok(());
        }
        if let Some(switching) = &self.switching {
            let context = self
                .state
                .lock()
                .unwrap()
                .context
                .clone()
                .ok_or_else(|| ProcessorError::fatal("parallel not set up"))?;
            if !switching
                .capture(self, branch, &frame, direction, &context)
                .await?
            {
                return Ok(());
            }
        }
        let mut queue_interruption = None;
        let output = {
            let mut state = self.state.lock().unwrap();
            if state.closed {
                return Ok(());
            }
            check(&state)?;
            let context = state
                .context
                .as_ref()
                .ok_or_else(|| ProcessorError::fatal("parallel not set up"))?
                .forwarding_context(source);
            if matches!(frame.data, FrameData::Interruption)
                && !state.interruptions.contains(frame.id)
            {
                state.interruptions.insert(frame.id);
                queue_interruption = Some((context.clone(), frame.clone(), direction));
            }
            let active_sentinel = state
                .batch
                .as_ref()
                .is_some_and(|batch| batch.sentinels.contains(&frame.id));
            if active_sentinel || state.sentinels.contains(frame.id) {
                if let Some(batch) = state.batch.as_mut() {
                    if batch.sentinels.get(branch) == Some(&frame.id)
                        && batch.direction == direction
                    {
                        batch.completed[branch] = true;
                        self.changed.notify_waiters();
                    }
                }
                None
            } else {
                let output = Output {
                    branch,
                    item: DirectedFrame::new(frame, direction),
                    context,
                };
                let id = output.item.frame.id;
                if state.seen.contains(id) {
                    return Ok(());
                }
                let system = output.item.frame.is_system();
                if !state.started || (state.terminal.is_some() && !state.terminal_forwarded) {
                    if state.buffered.len() >= self.config.buffered_frames {
                        return Err(ProcessorError::fatal("parallel lifecycle buffer is full"));
                    }
                    state.seen.insert(id);
                    state.buffered.push(output);
                    None
                } else if !system && self.order.is_some() && state.batch.is_some() {
                    let batch = state.batch.as_mut().unwrap();
                    if batch.output.len() >= self.config.buffered_frames {
                        return Err(ProcessorError::fatal("parallel input buffer is full"));
                    }
                    batch.output.push(output);
                    None
                } else {
                    state.seen.insert(id);
                    Some(output)
                }
            }
        };
        if let Some((context, frame, direction)) = queue_interruption {
            context.queue(frame, direction).await?;
        }
        if let Some(output) = output {
            self.emit(vec![output]).await?;
        }
        Ok(())
    }

    async fn emit(&self, mut outputs: Vec<Output>) -> Result<(), ProcessorError> {
        if let Some(switching) = &self.switching {
            outputs.retain(|output| switching.output_allowed(output.branch, &output.item.frame));
            for output in outputs {
                if switch_fenced(&output.item.frame) {
                    output
                        .context
                        .push_with_causal_fence(output.item.frame, output.item.direction)
                        .await?;
                } else {
                    output
                        .context
                        .push(output.item.frame, output.item.direction)
                        .await?;
                }
            }
            Ok(())
        } else {
            emit(outputs).await
        }
    }

    async fn wait_for(&self, batch: Option<u64>) -> Result<(), ProcessorError> {
        let wait = async {
            loop {
                let notified = self.changed.notified();
                tokio::pin!(notified);
                notified.as_mut().enable();
                {
                    let state = self.state.lock().unwrap();
                    check(&state)?;
                    let complete = match batch {
                        None => state.completed.iter().all(|completed| *completed),
                        Some(id) => state.batch.as_ref().is_none_or(|batch| {
                            batch.id != id || batch.completed.iter().all(|completed| *completed)
                        }),
                    };
                    if complete {
                        return Ok(());
                    }
                }
                notified.await;
            }
        };
        tokio::time::timeout(self.config.synchronization_timeout, wait)
            .await
            .map_err(|_| ProcessorError::fatal("parallel synchronization timed out"))?
    }

    async fn branch_finished(&self, index: usize, result: Result<WorkerReport, PipelineError>) {
        let failure = {
            let mut state = self.state.lock().unwrap();
            state.completed[index] = true;
            if result
                .as_ref()
                .is_ok_and(|report| report.state == pipecat_core::WorkerState::Stopped)
            {
                state.stopped[index] = true;
            }
            if state.closed {
                None
            } else {
                let failure = match result {
                    Err(error) => Some(format!("parallel branch {index} failed: {error}")),
                    Ok(report) if state.terminal.is_none() => Some(format!(
                        "parallel branch {index} closed unexpectedly: {:?}",
                        report.state
                    )),
                    Ok(_) => None,
                };
                if let Some(failure) = &failure {
                    state.failure.get_or_insert(failure.clone());
                }
                failure.zip(state.context.clone())
            }
        };
        self.changed.notify_waiters();
        if let Some((failure, context)) = failure {
            let mut error = ErrorFrame::new(failure);
            error.fatal = true;
            let _ = context.push_error(error).await;
        }
    }

    async fn cleanup(&self) -> Result<(), ProcessorError> {
        {
            let mut state = self.state.lock().unwrap();
            state.closed = true;
            state.batch = None;
            state.buffered.clear();
        }
        self.changed.notify_waiters();
        self.shutdown.send_replace(true);
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        let mut failure = None;
        for mut task in tasks {
            match tokio::time::timeout(self.config.branch.shutdown_timeout, &mut task.0).await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    failure.get_or_insert(format!("parallel monitor failed: {error}"));
                }
                Err(_) => {
                    task.0.abort();
                    let _ = (&mut task.0).await;
                    failure.get_or_insert("parallel cleanup timed out".into());
                }
            }
        }
        let stopped = {
            let mut state = self.state.lock().unwrap();
            let mut stopped = vec![];
            for (index, pending) in state.stopped.iter_mut().enumerate() {
                if std::mem::take(pending) {
                    stopped.push(index);
                }
            }
            stopped
        };
        // Stop intentionally retains resources. An explicit owner cleanup must
        // clean those processors, whose stopped workers no longer have a monitor.
        for index in stopped {
            for processor in &self.branches[index] {
                match tokio::time::timeout(self.config.branch.shutdown_timeout, processor.cleanup())
                    .await
                {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => {
                        failure.get_or_insert(error.to_string());
                    }
                    Err(_) => {
                        failure.get_or_insert("stopped branch cleanup timed out".into());
                    }
                }
            }
        }
        self.state.lock().unwrap().context = None;
        match failure {
            Some(message) => Err(ProcessorError::fatal(message)),
            None => Ok(()),
        }
    }
}
impl Drop for Engine {
    fn drop(&mut self) {
        self.shutdown.send_replace(true);
    }
}

fn check(state: &State) -> Result<(), ProcessorError> {
    if let Some(failure) = &state.failure {
        return Err(ProcessorError::fatal(failure.clone()));
    }
    if state.closed {
        return Err(ProcessorError::fatal("parallel pipeline is closed"));
    }
    Ok(())
}
fn branch_admission(error: PipelineError) -> ProcessorError {
    ProcessorError::fatal(format!("parallel branch admission failed: {error}"))
}
fn switch_fenced(frame: &Frame) -> bool {
    frame.interruptible && !switching::lifecycle(frame) && frame.data.as_error_frame().is_none()
}
async fn emit(outputs: Vec<Output>) -> Result<(), ProcessorError> {
    for output in outputs {
        output
            .context
            .push(output.item.frame, output.item.direction)
            .await?;
    }
    Ok(())
}
struct BatchGuard {
    engine: Weak<Engine>,
    id: u64,
}
impl Drop for BatchGuard {
    fn drop(&mut self) {
        if let Some(engine) = self.engine.upgrade() {
            let mut state = engine.state.lock().unwrap();
            if state
                .batch
                .as_ref()
                .is_some_and(|batch| batch.id == self.id)
            {
                let batch = state.batch.take().unwrap();
                if !state.closed && state.terminal.is_none() {
                    if state.buffered.len() + batch.output.len() <= engine.config.buffered_frames {
                        // Preserve causal snapshots until the interruption handler can
                        // discard stale emissions without losing fresh-generation ones.
                        state.buffered.extend(batch.output);
                    } else {
                        state
                            .failure
                            .get_or_insert("parallel interruption buffer is full".into());
                    }
                }
            }
        }
    }
}
struct Bookend {
    engine: Weak<Engine>,
    index: usize,
    source: bool,
}
#[async_trait]
impl FrameProcessor for Bookend {
    fn name(&self) -> &str {
        if self.source {
            "ParallelSource"
        } else {
            "ParallelSink"
        }
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let Some(engine) = self.engine.upgrade() else {
            return Err(ProcessorError::fatal("parallel owner dropped"));
        };
        let outward = matches!(
            (self.source, direction),
            (true, Direction::Upstream) | (false, Direction::Downstream)
        );
        if !outward {
            if self.source && frame.data.is_start() {
                frame = engine
                    .state
                    .lock()
                    .unwrap()
                    .initial_start
                    .clone()
                    .ok_or_else(|| ProcessorError::fatal("missing parent Start"))?;
            }
            return ctx.push(frame, direction).await;
        }
        if direction == Direction::Downstream && (frame.data.is_start() || frame.is_terminal()) {
            // The child runtime must observe lifecycle completion at its boundary.
            ctx.push(frame, direction).await
        } else {
            engine.capture(self.index, frame, direction, ctx).await
        }
    }
}
async fn monitor_branch(
    engine: Weak<Engine>,
    index: usize,
    mut worker: PipelineWorker,
    mut shutdown: watch::Receiver<bool>,
) {
    let mut cancelled = false;
    loop {
        if *shutdown.borrow() && !cancelled {
            cancelled = true;
            let _ = worker.cancel("parallel owner cleanup").await;
        }
        tokio::select! {
            biased;
            _ = shutdown.changed(), if !cancelled => {},
            output = worker.recv() => { if output.is_none() { break; } },
        }
    }
    let result = worker.join().await;
    if let Some(engine) = engine.upgrade() {
        engine.branch_finished(index, result).await;
    }
}
