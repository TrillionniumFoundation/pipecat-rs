//! Processor implementations can emit in either direction and launch owned,
//! interruption-aware asynchronous work through their context.
#[path = "processor_stream.rs"]
mod stream;

use crate::pipeline::{NodeState, Runtime};
use crate::{
    Direction, ErrorCategory, ErrorFrame, Frame, FrameData, ProcessorErrorOrigin, ProcessorHealth,
    QueueError, StartFrame,
};
use async_trait::async_trait;
use std::future::Future;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{message}")]
pub struct ProcessorError {
    pub message: String,
    pub category: ErrorCategory,
    pub fatal: bool,
    /// Local classification override; use constructors/builders rather than
    /// struct literals. None uses the error category, false preserves health.
    pub permanence_override: Option<bool>,
}
impl ProcessorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            category: ErrorCategory::Unknown,
            fatal: false,
            permanence_override: None,
        }
    }
    pub fn fatal(message: impl Into<String>) -> Self {
        Self {
            fatal: true,
            ..Self::new(message)
        }
    }
    /// Override health classification without changing the reported category
    /// or fatal behavior. False never restores an already unusable processor.
    pub fn with_permanence_override(mut self, permanent: bool) -> Self {
        self.permanence_override = Some(permanent);
        self
    }
    pub fn with_category(mut self, category: ErrorCategory) -> Self {
        self.category = category;
        self
    }
}
impl From<QueueError> for ProcessorError {
    fn from(error: QueueError) -> Self {
        Self::fatal(error.to_string())
    }
}

/// A system-frame call may run concurrently with one normal-frame call. Avoid
/// holding state locks across slow I/O. Futures dropped by cancellation must
/// leave processor state usable; launch long-running work through `spawn`.
#[async_trait]
pub trait FrameProcessor: Send + Sync {
    fn name(&self) -> &str;
    /// Supply a stable shared handle for pre-start health or external recovery.
    /// By default each runtime creates a usable handle for this processor.
    fn health(&self) -> Option<ProcessorHealth> {
        None
    }
    /// Plain processors continue receiving frames while unusable, matching the
    /// source base processor. Services may opt into dropping only their work
    /// frames. This hook is never applied to System, Start, End, Stop or Cancel.
    /// It must be fast and nonblocking. Dropped frames are counted, not replayed.
    fn process_when_unusable(&self, _frame: &Frame, _direction: Direction) -> bool {
        true
    }
    async fn setup(
        &self,
        _config: &StartFrame,
        _ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError>;
    /// Cancel lifecycle timers and flush processor-owned state before the
    /// runtime drains tracked work for End or Stop.
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        Ok(())
    }
}

tokio::task_local! {
    pub(crate) static BACKGROUND_INTERRUPTIBLE: bool;
}

#[derive(Clone)]
pub(crate) struct ForwardingOrigin {
    runtime: Arc<Runtime>,
    index: usize,
    generation: u64,
}

/// A non-owning snapshot of an invocation and every inherited causal origin.
/// It is independent of uninterruptible-frame and background-task survival.
#[derive(Clone)]
pub struct InvocationFence {
    origins: Vec<(std::sync::Weak<Runtime>, usize, Option<u64>)>,
}
impl std::fmt::Debug for InvocationFence {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InvocationFence")
            .field("origins", &self.origins.len())
            .finish()
    }
}
impl InvocationFence {
    /// Require both original scopes, without resampling or retaining runtimes.
    /// Exact duplicate origins count once; distinct generations remain separate
    /// restrictions. The result is bounded to 64 weak origins and uses the same
    /// single canonical admission-lock order as either input.
    pub fn intersection(&self, other: &Self) -> Result<Self, ProcessorError> {
        let mut origins = Vec::new();
        for origin in self.origins.iter().chain(&other.origins) {
            if origins.iter().any(
                |existing: &(std::sync::Weak<Runtime>, usize, Option<u64>)| {
                    std::sync::Weak::ptr_eq(&existing.0, &origin.0)
                        && existing.1 == origin.1
                        && existing.2 == origin.2
                },
            ) {
                continue;
            }
            if origins.len() == 64 {
                return Err(ProcessorError::new(
                    "invocation fence intersection exceeds 64 origins",
                ));
            }
            origins.push(origin.clone());
        }
        Ok(Self { origins })
    }
    pub(crate) fn same_scope(&self, other: &Self) -> bool {
        self.origins.len() == other.origins.len()
            && self
                .origins
                .iter()
                .zip(&other.origins)
                .all(|(left, right)| {
                    std::sync::Weak::ptr_eq(&left.0, &right.0)
                        && left.1 == right.1
                        && left.2 == right.2
                })
    }
    /// Observes freshness; use `with_current` for synchronous local admission.
    pub fn is_current(&self) -> bool {
        self.with_current(|| ()).is_some()
    }
    /// Locks all source runtime admission gates in their canonical order. This
    /// fences local enqueue only; it cannot make a remote write atomic.
    /// The closure must not call routing, spawning, or other runtime-admission
    /// APIs, which would reenter the same non-reentrant locks.
    pub fn with_current<T>(&self, operation: impl FnOnce() -> T) -> Option<T> {
        let origins: Option<Vec<_>> = self
            .origins
            .iter()
            .map(|(runtime, index, epoch)| {
                runtime.upgrade().map(|runtime| (runtime, *index, *epoch))
            })
            .collect();
        let origins = origins?;
        let mut runtimes: Vec<_> = origins.iter().map(|(runtime, _, _)| runtime).collect();
        runtimes.sort_unstable_by_key(|runtime| Arc::as_ptr(&runtime.admission) as usize);
        runtimes.dedup_by_key(|runtime| Arc::as_ptr(&runtime.admission) as usize);
        let _guards: Vec<_> = runtimes
            .iter()
            .map(|runtime| runtime.admission.lock().unwrap())
            .collect();
        if origins.iter().any(|(runtime, index, epoch)| {
            runtime.is_finished()
                || epoch.is_some_and(|epoch| runtime.queues[*index].generation() > epoch)
        }) {
            return None;
        }
        Some(operation())
    }
}

impl ForwardingOrigin {
    pub(crate) fn is_stale(&self) -> bool {
        self.runtime.queues[self.index].generation() > self.generation
    }
}

pub struct ProcessorContext {
    pub(crate) runtime: Arc<Runtime>,
    pub(crate) node: Arc<NodeState>,
    pub(crate) index: usize,
    pub(crate) name: String,
    pub(crate) generation: Option<AtomicU64>,
    pub(crate) system_frame_id: Option<u64>,
    pub(crate) origins: Vec<ForwardingOrigin>,
}
impl Clone for ProcessorContext {
    fn clone(&self) -> Self {
        Self {
            runtime: self.runtime.clone(),
            node: self.node.clone(),
            index: self.index,
            name: self.name.clone(),
            system_frame_id: self.system_frame_id,
            origins: self.origins.clone(),
            generation: self
                .generation
                .as_ref()
                .map(|epoch| AtomicU64::new(epoch.load(Ordering::SeqCst))),
        }
    }
}
impl ProcessorContext {
    /// Capture the raw invocation, including inherited origins even when its
    /// own generation is absent. The fence retains no runtime or processor.
    pub fn invocation_fence(&self) -> Result<InvocationFence, ProcessorError> {
        if self.origins.len() >= 64 {
            return Err(ProcessorError::new("invocation fence exceeds 64 origins"));
        }
        let mut origins = vec![(
            Arc::downgrade(&self.runtime),
            self.index,
            self.current_generation(),
        )];
        origins.extend(self.origins.iter().map(|origin| {
            (
                Arc::downgrade(&origin.runtime),
                origin.index,
                Some(origin.generation),
            )
        }));
        Ok(InvocationFence { origins })
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    /// Stable local runtime/index identity, independent of display names.
    pub fn processor_identity(&self) -> crate::ObservedProcessor {
        self.runtime.processor_info[self.index].clone()
    }
    pub fn health(&self) -> ProcessorHealth {
        self.node.health.clone()
    }
    pub fn is_usable(&self) -> bool {
        self.node.health.is_usable()
    }
    pub fn set_usable(&self, is_usable: bool) -> bool {
        self.node.health.set_usable(is_usable)
    }
    /// Whether this invocation's causal generation still matches its runtime
    /// and inherited origins. This is an observation, not an admission lease;
    /// it does not change system-frame or uninterruptible survival semantics.
    pub fn is_current(&self) -> bool {
        !self.source_is_stale(self.emission_generation())
    }
    /// Compare captured causal scopes without applying a background task's
    /// interruption-survival override. This compares destination runtime/index,
    /// stored invocation generation, and the complete ordered inherited-origin
    /// list (runtime/index/generation). It does not check freshness or grant an
    /// admission lease; publication still needs its normal generation checks.
    /// Buffering processors can use this to avoid combining unrelated scopes.
    pub fn same_causal_scope(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.runtime, &other.runtime)
            && self.index == other.index
            && self.current_generation() == other.current_generation()
            && self.origins.len() == other.origins.len()
            && self
                .origins
                .iter()
                .zip(&other.origins)
                .all(|(left, right)| {
                    Arc::ptr_eq(&left.runtime, &right.runtime)
                        && left.index == right.index
                        && left.generation == right.generation
                })
    }
    pub fn configuration(&self) -> &StartFrame {
        &self.runtime.config.start
    }
    /// Share the observer's monotonic timeline with explicitly driven service
    /// metrics. Nested runtimes inherit the same clock as their parent.
    pub fn observer_clock(&self) -> Arc<dyn crate::ObserverClock> {
        self.runtime.observers.clock.clone()
    }
    /// Use this processor as the destination while preserving the source
    /// invocation's causal epoch. Buffered relays remain invalidatable at both
    /// their source and destination, including across nested pipelines.
    pub fn forwarding_context(&self, source: &ProcessorContext) -> ProcessorContext {
        let generation = source.emission_generation();
        self.forwarding_context_at(source, generation)
    }
    fn forwarding_context_at(
        &self,
        source: &ProcessorContext,
        generation: Option<u64>,
    ) -> ProcessorContext {
        let mut context = self.clone();
        context.generation = generation.map(AtomicU64::new);
        context.system_frame_id = source.system_frame_id;
        context.origins = source.origins.clone();
        if let Some(generation) = generation {
            let origin = ForwardingOrigin {
                runtime: source.runtime.clone(),
                index: source.index,
                generation,
            };
            if !(Arc::ptr_eq(&context.runtime, &origin.runtime) && context.index == origin.index)
                && !context.origins.iter().any(|existing| {
                    Arc::ptr_eq(&existing.runtime, &origin.runtime)
                        && existing.index == origin.index
                        && existing.generation == origin.generation
                })
            {
                context.origins.push(origin);
            }
        }
        context
    }
    pub(crate) fn with_admission<T>(
        &self,
        destination: &Arc<Runtime>,
        operation: impl FnOnce() -> T,
    ) -> T {
        if self.origins.is_empty() && Arc::ptr_eq(&self.runtime, destination) {
            let _admission = self.runtime.admission.lock().unwrap();
            return operation();
        }
        let mut runtimes = vec![&self.runtime, destination];
        runtimes.extend(self.origins.iter().map(|origin| &origin.runtime));
        runtimes.sort_unstable_by_key(|runtime| Arc::as_ptr(&runtime.admission) as usize);
        runtimes.dedup_by_key(|runtime| Arc::as_ptr(&runtime.admission) as usize);
        let _guards: Vec<_> = runtimes
            .iter()
            .map(|runtime| runtime.admission.lock().unwrap())
            .collect();
        operation()
    }
    pub(crate) fn source_is_stale(&self, generation: Option<u64>) -> bool {
        generation.is_some_and(|epoch| {
            self.runtime.queues[self.index].generation() > epoch
                || self.origins.iter().any(|origin| {
                    origin.runtime.queues[origin.index].generation() > origin.generation
                })
        })
    }
    /// Forward to an adjacent processor, or to the worker's output at a
    /// boundary. Internal emission fails explicitly on a full lane instead of
    /// awaiting capacity, which would deadlock cyclic bidirectional flows.
    pub async fn push(&self, frame: Frame, direction: Direction) -> Result<(), ProcessorError> {
        self.try_push(frame, direction)
    }
    /// Bind and enqueue one sealed realtime context under the same ordered
    /// admission gates. Only a never-bound assistant request selected from a
    /// validated interruption-surviving native terminal captures the current
    /// local generation. Inherited origins remain exact; existing fences never
    /// renew. This admits a new request, not a new tool result or remote ack.
    pub fn try_push_realtime_context(
        &self,
        frame: Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        let cause = frame.native_realtime_context_cause()?.ok_or_else(|| {
            ProcessorError::new("realtime context admission requires a sealed cause")
        })?;
        cause.validate_direction(direction)?;
        let new_native_request = cause.schedules_surviving_native_terminal();
        self.with_admission(&self.runtime, || {
            if self.runtime.is_finished() {
                return Err(ProcessorError::new("pipeline is closed"));
            }
            frame.bind_realtime_context_fence(|| {
                let mut fence = self.invocation_fence()?;
                if new_native_request {
                    // This is the new assistant request's local admission
                    // boundary, never a refresh when a provider retries it.
                    fence.origins[0].2 = Some(self.runtime.queues[self.index].generation());
                }
                Ok(fence)
            })?;
            self.runtime
                .route_unlocked(self.index, self.frame_generation(&frame), frame, direction)
        })
    }
    /// Admit provider output under its original non-owning causal fence and
    /// this receiver's own raw scope. Locks are ordered once, never reentered.
    /// This is only a local queue-admission guarantee, not remote cancellation.
    pub fn try_push_with_invocation_fence(
        &self,
        frame: Frame,
        direction: Direction,
        fence: &InvocationFence,
    ) -> Result<(), ProcessorError> {
        let origins: Option<Vec<_>> = fence
            .origins
            .iter()
            .map(|(runtime, index, epoch)| {
                runtime.upgrade().map(|runtime| (runtime, *index, *epoch))
            })
            .collect();
        let Some(mut origins) = origins else {
            self.runtime.drop_frame();
            return Ok(());
        };
        origins.push((self.runtime.clone(), self.index, self.current_generation()));
        origins.extend(self.origins.iter().map(|origin| {
            (
                origin.runtime.clone(),
                origin.index,
                Some(origin.generation),
            )
        }));
        let mut runtimes: Vec<_> = origins.iter().map(|(runtime, _, _)| runtime).collect();
        runtimes.sort_unstable_by_key(|runtime| Arc::as_ptr(&runtime.admission) as usize);
        runtimes.dedup_by_key(|runtime| Arc::as_ptr(&runtime.admission) as usize);
        let _guards: Vec<_> = runtimes
            .iter()
            .map(|runtime| runtime.admission.lock().unwrap())
            .collect();
        if origins.iter().any(|(runtime, index, epoch)| {
            runtime.is_finished()
                || epoch.is_some_and(|epoch| runtime.queues[*index].generation() > epoch)
        }) {
            self.runtime.drop_frame();
            return Ok(());
        }
        self.runtime
            .route_unlocked(self.index, self.current_generation(), frame, direction)
    }
    /// Synchronous, nonblocking equivalent of `push`, with identical causal
    /// admission and backpressure behavior. Useful when a producer's state
    /// transition and enqueue must be serialized without holding a lock across
    /// an await. Observer delivery remains on its owned dispatch tasks.
    pub fn try_push(&self, frame: Frame, direction: Direction) -> Result<(), ProcessorError> {
        self.with_admission(&self.runtime, || {
            let generation = self.frame_generation(&frame);
            if frame.interruptible && self.source_is_stale(generation) {
                self.runtime.drop_frame();
                return Ok(());
            }
            self.runtime
                .route_unlocked(self.index, generation, frame, direction)
        })
    }
    /// Emit a derived frame here with a retained invocation's exact causal
    /// scope. Background task survival never removes this fence. Unlike normal
    /// `push`, even a retained system invocation is fenced; an explicitly
    /// uninterruptible frame still survives. Enqueue is nonblocking.
    pub fn try_push_from_invocation(
        &self,
        frame: Frame,
        direction: Direction,
        source: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let generation = source.current_generation();
        let context = self.forwarding_context_at(source, generation);
        context.with_admission(&context.runtime, || {
            let stale = generation
                .is_some_and(|epoch| context.runtime.queues[context.index].generation() > epoch)
                || context.origins.iter().any(|origin| {
                    origin.runtime.queues[origin.index].generation() > origin.generation
                });
            if frame.interruptible && stale {
                context.runtime.drop_frame();
                return Ok(());
            }
            context
                .runtime
                .route_unlocked(context.index, generation, frame, direction)
        })
    }
    /// Atomically fence output against this invocation and inherited origins,
    /// including a system frame forwarding its original identity. Ordinary
    /// `push` intentionally preserves that system-frame survival exception.
    /// Uninterruptible frames and background tasks retain their explicit
    /// survival semantics.
    pub async fn push_with_causal_fence(
        &self,
        frame: Frame,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        self.with_admission(&self.runtime, || {
            let generation = self.emission_generation();
            if frame.interruptible && self.source_is_stale(generation) {
                self.runtime.drop_frame();
                return Ok(());
            }
            self.runtime
                .route_unlocked(self.index, generation, frame, direction)
        })
    }
    pub async fn broadcast(&self, frame: Frame) -> Result<(), ProcessorError> {
        let (down, up) = frame.broadcast_pair();
        self.push(down, Direction::Downstream).await?;
        self.push(up, Direction::Upstream).await
    }
    /// Interrupt this processor as well as both adjacent pipeline branches.
    pub async fn broadcast_interruption(&self) -> Result<(), ProcessorError> {
        self.broadcast_interruption_with_metadata(Default::default())
            .await
    }
    /// Preserve coordination metadata on both interruption branches while
    /// invalidating this processor's own work before either branch is emitted.
    pub async fn broadcast_interruption_with_metadata(
        &self,
        metadata: std::collections::BTreeMap<String, serde_json::Value>,
    ) -> Result<(), ProcessorError> {
        let mut frame = Frame::interruption();
        frame.metadata = metadata;
        self.broadcast_interruption_frame(&frame).await.map(|_| ())
    }
    /// Broadcast one causal interruption, allowing nested owners to admit the
    /// same frame into child runtimes without advancing independent epochs.
    /// Returns false if this invocation or interruption is already stale.
    pub async fn broadcast_interruption_frame(
        &self,
        frame: &Frame,
    ) -> Result<bool, ProcessorError> {
        if !matches!(frame.data, FrameData::Interruption) {
            return Err(ProcessorError::new("expected an Interruption frame"));
        }
        let (down, up) = frame.broadcast_pair();
        let identity = down.id.min(up.id);
        let epoch = self.with_admission(&self.runtime, || {
            let generation = self.emission_generation();
            if self.source_is_stale(generation)
                || down.interruption_epoch.as_ref().is_some_and(|token| {
                    let epoch = token.load(Ordering::SeqCst);
                    epoch != 0 && epoch <= self.runtime.queues[self.index].generation()
                })
            {
                return None;
            }
            self.runtime.interrupt_unlocked(
                self.index,
                generation,
                identity,
                down.interruption_epoch.as_deref(),
            )
        });
        let Some(epoch) = epoch else {
            return Ok(false);
        };
        if let Some(generation) = &self.generation {
            generation.store(epoch, Ordering::SeqCst);
        }
        self.push(down, Direction::Downstream).await?;
        self.push(up, Direction::Upstream).await?;
        Ok(true)
    }
    pub async fn push_error(&self, error: ErrorFrame) -> Result<(), ProcessorError> {
        self.push_error_with_permanence(error, false).await
    }
    /// Report exhausted recovery without inventing a permanent category.
    /// Fatal remains independent: it fails the worker under every policy.
    pub async fn push_error_with_permanence(
        &self,
        error: ErrorFrame,
        force_treat_as_permanent: bool,
    ) -> Result<(), ProcessorError> {
        self.push_error_with_permanence_override(error, force_treat_as_permanent.then_some(true))
            .await
    }
    /// Override category-based permanence locally. None uses classification;
    /// false preserves current health, true marks unusable. Fatal is independent.
    pub async fn push_error_with_permanence_override(
        &self,
        mut error: ErrorFrame,
        permanence_override: Option<bool>,
    ) -> Result<(), ProcessorError> {
        if self.runtime.is_finished() {
            return Err(ProcessorError::new("pipeline is closed"));
        }
        if error.processor.is_none() {
            error.processor = Some(self.name.clone());
        }
        let health = self.health();
        let reported_health =
            if permanence_override.unwrap_or_else(|| error.category.is_permanent()) {
                health.transition(false).1
            } else {
                health.snapshot()
            };
        self.push_error_verdict(error, health, reported_health)
            .await
    }
    /// Report an admitted provider outcome only while its health generation is
    /// current. Explicit recovery also fences outcomes when usability stayed true.
    pub async fn push_error_if_health_generation(
        &self,
        mut error: ErrorFrame,
        force_treat_as_permanent: bool,
        generation: crate::ProcessorHealthGeneration,
    ) -> Result<bool, ProcessorError> {
        if self.runtime.is_finished() {
            return Err(ProcessorError::new("pipeline is closed"));
        }
        if !self.is_current() {
            return Ok(false);
        }
        if error.processor.is_none() {
            error.processor = Some(self.name.clone());
        }
        let health = self.health();
        let Some(reported_health) = health.verdict_if_generation(
            generation,
            force_treat_as_permanent || error.category.is_permanent(),
        ) else {
            return Ok(false);
        };
        self.push_error_verdict(error, health, reported_health)
            .await?;
        Ok(true)
    }
    async fn push_error_verdict(
        &self,
        error: ErrorFrame,
        health: ProcessorHealth,
        reported_health: crate::ProcessorHealthSnapshot,
    ) -> Result<(), ProcessorError> {
        let mut frame = Frame::new(FrameData::Error(error.clone()));
        frame.error_origin = Some(Arc::new(ProcessorErrorOrigin {
            processor: self.runtime.processor_info[self.index].clone(),
            health,
            reported_health,
        }));
        self.runtime.record_frame_error(&mut frame);
        let result = self.push(frame, Direction::Upstream).await;
        if error.fatal {
            self.runtime
                .fail(crate::PipelineError::Execution(error.error));
        }
        result
    }
    /// Enqueue at this processor without blocking the processor's own queue.
    pub async fn queue(&self, frame: Frame, direction: Direction) -> Result<(), ProcessorError> {
        self.with_admission(&self.runtime, || {
            let generation = self.frame_generation(&frame);
            if frame.interruptible && self.source_is_stale(generation) {
                self.runtime.drop_frame();
                return Ok(());
            }
            self.runtime
                .queue_from_unlocked(self.index, generation, frame, direction)
        })
    }
    /// Request cancellation of a tracked task. End/cleanup still joins it.
    /// Returns false if the task has already completed or its ID is unknown.
    pub fn cancel_task(&self, id: u64) -> bool {
        self.node.cancel_task(id)
    }
    /// Start tracked asynchronous work. End drains these tasks; interruption
    /// cancels only opted-in tasks; Cancel and cleanup cancel all tasks.
    pub fn spawn<F>(
        &self,
        name: impl Into<String>,
        cancel_on_interruption: bool,
        future: F,
    ) -> Result<u64, ProcessorError>
    where
        F: Future<Output = Result<(), ProcessorError>> + Send + 'static,
    {
        self.with_admission(&self.runtime, || {
            let generation = self.emission_generation();
            if cancel_on_interruption && self.source_is_stale(generation) {
                return Ok(0);
            }
            self.node
                .spawn(name.into(), cancel_on_interruption, generation, future)
        })
    }
    pub(crate) fn frame_generation(&self, frame: &Frame) -> Option<u64> {
        if frame.is_system() && self.system_frame_id == Some(frame.id) {
            None
        } else {
            self.emission_generation()
        }
    }
    pub(crate) fn current_generation(&self) -> Option<u64> {
        self.generation
            .as_ref()
            .map(|epoch| epoch.load(Ordering::SeqCst))
    }
    fn emission_generation(&self) -> Option<u64> {
        if BACKGROUND_INTERRUPTIBLE
            .try_with(|interruptible| !*interruptible)
            .unwrap_or(false)
        {
            None
        } else {
            self.generation
                .as_ref()
                .map(|epoch| epoch.load(Ordering::SeqCst))
        }
    }
}

#[derive(Debug, Default)]
pub struct IdentityProcessor;
#[async_trait]
impl FrameProcessor for IdentityProcessor {
    fn name(&self) -> &str {
        "IdentityProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}

#[cfg(test)]
mod causal_scope_tests;
