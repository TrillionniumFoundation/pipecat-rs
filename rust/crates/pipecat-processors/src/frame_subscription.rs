//! Single-use, bounded native Text/PCM subscriptions between independent pipelines.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use futures_util::FutureExt;
use pipecat_core::{
    async_trait, Direction, FencedQueueOutcome, Frame, FrameData, FrameProcessor, InvocationFence,
    LocalFrameTag, ProcessorContext, ProcessorError, StartFrame,
};
use serde_json::Value;
use std::{
    collections::BTreeMap,
    future::Future,
    mem::{align_of, size_of},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard, OnceLock, Weak},
    time::Duration,
};
use tokio::sync::{watch, Notify};

/// A borrowed, read-only predicate. Its future cannot outlive the input frame.
/// Callbacks must cooperate with cancellation, never block the executor or detach
/// work. Construction and polling are panic-contained and occur outside locks.
pub type AsyncFramePredicate = Arc<
    dyn for<'a> Fn(
            &'a Frame,
        )
            -> Pin<Box<dyn Future<Output = Result<bool, ProcessorError>> + Send + 'a>>
        + Send
        + Sync,
>;
/// Each subscriber gets an independent owned native clone before this callback.
/// `None` is not an output/drop protocol; return a supported frame or an error.
pub type FrameTransformer = Arc<
    dyn Fn(Frame) -> Pin<Box<dyn Future<Output = Result<Frame, ProcessorError>> + Send>>
        + Send
        + Sync,
>;

/// Limits for one producer and each of its consumer subscriptions.
#[derive(Clone, Debug)]
pub struct FrameSubscriptionConfig {
    pub max_consumers: usize,
    /// Includes the single worker's in-flight frame until target admission/drop.
    pub max_pending_frames: usize,
    /// Bridge-owned frame/envelope capacity, including the fixed queue slots.
    /// Callback temporary allocations, allocator bookkeeping, shared observer
    /// identity bookkeeping, and downstream allocations are not a whole-heap bound.
    pub max_pending_bytes: usize,
    /// Per-frame retained capacity, not serialized length. Includes envelopes and
    /// conservative bounded scope/local-provenance storage.
    pub max_frame_bytes: usize,
    pub max_json_depth: usize,
    pub max_json_nodes: usize,
    /// Cooperative deadline, positive and at most sixty seconds.
    pub callback_timeout: Duration,
}
impl Default for FrameSubscriptionConfig {
    fn default() -> Self {
        Self {
            max_consumers: 16,
            max_pending_frames: 32,
            max_pending_bytes: 8 * 1024 * 1024,
            max_frame_bytes: 1024 * 1024,
            max_json_depth: 32,
            max_json_nodes: 8192,
            callback_timeout: Duration::from_secs(10),
        }
    }
}
impl FrameSubscriptionConfig {
    fn validate(&self) -> Result<(), ProcessorError> {
        if !supported_json_storage() {
            return Err(error("unsupported serde_json storage profile"));
        }
        if self.max_consumers == 0
            || self.max_consumers > 256
            || self.max_pending_frames == 0
            || self.max_pending_frames > 4096
            || self.max_frame_bytes == 0
            || self.max_frame_bytes > 64 * 1024 * 1024
            || self.max_pending_bytes == 0
            || self.max_pending_bytes > 256 * 1024 * 1024
            || self.queue_bytes() >= self.max_pending_bytes
            || self.max_json_depth == 0
            || self.max_json_depth > 64
            || self.max_json_nodes == 0
            || self.max_json_nodes > 65536
            || self.callback_timeout.is_zero()
            || self.callback_timeout > Duration::from_secs(60)
        {
            return Err(error("invalid configuration"));
        }
        Ok(())
    }
    fn queue_bytes(&self) -> usize {
        self.max_pending_frames * size_of::<Option<Envelope>>()
    }
}

// Audited against the workspace's locked serde_json 1.0 implementation: its
// default Map is BTreeMap, while preserve_order substitutes IndexMap, whose
// retained capacity the public Map API cannot inspect. Arbitrary-precision
// Numbers similarly hide owned strings. Reject these feature-unified profiles
// rather than treating their lengths as retained capacity. This is deliberately
// not a claim about arbitrary future serde_json storage implementations.
fn supported_json_storage() -> bool {
    static SUPPORTED: OnceLock<bool> = OnceLock::new();
    *SUPPORTED.get_or_init(|| {
        let mut probe = serde_json::Map::new();
        probe.insert("z".into(), Value::Null);
        probe.insert("a".into(), Value::Null);
        probe.keys().next().is_some_and(|key| key == "a")
            && size_of::<serde_json::Map<String, Value>>() == size_of::<BTreeMap<String, Value>>()
            && align_of::<serde_json::Map<String, Value>>() == align_of::<BTreeMap<String, Value>>()
            && serde_json::from_str::<Value>("1e400").is_err()
            && size_of::<serde_json::Number>() <= 16
    })
}

/// Content-free aggregate diagnostics. Buffer admission is not delivery.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ProducerSubscriptionSnapshot {
    pub started: bool,
    pub closed: bool,
    pub consumers: usize,
    pub matched: u64,
    pub buffer_admitted: u64,
    pub stale: u64,
    pub failed: u64,
}
/// Counts survive closure. `target_queued` does not mean target processing,
/// external delivery, or effects retractable by later source cancellation.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ConsumerSubscriptionSnapshot {
    pub registered: bool,
    pub closed: bool,
    pub worker_running: bool,
    pub pending_frames: usize,
    pub pending_bytes: usize,
    pub buffer_admitted: u64,
    pub target_queued: u64,
    pub stale: u64,
    pub failed: u64,
    pub discarded_on_close: u64,
}
fn error(message: &str) -> ProcessorError {
    ProcessorError::new(format!("frame subscription: {message}"))
}
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(|poison| poison.into_inner())
}
fn terminal(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
    )
}
fn identity() -> FrameTransformer {
    Arc::new(|frame| Box::pin(async move { Ok(frame) }))
}

struct RegistryState {
    used: bool,
    next_id: u64,
    subscribers: Vec<(u64, Weak<Binding>)>,
    snapshot: ProducerSubscriptionSnapshot,
}
struct Registry {
    config: FrameSubscriptionConfig,
    state: Mutex<RegistryState>,
    cancel: watch::Sender<bool>,
}
impl Registry {
    fn close(&self) -> Vec<Arc<Binding>> {
        let subscribers = {
            let mut state = lock(&self.state);
            state.snapshot.closed = true;
            state.snapshot.consumers = 0;
            std::mem::take(&mut state.subscribers)
        };
        self.cancel.send_replace(true);
        let bindings: Vec<_> = subscribers
            .into_iter()
            .filter_map(|(_, binding)| binding.upgrade())
            .collect();
        for binding in &bindings {
            binding.close();
        }
        bindings
    }
    fn unregister(&self, id: u64) {
        let mut state = lock(&self.state);
        state.subscribers.retain(|(existing, _)| *existing != id);
        state.snapshot.consumers = state.subscribers.len();
    }
    fn destinations(&self) -> Result<Vec<Destination>, ProcessorError> {
        let bindings: Vec<_> = {
            let state = lock(&self.state);
            if !state.snapshot.started || state.snapshot.closed {
                return Err(error("producer is not running"));
            }
            state
                .subscribers
                .iter()
                .filter_map(|(_, b)| b.upgrade())
                .collect()
        };
        bindings
            .into_iter()
            .filter_map(|binding| {
                let ctx = lock(&binding.state).destination.clone();
                ctx.map(|ctx| {
                    let fence = ctx.invocation_fence()?;
                    Ok(Destination {
                        binding,
                        ctx,
                        fence,
                    })
                })
            })
            .collect()
    }
}
/// An inert registration handle. Registration occurs only on a consumer's Start
/// after this producer has completed setup and received its own Start.
#[derive(Clone)]
pub struct FrameSubscriptionProducer {
    registry: Arc<Registry>,
}
impl FrameSubscriptionProducer {
    pub fn snapshot(&self) -> ProducerSubscriptionSnapshot {
        lock(&self.registry.state).snapshot
    }
}
struct Destination {
    binding: Arc<Binding>,
    ctx: ProcessorContext,
    fence: InvocationFence,
}

/// A safe native profile of Python ProducerProcessor. Lifecycle frames always
/// pass locally and bypass callbacks. Other unmatched frames retain identity and
/// direction, even when passthrough is false. Matched unsupported payloads fail.
/// Fanout uses a stable registration-order snapshot captured before the predicate;
/// each transform gets a fresh owned clone. A failure preserves the already
/// admitted prefix and prevents the original passthrough for that invocation.
/// End/Stop/Cancel/cleanup permanently retire this object; use new objects for a
/// new run. Closing A only closes subscriptions, never sends controls into B.
pub struct ProducerProcessor {
    registry: Arc<Registry>,
    filter: AsyncFramePredicate,
    transformer: FrameTransformer,
    passthrough: bool,
}
impl ProducerProcessor {
    pub fn new(
        config: FrameSubscriptionConfig,
        filter: AsyncFramePredicate,
        transformer: Option<FrameTransformer>,
        passthrough: bool,
    ) -> Result<Self, ProcessorError> {
        config.validate()?;
        Ok(Self {
            registry: Arc::new(Registry {
                config,
                state: Mutex::new(RegistryState {
                    used: false,
                    next_id: 0,
                    subscribers: Vec::new(),
                    snapshot: ProducerSubscriptionSnapshot::default(),
                }),
                cancel: watch::channel(false).0,
            }),
            filter,
            transformer: transformer.unwrap_or_else(identity),
            passthrough,
        })
    }
    pub fn subscribe_handle(&self) -> FrameSubscriptionProducer {
        FrameSubscriptionProducer {
            registry: self.registry.clone(),
        }
    }
    pub fn snapshot(&self) -> ProducerSubscriptionSnapshot {
        lock(&self.registry.state).snapshot
    }
    async fn close(&self) {
        let bindings = self.registry.close();
        for binding in bindings {
            binding.join().await;
        }
    }
    async fn produce(&self, frame: &Frame, ctx: &ProcessorContext) -> Result<bool, ProcessorError> {
        // Freeze source, ordered membership, and every destination raw scope
        // before constructing or polling any user callback.
        let source = ctx.invocation_fence()?;
        let destinations = self.registry.destinations()?;
        let mut cancel = self.registry.cancel.subscribe();
        let matches = callback(
            || (self.filter)(frame),
            self.registry.config.callback_timeout,
            &mut cancel,
        )
        .await?;
        if !matches {
            return Ok(false);
        }
        frame_bytes(frame, &self.registry.config)?;
        {
            let mut state = lock(&self.registry.state);
            state.snapshot.matched = state.snapshot.matched.saturating_add(1);
        }
        for destination in destinations {
            let frame = callback(
                || (self.transformer)(frame.clone()),
                self.registry.config.callback_timeout,
                &mut cancel,
            )
            .await?;
            let bytes = envelope_bytes(&frame, &destination.ctx, &self.registry.config)?;
            let combined = source.intersection(&destination.fence)?;
            let mut incoming = Some((frame, destination.ctx));
            // Only local queue admission inside the fence. No routing/spawn,
            // user callbacks or destruction of a raw context under its locks.
            let admitted = combined.with_current(|| {
                destination
                    .binding
                    .admit(&mut incoming, source.clone(), bytes)
            });
            drop(incoming);
            // Wake only after all runtime admission gates are released. Custom
            // synchronous wakers may reenter processor routing.
            if matches!(&admitted, Some(Ok(()))) {
                destination.binding.changed.notify_one();
            }
            let mut state = lock(&self.registry.state);
            match admitted {
                Some(Ok(())) => {
                    state.snapshot.buffer_admitted =
                        state.snapshot.buffer_admitted.saturating_add(1)
                }
                Some(Err(error)) => return Err(error),
                None => state.snapshot.stale = state.snapshot.stale.saturating_add(1),
            }
        }
        Ok(true)
    }
}
impl Drop for ProducerProcessor {
    fn drop(&mut self) {
        self.registry.close();
    }
}
#[async_trait]
impl FrameProcessor for ProducerProcessor {
    fn name(&self) -> &str {
        "ProducerProcessor"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut state = lock(&self.registry.state);
        if state.used || state.snapshot.closed {
            return Err(error("producer is single-use"));
        }
        state.used = true;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            {
                let mut state = lock(&self.registry.state);
                if !state.used || state.snapshot.closed {
                    return Err(error("producer is closed or not set up"));
                }
                state.snapshot.started = true;
            }
            return ctx.push(frame, direction).await;
        }
        if terminal(&frame.data) {
            self.close().await;
            return ctx.push(frame, direction).await;
        }
        if matches!(frame.data, FrameData::Interruption) {
            return ctx.push(frame, direction).await;
        }
        let matched = match self.produce(&frame, ctx).await {
            Ok(value) => value,
            Err(error) => {
                let mut state = lock(&self.registry.state);
                state.snapshot.failed = state.snapshot.failed.saturating_add(1);
                return Err(error);
            }
        };
        if !matched || self.passthrough {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.close().await;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close().await;
        Ok(())
    }
}

struct BindingState {
    destination: Option<ProcessorContext>,
    slots: Box<[Option<Envelope>]>,
    head: usize,
    len: usize,
    base_bytes: usize,
    snapshot: ConsumerSubscriptionSnapshot,
}
struct Binding {
    id: u64,
    registry: Weak<Registry>,
    config: FrameSubscriptionConfig,
    state: Mutex<BindingState>,
    changed: Notify,
    drained: Notify,
    cancel: watch::Sender<bool>,
}
struct Envelope {
    frame: Frame,
    source: InvocationFence,
    destination: ProcessorContext,
    permit: Permit,
}
struct Permit {
    binding: Arc<Binding>,
    bytes: usize,
}
impl Permit {
    fn resize(&mut self, bytes: usize) -> Result<(), ProcessorError> {
        let mut state = lock(&self.binding.state);
        let pending = state.snapshot.pending_bytes - self.bytes;
        let total = pending
            .checked_add(bytes)
            .ok_or_else(|| error("pending byte limit exceeded"))?;
        if total > self.binding.config.max_pending_bytes {
            return Err(error("pending byte limit exceeded"));
        }
        state.snapshot.pending_bytes = total;
        self.bytes = bytes;
        Ok(())
    }
}
impl Drop for Permit {
    fn drop(&mut self) {
        let mut state = lock(&self.binding.state);
        state.snapshot.pending_frames -= 1;
        state.snapshot.pending_bytes -= self.bytes;
    }
}
impl Binding {
    fn new(
        id: u64,
        registry: &Arc<Registry>,
        ctx: &ProcessorContext,
    ) -> Result<Arc<Self>, ProcessorError> {
        let base_bytes = binding_base_bytes(&registry.config, ctx)?;
        Ok(Arc::new(Self {
            id,
            registry: Arc::downgrade(registry),
            config: registry.config.clone(),
            state: Mutex::new(BindingState {
                destination: Some(ctx.clone()),
                slots: std::iter::repeat_with(|| None)
                    .take(registry.config.max_pending_frames)
                    .collect(),
                head: 0,
                len: 0,
                base_bytes,
                snapshot: ConsumerSubscriptionSnapshot {
                    registered: true,
                    worker_running: true,
                    pending_bytes: base_bytes,
                    ..Default::default()
                },
            }),
            changed: Notify::new(),
            drained: Notify::new(),
            cancel: watch::channel(false).0,
        }))
    }
    fn admit(
        self: &Arc<Self>,
        incoming: &mut Option<(Frame, ProcessorContext)>,
        source: InvocationFence,
        bytes: usize,
    ) -> Result<(), ProcessorError> {
        let mut state = lock(&self.state);
        if state.snapshot.closed {
            return Err(error("consumer is closed"));
        }
        if state.snapshot.pending_frames >= self.config.max_pending_frames {
            return Err(error("pending frame limit exceeded"));
        }
        let total = state
            .snapshot
            .pending_bytes
            .checked_add(bytes)
            .ok_or_else(|| error("pending byte limit exceeded"))?;
        if total > self.config.max_pending_bytes {
            return Err(error("pending byte limit exceeded"));
        }
        let (frame, destination) = incoming.take().expect("one subscription admission");
        let index = (state.head + state.len) % state.slots.len();
        debug_assert!(state.slots[index].is_none());
        state.slots[index] = Some(Envelope {
            frame,
            source,
            destination,
            permit: Permit {
                binding: self.clone(),
                bytes,
            },
        });
        state.len += 1;
        state.snapshot.pending_frames += 1;
        state.snapshot.pending_bytes = total;
        state.snapshot.buffer_admitted = state.snapshot.buffer_admitted.saturating_add(1);
        Ok(())
    }
    fn take(&self) -> Option<Envelope> {
        let mut state = lock(&self.state);
        if state.snapshot.closed || state.len == 0 {
            return None;
        }
        let index = state.head;
        let frame = state.slots[index].take();
        state.head = (state.head + 1) % state.slots.len();
        state.len -= 1;
        frame
    }
    fn close(&self) {
        let (removed, context) = {
            let mut state = lock(&self.state);
            state.snapshot.closed = true;
            state.snapshot.registered = false;
            let removed = std::mem::replace(&mut state.slots, Box::new([]));
            let base_bytes = std::mem::take(&mut state.base_bytes);
            state.snapshot.pending_bytes -= base_bytes;
            state.snapshot.discarded_on_close = state
                .snapshot
                .discarded_on_close
                .saturating_add(state.len as u64);
            state.head = 0;
            state.len = 0;
            (removed, state.destination.take())
        };
        if let Some(registry) = self.registry.upgrade() {
            registry.unregister(self.id);
        }
        self.cancel.send_replace(true);
        // Permits and contexts may own runtimes. Release after every local lock.
        drop(removed);
        drop(context);
    }
    async fn join(&self) {
        loop {
            let drained = self.drained.notified();
            tokio::pin!(drained);
            drained.as_mut().enable();
            if !lock(&self.state).snapshot.worker_running {
                return;
            }
            drained.await;
        }
    }
    fn refresh_destination(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let base_bytes = match binding_base_bytes(&self.config, ctx) {
            Ok(bytes) => bytes,
            Err(error) => {
                self.close();
                return Err(error);
            }
        };
        let mut fresh = Some(ctx.clone());
        let result = {
            let mut state = lock(&self.state);
            if state.snapshot.closed {
                Ok(())
            } else {
                let total = (state.snapshot.pending_bytes - state.base_bytes)
                    .checked_add(base_bytes)
                    .filter(|bytes| *bytes <= self.config.max_pending_bytes);
                if let Some(total) = total {
                    state.snapshot.pending_bytes = total;
                    state.base_bytes = base_bytes;
                    std::mem::swap(&mut state.destination, &mut fresh);
                    Ok(())
                } else {
                    Err(error("registration context exceeds pending byte limit"))
                }
            }
        };
        drop(fresh);
        if result.is_err() {
            self.close();
        }
        result
    }
}
// Captured before calling spawn: rejection and an unpolled future's cancellation
// both unregister, drain and release ownership, exactly as normal termination.
struct Owned(Arc<Binding>);
impl Drop for Owned {
    fn drop(&mut self) {
        self.0.close();
        lock(&self.0.state).snapshot.worker_running = false;
        self.0.drained.notify_waiters();
    }
}
struct ConsumerLifecycle {
    used: bool,
    closed: bool,
    binding: Option<Arc<Binding>>,
}
/// One consumer owns one serial worker. Ordinary local frames only pass through;
/// only subscribed frames call its transformer, then self-queue at the configured
/// direction. Local interruption changes future admission scopes, never buffered
/// envelopes. Consumer shutdown unregisters only itself. Callback/admission errors
/// close its subscription and remain visible in its snapshot and worker errors.
/// An already-selected synchronous admission can race closure until the worker
/// joins; closure is not rollback. After a completed close/join no worker output
/// remains, and target-queued work is never retracted by source closure.
pub struct ConsumerProcessor {
    producer: FrameSubscriptionProducer,
    transformer: FrameTransformer,
    direction: Direction,
    lifecycle: Mutex<ConsumerLifecycle>,
}
impl ConsumerProcessor {
    pub fn new(
        producer: FrameSubscriptionProducer,
        transformer: Option<FrameTransformer>,
        direction: Direction,
    ) -> Self {
        Self {
            producer,
            transformer: transformer.unwrap_or_else(identity),
            direction,
            lifecycle: Mutex::new(ConsumerLifecycle {
                used: false,
                closed: false,
                binding: None,
            }),
        }
    }
    pub fn snapshot(&self) -> ConsumerSubscriptionSnapshot {
        let lifecycle = lock(&self.lifecycle);
        lifecycle.binding.as_ref().map_or_else(
            || ConsumerSubscriptionSnapshot {
                closed: lifecycle.closed,
                ..Default::default()
            },
            |binding| lock(&binding.state).snapshot,
        )
    }
    fn start(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let binding = {
            let mut lifecycle = lock(&self.lifecycle);
            if !lifecycle.used || lifecycle.closed {
                return Err(error("consumer is closed or not set up"));
            }
            if lifecycle.binding.is_some() {
                return Ok(());
            }
            let registry = &self.producer.registry;
            let mut state = lock(&registry.state);
            if !state.snapshot.started || state.snapshot.closed {
                return Err(error(
                    "producer must be started before consumer registration",
                ));
            }
            if state.subscribers.len() >= registry.config.max_consumers {
                return Err(error("consumer limit exceeded"));
            }
            let id = state
                .next_id
                .checked_add(1)
                .ok_or_else(|| error("registration IDs exhausted"))?;
            state.next_id = id;
            let binding = Binding::new(id, registry, ctx)?;
            state.subscribers.push((id, Arc::downgrade(&binding)));
            state.snapshot.consumers = state.subscribers.len();
            lifecycle.binding = Some(binding.clone());
            binding
        };
        let owned = Owned(binding);
        let transformer = self.transformer.clone();
        let direction = self.direction;
        ctx.spawn("frame-subscription", false, async move {
            let owned = owned;
            let result = consume(&owned.0, transformer, direction).await;
            if result.is_err() {
                let mut state = lock(&owned.0.state);
                state.snapshot.failed = state.snapshot.failed.saturating_add(1);
            }
            result
        })
        .map(|_| ())
    }
    async fn close(&self) {
        let binding = {
            let mut lifecycle = lock(&self.lifecycle);
            lifecycle.closed = true;
            lifecycle.binding.clone()
        };
        if let Some(binding) = binding {
            binding.close();
            binding.join().await;
        }
    }
}
impl Drop for ConsumerProcessor {
    fn drop(&mut self) {
        let binding = { lock(&self.lifecycle).binding.clone() };
        if let Some(binding) = binding {
            binding.close();
        }
    }
}
#[async_trait]
impl FrameProcessor for ConsumerProcessor {
    fn name(&self) -> &str {
        "ConsumerProcessor"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut lifecycle = lock(&self.lifecycle);
        if lifecycle.used || lifecycle.closed {
            return Err(error("consumer is single-use"));
        }
        lifecycle.used = true;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            self.start(ctx)?;
        }
        if terminal(&frame.data) {
            self.close().await;
        }
        if matches!(frame.data, FrameData::Interruption) {
            let binding = lock(&self.lifecycle).binding.clone();
            if let Some(binding) = binding {
                binding.refresh_destination(ctx)?;
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.close().await;
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.close().await;
        Ok(())
    }
}
async fn consume(
    binding: &Arc<Binding>,
    transformer: FrameTransformer,
    direction: Direction,
) -> Result<(), ProcessorError> {
    let mut cancel = binding.cancel.subscribe();
    loop {
        if *cancel.borrow() {
            return Ok(());
        }
        let changed = binding.changed.notified();
        tokio::pin!(changed);
        changed.as_mut().enable();
        if let Some(envelope) = binding.take() {
            let Envelope {
                frame,
                source,
                destination,
                mut permit,
            } = envelope;
            // Both original scopes are tested again by the atomic core call.
            // This early check merely avoids expensive work on already-stale data.
            if !source.is_current() || !destination.invocation_fence()?.is_current() {
                let mut state = lock(&binding.state);
                state.snapshot.stale = state.snapshot.stale.saturating_add(1);
                continue;
            }
            let frame = match callback(
                || transformer(frame),
                binding.config.callback_timeout,
                &mut cancel,
            )
            .await
            {
                Err(_) if *cancel.borrow() => {
                    let mut state = lock(&binding.state);
                    state.snapshot.discarded_on_close =
                        state.snapshot.discarded_on_close.saturating_add(1);
                    return Ok(());
                }
                result => result?,
            };
            let bytes = envelope_bytes(&frame, &destination, &binding.config)?;
            permit.resize(bytes)?;
            // Never refresh either original scope after a callback.
            match destination.try_queue_with_invocation_fence(frame, direction, &source)? {
                FencedQueueOutcome::Queued => {
                    let mut state = lock(&binding.state);
                    state.snapshot.target_queued = state.snapshot.target_queued.saturating_add(1);
                }
                FencedQueueOutcome::Stale => {
                    let mut state = lock(&binding.state);
                    state.snapshot.stale = state.snapshot.stale.saturating_add(1);
                }
            }
            drop(permit);
            continue;
        }
        tokio::select! {
            biased;
            _ = cancel.changed() => return Ok(()),
            _ = &mut changed => {},
        }
    }
}
async fn callback<T, F, Fut>(
    make: F,
    timeout: Duration,
    cancel: &mut watch::Receiver<bool>,
) -> Result<T, ProcessorError>
where
    F: FnOnce() -> Fut,
    Fut: Future<Output = Result<T, ProcessorError>>,
{
    if *cancel.borrow() {
        return Err(error("subscription closed during callback"));
    }
    // Calling the closure inside this async body catches construction panics too.
    let guarded = std::panic::AssertUnwindSafe(async move { make().await }).catch_unwind();
    tokio::select! {
        biased;
        _ = cancel.changed() => Err(error("subscription closed during callback")),
        result = tokio::time::timeout(timeout, guarded) => match result {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err(error("callback panicked")),
            Err(_) => Err(error("callback timed out")),
        },
    }
}

// Count inspectable owned capacities without serialization or cloning. JSON maps
// currently use BTreeMap: charge a conservative full internal node per entry
// (including spare key/value and edge slots), never just serialized byte length.
// Private local-tag slots and bounded causal-scope vectors receive conservative
// maximum reservations. Shared observer/native identity allocation is excluded.
fn binding_base_bytes(
    config: &FrameSubscriptionConfig,
    ctx: &ProcessorContext,
) -> Result<usize, ProcessorError> {
    // Validate before retaining a cloned context: its private origin vector is
    // bounded by the same fence API and its name can be supplied by a wrapper.
    ctx.invocation_fence()?;
    if ctx.name().len() > config.max_frame_bytes {
        return Err(error("registration name exceeds frame capacity limit"));
    }
    config
        .queue_bytes()
        .checked_add(size_of::<ProcessorContext>() + 64 * 128)
        .and_then(|bytes| bytes.checked_add(ctx.name().len()))
        .filter(|bytes| *bytes <= config.max_pending_bytes)
        .ok_or_else(|| error("registration context exceeds pending byte limit"))
}

fn envelope_bytes(
    frame: &Frame,
    destination: &ProcessorContext,
    config: &FrameSubscriptionConfig,
) -> Result<usize, ProcessorError> {
    // ProcessorContext::clone copies its name into a fresh String. Wrappers can
    // supply arbitrary processor names, so do not hide this owned allocation
    // inside the fixed causal-scope reservation.
    frame_bytes(frame, config)?
        .checked_add(destination.name().len())
        .filter(|bytes| *bytes <= config.max_frame_bytes)
        .ok_or_else(|| error("frame envelope capacity limit exceeded"))
}

fn frame_bytes(frame: &Frame, config: &FrameSubscriptionConfig) -> Result<usize, ProcessorError> {
    frame
        .require_no_native_llm_continuation()
        .map_err(|_| error("native semantic authority is unsupported"))?;
    if frame.error_origin().is_some() || frame.has_pipeline_pts() {
        return Err(error(
            "error origins and pipeline-clock PTS are unsupported",
        ));
    }
    let mut counter = Counter {
        bytes: 0,
        nodes: 0,
        config,
    };
    counter.add(
        size_of::<Envelope>()
            + 2 * 64 * 128
            + LocalFrameTag::MAX_TAGS_PER_FRAME * size_of::<LocalFrameTag>(),
    )?;
    counter.string(&frame.name)?;
    counter.optional(&frame.transport_source)?;
    counter.optional(&frame.transport_destination)?;
    counter.map(frame.metadata.len())?;
    for (key, value) in &frame.metadata {
        counter.string(key)?;
        counter.json(value, 1)?;
    }
    match &frame.data {
        FrameData::Text(text)
        | FrameData::LlmText(text)
        | FrameData::InputText(text)
        | FrameData::VisionText(text) => counter.string(&text.text)?,
        FrameData::AggregatedText(text) | FrameData::TtsText(text) => {
            counter.string(&text.text.text)?;
            counter.string(&text.aggregated_by)?;
            counter.optional(&text.context_id)?;
            counter.optional(&text.raw_text)?;
        }
        FrameData::Transcription(text)
        | FrameData::InterimTranscription(text)
        | FrameData::EagerTranscription(text)
        | FrameData::Translation(text) => {
            counter.string(&text.text)?;
            counter.transcription(&text.user_id, &text.timestamp, &text.language, &text.result)?;
        }
        FrameData::TranscriptionDetailed(text)
        | FrameData::InterimTranscriptionDetailed(text)
        | FrameData::EagerTranscriptionDetailed(text)
        | FrameData::TranslationDetailed(text) => {
            counter.string(&text.text.text)?;
            counter.transcription(&text.user_id, &text.timestamp, &text.language, &text.result)?;
        }
        FrameData::InputAudio(audio)
        | FrameData::OutputAudio(audio)
        | FrameData::TtsAudio(audio)
        | FrameData::SpeechOutputAudio(audio) => counter.audio(audio)?,
        FrameData::UserAudio(audio) => {
            counter.audio(&audio.audio)?;
            counter.string(&audio.user_id)?;
        }
        _ => return Err(error("matched frame type is unsupported")),
    }
    Ok(counter.bytes)
}
struct Counter<'a> {
    bytes: usize,
    nodes: usize,
    config: &'a FrameSubscriptionConfig,
}
impl Counter<'_> {
    fn add(&mut self, bytes: usize) -> Result<(), ProcessorError> {
        self.bytes = self
            .bytes
            .checked_add(bytes)
            .filter(|bytes| *bytes <= self.config.max_frame_bytes)
            .ok_or_else(|| error("frame capacity limit exceeded"))?;
        Ok(())
    }
    fn string(&mut self, text: &String) -> Result<(), ProcessorError> {
        self.add(text.capacity())
    }
    fn optional(&mut self, text: &Option<String>) -> Result<(), ProcessorError> {
        if let Some(text) = text {
            self.string(text)?;
        }
        Ok(())
    }
    fn map(&mut self, entries: usize) -> Result<(), ProcessorError> {
        // Removing the final key can leave an empty root allocation. Reserve
        // one complete node even when a map currently has no entries.
        let bytes = entries
            .max(1)
            .checked_mul(16 * (size_of::<(String, Value)>() + size_of::<usize>()) + 64)
            .ok_or_else(|| error("JSON capacity limit exceeded"))?;
        self.add(bytes)
    }
    fn json(&mut self, value: &Value, depth: usize) -> Result<(), ProcessorError> {
        self.nodes += 1;
        if depth > self.config.max_json_depth || self.nodes > self.config.max_json_nodes {
            return Err(error("JSON depth or node limit exceeded"));
        }
        match value {
            Value::String(text) => self.string(text)?,
            Value::Array(values) => {
                self.add(
                    values
                        .capacity()
                        .checked_mul(size_of::<Value>())
                        .ok_or_else(|| error("JSON capacity limit exceeded"))?,
                )?;
                for value in values {
                    self.json(value, depth + 1)?;
                }
            }
            Value::Object(values) => {
                self.map(values.len())?;
                for (key, value) in values {
                    self.string(key)?;
                    self.json(value, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn transcription(
        &mut self,
        user: &String,
        timestamp: &String,
        language: &Option<String>,
        result: &Option<Value>,
    ) -> Result<(), ProcessorError> {
        self.string(user)?;
        self.string(timestamp)?;
        self.optional(language)?;
        if let Some(result) = result {
            self.json(result, 1)?;
        }
        Ok(())
    }
    fn audio(&mut self, audio: &pipecat_core::AudioRawFrame) -> Result<(), ProcessorError> {
        audio
            .validate()
            .map_err(|_| error("invalid PCM geometry"))?;
        self.add(audio.audio.capacity())?;
        self.optional(&audio.context_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn locked_json_storage_profile_is_accountable() {
        assert!(supported_json_storage());
        assert!(FrameSubscriptionConfig::default().validate().is_ok());
    }
    #[test]
    fn empty_maps_reserve_retained_root_capacity() {
        let config = FrameSubscriptionConfig::default();
        let mut empty = serde_json::Map::new();
        let mut counter = Counter {
            bytes: 0,
            nodes: 0,
            config: &config,
        };
        counter.json(&Value::Object(empty.clone()), 1).unwrap();
        let fresh = counter.bytes;
        empty.insert("removed".into(), Value::Null);
        empty.remove("removed");
        let mut counter = Counter {
            bytes: 0,
            nodes: 0,
            config: &config,
        };
        counter.json(&Value::Object(empty), 1).unwrap();
        assert!(fresh > 0);
        assert_eq!(counter.bytes, fresh);
        let mut metadata = Frame::text("empty metadata");
        let original = frame_bytes(&metadata, &config).unwrap();
        metadata.metadata.insert("removed".into(), Value::Null);
        metadata.metadata.remove("removed");
        assert_eq!(frame_bytes(&metadata, &config).unwrap(), original);
    }
}
