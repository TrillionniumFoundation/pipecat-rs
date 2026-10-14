//! Bounded, owned observation without running user callbacks on processor paths.
use crate::{async_trait, Direction, Frame, FrameData};
use std::collections::BTreeSet;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Nanoseconds on one monotonic timeline. Implementations must be fast and
/// nonblocking; wall-clock timestamps from frame payloads use a different epoch.
pub trait ObserverClock: Send + Sync {
    fn now_ns(&self) -> u64;
}
#[derive(Debug)]
pub struct MonotonicObserverClock(tokio::time::Instant);
impl Default for MonotonicObserverClock {
    fn default() -> Self {
        Self(tokio::time::Instant::now())
    }
}
impl ObserverClock for MonotonicObserverClock {
    fn now_ns(&self) -> u64 {
        self.0.elapsed().as_nanos().min(u64::MAX as u128) as u64
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObservedProcessor {
    pub runtime_id: u64,
    pub index: usize,
    /// Application-configured processor name, never frame content.
    pub name: Arc<str>,
}
/// A strict allowlist: no text, audio, images, metadata, tool arguments, error
/// messages, transport identifiers, or original Frame is retained.
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedFrame {
    pub id: u64,
    /// Native identity only; synthetic observations may omit it.
    pub token: Option<crate::FrameObservationToken>,
    pub kind: &'static str,
    pub interruptible: bool,
    /// Endpointing silence, not the payload's wall-clock timestamp.
    pub vad_stop_secs: Option<f64>,
    /// At most 64 numeric, explicitly reported service metrics.
    pub metrics: Vec<crate::Metric>,
    pub omitted_metrics: usize,
}
impl ObservedFrame {
    pub(crate) fn capture(frame: &Frame) -> Self {
        Self {
            id: frame.id,
            token: Some(frame.observation_token()),
            kind: frame.data.source_class_name(),
            interruptible: frame.interruptible,
            vad_stop_secs: match &frame.data {
                FrameData::VadUserStoppedSpeaking(vad) => Some(vad.duration_secs),
                FrameData::VadUserStoppedSpeakingDetailed(vad) => Some(vad.stop_secs),
                _ => None,
            },
            metrics: match &frame.data {
                FrameData::Metrics(metrics) => metrics.iter().take(64).cloned().collect(),
                _ => vec![],
            },
            omitted_metrics: match &frame.data {
                FrameData::Metrics(metrics) => metrics.len().saturating_sub(64),
                _ => 0,
            },
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SetupOutcome {
    Completed,
    Failed,
    /// The setup future was dropped or unwound without returning.
    Aborted,
}
#[derive(Debug, Clone, PartialEq)]
pub enum PipelineObservation {
    RuntimeStarted {
        runtime_id: u64,
        timestamp_ns: u64,
    },
    ProcessorSetup {
        processor: ObservedProcessor,
        started_ns: u64,
        finished_ns: u64,
        outcome: SetupOutcome,
    },
    /// A health transition, emitted before the corresponding error is pushed.
    ProcessorUsabilityChanged {
        processor: ObservedProcessor,
        health: crate::ProcessorHealthSnapshot,
        timestamp_ns: u64,
    },
    /// Entry into process(), after terminal preparation and task drain.
    Process {
        processor: ObservedProcessor,
        frame: ObservedFrame,
        direction: Direction,
        timestamp_ns: u64,
    },
    /// A successful adjacent queue admission or runtime boundary delivery.
    /// Destination None means the runtime boundary, including worker requests.
    Push {
        source: ObservedProcessor,
        destination: Option<ObservedProcessor>,
        frame: ObservedFrame,
        direction: Direction,
        first_push: bool,
        timestamp_ns: u64,
    },
    /// Start reached this runtime's downstream boundary.
    PipelineStarted {
        runtime_id: u64,
        timestamp_ns: u64,
    },
}
#[derive(Debug, Clone)]
pub struct ObservationEvent {
    /// Per-registration sequence. Filtered repeated pushes do not consume it.
    pub sequence: u64,
    /// Queue losses before this event. Reducers must invalidate incomplete
    /// intervals when this changes rather than inventing missing milestones.
    pub dropped_before: u64,
    pub observation: Arc<PipelineObservation>,
}
/// Receives observations through a bounded, independently owned dispatcher.
/// Register lifecycle-owning instances once per root pipeline; nested runtimes
/// inherit that registration. Sharing an instance across registrations couples
/// cancellation and permits callbacks from separate dispatchers to overlap.
#[async_trait]
pub trait PipelineObserver: Send + Sync {
    /// Must yield while waiting. Panics isolate this observer from the runtime.
    async fn on_event(&self, event: ObservationEvent);
    /// Per-registration policy, snapshotted at startup. Defaults to no typed
    /// capture. Sensitive fields require explicit flags and bounded limits.
    fn capture_policy(&self) -> crate::ObserverCapturePolicy {
        crate::ObserverCapturePolicy::default()
    }
    /// Only this registration's bounded projection accompanies the content-free
    /// observation. The shared observation never contains these payloads.
    async fn on_captured_event(&self, event: ObservationEvent, _capture: crate::CapturedPush) {
        self.on_event(event).await;
    }
    /// Fast, nonblocking deadline observation. The dispatcher owns its timer.
    /// Queued events take priority and can delay expiration indefinitely.
    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        None
    }
    /// Serialized with on_event, outside runtime locks. Consume or advance the
    /// deadline; the dispatcher yields even if it remains expired.
    async fn on_timeout(&self) {}
    /// Idempotent, synchronous essential cancellation; must not block.
    /// Called on every dispatcher exit, including abort before first poll.
    fn cancel(&self) {}
    /// Optional explicit-shutdown cleanup, once per registration, after dispatch
    /// exit/cancel and within the original total drain deadline.
    async fn cleanup(&self) {}
}
#[derive(Debug, Clone)]
pub struct ObserverOptions {
    pub capacity: usize,
    pub observe_every_push: bool,
    /// Upper bound for draining this observer at graceful End/Stop.
    pub drain_timeout: Duration,
}
impl Default for ObserverOptions {
    fn default() -> Self {
        Self {
            capacity: 256,
            observe_every_push: true,
            drain_timeout: Duration::from_millis(100),
        }
    }
}
pub(crate) struct ObserverRegistration {
    pub observer: Arc<dyn PipelineObserver>,
    pub options: ObserverOptions,
    pub stats: ObserverHandle,
}
#[derive(Debug, Default)]
struct Counters {
    delivered: AtomicU64,
    dropped: AtomicU64,
    abandoned: AtomicU64,
    failures: AtomicU64,
}
/// A handle remains readable after worker shutdown; it does not keep tasks alive.
#[derive(Debug, Default, Clone)]
pub struct ObserverHandle(Arc<Counters>);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ObserverStats {
    pub delivered: u64,
    pub dropped: u64,
    /// Accepted events not completed when dispatch was cancelled or failed.
    pub abandoned: u64,
    pub failures: u64,
}
impl ObserverHandle {
    pub fn stats(&self) -> ObserverStats {
        ObserverStats {
            delivered: self.0.delivered.load(Ordering::Relaxed),
            dropped: self.0.dropped.load(Ordering::Relaxed),
            abandoned: self.0.abandoned.load(Ordering::Relaxed),
            failures: self.0.failures.load(Ordering::Relaxed),
        }
    }
}
struct Proxy {
    capture_policy: crate::ObserverCapturePolicy,
    observer: Arc<dyn PipelineObserver>,
    sender: Mutex<Option<mpsc::Sender<QueuedEvent>>>,
    task: Mutex<Option<JoinHandle<()>>>,
    sequence: AtomicU64,
    options: ObserverOptions,
    stats: ObserverHandle,
}
pub(crate) struct ObserverHub {
    pub id: u64,
    pub clock: Arc<dyn ObserverClock>,
    proxies: Vec<Proxy>,
}
struct DispatchGuard {
    stats: ObserverHandle,
    observer: Arc<dyn PipelineObserver>,
}
impl Drop for DispatchGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.stats.0.failures.fetch_add(1, Ordering::Relaxed);
        }
        if catch_unwind(AssertUnwindSafe(|| self.observer.cancel())).is_err() {
            self.stats.0.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}
struct CleanupGuard(ObserverHandle);
impl Drop for CleanupGuard {
    fn drop(&mut self) {
        if std::thread::panicking() {
            self.0 .0.failures.fetch_add(1, Ordering::Relaxed);
        }
    }
}
/// Cancelling shutdown never silently detaches its owned dispatch/cleanup task.
struct OwnedObserverTask(JoinHandle<()>);
impl Drop for OwnedObserverTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}
struct QueuedEvent {
    event: Option<ObservationEvent>,
    capture: Option<crate::CapturedPush>,
    stats: ObserverHandle,
    completed: bool,
}
impl Drop for QueuedEvent {
    fn drop(&mut self) {
        if !self.completed {
            self.stats.0.abandoned.fetch_add(1, Ordering::Relaxed);
        }
    }
}
impl ObserverHub {
    pub fn new(registrations: Vec<ObserverRegistration>, clock: Arc<dyn ObserverClock>) -> Self {
        let proxies = registrations
            .into_iter()
            .map(|registration| {
                let (sender, mut receiver) =
                    mpsc::channel::<QueuedEvent>(registration.options.capacity);
                let observer = registration.observer;
                let capture_policy = observer.capture_policy();
                let dispatched_observer = observer.clone();
                let stats = registration.stats.clone();
                // Construct before spawn: a never-polled future still owns and
                // drops essential cancellation, including a retained observer Arc.
                let guard = DispatchGuard {
                    stats: stats.clone(),
                    observer: observer.clone(),
                };
                let task = tokio::spawn(async move {
                    let _guard = guard;
                    loop {
                        let deadline = dispatched_observer.next_deadline();
                        tokio::select! {
                            biased;
                            queued = receiver.recv() => {
                                let Some(mut queued) = queued else { break; };
                                let event = queued.event.take().unwrap();
                                if let Some(capture) = queued.capture.take() {
                                    dispatched_observer.on_captured_event(event, capture).await;
                                } else {
                                    dispatched_observer.on_event(event).await;
                                }
                                queued.completed = true;
                                stats.0.delivered.fetch_add(1, Ordering::Relaxed);
                            }
                            _ = async {
                                match deadline {
                                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                                    None => std::future::pending::<()>().await,
                                }
                            } => {
                                dispatched_observer.on_timeout().await;
                                // An unchanged expired deadline must not spin
                                // without letting shutdown/abort be scheduled.
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                });
                Proxy {
                    capture_policy,
                    observer,
                    sender: Mutex::new(Some(sender)),
                    task: Mutex::new(Some(task)),
                    sequence: AtomicU64::new(0),
                    options: registration.options,
                    stats: registration.stats,
                }
            })
            .collect();
        Self {
            id: next_observation_domain_id(),
            clock,
            proxies,
        }
    }
    pub fn enabled(&self) -> bool {
        !self.proxies.is_empty()
    }
    pub fn emit(&self, observation: PipelineObservation) {
        self.emit_captured(observation, vec![]);
    }
    pub(crate) fn capture_push(
        &self,
        frame: &Frame,
        source: &ObservedProcessor,
        health: crate::ProcessorHealthSnapshot,
    ) -> Vec<Option<crate::CapturedPush>> {
        self.proxies
            .iter()
            .map(|proxy| proxy.capture_policy.capture(frame, source, health))
            .collect()
    }
    pub(crate) fn emit_captured(
        &self,
        observation: PipelineObservation,
        captures: Vec<Option<crate::CapturedPush>>,
    ) {
        if !self.enabled() {
            return;
        }
        let observation = Arc::new(observation);
        let mut captures = captures.into_iter();
        for proxy in &self.proxies {
            let capture = captures.next().flatten();
            if matches!(
                &*observation,
                PipelineObservation::Push {
                    first_push: false,
                    ..
                }
            ) && !proxy.options.observe_every_push
            {
                continue;
            }
            // Serialize sequence/loss capture with enqueue across concurrent lanes.
            let sender = proxy.sender.lock().unwrap();
            let Some(sender) = sender.as_ref() else {
                continue;
            };
            let sequence = proxy.sequence.fetch_add(1, Ordering::Relaxed);
            let event = ObservationEvent {
                sequence,
                dropped_before: proxy.stats.0.dropped.load(Ordering::Relaxed),
                observation: observation.clone(),
            };
            let queued = QueuedEvent {
                event: Some(event),
                capture,
                stats: proxy.stats.clone(),
                completed: false,
            };
            if let Err(error) = sender.try_send(queued) {
                // Rejected events were never accepted, so count only as dropped.
                let mut queued = error.into_inner();
                queued.completed = true;
                proxy.stats.0.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    pub fn abort(&self) {
        for proxy in &self.proxies {
            proxy.sender.lock().unwrap().take();
            if let Some(task) = proxy.task.lock().unwrap().as_ref() {
                task.abort();
            }
        }
    }
    pub async fn shutdown(&self, graceful: bool) {
        if !graceful {
            self.abort();
        }
        // Concurrent draining uses one deadline per observer, not a sum.
        let started = tokio::time::Instant::now();
        for proxy in &self.proxies {
            proxy.sender.lock().unwrap().take();
        }
        for proxy in &self.proxies {
            let task = proxy.task.lock().unwrap().take();
            if let Some(task) = task {
                let deadline = started + proxy.options.drain_timeout;
                let mut task = OwnedObserverTask(task);
                if tokio::time::timeout_at(deadline, &mut task.0)
                    .await
                    .is_err()
                {
                    task.0.abort();
                    let _ = (&mut task.0).await;
                }
                let observer = proxy.observer.clone();
                let guard = CleanupGuard(proxy.stats.clone());
                let mut cleanup = OwnedObserverTask(tokio::spawn(async move {
                    let _guard = guard;
                    observer.cleanup().await;
                }));
                if tokio::time::timeout_at(deadline, &mut cleanup.0)
                    .await
                    .is_err()
                {
                    cleanup.0.abort();
                    let _ = (&mut cleanup.0).await;
                }
            }
        }
    }
}
impl Drop for ObserverHub {
    fn drop(&mut self) {
        self.abort();
    }
}
/// Shared by Clone, reset by duplicate/deserialization, released with the last
/// clone. Observer group IDs live only as long as this logical frame; nested runtimes
/// share a group, so their first-push filtering is global too.
pub(crate) type ObservedRuntimes = Arc<OnceLock<Mutex<BTreeSet<u64>>>>;

#[cfg(test)]
mod lifecycle_recovery_tests;

#[cfg(test)]
mod lifecycle_review_tests;

// Private observation consumers reserve IDs from the same namespace without
// borrowing a hub's first-push identity.
pub(crate) fn next_observation_domain_id() -> u64 {
    static NEXT_DOMAIN_ID: AtomicU64 = AtomicU64::new(1);
    NEXT_DOMAIN_ID.fetch_add(1, Ordering::Relaxed)
}
