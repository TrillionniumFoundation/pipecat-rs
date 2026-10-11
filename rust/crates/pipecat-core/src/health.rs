//! Shared local processor health. Reporting a failure and restoring usability
//! are explicit operations; this module never retries provider work.
use crate::observer::ObserverHub;
use crate::{ObservedProcessor, PipelineObservation};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, Weak,
};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessorHealthSnapshot {
    pub is_usable: bool,
    /// Increments on transitions only, including restoration.
    pub revision: u64,
}

/// Admission token for outcomes that must not cross an explicit recovery.
/// Unlike transition revisions, restoration changes on every accepted true setter.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProcessorHealthGeneration {
    pub snapshot: ProcessorHealthSnapshot,
    pub restoration: u64,
}

/// A cloneable health handle that can be retained before startup and after
/// shutdown. It owns no tasks and does not keep a worker alive.
#[derive(Clone)]
pub struct ProcessorHealth(Arc<HealthInner>);

#[derive(Clone)]
pub(crate) struct WeakProcessorHealth(Weak<HealthInner>);
impl WeakProcessorHealth {
    pub(crate) fn upgrade(&self) -> Option<ProcessorHealth> {
        self.0.upgrade().map(ProcessorHealth)
    }
}

struct HealthInner {
    read_only: bool,
    changed: watch::Sender<ProcessorHealthSnapshot>,
    restoration: AtomicU64,
    observers: Mutex<Vec<Weak<HealthObservation>>>,
}

pub(crate) struct HealthObservation {
    processor: ObservedProcessor,
    hub: Arc<ObserverHub>,
}

impl std::fmt::Debug for ProcessorHealth {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("ProcessorHealth")
            .field(&self.snapshot())
            .finish()
    }
}

impl Default for ProcessorHealth {
    fn default() -> Self {
        Self::new(true)
    }
}

impl ProcessorHealth {
    pub(crate) fn downgrade(&self) -> WeakProcessorHealth {
        WeakProcessorHealth(Arc::downgrade(&self.0))
    }
    pub fn new(is_usable: bool) -> Self {
        let (changed, _) = watch::channel(ProcessorHealthSnapshot {
            is_usable,
            revision: 0,
        });
        Self(Arc::new(HealthInner {
            read_only: false,
            changed,
            restoration: AtomicU64::new(0),
            observers: Mutex::new(vec![]),
        }))
    }

    /// Build an externally immutable health view. Only the returned controller
    /// can update it; reporting a permanent error cannot overwrite derived health.
    pub fn read_only(is_usable: bool) -> (Self, ProcessorHealthController) {
        let (changed, _) = watch::channel(ProcessorHealthSnapshot {
            is_usable,
            revision: 0,
        });
        let health = Self(Arc::new(HealthInner {
            read_only: true,
            changed,
            restoration: AtomicU64::new(0),
            observers: Mutex::new(vec![]),
        }));
        (health.clone(), ProcessorHealthController(health))
    }
    pub fn snapshot(&self) -> ProcessorHealthSnapshot {
        *self.0.changed.borrow()
    }

    /// Capture transition and explicit-restoration state coherently for an
    /// admitted operation. Reading this token never changes health.
    pub fn generation(&self) -> ProcessorHealthGeneration {
        // The watch read guard serializes this pair with setters and verdicts.
        let snapshot = self.0.changed.borrow();
        ProcessorHealthGeneration {
            snapshot: *snapshot,
            restoration: self.0.restoration.load(Ordering::Relaxed),
        }
    }

    pub fn is_usable(&self) -> bool {
        self.snapshot().is_usable
    }

    /// Returns whether the value changed. Concurrent transitions are atomically
    /// ordered by revision. Setting true permits future work,
    /// but neither replays dropped work nor cancels an already requested End.
    /// Every accepted true setter advances the restoration generation, even
    /// when the boolean and transition notifications remain unchanged.
    pub fn set_usable(&self, is_usable: bool) -> bool {
        self.transition(is_usable).0
    }

    pub(crate) fn transition(&self, is_usable: bool) -> (bool, ProcessorHealthSnapshot) {
        if self.0.read_only {
            return (false, self.snapshot());
        }
        self.transition_owned(is_usable)
    }
    fn transition_owned(&self, is_usable: bool) -> (bool, ProcessorHealthSnapshot) {
        self.verdict(None, Some(is_usable))
            .expect("unconditional health verdict")
    }

    /// Atomically validate admission and capture the reporting verdict. A late
    /// provider outcome cannot disable health restored since its admission.
    pub(crate) fn verdict_if_generation(
        &self,
        generation: ProcessorHealthGeneration,
        permanent: bool,
    ) -> Option<ProcessorHealthSnapshot> {
        self.verdict(
            Some(generation),
            (permanent && !self.0.read_only).then_some(false),
        )
        .map(|(_, snapshot)| snapshot)
    }

    fn verdict(
        &self,
        expected: Option<ProcessorHealthGeneration>,
        is_usable: Option<bool>,
    ) -> Option<(bool, ProcessorHealthSnapshot)> {
        let observers: Vec<_> = {
            let mut observers = self.0.observers.lock().unwrap();
            observers.retain(|observer| observer.strong_count() != 0);
            observers.iter().filter_map(Weak::upgrade).collect()
        };
        let mut verdict = None;
        // Watch wakes registered Wakers synchronously after committing. They
        // may reenter a setter, so no registration mutex can be held here.
        let changed = self.0.changed.send_if_modified(|snapshot| {
            if expected.is_some_and(|expected| {
                expected.snapshot != *snapshot
                    || expected.restoration != self.0.restoration.load(Ordering::Relaxed)
            }) {
                return false;
            }
            let Some(is_usable) = is_usable else {
                verdict = Some(*snapshot);
                return false;
            };
            if is_usable {
                self.0.restoration.fetch_add(1, Ordering::Relaxed);
            }
            if snapshot.is_usable == is_usable {
                verdict = Some(*snapshot);
                return false;
            }
            snapshot.is_usable = is_usable;
            snapshot.revision = snapshot.revision.saturating_add(1);
            verdict = Some(*snapshot);
            true
        });
        let verdict = verdict?;
        if !changed {
            return Some((false, verdict));
        }
        // An injected clock may inspect health. Never invoke it while holding
        // the watch write guard or the registration mutex. Concurrent setters
        // can interleave delivery; revisions retain the exact transition order.
        for observer in observers {
            observer
                .hub
                .emit(PipelineObservation::ProcessorUsabilityChanged {
                    processor: observer.processor.clone(),
                    health: verdict,
                    timestamp_ns: observer.hub.clock.now_ns(),
                });
        }
        Some((changed, verdict))
    }

    /// Latest-value notification, not an unbounded event log. Slow receivers
    /// may coalesce transitions; revision detects that. Do not retain a watch
    /// borrow across a setter or await. Bounded pipeline observers receive each
    /// transition subject to their normal explicit loss accounting.
    pub fn watch(&self) -> watch::Receiver<ProcessorHealthSnapshot> {
        self.0.changed.subscribe()
    }

    pub(crate) fn observe(
        &self,
        processor: ObservedProcessor,
        hub: Arc<ObserverHub>,
    ) -> Arc<HealthObservation> {
        let observation = Arc::new(HealthObservation { processor, hub });
        let mut observers = self.0.observers.lock().unwrap();
        observers.retain(|observer| observer.strong_count() != 0);
        observers.push(Arc::downgrade(&observation));
        observation
    }
}

/// Local attribution attached by `ProcessorContext::push_error`. The text in
/// ErrorFrame.processor is descriptive and is never used to resolve identity.
#[derive(Debug, Clone)]
pub struct ProcessorErrorOrigin {
    pub processor: ObservedProcessor,
    pub health: ProcessorHealth,
    /// The verdict captured before reporting this error. The live handle may
    /// subsequently change, for example after application settings are fixed.
    pub reported_health: ProcessorHealthSnapshot,
}

/// Owner-only capability for updating a read-only derived health view.
/// Keeping the controller private prevents external writes through view clones.
#[derive(Debug)]
pub struct ProcessorHealthController(ProcessorHealth);
impl ProcessorHealthController {
    pub fn set_usable(&self, is_usable: bool) -> bool {
        self.0.transition_owned(is_usable).0
    }
}
