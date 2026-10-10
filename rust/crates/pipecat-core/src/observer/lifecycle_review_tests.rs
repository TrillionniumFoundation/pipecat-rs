//! Adversarial dispatch ownership, hook ordering, and timer-priority checks.
use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};
use std::sync::Weak;
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("observer fixture must make bounded progress")
}

async fn until(test: impl Fn() -> bool) {
    bounded(async {
        while !test() {
            tokio::task::yield_now().await;
        }
    })
    .await;
}

fn registration(
    observer: Arc<dyn PipelineObserver>,
    capacity: usize,
    drain_timeout: Duration,
) -> (ObserverRegistration, ObserverHandle) {
    let stats = ObserverHandle::default();
    (
        ObserverRegistration {
            observer,
            options: ObserverOptions {
                capacity,
                drain_timeout,
                ..ObserverOptions::default()
            },
            stats: stats.clone(),
        },
        stats,
    )
}

fn hub(registrations: Vec<ObserverRegistration>) -> Arc<ObserverHub> {
    Arc::new(ObserverHub::new(
        registrations,
        Arc::new(MonotonicObserverClock::default()),
    ))
}

fn emit(hub: &ObserverHub) {
    hub.emit(PipelineObservation::RuntimeStarted {
        runtime_id: hub.id,
        timestamp_ns: 0,
    });
}

#[derive(Default)]
struct LifecycleProbe {
    events: AtomicUsize,
    event_dropped: AtomicUsize,
    cancelled: AtomicUsize,
    cleanup_entered: AtomicUsize,
    cleanup_completed: AtomicUsize,
    cleanup_dropped: AtomicUsize,
    event_delay: Option<Duration>,
    cleanup_delay: Option<Duration>,
    hang_event: bool,
    panic_event: bool,
    panic_cancel: bool,
    panic_cleanup: bool,
}

struct IncrementOnDrop<'a>(&'a AtomicUsize);
impl Drop for IncrementOnDrop<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

#[async_trait]
impl PipelineObserver for LifecycleProbe {
    async fn on_event(&self, _: ObservationEvent) {
        self.events.fetch_add(1, Ordering::SeqCst);
        let _guard = IncrementOnDrop(&self.event_dropped);
        assert!(!self.panic_event, "intentional callback panic");
        if self.hang_event {
            std::future::pending::<()>().await;
        }
        if let Some(delay) = self.event_delay {
            tokio::time::sleep(delay).await;
        }
    }

    fn cancel(&self) {
        assert_eq!(
            self.events.load(Ordering::SeqCst),
            self.event_dropped.load(Ordering::SeqCst),
            "the active event future must be dropped before cancellation"
        );
        self.cancelled.fetch_add(1, Ordering::SeqCst);
        assert!(!self.panic_cancel, "intentional cancellation panic");
    }

    async fn cleanup(&self) {
        assert!(self.cancelled.load(Ordering::SeqCst) > 0);
        assert_eq!(
            self.events.load(Ordering::SeqCst),
            self.event_dropped.load(Ordering::SeqCst)
        );
        self.cleanup_entered.fetch_add(1, Ordering::SeqCst);
        let _guard = IncrementOnDrop(&self.cleanup_dropped);
        assert!(!self.panic_cleanup, "intentional cleanup panic");
        if let Some(delay) = self.cleanup_delay {
            tokio::time::sleep(delay).await;
        }
        self.cleanup_completed.fetch_add(1, Ordering::SeqCst);
    }
}

#[tokio::test]
async fn cancelled_shutdown_aborts_the_dispatch_task_it_owns() {
    let probe = Arc::new(LifecycleProbe {
        hang_event: true,
        ..LifecycleProbe::default()
    });
    let (registration, stats) = registration(probe.clone(), 4, Duration::from_secs(60));
    let hub = hub(vec![registration]);
    emit(&hub);
    until(|| probe.events.load(Ordering::SeqCst) == 1).await;
    let shutdown_hub = hub.clone();
    let shutdown = tokio::spawn(async move { shutdown_hub.shutdown(true).await });
    until(|| hub.proxies[0].task.lock().unwrap().is_none()).await;
    shutdown.abort();
    assert!(bounded(shutdown).await.unwrap_err().is_cancelled());
    until(|| probe.cancelled.load(Ordering::SeqCst) == 1).await;
    assert_eq!(probe.event_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(probe.cleanup_entered.load(Ordering::SeqCst), 0);
    assert_eq!(stats.stats().abandoned, 1);
    assert_eq!(stats.stats().failures, 0);
    // Retaining the hub does not retain the dispatcher taken by shutdown.
    bounded(hub.shutdown(false)).await;
}

#[tokio::test]
async fn drain_and_cleanup_share_the_original_deadline() {
    let probe = Arc::new(LifecycleProbe {
        event_delay: Some(Duration::from_millis(120)),
        cleanup_delay: Some(Duration::from_millis(120)),
        ..LifecycleProbe::default()
    });
    let (registration, stats) = registration(probe.clone(), 4, Duration::from_millis(200));
    let hub = hub(vec![registration]);
    emit(&hub);
    bounded(hub.shutdown(true)).await;
    assert_eq!(stats.stats().delivered, 1);
    assert_eq!(probe.cleanup_entered.load(Ordering::SeqCst), 1);
    assert_eq!(probe.cleanup_completed.load(Ordering::SeqCst), 0);
    assert_eq!(probe.cleanup_dropped.load(Ordering::SeqCst), 1);
    assert_eq!(stats.stats().failures, 0);
}

#[tokio::test]
async fn each_registration_uses_the_shared_shutdown_start_time() {
    let first = Arc::new(LifecycleProbe {
        cleanup_delay: Some(Duration::from_millis(120)),
        ..LifecycleProbe::default()
    });
    let second = Arc::new(LifecycleProbe {
        cleanup_delay: Some(Duration::from_millis(120)),
        ..LifecycleProbe::default()
    });
    let (first_registration, _) = registration(first.clone(), 4, Duration::from_millis(200));
    let (second_registration, _) = registration(second.clone(), 4, Duration::from_millis(200));
    let hub = hub(vec![first_registration, second_registration]);
    bounded(hub.shutdown(true)).await;
    assert_eq!(first.cleanup_completed.load(Ordering::SeqCst), 1);
    assert_eq!(second.cleanup_entered.load(Ordering::SeqCst), 1);
    assert_eq!(second.cleanup_completed.load(Ordering::SeqCst), 0);
    assert_eq!(second.cleanup_dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn sharing_an_observer_explicitly_couples_registration_cancellation() {
    let shared = Arc::new(LifecycleProbe::default());
    let (first, _) = registration(shared.clone(), 4, Duration::from_secs(1));
    let (second, _) = registration(shared.clone(), 4, Duration::from_secs(1));
    let first = hub(vec![first]);
    let second = hub(vec![second]);
    bounded(first.shutdown(false)).await;
    assert_eq!(shared.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(shared.cleanup_completed.load(Ordering::SeqCst), 1);
    emit(&second);
    bounded(second.shutdown(true)).await;
    assert_eq!(shared.cancelled.load(Ordering::SeqCst), 2);
    assert_eq!(shared.cleanup_completed.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn panic_accounting_preserves_backlog_losses_and_other_observers() {
    let failing = Arc::new(LifecycleProbe {
        panic_event: true,
        panic_cancel: true,
        panic_cleanup: true,
        ..LifecycleProbe::default()
    });
    let healthy = Arc::new(LifecycleProbe::default());
    let (failing_registration, failing_stats) =
        registration(failing.clone(), 8, Duration::from_secs(1));
    let (healthy_registration, healthy_stats) =
        registration(healthy.clone(), 8, Duration::from_secs(1));
    let hub = hub(vec![failing_registration, healthy_registration]);
    for _ in 0..3 {
        emit(&hub);
    }
    bounded(hub.shutdown(true)).await;
    assert_eq!(
        failing_stats.stats(),
        ObserverStats {
            delivered: 0,
            dropped: 0,
            abandoned: 3,
            failures: 3,
        }
    );
    assert_eq!(
        healthy_stats.stats(),
        ObserverStats {
            delivered: 3,
            dropped: 0,
            abandoned: 0,
            failures: 0,
        }
    );
    assert_eq!(healthy.cleanup_completed.load(Ordering::SeqCst), 1);
}

struct DeadlineProbe {
    deadline: Mutex<Option<tokio::time::Instant>>,
    replacement: Option<tokio::time::Instant>,
    events: AtomicUsize,
    timeouts: AtomicUsize,
    hooks: AtomicUsize,
    hub: Mutex<Weak<ObserverHub>>,
    self_feed_until: usize,
    events_at_timeout: AtomicUsize,
    release_timeout: Option<Semaphore>,
    timeout_dropped: AtomicBool,
}

impl DeadlineProbe {
    fn new(deadline: tokio::time::Instant) -> Self {
        Self {
            deadline: Mutex::new(Some(deadline)),
            replacement: None,
            events: AtomicUsize::new(0),
            timeouts: AtomicUsize::new(0),
            hooks: AtomicUsize::new(0),
            hub: Mutex::new(Weak::new()),
            self_feed_until: 0,
            events_at_timeout: AtomicUsize::new(usize::MAX),
            release_timeout: None,
            timeout_dropped: AtomicBool::new(false),
        }
    }

    fn check_hub_locks(&self) {
        let hub = self.hub.lock().unwrap().upgrade();
        if let Some(hub) = hub {
            for proxy in &hub.proxies {
                assert!(proxy.sender.try_lock().is_ok(), "hook held admission lock");
                assert!(proxy.task.try_lock().is_ok(), "hook held ownership lock");
            }
        }
        self.hooks.fetch_add(1, Ordering::SeqCst);
    }
}

struct MarkOnDrop<'a>(&'a AtomicBool);
impl Drop for MarkOnDrop<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

#[async_trait]
impl PipelineObserver for DeadlineProbe {
    async fn on_event(&self, event: ObservationEvent) {
        self.check_hub_locks();
        let count = self.events.fetch_add(1, Ordering::SeqCst) + 1;
        assert_eq!(event.sequence, (count - 1) as u64);
        if let Some(replacement) = self.replacement {
            *self.deadline.lock().unwrap() = Some(replacement);
        }
        if count < self.self_feed_until {
            let hub = self.hub.lock().unwrap().upgrade().unwrap();
            emit(&hub);
        }
    }

    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        self.check_hub_locks();
        *self.deadline.lock().unwrap()
    }

    async fn on_timeout(&self) {
        self.check_hub_locks();
        let _guard = MarkOnDrop(&self.timeout_dropped);
        self.events_at_timeout
            .store(self.events.load(Ordering::SeqCst), Ordering::SeqCst);
        self.timeouts.fetch_add(1, Ordering::SeqCst);
        *self.deadline.lock().unwrap() = None;
        if let Some(gate) = &self.release_timeout {
            gate.acquire().await.unwrap().forget();
        }
    }

    fn cancel(&self) {
        self.check_hub_locks();
        *self.deadline.lock().unwrap() = None;
    }

    async fn cleanup(&self) {
        self.check_hub_locks();
    }
}

fn deadline_hub(probe: Arc<DeadlineProbe>) -> (Arc<ObserverHub>, ObserverHandle) {
    let (registration, stats) = registration(probe.clone(), 8192, Duration::from_secs(60));
    let hub = hub(vec![registration]);
    *probe.hub.lock().unwrap() = Arc::downgrade(&hub);
    (hub, stats)
}

#[tokio::test]
async fn a_closed_queue_terminates_without_using_the_drain_timeout() {
    let probe = Arc::new(DeadlineProbe::new(
        tokio::time::Instant::now() + Duration::from_secs(3600),
    ));
    let (hub, stats) = deadline_hub(probe.clone());
    // Close input directly, so no shutdown timeout can hide a stuck dispatcher.
    hub.proxies[0].sender.lock().unwrap().take();
    until(|| {
        hub.proxies[0]
            .task
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .is_finished()
    })
    .await;
    assert_eq!(probe.timeouts.load(Ordering::SeqCst), 0);
    bounded(hub.shutdown(true)).await;
    assert_eq!(stats.stats().failures, 0);
}

#[tokio::test]
async fn callback_replaces_the_old_deadline_before_the_next_wait() {
    let now = tokio::time::Instant::now();
    let probe = Arc::new(DeadlineProbe {
        replacement: Some(now + Duration::from_secs(60)),
        ..DeadlineProbe::new(now)
    });
    let (hub, stats) = deadline_hub(probe.clone());
    emit(&hub);
    until(|| probe.events.load(Ordering::SeqCst) == 1).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
    assert_eq!(probe.timeouts.load(Ordering::SeqCst), 0);
    bounded(hub.shutdown(true)).await;
    assert_eq!(stats.stats().delivered, 1);
    assert_eq!(stats.stats().failures, 0);
}

#[tokio::test]
async fn continuously_ready_input_defers_expiration_across_cooperative_budgets() {
    let probe = Arc::new(DeadlineProbe {
        self_feed_until: 4096,
        ..DeadlineProbe::new(tokio::time::Instant::now())
    });
    let (hub, stats) = deadline_hub(probe.clone());
    emit(&hub);
    until(|| probe.timeouts.load(Ordering::SeqCst) == 1).await;
    assert_eq!(probe.events_at_timeout.load(Ordering::SeqCst), 4096);
    assert_eq!(probe.events.load(Ordering::SeqCst), 4096);
    bounded(hub.shutdown(true)).await;
    assert_eq!(stats.stats().delivered, 4096);
    assert_eq!(stats.stats().dropped, 0);
    assert_eq!(stats.stats().failures, 0);
    assert!(probe.hooks.load(Ordering::SeqCst) > 8192);
}

#[tokio::test]
async fn timeout_callbacks_serialize_with_later_admissions_and_abort_safely() {
    let probe = Arc::new(DeadlineProbe {
        release_timeout: Some(Semaphore::new(0)),
        ..DeadlineProbe::new(tokio::time::Instant::now())
    });
    let (hub, stats) = deadline_hub(probe.clone());
    until(|| probe.timeouts.load(Ordering::SeqCst) == 1).await;
    emit(&hub);
    tokio::task::yield_now().await;
    assert_eq!(probe.events.load(Ordering::SeqCst), 0);
    bounded(hub.shutdown(false)).await;
    assert!(probe.timeout_dropped.load(Ordering::SeqCst));
    assert_eq!(stats.stats().delivered, 0);
    assert_eq!(stats.stats().abandoned, 1);
    assert_eq!(stats.stats().failures, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_concurrent_shutdowns_cleanup_each_registration_once() {
    let probes: Vec<_> = (0..8)
        .map(|_| Arc::new(LifecycleProbe::default()))
        .collect();
    let registrations: Vec<_> = probes
        .iter()
        .map(|probe| registration(probe.clone(), 4, Duration::from_secs(1)).0)
        .collect();
    let hub = hub(registrations);
    emit(&hub);
    let tasks: Vec<_> = (0..8)
        .map(|_| {
            let hub = hub.clone();
            tokio::spawn(async move { hub.shutdown(true).await })
        })
        .collect();
    for task in tasks {
        bounded(task).await.unwrap();
    }
    for probe in probes {
        assert_eq!(probe.events.load(Ordering::SeqCst), 1);
        assert_eq!(probe.cancelled.load(Ordering::SeqCst), 1);
        assert_eq!(probe.cleanup_entered.load(Ordering::SeqCst), 1);
        assert_eq!(probe.cleanup_completed.load(Ordering::SeqCst), 1);
    }
}
