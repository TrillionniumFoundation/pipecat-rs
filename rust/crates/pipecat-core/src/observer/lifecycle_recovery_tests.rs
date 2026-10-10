use super::*;
use std::sync::atomic::{AtomicBool, AtomicUsize};

#[derive(Default)]
struct Probe {
    events: AtomicUsize,
    cancelled: AtomicUsize,
    cleaned: AtomicUsize,
    cleanup_dropped: AtomicUsize,
    timeouts: AtomicUsize,
    deadline: Mutex<Option<tokio::time::Instant>>,
    clear_on_event: bool,
    keep_expired: bool,
    event_panics: bool,
    deadline_panics: bool,
    timeout_panics: bool,
    cancel_panics: bool,
    cleanup_panics: bool,
    hang_event: bool,
    hang_cleanup: bool,
    cleanup_entered: AtomicBool,
}
struct CleanupDrop<'a>(&'a AtomicUsize);
impl Drop for CleanupDrop<'_> {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl PipelineObserver for Probe {
    async fn on_event(&self, _: ObservationEvent) {
        self.events.fetch_add(1, Ordering::SeqCst);
        assert!(!self.event_panics, "intentional event panic");
        if self.clear_on_event {
            *self.deadline.lock().unwrap() = None;
        }
        if self.hang_event {
            std::future::pending::<()>().await;
        }
    }
    fn next_deadline(&self) -> Option<tokio::time::Instant> {
        assert!(!self.deadline_panics, "intentional deadline panic");
        *self.deadline.lock().unwrap()
    }
    async fn on_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::SeqCst);
        assert!(!self.timeout_panics, "intentional timeout panic");
        if !self.keep_expired {
            *self.deadline.lock().unwrap() = None;
        }
    }
    fn cancel(&self) {
        self.cancelled.fetch_add(1, Ordering::SeqCst);
        *self.deadline.lock().unwrap() = None;
        assert!(!self.cancel_panics, "intentional cancel panic");
    }
    async fn cleanup(&self) {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        self.cleanup_entered.store(true, Ordering::SeqCst);
        let _guard = CleanupDrop(&self.cleanup_dropped);
        assert!(!self.cleanup_panics, "intentional cleanup panic");
        if self.hang_cleanup {
            std::future::pending::<()>().await;
        }
    }
}
fn hub(probe: Arc<Probe>, timeout: Duration) -> (Arc<ObserverHub>, ObserverHandle) {
    let stats = ObserverHandle::default();
    let hub = ObserverHub::new(
        vec![ObserverRegistration {
            observer: probe,
            options: ObserverOptions {
                capacity: 8,
                observe_every_push: true,
                drain_timeout: timeout,
            },
            stats: stats.clone(),
        }],
        Arc::new(MonotonicObserverClock::default()),
    );
    (Arc::new(hub), stats)
}
fn event(hub: &ObserverHub) {
    hub.emit(PipelineObservation::RuntimeStarted {
        runtime_id: 1,
        timestamp_ns: 0,
    });
}
async fn until(test: impl Fn() -> bool) {
    tokio::time::timeout(Duration::from_secs(2), async {
        while !test() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("bounded fixture progress");
}
#[tokio::test]
async fn abort_before_first_poll_cancels_then_cleans_once() {
    let p = Arc::new(Probe::default());
    let (h, s) = hub(p.clone(), Duration::from_secs(1));
    event(&h);
    h.shutdown(false).await;
    h.shutdown(false).await;
    assert_eq!(p.events.load(Ordering::SeqCst), 0);
    assert_eq!(p.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(p.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(s.stats().abandoned, 1);
}
#[tokio::test]
async fn hub_drop_before_first_poll_cancels_without_spawning_cleanup() {
    let p = Arc::new(Probe::default());
    let (h, _) = hub(p.clone(), Duration::from_secs(1));
    drop(h);
    until(|| p.cancelled.load(Ordering::SeqCst) == 1).await;
    assert_eq!(p.cleaned.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn concurrent_shutdown_drains_and_cleans_registration_once() {
    let p = Arc::new(Probe::default());
    let (h, s) = hub(p.clone(), Duration::from_secs(1));
    event(&h);
    tokio::join!(h.shutdown(true), h.shutdown(true));
    assert_eq!(p.events.load(Ordering::SeqCst), 1);
    assert_eq!(p.cancelled.load(Ordering::SeqCst), 1);
    assert_eq!(p.cleaned.load(Ordering::SeqCst), 1);
    assert_eq!(s.stats().delivered, 1);
}
#[tokio::test]
async fn callback_and_cancel_panics_do_not_double_unwind() {
    let p = Arc::new(Probe {
        event_panics: true,
        cancel_panics: true,
        ..Probe::default()
    });
    let (h, s) = hub(p.clone(), Duration::from_secs(1));
    event(&h);
    h.shutdown(true).await;
    assert_eq!(s.stats().failures, 2);
    assert_eq!(s.stats().abandoned, 1);
    assert_eq!(p.cleaned.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn cleanup_panic_is_isolated_and_counted() {
    let p = Arc::new(Probe {
        cleanup_panics: true,
        ..Probe::default()
    });
    let (h, s) = hub(p.clone(), Duration::from_secs(1));
    h.shutdown(true).await;
    assert_eq!(s.stats().failures, 1);
    assert_eq!(p.cleanup_dropped.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn pending_cleanup_is_aborted_and_joined_at_drain_deadline() {
    let p = Arc::new(Probe {
        hang_cleanup: true,
        ..Probe::default()
    });
    let (h, _) = hub(p.clone(), Duration::from_millis(30));
    tokio::time::timeout(Duration::from_secs(2), h.shutdown(true))
        .await
        .unwrap();
    assert_eq!(p.cleanup_dropped.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn dropping_shutdown_aborts_owned_cleanup() {
    let p = Arc::new(Probe {
        hang_cleanup: true,
        ..Probe::default()
    });
    let (h, _) = hub(p.clone(), Duration::from_secs(60));
    let j = tokio::spawn(async move {
        h.shutdown(true).await;
    });
    until(|| p.cleanup_entered.load(Ordering::SeqCst)).await;
    j.abort();
    let _ = j.await;
    until(|| p.cleanup_dropped.load(Ordering::SeqCst) == 1).await;
    assert_eq!(p.cancelled.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn queued_event_cancels_expired_deadline_before_timeout() {
    let p = Arc::new(Probe {
        clear_on_event: true,
        deadline: Mutex::new(Some(tokio::time::Instant::now())),
        ..Probe::default()
    });
    let (h, _) = hub(p.clone(), Duration::from_secs(1));
    event(&h);
    h.shutdown(true).await;
    assert_eq!(p.events.load(Ordering::SeqCst), 1);
    assert_eq!(p.timeouts.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn future_deadline_does_not_keep_closed_dispatch_alive() {
    let p = Arc::new(Probe {
        deadline: Mutex::new(Some(
            tokio::time::Instant::now() + Duration::from_secs(3600),
        )),
        ..Probe::default()
    });
    let (h, _) = hub(p.clone(), Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(2), h.shutdown(true))
        .await
        .unwrap();
    assert_eq!(p.timeouts.load(Ordering::SeqCst), 0);
}
#[tokio::test]
async fn unchanged_expired_deadline_yields_for_cancellation() {
    let p = Arc::new(Probe {
        keep_expired: true,
        deadline: Mutex::new(Some(tokio::time::Instant::now())),
        ..Probe::default()
    });
    let (h, _) = hub(p.clone(), Duration::from_secs(1));
    until(|| p.timeouts.load(Ordering::SeqCst) >= 2).await;
    h.shutdown(false).await;
    assert_eq!(p.cancelled.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn deadline_and_timeout_panics_cancel_and_cleanup() {
    for deadline_panics in [true, false] {
        let p = Arc::new(Probe {
            deadline_panics,
            timeout_panics: !deadline_panics,
            deadline: Mutex::new(Some(tokio::time::Instant::now())),
            ..Probe::default()
        });
        let (h, s) = hub(p.clone(), Duration::from_secs(1));
        until(|| p.cancelled.load(Ordering::SeqCst) == 1).await;
        h.shutdown(true).await;
        assert_eq!(s.stats().failures, 1);
        assert_eq!(p.cleaned.load(Ordering::SeqCst), 1);
    }
}
