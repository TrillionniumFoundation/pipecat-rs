//! Explicit recovery fences late outcomes without manufacturing transitions.
use pipecat_core::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Probe(Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "health-generation"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
        Ok(())
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
impl Probe {
    fn ctx(&self) -> ProcessorContext {
        self.0.lock().unwrap().as_ref().unwrap().clone()
    }
}
async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}

#[test]
fn redundant_restoration_changes_admission_but_not_transition_observations() {
    let health = ProcessorHealth::default();
    let receiver = health.watch();
    let old = health.generation();
    assert!(!health.set_usable(true));
    assert_eq!(health.snapshot(), old.snapshot);
    assert_eq!(health.generation().restoration, old.restoration + 1);
    assert!(!receiver.has_changed().unwrap());
    let (view, owner) = ProcessorHealth::read_only(true);
    let old = view.generation();
    assert!(!view.set_usable(true));
    assert_eq!(view.generation(), old);
    assert!(!owner.set_usable(true));
    assert_eq!(view.generation().restoration, old.restoration + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stale_and_concurrently_restored_outcomes_cannot_poison_current_health() {
    let probe = Arc::new(Probe::default());
    let mut worker = Pipeline::new(vec![probe.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let ctx = probe.ctx();
    let health = ctx.health();
    let old = health.generation();
    health.set_usable(true);
    assert!(!ctx
        .push_error_if_health_generation(ErrorFrame::new("stale"), true, old)
        .await
        .unwrap());
    let mut published = 0;
    for _ in 0..128 {
        let admitted = health.generation();
        let barrier = Arc::new(std::sync::Barrier::new(2));
        let restore_health = health.clone();
        let restore_barrier = barrier.clone();
        let restore = std::thread::spawn(move || {
            restore_barrier.wait();
            restore_health.set_usable(true);
        });
        barrier.wait();
        if ctx
            .push_error_if_health_generation(ErrorFrame::new("current"), true, admitted)
            .await
            .unwrap()
        {
            published += 1;
            let frame = bounded(worker.recv()).await.unwrap();
            assert!(
                !frame
                    .frame
                    .error_origin()
                    .unwrap()
                    .reported_health
                    .is_usable
            );
        }
        restore.join().unwrap();
        assert!(health.is_usable(), "a late outcome overwrote recovery");
    }
    worker.end().await.unwrap();
    let (_, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.total_errors, published);
}

struct RestoreOnWake {
    health: ProcessorHealth,
    invoked: AtomicBool,
}
impl std::task::Wake for RestoreOnWake {
    fn wake(self: Arc<Self>) {
        if !self.invoked.swap(true, Ordering::SeqCst) {
            self.health.set_usable(true);
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_error_reentrant_watch_restores_without_deadlock_or_losing_verdict() {
    let probe = Arc::new(Probe::default());
    let mut worker = Pipeline::new(vec![probe.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let ctx = probe.ctx();
    let health = ctx.health();
    let admitted = health.generation();
    let mut receiver = health.watch();
    let restorer = Arc::new(RestoreOnWake {
        health: health.clone(),
        invoked: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(restorer.clone());
    let mut task_context = std::task::Context::from_waker(&waker);
    let mut changed = Box::pin(receiver.changed());
    assert!(changed.as_mut().poll(&mut task_context).is_pending());
    assert!(bounded(ctx.push_error_if_health_generation(
        ErrorFrame::new("silent"),
        true,
        admitted
    ))
    .await
    .unwrap());
    assert!(restorer.invoked.load(Ordering::SeqCst));
    assert!(health.is_usable());
    let frame = bounded(worker.recv()).await.unwrap();
    let origin = frame.frame.error_origin().unwrap();
    assert!(!origin.reported_health.is_usable);
    assert_eq!(origin.reported_health.revision, 1);
    assert_eq!(health.snapshot().revision, 2);
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
}

struct RestoreOnObservation {
    health: Mutex<Option<ProcessorHealth>>,
    restored: tokio::sync::Semaphore,
}
#[async_trait]
impl PipelineObserver for RestoreOnObservation {
    async fn on_event(&self, event: ObservationEvent) {
        if let PipelineObservation::ProcessorUsabilityChanged { health, .. } =
            event.observation.as_ref()
        {
            if !health.is_usable {
                self.health
                    .lock()
                    .unwrap()
                    .as_ref()
                    .unwrap()
                    .set_usable(true);
            } else {
                self.restored.add_permits(1);
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn conditional_error_observer_can_restore_and_keep_original_error_attribution() {
    let probe = Arc::new(Probe::default());
    let observer = Arc::new(RestoreOnObservation {
        health: Mutex::new(None),
        restored: tokio::sync::Semaphore::new(0),
    });
    let mut worker = Pipeline::new(vec![probe.clone()])
        .with_observer(observer.clone(), ObserverOptions::default())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let ctx = probe.ctx();
    *observer.health.lock().unwrap() = Some(ctx.health());
    assert!(ctx
        .push_error_if_health_generation(ErrorFrame::new("silent"), true, ctx.health().generation())
        .await
        .unwrap());
    bounded(observer.restored.acquire()).await.unwrap().forget();
    assert!(ctx.is_usable());
    let frame = bounded(worker.recv()).await.unwrap();
    assert!(
        !frame
            .frame
            .error_origin()
            .unwrap()
            .reported_health
            .is_usable
    );
    assert_eq!(ctx.health().snapshot().revision, 2);
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
}
