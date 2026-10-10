//! Synchronous error admission retains the asynchronous reporting contract.
use pipecat_core::*;
use std::{
    future::Future,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

#[derive(Default)]
struct Probe {
    context: Mutex<Option<ProcessorContext>>,
    health: ProcessorHealth,
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "same-display-name"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "capture") {
            *self.context.lock().unwrap() = Some(ctx.clone());
        }
        ctx.push(frame, direction).await
    }
}
impl Probe {
    fn context(&self) -> ProcessorContext {
        self.context.lock().unwrap().as_ref().unwrap().clone()
    }
}

#[derive(Clone, Copy, Debug)]
enum Api {
    Sync,
    Async,
}
impl Api {
    async fn report(
        self,
        ctx: &ProcessorContext,
        error: ErrorFrame,
        force: bool,
        generation: ProcessorHealthGeneration,
    ) -> Result<bool, ProcessorError> {
        match self {
            Self::Sync => ctx.try_push_error_if_health_generation(error, force, generation),
            Self::Async => {
                ctx.push_error_if_health_generation(error, force, generation)
                    .await
            }
        }
    }
}
const APIS: [Api; 2] = [Api::Sync, Api::Async];

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}
async fn started(probe: Arc<Probe>, config: PipelineConfig) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![probe]).start(config).await.unwrap();
    assert!(bounded(worker.recv()).await.unwrap().frame.data.is_start());
    worker
}
async fn finish(worker: PipelineWorker) -> WorkerReport {
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap().1
}

#[tokio::test]
async fn same_true_restoration_rejects_stale_permanent_and_fatal_errors() {
    for api in APIS {
        let probe = Arc::new(Probe::default());
        let worker = started(probe.clone(), PipelineConfig::default()).await;
        let ctx = probe.context();
        let generation = probe.health.generation();
        let watch = probe.health.watch();
        assert!(!probe.health.set_usable(true));
        assert_eq!(probe.health.snapshot(), generation.snapshot);
        assert!(!watch.has_changed().unwrap());
        assert!(!api
            .report(
                &ctx,
                ErrorFrame {
                    fatal: true,
                    category: ErrorCategory::Authentication,
                    ..ErrorFrame::new("obsolete permanent failure")
                },
                false,
                generation,
            )
            .await
            .unwrap());
        assert!(probe.health.is_usable());
        assert_eq!(worker.total_errors(), 0);
        assert_eq!(worker.state(), WorkerState::Running);
        assert_eq!(finish(worker).await.total_errors, 0);
    }
}

#[tokio::test]
async fn identity_permanence_and_once_only_recording_match_both_apis() {
    for api in APIS {
        for (category, force, label) in [
            (ErrorCategory::Authentication, false, None),
            (ErrorCategory::Connectivity, true, Some("untrusted label")),
            (ErrorCategory::Connectivity, false, None),
        ] {
            let first = Arc::new(Probe::default());
            let last = Arc::new(Probe::default());
            let mut worker = Pipeline::new(vec![first.clone(), last.clone()])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            bounded(worker.recv()).await.unwrap();
            let ctx = last.context();
            let error = ErrorFrame {
                processor: label.map(str::to_owned),
                category,
                ..ErrorFrame::new("provider outcome")
            };
            assert!(api
                .report(&ctx, error, force, last.health.generation())
                .await
                .unwrap());
            let output = bounded(worker.recv()).await.unwrap();
            assert_eq!(output.direction, Direction::Upstream);
            let frame_error = output.frame.data.as_error_frame().unwrap();
            assert_eq!(frame_error.category, category);
            assert_eq!(
                frame_error.processor.as_deref(),
                Some(label.unwrap_or("same-display-name"))
            );
            let origin = output.frame.error_origin().unwrap();
            assert_eq!(origin.processor, ctx.processor_identity());
            assert_eq!(origin.processor.index, 1);
            assert_eq!(
                origin.reported_health.is_usable,
                !(force || category.is_permanent())
            );
            assert_eq!(origin.reported_health, last.health.snapshot());
            assert!(first.health.is_usable());
            assert_eq!(worker.total_errors(), 1);
            assert_eq!(worker.errors().len(), 1);
            assert_eq!(finish(worker).await.total_errors, 1);
        }
    }
}

#[tokio::test]
async fn output_backpressure_records_the_error_and_preserves_first_failure() {
    for api in APIS {
        for fatal in [false, true] {
            let probe = Arc::new(Probe::default());
            let mut worker = started(
                probe.clone(),
                PipelineConfig {
                    output_capacity: 1,
                    ..PipelineConfig::default()
                },
            )
            .await;
            let ctx = probe.context();
            ctx.try_push(Frame::text("fill output"), Direction::Upstream)
                .unwrap();
            let result = api
                .report(
                    &ctx,
                    ErrorFrame {
                        fatal,
                        ..ErrorFrame::new("report while full")
                    },
                    true,
                    probe.health.generation(),
                )
                .await;
            assert!(result.unwrap_err().fatal);
            assert!(!probe.health.is_usable());
            assert_eq!(worker.total_errors(), 1);
            assert_eq!(worker.errors()[0].fatal, fatal);
            assert_eq!(worker.state(), WorkerState::Failed);
            assert!(matches!(
                bounded(worker.join()).await,
                Err(PipelineError::Backpressure(_))
            ));
        }
    }
}

#[tokio::test]
async fn fatal_transient_error_fails_without_disabling_health_under_every_policy() {
    for api in APIS {
        for policy in [
            ProcessorUnusablePolicy::Continue,
            ProcessorUnusablePolicy::End,
            ProcessorUnusablePolicy::Cancel,
        ] {
            let probe = Arc::new(Probe::default());
            let mut worker = started(
                probe.clone(),
                PipelineConfig {
                    processor_unusable_policy: policy,
                    ..PipelineConfig::default()
                },
            )
            .await;
            assert!(api
                .report(
                    &probe.context(),
                    ErrorFrame {
                        fatal: true,
                        ..ErrorFrame::new("fatal transient outcome")
                    },
                    false,
                    probe.health.generation(),
                )
                .await
                .unwrap());
            assert!(probe.health.is_usable());
            assert_eq!(worker.total_errors(), 1);
            assert!(worker.unusable_processors().is_empty());
            assert!(matches!(
                bounded(worker.join()).await,
                Err(PipelineError::Execution(message)) if message == "fatal transient outcome"
            ));
        }
    }
}

#[tokio::test]
async fn closed_pipeline_rejects_before_mutating_health_or_recording() {
    for api in APIS {
        let probe = Arc::new(Probe::default());
        let worker = started(probe.clone(), PipelineConfig::default()).await;
        let ctx = probe.context();
        assert_eq!(finish(worker).await.total_errors, 0);
        let generation = probe.health.generation();
        let error = api
            .report(&ctx, ErrorFrame::new("closed"), true, generation)
            .await
            .unwrap_err();
        assert_eq!(error.message, "pipeline is closed");
        assert_eq!(probe.health.generation(), generation);
    }
}

#[tokio::test]
async fn stale_local_and_inherited_causal_scopes_reject_before_health_verdict() {
    for api in APIS {
        let source = Arc::new(Probe::default());
        let receiver = Arc::new(Probe::default());
        let mut source_worker = started(source.clone(), PipelineConfig::default()).await;
        let receiver_worker = started(receiver.clone(), PipelineConfig::default()).await;
        source_worker
            .sender()
            .send(Frame::text("capture"), Direction::Downstream)
            .await
            .unwrap();
        bounded(source_worker.recv()).await.unwrap();
        let local = source.context();
        let inherited = receiver.context().forwarding_context(&local);
        source_worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        bounded(source_worker.recv()).await.unwrap();
        for ctx in [local, inherited] {
            let generation = ctx.health().generation();
            assert!(!api
                .report(&ctx, ErrorFrame::new("stale cause"), true, generation)
                .await
                .unwrap());
            assert_eq!(ctx.health().generation(), generation);
        }
        assert_eq!(finish(source_worker).await.total_errors, 0);
        assert_eq!(finish(receiver_worker).await.total_errors, 0);
    }
}

#[tokio::test]
async fn read_only_health_keeps_its_owner_control_and_reporting_verdict() {
    for api in APIS {
        let (health, controller) = ProcessorHealth::read_only(true);
        let probe = Arc::new(Probe {
            health,
            ..Probe::default()
        });
        let mut worker = started(probe.clone(), PipelineConfig::default()).await;
        let ctx = probe.context();
        let generation = probe.health.generation();
        assert!(api
            .report(&ctx, ErrorFrame::new("forced"), true, generation)
            .await
            .unwrap());
        let output = bounded(worker.recv()).await.unwrap();
        assert_eq!(
            output.frame.error_origin().unwrap().reported_health,
            generation.snapshot
        );
        assert_eq!(probe.health.generation(), generation);
        assert!(!controller.set_usable(true));
        assert!(!api
            .report(&ctx, ErrorFrame::new("obsolete"), true, generation)
            .await
            .unwrap());
        assert_eq!(finish(worker).await.total_errors, 1);
    }
}

struct NoopObserver;
#[async_trait]
impl PipelineObserver for NoopObserver {
    async fn on_event(&self, _: ObservationEvent) {}
}
struct InspectingClock {
    health: ProcessorHealth,
    caller_lock: Arc<Mutex<()>>,
    armed: AtomicBool,
    calls_while_locked: AtomicUsize,
}
impl ObserverClock for InspectingClock {
    fn now_ns(&self) -> u64 {
        let _ = self.health.generation();
        if self.armed.load(Ordering::SeqCst) && self.caller_lock.try_lock().is_err() {
            self.calls_while_locked.fetch_add(1, Ordering::SeqCst);
        }
        0
    }
}
struct InspectingRestorer {
    health: ProcessorHealth,
    caller_lock: Arc<Mutex<()>>,
    invoked: AtomicBool,
    saw_caller_lock: AtomicBool,
}
impl std::task::Wake for InspectingRestorer {
    fn wake(self: Arc<Self>) {
        if !self.invoked.swap(true, Ordering::SeqCst) {
            self.saw_caller_lock
                .store(self.caller_lock.try_lock().is_err(), Ordering::SeqCst);
            let _ = self.health.generation();
            self.health.set_usable(true);
        }
    }
}

#[tokio::test]
async fn synchronous_hooks_can_read_health_but_execute_inside_the_callers_lock_scope() {
    let probe = Arc::new(Probe::default());
    let caller_lock = Arc::new(Mutex::new(()));
    let clock = Arc::new(InspectingClock {
        health: probe.health.clone(),
        caller_lock: caller_lock.clone(),
        armed: AtomicBool::new(false),
        calls_while_locked: AtomicUsize::new(0),
    });
    let mut worker = Pipeline::new(vec![probe.clone()])
        .with_observer(Arc::new(NoopObserver), ObserverOptions::default())
        .with_observer_clock(clock.clone())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let ctx = probe.context();
    let admitted = probe.health.generation();
    let mut receiver = probe.health.watch();
    let restorer = Arc::new(InspectingRestorer {
        health: probe.health.clone(),
        caller_lock: caller_lock.clone(),
        invoked: AtomicBool::new(false),
        saw_caller_lock: AtomicBool::new(false),
    });
    let waker = std::task::Waker::from(restorer.clone());
    let mut task_context = std::task::Context::from_waker(&waker);
    let mut changed = Box::pin(receiver.changed());
    assert!(changed.as_mut().poll(&mut task_context).is_pending());
    let (completed, completion) = std::sync::mpsc::channel();
    let reporting_ctx = ctx.clone();
    let reporting_clock = clock.clone();
    let reporting = std::thread::spawn(move || {
        // try_lock detects reentry without actually deadlocking the test.
        let _guard = caller_lock.lock().unwrap();
        reporting_clock.armed.store(true, Ordering::SeqCst);
        let result = reporting_ctx.try_push_error_if_health_generation(
            ErrorFrame::new("accepted verdict"),
            true,
            admitted,
        );
        reporting_clock.armed.store(false, Ordering::SeqCst);
        completed.send(result).unwrap();
    });
    assert!(completion
        .recv_timeout(Duration::from_secs(3))
        .unwrap()
        .unwrap());
    reporting.join().unwrap();
    assert!(restorer.invoked.load(Ordering::SeqCst));
    assert!(restorer.saw_caller_lock.load(Ordering::SeqCst));
    assert!(clock.calls_while_locked.load(Ordering::SeqCst) >= 2);
    assert!(probe.health.is_usable());
    assert_eq!(probe.health.snapshot().revision, 2);
    let output = bounded(worker.recv()).await.unwrap();
    let origin = output.frame.error_origin().unwrap();
    assert_eq!(origin.processor, ctx.processor_identity());
    assert_eq!(
        origin.reported_health,
        ProcessorHealthSnapshot {
            is_usable: false,
            revision: 1
        }
    );
    assert!(origin.health.is_usable());
    assert_eq!(finish(worker).await.total_errors, 1);
}
