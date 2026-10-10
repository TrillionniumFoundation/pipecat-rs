//! Health and policy contracts use owned gates, never provider calls or sleeps.
use pipecat_core::*;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("health or policy operation stalled")
}
async fn permit(gate: &Semaphore) {
    bounded(gate.acquire()).await.unwrap().forget();
}
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    bounded(worker.recv()).await.expect("output closed")
}
async fn send(worker: &PipelineWorker, text: &str) {
    worker
        .sender()
        .send(Frame::text(text), Direction::Downstream)
        .await
        .unwrap();
}

struct Probe {
    health: ProcessorHealth,
    gate_text: bool,
    category: ErrorCategory,
    force: bool,
    context: Mutex<Option<ProcessorContext>>,
    entered: Semaphore,
    release: Semaphore,
    prepared: AtomicUsize,
    cleaned: AtomicUsize,
    text_calls: AtomicUsize,
    active_dropped: Arc<AtomicBool>,
}
impl Default for Probe {
    fn default() -> Self {
        Self {
            health: ProcessorHealth::default(),
            gate_text: false,
            category: ErrorCategory::Authentication,
            force: false,
            context: Mutex::new(None),
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            prepared: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
            text_calls: AtomicUsize::new(0),
            active_dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}
impl Probe {
    fn context(&self) -> ProcessorContext {
        self.context.lock().unwrap().as_ref().unwrap().clone()
    }
    async fn error(&self) {
        self.context()
            .push_error_with_permanence(
                ErrorFrame {
                    category: self.category,
                    ..ErrorFrame::new("provider failed")
                },
                self.force,
            )
            .await
            .unwrap();
    }
}
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for Probe {
    fn name(&self) -> &str {
        "same-name"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    fn process_when_unusable(&self, frame: &Frame, _: Direction) -> bool {
        !(self.gate_text && matches!(frame.data, FrameData::Text(_)))
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
        if let FrameData::Text(text) = &frame.data {
            self.text_calls.fetch_add(1, Ordering::SeqCst);
            match text.text.as_str() {
                "error" => {
                    return ctx
                        .push_error_with_permanence(
                            ErrorFrame {
                                category: self.category,
                                ..ErrorFrame::new("provider failed")
                            },
                            self.force,
                        )
                        .await;
                }
                "returned" => {
                    return Err(ProcessorError::new("reported result").with_category(self.category));
                }
                "hold" => {
                    let _dropped = DropFlag(self.active_dropped.clone());
                    self.entered.add_permits(1);
                    self.release.acquire().await.unwrap().forget();
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.prepared.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn start(probe: Arc<Probe>, policy: ProcessorUnusablePolicy) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![probe])
        .start(PipelineConfig {
            processor_unusable_policy: policy,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    worker
}

#[test]
fn concurrent_health_changes_are_idempotent_and_watch_is_revisioned() {
    let health = ProcessorHealth::default();
    let receiver = health.watch();
    let barrier = Arc::new(std::sync::Barrier::new(3));
    let setters: Vec<_> = (0..2)
        .map(|_| {
            let health = health.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                health.set_usable(false)
            })
        })
        .collect();
    barrier.wait();
    let transitions = setters
        .into_iter()
        .map(|setter| usize::from(setter.join().unwrap()))
        .sum::<usize>();
    assert_eq!(transitions, 1);
    assert_eq!(
        health.snapshot(),
        ProcessorHealthSnapshot {
            is_usable: false,
            revision: 1
        }
    );
    assert!(receiver.has_changed().unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_handle_has_no_op_updates_and_coalesced_revision_notifications() {
    let health = ProcessorHealth::default();
    let mut watcher = health.watch();
    assert!(health.is_usable());
    assert!(!health.set_usable(true));
    assert!(!watcher.has_changed().unwrap());
    assert!(health.set_usable(false));
    assert!(!health.set_usable(false));
    assert!(health.set_usable(true));
    bounded(watcher.changed()).await.unwrap();
    assert_eq!(
        *watcher.borrow_and_update(),
        ProcessorHealthSnapshot {
            is_usable: true,
            revision: 2
        }
    );
    assert!(!watcher.has_changed().unwrap());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn only_permanent_categories_change_health_and_plain_processors_keep_working() {
    for category in [
        ErrorCategory::Unknown,
        ErrorCategory::Authentication,
        ErrorCategory::Authorization,
        ErrorCategory::InvalidRequest,
        ErrorCategory::RateLimit,
        ErrorCategory::Quota,
        ErrorCategory::Connectivity,
        ErrorCategory::Server,
        ErrorCategory::Application,
    ] {
        let probe = Arc::new(Probe {
            category,
            ..Probe::default()
        });
        let mut worker = start(probe.clone(), ProcessorUnusablePolicy::Continue).await;
        for input in ["error", "returned"] {
            send(&worker, input).await;
            let error = next(&mut worker).await;
            assert_eq!(error.direction, Direction::Upstream);
            assert_eq!(
                error.frame.data.as_error_frame().unwrap().category,
                category
            );
            let origin = error.frame.error_origin().unwrap();
            assert_eq!(origin.reported_health.is_usable, !category.is_permanent());
            assert_eq!(origin.health.is_usable(), !category.is_permanent());
            assert_eq!(probe.health.is_usable(), !category.is_permanent());
        }
        assert_eq!(
            worker.unusable_processors().len(),
            usize::from(category.is_permanent())
        );
        send(&worker, "still called").await;
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::Text(_)
        ));
        worker.end().await.unwrap();
        let report = bounded(worker.collect()).await.unwrap().1;
        assert_eq!(report.total_errors, 2);
        assert_eq!(report.state, WorkerState::Ended);
        assert_eq!(probe.text_calls.load(Ordering::SeqCst), 3);
        assert_eq!(probe.cleaned.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn forced_permanence_preserves_category_and_each_policy_has_its_own_terminal() {
    for policy in [
        ProcessorUnusablePolicy::Continue,
        ProcessorUnusablePolicy::End,
        ProcessorUnusablePolicy::Cancel,
    ] {
        let probe = Arc::new(Probe {
            category: ErrorCategory::Server,
            force: true,
            ..Probe::default()
        });
        let mut worker = start(probe.clone(), policy).await;
        send(&worker, "error").await;
        let error = next(&mut worker).await;
        assert_eq!(
            error.frame.data.as_error_frame().unwrap().category,
            ErrorCategory::Server
        );
        assert!(
            !error
                .frame
                .error_origin()
                .unwrap()
                .reported_health
                .is_usable
        );
        if policy == ProcessorUnusablePolicy::Continue {
            assert_eq!(worker.state(), WorkerState::Running);
            worker.end().await.unwrap();
        }
        let (frames, report) = bounded(worker.collect()).await.unwrap();
        assert_eq!(
            frames
                .iter()
                .filter(|item| item.frame.is_terminal())
                .count(),
            1
        );
        assert_eq!(
            report.state,
            if policy == ProcessorUnusablePolicy::Cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(probe.cleaned.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn merely_becoming_unusable_does_not_trigger_policy_but_reporting_does() {
    let probe = Arc::new(Probe {
        category: ErrorCategory::Unknown,
        ..Probe::default()
    });
    let mut worker = start(probe.clone(), ProcessorUnusablePolicy::End).await;
    probe.health.set_usable(false);
    send(&worker, "still running").await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Text(_)
    ));
    assert!(worker.unusable_processors().is_empty());
    send(&worker, "error").await;
    assert!(
        !next(&mut worker)
            .await
            .frame
            .error_origin()
            .unwrap()
            .reported_health
            .is_usable
    );
    assert_eq!(
        bounded(worker.collect()).await.unwrap().1.state,
        WorkerState::Ended
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn opt_in_gating_is_bidirectional_and_restoration_only_admits_new_work() {
    let probe = Arc::new(Probe {
        health: ProcessorHealth::new(false),
        gate_text: true,
        ..Probe::default()
    });
    let mut worker = start(probe.clone(), ProcessorUnusablePolicy::Continue).await;
    for direction in [Direction::Downstream, Direction::Upstream] {
        worker
            .sender()
            .send(Frame::text("drop"), direction)
            .await
            .unwrap();
        // Same normal lane, so the control receipt fences the preceding text.
        worker
            .sender()
            .send(Frame::new(FrameData::LlmRun), direction)
            .await
            .unwrap();
        let receipt = next(&mut worker).await;
        assert!(matches!(receipt.frame.data, FrameData::LlmRun));
        assert_eq!(receipt.direction, direction);
    }
    worker
        .sender()
        .send(Frame::interruption(), Direction::Upstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert_eq!(probe.text_calls.load(Ordering::SeqCst), 0);
    probe.health.set_usable(true);
    for direction in [Direction::Downstream, Direction::Upstream] {
        worker
            .sender()
            .send(Frame::text("restored"), direction)
            .await
            .unwrap();
        assert_eq!(next(&mut worker).await.direction, direction);
    }
    probe.health.set_usable(false);
    worker.end().await.unwrap();
    let report = bounded(worker.collect()).await.unwrap().1;
    assert_eq!(report.dropped_frames, 2);
    assert_eq!(probe.text_calls.load(Ordering::SeqCst), 2);
    assert_eq!(probe.prepared.load(Ordering::SeqCst), 1);
    assert_eq!(probe.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_change_does_not_cancel_in_flight_work_and_gates_queued_work_at_entry() {
    let probe = Arc::new(Probe {
        gate_text: true,
        ..Probe::default()
    });
    let mut worker = start(probe.clone(), ProcessorUnusablePolicy::Continue).await;
    send(&worker, "hold").await;
    permit(&probe.entered).await;
    send(&worker, "queued").await;
    probe.health.set_usable(false);
    probe.release.add_permits(1);
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Text(_)
    ));
    worker.end().await.unwrap();
    let report = bounded(worker.collect()).await.unwrap().1;
    assert_eq!(report.dropped_frames, 1);
    assert_eq!(probe.text_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_policy_retries_owned_terminal_admission_without_waiting_on_producer_lock() {
    let probe = Arc::new(Probe::default());
    let mut worker = Pipeline::new(vec![probe.clone()])
        .start(PipelineConfig {
            queue_capacity: 1,
            processor_unusable_policy: ProcessorUnusablePolicy::End,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    send(&worker, "hold").await;
    permit(&probe.entered).await;
    send(&worker, "queued").await;
    let sender = worker.sender();
    let blocked = tokio::spawn(async move {
        sender
            .send(Frame::text("rejected"), Direction::Downstream)
            .await
    });
    probe.error().await;
    assert!(next(&mut worker)
        .await
        .frame
        .data
        .as_error_frame()
        .is_some());
    assert_eq!(bounded(blocked).await.unwrap(), Err(PipelineError::Closed));
    assert_eq!(worker.state(), WorkerState::Running);
    probe.release.add_permits(1);
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    let texts: Vec<_> = frames
        .iter()
        .filter_map(|item| match &item.frame.data {
            FrameData::Text(text) => Some(text.text.as_str()),
            _ => None,
        })
        .collect();
    assert_eq!(texts, ["hold", "queued"]);
    assert!(frames.last().unwrap().frame.is_terminal());
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(report.dropped_frames, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_policy_preempts_active_and_owned_background_work() {
    let probe = Arc::new(Probe::default());
    let worker = start(probe.clone(), ProcessorUnusablePolicy::Cancel).await;
    let background_entered = Arc::new(Semaphore::new(0));
    let background_dropped = Arc::new(AtomicBool::new(false));
    let entered = background_entered.clone();
    let dropped = background_dropped.clone();
    probe
        .context()
        .spawn("owned", false, async move {
            let _dropped = DropFlag(dropped);
            entered.add_permits(1);
            std::future::pending::<()>().await;
            Ok(())
        })
        .unwrap();
    permit(&background_entered).await;
    worker
        .sender()
        .send(
            Frame::text("hold").with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    permit(&probe.entered).await;
    send(&worker, "queued").await;
    probe.error().await;
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(frames.iter().any(|item| item.frame.data.is_cancel()));
    assert!(!frames
        .iter()
        .any(|item| matches!(item.frame.data, FrameData::Text(_))));
    assert!(background_dropped.load(Ordering::SeqCst));
    assert!(probe.active_dropped.load(Ordering::SeqCst));
    assert_eq!(probe.cleaned.load(Ordering::SeqCst), 1);
}

struct InterceptError {
    consume: bool,
    received: Semaphore,
    release: Semaphore,
    errors: Mutex<Vec<Frame>>,
}
impl InterceptError {
    fn new(consume: bool) -> Self {
        Self {
            consume,
            received: Semaphore::new(0),
            release: Semaphore::new(0),
            errors: Mutex::new(vec![]),
        }
    }
}
#[async_trait]
impl FrameProcessor for InterceptError {
    fn name(&self) -> &str {
        "same-name"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.as_error_frame().is_some() {
            self.errors.lock().unwrap().push(frame.clone());
            self.received.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            if self.consume {
                return Ok(());
            }
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_before_the_worker_boundary_suppresses_policy_without_rewriting_report_snapshot() {
    let interceptor = Arc::new(InterceptError::new(false));
    let probe = Arc::new(Probe::default());
    let mut worker = Pipeline::new(vec![interceptor.clone(), probe.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::End,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    send(&worker, "error").await;
    permit(&interceptor.received).await;
    probe.health.set_usable(true);
    interceptor.release.add_permits(1);
    let error = next(&mut worker).await;
    let origin = error.frame.error_origin().unwrap();
    assert!(!origin.reported_health.is_usable);
    assert!(origin.health.is_usable());
    assert_eq!(origin.processor.index, 1);
    assert!(worker.unusable_processors().is_empty());
    send(&worker, "still usable").await;
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Text(_)
    ));
    // A fresh permanent error after recovery can make the processor unusable
    // again. Health recovery never grants immunity to later error reports.
    send(&worker, "error").await;
    permit(&interceptor.received).await;
    interceptor.release.add_permits(1);
    let error = next(&mut worker).await;
    assert_eq!(
        error.frame.error_origin().unwrap().reported_health.revision,
        3
    );
    assert_eq!(worker.unusable_processors().len(), 1);
    assert_eq!(
        bounded(worker.collect()).await.unwrap().1.state,
        WorkerState::Ended
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consumed_errors_never_trigger_worker_policy_but_remain_in_history() {
    let interceptor = Arc::new(InterceptError::new(true));
    let probe = Arc::new(Probe::default());
    let mut worker = Pipeline::new(vec![interceptor.clone(), probe.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    send(&worker, "error").await;
    permit(&interceptor.received).await;
    interceptor.release.add_permits(1);
    // This travels through the same system lane after the consumed error.
    worker
        .sender()
        .send(Frame::interruption(), Direction::Upstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert_eq!(worker.state(), WorkerState::Running);
    assert_eq!(worker.total_errors(), 1);
    assert!(worker.unusable_processors().is_empty());
    worker.end().await.unwrap();
    assert_eq!(
        bounded(worker.collect()).await.unwrap().1.state,
        WorkerState::Ended
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn policy_is_once_per_processor_even_after_recovery_and_names_are_not_identities() {
    let first = Arc::new(Probe::default());
    let second = Arc::new(Probe::default());
    let mut worker = Pipeline::new(vec![first.clone(), second.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    first.error().await;
    next(&mut worker).await;
    let first_identity = worker.unusable_processors()[0].clone();
    first.health.set_usable(true);
    first.error().await;
    next(&mut worker).await;
    assert_eq!(worker.unusable_processors(), [first_identity]);
    second.error().await;
    next(&mut worker).await;
    let seen = worker.unusable_processors();
    assert_eq!(seen.len(), 2);
    assert_eq!(seen[0].name, seen[1].name);
    assert_ne!(seen[0].index, seen[1].index);
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cross_runtime_forwarding_preserves_local_origin_but_wire_decode_cannot_claim_health() {
    let probe = Arc::new(Probe::default());
    let mut source = start(probe.clone(), ProcessorUnusablePolicy::Continue).await;
    probe.error().await;
    let error = next(&mut source).await.frame;
    let source_identity = error.error_origin().unwrap().processor.clone();
    let mut target = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::End,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut target).await;
    target
        .sender()
        .send(error.duplicate(), Direction::Upstream)
        .await
        .unwrap();
    let forwarded = next(&mut target).await;
    assert_eq!(
        forwarded.frame.error_origin().unwrap().processor,
        source_identity
    );
    assert_eq!(target.unusable_processors(), [source_identity]);
    assert!(target.processor_health(0).unwrap().is_usable());
    assert_eq!(
        bounded(target.collect()).await.unwrap().1.state,
        WorkerState::Ended
    );

    let wire = serde_json::to_value(&error).unwrap();
    assert!(wire.get("error_origin").is_none());
    let decoded: Frame = serde_json::from_value(wire).unwrap();
    assert_eq!(decoded, error);
    assert!(decoded.error_origin().is_none());
    assert!(decoded.clone().error_origin().is_none());
    let mut target = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut target).await;
    target.processor_health(0).unwrap().set_usable(false);
    target
        .sender()
        .send(decoded, Direction::Upstream)
        .await
        .unwrap();
    assert!(next(&mut target).await.frame.error_origin().is_none());
    assert!(target.unusable_processors().is_empty());
    assert_eq!(target.state(), WorkerState::Running);
    target.end().await.unwrap();
    bounded(target.collect()).await.unwrap();
    source.end().await.unwrap();
    bounded(source.collect()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn default_health_is_runtime_scoped_even_when_a_processor_instance_is_reused() {
    let identity = Arc::new(IdentityProcessor);
    let first = Pipeline::new(vec![identity.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let second = Pipeline::new(vec![identity])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    first.processor_health(0).unwrap().set_usable(false);
    assert!(second.processor_health(0).unwrap().is_usable());
    assert!(first.processor_health(1).is_none());
    first.end().await.unwrap();
    second.end().await.unwrap();
    bounded(first.collect()).await.unwrap();
    bounded(second.collect()).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fatal_errors_keep_native_failure_semantics_without_implying_unusability() {
    for policy in [
        ProcessorUnusablePolicy::Continue,
        ProcessorUnusablePolicy::End,
        ProcessorUnusablePolicy::Cancel,
    ] {
        let probe = Arc::new(Probe::default());
        let mut worker = start(probe.clone(), policy).await;
        probe
            .context()
            .push_error(ErrorFrame {
                fatal: true,
                ..ErrorFrame::new("fatal")
            })
            .await
            .unwrap();
        assert!(matches!(
            bounded(worker.join()).await,
            Err(PipelineError::Execution(_))
        ));
        assert!(probe.health.is_usable());
        assert!(worker.unusable_processors().is_empty());
        assert_eq!(probe.cleaned.load(Ordering::SeqCst), 1);
    }
}

#[derive(Default)]
struct HealthObserver(Mutex<Vec<ObservationEvent>>);
#[async_trait]
impl PipelineObserver for HealthObserver {
    async fn on_event(&self, event: ObservationEvent) {
        self.0.lock().unwrap().push(event);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_observations_precede_error_pushes_and_detach_on_shutdown() {
    let probe = Arc::new(Probe::default());
    let observer = Arc::new(HealthObserver::default());
    let mut worker = Pipeline::new(vec![probe.clone()])
        .with_observer(observer.clone(), ObserverOptions::default())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    next(&mut worker).await;
    probe.error().await;
    next(&mut worker).await;
    probe.error().await;
    next(&mut worker).await;
    probe.health.set_usable(true);
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
    let events = observer.0.lock().unwrap().clone();
    let health: Vec<_> = events
        .iter()
        .filter_map(|event| match event.observation.as_ref() {
            PipelineObservation::ProcessorUsabilityChanged { health, .. } => {
                Some((event.sequence, *health))
            }
            _ => None,
        })
        .collect();
    assert_eq!(health.len(), 2);
    assert_eq!(
        health[0].1,
        ProcessorHealthSnapshot {
            is_usable: false,
            revision: 1
        }
    );
    assert_eq!(
        health[1].1,
        ProcessorHealthSnapshot {
            is_usable: true,
            revision: 2
        }
    );
    let first_error = events.iter().find(|event| matches!(event.observation.as_ref(), PipelineObservation::Push { frame, .. } if frame.kind == "ErrorFrame")).unwrap();
    assert!(health[0].0 < first_error.sequence);
    probe.health.set_usable(false);
    assert_eq!(observer.0.lock().unwrap().len(), events.len());
}

struct RejectNormal {
    gated: AtomicUsize,
    cleaned: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for RejectNormal {
    fn name(&self) -> &str {
        "reject-normal"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(ProcessorHealth::new(false))
    }
    fn process_when_unusable(&self, _: &Frame, _: Direction) -> bool {
        self.gated.fetch_add(1, Ordering::SeqCst);
        false
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn even_an_all_normal_gate_cannot_swallow_start_system_end_stop_or_cancel() {
    for (terminal, state, cleanup) in [
        (Frame::end(), WorkerState::Ended, 1),
        (Frame::new(FrameData::Stop), WorkerState::Stopped, 0),
        (Frame::cancel("done"), WorkerState::Cancelled, 1),
    ] {
        let processor = Arc::new(RejectNormal {
            gated: AtomicUsize::new(0),
            cleaned: AtomicUsize::new(0),
        });
        let mut worker = Pipeline::new(vec![processor.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        assert!(next(&mut worker).await.frame.data.is_start());
        worker
            .sender()
            .send(Frame::interruption(), Direction::Downstream)
            .await
            .unwrap();
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::Interruption
        ));
        worker
            .sender()
            .send(terminal, Direction::Downstream)
            .await
            .unwrap();
        let (frames, report) = bounded(worker.collect()).await.unwrap();
        assert_eq!(report.state, state);
        assert!(frames.last().unwrap().frame.is_terminal());
        assert_eq!(processor.gated.load(Ordering::SeqCst), 0);
        assert_eq!(processor.cleaned.load(Ordering::SeqCst), cleanup);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restoration_before_queued_invocation_uses_current_health() {
    let probe = Arc::new(Probe {
        gate_text: true,
        ..Probe::default()
    });
    let mut worker = start(probe.clone(), ProcessorUnusablePolicy::Continue).await;
    send(&worker, "hold").await;
    permit(&probe.entered).await;
    probe.health.set_usable(false);
    send(&worker, "queued while unusable").await;
    probe.health.set_usable(true);
    probe.release.add_permits(1);
    next(&mut worker).await;
    assert!(
        matches!(next(&mut worker).await.frame.data, FrameData::Text(text) if text.text == "queued while unusable")
    );
    worker.end().await.unwrap();
    assert_eq!(bounded(worker.collect()).await.unwrap().1.dropped_frames, 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn late_background_permanent_report_can_make_recovered_processor_unusable_again() {
    let probe = Arc::new(Probe::default());
    let mut worker = start(probe.clone(), ProcessorUnusablePolicy::End).await;
    let entered = Arc::new(Semaphore::new(0));
    let release = Arc::new(Semaphore::new(0));
    let started = entered.clone();
    let released = release.clone();
    probe
        .context()
        .spawn("late error", false, async move {
            started.add_permits(1);
            released.acquire().await.unwrap().forget();
            Err(ProcessorError::new("late permanent failure")
                .with_category(ErrorCategory::Authentication))
        })
        .unwrap();
    permit(&entered).await;
    probe.health.set_usable(false);
    probe.health.set_usable(true);
    release.add_permits(1);
    let error = next(&mut worker).await;
    assert_eq!(
        error.frame.error_origin().unwrap().reported_health,
        ProcessorHealthSnapshot {
            is_usable: false,
            revision: 3
        }
    );
    assert_eq!(
        bounded(worker.collect()).await.unwrap().1.state,
        WorkerState::Ended
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_finished_context_cannot_change_shared_health_reused_in_a_later_runtime() {
    let probe = Arc::new(Probe::default());
    let first = start(probe.clone(), ProcessorUnusablePolicy::Continue).await;
    let old_context = probe.context();
    first.end().await.unwrap();
    bounded(first.collect()).await.unwrap();
    let second = start(probe.clone(), ProcessorUnusablePolicy::End).await;
    assert!(old_context
        .push_error_with_permanence(ErrorFrame::new("stale context"), true)
        .await
        .is_err());
    assert!(probe.health.is_usable());
    assert_eq!(second.state(), WorkerState::Running);
    second.end().await.unwrap();
    bounded(second.collect()).await.unwrap();
}
