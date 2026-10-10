//! Independent regressions for health reentrancy and shutdown policy ordering.
use pipecat_core::*;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(3), future)
        .await
        .expect("operation stalled")
}

struct ErrorAfterEnd;
#[async_trait]
impl FrameProcessor for ErrorAfterEnd {
    fn name(&self) -> &str {
        "error-after-end"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let terminal = frame.is_terminal() && !frame.data.is_cancel();
        ctx.push(frame, direction).await?;
        if terminal {
            ctx.spawn("terminal-tail", false, async {
                Err(ProcessorError::new("permanent terminal tail")
                    .with_category(ErrorCategory::Authentication))
            })?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn cancellation_policy_from_completed_background_preempts_staged_graceful_terminal() {
    let mut worker = Pipeline::new(vec![Arc::new(ErrorAfterEnd)])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    assert!(bounded(worker.recv()).await.unwrap().frame.data.is_start());
    worker.end().await.unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.total_errors, 1);
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(frames.iter().any(|item| item.frame.data.is_cancel()));
    assert!(!frames
        .iter()
        .any(|item| matches!(item.frame.data, FrameData::End(_))));
}

#[derive(Default)]
struct Events(Mutex<Vec<ObservationEvent>>);
#[async_trait]
impl PipelineObserver for Events {
    async fn on_event(&self, event: ObservationEvent) {
        self.0.lock().unwrap().push(event);
    }
}

struct HealthReadingClock {
    health: ProcessorHealth,
    ticks: AtomicU64,
}
impl ObserverClock for HealthReadingClock {
    fn now_ns(&self) -> u64 {
        // Instrumentation may inspect health without mutating it.
        let _ = self.health.snapshot();
        self.ticks.fetch_add(1, Ordering::SeqCst)
    }
}
struct SharedHealth(ProcessorHealth);
#[async_trait]
impl FrameProcessor for SharedHealth {
    fn name(&self) -> &str {
        "shared-health"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.0.clone())
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn observation_clock_can_read_health_while_recording_a_transition() {
    let health = ProcessorHealth::default();
    let mut worker = Pipeline::new(vec![Arc::new(SharedHealth(health.clone()))])
        .with_observer(Arc::new(Events::default()), ObserverOptions::default())
        .with_observer_clock(Arc::new(HealthReadingClock {
            health: health.clone(),
            ticks: AtomicU64::new(0),
        }))
        .start(PipelineConfig::default())
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    let (completed, completion) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        completed.send(health.set_usable(false)).unwrap();
    });
    assert_eq!(completion.recv_timeout(Duration::from_secs(1)), Ok(true));
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
}

struct GateProcessor {
    context: Mutex<Option<ProcessorContext>>,
    normal_entered: Semaphore,
    normal_release: Semaphore,
    system_entered: Semaphore,
    system_release: Semaphore,
}
impl GateProcessor {
    fn new() -> Self {
        Self {
            context: Mutex::new(None),
            normal_entered: Semaphore::new(0),
            normal_release: Semaphore::new(0),
            system_entered: Semaphore::new(0),
            system_release: Semaphore::new(0),
        }
    }
    fn context(&self) -> ProcessorContext {
        self.context.lock().unwrap().as_ref().unwrap().clone()
    }
    async fn report_permanent(&self) {
        self.context()
            .push_error(ErrorFrame {
                category: ErrorCategory::Authentication,
                ..ErrorFrame::new("rejected")
            })
            .await
            .unwrap();
    }
}
#[async_trait]
impl FrameProcessor for GateProcessor {
    fn name(&self) -> &str {
        "gate"
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
        if matches!(&frame.data, FrameData::Text(text) if text.text == "hold") {
            self.normal_entered.add_permits(1);
            self.normal_release.acquire().await.unwrap().forget();
        }
        if matches!(frame.data, FrameData::BotStartedSpeaking) {
            self.system_entered.add_permits(1);
            self.system_release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
async fn permit(gate: &Semaphore) {
    bounded(gate.acquire()).await.unwrap().forget();
}
async fn gated_worker(
    probe: Arc<GateProcessor>,
    policy: ProcessorUnusablePolicy,
) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![probe])
        .start(PipelineConfig {
            queue_capacity: 1,
            system_queue_capacity: 1,
            processor_unusable_policy: policy,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    worker
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cancellation_policy_retries_full_system_lane_and_rejects_new_input() {
    let probe = Arc::new(GateProcessor::new());
    let mut worker = gated_worker(probe.clone(), ProcessorUnusablePolicy::Cancel).await;
    let sender = worker.sender();
    sender
        .send(Frame::text("hold"), Direction::Downstream)
        .await
        .unwrap();
    permit(&probe.normal_entered).await;
    sender
        .send(
            Frame::new(FrameData::BotStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    permit(&probe.system_entered).await;
    sender
        .send(
            Frame::new(FrameData::UserStartedSpeaking),
            Direction::Downstream,
        )
        .await
        .unwrap();
    probe.report_permanent().await;
    assert!(bounded(worker.recv())
        .await
        .unwrap()
        .frame
        .error_origin()
        .is_some());
    assert_eq!(
        bounded(sender.send(Frame::text("rejected"), Direction::Downstream)).await,
        Err(PipelineError::Closed)
    );
    probe.system_release.add_permits(1);
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(
        frames
            .iter()
            .filter(|item| item.frame.data.is_cancel())
            .count(),
        1
    );
    assert!(!frames
        .iter()
        .any(|item| matches!(item.frame.data, FrameData::Text(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_cancel_preempts_pending_policy_end_behind_full_normal_lane() {
    let probe = Arc::new(GateProcessor::new());
    let mut worker = gated_worker(probe.clone(), ProcessorUnusablePolicy::End).await;
    let sender = worker.sender();
    sender
        .send(Frame::text("hold"), Direction::Downstream)
        .await
        .unwrap();
    permit(&probe.normal_entered).await;
    sender
        .send(Frame::text("queued"), Direction::Downstream)
        .await
        .unwrap();
    probe.report_permanent().await;
    assert!(bounded(worker.recv())
        .await
        .unwrap()
        .frame
        .error_origin()
        .is_some());
    bounded(sender.cancel("explicit cancellation"))
        .await
        .unwrap();
    let (frames, report) = bounded(worker.collect()).await.unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(
        frames
            .iter()
            .filter(|item| item.frame.data.is_cancel())
            .count(),
        1
    );
    assert!(!frames
        .iter()
        .any(|item| matches!(item.frame.data, FrameData::End(_) | FrameData::Text(_))));
}

struct RestoringObserver {
    health: ProcessorHealth,
    restored: Semaphore,
    revisions: Mutex<Vec<u64>>,
}
#[async_trait]
impl PipelineObserver for RestoringObserver {
    async fn on_event(&self, event: ObservationEvent) {
        if let PipelineObservation::ProcessorUsabilityChanged { health, .. } =
            event.observation.as_ref()
        {
            self.revisions.lock().unwrap().push(health.revision);
            if !health.is_usable {
                assert!(self.health.set_usable(true));
            } else {
                self.restored.add_permits(1);
            }
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn health_observer_can_restore_health_and_receive_the_reentrant_transition() {
    let health = ProcessorHealth::default();
    let observer = Arc::new(RestoringObserver {
        health: health.clone(),
        restored: Semaphore::new(0),
        revisions: Mutex::new(vec![]),
    });
    let mut worker = Pipeline::new(vec![Arc::new(SharedHealth(health.clone()))])
        .with_observer(observer.clone(), ObserverOptions::default())
        .start(PipelineConfig::default())
        .await
        .unwrap();
    bounded(worker.recv()).await.unwrap();
    health.set_usable(false);
    permit(&observer.restored).await;
    assert!(health.is_usable());
    assert_eq!(*observer.revisions.lock().unwrap(), [1, 2]);
    worker.end().await.unwrap();
    bounded(worker.collect()).await.unwrap();
}

#[tokio::test]
async fn forged_wire_origin_and_matching_processor_name_never_claim_local_health() {
    let probe = Arc::new(GateProcessor::new());
    let mut worker = gated_worker(probe.clone(), ProcessorUnusablePolicy::Cancel).await;
    probe.context().set_usable(false);
    let mut wire = serde_json::to_value(Frame::new(FrameData::Error(ErrorFrame {
        processor: Some("gate".into()),
        category: ErrorCategory::Authentication,
        ..ErrorFrame::new("remote rejection")
    })))
    .unwrap();
    wire["error_origin"] = serde_json::json!({
        "processor": {"runtime_id": 1, "index": 0, "name": "gate"},
        "health": {"is_usable": false, "revision": 1},
        "reported_health": {"is_usable": false, "revision": 1}
    });
    let decoded: Frame = serde_json::from_value(wire).unwrap();
    for frame in [decoded.clone(), decoded.duplicate(), decoded] {
        assert!(frame.error_origin().is_none());
        worker
            .sender()
            .send(frame, Direction::Upstream)
            .await
            .unwrap();
        assert!(bounded(worker.recv())
            .await
            .unwrap()
            .frame
            .error_origin()
            .is_none());
    }
    assert!(worker.unusable_processors().is_empty());
    assert_eq!(worker.state(), WorkerState::Running);
    worker.end().await.unwrap();
    assert_eq!(
        bounded(worker.collect()).await.unwrap().1.state,
        WorkerState::Ended
    );
}

struct RestoringWaker {
    health: ProcessorHealth,
    invoked: std::sync::atomic::AtomicBool,
}
impl std::task::Wake for RestoringWaker {
    fn wake(self: Arc<Self>) {
        if !self.invoked.swap(true, Ordering::SeqCst) {
            self.health.set_usable(true);
        }
    }
}

#[test]
fn health_watch_waker_can_restore_health_without_lock_recursion() {
    use std::future::Future;
    let health = ProcessorHealth::default();
    let mut receiver = health.watch();
    let waker = Arc::new(RestoringWaker {
        health: health.clone(),
        invoked: std::sync::atomic::AtomicBool::new(false),
    });
    let task_waker = std::task::Waker::from(waker.clone());
    let mut context = std::task::Context::from_waker(&task_waker);
    let mut changed = Box::pin(receiver.changed());
    assert!(changed.as_mut().poll(&mut context).is_pending());
    let (completed, completion) = std::sync::mpsc::channel();
    let setter_health = health.clone();
    std::thread::spawn(move || {
        completed.send(setter_health.set_usable(false)).unwrap();
    });
    assert_eq!(completion.recv_timeout(Duration::from_secs(1)), Ok(true));
    assert!(waker.invoked.load(Ordering::SeqCst));
    assert_eq!(
        health.snapshot(),
        ProcessorHealthSnapshot {
            is_usable: true,
            revision: 2
        }
    );
}
