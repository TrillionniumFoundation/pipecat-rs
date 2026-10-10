use pipecat_core::*;
use pipecat_pipeline::{ParallelPipelineConfig, ServiceSwitcher, ServiceSwitcherStrategy};
use std::future::Future;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Condvar, Mutex,
};
use std::task::{Context, Wake, Waker};
use std::time::Duration;
use tokio::sync::Notify;

/// A synchronization seam in the last output predicate, after the generation
/// observation but before output admission. Ordinary thread preemption can
/// produce the same interleaving without a blocking processor name.
struct OutputRace {
    name_calls: AtomicUsize,
    entered: Notify,
    release: (Mutex<bool>, Condvar),
}
#[async_trait]
impl FrameProcessor for OutputRace {
    fn name(&self) -> &str {
        if self.name_calls.load(Ordering::SeqCst) != 0
            && self.name_calls.fetch_add(1, Ordering::SeqCst) == 2
        {
            self.entered.notify_one();
            let (released, condvar) = &self.release;
            let mut released = released.lock().unwrap();
            while !*released {
                released = condvar.wait(released).unwrap();
            }
        }
        "race"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            self.name_calls.store(1, Ordering::SeqCst);
            ctx.push(
                Frame::new(FrameData::ServiceMetadata(ServiceMetadataFrame {
                    service_name: "race".into(),
                    user_turn_strategies: None,
                })),
                direction,
            )
            .await
        } else {
            ctx.push(frame, direction).await
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn system_metadata_admission_is_atomic_with_external_interruption() {
    let service = Arc::new(OutputRace {
        name_calls: AtomicUsize::new(0),
        entered: Notify::new(),
        release: (Mutex::new(false), Condvar::new()),
    });
    let switcher = Arc::new(ServiceSwitcher::new(vec![service.clone()]).unwrap());
    let worker = Pipeline::new(vec![switcher])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("emit metadata"), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), service.entered.notified())
        .await
        .expect("metadata did not reach the final output predicate");
    let interruption = worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await;
    *service.release.0.lock().unwrap() = true;
    service.release.1.notify_one();
    interruption.unwrap();
    worker.sender().end().await.unwrap();
    let (outputs, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty());
    assert!(
        !outputs
            .iter()
            .any(|out| matches!(out.frame.data, FrameData::ServiceMetadata(_))),
        "metadata checked before the interruption escaped after its admission"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn private_settings_copies_do_not_escape_when_recent_id_window_is_small() {
    let switcher = Arc::new(
        ServiceSwitcher::with_config(
            vec![
                Arc::new(IdentityProcessor),
                Arc::new(IdentityProcessor),
                Arc::new(IdentityProcessor),
            ],
            ServiceSwitcherStrategy::Manual,
            ParallelPipelineConfig {
                deduplication_capacity: 1,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![switcher])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let settings = Frame::new(FrameData::LlmUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            reach_inactive_services: true,
            ..Default::default()
        },
    )));
    let original = settings.id;
    worker
        .sender()
        .send(settings, Direction::Downstream)
        .await
        .unwrap();
    worker.sender().end().await.unwrap();
    let (outputs, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty());
    let updates: Vec<_> = outputs
        .iter()
        .filter(|out| matches!(out.frame.data, FrameData::LlmUpdateSettings(_)))
        .map(|out| out.frame.id)
        .collect();
    assert_eq!(updates, vec![original]);
}

struct SelectionGate {
    entered: Notify,
    release: (Mutex<bool>, Condvar),
}
impl Wake for SelectionGate {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.entered.notify_one();
        let (released, condvar) = &self.release;
        let mut released = released.lock().unwrap();
        while !*released {
            released = condvar.wait(released).unwrap();
        }
    }
}
struct FailureSource {
    health: ProcessorHealth,
    context: Mutex<Option<ProcessorContext>>,
    ending: Arc<Notify>,
}
#[async_trait]
impl FrameProcessor for FailureSource {
    fn name(&self) -> &str {
        "failure source"
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
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.ending.notify_one();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.context.lock().unwrap().take();
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_metadata_commit_and_terminal_admission_are_serialized() {
    let ending = Arc::new(Notify::new());
    let source = Arc::new(FailureSource {
        health: ProcessorHealth::default(),
        context: Mutex::new(None),
        ending: ending.clone(),
    });
    let switcher = Arc::new(
        ServiceSwitcher::with_strategy(
            vec![source.clone(), Arc::new(IdentityProcessor)],
            ServiceSwitcherStrategy::Failover,
        )
        .unwrap(),
    );
    let worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut selection = switcher.watch_selection();
    let gate = Arc::new(SelectionGate {
        entered: Notify::new(),
        release: (Mutex::new(false), Condvar::new()),
    });
    let waker = Waker::from(gate.clone());
    let mut context = Context::from_waker(&waker);
    let mut changed = Box::pin(selection.changed());
    assert!(changed.as_mut().poll(&mut context).is_pending());
    let source_context = source.context.lock().unwrap().as_ref().unwrap().clone();
    source_context
        .push_error(ErrorFrame {
            category: ErrorCategory::Authentication,
            ..ErrorFrame::new("fail over")
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), gate.entered.notified())
        .await
        .unwrap();
    worker.sender().end().await.unwrap();
    // Give terminal processing a turn while selection publication is paused.
    // Correct serialization keeps child terminal admission behind the switch.
    let _ = tokio::time::timeout(Duration::from_millis(100), ending.notified()).await;
    *gate.release.0.lock().unwrap() = true;
    gate.release.1.notify_one();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .expect("failover metadata must not race closed child input");
    assert_eq!(report.state, WorkerState::Ended);
    assert!(report.errors.is_empty());
}

struct SettingsForwarder {
    count: AtomicUsize,
    hold_first: bool,
    entered: Notify,
    release: Notify,
}
impl SettingsForwarder {
    fn new(hold_first: bool) -> Arc<Self> {
        Arc::new(Self {
            count: AtomicUsize::new(0),
            hold_first,
            entered: Notify::new(),
            release: Notify::new(),
        })
    }
}
#[async_trait]
impl FrameProcessor for SettingsForwarder {
    fn name(&self) -> &str {
        "settings forwarder"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmUpdateSettings(_)) {
            if self.count.fetch_add(1, Ordering::SeqCst) == 0 && self.hold_first {
                self.entered.notify_one();
                self.release.notified().await;
            }
            // Provenance must survive processors making a fresh frame identity.
            ctx.push(frame.duplicate(), direction).await
        } else {
            ctx.push(frame, direction).await
        }
    }
}
fn all_member_settings() -> Frame {
    Frame::new(FrameData::LlmUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            reach_inactive_services: true,
            ..Default::default()
        },
    )))
}
async fn next_output(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), worker.recv())
        .await
        .unwrap()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn delayed_private_settings_stay_private_after_many_updates_and_reactivation() {
    let delayed = SettingsForwarder::new(true);
    let switcher = Arc::new(
        ServiceSwitcher::with_config(
            vec![Arc::new(IdentityProcessor), delayed.clone()],
            ServiceSwitcherStrategy::Manual,
            ParallelPipelineConfig {
                deduplication_capacity: 2,
                ..Default::default()
            },
        )
        .unwrap(),
    );
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for _ in 0..32 {
        let settings = all_member_settings();
        let id = settings.id;
        worker
            .sender()
            .send(settings, Direction::Downstream)
            .await
            .unwrap();
        loop {
            let output = next_output(&mut worker).await;
            if matches!(output.frame.data, FrameData::LlmUpdateSettings(_)) {
                assert_eq!(output.frame.id, id);
                break;
            }
        }
    }
    tokio::time::timeout(Duration::from_secs(3), delayed.entered.notified())
        .await
        .unwrap();
    let mut selection = switcher.watch_selection();
    worker
        .sender()
        .send(switcher.switch_frame(1).unwrap(), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while selection.borrow_and_update().active_index != 1 {
            selection.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
    delayed.release.notify_one();
    let barrier = Frame::text("settings drained");
    let barrier_id = barrier.id;
    worker
        .sender()
        .send(barrier, Direction::Downstream)
        .await
        .unwrap();
    loop {
        let output = next_output(&mut worker).await;
        assert!(!matches!(
            output.frame.data,
            FrameData::LlmUpdateSettings(_)
        ));
        if output.frame.id == barrier_id {
            break;
        }
    }
    assert_eq!(delayed.count.load(Ordering::SeqCst), 32);
    worker.sender().end().await.unwrap();
    let (outputs, report) = worker.collect().await.unwrap();
    assert!(report.errors.is_empty());
    assert!(outputs
        .iter()
        .all(|out| !matches!(out.frame.data, FrameData::LlmUpdateSettings(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_switchers_keep_private_copy_ownership_scoped_to_each_compound() {
    for inner_active in [true, false] {
        let first = SettingsForwarder::new(false);
        let second = SettingsForwarder::new(false);
        let config = ParallelPipelineConfig {
            deduplication_capacity: 1,
            ..Default::default()
        };
        let inner = Arc::new(
            ServiceSwitcher::with_config(
                vec![first.clone(), second.clone()],
                ServiceSwitcherStrategy::Manual,
                config.clone(),
            )
            .unwrap(),
        );
        let members: Vec<Arc<dyn FrameProcessor>> = if inner_active {
            vec![inner, Arc::new(IdentityProcessor)]
        } else {
            vec![Arc::new(IdentityProcessor), inner]
        };
        let outer = Arc::new(
            ServiceSwitcher::with_config(members, ServiceSwitcherStrategy::Manual, config).unwrap(),
        );
        let worker = Pipeline::new(vec![outer])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        for _ in 0..16 {
            worker
                .sender()
                .send(all_member_settings(), Direction::Downstream)
                .await
                .unwrap();
        }
        worker.sender().end().await.unwrap();
        let (outputs, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty());
        assert_eq!(first.count.load(Ordering::SeqCst), 16);
        assert_eq!(second.count.load(Ordering::SeqCst), 16);
        assert_eq!(
            outputs
                .iter()
                .filter(|out| matches!(out.frame.data, FrameData::LlmUpdateSettings(_)))
                .count(),
            16
        );
    }
}

struct CleanupCounter(AtomicUsize);
#[async_trait]
impl FrameProcessor for CleanupCounter {
    fn name(&self) -> &str {
        "cleanup counter"
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
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_stop_retains_all_members_until_exactly_once_owner_cleanup() {
    let a = Arc::new(CleanupCounter(AtomicUsize::new(0)));
    let b = Arc::new(CleanupCounter(AtomicUsize::new(0)));
    let c = Arc::new(CleanupCounter(AtomicUsize::new(0)));
    let inner = Arc::new(ServiceSwitcher::new(vec![a.clone(), b.clone()]).unwrap());
    let outer = Arc::new(ServiceSwitcher::new(vec![inner.clone(), c.clone()]).unwrap());
    let worker = Pipeline::new(vec![outer.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Stopped);
    for member in [&a, &b, &c] {
        assert_eq!(member.0.load(Ordering::SeqCst), 0);
    }
    outer.cleanup().await.unwrap();
    outer.cleanup().await.unwrap();
    inner.cleanup().await.unwrap();
    for member in [&a, &b, &c] {
        assert_eq!(member.0.load(Ordering::SeqCst), 1);
    }
}
