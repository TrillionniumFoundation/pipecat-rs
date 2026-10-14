use pipecat_core::*;
use std::{
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::Semaphore;
const WAIT: Duration = Duration::from_secs(3);
fn idle(action: IdleTimeoutAction) -> WorkerIdleConfig {
    WorkerIdleConfig {
        timeout: Duration::from_millis(100),
        action,
        ..Default::default()
    }
}
fn cfg() -> PipelineConfig {
    PipelineConfig {
        queue_capacity: 2,
        output_capacity: 8,
        shutdown_timeout: Duration::from_millis(500),
        ..Default::default()
    }
}
type Case<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
async fn run(
    pipeline: Pipeline,
    config: PipelineConfig,
    case: impl for<'a> FnOnce(&'a mut PipelineWorker) -> Case<'a>,
) {
    run_expected(pipeline, config, 0, case).await;
}
async fn run_expected(
    pipeline: Pipeline,
    config: PipelineConfig,
    expected_errors: usize,
    case: impl for<'a> FnOnce(&'a mut PipelineWorker) -> Case<'a>,
) {
    let mut worker = pipeline.start(config).await.unwrap();
    let result = tokio::time::timeout(WAIT, case(&mut worker)).await;
    let cleanup_errors = cleanup_expected(&mut worker, expected_errors).await;
    assert!(
        cleanup_errors.is_empty(),
        "cleanup: {cleanup_errors:?}; scenario: {result:?}"
    );
    result
        .expect("scenario timed out")
        .expect("scenario failed");
}
async fn cleanup(worker: &mut PipelineWorker) -> Vec<String> {
    cleanup_expected(worker, 0).await
}
async fn cleanup_expected(worker: &mut PipelineWorker, expected_errors: usize) -> Vec<String> {
    let mut cleanup_errors = Vec::new();
    if !worker.has_finished() {
        match tokio::time::timeout(WAIT, worker.cancel("test cleanup")).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) if worker.has_finished() => {
                let _ = error;
            }
            other => cleanup_errors.push(format!("cancel: {other:?}")),
        }
    }
    let joined = match tokio::time::timeout(WAIT, worker.join()).await {
        Ok(result) => Some(result),
        Err(_) => {
            cleanup_errors.push("initial join timeout".into());
            let cancel = tokio::time::timeout(WAIT, worker.cancel("join timeout fallback")).await;
            if !matches!(cancel, Ok(Ok(()))) && !worker.has_finished() {
                cleanup_errors.push(format!("fallback cancel: {cancel:?}"));
            }
            // The core's finite shutdown timeout bounds cancellation cleanup.
            // Retain ownership until its supervisor actually joins.
            Some(worker.join().await)
        }
    };
    match joined {
        Some(Ok(report)) if report.errors.len() == expected_errors => {}
        other => cleanup_errors.push(format!("join: {other:?}")),
    }
    cleanup_errors
}
macro_rules! check {
    ($condition:expr) => {
        if !$condition {
            return Err(stringify!($condition).into());
        }
    };
}
async fn until(
    m: &mut WorkerIdleMonitor,
    predicate: impl Fn(&WorkerIdleSnapshot) -> bool,
) -> Result<WorkerIdleSnapshot, String> {
    loop {
        let s = m.snapshot();
        if predicate(&s) {
            return Ok(s);
        }
        m.changed().await.map_err(|e| e.to_string())?;
    }
}

#[tokio::test]
async fn default_off_and_heartbeat_does_not_keep_a_conversation_active() {
    run(Pipeline::new(vec![]), cfg(), |w| {
        Box::pin(async move {
            check!(w.idle_monitor().is_none());
            Ok(())
        })
    })
    .await;
    run(
        Pipeline::new(vec![])
            .with_idle_timeout(idle(IdleTimeoutAction::NotifyOnly))
            .with_heartbeats(HeartbeatConfig {
                period: Duration::from_millis(20),
                ..Default::default()
            }),
        cfg(),
        |w| {
            Box::pin(async move {
                let mut monitor = w.idle_monitor().ok_or("missing idle monitor")?;
                until(&mut monitor, |s| s.timeouts >= 2).await?;
                check!(
                    w.heartbeat_monitor()
                        .ok_or("no heartbeat")?
                        .snapshot()
                        .received
                        > 0
                );
                check!(
                    monitor.snapshot().activities == 1
                        && w.state() == WorkerState::Running
                        && w.errors().is_empty()
                );
                Ok(())
            })
        },
    )
    .await;
}
#[tokio::test]
async fn selected_activity_in_either_direction_resets_notify_only_monitor() {
    run(
        Pipeline::new(vec![
            Arc::new(IdentityProcessor),
            Arc::new(IdentityProcessor),
        ])
        .with_idle_timeout(idle(IdleTimeoutAction::NotifyOnly)),
        cfg(),
        |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                let mut monitor = w.idle_monitor().ok_or("no monitor")?;
                for d in [Direction::Downstream, Direction::Upstream] {
                    w.sender()
                        .send(Frame::new(FrameData::UserSpeaking), d)
                        .await
                        .map_err(|e| e.to_string())?;
                    let out = w.recv().await.ok_or("no activity")?;
                    check!(out.direction == d);
                }
                until(&mut monitor, |s| s.activities == 3).await?;
                until(&mut monitor, |s| s.timeouts > 0).await?;
                check!(!monitor.snapshot().cancel_requested && w.errors().is_empty());
                Ok(())
            })
        },
    )
    .await;
}
#[tokio::test]
async fn explicit_cancel_root_times_out_once_and_cleans_owned_work() {
    run(
        Pipeline::new(vec![]).with_idle_timeout(idle(IdleTimeoutAction::CancelRoot)),
        cfg(),
        |w| {
            Box::pin(async move {
                let mut monitor = w.idle_monitor().ok_or("no monitor")?;
                until(&mut monitor, |s| s.cancel_requested).await?;
                while w.recv().await.is_some() {}
                check!(w.state() == WorkerState::Cancelled);
                let s = monitor.snapshot();
                check!(s.timeouts == 1 && !s.active && s.stopped_for_shutdown);
                Ok(())
            })
        },
    )
    .await;
}
struct Hold {
    start: bool,
    entered: Semaphore,
    release: Semaphore,
    cleaned: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for Hold {
    fn name(&self) -> &str {
        "hold"
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if (self.start && f.data.is_start())
            || (!self.start && matches!(f.data, FrameData::Text(_)))
        {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        c.push(f, d).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test]
async fn idle_timer_precedes_start_sink_and_cancel_joins_failed_startup() {
    let h = Arc::new(Hold {
        start: true,
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        cleaned: AtomicUsize::new(0),
    });
    let p = Pipeline::new(vec![h.clone()]).with_idle_timeout(idle(IdleTimeoutAction::CancelRoot));
    let result = tokio::time::timeout(WAIT, p.start(cfg())).await;
    let description = match result {
        Ok(Err(error)) => format!("{error:?}"),
        Ok(Ok(mut worker)) => {
            let errors = cleanup(&mut worker).await;
            format!("unexpected running worker; cleanup={errors:?}")
        }
        Err(_) => "outer deadline elapsed".into(),
    };
    // A stuck Start owns the system lane, so queued Cancel cannot overtake it.
    // Idle has nevertheless armed the existing finite shutdown deadline.
    assert_eq!(description, "ShutdownTimeout");
    assert_eq!(h.cleaned.load(Ordering::SeqCst), 1);
}
#[tokio::test]
async fn end_input_closure_disarms_idle_during_graceful_drain() {
    let h = Arc::new(Hold {
        start: false,
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        cleaned: AtomicUsize::new(0),
    });
    let gate = h.clone();
    run(
        Pipeline::new(vec![h]).with_idle_timeout(idle(IdleTimeoutAction::CancelRoot)),
        cfg(),
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                w.sender()
                    .send(Frame::text("hold"), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                gate.entered
                    .acquire()
                    .await
                    .map_err(|e| e.to_string())?
                    .forget();
                w.end().await.map_err(|e| e.to_string())?;
                let s = w.idle_monitor().ok_or("no monitor")?.snapshot();
                check!(!s.active && s.stopped_for_shutdown);
                tokio::time::sleep(Duration::from_millis(220)).await;
                check!(
                    !w.idle_monitor()
                        .ok_or("no monitor")?
                        .snapshot()
                        .cancel_requested
                );
                gate.release.add_permits(1);
                while w.recv().await.is_some() {}
                check!(w.state() == WorkerState::Ended);
                Ok(())
            })
        },
    )
    .await;
}

#[derive(Default)]
struct Capture {
    context: Mutex<Option<ProcessorContext>>,
}
#[async_trait]
impl FrameProcessor for Capture {
    fn name(&self) -> &str {
        "capture"
    }
    async fn setup(&self, _: &StartFrame, c: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(c.clone());
        Ok(())
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        c.push(f, d).await
    }
}
struct Recorder {
    pushes: Mutex<Vec<(u64, bool)>>,
    changed: Semaphore,
}
impl Recorder {
    fn new() -> Self {
        Self {
            pushes: Mutex::new(vec![]),
            changed: Semaphore::new(0),
        }
    }
    async fn first(&self, id: u64) -> Result<(), String> {
        loop {
            if self.pushes.lock().unwrap().contains(&(id, true)) {
                return Ok(());
            }
            self.changed
                .acquire()
                .await
                .map_err(|e| e.to_string())?
                .forget();
        }
    }
}
#[async_trait]
impl PipelineObserver for Recorder {
    async fn on_event(&self, event: ObservationEvent) {
        if let PipelineObservation::Push {
            frame, first_push, ..
        } = &*event.observation
        {
            self.pushes.lock().unwrap().push((frame.id, *first_push));
            self.changed.add_permits(1);
        }
    }
}
fn first_only() -> ObserverOptions {
    ObserverOptions {
        observe_every_push: false,
        ..Default::default()
    }
}
async fn child_observation_scenario(shared: bool) {
    let root_observer = Arc::new(Recorder::new());
    let recorded = root_observer.clone();
    let capture = Arc::new(Capture::default());
    let cap = capture.clone();
    run(
        Pipeline::new(vec![capture])
            .with_observer(root_observer, first_only())
            .with_idle_timeout(idle(IdleTimeoutAction::NotifyOnly)),
        cfg(),
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("missing root Start")?;
                let ctx = cap
                    .context
                    .lock()
                    .unwrap()
                    .as_ref()
                    .ok_or("no context")?
                    .clone();
                let child_observer = if shared {
                    recorded.clone()
                } else {
                    Arc::new(Recorder::new())
                };
                let child_pipeline = Pipeline::new(vec![]).inherit_lifecycle(&ctx);
                let child_pipeline = if shared {
                    child_pipeline.inherit_observers(&ctx)
                } else {
                    child_pipeline.with_observer(child_observer.clone(), first_only())
                };
                let mut child = child_pipeline
                    .start(cfg())
                    .await
                    .map_err(|e| e.to_string())?;
                let result = tokio::time::timeout(WAIT, async {
                    check!(child.idle_monitor().is_none());
                    let invalid = Pipeline::new(vec![])
                        .inherit_lifecycle(&ctx)
                        .with_idle_timeout(WorkerIdleConfig::default())
                        .start(cfg())
                        .await;
                    check!(matches!(
                        invalid,
                        Err(PipelineError::InvalidConfiguration(_))
                    ));
                    child.recv().await.ok_or("missing child Start")?;
                    child
                        .sender()
                        .send(Frame::new(FrameData::UserSpeaking), Direction::Downstream)
                        .await
                        .map_err(|e| e.to_string())?;
                    let f = child.recv().await.ok_or("missing child activity")?.frame;
                    let id = f.id;
                    child_observer.first(id).await?;
                    let mut monitor = w.idle_monitor().ok_or("no monitor")?;
                    until(&mut monitor, |s| s.activities == 3).await?;
                    ctx.push(f.clone(), Direction::Downstream)
                        .await
                        .map_err(|e| e.to_string())?;
                    w.recv().await.ok_or("missing root activity")?;
                    recorded.first(id).await?;
                    check!(
                        recorded
                            .pushes
                            .lock()
                            .unwrap()
                            .iter()
                            .filter(|(frame, _)| *frame == id)
                            .count()
                            == 1
                    );
                    check!(monitor.snapshot().activities == 3);
                    // Same native identity, initially non-selected in the child, must
                    // not become a new idle activity after a payload rewrite at root.
                    child
                        .sender()
                        .send(Frame::text("first non-activity"), Direction::Downstream)
                        .await
                        .map_err(|e| e.to_string())?;
                    let mut f = child.recv().await.ok_or("missing child text")?.frame;
                    f.data = FrameData::UserSpeaking;
                    ctx.push(f, Direction::Downstream)
                        .await
                        .map_err(|e| e.to_string())?;
                    w.recv().await.ok_or("missing root rewritten frame")?;
                    check!(monitor.snapshot().activities == 3 && !monitor.snapshot().coverage_lost);
                    Ok(())
                })
                .await;
                let errors = cleanup(&mut child).await;
                check!(errors.is_empty());
                result.map_err(|_| "child scenario timeout".to_string())?
            })
        },
    )
    .await;
}
#[tokio::test]
async fn coverage_loss_disarms_auto_cancel_but_preserves_actual_observer_first_push() {
    let saved = Arc::new(Mutex::new(None));
    let slot = saved.clone();
    run(
        Pipeline::new(vec![]).with_observer(Arc::new(Recorder::new()), first_only()),
        cfg(),
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no source Start")?;
                w.sender()
                    .send(Frame::new(FrameData::UserSpeaking), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                *slot.lock().unwrap() = Some(w.recv().await.ok_or("no source activity")?.frame);
                Ok(())
            })
        },
    )
    .await;
    let observer = Arc::new(Recorder::new());
    let recorded = observer.clone();
    run(
        Pipeline::new(vec![])
            .with_observer(observer, first_only())
            .with_idle_timeout(WorkerIdleConfig {
                max_frame_observation_domains: 1,
                ..idle(IdleTimeoutAction::CancelRoot)
            }),
        cfg(),
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                let frame = saved.lock().unwrap().take().ok_or("no saved frame")?;
                let id = frame.id;
                w.sender()
                    .send(frame, Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                w.recv().await.ok_or("no forwarded frame")?;
                recorded.first(id).await?;
                let mut monitor = w.idle_monitor().ok_or("no monitor")?;
                until(&mut monitor, |s| s.coverage_lost).await?;
                tokio::time::sleep(Duration::from_millis(220)).await;
                let s = monitor.snapshot();
                check!(
                    !s.active
                        && !s.cancel_requested
                        && s.timeouts == 0
                        && w.state() == WorkerState::Running
                );
                w.sender()
                    .send(Frame::new(FrameData::UserSpeaking), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                w.recv().await.ok_or("no fresh activity")?;
                check!(
                    monitor.snapshot().coverage_lost
                        && !monitor.snapshot().active
                        && w.errors().is_empty()
                );
                Ok(())
            })
        },
    )
    .await;
}

struct HoldSystem {
    entered: Semaphore,
    release: Semaphore,
}
#[async_trait]
impl FrameProcessor for HoldSystem {
    fn name(&self) -> &str {
        "hold system"
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(f.data, FrameData::UserSpeaking) {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        c.push(f, d).await
    }
}
#[tokio::test]
async fn cancel_admission_retries_full_system_lane_without_retracting_decision() {
    let gate = Arc::new(HoldSystem {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
    });
    let g = gate.clone();
    run(
        Pipeline::new(vec![gate]).with_idle_timeout(idle(IdleTimeoutAction::CancelRoot)),
        PipelineConfig {
            system_queue_capacity: 1,
            ..cfg()
        },
        move |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                w.sender()
                    .send(Frame::new(FrameData::UserSpeaking), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                g.entered
                    .acquire()
                    .await
                    .map_err(|e| e.to_string())?
                    .forget();
                w.sender()
                    .send(Frame::new(FrameData::UserSpeaking), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                let mut monitor = w.idle_monitor().ok_or("no monitor")?;
                until(&mut monitor, |s| s.cancel_requested).await?;
                check!(
                    monitor.snapshot().timeouts == 1
                        && w.errors().is_empty()
                        && w.state() == WorkerState::Running
                );
                g.release.add_permits(2);
                while w.recv().await.is_some() {}
                check!(w.state() == WorkerState::Cancelled && monitor.snapshot().timeouts == 1);
                Ok(())
            })
        },
    )
    .await;
}
struct BecomeUnusable;
#[async_trait]
impl FrameProcessor for BecomeUnusable {
    fn name(&self) -> &str {
        "become unusable"
    }
    async fn process(
        &self,
        f: Frame,
        d: Direction,
        c: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(f.data, FrameData::Text(_)) {
            Err(ProcessorError::new("fixed test failure").with_permanence_override(true))
        } else {
            c.push(f, d).await
        }
    }
}
#[tokio::test]
async fn existing_unusable_end_policy_wins_without_idle_cancel_override() {
    run_expected(
        Pipeline::new(vec![Arc::new(BecomeUnusable)])
            .with_idle_timeout(idle(IdleTimeoutAction::CancelRoot)),
        PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::End,
            ..cfg()
        },
        1,
        |w| {
            Box::pin(async move {
                w.recv().await.ok_or("no Start")?;
                w.sender()
                    .send(Frame::text("become unusable"), Direction::Downstream)
                    .await
                    .map_err(|e| e.to_string())?;
                while w.recv().await.is_some() {}
                let s = w.idle_monitor().ok_or("no monitor")?.snapshot();
                check!(
                    w.state() == WorkerState::Ended
                        && s.stopped_for_shutdown
                        && !s.cancel_requested
                        && s.timeouts == 0
                );
                Ok(())
            })
        },
    )
    .await;
}

#[tokio::test]
async fn independently_observed_child_activity_does_not_steal_root_first_push() {
    child_observation_scenario(false).await;
}
#[tokio::test]
async fn inherited_observer_child_counts_activity_once_and_preserves_first_push() {
    child_observation_scenario(true).await;
}
#[test]
fn pinned_source_defaults_agree_with_explicit_enabled_config() {
    let source: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/python_worker_idle_source.json")).unwrap();
    assert_eq!(
        source["defaults"]["idle_timeout_secs"].as_u64(),
        Some(WorkerIdleConfig::default().timeout.as_secs())
    );
    assert_eq!(source["defaults"]["cancel_on_idle_timeout"], true);
    assert_eq!(
        source["observer"]["base_observer_options"]["observe_every_push"],
        false
    );
    assert_eq!(
        source["defaults"]["idle_timeout_frames"]
            .as_array()
            .unwrap()
            .len(),
        5
    );
}
