use pipecat_core::*;
use pipecat_pipeline::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};
use std::time::Duration;
use tokio::sync::{watch, Notify};

struct Service {
    name: String,
    health: ProcessorHealth,
    frames: Mutex<Vec<(Frame, Direction)>>,
    context: Mutex<Option<ProcessorContext>>,
    cleanup: AtomicUsize,
    fail_setup: bool,
}
impl Service {
    fn new(name: &str) -> Arc<Self> {
        Arc::new(Self {
            name: name.into(),
            health: ProcessorHealth::default(),
            frames: Mutex::new(vec![]),
            context: Mutex::new(None),
            cleanup: AtomicUsize::new(0),
            fail_setup: false,
        })
    }
    fn count_text(&self, text: &str) -> usize {
        self.frames
            .lock()
            .unwrap()
            .iter()
            .filter(|(f, _)| matches!(&f.data,FrameData::Text(t) if t.text == text))
            .count()
    }
    async fn error(&self, category: ErrorCategory, fatal: bool) {
        let context = self.context.lock().unwrap().clone().unwrap();
        let mut error = ErrorFrame::new("service failed");
        error.category = category;
        error.fatal = fatal;
        context.push_error(error).await.unwrap();
    }
    async fn metadata(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        ctx.push(
            Frame::new(FrameData::ServiceMetadata(ServiceMetadataFrame {
                service_name: self.name.clone(),
                user_turn_strategies: None,
            })),
            Direction::Downstream,
        )
        .await
    }
}
#[async_trait]
impl FrameProcessor for Service {
    fn name(&self) -> &str {
        &self.name
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.health.clone())
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        if self.fail_setup {
            Err(ProcessorError::new("bad setup").with_category(ErrorCategory::Authentication))
        } else {
            Ok(())
        }
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.frames.lock().unwrap().push((frame.clone(), direction));
        if frame.data.is_start()
            || matches!(frame.data, FrameData::ServiceSwitcherRequestMetadata(_))
        {
            self.metadata(ctx).await?;
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleanup.fetch_add(1, Ordering::SeqCst);
        self.context.lock().unwrap().take();
        Ok(())
    }
}
fn switcher(
    a: &Arc<Service>,
    b: &Arc<Service>,
    strategy: ServiceSwitcherStrategy,
) -> Arc<ServiceSwitcher> {
    Arc::new(ServiceSwitcher::with_strategy(vec![a.clone(), b.clone()], strategy).unwrap())
}
async fn next_matching(
    worker: &mut PipelineWorker,
    pred: impl Fn(&DirectedFrame) -> bool,
) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let output = worker.recv().await.expect("worker output closed");
            if pred(&output) {
                return output;
            }
        }
    })
    .await
    .expect("expected output timeout")
}
async fn selection(watch: &mut watch::Receiver<ServiceSelection>, index: usize) {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if watch.borrow_and_update().active_index == index {
                break;
            }
            watch.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}
async fn finish(worker: PipelineWorker) -> WorkerReport {
    worker.sender().end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap()
        .1
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
async fn text(worker: &mut PipelineWorker, text: &str) {
    send(worker, Frame::text(text)).await;
    next_matching(
        worker,
        |out| matches!(&out.frame.data,FrameData::Text(t) if t.text == text),
    )
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_switch_routes_real_work_refreshes_metadata_and_cleans_every_member() {
    let a = Service::new("a");
    let b = Service::new("b");
    let switcher = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let mut worker = Pipeline::new(vec![switcher.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    let meta = next_matching(&mut worker, |out| {
        matches!(out.frame.data, FrameData::ServiceMetadata(_))
    })
    .await;
    assert!(matches!(meta.frame.data,FrameData::ServiceMetadata(m) if m.service_name=="a"));
    text(&mut worker, "first").await;
    send(&worker, switcher.switch_frame(1).unwrap()).await;
    let meta = next_matching(&mut worker, |out| {
        matches!(out.frame.data, FrameData::ServiceMetadata(_))
    })
    .await;
    assert!(matches!(meta.frame.data,FrameData::ServiceMetadata(m) if m.service_name=="b"));
    text(&mut worker, "second").await;
    assert_eq!(a.count_text("first"), 1);
    assert_eq!(a.count_text("second"), 0);
    assert_eq!(b.count_text("first"), 0);
    assert_eq!(b.count_text("second"), 1);
    assert!(finish(worker).await.errors.is_empty());
    assert_eq!(a.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(b.cleanup.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_consumes_member_error_before_parent_cancel_policy_and_does_not_replay() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
    let mut selected = s.watch_selection();
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await
        .unwrap();
    text(&mut worker, "already sent").await;
    a.error(ErrorCategory::Authentication, false).await;
    selection(&mut selected, 1).await;
    text(&mut worker, "fresh").await;
    assert_eq!(b.count_text("already sent"), 0);
    assert!(worker.unusable_processors().is_empty());
    assert_eq!(finish(worker).await.state, WorkerState::Ended);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn transient_errors_pass_through_without_switching() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    a.error(ErrorCategory::Connectivity, false).await;
    let output = next_matching(&mut worker, |out| out.frame.data.as_error_frame().is_some()).await;
    assert_eq!(
        output.frame.error_origin().unwrap().processor.name.as_ref(),
        "a"
    );
    assert_eq!(s.active_index(), 0);
    assert!(s.is_usable());
    finish(worker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn inactive_errors_are_consumed_but_health_recovery_is_observable() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    b.error(ErrorCategory::Authorization, false).await;
    text(&mut worker, "barrier").await;
    assert_eq!(s.active_index(), 0);
    assert_eq!(s.usable_services(), vec![0]);
    assert!(finish(worker).await.errors.is_empty());
    b.health.set_usable(true);
    assert_eq!(s.usable_services(), vec![0, 1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn all_lost_error_is_attributed_to_switcher_and_applies_parent_policy() {
    for policy in [
        ProcessorUnusablePolicy::Continue,
        ProcessorUnusablePolicy::End,
        ProcessorUnusablePolicy::Cancel,
    ] {
        let a = Service::new("a");
        let b = Service::new("b");
        b.health.set_usable(false);
        let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
        let mut worker = Pipeline::new(vec![s.clone()])
            .start(PipelineConfig {
                processor_unusable_policy: policy,
                ..Default::default()
            })
            .await
            .unwrap();
        a.error(ErrorCategory::Authentication, false).await;
        let error =
            next_matching(&mut worker, |out| out.frame.data.as_error_frame().is_some()).await;
        assert_eq!(
            error.frame.error_origin().unwrap().processor.name.as_ref(),
            "ServiceSwitcher"
        );
        assert!(!error.frame.error_origin().unwrap().health.is_usable());
        assert!(!s.is_usable());
        if policy == ProcessorUnusablePolicy::Continue {
            finish(worker).await;
        } else {
            let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                report.state,
                if policy == ProcessorUnusablePolicy::End {
                    WorkerState::Ended
                } else {
                    WorkerState::Cancelled
                }
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn manual_strategy_reports_unusable_active_member_but_remains_usable_with_a_backup() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig {
            processor_unusable_policy: ProcessorUnusablePolicy::Cancel,
            ..Default::default()
        })
        .await
        .unwrap();
    a.error(ErrorCategory::Authentication, false).await;
    let error = next_matching(&mut worker, |out| out.frame.data.as_error_frame().is_some()).await;
    assert!(error.frame.error_origin().unwrap().health.is_usable());
    assert_eq!(s.active_index(), 0);
    assert_eq!(finish(worker).await.state, WorkerState::Ended);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn refused_and_unknown_targets_forward_unchanged_and_duplicate_names_require_local_identity()
{
    let a = Service::new("same");
    let b = Service::new("same");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    for name in ["same", "elsewhere"] {
        let frame = Frame::new(FrameData::ManuallySwitchService(ProcessorReference::Name(
            name.into(),
        )));
        let id = frame.id;
        send(&worker, frame).await;
        next_matching(&mut worker, |out| out.frame.id == id).await;
        assert_eq!(s.active_index(), 0);
    }
    b.health.set_usable(false);
    let frame = s.switch_frame(1).unwrap();
    let id = frame.id;
    send(&worker, frame).await;
    next_matching(&mut worker, |out| out.frame.id == id).await;
    assert_eq!(s.active_index(), 0);
    b.health.set_usable(true);
    let mut selected = s.watch_selection();
    send(&worker, s.switch_frame(1).unwrap()).await;
    selection(&mut selected, 1).await;
    finish(worker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn aggregate_health_is_read_only_and_recovers_after_all_services_lost() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let aggregate = worker.processor_health(0).unwrap();
    assert!(!aggregate.set_usable(false));
    assert!(aggregate.is_usable());
    a.health.set_usable(false);
    b.health.set_usable(false);
    assert!(!s.is_usable());
    assert!(!aggregate.set_usable(true));
    assert!(!aggregate.is_usable());
    b.health.set_usable(true);
    assert!(s.is_usable());
    assert!(aggregate.is_usable());
    finish(worker).await;
}

fn settings(target: Option<ProcessorReference>, all: bool) -> Frame {
    Frame::new(FrameData::LlmUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            service: target,
            reach_inactive_services: all,
            ..Default::default()
        },
    )))
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_reach_active_targeted_and_opted_in_inactive_members_in_both_directions() {
    for direction in [Direction::Downstream, Direction::Upstream] {
        let a = Service::new("a");
        let b = Service::new("b");
        let s = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
        let worker = Pipeline::new(vec![s.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let mut ids = vec![];
        for (target, all) in [
            (None, false),
            (None, true),
            (Some(ProcessorReference::Name("b".into())), false),
            (Some(ProcessorReference::Name("outside".into())), true),
        ] {
            let f = settings(target, all);
            ids.push(f.id);
            worker.sender().send(f, direction).await.unwrap();
        }
        worker.sender().end().await.unwrap();
        let (outputs, _) = worker.collect().await.unwrap();
        let updates: Vec<_> = outputs
            .iter()
            .filter(|out| matches!(out.frame.data, FrameData::LlmUpdateSettings(_)))
            .collect();
        assert_eq!(updates.len(), 4);
        for update in updates {
            assert!(ids.contains(&update.frame.id));
            assert_eq!(update.direction, direction);
        }
        assert_eq!(
            a.frames
                .lock()
                .unwrap()
                .iter()
                .filter(|(f, _)| matches!(f.data, FrameData::LlmUpdateSettings(_)))
                .count(),
            4
        );
        assert_eq!(
            b.frames
                .lock()
                .unwrap()
                .iter()
                .filter(|(f, _)| matches!(f.data, FrameData::LlmUpdateSettings(_)))
                .count(),
            2
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_handles_setup_error_without_sending_work_to_failed_service() {
    let mut a = Service::new("a");
    Arc::get_mut(&mut a).unwrap().fail_setup = true;
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    let mut selected = s.watch_selection();
    selection(&mut selected, 1).await;
    text(&mut worker, "hello").await;
    assert_eq!(a.count_text("hello"), 0);
    assert_eq!(b.count_text("hello"), 1);
    finish(worker).await;
}

struct SlowService {
    ready: Arc<Notify>,
    released: Arc<Notify>,
    dropped: Arc<AtomicUsize>,
    cleaned: Arc<AtomicUsize>,
}
struct DropCount(Arc<AtomicUsize>);
impl Drop for DropCount {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for SlowService {
    fn name(&self) -> &str {
        "slow"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            let ready = self.ready.clone();
            let release = self.released.clone();
            let guard = DropCount(self.dropped.clone());
            let output = ctx.clone();
            ctx.spawn("slow-response", true, async move {
                let _guard = guard;
                ready.notify_one();
                release.notified().await;
                output
                    .push(Frame::text("stale"), Direction::Downstream)
                    .await
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn switching_cancels_owned_work_and_old_outputs_stay_fenced_after_reactivation() {
    let slow = Arc::new(SlowService {
        ready: Arc::new(Notify::new()),
        released: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicUsize::new(0)),
        cleaned: Arc::new(AtomicUsize::new(0)),
    });
    let b = Service::new("b");
    let s = Arc::new(ServiceSwitcher::new(vec![slow.clone(), b.clone()]).unwrap());
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::text("start")).await;
    slow.ready.notified().await;
    send(&worker, s.switch_frame(1).unwrap()).await;
    next_matching(
        &mut worker,
        |out| matches!(&out.frame.data,FrameData::ServiceMetadata(m) if m.service_name=="b"),
    )
    .await;
    text(&mut worker, "fresh").await;
    let mut selected = s.watch_selection();
    send(&worker, s.switch_frame(0).unwrap()).await;
    selection(&mut selected, 0).await;
    slow.released.notify_one();
    let report = finish(worker).await;
    assert!(report.errors.is_empty());
    assert_eq!(slow.dropped.load(Ordering::SeqCst), 1);
    assert_eq!(slow.cleaned.load(Ordering::SeqCst), 1);
}

#[test]
fn construction_rejects_empty_and_duplicate_service_identity() {
    assert!(ServiceSwitcher::new(vec![]).is_err());
    let a = Service::new("a");
    assert!(ServiceSwitcher::new(vec![a.clone(), a]).is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn failover_skips_unusable_members_and_wraps_only_after_explicit_recovery() {
    let a = Service::new("a");
    let b = Service::new("b");
    let c = Service::new("c");
    b.health.set_usable(false);
    let s = Arc::new(
        ServiceSwitcher::with_strategy(
            vec![a.clone(), b.clone(), c.clone()],
            ServiceSwitcherStrategy::Failover,
        )
        .unwrap(),
    );
    let mut selected = s.watch_selection();
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    a.error(ErrorCategory::Authentication, false).await;
    selection(&mut selected, 2).await;
    text(&mut worker, "on c").await;
    assert_eq!(c.count_text("on c"), 1);
    a.health.set_usable(true);
    c.error(ErrorCategory::Authorization, false).await;
    selection(&mut selected, 0).await;
    text(&mut worker, "on a").await;
    assert_eq!(a.count_text("on a"), 1);
    assert_eq!(b.count_text("on c"), 0);
    finish(worker).await;
}

struct OutsideError;
#[async_trait]
impl FrameProcessor for OutsideError {
    fn name(&self) -> &str {
        "a"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            ctx.push_error(ErrorFrame {
                category: ErrorCategory::Authentication,
                ..ErrorFrame::new("unrelated")
            })
            .await?;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unrelated_attributed_and_name_only_errors_cannot_trigger_failover() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
    let mut worker = Pipeline::new(vec![s.clone(), Arc::new(OutsideError)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::text("outside error")).await;
    let error = next_matching(&mut worker, |out| out.frame.data.as_error_frame().is_some()).await;
    assert_eq!(
        error.frame.data.as_error_frame().unwrap().error,
        "unrelated"
    );
    assert_eq!(s.active_index(), 0);
    assert!(a.health.is_usable());
    let forged = Frame::new(FrameData::Error(ErrorFrame {
        processor: Some("a".into()),
        category: ErrorCategory::Authentication,
        ..ErrorFrame::new("forged name")
    }));
    let id = forged.id;
    worker
        .sender()
        .send(forged, Direction::Upstream)
        .await
        .unwrap();
    let error = next_matching(&mut worker, |out| out.frame.id == id).await;
    assert!(error.frame.error_origin().is_none());
    assert_eq!(s.active_index(), 0);
    finish(worker).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn nested_switcher_interruption_keeps_future_parent_and_child_admissions_live() {
    let a = Service::new("a");
    let b = Service::new("b");
    let inner = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let c = Service::new("c");
    let outer = Arc::new(ServiceSwitcher::new(vec![inner.clone(), c.clone()]).unwrap());
    let mut selected = outer.watch_selection();
    let mut worker = Pipeline::new(vec![outer.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    text(&mut worker, "inner before").await;
    send(&worker, outer.switch_frame(1).unwrap()).await;
    selection(&mut selected, 1).await;
    text(&mut worker, "outer backup").await;
    send(&worker, outer.switch_frame(0).unwrap()).await;
    selection(&mut selected, 0).await;
    text(&mut worker, "inner after").await;
    let mut inner_selected = inner.watch_selection();
    send(&worker, inner.switch_frame(1).unwrap()).await;
    selection(&mut inner_selected, 1).await;
    // A nested selection notification is local; wait until its interruption
    // reaches the root boundary before admitting another root input.
    next_matching(&mut worker, |out| {
        out.direction == Direction::Upstream && matches!(out.frame.data, FrameData::Interruption)
    })
    .await;
    text(&mut worker, "inner backup").await;
    assert_eq!(a.count_text("inner after"), 1);
    assert_eq!(b.count_text("inner backup"), 1);
    assert_eq!(c.count_text("outer backup"), 1);
    finish(worker).await;
    assert_eq!(a.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(b.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(c.cleanup.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn drop_cancels_owned_service_tasks_and_cleans_inactive_members() {
    let slow = Arc::new(SlowService {
        ready: Arc::new(Notify::new()),
        released: Arc::new(Notify::new()),
        dropped: Arc::new(AtomicUsize::new(0)),
        cleaned: Arc::new(AtomicUsize::new(0)),
    });
    let b = Service::new("b");
    let s = Arc::new(ServiceSwitcher::new(vec![slow.clone(), b.clone()]).unwrap());
    let worker = Pipeline::new(vec![s])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::text("start")).await;
    slow.ready.notified().await;
    drop(worker);
    tokio::time::timeout(Duration::from_secs(3), async {
        while slow.cleaned.load(Ordering::SeqCst) != 1 || b.cleanup.load(Ordering::SeqCst) != 1 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(slow.dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn fatal_member_error_still_fails_parent_and_cleans_every_member() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Failover);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    a.error(ErrorCategory::Server, true).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        while worker.recv().await.is_some() {}
    })
    .await
    .unwrap();
    assert!(worker.join().await.is_err());
    assert_eq!(s.active_index(), 0);
    assert_eq!(a.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(b.cleanup.load(Ordering::SeqCst), 1);
}

#[test]
fn pinned_source_contract_explicitly_has_no_llm_context_replay() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/switching_contract.json")).unwrap();
    assert_eq!(
        fixture["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(fixture["llm_context_replay_on_switch"], false);
    assert_eq!(
        fixture["metadata_refresh"],
        "request service metadata, no inference"
    );
    assert_eq!(fixture["source_sha256"].as_object().unwrap().len(), 6);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stopped_switcher_retains_resources_until_explicit_cleanup_then_cleans_once() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    send(&worker, Frame::new(FrameData::Stop)).await;
    assert_eq!(
        worker.collect().await.unwrap().1.state,
        WorkerState::Stopped
    );
    assert_eq!(a.cleanup.load(Ordering::SeqCst), 0);
    assert_eq!(b.cleanup.load(Ordering::SeqCst), 0);
    s.cleanup().await.unwrap();
    s.cleanup().await.unwrap();
    assert_eq!(a.cleanup.load(Ordering::SeqCst), 1);
    assert_eq!(b.cleanup.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upstream_manual_selection_and_work_keep_direction() {
    let a = Service::new("a");
    let b = Service::new("b");
    let s = switcher(&a, &b, ServiceSwitcherStrategy::Manual);
    let mut worker = Pipeline::new(vec![s.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker
        .sender()
        .send(s.switch_frame(1).unwrap(), Direction::Upstream)
        .await
        .unwrap();
    let mut selected = s.watch_selection();
    selection(&mut selected, 1).await;
    worker
        .sender()
        .send(Frame::text("upstream"), Direction::Upstream)
        .await
        .unwrap();
    let text = next_matching(
        &mut worker,
        |out| matches!(&out.frame.data, FrameData::Text(text) if text.text == "upstream"),
    )
    .await;
    assert_eq!(text.direction, Direction::Upstream);
    assert_eq!(a.count_text("upstream"), 0);
    assert_eq!(b.count_text("upstream"), 1);
    finish(worker).await;
}
