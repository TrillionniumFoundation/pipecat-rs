//! Native lifecycle and timing witnesses for the pinned idle-frame contract.
use pipecat_core::*;
use pipecat_processors::*;
use std::{
    future::pending,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::Notify;

fn config() -> IdleFrameProcessorConfig {
    IdleFrameProcessorConfig {
        idle_timeout: Duration::from_secs(5),
        callback_timeout: Duration::from_secs(10),
    }
}
fn counter(calls: &Arc<AtomicUsize>) -> IdleCallback {
    let calls = calls.clone();
    Arc::new(move || {
        calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async { Ok(()) })
    })
}
async fn settle() {
    for _ in 0..32 {
        tokio::task::yield_now().await;
    }
}
async fn advance(seconds: u64) {
    tokio::time::advance(Duration::from_secs(seconds)).await;
    settle().await;
}
async fn start(processor: Arc<IdleFrameProcessor>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(Default::default())
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(100), worker.recv())
            .await
            .unwrap()
            .unwrap()
            .frame
            .data
            .is_start()
    );
    settle().await;
    worker
}
async fn pass(worker: &mut PipelineWorker, mut frame: Frame, direction: Direction) {
    let tag = LocalFrameTag::new();
    tag.mark(&mut frame).unwrap();
    frame
        .metadata
        .insert("sentinel".into(), serde_json::json!(["unchanged", 3]));
    frame.pts = Some(123);
    frame.transport_source = Some("origin".into());
    let expected = serde_json::to_value(&frame).unwrap();
    let id = frame.id;
    worker.sender().send(frame, direction).await.unwrap();
    loop {
        let actual = tokio::time::timeout(Duration::from_secs(100), worker.recv())
            .await
            .unwrap()
            .unwrap();
        if actual.frame.id == id {
            assert_eq!(actual.direction, direction);
            assert_eq!(serde_json::to_value(&actual.frame).unwrap(), expected);
            assert!(tag.contains(&actual.frame));
            break;
        }
        assert!(matches!(actual.frame.data, FrameData::Error(_)));
    }
    settle().await;
}
async fn finish(worker: PipelineWorker, terminal: FrameData) -> WorkerReport {
    worker
        .sender()
        .send(Frame::new(terminal), Direction::Downstream)
        .await
        .unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(100), worker.collect())
        .await
        .unwrap()
        .unwrap();
    report
}

#[tokio::test(start_paused = true)]
async fn all_frames_reset_both_directions_and_callbacks_repeat() {
    let calls = Arc::new(AtomicUsize::new(0));
    let processor = Arc::new(IdleFrameProcessor::new(config(), None, counter(&calls)).unwrap());
    let mut worker = start(processor).await;
    advance(4).await;
    pass(&mut worker, Frame::text("up"), Direction::Upstream).await;
    advance(4).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    pass(&mut worker, Frame::text("activity"), Direction::Downstream).await;
    advance(4).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    advance(1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    advance(5).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(finish(worker, FrameData::End(Default::default()))
        .await
        .errors
        .is_empty());
}

#[tokio::test(start_paused = true)]
async fn explicit_filter_preserves_nonmatches_and_interruption_preserves_monitor() {
    let calls = Arc::new(AtomicUsize::new(0));
    let filter: FramePredicate = Arc::new(|frame| matches!(frame.data, FrameData::Text(_)));
    let processor =
        Arc::new(IdleFrameProcessor::new(config(), Some(filter), counter(&calls)).unwrap());
    let mut worker = start(processor).await;
    advance(3).await;
    pass(&mut worker, Frame::interruption(), Direction::Downstream).await;
    advance(2).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    advance(3).await;
    pass(&mut worker, Frame::text("reset"), Direction::Upstream).await;
    advance(4).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    advance(1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(finish(worker, FrameData::Stop).await.errors.is_empty());
}

struct Dropped(Arc<AtomicUsize>);
impl Drop for Dropped {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test(start_paused = true)]
async fn callbacks_are_serial_activity_coalesces_and_interval_starts_after_completion() {
    let calls = Arc::new(AtomicUsize::new(0));
    let drops = Arc::new(AtomicUsize::new(0));
    let release = Arc::new(Notify::new());
    let callback: IdleCallback = {
        let calls = calls.clone();
        let drops = drops.clone();
        let release = release.clone();
        Arc::new(move || {
            calls.fetch_add(1, Ordering::SeqCst);
            let guard = Dropped(drops.clone());
            let release = release.clone();
            Box::pin(async move {
                let _guard = guard;
                release.notified().await;
                Ok(())
            })
        })
    };
    let processor = Arc::new(IdleFrameProcessor::new(config(), None, callback).unwrap());
    let mut worker = start(processor).await;
    advance(5).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    for _ in 0..8 {
        pass(&mut worker, Frame::text("activity"), Direction::Downstream).await;
    }
    advance(6).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    release.notify_one();
    settle().await;
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    advance(4).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    advance(1).await;
    assert_eq!(calls.load(Ordering::SeqCst), 2);
    assert!(finish(worker, FrameData::Stop).await.errors.is_empty());
    assert_eq!(drops.load(Ordering::SeqCst), 2);
}

fn terminals() -> Vec<FrameData> {
    vec![
        FrameData::End(Default::default()),
        FrameData::EndDetailed(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(Default::default()),
    ]
}
#[tokio::test(start_paused = true)]
async fn every_terminal_cancels_waiting_or_active_callback_and_allows_joined_reuse() {
    for terminal in terminals() {
        for active in [false, true] {
            let calls = Arc::new(AtomicUsize::new(0));
            let drops = Arc::new(AtomicUsize::new(0));
            let callback: IdleCallback = {
                let calls = calls.clone();
                let drops = drops.clone();
                Arc::new(move || {
                    calls.fetch_add(1, Ordering::SeqCst);
                    let guard = Dropped(drops.clone());
                    Box::pin(async move {
                        let _guard = guard;
                        pending::<()>().await;
                        Ok(())
                    })
                })
            };
            let processor = Arc::new(IdleFrameProcessor::new(config(), None, callback).unwrap());
            let worker = start(processor.clone()).await;
            if active {
                advance(5).await;
            }
            assert!(finish(worker, terminal.clone()).await.errors.is_empty());
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(active));
            assert_eq!(drops.load(Ordering::SeqCst), usize::from(active));
            let worker = start(processor).await;
            advance(5).await;
            assert_eq!(calls.load(Ordering::SeqCst), usize::from(active) + 1);
            assert!(finish(worker, FrameData::End(Default::default()))
                .await
                .errors
                .is_empty());
            assert_eq!(drops.load(Ordering::SeqCst), usize::from(active) + 1);
        }
    }
}

#[tokio::test(start_paused = true)]
async fn callback_error_construction_panic_poll_panic_and_timeout_stop_once() {
    for mode in 0..4 {
        let calls = Arc::new(AtomicUsize::new(0));
        let callback: IdleCallback = {
            let calls = calls.clone();
            Arc::new(move || {
                calls.fetch_add(1, Ordering::SeqCst);
                assert_ne!(mode, 1, "private construction panic");
                Box::pin(async move {
                    assert_ne!(mode, 2, "private poll panic");
                    if mode == 3 {
                        pending::<()>().await;
                    }
                    Err(ProcessorError::new("private callback error"))
                })
            })
        };
        let processor = Arc::new(IdleFrameProcessor::new(config(), None, callback).unwrap());
        let mut worker = start(processor.clone()).await;
        advance(5).await;
        if mode == 3 {
            advance(10).await;
        }
        pass(&mut worker, Frame::text("activity"), Direction::Downstream).await;
        advance(30).await;
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        let report = finish(worker, FrameData::End(Default::default())).await;
        assert_eq!(report.errors.len(), 1);
        let diagnostic = format!("{:?}", report.errors);
        assert!(!diagnostic.contains("private"));
        assert!(diagnostic.contains(if mode == 0 {
            "callback failed"
        } else if mode == 3 {
            "callback timed out"
        } else {
            "callback panicked"
        }));
        let worker = start(processor).await;
        advance(5).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        finish(worker, FrameData::Cancel(Default::default())).await;
    }
}

#[test]
fn validates_positive_bounded_durations() {
    let callback: IdleCallback = Arc::new(|| Box::pin(async { Ok(()) }));
    for invalid in [Duration::ZERO, Duration::from_secs(86401)] {
        let mut cfg = config();
        cfg.idle_timeout = invalid;
        assert!(IdleFrameProcessor::new(cfg, None, callback.clone()).is_err());
    }
    for invalid in [Duration::ZERO, Duration::from_secs(61)] {
        let mut cfg = config();
        cfg.callback_timeout = invalid;
        assert!(IdleFrameProcessor::new(cfg, None, callback.clone()).is_err());
    }
    for valid in [Duration::from_nanos(1), Duration::from_secs(60)] {
        let cfg = IdleFrameProcessorConfig {
            idle_timeout: Duration::from_secs(86400),
            callback_timeout: valid,
        };
        assert!(IdleFrameProcessor::new(cfg, None, callback.clone()).is_ok());
    }
}

struct OccupyTaskSlot {
    inner: Arc<IdleFrameProcessor>,
}
#[async_trait]
impl FrameProcessor for OccupyTaskSlot {
    fn name(&self) -> &str {
        "OccupyTaskSlot"
    }
    async fn setup(
        &self,
        config: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(config, ctx).await?;
        ctx.spawn("occupied", false, async {
            pending::<()>().await;
            Ok(())
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.process(frame, direction, ctx).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.inner.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
#[tokio::test(start_paused = true)]
async fn task_capacity_failure_returns_startup_error_and_leaves_no_latent_monitor() {
    let calls = Arc::new(AtomicUsize::new(0));
    let processor = Arc::new(IdleFrameProcessor::new(config(), None, counter(&calls)).unwrap());
    let result = Pipeline::new(vec![Arc::new(OccupyTaskSlot {
        inner: processor.clone(),
    })])
    .start(PipelineConfig {
        max_background_tasks: 1,
        ..Default::default()
    })
    .await;
    let error = match result {
        Err(error) => error,
        Ok(worker) => {
            finish(worker, FrameData::Cancel(Default::default())).await;
            panic!("expected task-capacity startup failure");
        }
    };
    assert!(format!("{error:?}").contains("background task limit"));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let worker = start(processor).await;
    advance(5).await;
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(finish(worker, FrameData::End(Default::default()))
        .await
        .errors
        .is_empty());
}

#[tokio::test(start_paused = true)]
async fn native_continuation_carrier_keeps_identity_and_authority_in_both_directions() {
    let calls = Arc::new(AtomicUsize::new(0));
    let processor = Arc::new(
        IdleFrameProcessor::new(config(), Some(Arc::new(|_| false)), counter(&calls)).unwrap(),
    );
    let mut worker = start(processor).await;
    for direction in [Direction::Upstream, Direction::Downstream] {
        let carrier =
            Arc::new(NativeLlmContinuation::start(42, LlmContinuationProvider::Anthropic).unwrap());
        let mut frame = Frame::new(FrameData::LlmResponseStart);
        frame.set_native_llm_continuation(carrier.clone()).unwrap();
        let id = frame.id;
        worker.sender().send(frame, direction).await.unwrap();
        let actual = tokio::time::timeout(Duration::from_secs(100), worker.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(actual.frame.id, id);
        assert_eq!(actual.direction, direction);
        assert!(Arc::ptr_eq(
            actual.frame.native_llm_continuation().unwrap().unwrap(),
            &carrier
        ));
    }
    assert!(finish(worker, FrameData::End(Default::default()))
        .await
        .errors
        .is_empty());
}
