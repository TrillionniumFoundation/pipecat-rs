use pipecat_core::*;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(Duration::from_secs(2), worker.recv())
        .await
        .expect("frame timeout")
        .expect("output closed")
}
async fn join(worker: &mut PipelineWorker) -> Result<WorkerReport, PipelineError> {
    tokio::time::timeout(Duration::from_secs(2), worker.join())
        .await
        .expect("worker hung")
}
async fn started(processors: Vec<Arc<dyn FrameProcessor>>) -> PipelineWorker {
    let mut worker = Pipeline::new(processors)
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Start(_)
    ));
    worker
}
fn text(item: DirectedFrame) -> String {
    match item.frame.data {
        FrameData::Text(text) => text.text,
        other => panic!("expected text, got {other:?}"),
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn linear_pipeline_both_directions_and_graceful_end() {
    let mut config = PipelineConfig::default();
    config.start_metadata.insert("name".into(), "sample".into());
    let mut worker = Pipeline::new(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ])
    .start(config)
    .await
    .unwrap();
    let start = next(&mut worker).await;
    assert_eq!(start.frame.metadata["name"], "sample");
    for word in ["one", "two", "three"] {
        worker
            .sender()
            .send(Frame::text(word), Direction::Downstream)
            .await
            .unwrap();
    }
    for word in ["one", "two", "three"] {
        assert_eq!(text(next(&mut worker).await), word);
    }
    worker
        .sender()
        .send(Frame::text("up"), Direction::Upstream)
        .await
        .unwrap();
    let up = next(&mut worker).await;
    assert_eq!(up.direction, Direction::Upstream);
    assert_eq!(text(up), "up");
    worker.end().await.unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    assert_eq!(join(&mut worker).await.unwrap().state, WorkerState::Ended);
    assert!(matches!(
        worker
            .sender()
            .send(Frame::text("late"), Direction::Downstream)
            .await,
        Err(PipelineError::Closed)
    ));
}

struct Gate {
    entered: Notify,
    release: Semaphore,
    cleaned: AtomicUsize,
    dropped: Arc<AtomicBool>,
}
impl Gate {
    fn new() -> Self {
        Self {
            entered: Notify::new(),
            release: Semaphore::new(0),
            cleaned: AtomicUsize::new(0),
            dropped: Arc::new(AtomicBool::new(false)),
        }
    }
}
struct DropFlag(Arc<AtomicBool>);
impl Drop for DropFlag {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "blocked") {
            let _flag = DropFlag(self.dropped.clone());
            self.entered.notify_one();
            self.release.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_waits_for_active_and_queued_data() {
    let gate = Arc::new(Gate::new());
    let mut worker = started(vec![gate.clone()]).await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    gate.entered.notified().await;
    worker
        .sender()
        .send(Frame::text("queued"), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    assert!(!worker.has_finished());
    gate.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "blocked");
    assert_eq!(text(next(&mut worker).await), "queued");
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    assert_eq!(join(&mut worker).await.unwrap().state, WorkerState::Ended);
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_preempts_active_and_queued_frames_even_if_uninterruptible() {
    let gate = Arc::new(Gate::new());
    let mut worker = started(vec![gate.clone()]).await;
    worker
        .sender()
        .send(
            Frame::text("blocked").with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    gate.entered.notified().await;
    worker
        .sender()
        .send(
            Frame::text("queued").with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker.cancel("test").await.unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Cancel(_)
    ));
    assert_eq!(
        join(&mut worker).await.unwrap().state,
        WorkerState::Cancelled
    );
    assert!(gate.dropped.load(Ordering::SeqCst));
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_cancels_active_and_keeps_overrides_and_end() {
    let gate = Arc::new(Gate::new());
    let mut worker = started(vec![gate.clone()]).await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    gate.entered.notified().await;
    worker
        .sender()
        .send(
            Frame::text("keep").with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("drop"), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert_eq!(text(next(&mut worker).await), "keep");
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    let report = join(&mut worker).await.unwrap();
    assert_eq!(report.dropped_frames, 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_preserves_current_uninterruptible_frame() {
    let gate = Arc::new(Gate::new());
    let mut worker = started(vec![gate.clone()]).await;
    worker
        .sender()
        .send(
            Frame::text("blocked").with_interruptible(false),
            Direction::Downstream,
        )
        .await
        .unwrap();
    gate.entered.notified().await;
    worker
        .sender()
        .send(Frame::text("drop"), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert!(!gate.dropped.load(Ordering::SeqCst));
    gate.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "blocked");
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    assert_eq!(join(&mut worker).await.unwrap().dropped_frames, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_normal_queue_does_not_block_cancel_or_waiting_producers() {
    let gate = Arc::new(Gate::new());
    let config = PipelineConfig {
        queue_capacity: 1,
        ..PipelineConfig::default()
    };
    let mut worker = Pipeline::new(vec![gate.clone()])
        .start(config)
        .await
        .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    gate.entered.notified().await;
    worker
        .sender()
        .send(Frame::text("queued"), Direction::Downstream)
        .await
        .unwrap();
    let sender = worker.sender();
    let blocked = tokio::spawn(async move {
        sender
            .send(Frame::text("blocked sender"), Direction::Downstream)
            .await
    });
    tokio::task::yield_now().await;
    assert!(!blocked.is_finished());
    worker.cancel("cancel while full").await.unwrap();
    join(&mut worker).await.unwrap();
    assert_eq!(
        tokio::time::timeout(Duration::from_secs(1), blocked)
            .await
            .unwrap()
            .unwrap(),
        Err(PipelineError::Closed)
    );
}

struct Failing {
    setup: bool,
}
#[async_trait]
impl FrameProcessor for Failing {
    fn name(&self) -> &str {
        if self.setup {
            "setup failure"
        } else {
            "process failure"
        }
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.setup {
            Err(ProcessorError::new("bad setup"))
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
        if matches!(frame.data, FrameData::Text(_)) {
            Err(ProcessorError::new("bad frame"))
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn errors_travel_upstream_and_are_not_swallowed() {
    let mut worker = started(vec![
        Arc::new(IdentityProcessor),
        Arc::new(Failing { setup: false }),
    ])
    .await;
    worker
        .sender()
        .send(Frame::text("bad"), Direction::Downstream)
        .await
        .unwrap();
    let error = next(&mut worker).await;
    assert_eq!(error.direction, Direction::Upstream);
    assert!(
        matches!(error.frame.data, FrameData::Error(error) if error.error == "bad frame" && error.processor.as_deref() == Some("process failure"))
    );
    worker.end().await.unwrap();
    let report = join(&mut worker).await.unwrap();
    assert_eq!(report.errors.len(), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn each_setup_failure_is_reported() {
    let mut worker = started(vec![
        Arc::new(Failing { setup: true }),
        Arc::new(Failing { setup: true }),
    ])
    .await;
    for _ in 0..2 {
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::Error(_)
        ));
    }
    worker.end().await.unwrap();
    assert_eq!(join(&mut worker).await.unwrap().errors.len(), 2);
}

struct BlockAllText;
#[async_trait]
impl FrameProcessor for BlockAllText {
    fn name(&self) -> &str {
        "block text"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            std::future::pending::<()>().await;
        }
        ctx.push(frame, direction).await
    }
}
struct Flood {
    own: bool,
}
#[async_trait]
impl FrameProcessor for Flood {
    fn name(&self) -> &str {
        "flood"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            for i in 0..3 {
                if self.own {
                    ctx.queue(Frame::text(i.to_string()), direction).await?;
                } else {
                    ctx.push(Frame::text(i.to_string()), direction).await?;
                }
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn internal_and_self_backpressure_fail_instead_of_deadlock() {
    for own in [false, true] {
        let mut processors: Vec<Arc<dyn FrameProcessor>> = vec![Arc::new(Flood { own })];
        if !own {
            processors.push(Arc::new(BlockAllText));
        }
        let mut worker = Pipeline::new(processors)
            .start(PipelineConfig {
                queue_capacity: 1,
                ..PipelineConfig::default()
            })
            .await
            .unwrap();
        next(&mut worker).await;
        worker
            .sender()
            .send(Frame::text("flood"), Direction::Downstream)
            .await
            .unwrap();
        assert!(join(&mut worker).await.is_err());
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn output_backpressure_is_explicit_even_without_an_output_consumer() {
    let mut worker = Pipeline::new(vec![])
        .start(PipelineConfig {
            output_capacity: 1,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("overflow"), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        join(&mut worker).await,
        Err(PipelineError::Backpressure(_))
    ));
}

struct Background {
    entered: Arc<Notify>,
    release: Arc<Semaphore>,
    dropped: Arc<AtomicBool>,
    interruptible: bool,
    fail: bool,
}
#[async_trait]
impl FrameProcessor for Background {
    fn name(&self) -> &str {
        "background"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmRun) {
            let ctx2 = ctx.clone();
            let entered = self.entered.clone();
            let release = self.release.clone();
            let dropped = self.dropped.clone();
            let fail = self.fail;
            ctx.spawn("delayed output", self.interruptible, async move {
                let _flag = DropFlag(dropped);
                entered.notify_one();
                release.acquire().await.unwrap().forget();
                if fail {
                    return Err(ProcessorError::new("background failed"));
                }
                ctx2.push(Frame::text("background result"), Direction::Downstream)
                    .await
            })?;
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
fn background(interruptible: bool, fail: bool) -> Arc<Background> {
    Arc::new(Background {
        entered: Arc::new(Notify::new()),
        release: Arc::new(Semaphore::new(0)),
        dropped: Arc::new(AtomicBool::new(false)),
        interruptible,
        fail,
    })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_drains_background_work_before_forwarding_terminal() {
    let background = background(true, false);
    let mut worker = started(vec![background.clone()]).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    background.entered.notified().await;
    worker.end().await.unwrap();
    tokio::task::yield_now().await;
    assert!(!worker.has_finished());
    background.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "background result");
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    join(&mut worker).await.unwrap();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_cancels_background_even_while_end_is_draining() {
    let background = background(true, false);
    let mut worker = started(vec![background.clone()]).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    background.entered.notified().await;
    worker.end().await.unwrap();
    tokio::task::yield_now().await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    join(&mut worker).await.unwrap();
    assert!(background.dropped.load(Ordering::SeqCst));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn background_errors_reach_upstream() {
    let background = background(true, true);
    let mut worker = started(vec![background.clone()]).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    background.entered.notified().await;
    background.release.add_permits(1);
    let error = next(&mut worker).await;
    assert!(
        matches!(error.frame.data, FrameData::Error(error) if error.error == "background failed")
    );
    worker.end().await.unwrap();
    assert_eq!(join(&mut worker).await.unwrap().errors.len(), 1);
}

struct Broadcast;
#[async_trait]
impl FrameProcessor for Broadcast {
    fn name(&self) -> &str {
        "broadcast"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            ctx.broadcast(frame).await
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broadcasts_have_distinct_paired_ids() {
    let mut worker = started(vec![
        Arc::new(IdentityProcessor),
        Arc::new(Broadcast),
        Arc::new(IdentityProcessor),
    ])
    .await;
    worker
        .sender()
        .send(Frame::text("hi"), Direction::Downstream)
        .await
        .unwrap();
    let first = next(&mut worker).await;
    let second = next(&mut worker).await;
    assert_ne!(first.direction, second.direction);
    assert_ne!(first.frame.id, second.frame.id);
    assert_eq!(first.frame.broadcast_sibling_id, Some(second.frame.id));
    assert_eq!(second.frame.broadcast_sibling_id, Some(first.frame.id));
    worker.end().await.unwrap();
    join(&mut worker).await.unwrap();
}

struct Trace(Mutex<Vec<Direction>>);
#[async_trait]
impl FrameProcessor for Trace {
    fn name(&self) -> &str {
        "trace"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Flush(_)) {
            self.0.lock().unwrap().push(direction);
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn flush_makes_a_down_up_down_roundtrip() {
    let trace = Arc::new(Trace(Mutex::new(vec![])));
    let mut worker = started(vec![trace.clone()]).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::Flush(FlushFrame {
                returning: false,
                origin: None,
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Flush(FlushFrame {
            returning: true,
            ..
        })
    ));
    assert_eq!(
        *trace.0.lock().unwrap(),
        vec![
            Direction::Downstream,
            Direction::Upstream,
            Direction::Downstream
        ]
    );
    worker.end().await.unwrap();
    join(&mut worker).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn paused_normal_lane_can_be_resumed_while_a_sender_waits() {
    let mut worker = Pipeline::new(vec![Arc::new(IdentityProcessor)])
        .start(PipelineConfig {
            queue_capacity: 1,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(
            Frame::new(FrameData::ProcessorPauseUrgent {
                processor: "IdentityProcessor".into(),
            }),
            Direction::Downstream,
        )
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::ProcessorPauseUrgent { .. }
    ));
    worker
        .sender()
        .send(Frame::text("first"), Direction::Downstream)
        .await
        .unwrap();
    let sender = worker.sender();
    let waiting = tokio::spawn(async move {
        sender
            .send(Frame::text("second"), Direction::Downstream)
            .await
    });
    worker
        .sender()
        .send(
            Frame::new(FrameData::ProcessorResume {
                processor: "IdentityProcessor".into(),
            }),
            Direction::Downstream,
        )
        .await
        .unwrap();
    let mut texts = vec![];
    for _ in 0..3 {
        if let FrameData::Text(text) = next(&mut worker).await.frame.data {
            texts.push(text.text);
        }
    }
    assert_eq!(texts, vec!["first", "second"]);
    waiting.await.unwrap().unwrap();
    worker.end().await.unwrap();
    join(&mut worker).await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_skips_cleanup_but_end_and_cancel_do_not() {
    let gate = Arc::new(Gate::new());
    let mut worker = started(vec![gate.clone()]).await;
    worker
        .sender()
        .send(Frame::new(FrameData::Stop), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(join(&mut worker).await.unwrap().state, WorkerState::Stopped);
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 0);
    gate.cleanup().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stuck_end_times_out_and_cleans_up() {
    let gate = Arc::new(Gate::new());
    let mut worker = Pipeline::new(vec![gate.clone()])
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_millis(20),
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    next(&mut worker).await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    gate.entered.notified().await;
    worker.end().await.unwrap();
    assert!(matches!(
        join(&mut worker).await,
        Err(PipelineError::ShutdownTimeout)
    ));
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

struct Broken {
    swallow_start: bool,
    background_panic: bool,
    cleanup_fails: bool,
}
#[async_trait]
impl FrameProcessor for Broken {
    fn name(&self) -> &str {
        "broken"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match frame.data {
            FrameData::Start(_) if self.swallow_start => Ok(()),
            FrameData::Text(_) if self.background_panic => {
                ctx.spawn("panic", true, async { panic!("background panic") })?;
                Ok(())
            }
            FrameData::Text(_) => panic!("processor panic"),
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.cleanup_fails {
            Err(ProcessorError::new("cleanup failure"))
        } else {
            Ok(())
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn processor_and_background_panics_reach_join() {
    for background_panic in [false, true] {
        let mut worker = started(vec![Arc::new(Broken {
            swallow_start: false,
            background_panic,
            cleanup_fails: false,
        })])
        .await;
        worker
            .sender()
            .send(Frame::text("panic"), Direction::Downstream)
            .await
            .unwrap();
        assert!(matches!(
            join(&mut worker).await,
            Err(PipelineError::ProcessorPanic(_))
        ));
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn startup_timeout_is_an_error_and_cleanup_errors_fail_join() {
    let result = Pipeline::new(vec![Arc::new(Broken {
        swallow_start: true,
        background_panic: false,
        cleanup_fails: false,
    })])
    .start(PipelineConfig {
        startup_timeout: Duration::from_millis(10),
        ..PipelineConfig::default()
    })
    .await;
    assert!(matches!(result, Err(PipelineError::StartupTimeout)));
    let mut worker = started(vec![Arc::new(Broken {
        swallow_start: false,
        background_panic: false,
        cleanup_fails: true,
    })])
    .await;
    worker.end().await.unwrap();
    assert!(
        matches!(join(&mut worker).await, Err(PipelineError::Execution(error)) if error.contains("cleanup failure"))
    );
    assert_eq!(worker.errors().len(), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn joining_is_cancellation_safe() {
    let mut worker = started(vec![Arc::new(IdentityProcessor)]).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(1), worker.join())
            .await
            .is_err()
    );
    worker.end().await.unwrap();
    assert_eq!(join(&mut worker).await.unwrap().state, WorkerState::Ended);
}

struct AfterStart(AtomicBool);
#[async_trait]
impl FrameProcessor for AfterStart {
    fn name(&self) -> &str {
        "after start"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let start = matches!(frame.data, FrameData::Start(_));
        ctx.push(frame, direction).await?;
        if start {
            tokio::time::sleep(Duration::from_millis(5)).await;
            self.0.store(true, Ordering::SeqCst);
        }
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_notification_does_not_cancel_the_start_handler() {
    let processor = Arc::new(AfterStart(AtomicBool::new(false)));
    let mut worker = started(vec![processor.clone()]).await;
    worker.end().await.unwrap();
    join(&mut worker).await.unwrap();
    assert!(processor.0.load(Ordering::SeqCst));
}

struct LifecycleTimer(Mutex<Option<u64>>);
#[async_trait]
impl FrameProcessor for LifecycleTimer {
    fn name(&self) -> &str {
        "timer"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::LlmRun) {
            let id = ctx.spawn("lifecycle timer", false, async {
                std::future::pending::<()>().await;
                Ok(())
            })?;
            *self.0.lock().unwrap() = Some(id);
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if let Some(id) = self.0.lock().unwrap().take() {
            assert!(ctx.cancel_task(id));
        }
        ctx.push(Frame::text("timer flushed"), Direction::Downstream)
            .await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prepare_end_can_cancel_lifecycle_timers_and_flush_before_drain() {
    let mut worker = started(vec![Arc::new(LifecycleTimer(Mutex::new(None)))]).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    assert_eq!(text(next(&mut worker).await), "timer flushed");
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::End(_)
    ));
    join(&mut worker).await.unwrap();
}

struct LifecyclePanic {
    setup: bool,
}
#[async_trait]
impl FrameProcessor for LifecyclePanic {
    fn name(&self) -> &str {
        "lifecycle panic"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.setup {
            panic!("setup panic");
        }
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
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if !self.setup {
            panic!("cleanup panic");
        }
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn setup_panics_report_errors_and_cleanup_panics_close_output() {
    let mut worker = started(vec![Arc::new(LifecyclePanic { setup: true })]).await;
    assert!(
        matches!(next(&mut worker).await.frame.data, FrameData::Error(error) if error.error.contains("setup panic"))
    );
    worker.end().await.unwrap();
    assert_eq!(join(&mut worker).await.unwrap().errors.len(), 1);
    let worker = started(vec![Arc::new(LifecyclePanic { setup: false })]).await;
    worker.end().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(1), worker.collect())
        .await
        .expect("cleanup panic left output open");
    assert!(matches!(result, Err(PipelineError::ProcessorPanic(_))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_keeps_opted_out_background_tasks_but_cancel_stops_them() {
    let background = background(false, false);
    let mut worker = started(vec![background.clone()]).await;
    worker
        .sender()
        .send(Frame::new(FrameData::LlmRun), Direction::Downstream)
        .await
        .unwrap();
    background.entered.notified().await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    assert!(!background.dropped.load(Ordering::SeqCst));
    worker.cancel("shutdown").await.unwrap();
    join(&mut worker).await.unwrap();
    assert!(background.dropped.load(Ordering::SeqCst));
}

struct PendingSetup {
    entered: Notify,
    cleaned: Notify,
    background_dropped: Arc<AtomicBool>,
}
#[async_trait]
impl FrameProcessor for PendingSetup {
    fn name(&self) -> &str {
        "pending setup"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let started = Arc::new(Notify::new());
        let started2 = started.clone();
        let dropped = self.background_dropped.clone();
        ctx.spawn("setup background", false, async move {
            let _flag = DropFlag(dropped);
            started2.notify_one();
            std::future::pending::<()>().await;
            Ok(())
        })?;
        started.notified().await;
        self.entered.notify_one();
        std::future::pending::<()>().await;
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
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        assert!(self.background_dropped.load(Ordering::SeqCst));
        self.cleaned.notify_one();
        Ok(())
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancelling_startup_aborts_setup_and_background_and_cleans_resources() {
    let processor = Arc::new(PendingSetup {
        entered: Notify::new(),
        cleaned: Notify::new(),
        background_dropped: Arc::new(AtomicBool::new(false)),
    });
    let pipeline = Pipeline::new(vec![processor.clone()]);
    let start = tokio::spawn(async move { pipeline.start(PipelineConfig::default()).await });
    processor.entered.notified().await;
    start.abort();
    assert!(matches!(start.await, Err(error) if error.is_cancelled()));
    tokio::time::timeout(Duration::from_secs(1), processor.cleaned.notified())
        .await
        .expect("startup cancellation leaked resources");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_history_is_bounded_and_counts_evictions_without_double_reporting() {
    for capacity in [0, 2] {
        let mut worker = Pipeline::new(vec![
            Arc::new(IdentityProcessor),
            Arc::new(Failing { setup: false }),
        ])
        .start(PipelineConfig {
            error_history_capacity: capacity,
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
        next(&mut worker).await;
        for _ in 0..5 {
            worker
                .sender()
                .send(Frame::text("error"), Direction::Downstream)
                .await
                .unwrap();
            assert!(matches!(
                next(&mut worker).await.frame.data,
                FrameData::Error(_)
            ));
        }
        assert_eq!(worker.total_errors(), 5);
        assert_eq!(worker.errors().len(), capacity);
        assert_eq!(worker.evicted_errors(), 5 - capacity as u64);
        worker.end().await.unwrap();
        let report = join(&mut worker).await.unwrap();
        assert_eq!(report.total_errors, 5);
        assert_eq!(report.errors.len(), capacity);
        assert_eq!(report.evicted_errors, 5 - capacity as u64);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn worker_end_and_cancel_requests_turn_around_and_control_pipeline() {
    for cancel in [false, true] {
        let mut worker = started(vec![
            Arc::new(IdentityProcessor),
            Arc::new(IdentityProcessor),
        ])
        .await;
        let reason = ReasonFrame {
            reason: Some(serde_json::json!({"requested_by": "test"})),
        };
        let data = if cancel {
            FrameData::CancelWorker(reason)
        } else {
            FrameData::EndWorker(reason)
        };
        worker
            .sender()
            .send(Frame::new(data), Direction::Downstream)
            .await
            .unwrap();
        let terminal = next(&mut worker).await;
        assert_eq!(terminal.direction, Direction::Downstream);
        assert!(matches!(
            terminal.frame.data,
            FrameData::EndDetailed(_) | FrameData::CancelDetailed(_)
        ));
        assert_eq!(
            join(&mut worker).await.unwrap().state,
            if cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn detailed_flush_signals_its_local_event_after_a_complete_roundtrip() {
    let mut worker = started(vec![
        Arc::new(IdentityProcessor),
        Arc::new(IdentityProcessor),
    ])
    .await;
    let event = LocalFlushEvent::default();
    worker
        .sender()
        .send(
            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                event: Some(event.clone()),
                returning: false,
                origin: Some("local".into()),
            })),
            Direction::Downstream,
        )
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(1), event.wait())
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::FlushDetailed(PipelineFlushFrame {
            returning: true,
            ..
        })
    ));
    worker.end().await.unwrap();
    join(&mut worker).await.unwrap();
}
