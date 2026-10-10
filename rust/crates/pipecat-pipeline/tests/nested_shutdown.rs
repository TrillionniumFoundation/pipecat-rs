//! Bidirectional shutdown probes use gates to force late admitted work and cleanup.
use pipecat_core::*;
use pipecat_pipeline::*;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;
struct EndObserved {
    inner: Arc<dyn FrameProcessor>,
    end_done: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for EndObserved {
    fn name(&self) -> &str {
        "end-observed-inner"
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.inner.setup(start, ctx).await
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let terminal = frame.is_terminal();
        let result = self.inner.process(frame, direction, ctx).await;
        if terminal {
            self.end_done.add_permits(1);
        }
        result
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
struct Gate {
    entered: Arc<Semaphore>,
    end_done: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "opposite-direction-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) && direction == Direction::Upstream {
            self.entered.add_permits(1);
            self.end_done.acquire().await.unwrap().forget();
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admitted_upstream_survives_nested_terminal_staging() {
    let end_done = Arc::new(Semaphore::new(0));
    let entered = Arc::new(Semaphore::new(0));
    let inner =
        Arc::new(SyncParallelPipeline::new(vec![vec![Arc::new(IdentityProcessor)]]).unwrap());
    let outer = ParallelPipeline::new(vec![
        vec![
            Arc::new(EndObserved {
                inner,
                end_done: end_done.clone(),
            }),
            Arc::new(Gate {
                entered: entered.clone(),
                end_done,
            }),
        ],
        vec![Arc::new(IdentityProcessor)],
    ])
    .unwrap();
    let mut worker = Pipeline::new(vec![Arc::new(outer)])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    let input = Frame::text("upstream");
    let id = input.id;
    worker
        .sender()
        .send(input, Direction::Upstream)
        .await
        .unwrap();
    entered.acquire().await.unwrap().forget();
    assert_eq!(worker.recv().await.unwrap().frame.id, id);
    worker.end().await.unwrap();
    let result = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap();
    let (_, report) = result.unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
struct FlushReverse {
    background: bool,
    release: Arc<Semaphore>,
    output: Frame,
}
#[async_trait]
impl FrameProcessor for FlushReverse {
    fn name(&self) -> &str {
        "flush-reverse-after-nested-end"
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.background {
            self.release.add_permits(1);
            Ok(())
        } else {
            ctx.push(self.output.clone(), Direction::Upstream).await
        }
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if self.background && matches!(&frame.data,FrameData::Text(t) if t.text=="arm") {
            let output = self.output.clone();
            let ctx = ctx.clone();
            let release = self.release.clone();
            ctx.clone().spawn("reverse-end-flush", false, async move {
                release.acquire().await.unwrap().forget();
                ctx.push(output, Direction::Upstream).await
            })?;
        }
        ctx.push(frame, direction).await
    }
}
fn nested(sync: bool, leaf: Arc<dyn FrameProcessor>) -> Arc<dyn FrameProcessor> {
    if sync {
        Arc::new(SyncParallelPipeline::new(vec![vec![leaf]]).unwrap())
    } else {
        Arc::new(ParallelPipeline::new(vec![vec![leaf]]).unwrap())
    }
}
async fn reverse_flush(background: bool) {
    for sync in [false, true] {
        for stop in [false, true] {
            let inner = nested(sync, Arc::new(IdentityProcessor));
            let output = Frame::text("flush-upstream");
            let id = output.id;
            let flush = Arc::new(FlushReverse {
                background,
                release: Arc::new(Semaphore::new(0)),
                output,
            });
            let mut worker = Pipeline::new(vec![inner.clone(), flush])
                .start(PipelineConfig::default())
                .await
                .unwrap();
            worker.recv().await.unwrap();
            if background {
                worker
                    .sender()
                    .send(Frame::text("arm"), Direction::Downstream)
                    .await
                    .unwrap();
                worker.recv().await.unwrap();
            }
            let terminal = if stop {
                Frame::new(FrameData::Stop)
            } else {
                Frame::end()
            };
            let terminal_id = terminal.id;
            worker
                .sender()
                .send(terminal, Direction::Downstream)
                .await
                .unwrap();
            let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(frames.len(), 2);
            assert_eq!(frames[0].frame.id, id);
            assert_eq!(frames[0].direction, Direction::Upstream);
            assert_eq!(frames[1].frame.id, terminal_id);
            assert_eq!(
                report.state,
                if stop {
                    WorkerState::Stopped
                } else {
                    WorkerState::Ended
                }
            );
            if stop {
                inner.cleanup().await.unwrap();
            }
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downstream_prepare_end_reverse_output_drains_before_final_terminal() {
    reverse_flush(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn downstream_owned_task_reverse_output_drains_before_final_terminal() {
    reverse_flush(true).await;
}

struct CleanupGate {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    cleaned: std::sync::atomic::AtomicUsize,
    context: std::sync::Mutex<Option<ProcessorContext>>,
    fail: bool,
}
#[async_trait]
impl FrameProcessor for CleanupGate {
    fn name(&self) -> &str {
        "nested-cleanup-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.context.lock().unwrap() = Some(ctx.clone());
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.cleaned
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.fail {
            Err(ProcessorError::new("descendant cleanup failed"))
        } else {
            Ok(())
        }
    }
}
fn cleanup_gate(fail: bool) -> Arc<CleanupGate> {
    Arc::new(CleanupGate {
        entered: Arc::new(Semaphore::new(0)),
        release: Arc::new(Semaphore::new(0)),
        cleaned: std::sync::atomic::AtomicUsize::new(0),
        context: std::sync::Mutex::new(None),
        fail,
    })
}

#[derive(Default)]
struct RootContext(std::sync::Mutex<Option<ProcessorContext>>);
#[async_trait]
impl FrameProcessor for RootContext {
    fn name(&self) -> &str {
        "sealed-root-context"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        *self.0.lock().unwrap() = Some(ctx.clone());
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn final_end_waits_for_descendant_cleanup_and_cancel_can_preempt_it() {
    for cancel in [false, true] {
        let gate = cleanup_gate(false);
        let outer = nested(false, nested(true, gate.clone()));
        let root_context = Arc::new(RootContext::default());
        let mut worker = Pipeline::new(vec![root_context.clone(), outer])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        worker.recv().await.unwrap();
        worker.end().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), gate.entered.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        assert!(!worker.has_finished());
        assert_eq!(
            worker
                .sender()
                .send(Frame::text("external-after-cutoff"), Direction::Downstream)
                .await,
            Err(PipelineError::Closed)
        );
        let context = root_context.0.lock().unwrap().clone().unwrap();
        assert!(context
            .spawn("after-seal", false, async { Ok(()) })
            .is_err());
        assert!(context
            .queue(Frame::text("after-seal"), Direction::Upstream)
            .await
            .is_err());
        if cancel {
            worker
                .cancel("cancel while descendant cleanup waits")
                .await
                .unwrap();
        }
        gate.release.add_permits(1);
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            report.state,
            if cancel {
                WorkerState::Cancelled
            } else {
                WorkerState::Ended
            }
        );
        assert_eq!(frames.iter().filter(|f| f.frame.is_terminal()).count(), 1);
        assert_eq!(gate.cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn descendant_cleanup_failure_prevents_successful_end() {
    let gate = cleanup_gate(true);
    gate.release.add_permits(1);
    let mut worker = Pipeline::new(vec![nested(false, nested(true, gate))])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    let mut frames = vec![];
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            frames.push(frame)
        }
    })
    .await
    .unwrap();
    assert!(!frames.iter().any(|f| f.frame.is_terminal()));
    assert!(
        matches!(worker.join().await,Err(PipelineError::Execution(message)) if message.contains("cleanup"))
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_stop_retains_resources_until_explicit_cleanup() {
    let gate = cleanup_gate(false);
    gate.release.add_permits(1);
    let outer = nested(false, nested(true, gate.clone()));
    let mut worker = Pipeline::new(vec![outer.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
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
    assert_eq!(gate.cleaned.load(std::sync::atomic::Ordering::SeqCst), 0);
    tokio::time::timeout(Duration::from_secs(3), outer.cleanup())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gate.cleaned.load(std::sync::atomic::Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn root_shutdown_deadline_bounds_blocked_descendant_cleanup() {
    let gate = cleanup_gate(false);
    let outer = nested(false, nested(true, gate.clone()));
    let config = PipelineConfig {
        shutdown_timeout: Duration::from_millis(150),
        ..PipelineConfig::default()
    };
    let mut worker = Pipeline::new(vec![outer]).start(config).await.unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert!(!worker.has_finished());
    let result = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap();
    assert!(
        matches!(result, Err(PipelineError::ShutdownTimeout)),
        "{result:?}"
    );
    // Allow the descendant's own bounded cleanup to finish as well.
    gate.release.add_permits(1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dropping_root_during_descendant_cleanup_releases_owned_monitors() {
    let gate = cleanup_gate(false);
    let weak = Arc::downgrade(&gate);
    let outer = nested(false, nested(true, gate.clone()));
    let weak_outer = Arc::downgrade(&outer);
    let mut worker = Pipeline::new(vec![outer.clone()])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), gate.entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    drop(worker);
    gate.release.add_permits(1);
    gate.context.lock().unwrap().take();
    drop(gate);
    drop(outer);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak.upgrade().is_some() || weak_outer.upgrade().is_some() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

struct CleanupLifetime(Arc<std::sync::atomic::AtomicUsize>);
impl Drop for CleanupLifetime {
    fn drop(&mut self) {
        self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    }
}
struct NeverCleanup {
    entered: Arc<Semaphore>,
    released: Arc<std::sync::atomic::AtomicUsize>,
}
#[async_trait]
impl FrameProcessor for NeverCleanup {
    fn name(&self) -> &str {
        "bounded-never-completing-cleanup"
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
        let _lifetime = CleanupLifetime(self.released.clone());
        self.entered.add_permits(1);
        std::future::pending().await
    }
}
struct LifetimeObserver;
#[async_trait]
impl PipelineObserver for LifetimeObserver {
    async fn on_event(&self, _: ObservationEvent) {}
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_cleanup_releases_descendants_and_observers_at_child_deadline_without_manual_release(
) {
    let entered = Arc::new(Semaphore::new(0));
    let released = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let leaf = Arc::new(NeverCleanup {
        entered: entered.clone(),
        released: released.clone(),
    });
    let weak_leaf = Arc::downgrade(&leaf);
    let outer = Arc::new(
        ParallelPipeline::with_config(
            vec![vec![leaf.clone()]],
            ParallelPipelineConfig {
                branch: PipelineConfig {
                    shutdown_timeout: Duration::from_millis(300),
                    ..PipelineConfig::default()
                },
                ..ParallelPipelineConfig::default()
            },
        )
        .unwrap(),
    );
    let weak_outer = Arc::downgrade(&outer);
    let root_context = Arc::new(RootContext::default());
    let observer = Arc::new(LifetimeObserver);
    let weak_observer = Arc::downgrade(&observer);
    let mut worker = Pipeline::new(vec![root_context.clone(), outer.clone()])
        .with_observer(observer.clone(), ObserverOptions::default())
        .start(PipelineConfig {
            shutdown_timeout: Duration::from_millis(100),
            ..PipelineConfig::default()
        })
        .await
        .unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), entered.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let context = root_context.0.lock().unwrap().take().unwrap();
    assert!(context
        .push(Frame::text("post-seal-effect"), Direction::Downstream)
        .await
        .is_err());
    assert!(context
        .spawn("post-seal-task", false, async { Ok(()) })
        .is_err());
    drop(context);
    assert!(matches!(
        tokio::time::timeout(Duration::from_secs(2), worker.collect())
            .await
            .unwrap(),
        Err(PipelineError::ShutdownTimeout)
    ));
    root_context.0.lock().unwrap().take();
    drop(root_context);
    drop(outer);
    drop(leaf);
    drop(observer);
    tokio::time::timeout(Duration::from_secs(3), async {
        while weak_leaf.upgrade().is_some()
            || weak_outer.upgrade().is_some()
            || weak_observer.upgrade().is_some()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(released.load(std::sync::atomic::Ordering::SeqCst), 1);
}
