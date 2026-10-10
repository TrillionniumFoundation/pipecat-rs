use pipecat_core::*;
use pipecat_pipeline::ParallelPipeline;
use std::{sync::Arc, time::Duration};
use tokio::sync::Semaphore;

struct CleanupLatch {
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
    completed: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for CleanupLatch {
    fn name(&self) -> &str {
        "independent-cleanup-latch"
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
        self.entered.add_permits(1);
        self.release.acquire().await.unwrap().forget();
        self.completed.add_permits(1);
        Ok(())
    }
}
struct HoldCancel {
    inner: Arc<dyn FrameProcessor>,
    entered: Arc<Semaphore>,
    release: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for HoldCancel {
    fn name(&self) -> &str {
        "independent-held-cancel"
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
        if frame.data.is_cancel() {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            // This application wrapper owns cancellation; its cleanup closes the
            // nested engine. The test isolates root quiescence from fanout policy.
            ctx.push(frame, direction).await
        } else {
            self.inner.process(frame, direction, ctx).await
        }
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.inner.cleanup().await
    }
}
async fn acquired(semaphore: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(3), semaphore.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_active_cancel_cannot_be_overtaken_by_completed_descendant_cleanup() {
    let cleanup_entered = Arc::new(Semaphore::new(0));
    let cleanup_release = Arc::new(Semaphore::new(0));
    let cleanup_completed = Arc::new(Semaphore::new(0));
    let cancel_entered = Arc::new(Semaphore::new(0));
    let cancel_release = Arc::new(Semaphore::new(0));
    let child = Arc::new(CleanupLatch {
        entered: cleanup_entered.clone(),
        release: cleanup_release.clone(),
        completed: cleanup_completed.clone(),
    });
    let inner = Arc::new(ParallelPipeline::new(vec![vec![child]]).unwrap());
    let root = Arc::new(HoldCancel {
        inner,
        entered: cancel_entered.clone(),
        release: cancel_release.clone(),
    });
    let mut worker = Pipeline::new(vec![root])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(worker.recv().await.unwrap().frame.data.is_start());
    worker.end().await.unwrap();
    acquired(&cleanup_entered).await;
    worker
        .cancel("independent held cancellation")
        .await
        .unwrap();
    acquired(&cancel_entered).await;
    cleanup_release.add_permits(1);
    acquired(&cleanup_completed).await;
    // The control handler is held, with no ordinary work left. Give the root
    // supervisor multiple scheduled ticks; it must not publish End over Cancel.
    assert!(
        tokio::time::timeout(Duration::from_millis(150), worker.recv())
            .await
            .is_err(),
        "End overtook an admitted active cancellation"
    );
    assert!(!worker.has_finished());
    cancel_release.add_permits(1);
    let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(frames.len(), 1);
    assert!(frames[0].frame.data.is_cancel());
}
