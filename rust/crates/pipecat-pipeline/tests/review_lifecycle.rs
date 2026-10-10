use pipecat_core::*;
use pipecat_pipeline::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};
const DEADLINE: Duration = Duration::from_secs(3);
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .unwrap()
        .expect("pipeline unexpectedly closed")
}
async fn run(processor: Arc<dyn FrameProcessor>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    worker
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_sync_sentinels_are_not_evicted_by_small_identity_history() {
    let parallel = SyncParallelPipeline::with_config(
        vec![
            vec![Arc::new(IdentityProcessor)],
            vec![Arc::new(IdentityProcessor)],
        ],
        FrameOrder::Arrival,
        ParallelPipelineConfig {
            deduplication_capacity: 1,
            synchronization_timeout: Duration::from_millis(200),
            ..Default::default()
        },
    )
    .unwrap();
    let mut worker = run(Arc::new(parallel)).await;
    worker
        .sender()
        .send(Frame::text("payload"), Direction::Downstream)
        .await
        .unwrap();
    let frame = next(&mut worker).await;
    assert!(
        matches!(&frame.frame.data, FrameData::Text(text) if text.text == "payload"),
        "sync leaked a private sentinel or failed: {:?}",
        frame.frame.data
    );
    worker.end().await.unwrap();
    let (_, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert!(report.errors.is_empty(), "{:?}", report.errors);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nested_parallel_compositions_preserve_identity_direction_and_end() {
    for sync_outer in [false, true] {
        for sync_inner in [false, true] {
            let leaves: Vec<Branch> = vec![
                vec![Arc::new(IdentityProcessor)],
                vec![Arc::new(IdentityProcessor)],
            ];
            let inner: Arc<dyn FrameProcessor> = if sync_inner {
                Arc::new(SyncParallelPipeline::new(leaves).unwrap())
            } else {
                Arc::new(ParallelPipeline::new(leaves).unwrap())
            };
            let branches: Vec<Branch> = vec![vec![inner], vec![Arc::new(IdentityProcessor)]];
            let outer: Arc<dyn FrameProcessor> = if sync_outer {
                Arc::new(SyncParallelPipeline::new(branches).unwrap())
            } else {
                Arc::new(ParallelPipeline::new(branches).unwrap())
            };
            let mut worker = run(outer).await;
            for direction in [Direction::Downstream, Direction::Upstream] {
                let input = Frame::text("nested");
                let id = input.id;
                worker.sender().send(input, direction).await.unwrap();
                let output = next(&mut worker).await;
                assert_eq!(output.frame.id, id);
                assert_eq!(output.direction, direction);
            }
            worker.end().await.unwrap();
            let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert_eq!(report.state, WorkerState::Ended);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(frames.iter().filter(|f| f.frame.is_terminal()).count(), 1);
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::Text(_)))
                    .count(),
                0
            );
        }
    }
}
struct AsyncBranch {
    started: Notify,
    release: Arc<Semaphore>,
    cleaned: AtomicUsize,
    interruptible: bool,
}
#[async_trait]
impl FrameProcessor for AsyncBranch {
    fn name(&self) -> &str {
        "review-async-branch"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "background") {
            let release = self.release.clone();
            let output = ctx.clone();
            ctx.spawn("review-detached-work", self.interruptible, async move {
                release.acquire().await.unwrap().forget();
                output.push(Frame::text("async-result"), direction).await
            })?;
            self.started.notify_one();
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
fn background(interruptible: bool) -> Arc<AsyncBranch> {
    Arc::new(AsyncBranch {
        started: Notify::new(),
        release: Arc::new(Semaphore::new(0)),
        cleaned: AtomicUsize::new(0),
        interruptible,
    })
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_sentinel_does_not_wait_for_explicit_background_work_but_end_does() {
    let branch = background(false);
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![vec![branch.clone()]]).unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("background"), Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(DEADLINE, branch.started.notified())
        .await
        .unwrap();
    worker
        .sender()
        .send(Frame::text("following"), Direction::Downstream)
        .await
        .unwrap();
    assert!(
        matches!(next(&mut worker).await.frame.data, FrameData::Text(text) if text.text == "following")
    );
    worker.end().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), worker.recv())
            .await
            .is_err()
    );
    branch.release.add_permits(1);
    let (frames, report) = tokio::time::timeout(DEADLINE, worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Ended);
    assert_eq!(
        frames
            .iter()
            .filter(|f| matches!(&f.frame.data, FrameData::Text(t) if t.text == "async-result"))
            .count(),
        1
    );
    assert!(frames.last().unwrap().frame.is_terminal());
    assert_eq!(branch.cleaned.load(Ordering::SeqCst), 1);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retained_parallel_processor_does_not_keep_children_alive_after_worker_drop() {
    for sync in [false, true] {
        let branch = background(false);
        let branches: Vec<Branch> = vec![vec![branch.clone()]];
        let processor: Arc<dyn FrameProcessor> = if sync {
            Arc::new(SyncParallelPipeline::new(branches).unwrap())
        } else {
            Arc::new(ParallelPipeline::new(branches).unwrap())
        };
        let worker = run(processor.clone()).await;
        worker
            .sender()
            .send(Frame::text("background"), Direction::Downstream)
            .await
            .unwrap();
        tokio::time::timeout(DEADLINE, branch.started.notified())
            .await
            .unwrap();
        drop(worker);
        tokio::time::timeout(DEADLINE, async {
            while branch.cleaned.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("retained parent Arc leaked child workers");
        assert_eq!(branch.cleaned.load(Ordering::SeqCst), 1);
        assert!(Arc::strong_count(&processor) >= 1);
    }
}
