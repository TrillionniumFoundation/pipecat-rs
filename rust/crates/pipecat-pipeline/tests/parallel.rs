use pipecat_core::*;
use pipecat_pipeline::*;
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::{Notify, Semaphore};

const DEADLINE: Duration = Duration::from_secs(3);
async fn next(worker: &mut PipelineWorker) -> DirectedFrame {
    tokio::time::timeout(DEADLINE, worker.recv())
        .await
        .expect("output timeout")
        .expect("output closed")
}
async fn join(worker: &mut PipelineWorker) -> Result<WorkerReport, PipelineError> {
    tokio::time::timeout(DEADLINE, worker.join())
        .await
        .expect("join timeout")
}
async fn run(processor: Arc<dyn FrameProcessor>) -> PipelineWorker {
    let mut worker = Pipeline::new(vec![processor])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    worker
}
fn text(frame: DirectedFrame) -> String {
    match frame.frame.data {
        FrameData::Text(text) => text.text,
        other => panic!("expected text, got {other:?}"),
    }
}
struct Rewrite(&'static str, Duration);
#[async_trait]
impl FrameProcessor for Rewrite {
    fn name(&self) -> &str {
        self.0
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Text(input) = &frame.data {
            tokio::time::sleep(self.1).await;
            ctx.push(Frame::text(format!("{}:{}", self.0, input.text)), direction)
                .await
        } else {
            ctx.push(frame, direction).await
        }
    }
}
struct Gate {
    target: &'static str,
    entered: Notify,
    release: Semaphore,
    cleaned: AtomicUsize,
    active: Arc<AtomicBool>,
}
impl Gate {
    fn new(target: &'static str) -> Self {
        Self {
            target,
            entered: Notify::new(),
            release: Semaphore::new(0),
            cleaned: AtomicUsize::new(0),
            active: Arc::new(AtomicBool::new(false)),
        }
    }
}
struct Active(Arc<AtomicBool>);
impl Drop for Active {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for Gate {
    fn name(&self) -> &str {
        "Gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let matches = match self.target {
            "start" => frame.data.is_start(),
            "end" => matches!(frame.data, FrameData::End(_) | FrameData::EndDetailed(_)),
            "cancel" => frame.data.is_cancel(),
            target => matches!(&frame.data, FrameData::Text(text) if text.text == target),
        };
        if matches {
            self.active.store(true, Ordering::SeqCst);
            let _active = Active(self.active.clone());
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
async fn entered(gate: &Gate) {
    tokio::time::timeout(DEADLINE, gate.entered.notified())
        .await
        .expect("branch did not enter");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_is_deduplicated_in_both_directions() {
    for sync in [false, true] {
        let branches = vec![
            vec![Arc::new(IdentityProcessor) as Arc<dyn FrameProcessor>],
            vec![Arc::new(IdentityProcessor)],
        ];
        let processor: Arc<dyn FrameProcessor> = if sync {
            Arc::new(SyncParallelPipeline::new(branches).unwrap())
        } else {
            Arc::new(ParallelPipeline::new(branches).unwrap())
        };
        let mut worker = run(processor).await;
        for direction in [Direction::Downstream, Direction::Upstream] {
            let frame = Frame::text("same");
            let id = frame.id;
            worker.sender().send(frame, direction).await.unwrap();
            let output = next(&mut worker).await;
            assert_eq!(output.frame.id, id);
            assert_eq!(output.direction, direction);
        }
        worker.end().await.unwrap();
        assert!(matches!(
            next(&mut worker).await.frame.data,
            FrameData::End(_)
        ));
        assert_eq!(join(&mut worker).await.unwrap().state, WorkerState::Ended);
        assert!(worker.recv().await.is_none());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn independent_parallel_branches_do_not_wait_for_slow_peer() {
    let gate = Arc::new(Gate::new("blocked"));
    let mut worker = run(Arc::new(
        ParallelPipeline::new(vec![
            vec![gate.clone()],
            vec![Arc::new(Rewrite("fast", Duration::ZERO))],
        ])
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    entered(&gate).await;
    assert_eq!(text(next(&mut worker).await), "fast:blocked");
    assert!(gate.active.load(Ordering::SeqCst));
    gate.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "blocked");
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_holds_fast_output_until_slow_branch_sentinel() {
    let gate = Arc::new(Gate::new("blocked"));
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![
            vec![gate.clone()],
            vec![Arc::new(Rewrite("fast", Duration::ZERO))],
        ])
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    entered(&gate).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(30), worker.recv())
            .await
            .is_err()
    );
    gate.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "fast:blocked");
    assert_eq!(text(next(&mut worker).await), "blocked");
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sync_order_matches_source_fixture() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/parallel_contract.json")).unwrap();
    for (order, expected) in [
        (FrameOrder::Arrival, "arrival"),
        (FrameOrder::Pipeline, "pipeline"),
    ] {
        let mut worker = run(Arc::new(
            SyncParallelPipeline::with_order(
                vec![
                    vec![Arc::new(Rewrite("first", Duration::from_millis(40)))],
                    vec![Arc::new(Rewrite("second", Duration::ZERO))],
                ],
                order,
            )
            .unwrap(),
        ))
        .await;
        worker
            .sender()
            .send(Frame::text("input"), Direction::Downstream)
            .await
            .unwrap();
        let output = vec![text(next(&mut worker).await), text(next(&mut worker).await)];
        assert_eq!(
            serde_json::to_value(output).unwrap(),
            fixture["sync_order"][expected]
        );
        worker.end().await.unwrap();
        assert!(next(&mut worker).await.frame.is_terminal());
        assert!(join(&mut worker).await.is_ok());
    }
}

struct StartupEmission;
#[async_trait]
impl FrameProcessor for StartupEmission {
    fn name(&self) -> &str {
        "StartupEmission"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            ctx.push(Frame::text("before-start"), Direction::Upstream)
                .await?;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_barrier_covers_slow_branch_and_startup_output() {
    let gate = Arc::new(Gate::new("start"));
    let parallel =
        ParallelPipeline::new(vec![vec![Arc::new(StartupEmission)], vec![gate.clone()]]).unwrap();
    let start =
        tokio::spawn(Pipeline::new(vec![Arc::new(parallel)]).start(PipelineConfig::default()));
    entered(&gate).await;
    assert!(!start.is_finished());
    gate.release.add_permits(1);
    let mut worker = start.await.unwrap().unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    assert_eq!(text(next(&mut worker).await), "before-start");
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn end_and_cancel_wait_for_every_branch_and_cleanup() {
    for terminal in ["end", "cancel"] {
        for sync in [false, true] {
            let gate = Arc::new(Gate::new(terminal));
            let branches = vec![
                vec![Arc::new(IdentityProcessor) as Arc<dyn FrameProcessor>],
                vec![gate.clone() as Arc<dyn FrameProcessor>],
            ];
            let processor: Arc<dyn FrameProcessor> = if sync {
                Arc::new(SyncParallelPipeline::new(branches).unwrap())
            } else {
                Arc::new(ParallelPipeline::new(branches).unwrap())
            };
            let mut worker = run(processor).await;
            if terminal == "end" {
                worker.end().await.unwrap();
            } else {
                worker.cancel("test").await.unwrap();
            }
            entered(&gate).await;
            assert!(
                tokio::time::timeout(Duration::from_millis(25), worker.recv())
                    .await
                    .is_err()
            );
            gate.release.add_permits(1);
            assert!(next(&mut worker).await.frame.is_terminal());
            assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
            assert!(join(&mut worker).await.is_ok());
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancellation_releases_blocked_normal_and_sync_work() {
    for sync in [false, true] {
        let gate = Arc::new(Gate::new("blocked"));
        let branches = vec![
            vec![gate.clone() as Arc<dyn FrameProcessor>],
            vec![Arc::new(Rewrite("fast", Duration::ZERO))],
        ];
        let processor: Arc<dyn FrameProcessor> = if sync {
            Arc::new(SyncParallelPipeline::new(branches).unwrap())
        } else {
            Arc::new(ParallelPipeline::new(branches).unwrap())
        };
        let mut worker = run(processor).await;
        worker
            .sender()
            .send(Frame::text("blocked"), Direction::Downstream)
            .await
            .unwrap();
        entered(&gate).await;
        worker.cancel("test").await.unwrap();
        loop {
            if next(&mut worker).await.frame.data.is_cancel() {
                break;
            }
        }
        assert_eq!(
            join(&mut worker).await.unwrap().state,
            WorkerState::Cancelled
        );
        assert!(!gate.active.load(Ordering::SeqCst));
        assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_clears_pending_sync_batch_and_next_input_survives() {
    let gate = Arc::new(Gate::new("blocked"));
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![
            vec![gate.clone()],
            vec![Arc::new(Rewrite("fast", Duration::ZERO))],
        ])
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    entered(&gate).await;
    tokio::time::sleep(Duration::from_millis(20)).await;
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
        .send(Frame::text("fresh"), Direction::Downstream)
        .await
        .unwrap();
    let mut output = vec![text(next(&mut worker).await), text(next(&mut worker).await)];
    output.sort();
    assert_eq!(output, ["fast:fresh", "fresh"]);
    assert!(!gate.active.load(Ordering::SeqCst));
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

struct Failure;
#[async_trait]
impl FrameProcessor for Failure {
    fn name(&self) -> &str {
        "Failure"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            return Err(ProcessorError::fatal("branch broke"));
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failed_branch_fails_parent_and_cleans_peers() {
    let gate = Arc::new(Gate::new("blocked"));
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![vec![gate.clone()], vec![Arc::new(Failure)]]).unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    assert!(join(&mut worker).await.is_err());
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
    assert!(!gate.active.load(Ordering::SeqCst));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_cancels_and_cleans_owned_branch_workers() {
    let gate = Arc::new(Gate::new("blocked"));
    let worker = run(Arc::new(
        ParallelPipeline::new(vec![vec![gate.clone()]]).unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    entered(&gate).await;
    drop(worker);
    tokio::time::timeout(DEADLINE, async {
        while gate.cleaned.load(Ordering::SeqCst) == 0 {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert!(!gate.active.load(Ordering::SeqCst));
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

struct Burst(usize);
#[async_trait]
impl FrameProcessor for Burst {
    fn name(&self) -> &str {
        "Burst"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            for index in 0..self.0 {
                ctx.push(Frame::text(index.to_string()), direction).await?;
            }
            Ok(())
        } else {
            ctx.push(frame, direction).await
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_sync_output_fails_instead_of_deadlocking() {
    let config = ParallelPipelineConfig {
        buffered_frames: 2,
        ..Default::default()
    };
    let mut worker = run(Arc::new(
        SyncParallelPipeline::with_config(
            vec![vec![Arc::new(Burst(5))]],
            FrameOrder::Arrival,
            config,
        )
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("burst"), Direction::Downstream)
        .await
        .unwrap();
    assert!(join(&mut worker).await.is_err());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn full_branch_queue_fails_without_hanging_cleanup() {
    let gate = Arc::new(Gate::new("blocked"));
    let mut config = ParallelPipelineConfig::default();
    config.branch.queue_capacity = 1;
    let mut worker = run(Arc::new(
        ParallelPipeline::with_config(vec![vec![gate.clone()]], config).unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    entered(&gate).await;
    for _ in 0..8 {
        if worker
            .sender()
            .send(Frame::text("queued"), Direction::Downstream)
            .await
            .is_err()
        {
            break;
        }
    }
    assert!(join(&mut worker).await.is_err());
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn terminal_timeout_is_explicit_and_cleans_branches() {
    let gate = Arc::new(Gate::new("end"));
    let config = ParallelPipelineConfig {
        synchronization_timeout: Duration::from_millis(60),
        ..Default::default()
    };
    let mut worker = run(Arc::new(
        ParallelPipeline::with_config(vec![vec![gate.clone()]], config).unwrap(),
    ))
    .await;
    worker.end().await.unwrap();
    entered(&gate).await;
    assert!(join(&mut worker).await.is_err());
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

#[test]
fn configuration_rejects_empty_or_unbounded_zero_limits() {
    assert!(ParallelPipeline::new(vec![]).is_err());
    assert!(SyncParallelPipeline::new(vec![]).is_err());
    let config = ParallelPipelineConfig {
        buffered_frames: 0,
        ..Default::default()
    };
    assert!(ParallelPipeline::with_config(vec![vec![]], config).is_err());
}

struct MutateIdentity(&'static str, Duration);
#[async_trait]
impl FrameProcessor for MutateIdentity {
    fn name(&self) -> &str {
        self.0
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Text(text) = &mut frame.data {
            tokio::time::sleep(self.1).await;
            text.text = self.0.into();
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pipeline_order_deduplicates_after_ordering_and_per_input_only() {
    let mut worker = run(Arc::new(
        SyncParallelPipeline::with_order(
            vec![
                vec![Arc::new(MutateIdentity("first", Duration::from_millis(25)))],
                vec![Arc::new(MutateIdentity("second", Duration::ZERO))],
            ],
            FrameOrder::Pipeline,
        )
        .unwrap(),
    ))
    .await;
    let frame = Frame::text("original");
    for _ in 0..2 {
        worker
            .sender()
            .send(frame.clone(), Direction::Downstream)
            .await
            .unwrap();
        let output = next(&mut worker).await;
        assert_eq!(output.frame.id, frame.id);
        assert_eq!(text(output), "first");
    }
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

struct Bidirectional;
#[async_trait]
impl FrameProcessor for Bidirectional {
    fn name(&self) -> &str {
        "Bidirectional"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let FrameData::Text(text) = &frame.data {
            ctx.push(
                Frame::text(format!("reverse:{}", text.text)),
                direction.reversed(),
            )
            .await?;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synchronized_branches_retain_output_direction() {
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![vec![Arc::new(Bidirectional)]]).unwrap(),
    ))
    .await;
    for direction in [Direction::Downstream, Direction::Upstream] {
        worker
            .sender()
            .send(Frame::text("input"), direction)
            .await
            .unwrap();
        let first = next(&mut worker).await;
        let second = next(&mut worker).await;
        let mut output = vec![
            (text(first.clone()), first.direction),
            (text(second.clone()), second.direction),
        ];
        output.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(
            output,
            [
                ("input".into(), direction),
                ("reverse:input".into(), direction.reversed())
            ]
        );
    }
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

struct Interrupting {
    fire: Arc<Semaphore>,
}
#[async_trait]
impl FrameProcessor for Interrupting {
    fn name(&self) -> &str {
        "Interrupting"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "blocked") {
            self.fire.acquire().await.unwrap().forget();
            ctx.broadcast_interruption().await?;
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn branch_originated_interruption_reaches_parent_siblings_and_both_directions() {
    let gate = Arc::new(Gate::new("blocked"));
    let fire = Arc::new(Semaphore::new(0));
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![
            vec![gate.clone()],
            vec![Arc::new(Interrupting { fire: fire.clone() })],
        ])
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("blocked"), Direction::Downstream)
        .await
        .unwrap();
    entered(&gate).await;
    fire.add_permits(1);
    let first = next(&mut worker).await;
    let second = next(&mut worker).await;
    assert!(matches!(first.frame.data, FrameData::Interruption));
    assert!(matches!(second.frame.data, FrameData::Interruption));
    assert_ne!(first.direction, second.direction);
    assert_eq!(first.frame.broadcast_sibling_id, Some(second.frame.id));
    worker
        .sender()
        .send(Frame::text("fresh"), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(text(next(&mut worker).await), "fresh");
    assert!(!gate.active.load(Ordering::SeqCst));
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

struct FlushAtEnd;
#[async_trait]
impl FrameProcessor for FlushAtEnd {
    fn name(&self) -> &str {
        "FlushAtEnd"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::End(_)) {
            ctx.push(Frame::text("flushed"), direction).await?;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_end_releases_all_flush_output_before_terminal() {
    let gate = Arc::new(Gate::new("end"));
    let mut worker = run(Arc::new(
        ParallelPipeline::new(vec![vec![Arc::new(FlushAtEnd)], vec![gate.clone()]]).unwrap(),
    ))
    .await;
    worker.end().await.unwrap();
    entered(&gate).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(25), worker.recv())
            .await
            .is_err()
    );
    gate.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "flushed");
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_supersedes_pending_end_without_closed_branch_failure() {
    let gate = Arc::new(Gate::new("end"));
    let mut worker = run(Arc::new(
        ParallelPipeline::new(vec![vec![Arc::new(IdentityProcessor)], vec![gate.clone()]]).unwrap(),
    ))
    .await;
    worker.end().await.unwrap();
    entered(&gate).await;
    tokio::time::sleep(Duration::from_millis(25)).await;
    worker.cancel("override").await.unwrap();
    assert!(next(&mut worker).await.frame.data.is_cancel());
    assert_eq!(
        join(&mut worker).await.unwrap().state,
        WorkerState::Cancelled
    );
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

struct SwallowControls;
#[async_trait]
impl FrameProcessor for SwallowControls {
    fn name(&self) -> &str {
        "SwallowControls"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Control) {
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_sentinel_times_out_and_joins_branch() {
    let config = ParallelPipelineConfig {
        synchronization_timeout: Duration::from_millis(50),
        ..Default::default()
    };
    let mut worker = run(Arc::new(
        SyncParallelPipeline::with_config(
            vec![vec![Arc::new(SwallowControls)]],
            FrameOrder::Arrival,
            config,
        )
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("no sentinel"), Direction::Downstream)
        .await
        .unwrap();
    assert!(join(&mut worker).await.is_err());
}

struct MixedFlush;
#[async_trait]
impl FrameProcessor for MixedFlush {
    fn name(&self) -> &str {
        "MixedFlush"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::End(_)) {
            ctx.push(Frame::text("stale"), direction).await?;
            ctx.push(Frame::text("retained").with_interruptible(false), direction)
                .await?;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn interruption_invalidates_buffered_output_from_already_ended_branch() {
    let gate = Arc::new(Gate::new("end"));
    let mut worker = run(Arc::new(
        ParallelPipeline::new(vec![vec![Arc::new(MixedFlush)], vec![gate.clone()]]).unwrap(),
    ))
    .await;
    worker.end().await.unwrap();
    entered(&gate).await;
    tokio::time::sleep(Duration::from_millis(30)).await;
    worker
        .sender()
        .send(Frame::interruption(), Direction::Downstream)
        .await
        .unwrap();
    assert!(matches!(
        next(&mut worker).await.frame.data,
        FrameData::Interruption
    ));
    gate.release.add_permits(1);
    assert_eq!(text(next(&mut worker).await), "retained");
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_parallel_identity_window_allows_old_replay_after_eviction() {
    let config = ParallelPipelineConfig {
        deduplication_capacity: 2,
        ..Default::default()
    };
    let mut worker = run(Arc::new(
        ParallelPipeline::with_config(vec![vec![Arc::new(IdentityProcessor)]], config).unwrap(),
    ))
    .await;
    let old = Frame::text("old");
    for frame in [
        old.clone(),
        Frame::text("new-1"),
        Frame::text("new-2"),
        old.clone(),
    ] {
        worker
            .sender()
            .send(frame.clone(), Direction::Downstream)
            .await
            .unwrap();
        assert_eq!(next(&mut worker).await.frame.id, frame.id);
    }
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn active_sentinels_survive_smaller_recent_history_capacity() {
    let config = ParallelPipelineConfig {
        deduplication_capacity: 1,
        ..Default::default()
    };
    let mut worker = run(Arc::new(
        SyncParallelPipeline::with_config(
            vec![
                vec![Arc::new(IdentityProcessor)],
                vec![Arc::new(IdentityProcessor)],
            ],
            FrameOrder::Arrival,
            config,
        )
        .unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("input"), Direction::Downstream)
        .await
        .unwrap();
    assert_eq!(text(next(&mut worker).await), "input");
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}

struct Nonfatal;
#[async_trait]
impl FrameProcessor for Nonfatal {
    fn name(&self) -> &str {
        "Nonfatal"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            return Err(ProcessorError::new("recoverable branch failure"));
        }
        ctx.push(frame, direction).await
    }
}
struct ConsumeErrors;
#[async_trait]
impl FrameProcessor for ConsumeErrors {
    fn name(&self) -> &str {
        "ConsumeErrors"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.as_error_frame().is_some() {
            return Ok(());
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_records_nonfatal_branch_error_even_when_upstream_consumes_it() {
    let parallel = Arc::new(ParallelPipeline::new(vec![vec![Arc::new(Nonfatal)]]).unwrap());
    let mut worker = Pipeline::new(vec![Arc::new(ConsumeErrors), parallel])
        .start(PipelineConfig::default())
        .await
        .unwrap();
    assert!(next(&mut worker).await.frame.data.is_start());
    worker
        .sender()
        .send(Frame::text("fail"), Direction::Downstream)
        .await
        .unwrap();
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    let report = join(&mut worker).await.unwrap();
    assert_eq!(report.errors.len(), 1);
    assert_eq!(report.total_errors, 1);
    assert_eq!(report.errors[0].error, "recoverable branch failure");
    assert!(!report.errors[0].fatal);
}

struct UpstreamEnd;
#[async_trait]
impl FrameProcessor for UpstreamEnd {
    fn name(&self) -> &str {
        "UpstreamEnd"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            ctx.push(Frame::end(), Direction::Upstream).await?;
        }
        ctx.push(frame, direction).await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn generated_upstream_terminal_is_relayed_without_ending_worker() {
    let mut worker = run(Arc::new(
        SyncParallelPipeline::new(vec![vec![Arc::new(UpstreamEnd)]]).unwrap(),
    ))
    .await;
    worker
        .sender()
        .send(Frame::text("input"), Direction::Downstream)
        .await
        .unwrap();
    let first = next(&mut worker).await;
    let second = next(&mut worker).await;
    let outputs = [first, second];
    assert!(outputs
        .iter()
        .any(|item| item.frame.is_terminal() && item.direction == Direction::Upstream));
    assert!(outputs
        .iter()
        .any(|item| matches!(item.frame.data, FrameData::Text(_))
            && item.direction == Direction::Downstream));
    assert_eq!(worker.state(), WorkerState::Running);
    worker.end().await.unwrap();
    assert!(next(&mut worker).await.frame.is_terminal());
    assert!(join(&mut worker).await.is_ok());
}
