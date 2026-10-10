use pipecat_core::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;

struct UpstreamGate {
    entered: Semaphore,
    release: Semaphore,
    background: bool,
    normal: bool,
    cleaned: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for UpstreamGate {
    fn name(&self) -> &str {
        "upstream-gate"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::UserStartedSpeaking)
            || matches!(&frame.data, FrameData::Text(t) if t.text == "upstream")
        {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
            if self.background {
                let ctx2 = ctx.clone();
                ctx.spawn("late-system-output", false, async move {
                    tokio::time::sleep(Duration::from_millis(20)).await;
                    ctx2.push(Frame::text("background"), Direction::Upstream)
                        .await
                })?;
            }
        }
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.cleaned.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}
struct FlushUpstream(Arc<UpstreamGate>);
#[async_trait]
impl FrameProcessor for FlushUpstream {
    fn name(&self) -> &str {
        "flush-upstream"
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        ctx.push(
            if self.0.normal {
                Frame::text("upstream")
            } else {
                Frame::new(FrameData::UserStartedSpeaking)
            },
            Direction::Upstream,
        )
        .await?;
        self.0.entered.acquire().await.unwrap().forget();
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
}
struct TerminalBoundary(Arc<Semaphore>);
#[async_trait]
impl FrameProcessor for TerminalBoundary {
    fn name(&self) -> &str {
        "terminal-boundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let terminal = frame.is_terminal();
        ctx.push(frame, direction).await?;
        if terminal {
            self.0.add_permits(1);
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn graceful_terminal_drains_admitted_upstream_system_and_background_output() {
    for terminal in [FrameData::End(EndFrame::default()), FrameData::Stop] {
        for background in [false, true] {
            let gate = Arc::new(UpstreamGate {
                entered: Semaphore::new(0),
                release: Semaphore::new(0),
                background,
                normal: false,
                cleaned: AtomicUsize::new(0),
            });
            let boundary = Arc::new(Semaphore::new(0));
            let mut worker = Pipeline::new(vec![
                gate.clone(),
                Arc::new(FlushUpstream(gate.clone())),
                Arc::new(TerminalBoundary(boundary.clone())),
            ])
            .start(PipelineConfig::default())
            .await
            .unwrap();
            worker.recv().await.unwrap();
            worker
                .sender()
                .send(Frame::new(terminal.clone()), Direction::Downstream)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(2), boundary.acquire())
                .await
                .unwrap()
                .unwrap()
                .forget();
            assert!(
                !worker.has_finished(),
                "terminal must wait for the admitted upstream handler"
            );
            gate.release.add_permits(1);
            let (frames, report) = tokio::time::timeout(Duration::from_secs(2), worker.collect())
                .await
                .unwrap()
                .unwrap();
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(
                gate.cleaned.load(Ordering::SeqCst),
                usize::from(!matches!(terminal, FrameData::Stop))
            );
            assert!(frames.last().unwrap().frame.is_terminal());
            assert_eq!(
                frames
                    .iter()
                    .filter(|f| matches!(f.frame.data, FrameData::UserStartedSpeaking))
                    .count(),
                1
            );
            assert_eq!(
                frames
                    .iter()
                    .filter(
                        |f| matches!(&f.frame.data, FrameData::Text(t) if t.text == "background")
                    )
                    .count(),
                usize::from(background)
            );
        }
    }
}

async fn pending_terminal(normal: bool, timeout: Duration) -> (PipelineWorker, Arc<UpstreamGate>) {
    let gate = Arc::new(UpstreamGate {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        background: false,
        normal,
        cleaned: AtomicUsize::new(0),
    });
    let boundary = Arc::new(Semaphore::new(0));
    let config = PipelineConfig {
        shutdown_timeout: timeout,
        ..PipelineConfig::default()
    };
    let mut worker = Pipeline::new(vec![
        gate.clone(),
        Arc::new(FlushUpstream(gate.clone())),
        Arc::new(TerminalBoundary(boundary.clone())),
    ])
    .start(config)
    .await
    .unwrap();
    worker.recv().await.unwrap();
    worker.end().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), boundary.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    (worker, gate)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn blocked_upstream_system_drain_obeys_shutdown_timeout() {
    let (worker, gate) = pending_terminal(false, Duration::from_millis(100)).await;
    let result = tokio::time::timeout(Duration::from_secs(2), worker.collect())
        .await
        .unwrap();
    assert!(matches!(result, Err(PipelineError::ShutdownTimeout)));
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cancel_preempts_pending_terminal_and_blocked_upstream_normal_handler() {
    let (worker, gate) = pending_terminal(true, Duration::from_secs(2)).await;
    worker.cancel("cancel drain").await.unwrap();
    let (frames, report) = tokio::time::timeout(Duration::from_secs(1), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert!(report.errors.is_empty());
    assert!(frames.last().unwrap().frame.data.is_cancel());
    assert!(!frames
        .iter()
        .any(|f| matches!(f.frame.data, FrameData::End(_))));
    assert_eq!(gate.cleaned.load(Ordering::SeqCst), 1);
}
