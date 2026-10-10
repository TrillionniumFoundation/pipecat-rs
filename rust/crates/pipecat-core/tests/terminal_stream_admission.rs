//! Late owned terminal relays must participate in global quiescence.
use pipecat_core::*;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tokio::sync::Semaphore;
struct Relay {
    registered: Arc<Semaphore>,
    admitted: Arc<Semaphore>,
    cleaned: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for Relay {
    fn name(&self) -> &str {
        "TerminalRelay"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream
            && matches!(
                frame.data,
                FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
            )
        {
            let output = ctx.clone();
            let admitted = self.admitted.clone();
            ctx.spawn("late-terminal", false, async move {
                output.push_downstream_terminal_wait(frame).await?;
                admitted.add_permits(1);
                Ok(())
            })?;
            self.registered.add_permits(1);
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
struct Sink {
    entered: Semaphore,
    release: Semaphore,
    processed: AtomicUsize,
}
#[async_trait]
impl FrameProcessor for Sink {
    fn name(&self) -> &str {
        "Sink"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(&frame.data, FrameData::Text(text) if text.text == "hold") {
            self.entered.add_permits(1);
            self.release.acquire().await.unwrap().forget();
        }
        if frame.is_terminal() {
            self.processed.fetch_add(1, Ordering::SeqCst);
        }
        ctx.push(frame, direction).await
    }
}
async fn wait(gate: &Semaphore) {
    tokio::time::timeout(Duration::from_secs(2), gate.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
}
async fn send(worker: &PipelineWorker, frame: Frame) {
    worker
        .sender()
        .send(frame, Direction::Downstream)
        .await
        .unwrap();
}
#[tokio::test]
async fn late_owned_end_detailed_end_and_stop_wait_and_do_not_cleanup_early() {
    for terminal in [
        Frame::end(),
        Frame::new(FrameData::EndDetailed(ReasonFrame::default())),
        Frame::new(FrameData::Stop),
    ] {
        let stopping = matches!(terminal.data, FrameData::Stop);
        let relay = Arc::new(Relay {
            registered: Arc::new(Semaphore::new(0)),
            admitted: Arc::new(Semaphore::new(0)),
            cleaned: AtomicUsize::new(0),
        });
        let sink = Arc::new(Sink {
            entered: Semaphore::new(0),
            release: Semaphore::new(0),
            processed: AtomicUsize::new(0),
        });
        let mut worker = Pipeline::new(vec![relay.clone(), sink.clone()])
            .start(PipelineConfig {
                queue_capacity: 1,
                ..Default::default()
            })
            .await
            .unwrap();
        worker.recv().await.unwrap();
        send(&worker, Frame::text("hold")).await;
        wait(&sink.entered).await;
        send(&worker, Frame::text("queued")).await;
        send(&worker, terminal).await;
        wait(&relay.registered).await;
        assert_eq!(relay.admitted.available_permits(), 0);
        assert_eq!(relay.cleaned.load(Ordering::SeqCst), 0);
        assert!(!worker.has_finished());
        sink.release.add_permits(1);
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(sink.processed.load(Ordering::SeqCst), 1);
        assert_eq!(frames.iter().filter(|f| f.frame.is_terminal()).count(), 1);
        assert_eq!(relay.cleaned.load(Ordering::SeqCst), usize::from(!stopping));
    }
}
#[tokio::test]
async fn cancel_aborts_owned_terminal_wait_without_releasing_stalled_sink() {
    let relay = Arc::new(Relay {
        registered: Arc::new(Semaphore::new(0)),
        admitted: Arc::new(Semaphore::new(0)),
        cleaned: AtomicUsize::new(0),
    });
    let sink = Arc::new(Sink {
        entered: Semaphore::new(0),
        release: Semaphore::new(0),
        processed: AtomicUsize::new(0),
    });
    let worker = Pipeline::new(vec![relay.clone(), sink.clone()])
        .start(PipelineConfig {
            queue_capacity: 1,
            ..Default::default()
        })
        .await
        .unwrap();
    send(&worker, Frame::text("hold")).await;
    wait(&sink.entered).await;
    send(&worker, Frame::text("queued")).await;
    send(&worker, Frame::end()).await;
    wait(&relay.registered).await;
    worker.cancel("blocked terminal").await.unwrap();
    let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(report.state, WorkerState::Cancelled);
    assert_eq!(relay.admitted.available_permits(), 0);
    assert_eq!(relay.cleaned.load(Ordering::SeqCst), 1);
}
