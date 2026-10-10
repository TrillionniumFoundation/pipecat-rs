use super::*;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, Pipeline, ProcessorContext,
};
use std::sync::{mpsc, TryLockError};

#[derive(Default)]
struct CaptureFence(Mutex<Option<InvocationFence>>);
#[async_trait]
impl FrameProcessor for CaptureFence {
    fn name(&self) -> &str {
        "StoppedCloneFenceCapture"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(frame.data, FrameData::Text(_)) {
            *self.0.lock().unwrap() = Some(ctx.invocation_fence()?);
        }
        ctx.push(frame, direction).await
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_cloned_terminal_batch_cannot_close_before_in_progress_record_admission() {
    let capture = Arc::new(CaptureFence::default());
    let mut worker = Pipeline::new(vec![capture.clone()])
        .start(Default::default())
        .await
        .unwrap();
    let marker = Frame::text("capture original scope");
    let id = marker.id;
    worker
        .sender()
        .send(marker, Direction::Downstream)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(frame) = worker.recv().await {
            if frame.frame.id == id {
                return;
            }
        }
        panic!("scope capture did not reach pipeline output");
    })
    .await
    .unwrap();
    let fence = capture.0.lock().unwrap().take().unwrap();
    let (hub, mut receiver) = StoppedHub::new(Default::default()).unwrap();
    let record = hub
        .reserve(StoppedTurn {
            text: "terminal committed record",
            started_at: "source timestamp",
            interrupted: false,
            session_id: 1,
            turn_id: 1,
            job_generation: None,
            cause: Some(fence.clone()),
        })
        .unwrap();
    let close = hub.terminal(AssistantStoppedClose::End);
    // Hold the real invocation admission lock so the first emitter cannot yet
    // publish. Both native threads execute the same cloned, ordered terminal
    // batch; none runs application code or a detached completion task.
    let (in_progress, finished_early, closed_early) = std::thread::scope(|scope| {
        let (held_tx, held_rx) = mpsc::sync_channel(1);
        let (release_tx, release_rx) = mpsc::sync_channel(1);
        let holding = scope.spawn(move || {
            fence
                .with_current(|| {
                    held_tx.send(()).unwrap();
                    release_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                })
                .expect("original invocation is current");
        });
        held_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let first_batch = [record.clone(), close.clone()];
        let first = scope.spawn(move || {
            for action in first_batch {
                action.admit().unwrap();
            }
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        let in_progress = loop {
            // Accept both a still-held ticket and the old take-and-unlock shape
            // as evidence the first emitter reached admission. The assertion is
            // about the second emitter's observable ordering, not lock shape.
            let started = match record.0.pending.try_lock() {
                Err(TryLockError::WouldBlock) => true,
                Ok(ticket) => ticket.is_none(),
                Err(TryLockError::Poisoned(_)) => panic!("ticket poisoned"),
            };
            if started || std::time::Instant::now() >= deadline {
                break started;
            }
            std::thread::sleep(Duration::from_millis(1));
        };
        let (started_tx, started_rx) = mpsc::sync_channel(1);
        let (finished_tx, finished_rx) = mpsc::sync_channel(1);
        let second_batch = [record.clone(), close.clone()];
        let second = scope.spawn(move || {
            started_tx.send(()).unwrap();
            for action in second_batch {
                action.admit().unwrap();
            }
            finished_tx.send(()).unwrap();
        });
        started_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        let finished_early = finished_rx.recv_timeout(Duration::from_millis(100)).is_ok();
        let closed_early = receiver.closed_reason();
        release_tx.send(()).unwrap();
        holding.join().unwrap();
        first.join().unwrap();
        second.join().unwrap();
        (in_progress, finished_early, closed_early)
    });
    worker.end().await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap()
            .1
            .errors
            .is_empty()
    );
    assert!(
        in_progress,
        "first emitter must be waiting on the original scope"
    );
    assert!(
        !finished_early,
        "a clone cannot advance past the in-progress record into terminal close"
    );
    assert_eq!(closed_early, None);
    let delivered = receiver.recv().await.unwrap();
    assert_eq!(delivered.completion().text(), "terminal committed record");
    drop(delivered);
    assert!(receiver.recv().await.is_none());
    assert_eq!(receiver.closed_reason(), Some(AssistantStoppedClose::End));
    assert_eq!(receiver.stats().delivered, 1);
    assert_eq!(receiver.stats().retained_records, 0);
    assert_eq!(receiver.stats().retained_bytes, 0);
}
