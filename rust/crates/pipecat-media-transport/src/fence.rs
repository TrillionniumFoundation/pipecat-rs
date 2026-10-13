use pipecat_core::{Direction, Frame, ProcessorContext, ProcessorError};
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Pending,
    Released,
    Aborted,
    Revoked,
}

/// Independent lanes distinguish expected interruption from a failed worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum FenceOutcome {
    Completed,
    Revoked,
    Aborted,
}
struct State {
    remaining: usize,
    frame: Option<Frame>,
}
struct Fence {
    state: Mutex<State>,
    description: String,
    status: watch::Sender<Status>,
}

/// A queue retains at most one weak handle for its currently popped fence.
/// It does not keep a frame or completed rendezvous alive.
pub(crate) struct FenceCancellation(Weak<Fence>);
impl FenceCancellation {
    pub fn revoke(&self) {
        self.signal(Status::Revoked);
    }
    pub fn abort(&self) {
        self.signal(Status::Aborted);
    }
    fn signal(&self, outcome: Status) {
        if let Some(fence) = self.0.upgrade() {
            let _state = fence.state.lock().unwrap();
            fence.status.send_if_modified(|status| {
                if *status == Status::Pending {
                    *status = outcome;
                    true
                } else {
                    false
                }
            });
        }
    }
}

/// An owned queue entry: discarding any participant invalidates the whole fence.
pub(crate) struct FenceParticipant {
    fence: Arc<Fence>,
    completed: bool,
}
impl FenceParticipant {
    pub fn group(frame: Frame, destinations: usize) -> Vec<Self> {
        let (status, _) = watch::channel(Status::Pending);
        let fence = Arc::new(Fence {
            description: description(&frame),
            state: Mutex::new(State {
                remaining: destinations,
                frame: Some(frame),
            }),
            status,
        });
        (0..destinations)
            .map(|_| Self {
                fence: fence.clone(),
                completed: false,
            })
            .collect()
    }
    pub fn cancellation(&self) -> FenceCancellation {
        FenceCancellation(Arc::downgrade(&self.fence))
    }
    pub fn error(&self, message: &str) -> ProcessorError {
        ProcessorError::fatal(format!("{}: {message}", self.fence.description))
    }
    /// Mark expected epoch revocation before a queued or popped participant is
    /// dropped. Synchronize with independent final downstream admission.
    pub fn revoke(&self) {
        let _state = self.fence.state.lock().unwrap();
        self.fence.status.send_if_modified(|status| {
            if *status == Status::Pending {
                *status = Status::Revoked;
                true
            } else {
                false
            }
        });
    }
    /// Decide a lane's preceding-delivery failure against the same boundary as
    /// revoke/abort. An already revoked fence is not a new fatal delivery error;
    /// a failure selected while Pending aborts it and cannot later be erased by
    /// interruption or Cancel. No queue lock is acquired under this lock.
    pub fn check_delivery(
        &self,
        incomplete: bool,
        reason: &str,
    ) -> Result<Option<FenceOutcome>, ProcessorError> {
        let _state = self.fence.state.lock().unwrap();
        let status = *self.fence.status.borrow();
        match status {
            Status::Released => Ok(Some(FenceOutcome::Completed)),
            Status::Revoked => Ok(Some(FenceOutcome::Revoked)),
            Status::Aborted => Ok(Some(FenceOutcome::Aborted)),
            Status::Pending if incomplete => {
                self.fence.status.send_replace(Status::Aborted);
                Err(self.error(reason))
            }
            Status::Pending => Ok(None),
        }
    }
    /// Opt-in completion preserves the reason for an unsuccessful rendezvous.
    /// Final try_push is synchronous and shares this lock with revoke/Drop;
    /// successful downstream admission is an irreversible completion boundary.
    /// No MediaQueue lock is acquired here (queue interruption locks queue then
    /// coordinator), and no lock is held across an await.
    pub async fn complete_independent(
        &mut self,
        ctx: &ProcessorContext,
    ) -> Result<FenceOutcome, ProcessorError> {
        let mut status = self.fence.status.subscribe();
        {
            let mut state = self.fence.state.lock().unwrap();
            match *status.borrow() {
                Status::Revoked => return Ok(FenceOutcome::Revoked),
                Status::Aborted => return Ok(FenceOutcome::Aborted),
                Status::Released => {
                    self.completed = true;
                    return Ok(FenceOutcome::Completed);
                }
                Status::Pending => {}
            }
            state.remaining -= 1;
            if state.remaining == 0 {
                if let Some(frame) = state.frame.take() {
                    if let Err(problem) = ctx.try_push(frame, Direction::Downstream) {
                        self.fence.status.send_replace(Status::Aborted);
                        return Err(problem);
                    }
                }
                self.fence.status.send_replace(Status::Released);
                self.completed = true;
                return Ok(FenceOutcome::Completed);
            }
        }
        loop {
            match *status.borrow_and_update() {
                Status::Released => {
                    self.completed = true;
                    return Ok(FenceOutcome::Completed);
                }
                Status::Revoked => return Ok(FenceOutcome::Revoked),
                Status::Aborted => return Ok(FenceOutcome::Aborted),
                Status::Pending => {}
            }
            status
                .changed()
                .await
                .map_err(|_| self.error("coordinator closed before completion"))?;
        }
    }
    /// Wait for every destination's preceding media, forwarding the marker once
    /// before releasing any destination to process its later queue entries.
    pub async fn complete(&mut self, ctx: &ProcessorContext) -> Result<bool, ProcessorError> {
        let mut status = self.fence.status.subscribe();
        let frame = {
            let mut state = self.fence.state.lock().unwrap();
            if matches!(*status.borrow(), Status::Aborted | Status::Revoked) {
                return Ok(false);
            }
            state.remaining -= 1;
            if state.remaining == 0 {
                state.frame.take()
            } else {
                None
            }
        };
        if let Some(frame) = frame {
            ctx.push(frame, Direction::Downstream).await?;
            self.fence.status.send_replace(Status::Released);
        }
        loop {
            match *status.borrow_and_update() {
                Status::Released => {
                    self.completed = true;
                    return Ok(true);
                }
                Status::Aborted | Status::Revoked => return Ok(false),
                Status::Pending => {}
            }
            status
                .changed()
                .await
                .map_err(|_| self.error("coordinator closed before completion"))?;
        }
    }
}
impl Drop for FenceParticipant {
    fn drop(&mut self) {
        if !self.completed {
            let _state = self.fence.state.lock().unwrap();
            self.fence.status.send_if_modified(|status| {
                if *status == Status::Pending {
                    *status = Status::Aborted;
                    true
                } else {
                    false
                }
            });
        }
    }
}
pub(crate) fn description(frame: &Frame) -> String {
    let flow_id = frame
        .metadata
        .get("pipecat.flows.barrier")
        .and_then(serde_json::Value::as_u64);
    format!(
        "media flush fence frame {} (flow barrier {:?})",
        frame.id, flow_id
    )
}

#[cfg(test)]
mod independent_tests {
    use super::*;
    use crate::queue::MediaQueue;
    use pipecat_core::*;
    use std::future::{poll_fn, Future};
    use std::pin::Pin;
    use std::task::Poll;
    use std::time::Duration;
    use tokio::sync::Semaphore;
    use tokio::time::Instant;

    fn marker() -> Frame {
        Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            event: Some(LocalFlushEvent::default()),
            ..Default::default()
        }))
        .with_interruptible(true)
    }
    fn event(frame: &Frame) -> LocalFlushEvent {
        match &frame.data {
            FrameData::FlushDetailed(flush) => flush.event.clone().unwrap(),
            _ => unreachable!(),
        }
    }
    fn pair(frame: Frame) -> (Arc<MediaQueue>, Arc<MediaQueue>) {
        let first = MediaQueue::new(4, 4096);
        let second = MediaQueue::new(4, 4096);
        MediaQueue::fence(&[first.clone(), second.clone()], frame).unwrap();
        (first, second)
    }
    async fn pending<T>(mut future: Pin<&mut impl Future<Output = T>>) -> bool {
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
    }
    struct ContextCapture {
        context: Mutex<Option<ProcessorContext>>,
        ready: Semaphore,
    }
    #[async_trait::async_trait]
    impl FrameProcessor for ContextCapture {
        fn name(&self) -> &str {
            "independent-fence-unit-context"
        }
        async fn setup(
            &self,
            _: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.context.lock().unwrap() = Some(ctx.clone());
            Ok(())
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let start = frame.data.is_start();
            ctx.push(frame, direction).await?;
            if start {
                self.ready.add_permits(1);
            }
            Ok(())
        }
    }
    async fn context() -> (PipelineWorker, ProcessorContext) {
        let capture = Arc::new(ContextCapture {
            context: Mutex::new(None),
            ready: Semaphore::new(0),
        });
        let worker = Pipeline::new(vec![capture.clone()])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(3), capture.ready.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let ctx = capture.context.lock().unwrap().clone().unwrap();
        (worker, ctx)
    }
    async fn finish(worker: PipelineWorker) -> Vec<DirectedFrame> {
        worker.end().await.unwrap();
        let (frames, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        frames
    }

    #[test]
    fn opted_pop_tracks_only_one_weak_participant_and_keeps_legacy_pop_unchanged() {
        for independent in [false, true] {
            let (first, second) = pair(marker());
            let item = if independent {
                first.pop_ready_independent(Instant::now()).unwrap()
            } else {
                first.pop_ready(Instant::now()).unwrap()
            };
            let fence = item.fence.as_ref().unwrap();
            let weak = Arc::downgrade(&fence.fence);
            first.interrupt();
            assert_eq!(
                *fence.fence.status.borrow() == Status::Revoked,
                independent,
                "legacy popped participants retain the original behavior"
            );
            // Clearing the remaining charged queue participants plus dropping
            // the current item must free the coordinator despite its weak slot.
            second.cancel();
            drop(item);
            assert!(weak.upgrade().is_none());
        }
    }

    #[test]
    fn queued_and_popped_fences_revoke_at_named_queue_cutoff_and_cancel_aborts() {
        for popped in [false, true] {
            for interruptible in [false, true] {
                let (first, second) = pair(marker().with_interruptible(interruptible));
                let held = first.pop_ready_independent(Instant::now()).unwrap();
                let other = popped.then(|| second.pop_ready_independent(Instant::now()).unwrap());
                second.interrupt();
                let group = &held.fence.as_ref().unwrap().fence;
                assert_eq!(*group.status.borrow() == Status::Revoked, interruptible);
                if !interruptible {
                    assert!(*group.status.borrow() == Status::Pending);
                    second.cancel();
                    assert!(*group.status.borrow() == Status::Aborted);
                }
                drop(other);
            }
        }
    }

    #[tokio::test]
    async fn popped_named_revocation_wins_without_rescheduling_arrived_waiter_and_successor_runs() {
        let (worker, ctx) = context().await;
        let stale = marker();
        let (first, second) = pair(stale.clone());
        let mut arrived = first
            .pop_ready_independent(Instant::now())
            .unwrap()
            .fence
            .unwrap();
        let mut last = second
            .pop_ready_independent(Instant::now())
            .unwrap()
            .fence
            .unwrap();
        let state = arrived.fence.clone();
        let completion = arrived.complete_independent(&ctx);
        tokio::pin!(completion);
        assert!(pending(completion.as_mut()).await);
        assert_eq!(
            state.state.lock().unwrap().remaining,
            1,
            "real first arrival established"
        );
        // Do not poll the arrived helper after the named interruption. The
        // queue's weak current handle must prevent another lane's final release.
        first.interrupt();
        let last_result = last.complete_independent(&ctx).await.unwrap();
        let arrived_result = completion.await.unwrap();

        let fresh = marker();
        MediaQueue::fence(&[first.clone(), second.clone()], fresh.clone()).unwrap();
        let mut a = first
            .pop_ready_independent(Instant::now())
            .unwrap()
            .fence
            .unwrap();
        let mut b = second
            .pop_ready_independent(Instant::now())
            .unwrap()
            .fence
            .unwrap();
        let waiting = a.complete_independent(&ctx);
        tokio::pin!(waiting);
        assert!(pending(waiting.as_mut()).await);
        let fresh_b = b.complete_independent(&ctx).await.unwrap();
        let fresh_a = waiting.await.unwrap();
        // Core reflects Flow markers before publishing the returning receipt.
        // Complete that public round trip before admitting normal End.
        let reflected = tokio::time::timeout(Duration::from_secs(3), event(&fresh).wait()).await;
        let frames = finish(worker).await;
        reflected.unwrap();
        assert_eq!(last_result, FenceOutcome::Revoked);
        assert_eq!(arrived_result, FenceOutcome::Revoked);
        assert_eq!(fresh_a, FenceOutcome::Completed);
        assert_eq!(fresh_b, FenceOutcome::Completed);
        assert!(!frames.iter().any(|f| f.frame.id == stale.id));
        assert_eq!(frames.iter().filter(|f| f.frame.id == fresh.id).count(), 1);
    }

    #[tokio::test]
    async fn uninterruptible_arrival_survives_epochs_but_cancel_and_worker_drop_abort() {
        let (worker, ctx) = context().await;
        let mut expected = vec![];
        let mut receipts = vec![];
        let mut outcomes = vec![];
        for action in 0..3 {
            let frame = marker().with_interruptible(false);
            let (first, second) = pair(frame.clone());
            let mut arrived = first
                .pop_ready_independent(Instant::now())
                .unwrap()
                .fence
                .unwrap();
            let other = second.pop_ready_independent(Instant::now()).unwrap();
            let group = arrived.fence.clone();
            let completion = arrived.complete_independent(&ctx);
            tokio::pin!(completion);
            assert!(pending(completion.as_mut()).await);
            assert_eq!(group.state.lock().unwrap().remaining, 1);
            first.interrupt();
            second.interrupt();
            if action == 0 {
                let mut other = other.fence.unwrap();
                outcomes.push(other.complete_independent(&ctx).await.unwrap());
                expected.push(frame.id);
                receipts.push(event(&frame));
            } else if action == 1 {
                second.cancel();
                drop(other);
            } else {
                // Actual participant ownership loss, distinct from an epoch.
                drop(other);
            }
            outcomes.push(completion.await.unwrap());
        }
        let mut reflection_ok = true;
        for receipt in receipts {
            reflection_ok &= tokio::time::timeout(Duration::from_secs(3), receipt.wait())
                .await
                .is_ok();
        }
        let frames = finish(worker).await;
        assert!(reflection_ok);
        assert_eq!(
            outcomes,
            vec![
                FenceOutcome::Completed,
                FenceOutcome::Completed,
                FenceOutcome::Aborted,
                FenceOutcome::Aborted
            ]
        );
        let markers: Vec<_> = frames
            .iter()
            .filter(|f| matches!(f.frame.data, FrameData::FlushDetailed(_)))
            .map(|f| f.frame.id)
            .collect();
        assert_eq!(markers, expected);
    }

    #[tokio::test]
    async fn legacy_complete_maps_revoked_to_false_before_arrival_without_counting_it() {
        let (worker, ctx) = context().await;
        let (first, second) = pair(marker());
        let mut fence = first.pop_ready(Instant::now()).unwrap().fence.unwrap();
        let group = fence.fence.clone();
        second.interrupt();
        let result = fence.complete(&ctx).await.unwrap();
        let remaining = group.state.lock().unwrap().remaining;
        let frames = finish(worker).await;
        assert!(!result);
        assert_eq!(remaining, 2);
        assert!(!frames
            .iter()
            .any(|f| matches!(f.frame.data, FrameData::FlushDetailed(_))));
    }

    #[tokio::test]
    async fn cancel_current_participant_blocks_final_arrival_without_waiter_repoll_or_drop() {
        let (worker, ctx) = context().await;
        let mut results = vec![];
        let mut ids = vec![];
        for interruptible in [false, true] {
            let frame = marker().with_interruptible(interruptible);
            ids.push(frame.id);
            let (first, second) = pair(frame);
            let mut arrived = first
                .pop_ready_independent(Instant::now())
                .unwrap()
                .fence
                .unwrap();
            let mut last = second
                .pop_ready_independent(Instant::now())
                .unwrap()
                .fence
                .unwrap();
            let group = arrived.fence.clone();
            let completion = arrived.complete_independent(&ctx);
            tokio::pin!(completion);
            assert!(pending(completion.as_mut()).await);
            assert_eq!(group.state.lock().unwrap().remaining, 1);
            // Neither participant is dropped, nor is the arrived future polled:
            // cancellation itself must abort the weak registered current fence.
            first.cancel();
            results.push(last.complete_independent(&ctx).await.unwrap());
            results.push(completion.await.unwrap());
        }
        let frames = finish(worker).await;
        assert_eq!(results, vec![FenceOutcome::Aborted; 4]);
        assert!(!frames.iter().any(|frame| ids.contains(&frame.frame.id)));
    }

    #[test]
    fn terminal_fence_status_precedes_new_failure_and_selected_failure_stays_aborted() {
        for cancel in [false, true] {
            let (first, second) = pair(marker());
            let held = first.pop_ready_independent(Instant::now()).unwrap();
            if cancel {
                second.cancel();
            } else {
                second.interrupt();
            }
            let expected = if cancel {
                FenceOutcome::Aborted
            } else {
                FenceOutcome::Revoked
            };
            assert_eq!(
                held.fence
                    .as_ref()
                    .unwrap()
                    .check_delivery(true, "prior image failed")
                    .unwrap(),
                Some(expected)
            );
        }
        let (first, second) = pair(marker());
        let held = first.pop_ready_independent(Instant::now()).unwrap();
        let fence = held.fence.as_ref().unwrap();
        let failure = fence
            .check_delivery(true, "prior image failed")
            .unwrap_err();
        second.interrupt();
        assert!(failure.fatal);
        assert_eq!(
            fence.check_delivery(false, "unused").unwrap(),
            Some(FenceOutcome::Aborted)
        );
    }
}
