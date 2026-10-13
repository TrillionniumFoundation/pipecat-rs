//! Opt-in live-image lane. No PCM state or independent lifecycle owner lives here.
use super::{
    operation, wait_delivery, Destination, LiveVideoOutputConfig, OutputBackend,
    OutputTransportConfig,
};
use crate::fence::{FenceOutcome, FenceParticipant};
use crate::queue::{Item, MediaQueue};
use crate::{error, SpokenHistory};
use pipecat_core::*;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::Instant;

pub(super) struct LiveVideo {
    pub(super) queue: Arc<MediaQueue>,
    image_gate: Mutex<()>,
    period: Option<Duration>,
}
impl LiveVideo {
    pub(super) fn new(config: LiveVideoOutputConfig) -> Self {
        Self {
            queue: MediaQueue::new(config.queue_capacity, config.queue_byte_capacity),
            image_gate: Mutex::new(()),
            period: config
                .framerate
                .map(|fps| Duration::from_nanos(1_000_000_000_u64.div_ceil(u64::from(fps)))),
        }
    }
}

pub(super) fn is_live_image(frame: &Frame) -> bool {
    match &frame.data {
        FrameData::OutputImage(_) => true,
        FrameData::OutputImageDetailed(image) => !image.sync_with_audio,
        FrameData::UrlImage(image) => !image.image.sync_with_audio,
        FrameData::AssistantImage(image) => !image.image.sync_with_audio,
        _ => false,
    }
}

/// Readiness/PTS/FPS waits precede this operation. Once competing for the gate,
/// acquisition, a newly discovered pause, and the callback share one absolute
/// timeout. Keep the acquired gate through success-forward, without extending
/// that backend deadline or serializing any PCM operation.
pub(super) async fn write_image(
    live: &LiveVideo,
    queue: &MediaQueue,
    backend: &Arc<dyn OutputBackend>,
    ctx: &ProcessorContext,
    item: (Frame, u64),
    timeout: Duration,
    mut last_attempt: Option<&mut Option<(u64, Instant)>>,
) -> Result<(), ProcessorError> {
    let (frame, epoch) = item;
    if !wait_delivery(queue, epoch, frame.interruptible).await {
        return Ok(());
    }
    let result = operation(queue, epoch, frame.interruptible, timeout, async {
        let gate = live.image_gate.lock().await;
        if !wait_delivery(queue, epoch, frame.interruptible).await {
            return Ok(None);
        }
        if let Some(last) = last_attempt.as_mut() {
            **last = Some((queue.signal().epoch, Instant::now()));
        }
        backend.write_image(&frame).await?;
        Ok(Some(gate))
    })
    .await;
    let Some(result) = result else {
        return Ok(());
    };
    let Some(_gate) = result? else {
        return Ok(());
    };
    // Successful downstream admission is irreversible. Share its final check
    // with queue cutoff transitions, including concurrent runtime threads.
    queue.forward_if_current_image(epoch, frame.interruptible, || {
        ctx.try_push(frame, Direction::Downstream)
    })
}

/// A popped participant is outside queue retention. Observe its epoch during
/// rendezvous too, then explicitly revoke before Drop so siblings can distinguish
/// expected interruption from worker failure. The fence is not a backend call:
/// it has no write_timeout, and the pipeline retains its overall drain bound.
pub(super) async fn complete_fence(
    queue: &MediaQueue,
    epoch: u64,
    interruptible: bool,
    fence: &mut FenceParticipant,
    ctx: &ProcessorContext,
) -> Result<FenceOutcome, ProcessorError> {
    let mut changed = queue.changed.subscribe();
    enum Wake {
        Finished(Result<FenceOutcome, ProcessorError>),
        Revoked,
        Cancelled,
    }
    let wake = {
        let completion = fence.complete_independent(ctx);
        tokio::pin!(completion);
        loop {
            let signal = *changed.borrow_and_update();
            if signal.cancelled {
                break Wake::Cancelled;
            }
            if interruptible && signal.epoch > epoch {
                break Wake::Revoked;
            }
            tokio::select! { biased;
                result = changed.changed() => {
                    if result.is_err() {
                        break Wake::Cancelled;
                    }
                },
                result = &mut completion => break Wake::Finished(result),
            }
        }
    };
    match wake {
        Wake::Finished(result) => result,
        Wake::Revoked => {
            fence.revoke();
            Ok(FenceOutcome::Revoked)
        }
        Wake::Cancelled => Ok(FenceOutcome::Aborted),
    }
}

pub(super) struct VideoSender {
    destination: Arc<Destination>,
    live: Arc<LiveVideo>,
    backend: Arc<dyn OutputBackend>,
    config: OutputTransportConfig,
    ctx: ProcessorContext,
    started_at: Instant,
    clock_origin_ns: u64,
    epoch: u64,
    last_attempt: Option<(u64, Instant)>,
    incomplete: bool,
    spoken_history: Option<SpokenHistory>,
}
impl Drop for VideoSender {
    fn drop(&mut self) {
        // Normal media exit must not close video, nor video exit media/urgent.
        // Dropping queued participants still releases a failed rendezvous.
        self.live.queue.cancel();
    }
}
impl VideoSender {
    pub(super) fn new(
        destination: Arc<Destination>,
        live: Arc<LiveVideo>,
        backend: Arc<dyn OutputBackend>,
        config: OutputTransportConfig,
        ctx: ProcessorContext,
        origin: (Instant, u64),
        spoken_history: Option<SpokenHistory>,
    ) -> Self {
        let (started_at, clock_origin_ns) = origin;
        Self {
            destination,
            live,
            backend,
            config,
            ctx,
            started_at,
            clock_origin_ns,
            epoch: 0,
            last_attempt: None,
            incomplete: false,
            spoken_history,
        }
    }
    pub(super) async fn run(mut self) -> Result<(), ProcessorError> {
        let mut changed = self.live.queue.changed.subscribe();
        loop {
            let signal = self.live.queue.signal();
            self.observe_epoch(signal.epoch);
            if signal.cancelled {
                return Ok(());
            }
            if signal.paused && !signal.ending {
                changed.changed().await.map_err(error)?;
                continue;
            }
            if let Some(mut item) = self.live.queue.pop_ready_independent(Instant::now()) {
                if item.frame.interruptible && item.epoch < self.epoch {
                    if let Some(fence) = &item.fence {
                        fence.revoke();
                    }
                    continue;
                }
                if let Some(mut fence) = item.fence.take() {
                    let result = async {
                        let signal = self.live.queue.signal();
                        if signal.cancelled {
                            return Ok(FenceOutcome::Aborted);
                        }
                        if item.frame.interruptible && signal.epoch > item.epoch {
                            fence.revoke();
                            return Ok(FenceOutcome::Revoked);
                        }
                        if let Some(outcome) = fence.check_delivery(
                            self.incomplete
                                || !self
                                    .destination
                                    .usable
                                    .load(std::sync::atomic::Ordering::SeqCst),
                            "preceding output lacks a completed delivery acknowledgement",
                        )? {
                            return Ok(outcome);
                        }
                        complete_fence(
                            &self.live.queue,
                            item.epoch,
                            item.frame.interruptible,
                            &mut fence,
                            &self.ctx,
                        )
                        .await
                    }
                    .await;
                    match result {
                        Ok(FenceOutcome::Completed | FenceOutcome::Revoked) => {}
                        Ok(FenceOutcome::Aborted) => return Ok(()),
                        Err(problem) => {
                            self.report(problem).await?;
                            return Ok(());
                        }
                    }
                } else if let Err(problem) = self.handle(item).await {
                    self.incomplete = true;
                    self.report(problem).await?;
                }
                continue;
            }
            if signal.ending && self.live.queue.is_empty() {
                return Ok(());
            }
            let available = self.live.queue.available.notified();
            let deadline = self.live.queue.next_deadline();
            let timed = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = available => {},
                _ = timed => {},
                _ = changed.changed() => {},
            }
        }
    }
    fn observe_epoch(&mut self, epoch: u64) {
        if epoch != self.epoch {
            self.epoch = epoch;
            // An uninterruptible gate waiter can start its callback after the
            // interruption but before this worker next observes the epoch.
            // Keep that new-generation pacing slot rather than allowing a burst.
            self.last_attempt = self
                .last_attempt
                .filter(|(attempt_epoch, _)| *attempt_epoch >= epoch);
        }
    }
    async fn handle(&mut self, item: Item) -> Result<(), ProcessorError> {
        let Item {
            mut frame, epoch, ..
        } = item;
        let pts = if frame.has_pipeline_pts() {
            // The shared-clock admission deadline already passed in pop_ready.
            match frame.rebase_pipeline_pts(self.clock_origin_ns) {
                Ok(()) => {}
                Err(FrameTimingError::BeforeOrigin) => frame.set_relative_pts(Some(0)),
                Err(problem) => return Err(ProcessorError::new(problem.to_string())),
            }
            None
        } else {
            frame
                .pts
                .map(|pts| {
                    self.started_at
                        .checked_add(Duration::from_nanos(pts))
                        .ok_or_else(|| ProcessorError::new("legacy output PTS deadline overflow"))
                })
                .transpose()?
        };
        if !self.wait_slot(epoch, frame.interruptible, pts).await? {
            return Ok(());
        }
        write_image(
            &self.live,
            &self.live.queue,
            &self.backend,
            &self.ctx,
            (frame, epoch),
            self.config.write_timeout,
            Some(&mut self.last_attempt),
        )
        .await
    }
    async fn wait_slot(
        &mut self,
        epoch: u64,
        interruptible: bool,
        pts: Option<Instant>,
    ) -> Result<bool, ProcessorError> {
        let mut changed = self.live.queue.changed.subscribe();
        loop {
            let signal = *changed.borrow_and_update();
            self.observe_epoch(signal.epoch);
            if signal.cancelled || (interruptible && signal.epoch > epoch) {
                return Ok(false);
            }
            if signal.paused && !signal.ending {
                if changed.changed().await.is_err() {
                    return Ok(false);
                }
                continue;
            }
            let pacing = self
                .last_attempt
                .zip(self.live.period)
                .map(|((_, last), period)| {
                    last.checked_add(period)
                        .ok_or_else(|| ProcessorError::new("live video pacing deadline overflow"))
                })
                .transpose()?;
            let deadline = pts.into_iter().chain(pacing).max();
            if deadline.is_none_or(|deadline| deadline <= Instant::now()) {
                return Ok(true);
            }
            tokio::select! { biased;
                result = changed.changed() => {
                    if result.is_err() { return Ok(false); }
                },
                _ = tokio::time::sleep_until(deadline.expect("future deadline")) => {},
            }
        }
    }
    async fn report(&self, problem: ProcessorError) -> Result<(), ProcessorError> {
        // Images are not speech evidence. Still-pending contexts retain the
        // conservative delivery-failure policy; committed contexts stay intact.
        if let Some(history) = &self.spoken_history {
            history.invalidate_reason("live image transformation or delivery failed");
        }
        self.ctx
            .push_error(ErrorFrame {
                error: problem.message,
                fatal: problem.fatal,
                processor: None,
                category: problem.category,
            })
            .await
    }
}
