//! Explicit non-live raw-image cycling. Repeats never enter the frame pipeline.
//!
//! The normal lane performs its own ordered first draw, rather than waiting on
//! this worker's queue: an all-lane flow fence must never create a wait cycle.
use super::{video, Destination, OutputBackend, OutputTransportConfig};
use crate::fence::FenceOutcome;
use crate::queue::{frame_bytes, MediaQueue};
use pipecat_core::*;
use std::future::Future;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::{watch, Mutex as AsyncMutex};
use tokio::time::Instant;

/// Opt-in, same-size raw-image output. No defaults or implicit backend opt-in.
/// Queue and retained budgets are per destination and additive. Retained image
/// slots count both the original activation and its callback copy until the
/// first successful draw forwards the original; afterwards only callbacks count.
/// Retained bytes conservatively include frame envelopes and all callback copies.
/// Old in-flight snapshots retain their charge until the last owner drops them.
#[derive(Debug, Clone, Copy)]
pub struct CyclicVideoOutputConfig {
    pub queue_capacity: usize,
    pub queue_byte_capacity: usize,
    pub retained_image_capacity: usize,
    pub retained_byte_capacity: usize,
    /// Positive fixed-delay rate: wait this period after each completed draw.
    pub framerate: u32,
}

pub(super) fn validate_config(config: CyclicVideoOutputConfig) -> Result<(), ProcessorError> {
    if config.queue_capacity == 0
        || config.queue_byte_capacity == 0
        || config.retained_image_capacity == 0
        || config.retained_byte_capacity == 0
        || !(1..=1_000_000_000).contains(&config.framerate)
        || config
            .queue_capacity
            .checked_add(config.retained_image_capacity)
            .is_none()
        || config
            .queue_byte_capacity
            .checked_add(config.retained_byte_capacity)
            .is_none()
    {
        return Err(ProcessorError::new(
            "invalid cyclic video output configuration",
        ));
    }
    Ok(())
}

struct Budget {
    used: Mutex<(usize, usize)>,
    image_limit: usize,
    byte_limit: usize,
}
struct Charge {
    budget: Arc<Budget>,
    images: usize,
    bytes: usize,
}
impl Charge {
    fn shrink(&mut self, images: usize, bytes: usize) {
        let mut used = self.budget.used.lock().unwrap();
        used.0 -= self.images - images;
        used.1 -= self.bytes - bytes;
        self.images = images;
        self.bytes = bytes;
    }
}
impl Drop for Charge {
    fn drop(&mut self) {
        let mut used = self.budget.used.lock().unwrap();
        used.0 -= self.images;
        used.1 -= self.bytes;
    }
}
impl Budget {
    fn reserve(self: &Arc<Self>, images: usize, bytes: usize) -> Result<Charge, ProcessorError> {
        let mut used = self.used.lock().unwrap();
        if images > self.image_limit.saturating_sub(used.0)
            || bytes > self.byte_limit.saturating_sub(used.1)
        {
            return Err(ProcessorError::new(
                "cyclic video retained image/byte budget exhausted",
            ));
        }
        used.0 += images;
        used.1 += bytes;
        Ok(Charge {
            budget: self.clone(),
            images,
            bytes,
        })
    }
}
struct CycleSet {
    images: Vec<Frame>,
    invocation: InvocationFence,
    epoch: u64,
    _charge: Charge,
}
#[derive(Clone)]
struct Active {
    set: Arc<CycleSet>,
    generation: u64,
    next: usize,
    deadline: Instant,
}
struct State {
    generation: u64,
    active: Option<Active>,
    ending: bool,
    cancelled: bool,
}

pub(super) struct CyclicVideo {
    pub(super) queue: Arc<MediaQueue>,
    activation_gate: AsyncMutex<()>,
    image_gate: AsyncMutex<()>,
    state: Mutex<State>,
    changed: watch::Sender<()>,
    budget: Arc<Budget>,
    period: Duration,
    incomplete: AtomicBool,
}
impl CyclicVideo {
    pub(super) fn new(config: CyclicVideoOutputConfig) -> Self {
        let (changed, _) = watch::channel(());
        Self {
            queue: MediaQueue::new(config.queue_capacity, config.queue_byte_capacity),
            activation_gate: AsyncMutex::new(()),
            image_gate: AsyncMutex::new(()),
            state: Mutex::new(State {
                generation: 0,
                active: None,
                ending: false,
                cancelled: false,
            }),
            changed,
            budget: Arc::new(Budget {
                used: Mutex::new((0, 0)),
                image_limit: config.retained_image_capacity,
                byte_limit: config.retained_byte_capacity,
            }),
            period: Duration::from_nanos(1_000_000_000_u64.div_ceil(u64::from(config.framerate))),
            incomplete: AtomicBool::new(false),
        }
    }
    pub(super) fn validate(&self, frame: &Frame) -> Result<(), ProcessorError> {
        frame
            .require_no_native_llm_continuation()
            .map_err(|e| ProcessorError::new(e.to_string()))?;
        if frame.error_origin().is_some()
            || frame.pts.is_some()
            || frame.has_pipeline_pts()
            || !frame.metadata.is_empty()
        {
            return Err(ProcessorError::new(
                "cyclic raw images cannot carry PTS, metadata, or error origin",
            ));
        }
        match &frame.data {
            FrameData::OutputImage(_) | FrameData::OutputImageDetailed(_) => Ok(()),
            FrameData::Sprite(sprite) if !sprite.images.is_empty() => Ok(()),
            FrameData::Sprite(_) => {
                Err(ProcessorError::new("cyclic video rejects an empty sprite"))
            }
            _ => Err(ProcessorError::new(
                "cyclic video requires a raw output image or sprite",
            )),
        }
    }
    pub(super) fn incomplete(&self) -> bool {
        self.incomplete.load(Ordering::SeqCst)
    }
    pub(super) fn interrupt(&self) {
        self.queue.interrupt();
        self.clear(false, false);
    }
    pub(super) fn end(&self) {
        self.queue.end();
        self.clear(true, false);
    }
    pub(super) fn cancel(&self) {
        self.queue.cancel();
        self.clear(false, true);
    }
    fn clear(&self, ending: bool, cancelled: bool) {
        let (old, exhausted) = {
            let mut state = self.state.lock().unwrap();
            state.ending |= ending;
            state.cancelled |= cancelled;
            let exhausted = match state.generation.checked_add(1) {
                Some(generation) => {
                    state.generation = generation;
                    false
                }
                None => {
                    state.cancelled = true;
                    true
                }
            };
            (state.active.take(), exhausted)
        };
        // Destruction, queue retirement and synchronous notification stay
        // outside the state lock, including the fail-closed exhaustion path.
        drop(old);
        if exhausted {
            self.queue.cancel();
        }
        self.changed.send_replace(());
    }
    fn begin_activation(&self) -> Result<u64, ProcessorError> {
        let (generation, old) = {
            let mut state = self.state.lock().unwrap();
            if state.cancelled {
                return Err(ProcessorError::new("cyclic video is cancelled"));
            }
            let generation = match state.generation.checked_add(1) {
                Some(generation) => {
                    state.generation = generation;
                    Ok(generation)
                }
                None => {
                    state.cancelled = true;
                    Err(ProcessorError::fatal("cyclic video generation exhausted"))
                }
            };
            (generation, state.active.take())
        };
        drop(old);
        if generation.is_err() {
            self.queue.cancel();
        }
        self.changed.send_replace(());
        generation
    }
    fn snapshot(&self) -> Option<Active> {
        self.state.lock().unwrap().active.clone()
    }
    fn is_generation(&self, generation: u64) -> bool {
        let state = self.state.lock().unwrap();
        !state.ending && !state.cancelled && state.generation == generation
    }
    fn fail_generation(&self, generation: u64, invocation: &InvocationFence) -> bool {
        // The original causal scopes and the destination replacement ticket
        // choose this failure together. A stale callback must not poison a
        // surviving/new generation in the gap after its async result returns.
        let selected = invocation.with_current(|| {
            let mut state = self.state.lock().unwrap();
            if state.generation != generation || state.ending || state.cancelled {
                return (false, None);
            }
            self.incomplete.store(true, Ordering::SeqCst);
            (true, state.active.take())
        });
        let Some((true, old)) = selected else {
            return false;
        };
        // Neither retained payload destruction nor wakeups/reporting occurs
        // under core admission or the cycle-state lock.
        drop(old);
        self.changed.send_replace(());
        true
    }
    fn retire_generation(&self, generation: u64) {
        let old = {
            let mut state = self.state.lock().unwrap();
            if state.generation != generation {
                return;
            }
            state.active.take()
        };
        drop(old);
        self.changed.send_replace(());
    }
}

/// Both ordinary singleton images and Sprite are independent. A Sprite child's
/// sync flag never reroutes the whole Sprite through the normal media queue.
pub(super) fn is_independent(frame: &Frame) -> bool {
    match &frame.data {
        FrameData::OutputImage(_) | FrameData::Sprite(_) => true,
        FrameData::OutputImageDetailed(image) => !image.sync_with_audio,
        _ => false,
    }
}

fn costs(frame: &Frame) -> Result<(usize, usize, usize, usize), ProcessorError> {
    let images = match &frame.data {
        FrameData::Sprite(sprite) => sprite.images.len(),
        _ => 1,
    };
    let envelope = frame
        .name
        .capacity()
        .checked_add(frame.transport_source.as_ref().map_or(0, String::capacity))
        .and_then(|n| {
            n.checked_add(
                frame
                    .transport_destination
                    .as_ref()
                    .map_or(0, String::capacity),
            )
        })
        .and_then(|n| n.checked_add(size_of::<Frame>()));
    let pixel_bytes = |image: &ImageRawFrame| {
        image
            .image
            .capacity()
            .checked_add(image.format.as_ref().map_or(0, String::capacity))
    };
    let payload = match &frame.data {
        FrameData::OutputImage(image) => pixel_bytes(image),
        FrameData::OutputImageDetailed(image) => pixel_bytes(&image.image),
        FrameData::Sprite(sprite) => sprite
            .images
            .iter()
            .try_fold(0usize, |n, image| n.checked_add(pixel_bytes(&image.image)?)),
        _ => None,
    };
    let callbacks = envelope
        .and_then(|n| n.checked_mul(images))
        .and_then(|n| n.checked_add(payload?))
        .and_then(|n| n.checked_add(size_of::<CycleSet>()))
        // One moved InvocationFence, at most 64 weak causal origins. No fence
        // clone is retained during activation or repeated callback snapshots.
        .and_then(|n| n.checked_add(8192));
    // The original may carry bounded local tags and an allocated empty map.
    // Fresh callback frames have neither. This is a conservative logical bound,
    // not allocator/RSS accounting for shared observer bookkeeping.
    let original = frame_bytes(frame)
        .checked_add(size_of::<Frame>())
        .and_then(|n| n.checked_add(LocalFrameTag::MAX_TAGS_PER_FRAME * size_of::<LocalFrameTag>()))
        .and_then(|n| {
            n.checked_add(12 * (size_of::<(String, serde_json::Value)>() + size_of::<usize>()))
        });
    match (images.checked_mul(2), callbacks, original) {
        (Some(total_images), Some(callbacks), Some(original)) => callbacks
            .checked_add(original)
            .map(|total| (images, total_images, callbacks, total)),
        _ => None,
    }
    .ok_or_else(|| ProcessorError::new("cyclic video retained budget overflow"))
}
fn callbacks(frame: &Frame) -> Vec<Frame> {
    let make = |data| {
        let mut image = Frame::new(data);
        // Clone precisely the public raw-image envelope. No metadata, authority,
        // original identity, callbacks or history receipt enters repeated draws.
        image.name = frame.name.clone();
        image.transport_source = frame.transport_source.clone();
        image.transport_destination = frame.transport_destination.clone();
        image.interruptible = frame.interruptible;
        image
    };
    match &frame.data {
        FrameData::OutputImage(image) => vec![make(FrameData::OutputImage(image.clone()))],
        FrameData::OutputImageDetailed(image) => {
            vec![make(FrameData::OutputImageDetailed(image.clone()))]
        }
        FrameData::Sprite(sprite) => sprite
            .images
            .iter()
            .map(|image| make(FrameData::OutputImageDetailed(image.clone())))
            .collect(),
        _ => unreachable!("validated raw-image profile"),
    }
}

/// A raw scope is checked before callback polling and again before completion.
/// Inherited origins have no notification API; a small cancellation-only timer
/// bounds observation latency without producing draws or catch-up ticks. As with
/// all backend hooks, these checks cannot make a device write remotely atomic.
async fn scoped_operation<T>(
    cycle: &CyclicVideo,
    queue: &MediaQueue,
    epochs: (u64, u64),
    invocation: &InvocationFence,
    generation: Option<u64>,
    timeout: Duration,
    future: impl Future<Output = Result<T, ProcessorError>>,
) -> Option<Result<T, ProcessorError>> {
    let (epoch, cycle_epoch) = epochs;
    let mut changed = queue.changed.subscribe();
    let mut replaced = cycle.changed.subscribe();
    let future = super::bounded(timeout, future);
    tokio::pin!(future);
    loop {
        // A held repeat must yield to finite queued work, including its next
        // replacement or flush. Register before checking to avoid a lost wake.
        let queued = queue.available.notified();
        tokio::pin!(queued);
        if generation.is_some() {
            queued.as_mut().enable();
        }
        let signal = *changed.borrow_and_update();
        let closed = cycle.state.lock().unwrap().cancelled;
        if generation.is_some() && !queue.is_empty() {
            return None;
        }
        if signal.cancelled
            || signal.epoch > epoch
            || cycle.queue.signal().epoch > cycle_epoch
            || closed
            || !invocation.is_current()
            || generation.is_some_and(|generation| !cycle.is_generation(generation))
        {
            return None;
        }
        tokio::select! { biased;
            _ = changed.changed() => {},
            _ = replaced.changed() => {},
            _ = &mut queued, if generation.is_some() => {},
            _ = tokio::time::sleep(Duration::from_millis(10)) => {},
            result = &mut future => {
                let signal = queue.signal();
                if signal.cancelled || signal.epoch > epoch || cycle.queue.signal().epoch > cycle_epoch
                    || cycle.state.lock().unwrap().cancelled
                    || !invocation.is_current()
                    || generation.is_some_and(|generation| !cycle.is_generation(generation))
                { return None; }
                return Some(result);
            },
        }
    }
}

async fn wait_runnable(queue: &MediaQueue, epoch: u64) -> bool {
    super::wait_delivery(queue, epoch, true).await
}

/// First draw and original-frame acknowledgement are owned by the calling lane.
/// Two activation lanes can never cancel each other's incomplete first draw.
pub(super) async fn activate(
    cycle: &CyclicVideo,
    queue: &MediaQueue,
    backend: &Arc<dyn OutputBackend>,
    ctx: &ProcessorContext,
    item: (Frame, u64, InvocationFence),
    timeout: Duration,
) -> Result<(), ProcessorError> {
    let (frame, epoch, invocation) = item;
    let cycle_epoch = cycle.queue.signal().epoch;
    cycle.validate(&frame)?;
    let (image_count, total_images, callback_bytes, total_bytes) = costs(&frame)?;
    // Reserve before cloning or waiting. This counts both lanes' pending
    // activations and every old in-flight snapshot, not only the active slot.
    let mut charge = cycle.budget.reserve(total_images, total_bytes)?;
    let images = callbacks(&frame);
    let result = scoped_operation(
        cycle,
        queue,
        (epoch, cycle_epoch),
        &invocation,
        None,
        timeout,
        async {
            let activation_gate = cycle.activation_gate.lock().await;
            if !wait_runnable(queue, epoch).await {
                return Ok(None);
            }
            let generation = cycle.begin_activation()?;
            let image_gate = cycle.image_gate.lock().await;
            if !wait_runnable(queue, epoch).await || !invocation.is_current() {
                return Ok(None);
            }
            backend.write_image(&images[0]).await?;
            Ok(Some((activation_gate, image_gate, generation)))
        },
    )
    .await;
    let Some(result) = result else {
        return Ok(());
    };
    let Some((_activation_gate, _image_gate, generation)) = result? else {
        return Ok(());
    };
    // Queue cutoff and original causal scope both guard the sole acknowledgement.
    // No state lock is held while entering core admission or destroying a frame.
    queue.forward_if_current_image(epoch, true, || {
        ctx.try_push_with_invocation_fence(frame, Direction::Downstream, &invocation)
    })?;
    charge.shrink(image_count, callback_bytes);
    let deadline = Instant::now()
        .checked_add(cycle.period)
        .ok_or_else(|| ProcessorError::new("cyclic video delay overflow"))?;
    let set = Arc::new(CycleSet {
        images,
        invocation,
        epoch: cycle_epoch,
        _charge: charge,
    });
    let mut proposed = Some(Active {
        next: usize::from(image_count > 1),
        set: set.clone(),
        generation,
        deadline,
    });
    set.invocation.with_current(|| {
        let mut state = cycle.state.lock().unwrap();
        if !state.ending && !state.cancelled && state.generation == generation {
            debug_assert!(state.active.is_none());
            state.active = proposed.take();
        }
    });
    drop(proposed);
    cycle.changed.send_replace(());
    Ok(())
}

pub(super) struct CycleSender {
    destination: Arc<Destination>,
    cycle: Arc<CyclicVideo>,
    backend: Arc<dyn OutputBackend>,
    config: OutputTransportConfig,
    ctx: ProcessorContext,
    gracefully_drained: bool,
}
impl Drop for CycleSender {
    fn drop(&mut self) {
        if self.gracefully_drained {
            self.cycle.queue.cancel();
            self.cycle.clear(true, false);
        } else {
            self.cycle.cancel();
        }
    }
}
impl CycleSender {
    pub(super) fn new(
        destination: Arc<Destination>,
        cycle: Arc<CyclicVideo>,
        backend: Arc<dyn OutputBackend>,
        config: OutputTransportConfig,
        ctx: ProcessorContext,
    ) -> Self {
        Self {
            destination,
            cycle,
            backend,
            config,
            ctx,
            gracefully_drained: false,
        }
    }

    pub(super) async fn run(mut self) -> Result<(), ProcessorError> {
        let mut changed = self.cycle.queue.changed.subscribe();
        let mut replaced = self.cycle.changed.subscribe();
        loop {
            let signal = self.cycle.queue.signal();
            if signal.cancelled {
                return Ok(());
            }
            if signal.paused && !signal.ending {
                changed.changed().await.map_err(crate::error)?;
                continue;
            }
            if let Some(mut item) = self.cycle.queue.pop_ready_independent(Instant::now()) {
                if item.frame.interruptible && self.cycle.queue.signal().epoch > item.epoch {
                    if let Some(fence) = &item.fence {
                        fence.revoke();
                    }
                    continue;
                }
                if let Some(mut fence) = item.fence.take() {
                    let result = async {
                        if let Some(outcome) = fence.check_delivery(
                            self.cycle.incomplete()
                                || !self.destination.usable.load(Ordering::SeqCst),
                            "preceding cyclic image lacks a completed delivery acknowledgement",
                        )? {
                            return Ok(outcome);
                        }
                        video::complete_fence(
                            &self.cycle.queue,
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
                } else {
                    let result = match item.invocation {
                        Some(invocation) => {
                            activate(
                                &self.cycle,
                                &self.cycle.queue,
                                &self.backend,
                                &self.ctx,
                                (item.frame, item.epoch, invocation),
                                self.config.write_timeout,
                            )
                            .await
                        }
                        None => Err(ProcessorError::new(
                            "cyclic image activation lacks its original invocation",
                        )),
                    };
                    if let Err(problem) = result {
                        self.cycle.incomplete.store(true, Ordering::SeqCst);
                        self.report(problem).await?;
                    }
                }
                continue;
            }
            if signal.ending && self.cycle.queue.is_empty() {
                self.gracefully_drained = true;
                return Ok(());
            }
            let active = self.cycle.snapshot();
            if let Some(active) = &active {
                if !active.set.invocation.is_current() || active.set.epoch < signal.epoch {
                    self.cycle.retire_generation(active.generation);
                    continue;
                }
                if active.deadline <= Instant::now() {
                    self.draw_repeat(active.clone()).await?;
                    continue;
                }
            }
            let available = self.cycle.queue.available.notified();
            let timer = async {
                if let Some(active) = &active {
                    tokio::time::sleep_until(active.deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! { biased;
                _ = changed.changed() => {},
                _ = replaced.changed() => {},
                _ = available => {},
                _ = timer => {},
            }
        }
    }
    async fn draw_repeat(&self, active: Active) -> Result<(), ProcessorError> {
        let result = scoped_operation(
            &self.cycle,
            &self.cycle.queue,
            (active.set.epoch, active.set.epoch),
            &active.set.invocation,
            Some(active.generation),
            self.config.write_timeout,
            async {
                let _gate = self.cycle.image_gate.lock().await;
                if !wait_runnable(&self.cycle.queue, active.set.epoch).await
                    || !active.set.invocation.is_current()
                    || !self.cycle.is_generation(active.generation)
                {
                    return Ok(false);
                }
                self.backend
                    .write_image(&active.set.images[active.next])
                    .await?;
                Ok(true)
            },
        )
        .await;
        match result {
            Some(Ok(true)) => {
                let deadline = Instant::now()
                    .checked_add(self.cycle.period)
                    .ok_or_else(|| ProcessorError::new("cyclic video delay overflow"))?;
                let mut state = self.cycle.state.lock().unwrap();
                if let Some(current) = &mut state.active {
                    if current.generation == active.generation {
                        current.next = (active.next + 1) % active.set.images.len();
                        current.deadline = deadline;
                    }
                }
            }
            Some(Err(problem))
                if self
                    .cycle
                    .fail_generation(active.generation, &active.set.invocation) =>
            {
                // A retired callback's late failure cannot poison its replacement.
                self.report(problem).await?;
            }
            _ => {}
        }
        Ok(())
    }
    async fn report(&self, problem: ProcessorError) -> Result<(), ProcessorError> {
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> CyclicVideoOutputConfig {
        CyclicVideoOutputConfig {
            queue_capacity: 8,
            queue_byte_capacity: 64 * 1024,
            retained_image_capacity: 8,
            retained_byte_capacity: 128 * 1024,
            framerate: 30,
        }
    }
    fn image() -> ImageRawFrame {
        ImageRawFrame {
            image: vec![1, 2, 3],
            size: (1, 1),
            format: Some("RGB".into()),
        }
    }
    #[test]
    fn raw_profile_and_sprite_child_routing_are_explicit() {
        let cycle = CyclicVideo::new(config());
        let sprite = Frame::new(FrameData::Sprite(SpriteFrame {
            images: vec![
                OutputImageRawFrame {
                    image: image(),
                    sync_with_audio: true,
                },
                OutputImageRawFrame {
                    image: image(),
                    sync_with_audio: false,
                },
            ],
        }));
        cycle.validate(&sprite).unwrap();
        assert!(is_independent(&sprite));
        let sync = Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
            image: image(),
            sync_with_audio: true,
        }));
        cycle.validate(&sync).unwrap();
        assert!(!is_independent(&sync));
        let mut relative = sync.clone();
        relative.set_relative_pts(Some(0));
        assert!(cycle.validate(&relative).is_err());
        let mut pipeline = sync.clone();
        pipeline.set_pipeline_pts(0);
        assert!(cycle.validate(&pipeline).is_err());
        let mut metadata = sync;
        metadata
            .metadata
            .insert("note".into(), serde_json::Value::Null);
        assert!(cycle.validate(&metadata).is_err());
        assert!(cycle
            .validate(&Frame::new(FrameData::Sprite(SpriteFrame {
                images: vec![]
            })))
            .is_err());
    }
    #[test]
    fn charges_include_both_activation_copies_and_spare_allocations() {
        let mut frame = Frame::new(FrameData::OutputImage(image()));
        let (count, total_count, callbacks, total) = costs(&frame).unwrap();
        assert_eq!((count, total_count), (1, 2));
        assert!(callbacks >= 8192);
        assert!(total > callbacks + frame_bytes(&frame));
        let before = total;
        let old_capacity = frame.name.capacity() + image().image.capacity();
        frame.name.reserve(4096);
        frame.transport_destination = Some(String::with_capacity(8192));
        if let FrameData::OutputImage(image) = &mut frame.data {
            image.image.reserve(32768);
        }
        let (_, _, _, after) = costs(&frame).unwrap();
        let FrameData::OutputImage(image) = &frame.data else {
            unreachable!()
        };
        let new_capacity = frame.name.capacity()
            + frame.transport_destination.as_ref().unwrap().capacity()
            + image.image.capacity();
        // reserve(additional) guarantees len + additional, not old capacity +
        // additional. Charge the actual allocation deltas of both copies.
        assert_eq!(after - before, 2 * (new_capacity - old_capacity));
    }
    #[test]
    fn shared_snapshot_charge_is_released_only_after_last_owner() {
        let budget = Arc::new(Budget {
            used: Mutex::new((0, 0)),
            image_limit: 3,
            byte_limit: 30,
        });
        let charge = budget.reserve(2, 20).unwrap();
        let active = Arc::new(charge);
        let in_flight = active.clone();
        drop(active);
        assert!(budget.reserve(2, 20).is_err());
        assert_eq!(*budget.used.lock().unwrap(), (2, 20));
        drop(in_flight);
        let mut replacement = budget.reserve(2, 20).unwrap();
        replacement.shrink(1, 10);
        assert_eq!(*budget.used.lock().unwrap(), (1, 10));
        let pending = budget.reserve(2, 20).unwrap();
        assert!(budget.reserve(1, 1).is_err());
        drop((pending, replacement));
        assert_eq!(*budget.used.lock().unwrap(), (0, 0));
    }
    struct Capture {
        invocation: Mutex<Option<InvocationFence>>,
        ready: tokio::sync::Semaphore,
    }
    #[async_trait::async_trait]
    impl FrameProcessor for Capture {
        fn name(&self) -> &str {
            "cyclic-failure-scope-capture"
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            let start = frame.data.is_start();
            if start {
                *self.invocation.lock().unwrap() = Some(ctx.invocation_fence()?);
            }
            ctx.push(frame, direction).await?;
            if start {
                self.ready.add_permits(1);
            }
            Ok(())
        }
    }
    async fn scope() -> (PipelineWorker, InvocationFence) {
        let capture = Arc::new(Capture {
            invocation: Mutex::new(None),
            ready: tokio::sync::Semaphore::new(0),
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
        let invocation = capture.invocation.lock().unwrap().clone().unwrap();
        (worker, invocation)
    }
    async fn finish(worker: PipelineWorker, cancel: bool) {
        if cancel {
            worker.cancel("retire test invocation").await.unwrap();
        } else {
            worker.end().await.unwrap();
        }
        let (_, report) = tokio::time::timeout(Duration::from_secs(3), worker.collect())
            .await
            .unwrap()
            .unwrap();
        assert!(report.errors.is_empty(), "{:?}", report.errors);
    }
    #[tokio::test]
    async fn stale_generation_failure_cannot_poison_its_replacement() {
        let (worker, invocation) = scope().await;
        let cycle = CyclicVideo::new(config());
        let old = cycle.begin_activation().unwrap();
        let fresh = cycle.begin_activation().unwrap();
        let old_selected = cycle.fail_generation(old, &invocation);
        let old_incomplete = cycle.incomplete();
        let fresh_selected = cycle.fail_generation(fresh, &invocation);
        let fresh_incomplete = cycle.incomplete();
        finish(worker, false).await;
        assert!(!old_selected);
        assert!(!old_incomplete);
        assert!(fresh_selected);
        assert!(fresh_incomplete);
    }
    #[tokio::test]
    async fn retired_original_scope_cannot_publish_a_delivery_failure() {
        let (worker, invocation) = scope().await;
        let cycle = CyclicVideo::new(config());
        let current = cycle.begin_activation().unwrap();
        assert!(invocation.is_current());
        finish(worker, true).await;
        assert!(!invocation.is_current());
        assert!(!cycle.fail_generation(current, &invocation));
        assert!(!cycle.incomplete());
    }
    #[tokio::test]
    async fn generation_exhaustion_closes_instead_of_reusing_a_ticket() {
        let (worker, invocation) = scope().await;
        let mut outcomes = vec![];
        for activation in [false, true] {
            let cycle = CyclicVideo::new(config());
            let frame = Frame::new(FrameData::OutputImage(image()));
            let (_, _, bytes, _) = costs(&frame).unwrap();
            let charge = cycle.budget.reserve(1, bytes).unwrap();
            {
                let mut state = cycle.state.lock().unwrap();
                state.generation = u64::MAX;
                state.active = Some(Active {
                    set: Arc::new(CycleSet {
                        images: callbacks(&frame),
                        invocation: invocation.clone(),
                        epoch: 0,
                        _charge: charge,
                    }),
                    generation: u64::MAX,
                    next: 0,
                    deadline: Instant::now(),
                });
            }
            let fatal = if activation {
                cycle.begin_activation().unwrap_err().fatal
            } else {
                cycle.clear(false, false);
                true
            };
            let (cancelled, empty) = {
                let state = cycle.state.lock().unwrap();
                (state.cancelled, state.active.is_none())
            };
            outcomes.push((
                fatal,
                cancelled,
                empty,
                cycle.queue.signal().cancelled,
                *cycle.budget.used.lock().unwrap(),
            ));
        }
        finish(worker, false).await;
        for (fatal, cancelled, empty, queue_cancelled, used) in outcomes {
            assert!(fatal && cancelled && empty && queue_cancelled);
            assert_eq!(used, (0, 0));
        }
    }
}
