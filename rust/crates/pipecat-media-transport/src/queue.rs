use crate::fence::{FenceCancellation, FenceParticipant};
use pipecat_core::{Frame, FrameData, InvocationFence, ProcessorError};
use std::collections::{BTreeMap, VecDeque};
use std::future::{poll_fn, Future};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::Duration;
use tokio::sync::{watch, Notify};
use tokio::time::Instant;

#[derive(Clone, Copy, Default)]
pub(crate) struct Signal {
    pub epoch: u64,
    pub paused: bool,
    pub ending: bool,
    pub cancelled: bool,
}
pub(crate) struct Item {
    pub frame: Frame,
    pub invocation: Option<InvocationFence>,
    pub epoch: u64,
    pub fence: Option<FenceParticipant>,
    deadline: Option<Instant>,
    charged_bytes: usize,
}
struct State {
    frames: VecDeque<Item>,
    bytes: usize,
    signal: Signal,
    // Opt-in serial workers register their one popped participant atomically
    // with pop. No strong frame/group retention; default pop is unchanged.
    current_fence: Option<(u64, bool, FenceCancellation)>,
}
pub(crate) struct MediaQueue {
    state: Mutex<State>,
    count_limit: usize,
    byte_limit: usize,
    pub changed: watch::Sender<Signal>,
    pub available: Notify,
    capacity_available: Notify,
}
#[derive(Clone, Copy)]
enum AdmissionError {
    Full,
    Closed,
    Interrupted,
}
impl AdmissionError {
    fn message(self) -> &'static str {
        match self {
            Self::Full => "bounded media queue is full",
            Self::Closed => "media queue is closed",
            Self::Interrupted => "media queue admission interrupted",
        }
    }
}
impl MediaQueue {
    pub fn new(count_limit: usize, byte_limit: usize) -> Arc<Self> {
        let signal = Signal::default();
        let (changed, _) = watch::channel(signal);
        Arc::new(Self {
            state: Mutex::new(State {
                frames: VecDeque::new(),
                bytes: 0,
                signal,
                current_fence: None,
            }),
            count_limit,
            byte_limit,
            changed,
            available: Notify::new(),
            capacity_available: Notify::new(),
        })
    }
    pub fn push(&self, frame: Frame) -> Result<(), ProcessorError> {
        self.enqueue(frame, false, None)
    }
    pub fn push_front(&self, frame: Frame) -> Result<(), ProcessorError> {
        self.enqueue(frame, true, None)
    }
    pub fn push_timed(&self, frame: Frame, deadline: Instant) -> Result<(), ProcessorError> {
        self.enqueue(frame, false, Some(deadline))
    }
    /// Only the caller's current frame is retained while bounded capacity is
    /// unavailable. Register before the atomic attempt to avoid losing a pop.
    pub async fn push_wait(
        &self,
        frame: Frame,
        deadline: Option<Instant>,
        timeout: Duration,
    ) -> Result<(), ProcessorError> {
        self.push_wait_inner(frame, deadline, timeout, None).await
    }
    /// Retain the original cyclic-image invocation across bounded admission.
    pub async fn push_wait_with_invocation(
        &self,
        frame: Frame,
        deadline: Option<Instant>,
        timeout: Duration,
        invocation: InvocationFence,
    ) -> Result<(), ProcessorError> {
        self.push_wait_inner(frame, deadline, timeout, Some(invocation))
            .await
    }
    async fn push_wait_inner(
        &self,
        frame: Frame,
        deadline: Option<Instant>,
        timeout: Duration,
        mut invocation: Option<InvocationFence>,
    ) -> Result<(), ProcessorError> {
        let bytes = scoped_frame_bytes(&frame, invocation.is_some());
        if self.count_limit == 0 || bytes > self.byte_limit {
            return Err(ProcessorError::new(
                "bounded media queue is full: frame exceeds destination capacity",
            ));
        }
        let expires = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| ProcessorError::new("media queue admission deadline overflow"))?;
        let epoch = self.signal().epoch;
        let mut frame = Some(frame);
        loop {
            let capacity = self.capacity_available.notified();
            tokio::pin!(capacity);
            capacity.as_mut().enable();
            match self.try_enqueue(
                &mut frame,
                false,
                deadline,
                Some(epoch),
                bytes,
                &mut invocation,
            ) {
                Ok(()) | Err(AdmissionError::Interrupted) => return Ok(()),
                Err(AdmissionError::Closed) => {
                    return Err(ProcessorError::new(AdmissionError::Closed.message()));
                }
                Err(AdmissionError::Full) => {}
            }
            if tokio::time::timeout_at(expires, capacity).await.is_err()
                || Instant::now() >= expires
            {
                return Err(ProcessorError::new(
                    "bounded media queue admission timed out",
                ));
            }
        }
    }
    pub fn push_with_invocation(
        &self,
        frame: Frame,
        deadline: Option<Instant>,
        invocation: InvocationFence,
    ) -> Result<(), ProcessorError> {
        let bytes = scoped_frame_bytes(&frame, true);
        self.try_enqueue(
            &mut Some(frame),
            false,
            deadline,
            None,
            bytes,
            &mut Some(invocation),
        )
        .map_err(|error| ProcessorError::new(error.message()))
    }
    fn enqueue(
        &self,
        frame: Frame,
        urgent: bool,
        deadline: Option<Instant>,
    ) -> Result<(), ProcessorError> {
        let bytes = frame_bytes(&frame);
        self.try_enqueue(&mut Some(frame), urgent, deadline, None, bytes, &mut None)
            .map_err(|error| ProcessorError::new(error.message()))
    }
    fn try_enqueue(
        &self,
        frame: &mut Option<Frame>,
        urgent: bool,
        deadline: Option<Instant>,
        expected_epoch: Option<u64>,
        bytes: usize,
        invocation: &mut Option<InvocationFence>,
    ) -> Result<(), AdmissionError> {
        let mut state = self.state.lock().unwrap();
        if state.signal.ending || state.signal.cancelled {
            return Err(AdmissionError::Closed);
        }
        if frame
            .as_ref()
            .expect("admission owns its frame")
            .interruptible
            && expected_epoch.is_some_and(|epoch| epoch < state.signal.epoch)
        {
            return Err(AdmissionError::Interrupted);
        }
        if state.frames.len() >= self.count_limit
            || bytes > self.byte_limit.saturating_sub(state.bytes)
        {
            return Err(AdmissionError::Full);
        }
        let epoch = state.signal.epoch;
        state.bytes += bytes;
        let item = Item {
            frame: frame.take().expect("admission owns its frame"),
            invocation: invocation.take(),
            epoch,
            fence: None,
            deadline,
            charged_bytes: bytes,
        };
        if urgent {
            let after_fence = state
                .frames
                .iter()
                .rposition(|item| item.fence.is_some())
                .map_or(0, |index| index + 1);
            state.frames.insert(after_fence, item);
        } else {
            state.frames.push_back(item);
        }
        self.available.notify_one();
        Ok(())
    }
    /// Atomic bounded admission prevents a partially published all-destination
    /// fence from waiting forever when one destination has no queue capacity.
    pub fn fence(queues: &[Arc<Self>], frame: Frame) -> Result<(), ProcessorError> {
        Self::try_fence(queues, &frame, None)
            .map_err(|error| Self::fence_error(&frame, error.message()))
    }
    pub async fn fence_wait(
        queues: &[Arc<Self>],
        frame: Frame,
        timeout: Duration,
    ) -> Result<(), ProcessorError> {
        let bytes = frame_bytes(&frame);
        if queues
            .iter()
            .any(|queue| queue.count_limit == 0 || bytes > queue.byte_limit)
        {
            return Err(Self::fence_error(
                &frame,
                "frame exceeds destination capacity",
            ));
        }
        let expires = Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| Self::fence_error(&frame, "admission deadline overflow"))?;
        let epochs = queues
            .iter()
            .map(|queue| (Arc::as_ptr(queue) as usize, queue.signal().epoch))
            .collect::<BTreeMap<_, _>>();
        loop {
            // All destination notifications must be registered before checking
            // any budget; publication remains all-or-none under sorted locks.
            let mut capacity: Vec<_> = queues
                .iter()
                .map(|queue| Box::pin(queue.capacity_available.notified()))
                .collect();
            for notified in &mut capacity {
                notified.as_mut().enable();
            }
            match Self::try_fence(queues, &frame, Some(&epochs)) {
                Ok(()) | Err(AdmissionError::Interrupted) => return Ok(()),
                Err(AdmissionError::Full) => {}
                Err(error) => return Err(Self::fence_error(&frame, error.message())),
            }
            let changed = poll_fn(|cx| {
                for notified in &mut capacity {
                    if notified.as_mut().poll(cx).is_ready() {
                        return Poll::Ready(());
                    }
                }
                Poll::Pending
            });
            if tokio::time::timeout_at(expires, changed).await.is_err() || Instant::now() >= expires
            {
                return Err(Self::fence_error(&frame, "admission timed out"));
            }
        }
    }
    fn fence_error(frame: &Frame, reason: &str) -> ProcessorError {
        ProcessorError::fatal(format!(
            "{}: cannot admit into every bounded destination queue: {reason}",
            crate::fence::description(frame)
        ))
    }
    fn try_fence(
        queues: &[Arc<Self>],
        frame: &Frame,
        epochs: Option<&BTreeMap<usize, u64>>,
    ) -> Result<(), AdmissionError> {
        let bytes = frame_bytes(frame);
        let mut queues: Vec<_> = queues.iter().collect();
        queues.sort_unstable_by_key(|queue| Arc::as_ptr(queue) as usize);
        queues.dedup_by_key(|queue| Arc::as_ptr(queue) as usize);
        let mut states: Vec<_> = queues
            .iter()
            .map(|queue| queue.state.lock().unwrap())
            .collect();
        if states
            .iter()
            .any(|state| state.signal.ending || state.signal.cancelled)
        {
            return Err(AdmissionError::Closed);
        }
        if frame.interruptible
            && epochs.is_some_and(|epochs| {
                states.iter().zip(&queues).any(|(state, queue)| {
                    epochs
                        .get(&(Arc::as_ptr(queue) as usize))
                        .is_some_and(|epoch| *epoch < state.signal.epoch)
                })
            })
        {
            return Err(AdmissionError::Interrupted);
        }
        if states.iter().zip(&queues).any(|(state, queue)| {
            state.frames.len() >= queue.count_limit
                || bytes > queue.byte_limit.saturating_sub(state.bytes)
        }) {
            return Err(AdmissionError::Full);
        }
        for (state, fence) in states
            .iter_mut()
            .zip(FenceParticipant::group(frame.clone(), queues.len()))
        {
            let epoch = state.signal.epoch;
            state.bytes += bytes;
            state.frames.push_back(Item {
                frame: frame.clone(),
                invocation: None,
                epoch,
                fence: Some(fence),
                deadline: None,
                charged_bytes: bytes,
            });
        }
        drop(states);
        for queue in queues {
            queue.available.notify_one();
        }
        Ok(())
    }
    pub fn pop(&self) -> Option<Item> {
        let mut state = self.state.lock().unwrap();
        let item = state.frames.pop_front()?;
        state.bytes -= item.charged_bytes;
        self.capacity_available.notify_waiters();
        Some(item)
    }
    /// Deferred metadata remains in the same bounded admission queue. PCM may
    /// pass future metadata; every other frame, including a fence, is a barrier.
    pub fn pop_ready(&self, now: Instant) -> Option<Item> {
        self.pop_ready_inner(now, false)
    }
    /// Register the current fence in the same locked pop that removes it.
    /// Each opted-in queue has exactly one serial owned worker.
    pub fn pop_ready_independent(&self, now: Instant) -> Option<Item> {
        self.pop_ready_inner(now, true)
    }
    fn pop_ready_inner(&self, now: Instant, track_fence: bool) -> Option<Item> {
        let mut state = self.state.lock().unwrap();
        let first = state.frames.front()?;
        let index = if first.deadline.is_none_or(|deadline| deadline <= now) {
            0
        } else {
            if !timed_metadata(&first.frame) {
                return None;
            }
            let mut selected = None;
            for (i, item) in state.frames.iter().enumerate().skip(1) {
                if timed_metadata(&item.frame) {
                    continue;
                }
                if matches!(
                    item.frame.data,
                    FrameData::OutputAudio(_)
                        | FrameData::TtsAudio(_)
                        | FrameData::SpeechOutputAudio(_)
                ) && item.deadline.is_none_or(|deadline| deadline <= now)
                {
                    selected = Some(i);
                }
                // Only the first non-metadata item can bypass the clock lane.
                break;
            }
            selected?
        };
        let item = state.frames.remove(index)?;
        if track_fence {
            state.current_fence = item
                .fence
                .as_ref()
                .map(|fence| (item.epoch, item.frame.interruptible, fence.cancellation()));
        }
        state.bytes -= item.charged_bytes;
        self.capacity_available.notify_waiters();
        Some(item)
    }
    pub fn next_deadline(&self) -> Option<Instant> {
        self.state
            .lock()
            .unwrap()
            .frames
            .front()
            .and_then(|item| item.deadline)
    }
    pub fn is_empty(&self) -> bool {
        self.state.lock().unwrap().frames.is_empty()
    }
    pub fn signal(&self) -> Signal {
        self.state.lock().unwrap().signal
    }
    /// Serialize an opted-in image's final downstream admission with its
    /// queue's interruption/Cancel cutoff. The completion must be synchronous
    /// and nonblocking; try_push clocks/wakers must obey their non-reentry rule.
    pub fn forward_if_current_image(
        &self,
        epoch: u64,
        interruptible: bool,
        forward: impl FnOnce() -> Result<(), ProcessorError>,
    ) -> Result<(), ProcessorError> {
        let state = self.state.lock().unwrap();
        if state.signal.cancelled || (interruptible && state.signal.epoch > epoch) {
            return Ok(());
        }
        forward()
    }
    pub fn interrupt(&self) {
        let mut state = self.state.lock().unwrap();
        state.signal.epoch += 1;
        if let Some((epoch, interruptible, fence)) = &state.current_fence {
            if *interruptible && *epoch < state.signal.epoch {
                fence.revoke();
            }
        }
        state.frames.retain(|item| {
            if item.frame.interruptible {
                if let Some(fence) = &item.fence {
                    fence.revoke();
                }
                false
            } else {
                true
            }
        });
        state.bytes = state.frames.iter().map(|item| item.charged_bytes).sum();
        self.changed.send_replace(state.signal);
        self.available.notify_one();
        self.capacity_available.notify_waiters();
    }
    pub fn pause(&self, paused: bool) {
        let mut state = self.state.lock().unwrap();
        state.signal.paused = paused;
        self.changed.send_replace(state.signal);
        self.capacity_available.notify_waiters();
    }
    pub fn end(&self) {
        let mut state = self.state.lock().unwrap();
        state.signal.ending = true;
        state.signal.paused = false;
        self.changed.send_replace(state.signal);
        self.available.notify_one();
        self.capacity_available.notify_waiters();
    }
    pub fn cancel(&self) {
        let mut state = self.state.lock().unwrap();
        state.signal.cancelled = true;
        if let Some((_, _, fence)) = state.current_fence.take() {
            fence.abort();
        }
        state.frames.clear();
        state.bytes = 0;
        self.changed.send_replace(state.signal);
        self.available.notify_one();
        self.capacity_available.notify_waiters();
    }
}
fn timed_metadata(frame: &Frame) -> bool {
    frame.has_pipeline_pts()
        && matches!(
            frame.data,
            FrameData::AggregatedText(_)
                | FrameData::TtsText(_)
                | FrameData::AggregatedTextProgress(_)
                | FrameData::TtsAlignmentDiagnostic(_)
        )
}
#[cfg(test)]
mod admission_wait_tests {
    use super::*;
    use pipecat_core::{AggregatedTextFrame, AudioRawFrame, PipelineFlushFrame, TextFrame};

    async fn pending<T>(future: std::pin::Pin<&mut impl Future<Output = T>>) -> bool {
        let mut future = future;
        poll_fn(|cx| Poll::Ready(future.as_mut().poll(cx).is_pending())).await
    }
    fn marker() -> Frame {
        Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            ..Default::default()
        }))
    }
    #[tokio::test]
    async fn count_and_byte_capacity_release_the_owned_waiter() {
        for (count, bytes) in [(1, 64), (4, 6)] {
            let queue = MediaQueue::new(count, bytes);
            queue.push(Frame::text("before")).unwrap();
            let frame = Frame::text("after");
            let id = frame.id;
            let wait = queue.push_wait(frame, None, Duration::from_secs(5));
            tokio::pin!(wait);
            assert!(pending(wait.as_mut()).await);
            assert_eq!(queue.state.lock().unwrap().frames.len(), 1);
            queue.pop().unwrap();
            wait.await.unwrap();
            assert_eq!(queue.pop().unwrap().frame.id, id);
            assert_eq!(queue.state.lock().unwrap().bytes, 0);
        }
    }
    #[tokio::test(start_paused = true)]
    async fn oversized_frame_fails_without_waiting_or_changing_the_budget() {
        let queue = MediaQueue::new(1, 3);
        let before = Instant::now();
        let error = queue
            .push_wait(Frame::text("four"), None, Duration::from_secs(5))
            .await
            .unwrap_err();
        assert!(error.message.contains("exceeds destination capacity"));
        assert_eq!(Instant::now(), before);
        assert!(queue.is_empty());
    }
    #[tokio::test(start_paused = true)]
    async fn timeout_is_absolute_across_pause_notifications() {
        let queue = MediaQueue::new(1, 64);
        queue.push(Frame::text("full")).unwrap();
        let wait = queue.push_wait(Frame::text("after"), None, Duration::from_millis(50));
        tokio::pin!(wait);
        assert!(pending(wait.as_mut()).await);
        tokio::time::advance(Duration::from_millis(30)).await;
        queue.pause(true);
        assert!(pending(wait.as_mut()).await);
        tokio::time::advance(Duration::from_millis(20)).await;
        assert!(wait
            .await
            .unwrap_err()
            .message
            .contains("admission timed out"));
        assert_eq!(queue.state.lock().unwrap().frames.len(), 1);
    }
    #[tokio::test]
    async fn end_and_cancel_release_blocked_admission() {
        for cancel in [false, true] {
            let queue = MediaQueue::new(1, 64);
            queue.push(Frame::text("full")).unwrap();
            let wait = queue.push_wait(Frame::text("after"), None, Duration::from_secs(5));
            tokio::pin!(wait);
            assert!(pending(wait.as_mut()).await);
            if cancel {
                queue.cancel();
            } else {
                queue.end();
            }
            assert!(wait.await.unwrap_err().message.contains("closed"));
        }
    }
    #[tokio::test]
    async fn interruption_drops_waiting_interruptible_frame_but_preserves_uninterruptible() {
        for interruptible in [false, true] {
            let queue = MediaQueue::new(1, 64);
            queue
                .push(Frame::text("full").with_interruptible(false))
                .unwrap();
            let wait = queue.push_wait(
                Frame::text("after").with_interruptible(interruptible),
                None,
                Duration::from_secs(5),
            );
            tokio::pin!(wait);
            assert!(pending(wait.as_mut()).await);
            queue.interrupt();
            if !interruptible {
                assert!(pending(wait.as_mut()).await);
            }
            queue.pop().unwrap();
            wait.await.unwrap();
            assert_eq!(queue.is_empty(), interruptible);
            if !interruptible {
                assert_eq!(queue.pop().unwrap().epoch, 1);
            }
        }
    }
    #[tokio::test]
    async fn cancelling_a_waiter_does_not_publish_it_later() {
        let queue = MediaQueue::new(1, 64);
        queue.push(Frame::text("full")).unwrap();
        let mut wait =
            Box::pin(queue.push_wait(Frame::text("after"), None, Duration::from_secs(5)));
        assert!(pending(wait.as_mut()).await);
        drop(wait);
        queue.pop().unwrap();
        assert!(queue.is_empty());
    }
    #[tokio::test]
    async fn timed_pcm_bypass_releases_capacity_without_releasing_metadata() {
        let queue = MediaQueue::new(2, 128);
        let mut metadata = Frame::new(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("future"),
            aggregated_by: "word".into(),
            context_id: None,
            raw_text: None,
            will_be_spoken: true,
        }));
        metadata.set_pipeline_pts(1_000_000_000);
        queue
            .push_timed(metadata, Instant::now() + Duration::from_secs(1))
            .unwrap();
        let pcm = Frame::new(FrameData::TtsAudio(
            AudioRawFrame::new(vec![0; 4], 8000, 1).unwrap(),
        ));
        let id = pcm.id;
        queue.push(pcm).unwrap();
        let wait = queue.push_wait(Frame::text("after"), None, Duration::from_secs(5));
        tokio::pin!(wait);
        assert!(pending(wait.as_mut()).await);
        assert_eq!(queue.pop_ready(Instant::now()).unwrap().frame.id, id);
        wait.await.unwrap();
        assert_eq!(queue.state.lock().unwrap().frames.len(), 2);
        assert!(queue.pop_ready(Instant::now()).is_none());
    }
    #[tokio::test]
    async fn fence_wait_is_atomic_until_every_destination_has_capacity() {
        let first = MediaQueue::new(1, 64);
        let second = MediaQueue::new(1, 64);
        second.push(Frame::text("full")).unwrap();
        let queues = [first.clone(), second.clone()];
        let wait = MediaQueue::fence_wait(&queues, marker(), Duration::from_secs(5));
        tokio::pin!(wait);
        assert!(pending(wait.as_mut()).await);
        assert!(first.is_empty());
        second.pop().unwrap();
        wait.await.unwrap();
        assert!(first.pop().unwrap().fence.is_some());
        assert!(second.pop().unwrap().fence.is_some());
    }
    #[tokio::test(start_paused = true)]
    async fn closed_or_timed_out_fence_never_publishes_a_partial_participant() {
        for closed in [false, true] {
            let first = MediaQueue::new(1, 64);
            let second = MediaQueue::new(1, 64);
            second.push(Frame::text("full")).unwrap();
            second.pause(true);
            let queues = [first.clone(), second.clone()];
            let wait = MediaQueue::fence_wait(&queues, marker(), Duration::from_millis(50));
            tokio::pin!(wait);
            assert!(pending(wait.as_mut()).await);
            if closed {
                second.cancel();
            } else {
                tokio::time::advance(Duration::from_millis(50)).await;
            }
            assert!(wait.await.unwrap_err().fatal);
            assert!(first.is_empty());
            assert_eq!(second.is_empty(), closed);
        }
    }
}
// A captured invocation has at most 64 weak origins. Reserve conservatively
// rather than treating its bounded heap allocation as free queue retention.
fn scoped_frame_bytes(frame: &Frame, scoped: bool) -> usize {
    frame_bytes(frame).saturating_add(if scoped { 64 * 128 } else { 0 })
}
pub(crate) fn frame_bytes(frame: &Frame) -> usize {
    if let FrameData::Sprite(sprite) = &frame.data {
        let entry = 12
            * (std::mem::size_of::<(String, serde_json::Value)>() + std::mem::size_of::<usize>());
        let bytes = sprite
            .images
            .iter()
            .fold(
                sprite
                    .images
                    .capacity()
                    .saturating_mul(std::mem::size_of::<pipecat_core::OutputImageRawFrame>()),
                |bytes, image| {
                    bytes
                        .saturating_add(image.image.image.capacity())
                        .saturating_add(image.image.format.as_ref().map_or(0, String::capacity))
                },
            )
            .saturating_add(frame.name.capacity())
            .saturating_add(frame.transport_source.as_ref().map_or(0, String::capacity))
            .saturating_add(
                frame
                    .transport_destination
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(frame.native_llm_continuation_retained_bytes())
            .saturating_add(frame.metadata.len().saturating_mul(entry));
        return frame.metadata.iter().fold(bytes, |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(continuation_argument_bytes(value, 0))
        });
    }
    let input_audio = match &frame.data {
        FrameData::InputAudio(audio) => Some((audio, 0)),
        FrameData::UserAudio(audio) => Some((&audio.audio, audio.user_id.capacity())),
        _ => None,
    };
    if let Some((audio, user_bytes)) = input_audio {
        let entry = 12
            * (std::mem::size_of::<(String, serde_json::Value)>() + std::mem::size_of::<usize>());
        // Fixed frame headers and normal generated names are count-bounded.
        let bytes = audio
            .audio
            .capacity()
            .saturating_add(audio.context_id.as_ref().map_or(0, String::capacity))
            .saturating_add(user_bytes)
            .saturating_add(frame.name.capacity().saturating_sub(64))
            .saturating_add(frame.transport_source.as_ref().map_or(0, String::capacity))
            .saturating_add(
                frame
                    .transport_destination
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(frame.native_llm_continuation_retained_bytes())
            .saturating_add(frame.metadata.len().saturating_mul(entry));
        return frame.metadata.iter().fold(bytes, |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(continuation_argument_bytes(value, 0))
        });
    }
    let output_image = match &frame.data {
        FrameData::OutputImage(image) => Some((image, 0)),
        FrameData::OutputImageDetailed(image) => Some((&image.image, 0)),
        FrameData::UrlImage(image) => Some((
            &image.image.image,
            image.url.as_ref().map_or(0, String::capacity),
        )),
        FrameData::AssistantImage(image) => Some((
            &image.image.image,
            image
                .original_data
                .as_ref()
                .map_or(0, Vec::capacity)
                .saturating_add(
                    image
                        .original_mime_type
                        .as_ref()
                        .map_or(0, String::capacity),
                ),
        )),
        _ => None,
    };
    if let Some((image, extra_bytes)) = output_image {
        // Count retained allocations, including spare capacity and nested metadata.
        // The bounded-depth walker allocates no temporary metadata work queue.
        let entry = 12
            * (std::mem::size_of::<(String, serde_json::Value)>() + std::mem::size_of::<usize>());
        let bytes = image
            .image
            .capacity()
            .saturating_add(image.format.as_ref().map_or(0, String::capacity))
            .saturating_add(extra_bytes)
            .saturating_add(frame.name.capacity())
            .saturating_add(frame.transport_source.as_ref().map_or(0, String::capacity))
            .saturating_add(
                frame
                    .transport_destination
                    .as_ref()
                    .map_or(0, String::capacity),
            )
            .saturating_add(frame.native_llm_continuation_retained_bytes())
            .saturating_add(frame.metadata.len().saturating_mul(entry));
        return frame.metadata.iter().fold(bytes, |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(continuation_argument_bytes(value, 0))
        });
    }

    let image_envelope = matches!(
        frame.data,
        FrameData::InputImage(_)
            | FrameData::UserImage(_)
            | FrameData::OutputImage(_)
            | FrameData::OutputImageDetailed(_)
            | FrameData::UrlImage(_)
    );
    let envelope = if matches!(
        frame.data,
        FrameData::TtsText(_)
            | FrameData::AggregatedText(_)
            | FrameData::AggregatedTextProgress(_)
            | FrameData::TtsAlignmentDiagnostic(_)
            | FrameData::InputImage(_)
            | FrameData::UserImage(_)
            | FrameData::OutputImage(_)
            | FrameData::OutputImageDetailed(_)
            | FrameData::UrlImage(_)
    ) {
        let mut bytes = frame
            .name
            .len()
            .saturating_add(frame.transport_source.as_ref().map_or(0, String::len))
            .saturating_add(frame.transport_destination.as_ref().map_or(0, String::len));
        let mut pending = Vec::new();
        for (key, value) in &frame.metadata {
            bytes = bytes.saturating_add(key.len());
            pending.push(value);
        }
        while let Some(value) = pending.pop() {
            // Empty/null image metadata still occupies retained queue memory.
            bytes = bytes.saturating_add(usize::from(image_envelope));
            match value {
                serde_json::Value::String(text) => bytes = bytes.saturating_add(text.len()),
                serde_json::Value::Number(_) => bytes = bytes.saturating_add(16),
                serde_json::Value::Bool(_) => bytes = bytes.saturating_add(1),
                serde_json::Value::Array(values) => pending.extend(values),
                serde_json::Value::Object(values) => {
                    for (key, value) in values {
                        bytes = bytes.saturating_add(key.len());
                        pending.push(value);
                    }
                }
                serde_json::Value::Null => {}
            }
        }
        bytes
    } else {
        0
    };
    let payload: usize = match &frame.data {
        FrameData::InputAudio(a)
        | FrameData::OutputAudio(a)
        | FrameData::TtsAudio(a)
        | FrameData::SpeechOutputAudio(a) => a.audio.len(),
        FrameData::InputImage(i) => i
            .image
            .len()
            .saturating_add(i.format.as_ref().map_or(0, String::len)),
        FrameData::OutputImage(i) => i
            .image
            .len()
            .saturating_add(i.format.as_ref().map_or(0, String::len)),
        FrameData::OutputImageDetailed(i) => i
            .image
            .image
            .len()
            .saturating_add(i.image.format.as_ref().map_or(0, String::len)),
        FrameData::UrlImage(i) => i
            .image
            .image
            .image
            .len()
            .saturating_add(i.image.image.format.as_ref().map_or(0, String::len))
            .saturating_add(i.url.as_ref().map_or(0, String::len)),
        FrameData::UserImage(i) => i
            .image
            .image
            .len()
            .saturating_add(i.user_id.len())
            .saturating_add(i.image.format.as_ref().map_or(0, String::len))
            .saturating_add(i.text.as_ref().map_or(0, String::len))
            .saturating_add(
                i.request
                    .as_ref()
                    .map_or(0, |request| crate::input_video::request_bytes(request)),
            ),
        FrameData::Flush(_) | FrameData::FlushDetailed(_) => flush_retained_bytes(frame),
        FrameData::FunctionCallsStarted(calls)
            if frame.native_llm_continuation_retained_bytes() != 0 =>
        {
            // The native snapshot and the public tool payload own separate
            // allocations, even when their values compare equal.
            if frame.native_llm_continuation().is_err() {
                usize::MAX
            } else {
                calls.iter().fold(
                    calls
                        .capacity()
                        .saturating_mul(std::mem::size_of::<pipecat_core::FunctionCall>()),
                    |bytes, call| {
                        bytes
                            .saturating_add(call.function_name.capacity())
                            .saturating_add(call.tool_call_id.capacity())
                            .saturating_add(continuation_argument_bytes(&call.arguments, 0))
                    },
                )
            }
        }
        FrameData::Text(t) => t.text.len(),
        FrameData::TtsText(t) | FrameData::AggregatedText(t) => t
            .text
            .text
            .len()
            .saturating_add(t.raw_text.as_ref().map_or(0, String::len))
            .saturating_add(t.aggregated_by.len())
            .saturating_add(t.context_id.as_ref().map_or(0, String::len)),
        FrameData::AggregatedTextProgress(t) => t
            .text
            .len()
            .saturating_add(t.accumulated_text.len())
            .saturating_add(t.remaining_text.len())
            .saturating_add(t.aggregated_by.len())
            .saturating_add(t.context_id.as_ref().map_or(0, String::len)),
        FrameData::TtsAlignmentDiagnostic(t) => t
            .synthesis_text
            .len()
            .saturating_add(t.original_text.as_ref().map_or(0, String::len))
            .saturating_add(t.context_id.len()),
        _ => 0,
    };
    payload
        .saturating_add(envelope)
        .saturating_add(frame.native_llm_continuation_retained_bytes())
}

/// Empty lifecycle controls reserve their fixed header through the count limit.
/// Dynamic Flush annotations must not bypass the retained-byte budget.
fn flush_retained_bytes(frame: &Frame) -> usize {
    use std::mem::size_of;
    // A generated class name plus a u64 suffix fits this fixed allowance.
    let mut bytes = frame
        .name
        .capacity()
        .saturating_sub(64)
        .saturating_add(frame.transport_source.as_ref().map_or(0, String::capacity))
        .saturating_add(
            frame
                .transport_destination
                .as_ref()
                .map_or(0, String::capacity),
        );
    let origin = match &frame.data {
        FrameData::Flush(flush) => flush.origin.as_ref(),
        FrameData::FlushDetailed(flush) => flush.origin.as_ref(),
        _ => None,
    };
    if let Some(origin) = origin {
        bytes = bytes.saturating_add(if origin == "pipecat.flows" {
            origin.capacity().saturating_sub(16)
        } else {
            origin.capacity()
        });
    }
    let entry = 12 * (size_of::<(String, serde_json::Value)>() + size_of::<usize>());
    frame.metadata.iter().fold(
        bytes.saturating_add(frame.metadata.len().saturating_mul(entry)),
        |bytes, (key, value)| {
            bytes
                .saturating_add(key.capacity())
                .saturating_add(continuation_argument_bytes(value, 0))
        },
    )
}

// Conservative JSON allocation accounting with a bounded traversal depth.
fn continuation_argument_bytes(value: &serde_json::Value, depth: usize) -> usize {
    use serde_json::Value;
    use std::mem::size_of;
    if depth > pipecat_core::NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
        return usize::MAX;
    }
    match value {
        Value::String(text) => text.capacity(),
        Value::Array(values) => values.iter().fold(
            values.capacity().saturating_mul(size_of::<Value>()),
            |bytes, value| bytes.saturating_add(continuation_argument_bytes(value, depth + 1)),
        ),
        Value::Object(values) => {
            // Match the carrier's conservative full B-tree node reservation.
            let entry = 12 * (size_of::<(String, Value)>() + size_of::<usize>());
            values
                .iter()
                .fold(values.len().saturating_mul(entry), |bytes, (key, value)| {
                    bytes
                        .saturating_add(key.capacity())
                        .saturating_add(continuation_argument_bytes(value, depth + 1))
                })
        }
        Value::Null | Value::Bool(_) | Value::Number(_) => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::PipelineFlushFrame;

    fn marker() -> Frame {
        Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            ..Default::default()
        }))
    }
    #[test]
    fn all_destination_fence_admission_is_atomic_on_overload_or_close() {
        for closed in [false, true] {
            let first = MediaQueue::new(1, 64);
            let second = MediaQueue::new(1, 64);
            if closed {
                second.cancel();
            } else {
                second.push(Frame::text("full")).unwrap();
            }
            let error = MediaQueue::fence(&[first.clone(), second.clone()], marker()).unwrap_err();
            assert!(error.fatal);
            assert!(
                first.pop().is_none(),
                "a rejected fence was partially admitted"
            );
            if !closed {
                assert!(matches!(
                    second.pop().unwrap().frame.data,
                    FrameData::Text(_)
                ));
            }
        }
    }
    #[test]
    fn urgent_later_media_never_jumps_an_accepted_fence() {
        let queue = MediaQueue::new(4, 64);
        queue.push(Frame::text("before")).unwrap();
        MediaQueue::fence(std::slice::from_ref(&queue), marker()).unwrap();
        queue.push(Frame::text("after")).unwrap();
        queue.push_front(Frame::text("urgent-after")).unwrap();
        assert!(
            matches!(&queue.pop().unwrap().frame.data, FrameData::Text(text) if text.text == "before")
        );
        assert!(queue.pop().unwrap().fence.is_some());
        assert!(
            matches!(&queue.pop().unwrap().frame.data, FrameData::Text(text) if text.text == "urgent-after")
        );
        assert!(
            matches!(&queue.pop().unwrap().frame.data, FrameData::Text(text) if text.text == "after")
        );
    }
}

#[cfg(test)]
mod timed_lane_tests {
    use super::*;
    use pipecat_core::{AggregatedTextFrame, AudioRawFrame, TextFrame, TtsStoppedFrame};
    use std::time::Duration;
    fn metadata() -> Frame {
        let mut frame = Frame::new(FrameData::TtsText(AggregatedTextFrame {
            text: TextFrame::new("future"),
            aggregated_by: "word".into(),
            context_id: Some("ctx".into()),
            raw_text: Some("future".into()),
            will_be_spoken: true,
        }));
        frame.set_pipeline_pts(1_000_000_000);
        frame
    }
    fn audio() -> Frame {
        Frame::new(FrameData::TtsAudio(
            AudioRawFrame::new(vec![0; 4], 16000, 1).unwrap(),
        ))
    }
    #[test]
    fn pcm_bypasses_future_metadata_without_releasing_its_count_or_byte_reservation() {
        let queue = MediaQueue::new(2, 64);
        let now = Instant::now();
        queue
            .push_timed(metadata(), now + Duration::from_secs(1))
            .unwrap();
        queue.push(audio()).unwrap();
        assert!(queue.push(Frame::text("overflow")).is_err());
        assert!(matches!(
            queue.pop_ready(now).unwrap().frame.data,
            FrameData::TtsAudio(_)
        ));
        assert!(queue.pop_ready(now).is_none());
        assert!(queue.push(Frame::text("x".repeat(64))).is_err());
        queue.push(audio()).unwrap();
        assert!(matches!(
            queue.pop_ready(now).unwrap().frame.data,
            FrameData::TtsAudio(_)
        ));
        assert!(matches!(
            queue
                .pop_ready(now + Duration::from_secs(1))
                .unwrap()
                .frame
                .data,
            FrameData::TtsText(_)
        ));
        assert!(queue.is_empty());
    }
    #[test]
    fn stop_and_fence_remain_barriers_and_interruption_clears_timed_words() {
        let now = Instant::now();
        let queue = MediaQueue::new(4, 64);
        queue
            .push_timed(metadata(), now + Duration::from_secs(1))
            .unwrap();
        queue
            .push(Frame::new(FrameData::TtsStopped(TtsStoppedFrame {
                context_id: Some("ctx".into()),
            })))
            .unwrap();
        queue.push(audio()).unwrap();
        assert!(queue.pop_ready(now).is_none());
        queue.interrupt();
        assert!(queue.is_empty());
        queue
            .push_timed(metadata(), now + Duration::from_secs(1))
            .unwrap();
        let fence = Frame::new(FrameData::FlushDetailed(pipecat_core::PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            ..Default::default()
        }));
        MediaQueue::fence(std::slice::from_ref(&queue), fence).unwrap();
        queue.push(audio()).unwrap();
        assert!(queue.pop_ready(now).is_none());
        queue.interrupt();
        assert!(queue.pop_ready(now).unwrap().fence.is_some());
    }
    #[test]
    fn legacy_unmarked_metadata_keeps_fifo_semantics() {
        let queue = MediaQueue::new(2, 64);
        let mut frame = metadata();
        frame.set_relative_pts(Some(1));
        queue.push(frame).unwrap();
        queue.push(audio()).unwrap();
        assert!(matches!(
            queue.pop_ready(Instant::now()).unwrap().frame.data,
            FrameData::TtsText(_)
        ));
    }
}

#[cfg(test)]
mod native_history_budget_tests {
    use super::*;
    use pipecat_core::{LlmContinuationBlock, LlmContinuationProvider, NativeLlmContinuation};
    fn frame() -> Frame {
        let mut frame = Frame::new(FrameData::LlmResponseEnd);
        frame
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::new(
                    1,
                    LlmContinuationProvider::Anthropic,
                    vec![LlmContinuationBlock::Thinking {
                        text: "thinking".into(),
                        signature: "s".repeat(1024),
                    }],
                )
                .unwrap(),
            ))
            .unwrap();
        frame
    }
    #[test]
    fn semantic_history_reserves_bytes_for_all_enqueue_paths_and_releases_them() {
        let frame = frame();
        let bytes = frame.native_llm_continuation_retained_bytes();
        assert!(bytes > 1024);
        for urgent in [false, true] {
            let small = MediaQueue::new(4, bytes - 1);
            assert!(small.push(frame.clone()).is_err());
            assert!(small.push_front(frame.clone()).is_err());
            assert!(small.push_timed(frame.clone(), Instant::now()).is_err());
            assert!(small.pop().is_none());
            let exact = MediaQueue::new(4, bytes);
            if urgent {
                exact.push_front(frame.clone()).unwrap();
            } else {
                exact.push(frame.clone()).unwrap();
            }
            assert!(exact.push(frame.clone()).is_err());
            exact.pop().unwrap();
            exact.push(frame.clone()).unwrap();
            exact.interrupt();
            exact.push(frame.clone()).unwrap();
        }
    }
    #[test]
    fn semantic_history_fence_admission_is_atomic_and_budgeted() {
        let frame = frame();
        let bytes = frame.native_llm_continuation_retained_bytes();
        let exact = MediaQueue::new(4, bytes);
        let small = MediaQueue::new(4, bytes - 1);
        assert!(MediaQueue::fence(&[exact.clone(), small.clone()], frame.clone()).is_err());
        assert!(exact.pop().is_none());
        assert!(small.pop().is_none());
        let mut mutated = frame.clone();
        mutated.data = Frame::end().data;
        assert!(MediaQueue::fence(std::slice::from_ref(&small), mutated).is_err());
        MediaQueue::fence(std::slice::from_ref(&small), Frame::end()).unwrap();
        assert!(small.pop().unwrap().fence.is_some());
        assert!(small.pop().is_none());
        MediaQueue::fence(std::slice::from_ref(&exact), frame.clone()).unwrap();
        assert!(exact.push(frame.clone()).is_err());
        assert!(exact.pop().unwrap().fence.is_some());
        exact.push(frame).unwrap();
    }
    #[test]
    fn mirrored_calls_charge_their_actual_spare_capacity_without_changing_unsigned_policy() {
        use pipecat_core::FunctionCall;
        fn tool_frame(spare: bool, attached: bool) -> Frame {
            let call = FunctionCall {
                function_name: "lookup".into(),
                tool_call_id: "call".into(),
                arguments: serde_json::json!({"q": ["value"]}),
            };
            let continuation = Arc::new(
                NativeLlmContinuation::new(
                    1,
                    LlmContinuationProvider::Anthropic,
                    vec![LlmContinuationBlock::ToolCall(call.clone())],
                )
                .unwrap(),
            );
            let mut calls = vec![call];
            if spare {
                calls.reserve(1024);
                calls[0].function_name.reserve(8192);
                calls[0].tool_call_id.reserve(8192);
                let values = calls[0].arguments["q"].as_array_mut().unwrap();
                values.reserve(8192);
                if let serde_json::Value::String(value) = &mut values[0] {
                    value.reserve(8192);
                }
            }
            let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls));
            if attached {
                frame.set_native_llm_continuation(continuation).unwrap();
            }
            frame
        }
        let compact = tool_frame(false, true);
        let reserved = tool_frame(true, true);
        assert_eq!(compact.data, reserved.data);
        assert_eq!(
            compact.native_llm_continuation_retained_bytes(),
            reserved.native_llm_continuation_retained_bytes()
        );
        assert!(frame_bytes(&compact) > compact.native_llm_continuation_retained_bytes());
        assert!(frame_bytes(&reserved) > frame_bytes(&compact) + 32_768);
        assert!(MediaQueue::new(2, frame_bytes(&compact))
            .push(reserved)
            .is_err());
        let reserved = tool_frame(true, true);
        let exact = MediaQueue::new(2, frame_bytes(&reserved));
        exact.push(reserved).unwrap();
        exact.pop().unwrap();
        assert_eq!(exact.state.lock().unwrap().bytes, 0);
        let reserved = tool_frame(true, true);
        let exact = MediaQueue::new(2, frame_bytes(&reserved));
        MediaQueue::fence(std::slice::from_ref(&exact), reserved).unwrap();
        assert!(exact.pop().unwrap().fence.is_some());
        assert_eq!(exact.state.lock().unwrap().bytes, 0);
        let reserved = tool_frame(true, true);
        MediaQueue::fence(std::slice::from_ref(&exact), reserved).unwrap();
        exact.interrupt();
        assert_eq!(exact.state.lock().unwrap().bytes, 0);
        let unsigned = tool_frame(true, false);
        assert_eq!(frame_bytes(&unsigned), 0);
        MediaQueue::new(1, 1).push(unsigned).unwrap();
    }
    #[test]
    fn empty_lifecycle_fences_still_admit_and_drain_at_full_media_byte_capacity() {
        use pipecat_core::{AggregatedTextFrame, AudioRawFrame, PipelineFlushFrame, TextFrame};
        for boundary in [
            Frame::end(),
            Frame::new(FrameData::Stop),
            Frame::new(FrameData::FlushDetailed(PipelineFlushFrame {
                origin: Some("pipecat.flows".into()),
                ..Default::default()
            })),
        ] {
            assert_eq!(frame_bytes(&boundary), 0);
            for timed in [false, true] {
                let media = if timed {
                    Frame::new(FrameData::TtsText(AggregatedTextFrame {
                        text: TextFrame::new("waiting"),
                        aggregated_by: "word".into(),
                        context_id: Some("ctx".into()),
                        raw_text: None,
                        will_be_spoken: true,
                    }))
                } else {
                    Frame::new(FrameData::OutputAudio(
                        AudioRawFrame::new(vec![0; 32], 16000, 1).unwrap(),
                    ))
                };
                let queue = MediaQueue::new(2, frame_bytes(&media));
                let now = Instant::now();
                let deadline = now + std::time::Duration::from_secs(1);
                if timed {
                    queue.push_timed(media, deadline).unwrap();
                } else {
                    queue.push(media).unwrap();
                }
                MediaQueue::fence(std::slice::from_ref(&queue), boundary.clone()).unwrap();
                if timed {
                    assert!(queue.pop_ready(now).is_none());
                }
                assert!(queue.pop_ready(deadline).unwrap().fence.is_none());
                assert!(queue.pop_ready(deadline).unwrap().fence.is_some());
                assert!(queue.pop().is_none());
                assert_eq!(queue.state.lock().unwrap().bytes, 0);
                queue.cancel();
                assert!(queue.pop().is_none());
            }
        }
    }
}

#[cfg(test)]
#[path = "queue/recovered_admission_tests.rs"]
mod recovered_admission_tests;

#[cfg(test)]
mod image_queue_tests {
    use super::*;
    use pipecat_core::{ImageRawFrame, OutputImageRawFrame, UrlImageRawFrame};
    fn image() -> Frame {
        Frame::new(FrameData::UrlImage(UrlImageRawFrame {
            image: OutputImageRawFrame {
                image: ImageRawFrame {
                    image: vec![1, 2, 3],
                    size: (1, 1),
                    format: Some("RGB".into()),
                },
                sync_with_audio: false,
            },
            url: Some("https://images.example/test".into()),
        }))
    }
    #[test]
    fn url_image_charges_pixels_url_envelope_and_releases_capacity() {
        let mut frame = image();
        frame.metadata.insert(
            "nested".into(),
            serde_json::json!({"lots": [null,null,null]}),
        );
        let bytes = frame_bytes(&frame);
        assert!(bytes > 3 + "https://images.example/test".len());
        assert!(MediaQueue::new(1, frame_bytes(&frame.clone()) - 1)
            .push(frame.clone())
            .is_err());
        let queue = MediaQueue::new(1, bytes);
        queue.push(frame.clone()).unwrap();
        assert!(queue.push(frame.clone()).is_err());
        queue.pop().unwrap();
        queue.push(frame).unwrap();
        queue.interrupt();
        assert!(queue.is_empty());
    }
    #[test]
    fn spare_capacity_and_deep_image_metadata_cannot_bypass_budget() {
        let mut frame = image();
        if let FrameData::UrlImage(image) = &mut frame.data {
            image.image.image.image.reserve(4096);
            image.url.as_mut().unwrap().reserve(4096);
        }
        assert!(frame_bytes(&frame) >= 8192);
        assert!(MediaQueue::new(1, 1024).push(frame).is_err());
        let mut frame = image();
        let mut metadata = serde_json::Value::Null;
        for _ in 0..=pipecat_core::NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
            metadata = serde_json::json!([metadata]);
        }
        frame.metadata.insert("deep".into(), metadata);
        assert_eq!(frame_bytes(&frame), usize::MAX);
    }
}

#[cfg(test)]
mod assistant_image_queue_tests {
    use super::*;
    use pipecat_core::{AssistantImageRawFrame, ImageRawFrame, OutputImageRawFrame};

    fn assistant() -> Frame {
        let mut frame = Frame::new(FrameData::AssistantImage(AssistantImageRawFrame {
            image: OutputImageRawFrame {
                image: ImageRawFrame {
                    image: vec![1, 2, 3],
                    size: (1, 1),
                    format: Some("RGB".into()),
                },
                sync_with_audio: true,
            },
            original_data: None,
            original_mime_type: None,
        }));
        frame.name = "assistant-image".into();
        frame
    }

    #[test]
    fn original_allocations_add_their_exact_retained_capacity() {
        let mut frame = assistant();
        let baseline = frame_bytes(&frame);
        let mut original = Vec::with_capacity(4096);
        original.push(9);
        let original_capacity = original.capacity();
        let mut mime = String::with_capacity(2048);
        mime.push_str("image/png");
        let mime_capacity = mime.capacity();
        if let FrameData::AssistantImage(image) = &mut frame.data {
            image.original_data = Some(original);
            image.original_mime_type = Some(mime);
        }
        assert_eq!(
            frame_bytes(&frame),
            baseline + original_capacity + mime_capacity
        );
    }

    #[test]
    fn assistant_exact_byte_boundary_and_released_reservations() {
        let bytes = frame_bytes(&assistant());
        assert!(bytes > 3);
        assert!(MediaQueue::new(2, bytes - 1).push(assistant()).is_err());
        let queue = MediaQueue::new(2, bytes);
        queue.push(assistant()).unwrap();
        assert_eq!(queue.state.lock().unwrap().bytes, bytes);
        assert!(queue.push(assistant()).is_err());
        queue.pop().unwrap();
        assert_eq!(queue.state.lock().unwrap().bytes, 0);
        queue.push(assistant()).unwrap();
        queue.interrupt();
        assert_eq!(queue.state.lock().unwrap().bytes, 0);
        queue.push(assistant()).unwrap();
        queue.cancel();
        assert_eq!(queue.state.lock().unwrap().bytes, 0);
        assert!(queue.is_empty());
    }

    #[tokio::test]
    async fn every_image_enqueue_path_uses_the_same_retained_budget() {
        fn oversized() -> Frame {
            let mut frame = assistant();
            if let FrameData::AssistantImage(image) = &mut frame.data {
                image.original_data = Some(Vec::with_capacity(4096));
            }
            frame
        }
        let queue = MediaQueue::new(4, 1024);
        assert!(queue.push(oversized()).is_err());
        assert!(queue.push_front(oversized()).is_err());
        assert!(queue.push_timed(oversized(), Instant::now()).is_err());
        let waited = tokio::time::timeout(
            Duration::from_secs(1),
            queue.push_wait(oversized(), None, Duration::from_secs(1)),
        )
        .await
        .unwrap();
        assert!(waited
            .unwrap_err()
            .message
            .contains("frame exceeds destination capacity"));
        assert_eq!(queue.state.lock().unwrap().bytes, 0);
        assert!(queue.is_empty());
        queue.push(assistant()).unwrap();
        queue.pop().unwrap();
        assert_eq!(queue.state.lock().unwrap().bytes, 0);
    }

    #[test]
    fn assistant_nested_metadata_uses_the_existing_depth_cap() {
        let mut frame = assistant();
        let mut value = serde_json::Value::Null;
        for _ in 0..=pipecat_core::NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
            value = serde_json::json!([value]);
        }
        frame.metadata.insert("nested".into(), value);
        assert_eq!(frame_bytes(&frame), usize::MAX);
        assert!(MediaQueue::new(1, 1024).push(frame).is_err());
    }
}

#[cfg(test)]
mod independent_image_cutoff_tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[test]
    fn cancel_and_interrupt_cutoffs_precede_image_forward_but_uninterruptible_survives_epoch() {
        for cancelled in [false, true] {
            for interruptible in [false, true] {
                let queue = MediaQueue::new(1, 4096);
                let epoch = queue.signal().epoch;
                if cancelled {
                    queue.cancel();
                } else {
                    queue.interrupt();
                }
                let forwarded = AtomicUsize::new(0);
                queue
                    .forward_if_current_image(epoch, interruptible, || {
                        assert!(
                            queue.state.try_lock().is_err(),
                            "forward escaped its cutoff lock"
                        );
                        forwarded.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    })
                    .unwrap();
                assert_eq!(
                    forwarded.load(Ordering::SeqCst),
                    usize::from(!cancelled && !interruptible)
                );
            }
        }
    }

    #[test]
    fn admitted_image_forward_wins_concurrent_cancel_without_retraction() {
        let queue = MediaQueue::new(1, 4096);
        let contender = queue.clone();
        let (start_tx, start_rx) = std::sync::mpsc::channel();
        let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
        let (done_tx, done_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            start_rx.recv_timeout(Duration::from_secs(3)).unwrap();
            attempt_tx.send(()).unwrap();
            contender.cancel();
            done_tx.send(()).unwrap();
        });
        let mut forwarded = false;
        queue
            .forward_if_current_image(0, true, || {
                assert!(
                    queue.state.try_lock().is_err(),
                    "forward escaped its cutoff lock"
                );
                start_tx.send(()).unwrap();
                attempt_rx.recv_timeout(Duration::from_secs(3)).unwrap();
                assert!(matches!(
                    done_rx.try_recv(),
                    Err(std::sync::mpsc::TryRecvError::Empty)
                ));
                forwarded = true;
                Ok(())
            })
            .unwrap();
        done_rx.recv_timeout(Duration::from_secs(3)).unwrap();
        thread.join().unwrap();
        assert!(queue.signal().cancelled);
        assert!(forwarded, "later Cancel retracted admitted image");
        let mut stale_forwarded = false;
        queue
            .forward_if_current_image(0, false, || {
                stale_forwarded = true;
                Ok(())
            })
            .unwrap();
        assert!(
            !stale_forwarded,
            "Cancel permitted later uninterruptible image"
        );
    }
}

#[cfg(test)]
mod sprite_budget_tests {
    use super::*;
    use pipecat_core::{ImageRawFrame, OutputImageRawFrame, SpriteFrame};

    fn sprite() -> Frame {
        let mut frame = Frame::new(FrameData::Sprite(SpriteFrame {
            images: vec![OutputImageRawFrame {
                image: ImageRawFrame {
                    image: vec![1, 2, 3],
                    size: (1, 1),
                    format: Some("RGB".into()),
                },
                sync_with_audio: false,
            }],
        }));
        frame.name = "sprite".into();
        frame
    }

    #[test]
    fn sprite_container_pixels_format_and_outer_envelope_are_charged() {
        let mut frame = sprite();
        let original = frame_bytes(&frame);
        assert!(original >= std::mem::size_of::<OutputImageRawFrame>() + 6);
        if let FrameData::Sprite(sprite) = &mut frame.data {
            let before = sprite.images.capacity();
            sprite.images.reserve(20);
            let delta =
                (sprite.images.capacity() - before) * std::mem::size_of::<OutputImageRawFrame>();
            assert_eq!(frame_bytes(&frame), original + delta);
        }
        let baseline = frame_bytes(&frame);
        if let FrameData::Sprite(sprite) = &mut frame.data {
            let image = &mut sprite.images[0].image;
            let before = image.image.capacity();
            image.image.reserve(4096);
            let delta = image.image.capacity() - before;
            assert_eq!(frame_bytes(&frame), baseline + delta);
        }
        let baseline = frame_bytes(&frame);
        frame.transport_destination = Some(String::with_capacity(1024));
        frame.metadata.insert(
            "note".into(),
            serde_json::Value::String(String::with_capacity(2048)),
        );
        assert!(frame_bytes(&frame) >= baseline + 3072);
    }

    #[tokio::test]
    async fn sprite_budget_applies_to_immediate_wait_and_timed_admission() {
        let frame = sprite();
        let bytes = frame_bytes(&frame);
        let queue = MediaQueue::new(4, bytes - 1);
        assert!(queue.push(frame.clone()).is_err());
        assert!(queue.push_timed(frame.clone(), Instant::now()).is_err());
        assert!(queue
            .push_wait(frame, None, Duration::from_millis(1))
            .await
            .is_err());
        assert!(queue.is_empty());
        let queue = MediaQueue::new(1, bytes);
        queue.push(sprite()).unwrap();
        assert!(queue.push(sprite()).is_err());
        queue.pop().unwrap();
        queue.push(sprite()).unwrap();
    }

    #[test]
    fn empty_sprite_spare_container_capacity_is_not_free() {
        let frame = Frame::new(FrameData::Sprite(SpriteFrame {
            images: Vec::with_capacity(128),
        }));
        assert!(frame_bytes(&frame) >= 128 * std::mem::size_of::<OutputImageRawFrame>());
    }
}
