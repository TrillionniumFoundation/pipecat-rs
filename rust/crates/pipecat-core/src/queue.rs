//! Bounded frame admission with a reserved system lane and stable priorities.
use crate::{DirectedFrame, FrameData, FramePriority};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex,
};
use tokio::sync::Notify;

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum QueueError {
    #[error("frame queue is full")]
    Full,
    #[error("frame queue is closed")]
    Closed,
}
/// Shared logical epochs make cutoffs comparable across bidirectional nodes.
#[derive(Default)]
pub(crate) struct InterruptionRegistry {
    generation: u64,
    epochs: BTreeMap<u64, u64>,
    recent: VecDeque<u64>,
}
impl InterruptionRegistry {
    fn reserve(&mut self, identity: Option<u64>, token: Option<&AtomicU64>) -> u64 {
        static NEXT_EPOCH: AtomicU64 = AtomicU64::new(1);
        let remembered = identity.and_then(|id| self.epochs.get(&id).copied());
        let existing = token
            .map(|token| token.load(Ordering::SeqCst))
            .filter(|epoch| *epoch != 0);
        let epoch = existing
            .or(remembered)
            .unwrap_or_else(|| NEXT_EPOCH.fetch_add(1, Ordering::SeqCst));
        let epoch = match token {
            Some(token) => {
                match token.compare_exchange(0, epoch, Ordering::SeqCst, Ordering::SeqCst) {
                    Ok(_) => epoch,
                    Err(existing) => existing,
                }
            }
            None => epoch,
        };
        self.generation = self.generation.max(epoch);
        if let Some(id) = identity {
            if self.epochs.insert(id, epoch).is_none() {
                self.recent.push_back(id);
                if self.recent.len() > 512 {
                    if let Some(old) = self.recent.pop_front() {
                        self.epochs.remove(&old);
                    }
                }
            }
        }
        epoch
    }
}
#[derive(Default)]
struct Queues {
    start: VecDeque<DirectedFrame>,
    system: VecDeque<(u64, DirectedFrame)>,
    normal: VecDeque<(u64, DirectedFrame)>,
    generation: u64,
    interruptions: VecDeque<u64>,
    interruption_ids: BTreeSet<u64>,
    normal_paused: bool,
    system_in_progress: bool,
    normal_in_progress: bool,
    closed: bool,
}

/// Separate finite capacities ensure normal-frame backpressure cannot prevent
/// an interruption or cancellation from entering the system lane.
pub struct FrameQueue {
    queues: Mutex<Queues>,
    // Observation-only cutoff. Every locked generation writer publishes before
    // dropping queued payloads, whose destructors may acquire source locks.
    observed_generation: AtomicU64,
    admission: Arc<Mutex<()>>,
    interruptions: Arc<Mutex<InterruptionRegistry>>,
    dropped: Arc<AtomicU64>,
    normal_capacity: usize,
    system_capacity: usize,
    changed: Notify,
}
impl FrameQueue {
    pub fn new(capacity: usize) -> Self {
        Self::with_capacities(capacity, capacity)
    }
    pub fn with_capacities(normal_capacity: usize, system_capacity: usize) -> Self {
        Self::with_admission(
            normal_capacity,
            system_capacity,
            Arc::new(Mutex::new(())),
            Arc::new(Mutex::new(InterruptionRegistry::default())),
            Arc::new(AtomicU64::new(0)),
        )
    }
    pub(crate) fn with_admission(
        normal_capacity: usize,
        system_capacity: usize,
        admission: Arc<Mutex<()>>,
        interruptions: Arc<Mutex<InterruptionRegistry>>,
        dropped: Arc<AtomicU64>,
    ) -> Self {
        assert!(
            normal_capacity > 0 && system_capacity > 0,
            "capacities must be positive"
        );
        Self {
            queues: Mutex::new(Queues::default()),
            observed_generation: AtomicU64::new(0),
            admission,
            interruptions,
            dropped,
            normal_capacity,
            system_capacity,
            changed: Notify::new(),
        }
    }
    pub fn try_send(&self, item: DirectedFrame) -> Result<(), (QueueError, Box<DirectedFrame>)> {
        let _admission = self.admission.lock().unwrap();
        self.try_send_unlocked(item, None)
    }
    pub(crate) fn try_send_unlocked(
        &self,
        item: DirectedFrame,
        origin_generation: Option<u64>,
    ) -> Result<(), (QueueError, Box<DirectedFrame>)> {
        // Reject this item before touching interruption epochs, lane state or
        // queued payloads. Keep the legacy QueueError surface unchanged.
        if item.frame.validate_selected_tool_authority().is_err() {
            return Err((QueueError::Closed, Box::new(item)));
        }
        let mut queues = self.queues.lock().unwrap();
        if queues.closed {
            return Err((QueueError::Closed, Box::new(item)));
        }
        let interruption_id = matches!(item.frame.data, FrameData::Interruption).then(|| {
            item.frame
                .id
                .min(item.frame.broadcast_sibling_id.unwrap_or(item.frame.id))
        });
        if interruption_id.is_some_and(|id| queues.interruption_ids.contains(&id)) {
            return Ok(());
        }
        if interruption_id.is_some()
            && item.frame.interruption_epoch.as_ref().is_some_and(|token| {
                let epoch = token.load(Ordering::SeqCst);
                epoch != 0 && epoch <= queues.generation
            })
        {
            return Ok(());
        }
        let priority = item.frame.priority();
        let full = match priority {
            FramePriority::Start => !queues.start.is_empty(),
            FramePriority::System => queues.system.len() >= self.system_capacity,
            FramePriority::Normal => queues.normal.len() >= self.normal_capacity,
        };
        if full {
            return Err((QueueError::Full, Box::new(item)));
        }
        match priority {
            FramePriority::Start => queues.start.push_back(item),
            FramePriority::System => {
                let generation = if let Some(id) = interruption_id {
                    Self::remember_interruption(&mut queues, id);
                    let epoch = self
                        .interruptions
                        .lock()
                        .unwrap()
                        .reserve(Some(id), item.frame.interruption_epoch.as_deref());
                    if epoch <= queues.generation {
                        return Ok(());
                    }
                    queues.generation = epoch;
                    self.observed_generation
                        .store(queues.generation, Ordering::SeqCst);
                    let removed = Self::reset_before(&mut queues, epoch);
                    self.dropped.fetch_add(removed as u64, Ordering::Relaxed);
                    epoch
                } else {
                    origin_generation
                        .unwrap_or_else(|| self.interruptions.lock().unwrap().generation)
                };
                queues.system.push_back((generation, item));
            }
            FramePriority::Normal => {
                let generation = origin_generation
                    .unwrap_or_else(|| self.interruptions.lock().unwrap().generation);
                queues.normal.push_back((generation, item));
            }
        }
        drop(queues);
        self.changed.notify_waiters();
        Ok(())
    }
    pub async fn send(&self, item: DirectedFrame) -> Result<(), QueueError> {
        self.send_checked(item, || true, || {}).await
    }
    /// Check lifecycle admission and commit its side effects atomically with
    /// each capacity attempt. No lock is retained while awaiting queue space.
    pub(crate) async fn send_checked(
        &self,
        mut item: DirectedFrame,
        allowed: impl Fn() -> bool,
        admitted: impl Fn(),
    ) -> Result<(), QueueError> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let result = {
                let _admission = self.admission.lock().unwrap();
                if !allowed() {
                    return Err(QueueError::Closed);
                }
                let result = self.try_send_unlocked(item, None);
                if result.is_ok() {
                    admitted();
                }
                result
            };
            match result {
                Ok(()) => return Ok(()),
                Err((QueueError::Closed, _)) => return Err(QueueError::Closed),
                Err((QueueError::Full, returned)) => item = *returned,
            }
            notified.await;
        }
    }
    pub(crate) fn admission_changed(&self) -> tokio::sync::futures::Notified<'_> {
        self.changed.notified()
    }
    pub(crate) fn wake_senders(&self) {
        self.changed.notify_waiters();
    }
    pub fn try_recv(&self) -> Option<DirectedFrame> {
        self.pop()
    }
    pub async fn recv(&self) -> Option<DirectedFrame> {
        self.receive().await
    }
    pub(crate) async fn recv_start(&self) -> Option<(u64, DirectedFrame)> {
        self.receive_system(true).await
    }
    pub(crate) async fn recv_system(&self) -> Option<(u64, DirectedFrame)> {
        self.receive_system(false).await
    }
    async fn receive_system(&self, start_only: bool) -> Option<(u64, DirectedFrame)> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (item, closed) = {
                let mut queues = self.queues.lock().unwrap();
                let item = queues
                    .start
                    .pop_front()
                    .map(|item| (queues.generation, item))
                    .or_else(|| {
                        if start_only {
                            None
                        } else {
                            queues.system.pop_front()
                        }
                    });
                if item.is_some() {
                    queues.system_in_progress = true;
                }
                (item, queues.closed)
            };
            if item.is_some() {
                self.changed.notify_waiters();
                return item;
            }
            if closed {
                return None;
            }
            notified.await;
        }
    }
    pub(crate) fn finish_system(&self) {
        self.queues.lock().unwrap().system_in_progress = false;
        self.changed.notify_waiters();
    }
    pub(crate) async fn recv_normal(&self) -> Option<(u64, DirectedFrame)> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let (item, closed) = {
                let mut queues = self.queues.lock().unwrap();
                let item = if !queues.normal_paused
                    && !queues.system_in_progress
                    && queues.start.is_empty()
                    && queues.system.is_empty()
                {
                    queues.normal.pop_front()
                } else {
                    None
                };
                if item.is_some() {
                    queues.normal_in_progress = true;
                }
                (item, queues.closed)
            };
            if item.is_some() {
                self.changed.notify_waiters();
                return item;
            }
            if closed {
                return None;
            }
            notified.await;
        }
    }
    pub(crate) fn finish_normal(&self) {
        self.queues.lock().unwrap().normal_in_progress = false;
        self.changed.notify_waiters();
    }
    pub(crate) fn is_idle(&self) -> bool {
        let queues = self.queues.lock().unwrap();
        queues.start.is_empty()
            && queues.system.is_empty()
            && queues.normal.is_empty()
            && !queues.system_in_progress
            && !queues.normal_in_progress
    }
    pub(crate) fn generation(&self) -> u64 {
        self.queues.lock().unwrap().generation
    }
    /// Observe the published local cutoff without waiting for queue payload
    /// destruction. This observation does not reserve runtime admission.
    pub(crate) fn observed_generation(&self) -> u64 {
        self.observed_generation.load(Ordering::SeqCst)
    }
    fn remember_interruption(queues: &mut Queues, id: u64) {
        queues.interruption_ids.insert(id);
        queues.interruptions.push_back(id);
        if queues.interruptions.len() > 512 {
            if let Some(old) = queues.interruptions.pop_front() {
                queues.interruption_ids.remove(&old);
            }
        }
    }
    pub(crate) fn set_normal_paused(&self, paused: bool) {
        self.queues.lock().unwrap().normal_paused = paused;
        self.changed.notify_waiters();
    }
    async fn receive(&self) -> Option<DirectedFrame> {
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(frame) = self.pop() {
                return Some(frame);
            }
            if self.queues.lock().unwrap().closed {
                return None;
            }
            notified.await;
        }
    }
    fn pop(&self) -> Option<DirectedFrame> {
        let mut queues = self.queues.lock().unwrap();
        let item = queues
            .start
            .pop_front()
            .or_else(|| queues.system.pop_front().map(|(_, item)| item))
            .or_else(|| queues.normal.pop_front().map(|(_, item)| item));
        drop(queues);
        if item.is_some() {
            self.changed.notify_waiters();
        }
        item
    }
    /// Drop only interruptible normal frames. System frames remain queued.
    pub fn reset_interruptible(&self) -> usize {
        let _admission = self.admission.lock().unwrap();
        self.interrupt_now(None, None).1
    }
    pub(crate) fn interrupt_now(
        &self,
        identity: Option<u64>,
        token: Option<&AtomicU64>,
    ) -> (u64, usize) {
        let mut queues = self.queues.lock().unwrap();
        if let Some(id) = identity {
            Self::remember_interruption(&mut queues, id);
        }
        let generation = self.interruptions.lock().unwrap().reserve(identity, token);
        queues.generation = queues.generation.max(generation);
        self.observed_generation
            .store(queues.generation, Ordering::SeqCst);
        let removed = Self::reset_before(&mut queues, generation);
        drop(queues);
        self.changed.notify_waiters();
        (generation, removed)
    }
    /// A queued interruption only invalidates work admitted before its boundary.
    pub(crate) fn reset_before_generation(&self, generation: u64) -> usize {
        let mut queues = self.queues.lock().unwrap();
        let removed = Self::reset_before(&mut queues, generation);
        drop(queues);
        self.changed.notify_waiters();
        removed
    }
    fn reset_before(queues: &mut Queues, generation: u64) -> usize {
        let before = queues.normal.len();
        queues
            .normal
            .retain(|(epoch, item)| *epoch >= generation || !item.frame.interruptible);
        before - queues.normal.len()
    }
    pub fn has_uninterruptible(&self) -> bool {
        self.queues
            .lock()
            .unwrap()
            .normal
            .iter()
            .any(|(_, item)| !item.frame.interruptible)
    }
    pub fn len(&self) -> usize {
        let queues = self.queues.lock().unwrap();
        queues.start.len() + queues.system.len() + queues.normal.len()
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn close(&self) {
        self.queues.lock().unwrap().closed = true;
        self.changed.notify_waiters();
    }
    pub(crate) fn discard_normal(&self) -> usize {
        let mut queues = self.queues.lock().unwrap();
        let removed = queues.normal.len();
        queues.normal.clear();
        drop(queues);
        self.changed.notify_waiters();
        removed
    }
}
#[cfg(test)]
#[path = "queue_observation_tests.rs"]
mod observation_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Direction, Frame, FrameData};
    use std::sync::Arc;
    use std::time::Duration;
    fn down(frame: Frame) -> DirectedFrame {
        DirectedFrame::new(frame, Direction::Downstream)
    }
    #[tokio::test]
    async fn stable_start_system_data_priority() {
        let queue = FrameQueue::new(8);
        queue.send(down(Frame::text("a"))).await.unwrap();
        queue
            .send(down(Frame::new(FrameData::BotStartedSpeaking)))
            .await
            .unwrap();
        queue.send(down(Frame::text("b"))).await.unwrap();
        queue.send(down(Frame::start())).await.unwrap();
        queue
            .send(down(Frame::new(FrameData::UserStartedSpeaking)))
            .await
            .unwrap();
        assert!(matches!(
            queue.recv().await.unwrap().frame.data,
            FrameData::Start(_)
        ));
        assert!(matches!(
            queue.recv().await.unwrap().frame.data,
            FrameData::BotStartedSpeaking
        ));
        assert!(matches!(
            queue.recv().await.unwrap().frame.data,
            FrameData::UserStartedSpeaking
        ));
        for expected in ["a", "b"] {
            match queue.recv().await.unwrap().frame.data {
                FrameData::Text(text) => assert_eq!(text.text, expected),
                _ => panic!(),
            }
        }
    }
    #[tokio::test]
    async fn interruption_keeps_instance_overrides_and_terminal() {
        let queue = FrameQueue::new(8);
        queue
            .send(down(Frame::text("keep").with_interruptible(false)))
            .await
            .unwrap();
        queue
            .send(down(Frame::end().with_interruptible(true)))
            .await
            .unwrap();
        queue.send(down(Frame::end())).await.unwrap();
        assert_eq!(queue.reset_interruptible(), 1);
        assert_eq!(queue.len(), 2);
        assert!(queue.has_uninterruptible());
    }
    #[tokio::test]
    async fn blocked_sender_wakes_on_capacity_or_close() {
        let queue = Arc::new(FrameQueue::new(1));
        queue.send(down(Frame::text("a"))).await.unwrap();
        let pending = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.send(down(Frame::text("b"))).await })
        };
        tokio::task::yield_now().await;
        assert!(!pending.is_finished());
        queue.recv().await.unwrap();
        tokio::time::timeout(Duration::from_secs(1), pending)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let pending = {
            let queue = queue.clone();
            tokio::spawn(async move { queue.send(down(Frame::text("c"))).await })
        };
        queue.close();
        assert_eq!(pending.await.unwrap(), Err(QueueError::Closed));
    }
}
