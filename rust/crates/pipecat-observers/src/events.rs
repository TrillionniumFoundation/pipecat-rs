//! Shared bounded output and loss guards for source lifecycle observers.
use pipecat_core::{Direction, ObservationEvent, ObserverHandle, PipelineObservation};
use tokio::sync::broadcast;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceInvalidation {
    SequenceGap,
    NonMonotonicTimestamp,
    CaptureTruncated,
    StateCapacity,
    InvalidVadOffset,
}
#[derive(Debug, Clone, PartialEq)]
pub enum SourceEvent<T> {
    Record(T),
    ObservationLoss { dropped_total: u64 },
    Invalidated { reason: SourceInvalidation },
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SourceObserverStats {
    pub observations_dropped: u64,
    pub events_emitted: u64,
    pub events_without_receivers: u64,
    pub closed: bool,
}
pub(crate) struct EventState<T: Clone> {
    events: Option<broadcast::Sender<SourceEvent<T>>>,
    pub(crate) health: Option<ObserverHandle>,
    sequence: Option<u64>,
    delivered_loss: u64,
    dropped: u64,
    emitted: u64,
    without_receivers: u64,
    pub(crate) closed: bool,
}
pub(crate) struct Admission {
    pub(crate) accept: bool,
    pub(crate) invalidate: bool,
}
impl<T: Clone> EventState<T> {
    pub(crate) fn new(capacity: usize) -> Result<Self, &'static str> {
        if !(1..=65_536).contains(&capacity) {
            return Err("source event capacity must be between 1 and 65536");
        }
        let (events, _) = broadcast::channel(capacity);
        Ok(Self {
            events: Some(events),
            health: None,
            sequence: None,
            delivered_loss: 0,
            dropped: 0,
            emitted: 0,
            without_receivers: 0,
            closed: false,
        })
    }
    pub(crate) fn fresh(&self) -> bool {
        !self.closed && self.health.is_none() && self.sequence.is_none()
    }
    pub(crate) fn subscribe(&self) -> broadcast::Receiver<SourceEvent<T>> {
        if let Some(events) = &self.events {
            events.subscribe()
        } else {
            let (_, receiver) = broadcast::channel(1);
            receiver
        }
    }
    pub(crate) fn emit(&mut self, event: SourceEvent<T>) {
        if self.closed {
            return;
        }
        self.emitted = self.emitted.saturating_add(1);
        if self
            .events
            .as_ref()
            .is_none_or(|events| events.send(event).is_err())
        {
            self.without_receivers = self.without_receivers.saturating_add(1);
        }
    }
    pub(crate) fn invalidate(&mut self, reason: SourceInvalidation) {
        self.emit(SourceEvent::Invalidated { reason });
    }
    pub(crate) fn check_loss(&mut self, delivered: u64) -> bool {
        let dropped = self
            .health
            .as_ref()
            .map_or(delivered, |health| health.stats().dropped.max(delivered));
        if dropped <= self.dropped {
            return false;
        }
        self.dropped = dropped;
        self.emit(SourceEvent::ObservationLoss {
            dropped_total: dropped,
        });
        true
    }
    pub(crate) fn admit(&mut self, event: &ObservationEvent) -> Admission {
        if self.closed {
            return Admission {
                accept: false,
                invalidate: false,
            };
        }
        let mut invalidate = self.check_loss(event.dropped_before);
        if self.sequence.is_some_and(|last| event.sequence <= last) {
            return Admission {
                accept: false,
                invalidate,
            };
        }
        let expected = self.sequence.map_or(0, |last| last.saturating_add(1));
        if event.sequence != expected && !invalidate && event.dropped_before == self.delivered_loss
        {
            self.invalidate(SourceInvalidation::SequenceGap);
            invalidate = true;
        }
        self.sequence = Some(event.sequence);
        self.delivered_loss = event.dropped_before;
        Admission {
            accept: event.dropped_before >= self.dropped,
            invalidate,
        }
    }
    pub(crate) fn close(&mut self) {
        self.check_loss(self.delivered_loss);
        self.closed = true;
        self.events.take();
    }
    pub(crate) fn stats(&self) -> SourceObserverStats {
        SourceObserverStats {
            observations_dropped: self.health.as_ref().map_or(self.dropped, |health| {
                health.stats().dropped.max(self.dropped)
            }),
            events_emitted: self.emitted,
            events_without_receivers: self.without_receivers,
            closed: self.closed,
        }
    }
}
pub(crate) fn terminal(event: &ObservationEvent) -> bool {
    matches!(&*event.observation, PipelineObservation::Push {
        frame, first_push: true, direction: Direction::Downstream, ..
    } if matches!(frame.kind, "EndFrame" | "StopFrame" | "CancelFrame"))
}
