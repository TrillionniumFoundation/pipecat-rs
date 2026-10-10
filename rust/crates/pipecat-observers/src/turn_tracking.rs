//! Content-free conversation turns with a bounded event stream and owned timer.
use pipecat_core::{
    async_trait, FrameObservationToken, ObservationEvent, ObserverHandle, ObserverOptions,
    Pipeline, PipelineObservation, PipelineObserver,
};
use serde::{Deserialize, Serialize};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct TurnTrackingConfig {
    /// Silence after bot speech before closing the conversation turn.
    pub turn_end_timeout: Duration,
    /// Bounded broadcast history. Slow receivers get `RecvError::Lagged`.
    pub event_capacity: usize,
}
impl Default for TurnTrackingConfig {
    fn default() -> Self {
        Self {
            turn_end_timeout: Duration::from_millis(2500),
            event_capacity: 256,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnEndReason {
    BotStoppedTimeout,
    UserStarted,
    End,
    Cancel,
    Stop,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TurnInvalidationReason {
    NonMonotonicTimestamp,
    SequenceGap,
    TimerDeadlineOverflow,
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TurnTrackingEvent {
    Started {
        turn_number: u64,
        /// Native control-frame identity; absent after wire serialization.
        #[serde(skip)]
        boundary: Option<FrameObservationToken>,
        timestamp_ns: u64,
        /// False after any input loss: numbers count observed starts only.
        numbering_complete: bool,
    },
    Ended {
        turn_number: u64,
        /// For a timeout, identifies its BotStoppedSpeakingFrame.
        #[serde(skip)]
        boundary: Option<FrameObservationToken>,
        started_ns: u64,
        /// For a timeout, this is the triggering bot-stop time, not timer firing.
        timestamp_ns: u64,
        duration_seconds: f64,
        was_interrupted: bool,
        reason: TurnEndReason,
        numbering_complete: bool,
    },
    /// No synthetic Ended event or duration is emitted for an incomplete turn.
    ObservationLoss {
        dropped_total: u64,
        invalidated_turn: Option<u64>,
    },
    Invalidated {
        turn_number: Option<u64>,
        reason: TurnInvalidationReason,
    },
}
/// Native-only, bounded recording stream. Controls and their resulting turn
/// events share one serialized order; consumers must invalidate on any lag.
#[derive(Debug, Clone)]
pub enum TurnRecordingUpdate {
    Control {
        token: Option<FrameObservationToken>,
        kind: &'static str,
    },
    Turn(TurnTrackingEvent),
}
/// Controls that change a recorder's run or recording state. Both source
/// observation and recorder processing use this exact classification.
pub fn is_turn_recording_control(kind: &str) -> bool {
    matches!(
        kind,
        "StartFrame"
            | "UserStartedSpeakingFrame"
            | "UserStoppedSpeakingFrame"
            | "BotStartedSpeakingFrame"
            | "BotStoppedSpeakingFrame"
            | "AudioBufferStartRecordingFrame"
            | "AudioBufferStopRecordingFrame"
            | "EndFrame"
            | "CancelFrame"
            | "StopFrame"
    )
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TurnTrackingStats {
    pub turns_started: u64,
    pub active_turn: Option<u64>,
    pub numbering_complete: bool,
    pub observations_dropped: u64,
    pub events_emitted: u64,
    /// Emissions with no subscribers, distinct from per-receiver lag.
    pub events_without_receivers: u64,
    pub timer_pending: bool,
    pub closed: bool,
}
struct ActiveTurn {
    number: u64,
    started_ns: u64,
    bot_speaking: bool,
    bot_has_spoken: bool,
}
#[derive(Clone, PartialEq, Eq)]
struct PendingEnd {
    deadline: Instant,
    stopped_ns: u64,
    boundary: Option<FrameObservationToken>,
}
struct State {
    active: Option<ActiveTurn>,
    turn_count: u64,
    pending: Option<PendingEnd>,
    last_sequence: Option<u64>,
    last_delivered_dropped: u64,
    last_timestamp_ns: Option<u64>,
    dropped: u64,
    numbering_complete: bool,
    events: Option<broadcast::Sender<TurnTrackingEvent>>,
    recording_updates: Option<broadcast::Sender<TurnRecordingUpdate>>,
    events_emitted: u64,
    events_without_receivers: u64,
    health: Option<ObserverHandle>,
    closed: bool,
}
impl State {
    fn emit(&mut self, event: TurnTrackingEvent) {
        if let Some(updates) = &self.recording_updates {
            let _ = updates.send(TurnRecordingUpdate::Turn(event.clone()));
        }
        self.events_emitted = self.events_emitted.saturating_add(1);
        if self
            .events
            .as_ref()
            .is_none_or(|events| events.send(event).is_err())
        {
            self.events_without_receivers = self.events_without_receivers.saturating_add(1);
        }
    }
    fn invalidate(&mut self, reason: TurnInvalidationReason) {
        let turn_number = self.active.take().map(|turn| turn.number);
        self.pending = None;
        self.numbering_complete = false;
        self.emit(TurnTrackingEvent::Invalidated {
            turn_number,
            reason,
        });
    }
    fn check_loss(&mut self, delivered_loss: u64) {
        // The live handle also detects a final queue overflow with no subsequent
        // delivered event. Such a loss must cancel an outstanding turn deadline.
        let dropped = self.health.as_ref().map_or(delivered_loss, |handle| {
            handle.stats().dropped.max(delivered_loss)
        });
        if dropped > self.dropped {
            self.dropped = dropped;
            let invalidated_turn = self.active.take().map(|turn| turn.number);
            self.pending = None;
            self.numbering_complete = false;
            self.emit(TurnTrackingEvent::ObservationLoss {
                dropped_total: dropped,
                invalidated_turn,
            });
        }
    }
    fn start(&mut self, at: u64, boundary: Option<FrameObservationToken>) {
        self.turn_count = self.turn_count.saturating_add(1);
        self.active = Some(ActiveTurn {
            number: self.turn_count,
            started_ns: at,
            bot_speaking: false,
            bot_has_spoken: false,
        });
        self.emit(TurnTrackingEvent::Started {
            turn_number: self.turn_count,
            boundary,
            timestamp_ns: at,
            numbering_complete: self.numbering_complete,
        });
    }
    fn end(
        &mut self,
        at: u64,
        interrupted: bool,
        reason: TurnEndReason,
        boundary: Option<FrameObservationToken>,
    ) {
        self.pending = None;
        let Some(turn) = self.active.take() else {
            return;
        };
        let Some(duration) = at.checked_sub(turn.started_ns) else {
            self.active = Some(turn);
            self.invalidate(TurnInvalidationReason::NonMonotonicTimestamp);
            return;
        };
        self.emit(TurnTrackingEvent::Ended {
            turn_number: turn.number,
            boundary,
            started_ns: turn.started_ns,
            timestamp_ns: at,
            duration_seconds: duration as f64 / 1e9,
            was_interrupted: interrupted,
            reason,
            numbering_complete: self.numbering_complete,
        });
    }
    fn observe(&mut self, event: &ObservationEvent, timeout: Duration) {
        if self.closed {
            return;
        }
        let prior_loss = self.dropped;
        self.check_loss(event.dropped_before);
        if self
            .last_sequence
            .is_some_and(|last| event.sequence <= last)
        {
            return;
        }
        let expected = self.last_sequence.map_or(0, |last| last.saturating_add(1));
        if event.sequence != expected
            && prior_loss == self.dropped
            && event.dropped_before == self.last_delivered_dropped
        {
            self.invalidate(TurnInvalidationReason::SequenceGap);
        }
        self.last_sequence = Some(event.sequence);
        self.last_delivered_dropped = event.dropped_before;
        // Events queued before a newly detected loss cannot establish a reliable
        // fresh turn, even if they are delivered after the timer sees the loss.
        if event.dropped_before < self.dropped {
            return;
        }
        let PipelineObservation::Push {
            frame,
            first_push: true,
            timestamp_ns: at,
            ..
        } = &*event.observation
        else {
            return;
        };
        if is_turn_recording_control(frame.kind) {
            if let Some(updates) = &self.recording_updates {
                let _ = updates.send(TurnRecordingUpdate::Control {
                    token: frame.token.clone(),
                    kind: frame.kind,
                });
            }
        }
        if !matches!(
            frame.kind,
            "StartFrame"
                | "UserStartedSpeakingFrame"
                | "BotStartedSpeakingFrame"
                | "BotStoppedSpeakingFrame"
                | "EndFrame"
                | "CancelFrame"
                | "StopFrame"
        ) {
            return;
        }
        // Late stop broadcasts belong to the interrupted speech, not the new
        // turn. Ignore them even if their captured timestamp predates rollover.
        if (frame.kind == "BotStoppedSpeakingFrame"
            && !self.active.as_ref().is_some_and(|turn| turn.bot_speaking))
            || (frame.kind == "StartFrame" && self.turn_count != 0)
            || (frame.kind == "BotStartedSpeakingFrame" && self.active.is_none())
        {
            return;
        }
        if self.last_timestamp_ns.is_some_and(|last| *at < last) {
            self.invalidate(TurnInvalidationReason::NonMonotonicTimestamp);
            // Terminal events must still close resources even with a bad clock.
            if matches!(frame.kind, "EndFrame" | "CancelFrame" | "StopFrame") {
                self.close();
            }
            return;
        }
        self.last_timestamp_ns = Some(*at);
        match frame.kind {
            "StartFrame" if self.turn_count == 0 => self.start(*at, frame.token.clone()),
            "UserStartedSpeakingFrame" => {
                if let Some(turn) = self.active.as_ref() {
                    if turn.bot_speaking || turn.bot_has_spoken {
                        self.end(
                            *at,
                            turn.bot_speaking,
                            TurnEndReason::UserStarted,
                            frame.token.clone(),
                        );
                    }
                }
                if self.active.is_none() {
                    self.start(*at, frame.token.clone());
                }
            }
            "BotStartedSpeakingFrame" => {
                if let Some(turn) = self.active.as_mut() {
                    turn.bot_speaking = true;
                    turn.bot_has_spoken = true;
                    self.pending = None;
                }
            }
            "BotStoppedSpeakingFrame" => {
                if let Some(turn) = self.active.as_mut().filter(|turn| turn.bot_speaking) {
                    turn.bot_speaking = false;
                    if let Some(deadline) = Instant::now().checked_add(timeout) {
                        self.pending = Some(PendingEnd {
                            deadline,
                            stopped_ns: *at,
                            boundary: frame.token.clone(),
                        });
                    } else {
                        self.invalidate(TurnInvalidationReason::TimerDeadlineOverflow);
                    }
                }
            }
            "EndFrame" | "CancelFrame" | "StopFrame" => {
                let reason = match frame.kind {
                    "EndFrame" => TurnEndReason::End,
                    "CancelFrame" => TurnEndReason::Cancel,
                    _ => TurnEndReason::Stop,
                };
                self.end(*at, true, reason, frame.token.clone());
                self.close();
            }
            _ => {}
        }
    }
    fn close(&mut self) {
        self.closed = true;
        self.pending = None;
        self.active = None;
        self.events.take();
        self.recording_updates.take();
    }
}
struct Shared {
    state: Mutex<State>,
}
/// Allocate one instance per root pipeline and install it with `register`.
/// Nested pipelines inherit that root registration. Only allowlisted frame
/// kinds and monotonic timestamps are retained; there is no content capture.
///
/// The pipeline dispatcher owns its deadline and serializes expiration with
/// observations. This observer never spawns a background task. Standalone
/// fixtures can explicitly advance Tokio time and invoke `on_timeout`.
pub struct TurnTrackingObserver {
    config: TurnTrackingConfig,
    shared: Arc<Shared>,
}
impl Default for TurnTrackingObserver {
    fn default() -> Self {
        Self::new()
    }
}
impl TurnTrackingObserver {
    pub fn new() -> Self {
        Self::with_config(TurnTrackingConfig::default())
            .expect("valid default turn tracking config")
    }
    pub fn with_config(config: TurnTrackingConfig) -> Result<Self, &'static str> {
        if config.event_capacity == 0 || config.event_capacity > 65_536 {
            return Err("turn event capacity must be between 1 and 65536");
        }
        if Instant::now()
            .checked_add(config.turn_end_timeout)
            .is_none()
        {
            return Err("turn timeout exceeds the monotonic clock range");
        }
        let (events, _) = broadcast::channel(config.event_capacity);
        Ok(Self {
            config,
            shared: Arc::new(Shared {
                state: Mutex::new(State {
                    active: None,
                    turn_count: 0,
                    pending: None,
                    last_sequence: None,
                    last_delivered_dropped: 0,
                    last_timestamp_ns: None,
                    dropped: 0,
                    numbering_complete: true,
                    events: Some(events),
                    events_emitted: 0,
                    recording_updates: None,
                    events_without_receivers: 0,
                    health: None,
                    closed: false,
                }),
            }),
        })
    }
    /// Installs first-push observation and binds live queue-loss health, so a
    /// timer also detects losses with no later delivered event. Directly calling
    /// `Pipeline::add_observer` omits this final-loss protection.
    pub fn register(
        self: &Arc<Self>,
        pipeline: &mut Pipeline,
        mut options: ObserverOptions,
    ) -> Result<ObserverHandle, &'static str> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed || state.health.is_some() || state.last_sequence.is_some() {
            return Err("turn tracker must be fresh and registered only once");
        }
        options.observe_every_push = false;
        let health = pipeline.add_observer(self.clone(), options);
        state.health = Some(health.clone());
        Ok(health)
    }
    /// Subscribe before startup. The stream closes on End/Cancel/Stop/cleanup.
    /// `Lagged` means the receiver must discard incomplete turn attribution.
    pub fn subscribe(&self) -> broadcast::Receiver<TurnTrackingEvent> {
        let state = self.shared.state.lock().unwrap();
        if let Some(events) = &state.events {
            events.subscribe()
        } else {
            let (_, receiver) = broadcast::channel(1);
            receiver
        }
    }
    /// Subscribe before startup to one causally ordered stream of native
    /// controls and turn events. It uses `event_capacity` independently of the
    /// ordinary public event stream and closes with the tracker.
    pub fn subscribe_recording(&self) -> broadcast::Receiver<TurnRecordingUpdate> {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            let (_, receiver) = broadcast::channel(1);
            return receiver;
        }
        state
            .recording_updates
            .get_or_insert_with(|| broadcast::channel(self.config.event_capacity).0)
            .subscribe()
    }
    pub fn stats(&self) -> TurnTrackingStats {
        let state = self.shared.state.lock().unwrap();
        TurnTrackingStats {
            turns_started: state.turn_count,
            active_turn: state.active.as_ref().map(|turn| turn.number),
            numbering_complete: state.numbering_complete,
            observations_dropped: state.dropped,
            events_emitted: state.events_emitted,
            events_without_receivers: state.events_without_receivers,
            timer_pending: state.pending.is_some(),
            closed: state.closed,
        }
    }
    pub fn cancel(&self) {
        self.shared.state.lock().unwrap().close();
    }
    /// Idempotent cleanup emits no invented terminal event when none was seen.
    pub async fn cleanup(&self) {
        self.cancel();
    }
}
#[async_trait]
impl PipelineObserver for TurnTrackingObserver {
    async fn on_event(&self, event: ObservationEvent) {
        self.shared
            .state
            .lock()
            .unwrap()
            .observe(&event, self.config.turn_end_timeout);
    }
    fn next_deadline(&self) -> Option<Instant> {
        self.shared
            .state
            .lock()
            .unwrap()
            .pending
            .as_ref()
            .map(|pending| pending.deadline)
    }
    async fn on_timeout(&self) {
        let mut state = self.shared.state.lock().unwrap();
        if state.closed {
            return;
        }
        state.check_loss(0);
        if let Some(pending) = state
            .pending
            .clone()
            .filter(|pending| pending.deadline <= Instant::now())
        {
            state.end(
                pending.stopped_ns,
                false,
                TurnEndReason::BotStoppedTimeout,
                pending.boundary,
            );
        }
    }
    fn cancel(&self) {
        self.cancel();
    }
    async fn cleanup(&self) {
        self.cleanup().await;
    }
}
impl Drop for TurnTrackingObserver {
    fn drop(&mut self) {
        self.cancel();
    }
}

#[cfg(test)]
#[path = "turn_tracking_tests.rs"]
mod tests;
