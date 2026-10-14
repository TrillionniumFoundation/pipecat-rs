//! Opt-in bounded root-sink traversal diagnostics, not business-health authority.
use crate::{Frame, FrameData, FrameObservationToken, LocalFrameTag, PipelineError};
use std::{collections::VecDeque, time::Duration};
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Clone, Copy)]
pub struct HeartbeatConfig {
    /// Fixed delay between attempts. 10ms..=24h; default 1s.
    pub period: Duration,
    /// No-receipt timeout and individual probe lifetime. 10ms..=24h; default 10s.
    pub timeout: Duration,
    /// Retained weak probe identities, 1..=128; default 32.
    pub max_pending: usize,
}
impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            period: Duration::from_secs(1),
            timeout: Duration::from_secs(10),
            max_pending: 32,
        }
    }
}
impl HeartbeatConfig {
    pub(crate) fn validate(self) -> Result<(), PipelineError> {
        let range = Duration::from_millis(10)..=Duration::from_secs(24 * 60 * 60);
        if !range.contains(&self.period)
            || !range.contains(&self.timeout)
            || !(1..=128).contains(&self.max_pending)
        {
            return Err(PipelineError::InvalidConfiguration(
                "heartbeat intervals must be 10ms..=24h and pending capacity 1..=128".into(),
            ));
        }
        Ok(())
    }
}

/// Monotonic saturating counters and the most recent accepted round trip.
/// `active` means the monitor runs, not that all business work is healthy.
/// Watch consumers may miss intermediate snapshots; counters preserve totals.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct HeartbeatSnapshot {
    pub active: bool,
    pub draining: bool,
    pub sent: u64,
    pub received: u64,
    pub timeouts: u64,
    pub admission_failures: u64,
    pub capacity_skips: u64,
    pub retired_unreceived: u64,
    pub ignored_receipts: u64,
    pub pending: usize,
    pub last_round_trip: Option<Duration>,
}
/// A bounded latest-value watch, with no runtime/task ownership or callbacks.
#[derive(Clone)]
pub struct HeartbeatMonitor {
    receiver: watch::Receiver<HeartbeatSnapshot>,
}
impl HeartbeatMonitor {
    pub fn snapshot(&self) -> HeartbeatSnapshot {
        self.receiver.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<HeartbeatSnapshot, watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.receiver.borrow_and_update().clone())
    }
}
struct Probe {
    token: FrameObservationToken,
    timestamp: u64,
    sent: Instant,
}
pub(crate) struct HeartbeatState {
    config: HeartbeatConfig,
    closed: bool,
    origin: LocalFrameTag,
    started: Option<Instant>,
    next_send: Option<Instant>,
    next_timeout: Option<Instant>,
    pending: VecDeque<Probe>,
    status: HeartbeatSnapshot,
    published: watch::Sender<HeartbeatSnapshot>,
}
impl HeartbeatState {
    pub(crate) fn new(config: HeartbeatConfig) -> Self {
        let (published, _) = watch::channel(HeartbeatSnapshot::default());
        Self {
            config,
            closed: false,
            origin: LocalFrameTag::new(),
            started: None,
            next_send: None,
            next_timeout: None,
            pending: VecDeque::new(),
            status: Default::default(),
            published,
        }
    }
    pub(crate) fn monitor(&self) -> HeartbeatMonitor {
        HeartbeatMonitor {
            receiver: self.published.subscribe(),
        }
    }
    pub(crate) fn owns(&self, frame: &Frame) -> bool {
        matches!(frame.data, FrameData::Heartbeat { .. }) && self.origin.contains(frame)
    }
    fn publish(&mut self) {
        self.status.pending = self.pending.len();
        if *self.published.borrow() != self.status {
            self.published.send_replace(self.status.clone());
        }
    }
    fn retire(&mut self, now: Instant) {
        let before = self.pending.len();
        self.pending.retain(|p| {
            p.token.is_alive() && now.saturating_duration_since(p.sent) < self.config.timeout
        });
        self.status.retired_unreceived = self
            .status
            .retired_unreceived
            .saturating_add((before - self.pending.len()) as u64);
    }
    pub(crate) fn tick(
        &mut self,
        now: Instant,
        accepting: bool,
        mut enqueue: impl FnMut(Frame) -> bool,
    ) {
        if self.closed {
            return;
        }
        if self.started.is_none() {
            self.started = Some(now);
            self.next_send = Some(now);
            self.next_timeout = Some(now + self.config.timeout);
            self.status.active = true;
        }
        if !self.status.active {
            return;
        }
        self.status.draining = !accepting;
        self.retire(now);
        if self.next_timeout.is_some_and(|due| now >= due) {
            self.status.timeouts = self.status.timeouts.saturating_add(1);
            self.next_timeout = Some(now + self.config.timeout);
        }
        if accepting && self.next_send.is_some_and(|due| now >= due) {
            self.next_send = Some(now + self.config.period);
            if self.pending.len() >= self.config.max_pending {
                self.status.capacity_skips = self.status.capacity_skips.saturating_add(1);
            } else {
                let timestamp = now
                    .saturating_duration_since(self.started.unwrap())
                    .as_nanos()
                    .min(u64::MAX as u128) as u64;
                let mut frame = Frame::new(FrameData::Heartbeat { timestamp });
                // A fresh frame has no local tags; one private provenance tag fits.
                self.origin
                    .mark(&mut frame)
                    .expect("one tag fits a fresh frame");
                let token = frame.observation_token();
                if enqueue(frame) {
                    self.pending.push_back(Probe {
                        token,
                        timestamp,
                        sent: now,
                    });
                    self.status.sent = self.status.sent.saturating_add(1);
                } else {
                    self.status.admission_failures =
                        self.status.admission_failures.saturating_add(1);
                }
            }
        }
        self.publish();
    }
    /// Consume only this run's diagnostic frames. An acknowledgement requires
    /// the original native logical identity, timestamp and an unexpired slot.
    pub(crate) fn receive(&mut self, frame: &Frame, now: Instant) -> bool {
        if !self.owns(frame) {
            return false;
        }
        if !self.status.active {
            return true;
        }
        self.retire(now);
        let token = frame.observation_token();
        let FrameData::Heartbeat { timestamp } = frame.data else {
            unreachable!()
        };
        if let Some(index) = self
            .pending
            .iter()
            .position(|p| p.token == token && p.timestamp == timestamp)
        {
            let probe = self.pending.remove(index).unwrap();
            self.status.received = self.status.received.saturating_add(1);
            self.status.last_round_trip = Some(now.saturating_duration_since(probe.sent));
            self.next_timeout = Some(now + self.config.timeout);
        } else {
            self.status.ignored_receipts = self.status.ignored_receipts.saturating_add(1);
        }
        self.publish();
        true
    }
    pub(crate) fn pressure_drop(&mut self, frame: &Frame) -> bool {
        if !self.owns(frame) {
            return false;
        }
        if self.status.active {
            self.status.admission_failures = self.status.admission_failures.saturating_add(1);
            // Another fanout clone may still complete a real root path. Keep
            // the weak slot until receipt, expiry or all clones are dropped.
            self.publish();
        }
        true
    }
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.status.active = false;
        self.status.draining = false;
        self.pending.clear();
        self.publish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> HeartbeatConfig {
        HeartbeatConfig {
            period: Duration::from_millis(10),
            timeout: Duration::from_millis(50),
            max_pending: 2,
        }
    }
    fn tick(state: &mut HeartbeatState, at: Instant, accepting: bool) -> Vec<Frame> {
        let mut frames = vec![];
        state.tick(at, accepting, |f| {
            frames.push(f);
            true
        });
        frames
    }
    #[test]
    fn dropping_one_fanout_clone_preserves_other_real_receipt() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        let frames = tick(&mut state, now, true);
        let branch = frames[0].clone();
        assert!(state.pressure_drop(&branch));
        drop(branch);
        assert!(state.receive(&frames[0], now + Duration::from_millis(1)));
        assert_eq!(state.status.received, 1);
    }
    #[test]
    fn close_before_activation_cannot_restart_monitor() {
        let mut state = HeartbeatState::new(config());
        state.close();
        assert!(tick(&mut state, Instant::now(), true).is_empty());
        assert!(!state.monitor().snapshot().active);
    }
    #[test]
    fn source_defaults_and_native_config_bounds() {
        let c = HeartbeatConfig::default();
        assert_eq!(c.period, Duration::from_secs(1));
        assert_eq!(c.timeout, Duration::from_secs(10));
        c.validate().unwrap();
        for value in [
            Duration::ZERO,
            Duration::from_millis(9),
            Duration::from_secs(86401),
        ] {
            assert!(HeartbeatConfig { period: value, ..c }.validate().is_err());
            assert!(HeartbeatConfig {
                timeout: value,
                ..c
            }
            .validate()
            .is_err());
        }
        for value in [0, 129] {
            assert!(HeartbeatConfig {
                max_pending: value,
                ..c
            }
            .validate()
            .is_err());
        }
        HeartbeatConfig {
            period: Duration::from_millis(10),
            timeout: Duration::from_secs(86400),
            max_pending: 128,
        }
        .validate()
        .unwrap();
    }
    #[test]
    fn immediate_probe_receipt_resets_deadline_and_has_exact_rtt() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        let monitor = state.monitor();
        assert!(!monitor.snapshot().active);
        let frames = tick(&mut state, now, true);
        assert_eq!(frames.len(), 1);
        assert!(state.receive(&frames[0], now + Duration::from_millis(20)));
        assert_eq!(
            monitor.snapshot().last_round_trip,
            Some(Duration::from_millis(20))
        );
        tick(&mut state, now + Duration::from_millis(69), false);
        assert_eq!(monitor.snapshot().timeouts, 0);
        tick(&mut state, now + Duration::from_millis(70), false);
        assert_eq!(monitor.snapshot().timeouts, 1);
    }
    #[test]
    fn duplicate_public_id_wire_reconstruction_and_modified_timestamp_do_not_refresh() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        let frames = tick(&mut state, now, true);
        let frame = &frames[0];
        let mut duplicate = frame.duplicate();
        duplicate.id = frame.id;
        assert!(state.receive(&duplicate, now + Duration::from_millis(5)));
        let restored: Frame = serde_json::from_value(serde_json::to_value(frame).unwrap()).unwrap();
        assert!(!state.receive(&restored, now + Duration::from_millis(6)));
        let mut changed = frame.clone();
        changed.data = FrameData::Heartbeat { timestamp: 999 };
        assert!(state.receive(&changed, now + Duration::from_millis(7)));
        assert_eq!(state.status.received, 0);
        assert!(state.receive(frame, now + Duration::from_millis(8)));
        assert!(state.receive(frame, now + Duration::from_millis(45)));
        tick(&mut state, now + Duration::from_millis(58), false);
        assert_eq!((state.status.received, state.status.timeouts), (1, 1));
    }
    #[test]
    fn pending_bound_expiry_and_repeated_timeouts_recover_on_a_fresh_probe() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(HeartbeatConfig {
            max_pending: 1,
            ..config()
        });
        let old = tick(&mut state, now, true);
        assert!(tick(&mut state, now + Duration::from_millis(10), true).is_empty());
        assert_eq!(state.status.capacity_skips, 1);
        let fresh = tick(&mut state, now + Duration::from_millis(50), true);
        assert_eq!(
            (
                state.status.timeouts,
                state.status.retired_unreceived,
                state.pending.len()
            ),
            (1, 1, 1)
        );
        state.receive(&old[0], now + Duration::from_millis(55));
        assert_eq!(state.status.received, 0);
        tick(&mut state, now + Duration::from_millis(100), false);
        assert_eq!(state.status.timeouts, 2);
        state.receive(&fresh[0], now + Duration::from_millis(100));
        assert_eq!(state.status.received, 0, "deadline equality expires");
        let newest = tick(&mut state, now + Duration::from_millis(101), true);
        state.receive(&newest[0], now + Duration::from_millis(102));
        assert_eq!(state.status.received, 1);
    }
    #[test]
    fn rejected_queue_admission_is_not_a_receipt_and_does_not_retain_a_frame() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        state.tick(now, true, |_| false);
        assert_eq!(
            (
                state.status.admission_failures,
                state.status.sent,
                state.pending.len()
            ),
            (1, 0, 0)
        );
        state.tick(now + Duration::from_millis(50), true, |_| false);
        assert_eq!(state.status.timeouts, 1);
        let frames = tick(&mut state, now + Duration::from_millis(60), true);
        state.receive(&frames[0], now + Duration::from_millis(61));
        assert_eq!(state.status.received, 1);
    }
    #[test]
    fn dropped_frames_retire_weak_identity_and_never_hold_the_payload_alive() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        let frames = tick(&mut state, now, true);
        let token = frames[0].observation_token();
        drop(frames);
        assert!(!token.is_alive());
        tick(&mut state, now + Duration::from_millis(1), false);
        assert_eq!(
            (state.pending.len(), state.status.retired_unreceived),
            (0, 1)
        );
    }
    #[test]
    fn pressure_drop_is_private_origin_scoped_even_after_probe_expiry() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        let old = tick(&mut state, now, true);
        tick(&mut state, now + Duration::from_millis(50), false);
        assert!(state.pressure_drop(&old[0]));
        assert_eq!(state.status.admission_failures, 1);
        let mut foreign = Frame::new(old[0].data.clone());
        foreign.id = old[0].id;
        assert!(!state.pressure_drop(&foreign));
        let mut changed = old[0].clone();
        changed.data = FrameData::Data;
        assert!(!state.pressure_drop(&changed));
        let mut next = HeartbeatState::new(config());
        assert!(!next.receive(&old[0], now));
        assert!(!next.pressure_drop(&old[0]));
    }
    #[test]
    fn drain_stops_injection_without_resetting_timeout_and_close_freezes_counters() {
        let now = Instant::now();
        let mut state = HeartbeatState::new(config());
        let old = tick(&mut state, now, true);
        assert!(tick(&mut state, now + Duration::from_millis(50), false).is_empty());
        assert!(state.status.draining);
        assert_eq!(state.status.timeouts, 1);
        state.close();
        let before = state.monitor().snapshot();
        assert!(!before.active);
        assert_eq!(before.pending, 0);
        state.receive(&old[0], now + Duration::from_millis(51));
        state.pressure_drop(&old[0]);
        tick(&mut state, now + Duration::from_millis(100), true);
        assert_eq!(state.monitor().snapshot(), before);
    }
}
