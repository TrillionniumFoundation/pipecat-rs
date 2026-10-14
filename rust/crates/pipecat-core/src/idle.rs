//! Explicit root-owned idle policy; activity is successful first-push admission.
use crate::{FrameData, PipelineError};
use std::time::Duration;
use tokio::{sync::watch, time::Instant};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum IdleTimeoutAction {
    NotifyOnly,
    /// Cancel this root composition only. Does not cancel a runner or other roots.
    #[default]
    CancelRoot,
}
#[derive(Debug, Clone, Copy)]
pub struct WorkerIdleConfig {
    /// 10ms..=24h; the source default is 300 seconds.
    pub timeout: Duration,
    pub action: IdleTimeoutAction,
    /// Before adding this root's idle key, cap the frame's existing observation
    /// domain count. 1..=4096, default 64. Existing observer keys are not limited.
    pub max_frame_observation_domains: usize,
}
impl Default for WorkerIdleConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(300),
            action: IdleTimeoutAction::CancelRoot,
            max_frame_observation_domains: 64,
        }
    }
}
impl WorkerIdleConfig {
    pub(crate) fn validate(self) -> Result<(), PipelineError> {
        if !(Duration::from_millis(10)..=Duration::from_secs(86400)).contains(&self.timeout)
            || !(1..=4096).contains(&self.max_frame_observation_domains)
        {
            return Err(PipelineError::InvalidConfiguration(
                "idle timeout must be 10ms..=24h and observation domain limit 1..=4096".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WorkerIdleSnapshot {
    /// A running timer, not a statement that business work is healthy.
    pub active: bool,
    pub activities: u64,
    pub timeouts: u64,
    /// A one-way decision. Subsequent activity cannot revoke cancellation.
    pub cancel_requested: bool,
    /// A missing first-push identity disables this run's idle policy permanently.
    pub coverage_lost: bool,
    /// Native policy stops at root input closure, including graceful End drain.
    pub stopped_for_shutdown: bool,
}
#[derive(Clone)]
pub struct WorkerIdleMonitor {
    receiver: watch::Receiver<WorkerIdleSnapshot>,
}
impl WorkerIdleMonitor {
    pub fn snapshot(&self) -> WorkerIdleSnapshot {
        self.receiver.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<WorkerIdleSnapshot, watch::error::RecvError> {
        self.receiver.changed().await?;
        Ok(self.receiver.borrow_and_update().clone())
    }
}
pub(crate) struct IdleState {
    config: WorkerIdleConfig,
    idle_domain_id: u64,
    started: bool,
    closed: bool,
    activity_pending: bool,
    deadline: Option<Instant>,
    status: WorkerIdleSnapshot,
    published: watch::Sender<WorkerIdleSnapshot>,
}
impl IdleState {
    pub(crate) fn new(config: WorkerIdleConfig, idle_domain_id: u64) -> Self {
        let (published, _) = watch::channel(WorkerIdleSnapshot::default());
        Self {
            config,
            idle_domain_id,
            started: false,
            closed: false,
            activity_pending: false,
            deadline: None,
            status: WorkerIdleSnapshot::default(),
            published,
        }
    }
    pub(crate) fn monitor(&self) -> WorkerIdleMonitor {
        WorkerIdleMonitor {
            receiver: self.published.subscribe(),
        }
    }
    fn publish(&self) {
        if *self.published.borrow() != self.status {
            self.published.send_replace(self.status.clone());
        }
    }
    pub(crate) fn selected(data: &FrameData) -> bool {
        matches!(
            data,
            FrameData::Start(_)
                | FrameData::BotSpeaking
                | FrameData::UserSpeaking
                | FrameData::UserStartedSpeaking
                | FrameData::Transcription(_)
                | FrameData::TranscriptionDetailed(_)
                | FrameData::InterimTranscription(_)
                | FrameData::InterimTranscriptionDetailed(_)
        )
    }
    /// Called after successful native push admission, under composition admission.
    /// The private idle domain is distinct from every actual ObserverHub. Child
    /// activity must not consume a root observer's first actual notification.
    pub(crate) fn observe_push(
        &mut self,
        domains: &crate::observer::ObservedRuntimes,
        selected: bool,
        observer_id: u64,
        observed: bool,
    ) -> bool {
        let mut domains = domains.get_or_init(Default::default).lock().unwrap();
        let mut inserted_idle = false;
        if !self.closed && !self.status.coverage_lost && !domains.contains(&self.idle_domain_id) {
            if domains.len() >= self.config.max_frame_observation_domains {
                self.status.coverage_lost = true;
                self.status.active = false;
                self.activity_pending = false;
                self.deadline = None;
            } else {
                domains.insert(self.idle_domain_id);
                inserted_idle = true;
            }
        }
        let ordinary_first = observed && domains.insert(observer_id);
        drop(domains);
        if inserted_idle && selected {
            self.activity_pending = true;
            self.status.activities = self.status.activities.saturating_add(1);
        }
        self.publish();
        ordinary_first
    }
    /// Starts after setup and before processor actors, without waiting for Start
    /// to reach the sink. Setup-time activity stays coalesced for consumption.
    pub(crate) fn start(&mut self, now: Instant) {
        if self.started || self.closed || self.status.coverage_lost {
            return;
        }
        self.started = true;
        self.status.active = true;
        self.deadline = Some(now + self.config.timeout);
        self.publish();
    }
    /// True requests one root cancellation; retries belong to terminal admission.
    pub(crate) fn tick(&mut self, now: Instant) -> bool {
        if !self.status.active || self.closed || self.status.coverage_lost {
            return false;
        }
        if self.activity_pending {
            self.activity_pending = false;
            self.deadline = Some(now + self.config.timeout);
            return false;
        }
        if self.deadline.is_none_or(|deadline| now < deadline) {
            return false;
        }
        self.status.timeouts = self.status.timeouts.saturating_add(1);
        let cancel = self.config.action == IdleTimeoutAction::CancelRoot;
        if cancel {
            self.closed = true;
            self.status.active = false;
            self.status.cancel_requested = true;
            self.deadline = None;
        } else {
            self.deadline = Some(now + self.config.timeout);
        }
        self.publish();
        cancel
    }
    pub(crate) fn close(&mut self) {
        self.closed = true;
        self.status.active = false;
        self.status.stopped_for_shutdown = true;
        self.activity_pending = false;
        self.deadline = None;
        self.publish();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Frame, StartFrame};
    fn config() -> WorkerIdleConfig {
        WorkerIdleConfig {
            timeout: Duration::from_millis(100),
            action: IdleTimeoutAction::NotifyOnly,
            max_frame_observation_domains: 4,
        }
    }
    fn push(state: &mut IdleState, frame: &Frame, hub: u64, observed: bool) -> bool {
        state.observe_push(
            &frame.observed_runtimes,
            IdleState::selected(&frame.data),
            hub,
            observed,
        )
    }
    #[test]
    fn source_defaults_native_bounds_and_exact_default_selector() {
        let c = WorkerIdleConfig::default();
        assert_eq!(c.timeout, Duration::from_secs(300));
        assert_eq!(c.action, IdleTimeoutAction::CancelRoot);
        for timeout in [
            Duration::ZERO,
            Duration::from_millis(9),
            Duration::from_secs(86401),
        ] {
            assert!(WorkerIdleConfig { timeout, ..c }.validate().is_err());
        }
        for cap in [0, 4097] {
            assert!(WorkerIdleConfig {
                max_frame_observation_domains: cap,
                ..c
            }
            .validate()
            .is_err());
        }
        assert!(IdleState::selected(
            &FrameData::Start(StartFrame::default())
        ));
        for data in [
            FrameData::BotSpeaking,
            FrameData::UserSpeaking,
            FrameData::UserStartedSpeaking,
        ] {
            assert!(IdleState::selected(&data));
        }
        for data in [
            FrameData::Heartbeat { timestamp: 0 },
            FrameData::Data,
            FrameData::BotStartedSpeaking,
            FrameData::UserStoppedSpeaking,
            FrameData::ProposedUserStartedSpeaking,
        ] {
            assert!(!IdleState::selected(&data));
        }
    }
    #[test]
    fn notify_only_repeats_and_consumes_coalesced_activity_at_full_interval() {
        let now = Instant::now();
        let mut s = IdleState::new(config(), 1);
        s.start(now);
        push(&mut s, &Frame::new(FrameData::UserSpeaking), 2, false);
        push(&mut s, &Frame::new(FrameData::BotSpeaking), 2, false);
        assert!(!s.tick(now + Duration::from_millis(90)));
        assert!(!s.tick(now + Duration::from_millis(189)));
        assert_eq!(s.status.timeouts, 0);
        assert!(!s.tick(now + Duration::from_millis(190)));
        assert_eq!(s.status.timeouts, 1);
        assert!(!s.tick(now + Duration::from_millis(290)));
        assert_eq!(s.status.timeouts, 2);
        assert_eq!(s.status.activities, 2);
    }
    #[test]
    fn cancel_decision_is_once_and_cannot_be_retracted_by_activity() {
        let now = Instant::now();
        let mut s = IdleState::new(
            WorkerIdleConfig {
                action: IdleTimeoutAction::CancelRoot,
                ..config()
            },
            1,
        );
        s.start(now);
        assert!(s.tick(now + Duration::from_millis(100)));
        let before = s.monitor().snapshot();
        push(&mut s, &Frame::new(FrameData::UserSpeaking), 2, false);
        assert!(!s.tick(now + Duration::from_secs(1)));
        assert_eq!(before, s.monitor().snapshot());
        assert!(before.cancel_requested && !before.active);
    }
    #[test]
    fn every_frame_marks_first_push_and_mutation_cannot_create_activity() {
        let mut s = IdleState::new(config(), 1);
        let mut f = Frame::text("first non-activity");
        assert!(push(&mut s, &f, 2, true));
        f.data = FrameData::UserSpeaking;
        assert!(!push(&mut s, &f, 2, true));
        assert_eq!(s.status.activities, 0);
        let other = f.duplicate();
        assert!(push(&mut s, &other, 2, true));
        assert_eq!(s.status.activities, 1);
    }
    #[test]
    fn clones_deduplicate_but_wire_reconstruction_and_new_roots_do_not() {
        let mut a = IdleState::new(config(), 1);
        let f = Frame::new(FrameData::UserSpeaking);
        push(&mut a, &f, 2, false);
        push(&mut a, &f.clone(), 2, false);
        assert_eq!(a.status.activities, 1);
        let wire: Frame = serde_json::from_value(serde_json::to_value(&f).unwrap()).unwrap();
        push(&mut a, &wire, 2, false);
        assert_eq!(a.status.activities, 2);
        let mut b = IdleState::new(config(), 3);
        push(&mut b, &f, 4, false);
        assert_eq!(b.status.activities, 1);
    }
    #[test]
    fn independent_child_observer_does_not_consume_root_first_push() {
        let mut s = IdleState::new(config(), 10);
        let f = Frame::new(FrameData::UserSpeaking);
        assert!(push(&mut s, &f, 20, true));
        assert!(push(&mut s, &f, 30, true));
        assert!(!push(&mut s, &f, 30, true));
        assert_eq!(s.status.activities, 1);
    }
    #[test]
    fn cap_loses_coverage_permanently_but_does_not_limit_ordinary_observers() {
        let now = Instant::now();
        let mut s = IdleState::new(
            WorkerIdleConfig {
                max_frame_observation_domains: 1,
                action: IdleTimeoutAction::CancelRoot,
                ..config()
            },
            10,
        );
        s.start(now);
        let f = Frame::new(FrameData::UserSpeaking);
        f.observed_runtimes
            .get_or_init(Default::default)
            .lock()
            .unwrap()
            .insert(20);
        assert!(push(&mut s, &f, 30, true));
        assert!(s.status.coverage_lost);
        assert!(!s.status.active);
        assert!(!s.tick(now + Duration::from_secs(1)));
        let fresh = Frame::new(FrameData::UserSpeaking);
        assert!(push(&mut s, &fresh, 30, true));
        assert_eq!(s.status.activities, 0);
        assert!(!s.status.cancel_requested);
        assert_eq!(s.status.timeouts, 0);
    }
    #[test]
    fn existing_idle_key_above_cap_is_still_duplicate_not_coverage_loss() {
        let mut s = IdleState::new(
            WorkerIdleConfig {
                max_frame_observation_domains: 1,
                ..config()
            },
            10,
        );
        let f = Frame::new(FrameData::UserSpeaking);
        assert!(push(&mut s, &f, 20, true));
        assert!(push(&mut s, &f, 30, true));
        assert!(!s.status.coverage_lost);
        assert_eq!(s.status.activities, 1);
    }
    #[test]
    fn close_before_start_and_during_drain_are_final() {
        let mut s = IdleState::new(config(), 1);
        s.close();
        s.start(Instant::now());
        assert!(!s.tick(Instant::now() + Duration::from_secs(1)));
        assert!(!s.status.active);
        assert!(s.status.stopped_for_shutdown);
    }
}
