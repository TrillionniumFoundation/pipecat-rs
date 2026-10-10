//! Generation-based session ownership shared by browser code and native tests.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum State {
    #[default]
    Idle,
    Connecting,
    Connected,
}

#[derive(Debug, Default)]
pub struct Lifecycle {
    pub state: State,
    pub generation: u64,
}

impl Lifecycle {
    /// Repeated Connect cannot allocate an overlapping session.
    pub fn begin(&mut self) -> Option<u64> {
        if self.state != State::Idle {
            return None;
        }
        self.generation = self.generation.wrapping_add(1);
        self.state = State::Connecting;
        Some(self.generation)
    }

    /// Every callback and asynchronous continuation must check this generation.
    pub fn accepts(&self, generation: u64) -> bool {
        self.generation == generation && self.state != State::Idle
    }

    pub fn connected(&mut self, generation: u64) -> bool {
        if !self.accepts(generation) || self.state != State::Connecting {
            return false;
        }
        self.state = State::Connected;
        true
    }

    /// Invalidate pending permission, start, offer, and transport callbacks.
    pub fn cancel(&mut self) {
        self.generation = self.generation.wrapping_add(1);
        self.state = State::Idle;
    }
}

pub fn capture_allowed(state: State, microphone: bool, buffered_bytes: u32) -> bool {
    state == State::Connected && microphone && buffered_bytes <= 256 * 1024
}

pub fn playback_allowed(now: f64, queued_until: f64, duration: f64, nodes: usize) -> bool {
    now.is_finite()
        && queued_until.is_finite()
        && duration.is_finite()
        && duration > 0.
        && queued_until.max(now + 0.01) + duration - now <= 3.
        && nodes < 256
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repeated_connect_never_overlaps() {
        let mut session = Lifecycle::default();
        let generation = session.begin().unwrap();
        assert_eq!(session.begin(), None);
        assert!(session.connected(generation));
        assert!(!session.connected(generation));
        assert_eq!(session.begin(), None);
        session.cancel();
        assert_eq!(session.state, State::Idle);
        assert!(!session.accepts(generation));
    }

    #[test]
    fn every_interrupted_stage_rejects_late_completions() {
        for stage in ["microphone", "start", "offer", "socket-open", "peer-open"] {
            let mut session = Lifecycle::default();
            let old = session.begin().unwrap();
            session.cancel();
            let fresh = session.begin().unwrap();
            assert!(!session.accepts(old), "late {stage} callback");
            assert!(!session.connected(old));
            assert_eq!(session.state, State::Connecting);
            assert!(session.connected(fresh));
            assert!(!session.accepts(old), "late {stage} error");
            assert_eq!(session.state, State::Connected);
        }
    }

    #[test]
    fn disconnect_is_idempotent_and_reconnectable() {
        let mut session = Lifecycle::default();
        for _ in 0..50 {
            let old = session.begin().unwrap();
            session.connected(old);
            session.cancel();
            session.cancel();
            assert!(!session.accepts(old));
            assert_eq!(session.state, State::Idle);
        }
    }

    #[test]
    fn microphone_capture_obeys_mute_state_and_backpressure() {
        assert!(capture_allowed(State::Connected, true, 256 * 1024));
        assert!(!capture_allowed(State::Connected, true, 256 * 1024 + 1));
        assert!(!capture_allowed(State::Connected, false, 0));
        assert!(!capture_allowed(State::Connecting, true, 0));
        assert!(!capture_allowed(State::Idle, true, 0));
    }

    #[test]
    fn playback_has_time_and_node_bounds() {
        assert!(playback_allowed(1., 1., 0.02, 0));
        assert!(playback_allowed(1., 3., 1., 255));
        assert!(!playback_allowed(1., 3., 1.01, 1));
        assert!(!playback_allowed(1., 1., 0.02, 256));
        assert!(!playback_allowed(1., 1., f64::NAN, 1));
        assert!(!playback_allowed(1., f64::INFINITY, 1., 1));
    }
}
