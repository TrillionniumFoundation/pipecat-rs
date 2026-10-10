use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc,
};

/// SDK shutdown reports task termination, even when a transport's cleanup
/// failed. Keep that outcome separately so close() cannot report false success.
#[derive(Clone, Default)]
pub(crate) struct CleanupStatus(Arc<AtomicU8>);
impl CleanupStatus {
    pub(crate) fn begin(&self) -> CleanupAttempt {
        self.0.store(1, Ordering::Release);
        CleanupAttempt {
            status: self.clone(),
            complete: false,
        }
    }
    pub(crate) fn failed(&self) -> bool {
        matches!(self.0.load(Ordering::Acquire), 1 | 3)
    }
}
pub(crate) struct CleanupAttempt {
    status: CleanupStatus,
    complete: bool,
}
impl CleanupAttempt {
    pub(crate) fn success(mut self) {
        self.status.0.store(2, Ordering::Release);
        self.complete = true;
    }
}
impl Drop for CleanupAttempt {
    fn drop(&mut self) {
        if !self.complete {
            self.status.0.store(3, Ordering::Release);
        }
    }
}
