//! Scheduling isolation for same-process native loading, with strict ownership.
//!
//! Cancellation prevents admission and discards a completed result; it cannot
//! interrupt native code. Every admitted thread is actually joined before its
//! result is released. Dropping either owner uses a synchronous join fallback.
use crate::{Cancellation, EvalError};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, MutexGuard, TryLockError,
    },
    thread::{self, JoinHandle},
    time::Duration,
};
use tokio::sync::oneshot;

const JOIN_POLL_INTERVAL: Duration = Duration::from_millis(1);
const MAX_LIVE_LOADERS: usize = 16;
// At most 128 distinct settings preflights and 128 release-suite attempts.
const MAX_REGISTERED_LOADERS: usize = 256;

/// The invocation's outer owner. Unlike JoinSet cancellation, dropping this
/// registry synchronously joins native loaders even if child task destruction
/// has not yet been scheduled. Keep it alive through preflight and all jobs.
pub(crate) struct LoaderRegistry {
    shared: Arc<RegistryShared>,
}

/// Cloneable admission capability; it cannot extend the outer owner's lifetime.
#[derive(Clone)]
pub(crate) struct LoaderRegistrar {
    shared: Arc<RegistryShared>,
}

struct RegistryShared {
    state: Mutex<RegistryState>,
    live: Arc<AtomicUsize>,
}
struct RegistryState {
    closed: bool,
    entries: Vec<Arc<dyn JoinOwned>>,
}
trait JoinOwned: Send + Sync {
    fn join_and_discard(&self);
}

impl LoaderRegistry {
    pub(crate) fn new() -> Self {
        Self {
            shared: Arc::new(RegistryShared {
                state: Mutex::new(RegistryState {
                    closed: false,
                    entries: Vec::with_capacity(MAX_REGISTERED_LOADERS),
                }),
                live: Arc::new(AtomicUsize::new(0)),
            }),
        }
    }
    pub(crate) fn registrar(&self) -> LoaderRegistrar {
        LoaderRegistrar {
            shared: self.shared.clone(),
        }
    }
}
impl Drop for LoaderRegistry {
    fn drop(&mut self) {
        let entries = {
            let mut state = lock(&self.shared.state);
            state.closed = true;
            std::mem::take(&mut state.entries)
        };
        // Registration is closed before releasing the global lock. A task
        // retaining a registrar can no longer race a new thread past shutdown.
        for entry in entries {
            entry.join_and_discard();
        }
    }
}

/// Run one operation in the invocation's bounded ownership registry. The suite
/// scheduler additionally bounds loading + unclaimed + running sessions to 16.
pub(crate) async fn load_native<T, F>(
    cancel: &Cancellation,
    loaders: &LoaderRegistrar,
    load: F,
) -> Result<Option<T>, EvalError>
where
    T: Send + 'static,
    F: FnOnce() -> Result<T, EvalError> + Send + 'static,
{
    if cancel.is_cancelled() {
        return Ok(None);
    }
    // No await may separate spawning from storing both ownership references.
    let Some(owned) = OwnedNativeLoad::spawn(cancel.clone(), loaders, load)? else {
        return Ok(None);
    };
    owned.wait(cancel).await
}

struct NativeEntry<T> {
    handle: Mutex<Option<JoinHandle<Result<T, EvalError>>>>,
    live: Arc<AtomicUsize>,
}
impl<T> NativeEntry<T> {
    fn join_locked(
        &self,
        slot: &mut Option<JoinHandle<Result<T, EvalError>>>,
    ) -> Option<thread::Result<Result<T, EvalError>>> {
        let handle = slot.take()?;
        // The caller retains the entry lock through actual join. Another owner
        // cannot interpret the empty slot as a completed join prematurely.
        let result = handle.join();
        self.live.fetch_sub(1, Ordering::SeqCst);
        Some(result)
    }
}
impl<T: Send> JoinOwned for NativeEntry<T> {
    fn join_and_discard(&self) {
        let mut handle = lock(&self.handle);
        drop(self.join_locked(&mut handle));
    }
}

struct OwnedNativeLoad<T: Send> {
    entry: Arc<NativeEntry<T>>,
    completed: oneshot::Receiver<()>,
}

/// Also wakes the owner when an ordinary Rust panic unwinds the worker.
struct Completion(Option<oneshot::Sender<()>>);
impl Drop for Completion {
    fn drop(&mut self) {
        if let Some(sender) = self.0.take() {
            let _ = sender.send(());
        }
    }
}

impl<T: Send + 'static> OwnedNativeLoad<T> {
    fn spawn<F>(
        cancel: Cancellation,
        loaders: &LoaderRegistrar,
        load: F,
    ) -> Result<Option<Self>, EvalError>
    where
        F: FnOnce() -> Result<T, EvalError> + Send + 'static,
    {
        // Spawning and registering are one critical section against outer Drop.
        let mut state = lock(&loaders.shared.state);
        if state.closed || cancel.is_cancelled() {
            return Ok(None);
        }
        if state.entries.len() >= MAX_REGISTERED_LOADERS {
            return Err(EvalError::Limit(
                "native loader invocation admissions (256)",
            ));
        }
        if loaders.shared.live.load(Ordering::SeqCst) >= MAX_LIVE_LOADERS {
            return Err(EvalError::Limit("live native loaders (16)"));
        }
        let (sender, completed) = oneshot::channel();
        let entry = Arc::new(NativeEntry {
            handle: Mutex::new(None),
            live: loaders.shared.live.clone(),
        });
        let handle = thread::Builder::new()
            .name("eval-native-loader".into())
            .spawn(move || {
                let _completion = Completion(Some(sender));
                // Close the admission race before actually entering native code.
                if cancel.is_cancelled() {
                    return Err(EvalError::Cancelled);
                }
                let result = load();
                if cancel.is_cancelled() {
                    // Prefer destroying a rejected native result on this thread.
                    drop(result);
                    Err(EvalError::Cancelled)
                } else {
                    result
                }
            })
            .map_err(|_| {
                EvalError::Scenario("bot transcription native loader thread could not start".into())
            })?;
        *lock(&entry.handle) = Some(handle);
        loaders.shared.live.fetch_add(1, Ordering::SeqCst);
        state.entries.push(entry.clone());
        Ok(Some(Self { entry, completed }))
    }

    async fn wait(mut self, cancel: &Cancellation) -> Result<Option<T>, EvalError> {
        // Completion is only a hint: it can precede actual thread termination.
        // Keep the handle and unclaimed result owned, including on cancellation.
        let _ = (&mut self.completed).await;
        let joined = loop {
            // Outer Drop may concurrently be synchronously joining this entry.
            // Do not block a Tokio worker on its mutex in the normal wait path.
            let joined = {
                match self.entry.handle.try_lock() {
                    Ok(mut slot) => match slot.as_ref() {
                        None => return Ok(None), // Outer owner already joined.
                        Some(handle) if handle.is_finished() => self.entry.join_locked(&mut slot),
                        Some(_) => None,
                    },
                    Err(TryLockError::WouldBlock) => None,
                    Err(TryLockError::Poisoned(error)) => {
                        let mut slot = error.into_inner();
                        match slot.as_ref() {
                            None => return Ok(None),
                            Some(handle) if handle.is_finished() => {
                                self.entry.join_locked(&mut slot)
                            }
                            Some(_) => None,
                        }
                    }
                }
            };
            if let Some(joined) = joined {
                break joined;
            }
            tokio::time::sleep(JOIN_POLL_INTERVAL).await;
        };
        // Cancellation wins admission when completion and cancellation race.
        if cancel.is_cancelled() {
            drop(joined);
            Ok(None)
        } else {
            joined
                .map_err(|_| {
                    // Never copy a potentially sensitive native panic payload.
                    EvalError::Scenario("bot transcription native loader thread panicked".into())
                })?
                .map(Some)
        }
    }
}

impl<T: Send> Drop for OwnedNativeLoad<T> {
    fn drop(&mut self) {
        // Explicit external-drop fallback: may block without a deadline, but
        // never detaches native work. The registry retains its strong entry too.
        self.entry.join_and_discard();
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{
        atomic::{AtomicUsize, Ordering},
        mpsc, Arc,
    };

    async fn load_native<T, F>(cancel: &Cancellation, load: F) -> Result<Option<T>, EvalError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, EvalError> + Send + 'static,
    {
        let registry = LoaderRegistry::new();
        super::load_native(cancel, &registry.registrar(), load).await
    }

    struct DropCount(Arc<AtomicUsize>);
    impl Drop for DropCount {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocked_native_work_keeps_timer_and_cancel_live_until_join() {
        let cancel = Cancellation::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let result_drops = drops.clone();
        let (started, started_rx) = oneshot::channel();
        let (release, release_rx) = mpsc::channel();
        let (progress, progress_rx) = mpsc::channel();
        // A watchdog releases the native barrier even if a regression blocks
        // this current-thread runtime, so failure cannot wedge the test runner.
        let watchdog = thread::spawn(move || {
            let progressed = progress_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            let _ = release.send(());
            progressed
        });
        let mut pending = Box::pin(load_native(&cancel, move || {
            let _ = started.send(());
            release_rx.recv().expect("release native work");
            Ok(DropCount(result_drops))
        }));
        let mut early = tokio::select! {
            result = &mut pending => Some(result),
            _ = async {
                let _ = started_rx.await;
                tokio::time::sleep(Duration::from_millis(5)).await;
                cancel.cancel();
                cancel.cancelled().await;
            } => None,
        };
        if early.is_none() {
            early = tokio::select! {
                result = &mut pending => Some(result),
                _ = tokio::time::sleep(Duration::from_millis(5)) => None,
            };
        }
        let returned_before_release = early.is_some();
        let _ = progress.send(());
        let result = match early {
            Some(result) => result,
            None => pending.await,
        };
        let timer_progressed = watchdog.join().unwrap();
        assert!(timer_progressed);
        assert!(!returned_before_release);
        assert!(cancel.is_cancelled());
        assert!(matches!(result, Ok(None)));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn completion_notification_does_not_release_a_live_thread() {
        let cancel = Cancellation::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let result_drops = drops.clone();
        let (completed, completed_rx) = oneshot::channel();
        let (tail_entered, tail_rx) = oneshot::channel();
        let (release, release_rx) = mpsc::channel();
        let (progress, progress_rx) = mpsc::channel();
        let watchdog = thread::spawn(move || {
            let progressed = progress_rx.recv_timeout(Duration::from_secs(2)).is_ok();
            let _ = release.send(());
            progressed
        });
        // Model the real notification/termination gap deterministically. The
        // production wait path must not call join while this tail is blocked.
        let handle = thread::spawn(move || {
            let result = Ok(DropCount(result_drops));
            let _ = completed.send(());
            let _ = tail_entered.send(());
            release_rx.recv().expect("release thread tail");
            result
        });
        let owned = OwnedNativeLoad {
            entry: Arc::new(NativeEntry {
                handle: Mutex::new(Some(handle)),
                live: Arc::new(AtomicUsize::new(1)),
            }),
            completed: completed_rx,
        };
        let _ = tail_rx.await;
        cancel.cancel();
        // Both completion and cancellation are ready, but the native thread
        // still owns the result and must not be joined synchronously here.
        let mut pending = Box::pin(owned.wait(&cancel));
        let early = tokio::select! {
            result = &mut pending => Some(result),
            _ = tokio::time::sleep(Duration::from_millis(5)) => None,
        };
        let returned_before_release = early.is_some();
        let retained_until_release = drops.load(Ordering::SeqCst) == 0;
        let _ = progress.send(());
        let result = match early {
            Some(result) => result,
            None => pending.await,
        };
        let timer_progressed = watchdog.join().unwrap();
        assert!(timer_progressed);
        assert!(!returned_before_release);
        assert!(retained_until_release);
        assert!(matches!(result, Ok(None)));
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn precancel_does_not_invoke_native_work() {
        let cancel = Cancellation::new();
        cancel.cancel();
        let calls = Arc::new(AtomicUsize::new(0));
        let worker_calls = calls.clone();
        let result = load_native(&cancel, move || {
            worker_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        })
        .await;
        assert!(matches!(result, Ok(None)));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn success_error_and_panic_are_joined_and_reported_safely() {
        let cancel = Cancellation::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let result_drops = drops.clone();
        let value = load_native(&cancel, move || Ok(DropCount(result_drops)))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        drop(value);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        let error =
            load_native::<(), _>(&cancel, || Err(EvalError::Limit("test native limit"))).await;
        assert!(matches!(error, Err(EvalError::Limit("test native limit"))));
        let error = load_native::<(), _>(&cancel, || panic!("private panic payload")).await;
        assert!(matches!(
            error,
            Err(EvalError::Scenario(message))
                if message == "bot transcription native loader thread panicked"
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn finished_unclaimed_results_keep_all_sixteen_slots_charged() {
        let registry = LoaderRegistry::new();
        let registrar = registry.registrar();
        let cancel = Cancellation::new();
        let drops = Arc::new(AtomicUsize::new(0));
        let mut owners = Vec::new();
        for _ in 0..MAX_LIVE_LOADERS {
            let result_drops = drops.clone();
            owners.push(
                OwnedNativeLoad::spawn(cancel.clone(), &registrar, move || {
                    Ok(DropCount(result_drops))
                })
                .unwrap()
                .unwrap(),
            );
        }
        while owners.iter().any(|owner| {
            lock(&owner.entry.handle)
                .as_ref()
                .is_some_and(|handle| !handle.is_finished())
        }) {
            tokio::time::sleep(JOIN_POLL_INTERVAL).await;
        }
        assert_eq!(registry.shared.live.load(Ordering::SeqCst), 16);
        assert_eq!(drops.load(Ordering::SeqCst), 0);
        assert!(matches!(
            OwnedNativeLoad::spawn(cancel.clone(), &registrar, || Ok(())),
            Err(EvalError::Limit("live native loaders (16)"))
        ));
        // All completions are ready when cancellation arrives. No unclaimed
        // result is admitted, and joining reclaims each live slot exactly once.
        cancel.cancel();
        for owner in owners {
            assert!(matches!(owner.wait(&cancel).await, Ok(None)));
        }
        assert_eq!(drops.load(Ordering::SeqCst), 16);
        assert_eq!(registry.shared.live.load(Ordering::SeqCst), 0);
        assert!(matches!(
            super::load_native(&Cancellation::new(), &registrar, || Ok(())).await,
            Ok(Some(()))
        ));
        assert_eq!(registry.shared.live.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn cancellation_wins_over_already_completed_panic() {
        let registry = LoaderRegistry::new();
        let cancel = Cancellation::new();
        let owner = OwnedNativeLoad::spawn(cancel.clone(), &registry.registrar(), || {
            panic!("discarded native panic");
            #[allow(unreachable_code)]
            Ok(())
        })
        .unwrap()
        .unwrap();
        while lock(&owner.entry.handle)
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
        {
            tokio::time::sleep(JOIN_POLL_INTERVAL).await;
        }
        cancel.cancel();
        assert!(matches!(owner.wait(&cancel).await, Ok(None)));
        assert_eq!(registry.shared.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn registry_drop_waits_for_guard_join_and_closes_admission() {
        let registry = LoaderRegistry::new();
        let registrar = registry.registrar();
        let drops = Arc::new(AtomicUsize::new(0));
        let result_drops = drops.clone();
        let (entered, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let owned = OwnedNativeLoad::spawn(Cancellation::new(), &registrar, move || {
            let _ = entered.send(());
            release_rx.recv().expect("release competing joins");
            Ok(DropCount(result_drops))
        })
        .unwrap()
        .unwrap();
        let worker_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let entry = owned.entry.clone();
        let guard_dropper = thread::spawn(move || drop(owned));
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        let mut guard_is_joining = false;
        while std::time::Instant::now() < deadline {
            if matches!(entry.handle.try_lock(), Err(TryLockError::WouldBlock)) {
                guard_is_joining = true;
                break;
            }
            thread::sleep(JOIN_POLL_INTERVAL);
        }
        let (finished, finished_rx) = mpsc::channel();
        let registry_dropper = thread::spawn(move || {
            drop(registry);
            let _ = finished.send(());
        });
        let deadline = std::time::Instant::now() + Duration::from_secs(2);
        while !lock(&registrar.shared.state).closed && std::time::Instant::now() < deadline {
            thread::sleep(JOIN_POLL_INTERVAL);
        }
        let admission_closed = matches!(
            OwnedNativeLoad::spawn(Cancellation::new(), &registrar, || Ok(())),
            Ok(None)
        );
        let registry_is_waiting = finished_rx.recv_timeout(Duration::from_millis(20)).is_err();
        let _ = release.send(());
        let guard_join = guard_dropper.join();
        let registry_join = registry_dropper.join();
        guard_join.unwrap();
        registry_join.unwrap();
        assert!(worker_entered);
        assert!(guard_is_joining);
        assert!(admission_closed);
        assert!(registry_is_waiting);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
        assert_eq!(registrar.shared.live.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn external_drop_joins_before_returning() {
        let drops = Arc::new(AtomicUsize::new(0));
        let result_drops = drops.clone();
        let (entered, entered_rx) = mpsc::channel();
        let (dropping, dropping_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let (finished, finished_rx) = mpsc::channel();
        // Drop on a separate OS thread: synchronous fallback is explicit and
        // must never block the test controller that releases native execution.
        let dropper = thread::spawn(move || {
            let registry = LoaderRegistry::new();
            let owned =
                OwnedNativeLoad::spawn(Cancellation::new(), &registry.registrar(), move || {
                    let _ = entered.send(());
                    release_rx
                        .recv()
                        .expect("release externally dropped worker");
                    Ok(DropCount(result_drops))
                })
                .unwrap()
                .unwrap();
            let _ = dropping.send(());
            drop(owned);
            let _ = finished.send(());
        });
        let worker_entered = entered_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let drop_started = dropping_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let blocked = finished_rx.recv_timeout(Duration::from_millis(20)).is_err();
        let retained = drops.load(Ordering::SeqCst) == 0;
        let _ = release.send(());
        let finished_after_release =
            !blocked || finished_rx.recv_timeout(Duration::from_secs(2)).is_ok();
        let drop_join = dropper.join();
        drop_join.unwrap();
        assert!(worker_entered);
        assert!(drop_started);
        assert!(finished_after_release);
        assert!(blocked);
        assert!(retained);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    }
}
