use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerReady {
    pub worker_name: String,
    pub runner: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerRegistration {
    pub ready: WorkerReady,
    pub parent: Option<String>,
    pub active: bool,
    pub bridged: bool,
}
impl From<WorkerReady> for WorkerRegistration {
    fn from(ready: WorkerReady) -> Self {
        Self {
            ready,
            parent: None,
            active: false,
            bridged: false,
        }
    }
}
/// Shared discovery with local lookup precedence and atomic readiness watches.
/// Network snapshots replace one remote runner only. Imported changes never
/// increment the local revision used by outgoing registry synchronization.
#[derive(Clone)]
pub struct WorkerRegistry {
    runner: String,
    inner: Arc<Mutex<RegistryState>>,
    local_revision: watch::Sender<u64>,
}
#[derive(Default)]
struct RegistryState {
    workers: BTreeMap<String, BTreeMap<String, WorkerRegistration>>,
    watches: BTreeMap<String, watch::Sender<Option<WorkerReady>>>,
    remote_claims: BTreeMap<(String, String), u64>,
    next_claim: u64,
}
/// Opaque ownership of one remote worker name, including empty discovery snapshots.
/// Any external replacement or removal invalidates the generation permanently.
#[derive(Clone)]
pub struct RemoteEntryClaim {
    registry: Weak<Mutex<RegistryState>>,
    runner: String,
    name: String,
    generation: u64,
}
impl WorkerRegistry {
    pub fn new(runner: impl Into<String>) -> Self {
        Self {
            runner: runner.into(),
            inner: Arc::new(Mutex::new(RegistryState::default())),
            local_revision: watch::channel(0).0,
        }
    }
    pub fn runner_name(&self) -> &str {
        &self.runner
    }
    fn lookup(&self, state: &RegistryState, name: &str) -> Option<WorkerReady> {
        state
            .workers
            .get(&self.runner)
            .and_then(|r| r.get(name))
            .or_else(|| state.workers.values().find_map(|r| r.get(name)))
            .map(|entry| entry.ready.clone())
    }
    fn notify(&self, state: &mut RegistryState) {
        state
            .watches
            .retain(|_, sender| sender.receiver_count() > 0);
        for (name, sender) in &state.watches {
            let selected = self.lookup(state, name);
            sender.send_if_modified(|old| {
                if *old == selected {
                    false
                } else {
                    *old = selected;
                    true
                }
            });
        }
    }
    fn changed_local(&self) {
        self.local_revision
            .send_modify(|revision| *revision = revision.wrapping_add(1));
    }
    pub fn local_changes(&self) -> watch::Receiver<u64> {
        self.local_revision.subscribe()
    }
    pub fn register(&self, worker: WorkerReady) -> bool {
        self.register_entry(worker.into())
    }
    pub fn register_entry(&self, entry: WorkerRegistration) -> bool {
        let mut state = self.inner.lock().expect("registry lock poisoned");
        let runner = entry.ready.runner.clone();
        let target = state.workers.entry(runner.clone()).or_default();
        if target.contains_key(&entry.ready.worker_name) {
            return false;
        }
        let name = entry.ready.worker_name.clone();
        target.insert(name.clone(), entry);
        state.remote_claims.remove(&(runner.clone(), name));
        self.notify(&mut state);
        if runner == self.runner {
            self.changed_local();
        }
        true
    }
    pub fn set_active(&self, name: &str, active: bool) {
        let mut state = self.inner.lock().expect("registry lock poisoned");
        if let Some(entry) = state
            .workers
            .get_mut(&self.runner)
            .and_then(|r| r.get_mut(name))
        {
            if entry.active != active {
                entry.active = active;
                self.changed_local();
            }
        }
    }
    pub fn unregister(&self, name: &str, runner: &str) -> bool {
        let mut state = self.inner.lock().expect("registry lock poisoned");
        state
            .remote_claims
            .remove(&(runner.to_owned(), name.to_owned()));
        let removed = state
            .workers
            .get_mut(runner)
            .and_then(|r| r.remove(name))
            .is_some();
        state.workers.retain(|_, entries| !entries.is_empty());
        self.notify(&mut state);
        if removed && runner == self.runner {
            self.changed_local();
        }
        removed
    }
    /// Replace an authoritative remote snapshot. Local entries cannot be replaced
    /// through this API. Callers enforce peer and entry-count limits at admission.
    pub fn replace_remote(&self, runner: &str, workers: Vec<WorkerRegistration>) -> bool {
        if runner == self.runner || workers.iter().any(|entry| entry.ready.runner != runner) {
            return false;
        }
        let mut state = self.inner.lock().expect("registry lock poisoned");
        let entries = workers
            .into_iter()
            .map(|entry| (entry.ready.worker_name.clone(), entry))
            .collect();
        state.remote_claims.retain(|(owner, _), _| owner != runner);
        state.workers.insert(runner.to_owned(), entries);
        state.workers.retain(|_, entries| !entries.is_empty());
        self.notify(&mut state);
        true
    }
    /// Claim an absent remote worker name. The claim remains exclusive while its
    /// entry is absent, so a readiness removal/reappearance cannot transfer ownership.
    /// Returns None for local names, conflicting claims, existing entries or invalid metadata.
    pub fn claim_remote_entry(
        &self,
        runner: &str,
        name: &str,
        entry: Option<WorkerRegistration>,
    ) -> Option<RemoteEntryClaim> {
        if !self.valid_remote_entry(runner, name, entry.as_ref()) {
            return None;
        }
        let mut state = self.inner.lock().expect("registry lock poisoned");
        let key = (runner.to_owned(), name.to_owned());
        if state.remote_claims.contains_key(&key)
            || state
                .workers
                .get(runner)
                .is_some_and(|entries| entries.contains_key(name))
        {
            return None;
        }
        let generation = state.next_claim.checked_add(1)?;
        state.next_claim = generation;
        state.remote_claims.insert(key, generation);
        if let Some(entry) = entry {
            state
                .workers
                .entry(runner.to_owned())
                .or_default()
                .insert(name.to_owned(), entry);
        }
        self.notify(&mut state);
        Some(RemoteEntryClaim {
            registry: Arc::downgrade(&self.inner),
            runner: runner.to_owned(),
            name: name.to_owned(),
            generation,
        })
    }
    /// Update only the generation named by an opaque claim. Other entries and
    /// local discovery revisions are untouched. Invalidated claims cannot be revived.
    pub fn update_remote_entry(
        &self,
        claim: &RemoteEntryClaim,
        entry: Option<WorkerRegistration>,
    ) -> bool {
        if !Weak::ptr_eq(&claim.registry, &Arc::downgrade(&self.inner))
            || !self.valid_remote_entry(&claim.runner, &claim.name, entry.as_ref())
        {
            return false;
        }
        let mut state = self.inner.lock().expect("registry lock poisoned");
        let key = (claim.runner.clone(), claim.name.clone());
        if state.remote_claims.get(&key) != Some(&claim.generation) {
            return false;
        }
        if let Some(entry) = entry {
            state
                .workers
                .entry(claim.runner.clone())
                .or_default()
                .insert(claim.name.clone(), entry);
        } else if let Some(entries) = state.workers.get_mut(&claim.runner) {
            entries.remove(&claim.name);
        }
        state.workers.retain(|_, entries| !entries.is_empty());
        self.notify(&mut state);
        true
    }
    /// Release a claim and its current entry. An old generation cannot remove a
    /// newer owner's identical registration, including after full-runner replacement.
    pub fn release_remote_entry(&self, claim: RemoteEntryClaim) -> bool {
        if !Weak::ptr_eq(&claim.registry, &Arc::downgrade(&self.inner)) {
            return false;
        }
        let mut state = self.inner.lock().expect("registry lock poisoned");
        let key = (claim.runner.clone(), claim.name.clone());
        if state.remote_claims.get(&key) != Some(&claim.generation) {
            return false;
        }
        state.remote_claims.remove(&key);
        if let Some(entries) = state.workers.get_mut(&claim.runner) {
            entries.remove(&claim.name);
        }
        state.workers.retain(|_, entries| !entries.is_empty());
        self.notify(&mut state);
        true
    }
    fn valid_remote_entry(
        &self,
        runner: &str,
        name: &str,
        entry: Option<&WorkerRegistration>,
    ) -> bool {
        runner != self.runner
            && !runner.is_empty()
            && !name.is_empty()
            && entry
                .is_none_or(|entry| entry.ready.runner == runner && entry.ready.worker_name == name)
    }
    pub fn remove_runner(&self, runner: &str) -> bool {
        if runner == self.runner {
            return false;
        }
        let mut state = self.inner.lock().expect("registry lock poisoned");
        state.remote_claims.retain(|(owner, _), _| owner != runner);
        let removed = state.workers.remove(runner).is_some();
        self.notify(&mut state);
        removed
    }
    pub fn entries(&self, runner: &str) -> Vec<WorkerRegistration> {
        self.inner
            .lock()
            .expect("registry lock poisoned")
            .workers
            .get(runner)
            .map(|r| r.values().cloned().collect())
            .unwrap_or_default()
    }
    pub fn get(&self, name: &str) -> Option<WorkerReady> {
        self.lookup(&self.inner.lock().expect("registry lock poisoned"), name)
    }
    pub fn local_workers(&self) -> Vec<String> {
        self.entries(&self.runner)
            .into_iter()
            .map(|entry| entry.ready.worker_name)
            .collect()
    }
    pub fn remote_workers(&self) -> Vec<String> {
        self.inner
            .lock()
            .expect("registry lock poisoned")
            .workers
            .iter()
            .filter(|(runner, _)| **runner != self.runner)
            .flat_map(|(_, r)| r.keys().cloned())
            .collect()
    }
    /// Subscribes atomically with initial lookup; registration cannot be missed.
    pub fn watch(&self, name: impl Into<String>) -> watch::Receiver<Option<WorkerReady>> {
        let name = name.into();
        let mut state = self.inner.lock().expect("registry lock poisoned");
        state
            .watches
            .retain(|_, sender| sender.receiver_count() > 0);
        let current = self.lookup(&state, &name);
        state
            .watches
            .entry(name)
            .or_insert_with(|| watch::channel(current).0)
            .subscribe()
    }
    pub async fn wait_ready(&self, name: &str) -> WorkerReady {
        let mut rx = self.watch(name);
        loop {
            if let Some(value) = rx.borrow_and_update().clone() {
                return value;
            }
            if rx.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }
}
