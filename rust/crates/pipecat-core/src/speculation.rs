//! Process-local eager-turn decisions. Wire data cannot manufacture a verdict.
use std::sync::{Arc, Mutex};
use tokio::sync::watch;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeculationStatus {
    Pending,
    /// The final transcript matched; its stop boundary has not passed the LLM.
    Confirmed,
    Released,
    Cancelled,
}
struct Inner {
    state: Mutex<State>,
    changed: watch::Sender<()>,
}
struct State {
    status: SpeculationStatus,
    inference_claimed: bool,
    confirmed_context: Option<crate::LlmContextData>,
    admission: Option<(
        crate::ProcessorContext,
        crate::ProcessorHealthGeneration,
        Option<u64>,
    )>,
}
/// Read/cancel capability carried only by native frames. Only its owner can
/// confirm the transcript. Clones share cancellation and single-use inference.
#[derive(Clone)]
pub struct NativeSpeculation(Arc<Inner>);
impl std::fmt::Debug for NativeSpeculation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("NativeSpeculation")
            .field(&self.status())
            .finish()
    }
}
impl PartialEq for NativeSpeculation {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}
impl Eq for NativeSpeculation {}
/// Keep this owner until the next turn or session teardown. Dropping it closes
/// every outstanding inference/output/effect capability, including confirmed work.
pub struct SpeculationOwner(NativeSpeculation);
impl std::fmt::Debug for SpeculationOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SpeculationOwner").field(&self.0).finish()
    }
}
impl Default for SpeculationOwner {
    fn default() -> Self {
        Self::new()
    }
}
impl SpeculationOwner {
    pub fn new() -> Self {
        let (changed, _) = watch::channel(());
        Self(NativeSpeculation(Arc::new(Inner {
            state: Mutex::new(State {
                status: SpeculationStatus::Pending,
                inference_claimed: false,
                confirmed_context: None,
                admission: None,
            }),
            changed,
        })))
    }
    pub fn capability(&self) -> NativeSpeculation {
        self.0.clone()
    }
    /// Cancellation is terminal, so a delayed/duplicate confirmation cannot revive it.
    pub fn confirm(&self) -> bool {
        self.confirm_context(None)
    }
    pub fn confirm_with_context(&self, context: crate::LlmContextData) -> bool {
        self.confirm_context(Some(context))
    }
    fn confirm_context(&self, context: Option<crate::LlmContextData>) -> bool {
        let Ok(mut state) = self.0 .0.state.lock() else {
            return false;
        };
        if state.status != SpeculationStatus::Pending {
            return false;
        }
        if state
            .admission
            .as_ref()
            .is_some_and(|(context, health, epoch)| {
                context.source_is_stale(*epoch) || context.health().generation() != *health
            })
        {
            drop(state);
            self.0.cancel();
            return false;
        }
        state.confirmed_context = context;
        state.status = SpeculationStatus::Confirmed;
        drop(state);
        self.0 .0.changed.send_replace(());
        true
    }
    pub fn cancel(&self) -> bool {
        self.0.cancel()
    }
}
impl Drop for SpeculationOwner {
    fn drop(&mut self) {
        self.0.cancel();
    }
}
impl NativeSpeculation {
    pub fn status(&self) -> SpeculationStatus {
        self.0
            .state
            .lock()
            .map_or(SpeculationStatus::Cancelled, |state| state.status)
    }
    pub fn confirmed_context(&self) -> Option<crate::LlmContextData> {
        self.0.state.lock().ok()?.confirmed_context.clone()
    }
    pub fn watch(&self) -> watch::Receiver<()> {
        self.0.changed.subscribe()
    }
    fn transition(&self, from: SpeculationStatus, to: SpeculationStatus) -> bool {
        let changed = if let Ok(mut state) = self.0.state.lock() {
            if state.status != from {
                return false;
            }
            state.status = to;
            true
        } else {
            false
        };
        if changed {
            self.0.changed.send_replace(());
        }
        changed
    }
    pub fn cancel(&self) -> bool {
        let changed = if let Ok(mut state) = self.0.state.lock() {
            if state.status == SpeculationStatus::Cancelled {
                return false;
            }
            state.status = SpeculationStatus::Cancelled;
            state.admission = None;
            state.confirmed_context = None;
            true
        } else {
            false
        };
        if changed {
            self.0.changed.send_replace(());
        }
        changed
    }
    /// Called only after forwarding the matching stop boundary. An unconfirmed
    /// or cancelled capability is never opened by a speaking frame alone.
    pub fn release_after_boundary(&self) -> bool {
        self.transition(SpeculationStatus::Confirmed, SpeculationStatus::Released)
    }
    /// Exactly one context frame, including cloned/duplicated frames, may start
    /// inference. A confirmation that overtook its context remains claimable.
    pub fn claim_inference(&self) -> bool {
        let Ok(mut state) = self.0.state.lock() else {
            return false;
        };
        if state.inference_claimed || state.status == SpeculationStatus::Cancelled {
            return false;
        }
        state.inference_claimed = true;
        true
    }
    /// Bind the one admitted inference to its causal invocation and health
    /// generation. Dropped owners/cancelled turns release the retained context.
    pub fn bind_admission(
        &self,
        context: crate::ProcessorContext,
        generation: crate::ProcessorHealthGeneration,
    ) -> bool {
        let Ok(mut state) = self.0.state.lock() else {
            return false;
        };
        if !state.inference_claimed
            || state.admission.is_some()
            || state.status == SpeculationStatus::Cancelled
        {
            return false;
        }
        let epoch = context.current_generation();
        state.admission = Some((context, generation, epoch));
        true
    }
    /// Source semantics: discovering a tool before the transcript is confirmed
    /// withdraws the entire prediction, rather than parking a side effect.
    pub fn admit_tool_intent(&self) -> bool {
        let Ok(mut state) = self.0.state.lock() else {
            return false;
        };
        match state.status {
            SpeculationStatus::Pending => {
                state.status = SpeculationStatus::Cancelled;
                state.admission = None;
                drop(state);
                self.0.changed.send_replace(());
                false
            }
            SpeculationStatus::Confirmed | SpeculationStatus::Released => true,
            SpeculationStatus::Cancelled => false,
        }
    }
    /// Recheck at the handler boundary, after any queueing or task scheduling.
    pub fn admits_effect(&self) -> bool {
        let Ok(state) = self.0.state.lock() else {
            return false;
        };
        state.status == SpeculationStatus::Released
            && state
                .admission
                .as_ref()
                .is_some_and(|(context, health, epoch)| {
                    !context.source_is_stale(*epoch)
                        && context.health().generation() == *health
                        && health.snapshot.is_usable
                })
    }
    /// Fence every poll of a cancellation-safe side-effect future. Construct
    /// callbacks inside the future, not before this guard. Cancellation cannot
    /// undo a side effect whose poll was already admitted.
    pub async fn run_effect<F: std::future::Future>(&self, future: F) -> Option<F::Output> {
        let mut changed = self.watch();
        let mut health_changed = self
            .0
            .state
            .lock()
            .ok()?
            .admission
            .as_ref()?
            .0
            .health()
            .watch();
        tokio::pin!(future);
        let guarded = std::future::poll_fn(|cx| {
            if !self.admits_effect() {
                return std::task::Poll::Ready(None);
            }
            future.as_mut().poll(cx).map(Some)
        });
        tokio::pin!(guarded);
        loop {
            if !self.admits_effect() {
                return None;
            }
            tokio::select! {
                biased;
                _ = changed.changed() => {},
                _ = health_changed.changed() => {},
                result = &mut guarded => return result,
            }
        }
    }
    /// Whether ordinary held output may leave the originating inference.
    pub fn is_released(&self) -> bool {
        self.status() == SpeculationStatus::Released
    }
}
