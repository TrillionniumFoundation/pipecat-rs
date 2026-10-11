//! Process-local eager-turn decisions. Wire data cannot manufacture a verdict.
use std::sync::{Arc, Mutex, Weak};
use tokio::sync::watch;
#[path = "eager_source.rs"]
mod eager_source;
pub use eager_source::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpeculationStatus {
    Pending,
    /// The final transcript matched; its stop boundary has not passed the LLM.
    Confirmed,
    Released,
    Cancelled,
}
struct Inner {
    // Immutable origin is outside C, so every bound observation can acquire
    // source lease L before capability C. No C-held path reacquires L.
    origin: Option<eager_source::SourceOrigin>,
    state: Mutex<State>,
    changed: Weak<watch::Sender<()>>,
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
fn admission_current(state: &State) -> bool {
    state
        .admission
        .as_ref()
        .is_none_or(|(context, health, epoch)| {
            !context.source_is_stale(*epoch) && context.health().generation() == *health
        })
}
fn bound_admission_current(state: &State) -> bool {
    state
        .admission
        .as_ref()
        .is_none_or(|(context, health, epoch)| {
            !context.source_is_stale_observed(*epoch) && context.health().generation() == *health
        })
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
pub struct SpeculationOwner {
    capability: NativeSpeculation,
    // Only the owner retains the sender. A queued capability's final Drop must
    // not close a watch channel and synchronously invoke arbitrary wakers.
    changed: Arc<watch::Sender<()>>,
}
impl std::fmt::Debug for SpeculationOwner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("SpeculationOwner")
            .field(&self.capability)
            .finish()
    }
}
impl Default for SpeculationOwner {
    fn default() -> Self {
        Self::new()
    }
}
impl SpeculationOwner {
    pub fn new() -> Self {
        Self::with_origin(None)
    }
    fn with_origin(origin: Option<eager_source::SourceOrigin>) -> Self {
        let (changed, _) = watch::channel(());
        let changed = Arc::new(changed);
        Self {
            capability: NativeSpeculation(Arc::new(Inner {
                origin,
                state: Mutex::new(State {
                    status: SpeculationStatus::Pending,
                    inference_claimed: false,
                    confirmed_context: None,
                    admission: None,
                }),
                changed: Arc::downgrade(&changed),
            })),
            changed,
        }
    }
    pub fn capability(&self) -> NativeSpeculation {
        self.capability.clone()
    }
    /// Unbound legacy confirmation. Bound owners require their exact source
    /// receipt through `confirm_source_with_context`; text equality is no grant.
    pub fn confirm(&self) -> bool {
        self.confirm_context(None)
    }
    pub fn confirm_with_context(&self, context: crate::LlmContextData) -> bool {
        self.confirm_context(Some(context))
    }
    fn confirm_context(&self, context: Option<crate::LlmContextData>) -> bool {
        if self.capability.0.origin.is_some() {
            return false;
        }
        let Ok(mut state) = self.capability.0.state.lock() else {
            return false;
        };
        if state.status != SpeculationStatus::Pending {
            return false;
        }
        if !admission_current(&state) {
            drop(state);
            self.capability.cancel();
            return false;
        }
        state.confirmed_context = context;
        state.status = SpeculationStatus::Confirmed;
        drop(state);
        self.changed.send_replace(());
        true
    }
    pub fn cancel(&self) -> bool {
        self.capability.cancel()
    }
}
impl Drop for SpeculationOwner {
    fn drop(&mut self) {
        self.capability.cancel();
    }
}
impl NativeSpeculation {
    // Private closures do bounded state transitions only. They never invoke
    // user callbacks, poll a future, route, notify, or acquire runtime admission.
    fn with_state<T>(&self, operation: impl FnOnce(&mut State, bool) -> T) -> Option<T> {
        let apply = |live| {
            let mut state = self.0.state.lock().ok()?;
            Some(operation(&mut state, live))
        };
        match &self.0.origin {
            Some(origin) => origin.with_live(apply).flatten(),
            None => apply(true),
        }
    }
    /// Effective source authority, independent of local self-cancellation.
    /// Errors use this after cancelling their own failed inference.
    pub fn source_is_current(&self) -> bool {
        self.0
            .origin
            .as_ref()
            .is_none_or(eager_source::SourceOrigin::is_live)
    }
    pub fn is_source_prediction(&self, prediction: &NativeEagerPrediction) -> bool {
        self.0
            .origin
            .as_ref()
            .is_some_and(|origin| origin.matches(prediction))
    }
    /// Check immediately before every completion poll, including FIFO waiting.
    /// The future itself must be polled only after this observation releases L/C.
    pub fn admits_completion(&self) -> bool {
        self.with_state(|state, live| live && state.status != SpeculationStatus::Cancelled)
            .unwrap_or(false)
    }
    pub fn status(&self) -> SpeculationStatus {
        self.with_state(|state, live| {
            if live {
                state.status
            } else {
                SpeculationStatus::Cancelled
            }
        })
        .unwrap_or(SpeculationStatus::Cancelled)
    }
    pub fn confirmed_context(&self) -> Option<crate::LlmContextData> {
        self.with_state(|state, live| {
            if live {
                state.confirmed_context.clone()
            } else {
                None
            }
        })
        .flatten()
    }
    pub fn watch(&self) -> watch::Receiver<()> {
        self.0.changed.upgrade().map_or_else(
            || {
                let (_, receiver) = watch::channel(());
                receiver
            },
            |changed| changed.subscribe(),
        )
    }
    fn notify(&self) {
        if let Some(changed) = self.0.changed.upgrade() {
            changed.send_replace(());
        }
    }
    pub fn cancel(&self) -> bool {
        let retired = {
            let Ok(mut state) = self.0.state.lock() else {
                return false;
            };
            if state.status == SpeculationStatus::Cancelled {
                return false;
            }
            state.status = SpeculationStatus::Cancelled;
            (state.admission.take(), state.confirmed_context.take())
        };
        // Contexts may own runtimes; release them and wake watchers outside C/L.
        drop(retired);
        self.notify();
        true
    }
    /// Called only after forwarding the matching stop boundary. An unconfirmed
    /// or cancelled capability is never opened by a speaking frame alone.
    pub fn release_after_boundary(&self) -> bool {
        let changed = self
            .with_state(|state, live| {
                if !live || state.status != SpeculationStatus::Confirmed {
                    return false;
                }
                state.status = SpeculationStatus::Released;
                true
            })
            .unwrap_or(false);
        if changed {
            self.notify();
        }
        changed
    }
    /// Exactly one context frame, including cloned/duplicated frames, may start
    /// inference. A confirmation that overtook its context remains claimable.
    pub fn claim_inference(&self) -> bool {
        self.with_state(|state, live| {
            if !live || state.inference_claimed || state.status == SpeculationStatus::Cancelled {
                return false;
            }
            state.inference_claimed = true;
            true
        })
        .unwrap_or(false)
    }
    /// Bind the one admitted inference to its causal invocation and health
    /// generation. Dropped owners/cancelled turns release the retained context.
    pub fn bind_admission(
        &self,
        context: crate::ProcessorContext,
        generation: crate::ProcessorHealthGeneration,
    ) -> bool {
        let epoch = context.current_generation();
        let mut pending = Some((context, generation, epoch));
        self.with_state(|state, live| {
            if !live
                || !state.inference_claimed
                || state.admission.is_some()
                || state.status == SpeculationStatus::Cancelled
            {
                return false;
            }
            state.admission = pending.take();
            true
        })
        .unwrap_or(false)
    }
    /// Discovering a tool before confirmation withdraws the prediction rather
    /// than parking a side effect. Source admission is checked before the intent.
    pub fn admit_tool_intent(&self) -> bool {
        let Some((admitted, changed, retired)) = self.with_state(|state, live| {
            if !live {
                return (false, false, None);
            }
            match state.status {
                SpeculationStatus::Pending => {
                    state.status = SpeculationStatus::Cancelled;
                    (false, true, state.admission.take())
                }
                SpeculationStatus::Confirmed | SpeculationStatus::Released => (true, false, None),
                SpeculationStatus::Cancelled => (false, false, None),
            }
        }) else {
            return false;
        };
        drop(retired);
        if changed {
            self.notify();
        }
        admitted
    }
    /// Recheck at the handler boundary, after any queueing or task scheduling.
    pub fn admits_effect(&self) -> bool {
        let bound = self.0.origin.is_some();
        self.with_state(|state, live| {
            live && state.status == SpeculationStatus::Released
                && state
                    .admission
                    .as_ref()
                    .is_some_and(|(_, health, _)| health.snapshot.is_usable)
                && if bound {
                    bound_admission_current(state)
                } else {
                    admission_current(state)
                }
        })
        .unwrap_or(false)
    }
    /// Fence every poll of a cancellation-safe effect. Callback construction
    /// belongs inside the future; an already admitted effect is not rolled back.
    pub async fn run_effect<F: std::future::Future>(&self, future: F) -> Option<F::Output> {
        let mut changed = self.watch();
        let mut health_changed = self
            .with_state(|state, live| {
                if !live {
                    return None;
                }
                Some(state.admission.as_ref()?.0.health().watch())
            })
            .flatten()?;
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
    pub fn is_released(&self) -> bool {
        self.status() == SpeculationStatus::Released
    }
}
