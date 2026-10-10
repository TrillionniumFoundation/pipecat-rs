//! Bounded source function-call moments; content capture is independently opt-in.
use crate::events::{terminal, EventState};
use crate::{SourceEvent, SourceInvalidation, SourceObserverStats};
use pipecat_core::{
    async_trait, CaptureLimits, CapturedFunctionCall, CapturedPayload, CapturedPush, Direction,
    FunctionCallPhase, FunctionCaptureOptions, ObservationEvent, ObserverCapturePolicy,
    ObserverHandle, ObserverOptions, Pipeline, PipelineObservation, PipelineObserver,
};
use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct FunctionCallObserverConfig {
    pub event_capacity: usize,
    /// At most this many pending/running calls are retained. IDs must also fit
    /// capture limits; no identity capture means no retained correlation state.
    pub max_active_calls: usize,
    /// Aggregate UTF-8 bytes in retained exact IDs, checked before cloning.
    pub max_correlation_bytes: usize,
    pub capture: FunctionCaptureOptions,
    pub capture_limits: CaptureLimits,
}
impl Default for FunctionCallObserverConfig {
    fn default() -> Self {
        Self {
            event_capacity: 256,
            max_active_calls: 256,
            max_correlation_bytes: 65_536,
            capture: FunctionCaptureOptions::default(),
            capture_limits: CaptureLimits::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct FunctionCallEvent {
    pub timestamp_ns: u64,
    pub call: CapturedFunctionCall,
    pub started_ns: Option<u64>,
    pub in_progress_ns: Option<u64>,
}
struct ActiveCall {
    started: Option<u64>,
    in_progress: Option<u64>,
}
struct State {
    events: EventState<FunctionCallEvent>,
    active: BTreeMap<String, ActiveCall>,
    identity_bytes: usize,
    timestamp: Option<u64>,
}
impl State {
    fn clear(&mut self) {
        self.active.clear();
        self.identity_bytes = 0;
        self.timestamp = None;
    }
    fn invalidate(&mut self, reason: SourceInvalidation) {
        self.clear();
        self.events.invalidate(reason);
    }
    fn close(&mut self) {
        self.clear();
        self.events.close();
    }
}
pub struct FunctionCallObserver {
    config: FunctionCallObserverConfig,
    state: Mutex<State>,
}
impl Default for FunctionCallObserver {
    fn default() -> Self {
        Self::new()
    }
}
impl FunctionCallObserver {
    pub fn new() -> Self {
        Self::with_config(FunctionCallObserverConfig::default())
            .expect("valid default function config")
    }
    pub fn with_config(config: FunctionCallObserverConfig) -> Result<Self, &'static str> {
        if !(1..=4096).contains(&config.max_active_calls) {
            return Err("active call capacity must be between 1 and 4096");
        }
        if !(1..=4_194_304).contains(&config.max_correlation_bytes) {
            return Err("correlation byte capacity must be between 1 and 4194304");
        }
        let events = EventState::new(config.event_capacity)?;
        Ok(Self {
            config,
            state: Mutex::new(State {
                events,
                active: BTreeMap::new(),
                identity_bytes: 0,
                timestamp: None,
            }),
        })
    }
    pub fn register(
        self: &Arc<Self>,
        pipeline: &mut Pipeline,
        mut options: ObserverOptions,
    ) -> Result<ObserverHandle, &'static str> {
        let mut state = self.state.lock().unwrap();
        if !state.events.fresh() {
            return Err("function observer must be fresh and registered once");
        }
        options.observe_every_push = false;
        let handle = pipeline.add_observer(self.clone(), options);
        state.events.health = Some(handle.clone());
        Ok(handle)
    }
    /// Lagged means incomplete output history. Never silently pair across it.
    pub fn subscribe(&self) -> broadcast::Receiver<SourceEvent<FunctionCallEvent>> {
        self.state.lock().unwrap().events.subscribe()
    }
    pub fn stats(&self) -> SourceObserverStats {
        self.state.lock().unwrap().events.stats()
    }
    pub fn active_calls(&self) -> usize {
        self.state.lock().unwrap().active.len()
    }
    fn observe(&self, event: ObservationEvent, capture: Option<CapturedPush>) {
        let mut state = self.state.lock().unwrap();
        let admission = state.events.admit(&event);
        if admission.invalidate {
            state.clear();
        }
        if !admission.accept {
            return;
        }
        if terminal(&event) {
            state.close();
            return;
        }
        let PipelineObservation::Push {
            first_push: true,
            timestamp_ns: at,
            direction,
            ..
        } = &*event.observation
        else {
            return;
        };
        let Some(CapturedPush {
            broadcast,
            payload: CapturedPayload::Functions(calls),
        }) = capture
        else {
            return;
        };
        if broadcast && *direction != Direction::Downstream {
            return;
        }
        if state.timestamp.is_some_and(|last| *at < last) {
            state.invalidate(SourceInvalidation::NonMonotonicTimestamp);
            return;
        }
        state.timestamp = Some(*at);
        let identities_complete = calls.omitted_calls == 0
            && calls
                .calls
                .iter()
                .all(|call| !call.tool_call_id.is_omitted() && !call.function_name.is_omitted());
        if !identities_complete {
            state.invalidate(SourceInvalidation::CaptureTruncated);
        }
        for call in calls.calls {
            let mut started_ns = None;
            let mut in_progress_ns = None;
            // A truncated batch may hide another transition of any captured ID.
            // No member of that batch can establish reliable correlation.
            if let Some(id) = call.tool_call_id.value().filter(|_| identities_complete) {
                use FunctionCallPhase::*;
                match call.phase {
                    Started | InProgress => {
                        if !state.active.contains_key(id)
                            && (state.active.len() >= self.config.max_active_calls
                                || id.len()
                                    > self.config.max_correlation_bytes - state.identity_bytes)
                        {
                            state.invalidate(SourceInvalidation::StateCapacity);
                        }
                        if id.len() > self.config.max_correlation_bytes {
                            // A single identity may fit capture but exceed the
                            // smaller retained-state budget. Keep no correlation.
                        } else if call.phase == Started {
                            if !state.active.contains_key(id) {
                                state.identity_bytes += id.len();
                            }
                            state.active.insert(
                                id.clone(),
                                ActiveCall {
                                    started: Some(*at),
                                    in_progress: None,
                                },
                            );
                        } else {
                            if !state.active.contains_key(id) {
                                state.identity_bytes += id.len();
                            }
                            let active = state.active.entry(id.clone()).or_insert(ActiveCall {
                                started: None,
                                in_progress: None,
                            });
                            started_ns = active.started.take();
                            active.in_progress = Some(*at);
                        }
                    }
                    Completed | Failed | TimedOut | Cancelled => {
                        if let Some(active) = state.active.remove(id) {
                            state.identity_bytes -= id.len();
                            in_progress_ns = active.in_progress;
                        }
                    }
                }
            }
            state.events.emit(SourceEvent::Record(FunctionCallEvent {
                timestamp_ns: *at,
                call,
                started_ns,
                in_progress_ns,
            }));
        }
    }
}
#[async_trait]
impl PipelineObserver for FunctionCallObserver {
    fn capture_policy(&self) -> ObserverCapturePolicy {
        ObserverCapturePolicy {
            functions: Some(self.config.capture),
            limits: self.config.capture_limits,
            ..Default::default()
        }
    }
    async fn on_event(&self, event: ObservationEvent) {
        self.observe(event, None);
    }
    async fn on_captured_event(&self, event: ObservationEvent, capture: CapturedPush) {
        self.observe(event, Some(capture));
    }
    fn cancel(&self) {
        self.state.lock().unwrap().close();
    }
    async fn cleanup(&self) {
        self.cancel();
    }
}
