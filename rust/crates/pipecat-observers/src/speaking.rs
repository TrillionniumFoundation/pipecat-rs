//! Acoustic speech, semantic turns, and bot speech on one monotonic timeline.
use crate::events::{terminal, EventState};
use crate::{SourceEvent, SourceInvalidation, SourceObserverStats};
use pipecat_core::{
    async_trait, CapturedPayload, CapturedPush, Direction, ObservationEvent, ObserverCapturePolicy,
    ObserverHandle, ObserverOptions, Pipeline, PipelineObservation, PipelineObserver,
    SpeechEventKind,
};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone, PartialEq)]
pub struct SpeechEvent {
    pub kind: SpeechEventKind,
    pub timestamp_ns: u64,
    pub started_ns: Option<u64>,
}
struct State {
    events: EventState<SpeechEvent>,
    open: [Option<u64>; 3],
    timestamp: Option<u64>,
}
impl State {
    fn clear(&mut self) {
        self.open = [None; 3];
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
pub struct SpeakingObserver {
    state: Mutex<State>,
}
impl Default for SpeakingObserver {
    fn default() -> Self {
        Self::new()
    }
}
impl SpeakingObserver {
    pub fn new() -> Self {
        Self::with_capacity(256).expect("valid default speaking config")
    }
    pub fn with_capacity(event_capacity: usize) -> Result<Self, &'static str> {
        Ok(Self {
            state: Mutex::new(State {
                events: EventState::new(event_capacity)?,
                open: [None; 3],
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
            return Err("speaking observer must be fresh and registered once");
        }
        options.observe_every_push = false;
        let handle = pipeline.add_observer(self.clone(), options);
        state.events.health = Some(handle.clone());
        Ok(handle)
    }
    pub fn subscribe(&self) -> broadcast::Receiver<SourceEvent<SpeechEvent>> {
        self.state.lock().unwrap().events.subscribe()
    }
    pub fn stats(&self) -> SourceObserverStats {
        self.state.lock().unwrap().events.stats()
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
            timestamp_ns,
            direction,
            ..
        } = &*event.observation
        else {
            return;
        };
        let Some(CapturedPush {
            broadcast,
            payload: CapturedPayload::Speech(speech),
        }) = capture
        else {
            return;
        };
        if broadcast && *direction != Direction::Downstream {
            return;
        }
        if state.timestamp.is_some_and(|last| *timestamp_ns < last) {
            state.invalidate(SourceInvalidation::NonMonotonicTimestamp);
            return;
        }
        state.timestamp = Some(*timestamp_ns);
        let offset = speech.confirmation_secs * 1e9;
        if !offset.is_finite() || offset < 0.0 || offset >= u64::MAX as f64 {
            state.invalidate(SourceInvalidation::InvalidVadOffset);
            return;
        }
        let Some(at) = timestamp_ns.checked_sub(offset as u64) else {
            state.invalidate(SourceInvalidation::InvalidVadOffset);
            return;
        };
        use SpeechEventKind::*;
        let layer = match speech.kind {
            UserSpeechStarted | UserSpeechStopped => Some(0),
            UserTurnStarted | UserTurnStopped => Some(1),
            BotSpeechStarted | BotSpeechStopped => Some(2),
            Interruption => None,
        };
        let mut started_ns = None;
        if let Some(layer) = layer {
            if matches!(
                speech.kind,
                UserSpeechStarted | UserTurnStarted | BotSpeechStarted
            ) {
                state.open[layer] = Some(at);
            } else {
                started_ns = state.open[layer].take();
                if started_ns.is_some_and(|start| start > at) {
                    state.invalidate(SourceInvalidation::NonMonotonicTimestamp);
                    return;
                }
            }
        }
        state.events.emit(SourceEvent::Record(SpeechEvent {
            kind: speech.kind,
            timestamp_ns: at,
            started_ns,
        }));
    }
}
#[async_trait]
impl PipelineObserver for SpeakingObserver {
    fn capture_policy(&self) -> ObserverCapturePolicy {
        ObserverCapturePolicy {
            speech: true,
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
