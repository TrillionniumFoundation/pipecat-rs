//! Errors observed at their first admitted push, including recovered failures.
use crate::events::{terminal, EventState};
use crate::{SourceEvent, SourceObserverStats};
use pipecat_core::{
    async_trait, CaptureLimits, Captured, CapturedPayload, CapturedPush, ErrorCaptureOptions,
    ErrorCategory, ObservationEvent, ObservedProcessor, ObserverCapturePolicy, ObserverHandle,
    ObserverOptions, Pipeline, PipelineObservation, PipelineObserver,
};
use std::sync::{Arc, Mutex};
use tokio::sync::broadcast;

#[derive(Debug, Clone)]
pub struct ErrorObserverConfig {
    pub event_capacity: usize,
    pub capture_message: bool,
    pub capture_limits: CaptureLimits,
}
impl Default for ErrorObserverConfig {
    fn default() -> Self {
        Self {
            event_capacity: 256,
            capture_message: false,
            capture_limits: CaptureLimits::default(),
        }
    }
}
#[derive(Debug, Clone, PartialEq)]
pub struct ErrorEvent {
    pub timestamp_ns: u64,
    pub processor: ObservedProcessor,
    pub processor_usable: bool,
    pub category: ErrorCategory,
    pub fatal: bool,
    pub message: Captured<String>,
}
pub struct ErrorObserver {
    config: ErrorObserverConfig,
    state: Mutex<EventState<ErrorEvent>>,
}
impl Default for ErrorObserver {
    fn default() -> Self {
        Self::new()
    }
}
impl ErrorObserver {
    pub fn new() -> Self {
        Self::with_config(ErrorObserverConfig::default()).expect("valid default error config")
    }
    pub fn with_config(config: ErrorObserverConfig) -> Result<Self, &'static str> {
        let state = Mutex::new(EventState::new(config.event_capacity)?);
        Ok(Self { config, state })
    }
    pub fn register(
        self: &Arc<Self>,
        pipeline: &mut Pipeline,
        mut options: ObserverOptions,
    ) -> Result<ObserverHandle, &'static str> {
        let mut state = self.state.lock().unwrap();
        if !state.fresh() {
            return Err("error observer must be fresh and registered once");
        }
        options.observe_every_push = false;
        let handle = pipeline.add_observer(self.clone(), options);
        state.health = Some(handle.clone());
        Ok(handle)
    }
    /// Slow subscribers receive Lagged and must discard incomplete histories.
    pub fn subscribe(&self) -> broadcast::Receiver<SourceEvent<ErrorEvent>> {
        self.state.lock().unwrap().subscribe()
    }
    pub fn stats(&self) -> SourceObserverStats {
        self.state.lock().unwrap().stats()
    }
    fn observe(&self, event: ObservationEvent, capture: Option<CapturedPush>) {
        let mut state = self.state.lock().unwrap();
        let admission = state.admit(&event);
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
            ..
        } = &*event.observation
        else {
            return;
        };
        let Some(CapturedPush {
            payload: CapturedPayload::Error(error),
            ..
        }) = capture
        else {
            return;
        };
        state.emit(SourceEvent::Record(ErrorEvent {
            timestamp_ns: *timestamp_ns,
            processor: error.processor,
            processor_usable: error.processor_usable,
            category: error.category,
            fatal: error.fatal,
            message: error.message,
        }));
    }
}
#[async_trait]
impl PipelineObserver for ErrorObserver {
    fn capture_policy(&self) -> ObserverCapturePolicy {
        ObserverCapturePolicy {
            errors: Some(ErrorCaptureOptions {
                message: self.config.capture_message,
            }),
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
