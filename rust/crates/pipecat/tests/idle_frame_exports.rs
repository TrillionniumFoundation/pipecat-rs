//! The generic idle monitor is available without optional features.
use pipecat::{
    core::FrameProcessor,
    processors::{IdleCallback, IdleFrameProcessor, IdleFrameProcessorConfig},
};
use std::sync::Arc;

#[test]
fn default_facade_exposes_bounded_generic_idle_monitor() {
    let callback: IdleCallback = Arc::new(|| Box::pin(async { Ok(()) }));
    let processor =
        IdleFrameProcessor::new(IdleFrameProcessorConfig::default(), None, callback).unwrap();
    assert_eq!(processor.name(), "IdleFrameProcessor");
}
