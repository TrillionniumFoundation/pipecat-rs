//! The formal wake-notifier adapter is available through the default facade.
use pipecat::{
    core::FrameProcessor,
    processors::{
        AsyncFramePredicate, AsyncWakeNotification, FramePredicate, WakeNotifierFilter,
        WakeNotifierFilterConfig,
    },
};
use std::sync::Arc;
#[test]
fn default_facade_exposes_frame_driven_wake_notifier() {
    let selector: FramePredicate = Arc::new(|_| true);
    let filter: AsyncFramePredicate = Arc::new(|_| Box::pin(async { Ok(true) }));
    let notify: AsyncWakeNotification = Arc::new(|| Box::pin(async { Ok(()) }));
    let processor = WakeNotifierFilter::new(
        WakeNotifierFilterConfig::default(),
        selector,
        filter,
        notify,
    )
    .unwrap();
    assert_eq!(processor.name(), "WakeNotifierFilter");
}
