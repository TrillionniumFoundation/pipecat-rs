//! The standalone generated-response utility is available without optional features.
use pipecat::{
    core::{FrameProcessor, ProcessorError},
    processors::{
        FullResponseAggregatorConfig, FullResponseCallback, FullResponseCompletion,
        FullResponseStats, LlmFullResponseAggregator,
    },
};
use std::sync::Arc;

#[test]
fn full_response_default_facade_exposes_typed_owned_callback_and_bounds() {
    let callback: FullResponseCallback = Arc::new(|completion: FullResponseCompletion| {
        Box::pin(async move {
            let _correlation = (completion.session_id, completion.sequence);
            Ok::<(), ProcessorError>(())
        })
    });
    let processor =
        LlmFullResponseAggregator::new(FullResponseAggregatorConfig::default(), callback.clone())
            .unwrap();
    assert_eq!(processor.name(), "LlmFullResponseAggregator");
    let stats: FullResponseStats = processor.stats();
    assert_eq!(stats.in_flight_callbacks, 0);
    assert_eq!(stats.buffered_bytes, 0);
    let invalid = FullResponseAggregatorConfig {
        max_in_flight_callbacks: 0,
        ..Default::default()
    };
    assert!(LlmFullResponseAggregator::new(invalid, callback).is_err());
}
