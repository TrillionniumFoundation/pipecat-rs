//! Default facade exposes the restricted native subscription pair and core outcome.
use pipecat::{
    core::{Direction, FencedQueueOutcome, FrameProcessor},
    processors::{
        AsyncFramePredicate, ConsumerProcessor, FrameSubscriptionConfig, FrameTransformer,
        ProducerProcessor,
    },
};
use std::sync::Arc;
#[test]
fn default_facade_exposes_bounded_subscription_pair() {
    let filter: AsyncFramePredicate = Arc::new(|_| Box::pin(async { Ok(true) }));
    let transform: FrameTransformer = Arc::new(|frame| Box::pin(async { Ok(frame) }));
    let producer = ProducerProcessor::new(
        FrameSubscriptionConfig::default(),
        filter,
        Some(transform),
        true,
    )
    .unwrap();
    let consumer = ConsumerProcessor::new(producer.subscribe_handle(), None, Direction::Upstream);
    assert_eq!(producer.name(), "ProducerProcessor");
    assert_eq!(consumer.name(), "ConsumerProcessor");
    assert!(!producer.snapshot().started);
    assert!(!consumer.snapshot().registered);
    assert_ne!(FencedQueueOutcome::Queued, FencedQueueOutcome::Stale);
}
