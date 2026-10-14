//! Stateless transformation is available through the default native facade.
use pipecat::{
    core::{FrameProcessor, ProcessorError},
    processors::{AsyncTextTransform, StatelessTextTransformer, StatelessTextTransformerConfig},
};
use std::sync::Arc;

#[test]
fn default_facade_exposes_sync_and_async_text_transformers() {
    let config = StatelessTextTransformerConfig::default();
    let synchronous = StatelessTextTransformer::new_sync(config.clone(), |text| {
        Ok::<_, ProcessorError>(text.to_uppercase())
    })
    .unwrap();
    let callback: AsyncTextTransform = Arc::new(|text| Box::pin(async { Ok(text) }));
    let asynchronous = StatelessTextTransformer::new_async(config, callback).unwrap();
    assert_eq!(synchronous.name(), "StatelessTextTransformer");
    assert_eq!(asynchronous.name(), "StatelessTextTransformer");
}
