#![cfg(feature = "images")]
//! Public facade wiring only; network and RGB delivery live in images tests.
use pipecat::{
    core::FrameProcessor,
    images::{AzureImageClient, AzureImageConfig, ImageGenProcessor},
};
use std::time::Duration;

#[tokio::test]
async fn azure_public_facade_preserves_source_defaults_and_distinct_service_name() {
    let mut config = AzureImageConfig::new("synthetic-fixture-key", "http://127.0.0.1:1/");
    assert_eq!(config.api_version, "2023-06-01-preview");
    assert_eq!(config.settings.model, None);
    assert_eq!(config.settings.image_size, None);
    assert_eq!(config.poll_interval, Duration::from_secs(1));
    assert_eq!(config.max_polls, 119);
    config.allow_loopback_api = true;
    let processor = ImageGenProcessor::azure(AzureImageClient::new(config).unwrap());
    assert_eq!(processor.name(), "AzureImageGenServiceREST");
    assert_eq!(processor.settings().unwrap().model, None);
}
