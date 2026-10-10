//! Optional public API without loading native libraries or models.
#![cfg(feature = "moonshine")]
use pipecat::moonshine::{
    Model, MoonshineProcessorOptions, MoonshineSttProcessor, SegmentationMode, SAMPLE_RATE,
};

#[test]
fn facade_exposes_native_moonshine_without_loading_artifacts() {
    let options = MoonshineProcessorOptions::default();
    assert_eq!(options.settings.model, Model::SmallStreaming);
    assert_eq!(options.settings.language.as_str(), "en");
    assert_eq!(options.segmentation, SegmentationMode::Vad);
    assert_eq!(SAMPLE_RATE, 16_000);
    assert!(std::mem::size_of::<MoonshineSttProcessor>() > 0);
}
