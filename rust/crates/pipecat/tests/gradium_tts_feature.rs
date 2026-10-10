#![cfg(feature = "gradium")]
#[test]
fn facade_exposes_native_gradium_stt_and_tts_without_credentials() {
    use pipecat::gradium::{GradiumTtsClient, GradiumTtsConfig, GradiumTtsProcessor};
    let config = GradiumTtsConfig::new("synthetic");
    assert_eq!(config.sample_rate, 48000);
    let processor = GradiumTtsProcessor::new(GradiumTtsClient::new(config).unwrap());
    assert_eq!(
        pipecat::core::FrameProcessor::name(&processor),
        "GradiumTTSService"
    );
    fn assert_processor<T: pipecat::core::FrameProcessor>() {}
    assert_processor::<pipecat::gradium::GradiumSttProcessor>();
}
