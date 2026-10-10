//! Public facade APIs without credentials, network access, or native artifact loading.
// Copyright (c) 2024-2026, Daily. SPDX-License-Identifier: BSD-2-Clause

#[cfg(feature = "aws-bedrock")]
#[test]
fn bedrock_exposes_bounded_settings_and_pipeline_inference() {
    use pipecat::aws_bedrock::{BedrockLimits, BedrockProcessor, BedrockSettings};

    fn assert_inference<T: pipecat::services::LlmInference>() {}
    assert_inference::<BedrockProcessor>();
    assert!(BedrockLimits::default().queue_capacity > 0);
    assert!(matches!(
        BedrockSettings::defaults().base.model,
        pipecat::core::Setting::Value(model) if !model.is_empty()
    ));
}

#[cfg(feature = "aws-speech")]
#[test]
fn aws_speech_exposes_polly_and_transcribe_pipeline_apis() {
    use pipecat::aws_speech::{
        PartialResultsStability, PollySettings, PollyTtsProcessor, TranscribeSttProcessor,
    };

    fn assert_processor<T: pipecat::core::FrameProcessor>() {}
    assert_processor::<PollyTtsProcessor>();
    assert_processor::<TranscribeSttProcessor>();
    PollySettings::default().validate().unwrap();
    assert_eq!(PartialResultsStability::High.as_str(), "high");
}

#[cfg(feature = "moq")]
#[test]
fn moq_exposes_client_configuration_without_connecting() {
    use pipecat::moq::{MoqConfig, MoqConnection, MoqMode, TlsTrust};

    let config = MoqConfig::new("https://relay.example.invalid");
    assert_eq!(config.mode, MoqMode::Client);
    assert!(matches!(config.trust, TlsTrust::WebPki));
    assert_eq!(config.audio_out_sample_rate, 48_000);
    assert!(std::mem::size_of::<MoqConnection>() > 0);
}

#[cfg(feature = "piper")]
#[test]
fn piper_exposes_synthesis_options_and_pipeline_processor() {
    use pipecat::piper::{PiperProcessorOptions, PiperSettings, PiperTtsProcessor};

    fn assert_processor<T: pipecat::core::FrameProcessor>() {}
    assert_processor::<PiperTtsProcessor>();
    let options = PiperProcessorOptions::default();
    assert!(options.max_pending > 0);
    assert!(options.chunk_samples > 0);
    assert_eq!(PiperSettings::default().speaker_id, None);
}

#[cfg(feature = "rnnoise")]
#[test]
fn rnnoise_exposes_audio_filter_and_explicit_rate_conversion() {
    use pipecat::rnnoise::{RateConversion, RnNoiseFilter, FRAME_SAMPLES, SAMPLE_RATE};

    fn assert_filter<T: pipecat::media_transport::AudioFilter>() {}
    assert_filter::<RnNoiseFilter>();
    assert_eq!(RateConversion::default(), RateConversion::Require48Khz);
    assert_eq!(SAMPLE_RATE, 48_000);
    assert_eq!(FRAME_SAMPLES, 480);
}
