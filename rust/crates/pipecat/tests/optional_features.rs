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

#[cfg(feature = "classifiers")]
#[test]
fn classifiers_expose_typed_questions_and_native_inference_backends() {
    use pipecat::classifiers::{
        Classifier, ClassifierQuestion, JevClassifier, LlmClassifier, LlmClassifierConfig,
        Questions, YesNoQuestion, YesNoResult,
    };

    fn assert_classifier<T: Classifier>() {}
    assert_classifier::<LlmClassifier>();
    assert_classifier::<JevClassifier>();
    let questions = Questions::from([(
        "answered".into(),
        ClassifierQuestion::from(YesNoQuestion::new("Was the question answered?")),
    )]);
    assert!(matches!(
        questions.get("answered"),
        Some(ClassifierQuestion::YesNo(_))
    ));
    assert!(LlmClassifierConfig::default().limits.max_in_flight > 0);
    assert!(YesNoResult { probability: 0.5 }.is_yes());
}

#[cfg(feature = "deepgram-flux")]
#[test]
fn deepgram_flux_exposes_listen_v2_settings_and_pipeline_processor() {
    use pipecat::deepgram_flux::{
        FluxClient, FluxConfig, FluxModel, FluxSettings, FluxSttProcessor,
    };

    fn assert_processor<T: pipecat::core::FrameProcessor>() {}
    assert_processor::<FluxSttProcessor>();
    assert!(std::mem::size_of::<FluxClient>() > 0);
    FluxSettings::default().validate().unwrap();
    let config = FluxConfig::new(String::new());
    assert_eq!(config.settings.model, FluxModel::English);
    assert_eq!(config.settings.model.as_str(), "flux-general-en");
    assert_eq!(config.sample_rate, 16_000);
    assert!(!config.eager_end_of_turn);
    assert!(config.should_interrupt);
}
