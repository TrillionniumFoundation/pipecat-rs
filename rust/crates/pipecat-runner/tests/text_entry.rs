//! Native configuration entry; unsupported CLI mechanisms fail before sessions/providers.
use pipecat_cli::receipt_output::PlaybackProtocol;
use pipecat_runner::{NativeRunner, Options, ProviderConfig, RunnerConfig};

fn providers() -> ProviderConfig {
    ProviderConfig {
        deepgram_key: "synthetic-only".into(),
        openai_key: "synthetic-only".into(),
        cartesia_key: "synthetic-only".into(),
        deepgram_endpoint: "ws://127.0.0.1:9/unopened-stt".into(),
        openai_endpoint: "http://127.0.0.1:9/unopened-llm".into(),
        cartesia_endpoint: "ws://127.0.0.1:9/unopened-tts".into(),
        model: "synthetic".into(),
        voice_id: "synthetic".into(),
        system_prompt: "test".into(),
        input_rate: 16000,
        output_rate: 24000,
    }
}

#[tokio::test]
async fn default_generated_text_rtvi_constructor_remains_available() {
    let runner = NativeRunner::new(RunnerConfig::default(), providers()).unwrap();
    let active = runner.active_sessions();
    runner.shutdown().await;
    assert_eq!(active, 0);
    eprintln!("default RTVI constructor accepted; zero sessions; providers never started");
}

#[tokio::test]
async fn incompatible_receipt_options_fail_before_any_browser_session() {
    let result = NativeRunner::new(
        RunnerConfig {
            options: Options {
                playback_protocol: PlaybackProtocol::ChunkV2,
                ..Default::default()
            },
            ..Default::default()
        },
        providers(),
    );
    match result {
        Ok(runner) => {
            let active = runner.active_sessions();
            runner.shutdown().await;
            assert_eq!(active, 0);
            panic!("runner silently accepted CLI chunk-v2 options on the generated-text RTVI route");
        }
        Err(error) => assert_eq!(
            error.to_string(),
            "invalid voice cascade configuration: browser RTVI does not support CLI playback receipts or image channels"
        ),
    }
}

fn unsupported_options() -> Vec<Options> {
    use pipecat_cli::{config::StartupImage, receipt_output::ImageChannelConfig};
    vec![
        Options {
            playback_protocol: PlaybackProtocol::ChunkV2,
            ..Default::default()
        },
        Options {
            image_channel: Some(ImageChannelConfig::default()),
            ..Default::default()
        },
        Options {
            initial_image: Some(StartupImage::from_rgb(vec![1, 2, 3], (1, 1)).unwrap()),
            ..Default::default()
        },
        Options {
            playback_protocol: PlaybackProtocol::ChunkV2,
            image_channel: Some(ImageChannelConfig::default()),
            initial_image: Some(StartupImage::from_rgb(vec![1, 2, 3], (1, 1)).unwrap()),
            ..Default::default()
        },
    ]
}
async fn rejected(result: Result<NativeRunner, pipecat_runner::RunnerError>) {
    let error = match result {
        Ok(runner) => {
            runner.shutdown().await;
            panic!("unsupported browser configuration accepted")
        }
        Err(error) => error,
    };
    assert_eq!(error.to_string(), "invalid voice cascade configuration: browser RTVI does not support CLI playback receipts or image channels");
}
#[tokio::test]
async fn every_image_or_receipt_option_is_rejected_by_default_constructor() {
    for options in unsupported_options() {
        rejected(NativeRunner::new(
            RunnerConfig {
                options,
                ..Default::default()
            },
            providers(),
        ))
        .await;
    }
}
#[cfg(feature = "webrtc")]
#[tokio::test]
async fn webrtc_constructor_preserves_default_and_rejects_cli_mechanisms() {
    use pipecat_runner::WebRtcRunnerConfig;
    let runner = NativeRunner::new_with_webrtc(
        RunnerConfig::default(),
        providers(),
        WebRtcRunnerConfig::default(),
    )
    .unwrap();
    runner.shutdown().await;
    for options in unsupported_options() {
        rejected(NativeRunner::new_with_webrtc(
            RunnerConfig {
                options,
                ..Default::default()
            },
            providers(),
            WebRtcRunnerConfig::default(),
        ))
        .await;
    }
}
#[cfg(feature = "soniox")]
#[tokio::test]
async fn soniox_constructor_preserves_default_and_rejects_cli_mechanisms() {
    use pipecat_runner::SonioxVoiceConfig;
    let runner = NativeRunner::new_soniox(
        RunnerConfig::default(),
        SonioxVoiceConfig::new("synthetic", "synthetic"),
    )
    .unwrap();
    runner.shutdown().await;
    for options in unsupported_options() {
        rejected(NativeRunner::new_soniox(
            RunnerConfig {
                options,
                ..Default::default()
            },
            SonioxVoiceConfig::new("synthetic", "synthetic"),
        ))
        .await;
    }
}
#[cfg(feature = "whisper")]
#[tokio::test]
async fn whisper_selection_rejects_cli_mechanisms_without_loading_a_model() {
    use pipecat_runner::{LocalWhisperConfig, SttProvider, WhisperSegmentation};
    // Configuration requires an existing regular path, but construction must not
    // parse/load it as a model before any session starts. This is deliberately not a model.
    let mut local = LocalWhisperConfig::new(
        std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"),
    );
    local.segmentation = WhisperSegmentation::FixedWindow(std::time::Duration::from_secs(1));
    let runner = NativeRunner::new(
        RunnerConfig {
            stt: SttProvider::Whisper(local.clone()),
            ..Default::default()
        },
        providers(),
    )
    .unwrap();
    runner.shutdown().await;
    for options in unsupported_options() {
        rejected(NativeRunner::new(
            RunnerConfig {
                options,
                stt: SttProvider::Whisper(local.clone()),
                ..Default::default()
            },
            providers(),
        ))
        .await;
    }
}
