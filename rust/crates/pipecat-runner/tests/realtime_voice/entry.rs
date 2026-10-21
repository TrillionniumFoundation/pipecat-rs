use pipecat_runner::{NativeRunner, OpenAiRealtimeVoiceConfig, RunnerConfig};
#[allow(dead_code)]
#[path = "../support/browser_host.rs"]
mod host;

#[test]
fn selected_environment_reads_only_openai_credential_and_redacts_debug() {
    let config = OpenAiRealtimeVoiceConfig::from_lookup(|key| {
        assert!(!matches!(
            key,
            "DEEPGRAM_API_KEY" | "CARTESIA_API_KEY" | "SONIOX_API_KEY"
        ));
        (key == "OPENAI_API_KEY").then(|| "dummy-secret-not-debug".into())
    })
    .unwrap();
    assert!(!format!("{config:?}").contains("dummy-secret"));
    assert!(OpenAiRealtimeVoiceConfig::from_lookup(|_| None)
        .unwrap_err()
        .to_string()
        .contains("OPENAI_API_KEY"));
}
#[test]
fn contradictory_configuration_is_rejected_before_constructing_clients() {
    for key in [
        "PIPECAT_VOICE_CASCADE",
        "PIPECAT_STT_PROVIDER",
        "PIPECAT_WHISPER_MODEL",
        "PIPECAT_INPUT_SAMPLE_RATE",
        "PIPECAT_OUTPUT_SAMPLE_RATE",
        "SONIOX_VOICE",
        "CARTESIA_VOICE_ID",
        "OPENAI_MODEL",
    ] {
        let result = OpenAiRealtimeVoiceConfig::from_lookup(|name| {
            if name == key {
                Some("contradiction".into())
            } else if name == "OPENAI_API_KEY" {
                panic!("credentials read before contradictory settings rejected")
            } else {
                None
            }
        });
        assert!(result.is_err());
    }
    let mut config = OpenAiRealtimeVoiceConfig::new("dummy");
    config.endpoint = "ws://example.invalid/realtime".into();
    assert!(config.validate().is_err());
    config.endpoint = "ws://127.0.0.1:9/realtime".into();
    config.limits.queue_capacity = 1000;
    assert!(config.validate().is_err());
}
#[test]
fn typed_constructor_rejects_local_vad_and_cli_images() {
    let mut runner = RunnerConfig::default();
    runner.options.vad = Some(pipecat_cli::config::VadPaths {
        model: "unused".into(),
        runtime: "unused".into(),
    });
    assert!(
        NativeRunner::new_openai_realtime(runner, OpenAiRealtimeVoiceConfig::new("dummy")).is_err()
    );
    let mut runner = RunnerConfig::default();
    runner.options.image_channel = Some(Default::default());
    assert!(
        NativeRunner::new_openai_realtime(runner, OpenAiRealtimeVoiceConfig::new("dummy")).is_err()
    );
}
#[tokio::test]
async fn compiled_cli_help_and_configuration_errors_do_not_listen_or_connect() {
    let help = host::exit_only(&["--help"], false).await.unwrap();
    assert_eq!(help.status.unwrap().code(), Some(0));
    assert!(help.stdout.text.contains("--realtime openai"));
    assert!(help.stdout.text.contains("only OPENAI_API_KEY"));
    for args in [
        vec!["--realtime"],
        vec!["--realtime", "gemini"],
        vec!["--realtime", "openai", "--realtime", "openai"],
        vec![
            "--realtime",
            "openai",
            "--vad-model",
            "unused",
            "--onnx-runtime",
            "unused",
        ],
    ] {
        let probe = host::exit_only(&args, false).await.unwrap();
        assert_eq!(probe.status.unwrap().code(), Some(1));
        assert!(probe.stdout.raw.is_empty());
        assert!(probe.cleanup.is_empty());
    }
    let output = host::exit_only(&["--realtime", "openai"], false)
        .await
        .unwrap();
    assert_eq!(output.status.unwrap().code(), Some(1));
    assert!(output.stderr.text.contains("OPENAI_API_KEY"));
    assert!(output.stdout.raw.is_empty());
    assert!(output.cleanup.is_empty());
}
#[tokio::test]
async fn compiled_cli_openai_only_config_and_sigint_release_listener() {
    for args in [
        &["--realtime", "openai"][..],
        &["--realtime", "openai", "--text-input"][..],
    ] {
        let probe = host::hosted_with_env(args, &[("OPENAI_API_KEY", "dummy-no-provider-session")])
            .await
            .unwrap();
        let (_, value) = probe.config.as_ref().unwrap();
        assert_eq!(value["voice_mode"], "openai-realtime");
        assert_eq!(value["input_sample_rate"], 24000);
        assert_eq!(value["output_sample_rate"], 24000);
        assert_eq!(value["playback_acknowledgements"], false);
        assert!(probe.cleanup.is_empty());
        let config = probe.into_config().unwrap();
        assert_eq!(
            config.transports,
            vec![pipecat_browser_client::Transport::WebSocket]
        );
        assert_eq!(
            config.input_mode.captures_microphone(),
            !args.contains(&"--text-input")
        );
    }
}
