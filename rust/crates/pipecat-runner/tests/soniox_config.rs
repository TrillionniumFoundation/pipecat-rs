#![cfg(feature = "soniox")]
use pipecat_runner::{NativeRunner, RunnerConfig, SonioxVoiceConfig, VoiceCascade};
use pipecat_services::Provider;

fn failure(config: SonioxVoiceConfig) -> String {
    match NativeRunner::new_soniox(RunnerConfig::default(), config) {
        Ok(_) => panic!("invalid config accepted"),
        Err(error) => error.to_string(),
    }
}
#[test]
fn typed_config_uses_only_selected_credentials_and_redacts_debug() {
    let config = SonioxVoiceConfig::from_lookup(|key| {
        assert!(!matches!(key, "DEEPGRAM_API_KEY" | "CARTESIA_API_KEY"));
        matches!(key, "SONIOX_API_KEY" | "OPENAI_API_KEY").then(|| "do-not-display-this-key".into())
    })
    .unwrap();
    assert!(!config.stt.vad_force_turn_endpoint);
    assert_eq!(config.stt.num_channels, 1);
    assert!(!format!("{config:?}").contains("do-not-display-this-key"));
    assert!(NativeRunner::new_soniox(RunnerConfig::default(), config).is_ok());
}
#[test]
fn exact_missing_credentials_and_unused_options_fail_without_provider_work() {
    assert_eq!(
        SonioxVoiceConfig::from_lookup(|_| None)
            .unwrap_err()
            .to_string(),
        "invalid voice cascade configuration: Soniox cascade requires SONIOX_API_KEY"
    );
    assert_eq!(
        SonioxVoiceConfig::from_lookup(|key| (key == "SONIOX_API_KEY").then(|| "synthetic".into()))
            .unwrap_err()
            .to_string(),
        "invalid voice cascade configuration: Soniox cascade requires OPENAI_API_KEY"
    );
    for key in [
        "PIPECAT_STT_PROVIDER",
        "PIPECAT_WHISPER_MODEL",
        "PIPECAT_WHISPER_SEGMENT_MS",
        "PIPECAT_WHISPER_THREADS",
        "PIPECAT_WHISPER_LANGUAGE",
        "PIPECAT_WHISPER_PRE_ROLL_MS",
        "CARTESIA_VOICE_ID",
    ] {
        assert_eq!(VoiceCascade::from_lookup(|name| if name == "PIPECAT_VOICE_CASCADE" { Some("soniox".into()) } else { (name == key).then(|| "unused".into()) }).unwrap_err().to_string(), "invalid voice cascade configuration: Soniox cascade does not accept legacy STT or Cartesia options");
    }
    for value in ["no", "-1", "0", "48001"] {
        assert_eq!(SonioxVoiceConfig::from_lookup(|key| match key { "SONIOX_API_KEY" | "OPENAI_API_KEY" => Some("synthetic".into()), "PIPECAT_INPUT_SAMPLE_RATE" => Some(value.into()), _ => None }).unwrap_err().to_string(), "invalid voice cascade configuration: sample rate must be an integer from 8000 to 48000");
    }
}
#[test]
fn provider_shape_rates_and_openai_are_validated_eagerly() {
    let base = SonioxVoiceConfig::new("synthetic", "synthetic");
    let mut config = base.clone();
    config.stt.api_key = " ".into();
    assert_eq!(
        failure(config),
        "invalid voice cascade configuration: Soniox credentials must be nonempty"
    );
    let mut config = base.clone();
    config.tts.api_key.clear();
    assert_eq!(
        failure(config),
        "invalid voice cascade configuration: Soniox credentials must be nonempty"
    );
    let mut config = base.clone();
    config.stt.vad_force_turn_endpoint = true;
    assert_eq!(failure(config), "invalid voice cascade configuration: Soniox cascade requires mono audio and provider endpoint detection");
    let mut config = base.clone();
    config.stt.num_channels = 2;
    assert_eq!(failure(config), "invalid voice cascade configuration: Soniox cascade requires mono audio and provider endpoint detection");
    let mut config = base.clone();
    config.tts.sample_rate = 22050;
    assert_eq!(
        failure(config),
        "invalid voice cascade configuration: invalid Soniox TTS configuration"
    );
    let mut config = base.clone();
    config.llm.provider = Provider::Anthropic;
    assert_eq!(failure(config), "invalid voice cascade configuration: Soniox cascade requires the native OpenAI chat provider");
    let mut config = base.clone();
    config.llm.api_key = Some(" ".into());
    assert_eq!(
        failure(config),
        "invalid voice cascade configuration: OpenAI credentials must be nonempty"
    );
    let mut config = base.clone();
    config.llm.endpoint = "http://remote.invalid/chat".into();
    assert_eq!(
        failure(config),
        "invalid voice cascade configuration: invalid OpenAI request configuration"
    );
    let mut config = base.clone();
    config.llm.request_timeout = std::time::Duration::ZERO;
    assert_eq!(
        failure(config),
        "invalid voice cascade configuration: invalid OpenAI client configuration"
    );
    let mut config = base.clone();
    config.model.clear();
    assert_eq!(failure(config), "invalid voice cascade configuration: OpenAI model and system prompt must be nonempty and bounded");
    let runner = RunnerConfig {
        options: pipecat_runner::Options {
            vad: Some(pipecat_cli::config::VadPaths {
                model: "not-opened".into(),
                runtime: "not-opened".into(),
            }),
            ..Default::default()
        },
        ..Default::default()
    };
    let error = NativeRunner::new_soniox(runner, base).err().unwrap();
    assert_eq!(error.to_string(), "invalid voice cascade configuration: Soniox cascade uses provider endpoint detection; local STT and VAD are unsupported");
}
