//! Actual native binary/HTTP boundary, not browser execution.
#[path = "support/browser_host.rs"]
mod host;
use pipecat_browser_client::{InputMode, Transport};

#[tokio::test]
async fn default_hosted_config_overrides_assets_and_remains_voice() {
    let probe = host::hosted(&[]).await.expect("owned child probe");
    assert!(
        probe
            .config
            .as_ref()
            .is_ok_and(|(_, value)| value.get("input_mode").is_none()),
        "default configuration changed: {probe:?}"
    );
    let config = probe
        .into_config()
        .expect("default host config and joined shutdown");
    assert_eq!(config.input_mode, InputMode::Voice);
    assert_eq!(config.transports, vec![Transport::WebSocket]);
    eprintln!("default hosted voice configuration and SIGINT join passed");
}
#[tokio::test]
async fn explicit_text_input_reaches_actual_hosted_config() {
    let probe = host::hosted(&["--text-input"])
        .await
        .expect("owned child probe");
    if probe.config.is_err() {
        // This is only the honest old-code witness, never an accepted success.
        eprintln!("exact old text-input rejection after reap: {probe:?}");
        assert_eq!(probe.status.and_then(|status| status.code()), Some(1));
        assert_eq!(probe.stderr.text, "unknown argument; use --help\n");
        assert_eq!(probe.stderr.raw, b"unknown argument; use --help\n");
        assert!(probe.stdout.raw.is_empty());
        assert!(probe.stdout.error.is_none() && probe.stderr.error.is_none());
        assert!(probe.cleanup.is_empty());
    }
    let config = probe
        .into_config()
        .expect("actual binary lacks hosted text-input entry");
    assert_eq!(config.input_mode, InputMode::Text);
    assert!(!config.input_mode.captures_microphone());
    assert_eq!(config.transports, vec![Transport::WebSocket]);
}

#[tokio::test]
async fn help_remains_credential_free_and_describes_opt_in() {
    let probe = host::exit_only(&["--help"], false).await.unwrap();
    eprintln!("help child: {probe:?}");
    assert!(probe.cleanup.is_empty());
    assert!(probe.stdout.error.is_none() && probe.stderr.error.is_none());
    assert_eq!(probe.status.and_then(|s| s.code()), Some(0));
    assert!(probe.stderr.raw.is_empty());
    assert!(probe.stdout.text.contains("--text-input requires --assets"));
    assert!(probe
        .stdout
        .text
        .contains("Provider/STT credentials and initialization are unchanged"));
}

#[cfg(not(feature = "webrtc"))]
#[tokio::test]
async fn receive_only_without_feature_fails_before_credentials_or_host_start() {
    for args in [
        vec!["--text-input", "--webrtc", "--assets", "unused"],
        vec!["--webrtc", "--assets", "unused", "--text-input"],
    ] {
        let probe = host::exit_only(&args, false).await.unwrap();
        eprintln!("incompatible child: {probe:?}");
        assert!(probe.cleanup.is_empty());
        assert!(probe.stdout.error.is_none() && probe.stderr.error.is_none());
        assert_eq!(probe.status.and_then(|s| s.code()), Some(1));
        assert!(probe.stdout.raw.is_empty());
        assert_eq!(
            probe.stderr.text,
            "--webrtc requires building pipecat-runner with --features webrtc\n"
        );
    }
}

#[tokio::test]
async fn absent_assets_and_unknown_flags_are_explicit_errors() {
    for (args, error) in [
        (
            vec!["--text-input"],
            "--text-input requires --assets DIRECTORY\n",
        ),
        (vec!["--unknown"], "unknown argument; use --help\n"),
        (vec!["--assets"], "--assets requires directory\n"),
    ] {
        let probe = host::exit_only(&args, false).await.unwrap();
        eprintln!("invalid child: {probe:?}");
        assert!(probe.cleanup.is_empty());
        assert!(probe.stdout.error.is_none() && probe.stderr.error.is_none());
        assert_eq!(probe.status.and_then(|s| s.code()), Some(1));
        assert!(probe.stdout.raw.is_empty());
        assert_eq!(probe.stderr.text, error);
    }
}

#[tokio::test]
async fn invalid_asset_bundle_exits_after_owned_startup_without_ready_line() {
    let missing = std::env::temp_dir().join(format!("pipecat-missing-{}", uuid::Uuid::new_v4()));
    assert!(!missing.exists());
    let probe = host::exit_only(
        &["--text-input", "--assets", missing.to_str().unwrap()],
        true,
    )
    .await
    .unwrap();
    eprintln!("asset failure child: {probe:?}");
    assert!(probe.cleanup.is_empty());
    assert!(probe.stdout.error.is_none() && probe.stderr.error.is_none());
    assert_eq!(probe.status.and_then(|s| s.code()), Some(1));
    assert!(probe.stdout.raw.is_empty());
    assert_eq!(
        probe.stderr.text,
        "invalid or oversized browser asset bundle\n"
    );
}

#[cfg(feature = "webrtc")]
#[tokio::test]
async fn existing_webrtc_hosted_voice_selection_remains_compatible() {
    let config = host::hosted(&["--webrtc"])
        .await
        .unwrap()
        .into_config()
        .unwrap();
    assert_eq!(config.input_mode, InputMode::Voice);
    assert_eq!(
        config.transports,
        vec![Transport::WebSocket, Transport::SmallWebRtc]
    );
}

#[tokio::test]
async fn repeated_text_hosts_have_fresh_owned_lifetimes() {
    for _ in 0..2 {
        let config = host::hosted(&["--text-input"])
            .await
            .unwrap()
            .into_config()
            .unwrap();
        assert_eq!(config.input_mode, InputMode::Text);
        assert_eq!(config.transports, vec![Transport::WebSocket]);
    }
}

#[cfg(feature = "webrtc")]
#[tokio::test]
async fn explicit_receive_only_config_accepts_both_flag_orders() {
    for args in [["--text-input", "--webrtc"], ["--webrtc", "--text-input"]] {
        let config = host::hosted(&args).await.unwrap().into_config().unwrap();
        assert_eq!(config.input_mode, InputMode::Text);
        assert!(!config.input_mode.captures_microphone());
        assert_eq!(
            config.transports,
            vec![Transport::WebSocket, Transport::SmallWebRtc]
        );
    }
}
