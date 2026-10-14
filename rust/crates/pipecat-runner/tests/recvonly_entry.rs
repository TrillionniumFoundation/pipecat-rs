//! Actual hosted entry witness. Native media and browser APIs are separate tests.
#![cfg(feature = "webrtc")]
#[allow(dead_code)] // Shared helper also serves exit-only tests in another harness.
#[path = "support/browser_host.rs"]
mod host;
use pipecat_browser_client::{InputMode, Transport};

#[tokio::test]
async fn ordinary_webrtc_voice_entry_remains_available() {
    let config = host::hosted(&["--webrtc"])
        .await
        .expect("owned child probe")
        .into_config()
        .expect("voice HTTP config and joined child");
    assert_eq!(config.input_mode, InputMode::Voice);
    assert_eq!(
        config.transports,
        vec![Transport::WebSocket, Transport::SmallWebRtc]
    );
    eprintln!("ordinary RTC voice entry and child SIGINT join passed");
}

#[tokio::test]
async fn explicit_text_webrtc_entry_is_available() {
    let probe = host::hosted(&["--text-input", "--webrtc"])
        .await
        .expect("owned child probe");
    if probe.config.is_err() {
        // Exact old entry refusal, after ownership is already closed. Never a pass.
        eprintln!("old text RTC rejection after reap: {probe:?}");
        assert_eq!(probe.status.and_then(|status| status.code()), Some(1));
        assert_eq!(
            probe.stderr.text,
            "--text-input supports WebSocket only; --webrtc is incompatible\n"
        );
        assert_eq!(
            probe.stderr.raw,
            b"--text-input supports WebSocket only; --webrtc is incompatible\n"
        );
        assert!(probe.stdout.raw.is_empty());
        assert!(probe.stdout.error.is_none() && probe.stderr.error.is_none());
        assert!(probe.cleanup.is_empty());
    }
    let config = probe
        .into_config()
        .expect("actual host lacks microphone-free WebRTC entry");
    assert_eq!(config.input_mode, InputMode::Text);
    assert!(!config.input_mode.captures_microphone());
    assert_eq!(
        config.transports,
        vec![Transport::WebSocket, Transport::SmallWebRtc]
    );
}
