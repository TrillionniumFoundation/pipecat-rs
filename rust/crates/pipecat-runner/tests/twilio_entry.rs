use pipecat_runner::twilio::{HangupPolicy, TwilioConfig};
use std::process::Command;

#[test]
fn compiled_entry_is_disabled_and_help_does_not_require_credentials() {
    for args in [vec![], vec!["--help"], vec!["--enable"]] {
        let output = Command::new(env!("CARGO_BIN_EXE_pipecat-twilio-agent"))
            .env_clear()
            .args(&args)
            .output()
            .unwrap();
        if args == ["--help"] {
            assert!(output.status.success());
            assert!(String::from_utf8_lossy(&output.stdout).contains("Disabled by default"));
        } else {
            assert!(!output.status.success());
            let error = String::from_utf8_lossy(&output.stderr);
            assert!(error.contains(if args.is_empty() {
                "entry is disabled"
            } else {
                "missing Twilio setting"
            }));
        }
    }
    let output = Command::new(env!("CARGO_BIN_EXE_pipecat-twilio-agent"))
        .env_clear()
        .args(["--enable", "--bind", "0.0.0.0:7861"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("literal loopback"));
}
#[test]
fn compiled_entry_preserves_argument_validation_before_credentials() {
    for (args, expected) in [
        (
            vec!["--enable", "--host", "127.0.0.1"],
            "unknown Twilio argument",
        ),
        (
            vec!["--enable", "--transport", "websocket"],
            "unknown Twilio argument",
        ),
        (
            vec!["--enable", "--bind"],
            "--bind requires a literal loopback address",
        ),
        (
            vec!["--enable", "--bind", "localhost:7861"],
            "invalid socket address syntax",
        ),
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_pipecat-twilio-agent"))
            .env_clear()
            .args(args)
            .output()
            .unwrap();
        assert!(!output.status.success());
        let error = String::from_utf8_lossy(&output.stderr);
        assert!(error.contains(expected), "{error}");
        assert!(!error.contains("missing Twilio setting"));
    }
    for args in [
        vec!["-h"],
        vec!["--enable", "--help"],
        vec!["--no-intro", "--help"],
    ] {
        let output = Command::new(env!("CARGO_BIN_EXE_pipecat-twilio-agent"))
            .env_clear()
            .args(args)
            .output()
            .unwrap();
        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("Disabled by default"));
        assert!(output.stderr.is_empty());
    }
}
#[test]
fn typed_and_lookup_configuration_are_explicit_closed_and_redacted() {
    let lookup = |key: &str| match key {
        "TWILIO_ACCOUNT_SID" => Some(format!("AC{}", "0".repeat(32))),
        "TWILIO_AUTH_TOKEN" => Some("synthetic-secret-never-log".into()),
        "PIPECAT_TWILIO_WEBHOOK_URL" => Some("https://phone.example.test/twilio/voice".into()),
        "PIPECAT_TWILIO_MEDIA_URL" => Some("wss://phone.example.test/twilio/media".into()),
        _ => None,
    };
    let config = TwilioConfig::from_lookup(lookup).unwrap();
    assert_eq!(config.hangup_policy, HangupPolicy::Disabled);
    let debug = format!("{config:?}");
    assert!(!debug.contains("synthetic-secret"));
    assert!(!debug.contains(&config.account_sid));
    assert!(
        TwilioConfig::from_lookup(|key| if key == "PIPECAT_TWILIO_HANGUP" {
            Some("automatic".into())
        } else {
            lookup(key)
        })
        .is_err()
    );
    for (webhook, media) in [
        (
            "http://phone.example.test/twilio/voice",
            "wss://phone.example.test/twilio/media",
        ),
        (
            "https://phone.example.test/twilio/voice?x=1",
            "wss://phone.example.test/twilio/media",
        ),
        (
            "https://phone.example.test/twilio/voice",
            "ws://localhost/twilio/media",
        ),
        (
            "https://phone.example.test/twilio/voice",
            "wss://phone.example.test/wrong",
        ),
    ] {
        let mut candidate = config.clone();
        candidate.webhook_url = webhook.into();
        candidate.media_url = media.into();
        assert!(candidate.validate().is_err());
    }
    for max in [0, 129] {
        let mut c = config.clone();
        c.max_calls = max;
        assert!(c.validate().is_err());
    }
}
