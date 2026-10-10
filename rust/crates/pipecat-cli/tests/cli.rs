//! Exercise the actual executable with a clean environment and no credentials.
use std::process::Command;
fn command() -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_pipecat-voice-agent"));
    command.env_clear();
    command
}
#[test]
fn help_does_not_require_provider_credentials() {
    let output = command().arg("--help").output().unwrap();
    assert!(output.status.success());
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("--smoke"));
    assert!(text.contains("playback-complete"));
}
#[test]
fn missing_credentials_are_actionable_without_printing_secret_values() {
    let output = command().output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("DEEPGRAM_API_KEY is required"));
}
#[test]
fn unauthenticated_public_bind_is_rejected_before_any_network_work() {
    let output = command().args(["--bind", "0.0.0.0:7860"]).output().unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8(output.stderr)
        .unwrap()
        .contains("loopback"));
}
#[test]
fn smoke_binary_exercises_the_full_pipeline_without_credentials() {
    let output = command().arg("--smoke").output().unwrap();
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let text = String::from_utf8(output.stdout).unwrap();
    assert!(text.contains("PASS:"));
    assert!(text.contains("No paid provider calls"));
}
