//! One source-bound acceptance/regression executable avoids duplicate large links.
// Standalone regression modules intentionally retain their private fixture copies.
// Keep their original imports unchanged while sharing one acceptance executable.
#![allow(clippy::duplicate_mod)]
#[path = "session_cleanup.rs"]
mod existing_cleanup;
#[path = "hosted_text_entry.rs"]
mod existing_hosted_entry;
#[path = "integration.rs"]
mod existing_integration;
#[path = "lifecycle.rs"]
mod existing_lifecycle;
#[cfg(feature = "soniox")]
#[path = "soniox_config.rs"]
mod existing_soniox_config;
#[cfg(feature = "soniox")]
#[path = "soniox_cascade.rs"]
mod existing_soniox_flow;
#[path = "text_entry.rs"]
mod existing_text_entry;
#[path = "text_flow.rs"]
mod existing_text_flow;
#[cfg(feature = "realtime")]
#[path = "realtime_voice/entry.rs"]
mod realtime_entry;
#[cfg(feature = "realtime")]
#[path = "realtime_voice/flow.rs"]
mod realtime_flow;
#[cfg(feature = "realtime")]
#[path = "realtime_voice/lifecycle.rs"]
mod realtime_lifecycle;
#[cfg(feature = "realtime")]
#[path = "support/realtime.rs"]
mod realtime_support;

#[cfg(not(feature = "realtime"))]
#[allow(dead_code)]
#[path = "support/browser_host.rs"]
mod disabled_realtime_host;
#[cfg(not(feature = "realtime"))]
#[tokio::test]
async fn realtime_selection_without_feature_fails_before_credentials() {
    let probe = disabled_realtime_host::exit_only(&["--realtime", "openai"], false)
        .await
        .unwrap();
    assert_eq!(probe.status.unwrap().code(), Some(1));
    assert!(probe.stdout.raw.is_empty());
    assert!(probe.stderr.text.contains("--features realtime"));
    assert!(probe.cleanup.is_empty());
}
