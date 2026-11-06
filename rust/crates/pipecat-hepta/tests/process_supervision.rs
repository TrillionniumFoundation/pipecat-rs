//! Real child-process/pipe fault tests. They prove containment on this OS, not
//! microphone permission, native model quality, audibility or remote rollback.
#![cfg(all(unix, feature = "process-supervision"))]
use pipecat_hepta::process_supervision::*;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::process::Command;

fn limits() -> MediaProcessLimits {
    MediaProcessLimits {
        startup: Duration::from_secs(2),
        shutdown: Duration::from_millis(120),
        control: Duration::from_millis(120),
        pipe_write: Duration::from_millis(100),
        reap: Duration::from_secs(2),
    }
}
fn child(script: &str) -> Command {
    let mut command = Command::new("/bin/sh");
    command.args(["-c", script]);
    command
}
const READY: &str = "printf '%s\\n' '{\"type\":\"media_process_ready\",\"connection_id\":41}'; ";
// Shell builtins only: no spawned sleep process is left behind after the kill.
const HANG: &str = "while :; do :; done";
const TERMINAL: &str = r#"{"type":"media_terminal","input_errors":0,"error":null,"local_stop_error":null,"input_error":null,"cleanup_error":null,"verification_error":null}"#;

#[tokio::test]
async fn forwards_exact_commands_and_events_but_hides_private_handshake() {
    let (input, owner) = tokio::io::duplex(1024);
    let (output, mut receiver) = tokio::io::duplex(1024);
    let terminal = format!("{},\"text\":\"中文😀\"}}", &TERMINAL[..TERMINAL.len() - 1]);
    let script = format!(
        "{READY} IFS= read -r command; printf '%s\\n' \"$command\"; printf '%s\\n' '{terminal}'"
    );
    let task = tokio::spawn(async move {
        supervise_media_process(&mut child(&script), 41, input, output, limits())
            .await
            .unwrap()
    });
    let mut owner = owner;
    owner
        .write_all(b"{\"type\": \"shutdown\"}\n{\"type\":\"interrupt\"}\n")
        .await
        .unwrap();
    let mut bytes = Vec::new();
    receiver.read_to_end(&mut bytes).await.unwrap();
    let report = task.await.unwrap();
    assert!(report.succeeded(), "{report:?}");
    assert!(report.stop_requested && report.reaped());
    assert_eq!(
        String::from_utf8(bytes).unwrap(),
        format!("{{\"type\": \"shutdown\"}}\n{terminal}\n")
    );
}

#[tokio::test]
async fn shutdown_kills_and_reaps_a_native_call_that_never_polls_tokio() {
    let script = format!("{READY}{HANG}");
    let started = Instant::now();
    let report = supervise_media_process(
        &mut child(&script),
        41,
        &b"{\"type\":\"shutdown\"}\n"[..],
        tokio::io::sink(),
        limits(),
    )
    .await
    .unwrap();
    assert!(report.ready && report.stop_requested, "{report:?}");
    assert!(report.kill_requested && report.reaped(), "{report:?}");
    assert!(!report.succeeded());
    assert!(report.error.unwrap().contains("effects remain unknown"));
    assert!(started.elapsed() < Duration::from_secs(3));
}

#[tokio::test]
async fn revoke_capture_is_a_total_deadline_without_waiting_for_ack() {
    let script = format!("{READY}{HANG}");
    let report = supervise_media_process(
        &mut child(&script),
        41,
        &b"{\"type\":\"revoke_capture\",\"request_id\":8}\n"[..],
        tokio::io::sink(),
        limits(),
    )
    .await
    .unwrap();
    assert!(report.stop_requested && report.kill_requested && report.reaped());
    assert!(!report.succeeded());
}

#[tokio::test]
async fn startup_hang_is_contained_while_owner_input_is_still_open() {
    let (input, _owner) = tokio::io::duplex(64);
    let report = supervise_media_process(
        &mut child(HANG),
        41,
        input,
        tokio::io::sink(),
        MediaProcessLimits {
            startup: Duration::from_millis(120),
            ..limits()
        },
    )
    .await
    .unwrap();
    assert!(!report.ready && !report.stop_requested);
    assert!(report.kill_requested && report.reaped());
    assert!(report.error.unwrap().contains("startup deadline"));
}

#[tokio::test]
async fn stop_during_blocking_startup_uses_the_shorter_shutdown_deadline() {
    let started = Instant::now();
    let report = supervise_media_process(
        &mut child(HANG),
        41,
        &b"{\"type\":\"shutdown\"}\n"[..],
        tokio::io::sink(),
        limits(),
    )
    .await
    .unwrap();
    assert!(!report.ready && report.stop_requested);
    assert!(report.kill_requested && report.reaped());
    assert!(report.error.unwrap().contains("shutdown deadline"));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn eof_allows_native_terminal_to_drain_without_fabricating_success() {
    let script =
        format!("{READY} while IFS= read -r command; do :; done; printf '%s\\n' '{TERMINAL}'");
    let (output, mut receiver) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        supervise_media_process(
            &mut child(&script),
            41,
            tokio::io::empty(),
            output,
            limits(),
        )
        .await
        .unwrap()
    });
    let mut bytes = Vec::new();
    receiver.read_to_end(&mut bytes).await.unwrap();
    let report = task.await.unwrap();
    assert!(report.succeeded(), "{report:?}");
    assert_eq!(bytes, format!("{TERMINAL}\n").as_bytes());
}

#[tokio::test]
async fn malformed_command_is_not_forwarded_or_resynchronized() {
    let script =
        format!("{READY} while IFS= read -r command; do printf '%s\\n' \"$command\"; done");
    let (output, mut receiver) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        supervise_media_process(
            &mut child(&script),
            41,
            &b"{\"type\":\"shutdown\",\"extra\":true}\n{\"type\":\"interrupt\"}\n"[..],
            output,
            limits(),
        )
        .await
        .unwrap()
    });
    let mut bytes = Vec::new();
    receiver.read_to_end(&mut bytes).await.unwrap();
    let report = task.await.unwrap();
    assert!(!report.succeeded() && report.reaped(), "{report:?}");
    assert!(report.error.unwrap().contains("invalid JSONL command"));
    assert!(bytes.is_empty());
}

#[tokio::test]
async fn partial_utf8_command_at_eof_is_a_terminal_failure() {
    let script = format!("{READY} while IFS= read -r command; do :; done");
    let report = supervise_media_process(
        &mut child(&script),
        41,
        &b"{\"type\":\"speak\",\"text\":\"\xe4\xb8"[..],
        tokio::io::sink(),
        limits(),
    )
    .await
    .unwrap();
    assert!(report.reaped() && !report.succeeded());
    assert!(report.error.unwrap().contains("incomplete JSONL command"));
}

#[tokio::test]
async fn wrong_connection_readiness_fails_without_exposing_child_output() {
    let script = format!(
        "printf '%s\\n' '{{\"type\":\"media_process_ready\",\"connection_id\":42}}'; {HANG}"
    );
    let (input, _owner) = tokio::io::duplex(64);
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(!report.ready && report.reaped() && !report.succeeded());
    assert!(report.error.unwrap().contains("connection mismatch"));
}

#[tokio::test]
async fn successful_exit_without_native_readiness_is_not_success() {
    let report = supervise_media_process(
        &mut child("exit 0"),
        41,
        tokio::io::empty(),
        tokio::io::sink(),
        limits(),
    )
    .await
    .unwrap();
    assert!(report.reaped());
    assert!(!report.ready && !report.succeeded());
    assert!(report.error.unwrap().contains("before readiness"));
}

#[tokio::test]
async fn stdout_eof_is_not_mistaken_for_native_thread_exit() {
    let script = format!("{READY}exec 1>&-; {HANG}");
    let (input, _owner) = tokio::io::duplex(64);
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(report.ready && report.kill_requested && report.reaped());
    assert!(!report.succeeded());
}

#[tokio::test]
async fn stalled_owner_output_does_not_leave_native_child_running() {
    let script = format!("{READY}printf '%s\\n' '{TERMINAL}'; {HANG}");
    let (input, _owner) = tokio::io::duplex(64);
    let (output, _receiver) = tokio::io::duplex(1);
    let report = supervise_media_process(&mut child(&script), 41, input, output, limits())
        .await
        .unwrap();
    assert!(report.ready && report.kill_requested && report.reaped());
    assert!(!report.succeeded());
    assert!(report.error.unwrap().contains("receiver stalled"));
}

#[tokio::test]
async fn oversized_native_event_is_rejected_without_unbounded_buffering() {
    let script = format!("{READY}printf '%s\\n' '{}'; {HANG}", "x".repeat(65_536));
    let (input, _owner) = tokio::io::duplex(64);
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(report.ready && report.reaped() && !report.succeeded());
    assert!(report.error.unwrap().contains("exceeds 64 KiB"));
}

#[tokio::test]
async fn invalid_deadlines_and_connection_are_rejected_before_spawn() {
    let mut nonexistent = Command::new("/not/an/executable");
    let error = supervise_media_process(
        &mut nonexistent,
        41,
        tokio::io::empty(),
        tokio::io::sink(),
        MediaProcessLimits {
            shutdown: Duration::ZERO,
            ..limits()
        },
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("deadlines"));
    let error = supervise_media_process(
        &mut nonexistent,
        0,
        tokio::io::empty(),
        tokio::io::sink(),
        limits(),
    )
    .await
    .unwrap_err();
    assert!(error.to_string().contains("zero owner connection"));
}

#[tokio::test]
async fn ready_then_exit_without_protocol_terminal_is_not_success() {
    let (input, _owner) = tokio::io::duplex(64);
    let report = supervise_media_process(&mut child(READY), 41, input, tokio::io::sink(), limits())
        .await
        .unwrap();
    assert!(report.ready && report.reaped());
    assert!(!report.terminal_forwarded && !report.succeeded());
    assert!(report
        .error
        .unwrap()
        .contains("without forwarding a terminal"));
}

#[tokio::test]
async fn uncorrelated_vad_event_cannot_acknowledge_a_blocked_interrupt() {
    let script = format!("{READY}printf '%s\\n' '{{\"type\":\"turn_interrupt\"}}'; {HANG}");
    let (input, mut owner) = tokio::io::duplex(64);
    owner
        .write_all(b"{\"type\":\"interrupt\"}\n")
        .await
        .unwrap();
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(report.ready && report.kill_requested && report.reaped());
    assert!(!report.succeeded());
    assert!(report.error.unwrap().contains("control deadline"));
}

#[tokio::test]
async fn retire_ack_requires_private_processed_command_sequence() {
    let script = format!("{READY}printf '%s\\n' '{{\"type\":\"media_ack\",\"request_id\":3,\"operation\":\"retire\",\"error\":null}}'; {HANG}");
    let (input, mut owner) = tokio::io::duplex(128);
    owner
        .write_all(b"{\"type\":\"retire\",\"request_id\":3,\"output_epoch\":5}\n")
        .await
        .unwrap();
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(report.kill_requested && report.reaped());
    assert!(report.error.unwrap().contains("control deadline"));
}

#[tokio::test]
async fn private_control_ack_is_removed_and_command_sequence_counts_non_controls() {
    let script = format!("{READY} IFS= read -r first; IFS= read -r second; printf '%s\\n' '{{\"type\":\"media_process_control_done\",\"command_sequence\":2}}'; IFS= read -r third; printf '%s\\n' '{TERMINAL}'");
    let turn = serde_json::json!({"type":"speak","request_id":7,"text":"fixture only","scope":{
        "principalId":"issuer", "deviceId":"device", "deviceGeneration":1,
        "organId":"organ", "organGeneration":1,"logicalSessionId":"thread:existing",
        "utteranceSequence":1,"connectionId":41,"outputEpoch":2,
        "turnId":"turn:existing","utteranceId":"utterance:existing"
    }});
    let input = format!("{turn}\n{{\"type\":\"interrupt\"}}\n{{\"type\":\"shutdown\"}}\n");
    let (output, mut receiver) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        supervise_media_process(&mut child(&script), 41, input.as_bytes(), output, limits())
            .await
            .unwrap()
    });
    let mut bytes = Vec::new();
    receiver.read_to_end(&mut bytes).await.unwrap();
    let report = task.await.unwrap();
    assert!(report.succeeded(), "{report:?}");
    assert_eq!(bytes, format!("{TERMINAL}\n").as_bytes());
}

#[tokio::test]
async fn valid_private_control_ack_is_hidden_from_the_owner() {
    let script = format!("{READY} IFS= read -r first; printf '%s\\n' '{{\"type\":\"media_process_control_done\",\"command_sequence\":1}}'; IFS= read -r second; printf '%s\\n' '{TERMINAL}'");
    let (output, mut receiver) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        supervise_media_process(
            &mut child(&script),
            41,
            &b"{\"type\":\"interrupt\"}\n{\"type\":\"shutdown\"}\n"[..],
            output,
            limits(),
        )
        .await
        .unwrap()
    });
    let mut bytes = Vec::new();
    receiver.read_to_end(&mut bytes).await.unwrap();
    let report = task.await.unwrap();
    assert!(report.succeeded(), "{report:?}");
    assert_eq!(bytes, format!("{TERMINAL}\n").as_bytes());
}

#[tokio::test]
async fn rejected_input_cause_is_retained_in_the_childs_existing_terminal() {
    let script =
        format!("{READY} while IFS= read -r command; do :; done; printf '%s\\n' '{TERMINAL}'");
    let (output, mut receiver) = tokio::io::duplex(1024);
    let task = tokio::spawn(async move {
        supervise_media_process(
            &mut child(&script),
            41,
            &b"{\"type\":\"invalid\"}\n"[..],
            output,
            limits(),
        )
        .await
        .unwrap()
    });
    let mut bytes = Vec::new();
    receiver.read_to_end(&mut bytes).await.unwrap();
    let report = task.await.unwrap();
    assert!(report.terminal_forwarded && report.reaped() && !report.succeeded());
    let terminal: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(terminal["error"], "invalid JSONL command");
}

#[tokio::test]
async fn bare_or_failed_terminal_cannot_make_exit_zero_successful() {
    for terminal in [
        "{\"type\":\"media_terminal\"}".to_string(),
        TERMINAL.replace("\"error\":null", "\"error\":\"native failed\""),
    ] {
        let script = format!("{READY}printf '%s\\n' '{terminal}'");
        let (input, _owner) = tokio::io::duplex(64);
        let report =
            supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
                .await
                .unwrap();
        assert!(report.reaped() && !report.succeeded(), "{report:?}");
        assert!(report.error.is_some());
    }
}

#[tokio::test]
async fn unsolicited_private_control_ack_is_rejected() {
    let script = format!("{READY}printf '%s\\n' '{{\"type\":\"media_process_control_done\",\"command_sequence\":99}}'; {HANG}");
    let (input, _owner) = tokio::io::duplex(64);
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(report.reaped() && !report.succeeded());
    assert!(report
        .error
        .unwrap()
        .contains("unexpected native media control"));
}

#[tokio::test]
async fn turn_that_blocks_native_retirement_is_also_contained() {
    let script = format!("{READY}{HANG}");
    let turn = serde_json::json!({"type":"turn","request_id":7,"scope":{
        "principalId":"issuer", "deviceId":"device", "deviceGeneration":1,
        "organId":"organ", "organGeneration":1,"logicalSessionId":"thread:existing",
        "utteranceSequence":1,"connectionId":41,"outputEpoch":2,
        "turnId":"turn:existing","utteranceId":"utterance:existing"
    }});
    let (input, mut owner) = tokio::io::duplex(1024);
    owner
        .write_all(format!("{turn}\n").as_bytes())
        .await
        .unwrap();
    let report =
        supervise_media_process(&mut child(&script), 41, input, tokio::io::sink(), limits())
            .await
            .unwrap();
    assert!(report.kill_requested && report.reaped());
    assert!(report.error.unwrap().contains("control deadline"));
}

#[tokio::test]
async fn timely_control_ack_survives_a_scheduling_stall_past_the_old_deadline() {
    struct StallOnce(Option<tokio::sync::oneshot::Sender<()>>);
    impl tokio::io::AsyncWrite for StallOnce {
        fn poll_write(
            mut self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
            bytes: &[u8],
        ) -> std::task::Poll<std::io::Result<usize>> {
            if let Some(done) = self.0.take() {
                // A deliberate scheduler stall after the private ACK was
                // consumed, while the event pump has not returned to select.
                std::thread::sleep(Duration::from_millis(180));
                let _ = done.send(());
            }
            std::task::Poll::Ready(Ok(bytes.len()))
        }
        fn poll_flush(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn poll_shutdown(
            self: std::pin::Pin<&mut Self>,
            _: &mut std::task::Context<'_>,
        ) -> std::task::Poll<std::io::Result<()>> {
            std::task::Poll::Ready(Ok(()))
        }
    }
    let script = format!("{READY} IFS= read -r first; printf '%s\\n' '{{\"type\":\"media_process_control_done\",\"command_sequence\":1}}'; printf '%s\\n' '{{\"type\":\"turn_interrupt\"}}'; IFS= read -r second; printf '%s\\n' '{TERMINAL}'");
    let (input, mut owner) = tokio::io::duplex(128);
    let (done, observed) = tokio::sync::oneshot::channel();
    let writer = tokio::spawn(async move {
        owner
            .write_all(b"{\"type\":\"interrupt\"}\n")
            .await
            .unwrap();
        observed.await.unwrap();
        tokio::time::sleep(Duration::from_millis(30)).await;
        owner.write_all(b"{\"type\":\"shutdown\"}\n").await.unwrap();
    });
    let report = supervise_media_process(
        &mut child(&script),
        41,
        input,
        StallOnce(Some(done)),
        MediaProcessLimits {
            control: Duration::from_millis(120),
            pipe_write: Duration::from_secs(1),
            ..limits()
        },
    )
    .await
    .unwrap();
    writer.await.unwrap();
    assert!(
        report.succeeded(),
        "timely processed control was killed by a stale timer: {report:?}"
    );
}
