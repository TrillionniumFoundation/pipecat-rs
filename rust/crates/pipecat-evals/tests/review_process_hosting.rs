// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
#![cfg(feature = "process-fixtures")]

use pipecat_evals::{
    process::{run_hosted, HostedResult, NativeCommand, ProcessOptions},
    release::{run_release, ReleaseManifest},
    serializer::{decode_control, EvalControl},
    Cancellation, ScenarioFile, ScriptScenario, SessionOptions,
};
use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
use serde_json::json;
use std::{
    collections::BTreeMap,
    io::Write,
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "pipecat-process-review-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}
#[path = "support/ports.rs"]
mod fixture_ports;
use fixture_ports::port;

fn scenario() -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"review","turns":[
        {"user":"hello","expect":[{"event":"response","text_contains":"hello"}]}]
    }))
    .unwrap()
    .scenarios
    .remove(0)
}
fn options() -> (SessionOptions, ProcessOptions) {
    let session = SessionOptions {
        transport: SessionConfig {
            connect_timeout: Duration::from_millis(100),
            write_timeout: Duration::from_millis(100),
            close_timeout: Duration::from_millis(50),
            ..Default::default()
        },
        handshake_timeout: Duration::from_secs(1),
        max_duration: Duration::from_secs(2),
        ..Default::default()
    };
    let process = ProcessOptions {
        startup_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_millis(300),
        output_drain_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    (session, process)
}
fn child_command(dir: &Path, mode: &str, port: u16) -> NativeCommand {
    NativeCommand {
        executable: std::env::current_exe().unwrap(),
        args: vec![
            "--exact".into(),
            "native_review_fixture".into(),
            "--nocapture".into(),
            "--test-threads=1".into(),
        ],
        cwd: dir.into(),
        env: BTreeMap::from([
            ("REVIEW_CHILD_MODE".into(), mode.into()),
            ("REVIEW_CHILD_PORT".into(), port.to_string()),
            (
                "REVIEW_CHILD_DIR".into(),
                dir.to_string_lossy().into_owned(),
            ),
        ]),
    }
}
async fn run_command(command: NativeCommand, port: u16, process: ProcessOptions) -> HostedResult {
    let (session, _) = options();
    let result = run_hosted(
        command,
        scenario(),
        format!("ws://127.0.0.1:{port}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(result.process.reaped, "{result:?}");
    #[cfg(target_os = "linux")]
    assert!(!Path::new(&format!("/proc/{}", result.process.pid.unwrap())).exists());
    result
}

// Run this test binary itself as a native child. No shell, interpreter or extra
// executable target is needed to emit bytes that cannot be represented in JSON.
#[test]
fn native_review_fixture() {
    let Ok(mode) = std::env::var("REVIEW_CHILD_MODE") else {
        return;
    };
    match mode.as_str() {
        "mask" => std::io::stderr().write_all(b"xE").unwrap(),
        "mask-overflow" => std::io::stderr().write_all(&[b'x'; 100]).unwrap(),
        "invalid" => std::io::stderr().write_all(&[0xff, 0xfe, b'!']).unwrap(),
        "invalid-overflow" => std::io::stderr().write_all(&[0xff; 100]).unwrap(),
        "overlap" => {
            std::io::stderr()
                .write_all(b"abcd abc bc [REDACTED] ")
                .unwrap();
            for byte in "密钥🔑".as_bytes() {
                std::io::stderr().write_all(&[*byte]).unwrap();
                std::io::stderr().flush().unwrap();
                std::thread::sleep(Duration::from_millis(2));
            }
        }
        "disconnect-handshake" => {
            tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    let port: u16 = std::env::var("REVIEW_CHILD_PORT").unwrap().parse().unwrap();
                    let dir = PathBuf::from(std::env::var("REVIEW_CHILD_DIR").unwrap());
                    let listener =
                        tokio::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port))
                            .await
                            .unwrap();
                    let mut count = 0;
                    loop {
                        let accepted = if count == 0 {
                            tokio::time::timeout(Duration::from_secs(1), listener.accept()).await
                        } else {
                            tokio::time::timeout(Duration::from_millis(180), listener.accept())
                                .await
                        };
                        let Ok(Ok((stream, _))) = accepted else {
                            break;
                        };
                        count += 1;
                        std::fs::write(dir.join("connections"), count.to_string()).unwrap();
                        let mut socket = WebSocketSession::accept(
                            stream,
                            SessionConfig {
                                close_timeout: Duration::from_millis(20),
                                ..Default::default()
                            },
                        )
                        .await
                        .unwrap();
                        assert!(socket.recv().await.is_some());
                        let _ = socket.cancel().await;
                    }
                });
        }
        _ => panic!("unknown fixture mode"),
    }
    std::io::stderr().flush().unwrap();
    std::process::exit(0);
}

#[tokio::test(flavor = "current_thread")]
async fn replacement_markers_are_not_redacted_recursively() {
    let dir = Temp::new();
    let p = port();
    let mut command = child_command(&dir.0, "mask", p);
    command.env.insert("A_SECRET".into(), "x".into());
    command.env.insert("B_SECRET".into(), "E".into());
    let result = run_command(command, p, options().1).await;
    assert_eq!(result.process.stderr, "[REDACTED][REDACTED]");
    assert!(!result.process.output_overflow);
}

#[tokio::test(flavor = "current_thread")]
async fn longest_exact_secrets_and_split_utf8_are_masked_once() {
    let dir = Temp::new();
    let p = port();
    let mut command = child_command(&dir.0, "overlap", p);
    for (i, value) in ["abc", "abcd", "bc", "[REDACTED]", "密钥🔑"]
        .iter()
        .enumerate()
    {
        command.env.insert(format!("SECRET_{i}"), (*value).into());
    }
    let result = run_command(command, p, options().1).await;
    assert_eq!(
        result.process.stderr,
        "[REDACTED] [REDACTED] [REDACTED] [REDACTED] [REDACTED]"
    );
    assert!(!result.process.output_overflow);
    assert!(!result.process.output_incomplete);
}

#[tokio::test(flavor = "current_thread")]
async fn redacted_and_lossy_utf8_output_obey_the_stream_budget() {
    for mode in ["mask-overflow", "invalid-overflow"] {
        let dir = Temp::new();
        let p = port();
        let mut command = child_command(&dir.0, mode, p);
        command.env.insert("SECRET".into(), "x".into());
        let mut process = options().1;
        process.max_stderr_bytes = 128;
        let result = run_command(command, p, process).await;
        assert!(result.process.output_overflow, "{mode}: {result:?}");
        assert!(result.process.stderr.is_empty());
        assert!(!result.result.passed);
        assert!(result
            .result
            .failures
            .iter()
            .any(|f| f.kind == "limit_exceeded"));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn bounded_invalid_utf8_is_reported_lossily_without_panicking() {
    let dir = Temp::new();
    let p = port();
    let result = run_command(child_command(&dir.0, "invalid", p), p, options().1).await;
    assert_eq!(result.process.stderr, "\u{fffd}\u{fffd}!");
    assert!(!result.process.output_overflow);
    assert!(!result.process.output_incomplete);
}

#[tokio::test(flavor = "current_thread")]
async fn established_connection_failure_never_replays_the_handshake() {
    let dir = Temp::new();
    let p = port();
    let result = run_command(
        child_command(&dir.0, "disconnect-handshake", p),
        p,
        options().1,
    )
    .await;
    assert!(!result.result.passed);
    assert_eq!(
        std::fs::read_to_string(dir.0.join("connections")).unwrap(),
        "1"
    );
    assert!(result
        .result
        .failures
        .iter()
        .any(|f| f.kind == "disconnected"));
}

#[tokio::test(flavor = "current_thread")]
async fn known_foreign_listener_is_rejected_without_spawning_or_connecting() {
    for mode in ["hang-start", "echo"] {
        let dir = Temp::new();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = listener.local_addr().unwrap().port();
        let accepted = Arc::new(AtomicU64::new(0));
        let count = accepted.clone();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            count.fetch_add(1, Ordering::Relaxed);
            let mut socket = WebSocketSession::accept(stream, options().0.transport)
                .await
                .unwrap();
            while let Some(packet) = socket.recv().await {
                let bytes = match packet {
                    Packet::Text(s) => s.into_bytes(),
                    Packet::Binary(b) => b.to_vec(),
                };
                let replies = match decode_control(&bytes, 1024 * 1024).unwrap() {
                    EvalControl::Cancel => break,
                    EvalControl::Rtvi(message) if message.message_type == "client-ready" => {
                        vec![("bot-ready", json!({}))]
                    }
                    EvalControl::Rtvi(message) if message.message_type == "send-text" => vec![
                        ("bot-llm-started", json!({})),
                        ("bot-llm-text", json!({"text":"hello"})),
                        ("bot-llm-stopped", json!({})),
                    ],
                    _ => vec![],
                };
                for (kind, data) in replies {
                    let _ = socket
                        .sender()
                        .send(Packet::Text(
                            json!({"label":"rtvi-ai","type":kind,"data":data}).to_string(),
                        ))
                        .await;
                }
            }
            let _ = socket.finish().await;
        });
        let command = NativeCommand {
            executable: env!("CARGO_BIN_EXE_eval-process-fixture").into(),
            args: vec![
                mode.into(),
                p.to_string(),
                dir.0.to_string_lossy().into_owned(),
            ],
            cwd: dir.0.clone(),
            env: BTreeMap::new(),
        };
        let (session, process) = options();
        let result = run_hosted(
            command,
            scenario(),
            format!("ws://127.0.0.1:{p}"),
            session,
            process,
            Cancellation::new(),
        )
        .await;
        server.abort();
        let _ = server.await;
        if let Ok(result) = result {
            assert!(
                !result.result.passed,
                "foreign listener supplied a false pass: {result:?}"
            );
            assert!(result.process.pid.is_none());
        }
        assert!(
            !dir.0.join("pid").exists(),
            "known occupied port still launched a child"
        );
        assert_eq!(
            accepted.load(Ordering::Relaxed),
            0,
            "connected to a known foreign listener"
        );
    }
}

fn manifest_file(dir: &Path, mode: &str) -> PathBuf {
    std::fs::write(
        dir.join("scenario.yaml"),
        "name: review\nturns:\n  - user: hello\n    expect:\n      - event: response\n        text_contains: hello\n",
    )
    .unwrap();
    let path = dir.join("manifest.json");
    std::fs::write(
        &path,
        json!({"native_version":1,"base_port":port(),"repeat":2,"suite":[{
            "name":"review","bot":env!("CARGO_BIN_EXE_eval-process-fixture"),
            "args":[mode,"{port}",dir.join("child"),"{runner_body}"],
            "runner_body":{"data":{"fixture":true}},"scenarios":["scenario.yaml"]
        }]})
        .to_string(),
    )
    .unwrap();
    path
}

#[tokio::test]
async fn modified_release_ports_and_caps_are_rejected_before_any_launch() {
    let dir = Temp::new();
    let path = manifest_file(&dir.0, "echo");
    for case in 0..8 {
        let mut manifest = ReleaseManifest::load(&path).unwrap();
        match case {
            0 => manifest.jobs[1].port = manifest.jobs[0].port,
            1 => manifest.jobs[1].port = 0,
            2 => manifest.jobs[1].concurrency = 0,
            3 => manifest.jobs[1].name.clear(),
            4 => manifest.jobs[1].attempt = 0,
            5 => manifest.jobs[1].attempt = 129,
            6 => manifest.jobs[1].bot = dir.0.join("different-executable"),
            _ => manifest.jobs[1].attempt = manifest.jobs[0].attempt,
        }
        let (session, process) = options();
        assert!(run_release(manifest, session, process, Cancellation::new())
            .await
            .is_err());
        assert!(!dir.0.join("child/pid").exists());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn shared_label_uses_its_lowest_cap_after_public_plan_changes() {
    let dir = Temp::new();
    for name in ["a", "b"] {
        std::fs::write(
            dir.0.join(format!("{name}.json")),
            json!({"name":name,"turns":[
                {"user":"hello","expect":[{"event":"response","text_contains":"hello"}]}]
            })
            .to_string(),
        )
        .unwrap();
    }
    let path = dir.0.join("manifest.json");
    std::fs::write(
        &path,
        json!({"native_version":1,"base_port":port(),"concurrency":2,"suite":[
            {"name":"shared","bot":env!("CARGO_BIN_EXE_eval-process-fixture"),"concurrency":1,
             "args":["hang-run","{port}",dir.0.join("first")],"scenarios":["a.json"]},
            {"name":"shared","bot":env!("CARGO_BIN_EXE_eval-process-fixture"),"concurrency":2,
             "args":["echo","{port}",dir.0.join("second")],"scenarios":["b.json"]}
        ]})
        .to_string(),
    )
    .unwrap();
    let mut manifest = ReleaseManifest::load(path).unwrap();
    assert_eq!(manifest.jobs[0].concurrency, 1);
    assert_eq!(manifest.jobs[1].concurrency, 1);
    manifest.jobs[1].concurrency = 32;
    let (session, process) = options();
    let cancel = Cancellation::new();
    let task = tokio::spawn(run_release(manifest, session, process, cancel.clone()));
    tokio::time::timeout(Duration::from_secs(2), async {
        while !dir.0.join("first/run").exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    tokio::time::sleep(Duration::from_millis(75)).await;
    let second_started = dir.0.join("second/pid").exists();
    cancel.cancel();
    let result = task.await.unwrap().unwrap();
    assert!(
        !second_started,
        "shared-label concurrency exceeded its lowest cap"
    );
    assert_eq!(result.runs.len(), 2);
    assert!(!result.passed);
    assert!(result.runs[0].hosted.process.reaped);
    assert!(result.runs[1].hosted.process.pid.is_none());
}

#[cfg(target_os = "linux")]
#[tokio::test(flavor = "current_thread")]
async fn private_runner_body_is_removed_after_cancel_and_dropped_suite() {
    for abort in [false, true] {
        let dir = Temp::new();
        let path = manifest_file(&dir.0, "hang-run");
        let mut manifest = ReleaseManifest::load(path).unwrap();
        manifest.jobs.truncate(1);
        let (session, process) = options();
        let cancel = Cancellation::new();
        let task = tokio::spawn(run_release(manifest, session, process, cancel.clone()));
        tokio::time::timeout(Duration::from_secs(2), async {
            while !dir.0.join("child/run").exists() {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        let pid = std::fs::read_to_string(dir.0.join("child/pid")).unwrap();
        let args = std::fs::read(format!("/proc/{pid}/cmdline")).unwrap();
        let body = args
            .split(|b| *b == 0)
            .find(|s| s.ends_with(b"runner-body.json"))
            .map(|s| PathBuf::from(std::str::from_utf8(s).unwrap()))
            .unwrap();
        assert!(body.exists());
        if abort {
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
        } else {
            cancel.cancel();
            let result = task.await.unwrap().unwrap();
            assert!(!result.passed);
        }
        tokio::time::timeout(Duration::from_secs(1), async {
            while body.exists() || Path::new(&format!("/proc/{pid}")).exists() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("body file or direct child outlived its suite owner");
    }
}
