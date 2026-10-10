// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
#![cfg(feature = "process-fixtures")]
use pipecat_evals::{
    process::{run_hosted, HostedResult, NativeCommand, ProcessOptions},
    release::{run_release, ReleaseManifest},
    Cancellation, ScenarioFile, ScriptScenario, SessionOptions,
};
use serde_json::{json, Value};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "pipecat-process-tests-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir(&dir).unwrap();
        Self(dir)
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

fn command(mode: &str, port: u16, dir: &Path) -> NativeCommand {
    NativeCommand {
        executable: env!("CARGO_BIN_EXE_eval-process-fixture").into(),
        args: vec![
            mode.into(),
            port.to_string(),
            dir.to_string_lossy().into_owned(),
        ],
        cwd: dir.to_path_buf(),
        env: BTreeMap::new(),
    }
}
fn scenario() -> ScriptScenario {
    ScenarioFile::from_value(json!({"name":"native","turns":[{"user":"hello","expect":[{"event":"response","text_contains":"hello"}]}]})).unwrap().scenarios.remove(0)
}
fn options() -> (SessionOptions, ProcessOptions) {
    let mut session = SessionOptions::default();
    session.transport.close_timeout = Duration::from_millis(50);
    session.transport.write_timeout = Duration::from_millis(100);
    session.transport.connect_timeout = Duration::from_millis(100);
    session.handshake_timeout = Duration::from_secs(1);
    session.max_duration = Duration::from_secs(3);
    let process = ProcessOptions {
        startup_timeout: Duration::from_secs(2),
        shutdown_grace: Duration::from_millis(100),
        output_drain_timeout: Duration::from_millis(200),
        ..Default::default()
    };
    (session, process)
}
async fn wait_marker(dir: &Path, name: &str) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while !dir.join(name).exists() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap_or_else(|_| {
        panic!(
            "missing fixture marker {name} in {} (files: {:?})",
            dir.display(),
            std::fs::read_dir(dir).map(|d| d
                .filter_map(Result::ok)
                .map(|e| e.file_name())
                .collect::<Vec<_>>())
        )
    });
}
fn assert_reaped(result: &HostedResult) {
    assert!(result.process.reaped, "{result:?}");
    #[cfg(target_os = "linux")]
    assert!(
        !Path::new(&format!("/proc/{}", result.process.pid.unwrap())).exists(),
        "child remains alive or zombie"
    );
}
async fn assert_no_tasks() {
    tokio::time::timeout(Duration::from_secs(1), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("owned task remained alive");
}
async fn run_mode(mode: &str) -> HostedResult {
    let dir = Temp::new();
    let p = port();
    let (session, process) = options();
    let result = run_hosted(
        command(mode, p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert_reaped(&result);
    assert_no_tasks().await;
    result
}
#[tokio::test]
async fn successful_native_child_is_reaped_and_output_retained() {
    let result = run_mode("echo").await;
    assert!(result.result.passed, "{result:?}");
    assert!(result.process.success);
    assert!(!result.process.forced);
    assert_eq!(result.process.stdout, "fixture started\n");
    assert_eq!(result.process.stderr, "fixture diagnostic\n");
}
#[tokio::test]
async fn startup_failure_is_actionable_without_launch() {
    let dir = Temp::new();
    let p = port();
    let (session, process) = options();
    let mut cmd = command("echo", p, &dir.0);
    cmd.executable = dir.0.join("missing-bot");
    let result = run_hosted(
        cmd,
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.result.failures[0].kind, "spawn_failed");
    assert!(result.process.pid.is_none());
    assert_no_tasks().await;
}
#[tokio::test]
async fn readiness_timeout_forces_reap() {
    let dir = Temp::new();
    let p = port();
    let (session, mut process) = options();
    process.startup_timeout = Duration::from_millis(150);
    let result = run_hosted(
        command("hang-start", p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert_eq!(result.result.failures[0].kind, "startup_timeout");
    assert!(result.process.forced);
    assert_reaped(&result);
    assert_no_tasks().await;
}
#[tokio::test]
async fn abnormal_exits_at_start_run_and_cleanup_fail() {
    for (mode, code) in [("exit-start", 17), ("exit-run", 18), ("exit-cleanup", 19)] {
        let result = run_mode(mode).await;
        assert!(!result.result.passed, "{mode}: {result:?}");
        assert_eq!(result.process.exit_code, Some(code));
        assert!(
            result
                .result
                .failures
                .iter()
                .any(|f| f.kind == "process_exit"),
            "{mode}: {result:?}"
        );
    }
}
#[tokio::test]
async fn cancellation_at_each_phase_reaps_and_joins() {
    for (mode, marker) in [
        ("hang-start", "pid"),
        ("hang-run", "run"),
        ("ignore-stop", "cleanup"),
    ] {
        let dir = Temp::new();
        let p = port();
        let (session, mut process) = options();
        process.shutdown_grace = Duration::from_secs(2);
        let cancel = Cancellation::new();
        let other = cancel.clone();
        let task = tokio::spawn(run_hosted(
            command(mode, p, &dir.0),
            scenario(),
            format!("ws://127.0.0.1:{p}"),
            session,
            process,
            cancel,
        ));
        wait_marker(&dir.0, marker).await;
        other.cancel();
        let result = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(!result.result.passed);
        assert!(result.result.failures.iter().any(|f| f.kind == "cancelled"));
        assert_reaped(&result);
        assert_no_tasks().await;
    }
}
#[tokio::test]
async fn pre_cancelled_does_not_spawn_and_drop_reaps_each_phase() {
    let dir = Temp::new();
    let p = port();
    let (session, process) = options();
    let cancel = Cancellation::new();
    cancel.cancel();
    let result = run_hosted(
        command("echo", p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        cancel,
    )
    .await
    .unwrap();
    assert!(result.process.pid.is_none());
    assert!(!dir.0.join("pid").exists());
    for (mode, marker) in [
        ("hang-start", "pid"),
        ("hang-run", "run"),
        ("ignore-stop", "cleanup"),
    ] {
        let dir = Temp::new();
        let p = port();
        let (session, mut process) = options();
        process.shutdown_grace = Duration::from_secs(2);
        let mut task = tokio::spawn(run_hosted(
            command(mode, p, &dir.0),
            scenario(),
            format!("ws://127.0.0.1:{p}"),
            session,
            process,
            Cancellation::new(),
        ));
        tokio::select! {
            _ = wait_marker(&dir.0, marker) => {},
            result = &mut task => panic!("{mode} child completed before marker {marker}: {result:?}"),
        }
        let pid = std::fs::read_to_string(dir.0.join("pid")).unwrap();
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        #[cfg(target_os = "linux")]
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
        assert_no_tasks().await;
    }
}
#[tokio::test]
async fn output_and_result_retention_overflow_fail_instead_of_passing() {
    let dir = Temp::new();
    let p = port();
    let (session, mut process) = options();
    process.max_stdout_bytes = 1024;
    let result = run_hosted(
        command("overflow-start", p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(result.process.output_overflow);
    assert!(result.process.stdout.is_empty());
    assert!(!result.result.passed);
    assert_reaped(&result);
    let p = port();
    let (session, mut process) = options();
    process.max_result_bytes = 1024;
    let result = run_hosted(
        command("large-reply", p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(!result.result.passed);
    assert_eq!(result.result.failures[0].kind, "limit_exceeded");
    assert!(serde_json::to_vec(&result).unwrap().len() <= 1024);
    assert_reaped(&result);
    assert_no_tasks().await;
}
#[tokio::test]
async fn literal_arguments_clean_environment_and_value_redaction() {
    let dir = Temp::new();
    let p = port();
    let (session, process) = options();
    let body = dir.0.join("body.json");
    std::fs::write(&body, r#"{"native":true}"#).unwrap();
    let mut cmd = command("inspect", p, &dir.0);
    cmd.args.extend([
        body.to_string_lossy().into_owned(),
        "hello; $(this is not a shell)".into(),
    ]);
    cmd.env
        .insert("EVAL_ALLOWED".into(), "exact-secret-value".into());
    assert!(!format!("{cmd:?}").contains("exact-secret-value"));
    let result = run_hosted(
        cmd,
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(result.result.passed, "{result:?}");
    assert!(result.process.stdout.contains(r#""inherited_path":null"#));
    assert!(result
        .process
        .stdout
        .contains("hello; $(this is not a shell)"));
    assert!(result.process.stdout.contains("[REDACTED]"));
    assert!(!result.process.stdout.contains("exact-secret-value"));
    assert_reaped(&result);
    assert_no_tasks().await;
}
fn write_manifest(dir: &Path, mode: &str, p: u16) -> PathBuf {
    std::fs::write(dir.join("scenario.yaml"),"name: native\nturns:\n  - user: hello\n    expect:\n      - event: response\n        text_contains: hello\n").unwrap();
    let manifest = json!({"native_version":1,"concurrency":2,"repeat":2,"base_port":p,"suite":[
        {"name":"native-a","bot":env!("CARGO_BIN_EXE_eval-process-fixture"),"concurrency":1,"args":[mode,"{port}",dir.join("a")],"scenarios":["scenario.yaml"]},
        {"name":"native-b","bot":env!("CARGO_BIN_EXE_eval-process-fixture"),"concurrency":1,"args":[mode,"{port}",dir.join("b")],"scenarios":["scenario.yaml"]}
    ]});
    let path = dir.join("manifest.json");
    std::fs::write(&path, manifest.to_string()).unwrap();
    path
}
#[tokio::test]
async fn release_runs_are_attempt_major_and_provenance_is_retained() {
    let dir = Temp::new();
    let p = port();
    if p > 65531 {
        return;
    }
    let path = write_manifest(&dir.0, "slow-reply", p);
    let manifest = ReleaseManifest::load(&path).unwrap();
    assert_eq!(
        manifest
            .jobs
            .iter()
            .map(|j| (j.name.as_str(), j.attempt))
            .collect::<Vec<_>>(),
        vec![
            ("native-a", 1),
            ("native-b", 1),
            ("native-a", 2),
            ("native-b", 2)
        ]
    );
    let (session, process) = options();
    let result = run_release(manifest, session, process, Cancellation::new())
        .await
        .unwrap();
    assert!(result.passed, "{result:?}");
    assert_eq!(result.runs.len(), 4);
    for (i, run) in result.runs.iter().enumerate() {
        assert_eq!(run.port, p + i as u16);
        assert_eq!(run.scenario, "native");
        assert_reaped(&run.hosted);
    }
    assert_no_tasks().await;
}
#[tokio::test]
async fn runner_body_path_and_inline_are_supplied_and_inline_file_removed() {
    for inline in [false, true] {
        let dir = Temp::new();
        let p = port();
        let path = write_manifest(&dir.0, "inspect", p);
        let body_dir = dir.0.join("bodies");
        std::fs::create_dir(&body_dir).unwrap();
        let body = body_dir.join("body.json");
        std::fs::write(&body, r#"{"native":true}"#).unwrap();
        let mut manifest: Value = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
        manifest["repeat"] = 1.into();
        manifest["suite"].as_array_mut().unwrap().truncate(1);
        manifest["suite"][0]["args"]
            .as_array_mut()
            .unwrap()
            .push("{runner_body}".into());
        manifest["suite"][0]["runner_body"] = if inline {
            json!({"data":{"native":true}})
        } else {
            json!({"path":"bodies/body.json"})
        };
        std::fs::write(&path, manifest.to_string()).unwrap();
        let (session, process) = options();
        let result = run_release(
            ReleaseManifest::load(path).unwrap(),
            session,
            process,
            Cancellation::new(),
        )
        .await
        .unwrap();
        assert!(result.passed, "{result:?}");
        let output = &result.runs[0].hosted.process.stdout;
        assert!(output.contains("body={\"native\":true}"));
        let cwd = if inline { &dir.0 } else { &body_dir };
        assert!(output.contains(&format!("\"cwd\":{}", serde_json::to_string(cwd).unwrap())));
        let body_path = std::fs::read_to_string(dir.0.join("a/body-path")).unwrap();
        assert_eq!(Path::new(&body_path).exists(), !inline);
        assert_no_tasks().await;
    }
}
#[test]
fn source_python_manifest_and_unsupported_audio_are_explicitly_rejected() {
    let original =
        Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../evals/release/manifest.yaml");
    assert!(matches!(
        ReleaseManifest::load(original),
        Err(pipecat_evals::EvalError::Unsupported(_))
    ));
    let dir = Temp::new();
    let path = write_manifest(&dir.0, "echo", port());
    for scenario in [
        "name: audio\nuser: {modality: audio}\nturns: []\n",
        "name: sim\npersona: caller\ngoal: order pizza\n",
        "name: judge\njudge: {eval: {service: ollama}}\nturns: []\n",
    ] {
        std::fs::write(dir.0.join("scenario.yaml"), scenario).unwrap();
        assert!(ReleaseManifest::load(&path).is_err());
    }
}

#[tokio::test(flavor = "current_thread")]
async fn dropping_blocked_pipe_writer_and_exited_child_never_leaves_a_zombie() {
    for mode in ["flood", "exit-start"] {
        for _ in 0..10 {
            let dir = Temp::new();
            let p = port();
            let (session, mut process) = options();
            process.max_stdout_bytes = 1024 * 1024;
            let task = tokio::spawn(run_hosted(
                command(mode, p, &dir.0),
                scenario(),
                format!("ws://127.0.0.1:{p}"),
                session,
                process,
                Cancellation::new(),
            ));
            wait_marker(&dir.0, "pid").await;
            let pid = std::fs::read_to_string(dir.0.join("pid")).unwrap();
            task.abort();
            let _ = task.await;
            #[cfg(target_os = "linux")]
            assert!(!Path::new(&format!("/proc/{pid}")).exists());
            assert_no_tasks().await;
        }
    }
}
#[tokio::test(flavor = "current_thread")]
async fn shutdown_grace_does_not_block_other_tasks_and_timeout_cancel_race_reaps() {
    let dir = Temp::new();
    let p = port();
    let (session, mut process) = options();
    process.shutdown_grace = Duration::from_millis(150);
    let task = tokio::spawn(run_hosted(
        command("ignore-stop", p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    ));
    wait_marker(&dir.0, "cleanup").await;
    for _ in 0..5 {
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!task.is_finished());
    }
    let result = task.await.unwrap().unwrap();
    assert!(result.process.forced);
    assert_reaped(&result);
    assert_no_tasks().await;
    for _ in 0..5 {
        let dir = Temp::new();
        let p = port();
        let (session, mut process) = options();
        process.startup_timeout = Duration::from_millis(50);
        let cancel = Cancellation::new();
        let other = cancel.clone();
        let task = tokio::spawn(run_hosted(
            command("hang-start", p, &dir.0),
            scenario(),
            format!("ws://127.0.0.1:{p}"),
            session,
            process,
            cancel,
        ));
        tokio::time::sleep(Duration::from_millis(50)).await;
        other.cancel();
        let result = task.await.unwrap().unwrap();
        assert_reaped(&result);
        assert!(!result.result.passed);
        assert_no_tasks().await;
    }
}
#[tokio::test]
async fn release_retention_overflow_and_cancelled_queue_preserve_bounded_provenance() {
    let dir = Temp::new();
    let path = write_manifest(&dir.0, "large-reply", port());
    let manifest = ReleaseManifest::load(&path).unwrap();
    let (session, process) = options();
    let cancel = Cancellation::new();
    let result = pipecat_evals::release::run_release_with_budget(
        manifest,
        session,
        process,
        cancel.clone(),
        4096,
    )
    .await
    .unwrap();
    assert!(!result.passed);
    assert!(cancel.is_cancelled());
    assert_eq!(result.runs.len(), 4);
    assert!(serde_json::to_vec(&result).unwrap().len() <= 4096);
    assert!(result.runs.iter().any(|r| r
        .hosted
        .result
        .failures
        .iter()
        .any(|f| f.kind == "limit_exceeded")));
    for r in &result.runs {
        if r.hosted.process.pid.is_some() {
            assert_reaped(&r.hosted);
        }
    }
    assert_no_tasks().await;
    let mut manifest = ReleaseManifest::load(&path).unwrap();
    manifest.concurrency = 1;
    let cancel = Cancellation::new();
    cancel.cancel();
    let (session, process) = options();
    let result = run_release(manifest, session, process, cancel)
        .await
        .unwrap();
    assert!(
        result
            .runs
            .iter()
            .all(|r| r.hosted.process.pid.is_none()
                && r.hosted.result.failures[0].kind == "cancelled")
    );
    assert_no_tasks().await;
}
#[tokio::test]
async fn dropping_release_suite_reaps_all_active_children() {
    let dir = Temp::new();
    let path = write_manifest(&dir.0, "hang-run", port());
    let manifest = ReleaseManifest::load(path).unwrap();
    let (session, process) = options();
    let task = tokio::spawn(run_release(manifest, session, process, Cancellation::new()));
    wait_marker(&dir.0.join("a"), "run").await;
    wait_marker(&dir.0.join("b"), "run").await;
    let pids = ["a", "b"].map(|n| std::fs::read_to_string(dir.0.join(n).join("pid")).unwrap());
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_no_tasks().await;
    #[cfg(target_os = "linux")]
    for pid in pids {
        assert!(!Path::new(&format!("/proc/{pid}")).exists());
    }
}

#[test]
fn scripts_and_invalid_host_config_are_rejected_without_execution() {
    let dir = Temp::new();
    let mut cmd = command("echo", port(), &dir.0);
    let script = dir.0.join("extensionless-script");
    std::fs::write(
        &script,
        b"#!/usr/bin/env python3\nprint('must not execute')\n",
    )
    .unwrap();
    cmd.executable = script.clone();
    assert!(matches!(
        cmd.validate(),
        Err(pipecat_evals::EvalError::Unsupported(_))
    ));
    std::fs::write(&script, b"echo extensionless text must never execute\n").unwrap();
    assert!(matches!(
        cmd.validate(),
        Err(pipecat_evals::EvalError::Unsupported(_))
    ));
    std::fs::write(&script, b"").unwrap();
    assert!(matches!(
        cmd.validate(),
        Err(pipecat_evals::EvalError::Unsupported(_))
    ));
    for extension in ["py", "sh", "cmd", "bat", "ps1"] {
        cmd.executable = dir.0.join(format!("script.{extension}"));
        assert!(matches!(
            cmd.validate(),
            Err(pipecat_evals::EvalError::Unsupported(_))
        ));
    }
    cmd = command("echo", port(), &dir.0);
    cmd.env.insert("INVALID=NAME".into(), "value".into());
    assert!(cmd.validate().is_err());
}
#[tokio::test]
async fn session_deadline_stops_and_reaps_native_bot() {
    let dir = Temp::new();
    let p = port();
    let (mut session, process) = options();
    session.max_duration = Duration::from_millis(100);
    let result = run_hosted(
        command("hang-run", p, &dir.0),
        scenario(),
        format!("ws://127.0.0.1:{p}"),
        session,
        process,
        Cancellation::new(),
    )
    .await
    .unwrap();
    assert!(!result.result.passed);
    assert!(result.result.failures.iter().any(|f| f.kind == "timeout"));
    assert_reaped(&result);
    assert_no_tasks().await;
}
#[tokio::test]
async fn impossible_release_summary_budget_fails_before_any_launch() {
    let dir = Temp::new();
    let path = write_manifest(&dir.0, "echo", port());
    let manifest = ReleaseManifest::load(path).unwrap();
    let (session, process) = options();
    let result = pipecat_evals::release::run_release_with_budget(
        manifest,
        session,
        process,
        Cancellation::new(),
        1024,
    )
    .await;
    assert!(matches!(
        result,
        Err(pipecat_evals::EvalError::Limit("release summary budget"))
    ));
    assert!(!dir.0.join("a/pid").exists());
    assert!(!dir.0.join("b/pid").exists());
    assert_no_tasks().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_native_launches_keep_distinct_ports_and_join_every_owner() {
    let directory = Temp::new();
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let dir = directory.0.join(index.to_string());
        std::fs::create_dir(&dir).unwrap();
        let p = port();
        let (session, process) = options();
        tasks.spawn(run_hosted(
            command("echo", p, &dir),
            scenario(),
            format!("ws://127.0.0.1:{p}"),
            session,
            process,
            Cancellation::new(),
        ));
    }
    let mut completed = 0;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap().unwrap();
        assert!(result.result.passed, "{result:?}");
        assert_reaped(&result);
        completed += 1;
    }
    assert_eq!(completed, 32);
    assert_no_tasks().await;
}
