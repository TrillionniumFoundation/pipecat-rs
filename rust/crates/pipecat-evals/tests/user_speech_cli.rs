//! Real direct CLI subprocesses, local Cartesia HTTP, and the native host pipeline.
//! The feature-disabled test is intentionally compiled without required-features.
#![cfg(unix)]

#[path = "support/user_speech_common.rs"]
mod common;
use common::*;
#[cfg(feature = "user-speech")]
#[path = "support/user_speech.rs"]
mod fixture;
#[cfg(feature = "user-speech")]
use fixture::*;
use std::{
    path::Path,
    process::{Output, Stdio},
    time::Duration,
};
use tokio::{io::AsyncReadExt, net::TcpListener, process::Command};

const KEY_ENV: &str = "PIPECAT_USER_SPEECH_FIXTURE_KEY";
fn runtime(base: &str, cache: Option<&Path>) -> serde_json::Value {
    serde_json::json!({"provider":"cartesia","base_url":base,"api_key_env":KEY_ENV,"request_timeout_ms":2000,"cache_dir":cache})
}

async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    (&mut pipe)
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 256 * 1024 {
        return Err(std::io::Error::other("CLI output exceeds fixture bound"));
    }
    Ok(bytes)
}
async fn cli(args: &[String], cwd: &Path, key: bool) -> Result<Output, Error> {
    let executable = std::env::var_os("PIPECAT_EVAL_REPLAY_CLI")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| std::path::PathBuf::from(env!("CARGO_BIN_EXE_pipecat-eval")));
    let mut command = Command::new(executable);
    command
        .args(args)
        .current_dir(cwd)
        .env_remove(KEY_ENV)
        .env(
            "CARTESIA_API_KEY",
            "synthetic-unapproved-fallback-must-not-be-read",
        )
        // Test the new direct transport in an independent process. Never mutate
        // concurrent test-process proxy or credential environment.
        .env("HTTP_PROXY", "http://127.0.0.1:9")
        .env("HTTPS_PROXY", "http://127.0.0.1:9")
        .env("ALL_PROXY", "http://127.0.0.1:9")
        .env("http_proxy", "http://127.0.0.1:9")
        .env("https_proxy", "http://127.0.0.1:9")
        .env("all_proxy", "http://127.0.0.1:9")
        .env("NO_PROXY", "")
        .env("no_proxy", "")
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    if key {
        command.env(KEY_ENV, SECRET);
    }
    let mut child = command.spawn()?;
    let stdout = child.stdout.take().unwrap();
    let stderr = child.stderr.take().unwrap();
    let operation = tokio::time::timeout(WAIT + Duration::from_secs(2), async {
        let (status, stdout, stderr) =
            tokio::try_join!(child.wait(), read_output(stdout), read_output(stderr))?;
        Ok::<_, std::io::Error>(Output {
            status,
            stdout,
            stderr,
        })
    })
    .await;
    match operation {
        Ok(Ok(output)) => Ok(output),
        other => {
            let killed = child.start_kill();
            let joined = child.wait().await;
            Err(
                format!("CLI failure {other:?}; kill={killed:?}; actual child reap={joined:?}")
                    .into(),
            )
        }
    }
}
fn args(scenario: &Path, url: &str, runtime: &Path) -> Vec<String> {
    vec![
        "run".into(),
        scenario.display().to_string(),
        url.into(),
        "--user-speech".into(),
        runtime.display().to_string(),
        "--stop-bot".into(),
    ]
}
fn assert_exit(out: &Output, code: i32) {
    assert_eq!(
        out.status.code(),
        Some(code),
        "stdout={}; stderr={}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
    assert!(!String::from_utf8_lossy(&out.stderr).contains(SECRET));
    assert!(!String::from_utf8_lossy(&out.stdout).contains(SECRET));
}
async fn no_connection(listener: &TcpListener) -> bool {
    tokio::time::timeout(Duration::from_millis(30), listener.accept())
        .await
        .is_err()
}

#[cfg(not(feature = "user-speech"))]
#[tokio::test]
async fn feature_disabled_cli_rejects_explicit_input_before_file_or_network_access() {
    let b = Bundle::new();
    let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}/", ws.local_addr().unwrap());
    let source = b.json("source.json", &source());
    let root = b.cache();
    let config = b.json(
        "runtime.json",
        &runtime(
            &format!("http://{}", http.local_addr().unwrap()),
            Some(&root),
        ),
    );
    let out = cli(&args(&source, &url, &config), &b.0, true)
        .await
        .unwrap();
    let ws_idle = no_connection(&ws).await;
    let http_idle = no_connection(&http).await;
    assert_exit(&out, 2);
    assert!(String::from_utf8_lossy(&out.stderr).contains("user-speech"));
    assert!(
        ws_idle && http_idle,
        "disabled feature attempted network access"
    );
    assert_eq!(std::fs::read_dir(&root).unwrap().count(), 0);
}

#[cfg(feature = "user-speech")]
mod enabled {
    use super::*;
    use pipecat_evals::matcher::ScriptResult;
    use serde_json::{json, Value};
    use std::sync::atomic::Ordering;

    #[tokio::test]
    async fn independent_cold_and_warm_cli_processes_require_complete_native_host_consumption() {
        let b = Bundle::new();
        let root = b.cache();
        let synthesized = pcm(107);
        let recorded = pcm(127);
        b.put("override.wav", wav(&recorded));
        let mut value = source();
        value["turns"] = json!([
            {"user":REFERENCE,"expect":[expected(1)]},
            {"expect":[{"event":"user_transcription","absent":true,"within_ms":40}]},
            {"user":"recorded override reference", "audio":"override.wav", "expect":[expected(2)]}
        ]);
        let source_path = b.json("source.json", &value);
        let http = Http::start(vec![Reply::pcm(synthesized.clone())]).await;
        let config = b.json("runtime.json", &runtime(&http.url, Some(&root)));
        let cold_host = Host::start(vec![synthesized.clone(), recorded.clone()]).await;
        cold_host.trace.gated.store(true, Ordering::SeqCst);
        let cold_args = args(&source_path, &cold_host.url, &config);
        let cwd = b.0.clone();
        let child = tokio::spawn(async move { cli(&cold_args, &cwd, true).await });
        let first_tail = tokio::time::timeout(WAIT, cold_host.trace.final_arrived.notified())
            .await
            .is_ok();
        tokio::time::sleep(Duration::from_millis(100)).await;
        let exited_before_tail_consumption = child.is_finished();
        let completed_before_release = cold_host.trace.completed.load(Ordering::SeqCst);
        cold_host.trace.final_gate.add_permits(2);
        let cold = child.await.unwrap();
        let (cold_report, cold_trace) = cold_host.finish().await;
        let http_trace = http.finish().await;
        // The warm process receives the same now-closed HTTP destination. A hit
        // must succeed without any live server, hidden credential, or retry.
        let warm_host = Host::start(vec![synthesized.clone(), recorded.clone()]).await;
        let warm = cli(&args(&source_path, &warm_host.url, &config), &b.0, true).await;
        let (warm_report, warm_trace) = warm_host.finish().await;
        let cold = cold.unwrap();
        let warm = warm.unwrap();
        assert!(first_tail, "cold CLI never delivered full tail");
        assert!(
            !exited_before_tail_consumption,
            "CLI accepted local sender admission as consumption"
        );
        assert_eq!(completed_before_release, 0);
        assert_exit(&cold, 0);
        assert_exit(&warm, 0);
        let cold_result: ScriptResult = serde_json::from_slice(&cold.stdout).unwrap();
        let warm_result: ScriptResult = serde_json::from_slice(&warm.stdout).unwrap();
        assert!(cold_result.passed && warm_result.passed);
        assert!(
            cold_report.is_ok() && warm_report.is_ok(),
            "cold={cold_report:?}, warm={warm_report:?}"
        );
        let clips = [synthesized.clone(), recorded];
        assert_consumed(&cold_trace, &clips);
        assert_consumed(&warm_trace, &clips);
        assert_private_cache(&root, &synthesized);
        let requests = http_trace.requests.lock().unwrap();
        assert_eq!(requests.len(), 1);
        assert_request(&requests[0], REFERENCE);
        assert_eq!(http_trace.joined.load(Ordering::SeqCst), 1);
        assert_eq!(http_trace.active.load(Ordering::SeqCst), 0);
        assert_redacted(&cold_result);
        assert_redacted(&warm_result);
    }

    #[tokio::test]
    async fn direct_cli_no_cache_uses_only_explicit_loopback_despite_hostile_proxy_environment() {
        let b = Bundle::new();
        let bytes = pcm(147);
        let source = b.json("source.json", &source());
        let http = Http::start(vec![Reply::pcm(bytes.clone())]).await;
        let config = b.json("runtime.json", &runtime(&http.url, None));
        let host = Host::start(vec![bytes.clone()]).await;
        let out = cli(&args(&source, &host.url, &config), &b.0, true).await;
        let (report, trace) = host.finish().await;
        let http_trace = http.finish().await;
        let out = out.unwrap();
        assert_exit(&out, 0);
        assert!(report.is_ok(), "{report:?}");
        assert_consumed(&trace, &[bytes]);
        assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
        assert_eq!(
            std::fs::read_dir(&b.0).unwrap().count(),
            2,
            "no implicit cache files or directories"
        );
    }

    #[tokio::test]
    async fn cli_non_200_complete_pcm_exits_failed_without_playback_or_cache() {
        for status in [201, 206] {
            let b = Bundle::new();
            let root = b.cache();
            let source = b.json("source.json", &source());
            let mut reply = Reply::pcm(pcm(167));
            reply.status = status;
            let http = Http::start(vec![reply]).await;
            let config = b.json("runtime.json", &runtime(&http.url, Some(&root)));
            let host = Host::start(vec![pcm(167)]).await;
            let out = cli(&args(&source, &host.url, &config), &b.0, true).await;
            let (report, trace) = host.finish().await;
            let http_trace = http.finish().await;
            let out = out.unwrap();
            assert_exit(&out, 1);
            let result: ScriptResult = serde_json::from_slice(&out.stdout).unwrap();
            assert!(!result.passed);
            assert!(
                result
                    .failures
                    .iter()
                    .any(|f| f.kind == "user_speech_error"),
                "{result:?}"
            );
            assert!(report.is_ok(), "{report:?}");
            assert_no_speech(&trace);
            assert_no_cached_audio(&root);
            assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
        }
    }

    #[tokio::test]
    async fn options_runtime_and_whole_scenario_preflight_reject_before_any_connection_or_cache_work(
    ) {
        let b = Bundle::new();
        let root = b.cache();
        let ws = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let http = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/", ws.local_addr().unwrap());
        let source_path = b.json("source.json", &source());
        let good_runtime = runtime(
            &format!("http://{}", http.local_addr().unwrap()),
            Some(&root),
        );
        let runtime_path = b.json("runtime.json", &good_runtime);
        let base = args(&source_path, &url, &runtime_path);
        let mut cases: Vec<(Vec<String>, bool)> = vec![];
        for trailing in [
            vec!["--user-speech", "duplicate.json"],
            vec!["--recorded-audio"],
            vec!["--bot-audio-wav", "out.wav"],
            vec!["--bot-audio-response", "stt.json"],
            vec!["--unknown"],
            vec!["--stop-bot"],
        ] {
            let mut flags = base.clone();
            flags.extend(trailing.into_iter().map(str::to_owned));
            cases.push((flags, true));
        }
        cases.push((
            vec![
                "run".into(),
                source_path.display().to_string(),
                url.clone(),
                "--user-speech".into(),
            ],
            true,
        ));
        cases.push((
            vec![
                "suite".into(),
                source_path.display().to_string(),
                "--user-speech".into(),
                runtime_path.display().to_string(),
            ],
            true,
        ));
        // The named variable is absent although a synthetic CARTESIA_API_KEY
        // exists. Falling back to that ambient name would contact the listeners.
        cases.push((base.clone(), false));
        for (i, (key, bad)) in [
            ("provider", json!("kokoro")),
            ("api_key", json!("forbidden inline key")),
            ("base_url", json!("https://unapproved.example.invalid")),
            ("request_timeout_ms", json!(0)),
            ("request_timeout_ms", json!(60001)),
            ("cache_max_entries", json!(257)),
            ("cache_max_bytes", json!(67108865u64)),
            ("unknown_field", json!(true)),
        ]
        .into_iter()
        .enumerate()
        {
            let mut value = good_runtime.clone();
            value[key] = bad;
            let path = b.json(&format!("runtime-bad-{i}.json"), &value);
            cases.push((args(&source_path, &url, &path), true));
        }
        let duplicate = b.put("duplicate-runtime.json", format!("{{\"provider\":\"cartesia\",\"provider\":\"cartesia\",\"base_url\":\"http://{}\",\"api_key_env\":\"{}\"}}", http.local_addr().unwrap(), KEY_ENV));
        cases.push((args(&source_path, &url, &duplicate), true));
        let oversized = b.put("oversized-runtime.json", vec![b' '; 32769]);
        cases.push((args(&source_path, &url, &oversized), true));
        for (i, (field, invalid)) in [
            ("service", json!("kokoro")),
            ("factory", json!("arbitrary.python.Factory")),
            ("api_key", json!("forbidden in source")),
            ("base_url", json!("http://127.0.0.1:1")),
            ("sample_rate", json!(24000)),
            ("speed", json!(1.1)),
            ("voice", Value::Null),
            ("language", json!("en\u{0000}")),
        ]
        .into_iter()
        .enumerate()
        {
            let mut value = source();
            value["user"]["speech"][field] = invalid;
            // A valid first scenario cannot be run before the invalid later
            // scenario is preflighted under inherited source defaults.
            let valid_turns = value["turns"].clone();
            let invalid_user = value["user"].clone();
            value = source();
            value["scenarios"] = json!([
                {"name":"first valid","turns":valid_turns},
                {"name":"second invalid","user":invalid_user}
            ]);
            let path = b.json(&format!("source-bad-{i}.json"), &value);
            cases.push((args(&path, &url, &runtime_path), true));
        }
        for (arguments, key) in cases {
            let out = cli(&arguments, &b.0, key).await.unwrap();
            let ws_idle = no_connection(&ws).await;
            let http_idle = no_connection(&http).await;
            assert_exit(&out, 2);
            assert!(
                ws_idle && http_idle,
                "invalid preflight connected: {arguments:?}"
            );
            assert_eq!(
                std::fs::read_dir(&root).unwrap().count(),
                0,
                "invalid preflight touched cache"
            );
        }
    }

    #[tokio::test]
    async fn redirect_is_not_followed_and_never_transmits_key_to_second_endpoint() {
        let b = Bundle::new();
        let source = b.json("source.json", &source());
        let redirect = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut reply = Reply::pcm(pcm(177));
        reply.status = 302;
        reply.redirect = Some(format!(
            "http://{}/capture-key",
            redirect.local_addr().unwrap()
        ));
        let http = Http::start(vec![reply]).await;
        let config = b.json("runtime.json", &runtime(&http.url, None));
        let host = Host::start(vec![pcm(177)]).await;
        let out = cli(&args(&source, &host.url, &config), &b.0, true).await;
        let (report, trace) = host.finish().await;
        let http_trace = http.finish().await;
        let no_redirect = no_connection(&redirect).await;
        assert_exit(&out.unwrap(), 1);
        assert!(
            no_redirect,
            "credential-bearing provider request followed redirect"
        );
        assert_eq!(http_trace.requests.lock().unwrap().len(), 1);
        assert!(report.is_ok());
        assert_no_speech(&trace);
    }
}
