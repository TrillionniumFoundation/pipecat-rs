//! Actual compiled native image bot and eval CLI, synthetic local assets and HTTP.
//! Feature-disabled CLI cases deliberately remain present in the default build.
#![cfg(unix)]
#[path = "support/image_cli_fixture.rs"]
mod fixture;
use fixture::*;
use std::{
    path::Path,
    process::{Output, Stdio},
    time::Duration,
};
use tokio::{
    io::AsyncReadExt,
    net::TcpListener,
    process::{Child, Command},
    task::JoinHandle,
};

async fn read_output(mut pipe: impl tokio::io::AsyncRead + Unpin) -> std::io::Result<Vec<u8>> {
    let mut bytes = Vec::new();
    (&mut pipe)
        .take(256 * 1024 + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.len() > 256 * 1024 {
        return Err(std::io::Error::other("CLI fixture output bound"));
    }
    Ok(bytes)
}
struct ChildOwner {
    child: Child,
    stdout: JoinHandle<std::io::Result<Vec<u8>>>,
    stderr: JoinHandle<std::io::Result<Vec<u8>>>,
}
impl ChildOwner {
    fn spawn(args: &[String], cwd: &Path) -> Result<Self, Error> {
        let mut command = Command::new(env!("CARGO_BIN_EXE_pipecat-eval"));
        command
            .args(args)
            .current_dir(cwd)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        // Never inherit provider credentials or proxy routing into this process.
        for key in [
            "OPENAI_API_KEY",
            "GEMINI_API_KEY",
            "GOOGLE_API_KEY",
            "CARTESIA_API_KEY",
        ] {
            command.env_remove(key);
        }
        for key in [
            "HTTP_PROXY",
            "HTTPS_PROXY",
            "ALL_PROXY",
            "http_proxy",
            "https_proxy",
            "all_proxy",
        ] {
            command.env(key, "http://127.0.0.1:9");
        }
        command.env("NO_PROXY", "").env("no_proxy", "");
        let mut child = command.spawn()?;
        let stdout = child.stdout.take().unwrap();
        let stderr = child.stderr.take().unwrap();
        Ok(Self {
            child,
            stdout: tokio::spawn(read_output(stdout)),
            stderr: tokio::spawn(read_output(stderr)),
        })
    }
    async fn finish(mut self) -> Result<Output, Error> {
        let waited = tokio::time::timeout(WAIT + Duration::from_secs(2), self.child.wait()).await;
        let status = match waited {
            Ok(Ok(status)) => Ok(status),
            other => {
                let kill = self.child.start_kill();
                let joined = self.child.wait().await;
                Err(format!(
                    "CLI wait failed: {other:?}; kill={kill:?}; actual reap={joined:?}"
                ))
            }
        };
        let stdout = join_pipe(self.stdout).await;
        let stderr = join_pipe(self.stderr).await;
        Ok(Output {
            status: status.map_err(|e| -> Error { e.into() })?,
            stdout: stdout?,
            stderr: stderr?,
        })
    }
}
async fn join_pipe(mut task: JoinHandle<std::io::Result<Vec<u8>>>) -> Result<Vec<u8>, Error> {
    match tokio::time::timeout(WAIT, &mut task).await {
        Ok(result) => Ok(result??),
        Err(_) => {
            task.abort();
            let _ = task.await;
            Err("pipe owner did not join".into())
        }
    }
}
async fn cli(args: &[String], cwd: &Path) -> Result<Output, Error> {
    ChildOwner::spawn(args, cwd)?.finish().await
}
fn args(source: &Path, url: &str) -> Vec<String> {
    vec![
        "run".into(),
        source.display().to_string(),
        url.into(),
        "--user-images".into(),
        "--stop-bot".into(),
    ]
}
fn assert_exit(output: &Output, code: i32) {
    assert_eq!(
        output.status.code(),
        Some(code),
        "stdout={}; stderr={}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(!String::from_utf8_lossy(&output.stdout).contains("synthetic-local-image-key"));
    assert!(!String::from_utf8_lossy(&output.stderr).contains("synthetic-local-image-key"));
}
async fn no_connection(listener: &TcpListener) -> bool {
    tokio::time::timeout(Duration::from_millis(30), listener.accept())
        .await
        .is_err()
}

#[cfg(not(feature = "user-images"))]
#[tokio::test]
async fn disabled_image_flag_fails_before_file_read_or_connection() {
    let bundle = Bundle::new();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let source = bundle.0.join("does-not-exist.yaml");
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let output = cli(&args(&source, &url), &bundle.0).await.unwrap();
    let idle = no_connection(&listener).await;
    assert_exit(&output, 2);
    assert!(String::from_utf8_lossy(&output.stderr).contains("user-images"));
    assert!(idle);
}
#[tokio::test]
async fn ordinary_cli_still_rejects_image_input_without_opt_in() {
    let bundle = Bundle::new();
    bundle.put("a.png", png_rgb(2, 2, [24, 80, 140]));
    let source = bundle.source("a.png", ANSWER);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let output = cli(
        &["run".into(), source.display().to_string(), url],
        &bundle.0,
    )
    .await
    .unwrap();
    let idle = no_connection(&listener).await;
    assert_exit(&output, 2);
    assert!(idle);
}
#[cfg(feature = "user-images")]
#[tokio::test]
async fn malformed_assets_and_unsupported_mode_combinations_are_preflight_errors() {
    let bundle = Bundle::new();
    bundle.put("a.png", b"malformed image");
    let source = bundle.source("a.png", ANSWER);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("ws://{}", listener.local_addr().unwrap());
    let malformed = cli(&args(&source, &url), &bundle.0).await.unwrap();
    assert_exit(&malformed, 2);
    assert!(!String::from_utf8_lossy(&malformed.stderr).contains("cannot combine"));
    // A valid asset isolates combination validation from codec/file failures.
    bundle.put("a.png", png_rgb(2, 2, [24, 80, 140]));
    for extra in [
        vec!["--recorded-audio"],
        vec!["--user-speech", "absent-runtime.json"],
        vec!["--bot-audio-response", "absent-config.json"],
        vec!["--bot-audio-wav", "output.wav"],
    ] {
        let mut arguments = args(&source, &url);
        arguments.extend(extra.into_iter().map(String::from));
        let output = cli(&arguments, &bundle.0).await.unwrap();
        assert_exit(&output, 2);
        assert!(String::from_utf8_lossy(&output.stderr)
            .contains("--user-images cannot combine with audio modes"));
    }
    let output = cli(
        &[
            "suite".into(),
            "missing-manifest.yaml".into(),
            "--user-images".into(),
        ],
        &bundle.0,
    )
    .await
    .unwrap();
    assert_exit(&output, 2);
    assert!(no_connection(&listener).await);
    assert!(!bundle.0.join("output.wav").exists());
}

#[cfg(all(feature = "user-images", feature = "native-bot-example"))]
mod enabled {
    use super::*;
    use pipecat_evals::matcher::ScriptResult;
    use std::sync::atomic::Ordering;
    use tokio::io::{AsyncBufReadExt, BufReader};
    struct BotChild {
        url: String,
        child: Child,
        stdout: JoinHandle<std::io::Result<Vec<u8>>>,
        stderr: JoinHandle<std::io::Result<Vec<u8>>>,
    }
    impl BotChild {
        async fn start(endpoint: &str, cwd: &Path) -> Result<Self, Error> {
            let mut command = Command::new(env!("CARGO_BIN_EXE_eval-image-fixture"));
            command
                .arg(endpoint)
                .current_dir(cwd)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .kill_on_drop(true);
            for key in ["OPENAI_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY"] {
                command.env_remove(key);
            }
            let mut child = command.spawn()?;
            let mut stdout = BufReader::new(child.stdout.take().unwrap());
            let stderr = tokio::spawn(read_output(child.stderr.take().unwrap()));
            let mut line = String::new();
            let readiness =
                tokio::time::timeout(WAIT, (&mut stdout).take(4097).read_line(&mut line)).await;
            if !matches!(readiness, Ok(Ok(1..)))
                || line.len() > 4096
                || !line.starts_with("ws://127.0.0.1:")
            {
                let _ = child.start_kill();
                let joined = child.wait().await;
                let errors = join_pipe(stderr).await;
                return Err(format!("bot readiness failed: {readiness:?}, line={line:?}, reaped={joined:?}, stderr={errors:?}").into());
            }
            Ok(Self {
                url: line.trim().to_owned(),
                child,
                stdout: tokio::spawn(read_output(stdout)),
                stderr,
            })
        }
        async fn finish(mut self) -> Result<Output, Error> {
            let waited = tokio::time::timeout(WAIT, self.child.wait()).await;
            let status = match waited {
                Ok(Ok(status)) => Ok(status),
                other => {
                    let kill = self.child.start_kill();
                    let joined = self.child.wait().await;
                    Err(format!(
                        "bot wait failed: {other:?}; kill={kill:?}; actual reap={joined:?}"
                    ))
                }
            };
            let stdout = join_pipe(self.stdout).await;
            let stderr = join_pipe(self.stderr).await;
            Ok(Output {
                status: status.map_err(|e| -> Error { e.into() })?,
                stdout: stdout?,
                stderr: stderr?,
            })
        }
    }
    fn interrupt(pid: Option<u32>) -> Result<(), Error> {
        let pid = pid.ok_or("CLI has no live PID")?;
        let status = std::process::Command::new("kill")
            .args(["-INT", &pid.to_string()])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()?;
        if !status.success() {
            return Err(format!("SIGINT failed: {status}").into());
        }
        Ok(())
    }
    #[tokio::test]
    async fn compiled_bot_and_cli_require_real_provider_completion_then_exit_zero_or_one() {
        for (expected, code) in [(ANSWER, 0), ("deliberately absent response", 1)] {
            let bundle = Bundle::new();
            bundle.put("a.png", png_rgb(16, 12, [24, 80, 140]));
            let source = bundle.source("a.png", expected);
            let http = Http::start().await;
            http.trace.gated.store(true, Ordering::SeqCst);
            let bot = BotChild::start(&http.url, &bundle.0).await.unwrap();
            let runner = ChildOwner::spawn(&args(&source, &bot.url), &bundle.0).unwrap();
            let task = tokio::spawn(runner.finish());
            let arrived = tokio::time::timeout(WAIT, http.trace.arrived.notified()).await;
            let finished_before_http = task.is_finished();
            http.trace.gate.add_permits(1);
            let output = task.await.unwrap();
            let bot_output = bot.finish().await;
            let http = http.finish().await;
            arrived.unwrap();
            assert!(
                !finished_before_http,
                "CLI accepted WS admission instead of native HTTP/consumer completion"
            );
            let output = output.unwrap();
            let bot_output = bot_output.unwrap();
            assert_exit(&output, code);
            assert_exit(&bot_output, 0);
            let result: ScriptResult = serde_json::from_slice(&output.stdout).unwrap();
            assert_eq!(result.passed, code == 0);
            let log = String::from_utf8_lossy(&bot_output.stderr);
            assert!(log.contains("image fixture joined"));
            assert!(log.contains("\"rgb_images\":1"));
            assert!(log.contains("\"cleaned\":1"));
            assert!(log.contains("\"dropped\":1"));
            let requests = http.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_http_image(&requests[0], TEXT, (16, 12), &rgb(16, 12, [24, 80, 140]), 1);
            assert_eq!(http.joined.load(Ordering::SeqCst), 1);
            assert_eq!(http.active.load(Ordering::SeqCst), 0);
        }
    }
    #[tokio::test]
    async fn ctrl_c_during_actual_http_wait_cancels_eval_and_reaps_both_native_children() {
        let bundle = Bundle::new();
        bundle.put("a.png", png_rgb(4, 4, [24, 80, 140]));
        let source = bundle.source("a.png", ANSWER);
        let http = Http::start().await;
        http.trace.gated.store(true, Ordering::SeqCst);
        let bot = BotChild::start(&http.url, &bundle.0).await.unwrap();
        let runner = ChildOwner::spawn(&args(&source, &bot.url), &bundle.0).unwrap();
        let arrived = tokio::time::timeout(WAIT, http.trace.arrived.notified()).await;
        let interrupted = interrupt(runner.child.id());
        let output = runner.finish().await;
        let bot_output = bot.finish().await;
        let http = http.finish().await;
        arrived.unwrap();
        interrupted.unwrap();
        let output = output.unwrap();
        let bot_output = bot_output.unwrap();
        assert_exit(&output, 1);
        assert_exit(&bot_output, 0);
        let result: ScriptResult = serde_json::from_slice(&output.stdout).unwrap();
        assert!(!result.passed);
        assert!(result.failures.iter().any(|failure| failure.kind.contains("cancel") || failure.reason.contains("cancel")), "{result:?}");
        assert_eq!(http.requests.lock().unwrap().len(), 1);
        assert_eq!(http.active.load(Ordering::SeqCst), 0);
        assert_eq!(http.joined.load(Ordering::SeqCst), 1);
        assert!(String::from_utf8_lossy(&bot_output.stderr).contains("image fixture joined"));
    }
}
