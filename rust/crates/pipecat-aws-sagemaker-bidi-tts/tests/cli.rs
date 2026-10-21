#![cfg(target_os = "linux")]
mod common;
use common::*;
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    net::TcpListener,
    time::{sleep, timeout},
};
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "sagemaker-bidi-tts-cli-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, s: &str) -> PathBuf {
        self.0.join(s)
    }
    fn input(&self) -> PathBuf {
        let p = self.path("input.json");
        fs::write(&p, br#"["first", "second"]"#).unwrap();
        p
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct ChildOwner(Option<Child>);
impl Drop for ChildOwner {
    fn drop(&mut self) {
        if let Some(child) = &mut self.0 {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
fn command(origin: &str) -> Command {
    let binary = std::env::var_os("PIPECAT_SAGEMAKER_BIDI_TTS_TEST_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_sagemaker_bidi_tts").into());
    let mut c = Command::new(binary);
    c.env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("PIPECAT_SAGEMAKER_BIDI_TTS_REGION", "us-east-1")
        .env("PIPECAT_SAGEMAKER_BIDI_TTS_ENDPOINT_NAME", "test-endpoint")
        .env("PIPECAT_SAGEMAKER_BIDI_TTS_ACCESS_KEY_ID", ACCESS)
        .env("PIPECAT_SAGEMAKER_BIDI_TTS_SECRET_ACCESS_KEY", SECRET)
        .env("PIPECAT_SAGEMAKER_BIDI_TTS_SESSION_TOKEN", TOKEN)
        .env("PIPECAT_SAGEMAKER_BIDI_TTS_LOOPBACK_ORIGIN", origin);
    c
}
fn spawn(c: &mut Command) -> ChildOwner {
    ChildOwner(Some(c.spawn().unwrap()))
}
async fn collect(mut c: ChildOwner) -> std::process::Output {
    timeout(Duration::from_secs(10), async {
        loop {
            if c.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("finite CLI deadline");
    let output = c.0.take().unwrap().wait_with_output().unwrap();
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(diagnostic.len() < 12000);
    for s in [SECRET, TOKEN, ACCESS, "private-transcript", "attacker-body"] {
        assert!(!diagnostic.contains(s));
    }
    output
}
fn active(c: &mut Command, input: &Path, output: &Path) {
    c.arg("--run")
        .arg(input)
        .arg(output)
        .arg("--timeout-ms")
        .arg("3000");
}
#[tokio::test]
async fn help_and_check_have_no_network_or_credential_dependency() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", l.local_addr().unwrap());
    for args in [vec![], vec!["--help"], vec!["--check-config"]] {
        let mut c = command(&origin);
        c.args(args)
            .env_remove("PIPECAT_SAGEMAKER_BIDI_TTS_ACCESS_KEY_ID")
            .env_remove("PIPECAT_SAGEMAKER_BIDI_TTS_SECRET_ACCESS_KEY");
        let out = collect(spawn(&mut c)).await;
        assert!(out.status.success());
        assert!(timeout(Duration::from_millis(30), l.accept())
            .await
            .is_err());
    }
}
#[tokio::test]
async fn invalid_input_overwrite_alias_symlink_and_unknown_settings_preflight_before_network() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", l.local_addr().unwrap());
    for case in 0..8 {
        let dir = Directory::new();
        let mut input = dir.input();
        let mut output = dir.path("output.pcm");
        let mut c = command(&origin);
        match case {
            0 => fs::write(&input, []).unwrap(),
            1 => fs::write(&input, [1]).unwrap(),
            2 => fs::write(&output, b"existing").unwrap(),
            3 => output = input.clone(),
            4 => {
                let link = dir.path("link.json");
                std::os::unix::fs::symlink(&input, &link).unwrap();
                input = link;
            }
            5 => {
                std::os::unix::fs::symlink(&input, &output).unwrap();
            }
            6 => {
                c.env(
                    "PIPECAT_SAGEMAKER_BIDI_TTS_SETTINGS_JSON",
                    r#"{"unknown":1}"#,
                );
            }
            _ => {
                c.env(
                    "PIPECAT_SAGEMAKER_BIDI_TTS_SETTINGS_JSON",
                    r#"{"sample_rate_hz":0}"#,
                );
            }
        }
        active(&mut c, &input, &output);
        let out = collect(spawn(&mut c)).await;
        assert!(!out.status.success(), "case {case}");
        assert!(timeout(Duration::from_millis(30), l.accept())
            .await
            .is_err());
        if case == 2 {
            assert_eq!(fs::read(&output).unwrap(), b"existing");
        }
    }
}
#[tokio::test]
async fn ordinary_binary_two_utterances_signed_sessions_audio_and_report() {
    let (origin, peer) = peer_sessions(2, |i, request, mut respond| async move {
        let mut upload = Upload::new(request);
        startup(&mut upload, if i == 0 { "first" } else { "second" }).await;
        let mut stream = respond.send_response(response(), false).unwrap();
        let mut wire = pcm_event(&[i as u8 + 1, 0]);
        wire.extend(event(json!({"type":"synthesize_session.updated"})));
        send(&mut stream, &wire, false, 7).await;
        complete(&mut upload, &mut stream).await;
    })
    .await;
    let dir = Directory::new();
    let input = dir.input();
    let output = dir.path("output.pcm");
    let mut c = command(&origin);
    active(&mut c, &input, &output);
    let result = collect(spawn(&mut c)).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_eq!(fs::read(&output).unwrap(), [1, 0, 2, 0]);
    let report: Value = serde_json::from_slice(&result.stderr).unwrap();
    assert_eq!(report["terminal"], "completed_with_audio");
    assert_eq!(report["network_joined"], true);
    assert_eq!(report["recorded"], true);
    assert_eq!(report["utterances"].as_array().unwrap().len(), 2);
    assert_ne!(
        report["utterances"][0]["context_id"],
        report["utterances"][1]["context_id"]
    );
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        fs::metadata(output).unwrap().permissions().mode() & 0o777,
        0o600
    );
    peer.await.unwrap();
}
#[tokio::test]
async fn empty_audio_and_provider_failure_are_distinct_and_redacted() {
    for failure in [false, true] {
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut upload = Upload::new(request);
            startup(&mut upload, "hello").await;
            let mut stream = respond.send_response(response(), false).unwrap();
            if failure {
                send(
                    &mut stream,
                    &event(json!({"type":"error","message":"attacker-body"})),
                    false,
                    4096,
                )
                .await;
                sleep(Duration::from_millis(100)).await;
            } else {
                complete(&mut upload, &mut stream).await;
            }
        })
        .await;
        let dir = Directory::new();
        let input = dir.input();
        fs::write(&input, br#"["hello"]"#).unwrap();
        let output = dir.path("output.pcm");
        let mut c = command(&origin);
        active(&mut c, &input, &output);
        let out = collect(spawn(&mut c)).await;
        assert_eq!(out.status.success(), !failure);
        let report: Value = serde_json::from_slice(&out.stderr).unwrap();
        assert_eq!(
            report["terminal"],
            if failure {
                "failed"
            } else {
                "completed_without_audio"
            }
        );
        assert_eq!(fs::read(output).unwrap(), Vec::<u8>::new());
        peer.await.unwrap();
    }
}
