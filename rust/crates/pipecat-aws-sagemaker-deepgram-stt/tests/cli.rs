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
            "sagemaker-deepgram-stt-cli-{}-{}",
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
        let p = self.path("input.pcm");
        fs::write(&p, vec![17; 6400]).unwrap();
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
    let binary = std::env::var_os("PIPECAT_SAGEMAKER_DEEPGRAM_STT_BIN")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_sagemaker_deepgram_stt").into());
    let mut c = Command::new(binary);
    c.env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("PIPECAT_SAGEMAKER_DEEPGRAM_REGION", "us-east-1")
        .env("PIPECAT_SAGEMAKER_DEEPGRAM_ENDPOINT_NAME", "test-endpoint")
        .env("PIPECAT_SAGEMAKER_DEEPGRAM_ACCESS_KEY_ID", ACCESS)
        .env("PIPECAT_SAGEMAKER_DEEPGRAM_SECRET_ACCESS_KEY", SECRET)
        .env("PIPECAT_SAGEMAKER_DEEPGRAM_SESSION_TOKEN", TOKEN)
        .env("PIPECAT_SAGEMAKER_DEEPGRAM_LOOPBACK_ORIGIN", origin);
    c
}
fn spawn(c: &mut Command) -> ChildOwner {
    ChildOwner(Some(c.spawn().unwrap()))
}
async fn collect(mut c: ChildOwner) -> std::process::Output {
    timeout(Duration::from_secs(40), async {
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
    c.arg("--run").arg(input).arg(output);
}
fn records(path: &Path) -> Vec<Value> {
    fs::read(path)
        .unwrap()
        .split(|&b| b == b'\n')
        .filter(|s| !s.is_empty())
        .map(|s| serde_json::from_slice(s).unwrap())
        .collect()
}
#[tokio::test]
async fn help_and_check_have_no_network_or_credential_dependency() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", l.local_addr().unwrap());
    for args in [vec![], vec!["--help"], vec!["--check-config"]] {
        let mut c = command(&origin);
        c.args(args)
            .env_remove("PIPECAT_SAGEMAKER_DEEPGRAM_ACCESS_KEY_ID")
            .env_remove("PIPECAT_SAGEMAKER_DEEPGRAM_SECRET_ACCESS_KEY");
        let out = collect(spawn(&mut c)).await;
        assert!(out.status.success());
        assert!(timeout(Duration::from_millis(30), l.accept())
            .await
            .is_err());
    }
}
#[tokio::test]
async fn normal_executable_exact_pcm_config_signed_stream_and_jsonl() {
    let (origin, peer) = peer(|request, mut response_head| async move {
        let mut u = Upload::new(request);
        u.finish().await;
        assert_pcm(&u.events, &vec![17; 6400]);
        let mut stream = response_head.send_response(response(), false).unwrap();
        let mut bytes = event(result("private-transcript", false, false));
        bytes.extend(event(result("final result", true, false)));
        // Eager one-byte DATA floods can exhaust h2's independent framing-
        // overhead budget before the EventStream consumer runs. Keep this
        // integration burst below 128 small DATA frames; bounded exhaustive
        // bytewise segmentation remains covered by the transport unit test.
        assert!(bytes.len().div_ceil(7) < 128);
        send(&mut stream, &bytes, true, 7).await;
    })
    .await;
    let dir = Directory::new();
    let input = dir.input();
    let output = dir.path("out.jsonl");
    let mut c = command(&origin);
    let settings = dir.path("settings.json");
    fs::write(&settings, br#"{"settings":{"user_id":"speaker"}}"#).unwrap();
    active(&mut c, &input, &output);
    c.arg("--settings").arg(&settings);
    let out = collect(spawn(&mut c)).await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = records(&output);
    assert_eq!(lines.len(), 3);
    assert_eq!(lines[0]["kind"], "interim");
    assert_eq!(lines[0]["text"], "private-transcript");
    assert_eq!(lines[1]["kind"], "final");
    assert_eq!(lines[1]["user_id"], "speaker");
    assert_eq!(lines[2]["terminal"], "completed_with_transcript");
    assert_eq!(lines[2]["progress"]["pcm_bytes_uploaded"], 6400);
    peer.await.unwrap();
}
#[tokio::test]
async fn invalid_input_overwrite_alias_symlink_and_unknown_settings_preflight_before_network() {
    let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", l.local_addr().unwrap());
    for case in 0..8 {
        let dir = Directory::new();
        let mut input = dir.input();
        let mut output = dir.path("output.jsonl");
        let mut c = command(&origin);
        match case {
            0 => fs::write(&input, []).unwrap(),
            1 => fs::write(&input, [1]).unwrap(),
            2 => fs::write(&output, b"existing").unwrap(),
            3 => output = input.clone(),
            4 => {
                let link = dir.path("link.pcm");
                std::os::unix::fs::symlink(&input, &link).unwrap();
                input = link;
            }
            5 => {
                std::os::unix::fs::symlink(&input, &output).unwrap();
            }
            _ => {}
        }
        active(&mut c, &input, &output);
        if case >= 6 {
            let settings = dir.path("settings.json");
            fs::write(
                &settings,
                if case == 6 {
                    br#"{"unknown":1}"#.as_slice()
                } else {
                    br#"{"settings":{"sample_rate_hz":0}}"#
                },
            )
            .unwrap();
            c.arg("--settings").arg(settings);
        }
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
async fn no_transcript_and_partial_error_have_distinct_terminal_records() {
    for partial in [false, true] {
        let (origin, peer) = peer(move |request, mut respond| async move {
            let mut u = Upload::new(request);
            u.finish().await;
            let mut stream = respond.send_response(response(), false).unwrap();
            if partial {
                send(
                    &mut stream,
                    &event(result("prefix", true, false)),
                    false,
                    4096,
                )
                .await;
                sleep(Duration::from_millis(60)).await;
                send(
                    &mut stream,
                    &event(json!({"type":"Error","message":"attacker-body"})),
                    true,
                    4096,
                )
                .await;
            } else {
                send(&mut stream, &[], true, 1).await;
            }
        })
        .await;
        let dir = Directory::new();
        let input = dir.input();
        let output = dir.path("output.jsonl");
        let mut c = command(&origin);
        active(&mut c, &input, &output);
        let out = collect(spawn(&mut c)).await;
        assert_eq!(out.status.success(), !partial);
        let lines = records(&output);
        assert_eq!(
            lines.last().unwrap()["terminal"],
            if partial {
                "partial_failure"
            } else {
                "completed_without_transcript"
            }
        );
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn path_substitution_never_writes_replacement_or_reports_success() {
    let dir = Directory::new();
    let input = dir.input();
    let output = dir.path("output.jsonl");
    let old = dir.path("original.jsonl");
    let output_peer = output.clone();
    let old_peer = old.clone();
    let (origin, peer) = peer(move |request, mut respond| async move {
        let mut u = Upload::new(request);
        u.finish().await;
        fs::rename(&output_peer, &old_peer).unwrap();
        fs::write(&output_peer, b"replacement").unwrap();
        let mut stream = respond.send_response(response(), false).unwrap();
        send(&mut stream, &event(result("result", true, false)), true, 7).await;
    })
    .await;
    let mut c = command(&origin);
    active(&mut c, &input, &output);
    let out = collect(spawn(&mut c)).await;
    assert!(!out.status.success());
    assert_eq!(fs::read(&output).unwrap(), b"replacement");
    assert!(fs::read(&old).unwrap().is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn real_file_write_failure_after_network_join_is_nonzero() {
    let (origin, peer) = peer(|request, mut respond| async move {
        let mut u = Upload::new(request);
        u.finish().await;
        let mut stream = respond.send_response(response(), false).unwrap();
        send(
            &mut stream,
            &event(result("result", true, false)),
            true,
            1024,
        )
        .await;
    })
    .await;
    let dir = Directory::new();
    let input = dir.input();
    let output = dir.path("output.jsonl");
    let mut c = command(&origin);
    active(&mut c, &input, &output);
    let mut constrained = Command::new("/bin/sh");
    constrained
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .arg("-c")
        .arg("trap '' XFSZ; ulimit -f 0; exec \"$@\"")
        .arg("stt-write-fault")
        .arg(c.get_program())
        .args(c.get_args());
    for (k, v) in c.get_envs() {
        if let Some(v) = v {
            constrained.env(k, v);
        }
    }
    let out = collect(spawn(&mut constrained)).await;
    assert!(!out.status.success());
    let r: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(r["network_joined"], true);
    assert_eq!(r["recorded"], false);
    assert!(fs::read(&output).unwrap().is_empty());
    peer.await.unwrap();
}

#[tokio::test]
async fn normal_binary_real_pause_keepalive_finalize_and_no_early_timer() {
    let (origin, peer) = peer(|request, mut respond| async move {
        let mut upload = Upload::new(request);
        assert!(upload.next().await.unwrap().get("binary").is_some());
        let start = tokio::time::Instant::now();
        let mut stream = respond.send_response(response(), false).unwrap();
        let mut controls = vec![];
        loop {
            let value = upload.next().await.unwrap();
            if value.get("binary").is_some() {
                break;
            }
            assert_eq!(value["type"], "KeepAlive");
            controls.push(start.elapsed());
        }
        assert!(controls.len() >= 2, "{controls:?}");
        assert!(controls[0] >= Duration::from_secs(5));
        assert!(controls[1] >= Duration::from_secs(10));
        assert_eq!(upload.next().await.unwrap()["type"], "Finalize");
        upload.finish().await;
        assert_pcm(&upload.events, &vec![17; 6400]);
        send(
            &mut stream,
            &event(result("confirmed", true, true)),
            true,
            4096,
        )
        .await;
    })
    .await;
    let dir = Directory::new();
    let input = dir.input();
    let output = dir.path("out.jsonl");
    let settings = dir.path("settings.json");
    fs::write(&settings,br#"{"timeline":[{"after_chunk":1,"pause_ms":12000},{"after_chunk":2,"vad_stopped":true}]}"#).unwrap();
    let mut c = command(&origin);
    active(&mut c, &input, &output);
    c.arg("--settings").arg(&settings);
    let out = collect(spawn(&mut c)).await;
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let lines = records(&output);
    assert_eq!(lines[0]["kind"], "final");
    assert_eq!(lines[0]["finalized"], true);
    assert!(
        lines.last().unwrap()["progress"]["keepalive_written"]
            .as_u64()
            .unwrap()
            >= 2
    );
    peer.await.unwrap();
}

#[tokio::test]
async fn strict_manifest_paths_shapes_and_aliases_reject_before_output_or_network() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}/", listener.local_addr().unwrap());
    for case in 0..12 {
        let dir = Directory::new();
        let input = dir.input();
        let output = dir.path("out.jsonl");
        let settings = dir.path("settings.json");
        match case {
            0 => fs::write(&settings, b"[]").unwrap(),
            1 => fs::write(&settings, br#"{"settings":[]}"#).unwrap(),
            2 => fs::write(&settings, br#"{"timeline":[[1,0,true]]}"#).unwrap(),
            3 => fs::write(&settings, br#"{"timeline":[{"after_chunk":0}]}"#).unwrap(),
            4 => fs::write(
                &settings,
                br#"{"timeline":[{"after_chunk":1},{"after_chunk":1}]}"#,
            )
            .unwrap(),
            5 => fs::write(&settings, br#"{"settings":{"mip_opt_out":null}}"#).unwrap(),
            6 => fs::write(&settings, br#"{"timeline":[],"timeline":[]}"#).unwrap(),
            7 => fs::write(&settings, vec![b' '; 16385]).unwrap(),
            8 => std::os::unix::fs::symlink(&input, &settings).unwrap(),
            9 => fs::hard_link(&input, &settings).unwrap(),
            10 => fs::write(
                &settings,
                br#"{"timeline":[{"after_chunk":1,"pause_ms":15001}]}"#,
            )
            .unwrap(),
            _ => fs::write(&settings, br#"{"timeline":[{"after_chunk":3}]}"#).unwrap(),
        }
        let mut c = command(&origin);
        active(&mut c, &input, &output);
        c.arg("--settings").arg(settings);
        let out = collect(spawn(&mut c)).await;
        assert!(!out.status.success(), "case{case}");
        assert!(!output.exists());
        assert!(timeout(Duration::from_millis(30), listener.accept())
            .await
            .is_err());
    }
}

#[tokio::test]
async fn settings_identity_substitution_cannot_report_success() {
    let dir = Directory::new();
    let input = dir.input();
    let output = dir.path("out.jsonl");
    let settings = dir.path("settings.json");
    fs::write(&settings, b"{}").unwrap();
    let changed = settings.clone();
    let (origin, peer) = peer(move |request, mut respond| async move {
        let mut upload = Upload::new(request);
        upload.finish().await;
        fs::remove_file(&changed).unwrap();
        fs::write(&changed, b"{}").unwrap();
        let mut stream = respond.send_response(response(), false).unwrap();
        send(&mut stream, &[], true, 1).await;
    })
    .await;
    let mut c = command(&origin);
    active(&mut c, &input, &output);
    c.arg("--settings").arg(settings);
    let out = collect(spawn(&mut c)).await;
    assert!(!out.status.success());
    let status: Value = serde_json::from_slice(&out.stderr).unwrap();
    assert_eq!(status["input_identity_preserved"], false);
    peer.await.unwrap();
}
