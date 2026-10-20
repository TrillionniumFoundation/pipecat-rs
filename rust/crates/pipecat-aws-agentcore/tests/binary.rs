//! Run the actual compiled executable with a cleared environment and synthetic
//! namespaced configuration. No SDK, ambient AWS profile, or live service is used.
mod common;
use common::*;
use serde_json::{json, Value};
use std::{
    fs,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    time::Duration,
};
use tokio::{
    io::AsyncWriteExt,
    sync::oneshot,
    time::{sleep, timeout},
};

struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        let path =
            std::env::temp_dir().join(format!("pipecat-agentcore-binary-{}", uuid::Uuid::new_v4()));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn input(&self, value: &Value) -> PathBuf {
        let path = self.path("input.json");
        fs::write(&path, serde_json::to_vec(value).unwrap()).unwrap();
        path
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
fn command(origin: Option<&str>) -> Command {
    let mut command = Command::new(env!("CARGO_BIN_EXE_agentcore_text"));
    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    if let Some(origin) = origin {
        command
            .env("PIPECAT_AGENTCORE_REGION", "us-east-1")
            .env("PIPECAT_AGENTCORE_RUNTIME_ARN", ARN)
            .env("PIPECAT_AGENTCORE_ACCESS_KEY_ID", ACCESS)
            .env("PIPECAT_AGENTCORE_SECRET_ACCESS_KEY", SECRET)
            .env("PIPECAT_AGENTCORE_SESSION_TOKEN", TOKEN)
            .env("PIPECAT_AGENTCORE_LOOPBACK_ORIGIN", origin);
    }
    command
}
fn spawn(command: &mut Command) -> ChildOwner {
    ChildOwner(Some(command.spawn().unwrap()))
}
async fn collect(mut owner: ChildOwner) -> Output {
    timeout(WAIT, async {
        loop {
            if owner.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("actual executable did not exit within bounded fixture window");
    owner.0.take().unwrap().wait_with_output().unwrap()
}
fn events(path: &Path) -> Vec<Value> {
    fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect()
}
fn input(prompt: &str) -> Value {
    json!({"contexts":[{"messages":[{"role":"user","content":prompt}]}]})
}
fn assert_no_secrets(output: &Output) {
    let messages = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [
        SECRET,
        TOKEN,
        ACCESS,
        ARN,
        "private-prompt",
        "private-response",
    ] {
        assert!(
            !messages.contains(secret),
            "process diagnostic leaked a fixture secret"
        );
    }
}
fn assert_complete(records: &[Value], count: u64) {
    let report = records.last().unwrap();
    assert_eq!(report["event"], "report");
    assert_eq!(report["complete"], true);
    assert_eq!(report["partial"], false);
    assert_eq!(report["pipeline_joined"], true);
    assert_eq!(report["invocations"], count);
    assert_eq!(report["completed_at_eof"], count);
    assert_eq!(report["owners_released"], count);
}
#[tokio::test]
async fn help_and_check_config_have_no_network_or_file_effects() {
    let directory = Directory::new();
    let listener = listener().await;
    let mut help = command(None);
    help.current_dir(&directory.0).arg("--help");
    let help = collect(spawn(&mut help)).await;
    assert!(help.status.success());
    let text = String::from_utf8(help.stdout).unwrap();
    assert!(text.contains("--check-config"));
    assert!(text.contains("--run"));
    assert!(text.contains("PIPECAT_AGENTCORE_"));
    let mut check = command(Some(&origin(&listener)));
    check.current_dir(&directory.0).arg("--check-config");
    let checked = collect(spawn(&mut check)).await;
    assert!(
        checked.status.success(),
        "{}",
        String::from_utf8_lossy(&checked.stderr)
    );
    assert!(String::from_utf8_lossy(&checked.stdout).contains("no network"));
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
    no_retry(&listener).await;
}
#[tokio::test]
async fn missing_and_invalid_configuration_or_arguments_cannot_create_output() {
    let directory = Directory::new();
    let input = directory.input(&input("private-prompt"));
    let output = directory.path("must-not-exist.jsonl");
    let listener = listener().await;
    let mut cases = vec![command(None)];
    for (name, value) in [
        ("PIPECAT_AGENTCORE_REGION", "us-west-2"),
        ("PIPECAT_AGENTCORE_RUNTIME_ARN", "agent-id-is-not-arn"),
        ("PIPECAT_AGENTCORE_LOOPBACK_ORIGIN", "http://localhost:1"),
        ("PIPECAT_AGENTCORE_LOOPBACK_ORIGIN", "http://192.0.2.1:1"),
        ("PIPECAT_AGENTCORE_CONNECT_ADDRESS", "127.0.0.1:443"),
        ("PIPECAT_AGENTCORE_ACCESS_KEY_ID", ""),
    ] {
        let mut value_command = command(Some(&origin(&listener)));
        value_command.env(name, value);
        cases.push(value_command);
    }
    for mut command in cases {
        command.arg("--run").arg(&input).arg(&output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        assert!(!output.exists());
        assert_no_secrets(&result);
    }
    for args in [
        vec!["--unknown"],
        vec!["--run"],
        vec!["--check-config", "extra"],
        vec!["--help", "a", "b", "c"],
    ] {
        let mut command = command(Some(&origin(&listener)));
        command.args(args);
        assert!(!collect(spawn(&mut command)).await.status.success());
    }
    no_retry(&listener).await;
}
#[tokio::test]
async fn entire_input_preflight_rejects_malformed_unsupported_and_excessive_files() {
    let directory = Directory::new();
    let listener = listener().await;
    let input_path = directory.path("input.json");
    let output = directory.path("must-not-exist.jsonl");
    let bad_inputs = vec![
        b"{".to_vec(),
        serde_json::to_vec(&json!({"contexts":[]})).unwrap(),
        serde_json::to_vec(&json!({"contexts":[{"messages":[{"role":"user","content":"valid first"}]},{"messages":[{"role":"tool","content":"unsupported"}]}]})).unwrap(),
        serde_json::to_vec(&json!({"contexts":[{"messages":[{"role":"user","content":[{"type":"image","text":"unsupported"}]}]}]})).unwrap(),
        serde_json::to_vec(&json!({"contexts":[{"messages":[{"role":"user","content":"private-prompt"}],"tools":[]}]})).unwrap(),
        serde_json::to_vec(&json!({"contexts":[{"messages":[{"role":"user","content":"private-prompt","name":"unsupported"}]}]})).unwrap(),
        serde_json::to_vec(&json!({"contexts":[{"messages":[{"role":"user","content":"x".repeat(65_537)}]}]})).unwrap(),
        vec![b' '; 1024 * 1024 + 1],
    ];
    for bytes in bad_inputs {
        fs::write(&input_path, bytes).unwrap();
        let mut command = command(Some(&origin(&listener)));
        command.arg("--run").arg(&input_path).arg(&output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        assert!(!output.exists());
        assert_no_secrets(&result);
    }
    no_retry(&listener).await;
}
#[cfg(unix)]
#[tokio::test]
async fn alias_symlink_directory_and_existing_output_preflights_preserve_files() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    let listener = listener().await;
    let input = directory.input(&input("private-prompt"));
    let original = fs::read(&input).unwrap();
    let existing = directory.path("existing.jsonl");
    fs::write(&existing, b"keep existing bytes").unwrap();
    let input_link = directory.path("input-link.json");
    symlink(&input, &input_link).unwrap();
    let output_link = directory.path("output-link.jsonl");
    symlink(&existing, &output_link).unwrap();
    let hardlink = directory.path("hard-link.jsonl");
    fs::hard_link(&input, &hardlink).unwrap();
    let new_output = directory.path("new.jsonl");
    for (source, target) in [
        (&input, &input),
        (&input, &existing),
        (&input, &output_link),
        (&input_link, &new_output),
        (&input, &hardlink),
        (&directory.0, &new_output),
    ] {
        let mut command = command(Some(&origin(&listener)));
        command.arg("--run").arg(source).arg(target);
        assert!(!collect(spawn(&mut command)).await.status.success());
    }
    assert_eq!(fs::read(&input).unwrap(), original);
    assert_eq!(fs::read(&existing).unwrap(), b"keep existing bytes");
    assert!(!new_output.exists());
    no_retry(&listener).await;
}
#[tokio::test]
async fn actual_binary_json_records_last_message_and_authenticated_wire() {
    let directory = Directory::new();
    let listener = listener().await;
    let endpoint = origin(&listener);
    let host = listener.local_addr().unwrap().to_string();
    let input = directory.input(&json!({"contexts":[{"messages":[{"role":"system","content":"never replayed"},{"role":"assistant","content":"not appended"},{"role":"user","content":[{"type":"text","text":""},{"type":"text","text":"private-prompt"},{"type":"text","text":""}]}]}]}));
    let output = directory.path("result.jsonl");
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        let request = read_request(&mut socket).await;
        verify_request(
            &request,
            &host,
            " private-prompt ",
            ACCESS,
            SECRET,
            Some(TOKEN),
        );
        response(
            &mut socket,
            "application/json",
            br#"{"response":"private-response"}"#,
            1,
        )
        .await;
        peer_eof(&mut socket).await;
        no_retry(&listener).await;
    });
    let mut command = command(Some(&endpoint));
    command.arg("--run").arg(&input).arg(&output);
    let result = collect(spawn(&mut command)).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    assert_no_secrets(&result);
    let records = events(&output);
    assert_eq!(
        records[..3],
        [
            json!({"event":"start"}),
            json!({"event":"text","text":"private-response"}),
            json!({"event":"end"})
        ]
    );
    assert_complete(&records, 1);
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            fs::metadata(&output).unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_sse_quiet_gap_does_not_finish_early_or_overlap_contexts() {
    let directory = Directory::new();
    let listener = listener().await;
    let endpoint = origin(&listener);
    let input = directory.input(&json!({"contexts":[{"messages":[{"role":"user","content":"one"}]},{"messages":[{"role":"user","content":"two"}]}]}));
    let output = directory.path("result.jsonl");
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = listener.accept().await.unwrap();
        let one = read_request(&mut first).await;
        head(&mut first, "text/event-stream").await;
        for byte in "data: {\"response\":\"雪\"}\r\n".as_bytes() {
            chunk(&mut first, &[*byte]).await;
        }
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        no_retry(&listener).await;
        chunk(&mut first, b"data: {\"response\":\"late\"}").await;
        end_chunks(&mut first).await;
        peer_eof(&mut first).await;
        let (mut second, _) = listener.accept().await.unwrap();
        let two = read_request(&mut second).await;
        assert_ne!(one.headers[SESSION], two.headers[SESSION]);
        response(
            &mut second,
            "text/event-stream",
            b"data: {\"response\":\"second\"}\ndata: {\"response\":\"adjacent\"}\n",
            1000,
        )
        .await;
        peer_eof(&mut second).await;
        no_retry(&listener).await;
    });
    let mut command = command(Some(&endpoint));
    command.arg("--run").arg(&input).arg(&output);
    let mut child = spawn(&mut command);
    timeout(WAIT, ready_rx).await.unwrap().unwrap();
    timeout(WAIT, async {
        loop {
            if fs::read_to_string(&output).ok().is_some_and(|text| {
                text.lines()
                    .filter_map(|line| serde_json::from_str::<Value>(line).ok())
                    .any(|record| record["event"] == "end")
            }) {
                break;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        child.0.as_mut().unwrap().try_wait().unwrap().is_none(),
        "binary exited at quiet gap"
    );
    assert_eq!(
        events(&output),
        [
            json!({"event":"start"}),
            json!({"event":"text","text":"雪"}),
            json!({"event":"end"})
        ]
    );
    release_tx.send(()).unwrap();
    let result = collect(child).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let records = events(&output);
    assert_eq!(
        records[..10],
        [
            json!({"event":"start"}),
            json!({"event":"text","text":"雪"}),
            json!({"event":"end"}),
            json!({"event":"start"}),
            json!({"event":"text","text":"late"}),
            json!({"event":"end"}),
            json!({"event":"start"}),
            json!({"event":"text","text":"second"}),
            json!({"event":"text","text":"adjacent"}),
            json!({"event":"end"})
        ]
    );
    assert_complete(&records, 2);
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_service_and_parser_failure_keep_explicit_partial_reports() {
    for (status, mime, body) in [
        ("503 Unavailable", "application/json", "private-response"),
        (
            "200 OK",
            "text/event-stream",
            "data: {\"response\":\"partial\"}\ndata: [DONE]\n",
        ),
        ("200 OK", "application/json", "{\"response\":42}"),
    ] {
        let directory = Directory::new();
        let listener = listener().await;
        let endpoint = origin(&listener);
        let input = directory.input(&input("private-prompt"));
        let output = directory.path("partial.jsonl");
        let peer = tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            read_request(&mut socket).await;
            socket.write_all(format!("HTTP/1.1 {status}\r\nContent-Type: {mime}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).as_bytes()).await.unwrap();
            peer_closed(&mut socket).await;
            no_retry(&listener).await;
        });
        let mut command = command(Some(&endpoint));
        command.arg("--run").arg(&input).arg(&output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        assert_no_secrets(&result);
        assert!(String::from_utf8_lossy(&result.stderr).contains("partial"));
        let records = events(&output);
        let report = records.last().unwrap();
        assert_eq!(report["event"], "report");
        assert_eq!(report["complete"], false);
        assert_eq!(report["partial"], true);
        assert_eq!(report["pipeline_joined"], true);
        assert_eq!(report["owners_released"], report["invocations"]);
        peer.await.unwrap();
    }
}
#[tokio::test]
async fn actual_binary_output_identity_failure_reaps_without_overwrite_or_retry() {
    let directory = Directory::new();
    let listener = listener().await;
    let endpoint = origin(&listener);
    let input = directory.input(&input("prompt"));
    let output = directory.path("output.jsonl");
    let moved = directory.path("retained-partial.jsonl");
    let (ready_tx, ready_rx) = oneshot::channel();
    let (release_tx, release_rx) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.unwrap();
        read_request(&mut socket).await;
        ready_tx.send(()).unwrap();
        release_rx.await.unwrap();
        response(
            &mut socket,
            "application/json",
            br#"{"response":"not-written-to-replacement"}"#,
            100,
        )
        .await;
        peer_closed(&mut socket).await;
        no_retry(&listener).await;
    });
    let mut command = command(Some(&endpoint));
    command.arg("--run").arg(&input).arg(&output);
    let child = spawn(&mut command);
    timeout(WAIT, ready_rx).await.unwrap().unwrap();
    fs::rename(&output, &moved).unwrap();
    fs::write(&output, b"replacement stays untouched").unwrap();
    release_tx.send(()).unwrap();
    let result = collect(child).await;
    assert!(!result.status.success());
    assert_no_secrets(&result);
    assert_eq!(fs::read(&output).unwrap(), b"replacement stays untouched");
    assert!(moved.exists());
    assert!(events(&moved).iter().all(|event| event["complete"] != true));
    peer.await.unwrap();
}
