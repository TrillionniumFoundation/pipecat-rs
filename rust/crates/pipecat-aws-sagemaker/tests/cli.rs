//! Actual compiled CLI, explicit synthetic credentials, retained loopback listener
//! handles and joined child processes. No live AWS or physical playback claim.
#![cfg(target_os = "linux")]
use serde_json::{json, Value};
use std::{
    fs, io,
    net::SocketAddr,
    path::{Path, PathBuf},
    process::{Child, Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::oneshot,
    time::{sleep, timeout},
};

const WAIT: Duration = Duration::from_secs(10);
const ACCESS: &str = "SyntheticCliAccess";
const SECRET: &str = "synthetic-cli-secret-never-live";
const TOKEN: &str = "synthetic-cli-session-token";
const PRIVATE_TEXT: &str = "private-utterance-not-a-diagnostic";
const PRIVATE_BODY: &str = "private-provider-error-body";
struct Directory(PathBuf);
impl Directory {
    fn new() -> Self {
        static ID: AtomicU64 = AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "sagemaker-cli-{}-{}",
            std::process::id(),
            ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self, name: &str) -> PathBuf {
        self.0.join(name)
    }
    fn input(&self, value: Value) -> PathBuf {
        let path = self.path("input.json");
        fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
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
fn command(address: SocketAddr) -> Command {
    // This test-only override lets the resource owner replay this corpus against
    // the separately built NORMAL production binary, not a feature-unified test
    // binary. Production code has no testing switch or synthetic credentials.
    let binary = std::env::var_os("PIPECAT_SAGEMAKER_TEST_BINARY")
        .unwrap_or_else(|| env!("CARGO_BIN_EXE_sagemaker_tts").into());
    let mut command = Command::new(binary);
    command
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .env("PIPECAT_SAGEMAKER_REGION", "us-east-1")
        .env("PIPECAT_SAGEMAKER_ENDPOINT_NAME", "magpie-cli-fixture")
        .env("PIPECAT_SAGEMAKER_ACCESS_KEY_ID", ACCESS)
        .env("PIPECAT_SAGEMAKER_SECRET_ACCESS_KEY", SECRET)
        .env("PIPECAT_SAGEMAKER_SESSION_TOKEN", TOKEN)
        .env(
            "PIPECAT_SAGEMAKER_LOOPBACK_ORIGIN",
            format!("http://{address}"),
        );
    command
}
fn active(command: &mut Command, input: &Path, output: &Path) {
    command.arg("--run").arg(input).arg(output);
}
fn spawn(command: &mut Command) -> ChildOwner {
    ChildOwner(Some(command.spawn().unwrap()))
}
async fn collect(mut child: ChildOwner) -> Output {
    timeout(WAIT, async {
        loop {
            if child.0.as_mut().unwrap().try_wait().unwrap().is_some() {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("compiled SageMaker process exceeded bounded fixture window");
    child.0.take().unwrap().wait_with_output().unwrap()
}
fn sanitized(output: &Output) {
    let diagnostic = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    for secret in [ACCESS, SECRET, TOKEN, PRIVATE_TEXT, PRIVATE_BODY] {
        assert!(
            !diagnostic.contains(secret),
            "private fixture content leaked"
        );
    }
    assert!(diagnostic.len() < 12_000);
}
fn report(output: &Output) -> Value {
    assert!(
        output.stdout.is_empty(),
        "active mode must not mix output or PCM on stdout"
    );
    sanitized(output);
    let lines: Vec<_> = output
        .stderr
        .split(|b| *b == b'\n')
        .filter(|s| !s.is_empty())
        .collect();
    assert_eq!(
        lines.len(),
        1,
        "expected exactly one sanitized JSON diagnostic"
    );
    let report: Value = serde_json::from_slice(lines[0]).unwrap();
    assert!(report["status"].is_string());
    assert!(report["stage"].is_string());
    report
}
async fn listener() -> TcpListener {
    TcpListener::bind("127.0.0.1:0").await.unwrap()
}
async fn no_connection(listener: &TcpListener) {
    assert!(timeout(Duration::from_millis(40), listener.accept())
        .await
        .is_err());
}
async fn request(listener: &TcpListener) -> (TcpStream, Value) {
    request_with_settings(listener, "Magpie-Multilingual.EN-US.Aria", "en-US", 24_000).await
}
async fn request_with_settings(
    listener: &TcpListener,
    voice: &str,
    language: &str,
    rate: u32,
) -> (TcpStream, Value) {
    let (mut stream, _) = timeout(WAIT, listener.accept()).await.unwrap().unwrap();
    let mut header = Vec::new();
    while !header.ends_with(b"\r\n\r\n") {
        let mut one = [0];
        assert_eq!(
            timeout(WAIT, stream.read(&mut one)).await.unwrap().unwrap(),
            1
        );
        header.push(one[0]);
        assert!(
            header.len() <= 16_384,
            "fixture request header exceeded bound"
        );
    }
    let header = String::from_utf8(header).unwrap();
    let mut lines = header.split("\r\n");
    assert_eq!(
        lines.next().unwrap(),
        "POST /endpoints/magpie-cli-fixture/invocations HTTP/1.1"
    );
    let headers: std::collections::BTreeMap<_, _> = lines
        .filter(|line| !line.is_empty())
        .map(|line| {
            let (name, value) = line.split_once(':').unwrap();
            (name.to_ascii_lowercase(), value.trim().to_owned())
        })
        .collect();
    assert_eq!(headers["host"], listener.local_addr().unwrap().to_string());
    assert_eq!(headers["content-type"], "application/json");
    assert_eq!(headers["accept"], "application/octet-stream");
    assert!(!headers.contains_key("x-amzn-sagemaker-accept"));
    assert_eq!(headers["x-amz-security-token"], TOKEN);
    assert!(headers["authorization"].contains("/us-east-1/sagemaker/aws4_request"));
    let length: usize = headers["content-length"].parse().unwrap();
    assert!(length <= 262_144);
    let mut body = vec![0; length];
    timeout(WAIT, stream.read_exact(&mut body))
        .await
        .unwrap()
        .unwrap();
    let json: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json.as_object().unwrap().len(), 4);
    assert_eq!(json["voice_name"], voice);
    assert_eq!(json["language_code"], language);
    assert_eq!(json["sample_rate_hz"], rate);
    (stream, json)
}
async fn write(stream: &mut TcpStream, bytes: &[u8]) {
    timeout(WAIT, stream.write_all(bytes))
        .await
        .unwrap()
        .unwrap();
}
async fn closed(stream: &mut TcpStream) {
    let mut byte = [0];
    match timeout(WAIT, stream.read(&mut byte))
        .await
        .expect("client socket not released")
    {
        Ok(0) => {}
        Err(e)
            if matches!(
                e.kind(),
                io::ErrorKind::ConnectionReset | io::ErrorKind::BrokenPipe
            ) => {}
        other => panic!("unexpected client bytes after request: {other:?}"),
    }
}
async fn chunked(stream: &mut TcpStream, chunks: &[&[u8]]) {
    write(stream, b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n").await;
    for chunk in chunks {
        write(stream, format!("{:x}\r\n", chunk.len()).as_bytes()).await;
        write(stream, chunk).await;
        write(stream, b"\r\n").await;
        tokio::task::yield_now().await;
    }
    write(stream, b"0\r\n\r\n").await;
    closed(stream).await;
}
fn complete(output: &Output, file: &Path, expected: &[u8], utterances: usize) {
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    let report = report(output);
    assert_eq!(report["status"], "complete");
    assert_eq!(report["stage"], "joined");
    assert_eq!(report["recording_only"], true);
    assert_eq!(report["terminal_report_received"], true);
    assert_eq!(report["application_successful"], true);
    assert_eq!(report["primary_failure"], false);
    assert_eq!(report["cleanup_failures"], 0);
    assert_eq!(report["utterances_seen"], utterances);
    assert_eq!(report["completed_at_eof"], utterances);
    assert_eq!(report["owners_released"], utterances);
    assert_eq!(report["owners_joined"], true);
    assert_eq!(report["pipeline_joined"], true);
    assert_eq!(report["output_finalized"], true);
    assert_eq!(report["output_path_matches"], true);
    assert_eq!(report["input_unchanged"], true);
    assert_eq!(report["retained_file_flushed"], true);
    assert_eq!(report["recorded_bytes"], expected.len());
    assert_eq!(report["buffered_bytes"], expected.len());
    assert_eq!(report["pcm_bytes_delivered_to_memory"], expected.len());
    assert_eq!(fs::read(file).unwrap(), expected);
    use std::os::unix::fs::{MetadataExt, PermissionsExt};
    let metadata = fs::metadata(file).unwrap();
    assert_eq!(metadata.permissions().mode() & 0o777, 0o600);
    assert_eq!(report["output_inode"], metadata.ino());
    assert_eq!(report["output_device"], metadata.dev());
}

#[tokio::test]
async fn default_help_and_config_check_ignore_credentials_and_have_no_effects() {
    use std::os::unix::ffi::OsStringExt;
    let directory = Directory::new();
    let listener = listener().await;
    for args in [vec![], vec!["--help"], vec!["--check-config"]] {
        let mut command = command(listener.local_addr().unwrap());
        command.current_dir(&directory.0).args(args);
        for name in ["ACCESS_KEY_ID", "SECRET_ACCESS_KEY", "SESSION_TOKEN"] {
            command.env(
                format!("PIPECAT_SAGEMAKER_{name}"),
                std::ffi::OsString::from_vec(vec![0xff; 20_000]),
            );
        }
        let output = collect(spawn(&mut command)).await;
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert!(output.stderr.is_empty());
    }
    assert_eq!(fs::read_dir(&directory.0).unwrap().count(), 0);
    no_connection(&listener).await;
}
#[tokio::test]
async fn invalid_arguments_and_explicit_configuration_have_no_network_or_output() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":"hello"}]}));
    let output = directory.path("absent.pcm");
    let listener = listener().await;
    for (name, value) in [
        ("REGION", "cn-north-1"),
        ("ENDPOINT_NAME", "../wrong"),
        ("ACCESS_KEY_ID", ""),
        ("SECRET_ACCESS_KEY", "bad\nsecret"),
        ("SESSION_TOKEN", "bad\ntoken"),
        ("LOOPBACK_ORIGIN", "http://localhost:7"),
        ("LOOPBACK_ORIGIN", "http://192.0.2.1:7"),
        ("LOOPBACK_ORIGIN", "http://127.0.0.1:0"),
        ("LOOPBACK_ORIGIN", "http://127.0.0.1:7/path"),
        ("CONNECT_ADDRESS", "127.0.0.1:443"),
    ] {
        let mut command = command(listener.local_addr().unwrap());
        command.env(format!("PIPECAT_SAGEMAKER_{name}"), value);
        active(&mut command, &input, &output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        assert_eq!(report(&result)["status"], "rejected");
        assert!(!output.exists());
    }
    let mut missing = command(listener.local_addr().unwrap());
    missing
        .env_remove("PIPECAT_SAGEMAKER_ACCESS_KEY_ID")
        .env("AWS_ACCESS_KEY_ID", ACCESS)
        .env("AWS_SECRET_ACCESS_KEY", SECRET);
    active(&mut missing, &input, &output);
    assert!(!collect(spawn(&mut missing)).await.status.success());
    for args in [
        vec!["--run"],
        vec!["--help", "extra"],
        vec!["--check-config", "extra"],
        vec!["--unknown"],
        vec!["--run", "in", "out", "--timeout-ms", "0"],
        vec![
            "--run",
            "in",
            "out",
            "--timeout-ms",
            "100",
            "--timeout-ms",
            "100",
        ],
        vec!["--run", "in", "out", "--max-output-bytes", "3"],
        vec!["--run", "in", "out", "--max-output-bytes", "16777218"],
        vec!["--run", "in", "out", "--timeout-ms", "120001"],
    ] {
        let mut command = command(listener.local_addr().unwrap());
        command.args(args);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        report(&result);
    }
    assert!(!output.exists());
    no_connection(&listener).await;
}
#[tokio::test]
async fn entire_strict_bounded_utf8_json_preflights_before_file_or_connection() {
    let directory = Directory::new();
    let input = directory.path("input.json");
    let output = directory.path("absent.pcm");
    let listener = listener().await;
    let mut cases = vec![
        vec![],
        vec![0xff; 16],
        vec![b' '; 1024 * 1024 + 1],
        br#"{"utterances":[]}"#.to_vec(),
        br#"{"utterances":[{"text":"hello"},{"text":null}]}"#.to_vec(),
        br#"{"utterances":[{"text":"hello","headers":{}}]}"#.to_vec(),
        br#"{"utterances":[{"text":"hello"}],"url":"http://invalid"}"#.to_vec(),
        br#"{"utterances":[{"text":"hello"}],"sample_rate_hz":0}"#.to_vec(),
        br#"{"utterances":[{"text":"hello"}],"sample_rate_hz":96001}"#.to_vec(),
        br#"{"utterances":[{"text":"hello"}],"voice_name":"bad/voice"}"#.to_vec(),
        br#"{"utterances":[{"text":"hello"}],"language_code":null}"#.to_vec(),
        br#"{"utterances":[{"text":"hello","text":"duplicate"}]}"#.to_vec(),
    ];
    cases.push(format!("{}0{}", "[".repeat(256), "]".repeat(256)).into_bytes());
    cases.push(
        serde_json::to_vec(
            &json!({"utterances":(0..65).map(|_| json!({"text":"hello"})).collect::<Vec<_>>()}),
        )
        .unwrap(),
    );
    cases.push(
        serde_json::to_vec(&json!({"utterances":[{"text":"hello"},{"text":"x".repeat(65_537)}]}))
            .unwrap(),
    );
    for bytes in cases {
        fs::write(&input, bytes).unwrap();
        let mut command = command(listener.local_addr().unwrap());
        active(&mut command, &input, &output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        assert_eq!(report(&result)["status"], "rejected");
        assert!(!output.exists());
    }
    no_connection(&listener).await;
}
#[tokio::test]
async fn regular_files_no_overwrite_alias_symlink_and_traversal_preflight() {
    use std::os::unix::fs::symlink;
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":"hello"}]}));
    let original = fs::read(&input).unwrap();
    let output = directory.path("absent.pcm");
    let existing = directory.path("existing.pcm");
    fs::write(&existing, b"keep").unwrap();
    fs::create_dir(directory.path("real")).unwrap();
    symlink(&input, directory.path("input-link")).unwrap();
    symlink(&existing, directory.path("output-link")).unwrap();
    symlink(directory.path("real"), directory.path("parent-link")).unwrap();
    fs::hard_link(&input, directory.path("hard")).unwrap();
    let listener = listener().await;
    for (source, target) in [
        (input.clone(), input.clone()),
        (input.clone(), existing.clone()),
        (directory.path("input-link"), output.clone()),
        (input.clone(), directory.path("output-link")),
        (input.clone(), directory.path("hard")),
        (directory.0.clone(), output.clone()),
        (input.clone(), directory.path("parent-link/new")),
        (input.clone(), directory.path("real/../new")),
    ] {
        let mut command = command(listener.local_addr().unwrap());
        active(&mut command, &source, &target);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        report(&result);
    }
    assert_eq!(fs::read(&input).unwrap(), original);
    assert_eq!(fs::read(&existing).unwrap(), b"keep");
    assert!(!output.exists());
    no_connection(&listener).await;
}
#[tokio::test]
async fn actual_binary_two_fragmented_utterances_records_exact_pcm_after_join() {
    let directory = Directory::new();
    let input = directory
        .input(json!({"utterances":[{"text":format!(" {PRIVATE_TEXT} ")},{"text":"第二句话"}]}));
    let output = directory.path("output.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, body) = request(&listener).await;
        assert_eq!(body["text"], PRIVATE_TEXT);
        chunked(&mut first, &[&[1], &[2, 3], &[4]]).await;
        drop(first);
        let (mut second, body) = request(&listener).await;
        assert_eq!(body["text"], "第二句话");
        write(&mut second, b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 4\r\nConnection: close\r\n\r\n").await;
        for part in [&[5][..], &[6, 7][..], &[8][..]] {
            write(&mut second, part).await;
            tokio::task::yield_now().await;
        }
        closed(&mut second).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    complete(&result, &output, &[1, 2, 3, 4, 5, 6, 7, 8], 2);
    peer.await.unwrap();
}
#[tokio::test]
async fn all_skipped_input_is_distinct_from_audio_success_without_connecting() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":"   "},{"text":"!!! 😀"}]}));
    let output = directory.path("empty.pcm");
    let listener = listener().await;
    let mut command = command(listener.local_addr().unwrap());
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let report = report(&result);
    assert_eq!(report["status"], "all_skipped");
    assert_eq!(report["skipped"], 2);
    assert_eq!(report["completed_at_eof"], 0);
    assert_eq!(report["recorded_bytes"], 0);
    assert_eq!(report["owners_joined"], true);
    assert!(fs::read(&output).unwrap().is_empty());
    no_connection(&listener).await;
}
#[tokio::test]
async fn provider_error_after_completed_utterance_retains_exact_sanitized_partial() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":PRIVATE_TEXT},{"text":"second"}]}));
    let output = directory.path("partial.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = request(&listener).await;
        chunked(&mut first, &[&[1, 2, 3, 4]]).await;
        let (mut second, _) = request(&listener).await;
        write(&mut second, format!("HTTP/1.1 424 Failed Dependency\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{PRIVATE_BODY}", PRIVATE_BODY.len()).as_bytes()).await;
        closed(&mut second).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    assert!(!result.status.success());
    let report = report(&result);
    assert_eq!(report["status"], "partial");
    assert_eq!(report["application_successful"], false);
    assert_eq!(report["completed_at_eof"], 1);
    assert_eq!(report["owners_joined"], true);
    assert_eq!(report["output_finalized"], true);
    assert_eq!(report["recorded_bytes"], 4);
    assert_eq!(fs::read(&output).unwrap(), [1, 2, 3, 4]);
    peer.await.unwrap();
}
#[tokio::test]
async fn timeout_after_completed_utterance_closes_socket_and_keeps_partial() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":"first"},{"text":"second"}]}));
    let output = directory.path("partial.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = request(&listener).await;
        chunked(&mut first, &[&[1, 2, 3, 4]]).await;
        let (mut second, _) = request(&listener).await;
        // No headers or EOF: only the application's real finite timer can end.
        closed(&mut second).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    command.args(["--timeout-ms", "500"]);
    let result = collect(spawn(&mut command)).await;
    assert!(!result.status.success());
    let report = report(&result);
    assert_eq!(report["status"], "partial");
    assert_eq!(report["owners_joined"], true);
    assert_eq!(report["output_finalized"], true);
    assert_eq!(report["recorded_bytes"], 4);
    assert_eq!(fs::read(&output).unwrap(), [1, 2, 3, 4]);
    peer.await.unwrap();
}
#[tokio::test]
async fn output_limit_is_enforced_and_successfully_delivered_prefix_is_preserved() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":"first"},{"text":"second"}]}));
    let output = directory.path("partial.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut first, _) = request(&listener).await;
        chunked(&mut first, &[&[1, 2]]).await;
        let (mut second, _) = request(&listener).await;
        write(&mut second, b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 2\r\nConnection: close\r\n\r\n\x03\x04").await;
        closed(&mut second).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    command.args(["--max-output-bytes", "2"]);
    let result = collect(spawn(&mut command)).await;
    assert!(!result.status.success());
    let report = report(&result);
    assert_eq!(report["status"], "partial");
    assert_eq!(report["recorded_bytes"], 2);
    assert_eq!(report["buffer_capacity"], 2);
    assert_eq!(fs::read(&output).unwrap(), [1, 2]);
    peer.await.unwrap();
}

// Both actual pathname and parent substitutions occur only after a real first
// invocation completes and the second request is observed. That provides a
// causal streaming barrier, not a quiet-period assumption or dropped port race.
async fn swap_while_running(mode: u8) {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":PRIVATE_TEXT},{"text":"second"}]}));
    fs::create_dir(directory.path("parent")).unwrap();
    let output = directory.path("parent/output.pcm");
    let retained = if mode == 1 {
        directory.path("moved/output.pcm")
    } else {
        directory.path("retained.pcm")
    };
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let (observed_tx, observed) = oneshot::channel();
    let (resume, resumed) = oneshot::channel();
    let peer = tokio::spawn(async move {
        let (mut first, _) = request(&listener).await;
        chunked(&mut first, &[&[1, 2, 3, 4]]).await;
        let (mut second, body) = request(&listener).await;
        assert_eq!(body["text"], "second");
        observed_tx.send(()).unwrap();
        timeout(WAIT, resumed).await.unwrap().unwrap();
        chunked(&mut second, &[&[5], &[6, 7], &[8]]).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    let mut child = spawn(&mut command);
    timeout(WAIT, observed).await.unwrap().unwrap();
    assert!(child.0.as_mut().unwrap().try_wait().unwrap().is_none());
    assert_eq!(
        fs::metadata(&output).unwrap().len(),
        0,
        "filesystem write before application join"
    );
    if mode == 1 {
        fs::rename(directory.path("parent"), directory.path("moved")).unwrap();
        fs::create_dir(directory.path("parent")).unwrap();
    } else if mode == 0 {
        fs::rename(&output, &retained).unwrap();
    } else {
        fs::rename(&input, directory.path("original.json")).unwrap();
        fs::write(&input, b"replacement input must not be consumed").unwrap();
    }
    if mode != 2 {
        fs::write(&output, b"replacement must remain untouched").unwrap();
    }
    resume.send(()).unwrap();
    let result = collect(child).await;
    assert!(!result.status.success());
    let report = report(&result);
    assert_eq!(report["status"], "partial");
    assert_eq!(report["application_successful"], true);
    assert_eq!(report["owners_joined"], true);
    assert_eq!(report["recorded_bytes"], 8);
    assert_eq!(report["retained_file_flushed"], true);
    if mode == 2 {
        assert_eq!(report["stage"], "input_identity");
        assert_eq!(report["input_unchanged"], false);
        assert_eq!(fs::read(&output).unwrap(), [1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(
            fs::read(&input).unwrap(),
            b"replacement input must not be consumed"
        );
    } else {
        assert_eq!(report["stage"], "persistence");
        assert_eq!(report["output_finalized"], false);
        assert_eq!(report["output_path_matches"], false);
        assert_eq!(
            fs::read(&output).unwrap(),
            b"replacement must remain untouched"
        );
        assert_eq!(fs::read(&retained).unwrap(), [1, 2, 3, 4, 5, 6, 7, 8]);
        use std::os::unix::fs::MetadataExt;
        assert_eq!(
            report["output_inode"],
            fs::metadata(&retained).unwrap().ino()
        );
    }
    peer.await.unwrap();
}
#[tokio::test]
async fn actual_binary_output_swap_never_writes_replacement() {
    swap_while_running(0).await;
}
#[tokio::test]
async fn actual_binary_parent_swap_keeps_original_descriptor_and_fails_identity() {
    swap_while_running(1).await;
}
#[tokio::test]
async fn actual_binary_input_swap_does_not_replace_preflighted_utterances() {
    swap_while_running(2).await;
}

#[tokio::test]
async fn empty_odd_and_wrong_format_success_responses_are_failures_not_speech() {
    for response in [
        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/octet-stream\r\nContent-Length: 1\r\nConnection: close\r\n\r\n\x01".as_slice(),
        b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 2\r\nConnection: close\r\n\r\n{}".as_slice(),
    ] {
        let directory = Directory::new();
        let input = directory.input(json!({"utterances":[{"text":PRIVATE_TEXT}]}));
        let output = directory.path("partial.pcm");
        let listener = listener().await;
        let address = listener.local_addr().unwrap();
        let peer = tokio::spawn(async move {
            let (mut stream, _) = request(&listener).await;
            write(&mut stream, response).await;
            closed(&mut stream).await;
            no_connection(&listener).await;
        });
        let mut command = command(address);
        active(&mut command, &input, &output);
        let result = collect(spawn(&mut command)).await;
        assert!(!result.status.success());
        let report = report(&result);
        assert_eq!(report["status"], "failed");
        assert_eq!(report["pcm_bytes_delivered_to_memory"], 0);
        assert_eq!(report["completed_at_eof"], 0);
        assert_eq!(report["recorded_bytes"], 0);
        assert!(fs::read(&output).unwrap().is_empty());
        peer.await.unwrap();
    }
}

#[tokio::test]
async fn actual_binary_explicit_voice_language_rate_reach_wire_and_recording_sink() {
    let directory = Directory::new();
    let input = directory.input(json!({
        "voice_name":"fixture.voice", "language_code":"de-DE", "sample_rate_hz":16000,
        "utterances":[{"text":"Hallo"}]
    }));
    let output = directory.path("explicit.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, body) =
            request_with_settings(&listener, "fixture.voice", "de-DE", 16_000).await;
        assert_eq!(body["text"], "Hallo");
        chunked(&mut stream, &[&[1], &[2, 3], &[4]]).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    complete(&result, &output, &[1, 2, 3, 4], 1);
    peer.await.unwrap();
}

#[tokio::test]
async fn real_binary_write_failure_after_join_is_nonzero_and_keeps_original_empty_file() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":"write fault"}]}));
    let output = directory.path("write-failed.pcm");
    let listener = listener().await;
    let mut command = command(listener.local_addr().unwrap());
    active(&mut command, &input, &output);
    // Test-only launcher changes this child, then execs the actual binary at the
    // same retained PID. Production invokes no shell and has no fault hook.
    // Ignored XFSZ makes its real write return EFBIG instead of killing it.
    let mut constrained = Command::new("/bin/sh");
    constrained
        .env_clear()
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    constrained
        .arg("-c")
        .arg("trap '' XFSZ; ulimit -f 0; exec \"$@\"")
        .arg("sagemaker-write-fault")
        .arg(command.get_program())
        .args(command.get_args());
    for (key, value) in command.get_envs() {
        if let Some(value) = value {
            constrained.env(key, value);
        }
    }
    command = constrained;
    let child = spawn(&mut command);
    let (mut socket, body) = request(&listener).await;
    assert_eq!(body["text"], "write fault");
    chunked(&mut socket, &[&[1, 2], &[3, 4]]).await;
    let result = collect(child).await;
    assert!(!result.status.success());
    let summary = report(&result);
    assert_eq!(summary["status"], "partial");
    assert_eq!(summary["stage"], "persistence");
    assert_eq!(summary["application_successful"], true);
    assert_eq!(summary["terminal_report_received"], true);
    assert_eq!(summary["pipeline_joined"], true);
    assert_eq!(summary["owners_joined"], true);
    assert_eq!(summary["output_finalized"], false);
    assert_eq!(summary["retained_file_flushed"], false);
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::metadata(&output).unwrap();
    assert_eq!(summary["output_inode"], metadata.ino());
    assert_eq!(summary["output_device"], metadata.dev());
    assert_eq!(summary["recorded_bytes"], 0);
    assert_eq!(summary["buffered_bytes"], 4);
    assert_eq!(summary["pcm_bytes_delivered_to_memory"], 4);
    assert_eq!(summary["output_path_matches"], true);
    assert!(fs::read(&output).unwrap().is_empty());
    no_connection(&listener).await;
}

#[tokio::test]
async fn initial_provider_failure_is_failed_without_claiming_partial_pcm() {
    let directory = Directory::new();
    let input = directory.input(json!({"utterances":[{"text":PRIVATE_TEXT}]}));
    let output = directory.path("failed.pcm");
    let listener = listener().await;
    let address = listener.local_addr().unwrap();
    let peer = tokio::spawn(async move {
        let (mut stream, _) = request(&listener).await;
        write(&mut stream, format!("HTTP/1.1 424 Failed Dependency\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{PRIVATE_BODY}", PRIVATE_BODY.len()).as_bytes()).await;
        closed(&mut stream).await;
        no_connection(&listener).await;
    });
    let mut command = command(address);
    active(&mut command, &input, &output);
    let result = collect(spawn(&mut command)).await;
    assert!(!result.status.success());
    let summary = report(&result);
    assert_eq!(summary["status"], "failed");
    assert_eq!(summary["primary_failure"], true);
    assert_eq!(summary["owners_joined"], true);
    assert_eq!(summary["failed"], 1);
    assert_eq!(summary["recorded_bytes"], 0);
    assert_eq!(summary["pcm_bytes_delivered_to_memory"], 0);
    assert!(fs::read(&output).unwrap().is_empty());
    peer.await.unwrap();
}
