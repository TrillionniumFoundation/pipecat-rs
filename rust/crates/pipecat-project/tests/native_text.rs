//! Acceptance of the independently generated, compiled text-eval application.
//! The expensive test inherits build serialization from its caller. Child Cargo
//! invocations must not try to acquire that caller's global build lock again.
use pipecat_evals::{
    process::ProcessOptions,
    release::{run_release, ReleaseManifest, ReleaseResult},
    Cancellation, ScenarioFile, ScriptScenario, SessionOptions,
};
use serde_json::{json, Value};
use std::{
    fs, io,
    net::{SocketAddr, TcpListener as StdListener},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const TEST_NAME: &str = "generated_text_app_builds_and_runs_real_two_turn_release";
const CHILD_PROJECT: &str = "PIPECAT_NATIVE_TEXT_RELEASE_PROJECT";
const SENTINEL: &str = "SHOULD_NEVER_LEAK_NATIVE_TEXT_49A7";
const SYSTEM: &str = "You are a concise text assistant. Use the add tool for arithmetic, include the result as a number, and remember it for the next turn.";
const FIRST_USER: &str = "Use the add tool to add 2 and 3. Say the result as a number.";
const SECOND_USER: &str = "What was that result? Include the same number.";
const FIRST_REPLY: &str = "The native sum is 5.";
const SECOND_REPLY: &str = "I remember the native sum is 5.";
const MAX_LOG_BYTES: u64 = 16 * 1024 * 1024;

fn source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn generate(parent: &Path) -> PathBuf {
    let mut config = pipecat_project::ProjectConfig::new("generated-text-check");
    config.eval = "text".into();
    let plan = pipecat_project::ProjectPlan::new(config, parent, &source()).unwrap();
    pipecat_project::generate(&plan).unwrap()
}

fn check_scenario(project: &Path) -> ScriptScenario {
    let mut file = ScenarioFile::load(project.join("evals/starter_text.yaml")).unwrap();
    assert_eq!(file.name, "starter_text");
    assert_eq!(file.scenarios.len(), 1);
    let scenario = file.scenarios.remove(0);
    assert_eq!(scenario.name, "starter_text/two_turn_tool");
    assert_eq!(scenario.turns.len(), 2);
    assert_eq!(scenario.turns[0].user.as_deref(), Some(FIRST_USER));
    assert_eq!(scenario.turns[1].user.as_deref(), Some(SECOND_USER));
    assert!(!scenario.needs_judge());
    assert!(scenario.judge.as_ref().unwrap().eval.is_none());
    let call = &scenario.turns[0].expect[0];
    assert_eq!(call.event, "function_call");
    let calls = call.calls.as_ref().unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].name.as_deref(), Some("add"));
    assert_eq!(
        serde_json::to_value(calls[0].args.as_ref().unwrap()).unwrap(),
        json!({"a": 2, "b": 3})
    );
    assert_eq!(scenario.turns[0].expect[1].event, "llm_response");
    assert_eq!(scenario.turns[1].expect[0].event, "llm_response");
    scenario
}

#[test]
fn generated_text_files_load_through_native_scenario_and_release_apis() {
    let temp = tempfile::tempdir().unwrap();
    let project = generate(temp.path());
    check_scenario(&project);
    let body: Value =
        serde_json::from_slice(&fs::read(project.join("evals/bot.json")).unwrap()).unwrap();
    assert_eq!(
        body,
        json!({"native_text_bot_version": 1, "llm_url": "", "model": ""})
    );
    let manifest = ReleaseManifest::load(project.join("evals/native-manifest.yaml")).unwrap();
    assert_eq!(manifest.concurrency, 1);
    assert_eq!(manifest.jobs.len(), 1);
    let job = &manifest.jobs[0];
    assert_eq!(job.name, "native-text-starter");
    assert_eq!(job.scenario.name, "starter_text/two_turn_tool");
    assert_eq!(job.attempt, 1);
    assert_eq!(job.port, 7900);
    assert_eq!(
        job.bot,
        project.join(format!(
            "evals/../target/debug/eval-bot{}",
            std::env::consts::EXE_SUFFIX
        ))
    );
    assert!(!job.bot.exists(), "loading a plan must not build its bot");
    assert!(!project.join("Cargo.lock").exists());
    assert!(!project.join("target").exists());
}

/// A failed acceptance run retains generated source, its own lockfile, request
/// history and logs. Build products are removed on both success and unwinding.
struct Evidence {
    root: PathBuf,
    project: PathBuf,
}
impl Drop for Evidence {
    fn drop(&mut self) {
        let target = self.project.join("target");
        if target.exists() {
            if let Err(error) = fs::remove_dir_all(&target) {
                eprintln!(
                    "could not remove owned build products {}: {error}",
                    target.display()
                );
            }
        }
        eprintln!("native text acceptance evidence: {}", self.root.display());
    }
}

struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = self.0.kill();
        }
        let _ = self.0.wait();
    }
}

struct Captured {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn run_command(command: &mut Command, directory: &Path, label: &str, limit: Duration) -> Captured {
    fs::create_dir_all(directory).unwrap();
    fs::write(
        directory.join(format!("{label}.command.txt")),
        format!("{command:?}\n"),
    )
    .unwrap();
    let stdout = directory.join(format!("{label}.stdout.txt"));
    let stderr = directory.join(format!("{label}.stderr.txt"));
    command
        .stdin(Stdio::null())
        .stdout(fs::File::create(&stdout).unwrap())
        .stderr(fs::File::create(&stderr).unwrap());
    let mut child = OwnedChild(command.spawn().unwrap());
    let deadline = Instant::now() + limit;
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "{label} exceeded {limit:?}; evidence: {}",
            directory.display()
        );
        for path in [&stdout, &stderr] {
            assert!(
                fs::metadata(path).unwrap().len() <= MAX_LOG_BYTES,
                "{label} output exceeded its cap"
            );
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    for path in [&stdout, &stderr] {
        assert!(
            fs::metadata(path).unwrap().len() <= MAX_LOG_BYTES,
            "{label} output exceeded its cap"
        );
    }
    let captured = Captured {
        status,
        stdout: fs::read_to_string(stdout).unwrap(),
        stderr: fs::read_to_string(stderr).unwrap(),
    };
    fs::write(
        directory.join(format!("{label}.status.txt")),
        format!("{status}\n"),
    )
    .unwrap();
    captured
}

fn successful(output: &Captured) {
    assert!(
        output.status.success(),
        "{}\n{}",
        output.stdout,
        output.stderr
    );
}

fn cargo(project: &Path, args: &[&str]) -> Command {
    // Invoke the rustup proxy explicitly; env!("CARGO") may be a toolchain's
    // direct Cargo binary, which does not interpret +toolchain arguments.
    let mut command = Command::new("cargo");
    command
        .arg("+1.99.0")
        .args(args)
        .current_dir(project)
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_INCREMENTAL", "0");
    for key in [
        "CARGO_TARGET_DIR",
        "CARGO_BUILD_TARGET",
        "RUSTC",
        "RUSTDOC",
        "RUSTC_WRAPPER",
        "RUSTC_WORKSPACE_WRAPPER",
        "OPENAI_API_KEY",
        "DEEPGRAM_API_KEY",
        "CARTESIA_API_KEY",
        "TYPESAFE_API_KEY",
    ] {
        command.env_remove(key);
    }
    command
}

#[test]
#[ignore = "fresh offline Rust 1.99.0 application build and real generated-bot release acceptance"]
fn generated_text_app_builds_and_runs_real_two_turn_release() {
    if let Some(project) = std::env::var_os(CHILD_PROJECT) {
        let project = PathBuf::from(project).canonicalize().unwrap();
        assert!(!std::env::current_dir().unwrap().starts_with(&project));
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(release_acceptance(&project));
        return;
    }

    let root = tempfile::Builder::new()
        .prefix("pipecat-native-text-acceptance-")
        .tempdir()
        .unwrap()
        .keep();
    let parent = root.join("generated parent with spaces");
    fs::create_dir(&parent).unwrap();
    let project = generate(&parent);
    let evidence = Evidence {
        root: root.clone(),
        project: project.clone(),
    };
    let logs = root.join("commands");
    fs::write(
        root.join("source-rust.txt"),
        format!("{}\n", source().display()),
    )
    .unwrap();
    for file in ["bot.json", "starter_text.yaml", "native-manifest.yaml"] {
        fs::copy(
            project.join("evals").join(file),
            root.join(format!("generated-{file}")),
        )
        .unwrap();
    }
    assert!(!project.join("Cargo.lock").exists());
    assert!(!project.join("target").exists());
    successful(&run_command(
        &mut cargo(&project, &["--version"]),
        &logs,
        "cargo-version",
        Duration::from_secs(15),
    ));
    successful(&run_command(
        &mut cargo(&project, &["generate-lockfile", "--offline"]),
        &logs,
        "generate-lockfile",
        Duration::from_secs(120),
    ));
    let lock = fs::read(project.join("Cargo.lock")).unwrap();
    assert!(!lock.is_empty());
    successful(&run_command(
        &mut cargo(
            &project,
            &[
                "build",
                "--bins",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
            ],
        ),
        &logs,
        "build-generated-binaries",
        Duration::from_secs(1200),
    ));
    successful(&run_command(
        &mut cargo(
            &project,
            &[
                "test",
                "--bin",
                "eval-bot",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
            ],
        ),
        &logs,
        "generated-runtime-unit-tests",
        Duration::from_secs(600),
    ));
    let voice = run_command(
        &mut cargo(
            &project,
            &[
                "run",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
                "--",
                "--help",
            ],
        ),
        &logs,
        "default-voice-help",
        Duration::from_secs(120),
    );
    successful(&voice);
    assert!(voice.stdout.contains("Native HTTP /api/start"));
    assert_eq!(fs::read(project.join("Cargo.lock")).unwrap(), lock);
    check_runtime_parser(&project, &logs);

    let unrelated = root.join("unrelated-cwd");
    fs::create_dir(&unrelated).unwrap();
    let mut child = Command::new(std::env::current_exe().unwrap());
    child
        .args(["--exact", TEST_NAME, "--ignored", "--nocapture"])
        .current_dir(&unrelated)
        .env_clear()
        .env(CHILD_PROJECT, &project);
    successful(&run_command(
        &mut child,
        &logs,
        "release-from-unrelated-cwd",
        Duration::from_secs(180),
    ));
    drop(evidence);
}

fn eval_binary(project: &Path) -> PathBuf {
    project.join(format!(
        "target/debug/eval-bot{}",
        std::env::consts::EXE_SUFFIX
    ))
}

fn bot_command(project: &Path) -> Command {
    let mut command = Command::new(eval_binary(project));
    command.current_dir(project).env_clear();
    for key in [
        "OPENAI_API_KEY",
        "DEEPGRAM_API_KEY",
        "CARTESIA_API_KEY",
        "TYPESAFE_API_KEY",
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
    ] {
        command.env(key, SENTINEL);
    }
    command
}

fn no_leak(output: &Captured) {
    assert!(!output.stdout.contains(SENTINEL));
    assert!(!output.stderr.contains(SENTINEL));
}

fn check_runtime_parser(project: &Path, logs: &Path) {
    let help = run_command(
        bot_command(project).arg("--help"),
        logs,
        "eval-help",
        Duration::from_secs(5),
    );
    successful(&help);
    no_leak(&help);
    assert!(help.stdout.contains("--config"));
    assert!(help.stdout.contains("--port"));

    // Holding the bot's port makes bind-before-validation observable: valid
    // configs reach the bind error; every invalid input must fail differently.
    let occupied = StdListener::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    let model = StdListener::bind("127.0.0.1:0").unwrap();
    model.set_nonblocking(true).unwrap();
    let endpoint = format!("http://{}/v1/chat/completions", model.local_addr().unwrap());
    let config_path = logs.join("parser-config.json");
    let valid = json!({"native_text_bot_version": 1, "llm_url": endpoint, "model": "synthetic"});
    fs::write(&config_path, valid.to_string()).unwrap();
    let bind_error = run_command(
        bot_command(project)
            .args(["--port", &port, "--config"])
            .arg(&config_path),
        logs,
        "valid-config-reaches-bind",
        Duration::from_secs(5),
    );
    assert!(!bind_error.status.success());
    no_leak(&bind_error);
    assert!(!bind_error.stderr.is_empty());

    for (index, args) in [
        vec![],
        vec!["--config"],
        vec!["--unknown"],
        vec!["--port", "0"],
        vec!["--port", "65536"],
        vec!["--port", "not-a-port"],
        vec!["--port", "1", "--port", "2"],
        vec!["--config", "missing.json", "--config", "missing.json"],
        vec!["--config", "missing.json"],
    ]
    .into_iter()
    .enumerate()
    {
        let output = run_command(
            bot_command(project).args(args),
            logs,
            &format!("invalid-args-{index}"),
            Duration::from_secs(5),
        );
        assert!(!output.status.success());
        no_leak(&output);
        assert_ne!(output.stderr, bind_error.stderr);
    }
    let mut invalid = vec![
        String::new(), "[]".into(), "null".into(), "{".into(),
        format!("{{\"{SENTINEL}\":\"{SENTINEL}\"}}"),
        format!("{{\"native_text_bot_version\":1,\"llm_url\":{},\"model\":\"synthetic\",\"model\":\"{SENTINEL}\"}}", json!(endpoint)),
        " ".repeat(16 * 1024 + 1),
        fs::read_to_string(project.join("evals/bot.json")).unwrap(),
    ];
    for (field, value) in [
        ("native_text_bot_version", json!(2)),
        ("native_text_bot_version", json!("1")),
        ("model", json!("")),
        ("model", json!("   ")),
        ("model", json!("x".repeat(257))),
        ("model", json!("bad\nmodel")),
        ("llm_url", json!("")),
        ("llm_url", json!(" ")),
        ("llm_url", json!("x".repeat(4097))),
        ("llm_url", json!("https://127.0.0.1/chat")),
        ("llm_url", json!("http://localhost/chat")),
        ("llm_url", json!("http://192.0.2.1/chat")),
        ("llm_url", json!("http://example.invalid/chat")),
        ("llm_url", json!("http://user:password@127.0.0.1/chat")),
        ("llm_url", json!("http://127.0.0.1/chat?key=hidden")),
        ("llm_url", json!("http://127.0.0.1/chat#hidden")),
        ("llm_url", json!("http://127.0.0.1/\nchat")),
        ("llm_url", json!("not a URL")),
    ] {
        let mut body = valid.clone();
        body[field] = value;
        invalid.push(body.to_string());
    }
    let mut unknown = valid.clone();
    unknown[SENTINEL] = json!(SENTINEL);
    invalid.push(unknown.to_string());
    for (index, contents) in invalid.into_iter().enumerate() {
        let config_path = logs.join(format!("invalid-config-{index}.json"));
        fs::write(&config_path, contents).unwrap();
        let output = run_command(
            bot_command(project)
                .args(["--port", &port, "--config"])
                .arg(&config_path),
            logs,
            &format!("invalid-config-{index}"),
            Duration::from_secs(5),
        );
        assert!(
            !output.status.success(),
            "invalid config {index} was accepted"
        );
        no_leak(&output);
        assert_ne!(
            output.stderr, bind_error.stderr,
            "invalid config {index} reached bind"
        );
        assert!(
            matches!(model.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock),
            "invalid config contacted the model"
        );
    }
    let directory = run_command(
        bot_command(project)
            .args(["--port", &port, "--config"])
            .arg(logs),
        logs,
        "nonregular-config",
        Duration::from_secs(5),
    );
    assert!(!directory.status.success());
    assert_ne!(directory.stderr, bind_error.stderr);
    no_leak(&directory);
    // Parser-level IPv6 acceptance does not require an IPv6 listener here.
    fs::write(&config_path, json!({"native_text_bot_version":1,"llm_url":"http://[::1]:9/v1/chat/completions","model":"synthetic"}).to_string()).unwrap();
    let ipv6 = run_command(
        bot_command(project)
            .args(["--port", &port, "--config"])
            .arg(&config_path),
        logs,
        "ipv6-config-reaches-bind",
        Duration::from_secs(5),
    );
    assert!(!ipv6.status.success());
    assert_eq!(ipv6.stderr, bind_error.stderr);
    assert!(matches!(model.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ReplyMode {
    Correct,
    WrongText,
    Blocked,
}

fn text_response(text: &str) -> String {
    format!(
        "data: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"content":text},"index":0}]}),
        json!({"choices":[{"delta":{},"finish_reason":"stop","index":0}]})
    )
}

fn tool_response() -> String {
    // Argument fragments cross SSE events and pass through the service decoder.
    format!(
        "data: {}\n\ndata: {}\n\ndata: {}\n\ndata: [DONE]\n\n",
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"id":"sum-call","type":"function","function":{"name":"add","arguments":"{\"a\":2,"}}]},"index":0}]}),
        json!({"choices":[{"delta":{"tool_calls":[{"index":0,"function":{"arguments":"\"b\":3}"}}]},"index":0}]}),
        json!({"choices":[{"delta":{},"finish_reason":"tool_calls","index":0}]})
    )
}

async fn request(socket: &mut TcpStream) -> Result<Value, String> {
    let mut data = Vec::new();
    let mut buffer = [0u8; 4096];
    let end = loop {
        let read = socket.read(&mut buffer).await.map_err(|e| e.to_string())?;
        if read == 0 {
            return Err("request ended in headers".into());
        }
        data.extend_from_slice(&buffer[..read]);
        if data.len() > 64 * 1024 {
            return Err("request headers exceed cap".into());
        }
        if let Some(index) = data.windows(4).position(|bytes| bytes == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = std::str::from_utf8(&data[..end])
        .map_err(|e| e.to_string())?
        .to_owned();
    let lengths = headers
        .lines()
        .filter_map(|line| {
            line.to_ascii_lowercase()
                .strip_prefix("content-length:")
                .map(|v| v.trim().parse::<usize>())
        })
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| e.to_string())?;
    if lengths.len() != 1 || lengths[0] > 1024 * 1024 {
        return Err("request requires one bounded content-length".into());
    }
    while data.len() < end + lengths[0] {
        let read = socket.read(&mut buffer).await.map_err(|e| e.to_string())?;
        if read == 0 {
            return Err("request ended in body".into());
        }
        data.extend_from_slice(&buffer[..read]);
    }
    let body: Value =
        serde_json::from_slice(&data[end..end + lengths[0]]).map_err(|e| e.to_string())?;
    Ok(json!({"headers": headers, "body": body}))
}

fn verify_request(request: &Value, index: usize, previous: &[Value]) -> Result<(), String> {
    let headers = request["headers"].as_str().ok_or("missing headers")?;
    if !headers.starts_with("POST /v1/chat/completions HTTP/1.1\r\n") || headers.contains(SENTINEL)
    {
        return Err("unexpected request method/path or leaked environment".into());
    }
    if !headers
        .to_ascii_lowercase()
        .contains("authorization: bearer native-loopback-eval\r\n")
    {
        return Err("expected the fixed nonsecret local-eval auth marker".into());
    }
    let body = &request["body"];
    if body["model"] != "synthetic" || body["stream"] != true {
        return Err("wrong model or non-streaming request".into());
    }
    let tools = body["tools"].as_array().ok_or("missing tools")?;
    if tools.len() != 1 || tools[0]["function"]["name"] != "add" {
        return Err("expected exactly the native add tool".into());
    }
    if tools[0]["function"]["parameters"]
        != json!({
            "type":"object", "properties":{"a":{"type":"integer"},"b":{"type":"integer"}},
            "required":["a","b"], "additionalProperties":false,
        })
    {
        return Err("native add tool schema changed".into());
    }
    let messages = body["messages"].as_array().ok_or("missing messages")?;
    if messages.len() != 2 + index * 2 {
        return Err(format!(
            "request {index} has unexpected history length {}",
            messages.len()
        ));
    }
    if messages[0] != json!({"role":"system","content":SYSTEM})
        || messages[1] != json!({"role":"user","content":FIRST_USER})
    {
        return Err("initial system/user context changed or contains prior-session history".into());
    }
    if index >= 1 {
        if messages[2]["role"] != "assistant" || messages[3]["role"] != "tool" {
            return Err("missing assistant/tool continuation pair".into());
        }
        let calls = messages[2]["tool_calls"]
            .as_array()
            .ok_or("missing assistant calls")?;
        if calls.len() != 1 || calls[0]["id"] != "sum-call" || calls[0]["function"]["name"] != "add"
        {
            return Err("wrong native tool call identity".into());
        }
        let arguments: Value = serde_json::from_str(
            calls[0]["function"]["arguments"]
                .as_str()
                .ok_or("missing arguments")?,
        )
        .map_err(|e| e.to_string())?;
        if arguments != json!({"a":2,"b":3}) || messages[3]["tool_call_id"] != "sum-call" {
            return Err("tool continuation arguments or correlation changed".into());
        }
        let result: Value = serde_json::from_str(
            messages[3]["content"]
                .as_str()
                .ok_or("missing tool result")?,
        )
        .map_err(|e| e.to_string())?;
        if result != json!({"sum":5,"session_invocation":1}) {
            return Err(format!("actual native tool result is wrong: {result}"));
        }
    }
    if index == 2 {
        let previous = previous[1]["body"]["messages"]
            .as_array()
            .ok_or("missing prior history")?;
        if &messages[..4] != previous.as_slice()
            || messages[4] != json!({"role":"assistant","content":FIRST_REPLY})
            || messages[5] != json!({"role":"user","content":SECOND_USER})
        {
            return Err("second turn did not carry the committed first-turn history".into());
        }
    }
    Ok(())
}

async fn synthetic_peer(
    listener: TcpListener,
    done: Cancellation,
    blocked: Cancellation,
    path: &Path,
    mode: ReplyMode,
) -> Result<Vec<Value>, String> {
    let mut requests = Vec::new();
    loop {
        let accepted = tokio::select! {
            biased;
            result = tokio::time::timeout(Duration::from_secs(30), listener.accept()) => result,
            _ = done.cancelled() => return Ok(requests),
        };
        let (mut socket, _) = accepted
            .map_err(|_| "model accept deadline")?
            .map_err(|e| e.to_string())?;
        let value = tokio::time::timeout(Duration::from_secs(5), request(&mut socket))
            .await
            .map_err(|_| "model request deadline")??;
        let index = requests.len();
        requests.push(value);
        fs::write(path, serde_json::to_vec_pretty(&requests).unwrap())
            .map_err(|e| e.to_string())?;
        if index >= 3 {
            return Err("unexpected extra model request".into());
        }
        verify_request(&requests[index], index, &requests)?;
        if mode == ReplyMode::Blocked {
            blocked.cancel();
            let mut byte = [0u8; 1];
            match tokio::time::timeout(Duration::from_secs(8), socket.read(&mut byte)).await {
                Ok(Ok(0)) => {}
                Ok(Err(error))
                    if matches!(
                        error.kind(),
                        io::ErrorKind::ConnectionReset | io::ErrorKind::ConnectionAborted
                    ) => {}
                outcome => {
                    return Err(format!(
                        "cancelled model connection did not close: {outcome:?}"
                    ))
                }
            }
            fs::write(
                path.with_extension("socket-closed.txt"),
                "blocked request observed EOF/reset after cancellation\n",
            )
            .map_err(|e| e.to_string())?;
            return Ok(requests);
        }
        let response = match index {
            0 => tool_response(),
            1 if mode == ReplyMode::WrongText => text_response("The native sum is six."),
            1 => text_response(FIRST_REPLY),
            _ => text_response(SECOND_REPLY),
        };
        let wire = format!("HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{response}", response.len());
        tokio::time::timeout(Duration::from_secs(5), async {
            socket.write_all(wire.as_bytes()).await?;
            socket.shutdown().await
        })
        .await
        .map_err(|_| "model response deadline")?
        .map_err(|e: io::Error| e.to_string())?;
    }
}

fn session_options() -> SessionOptions {
    SessionOptions {
        handshake_timeout: Duration::from_secs(3),
        default_timeout: Duration::from_secs(3),
        max_duration: Duration::from_secs(20),
        ..Default::default()
    }
}

fn process_options() -> ProcessOptions {
    ProcessOptions {
        startup_timeout: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(2),
        output_drain_timeout: Duration::from_secs(1),
        max_stdout_bytes: 64 * 1024,
        max_stderr_bytes: 64 * 1024,
        ..Default::default()
    }
}

async fn assert_reusable(address: SocketAddr) {
    assert!(
        matches!(
            tokio::time::timeout(Duration::from_secs(1), TcpStream::connect(address)).await,
            Ok(Err(_))
        ),
        "listener remains reachable at {address}"
    );
    // Match the host's socket behavior so TIME_WAIT is not mistaken for a
    // live listener; SO_REUSEADDR does not allow two listening owners here.
    let listener = TcpListener::bind(address)
        .await
        .expect("owned port was not reusable");
    drop(listener);
}

async fn check_process(result: &ReleaseResult, port: u16, graceful: bool) {
    assert_eq!(result.runs.len(), 1);
    let run = &result.runs[0];
    assert_eq!(run.name, "native-text-starter");
    assert_eq!(run.scenario, "starter_text/two_turn_tool");
    let process = &run.hosted.process;
    assert!(process.reaped, "{process:?}");
    if graceful {
        assert!(process.success && !process.forced, "{process:?}");
        assert_eq!(process.exit_code, Some(0));
    }
    assert!(
        !process.output_overflow && !process.output_incomplete,
        "{process:?}"
    );
    assert!(!process.stdout.contains(SENTINEL) && !process.stderr.contains(SENTINEL));
    #[cfg(target_os = "linux")]
    assert!(!Path::new(&format!("/proc/{}", process.pid.unwrap())).exists());
    assert_reusable(SocketAddr::from(([127, 0, 0, 1], port))).await;
    assert!(!run.hosted.result.events_seen.iter().any(|event| matches!(
        event["type"].as_str(),
        Some(
            "bot_started_speaking"
                | "bot_stopped_speaking"
                | "tts_started"
                | "tts_stopped"
                | "audio"
                | "bot_audio"
        )
    )));
}

async fn release_acceptance(project: &Path) {
    check_scenario(project);
    let evidence = project.parent().unwrap().parent().unwrap();
    let manifest_path = project.join("evals/native-manifest.yaml");
    let original = fs::read_to_string(&manifest_path).unwrap();
    assert_eq!(original.matches("base_port: 7900").count(), 1);
    for (label, mode) in [
        ("pass-1", ReplyMode::Correct),
        ("pass-2", ReplyMode::Correct),
        ("wrong-text", ReplyMode::WrongText),
        ("blocked-cancel", ReplyMode::Blocked),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let model_address = listener.local_addr().unwrap();
        let reservation = StdListener::bind("127.0.0.1:0").unwrap();
        let port = reservation.local_addr().unwrap().port();
        fs::write(
            project.join("evals/bot.json"),
            json!({
                "native_text_bot_version":1,
                "llm_url":format!("http://{model_address}/v1/chat/completions"),
                "model":"synthetic",
            })
            .to_string(),
        )
        .unwrap();
        fs::write(
            &manifest_path,
            original.replace("base_port: 7900", &format!("base_port: {port}")),
        )
        .unwrap();
        let manifest = ReleaseManifest::load(&manifest_path).unwrap();
        assert_eq!(
            manifest.jobs[0].bot.canonicalize().unwrap(),
            eval_binary(project).canonicalize().unwrap()
        );
        assert_eq!(manifest.jobs[0].port, port);
        drop(reservation);
        // This release-to-bind window has no listener-identity guarantee. A
        // competing bind is an actionable fixture failure, never silently retried.
        let done = Cancellation::new();
        let blocked = Cancellation::new();
        let cancel = Cancellation::new();
        let request_path = evidence.join(format!("{label}.requests.json"));
        let (release, peer, cancellation) = tokio::join!(
            async {
                let result = tokio::time::timeout(
                    Duration::from_secs(35),
                    run_release(
                        manifest,
                        session_options(),
                        process_options(),
                        cancel.clone(),
                    ),
                )
                .await;
                done.cancel();
                result
            },
            synthetic_peer(listener, done.clone(), blocked.clone(), &request_path, mode),
            async {
                if mode == ReplyMode::Blocked {
                    let ready =
                        tokio::time::timeout(Duration::from_secs(10), blocked.cancelled()).await;
                    cancel.cancel();
                    ready.map_err(|_| "blocked inference never reached the model")?;
                }
                Ok::<(), &str>(())
            },
        );
        fs::write(
            evidence.join(format!("{label}.peer-status.txt")),
            format!("{peer:?}\n"),
        )
        .unwrap();
        let result = release.expect("release exceeded total deadline").unwrap();
        fs::write(
            evidence.join(format!("{label}.result.json")),
            serde_json::to_vec_pretty(&result).unwrap(),
        )
        .unwrap();
        check_process(&result, port, mode != ReplyMode::Blocked).await;
        assert_reusable(model_address).await;
        let requests = peer.expect("synthetic peer rejected actual request/tool history");
        cancellation.unwrap();
        if mode == ReplyMode::Correct {
            assert!(result.passed, "{result:?}");
            assert_eq!(requests.len(), 3);
            let script = &result.runs[0].hosted.result;
            assert_eq!(script.turns.len(), 2);
            assert!(script.turns.iter().all(|turn| turn.status == "passed"));
            let calls: Vec<_> = script
                .events_seen
                .iter()
                .filter(|event| event["type"] == "function_call")
                .collect();
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0]["name"], "add");
            assert_eq!(calls[0]["args"], json!({"a":2,"b":3}));
        } else if mode == ReplyMode::WrongText {
            assert!(
                !result.passed,
                "wrong model text passed the generated scenario"
            );
            assert_eq!(requests.len(), 2);
            assert_eq!(result.runs[0].hosted.result.turns[0].status, "failed");
        } else {
            assert!(
                !result.passed,
                "cancelled inference passed the generated scenario"
            );
            assert_eq!(requests.len(), 1);
            assert!(result.runs[0]
                .hosted
                .result
                .failures
                .iter()
                .any(|failure| failure.kind == "cancelled"));
        }
    }

    // Adding an eval criterion without an explicit judge must fail preflight.
    // Holding the configured bot port and model port makes accidental startup
    // visible without relying on an arbitrary sleep or a subprocess PID race.
    let model = StdListener::bind("127.0.0.1:0").unwrap();
    model.set_nonblocking(true).unwrap();
    let bot = StdListener::bind("127.0.0.1:0").unwrap();
    bot.set_nonblocking(true).unwrap();
    let port = bot.local_addr().unwrap().port();
    fs::write(
        project.join("evals/bot.json"),
        json!({
            "native_text_bot_version":1,
            "llm_url":format!("http://{}/v1/chat/completions", model.local_addr().unwrap()),
            "model":"synthetic",
        })
        .to_string(),
    )
    .unwrap();
    fs::write(
        &manifest_path,
        original.replace("base_port: 7900", &format!("base_port: {port}")),
    )
    .unwrap();
    let mut manifest = ReleaseManifest::load(&manifest_path).unwrap();
    manifest.jobs[0].scenario.turns[0].expect[1].eval =
        Some("Contains the arithmetic result".into());
    let result = run_release(
        manifest,
        session_options(),
        process_options(),
        Cancellation::new(),
    )
    .await;
    let error = result.expect_err("implicit judge was accepted").to_string();
    fs::write(evidence.join("no-implicit-judge.txt"), &error).unwrap();
    assert!(error.contains("judge"), "{error}");
    assert!(matches!(bot.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
    assert!(matches!(model.accept(), Err(error) if error.kind() == io::ErrorKind::WouldBlock));
    tokio::time::timeout(Duration::from_secs(3), async {
        while tokio::runtime::Handle::current()
            .metrics()
            .num_alive_tasks()
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("native release left an owned task running");
}
