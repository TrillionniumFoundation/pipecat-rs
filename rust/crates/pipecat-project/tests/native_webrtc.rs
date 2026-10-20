//! Independent generated SmallWebRTC application acceptance, not browser testing.
//! The production binary is checked without opening provider connections. Real
//! encrypted RTC/RTVI/PCM is tested separately through its shared library using
//! the runner's existing loopback fixtures and explicit local ProviderConfig.
//! The caller owns build serialization; this test never shares a target directory.
#![cfg(unix)]

use serde_json::{json, Value};
use std::{
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::process::CommandExt,
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

const PACKAGE: &str = "generated-smallwebrtc-check";
const SENTINEL: &str = "SYNTHETIC_ONLY_GENERATED_RTC_CREDENTIAL_142";
const MAX_LOG_BYTES: u64 = 16 * 1024 * 1024;
const DISK_FLOOR: u64 = 2304 * 1024 * 1024;

fn source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

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
                    "could not remove owned target {}: {error}",
                    target.display()
                );
            }
        }
        eprintln!(
            "generated SmallWebRTC acceptance evidence: {}",
            self.root.display()
        );
    }
}

/// Every command gets its own process group. On timeout/panic, kill descendants
/// as well as the owner, then reap the owner before deleting its build products.
struct OwnedChild(Child);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !matches!(self.0.try_wait(), Ok(Some(_))) {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &format!("-{}", self.0.id())])
                .status();
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

fn available_bytes(path: &Path) -> u64 {
    let output = Command::new("df").arg("-Pk").arg(path).output().unwrap();
    assert!(output.status.success(), "disk availability check failed");
    let stdout = String::from_utf8(output.stdout).unwrap();
    stdout
        .lines()
        .next_back()
        .unwrap()
        .split_whitespace()
        .nth(3)
        .unwrap()
        .parse::<u64>()
        .unwrap()
        * 1024
}

fn check_logs(logs: &Path, label: &str) {
    for suffix in ["stdout", "stderr"] {
        assert!(
            fs::metadata(logs.join(format!("{label}.{suffix}.txt")))
                .unwrap()
                .len()
                <= MAX_LOG_BYTES,
            "{label} output exceeded its bound"
        );
    }
}

fn spawn(command: &mut Command, logs: &Path, label: &str) -> OwnedChild {
    fs::create_dir_all(logs).unwrap();
    fs::write(
        logs.join(format!("{label}.command.txt")),
        format!("{command:?}\n"),
    )
    .unwrap();
    command
        .stdin(Stdio::null())
        .stdout(fs::File::create(logs.join(format!("{label}.stdout.txt"))).unwrap())
        .stderr(fs::File::create(logs.join(format!("{label}.stderr.txt"))).unwrap())
        .process_group(0);
    OwnedChild(command.spawn().unwrap())
}

fn finish(child: &mut OwnedChild, logs: &Path, label: &str, limit: Duration) -> Captured {
    let deadline = Instant::now() + limit;
    let mut next_disk_check = Instant::now();
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(
            Instant::now() < deadline,
            "{label} exceeded {limit:?}; logs: {}",
            logs.display()
        );
        check_logs(logs, label);
        if Instant::now() >= next_disk_check {
            assert!(
                available_bytes(logs) >= DISK_FLOOR,
                "{label}: available disk fell below 2.25 GiB"
            );
            next_disk_check = Instant::now() + Duration::from_secs(1);
        }
        std::thread::sleep(Duration::from_millis(25));
    };
    check_logs(logs, label);
    fs::write(
        logs.join(format!("{label}.status.txt")),
        format!("{status}\n"),
    )
    .unwrap();
    Captured {
        status,
        stdout: fs::read_to_string(logs.join(format!("{label}.stdout.txt"))).unwrap(),
        stderr: fs::read_to_string(logs.join(format!("{label}.stderr.txt"))).unwrap(),
    }
}

fn run(command: &mut Command, logs: &Path, label: &str, limit: Duration) -> Captured {
    let mut child = spawn(command, logs, label);
    finish(&mut child, logs, label, limit)
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
    let mut command = Command::new("cargo");
    command
        .arg("+1.99.0")
        .args(args)
        .current_dir(project)
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_INCREMENTAL", "0")
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1");
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
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ] {
        command.env_remove(key);
    }
    command
}

fn install_loopback_tests(project: &Path) {
    let root = source();
    let mut manifest = fs::read_to_string(project.join("Cargo.toml")).unwrap();
    manifest.push_str("\n[dev-dependencies]\nbase64 = \"0.22\"\nserde_json = \"1\"\nurl = \"2\"\nreqwest = { version = \"0.12\", default-features = false, features = [\"rustls-tls\", \"json\"] }\n");
    for (name, relative) in [
        ("pipecat-browser-client", "client-templates"),
        ("pipecat-client-server", "crates/pipecat-client-server"),
        ("pipecat-cli", "crates/pipecat-cli"),
        ("pipecat-core", "crates/pipecat-core"),
        ("pipecat-transports", "crates/pipecat-transports"),
        ("pipecat-webrtc", "crates/pipecat-webrtc"),
    ] {
        let path = serde_json::to_string(root.join(relative).to_str().unwrap()).unwrap();
        manifest.push_str(&format!("{name} = {{ path = {path} }}\n"));
    }
    fs::write(project.join("Cargo.toml"), manifest).unwrap();
    fs::create_dir_all(project.join("tests/support")).unwrap();
    fs::write(
        project.join("tests/support/mod.rs"),
        include_str!("../../pipecat-runner/tests/support/mod.rs"),
    )
    .unwrap();
    for (name, original, constructors) in [
        (
            "generated_flow",
            include_str!("../../pipecat-runner/tests/webrtc_flow.rs"),
            1,
        ),
        (
            "generated_lifecycle",
            include_str!("../../pipecat-runner/tests/webrtc_lifecycle.rs"),
            5,
        ),
    ] {
        let constructor = "NativeRunner::new_with_webrtc(";
        assert_eq!(
            original.matches(constructor).count(),
            constructors,
            "review fixture drift before reusing it"
        );
        let mut adapted = original
            .strip_prefix("#![cfg(feature = \"webrtc\")]\n")
            .unwrap()
            .replace(constructor, "generated_smallwebrtc_check::create_runner(");
        assert!(!adapted.contains(constructor));
        if name == "generated_flow" {
            adapted = adapted.replace("{NativeRunner, Options,", "{Options,");
        }
        fs::write(project.join(format!("tests/{name}.rs")), adapted).unwrap();
    }
}

/// Only plain loopback HTTP is used by the parent harness. The RTC fixtures use
/// their own no-proxy local peers. Response bodies and socket waits are bounded.
fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
) -> std::io::Result<(u16, String)> {
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    let origin = format!("http://{address}");
    write!(socket, "{method} {path} HTTP/1.1\r\nHost: {address}\r\nOrigin: {origin}\r\nConnection: close\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}", body.len())?;
    let mut response = Vec::new();
    socket.take(65537).read_to_end(&mut response)?;
    assert!(response.len() <= 65536, "HTTP response exceeded its bound");
    let response = String::from_utf8(response).unwrap();
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response headers");
    let status = headers.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(
        !headers
            .to_ascii_lowercase()
            .contains("transfer-encoding: chunked"),
        "expected bounded full-body response"
    );
    Ok((status, body.into()))
}

fn binary(project: &Path) -> PathBuf {
    project.join(format!(
        "target/debug/{PACKAGE}{}",
        std::env::consts::EXE_SUFFIX
    ))
}

fn main_command(project: &Path) -> Command {
    let mut command = Command::new(binary(project));
    command.current_dir(project).env_clear();
    command
}

fn compiled_main_smoke(project: &Path, logs: &Path) {
    let help = run(
        main_command(project).arg("--help"),
        logs,
        "compiled-main-help",
        Duration::from_secs(5),
    );
    successful(&help);
    assert!(help.stdout.contains("SmallWebRTC by default"));
    assert!(!help.stdout.contains(SENTINEL));
    let missing_keys = run(
        main_command(project).arg("--no-intro"),
        logs,
        "compiled-main-missing-keys",
        Duration::from_secs(5),
    );
    assert!(!missing_keys.status.success());

    // These are inert HTTP asset fixtures, not a WASM build or browser validation.
    let assets = project.join("http-asset-fixture");
    fs::create_dir_all(assets.join("pkg")).unwrap();
    for (name, content) in [
        (
            "index.html",
            "<!doctype html><title>HTTP fixture only</title>",
        ),
        ("style.css", ""),
        ("config.json", "{\"transports\":[\"websocket\"]}"),
        ("pkg/pipecat_browser_client.js", "// inert HTTP fixture"),
        (
            "pkg/pipecat_browser_client_bg.wasm",
            "inert HTTP fixture, not WASM",
        ),
    ] {
        fs::write(assets.join(name), content).unwrap();
    }
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    let mut command = main_command(project);
    command
        .args(["--bind", &address.to_string(), "--no-intro", "--assets"])
        .arg(&assets);
    for name in ["DEEPGRAM_API_KEY", "OPENAI_API_KEY", "CARTESIA_API_KEY"] {
        command.env(name, SENTINEL);
    }
    let label = "compiled-main-default-transport";
    let mut child = spawn(&mut command, logs, label);
    let deadline = Instant::now() + Duration::from_secs(10);
    let (status, config) = loop {
        assert!(
            child.0.try_wait().unwrap().is_none(),
            "main exited before HTTP startup"
        );
        if let Ok(response) = http(address, "GET", "/config.json", "") {
            break response;
        }
        assert!(
            Instant::now() < deadline,
            "main did not bind its HTTP listener"
        );
        std::thread::sleep(Duration::from_millis(25));
    };
    assert_eq!(status, 200);
    let config: Value = serde_json::from_str(&config).unwrap();
    assert_eq!(config["transports"], json!(["smallwebrtc", "websocket"]));
    assert_eq!(config["start_endpoint"], "/api/start");
    fs::write(
        logs.join("compiled-main-browser-config.json"),
        serde_json::to_vec_pretty(&config).unwrap(),
    )
    .unwrap();
    let (status, start) = http(
        address,
        "POST",
        "/api/start",
        r#"{"transport":"webrtc","createDailyRoom":false,"enableDefaultIceServers":true}"#,
    )
    .unwrap();
    assert_eq!(status, 200);
    let start: Value = serde_json::from_str(&start).unwrap();
    assert_eq!(start["transport"], "webrtc");
    assert_eq!(start["icePolicy"], "host-only");
    assert_eq!(start["iceConfig"]["iceServers"], json!([]));
    assert_eq!(start["sessionId"].as_str().unwrap().len(), 64);
    assert_eq!(start["contextMode"], "generated-text");
    assert_eq!(start["playbackAcknowledgements"], false);
    assert!(!start.to_string().contains(SENTINEL));
    fs::write(
        logs.join("compiled-main-pending-offer.json"),
        serde_json::to_vec_pretty(&start).unwrap(),
    )
    .unwrap();
    // An unused offer must be cleaned up on normal process shutdown. No SDP,
    // client-ready, PCM, or provider-triggering work is sent to production main.
    assert!(Command::new("/bin/kill")
        .args(["-INT", &child.0.id().to_string()])
        .status()
        .unwrap()
        .success());
    let result = finish(&mut child, logs, label, Duration::from_secs(10));
    successful(&result);
    assert!(!result.stdout.contains(SENTINEL));
    assert!(!result.stderr.contains(SENTINEL));
    let rebound = TcpListener::bind(address).expect("main must release its listener");
    drop(rebound);
}

#[test]
#[ignore = "independent offline Rust 1.99 SmallWebRTC build and real local encrypted media integration; caller serializes builds"]
fn generated_smallwebrtc_app_builds_and_runs_local_rtc() {
    let root = tempfile::Builder::new()
        .prefix("pipecat-native-webrtc-acceptance-")
        .tempdir()
        .unwrap()
        .keep();
    let parent = root.join("generated parent with spaces");
    fs::create_dir(&parent).unwrap();
    let mut config = pipecat_project::ProjectConfig::new(PACKAGE);
    config.transport = "smallwebrtc".into();
    config.client = "rust-wasm".into();
    let plan = pipecat_project::ProjectPlan::new(config, &parent, &source()).unwrap();
    let project = pipecat_project::generate(&plan).unwrap();
    let evidence = Evidence {
        root: root.clone(),
        project: project.clone(),
    };
    let logs = root.join("commands");
    fs::create_dir(&logs).unwrap();
    fs::write(
        root.join("source-rust.txt"),
        format!("{}\n", source().display()),
    )
    .unwrap();
    assert!(!project.join("target").exists());
    assert!(!project.join("Cargo.lock").exists());
    let client: Value =
        serde_json::from_slice(&fs::read(project.join("client/web/config.json")).unwrap()).unwrap();
    assert_eq!(client["transports"], json!(["smallwebrtc"]));
    let main = fs::read_to_string(project.join("src/main.rs")).unwrap();
    assert!(main.contains("generated_smallwebrtc_check::run()"));
    let library = fs::read_to_string(project.join("src/lib.rs")).unwrap();
    assert!(library.contains("pipecat_runner::run_webrtc_from_env(create_runner)"));
    install_loopback_tests(&project);
    assert!(
        available_bytes(&root) >= DISK_FLOOR,
        "insufficient disk before independent build"
    );
    successful(&run(
        &mut cargo(&project, &["generate-lockfile", "--offline"]),
        &logs,
        "generate-lockfile",
        Duration::from_secs(120),
    ));
    let lock = fs::read(project.join("Cargo.lock")).unwrap();
    let metadata = run(
        &mut cargo(
            &project,
            &["metadata", "--offline", "--locked", "--format-version", "1"],
        ),
        &logs,
        "resolved-metadata",
        Duration::from_secs(120),
    );
    successful(&metadata);
    let metadata: Value = serde_json::from_str(&metadata.stdout).unwrap();
    let packages = metadata["packages"].as_array().unwrap();
    let webrtc: Vec<_> = packages.iter().filter(|p| p["name"] == "webrtc").collect();
    assert_eq!(webrtc.len(), 1);
    assert_eq!(webrtc[0]["version"], "0.14.0");
    assert!(
        webrtc[0]["source"].is_null(),
        "must retain the audited vendored WebRTC"
    );
    assert_eq!(
        Path::new(webrtc[0]["manifest_path"].as_str().unwrap())
            .canonicalize()
            .unwrap(),
        source()
            .join("vendor/webrtc-0.14.0/Cargo.toml")
            .canonicalize()
            .unwrap()
    );
    assert_eq!(
        Path::new(metadata["target_directory"].as_str().unwrap()),
        project.join("target")
    );
    successful(&run(
        &mut cargo(
            &project,
            &[
                "build",
                "--bins",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
                "-j",
                "1",
            ],
        ),
        &logs,
        "build-generated-main",
        Duration::from_secs(1800),
    ));
    compiled_main_smoke(&project, &logs);
    let tests = run(
        &mut cargo(
            &project,
            &[
                "test",
                "--test",
                "generated_flow",
                "--test",
                "generated_lifecycle",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
                "-j",
                "1",
                "--",
                "--test-threads=1",
            ],
        ),
        &logs,
        "generated-library-local-rtc",
        Duration::from_secs(1800),
    );
    successful(&tests);
    assert!(tests.stdout.contains("1 passed; 0 failed"));
    assert!(tests.stdout.contains("5 passed; 0 failed"));
    assert_eq!(fs::read(project.join("Cargo.lock")).unwrap(), lock);
    drop(evidence);
}
