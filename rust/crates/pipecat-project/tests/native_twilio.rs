//! Independent generated Twilio application acceptance, using synthetic data and
//! literal-loopback peers only. Production main is compiled before test-only
//! dependencies are installed; no media/start is sent to its hosted providers.
//! The full existing phone suite instead calls the generated library constructor.
//! The caller owns build serialization. Sources, locks and logs survive failure;
//! only this test's isolated build products are removed after child reaping.
#![cfg(unix)]

use base64::{engine::general_purpose::STANDARD, Engine as _};
use hmac::{Hmac, Mac};
use serde_json::{json, Value};
use sha1::Sha1;
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    fs,
    io::{Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    os::unix::{fs::PermissionsExt, process::CommandExt},
    path::{Path, PathBuf},
    process::{Child, Command, ExitStatus, Stdio},
    sync::atomic::{AtomicBool, Ordering},
    time::{Duration, Instant},
};

const PACKAGE: &str = "generated-twilio-check";
const ACCOUNT: &str = "AC00000000000000000000000000000147";
const CALL: &str = "CA00000000000000000000000000000147";
const SECRET: &str = "synthetic-generated-twilio-secret-not-an-account-147";
const PROVIDER_KEY: &str = "SYNTHETIC_ONLY_GENERATED_TWILIO_PROVIDER_147";
const MAX_LOG_BYTES: u64 = 16 * 1024 * 1024;
const DISK_FLOOR: u64 = 2304 * 1024 * 1024;
const STARTUP_RESERVE: u64 = 512 * 1024 * 1024;
// A later successful retry cannot erase an earlier uncertain cleanup result.
static RETAIN_OWNED_TARGET: AtomicBool = AtomicBool::new(false);
const FLOW: &str = include_str!("../../pipecat-runner/tests/twilio_flow.rs");
const TWO_CALL_CASE: &str =
    "signed_twiml_phone_loop_has_resampled_audio_dtmf_history_and_call_isolation";
const CASES: [&str; 7] = [
    TWO_CALL_CASE,
    "signatures_capabilities_and_canonical_urls_gate_provider_admission",
    "invalid_or_missing_start_never_creates_providers_or_hangs_up",
    "verified_local_end_cancel_disconnect_and_shutdown_own_one_hangup",
    "pending_and_active_capacity_is_released_by_expiry_cancel_and_shutdown",
    "established_calls_reject_duplicate_start_wrong_stream_and_invalid_controls",
    "setup_failure_and_shutdown_under_phone_queue_pressure_join_the_pending_owner",
];
const EXTRA_CASES: [&str; 2] = [
    "generated_established_stt_failure_joins_the_phone_and_providers",
    "generated_binary_and_unsupported_events_are_rejected_before_and_after_start",
];
type Manifest = BTreeMap<String, Value>;

fn source() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .unwrap()
}

fn fingerprint(path: &Path) -> Value {
    let bytes = fs::read(path).unwrap();
    json!({"bytes": bytes.len(), "sha256": format!("{:x}", Sha256::digest(&bytes))})
}

fn native_manifest(root: &Path) -> Manifest {
    let output = Command::new("git")
        .args([
            "ls-files",
            "-z",
            "--cached",
            "--others",
            "--exclude-standard",
            "--",
            ".",
        ])
        .current_dir(root)
        .output()
        .unwrap();
    assert!(output.status.success(), "cannot enumerate native source");
    output
        .stdout
        .split(|byte| *byte == 0)
        .filter(|path| !path.is_empty())
        .map(|path| {
            let name = std::str::from_utf8(path).unwrap().to_owned();
            let path = root.join(&name);
            assert!(
                fs::symlink_metadata(&path).unwrap().file_type().is_file(),
                "source evidence must contain ordinary files: {}",
                path.display()
            );
            (name, fingerprint(&path))
        })
        .collect()
}

fn tree_manifest(root: &Path) -> Manifest {
    fn visit(base: &Path, directory: &Path, result: &mut Manifest) {
        for entry in fs::read_dir(directory).unwrap() {
            let entry = entry.unwrap();
            if entry.file_name() == "target" {
                continue;
            }
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            if kind.is_dir() {
                visit(base, &path, result);
            } else {
                assert!(kind.is_file(), "evidence must not follow symlinks");
                result.insert(
                    path.strip_prefix(base).unwrap().to_str().unwrap().into(),
                    fingerprint(&path),
                );
            }
        }
    }
    let mut result = Manifest::new();
    visit(root, root, &mut result);
    result
}

fn executable_manifest(project: &Path) -> Manifest {
    let mut paths = vec![project.join(format!("target/debug/{PACKAGE}"))];
    let dependencies = project.join("target/debug/deps");
    if dependencies.is_dir() {
        for entry in fs::read_dir(dependencies).unwrap() {
            let entry = entry.unwrap();
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with("generated_flow-"))
                && entry.file_type().unwrap().is_file()
                && entry.metadata().unwrap().permissions().mode() & 0o111 != 0
            {
                paths.push(entry.path());
            }
        }
    }
    paths
        .into_iter()
        .filter(|path| path.is_file())
        .map(|path| {
            (
                path.strip_prefix(project).unwrap().to_str().unwrap().into(),
                fingerprint(&path),
            )
        })
        .collect()
}

fn write_json(path: &Path, value: &impl serde::Serialize) {
    fs::write(path, serde_json::to_vec_pretty(value).unwrap()).unwrap();
}

fn freeze_native(root: &Path, evidence: &Path, expected: &Manifest, label: &str) {
    let actual = native_manifest(root);
    write_json(
        &evidence.join(format!("native-source-{label}.json")),
        &actual,
    );
    assert_eq!(&actual, expected, "native source changed during {label}");
}

fn snapshot_project(project: &Path, destination: &Path) -> Manifest {
    let manifest = tree_manifest(project);
    for name in manifest.keys() {
        let destination = destination.join(name);
        fs::create_dir_all(destination.parent().unwrap()).unwrap();
        fs::copy(project.join(name), destination).unwrap();
    }
    manifest
}

struct Evidence {
    root: PathBuf,
    project: PathBuf,
}
impl Drop for Evidence {
    fn drop(&mut self) {
        // Child owners are declared later and must be reaped before this guard.
        // Never remove the generated sources, locks, command logs or other targets.
        let target = self.project.join("target");
        // Retain executable identities on failure too, before deleting any owned
        // products. This does not claim a partially built executable was tested.
        let _ = std::panic::catch_unwind(|| {
            write_json(
                &self.root.join("executables-before-target-cleanup.json"),
                &executable_manifest(&self.project),
            );
        });
        let retain = RETAIN_OWNED_TARGET.load(Ordering::Acquire);
        let removed = !retain && (!target.exists() || fs::remove_dir_all(&target).is_ok());
        let _ = fs::write(
            self.root.join("target-cleanup.json"),
            format!(
                "{{\"owned_target_removed\":{removed},\"retained_for_uncertain_child_cleanup\":{retain},\"owned_target_exists\":{}}}\n",
                target.exists()
            ),
        );
        if retain {
            eprintln!(
                "retaining owned target until child cleanup is independently verified: {}",
                target.display()
            );
        } else if !removed {
            eprintln!("could not remove owned target {}", target.display());
        }
        // Also retain final identities on a failed acceptance, without double panic.
        let _ = std::panic::catch_unwind(|| {
            write_json(
                &self.root.join("retained-evidence-sha256.json"),
                &tree_manifest(&self.root),
            );
        });
        eprintln!(
            "generated Twilio acceptance evidence: {}",
            self.root.display()
        );
    }
}

struct OwnedChild {
    child: Child,
    logs: PathBuf,
    label: String,
    reaped: bool,
}
impl OwnedChild {
    fn reap(&mut self) -> bool {
        if self.reaped {
            return true;
        }
        let pid = self.child.id().to_string();
        let group = format!("-{pid}");
        let group_alive = || {
            Command::new("/bin/kill")
                .args(["-0", "--", &group])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status()
                .map(|status| status.success())
                .unwrap_or_else(|_| {
                    // Failure to perform the probe is not evidence of absence.
                    RETAIN_OWNED_TARGET.store(true, Ordering::Release);
                    true
                })
        };
        // Kill descendants even if the group leader has already exited.
        if group_alive() {
            let _ = Command::new("/bin/kill")
                .args(["-KILL", "--", &group])
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .status();
        }
        if !matches!(self.child.try_wait(), Ok(Some(_))) {
            let _ = self.child.kill();
        }
        let waited = self.child.wait().is_ok();
        let deadline = Instant::now() + Duration::from_secs(2);
        while group_alive() && Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(20));
        }
        let gone = !group_alive();
        self.reaped = waited && gone;
        if !self.reaped {
            RETAIN_OWNED_TARGET.store(true, Ordering::Release);
        }
        let _ = fs::write(
            self.logs.join(format!("{}.cleanup.json", self.label)),
            format!(
                "{{\"pid\":{pid},\"owner_reaped\":{waited},\"process_group_gone\":{gone},\"target_retention_required\":{}}}\n",
                RETAIN_OWNED_TARGET.load(Ordering::Acquire)
            ),
        );
        self.reaped
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if !self.reap() {
            eprintln!(
                "{} did not confirm complete process-group cleanup",
                self.label
            );
        }
    }
}

struct Captured {
    status: ExitStatus,
    stdout: String,
    stderr: String,
}

fn available_bytes(path: &Path) -> u64 {
    let output = Command::new("df").args(["-Pk"]).arg(path).output().unwrap();
    assert!(output.status.success(), "disk availability check failed");
    String::from_utf8(output.stdout)
        .unwrap()
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

fn check_disk(logs: &Path, reserve: u64, label: &str) {
    let source_free = available_bytes(&source());
    let evidence_free = available_bytes(logs);
    let mut log = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(logs.join("disk-observations.jsonl"))
        .unwrap();
    writeln!(
        log,
        "{}",
        json!({"stage": label, "source_available": source_free,
            "evidence_available": evidence_free, "floor": DISK_FLOOR, "reserve": reserve})
    )
    .unwrap();
    for free in [source_free, evidence_free] {
        assert!(
            free >= DISK_FLOOR + reserve,
            "{label}: need at least 2.25 GiB plus {reserve} startup bytes on both filesystems"
        );
    }
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
    OwnedChild {
        child: command.spawn().unwrap(),
        logs: logs.into(),
        label: label.into(),
        reaped: false,
    }
}

fn finish(child: &mut OwnedChild, logs: &Path, label: &str, limit: Duration) -> Captured {
    let deadline = Instant::now() + limit;
    let mut next_disk_check = Instant::now();
    let status = loop {
        if let Some(status) = child.child.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "{label} exceeded {limit:?}");
        check_logs(logs, label);
        if Instant::now() >= next_disk_check {
            check_disk(logs, 0, label);
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
    assert!(child.reap(), "{label} left an owned child/process group");
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
        "command failed:\n{}\n{}",
        output.stdout,
        output.stderr
    );
}

fn cargo(project: &Path, args: &[&str]) -> Command {
    let mut command = Command::new("cargo");
    command
        .env_clear()
        .arg("+1.99.0")
        .args(args)
        .current_dir(project);
    // Retain only tool/cache discovery, never ambient credentials, proxies,
    // rustc wrappers, target overrides or feature-affecting Rust flags.
    for key in ["PATH", "HOME", "CARGO_HOME", "RUSTUP_HOME", "TMPDIR"] {
        if let Some(value) = std::env::var_os(key) {
            command.env(key, value);
        }
    }
    command
        .env("CARGO_NET_OFFLINE", "true")
        .env("CARGO_INCREMENTAL", "0")
        .env("CARGO_BUILD_JOBS", "1")
        .env("NO_PROXY", "127.0.0.1,localhost,::1")
        .env("no_proxy", "127.0.0.1,localhost,::1");
    command
}

fn generated_metadata(project: &Path, logs: &Path, label: &str) {
    let output = run(
        &mut cargo(
            project,
            &["metadata", "--offline", "--locked", "--format-version", "1"],
        ),
        logs,
        label,
        Duration::from_secs(120),
    );
    successful(&output);
    let metadata: Value = serde_json::from_str(&output.stdout).unwrap();
    assert_eq!(
        Path::new(metadata["target_directory"].as_str().unwrap()),
        project.join("target"),
        "generated app must use only its isolated target"
    );
    assert_eq!(
        Path::new(metadata["workspace_root"].as_str().unwrap()),
        project,
        "generated app must be an independent workspace"
    );
    let runners: Vec<_> = metadata["packages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|package| package["name"] == "pipecat-runner")
        .collect();
    assert_eq!(runners.len(), 1);
    assert!(runners[0]["source"].is_null());
    assert_eq!(
        Path::new(runners[0]["manifest_path"].as_str().unwrap())
            .canonicalize()
            .unwrap(),
        source().join("crates/pipecat-runner/Cargo.toml")
    );
}

fn main_command(project: &Path) -> Command {
    let mut command = Command::new(project.join(format!("target/debug/{PACKAGE}")));
    command.current_dir(project).env_clear();
    command
}

fn configured_main(project: &Path, address: SocketAddr) -> Command {
    let mut command = main_command(project);
    command
        .args(["--enable", "--no-intro", "--bind", &address.to_string()])
        .env("TWILIO_ACCOUNT_SID", ACCOUNT)
        .env("TWILIO_AUTH_TOKEN", SECRET)
        .env(
            "PIPECAT_TWILIO_WEBHOOK_URL",
            format!("http://{address}/twilio/voice"),
        )
        .env(
            "PIPECAT_TWILIO_MEDIA_URL",
            format!("ws://{address}/twilio/media"),
        );
    for name in ["DEEPGRAM_API_KEY", "OPENAI_API_KEY", "CARTESIA_API_KEY"] {
        command.env(name, PROVIDER_KEY);
    }
    command
}

fn assert_redacted(output: &Captured, capability: Option<&str>) {
    for private in [ACCOUNT, CALL, SECRET, PROVIDER_KEY]
        .into_iter()
        .chain(capability)
    {
        assert!(
            !output.stdout.contains(private),
            "main stdout leaked synthetic private data"
        );
        assert!(
            !output.stderr.contains(private),
            "main stderr leaked synthetic private data"
        );
    }
}

fn signature(url: &str, fields: &BTreeMap<&str, &str>) -> String {
    let mut mac = Hmac::<Sha1>::new_from_slice(SECRET.as_bytes()).unwrap();
    mac.update(url.as_bytes());
    for (name, value) in fields {
        mac.update(name.as_bytes());
        mac.update(value.as_bytes());
    }
    STANDARD.encode(mac.finalize().into_bytes())
}

struct HttpResponse {
    status: u16,
    headers: String,
    body: String,
}

/// A bounded literal-loopback HTTP client, with no proxy, DNS, redirect or media.
fn http(
    address: SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) -> std::io::Result<HttpResponse> {
    assert!(address.ip().is_loopback());
    let mut socket = TcpStream::connect_timeout(&address, Duration::from_millis(250))?;
    socket.set_read_timeout(Some(Duration::from_secs(2)))?;
    socket.set_write_timeout(Some(Duration::from_secs(2)))?;
    write!(
        socket,
        "{method} {path} HTTP/1.1\r\nHost: {address}\r\nConnection: close\r\nContent-Type: application/x-www-form-urlencoded\r\nContent-Length: {}\r\n",
        body.len()
    )?;
    for (key, value) in extra_headers {
        assert!(!key.contains(['\r', '\n']) && !value.contains(['\r', '\n']));
        write!(socket, "{key}: {value}\r\n")?;
    }
    write!(socket, "\r\n{body}")?;
    let mut bytes = Vec::new();
    socket.take(65537).read_to_end(&mut bytes)?;
    assert!(bytes.len() <= 65536, "HTTP response exceeded its bound");
    let response = String::from_utf8(bytes).unwrap();
    let (headers, body) = response
        .split_once("\r\n\r\n")
        .expect("HTTP response headers");
    assert!(!headers
        .to_ascii_lowercase()
        .contains("transfer-encoding: chunked"));
    Ok(HttpResponse {
        status: headers.split_whitespace().nth(1).unwrap().parse().unwrap(),
        headers: headers.to_ascii_lowercase(),
        body: body.into(),
    })
}

fn compiled_main_smoke(project: &Path, logs: &Path) {
    for (label, args, success, expected) in [
        ("help", vec!["--help"], true, "Disabled by default"),
        ("disabled", vec![], false, "entry is disabled"),
        (
            "missing-config",
            vec!["--enable"],
            false,
            "missing Twilio setting",
        ),
        (
            "nonloopback",
            vec!["--enable", "--bind", "0.0.0.0:7861"],
            false,
            "literal loopback",
        ),
        (
            "browser-transport",
            vec!["--enable", "--transport", "websocket"],
            false,
            "unknown Twilio argument",
        ),
        (
            "browser-assets",
            vec!["--enable", "--assets", "."],
            false,
            "unknown Twilio argument",
        ),
    ] {
        let result = run(
            main_command(project).args(args),
            logs,
            &format!("compiled-main-{label}"),
            Duration::from_secs(5),
        );
        assert_eq!(result.status.success(), success);
        assert!(if success {
            &result.stdout
        } else {
            &result.stderr
        }
        .contains(expected));
        assert_redacted(&result, None);
    }
    let reserved = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = reserved.local_addr().unwrap();
    drop(reserved);
    for (label, key, value) in [
        (
            "bad-account",
            "TWILIO_ACCOUNT_SID",
            "synthetic-invalid-account",
        ),
        ("bad-hangup", "PIPECAT_TWILIO_HANGUP", "automatic"),
        (
            "bad-canonical-url",
            "PIPECAT_TWILIO_WEBHOOK_URL",
            "http://127.0.0.1:1/wrong",
        ),
        ("missing-provider", "OPENAI_API_KEY", ""),
    ] {
        let result = run(
            configured_main(project, address).env(key, value),
            logs,
            &format!("compiled-main-{label}"),
            Duration::from_secs(5),
        );
        assert!(!result.status.success(), "{label} unexpectedly launched");
        assert_redacted(&result, None);
        assert!(
            TcpListener::bind(address).is_ok(),
            "rejected config retained a listener"
        );
    }
    let label = "compiled-main-signed-http";
    let mut child = spawn(&mut configured_main(project, address), logs, label);
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        assert!(
            child.child.try_wait().unwrap().is_none(),
            "main exited before startup"
        );
        check_logs(logs, label);
        check_disk(logs, 0, label);
        if let Ok(response) = http(address, "GET", "/", "", &[]) {
            assert_eq!(response.status, 404);
            break;
        }
        assert!(Instant::now() < deadline, "main never bound its listener");
        std::thread::sleep(Duration::from_millis(25));
    }
    for (method, path) in [
        ("POST", "/api/start"),
        ("GET", "/config.json"),
        ("GET", "/index.html"),
        ("GET", "/client"),
    ] {
        assert_eq!(http(address, method, path, "", &[]).unwrap().status, 404);
    }
    let fields = BTreeMap::from([("AccountSid", ACCOUNT), ("CallSid", CALL)]);
    let body = format!("AccountSid={ACCOUNT}&CallSid={CALL}");
    let webhook = format!("http://{address}/twilio/voice");
    assert_eq!(
        http(address, "POST", "/twilio/voice", &body, &[])
            .unwrap()
            .status,
        403
    );
    let wrong = signature("https://127.0.0.1:1/twilio/voice", &fields);
    assert_eq!(
        http(
            address,
            "POST",
            "/twilio/voice",
            &body,
            &[
                ("X-Twilio-Signature", &wrong),
                ("X-Forwarded-Proto", "https"),
                ("X-Forwarded-Host", "127.0.0.1:1"),
            ]
        )
        .unwrap()
        .status,
        403,
        "forwarded headers must not replace the configured canonical URL"
    );
    let signed = signature(&webhook, &fields);
    let accepted = http(
        address,
        "POST",
        "/twilio/voice",
        &body,
        &[("X-Twilio-Signature", &signed)],
    )
    .unwrap();
    assert_eq!(accepted.status, 200);
    assert!(accepted.headers.contains("content-type: application/xml"));
    assert!(accepted.headers.contains("cache-control: no-store"));
    let media = accepted
        .body
        .split_once("<Connect><Stream url=\"")
        .expect("production main did not return TwiML")
        .1
        .split_once('"')
        .unwrap()
        .0;
    assert!(accepted.body.ends_with(" /></Connect></Response>"));
    let (base, capability) = media.rsplit_once('/').unwrap();
    assert_eq!(base, format!("ws://{address}/twilio/media"));
    assert_eq!(capability.len(), 64);
    assert!(capability.bytes().all(|byte| byte.is_ascii_hexdigit()));
    fs::write(
        logs.join("compiled-main-synthetic-twiml.xml"),
        &accepted.body,
    )
    .unwrap();
    assert_eq!(
        http(
            address,
            "POST",
            "/twilio/voice",
            &body,
            &[("X-Twilio-Signature", &signed)]
        )
        .unwrap()
        .status,
        409
    );
    // The capability is deliberately not upgraded. No connected/start/audio or
    // provider-triggering work reaches the production binary's hosted endpoints.
    assert!(Command::new("/bin/kill")
        .args(["-INT", &child.child.id().to_string()])
        .status()
        .unwrap()
        .success());
    let output = finish(&mut child, logs, label, Duration::from_secs(10));
    successful(&output);
    assert!(output.stdout.contains("Twilio listener on "));
    assert!(output
        .stdout
        .contains("Twilio shutdown: joined_connections="));
    for counter in [
        "failed_calls=0",
        "pipeline_join_errors=0",
        "media_cleanup_errors=0",
        "hangup_errors=0",
        "task_errors=0",
    ] {
        assert!(
            output.stdout.contains(counter),
            "missing clean shutdown counter {counter}"
        );
    }
    assert_redacted(&output, Some(capability));
    drop(TcpListener::bind(address).expect("main must release its listener after Ctrl-C"));
}

fn replace_exact(text: &mut String, from: &str, to: &str, count: usize) {
    assert_eq!(
        text.matches(from).count(),
        count,
        "review fixture drift: {from}"
    );
    *text = text.replace(from, to);
}

fn install_loopback_tests(project: &Path) {
    assert_eq!(
        fs::read_to_string(source().join("crates/pipecat-runner/tests/twilio_flow.rs")).unwrap(),
        FLOW,
        "the compiled acceptance fixture differs from the frozen native corpus"
    );
    let mut manifest = fs::read_to_string(project.join("Cargo.toml")).unwrap();
    assert!(!manifest.contains("[dev-dependencies]"));
    manifest.push_str(
        "\n# Acceptance fixtures only. These are added after the pristine binary build.\n\
         [dev-dependencies]\nbase64 = \"0.22\"\nhmac = \"0.12\"\nsha1 = \"0.10\"\n\
         serde_json = \"1\"\nurl = \"2\"\nfutures-util = \"0.3\"\n\
         reqwest = { version = \"0.12\", default-features = false, features = [\"rustls-tls\", \"json\"] }\n\
         tokio-tungstenite = { version = \"0.28\", features = [\"rustls-tls-webpki-roots\"] }\n",
    );
    for name in ["pipecat-audio", "pipecat-services", "pipecat-transports"] {
        let path =
            serde_json::to_string(source().join("crates").join(name).to_str().unwrap()).unwrap();
        manifest.push_str(&format!("{name} = {{ path = {path} }}\n"));
    }
    fs::write(project.join("Cargo.toml"), manifest).unwrap();
    fs::create_dir(project.join("tests")).unwrap();
    let mut adapted = FLOW.to_owned();
    replace_exact(
        &mut adapted,
        "TwilioApp::new(",
        "generated_twilio_check::create_app(",
        1,
    );
    replace_exact(
        &mut adapted,
        "        let reports = app.subscribe();",
        "        fixture_owners().apps.lock().unwrap().push((app.clone(), address));\n        let reports = app.subscribe();",
        1,
    );
    replace_exact(
        &mut adapted,
        "use pipecat_transports::{Packet, SessionConfig, TransportError, WebSocketSession};",
        "use pipecat_transports::{Packet, SessionConfig, TransportError, WebSocketSession as NativeWebSocketSession};",
        1,
    );
    replace_exact(&mut adapted, "    task::JoinSet,\n", "", 1);
    replace_exact(&mut adapted, "JoinSet::new()", "FixtureTasks::new()", 4);
    replace_exact(&mut adapted, "JoinSet<()>", "FixtureTasks", 1);
    replace_exact(&mut adapted, "tokio::spawn(", "spawn_shutdown_waiter(", 2);
    replace_exact(
        &mut adapted,
        "            assert_eq!(summary.error, Some(TwilioError::Pipeline));\n            assert!(summary.generated_context.messages.is_empty());",
        "            assert_eq!(summary.error, Some(TwilioError::Pipeline));\n            assert_eq!(summary.pipeline_join_error, Some(TwilioError::Pipeline));\n            assert!(summary.media_cleanup_error.is_none());\n            assert!(summary.generated_context.messages.is_empty());",
        1,
    );
    // Preserve every original assertion and timeout. Only constructor dispatch
    // and ownership wrappers change, with explicit drift counts for each seam.
    assert_eq!(adapted.matches("#[tokio::test]").count(), CASES.len());
    for name in CASES {
        replace_exact(
            &mut adapted,
            &format!("#[tokio::test]\nasync fn {name}() {{"),
            &format!("async fn original_{name}() {{"),
            1,
        );
    }
    adapted.push_str(OWNED_FIXTURE_SUPPORT);
    for name in CASES {
        adapted.push_str(&format!(
            "\n#[tokio::test]\nasync fn {name}() {{\n    run_owned_fixture(\"{name}\", original_{name}()).await;\n}}\n"
        ));
    }
    assert!(
        !adapted.contains("TwilioApp::new("),
        "direct constructor fallback forbidden"
    );
    assert_eq!(
        adapted
            .matches("generated_twilio_check::create_app(")
            .count(),
        1
    );
    assert_eq!(adapted.matches("#[tokio::test]").count(), 10);
    fs::write(project.join("tests/generated_flow.rs"), adapted).unwrap();
}

#[test]
#[ignore = "independent offline Rust 1.99 Twilio build, compiled-main HTTP and full local phone integration; caller serializes builds"]
fn generated_twilio_app_builds_and_runs_local_phone() {
    let root = tempfile::Builder::new()
        .prefix("pipecat-native-twilio-acceptance-")
        .tempdir()
        .unwrap()
        .keep();
    let parent = root.join("generated parent with spaces");
    fs::create_dir(&parent).unwrap();
    let mut config = pipecat_project::ProjectConfig::new(PACKAGE);
    config.transport = "twilio".into();
    config.client = "none".into();
    config.eval = "none".into();
    let plan = pipecat_project::ProjectPlan::new(config, &parent, &source()).unwrap();
    let project = pipecat_project::generate(&plan).unwrap();
    let evidence = Evidence {
        root: root.clone(),
        project: project.clone(),
    };
    let logs = root.join("commands");
    fs::create_dir(&logs).unwrap();
    check_disk(&logs, STARTUP_RESERVE, "before-independent-build");
    fs::write(
        root.join("source-rust.txt"),
        format!("{}\n", source().display()),
    )
    .unwrap();
    for (name, args) in [
        ("source-head.txt", vec!["rev-parse", "HEAD"]),
        (
            "source-status.txt",
            vec!["status", "--porcelain=v1", "--untracked-files=all"],
        ),
        ("source-diff.patch", vec!["diff", "--binary", "HEAD"]),
    ] {
        let output = Command::new("git")
            .args(args)
            .current_dir(source())
            .output()
            .unwrap();
        assert!(output.status.success());
        fs::write(root.join(name), output.stdout).unwrap();
    }
    let native = native_manifest(&source());
    write_json(&root.join("native-source-before.json"), &native);
    assert!(!project.join("target").exists());
    assert!(!project.join("Cargo.lock").exists());
    assert!(!project.join("client").exists());
    assert!(!project.join("evals").exists());
    let main = fs::read_to_string(project.join("src/main.rs")).unwrap();
    assert!(main.contains("generated_twilio_check::run()"));
    let library = fs::read_to_string(project.join("src/lib.rs")).unwrap();
    assert_eq!(library.matches("TwilioApp::new(").count(), 1);
    assert!(library.contains("create_app"));
    assert!(library.contains("run_from_env(create_app)"));
    assert!(!library.contains("NativeRunner"));
    assert!(!library.contains("new_with_webrtc"));
    let pristine = snapshot_project(&project, &root.join("pristine-generated-source"));
    write_json(&root.join("pristine-generated-source.json"), &pristine);
    successful(&run(
        &mut cargo(&project, &["generate-lockfile", "--offline"]),
        &logs,
        "pristine-generate-lockfile",
        Duration::from_secs(120),
    ));
    let pristine_lock = fs::read(project.join("Cargo.lock")).unwrap();
    fs::write(root.join("pristine-Cargo.lock"), &pristine_lock).unwrap();
    generated_metadata(&project, &logs, "pristine-resolved-metadata");
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
        "build-pristine-generated-main",
        Duration::from_secs(1800),
    ));
    let pristine_executable = executable_manifest(&project);
    assert_eq!(pristine_executable.len(), 1);
    assert!(pristine_executable.contains_key(&format!("target/debug/{PACKAGE}")));
    write_json(
        &root.join("pristine-main-executable.json"),
        &pristine_executable,
    );
    assert_eq!(fs::read(project.join("Cargo.lock")).unwrap(), pristine_lock);
    freeze_native(&source(), &root, &native, "after-pristine-build");
    compiled_main_smoke(&project, &logs);
    assert_eq!(executable_manifest(&project), pristine_executable);
    for (name, expected) in &pristine {
        assert_eq!(
            &fingerprint(&project.join(name)),
            expected,
            "production source mutated"
        );
    }
    install_loopback_tests(&project);
    successful(&run(
        &mut cargo(&project, &["generate-lockfile", "--offline"]),
        &logs,
        "fixture-generate-lockfile",
        Duration::from_secs(120),
    ));
    generated_metadata(&project, &logs, "fixture-resolved-metadata");
    let frozen = snapshot_project(&project, &root.join("adapted-generated-source"));
    write_json(&root.join("adapted-generated-source.json"), &frozen);
    let output = run(
        &mut cargo(
            &project,
            &[
                "test",
                "--test",
                "generated_flow",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
                "-j",
                "1",
                "--",
                "--test-threads=1",
                "--nocapture",
            ],
        ),
        &logs,
        "generated-library-full-twilio-suite",
        Duration::from_secs(1800),
    );
    let tested_executables = executable_manifest(&project);
    write_json(
        &root.join("generated-suite-executables.json"),
        &tested_executables,
    );
    successful(&output);
    assert_eq!(
        tested_executables
            .keys()
            .filter(|path| path.starts_with("target/debug/deps/generated_flow-"))
            .count(),
        1,
        "must identify the exact independent generated fixture executable"
    );
    assert!(output.stdout.contains("10 passed; 0 failed"));
    for name in CASES.into_iter().chain(EXTRA_CASES) {
        assert!(output
            .stdout
            .contains(&format!("GENERATED_OWNERS_JOINED case={name} ")));
    }
    assert!(output
        .stdout
        .contains("GENERATED_FAILURE_GUARD_PROVED panic_and_timeout"));
    assert_eq!(
        tree_manifest(&project),
        frozen,
        "generated corpus changed during full suite"
    );
    freeze_native(&source(), &root, &native, "after-full-suite");
    let repeated = run(
        &mut cargo(
            &project,
            &[
                "test",
                "--test",
                "generated_flow",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
                "-j",
                "1",
                "--",
                "--exact",
                TWO_CALL_CASE,
                "--test-threads=1",
                "--nocapture",
            ],
        ),
        &logs,
        "generated-library-two-call-repeat",
        Duration::from_secs(120),
    );
    successful(&repeated);
    assert!(repeated.stdout.contains("1 passed; 0 failed"));
    assert!(repeated
        .stdout
        .contains(&format!("GENERATED_OWNERS_JOINED case={TWO_CALL_CASE} ")));
    assert_eq!(
        tree_manifest(&project),
        frozen,
        "generated corpus changed during repeat"
    );
    freeze_native(&source(), &root, &native, "after-repeat");
    assert_eq!(
        executable_manifest(&project),
        tested_executables,
        "the repeated phone case did not use identical executables"
    );
    successful(&run(
        &mut cargo(
            &project,
            &[
                "clippy",
                "--all-targets",
                "--offline",
                "--locked",
                "--target-dir",
                "target",
                "-j",
                "1",
                "--",
                "-D",
                "warnings",
            ],
        ),
        &logs,
        "generated-independent-all-target-clippy",
        Duration::from_secs(1800),
    ));
    assert_eq!(
        tree_manifest(&project),
        frozen,
        "generated corpus changed during strict Clippy"
    );
    freeze_native(&source(), &root, &native, "after-clippy");
    assert_eq!(executable_manifest(&project), tested_executables);
    write_json(
        &root.join("acceptance-result.json"),
        &json!({
            "pristine_main_compiled_before_fixture_dependencies": true,
            "production_main_accepted_media_sessions": 0,
            "original_phone_cases_passed": 7,
            "additional_provider_failure_and_wire_boundary_cases_passed": 2,
            "failure_cleanup_guard_cases_passed": 1,
            "repeated_two_call_cases_passed": 1,
            "unique_phone_calls_in_two_call_case": 2,
            "two_call_case_executions": 2,
            "native_source_unchanged": true,
            "generated_source_and_lock_unchanged_during_phone_runs": true,
            "generated_independent_all_target_clippy_passed": true,
            "live_provider_or_real_phone_validation": false,
        }),
    );
    drop(evidence);
}

// This support is fixture-only: it does not replace the generated library or
// any runtime transport. It preserves ownership across assertion unwind and
// existing outer test timeouts, then actually joins before reporting failure.
const OWNED_FIXTURE_SUPPORT: &str = r#"

#[derive(Default)]
struct FixtureOwners {
    apps: std::sync::Mutex<Vec<(TwilioApp, SocketAddr)>>,
    tasks: std::sync::Mutex<Vec<tokio::task::JoinSet<()>>>,
    sessions: std::sync::Mutex<Vec<NativeWebSocketSession>>,
    waiters: std::sync::Mutex<Vec<tokio::task::JoinHandle<ShutdownResult>>>,
}
tokio::task_local! {
    static FIXTURE_OWNERS: Arc<FixtureOwners>;
}
fn fixture_owners() -> Arc<FixtureOwners> {
    FIXTURE_OWNERS.with(Arc::clone)
}

struct WebSocketSession {
    session: Option<NativeWebSocketSession>,
    owners: Arc<FixtureOwners>,
}
impl std::ops::Deref for WebSocketSession {
    type Target = NativeWebSocketSession;
    fn deref(&self) -> &Self::Target { self.session.as_ref().unwrap() }
}
impl std::ops::DerefMut for WebSocketSession {
    fn deref_mut(&mut self) -> &mut Self::Target { self.session.as_mut().unwrap() }
}
impl Drop for WebSocketSession {
    fn drop(&mut self) {
        if let Some(session) = self.session.take() {
            self.owners.sessions.lock().unwrap().push(session);
        }
    }
}
impl WebSocketSession {
    async fn connect_with_request(request: Request, config: SessionConfig) -> Result<Self, TransportError> {
        let owners = fixture_owners();
        NativeWebSocketSession::connect_with_request(request, config).await
            .map(|session| Self { session: Some(session), owners })
    }
    async fn accept_with_request_policy<F>(stream: TcpStream, config: SessionConfig, policy: F)
        -> Result<Self, TransportError>
    where F: FnOnce(&tokio_tungstenite::tungstenite::handshake::server::Request) -> bool + Send + Unpin {
        let owners = fixture_owners();
        NativeWebSocketSession::accept_with_request_policy(stream, config, policy).await
            .map(|session| Self { session: Some(session), owners })
    }
}

struct FixtureTasks {
    tasks: Option<tokio::task::JoinSet<()>>,
    owners: Arc<FixtureOwners>,
}
impl FixtureTasks {
    fn new() -> Self {
        Self { tasks: Some(tokio::task::JoinSet::new()), owners: fixture_owners() }
    }
    fn spawn<F>(&mut self, future: F)
    where F: std::future::Future<Output = ()> + Send + 'static {
        self.tasks.as_mut().unwrap().spawn(FIXTURE_OWNERS.scope(self.owners.clone(), future));
    }
    async fn join_next(&mut self) -> Option<Result<(), tokio::task::JoinError>> {
        self.tasks.as_mut().unwrap().join_next().await
    }
}
impl Drop for FixtureTasks {
    fn drop(&mut self) {
        if let Some(tasks) = self.tasks.take() {
            self.owners.tasks.lock().unwrap().push(tasks);
        }
    }
}

type ShutdownResult = Result<pipecat_runner::twilio::ShutdownReport, TwilioError>;
struct ShutdownWaiter {
    task: Option<tokio::task::JoinHandle<ShutdownResult>>,
    owners: Arc<FixtureOwners>,
}
fn spawn_shutdown_waiter<F>(future: F) -> ShutdownWaiter
where F: std::future::Future<Output = ShutdownResult> + Send + 'static {
    ShutdownWaiter { task: Some(tokio::spawn(future)), owners: fixture_owners() }
}
impl ShutdownWaiter {
    fn abort(&self) { self.task.as_ref().unwrap().abort(); }
    fn is_finished(&self) -> bool { self.task.as_ref().unwrap().is_finished() }
}
impl std::future::Future for ShutdownWaiter {
    type Output = Result<ShutdownResult, tokio::task::JoinError>;
    fn poll(mut self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>)
        -> std::task::Poll<Self::Output> {
        let result = std::future::Future::poll(
            std::pin::Pin::new(self.task.as_mut().unwrap()), cx
        );
        if result.is_ready() { self.task.take(); }
        result
    }
}
impl Drop for ShutdownWaiter {
    fn drop(&mut self) {
        if let Some(task) = self.task.take() {
            self.owners.waiters.lock().unwrap().push(task);
        }
    }
}

#[derive(Debug)]
struct JoinedOwners {
    apps: usize,
    fixture_sets: usize,
    retained_peer_tasks: usize,
    retained_shutdown_waiters: usize,
    sessions: usize,
    shutdown_reports: Vec<pipecat_runner::twilio::ShutdownReport>,
    cleanup_errors: Vec<String>,
}
async fn join_owned_fixture(owners: &Arc<FixtureOwners>) -> JoinedOwners {
    let mut tasks = std::mem::take(&mut *owners.tasks.lock().unwrap());
    let apps = std::mem::take(&mut *owners.apps.lock().unwrap());
    let waiters = std::mem::take(&mut *owners.waiters.lock().unwrap());
    let mut report = JoinedOwners {
        apps: apps.len(), fixture_sets: tasks.len(), retained_peer_tasks: 0,
        retained_shutdown_waiters: waiters.len(),
        sessions: 0, shutdown_reports: Vec::new(), cleanup_errors: Vec::new(),
    };
    // A cancelled waiter never owns the application's actual supervisor.
    // Retain and join the waiter too, then finish its app through the saved clone.
    for waiter in waiters {
        waiter.abort();
        match waiter.await {
            Ok(_) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => report.cleanup_errors.push(format!("shutdown waiter join: {error}")),
        }
    }
    // Failed peers must not remain stuck on accept or held test barriers.
    // Abort alone is NOT completion: every handle is subsequently awaited.
    for set in &mut tasks {
        report.retained_peer_tasks += set.len();
        set.abort_all();
        while let Some(result) = set.join_next().await {
            if let Err(error) = result {
                if !error.is_cancelled() {
                    report.cleanup_errors.push(format!("fixture peer join: {error}"));
                }
            }
        }
    }
    // Aborted peer futures and dropped phones hand their socket owners here,
    // instead of dropping WebSocketSession's retained task handles.
    let sessions = std::mem::take(&mut *owners.sessions.lock().unwrap());
    report.sessions = sessions.len();
    for mut session in sessions {
        // An abrupt peer can return a protocol error after its real task joined.
        // Record it without replacing the original fixture failure.
        if let Err(error) = session.cancel_and_join().await {
            report.cleanup_errors.push(format!("socket join result: {error}"));
        }
    }
    for (app, address) in apps {
        match app.shutdown().await {
            Ok(shutdown) => report.shutdown_reports.push(shutdown),
            Err(error) => report.cleanup_errors.push(format!("app shutdown: {error}")),
        }
        assert_eq!(app.active_sessions(), 0, "failure cleanup retained an active call");
        assert_eq!(app.pending_sessions(), 0, "failure cleanup retained an admission");
        assert!(TcpStream::connect(address).await.is_err(), "failure cleanup retained its listener");
    }
    assert!(owners.tasks.lock().unwrap().is_empty());
    assert!(owners.sessions.lock().unwrap().is_empty());
    assert!(owners.apps.lock().unwrap().is_empty());
    assert!(owners.waiters.lock().unwrap().is_empty());
    report
}

async fn capture_owned_fixture<F>(future: F)
    -> (std::thread::Result<()>, JoinedOwners)
where F: std::future::Future<Output = ()> {
    use futures_util::FutureExt;
    let owners = Arc::new(FixtureOwners::default());
    let result = std::panic::AssertUnwindSafe(FIXTURE_OWNERS.scope(owners.clone(), future))
        .catch_unwind().await;
    let report = join_owned_fixture(&owners).await;
    (result, report)
}
async fn run_owned_fixture<F>(name: &str, future: F)
where F: std::future::Future<Output = ()> {
    let (result, report) = capture_owned_fixture(future).await;
    println!("GENERATED_OWNERS_JOINED case={name} {report:?}");
    if let Err(panic) = result {
        std::panic::resume_unwind(panic);
    }
    assert_eq!(report.retained_peer_tasks, 0, "success left peer fixtures unfinished");
    assert_eq!(report.retained_shutdown_waiters, 0, "success left shutdown waiters unfinished");
    assert!(report.cleanup_errors.is_empty(), "extra cleanup errors after success: {report:?}");
    assert_eq!(report.shutdown_reports.len(), report.apps, "every app needs its typed shutdown receipt");
    if name == "setup_failure_and_shutdown_under_phone_queue_pressure_join_the_pending_owner" {
        // These two deliberately rejected STT upgrades already report the
        // bounded startup failure through the pipeline's final join. Require
        // that exact expected diagnostic, not a blanket cleanup exemption.
        assert_eq!(report.apps, 2);
        for shutdown in &report.shutdown_reports {
            assert_eq!(shutdown.joined_connections, 2, "{shutdown:?}");
            assert_eq!(shutdown.failed_calls, 1, "{shutdown:?}");
            assert_eq!(shutdown.pipeline_join_errors, 1, "{shutdown:?}");
            assert_eq!(shutdown.media_cleanup_errors, 0, "{shutdown:?}");
            assert_eq!(shutdown.hangup_errors, 0, "{shutdown:?}");
            assert_eq!(shutdown.task_errors, 0, "{shutdown:?}");
        }
    } else {
        for shutdown in &report.shutdown_reports {
            assert!(!shutdown.failed_cleanup(), "unexpected app cleanup diagnostic: {shutdown:?}");
        }
    }
}

#[tokio::test]
async fn generated_failure_guard_joins_after_fixture_panic_and_timeout() {
    for timeout in [false, true] {
        let (result, report) = capture_owned_fixture(async move {
            let peers = Peers::new().await;
            let host = peers.app(|_| {}).await;
            let call = sid("CA", 147);
            let url = host.admit(&call).await;
            let (ready, started) = oneshot::channel();
            let mut tasks = FixtureTasks::new();
            tasks.spawn(async move {
                stt_call(&peers.stt, None, Some(ready)).await;
            });
            let _phone = Phone::connect(&url, call, sid("MZ", 147)).await;
            started.await.unwrap();
            if timeout {
                tokio::time::timeout(Duration::from_millis(10), std::future::pending::<()>())
                    .await.expect("synthetic fixture timeout exercises retained cleanup");
            } else {
                panic!("synthetic fixture panic exercises retained cleanup");
            }
        }).await;
        assert!(result.is_err(), "failure guard scenario unexpectedly succeeded");
        assert_eq!(report.apps, 1);
        assert_eq!(report.fixture_sets, 1);
        assert_eq!(report.retained_peer_tasks, 1);
        assert_eq!(report.sessions, 2, "both phone and provider owners must be joined");
        assert!(report.cleanup_errors.is_empty(), "failure-guard cleanup failed: {report:?}");
        assert_eq!(report.shutdown_reports.len(), 1);
        assert!(!report.shutdown_reports[0].failed_cleanup(), "{report:?}");
        println!("GENERATED_EXPECTED_FAILURE_JOINED timeout={timeout} {report:?}");
    }
    println!("GENERATED_FAILURE_GUARD_PROVED panic_and_timeout");
}

#[tokio::test]
async fn generated_established_stt_failure_joins_the_phone_and_providers() {
    run_owned_fixture("generated_established_stt_failure_joins_the_phone_and_providers", async {
        tokio::time::timeout(Duration::from_secs(15), async {
            let peers = Peers::new().await;
            let mut host = peers.app(|_| {}).await;
            let call = sid("CA", 148);
            let url = host.admit(&call).await;
            let Peers { stt, llm, tts, hangup } = peers;
            let mut tasks = FixtureTasks::new();
            tasks.spawn(async move {
                let mut session = accept_stt(&stt).await;
                // Receiving actual decoded/resampled phone audio proves this is
                // an established pipeline, not an HTTP-upgrade/setup rejection.
                let packet = tokio::time::timeout(IO_BOUND, session.recv()).await.unwrap().unwrap();
                let Packet::Binary(audio) = packet else { panic!("expected established STT PCM"); };
                assert!(!audio.is_empty());
                assert!(audio.len().is_multiple_of(2));
                assert!(audio.as_chunks::<2>().0.iter().all(|sample| *sample == [8, 0]));
                send(&session, json!({
                    "type":"Error", "status_code":401,
                    "description":"synthetic permanent failure after accepted phone audio"
                })).await;
                assert!(tokio::time::timeout(IO_BOUND, session.recv()).await.unwrap().is_none());
                session.finish_and_join().await.unwrap();
                assert_no_connection(&stt).await;
            });
            let mut phone = Phone::connect(&url, call.clone(), sid("MZ", 148)).await;
            phone.audio().await;
            phone.closed(true).await;
            let summary = host.report().await;
            assert_eq!(summary.call_sid, call);
            assert!(summary.verified_start);
            assert_eq!(summary.error, Some(TwilioError::Pipeline));
            assert!(!summary.successful());
            assert!(summary.generated_context.messages.is_empty());
            assert!(summary.pipeline_join_error.is_none(), "{summary:?}");
            assert!(summary.media_cleanup_error.is_none(), "{summary:?}");
            assert!(summary.hangup_error.is_none(), "{summary:?}");
            assert_eq!(host.app.active_sessions(), 0);
            assert_eq!(host.app.pending_sessions(), 0);
            host.shutdown().await;
            join_fixtures(&mut tasks).await;
            for unused in [&llm, &tts, &hangup] { assert_no_connection(unused).await; }
        }).await.expect("established STT failure must join the real phone, pipeline and peers");
    }).await;
}

#[tokio::test]
async fn generated_binary_and_unsupported_events_are_rejected_before_and_after_start() {
    run_owned_fixture("generated_binary_and_unsupported_events_are_rejected_before_and_after_start", async {
        tokio::time::timeout(Duration::from_secs(20), async {
            for established in [false, true] {
                for binary in [false, true] {
                    let peers = Peers::new().await;
                    let mut host = peers.app(|_| {}).await;
                    let call = sid("CA", 149 + usize::from(established) * 2 + usize::from(binary));
                    let stream = sid("MZ", 149);
                    let url = host.admit(&call).await;
                    let Peers { stt, llm, tts, hangup } = peers;
                    let stt = Arc::new(stt);
                    let mut tasks = FixtureTasks::new();
                    let (ready, started) = oneshot::channel();
                    if established {
                        let peer = stt.clone();
                        tasks.spawn(async move {
                            stt_call(&peer, None, Some(ready)).await;
                        });
                    } else {
                        drop(ready);
                    }
                    let mut phone = WebSocketSession::connect_with_request(
                        signed_upgrade(&url), session_config()
                    ).await.unwrap();
                    send(&phone, json!({"event":"connected","protocol":"Call","version":"1.0.0"})).await;
                    if established {
                        send(&phone, start(&call, &stream)).await;
                        started.await.unwrap();
                    }
                    if binary {
                        phone.sender().send(Packet::Binary(vec![0, 1, 2, 3].into())).await.unwrap();
                    } else {
                        send(&phone, json!({
                            "event":"synthetic-unsupported-event", "streamSid":stream,
                            "sequenceNumber":if established { "2" } else { "1" }
                        })).await;
                    }
                    assert!(tokio::time::timeout(IO_BOUND, phone.recv()).await.unwrap().is_none());
                    join_phone(&mut phone, true).await;
                    let summary = host.report().await;
                    assert_eq!(summary.call_sid, call);
                    assert_eq!(summary.verified_start, established);
                    assert!(matches!(summary.error, Some(TwilioError::Protocol(_))), "{summary:?}");
                    assert!(summary.generated_context.messages.is_empty());
                    assert!(summary.pipeline_join_error.is_none(), "{summary:?}");
                    assert!(summary.media_cleanup_error.is_none(), "{summary:?}");
                    assert!(summary.hangup_error.is_none(), "{summary:?}");
                    host.shutdown().await;
                    join_fixtures(&mut tasks).await;
                    // Before a verified start even STT must stay completely untouched.
                    // After start there is one joined STT peer and no reconnect.
                    assert_no_connection(&stt).await;
                    for unused in [&llm, &tts, &hangup] { assert_no_connection(unused).await; }
                }
            }
        }).await.expect("binary/unsupported phone events must fail with joined local owners");
    }).await;
}
"#;
