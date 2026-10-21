//! Test-only child HTTP/configuration owner; no browser or provider requests.
use pipecat_browser_client::Config;
use serde_json::{json, Value};
use std::{
    fs::{self, File},
    io::Read,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    time::{Duration, Instant},
};

pub type Outcome<T> = Result<T, String>;
const LIMIT: usize = 16 * 1024;

struct Files(PathBuf);
impl Files {
    fn new() -> Outcome<Self> {
        let this =
            Self(std::env::temp_dir().join(format!("pipecat-host-test-{}", uuid::Uuid::new_v4())));
        fs::create_dir(&this.0).map_err(|e| e.to_string())?;
        fs::create_dir(this.0.join("pkg")).map_err(|e| e.to_string())?;
        for (name, bytes) in [
            (
                "index.html",
                b"<!doctype html><title>HTTP fixture only</title>".as_slice(),
            ),
            ("style.css", b"/* HTTP fixture only */".as_slice()),
            (
                "pkg/pipecat_browser_client.js",
                b"// HTTP fixture only, never executed".as_slice(),
            ),
            (
                "pkg/pipecat_browser_client_bg.wasm",
                b"\0asm\x01\0\0\0".as_slice(),
            ),
        ] {
            fs::write(this.0.join(name), bytes).map_err(|e| e.to_string())?;
        }
        fs::write(this.0.join("config.json"), json!({"project_name":"Untrusted supplied name", "start_endpoint":"https://invalid.example/start", "transports":["smallwebrtc"], "input_mode":"text", "unexpected":"must not merge"}).to_string()).map_err(|e| e.to_string())?;
        Ok(this)
    }
    fn read(&self, name: &str) -> Outcome<String> {
        let mut bytes = vec![];
        File::open(self.0.join(name))
            .map_err(|e| e.to_string())?
            .take((LIMIT + 1) as u64)
            .read_to_end(&mut bytes)
            .map_err(|e| e.to_string())?;
        if bytes.len() > LIMIT {
            return Err(format!("{name} exceeds fixture output bound"));
        }
        String::from_utf8(bytes).map_err(|e| e.to_string())
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
struct OwnedChild(Option<Child>);
impl Drop for OwnedChild {
    fn drop(&mut self) {
        if let Some(mut child) = self.0.take() {
            // Emergency ownership fallback only; successful cases explicitly reap.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}
#[derive(Debug)]
pub struct Capture {
    pub text: String,
    pub raw: Vec<u8>,
    pub error: Option<String>,
}
impl Capture {
    fn read(files: &Files, name: &str) -> Self {
        let mut raw = Vec::new();
        let result = File::open(files.0.join(name))
            .and_then(|file| file.take((LIMIT + 1) as u64).read_to_end(&mut raw));
        let error = match result {
            Err(error) => Some(error.to_string()),
            Ok(_) if raw.len() > LIMIT => Some(format!(
                "{name} exceeds fixture output bound; prefix retained"
            )),
            Ok(_) if std::str::from_utf8(&raw).is_err() => {
                Some(format!("{name} is not UTF-8; raw prefix retained"))
            }
            Ok(_) => None,
        };
        Self {
            text: String::from_utf8_lossy(&raw).into_owned(),
            raw,
            error,
        }
    }
    fn valid(&self) -> bool {
        self.error.is_none() && self.raw == self.text.as_bytes()
    }
}
#[derive(Debug)]
pub struct Probe {
    pub config: Outcome<(Config, Value)>,
    pub status: Option<ExitStatus>,
    pub stdout: Capture,
    pub stderr: Capture,
    pub cleanup: Vec<String>,
}
impl Probe {
    pub fn into_config(self) -> Outcome<Config> {
        eprintln!("host child observation={self:?}");
        if !self.stdout.valid() || !self.stderr.valid() {
            return Err(format!(
                "child output: stdout={:?}; stderr={:?}; status={:?}; cleanup={:?}",
                self.stdout, self.stderr, self.status, self.cleanup
            ));
        }
        if !self.cleanup.is_empty() {
            return Err(format!("child cleanup: {:?}", self.cleanup));
        }
        if !self.status.is_some_and(|s| s.success()) {
            return Err(format!(
                "child exit {:?}: {}",
                self.status, self.stderr.text
            ));
        }
        let (config, value) = self.config?;
        if !self.stdout.text.starts_with("Native browser agent at ") {
            return Err("missing startup observation".into());
        }
        if value["project_name"] != "Pipecat Rust Voice Agent"
            || value["start_endpoint"] != "/api/start"
            || value.get("unexpected").is_some()
        {
            return Err("hosted config override/precedence changed".into());
        }
        Ok(config)
    }
}

pub async fn hosted(extra: &[&str]) -> Outcome<Probe> {
    hosted_with_env(
        extra,
        &[
            ("DEEPGRAM_API_KEY", "synthetic-no-provider-request"),
            ("OPENAI_API_KEY", "synthetic-no-provider-request"),
            ("CARTESIA_API_KEY", "synthetic-no-provider-request"),
        ],
    )
    .await
}
pub async fn hosted_with_env(extra: &[&str], environment: &[(&str, &str)]) -> Outcome<Probe> {
    let files = Files::new()?;
    let stdout = File::create(files.0.join("stdout")).map_err(|e| e.to_string())?;
    let stderr = File::create(files.0.join("stderr")).map_err(|e| e.to_string())?;
    let child = Command::new(env!("CARGO_BIN_EXE_pipecat-browser-agent"))
        .env_clear()
        .envs(environment.iter().copied())
        .env("RUST_BACKTRACE", "0")
        .args(["--bind", "127.0.0.1:0", "--assets"])
        .arg(&files.0)
        .arg("--no-intro")
        .args(extra)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .map_err(|e| e.to_string())?;
    let mut owner = OwnedChild(Some(child));
    let mut origin = None;
    let observation: Outcome<(Config, Value)> = async {
        let deadline = Instant::now() + Duration::from_secs(8);
        loop {
            let text = files.read("stdout")?;
            files.read("stderr")?;
            if let Some(line) = text
                .lines()
                .find(|line| line.starts_with("Native browser agent at "))
            {
                let address = line
                    .strip_prefix("Native browser agent at ")
                    .unwrap_or_default()
                    .split(';')
                    .next()
                    .ok_or("missing host address")?;
                let url = url::Url::parse(address).map_err(|e| e.to_string())?;
                if url.scheme() != "http"
                    || url.host_str() != Some("127.0.0.1")
                    || url.port().is_none()
                {
                    return Err("unexpected child origin".into());
                }
                origin = Some(address.to_owned());
                break;
            }
            if let Some(status) = owner
                .0
                .as_mut()
                .ok_or("child missing")?
                .try_wait()
                .map_err(|e| e.to_string())?
            {
                return Err(format!(
                    "host exited before HTTP configuration: {status}; {}",
                    files.read("stderr")?
                ));
            }
            if Instant::now() >= deadline {
                return Err("host startup deadline".into());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let address = origin.as_ref().ok_or("missing origin")?;
        let http = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(2))
            .build()
            .map_err(|e| e.to_string())?;
        let mut response = http
            .get(format!("{address}/config.json"))
            .send()
            .await
            .map_err(|e| e.to_string())?;
        if response.status() != 200
            || response
                .headers()
                .get("content-type")
                .and_then(|v| v.to_str().ok())
                != Some("application/json")
        {
            return Err("config HTTP status/type".into());
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|e| e.to_string())? {
            if chunk.len() > LIMIT.saturating_sub(bytes.len()) {
                return Err("config response exceeds fixture bound".into());
            }
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes).map_err(|e| e.to_string())?;
        let config: Config = serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
        config.validate(address)?;
        Ok((config, value))
    }
    .await;
    let mut cleanup = vec![];
    let child = owner.0.as_mut().ok_or("child missing before cleanup")?;
    let mut status = child.try_wait().map_err(|e| e.to_string())?;
    if status.is_none() {
        let signal = Command::new("/bin/kill")
            .args(["-INT", &child.id().to_string()])
            .status();
        if !signal.as_ref().is_ok_and(|s| s.success()) {
            cleanup.push(format!("SIGINT delivery failed: {signal:?}"));
        }
        let deadline = Instant::now() + Duration::from_secs(5);
        while status.is_none() && Instant::now() < deadline {
            tokio::time::sleep(Duration::from_millis(10)).await;
            match child.try_wait() {
                Ok(value) => status = value,
                Err(error) => {
                    cleanup.push(format!("child wait: {error}"));
                    break;
                }
            }
        }
    }
    if status.is_none() {
        cleanup.push("child graceful deadline; fallback kill required".into());
        let killed = child.kill();
        let joined = child.wait();
        cleanup.push(format!("fallback kill={killed:?}; join={joined:?}"));
        status = joined.ok();
    }
    if status.is_some() {
        owner.0.take();
    }
    if let Some(address) = origin {
        let endpoint = address
            .strip_prefix("http://")
            .ok_or("invalid recorded origin")?;
        match tokio::time::timeout(
            Duration::from_secs(1),
            tokio::net::TcpStream::connect(endpoint),
        )
        .await
        {
            Ok(Err(error)) if error.kind() == std::io::ErrorKind::ConnectionRefused => {}
            other => cleanup.push(format!("listener not confirmed closed: {other:?}")),
        }
    }
    let stdout = Capture::read(&files, "stdout");
    let stderr = Capture::read(&files, "stderr");
    Ok(Probe {
        config: observation,
        status,
        stdout,
        stderr,
        cleanup,
    })
}

/// Reap an expected immediate command exit, retaining every bounded diagnostic.
pub async fn exit_only(args: &[&str], credentials: bool) -> Outcome<Probe> {
    let files = Files::new()?;
    let stdout = File::create(files.0.join("stdout")).map_err(|e| e.to_string())?;
    let stderr = File::create(files.0.join("stderr")).map_err(|e| e.to_string())?;
    let mut command = Command::new(env!("CARGO_BIN_EXE_pipecat-browser-agent"));
    command.env_clear().env("RUST_BACKTRACE", "0");
    if credentials {
        command
            .env("DEEPGRAM_API_KEY", "synthetic-no-provider-request")
            .env("OPENAI_API_KEY", "synthetic-no-provider-request")
            .env("CARTESIA_API_KEY", "synthetic-no-provider-request");
    }
    let child = command
        .args(["--bind", "127.0.0.1:0"])
        .args(args)
        .stdin(Stdio::null())
        .stdout(stdout)
        .stderr(stderr)
        .spawn()
        .map_err(|e| e.to_string())?;
    let mut owner = OwnedChild(Some(child));
    let child = owner.0.as_mut().ok_or("missing child")?;
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut cleanup = vec![];
    let mut status = None;
    while status.is_none() && Instant::now() < deadline {
        match child.try_wait() {
            Ok(value) => status = value,
            Err(error) => {
                cleanup.push(format!("wait failed: {error}"));
                break;
            }
        }
        if let Err(error) = files.read("stdout").and_then(|_| files.read("stderr")) {
            cleanup.push(format!("output capture: {error}"));
            break;
        }
        if status.is_none() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    if status.is_none() {
        cleanup.push("expected exit deadline or capture failure; fallback kill required".into());
        let killed = child.kill();
        let joined = child.wait();
        cleanup.push(format!("fallback kill={killed:?}; join={joined:?}"));
        status = joined.ok();
    }
    if status.is_some() {
        owner.0.take();
    }
    Ok(Probe {
        config: Err("exit-only observation, no HTTP config requested".into()),
        status,
        stdout: Capture::read(&files, "stdout"),
        stderr: Capture::read(&files, "stderr"),
        cleanup,
    })
}
