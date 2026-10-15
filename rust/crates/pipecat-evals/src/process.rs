// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
//! Direct native-child ownership for scripted evaluations. See README for the
//! direct-child-only process boundary and operating-system reap limitations.
use crate::{
    matcher::{AssertionFailure, ScriptResult},
    suite::serialized_size,
    Cancellation, EvalError, EvalSession, RecordedAudio, ScriptScenario, SessionOptions,
};
use serde::Serialize;
use std::{
    collections::BTreeMap,
    fmt,
    io::Read,
    path::PathBuf,
    process::{Child, Command, ExitStatus, Stdio},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt},
    task::JoinSet,
    time::Instant,
};

/// An explicit trusted executable, with literal argv and an empty inherited
/// environment. Paths must be absolute. No PATH search, shell or dotenv loader.
#[derive(Clone)]
pub struct NativeCommand {
    pub executable: PathBuf,
    pub args: Vec<String>,
    pub cwd: PathBuf,
    /// The complete child environment. All nonempty values are exact-match
    /// redacted from captured output; never put credentials in arguments.
    pub env: BTreeMap<String, String>,
}
impl fmt::Debug for NativeCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("NativeCommand")
            .field("argument_count", &self.args.len())
            .field("environment_entries", &self.env.len())
            .finish_non_exhaustive()
    }
}
impl NativeCommand {
    pub fn validate(&self) -> Result<(), EvalError> {
        if !self.executable.is_absolute() || !self.cwd.is_absolute() {
            return Err(EvalError::Scenario(
                "native executable and cwd must be absolute".into(),
            ));
        }
        if self.args.len() > 256
            || self.env.len() > 128
            || self.args.iter().map(String::len).sum::<usize>() > 65536
            || self
                .env
                .iter()
                .map(|(k, v)| k.len() + v.len())
                .sum::<usize>()
                > 65536
            || self.args.iter().any(|s| s.contains('\0'))
            || self
                .env
                .iter()
                .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
        {
            return Err(EvalError::Scenario(
                "invalid or oversized native argv/environment".into(),
            ));
        }
        // A script path must never silently invoke the upstream Python runtime.
        if self.executable.extension().is_some_and(|s| {
            matches!(
                s.to_str().unwrap_or_default().to_ascii_lowercase().as_str(),
                "py" | "sh" | "bat" | "cmd" | "ps1"
            )
        }) {
            return Err(EvalError::Unsupported(
                "script executable; supply a compiled native bot".into(),
            ));
        }
        match self.executable.metadata() {
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
            Ok(metadata) => {
                if !metadata.is_file() {
                    return Err(EvalError::Unsupported(
                        "native executable must be a regular file".into(),
                    ));
                }
                let mut header = [0u8; 4];
                match std::fs::File::open(&self.executable)?.read_exact(&mut header) {
                    Err(error) if error.kind() != std::io::ErrorKind::UnexpectedEof => {
                        return Err(error.into())
                    }
                    result => {
                        if result.is_err() || !supported_native_header(header) {
                            return Err(EvalError::Unsupported(
                                "native executable format (expected ELF, PE/MZ or Mach-O)".into(),
                            ));
                        }
                    }
                }
            }
        }
        Ok(())
    }
}
// A compatibility check only: neither magic bytes nor a path inspection prove
// executable trust, and the path can change before spawn on an untrusted FS.
fn supported_native_header(header: [u8; 4]) -> bool {
    header == *b"\x7fELF"
        || header[..2] == *b"MZ"
        || matches!(
            u32::from_be_bytes(header),
            0xfeedface
                | 0xcefaedfe
                | 0xfeedfacf
                | 0xcffaedfe
                | 0xcafebabe
                | 0xbebafeca
                | 0xcafebabf
                | 0xbfbafeca
        )
}
#[derive(Debug, Clone)]
pub struct ProcessOptions {
    pub startup_timeout: Duration,
    pub shutdown_grace: Duration,
    pub output_drain_timeout: Duration,
    pub max_stdout_bytes: usize,
    pub max_stderr_bytes: usize,
    /// JSON size of the complete returned HostedResult, not only its events.
    pub max_result_bytes: usize,
}
impl Default for ProcessOptions {
    fn default() -> Self {
        Self {
            startup_timeout: Duration::from_secs(60),
            shutdown_grace: Duration::from_secs(2),
            output_drain_timeout: Duration::from_secs(1),
            max_stdout_bytes: 64 * 1024,
            max_stderr_bytes: 64 * 1024,
            max_result_bytes: 16 * 1024 * 1024,
        }
    }
}
impl ProcessOptions {
    pub fn validate(&self) -> Result<(), EvalError> {
        if [
            self.startup_timeout,
            self.shutdown_grace,
            self.output_drain_timeout,
        ]
        .iter()
        .any(|d| d.is_zero() || *d > Duration::from_secs(3600))
            || !(1..=1024 * 1024).contains(&self.max_stdout_bytes)
            || !(1..=1024 * 1024).contains(&self.max_stderr_bytes)
            || !(1024..=64 * 1024 * 1024).contains(&self.max_result_bytes)
        {
            return Err(EvalError::Scenario(
                "process limits: timeouts 0..1h, streams 1..1MiB, result 1KiB..64MiB".into(),
            ));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, Serialize)]
pub struct HostedResult {
    pub result: ScriptResult,
    pub process: ProcessReport,
}
#[derive(Debug, Clone, Serialize, Default)]
pub struct ProcessReport {
    pub pid: Option<u32>,
    pub exit_code: Option<i32>,
    pub success: bool,
    pub reaped: bool,
    pub forced: bool,
    pub stdout: String,
    pub stderr: String,
    pub output_overflow: bool,
    pub output_incomplete: bool,
}

/// Owns the child even when the caller drops/aborts the async evaluation. Drop
/// kills and synchronously reaps the direct child; no detached reaper is needed.
/// OS kill/wait cannot promise a deadline for a kernel-stuck child.
struct OwnedChild(Child);
impl OwnedChild {
    fn try_wait(&mut self) -> std::io::Result<Option<ExitStatus>> {
        self.0.try_wait()
    }
    fn kill_and_reap(&mut self) -> std::io::Result<ExitStatus> {
        if let Some(status) = self.0.try_wait()? {
            return Ok(status);
        }
        let killed = self.0.kill();
        // Always reap, including the race in which the child exits before kill.
        let waited = self.0.wait();
        if waited.is_err() {
            killed?;
        }
        waited
    }
}
impl Drop for OwnedChild {
    fn drop(&mut self) {
        let _ = self.kill_and_reap();
    }
}
#[derive(Default)]
struct Captured {
    bytes: Vec<u8>,
    overflow: bool,
    complete: bool,
    read_error: bool,
}
type Capture = Arc<Mutex<Captured>>;
fn drain<R: AsyncRead + Unpin + Send + 'static>(
    mut reader: R,
    max: usize,
    capture: Capture,
    overflow: Cancellation,
    tasks: &mut JoinSet<()>,
) {
    tasks.spawn(async move {
        let mut buf = [0; 4096];
        loop {
            match reader.read(&mut buf).await {
                Ok(0) => {
                    capture.lock().unwrap().complete = true;
                    break;
                }
                Ok(n) => {
                    let mut state = capture.lock().unwrap();
                    if n > max.saturating_sub(state.bytes.len()) {
                        state.overflow = true;
                        // Never retain a truncated prefix of a secret on overflow.
                        state.bytes.clear();
                        overflow.cancel();
                    } else if !state.overflow {
                        state.bytes.extend_from_slice(&buf[..n]);
                    }
                }
                Err(_) => {
                    capture.lock().unwrap().read_error = true;
                    overflow.cancel();
                    break;
                }
            }
        }
    });
}
async fn wait_child(child: &mut OwnedChild) -> std::io::Result<ExitStatus> {
    loop {
        if let Some(status) = child.try_wait()? {
            return Ok(status);
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
fn failure(result: &mut ScriptResult, kind: &str, reason: &str) {
    result.fail(AssertionFailure {
        turn_index: -1,
        expectation_index: -1,
        event_name: "process".into(),
        kind: kind.into(),
        reason: reason.into(),
    });
}
fn summary(name: &str, kind: &str, reason: &str) -> ScriptResult {
    let end = name
        .char_indices()
        .map(|(i, _)| i)
        .take_while(|i| *i <= 64)
        .last()
        .unwrap_or(0);
    let name = if name.len() <= 64 { name } else { &name[..end] };
    let mut result = ScriptResult::new(name, 0);
    failure(&mut result, kind, reason);
    result
}
fn redact(text: &str, command: &NativeCommand, max_bytes: usize) -> Option<String> {
    let mut values = command
        .env
        .values()
        .filter(|v| !v.is_empty())
        .collect::<Vec<_>>();
    values.sort_by_key(|s| std::cmp::Reverse(s.len()));
    values.dedup();
    // Match only original bytes. Replacement markers are never searched again,
    // preventing cascading expansion when one allowed value is a single letter.
    let mut output = String::new();
    let mut remaining = text;
    while !remaining.is_empty() {
        if let Some(value) = values.iter().find(|v| remaining.starts_with(v.as_str())) {
            if output.len() + "[REDACTED]".len() > max_bytes {
                return None;
            }
            output.push_str("[REDACTED]");
            remaining = &remaining[value.len()..];
        } else {
            let len = remaining.chars().next().unwrap().len_utf8();
            if output.len() + len > max_bytes {
                return None;
            }
            output.push_str(&remaining[..len]);
            remaining = &remaining[len..];
        }
    }
    Some(output)
}
/// Validate before spawning, connect with a startup retry deadline, execute one
/// real RTVI session, then stop and reap the child and join both pipe readers.
/// Hosted endpoints must be literal loopback URLs. Each run needs an exclusive
/// port; a loopback endpoint is not proof of server identity.
pub async fn run_hosted(
    command: NativeCommand,
    scenario: ScriptScenario,
    bot_url: String,
    session_options: SessionOptions,
    options: ProcessOptions,
    cancel: Cancellation,
) -> Result<HostedResult, EvalError> {
    run_hosted_with_recordings(
        command,
        scenario,
        bot_url,
        session_options,
        options,
        cancel,
        None,
    )
    .await
}

pub(crate) async fn run_hosted_with_recordings(
    command: NativeCommand,
    scenario: ScriptScenario,
    bot_url: String,
    mut session_options: SessionOptions,
    options: ProcessOptions,
    cancel: Cancellation,
    recordings: Option<Vec<(usize, RecordedAudio)>>,
) -> Result<HostedResult, EvalError> {
    command.validate()?;
    options.validate()?;
    let address = validate_local_url(&bot_url)?;
    session_options.stop_bot = true;
    let name = scenario.name.clone();
    let mut session = EvalSession::new(scenario, bot_url, session_options)?;
    if let Some(recordings) = recordings {
        session = session.with_recorded_audio_inputs(recordings)?;
    }
    let mut report = ProcessReport::default();
    let result = if cancel.is_cancelled() {
        summary(
            &name,
            "cancelled",
            "evaluation cancelled before process launch",
        )
    } else {
        let startup_deadline = Instant::now() + options.startup_timeout;
        match spawn_native(&command, address, &cancel, startup_deadline).await {
            Err((kind, message)) => summary(&name, kind, &message),
            Ok(child) => {
                let mut child = OwnedChild(child);
                report.pid = Some(child.0.id());
                let stdout = tokio::process::ChildStdout::from_std(child.0.stdout.take().unwrap())?;
                let stderr = tokio::process::ChildStderr::from_std(child.0.stderr.take().unwrap())?;
                let out = Capture::default();
                let err = Capture::default();
                let overflow = Cancellation::new();
                let mut tasks = JoinSet::new();
                drain(
                    stdout,
                    options.max_stdout_bytes,
                    out.clone(),
                    overflow.clone(),
                    &mut tasks,
                );
                drain(
                    stderr,
                    options.max_stderr_bytes,
                    err.clone(),
                    overflow.clone(),
                    &mut tasks,
                );
                let startup = tokio::select! {
                    biased;
                    _=cancel.cancelled()=>Some(("cancelled", "cancelled during process readiness")),
                    _=overflow.cancelled()=>Some(("limit_exceeded", "process output limit or read failure")),
                    _=wait_child(&mut child)=>Some(("process_exit", "process exited before readiness")),
                    _=tokio::time::sleep_until(startup_deadline)=>Some(("startup_timeout", "process readiness timed out")),
                    result=session.connect_hosted()=>{ result?; None },
                };
                let startup = if startup.is_none() && child.try_wait()?.is_some() {
                    Some(("process_exit", "process exited after readiness"))
                } else {
                    startup
                };
                let mut result = if let Some((kind, message)) = startup {
                    session.shutdown().await;
                    summary(&name, kind, message)
                } else {
                    let session_cancel = Cancellation::new();
                    let future = session.run(session_cancel.clone());
                    tokio::pin!(future);
                    let cause = tokio::select! {
                        biased;
                        _=cancel.cancelled()=>SomeResult::Stopped("cancelled", "cancelled during evaluation"),
                        _=overflow.cancelled()=>SomeResult::Stopped("limit_exceeded", "process output limit or read failure"),
                        status=wait_child(&mut child)=>SomeResult::Exited(status),
                        result=&mut future=>{
                            // Keep normal session cleanup inside its own owner.
                            SomeResult::Ready(result) },
                    };
                    match cause {
                        SomeResult::Ready(result) => result,
                        SomeResult::Exited(status) => {
                            let abnormal = !status.as_ref().is_ok_and(|s| s.success());
                            if abnormal {
                                session_cancel.cancel();
                            }
                            let mut result = tokio::select! {
                                biased;
                                _ = cancel.cancelled() => {
                                    session_cancel.cancel();
                                    let mut result = future.await;
                                    failure(&mut result, "cancelled", "cancelled after process exit");
                                    result
                                }
                                _ = overflow.cancelled() => {
                                    session_cancel.cancel();
                                    let mut result = future.await;
                                    failure(&mut result, "limit_exceeded", "process output retention exceeded");
                                    result
                                }
                                result = &mut future => result,
                            };
                            if abnormal {
                                failure(
                                    &mut result,
                                    "process_exit",
                                    "process exited during evaluation",
                                );
                            }
                            result
                        }
                        SomeResult::Stopped(kind, message) => {
                            session_cancel.cancel();
                            let mut result = future.await;
                            failure(&mut result, kind, message);
                            result
                        }
                    }
                };
                let status = if let Some(status) = child.try_wait()? {
                    Some(status)
                } else {
                    tokio::select! {
                        biased;
                        _=cancel.cancelled()=>None,
                        _=overflow.cancelled()=>None,
                        status=wait_child(&mut child)=>Some(status?),
                        _=tokio::time::sleep(options.shutdown_grace)=>None,
                    }
                };
                let status = match status {
                    Some(status) => status,
                    None => {
                        report.forced = true;
                        // Mandatory reaping is async on normal shutdown; only
                        // Drop uses blocking wait as its last-resort guarantee.
                        let killed = child.0.kill();
                        match child.try_wait()? {
                            Some(status) => status,
                            None => {
                                killed?;
                                wait_child(&mut child).await?
                            }
                        }
                    }
                };
                report.reaped = true;
                report.exit_code = status.code();
                report.success = status.success();
                if !status.success() && !report.forced {
                    failure(
                        &mut result,
                        "process_exit",
                        "native process exited unsuccessfully",
                    );
                }
                if cancel.is_cancelled() && !result.failures.iter().any(|f| f.kind == "cancelled") {
                    failure(&mut result, "cancelled", "cancelled during process cleanup");
                }
                let deadline = Instant::now() + options.output_drain_timeout;
                loop {
                    tokio::select! {
                        biased;
                        _=tokio::time::sleep_until(deadline)=>{ tasks.shutdown().await; break; },
                        joined=tasks.join_next()=>match joined {
                            None=>break, Some(Ok(()))=>{}, Some(Err(_))=>{
                                failure(&mut result,"process_output","output reader task failed");
                            }
                        }
                    }
                }
                let out = out.lock().unwrap();
                let err = err.lock().unwrap();
                report.output_overflow = out.overflow || err.overflow;
                report.output_incomplete =
                    !out.complete || !err.complete || out.read_error || err.read_error;
                // Incomplete output is omitted: an EOF-less prefix can contain a
                // partial secret that an exact-match redactor cannot recognize.
                if !report.output_overflow && !report.output_incomplete {
                    match (
                        redact(
                            &String::from_utf8_lossy(&out.bytes),
                            &command,
                            options.max_stdout_bytes,
                        ),
                        redact(
                            &String::from_utf8_lossy(&err.bytes),
                            &command,
                            options.max_stderr_bytes,
                        ),
                    ) {
                        (Some(stdout), Some(stderr)) => {
                            report.stdout = stdout;
                            report.stderr = stderr;
                        }
                        _ => {
                            report.output_overflow = true;
                        }
                    }
                }
                if cancel.is_cancelled() && !result.failures.iter().any(|f| f.kind == "cancelled") {
                    failure(&mut result, "cancelled", "cancelled during output cleanup");
                }
                if report.output_overflow {
                    failure(
                        &mut result,
                        "limit_exceeded",
                        "process output retention exceeded",
                    );
                }
                if report.output_incomplete {
                    failure(
                        &mut result,
                        "process_output",
                        "process pipes did not finish cleanly before the drain deadline",
                    );
                }
                result
            }
        }
    };
    let mut hosted = HostedResult {
        result,
        process: report,
    };
    if serialized_size(&hosted)? > options.max_result_bytes {
        hosted.result = summary(
            &name,
            "limit_exceeded",
            "hosted result budget exceeded; full result and process output omitted",
        );
        hosted.process.stdout.clear();
        hosted.process.stderr.clear();
    }
    if serialized_size(&hosted)? > options.max_result_bytes {
        return Err(EvalError::Limit("hosted summary size"));
    }
    Ok(hosted)
}
enum SomeResult {
    Ready(ScriptResult),
    Exited(std::io::Result<ExitStatus>),
    Stopped(&'static str, &'static str),
}
static LAUNCH: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
async fn spawn_native(
    command: &NativeCommand,
    address: std::net::SocketAddr,
    cancel: &Cancellation,
    deadline: Instant,
) -> Result<Child, (&'static str, String)> {
    // A concurrent fork can inherit another launch's close-on-exec reservation
    // until it execs. Serialize only this short synchronous launch phase so no
    // hosted fork can capture another hosted launch's temporary listener.
    let _launch = tokio::select! {
        biased;
        _ = cancel.cancelled() => return Err(("cancelled", "cancelled while waiting for native process launch".into())),
        _ = tokio::time::sleep_until(deadline) => return Err(("startup_timeout", "native process launch admission timed out".into())),
        guard = LAUNCH.lock() => guard,
    };
    if cancel.is_cancelled() {
        return Err(("cancelled", "cancelled before native process launch".into()));
    }
    if Instant::now() >= deadline {
        return Err((
            "startup_timeout",
            "native process launch admission timed out".into(),
        ));
    }
    // Reject known preexisting listeners without touching them. Holding this
    // reservation across spawn would prevent ordinary native bots from binding;
    // the unavoidable release-to-bind race is a documented caller constraint.
    let reservation = std::net::TcpListener::bind(address).map_err(|error| {
        (
            "port_unavailable",
            format!(
                "hosted loopback port cannot be reserved ({:?})",
                error.kind()
            ),
        )
    })?;
    drop(reservation);
    Command::new(&command.executable)
        .args(&command.args)
        .current_dir(&command.cwd)
        .env_clear()
        .envs(&command.env)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| {
            (
                "spawn_failed",
                format!("native process spawn failed ({:?})", error.kind()),
            )
        })
}
pub(crate) fn validate_local_url(raw: &str) -> Result<std::net::SocketAddr, EvalError> {
    let url =
        url::Url::parse(raw).map_err(|_| EvalError::Scenario("invalid hosted bot URL".into()))?;
    let ip = match url.host() {
        Some(url::Host::Ipv4(ip)) if ip.is_loopback() => std::net::IpAddr::V4(ip),
        Some(url::Host::Ipv6(ip)) if ip.is_loopback() => std::net::IpAddr::V6(ip),
        _ => {
            return Err(EvalError::Scenario(
                "hosted bot URL requires a literal loopback IP".into(),
            ))
        }
    };
    if url.scheme() != "ws"
        || !url.username().is_empty()
        || url.password().is_some()
        || url.fragment().is_some()
        || url.port_or_known_default().is_none_or(|port| port == 0)
    {
        return Err(EvalError::Scenario(
            "hosted bot URL must use ws:// with a nonzero port and no credentials/fragment".into(),
        ));
    }
    Ok(std::net::SocketAddr::new(
        ip,
        url.port_or_known_default().unwrap(),
    ))
}

#[cfg(test)]
mod tests {
    use super::{spawn_native, supported_native_header, NativeCommand, LAUNCH};
    use crate::Cancellation;
    use std::{collections::BTreeMap, time::Duration};
    use tokio::time::Instant;
    #[test]
    fn supported_native_headers_are_explicit_and_text_is_rejected() {
        for header in [
            *b"\x7fELF",
            *b"MZ\x00\x00",
            [0xfe, 0xed, 0xfa, 0xcf],
            [0xca, 0xfe, 0xba, 0xbe],
        ] {
            assert!(supported_native_header(header));
        }
        for header in [*b"#!/b", *b"echo", [0; 4], *b"PK\x03\x04"] {
            assert!(!supported_native_header(header));
        }
    }
    #[tokio::test]
    async fn launcher_admission_is_cancellable_timed_and_keeps_the_runtime_live() {
        let _held = LAUNCH.lock().await;
        let command = NativeCommand {
            executable: std::env::temp_dir().join("pipecat-admission-missing-executable"),
            cwd: std::env::temp_dir(),
            args: vec![],
            env: BTreeMap::new(),
        };
        let cancel = Cancellation::new();
        let notifier = cancel.clone();
        let timer = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            notifier.cancel();
        });
        let result = spawn_native(
            &command,
            "127.0.0.1:1".parse().unwrap(),
            &cancel,
            Instant::now() + Duration::from_secs(1),
        )
        .await;
        assert_eq!(result.unwrap_err().0, "cancelled");
        timer.await.unwrap();
        let result = spawn_native(
            &command,
            "127.0.0.1:1".parse().unwrap(),
            &Cancellation::new(),
            Instant::now() + Duration::from_millis(10),
        )
        .await;
        assert_eq!(result.unwrap_err().0, "startup_timeout");
        drop(_held);
        // A free gate must reject an expired deadline before inspecting this
        // occupied endpoint or attempting to launch the missing executable.
        let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let result = spawn_native(
            &command,
            occupied.local_addr().unwrap(),
            &Cancellation::new(),
            Instant::now() - Duration::from_millis(1),
        )
        .await;
        assert_eq!(result.unwrap_err().0, "startup_timeout");
    }
}
