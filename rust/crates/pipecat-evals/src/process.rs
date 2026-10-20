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
/// attempts kill and synchronous reap, but cannot report OS failure. It is not
/// evidence of successful reaping; only an observed status establishes that.
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
    session_options: SessionOptions,
    options: ProcessOptions,
    cancel: Cancellation,
    recordings: Option<Vec<(usize, RecordedAudio)>>,
) -> Result<HostedResult, EvalError> {
    run_hosted_prepared(
        command,
        scenario,
        bot_url,
        session_options,
        options,
        cancel,
        recordings,
        #[cfg(feature = "bot-audio-transcription")]
        None,
        #[cfg(feature = "user-speech")]
        None,
    )
    .await
}

/// Share exactly the same child, pipe and session teardown for plain sessions
/// and an independently prepared spoken-response configuration.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_hosted_prepared(
    command: NativeCommand,
    scenario: ScriptScenario,
    bot_url: String,
    mut session_options: SessionOptions,
    options: ProcessOptions,
    cancel: Cancellation,
    recordings: Option<Vec<(usize, RecordedAudio)>>,
    #[cfg(feature = "bot-audio-transcription")] transcription: Option<
        crate::BotTranscriptionConfig,
    >,
    #[cfg(feature = "user-speech")] speech: Option<crate::user_speech::PreparedHostedSpeech>,
) -> Result<HostedResult, EvalError> {
    command.validate()?;
    options.validate()?;
    let address = validate_local_url(&bot_url)?;
    session_options.stop_bot = true;
    let name = scenario.name.clone();
    #[cfg(all(feature = "user-speech", feature = "bot-audio-transcription"))]
    let (transcription, speech) = match (transcription, speech) {
        (Some(config), Some(prepared)) => {
            let clips = recordings.unwrap_or_default();
            let needs_synthesis = scenario.turns.iter().enumerate().any(|(index, turn)| {
                turn.user.is_some() && !clips.iter().any(|(recorded, _)| *recorded == index)
            });
            // A legal unused voice must not construct a synthesis/cache owner
            // for an all-WAV plan. The shared budget still owns every WAV use.
            let synthesis = if needs_synthesis {
                prepared.voice.map(|voice| (prepared.settings, voice))
            } else {
                None
            };
            let session = EvalSession::from_audio_to_audio_plan(
                scenario,
                &bot_url,
                session_options,
                config,
                synthesis,
                clips,
                prepared.budget,
            )?;
            // Keep the joint state selected: the generic hosted builder refuses
            // this composition and would mask the joint shutdown branch.
            return run_selected_hosted_speech(command, session, name, address, options, cancel)
                .await;
        }
        other => other,
    };
    #[cfg(feature = "bot-audio-transcription")]
    let mut session = match transcription {
        Some(config) => {
            EvalSession::with_bot_transcription(scenario, bot_url, session_options, config)?
        }
        None => EvalSession::new(scenario, bot_url, session_options)?,
    };
    #[cfg(not(feature = "bot-audio-transcription"))]
    let mut session = EvalSession::new(scenario, bot_url, session_options)?;
    if let Some(recordings) = recordings {
        session = session.with_recorded_audio_inputs(recordings)?;
    }
    #[cfg(feature = "user-speech")]
    if let Some(prepared) = speech {
        session = session.with_hosted_user_speech(prepared)?;
        return run_selected_hosted_speech(command, session, name, address, options, cancel).await;
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
/// The selected attempt's ordinary tail owns the exact prepared session (then
/// its original pinned run future), direct child and pipe readers. External
/// Drop/panic retains the weaker abort-owned fallback; it is not a join receipt.
#[cfg(feature = "user-speech")]
struct HostedSpeechAttempt {
    child: OwnedChild,
    pipes: JoinSet<()>,
    stdout: Capture,
    stderr: Capture,
    overflow: Cancellation,
    report: ProcessReport,
    #[cfg(test)]
    fault: Option<HostedSpeechFault>,
}
#[cfg(all(test, feature = "user-speech"))]
#[derive(Clone, Copy, PartialEq, Eq)]
enum HostedSpeechFault {
    StdoutAdapter,
    PostReadinessStatus,
    RunningWait,
    CleanupStatus,
}
#[cfg(feature = "user-speech")]
struct ProcessFailure {
    kind: &'static str,
    reason: String,
}
#[cfg(feature = "user-speech")]
impl ProcessFailure {
    fn new(kind: &'static str, reason: impl Into<String>) -> Self {
        Self {
            kind,
            reason: reason.into(),
        }
    }
    fn io(kind: &'static str, operation: &str, error: &std::io::Error) -> Self {
        // Fixed operation plus OS category, never a child-supplied path/secret.
        Self::new(kind, format!("{operation} ({:?})", error.kind()))
    }
    fn append(self, result: &mut ScriptResult) {
        failure(result, self.kind, &self.reason);
    }
    fn before_cleanup(self, result: &mut ScriptResult) {
        // The process cause was retained before awaiting the cancelled session.
        // Earlier assertion failures stay earlier; cleanup and induced cancellation
        // follow the stored primary cause, rather than replacing it.
        let at = result
            .failures
            .iter()
            .position(|failure| {
                failure.kind.ends_with("_cleanup_error") || failure.kind == "cancelled"
            })
            .unwrap_or(result.failures.len());
        self.append(result);
        let last = result.failures.len() - 1;
        result.failures[at..=last].rotate_right(1);
    }
}
#[cfg(feature = "user-speech")]
async fn run_selected_hosted_speech(
    command: NativeCommand,
    session: EvalSession,
    name: String,
    address: std::net::SocketAddr,
    options: ProcessOptions,
    cancel: Cancellation,
) -> Result<HostedResult, EvalError> {
    let mut hosted = if cancel.is_cancelled() {
        HostedResult {
            result: summary(
                &name,
                "cancelled",
                "evaluation cancelled before process launch",
            ),
            process: ProcessReport::default(),
        }
    } else {
        let deadline = Instant::now() + options.startup_timeout;
        match spawn_native(&command, address, &cancel, deadline).await {
            Err((kind, reason)) => HostedResult {
                result: summary(&name, kind, &reason),
                process: ProcessReport::default(),
            },
            Ok(child) => {
                let attempt = HostedSpeechAttempt {
                    report: ProcessReport {
                        pid: Some(child.id()),
                        ..Default::default()
                    },
                    child: OwnedChild(child),
                    pipes: JoinSet::new(),
                    stdout: Capture::default(),
                    stderr: Capture::default(),
                    overflow: Cancellation::new(),
                    #[cfg(test)]
                    fault: None,
                };
                attempt
                    .run(session, &command, &options, &cancel, deadline)
                    .await
            }
        }
    };
    // Serialization is deliberately after session, child and pipe settlement.
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
#[cfg(feature = "user-speech")]
impl HostedSpeechAttempt {
    #[cfg(test)]
    fn take_fault(&mut self, fault: HostedSpeechFault) -> bool {
        if self.fault == Some(fault) {
            self.fault.take();
            true
        } else {
            false
        }
    }
    /// No fallible post-spawn operation returns past this owned tail.
    async fn run(
        mut self,
        mut session: EvalSession,
        command: &NativeCommand,
        options: &ProcessOptions,
        cancel: &Cancellation,
        startup_deadline: Instant,
    ) -> HostedResult {
        let mut setup = Vec::new();
        let stdout = self
            .child
            .0
            .stdout
            .take()
            .map(tokio::process::ChildStdout::from_std);
        #[cfg(test)]
        let stdout = if self.take_fault(HostedSpeechFault::StdoutAdapter) {
            drop(stdout);
            Some(Err(std::io::Error::other(
                "injected stdout adapter failure",
            )))
        } else {
            stdout
        };
        match stdout {
            Some(Ok(stdout)) => drain(
                stdout,
                options.max_stdout_bytes,
                self.stdout.clone(),
                self.overflow.clone(),
                &mut self.pipes,
            ),
            Some(Err(error)) => setup.push(ProcessFailure::io(
                "process_output",
                "stdout adapter failed",
                &error,
            )),
            None => setup.push(ProcessFailure::new(
                "process_output",
                "native stdout pipe is missing",
            )),
        }
        match self
            .child
            .0
            .stderr
            .take()
            .map(tokio::process::ChildStderr::from_std)
        {
            Some(Ok(stderr)) => drain(
                stderr,
                options.max_stderr_bytes,
                self.stderr.clone(),
                self.overflow.clone(),
                &mut self.pipes,
            ),
            Some(Err(error)) => setup.push(ProcessFailure::io(
                "process_output",
                "stderr adapter failed",
                &error,
            )),
            None => setup.push(ProcessFailure::new(
                "process_output",
                "native stderr pipe is missing",
            )),
        }
        let mut startup = if setup.is_empty() {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => Some(ProcessFailure::new("cancelled", "cancelled during process readiness")),
                _ = self.overflow.cancelled() => Some(ProcessFailure::new("limit_exceeded", "process output limit or read failure")),
                status = wait_child(&mut self.child) => Some(match status {
                    Ok(_) => ProcessFailure::new("process_exit", "process exited before readiness"),
                    Err(error) => ProcessFailure::io("process_wait", "readiness child wait failed", &error),
                }),
                _ = tokio::time::sleep_until(startup_deadline) => Some(ProcessFailure::new("startup_timeout", "process readiness timed out")),
                connected = session.connect_hosted() => connected.err().map(|error| ProcessFailure::new("connect_failed", error.to_string())),
            }
        } else {
            Some(setup.remove(0))
        };
        if startup.is_none() {
            let status = self.child.try_wait();
            #[cfg(test)]
            let status = if self.take_fault(HostedSpeechFault::PostReadinessStatus) {
                Err(std::io::Error::other(
                    "injected post-readiness status failure",
                ))
            } else {
                status
            };
            startup = match status {
                Ok(None) => None,
                Ok(Some(_)) => Some(ProcessFailure::new(
                    "process_exit",
                    "process exited after readiness",
                )),
                Err(error) => Some(ProcessFailure::io(
                    "process_wait",
                    "post-readiness child status failed",
                    &error,
                )),
            };
        }
        let mut result = if let Some(primary) = startup {
            // Preserve this exact prepared session; shutdown records its cleanup
            // failures after the primary failure, including before run started.
            session
                .stop_hosted_before_run(primary.kind, &primary.reason)
                .await
        } else {
            let session_cancel = Cancellation::new();
            let future = session.run(session_cancel.clone());
            tokio::pin!(future);
            #[cfg(test)]
            let fail_wait = self.take_fault(HostedSpeechFault::RunningWait);
            let cause = tokio::select! {
                biased;
                _ = cancel.cancelled() => SomeResult::Stopped("cancelled", "cancelled during evaluation"),
                _ = self.overflow.cancelled() => SomeResult::Stopped("limit_exceeded", "process output limit or read failure"),
                status = async {
                    #[cfg(test)]
                    if fail_wait {
                        return Err(std::io::Error::other("injected running child wait failure"));
                    }
                    wait_child(&mut self.child).await
                } => SomeResult::Exited(status),
                result = &mut future => SomeResult::Ready(result),
            };
            match cause {
                SomeResult::Ready(result) => result,
                SomeResult::Stopped(kind, reason) => {
                    let primary = ProcessFailure::new(kind, reason);
                    session_cancel.cancel();
                    let mut result = future.await;
                    primary.before_cleanup(&mut result);
                    result
                }
                SomeResult::Exited(status) => {
                    let primary = match status {
                        Ok(status) if status.success() => None,
                        Ok(_) => Some(ProcessFailure::new(
                            "process_exit",
                            "process exited during evaluation",
                        )),
                        Err(error) => Some(ProcessFailure::io(
                            "process_wait",
                            "evaluation child wait failed",
                            &error,
                        )),
                    };
                    if primary.is_some() {
                        session_cancel.cancel();
                    }
                    let mut result = tokio::select! {
                        biased;
                        _ = cancel.cancelled() => {
                            let stopped = ProcessFailure::new("cancelled", "cancelled after process exit");
                            session_cancel.cancel();
                            let mut result = future.await;
                            stopped.before_cleanup(&mut result);
                            result
                        },
                        _ = self.overflow.cancelled() => {
                            let stopped = ProcessFailure::new("limit_exceeded", "process output retention exceeded");
                            session_cancel.cancel();
                            let mut result = future.await;
                            stopped.before_cleanup(&mut result);
                            result
                        },
                        result = &mut future => result,
                    };
                    if let Some(primary) = primary {
                        primary.before_cleanup(&mut result);
                    }
                    result
                }
            }
        };
        for error in setup {
            error.before_cleanup(&mut result);
        }
        self.reap(options, cancel, &mut result).await;
        self.join_output(command, options, &mut result).await;
        if cancel.is_cancelled()
            && !result
                .failures
                .iter()
                .any(|failure| failure.kind == "cancelled")
        {
            failure(&mut result, "cancelled", "cancelled during process cleanup");
        }
        HostedResult {
            result,
            process: self.report,
        }
    }

    async fn reap(
        &mut self,
        options: &ProcessOptions,
        cancel: &Cancellation,
        result: &mut ScriptResult,
    ) {
        let status = self.child.try_wait();
        #[cfg(test)]
        let status = if self.take_fault(HostedSpeechFault::CleanupStatus) {
            Err(std::io::Error::other(
                "injected cleanup child status failure",
            ))
        } else {
            status
        };
        let mut status = match status {
            Ok(status) => status,
            Err(error) => {
                ProcessFailure::io(
                    "process_cleanup_error",
                    "child status failed during cleanup",
                    &error,
                )
                .append(result);
                None
            }
        };
        if status.is_none() {
            status = tokio::select! {
                biased;
                _ = cancel.cancelled() => None,
                _ = self.overflow.cancelled() => None,
                waited = wait_child(&mut self.child) => match waited {
                    Ok(status) => Some(status),
                    Err(error) => {
                        ProcessFailure::io("process_cleanup_error", "child wait failed during cleanup", &error).append(result);
                        None
                    }
                },
                _ = tokio::time::sleep(options.shutdown_grace) => None,
            };
        }
        if status.is_none() {
            self.report.forced = true;
            let killed = self.child.0.kill();
            // Always attempt an actual wait even when status/kill failed. Normal
            // polls remain async; an OS wait error gets a direct final reap attempt.
            let waited = wait_child(&mut self.child).await;
            status = match waited {
                Ok(status) => Some(status),
                Err(error) => {
                    ProcessFailure::io(
                        "process_cleanup_error",
                        "child reap polling failed",
                        &error,
                    )
                    .append(result);
                    match self.child.0.wait() {
                        Ok(status) => Some(status),
                        Err(error) => {
                            ProcessFailure::io(
                                "process_cleanup_error",
                                "direct child could not be reaped",
                                &error,
                            )
                            .append(result);
                            None
                        }
                    }
                }
            };
            // A failed kill followed by successful wait is the normal exit race.
            if status.is_none() {
                if let Err(error) = killed {
                    ProcessFailure::io("process_cleanup_error", "child kill failed", &error)
                        .append(result);
                }
            }
        }
        if let Some(status) = status {
            self.report.reaped = true;
            self.report.exit_code = status.code();
            self.report.success = status.success();
            if !status.success() && !self.report.forced {
                failure(
                    result,
                    "process_exit",
                    "native process exited unsuccessfully",
                );
            }
        }
    }

    async fn join_output(
        &mut self,
        command: &NativeCommand,
        options: &ProcessOptions,
        result: &mut ScriptResult,
    ) {
        let deadline = Instant::now() + options.output_drain_timeout;
        loop {
            tokio::select! {
                biased;
                _ = tokio::time::sleep_until(deadline) => {
                    // Abort is only the request; shutdown actually awaits every
                    // retained reader before this attempt releases its slot.
                    self.pipes.shutdown().await;
                    break;
                },
                joined = self.pipes.join_next() => match joined {
                    None => break,
                    Some(Ok(())) => {},
                    Some(Err(_)) => failure(result, "process_output", "output reader task failed"),
                },
            }
        }
        // Native mutexes are acquired only after all asynchronous joins.
        let out = self.stdout.lock().unwrap();
        let err = self.stderr.lock().unwrap();
        self.report.output_overflow = out.overflow || err.overflow;
        self.report.output_incomplete =
            !out.complete || !err.complete || out.read_error || err.read_error;
        if !self.report.output_overflow && !self.report.output_incomplete {
            match (
                redact(
                    &String::from_utf8_lossy(&out.bytes),
                    command,
                    options.max_stdout_bytes,
                ),
                redact(
                    &String::from_utf8_lossy(&err.bytes),
                    command,
                    options.max_stderr_bytes,
                ),
            ) {
                (Some(stdout), Some(stderr)) => {
                    self.report.stdout = stdout;
                    self.report.stderr = stderr;
                }
                _ => self.report.output_overflow = true,
            }
        }
        if self.report.output_overflow {
            failure(
                result,
                "limit_exceeded",
                "process output retention exceeded",
            );
        }
        if self.report.output_incomplete {
            failure(
                result,
                "process_output",
                "process pipes did not finish cleanly before the drain deadline",
            );
        }
    }
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

#[cfg(all(test, feature = "user-speech"))]
pub(crate) mod hosted_speech_attempt_tests {
    use super::*;
    use crate::user_speech::{CartesiaUserSpeechSettings, PreparedHostedSpeech, UserSpeechBudget};
    use pipecat_speech::SpeechConfig;
    use pipecat_transports::{Packet, SessionConfig, WebSocketSession};
    use serde_json::json;

    const WAIT: Duration = Duration::from_secs(3);

    // Test-harness entry, not a substantive acceptance case. Only this compiled
    // unit-test executable sees this private fixture switch; no production env
    // or executable route can enable an injected failure.
    #[test]
    fn direct_child_fixture() {
        if std::env::var_os("PIPECAT_127_UNIT_CHILD").is_none() {
            return;
        }
        use std::io::Write;
        // The adapter-error case closes stdout before this child is scheduled.
        // BrokenPipe must not turn the intended held process into an early exit.
        let _ = writeln!(std::io::stdout().lock(), "owned native unit child");
        let _ = writeln!(std::io::stderr().lock(), "owned native unit diagnostic");
        std::thread::sleep(WAIT + WAIT);
    }

    #[cfg(feature = "bot-audio-transcription")]
    pub(crate) fn transcription(base: &str) -> crate::BotTranscriptionConfig {
        use pipecat_vad::{RuntimeOptions, SileroModel, SileroVadAnalyzer, VadParams};
        let runtime = std::env::var_os("PIPECAT_ONNX_RUNTIME")
            .map(PathBuf::from)
            .unwrap_or_else(|| "/usr/lib/x86_64-linux-gnu/libonnxruntime.so.1.21.0".into());
        let model = std::env::var_os("PIPECAT_SILERO_MODEL")
            .map(PathBuf::from)
            .unwrap_or_else(|| {
                PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                    .join("../../../src/pipecat/audio/vad/data/silero_vad.onnx")
            });
        let mut model = SileroModel::load(
            &RuntimeOptions {
                library_path: runtime,
            },
            model,
        )
        .expect("preinstalled native fixture assets must load");
        model.predict_pcm16(&[0; 512], 16_000).unwrap();
        model.reset();
        let analyzer = SileroVadAnalyzer::new(
            model,
            16_000,
            VadParams {
                stop_secs: 0.4,
                ..Default::default()
            },
        )
        .unwrap();
        crate::BotTranscriptionConfig::new(
            analyzer,
            SpeechConfig {
                base_url: base.into(),
                api_key: "synthetic-joint-owner-stt-key".into(),
                timeout: Duration::from_secs(10),
                max_audio_bytes: 2 * 1024 * 1024,
                max_json_bytes: 64 * 1024,
                queue_capacity: 1,
            },
            pipecat_speech::TranscriptionRequest::default(),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn post_spawn_adapter_status_and_wait_errors_keep_the_actual_owned_tail() {
        post_spawn_errors(false).await;
    }

    #[cfg(feature = "bot-audio-transcription")]
    #[tokio::test]
    async fn joint_post_spawn_errors_keep_the_original_prepared_session_and_child_pipes() {
        post_spawn_errors(true).await;
    }

    #[cfg(all(unix, feature = "bot-audio-transcription"))]
    #[tokio::test]
    async fn joint_pre_run_cancel_and_spawn_error_start_no_provider_cache_or_child_work() {
        use std::sync::atomic::{AtomicU64, Ordering};
        static NEXT: AtomicU64 = AtomicU64::new(0);
        struct Temp(PathBuf);
        impl Drop for Temp {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        let temp = Temp(std::env::temp_dir().join(format!(
            "pipecat-joint-pre-run-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        )));
        std::fs::create_dir(&temp.0).unwrap();
        let provider = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        provider.set_nonblocking(true).unwrap();
        let endpoint = format!("http://{}", provider.local_addr().unwrap());
        let cache = temp.0.join("never-open-cache");
        for already_cancelled in [true, false] {
            for all_wav in [false, true] {
                let reservation = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                let url = format!("ws://{}", reservation.local_addr().unwrap());
                drop(reservation);
                let scenario = serde_json::from_value(json!({"name":"prepared joint owner", "turns":[{
                    "user":"reference", "expect":[{"event":"response", "text_contains":"spoken"}]
                }]})).unwrap();
                let mut config = SpeechConfig::cartesia("synthetic-pre-run-tts-key");
                config.base_url = endpoint.clone();
                let prepared = PreparedHostedSpeech {
                    settings: CartesiaUserSpeechSettings::new(
                        config,
                        crate::UserSpeechCachePolicy::private_directory(&cache).unwrap(),
                    )
                    .unwrap(),
                    voice: Some(
                        crate::CartesiaUserSpeechVoice::new("unused-or-dormant", None, None)
                            .unwrap(),
                    ),
                    budget: UserSpeechBudget::new(if all_wav { 2 } else { 0 }).unwrap(),
                };
                let command = NativeCommand {
                    executable: temp.0.join("missing-native-executable"),
                    args: Vec::new(),
                    cwd: temp.0.clone(),
                    env: BTreeMap::new(),
                };
                let cancel = Cancellation::new();
                if already_cancelled {
                    cancel.cancel();
                }
                // Configuration owns only an initialized analyzer and unused
                // HTTP client here. No PCM worker exists until drive starts.
                let result = run_hosted_prepared(
                    command,
                    scenario,
                    url,
                    SessionOptions::default(),
                    ProcessOptions::default(),
                    cancel,
                    all_wav.then(|| vec![(0, RecordedAudio::from_pcm16(&[7, 0]).unwrap())]),
                    Some(transcription(&format!("{endpoint}/v1"))),
                    Some(prepared),
                )
                .await
                .unwrap();
                assert_eq!(
                    result.result.failures[0].kind,
                    if already_cancelled {
                        "cancelled"
                    } else {
                        "spawn_failed"
                    }
                );
                assert!(result.process.pid.is_none());
                assert!(!cache.exists());
                assert_eq!(
                    provider.accept().unwrap_err().kind(),
                    std::io::ErrorKind::WouldBlock
                );
            }
        }
    }

    async fn post_spawn_errors(joint: bool) {
        for fault in [
            HostedSpeechFault::StdoutAdapter,
            HostedSpeechFault::PostReadinessStatus,
            HostedSpeechFault::RunningWait,
            HostedSpeechFault::CleanupStatus,
        ] {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let url = format!("ws://{}", listener.local_addr().unwrap());
            let stop_peer = Cancellation::new();
            let peer_stop = stop_peer.clone();
            let mut peer = tokio::spawn(async move {
                let accepted = tokio::select! {
                    biased;
                    _ = peer_stop.cancelled() => return false,
                    accepted = listener.accept() => accepted,
                };
                let (stream, _) = accepted.unwrap();
                let mut socket = WebSocketSession::accept(
                    stream,
                    SessionConfig {
                        close_timeout: Duration::from_millis(50),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                let mut stopped = false;
                while let Some(packet) = socket.recv().await {
                    let bytes = match packet {
                        Packet::Text(text) => text.into_bytes(),
                        Packet::Binary(bytes) => bytes.to_vec(),
                    };
                    match crate::serializer::decode_control(&bytes, 1024 * 1024).unwrap() {
                        crate::serializer::EvalControl::Cancel => stopped = true,
                        crate::serializer::EvalControl::Rtvi(message)
                            if message.message_type == "client-ready" =>
                        {
                            socket
                                .sender()
                                .send(Packet::Text(
                                    json!({
                                        "label": "rtvi-ai", "type": "bot-ready", "data": {}
                                    })
                                    .to_string(),
                                ))
                                .await
                                .unwrap();
                        }
                        _ => {}
                    }
                }
                let _terminal = socket.cancel_and_join().await;
                stopped
            });
            let scenario =
                crate::ScenarioFile::from_value(json!({"name": "post-spawn errors", "turns": []}))
                    .unwrap()
                    .scenarios
                    .remove(0);
            let mut config = SpeechConfig::cartesia("synthetic-process-tail-key");
            config.base_url = "http://127.0.0.1:1".into();
            let options = SessionOptions {
                stop_bot: true,
                ..Default::default()
            };
            let prepared = PreparedHostedSpeech {
                settings: CartesiaUserSpeechSettings::new(
                    config,
                    crate::UserSpeechCachePolicy::Disabled,
                )
                .unwrap(),
                voice: None,
                budget: UserSpeechBudget::new(0).unwrap(),
            };
            let session = if joint {
                #[cfg(feature = "bot-audio-transcription")]
                {
                    EvalSession::from_audio_to_audio_plan(
                        scenario,
                        &url,
                        options,
                        transcription("http://127.0.0.1:1/v1"),
                        None,
                        Vec::new(),
                        prepared.budget,
                    )
                    .unwrap()
                }
                #[cfg(not(feature = "bot-audio-transcription"))]
                unreachable!("joint fixture requires both features")
            } else {
                EvalSession::new(scenario, &url, options)
                    .unwrap()
                    .with_hosted_user_speech(prepared)
                    .unwrap()
            };
            let command = NativeCommand {
                executable: std::env::current_exe().unwrap(),
                args: vec![
                    "--exact".into(),
                    "process::hosted_speech_attempt_tests::direct_child_fixture".into(),
                    "--nocapture".into(),
                ],
                cwd: std::env::current_dir().unwrap(),
                env: BTreeMap::from([("PIPECAT_127_UNIT_CHILD".into(), "1".into())]),
            };
            let child = Command::new(&command.executable)
                .args(&command.args)
                .current_dir(&command.cwd)
                .env_clear()
                .envs(&command.env)
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
                .unwrap();
            let attempt = HostedSpeechAttempt {
                report: ProcessReport {
                    pid: Some(child.id()),
                    ..Default::default()
                },
                child: OwnedChild(child),
                pipes: JoinSet::new(),
                stdout: Capture::default(),
                stderr: Capture::default(),
                overflow: Cancellation::new(),
                fault: Some(fault),
            };
            let options = ProcessOptions {
                shutdown_grace: Duration::from_millis(25),
                output_drain_timeout: WAIT,
                ..Default::default()
            };
            let result = attempt
                .run(
                    session,
                    &command,
                    &options,
                    &Cancellation::new(),
                    Instant::now() + WAIT,
                )
                .await;
            stop_peer.cancel();
            let peer_outcome = tokio::time::timeout(WAIT, &mut peer).await;
            let stop_seen = match peer_outcome {
                Ok(outcome) => outcome.unwrap(),
                Err(_) => {
                    peer.abort();
                    let terminal = peer.await;
                    panic!("peer did not settle; abort was actually joined: {terminal:?}");
                }
            };
            assert!(!result.result.passed);
            assert!(result.process.reaped);
            if fault == HostedSpeechFault::StdoutAdapter {
                // The libtest harness may exit on its own stdout BrokenPipe
                // before this helper starts. Reaping is required either way.
                assert_eq!(result.result.failures[0].kind, "process_output");
                assert!(result.process.output_incomplete);
                assert!(!stop_seen);
            } else {
                assert!(result.process.forced);
                assert!(
                    stop_seen,
                    "the original connected session sent its cleanup control"
                );
                assert!(!result.process.output_incomplete);
                assert_eq!(
                    result.result.failures[0].kind,
                    if fault == HostedSpeechFault::CleanupStatus {
                        "process_cleanup_error"
                    } else {
                        "process_wait"
                    }
                );
            }
        }
    }
}
