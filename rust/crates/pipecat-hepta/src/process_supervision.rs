//! Process containment for the existing owner-authorized native media endpoint.
//! The parent runs no model, opens no device and admits no conversation input.
//! Its private child handshake is liveness evidence, never an authority grant.
use crate::{MediaCommand, MediaCommandDecoder, MAX_MEDIA_COMMAND_BYTES};
use serde::Deserialize;
use std::{
    collections::BTreeMap,
    io,
    process::ExitStatus,
    process::Stdio,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader},
    process::{Child, Command},
    sync::{oneshot, Notify},
    time::{timeout, Instant},
};

#[derive(Clone, Copy, Debug)]
pub struct MediaProcessLimits {
    pub startup: Duration,
    pub shutdown: Duration,
    pub control: Duration,
    pub pipe_write: Duration,
    pub reap: Duration,
}
impl Default for MediaProcessLimits {
    fn default() -> Self {
        Self {
            startup: Duration::from_secs(120),
            shutdown: Duration::from_secs(10),
            control: Duration::from_secs(5),
            pipe_write: Duration::from_secs(2),
            reap: Duration::from_secs(5),
        }
    }
}
impl MediaProcessLimits {
    fn validate(self) -> io::Result<Self> {
        if [
            self.startup,
            self.shutdown,
            self.control,
            self.pipe_write,
            self.reap,
        ]
        .iter()
        .any(Duration::is_zero)
            || self.startup > Duration::from_secs(600)
            || self.shutdown > Duration::from_secs(30)
            || self.control > Duration::from_secs(30)
            || self.pipe_write > Duration::from_secs(30)
            || self.reap > Duration::from_secs(30)
        {
            return Err(io::Error::other("invalid media process deadlines"));
        }
        Ok(self)
    }
}

/// A killed/reaped process says nothing about audio already submitted or remote
/// effects. No acknowledgement or clean media terminal is manufactured here.
#[derive(Debug)]
pub struct MediaProcessReport {
    pub pid: u32,
    pub ready: bool,
    pub terminal_forwarded: bool,
    pub stop_requested: bool,
    pub kill_requested: bool,
    pub status: Option<ExitStatus>,
    pub error: Option<String>,
}
impl MediaProcessReport {
    pub fn succeeded(&self) -> bool {
        self.ready
            && self.terminal_forwarded
            && self.error.is_none()
            && !self.kill_requested
            && self.status.is_some_and(|status| status.success())
    }
    pub fn reaped(&self) -> bool {
        self.status.is_some()
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Ready {
    #[serde(rename = "type")]
    kind: String,
    connection_id: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ControlDone {
    #[serde(rename = "type")]
    kind: String,
    command_sequence: u64,
}

#[derive(Default)]
struct Controls {
    pending: Mutex<BTreeMap<u64, Instant>>,
    input_error: Mutex<Option<String>>,
    changed: Notify,
}
impl Controls {
    fn arm(&self, sequence: u64, deadline: Duration) -> Result<(), String> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| "control monitor poisoned")?;
        if pending.len() >= 8 || pending.contains_key(&sequence) {
            return Err("media control monitor capacity exceeded".into());
        }
        pending.insert(sequence, Instant::now() + deadline);
        self.changed.notify_one();
        Ok(())
    }
    fn acknowledge(&self, sequence: u64) -> Result<(), String> {
        let mut pending = self
            .pending
            .lock()
            .map_err(|_| "control monitor poisoned")?;
        match pending.remove(&sequence) {
            Some(deadline) if Instant::now() < deadline => {
                self.changed.notify_one();
                Ok(())
            }
            Some(_) => Err("native media control acknowledgement arrived after deadline".into()),
            None => Err("unexpected native media control acknowledgement".into()),
        }
    }
    fn deadline(&self) -> Result<Option<Instant>, String> {
        self.pending
            .lock()
            .map(|pending| pending.values().copied().min())
            .map_err(|_| "control monitor poisoned".into())
    }
}

fn terminal_failed(value: &serde_json::Value) -> Result<bool, String> {
    let input_errors = value
        .get("input_errors")
        .and_then(serde_json::Value::as_u64)
        .ok_or("native terminal lacks input_errors")?;
    let mut failed = input_errors != 0;
    for key in [
        "error",
        "local_stop_error",
        "input_error",
        "cleanup_error",
        "verification_error",
    ] {
        match value.get(key) {
            Some(serde_json::Value::Null) => {}
            Some(serde_json::Value::String(_)) => failed = true,
            _ => return Err(format!("native terminal has invalid {key}")),
        }
    }
    Ok(failed)
}

async fn commands<R: AsyncRead + Unpin>(
    input: R,
    mut child: tokio::process::ChildStdin,
    stop: oneshot::Sender<Instant>,
    write_deadline: Duration,
    controls: Arc<Controls>,
    control_deadline: Duration,
) -> Result<(), String> {
    let mut reader = BufReader::new(input);
    let mut decoder = MediaCommandDecoder::default();
    let mut raw = Vec::new();
    let mut stop = Some(stop);
    let mut sequence = 0u64;
    loop {
        let available = reader.fill_buf().await.map_err(|error| error.to_string())?;
        if available.is_empty() {
            decoder.finish().map_err(|error| error.to_string())?;
            if let Some(stop) = stop.take() {
                let _ = stop.send(Instant::now());
            }
            return Ok(()); // Drops this sole child-stdin owner and delivers EOF.
        }
        let (count, command) = decoder.push(available).map_err(|error| error.to_string())?;
        raw.extend_from_slice(&available[..count]);
        reader.consume(count);
        let Some(command) = command else { continue };
        sequence = sequence
            .checked_add(1)
            .ok_or("media command sequence exhausted")?;
        if matches!(
            command,
            MediaCommand::Interrupt {} | MediaCommand::Retire { .. } | MediaCommand::Turn { .. }
        ) {
            controls.arm(sequence, control_deadline)?;
        }
        let terminal = matches!(
            command,
            MediaCommand::Shutdown {} | MediaCommand::RevokeCapture { .. }
        );
        // Start the total stop deadline before waiting for a blocked child pipe.
        if terminal {
            if let Some(stop) = stop.take() {
                let _ = stop.send(Instant::now());
            }
        }
        timeout(write_deadline, child.write_all(&raw))
            .await
            .map_err(|_| "media command pipe stalled".to_string())?
            .map_err(|error| error.to_string())?;
        raw.clear();
        if terminal {
            return Ok(()); // Never forward commands following a terminal stop.
        }
    }
}

async fn bounded_line<R: AsyncRead + Unpin>(
    reader: &mut BufReader<R>,
    line: &mut Vec<u8>,
) -> Result<bool, String> {
    line.clear();
    loop {
        let available = reader.fill_buf().await.map_err(|error| error.to_string())?;
        if available.is_empty() {
            return if line.is_empty() {
                Ok(false)
            } else {
                Err("incomplete media event".into())
            };
        }
        let count = available
            .iter()
            .position(|byte| *byte == b'\n')
            .map_or(available.len(), |index| index + 1);
        if count > MAX_MEDIA_COMMAND_BYTES - line.len() {
            return Err("media event exceeds 64 KiB".into());
        }
        line.extend_from_slice(&available[..count]);
        reader.consume(count);
        if line.last() == Some(&b'\n') {
            return Ok(true);
        }
    }
}

async fn events<W: AsyncWrite + Unpin>(
    child: tokio::process::ChildStdout,
    mut output: W,
    connection_id: u64,
    ready: oneshot::Sender<Instant>,
    write_deadline: Duration,
    controls: Arc<Controls>,
    terminal_forwarded: Arc<AtomicBool>,
) -> Result<(), String> {
    let mut reader = BufReader::new(child);
    let mut line = Vec::new();
    if !bounded_line(&mut reader, &mut line).await? {
        return Err("native media exited before readiness".into());
    }
    let handshake: Ready =
        serde_json::from_slice(&line).map_err(|_| "invalid native media readiness")?;
    if handshake.kind != "media_process_ready" || handshake.connection_id != connection_id {
        return Err("native media readiness connection mismatch".into());
    }
    let _ = ready.send(Instant::now());
    // The handshake belongs only to these private pipes; preserve the owner's
    // existing external JSONL protocol. Preserve event bytes except for merging
    // an actual rejected-input cause into the child's existing terminal below.
    while bounded_line(&mut reader, &mut line).await? {
        if terminal_forwarded.load(Ordering::SeqCst) {
            return Err("native media emitted after terminal".into());
        }
        let mut value: serde_json::Value =
            serde_json::from_slice(&line).map_err(|_| "invalid native media event")?;
        let kind = value
            .get("type")
            .and_then(serde_json::Value::as_str)
            .ok_or("native media event has no type")?
            .to_owned();
        if kind == "media_process_control_done" {
            let done: ControlDone = serde_json::from_value(value)
                .map_err(|_| "invalid native media control acknowledgement")?;
            if done.kind != "media_process_control_done" {
                return Err("invalid native media control acknowledgement".into());
            }
            controls.acknowledge(done.command_sequence)?;
            continue;
        }
        let failed = if kind == "media_terminal" {
            terminal_failed(&value)?;
            if let Some(error) = controls
                .input_error
                .lock()
                .map_err(|_| "input monitor poisoned")?
                .as_ref()
            {
                // The child sees EOF after the parent rejects malformed input.
                // Preserve that actual upstream failure in its existing terminal;
                // never manufacture a terminal when the native child has none.
                value["error"] = serde_json::Value::String(match value["error"].as_str() {
                    Some(native) => format!("{native}; supervisor input: {error}"),
                    None => error.clone(),
                });
                line = serde_json::to_vec(&value).map_err(|_| "terminal encoding failed")?;
                line.push(b'\n');
                if line.len() > MAX_MEDIA_COMMAND_BYTES {
                    return Err("media terminal exceeds 64 KiB".into());
                }
            }
            terminal_failed(&value)?
        } else {
            false
        };
        timeout(write_deadline, async {
            output.write_all(&line).await?;
            output.flush().await
        })
        .await
        .map_err(|_| "media event receiver stalled".to_string())?
        .map_err(|error: io::Error| error.to_string())?;
        if kind == "media_terminal" {
            terminal_forwarded.store(true, Ordering::SeqCst);
            if failed {
                return Err("native media terminal reports failure".into());
            }
        }
    }
    Ok(())
}

async fn terminate(child: &mut Child, report: &mut MediaProcessReport, reap: Duration) {
    // try_wait closes the exit/kill race without treating a request as an exit.
    match child.try_wait() {
        Ok(Some(status)) => {
            report.status = Some(status);
            return;
        }
        Ok(None) => {}
        Err(error) => {
            report.error.get_or_insert_with(|| error.to_string());
        }
    }
    report.kill_requested = true;
    if let Err(error) = child.start_kill() {
        report.error.get_or_insert_with(|| error.to_string());
    }
    match timeout(reap, child.wait()).await {
        Ok(Ok(status)) => report.status = Some(status),
        Ok(Err(error)) => {
            report.error.get_or_insert_with(|| error.to_string());
        }
        Err(_) => {
            // A kernel-stuck process is not reported as reaped or effect-free.
            report.error = Some(format!(
                "{}; native child reap deadline exceeded; effects remain unknown",
                report.error.as_deref().unwrap_or("media process failed")
            ));
        }
    }
}

/// Run one explicit, owner-authorized media child. Never restarts or retries an
/// uncertain effect. The caller supplies the same existing connection ID used by
/// the child's scope gate. Ordinary cancellation/drop requests kill-on-drop;
/// abrupt supervisor death still requires the deployment's process supervisor.
pub async fn supervise_media_process<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    command: &mut Command,
    connection_id: u64,
    input: R,
    output: W,
    limits: MediaProcessLimits,
) -> io::Result<MediaProcessReport> {
    let limits = limits.validate()?;
    if connection_id == 0 {
        return Err(io::Error::other("zero owner connection"));
    }
    let mut child = command
        .kill_on_drop(true)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut report = MediaProcessReport {
        pid: child
            .id()
            .ok_or_else(|| io::Error::other("missing child pid"))?,
        ready: false,
        terminal_forwarded: false,
        stop_requested: false,
        kill_requested: false,
        status: None,
        error: None,
    };
    let (stop_tx, mut stop_rx) = oneshot::channel();
    let (ready_tx, mut ready_rx) = oneshot::channel();
    let controls = Arc::new(Controls::default());
    let terminal_forwarded = Arc::new(AtomicBool::new(false));
    let input = commands(
        input,
        child.stdin.take().expect("piped stdin"),
        stop_tx,
        limits.pipe_write,
        controls.clone(),
        limits.control,
    );
    let output = events(
        child.stdout.take().expect("piped stdout"),
        output,
        connection_id,
        ready_tx,
        limits.pipe_write,
        controls.clone(),
        terminal_forwarded.clone(),
    );
    tokio::pin!(input, output);
    let mut input_done = false;
    let mut output_done = false;
    let mut stop_seen = false;
    let mut ready_seen = false;
    let startup_deadline = Instant::now() + limits.startup;
    let mut stop_deadline = None;
    loop {
        let control_deadline = match controls.deadline() {
            Ok(deadline) => deadline,
            Err(error) => {
                report.error.get_or_insert(error);
                break;
            }
        };
        tokio::select! {
            biased;
            stop = &mut stop_rx, if !stop_seen => {
                stop_seen = true;
                if let Ok(observed) = stop {
                    report.stop_requested = true;
                    let deadline = observed + limits.shutdown;
                    stop_deadline = Some(stop_deadline.map_or(deadline, |prior: Instant| prior.min(deadline)));
                }
            }
            ready = &mut ready_rx, if !ready_seen => {
                ready_seen = true;
                match ready {
                    Ok(observed) if observed <= startup_deadline => report.ready = true,
                    Ok(_) => {
                        report.error.get_or_insert_with(|| "native media readiness arrived after startup deadline".into());
                        break;
                    }
                    Err(_) => {}
                }
            }
            _ = async {
                match control_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                // A timely ACK can remove the old deadline while events stays
                // Pending. Recheck at the decision boundary instead of killing
                // a child because this select retained an already-retired timer.
                match controls.deadline() {
                    Ok(Some(deadline)) if deadline <= Instant::now() => {
                        report.error.get_or_insert_with(|| "native media control deadline exceeded; effects remain unknown".into());
                        break;
                    }
                    Ok(_) => continue,
                    Err(error) => { report.error.get_or_insert(error); break; }
                }
            }
            _ = controls.changed.notified() => {}
            _ = async {
                match stop_deadline {
                    Some(deadline) => tokio::time::sleep_until(deadline).await,
                    None => std::future::pending().await,
                }
            } => {
                report.error.get_or_insert_with(|| "native media shutdown deadline exceeded; effects remain unknown".into());
                break;
            }
            _ = tokio::time::sleep_until(startup_deadline), if !report.ready => {
                report.error.get_or_insert_with(|| "native media startup deadline exceeded".into());
                break;
            }
            result = &mut input, if !input_done => {
                input_done = true;
                if let Err(error) = result {
                    match controls.input_error.lock() {
                        Ok(mut cause) => *cause = Some(error.clone()),
                        Err(_) => { report.error.get_or_insert_with(|| "input monitor poisoned".into()); }
                    }
                    report.error.get_or_insert(error);
                }
                stop_deadline.get_or_insert_with(|| Instant::now() + limits.shutdown);
            }
            result = &mut output, if !output_done => {
                output_done = true;
                if let Err(error) = result {
                    report.error.get_or_insert(error);
                    break;
                }
                // Closing stdout is not proof that native threads have exited.
                stop_deadline.get_or_insert_with(|| Instant::now() + limits.shutdown);
            }
            status = child.wait() => {
                match status {
                    Ok(status) => report.status = Some(status),
                    Err(error) => { report.error.get_or_insert_with(|| error.to_string()); }
                }
                // Read already-written terminal bytes before returning. This is
                // separately bounded if the owner's receiver stopped draining.
                if !output_done {
                    match timeout(limits.pipe_write, &mut output).await {
                        Ok(Ok(())) => {}
                        Ok(Err(error)) => { report.error.get_or_insert(error); }
                        Err(_) => { report.error.get_or_insert_with(|| "media event drain deadline exceeded".into()); }
                    }
                }
                if !ready_seen {
                    report.ready = ready_rx.try_recv().is_ok_and(|observed| observed <= startup_deadline);
                }
                if !stop_seen && stop_rx.try_recv().is_ok() {
                    report.stop_requested = true;
                }
                if !report.ready {
                    report.error.get_or_insert_with(|| "native media exited before readiness".into());
                }
                if !controls.deadline().is_ok_and(|deadline| deadline.is_none()) {
                    report.error.get_or_insert_with(|| "native media exited without acknowledging control; effects remain unknown".into());
                }
                report.terminal_forwarded = terminal_forwarded.load(Ordering::SeqCst);
                if !report.terminal_forwarded {
                    report.error.get_or_insert_with(|| "native media exited without forwarding a terminal".into());
                }
                return Ok(report);
            }
        }
    }
    terminate(&mut child, &mut report, limits.reap).await;
    if !ready_seen {
        report.ready = ready_rx
            .try_recv()
            .is_ok_and(|observed| observed <= startup_deadline);
    }
    if !stop_seen && stop_rx.try_recv().is_ok() {
        report.stop_requested = true;
    }
    report.terminal_forwarded = terminal_forwarded.load(Ordering::SeqCst);
    Ok(report)
}
