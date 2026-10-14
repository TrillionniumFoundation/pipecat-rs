//! Single-use, literal-IPv4 RTVI text/LLM client for a native pipeline.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use crate::{Packet, SessionConfig, WebSocketSession};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, InvocationFence, ProcessorContext,
    ProcessorError, StartFrame,
};
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer},
    rtvi::{Envelope, MESSAGE_LABEL, PROTOCOL_VERSION},
    WireMessage,
};
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex, MutexGuard},
    time::Duration,
};
use tokio::sync::{watch, Mutex as AsyncMutex, Notify};

/// This deliberately restricted profile has no DNS, proxy, credentials, audio,
/// tool callbacks, automatic reconnect, or reuse after setup. Limits are hard
/// ceilings as well as configuration validation, including nested JSON values.
#[derive(Clone)]
pub struct RtviClientConfig {
    pub endpoint: String,
    pub session: SessionConfig,
    pub handshake_timeout: Duration,
    pub outbox_capacity: usize,
    pub outbox_max_bytes: usize,
}
impl RtviClientConfig {
    pub fn new(endpoint: impl Into<String>) -> Self {
        Self {
            endpoint: endpoint.into(),
            session: SessionConfig {
                queue_capacity: 32,
                max_message_bytes: 64 * 1024,
                ..SessionConfig::default()
            },
            handshake_timeout: Duration::from_secs(10),
            outbox_capacity: 32,
            outbox_max_bytes: 1024 * 1024,
        }
    }
    fn validate(&self) -> Result<(), ProcessorError> {
        let invalid = || error("invalid RTVI client configuration");
        if self.endpoint.len() > 2048 {
            return Err(invalid());
        }
        let url = url::Url::parse(&self.endpoint).map_err(|_| invalid())?;
        if !matches!(url.scheme(), "ws" | "wss")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
            || !matches!(url.host(), Some(url::Host::Ipv4(_)))
            || self.outbox_capacity == 0
            || self.outbox_capacity > 1024
            || self.outbox_max_bytes == 0
            || self.outbox_max_bytes > 16 * 1024 * 1024
            || self.session.queue_capacity == 0
            || self.session.queue_capacity > 1024
            || self.session.max_message_bytes == 0
            || self.session.max_message_bytes > 1024 * 1024
            || self
                .session
                .queue_capacity
                .saturating_mul(self.session.max_message_bytes)
                > 16 * 1024 * 1024
            || [
                self.handshake_timeout,
                self.session.connect_timeout,
                self.session.write_timeout,
                self.session.close_timeout,
            ]
            .iter()
            .any(|d| d.is_zero() || *d > Duration::from_secs(60))
        {
            return Err(invalid());
        }
        Ok(())
    }
}
/// Reasons never contain endpoint, payload, or underlying transport diagnostics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtviClientFailure {
    Connect,
    HandshakeTimeout,
    Write,
    Backpressure,
    Protocol,
    Pipeline,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RtviClientStatus {
    Idle,
    Connecting,
    WaitingForBot,
    Ready,
    Closed,
    Failed(RtviClientFailure),
}
struct State {
    setup: bool,
    started: bool,
    owned: bool,
    status: RtviClientStatus,
    fence: Option<InvocationFence>,
    base: Option<ProcessorContext>,
    outbox: VecDeque<String>,
    bytes: usize,
    discarded_outbox_commands: usize,
}
struct Run {
    state: Mutex<State>,
    session: AsyncMutex<Option<WebSocketSession>>,
    cancel: watch::Sender<bool>,
    commands: Notify,
    drained: Notify,
}
impl Run {
    fn new() -> Self {
        Self {
            state: Mutex::new(State {
                setup: false,
                started: false,
                owned: false,
                status: RtviClientStatus::Idle,
                fence: None,
                base: None,
                outbox: VecDeque::new(),
                bytes: 0,
                discarded_outbox_commands: 0,
            }),
            session: AsyncMutex::new(None),
            cancel: watch::channel(false).0,
            commands: Notify::new(),
            drained: Notify::new(),
        }
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn close(&self, failure: Option<RtviClientFailure>) {
        {
            let mut s = self.lock();
            if !matches!(s.status, RtviClientStatus::Failed(_)) {
                s.status = failure
                    .map(RtviClientStatus::Failed)
                    .unwrap_or(RtviClientStatus::Closed);
            }
            s.discarded_outbox_commands =
                s.discarded_outbox_commands.saturating_add(s.outbox.len());
            s.outbox.clear();
            s.bytes = 0;
        }
        self.cancel.send_replace(true);
    }
    async fn drain(&self) {
        loop {
            let notified = self.drained.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !self.lock().owned {
                break;
            }
            notified.await;
        }
    }
    async fn shutdown(&self) -> Result<(), ProcessorError> {
        self.close(None);
        let base = self.lock().base.take();
        drop(base);
        self.drain().await;
        // The outer worker never consumes the session. Even an aborted/unpolled
        // tracked task leaves this owned resource available to core cleanup.
        let mut slot = self.session.lock().await;
        if let Some(session) = slot.as_mut() {
            let result = session.shutdown_owned(false).await;
            slot.take();
            result.map_err(|_| error("RTVI session shutdown failed"))?;
        }
        Ok(())
    }
}
struct Owned(Arc<Run>);
impl Drop for Owned {
    fn drop(&mut self) {
        self.0.close(None);
        self.0.lock().owned = false;
        self.0.drained.notify_waiters();
    }
}
/// A handle for this one run only. `close` requests cancellation; processor
/// prepare_end/cleanup joins its owned session. It does not claim delivery.
#[derive(Clone)]
pub struct RtviClientControl(Arc<Run>);
impl RtviClientControl {
    pub fn status(&self) -> RtviClientStatus {
        let (status, fence) = {
            let s = self.0.lock();
            (s.status, s.fence.clone())
        };
        if matches!(
            status,
            RtviClientStatus::Ready
                | RtviClientStatus::WaitingForBot
                | RtviClientStatus::Connecting
        ) && fence.is_some_and(|f| !f.is_current())
        {
            RtviClientStatus::Closed
        } else {
            status
        }
    }
    pub fn is_ready(&self) -> bool {
        self.status() == RtviClientStatus::Ready
    }
    pub fn is_closed(&self) -> bool {
        matches!(
            self.status(),
            RtviClientStatus::Closed | RtviClientStatus::Failed(_)
        )
    }
    /// Number of commands removed from the local outbox on closure. Does not
    /// count bytes already handed to the transport or its in-flight send.
    pub fn discarded_outbox_commands(&self) -> usize {
        self.0.lock().discarded_outbox_commands
    }
    pub fn close(&self) {
        self.0.close(None);
    }
}
/// Start is forwarded before connecting. Before bot-ready, LLM events are
/// discarded and outgoing RTVI commands are rejected. Afterwards only LLM
/// start/text/stop enters the pipeline. Other original frames pass unchanged.
/// Successfully admitted RTVI commands are consumed, not forwarded; admission
/// means bounded local buffering, not delivery. Interruption closes this entire
/// run, without rebinding a new epoch. End/Stop/Cancel discard pending commands.
/// Never share this processor across pipeline invocations, even sequentially.
pub struct RtviClientProcessor {
    config: RtviClientConfig,
    run: Arc<Run>,
}
impl RtviClientProcessor {
    pub fn new(mut config: RtviClientConfig) -> Result<Self, ProcessorError> {
        config.validate()?;
        // Connect using the validated canonical IP, never a pre-normalization
        // numeric hostname that another parser might resolve through DNS.
        config.endpoint = url::Url::parse(&config.endpoint)
            .map_err(|_| error("invalid RTVI client configuration"))?
            .to_string();
        if config.endpoint.len() > 2048 {
            return Err(error("invalid RTVI client configuration"));
        }
        Ok(Self {
            config,
            run: Arc::new(Run::new()),
        })
    }
    pub fn control(&self) -> RtviClientControl {
        RtviClientControl(self.run.clone())
    }
    fn start(&self, ctx: &ProcessorContext, fence: InvocationFence) -> Result<(), ProcessorError> {
        let base = {
            let mut s = self.run.lock();
            if s.started || *self.run.cancel.borrow() {
                return Ok(());
            }
            let base = s
                .base
                .clone()
                .ok_or_else(|| error("RTVI client missing setup"))?;
            s.started = true;
            s.owned = true;
            s.status = RtviClientStatus::Connecting;
            s.fence = Some(fence.clone());
            base
        };
        let owned = Owned(self.run.clone());
        let config = self.config.clone();
        ctx.spawn("rtvi-client", false, async move {
            let _owned = owned;
            let result = receive_loop(&_owned.0, &config, &base, &fence).await;
            _owned.0.close(result.as_ref().err().copied());
            // A cancelled join future retains both session and JoinHandle in
            // the run. Cleanup can resume the same join after core aborts us.
            let mut slot = _owned.0.session.lock().await;
            let joined = if let Some(session) = slot.as_mut() {
                session.shutdown_owned(false).await
            } else {
                Ok(())
            };
            slot.take();
            if result.is_err() || joined.is_err() {
                return Err(error("RTVI client session failed"));
            }
            Ok(())
        })
        .map(|_| ())
    }
    fn admit(
        &self,
        frame: &Frame,
        ctx: &ProcessorContext,
        text: String,
    ) -> Result<(), ProcessorError> {
        let original = self
            .run
            .lock()
            .fence
            .clone()
            .ok_or_else(|| error("RTVI client is not ready"))?;
        let fence = original.intersection(&ctx.invocation_fence()?)?;
        let result = fence.with_current(|| {
            let mut s = self.run.lock();
            if s.status != RtviClientStatus::Ready {
                return Err(error("RTVI client is not ready"));
            }
            // `frame` authority was validated by the fixed codec before admission.
            let _ = frame;
            enqueue(
                &mut s,
                text,
                self.config.outbox_capacity,
                self.config.outbox_max_bytes,
            )
        });
        match result {
            Some(Ok(())) => {
                self.run.commands.notify_one();
                Ok(())
            }
            Some(Err(e)) => {
                self.run.close(Some(RtviClientFailure::Backpressure));
                Err(e)
            }
            None => Ok(()), // stale source cannot admit bytes
        }
    }
}
#[async_trait]
impl FrameProcessor for RtviClientProcessor {
    fn name(&self) -> &str {
        "RtviClientProcessor"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        let mut s = self.run.lock();
        if s.setup {
            return Err(error("RTVI client processor is single-use"));
        }
        s.setup = true;
        s.base = Some(ctx.clone());
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() {
            let fence = ctx.invocation_fence()?;
            ctx.push(frame, direction).await?;
            return self.start(ctx, fence);
        }
        if matches!(
            frame.data,
            FrameData::Interruption
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
                | FrameData::End(_)
                | FrameData::EndDetailed(_)
        ) {
            self.run.shutdown().await?;
            return ctx.push(frame, direction).await;
        }
        // Base output processes urgent messages before its direction gate.
        if direction == Direction::Upstream
            && matches!(frame.data, FrameData::OutputTransportMessage(_))
        {
            return ctx.push(frame, direction).await;
        }
        if let FrameData::OutputTransportMessage(value)
        | FrameData::OutputTransportMessageUrgent(value) = &frame.data
        {
            if value.get("label").and_then(Value::as_str) == Some(MESSAGE_LABEL) {
                if frame.transport_destination.is_some()
                    || frame.pts.is_some()
                    || frame.has_pipeline_pts()
                {
                    return Err(error(
                        "RTVI client does not support named destinations or PTS scheduling",
                    ));
                }
                // Only a candidate the fixed codec could serialize needs a wire
                // budget. Its None branch must remain a real no-wire passthrough.
                check_json(value, self.config.session.max_message_bytes)?;
            }
            let wire = RtviClientFrameSerializer::default()
                .serialize(&frame)
                .map_err(|_| error("invalid RTVI outgoing command"))?;
            if let Some(WireMessage::Text(text)) = wire {
                if text.len() > self.config.session.max_message_bytes {
                    return Err(error("RTVI outgoing command exceeds limit"));
                }
                return self.admit(&frame, ctx, text);
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.run.shutdown().await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.run.shutdown().await
    }
}
fn enqueue(
    state: &mut State,
    text: String,
    count: usize,
    bytes: usize,
) -> Result<(), ProcessorError> {
    if state.outbox.len() >= count || text.len() > bytes.saturating_sub(state.bytes) {
        return Err(error("RTVI client outbox is full"));
    }
    state.bytes += text.len();
    state.outbox.push_back(text);
    Ok(())
}
fn error(message: &'static str) -> ProcessorError {
    ProcessorError::new(message)
}
struct BudgetWriter {
    remaining: usize,
}
impl std::io::Write for BudgetWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        if bytes.len() > self.remaining {
            return Err(std::io::Error::other("RTVI JSON exceeds profile limits"));
        }
        self.remaining -= bytes.len();
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Conservative serialized-size upper bound; no allocation proportional to an
/// untrusted string or wide JSON container before these checks succeed.
fn check_json(value: &Value, max: usize) -> Result<(), ProcessorError> {
    fn visit(v: &Value, depth: usize, nodes: &mut usize, budget: &mut usize) -> bool {
        if depth > 32 || *nodes >= 4096 {
            return false;
        }
        *nodes += 1;
        let cost = match v {
            Value::String(s) => s.len().saturating_mul(6).saturating_add(2),
            Value::Number(number) => {
                // Number can hold an arbitrarily large lexical representation
                // when dependency feature unification enables arbitrary_precision.
                // Count its actual encoding without allocating a String.
                let mut writer = BudgetWriter { remaining: *budget };
                if serde_json::to_writer(&mut writer, number).is_err() {
                    return false;
                }
                *budget = writer.remaining;
                return true;
            }
            Value::Bool(_) | Value::Null => 5,
            _ => 2,
        };
        if cost > *budget {
            return false;
        }
        *budget -= cost;
        match v {
            Value::Array(a) => a.iter().all(|v| {
                if *budget == 0 {
                    return false;
                }
                *budget -= 1;
                visit(v, depth + 1, nodes, budget)
            }),
            Value::Object(m) => m.iter().all(|(k, v)| {
                let cost = k.len().saturating_mul(6).saturating_add(4);
                if cost > *budget {
                    return false;
                }
                *budget -= cost;
                visit(v, depth + 1, nodes, budget)
            }),
            _ => true,
        }
    }
    let mut nodes = 0;
    let mut budget = max;
    if visit(value, 0, &mut nodes, &mut budget) {
        Ok(())
    } else {
        Err(error("RTVI JSON exceeds profile limits"))
    }
}
async fn retired(fence: &InvocationFence) {
    while fence.is_current() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
async fn receive_loop(
    run: &Run,
    config: &RtviClientConfig,
    base: &ProcessorContext,
    fence: &InvocationFence,
) -> Result<(), RtviClientFailure> {
    let mut cancel = run.cancel.subscribe();
    let mut slot = run.session.lock().await;
    if *cancel.borrow() || !fence.is_current() {
        return Ok(());
    }
    let session = tokio::select! {
        biased;
        _ = cancel.changed() => return Ok(()),
        _ = retired(fence) => return Ok(()),
        result = WebSocketSession::connect(&config.endpoint, config.session.clone()) => result.map_err(|_| RtviClientFailure::Connect)?,
    };
    // No await after connect until the spawned transport has a durable owner.
    *slot = Some(session);
    let socket = slot.as_mut().expect("connected session stored");
    let sender = socket.sender();
    let ready = Envelope {
        label: MESSAGE_LABEL.into(),
        message_type: "client-ready".into(),
        id: Some("client-ready".into()),
        data: Some(json!({"version": PROTOCOL_VERSION, "about": {"library":"pipecat"}})),
    }
    .encode()
    .map_err(|_| RtviClientFailure::Protocol)?;
    tokio::select! {
        biased;
        _ = cancel.changed() => return Ok(()),
        _ = retired(fence) => return Ok(()),
        result = tokio::time::timeout(config.session.write_timeout, sender.send(Packet::Text(ready))) => {
            result.map_err(|_| RtviClientFailure::Write)?.map_err(|_| RtviClientFailure::Write)?;
        }
    }
    {
        let mut s = run.lock();
        if !*cancel.borrow() {
            s.status = RtviClientStatus::WaitingForBot;
        }
    }
    let deadline = tokio::time::Instant::now() + config.handshake_timeout;
    let mut ready = false;
    let mut codec = RtviClientFrameSerializer::default();
    loop {
        if *cancel.borrow() || !fence.is_current() {
            return Ok(());
        }
        let notified = run.commands.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        let command = {
            let mut s = run.lock();
            let command = s.outbox.pop_front();
            if let Some(text) = &command {
                s.bytes -= text.len();
            }
            command
        };
        if let Some(text) = command {
            tokio::select! {
            biased;
            _ = cancel.changed() => return Ok(()),
            _ = retired(fence) => return Ok(()),
            result = tokio::time::timeout(config.session.write_timeout, sender.send(Packet::Text(text))) => {
                    result.map_err(|_| RtviClientFailure::Write)?.map_err(|_| RtviClientFailure::Write)?;
                }
            }
        }
        let pending_commands = !run.lock().outbox.is_empty();
        tokio::select! {
            biased;
            _ = cancel.changed() => return Ok(()),
            _ = retired(fence) => return Ok(()),
            _ = tokio::time::sleep_until(deadline), if !ready => return Err(RtviClientFailure::HandshakeTimeout),
            packet = socket.recv() => {
                let Some(packet) = packet else { return Ok(()); };
                let bytes: &[u8] = match &packet { Packet::Text(s) => s.as_bytes(), Packet::Binary(b) => b };
                // The Python ready predicate deliberately checks only label/type,
                // not the server-event payload, version, or correlation id.
                let Ok(value) = serde_json::from_slice::<Value>(bytes) else { continue; };
                if value.get("label").and_then(Value::as_str) == Some(MESSAGE_LABEL)
                    && value.get("type").and_then(Value::as_str) == Some("bot-ready") {
                    if !ready { ready = true; let mut s = run.lock(); if !*cancel.borrow() { s.status = RtviClientStatus::Ready; } }
                    continue;
                }
                let Ok(envelope) = Envelope::parse_with_limit(bytes, config.session.max_message_bytes) else { continue; };
                if !ready || !matches!(envelope.message_type.as_str(), "bot-llm-started" | "bot-llm-text" | "bot-llm-stopped") { continue; }
                let wire = match packet { Packet::Text(s) => WireMessage::Text(s), Packet::Binary(b) => WireMessage::Binary(b.to_vec()) };
                let Some(frame) = codec.deserialize(&wire).map_err(|_| RtviClientFailure::Protocol)? else { continue; };
                if !matches!(frame.data, FrameData::LlmResponseStart | FrameData::LlmText(_) | FrameData::LlmResponseEnd) { continue; }
                base.try_push_with_invocation_fence(frame, Direction::Downstream, fence).map_err(|_| RtviClientFailure::Pipeline)?;
            }
            // Receive already-available packets first, but do not impose a
            // polling delay on a buffered burst of outgoing commands.
            _ = async {}, if pending_commands => {},
            _ = &mut notified => {},
            _ = tokio::time::sleep(Duration::from_millis(20)) => {},
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn endpoint_and_resource_bounds() {
        for endpoint in [
            "ws://localhost/",
            "ws://user@127.0.0.1/",
            "ws://127.0.0.1/#secret",
            "https://127.0.0.1/",
        ] {
            assert!(RtviClientProcessor::new(RtviClientConfig::new(endpoint)).is_err());
        }
        assert!(RtviClientProcessor::new(RtviClientConfig::new("ws://[::1]/")).is_err());
        assert!(RtviClientProcessor::new(RtviClientConfig::new("wss://[::1]/")).is_err());
        for endpoint in ["ws://127.0.0.1:9000/", "wss://127.0.0.1/"] {
            assert!(RtviClientProcessor::new(RtviClientConfig::new(endpoint)).is_ok());
        }
        let mut config = RtviClientConfig::new("ws://127.0.0.1/");
        config.outbox_capacity = 1025;
        assert!(RtviClientProcessor::new(config).is_err());
    }
    #[test]
    fn normalized_numeric_hosts_never_reach_connect_as_hostnames() {
        for endpoint in ["ws://2130706433/", "ws://0x7f000001/"] {
            let processor = RtviClientProcessor::new(RtviClientConfig::new(endpoint)).unwrap();
            assert_eq!(processor.config.endpoint, "ws://127.0.0.1/");
        }
    }
    #[test]
    fn close_reports_only_discarded_outbox_commands() {
        let run = Arc::new(Run::new());
        enqueue(&mut run.lock(), "abc".into(), 2, 4).unwrap();
        let control = RtviClientControl(run);
        control.close();
        control.close();
        assert_eq!(control.discarded_outbox_commands(), 1);
    }
    #[test]
    fn number_writer_enforces_exact_serialized_budget() {
        use std::io::Write;
        let mut writer = BudgetWriter { remaining: 3 };
        writer.write_all(b"123").unwrap();
        assert_eq!(writer.remaining, 0);
        assert!(writer.write_all(b"4").is_err());
        assert_eq!(writer.remaining, 0);
        let mut writer = BudgetWriter { remaining: 2 };
        assert!(writer.write_all(b"123").is_err());
        assert_eq!(writer.remaining, 2);
        assert!(check_json(&json!(123), 3).is_ok());
        assert!(check_json(&json!(123), 2).is_err());
        // This is accepted only when serde_json's arbitrary_precision feature
        // is enabled by the build. The dedicated feature replay requires this
        // branch explicitly rather than treating a default-build skip as proof.
        let lexical = "1".repeat(1024);
        let parsed = serde_json::from_str::<serde_json::Number>(&lexical);
        if std::env::var_os("PIPECAT_TEST_ARBITRARY_PRECISION").is_some() {
            assert!(
                parsed.is_ok(),
                "feature witness requires arbitrary_precision"
            );
        }
        if let Ok(number) = parsed {
            assert!(check_json(&Value::Number(number), 64).is_err());
        }
    }
    #[test]
    fn json_depth_width_and_strings_are_bounded() {
        assert!(check_json(
            &json!({"label": MESSAGE_LABEL, "type":"send-text", "data":{"text":"hello"}}),
            1024
        )
        .is_ok());
        assert!(check_json(&json!("a".repeat(1000)), 1024).is_err());
        assert!(check_json(&json!(vec![0; 4097]), 1024 * 1024).is_err());
        let mut value = Value::Null;
        for _ in 0..34 {
            value = Value::Array(vec![value]);
        }
        assert!(check_json(&value, 1024 * 1024).is_err());
    }
    #[test]
    fn deterministic_count_and_byte_admission_bounds() {
        let run = Run::new();
        let mut state = run.lock();
        enqueue(&mut state, "12".into(), 2, 4).unwrap();
        enqueue(&mut state, "34".into(), 2, 4).unwrap();
        assert!(enqueue(&mut state, "".into(), 2, 4).is_err());
        assert_eq!(state.bytes, 4);
        assert_eq!(state.outbox.len(), 2);
        state.outbox.clear();
        state.bytes = 0;
        assert!(enqueue(&mut state, "12345".into(), 2, 4).is_err());
        assert_eq!(state.bytes, 0);
        assert!(state.outbox.is_empty());
    }
    #[tokio::test]
    async fn unpolled_owner_and_control_close_are_terminal() {
        let run = Arc::new(Run::new());
        run.lock().owned = true;
        let owned = Owned(run.clone());
        let future = async move {
            let _owned = owned;
            std::future::pending::<()>().await;
        };
        drop(future);
        run.shutdown().await.unwrap();
        assert!(!run.lock().owned);
        assert!(RtviClientControl(run).is_closed());
    }
}
