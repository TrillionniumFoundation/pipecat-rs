use super::{
    state::{Command, Outbound, OutboundId, Shared},
    ReceiptBackend, ReceiptConfig, GATE_TOKEN, INPUT_BYTE_CAPACITY, INPUT_CAPACITY,
    MAX_PACKET_BYTES, MAX_PCM_BYTES,
};
use crate::{
    wire::{self, ClientInput, V2Control},
    AgentError,
};
use pipecat_core::{
    Direction, Frame, FrameData, Pipeline, PipelineConfig, PipelineSender, WorkerReport,
};
use pipecat_media_transport::SpokenHistory;
use pipecat_transports::{Packet, SessionSender, WebSocketSession};
use serde_json::json;
use std::{
    collections::VecDeque,
    future::{pending, Future},
    pin::Pin,
    sync::Arc,
    time::Duration,
};
use tokio::{sync::mpsc, time::Instant};

type InputFuture = Pin<Box<dyn Future<Output = Result<(), AgentError>> + Send>>;
type WriteFuture = Pin<Box<dyn Future<Output = Result<OutboundId, AgentError>> + Send>>;
type CloseFuture =
    Pin<Box<dyn Future<Output = Result<(), pipecat_transports::TransportError>> + Send>>;

pub struct ReceiptRunConfig {
    pub input_rate: u32,
    pub idle_timeout: Duration,
    pub history: Option<SpokenHistory>,
    /// Trusted caller-owned native startup frames, not an untrusted ingress.
    /// Count admission is bounded; arbitrary native metadata/continuations and
    /// non-PCM startup payload allocation are not covered by the network-byte
    /// budget. The CLI supplies only a payload-free LlmRun here.
    pub initial_frames: Vec<Frame>,
}
pub struct ReceiptRunReport {
    pub pipeline: WorkerReport,
    pub shutdown_requested: bool,
}
/// Owns the negotiated WebSocket and all receipt state. Use run to obtain joined
/// completion. Dropping this owner fails closed, but cannot prove async flush or
/// task joins; callers must not describe fallback Drop as successful shutdown.
pub struct ReceiptSession {
    session: Option<WebSocketSession>,
    backend: Arc<ReceiptBackend>,
    commands: Option<mpsc::Receiver<Command>>,
}
impl Drop for ReceiptSession {
    fn drop(&mut self) {
        self.backend.shared.close();
    }
}
impl ReceiptSession {
    /// Application handshake occurs before any provider, VAD or pipeline startup.
    /// SessionSender admission is not a socket-write acknowledgement.
    pub async fn negotiate(
        mut session: WebSocketSession,
        config: ReceiptConfig,
        output_rate: u32,
    ) -> Result<Self, AgentError> {
        if let Err(error) = config.validate() {
            let _ = session.cancel().await;
            return Err(error);
        }
        if !(8000..=48000).contains(&output_rate) {
            let _ = session.cancel().await;
            return Err(AgentError::Config("invalid v2 output sample rate"));
        }
        let session_id = match super::nonce() {
            Ok(id) => id,
            Err(error) => {
                let _ = session.cancel().await;
                return Err(error);
            }
        };
        let deadline = Instant::now() + config.handshake_timeout;
        let result = async {
            session
                .sender()
                .send(wire::message(json!({
                    "type":"playback-v2-offer", "version":2, "session_id":session_id,
                    "output_rate":output_rate, "channels":1, "encoding":"pcm_s16le",
                    "max_pcm_bytes":MAX_PCM_BYTES,
                }))?)
                .await?;
            let packet = session.recv().await.ok_or(AgentError::Protocol(
                "client disconnected during v2 negotiation",
            ))?;
            check_packet(&packet)?;
            match wire::decode(packet, 16000)? {
                ClientInput::V2(V2Control::Accept {
                    version: 2,
                    session_id: accepted,
                    chunk_receipts: true,
                    cutoff_receipts: true,
                    flush_on_disconnect: true,
                }) if accepted == session_id => Ok(()),
                _ => Err(AgentError::Protocol(
                    "chunk-v2 requires exact playback and cutoff negotiation",
                )),
            }
        };
        match tokio::time::timeout_at(deadline, result).await {
            Ok(Ok(())) => {}
            result => {
                let _ = session.cancel().await;
                return match result {
                    Ok(Err(error)) => Err(error),
                    _ => Err(AgentError::Protocol("v2 negotiation timed out")),
                };
            }
        }
        let (shared, commands) = Shared::new(session_id, output_rate, config);
        Ok(Self {
            session: Some(session),
            backend: Arc::new(ReceiptBackend { shared }),
            commands: Some(commands),
        })
    }
    pub fn backend(&self) -> Arc<ReceiptBackend> {
        self.backend.clone()
    }
    pub async fn close(mut self) -> Result<(), AgentError> {
        self.backend.shared.close();
        if let Some(session) = self.session.take() {
            session.cancel().await?;
        }
        Ok(())
    }
    pub async fn run(
        mut self,
        pipeline: Pipeline,
        config: PipelineConfig,
        run: ReceiptRunConfig,
    ) -> Result<ReceiptRunReport, AgentError> {
        if run.idle_timeout.is_zero() || !(8000..=48000).contains(&run.input_rate) {
            self.backend.shared.close();
            if let Some(session) = self.session.take() {
                let _ = session.cancel().await;
            }
            return Err(AgentError::Config("invalid v2 session input configuration"));
        }
        let shared = self.backend.shared.clone();
        let mut session = self.session.take();
        let mut commands = self.commands.take().expect("receipt owner runs once");
        // start() does only local format validation; before any PCM is published,
        // partial-start cleanup is also local and does not await this driver.
        let mut worker = match pipeline.start(config).await {
            Ok(worker) => worker,
            Err(error) => {
                shared.close();
                if let Some(session) = session.take() {
                    let _ = session.cancel().await;
                }
                return Err(error.into());
            }
        };
        let sender = worker.sender();
        let transport_sender = session.as_ref().expect("session present").sender();
        let mut pump = InputPump::new(sender.clone(), shared.config.drain_timeout);
        let mut outcome: Result<(), AgentError> = Ok(());
        for frame in run.initial_frames {
            if let Err(error) = pump.admit(frame, false) {
                outcome = Err(error);
                break;
            }
        }
        let mut write: Option<WriteFuture> = None;
        let mut closing: Option<CloseFuture> = None;
        let mut close_failure: Option<AgentError> = None;
        let mut notices = VecDeque::new();
        let mut last_activity = Instant::now();
        let mut drain = None;
        let mut end_requested = false;
        let mut shutdown_requested = false;
        let mut failed = false;
        let mut failure_deadline = None;
        let mut cleanup_unverified = false;
        let mut committed = 0;
        loop {
            let changed = shared.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if failed && failure_deadline.is_some_and(|deadline| Instant::now() >= deadline) {
                cleanup_unverified = true;
                break;
            }
            if !failed && outcome.is_ok() {
                if shared.state.lock().unwrap().closed {
                    outcome = Err(AgentError::Protocol("v2 session owner failed closed"));
                } else if drain.is_some_and(|d| Instant::now() >= d) {
                    outcome = Err(AgentError::Protocol("v2 End drain timed out"));
                } else if shared.timed_out() {
                    outcome = Err(AgentError::Protocol("v2 client receipt timed out"));
                } else if Instant::now() >= last_activity + run.idle_timeout {
                    outcome = Err(AgentError::Protocol("v2 client idle timeout"));
                }
            }
            if outcome.is_err() && !failed {
                failed = true;
                failure_deadline = Some(Instant::now() + shared.config.drain_timeout);
                end_requested = true;
                shared.close();
                write = None;
                notices.clear();
                pump.cancel();
                if let Some(socket) = session.take() {
                    closing = Some(Box::pin(socket.cancel()));
                }
            }
            if let Some(history) = &run.history {
                let count = history.status().committed_contexts;
                if count > committed && !failed {
                    committed = count;
                    if notices.len() >= INPUT_CAPACITY {
                        outcome = Err(AgentError::Protocol("v2 control queue is full"));
                        continue;
                    }
                    match wire::message(json!({"type":"playback-v2-context-committed", "version":2,
                        "session_id":shared.session_id, "committed_contexts":count}))
                    {
                        Ok(packet) => notices.push_back(packet),
                        Err(error) => {
                            outcome = Err(error);
                            continue;
                        }
                    }
                }
            }
            if write.is_none() && !failed {
                let outbound = match shared.next_cutoff() {
                    Ok(Some(outbound)) => Some(outbound),
                    Ok(None) => {
                        if let Some(packet) = notices.pop_front() {
                            Some(Outbound {
                                id: OutboundId::Notice,
                                packet,
                                deadline: Instant::now() + shared.config.enqueue_timeout,
                            })
                        } else {
                            match commands.try_recv() {
                                Ok(command) => shared.begin_chunk(command),
                                Err(_) => None,
                            }
                        }
                    }
                    Err(error) => {
                        outcome = Err(error);
                        continue;
                    }
                };
                if let Some(outbound) = outbound {
                    write = Some(enqueue(transport_sender.clone(), outbound));
                }
            }
            pump.advance(failed || !shared.state.lock().unwrap().gate_closed);
            let deadline = if failed {
                failure_deadline.expect("failed cleanup has one absolute deadline")
            } else {
                shared
                    .deadline()
                    .into_iter()
                    .chain(drain)
                    .chain(Some(last_activity + run.idle_timeout))
                    .min()
                    .unwrap()
            };
            tokio::select! {
                result = async { match write.as_mut() { Some(write) => write.await, None => pending().await } }, if write.is_some() => {
                    write = None;
                    match result { Ok(id) => shared.enqueued(id), Err(error) => outcome = Err(error) }
                }
                result = async { match closing.as_mut() { Some(close) => close.await, None => pending().await } }, if closing.is_some() => {
                    closing = None;
                    if let Err(error) = result { close_failure = Some(error.into()); }
                }
                packet = async { match session.as_mut() { Some(socket) => socket.recv().await, None => pending().await } }, if session.is_some() => {
                    let Some(packet) = packet else {
                        outcome = Err(AgentError::Protocol("v2 client disconnected"));
                        continue;
                    };
                    if let Err(error) = check_packet(&packet) { outcome = Err(error); continue; }
                    match wire::decode(packet, run.input_rate) {
                        Ok(ClientInput::V2(control)) => { if let Err(error) = shared.receipt(control) { outcome = Err(error); } }
                        Ok(ClientInput::PlaybackComplete(_)) => outcome = Err(AgentError::Protocol("context playback ACK is not a chunk-v2 receipt")),
                        Ok(ClientInput::Cancel) => {
                            if let Some(history) = &run.history { history.invalidate(); }
                            outcome = Err(AgentError::Protocol("v2 client cancelled"));
                        }
                        Ok(ClientInput::End) if !end_requested => {
                            end_requested = true;
                            drain = Some(Instant::now() + shared.config.drain_timeout);
                            pump.pending_end = true;
                        }
                        Ok(ClientInput::End) => {},
                        Ok(ClientInput::Frame(mut frame)) if !end_requested => {
                            last_activity = Instant::now();
                            let cut = matches!(frame.data, FrameData::Interruption | FrameData::VadUserStartedSpeaking(_)) ||
                                matches!(&frame.data, FrameData::InputText(text) if !text.text.trim().is_empty());
                            if cut {
                                pump.discard_interruptible();
                                if let Some(history) = &run.history { history.invalidate(); }
                                match self.backend.input_cutoff() {
                                    Ok(token) => {
                                        if matches!(frame.data, FrameData::Interruption) {
                                            frame.metadata.insert(GATE_TOKEN.into(), json!(token));
                                        } else {
                                            let mut interruption = Frame::interruption();
                                            interruption.metadata.insert(GATE_TOKEN.into(), json!(token));
                                            if let Err(error) = pump.admit(interruption, true) { outcome = Err(error); continue; }
                                        }
                                    }
                                    Err(error) => { outcome = Err(error.into()); continue; }
                                }
                            }
                            let urgent = matches!(frame.data, FrameData::Interruption);
                            if let Err(error) = pump.admit(*frame, urgent) { outcome = Err(error); }
                        }
                        Ok(ClientInput::Frame(_)) => outcome = Err(AgentError::Protocol("ordinary input is closed after End")),
                        Err(error) => outcome = Err(error),
                    }
                }
                result = async { match pump.urgent_active.as_mut() { Some((_, work)) => work.await, None => pending().await } }, if pump.urgent_active.is_some() => {
                    pump.finish_urgent();
                    if let Err(error) = result { if !failed { outcome = Err(error); } }
                }
                result = async { match pump.normal_active.as_mut() { Some((_, _, work)) => work.await, None => pending().await } }, if pump.normal_active.is_some() => {
                    pump.finish_normal();
                    if let Err(error) = result { if !failed { outcome = Err(error); } }
                }
                frame = worker.recv() => {
                    match frame {
                        None => break,
                        Some(frame) if matches!(frame.frame.data, FrameData::Error(_)) && !failed => outcome = Err(AgentError::ProviderRequest),
                        Some(_) => {},
                    }
                }
                command = commands.recv(), if write.is_none() && !failed => {
                    if let Some(command) = command {
                        if let Some(outbound) = shared.begin_chunk(command) { write = Some(enqueue(transport_sender.clone(), outbound)); }
                    } else { outcome = Err(AgentError::Protocol("v2 command owner stopped")); }
                }
                _ = &mut changed => {},
                _ = tokio::time::sleep_until(deadline) => {
                    if failed { cleanup_unverified = true; break; }
                    outcome = Err(AgentError::Protocol(if drain.is_some_and(|d| Instant::now() >= d) {
                        "v2 End drain timed out"
                    } else if shared.timed_out() { "v2 client receipt timed out" } else { "v2 client idle timeout" }));
                }
                result = tokio::signal::ctrl_c(), if !failed => {
                    outcome = Err(match result {
                        Ok(()) => { shutdown_requested = true; AgentError::Protocol("v2 shutdown requested") },
                        Err(error) => error.into(),
                    });
                }
            }
        }
        // recv closes only after the core supervisor has completed owned task
        // drain and processor cleanup. Keep the receipt pump alive until then.
        if cleanup_unverified {
            // PipelineWorker has no force-cancel-and-retain-join API. Its Drop
            // requests runtime cancellation, but cannot certify supervisor
            // completion. Report the exact boundary instead of hanging or
            // returning a successful joined report.
            drop(worker);
            pump.clear();
            shared.close();
            if let Some(close) = closing {
                let _ = close.await;
            }
            if let Some(socket) = session.take() {
                let _ = socket.cancel().await;
            }
            let error =
                AgentError::Protocol("v2 pipeline cleanup deadline exceeded; join is unverified");
            return Err(if shutdown_requested {
                AgentError::ShutdownFailed(Box::new(error))
            } else {
                error
            });
        }
        let report = worker.join().await;
        pump.clear();
        if outcome.is_ok() {
            if let Some(history) = &run.history {
                let count = history.status().committed_contexts;
                if count > committed {
                    match wire::message(json!({"type":"playback-v2-context-committed", "version":2,
                        "session_id":shared.session_id, "committed_contexts":count}))
                    {
                        Ok(packet) if notices.len() < INPUT_CAPACITY => notices.push_back(packet),
                        Ok(_) => {
                            outcome = Err(AgentError::Protocol("v2 final control queue is full"))
                        }
                        Err(error) => outcome = Err(error),
                    }
                }
            }
        }
        // Flush already generated user-visible notices under one remaining
        // drain budget. There are no receipt-dependent callbacks after join.
        if outcome.is_ok() {
            let deadline = drain.unwrap_or_else(|| Instant::now() + shared.config.drain_timeout);
            let controls = async {
                if let Some(pending_write) = write.take() {
                    let id = pending_write.await?;
                    shared.enqueued(id);
                }
                while let Some(packet) = notices.pop_front() {
                    tokio::time::timeout_at(deadline, transport_sender.send(packet))
                        .await
                        .map_err(|_| AgentError::Protocol("v2 final control drain timed out"))??;
                }
                Ok::<(), AgentError>(())
            };
            if let Err(error) = match tokio::time::timeout_at(deadline, controls).await {
                Ok(result) => result,
                Err(_) => Err(AgentError::Protocol("v2 final control drain timed out")),
            } {
                outcome = Err(error);
            }
        }
        shared.close();
        if let Some(close) = closing {
            if let Err(error) = close.await {
                close_failure = Some(error.into());
            }
        }
        if let Some(socket) = session.take() {
            // Successful completion has no unresolved PCM. Finishing here is
            // ordinary close, never used as a cancellation/revocation claim.
            if outcome.is_ok() {
                socket.finish().await?;
            } else {
                if let Err(error) = socket.cancel().await {
                    close_failure = Some(error.into());
                }
            }
        }
        if shutdown_requested {
            // Ctrl+C is a requested cancellation, not graceful media success.
            // Preserve the joined core report so serve exits like legacy-v1.
            if let Some(error) = close_failure {
                return Err(AgentError::ShutdownFailed(Box::new(error)));
            }
            return Ok(ReceiptRunReport {
                pipeline: report
                    .map_err(|error| AgentError::ShutdownFailed(Box::new(error.into())))?,
                shutdown_requested: true,
            });
        }
        outcome?;
        if let Some(error) = close_failure {
            return Err(error);
        }
        let report = report?;
        if report.total_errors > 0 {
            return Err(AgentError::ProviderRequest);
        }
        Ok(ReceiptRunReport {
            pipeline: report,
            shutdown_requested,
        })
    }
}
fn check_packet(packet: &Packet) -> Result<(), AgentError> {
    let bytes = match packet {
        Packet::Text(text) => text.len(),
        Packet::Binary(bytes) => bytes.len(),
    };
    if bytes > MAX_PACKET_BYTES {
        return Err(AgentError::Protocol("v2 packet exceeds byte budget"));
    }
    Ok(())
}
fn enqueue(sender: SessionSender, outbound: Outbound) -> WriteFuture {
    Box::pin(async move {
        tokio::time::timeout_at(outbound.deadline, sender.send(outbound.packet))
            .await
            .map_err(|_| AgentError::Protocol("v2 network enqueue timed out"))??;
        Ok(outbound.id)
    })
}
struct InputPump {
    sender: PipelineSender,
    timeout: Duration,
    normal: VecDeque<(usize, Frame)>,
    urgent: VecDeque<(usize, Frame)>,
    normal_active: Option<(usize, bool, InputFuture)>,
    urgent_active: Option<(usize, InputFuture)>,
    count: usize,
    bytes: usize,
    pending_end: bool,
}
impl InputPump {
    fn new(sender: PipelineSender, timeout: Duration) -> Self {
        Self {
            sender,
            timeout,
            normal: VecDeque::new(),
            urgent: VecDeque::new(),
            normal_active: None,
            urgent_active: None,
            count: 0,
            bytes: 0,
            pending_end: false,
        }
    }
    fn admit(&mut self, frame: Frame, urgent: bool) -> Result<(), AgentError> {
        let bytes = match &frame.data {
            FrameData::InputAudio(audio)
            | FrameData::OutputAudio(audio)
            | FrameData::TtsAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => audio.audio.len(),
            FrameData::InputText(text) => text.text.len(),
            _ => 256,
        };
        if bytes > MAX_PCM_BYTES
            || self.count >= INPUT_CAPACITY
            || self.bytes + bytes > INPUT_BYTE_CAPACITY
        {
            return Err(AgentError::Protocol("v2 input backpressure limit reached"));
        }
        self.count += 1;
        self.bytes += bytes;
        if urgent {
            self.urgent.push_back((bytes, frame));
        } else {
            self.normal.push_back((bytes, frame));
        }
        Ok(())
    }
    fn work(&self, frame: Frame) -> InputFuture {
        let sender = self.sender.clone();
        let deadline = Instant::now() + self.timeout;
        Box::pin(async move {
            tokio::time::timeout_at(deadline, sender.send(frame, Direction::Downstream))
                .await
                .map_err(|_| AgentError::Protocol("v2 pipeline input admission timed out"))??;
            Ok(())
        })
    }
    fn advance(&mut self, ordinary_open: bool) {
        if self.urgent_active.is_none() {
            if let Some((bytes, frame)) = self.urgent.pop_front() {
                self.urgent_active = Some((bytes, self.work(frame)));
            }
        }
        if ordinary_open
            && self.urgent_active.is_none()
            && self.urgent.is_empty()
            && self.normal_active.is_none()
        {
            if let Some((bytes, frame)) = self.normal.pop_front() {
                let interruptible = frame.interruptible;
                self.normal_active = Some((bytes, interruptible, self.work(frame)));
            } else if self.pending_end {
                self.pending_end = false;
                self.count += 1;
                self.normal_active = Some((0, false, self.work(Frame::end())));
            }
        }
    }
    fn finish_normal(&mut self) {
        if let Some((bytes, _, _)) = self.normal_active.take() {
            self.count -= 1;
            self.bytes -= bytes;
        }
    }
    fn finish_urgent(&mut self) {
        if let Some((bytes, _)) = self.urgent_active.take() {
            self.count -= 1;
            self.bytes -= bytes;
        }
    }
    fn discard_interruptible(&mut self) {
        let mut removed_count = 0;
        let mut removed_bytes = 0;
        self.normal.retain(|(bytes, frame)| {
            if frame.interruptible {
                removed_count += 1;
                removed_bytes += bytes;
                false
            } else {
                true
            }
        });
        self.count -= removed_count;
        self.bytes -= removed_bytes;
        if self
            .normal_active
            .as_ref()
            .is_some_and(|(_, interruptible, _)| *interruptible)
        {
            self.finish_normal();
        }
    }
    fn clear(&mut self) {
        self.normal.clear();
        self.urgent.clear();
        self.normal_active = None;
        self.urgent_active = None;
        self.count = 0;
        self.bytes = 0;
        self.pending_end = false;
    }
    fn cancel(&mut self) {
        self.clear();
        let _ = self.admit(Frame::cancel("v2 session closing"), true);
    }
}

#[cfg(test)]
mod input_pump_tests {
    use super::*;
    #[tokio::test]
    async fn cutoff_discards_queued_and_active_old_input_and_releases_budget() {
        let worker = Pipeline::new(vec![Arc::new(pipecat_core::IdentityProcessor)])
            .start(PipelineConfig::default())
            .await
            .unwrap();
        let mut pump = InputPump::new(worker.sender(), Duration::from_secs(1));
        let input =
            |text: &str| Frame::new(FrameData::InputText(pipecat_core::TextFrame::new(text)));
        pump.admit(input("old-active"), false).unwrap();
        pump.advance(true);
        pump.admit(input("old-queued"), false).unwrap();
        let mut durable = input("keep");
        durable.interruptible = false;
        pump.admit(durable, false).unwrap();
        pump.discard_interruptible();
        assert!(pump.normal_active.is_none());
        assert_eq!(pump.normal.len(), 1);
        assert_eq!(pump.count, 1);
        assert_eq!(pump.bytes, 4);
        pump.advance(false);
        assert!(
            pump.normal_active.is_none(),
            "closed media gate must retain ordinary input"
        );
        pump.admit(Frame::interruption(), true).unwrap();
        pump.advance(false);
        assert!(
            pump.urgent_active.is_some(),
            "urgent admission must progress through a media gate"
        );
        pump.clear();
        tokio::time::timeout(Duration::from_secs(1), worker.cancel("unit complete"))
            .await
            .unwrap()
            .unwrap();
        tokio::time::timeout(Duration::from_secs(1), worker.collect())
            .await
            .unwrap()
            .unwrap();
    }
}
