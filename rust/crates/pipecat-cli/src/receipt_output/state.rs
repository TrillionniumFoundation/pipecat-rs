use super::{nonce, ReceiptConfig};
use crate::{
    wire::{self, V2Control},
    AgentError,
};
use pipecat_core::ProcessorError;
use pipecat_media_transport::PlaybackReceipt;
use pipecat_transports::Packet;
use serde_json::json;
use std::sync::{Arc, Mutex};
use tokio::{
    sync::{mpsc, oneshot, Notify},
    time::Instant,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum Phase {
    Registered,
    Sending,
    Enqueued,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum OutboundId {
    Chunk(u64),
    Cutoff(u64),
    Notice,
}
pub(super) struct Command {
    pub id: u64,
    pub packet: Packet,
}
pub(super) struct Outbound {
    pub id: OutboundId,
    pub packet: Packet,
    pub deadline: Instant,
}
pub(super) struct Pending {
    pub id: u64,
    pub epoch: u64,
    pub nonce: String,
    pub interruptible: bool,
    pub phase: Phase,
    pub acked: bool,
    pub revoked: bool,
    pub deadline: Instant,
    pub done: Option<oneshot::Sender<Result<PlaybackReceipt, ProcessorError>>>,
}
pub(super) struct Cutoff {
    pub id: u64,
    pub epoch: u64,
    pub nonce: String,
    pub all: bool,
    pub phase: Phase,
    pub acked: bool,
    pub deadline: Instant,
}
pub(super) struct Ledger {
    pub closed: bool,
    pub stopping: bool,
    pub started: bool,
    pub epoch: u64,
    pub play_epoch: u64,
    pub all_cutoff_floor: u64,
    pub chunk_id: u64,
    pub cutoff_id: u64,
    pub pending: Option<Pending>,
    pub cutoff: Option<Cutoff>,
    pub latest_gate: u64,
    pub media_gate: u64,
    pub probe_gate: u64,
    pub gate_closed: bool,
}
pub(super) struct Shared {
    pub session_id: String,
    pub output_rate: u32,
    pub config: ReceiptConfig,
    pub state: Mutex<Ledger>,
    pub changed: Notify,
    pub commands: mpsc::Sender<Command>,
}
impl Shared {
    pub fn new(
        session_id: String,
        output_rate: u32,
        config: ReceiptConfig,
    ) -> (Arc<Self>, mpsc::Receiver<Command>) {
        let (commands, rx) = mpsc::channel(8);
        (
            Arc::new(Self {
                session_id,
                output_rate,
                config,
                commands,
                changed: Notify::new(),
                state: Mutex::new(Ledger {
                    closed: false,
                    stopping: false,
                    started: false,
                    epoch: 1,
                    play_epoch: 1,
                    all_cutoff_floor: 1,
                    chunk_id: 0,
                    cutoff_id: 0,
                    pending: None,
                    cutoff: None,
                    latest_gate: 0,
                    media_gate: 0,
                    probe_gate: 0,
                    gate_closed: false,
                }),
            }),
            rx,
        )
    }
    fn close_locked(state: &mut Ledger, message: &'static str) {
        state.closed = true;
        state.stopping = true;
        if let Some(mut pending) = state.pending.take() {
            if let Some(done) = pending.done.take() {
                let _ = done.send(Err(ProcessorError::new(message)));
            }
        }
        state.cutoff = None;
    }
    pub fn close(&self) {
        let mut state = self.state.lock().unwrap();
        Self::close_locked(&mut state, "v2 session closed before playback completion");
        drop(state);
        self.changed.notify_waiters();
    }
    /// Reliable synchronous revocation. No bounded queue and no task spawn in Drop.
    pub fn revoke(&self, id: u64) {
        if self.cutoff_request(false, false, Some(id)).is_err() {
            self.close();
        }
    }
    pub fn request_cutoff(&self, all: bool, external: bool) -> Result<u64, ProcessorError> {
        self.cutoff_request(all, external, None)
    }
    fn cutoff_request(
        &self,
        mut all: bool,
        external: bool,
        expected: Option<u64>,
    ) -> Result<u64, ProcessorError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Err(ProcessorError::new("v2 session is closed"));
        }
        if let Some(expected) = expected {
            let Some(pending) = state.pending.as_ref().filter(|p| p.id == expected) else {
                return Ok(state.latest_gate);
            };
            all = !pending.interruptible;
        }
        macro_rules! advance {
            ($field:ident, $message:literal) => {
                match state
                    .$field
                    .checked_add(1)
                    .filter(|id| *id <= super::MAX_ID)
                {
                    Some(next) => state.$field = next,
                    None => {
                        Self::close_locked(&mut state, $message);
                        self.changed.notify_waiters();
                        return Err(ProcessorError::new($message));
                    }
                }
            };
        }
        if external {
            advance!(latest_gate, "v2 gate exhausted");
            state.gate_closed = true;
        }
        let token = state.latest_gate;
        let must_revoke = state
            .pending
            .as_ref()
            .is_some_and(|p| all || p.interruptible);
        let mut needs_flush = false;
        if must_revoke {
            let pending = state.pending.as_mut().unwrap();
            pending.revoked = true;
            needs_flush = pending.phase != Phase::Registered;
            if let Some(done) = pending.done.take() {
                let _ = done.send(Ok(PlaybackReceipt::Discarded));
            }
            if !needs_flush {
                state.pending = None;
            }
        }
        // A stronger all-audio request can never reuse a selective ACK.
        let upgrade = state.cutoff.as_ref().is_some_and(|c| all && !c.all);
        if state.cutoff.is_none() || upgrade {
            advance!(epoch, "v2 epoch exhausted");
            if needs_flush || upgrade {
                let random = match nonce() {
                    Ok(random) => random,
                    Err(_) => {
                        Self::close_locked(&mut state, "v2 cutoff entropy failed");
                        self.changed.notify_waiters();
                        return Err(ProcessorError::new("v2 cutoff entropy failed"));
                    }
                };
                advance!(cutoff_id, "v2 cutoff identity exhausted");
                if all {
                    state.all_cutoff_floor = state.all_cutoff_floor.max(state.epoch);
                }
                state.cutoff = Some(Cutoff {
                    id: state.cutoff_id,
                    epoch: state.epoch,
                    nonce: random,
                    all,
                    phase: Phase::Registered,
                    acked: false,
                    deadline: Instant::now() + self.config.cutoff_timeout,
                });
            }
        }
        if !state.gate_closed && state.cutoff.is_none() {
            state.play_epoch = state.epoch;
        }
        drop(state);
        self.changed.notify_waiters();
        Ok(token)
    }
    pub fn mark_media(&self, token: u64) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().unwrap();
        if token == 0 || token > state.latest_gate {
            return Err(ProcessorError::new("unknown v2 input cutoff token"));
        }
        state.media_gate = state.media_gate.max(token);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    pub fn next_cutoff(&self) -> Result<Option<Outbound>, AgentError> {
        let mut state = self.state.lock().unwrap();
        let Some(cutoff) = state
            .cutoff
            .as_mut()
            .filter(|c| c.phase == Phase::Registered)
        else {
            return Ok(None);
        };
        let packet = wire::message(json!({
            "type":"playback-cutoff", "version":2, "session_id":self.session_id,
            "epoch":cutoff.epoch, "cutoff_id":cutoff.id,
            "receipt_nonce":cutoff.nonce, "all":cutoff.all,
        }))?;
        cutoff.phase = Phase::Sending;
        Ok(Some(Outbound {
            id: OutboundId::Cutoff(cutoff.id),
            packet,
            deadline: cutoff
                .deadline
                .min(Instant::now() + self.config.enqueue_timeout),
        }))
    }
    pub fn begin_chunk(&self, command: Command) -> Option<Outbound> {
        let mut state = self.state.lock().unwrap();
        let pending = state.pending.as_mut()?;
        if pending.id != command.id || pending.revoked || pending.phase != Phase::Registered {
            return None;
        }
        pending.phase = Phase::Sending;
        Some(Outbound {
            id: OutboundId::Chunk(command.id),
            packet: command.packet,
            deadline: pending
                .deadline
                .min(Instant::now() + self.config.enqueue_timeout),
        })
    }
    pub fn enqueued(&self, id: OutboundId) {
        let mut state = self.state.lock().unwrap();
        match id {
            OutboundId::Chunk(id) => {
                if let Some(pending) = state.pending.as_mut().filter(|p| p.id == id) {
                    pending.phase = Phase::Enqueued;
                }
            }
            OutboundId::Cutoff(id) => {
                if let Some(cutoff) = state.cutoff.as_mut().filter(|c| c.id == id) {
                    cutoff.phase = Phase::Enqueued;
                }
            }
            OutboundId::Notice => {}
        }
        Self::settle(&mut state);
        drop(state);
        self.changed.notify_waiters();
    }
    pub fn receipt(&self, control: V2Control) -> Result<(), AgentError> {
        let mut state = self.state.lock().unwrap();
        if state.closed {
            return Ok(());
        }
        match control {
            V2Control::ChunkComplete {
                version,
                session_id,
                epoch,
                chunk_id,
                receipt_nonce,
            } => {
                if version == 2 && session_id == self.session_id {
                    if let Some(pending) = state.pending.as_mut().filter(|p| {
                        p.id == chunk_id
                            && p.epoch == epoch
                            && p.nonce == receipt_nonce
                            && !p.revoked
                            && p.phase != Phase::Registered
                            && Instant::now() < p.deadline
                    }) {
                        pending.acked = true;
                    }
                }
            }
            V2Control::CutoffComplete {
                version,
                session_id,
                epoch,
                cutoff_id,
                receipt_nonce,
                all,
            } => {
                if version == 2 && session_id == self.session_id {
                    if let Some(cutoff) = state.cutoff.as_mut().filter(|c| {
                        c.id == cutoff_id
                            && c.epoch == epoch
                            && c.nonce == receipt_nonce
                            && c.all == all
                            && c.phase != Phase::Registered
                            && Instant::now() < c.deadline
                    }) {
                        cutoff.acked = true;
                    }
                }
            }
            V2Control::Accept { .. } => {
                return Err(AgentError::Protocol("v2 handshake cannot be repeated"))
            }
        }
        Self::settle(&mut state);
        drop(state);
        self.changed.notify_waiters();
        Ok(())
    }
    fn settle(state: &mut Ledger) {
        if state.pending.as_ref().is_some_and(|p| {
            p.acked && !p.revoked && p.phase == Phase::Enqueued && Instant::now() < p.deadline
        }) {
            if let Some(mut pending) = state.pending.take() {
                if let Some(done) = pending.done.take() {
                    let _ = done.send(Ok(PlaybackReceipt::Played));
                }
            }
        }
        if state
            .cutoff
            .as_ref()
            .is_some_and(|c| c.acked && c.phase == Phase::Enqueued && Instant::now() < c.deadline)
        {
            let cutoff = state.cutoff.take().unwrap();
            if state.pending.as_ref().is_some_and(|p| {
                p.revoked && p.epoch < cutoff.epoch && (cutoff.all || p.interruptible)
            }) {
                state.pending = None;
            }
            // A pure backend cancellation has no queue-generation marker.
            // It can advance only when no external media gate is still closed.
            if !state.gate_closed {
                state.play_epoch = state.epoch;
            }
        }
    }
    pub fn deadline(&self) -> Option<Instant> {
        let state = self.state.lock().unwrap();
        state
            .pending
            .as_ref()
            .filter(|p| !p.revoked)
            .map(|p| p.deadline)
            .into_iter()
            .chain(state.cutoff.as_ref().map(|c| c.deadline))
            .min()
    }
    pub fn timed_out(&self) -> bool {
        self.deadline().is_some_and(|d| Instant::now() >= d)
    }
    pub async fn quiescent(&self, deadline: Instant) -> Result<(), ProcessorError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let state = self.state.lock().unwrap();
                if state.closed {
                    return Err(ProcessorError::new("v2 session closed during cutoff"));
                }
                // This no-argument backend hook cannot identify which queue
                // epoch the media Sender sampled. It never reopens admission.
                if state.cutoff.is_none() {
                    return Ok(());
                }
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| ProcessorError::new("v2 cutoff confirmation timed out"))?;
        }
    }
    pub fn probe(&self, token: u64) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().unwrap();
        if state.closed || token == 0 || token > state.media_gate {
            return Err(ProcessorError::new("unknown v2 media probe"));
        }
        state.probe_gate = state.probe_gate.max(token);
        Ok(())
    }
    pub async fn release(&self, token: u64, deadline: Instant) -> Result<(), ProcessorError> {
        loop {
            let changed = self.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            {
                let mut state = self.state.lock().unwrap();
                if state.closed {
                    return Err(ProcessorError::new(
                        "v2 session closed during media release",
                    ));
                }
                if token == 0 || token > state.media_gate || token > state.probe_gate {
                    return Err(ProcessorError::new(
                        "v2 media release lacks its ordered probe",
                    ));
                }
                if token < state.latest_gate || state.stopping {
                    return Ok(());
                }
                if token == state.latest_gate && token == state.media_gate && state.cutoff.is_none()
                {
                    state.gate_closed = false;
                    state.play_epoch = state.epoch;
                    drop(state);
                    self.changed.notify_waiters();
                    return Ok(());
                }
            }
            tokio::time::timeout_at(deadline, changed)
                .await
                .map_err(|_| ProcessorError::new("v2 ordered media release timed out"))?;
        }
    }
}
