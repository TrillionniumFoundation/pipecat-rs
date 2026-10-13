use super::{
    state::{Command, Pending, Phase, Shared},
    GATE_TOKEN, MAX_PACKET_BYTES, MAX_PCM_BYTES,
};
use crate::wire;
use pipecat_audio::PcmFormat;
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    StartFrame,
};
use pipecat_media_transport::{
    OutputBackend, OutputControl, OutputTransport, OutputTransportConfig, PlaybackReceipt,
    SpokenHistory,
};
use pipecat_protocol::protobuf::{self as pb, frame::Kind};
use std::{collections::BTreeMap, sync::Arc};
use tokio::{sync::oneshot, time::Instant};

pub struct ReceiptBackend {
    pub(super) shared: Arc<Shared>,
}
struct RequestGuard {
    shared: Arc<Shared>,
    id: u64,
    armed: bool,
}
impl Drop for RequestGuard {
    fn drop(&mut self) {
        if self.armed {
            self.shared.revoke(self.id);
        }
    }
}
impl ReceiptBackend {
    pub fn session_id(&self) -> &str {
        &self.shared.session_id
    }
    /// Close admission immediately; the session driver retains responsibility
    /// for flushing/closing transport and joining its owner tasks.
    pub fn fail_closed(&self) {
        self.shared.close();
    }
    pub(super) fn input_cutoff(&self) -> Result<u64, ProcessorError> {
        self.shared.request_cutoff(false, true)
    }
}
#[async_trait]
impl OutputBackend for ReceiptBackend {
    async fn start(
        &self,
        format: PcmFormat,
        destinations: &[Option<String>],
    ) -> Result<(), ProcessorError> {
        if format.sample_rate() != self.shared.output_rate
            || format.channels() != 1
            || destinations != [None]
        {
            return Err(ProcessorError::new(
                "v2 requires its negotiated mono rate and default destination",
            ));
        }
        let mut state = self.shared.state.lock().unwrap();
        if state.closed || state.stopping || state.started {
            return Err(ProcessorError::new(
                "v2 backend cannot be started twice or after shutdown",
            ));
        }
        state.started = true;
        Ok(())
    }
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
        frame
            .require_no_native_llm_continuation()
            .map_err(|_| ProcessorError::new("unconsumed native continuation"))?;
        frame
            .require_wire_pts()
            .map_err(|_| ProcessorError::new("pipeline PTS needs explicit rebase"))?;
        if frame.transport_destination.is_some() {
            return Err(ProcessorError::new("v2 has one default destination"));
        }
        let audio = match &frame.data {
            FrameData::OutputAudio(a)
            | FrameData::TtsAudio(a)
            | FrameData::SpeechOutputAudio(a) => a,
            _ => return Err(ProcessorError::new("v2 playback requires PCM")),
        };
        if audio.audio.is_empty()
            || audio.audio.len() > MAX_PCM_BYTES
            || audio.sample_rate != self.shared.output_rate
            || audio.num_channels != 1
        {
            return Err(ProcessorError::new(
                "v2 PCM differs from the negotiated format or byte budget",
            ));
        }
        audio
            .validate()
            .map_err(|_| ProcessorError::new("invalid v2 PCM"))?;
        let deadline = Instant::now() + self.shared.config.chunk_timeout;
        // Already selected uninterruptible media retains the last published
        // epoch while a selective cutoff gates interruptible admission.
        let random = super::nonce().map_err(|_| {
            self.shared.close();
            ProcessorError::new("v2 chunk entropy failed")
        })?;
        let (done, waiting) = oneshot::channel();
        let (id, packet) = {
            let mut state = self.shared.state.lock().unwrap();
            if state.closed || state.stopping || !state.started {
                return Err(ProcessorError::new("v2 output admission is closed"));
            }
            // An all-audio cutoff permanently rejects old epochs at the
            // client. Even preserved/new uninterruptible work cannot reuse
            // one while an external media gate delays play_epoch publication.
            if state.play_epoch < state.all_cutoff_floor {
                return Ok(PlaybackReceipt::Discarded);
            }
            if frame.interruptible && (state.gate_closed || state.cutoff.is_some()) {
                return Ok(PlaybackReceipt::Discarded);
            }
            if state.pending.is_some() {
                return Err(ProcessorError::new("v2 pending PCM budget exhausted"));
            }
            state.chunk_id = match state
                .chunk_id
                .checked_add(1)
                .filter(|id| *id <= super::MAX_ID)
            {
                Some(id) => id,
                None => {
                    state.closed = true;
                    state.stopping = true;
                    self.shared.changed.notify_waiters();
                    return Err(ProcessorError::new("v2 chunk identity exhausted"));
                }
            };
            let id = state.chunk_id;
            let epoch = state.play_epoch;
            let name = serde_json::json!({
                "version":2, "session_id":self.shared.session_id, "epoch":epoch,
                "chunk_id":id, "receipt_nonce":random, "pcm_bytes":audio.audio.len(),
                "interruptible":frame.interruptible,
            })
            .to_string();
            let packet = wire::packet(Kind::Audio(pb::AudioRawFrame {
                id,
                name,
                audio: audio.audio.clone(),
                sample_rate: audio.sample_rate,
                num_channels: u32::from(audio.num_channels),
                pts: frame.pts,
            }))
            .map_err(|_| ProcessorError::new("v2 PCM encoding failed"))?;
            let size = match &packet {
                pipecat_transports::Packet::Binary(b) => b.len(),
                pipecat_transports::Packet::Text(t) => t.len(),
            };
            if size > MAX_PACKET_BYTES {
                return Err(ProcessorError::new("v2 encoded PCM exceeds packet budget"));
            }
            state.pending = Some(Pending {
                id,
                epoch,
                nonce: random,
                interruptible: frame.interruptible,
                phase: Phase::Registered,
                acked: false,
                revoked: false,
                deadline,
                done: Some(done),
            });
            (id, packet)
        };
        let mut guard = RequestGuard {
            shared: self.shared.clone(),
            id,
            armed: true,
        };
        tokio::time::timeout_at(deadline, self.shared.commands.send(Command { id, packet }))
            .await
            .map_err(|_| ProcessorError::new("v2 output admission timed out"))?
            .map_err(|_| ProcessorError::new("v2 session owner stopped"))?;
        let result = tokio::time::timeout_at(deadline, waiting)
            .await
            .map_err(|_| ProcessorError::new("v2 client completion timed out"))?
            .map_err(|_| ProcessorError::new("v2 completion owner stopped"))?;
        // Discarded means the owner already retained and fenced any ambiguous
        // network enqueue; Played exists only at the exact two-condition fence.
        guard.armed = false;
        result
    }
    async fn interrupt(&self, destination: Option<&str>) -> Result<(), ProcessorError> {
        if destination.is_some() {
            return Err(ProcessorError::new("v2 has one default destination"));
        }
        let deadline = Instant::now() + self.shared.config.cutoff_timeout;
        self.shared.request_cutoff(false, false)?;
        self.shared.quiescent(deadline).await
    }
    async fn stop(&self) -> Result<(), ProcessorError> {
        {
            let mut state = self.shared.state.lock().unwrap();
            if state.closed {
                return Ok(());
            }
            state.stopping = true;
        }
        let deadline = Instant::now() + self.shared.config.cutoff_timeout;
        self.shared.request_cutoff(true, false)?;
        self.shared.quiescent(deadline).await
    }
    async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
        let FrameData::OutputTransportMessage(value) = &frame.data else {
            return Err(ProcessorError::new(
                "v2 does not support external transport controls",
            ));
        };
        let stage = value
            .get("pipecat-cli-private-gate")
            .and_then(serde_json::Value::as_str);
        if value.get("session_id").and_then(serde_json::Value::as_str)
            != Some(self.shared.session_id.as_str())
            || frame.interruptible
            || frame.pts.is_some()
            || frame.transport_destination.is_some()
        {
            return Err(ProcessorError::new(
                "v2 does not support external transport controls",
            ));
        }
        let token = frame
            .metadata
            .get(GATE_TOKEN)
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| ProcessorError::new("v2 private media marker lacks its identity"))?;
        match stage {
            Some("probe") => self.shared.probe(token),
            Some("release") => {
                self.shared
                    .release(token, Instant::now() + self.shared.config.cutoff_timeout)
                    .await
            }
            _ => Err(ProcessorError::new(
                "v2 does not support external transport controls",
            )),
        }
    }
    // supports_independent_video remains the trait's false default.
}

/// A narrow lifecycle delegate around the actual shared OutputTransport. The
/// mark is published only after its real media queue has observed Interruption.
/// This does not reimplement media scheduling, chunking or spoken-history logic.
pub struct ReceiptOutput {
    output: OutputTransport,
    backend: Arc<ReceiptBackend>,
}
impl ReceiptOutput {
    pub fn new(
        name: impl Into<String>,
        config: OutputTransportConfig,
        backend: Arc<ReceiptBackend>,
        history: Option<SpokenHistory>,
    ) -> Result<Self, ProcessorError> {
        if !config.destinations.is_empty() {
            return Err(ProcessorError::new(
                "v2 output only supports the default destination",
            ));
        }
        let mut output =
            OutputTransport::new(name, config, backend.clone(), None, BTreeMap::new())?;
        if let Some(history) = history {
            output = output.with_spoken_history(history)?;
        }
        Ok(Self { output, backend })
    }
    pub fn control(&self) -> OutputControl {
        self.output.control()
    }
}
#[async_trait]
impl FrameProcessor for ReceiptOutput {
    fn name(&self) -> &str {
        self.output.name()
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.output.setup(start, ctx).await
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let token = if matches!(frame.data, FrameData::Interruption) {
            Some(match frame.metadata.remove(GATE_TOKEN) {
                Some(value) => value
                    .as_u64()
                    .ok_or_else(|| ProcessorError::new("invalid v2 input cutoff token"))?,
                None => self.backend.input_cutoff()?,
            })
        } else {
            None
        };
        self.output.process(frame, direction, ctx).await?;
        if let Some(token) = token {
            self.backend.shared.mark_media(token)?;
            // Two ordered normal-lane callbacks are essential. The first may
            // be popped after this iteration's old-epoch observe. The second
            // necessarily follows another real Sender.observe_interruption.
            for stage in ["probe", "release"] {
                let mut marker = Frame::new(FrameData::OutputTransportMessage(serde_json::json!({
                    "pipecat-cli-private-gate":stage, "session_id":self.backend.shared.session_id,
                })));
                marker.interruptible = false;
                marker
                    .metadata
                    .insert(GATE_TOKEN.into(), serde_json::json!(token));
                self.output
                    .process(marker, Direction::Downstream, ctx)
                    .await?;
            }
        }
        Ok(())
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.output.prepare_end(ctx).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.output.cleanup().await
    }
}
