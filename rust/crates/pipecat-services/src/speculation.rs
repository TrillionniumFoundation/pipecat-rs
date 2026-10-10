//! Bounded native output ownership for eager completions.
use pipecat_core::{
    Direction, Frame, FrameData, NativeSpeculation, ProcessorContext, ProcessorError,
    SpeculationStatus,
};
use std::{io::Write, sync::Arc};
use tokio::sync::Mutex;

/// Limits include routing metadata and exact signed continuation carriers.
#[derive(Debug, Clone, Copy)]
pub struct SpeculationLimits {
    pub max_frames: usize,
    pub max_bytes: usize,
}
impl Default for SpeculationLimits {
    fn default() -> Self {
        Self {
            max_frames: 512,
            max_bytes: 4 * 1024 * 1024,
        }
    }
}
impl SpeculationLimits {
    pub fn validate(self) -> Result<Self, ProcessorError> {
        if self.max_frames == 0
            || self.max_frames > 65_536
            || self.max_bytes == 0
            || self.max_bytes > 64 * 1024 * 1024
        {
            return Err(ProcessorError::new(
                "speculation bounds require 1..65536 frames and 1..67108864 bytes",
            ));
        }
        Ok(self)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SpeculationSnapshot {
    pub status: Option<SpeculationStatus>,
    pub buffered_frames: usize,
    pub buffered_bytes: usize,
}
struct Held {
    frame: Frame,
    direction: Direction,
    source: ProcessorContext,
    health: pipecat_core::ProcessorHealthGeneration,
}
struct State {
    active: Option<NativeSpeculation>,
    held: Vec<Held>,
    bytes: usize,
    closed: bool,
    holding: bool,
}
/// One emission lock orders verdict boundaries, retained frames and new stream
/// output. It is never held during provider I/O or application handlers.
#[derive(Clone)]
pub(crate) struct OutputGate {
    state: Arc<Mutex<State>>,
    limits: SpeculationLimits,
}
impl OutputGate {
    pub fn new(limits: SpeculationLimits) -> Self {
        Self {
            state: Arc::new(Mutex::new(State {
                active: None,
                held: vec![],
                bytes: 0,
                closed: false,
                holding: false,
            })),
            limits,
        }
    }
    pub async fn snapshot(&self) -> SpeculationSnapshot {
        let state = self.state.lock().await;
        SpeculationSnapshot {
            status: state.active.as_ref().map(NativeSpeculation::status),
            buffered_frames: state.held.len(),
            buffered_bytes: state.bytes,
        }
    }
    pub async fn begin(&self, capability: Option<NativeSpeculation>) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        if state.closed {
            if let Some(capability) = capability {
                capability.cancel();
            }
            return Ok(());
        }
        if let Some(previous) = state.active.take() {
            previous.cancel();
        }
        flush(&mut state, false).await?;
        state.active = capability;
        Ok(())
    }
    pub async fn control(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        let terminal = frame.is_terminal();
        let cancel = terminal
            || matches!(
                frame.data,
                FrameData::Interruption | FrameData::EagerEndOfTurnCancel
            );
        let capability = frame.take_native_speculation();
        let matches_active = capability.as_ref().is_none_or(|capability| {
            state
                .active
                .as_ref()
                .is_none_or(|active| active == capability)
        });
        // Cancel carries identity when generated internally. A stale cancellation
        // cannot withdraw a replacement prediction; an external STT cancel can.
        if cancel && matches_active {
            if let Some(active) = &state.active {
                active.cancel();
            }
            if let Some(capability) = &capability {
                capability.cancel();
            }
        }
        if terminal {
            state.closed = true;
            flush(&mut state, false).await?;
            return ctx.push(frame, direction).await;
        }
        let confirm = direction == Direction::Downstream
            && matches!(frame.data, FrameData::UserStoppedSpeaking);
        // Other consumers do not need the private verdict capability.
        ctx.push(frame, direction).await?;
        if confirm {
            if let Some(capability) = capability {
                capability.release_after_boundary();
            }
        }
        if cancel && matches_active {
            flush(&mut state, false).await?;
        } else if state
            .active
            .as_ref()
            .is_some_and(|c| c.status() == SpeculationStatus::Released)
        {
            flush(&mut state, true).await?;
        }
        Ok(())
    }
    pub async fn push(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
        source: Option<&NativeSpeculation>,
    ) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        if source.is_some_and(|source| source.status() == SpeculationStatus::Cancelled) {
            return Ok(());
        }
        if state.closed && source.is_some() && frame.interruptible {
            return Ok(());
        }
        let generated_tool = source.is_some()
            && matches!(
                frame.data,
                FrameData::FunctionCallsStarted(_) | FrameData::FunctionCallsStartedDetailed(_)
            );
        if direction == Direction::Upstream || (frame.is_system() && !generated_tool) {
            return ctx.push(frame, direction).await;
        }
        if state
            .active
            .as_ref()
            .is_some_and(|c| c.status() == SpeculationStatus::Cancelled)
        {
            flush(&mut state, false).await?;
        }
        if state
            .active
            .as_ref()
            .is_some_and(|c| c.status() == SpeculationStatus::Released)
        {
            flush(&mut state, true).await?;
        }
        if source.is_some_and(|source| state.active.as_ref() == Some(source))
            && matches!(
                frame.data,
                FrameData::LlmResponseStart | FrameData::LlmFullResponseStart(_)
            )
        {
            state.holding = true;
        }
        let holding = state.holding
            && state.active.as_ref().is_some_and(|c| {
                matches!(
                    c.status(),
                    SpeculationStatus::Pending | SpeculationStatus::Confirmed
                )
            });
        if !holding {
            return ctx.push(frame, direction).await;
        }
        let budget = self.limits.max_bytes.saturating_sub(state.bytes);
        let bytes = frame_bytes(&frame, budget);
        if state.held.len() >= self.limits.max_frames || bytes.is_none() {
            let capability = state.active.clone().expect("holding capability");
            capability.cancel();
            flush(&mut state, false).await?;
            if !frame.interruptible {
                ctx.push(frame, direction).await?;
            }
            drop(state);
            return withdraw(ctx, capability).await;
        }
        state.bytes += bytes.expect("checked budget");
        state.held.push(Held {
            frame,
            direction,
            source: ctx.clone(),
            health: ctx.health().generation(),
        });
        Ok(())
    }
    pub async fn discard_cancelled(&self) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        if state
            .active
            .as_ref()
            .is_some_and(|c| c.status() == SpeculationStatus::Cancelled)
        {
            flush(&mut state, false).await?;
        }
        Ok(())
    }
    pub async fn close(&self) -> Result<(), ProcessorError> {
        let mut state = self.state.lock().await;
        state.closed = true;
        if let Some(active) = &state.active {
            active.cancel();
        }
        flush(&mut state, false).await
    }
}
async fn flush(state: &mut State, release: bool) -> Result<(), ProcessorError> {
    let held = std::mem::take(&mut state.held);
    state.bytes = 0;
    state.holding = false;
    for held in held {
        if (release && held.source.health().generation() == held.health)
            || !held.frame.interruptible
        {
            held.source
                .push_with_causal_fence(held.frame, held.direction)
                .await?;
        }
    }
    Ok(())
}
pub(crate) async fn withdraw(
    ctx: &ProcessorContext,
    capability: NativeSpeculation,
) -> Result<(), ProcessorError> {
    let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
    frame.set_native_speculation(capability)?;
    ctx.broadcast(frame).await
}
/// Counting serializer bounds retained payloads without allocating a second
/// copy, and refuses opaque local callbacks it cannot account for.
fn frame_bytes(frame: &Frame, limit: usize) -> Option<usize> {
    struct Counter {
        bytes: usize,
        limit: usize,
    }
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.bytes = self
                .bytes
                .checked_add(bytes.len())
                .filter(|bytes| *bytes <= self.limit)
                .ok_or_else(|| std::io::Error::other("speculation byte budget"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let overhead = std::mem::size_of::<Held>()
        .checked_add(frame.native_llm_continuation_retained_bytes())?
        .checked_add(frame.name.len())?
        .checked_add(frame.transport_source.as_ref().map_or(0, String::len))?
        .checked_add(frame.transport_destination.as_ref().map_or(0, String::len))?;
    let mut counter = Counter {
        bytes: overhead,
        limit,
    };
    if overhead > limit {
        return None;
    }
    serde_json::to_writer(&mut counter, &frame.metadata).ok()?;
    // A native checkpoint is intentionally not serializable. Its validated
    // carrier is already fully included in retained-byte accounting above.
    frame.native_llm_continuation().ok()?;
    if !matches!(frame.data, FrameData::LlmContinuationCheckpoint) {
        serde_json::to_writer(&mut counter, &frame.data).ok()?;
    }
    Some(counter.bytes)
}
