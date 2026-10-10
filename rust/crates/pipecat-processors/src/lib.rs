//! Native reusable frame filters and bounded gates.
// Copyright (c) 2024–2026, Daily. SPDX-License-Identifier: BSD-2-Clause
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FramePriority, FrameProcessor, ProcessorContext,
    ProcessorError,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};

pub type FramePredicate = Arc<dyn Fn(&Frame) -> bool + Send + Sync>;
pub type AsyncPredicate = Arc<
    dyn Fn(Frame) -> Pin<Box<dyn Future<Output = Result<bool, ProcessorError>> + Send>>
        + Send
        + Sync,
>;
fn system(frame: &Frame) -> bool {
    frame.data.priority() != FramePriority::Normal
}
fn lifecycle(frame: &Frame) -> bool {
    matches!(
        frame.data,
        FrameData::Start(_)
            | FrameData::StartDetailed(_)
            | FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
    )
}

#[derive(Default)]
pub struct IdentityFilter;
#[async_trait]
impl FrameProcessor for IdentityFilter {
    fn name(&self) -> &str {
        "IdentityFilter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        ctx.push(frame, direction).await
    }
}
#[derive(Default)]
pub struct NullFilter;
#[async_trait]
impl FrameProcessor for NullFilter {
    fn name(&self) -> &str {
        "NullFilter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if system(&frame) || lifecycle(&frame) {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
}
pub struct FrameFilter {
    predicate: FramePredicate,
}
impl FrameFilter {
    pub fn new(predicate: impl Fn(&Frame) -> bool + Send + Sync + 'static) -> Self {
        Self {
            predicate: Arc::new(predicate),
        }
    }
}
#[async_trait]
impl FrameProcessor for FrameFilter {
    fn name(&self) -> &str {
        "FrameFilter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if (self.predicate)(&frame) || system(&frame) || lifecycle(&frame) {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
}
/// The predicate runs on every frame, including bypassed directions, as in Python.
pub struct FunctionFilter {
    predicate: AsyncPredicate,
    pub direction: Option<Direction>,
    pub filter_system_frames: bool,
}
impl FunctionFilter {
    pub fn new(predicate: AsyncPredicate) -> Self {
        Self {
            predicate,
            direction: Some(Direction::Downstream),
            filter_system_frames: false,
        }
    }
}
#[async_trait]
impl FrameProcessor for FunctionFilter {
    fn name(&self) -> &str {
        "FunctionFilter"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let bypass = self.direction.is_some_and(|d| d != direction)
            || lifecycle(&frame)
            || (!self.filter_system_frames && system(&frame));
        let allowed = (self.predicate)(frame.clone()).await?;
        if bypass || allowed {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
}
struct GateState {
    open: bool,
    buffered: Vec<(Frame, Direction)>,
}
/// Bounded gate: opening frame precedes buffered frames, including the closing marker.
/// Lifecycle termination flushes pending frames; interruption discards interruptible frames.
pub struct GatedAggregator {
    opens: FramePredicate,
    closes: FramePredicate,
    direction: Direction,
    capacity: usize,
    state: Mutex<GateState>,
}
impl GatedAggregator {
    pub fn new(
        opens: FramePredicate,
        closes: FramePredicate,
        start_open: bool,
        direction: Direction,
        capacity: usize,
    ) -> Result<Self, ProcessorError> {
        if capacity == 0 {
            return Err(ProcessorError::new("gate capacity must be positive"));
        }
        Ok(Self {
            opens,
            closes,
            direction,
            capacity,
            state: Mutex::new(GateState {
                open: start_open,
                buffered: Vec::new(),
            }),
        })
    }
    pub fn buffered_len(&self) -> Result<usize, ProcessorError> {
        Ok(self
            .state
            .lock()
            .map_err(|_| ProcessorError::fatal("gate poisoned"))?
            .buffered
            .len())
    }
}
#[async_trait]
impl FrameProcessor for GatedAggregator {
    fn name(&self) -> &str {
        "GatedAggregator"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let outputs = {
            let mut state = self
                .state
                .lock()
                .map_err(|_| ProcessorError::fatal("gate poisoned"))?;
            if matches!(
                frame.data,
                FrameData::Interruption | FrameData::Cancel(_) | FrameData::CancelDetailed(_)
            ) {
                state.buffered.retain(|(f, _)| !f.interruptible);
            }
            if system(&frame) || direction != self.direction {
                vec![(frame, direction)]
            } else if lifecycle(&frame) {
                let mut pending = std::mem::take(&mut state.buffered);
                pending.push((frame, direction));
                pending
            } else {
                state.open = if state.open {
                    !(self.closes)(&frame)
                } else {
                    (self.opens)(&frame)
                };
                if state.open {
                    let mut outputs = vec![(frame, direction)];
                    outputs.append(&mut state.buffered);
                    outputs
                } else {
                    if state.buffered.len() >= self.capacity {
                        return Err(ProcessorError::new("gate capacity exceeded"));
                    }
                    state.buffered.push((frame, direction));
                    vec![]
                }
            }
        };
        for (frame, direction) in outputs {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("gate poisoned"))?
            .buffered
            .clear();
        Ok(())
    }
}

pub mod tools;

pub mod dtmf;
pub use dtmf::{DtmfAggregator, DtmfAggregatorConfig};
