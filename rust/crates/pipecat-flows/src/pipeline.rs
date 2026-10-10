use crate::{FlowError, FlowManager, FlowStatus};
use async_trait::async_trait;
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, PipelineSender, ProcessorContext, ProcessorError,
};
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};
use tokio::sync::oneshot;

/// A sink must preserve normal-frame ordering. drain() must acknowledge all
/// previously queued frames at the downstream action boundary, not just their
/// admission into a queue. SharedContext must be maintained by the sink's
/// pipeline/aggregators; FlowManager never duplicates those context writes.
#[async_trait]
pub trait FlowSink: Send + Sync {
    async fn queue_frame(&self, frame: Frame) -> Result<(), FlowError>;
    async fn drain(&self) -> Result<(), FlowError>;
    fn observe_frame(&self, _frame: &Frame) {}
}
const BARRIER: &str = "pipecat.flows.barrier";
static NEXT_BARRIER: AtomicU64 = AtomicU64::new(1);
#[derive(Default)]
pub struct PipelineFlowSink {
    sender: Mutex<Option<PipelineSender>>,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<()>>>>,
}
struct BarrierGuard {
    id: u64,
    pending: Arc<Mutex<BTreeMap<u64, oneshot::Sender<()>>>>,
}
impl Drop for BarrierGuard {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.pending.lock() {
            pending.remove(&self.id);
        }
    }
}
impl PipelineFlowSink {
    /// Construct before the pipeline, then bind its sender after start().
    pub fn new() -> Self {
        Self::default()
    }
    pub fn bind(&self, sender: PipelineSender) -> Result<(), FlowError> {
        let mut slot = self
            .sender
            .lock()
            .map_err(|_| FlowError::Pipeline("sink lock poisoned".into()))?;
        if slot.is_some() {
            return Err(FlowError::Config(
                "pipeline flow sink is already bound".into(),
            ));
        }
        *slot = Some(sender);
        Ok(())
    }
    fn sender(&self) -> Result<PipelineSender, FlowError> {
        self.sender
            .lock()
            .map_err(|_| FlowError::Pipeline("sink lock poisoned".into()))?
            .clone()
            .ok_or_else(|| {
                FlowError::Pipeline(
                    "bind the flow sink to a running pipeline before initialization".into(),
                )
            })
    }
    pub fn pending_barriers(&self) -> usize {
        self.pending.lock().map(|p| p.len()).unwrap_or(0)
    }
}
#[async_trait]
impl FlowSink for PipelineFlowSink {
    async fn queue_frame(&self, frame: Frame) -> Result<(), FlowError> {
        self.sender()?
            .send(frame, Direction::Downstream)
            .await
            .map_err(Into::into)
    }
    async fn drain(&self) -> Result<(), FlowError> {
        let sender = self.sender()?;
        let mut state = sender.watch();
        if state.borrow().is_finished() {
            return Err(FlowError::Closed);
        }
        let id = NEXT_BARRIER.fetch_add(1, Ordering::Relaxed);
        let (tx, mut rx) = oneshot::channel();
        self.pending
            .lock()
            .map_err(|_| FlowError::Pipeline("barrier lock poisoned".into()))?
            .insert(id, tx);
        let _guard = BarrierGuard {
            id,
            pending: self.pending.clone(),
        };
        let mut frame = Frame::new(FrameData::FlushDetailed(pipecat_core::PipelineFlushFrame {
            origin: Some("pipecat.flows".into()),
            ..Default::default()
        }))
        .with_interruptible(false);
        frame.metadata.insert(BARRIER.into(), id.into());
        sender.send(frame, Direction::Downstream).await?;
        loop {
            tokio::select! {
                biased;
                result = &mut rx => return result.map_err(|_| FlowError::Closed),
                changed = state.changed() => if changed.is_err() || state.borrow_and_update().is_finished() { return Err(FlowError::Closed); },
            }
        }
    }
    fn observe_frame(&self, frame: &Frame) {
        if !matches!(&frame.data, FrameData::FlushDetailed(flush) if flush.returning && flush.origin.as_deref() == Some("pipecat.flows"))
        {
            return;
        }
        if let Some(id) = frame
            .metadata
            .get(BARRIER)
            .and_then(serde_json::Value::as_u64)
        {
            if let Ok(mut pending) = self.pending.lock() {
                if let Some(tx) = pending.remove(&id) {
                    let _ = tx.send(());
                }
            }
        }
    }
}
/// Place after the LLM and before the assistant aggregator. Tool calls are
/// forwarded before execution, so the aggregator admits their declaration.
/// Pair this with FlowActionBoundary after aggregation and output transport.
pub struct FlowProcessor {
    manager: FlowManager,
}
impl FlowProcessor {
    pub fn new(manager: FlowManager) -> Self {
        Self { manager }
    }
}
#[async_trait]
impl FrameProcessor for FlowProcessor {
    fn name(&self) -> &str {
        "FlowProcessor"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let calls = if direction == Direction::Downstream
            && frame
                .metadata
                .get("pipecat.flows.managed_batch")
                .and_then(serde_json::Value::as_bool)
                != Some(true)
        {
            match &frame.data {
                FrameData::FunctionCallsStarted(calls) => Some(calls.clone()),
                FrameData::FunctionCallsStartedDetailed(calls) => Some(
                    calls
                        .iter()
                        .map(|call| pipecat_core::FunctionCall {
                            function_name: call.function_name.clone(),
                            tool_call_id: call.tool_call_id.clone(),
                            arguments: serde_json::Value::Object(
                                call.arguments.clone().into_iter().collect(),
                            ),
                        })
                        .collect(),
                ),
                _ => None,
            }
        } else {
            None
        };
        if direction == Direction::Downstream && matches!(frame.data, FrameData::Interruption) {
            self.manager.interrupt();
        }
        ctx.push(frame, direction).await?;
        if let Some(calls) = calls {
            let manager = self.manager.clone();
            ctx.spawn("flow-tool-batch", false, async move {
                match manager.execute_started_tools(calls).await {
                    Ok(_) | Err(FlowError::Cancelled | FlowError::Stale | FlowError::Closed) => {
                        Ok(())
                    }
                    Err(error) => Err(ProcessorError::new(error.to_string())),
                }
            })?;
        }
        Ok(())
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.manager.mark_ended();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.manager.cancel();
        Ok(())
    }
}
/// Final flow observation boundary, after the assistant aggregator and output
/// transport. Receipts acknowledge ordered traversal, not device playback;
/// output transports must hold ordered controls until prior playback finishes.
pub struct FlowActionBoundary {
    manager: FlowManager,
}
impl FlowActionBoundary {
    pub fn new(manager: FlowManager) -> Self {
        Self { manager }
    }
}
#[async_trait]
impl FrameProcessor for FlowActionBoundary {
    fn name(&self) -> &str {
        "FlowActionBoundary"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if direction == Direction::Downstream {
            self.manager.observe_frame(&frame);
            if matches!(&frame.data, FrameData::FlushDetailed(flush) if flush.returning && flush.origin.as_deref() == Some("pipecat.flows"))
                && frame
                    .metadata
                    .get(BARRIER)
                    .and_then(serde_json::Value::as_u64)
                    .is_some()
            {
                return Ok(());
            }
            match &frame.data {
                FrameData::Interruption => self.manager.interrupt(),
                FrameData::Cancel(_) | FrameData::CancelDetailed(_) => self.manager.cancel(),
                FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                    self.manager.mark_ended()
                }
                FrameData::BotStoppedSpeaking
                    if self.manager.status() == FlowStatus::Running
                        && self.manager.action_manager().ongoing_actions() == 0 =>
                {
                    let manager = self.manager.clone();
                    let generation = manager.generation();
                    ctx.spawn("flow-post-actions", false, async move {
                        match manager.bot_stopped_speaking_at(generation).await {
                            Ok(())
                            | Err(FlowError::Cancelled | FlowError::Stale | FlowError::Closed) => {
                                Ok(())
                            }
                            Err(error) => Err(ProcessorError::new(error.to_string())),
                        }
                    })?;
                }
                _ => {}
            }
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.manager.mark_ended();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.manager.cancel();
        Ok(())
    }
}
