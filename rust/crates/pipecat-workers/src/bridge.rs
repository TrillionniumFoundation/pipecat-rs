//! Pipeline-local bus bridges. Incoming frames bypass this processor and carry
//! provenance, so an edge tap elsewhere in the same pipeline cannot echo them.
use crate::{BusMessage, LocalBus, MessageData};
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError, StartFrame,
};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use tokio::sync::watch;

pub(crate) const BUS_ORIGIN: &str = "pipecat.bus.origin";
static NEXT_BRIDGE: AtomicU64 = AtomicU64::new(1);
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BridgeMode {
    /// A mid-pipeline bridge consumes outgoing frames after bus admission.
    Consume,
    /// Output-only edge tap; outgoing frames also continue locally. Pair with
    /// PipelineBusInput or one consuming bridge for receive-side injection.
    Tee,
}
#[derive(Debug, Clone)]
pub struct BusBridgeOptions {
    pub worker_name: String,
    /// Restrict received sources and address outgoing publications to this peer.
    pub target_worker: Option<String>,
    /// Restrict incoming bridge tags and tag outgoing frames.
    pub bridge: Option<String>,
    pub mode: BridgeMode,
    /// None publishes both directions; edge taps commonly select one direction.
    pub outgoing_direction: Option<Direction>,
}
impl BusBridgeOptions {
    pub fn new(worker_name: impl Into<String>) -> Self {
        Self {
            worker_name: worker_name.into(),
            target_worker: None,
            bridge: None,
            mode: BridgeMode::Consume,
            outgoing_direction: None,
        }
    }
}
type FrameFilter = Arc<dyn Fn(&Frame) -> bool + Send + Sync>;
pub struct BusBridgeProcessor {
    name: String,
    bus: LocalBus,
    options: BusBridgeOptions,
    excluded: Option<FrameFilter>,
    stop: watch::Sender<bool>,
}
impl BusBridgeProcessor {
    pub fn new(name: impl Into<String>, bus: LocalBus, options: BusBridgeOptions) -> Self {
        Self {
            name: name.into(),
            bus,
            options,
            excluded: None,
            stop: watch::channel(false).0,
        }
    }
    /// Extra frames that remain local. Lifecycle and urgent transport messages
    /// always remain local, irrespective of this predicate.
    pub fn exclude(mut self, predicate: impl Fn(&Frame) -> bool + Send + Sync + 'static) -> Self {
        self.excluded = Some(Arc::new(predicate));
        self
    }
}
fn local_only(frame: &Frame) -> bool {
    frame.data.is_start()
        || frame.data.is_cancel()
        || matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::OutputTransportMessageUrgent(_)
        )
}
#[pipecat_core::async_trait]
impl FrameProcessor for BusBridgeProcessor {
    fn name(&self) -> &str {
        &self.name
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.options.mode == BridgeMode::Tee {
            return Ok(());
        }
        let subscription = self
            .bus
            .subscribe(format!(
                "{}/bridge/{}",
                self.options.worker_name,
                NEXT_BRIDGE.fetch_add(1, Ordering::Relaxed)
            ))
            .map_err(|error| ProcessorError::fatal(error.to_string()))?;
        let options = self.options.clone();
        let context = ctx.clone();
        self.stop.send_replace(false);
        let mut stop = self.stop.subscribe();
        ctx.spawn("bus-bridge-input", false, async move {
            loop {
                let message = tokio::select! { biased;
                    _ = stop.changed() => return Ok(()),
                    message = subscription.recv() => message.map_err(|error| ProcessorError::fatal(error.to_string()))?,
                };
                let Some(message) = message else { return Err(ProcessorError::fatal("bridge bus closed")); };
                if message.source == options.worker_name
                    || options.target_worker.as_ref().is_some_and(|peer| peer != &message.source)
                    || message.target.as_ref().is_some_and(|target| target != &options.worker_name) { continue; }
                if let MessageData::Frame { mut frame, direction, bridge } = message.data {
                    if options.bridge.as_ref().is_some_and(|name| Some(name) != bridge.as_ref()) || local_only(&frame) { continue; }
                    frame.metadata.insert(BUS_ORIGIN.into(), message.source.into());
                    context.push(*frame, direction).await?;
                }
            }
        })?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if local_only(&frame)
            || frame.metadata.contains_key(BUS_ORIGIN)
            || self.excluded.as_ref().is_some_and(|filter| filter(&frame))
            || self
                .options
                .outgoing_direction
                .is_some_and(|expected| expected != direction)
        {
            return ctx.push(frame, direction).await;
        }
        self.bus
            .publish(BusMessage {
                source: self.options.worker_name.clone(),
                target: self.options.target_worker.clone(),
                data: MessageData::Frame {
                    frame: Box::new(frame.clone()),
                    direction,
                    bridge: self.options.bridge.clone(),
                },
            })
            .map_err(|error| ProcessorError::fatal(error.to_string()))?;
        if self.options.mode == BridgeMode::Tee {
            ctx.push(frame, direction).await?;
        }
        Ok(())
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.stop.send_replace(true);
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.stop.send_replace(true);
        Ok(())
    }
}
