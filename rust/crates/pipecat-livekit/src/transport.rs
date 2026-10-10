use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc, Mutex,
};

use async_trait::async_trait;
use pipecat_core::{
    Direction, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError, StartFrame,
};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use crate::{ConnectionState, Error, LiveKitConnection, LiveKitEvent};

/// Pipeline endpoints around an already-connected room. Construction never creates tokens.
/// The input endpoint owns event consumption; do not also call `connection.recv()`.
pub struct LiveKitTransport {
    input: Arc<LiveKitInput>,
    output: Arc<LiveKitOutput>,
}
impl LiveKitTransport {
    pub fn new(connection: Arc<LiveKitConnection>) -> Self {
        let capacity = connection.config().queue_capacity;
        Self {
            input: Arc::new(LiveKitInput {
                connection: connection.clone(),
                stop: CancellationToken::new(),
                started: AtomicBool::new(false),
            }),
            output: Arc::new(LiveKitOutput {
                connection,
                normal: OutputLane::new(capacity),
                urgent: OutputLane::new(capacity),
                epoch: Arc::new(AtomicU64::new(0)),
                started: AtomicBool::new(false),
            }),
        }
    }
    pub fn input(&self) -> Arc<LiveKitInput> {
        self.input.clone()
    }
    pub fn output(&self) -> Arc<LiveKitOutput> {
        self.output.clone()
    }
}
pub struct LiveKitInput {
    connection: Arc<LiveKitConnection>,
    stop: CancellationToken,
    started: AtomicBool,
}
#[async_trait]
impl FrameProcessor for LiveKitInput {
    fn name(&self) -> &str {
        "livekit-input"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        ensure_ready(&self.connection)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() && !self.started.swap(true, Ordering::SeqCst) {
            ctx.push(frame, direction).await?;
            let connection = self.connection.clone();
            let stop = self.stop.clone();
            let output = ctx.clone();
            ctx.spawn("livekit-input-events", false, async move {
                loop {
                    let event = tokio::select! { biased; _ = stop.cancelled() => return Ok(()), event = connection.recv() => event };
                    match event {
                        Some(LiveKitEvent::Disconnected { reason }) => return Err(ProcessorError::fatal(format!("LiveKit disconnected: {reason}"))),
                        Some(LiveKitEvent::Error(message)) => return Err(ProcessorError::fatal(message)),
                        Some(event) => if let Some(frame) = event.into_frame().map_err(processor_error)? { output.push(frame, Direction::Downstream).await?; },
                        None => return Err(ProcessorError::fatal(format!("LiveKit event stream closed: {:?}", connection.state()))),
                    }
                }
            })?;
            Ok(())
        } else {
            if matches!(
                frame.data,
                FrameData::Cancel(_) | FrameData::CancelDetailed(_)
            ) {
                self.stop.cancel();
            }
            ctx.push(frame, direction).await
        }
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.stop.cancel();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.stop.cancel();
        self.connection.close().await.map_err(processor_error)
    }
}
struct Queued {
    frame: Frame,
    epoch: u64,
    audio_generation: u64,
}
pub struct LiveKitOutput {
    connection: Arc<LiveKitConnection>,
    normal: OutputLane,
    urgent: OutputLane,
    epoch: Arc<AtomicU64>,
    started: AtomicBool,
}
#[async_trait]
impl FrameProcessor for LiveKitOutput {
    fn name(&self) -> &str {
        "livekit-output"
    }
    async fn setup(&self, _: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        ensure_ready(&self.connection)
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() && !self.started.swap(true, Ordering::SeqCst) {
            ctx.push(frame, direction).await?;
            start_output_lane(
                "livekit-output-publish",
                &self.normal,
                self.connection.clone(),
                self.epoch.clone(),
                ctx,
            )?;
            start_output_lane(
                "livekit-output-urgent",
                &self.urgent,
                self.connection.clone(),
                self.epoch.clone(),
                ctx,
            )?;
            ctx.push(
                Frame::new(FrameData::OutputTransportReady),
                Direction::Upstream,
            )
            .await?;
            return Ok(());
        }
        if matches!(
            frame.data,
            FrameData::Interruption | FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.epoch.fetch_add(1, Ordering::SeqCst);
            self.connection.interrupt_audio();
            if matches!(
                frame.data,
                FrameData::Cancel(_) | FrameData::CancelDetailed(_)
            ) {
                self.normal.close();
                self.urgent.close();
            }
            return ctx.push(frame, direction).await;
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        let urgent = matches!(
            frame.data,
            FrameData::OutputTransportMessageUrgent(_) | FrameData::OutputDtmfUrgent(_)
        );
        let supported = matches!(
            frame.data,
            FrameData::OutputAudio(_)
                | FrameData::TtsAudio(_)
                | FrameData::OutputTransportMessage(_)
                | FrameData::OutputTransportMessageUrgent(_)
                | FrameData::OutputDtmf(_)
                | FrameData::OutputDtmfUrgent(_)
        );
        check_frame_size(&frame, &self.connection)?;
        if !supported {
            return ctx.push(frame, direction).await;
        }
        let queued = Queued {
            frame,
            epoch: self.epoch.load(Ordering::SeqCst),
            audio_generation: self.connection.audio_generation(),
        };
        if urgent {
            self.urgent.push(queued)
        } else {
            self.normal.push(queued)
        }
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.normal.close();
        self.urgent.close();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.normal.close();
        self.urgent.close();
        self.connection.close().await.map_err(processor_error)
    }
}
fn ensure_ready(connection: &LiveKitConnection) -> Result<(), ProcessorError> {
    if connection.state() == ConnectionState::Connected {
        Ok(())
    } else {
        Err(ProcessorError::new(
            "LiveKit room must be connected before pipeline setup",
        ))
    }
}
fn processor_error(error: impl std::fmt::Display) -> ProcessorError {
    ProcessorError::new(error.to_string())
}
fn check_frame_size(frame: &Frame, connection: &LiveKitConnection) -> Result<(), ProcessorError> {
    crate::protocol::bounded_json(&frame.metadata, 65536).map_err(processor_error)?;
    if frame
        .transport_destination
        .as_ref()
        .is_some_and(|s| s.len() > 256)
    {
        return Err(ProcessorError::new("destination exceeds 256 bytes"));
    }
    match &frame.data {
        FrameData::OutputAudio(audio) | FrameData::TtsAudio(audio) => {
            audio.validate().map_err(processor_error)?;
            if audio.audio.len() > connection.config().max_audio_frame_bytes {
                return Err(processor_error(Error::Limit("output audio frame bytes")));
            }
            if frame.transport_destination.is_some() {
                return Err(ProcessorError::new("LiveKit publishes one room audio track; per-participant audio destinations are unsupported"));
            }
        }
        FrameData::OutputTransportMessage(message)
        | FrameData::OutputTransportMessageUrgent(message) => {
            if crate::protocol::bounded_json(message, connection.config().max_data_bytes)
                .map_err(processor_error)?
                .len()
                > connection.config().max_data_bytes
            {
                return Err(processor_error(Error::Limit("output data bytes")));
            }
        }
        FrameData::OutputImage(_) => {
            return Err(ProcessorError::new(
                "LiveKit video output is not implemented",
            ))
        }
        _ => {}
    }
    Ok(())
}
async fn publish(connection: &LiveKitConnection, frame: &Frame) -> crate::Result<bool> {
    match &frame.data {
        FrameData::OutputAudio(audio) | FrameData::TtsAudio(audio) => {
            connection.publish_audio(audio).await?;
        }
        FrameData::OutputTransportMessage(message)
        | FrameData::OutputTransportMessageUrgent(message) => {
            let topic = frame
                .metadata
                .get("livekit.topic")
                .and_then(|v| v.as_str())
                .map(str::to_owned);
            connection
                .publish_data(
                    crate::protocol::bounded_json(message, connection.config().max_data_bytes)?,
                    frame.transport_destination.as_deref(),
                    topic,
                    true,
                )
                .await?;
        }
        FrameData::OutputDtmf(digit) | FrameData::OutputDtmfUrgent(digit) => {
            connection
                .publish_dtmf(*digit, frame.transport_destination.as_deref())
                .await?;
        }
        _ => return Ok(false),
    }
    Ok(true)
}

struct OutputLane {
    tx: Mutex<Option<mpsc::Sender<Queued>>>,
    rx: Mutex<Option<mpsc::Receiver<Queued>>>,
}
impl OutputLane {
    fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            tx: Mutex::new(Some(tx)),
            rx: Mutex::new(Some(rx)),
        }
    }
    fn close(&self) {
        self.tx.lock().unwrap().take();
    }
    fn push(&self, queued: Queued) -> Result<(), ProcessorError> {
        self.tx
            .lock()
            .unwrap()
            .as_ref()
            .ok_or_else(|| ProcessorError::new("LiveKit output closed"))?
            .try_send(queued)
            .map_err(|e| ProcessorError::new(format!("LiveKit bounded output queue: {e}")))
    }
}
fn start_output_lane(
    name: &'static str,
    lane: &OutputLane,
    connection: Arc<LiveKitConnection>,
    epoch: Arc<AtomicU64>,
    ctx: &ProcessorContext,
) -> Result<(), ProcessorError> {
    let mut rx = lane
        .rx
        .lock()
        .unwrap()
        .take()
        .ok_or_else(|| ProcessorError::new("LiveKit output is single-use"))?;
    ctx.spawn(name, false, async move {
        while let Some(queued) = rx.recv().await {
            if queued.frame.interruptible
                && (queued.epoch != epoch.load(Ordering::SeqCst)
                    || queued.audio_generation != connection.audio_generation())
            {
                continue;
            }
            match publish(&connection, &queued.frame).await {
                Err(Error::Interrupted) => continue,
                Err(error) => return Err(processor_error(error)),
                Ok(_) => {}
            }
        }
        Ok(())
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn queued() -> Queued {
        Queued {
            frame: Frame::new(FrameData::OutputTransportMessage(
                serde_json::json!({"msg":"test"}),
            )),
            epoch: 0,
            audio_generation: 0,
        }
    }
    #[tokio::test]
    async fn output_lane_is_bounded_and_close_drains_without_detaching() {
        let lane = OutputLane::new(1);
        lane.push(queued()).unwrap();
        assert!(lane.push(queued()).is_err());
        lane.close();
        assert!(lane.push(queued()).is_err());
        let mut rx = lane.rx.lock().unwrap().take().unwrap();
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_none());
    }
    #[test]
    fn urgent_lane_admits_while_normal_audio_lane_is_full() {
        let normal = OutputLane::new(1);
        let urgent = OutputLane::new(1);
        normal.push(queued()).unwrap();
        assert!(normal.push(queued()).is_err());
        urgent.push(queued()).unwrap();
    }
}
