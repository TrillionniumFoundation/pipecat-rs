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
        let video_capacity = connection
            .config()
            .video_output
            .as_ref()
            .map_or(1, |video| video.queue_capacity);
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
                video: OutputLane::new(video_capacity),
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
                        Some(event) => dispatch_input(event, &output).await?,
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
    video_generation: u64,
}
pub struct LiveKitOutput {
    connection: Arc<LiveKitConnection>,
    normal: OutputLane,
    urgent: OutputLane,
    video: OutputLane,
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
            start_output_lane(
                "livekit-output-video",
                &self.video,
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
            self.connection.interrupt_video();
            if matches!(
                frame.data,
                FrameData::Cancel(_) | FrameData::CancelDetailed(_)
            ) {
                self.normal.close();
                self.urgent.close();
                self.connection.stop_video();
                self.video.discard();
            }
            return ctx.push(frame, direction).await;
        }
        let route = output_route(&frame, direction, self.connection.config())?;
        if route == OutputRoute::Pass {
            return ctx.push(frame, direction).await;
        }
        let queued = Queued {
            frame,
            epoch: self.epoch.load(Ordering::SeqCst),
            audio_generation: self.connection.audio_generation(),
            video_generation: self.connection.video_generation(),
        };
        match route {
            OutputRoute::Video => self.video.push(queued),
            OutputRoute::Urgent => self.urgent.push(queued),
            OutputRoute::Normal => self.normal.push(queued),
            OutputRoute::Pass => unreachable!("handled above"),
        }
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.normal.close();
        self.urgent.close();
        self.video.close();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.normal.close();
        self.urgent.close();
        self.video.discard();
        self.connection.close().await.map_err(processor_error)
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OutputRoute {
    Pass,
    Normal,
    Urgent,
    Video,
}
fn output_route(
    frame: &Frame,
    direction: Direction,
    config: &crate::LiveKitConfig,
) -> Result<OutputRoute, ProcessorError> {
    if direction == Direction::Upstream {
        return Ok(OutputRoute::Pass);
    }
    check_frame_size(frame, config)?;
    Ok(match &frame.data {
        FrameData::OutputImage(_) | FrameData::OutputImageDetailed(_) => OutputRoute::Video,
        FrameData::OutputTransportMessageUrgent(_) | FrameData::OutputDtmfUrgent(_) => {
            OutputRoute::Urgent
        }
        FrameData::OutputAudio(_)
        | FrameData::TtsAudio(_)
        | FrameData::OutputTransportMessage(_)
        | FrameData::OutputDtmf(_) => OutputRoute::Normal,
        _ => OutputRoute::Pass,
    })
}
fn stale(queued: &Queued, epoch: u64, audio_generation: u64) -> bool {
    queued.frame.interruptible
        && (queued.epoch != epoch || queued.audio_generation != audio_generation)
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
fn check_frame_size(frame: &Frame, config: &crate::LiveKitConfig) -> Result<(), ProcessorError> {
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
            if audio.audio.len() > config.max_audio_frame_bytes {
                return Err(processor_error(Error::Limit("output audio frame bytes")));
            }
            if frame.transport_destination.is_some() {
                return Err(ProcessorError::new("LiveKit publishes one room audio track; per-participant audio destinations are unsupported"));
            }
        }
        FrameData::OutputTransportMessage(message)
        | FrameData::OutputTransportMessageUrgent(message) => {
            if crate::protocol::bounded_json(message, config.max_data_bytes)
                .map_err(processor_error)?
                .len()
                > config.max_data_bytes
            {
                return Err(processor_error(Error::Limit("output data bytes")));
            }
        }
        FrameData::OutputImage(_) | FrameData::OutputImageDetailed(_) => {
            let image = crate::video::video_input(&frame.data).expect("matched image variant");
            config
                .video_output
                .as_ref()
                .ok_or_else(|| processor_error(Error::VideoDisabled))?
                .validate_image(image, frame.transport_destination.as_deref())
                .map_err(processor_error)?;
        }
        _ => {}
    }
    Ok(())
}
async fn publish(connection: &LiveKitConnection, queued: &Queued) -> crate::Result<bool> {
    let frame = &queued.frame;
    match &frame.data {
        FrameData::OutputImage(_) | FrameData::OutputImageDetailed(_) => {
            let image = crate::video::video_input(&frame.data).expect("matched image variant");
            connection.publish_video_generation(
                image,
                frame.interruptible.then_some(queued.video_generation),
            )?;
        }
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
    discard: CancellationToken,
}
impl OutputLane {
    fn new(capacity: usize) -> Self {
        let (tx, rx) = mpsc::channel(capacity);
        Self {
            tx: Mutex::new(Some(tx)),
            rx: Mutex::new(Some(rx)),
            discard: CancellationToken::new(),
        }
    }
    fn close(&self) {
        self.tx.lock().unwrap().take();
    }
    fn discard(&self) {
        self.discard.cancel();
        self.close();
        // If not started yet, release queued buffers here rather than retaining until Drop.
        self.rx.lock().unwrap().take();
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
    let discard = lane.discard.clone();
    ctx.spawn(name, false, async move {
        loop {
            let queued = tokio::select! { biased;
                _ = discard.cancelled() => return Ok(()),
                queued = rx.recv() => match queued { Some(queued) => queued, None => break },
            };
            if discard.is_cancelled() {
                return Ok(());
            }
            if stale(
                &queued,
                epoch.load(Ordering::SeqCst),
                connection.audio_generation(),
            ) {
                continue;
            }
            match publish(&connection, &queued).await {
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
            video_generation: 0,
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

#[cfg(test)]
mod video_lane_tests {
    use super::*;
    use pipecat_core::{ImageRawFrame, OutputImageRawFrame};
    fn queued_image() -> Queued {
        Queued {
            frame: Frame::new(FrameData::OutputImageDetailed(OutputImageRawFrame {
                image: ImageRawFrame {
                    image: vec![80, 90, 240],
                    size: (1, 1),
                    format: Some("I420".into()),
                },
                sync_with_audio: false,
            })),
            epoch: 0,
            audio_generation: 0,
            video_generation: 0,
        }
    }
    #[test]
    fn production_route_passes_upstream_and_routes_only_valid_video_to_dedicated_lane() {
        let mut config = crate::LiveKitConfig::default();
        let queued = queued_image();
        assert_eq!(
            output_route(&queued.frame, Direction::Upstream, &config).unwrap(),
            OutputRoute::Pass
        );
        assert!(output_route(&queued.frame, Direction::Downstream, &config).is_err());
        config.video_output = Some(crate::VideoOutputConfig {
            width: 1,
            height: 1,
            ..Default::default()
        });
        assert_eq!(
            output_route(&queued.frame, Direction::Downstream, &config).unwrap(),
            OutputRoute::Video
        );
        let urgent = Frame::new(FrameData::OutputTransportMessageUrgent(
            serde_json::json!({"m":1}),
        ));
        assert_eq!(
            output_route(&urgent, Direction::Downstream, &config).unwrap(),
            OutputRoute::Urgent
        );
        let normal = Frame::new(FrameData::OutputTransportMessage(
            serde_json::json!({"m":1}),
        ));
        assert_eq!(
            output_route(&normal, Direction::Downstream, &config).unwrap(),
            OutputRoute::Normal
        );
    }
    #[test]
    fn both_core_image_variants_share_route_limits_and_destination_rejection() {
        for detailed in [false, true] {
            let output = OutputImageRawFrame {
                image: ImageRawFrame {
                    image: vec![80, 90, 240],
                    size: (1, 1),
                    format: Some("I420".into()),
                },
                sync_with_audio: false,
            };
            let mut frame = Frame::new(if detailed {
                FrameData::OutputImageDetailed(output)
            } else {
                FrameData::OutputImage(output.image)
            });
            let mut config = crate::LiveKitConfig::default();
            assert_eq!(
                output_route(&frame, Direction::Upstream, &config).unwrap(),
                OutputRoute::Pass
            );
            assert!(output_route(&frame, Direction::Downstream, &config).is_err());
            config.video_output = Some(crate::VideoOutputConfig {
                width: 1,
                height: 1,
                ..Default::default()
            });
            assert_eq!(
                output_route(&frame, Direction::Downstream, &config).unwrap(),
                OutputRoute::Video
            );
            frame.transport_destination = Some("alice".into());
            assert!(output_route(&frame, Direction::Downstream, &config).is_err());
            frame.transport_destination = None;
            match &mut frame.data {
                FrameData::OutputImage(image) => image.image.push(0),
                FrameData::OutputImageDetailed(output) => output.image.image.push(0),
                _ => unreachable!(),
            }
            assert!(output_route(&frame, Direction::Downstream, &config).is_err());
        }
    }
    #[test]
    fn production_epoch_check_discards_stale_interruptible_not_noninterruptible_video() {
        let mut queued = queued_image();
        queued.frame.interruptible = true;
        assert!(!stale(&queued, 0, 0));
        assert!(stale(&queued, 1, 0));
        assert!(stale(&queued, 0, 1));
        queued.frame.interruptible = false;
        assert!(!stale(&queued, 1, 1));
    }
    #[test]
    fn video_admission_validates_disabled_destination_sync_and_payload_before_queue() {
        let mut config = crate::LiveKitConfig::default();
        let mut queued = queued_image();
        assert!(check_frame_size(&queued.frame, &config).is_err());
        config.video_output = Some(crate::VideoOutputConfig {
            width: 1,
            height: 1,
            ..Default::default()
        });
        assert!(check_frame_size(&queued.frame, &config).is_ok());
        queued.frame.transport_destination = Some("alice".into());
        assert!(check_frame_size(&queued.frame, &config).is_err());
        queued.frame.transport_destination = None;
        if let FrameData::OutputImageDetailed(image) = &mut queued.frame.data {
            image.sync_with_audio = true;
        }
        assert!(check_frame_size(&queued.frame, &config).is_err());
        if let FrameData::OutputImageDetailed(image) = &mut queued.frame.data {
            image.sync_with_audio = false;
            image.image.image.push(0);
        }
        assert!(check_frame_size(&queued.frame, &config).is_err());
    }
    #[tokio::test]
    async fn separate_video_lane_bounds_images_and_end_drains() {
        let video = OutputLane::new(2);
        let normal = OutputLane::new(1);
        video.push(queued_image()).unwrap();
        video.push(queued_image()).unwrap();
        assert!(video.push(queued_image()).is_err());
        normal.push(queued_image()).unwrap(); // independent capacity, production only routes images to video
        video.close();
        assert!(video.push(queued_image()).is_err());
        let mut rx = video.rx.lock().unwrap().take().unwrap();
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_some());
        assert!(rx.recv().await.is_none());
    }
    #[test]
    fn video_cancel_discards_noninterruptible_before_start_and_signals_owned_worker() {
        let lane = OutputLane::new(2);
        let mut image = queued_image();
        image.frame.interruptible = false;
        lane.push(image).unwrap();
        lane.discard();
        assert!(lane.rx.lock().unwrap().is_none());
        assert!(lane.push(queued_image()).is_err());
        let started = OutputLane::new(2);
        let _owned_receiver = started.rx.lock().unwrap().take().unwrap();
        started.push(queued_image()).unwrap();
        started.discard();
        assert!(started.discard.is_cancelled());
    }
}

/// Shared production input dispatch; push acknowledges bounded pipeline admission only.
pub(crate) async fn dispatch_input(
    event: LiveKitEvent,
    output: &ProcessorContext,
) -> Result<(), ProcessorError> {
    if let Some(frame) = event.into_frame().map_err(processor_error)? {
        output.push(frame, Direction::Downstream).await?;
    }
    Ok(())
}
