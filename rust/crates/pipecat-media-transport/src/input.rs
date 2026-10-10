use crate::input_video::VideoInput;
use crate::output::bounded;
use crate::queue::MediaQueue;
use crate::video::*;
use crate::{error, AudioFilter};
use async_trait::async_trait;
use pipecat_audio::{PcmFormat, SincResampler};
use pipecat_core::*;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

/// Capture runs as a processor-owned task. Implementations must release any
/// capture resources in `stop`, and cancellation must not detach device tasks.
#[async_trait]
pub trait InputBackend: Send + Sync {
    /// Explicit capability for configured captures. Implementations open these
    /// sources, then register availability on ingress before connected callbacks.
    fn supports_video_source(&self, _source: &VideoSourceId) -> bool {
        false
    }
    async fn start(&self, format: PcmFormat) -> Result<(), ProcessorError>;
    async fn capture(&self, ingress: InputIngress) -> Result<(), ProcessorError>;
    async fn stop(&self) -> Result<(), ProcessorError>;
}
#[derive(Debug, Clone)]
pub struct InputTransportConfig {
    pub sample_rate: Option<u32>,
    pub channels: u16,
    pub audio_enabled: bool,
    pub video_enabled: bool,
    pub video: VideoInputConfig,
    pub stream_on_start: bool,
    pub passthrough: bool,
    pub queue_capacity: usize,
    pub queue_byte_capacity: usize,
    pub operation_timeout: Duration,
}
impl Default for InputTransportConfig {
    fn default() -> Self {
        Self {
            sample_rate: None,
            channels: 1,
            audio_enabled: true,
            video_enabled: false,
            video: VideoInputConfig::default(),
            stream_on_start: true,
            passthrough: true,
            queue_capacity: 128,
            queue_byte_capacity: 2 * 1024 * 1024,
            operation_timeout: Duration::from_secs(10),
        }
    }
}
#[derive(Clone)]
pub struct InputIngress {
    queue: Arc<MediaQueue>,
    ready: Arc<AtomicBool>,
    paused: Arc<AtomicBool>,
    audio_enabled: bool,
    video_enabled: bool,
    video: Arc<VideoInput>,
}
impl InputIngress {
    pub fn pending_image_requests(&self) -> usize {
        self.video.pending()
    }
    pub fn configured_video_sources(
        &self,
    ) -> &std::collections::BTreeMap<VideoSourceId, VideoInSourceParams> {
        &self.video.config.sources
    }
    /// The backend must already have opened each configured capture. Register
    /// before publishing its connected event; unavailable sources stay captured
    /// but reject requests until their availability changes.
    pub fn video_participant_connected(
        &self,
        participant: VideoParticipantId,
        available: &[VideoSourceId],
    ) -> Result<(), ProcessorError> {
        self.require_video()?;
        self.video.connected(participant, available)
    }
    /// Attests backend capture, not an implicit camera or codec operation.
    pub fn capture_video_source(
        &self,
        key: VideoSourceKey,
        params: VideoInSourceParams,
        available: bool,
    ) -> Result<(), ProcessorError> {
        self.require_video()?;
        self.video.capture(key, params, available)
    }
    pub fn set_video_source_available(
        &self,
        key: &VideoSourceKey,
        available: bool,
    ) -> Result<(), ProcessorError> {
        self.require_video()?;
        self.video.availability(key, available)
    }
    pub fn remove_video_participant(&self, participant: &VideoParticipantId) {
        self.video.remove(Some(participant));
    }
    pub fn disconnect_video(&self) {
        self.video.remove(None);
    }
    /// Requests are owned only after successful admission. Duplicate or bounded
    /// admission failures return an error and do not invoke an unowned callback.
    pub fn request_image(&self, frame: Frame) -> Result<(), ProcessorError> {
        self.require_video()?;
        if frame.transport_destination != self.video.config.request_destination {
            return Err(ProcessorError::new(
                "image request targets a different input route",
            ));
        }
        self.video.request(frame)
    }
    pub fn push_video_frame(
        &self,
        key: &VideoSourceKey,
        frame: Frame,
    ) -> Result<(), ProcessorError> {
        self.require_video()?;
        if self.paused.load(Ordering::SeqCst) {
            return Ok(());
        }
        self.video.push(key, frame)
    }
    fn require_video(&self) -> Result<(), ProcessorError> {
        if !self.video_enabled || !self.is_ready() {
            return Err(ProcessorError::new("video input is disabled or not ready"));
        }
        Ok(())
    }
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    /// Muting capture drops newly arriving frames, without affecting output playback.
    pub fn pause(&self) {
        self.paused.store(true, Ordering::SeqCst);
    }
    pub fn resume(&self) {
        self.paused.store(false, Ordering::SeqCst);
    }
    /// Nonblocking bounded admission. A full queue is an explicit error.
    pub fn push(&self, frame: Frame) -> Result<(), ProcessorError> {
        if !self.is_ready() {
            return Err(ProcessorError::new("input transport is not ready"));
        }
        if self.paused.load(Ordering::SeqCst) {
            return Ok(());
        }
        match &frame.data {
            FrameData::InputAudio(a) if self.audio_enabled => {
                a.validate().map_err(error)?;
            }
            FrameData::InputImage(_) if self.video_enabled => {}
            FrameData::InputAudio(_) | FrameData::InputImage(_) => return Ok(()),
            _ => {
                return Err(ProcessorError::new(
                    "capture ingress accepts only input audio and input images",
                ))
            }
        }
        self.queue.push(frame)
    }
}
pub struct InputTransport {
    name: String,
    config: InputTransportConfig,
    backend: Arc<dyn InputBackend>,
    filter: Option<Arc<dyn AudioFilter>>,
    ingress: InputIngress,
    format: Mutex<Option<PcmFormat>>,
    capture: Mutex<Option<u64>>,
    streaming: Arc<AtomicBool>,
    cleaned: AtomicBool,
    started: AtomicBool,
}
impl InputTransport {
    pub fn new(
        name: impl Into<String>,
        config: InputTransportConfig,
        backend: Arc<dyn InputBackend>,
        filter: Option<Arc<dyn AudioFilter>>,
    ) -> Result<Self, ProcessorError> {
        if config.channels == 0
            || config.queue_capacity == 0
            || config.queue_byte_capacity == 0
            || config.operation_timeout.is_zero()
        {
            return Err(ProcessorError::new("invalid input transport configuration"));
        }
        config.video.validate()?;
        if !config.video_enabled && !config.video.sources.is_empty() {
            return Err(ProcessorError::new(
                "configured video sources require enabled video input",
            ));
        }
        let queue = MediaQueue::new(config.queue_capacity, config.queue_byte_capacity);
        let video = VideoInput::new(
            config.video.clone(),
            queue.clone(),
            config.queue_byte_capacity,
        )?;
        let ingress = InputIngress {
            queue,
            ready: Arc::new(AtomicBool::new(false)),
            paused: Arc::new(AtomicBool::new(false)),
            audio_enabled: config.audio_enabled,
            video_enabled: config.video_enabled,
            video,
        };
        Ok(Self {
            name: name.into(),
            config,
            backend,
            filter,
            ingress,
            format: Mutex::new(None),
            capture: Mutex::new(None),
            streaming: Arc::new(AtomicBool::new(false)),
            cleaned: AtomicBool::new(false),
            started: AtomicBool::new(false),
        })
    }
    pub fn ingress(&self) -> InputIngress {
        self.ingress.clone()
    }
    fn start_capture(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        if self.streaming.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let backend = self.backend.clone();
        let ingress = self.ingress.clone();
        let streaming = self.streaming.clone();
        let result = ctx.spawn(format!("{}-capture", self.name), false, async move {
            let _guard = StreamingGuard(streaming);
            let result = backend.capture(ingress.clone()).await;
            ingress.disconnect_video();
            result
        });
        match result {
            Ok(id) => {
                *self.capture.lock().unwrap() = Some(id);
                Ok(())
            }
            Err(e) => {
                self.streaming.store(false, Ordering::SeqCst);
                Err(e)
            }
        }
    }
}
struct StreamingGuard(Arc<AtomicBool>);
impl Drop for StreamingGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::SeqCst);
    }
}
#[async_trait]
impl FrameProcessor for InputTransport {
    fn name(&self) -> &str {
        &self.name
    }
    async fn setup(&self, start: &StartFrame, _: &ProcessorContext) -> Result<(), ProcessorError> {
        for source in self.config.video.sources.keys() {
            if !self.backend.supports_video_source(source) {
                return Err(ProcessorError::new(
                    "backend does not support a configured video source",
                ));
            }
        }
        let format = PcmFormat::new(
            self.config
                .sample_rate
                .unwrap_or(start.audio_in_sample_rate),
            self.config.channels,
        )
        .map_err(error)?;
        *self.format.lock().unwrap() = Some(format);
        if let Some(filter) = &self.filter {
            bounded(self.config.operation_timeout, filter.start(format)).await?;
        }
        bounded(self.config.operation_timeout, self.backend.start(format)).await?;
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        match &frame.data {
            data if data.is_start() => {
                if !self.started.load(Ordering::SeqCst) {
                    return Err(ProcessorError::fatal(
                        "input backend did not become ready during setup",
                    ));
                }
                ctx.push(frame, direction).await?;
                let queue = self.ingress.queue.clone();
                let filter = self.filter.clone();
                let format = self.format.lock().unwrap().unwrap();
                let config = self.config.clone();
                let worker_ctx = ctx.clone();
                let video = self.ingress.video.clone();
                ctx.spawn(format!("{}-input-media", self.name), false, async move {
                    input_worker(queue, video, filter, format, config, worker_ctx).await
                })?;
                self.ingress.video.start();
                self.ingress.ready.store(true, Ordering::SeqCst);
                if self.config.stream_on_start {
                    self.start_capture(ctx)?;
                }
                Ok(())
            }
            FrameData::InputTransportStartAudioStreaming => self.start_capture(ctx),
            FrameData::InputAudio(_) | FrameData::InputImage(_) => self.ingress.push(frame),
            FrameData::UserImageRequest(_)
                if frame.transport_destination == self.config.video.request_destination =>
            {
                self.ingress.request_image(frame)
            }
            FrameData::FilterEnable(_)
            | FrameData::FilterControl
            | FrameData::FilterUpdateSettings(_) => self.ingress.queue.push(frame),
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                self.ingress.ready.store(false, Ordering::SeqCst);
                self.ingress.video.shutdown(true);
                ctx.push(frame, direction).await
            }
            _ => ctx.push(frame, direction).await,
        }
    }
    async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.ingress.ready.store(false, Ordering::SeqCst);
        if let Some(id) = self.capture.lock().unwrap().take() {
            ctx.cancel_task(id);
        }
        self.ingress.video.shutdown(false);
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.cleaned.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        self.ingress.ready.store(false, Ordering::SeqCst);
        self.ingress.video.shutdown(true);
        // Application callbacks cannot delay release of capture resources when
        // the runtime's overall cleanup deadline is shorter than their timeout.
        let (video_result, backend_result, filter_result) = tokio::join!(
            self.ingress.video.complete(self.config.operation_timeout),
            bounded(self.config.operation_timeout, self.backend.stop()),
            async {
                if let Some(filter) = &self.filter {
                    bounded(self.config.operation_timeout, filter.stop()).await
                } else {
                    Ok(())
                }
            }
        );
        video_result.and(backend_result).and(filter_result)
    }
}
async fn input_worker(
    queue: Arc<MediaQueue>,
    video: Arc<VideoInput>,
    filter: Option<Arc<dyn AudioFilter>>,
    format: PcmFormat,
    config: InputTransportConfig,
    ctx: ProcessorContext,
) -> Result<(), ProcessorError> {
    let mut stream: Option<(SincResampler, Frame)> = None;
    loop {
        let available = queue.available.notified();
        if let Err(e) = video.complete(config.operation_timeout).await {
            ctx.push_error(ErrorFrame::new(e.message)).await?;
        }
        if queue.signal().cancelled {
            return Ok(());
        }
        let Some(item) = queue.pop() else {
            if queue.signal().ending {
                if let Err(e) = flush_input(&mut stream, &filter, format, &config, &ctx).await {
                    ctx.push_error(ErrorFrame::new(e.message)).await?;
                }
                return Ok(());
            }
            available.await;
            continue;
        };
        let mut frame = item.frame;
        let image_request_id = video.request_for_image(&frame);
        let result = async {
            if let FrameData::InputAudio(input) = &frame.data {
                if input.num_channels != format.channels() {
                    return Err(ProcessorError::new(
                        "input channel conversion requires an explicit converter",
                    ));
                }
                let reset = stream.as_ref().is_some_and(|(resampler, previous)| {
                    resampler.input_format().sample_rate() != input.sample_rate
                        || previous.transport_source != frame.transport_source
                });
                if reset {
                    flush_input(&mut stream, &filter, format, &config, &ctx).await?;
                }
                if stream.is_none() {
                    stream = Some((
                        SincResampler::new(
                            input.sample_rate,
                            format.sample_rate(),
                            format.channels(),
                        )
                        .map_err(error)?,
                        frame.clone(),
                    ));
                }
                let (resampler, template) = stream.as_mut().unwrap();
                let data = resampler.process(&input.audio).map_err(error)?;
                *template = frame.clone();
                if let FrameData::InputAudio(input) = &mut frame.data {
                    input.audio = data;
                    input.sample_rate = format.sample_rate();
                }
                return filter_input(frame, &filter, format, &config, &ctx).await;
            }
            flush_input(&mut stream, &filter, format, &config, &ctx).await?;
            if matches!(
                frame.data,
                FrameData::FilterEnable(_)
                    | FrameData::FilterControl
                    | FrameData::FilterUpdateSettings(_)
            ) {
                if let Some(filter) = &filter {
                    bounded(config.operation_timeout, filter.control(&frame.data)).await?;
                }
            }
            video.forward(frame, &ctx).await
        }
        .await;
        if let Err(e) = result {
            video.fail_image(image_request_id);
            ctx.push_error(ErrorFrame::new(e.message)).await?;
        }
    }
}
async fn flush_input(
    stream: &mut Option<(SincResampler, Frame)>,
    filter: &Option<Arc<dyn AudioFilter>>,
    format: PcmFormat,
    config: &InputTransportConfig,
    ctx: &ProcessorContext,
) -> Result<(), ProcessorError> {
    let Some((mut resampler, template)) = stream.take() else {
        return Ok(());
    };
    let mut frame = template.duplicate();
    if let FrameData::InputAudio(input) = &mut frame.data {
        input.audio = resampler.flush().map_err(error)?;
        input.sample_rate = format.sample_rate();
    }
    filter_input(frame, filter, format, config, ctx).await
}
async fn filter_input(
    mut frame: Frame,
    filter: &Option<Arc<dyn AudioFilter>>,
    format: PcmFormat,
    config: &InputTransportConfig,
    ctx: &ProcessorContext,
) -> Result<(), ProcessorError> {
    if let FrameData::InputAudio(input) = &mut frame.data {
        if input.audio.is_empty() {
            return Ok(());
        }
        if let Some(filter) = filter {
            input.audio = bounded(
                config.operation_timeout,
                filter.filter(std::mem::take(&mut input.audio)),
            )
            .await?;
        }
        if input.audio.len() > config.queue_byte_capacity {
            return Err(ProcessorError::new(
                "filtered input exceeds the media byte budget",
            ));
        }
        format.validate(&input.audio).map_err(error)?;
        if input.audio.is_empty() || !config.passthrough {
            return Ok(());
        }
    }
    ctx.push(frame, Direction::Downstream).await
}
