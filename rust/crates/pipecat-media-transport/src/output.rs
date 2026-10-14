#[path = "output_cycle.rs"]
mod cycle;
#[path = "output_video.rs"]
mod video;
pub use cycle::CyclicVideoOutputConfig;

use crate::queue::{Item, MediaQueue};
use crate::{error, AudioFilter, AudioMixer, SpokenHistory};
use async_trait::async_trait;
use pipecat_audio::{is_silence, load_dtmf_audio, scale_audio, PcmFormat, SincResampler};
use pipecat_core::*;
use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

/// Acknowledgement of completed playback, not network/device queue acceptance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlaybackReceipt {
    /// The PCM has reached the backend's playback boundary.
    Played,
    /// The backend intentionally discarded it; no speaking or audio event is emitted.
    Discarded,
}

/// Implementations must be cancellation safe: dropping `play_audio` prevents
/// its unacknowledged audio from later playing, or `interrupt` must flush it.
/// Buffered devices must await their playback acknowledgement before returning
/// `Played`. Network-send completion alone is not evidence of playback.
#[async_trait]
pub trait OutputBackend: Send + Sync {
    async fn start(
        &self,
        format: PcmFormat,
        destinations: &[Option<String>],
    ) -> Result<(), ProcessorError>;
    async fn play_audio(&self, frame: &Frame) -> Result<PlaybackReceipt, ProcessorError>;
    async fn interrupt(&self, destination: Option<&str>) -> Result<(), ProcessorError>;
    async fn stop(&self) -> Result<(), ProcessorError>;
    /// Return false when native keypad signalling is unavailable; the runtime
    /// then plays the bundled PCM tones, bypassing speech filters and gain.
    async fn send_dtmf(&self, _frame: &Frame) -> Result<bool, ProcessorError> {
        Ok(false)
    }
    /// Send one transport control message. Downstream urgent messages may call
    /// this concurrently with media operations on the same backend. Completion
    /// attests this hook only, not remote receipt. A timeout or shutdown can drop
    /// the future but cannot undo bytes already sent; do not detach work or rely
    /// on retries. The shared output never retries an ambiguous message send.
    async fn send_message(&self, _frame: &Frame) -> Result<(), ProcessorError> {
        Err(ProcessorError::new(
            "backend does not support transport messages",
        ))
    }
    /// Explicit opt-in contract for independent live-image output. Returning
    /// true promises that one image write per destination may overlap audio,
    /// DTMF and ordinary controls on this backend. Dropping `write_image` must
    /// cancel all unfinished image work, without detached writes or replay.
    /// `interrupt` may overlap images and must not invalidate unrelated,
    /// new-generation or uninterruptible image work. Backends with a global
    /// video flush must provide their own safe coordination before opting in.
    /// Successful image completion attests this hook, not physical display.
    fn supports_independent_video(&self) -> bool {
        false
    }
    async fn write_image(&self, _frame: &Frame) -> Result<(), ProcessorError> {
        Err(ProcessorError::new("backend does not support image output"))
    }
}

/// Admission behavior when a destination's bounded normal media queue is full.
/// Urgent/system frames always use fail-fast admission. Normal all-destination
/// flush fences wait for capacity atomically under the same policy as media.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputAdmissionPolicy {
    /// Retain the current normal-lane frame until capacity becomes available.
    /// The timeout includes pauses and future-PTS waits; it does not restart on
    /// notifications. End/Stop remain ordered behind this admission attempt.
    Wait { timeout: Duration },
    /// Reject immediately when either the count or byte budget is exhausted.
    FailFast,
}
impl Default for OutputAdmissionPolicy {
    fn default() -> Self {
        Self::Wait {
            timeout: Duration::from_secs(10),
        }
    }
}

#[derive(Debug, Clone)]
pub struct OutputTransportConfig {
    pub sample_rate: Option<u32>,
    pub channels: u16,
    pub chunk_duration: Duration,
    pub queue_capacity: usize,
    pub queue_byte_capacity: usize,
    /// Backpressure for normal frames, including timed metadata. At most the
    /// current normal-lane frame waits outside the queue; an individual frame
    /// larger than its byte budget is rejected immediately under either policy.
    pub admission_policy: OutputAdmissionPolicy,
    pub destinations: Vec<String>,
    pub volume: f32,
    pub write_timeout: Duration,
    /// Maximum admitted future delay for explicitly pipeline-clocked frames.
    pub max_future_pts: Duration,
    pub end_silence: Duration,
    /// Generate silence/background audio during idle periods, paced to chunk duration.
    pub auto_silence: bool,
    pub speech_idle_timeout: Duration,
}
impl Default for OutputTransportConfig {
    fn default() -> Self {
        Self {
            sample_rate: None,
            channels: 1,
            chunk_duration: Duration::from_millis(40),
            queue_capacity: 128,
            queue_byte_capacity: 2 * 1024 * 1024,
            admission_policy: OutputAdmissionPolicy::default(),
            destinations: vec![],
            volume: 1.0,
            write_timeout: Duration::from_secs(10),
            max_future_pts: Duration::from_secs(60),
            end_silence: Duration::from_secs(2),
            auto_silence: true,
            speech_idle_timeout: Duration::from_secs(1),
        }
    }
}
/// Explicit additive queue budget for independently scheduled false images.
/// There is no default: applications must choose both retained queue bounds.
#[derive(Debug, Clone, Copy)]
pub struct LiveVideoOutputConfig {
    pub queue_capacity: usize,
    pub queue_byte_capacity: usize,
    /// None writes each eligible image immediately. Some(1..=1_000_000_000)
    /// caps live callback starts; synchronous images are not rate-limited.
    pub framerate: Option<u32>,
}

struct Destination {
    name: Option<String>,
    queue: Arc<MediaQueue>,
    usable: AtomicBool,
    started_at: Mutex<Option<Instant>>,
    mixer: Option<Arc<dyn AudioMixer>>,
    // This narrow lock also serializes publication with old control clones.
    // Never hold it over an await or while completing a fence.
    live: Mutex<Option<Arc<video::LiveVideo>>>,
    cyclic: Mutex<Option<Arc<cycle::CyclicVideo>>>,
}
impl Destination {
    fn live(&self) -> Option<Arc<video::LiveVideo>> {
        self.live.lock().unwrap().clone()
    }
    fn cyclic(&self) -> Option<Arc<cycle::CyclicVideo>> {
        self.cyclic.lock().unwrap().clone()
    }
    fn independent_video(&self) -> bool {
        self.live().is_some() || self.cyclic().is_some()
    }
    fn pause(&self, paused: bool) {
        let live = self.live.lock().unwrap();
        let cyclic = self.cyclic.lock().unwrap();
        // Once opted-in drain closes admission, late controls cannot close its
        // runnable gate again. Preserve the legacy constructor's old behavior.
        let paused =
            paused && !((live.is_some() || cyclic.is_some()) && self.queue.signal().ending);
        self.queue.pause(paused);
        if let Some(live) = live.as_ref() {
            live.queue.pause(paused);
        }
        if let Some(cyclic) = cyclic.as_ref() {
            cyclic.queue.pause(paused);
        }
    }
    fn interrupt(&self) {
        let live = self.live.lock().unwrap();
        let cyclic = self.cyclic.lock().unwrap();
        self.queue.interrupt();
        if let Some(live) = live.as_ref() {
            live.queue.interrupt();
        }
        if let Some(cyclic) = cyclic.as_ref() {
            cyclic.interrupt();
        }
    }
    fn end(&self) {
        let live = self.live.lock().unwrap();
        let cyclic = self.cyclic.lock().unwrap();
        self.queue.end();
        if let Some(live) = live.as_ref() {
            live.queue.end();
        }
        if let Some(cyclic) = cyclic.as_ref() {
            cyclic.end();
        }
    }
    fn cancel(&self) {
        let live = self.live.lock().unwrap();
        let cyclic = self.cyclic.lock().unwrap();
        self.queue.cancel();
        if let Some(live) = live.as_ref() {
            live.queue.cancel();
        }
        if let Some(cyclic) = cyclic.as_ref() {
            cyclic.cancel();
        }
    }
}
#[derive(Clone)]
pub struct OutputControl {
    destinations: Arc<BTreeMap<Option<String>, Arc<Destination>>>,
    ready: Arc<AtomicBool>,
}
impl OutputControl {
    pub fn is_ready(&self) -> bool {
        self.ready.load(Ordering::SeqCst)
    }
    pub fn is_usable(&self, destination: Option<&str>) -> Result<bool, ProcessorError> {
        Ok(self.destination(destination)?.usable.load(Ordering::SeqCst))
    }
    /// Pause between writes, retaining queued PCM and resampler history.
    pub fn pause(&self, destination: Option<&str>) -> Result<(), ProcessorError> {
        self.destination(destination)?.pause(true);
        Ok(())
    }
    pub fn resume(&self, destination: Option<&str>) -> Result<(), ProcessorError> {
        self.destination(destination)?.pause(false);
        Ok(())
    }
    fn destination(&self, destination: Option<&str>) -> Result<&Arc<Destination>, ProcessorError> {
        self.destinations
            .get(&destination.map(str::to_owned))
            .ok_or_else(|| ProcessorError::new("unknown output destination"))
    }
}

pub struct OutputTransport {
    name: String,
    config: OutputTransportConfig,
    backend: Arc<dyn OutputBackend>,
    filter: Option<Arc<dyn AudioFilter>>,
    control: OutputControl,
    format: Mutex<Option<PcmFormat>>,
    // Setup's generation-free destination context is only the receiver of a
    // cyclic acknowledgement; each queued activation still carries its exact
    // original invocation fence. Cleanup releases this strong runtime owner.
    cyclic_ack_context: Mutex<Option<ProcessorContext>>,
    cyclic_setup_started: AtomicBool,
    started: AtomicBool,
    workers_started: AtomicBool,
    cleaned: AtomicBool,
    urgent_cancelled: tokio::sync::watch::Sender<bool>,
    spoken_history: Option<SpokenHistory>,
}
impl OutputTransport {
    pub fn new(
        name: impl Into<String>,
        config: OutputTransportConfig,
        backend: Arc<dyn OutputBackend>,
        filter: Option<Arc<dyn AudioFilter>>,
        mixers: BTreeMap<Option<String>, Arc<dyn AudioMixer>>,
    ) -> Result<Self, ProcessorError> {
        if config.queue_capacity == 0
            || config.queue_byte_capacity == 0
            || config.channels == 0
            || config.chunk_duration.is_zero()
            || config.write_timeout.is_zero()
            || config.max_future_pts.is_zero()
            || !config.volume.is_finite()
            || config.speech_idle_timeout.is_zero()
            || matches!(config.admission_policy, OutputAdmissionPolicy::Wait { timeout } if timeout.is_zero())
        {
            return Err(ProcessorError::new(
                "invalid output transport configuration",
            ));
        }
        let mut destinations = BTreeMap::new();
        for name in std::iter::once(None).chain(config.destinations.iter().cloned().map(Some)) {
            if destinations.contains_key(&name) {
                return Err(ProcessorError::new("duplicate output destination"));
            }
            destinations.insert(
                name.clone(),
                Arc::new(Destination {
                    name: name.clone(),
                    queue: MediaQueue::new(config.queue_capacity, config.queue_byte_capacity),
                    usable: AtomicBool::new(true),
                    started_at: Mutex::new(None),
                    mixer: mixers.get(&name).cloned(),
                    live: Mutex::new(None),
                    cyclic: Mutex::new(None),
                }),
            );
        }
        if mixers.keys().any(|name| !destinations.contains_key(name)) {
            return Err(ProcessorError::new(
                "mixer configured for unknown destination",
            ));
        }
        Ok(Self {
            name: name.into(),
            config,
            backend,
            filter,
            control: OutputControl {
                destinations: Arc::new(destinations),
                ready: Arc::new(AtomicBool::new(false)),
            },
            format: Mutex::new(None),
            cyclic_ack_context: Mutex::new(None),
            cyclic_setup_started: AtomicBool::new(false),
            started: AtomicBool::new(false),
            workers_started: AtomicBool::new(false),
            cleaned: AtomicBool::new(false),
            urgent_cancelled: tokio::sync::watch::channel(false).0,
            spoken_history: None,
        })
    }
    /// Add one bounded, pipeline-owned live-image worker per destination.
    /// False images may overtake media; true images retain audio ordering.
    /// Existing control handles remain attached. No I/O occurs in this builder.
    pub fn with_live_video(self, config: LiveVideoOutputConfig) -> Result<Self, ProcessorError> {
        if self.started.load(Ordering::SeqCst)
            || self.cleaned.load(Ordering::SeqCst)
            || self
                .control
                .destinations
                .values()
                .any(|d| d.independent_video())
        {
            return Err(ProcessorError::new(
                "live video must be configured once before output starts",
            ));
        }
        if config.queue_capacity == 0
            || config.queue_byte_capacity == 0
            || config
                .framerate
                .is_some_and(|fps| fps == 0 || fps > 1_000_000_000)
        {
            return Err(ProcessorError::new("invalid live video configuration"));
        }
        let destinations = self.control.destinations.len();
        // Queued media+video, one current item per worker and one pending
        // normal admission. These are logical bounds, not total heap/RSS.
        let counts = self
            .config
            .queue_capacity
            .checked_add(config.queue_capacity)
            .and_then(|n| n.checked_add(2))
            .and_then(|n| n.checked_mul(destinations))
            .and_then(|n| n.checked_add(1));
        let bytes = self
            .config
            .queue_byte_capacity
            .checked_add(config.queue_byte_capacity)
            .and_then(|n| n.checked_mul(destinations));
        if counts.is_none() || bytes.is_none() {
            return Err(ProcessorError::new(
                "aggregate live video queue budget overflow",
            ));
        }
        if !self.backend.supports_independent_video() {
            return Err(ProcessorError::new(
                "backend does not support independent video",
            ));
        }
        for destination in self.control.destinations.values() {
            let mut slot = destination.live.lock().unwrap();
            let live = Arc::new(video::LiveVideo::new(config));
            live.queue.pause(destination.queue.signal().paused);
            *slot = Some(live);
        }
        Ok(self)
    }
    /// Opt in to bounded non-live raw-image/sprite cycling. Mutually exclusive
    /// with live video. Repeated backend writes do not repeat downstream frames.
    /// See CYCLIC_VIDEO.md for the intentionally restricted payload/lifecycle profile.
    pub fn with_cyclic_video(
        self,
        config: CyclicVideoOutputConfig,
    ) -> Result<Self, ProcessorError> {
        if self.started.load(Ordering::SeqCst)
            || self.cleaned.load(Ordering::SeqCst)
            || self
                .control
                .destinations
                .values()
                .any(|d| d.independent_video())
        {
            return Err(ProcessorError::new(
                "cyclic video must be configured once before output starts",
            ));
        }
        cycle::validate_config(config)?;
        let destinations = self.control.destinations.len();
        self.config
            .queue_capacity
            .checked_add(config.queue_capacity)
            .and_then(|n| n.checked_add(config.retained_image_capacity))
            .and_then(|n| n.checked_mul(destinations))
            .and_then(|n| n.checked_add(2))
            .ok_or_else(|| ProcessorError::new("aggregate cyclic image count overflow"))?;
        self.config
            .queue_byte_capacity
            .checked_add(config.queue_byte_capacity)
            .and_then(|n| n.checked_add(config.retained_byte_capacity))
            .and_then(|n| n.checked_mul(destinations))
            .ok_or_else(|| ProcessorError::new("aggregate cyclic image byte budget overflow"))?;
        if !self.backend.supports_independent_video() {
            return Err(ProcessorError::new(
                "backend does not support independent video",
            ));
        }
        for destination in self.control.destinations.values() {
            let _live = destination.live.lock().unwrap();
            let mut slot = destination.cyclic.lock().unwrap();
            let cyclic = Arc::new(cycle::CyclicVideo::new(config));
            cyclic.queue.pause(destination.queue.signal().paused);
            *slot = Some(cyclic);
        }
        Ok(self)
    }
    /// Opt in to whole-context spoken history through an atomic application sink.
    /// Only the default destination and the built-in resampler/chunker/gain are
    /// supported. Arbitrary filters/mixers cannot attest preservation of speech.
    /// Keep a clone of the handle to invalidate before input-side interruption.
    pub fn with_spoken_history(mut self, history: SpokenHistory) -> Result<Self, ProcessorError> {
        if self.spoken_history.is_some() || self.started.load(Ordering::SeqCst) {
            return Err(ProcessorError::new(
                "spoken history must be bound once before output starts",
            ));
        }
        if !self.config.destinations.is_empty()
            || self.filter.is_some()
            || self
                .control
                .destinations
                .values()
                .any(|destination| destination.mixer.is_some())
            || self.config.volume == 0.0
        {
            return Err(ProcessorError::new(
                "spoken history requires one destination, no custom filter/mixer, and nonzero gain",
            ));
        }
        history.bind()?;
        self.spoken_history = Some(history);
        Ok(self)
    }
    pub fn control(&self) -> OutputControl {
        self.control.clone()
    }
}
#[async_trait]
impl FrameProcessor for OutputTransport {
    fn name(&self) -> &str {
        &self.name
    }
    async fn setup(
        &self,
        start: &StartFrame,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let cyclic = self
            .control
            .destinations
            .values()
            .any(|d| d.cyclic().is_some());
        if cyclic
            && (self.cleaned.load(Ordering::SeqCst)
                || self.cyclic_setup_started.swap(true, Ordering::SeqCst))
        {
            return Err(ProcessorError::new(
                "cyclic output is single-use and cannot be set up twice",
            ));
        }
        let format = PcmFormat::new(
            self.config
                .sample_rate
                .unwrap_or(start.audio_out_sample_rate),
            self.config.channels,
        )
        .map_err(error)?;
        let samples = (u128::from(format.sample_rate()) * self.config.chunk_duration.as_nanos())
            / 1_000_000_000;
        if samples == 0
            || samples > self.config.queue_byte_capacity as u128 / format.frame_bytes() as u128
        {
            return Err(ProcessorError::new(
                "output chunk must fit the byte budget and contain at least one PCM frame",
            ));
        }
        *self.format.lock().unwrap() = Some(format);
        if let Some(filter) = &self.filter {
            bounded(self.config.write_timeout, filter.start(format)).await?;
        }
        for destination in self.control.destinations.values() {
            if let Some(mixer) = &destination.mixer {
                bounded(self.config.write_timeout, mixer.start(format)).await?;
            }
        }
        bounded(
            self.config.write_timeout,
            self.backend.start(
                format,
                &self
                    .control
                    .destinations
                    .keys()
                    .cloned()
                    .collect::<Vec<_>>(),
            ),
        )
        .await?;
        if cyclic {
            // Do not retain the runtime during fallible/cancellable setup. The
            // receiver is published only after the final successful backend await.
            let mut receiver = self.cyclic_ack_context.lock().unwrap();
            if self.cleaned.load(Ordering::SeqCst) {
                return Err(ProcessorError::new("cyclic output closed during setup"));
            }
            *receiver = Some(ctx.clone());
        }
        self.started.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if let Some(history) = &self.spoken_history {
            history.admit(&mut frame, direction, ctx)?;
            // Only the atomic sink can attest this output's speech. Upstream
            // metadata must not independently commit unverified text.
            if matches!(frame.data, FrameData::TtsText(_)) {
                frame.metadata.remove("spoken");
            }
        }
        match &frame.data {
            data if data.is_start() => {
                if !self.started.load(Ordering::SeqCst) {
                    return Err(ProcessorError::fatal(
                        "output backend did not become ready during setup",
                    ));
                }
                if self
                    .control
                    .destinations
                    .values()
                    .any(|d| d.independent_video())
                    && (self.cleaned.load(Ordering::SeqCst)
                        || self.workers_started.swap(true, Ordering::SeqCst))
                {
                    return Err(ProcessorError::new(
                        "output workers already started or closed",
                    ));
                }
                ctx.push(frame, direction).await?;
                let format = self.format.lock().unwrap().expect("setup precedes start");
                for destination in self.control.destinations.values() {
                    let mut worker = Sender::new(
                        destination.clone(),
                        self.backend.clone(),
                        if destination.name.is_none() {
                            self.filter.clone()
                        } else {
                            None
                        },
                        self.config.clone(),
                        format,
                        ctx.clone(),
                        self.spoken_history.clone(),
                    );
                    // Sender establishes the one destination clock before
                    // either owned task starts or readiness is published.
                    let video_worker = destination.live().map(|live| {
                        video::VideoSender::new(
                            destination.clone(),
                            live,
                            self.backend.clone(),
                            self.config.clone(),
                            ctx.clone(),
                            (worker.started_at, worker.clock_origin_ns),
                            self.spoken_history.clone(),
                        )
                    });
                    let cycle_worker = if let Some(cyclic) = destination.cyclic() {
                        let receiver =
                            self.cyclic_ack_context
                                .lock()
                                .unwrap()
                                .clone()
                                .ok_or_else(|| {
                                    ProcessorError::new("cyclic output setup receiver is missing")
                                })?;
                        worker.cyclic_ack_context = Some(receiver.clone());
                        Some(cycle::CycleSender::new(
                            destination.clone(),
                            cyclic,
                            self.backend.clone(),
                            self.config.clone(),
                            receiver,
                        ))
                    } else {
                        None
                    };
                    ctx.spawn(
                        format!("{}-media-{:?}", self.name, destination.name),
                        false,
                        worker.run(),
                    )?;
                    if let Some(worker) = video_worker {
                        ctx.spawn(
                            format!("{}-video-{:?}", self.name, destination.name),
                            false,
                            worker.run(),
                        )?;
                    }
                    if let Some(worker) = cycle_worker {
                        ctx.spawn(
                            format!("{}-cycle-{:?}", self.name, destination.name),
                            false,
                            worker.run(),
                        )?;
                    }
                }
                self.control.ready.store(true, Ordering::SeqCst);
                ctx.push(
                    Frame::new(FrameData::OutputTransportReady),
                    Direction::Upstream,
                )
                .await
            }
            data if flow_flush_returning(data).is_some() => {
                if flow_flush_returning(data) == Some(true) || direction == Direction::Upstream {
                    return ctx.push(frame, direction).await;
                }
                if !self.control.is_ready() {
                    return Err(ProcessorError::fatal(
                        "output transport is not ready for a media flush fence",
                    ));
                }
                let queues = self
                    .control
                    .destinations
                    .values()
                    .flat_map(|destination| {
                        std::iter::once(destination.queue.clone())
                            .chain(destination.live().map(|live| live.queue.clone()))
                            .chain(destination.cyclic().map(|cyclic| cyclic.queue.clone()))
                    })
                    .collect::<Vec<_>>();
                match self.config.admission_policy {
                    OutputAdmissionPolicy::Wait { timeout } if !frame.is_system() => {
                        MediaQueue::fence_wait(&queues, frame, timeout).await
                    }
                    _ => MediaQueue::fence(&queues, frame),
                }
            }
            FrameData::Interruption => {
                if frame.transport_destination.is_some() {
                    self.control
                        .destination(frame.transport_destination.as_deref())?
                        .interrupt();
                } else {
                    for destination in self.control.destinations.values() {
                        destination.interrupt();
                    }
                }
                ctx.push(frame, direction).await
            }
            FrameData::Cancel(_) | FrameData::CancelDetailed(_) => {
                self.urgent_cancelled.send_replace(true);
                for destination in self.control.destinations.values() {
                    destination.cancel();
                }
                self.control.ready.store(false, Ordering::SeqCst);
                ctx.push(frame, direction).await
            }
            FrameData::ProcessorPauseUrgent { processor } if processor == &self.name => {
                for destination in self.control.destinations.values() {
                    destination.pause(true);
                }
                ctx.push(frame, direction).await
            }
            FrameData::ProcessorPauseUrgentTarget(processor) if processor.name() == self.name => {
                for destination in self.control.destinations.values() {
                    destination.pause(true);
                }
                ctx.push(frame, direction).await
            }
            FrameData::ProcessorResumeTarget(processor) if processor.name() == self.name => {
                for destination in self.control.destinations.values() {
                    destination.pause(false);
                }
                ctx.push(frame, direction).await
            }
            FrameData::ProcessorResume { processor }
            | FrameData::ProcessorResumeUrgent { processor }
                if processor == &self.name =>
            {
                for destination in self.control.destinations.values() {
                    destination.pause(false);
                }
                ctx.push(frame, direction).await
            }
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop => {
                self.control.ready.store(false, Ordering::SeqCst);
                ctx.push(frame, direction).await
            }
            FrameData::OutputDtmfUrgent(_) | FrameData::OutputDtmfUrgentSequence(_) => self
                .control
                .destination(frame.transport_destination.as_deref())?
                .queue
                .push_front(frame),
            FrameData::OutputTransportMessageUrgent(_) if direction == Direction::Downstream => {
                if !self.control.is_ready() || self.cleaned.load(Ordering::SeqCst) {
                    return Err(ProcessorError::new("output transport is not ready"));
                }
                let destination = self
                    .control
                    .destination(frame.transport_destination.as_deref())?;
                // This snapshot shares the queue's lock with end/cancel. An
                // admitted control write belongs to this System invocation;
                // prepare_end closes later admission, without discarding it.
                let signal = destination.queue.signal();
                if signal.ending || signal.cancelled {
                    return Err(ProcessorError::new("media queue is closed"));
                }
                // Protocol replies bypass media pause/backlog/PTS and playback
                // fences. They are System traffic: an audio interruption must
                // not drop them merely because the Frame defaults interruptible.
                // A media Sender's graceful Drop also cancels its queue. That
                // is not cancellation of this already-admitted protocol write:
                // only the output's actual Cancel/cleanup signal owns it.
                let mut cancelled = self.urgent_cancelled.subscribe();
                let sending = bounded(self.config.write_timeout, self.backend.send_message(&frame));
                tokio::pin!(sending);
                loop {
                    if *cancelled.borrow_and_update() {
                        return Ok(());
                    }
                    tokio::select! {
                        biased;
                        changed = cancelled.changed() => {
                            if changed.is_err() {
                                return Ok(());
                            }
                        },
                        result = &mut sending => return result,
                    }
                }
            }
            _ if direction == Direction::Upstream || frame.is_system() => {
                ctx.push(frame, direction).await
            }
            _ => {
                if !self.control.is_ready() {
                    return Err(ProcessorError::new("output transport is not ready"));
                }
                let result = async {
                    let destination = self
                        .control
                        .destination(frame.transport_destination.as_deref())?;
                    let deadline = if frame.has_pipeline_pts() {
                        let pts = frame
                            .pts
                            .ok_or_else(|| ProcessorError::new("pipeline-clock frame lacks PTS"))?;
                        let delay =
                            Duration::from_nanos(pts.saturating_sub(ctx.observer_clock().now_ns()));
                        if delay > self.config.max_future_pts {
                            return Err(ProcessorError::new(
                                "pipeline PTS exceeds output future horizon",
                            ));
                        }
                        let deadline = Instant::now()
                            .checked_add(delay)
                            .ok_or_else(|| ProcessorError::new("output PTS deadline overflow"))?;
                        Some(deadline)
                    } else {
                        if let Some(pts) = frame.pts {
                            let origin =
                                destination.started_at.lock().unwrap().ok_or_else(|| {
                                    ProcessorError::new("output clock not started")
                                })?;
                            let deadline = origin
                                .checked_add(Duration::from_nanos(pts))
                                .ok_or_else(|| {
                                    ProcessorError::new("legacy output PTS deadline overflow")
                                })?;
                            if deadline.saturating_duration_since(Instant::now())
                                > self.config.max_future_pts
                            {
                                return Err(ProcessorError::new(
                                    "legacy PTS exceeds output future horizon",
                                ));
                            }
                        }
                        None
                    };
                    let cyclic = destination.cyclic();
                    let invocation = if let Some(cyclic) = cyclic.as_ref().filter(|_| {
                        matches!(
                            frame.data,
                            FrameData::OutputImage(_)
                                | FrameData::OutputImageDetailed(_)
                                | FrameData::Sprite(_)
                                | FrameData::UrlImage(_)
                                | FrameData::AssistantImage(_)
                        )
                    }) {
                        cyclic.validate(&frame)?;
                        Some(ctx.invocation_fence()?)
                    } else {
                        None
                    };
                    if let Some(invocation) = invocation {
                        let queue = if cycle::is_independent(&frame) {
                            cyclic
                                .as_ref()
                                .expect("validated cyclic mode")
                                .queue
                                .clone()
                        } else {
                            destination.queue.clone()
                        };
                        return match self.config.admission_policy {
                            OutputAdmissionPolicy::Wait { timeout } => {
                                queue
                                    .push_wait_with_invocation(frame, deadline, timeout, invocation)
                                    .await
                            }
                            OutputAdmissionPolicy::FailFast => {
                                queue.push_with_invocation(frame, deadline, invocation)
                            }
                        };
                    }
                    let queue = if video::is_live_image(&frame) {
                        destination.live().map(|live| live.queue.clone())
                    } else {
                        None
                    }
                    .unwrap_or_else(|| destination.queue.clone());
                    match self.config.admission_policy {
                        OutputAdmissionPolicy::Wait { timeout } => {
                            queue.push_wait(frame, deadline, timeout).await
                        }
                        OutputAdmissionPolicy::FailFast => match deadline {
                            Some(deadline) => queue.push_timed(frame, deadline),
                            None => queue.push(frame),
                        },
                    }
                }
                .await;
                if result.is_err() {
                    if let Some(history) = &self.spoken_history {
                        history.invalidate_reason("media queue admission failed");
                    }
                }
                result
            }
        }
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        for destination in self.control.destinations.values() {
            destination.end();
        }
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        if self.cleaned.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let receiver = self.cyclic_ack_context.lock().unwrap().take();
        drop(receiver);
        self.urgent_cancelled.send_replace(true);
        self.control.ready.store(false, Ordering::SeqCst);
        if let Some(history) = &self.spoken_history {
            history.invalidate_reason("output closed with incomplete contexts");
        }
        let mut failure = None;
        for destination in self.control.destinations.values() {
            destination.cancel();
            if self.started.load(Ordering::SeqCst) {
                if let Err(e) = bounded(
                    self.config.write_timeout,
                    self.backend.interrupt(destination.name.as_deref()),
                )
                .await
                {
                    failure.get_or_insert(e);
                }
            }
            if let Some(mixer) = &destination.mixer {
                if let Err(e) = bounded(self.config.write_timeout, mixer.stop()).await {
                    failure.get_or_insert(e);
                }
            }
        }
        if let Some(filter) = &self.filter {
            if let Err(e) = bounded(self.config.write_timeout, filter.stop()).await {
                failure.get_or_insert(e);
            }
        }
        if let Err(e) = bounded(self.config.write_timeout, self.backend.stop()).await {
            failure.get_or_insert(e);
        }
        failure.map_or(Ok(()), Err)
    }
}

struct AudioRun {
    frame: Frame,
    audio: VecDeque<u8>,
    epoch: u64,
}
struct Sender {
    destination: Arc<Destination>,
    backend: Arc<dyn OutputBackend>,
    filter: Option<Arc<dyn AudioFilter>>,
    config: OutputTransportConfig,
    format: PcmFormat,
    ctx: ProcessorContext,
    cyclic_ack_context: Option<ProcessorContext>,
    runs: VecDeque<AudioRun>,
    resampler: Option<SincResampler>,
    source: Option<(Frame, u64)>,
    epoch: u64,
    chunk_bytes: usize,
    volume: f32,
    gain: f32,
    speaking: bool,
    // TTS ownership lasts until this destination stops speaking. Only a
    // successfully Played TTS chunk can acquire it; avatar speech cannot.
    tts_audio_played: bool,
    last_speech: Option<Instant>,
    last_speaking_event: Option<Instant>,
    started_at: Instant,
    clock_origin_ns: u64,
    incomplete_media: bool,
    readiness_failed: bool,
    spoken_history: Option<SpokenHistory>,
}
impl Drop for Sender {
    fn drop(&mut self) {
        // Closing an actor also drops queued rendezvous participants, so a
        // sibling destination cannot remain blocked by a failed worker.
        self.destination.queue.cancel();
    }
}
impl Sender {
    fn new(
        destination: Arc<Destination>,
        backend: Arc<dyn OutputBackend>,
        filter: Option<Arc<dyn AudioFilter>>,
        config: OutputTransportConfig,
        format: PcmFormat,
        ctx: ProcessorContext,
        spoken_history: Option<SpokenHistory>,
    ) -> Self {
        let chunk_bytes = ((u128::from(format.sample_rate()) * config.chunk_duration.as_nanos())
            / 1_000_000_000) as usize
            * format.frame_bytes();
        let clock_origin_ns = ctx.observer_clock().now_ns();
        let started_at = Instant::now();
        *destination.started_at.lock().unwrap() = Some(started_at);
        Self {
            destination,
            backend,
            filter,
            volume: config.volume,
            config,
            format,
            ctx,
            cyclic_ack_context: None,
            runs: VecDeque::new(),
            resampler: None,
            source: None,
            epoch: 0,
            chunk_bytes,
            gain: 1.0,
            speaking: false,
            tts_audio_played: false,
            last_speech: None,
            last_speaking_event: None,
            started_at,
            clock_origin_ns,
            incomplete_media: false,
            readiness_failed: false,
            spoken_history,
        }
    }
    async fn run(mut self) -> Result<(), ProcessorError> {
        let mut changed = self.destination.queue.changed.subscribe();
        let mut next_idle = Instant::now() + self.config.chunk_duration;
        loop {
            self.observe_interruption().await?;
            let signal = self.destination.queue.signal();
            if signal.cancelled {
                return Ok(());
            }
            if signal.paused {
                changed.changed().await.map_err(error)?;
                continue;
            }
            let item = if self.destination.independent_video() {
                self.destination.queue.pop_ready_independent(Instant::now())
            } else {
                self.destination.queue.pop_ready(Instant::now())
            };
            if let Some(mut item) = item {
                if item.frame.interruptible && item.epoch < self.epoch {
                    if let Some(fence) = &item.fence {
                        fence.revoke();
                    }
                    continue;
                }
                if let Some(mut fence) = item.fence.take() {
                    if self.destination.independent_video() {
                        let result = async {
                            self.flush_resampler()?;
                            self.flush_chunks(true).await?;
                            let signal = self.destination.queue.signal();
                            if signal.cancelled {
                                return Ok(crate::fence::FenceOutcome::Aborted);
                            }
                            if item.frame.interruptible && signal.epoch > item.epoch {
                                fence.revoke();
                                return Ok(crate::fence::FenceOutcome::Revoked);
                            }
                            if let Some(outcome) = fence.check_delivery(
                                self.incomplete_media || !self.destination.usable.load(Ordering::SeqCst),
                                "preceding media lacks a completed playback or discard acknowledgement",
                            )? {
                                return Ok(outcome);
                            }
                            video::complete_fence(&self.destination.queue, item.epoch,
                                item.frame.interruptible, &mut fence, &self.ctx).await
                        }.await;
                        match result {
                            Ok(
                                crate::fence::FenceOutcome::Completed
                                | crate::fence::FenceOutcome::Revoked,
                            ) => {}
                            Ok(crate::fence::FenceOutcome::Aborted) => return Ok(()),
                            Err(_) => {
                                self.report(fence.error(
                                    "could not complete preceding media on every output lane",
                                ))
                                .await?;
                                return Ok(());
                            }
                        }
                        next_idle = Instant::now() + self.config.chunk_duration;
                        continue;
                    }
                    let result = async {
                        self.flush_resampler()?;
                        self.flush_chunks(true).await?;
                        if self.incomplete_media || !self.destination.usable.load(Ordering::SeqCst) {
                            return Err(fence.error("preceding media lacks a completed playback or discard acknowledgement"));
                        }
                        fence.complete(&self.ctx).await
                    }.await;
                    match result {
                        Ok(true) => {}
                        // The failing participant already reports its error. A
                        // cancelled sibling must not convert Cancel into failure.
                        Ok(false) => return Ok(()),
                        Err(_) => {
                            self.report(fence.error(
                                "could not complete preceding media on every output destination",
                            ))
                            .await?;
                            return Ok(());
                        }
                    }
                } else if let Err(e) = self.handle(item).await {
                    self.incomplete_media = true;
                    if let Some(history) = &self.spoken_history {
                        history.invalidate_reason("media transformation or delivery failed");
                    }
                    self.report(e).await?;
                }
                next_idle = Instant::now() + self.config.chunk_duration;
                continue;
            }
            if signal.ending && self.destination.queue.is_empty() {
                if let Err(e) = self.flush_resampler() {
                    self.report(e).await?;
                }
                if let Err(e) = self.flush_chunks(true).await {
                    self.report(e).await?;
                }
                self.stop_speaking().await?;
                let frames = (u128::from(self.format.sample_rate())
                    * self.config.end_silence.as_nanos()
                    / 1_000_000_000) as usize;
                let mut remaining = frames.saturating_mul(self.format.frame_bytes());
                while remaining > 0 && self.destination.usable.load(Ordering::SeqCst) {
                    let size = remaining.min(self.chunk_bytes);
                    let frame = self.audio_frame(vec![0; size], false)?;
                    self.deliver(frame, self.epoch, true).await?;
                    remaining -= size;
                }
                return Ok(());
            }
            if self.speaking
                && self
                    .last_speech
                    .is_some_and(|last| last.elapsed() >= self.config.speech_idle_timeout)
            {
                self.stop_speaking().await?;
            }
            let available = self.destination.queue.available.notified();
            let timed_deadline = self.destination.queue.next_deadline();
            let timed_ready = async {
                if let Some(deadline) = timed_deadline {
                    tokio::time::sleep_until(deadline).await;
                } else {
                    std::future::pending::<()>().await;
                }
            };
            tokio::select! {
                _ = available => {},
                _ = timed_ready => {},
                _ = changed.changed() => {},
                _ = tokio::time::sleep_until(next_idle) => {
                    if self.config.auto_silence && self.destination.usable.load(Ordering::SeqCst) {
                        let mut frame = self.audio_frame(vec![0; self.chunk_bytes], false)?;
                        if let Some(mixer) = &self.destination.mixer {
                            if let Some(result) = operation(&self.destination.queue, self.epoch, true, self.config.write_timeout, mixer.mix(vec![0; self.chunk_bytes])).await {
                                match result {
                                    Ok(data) => {
                                        set_audio(&mut frame, data);
                                        if let Err(e) = self.deliver(frame, self.epoch, true).await { self.report(e).await?; }
                                    }
                                    Err(e) => self.report(e).await?,
                                }
                            }
                        } else {
                            self.deliver(frame, self.epoch, true).await?;
                        }
                    }
                    next_idle = Instant::now() + self.config.chunk_duration;
                }
            }
        }
    }
    async fn observe_interruption(&mut self) -> Result<(), ProcessorError> {
        let epoch = self.destination.queue.signal().epoch;
        if epoch == self.epoch {
            return Ok(());
        }
        self.epoch = epoch;
        if let Some(history) = &self.spoken_history {
            history.invalidate();
        }
        self.runs.retain(|run| !run.frame.interruptible);
        if self
            .source
            .as_ref()
            .is_some_and(|(frame, _)| frame.interruptible)
        {
            self.resampler = None;
            self.source = None;
        }
        if let Err(e) = bounded(
            self.config.write_timeout,
            self.backend.interrupt(self.destination.name.as_deref()),
        )
        .await
        {
            self.fail_peer(e).await?;
        }
        self.stop_speaking().await
    }
    async fn handle(&mut self, item: Item) -> Result<(), ProcessorError> {
        let Item {
            mut frame,
            epoch,
            invocation,
            ..
        } = item;
        // Pipeline-marked items have already passed the shared-clock deadline
        // gate. Wire consumers use this destination's elapsed-time convention.
        // A deadline before this destination existed is overdue, so rebase it
        // explicitly to zero rather than wrapping into the distant future.
        let pipeline_timed = frame.has_pipeline_pts();
        if pipeline_timed {
            match frame.rebase_pipeline_pts(self.clock_origin_ns) {
                Ok(()) => {}
                Err(FrameTimingError::BeforeOrigin) => frame.set_relative_pts(Some(0)),
                Err(error) => return Err(ProcessorError::new(error.to_string())),
            }
        }
        if let Some(history) = &self.spoken_history {
            history.processed(&frame);
        }
        match &frame.data {
            FrameData::OutputAudio(a)
            | FrameData::TtsAudio(a)
            | FrameData::SpeechOutputAudio(a) => {
                a.validate().map_err(error)?;
                if a.num_channels != self.format.channels() {
                    return Err(ProcessorError::new("output channel conversion requires an explicit backend or upstream converter"));
                }
                let boundary = self.source.as_ref().is_some_and(|(previous, _)| {
                    std::mem::discriminant(&previous.data) != std::mem::discriminant(&frame.data)
                        || audio(previous).unwrap().context_id != a.context_id
                        || previous.transport_source != frame.transport_source
                        || previous.metadata != frame.metadata
                });
                let changed = boundary
                    || self.source.as_ref().is_some_and(|(previous, _)| {
                        let p = audio(previous).unwrap();
                        p.sample_rate != a.sample_rate
                            || previous.interruptible != frame.interruptible
                            || std::mem::discriminant(&previous.data)
                                != std::mem::discriminant(&frame.data)
                            || p.context_id != a.context_id
                    });
                if changed {
                    self.flush_resampler()?;
                    if boundary {
                        self.flush_chunks(true).await?;
                    }
                }
                if frame.interruptible && self.destination.queue.signal().epoch > epoch {
                    return Ok(());
                }
                if self.resampler.is_none() || changed {
                    self.resampler = Some(
                        SincResampler::new(
                            a.sample_rate,
                            self.format.sample_rate(),
                            a.num_channels,
                        )
                        .map_err(error)?,
                    );
                }
                let data = self
                    .resampler
                    .as_mut()
                    .unwrap()
                    .process(&a.audio)
                    .map_err(error)?;
                self.source = Some((frame.clone(), epoch));
                self.buffer(frame, epoch, data);
                self.flush_chunks(false).await
            }
            FrameData::TtsStopped(_) => {
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                if self.tts_audio_played {
                    self.stop_speaking().await?;
                }
                self.forward(frame, epoch).await
            }
            FrameData::FilterEnable(_)
            | FrameData::FilterControl
            | FrameData::FilterUpdateSettings(_) => {
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                if let Some(filter) = &self.filter {
                    let Some(result) = operation(
                        &self.destination.queue,
                        epoch,
                        frame.interruptible,
                        self.config.write_timeout,
                        filter.control(&frame.data),
                    )
                    .await
                    else {
                        return Ok(());
                    };
                    result?;
                }
                self.forward(frame, epoch).await
            }
            FrameData::MixerEnable(_)
            | FrameData::MixerControl
            | FrameData::MixerUpdateSettings(_) => {
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                if let Some(mixer) = &self.destination.mixer {
                    let Some(result) = operation(
                        &self.destination.queue,
                        epoch,
                        frame.interruptible,
                        self.config.write_timeout,
                        mixer.control(&frame.data),
                    )
                    .await
                    else {
                        return Ok(());
                    };
                    result?;
                }
                Ok(())
            }
            FrameData::Volume(v) | FrameData::VolumeGain(v) => {
                if !v.is_finite() {
                    return Err(ProcessorError::new("volume must be finite"));
                }
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                if matches!(frame.data, FrameData::Volume(_)) {
                    self.volume = *v;
                } else {
                    self.gain = *v;
                }
                self.forward(frame, epoch).await
            }
            FrameData::ProcessorPause { processor } if processor == self.ctx.name() => {
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                self.destination.pause(true);
                self.forward(frame, epoch).await
            }
            FrameData::ProcessorPauseTarget(processor) if processor.name() == self.ctx.name() => {
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                self.destination.pause(true);
                self.forward(frame, epoch).await
            }
            _ => {
                self.flush_resampler()?;
                self.flush_chunks(true).await?;
                if let Some(pts) = frame.pts.filter(|_| !pipeline_timed) {
                    let deadline = self
                        .started_at
                        .checked_add(Duration::from_nanos(pts))
                        .ok_or_else(|| {
                            ProcessorError::new("legacy output PTS deadline overflow")
                        })?;
                    if deadline.saturating_duration_since(Instant::now())
                        > self.config.max_future_pts
                    {
                        return Err(ProcessorError::new(
                            "legacy PTS exceeds output future horizon",
                        ));
                    }
                    let Some(result) = operation(
                        &self.destination.queue,
                        epoch,
                        frame.interruptible,
                        deadline
                            .saturating_duration_since(Instant::now())
                            .saturating_add(self.config.write_timeout),
                        async {
                            tokio::time::sleep_until(deadline).await;
                            Ok(())
                        },
                    )
                    .await
                    else {
                        return Ok(());
                    };
                    result?;
                }
                match &frame.data {
                    FrameData::OutputTransportMessage(_)
                    | FrameData::OutputTransportMessageUrgent(_) => {
                        let Some(result) = operation(
                            &self.destination.queue,
                            epoch,
                            frame.interruptible,
                            self.config.write_timeout,
                            self.backend.send_message(&frame),
                        )
                        .await
                        else {
                            return Ok(());
                        };
                        result?;
                    }
                    FrameData::OutputImage(_)
                    | FrameData::OutputImageDetailed(_)
                    | FrameData::UrlImage(_)
                    | FrameData::AssistantImage(_) => {
                        if let Some(cyclic) = self.destination.cyclic() {
                            return cycle::activate(
                                &cyclic,
                                &self.destination.queue,
                                &self.backend,
                                self.cyclic_ack_context.as_ref().ok_or_else(|| {
                                    ProcessorError::new("cyclic output receiver is missing")
                                })?,
                                (
                                    frame,
                                    epoch,
                                    invocation.ok_or_else(|| {
                                        ProcessorError::new("cyclic image lost invocation scope")
                                    })?,
                                ),
                                self.config.write_timeout,
                            )
                            .await;
                        }
                        if let Some(live) = self.destination.live() {
                            return video::write_image(
                                &live,
                                &self.destination.queue,
                                &self.backend,
                                &self.ctx,
                                (frame, epoch),
                                self.config.write_timeout,
                                None,
                            )
                            .await;
                        }
                        let Some(result) = operation(
                            &self.destination.queue,
                            epoch,
                            frame.interruptible,
                            self.config.write_timeout,
                            self.backend.write_image(&frame),
                        )
                        .await
                        else {
                            return Ok(());
                        };
                        result?;
                    }
                    FrameData::OutputDtmf(_)
                    | FrameData::OutputDtmfUrgent(_)
                    | FrameData::OutputDtmfSequence(_)
                    | FrameData::OutputDtmfUrgentSequence(_) => self.dtmf(&frame, epoch).await?,
                    FrameData::Sprite(_) => {
                        return Err(ProcessorError::new(
                            "sprite cycling is not implemented by this media transport",
                        ))
                    }
                    _ => {}
                }
                if let Some(history) = &self.spoken_history {
                    let signal = self.destination.queue.signal();
                    if signal.cancelled || signal.epoch > epoch {
                        history.invalidate();
                    } else {
                        if let Some(effects) = history.complete(&frame)? {
                            // The sink has already committed while holding the
                            // ledger lock. Admit ordered frames and completion
                            // records outside it with the original receipt scopes.
                            for effect in effects.ordered {
                                match effect {
                                    crate::SpokenHistoryEffect::Frame(effect) => {
                                        effects.context.try_push_with_invocation_fence(
                                            effect.frame,
                                            effect.direction,
                                            &effects.cause,
                                        )?
                                    }
                                    #[cfg(feature = "assistant-history")]
                                    crate::SpokenHistoryEffect::AssistantStopped(action) => {
                                        action.admit_with_fence(&effects.cause)?
                                    }
                                }
                            }
                        }
                    }
                }
                self.forward_ready(frame, epoch).await
            }
        }
    }
    async fn forward_ready(&mut self, frame: Frame, epoch: u64) -> Result<(), ProcessorError> {
        let source_start = matches!(
            frame.data,
            FrameData::LlmResponseStart
                | FrameData::LlmFullResponseStart(_)
                | FrameData::VisionFullResponseStart(_)
        );
        if !source_start && !matches!(frame.data, FrameData::TtsStarted(_)) {
            return self.forward(frame, epoch).await;
        }
        let readiness = match &self.spoken_history {
            Some(history) => history.readiness(&frame),
            None => Ok(None),
        };
        if matches!(&readiness, Ok(None)) {
            return self.forward(frame, epoch).await;
        }
        let interruptible = frame.interruptible;
        // Legacy PTS scheduling and preceding PCM drain have already completed.
        // The downstream normal lane runs independently of this owned Sender.
        let forwarded = self.forward(frame, epoch).await;
        let result = match (readiness, forwarded) {
            (_, Err(error)) | (Err(error), _) => Err(error),
            (Ok(Some(ready)), Ok(())) => match operation(
                &self.destination.queue,
                epoch,
                interruptible,
                self.config.write_timeout,
                ready,
            )
            .await
            {
                Some(result) => result,
                None => {
                    self.readiness_failed = true;
                    return Ok(());
                }
            },
            (Ok(None), Ok(())) => Ok(()),
        };
        if result.is_err() {
            self.readiness_failed = true;
        } else if source_start {
            self.readiness_failed = false;
        }
        result
    }
    async fn forward(&self, frame: Frame, epoch: u64) -> Result<(), ProcessorError> {
        let signal = self.destination.queue.signal();
        if signal.cancelled || (frame.interruptible && signal.epoch > epoch) {
            return Ok(());
        }
        self.ctx.push(frame, Direction::Downstream).await
    }
    async fn dtmf(&mut self, frame: &Frame, epoch: u64) -> Result<(), ProcessorError> {
        let Some(native) = operation(
            &self.destination.queue,
            epoch,
            frame.interruptible,
            self.config.write_timeout,
            self.backend.send_dtmf(frame),
        )
        .await
        else {
            return Ok(());
        };
        if native? {
            return Ok(());
        }
        let buttons = match &frame.data {
            FrameData::OutputDtmf(key) | FrameData::OutputDtmfUrgent(key) => vec![*key],
            FrameData::OutputDtmfSequence(sequence)
            | FrameData::OutputDtmfUrgentSequence(sequence) => sequence.buttons.clone(),
            _ => return Ok(()),
        };
        for button in buttons {
            let button = pipecat_audio::KeypadEntry::try_from(button.as_char()).map_err(error)?;
            let mono = load_dtmf_audio(button, self.format.sample_rate()).map_err(error)?;
            let data: Vec<u8> = mono
                .as_chunks::<2>()
                .0
                .iter()
                .flat_map(|sample| {
                    (0..self.format.channels()).flat_map(move |_| sample.iter().copied())
                })
                .collect();
            for chunk in data.chunks(self.chunk_bytes) {
                let mut tone = self.audio_frame(chunk.to_vec(), false)?;
                tone.interruptible = frame.interruptible;
                self.deliver(tone, epoch, true).await?;
                if frame.interruptible && self.destination.queue.signal().epoch > epoch {
                    return Ok(());
                }
            }
        }
        Ok(())
    }
    fn buffer(&mut self, frame: Frame, epoch: u64, data: Vec<u8>) {
        if !data.is_empty() {
            self.runs.push_back(AudioRun {
                frame,
                audio: data.into(),
                epoch,
            });
        }
    }
    fn flush_resampler(&mut self) -> Result<(), ProcessorError> {
        if let Some(resampler) = &mut self.resampler {
            let data = resampler.flush().map_err(error)?;
            if let Some((frame, epoch)) = self.source.clone() {
                self.buffer(frame, epoch, data);
            }
        }
        Ok(())
    }
    async fn flush_chunks(&mut self, pad: bool) -> Result<(), ProcessorError> {
        loop {
            self.observe_interruption().await?;
            let count: usize = self.runs.iter().map(|run| run.audio.len()).sum();
            if count == 0 || (count < self.chunk_bytes && !pad) {
                return Ok(());
            }
            let mut frame = self.runs.front().unwrap().frame.duplicate();
            let mut epoch = self.runs.front().unwrap().epoch;
            let mut data = Vec::with_capacity(self.chunk_bytes);
            let mut interruptible = true;
            while data.len() < self.chunk_bytes {
                let Some(run) = self.runs.front_mut() else {
                    break;
                };
                let size = (self.chunk_bytes - data.len()).min(run.audio.len());
                data.extend(run.audio.drain(..size));
                interruptible &= run.frame.interruptible;
                epoch = epoch.min(run.epoch);
                if run.audio.is_empty() {
                    self.runs.pop_front();
                }
            }
            data.resize(self.chunk_bytes, 0);
            frame.interruptible = interruptible;
            let pcm = audio_mut(&mut frame).unwrap();
            pcm.audio = data;
            pcm.sample_rate = self.format.sample_rate();
            frame.transport_destination = self.destination.name.clone();
            self.deliver(frame, epoch, false).await?;
        }
    }
    fn audio_frame(&self, data: Vec<u8>, speech: bool) -> Result<Frame, ProcessorError> {
        let a = AudioRawFrame::new(data, self.format.sample_rate(), self.format.channels())
            .map_err(error)?;
        let mut frame = Frame::new(if speech {
            FrameData::TtsAudio(a)
        } else {
            FrameData::OutputAudio(a)
        });
        frame.transport_destination = self.destination.name.clone();
        Ok(frame)
    }
    async fn deliver(
        &mut self,
        mut frame: Frame,
        epoch: u64,
        generated: bool,
    ) -> Result<(), ProcessorError> {
        let queue = self.destination.queue.clone();
        if !generated && self.readiness_failed {
            if let Some(history) = &self.spoken_history {
                history.rejected_frame(&frame, "assistant playback boundary was not ready");
            }
            return Ok(());
        }
        if !self.destination.usable.load(Ordering::SeqCst) {
            if let Some(history) = &self.spoken_history {
                history.rejected_frame(&frame, "unusable playback backend");
            }
            return Ok(());
        }
        if !wait_delivery(&queue, epoch, frame.interruptible).await {
            if let Some(history) = &self.spoken_history {
                history.invalidate();
            }
            return Ok(());
        }
        if !generated {
            if self.volume * self.gain == 0.0 {
                if let Some(history) = &self.spoken_history {
                    history.rejected_frame(&frame, "muted PCM cannot attest speech");
                }
            }
            if let Some(filter) = &self.filter {
                let Some(result) = operation(
                    &queue,
                    epoch,
                    frame.interruptible,
                    self.config.write_timeout,
                    filter.filter(audio(&frame).unwrap().audio.clone()),
                )
                .await
                else {
                    return Ok(());
                };
                set_audio(&mut frame, result?);
            }
            if audio(&frame).unwrap().audio.is_empty() {
                return Ok(());
            }
            let scaled = scale_audio(&audio(&frame).unwrap().audio, self.volume * self.gain)
                .map_err(error)?;
            set_audio(&mut frame, scaled);
            if let Some(mixer) = &self.destination.mixer {
                let Some(result) = operation(
                    &queue,
                    epoch,
                    frame.interruptible,
                    self.config.write_timeout,
                    mixer.mix(audio(&frame).unwrap().audio.clone()),
                )
                .await
                else {
                    return Ok(());
                };
                set_audio(&mut frame, result?);
            }
        }
        if audio(&frame).unwrap().audio.len() > self.config.queue_byte_capacity {
            return Err(ProcessorError::new(
                "transformed audio exceeds the media byte budget",
            ));
        }
        self.format
            .validate(&audio(&frame).unwrap().audio)
            .map_err(error)?;
        if audio(&frame).unwrap().audio.is_empty() {
            return Ok(());
        }
        if !wait_delivery(&queue, epoch, frame.interruptible).await {
            if let Some(history) = &self.spoken_history {
                history.invalidate();
            }
            return Ok(());
        }
        let result = operation(
            &queue,
            epoch,
            frame.interruptible,
            self.config.write_timeout,
            self.backend.play_audio(&frame),
        )
        .await;
        match result {
            Some(Ok(PlaybackReceipt::Played)) => {
                if !generated {
                    if let Some(history) = &self.spoken_history {
                        history.played(&frame);
                    }
                    self.track_speech(&frame).await?;
                }
                self.ctx.push(frame, Direction::Downstream).await?;
            }
            Some(Err(e)) => self.fail_peer(e).await?,
            Some(Ok(PlaybackReceipt::Discarded)) | None => {
                if let Some(history) = &self.spoken_history {
                    history.rejected_frame(&frame, "PCM lacks an explicit Played receipt");
                }
            }
        }
        Ok(())
    }
    async fn track_speech(&mut self, frame: &Frame) -> Result<(), ProcessorError> {
        let speech = match &frame.data {
            FrameData::TtsAudio(_) => {
                self.tts_audio_played = true;
                true
            }
            FrameData::SpeechOutputAudio(a) => !is_silence(&a.audio).map_err(error)?,
            _ => false,
        };
        if speech {
            if !self.speaking {
                self.speaking = true;
                self.broadcast(FrameData::BotStartedSpeaking).await?;
            }
            if self
                .last_speaking_event
                .is_none_or(|last| last.elapsed() >= Duration::from_millis(200))
            {
                self.broadcast(FrameData::BotSpeaking).await?;
                self.last_speaking_event = Some(Instant::now());
            }
            self.last_speech = Some(Instant::now());
        }
        Ok(())
    }
    async fn stop_speaking(&mut self) -> Result<(), ProcessorError> {
        self.tts_audio_played = false;
        if self.speaking {
            self.speaking = false;
            self.broadcast(FrameData::BotStoppedSpeaking).await?;
        }
        Ok(())
    }
    async fn broadcast(&self, data: FrameData) -> Result<(), ProcessorError> {
        let mut frame = Frame::new(data);
        frame.transport_destination = self.destination.name.clone();
        self.ctx.broadcast(frame).await
    }
    async fn report(&self, error: ProcessorError) -> Result<(), ProcessorError> {
        self.ctx
            .push_error(ErrorFrame {
                error: error.message,
                fatal: error.fatal,
                processor: None,
                category: error.category,
            })
            .await
    }
    async fn fail_peer(&self, error: ProcessorError) -> Result<(), ProcessorError> {
        if let Some(history) = &self.spoken_history {
            history.invalidate_reason("playback backend failed or timed out");
        }
        if self.destination.usable.swap(false, Ordering::SeqCst) {
            self.report(error.with_category(ErrorCategory::Connectivity))
                .await?;
        }
        Ok(())
    }
}
fn flow_flush_returning(data: &FrameData) -> Option<bool> {
    match data {
        FrameData::Flush(flush) if flush.origin.as_deref() == Some("pipecat.flows") => {
            Some(flush.returning)
        }
        FrameData::FlushDetailed(flush) if flush.origin.as_deref() == Some("pipecat.flows") => {
            Some(flush.returning)
        }
        _ => None,
    }
}
fn audio(frame: &Frame) -> Option<&AudioRawFrame> {
    match &frame.data {
        FrameData::OutputAudio(a) | FrameData::TtsAudio(a) | FrameData::SpeechOutputAudio(a) => {
            Some(a)
        }
        _ => None,
    }
}
fn audio_mut(frame: &mut Frame) -> Option<&mut AudioRawFrame> {
    match &mut frame.data {
        FrameData::OutputAudio(a) | FrameData::TtsAudio(a) | FrameData::SpeechOutputAudio(a) => {
            Some(a)
        }
        _ => None,
    }
}
fn set_audio(frame: &mut Frame, data: Vec<u8>) {
    audio_mut(frame).unwrap().audio = data;
}
pub(crate) async fn bounded<T>(
    timeout: Duration,
    future: impl Future<Output = Result<T, ProcessorError>>,
) -> Result<T, ProcessorError> {
    tokio::time::timeout(timeout, future)
        .await
        .map_err(|_| ProcessorError::new("media backend operation timed out"))?
}
async fn operation<T>(
    queue: &MediaQueue,
    epoch: u64,
    interruptible: bool,
    timeout: Duration,
    future: impl Future<Output = Result<T, ProcessorError>>,
) -> Option<Result<T, ProcessorError>> {
    let mut changed = queue.changed.subscribe();
    let future = bounded(timeout, future);
    tokio::pin!(future);
    loop {
        let signal = *changed.borrow_and_update();
        if signal.cancelled || (interruptible && signal.epoch > epoch) {
            return None;
        }
        tokio::select! { biased;
            _ = changed.changed() => {},
            result = &mut future => return Some(result),
        }
    }
}
async fn wait_delivery(queue: &MediaQueue, epoch: u64, interruptible: bool) -> bool {
    let mut changed = queue.changed.subscribe();
    loop {
        let signal = *changed.borrow_and_update();
        if signal.cancelled || (interruptible && signal.epoch > epoch) {
            return false;
        }
        if !signal.paused || signal.ending {
            return true;
        }
        if changed.changed().await.is_err() {
            return false;
        }
    }
}

#[cfg(test)]
mod urgent_control_lifecycle_tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Semaphore;

    const WAIT: Duration = Duration::from_secs(5);
    type TestResult<T> = Result<T, String>;

    async fn bounded_test<T, E: std::fmt::Display>(
        label: &str,
        future: impl Future<Output = Result<T, E>>,
    ) -> TestResult<T> {
        tokio::time::timeout(WAIT, future)
            .await
            .map_err(|_| format!("{label}: deadline"))?
            .map_err(|error| format!("{label}: {error}"))
    }
    struct Backend {
        entered: Semaphore,
        release: Semaphore,
        calls: Mutex<Vec<Frame>>,
        completed: Mutex<Vec<Frame>>,
        dropped: AtomicUsize,
        active: AtomicUsize,
        stopped: AtomicUsize,
        image_entered: Semaphore,
        image_release: Semaphore,
        image_written: AtomicUsize,
        hold_audio: AtomicBool,
        audio_entered: Semaphore,
        audio_release: Semaphore,
    }
    impl Backend {
        fn new() -> Self {
            Self {
                entered: Semaphore::new(0),
                release: Semaphore::new(0),
                calls: Mutex::new(vec![]),
                completed: Mutex::new(vec![]),
                dropped: AtomicUsize::new(0),
                active: AtomicUsize::new(0),
                stopped: AtomicUsize::new(0),
                image_entered: Semaphore::new(0),
                image_release: Semaphore::new(0),
                image_written: AtomicUsize::new(0),
                hold_audio: AtomicBool::new(false),
                audio_entered: Semaphore::new(0),
                audio_release: Semaphore::new(0),
            }
        }
    }
    struct Active<'a> {
        backend: &'a Backend,
        completed: bool,
    }
    impl Drop for Active<'_> {
        fn drop(&mut self) {
            self.backend.active.fetch_sub(1, Ordering::SeqCst);
            if !self.completed {
                self.backend.dropped.fetch_add(1, Ordering::SeqCst);
            }
        }
    }
    #[async_trait]
    impl OutputBackend for Backend {
        fn supports_independent_video(&self) -> bool {
            true
        }
        async fn start(&self, _: PcmFormat, _: &[Option<String>]) -> Result<(), ProcessorError> {
            Ok(())
        }
        async fn play_audio(&self, _: &Frame) -> Result<PlaybackReceipt, ProcessorError> {
            if !self.hold_audio.load(Ordering::SeqCst) {
                return Ok(PlaybackReceipt::Discarded);
            }
            self.active.fetch_add(1, Ordering::SeqCst);
            let mut active = Active {
                backend: self,
                completed: false,
            };
            self.audio_entered.add_permits(1);
            self.audio_release.acquire().await.map_err(error)?.forget();
            active.completed = true;
            Ok(PlaybackReceipt::Played)
        }
        async fn write_image(&self, _: &Frame) -> Result<(), ProcessorError> {
            self.active.fetch_add(1, Ordering::SeqCst);
            let mut active = Active {
                backend: self,
                completed: false,
            };
            self.image_entered.add_permits(1);
            self.image_release.acquire().await.map_err(error)?.forget();
            self.image_written.fetch_add(1, Ordering::SeqCst);
            active.completed = true;
            Ok(())
        }
        async fn interrupt(&self, _: Option<&str>) -> Result<(), ProcessorError> {
            Ok(())
        }
        async fn send_message(&self, frame: &Frame) -> Result<(), ProcessorError> {
            self.active.fetch_add(1, Ordering::SeqCst);
            let mut active = Active {
                backend: self,
                completed: false,
            };
            self.calls.lock().unwrap().push(frame.clone());
            self.entered.add_permits(1);
            self.release.acquire().await.map_err(error)?.forget();
            self.completed.lock().unwrap().push(frame.clone());
            active.completed = true;
            Ok(())
        }
        async fn stop(&self) -> Result<(), ProcessorError> {
            self.stopped.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }
    struct ContextOutput {
        output: Arc<OutputTransport>,
        context: Mutex<Option<ProcessorContext>>,
    }
    #[async_trait]
    impl FrameProcessor for ContextOutput {
        fn name(&self) -> &str {
            self.output.name()
        }
        async fn setup(
            &self,
            start: &StartFrame,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            *self.context.lock().unwrap() = Some(ctx.clone());
            self.output.setup(start, ctx).await
        }
        async fn process(
            &self,
            frame: Frame,
            direction: Direction,
            ctx: &ProcessorContext,
        ) -> Result<(), ProcessorError> {
            self.output.process(frame, direction, ctx).await
        }
        async fn prepare_end(&self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
            self.output.prepare_end(ctx).await
        }
        async fn cleanup(&self) -> Result<(), ProcessorError> {
            self.output.cleanup().await
        }
    }
    async fn start(
        backend: Arc<Backend>,
    ) -> (PipelineWorker, Arc<OutputTransport>, ProcessorContext) {
        start_mode(backend, false).await
    }
    async fn start_mode(
        backend: Arc<Backend>,
        independent: bool,
    ) -> (PipelineWorker, Arc<OutputTransport>, ProcessorContext) {
        let output = OutputTransport::new(
            "urgent-lifecycle-output",
            OutputTransportConfig {
                sample_rate: Some(8000),
                auto_silence: false,
                end_silence: Duration::ZERO,
                write_timeout: Duration::from_secs(2),
                ..Default::default()
            },
            backend,
            None,
            BTreeMap::new(),
        )
        .unwrap();
        let output = if independent {
            output
                .with_live_video(LiveVideoOutputConfig {
                    queue_capacity: 8,
                    queue_byte_capacity: 8192,
                    framerate: None,
                })
                .unwrap()
        } else {
            output
        };
        let output = Arc::new(output);
        let processor = Arc::new(ContextOutput {
            output: output.clone(),
            context: Mutex::new(None),
        });
        let mut worker = bounded_test(
            "start",
            Pipeline::new(vec![processor.clone()]).start(PipelineConfig::default()),
        )
        .await
        .unwrap();
        let ready = bounded_test("ready", async {
            loop {
                let item = worker.recv().await.ok_or("output closed before ready")?;
                if item.direction == Direction::Upstream
                    && matches!(item.frame.data, FrameData::OutputTransportReady)
                {
                    return Ok::<(), &str>(());
                }
            }
        })
        .await;
        if let Err(problem) = ready {
            let _ = bounded_test("startup fallback cancel", worker.cancel(problem.clone())).await;
            let _ = finish(worker, true).await;
            panic!("{problem}");
        }
        let context = processor
            .context
            .lock()
            .unwrap()
            .clone()
            .expect("setup captures context");
        (worker, output, context)
    }
    async fn enter(
        future: &mut (impl Future<Output = Result<(), ProcessorError>> + Unpin),
        backend: &Backend,
    ) -> TestResult<()> {
        bounded_test("message entered", async {
            tokio::select! {
                biased;
                result = future => Err(ProcessorError::new(format!("send returned before release: {result:?}"))),
                permit = backend.entered.acquire() => {
                    permit.map_err(error)?.forget();
                    Ok(())
                }
            }
        }).await
    }
    async fn finish(
        mut worker: PipelineWorker,
        terminal_requested: bool,
    ) -> TestResult<WorkerReport> {
        let ended = if terminal_requested {
            Ok(())
        } else {
            bounded_test("end", worker.end()).await
        };
        if ended.is_err() {
            let _ = bounded_test("fallback cancel", worker.cancel("end failed")).await;
        }
        let drained = bounded_test("drain", async {
            while worker.recv().await.is_some() {}
            Ok::<(), String>(())
        })
        .await;
        if drained.is_err() {
            let _ = bounded_test("fallback cancel", worker.cancel("drain failed")).await;
        }
        let joined = bounded_test("join", worker.join()).await;
        if joined.is_err() {
            let _ = bounded_test("fallback cancel", worker.cancel("join failed")).await;
            let _ = bounded_test("fallback join", worker.join()).await;
        }
        let report = joined?;
        ended?;
        drained?;
        Ok(report)
    }
    fn urgent() -> Frame {
        Frame::new(FrameData::OutputTransportMessageUrgent(
            serde_json::json!({"type":"lifecycle-control"}),
        ))
    }

    #[tokio::test]
    async fn admitted_urgent_survives_observed_media_sender_drop() {
        let backend = Arc::new(Backend::new());
        let (worker, output, ctx) = start(backend.clone()).await;
        let expected = urgent();
        // Keep this public-method future locally pinned: after entry it is not
        // polled again until the real pipeline-owned media Sender has dropped.
        let mut sending = Box::pin(output.process(expected.clone(), Direction::Downstream, &ctx));
        let entered = enter(&mut sending, &backend).await;
        let observed_drop = if entered.is_ok() {
            let queue = output.control.destination(None).unwrap().queue.clone();
            let mut changed = queue.changed.subscribe();
            let prepared = bounded_test("prepare end", output.prepare_end(&ctx)).await;
            if prepared.is_ok() {
                bounded_test("actual media Sender drop", async {
                    changed
                        .wait_for(|signal| signal.ending && signal.cancelled)
                        .await
                        .map(|_| ())
                        .map_err(error)
                })
                .await
            } else {
                prepared
            }
        } else {
            Err("send never entered".into())
        };
        // No Cancel or cleanup has run. The observed cancelled flag therefore
        // comes from the actual Sender::drop, not a timer or a synthetic signal.
        backend.release.add_permits(1);
        let sent = bounded_test("finish admitted urgent", &mut sending).await;
        drop(sending);
        let finished = finish(worker, false).await;
        assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
        entered.unwrap();
        observed_drop.unwrap();
        sent.unwrap();
        let report = finished.unwrap();
        assert_eq!(report.state, WorkerState::Ended);
        assert!(report.errors.is_empty(), "{:?}", report.errors);
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
        assert_eq!(backend.calls.lock().unwrap().len(), 1);
        assert_eq!(
            backend.dropped.load(Ordering::SeqCst),
            0,
            "graceful media actor exit must not cancel admitted protocol send"
        );
        let completed = backend.completed.lock().unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0], expected);
        assert_eq!(
            completed[0].observation_token(),
            expected.observation_token()
        );
    }

    #[tokio::test]
    async fn actual_cancel_and_cleanup_drop_local_urgent_future_without_late_send() {
        for cancel in [true, false] {
            let backend = Arc::new(Backend::new());
            let (worker, output, ctx) = start(backend.clone()).await;
            let expected = urgent();
            let mut sending =
                Box::pin(output.process(expected.clone(), Direction::Downstream, &ctx));
            let entered = enter(&mut sending, &backend).await;
            let closed = if cancel {
                bounded_test(
                    "actual Cancel",
                    output.process(Frame::cancel("cancel urgent"), Direction::Downstream, &ctx),
                )
                .await
            } else {
                bounded_test("actual cleanup", output.cleanup()).await
            };
            // The test, not a pipeline actor, owns this future. A core actor
            // abort cannot satisfy the assertion: the send itself must return
            // after the real output lifecycle action, without backend release.
            let sent = bounded_test("cancel owned send", &mut sending).await;
            drop(sending);
            let completed_before_release = backend.completed.lock().unwrap().len();
            backend.release.add_permits(1);
            let finished = finish(worker, cancel && closed.is_ok()).await;
            assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
            entered.unwrap();
            closed.unwrap();
            sent.unwrap();
            let report = finished.unwrap();
            assert_eq!(
                report.state,
                if cancel {
                    WorkerState::Cancelled
                } else {
                    WorkerState::Ended
                }
            );
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert_eq!(backend.active.load(Ordering::SeqCst), 0);
            assert_eq!(backend.calls.lock().unwrap().len(), 1);
            assert_eq!(backend.dropped.load(Ordering::SeqCst), 1);
            assert_eq!(completed_before_release, 0);
            assert!(
                backend.completed.lock().unwrap().is_empty(),
                "released permit must not resurrect a cancelled write"
            );
        }
    }

    #[tokio::test]
    async fn opted_in_lane_drop_does_not_cancel_other_lane_or_admitted_urgent() {
        for media_exits_first in [true, false] {
            let backend = Arc::new(Backend::new());
            backend
                .hold_audio
                .store(!media_exits_first, Ordering::SeqCst);
            let (worker, output, ctx) = start_mode(backend.clone(), true).await;
            let held = if media_exits_first {
                Frame::new(FrameData::OutputImage(ImageRawFrame {
                    image: vec![1, 2, 3],
                    size: (1, 1),
                    format: Some("RGB".into()),
                }))
            } else {
                Frame::new(FrameData::OutputAudio(
                    AudioRawFrame::new(vec![1; 640], 8000, 1).unwrap(),
                ))
            };
            let admitted = bounded_test(
                "held lane admission",
                output.process(held, Direction::Downstream, &ctx),
            )
            .await;
            let held_entered = if media_exits_first {
                bounded_test("held live image", backend.image_entered.acquire())
                    .await
                    .map(|p| p.forget())
            } else {
                bounded_test("held PCM", backend.audio_entered.acquire())
                    .await
                    .map(|p| p.forget())
            };
            let expected = urgent();
            let mut sending =
                Box::pin(output.process(expected.clone(), Direction::Downstream, &ctx));
            let entered = enter(&mut sending, &backend).await;
            let destination = output.control.destination(None).unwrap();
            let live = destination.live().unwrap();
            let (exiting, surviving) = if media_exits_first {
                (destination.queue.clone(), live.queue.clone())
            } else {
                (live.queue.clone(), destination.queue.clone())
            };
            let mut changed = exiting.changed.subscribe();
            let prepared = bounded_test("prepare both lanes", output.prepare_end(&ctx)).await;
            let observed_drop = bounded_test("actual independent lane Drop", async {
                changed
                    .wait_for(|signal| signal.ending && signal.cancelled)
                    .await
                    .map(|_| ())
                    .map_err(error)
            })
            .await;
            let other_still_live = !surviving.signal().cancelled;
            let active_before_release = backend.active.load(Ordering::SeqCst);
            backend.image_release.add_permits(8);
            backend.audio_release.add_permits(8);
            backend.release.add_permits(1);
            let sent = bounded_test("finish admitted urgent", &mut sending).await;
            drop(sending);
            let finished = finish(worker, false).await;
            admitted.unwrap();
            held_entered.unwrap();
            entered.unwrap();
            prepared.unwrap();
            observed_drop.unwrap();
            sent.unwrap();
            let report = finished.unwrap();
            assert_eq!(report.state, WorkerState::Ended);
            assert!(report.errors.is_empty(), "{:?}", report.errors);
            assert!(
                other_still_live,
                "graceful one-lane Drop must not cancel its sibling"
            );
            assert_eq!(
                active_before_release, 2,
                "held media/image plus admitted urgent remain active"
            );
            assert_eq!(backend.active.load(Ordering::SeqCst), 0);
            assert_eq!(backend.dropped.load(Ordering::SeqCst), 0);
            assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
            assert_eq!(backend.completed.lock().unwrap().as_slice(), &[expected]);
            assert_eq!(
                backend.image_written.load(Ordering::SeqCst),
                usize::from(media_exits_first)
            );
        }
    }

    #[test]
    fn live_publication_and_old_control_transitions_share_one_local_lock() {
        // Bounded, joined test threads exercise actual concurrent construction;
        // production adds no thread or per-frame task. Both possible lock orders
        // must leave the real media and live queue flags at the control's value.
        for paused in [false, true] {
            for _ in 0..32 {
                let backend = Arc::new(Backend::new());
                let output = OutputTransport::new(
                    "publication-control",
                    OutputTransportConfig::default(),
                    backend.clone(),
                    None,
                    BTreeMap::new(),
                )
                .unwrap();
                let control = output.control();
                if !paused {
                    control.pause(None).unwrap();
                }
                let barrier = Arc::new(std::sync::Barrier::new(2));
                let output = std::thread::scope(|scope| {
                    let builder_barrier = barrier.clone();
                    let builder = scope.spawn(move || {
                        builder_barrier.wait();
                        output.with_live_video(LiveVideoOutputConfig {
                            queue_capacity: 1,
                            queue_byte_capacity: 1024,
                            framerate: None,
                        })
                    });
                    let transition = scope.spawn(move || {
                        barrier.wait();
                        if paused {
                            control.pause(None)
                        } else {
                            control.resume(None)
                        }
                    });
                    let output = builder.join().unwrap().unwrap();
                    transition.join().unwrap().unwrap();
                    output
                });
                let destination = output.control.destination(None).unwrap();
                let live = destination.live().unwrap();
                assert_eq!(destination.queue.signal().paused, paused);
                assert_eq!(live.queue.signal().paused, paused);
                assert_eq!(backend.stopped.load(Ordering::SeqCst), 0);
            }
        }
    }

    #[tokio::test]
    async fn live_builder_rejects_after_real_backend_setup_before_worker_start() {
        let (worker, _, ctx) = start(Arc::new(Backend::new())).await;
        let backend = Arc::new(Backend::new());
        let output = OutputTransport::new(
            "late-live-builder",
            OutputTransportConfig {
                sample_rate: Some(8000),
                auto_silence: false,
                end_silence: Duration::ZERO,
                ..Default::default()
            },
            backend.clone(),
            None,
            BTreeMap::new(),
        )
        .unwrap();
        let setup = bounded_test(
            "actual output setup",
            output.setup(&StartFrame::default(), &ctx),
        )
        .await;
        let rejected = output
            .with_live_video(LiveVideoOutputConfig {
                queue_capacity: 1,
                queue_byte_capacity: 8192,
                framerate: None,
            })
            .is_err();
        // The consuming rejected builder no longer returns the transport. This
        // test's recording backend owns no device; explicitly close its hook.
        let stopped = backend.stop().await;
        let finished = finish(worker, false).await;
        setup.unwrap();
        stopped.unwrap();
        finished.unwrap();
        assert!(rejected);
        assert_eq!(backend.stopped.load(Ordering::SeqCst), 1);
        assert_eq!(backend.active.load(Ordering::SeqCst), 0);
    }
}
