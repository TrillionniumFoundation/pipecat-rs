//! Owned native pipeline endpoints. Publication is never a playback receipt.
mod audio;
mod queue;

use crate::{
    AudioRawFrame, Error, MoqConfig, MoqConnection, MoqEvent, MoqMonitor, MoqSender, Result,
};
use async_trait::async_trait;
use pipecat_audio::SincResampler;
use pipecat_core::{
    Direction, ErrorCategory, Frame, FrameData, FrameProcessor, ProcessorContext, ProcessorError,
    ProcessorHealth, ProcessorHealthController, StartFrame,
};
use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};
use tokio::sync::{mpsc, oneshot, watch};

/// Pipeline-side bounds; network/track limits remain in `MoqConfig`.
/// Settings are immutable for one single-use pair of endpoints.
#[derive(Debug, Clone)]
pub struct MoqTransportConfig {
    /// None follows StartFrame (normally 16 kHz); input always emits mono PCM.
    pub input_sample_rate: Option<u32>,
    pub queue_capacity: usize,
    pub queue_byte_capacity: usize,
    /// Serialized frame bound, including PCM, routing metadata, and text.
    pub max_frame_bytes: usize,
    pub admission_timeout: Duration,
    pub operation_timeout: Duration,
}
impl Default for MoqTransportConfig {
    fn default() -> Self {
        Self {
            input_sample_rate: None,
            queue_capacity: 64,
            queue_byte_capacity: 2 * 1024 * 1024,
            max_frame_bytes: 256 * 1024,
            admission_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(5),
        }
    }
}
impl MoqTransportConfig {
    fn validate(&self) -> Result<()> {
        if let Some(rate) = self.input_sample_rate {
            audio::validate_rate(rate)?;
        }
        if !(1..=1024).contains(&self.queue_capacity)
            || !(1024..=16 * 1024 * 1024).contains(&self.queue_byte_capacity)
            || !(1024..=self.queue_byte_capacity).contains(&self.max_frame_bytes)
            || self.admission_timeout.is_zero()
            || self.admission_timeout > Duration::from_secs(120)
            || self.operation_timeout.is_zero()
            || self.operation_timeout > Duration::from_secs(120)
        {
            return Err(Error::InvalidConfig("pipeline limits"));
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MoqTransportState {
    Connected,
    Draining,
    Closed,
    Failed(Error),
}

/// Owns one real connection shared by input/output processors. End releases
/// each registered holder only after its owned work drains. Cancel/cleanup
/// close the whole connection immediately. An unused endpoint holds no claim.
///
/// Use the two endpoints in the same pipeline, with input before output, or use
/// either endpoint alone. Existing sender clones should not concurrently publish
/// audio: the pipeline owns packetization and its interruption generations.
pub struct MoqTransport {
    input: Arc<MoqInputTransport>,
    output: Arc<MoqOutputTransport>,
    shared: Arc<Shared>,
}
impl MoqTransport {
    pub async fn connect(network: MoqConfig, config: MoqTransportConfig) -> Result<Self> {
        config.validate()?;
        Self::new(MoqConnection::connect(network).await?, config)
    }
    pub fn new(mut connection: MoqConnection, config: MoqTransportConfig) -> Result<Self> {
        config.validate()?;
        let events = connection.take_events().ok_or(Error::InvalidConfig(
            "connection events already transferred",
        ))?;
        if let Some(result) = connection.monitor().result() {
            result?;
            return Err(Error::Closed);
        }
        let sender = connection.sender();
        let generation = sender.generation();
        let (health, health_owner) = ProcessorHealth::read_only(true);
        let shared = Arc::new(Shared {
            monitor: connection.monitor(),
            connection: Mutex::new(Some(connection)),
            sender,
            events: Mutex::new(Some(events)),
            holders: Mutex::new(0),
            registration: Mutex::new(Registration::default()),
            state: watch::channel(MoqTransportState::Connected).0,
            close_result: watch::channel(None).0,
            closing: AtomicBool::new(false),
            input_registered: AtomicBool::new(false),
            events_stop: watch::channel(false).0,
            force_cancel: watch::channel(false).0,
            health,
            health_owner,
            config: config.clone(),
            #[cfg(test)]
            audio_processed: watch::channel(0).0,
        });
        Ok(Self {
            input: Arc::new(MoqInputTransport {
                shared: shared.clone(),
                registered: AtomicBool::new(false),
                released: AtomicBool::new(false),
                started: AtomicBool::new(false),
                rate: Mutex::new(None),
            }),
            output: Arc::new(MoqOutputTransport {
                shared: shared.clone(),
                registered: AtomicBool::new(false),
                released: AtomicBool::new(false),
                started: AtomicBool::new(false),
                normal: Arc::new(queue::Queue::new(&config, generation)),
                urgent: Arc::new(queue::Queue::new(&config, generation)),
            }),
            shared,
        })
    }
    pub fn input(&self) -> Arc<MoqInputTransport> {
        self.input.clone()
    }
    pub fn output(&self) -> Arc<MoqOutputTransport> {
        self.output.clone()
    }
    pub fn state(&self) -> MoqTransportState {
        self.shared.state.borrow().clone()
    }
    pub fn health(&self) -> ProcessorHealth {
        self.shared.health.clone()
    }
    pub fn monitor(&self) -> MoqMonitor {
        self.shared.monitor.clone()
    }
    #[cfg(test)]
    pub(crate) fn audio_progress_for_test(&self) -> watch::Receiver<u64> {
        self.shared.audio_processed.subscribe()
    }
    /// Hard shutdown, also safe before pipeline setup or after End.
    pub async fn cancel(&self) -> Result<()> {
        self.shared.close(false).await
    }
}
#[derive(Default)]
struct Registration {
    runtime: Option<u64>,
    input: Option<usize>,
    output: Option<usize>,
}
struct Shared {
    connection: Mutex<Option<MoqConnection>>,
    sender: MoqSender,
    monitor: MoqMonitor,
    events: Mutex<Option<mpsc::Receiver<MoqEvent>>>,
    holders: Mutex<usize>,
    registration: Mutex<Registration>,
    input_registered: AtomicBool,
    state: watch::Sender<MoqTransportState>,
    close_result: watch::Sender<Option<Result<()>>>,
    closing: AtomicBool,
    events_stop: watch::Sender<bool>,
    force_cancel: watch::Sender<bool>,
    health: ProcessorHealth,
    health_owner: ProcessorHealthController,
    config: MoqTransportConfig,
    #[cfg(test)]
    audio_processed: watch::Sender<u64>,
}
impl Shared {
    fn register(
        &self,
        registered: &AtomicBool,
        context: &ProcessorContext,
        input: bool,
    ) -> Result<()> {
        let mut holders = self.holders.lock().unwrap();
        if self.closing.load(Ordering::SeqCst) || self.monitor.result().is_some() {
            return Err(Error::Closed);
        }
        let identity = context.processor_identity();
        let mut registration = self.registration.lock().unwrap();
        if registration
            .runtime
            .is_some_and(|runtime| runtime != identity.runtime_id)
        {
            return Err(Error::InvalidConfig(
                "MoQ endpoints must share one pipeline",
            ));
        }
        if (input
            && registration
                .output
                .is_some_and(|index| index <= identity.index))
            || (!input
                && registration
                    .input
                    .is_some_and(|index| index >= identity.index))
        {
            return Err(Error::InvalidConfig("MoQ input must precede output"));
        }
        if registered.swap(true, Ordering::SeqCst) {
            return Err(Error::InvalidConfig("pipeline endpoint is single-use"));
        }
        registration.runtime = Some(identity.runtime_id);
        if input {
            registration.input = Some(identity.index);
        } else {
            registration.output = Some(identity.index);
        }
        *holders += 1;
        Ok(())
    }
    async fn release(&self, registered: &AtomicBool, released: &AtomicBool) -> Result<()> {
        if !registered.load(Ordering::SeqCst) || released.swap(true, Ordering::SeqCst) {
            return Ok(());
        }
        let last = {
            let mut holders = self.holders.lock().unwrap();
            *holders = holders.saturating_sub(1);
            *holders == 0
        };
        if last {
            self.close(true).await
        } else {
            Ok(())
        }
    }
    fn fail(&self, error: Error) {
        self.health_owner.set_usable(false);
        self.state.send_replace(MoqTransportState::Failed(error));
    }
    async fn close(&self, graceful: bool) -> Result<()> {
        if !graceful {
            self.force_cancel.send_replace(true);
        }
        // Extract ownership before awaiting. A dropped close future drops the
        // connection (aborting it), and publishes a terminal result for waiters.
        let connection = {
            let mut slot = self.connection.lock().unwrap();
            let value = slot.take();
            if value.is_some() {
                self.closing.store(true, Ordering::SeqCst);
                if !matches!(*self.state.borrow(), MoqTransportState::Failed(_)) {
                    self.state.send_replace(MoqTransportState::Draining);
                }
            }
            value
        };
        self.events_stop.send_replace(true);
        if let Some(connection) = connection {
            let mut guard = CloseGuard {
                connection: Some(connection),
                done: self.close_result.clone(),
                owner: self,
            };
            let connection = guard.connection.as_mut().unwrap();
            let mut cancellation = self.force_cancel.subscribe();
            let hard_stop = !graceful || *cancellation.borrow();
            let result = if hard_stop {
                connection.cancel().await
            } else {
                tokio::select! {
                    biased;
                    _ = cancellation.changed() => connection.cancel().await,
                    result = connection.end() => result,
                }
            };
            self.health_owner.set_usable(false);
            match &result {
                Err(error) => self.fail(error.clone()),
                Ok(()) if !matches!(*self.state.borrow(), MoqTransportState::Failed(_)) => {
                    self.state.send_replace(MoqTransportState::Closed);
                }
                _ => {}
            }
            guard.done.send_replace(Some(result.clone()));
            return result;
        }
        let mut done = self.close_result.subscribe();
        loop {
            if let Some(result) = done.borrow_and_update().clone() {
                return result;
            }
            done.changed().await.map_err(|_| Error::Closed)?;
        }
    }
    fn start_events(
        self: &Arc<Self>,
        ctx: &ProcessorContext,
        input_rate: Option<u32>,
    ) -> std::result::Result<(), ProcessorError> {
        if input_rate.is_none() && self.input_registered.load(Ordering::SeqCst) {
            return Ok(());
        }
        let events = self.events.lock().unwrap().take().ok_or_else(|| {
            processor_error(Error::InvalidConfig("event pump already started"), false)
        })?;
        let shared = self.clone();
        let output = ctx.clone();
        ctx.spawn("moq-input-events", false, async move {
            let result = receive_events(shared.clone(), events, input_rate, output).await;
            if let Err(error) = &result {
                shared.fail(error.clone());
                let _ = shared.close(false).await;
            }
            result.map_err(|error| processor_error(error, true))
        })?;
        Ok(())
    }
}
struct CloseGuard<'a> {
    owner: &'a Shared,
    connection: Option<MoqConnection>,
    done: watch::Sender<Option<Result<()>>>,
}
impl Drop for CloseGuard<'_> {
    fn drop(&mut self) {
        self.connection.take();
        if self.done.borrow().is_none() {
            self.owner.fail(Error::Closed);
            self.done.send_replace(Some(Err(Error::Closed)));
        }
    }
}

pub struct MoqInputTransport {
    shared: Arc<Shared>,
    registered: AtomicBool,
    released: AtomicBool,
    started: AtomicBool,
    rate: Mutex<Option<u32>>,
}
#[async_trait]
impl FrameProcessor for MoqInputTransport {
    fn name(&self) -> &str {
        "moq-input"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.shared.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        context: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        let rate = self
            .shared
            .config
            .input_sample_rate
            .unwrap_or(start.audio_in_sample_rate);
        audio::validate_rate(rate).map_err(|e| processor_error(e, false))?;
        self.shared
            .register(&self.registered, context, true)
            .map_err(|e| processor_error(e, true))?;
        *self.rate.lock().unwrap() = Some(rate);
        self.shared.input_registered.store(true, Ordering::SeqCst);
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if frame.data.is_start() {
            if !self.registered.load(Ordering::SeqCst) || self.started.swap(true, Ordering::SeqCst)
            {
                return Err(processor_error(
                    Error::InvalidConfig("endpoint setup incomplete or already started"),
                    true,
                ));
            }
            ctx.push(frame, direction).await?;
            self.shared.start_events(ctx, *self.rate.lock().unwrap())?;
            return Ok(());
        }
        if matches!(
            frame.data,
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
        ) {
            self.shared
                .release(&self.registered, &self.released)
                .await
                .map_err(|e| processor_error(e, true))?;
        }
        if matches!(
            frame.data,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.shared
                .close(false)
                .await
                .map_err(|e| processor_error(e, true))?;
        }
        ctx.push(frame, direction).await
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        self.shared.events_stop.send_replace(true);
        Ok(())
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.shared
            .close(false)
            .await
            .map_err(|e| processor_error(e, true))
    }
}

pub struct MoqOutputTransport {
    shared: Arc<Shared>,
    registered: AtomicBool,
    released: AtomicBool,
    started: AtomicBool,
    normal: Arc<queue::Queue>,
    urgent: Arc<queue::Queue>,
}
#[async_trait]
impl FrameProcessor for MoqOutputTransport {
    fn name(&self) -> &str {
        "moq-output"
    }
    fn health(&self) -> Option<ProcessorHealth> {
        Some(self.shared.health.clone())
    }
    async fn setup(
        &self,
        start: &StartFrame,
        context: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        audio::validate_rate(start.audio_out_sample_rate).map_err(|e| processor_error(e, false))?;
        self.shared
            .register(&self.registered, context, false)
            .map_err(|e| processor_error(e, true))
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> std::result::Result<(), ProcessorError> {
        if frame.data.is_start() {
            if !self.registered.load(Ordering::SeqCst) || self.started.swap(true, Ordering::SeqCst)
            {
                return Err(processor_error(
                    Error::InvalidConfig("endpoint setup incomplete or already started"),
                    true,
                ));
            }
            ctx.push(frame, direction).await?;
            for (queue, urgent) in [(self.normal.clone(), false), (self.urgent.clone(), true)] {
                let shared = self.shared.clone();
                let output = ctx.clone();
                ctx.spawn(
                    if urgent {
                        "moq-urgent-publish"
                    } else {
                        "moq-audio-publish"
                    },
                    false,
                    async move {
                        let result = publish_lane(shared.clone(), queue.clone(), output).await;
                        if let Err(error) = &result {
                            queue.cancel();
                            shared.fail(error.clone());
                            let _ = shared.close(false).await;
                        }
                        result.map_err(|error| processor_error(error, true))
                    },
                )?;
            }
            self.shared.start_events(ctx, None)?;
            return ctx
                .push(
                    Frame::new(FrameData::OutputTransportReady),
                    Direction::Upstream,
                )
                .await;
        }
        if matches!(frame.data, FrameData::Interruption) {
            if frame.transport_destination.is_some() {
                return Err(processor_error(
                    Error::Unsupported("MoQ output destinations"),
                    false,
                ));
            }
            self.shared
                .sender
                .interrupt()
                .await
                .map_err(|e| processor_error(e, true))?;
            let generation = self.shared.sender.generation();
            self.normal.interrupt(generation);
            self.urgent.interrupt(generation);
            return ctx.push(frame, direction).await;
        }
        if matches!(
            frame.data,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        ) {
            self.normal.cancel();
            self.urgent.cancel();
            self.shared
                .close(false)
                .await
                .map_err(|e| processor_error(e, true))?;
            return ctx.push(frame, direction).await;
        }
        if matches!(
            frame.data,
            FrameData::End(_) | FrameData::EndDetailed(_) | FrameData::Stop
        ) {
            self.shared
                .release(&self.registered, &self.released)
                .await
                .map_err(|e| processor_error(e, true))?;
            return ctx.push(frame, direction).await;
        }
        if direction == Direction::Upstream {
            return ctx.push(frame, direction).await;
        }
        validate_output(&frame).map_err(|e| processor_error(e, false))?;
        if raw_audio(&frame).is_some() {
            self.shared
                .sender
                .ensure_audio_output()
                .map_err(|e| processor_error(e, false))?;
        }
        if let FrameData::OutputTransportMessage(message)
        | FrameData::OutputTransportMessageUrgent(message) = &frame.data
        {
            self.shared
                .sender
                .validate_json(message)
                .map_err(|e| processor_error(e, false))?;
        }
        let urgent = matches!(frame.data, FrameData::OutputTransportMessageUrgent(_));
        if frame.is_system()
            && !urgent
            && !matches!(
                frame.data,
                FrameData::Flush(_) | FrameData::FlushDetailed(_)
            )
        {
            return ctx.push(frame, direction).await;
        }
        let generation = self.shared.sender.generation();
        let queue = if urgent { &self.urgent } else { &self.normal };
        queue
            .push_with_context(frame, generation, urgent, Some(ctx.clone()))
            .await
            .map_err(|e| processor_error(e, false))
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> std::result::Result<(), ProcessorError> {
        if !self.shared.input_registered.load(Ordering::SeqCst) {
            self.shared.events_stop.send_replace(true);
        }
        self.normal.end();
        self.urgent.end();
        Ok(())
    }
    async fn cleanup(&self) -> std::result::Result<(), ProcessorError> {
        self.normal.cancel();
        self.urgent.cancel();
        self.shared
            .close(false)
            .await
            .map_err(|e| processor_error(e, true))
    }
}
fn raw_audio(frame: &Frame) -> Option<&AudioRawFrame> {
    match &frame.data {
        FrameData::OutputAudio(a) | FrameData::TtsAudio(a) | FrameData::SpeechOutputAudio(a) => {
            Some(a)
        }
        _ => None,
    }
}
fn validate_output(frame: &Frame) -> Result<()> {
    if frame.transport_destination.is_some() {
        return Err(Error::Unsupported("MoQ output destinations"));
    }
    if frame.pts.is_some() {
        return Err(Error::Unsupported("scheduled pipeline output PTS"));
    }
    if let Some(audio) = raw_audio(frame) {
        audio::validate_audio(audio)?;
    }
    match &frame.data {
        FrameData::OutputTransportMessage(message)
        | FrameData::OutputTransportMessageUrgent(message)
            if !message.is_object() =>
        {
            return Err(Error::InvalidConfig("transcript message must be an object"))
        }
        FrameData::OutputImage(_)
        | FrameData::OutputImageDetailed(_)
        | FrameData::UrlImage(_)
        | FrameData::Sprite(_)
        | FrameData::OutputDtmf(_)
        | FrameData::OutputDtmfUrgent(_)
        | FrameData::OutputDtmfSequence(_)
        | FrameData::OutputDtmfUrgentSequence(_)
        | FrameData::FilterEnable(_)
        | FrameData::FilterControl
        | FrameData::FilterUpdateSettings(_)
        | FrameData::MixerEnable(_)
        | FrameData::MixerControl
        | FrameData::MixerUpdateSettings(_)
        | FrameData::Volume(_)
        | FrameData::VolumeGain(_) => return Err(Error::Unsupported("rich media output control")),
        FrameData::ProcessorPause { processor }
        | FrameData::ProcessorPauseUrgent { processor }
        | FrameData::ProcessorResume { processor }
        | FrameData::ProcessorResumeUrgent { processor }
            if processor == "moq-output" =>
        {
            return Err(Error::Unsupported("MoQ output pause/resume"))
        }
        FrameData::ProcessorPauseTarget(processor)
        | FrameData::ProcessorPauseUrgentTarget(processor)
        | FrameData::ProcessorResumeTarget(processor)
            if processor.name() == "moq-output" =>
        {
            return Err(Error::Unsupported("MoQ output pause/resume"))
        }
        _ => {}
    }
    Ok(())
}
fn processor_error(error: Error, terminal: bool) -> ProcessorError {
    let category = match error {
        Error::InvalidConfig(_) | Error::InvalidAudio(_) | Error::Unsupported(_) => {
            ErrorCategory::InvalidRequest
        }
        Error::Network(_) | Error::Closed | Error::Timeout(_) => ErrorCategory::Connectivity,
        Error::Backpressure | Error::Limit(_) => ErrorCategory::Application,
        Error::Protocol(_)
        | Error::Media(_)
        | Error::Task(_)
        | Error::Stale
        | Error::Interrupted => ErrorCategory::Server,
    };
    ProcessorError::new(error.to_string())
        .with_category(category)
        .with_permanence_override(terminal)
}
async fn timed<T>(
    duration: Duration,
    operation: &'static str,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    tokio::time::timeout(duration, future)
        .await
        .map_err(|_| Error::Timeout(operation))?
}
async fn receive_events(
    shared: Arc<Shared>,
    mut events: mpsc::Receiver<MoqEvent>,
    rate: Option<u32>,
    ctx: ProcessorContext,
) -> Result<()> {
    let mut stop = shared.events_stop.subscribe();
    let mut monitor = shared.monitor.clone();
    let mut resampler = rate
        .map(|rate| SincResampler::new(48_000, rate, 1).map_err(Error::media))
        .transpose()?;
    let mut previous_timestamp = None;
    let mut expected_timestamp = None;
    loop {
        if *stop.borrow() {
            if let Some(resampler) = &mut resampler {
                let bytes = resampler.flush().map_err(Error::media)?;
                emit_input(bytes, rate.unwrap(), previous_timestamp, &shared, &ctx).await?;
            }
            return Ok(());
        }
        let event = tokio::select! {
            biased;
            _ = stop.changed() => continue,
            result = monitor.wait_closed() => {
                if shared.closing.load(Ordering::SeqCst) { return Ok(()); }
                return result.and(Err(Error::Closed));
            }
            event = events.recv() => event,
        };
        match event {
            Some(MoqEvent::PeerConnected) => {
                ctx.push(
                    Frame::new(FrameData::ClientConnected),
                    Direction::Downstream,
                )
                .await
                .map_err(|e| Error::Task(e.message))?;
            }
            Some(MoqEvent::Message(message)) => {
                // The source's RTVI processor precedes the input transport.
                let frame = Frame::new(FrameData::InputTransportMessage(message));
                queue::frame_size(&frame, shared.config.max_frame_bytes)?;
                ctx.push(frame, Direction::Upstream)
                    .await
                    .map_err(|e| Error::Task(e.message))?;
            }
            Some(MoqEvent::Audio {
                audio,
                timestamp_us,
            }) => {
                if let Some(resampler) = &mut resampler {
                    if audio.sample_rate != 48_000 || audio.num_channels != 1 {
                        return Err(Error::InvalidAudio(
                            "native receive PCM must be mono 48000 Hz",
                        ));
                    }
                    audio::validate_audio(&audio)?;
                    if expected_timestamp.is_some_and(|expected| expected != timestamp_us) {
                        let tail = resampler.flush().map_err(Error::media)?;
                        emit_input(tail, rate.unwrap(), previous_timestamp, &shared, &ctx).await?;
                    }
                    let bytes = resampler.process(&audio.audio).map_err(Error::media)?;
                    emit_input(bytes, rate.unwrap(), Some(timestamp_us), &shared, &ctx).await?;
                    expected_timestamp =
                        timestamp_us.checked_add(audio.num_frames() as u64 * 1_000_000 / 48_000);
                    previous_timestamp = Some(timestamp_us);
                }
            }
            None if shared.closing.load(Ordering::SeqCst) => return Ok(()),
            None => {
                return Err(monitor
                    .result()
                    .and_then(|r| r.err())
                    .unwrap_or(Error::Closed))
            }
        }
    }
}
async fn emit_input(
    bytes: Vec<u8>,
    rate: u32,
    timestamp: Option<u64>,
    shared: &Shared,
    ctx: &ProcessorContext,
) -> Result<()> {
    if bytes.is_empty() {
        return Ok(());
    }
    let mut frame = Frame::new(FrameData::InputAudio(
        AudioRawFrame::new(bytes, rate, 1).map_err(Error::media)?,
    ));
    frame.transport_source = Some("moq-peer".into());
    if let Some(timestamp) = timestamp {
        frame
            .metadata
            .insert("moq.packet_timestamp_us".into(), timestamp.into());
    }
    // Wire timestamps are deliberately not reinterpreted as pipeline-clock PTS.
    queue::frame_size(&frame, shared.config.max_frame_bytes)?;
    timed(
        shared.config.operation_timeout,
        "pipeline input admission",
        async {
            ctx.push_downstream_wait(frame)
                .await
                .map_err(|e| Error::Task(e.message))
        },
    )
    .await
}
struct PendingAudio {
    stream: audio::Conversion,
    context: ProcessorContext,
    latest: ProcessorContext,
}
/// A cancelled owned publication must also invalidate commands already queued
/// in the independently driven native connection. No task is detached.
struct PublicationFence {
    generation: u64,
    sender: MoqSender,
    armed: bool,
}
impl Drop for PublicationFence {
    fn drop(&mut self) {
        if self.armed {
            let _ = self.sender.interrupt_if_generation(self.generation);
        }
    }
}
async fn owned_publication<T: Send + 'static>(
    ctx: ProcessorContext,
    interruptible: bool,
    shared: Arc<Shared>,
    future: impl std::future::Future<Output = Result<T>> + Send + 'static,
) -> Result<Option<T>> {
    let (tx, rx) = oneshot::channel();
    let admission = ctx.clone();
    ctx.spawn("moq-frame-publication", interruptible, async move {
        let mut fence = PublicationFence {
            generation: shared.sender.generation(),
            sender: shared.sender.clone(),
            armed: true,
        };
        if interruptible && !admission.is_current() {
            fence.armed = false;
            let _ = tx.send(Ok(None));
            return Ok(());
        }
        let result = future.await;
        fence.armed = false;
        let _ = tx.send(result.map(Some));
        Ok(())
    })
    .map_err(|error| Error::Task(error.message))?;
    // Dropped/aborted descendants discard their converter state. The runtime
    // owns and joins their handles, including during End and failed setup.
    rx.await.unwrap_or(Ok(None))
}
async fn publish_lane(
    shared: Arc<Shared>,
    queue: Arc<queue::Queue>,
    _: ProcessorContext,
) -> Result<()> {
    let mut seen = shared.sender.generation();
    let mut pending: Option<PendingAudio> = None;
    loop {
        match queue.next(&mut seen).await {
            queue::Next::Cancel => return Ok(()),
            queue::Next::End => {
                flush_owned(pending.take(), shared.clone()).await?;
                return Ok(());
            }
            queue::Next::Interrupted(generation) => {
                if let Some(audio) = &mut pending {
                    if audio.stream.interruptible {
                        pending = None;
                    } else {
                        audio.stream.generation = generation;
                    }
                }
            }
            queue::Next::Item(mut item) => {
                if item.frame.interruptible && item.generation != shared.sender.generation() {
                    continue;
                }
                let generation = if item.frame.interruptible {
                    item.generation
                } else {
                    shared.sender.generation()
                };
                let context = item
                    .context
                    .take()
                    .ok_or(Error::InvalidConfig("missing publication context"))?;
                let interruptible = item.frame.interruptible;
                if let Some(audio) = raw_audio(&item.frame) {
                    if audio.audio.is_empty() {
                        continue;
                    }
                    if pending.as_ref().is_some_and(|previous| {
                        !previous.stream.matches(audio, generation, interruptible)
                            || !previous.context.same_causal_scope(&context)
                    }) {
                        flush_owned(pending.take(), shared.clone()).await?;
                    }
                    let (mut stream, origin) = match pending.take() {
                        Some(previous) => (previous.stream, previous.context),
                        None => (
                            audio::Conversion::new(audio, generation, interruptible)?,
                            context.clone(),
                        ),
                    };
                    stream.push(audio)?;
                    if !stream.has_packet() {
                        // Pure conversion has no external effect. Tiny chunks
                        // need not allocate one runtime task per PCM sample;
                        // the retained context fences the eventual publication.
                        pending = Some(PendingAudio {
                            stream,
                            context: origin,
                            latest: context,
                        });
                        #[cfg(test)]
                        shared.audio_processed.send_modify(|n| *n += 1);
                        continue;
                    }
                    let source = context.clone();
                    let connection = shared.clone();
                    pending = owned_publication(
                        origin.clone(),
                        interruptible,
                        shared.clone(),
                        async move {
                            if !write_packets(&mut stream, &connection, &origin, &source).await? {
                                return Ok(None);
                            }
                            Ok(Some(PendingAudio {
                                stream,
                                context: origin,
                                latest: source,
                            }))
                        },
                    )
                    .await?
                    .flatten();
                    #[cfg(test)]
                    shared.audio_processed.send_modify(|n| *n += 1);
                    continue;
                }
                if matches!(
                    item.frame.data,
                    FrameData::TtsText(_) | FrameData::AggregatedTextProgress(_)
                ) {
                    // Publication cannot attest the playback-associated metadata
                    // which generic media output emits on a Played receipt.
                    continue;
                }
                flush_owned(pending.take(), shared.clone()).await?;
                let source = context.clone();
                let connection = shared.clone();
                owned_publication(context, interruptible, shared.clone(), async move {
                    match &item.frame.data {
                        FrameData::OutputTransportMessage(message)
                        | FrameData::OutputTransportMessageUrgent(message) => {
                            let result = timed(
                                connection.config.operation_timeout,
                                "JSON publication",
                                async {
                                    if interruptible {
                                        connection
                                            .sender
                                            .publish_json_for_generation(
                                                message.clone(),
                                                generation,
                                            )
                                            .await
                                    } else {
                                        connection.sender.publish_json(message.clone()).await
                                    }
                                },
                            )
                            .await;
                            match result {
                                Err(Error::Interrupted) => Ok(()),
                                result => result,
                            }
                        }
                        _ => {
                            timed(
                                connection.config.operation_timeout,
                                "pipeline output admission",
                                async {
                                    if matches!(
                                        item.frame.data,
                                        FrameData::Flush(_) | FrameData::FlushDetailed(_)
                                    ) {
                                        if source.has_downstream_processor() {
                                            source.push_downstream_flush_wait(item.frame).await
                                        } else {
                                            source
                                                .push_with_causal_fence(
                                                    item.frame,
                                                    Direction::Downstream,
                                                )
                                                .await
                                        }
                                    } else if stream_data(&item.frame.data) {
                                        source.push_downstream_wait(item.frame).await
                                    } else {
                                        source
                                            .push_with_causal_fence(
                                                item.frame,
                                                Direction::Downstream,
                                            )
                                            .await
                                    }
                                    .map_err(|e| Error::Task(e.message))
                                },
                            )
                            .await
                        }
                    }
                })
                .await?;
            }
        }
    }
}
async fn flush_owned(pending: Option<PendingAudio>, shared: Arc<Shared>) -> Result<()> {
    let Some(mut audio) = pending else {
        return Ok(());
    };
    let connection = shared.clone();
    let context = audio.context.clone();
    owned_publication(context, audio.stream.interruptible, shared, async move {
        if audio.stream.interruptible && audio.stream.generation != connection.sender.generation() {
            return Ok(());
        }
        audio.stream.finish()?;
        write_packets(
            &mut audio.stream,
            &connection,
            &audio.context,
            &audio.latest,
        )
        .await?;
        Ok(())
    })
    .await?;
    Ok(())
}
async fn write_packets(
    stream: &mut audio::Conversion,
    shared: &Shared,
    context: &ProcessorContext,
    latest: &ProcessorContext,
) -> Result<bool> {
    while let Some(packet) = stream.packet() {
        if stream.interruptible && (!context.is_current() || !latest.is_current()) {
            return Ok(false);
        }
        let result = timed(shared.config.operation_timeout, "PCM publication", async {
            loop {
                if !stream.interruptible {
                    stream.generation = shared.sender.generation();
                }
                match shared
                    .sender
                    .publish_audio_for_generation(packet.clone(), stream.generation)
                    .await
                {
                    Err(Error::Interrupted) if !stream.interruptible => continue,
                    result => return result,
                }
            }
        })
        .await;
        match result {
            Err(Error::Interrupted) => return Ok(false),
            result => result?,
        }
    }
    Ok(true)
}
fn stream_data(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::Text(_)
            | FrameData::LlmText(_)
            | FrameData::InputText(_)
            | FrameData::VisionText(_)
            | FrameData::AggregatedText(_)
            | FrameData::TtsTextInput(_)
            | FrameData::TtsAlignmentDiagnostic(_)
            | FrameData::Transcription(_)
            | FrameData::InterimTranscription(_)
            | FrameData::EagerTranscription(_)
            | FrameData::Translation(_)
            | FrameData::InputAudio(_)
            | FrameData::InputImage(_)
            | FrameData::InputFile(_)
            | FrameData::UserFile(_)
            | FrameData::TtsStarted(_)
            | FrameData::TtsStopped(_)
            | FrameData::TtsSpeak(_)
            | FrameData::LlmResponseStart
            | FrameData::LlmResponseEnd
            | FrameData::LlmFullResponseStart(_)
            | FrameData::LlmFullResponseEnd(_)
            | FrameData::LlmMarker(_)
            | FrameData::LlmMarkerResponse(_)
            | FrameData::LlmThoughtStart
            | FrameData::LlmThoughtText(_)
            | FrameData::LlmThoughtEnd
            | FrameData::Metrics(_)
            | FrameData::ServiceMetadata(_)
            | FrameData::SttMetadata(_)
            | FrameData::LlmServiceMetadata(_)
    )
}
#[cfg(test)]
mod tests;
