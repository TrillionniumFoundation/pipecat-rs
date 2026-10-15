//! Session-owned, paced bot PCM transcription. Remote speaking boundaries are
//! deliberately not inputs: system frames can overtake the corresponding PCM.
use crate::EvalError;
use pipecat_core::{Direction, Frame, FrameData, TranscriptionFrame, VadSpeakingFrame};
use pipecat_services::CancellationToken;
use pipecat_speech::{SpeechClient, SpeechConfig, Transcript, TranscriptionRequest};
use pipecat_turns::{
    AggregationEvent, AggregationOutput, LLMUserAggregator, SharedContext, UserAggregatorConfig,
    UserTurnConfig,
};
use pipecat_vad::{SileroVadAnalyzer, VadState};
use std::{
    collections::VecDeque,
    future::Future,
    pin::Pin,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{self, Receiver, SyncSender, TryRecvError, TrySendError},
        Arc, Mutex,
    },
    thread::{self, JoinHandle},
    time::{Duration, Instant},
};
use tokio::sync::mpsc as async_mpsc;

const SAMPLE_RATE: u32 = 16_000;
const BYTES_PER_SECOND: usize = 32_000;
const TICK_BYTES: usize = 1_280;
const TICK: Duration = Duration::from_millis(40);
const PREROLL_BYTES: usize = 9_600;
const COMMAND_CAPACITY: usize = 32;
const OUTPUT_CAPACITY: usize = 16;
const MAX_JSON_BYTES: usize = 64 * 1024;

/// Configuration is consumed by one evaluation session. The caller supplies
/// an already-loaded native Silero model; no model or runtime is downloaded.
/// Only 16 kHz mono little-endian PCM16 and a 400 ms VAD stop are supported.
/// HTTP settings are bounded before constructing the existing speech client,
/// including the response-body limit that precedes JSON parsing.
pub struct BotTranscriptionConfig {
    analyzer: SileroVadAnalyzer,
    speech: SpeechClient,
    request: TranscriptionRequest,
    request_timeout: Duration,
    max_total_pcm_bytes: usize,
    max_buffered_pcm_bytes: usize,
    max_segment_bytes: usize,
    max_pending_segments: usize,
    max_transcript_bytes: usize,
}
impl BotTranscriptionConfig {
    pub fn new(
        analyzer: SileroVadAnalyzer,
        speech: SpeechConfig,
        request: TranscriptionRequest,
    ) -> Result<Self, EvalError> {
        if speech.timeout.is_zero()
            || speech.timeout > Duration::from_secs(30)
            || speech.max_audio_bytes == 0
            || speech.max_audio_bytes > 2 * 1024 * 1024
            || speech.max_json_bytes == 0
            || speech.max_json_bytes > MAX_JSON_BYTES
            || speech.queue_capacity == 0
            || speech.queue_capacity > 32
        {
            return Err(EvalError::Scenario("bot transcription requires HTTP timeout <= 30 s, audio <= 2 MiB, JSON <= 64 KiB and queue capacity <= 32".into()));
        }
        let request_timeout = speech.timeout;
        let speech = SpeechClient::new(speech).map_err(|_| {
            EvalError::Scenario("invalid bot transcription HTTP configuration".into())
        })?;
        let config = Self {
            analyzer,
            speech,
            request,
            request_timeout,
            max_total_pcm_bytes: 8 * 1024 * 1024,
            max_buffered_pcm_bytes: 256 * 1024,
            max_segment_bytes: 30 * BYTES_PER_SECOND,
            max_pending_segments: 4,
            max_transcript_bytes: 64 * 1024,
        };
        config.validate()?;
        Ok(config)
    }
    /// The lifetime admission budget is not replenished by interruptions.
    pub fn with_max_total_pcm_bytes(mut self, bytes: usize) -> Self {
        self.max_total_pcm_bytes = bytes;
        self
    }
    pub fn with_max_buffered_pcm_bytes(mut self, bytes: usize) -> Self {
        self.max_buffered_pcm_bytes = bytes;
        self
    }
    /// Includes preroll and the silence that confirms a local VAD stop.
    pub fn with_max_segment_duration(mut self, duration: Duration) -> Self {
        self.max_segment_bytes = duration
            .as_nanos()
            .checked_mul(BYTES_PER_SECOND as u128)
            .and_then(|value| usize::try_from(value / 1_000_000_000).ok())
            .unwrap_or(usize::MAX);
        self
    }
    /// Waiting segments, in addition to the single active HTTP request.
    pub fn with_max_pending_segments(mut self, count: usize) -> Self {
        self.max_pending_segments = count;
        self
    }
    /// Bounds all accepted transcript text and retained context in an epoch.
    pub fn with_max_transcript_bytes(mut self, bytes: usize) -> Self {
        self.max_transcript_bytes = bytes;
        self
    }
    pub(crate) fn validate(&self) -> Result<(), EvalError> {
        if self.analyzer.sample_rate() != SAMPLE_RATE {
            return Err(EvalError::Unsupported(
                "bot transcription requires 16000 Hz mono PCM".into(),
            ));
        }
        if (self.analyzer.params().stop_secs - 0.4).abs() > 1e-9 {
            return Err(EvalError::Scenario(
                "bot transcription requires VAD stop_secs = 0.4".into(),
            ));
        }
        if self.max_total_pcm_bytes == 0
            || self.max_total_pcm_bytes > 16 * 1024 * 1024
            || self.max_buffered_pcm_bytes == 0
            || self.max_buffered_pcm_bytes > 1024 * 1024
            || self.max_segment_bytes < PREROLL_BYTES + TICK_BYTES
            || self.max_segment_bytes > 30 * BYTES_PER_SECOND
            || !self.max_segment_bytes.is_multiple_of(2)
            || self.max_pending_segments == 0
            || self.max_pending_segments > 16
            || self.max_transcript_bytes == 0
            || self.max_transcript_bytes > 1024 * 1024
        {
            return Err(EvalError::Scenario(
                "invalid bot transcription bounds".into(),
            ));
        }
        if self.request.model.is_empty()
            || self.request.model.len() > 256
            || self
                .request
                .language
                .as_ref()
                .is_some_and(|value| value.len() > 64)
            || self
                .request
                .prompt
                .as_ref()
                .is_some_and(|value| value.len() > 8192)
        {
            return Err(EvalError::Scenario(
                "invalid bot transcription request fields".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
enum Failure {
    Limit(&'static str),
    State(&'static str),
}
impl Failure {
    fn error(self) -> EvalError {
        match self {
            Self::Limit(message) => EvalError::Limit(message),
            Self::State(message) => EvalError::Scenario(message.into()),
        }
    }
}
#[derive(Default)]
struct Activity {
    queued_bytes: usize,
    processing: bool,
    vad_busy: bool,
    tail_unsettled: bool,
    output_events: usize,
}
struct Shared {
    epoch: AtomicU64,
    stop: AtomicBool,
    failure: Mutex<Option<Failure>>,
    activity: Mutex<Activity>,
}
impl Shared {
    fn fail(&self, failure: Failure) {
        let mut stored = self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if stored.is_none() {
            *stored = Some(failure);
        }
        self.stop.store(true, Ordering::Release);
    }
    fn failure(&self) -> Option<Failure> {
        *self
            .failure
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }
    fn release_bytes(&self, bytes: usize) {
        let mut activity = self
            .activity
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        activity.queued_bytes = activity.queued_bytes.saturating_sub(bytes);
    }
    fn received_event(&self) {
        let mut activity = self
            .activity
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        activity.output_events = activity.output_events.saturating_sub(1);
    }
}
struct AudioCommand {
    epoch: u64,
    pcm: Box<[u8]>,
}
enum ThreadEvent {
    Started { epoch: u64 },
    Segment { epoch: u64, pcm: Vec<u8> },
}
struct Segment {
    epoch: u64,
    pcm: Vec<u8>,
}
type HttpFuture = Pin<Box<dyn Future<Output = Result<Transcript, EvalError>> + Send>>;
struct InFlight {
    epoch: u64,
    cancel: CancellationToken,
    future: HttpFuture,
}
impl Drop for InFlight {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Owns exactly one joined native-analysis thread and at most one HTTP future.
/// Cancelling `next` does not detach or discard the in-progress HTTP request.
pub(crate) struct BotTranscription {
    commands: Option<SyncSender<AudioCommand>>,
    events: async_mpsc::Receiver<ThreadEvent>,
    worker: Option<JoinHandle<()>>,
    shared: Arc<Shared>,
    speech: SpeechClient,
    request: TranscriptionRequest,
    request_timeout: Duration,
    turn_config: UserAggregatorConfig,
    aggregator: LLMUserAggregator,
    origin: Instant,
    epoch: u64,
    total_pcm_bytes: usize,
    max_total_pcm_bytes: usize,
    max_buffered_pcm_bytes: usize,
    max_pending_segments: usize,
    max_transcript_bytes: usize,
    transcript_bytes: usize,
    pending: VecDeque<Segment>,
    inflight: Option<InFlight>,
    ready: VecDeque<(u64, String)>,
    local_speaking: bool,
    held_stop: bool,
    turn_active: bool,
    finished: bool,
    idle_notified: bool,
}
impl BotTranscription {
    pub(crate) fn new(config: BotTranscriptionConfig) -> Result<Self, EvalError> {
        config.validate()?;
        let shared = Arc::new(Shared {
            epoch: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            failure: Mutex::new(None),
            activity: Mutex::new(Activity::default()),
        });
        let (commands, input) = mpsc::sync_channel(COMMAND_CAPACITY);
        let (output, events) = async_mpsc::channel(OUTPUT_CAPACITY);
        let thread_shared = shared.clone();
        let max_segment_bytes = config.max_segment_bytes;
        let mut analyzer = config.analyzer;
        // A caller may have probed the supplied analyzer before moving it here.
        analyzer.reset();
        let worker = thread::Builder::new()
            .name("eval-bot-vad".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_worker(analyzer, input, output, &thread_shared, max_segment_bytes);
                }));
                if result.is_err() {
                    thread_shared.fail(Failure::State("bot audio VAD thread panicked"));
                }
            })?;
        let turn_config = aggregator_config(config.request_timeout, config.max_pending_segments);
        Ok(Self {
            commands: Some(commands),
            events,
            worker: Some(worker),
            shared,
            speech: config.speech,
            request: config.request,
            request_timeout: config.request_timeout,
            aggregator: LLMUserAggregator::new(SharedContext::default(), turn_config.clone()),
            turn_config,
            origin: Instant::now(),
            epoch: 0,
            total_pcm_bytes: 0,
            max_total_pcm_bytes: config.max_total_pcm_bytes,
            max_buffered_pcm_bytes: config.max_buffered_pcm_bytes,
            max_pending_segments: config.max_pending_segments,
            max_transcript_bytes: config.max_transcript_bytes,
            transcript_bytes: 0,
            pending: VecDeque::new(),
            inflight: None,
            ready: VecDeque::new(),
            local_speaking: false,
            held_stop: false,
            turn_active: false,
            finished: false,
            idle_notified: false,
        })
    }
    pub(crate) fn ingest(&mut self, pcm: Vec<u8>, rate: u32, epoch: u64) -> Result<(), EvalError> {
        self.check()?;
        // Check ownership before either allocation or budget admission.
        if epoch != self.epoch || epoch != self.shared.epoch.load(Ordering::Acquire) {
            return Ok(());
        }
        if rate != SAMPLE_RATE {
            return Err(EvalError::Unsupported(
                "bot transcription requires 16000 Hz mono PCM".into(),
            ));
        }
        if pcm.is_empty() || !pcm.len().is_multiple_of(2) {
            return Err(EvalError::Scenario(
                "bot transcription requires nonempty whole PCM16 samples".into(),
            ));
        }
        if pcm.len() > BYTES_PER_SECOND {
            return self.fail(Failure::Limit("bot transcription wire PCM frame"));
        }
        let bytes = pcm.len();
        let Some(total) = self
            .total_pcm_bytes
            .checked_add(bytes)
            .filter(|total| *total <= self.max_total_pcm_bytes)
        else {
            return self.fail(Failure::Limit("bot transcription total PCM"));
        };
        let reserved = {
            let mut activity = self
                .shared
                .activity
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if let Some(total) = activity
                .queued_bytes
                .checked_add(bytes)
                .filter(|total| *total <= self.max_buffered_pcm_bytes)
            {
                activity.queued_bytes = total;
                true
            } else {
                false
            }
        };
        if !reserved {
            return self.fail(Failure::Limit("bot transcription buffered PCM"));
        }
        // Boxed slices do not retain an arbitrarily large caller Vec capacity.
        let command = AudioCommand {
            epoch,
            pcm: pcm.into_boxed_slice(),
        };
        match self
            .commands
            .as_ref()
            .expect("checked live engine")
            .try_send(command)
        {
            Ok(()) => {
                self.total_pcm_bytes = total;
                self.idle_notified = false;
                self.wake();
                Ok(())
            }
            Err(error) => {
                self.shared.release_bytes(bytes);
                self.fail(match error {
                    TrySendError::Full(_) => Failure::Limit("bot transcription command queue"),
                    TrySendError::Disconnected(_) => {
                        Failure::State("bot audio VAD thread disconnected")
                    }
                })
            }
        }
    }
    /// Epochs must be monotonic. This cancels old HTTP immediately; the native
    /// worker observes the same atomic fence before analysis and publication.
    pub(crate) fn set_epoch(&mut self, epoch: u64) -> Result<(), EvalError> {
        self.check()?;
        if epoch < self.epoch {
            return Err(EvalError::Scenario(
                "bot transcription epoch moved backwards".into(),
            ));
        }
        if epoch == self.epoch {
            return Ok(());
        }
        self.shared.epoch.store(epoch, Ordering::Release);
        self.epoch = epoch;
        self.idle_notified = false;
        self.inflight = None;
        self.pending.clear();
        self.ready.clear();
        self.local_speaking = false;
        self.held_stop = false;
        self.turn_active = false;
        self.transcript_bytes = 0;
        self.aggregator =
            LLMUserAggregator::new(SharedContext::default(), self.turn_config.clone());
        while self.events.try_recv().is_ok() {
            self.shared.received_event();
        }
        self.wake();
        Ok(())
    }
    /// Arm a control wake for the next settled state without consuming work.
    /// The session can then await pump without a polling timeout that cancels
    /// a partially committed transcript.
    pub(crate) fn request_idle_notification(&mut self) {
        self.idle_notified = false;
    }
    pub(crate) fn is_idle(&self) -> bool {
        if self.finished || self.shared.failure().is_some() {
            return false;
        }
        let activity = self
            .shared
            .activity
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        activity.queued_bytes == 0
            && !activity.processing
            && !activity.vad_busy
            && !activity.tail_unsettled
            && activity.output_events == 0
            && self.pending.is_empty()
            && self.inflight.is_none()
            && self.ready.is_empty()
            && !self.local_speaking
            && !self.held_stop
            && !self.turn_active
    }
    pub(crate) async fn next(&mut self) -> Result<(u64, String), EvalError> {
        let mut ticks = tokio::time::interval(Duration::from_millis(20));
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            self.check()?;
            if let Some((epoch, text)) = self.ready.pop_front() {
                if epoch == self.epoch && epoch == self.shared.epoch.load(Ordering::Acquire) {
                    return Ok((epoch, text));
                }
                continue;
            }
            if !self.idle_notified && self.is_idle() {
                self.idle_notified = true;
                // A control notification for the session's ordinary-send
                // settling wait. It is never a transcript/response event.
                return Ok((self.epoch, String::new()));
            }
            self.start_request();
            tokio::select! {
                biased;
                event = self.events.recv() => {
                    let Some(event) = event else {
                        self.check()?;
                        return self.fail(Failure::State("bot audio VAD thread disconnected"));
                    };
                    self.shared.received_event();
                    self.process_event(event)?;
                }
                result = async { self.inflight.as_mut().expect("guarded HTTP future").future.as_mut().await }, if self.inflight.is_some() => {
                    let flight = self.inflight.take().expect("completed HTTP future");
                    if flight.epoch == self.epoch && flight.epoch == self.shared.epoch.load(Ordering::Acquire) {
                        let transcript = match result {
                            Ok(transcript) => transcript,
                            Err(error) => {
                                self.shared.fail(Failure::State("bot audio transcription request failed"));
                                self.wake();
                                return Err(error);
                            }
                        };
                        self.commit_transcript(flight.epoch, transcript.text)?;
                    }
                }
                _ = ticks.tick() => {
                    let output = self.aggregator.advance(self.origin.elapsed())
                        .map_err(|_| EvalError::Scenario("bot transcript turn aggregation failed".into()))?;
                    self.collect(output)?;
                }
            }
        }
    }
    fn process_event(&mut self, event: ThreadEvent) -> Result<(), EvalError> {
        match event {
            ThreadEvent::Started { epoch } if self.current(epoch) => {
                self.idle_notified = false;
                self.local_speaking = true;
                self.held_stop = false;
                self.process_frame(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                    duration_secs: 0.0,
                    timestamp: 0.0,
                }))?;
            }
            ThreadEvent::Segment { epoch, pcm } if self.current(epoch) => {
                self.local_speaking = false;
                self.held_stop = true;
                if self.pending.len() >= self.max_pending_segments {
                    return self.fail(Failure::Limit("bot transcription pending segments"));
                }
                self.pending.push_back(Segment { epoch, pcm });
            }
            _ => {}
        }
        Ok(())
    }
    fn start_request(&mut self) {
        if self.inflight.is_some() {
            return;
        }
        while let Some(segment) = self.pending.pop_front() {
            if !self.current(segment.epoch) {
                continue;
            }
            let speech = self.speech.clone();
            let request = self.request.clone();
            let timeout = self.request_timeout;
            let cancel = CancellationToken::new();
            let request_cancel = cancel.clone();
            let future = Box::pin(async move {
                match tokio::time::timeout(
                    timeout,
                    speech.transcribe_pcm(&segment.pcm, SAMPLE_RATE, 1, request, request_cancel),
                )
                .await
                {
                    Ok(Ok(transcript)) => Ok(transcript),
                    Ok(Err(pipecat_speech::SpeechError::Timeout)) | Err(_) => {
                        Err(EvalError::Timeout("bot transcription HTTP"))
                    }
                    Ok(Err(_)) => Err(EvalError::Scenario("bot transcription HTTP failed".into())),
                }
            });
            self.inflight = Some(InFlight {
                epoch: segment.epoch,
                cancel,
                future,
            });
            break;
        }
    }
    fn commit_transcript(&mut self, epoch: u64, text: String) -> Result<(), EvalError> {
        if !self.current(epoch) {
            return Ok(());
        }
        let Some(bytes) = self
            .transcript_bytes
            .checked_add(text.len())
            .and_then(|bytes| bytes.checked_add(1))
            .filter(|bytes| *bytes <= self.max_transcript_bytes)
        else {
            return self.fail(Failure::Limit("bot transcription transcript bytes"));
        };
        self.transcript_bytes = bytes;
        // These are provider-final transcripts, never synthesized interim text.
        // Hold the latest local stop while serial STT is outstanding: otherwise
        // an older segment's final could close a later segment prematurely.
        self.process_frame(FrameData::Transcription(TranscriptionFrame {
            text,
            finalized: true,
            ..TranscriptionFrame::default()
        }))?;
        if self.current(epoch)
            && !self.local_speaking
            && self.held_stop
            && self.pending.is_empty()
            && self.inflight.is_none()
        {
            self.held_stop = false;
            self.process_frame(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
                duration_secs: 0.4,
                timestamp: 0.0,
            }))?;
        }
        Ok(())
    }
    fn process_frame(&mut self, data: FrameData) -> Result<(), EvalError> {
        if !self.current(self.epoch) {
            return Ok(());
        }
        let output = self
            .aggregator
            .process_at(
                Frame::new(data),
                Direction::Downstream,
                self.origin.elapsed(),
            )
            .map_err(|_| EvalError::Scenario("bot transcript turn aggregation failed".into()))?;
        self.collect(output)
    }
    fn collect(&mut self, output: AggregationOutput) -> Result<(), EvalError> {
        if !self.current(self.epoch) {
            return Ok(());
        }
        for event in output.events {
            match event {
                AggregationEvent::UserTurnStarted => self.turn_active = true,
                AggregationEvent::UserTurnStopped { text, .. } => {
                    self.turn_active = false;
                    if !text.trim().is_empty() {
                        if self.ready.len() >= OUTPUT_CAPACITY
                            || text.len() > self.max_transcript_bytes
                        {
                            return self.fail(Failure::Limit("bot transcription completed turns"));
                        }
                        self.ready.push_back((self.epoch, text));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
    fn current(&self, epoch: u64) -> bool {
        epoch == self.epoch
            && epoch == self.shared.epoch.load(Ordering::Acquire)
            && !self.shared.stop.load(Ordering::Acquire)
    }
    fn check(&self) -> Result<(), EvalError> {
        if let Some(error) = self.shared.failure() {
            return Err(error.error());
        }
        if self.finished || self.shared.stop.load(Ordering::Acquire) {
            return Err(EvalError::Cancelled);
        }
        Ok(())
    }
    fn fail<T>(&mut self, failure: Failure) -> Result<T, EvalError> {
        self.shared.fail(failure);
        self.inflight = None;
        self.pending.clear();
        self.wake();
        Err(failure.error())
    }
    fn wake(&self) {
        if let Some(worker) = &self.worker {
            worker.thread().unpark();
        }
    }
    /// Cancels HTTP and actually joins the native thread. There is no detached
    /// blocking task and no join handle is moved across a cancellable await.
    /// A 40 ms audio batch bounds inference work, not inference wall time.
    pub(crate) fn finish(&mut self) -> Result<(), EvalError> {
        self.shared.stop.store(true, Ordering::Release);
        self.inflight = None;
        self.pending.clear();
        self.ready.clear();
        self.commands = None;
        self.events.close();
        self.wake();
        if let Some(worker) = self.worker.take() {
            if worker.join().is_err() {
                self.shared
                    .fail(Failure::State("bot audio VAD thread panicked"));
            }
        }
        self.finished = true;
        if let Some(error) = self.shared.failure() {
            Err(error.error())
        } else {
            Ok(())
        }
    }
}
impl Drop for BotTranscription {
    fn drop(&mut self) {
        let _ = self.finish();
    }
}

fn aggregator_config(
    request_timeout: Duration,
    max_pending_segments: usize,
) -> UserAggregatorConfig {
    // The local stop remains withheld while any serial HTTP receipt is missing.
    // Keep the watchdog conservative as well, even though audible/local-open
    // speech cannot be ended by the controller's watchdog.
    let stt_bound = request_timeout
        .saturating_mul((max_pending_segments + 1) as u32)
        .saturating_add(Duration::from_secs(1));
    UserAggregatorConfig {
        turn: UserTurnConfig {
            speech_timeout: Duration::from_millis(600),
            stt_timeout: Duration::ZERO,
            vad_stop_duration: Duration::from_millis(400),
            stop_timeout: stt_bound.saturating_add(Duration::from_secs(1)),
            enable_interruptions: false,
            wait_for_transcript: false,
            ..UserTurnConfig::default()
        },
        empty_user_turn: None,
        ..UserAggregatorConfig::default()
    }
}

/// Stores bounded preroll, and emits only a Speaking -> Quiet segment. Brief
/// Starting noise never yields a segment; Stopping remains inside the segment.
struct Segmenter {
    preroll: VecDeque<u8>,
    segment: Option<Vec<u8>>,
    maximum: usize,
}
impl Segmenter {
    fn new(maximum: usize) -> Self {
        Self {
            preroll: VecDeque::new(),
            segment: None,
            maximum,
        }
    }
    fn reset(&mut self) {
        self.preroll.clear();
        self.segment = None;
    }
    fn push(
        &mut self,
        block: &[u8; TICK_BYTES],
        state: VadState,
    ) -> Result<(bool, Option<Vec<u8>>), Failure> {
        let started = state == VadState::Speaking && self.segment.is_none();
        if started {
            self.segment = Some(self.preroll.iter().copied().collect());
        }
        if let Some(segment) = &mut self.segment {
            if segment
                .len()
                .checked_add(block.len())
                .is_none_or(|size| size > self.maximum)
            {
                return Err(Failure::Limit("bot transcription VAD segment"));
            }
            segment.extend_from_slice(block);
        }
        let completed = if state == VadState::Quiet {
            self.segment.take()
        } else {
            None
        };
        self.preroll.extend(block);
        if self.preroll.len() > PREROLL_BYTES {
            self.preroll.drain(..self.preroll.len() - PREROLL_BYTES);
        }
        Ok((started, completed))
    }
}
fn publish(
    output: &async_mpsc::Sender<ThreadEvent>,
    shared: &Shared,
    epoch: u64,
    event: ThreadEvent,
) -> bool {
    if shared.stop.load(Ordering::Acquire) || shared.epoch.load(Ordering::Acquire) != epoch {
        return false;
    }
    {
        let mut activity = shared
            .activity
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        activity.output_events += 1;
    }
    match output.try_send(event) {
        Ok(()) => true,
        Err(error) => {
            shared.received_event();
            if !shared.stop.load(Ordering::Acquire) {
                shared.fail(match error {
                    async_mpsc::error::TrySendError::Full(_) => {
                        Failure::Limit("bot transcription VAD output queue")
                    }
                    async_mpsc::error::TrySendError::Closed(_) => {
                        Failure::State("bot transcription consumer closed")
                    }
                });
            }
            false
        }
    }
}
fn run_worker(
    mut analyzer: SileroVadAnalyzer,
    input: Receiver<AudioCommand>,
    output: async_mpsc::Sender<ThreadEvent>,
    shared: &Shared,
    max_segment_bytes: usize,
) {
    let origin = Instant::now();
    let mut next_tick = origin;
    let mut epoch = shared.epoch.load(Ordering::Acquire);
    let mut buffered = VecDeque::new();
    let mut segmenter = Segmenter::new(max_segment_bytes);
    while !shared.stop.load(Ordering::Acquire) {
        let current = shared.epoch.load(Ordering::Acquire);
        if current != epoch {
            shared.release_bytes(buffered.len());
            buffered.clear();
            analyzer.reset();
            segmenter.reset();
            epoch = current;
            {
                let mut activity = shared
                    .activity
                    .lock()
                    .unwrap_or_else(|error| error.into_inner());
                activity.vad_busy = false;
                activity.tail_unsettled = false;
            }
        }
        // Bound each drain pass even when the producer continuously refills it.
        for _ in 0..COMMAND_CAPACITY {
            match input.try_recv() {
                Ok(command) => {
                    // An epoch can change while a command is received. Reset
                    // before classifying it, so fresh audio is never discarded
                    // merely because the worker still had the prior epoch.
                    let current = shared.epoch.load(Ordering::Acquire);
                    if current != epoch {
                        shared.release_bytes(buffered.len());
                        buffered.clear();
                        analyzer.reset();
                        segmenter.reset();
                        epoch = current;
                        {
                            let mut activity = shared
                                .activity
                                .lock()
                                .unwrap_or_else(|error| error.into_inner());
                            activity.vad_busy = false;
                            activity.tail_unsettled = false;
                        }
                    }
                    if command.epoch == epoch && epoch == shared.epoch.load(Ordering::Acquire) {
                        buffered.extend(command.pcm.iter().copied());
                    } else {
                        shared.release_bytes(command.pcm.len());
                    }
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => return,
            }
        }
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        if epoch != shared.epoch.load(Ordering::Acquire) {
            continue;
        }
        let now = Instant::now();
        if now < next_tick {
            thread::park_timeout(next_tick - now);
            continue;
        }
        // Schedule from this real start, never replay missed ticks in a burst.
        next_tick = now + TICK;
        shared
            .activity
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .processing = true;
        let mut block = [0; TICK_BYTES];
        let consumed = buffered.len().min(TICK_BYTES);
        for byte in &mut block[..consumed] {
            *byte = buffered.pop_front().expect("bounded PCM consumption");
        }
        shared.release_bytes(consumed);
        let analysis = analyzer.analyze_at(&block, origin.elapsed());
        if shared.stop.load(Ordering::Acquire) {
            break;
        }
        if epoch != shared.epoch.load(Ordering::Acquire) {
            shared
                .activity
                .lock()
                .unwrap_or_else(|error| error.into_inner())
                .processing = false;
            continue;
        }
        let analysis = match analysis {
            Ok(analysis) => analysis,
            Err(_) => {
                shared.fail(Failure::State("bot audio native VAD inference failed"));
                break;
            }
        };
        let (started, completed) = match segmenter.push(&block, analysis.state) {
            Ok(events) => events,
            Err(error) => {
                shared.fail(error);
                break;
            }
        };
        if started
            && !publish(&output, shared, epoch, ThreadEvent::Started { epoch })
            && shared.stop.load(Ordering::Acquire)
        {
            break;
        }
        if let Some(pcm) = completed {
            if !publish(&output, shared, epoch, ThreadEvent::Segment { epoch, pcm })
                && shared.stop.load(Ordering::Acquire)
            {
                break;
            }
        }
        let mut activity = shared
            .activity
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        activity.vad_busy = analysis.state != VadState::Quiet;
        if consumed > 0 {
            activity.tail_unsettled = true;
        } else if analysis.state == VadState::Quiet {
            // One subsequent full silence tick flushes any actual PCM left
            // inside the analyzer's incomplete 32 ms model window.
            activity.tail_unsettled = false;
        }
        activity.processing = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segmentation_retains_bounded_preroll_and_waits_for_quiet() {
        let mut segmenter = Segmenter::new(30 * BYTES_PER_SECOND);
        for _ in 0..100 {
            let (started, completed) = segmenter.push(&[1; TICK_BYTES], VadState::Quiet).unwrap();
            assert!(!started && completed.is_none());
        }
        assert_eq!(segmenter.preroll.len(), PREROLL_BYTES);
        assert!(
            !segmenter
                .push(&[2; TICK_BYTES], VadState::Starting)
                .unwrap()
                .0
        );
        let (started, completed) = segmenter
            .push(&[3; TICK_BYTES], VadState::Speaking)
            .unwrap();
        assert!(started && completed.is_none());
        assert!(segmenter
            .push(&[4; TICK_BYTES], VadState::Stopping)
            .unwrap()
            .1
            .is_none());
        let (started, completed) = segmenter.push(&[0; TICK_BYTES], VadState::Quiet).unwrap();
        assert!(!started);
        let pcm = completed.unwrap();
        assert_eq!(pcm.len(), PREROLL_BYTES + 3 * TICK_BYTES);
        assert_eq!(
            &pcm[PREROLL_BYTES..PREROLL_BYTES + TICK_BYTES],
            &[3; TICK_BYTES]
        );
    }
    #[test]
    fn segment_limit_fails_closed_and_epoch_reset_discards_old_audio() {
        let mut segmenter = Segmenter::new(TICK_BYTES);
        segmenter
            .push(&[7; TICK_BYTES], VadState::Speaking)
            .unwrap();
        assert!(matches!(
            segmenter.push(&[8; TICK_BYTES], VadState::Speaking),
            Err(Failure::Limit(_))
        ));
        segmenter.reset();
        let (_, completed) = segmenter.push(&[0; TICK_BYTES], VadState::Quiet).unwrap();
        assert!(completed.is_none());
        assert!(segmenter.segment.is_none());
    }
    #[test]
    fn a_full_output_queue_sets_out_of_band_failure_without_blocking() {
        let shared = Shared {
            epoch: AtomicU64::new(3),
            stop: AtomicBool::new(false),
            failure: Mutex::new(None),
            activity: Mutex::new(Activity::default()),
        };
        let (output, _input) = async_mpsc::channel(1);
        assert!(!publish(
            &output,
            &shared,
            2,
            ThreadEvent::Started { epoch: 2 }
        ));
        assert!(publish(
            &output,
            &shared,
            3,
            ThreadEvent::Started { epoch: 3 }
        ));
        assert!(!publish(
            &output,
            &shared,
            3,
            ThreadEvent::Started { epoch: 3 }
        ));
        assert!(matches!(shared.failure(), Some(Failure::Limit(_))));
        assert_eq!(shared.activity.lock().unwrap().output_events, 1);
    }

    fn test_engine() -> (
        BotTranscription,
        async_mpsc::Sender<ThreadEvent>,
        Receiver<AudioCommand>,
    ) {
        let (commands, input) = mpsc::sync_channel(COMMAND_CAPACITY);
        let (output, events) = async_mpsc::channel(OUTPUT_CAPACITY);
        let shared = Arc::new(Shared {
            epoch: AtomicU64::new(0),
            stop: AtomicBool::new(false),
            failure: Mutex::new(None),
            activity: Mutex::new(Activity::default()),
        });
        let turn_config = aggregator_config(Duration::from_secs(30), 4);
        let mut speech = SpeechConfig::openai("local-test-key");
        speech.base_url = "http://127.0.0.1:1/v1".into();
        let engine = BotTranscription {
            commands: Some(commands),
            events,
            worker: None,
            shared,
            speech: SpeechClient::new(speech).unwrap(),
            request: TranscriptionRequest::default(),
            request_timeout: Duration::from_secs(30),
            aggregator: LLMUserAggregator::new(SharedContext::default(), turn_config.clone()),
            turn_config,
            origin: Instant::now(),
            epoch: 0,
            total_pcm_bytes: 0,
            max_total_pcm_bytes: 8 * 1024 * 1024,
            max_buffered_pcm_bytes: 256 * 1024,
            max_pending_segments: 4,
            max_transcript_bytes: 64 * 1024,
            transcript_bytes: 0,
            pending: VecDeque::new(),
            inflight: None,
            ready: VecDeque::new(),
            local_speaking: false,
            held_stop: false,
            turn_active: false,
            finished: false,
            idle_notified: false,
        };
        (engine, output, input)
    }

    #[tokio::test]
    async fn delayed_first_final_cannot_close_second_untranscribed_segment() {
        let (mut engine, _output, _input) = test_engine();
        engine.turn_config = aggregator_config(Duration::from_millis(10), 4);
        assert!(engine.turn_config.turn.stop_timeout < Duration::from_secs(3));
        engine.aggregator =
            LLMUserAggregator::new(SharedContext::default(), engine.turn_config.clone());
        engine
            .process_event(ThreadEvent::Started { epoch: 0 })
            .unwrap();
        engine
            .process_event(ThreadEvent::Segment {
                epoch: 0,
                pcm: vec![1, 0],
            })
            .unwrap();
        let first = engine.pending.pop_front().unwrap();
        // The second start is observed before the first HTTP completion.
        // This must reuse, rather than replace, the active aggregator.
        engine
            .process_event(ThreadEvent::Started { epoch: 0 })
            .unwrap();
        assert!(!engine.held_stop);
        engine
            .process_event(ThreadEvent::Segment {
                epoch: 0,
                pcm: vec![2, 0],
            })
            .unwrap();
        engine
            .commit_transcript(first.epoch, "first segment".into())
            .unwrap();
        let output = engine.aggregator.advance(Duration::from_secs(200)).unwrap();
        engine.collect(output).unwrap();
        assert!(engine.turn_active);
        assert!(engine.ready.is_empty());
        assert!(engine.held_stop);
        let second = engine.pending.pop_front().unwrap();
        engine
            .commit_transcript(second.epoch, "second segment".into())
            .unwrap();
        let output = engine.aggregator.advance(Duration::from_secs(201)).unwrap();
        engine.collect(output).unwrap();
        assert!(!engine.turn_active);
        assert_eq!(
            engine.ready.pop_front(),
            Some((0, "first segment second segment".into()))
        );
        assert!(engine.ready.is_empty());
    }

    #[tokio::test]
    async fn empty_http_receipt_closes_real_turn_without_a_fake_response() {
        let (mut engine, _output, _input) = test_engine();
        engine
            .process_event(ThreadEvent::Started { epoch: 0 })
            .unwrap();
        engine
            .process_event(ThreadEvent::Segment {
                epoch: 0,
                pcm: vec![1, 0],
            })
            .unwrap();
        engine.pending.pop_front().unwrap();
        engine.commit_transcript(0, String::new()).unwrap();
        assert!(!engine.held_stop);
        let output = engine.aggregator.advance(Duration::from_secs(1)).unwrap();
        engine.collect(output).unwrap();
        assert!(!engine.turn_active);
        assert!(engine.ready.is_empty());
        assert!(engine.is_idle());
        assert_eq!(engine.next().await.unwrap(), (0, String::new()));
        assert!(
            tokio::time::timeout(Duration::from_millis(5), engine.next())
                .await
                .is_err()
        );
    }

    #[tokio::test]
    async fn epoch_cancels_owned_http_and_rejects_every_stale_path() {
        let (mut engine, _output, _input) = test_engine();
        let cancel = CancellationToken::new();
        engine.inflight = Some(InFlight {
            epoch: 0,
            cancel: cancel.clone(),
            future: Box::pin(std::future::pending()),
        });
        engine
            .process_event(ThreadEvent::Started { epoch: 0 })
            .unwrap();
        engine.set_epoch(1).unwrap();
        assert!(cancel.is_cancelled());
        engine.ingest(vec![1, 0], SAMPLE_RATE, 0).unwrap();
        assert_eq!(engine.total_pcm_bytes, 0);
        engine
            .process_event(ThreadEvent::Started { epoch: 0 })
            .unwrap();
        engine
            .process_event(ThreadEvent::Segment {
                epoch: 0,
                pcm: vec![1, 0],
            })
            .unwrap();
        engine
            .commit_transcript(0, "stale transcript".into())
            .unwrap();
        assert!(!engine.turn_active);
        assert!(engine.pending.is_empty());
        assert!(engine.ready.is_empty());
        assert_eq!(engine.transcript_bytes, 0);
        assert!(engine.set_epoch(0).is_err());
    }

    #[tokio::test]
    async fn unsettled_model_window_prevents_idle_after_last_pcm_bytes() {
        let (mut engine, _output, _input) = test_engine();
        engine.shared.activity.lock().unwrap().tail_unsettled = true;
        assert!(!engine.is_idle());
        engine.shared.activity.lock().unwrap().tail_unsettled = false;
        assert!(engine.is_idle());
        assert_eq!(engine.next().await.unwrap(), (0, String::new()));
    }

    #[tokio::test]
    async fn finish_cancels_http_and_joins_the_owned_thread() {
        let (mut engine, _output, _input) = test_engine();
        let exited = Arc::new(AtomicBool::new(false));
        let thread_exited = exited.clone();
        let shared = engine.shared.clone();
        engine.worker = Some(thread::spawn(move || {
            while !shared.stop.load(Ordering::Acquire) {
                thread::park_timeout(TICK);
            }
            thread_exited.store(true, Ordering::Release);
        }));
        let cancel = CancellationToken::new();
        engine.inflight = Some(InFlight {
            epoch: 0,
            cancel: cancel.clone(),
            future: Box::pin(std::future::pending()),
        });
        engine.finish().unwrap();
        assert!(cancel.is_cancelled());
        assert!(engine.inflight.is_none());
        assert!(engine.worker.is_none());
        assert!(exited.load(Ordering::Acquire));
    }

    #[tokio::test]
    async fn failed_admission_does_not_consume_the_lifetime_or_buffer_budget() {
        let (mut engine, _output, _input) = test_engine();
        engine.max_total_pcm_bytes = 4;
        engine.ingest(vec![1, 0], SAMPLE_RATE, 0).unwrap();
        assert!(matches!(
            engine.ingest(vec![2, 0, 3, 0], SAMPLE_RATE, 0),
            Err(EvalError::Limit(_))
        ));
        assert_eq!(engine.total_pcm_bytes, 2);
        assert_eq!(engine.shared.activity.lock().unwrap().queued_bytes, 2);

        let (mut engine, _output, _input) = test_engine();
        for _ in 0..COMMAND_CAPACITY {
            engine.ingest(vec![1, 0], SAMPLE_RATE, 0).unwrap();
        }
        assert!(matches!(
            engine.ingest(vec![1, 0], SAMPLE_RATE, 0),
            Err(EvalError::Limit("bot transcription command queue"))
        ));
        assert_eq!(engine.total_pcm_bytes, COMMAND_CAPACITY * 2);
        assert_eq!(
            engine.shared.activity.lock().unwrap().queued_bytes,
            COMMAND_CAPACITY * 2
        );
    }

    #[tokio::test]
    async fn cancellation_of_next_preserves_the_owned_http_until_finish() {
        let (mut engine, _output, _input) = test_engine();
        let cancel = CancellationToken::new();
        engine.inflight = Some(InFlight {
            epoch: 0,
            cancel: cancel.clone(),
            future: Box::pin(std::future::pending()),
        });
        assert!(
            tokio::time::timeout(Duration::from_millis(5), engine.next())
                .await
                .is_err()
        );
        assert!(engine.inflight.is_some());
        assert!(!cancel.is_cancelled());
        engine.finish().unwrap();
        assert!(cancel.is_cancelled());
    }
    #[tokio::test]
    async fn rearmed_idle_notification_preserves_completed_response_priority() {
        let (mut engine, _output, _input) = test_engine();
        engine.idle_notified = true;
        engine.ready.push_back((0, "completed response".into()));
        engine.request_idle_notification();
        assert_eq!(engine.ready.len(), 1);
        assert_eq!(
            engine.next().await.unwrap(),
            (0, "completed response".into())
        );
        assert_eq!(engine.next().await.unwrap(), (0, String::new()));
        assert!(engine.idle_notified);
        engine.finish().unwrap();
    }
}
