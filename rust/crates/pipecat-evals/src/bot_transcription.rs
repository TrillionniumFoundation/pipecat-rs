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
use pipecat_vad::{
    EndOfTurnState, SileroVadAnalyzer, SmartTurnAnalysis, SmartTurnAnalyzer, SmartTurnCancellation,
    SmartTurnCompletionReason, SmartTurnParams, VadError, VadState,
};
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
    smart_turn: Option<SmartTurnAnalyzer>,
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
            smart_turn: None,
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
    /// Opt into locally paced Smart Turn endpointing using an already-loaded
    /// analyzer. Only the bounded 16 kHz, 8 s model window, 500 ms preroll and
    /// 3 s silence policy are supported. No model or runtime is downloaded.
    /// Inference deadlines are disabled: only a model COMPLETE verdict or the
    /// analyzer's sample-counted silence fallback may release a local stop.
    pub fn with_smart_turn(mut self, mut analyzer: SmartTurnAnalyzer) -> Result<Self, EvalError> {
        validate_smart_turn(&analyzer)?;
        analyzer.set_inference_timeout(None).map_err(|_| {
            EvalError::Scenario("invalid bot transcription Smart Turn timeout".into())
        })?;
        analyzer
            .update_vad_start_secs(self.analyzer.params().start_secs)
            .map_err(|_| EvalError::Scenario("invalid bot transcription VAD start delay".into()))?;
        analyzer.clear();
        self.smart_turn = Some(analyzer);
        self.validate()?;
        Ok(self)
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
        if let Some(analyzer) = &self.smart_turn {
            validate_smart_turn(analyzer)?;
        }
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

fn validate_smart_turn(analyzer: &SmartTurnAnalyzer) -> Result<(), EvalError> {
    if analyzer.buffer().sample_rate() != SAMPLE_RATE
        || analyzer.buffer().params() != &SmartTurnParams::default()
    {
        return Err(EvalError::Scenario(
            "bot transcription Smart Turn requires 16000 Hz, an 8 s window, 500 ms preroll and 3 s silence fallback".into(),
        ));
    }
    Ok(())
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
    Started {
        epoch: u64,
    },
    Pcm {
        epoch: u64,
        pcm: Box<[u8; TICK_BYTES]>,
        is_speech: bool,
    },
    Segment {
        epoch: u64,
        pcm: Vec<u8>,
    },
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

type EndpointFuture = Pin<Box<dyn Future<Output = pipecat_vad::Result<SmartTurnAnalysis>> + Send>>;
struct EndpointInFlight {
    epoch: u64,
    revision: u64,
    cancel: SmartTurnCancellation,
    future: EndpointFuture,
}
impl Drop for EndpointInFlight {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}
struct Endpoint {
    analyzer: SmartTurnAnalyzer,
    pending: Option<EndpointInFlight>,
    requested: bool,
    complete: bool,
    revision: u64,
}
impl Endpoint {
    fn new(analyzer: SmartTurnAnalyzer) -> Self {
        Self {
            analyzer,
            pending: None,
            requested: false,
            complete: false,
            revision: 0,
        }
    }
    fn invalidate(&mut self) {
        self.revision = self.revision.wrapping_add(1);
        self.requested = false;
        self.complete = false;
        if let Some(pending) = &self.pending {
            pending.cancel.cancel();
        }
    }
    fn clear(&mut self) {
        self.invalidate();
        self.analyzer.clear();
    }
}

/// Owns one joined VAD thread, at most one HTTP future and an optional joined
/// Smart Turn worker. Cancelling `next` preserves both owned in-progress futures.
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
    endpoint: Option<Endpoint>,
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
        let smart_turn_enabled = config.smart_turn.is_some();
        let mut analyzer = config.analyzer;
        // A caller may have probed the supplied analyzer before moving it here.
        analyzer.reset();
        let worker = thread::Builder::new()
            .name("eval-bot-vad".into())
            .spawn(move || {
                let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    run_worker(
                        analyzer,
                        input,
                        output,
                        &thread_shared,
                        max_segment_bytes,
                        smart_turn_enabled,
                    );
                }));
                if result.is_err() {
                    thread_shared.fail(Failure::State("bot audio VAD thread panicked"));
                }
            })?;
        let mut turn_config =
            aggregator_config(config.request_timeout, config.max_pending_segments);
        if smart_turn_enabled {
            // The endpoint gate holds the local stop, so no second fixed delay
            // should be added after both the model and HTTP receipts are ready.
            turn_config.turn.speech_timeout = Duration::ZERO;
        }
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
            endpoint: config.smart_turn.map(Endpoint::new),
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
        if let Some(endpoint) = &mut self.endpoint {
            endpoint.clear();
            // Retain the canceled old-epoch receipt until the native predictor
            // exits. A fresh stop can request the next snapshot meanwhile,
            // without filling the analyzer's single waiting-request slot.
        }
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
            && self.endpoint.as_ref().is_none_or(|endpoint| {
                endpoint.pending.is_none() && !endpoint.requested && !endpoint.complete
            })
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
            self.release_held_stop()?;
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
            self.start_analysis()?;
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
                result = async {
                    self.endpoint.as_mut().expect("guarded endpoint")
                        .pending.as_mut().expect("guarded analysis").future.as_mut().await
                }, if self.endpoint.as_ref().is_some_and(|endpoint| endpoint.pending.is_some()) => {
                    let pending = self.endpoint.as_mut().expect("completed endpoint")
                        .pending.take().expect("completed analysis");
                    // Events admitted between select's event poll and the ready
                    // prediction still have priority. Started invalidates even
                    // before its matching PCM can update analyzer generation.
                    self.drain_events()?;
                    self.apply_analysis(pending.epoch, pending.revision, result)?;
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
                if let Some(endpoint) = &mut self.endpoint {
                    // Retain the utterance buffer on resumed speech and retain
                    // its canceled future until the old native job resolves.
                    endpoint.invalidate();
                }
                self.process_frame(FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
                    duration_secs: 0.0,
                    timestamp: 0.0,
                }))?;
            }
            ThreadEvent::Pcm {
                epoch,
                pcm,
                is_speech,
            } if self.current(epoch) => {
                if let Some(endpoint) = &mut self.endpoint {
                    let state = match endpoint.analyzer.append_audio(pcm.as_ref(), is_speech) {
                        Ok(state) => state,
                        Err(_) => {
                            return self
                                .fail(Failure::State("bot audio Smart Turn buffering failed"));
                        }
                    };
                    if state == EndOfTurnState::Complete {
                        // append_audio has already cleared the buffer and
                        // canceled its old generation. Keep this verdict while
                        // HTTP is pending; never analyze the now-empty buffer.
                        endpoint.complete = true;
                        endpoint.requested = false;
                    }
                }
            }
            ThreadEvent::Segment { epoch, pcm } if self.current(epoch) => {
                self.local_speaking = false;
                self.held_stop = true;
                if self.pending.len() >= self.max_pending_segments {
                    return self.fail(Failure::Limit("bot transcription pending segments"));
                }
                self.pending.push_back(Segment { epoch, pcm });
                if let Some(endpoint) = &mut self.endpoint {
                    endpoint.requested = !endpoint.complete;
                }
            }
            _ => {}
        }
        Ok(())
    }
    fn drain_events(&mut self) -> Result<(), EvalError> {
        // A bounded pass cannot be starved by a continuously publishing worker.
        // The release fence below refuses to commit while any event remains.
        for _ in 0..OUTPUT_CAPACITY {
            match self.events.try_recv() {
                Ok(event) => {
                    self.shared.received_event();
                    self.process_event(event)?;
                }
                Err(async_mpsc::error::TryRecvError::Empty) => break,
                Err(async_mpsc::error::TryRecvError::Disconnected) => {
                    self.check()?;
                    return self.fail(Failure::State("bot audio VAD thread disconnected"));
                }
            }
        }
        Ok(())
    }
    fn start_analysis(&mut self) -> Result<(), EvalError> {
        let Some(endpoint) = &mut self.endpoint else {
            return Ok(());
        };
        if endpoint.pending.is_some() || !endpoint.requested {
            return Ok(());
        }
        endpoint.requested = false;
        let analysis = match endpoint.analyzer.begin_analysis() {
            Ok(analysis) => analysis,
            Err(_) => return self.fail(Failure::State("bot audio Smart Turn request failed")),
        };
        endpoint.pending = Some(EndpointInFlight {
            epoch: self.epoch,
            revision: endpoint.revision,
            cancel: analysis.cancellation(),
            future: Box::pin(analysis.wait()),
        });
        Ok(())
    }
    fn apply_analysis(
        &mut self,
        epoch: u64,
        revision: u64,
        result: pipecat_vad::Result<SmartTurnAnalysis>,
    ) -> Result<(), EvalError> {
        if !self.current(epoch) || self.local_speaking {
            return Ok(());
        }
        let Some(endpoint) = &mut self.endpoint else {
            return Ok(());
        };
        if revision != endpoint.revision || endpoint.complete {
            return Ok(());
        }
        let analysis = match result {
            Ok(analysis) => analysis,
            Err(VadError::Cancelled) => return Ok(()),
            Err(_) => return self.fail(Failure::State("bot audio Smart Turn inference failed")),
        };
        if analysis.reason == SmartTurnCompletionReason::Timeout {
            return self.fail(Failure::State("bot audio Smart Turn inference timed out"));
        }
        if endpoint.analyzer.apply_analysis(&analysis)
            && analysis.state == EndOfTurnState::Complete
            && analysis.reason == SmartTurnCompletionReason::Model
        {
            endpoint.complete = true;
            endpoint.requested = false;
        }
        self.release_held_stop()
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
        self.release_held_stop()
    }
    fn release_held_stop(&mut self) -> Result<(), EvalError> {
        if !self.current(self.epoch)
            || self.local_speaking
            || !self.held_stop
            || !self.pending.is_empty()
            || self.inflight.is_some()
        {
            return Ok(());
        }
        // Only opt-in endpointing uses the worker fence. Holding this lock
        // through the synchronous stop commit linearizes it before subsequent
        // input admission and VAD processing/publication. A concurrent native
        // batch or already-admitted event must be observed before completion.
        // Audio admitted after this commit belongs to the next local turn.
        let shared = self.shared.clone();
        let activity = if let Some(endpoint) = &self.endpoint {
            if !endpoint.complete {
                return Ok(());
            }
            let activity = shared
                .activity
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if activity.queued_bytes != 0
                || activity.processing
                || activity.vad_busy
                || activity.tail_unsettled
                || activity.output_events != 0
            {
                return Ok(());
            }
            Some(activity)
        } else {
            None
        };
        self.held_stop = false;
        let result = self.process_frame(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.4,
            timestamp: 0.0,
        }));
        drop(activity);
        result
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
                    if let Some(endpoint) = &mut self.endpoint {
                        endpoint.clear();
                    }
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
        if let Some(endpoint) = &mut self.endpoint {
            endpoint.clear();
            endpoint.pending = None;
        }
        self.wake();
        Err(failure.error())
    }
    fn wake(&self) {
        if let Some(worker) = &self.worker {
            worker.thread().unpark();
        }
    }
    /// Cancels HTTP and actually joins both native owners. There is no detached
    /// blocking task and no join handle is moved across a cancellable await.
    /// Shutdown is synchronous, including an active Smart Turn prediction:
    /// cancellation is cooperative and neither native join has a wall-time
    /// bound. A 40 ms audio batch bounds VAD input, not inference wall time.
    pub(crate) fn finish(&mut self) -> Result<(), EvalError> {
        self.shared.stop.store(true, Ordering::Release);
        self.inflight = None;
        if let Some(endpoint) = &mut self.endpoint {
            endpoint.clear();
            endpoint.pending = None;
        }
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
        // Drop joins the analyzer's actual inference worker after audio feeding
        // has stopped. Async cleanup can move its join to a cancellable task;
        // this owner deliberately retains the synchronous completion boundary.
        self.endpoint = None;
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
    smart_turn_enabled: bool,
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
        if smart_turn_enabled
            && !publish(
                &output,
                shared,
                epoch,
                ThreadEvent::Pcm {
                    epoch,
                    pcm: Box::new(block),
                    // Latched local speech includes Stopping. Starting noise
                    // before the first real start does not count as speech.
                    is_speech: segmenter.segment.is_some(),
                },
            )
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
    use pipecat_vad::{MonotonicSmartTurnClock, SmartTurnPrediction, SmartTurnPredictor};

    struct PredictionCall {
        cancellation: SmartTurnCancellation,
        samples: Vec<i16>,
    }
    struct ControlledPredictor {
        calls: mpsc::Sender<PredictionCall>,
        replies: Receiver<pipecat_vad::Result<SmartTurnPrediction>>,
    }
    impl SmartTurnPredictor for ControlledPredictor {
        fn predict(
            &mut self,
            samples: &[i16],
            sample_rate: u32,
            cancellation: &SmartTurnCancellation,
        ) -> pipecat_vad::Result<SmartTurnPrediction> {
            assert_eq!(sample_rate, SAMPLE_RATE);
            self.calls
                .send(PredictionCall {
                    cancellation: cancellation.clone(),
                    samples: samples.to_vec(),
                })
                .unwrap();
            loop {
                if cancellation.is_cancelled() {
                    return Err(VadError::Cancelled);
                }
                match self.replies.recv_timeout(Duration::from_millis(5)) {
                    Ok(reply) => return reply,
                    Err(mpsc::RecvTimeoutError::Timeout) => {}
                    Err(mpsc::RecvTimeoutError::Disconnected) => return Err(VadError::Cancelled),
                }
            }
        }
    }
    struct SmartHarness {
        engine: BotTranscription,
        output: async_mpsc::Sender<ThreadEvent>,
        _input: Receiver<AudioCommand>,
        calls: Receiver<PredictionCall>,
        replies: mpsc::Sender<pipecat_vad::Result<SmartTurnPrediction>>,
    }
    impl SmartHarness {
        fn new() -> Self {
            let (mut engine, output, input) = test_engine();
            let (calls_tx, calls) = mpsc::channel();
            let (replies, replies_rx) = mpsc::channel();
            let analyzer = SmartTurnAnalyzer::with_predictor(
                ControlledPredictor {
                    calls: calls_tx,
                    replies: replies_rx,
                },
                SAMPLE_RATE,
                SmartTurnParams::default(),
                Arc::new(MonotonicSmartTurnClock::default()),
                None,
            )
            .unwrap();
            engine.endpoint = Some(Endpoint::new(analyzer));
            engine.turn_config.turn.speech_timeout = Duration::ZERO;
            engine.aggregator =
                LLMUserAggregator::new(SharedContext::default(), engine.turn_config.clone());
            Self {
                engine,
                output,
                _input: input,
                calls,
                replies,
            }
        }
        fn call(&self) -> PredictionCall {
            self.calls.recv_timeout(Duration::from_secs(2)).unwrap()
        }
        fn reply(&self, complete: bool) {
            self.replies
                .send(Ok(SmartTurnPrediction {
                    probability: if complete { 0.9 } else { 0.1 },
                    complete,
                }))
                .unwrap();
        }
        async fn resolve(&mut self) -> Result<(), EvalError> {
            let (epoch, revision, result) = {
                let pending = self
                    .engine
                    .endpoint
                    .as_mut()
                    .unwrap()
                    .pending
                    .as_mut()
                    .unwrap();
                (
                    pending.epoch,
                    pending.revision,
                    tokio::time::timeout(Duration::from_secs(2), pending.future.as_mut())
                        .await
                        .unwrap(),
                )
            };
            self.engine.endpoint.as_mut().unwrap().pending = None;
            self.engine.drain_events()?;
            self.engine.apply_analysis(epoch, revision, result)
        }
        fn receipt(&mut self, text: &str) {
            let segment = self.engine.pending.pop_front().unwrap();
            self.engine
                .commit_transcript(segment.epoch, text.into())
                .unwrap();
        }
    }
    fn pcm_tick(engine: &mut BotTranscription, is_speech: bool) {
        engine
            .process_event(ThreadEvent::Pcm {
                epoch: engine.epoch,
                pcm: Box::new([if is_speech { 7 } else { 0 }; TICK_BYTES]),
                is_speech,
            })
            .unwrap();
    }
    fn local_segment(engine: &mut BotTranscription) {
        engine
            .process_event(ThreadEvent::Started {
                epoch: engine.epoch,
            })
            .unwrap();
        pcm_tick(engine, true);
        pcm_tick(engine, false);
        engine
            .process_event(ThreadEvent::Segment {
                epoch: engine.epoch,
                pcm: vec![7, 0],
            })
            .unwrap();
    }

    #[tokio::test]
    async fn smart_turn_incomplete_retains_utterance_until_later_model_complete() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("first segment");
        harness.engine.start_analysis().unwrap();
        let first = harness.call();
        harness.reply(false);
        harness.resolve().await.unwrap();
        assert!(harness.engine.held_stop);
        assert!(harness.engine.turn_active);
        assert!(harness.engine.ready.is_empty());
        assert!(!harness.engine.is_idle());
        let output = harness
            .engine
            .aggregator
            .advance(Duration::from_secs(200))
            .unwrap();
        harness.engine.collect(output).unwrap();
        assert!(harness.engine.ready.is_empty());

        local_segment(&mut harness.engine);
        harness.receipt("second segment");
        harness.engine.start_analysis().unwrap();
        let second = harness.call();
        assert!(second.samples.len() > first.samples.len());
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert!(!harness.engine.held_stop);
        assert!(!harness.engine.turn_active);
        assert_eq!(
            harness.engine.next().await.unwrap(),
            (0, "first segment second segment".into())
        );
        assert!(harness.engine.is_idle());
    }

    #[tokio::test]
    async fn smart_turn_complete_waits_for_http_and_new_start_revokes_old_completion() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert!(harness.engine.endpoint.as_ref().unwrap().complete);
        assert!(harness.engine.ready.is_empty());
        assert!(harness.engine.held_stop);

        local_segment(&mut harness.engine);
        assert!(!harness.engine.endpoint.as_ref().unwrap().complete);
        harness.receipt("first");
        assert!(harness.engine.ready.is_empty());
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert!(harness.engine.ready.is_empty());
        assert!(harness.engine.held_stop);
        harness.receipt("second");
        assert_eq!(
            harness.engine.ready.pop_front(),
            Some((0, "first second".into()))
        );
        assert!(!harness.engine.turn_active);
    }

    #[tokio::test]
    async fn smart_turn_resume_cancels_prediction_and_retains_latest_stop_request() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("first");
        harness.engine.start_analysis().unwrap();
        let first = harness.call();
        let old_revision = harness.engine.endpoint.as_ref().unwrap().revision;
        let old_generation = harness
            .engine
            .endpoint
            .as_ref()
            .unwrap()
            .analyzer
            .buffer()
            .generation();
        local_segment(&mut harness.engine);
        harness.receipt("second");
        assert!(first.cancellation.is_cancelled());
        assert!(harness.engine.endpoint.as_ref().unwrap().requested);
        harness.engine.start_analysis().unwrap();
        assert!(harness.calls.try_recv().is_err());
        harness
            .engine
            .apply_analysis(
                0,
                old_revision,
                Ok(SmartTurnAnalysis {
                    state: EndOfTurnState::Complete,
                    metrics: None,
                    reason: SmartTurnCompletionReason::Model,
                    generation: old_generation,
                }),
            )
            .unwrap();
        assert!(harness.engine.ready.is_empty());
        harness.resolve().await.unwrap();
        assert!(harness.engine.endpoint.as_ref().unwrap().requested);
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert_eq!(
            harness.engine.ready.pop_front(),
            Some((0, "first second".into()))
        );
    }

    #[tokio::test]
    async fn smart_turn_epoch_cancels_native_work_and_rejects_old_pcm_and_verdict() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("discarded");
        harness.engine.start_analysis().unwrap();
        let first = harness.call();
        let old_revision = harness.engine.endpoint.as_ref().unwrap().revision;
        let old_generation = harness
            .engine
            .endpoint
            .as_ref()
            .unwrap()
            .analyzer
            .buffer()
            .generation();
        harness.engine.set_epoch(1).unwrap();
        assert!(first.cancellation.is_cancelled());
        assert!(harness.engine.endpoint.as_ref().unwrap().pending.is_some());
        assert_eq!(
            harness
                .engine
                .endpoint
                .as_ref()
                .unwrap()
                .analyzer
                .buffer()
                .buffered_samples(),
            0
        );
        harness
            .engine
            .process_event(ThreadEvent::Pcm {
                epoch: 0,
                pcm: Box::new([9; TICK_BYTES]),
                is_speech: true,
            })
            .unwrap();
        harness
            .engine
            .apply_analysis(
                0,
                old_revision,
                Ok(SmartTurnAnalysis {
                    state: EndOfTurnState::Complete,
                    metrics: None,
                    reason: SmartTurnCompletionReason::Model,
                    generation: old_generation,
                }),
            )
            .unwrap();
        assert_eq!(
            harness
                .engine
                .endpoint
                .as_ref()
                .unwrap()
                .analyzer
                .buffer()
                .buffered_samples(),
            0
        );
        local_segment(&mut harness.engine);
        harness.receipt("fresh");
        harness.engine.start_analysis().unwrap();
        assert!(harness.calls.try_recv().is_err());
        harness.resolve().await.unwrap();
        assert!(harness.engine.endpoint.as_ref().unwrap().requested);
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert_eq!(harness.engine.ready.pop_front(), Some((1, "fresh".into())));
    }

    #[tokio::test]
    async fn smart_turn_analyzer_generation_rejection_never_opens_held_stop() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("kept");
        let endpoint = harness.engine.endpoint.as_ref().unwrap();
        let revision = endpoint.revision;
        let stale = endpoint.analyzer.buffer().generation().wrapping_sub(1);
        harness
            .engine
            .apply_analysis(
                0,
                revision,
                Ok(SmartTurnAnalysis {
                    state: EndOfTurnState::Complete,
                    metrics: None,
                    reason: SmartTurnCompletionReason::Model,
                    generation: stale,
                }),
            )
            .unwrap();
        assert!(harness.engine.held_stop);
        assert!(harness.engine.ready.is_empty());
        assert!(!harness.engine.endpoint.as_ref().unwrap().complete);
    }

    #[tokio::test]
    async fn smart_turn_three_second_pcm_silence_fallback_is_not_a_fixed_turn_timer() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("fallback");
        harness.engine.start_analysis().unwrap();
        let call = harness.call();
        // The local stop's quiet block contributed the first 40 ms. Another
        // 2,920 ms is still INCOMPLETE even if the wall clock advances far ahead.
        for _ in 0..73 {
            pcm_tick(&mut harness.engine, false);
        }
        let output = harness
            .engine
            .aggregator
            .advance(Duration::from_secs(200))
            .unwrap();
        harness.engine.collect(output).unwrap();
        assert!(harness.engine.ready.is_empty());
        assert!(!harness.engine.endpoint.as_ref().unwrap().complete);
        pcm_tick(&mut harness.engine, false);
        assert!(harness.engine.endpoint.as_ref().unwrap().complete);
        assert!(call.cancellation.is_cancelled());
        assert!(!harness.engine.endpoint.as_ref().unwrap().requested);
        assert_eq!(
            harness
                .engine
                .endpoint
                .as_ref()
                .unwrap()
                .analyzer
                .buffer()
                .buffered_samples(),
            0
        );
        harness.engine.release_held_stop().unwrap();
        assert_eq!(
            harness.engine.ready.pop_front(),
            Some((0, "fallback".into()))
        );
        assert!(!harness.engine.is_idle());
        harness.resolve().await.unwrap();
        // Idle silence is bounded preroll, not a new analysis or unfinished turn.
        for _ in 0..250 {
            pcm_tick(&mut harness.engine, false);
        }
        assert!(harness.engine.is_idle());
        assert!(!harness.engine.endpoint.as_ref().unwrap().requested);
        assert!(harness.calls.try_recv().is_err());
    }

    #[tokio::test]
    async fn smart_turn_empty_final_waits_for_endpoint_without_fabricating_transcript() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("");
        assert!(!harness.engine.is_idle());
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert!(harness.engine.ready.is_empty());
        assert!(harness.engine.is_idle());
        assert_eq!(harness.engine.next().await.unwrap(), (0, String::new()));
    }

    #[tokio::test]
    async fn smart_turn_errors_and_predictor_timeout_fail_closed() {
        for error in [
            VadError::Runtime("controlled failure".into()),
            VadError::SmartTurnTimeout,
        ] {
            let mut harness = SmartHarness::new();
            local_segment(&mut harness.engine);
            harness.receipt("must not escape");
            harness.engine.start_analysis().unwrap();
            harness.call();
            harness.replies.send(Err(error)).unwrap();
            assert!(harness.resolve().await.is_err());
            assert!(harness.engine.shared.failure().is_some());
            assert!(harness.engine.ready.is_empty());
            assert!(harness.engine.next().await.is_err());
        }
    }

    #[tokio::test]
    async fn smart_turn_release_waits_for_every_admitted_worker_activity() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        let segment = harness.engine.pending.pop_front().unwrap();
        harness.engine.shared.activity.lock().unwrap().queued_bytes = 2;
        harness
            .engine
            .commit_transcript(segment.epoch, "held".into())
            .unwrap();
        for blocker in 0..5 {
            {
                let mut activity = harness.engine.shared.activity.lock().unwrap();
                *activity = Activity::default();
                match blocker {
                    0 => activity.queued_bytes = 2,
                    1 => activity.processing = true,
                    2 => activity.vad_busy = true,
                    3 => activity.tail_unsettled = true,
                    _ => activity.output_events = 1,
                }
            }
            harness.engine.release_held_stop().unwrap();
            assert!(harness.engine.held_stop);
            assert!(harness.engine.ready.is_empty());
        }
        *harness.engine.shared.activity.lock().unwrap() = Activity::default();
        harness.engine.release_held_stop().unwrap();
        assert_eq!(harness.engine.ready.pop_front(), Some((0, "held".into())));
    }

    #[tokio::test]
    async fn smart_turn_already_admitted_start_beats_a_ready_prediction() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("held");
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        assert!(publish(
            &harness.output,
            &harness.engine.shared,
            0,
            ThreadEvent::Started { epoch: 0 }
        ));
        harness.resolve().await.unwrap();
        assert!(harness.engine.local_speaking);
        assert!(harness.engine.ready.is_empty());
        assert!(!harness.engine.endpoint.as_ref().unwrap().complete);
        assert_eq!(
            harness.engine.shared.activity.lock().unwrap().output_events,
            0
        );
    }

    #[tokio::test]
    async fn smart_turn_complete_cannot_release_past_an_unconsumed_start() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.engine.start_analysis().unwrap();
        harness.call();
        harness.reply(true);
        harness.resolve().await.unwrap();
        assert!(publish(
            &harness.output,
            &harness.engine.shared,
            0,
            ThreadEvent::Started { epoch: 0 }
        ));
        harness.receipt("held");
        assert!(harness.engine.held_stop);
        assert!(harness.engine.ready.is_empty());
        harness.engine.drain_events().unwrap();
        assert!(!harness.engine.endpoint.as_ref().unwrap().complete);
        assert!(harness.engine.local_speaking);
        assert!(harness.engine.ready.is_empty());
    }

    #[tokio::test]
    async fn cancellation_of_next_preserves_the_owned_smart_turn_future() {
        let mut harness = SmartHarness::new();
        local_segment(&mut harness.engine);
        harness.receipt("kept");
        harness.engine.start_analysis().unwrap();
        let call = harness.call();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), harness.engine.next())
                .await
                .is_err()
        );
        assert!(!call.cancellation.is_cancelled());
        assert!(harness.engine.endpoint.as_ref().unwrap().pending.is_some());
        harness.reply(true);
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), harness.engine.next())
                .await
                .unwrap()
                .unwrap(),
            (0, "kept".into())
        );
    }

    #[tokio::test]
    async fn finish_cancels_and_synchronously_joins_a_noncooperative_predictor() {
        struct BlockingPredictor {
            entered: mpsc::Sender<SmartTurnCancellation>,
            release: Receiver<()>,
            exited: Arc<AtomicBool>,
        }
        impl SmartTurnPredictor for BlockingPredictor {
            fn predict(
                &mut self,
                _samples: &[i16],
                _sample_rate: u32,
                cancellation: &SmartTurnCancellation,
            ) -> pipecat_vad::Result<SmartTurnPrediction> {
                self.entered.send(cancellation.clone()).unwrap();
                // Deliberately ignore cancellation until the test releases the
                // real native owner. Dropping only its future is insufficient.
                self.release.recv().unwrap();
                self.exited.store(true, Ordering::Release);
                Ok(SmartTurnPrediction {
                    probability: 0.9,
                    complete: true,
                })
            }
        }
        let (mut engine, _output, _input) = test_engine();
        let (entered_tx, entered) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let exited = Arc::new(AtomicBool::new(false));
        engine.endpoint = Some(Endpoint::new(
            SmartTurnAnalyzer::with_predictor(
                BlockingPredictor {
                    entered: entered_tx,
                    release: release_rx,
                    exited: exited.clone(),
                },
                SAMPLE_RATE,
                SmartTurnParams::default(),
                Arc::new(MonotonicSmartTurnClock::default()),
                None,
            )
            .unwrap(),
        ));
        local_segment(&mut engine);
        engine.start_analysis().unwrap();
        let cancel = entered.recv_timeout(Duration::from_secs(2)).unwrap();
        let (returned_tx, returned) = mpsc::channel();
        let finisher = thread::spawn(move || {
            engine.finish().unwrap();
            assert!(engine.endpoint.is_none());
            returned_tx.send(()).unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(2);
        while !cancel.is_cancelled() && Instant::now() < deadline {
            thread::yield_now();
        }
        let cancelled = cancel.is_cancelled();
        let returned_early = returned.recv_timeout(Duration::from_millis(20)).is_ok();
        let exited_early = exited.load(Ordering::Acquire);
        release.send(()).unwrap();
        finisher.join().unwrap();
        assert!(cancelled);
        assert!(!returned_early, "finish detached a live native predictor");
        assert!(!exited_early);
        assert!(exited.load(Ordering::Acquire));
        returned.recv_timeout(Duration::from_secs(2)).unwrap();
    }

    #[test]
    fn smart_turn_supported_rate_and_buffer_policy_are_bounded() {
        fn analyzer(rate: u32, params: SmartTurnParams) -> SmartTurnAnalyzer {
            let (calls, _calls_rx) = mpsc::channel();
            let (_replies_tx, replies) = mpsc::channel();
            SmartTurnAnalyzer::with_predictor(
                ControlledPredictor { calls, replies },
                rate,
                params,
                Arc::new(MonotonicSmartTurnClock::default()),
                None,
            )
            .unwrap()
        }
        assert!(validate_smart_turn(&analyzer(SAMPLE_RATE, SmartTurnParams::default())).is_ok());
        assert!(validate_smart_turn(&analyzer(8_000, SmartTurnParams::default())).is_err());
        for params in [
            SmartTurnParams {
                stop_secs: 2.0,
                ..SmartTurnParams::default()
            },
            SmartTurnParams {
                pre_speech_ms: 501.0,
                ..SmartTurnParams::default()
            },
            SmartTurnParams {
                max_duration_secs: 9.0,
                ..SmartTurnParams::default()
            },
        ] {
            assert!(validate_smart_turn(&analyzer(SAMPLE_RATE, params)).is_err());
        }
    }

    #[test]
    fn smart_turn_pcm_publication_uses_the_existing_fail_closed_output_bound() {
        let mut harness = SmartHarness::new();
        for _ in 0..OUTPUT_CAPACITY {
            assert!(publish(
                &harness.output,
                &harness.engine.shared,
                0,
                ThreadEvent::Pcm {
                    epoch: 0,
                    pcm: Box::new([0; TICK_BYTES]),
                    is_speech: false,
                }
            ));
        }
        assert!(!publish(
            &harness.output,
            &harness.engine.shared,
            0,
            ThreadEvent::Pcm {
                epoch: 0,
                pcm: Box::new([0; TICK_BYTES]),
                is_speech: false,
            }
        ));
        assert!(matches!(harness.engine.check(), Err(EvalError::Limit(_))));
        assert_eq!(
            harness.engine.shared.activity.lock().unwrap().output_events,
            OUTPUT_CAPACITY
        );
        assert!(harness.engine.finish().is_err());
    }

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
        assert!(segmenter.segment.is_none());
        let (started, completed) = segmenter
            .push(&[3; TICK_BYTES], VadState::Speaking)
            .unwrap();
        assert!(started && completed.is_none());
        assert!(segmenter.segment.is_some());
        assert!(segmenter
            .push(&[4; TICK_BYTES], VadState::Stopping)
            .unwrap()
            .1
            .is_none());
        assert!(segmenter.segment.is_some());
        let (started, completed) = segmenter.push(&[0; TICK_BYTES], VadState::Quiet).unwrap();
        assert!(!started);
        assert!(segmenter.segment.is_none());
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
            endpoint: None,
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
