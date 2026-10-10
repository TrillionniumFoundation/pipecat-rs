//! Single-session voicemail classification and bounded, ordered speech admission.
//!
//! Transcripts still reach the conversation while detection is pending. This
//! module neither authorizes nor prevents LLM/tool effects. An event proves only
//! a classification/readiness decision, never synthesis, playback or delivery.
mod budget;

use futures_util::FutureExt;
use pipecat_classifiers::{CallOptions, CancellationToken, ChoiceQuestion, Classifier};
use pipecat_core::{
    async_trait, Direction, Frame, FrameData, FrameProcessor, LocalFrameTag, ObservedProcessor,
    ProcessorContext, ProcessorError, StartFrame,
};
use serde_json::Value;
use std::{
    collections::{BTreeMap, VecDeque},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex, MutexGuard,
    },
    time::Duration,
};
use tokio::{
    sync::{watch, Notify},
    time::Instant,
};

/// The bounded first slice accepts compact, noninterleaved response boundaries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum VoicemailTopology {
    /// TTS preserves original compact LlmResponseStart/LlmResponseEnd frames.
    /// This mode makes no spoken-history qualification claim.
    #[default]
    CompactTts,
    /// Explicitly unsupported until the complete ledger topology is bound.
    SpokenHistory,
    /// Detailed boundaries and custom/interleaved adapters are not qualified.
    Other,
}

#[derive(Clone, Debug)]
pub struct VoicemailConfig {
    pub topology: VoicemailTopology,
    pub decision_silence: Duration,
    pub message_silence: Duration,
    pub classifier_timeout: Duration,
    /// Safety deadline, starting at first detection input or held speech.
    pub max_unresolved_hold: Duration,
    /// Finite capacity wait for each admitted output frame.
    pub drain_timeout: Duration,
    pub max_transcript_bytes: usize,
    pub max_transcript_segments: usize,
    /// Combined allocation capacities of cumulative and in-flight transcripts.
    pub max_evidence_bytes: usize,
    pub max_frames: usize,
    /// Accounted gate-owned payload/metadata allocation, including drain copies.
    /// Opaque shared core runtime bookkeeping and external clones are excluded.
    /// Use newly generated single-runtime frames; CompactTts cannot prove this.
    pub max_frame_bytes: usize,
    pub max_pcm_bytes: usize,
    pub max_pcm_duration: Duration,
}
impl Default for VoicemailConfig {
    fn default() -> Self {
        Self {
            topology: VoicemailTopology::CompactTts,
            decision_silence: Duration::from_secs(1),
            message_silence: Duration::from_secs(2),
            classifier_timeout: Duration::from_secs(5),
            max_unresolved_hold: Duration::from_secs(30),
            drain_timeout: Duration::from_secs(5),
            max_transcript_bytes: 16 * 1024,
            max_transcript_segments: 256,
            max_evidence_bytes: 32 * 1024,
            max_frames: 256,
            max_frame_bytes: 2 * 1024 * 1024,
            max_pcm_bytes: 1024 * 1024,
            max_pcm_duration: Duration::from_secs(10),
        }
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoicemailConfigError {
    UnsupportedTopology,
    DurationLimit,
    RetentionLimit,
    CountLimit,
    SessionExhausted,
}
impl std::fmt::Display for VoicemailConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "voicemail configuration: {self:?}")
    }
}
impl std::error::Error for VoicemailConfigError {}
impl VoicemailConfig {
    fn validate(&self) -> Result<(), VoicemailConfigError> {
        if self.topology != VoicemailTopology::CompactTts {
            return Err(VoicemailConfigError::UnsupportedTopology);
        }
        for duration in [
            self.decision_silence,
            self.message_silence,
            self.classifier_timeout,
            self.max_unresolved_hold,
            self.drain_timeout,
            self.max_pcm_duration,
        ] {
            if duration.is_zero() || duration > Duration::from_secs(3600) {
                return Err(VoicemailConfigError::DurationLimit);
            }
        }
        for limit in [
            self.max_transcript_bytes,
            self.max_transcript_segments,
            self.max_evidence_bytes,
            self.max_frames,
            self.max_frame_bytes,
            self.max_pcm_bytes,
        ] {
            if limit == 0 || limit > 64 * 1024 * 1024 {
                return Err(VoicemailConfigError::RetentionLimit);
            }
        }
        if self.max_transcript_segments > 65536 || self.max_frames > 65536 {
            return Err(VoicemailConfigError::CountLimit);
        }
        Ok(())
    }
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DetectionPhase {
    Pending,
    Conversation,
    VoicemailWaiting,
    VoicemailReady,
    Failed,
    Closed,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EvidenceReason {
    Classifier,
    NoAnswerFallback,
}
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum VoicemailEvent {
    ConversationDetected {
        session_id: u64,
        reason: EvidenceReason,
    },
    VoicemailReady {
        session_id: u64,
        reason: EvidenceReason,
    },
}
/// Payload-free errors; neither transcripts nor classifier error strings survive.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Failure {
    PairAttachment,
    UnsupportedTopology,
    TranscriptLimit,
    EvidenceLimit,
    FrameLimit,
    PcmLimit,
    HoldTimeout,
    DrainTimeout,
    DrainFailure,
}
fn error(failure: Failure) -> ProcessorError {
    if failure == Failure::PairAttachment {
        ProcessorError::fatal("voicemail: PairAttachment")
    } else {
        ProcessorError::new(format!("voicemail: {failure:?}"))
    }
}
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VoicemailStatus {
    pub session_id: u64,
    pub phase: DetectionPhase,
    /// At most one immutable event is retained; no callback/event retry exists.
    pub event: Option<VoicemailEvent>,
    pub failure: Option<Failure>,
    pub speaking: bool,
    pub transcript_segments: usize,
    pub transcript_bytes: usize,
    pub evidence_bytes: usize,
    pub revision: u64,
    pub attempted_revision: u64,
    pub attempts: u64,
    pub missed_answers: u64,
    pub in_flight_classifier: bool,
    /// Includes the frame owned by a blocked drain future.
    pub retained_frames: usize,
    pub retained_frame_bytes: usize,
    pub retained_pcm_bytes: usize,
    pub retained_pcm_duration: Duration,
    pub drain_in_flight: bool,
    pub drain_owner_running: bool,
    /// Drain futures returning Ok, including stale frames suppressed by the core.
    /// This is not a local-admission, transport-submission or playback count.
    pub completed_release_attempts: u64,
    pub discarded_frames: u64,
    pub supervisors: usize,
}
/// A payload-free watch cell. Cloning it neither keeps the pair alive nor grants
/// control. Slow/dropped readers do not block processing or repeat events.
#[derive(Clone)]
pub struct VoicemailStatusHandle(watch::Receiver<VoicemailStatus>);
impl VoicemailStatusHandle {
    pub fn snapshot(&self) -> VoicemailStatus {
        self.0.borrow().clone()
    }
    pub async fn changed(&mut self) -> Result<VoicemailStatus, watch::error::RecvError> {
        self.0.changed().await?;
        Ok(self.0.borrow_and_update().clone())
    }
}

pub struct VoicemailDetector {
    shared: Arc<Shared>,
    classifier: Arc<dyn Classifier>,
}
pub struct TtsGate {
    shared: Arc<Shared>,
}
/// Borrowing the classifier never transfers setup/cleanup ownership. Closing a
/// pair cancels only its own call; it cannot close a caller-shared client/service.
pub fn voicemail_pair(
    classifier: Arc<dyn Classifier>,
    config: VoicemailConfig,
) -> Result<(VoicemailDetector, TtsGate, VoicemailStatusHandle), VoicemailConfigError> {
    config.validate()?;
    static NEXT_SESSION: AtomicU64 = AtomicU64::new(1);
    let session = NEXT_SESSION
        .try_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| VoicemailConfigError::SessionExhausted)?;
    let status = VoicemailStatus {
        session_id: session,
        phase: DetectionPhase::Pending,
        event: None,
        failure: None,
        speaking: false,
        transcript_segments: 0,
        transcript_bytes: 0,
        evidence_bytes: 0,
        revision: 0,
        attempted_revision: 0,
        attempts: 0,
        missed_answers: 0,
        in_flight_classifier: false,
        retained_frames: 0,
        retained_frame_bytes: 0,
        retained_pcm_bytes: 0,
        retained_pcm_duration: Duration::ZERO,
        drain_in_flight: false,
        drain_owner_running: false,
        completed_release_attempts: 0,
        discarded_frames: 0,
        supervisors: 0,
    };
    let (status_tx, status_rx) = watch::channel(status.clone());
    let shared = Arc::new(Shared {
        config,
        state: Mutex::new(State {
            status,
            detector: None,
            gate: None,
            ready: false,
            closing: false,
            detector_ending: false,
            transcript: String::new(),
            snapshot_bytes: 0,
            last_answer: None,
            decision_deadline: None,
            message_deadline: None,
            hold_deadline: None,
            queue: VecDeque::new(),
            in_flight: None,
            drainer_running: false,
            response: false,
            tts_context: None,
            topology_bytes: 0,
            release: false,
            voicemail_open: false,
        }),
        status_tx,
        detector_wake: Notify::new(),
        gate_wake: Notify::new(),
        cancelled: CancellationToken::new(),
        drain_cancelled: Mutex::new(CancellationToken::new()),
        voicemail_interruption: LocalFrameTag::new(),
    });
    Ok((
        VoicemailDetector {
            shared: shared.clone(),
            classifier,
        },
        TtsGate { shared },
        VoicemailStatusHandle(status_rx),
    ))
}

struct Shared {
    config: VoicemailConfig,
    state: Mutex<State>,
    status_tx: watch::Sender<VoicemailStatus>,
    detector_wake: Notify,
    gate_wake: Notify,
    cancelled: CancellationToken,
    drain_cancelled: Mutex<CancellationToken>,
    voicemail_interruption: LocalFrameTag,
}
struct State {
    status: VoicemailStatus,
    detector: Option<ObservedProcessor>,
    gate: Option<ObservedProcessor>,
    ready: bool,
    closing: bool,
    detector_ending: bool,
    transcript: String,
    snapshot_bytes: usize,
    last_answer: Option<bool>,
    decision_deadline: Option<Instant>,
    message_deadline: Option<Instant>,
    hold_deadline: Option<Instant>,
    queue: VecDeque<Held>,
    in_flight: Option<Held>,
    drainer_running: bool,
    response: bool,
    tts_context: Option<Option<String>>,
    topology_bytes: usize,
    release: bool,
    voicemail_open: bool,
}
struct Held {
    frame: Frame,
    direction: Direction,
    context: ProcessorContext,
    charge: budget::Charge,
}
impl Shared {
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap()
    }
    fn publish(&self, state: &mut State) {
        state.status.drain_owner_running = state.drainer_running;
        state.status.transcript_bytes = state.transcript.len();
        state.status.evidence_bytes = state.transcript.capacity() + state.snapshot_bytes;
        self.status_tx.send_replace(state.status.clone());
    }
    fn touch(&self, state: &mut State) {
        if state.status.phase == DetectionPhase::Pending && state.hold_deadline.is_none() {
            state.hold_deadline = Some(Instant::now() + self.config.max_unresolved_hold);
        }
    }
    fn discard_queue(&self, state: &mut State, all: bool) {
        let mut kept = VecDeque::new();
        while let Some(held) = state.queue.pop_front() {
            if all || held.frame.interruptible {
                uncharge(state, held.charge);
                state.status.discarded_frames += 1;
            } else {
                kept.push_back(held);
            }
        }
        state.queue = kept;
        state.response = false;
        state.tts_context = None;
        state.status.retained_frame_bytes -= state.topology_bytes;
        state.topology_bytes = 0;
    }
    fn stop_drain(&self) {
        let mut cancel = self.drain_cancelled.lock().unwrap();
        cancel.cancel();
        *cancel = CancellationToken::new();
    }
    fn fail(&self, failure: Failure) {
        {
            let mut state = self.lock();
            if matches!(
                state.status.phase,
                DetectionPhase::Failed | DetectionPhase::Closed
            ) {
                return;
            }
            state.status.phase = DetectionPhase::Failed;
            state.status.failure = Some(failure);
            state.closing = true;
            state.release = false;
            state.transcript = String::new();
            state.decision_deadline = None;
            state.message_deadline = None;
            state.hold_deadline = None;
            self.discard_queue(&mut state, true);
            self.publish(&mut state);
        }
        self.cancelled.cancel();
        self.stop_drain();
        self.detector_wake.notify_one();
        self.gate_wake.notify_waiters();
    }
    /// The detector precedes asynchronous TTS. In Conversation, its terminal
    /// stops detection only; the gate closes at its own ordered terminal after
    /// upstream synthesis has completed the core's prepare_end/task drain.
    fn end_detector(&self) {
        let keep_gate = {
            let mut state = self.lock();
            if !state.closing && state.status.phase == DetectionPhase::Conversation {
                state.detector_ending = true;
                state.transcript = String::new();
                state.decision_deadline = None;
                state.message_deadline = None;
                state.hold_deadline = None;
                self.publish(&mut state);
                true
            } else {
                false
            }
        };
        if keep_gate {
            self.cancelled.cancel();
            self.detector_wake.notify_one();
        } else {
            self.close(true);
        }
    }
    fn close(&self, graceful: bool) {
        {
            let mut state = self.lock();
            state.closing = true;
            state.transcript = String::new();
            state.decision_deadline = None;
            state.message_deadline = None;
            state.hold_deadline = None;
            if !graceful || !state.release {
                state.release = false;
                self.discard_queue(&mut state, true);
            }
            if state.status.phase != DetectionPhase::Failed {
                state.status.phase = DetectionPhase::Closed;
            }
            self.publish(&mut state);
        }
        self.cancelled.cancel();
        if !graceful {
            self.stop_drain();
        }
        self.detector_wake.notify_one();
        self.gate_wake.notify_waiters();
    }
    fn interrupt(&self, owned: bool) {
        self.stop_drain();
        let mut state = self.lock();
        self.discard_queue(&mut state, owned);
        if owned
            && matches!(
                state.status.phase,
                DetectionPhase::VoicemailWaiting | DetectionPhase::VoicemailReady
            )
        {
            state.voicemail_open = true;
        }
        self.publish(&mut state);
        self.gate_wake.notify_waiters();
    }
}
fn uncharge(state: &mut State, c: budget::Charge) {
    state.status.retained_frames -= 1;
    state.status.retained_frame_bytes -= c.bytes;
    state.status.retained_pcm_bytes -= c.pcm;
    state.status.retained_pcm_duration -= c.duration;
}
struct SupervisorGuard {
    shared: Arc<Shared>,
    detector: bool,
}
impl Drop for SupervisorGuard {
    fn drop(&mut self) {
        // Constructed before spawn, so a task cancelled before first poll also
        // clears resources; retained status readers never own processor state.
        let detector_finished = {
            let state = self.shared.lock();
            self.detector && (state.closing || state.detector_ending)
        };
        if !detector_finished {
            self.shared.close(false);
        }
        let mut state = self.shared.lock();
        state.status.supervisors -= 1;
        if self.detector {
            state.snapshot_bytes = 0;
            state.status.in_flight_classifier = false;
        }
        self.shared.publish(&mut state);
    }
}
struct DrainGuard {
    shared: Arc<Shared>,
    released: bool,
}
impl DrainGuard {
    fn mark_released(&mut self) {
        self.released = true;
    }
}
impl Drop for DrainGuard {
    fn drop(&mut self) {
        let mut state = self.shared.lock();
        if let Some(held) = state.in_flight.take() {
            if !self.released
                && !held.frame.interruptible
                && !state.closing
                && matches!(
                    state.status.phase,
                    DetectionPhase::Pending | DetectionPhase::Conversation
                )
            {
                state.queue.push_front(held);
            } else {
                uncharge(&mut state, held.charge);
                if self.released {
                    state.status.completed_release_attempts += 1;
                } else {
                    state.status.discarded_frames += 1;
                }
            }
        }
        state.status.drain_in_flight = false;
        self.shared.publish(&mut state);
        self.shared.gate_wake.notify_waiters();
    }
}

fn question() -> BTreeMap<String, ChoiceQuestion> {
    BTreeMap::from([("voicemail".into(), ChoiceQuestion {
        instructions: Value::String("A bot has placed an outbound phone call. This is what was heard after the call connected. Decide whether a person answered or the call went to voicemail.".into()),
        options: BTreeMap::from([
            ("conversation".into(), Some(Value::String("a person answered: a greeting such as 'hello?', 'hi', 'yeah?' or 'John speaking'; a question to the caller such as 'who is this?' or 'can I help you?'; spontaneous speech that expects a reply".into()))),
            ("voicemail".into(), Some(Value::String("an automated greeting or carrier message: 'you've reached', 'leave a message', 'I'm not available right now', 'call me back', 'mailbox is full', 'not in service', 'all circuits are busy', 'our office is currently closed'".into()))),
        ]),
    })])
}
async fn until(deadline: Option<Instant>) {
    match deadline {
        Some(time) => tokio::time::sleep_until(time).await,
        None => std::future::pending().await,
    }
}
enum Step {
    Wait(Option<Instant>),
    Infer(Value, u64, Instant),
    Voicemail,
    Done,
    Fail(Failure),
}
fn next_step(shared: &Shared) -> Step {
    let mut state = shared.lock();
    if state.closing || state.detector_ending {
        return Step::Done;
    }
    if !state.ready {
        return Step::Wait(None);
    }
    let now = Instant::now();
    if state.status.phase == DetectionPhase::Pending {
        if state.hold_deadline.is_some_and(|d| now >= d) {
            return Step::Fail(Failure::HoldTimeout);
        }
        if state.status.revision > state.status.attempted_revision {
            if state.transcript.capacity() + state.transcript.len()
                > shared.config.max_evidence_bytes
            {
                return Step::Fail(Failure::EvidenceLimit);
            }
            let mut copy = String::new();
            if copy.try_reserve_exact(state.transcript.len()).is_err() {
                return Step::Fail(Failure::EvidenceLimit);
            }
            copy.push_str(&state.transcript);
            if state.transcript.capacity() + copy.capacity() > shared.config.max_evidence_bytes {
                return Step::Fail(Failure::EvidenceLimit);
            }
            state.snapshot_bytes = copy.capacity();
            state.status.in_flight_classifier = true;
            state.status.attempts += 1;
            let revision = state.status.revision;
            let deadline =
                (now + shared.config.classifier_timeout).min(state.hold_deadline.unwrap());
            shared.publish(&mut state);
            return Step::Infer(Value::String(copy), revision, deadline);
        }
        if !state.status.speaking && state.decision_deadline.is_some_and(|d| now >= d) {
            state.transcript = String::new();
            state.decision_deadline = None;
            state.hold_deadline = None;
            if state.last_answer == Some(true) {
                state.status.phase = DetectionPhase::VoicemailWaiting;
                shared.discard_queue(&mut state, true);
                shared.publish(&mut state);
                return Step::Voicemail;
            }
            state.status.phase = DetectionPhase::Conversation;
            let reason = if state.last_answer.is_some() {
                EvidenceReason::Classifier
            } else {
                EvidenceReason::NoAnswerFallback
            };
            state.status.event = Some(VoicemailEvent::ConversationDetected {
                session_id: state.status.session_id,
                reason,
            });
            state.release = true;
            shared.publish(&mut state);
            shared.gate_wake.notify_waiters();
        }
    } else if state.status.phase == DetectionPhase::VoicemailWaiting
        && !state.status.speaking
        && state.message_deadline.is_some_and(|d| now >= d)
    {
        state.status.phase = DetectionPhase::VoicemailReady;
        state.message_deadline = None;
        state.status.event = Some(VoicemailEvent::VoicemailReady {
            session_id: state.status.session_id,
            reason: EvidenceReason::Classifier,
        });
        shared.publish(&mut state);
    }
    let deadline = [
        state.decision_deadline.filter(|_| !state.status.speaking),
        state.message_deadline,
        state.hold_deadline,
    ]
    .into_iter()
    .flatten()
    .min();
    Step::Wait(deadline)
}
async fn supervise_detector(
    shared: Arc<Shared>,
    classifier: Arc<dyn Classifier>,
    ctx: ProcessorContext,
    guard: SupervisorGuard,
) -> Result<(), ProcessorError> {
    let _guard = guard;
    loop {
        let wake = shared.detector_wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        match next_step(&shared) {
            Step::Done => return Ok(()),
            Step::Fail(failure) => {
                shared.fail(failure);
                return Err(error(failure));
            }
            Step::Wait(deadline) => tokio::select! {
                biased;
                _ = shared.cancelled.cancelled() => return Ok(()),
                _ = &mut wake => {},
                _ = until(deadline) => {},
            },
            Step::Infer(snapshot, revision, deadline) => {
                let options = CallOptions {
                    cancellation: shared.cancelled.clone(),
                    deadline: Some(deadline),
                };
                let questions = question();
                let result = tokio::select! {
                    biased;
                    _ = shared.cancelled.cancelled() => None,
                    result = tokio::time::timeout_at(deadline, std::panic::AssertUnwindSafe(classifier.choice(&snapshot, questions, options)).catch_unwind()) => Some(result),
                };
                // Drop the immutable transcript before returning its reservation.
                drop(snapshot);
                let mut state = shared.lock();
                state.snapshot_bytes = 0;
                state.status.in_flight_classifier = false;
                if !state.closing && state.status.phase == DetectionPhase::Pending {
                    state.status.attempted_revision = revision;
                    let answer = if Instant::now() >= deadline {
                        None
                    } else {
                        result
                            .and_then(Result::ok)
                            .and_then(Result::ok)
                            .and_then(Result::ok)
                            .and_then(|batch| {
                                batch.answers.get("voicemail").and_then(|answer| {
                                    match answer.choice.as_str() {
                                        "voicemail" => Some(true),
                                        "conversation" => Some(false),
                                        _ => None,
                                    }
                                })
                            })
                    };
                    if let Some(answer) = answer {
                        state.last_answer = Some(answer);
                    } else {
                        state.status.missed_answers += 1;
                    }
                }
                shared.publish(&mut state);
            }
            Step::Voicemail => {
                shared.stop_drain();
                let mut interruption = Frame::interruption();
                shared.voicemail_interruption.mark(&mut interruption)?;
                if let Err(err) = ctx.broadcast_interruption_frame(&interruption).await {
                    shared.fail(Failure::DrainFailure);
                    return Err(err);
                }
                let mut state = shared.lock();
                if !state.closing
                    && state.status.phase == DetectionPhase::VoicemailWaiting
                    && !state.status.speaking
                {
                    state.message_deadline = Some(Instant::now() + shared.config.message_silence);
                }
                shared.publish(&mut state);
                shared.gate_wake.notify_waiters();
            }
        }
    }
}

struct DrainerGuard(Arc<Shared>);
impl Drop for DrainerGuard {
    fn drop(&mut self) {
        let mut state = self.0.lock();
        state.drainer_running = false;
        self.0.publish(&mut state);
        self.0.gate_wake.notify_waiters();
    }
}
async fn drain_queue(
    shared: Arc<Shared>,
    cancellation: CancellationToken,
    guard: DrainerGuard,
) -> Result<(), ProcessorError> {
    let _guard = guard;
    loop {
        let wake = shared.gate_wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let output = {
            let mut state = shared.lock();
            if cancellation.is_cancelled() || !state.release {
                return Ok(());
            }
            if let Some(held) = state.queue.pop_front() {
                let output = Held {
                    frame: held.frame.clone(),
                    direction: held.direction,
                    context: held.context.clone(),
                    charge: held.charge,
                };
                state.in_flight = Some(held);
                if state.queue.is_empty() {
                    state.queue = VecDeque::new();
                }
                state.status.drain_in_flight = true;
                shared.publish(&mut state);
                Some(output)
            } else {
                if state.closing {
                    return Ok(());
                }
                None
            }
        };
        let Some(held) = output else {
            tokio::select! {
                biased;
                _ = cancellation.cancelled() => return Ok(()),
                _ = &mut wake => {},
            }
            continue;
        };
        let mut lease = DrainGuard {
            shared: shared.clone(),
            released: false,
        };
        let deadline = Instant::now() + shared.config.drain_timeout;
        let send = async {
            if held.direction == Direction::Downstream {
                held.context.push_downstream_wait(held.frame).await
            } else {
                held.context
                    .try_push_from_invocation(held.frame, held.direction, &held.context)
            }
        };
        let result = tokio::select! {
            biased;
            _ = cancellation.cancelled() => return Ok(()),
            result = tokio::time::timeout_at(deadline, send) => result,
        };
        match result {
            Ok(Ok(())) => lease.mark_released(),
            other => {
                let failure = if other.is_err() {
                    Failure::DrainTimeout
                } else {
                    Failure::DrainFailure
                };
                shared.fail(failure);
                return Err(error(failure));
            }
        }
        // Settle the current frame before admitting the next original. The
        // task remains alive while idle, rather than consuming a task per frame.
        drop(lease);
    }
}
async fn supervise_gate(
    shared: Arc<Shared>,
    ctx: ProcessorContext,
    guard: SupervisorGuard,
) -> Result<(), ProcessorError> {
    let _guard = guard;
    loop {
        let wake = shared.gate_wake.notified();
        tokio::pin!(wake);
        wake.as_mut().enable();
        let start = {
            let mut state = shared.lock();
            if state.closing && !state.drainer_running && state.queue.is_empty() {
                return Ok(());
            }
            if state.release && !state.drainer_running {
                state.drainer_running = true;
                shared.publish(&mut state);
                true
            } else {
                false
            }
        };
        if start {
            let cancellation = shared.drain_cancelled.lock().unwrap().clone();
            let lease = DrainerGuard(shared.clone());
            // This one drain is interruption-cancellable. Its per-frame contexts
            // retain the raw epochs; the surviving supervisor never emits PCM.
            if let Err(err) = ctx.spawn(
                "voicemail-ordered-drain",
                true,
                drain_queue(shared.clone(), cancellation, lease),
            ) {
                shared.fail(Failure::DrainFailure);
                return Err(err);
            }
        } else {
            wake.await;
        }
    }
}

fn detector_allows(frame: &Frame) -> bool {
    frame.is_system()
        || matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Worker
                | FrameData::WorkerSystem
                | FrameData::EndWorker(_)
                | FrameData::StopWorker
                | FrameData::CancelWorker(_)
                | FrameData::InterruptionWorker
                | FrameData::Task
                | FrameData::TaskSystem
        )
}
fn terminal(frame: &Frame) -> bool {
    matches!(
        frame.data,
        FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_)
    )
}
fn gated(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::TtsStarted(_)
            | FrameData::TtsStopped(_)
            | FrameData::TtsText(_)
            | FrameData::TtsAudio(_)
            | FrameData::SpeechOutputAudio(_)
            | FrameData::TtsTextInput(_)
            | FrameData::TtsAlignmentDiagnostic(_)
            | FrameData::AggregatedTextProgress(_)
            | FrameData::LlmResponseStart
            | FrameData::LlmResponseEnd
    )
}
impl VoicemailDetector {
    fn input(&self, frame: &Frame) -> Result<(), ProcessorError> {
        let shared = &self.shared;
        let mut state = shared.lock();
        if state.closing || state.detector_ending {
            return Ok(());
        }
        match &frame.data {
            FrameData::UserStartedSpeaking => {
                shared.touch(&mut state);
                state.status.speaking = true;
                state.decision_deadline = None;
                state.message_deadline = None;
            }
            FrameData::UserStoppedSpeaking => {
                shared.touch(&mut state);
                state.status.speaking = false;
                if state.status.phase == DetectionPhase::Pending {
                    state.decision_deadline = Some(Instant::now() + shared.config.decision_silence);
                } else if state.status.phase == DetectionPhase::VoicemailWaiting {
                    state.message_deadline = Some(Instant::now() + shared.config.message_silence);
                }
            }
            _ if state.status.phase == DetectionPhase::Pending => {
                let text = match &frame.data {
                    FrameData::Transcription(t) => t.text.trim(),
                    FrameData::TranscriptionDetailed(t) => t.text.text.trim(),
                    _ => "",
                };
                if !text.is_empty() {
                    shared.touch(&mut state);
                    let required = state
                        .transcript
                        .len()
                        .saturating_add(text.len())
                        .saturating_add(usize::from(!state.transcript.is_empty()));
                    let failure = if required > shared.config.max_transcript_bytes
                        || state.status.transcript_segments >= shared.config.max_transcript_segments
                    {
                        Some(Failure::TranscriptLimit)
                    } else if required + state.snapshot_bytes > shared.config.max_evidence_bytes {
                        Some(Failure::EvidenceLimit)
                    } else {
                        let reserve = required.saturating_sub(state.transcript.len());
                        if state.transcript.try_reserve_exact(reserve).is_err()
                            || state.transcript.capacity() > shared.config.max_transcript_bytes
                            || state.transcript.capacity() + state.snapshot_bytes
                                > shared.config.max_evidence_bytes
                        {
                            Some(Failure::EvidenceLimit)
                        } else {
                            None
                        }
                    };
                    if let Some(failure) = failure {
                        drop(state);
                        shared.fail(failure);
                        return Err(error(failure));
                    }
                    if !state.transcript.is_empty() {
                        state.transcript.push(' ');
                    }
                    state.transcript.push_str(text);
                    state.status.transcript_segments += 1;
                    state.status.revision += 1;
                    if !state.status.speaking {
                        state.decision_deadline =
                            Some(Instant::now() + shared.config.decision_silence);
                    }
                }
            }
            _ => {}
        }
        shared.publish(&mut state);
        shared.detector_wake.notify_one();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for VoicemailDetector {
    fn name(&self) -> &str {
        "VoicemailDetector"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        {
            let mut state = self.shared.lock();
            if state.detector.is_some() || state.closing {
                drop(state);
                self.shared.fail(Failure::PairAttachment);
                return Err(error(Failure::PairAttachment));
            }
            state.detector = Some(ctx.processor_identity());
            state.status.supervisors += 1;
            self.shared.publish(&mut state);
        }
        let guard = SupervisorGuard {
            shared: self.shared.clone(),
            detector: true,
        };
        ctx.spawn(
            "voicemail-detector",
            false,
            supervise_detector(
                self.shared.clone(),
                self.classifier.clone(),
                ctx.clone(),
                guard,
            ),
        )?;
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() && {
            let state = self.shared.lock();
            !state.ready || state.closing
        } {
            self.shared.fail(Failure::PairAttachment);
            return Err(error(Failure::PairAttachment));
        }
        if terminal(&frame) {
            if frame.data.is_cancel() {
                self.shared.close(false);
            } else {
                self.shared.end_detector();
            }
            return ctx.push(frame, direction).await;
        }
        let result = ctx
            .invocation_fence()?
            .with_current(|| self.input(&frame))
            .unwrap_or(Ok(()));
        let allow = {
            let state = self.shared.lock();
            !matches!(
                state.status.phase,
                DetectionPhase::VoicemailWaiting
                    | DetectionPhase::VoicemailReady
                    | DetectionPhase::Failed
                    | DetectionPhase::Closed
            ) || detector_allows(&frame)
        };
        if allow {
            ctx.push(frame, direction).await?;
        }
        result
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.shared.end_detector();
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.shared.close(false);
        Ok(())
    }
}
impl Drop for VoicemailDetector {
    fn drop(&mut self) {
        self.shared.close(false);
    }
}

impl TtsGate {
    fn admit(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if !ctx.invocation_fence()?.is_current() && frame.interruptible {
            return Ok(());
        }
        let mut charge = match budget::charge(&frame, self.shared.config.max_frame_bytes) {
            Ok(charge) => charge,
            Err(failure) => {
                self.shared.fail(failure);
                return Err(error(failure));
            }
        };
        charge.bytes = match charge.bytes.checked_add(ctx.name().len().saturating_mul(2)) {
            Some(bytes) if bytes <= self.shared.config.max_frame_bytes => bytes,
            _ => {
                self.shared.fail(Failure::FrameLimit);
                return Err(error(Failure::FrameLimit));
            }
        };
        let mut state = self.shared.lock();
        if state.closing
            || matches!(
                state.status.phase,
                DetectionPhase::Failed | DetectionPhase::Closed
            )
        {
            return Ok(());
        }
        if matches!(
            state.status.phase,
            DetectionPhase::VoicemailWaiting | DetectionPhase::VoicemailReady
        ) {
            if !state.voicemail_open {
                state.status.discarded_frames += 1;
                self.shared.publish(&mut state);
                return Ok(());
            }
            // A new post-interruption invocation is required even for an
            // uninterruptible source tail. Never refresh an old source scope.
            drop(state);
            return ctx.try_push_with_invocation_fence(frame, direction, &ctx.invocation_fence()?);
        }
        if state.status.retained_frames >= self.shared.config.max_frames
            || charge.bytes
                > self
                    .shared
                    .config
                    .max_frame_bytes
                    .saturating_sub(state.status.retained_frame_bytes)
        {
            drop(state);
            self.shared.fail(Failure::FrameLimit);
            return Err(error(Failure::FrameLimit));
        }
        if charge.pcm
            > self
                .shared
                .config
                .max_pcm_bytes
                .saturating_sub(state.status.retained_pcm_bytes)
            || charge.duration
                > self
                    .shared
                    .config
                    .max_pcm_duration
                    .saturating_sub(state.status.retained_pcm_duration)
        {
            drop(state);
            self.shared.fail(Failure::PcmLimit);
            return Err(error(Failure::PcmLimit));
        }
        // The supported compact topology is one noninterleaved response. Keep
        // original boundaries rather than synthesizing a replacement end.
        match &frame.data {
            FrameData::LlmResponseStart => {
                if state.response {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
                state.response = true;
            }
            FrameData::LlmResponseEnd => {
                if !state.response || state.tts_context.is_some() {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
                state.response = false;
            }
            FrameData::TtsStarted(start) => {
                if state.tts_context.is_some() {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
                let extra = start.context_id.as_ref().map_or(0, String::len);
                if charge.bytes.saturating_add(extra)
                    > self
                        .shared
                        .config
                        .max_frame_bytes
                        .saturating_sub(state.status.retained_frame_bytes)
                {
                    drop(state);
                    self.shared.fail(Failure::FrameLimit);
                    return Err(error(Failure::FrameLimit));
                }
                state.tts_context = Some(start.context_id.clone());
                state.topology_bytes = state
                    .tts_context
                    .as_ref()
                    .and_then(Option::as_ref)
                    .map_or(0, String::capacity);
                state.status.retained_frame_bytes += state.topology_bytes;
            }
            FrameData::TtsStopped(stop) => {
                if state.tts_context.as_ref() != Some(&stop.context_id) {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
                state.tts_context = None;
                state.status.retained_frame_bytes -= state.topology_bytes;
                state.topology_bytes = 0;
            }
            FrameData::TtsAudio(audio) | FrameData::SpeechOutputAudio(audio) => {
                if state.tts_context.as_ref() != Some(&audio.context_id) {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
            }
            FrameData::TtsText(text) if state.tts_context.is_some() => {
                if state.tts_context.as_ref() != Some(&text.context_id) {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
            }
            FrameData::AggregatedTextProgress(progress) if state.tts_context.is_some() => {
                if state.tts_context.as_ref() != Some(&progress.context_id) {
                    drop(state);
                    self.shared.fail(Failure::UnsupportedTopology);
                    return Err(error(Failure::UnsupportedTopology));
                }
            }
            FrameData::TtsAlignmentDiagnostic(info)
                if state.tts_context.as_ref().and_then(Option::as_ref)
                    != Some(&info.context_id) =>
            {
                drop(state);
                self.shared.fail(Failure::UnsupportedTopology);
                return Err(error(Failure::UnsupportedTopology));
            }
            _ => {}
        }
        self.shared.touch(&mut state);
        state.status.retained_frames += 1;
        state.status.retained_frame_bytes += charge.bytes;
        state.status.retained_pcm_bytes += charge.pcm;
        state.status.retained_pcm_duration += charge.duration;
        state.queue.push_back(Held {
            frame,
            direction,
            context: ctx.clone(),
            charge,
        });
        self.shared.publish(&mut state);
        self.shared.gate_wake.notify_waiters();
        self.shared.detector_wake.notify_one();
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for TtsGate {
    fn name(&self) -> &str {
        "VoicemailTtsGate"
    }
    async fn setup(&self, _: &StartFrame, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        {
            let mut state = self.shared.lock();
            let identity = ctx.processor_identity();
            if state.closing
                || state.gate.is_some()
                || state.detector.as_ref().is_none_or(|detector| {
                    detector.runtime_id != identity.runtime_id || detector.index >= identity.index
                })
            {
                drop(state);
                self.shared.fail(Failure::PairAttachment);
                return Err(error(Failure::PairAttachment));
            }
            state.gate = Some(identity);
            state.ready = true;
            state.status.supervisors += 1;
            self.shared.publish(&mut state);
        }
        let guard = SupervisorGuard {
            shared: self.shared.clone(),
            detector: false,
        };
        ctx.spawn(
            "voicemail-gate",
            false,
            supervise_gate(self.shared.clone(), ctx.clone(), guard),
        )?;
        self.shared.detector_wake.notify_one();
        Ok(())
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if frame.data.is_start() && {
            let state = self.shared.lock();
            !state.ready || state.closing
        } {
            self.shared.fail(Failure::PairAttachment);
            return Err(error(Failure::PairAttachment));
        }
        if terminal(&frame) {
            self.shared.close(!frame.data.is_cancel());
            if !frame.data.is_cancel()
                && direction == Direction::Downstream
                && ctx.has_downstream_processor()
            {
                let output = ctx.clone();
                ctx.spawn("voicemail-terminal", false, async move {
                    output.push_downstream_terminal_wait(frame).await
                })?;
                return Ok(());
            }
            return ctx.push(frame, direction).await;
        }
        if matches!(frame.data, FrameData::Interruption) {
            self.shared
                .interrupt(self.shared.voicemail_interruption.contains(&frame));
            return ctx.push(frame, direction).await;
        }
        if matches!(
            frame.data,
            FrameData::LlmFullResponseStart(_)
                | FrameData::LlmFullResponseEnd(_)
                | FrameData::VisionFullResponseStart(_)
                | FrameData::VisionFullResponseEnd(_)
        ) {
            self.shared.fail(Failure::UnsupportedTopology);
            return Err(error(Failure::UnsupportedTopology));
        }
        if gated(&frame.data) {
            let result = self.admit(frame, direction, ctx);
            if result.is_err() && !self.shared.lock().closing {
                self.shared.fail(Failure::FrameLimit);
            }
            result
        } else {
            ctx.push(frame, direction).await
        }
    }
    async fn prepare_end(&self, _: &ProcessorContext) -> Result<(), ProcessorError> {
        self.shared.close(true);
        Ok(())
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.shared.close(false);
        Ok(())
    }
}
impl Drop for TtsGate {
    fn drop(&mut self) {
        self.shared.close(false);
    }
}
