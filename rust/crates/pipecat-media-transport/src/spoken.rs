//! Opt-in, whole-context playback attestation for a single output destination.
//!
//! A successful source response, a matching final TTS stop, and explicit Played
//! receipts for every transformed PCM write are independent requirements. This
//! ledger never infers word alignment or playback from a traversing text frame.
use pipecat_core::{
    async_trait, AggregatedTextFrame, DirectedFrame, Direction, Frame, FrameData, FrameProcessor,
    InvocationFence, ProcessorContext, ProcessorError,
};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

const TOKEN: &str = "pipecat.media.spoken.response";
const SOURCE: &str = "pipecat.media.spoken.source";

/// Bounds include lifetime replay protection. Once the context-ID budget is
/// exhausted, attestation remains disabled for this ledger; IDs are not evicted.
#[derive(Debug, Clone)]
pub struct SpokenHistoryConfig {
    pub max_pending_responses: usize,
    pub max_context_ids: usize,
    pub max_source_frame_ids: usize,
    pub max_context_id_bytes: usize,
    pub max_text_bytes: usize,
    pub max_frames_per_response: usize,
    /// Ordered frame and completion-record effects from one synchronous commit.
    pub max_effect_frames: usize,
}
impl Default for SpokenHistoryConfig {
    fn default() -> Self {
        Self {
            max_pending_responses: 16,
            max_context_ids: 4096,
            max_source_frame_ids: 8192,
            max_context_id_bytes: 256,
            max_text_bytes: 1024 * 1024,
            max_frames_per_response: 8192,
            max_effect_frames: 16,
        }
    }
}

/// A whole synthesis context verified at the backend's playback boundary.
/// This is not a claim of physical-device audibility or word-level alignment.
#[derive(Debug)]
pub struct SpokenContext {
    pub context_id: String,
    pub texts: Vec<AggregatedTextFrame>,
}

/// Caller-owned readiness wait; the media sender supplies cancellation and a deadline.
pub type SpokenHistoryReadiness =
    std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), ProcessorError>> + Send>>;

/// Synchronous context mutation is serialized with `SpokenHistory::invalidate`.
///
/// Implementations must commit before returning, must not reenter the ledger,
/// and should return promptly. Return any downstream context-update frames (or
/// upstream inference frames) after the mutation; these are emitted outside the
/// lock. Never defer the actual history mutation to those frames.
///
/// With `assistant-history`, use `AssistantSpokenHistorySink` to preserve both
/// downstream source/TTS readiness and ordered completion-record admission.
/// Custom frame-only sinks retain their existing behavior and do not acquire
/// either guarantee. Do not also submit these texts with `spoken=true`.
pub trait SpokenHistorySink: Send + Sync {
    /// Register before the exact source/TTS start is forwarded downstream. The
    /// future is awaited outside all locks by the existing media sender before
    /// any following PCM is submitted. Custom frame-only sinks need no barrier.
    fn readiness(
        &self,
        _frame: &Frame,
        _cause: Option<InvocationFence>,
    ) -> Result<Option<SpokenHistoryReadiness>, ProcessorError> {
        Ok(None)
    }
    fn commit(&self, context: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError>;
    /// Ordered effects are emitted outside the ledger lock. The default keeps
    /// existing frame-only sinks source compatible.
    fn commit_ordered(
        &self,
        context: SpokenContext,
    ) -> Result<Vec<SpokenHistoryEffect>, ProcessorError> {
        self.commit(context).map(|frames| {
            frames
                .into_iter()
                .map(|frame| SpokenHistoryEffect::Frame(Box::new(frame)))
                .collect()
        })
    }
}

/// The narrow ordered effects accepted from a synchronous history commit.
#[derive(Debug)]
pub enum SpokenHistoryEffect {
    Frame(Box<DirectedFrame>),
    #[cfg(feature = "assistant-history")]
    AssistantStopped(pipecat_turns::AssistantStoppedAction),
}

/// Native adapter for playback-confirmed assistant history and completion data.
/// It does not attest physical audibility or broaden the ledger's receipt fence.
#[cfg(feature = "assistant-history")]
pub struct AssistantSpokenHistorySink {
    assistant: Arc<pipecat_turns::LLMAssistantAggregator>,
}
#[cfg(feature = "assistant-history")]
impl AssistantSpokenHistorySink {
    pub fn new(assistant: Arc<pipecat_turns::LLMAssistantAggregator>) -> Self {
        Self { assistant }
    }
}
#[cfg(feature = "assistant-history")]
impl SpokenHistorySink for AssistantSpokenHistorySink {
    fn readiness(
        &self,
        frame: &Frame,
        cause: Option<InvocationFence>,
    ) -> Result<Option<SpokenHistoryReadiness>, ProcessorError> {
        let cause = cause.ok_or_else(|| {
            ProcessorError::new("assistant playback lacks a valid source boundary")
        })?;
        Ok(self
            .assistant
            .playback_readiness(frame, cause)?
            .map(|ready| Box::pin(ready.wait()) as SpokenHistoryReadiness))
    }
    fn commit(&self, _context: SpokenContext) -> Result<Vec<DirectedFrame>, ProcessorError> {
        Err(ProcessorError::new(
            "assistant history requires ordered effect admission",
        ))
    }
    fn commit_ordered(
        &self,
        context: SpokenContext,
    ) -> Result<Vec<SpokenHistoryEffect>, ProcessorError> {
        self.assistant
            .confirm_playback(&context.context_id, &context.texts)?
            .actions
            .into_iter()
            .map(|action| match action {
                pipecat_turns::AggregationAction::Push(frame) => {
                    Ok(SpokenHistoryEffect::Frame(Box::new(frame)))
                }
                pipecat_turns::AggregationAction::AssistantStopped(action) => {
                    Ok(SpokenHistoryEffect::AssistantStopped(action))
                }
                _ => Err(ProcessorError::new(
                    "unsupported assistant playback history effect",
                )),
            })
            .collect()
    }
}

/// Inspectable fail-closed history status. Custom frame-only sinks preserve
/// normal playback. The explicit assistant adapter additionally requires
/// downstream boundary readiness before admitting audio.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SpokenHistoryStatus {
    pub pending_responses: usize,
    pub pending_source_responses: usize,
    pub buffered_text_bytes: usize,
    pub retained_context_ids: usize,
    pub retained_source_frame_ids: usize,
    pub committed_contexts: u64,
    pub withheld_contexts: u64,
    pub exhausted: bool,
    pub last_rejection: Option<String>,
}

struct Pending {
    cause: InvocationFence,
    context: ProcessorContext,
    context_id: Option<String>,
    append_to_context: bool,
    texts: Vec<AggregatedTextFrame>,
    text_bytes: usize,
    frame_ids: BTreeSet<u64>,
    accepted_audio: usize,
    processed_audio: usize,
    played: bool,
    stopped: bool,
    drained: bool,
    end_id: Option<u64>,
    invalid: bool,
}
impl Pending {
    fn new(cause: InvocationFence, context: ProcessorContext) -> Self {
        Self {
            cause,
            context,
            context_id: None,
            append_to_context: true,
            texts: vec![],
            text_bytes: 0,
            frame_ids: BTreeSet::new(),
            accepted_audio: 0,
            processed_audio: 0,
            played: false,
            stopped: false,
            drained: false,
            end_id: None,
            invalid: false,
        }
    }
}
struct SourceResponse {
    cause: InvocationFence,
    start_id: u64,
    end_id: Option<u64>,
    invalid: bool,
}
#[derive(Default)]
struct State {
    bound: bool,
    source_bound: bool,
    next_source: u64,
    source_current: Option<u64>,
    source_frames: BTreeSet<u64>,
    sources: BTreeMap<u64, SourceResponse>,
    current: Option<u64>,
    pending: BTreeMap<u64, Pending>,
    seen: BTreeSet<String>,
    status: SpokenHistoryStatus,
}
pub(crate) struct SpokenEffects {
    pub ordered: Vec<SpokenHistoryEffect>,
    pub cause: InvocationFence,
    pub context: ProcessorContext,
}

struct Inner {
    config: SpokenHistoryConfig,
    sink: Arc<dyn SpokenHistorySink>,
    state: Mutex<State>,
}

/// Cloneable immediate-invalidation handle for one OutputTransport.
///
/// Call `invalidate()` in an input/control path *before* admitting interruption
/// or cancellation to the pipeline. That establishes an ordering against the
/// synchronous commit callback. Output-frame processing also invalidates as a
/// fallback, but cannot observe an interruption still queued elsewhere.
///
/// Install `source_boundary()` before TTS and bind this ledger to output after
/// TTS. Only a noninterleaved TTS context enclosed in one LLM response is supported.
/// A standalone TtsSpeak/TtsStopped has no reliable source-success fence and is
/// withheld. Partial playback is always withheld. IDs cannot be reused, even
/// after interruption. Use a fresh ledger only for a genuinely new session.
#[derive(Clone)]
pub struct SpokenHistory(Arc<Inner>);
impl SpokenHistory {
    pub fn new(
        config: SpokenHistoryConfig,
        sink: Arc<dyn SpokenHistorySink>,
    ) -> Result<Self, ProcessorError> {
        if config.max_pending_responses == 0
            || config.max_context_ids == 0
            || config.max_source_frame_ids == 0
            || config.max_context_id_bytes == 0
            || config.max_text_bytes == 0
            || config.max_frames_per_response == 0
            || config.max_effect_frames == 0
        {
            return Err(ProcessorError::new("spoken-history bounds must be nonzero"));
        }
        Ok(Self(Arc::new(Inner {
            config,
            sink,
            state: Mutex::new(State::default()),
        })))
    }
    /// Place this boundary before asynchronous TTS synthesis and the bound
    /// OutputTransport after synthesis. TTS must preserve the original LLM
    /// start/end frames and their metadata, as the native Cartesia processor does.
    /// The boundary ties source success to one original response generation.
    pub fn source_boundary(
        &self,
        name: impl Into<String>,
    ) -> Result<SpokenSourceBoundary, ProcessorError> {
        let mut state = self.0.state.lock().unwrap();
        if state.source_bound {
            return Err(ProcessorError::new(
                "spoken-history source boundary already bound",
            ));
        }
        state.source_bound = true;
        Ok(SpokenSourceBoundary {
            name: name.into(),
            history: self.clone(),
        })
    }
    fn source_frame(
        &self,
        frame: &mut Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        if matches!(
            frame.data,
            FrameData::Interruption
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
                | FrameData::Error(_)
        ) {
            self.invalidate();
            return Ok(());
        }
        if direction != Direction::Downstream {
            return Ok(());
        }
        frame.metadata.remove(SOURCE);
        if !response_start(&frame.data) && !response_end(&frame.data) {
            return Ok(());
        }
        let mut state = self.0.state.lock().unwrap();
        if state.status.exhausted {
            return Ok(());
        }
        if state.source_frames.contains(&frame.id) {
            for source in state.sources.values_mut() {
                source.invalid = true;
            }
            reject_all(&mut state, "replayed source response frame identity");
            return Ok(());
        }
        if state.source_frames.len() >= self.0.config.max_source_frame_ids {
            exhaust(
                &mut state,
                "spoken-history source identity capacity exhausted",
            );
            return Ok(());
        }
        state.source_frames.insert(frame.id);
        if response_start(&frame.data) {
            let overlapping = state.source_current.is_some();
            if overlapping {
                for source in state.sources.values_mut() {
                    source.invalid = true;
                }
                reject_all(&mut state, "interleaved source responses");
            }
            if state.sources.len() >= self.0.config.max_pending_responses
                || state.next_source == u64::MAX
            {
                exhaust(
                    &mut state,
                    "spoken-history source response capacity exhausted",
                );
                return Ok(());
            }
            state.next_source += 1;
            let token = state.next_source;
            state.sources.insert(
                token,
                SourceResponse {
                    cause: ctx.invocation_fence()?,
                    start_id: frame.id,
                    end_id: None,
                    invalid: overlapping,
                },
            );
            state.source_current = Some(token);
            frame.metadata.insert(SOURCE.into(), token.into());
        } else if let Some(token) = state.source_current.take() {
            if let Some(source) = state.sources.get_mut(&token) {
                source.cause = source.cause.intersection(&ctx.invocation_fence()?)?;
                source.end_id = Some(frame.id);
                frame.metadata.insert(SOURCE.into(), token.into());
            }
        }
        Ok(())
    }
    pub(crate) fn readiness(
        &self,
        frame: &Frame,
    ) -> Result<Option<SpokenHistoryReadiness>, ProcessorError> {
        if !response_start(&frame.data) && !matches!(frame.data, FrameData::TtsStarted(_)) {
            return Ok(None);
        }
        let cause = {
            let state = self.0.state.lock().unwrap();
            frame
                .metadata
                .get(TOKEN)
                .and_then(serde_json::Value::as_u64)
                .and_then(|token| state.pending.get(&token))
                .filter(|pending| !pending.invalid)
                .map(|pending| pending.cause.clone())
        };
        self.0.sink.readiness(frame, cause)
    }
    pub fn status(&self) -> SpokenHistoryStatus {
        let state = self.0.state.lock().unwrap();
        let mut status = state.status.clone();
        status.pending_responses = state.pending.len();
        status.pending_source_responses = state.sources.len();
        status.retained_context_ids = state.seen.len();
        status.retained_source_frame_ids = state.source_frames.len();
        status
    }
    /// Discard all uncommitted evidence, atomically ordered against a commit.
    /// Already committed, fully played contexts are retained by the sink.
    pub fn invalidate(&self) {
        self.invalidate_reason("interrupted or cancelled playback");
    }
    pub(crate) fn invalidate_reason(&self, reason: &str) {
        let mut state = self.0.state.lock().unwrap();
        let withheld = state
            .pending
            .values()
            .filter(|p| p.context_id.is_some())
            .count() as u64;
        state.status.withheld_contexts = state.status.withheld_contexts.saturating_add(withheld);
        state.pending.clear();
        state.sources.clear();
        state.source_current = None;
        state.current = None;
        state.status.buffered_text_bytes = 0;
        if withheld > 0 {
            state.status.last_rejection = Some(reason.into());
        }
    }
    pub(crate) fn bind(&self) -> Result<(), ProcessorError> {
        let mut state = self.0.state.lock().unwrap();
        if state.bound {
            return Err(ProcessorError::new(
                "spoken-history ledger already bound to an output",
            ));
        }
        state.bound = true;
        Ok(())
    }
    /// Assign an internal response token before media queue admission. Tokens
    /// supplied by callers are removed; transformed chunks inherit this token.
    pub(crate) fn admit(
        &self,
        frame: &mut Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        frame.metadata.remove(TOKEN);
        if matches!(
            frame.data,
            FrameData::Interruption
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
                | FrameData::Error(_)
        ) {
            self.invalidate();
            return Ok(());
        }
        if direction != Direction::Downstream {
            return Ok(());
        }
        if source_context(&frame.data).is_none()
            && !response_start(&frame.data)
            && !response_end(&frame.data)
            && !matches!(frame.data, FrameData::Volume(_) | FrameData::VolumeGain(_))
        {
            return Ok(());
        }
        let mut state = self.0.state.lock().unwrap();
        if state.status.exhausted {
            return Ok(());
        }
        if response_start(&frame.data) || response_end(&frame.data) {
            let source_token = frame
                .metadata
                .get(SOURCE)
                .and_then(serde_json::Value::as_u64);
            let valid = source_token
                .and_then(|token| state.sources.get(&token))
                .is_some_and(|source| {
                    !source.invalid
                        && if response_start(&frame.data) {
                            source.start_id == frame.id
                        } else {
                            source.end_id == Some(frame.id)
                        }
                });
            if !valid || (response_end(&frame.data) && source_token != state.current) {
                reject_all(
                    &mut state,
                    "missing, stale, or mismatched source response boundary",
                );
                return Ok(());
            }
        }
        // Late material for a response whose end is already queued still
        // invalidates that response until its atomic commit has happened.
        if let Some(context) = source_context(&frame.data) {
            let late: Vec<_> = state
                .pending
                .iter()
                .filter_map(|(token, pending)| {
                    (pending.end_id.is_some() && context == pending.context_id.as_deref())
                        .then_some(*token)
                })
                .collect();
            for token in late {
                reject(&mut state, token, "late material after source response end");
            }
        }
        if let FrameData::TtsStarted(start) = &frame.data {
            let Some(id) = start
                .context_id
                .as_deref()
                .filter(|id| !id.is_empty() && id.len() <= self.0.config.max_context_id_bytes)
            else {
                reject_all(&mut state, "missing or oversized TTS context ID");
                return Ok(());
            };
            if state.seen.contains(id) {
                reject_all(&mut state, "reused TTS context ID");
                return Ok(());
            }
            if state.seen.len() >= self.0.config.max_context_ids {
                exhaust(
                    &mut state,
                    "spoken-history replay-protection capacity exhausted",
                );
                return Ok(());
            }
            state.seen.insert(id.into());
        }
        if response_start(&frame.data) {
            if state.current.is_some() {
                reject_all(&mut state, "interleaved LLM responses");
            }
            if state.pending.len() >= self.0.config.max_pending_responses {
                exhaust(&mut state, "spoken-history response capacity exhausted");
                return Ok(());
            }
            let token = frame
                .metadata
                .get(SOURCE)
                .and_then(serde_json::Value::as_u64)
                .unwrap();
            if state.pending.contains_key(&token) {
                reject_all(&mut state, "duplicate source response start");
                return Ok(());
            }
            let cause = state.sources[&token]
                .cause
                .intersection(&ctx.invocation_fence()?)?;
            let mut pending = Pending::new(cause, ctx.clone());
            // A new response cannot certify an unfinished preceding response.
            pending.invalid = state.current.is_some();
            state.pending.insert(token, pending);
            state.current = Some(token);
        }
        let Some(token) = state.current else {
            if matches!(
                frame.data,
                FrameData::TtsAudio(_)
                    | FrameData::OutputAudio(_)
                    | FrameData::SpeechOutputAudio(_)
            ) && source_context(&frame.data) == Some(None)
            {
                reject_all(&mut state, "unattributed PCM before playback confirmation");
            }
            if matches!(
                frame.data,
                FrameData::TtsStarted(_) | FrameData::TtsText(_) | FrameData::TtsStopped(_)
            ) {
                state.status.last_rejection =
                    Some("standalone or late TTS lacks a matching successful LLM response".into());
            }
            return Ok(());
        };
        let mut cause = state.pending[&token]
            .cause
            .intersection(&ctx.invocation_fence()?)?;
        if response_end(&frame.data) {
            cause = cause.intersection(&state.sources[&token].cause)?;
        }
        state.pending.get_mut(&token).unwrap().cause = cause;
        frame.metadata.insert(TOKEN.into(), token.into());
        if frame
            .metadata
            .get("failed")
            .is_some_and(|failed| failed.as_bool() != Some(false))
        {
            reject(&mut state, token, "source synthesis response failed");
        }
        let config = &self.0.config;
        if state.pending[&token].invalid {
            if response_end(&frame.data) {
                state.pending.get_mut(&token).unwrap().end_id = Some(frame.id);
                state.current = None;
            }
            return Ok(());
        }
        if let FrameData::TtsStarted(start) = &frame.data {
            if state.pending[&token].context_id.is_some() {
                reject(&mut state, token, "interleaved TTS contexts");
                return Ok(());
            }
            let pending = state.pending.get_mut(&token).unwrap();
            pending.context_id = start.context_id.clone();
            pending.append_to_context = start.append_to_context;
        }
        let Some(pending) = state.pending.get_mut(&token) else {
            return Ok(());
        };
        if pending.frame_ids.len() >= config.max_frames_per_response
            || !pending.frame_ids.insert(frame.id)
        {
            reject(
                &mut state,
                token,
                "duplicate frame or spoken-history frame capacity exceeded",
            );
            if response_end(&frame.data) {
                state.pending.get_mut(&token).unwrap().end_id = Some(frame.id);
                state.current = None;
            }
            return Ok(());
        }
        match &frame.data {
            FrameData::TtsText(text) => {
                let bytes = text
                    .text
                    .text
                    .len()
                    .saturating_add(text.raw_text.as_ref().map_or(0, String::len))
                    .saturating_add(text.aggregated_by.len())
                    .saturating_add(text.context_id.as_ref().map_or(0, String::len));
                if pending.stopped
                    || pending.context_id.is_none()
                    || text.context_id != pending.context_id
                    || !text.will_be_spoken
                {
                    reject(
                        &mut state,
                        token,
                        "TTS text does not match its open speech context",
                    );
                } else if bytes
                    > config
                        .max_text_bytes
                        .saturating_sub(state.status.buffered_text_bytes)
                {
                    reject(&mut state, token, "spoken-history text capacity exceeded");
                } else {
                    let pending = state.pending.get_mut(&token).unwrap();
                    if !pending.invalid {
                        pending.text_bytes += bytes;
                        let mut text = text.clone();
                        text.text.append_to_context &= pending.append_to_context;
                        pending.texts.push(text);
                        state.status.buffered_text_bytes += bytes;
                    }
                }
            }
            FrameData::TtsAudio(audio)
            | FrameData::OutputAudio(audio)
            | FrameData::SpeechOutputAudio(audio) => {
                if pending.stopped
                    || pending.context_id.is_none()
                    || audio.context_id != pending.context_id
                    || audio.audio.is_empty()
                    || audio.validate().is_err()
                {
                    reject(
                        &mut state,
                        token,
                        "unknown, late, empty, or invalid context PCM",
                    );
                } else {
                    pending.accepted_audio += 1;
                }
            }
            FrameData::TtsStopped(stop) => {
                if pending.stopped
                    || pending.context_id.is_none()
                    || stop.context_id != pending.context_id
                {
                    reject(
                        &mut state,
                        token,
                        "TTS stop does not match its open speech context",
                    );
                } else {
                    pending.stopped = true;
                }
            }
            FrameData::Volume(value) | FrameData::VolumeGain(value)
                if *value == 0.0 || !value.is_finite() =>
            {
                reject(&mut state, token, "muted or invalid output gain");
            }
            _ if response_end(&frame.data) => {
                pending.end_id = Some(frame.id);
                state.current = None;
            }
            _ => {}
        }
        Ok(())
    }
    pub(crate) fn rejected_frame(&self, frame: &Frame, reason: &str) {
        if let Some(token) = token(frame) {
            reject(&mut self.0.state.lock().unwrap(), token, reason);
        }
    }
    pub(crate) fn processed(&self, frame: &Frame) {
        let Some(token) = token(frame) else {
            return;
        };
        let mut state = self.0.state.lock().unwrap();
        let Some(pending) = state.pending.get_mut(&token) else {
            return;
        };
        match &frame.data {
            FrameData::TtsAudio(_)
            | FrameData::OutputAudio(_)
            | FrameData::SpeechOutputAudio(_) => pending.processed_audio += 1,
            FrameData::TtsStopped(stop) if stop.context_id == pending.context_id => {
                pending.drained = true
            }
            _ => {}
        }
    }
    pub(crate) fn played(&self, frame: &Frame) {
        let Some(token) = token(frame) else {
            return;
        };
        let mut state = self.0.state.lock().unwrap();
        if let Some(pending) = state.pending.get_mut(&token) {
            pending.played = true;
        }
    }
    pub(crate) fn complete(&self, frame: &Frame) -> Result<Option<SpokenEffects>, ProcessorError> {
        if !response_end(&frame.data) {
            return Ok(None);
        }
        let Some(token) = token(frame) else {
            return Ok(None);
        };
        let mut state = self.0.state.lock().unwrap();
        let Some(pending) = state.pending.remove(&token) else {
            return Ok(None);
        };
        state.sources.remove(&token);
        state.status.buffered_text_bytes = state
            .status
            .buffered_text_bytes
            .saturating_sub(pending.text_bytes);
        let valid = !pending.invalid
            && pending.end_id == Some(frame.id)
            && pending.stopped
            && pending.drained
            && pending.played
            && pending.accepted_audio > 0
            && pending.accepted_audio == pending.processed_audio
            && !pending.texts.is_empty();
        let Some(context_id) = pending.context_id else {
            return Ok(None);
        };
        if !valid {
            state.status.withheld_contexts = state.status.withheld_contexts.saturating_add(1);
            state
                .status
                .last_rejection
                .get_or_insert_with(|| "incomplete context or playback proof".into());
            return Ok(None);
        }
        // This lock is also held by immediate input invalidation. Checking the
        // generation and mutating history are one indivisible operation.
        let effects = self.0.sink.commit_ordered(SpokenContext {
            context_id,
            texts: pending.texts,
        })?;
        state.status.committed_contexts = state.status.committed_contexts.saturating_add(1);
        if effects.len() > self.0.config.max_effect_frames {
            return Err(ProcessorError::new(
                "spoken-history sink returned too many effect frames",
            ));
        }
        Ok(Some(SpokenEffects {
            ordered: effects,
            cause: pending.cause,
            context: pending.context,
        }))
    }
}
fn token(frame: &Frame) -> Option<u64> {
    frame
        .metadata
        .get(TOKEN)
        .and_then(serde_json::Value::as_u64)
}
fn response_start(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::LlmResponseStart
            | FrameData::LlmFullResponseStart(_)
            | FrameData::VisionFullResponseStart(_)
    )
}
fn response_end(data: &FrameData) -> bool {
    matches!(
        data,
        FrameData::LlmResponseEnd
            | FrameData::LlmFullResponseEnd(_)
            | FrameData::VisionFullResponseEnd(_)
    )
}
fn reject(state: &mut State, token: u64, reason: &str) {
    if let Some(pending) = state.pending.get_mut(&token) {
        pending.invalid = true;
        state.status.buffered_text_bytes = state
            .status
            .buffered_text_bytes
            .saturating_sub(pending.text_bytes);
        pending.text_bytes = 0;
        pending.texts.clear();
    }
    state.status.last_rejection = Some(reason.into());
}
fn reject_all(state: &mut State, reason: &str) {
    state.status.last_rejection = Some(reason.into());
    let tokens: Vec<_> = state.pending.keys().copied().collect();
    for token in tokens {
        reject(state, token, reason);
    }
}
fn exhaust(state: &mut State, reason: &str) {
    reject_all(state, reason);
    state.status.exhausted = true;
    state.pending.clear();
    state.sources.clear();
    state.source_current = None;
    state.current = None;
}

fn source_context(data: &FrameData) -> Option<Option<&str>> {
    match data {
        FrameData::TtsStarted(start) => Some(start.context_id.as_deref()),
        FrameData::TtsText(text) => Some(text.context_id.as_deref()),
        FrameData::TtsStopped(stop) => Some(stop.context_id.as_deref()),
        FrameData::TtsAudio(audio)
        | FrameData::OutputAudio(audio)
        | FrameData::SpeechOutputAudio(audio) => Some(audio.context_id.as_deref()),
        _ => None,
    }
}

/// Source-generation boundary for an opt-in SpokenHistory ledger.
/// Place before TTS; the associated OutputTransport belongs after TTS.
pub struct SpokenSourceBoundary {
    name: String,
    history: SpokenHistory,
}
#[async_trait]
impl FrameProcessor for SpokenSourceBoundary {
    fn name(&self) -> &str {
        &self.name
    }
    async fn process(
        &self,
        mut frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        self.history.source_frame(&mut frame, direction, ctx)?;
        ctx.push(frame, direction).await
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.history
            .invalidate_reason("source closed with incomplete responses");
        Ok(())
    }
}
