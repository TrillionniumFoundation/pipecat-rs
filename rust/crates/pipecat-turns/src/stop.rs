//! Native end-of-turn policies. All clocks are monotonic and caller-owned.
use crate::TurnStopReason;
use pipecat_core::{FrameData, ProcessorError};
use std::time::Duration;

/// A detector proposes completion; a deferred detector instead requests an
/// external verdict. It never admits conversational LLM inference by itself.
#[derive(Debug, Clone)]
pub struct UserTurnStopStrategy {
    kind: StopKind,
    deferred: bool,
}
#[derive(Debug, Clone)]
enum StopKind {
    SpeechTimeout {
        timeout: Duration,
        wait_for_transcript: bool,
    },
    TurnAnalyzer {
        wait_for_transcript: bool,
    },
    External {
        timeout: Duration,
        wait_for_transcript: bool,
    },
    ExternalCompletion,
}
impl UserTurnStopStrategy {
    pub fn speech_timeout(timeout: Duration, wait_for_transcript: bool) -> Self {
        Self {
            kind: StopKind::SpeechTimeout {
                timeout,
                wait_for_transcript,
            },
            deferred: false,
        }
    }
    /// The actual analyzer lives in `SmartTurnUserAggregator`. This policy
    /// consumes its verdict and does not manufacture a model classification.
    pub fn turn_analyzer(wait_for_transcript: bool) -> Self {
        Self {
            kind: StopKind::TurnAnalyzer {
                wait_for_transcript,
            },
            deferred: false,
        }
    }
    pub fn external(timeout: Duration, wait_for_transcript: bool) -> Self {
        Self {
            kind: StopKind::External {
                timeout,
                wait_for_transcript,
            },
            deferred: false,
        }
    }
    pub fn external_completion() -> Self {
        Self {
            kind: StopKind::ExternalCompletion,
            deferred: false,
        }
    }
    /// Request an external verdict while retaining accepted text in the open
    /// turn. No conversational context or speculative response is emitted.
    pub fn deferred(mut self) -> Self {
        self.deferred = true;
        self
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StopDecision {
    pub index: usize,
    pub deferred: bool,
    pub emit_speaking: bool,
    pub reason: TurnStopReason,
}

#[derive(Debug, Clone)]
struct StopState {
    has_text: bool,
    finalized: bool,
    interim: bool,
    vad_stopped: bool,
    model_complete: bool,
    speech_deadline: Option<Duration>,
    stt_deadline: Option<Duration>,
    external_deadline: Option<Duration>,
    external_emit: bool,
    external_speaking: bool,
    fired: bool,
}
impl Default for StopState {
    fn default() -> Self {
        Self {
            has_text: false,
            finalized: false,
            interim: false,
            vad_stopped: false,
            model_complete: false,
            speech_deadline: None,
            stt_deadline: None,
            external_deadline: None,
            external_emit: true,
            external_speaking: false,
            fired: false,
        }
    }
}
impl StopState {
    fn clear_pending(&mut self) {
        self.finalized = false;
        self.vad_stopped = false;
        self.model_complete = false;
        self.speech_deadline = None;
        self.stt_deadline = None;
        self.external_deadline = None;
        self.fired = false;
    }
}

/// Ordered OR policies. Each detector has independent timers, consumes its
/// deadline once, and is reset at both semantic turn boundaries. An empty list
/// leaves only the controller's watchdog and explicit terminal boundaries.
#[derive(Debug, Clone)]
pub struct UserTurnStopStrategies {
    entries: Vec<(UserTurnStopStrategy, StopState)>,
    active: bool,
    speaking: bool,
    stt_timeout: Duration,
    vad_stop_duration: Duration,
    now: Duration,
}
impl UserTurnStopStrategies {
    pub fn new(strategies: Vec<UserTurnStopStrategy>) -> Result<Self, ProcessorError> {
        if strategies.len() > 64 {
            return Err(ProcessorError::new(
                "at most 64 stop strategies are supported",
            ));
        }
        for strategy in &strategies {
            match strategy.kind {
                StopKind::External { timeout, .. } if timeout.is_zero() => {
                    return Err(ProcessorError::new(
                        "external transcript timeout must be positive",
                    ));
                }
                StopKind::SpeechTimeout { timeout, .. } | StopKind::External { timeout, .. }
                    if timeout > Duration::from_secs(86_400) =>
                {
                    return Err(ProcessorError::new(
                        "stop strategy timeout must not exceed 86400 seconds",
                    ));
                }
                StopKind::ExternalCompletion if strategy.deferred => {
                    return Err(ProcessorError::new(
                        "external completion finalizes a turn and cannot itself be deferred",
                    ));
                }
                _ => {}
            }
        }
        Ok(Self {
            entries: strategies
                .into_iter()
                .map(|s| (s, StopState::default()))
                .collect(),
            active: false,
            speaking: false,
            stt_timeout: Duration::ZERO,
            vad_stop_duration: Duration::from_millis(200),
            now: Duration::ZERO,
        })
    }
    pub fn resolves_proposed_turn_stop_frames(&self) -> bool {
        self.entries
            .iter()
            .any(|(s, _)| matches!(s.kind, StopKind::External { .. }))
    }
    pub fn uses_turn_analyzer(&self) -> bool {
        self.entries
            .iter()
            .any(|(s, _)| matches!(s.kind, StopKind::TurnAnalyzer { .. }))
    }
    pub fn accepts_external_completion(&self) -> bool {
        self.entries
            .iter()
            .any(|(s, _)| matches!(s.kind, StopKind::ExternalCompletion))
    }
    pub(crate) fn configure(&mut self, stt_timeout: Duration, vad_stop_duration: Duration) {
        self.stt_timeout = stt_timeout;
        self.vad_stop_duration = vad_stop_duration;
    }
    pub(crate) fn start(&mut self, speaking: bool, now: Duration) {
        self.active = true;
        self.speaking = speaking;
        self.now = self.now.max(now);
        for (_, state) in &mut self.entries {
            *state = StopState::default();
        }
    }
    pub(crate) fn stop(&mut self) {
        self.active = false;
        self.speaking = false;
        for (_, state) in &mut self.entries {
            *state = StopState::default();
        }
    }
    pub fn reset(&mut self) {
        self.stop();
        self.now = Duration::ZERO;
    }
    pub(crate) fn process(
        &mut self,
        data: &FrameData,
        now: Duration,
        speaking: bool,
        stt_safety_deadline: Option<Duration>,
    ) -> Vec<StopDecision> {
        self.now = self.now.max(now);
        let now = self.now;
        self.speaking = speaking;
        if !self.active {
            return vec![];
        }
        let mut decisions = vec![];
        for (index, (strategy, state)) in self.entries.iter_mut().enumerate() {
            match data {
                FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_)
                | FrameData::UserStartedSpeaking
                | FrameData::ProposedUserStartedSpeaking => {
                    state.clear_pending();
                    if matches!(strategy.kind, StopKind::External { .. })
                        && matches!(
                            data,
                            FrameData::UserStartedSpeaking | FrameData::ProposedUserStartedSpeaking
                        )
                    {
                        state.external_speaking = true;
                        state.external_emit =
                            matches!(data, FrameData::ProposedUserStartedSpeaking);
                    }
                }
                FrameData::VadUserStoppedSpeaking(_)
                | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                    state.vad_stopped = true;
                    state.fired = false;
                    state.stt_deadline = stt_safety_deadline;
                    state.speech_deadline = match strategy.kind {
                        StopKind::SpeechTimeout { timeout, .. } => {
                            Some(now.saturating_add(timeout))
                        }
                        _ => None,
                    };
                }
                FrameData::InterimTranscription(_) | FrameData::InterimTranscriptionDetailed(_) => {
                    state.finalized = false;
                    state.interim = true;
                }
                FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_) => {
                    let (text, finalized) = match data {
                        FrameData::Transcription(t) => (&t.text, t.finalized),
                        FrameData::TranscriptionDetailed(t) => (&t.text.text, t.finalized),
                        _ => unreachable!(),
                    };
                    if text.trim().is_empty() {
                        continue;
                    }
                    state.has_text = true;
                    state.finalized |= finalized;
                    state.interim = false;
                    state.fired = false;
                    match strategy.kind {
                        StopKind::SpeechTimeout { timeout, .. }
                            if !speaking && !state.vad_stopped =>
                        {
                            state.speech_deadline = Some(now.saturating_add(timeout));
                            state.stt_deadline = Some(now);
                        }
                        StopKind::TurnAnalyzer { .. } if !speaking && !state.vad_stopped => {
                            // Source transcript-only fallback has no acoustic verdict.
                            let ready = state.model_complete && finalized;
                            state.model_complete = true;
                            state.stt_deadline = Some(if ready {
                                now
                            } else {
                                now.saturating_add(
                                    self.stt_timeout.saturating_sub(self.vad_stop_duration),
                                )
                            });
                        }
                        StopKind::External { timeout, .. } => {
                            state.external_deadline = Some(now.saturating_add(timeout));
                        }
                        _ => {}
                    }
                }
                FrameData::UserStoppedSpeaking | FrameData::ProposedUserStoppedSpeaking => {
                    if matches!(strategy.kind, StopKind::External { .. }) {
                        state.external_speaking = false;
                        state.external_emit =
                            matches!(data, FrameData::ProposedUserStoppedSpeaking);
                        state.fired = false;
                        state.external_deadline = Some(now);
                    }
                }
                FrameData::UserTurnInferenceCompleted
                    if matches!(strategy.kind, StopKind::ExternalCompletion) && !speaking =>
                {
                    decisions.push(StopDecision {
                        index,
                        deferred: false,
                        emit_speaking: true,
                        reason: TurnStopReason::ExternalCompletion,
                    });
                }
                _ => {}
            }
        }
        decisions.extend(self.advance(now));
        decisions.sort_by_key(|decision| decision.index);
        decisions
    }
    pub(crate) fn model_complete(&mut self, now: Duration) -> Vec<StopDecision> {
        if self.active && !self.speaking {
            for (strategy, state) in &mut self.entries {
                if matches!(strategy.kind, StopKind::TurnAnalyzer { .. }) {
                    state.model_complete = true;
                }
            }
        }
        self.advance(now)
    }
    fn ready_at(
        strategy: &UserTurnStopStrategy,
        state: &StopState,
        now: Duration,
    ) -> Option<Duration> {
        if state.fired {
            return None;
        }
        match strategy.kind {
            StopKind::SpeechTimeout {
                wait_for_transcript,
                ..
            } => {
                if wait_for_transcript && !state.has_text {
                    return None;
                }
                let speech = state.speech_deadline?;
                // STT wait is independent of transcript presence, matching the source.
                Some(if state.finalized {
                    speech
                } else {
                    speech.max(state.stt_deadline?)
                })
            }
            StopKind::TurnAnalyzer {
                wait_for_transcript,
            } => {
                if !state.model_complete || (wait_for_transcript && !state.has_text) {
                    return None;
                }
                if !wait_for_transcript || (state.finalized && state.vad_stopped) {
                    Some(now)
                } else {
                    state.stt_deadline
                }
            }
            StopKind::External {
                wait_for_transcript,
                ..
            } => {
                if state.external_speaking
                    || (wait_for_transcript && (!state.has_text || state.interim))
                {
                    return None;
                }
                state.external_deadline
            }
            StopKind::ExternalCompletion => None,
        }
    }
    pub fn deadline(&self) -> Option<Duration> {
        if !self.active || self.speaking {
            return None;
        }
        self.entries
            .iter()
            .filter_map(|(s, state)| Self::ready_at(s, state, self.now))
            .min()
    }
    pub(crate) fn advance(&mut self, now: Duration) -> Vec<StopDecision> {
        self.now = self.now.max(now);
        let now = self.now;
        if !self.active || self.speaking {
            return vec![];
        }
        let mut decisions = vec![];
        for (index, (strategy, state)) in self.entries.iter_mut().enumerate() {
            if Self::ready_at(strategy, state, now).is_some_and(|d| now >= d) {
                state.fired = true;
                let (reason, emit_speaking) = match strategy.kind {
                    StopKind::External { .. } => (TurnStopReason::External, state.external_emit),
                    StopKind::TurnAnalyzer { .. } if state.vad_stopped => {
                        (TurnStopReason::ModelCompletion, true)
                    }
                    _ => (TurnStopReason::SpeechTimeout, true),
                };
                decisions.push(StopDecision {
                    index,
                    deferred: strategy.deferred,
                    emit_speaking,
                    reason,
                });
            }
        }
        decisions
    }
}
