//! Deterministic VAD/transcription turn detection; no audio model inference.
use crate::{
    UserStartStrategyEvent, UserTurnStartStrategies, UserTurnStartedParams, UserTurnStopStrategies,
};
use std::sync::atomic::{AtomicU64, Ordering};
static NEXT_INFERENCE_REQUEST: AtomicU64 = AtomicU64::new(1);
use pipecat_core::FrameData;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone)]
pub struct UserTurnConfig {
    /// Silence policy floor, independent of STT finalization.
    pub speech_timeout: Duration,
    /// STT P99 latency measured from the physical end of speech.
    pub stt_timeout: Duration,
    /// Silence already observed by the VAD when its stop frame arrives.
    pub vad_stop_duration: Duration,
    pub stop_timeout: Duration,
    pub wait_for_transcript: bool,
    pub start_on_transcription: bool,
    /// Resolve external proposals as well as adopting real speaking frames.
    pub resolve_proposals: bool,
    pub enable_interruptions: bool,
    /// Require an acoustic model verdict after VAD has observed this turn.
    pub require_model_completion: bool,
    /// Ordered opt-in start policies. None preserves the native default starts,
    /// including adoption of already-announced external boundaries.
    pub start_strategies: Option<UserTurnStartStrategies>,
    /// Explicit stop policies; None preserves the native default controller.
    pub stop_strategies: Option<UserTurnStopStrategies>,
}
impl Default for UserTurnConfig {
    fn default() -> Self {
        Self {
            speech_timeout: Duration::from_millis(600),
            stt_timeout: Duration::ZERO,
            vad_stop_duration: Duration::from_millis(200),
            stop_timeout: Duration::from_secs(5),
            wait_for_transcript: true,
            start_on_transcription: true,
            resolve_proposals: false,
            enable_interruptions: true,
            require_model_completion: false,
            start_strategies: None,
            stop_strategies: None,
        }
    }
}
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TurnStopReason {
    SpeechTimeout,
    ModelCompletion,
    External,
    ExternalCompletion,
    Watchdog,
    SessionEnd,
    Muted,
}
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum UserTurnEvent {
    /// Request an out-of-band verdict, without finalizing or committing text.
    InferenceRequested {
        request_id: u64,
        strategy_index: usize,
        reason: TurnStopReason,
    },
    ResetAggregation,
    StartStrategy(UserStartStrategyEvent),
    Started {
        emit_speaking: bool,
        interrupt: bool,
    },
    Stopped {
        emit_speaking: bool,
        reason: TurnStopReason,
    },
}

/// A caller supplies a monotonic clock. `advance` is explicit so golden traces
/// and delayed/out-of-order delivery do not depend on wall-clock sleeps.
#[derive(Debug, Clone)]
pub struct UserTurnController {
    config: UserTurnConfig,
    start_strategies: Option<UserTurnStartStrategies>,
    stop_strategies: Option<UserTurnStopStrategies>,
    pending_inference_request: Option<u64>,
    active: bool,
    model_complete: bool,
    vad_observed: bool,
    speaking: bool,
    has_text: bool,
    finalized: bool,
    interim_pending: bool,
    vad_stopped_at: Option<Duration>,
    speech_deadline: Option<Duration>,
    stt_deadline: Option<Duration>,
    stt_safety_deadline: Option<Duration>,
    watchdog_deadline: Option<Duration>,
    external_stop: Option<bool>,
    now: Duration,
}
impl UserTurnController {
    pub fn new(config: UserTurnConfig) -> Self {
        let mut start_strategies = config.start_strategies.clone();
        if let Some(strategies) = &mut start_strategies {
            strategies.reset();
        }
        let mut stop_strategies = config.stop_strategies.clone();
        if let Some(strategies) = &mut stop_strategies {
            strategies.reset();
            strategies.configure(config.stt_timeout, config.vad_stop_duration);
        }
        Self {
            config,
            stop_strategies,
            pending_inference_request: None,
            start_strategies,
            active: false,
            model_complete: false,
            vad_observed: false,
            speaking: false,
            has_text: false,
            finalized: false,
            interim_pending: false,
            vad_stopped_at: None,
            speech_deadline: None,
            stt_deadline: None,
            stt_safety_deadline: None,
            watchdog_deadline: None,
            external_stop: None,
            now: Duration::ZERO,
        }
    }
    pub fn config(&self) -> &UserTurnConfig {
        &self.config
    }
    pub(crate) fn apply_realtime_mode(&mut self) {
        self.config.start_on_transcription = false;
        self.config.wait_for_transcript = false;
        for strategies in [
            &mut self.config.start_strategies,
            &mut self.start_strategies,
        ]
        .into_iter()
        .flatten()
        {
            strategies.remove_transcription_start();
        }
        for strategies in [&mut self.config.stop_strategies, &mut self.stop_strategies]
            .into_iter()
            .flatten()
        {
            strategies.disable_transcript_wait();
        }
    }
    pub(crate) fn replace_strategies(
        &mut self,
        start: Option<UserTurnStartStrategies>,
        stop: Option<UserTurnStopStrategies>,
    ) {
        self.pending_inference_request = None;
        self.config.start_strategies = start.clone();
        self.config.stop_strategies = stop.clone();
        self.start_strategies = start;
        self.stop_strategies = stop;
        if let Some(strategies) = &mut self.stop_strategies {
            strategies.configure(self.config.stt_timeout, self.config.vad_stop_duration);
            if self.active {
                strategies.start(self.speaking, self.now);
            }
        }
    }
    pub(crate) fn configure_model_gate(&mut self) -> Result<(), pipecat_core::ProcessorError> {
        if self.active {
            return Err(pipecat_core::ProcessorError::new(
                "configure the model gate before a user turn starts",
            ));
        }
        if self
            .stop_strategies
            .as_ref()
            .is_some_and(|s| !s.uses_turn_analyzer())
        {
            return Err(pipecat_core::ProcessorError::new(
                "Smart Turn requires a turn_analyzer stop strategy in an explicit stop list",
            ));
        }
        self.config.require_model_completion = true;
        self.config.speech_timeout = Duration::ZERO;
        Ok(())
    }
    pub fn is_active(&self) -> bool {
        self.active
    }
    pub fn is_speaking(&self) -> bool {
        self.speaking
    }
    pub fn resolves_proposed_turn_start_frames(&self) -> bool {
        self.start_strategies.as_ref().map_or(
            self.config.resolve_proposals,
            UserTurnStartStrategies::resolves_proposed_turn_start_frames,
        )
    }
    pub fn resolves_proposed_turn_stop_frames(&self) -> bool {
        self.stop_strategies.as_ref().map_or(
            self.config.resolve_proposals,
            UserTurnStopStrategies::resolves_proposed_turn_stop_frames,
        )
    }
    pub fn pending_inference_request(&self) -> Option<u64> {
        self.pending_inference_request
    }
    /// Correlated verdicts cannot finalize a resumed or revised user turn.
    pub fn complete_inference(&mut self, request_id: u64, now: Duration) -> Vec<UserTurnEvent> {
        if self.pending_inference_request != Some(request_id)
            || self.speaking
            || !self
                .stop_strategies
                .as_ref()
                .is_some_and(UserTurnStopStrategies::accepts_external_completion)
        {
            return vec![];
        }
        self.now = self.now.max(now);
        self.finish(true, TurnStopReason::ExternalCompletion)
    }
    /// Reinitialize a standalone controller for a new monotonic-clock session.
    /// Pipeline aggregators are single-session processors and use cleanup.
    pub fn reset(&mut self) {
        *self = Self::new(self.config.clone());
    }
    /// Cancel strategy deadlines and release bounded per-session state.
    pub fn cleanup(&mut self) {
        self.finish(false, TurnStopReason::SessionEnd);
        if let Some(strategies) = &mut self.start_strategies {
            strategies.cleanup();
        }
    }
    /// The next useful deadline. The watchdog never terminates audible speech.
    pub fn deadline(&self) -> Option<Duration> {
        [
            self.turn_deadline(),
            self.start_strategies
                .as_ref()
                .and_then(UserTurnStartStrategies::deadline),
        ]
        .into_iter()
        .flatten()
        .min()
    }
    fn turn_deadline(&self) -> Option<Duration> {
        if !self.active || self.speaking {
            return None;
        }
        if let Some(strategies) = &self.stop_strategies {
            return [strategies.deadline(), self.watchdog_deadline]
                .into_iter()
                .flatten()
                .min();
        }
        let policy = if self.model_ready() && (self.has_text || !self.config.wait_for_transcript) {
            self.speech_deadline.map(|speech| {
                // Transcript-optional speech timeouts still honor STT safety.
                // Only the model gate may release immediately without text.
                if self.config.wait_for_transcript || !self.config.require_model_completion {
                    speech.max(self.stt_deadline.unwrap_or(speech))
                } else {
                    speech
                }
            })
        } else {
            None
        };
        [policy, self.watchdog_deadline].into_iter().flatten().min()
    }
    pub fn process(&mut self, data: &FrameData, now: Duration) -> Vec<UserTurnEvent> {
        self.now = self.now.max(now);
        let now = self.now;
        let mut events = Vec::new();
        let custom = self.start_strategies.is_some();
        if matches!(
            data,
            FrameData::VadUserStartedSpeaking(_)
                | FrameData::VadUserStartedSpeakingDetailed(_)
                | FrameData::UserStartedSpeaking
                | FrameData::ProposedUserStartedSpeaking
                | FrameData::Transcription(_)
                | FrameData::TranscriptionDetailed(_)
                | FrameData::InterimTranscription(_)
                | FrameData::InterimTranscriptionDetailed(_)
        ) {
            self.pending_inference_request = None;
        }
        if let Some(strategies) = &mut self.start_strategies {
            let output = strategies.process(data, now);
            events.extend(output.events.into_iter().map(UserTurnEvent::StartStrategy));
            if output.reset_aggregation {
                events.push(UserTurnEvent::ResetAggregation);
            }
            if let Some(params) = output.start {
                self.start_with(params, &mut events);
            }
        }
        match data {
            FrameData::VadUserStartedSpeaking(_) | FrameData::VadUserStartedSpeakingDetailed(_) => {
                self.speaking = true;
                if !custom {
                    self.start(true, &mut events);
                }
                self.vad_observed = true;
                self.clear_pending_stop();
                self.touch(now);
            }
            FrameData::UserStartedSpeaking => {
                self.speaking = true;
                if !custom {
                    self.start(false, &mut events);
                }
                self.clear_pending_stop();
                self.touch(now);
            }
            FrameData::ProposedUserStartedSpeaking if custom || self.config.resolve_proposals => {
                self.speaking = true;
                if !custom {
                    self.start(true, &mut events);
                }
                self.clear_pending_stop();
                self.touch(now);
            }
            FrameData::VadUserStoppedSpeaking(_) | FrameData::VadUserStoppedSpeakingDetailed(_) => {
                let stop_secs = match data {
                    FrameData::VadUserStoppedSpeaking(stop) => stop.duration_secs,
                    FrameData::VadUserStoppedSpeakingDetailed(stop) => stop.stop_secs,
                    _ => unreachable!(),
                };
                if let Some(duration) = valid_duration(stop_secs) {
                    self.config.vad_stop_duration = duration;
                }
                self.vad_observed = true;
                self.speaking = false;
                if self.active {
                    self.vad_stopped_at = Some(now);
                    self.speech_deadline = Some(now.saturating_add(self.config.speech_timeout));
                    let timestamp = match data {
                        FrameData::VadUserStoppedSpeaking(stop) => stop.timestamp,
                        FrameData::VadUserStoppedSpeakingDetailed(stop) => stop.timestamp,
                        _ => unreachable!(),
                    };
                    let delivered_after = if timestamp.is_finite() && timestamp > 0.0 {
                        valid_duration(
                            (SystemTime::now()
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_secs_f64()
                                - timestamp)
                                .max(0.0),
                        )
                        .unwrap_or(Duration::from_secs(86_400))
                    } else {
                        Duration::ZERO
                    };
                    let safety = now.saturating_add(
                        self.config
                            .stt_timeout
                            .saturating_sub(self.config.vad_stop_duration)
                            .saturating_sub(delivered_after),
                    );
                    self.stt_safety_deadline = Some(safety);
                    self.stt_deadline = Some(if self.finalized { now } else { safety });
                    self.touch(now);
                }
            }
            FrameData::UserStoppedSpeaking => {
                self.speaking = false;
                self.external_stop = Some(false);
                self.touch(now);
            }
            FrameData::ProposedUserStoppedSpeaking if self.resolves_proposed_turn_stop_frames() => {
                self.speaking = false;
                self.external_stop = Some(true);
                self.touch(now);
            }
            FrameData::Transcription(_) | FrameData::TranscriptionDetailed(_)
                if transcription(data).is_some_and(|(text, _)| !text.trim().is_empty()) =>
            {
                let (_, finalized) = transcription(data).expect("matched transcription");
                if !custom && self.config.start_on_transcription {
                    self.start(true, &mut events);
                }
                if self.active {
                    self.has_text = true;
                    self.interim_pending = false;
                    self.finalized |= finalized;
                    self.touch(now);
                    if !self.speaking
                        && self.vad_stopped_at.is_none()
                        && self.external_stop.is_none()
                    {
                        // The first no-VAD segment establishes an STT fallback.
                        // A later final releases it; a nonfinal refreshes it.
                        // Observe the old deadline before finalization overwrites it.
                        let fallback_pending = self.stt_deadline.is_some();
                        self.speech_deadline = Some(now.saturating_add(self.config.speech_timeout));
                        self.stt_deadline = Some(
                            if self.config.require_model_completion
                                && !(fallback_pending && finalized)
                            {
                                now.saturating_add(
                                    self.config
                                        .stt_timeout
                                        .saturating_sub(self.config.vad_stop_duration),
                                )
                            } else {
                                now
                            },
                        );
                    } else if finalized {
                        self.stt_deadline = Some(now);
                    }
                }
            }
            FrameData::InterimTranscription(_) | FrameData::InterimTranscriptionDetailed(_)
                if transcription(data).is_some_and(|(text, _)| !text.trim().is_empty()) =>
            {
                if !custom && self.config.start_on_transcription {
                    self.start(true, &mut events);
                }
                if self.active {
                    self.finalized = false;
                    self.interim_pending = true;
                    self.touch(now);
                    if self.vad_stopped_at.is_some() {
                        self.stt_deadline = self.stt_safety_deadline;
                    }
                }
            }
            FrameData::SttMetadata(metadata) => {
                if let Some(duration) = valid_duration(metadata.ttfs_p99_latency) {
                    self.config.stt_timeout = duration;
                }
            }
            _ => {}
        }
        if let Some(strategies) = &mut self.stop_strategies {
            strategies.configure(self.config.stt_timeout, self.config.vad_stop_duration);
            let decisions = strategies.process(data, now, self.speaking, self.stt_safety_deadline);
            events.extend(self.apply_stop_decisions(decisions));
        }
        events.extend(self.advance(now));
        events
    }
    fn apply_stop_decisions(
        &mut self,
        decisions: Vec<crate::stop::StopDecision>,
    ) -> Vec<UserTurnEvent> {
        let mut events = vec![];
        for decision in decisions {
            if !self.active || self.speaking {
                break;
            }
            if decision.deferred {
                if self.pending_inference_request.is_none() {
                    let request_id = NEXT_INFERENCE_REQUEST.fetch_add(1, Ordering::Relaxed);
                    self.pending_inference_request = Some(request_id);
                    self.touch(self.now);
                    events.push(UserTurnEvent::InferenceRequested {
                        request_id,
                        strategy_index: decision.index,
                        reason: decision.reason,
                    });
                }
            } else {
                // An immediate finalizer supersedes requests from earlier
                // detectors in this same batch before any observer sees them.
                events.retain(|event| !matches!(event, UserTurnEvent::InferenceRequested { .. }));
                events.extend(self.finish(decision.emit_speaking, decision.reason));
            }
        }
        events
    }
    pub fn advance(&mut self, now: Duration) -> Vec<UserTurnEvent> {
        self.now = self.now.max(now);
        let mut events: Vec<_> = self
            .start_strategies
            .as_mut()
            .map(|strategies| strategies.advance(self.now))
            .unwrap_or_default()
            .into_iter()
            .map(UserTurnEvent::StartStrategy)
            .collect();
        if let Some(strategies) = &mut self.stop_strategies {
            let decisions = strategies.advance(self.now);
            events.extend(self.apply_stop_decisions(decisions));
        }
        events.extend(self.advance_turn());
        events
    }
    fn advance_turn(&mut self) -> Vec<UserTurnEvent> {
        let now = self.now;
        if !self.active || self.speaking {
            return vec![];
        }
        let text_ready = self.has_text || !self.config.wait_for_transcript;
        if text_ready && self.stop_strategies.is_none() {
            if let Some(emit) = self.external_stop {
                if !self.interim_pending || !self.config.wait_for_transcript {
                    return self.finish(emit, TurnStopReason::External);
                }
            }
            if self.model_ready()
                && self.speech_deadline.is_some_and(|deadline| now >= deadline)
                && ((self.config.require_model_completion && !self.config.wait_for_transcript)
                    || self.stt_deadline.is_some_and(|deadline| now >= deadline))
            {
                return self.finish(
                    true,
                    if self.config.require_model_completion && self.vad_observed {
                        TurnStopReason::ModelCompletion
                    } else {
                        TurnStopReason::SpeechTimeout
                    },
                );
            }
        }
        if self
            .watchdog_deadline
            .is_some_and(|deadline| now >= deadline)
        {
            return self.finish(true, TurnStopReason::Watchdog);
        }
        vec![]
    }
    fn model_ready(&self) -> bool {
        !self.config.require_model_completion || !self.vad_observed || self.model_complete
    }
    /// Apply a current model verdict without extending the STT safety budget.
    pub fn mark_model_complete(&mut self, now: Duration) -> Vec<UserTurnEvent> {
        self.now = self.now.max(now);
        if let Some(strategies) = &mut self.stop_strategies {
            let decisions = strategies.model_complete(self.now);
            let mut events = self.apply_stop_decisions(decisions);
            events.extend(self.advance(now));
            return events;
        }
        if self.active && !self.speaking {
            self.model_complete = true;
            self.speech_deadline = Some(now);
        }
        self.advance(now)
    }
    /// Explicit terminal boundaries may close a speaking turn, unlike a timeout.
    pub fn finish(&mut self, emit_speaking: bool, reason: TurnStopReason) -> Vec<UserTurnEvent> {
        let active = self.active;
        self.pending_inference_request = None;
        if let Some(strategies) = &mut self.stop_strategies {
            strategies.stop();
        }
        self.active = false;
        self.vad_observed = false;
        self.speaking = false;
        self.has_text = false;
        self.interim_pending = false;
        self.clear_pending_stop();
        self.watchdog_deadline = None;
        if active {
            if let Some(strategies) = &mut self.start_strategies {
                strategies.handle_user_turn_stopped();
            }
            vec![UserTurnEvent::Stopped {
                emit_speaking,
                reason,
            }]
        } else {
            vec![]
        }
    }
    fn start(&mut self, emit_speaking: bool, events: &mut Vec<UserTurnEvent>) {
        self.start_with(
            UserTurnStartedParams {
                enable_user_speaking_frames: emit_speaking,
                enable_interruptions: emit_speaking && self.config.enable_interruptions,
            },
            events,
        );
    }
    fn start_with(&mut self, params: UserTurnStartedParams, events: &mut Vec<UserTurnEvent>) {
        if self.active {
            return;
        }
        self.active = true;
        self.has_text = false;
        self.interim_pending = false;
        // VAD observations from a completed acoustic segment do not gate a
        // later transcript-driven turn. Live speech still belongs to the user
        // across the start boundary and must retain its model requirement.
        if !self.speaking {
            self.vad_observed = false;
        }
        self.clear_pending_stop();
        if let Some(strategies) = &mut self.start_strategies {
            strategies.handle_user_turn_started(self.now);
        }
        if let Some(strategies) = &mut self.stop_strategies {
            strategies.start(self.speaking, self.now);
        }
        self.touch(self.now);
        events.push(UserTurnEvent::Started {
            emit_speaking: params.enable_user_speaking_frames,
            // Adopted real speaking frames have already been resolved upstream.
            interrupt: params.enable_interruptions && self.config.enable_interruptions,
        });
    }
    fn clear_pending_stop(&mut self) {
        self.model_complete = false;
        self.finalized = false;
        self.vad_stopped_at = None;
        self.speech_deadline = None;
        self.stt_deadline = None;
        self.stt_safety_deadline = None;
        self.external_stop = None;
    }
    fn touch(&mut self, now: Duration) {
        if self.active {
            self.watchdog_deadline = Some(now.saturating_add(self.config.stop_timeout));
        }
    }
}

fn valid_duration(seconds: f64) -> Option<Duration> {
    (seconds.is_finite() && (0.0..=86_400.0).contains(&seconds))
        .then(|| Duration::from_secs_f64(seconds))
}
fn transcription(data: &FrameData) -> Option<(&str, bool)> {
    match data {
        FrameData::Transcription(text) | FrameData::InterimTranscription(text) => {
            Some((&text.text, text.finalized))
        }
        FrameData::TranscriptionDetailed(text) | FrameData::InterimTranscriptionDetailed(text) => {
            Some((&text.text.text, text.finalized))
        }
        _ => None,
    }
}
