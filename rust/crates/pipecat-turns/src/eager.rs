//! Provisional user-turn inference and transcript matching.
use crate::{
    AggregationAction, AggregationOutput, SharedContext, TurnStopReason, UserTurnConfig,
    UserTurnStartStrategies, UserTurnStartStrategy, UserTurnStopStrategies, UserTurnStopStrategy,
};
use caseless::Caseless;
use pipecat_core::{
    ChatRole, DetailedLlmContextFrame, Direction, Frame, FrameData, LlmContextData,
    NativeSpeculation, ProcessorError, SpeculationOwner,
};
use std::time::Duration;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum EagerMatchPolicy {
    Exact,
    #[default]
    Normalized,
}
impl EagerMatchPolicy {
    pub fn matches(self, eager: &str, final_text: &str) -> bool {
        match self {
            Self::Exact => eager == final_text,
            Self::Normalized => normalize(eager) == normalize(final_text),
        }
    }
}
fn normalize(text: &str) -> String {
    static WORD: std::sync::OnceLock<regex::Regex> = std::sync::OnceLock::new();
    let word =
        WORD.get_or_init(|| regex::Regex::new(r"^[\p{L}\p{N}_]$").expect("Unicode word category"));
    let mut result = String::new();
    let mut space = false;
    for c in text.nfkc().default_case_fold() {
        if matches!(c, '\'' | '\u{2019}' | '\u{02bc}') {
            continue;
        }
        // Python re's Unicode \w is alphanumeric plus underscore, not marks.
        let mut utf8 = [0; 4];
        if word.is_match(c.encode_utf8(&mut utf8)) {
            if space && !result.is_empty() {
                result.push(' ');
            }
            result.push(c);
            space = false;
        } else {
            space = true;
        }
    }
    result
}
#[derive(Debug, Clone)]
pub struct EagerUserTurnConfig {
    policy: EagerMatchPolicy,
    timeout: Duration,
    max_transcript_bytes: usize,
}
impl Default for EagerUserTurnConfig {
    fn default() -> Self {
        Self {
            policy: EagerMatchPolicy::Normalized,
            timeout: Duration::from_secs(5),
            max_transcript_bytes: 64 * 1024,
        }
    }
}
impl EagerUserTurnConfig {
    pub fn new(
        policy: EagerMatchPolicy,
        timeout: Duration,
        max_transcript_bytes: usize,
    ) -> Result<Self, ProcessorError> {
        if timeout.is_zero()
            || timeout > Duration::from_secs(3600)
            || max_transcript_bytes == 0
            || max_transcript_bytes > 1024 * 1024
        {
            return Err(ProcessorError::new("eager turn limits require a positive timeout up to one hour and 1..1048576 transcript bytes"));
        }
        Ok(Self {
            policy,
            timeout,
            max_transcript_bytes,
        })
    }
    pub fn match_policy(&self) -> EagerMatchPolicy {
        self.policy
    }
    pub fn speculation_timeout(&self) -> Duration {
        self.timeout
    }
    pub(crate) fn configure_turn(&self, config: &mut UserTurnConfig) {
        config.require_model_completion = false;
        config.start_on_transcription = false;
        config.start_strategies = Some(
            UserTurnStartStrategies::new(vec![
                UserTurnStartStrategy::external().with_interruptions(config.enable_interruptions)
            ])
            .expect("one strategy"),
        );
        config.stop_strategies = Some(
            UserTurnStopStrategies::new(vec![UserTurnStopStrategy::external(
                Duration::from_millis(500),
                true,
            )])
            .expect("one strategy"),
        );
    }
}
struct Prediction {
    owner: SpeculationOwner,
    text: String,
    baseline: LlmContextData,
    deadline: Duration,
}
pub(crate) struct EagerState {
    config: EagerUserTurnConfig,
    pending: Option<Prediction>,
    settled: Option<SpeculationOwner>,
    // ExternalUserTurnStopStrategy concatenates source transcript segments
    // verbatim. The history aggregator independently formats committed text.
    transcript: Option<String>,
}
impl EagerState {
    pub fn new(config: EagerUserTurnConfig) -> Self {
        Self {
            config,
            pending: None,
            settled: None,
            transcript: Some(String::new()),
        }
    }
    pub fn reset_transcript(&mut self) {
        self.transcript = Some(String::new());
    }
    pub fn record_transcript(
        &mut self,
        text: &str,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        if let Some(transcript) = &mut self.transcript {
            if transcript.len().saturating_add(text.len()) <= self.config.max_transcript_bytes {
                transcript.push_str(text);
            } else {
                self.transcript = None;
                self.cancel(output)?;
            }
        }
        Ok(())
    }
    pub fn deadline(&self) -> Option<Duration> {
        self.pending.as_ref().map(|prediction| prediction.deadline)
    }
    pub fn cancel(&mut self, output: &mut AggregationOutput) -> Result<(), ProcessorError> {
        if let Some(prediction) = self.pending.take() {
            prediction.owner.cancel();
            let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
            frame.set_native_speculation(prediction.owner.capability())?;
            output.actions.push(AggregationAction::Broadcast(frame));
        }
        if let Some(owner) = self.settled.take() {
            owner.cancel();
        }
        Ok(())
    }
    pub fn cancel_matching(
        &mut self,
        capability: Option<&NativeSpeculation>,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let matches = capability.is_none_or(|capability| {
            self.pending
                .as_ref()
                .is_some_and(|p| p.owner.capability() == *capability)
                || self
                    .settled
                    .as_ref()
                    .is_some_and(|p| p.capability() == *capability)
        });
        if matches {
            self.cancel(output)?;
        }
        Ok(())
    }
    pub fn advance(
        &mut self,
        now: Duration,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        if self.deadline().is_some_and(|deadline| now >= deadline) {
            self.cancel(output)?;
        }
        Ok(())
    }
    pub fn begin(
        &mut self,
        eager_text: &str,
        context: &SharedContext,
        now: Duration,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        self.cancel(output)?;
        let Some(transcript) = &self.transcript else {
            return Err(ProcessorError::new(
                "eager transcript exceeds its byte limit",
            ));
        };
        if transcript.len().saturating_add(eager_text.len()) > self.config.max_transcript_bytes {
            return Err(ProcessorError::new(
                "eager transcript exceeds its byte limit",
            ));
        }
        let text = format!("{transcript}{eager_text}");
        if text.trim().is_empty() {
            return Err(ProcessorError::new(
                "eager transcript must be nonempty and within its byte limit",
            ));
        }
        let baseline = context.snapshot_rich()?;
        let owner = SpeculationOwner::new();
        let mut provisional = baseline.clone();
        provisional
            .messages
            .push(crate::context::text_message(ChatRole::User, text.clone()));
        let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: provisional,
            speculation: true,
        }));
        frame.set_native_speculation(owner.capability())?;
        self.pending = Some(Prediction {
            owner,
            text,
            baseline,
            deadline: now.saturating_add(self.config.timeout),
        });
        output.push(frame, Direction::Downstream);
        Ok(())
    }
    pub fn finish(
        &mut self,
        committed_text: &str,
        reason: TurnStopReason,
        now: Duration,
        context: &SharedContext,
        output: &mut AggregationOutput,
    ) -> Result<Option<NativeSpeculation>, ProcessorError> {
        let transcript = self.transcript.take();
        self.reset_transcript();
        let matches = self.pending.as_ref().is_some_and(|prediction| {
            now < prediction.deadline
                && matches!(reason, TurnStopReason::External)
                && transcript
                    .as_ref()
                    .is_some_and(|text| self.config.policy.matches(&prediction.text, text))
        });
        let matches = matches
            && self.pending.as_ref().is_some_and(|p| {
                context
                    .snapshot_rich()
                    .is_ok_and(|context| context == p.baseline)
            });
        if !matches {
            self.cancel(output)?;
            return Ok(None);
        }
        let prediction = self.pending.take().expect("matched prediction");
        let mut committed = prediction.baseline.clone();
        if !committed_text.trim().is_empty() {
            committed.messages.push(crate::context::text_message(
                ChatRole::User,
                committed_text.trim(),
            ));
        }
        if !prediction.owner.confirm_with_context(committed) {
            let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
            frame.set_native_speculation(prediction.owner.capability())?;
            output.actions.push(AggregationAction::Broadcast(frame));
            return Ok(None);
        }
        let capability = prediction.owner.capability();
        self.settled = Some(prediction.owner);
        Ok(Some(capability))
    }
}
/// Native STT-side lifecycle helper. Providers invoke it when their protocol
/// predicts, withdraws, or commits a turn; disabled instances remain inert.
#[derive(Debug, Default)]
pub struct EagerEndOfTurn {
    enabled: bool,
    pending: bool,
    match_policy: EagerMatchPolicy,
}
impl EagerEndOfTurn {
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            pending: false,
            match_policy: EagerMatchPolicy::default(),
        }
    }
    pub fn with_match_policy(mut self, policy: EagerMatchPolicy) -> Self {
        self.match_policy = policy;
        self
    }
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
    pub fn is_pending(&self) -> bool {
        self.pending
    }
    pub fn predict(
        &mut self,
        transcript: pipecat_core::DetailedTranscriptionFrame,
    ) -> Option<Frame> {
        if !self.enabled {
            return None;
        }
        self.pending = true;
        Some(Frame::new(FrameData::EagerTranscriptionDetailed(
            transcript,
        )))
    }
    pub fn cancel(&mut self) -> Option<Frame> {
        if !std::mem::take(&mut self.pending) {
            return None;
        }
        Some(Frame::new(FrameData::EagerEndOfTurnCancel))
    }
    pub fn clear(&mut self) {
        self.pending = false;
    }
    pub fn recommended_eager_config(&self) -> Option<EagerUserTurnConfig> {
        self.enabled.then(|| EagerUserTurnConfig {
            policy: self.match_policy,
            ..Default::default()
        })
    }
}
