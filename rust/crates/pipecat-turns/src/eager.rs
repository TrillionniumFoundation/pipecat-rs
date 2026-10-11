//! Provisional user-turn inference and transcript matching.
use crate::{
    AggregationAction, AggregationOutput, SharedContext, TurnStopReason, UserTurnConfig,
    UserTurnStartStrategies, UserTurnStartStrategy, UserTurnStopStrategies, UserTurnStopStrategy,
};
use caseless::Caseless;
use pipecat_core::{
    ChatRole, DetailedLlmContextFrame, DirectedFrame, Direction, Frame, FrameData, LlmContextData,
    NativeEagerCommit, NativeEagerConsumer, NativeEagerPrediction, NativeEagerTurn,
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
    source: Option<NativeEagerPrediction>,
}
struct SourceFinal {
    receipt: NativeEagerCommit,
    stop_seen: bool,
    ordinary_offset: usize,
    prefix_spaces: bool,
    // Separator inserted by the legacy buffer before the first later segment,
    // and that segment's spacing mode. The rest of the legacy text is unchanged.
    suffix: Option<(usize, bool)>,
}
/// A retired source final must not be confused with an ordinary mismatch:
/// neither its text nor empty-turn recovery is authorized in that case.
pub(crate) enum BoundFinish {
    Confirmed {
        capability: NativeSpeculation,
        text: String,
    },
    CurrentFinalFallback {
        text: String,
    },
    RetiredScopedDiscard,
    NoBoundDecision,
}
pub(crate) struct EagerState {
    config: EagerUserTurnConfig,
    pending: Option<Prediction>,
    settled: Option<SpeculationOwner>,
    // ExternalUserTurnStopStrategy concatenates source transcript segments
    // verbatim. The history aggregator independently formats committed text.
    transcript: Option<String>,
    consumer: NativeEagerConsumer,
    // One cumulative final is retained separately from legacy text. A receipt
    // clone shares its immutable, core-bounded payload; the transcript ring
    // receives only the already-consumed public frame.
    source_final: Option<SourceFinal>,
    early_stop: Option<(NativeEagerCommit, DirectedFrame)>,
    foreign_transcript: bool,
    ordinary_last_spaces: bool,
    ordinary_ends_space: bool,
}
impl EagerState {
    pub fn new(config: EagerUserTurnConfig) -> Self {
        Self {
            config,
            pending: None,
            settled: None,
            transcript: Some(String::new()),
            consumer: NativeEagerConsumer::new(),
            source_final: None,
            early_stop: None,
            foreign_transcript: false,
            ordinary_last_spaces: false,
            ordinary_ends_space: false,
        }
    }
    pub fn reset_transcript(&mut self) {
        self.transcript = Some(String::new());
        self.foreign_transcript = false;
        self.ordinary_last_spaces = false;
        self.ordinary_ends_space = false;
    }
    pub fn reset_turn(&mut self) {
        self.reset_transcript();
        self.source_final = None;
        self.early_stop = None;
    }
    pub fn claim_source_start(&self, turn: &NativeEagerTurn) -> bool {
        turn.claim_start(&self.consumer)
    }
    /// Drop only scoped provisional bookkeeping while retaining unrelated
    /// ordinary text and its spacing/matching metadata for the next owner.
    pub fn abandon_source_turn(
        &mut self,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        self.cancel(output)?;
        self.source_final = None;
        self.early_stop = None;
        Ok(())
    }
    pub fn has_source_final(&self) -> bool {
        self.source_final.is_some()
    }
    pub fn claim_empty_stop(&self, turn: &NativeEagerTurn) -> bool {
        turn.claim_stop(&self.consumer, None)
    }
    pub fn record_transcript(
        &mut self,
        text: &str,
        append: bool,
        spaces: bool,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        self.foreign_transcript = true;
        if append && !text.trim().is_empty() {
            if let Some(staged) = &mut self.source_final {
                if staged.suffix.is_none() {
                    let separator = staged.ordinary_offset != 0
                        && ((!spaces && !self.ordinary_last_spaces)
                            || (spaces != self.ordinary_last_spaces
                                && !self.ordinary_ends_space
                                && !text.starts_with(char::is_whitespace)));
                    staged.suffix = Some((usize::from(separator), spaces));
                }
            }
            self.ordinary_last_spaces = spaces;
            self.ordinary_ends_space = text.ends_with(char::is_whitespace);
        }
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
    /// Reserve once-only adoption before generic input side effects. The
    /// locally held RAII owner cancels on Drop if installation cannot complete.
    pub fn claim_source_prediction(
        &self,
        source: &NativeEagerPrediction,
    ) -> Result<Option<SpeculationOwner>, ProcessorError> {
        let text = source.text();
        if text.trim().is_empty() || text.len() > self.config.max_transcript_bytes {
            return Err(ProcessorError::new(
                "source eager transcript must be nonempty and within its byte limit",
            ));
        }
        if self.source_final.is_some() {
            return Ok(None);
        }
        Ok(SpeculationOwner::try_adopt_source_prediction(
            source,
            &self.consumer,
        ))
    }
    pub fn begin_source(
        &mut self,
        source: &NativeEagerPrediction,
        owner: SpeculationOwner,
        context: &SharedContext,
        now: Duration,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let text = source.text();
        let baseline = context.snapshot_rich()?;
        let mut provisional = baseline.clone();
        provisional
            .messages
            .push(crate::context::text_message(ChatRole::User, text));
        let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: provisional,
            speculation: true,
        }));
        frame.set_native_speculation(owner.capability())?;
        self.cancel(output)?;
        self.pending = Some(Prediction {
            owner,
            text: text.to_owned(),
            baseline,
            deadline: now.saturating_add(self.config.timeout),
            source: Some(source.clone()),
        });
        output.push(frame, Direction::Downstream);
        Ok(())
    }
    pub fn cancel_source(
        &mut self,
        source: &NativeEagerPrediction,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        // A passive token is not destructive source authority. Resume/revoke
        // must already have retired it before this cleanup carrier arrives.
        if source.is_live() {
            return Ok(());
        }
        if self
            .pending
            .as_ref()
            .is_some_and(|prediction| prediction.owner.capability().is_source_prediction(source))
        {
            // A transferred/settled owner is deliberately outside STT authority.
            if let Some(prediction) = self.pending.take() {
                prediction.owner.cancel();
                let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
                frame.set_native_speculation(prediction.owner.capability())?;
                output.actions.push(AggregationAction::Broadcast(frame));
            }
        }
        Ok(())
    }
    pub fn stage_source_final(
        &mut self,
        receipt: &NativeEagerCommit,
        ordinary_offset: usize,
    ) -> Result<bool, ProcessorError> {
        if let Some(staged) = &self.source_final {
            if staged.receipt == *receipt {
                return Ok(false);
            }
            return Err(ProcessorError::new(
                "only one cumulative source final may await a stop decision",
            ));
        }
        if receipt.text().len() > self.config.max_transcript_bytes {
            return Err(ProcessorError::new(
                "source final exceeds the eager transcript byte limit",
            ));
        }
        // Exact current-turn ownership was checked by the user aggregator;
        // core also requires that turn's once-only Start for this consumer.
        if !receipt.claim_final(&self.consumer) {
            return Ok(false);
        }
        if self
            .early_stop
            .as_ref()
            .is_some_and(|(stop, _)| stop != receipt)
        {
            self.early_stop = None;
        }
        self.source_final = Some(SourceFinal {
            receipt: receipt.clone(),
            stop_seen: false,
            ordinary_offset,
            prefix_spaces: self.ordinary_last_spaces,
            suffix: None,
        });
        Ok(true)
    }
    /// A system stop may overtake its normal final. Keep that exact stop until
    /// the final is claimed; never let a foreign stop close a staged turn.
    pub fn source_stop(
        &mut self,
        receipt: &NativeEagerCommit,
        frame: Frame,
        direction: Direction,
    ) -> bool {
        if let Some(staged) = &mut self.source_final {
            if staged.receipt != *receipt || staged.stop_seen {
                return false;
            }
            if !receipt.claim_stop(&self.consumer) {
                return false;
            }
            staged.stop_seen = true;
            return true;
        }
        if self.early_stop.is_none() && receipt.claim_stop(&self.consumer) {
            self.early_stop = Some((receipt.clone(), DirectedFrame::new(frame, direction)));
        }
        false
    }
    pub fn take_ready_source_stop(&mut self) -> Option<DirectedFrame> {
        let staged = self.source_final.as_mut()?;
        let (receipt, _) = self.early_stop.as_ref()?;
        if staged.receipt != *receipt {
            return None;
        }
        staged.stop_seen = true;
        self.early_stop.take().map(|(_, frame)| frame)
    }
    pub fn finish_source(
        &mut self,
        ordinary_text: &str,
        reason: TurnStopReason,
        now: Duration,
        context: &SharedContext,
        output: &mut AggregationOutput,
    ) -> Result<BoundFinish, ProcessorError> {
        self.early_stop = None;
        let Some(staged) = self.source_final.take() else {
            // With no scoped final, preserve legacy empty-turn recovery. An
            // empty provider End may withdraw its prediction without a final.
            return Ok(BoundFinish::NoBoundDecision);
        };
        let (append, spaces) = match staged.receipt.payload() {
            FrameData::Transcription(_) => (true, false),
            FrameData::TranscriptionDetailed(final_text) => (
                final_text.text.append_to_context,
                final_text.text.includes_inter_frame_spaces,
            ),
            _ => return Err(ProcessorError::fatal("invalid native source final payload")),
        };
        // Merge only this bounded insertion, preserving the existing generic
        // buffer byte-for-byte if admission is retired. Do not keep a rollback
        // log or concatenate foreign text into the receipt's matching input.
        let text = if append && !staged.receipt.text().trim().is_empty() {
            let prefix = ordinary_text
                .get(..staged.ordinary_offset)
                .ok_or_else(|| ProcessorError::fatal("source final insertion boundary changed"))?;
            let (separator, suffix_spaces) = staged.suffix.unwrap_or((0, false));
            let suffix = ordinary_text
                .get(staged.ordinary_offset.saturating_add(separator)..)
                .ok_or_else(|| ProcessorError::fatal("source final suffix boundary changed"))?;
            let mut merged = crate::TextBuffer::default();
            merged.append(prefix, staged.prefix_spaces);
            merged.append(staged.receipt.text(), spaces);
            merged.append(suffix, suffix_spaces);
            merged.take()
        } else {
            ordinary_text.trim().to_owned()
        };
        let matches = staged.stop_seen
            && matches!(reason, TurnStopReason::External)
            && !self.foreign_transcript
            && ordinary_text.trim().is_empty()
            && self.pending.as_ref().is_some_and(|prediction| {
                now < prediction.deadline
                    && prediction
                        .source
                        .as_ref()
                        .is_some_and(|source| staged.receipt.matches_prediction(source))
                    && self
                        .config
                        .policy
                        .matches(&prediction.text, staged.receipt.text())
                    && context
                        .snapshot_rich()
                        .is_ok_and(|current| current == prediction.baseline)
            });
        if matches {
            let prediction = self.pending.take().expect("matched source prediction");
            let mut committed = prediction.baseline.clone();
            if !text.trim().is_empty() {
                committed
                    .messages
                    .push(crate::context::text_message(ChatRole::User, text.trim()));
            }
            if prediction.owner.confirm_source_with_context(
                &staged.receipt,
                &self.consumer,
                committed,
            ) {
                let capability = prediction.owner.capability();
                self.settled = Some(prediction.owner);
                self.reset_transcript();
                return Ok(BoundFinish::Confirmed { capability, text });
            }
            let mut frame = Frame::new(FrameData::EagerEndOfTurnCancel);
            frame.set_native_speculation(prediction.owner.capability())?;
            output.actions.push(AggregationAction::Broadcast(frame));
        }
        self.cancel(output)?;
        // This is the once-only ordinary admission point, serialized against
        // source retirement in core. A later revoke cannot undo this commit.
        if staged.receipt.consume_fallback(&self.consumer) {
            self.reset_transcript();
            Ok(BoundFinish::CurrentFinalFallback { text })
        } else {
            Ok(BoundFinish::RetiredScopedDiscard)
        }
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
            source: None,
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
            prediction.source.is_none()
                && now < prediction.deadline
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
