//! Loss-aware conversation aggregation, deterministic turns, and native Smart Turn.
//!
//! VAD decisions and STT frames drive the controller; optional Smart Turn uses
//! the native ONNX analyzer. Playback acknowledgements remain transport-owned.
mod assistant;
mod completion;
mod context;
mod controller;
mod eager;
mod empty;
mod tool_history;
pub use eager::{EagerEndOfTurn, EagerMatchPolicy, EagerUserTurnConfig};
mod idle;
mod realtime;
pub use realtime::{RealtimeServiceMode, RealtimeUserContext};
mod signed_history;
mod smart_turn;
mod stop;
mod strategies;
mod summary;
mod user;

pub use assistant::{AssistantAggregatorConfig, AssistantTextMode, LLMAssistantAggregator};
pub use completion::{
    LLMTurnCompletionConfig, LLMTurnCompletionProcessor, LLMTurnCompletionStrategy,
    USER_TURN_COMPLETE_MARKER, USER_TURN_INCOMPLETE_LONG_MARKER, USER_TURN_INCOMPLETE_SHORT_MARKER,
};
pub use context::{context_to_frame, SharedContext};
pub use controller::{TurnStopReason, UserTurnConfig, UserTurnController, UserTurnEvent};
pub use empty::{EmptyUserTurnConfig, DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT};
pub use idle::UserIdleController;
pub use smart_turn::SmartTurnUserAggregator;
pub use stop::{UserTurnStopStrategies, UserTurnStopStrategy};
pub use strategies::{
    UserMuteStrategies, UserMuteStrategy, UserStartStrategyEvent, UserStartStrategyOutput,
    UserTurnStartStrategies, UserTurnStartStrategy, UserTurnStartedParams,
};
pub use summary::{
    estimate_context_tokens, get_messages_to_summarize, ContextSummarizationConfig,
    LLMContextSummarizer, LLMContextSummaryProcessor, SummaryAppliedEvent, SummaryLlm,
    DEFAULT_SUMMARIZATION_PROMPT,
};
pub use user::{LLMUserAggregator, UserAggregatorConfig};

use pipecat_core::{DirectedFrame, Direction, Frame, FrameData, ProcessorContext, ProcessorError};
use pipecat_services::Context;
use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

/// Accepted final text for an external, out-of-band turn-completion decision.
/// This is not a speculative conversational LLM request.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UserTurnInferenceRequest {
    pub request_id: u64,
    pub strategy_index: usize,
    pub text: String,
    pub reason: TurnStopReason,
}

impl UserTurnInferenceRequest {
    /// Send this through the same pipeline input that carries speech frames.
    /// The aggregator rejects it after new evidence or a terminal boundary.
    pub fn completion_frame(&self) -> Frame {
        let mut frame = Frame::new(FrameData::UserTurnInferenceCompleted);
        frame
            .metadata
            .insert("user_turn_inference_id".into(), self.request_id.into());
        frame
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AggregationEvent {
    UserStartStrategy(UserStartStrategyEvent),
    UserMuteChanged {
        muted: bool,
    },
    UserTurnStarted,
    /// External classification is requested; no conversational inference has run.
    UserTurnInferenceRequested(UserTurnInferenceRequest),
    UserTurnIdle,
    UserTurnStopped {
        text: String,
        reason: TurnStopReason,
    },
    /// The realtime turn stopped; its transcript is still awaiting handoff.
    RealtimeUserTurnStopped {
        reason: TurnStopReason,
    },
    /// Final user text was written by a realtime handoff or terminal flush.
    UserTurnMessageAdded {
        text: String,
    },
    AssistantTurnStarted,
    SummaryApplied(SummaryAppliedEvent),
    AssistantTurnStopped {
        text: String,
        interrupted: bool,
    },
    ToolResultIgnored {
        tool_call_id: String,
    },
}
#[derive(Debug, Clone, PartialEq)]
pub enum AggregationAction {
    Push(DirectedFrame),
    Broadcast(Frame),
    Interrupt,
    /// Cancel the previous response without discarding the context admitted in this batch.
    InterruptBeforeContext,
    /// Run a result callback in a tracked task, after its context update.
    ContextUpdated(pipecat_core::LocalAsyncCallback),
    RealtimeUserContext(RealtimeUserContext),
}
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AggregationOutput {
    pub actions: Vec<AggregationAction>,
    pub events: Vec<AggregationEvent>,
}
impl AggregationOutput {
    fn push(&mut self, frame: Frame, direction: Direction) {
        self.actions
            .push(AggregationAction::Push(DirectedFrame::new(
                frame, direction,
            )));
    }
    fn broadcast(&mut self, data: FrameData) {
        self.actions
            .push(AggregationAction::Broadcast(Frame::new(data)));
    }
    fn context(
        &mut self,
        context: &SharedContext,
        direction: Direction,
    ) -> Result<(), ProcessorError> {
        self.context_frame(context.frame_data()?, direction);
        Ok(())
    }
    fn context_frame(&mut self, context: FrameData, direction: Direction) {
        if direction == Direction::Downstream {
            for action in &mut self.actions {
                let boundary = match action {
                    AggregationAction::Broadcast(frame) => Some(frame),
                    AggregationAction::Push(frame) => Some(&mut frame.frame),
                    AggregationAction::Interrupt => {
                        *action = AggregationAction::InterruptBeforeContext;
                        None
                    }
                    AggregationAction::InterruptBeforeContext
                    | AggregationAction::ContextUpdated(_)
                    | AggregationAction::RealtimeUserContext(_) => None,
                };
                if let Some(frame) = boundary {
                    if matches!(
                        frame.data,
                        FrameData::UserStartedSpeaking | FrameData::UserStoppedSpeaking
                    ) {
                        frame
                            .metadata
                            .insert("user_context_follows".into(), true.into());
                    }
                }
            }
        }
        self.push(Frame::new(context), direction);
    }
    pub async fn emit(self, ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        for action in self.actions {
            match action {
                AggregationAction::Push(frame) => {
                    let native_context = frame.frame.native_realtime_context_cause()?.is_some();
                    let emitted = if native_context {
                        ctx.try_push_realtime_context(frame.frame, frame.direction)
                    } else {
                        // Keep ordinary cascade routing byte/behavior compatible.
                        ctx.push(frame.frame, frame.direction).await
                    };
                    emitted.map_err(|mut error| {
                        if native_context {
                            error.fatal = true;
                            error.message = format!("realtime context could not be emitted; retained history requires a rebuilt pair and provider session: {}", error.message);
                        }
                        error
                    })?;
                }
                AggregationAction::Broadcast(frame) => ctx.broadcast(frame).await?,
                AggregationAction::Interrupt => ctx.broadcast_interruption().await?,
                AggregationAction::ContextUpdated(callback) => {
                    ctx.spawn("tool-context-updated", false, async move {
                        callback.call().await
                    })?;
                }
                AggregationAction::RealtimeUserContext(context) => context.emit(ctx)?,
                AggregationAction::InterruptBeforeContext => {
                    ctx.broadcast_interruption_with_metadata(
                        [("user_context_follows".into(), true.into())].into(),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
}

/// Both halves share the same provider-neutral context. Place the user half
/// before the LLM and the assistant half after text delivery/audio output.
pub struct LLMContextAggregatorPair {
    context: SharedContext,
    user: Arc<LLMUserAggregator>,
    assistant: Arc<LLMAssistantAggregator>,
}
impl LLMContextAggregatorPair {
    pub fn new(
        context: Context,
        user: UserAggregatorConfig,
        assistant: AssistantAggregatorConfig,
    ) -> Self {
        Self::with_shared_context(SharedContext::new(context), user, assistant)
    }
    pub fn new_rich(
        context: pipecat_core::LlmContextData,
        user: UserAggregatorConfig,
        assistant: AssistantAggregatorConfig,
    ) -> Self {
        Self::with_shared_context(SharedContext::new_rich(context), user, assistant)
    }
    pub fn with_shared_context(
        context: SharedContext,
        user: UserAggregatorConfig,
        assistant: AssistantAggregatorConfig,
    ) -> Self {
        Self::with_shared_context_and_realtime_mode(
            context,
            user,
            assistant,
            RealtimeServiceMode::Auto,
        )
        .expect("auto mode is valid until realtime metadata is received")
    }
    /// Configure both halves together. Explicit realtime mode rejects eager
    /// speculation; Auto reports that incompatibility if metadata enables it.
    pub fn with_realtime_mode(
        context: Context,
        user: UserAggregatorConfig,
        assistant: AssistantAggregatorConfig,
        mode: RealtimeServiceMode,
    ) -> Result<Self, ProcessorError> {
        Self::with_shared_context_and_realtime_mode(
            SharedContext::new(context),
            user,
            assistant,
            mode,
        )
    }
    pub fn with_shared_context_and_realtime_mode(
        context: SharedContext,
        user: UserAggregatorConfig,
        assistant: AssistantAggregatorConfig,
        mode: RealtimeServiceMode,
    ) -> Result<Self, ProcessorError> {
        if mode == RealtimeServiceMode::Enabled && user.eager.is_some() {
            return Err(ProcessorError::new(
                "realtime aggregation and eager speculation cannot share a pair",
            ));
        }
        let realtime = Arc::new(realtime::RealtimeState::new(
            mode,
            true,
            user.eager.is_some(),
        ));
        let assistant = Arc::new(
            LLMAssistantAggregator::new(context.clone(), assistant).with_realtime(realtime.clone()),
        );
        let user = Arc::new(
            LLMUserAggregator::new_paired(context.clone(), user, realtime)
                .with_assistant(Arc::downgrade(&assistant)),
        );
        Ok(Self {
            user,
            assistant,
            context,
        })
    }
    pub fn context(&self) -> SharedContext {
        self.context.clone()
    }
    pub fn user(&self) -> Arc<LLMUserAggregator> {
        self.user.clone()
    }
    pub fn assistant(&self) -> Arc<LLMAssistantAggregator> {
        self.assistant.clone()
    }
}

#[derive(Debug, Clone, Default)]
struct TextBuffer {
    text: String,
    last_includes_spaces: bool,
}
impl TextBuffer {
    /// Mirrors concatenate_aggregated_text, including transitions between
    /// token streams containing whitespace and independently stripped parts.
    fn append(&mut self, text: &str, includes_spaces: bool) {
        if text.is_empty() {
            return;
        }
        if !self.text.is_empty()
            && ((!includes_spaces && !self.last_includes_spaces)
                || (includes_spaces != self.last_includes_spaces
                    && !self.text.ends_with(char::is_whitespace)
                    && !text.starts_with(char::is_whitespace)))
        {
            self.text.push(' ');
        }
        self.text.push_str(text);
        self.last_includes_spaces = includes_spaces;
    }
    fn take(&mut self) -> String {
        self.last_includes_spaces = false;
        std::mem::take(&mut self.text).trim().to_owned()
    }
}

/// Replay protection is bounded. Distinct frame identities are never deduped
/// merely because a user said the same words on two different turns.
#[derive(Debug, Default)]
struct RecentFrames {
    order: VecDeque<u64>,
    set: HashSet<u64>,
}
impl RecentFrames {
    fn insert(&mut self, id: u64) -> bool {
        if !self.set.insert(id) {
            return false;
        }
        self.order.push_back(id);
        if self.order.len() > 512 {
            if let Some(old) = self.order.pop_front() {
                self.set.remove(&old);
            }
        }
        true
    }
}
