use crate::{
    context::{text_message, tool_message},
    AggregationEvent, AggregationOutput, RecentFrames, SharedContext, TextBuffer,
};
use pipecat_core::{
    async_trait, ChatRole, ContextMessage, Direction, Frame, FrameData, FrameProcessor,
    FunctionCallInProgressFrame, FunctionCallResultFrame, MessageContent, ProcessorContext,
    ProcessorError,
};
use pipecat_services::{Message, Role, ToolCall};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex, MutexGuard};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum AssistantTextMode {
    /// Text received at this processor has been delivered to the user.
    #[default]
    Text,
    /// Only TtsText bearing metadata["spoken"] = true is committed. A normal
    /// Flush with origin="assistant_turn_end" is the playback-completion fence.
    Spoken,
}
#[derive(Debug, Clone, Default)]
pub struct AssistantAggregatorConfig {
    pub text_mode: AssistantTextMode,
}
struct AssistantState {
    active: bool,
    llm_active: bool,
    generated_speech: bool,
    user_speaking: bool,
    awaiting_user_inference: bool,
    bot_speaking: bool,
    aggregation: TextBuffer,
    transcript_markers: crate::completion::transcript::AssistantTranscriptMarkers,
    pending: crate::tool_history::PendingTools,
    completed: crate::tool_history::ToolTombstones,
    interrupted_declarations: crate::tool_history::ToolTombstones,
    native_tools: crate::tool_history::NativeTools,
    declared_tools: crate::tool_history::DeclaredTools,
    signed_declarations: crate::tool_history::DeclaredTools,
    provisional_tools: crate::tool_history::ProvisionalTools,
    seen: RecentFrames,
    context_ids: Vec<String>,
    pending_playback: BTreeSet<String>,
    cancelled_contexts: VecDeque<String>,
    run_owed: bool,
    tool_run_owed: bool,
    pending_native_scheduling: Option<pipecat_core::RealtimeNativeToolSchedulingReason>,
    cancelled_run_groups: BTreeSet<String>,
    skip_tts: Option<bool>,
    thought: TextBuffer,
    thought_llm: Option<String>,
    thought_append: bool,
    signed_history: Option<crate::signed_history::SignedHistory>,
    completed_responses: VecDeque<u64>,
    ended: bool,
    callbacks_closed: bool,
    context_cause_failed: bool,
}
impl AssistantState {
    fn is_completed(&self, id: &str) -> bool {
        self.completed.contains(id)
            || self
                .native_tools
                .calls
                .get(id)
                .is_some_and(|call| call.terminal.is_some())
    }
    fn clear_runs(&mut self) {
        self.run_owed = false;
        self.tool_run_owed = false;
        self.pending_native_scheduling = None;
        self.cancelled_run_groups.clear();
    }
}
/// Commits delivered assistant text and maintains ordered tool-call/results.
/// This processor records calls; it never executes a returned tool.
pub struct LLMAssistantAggregator {
    context: SharedContext,
    config: AssistantAggregatorConfig,
    summarizer: crate::LLMContextSummarizer,
    state: Mutex<AssistantState>,
    callback_tasks: Arc<Mutex<BTreeMap<u64, ProcessorContext>>>,
    realtime: Arc<crate::realtime::RealtimeState>,
    context_causes: pipecat_core::NativeRealtimeContextProducer,
}
impl LLMAssistantAggregator {
    pub fn new(context: SharedContext, config: AssistantAggregatorConfig) -> Self {
        Self {
            summarizer: crate::LLMContextSummarizer::new(context.clone(), Default::default())
                .expect("valid default summary configuration"),
            context,
            config,
            realtime: Arc::new(crate::realtime::RealtimeState::new(
                crate::RealtimeServiceMode::Auto,
                false,
                false,
            )),
            callback_tasks: Arc::new(Mutex::new(BTreeMap::new())),
            context_causes: Default::default(),
            state: Mutex::new(AssistantState {
                active: false,
                llm_active: false,
                generated_speech: false,
                user_speaking: false,
                awaiting_user_inference: false,
                bot_speaking: false,
                aggregation: TextBuffer::default(),
                transcript_markers: Default::default(),
                pending: Default::default(),
                completed: Default::default(),
                interrupted_declarations: Default::default(),
                native_tools: Default::default(),
                declared_tools: Default::default(),
                signed_declarations: Default::default(),
                provisional_tools: Default::default(),
                seen: RecentFrames::default(),
                context_ids: vec![],
                pending_playback: BTreeSet::new(),
                cancelled_contexts: VecDeque::new(),
                run_owed: false,
                tool_run_owed: false,
                pending_native_scheduling: None,
                cancelled_run_groups: BTreeSet::new(),
                skip_tts: None,
                thought: TextBuffer::default(),
                thought_llm: None,
                thought_append: false,
                signed_history: None,
                completed_responses: VecDeque::new(),
                ended: false,
                callbacks_closed: false,
                context_cause_failed: false,
            }),
        }
    }
    /// Configure automatic thresholds and the default manual summary settings.
    pub fn with_summarization(
        mut self,
        config: crate::ContextSummarizationConfig,
    ) -> Result<Self, ProcessorError> {
        self.summarizer.configure(config)?;
        Ok(self)
    }
    /// Use a separate real native LLM instead of upstream request/result frames.
    pub fn with_summary_llm(mut self, llm: crate::SummaryLlm) -> Self {
        self.summarizer = self.summarizer.with_llm(llm);
        self
    }
    pub fn summarizer(&self) -> &crate::LLMContextSummarizer {
        &self.summarizer
    }
    pub fn context(&self) -> SharedContext {
        self.context.clone()
    }
    pub(crate) fn with_realtime(mut self, realtime: Arc<crate::realtime::RealtimeState>) -> Self {
        self.realtime = realtime;
        self
    }
    pub fn realtime_service_enabled(&self) -> Result<bool, ProcessorError> {
        self.realtime.enabled()
    }
    pub fn has_function_calls_in_progress(&self) -> Result<bool, ProcessorError> {
        Ok(!self.lock()?.pending.is_empty())
    }
    fn lock(&self) -> Result<MutexGuard<'_, AssistantState>, ProcessorError> {
        self.state
            .lock()
            .map_err(|_| ProcessorError::fatal("assistant aggregator lock poisoned"))
    }
    pub fn process_frame(
        &self,
        frame: Frame,
        direction: Direction,
    ) -> Result<AggregationOutput, ProcessorError> {
        self.process_at(frame, direction, self.realtime.origin.elapsed())
    }
    /// Deterministic paired handoff clock, on the same timeline as the user.
    pub fn process_at(
        &self,
        frame: Frame,
        direction: Direction,
        now: std::time::Duration,
    ) -> Result<AggregationOutput, ProcessorError> {
        self.process_at_inner(frame, direction, now, None)
    }
    fn process_at_inner(
        &self,
        mut frame: Frame,
        direction: Direction,
        now: std::time::Duration,
        cause: Option<&ProcessorContext>,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut state = self.lock()?;
        let mut output = AggregationOutput::default();
        if state.context_cause_failed
            && !matches!(
                frame.data,
                FrameData::End(_)
                    | FrameData::EndDetailed(_)
                    | FrameData::Stop
                    | FrameData::Cancel(_)
                    | FrameData::CancelDetailed(_)
            )
        {
            return Err(ProcessorError::fatal("realtime assistant inference could not be delivered; rebuild the pair and provider session from retained history"));
        }
        let native_batch = frame.take_native_tool_batch()?;
        let native_outcome = frame.take_native_tool_outcome()?;
        if let Some(outcome) = native_outcome {
            if state.ended {
                return Ok(output);
            }
            if direction != Direction::Downstream {
                return Err(ProcessorError::new(
                    "native tool settlement must arrive downstream",
                ));
            }
            return self
                .native_result(&mut state, &frame.data, outcome, &mut output)
                .map(|()| output);
        }
        if matches!(
            frame.data,
            FrameData::LlmText(_)
                | FrameData::Text(_)
                | FrameData::VisionText(_)
                | FrameData::TtsText(_)
                | FrameData::AggregatedText(_)
                | FrameData::LlmMessagesAppend(_)
                | FrameData::LlmMessagesUpdate(_)
                | FrameData::FunctionCallResult(_)
                | FrameData::LlmResponseStart
                | FrameData::LlmResponseEnd
                | FrameData::LlmFullResponseStart(_)
                | FrameData::LlmFullResponseEnd(_)
                | FrameData::VisionFullResponseStart(_)
                | FrameData::VisionFullResponseEnd(_)
                | FrameData::LlmMessagesAppendDetailed(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
                | FrameData::FunctionCallResultDetailed(_)
                | FrameData::LlmThoughtStartDetailed(_)
                | FrameData::LlmThoughtEndDetailed(_)
                | FrameData::LlmThoughtText(_)
                | FrameData::LlmContinuationCheckpoint
                | FrameData::LlmMarker(_)
                | FrameData::LlmSummarizeContext(_)
                | FrameData::LlmContextSummaryResult(_)
        ) && !state.seen.insert(frame.id)
        {
            return Ok(output);
        }
        if state.ended {
            return Ok(output);
        }
        self.realtime.observe(&frame.data)?;
        if self.realtime.enabled()?
            && frame
                .native_llm_continuation()
                .map_err(|error| ProcessorError::new(error.to_string()))?
                .is_some()
        {
            return Err(ProcessorError::new(
                "signed cascade continuation is incompatible with realtime aggregation",
            ));
        }
        if let Some(history) = frame
            .take_native_llm_continuation()
            .map_err(|error| ProcessorError::new(error.to_string()))?
        {
            if state.completed_responses.contains(&history.response_id()) {
                return Ok(output);
            }
            if state
                .signed_history
                .as_ref()
                .is_some_and(|existing| existing.id() != history.response_id())
            {
                if state.active || !state.pending_playback.is_empty() {
                    return Err(ProcessorError::new(
                        "new signed response overlaps pending delivered text",
                    ));
                }
                state.signed_history = None;
            }
            if state.signed_history.is_none() {
                state.signed_history = Some(crate::signed_history::SignedHistory::new(
                    history.response_id(),
                    history.provider(),
                    &*self.context.lock()?,
                ));
            }
            if history.is_complete() || history.is_checkpoint() {
                let signed_calls: Vec<_> = if history.is_complete() {
                    history
                        .blocks()
                        .iter()
                        .filter_map(|block| {
                            if let pipecat_core::LlmContinuationBlock::ToolCall(call) = block {
                                Some(call.clone())
                            } else {
                                None
                            }
                        })
                        .collect()
                } else {
                    vec![]
                };
                let checkpoint = history.is_checkpoint();
                let pending = state.aggregation.text.trim().to_owned();
                let mut context = self.context.lock()?;
                let tracking = state
                    .signed_history
                    .as_mut()
                    .expect("initialized response tracking");
                tracking.check_context(&context)?;
                let updated = tracking.update(history, &pending)?;
                if checkpoint && updated {
                    tracking.reconcile(&mut context)?;
                }
                let lineage = context.lineage;
                drop(context);
                for call in signed_calls {
                    state.signed_declarations.insert(&call, lineage)?;
                }
            }
        }
        if let Some(batch) = native_batch {
            if direction != Direction::Downstream {
                return Err(ProcessorError::new(
                    "native tool declarations must arrive downstream",
                ));
            }
            self.admit_native_batch(&mut state, &batch)?;
        }
        match &frame.data {
            FrameData::Interruption
            | FrameData::LlmMessagesUpdate(_)
            | FrameData::LlmMessagesUpdateDetailed(_)
            | FrameData::LlmMessagesTransform(_) => self.summarizer.cancel()?,
            FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_) => self.summarizer.close()?,
            FrameData::LlmContextSummaryResult(result) => {
                match self.summarizer.apply(result) {
                    Ok(Some(event)) => output.events.push(AggregationEvent::SummaryApplied(event)),
                    Ok(None) => {}
                    Err(error) => {
                        output.push(crate::summary::error_frame(error), Direction::Upstream)
                    }
                }
                return Ok(output);
            }
            FrameData::LlmSummarizeContext(config) => {
                if let Some(request) = self.summarizer.request(config.as_ref(), true)? {
                    output.push(
                        Frame::new(FrameData::LlmContextSummaryRequest(request)),
                        Direction::Upstream,
                    );
                }
                return Ok(output);
            }
            FrameData::LlmResponseStart
            | FrameData::LlmFullResponseStart(_)
            | FrameData::VisionFullResponseStart(_) => match self.summarizer.request(None, false) {
                Ok(Some(request)) => output.push(
                    Frame::new(FrameData::LlmContextSummaryRequest(request)),
                    Direction::Upstream,
                ),
                Ok(None) => {}
                Err(error) => output.push(crate::summary::error_frame(error), Direction::Upstream),
            },
            _ => {}
        }
        match &frame.data {
            FrameData::LlmContinuationCheckpoint => {}
            FrameData::LlmResponseStart | FrameData::LlmFullResponseStart(_)
            | FrameData::VisionFullResponseStart(_) => {
                self.realtime.handoff(now, cause)?;
                state.skip_tts = match &frame.data {
                    FrameData::LlmFullResponseStart(start) | FrameData::VisionFullResponseStart(start) => start.skip_tts,
                    _ => frame.metadata.get("skip_tts").and_then(Value::as_bool),
                };
                let awaited = std::mem::take(&mut state.awaiting_user_inference);
                if !state.llm_active && !awaited {
                    state.clear_runs();
                }
                state.llm_active = true;
                Self::start(&mut state, &mut output);
            }
            FrameData::LlmResponseEnd | FrameData::LlmFullResponseEnd(_)
            | FrameData::VisionFullResponseEnd(_) => {
                self.realtime.flush(&self.context, None, &mut output)?;
                state.llm_active = false;
                let pending = state.aggregation.text.trim().to_owned();
                if let Some(history) = &mut state.signed_history {
                    history.finish_response();
                    if history.has_snapshot() {
                        history.check_text(&pending)?;
                        history.reconcile(&mut *self.context.lock()?)?;
                    } else { Self::close_signed_history(&mut state); }
                }
                if self.config.text_mode == AssistantTextMode::Text
                    || (!state.generated_speech && state.pending_playback.is_empty())
                {
                    let failed = frame
                        .metadata
                        .get("failed")
                        .and_then(Value::as_bool)
                        .unwrap_or(false);
                    self.finish(&mut state, failed, &mut output)?;
                }
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::TtsStarted(started) => {
                if let Some(id) = &started.context_id {
                    if state.cancelled_contexts.contains(id) {
                        return Ok(output);
                    }
                    if !state.context_ids.contains(id) {
                        state.context_ids.push(id.clone());
                    }
                    if let Some(history) = &mut state.signed_history { history.playback_started(id)?; }
                }
                if started.append_to_context {
                    Self::start(&mut state, &mut output);
                    state.generated_speech = true;
                }
                output.push(frame, direction);
            }
            FrameData::TtsText(text) => {
                let acknowledged = self.config.text_mode == AssistantTextMode::Text
                    || frame.metadata.get("spoken").and_then(Value::as_bool) == Some(true);
                let stale = text
                    .context_id
                    .as_ref()
                    .is_some_and(|id| state.cancelled_contexts.contains(id));
                if acknowledged && !stale && state.active && text.text.append_to_context {
                    let content = text
                        .raw_text
                        .as_deref()
                        .filter(|raw| !raw.is_empty())
                        .unwrap_or(&text.text.text);
                    state
                        .aggregation
                        .append(content, text.text.includes_inter_frame_spaces);
                }
            }
            FrameData::LlmText(text) | FrameData::Text(text) | FrameData::VisionText(text) => {
                if state.active
                    && text.append_to_context
                    && text.skip_tts.or(state.skip_tts) != Some(true)
                    && !text.text.trim().is_empty()
                {
                    state.generated_speech = true;
                }
                if (self.config.text_mode == AssistantTextMode::Text || text.skip_tts.or(state.skip_tts) == Some(true))
                    && state.active
                    && text.append_to_context
                {
                    state
                        .aggregation
                        .append(&text.text, text.includes_inter_frame_spaces);
                }
            }
            FrameData::AggregatedText(text) => {
                if (self.config.text_mode == AssistantTextMode::Text || text.text.skip_tts.or(state.skip_tts) == Some(true))
                    && state.active
                    && text.text.append_to_context
                {
                    let content = text
                        .raw_text
                        .as_deref()
                        .filter(|raw| !raw.is_empty())
                        .unwrap_or(&text.text.text);
                    state
                        .aggregation
                        .append(content, text.text.includes_inter_frame_spaces);
                }
            }
            FrameData::LlmThoughtStart | FrameData::LlmThoughtStartDetailed(_) => {
                state.thought = TextBuffer::default();
                state.thought_append = false;
                state.thought_llm = None;
                if let FrameData::LlmThoughtStartDetailed(thought) = &frame.data {
                    if thought.append_to_context && thought.llm.as_deref().is_none_or(str::is_empty) {
                        return Err(ProcessorError::new("context-appending thought requires its LLM identity"));
                    }
                    state.thought_append = thought.append_to_context;
                    state.thought_llm = thought.llm.clone();
                }
            }
            FrameData::LlmThoughtText(text) => state.thought.append(&text.text, text.includes_inter_frame_spaces),
            FrameData::LlmThoughtEnd | FrameData::LlmThoughtEndDetailed(_) => {
                let thought = state.thought.take();
                if state.thought_append {
                    let mut message = text_message(ChatRole::Assistant, thought);
                    message.extra.insert("llm".into(), state.thought_llm.clone().into());
                    message.extra.insert("type".into(), "thought".into());
                    if let FrameData::LlmThoughtEndDetailed(end) = &frame.data {
                        message.extra.insert("signature".into(), end.signature.clone().unwrap_or(Value::Null));
                    }
                    self.context.append_rich(message)?;
                }
                state.thought_append = false;
                state.thought_llm = None;
            }
            FrameData::Transcription(_) | FrameData::InterimTranscription(_)
            | FrameData::EagerTranscription(_) | FrameData::Translation(_)
            | FrameData::TranscriptionDetailed(_) | FrameData::InterimTranscriptionDetailed(_)
            | FrameData::EagerTranscriptionDetailed(_) | FrameData::TranslationDetailed(_) => {}
            FrameData::LlmMarker(marker) => {
                state.transcript_markers.observe(&marker.marker)?;
                if marker.append_to_context_immediately {
                    self.context.append(Message::text(Role::Assistant, &marker.marker))?;
                    output.context(&self.context, Direction::Downstream)?;
                } else {
                    state.aggregation.append(&marker.marker, false);
                }
            }
            FrameData::LlmAssistantPushAggregation => {
                self.finish(&mut state, false, &mut output)?;
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::FlushDetailed(flush) if flush.origin.as_deref() == Some("assistant_turn_end") => {
                self.finish(&mut state, false, &mut output)?;
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::Flush(flush) if flush.origin.as_deref() == Some("assistant_turn_end") => {
                self.finish(&mut state, false, &mut output)?;
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::Interruption => {
                self.finish_on_cancellation(&mut state, true, &mut output)?;
                Self::close_signed_history(&mut state);
                state.llm_active = false;
                state.thought = TextBuffer::default();
                state.thought_append = false;
                state.bot_speaking = false;
                if frame
                    .metadata
                    .get("user_context_follows")
                    .and_then(Value::as_bool)
                    != Some(true)
                {
                    state.clear_runs();
                    state.awaiting_user_inference = false;
                }
                Self::invalidate_audio(&mut state);
                for id in std::mem::take(&mut state.pending_playback) {
                    state.cancelled_contexts.push_back(id);
                    if state.cancelled_contexts.len() > 512 {
                        state.cancelled_contexts.pop_front();
                    }
                }
                let cancel: Vec<_> = state
                    .pending
                    .values()
                    .flatten()
                    .filter(|call| call.cancel_on_interruption && !state.native_tools.calls.contains_key(&call.tool_call_id))
                    .cloned()
                    .collect();
                for call in cancel {
                    state.provisional_tools.insert(call.clone(), self.context.lock()?.lineage, true)?;
                    self.cancel_call(&mut state, &call.tool_call_id)?;
                }
                let declarations: Vec<_> = state
                    .pending
                    .iter()
                    .filter(|(id, call)| call.is_none() && !state.native_tools.calls.contains_key(*id))
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in declarations {
                    state.pending.remove(&id);
                    state.declared_tools.remove(&id);
                    state.interrupted_declarations.insert(&id);
                }

                if frame
                    .metadata
                    .get("user_turn_start")
                    .and_then(Value::as_bool)
                    == Some(true)
                {
                    state.user_speaking = true;
                }
                output.push(frame, direction);
            }
            FrameData::End(_)
            | FrameData::EndDetailed(_)
            | FrameData::Stop
            | FrameData::Cancel(_)
            | FrameData::CancelDetailed(_) => {
                self.finish_on_cancellation(
                    &mut state,
                    matches!(
                        frame.data,
                        FrameData::Cancel(_) | FrameData::CancelDetailed(_)
                    ),
                    &mut output,
                )?;
                state.ended = true;
                state.native_tools.clear();
                state.declared_tools.clear();
                state.signed_declarations.clear();
                state.provisional_tools.clear();
                state.pending.clear();
                state.completed.clear();
                state.interrupted_declarations.clear();
                state.clear_runs();
                Self::close_signed_history(&mut state);
                state.llm_active = false;
                output.push(frame, direction);
            }
            FrameData::UserStartedSpeaking => {
                state.user_speaking = true;
                if frame
                    .metadata
                    .get("user_context_follows")
                    .and_then(Value::as_bool)
                    != Some(true)
                {
                    state.awaiting_user_inference = false;
                }
                output.push(frame, direction);
            }
            FrameData::UserStoppedSpeaking => {
                state.user_speaking = false;
                // Empty/muted turns have no user inference to consume deferred tool results.
                let no_user_context = frame
                    .metadata
                    .get("user_context_follows")
                    .and_then(Value::as_bool)
                    == Some(false);
                let frame_context_follows = frame
                    .metadata
                    .get("user_context_follows")
                    .and_then(Value::as_bool)
                    == Some(true);
                output.push(frame, direction);
                if frame_context_follows {
                    state.awaiting_user_inference = true;
                }
                if no_user_context {
                    state.awaiting_user_inference = false;
                    self.maybe_run(&mut state, &mut output)?;
                }
            }
            FrameData::BotStartedSpeaking => {
                state.bot_speaking = true;
                output.push(frame, direction);
            }
            FrameData::BotStoppedSpeaking => {
                state.bot_speaking = !state.pending_playback.is_empty();
                output.push(frame, direction);
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::FunctionCallsStarted(calls) => {
                for call in calls {
                    self.declare_call(&mut state, call)?;
                    if !state.is_completed(&call.tool_call_id)
                        && !state.pending.contains_key(&call.tool_call_id)
                    {
                        // This system frame can overtake the normal response-start marker.
                        state.llm_active = true;
                        state.pending.insert(call.tool_call_id.clone(), None)?;
                    }
                }
            }
            FrameData::FunctionCallsStartedDetailed(calls) => {
                for call in calls {
                    self.declare_call(&mut state, &pipecat_core::FunctionCall { function_name: call.function_name.clone(), tool_call_id: call.tool_call_id.clone(), arguments: serde_json::to_value(&call.arguments).map_err(|error| ProcessorError::new(error.to_string()))? })?;
                    if !state.is_completed(&call.tool_call_id) && !state.pending.contains_key(&call.tool_call_id) {
                        state.llm_active = true;
                        state.pending.insert(call.tool_call_id.clone(), None)?;
                    }
                }
            }
            FrameData::FunctionCallInProgress(call) => self.start_call(&mut state, call)?,
            FrameData::FunctionCallResult(result) => {
                if state.native_tools.calls.contains_key(&result.tool_call_id) {
                    return Err(ProcessorError::new("native tool requires its terminal receipt"));
                }
                self.result(&mut state, result, None, &mut output)?
            }
            FrameData::FunctionCallResultDetailed(result) => {
                if state.native_tools.calls.contains_key(&result.tool_call_id) {
                    return Err(ProcessorError::new("native tool requires its terminal receipt"));
                }
                let accepted = state.pending.get(&result.tool_call_id).is_some_and(Option::is_some);
                let properties = result.properties.as_ref();
                self.result(&mut state, &FunctionCallResultFrame {
                    function_name: result.function_name.clone(), tool_call_id: result.tool_call_id.clone(),
                    arguments: result.arguments.clone(), result: result.result.clone(),
                    run_llm: properties.and_then(|properties| properties.run_llm).or(result.run_llm),
                    is_final: properties.is_none_or(|properties| properties.is_final), error: result.error.clone(),
                }, None, &mut output)?;
                if accepted {
                    if let Some(callback) = properties.and_then(|properties| properties.on_context_updated.clone()) {
                        output.actions.push(crate::AggregationAction::ContextUpdated(callback));
                    }
                }
            }
            FrameData::FunctionCallCancel(cancel) => {
                if direction == Direction::Upstream { output.push(frame, direction); return Ok(output); }
                if state.native_tools.calls.contains_key(&cancel.tool_call_id) { return Ok(output); }
                let group = state.pending.get(&cancel.tool_call_id)
                    .and_then(Option::as_ref)
                    .and_then(|call| call.group_id.clone());
                let declared = state
                    .pending
                    .get(&cancel.tool_call_id)
                    .is_some_and(Option::is_none);
                let existed = if declared {
                    state.pending.remove(&cancel.tool_call_id);
                    state.declared_tools.remove(&cancel.tool_call_id);
                    Self::mark_completed(&mut state, &cancel.tool_call_id);
                    true
                } else {
                    self.cancel_call(&mut state, &cancel.tool_call_id)?
                };
                if existed && cancel.run_llm {
                    if let Some(group) = group.filter(|group| !group.is_empty()) {
                        state.cancelled_run_groups.insert(group);
                    } else if declared {
                        state.run_owed = true;
                    } else {
                        state.tool_run_owed = true;
                    }
                    self.maybe_run(&mut state, &mut output)?;
                }
            }
            FrameData::LlmRun => {
                state.run_owed = true;
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::LlmMessagesAppend(messages) | FrameData::LlmMessagesUpdate(messages) => {
                let mut context = self.context.lock()?;
                context.append_legacy(&messages.messages, matches!(frame.data, FrameData::LlmMessagesUpdate(_)));
                drop(context);
                state.run_owed |= messages.run_llm;
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::LlmMessagesAppendDetailed(messages) | FrameData::LlmMessagesUpdateDetailed(messages) => {
                let mut context = self.context.lock()?;
                if matches!(frame.data, FrameData::LlmMessagesUpdateDetailed(_)) {
                    context.set_rich(messages.messages.clone());
                } else { context.append_rich(messages.messages.clone()); }
                drop(context);
                state.run_owed |= messages.run_llm == Some(true);
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::LlmMessagesTransform(transform) => {
                self.context.lock()?.transform(&transform.transform)?;
                state.run_owed |= transform.run_llm == Some(true);
                self.maybe_run(&mut state, &mut output)?;
            }
            FrameData::LlmSetTools(tools) => self.context.lock()?.tools = tools.clone(),
            FrameData::LlmSetToolChoice(choice) => self.context.lock()?.tool_choice = Some(choice.clone()),
            FrameData::ProposedUserStartedSpeaking if self.realtime.resolves(true)? => {}
            FrameData::ProposedUserStoppedSpeaking if self.realtime.resolves(false)? => {}
            FrameData::UserImage(_) | FrameData::AssistantImage(_) | FrameData::UserFileDetailed(_)
            | FrameData::UserFile(_) => return Err(ProcessorError::new("raw image/file context conversion is not supported; submit encoded rich context messages")),
            _ => output.push(frame, direction),
        }
        Ok(output)
    }
    fn admit_native_batch(
        &self,
        state: &mut AssistantState,
        batch: &Arc<pipecat_core::NativeToolCallBatch>,
    ) -> Result<(), ProcessorError> {
        crate::tool_history::NativeTools::check_batch(batch)?;
        let context = self.context.lock()?;
        let calls: Vec<_> = batch
            .members()
            .iter()
            .map(|(call, policy)| FunctionCallInProgressFrame {
                function_name: call.function_name.clone(),
                tool_call_id: call.tool_call_id.clone(),
                arguments: call.arguments.clone(),
                cancel_on_interruption: *policy,
                group_id: Some(batch.group_id().into()),
            })
            .collect();
        for call in &calls {
            if let Some(Some(old)) = state.pending.get(&call.tool_call_id) {
                if old != call {
                    return Err(ProcessorError::new(
                        "native batch differs from running policy",
                    ));
                }
            }
            if !state.native_tools.calls.contains_key(&call.tool_call_id)
                && !state
                    .pending
                    .get(&call.tool_call_id)
                    .is_some_and(Option::is_some)
                && context
                    .messages
                    .iter()
                    .flat_map(|message| &message.tool_calls)
                    .any(|existing| existing.tool_call_id == call.tool_call_id)
                && !state
                    .signed_declarations
                    .calls
                    .get(&call.tool_call_id)
                    .is_some_and(|(signed, lineage, _)| {
                        signed.function_name == call.function_name
                            && signed.arguments == call.arguments
                            && *lineage == context.lineage
                    })
            {
                return Err(ProcessorError::new(
                    "native batch cannot adopt existing conversation calls",
                ));
            }
        }
        state.pending.check_calls(calls.iter().filter(|call| {
            !state
                .native_tools
                .calls
                .get(&call.tool_call_id)
                .is_some_and(|entry| entry.terminal.is_some())
        }))?;
        state
            .native_tools
            .reserve(&calls, context.lineage, Some(batch))
    }
    fn native_result(
        &self,
        state: &mut AssistantState,
        data: &FrameData,
        outcome: Arc<pipecat_core::NativeToolCallOutcome>,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let id = &outcome.call().tool_call_id;
        let cancelled = matches!(data, FrameData::FunctionCallCancel(_));
        if let Some((old, was_cancelled)) = state
            .native_tools
            .calls
            .get(id)
            .and_then(|entry| entry.terminal.as_ref())
        {
            return if *old == outcome.id() && *was_cancelled == cancelled {
                Ok(())
            } else {
                Err(ProcessorError::new(
                    "conflicting committed native tool outcome",
                ))
            };
        }
        let calls: Vec<_> = if let Some(batch) = outcome.batch() {
            crate::tool_history::NativeTools::check_batch(batch)?;
            batch
                .members()
                .iter()
                .map(
                    |(call, cancel_on_interruption)| FunctionCallInProgressFrame {
                        function_name: call.function_name.clone(),
                        tool_call_id: call.tool_call_id.clone(),
                        arguments: call.arguments.clone(),
                        cancel_on_interruption: *cancel_on_interruption,
                        group_id: Some(batch.group_id().into()),
                    },
                )
                .collect()
        } else {
            let group_id = state
                .pending
                .get(id)
                .and_then(Option::as_ref)
                .and_then(|call| call.group_id.clone())
                .or_else(|| {
                    state
                        .provisional_tools
                        .calls
                        .get(id)
                        .and_then(|entry| entry.call.group_id.clone())
                });
            vec![FunctionCallInProgressFrame {
                function_name: outcome.call().function_name.clone(),
                tool_call_id: id.clone(),
                arguments: outcome.call().arguments.clone(),
                cancel_on_interruption: outcome.cancel_on_interruption(),
                group_id,
            }]
        };
        let context = self.context.lock()?;
        let lineage = context.lineage;
        for call in &calls {
            if let Some((declared, declared_lineage, _)) =
                state.declared_tools.calls.get(&call.tool_call_id)
            {
                if declared.function_name != call.function_name
                    || declared.arguments != call.arguments
                    || *declared_lineage != lineage
                {
                    return Err(ProcessorError::new(
                        "native outcome differs from declared call",
                    ));
                }
            }
            if let Some(Some(old)) = state.pending.get(&call.tool_call_id) {
                if old != call {
                    return Err(ProcessorError::new(
                        "native tool outcome differs from running policy",
                    ));
                }
            }
            let native_terminal = state
                .native_tools
                .calls
                .get(&call.tool_call_id)
                .is_some_and(|entry| entry.terminal.is_some());
            let provisional = state.provisional_tools.calls.get(&call.tool_call_id);
            if state.is_completed(&call.tool_call_id) && !native_terminal && provisional.is_none() {
                return Err(ProcessorError::new(
                    "native outcome cannot replace committed tool history",
                ));
            }
            if let Some(provisional) = provisional {
                if provisional.call != *call || provisional.lineage != lineage {
                    return Err(ProcessorError::new(
                        "native outcome differs from provisional tool identity",
                    ));
                }
                if provisional.materialized
                    && !context.messages.iter().any(|message| {
                        message.role == ChatRole::Tool
                            && message.tool_call_id.as_deref() == Some(call.tool_call_id.as_str())
                            && message.content == MessageContent::Text("CANCELLED".into())
                    })
                {
                    return Err(ProcessorError::new("provisional tool placeholder changed"));
                }
            }
            if let Some(existing) = context
                .messages
                .iter()
                .flat_map(|message| &message.tool_calls)
                .find(|existing| existing.tool_call_id == call.tool_call_id)
            {
                if existing.function_name != call.function_name
                    || existing.arguments != call.arguments
                {
                    return Err(ProcessorError::new(
                        "native tool identity conflicts with conversation",
                    ));
                }
                if !state
                    .pending
                    .get(&call.tool_call_id)
                    .is_some_and(Option::is_some)
                    && !native_terminal
                    && !state
                        .signed_history
                        .as_ref()
                        .is_some_and(|history| history.owns_call(existing))
                    && !state
                        .signed_declarations
                        .calls
                        .get(&call.tool_call_id)
                        .is_some_and(|(signed, signed_lineage, _)| {
                            signed == existing && *signed_lineage == lineage
                        })
                    && !provisional.is_some_and(|entry| entry.materialized)
                {
                    return Err(ProcessorError::new(
                        "native tool cannot adopt an existing conversation call",
                    ));
                }
            }
        }
        if let Some(history) = &state.signed_history {
            let mut additions = Vec::new();
            for call in &calls {
                if state
                    .native_tools
                    .calls
                    .get(&call.tool_call_id)
                    .is_some_and(|entry| entry.terminal.is_some())
                    || state
                        .pending
                        .get(&call.tool_call_id)
                        .is_some_and(Option::is_some)
                    || state
                        .provisional_tools
                        .calls
                        .get(&call.tool_call_id)
                        .is_some_and(|entry| entry.materialized)
                {
                    continue;
                }
                if !context
                    .messages
                    .iter()
                    .flat_map(|message| &message.tool_calls)
                    .any(|old| old.tool_call_id == call.tool_call_id)
                {
                    additions.push(pipecat_services::frame_context::to_core_message(
                        &Message::assistant_tool_calls(vec![ToolCall {
                            id: call.tool_call_id.clone(),
                            name: call.function_name.clone(),
                            arguments: call.arguments.clone(),
                        }]),
                    ));
                }
                additions.push(if call.cancel_on_interruption {
                    tool_message(&call.tool_call_id, "IN_PROGRESS")
                } else {
                    async_message(&call.tool_call_id, "started", None)
                });
            }
            let call = calls
                .iter()
                .find(|call| &call.tool_call_id == id)
                .expect("native batch contains terminal call");
            match data {
                FrameData::FunctionCallResult(result) if is_deferred(&context.messages, call) => {
                    let content = if let Some(error) = &result.error {
                        json!({"error": error}).to_string()
                    } else if result.result.is_null() {
                        "COMPLETED".into()
                    } else {
                        result.result.to_string()
                    };
                    additions.push(async_message(id, "final", Some(&content)));
                }
                FrameData::FunctionCallCancel(_) if !call.cancel_on_interruption => {
                    additions.push(async_message(
                        id,
                        "cancelled",
                        Some("CANCELLED: this tool call was cancelled before it returned a result"),
                    ));
                }
                _ => {}
            }
            history.check_messages(context.messages.len(), &additions)?;
        }
        drop(context);
        state.pending.check_calls(calls.iter().filter(|call| {
            !state
                .native_tools
                .calls
                .get(&call.tool_call_id)
                .is_some_and(|entry| entry.terminal.is_some())
        }))?;
        state
            .native_tools
            .reserve(&calls, lineage, outcome.batch())?;
        for call in &calls {
            if state
                .native_tools
                .calls
                .get(&call.tool_call_id)
                .is_some_and(|entry| entry.terminal.is_some())
            {
                continue;
            }
            if let Some(provisional) = state.provisional_tools.remove(&call.tool_call_id) {
                state.completed.remove(&call.tool_call_id);
                if provisional.materialized {
                    update_tool_result(
                        &mut self.context.lock()?.messages,
                        &call.tool_call_id,
                        "IN_PROGRESS",
                    )?;
                    state
                        .pending
                        .insert(call.tool_call_id.clone(), Some(call.clone()))?;
                    continue;
                }
            }
            state.interrupted_declarations.remove(&call.tool_call_id);
            self.start_call(state, call)?;
        }
        match data {
            FrameData::FunctionCallResult(result) => self.result(
                state,
                result,
                Some(pipecat_core::RealtimeNativeToolSchedulingReason::from_outcome(&outcome)),
                output,
            )?,
            FrameData::FunctionCallCancel(cancel) => {
                let group = state
                    .pending
                    .get(id)
                    .and_then(Option::as_ref)
                    .and_then(|call| call.group_id.clone());
                if self.cancel_call(state, id)? {
                    if cancel.run_llm {
                        state.pending_native_scheduling = Some(
                            pipecat_core::RealtimeNativeToolSchedulingReason::from_outcome(
                                &outcome,
                            ),
                        );
                        if let Some(group) = group.filter(|group| !group.is_empty()) {
                            state.cancelled_run_groups.insert(group);
                        } else {
                            state.tool_run_owed = true;
                        }
                    }
                    // A false cancellation creates no new intent, but clearing
                    // the last grouped call can release an already owed request.
                    self.maybe_run(state, output)?;
                }
            }
            _ => {
                return Err(ProcessorError::new(
                    "native tool outcome requires a terminal frame",
                ))
            }
        }
        state.native_tools.finish(outcome, cancelled);
        Ok(())
    }
    fn cancel_callbacks(&self) -> Result<(), ProcessorError> {
        self.lock()?.callbacks_closed = true;
        let tasks = std::mem::take(
            &mut *self
                .callback_tasks
                .lock()
                .map_err(|_| ProcessorError::fatal("callback task lock poisoned"))?,
        );
        for (id, ctx) in tasks {
            ctx.cancel_task(id);
        }
        Ok(())
    }
    /// Register audio that awaits playback acknowledgement without claiming it
    /// was heard. This gates tool-triggered inference while filler is playing.
    pub fn begin_playback(&self, context_id: &str) -> Result<(), ProcessorError> {
        if self.config.text_mode != AssistantTextMode::Spoken || context_id.is_empty() {
            return Err(ProcessorError::new(
                "playback registration requires Spoken mode and a context ID",
            ));
        }
        let mut state = self.lock()?;
        if state.ended || state.cancelled_contexts.iter().any(|id| id == context_id) {
            return Err(ProcessorError::new("playback context is already closed"));
        }
        if let Some(history) = &mut state.signed_history {
            history.playback_started(context_id)?;
        }
        state.pending_playback.insert(context_id.to_owned());
        state.bot_speaking = true;
        Ok(())
    }
    /// Atomically commit a complete, verified playback context. A transport
    /// calls this before acknowledging playback to its client, so a system-lane
    /// interruption cannot overtake normal-lane text acknowledgements. The
    /// transport must validate the context ID, generation, and completion.
    /// These parts must not also be submitted through the frame interface.
    pub fn confirm_playback(
        &self,
        context_id: &str,
        parts: &[pipecat_core::AggregatedTextFrame],
    ) -> Result<AggregationOutput, ProcessorError> {
        if self.config.text_mode != AssistantTextMode::Spoken {
            return Err(ProcessorError::new(
                "playback confirmation requires Spoken mode",
            ));
        }
        if context_id.is_empty()
            || parts
                .iter()
                .any(|part| part.context_id.as_deref() != Some(context_id))
        {
            return Err(ProcessorError::new(
                "playback text must match its nonempty context ID",
            ));
        }
        let mut state = self.lock()?;
        let mut output = AggregationOutput::default();
        if state.ended || state.cancelled_contexts.iter().any(|id| id == context_id) {
            return Ok(output);
        }
        if self.realtime.enabled()? {
            // Validate the user projection before consuming the receipt or
            // adding any of its acknowledged words to pending assistant text.
            self.context.frame_data()?;
            self.realtime.flush(&self.context, None, &mut output)?;
        }
        let signed = state.signed_history.is_some();
        if let Some(history) = &state.signed_history {
            let mut prospective = state.aggregation.clone();
            for part in parts.iter().filter(|part| part.text.append_to_context) {
                prospective.append(
                    part.raw_text
                        .as_deref()
                        .filter(|raw| !raw.is_empty())
                        .unwrap_or(&part.text.text),
                    part.text.includes_inter_frame_spaces,
                );
            }
            history.check_context(&*self.context.lock()?)?;
            history.check_text(prospective.text.trim())?;
            history.check_playback(context_id)?;
        }
        if !signed {
            state.pending_playback.remove(context_id);
            state.bot_speaking = !state.pending_playback.is_empty();
        }
        if !parts
            .iter()
            .any(|part| part.text.append_to_context && !part.text.text.is_empty())
        {
            if signed {
                self.finish(&mut state, false, &mut output)?;
                Self::complete_signed_playback(&mut state, context_id)?;
            } else {
                state.cancelled_contexts.push_back(context_id.to_owned());
                if state.cancelled_contexts.len() > 512 {
                    state.cancelled_contexts.pop_front();
                }
            }
            self.maybe_run(&mut state, &mut output)?;
            return Ok(output);
        }
        let prior = signed.then(|| {
            (
                state.aggregation.clone(),
                state.active,
                state.generated_speech,
            )
        });
        Self::start(&mut state, &mut output);
        if !signed && !state.context_ids.iter().any(|id| id == context_id) {
            state.context_ids.push(context_id.into());
        }
        for part in parts {
            if part.text.append_to_context {
                let text = part
                    .raw_text
                    .as_deref()
                    .filter(|text| !text.is_empty())
                    .unwrap_or(&part.text.text);
                state
                    .aggregation
                    .append(text, part.text.includes_inter_frame_spaces);
            }
        }
        if let Err(error) = self.finish(&mut state, false, &mut output) {
            if let Some((aggregation, active, generated_speech)) = prior {
                state.aggregation = aggregation;
                state.active = active;
                state.generated_speech = generated_speech;
            }
            return Err(error);
        }
        if signed {
            Self::complete_signed_playback(&mut state, context_id)?;
        }
        self.maybe_run(&mut state, &mut output)?;
        Ok(output)
    }
    fn complete_signed_playback(
        state: &mut AssistantState,
        id: &str,
    ) -> Result<(), ProcessorError> {
        if let Some(history) = &mut state.signed_history {
            history.playback_confirmed(id)?;
        }
        state.context_ids.retain(|context| context != id);
        state.cancelled_contexts.push_back(id.into());
        if state.cancelled_contexts.len() > 512 {
            state.cancelled_contexts.pop_front();
        }
        state.pending_playback.remove(id);
        state.bot_speaking = !state.pending_playback.is_empty();
        Ok(())
    }
    pub(crate) fn prepare_user_interrupt(
        &self,
        speaking: bool,
    ) -> Result<AggregationOutput, ProcessorError> {
        let mut frame = Frame::interruption();
        frame
            .metadata
            .insert("user_turn_start".into(), speaking.into());
        self.process_frame(frame, Direction::Downstream)
    }
    /// Snapshot and acknowledge prior tool results under the same lock that
    /// admits new results. Completions after this snapshot still owe inference.
    pub(crate) fn prepare_user_context(&self) -> Result<FrameData, ProcessorError> {
        self.prepare_user_context_at_lineage(None)
    }
    pub(crate) fn prepare_user_context_at_lineage(
        &self,
        expected: Option<u64>,
    ) -> Result<FrameData, ProcessorError> {
        // Preserve assistant → context lock order. A conflicting replacement must
        // not acknowledge any tool result or authorize a substituted snapshot.
        let mut state = self.lock()?;
        let context = self.context.frame_data_at_lineage(expected)?;
        state.clear_runs();
        state.awaiting_user_inference = true;
        Ok(context)
    }
    pub(crate) fn prepare_user_start(&self) -> Result<(), ProcessorError> {
        let mut state = self.lock()?;
        state.user_speaking = true;
        state.awaiting_user_inference = false;
        Ok(())
    }
    fn start(state: &mut AssistantState, output: &mut AggregationOutput) {
        if state.active {
            return;
        }
        state.transcript_markers.clear();
        state.active = true;
        state.generated_speech = false;
        output.events.push(AggregationEvent::AssistantTurnStarted);
    }
    fn finish(
        &self,
        state: &mut AssistantState,
        interrupted: bool,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        if state.active {
            self.realtime.flush(&self.context, None, output)?;
        }
        if !state.active {
            state.transcript_markers.clear();
            return Ok(());
        }
        let text = state.aggregation.text.trim().to_owned();
        let transcript = state.transcript_markers.project(&text);
        let mut context = self.context.lock()?;
        if let Some(history) = &state.signed_history {
            history.check_text(&text)?;
            history.check_context(&context)?;
            if !text.is_empty() {
                history.check_text_commit(&context, &text)?;
            }
        }
        state.aggregation.take();
        if !text.is_empty() {
            context.append_native(Message::text(Role::Assistant, text.clone()));
            if let Some(history) = &mut state.signed_history {
                let index = context.messages.len() - 1;
                history.text(index, &context.messages[index], text.clone())?;
                history.reconcile(&mut context)?;
            }
        }
        drop(context);
        if !text.is_empty() {
            output.context(&self.context, Direction::Downstream)?;
        }
        state.active = false;
        if interrupted || state.signed_history.is_none() {
            Self::invalidate_audio(state);
        }
        output.events.push(AggregationEvent::AssistantTurnStopped {
            text: transcript,
            interrupted,
        });
        state.transcript_markers.clear();
        Ok(())
    }
    fn close_signed_history(state: &mut AssistantState) {
        if let Some(history) = state.signed_history.take() {
            state.completed_responses.push_back(history.id());
            if state.completed_responses.len() > 512 {
                state.completed_responses.pop_front();
            }
        }
    }
    fn finish_on_cancellation(
        &self,
        state: &mut AssistantState,
        interrupted: bool,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        match self.finish(state, interrupted, output) {
            Ok(()) => Ok(()),
            Err(error) if state.signed_history.is_some() => {
                state.aggregation = TextBuffer::default();
                state.transcript_markers.clear();
                state.active = false;
                state.generated_speech = false;
                Self::invalidate_audio(state);
                output.push(
                    Frame::new(FrameData::Error(pipecat_core::ErrorFrame {
                        error: error.message,
                        fatal: error.fatal,
                        processor: Some("LLMAssistantAggregator".into()),
                        category: error.category,
                    })),
                    Direction::Upstream,
                );
                output.events.push(AggregationEvent::AssistantTurnStopped {
                    text: String::new(),
                    interrupted: true,
                });
                Ok(())
            }
            Err(error) => Err(error),
        }
    }
    fn invalidate_audio(state: &mut AssistantState) {
        for id in state.context_ids.drain(..) {
            state.cancelled_contexts.push_back(id);
            if state.cancelled_contexts.len() > 512 {
                state.cancelled_contexts.pop_front();
            }
        }
    }
    fn declare_call(
        &self,
        state: &mut AssistantState,
        call: &pipecat_core::FunctionCall,
    ) -> Result<(), ProcessorError> {
        if let Some(native) = state.native_tools.calls.get(&call.tool_call_id) {
            if native.call.function_name != call.function_name
                || native.call.arguments != call.arguments
            {
                return Err(ProcessorError::new(
                    "tool declaration differs from native identity",
                ));
            }
            return Ok(());
        }
        if state.is_completed(&call.tool_call_id) {
            return Ok(());
        }
        if let Some(Some(policy)) = state.pending.get(&call.tool_call_id) {
            if policy.function_name != call.function_name || policy.arguments != call.arguments {
                return Err(ProcessorError::new(
                    "tool declaration differs from running identity",
                ));
            }
            return Ok(());
        }
        state
            .declared_tools
            .insert(call, self.context.lock()?.lineage)
    }
    fn start_call(
        &self,
        state: &mut AssistantState,
        call: &FunctionCallInProgressFrame,
    ) -> Result<(), ProcessorError> {
        if call.tool_call_id.is_empty()
            || call.function_name.is_empty()
            || !call.arguments.is_object()
        {
            return Err(ProcessorError::new(
                "tool call requires id, name, and object arguments",
            ));
        }
        if let Some((declared, lineage, _)) = state.declared_tools.calls.get(&call.tool_call_id) {
            if declared.function_name != call.function_name
                || declared.arguments != call.arguments
                || *lineage != self.context.lock()?.lineage
            {
                return Err(ProcessorError::new("tool policy differs from declaration"));
            }
        }
        if let Some(native) = state.native_tools.calls.get(&call.tool_call_id) {
            if native.call != *call {
                return Err(ProcessorError::new(
                    "tool declaration differs from native policy",
                ));
            }
        }
        if state.is_completed(&call.tool_call_id) {
            return Ok(());
        }
        if state.interrupted_declarations.remove(&call.tool_call_id) && call.cancel_on_interruption
        {
            state
                .provisional_tools
                .insert(call.clone(), self.context.lock()?.lineage, false)?;
            Self::mark_completed(state, &call.tool_call_id);
            return Ok(());
        }
        if let Some(Some(old)) = state.pending.get(&call.tool_call_id) {
            if old != call {
                return Err(ProcessorError::new("conflicting duplicate tool call id"));
            }
            return Ok(());
        }
        state.pending.check(&call.tool_call_id, Some(call))?;
        let mut context = self.context.lock()?;
        let canonical = pipecat_core::FunctionCall {
            function_name: call.function_name.clone(),
            tool_call_id: call.tool_call_id.clone(),
            arguments: call.arguments.clone(),
        };
        let existing = context
            .messages
            .iter()
            .flat_map(|message| &message.tool_calls)
            .find(|old| old.tool_call_id == call.tool_call_id);
        let retained = existing.is_some_and(|existing| existing == &canonical)
            && (state
                .signed_history
                .as_ref()
                .is_some_and(|history| history.owns_call(&canonical))
                || state
                    .signed_declarations
                    .calls
                    .get(&call.tool_call_id)
                    .is_some_and(|(signed, lineage, _)| {
                        signed == &canonical && *lineage == context.lineage
                    }));
        if !retained && existing.is_some() {
            return Err(ProcessorError::new(
                "tool call id already exists in conversation",
            ));
        }
        let declaration = (!retained).then(|| {
            Message::assistant_tool_calls(vec![ToolCall {
                id: call.tool_call_id.clone(),
                name: call.function_name.clone(),
                arguments: call.arguments.clone(),
            }])
        });
        let started = if call.cancel_on_interruption {
            tool_message(&call.tool_call_id, "IN_PROGRESS")
        } else {
            async_message(&call.tool_call_id, "started", None)
        };
        if let Some(history) = &state.signed_history {
            let additions: Vec<_> = declaration
                .iter()
                .map(pipecat_services::frame_context::to_core_message)
                .chain(std::iter::once(started.clone()))
                .collect();
            history.check_messages(context.messages.len(), &additions)?;
        }
        let first_new = context.messages.len();
        if let Some(declaration) = declaration {
            context.append_native(declaration);
        }
        context.messages.push(started);
        if let Some(history) = &mut state.signed_history {
            for index in first_new..context.messages.len() {
                history.message(index, &context.messages[index])?;
            }
        }
        state.declared_tools.remove(&call.tool_call_id);
        state
            .pending
            .insert(call.tool_call_id.clone(), Some(call.clone()))?;
        Ok(())
    }
    fn result(
        &self,
        state: &mut AssistantState,
        result: &FunctionCallResultFrame,
        native_reason: Option<pipecat_core::RealtimeNativeToolSchedulingReason>,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        let Some(Some(call)) = state.pending.get(&result.tool_call_id).cloned() else {
            output.events.push(AggregationEvent::ToolResultIgnored {
                tool_call_id: result.tool_call_id.clone(),
            });
            return Ok(());
        };
        if call.function_name != result.function_name || call.arguments != result.arguments {
            return Err(ProcessorError::new(
                "tool result does not match its running call",
            ));
        }
        let content = if let Some(error) = &result.error {
            json!({"error": error}).to_string()
        } else if result.result.is_null() {
            "COMPLETED".to_owned()
        } else {
            result.result.to_string()
        };
        let mut context = self.context.lock()?;
        let first_new = context.messages.len();
        let addition = if !result.is_final {
            Some(async_message(
                &call.tool_call_id,
                "intermediate",
                Some(&content),
            ))
        } else if is_deferred(&context.messages, &call) {
            Some(async_message(&call.tool_call_id, "final", Some(&content)))
        } else {
            None
        };
        if let (Some(history), Some(addition)) = (&state.signed_history, &addition) {
            history.check_messages(first_new, std::slice::from_ref(addition))?;
        }
        if result.is_final {
            if let Some(addition) = addition {
                context.messages.push(addition);
            } else {
                update_tool_result(&mut context.messages, &call.tool_call_id, &content)?;
            }
            state.pending.remove(&call.tool_call_id);
            state.signed_declarations.remove(&call.tool_call_id);
            Self::mark_completed(state, &call.tool_call_id);
        } else {
            context
                .messages
                .push(addition.expect("intermediate message"));
        }
        if let Some(history) = &mut state.signed_history {
            for index in first_new..context.messages.len() {
                history.message(index, &context.messages[index])?;
            }
        }
        drop(context);
        // Intermediate updates can request a response while their call remains
        // open. Default group coalescing considers only the other siblings.
        let run = result.run_llm.unwrap_or_else(|| {
            (!result.result.is_null() || result.error.is_some())
                && !Self::group_running_except(
                    state,
                    call.group_id.as_deref(),
                    Some(&call.tool_call_id),
                )
        });
        state.tool_run_owed |= run;
        if run && native_reason.is_some() {
            state.pending_native_scheduling = native_reason;
        }
        self.maybe_run(state, output)
    }
    fn cancel_call(&self, state: &mut AssistantState, id: &str) -> Result<bool, ProcessorError> {
        let Some(Some(call)) = state.pending.get(id).cloned() else {
            return Ok(false);
        };
        let mut context = self.context.lock()?;
        let first_new = context.messages.len();
        if call.cancel_on_interruption {
            update_tool_result(&mut context.messages, id, "CANCELLED")?;
        } else {
            let addition = async_message(
                id,
                "cancelled",
                Some("CANCELLED: this tool call was cancelled before it returned a result"),
            );
            if let Some(history) = &state.signed_history {
                history.check_messages(first_new, std::slice::from_ref(&addition))?;
            }
            context.messages.push(addition);
        }
        if let Some(history) = &mut state.signed_history {
            for index in first_new..context.messages.len() {
                history.message(index, &context.messages[index])?;
            }
        }
        state.pending.remove(id);
        state.signed_declarations.remove(id);
        Self::mark_completed(state, id);
        Ok(true)
    }
    fn group_running_except(
        state: &AssistantState,
        group: Option<&str>,
        except: Option<&str>,
    ) -> bool {
        group
            .filter(|group| !group.is_empty())
            .is_some_and(|group| {
                state.pending.values().flatten().any(|call| {
                    call.group_id.as_deref() == Some(group)
                        && Some(call.tool_call_id.as_str()) != except
                })
            })
    }
    fn mark_completed(state: &mut AssistantState, id: &str) {
        state.completed.insert(id);
    }
    fn maybe_run(
        &self,
        state: &mut AssistantState,
        output: &mut AggregationOutput,
    ) -> Result<(), ProcessorError> {
        if !state.active
            && state.pending_playback.is_empty()
            && state
                .signed_history
                .as_ref()
                .is_some_and(|history| history.is_terminal() && !history.playback_pending())
        {
            Self::close_signed_history(state);
        }
        let grouped_pending = state
            .pending
            .values()
            .any(|call| call.as_ref().is_none_or(|call| call.group_id.is_some()));
        // Closed groups need only the coalesced inference bit, including while
        // speech postpones emission. Retained labels are a subset of live calls.
        for group in std::mem::take(&mut state.cancelled_run_groups) {
            if Self::group_running_except(state, Some(&group), None) {
                state.cancelled_run_groups.insert(group);
            } else {
                state.tool_run_owed = true;
            }
        }
        if (state.tool_run_owed || (state.run_owed && !grouped_pending))
            && !state.awaiting_user_inference
            && !state.user_speaking
            && !state.bot_speaking
            && !state.llm_active
            && !state.active
            && state.pending_playback.is_empty()
            && state
                .signed_history
                .as_ref()
                .is_none_or(|history| !history.playback_pending())
        {
            if self.realtime.enabled()? {
                // Seal at the assistant's scheduling point, not when a provider
                // eventually consumes this durable request. Binding happens at
                // emission in the same invocation, after all playback/group gates.
                let result = (|| {
                    let mut frame = Frame::new(self.context.frame_data()?);
                    self.context_causes.attach_assistant_continuation(
                        &mut frame,
                        state.pending_native_scheduling,
                    )?;
                    output.push(frame, Direction::Upstream);
                    Ok::<(), ProcessorError>(())
                })();
                if let Err(error) = result {
                    state.context_cause_failed = true;
                    return Err(ProcessorError::fatal(format!("realtime assistant history retained but inference not delivered: {}; rebuild the pair and provider session", error.message)));
                }
            } else {
                output.context(&self.context, Direction::Upstream)?;
            }
            state.clear_runs();
        }
        Ok(())
    }
}
#[async_trait]
impl FrameProcessor for LLMAssistantAggregator {
    fn name(&self) -> &str {
        "LLMAssistantAggregator"
    }
    async fn process(
        &self,
        frame: Frame,
        direction: Direction,
        ctx: &ProcessorContext,
    ) -> Result<(), ProcessorError> {
        let terminal = matches!(
            frame.data,
            FrameData::End(_)
                | FrameData::EndDetailed(_)
                | FrameData::Stop
                | FrameData::Cancel(_)
                | FrameData::CancelDetailed(_)
        );
        self.realtime.validate_assistant(ctx, &frame.data)?;
        let output =
            self.process_at_inner(frame, direction, self.realtime.origin.elapsed(), Some(ctx))?;
        let emitted: Result<(), ProcessorError> = async {
            if terminal {
                self.cancel_callbacks()?;
            }
            for action in output.actions {
                if let crate::AggregationAction::Push(ref directed) = action {
                    if let FrameData::LlmContextSummaryRequest(request) = &directed.frame.data {
                        if self.summarizer.arm(request, ctx)? {
                            continue;
                        }
                    }
                }
                if let crate::AggregationAction::ContextUpdated(callback) = action {
                    if self.lock()?.callbacks_closed {
                        continue;
                    }
                    let (registered, registration) = tokio::sync::oneshot::channel();
                    let tasks = self.callback_tasks.clone();
                    let id = ctx.spawn("tool-context-updated", false, async move {
                        let Ok(id) = registration.await else {
                            return Ok(());
                        };
                        struct RemoveOnDrop(Arc<Mutex<BTreeMap<u64, ProcessorContext>>>, u64);
                        impl Drop for RemoveOnDrop {
                            fn drop(&mut self) {
                                if let Ok(mut tasks) = self.0.lock() {
                                    tasks.remove(&self.1);
                                }
                            }
                        }
                        let _registration = RemoveOnDrop(tasks, id);
                        callback.call().await
                    })?;
                    {
                        let state = self.lock()?;
                        if state.ended || state.callbacks_closed {
                            ctx.cancel_task(id);
                        } else {
                            self.callback_tasks
                                .lock()
                                .map_err(|_| ProcessorError::fatal("callback task lock poisoned"))?
                                .insert(id, ctx.clone());
                        }
                    }
                    let _ = registered.send(id);
                } else {
                    AggregationOutput {
                        actions: vec![action],
                        events: vec![],
                    }
                    .emit(ctx)
                    .await?;
                }
            }
            Ok(())
        }
        .await;
        let released = if terminal {
            self.realtime.terminal_emitted()
        } else {
            Ok(())
        };
        if let Err(error) = emitted {
            if error.fatal {
                self.lock()?.context_cause_failed = true;
            }
            return Err(error);
        }
        released
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.summarizer.close()?;
        self.cancel_callbacks()
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.summarizer.close()?;
        {
            let mut state = self.lock()?;
            state.transcript_markers.clear();
            state.ended = true;
            state.native_tools.clear();
            state.declared_tools.clear();
            state.signed_declarations.clear();
            state.provisional_tools.clear();
            state.pending.clear();
            state.completed.clear();
            state.interrupted_declarations.clear();
            state.clear_runs();
            state.signed_history = None;
        }
        self.cancel_callbacks()
    }
}
fn update_tool_result(
    messages: &mut [ContextMessage],
    id: &str,
    text: &str,
) -> Result<(), ProcessorError> {
    let Some(message) = messages.iter_mut().find(|message| {
        message.role == ChatRole::Tool && message.tool_call_id.as_deref() == Some(id)
    }) else {
        return Err(ProcessorError::new(
            "tool placeholder is missing from conversation",
        ));
    };
    message.content = MessageContent::Text(text.into());
    Ok(())
}
fn is_deferred(messages: &[ContextMessage], call: &FunctionCallInProgressFrame) -> bool {
    if call.cancel_on_interruption {
        return false;
    }
    let Some(index) = messages.iter().position(|message| {
        message.role == ChatRole::Tool
            && message.tool_call_id.as_deref() == Some(&call.tool_call_id)
    }) else {
        return true;
    };
    messages[index + 1..].iter().any(|message| {
        if matches!(message.role, ChatRole::Tool | ChatRole::Assistant) {
            return false;
        }
        if let MessageContent::Text(text) = &message.content {
            if let Ok(payload) = serde_json::from_str::<Value>(text) {
                if payload["type"] == "async_tool"
                    && payload["tool_call_id"]
                        .as_str()
                        .is_some_and(|id| id != call.tool_call_id)
                {
                    return false;
                }
            }
        }
        true
    })
}
fn async_message(id: &str, kind: &str, result: Option<&str>) -> ContextMessage {
    let description = match kind {
        "started" => "This tool is still running. You will be given its result later. Do not call it again and do not invent a result in the meantime.",
        "intermediate" => "This is a partial result and the task is still running. More may follow. Do not call this tool again and do not treat this as the final answer.",
        "cancelled" => "The asynchronous task associated with this tool_call_id was cancelled before it produced a result, either because it ran past its deadline or because cancellation was requested. No further results will arrive for this tool_call_id. If the user is still waiting on it, tell them it did not complete rather than leaving it unanswered.",
        _ => "This is the final result for the asynchronous task associated with this tool_call_id. The task has completed. No further results will arrive for this tool_call_id. You must convey this result to the user, even if the conversation has moved on. Never leave it unsaid. If the user has said something you have not answered yet, first finish responding to that, then deliver the result at the end of your response; if you have already answered, deliver just the result and do not repeat your earlier reply. How you deliver it depends on its size: if the result is short, simply state it; if it is long or complex, name what has come back and offer the details. Convey it once; do not repeat it in later responses.",
    };
    let mut payload = json!({"type":"async_tool", "status":if kind == "started" || kind == "intermediate" { "running" } else { "finished" }, "tool_call_id": id, "description": description});
    if let Some(result) = result {
        payload["result"] = result.into();
    }
    if kind == "started" {
        tool_message(id, payload.to_string())
    } else {
        text_message(ChatRole::Developer, payload.to_string())
    }
}

#[cfg(test)]
#[path = "review_snapshot_lineage.rs"]
mod review_snapshot_lineage;

#[cfg(test)]
mod realtime_continuation_tests {
    use super::*;
    use crate::{LLMContextAggregatorPair, RealtimeServiceMode};
    use pipecat_core::{RealtimeContextAdmission, RealtimeContextIntent};
    use pipecat_services::Context;

    fn pair(mode: RealtimeServiceMode) -> LLMContextAggregatorPair {
        LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            mode,
        )
        .unwrap()
    }
    #[test]
    fn assistant_continuation_has_independent_sequence_and_preserves_cascade() {
        for mode in [RealtimeServiceMode::Enabled, RealtimeServiceMode::Disabled] {
            let pair = pair(mode);
            let assistant = pair.assistant();
            let mut state = assistant.lock().unwrap();
            let mut cursor = RealtimeContextAdmission::default();
            for sequence in 1..=2 {
                state.tool_run_owed = true;
                let mut output = AggregationOutput::default();
                assistant.maybe_run(&mut state, &mut output).unwrap();
                assert!(!state.tool_run_owed);
                let crate::AggregationAction::Push(context) = &output.actions[0] else {
                    panic!("missing context")
                };
                assert_eq!(context.direction, Direction::Upstream);
                assert!(!context.frame.is_realtime_context_update());
                let cause = context.frame.native_realtime_context_cause().unwrap();
                if mode == RealtimeServiceMode::Enabled {
                    let cause = cause.unwrap();
                    assert_eq!(cause.intent(), RealtimeContextIntent::AssistantContinuation);
                    assert!(cause.run_llm());
                    assert_eq!(cause.sequence(), sequence);
                    assert!(cause.inference_fence().is_none());
                    cause.validate_direction(Direction::Upstream).unwrap();
                    assert!(cause.validate_direction(Direction::Downstream).is_err());
                    assert!(cursor.check(cause).unwrap());
                    cursor.commit(cause.clone());
                    assert!(!cursor.check(cause).unwrap());
                } else {
                    assert!(cause.is_none());
                }
            }
        }
    }
    #[test]
    fn assistant_continuation_seal_failure_retains_history_and_owed_inference() {
        let pair = pair(RealtimeServiceMode::Enabled);
        pair.context()
            .append(Message::text(Role::User, "x".repeat(1024 * 1024)))
            .unwrap();
        let assistant = pair.assistant();
        let mut state = assistant.lock().unwrap();
        state.tool_run_owed = true;
        let mut output = AggregationOutput::default();
        let error = assistant.maybe_run(&mut state, &mut output).unwrap_err();
        assert!(error.fatal);
        assert!(output.actions.is_empty());
        assert!(state.tool_run_owed);
        assert!(state.context_cause_failed);
        drop(state);
        let error = assistant
            .process_at(
                Frame::new(FrameData::LlmResponseEnd),
                Direction::Downstream,
                std::time::Duration::ZERO,
            )
            .unwrap_err();
        assert!(error.fatal);
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
        let mut probe = Frame::new(FrameData::LlmContext(Default::default()));
        assistant
            .context_causes
            .attach(
                &mut probe,
                RealtimeContextIntent::AssistantContinuation,
                true,
            )
            .unwrap();
        assert_eq!(
            probe
                .native_realtime_context_cause()
                .unwrap()
                .unwrap()
                .sequence(),
            1
        );
    }
}
