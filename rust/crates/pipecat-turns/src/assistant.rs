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
    pending: BTreeMap<String, Option<FunctionCallInProgressFrame>>,
    completed: VecDeque<String>,
    interrupted_declarations: VecDeque<String>,
    seen: RecentFrames,
    context_ids: Vec<String>,
    pending_playback: BTreeSet<String>,
    cancelled_contexts: VecDeque<String>,
    run_owed: bool,
    tool_run_owed: bool,
    cancelled_run_groups: BTreeSet<String>,
    skip_tts: Option<bool>,
    thought: TextBuffer,
    thought_llm: Option<String>,
    thought_append: bool,
    signed_history: Option<crate::signed_history::SignedHistory>,
    completed_responses: VecDeque<u64>,
    ended: bool,
    callbacks_closed: bool,
}
impl AssistantState {
    fn clear_runs(&mut self) {
        self.run_owed = false;
        self.tool_run_owed = false;
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
            state: Mutex::new(AssistantState {
                active: false,
                llm_active: false,
                generated_speech: false,
                user_speaking: false,
                awaiting_user_inference: false,
                bot_speaking: false,
                aggregation: TextBuffer::default(),
                transcript_markers: Default::default(),
                pending: BTreeMap::new(),
                completed: VecDeque::new(),
                interrupted_declarations: VecDeque::new(),
                seen: RecentFrames::default(),
                context_ids: vec![],
                pending_playback: BTreeSet::new(),
                cancelled_contexts: VecDeque::new(),
                run_owed: false,
                tool_run_owed: false,
                cancelled_run_groups: BTreeSet::new(),
                skip_tts: None,
                thought: TextBuffer::default(),
                thought_llm: None,
                thought_append: false,
                signed_history: None,
                completed_responses: VecDeque::new(),
                ended: false,
                callbacks_closed: false,
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
            }
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
                    .filter(|call| call.cancel_on_interruption)
                    .cloned()
                    .collect();
                for call in cancel {
                    self.cancel_call(&mut state, &call.tool_call_id)?;
                }
                let declarations: Vec<_> = state
                    .pending
                    .iter()
                    .filter(|(_, call)| call.is_none())
                    .map(|(id, _)| id.clone())
                    .collect();
                for id in declarations {
                    state.interrupted_declarations.push_back(id);
                    if state.interrupted_declarations.len() > 512 {
                        state.interrupted_declarations.pop_front();
                    }
                }
                state.pending.retain(|_, call| call.is_some());
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
                    if !state.completed.contains(&call.tool_call_id)
                        && !state.pending.contains_key(&call.tool_call_id)
                    {
                        // This system frame can overtake the normal response-start marker.
                        state.llm_active = true;
                        state.pending.insert(call.tool_call_id.clone(), None);
                    }
                }
            }
            FrameData::FunctionCallsStartedDetailed(calls) => {
                for call in calls {
                    if !state.completed.contains(&call.tool_call_id) && !state.pending.contains_key(&call.tool_call_id) {
                        state.llm_active = true;
                        state.pending.insert(call.tool_call_id.clone(), None);
                    }
                }
            }
            FrameData::FunctionCallInProgress(call) => self.start_call(&mut state, call)?,
            FrameData::FunctionCallResult(result) => {
                self.result(&mut state, result, &mut output)?
            }
            FrameData::FunctionCallResultDetailed(result) => {
                let accepted = state.pending.get(&result.tool_call_id).is_some_and(Option::is_some);
                let properties = result.properties.as_ref();
                self.result(&mut state, &FunctionCallResultFrame {
                    function_name: result.function_name.clone(), tool_call_id: result.tool_call_id.clone(),
                    arguments: result.arguments.clone(), result: result.result.clone(),
                    run_llm: properties.and_then(|properties| properties.run_llm).or(result.run_llm),
                    is_final: properties.is_none_or(|properties| properties.is_final), error: result.error.clone(),
                }, &mut output)?;
                if accepted {
                    if let Some(callback) = properties.and_then(|properties| properties.on_context_updated.clone()) {
                        output.actions.push(crate::AggregationAction::ContextUpdated(callback));
                    }
                }
            }
            FrameData::FunctionCallCancel(cancel) => {
                let group = state.pending.get(&cancel.tool_call_id)
                    .and_then(Option::as_ref)
                    .and_then(|call| call.group_id.clone());
                let declared = state
                    .pending
                    .get(&cancel.tool_call_id)
                    .is_some_and(Option::is_none);
                let existed = if declared {
                    state.pending.remove(&cancel.tool_call_id);
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
        let mut state = self.lock()?;
        let context = self.context.frame_data()?;
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
        }
        state.aggregation.take();
        if !text.is_empty() {
            context.append_native(Message::text(Role::Assistant, text.clone()));
            if let Some(history) = &mut state.signed_history {
                let index = context.messages.len() - 1;
                history.text(index, context.messages[index].clone(), text.clone());
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
        if state.completed.contains(&call.tool_call_id) {
            return Ok(());
        }
        if let Some(index) = state
            .interrupted_declarations
            .iter()
            .position(|id| id == &call.tool_call_id)
        {
            state.interrupted_declarations.remove(index);
            if call.cancel_on_interruption {
                Self::mark_completed(state, &call.tool_call_id);
                return Ok(());
            }
        }
        if let Some(Some(old)) = state.pending.get(&call.tool_call_id) {
            if old != call {
                return Err(ProcessorError::new("conflicting duplicate tool call id"));
            }
            return Ok(());
        }
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
            && state
                .signed_history
                .as_ref()
                .is_some_and(|history| history.owns_call(&canonical));
        if !retained && existing.is_some() {
            return Err(ProcessorError::new(
                "tool call id already exists in conversation",
            ));
        }
        if !retained {
            context.append_native(Message::assistant_tool_calls(vec![ToolCall {
                id: call.tool_call_id.clone(),
                name: call.function_name.clone(),
                arguments: call.arguments.clone(),
            }]));
            if let Some(history) = &mut state.signed_history {
                let index = context.messages.len() - 1;
                history.message(index, context.messages[index].clone());
            }
        }
        context.messages.push(if call.cancel_on_interruption {
            tool_message(&call.tool_call_id, "IN_PROGRESS")
        } else {
            async_message(&call.tool_call_id, "started", None)
        });
        if let Some(history) = &mut state.signed_history {
            let index = context.messages.len() - 1;
            history.message(index, context.messages[index].clone());
        }
        state
            .pending
            .insert(call.tool_call_id.clone(), Some(call.clone()));
        Ok(())
    }
    fn result(
        &self,
        state: &mut AssistantState,
        result: &FunctionCallResultFrame,
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
        if result.is_final {
            if is_deferred(&context.messages, &call) {
                context
                    .messages
                    .push(async_message(&call.tool_call_id, "final", Some(&content)));
            } else {
                update_tool_result(&mut context.messages, &call.tool_call_id, &content)?;
            }
            state.pending.remove(&call.tool_call_id);
            Self::mark_completed(state, &call.tool_call_id);
        } else {
            context.messages.push(async_message(
                &call.tool_call_id,
                "intermediate",
                Some(&content),
            ));
        }
        if let Some(history) = &mut state.signed_history {
            for index in first_new..context.messages.len() {
                history.message(index, context.messages[index].clone());
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
            context.messages.push(async_message(
                id,
                "cancelled",
                Some("CANCELLED: this tool call was cancelled before it returned a result"),
            ));
        }
        if let Some(history) = &mut state.signed_history {
            for index in first_new..context.messages.len() {
                history.message(index, context.messages[index].clone());
            }
        }
        state.pending.remove(id);
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
        state.completed.push_back(id.to_owned());
        if state.completed.len() > 512 {
            state.completed.pop_front();
        }
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
        let cancelled_group_ready = state
            .cancelled_run_groups
            .iter()
            .any(|group| !Self::group_running_except(state, Some(group), None));
        if (state.tool_run_owed || cancelled_group_ready || (state.run_owed && !grouped_pending))
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
            output.context(&self.context, Direction::Upstream)?;
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
        emitted?;
        released
    }
    async fn prepare_end(&self, _ctx: &ProcessorContext) -> Result<(), ProcessorError> {
        self.summarizer.close()?;
        self.cancel_callbacks()
    }
    async fn cleanup(&self) -> Result<(), ProcessorError> {
        self.summarizer.close()?;
        self.lock()?.transcript_markers.clear();
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
