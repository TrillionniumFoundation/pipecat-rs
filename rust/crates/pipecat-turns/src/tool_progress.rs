//! Selected native tool history. This is deliberately separate from the
//! transcript-prefix receipt protocol: only the real assistant acknowledges it.
use crate::context::ContextState;
use pipecat_core::{
    ChatRole, ContextMessage, Frame, FrameData, FunctionCallInProgressFrame,
    NativeToolProgressRoute, ProcessorError,
};
use std::collections::BTreeMap;

pub(crate) const MAX_MESSAGES: usize = 1024;
pub(crate) const MAX_HISTORY_BYTES: usize = 1024 * 1024;

pub(crate) struct History {
    route: NativeToolProgressRoute,
    lineage: u64,
    _metadata: pipecat_core::NativeToolProgressMetadataCharge,
    // Compact private positional attribution. A public JSON string never adds
    // an entry here, even when it has exactly the canonical envelope shape.
    digests: Vec<[u8; 32]>,
    owned: BTreeMap<usize, usize>,
    revision: u64,
}
impl std::fmt::Debug for History {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ToolProgressHistory")
            .field("lineage", &self.lineage)
            .field("messages", &self.digests.len())
            .field("revision", &self.revision)
            .finish()
    }
}
impl History {
    pub(crate) fn new(
        route: NativeToolProgressRoute,
        context: &ContextState,
    ) -> Result<Self, ProcessorError> {
        let metadata = route.reserve_metadata(64 * 1024)?;
        let mut history = Self {
            route,
            lineage: context.lineage,
            _metadata: metadata,
            digests: Vec::new(),
            owned: BTreeMap::new(),
            revision: 0,
        };
        history.check_size(&context.messages, &[])?;
        history.check_retained(context, &[])?;
        history.record(&context.messages)?;
        Ok(history)
    }
    pub(crate) fn validate(&self, context: &ContextState) -> Result<(), ProcessorError> {
        if self.lineage != context.lineage || self.digests.len() != context.messages.len() {
            return Err(ProcessorError::fatal(
                "selected tool history lineage or message range changed",
            ));
        }
        self.check_size(&context.messages, &[])?;
        self.check_retained(context, &[])?;
        for (index, digest) in self.digests.iter().enumerate() {
            if pipecat_core::NativeToolProgressHistoryCommit::new(
                1,
                index,
                index + 1,
                &context.messages,
            )?
            .digest
                != *digest
            {
                return Err(ProcessorError::fatal(
                    "selected tool history was modified outside its consumer",
                ));
            }
        }
        Ok(())
    }
    pub(crate) fn check_retained(
        &self,
        context: &ContextState,
        additions: &[ContextMessage],
    ) -> Result<(), ProcessorError> {
        let mut bytes = context
            .tool_progress_retained_bytes()?
            .saturating_add(context.tool_progress_growth_bytes(additions.len()));
        for message in additions {
            bytes = bytes.saturating_add(bounded_message(message)?);
        }
        if bytes > self.route.history_max_bytes().min(MAX_HISTORY_BYTES) {
            return Err(ProcessorError::fatal(
                "selected actual history buffers exceed byte budget",
            ));
        }
        Ok(())
    }
    pub(crate) fn max_bytes(&self) -> usize {
        self.route.history_max_bytes().min(MAX_HISTORY_BYTES)
    }
    pub(crate) fn check_size(
        &self,
        messages: &[ContextMessage],
        additions: &[ContextMessage],
    ) -> Result<(), ProcessorError> {
        self.check_messages(messages.iter().chain(additions))
    }
    pub(crate) fn check_replacement(
        &self,
        messages: &[ContextMessage],
        index: usize,
        replacement: &ContextMessage,
    ) -> Result<(), ProcessorError> {
        self.check_messages(messages.iter().enumerate().map(|(at, old)| {
            if at == index {
                replacement
            } else {
                old
            }
        }))
    }
    fn check_messages<'a>(
        &self,
        messages: impl Iterator<Item = &'a ContextMessage>,
    ) -> Result<(), ProcessorError> {
        let limit = self.route.history_max_bytes().min(MAX_HISTORY_BYTES);
        let mut counter = crate::tool_history::ByteCounter { bytes: 2, limit };
        let mut retained = 0usize;
        for (count, message) in messages.enumerate() {
            if count >= MAX_MESSAGES {
                return Err(ProcessorError::fatal(
                    "selected tool history exceeds 1024 messages",
                ));
            }
            retained = retained
                .checked_add(bounded_message(message)?.saturating_mul(3))
                .filter(|bytes| *bytes <= limit)
                .ok_or_else(|| {
                    ProcessorError::fatal(
                        "selected tool history retained buffers exceed byte budget",
                    )
                })?;
            serde_json::to_writer(&mut counter, message).map_err(|_| {
                ProcessorError::fatal("selected tool history exceeds its encoded byte budget")
            })?;
            std::io::Write::write_all(&mut counter, b",").map_err(|_| {
                ProcessorError::fatal("selected tool history exceeds its encoded byte budget")
            })?;
        }
        Ok(())
    }
    pub(crate) fn record(&mut self, messages: &[ContextMessage]) -> Result<(), ProcessorError> {
        self.digests = (0..messages.len())
            .map(|index| {
                pipecat_core::NativeToolProgressHistoryCommit::new(1, index, index + 1, messages)
                    .map(|commit| commit.digest)
            })
            .collect::<Result<_, _>>()?;
        Ok(())
    }
    pub(crate) fn check_revisions(&self, count: usize) -> Result<(), ProcessorError> {
        self.revision
            .checked_add(
                u64::try_from(count)
                    .map_err(|_| ProcessorError::fatal("history revision overflow"))?,
            )
            .ok_or_else(|| ProcessorError::fatal("selected history revision exhausted"))?;
        Ok(())
    }
    pub(crate) fn attribute(
        &mut self,
        start: usize,
        end: usize,
        id: &str,
        messages: &[ContextMessage],
    ) -> Result<u64, ProcessorError> {
        if start >= end || end > self.digests.len() {
            return Err(ProcessorError::fatal(
                "invalid selected history attribution range",
            ));
        }
        let revision = self
            .revision
            .checked_add(1)
            .ok_or_else(|| ProcessorError::fatal("selected history revision exhausted"))?;
        let owner = messages
            .iter()
            .position(|message| {
                message.role == ChatRole::Tool && message.tool_call_id.as_deref() == Some(id)
            })
            .ok_or_else(|| {
                ProcessorError::fatal("selected attribution has no canonical tool placeholder")
            })?;
        for index in start..end {
            self.owned.insert(index, owner);
        }
        self.revision = revision;
        Ok(revision)
    }
    pub(crate) fn is_deferred(
        &self,
        messages: &[ContextMessage],
        call: &FunctionCallInProgressFrame,
    ) -> bool {
        if call.cancel_on_interruption {
            return false;
        }
        let Some(index) = messages.iter().position(|message| {
            message.role == ChatRole::Tool
                && message.tool_call_id.as_deref() == Some(call.tool_call_id.as_str())
        }) else {
            return true;
        };
        messages
            .iter()
            .enumerate()
            .skip(index + 1)
            .any(|(index, message)| {
                if matches!(message.role, ChatRole::Assistant | ChatRole::Tool) {
                    return false;
                }
                !self
                    .owned
                    .get(&index)
                    .and_then(|owner| messages.get(*owner))
                    .and_then(|message| message.tool_call_id.as_deref())
                    .is_some_and(|id| id != call.tool_call_id)
            })
    }
}

// No recursive walk and no attacker-sized traversal stack. The total visited
// count and value depth are checked before any serde recursion or Value clone.
pub(crate) fn bounded_value(value: &serde_json::Value) -> Result<usize, ProcessorError> {
    let mut pending = vec![(value, 0usize)];
    let mut nodes = 0usize;
    let mut bytes = 0usize;
    while let Some((value, depth)) = pending.pop() {
        nodes += 1;
        if nodes > 4096 || depth > 32 {
            return Err(ProcessorError::fatal(
                "selected history JSON exceeds depth or node bound",
            ));
        }
        match value {
            serde_json::Value::String(value) => bytes = bytes.saturating_add(value.capacity()),
            serde_json::Value::Array(values) => {
                bytes = bytes.saturating_add(
                    values
                        .capacity()
                        .saturating_mul(std::mem::size_of::<serde_json::Value>()),
                );
                if pending.len().saturating_add(values.len()) > 4096 {
                    return Err(ProcessorError::fatal(
                        "selected history JSON exceeds node bound",
                    ));
                }
                pending.extend(values.iter().map(|value| (value, depth + 1)));
            }
            serde_json::Value::Object(values) => {
                bytes = bytes.saturating_add(
                    values
                        .len()
                        .saturating_mul(std::mem::size_of::<(String, serde_json::Value)>()),
                );
                if pending.len().saturating_add(values.len()) > 4096 {
                    return Err(ProcessorError::fatal(
                        "selected history JSON exceeds node bound",
                    ));
                }
                for (key, value) in values {
                    bytes = bytes.saturating_add(key.capacity());
                    pending.push((value, depth + 1));
                }
            }
            _ => {}
        }
        if bytes > MAX_HISTORY_BYTES {
            return Err(ProcessorError::fatal(
                "selected history JSON retained buffers exceed bound",
            ));
        }
    }
    Ok(bytes)
}
pub(crate) fn bounded_message(message: &ContextMessage) -> Result<usize, ProcessorError> {
    let pipecat_core::MessageContent::Text(text) = &message.content else {
        return Err(ProcessorError::fatal(
            "selected tool history requires textual messages",
        ));
    };
    if text.capacity() > MAX_HISTORY_BYTES
        || message.tool_calls.len() > 1024
        || message.extra.len() > 64
    {
        return Err(ProcessorError::fatal(
            "selected tool history retained message exceeds bound",
        ));
    }
    let mut bytes = std::mem::size_of::<ContextMessage>()
        .saturating_add(text.capacity())
        .saturating_add(message.name.as_ref().map_or(0, String::capacity))
        .saturating_add(message.tool_call_id.as_ref().map_or(0, String::capacity))
        .saturating_add(
            message
                .tool_calls
                .capacity()
                .saturating_mul(std::mem::size_of::<pipecat_core::FunctionCall>()),
        );
    for call in &message.tool_calls {
        bytes = bytes
            .saturating_add(call.function_name.capacity())
            .saturating_add(call.tool_call_id.capacity())
            .saturating_add(bounded_value(&call.arguments)?);
    }
    for (key, value) in &message.extra {
        bytes = bytes
            .saturating_add(std::mem::size_of::<(String, serde_json::Value)>())
            .saturating_add(key.capacity())
            .saturating_add(bounded_value(value)?);
    }
    Ok(bytes)
}
pub(crate) fn bounded_native_message(
    message: &pipecat_services::Message,
) -> Result<usize, ProcessorError> {
    let content = match &message.content {
        None => 0,
        Some(pipecat_services::Content::Text(text)) => text.capacity(),
        Some(pipecat_services::Content::Parts(_)) => {
            return Err(ProcessorError::fatal(
                "selected native history must be textual and unsigned",
            ))
        }
    };
    let mut bytes = std::mem::size_of::<pipecat_services::Message>()
        .saturating_add(content)
        .saturating_add(message.name.as_ref().map_or(0, String::capacity))
        .saturating_add(message.tool_call_id.as_ref().map_or(0, String::capacity))
        .saturating_add(
            message
                .tool_calls
                .capacity()
                .saturating_mul(std::mem::size_of::<pipecat_services::ToolCall>()),
        );
    for call in &message.tool_calls {
        bytes = bytes
            .saturating_add(call.id.capacity())
            .saturating_add(call.name.capacity())
            .saturating_add(bounded_value(&call.arguments)?);
    }
    if bytes > MAX_HISTORY_BYTES {
        return Err(ProcessorError::fatal(
            "selected native history retained buffers exceed bound",
        ));
    }
    Ok(bytes)
}
pub(crate) fn validate_legacy_messages(
    messages: &[pipecat_core::ChatMessage],
) -> Result<(), ProcessorError> {
    if messages.len() > MAX_MESSAGES {
        return Err(ProcessorError::fatal(
            "selected append exceeds message bound",
        ));
    }
    let mut retained = 0usize;
    for message in messages {
        retained = retained
            .saturating_add(message.content.capacity())
            .saturating_add(message.name.as_ref().map_or(0, String::capacity))
            .saturating_add(message.tool_call_id.as_ref().map_or(0, String::capacity));
        if message.role == ChatRole::Tool
            || !message.tool_calls.is_empty()
            || message.tool_call_id.is_some()
        {
            return Err(ProcessorError::fatal(
                "ordinary selected appends cannot supply tool ownership",
            ));
        }
        if retained > MAX_HISTORY_BYTES {
            return Err(ProcessorError::fatal(
                "selected append exceeds retained byte budget",
            ));
        }
    }
    Ok(())
}

pub(crate) fn validate_control(frame: &Frame) -> Result<(), ProcessorError> {
    // Validate all private authority before any public variant can mutate user
    // history. In particular a progress frame changed to Append must fail here.
    frame.native_tool_batch()?;
    frame.native_tool_outcome()?;
    frame.native_tool_progress_admission()?;
    frame.native_tool_progress()?;
    match &frame.data {
        FrameData::LlmMessagesAppend(messages) => validate_legacy_messages(&messages.messages)?,
        FrameData::LlmMessagesAppendDetailed(messages) => {
            if messages.messages.len() > MAX_MESSAGES {
                return Err(ProcessorError::fatal(
                    "selected rich append exceeds message bound",
                ));
            }
            let mut bytes = 0usize;
            for message in &messages.messages {
                if message.role == ChatRole::Tool
                    || !message.tool_calls.is_empty()
                    || message.tool_call_id.is_some()
                {
                    return Err(ProcessorError::fatal(
                        "ordinary selected appends cannot supply tool ownership",
                    ));
                }
                bytes = bytes.saturating_add(bounded_message(message)?);
                if bytes > MAX_HISTORY_BYTES {
                    return Err(ProcessorError::fatal(
                        "selected append exceeds retained byte budget",
                    ));
                }
            }
        }
        _ => {}
    }
    if frame.native_speculation().is_some()
        || frame.native_eager_source().is_some()
        || frame
            .native_llm_continuation()
            .map_err(|error| ProcessorError::fatal(error.to_string()))?
            .is_some()
        || matches!(
            &frame.data,
            FrameData::LlmMessagesUpdate(_)
                | FrameData::LlmMessagesUpdateDetailed(_)
                | FrameData::LlmMessagesTransform(_)
                | FrameData::LlmSummarizeContext(_)
                | FrameData::LlmContextSummaryRequest(_)
                | FrameData::LlmContextSummaryResult(_)
                | FrameData::LlmContinuationCheckpoint
                | FrameData::EagerTranscription(_)
                | FrameData::EagerTranscriptionDetailed(_)
                | FrameData::AssistantImage(_)
                | FrameData::UserImage(_)
                | FrameData::UserFile(_)
                | FrameData::UserFileDetailed(_)
        )
        || matches!(&frame.data, FrameData::LlmContextDetailed(context) if context.speculation)
    {
        return Err(ProcessorError::fatal("selected tool progress excludes replacement, summary, speculation, signed and multimodal history"));
    }
    Ok(())
}

#[derive(Default)]
pub(crate) struct Consumer {
    pub(crate) leases: BTreeMap<u64, LeaseHistory>,
    pub(crate) starts: BTreeMap<String, pipecat_core::NativeToolProgressHistoryCommit>,
    pub(crate) effects: Vec<Effect>,
    pub(crate) terminal_commits: BTreeMap<String, pipecat_core::NativeToolProgressHistoryCommit>,
    pub(crate) metadata: Vec<pipecat_core::NativeToolProgressMetadataCharge>,
}
pub(crate) struct LeaseHistory {
    pub(crate) admission: Option<pipecat_core::NativeToolProgressAdmission>,
    pub(crate) route_id: u64,
    pub(crate) through: u64,
    pub(crate) committed: BTreeMap<u64, (u64, pipecat_core::NativeToolProgressHistoryCommit)>,
    pub(crate) pending: BTreeMap<u64, std::sync::Arc<pipecat_core::NativeToolProgress>>,
    pub(crate) terminal: Option<(
        FrameData,
        std::sync::Arc<pipecat_core::NativeToolCallOutcome>,
    )>,
    pub(crate) terminal_id: Option<u64>,
}
pub(crate) enum Effect {
    Progress(pipecat_core::NativeToolProgressHistoryNotification),
    Context(pipecat_core::NativeToolProgressContextNotification),
}

impl Consumer {
    pub(crate) fn register(
        &mut self,
        admission: &pipecat_core::NativeToolProgressAdmission,
        route: &NativeToolProgressRoute,
    ) -> Result<(), ProcessorError> {
        if let Some(old) = self.leases.get(&admission.lease_id()) {
            if old.route_id != admission.route_id()
                || old.admission.as_ref().is_some_and(|old| {
                    old.call() != admission.call()
                        || old.batch() != admission.batch()
                        || old.reporting() != admission.reporting()
                })
            {
                return Err(ProcessorError::fatal(
                    "selected tool lease changed identity",
                ));
            }
            return Ok(());
        }
        if self.leases.len() >= 1024 {
            return Err(ProcessorError::fatal(
                "selected tool consumer lease metadata capacity exceeded",
            ));
        }
        if self.leases.values().any(|old| {
            old.admission
                .as_ref()
                .is_some_and(|old| old.call().tool_call_id == admission.call().tool_call_id)
        }) {
            return Err(ProcessorError::fatal(
                "selected call was assigned a second lease",
            ));
        }
        let charge = route.reserve_metadata(
            std::mem::size_of::<LeaseHistory>() + std::mem::size_of::<u64>() * 4,
        )?;
        self.metadata.push(charge);
        self.leases.insert(
            admission.lease_id(),
            LeaseHistory {
                admission: Some(admission.clone()),
                route_id: admission.route_id(),
                through: 0,
                committed: BTreeMap::new(),
                pending: BTreeMap::new(),
                terminal: None,
                terminal_id: None,
            },
        );
        Ok(())
    }
    pub(crate) fn staged_records(&self) -> usize {
        self.leases.values().map(|lease| lease.pending.len()).sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{LLMContextAggregatorPair, RealtimeServiceMode, SharedContext};
    use pipecat_core::{Direction, MessageContent};
    use pipecat_services::{Context, Message, Role};
    use std::time::Duration;

    fn route() -> NativeToolProgressRoute {
        NativeToolProgressRoute::new(Default::default()).unwrap()
    }
    fn pair() -> LLMContextAggregatorPair {
        LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            Default::default(),
            Default::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap()
    }
    fn call(id: &str) -> FunctionCallInProgressFrame {
        FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: id.into(),
            arguments: serde_json::json!({}),
            cancel_on_interruption: false,
            group_id: Some("batch".into()),
        }
    }
    fn message(role: ChatRole, text: impl Into<String>) -> ContextMessage {
        crate::context::text_message(role, text)
    }

    #[test]
    fn selected_binding_requires_enabled_unused_pair_and_isolates_user_receipts() {
        let automatic = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        assert!(automatic.bind_tool_progress(route()).is_err());
        let pair = pair();
        pair.bind_tool_progress(route()).unwrap();
        assert!(pair.bind_tool_progress(route()).is_err());
        assert!(pair.reserve_realtime_user_history().is_err());
        let other = self::pair();
        let _endpoint = other.reserve_realtime_user_history().unwrap();
        assert!(other.bind_tool_progress(route()).is_err());
    }

    #[test]
    fn binding_preflights_initial_message_budget_without_changing_history() {
        let context = Context {
            messages: (0..=MAX_MESSAGES)
                .map(|_| Message::text(Role::User, "x"))
                .collect(),
            ..Default::default()
        };
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            context,
            Default::default(),
            Default::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap();
        assert!(pair.bind_tool_progress(route()).is_err());
        assert_eq!(
            pair.context().snapshot().unwrap().messages.len(),
            MAX_MESSAGES + 1
        );
        assert!(pair.context().lock().unwrap().tool_progress.is_none());
    }

    #[test]
    fn no_run_append_budget_is_atomic_and_counts_retained_capacity() {
        let pair = pair();
        pair.bind_tool_progress(route()).unwrap();
        pair.context()
            .append(Message::text(Role::User, "keep"))
            .unwrap();
        let mut oversized = String::with_capacity(MAX_HISTORY_BYTES / 2);
        oversized.push('x');
        assert!(pair
            .context()
            .append_rich(message(ChatRole::User, oversized))
            .is_err());
        assert_eq!(
            pair.context().snapshot().unwrap().messages,
            vec![Message::text(Role::User, "keep")]
        );
        pair.context()
            .append(Message::text(Role::User, "valid"))
            .unwrap();
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), 2);
    }

    #[test]
    fn retained_context_replacement_is_detected_at_next_actual_boundary() {
        let pair = pair();
        pair.bind_tool_progress(route()).unwrap();
        assert!(pair.context().replace(Context::default()).is_err());
        assert!(pair
            .context()
            .replace_rich(pipecat_core::LlmContextFrame::default().into())
            .is_err());
        // A private in-place lineage edit still fails the independent binding
        // identity check even though public replacement is rejected atomically.
        pair.context().lock().unwrap().lineage += 1;
        assert!(pair
            .user()
            .process_at(
                Frame::new(FrameData::LlmRun),
                Direction::Downstream,
                Duration::ZERO
            )
            .is_err());
        assert!(pair
            .assistant()
            .process_frame(Frame::new(FrameData::LlmRun), Direction::Downstream)
            .is_err());
    }

    #[test]
    fn ordinary_json_sibling_text_does_not_gain_history_attribution() {
        let context = SharedContext::default();
        let mut state = context.lock().unwrap();
        state
            .messages
            .push(crate::context::tool_message("own", "started"));
        state.messages.push(message(
            ChatRole::Developer,
            r#"{"type":"async_tool","tool_call_id":"sibling"}"#,
        ));
        let mut history = History::new(route(), &state).unwrap();
        assert!(history.is_deferred(&state.messages, &call("own")));
        state
            .messages
            .push(crate::context::tool_message("sibling", "started"));
        history.record(&state.messages).unwrap();
        history.attribute(1, 2, "sibling", &state.messages).unwrap();
        assert!(!history.is_deferred(&state.messages, &call("own")));
        state.messages.push(message(
            ChatRole::User,
            r#"{"type":"async_tool","tool_call_id":"sibling"}"#,
        ));
        history.record(&state.messages).unwrap();
        assert!(history.is_deferred(&state.messages, &call("own")));
        state.messages.pop();
        history.owned.insert(1, 0);
        assert!(history.is_deferred(&state.messages, &call("own")));
    }

    #[test]
    fn private_history_detects_in_place_mutation_and_revision_exhaustion() {
        let context = SharedContext::default();
        let mut state = context.lock().unwrap();
        state.messages.push(message(ChatRole::User, "original"));
        let mut history = History::new(route(), &state).unwrap();
        history.validate(&state).unwrap();
        state.messages[0].content = MessageContent::Text("modified".into());
        assert!(history.validate(&state).is_err());
        history.revision = u64::MAX;
        assert!(history.check_revisions(1).is_err());
    }

    #[test]
    fn selected_control_rejects_replacement_before_legacy_mutation() {
        let pair = pair();
        pair.bind_tool_progress(route()).unwrap();
        pair.context()
            .append(Message::text(Role::User, "keep"))
            .unwrap();
        let frame = Frame::new(FrameData::LlmMessagesUpdate(pipecat_core::MessagesFrame {
            messages: vec![],
            run_llm: false,
        }));
        assert!(pair
            .user()
            .process_at(frame.clone(), Direction::Downstream, Duration::ZERO)
            .is_err());
        assert!(pair
            .assistant()
            .process_frame(frame, Direction::Downstream)
            .is_err());
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    }

    #[test]
    fn selected_binding_rejects_used_pair_and_automatic_summarization() {
        let pair = pair();
        pair.user()
            .process_at(
                Frame::new(FrameData::LlmRun),
                Direction::Downstream,
                Duration::ZERO,
            )
            .unwrap();
        assert!(pair.bind_tool_progress(route()).is_err());
        let assistant =
            crate::LLMAssistantAggregator::new(SharedContext::default(), Default::default())
                .with_summarization(crate::ContextSummarizationConfig {
                    auto_trigger: true,
                    ..Default::default()
                })
                .unwrap();
        assert!(assistant.validate_history_configuration().is_err());
        let user = crate::UserAggregatorConfig {
            eager: Some(Default::default()),
            ..Default::default()
        };
        assert!(LLMContextAggregatorPair::with_realtime_mode(
            Context::default(),
            user,
            Default::default(),
            RealtimeServiceMode::Enabled
        )
        .is_err());
    }

    #[test]
    fn full_history_allows_exact_placeholder_replacement_without_an_append_slot() {
        let context = SharedContext::default();
        let mut state = context.lock().unwrap();
        state.messages = (0..MAX_MESSAGES - 1)
            .map(|_| message(ChatRole::User, "x"))
            .collect();
        state
            .messages
            .push(crate::context::tool_message("own", "IN_PROGRESS"));
        state.tool_progress = Some(History::new(route(), &state).unwrap());
        state
            .preflight_tool_replacement(
                MAX_MESSAGES - 1,
                &crate::context::tool_message("own", "done"),
            )
            .unwrap();
        assert!(state
            .preflight_tool_progress(&[message(ChatRole::Developer, "extra")])
            .is_err());
        assert_eq!(state.messages.len(), MAX_MESSAGES);
    }

    #[test]
    fn initial_native_retained_capacities_are_checked_before_projection_clones() {
        let mut content = String::with_capacity(MAX_HISTORY_BYTES);
        content.push('x');
        let pair = LLMContextAggregatorPair::with_realtime_mode(
            Context {
                messages: vec![Message::text(Role::User, content)],
                ..Default::default()
            },
            Default::default(),
            Default::default(),
            RealtimeServiceMode::Enabled,
        )
        .unwrap();
        assert!(pair.bind_tool_progress(route()).is_err());
        assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    }

    #[test]
    fn bounded_json_walk_rejects_depth_without_recursive_cloning() {
        let mut value = serde_json::Value::Null;
        for _ in 0..34 {
            value = serde_json::Value::Array(vec![value]);
        }
        assert!(bounded_value(&value).is_err());
    }
}
