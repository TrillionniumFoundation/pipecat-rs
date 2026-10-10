//! Bounded, append-only reconciliation of local snapshots with server-owned items.
use crate::*;
use pipecat_core::{
    ChatMessage, ChatRole, ContextMessage, FunctionCall, InvocationFence, LlmContextData,
    MessageContent, MessagePart, ProcessorError, ToolSet,
};
use serde_json::Value;
use std::collections::{BTreeMap, VecDeque};

const MAX_MESSAGES: usize = 1024;
#[derive(Clone)]
pub(crate) struct HeldContinuation {
    pub fence: InvocationFence,
    pub result_revision: u64,
    pub reevaluation_queued: bool,
}
#[derive(Clone, Default)]
pub(crate) struct ContextState {
    pub initialized: bool,
    pub history: Vec<ChatMessage>,
    pub observed: bool,
    user_text: VecDeque<String>,
    assistant_text: BTreeMap<String, String>,
    calls: BTreeMap<String, FunctionCall>,
    results: BTreeMap<String, String>,
    pub session: Option<SessionProperties>,
    pub instructions_override: Option<String>,
    pub tools_controlled: bool,
    pub choice_controlled: bool,
    pub continuation_owed: Option<HeldContinuation>,
    outcome_revisions: BTreeMap<u64, u64>,
    pub result_revision: u64,
    pub continued_revision: u64,
}
impl ContextState {
    pub fn explicit_input(
        &mut self,
        text: &str,
        limit: usize,
    ) -> Result<Vec<Action>, ProcessorError> {
        self.explicit_append(vec![ChatMessage::new(ChatRole::User, text)], limit)
    }
    pub fn explicit_append(
        &mut self,
        messages: Vec<ChatMessage>,
        limit: usize,
    ) -> Result<Vec<Action>, ProcessorError> {
        if self.history.len().saturating_add(messages.len()) > MAX_MESSAGES
            || crate::client::bounded_serialized(&messages, limit).is_err()
        {
            return Err(ProcessorError::new("realtime append exceeds limit"));
        }
        let mut actions = vec![];
        for message in &messages {
            if message.role == ChatRole::Tool || !message.tool_calls.is_empty() {
                return Err(ProcessorError::new(
                    "live tool history requires native terminal receipts",
                ));
            }
            actions.extend(message_items(message)?);
        }
        self.history.extend(messages);
        self.initialized = true;
        self.bound(limit)?;
        Ok(actions)
    }
    pub fn with_instructions(instructions_override: Option<String>) -> Self {
        Self {
            instructions_override,
            ..Default::default()
        }
    }
    pub fn has_open_calls(&self) -> bool {
        self.calls.keys().any(|id| !self.results.contains_key(id))
    }
    pub fn record_outcome(&mut self, id: u64, limit: usize) -> Result<(), ProcessorError> {
        self.outcome_revisions.insert(id, self.result_revision);
        self.bound(limit)
    }
    pub fn outcome_revision(&self, id: u64) -> Result<u64, ProcessorError> {
        self.outcome_revisions.get(&id).copied().ok_or_else(|| {
            ProcessorError::new("assistant continuation refers to an unadmitted native outcome")
        })
    }
    pub fn validate_call(&self, call: &FunctionCall) -> Result<(), ProcessorError> {
        if self.calls.get(&call.tool_call_id) != Some(call) {
            return Err(ProcessorError::new(
                "tool outcome does not match provider call",
            ));
        }
        Ok(())
    }
    fn advance_result(&mut self) -> Result<(), ProcessorError> {
        self.result_revision = self
            .result_revision
            .checked_add(1)
            .ok_or_else(|| ProcessorError::new("realtime result revision exhausted"))?;
        Ok(())
    }
    pub fn observe(&mut self, event: &Event, limit: usize) -> Result<(), ProcessorError> {
        match event {
            Event::ResponseCreated { response } => {
                self.observed = true;
                self.assistant_text
                    .insert(response.id.clone(), String::new());
            }
            Event::TextDelta {
                delta, response_id, ..
            }
            | Event::AudioTranscriptDelta {
                delta, response_id, ..
            } => {
                self.assistant_text
                    .entry(response_id.clone())
                    .or_default()
                    .push_str(delta);
            }
            Event::InputTranscriptionCompleted { transcript, .. } => {
                self.observed = true;
                self.user_text.push_back(transcript.trim().to_owned());
            }
            Event::FunctionArgumentsDone {
                call_id,
                name,
                arguments,
                ..
            } => {
                let call = FunctionCall {
                    function_name: name
                        .clone()
                        .ok_or_else(|| ProcessorError::new("tool name missing"))?,
                    tool_call_id: call_id.clone(),
                    arguments: serde_json::from_str(arguments)
                        .map_err(|_| ProcessorError::new("invalid tool arguments"))?,
                };
                if self.calls.get(call_id).is_some_and(|old| old != &call) {
                    return Err(ProcessorError::new("conflicting provider tool identity"));
                }
                self.calls.insert(call_id.clone(), call);
            }
            _ => {}
        }
        self.bound(limit)
    }
    pub fn bound(&self, limit: usize) -> Result<(), ProcessorError> {
        if self.history.len() > MAX_MESSAGES
            || self.calls.len() > MAX_MESSAGES
            || self.user_text.len() > MAX_MESSAGES
            || self.assistant_text.len() > MAX_MESSAGES
            || self.outcome_revisions.len() > MAX_MESSAGES
        {
            return Err(ProcessorError::new(
                "realtime context state exceeds message limit",
            ));
        }
        crate::client::bounded_serialized(
            &(
                &self.history,
                &self.user_text,
                &self.assistant_text,
                &self.calls,
                &self.results,
                &self.outcome_revisions,
            ),
            limit,
        )
        .map_err(|_| ProcessorError::new("realtime context state exceeds byte limit"))
    }
    fn known_text(&mut self, message: &ChatMessage) -> bool {
        if message.role == ChatRole::User {
            let mut joined = String::new();
            for (index, text) in self.user_text.iter().enumerate() {
                if !joined.is_empty() {
                    joined.push(' ');
                }
                joined.push_str(text);
                if joined == message.content.trim() {
                    self.user_text.drain(..=index);
                    return true;
                }
            }
        }
        if message.role == ChatRole::Assistant {
            for text in self.assistant_text.values_mut() {
                let trimmed = text.trim_start();
                if !message.content.trim().is_empty() && trimmed.starts_with(message.content.trim())
                {
                    *text = trimmed[message.content.trim().len()..].to_owned();
                    return true;
                }
            }
        }
        false
    }
    pub fn snapshot(
        &mut self,
        messages: Vec<ChatMessage>,
        state_only: bool,
        limit: usize,
    ) -> Result<Vec<Action>, ProcessorError> {
        self.snapshot_with_intent(messages, state_only, limit, None, false)
    }
    pub fn snapshot_with_intent(
        &mut self,
        messages: Vec<ChatMessage>,
        state_only: bool,
        limit: usize,
        explicit_append: Option<(usize, usize)>,
        sealed: bool,
    ) -> Result<Vec<Action>, ProcessorError> {
        if messages.len() > MAX_MESSAGES
            || crate::client::bounded_serialized(&messages, limit).is_err()
        {
            return Err(ProcessorError::new(
                "realtime context snapshot exceeds limit",
            ));
        }
        if !state_only && !self.initialized && !self.observed {
            let actions = seed_items(&messages)?;
            self.history = messages;
            self.initialized = true;
            self.bound(limit)?;
            return Ok(actions);
        }
        let mut actions = vec![];
        if state_only {
            for (old, message) in self.history.iter().zip(&messages) {
                if old != message
                    && !(old.role == ChatRole::Tool
                        && message.role == ChatRole::Tool
                        && old.tool_call_id == message.tool_call_id
                        && old.content == "IN_PROGRESS")
                {
                    return Err(ProcessorError::new(
                        "realtime trailing snapshot rewrites accepted local history",
                    ));
                }
            }
            if messages.len() < self.history.len() {
                return Ok(actions);
            }
            // This is a local trailing write, including in an audio-only session.
            // It never owns provider seeding or creates a response.
            for message in messages.iter().skip(self.history.len()) {
                self.known_text(message);
            }
        } else {
            if messages.len() < self.history.len() && !sealed {
                return Err(ProcessorError::new(
                    "realtime history replacement requires a new session",
                ));
            }
            for (index, message) in messages.iter().enumerate() {
                let previous = self.history.get(index);
                if explicit_append.is_some_and(|(start, end)| index >= start && index < end) {
                    if message.role == ChatRole::Tool || !message.tool_calls.is_empty() {
                        return Err(ProcessorError::new(
                            "live tool history requires native terminal receipts",
                        ));
                    }
                    if previous.is_some_and(|old| old != message) {
                        return Err(ProcessorError::new(
                            "explicit realtime append conflicts with observed snapshot",
                        ));
                    }
                    actions.extend(message_items(message)?);
                    continue;
                }
                if previous == Some(message) {
                    continue;
                }
                if sealed
                    && previous.is_some_and(|old| {
                        old.role == ChatRole::Tool
                            && message.role == ChatRole::Tool
                            && old.tool_call_id == message.tool_call_id
                            && message.content == "IN_PROGRESS"
                    })
                {
                    continue;
                }
                let tool_change = previous.is_some_and(|old| {
                    old.role == ChatRole::Tool
                        && message.role == ChatRole::Tool
                        && old.tool_call_id == message.tool_call_id
                        && old.content == "IN_PROGRESS"
                });
                if previous.is_some() && !tool_change {
                    return Err(ProcessorError::new(
                        "realtime history mutation requires a new session",
                    ));
                }
                if !message.tool_calls.is_empty() {
                    if !message.content.is_empty()
                        || message.role != ChatRole::Assistant
                        || message
                            .tool_calls
                            .iter()
                            .any(|call| self.calls.get(&call.tool_call_id) != Some(call))
                    {
                        return Err(ProcessorError::new(
                            "context contains unknown realtime tool calls",
                        ));
                    }
                    continue;
                }
                if message.role == ChatRole::Tool {
                    let id = message
                        .tool_call_id
                        .as_ref()
                        .ok_or_else(|| ProcessorError::new("tool message lacks call ID"))?;
                    if !self.calls.contains_key(id) {
                        return Err(ProcessorError::new(
                            "context contains unknown realtime tool result",
                        ));
                    }
                    if message.content == "IN_PROGRESS" {
                        continue;
                    }
                    if let Some(old) = self.results.get(id) {
                        if old != &message.content {
                            return Err(ProcessorError::new(
                                "context conflicts with delivered tool result",
                            ));
                        }
                    } else {
                        return Err(ProcessorError::new(
                            "context tool outcome lacks a native terminal receipt",
                        ));
                    }
                    continue;
                }
                if self.known_text(message) {
                    continue;
                }
                if self.observed && message.role == ChatRole::Assistant {
                    return Err(ProcessorError::new(
                        "unowned assistant history requires an explicit seed in a new session",
                    ));
                }
                if let Ok(value) = serde_json::from_str::<Value>(&message.content) {
                    if value["type"] == "async_tool" {
                        return Err(ProcessorError::new("asynchronous tool context envelopes are not supported by this realtime adapter"));
                    }
                }
                actions.extend(message_items(message)?);
            }
        }
        if sealed {
            for (index, message) in messages.into_iter().enumerate() {
                if let Some(previous) = self.history.get_mut(index) {
                    if previous.role == ChatRole::Tool
                        && message.role == ChatRole::Tool
                        && previous.tool_call_id == message.tool_call_id
                        && message.content == "IN_PROGRESS"
                    {
                        continue;
                    }
                    *previous = message;
                } else {
                    self.history.push(message);
                }
            }
        } else {
            self.history = messages;
        }
        self.initialized = true;
        // Empty consumed slots have no further ownership information.
        self.assistant_text
            .retain(|_, text| !text.trim().is_empty());
        self.bound(limit)?;
        Ok(actions)
    }
    pub fn result(
        &mut self,
        call: &FunctionCall,
        content: String,
        final_result: bool,
        limit: usize,
    ) -> Result<Option<Action>, ProcessorError> {
        if self.calls.get(&call.tool_call_id) != Some(call) {
            return Err(ProcessorError::new(
                "tool result does not match a provider call",
            ));
        }
        if !final_result {
            return Err(ProcessorError::new("intermediate realtime tool results require unsupported asynchronous context reconciliation"));
        }
        if let Some(old) = self.results.get(&call.tool_call_id) {
            if old == &content {
                return Ok(None);
            }
            return Err(ProcessorError::new(
                "conflicting duplicate realtime tool result",
            ));
        }
        self.results
            .insert(call.tool_call_id.clone(), content.clone());
        self.advance_result()?;
        self.bound(limit)?;
        Ok(Some(Action::ToolResult {
            call_id: call.tool_call_id.clone(),
            output: Value::String(content),
            is_final: true,
            run_response: false,
        }))
    }
    pub fn cancel(
        &mut self,
        id: &str,
        name: &str,
        limit: usize,
    ) -> Result<Option<Action>, ProcessorError> {
        let call = self
            .calls
            .get(id)
            .ok_or_else(|| ProcessorError::new("unknown realtime tool cancellation"))?
            .clone();
        if call.function_name != name {
            return Err(ProcessorError::new("tool cancellation name mismatch"));
        }
        if self.results.contains_key(id) {
            return Ok(None);
        }
        self.result(&call, "CANCELLED".into(), true, limit)
    }
    pub fn interrupted(&mut self) {
        self.continuation_owed = None;
        self.continued_revision = self.result_revision;
    }
}
pub(crate) fn project(messages: &[ContextMessage]) -> Result<Vec<ChatMessage>, ProcessorError> {
    messages
        .iter()
        .map(|message| {
            if (!message.tool_calls.is_empty() && message.role != ChatRole::Assistant)
                || message.tool_calls.iter().any(|call| {
                    call.function_name.is_empty()
                        || call.tool_call_id.is_empty()
                        || !call.arguments.is_object()
                })
                || (message.role == ChatRole::Tool
                    && message.tool_call_id.as_ref().is_none_or(String::is_empty))
                || (message.role != ChatRole::Tool && message.tool_call_id.is_some())
            {
                return Err(ProcessorError::new(
                    "invalid realtime context tool identity",
                ));
            }
            if !message.extra.is_empty() || message.name.is_some() {
                return Err(ProcessorError::new(
                    "realtime context extensions and named messages are unsupported",
                ));
            }
            let content = match &message.content {
                MessageContent::Text(text) => text.clone(),
                MessageContent::Parts(parts) => {
                    let mut result = String::new();
                    for part in parts {
                        match part {
                            MessagePart::Text { text } => result.push_str(text),
                            _ => {
                                return Err(ProcessorError::new(
                                    "realtime frame adapter requires lossless text context",
                                ))
                            }
                        }
                    }
                    result
                }
            };
            Ok(ChatMessage {
                role: message.role,
                content,
                name: None,
                tool_call_id: message.tool_call_id.clone(),
                tool_calls: message.tool_calls.clone(),
            })
        })
        .collect()
}
pub(crate) fn session(context: &LlmContextData) -> Result<SessionProperties, ProcessorError> {
    let mut update = SessionProperties::new();
    if let Some(message) = context
        .messages
        .first()
        .filter(|m| m.role == ChatRole::System)
    {
        update.instructions = Some(project(std::slice::from_ref(message))?[0].content.clone());
    }
    let tools = match &context.tools {
        ToolSet::NotGiven => None,
        ToolSet::Functions(tools) => Some(tools.clone()),
        ToolSet::Direct(tools) => Some(tools.iter().map(|tool| tool.schema.clone()).collect()),
        _ => {
            return Err(ProcessorError::new(
                "realtime tools require typed function definitions",
            ))
        }
    };
    update.tools = tools.map(|tools| {
        tools
            .iter()
            .map(|tool| FunctionTool::new(&tool.name, &tool.description, tool.parameters.clone()))
            .collect()
    });
    update.tool_choice = match &context.tool_choice {
        None => None,
        Some(pipecat_core::ToolChoice::Auto) => Some(ToolChoice::Auto),
        Some(pipecat_core::ToolChoice::None) => Some(ToolChoice::None),
        Some(pipecat_core::ToolChoice::Required) => Some(ToolChoice::Required),
        Some(pipecat_core::ToolChoice::Function(_)) => {
            return Err(ProcessorError::new(
                "named realtime tool choice is unsupported",
            ))
        }
    };
    Ok(update)
}
const SEED_INTRO: &str = "\n        This is a previously saved conversation. Please treat this conversation history as a\n        starting point for the current conversation.";
const SEED_TRAILING: &str = "\n        This is the end of the previously saved conversation. Please continue the conversation\n        from here. If the last message is a user instruction or question, act on that instruction\n        or answer the question. If the last message is an assistant response, simple say that you\n        are ready to continue the conversation.";

#[derive(serde::Serialize)]
struct SeedMessage<'a> {
    role: ChatRole,
    content: &'a str,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    tool_calls: Vec<SeedCall<'a>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    tool_call_id: Option<&'a str>,
}
#[derive(serde::Serialize)]
struct SeedCall<'a> {
    id: &'a str,
    #[serde(rename = "type")]
    kind: &'static str,
    function: SeedFunction<'a>,
}
#[derive(serde::Serialize)]
struct SeedFunction<'a> {
    name: &'a str,
    arguments: String,
}

fn seed_items(messages: &[ChatMessage]) -> Result<Vec<Action>, ProcessorError> {
    let messages = if messages.first().is_some_and(|m| m.role == ChatRole::System) {
        &messages[1..]
    } else {
        messages
    };
    if messages.is_empty() {
        return Ok(vec![]);
    }
    let mut standard = Vec::with_capacity(messages.len());
    for message in messages {
        if message.name.is_some() {
            return Err(ProcessorError::new(
                "named realtime seed messages are unsupported",
            ));
        }
        let role = match message.role {
            ChatRole::System | ChatRole::Developer => ChatRole::User,
            role => role,
        };
        standard.push(SeedMessage {
            role,
            content: &message.content,
            tool_call_id: message.tool_call_id.as_deref(),
            tool_calls: message
                .tool_calls
                .iter()
                .map(|call| SeedCall {
                    id: &call.tool_call_id,
                    kind: "function",
                    function: SeedFunction {
                        name: &call.function_name,
                        arguments: call.arguments.to_string(),
                    },
                })
                .collect(),
        });
    }
    if standard.len() == 1 && standard[0].role == ChatRole::User {
        return message_items(&ChatMessage::new(ChatRole::User, standard[0].content));
    }
    let history = serde_json::to_string_pretty(&standard)
        .map_err(|_| ProcessorError::new("invalid realtime seed JSON"))?;
    message_items(&ChatMessage::new(
        ChatRole::User,
        format!("{SEED_INTRO}\n\n{history}\n\n{SEED_TRAILING}"),
    ))
}

fn message_items(message: &ChatMessage) -> Result<Vec<Action>, ProcessorError> {
    if message.name.is_some() {
        return Err(ProcessorError::new(
            "named realtime messages are unsupported",
        ));
    }
    let mut items = vec![];
    if !message.tool_calls.is_empty() {
        if message.role != ChatRole::Assistant || !message.content.is_empty() {
            return Err(ProcessorError::new(
                "seed tool calls require a separate empty assistant message",
            ));
        }
        for call in &message.tool_calls {
            if call.tool_call_id.is_empty()
                || call.function_name.is_empty()
                || !call.arguments.is_object()
            {
                return Err(ProcessorError::new("invalid seed tool call"));
            }
            items.push(ConversationItem::FunctionCall {
                id: next_item(),
                call_id: call.tool_call_id.clone(),
                name: call.function_name.clone(),
                arguments: call.arguments.to_string(),
            });
        }
    } else if message.role == ChatRole::Tool {
        if message.content == "IN_PROGRESS" {
            return Err(ProcessorError::new(
                "cannot seed an unresolved tool placeholder",
            ));
        }
        items.push(ConversationItem::FunctionCallOutput {
            id: next_item(),
            call_id: message
                .tool_call_id
                .clone()
                .ok_or_else(|| ProcessorError::new("tool message requires call ID"))?,
            output: message.content.clone(),
        });
    } else {
        let role = match message.role {
            ChatRole::User => Role::User,
            ChatRole::Assistant => Role::Assistant,
            _ => Role::System,
        };
        let content = if role == Role::Assistant {
            ItemContent::OutputText {
                text: message.content.clone(),
            }
        } else {
            ItemContent::InputText {
                text: message.content.clone(),
            }
        };
        items.push(ConversationItem::Message {
            id: next_item(),
            role,
            content: vec![content],
        });
    }
    Ok(items
        .into_iter()
        .map(|item| Action::CreateItem {
            item,
            previous_item_id: None,
        })
        .collect())
}
fn next_item() -> String {
    static ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    format!(
        "pc_context_{}",
        ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use pipecat_core::{
        DetailedLlmContextFrame, Frame, FrameData, LlmContextFrame,
        REALTIME_CONTEXT_UPDATE_METADATA,
    };
    #[test]
    fn first_state_only_snapshot_never_owns_seed_and_append_is_incremental() {
        let mut state = ContextState::default();
        let heard = ChatMessage::new(ChatRole::User, "heard");
        assert!(state
            .snapshot(vec![heard.clone()], true, 4096)
            .unwrap()
            .is_empty());
        assert!(state
            .snapshot(vec![heard.clone()], true, 4096)
            .unwrap()
            .is_empty());
        let actions = state
            .snapshot(
                vec![heard, ChatMessage::new(ChatRole::User, "next")],
                false,
                4096,
            )
            .unwrap();
        assert_eq!(actions.len(), 1);
        assert!(
            matches!(&actions[0], Action::CreateItem { item: ConversationItem::Message { content, .. }, .. } if matches!(&content[0], ItemContent::InputText { text } if text == "next"))
        );
    }
    #[test]
    fn both_context_forms_recognize_marker_but_speculation_does_not() {
        for data in [
            FrameData::LlmContext(LlmContextFrame::default()),
            FrameData::LlmContextDetailed(DetailedLlmContextFrame {
                context: LlmContextFrame::default().into(),
                speculation: false,
            }),
        ] {
            let mut frame = Frame::new(data);
            frame
                .metadata
                .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
            assert!(frame.is_realtime_context_update());
        }
        let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextFrame::default().into(),
            speculation: true,
        }));
        frame
            .metadata
            .insert(REALTIME_CONTEXT_UPDATE_METADATA.into(), true.into());
        assert!(!frame.is_realtime_context_update());
    }
    #[test]
    fn text_parts_are_lossless_and_opaque_parts_fail() {
        let mut message: ContextMessage = ChatMessage::new(ChatRole::User, "").into();
        message.content = MessageContent::Parts(vec![
            MessagePart::Text { text: "a ".into() },
            MessagePart::Text { text: "b".into() },
        ]);
        assert_eq!(project(&[message.clone()]).unwrap()[0].content, "a b");
        message.content = MessageContent::Parts(vec![MessagePart::Thinking {
            text: "private".into(),
            signature: Some("signed".into()),
        }]);
        assert!(project(&[message]).is_err());
    }
    #[test]
    fn history_replacement_and_unowned_assistant_are_explicit_errors() {
        let mut state = ContextState::default();
        state
            .snapshot(
                vec![ChatMessage::new(ChatRole::User, "original")],
                false,
                4096,
            )
            .unwrap();
        assert!(state
            .snapshot(
                vec![ChatMessage::new(ChatRole::User, "changed")],
                false,
                4096
            )
            .is_err());
        assert_eq!(state.history[0].content, "original");
        state.observed = true;
        assert!(state
            .snapshot(
                vec![
                    ChatMessage::new(ChatRole::User, "original"),
                    ChatMessage::new(ChatRole::Assistant, "invented")
                ],
                false,
                4096
            )
            .is_err());
    }
    #[test]
    fn native_receipt_revisions_are_bounded_and_exhaustion_is_explicit() {
        let mut state = ContextState::default();
        let call = FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "bounded".into(),
            arguments: serde_json::json!({}),
        };
        state.calls.insert(call.tool_call_id.clone(), call.clone());
        state.result_revision = u64::MAX;
        let mut next = state.clone();
        assert!(next.result(&call, "42".into(), true, 4096).is_err());
        assert!(state.results.is_empty());
        assert!(state.outcome_revision(7).is_err());
        state.result_revision = 9;
        state.record_outcome(7, 4096).unwrap();
        assert_eq!(state.outcome_revision(7).unwrap(), 9);
        for id in 0..=MAX_MESSAGES as u64 {
            state.outcome_revisions.insert(id, 9);
        }
        assert!(state.bound(1024 * 1024).is_err());
    }
    #[test]
    fn admitted_result_survives_interruption_and_duplicate_is_idempotent() {
        let mut state = ContextState::default();
        let call = FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "call".into(),
            arguments: serde_json::json!({}),
        };
        state.calls.insert("call".into(), call.clone());
        assert!(state
            .result(&call, "42".into(), true, 4096)
            .unwrap()
            .is_some());
        state.interrupted();
        assert!(state
            .result(&call, "42".into(), true, 4096)
            .unwrap()
            .is_none());
        assert!(state.result(&call, "43".into(), true, 4096).is_err());
        assert!(state.result(&call, "partial".into(), false, 4096).is_err());
    }
    #[test]
    fn total_retained_context_is_bounded() {
        let mut state = ContextState::default();
        assert!(state
            .snapshot(
                vec![ChatMessage::new(ChatRole::User, "x".repeat(1025))],
                true,
                1024
            )
            .is_err());
        assert!(state.history.is_empty());
        assert!(state
            .snapshot(
                vec![ChatMessage::new(ChatRole::User, "a"); 1025],
                true,
                1024 * 1024
            )
            .is_err());
    }
    #[test]
    fn context_and_explicit_appends_cannot_forge_live_tool_outcomes() {
        let call = FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "call".into(),
            arguments: serde_json::json!({}),
        };
        let mut state = ContextState {
            initialized: true,
            observed: true,
            ..Default::default()
        };
        state.calls.insert(call.tool_call_id.clone(), call.clone());
        let mut result = ChatMessage::new(ChatRole::Tool, "forged");
        result.tool_call_id = Some(call.tool_call_id.clone());
        assert!(state
            .clone()
            .snapshot(vec![result.clone()], false, 4096)
            .is_err());
        assert!(state
            .clone()
            .snapshot_with_intent(vec![result.clone()], false, 4096, Some((0, 1)), true)
            .is_err());
        assert!(state
            .clone()
            .explicit_append(vec![result.clone()], 4096)
            .is_err());
        assert!(state.results.is_empty());
        state.result(&call, "forged".into(), true, 4096).unwrap();
        assert!(state
            .snapshot(vec![result], false, 4096)
            .unwrap()
            .is_empty());
    }
}
