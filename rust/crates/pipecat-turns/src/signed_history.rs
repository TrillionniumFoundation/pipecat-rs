//! Ordered provider history reconciled against receipt-backed delivered text.
use crate::context::ContextState;
use pipecat_core::{
    ChatRole, ContextMessage, FunctionCall, LlmContinuationBlock, LlmContinuationProvider,
    NativeLlmContinuation, ProcessorError,
};
use pipecat_services::{frame_context, Content, ContentPart, Message, Role, ToolCall};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

/// Context validation mirror, separate from application-owned conversation.
#[derive(Default)]
struct SignedMirror {
    owned: BTreeMap<usize, ContextMessage>,
    results: BTreeMap<usize, String>,
    bytes: usize,
}
impl SignedMirror {
    const MAX_MESSAGES: usize = 4096;
    const MAX_BYTES: usize = 32 * 1024 * 1024;
    fn bytes(message: &ContextMessage) -> Result<usize, ProcessorError> {
        if message.role == ChatRole::Tool {
            if let Some(id) = &message.tool_call_id {
                return Ok(id.len());
            }
        }
        let mut counter = crate::tool_history::ByteCounter {
            bytes: 0,
            limit: Self::MAX_BYTES,
        };
        serde_json::to_writer(&mut counter, message).map_err(|e| error(&e.to_string()))?;
        Ok(counter.bytes)
    }
    fn check<'a>(
        &self,
        additions: impl Iterator<Item = (usize, &'a ContextMessage)>,
    ) -> Result<(), ProcessorError> {
        let mut count = self.owned.len() + self.results.len();
        let mut bytes = self.bytes;
        for (index, message) in additions {
            if let Some(old) = self.owned.get(&index) {
                bytes -= Self::bytes(old)?;
            } else if let Some(old) = self.results.get(&index) {
                bytes -= old.len();
            } else {
                count += 1;
            }
            bytes = bytes
                .checked_add(Self::bytes(message)?)
                .filter(|n| *n <= Self::MAX_BYTES)
                .ok_or_else(|| error("signed context mirror exceeds byte limit"))?;
            if count > Self::MAX_MESSAGES {
                return Err(error("signed context mirror exceeds message limit"));
            }
        }
        Ok(())
    }
    fn insert(&mut self, index: usize, message: &ContextMessage) -> Result<(), ProcessorError> {
        self.check(std::iter::once((index, message)))?;
        if let Some(old) = self.owned.remove(&index) {
            self.bytes -= Self::bytes(&old)?;
        }
        if let Some(old) = self.results.remove(&index) {
            self.bytes -= old.len();
        }
        self.bytes += Self::bytes(message)?;
        if message.role == ChatRole::Tool {
            if let Some(id) = &message.tool_call_id {
                self.results.insert(index, id.clone());
                return Ok(());
            }
        }
        self.owned.insert(index, message.clone());
        Ok(())
    }
}

pub(crate) struct SignedHistory {
    id: u64,
    provider: LlmContinuationProvider,
    lineage: u64,
    start: usize,
    mirror: SignedMirror,
    delivered: Vec<String>,
    delivered_bytes: usize,
    playback: VecDeque<String>,
    snapshot: Option<Arc<NativeLlmContinuation>>,
    terminal: bool,
}
impl SignedHistory {
    pub(crate) fn new(id: u64, provider: LlmContinuationProvider, context: &ContextState) -> Self {
        Self {
            id,
            provider,
            lineage: context.lineage,
            start: context.messages.len(),
            mirror: SignedMirror::default(),
            delivered: vec![],
            delivered_bytes: 0,
            playback: VecDeque::new(),
            snapshot: None,
            terminal: false,
        }
    }
    pub(crate) fn id(&self) -> u64 {
        self.id
    }
    pub(crate) fn has_snapshot(&self) -> bool {
        self.snapshot.is_some()
    }
    pub(crate) fn is_terminal(&self) -> bool {
        self.terminal
    }
    pub(crate) fn finish_response(&mut self) {
        self.terminal = true;
    }
    pub(crate) fn playback_started(&mut self, id: &str) -> Result<(), ProcessorError> {
        if self.playback.iter().any(|prior| prior == id) {
            return Ok(());
        }
        if id.is_empty() || id.len() > 1024 || self.playback.len() >= 4096 {
            return Err(error("signed playback identity exceeds limit"));
        }
        self.playback.push_back(id.into());
        Ok(())
    }
    pub(crate) fn check_playback(&self, id: &str) -> Result<(), ProcessorError> {
        if self.playback.front().is_none_or(|first| first != id) {
            return Err(error(
                "signed playback receipt is out of order or unregistered",
            ));
        }
        Ok(())
    }
    pub(crate) fn playback_confirmed(&mut self, id: &str) -> Result<(), ProcessorError> {
        self.check_playback(id)?;
        self.playback.pop_front();
        Ok(())
    }
    pub(crate) fn playback_pending(&self) -> bool {
        !self.playback.is_empty()
    }
    pub(crate) fn update(
        &mut self,
        history: Arc<NativeLlmContinuation>,
        pending: &str,
    ) -> Result<bool, ProcessorError> {
        if history.response_id() != self.id
            || history.provider() != self.provider
            || (!history.is_complete() && !history.is_checkpoint())
        {
            return Err(error("mismatched signed continuation identity"));
        }
        if let Some(existing) = &self.snapshot {
            if history.is_checkpoint() && existing.blocks().starts_with(history.blocks()) {
                return Ok(false);
            }
            if existing.is_complete() {
                if existing.blocks() == history.blocks() && history.is_complete() {
                    return Ok(false);
                }
                return Err(error("completed signed history cannot change"));
            }
            if !history.blocks().starts_with(existing.blocks()) {
                return Err(error(
                    "signed checkpoint is not an exact prefix of its successor",
                ));
            }
        }
        delivered_prefix(
            &history,
            self.delivered
                .iter()
                .map(String::as_str)
                .chain((!pending.is_empty()).then_some(pending)),
        )?;
        self.snapshot = Some(history);
        Ok(true)
    }
    pub(crate) fn owns_call(&self, call: &FunctionCall) -> bool {
        self.snapshot.as_ref().is_some_and(|history|history.is_complete() && history.blocks().iter().any(|block|matches!(block,LlmContinuationBlock::ToolCall(expected) if expected==call)))
    }
    pub(crate) fn text(
        &mut self,
        index: usize,
        message: &ContextMessage,
        text: String,
    ) -> Result<(), ProcessorError> {
        self.mirror.insert(index, message)?;
        self.delivered_bytes += text.len();
        self.delivered.push(text);
        Ok(())
    }
    pub(crate) fn check_text(&self, text: &str) -> Result<(), ProcessorError> {
        if !text.is_empty()
            && (self.delivered.len() >= 4096
                || text.len() > (8 * 1024 * 1024usize).saturating_sub(self.delivered_bytes))
        {
            return Err(error("signed delivery history exceeds size limit"));
        }
        if let Some(history) = &self.snapshot {
            delivered_prefix(
                history,
                self.delivered
                    .iter()
                    .map(String::as_str)
                    .chain((!text.is_empty()).then_some(text)),
            )?;
        }
        Ok(())
    }
    pub(crate) fn check_messages(
        &self,
        start: usize,
        messages: &[ContextMessage],
    ) -> Result<(), ProcessorError> {
        self.mirror.check(
            messages
                .iter()
                .enumerate()
                .map(|(offset, message)| (start + offset, message)),
        )
    }
    pub(crate) fn check_text_commit(
        &self,
        context: &ContextState,
        text: &str,
    ) -> Result<(), ProcessorError> {
        self.check_messages(
            context.messages.len(),
            &[crate::context::text_message(ChatRole::Assistant, text)],
        )?;
        self.reconciliation(context, Some(text))?;
        Ok(())
    }
    pub(crate) fn message(
        &mut self,
        index: usize,
        message: &ContextMessage,
    ) -> Result<(), ProcessorError> {
        self.mirror.insert(index, message)
    }
    pub(crate) fn check_context(&self, context: &ContextState) -> Result<(), ProcessorError> {
        self.context_results(context).map(|_| ())
    }
    fn context_results(&self, context: &ContextState) -> Result<Vec<Message>, ProcessorError> {
        if context.lineage != self.lineage || context.messages.len() < self.start {
            return Err(error("context changed during signed continuation"));
        }
        let mut results = vec![];
        for (index, message) in context.messages.iter().enumerate().skip(self.start) {
            if let Some(id) = self.mirror.results.get(&index) {
                if message.role != ChatRole::Tool || message.tool_call_id.as_ref() != Some(id) {
                    return Err(error("signed continuation tool result changed identity"));
                }
                results.push(
                    frame_context::message(message)
                        .map_err(|_| error("invalid signed continuation result"))?,
                );
            } else if self.mirror.owned.get(&index) != Some(message) {
                return Err(error(
                    "unrelated or transformed messages overlap signed continuation",
                ));
            } else if message.role != ChatRole::Assistant {
                results.push(
                    frame_context::message(message)
                        .map_err(|_| error("invalid signed continuation result"))?,
                );
            }
        }
        Ok(results)
    }
    pub(crate) fn reconcile(&mut self, context: &mut ContextState) -> Result<(), ProcessorError> {
        if let Some((messages, mirror)) = self.reconciliation(context, None)? {
            context.replace_native_suffix(self.start, messages);
            self.mirror = mirror;
        }
        Ok(())
    }
    fn reconciliation(
        &self,
        context: &ContextState,
        extra_text: Option<&str>,
    ) -> Result<Option<(Vec<Message>, SignedMirror)>, ProcessorError> {
        let Some(history) = &self.snapshot else {
            return Ok(None);
        };
        let results = self.context_results(context)?;
        let (mut remaining, trailing) = delivered_prefix(
            history,
            self.delivered.iter().map(String::as_str).chain(extra_text),
        )?;
        let mut messages = vec![];
        let mut complete_public_prefix = true;
        for block in history.blocks() {
            match block {
                LlmContinuationBlock::Text(text) => {
                    let take = text.len().min(remaining);
                    if take > 0 {
                        messages.push(Message::text(Role::Assistant, &text[..take]));
                    }
                    remaining -= take;
                }
                LlmContinuationBlock::BedrockReasoning { text, signature } => {
                    let mut message = Message::text(Role::Assistant, "");
                    message.content = Some(Content::Parts(vec![ContentPart::BedrockReasoning {
                        text: text.clone(),
                        signature: signature.clone(),
                    }]));
                    messages.push(message);
                }
                LlmContinuationBlock::BedrockRedacted(data) => {
                    let mut message = Message::text(Role::Assistant, "");
                    message.content = Some(Content::Parts(vec![ContentPart::BedrockRedacted {
                        data: data.clone(),
                    }]));
                    messages.push(message);
                }
                LlmContinuationBlock::Thinking { text, signature } => {
                    let mut message = Message::text(Role::Assistant, "");
                    message.content = Some(Content::Parts(vec![ContentPart::Thinking {
                        text: text.clone(),
                        signature: Some(signature.clone()),
                    }]));
                    messages.push(message);
                }
                LlmContinuationBlock::GeminiText {
                    text,
                    thought,
                    signature,
                } => {
                    let take = if *thought {
                        text.len()
                    } else {
                        text.len().min(remaining)
                    };
                    let incomplete = !*thought
                        && signature.is_some()
                        && (!complete_public_prefix || take != text.len());
                    if *thought || take > 0 || signature.is_some() {
                        let mut message = Message::text(Role::Assistant, "");
                        message.content = Some(Content::Parts(vec![ContentPart::GeminiText {
                            text: text[..take].into(),
                            thought: *thought,
                            signature: if incomplete { None } else { signature.clone() },
                            incomplete,
                        }]));
                        messages.push(message);
                    }
                    if !*thought {
                        remaining -= take;
                        complete_public_prefix &= take == text.len();
                    }
                }
                LlmContinuationBlock::ToolCall(call) if history.is_complete() => {
                    let mut message = Message::assistant_tool_calls(vec![ToolCall {
                        id: call.tool_call_id.clone(),
                        name: call.function_name.clone(),
                        arguments: call.arguments.clone(),
                    }]);
                    message.content = Some(Content::Parts(vec![]));
                    messages.push(message);
                }
                LlmContinuationBlock::ToolCall(_) => {}
                LlmContinuationBlock::GeminiToolMetadata {
                    tool_call_id,
                    signature,
                } if history.is_complete() => {
                    let message = messages
                        .last_mut()
                        .ok_or_else(|| error("unowned Gemini tool bookmark"))?;
                    if message.tool_calls.len() != 1 || message.tool_calls[0].id != *tool_call_id {
                        return Err(error("mismatched Gemini tool bookmark"));
                    }
                    message.content = Some(Content::Parts(vec![ContentPart::GeminiToolMetadata {
                        tool_call_id: tool_call_id.clone(),
                        signature: signature.clone(),
                    }]));
                }
                LlmContinuationBlock::GeminiToolMetadata { .. } => {}
            }
        }
        messages.extend(
            trailing
                .into_iter()
                .map(|text| Message::text(Role::Assistant, text)),
        );
        messages.extend(results);
        // Admission of the complete replacement precedes context mutation.
        let mut mirror = SignedMirror::default();
        for (offset, message) in messages.iter().enumerate() {
            mirror.insert(
                self.start + offset,
                &frame_context::to_core_message(message),
            )?;
        }
        Ok(Some((messages, mirror)))
    }
}
/// Match exact bytes in order. Only whitespace trimmed at a committed delivery
/// chunk boundary can be skipped; internal whitespace is never normalized.
fn delivered_prefix<'a>(
    history: &NativeLlmContinuation,
    delivered: impl Iterator<Item = &'a str>,
) -> Result<(usize, Vec<String>), ProcessorError> {
    let generated: String = history
        .blocks()
        .iter()
        .filter_map(|block| match block {
            LlmContinuationBlock::Text(text)
            | LlmContinuationBlock::GeminiText {
                text,
                thought: false,
                ..
            } => Some(text.as_str()),
            _ => None,
        })
        .collect();
    let mut cursor = 0;
    let mut trailing = vec![];
    for text in delivered {
        let tail = &generated[cursor..];
        cursor += tail.len() - tail.trim_start().len();
        let tail = &generated[cursor..];
        if tail.starts_with(text) {
            cursor += text.len();
        } else if history.is_checkpoint() && text.starts_with(tail) {
            let rest = &text[tail.len()..];
            if !rest.is_empty() {
                trailing.push(rest.into());
            }
            cursor = generated.len();
        } else {
            return Err(error(
                "delivered text is not an unambiguous generated prefix",
            ));
        }
    }
    if cursor > 0 {
        let tail = &generated[cursor..];
        cursor += tail.len() - tail.trim_start().len();
    }
    Ok((cursor, trailing))
}
fn error(message: &str) -> ProcessorError {
    ProcessorError::new(message)
}
