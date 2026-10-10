//! Ordered provider history reconciled against receipt-backed delivered text.
use crate::context::ContextState;
use pipecat_core::{
    ChatRole, ContextMessage, FunctionCall, LlmContinuationBlock, LlmContinuationProvider,
    NativeLlmContinuation, ProcessorError,
};
use pipecat_services::{frame_context, Content, ContentPart, Message, Role, ToolCall};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

pub(crate) struct SignedHistory {
    id: u64,
    provider: LlmContinuationProvider,
    lineage: u64,
    start: usize,
    owned: BTreeMap<usize, ContextMessage>,
    results: BTreeMap<usize, String>,
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
            owned: BTreeMap::new(),
            results: BTreeMap::new(),
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
    pub(crate) fn text(&mut self, index: usize, message: ContextMessage, text: String) {
        self.delivered_bytes += text.len();
        self.owned.insert(index, message);
        self.delivered.push(text);
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
    pub(crate) fn message(&mut self, index: usize, message: ContextMessage) {
        if message.role == ChatRole::Tool {
            if let Some(id) = &message.tool_call_id {
                self.results.insert(index, id.clone());
                return;
            }
        }
        self.owned.insert(index, message);
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
            if let Some(id) = self.results.get(&index) {
                if message.role != ChatRole::Tool || message.tool_call_id.as_ref() != Some(id) {
                    return Err(error("signed continuation tool result changed identity"));
                }
                results.push(
                    frame_context::message(message)
                        .map_err(|_| error("invalid signed continuation result"))?,
                );
            } else if self.owned.get(&index) != Some(message) {
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
        let Some(history) = &self.snapshot else {
            return Ok(());
        };
        let results = self.context_results(context)?;
        let (mut remaining, trailing) =
            delivered_prefix(history, self.delivered.iter().map(String::as_str))?;
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
        context.replace_native_suffix(self.start, messages);
        self.owned.clear();
        self.results.clear();
        for (index, message) in context.messages.iter().enumerate().skip(self.start) {
            self.message(index, message.clone());
        }
        Ok(())
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
