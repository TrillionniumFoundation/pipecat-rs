use pipecat_core::{
    ChatMessage, ChatRole, ContextMessage, DetailedLlmContextFrame, FrameData, LlmContextData,
    LlmContextFrame, MessageContent, ProcessorError, ToolSet,
};
use pipecat_services::{frame_context, Content, Context, Message};
use std::collections::BTreeMap;
use std::ops::{Deref, DerefMut};
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc, Mutex, MutexGuard,
};

static CONTEXT_LINEAGE: AtomicU64 = AtomicU64::new(1);
fn next_lineage() -> u64 {
    CONTEXT_LINEAGE.fetch_add(1, Ordering::Relaxed)
}

/// Shared conversation history. Rich content, extensions, and executable tool
/// definitions remain intact. Legacy snapshots are checked projections.
#[derive(Debug, Clone)]
pub struct SharedContext(Arc<Mutex<ContextState>>);
impl Default for SharedContext {
    fn default() -> Self {
        Self::new(Context::default())
    }
}
#[derive(Debug)]
pub(crate) struct ContextState {
    data: LlmContextData,
    // Native messages may distinguish forms with equal universal projections.
    originals: BTreeMap<usize, (ContextMessage, Message)>,
    rich: bool,
    pub(crate) lineage: u64,
}
impl Deref for ContextState {
    type Target = LlmContextData;
    fn deref(&self) -> &Self::Target {
        &self.data
    }
}
impl DerefMut for ContextState {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.data
    }
}
impl ContextState {
    fn frame_data(&self) -> Result<FrameData, ProcessorError> {
        self.require_lossless_rich_projection()?;
        if !self.rich && !matches!(&self.tools, ToolSet::Functions(tools) if tools.is_empty()) {
            if let Ok(frame) = self.legacy_frame() {
                return Ok(FrameData::LlmContext(frame));
            }
        }
        Ok(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: self.data.clone(),
            speculation: false,
        }))
    }
    /// Check the positional edit lineage and project under the same lock.
    pub(crate) fn frame_data_at_lineage(
        &self,
        expected: Option<u64>,
    ) -> Result<FrameData, ProcessorError> {
        if expected.is_some_and(|lineage| lineage != self.lineage) {
            return Err(ProcessorError::new(
                "local context lineage changed before realtime intent snapshot",
            ));
        }
        self.frame_data()
    }
    fn new(context: Context) -> Self {
        let mut state = Self {
            data: LlmContextData {
                messages: vec![],
                tools: context.tools.map_or(ToolSet::NotGiven, |tools| {
                    ToolSet::Functions(
                        tools
                            .into_iter()
                            .map(|tool| pipecat_core::ToolDefinition {
                                name: tool.name,
                                description: tool.description,
                                parameters: tool.parameters,
                            })
                            .collect(),
                    )
                }),
                tool_choice: context.tool_choice.map(|choice| match choice {
                    pipecat_services::ToolChoice::Auto => pipecat_core::ToolChoice::Auto,
                    pipecat_services::ToolChoice::None => pipecat_core::ToolChoice::None,
                    pipecat_services::ToolChoice::Required => pipecat_core::ToolChoice::Required,
                    pipecat_services::ToolChoice::Function(name) => {
                        pipecat_core::ToolChoice::Function(name)
                    }
                }),
            },
            originals: BTreeMap::new(),
            rich: false,
            lineage: next_lineage(),
        };
        for message in context.messages {
            state.append_native(message);
        }
        state
    }
    pub(crate) fn replace_native_suffix(&mut self, start: usize, messages: Vec<Message>) {
        self.messages.truncate(start);
        self.originals.retain(|index, _| *index < start);
        for message in messages {
            self.append_native(message);
        }
    }
    pub(crate) fn append_native(&mut self, message: Message) {
        let mut projection = frame_context::to_core_message(&message);
        if message.content.is_none() {
            projection.content = MessageContent::Text(String::new());
        }
        self.originals
            .insert(self.messages.len(), (projection.clone(), message));
        self.messages.push(projection);
    }
    pub(crate) fn append_legacy(&mut self, messages: &[ChatMessage], replace: bool) {
        if replace {
            self.lineage = next_lineage();
            self.messages.clear();
            self.originals.clear();
            self.rich = false;
        }
        for message in messages {
            self.append_native(frame_context::legacy_message(message));
        }
    }
    pub(crate) fn append_rich(&mut self, messages: impl IntoIterator<Item = ContextMessage>) {
        self.rich = true;
        self.messages.extend(messages);
    }
    pub(crate) fn set_rich(&mut self, messages: Vec<ContextMessage>) {
        self.lineage = next_lineage();
        self.rich = true;
        self.messages = messages;
        self.originals.clear();
    }
    pub(crate) fn transform(
        &mut self,
        transform: &pipecat_core::LocalMessageTransform,
    ) -> Result<(), ProcessorError> {
        let transformed = transform.apply(self.messages.clone())?;
        let mut retained = BTreeMap::new();
        let mut consumed = vec![false; self.messages.len()];
        for (new_index, message) in transformed.iter().enumerate() {
            let candidates: Vec<_> = self
                .messages
                .iter()
                .enumerate()
                .filter_map(|(index, old)| (old == message).then_some(index))
                .collect();
            let Some(&first) = candidates.first() else {
                continue;
            };
            let native = |index| {
                self.originals
                    .get(&index)
                    .filter(|(projection, _)| projection == message)
                    .map(|(_, original)| original.clone())
                    .or_else(|| frame_context::message(message).ok())
            };
            let equivalent = candidates
                .iter()
                .all(|&index| native(index) == native(first));
            let source = if equivalent {
                first
            } else {
                if transformed.iter().filter(|other| *other == message).count() != candidates.len()
                {
                    return Err(ProcessorError::new("message transform ambiguously selects distinct native content with identical rich projections"));
                }
                candidates
                    .into_iter()
                    .find(|&index| !consumed[index])
                    .expect("equal occurrence counts")
            };
            consumed[source] = true;
            if let Some(original) = self
                .originals
                .get(&source)
                .filter(|(projection, _)| projection == message)
            {
                retained.insert(new_index, original.clone());
            }
        }
        self.lineage = next_lineage();
        self.messages = transformed;
        self.originals = retained;
        Ok(())
    }
    /// Replace only the admitted range, retaining exact native originals by index.
    pub(crate) fn apply_summary(&mut self, start: usize, end: usize, summary: ContextMessage) {
        let removed = end - start;
        let originals = std::mem::take(&mut self.originals);
        for (index, original) in originals {
            if index < start {
                self.originals.insert(index, original);
            } else if index >= end {
                self.originals.insert(index - removed + 1, original);
            }
        }
        self.messages.splice(start..end, [summary]);
        self.lineage = next_lineage();
    }
    /// Summary transcripts preserve native distinctions in an explicit extension.
    /// These snapshots are data for the summarizer, not conversation inference.
    pub(crate) fn summary_snapshot(&self) -> Result<LlmContextData, ProcessorError> {
        let mut data = LlmContextData {
            messages: self.messages.clone(),
            tools: ToolSet::NotGiven,
            tool_choice: None,
        };
        for (index, (projection, original)) in &self.originals {
            if self.messages.get(*index) == Some(projection)
                && frame_context::message(projection).ok().as_ref() != Some(original)
            {
                data.messages[*index].extra.insert(
                    "pipecat_native_message".into(),
                    serde_json::to_value(original)
                        .map_err(|error| ProcessorError::new(error.to_string()))?,
                );
            }
        }
        Ok(data)
    }
    fn snapshot(&self) -> Result<Context, ProcessorError> {
        let messages = self
            .messages
            .iter()
            .enumerate()
            .map(|(index, message)| {
                if let Some((_, original)) = self
                    .originals
                    .get(&index)
                    .filter(|(projection, _)| projection == message)
                {
                    return Ok(original.clone());
                }
                frame_context::message(message)
                    .map_err(|error| ProcessorError::new(error.to_string()))
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Context {
            messages,
            tools: frame_context::tools(&self.tools)
                .map_err(|error| ProcessorError::new(error.to_string()))?,
            tool_choice: self.tool_choice.as_ref().map(frame_context::tool_choice),
        })
    }
    fn require_lossless_rich_projection(&self) -> Result<(), ProcessorError> {
        if self.originals.iter().any(|(index, (projection, original))| self.messages.get(*index) == Some(projection)
            && matches!(&original.content, Some(Content::Parts(parts)) if parts.iter().any(|part| matches!(part, pipecat_services::ContentPart::InlineData { .. })))) {
            return Err(ProcessorError::new("native InlineData requires an explicit image/file conversion before rich projection"));
        }
        Ok(())
    }
    fn legacy_frame(&self) -> Result<LlmContextFrame, ProcessorError> {
        if self
            .messages
            .iter()
            .any(|message| !message.extra.is_empty())
        {
            return Err(ProcessorError::new(
                "message extensions require a detailed context frame",
            ));
        }
        let tools = match &self.tools {
            ToolSet::NotGiven => vec![],
            ToolSet::Functions(tools) if !tools.is_empty() => tools.clone(),
            ToolSet::Functions(_) => {
                return Err(ProcessorError::new(
                    "explicit empty tool list requires a detailed context frame",
                ))
            }
            _ => {
                return Err(ProcessorError::new(
                    "rich tool definitions require a detailed context frame",
                ))
            }
        };
        let messages = self
            .messages
            .iter()
            .map(|message| {
                let MessageContent::Text(text) = &message.content else {
                    return Err(ProcessorError::new(
                        "multimodal context requires a detailed context frame",
                    ));
                };
                Ok(ChatMessage {
                    role: message.role,
                    content: text.clone(),
                    name: message.name.clone(),
                    tool_call_id: message.tool_call_id.clone(),
                    tool_calls: message.tool_calls.clone(),
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(LlmContextFrame {
            messages,
            tools,
            tool_choice: self.tool_choice.clone(),
        })
    }
}
impl SharedContext {
    pub fn new(context: Context) -> Self {
        Self(Arc::new(Mutex::new(ContextState::new(context))))
    }
    pub fn new_rich(context: LlmContextData) -> Self {
        Self(Arc::new(Mutex::new(ContextState {
            data: context,
            originals: BTreeMap::new(),
            rich: true,
            lineage: next_lineage(),
        })))
    }
    pub fn snapshot(&self) -> Result<Context, ProcessorError> {
        self.lock()?.snapshot()
    }
    pub fn snapshot_rich(&self) -> Result<LlmContextData, ProcessorError> {
        let state = self.lock()?;
        state.require_lossless_rich_projection()?;
        Ok(state.data.clone())
    }
    pub fn append(&self, message: Message) -> Result<(), ProcessorError> {
        self.lock()?.append_native(message);
        Ok(())
    }
    /// Commit only when the resulting context has a lossless frame projection.
    pub(crate) fn append_with_frame(&self, message: Message) -> Result<FrameData, ProcessorError> {
        self.append_with_prepared_frame(message, Ok)
    }
    /// A private synchronous preparation step participates in the same rollback
    /// as context projection. It must not route, await, or invoke user code.
    pub(crate) fn append_with_prepared_frame<T>(
        &self,
        message: Message,
        prepare: impl FnOnce(FrameData) -> Result<T, ProcessorError>,
    ) -> Result<T, ProcessorError> {
        let mut state = self.lock()?;
        state.require_lossless_rich_projection()?;
        let index = state.messages.len();
        state.append_native(message);
        match state.frame_data().and_then(prepare) {
            Ok(frame) => Ok(frame),
            Err(error) => {
                state.messages.truncate(index);
                state.originals.remove(&index);
                Err(error)
            }
        }
    }
    pub fn append_rich(&self, message: ContextMessage) -> Result<(), ProcessorError> {
        self.lock()?.append_rich([message]);
        Ok(())
    }
    pub fn replace(&self, context: Context) -> Result<(), ProcessorError> {
        *self.lock()? = ContextState::new(context);
        Ok(())
    }
    pub fn replace_rich(&self, context: LlmContextData) -> Result<(), ProcessorError> {
        *self.lock()? = ContextState {
            data: context,
            originals: BTreeMap::new(),
            rich: true,
            lineage: next_lineage(),
        };
        Ok(())
    }
    /// Explicit legacy conversion; rejects content the text-only frame cannot carry.
    pub fn frame(&self) -> Result<LlmContextFrame, ProcessorError> {
        self.lock()?.legacy_frame()
    }
    /// Select a lossless frame representation, preserving rich tool sets and extras.
    pub fn frame_data(&self) -> Result<FrameData, ProcessorError> {
        self.lock()?.frame_data()
    }
    pub(crate) fn frame_data_at_lineage(
        &self,
        expected: Option<u64>,
    ) -> Result<FrameData, ProcessorError> {
        self.lock()?.frame_data_at_lineage(expected)
    }
    pub(crate) fn lock(&self) -> Result<MutexGuard<'_, ContextState>, ProcessorError> {
        self.0
            .lock()
            .map_err(|_| ProcessorError::fatal("conversation context lock poisoned"))
    }
}

/// Text-only compatibility conversion. Use SharedContext::frame_data for rich content.
pub fn context_to_frame(context: &Context) -> Result<LlmContextFrame, ProcessorError> {
    SharedContext::new(context.clone()).frame()
}
pub(crate) fn text_message(role: ChatRole, text: impl Into<String>) -> ContextMessage {
    ContextMessage {
        role,
        content: MessageContent::Text(text.into()),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    }
}
pub(crate) fn tool_message(id: &str, text: impl Into<String>) -> ContextMessage {
    ContextMessage {
        tool_call_id: Some(id.into()),
        ..text_message(ChatRole::Tool, text)
    }
}
