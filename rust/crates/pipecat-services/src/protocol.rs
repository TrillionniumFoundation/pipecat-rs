//! Stateful conversion of provider SSE records into portable completion events.
use crate::sse::SseEvent;
use crate::{
    Content, ContentPart, FinishReason, Message, Provider, Role, ServiceError, StreamEvent,
    ToolCall, Usage,
};
use serde_json::{json, Value};
use std::collections::{BTreeMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};

static RESPONSE_SEQUENCE: AtomicU64 = AtomicU64::new(1);
const MAX_CONTENT_BLOCKS: usize = 1024;
pub const MAX_HISTORY_BYTES: usize = 8 * 1024 * 1024;
pub const MAX_SIGNATURE_BYTES: usize = 1024 * 1024;
const MAX_TOOLS: usize = 128;
const MAX_TOOL_BYTES: usize = 4 * 1024 * 1024;

#[derive(Default)]
struct PartialTool {
    id: String,
    name: String,
    arguments: String,
    initial: Option<Value>,
    complete: bool,
}

pub struct ProtocolDecoder {
    provider: Provider,
    tools: BTreeMap<u32, PartialTool>,
    blocks: BTreeMap<u32, String>,
    usage: Option<Usage>,
    anthropic_uncached_input_tokens: u64,
    finish_reason: Option<FinishReason>,
    done: bool,
    response_sequence: u64,
    failed: bool,
    anthropic_history: BTreeMap<u32, Message>,
    gemini_history: Vec<Message>,
    gemini_signed: bool,
    history_bytes: usize,
    checkpoint_blocks: usize,
    nvidia: Option<crate::nvidia::NvidiaReasoning>,
    delivered_thought_depth: usize,
    response_progress_reported: bool,
}
impl ProtocolDecoder {
    pub fn new(provider: Provider) -> Self {
        Self {
            provider,
            tools: BTreeMap::new(),
            blocks: BTreeMap::new(),
            usage: None,
            anthropic_uncached_input_tokens: 0,
            finish_reason: None,
            done: false,
            response_sequence: RESPONSE_SEQUENCE.fetch_add(1, Ordering::Relaxed),
            failed: false,
            anthropic_history: BTreeMap::new(),
            gemini_history: vec![],
            gemini_signed: false,
            history_bytes: 0,
            checkpoint_blocks: 0,
            nvidia: None,
            delivered_thought_depth: 0,
            response_progress_reported: false,
        }
    }
    pub(crate) fn with_preset(
        provider: Provider,
        preset: Option<crate::presets::OpenAiPreset>,
    ) -> Self {
        let mut decoder = Self::new(provider);
        if preset == Some(crate::presets::OpenAiPreset::Nvidia) {
            decoder.nvidia = Some(crate::nvidia::NvidiaReasoning::default());
        }
        decoder
    }
    pub(crate) fn thought_boundary_delivered(&mut self, start: bool) {
        if start {
            self.delivered_thought_depth += 1;
        } else {
            self.delivered_thought_depth = self.delivered_thought_depth.saturating_sub(1);
        }
    }
    pub(crate) fn finalize_response(&mut self, completed: bool) -> Vec<StreamEvent> {
        let Some(reasoning) = self.nvidia.as_mut() else {
            return Vec::new();
        };
        let events = reasoning.finish(completed);
        if completed {
            events
        } else {
            // Cancellation can interrupt delivery halfway through one decoded
            // record. Close only starts that reached the bounded event channel.
            (0..std::mem::take(&mut self.delivered_thought_depth))
                .map(|_| StreamEvent::ThoughtEnd)
                .collect()
        }
    }
    pub fn is_done(&self) -> bool {
        self.done
    }
    pub fn usage(&self) -> Option<&Usage> {
        self.usage.as_ref()
    }
    pub fn finish_reason(&self) -> Option<&FinishReason> {
        self.finish_reason.as_ref()
    }

    pub fn decode(&mut self, event: SseEvent) -> Result<Vec<StreamEvent>, ServiceError> {
        if self.failed {
            return Err(ServiceError::Protocol(
                "decoder is poisoned by a prior stream failure".into(),
            ));
        }
        let reasoning = self.nvidia.clone();
        let result = self.decode_event(event);
        if result.is_err() {
            self.nvidia = reasoning;
            self.failed = true;
        }
        result
    }
    fn decode_event(&mut self, event: SseEvent) -> Result<Vec<StreamEvent>, ServiceError> {
        if self.done {
            return Ok(vec![]);
        }
        if event.data.trim().is_empty() {
            return Ok(vec![]);
        }
        if self.provider == Provider::OpenAi && event.data.trim() == "[DONE]" {
            self.done = true;
            return self.complete_tools();
        }
        let value: Value = serde_json::from_str(&event.data)
            .map_err(|error| ServiceError::Protocol(format!("invalid event JSON: {error}")))?;
        if !value.is_object() {
            return Err(ServiceError::Protocol(
                "provider event must be an object".into(),
            ));
        }
        if let Some(error) = value.get("error") {
            return Err(ServiceError::Provider {
                kind: error
                    .get("type")
                    .or_else(|| error.get("status"))
                    .and_then(Value::as_str)
                    .unwrap_or("unknown")
                    .into(),
                message: error
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("provider returned an error")
                    .into(),
            });
        }
        let mut events = match self.provider {
            Provider::OpenAi => self.openai(&value),
            Provider::Anthropic => self.anthropic(&value, event.event.as_deref()),
            Provider::Gemini => self.gemini(&value),
        }?;
        let total_tool_bytes: usize = self
            .tools
            .values()
            .map(|tool| {
                tool.id.len()
                    + tool.name.len()
                    + tool.arguments.len()
                    + tool
                        .initial
                        .as_ref()
                        .map_or(0, |input| input.to_string().len())
            })
            .sum();
        if total_tool_bytes > MAX_TOOL_BYTES {
            return Err(ServiceError::Protocol(
                "aggregate tool calls exceed size limit".into(),
            ));
        }
        // Match the reference adapters' first model event, including packets
        // without visible text. Commit only after the whole record is accepted.
        let model_progress = match self.provider {
            Provider::OpenAi => {
                self.nvidia.is_none()
                    && value
                        .get("choices")
                        .and_then(Value::as_array)
                        .is_some_and(|choices| !choices.is_empty())
            }
            Provider::Anthropic => matches!(
                value
                    .get("type")
                    .and_then(Value::as_str)
                    .or(event.event.as_deref()),
                Some("content_block_start" | "content_block_delta")
            ),
            Provider::Gemini => value
                .get("candidates")
                .and_then(Value::as_array)
                .is_some_and(|candidates| !candidates.is_empty()),
        };
        if model_progress && !self.response_progress_reported {
            self.response_progress_reported = true;
            events.insert(0, StreamEvent::ResponseProgress);
        }
        Ok(events)
    }
    /// A clean TCP EOF is insufficient evidence that a streamed response completed.
    pub fn end_of_stream(&mut self) -> Result<Vec<StreamEvent>, ServiceError> {
        if self.failed {
            return Err(ServiceError::Protocol(
                "decoder is poisoned by a prior stream failure".into(),
            ));
        }
        let result = self.finish_stream();
        if result.is_err() {
            self.failed = true;
        }
        result
    }
    fn finish_stream(&mut self) -> Result<Vec<StreamEvent>, ServiceError> {
        if self.done {
            return Ok(vec![]);
        }
        if self.provider == Provider::Gemini && self.finish_reason.is_some() {
            if !self.tools.is_empty() && self.finish_reason != Some(FinishReason::Stop) {
                return Err(ServiceError::Protocol(
                    "Gemini tool admission requires a successful STOP finish".into(),
                ));
            }
            let mut events = self.complete_tools()?;
            if self.gemini_signed || !self.tools.is_empty() {
                events.push(StreamEvent::GeminiHistory(std::mem::take(
                    &mut self.gemini_history,
                )));
            }
            self.done = true;
            return Ok(events);
        }
        Err(ServiceError::Protocol(
            "stream ended before the provider completion marker".into(),
        ))
    }
    fn tool(&mut self, index: u32) -> Result<&mut PartialTool, ServiceError> {
        if !self.tools.contains_key(&index) && self.tools.len() >= MAX_TOOLS {
            return Err(ServiceError::Protocol("too many tool calls".into()));
        }
        Ok(self.tools.entry(index).or_default())
    }
    fn complete_tool(&mut self, index: u32) -> Result<Option<StreamEvent>, ServiceError> {
        let tool = self
            .tools
            .get_mut(&index)
            .ok_or_else(|| ServiceError::Protocol("unknown tool index".into()))?;
        if tool.complete {
            return Ok(None);
        }
        if tool.id.is_empty() || tool.name.is_empty() {
            return Err(ServiceError::Protocol(
                "tool call missing id or name".into(),
            ));
        }
        let arguments = if tool.arguments.is_empty() {
            tool.initial
                .clone()
                .ok_or_else(|| ServiceError::Protocol("tool call missing JSON arguments".into()))?
        } else {
            serde_json::from_str(&tool.arguments).map_err(|error| {
                ServiceError::Protocol(format!("incomplete or invalid tool arguments: {error}"))
            })?
        };
        if !arguments.is_object() {
            return Err(ServiceError::Protocol(
                "tool arguments must be a JSON object".into(),
            ));
        }
        tool.complete = true;
        Ok(Some(StreamEvent::ToolCall(ToolCall {
            id: tool.id.clone(),
            name: tool.name.clone(),
            arguments,
        })))
    }
    fn complete_tools(&mut self) -> Result<Vec<StreamEvent>, ServiceError> {
        let mut ids = HashSet::new();
        for tool in self.tools.values() {
            if !ids.insert(tool.id.as_str()) {
                return Err(ServiceError::Protocol("duplicate tool call id".into()));
            }
        }
        let mut events = vec![];
        for index in self.tools.keys().copied().collect::<Vec<_>>() {
            if let Some(event) = self.complete_tool(index)? {
                events.push(event);
            }
        }
        Ok(events)
    }
    fn openai(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ServiceError> {
        if let Some(usage) = value.get("usage").filter(|u| u.is_object()) {
            let prompt = count(usage, "prompt_tokens");
            let completion = count(usage, "completion_tokens");
            self.usage = Some(Usage {
                prompt_tokens: prompt,
                completion_tokens: completion,
                total_tokens: usage
                    .get("total_tokens")
                    .and_then(Value::as_u64)
                    .unwrap_or(prompt.saturating_add(completion)),
                cache_read_input_tokens: usage
                    .pointer("/prompt_tokens_details/cached_tokens")
                    .and_then(Value::as_u64),
                cache_creation_input_tokens: usage
                    .pointer("/prompt_tokens_details/cache_write_tokens")
                    .and_then(Value::as_u64),
                reasoning_tokens: usage
                    .pointer("/completion_tokens_details/reasoning_tokens")
                    .and_then(Value::as_u64),
            });
        }
        let choices = value
            .get("choices")
            .and_then(Value::as_array)
            .ok_or_else(|| ServiceError::Protocol("OpenAI event missing choices".into()))?;
        let mut events = vec![];
        if !choices.is_empty() {
            if let Some(reasoning) = &mut self.nvidia {
                reasoning.progress(&mut events);
            }
        }
        for choice in choices {
            if choice.get("index").and_then(Value::as_u64).unwrap_or(0) != 0 {
                return Err(ServiceError::Unsupported(
                    "multiple OpenAI completion choices".into(),
                ));
            }
            if let Some(reason) = choice.get("finish_reason").and_then(Value::as_str) {
                self.finish_reason = Some(normalize_reason(reason));
            }
            let Some(delta) = choice.get("delta") else {
                continue;
            };
            if let Some(refusal) = delta
                .get("refusal")
                .and_then(Value::as_str)
                .filter(|s| !s.is_empty())
            {
                return Err(ServiceError::Provider {
                    kind: "refusal".into(),
                    message: refusal.into(),
                });
            }
            let content_events_start = events.len();
            let content = optional_string(delta, "content")?.filter(|s| !s.is_empty());
            if let Some(reasoning) = &mut self.nvidia {
                reasoning.delta(delta, content, &mut events)?;
            } else {
                if let Some(text) = content {
                    events.push(StreamEvent::TextDelta(text.into()));
                }
                if let Some(thought) = delta
                    .get("reasoning_content")
                    .or_else(|| delta.get("reasoning"))
                    .and_then(Value::as_str)
                    .filter(|s| !s.is_empty())
                {
                    events.push(StreamEvent::ThoughtDelta(thought.into()));
                }
            }
            if delta.get("function_call").is_some() {
                return Err(ServiceError::Unsupported(
                    "legacy function_call; use tools/tool_calls".into(),
                ));
            }
            if let Some(raw_calls) = delta.get("tool_calls").filter(|calls| !calls.is_null()) {
                let calls = raw_calls
                    .as_array()
                    .ok_or_else(|| ServiceError::Protocol("tool_calls must be an array".into()))?;
                for call in calls {
                    let index = index(call)?;
                    if optional_string(call, "type")?.is_some_and(|kind| kind != "function") {
                        return Err(ServiceError::Unsupported(
                            "non-function OpenAI tool calls".into(),
                        ));
                    }
                    let id = optional_string(call, "id")?;
                    let function = &call["function"];
                    let name = optional_string(function, "name")?;
                    let arguments = optional_string(function, "arguments")?.unwrap_or("");
                    let tool = self.tool(index)?;
                    if tool.complete {
                        return Err(ServiceError::Protocol("delta after completed tool".into()));
                    }
                    if let Some(id) = id {
                        tool.id.push_str(id);
                    }
                    if let Some(name) = name {
                        tool.name.push_str(name);
                    }
                    tool.arguments.push_str(arguments);
                    check_tool_size(tool)?;
                    events.push(StreamEvent::ToolCallDelta {
                        index,
                        id: id.map(str::to_string),
                        name: name.map(str::to_string),
                        arguments: arguments.into(),
                    });
                }
            }
            // The source selects audio only after tools and visible content.
            // NVIDIA filters content before that selection, so a held prefix
            // or hidden-only thought must not suppress a spoken transcript.
            // Inspect this choice only; do not feed audio into the think parser.
            let visible_content = events[content_events_start..]
                .iter()
                .any(|event| matches!(event, StreamEvent::TextDelta(text) if !text.is_empty()));
            let has_tools = delta
                .get("tool_calls")
                .and_then(Value::as_array)
                .is_some_and(|calls| !calls.is_empty());
            if !visible_content && !has_tools {
                if let Some(audio) = delta.get("audio").filter(|audio| !audio.is_null()) {
                    if !audio.is_object() {
                        return Err(ServiceError::Protocol(
                            "audio must be an object or null".into(),
                        ));
                    }
                    if let Some(text) =
                        optional_string(audio, "transcript")?.filter(|text| !text.is_empty())
                    {
                        events.push(StreamEvent::TextDelta(text.into()));
                    }
                }
            }
        }
        Ok(events)
    }
    fn anthropic(
        &mut self,
        value: &Value,
        event: Option<&str>,
    ) -> Result<Vec<StreamEvent>, ServiceError> {
        let kind = value
            .get("type")
            .and_then(Value::as_str)
            .or(event)
            .ok_or_else(|| ServiceError::Protocol("Anthropic event missing type".into()))?;
        let mut events = vec![];
        match kind {
            "ping" => {}
            "message_start" => {
                self.update_anthropic_usage(value.pointer("/message/usage"), true)?;
            }
            "content_block_start" => {
                let index = index(value)?;
                if self.anthropic_history.len() >= MAX_CONTENT_BLOCKS
                    || self.anthropic_history.contains_key(&index)
                {
                    return Err(ServiceError::Protocol(
                        "duplicate or excessive Anthropic content blocks".into(),
                    ));
                }
                let block = &value["content_block"];
                let kind = required_string(block, "type")?;
                if kind == "thinking" && self.blocks.values().any(|kind| kind == "thinking") {
                    return Err(ServiceError::Unsupported(
                        "overlapping Anthropic thinking blocks cannot share one thought stream"
                            .into(),
                    ));
                }
                let mut message = Message::text(Role::Assistant, "");
                match kind {
                    "tool_use" => {
                        let tool = self.tool(index)?;
                        tool.id = required_string(block, "id")?.into();
                        tool.name = required_string(block, "name")?.into();
                        tool.initial = block.get("input").cloned();
                        message.content = None;
                        events.push(StreamEvent::ToolCallDelta {
                            index,
                            id: Some(tool.id.clone()),
                            name: Some(tool.name.clone()),
                            arguments: String::new(),
                        });
                    }
                    "text" => {
                        let text = optional_block_string(block, "text")?;
                        self.retain_bytes(text.len())?;
                        message.content = Some(Content::Text(text.into()));
                        if !text.is_empty() {
                            events.push(StreamEvent::TextDelta(text.into()));
                        }
                    }
                    "redacted_thinking" => {
                        let data = required_string(block, "data")?;
                        if data.is_empty() {
                            return Err(ServiceError::Protocol(
                                "empty Anthropic redacted thinking".into(),
                            ));
                        }
                        self.retain_bytes(data.len())?;
                        message.content =
                            Some(Content::Parts(vec![ContentPart::RedactedThinking {
                                data: data.into(),
                            }]));
                    }
                    "thinking" => {
                        let text = optional_block_string(block, "thinking")?;
                        let signature = match block.get("signature") {
                            None => None,
                            Some(value) => Some(
                                value
                                    .as_str()
                                    .ok_or_else(|| {
                                        ServiceError::Protocol(
                                            "thinking signature must be a string".into(),
                                        )
                                    })?
                                    .to_owned(),
                            ),
                        };
                        if signature
                            .as_ref()
                            .is_some_and(|s| s.len() > MAX_SIGNATURE_BYTES)
                        {
                            return Err(ServiceError::Protocol(
                                "thinking signature exceeds limit".into(),
                            ));
                        }
                        self.retain_bytes(text.len() + signature.as_ref().map_or(0, String::len))?;
                        message.content = Some(Content::Parts(vec![ContentPart::Thinking {
                            text: text.into(),
                            signature,
                        }]));
                        events.push(StreamEvent::AnthropicThoughtStart);
                        if !text.is_empty() {
                            events.push(StreamEvent::ThoughtDelta(text.into()));
                        }
                    }
                    other => {
                        return Err(ServiceError::Unsupported(format!(
                            "Anthropic content block {other}"
                        )))
                    }
                }
                self.blocks.insert(index, kind.into());
                self.anthropic_history.insert(index, message);
            }
            "content_block_delta" => {
                let index = index(value)?;
                let block_kind = self.blocks.get(&index).ok_or_else(|| {
                    ServiceError::Protocol("delta without content block start".into())
                })?;
                let delta = &value["delta"];
                let delta_kind = required_string(delta, "type")?;
                let expected = match delta_kind {
                    "text_delta" => "text",
                    "thinking_delta" | "signature_delta" => "thinking",
                    "input_json_delta" => "tool_use",
                    other => {
                        return Err(ServiceError::Unsupported(format!(
                            "Anthropic delta {other}"
                        )))
                    }
                };
                if block_kind != expected {
                    return Err(ServiceError::Protocol(
                        "delta does not match content block type".into(),
                    ));
                }
                match delta_kind {
                    "input_json_delta" => {
                        let arguments = required_string(delta, "partial_json")?;
                        let tool = self.tools.get_mut(&index).ok_or_else(|| {
                            ServiceError::Protocol("tool delta for non-tool block".into())
                        })?;
                        tool.arguments.push_str(arguments);
                        check_tool_size(tool)?;
                        events.push(StreamEvent::ToolCallDelta {
                            index,
                            id: None,
                            name: None,
                            arguments: arguments.into(),
                        });
                    }
                    "text_delta" => {
                        let text = required_string(delta, "text")?;
                        self.retain_bytes(text.len())?;
                        let Some(Content::Text(retained)) = self
                            .anthropic_history
                            .get_mut(&index)
                            .and_then(|m| m.content.as_mut())
                        else {
                            return Err(ServiceError::Protocol("missing text history".into()));
                        };
                        retained.push_str(text);
                        events.push(StreamEvent::TextDelta(text.into()));
                    }
                    "thinking_delta" | "signature_delta" => {
                        let fragment = required_string(
                            delta,
                            if delta_kind == "thinking_delta" {
                                "thinking"
                            } else {
                                "signature"
                            },
                        )?;
                        self.retain_bytes(fragment.len())?;
                        let Some(Content::Parts(parts)) = self
                            .anthropic_history
                            .get_mut(&index)
                            .and_then(|m| m.content.as_mut())
                        else {
                            return Err(ServiceError::Protocol("missing thinking history".into()));
                        };
                        let Some(ContentPart::Thinking { text, signature }) = parts.first_mut()
                        else {
                            return Err(ServiceError::Protocol("missing thinking history".into()));
                        };
                        if delta_kind == "thinking_delta" {
                            if signature.as_ref().is_some_and(|s| !s.is_empty()) {
                                return Err(ServiceError::Protocol(
                                    "thinking text follows its signature".into(),
                                ));
                            }
                            text.push_str(fragment);
                            events.push(StreamEvent::ThoughtDelta(fragment.into()));
                        } else {
                            let signature = signature.get_or_insert_with(String::new);
                            if fragment.len() > MAX_SIGNATURE_BYTES.saturating_sub(signature.len())
                            {
                                return Err(ServiceError::Protocol(
                                    "thinking signature exceeds limit".into(),
                                ));
                            }
                            signature.push_str(fragment);
                        }
                    }
                    _ => unreachable!(),
                }
            }
            "content_block_stop" => {
                let index = index(value)?;
                let kind = self.blocks.remove(&index).ok_or_else(|| {
                    ServiceError::Protocol("stop for unknown content block".into())
                })?;
                if kind == "thinking"
                    && matches!(self.anthropic_history.get(&index).and_then(|m| m.content.as_ref()), Some(Content::Parts(parts)) if matches!(parts.first(), Some(ContentPart::Thinking { signature, .. }) if signature.as_ref().is_none_or(String::is_empty)))
                {
                    return Err(ServiceError::Protocol(
                        "Anthropic thinking block missing signature".into(),
                    ));
                }
                if kind == "tool_use" {
                    if let Some(event) = self.complete_tool(index)? {
                        if let StreamEvent::ToolCall(call) = &event {
                            self.retain_bytes(
                                call.id.len() + call.name.len() + call.arguments.to_string().len(),
                            )?;
                            self.anthropic_history
                                .get_mut(&index)
                                .expect("retained tool block")
                                .tool_calls
                                .push(call.clone());
                        }
                        events.push(event);
                    }
                }
                let prefix: Vec<_> = self
                    .anthropic_history
                    .iter()
                    .enumerate()
                    .take_while(|(expected, (index, _))| {
                        *expected as u32 == **index && !self.blocks.contains_key(index)
                    })
                    .map(|(_, (_, message))| message.clone())
                    .collect();
                if prefix.len() > self.checkpoint_blocks && prefix.iter().any(has_thinking) {
                    self.checkpoint_blocks = prefix.len();
                    events.push(StreamEvent::AnthropicCheckpoint(prefix));
                }
                if kind == "thinking" {
                    events.push(StreamEvent::AnthropicThoughtEnd);
                }
            }
            "message_delta" => {
                // Reject a malformed snapshot before changing either usage or reason.
                self.update_anthropic_usage(value.get("usage"), false)?;
                if let Some(reason) = value.pointer("/delta/stop_reason").and_then(Value::as_str) {
                    self.finish_reason = Some(normalize_reason(reason));
                }
            }
            "message_stop" => {
                if !self.blocks.is_empty() {
                    return Err(ServiceError::Protocol(
                        "message stopped with active content blocks".into(),
                    ));
                }
                if self
                    .anthropic_history
                    .keys()
                    .enumerate()
                    .any(|(expected, index)| expected as u32 != *index)
                {
                    return Err(ServiceError::Protocol(
                        "noncontiguous Anthropic content indexes".into(),
                    ));
                }
                events.extend(self.complete_tools()?);
                if self.anthropic_history.values().any(has_thinking) {
                    events.push(StreamEvent::AnthropicHistory(
                        self.anthropic_history.values().cloned().collect(),
                    ));
                }
                self.done = true;
            }
            "error" => {
                return Err(ServiceError::Provider {
                    kind: "unknown".into(),
                    message: "Anthropic stream error".into(),
                })
            }
            // Providers may add events. Unknown events are ignored as documented.
            _ => {}
        }
        Ok(events)
    }
    fn update_anthropic_usage(
        &mut self,
        value: Option<&Value>,
        initialize: bool,
    ) -> Result<(), ServiceError> {
        let Some(value) = value.filter(|value| !value.is_null()) else {
            return Ok(());
        };
        let object = value
            .as_object()
            .ok_or_else(|| ServiceError::Protocol("Anthropic usage must be an object".into()))?;
        let optional_count = |key: &str| -> Result<Option<u64>, ServiceError> {
            match object.get(key) {
                None | Some(Value::Null) => Ok(None),
                Some(value) => value.as_u64().map(Some).ok_or_else(|| {
                    ServiceError::Protocol(format!("Anthropic usage {key} must be a u64"))
                }),
            }
        };
        // Validate the complete record before mutating any cumulative state.
        let input = optional_count("input_tokens")?;
        let output = optional_count("output_tokens")?;
        let read = optional_count("cache_read_input_tokens")?;
        let write = optional_count("cache_creation_input_tokens")?;
        let reasoning = match object.get("output_tokens_details") {
            None | Some(Value::Null) => None,
            Some(details) => Some(
                details
                    .as_object()
                    .and_then(|details| details.get("thinking_tokens"))
                    .and_then(Value::as_u64)
                    .ok_or_else(|| {
                        ServiceError::Protocol(
                            "Anthropic output_tokens_details requires u64 thinking_tokens".into(),
                        )
                    })?,
            ),
        };
        if !initialize
            && self.usage.is_none()
            && [input, output, read, write, reasoning]
                .iter()
                .all(Option::is_none)
        {
            return Ok(());
        }
        // Keep uncached input separately: subtraction cannot undo a saturated
        // gross prompt when a later cumulative cache count decreases.
        let uncached = input.unwrap_or(self.anthropic_uncached_input_tokens);
        let mut usage = self.usage.clone().unwrap_or_default();
        usage.completion_tokens = output.unwrap_or(usage.completion_tokens);
        usage.cache_read_input_tokens = read.or(usage.cache_read_input_tokens);
        usage.cache_creation_input_tokens = write.or(usage.cache_creation_input_tokens);
        usage.reasoning_tokens = reasoning.or(usage.reasoning_tokens);
        usage.prompt_tokens = uncached
            .saturating_add(usage.cache_read_input_tokens.unwrap_or(0))
            .saturating_add(usage.cache_creation_input_tokens.unwrap_or(0));
        usage.total_tokens = usage.prompt_tokens.saturating_add(usage.completion_tokens);
        self.anthropic_uncached_input_tokens = uncached;
        self.usage = Some(usage);
        Ok(())
    }
    fn retain_bytes(&mut self, count: usize) -> Result<(), ServiceError> {
        self.history_bytes = self
            .history_bytes
            .checked_add(count)
            .filter(|count| *count <= MAX_HISTORY_BYTES)
            .ok_or_else(|| ServiceError::Protocol("Anthropic history exceeds size limit".into()))?;
        Ok(())
    }
    fn gemini(&mut self, value: &Value) -> Result<Vec<StreamEvent>, ServiceError> {
        if value.get("candidates").is_some_and(|c| !c.is_array()) {
            return Err(ServiceError::Protocol("invalid Gemini candidates".into()));
        }
        if let Some(usage) = value.get("usageMetadata") {
            let prompt = count(usage, "promptTokenCount")
                .saturating_add(count(usage, "toolUsePromptTokenCount"));
            let reasoning = usage.get("thoughtsTokenCount").and_then(Value::as_u64);
            let completion =
                count(usage, "candidatesTokenCount").saturating_add(reasoning.unwrap_or(0));
            self.usage = Some(Usage {
                prompt_tokens: prompt,
                completion_tokens: completion,
                total_tokens: usage
                    .get("totalTokenCount")
                    .and_then(Value::as_u64)
                    .unwrap_or(prompt.saturating_add(completion)),
                reasoning_tokens: reasoning,
                cache_read_input_tokens: usage
                    .get("cachedContentTokenCount")
                    .and_then(Value::as_u64),
                cache_creation_input_tokens: None,
            });
        }
        if let Some(reason) = value
            .pointer("/promptFeedback/blockReason")
            .and_then(Value::as_str)
        {
            return Err(ServiceError::Provider {
                kind: reason.into(),
                message: "Gemini blocked the prompt".into(),
            });
        }
        let mut events = vec![];
        if let Some(candidates) = value.get("candidates").and_then(Value::as_array) {
            if candidates.len() > 1 {
                return Err(ServiceError::Unsupported(
                    "multiple Gemini candidates".into(),
                ));
            }
            for candidate in candidates {
                if !candidate.is_object()
                    || candidate
                        .get("index")
                        .is_some_and(|index| index.as_u64().is_none())
                {
                    return Err(ServiceError::Protocol("invalid Gemini candidate".into()));
                }
                if candidate.get("index").and_then(Value::as_u64).unwrap_or(0) != 0 {
                    return Err(ServiceError::Unsupported(
                        "multiple Gemini candidates".into(),
                    ));
                }
                if self.finish_reason.is_some() && candidate.get("content").is_some() {
                    return Err(ServiceError::Protocol(
                        "Gemini content after finish marker".into(),
                    ));
                }
                if let Some(reason) = optional_string(candidate, "finishReason")? {
                    if self.finish_reason.is_some() {
                        return Err(ServiceError::Protocol(
                            "duplicate Gemini finish marker".into(),
                        ));
                    }
                    let normalized = normalize_reason(reason);
                    self.finish_reason =
                        Some(if normalized == FinishReason::Stop && reason != "STOP" {
                            FinishReason::Other(reason.into())
                        } else {
                            normalized
                        });
                }
                if candidate.get("content").is_some_and(|content| {
                    !content.is_object()
                        || content.get("parts").is_none_or(|parts| !parts.is_array())
                }) {
                    return Err(ServiceError::Protocol(
                        "invalid Gemini content Parts".into(),
                    ));
                }
                if let Some(parts) = candidate
                    .pointer("/content/parts")
                    .and_then(Value::as_array)
                {
                    for part in parts {
                        if !part.is_object()
                            || (part.get("text").is_none() && part.get("functionCall").is_none())
                        {
                            return Err(ServiceError::Unsupported(
                                "unsupported or malformed Gemini Part".into(),
                            ));
                        }
                        if self.gemini_history.len() + self.tools.len() >= MAX_CONTENT_BLOCKS {
                            return Err(ServiceError::Protocol("too many Gemini Parts".into()));
                        }
                        let signature = match part.get("thoughtSignature") {
                            None => None,
                            Some(Value::String(signature))
                                if !signature.is_empty()
                                    && signature.len() <= MAX_SIGNATURE_BYTES =>
                            {
                                Some(signature.clone())
                            }
                            Some(_) => {
                                return Err(ServiceError::Protocol(
                                    "invalid or oversized Gemini thought signature".into(),
                                ))
                            }
                        };
                        let thought = match part.get("thought") {
                            None => false,
                            Some(Value::Bool(thought)) => *thought,
                            Some(_) => {
                                return Err(ServiceError::Protocol(
                                    "invalid Gemini thought flag".into(),
                                ))
                            }
                        };
                        if part.get("text").is_some() && part.get("functionCall").is_some() {
                            return Err(ServiceError::Protocol("ambiguous Gemini Part".into()));
                        }
                        if let Some(text) = part.get("text") {
                            let text = text.as_str().ok_or_else(|| {
                                ServiceError::Protocol("invalid Gemini text Part".into())
                            })?;
                            self.retain_bytes(
                                text.len() + signature.as_ref().map_or(0, String::len),
                            )?;
                            let mut message = Message::text(Role::Assistant, "");
                            message.content = Some(Content::Parts(vec![ContentPart::GeminiText {
                                text: text.into(),
                                thought,
                                signature: signature.clone(),
                                incomplete: false,
                            }]));
                            self.gemini_history.push(message);
                            if !text.is_empty() {
                                events.push(if thought {
                                    StreamEvent::ThoughtDelta(text.into())
                                } else {
                                    StreamEvent::TextDelta(text.into())
                                });
                            }
                        } else if let Some(call) = part.get("functionCall") {
                            if thought {
                                return Err(ServiceError::Protocol(
                                    "Gemini function call marked as thought text".into(),
                                ));
                            }
                            let index = self.tools.len() as u32;
                            let id = optional_string(call, "id")?
                                .map(str::to_owned)
                                .unwrap_or_else(|| {
                                    format!("gemini-{}-{index}", self.response_sequence)
                                });
                            if self.tools.values().any(|tool| tool.id == id) {
                                return Err(ServiceError::Protocol(
                                    "duplicate Gemini tool call id".into(),
                                ));
                            }
                            let tool = self.tool(index)?;
                            tool.id = id;
                            tool.name = required_string(call, "name")?.into();
                            if [&tool.id, &tool.name].iter().any(|identity| {
                                identity.is_empty()
                                    || identity
                                        .chars()
                                        .any(|ch| ch.is_whitespace() || ch.is_control())
                            }) {
                                return Err(ServiceError::Protocol(
                                    "invalid Gemini tool identity".into(),
                                ));
                            }
                            tool.initial =
                                Some(call.get("args").cloned().unwrap_or_else(|| json!({})));
                            if let Some(event) = self.complete_tool(index)? {
                                let StreamEvent::ToolCall(call) = &event else {
                                    unreachable!()
                                };
                                self.retain_bytes(
                                    2 * call.id.len()
                                        + call.name.len()
                                        + call.arguments.to_string().len()
                                        + signature.as_ref().map_or(0, String::len),
                                )?;
                                let mut message = Message::assistant_tool_calls(vec![call.clone()]);
                                message.content =
                                    Some(Content::Parts(vec![ContentPart::GeminiToolMetadata {
                                        tool_call_id: call.id.clone(),
                                        signature: signature.clone(),
                                    }]));
                                self.gemini_history.push(message);
                                events.push(event);
                            }
                        } else if signature.is_some() {
                            return Err(ServiceError::Unsupported(
                                "Gemini signature on unsupported Part type".into(),
                            ));
                        }
                        self.gemini_signed |= signature.is_some();
                        if part.get("inlineData").is_some()
                            || part.get("fileData").is_some()
                            || part.get("executableCode").is_some()
                            || part.get("codeExecutionResult").is_some()
                        {
                            return Err(ServiceError::Unsupported(
                                "Gemini multimodal output and code execution".into(),
                            ));
                        }
                    }
                }
            }
        }
        if self.gemini_signed && self.gemini_history.len() > self.checkpoint_blocks {
            self.checkpoint_blocks = self.gemini_history.len();
            events.push(StreamEvent::GeminiCheckpoint(self.gemini_history.clone()));
        }
        Ok(events)
    }
}
fn count(value: &Value, key: &str) -> u64 {
    value.get(key).and_then(Value::as_u64).unwrap_or(0)
}
fn optional_string<'a>(value: &'a Value, key: &str) -> Result<Option<&'a str>, ServiceError> {
    match value.get(key) {
        None | Some(Value::Null) => Ok(None),
        Some(Value::String(text)) => Ok(Some(text)),
        Some(_) => Err(ServiceError::Protocol(format!(
            "{key} must be a string or null"
        ))),
    }
}
fn required_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ServiceError> {
    value
        .get(key)
        .and_then(Value::as_str)
        .ok_or_else(|| ServiceError::Protocol(format!("missing string {key}")))
}
fn index(value: &Value) -> Result<u32, ServiceError> {
    value
        .get("index")
        .and_then(Value::as_u64)
        .and_then(|n| n.try_into().ok())
        .ok_or_else(|| ServiceError::Protocol("missing or invalid stream index".into()))
}
fn check_tool_size(tool: &PartialTool) -> Result<(), ServiceError> {
    if tool.id.len() + tool.name.len() + tool.arguments.len() > MAX_TOOL_BYTES {
        Err(ServiceError::Protocol(
            "tool call exceeds size limit".into(),
        ))
    } else {
        Ok(())
    }
}
fn normalize_reason(reason: &str) -> FinishReason {
    match reason {
        "stop" | "end_turn" | "stop_sequence" | "STOP" => FinishReason::Stop,
        "tool_calls" | "tool_use" => FinishReason::ToolCalls,
        "length" | "max_tokens" | "MAX_TOKENS" => FinishReason::Length,
        "content_filter" | "SAFETY" | "PROHIBITED_CONTENT" | "RECITATION" | "BLOCKLIST"
        | "SPII" => FinishReason::ContentFilter,
        other => FinishReason::Other(other.into()),
    }
}

fn optional_block_string<'a>(value: &'a Value, key: &str) -> Result<&'a str, ServiceError> {
    match value.get(key) {
        None => Ok(""),
        Some(value) => value
            .as_str()
            .ok_or_else(|| ServiceError::Protocol(format!("{key} must be a string"))),
    }
}
fn has_thinking(message: &Message) -> bool {
    matches!(&message.content, Some(Content::Parts(parts)) if parts.iter().any(|part| matches!(part, ContentPart::Thinking { .. } | ContentPart::RedactedThinking { .. })))
}

#[cfg(test)]
mod openai_audio_transcript_tests {
    use super::*;

    fn record(
        decoder: &mut ProtocolDecoder,
        value: Value,
    ) -> Result<Vec<StreamEvent>, ServiceError> {
        decoder.decode(SseEvent {
            event: None,
            data: value.to_string(),
        })
    }
    fn delta(
        decoder: &mut ProtocolDecoder,
        value: Value,
    ) -> Result<Vec<StreamEvent>, ServiceError> {
        record(decoder, json!({"choices":[{"index":0,"delta":value}]}))
    }
    fn done(decoder: &mut ProtocolDecoder) -> Result<Vec<StreamEvent>, ServiceError> {
        decoder.decode(SseEvent {
            event: None,
            data: "[DONE]".into(),
        })
    }
    fn tool() -> Value {
        json!({"index":0,"id":"call-audio","type":"function","function":{"name":"lookup","arguments":"{}"}})
    }

    #[test]
    fn audio_transcript_fallback_ignores_partials_and_preserves_text_verbatim() {
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        for (i, value) in [
            json!({}),
            json!({"audio":null}),
            json!({"audio":{}}),
            json!({"audio":{"id":"ignored","data":"not-decoded","expires_at":12}}),
            json!({"audio":{"transcript":null}}),
            json!({"audio":{"transcript":""}}),
        ]
        .into_iter()
        .enumerate()
        {
            assert_eq!(
                delta(&mut decoder, value).unwrap(),
                if i == 0 {
                    vec![StreamEvent::ResponseProgress]
                } else {
                    vec![]
                }
            );
        }
        for text in [
            " ",
            "你好🙂",
            "<think>literal spoken words</think>",
            " tail\n",
        ] {
            assert_eq!(delta(&mut decoder, json!({"content":"","tool_calls":[],"audio":{"transcript":text,"data":"ignored","id":"ignored"}})).unwrap(), vec![StreamEvent::TextDelta(text.into())]);
        }
        assert!(done(&mut decoder).unwrap().is_empty());
    }

    #[test]
    fn selected_audio_wrong_types_fail_and_poison_without_coercion() {
        let mut values = vec![];
        for audio in [json!(false), json!(0), json!(""), json!([]), json!([1])] {
            values.push(json!({"audio":audio}));
        }
        for transcript in [json!(false), json!(0), json!(true), json!([]), json!({})] {
            values.push(json!({"audio":{"transcript":transcript}}));
        }
        for value in values {
            let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
            assert!(matches!(
                delta(&mut decoder, value),
                Err(ServiceError::Protocol(_))
            ));
            assert!(
                matches!(delta(&mut decoder, json!({"audio":{"transcript":"late"}})), Err(ServiceError::Protocol(message)) if message.contains("poisoned"))
            );
            assert!(matches!(done(&mut decoder), Err(ServiceError::Protocol(_))));
        }
    }

    #[test]
    fn audio_priority_is_choice_local_without_changing_content_tool_coemission() {
        for content in ["visible", " "] {
            let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
            assert_eq!(
                delta(&mut decoder, json!({"content":content,"audio":false})).unwrap(),
                vec![
                    StreamEvent::ResponseProgress,
                    StreamEvent::TextDelta(content.into())
                ]
            );
        }
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        let events = delta(
            &mut decoder,
            json!({"content":"visible","tool_calls":[tool()],"audio":{"transcript":[]}}),
        )
        .unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], StreamEvent::ResponseProgress);
        assert_eq!(events[1], StreamEvent::TextDelta("visible".into()));
        assert!(
            matches!(&events[2], StreamEvent::ToolCallDelta { name: Some(name), .. } if name == "lookup")
        );
        assert!(matches!(
            done(&mut decoder).unwrap().as_slice(),
            [StreamEvent::ToolCall(_)]
        ));
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        let events = delta(&mut decoder, json!({"tool_calls":[tool()],"audio":false})).unwrap();
        assert!(matches!(
            events.as_slice(),
            [
                StreamEvent::ResponseProgress,
                StreamEvent::ToolCallDelta { .. }
            ]
        ));
        for value in [
            json!({"tool_calls":{},"audio":{"transcript":"hidden"}}),
            json!({"content":false,"audio":{"transcript":"hidden"}}),
        ] {
            assert!(matches!(
                delta(&mut ProtocolDecoder::new(Provider::OpenAi), value),
                Err(ServiceError::Protocol(_))
            ));
        }
        let mut reasoning = ProtocolDecoder::new(Provider::OpenAi);
        assert_eq!(
            delta(
                &mut reasoning,
                json!({"reasoning_content":"plan","audio":{"transcript":"spoken"}})
            )
            .unwrap(),
            vec![
                StreamEvent::ResponseProgress,
                StreamEvent::ThoughtDelta("plan".into()),
                StreamEvent::TextDelta("spoken".into())
            ]
        );
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        assert_eq!(record(&mut decoder, json!({"choices":[
            {"index":0,"delta":{"content":"first"}},
            {"index":0,"delta":{"content":null,"tool_calls":[],"audio":{"transcript":"second"}}}
        ]})).unwrap(), vec![StreamEvent::ResponseProgress, StreamEvent::TextDelta("first".into()), StreamEvent::TextDelta("second".into())]);
    }

    #[test]
    fn audio_terminal_chunk_and_usage_keep_existing_done_and_eof_contracts() {
        for reason in ["stop", "length"] {
            let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
            assert_eq!(record(&mut decoder, json!({"choices":[{"index":0,"finish_reason":reason,"delta":{"audio":{"transcript":"tail"}}}]})).unwrap(), vec![StreamEvent::ResponseProgress, StreamEvent::TextDelta("tail".into())]);
            assert!(!decoder.is_done());
            assert_eq!(
                delta(&mut decoder, json!({"audio":{"transcript":"before DONE"}})).unwrap(),
                vec![StreamEvent::TextDelta("before DONE".into())]
            );
            for tokens in [2, 5] {
                assert!(record(&mut decoder, json!({"choices":[],"usage":{"prompt_tokens":1,"completion_tokens":tokens,"total_tokens":tokens+1}})).unwrap().is_empty());
            }
            assert_eq!(decoder.usage().unwrap().completion_tokens, 5);
            assert!(done(&mut decoder).unwrap().is_empty());
            assert!(decoder.is_done());
            assert!(delta(&mut decoder, json!({"audio":false}))
                .unwrap()
                .is_empty());
            assert!(decoder.end_of_stream().unwrap().is_empty());
        }
        let mut decoder = ProtocolDecoder::new(Provider::OpenAi);
        delta(&mut decoder, json!({"audio":{"transcript":"partial"}})).unwrap();
        assert!(matches!(
            decoder.end_of_stream(),
            Err(ServiceError::Protocol(_))
        ));
    }

    #[test]
    fn nvidia_audio_bypasses_think_filter_without_consuming_held_content_or_boundaries() {
        let mut decoder = ProtocolDecoder::with_preset(
            Provider::OpenAi,
            Some(crate::presets::OpenAiPreset::Nvidia),
        );
        assert_eq!(
            delta(
                &mut decoder,
                json!({"content":"<thi","audio":{"transcript":"<think>literal</think>"}})
            )
            .unwrap(),
            vec![
                StreamEvent::ResponseProgress,
                StreamEvent::TextDelta("<think>literal</think>".into())
            ]
        );
        assert_eq!(
            delta(
                &mut decoder,
                json!({"content":"nk>hidden</think>","audio":{"transcript":"spoken"}})
            )
            .unwrap(),
            vec![
                StreamEvent::ThoughtStart,
                StreamEvent::ThoughtDelta("hidden".into()),
                StreamEvent::ThoughtEnd,
                StreamEvent::TextDelta("spoken".into())
            ]
        );
        assert_eq!(
            delta(&mut decoder, json!({"content":"visible","audio":false})).unwrap(),
            vec![StreamEvent::TextDelta("visible".into())]
        );
        done(&mut decoder).unwrap();
        assert!(decoder.finalize_response(true).is_empty());

        let mut decoder = ProtocolDecoder::with_preset(
            Provider::OpenAi,
            Some(crate::presets::OpenAiPreset::Nvidia),
        );
        assert_eq!(
            delta(
                &mut decoder,
                json!({"content":"<thi","audio":{"transcript":"first"}})
            )
            .unwrap(),
            vec![
                StreamEvent::ResponseProgress,
                StreamEvent::TextDelta("first".into())
            ]
        );
        done(&mut decoder).unwrap();
        assert_eq!(
            decoder.finalize_response(true),
            vec![StreamEvent::TextDelta("<thi".into())]
        );

        let mut decoder = ProtocolDecoder::with_preset(
            Provider::OpenAi,
            Some(crate::presets::OpenAiPreset::Nvidia),
        );
        assert_eq!(
            delta(
                &mut decoder,
                json!({"reasoning_content":"plan","audio":{"transcript":"spoken"}})
            )
            .unwrap(),
            vec![
                StreamEvent::ResponseProgress,
                StreamEvent::ThoughtStart,
                StreamEvent::ThoughtDelta("plan".into()),
                StreamEvent::TextDelta("spoken".into())
            ]
        );
        done(&mut decoder).unwrap();
        assert_eq!(
            decoder.finalize_response(true),
            vec![StreamEvent::ThoughtEnd]
        );

        let mut decoder = ProtocolDecoder::with_preset(
            Provider::OpenAi,
            Some(crate::presets::OpenAiPreset::Nvidia),
        );
        assert_eq!(
            delta(
                &mut decoder,
                json!({"content":"<think>hidden</think>visible rest","audio":false})
            )
            .unwrap(),
            vec![
                StreamEvent::ResponseProgress,
                StreamEvent::ThoughtStart,
                StreamEvent::ThoughtDelta("hidden".into()),
                StreamEvent::ThoughtEnd,
                StreamEvent::TextDelta("visible rest".into())
            ]
        );

        let mut decoder = ProtocolDecoder::with_preset(
            Provider::OpenAi,
            Some(crate::presets::OpenAiPreset::Nvidia),
        );
        delta(
            &mut decoder,
            json!({"content":"<thi","audio":{"transcript":"already delivered"}}),
        )
        .unwrap();
        // Cancellation discards the held content prefix. Audio neither flushes
        // it early nor creates a new thought boundary to close.
        assert!(decoder.finalize_response(false).is_empty());
    }
}
