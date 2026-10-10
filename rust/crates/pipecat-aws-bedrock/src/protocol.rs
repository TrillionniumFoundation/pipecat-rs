//! Bounded ConverseStream state machine over CRC-checked AWS EventStream records.
use pipecat_aws_common::eventstream;
use pipecat_services::{FinishReason, ServiceError, ToolCall, Usage};
use serde_json::{json, Value};
use std::collections::{BTreeMap, BTreeSet};
#[derive(Clone, PartialEq)]
pub enum BedrockBlock {
    Text(String),
    Reasoning {
        text: String,
        signature: Option<String>,
    },
    Redacted(Vec<u8>),
    Tool(ToolCall),
}
impl std::fmt::Debug for BedrockBlock {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Text(_) => "BedrockText([redacted])",
            Self::Reasoning { .. } => "BedrockReasoning([redacted])",
            Self::Redacted(_) => "BedrockRedacted([redacted])",
            Self::Tool(_) => "BedrockTool([redacted])",
        })
    }
}
#[derive(Debug, Clone, PartialEq)]
pub enum BedrockEvent {
    Started,
    ContentStarted,
    TextDelta(String),
    ThoughtDelta(String),
    Checkpoint(Vec<BedrockBlock>),
    Usage(Usage),
    Completed {
        blocks: Vec<BedrockBlock>,
        reason: FinishReason,
    },
}
#[derive(Default)]
enum Pending {
    #[default]
    Empty,
    Text(String),
    Reasoning {
        text: String,
        signature: String,
    },
    Redacted(Vec<u8>),
    Tool {
        id: String,
        name: String,
        input: String,
    },
}
pub struct ConverseDecoder {
    max_event_bytes: usize,
    max_output_bytes: usize,
    retained: usize,
    started: bool,
    content_seen: bool,
    stopped: Option<FinishReason>,
    metadata: bool,
    pending: BTreeMap<u32, Pending>,
    closed: BTreeMap<u32, BedrockBlock>,
    ids: BTreeSet<String>,
    poisoned: bool,
}
fn error() -> ServiceError {
    ServiceError::Protocol("invalid Bedrock ConverseStream sequence or payload".into())
}
fn string<'a>(v: &'a Value, key: &str) -> Result<&'a str, ServiceError> {
    v.get(key).and_then(Value::as_str).ok_or_else(error)
}
fn index(v: &Value) -> Result<u32, ServiceError> {
    v.get("contentBlockIndex")
        .and_then(Value::as_u64)
        .and_then(|n| u32::try_from(n).ok())
        .filter(|n| *n < 1024)
        .ok_or_else(error)
}
impl ConverseDecoder {
    pub fn new(max_event_bytes: usize, max_output_bytes: usize) -> Self {
        Self {
            max_event_bytes,
            max_output_bytes,
            retained: 0,
            started: false,
            content_seen: false,
            stopped: None,
            metadata: false,
            pending: BTreeMap::new(),
            closed: BTreeMap::new(),
            ids: BTreeSet::new(),
            poisoned: false,
        }
    }
    pub fn event(&mut self, bytes: &[u8]) -> Result<Vec<BedrockEvent>, ServiceError> {
        if self.poisoned {
            return Err(error());
        }
        let result = self.event_inner(bytes);
        if result.is_err() {
            self.poisoned = true;
        }
        result
    }
    fn event_inner(&mut self, bytes: &[u8]) -> Result<Vec<BedrockEvent>, ServiceError> {
        let event = eventstream::decode(bytes, self.max_event_bytes).map_err(|_| error())?;
        if event.header(":message-type") == Some("exception")
            || event.header(":message-type") == Some("error")
        {
            return Err(ServiceError::Provider {
                kind: match event
                    .header(":exception-type")
                    .or_else(|| event.header(":error-code"))
                {
                    Some("validationException") => "validationException",
                    Some("throttlingException") => "throttlingException",
                    Some("serviceUnavailableException") => "serviceUnavailableException",
                    Some("modelStreamErrorException") => "modelStreamErrorException",
                    Some("internalServerException") => "internalServerException",
                    _ => "BedrockException",
                }
                .into(),
                message: "Bedrock rejected the stream".into(),
            });
        }
        if event.header(":message-type") != Some("event")
            || event.header(":content-type") != Some("application/json")
        {
            return Err(error());
        }
        let kind = event.header(":event-type").ok_or_else(error)?;
        let value: Value = serde_json::from_slice(event.payload).map_err(|_| error())?;
        if !value.is_object() {
            return Err(error());
        }
        let mut output = vec![];
        if matches!(kind, "contentBlockStart" | "contentBlockDelta") && !self.content_seen {
            self.content_seen = true;
            output.push(BedrockEvent::ContentStarted);
        }
        match kind {
            "messageStart" => {
                if self.started || self.stopped.is_some() || string(&value, "role")? != "assistant"
                {
                    return Err(error());
                }
                self.started = true;
                output.push(BedrockEvent::Started);
            }
            "contentBlockStart" => {
                self.require_content()?;
                let i = index(&value)?;
                if self.pending.contains_key(&i) || self.closed.contains_key(&i) {
                    return Err(error());
                }
                let start = value
                    .get("start")
                    .and_then(Value::as_object)
                    .ok_or_else(error)?;
                let p = if start.is_empty() {
                    Pending::Empty
                } else if start.len() == 1 {
                    let tool = start.get("toolUse").ok_or_else(error)?;
                    let id = string(tool, "toolUseId")?;
                    let name = string(tool, "name")?;
                    if !valid_identity(id) || !valid_identity(name) || !self.ids.insert(id.into()) {
                        return Err(error());
                    }
                    self.charge(id.len() + name.len())?;
                    Pending::Tool {
                        id: id.into(),
                        name: name.into(),
                        input: String::new(),
                    }
                } else {
                    return Err(error());
                };
                self.pending.insert(i, p);
            }
            "contentBlockDelta" => {
                self.require_content()?;
                let i = index(&value)?;
                if self.closed.contains_key(&i) {
                    return Err(error());
                }
                let delta = value
                    .get("delta")
                    .and_then(Value::as_object)
                    .filter(|v| v.len() == 1)
                    .ok_or_else(error)?;
                let mut p = self.pending.remove(&i).unwrap_or_default();
                if let Some(t) = delta.get("text").and_then(Value::as_str) {
                    self.charge(t.len())?;
                    if matches!(p, Pending::Empty) {
                        p = Pending::Text(String::new());
                    }
                    match &mut p {
                        Pending::Text(text) => text.push_str(t),
                        _ => return Err(error()),
                    };
                    output.push(BedrockEvent::TextDelta(t.into()));
                } else if let Some(tool) = delta.get("toolUse") {
                    let input = string(tool, "input")?;
                    self.charge(input.len())?;
                    match &mut p {
                        Pending::Tool { input: s, .. } => s.push_str(input),
                        _ => return Err(error()),
                    };
                } else if let Some(reasoning) = delta
                    .get("reasoningContent")
                    .and_then(Value::as_object)
                    .filter(|v| v.len() == 1)
                {
                    if let Some(redacted) = reasoning.get("redactedContent").and_then(Value::as_str)
                    {
                        use base64::Engine;
                        let bytes = base64::engine::general_purpose::STANDARD
                            .decode(redacted)
                            .map_err(|_| error())?;
                        self.charge(bytes.len())?;
                        if matches!(p, Pending::Empty) {
                            p = Pending::Redacted(vec![]);
                        }
                        match &mut p {
                            Pending::Redacted(v) => v.extend(bytes),
                            _ => return Err(error()),
                        };
                    } else {
                        if matches!(p, Pending::Empty) {
                            p = Pending::Reasoning {
                                text: String::new(),
                                signature: String::new(),
                            };
                        }
                        match &mut p {
                            Pending::Reasoning { text, signature } => {
                                if let Some(t) = reasoning.get("text").and_then(Value::as_str) {
                                    if !signature.is_empty() {
                                        return Err(error());
                                    }
                                    self.charge(t.len())?;
                                    text.push_str(t);
                                    output.push(BedrockEvent::ThoughtDelta(t.into()));
                                } else if let Some(s) =
                                    reasoning.get("signature").and_then(Value::as_str)
                                {
                                    if s.len() > (1024 * 1024usize).saturating_sub(signature.len())
                                    {
                                        return Err(error());
                                    }
                                    self.charge(s.len())?;
                                    signature.push_str(s);
                                } else {
                                    return Err(error());
                                }
                            }
                            _ => return Err(error()),
                        };
                    }
                } else {
                    return Err(ServiceError::Unsupported(
                        "Bedrock output content variant".into(),
                    ));
                }
                self.pending.insert(i, p);
            }
            "contentBlockStop" => {
                self.require_content()?;
                let i = index(&value)?;
                let p = self.pending.remove(&i).ok_or_else(error)?;
                let block = match p {
                    Pending::Text(t) => BedrockBlock::Text(t),
                    Pending::Reasoning { text, signature } => BedrockBlock::Reasoning {
                        text,
                        signature: (!signature.is_empty()).then_some(signature),
                    },
                    Pending::Redacted(v) => {
                        if v.is_empty() {
                            return Err(error());
                        }
                        BedrockBlock::Redacted(v)
                    }
                    Pending::Tool { id, name, input } => {
                        let arguments = if input.is_empty() {
                            json!({})
                        } else {
                            serde_json::from_str::<Value>(&input).map_err(|_| error())?
                        };
                        if !arguments.is_object() {
                            return Err(error());
                        }
                        BedrockBlock::Tool(ToolCall {
                            id,
                            name,
                            arguments,
                        })
                    }
                    Pending::Empty => return Err(error()),
                };
                self.closed.insert(i, block);
                // Only a contiguous closed prefix may be retained after interruption.
                let prefix: Vec<_> = (0..1024)
                    .map_while(|i| self.closed.get(&i).cloned())
                    .collect();
                if prefix.iter().any(|b| {
                    matches!(
                        b,
                        BedrockBlock::Reasoning {
                            signature: Some(_),
                            ..
                        } | BedrockBlock::Redacted(_)
                    )
                }) {
                    output.push(BedrockEvent::Checkpoint(prefix));
                }
            }
            "messageStop" => {
                self.require_content()?;
                if !self.pending.is_empty()
                    || self.closed.keys().copied().ne(0..self.closed.len() as u32)
                {
                    return Err(error());
                }
                self.stopped = Some(match string(&value, "stopReason")? {
                    "end_turn" | "stop_sequence" => FinishReason::Stop,
                    "tool_use" => FinishReason::ToolCalls,
                    "malformed_tool_use" | "malformed_model_output" => return Err(error()),
                    "max_tokens" | "model_context_window_exceeded" => FinishReason::Length,
                    "guardrail_intervened" | "content_filtered" => FinishReason::ContentFilter,
                    v if v.len() <= 128 => FinishReason::Other(v.into()),
                    _ => return Err(error()),
                });
            }
            "metadata" => {
                if self.stopped.is_none() || self.metadata {
                    return Err(error());
                }
                self.metadata = true;
                if let Some(u) = value.get("usage") {
                    let n = |key: &str| u.get(key).and_then(Value::as_u64).ok_or_else(error);
                    let prompt = n("inputTokens")?;
                    let completion = n("outputTokens")?;
                    let read = u
                        .get("cacheReadInputTokens")
                        .map(|v| v.as_u64().ok_or_else(error))
                        .transpose()?;
                    let write = u
                        .get("cacheWriteInputTokens")
                        .map(|v| v.as_u64().ok_or_else(error))
                        .transpose()?;
                    let total = prompt
                        .saturating_add(completion)
                        .saturating_add(read.unwrap_or(0))
                        .saturating_add(write.unwrap_or(0));
                    if total > 0 {
                        output.push(BedrockEvent::Usage(Usage {
                            prompt_tokens: prompt,
                            completion_tokens: completion,
                            total_tokens: total,
                            cache_read_input_tokens: read,
                            cache_creation_input_tokens: write,
                            reasoning_tokens: None,
                        }));
                    }
                }
            }
            _ => return Err(ServiceError::Unsupported("Bedrock stream event".into())),
        }
        Ok(output)
    }
    fn charge(&mut self, n: usize) -> Result<(), ServiceError> {
        if n > self.max_output_bytes.saturating_sub(self.retained) {
            return Err(ServiceError::Protocol(
                "Bedrock output exceeds configured limit".into(),
            ));
        }
        self.retained += n;
        Ok(())
    }
    fn require_content(&self) -> Result<(), ServiceError> {
        if !self.started || self.stopped.is_some() {
            Err(error())
        } else {
            Ok(())
        }
    }
    pub fn finish(&mut self) -> Result<BedrockEvent, ServiceError> {
        if self.poisoned || !self.pending.is_empty() {
            return Err(error());
        }
        let reason = self.stopped.take().ok_or_else(error)?;
        self.poisoned = true;
        let blocks: Vec<_> = std::mem::take(&mut self.closed).into_values().collect();
        let has_tools = blocks
            .iter()
            .any(|block| matches!(block, BedrockBlock::Tool(_)));
        if has_tools != (reason == FinishReason::ToolCalls) {
            return Err(ServiceError::Protocol(
                "Bedrock tool blocks require an explicit tool_use completion".into(),
            ));
        }
        Ok(BedrockEvent::Completed { blocks, reason })
    }
}
fn valid_identity(s: &str) -> bool {
    !s.is_empty() && s.len() <= 256 && !s.chars().any(|c| c.is_control() || c.is_whitespace())
}
