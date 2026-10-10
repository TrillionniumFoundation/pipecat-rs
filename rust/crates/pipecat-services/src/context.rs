use crate::types::{ServiceError, ToolCall};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Role {
    System,
    Developer,
    User,
    Assistant,
    Tool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum Content {
    Text(String),
    Parts(Vec<ContentPart>),
}
impl From<&str> for Content {
    fn from(s: &str) -> Self {
        Self::Text(s.into())
    }
}
impl From<String> for Content {
    fn from(s: String) -> Self {
        Self::Text(s)
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ContentPart {
    BedrockReasoning {
        text: String,
        signature: Option<String>,
    },
    BedrockRedacted {
        data: Vec<u8>,
    },
    GeminiText {
        text: String,
        thought: bool,
        signature: Option<String>,
        #[serde(default)]
        incomplete: bool,
    },
    GeminiToolMetadata {
        tool_call_id: String,
        signature: Option<String>,
    },
    Text {
        text: String,
    },
    ImageUrl {
        url: String,
    },
    ImageUrlDetailed {
        url: String,
        detail: String,
    },
    InputAudio {
        data: String,
        format: String,
    },
    File {
        data: String,
        mime_type: String,
        filename: Option<String>,
    },
    Thinking {
        text: String,
        signature: Option<String>,
    },
    Refusal {
        text: String,
    },
    /// Encoded bytes are supplied by the caller; the client never reads files.
    InlineData {
        mime_type: String,
        data: String,
    },
}

impl std::fmt::Debug for ContentPart {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BedrockReasoning { text, signature } => f
                .debug_struct("BedrockReasoning")
                .field("text_bytes", &text.len())
                .field("signature_bytes", &signature.as_ref().map(String::len))
                .finish(),
            Self::BedrockRedacted { data } => f
                .debug_struct("BedrockRedacted")
                .field("bytes", &data.len())
                .finish(),
            Self::GeminiText {
                text,
                thought,
                signature,
                incomplete,
            } => f
                .debug_struct("GeminiText")
                .field("text_bytes", &text.len())
                .field("thought", thought)
                .field("signature", &signature.as_ref().map(|_| "[redacted]"))
                .field("incomplete", incomplete)
                .finish(),
            Self::GeminiToolMetadata { signature, .. } => f
                .debug_struct("GeminiToolMetadata")
                .field("signature", &signature.as_ref().map(|_| "[redacted]"))
                .finish_non_exhaustive(),
            Self::Thinking { text, signature } => f
                .debug_struct("Thinking")
                .field("text", text)
                .field("signature", &signature.as_ref().map(|_| "[redacted]"))
                .finish(),
            Self::Text { text } => f.debug_struct("Text").field("text", text).finish(),
            Self::Refusal { text } => f.debug_struct("Refusal").field("text", text).finish(),
            Self::ImageUrl { url } => f.debug_struct("ImageUrl").field("url", url).finish(),
            Self::ImageUrlDetailed { url, detail } => f
                .debug_struct("ImageUrlDetailed")
                .field("url", url)
                .field("detail", detail)
                .finish(),
            Self::InputAudio { data, format } => f
                .debug_struct("InputAudio")
                .field("data", data)
                .field("format", format)
                .finish(),
            Self::File {
                data,
                mime_type,
                filename,
            } => f
                .debug_struct("File")
                .field("data", data)
                .field("mime_type", mime_type)
                .field("filename", filename)
                .finish(),
            Self::InlineData { mime_type, data } => f
                .debug_struct("InlineData")
                .field("mime_type", mime_type)
                .field("data", data)
                .finish(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Message {
    pub role: Role,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub content: Option<Content>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tool_calls: Vec<ToolCall>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_call_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}
impl Message {
    pub fn text(role: Role, text: impl Into<String>) -> Self {
        Self {
            role,
            content: Some(Content::Text(text.into())),
            tool_calls: vec![],
            tool_call_id: None,
            name: None,
        }
    }
    pub fn tool_result(id: impl Into<String>, content: impl Into<String>) -> Self {
        Self {
            role: Role::Tool,
            content: Some(Content::Text(content.into())),
            tool_calls: vec![],
            tool_call_id: Some(id.into()),
            name: None,
        }
    }
    pub fn assistant_tool_calls(calls: Vec<ToolCall>) -> Self {
        Self {
            role: Role::Assistant,
            content: None,
            tool_calls: calls,
            tool_call_id: None,
            name: None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ToolDefinition {
    pub name: String,
    pub description: String,
    pub parameters: Value,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolChoice {
    Auto,
    None,
    Required,
    Function(String),
}

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct Context {
    #[serde(default)]
    pub messages: Vec<Message>,
    /// None omits tools entirely; Some(empty) explicitly sends an empty list.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tools: Option<Vec<ToolDefinition>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tool_choice: Option<ToolChoice>,
}
impl Context {
    pub fn new(messages: Vec<Message>) -> Self {
        Self {
            messages,
            ..Self::default()
        }
    }
    pub fn validate(&self) -> Result<(), ServiceError> {
        let mut call_ids = std::collections::HashSet::new();
        let mut thinking_bytes = 0usize;
        for message in &self.messages {
            if let Some(Content::Parts(parts)) = &message.content {
                let mut gemini_ids = std::collections::HashSet::new();
                for part in parts {
                    if let ContentPart::BedrockReasoning { text, signature } = part {
                        if message.role != Role::Assistant
                            || signature.as_ref().is_some_and(|s| {
                                s.is_empty() || s.len() > crate::protocol::MAX_SIGNATURE_BYTES
                            })
                        {
                            return Err(ServiceError::InvalidRequest(
                                "invalid Bedrock reasoning role or signature".into(),
                            ));
                        }
                        thinking_bytes = thinking_bytes
                            .saturating_add(text.len())
                            .saturating_add(signature.as_ref().map_or(0, String::len));
                    }
                    if let ContentPart::BedrockRedacted { data } = part {
                        if message.role != Role::Assistant || data.is_empty() {
                            return Err(ServiceError::InvalidRequest(
                                "invalid Bedrock redacted content".into(),
                            ));
                        }
                        thinking_bytes = thinking_bytes.saturating_add(data.len());
                    }
                    if thinking_bytes > crate::protocol::MAX_HISTORY_BYTES {
                        return Err(ServiceError::InvalidRequest(
                            "Bedrock history exceeds limit".into(),
                        ));
                    }
                    let gemini_bytes = match part {
                        ContentPart::GeminiText {
                            text,
                            signature,
                            incomplete,
                            thought,
                        } => {
                            if !message.tool_calls.is_empty()
                                || (*incomplete && (signature.is_some() || *thought))
                            {
                                return Err(ServiceError::InvalidRequest(
                                    "ambiguous or incomplete Gemini text Part".into(),
                                ));
                            }
                            Some((text.len(), signature))
                        }
                        ContentPart::GeminiToolMetadata {
                            tool_call_id,
                            signature,
                        } => {
                            let call = message
                                .tool_calls
                                .iter()
                                .find(|call| &call.id == tool_call_id);
                            if parts.len() != 1
                                || message.tool_calls.len() != 1
                                || !gemini_ids.insert(tool_call_id)
                                || call.is_none()
                            {
                                return Err(ServiceError::InvalidRequest(
                                    "unowned or duplicate Gemini tool bookmark".into(),
                                ));
                            }
                            let call = call.expect("bookmark ownership checked");
                            gemini_argument_bytes(&call.arguments, 0, &mut thinking_bytes)?;
                            Some((
                                tool_call_id
                                    .len()
                                    .saturating_add(call.id.len())
                                    .saturating_add(call.name.len()),
                                signature,
                            ))
                        }
                        _ => None,
                    };
                    if let Some((bytes, signature)) = gemini_bytes {
                        if message.role != Role::Assistant
                            || signature.as_ref().is_some_and(|s| {
                                s.is_empty() || s.len() > crate::protocol::MAX_SIGNATURE_BYTES
                            })
                        {
                            return Err(ServiceError::InvalidRequest(
                                "invalid Gemini history signature or role".into(),
                            ));
                        }
                        let bytes = bytes.saturating_add(signature.as_ref().map_or(0, String::len));
                        if bytes > crate::protocol::MAX_HISTORY_BYTES.saturating_sub(thinking_bytes)
                        {
                            return Err(ServiceError::InvalidRequest(
                                "Gemini history exceeds size limit".into(),
                            ));
                        }
                        thinking_bytes += bytes;
                    }
                    if let ContentPart::Thinking { text, signature } = part {
                        if signature.as_ref().is_some_and(|s| {
                            s.is_empty() || s.len() > crate::protocol::MAX_SIGNATURE_BYTES
                        }) {
                            return Err(ServiceError::InvalidRequest(
                                "invalid or oversized thinking signature".into(),
                            ));
                        }
                        let bytes = text
                            .len()
                            .saturating_add(signature.as_ref().map_or(0, String::len));
                        if bytes > crate::protocol::MAX_HISTORY_BYTES.saturating_sub(thinking_bytes)
                        {
                            return Err(ServiceError::InvalidRequest(
                                "signed thinking history exceeds size limit".into(),
                            ));
                        }
                        thinking_bytes += bytes;
                    }
                    if matches!(
                        part,
                        ContentPart::Thinking { .. } | ContentPart::Refusal { .. }
                    ) && message.role != Role::Assistant
                    {
                        return Err(ServiceError::InvalidRequest(
                            "thinking and refusal parts require assistant role".into(),
                        ));
                    }
                    if matches!(part, ContentPart::InputAudio { .. }) && message.role != Role::User
                    {
                        return Err(ServiceError::InvalidRequest(
                            "input audio requires user role".into(),
                        ));
                    }
                }
            }
            if message.role == Role::Tool
                && message.tool_call_id.as_deref().is_none_or(str::is_empty)
            {
                return Err(ServiceError::InvalidRequest(
                    "tool result requires tool_call_id".into(),
                ));
            }
            if !message.tool_calls.is_empty() && message.role != Role::Assistant {
                return Err(ServiceError::InvalidRequest(
                    "tool calls require assistant role".into(),
                ));
            }
            for call in &message.tool_calls {
                if call.id.is_empty() || call.name.is_empty() || !call.arguments.is_object() {
                    return Err(ServiceError::InvalidRequest(
                        "tool calls require id, name and object arguments".into(),
                    ));
                }
                if !call_ids.insert(&call.id) {
                    return Err(ServiceError::InvalidRequest(
                        "duplicate tool call id".into(),
                    ));
                }
            }
        }
        if let Some(tools) = &self.tools {
            let mut names = std::collections::HashSet::new();
            for tool in tools {
                if tool.name.is_empty() || !tool.parameters.is_object() || !names.insert(&tool.name)
                {
                    return Err(ServiceError::InvalidRequest(
                        "tools require unique names and object schemas".into(),
                    ));
                }
            }
        }
        Ok(())
    }
}
fn gemini_argument_bytes(
    value: &Value,
    depth: usize,
    bytes: &mut usize,
) -> Result<(), ServiceError> {
    if depth > pipecat_core::NativeLlmContinuation::MAX_ARGUMENT_DEPTH {
        return Err(ServiceError::InvalidRequest(
            "Gemini tool history exceeds nesting limit".into(),
        ));
    }
    *bytes = bytes.saturating_add(std::mem::size_of::<Value>());
    match value {
        Value::String(text) => *bytes = bytes.saturating_add(text.len()),
        Value::Array(values) => {
            for value in values {
                gemini_argument_bytes(value, depth + 1, bytes)?;
            }
        }
        Value::Object(values) => {
            for (key, value) in values {
                *bytes = bytes.saturating_add(key.len());
                gemini_argument_bytes(value, depth + 1, bytes)?;
            }
        }
        _ => {}
    }
    if *bytes > crate::protocol::MAX_HISTORY_BYTES {
        return Err(ServiceError::InvalidRequest(
            "Gemini history exceeds size limit".into(),
        ));
    }
    Ok(())
}
