// Copyright (c) 2024-2026, Daily
// SPDX-License-Identifier: BSD-2-Clause
use crate::{ServiceError, ToolChoice, ToolDefinition};
use pipecat_services::{Content, ContentPart, Context, Message, Role};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::fmt;

#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReasoningItem {
    pub id: String,
    #[serde(default)]
    pub summary: Vec<String>,
    pub encrypted_content: Option<String>,
}
impl fmt::Debug for ReasoningItem {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReasoningItem")
            .field("id", &self.id)
            .field("summary_parts", &self.summary.len())
            .field(
                "encrypted_content",
                &self.encrypted_content.as_ref().map(|_| "[redacted]"),
            )
            .finish()
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub enum InputContent {
    Text(String),
    Parts(Vec<InputPart>),
}
#[derive(Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum InputPart {
    InputText {
        text: String,
    },
    InputImage {
        image_url: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        detail: Option<String>,
    },
    InputFile {
        #[serde(skip_serializing_if = "Option::is_none")]
        filename: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_data: Option<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        file_url: Option<String>,
    },
}
impl fmt::Debug for InputPart {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InputText { text } => f.debug_struct("InputText").field("text", text).finish(),
            Self::InputImage { .. } => f.write_str("InputImage([redacted])"),
            Self::InputFile { .. } => f.write_str("InputFile([redacted])"),
        }
    }
}
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum InputItem {
    Message { role: Role, content: InputContent },
    FunctionCall(crate::ToolCall),
    FunctionOutput { call_id: String, output: String },
    Reasoning(ReasoningItem),
}
impl InputItem {
    pub fn text(role: Role, text: impl Into<String>) -> Self {
        Self::Message {
            role,
            content: InputContent::Text(text.into()),
        }
    }
    pub(crate) fn wire(&self) -> Result<Value, ServiceError> {
        Ok(match self {
            Self::Message { role, content } => {
                let role = match role {
                    Role::System | Role::Developer => "developer",
                    Role::User => "user",
                    Role::Assistant => "assistant",
                    Role::Tool => return Err(invalid("tool messages require FunctionOutput")),
                };
                if let InputContent::Parts(parts) = content {
                    for part in parts {
                        match part {
                            InputPart::InputImage { image_url, detail } => {
                                validate_media_url(image_url, true)?;
                                if detail.as_deref().is_some_and(|s| {
                                    !["auto", "low", "high", "original"].contains(&s)
                                }) {
                                    return Err(invalid("invalid image detail"));
                                }
                            }
                            InputPart::InputFile {
                                file_url,
                                file_data,
                                filename,
                            } => {
                                if file_url.is_some() == file_data.is_some() {
                                    return Err(invalid(
                                        "file requires exactly one URL or encoded payload",
                                    ));
                                }
                                if let Some(url) = file_url {
                                    validate_media_url(url, false)?;
                                }
                                if let Some(data) = file_data {
                                    if !data.starts_with("data:")
                                        || !data.contains(";base64,")
                                        || filename.as_deref().is_none_or(str::is_empty)
                                    {
                                        return Err(invalid(
                                            "inline file requires filename and a base64 data URL",
                                        ));
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
                json!({"role":role,"content":content})
            }
            Self::FunctionCall(call) => {
                if call.id.is_empty() || call.name.is_empty() || !call.arguments.is_object() {
                    return Err(invalid(
                        "function call requires id, name and object arguments",
                    ));
                }
                json!({"type":"function_call","call_id":call.id,"name":call.name,"arguments":call.arguments.to_string()})
            }
            Self::FunctionOutput { call_id, output } => {
                if call_id.is_empty() {
                    return Err(invalid("function output requires call_id"));
                }
                json!({"type":"function_call_output","call_id":call_id,"output":output})
            }
            Self::Reasoning(item) => {
                if item.id.is_empty() {
                    return Err(invalid("reasoning item requires id"));
                }
                let mut value = json!({"type":"reasoning","id":item.id,"summary":item.summary.iter().map(|text| json!({"type":"summary_text","text":text})).collect::<Vec<_>>()});
                if let Some(encrypted) = &item.encrypted_content {
                    value["encrypted_content"] = json!(encrypted);
                }
                value
            }
        })
    }
}
fn invalid(message: &str) -> ServiceError {
    ServiceError::InvalidRequest(message.into())
}
fn validate_media_url(value: &str, data_allowed: bool) -> Result<(), ServiceError> {
    if data_allowed && value.starts_with("data:image/") && value.contains(";base64,") {
        return Ok(());
    }
    let url = url::Url::parse(value).map_err(|_| invalid("invalid media URL"))?;
    if !["http", "https"].contains(&url.scheme())
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return Err(invalid("media requires an HTTP(S) URL without credentials"));
    }
    Ok(())
}
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct ResponsesContext {
    pub items: Vec<InputItem>,
    pub tools: Option<Vec<ToolDefinition>>,
    pub tool_choice: Option<ToolChoice>,
}
impl ResponsesContext {
    pub fn new(items: Vec<InputItem>) -> Self {
        Self {
            items,
            ..Self::default()
        }
    }
    pub fn from_core(context: &pipecat_core::LlmContextData) -> Result<Self, ServiceError> {
        let mut items = vec![];
        for message in &context.messages {
            if let Some(reasoning) = message.extra.get("openai_responses_reasoning") {
                if message.extra.len() != 1
                    || message.role != pipecat_core::ChatRole::Assistant
                    || !matches!(&message.content, pipecat_core::MessageContent::Text(text) if text.is_empty())
                    || message.name.is_some()
                    || message.tool_call_id.is_some()
                    || !message.tool_calls.is_empty()
                {
                    return Err(invalid(
                        "reasoning extension must be an otherwise empty assistant message",
                    ));
                }
                let reasoning: ReasoningItem = serde_json::from_value(reasoning.clone())
                    .map_err(|_| invalid("invalid Responses reasoning extension"))?;
                items.push(InputItem::Reasoning(reasoning));
            } else {
                if !message.extra.is_empty() {
                    return Err(ServiceError::Unsupported(
                        "unknown Responses universal message extension".into(),
                    ));
                }
                let message = pipecat_services::frame_context::message(message)?;
                Context::new(vec![message.clone()]).validate()?;
                convert_message(&message, &mut items)?;
            }
        }
        Ok(Self {
            items,
            tools: pipecat_services::frame_context::tools(&context.tools)?,
            tool_choice: context
                .tool_choice
                .as_ref()
                .map(pipecat_services::frame_context::tool_choice),
        })
    }
    /// Returns a sanitized copy; encoded media and encrypted reasoning never appear.
    pub fn for_logging(&self) -> Result<Value, ServiceError> {
        let mut items = self
            .items
            .iter()
            .map(InputItem::wire)
            .collect::<Result<Vec<_>, _>>()?;
        fn redact(value: &mut Value) {
            match value {
                Value::Object(map) => {
                    for (key, value) in map.iter_mut() {
                        if ["encrypted_content", "file_data", "image_url", "file_url"]
                            .contains(&key.as_str())
                        {
                            *value = json!("[redacted]");
                        } else {
                            redact(value);
                        }
                    }
                }
                Value::Array(items) => {
                    for value in items {
                        redact(value);
                    }
                }
                _ => {}
            }
        }
        for value in &mut items {
            redact(value);
        }
        Ok(json!(items))
    }
}
impl TryFrom<&Context> for ResponsesContext {
    type Error = ServiceError;
    fn try_from(context: &Context) -> Result<Self, Self::Error> {
        context.validate()?;
        let mut items = vec![];
        for message in &context.messages {
            convert_message(message, &mut items)?;
        }
        Ok(Self {
            items,
            tools: context.tools.clone(),
            tool_choice: context.tool_choice.clone(),
        })
    }
}
fn convert_message(message: &Message, items: &mut Vec<InputItem>) -> Result<(), ServiceError> {
    if message.role != Role::Tool && message.tool_call_id.is_some() {
        return Err(ServiceError::InvalidRequest(
            "tool_call_id requires tool role".into(),
        ));
    }
    if message.name.is_some() {
        return Err(ServiceError::Unsupported("Responses message name".into()));
    }
    if message.role == Role::Tool {
        let output = match &message.content {
            None => String::new(),
            Some(Content::Text(text)) => text.clone(),
            _ => {
                return Err(ServiceError::Unsupported(
                    "multimodal function output".into(),
                ))
            }
        };
        items.push(InputItem::FunctionOutput {
            call_id: message.tool_call_id.clone().unwrap_or_default(),
            output,
        });
    } else if !message.tool_calls.is_empty() {
        // Source-compatible assistant tool-call messages contain only calls.
        if matches!(&message.content, Some(Content::Text(text)) if !text.is_empty())
            || matches!(&message.content, Some(Content::Parts(parts)) if !parts.is_empty())
        {
            return Err(ServiceError::Unsupported(
                "assistant content combined with function calls".into(),
            ));
        }
        items.extend(
            message
                .tool_calls
                .iter()
                .cloned()
                .map(InputItem::FunctionCall),
        );
    } else {
        let content = match &message.content {
            None => InputContent::Text(String::new()),
            Some(Content::Text(text)) => InputContent::Text(text.clone()),
            Some(Content::Parts(parts)) => InputContent::Parts(
                parts
                    .iter()
                    .map(|part| {
                        Ok(match part {
                            ContentPart::Text { text } => {
                                InputPart::InputText { text: text.clone() }
                            }
                            ContentPart::ImageUrl { url } => InputPart::InputImage {
                                image_url: url.clone(),
                                detail: Some("auto".into()),
                            },
                            ContentPart::ImageUrlDetailed { url, detail } => {
                                InputPart::InputImage {
                                    image_url: url.clone(),
                                    detail: Some(detail.clone()),
                                }
                            }
                            ContentPart::File {
                                data,
                                mime_type,
                                filename,
                            } => file_part(data, mime_type, filename.clone()),
                            ContentPart::InlineData { data, mime_type } => {
                                file_part(data, mime_type, None)
                            }
                            ContentPart::InputAudio { .. } => {
                                return Err(ServiceError::Unsupported(
                                    "Responses input audio".into(),
                                ))
                            }
                            ContentPart::Thinking { .. } => {
                                return Err(ServiceError::Unsupported(
                                    "use Responses ReasoningItem for encrypted reasoning history"
                                        .into(),
                                ))
                            }
                            ContentPart::GeminiText { .. }
                            | ContentPart::GeminiToolMetadata { .. } => {
                                return Err(ServiceError::Unsupported(
                                    "Gemini provider history in Responses".into(),
                                ))
                            }
                            ContentPart::Refusal { .. } => {
                                return Err(ServiceError::Unsupported(
                                    "Responses refusal history".into(),
                                ))
                            }
                        })
                    })
                    .collect::<Result<Vec<_>, ServiceError>>()?,
            ),
        };
        items.push(InputItem::Message {
            role: message.role,
            content,
        });
    }
    Ok(())
}
fn file_part(data: &str, mime: &str, filename: Option<String>) -> InputPart {
    let data = if data.starts_with("data:") {
        data.to_owned()
    } else {
        format!("data:{mime};base64,{data}")
    };
    if mime.starts_with("image/") {
        InputPart::InputImage {
            image_url: data,
            detail: None,
        }
    } else {
        InputPart::InputFile {
            filename,
            file_data: Some(data),
            file_url: None,
        }
    }
}

impl ReasoningItem {
    /// Universal-context representation preserving ordering and opaque payload.
    pub fn to_core_message(&self) -> pipecat_core::ContextMessage {
        let mut message: pipecat_core::ContextMessage =
            pipecat_core::ChatMessage::new(pipecat_core::ChatRole::Assistant, "").into();
        message.extra.insert(
            "openai_responses_reasoning".into(),
            serde_json::to_value(self).expect("reasoning item is serializable"),
        );
        message
    }
}
