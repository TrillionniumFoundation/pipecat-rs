//! Conversion to the Bedrock Converse schema. No OpenAI-compatible route is used.
use crate::settings::{invalid, BedrockSettings};
use base64::{engine::general_purpose::STANDARD, Engine};
use pipecat_services::{
    CompletionRequest, Content, ContentPart, Message, Role, ServiceError, ToolChoice,
};
use serde_json::{json, Value};
pub struct PreparedRequest {
    pub model: String,
    pub body: Value,
    pub noop_tool: bool,
}
impl std::fmt::Debug for PreparedRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PreparedRequest")
            .field("model", &self.model)
            .field("body", &"[redacted]")
            .field("noop_tool", &self.noop_tool)
            .finish()
    }
}

fn text(value: &str) -> Value {
    json!({"text": if value.is_empty() { "(empty)" } else { value }})
}
fn format(
    mime: &str,
    filename: Option<&str>,
) -> Result<(&'static str, &'static str), ServiceError> {
    Ok(match mime {
        "image/png" => ("image", "png"),
        "image/jpeg" => ("image", "jpeg"),
        "image/gif" => ("image", "gif"),
        "image/webp" => ("image", "webp"),
        "video/x-flv" => ("video", "flv"),
        "video/x-matroska" => ("video", "mkv"),
        "video/quicktime" => ("video", "mov"),
        "video/mpeg" => (
            "video",
            if filename.is_some_and(|n| n.to_ascii_lowercase().ends_with(".mpg")) {
                "mpg"
            } else {
                "mpeg"
            },
        ),
        "video/mp4" => ("video", "mp4"),
        "video/3gpp" => ("video", "three_gp"),
        "video/webm" => ("video", "webm"),
        "video/x-ms-wmv" => ("video", "wmv"),
        "application/pdf" => ("document", "pdf"),
        "text/csv" => ("document", "csv"),
        "application/msword" => ("document", "doc"),
        "application/vnd.openxmlformats-officedocument.wordprocessingml.document" => {
            ("document", "docx")
        }
        "application/vnd.ms-excel" => ("document", "xls"),
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet" => ("document", "xlsx"),
        "text/html" => ("document", "html"),
        "text/plain" => ("document", "txt"),
        "text/markdown" => ("document", "md"),
        _ => return Err(ServiceError::Unsupported("Bedrock file MIME type".into())),
    })
}
pub fn document_name(name: &str) -> String {
    let name: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c.is_whitespace() || "-()[]".contains(c) {
                c
            } else {
                ' '
            }
        })
        .collect();
    let name = name.split_whitespace().collect::<Vec<_>>().join(" ");
    if name.is_empty() {
        "document".into()
    } else {
        name
    }
}
fn media(data: &str, mime: &str, filename: Option<&str>) -> Result<Value, ServiceError> {
    let (kind, format) = format(mime, filename)?;
    let source = if data.starts_with("s3://") {
        let url = url::Url::parse(data).map_err(|_| invalid("invalid Bedrock S3 URI"))?;
        if url.host_str().is_none()
            || url.username() != ""
            || url.password().is_some()
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid("invalid Bedrock S3 URI"));
        }
        json!({"s3Location":{"uri":data}})
    } else {
        let bytes = STANDARD
            .decode(data)
            .map_err(|_| invalid("invalid Bedrock base64 content"))?;
        if bytes.is_empty() || bytes.len() > 20 * 1024 * 1024 {
            return Err(invalid("Bedrock media size limit"));
        }
        // REST JSON serializes blob values as base64; SDK Python exposes raw bytes.
        json!({"bytes":STANDARD.encode(bytes)})
    };
    let mut body = json!({"format":format,"source":source});
    if kind == "document" {
        body["name"] = document_name(filename.unwrap_or("document")).into();
    }
    Ok(json!({kind:body}))
}
fn content(message: &Message) -> Result<Vec<Value>, ServiceError> {
    let mut parts = match &message.content {
        None => vec![],
        Some(Content::Text(s)) => vec![text(s)],
        Some(Content::Parts(parts)) => parts
            .iter()
            .map(|part| match part {
                ContentPart::Text { text: t } => Ok(text(t)),
                ContentPart::File {
                    data,
                    mime_type,
                    filename,
                } => media(data, mime_type, filename.as_deref()),
                ContentPart::InlineData { data, mime_type } => media(data, mime_type, None),
                ContentPart::ImageUrl { url } | ContentPart::ImageUrlDetailed { url, .. } => {
                    let (head, data) = url
                        .strip_prefix("data:")
                        .and_then(|v| v.split_once(","))
                        .ok_or_else(|| {
                            ServiceError::Unsupported(
                                "Bedrock image requires inline data URL".into(),
                            )
                        })?;
                    let mime = head
                        .strip_suffix(";base64")
                        .filter(|v| v.starts_with("image/"))
                        .ok_or_else(|| invalid("invalid Bedrock image data URL"))?;
                    media(data, mime, None)
                }
                ContentPart::BedrockReasoning { text, signature } => {
                    let mut reasoning = json!({"text":text});
                    if let Some(s) = signature {
                        reasoning["signature"] = s.clone().into();
                    }
                    Ok(json!({"reasoningContent":{"reasoningText":reasoning}}))
                }
                ContentPart::BedrockRedacted { data } => {
                    Ok(json!({"reasoningContent":{"redactedContent":STANDARD.encode(data)}}))
                }
                ContentPart::Thinking { .. } => Err(ServiceError::Unsupported(
                    "foreign provider reasoning history".into(),
                )),
                _ => Err(ServiceError::Unsupported("Bedrock content part".into())),
            })
            .collect::<Result<Vec<_>, _>>()?,
    };
    if message.role == Role::Tool {
        let inner = match &message.content {
            Some(Content::Text(s)) => match serde_json::from_str::<Value>(s) {
                Ok(v) if v.is_object() => json!({"json":v}),
                _ => text(s),
            },
            _ => {
                return Err(invalid(
                    "Bedrock tool result requires text or serialized object",
                ))
            }
        };
        return Ok(vec![
            json!({"toolResult":{"toolUseId":message.tool_call_id,"content":[inner]}}),
        ]);
    }
    for call in &message.tool_calls {
        parts
            .push(json!({"toolUse":{"toolUseId":call.id,"name":call.name,"input":call.arguments}}));
    }
    for kind in ["image", "document", "video"] {
        let media: Vec<_> = parts
            .iter()
            .enumerate()
            .filter(|(_, v)| v.get(kind).is_some())
            .map(|(i, _)| i)
            .collect();
        if media.len() == 1 {
            if let Some(t) = parts.iter().position(|v| v.get("text").is_some()) {
                if media[0] > t {
                    let p = parts.remove(media[0]);
                    parts.insert(t, p);
                }
            }
        }
    }
    if parts.is_empty() {
        parts.push(text(""));
    }
    Ok(parts)
}
pub fn prepare(
    request: &CompletionRequest,
    streaming: bool,
) -> Result<PreparedRequest, ServiceError> {
    request.context.validate()?;
    let settings = BedrockSettings::resolve(request)?;
    let mut system = settings
        .system_instruction
        .clone()
        .filter(|s| !s.is_empty());
    let mut messages: Vec<Value> = vec![];
    for (i, message) in request.context.messages.iter().enumerate() {
        if i == 0 && message.role == Role::System && request.context.messages.len() > 1 {
            if system.is_none() {
                system = Some(match &message.content {
                    Some(Content::Text(s)) => s.clone(),
                    Some(Content::Parts(p)) => p
                        .iter()
                        .map(|p| match p {
                            ContentPart::Text { text } => Ok(text.clone()),
                            _ => Err(invalid("system content must be text")),
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" "),
                    None => String::new(),
                });
            }
            continue;
        }
        let role = if message.role == Role::Assistant {
            "assistant"
        } else {
            "user"
        };
        let parts = content(message)?;
        if message.role != Role::User
            && parts.iter().any(|p| {
                p.get("image").is_some() || p.get("document").is_some() || p.get("video").is_some()
            })
        {
            return Err(invalid("Bedrock media requires user role"));
        }
        if let Some(last) = messages.last_mut().filter(|v| v["role"] == role) {
            last["content"].as_array_mut().expect("array").extend(parts);
        } else {
            messages.push(json!({"role":role,"content":parts}));
        }
    }
    if messages.is_empty() {
        return Err(invalid(
            "Bedrock requires at least one conversation message",
        ));
    }
    if settings.inject_trailing_user() && messages.last().is_some_and(|m| m["role"] == "assistant")
    {
        messages.push(json!({"role":"user","content":[{"text":"."}]}));
    }
    let has_tools = messages
        .iter()
        .flat_map(|m| m["content"].as_array().expect("array"))
        .any(|p| p.get("toolUse").is_some() || p.get("toolResult").is_some());
    let mut body = json!({"messages":messages,"additionalModelRequestFields":settings.additional});
    if !settings.inference.is_empty() {
        body["inferenceConfig"] = settings.inference.into();
    }
    if let Some(system) = system.filter(|s| !s.is_empty()) {
        body["system"] = json!([{"text":system}]);
    }
    if let Some(effort) = settings.effort {
        body["outputConfig"] = json!({"effort":effort});
    }
    let mut noop_tool = false;
    if streaming {
        let mut tools:Vec<Value>=request.context.tools.as_deref().unwrap_or_default().iter().map(|t|json!({"toolSpec":{"name":t.name,"description":t.description,"inputSchema":{"json":t.parameters}}})).collect();
        if tools.is_empty() && has_tools {
            noop_tool = true;
            tools.push(json!({"toolSpec":{"name":"no_operation","description":"Internal placeholder function. Do not call this function.","inputSchema":{"json":{"type":"object","properties":{},"required":[]}}}}));
        }
        if !tools.is_empty() {
            body["toolConfig"] = json!({"tools":tools});
            if !noop_tool {
                if let Some(choice) = &request.context.tool_choice {
                    match choice {
                        ToolChoice::Auto => body["toolConfig"]["toolChoice"] = json!({"auto":{}}),
                        ToolChoice::Required => {
                            body["toolConfig"]["toolChoice"] = json!({"any":{}})
                        }
                        ToolChoice::Function(name) => {
                            if !request
                                .context
                                .tools
                                .as_deref()
                                .unwrap_or_default()
                                .iter()
                                .any(|t| &t.name == name)
                            {
                                return Err(invalid("unknown forced Bedrock tool"));
                            }
                            body["toolConfig"]["toolChoice"] = json!({"tool":{"name":name}})
                        }
                        ToolChoice::None => {}
                    }
                }
            }
        }
        if let Some(latency) = settings.latency {
            body["performanceConfig"] = json!({"latency":latency});
        }
        if settings.prompt_caching {
            for path in ["/system", "/toolConfig/tools"] {
                if let Some(list) = body.pointer_mut(path).and_then(Value::as_array_mut) {
                    list.push(json!({"cachePoint":{"type":"default"}}));
                }
            }
        }
    }
    Ok(PreparedRequest {
        model: settings.model,
        body,
        noop_tool,
    })
}
