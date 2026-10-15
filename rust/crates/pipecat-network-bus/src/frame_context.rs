//! Common-context wire adapters. Provider-specific objects and unrepresentable
//! content parts fail explicitly instead of being flattened to text.
use super::extended::{field, no_local, parse, tagged};
use super::*;
use std::collections::BTreeMap;
const TOOLS: &str = "pipecat.adapters.schemas.tools_schema.ToolsSchema";
pub(super) fn supported_type(name: &str) -> bool {
    name == CONTEXT || name == TOOLS
}
pub(super) fn decode(value: &Value) -> Result<LlmContextData, CodecError> {
    let f = tagged(value, CONTEXT)?;
    Ok(LlmContextData {
        messages: decode_messages(field(f, "messages")?)?,
        tools: match f.get("tools") {
            None => ToolSet::NotGiven,
            Some(value) => decode_tools_data(value)?,
        },
        tool_choice: f
            .get("tool_choice")
            .filter(|v| !v.is_null())
            .map(decode_choice)
            .transpose()?,
    })
}
pub(super) fn encode(context: &LlmContextData) -> Result<Value, CodecError> {
    let mut f = json!({"messages":encode_messages(&context.messages)?});
    if let Some(tools) = encode_tools(&context.tools)? {
        f["tools"] = tools["__data__"].clone();
    }
    if let Some(choice) = &context.tool_choice {
        f["tool_choice"] = encode_choice(choice);
    }
    Ok(tag(CONTEXT, f))
}
pub(super) fn decode_choice(value: &Value) -> Result<ToolChoice, CodecError> {
    match value.as_str() {
        Some("auto") => Ok(ToolChoice::Auto),
        Some("none") => Ok(ToolChoice::None),
        Some("required") => Ok(ToolChoice::Required),
        Some(_) => Err(invalid("tool_choice")),
        None => {
            let f = value.as_object().ok_or_else(|| invalid("tool_choice"))?;
            exact(f, &["type", "function"])?;
            if f.get("type").and_then(Value::as_str) != Some("function") {
                return Err(invalid("tool_choice.type"));
            }
            let function = field(f, "function")?
                .as_object()
                .ok_or_else(|| invalid("tool_choice.function"))?;
            exact(function, &["name"])?;
            Ok(ToolChoice::Function(required(function, "name")?))
        }
    }
}
pub(super) fn encode_choice(value: &ToolChoice) -> Value {
    match value {
        ToolChoice::Auto => "auto".into(),
        ToolChoice::None => "none".into(),
        ToolChoice::Required => "required".into(),
        ToolChoice::Function(name) => json!({"type":"function","function":{"name":name}}),
    }
}
pub(super) fn decode_tools(value: &Value) -> Result<ToolSet, CodecError> {
    decode_tools_data(&Value::Object(tagged(value, TOOLS)?.clone()))
}
fn decode_tools_data(value: &Value) -> Result<ToolSet, CodecError> {
    let f = value.as_object().ok_or_else(|| invalid("context.tools"))?;
    let values = field(f, "standard_tools")?
        .as_array()
        .ok_or_else(|| invalid("standard_tools"))?;
    let standard_tools = values
        .iter()
        .map(|v| {
            let f = v.as_object().ok_or_else(|| invalid("standard tool"))?;
            exact(f, &["name", "description", "parameters"])?;
            Ok(StandardTool::Function(parse::<ToolDefinition>(
                f,
                json!({"description":"","parameters":{}}),
            )?))
        })
        .collect::<Result<Vec<_>, CodecError>>()?;
    let custom_tools = optional(f, "custom_tools")?;
    Ok(ToolSet::Schema(ToolsSchema {
        standard_tools,
        custom_tools,
    }))
}
pub(super) fn encode_tools(value: &ToolSet) -> Result<Option<Value>, CodecError> {
    let (tools, custom) = match value {
        ToolSet::NotGiven => return Ok(None),
        ToolSet::Functions(tools) => (tools.iter().collect::<Vec<_>>(), None),
        ToolSet::Schema(schema) => (
            schema
                .standard_tools
                .iter()
                .map(|t| match t {
                    StandardTool::Function(f) => Ok(f),
                    StandardTool::Direct(_) => Err(invalid("direct tool handler is local-only")),
                })
                .collect::<Result<Vec<_>, _>>()?,
            schema.custom_tools.as_ref(),
        ),
        ToolSet::Direct(_) => return Err(invalid("direct tool handlers are local-only")),
        ToolSet::Grouped(_) => return Err(invalid("grouped tools have no source bus adapter")),
    };
    let mut f = json!({"standard_tools":tools});
    if let Some(custom) = custom {
        f["custom_tools"] = serde_json::to_value(custom)?;
    }
    Ok(Some(tag(TOOLS, f)))
}
fn exact(f: &Map<String, Value>, keys: &[&str]) -> Result<(), CodecError> {
    if f.keys().any(|k| !keys.contains(&k.as_str())) {
        return Err(invalid("unsupported structured context field"));
    }
    Ok(())
}
fn object<'a>(f: &'a Map<String, Value>, key: &str) -> Result<&'a Map<String, Value>, CodecError> {
    field(f, key)?.as_object().ok_or_else(|| invalid(key))
}
fn part(value: &Value) -> Result<MessagePart, CodecError> {
    let f = value.as_object().ok_or_else(|| invalid("message part"))?;
    match field(f, "type")?.as_str() {
        Some("text") => {
            exact(f, &["type", "text"])?;
            Ok(MessagePart::Text {
                text: required(f, "text")?,
            })
        }
        Some("image_url") => {
            exact(f, &["type", "image_url"])?;
            let v = object(f, "image_url")?;
            exact(v, &["url", "detail"])?;
            Ok(MessagePart::Image {
                url: required(v, "url")?,
                detail: optional(v, "detail")?,
            })
        }
        Some("input_audio") => {
            exact(f, &["type", "input_audio"])?;
            let v = object(f, "input_audio")?;
            exact(v, &["data", "format"])?;
            Ok(MessagePart::Audio {
                data: required(v, "data")?,
                format: required(v, "format")?,
            })
        }
        Some("file") => {
            exact(f, &["type", "file"])?;
            let v = object(f, "file")?;
            exact(v, &["file_data", "filename"])?;
            let data: String = required(v, "file_data")?;
            let (mime, encoded) = data
                .strip_prefix("data:")
                .and_then(|s| s.split_once(";base64,"))
                .ok_or_else(|| invalid("file_data must be a base64 data URL"))?;
            Ok(MessagePart::File {
                data: encoded.to_owned(),
                mime_type: mime.to_owned(),
                filename: optional(v, "filename")?,
            })
        }
        Some("thinking") => {
            exact(f, &["type", "thinking", "signature"])?;
            Ok(MessagePart::Thinking {
                text: required(f, "thinking")?,
                signature: optional(f, "signature")?,
            })
        }
        Some("refusal") => {
            exact(f, &["type", "refusal"])?;
            Ok(MessagePart::Refusal {
                text: required(f, "refusal")?,
            })
        }
        _ => Err(invalid("unsupported message part type")),
    }
}
fn encode_part(part: &MessagePart) -> Result<Value, CodecError> {
    Ok(match part {
        MessagePart::GeminiText { .. } | MessagePart::GeminiToolMetadata { .. } => {
            return Err(invalid(
                "Gemini provider history has no common-context wire representation",
            ))
        }
        MessagePart::Text { text } => json!({"type":"text","text":text}),
        MessagePart::Image { url, detail } => {
            let mut image = json!({"url":url});
            if let Some(detail) = detail {
                image["detail"] = detail.clone().into();
            }
            json!({"type":"image_url","image_url":image})
        }
        MessagePart::Audio { data, format } => {
            json!({"type":"input_audio","input_audio":{"data":data,"format":format}})
        }
        MessagePart::File {
            data,
            mime_type,
            filename,
        } => {
            let mut file = json!({"file_data":format!("data:{mime_type};base64,{data}")});
            if let Some(filename) = filename {
                file["filename"] = filename.clone().into();
            }
            json!({"type":"file","file":file})
        }
        MessagePart::RedactedThinking { .. } => {
            return Err(invalid(
                "native Anthropic redacted history cannot cross this wire protocol",
            ))
        }
        MessagePart::Thinking { text, signature } => {
            let mut out = json!({"type":"thinking","thinking":text});
            if let Some(signature) = signature {
                out["signature"] = signature.clone().into();
            }
            out
        }
        MessagePart::BedrockReasoning { .. } | MessagePart::BedrockRedacted { .. } => {
            return Err(invalid(
                "native Bedrock history cannot cross this wire protocol",
            ))
        }
        MessagePart::Refusal { text } => json!({"type":"refusal","refusal":text}),
    })
}
fn call(value: &Value) -> Result<FunctionCall, CodecError> {
    let f = value.as_object().ok_or_else(|| invalid("tool call"))?;
    exact(f, &["id", "type", "function"])?;
    if f.get("type").and_then(Value::as_str) != Some("function") {
        return Err(invalid("tool call type"));
    }
    let function = object(f, "function")?;
    exact(function, &["name", "arguments"])?;
    // Arguments are JSON text in the universal Python message schema. Keep
    // their typed JSON meaning; the envelope retains the original text.
    let raw: String = required(function, "arguments")?;
    crate::codec::validate_integer_range(raw.as_bytes())?;
    let arguments = serde_json::from_str(&raw).map_err(|_| invalid("tool call arguments JSON"))?;
    Ok(FunctionCall {
        function_name: required(function, "name")?,
        tool_call_id: required(f, "id")?,
        arguments,
    })
}
pub(super) fn decode_messages(value: &Value) -> Result<Vec<ContextMessage>, CodecError> {
    value
        .as_array()
        .ok_or_else(|| invalid("messages"))?
        .iter()
        .map(|value| {
            let f = value.as_object().ok_or_else(|| invalid("message"))?;
            no_local(f, &["__specific__"])?;
            let content = match field(f, "content")? {
                Value::String(text) => MessageContent::Text(text.clone()),
                Value::Array(parts) => {
                    MessageContent::Parts(parts.iter().map(part).collect::<Result<_, _>>()?)
                }
                _ => return Err(invalid("unsupported null or missing message content")),
            };
            let tool_calls = match f.get("tool_calls") {
                None => vec![],
                Some(Value::Array(calls)) => calls.iter().map(call).collect::<Result<_, _>>()?,
                _ => return Err(invalid("tool_calls")),
            };
            let extra = f
                .iter()
                .filter(|(k, _)| {
                    !["role", "content", "name", "tool_call_id", "tool_calls"].contains(&k.as_str())
                })
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<BTreeMap<_, _>>();
            Ok(ContextMessage {
                role: required(f, "role")?,
                content,
                name: optional(f, "name")?,
                tool_call_id: optional(f, "tool_call_id")?,
                tool_calls,
                extra,
            })
        })
        .collect()
}
pub(super) fn encode_messages(messages: &[ContextMessage]) -> Result<Value, CodecError> {
    Ok(Value::Array(messages.iter().map(|message|{
        if message.extra.keys().any(|k|["role","content","name","tool_call_id","tool_calls","__specific__"].contains(&k.as_str())){return Err(invalid("context extra conflicts with typed field"));}
        let mut f:Map<String,Value>=message.extra.clone().into_iter().collect();
        f.insert("role".into(),serde_json::to_value(message.role)?);
        f.insert("content".into(),match &message.content{MessageContent::Text(text)=>text.clone().into(),MessageContent::Parts(parts)=>Value::Array(parts.iter().map(encode_part).collect::<Result<_,_>>()?)});
        put_optional(&mut f,"name",&message.name)?;put_optional(&mut f,"tool_call_id",&message.tool_call_id)?;
        if !message.tool_calls.is_empty(){f.insert("tool_calls".into(),Value::Array(message.tool_calls.iter().map(|call|json!({"id":call.tool_call_id,"type":"function","function":{"name":call.function_name,"arguments":call.arguments.to_string()}})).collect()));}
        Ok(Value::Object(f))
    }).collect::<Result<_,CodecError>>()?))
}
