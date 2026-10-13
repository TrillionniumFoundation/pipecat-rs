//! Universal context conversion, based on the Python adapters' wire contracts.
use crate::{
    CompletionRequest, Content, ContentPart, Message, Provider, Role, ServiceError, ToolChoice,
};
use serde_json::{json, Value};

pub fn request_body(
    provider: Provider,
    request: &CompletionRequest,
) -> Result<Value, ServiceError> {
    let resolved = crate::settings::resolve(request)?;
    let request = &resolved;
    request.context.validate()?;
    if request.model.trim().is_empty() {
        return Err(ServiceError::InvalidRequest("model is empty".into()));
    }
    if request.max_tokens == Some(0) {
        return Err(ServiceError::InvalidRequest(
            "max_tokens must be positive".into(),
        ));
    }
    let max_temperature = if provider == Provider::Anthropic {
        1.0
    } else {
        2.0
    };
    if request
        .temperature
        .is_some_and(|t| !t.is_finite() || !(0.0..=max_temperature).contains(&t))
    {
        return Err(ServiceError::InvalidRequest(format!(
            "temperature must be finite and between 0 and {max_temperature}"
        )));
    }
    let mut body = match provider {
        Provider::OpenAi => openai(request)?,
        Provider::Anthropic => anthropic(request)?,
        Provider::Gemini => gemini(request)?,
    };
    crate::settings::apply_wire(provider, &request.settings, &mut body)?;
    let object = body.as_object_mut().expect("adapter returns object");
    for (key, value) in &request.extra {
        if matches!(
            key.as_str(),
            "model"
                | "stream"
                | "messages"
                | "tools"
                | "tool_choice"
                | "system"
                | "contents"
                | "systemInstruction"
                | "toolConfig"
                | "stream_options"
                | "n"
                | "candidateCount"
        ) || object.contains_key(key)
        {
            return Err(ServiceError::InvalidRequest(format!(
                "extra field {key} would override the streaming contract"
            )));
        }
        if key == "generationConfig"
            && value
                .get("candidateCount")
                .and_then(Value::as_u64)
                .is_some_and(|n| n != 1)
        {
            return Err(ServiceError::Unsupported(
                "multiple completion candidates".into(),
            ));
        }
        object.insert(key.clone(), value.clone());
    }
    if provider == Provider::Anthropic && !object.contains_key("thinking") {
        let lower = request.model.to_ascii_lowercase();
        let generation = lower.match_indices("sonnet-").find_map(|(start, _)| {
            let digits: String = lower[start + 7..]
                .chars()
                .take_while(char::is_ascii_digit)
                .collect();
            (digits.len() <= 2)
                .then(|| digits.parse::<u8>().ok())
                .flatten()
        });
        if generation.is_some_and(|generation| generation >= 5) {
            object.insert("thinking".into(), json!({"type":"disabled"}));
        }
    }
    Ok(body)
}

/// Append validated advertisement-only declarations after standard functions.
/// The client checks the provider and combined inventory before constructing
/// this body; the declarations grant no callback registration authority.
pub(crate) fn append_openai_declarations(
    mut body: Value,
    declarations: &crate::OpenAiToolDeclarations,
) -> Result<Value, ServiceError> {
    if declarations.is_empty() {
        return Ok(body);
    }
    let object = body.as_object_mut().ok_or_else(|| {
        ServiceError::InvalidRequest("OpenAI request body is not an object".into())
    })?;
    let tools = object
        .entry("tools")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| ServiceError::InvalidRequest("OpenAI tools are not an array".into()))?;
    for declaration in declarations.as_slice() {
        tools.push(Value::Object(declaration.clone().into_iter().collect()));
    }
    Ok(body)
}

/// Match the SDK tool order: search advertisements before the standard function
/// group. Existing standard declarations are retained without conversion changes.
pub(crate) fn prepend_gemini_declarations(
    mut body: Value,
    declarations: &crate::GeminiToolDeclarations,
) -> Result<Value, ServiceError> {
    if declarations.is_empty() {
        return Ok(body);
    }
    let object = body.as_object_mut().ok_or_else(|| {
        ServiceError::InvalidRequest("Gemini request body is not an object".into())
    })?;
    let standard = object
        .entry("tools")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| ServiceError::InvalidRequest("Gemini tools are not an array".into()))?;
    let mut tools = Vec::with_capacity(declarations.len() + standard.len());
    tools.extend(
        declarations
            .as_slice()
            .iter()
            .map(|_| json!({"googleSearch": {}})),
    );
    tools.append(standard);
    *standard = tools;
    Ok(body)
}

/// A redacted copy for diagnostics; prompt and tool arguments remain visible.
pub fn request_body_for_logging(
    provider: Provider,
    request: &CompletionRequest,
) -> Result<Value, ServiceError> {
    let mut body = request_body(provider, request)?;
    fn redact(value: &mut Value) {
        match value {
            Value::Object(object) => {
                for (key, value) in object {
                    if matches!(
                        key.as_str(),
                        "signature" | "thoughtSignature" | "data" | "file_data"
                    ) {
                        *value = json!("[redacted]");
                    } else {
                        redact(value);
                    }
                }
            }
            Value::Array(values) => values.iter_mut().for_each(redact),
            _ => {}
        }
    }
    redact(&mut body);
    Ok(body)
}

fn openai(request: &CompletionRequest) -> Result<Value, ServiceError> {
    let mut messages = vec![];
    if let Some(system) = request
        .system_instruction
        .as_ref()
        .filter(|s| !s.is_empty())
    {
        messages.push(json!({"role":"system", "content":system}));
    }
    for message in &request.context.messages {
        let mut out = json!({"role":message.role});
        out["content"] = match &message.content {
            Some(content) => openai_content(content)?,
            None => Value::Null,
        };
        if let Some(name) = &message.name {
            out["name"] = json!(name);
        }
        if let Some(id) = &message.tool_call_id {
            out["tool_call_id"] = json!(id);
        }
        if !message.tool_calls.is_empty() {
            out["tool_calls"] = Value::Array(message.tool_calls.iter().map(|call| json!({
                "id":call.id, "type":"function", "function":{"name":call.name,"arguments":call.arguments.to_string()}
            })).collect());
        }
        messages.push(out);
    }
    let mut body = json!({"model":request.model, "messages":messages, "stream":true, "stream_options":{"include_usage":true}});
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        body["max_completion_tokens"] = json!(max_tokens);
    }
    if let Some(tools) = &request.context.tools {
        body["tools"] = Value::Array(tools.iter().map(|t| json!({"type":"function","function":{"name":t.name,"description":t.description,"parameters":t.parameters}})).collect());
    }
    if let Some(choice) = &request.context.tool_choice {
        body["tool_choice"] = match choice {
            ToolChoice::Auto => json!("auto"),
            ToolChoice::None => json!("none"),
            ToolChoice::Required => json!("required"),
            ToolChoice::Function(name) => json!({"type":"function","function":{"name":name}}),
        };
    }
    Ok(body)
}
fn openai_content(content: &Content) -> Result<Value, ServiceError> {
    match content {
        Content::Text(text) => Ok(json!(text)),
        Content::Parts(parts) => parts.iter().map(|part| match part {
            ContentPart::GeminiText { .. } | ContentPart::GeminiToolMetadata { .. } => Err(ServiceError::Unsupported("Gemini provider history in Chat Completions".into())),
            ContentPart::Text { text } => Ok(json!({"type":"text","text":text})),
            ContentPart::ImageUrl { url } => Ok(json!({"type":"image_url","image_url":{"url":url}})),
            ContentPart::InlineData { mime_type, data } if mime_type.starts_with("image/") => Ok(json!({"type":"image_url","image_url":{"url":format!("data:{mime_type};base64,{data}")}})),
            ContentPart::InlineData { .. } => Err(ServiceError::Unsupported("non-image inline data in Chat Completions".into())),
            ContentPart::ImageUrlDetailed { url, detail } => {
                if !matches!(detail.as_str(), "auto" | "low" | "high") { return Err(ServiceError::InvalidRequest("image detail must be auto, low or high".into())); }
                Ok(json!({"type":"image_url","image_url":{"url":url,"detail":detail}}))
            }
            ContentPart::InputAudio { data, format } if matches!(format.as_str(), "wav" | "mp3") => Ok(json!({"type":"input_audio","input_audio":{"data":data,"format":format}})),
            ContentPart::InputAudio { .. } => Err(ServiceError::Unsupported("OpenAI input audio must use wav or mp3".into())),
            ContentPart::File { data, mime_type, filename } => {
                let mut file = json!({"file_data":format!("data:{mime_type};base64,{data}")});
                if let Some(filename) = filename { file["filename"] = json!(filename); }
                Ok(json!({"type":"file","file":file}))
            }
            ContentPart::BedrockReasoning { .. } | ContentPart::BedrockRedacted { .. } => Err(ServiceError::Unsupported("Bedrock history in Chat Completions".into())),
            ContentPart::Refusal { text } => Ok(json!({"type":"refusal","refusal":text})),
            ContentPart::Thinking { .. } => Err(ServiceError::Unsupported("signed thinking replay in Chat Completions".into())),
        }).collect::<Result<Vec<_>,_>>().map(Value::Array),
    }
}

fn split_system(request: &CompletionRequest) -> Result<(Option<String>, &[Message]), ServiceError> {
    let messages = &request.context.messages;
    // Keep a single system message as a user turn rather than producing an
    // empty conversation, matching the universal Python adapter contract.
    if messages.len() == 1 {
        return Ok((
            request.system_instruction.clone().filter(|s| !s.is_empty()),
            messages,
        ));
    }
    let (initial, remaining) = match messages.first() {
        Some(Message {
            role: Role::System,
            content,
            ..
        }) => {
            let instruction = match content {
                Some(Content::Text(text)) => Some(text.clone()),
                Some(Content::Parts(parts)) => Some(
                    parts
                        .iter()
                        .map(|part| match part {
                            ContentPart::Text { text } => Ok(text.as_str()),
                            _ => Err(ServiceError::Unsupported(
                                "non-text initial system content".into(),
                            )),
                        })
                        .collect::<Result<Vec<_>, _>>()?
                        .join(" "),
                ),
                None => None,
            };
            (instruction, &messages[1..])
        }
        _ => (None, messages.as_slice()),
    };
    Ok((
        request
            .system_instruction
            .clone()
            .filter(|s| !s.is_empty())
            .or(initial.filter(|s| !s.is_empty())),
        remaining,
    ))
}

fn anthropic(request: &CompletionRequest) -> Result<Value, ServiceError> {
    let (system, input) = split_system(request)?;
    let mut messages = vec![];
    for message in input {
        let role = if message.role == Role::Assistant {
            "assistant"
        } else {
            "user"
        };
        let mut content = vec![];
        if message.role == Role::Tool {
            content.push(json!({"type":"tool_result","tool_use_id":message.tool_call_id,"content":anthropic_content(message.content.as_ref())?}));
        } else {
            if message.content.is_some() {
                content.extend(anthropic_content(message.content.as_ref())?);
            }
            for call in &message.tool_calls {
                content.push(
                    json!({"type":"tool_use","id":call.id,"name":call.name,"input":call.arguments}),
                );
            }
        }
        if content.is_empty() {
            content.push(json!({"type":"text","text":"(empty)"}));
        }
        merge_message(&mut messages, role, "content", content);
    }
    let mut body = json!({"model":request.model,"messages":messages,"stream":true,"max_tokens":request.max_tokens.unwrap_or(4096)});
    if let Some(system) = system {
        body["system"] = json!(system);
    }
    if let Some(temperature) = request.temperature {
        body["temperature"] = json!(temperature);
    }
    if let Some(tools) = &request.context.tools {
        body["tools"] = Value::Array(tools.iter().map(|t| json!({"name":t.name,"description":t.description,"input_schema":t.parameters})).collect());
    }
    if let Some(choice) = &request.context.tool_choice {
        body["tool_choice"] = match choice {
            ToolChoice::Auto => json!({"type":"auto"}),
            ToolChoice::None => json!({"type":"none"}),
            ToolChoice::Required => json!({"type":"any"}),
            ToolChoice::Function(name) => json!({"type":"tool","name":name}),
        };
    }
    Ok(body)
}
fn anthropic_content(content: Option<&Content>) -> Result<Vec<Value>, ServiceError> {
    match content {
        None => Ok(vec![]),
        Some(Content::Text(text)) => Ok(vec![
            json!({"type":"text","text":if text.is_empty() {"(empty)"} else {text}}),
        ]),
        Some(Content::Parts(parts)) => parts
            .iter()
            .map(|part| match part {
                ContentPart::Text { text } => {
                    Ok(json!({"type":"text","text":if text.is_empty() {"(empty)"} else {text}}))
                }
                ContentPart::ImageUrl { url } if url.starts_with("data:") => {
                    let (mime_type, data) = data_url(url)?;
                    anthropic_data(mime_type, data)
                }
                ContentPart::ImageUrl { url }
                    if url.starts_with("https://") || url.starts_with("http://") =>
                {
                    Ok(json!({"type":"image","source":{"type":"url","url":url}}))
                }
                ContentPart::ImageUrl { .. } => {
                    Err(ServiceError::Unsupported("image URL scheme".into()))
                }
                ContentPart::InlineData { mime_type, data } => anthropic_data(mime_type, data),
                ContentPart::ImageUrlDetailed { url, detail } if detail == "auto" => {
                    anthropic_content(Some(&Content::Parts(vec![ContentPart::ImageUrl {
                        url: url.clone(),
                    }])))
                    .map(|mut parts| parts.remove(0))
                }
                ContentPart::File {
                    data,
                    mime_type,
                    filename,
                } => {
                    let mut part = anthropic_data(mime_type, data)?;
                    if part["type"] == "document" {
                        if let Some(filename) = filename {
                            part["title"] = json!(filename);
                        }
                    }
                    Ok(part)
                }
                ContentPart::Thinking {
                    text,
                    signature: Some(signature),
                } if !signature.is_empty() => {
                    Ok(json!({"type":"thinking","thinking":text,"signature":signature}))
                }
                ContentPart::Thinking { .. } => Err(ServiceError::Unsupported(
                    "Anthropic thinking replay requires a signature".into(),
                )),
                ContentPart::GeminiText { .. } | ContentPart::GeminiToolMetadata { .. } => Err(
                    ServiceError::Unsupported("Gemini provider history in Anthropic".into()),
                ),
                ContentPart::ImageUrlDetailed { .. } => Err(ServiceError::Unsupported(
                    "Anthropic image detail levels".into(),
                )),
                ContentPart::InputAudio { .. } => {
                    Err(ServiceError::Unsupported("Anthropic input audio".into()))
                }
                ContentPart::BedrockReasoning { .. } | ContentPart::BedrockRedacted { .. } => Err(
                    ServiceError::Unsupported("Bedrock history in Anthropic Messages".into()),
                ),
                ContentPart::Refusal { .. } => Err(ServiceError::Unsupported(
                    "Anthropic refusal content parts".into(),
                )),
            })
            .collect(),
    }
}
fn anthropic_data(mime_type: &str, data: &str) -> Result<Value, ServiceError> {
    let kind = if mime_type.starts_with("image/") {
        "image"
    } else if mime_type == "application/pdf" {
        "document"
    } else {
        return Err(ServiceError::Unsupported(format!(
            "Anthropic MIME type {mime_type}"
        )));
    };
    Ok(json!({"type":kind,"source":{"type":"base64","media_type":mime_type,"data":data}}))
}
fn data_url(url: &str) -> Result<(&str, &str), ServiceError> {
    let (header, data) = url
        .strip_prefix("data:")
        .and_then(|s| s.split_once(','))
        .ok_or_else(|| ServiceError::InvalidRequest("invalid data URL".into()))?;
    let mime = header
        .strip_suffix(";base64")
        .ok_or_else(|| ServiceError::Unsupported("non-base64 data URL".into()))?;
    if mime.is_empty() || data.is_empty() {
        return Err(ServiceError::InvalidRequest("empty data URL".into()));
    }
    Ok((mime, data))
}

fn gemini(request: &CompletionRequest) -> Result<Value, ServiceError> {
    let (system, input) = split_system(request)?;
    let mut messages = vec![];
    let mut native_calls = std::collections::HashSet::new();
    let names: std::collections::HashMap<_, _> = request
        .context
        .messages
        .iter()
        .flat_map(|m| &m.tool_calls)
        .map(|c| (c.id.as_str(), c.name.as_str()))
        .collect();
    for message in input {
        let role = if message.role == Role::Assistant {
            "model"
        } else {
            "user"
        };
        let mut parts = vec![];
        if message.role == Role::Tool {
            let name = message
                .name
                .as_deref()
                .or_else(|| {
                    message
                        .tool_call_id
                        .as_deref()
                        .and_then(|id| names.get(id).copied())
                })
                .ok_or_else(|| {
                    ServiceError::InvalidRequest(
                        "Gemini tool result needs matching call or name".into(),
                    )
                })?;
            let content = match &message.content {
                Some(Content::Text(text)) => {
                    serde_json::from_str::<Value>(text).unwrap_or_else(|_| json!({"value":text}))
                }
                _ => {
                    return Err(ServiceError::Unsupported(
                        "non-text tool result for Gemini".into(),
                    ))
                }
            };
            let response = if content.is_object() {
                content
            } else {
                json!({"value":content})
            };
            parts.push(json!({"functionResponse":{"id":message.tool_call_id,"name":name,"response":response}}));
        } else {
            match &message.content {
                Some(Content::Text(text)) => parts.push(json!({"text":text})),
                Some(Content::Parts(content)) => {
                    for part in content {
                        if let ContentPart::GeminiToolMetadata { tool_call_id, .. } = part {
                            native_calls.insert(tool_call_id.clone());
                            continue;
                        }
                        let converted = match part{
                        ContentPart::GeminiText{text,thought,signature,incomplete}=>{
                            if *incomplete{return Err(ServiceError::InvalidRequest("incomplete signed Gemini text requires an explicit application history decision".into()));}
                            let mut part = json!({"text":text});
                            if *thought {
                                part["thought"] = json!(true);
                            }
                            if let Some(signature) = signature {
                                part["thoughtSignature"] = json!(signature);
                            }
                            part
                        }
                        ContentPart::GeminiToolMetadata{..}=>unreachable!("metadata handled separately"),
                        ContentPart::Text{text}=>json!({"text":text}),
                        ContentPart::InlineData{mime_type,data}=>json!({"inlineData":{"mimeType":mime_type,"data":data}}),
                        ContentPart::ImageUrl{url} if url.starts_with("data:")=>{let(mime,data)=data_url(url)?;json!({"inlineData":{"mimeType":mime,"data":data}})},
                        ContentPart::ImageUrl{..}=>return Err(ServiceError::Unsupported("Gemini remote image URLs require an explicit MIME-typed file API integration".into())),
                        ContentPart::ImageUrlDetailed{url,detail} if detail=="auto" && url.starts_with("data:")=>{let(mime,data)=data_url(url)?;json!({"inlineData":{"mimeType":mime,"data":data}})},
                        ContentPart::File{data,mime_type,..}=>json!({"inlineData":{"mimeType":mime_type,"data":data}}),
                        ContentPart::InputAudio{data,format} if matches!(format.as_str(),"wav"|"mp3")=>json!({"inlineData":{"mimeType":if format=="wav"{"audio/wav"}else{"audio/mp3"},"data":data}}),
                        ContentPart::InputAudio{..}=>return Err(ServiceError::Unsupported("Gemini input audio format".into())),
                        ContentPart::ImageUrlDetailed{..}=>return Err(ServiceError::Unsupported("Gemini image detail or remote image URL".into())),
                        ContentPart::BedrockReasoning { .. } | ContentPart::BedrockRedacted { .. } => return Err(ServiceError::Unsupported("Bedrock history in Gemini".into())),
                        ContentPart::Thinking{..}=>return Err(ServiceError::Unsupported("Gemini thought-signature context replay".into())),
                        ContentPart::Refusal{..}=>return Err(ServiceError::Unsupported("Gemini refusal content parts".into())),
                    };
                        parts.push(converted);
                    }
                }
                None => {}
            }
            for call in &message.tool_calls {
                let mut part =
                    json!({"functionCall":{"id":call.id,"name":call.name,"args":call.arguments}});
                if let Some(Content::Parts(content)) = &message.content {
                    if let Some(signature) = content.iter().find_map(|part| match part {
                        ContentPart::GeminiToolMetadata {
                            tool_call_id,
                            signature,
                        } if tool_call_id == &call.id => signature.as_ref(),
                        _ => None,
                    }) {
                        part["thoughtSignature"] = json!(signature);
                    }
                }
                parts.push(part);
            }
        }
        if parts.is_empty() {
            parts.push(json!({"text":"(empty)"}));
        }
        let native_text = matches!(&message.content,Some(Content::Parts(content))if content.iter().any(|part|matches!(part,ContentPart::GeminiText{..})));
        messages.push((json!({"role":role,"parts":parts}), native_text));
    }
    let messages = gemini_group_signed_tools(messages)?;
    let mut merged = vec![];
    for message in messages {
        let role = message["role"].as_str().expect("generated role");
        merge_message(
            &mut merged,
            role,
            "parts",
            message["parts"]
                .as_array()
                .expect("generated parts")
                .clone(),
        );
    }
    let mut messages = merged;
    for message in &mut messages {
        if message["role"] != "model" {
            continue;
        }
        let parts = message["parts"].as_array_mut().expect("generated parts");
        let signed = parts
            .iter()
            .any(|part| part.get("thoughtSignature").is_some());
        let native = parts.iter().any(|part| {
            part.pointer("/functionCall/id")
                .and_then(Value::as_str)
                .is_some_and(|id| native_calls.contains(id))
        });
        if native
            && gemini_requires_tool_signature(&request.model)
            && parts
                .iter()
                .find(|part| part.get("functionCall").is_some())
                .is_none_or(|part| part.get("thoughtSignature").is_none())
        {
            return Err(ServiceError::InvalidRequest(
                "native Gemini tool group is missing its signed anchor".into(),
            ));
        }
        if !signed && !native {
            for part in parts {
                if part.get("functionCall").is_some() {
                    part["thoughtSignature"] = json!("skip_thought_signature_validator");
                }
            }
        }
    }
    let supports_prefill = ["gemini-2.", "gemini-3-", "gemini-3.1-", "gemini-pro-latest"]
        .iter()
        .any(|prefix| request.model.starts_with(prefix));
    if !supports_prefill
        && messages
            .last()
            .is_some_and(|message| message["role"] == "model")
    {
        messages.push(json!({"role":"user","parts":[{"text":"."}]}));
    }
    let mut body = json!({"contents":messages});
    if let Some(system) = system {
        body["systemInstruction"] = json!({"parts":[{"text":system}]});
    }
    let mut config = json!({});
    if let Some(temperature) = request.temperature {
        config["temperature"] = json!(temperature);
    }
    if let Some(max_tokens) = request.max_tokens {
        config["maxOutputTokens"] = json!(max_tokens);
    }
    if !config.as_object().unwrap().is_empty() {
        body["generationConfig"] = config;
    }
    if let Some(tools) = &request.context.tools {
        body["tools"] = if tools.is_empty() {
            json!([])
        } else {
            json!([{"functionDeclarations":tools.iter().map(|t|json!({"name":t.name,"description":t.description,"parameters":gemini_native_wire_schema(&t.parameters)})).collect::<Vec<_>>()}])
        };
    }
    if let Some(choice) = &request.context.tool_choice {
        body["toolConfig"] = json!({"functionCallingConfig":match choice{ToolChoice::Auto=>json!({"mode":"AUTO"}),ToolChoice::None=>json!({"mode":"NONE"}),ToolChoice::Required=>json!({"mode":"ANY"}),ToolChoice::Function(name)=>json!({"mode":"ANY","allowedFunctionNames":[name]})}});
    }
    Ok(body)
}
fn gemini_group_signed_tools(messages: Vec<(Value, bool)>) -> Result<Vec<Value>, ServiceError> {
    fn only(message: &Value, role: &str, field: &str) -> bool {
        message["role"] == role
            && message["parts"].as_array().is_some_and(|parts| {
                !parts.is_empty() && parts.iter().all(|part| part.get(field).is_some())
            })
    }
    fn signed_call(message: &Value) -> bool {
        only(message, "model", "functionCall")
            && message["parts"]
                .as_array()
                .unwrap()
                .iter()
                .any(|part| part.get("thoughtSignature").is_some())
    }
    let mut input = messages.into_iter().peekable();
    let mut output = vec![];
    while let Some((message, _)) = input.next() {
        if !signed_call(&message) {
            output.push(message);
            continue;
        }
        let mut calls = message["parts"].as_array().unwrap().clone();
        let mut responses = vec![];
        let mut other = vec![];
        while let Some((next, native_text)) = input.peek() {
            if signed_call(next)
                || (next["role"] == "user" && !only(next, "user", "functionResponse"))
                || (*native_text && !responses.is_empty())
            {
                break;
            }
            let (next, native_text) = input.next().unwrap();
            if native_text || only(&next, "model", "functionCall") {
                calls.extend(next["parts"].as_array().unwrap().iter().cloned());
            } else if only(&next, "user", "functionResponse") {
                responses.extend(next["parts"].as_array().unwrap().iter().cloned());
            } else {
                other.push(next);
            }
        }
        if !responses.is_empty() {
            let mut by_id = std::collections::HashMap::new();
            for response in responses {
                let id = response
                    .pointer("/functionResponse/id")
                    .and_then(Value::as_str)
                    .ok_or_else(|| {
                        ServiceError::InvalidRequest("Gemini tool result missing identity".into())
                    })?
                    .to_owned();
                if by_id.insert(id, response).is_some() {
                    return Err(ServiceError::InvalidRequest(
                        "duplicate Gemini tool result".into(),
                    ));
                }
            }
            responses = Vec::with_capacity(calls.len());
            for call in calls
                .iter()
                .filter(|part| part.get("functionCall").is_some())
            {
                let id = call
                    .pointer("/functionCall/id")
                    .and_then(Value::as_str)
                    .expect("validated call identity");
                responses.push(by_id.remove(id).ok_or_else(|| {
                    ServiceError::InvalidRequest(
                        "signed Gemini group has missing tool result".into(),
                    )
                })?);
            }
            if !by_id.is_empty() {
                return Err(ServiceError::InvalidRequest(
                    "signed Gemini group has unrelated tool result".into(),
                ));
            }
        }
        output.push(json!({"role":"model","parts":calls}));
        if !responses.is_empty() {
            output.push(json!({"role":"user","parts":responses}));
        }
        output.extend(other);
    }
    Ok(output)
}
pub(crate) fn gemini_requires_tool_signature(model: &str) -> bool {
    model
        .strip_prefix("models/")
        .unwrap_or(model)
        .starts_with("gemini-3")
}

fn gemini_native_wire_schema(schema: &Value) -> Value {
    fn canonicalize(schema: &mut Value) {
        let Some(object) = schema.as_object_mut() else {
            return;
        };
        if let Some(Value::String(kind)) = object.get_mut("type") {
            if let Some(canonical) = [
                "TYPE_UNSPECIFIED",
                "STRING",
                "NUMBER",
                "INTEGER",
                "BOOLEAN",
                "ARRAY",
                "OBJECT",
                "NULL",
            ]
            .into_iter()
            .find(|canonical| kind.eq_ignore_ascii_case(canonical))
            {
                *kind = canonical.to_string();
            }
        }
        if let Some(Value::Object(properties)) = object.get_mut("properties") {
            for property in properties.values_mut() {
                canonicalize(property);
            }
        }
        if let Some(items) = object.get_mut("items") {
            canonicalize(items);
        }
        if let Some(Value::Array(members)) = object.get_mut("anyOf") {
            for member in members {
                canonicalize(member);
            }
        }
    }
    let (mut schema, _) = adapt_gemini_tool_schema(schema);
    canonicalize(&mut schema);
    schema
}

/// Adapt the source adapter's known JSON Schema incompatibilities and report
/// every kind of removed constraint. Parameter names are never schema keywords.
/// Other fields are preserved; this is not a general schema sanitization pass.
pub fn adapt_gemini_tool_schema(schema: &Value) -> (Value, Vec<String>) {
    fn adapt(schema: &Value, notes: &mut std::collections::BTreeSet<String>) -> Value {
        let Some(object) = schema.as_object() else {
            return schema.clone();
        };
        let mut result = serde_json::Map::new();
        for (key, value) in object {
            if key == "additionalProperties" || key.starts_with("x-") {
                notes.insert(format!("dropped unsupported key '{key}'"));
            } else if key == "properties" && value.is_object() {
                result.insert(
                    key.clone(),
                    Value::Object(
                        value
                            .as_object()
                            .unwrap()
                            .iter()
                            .map(|(name, property)| (name.clone(), adapt(property, notes)))
                            .collect(),
                    ),
                );
            } else if key == "type" && value.is_array() {
                notes.insert("converted a union 'type' to 'anyOf'".into());
                result.insert(
                    "anyOf".into(),
                    Value::Array(
                        value
                            .as_array()
                            .unwrap()
                            .iter()
                            .map(|member| json!({"type":member}))
                            .collect(),
                    ),
                );
            } else if key == "enum"
                && value
                    .as_array()
                    .is_some_and(|values| values.iter().any(|value| !value.is_string()))
            {
                notes.insert("dropped a non-string 'enum', losing its constraint".into());
            } else {
                let value = match value {
                    Value::Object(_) => adapt(value, notes),
                    Value::Array(values) => {
                        Value::Array(values.iter().map(|value| adapt(value, notes)).collect())
                    }
                    _ => value.clone(),
                };
                result.insert(key.clone(), value);
            }
        }
        Value::Object(result)
    }
    let mut notes = std::collections::BTreeSet::new();
    let schema = adapt(schema, &mut notes);
    (schema, notes.into_iter().collect())
}

fn merge_message(messages: &mut Vec<Value>, role: &str, field: &str, parts: Vec<Value>) {
    if let Some(last) = messages.last_mut().filter(|m| m["role"] == role) {
        last[field]
            .as_array_mut()
            .expect("message parts are an array")
            .extend(parts);
    } else {
        let mut message = json!({"role":role});
        message[field] = Value::Array(parts);
        messages.push(message);
    }
}
