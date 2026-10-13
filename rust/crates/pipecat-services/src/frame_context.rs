//! Loss-aware conversion from the core's universal context frames.
use crate::{
    Content, ContentPart, Context, Message, OpenAiToolDeclarations, Provider, Role, ServiceError,
    ToolCall, ToolChoice, ToolDefinition,
};
use pipecat_core::{
    AdapterType, ChatMessage, ChatRole, ContextMessage, LlmContextData, LlmContextFrame,
    MessageContent, MessagePart, StandardTool, ToolSet,
};
use std::borrow::Cow;

pub fn tool_choice(choice: &pipecat_core::ToolChoice) -> ToolChoice {
    match choice {
        pipecat_core::ToolChoice::Auto => ToolChoice::Auto,
        pipecat_core::ToolChoice::None => ToolChoice::None,
        pipecat_core::ToolChoice::Required => ToolChoice::Required,
        pipecat_core::ToolChoice::Function(name) => ToolChoice::Function(name.clone()),
    }
}
fn definition(tool: &pipecat_core::ToolDefinition) -> ToolDefinition {
    ToolDefinition {
        name: tool.name.clone(),
        description: tool.description.clone(),
        parameters: tool.parameters.clone(),
    }
}
pub fn tools(tools: &ToolSet) -> Result<Option<Vec<ToolDefinition>>, ServiceError> {
    convert_tools(tools, false, None)
}
/// Convert ordinary handler-free tools using the selected provider's adapter.
/// Executable Direct entries still require projection by their owning executor.
pub fn tools_for_provider(
    tools: &ToolSet,
    provider: Provider,
) -> Result<Option<Vec<ToolDefinition>>, ServiceError> {
    convert_tools(tools, false, Some(provider))
}
/// Convert ordinary tools and retain validated OpenAI declarations separately.
/// Direct entries still require their executor; frame owners must check native
/// sealed/speculative authority before using this declaration-aware conversion.
pub fn declared_tools_for_provider(
    tools: &ToolSet,
    provider: Provider,
) -> Result<(Option<Vec<ToolDefinition>>, OpenAiToolDeclarations), ServiceError> {
    let (tools, declarations) = declared_tool_selection(tools, provider)?;
    Ok((convert_tools(&tools, false, Some(provider))?, declarations))
}
fn declared_tool_selection(
    tools: &ToolSet,
    provider: Provider,
) -> Result<(Cow<'_, ToolSet>, OpenAiToolDeclarations), ServiceError> {
    let (Provider::OpenAi, ToolSet::Schema(schema)) = (provider, tools) else {
        return Ok((Cow::Borrowed(tools), OpenAiToolDeclarations::default()));
    };
    let Some(own) = schema
        .custom_tools
        .as_ref()
        .and_then(|tools| tools.get(&AdapterType::Openai))
        .filter(|tools| !tools.is_empty())
    else {
        return Ok((Cow::Borrowed(tools), OpenAiToolDeclarations::default()));
    };
    // Bound the borrowed data before retaining owning metadata in the sidecar.
    let declarations = OpenAiToolDeclarations::from_slice(own)?;
    let selected = pipecat_core::ToolsSchema {
        standard_tools: schema.standard_tools.clone(),
        custom_tools: None,
    };
    Ok((Cow::Owned(ToolSet::Schema(selected)), declarations))
}
fn convert_tools(
    tools: &ToolSet,
    advertise_handlers: bool,
    provider: Option<Provider>,
) -> Result<Option<Vec<ToolDefinition>>, ServiceError> {
    match tools {
        ToolSet::NotGiven => Ok(None),
        ToolSet::Functions(tools) => Ok(Some(tools.iter().map(definition).collect())),
        ToolSet::Schema(schema) => {
            if schema
                .custom_tools
                .as_ref()
                .is_some_and(|tools| match provider {
                    // The source OpenAI adapter selects only its own entries.
                    // Those entries still need an implemented wire contract.
                    Some(Provider::OpenAi) => tools
                        .get(&AdapterType::Openai)
                        .is_some_and(|tools| !tools.is_empty()),
                    // Gemini likewise selects only entries for its own adapter.
                    Some(Provider::Gemini) => tools
                        .get(&AdapterType::Gemini)
                        .is_some_and(|tools| !tools.is_empty()),
                    // The source Anthropic adapter converts only standard tools.
                    Some(Provider::Anthropic) => false,
                    _ => tools.values().any(|tools| !tools.is_empty()),
                })
            {
                return Err(ServiceError::Unsupported(
                    "provider-specific custom tool schemas".into(),
                ));
            }
            schema
                .standard_tools
                .iter()
                .map(|tool| match tool {
                    StandardTool::Function(tool) => Ok(definition(tool)),
                    StandardTool::Direct(tool) if advertise_handlers => {
                        Ok(definition(&tool.schema))
                    }
                    StandardTool::Direct(_) => Err(ServiceError::Unsupported(
                        "direct tool handlers must be registered with a tool executor".into(),
                    )),
                })
                .collect::<Result<Vec<_>, _>>()
                .map(Some)
        }
        ToolSet::Direct(tools) if advertise_handlers => Ok(Some(
            tools.iter().map(|tool| definition(&tool.schema)).collect(),
        )),
        ToolSet::Direct(_) => Err(ServiceError::Unsupported(
            "direct tool handlers must be registered with a tool executor".into(),
        )),
        ToolSet::Grouped(_) => Err(ServiceError::Unsupported(
            "grouped provider-specific tool schemas".into(),
        )),
    }
}
pub fn context(context: &LlmContextData) -> Result<Context, ServiceError> {
    convert_context(context, false, None, &context.tools)
}
/// Validate an ordinary context using the selected provider's tool adapter.
/// This performs no I/O and never registers or executes handlers. Frame owners
/// must check sealed/speculative capabilities before calling this conversion.
pub fn context_for_provider(
    context: &LlmContextData,
    provider: Provider,
) -> Result<Context, ServiceError> {
    convert_context(context, false, Some(provider), &context.tools)
}
/// Convert an ordinary context and preserve validated OpenAI declarations.
/// This performs no I/O or execution. Sealed/speculative frame owners must use
/// the strict conversions instead of extracting declarations from their frames.
pub fn declared_context_for_provider(
    context: &LlmContextData,
    provider: Provider,
) -> Result<(Context, OpenAiToolDeclarations), ServiceError> {
    let (tools, declarations) = declared_tool_selection(&context.tools, provider)?;
    Ok((
        convert_context(context, false, Some(provider), &tools)?,
        declarations,
    ))
}
/// Out-of-band inference advertises tools but has no execution lifecycle.
pub(crate) fn inference_context_for_provider(
    context: &LlmContextData,
    provider: Provider,
) -> Result<(Context, OpenAiToolDeclarations), ServiceError> {
    let (tools, declarations) = declared_tool_selection(&context.tools, provider)?;
    Ok((
        convert_context(context, true, Some(provider), &tools)?,
        declarations,
    ))
}
fn convert_context(
    context: &LlmContextData,
    advertise_handlers: bool,
    provider: Option<Provider>,
    tools: &ToolSet,
) -> Result<Context, ServiceError> {
    let result = Context {
        messages: context
            .messages
            .iter()
            .map(message)
            .collect::<Result<_, _>>()?,
        tools: convert_tools(tools, advertise_handlers, provider)?,
        tool_choice: context.tool_choice.as_ref().map(tool_choice),
    };
    result.validate()?;
    Ok(result)
}
pub fn legacy_context(context: &LlmContextFrame) -> Context {
    Context {
        messages: context.messages.iter().map(legacy_message).collect(),
        // The old frame did not distinguish omission from an empty list.
        tools: (!context.tools.is_empty()).then(|| context.tools.iter().map(definition).collect()),
        tool_choice: context.tool_choice.as_ref().map(tool_choice),
    }
}
fn role(role: ChatRole) -> Role {
    match role {
        ChatRole::System => Role::System,
        ChatRole::Developer => Role::Developer,
        ChatRole::User => Role::User,
        ChatRole::Assistant => Role::Assistant,
        ChatRole::Tool => Role::Tool,
    }
}
pub fn legacy_message(message: &ChatMessage) -> Message {
    Message {
        role: role(message.role),
        content: Some(Content::Text(message.content.clone())),
        name: message.name.clone(),
        tool_call_id: message.tool_call_id.clone(),
        tool_calls: message
            .tool_calls
            .iter()
            .map(|call| ToolCall {
                id: call.tool_call_id.clone(),
                name: call.function_name.clone(),
                arguments: call.arguments.clone(),
            })
            .collect(),
    }
}
pub fn message(message: &ContextMessage) -> Result<Message, ServiceError> {
    if !message.extra.is_empty() {
        return Err(ServiceError::Unsupported(format!(
            "universal message extensions: {}",
            message.extra.keys().cloned().collect::<Vec<_>>().join(", ")
        )));
    }
    Ok(Message {
        role: role(message.role),
        content: Some(match &message.content {
            MessageContent::Text(text) => Content::Text(text.clone()),
            MessageContent::Parts(parts) => Content::Parts(
                parts
                    .iter()
                    .map(|part| match part {
                        MessagePart::GeminiText {
                            text,
                            thought,
                            signature,
                            incomplete,
                        } => ContentPart::GeminiText {
                            text: text.clone(),
                            thought: *thought,
                            signature: signature.clone(),
                            incomplete: *incomplete,
                        },
                        MessagePart::GeminiToolMetadata {
                            tool_call_id,
                            signature,
                        } => ContentPart::GeminiToolMetadata {
                            tool_call_id: tool_call_id.clone(),
                            signature: signature.clone(),
                        },
                        MessagePart::BedrockReasoning { text, signature } => {
                            ContentPart::BedrockReasoning {
                                text: text.clone(),
                                signature: signature.clone(),
                            }
                        }
                        MessagePart::BedrockRedacted { data } => {
                            ContentPart::BedrockRedacted { data: data.clone() }
                        }
                        MessagePart::Text { text } => ContentPart::Text { text: text.clone() },
                        MessagePart::Image { url, detail } => match detail {
                            None => ContentPart::ImageUrl { url: url.clone() },
                            Some(detail) => ContentPart::ImageUrlDetailed {
                                url: url.clone(),
                                detail: detail.clone(),
                            },
                        },
                        MessagePart::Audio { data, format } => ContentPart::InputAudio {
                            data: data.clone(),
                            format: format.clone(),
                        },
                        MessagePart::File {
                            data,
                            mime_type,
                            filename,
                        } => ContentPart::File {
                            data: data.clone(),
                            mime_type: mime_type.clone(),
                            filename: filename.clone(),
                        },
                        MessagePart::Thinking { text, signature } => ContentPart::Thinking {
                            text: text.clone(),
                            signature: signature.clone(),
                        },
                        MessagePart::Refusal { text } => {
                            ContentPart::Refusal { text: text.clone() }
                        }
                    })
                    .collect(),
            ),
        }),
        name: message.name.clone(),
        tool_call_id: message.tool_call_id.clone(),
        tool_calls: message
            .tool_calls
            .iter()
            .map(|call| ToolCall {
                id: call.tool_call_id.clone(),
                name: call.function_name.clone(),
                arguments: call.arguments.clone(),
            })
            .collect(),
    })
}

pub fn to_core_message(message: &Message) -> ContextMessage {
    ContextMessage {
        role: match message.role {
            Role::System => ChatRole::System,
            Role::Developer => ChatRole::Developer,
            Role::User => ChatRole::User,
            Role::Assistant => ChatRole::Assistant,
            Role::Tool => ChatRole::Tool,
        },
        content: match &message.content {
            None => MessageContent::Parts(vec![]),
            Some(Content::Text(text)) => MessageContent::Text(text.clone()),
            Some(Content::Parts(parts)) => MessageContent::Parts(
                parts
                    .iter()
                    .map(|part| match part {
                        ContentPart::GeminiText {
                            text,
                            thought,
                            signature,
                            incomplete,
                        } => MessagePart::GeminiText {
                            text: text.clone(),
                            thought: *thought,
                            signature: signature.clone(),
                            incomplete: *incomplete,
                        },
                        ContentPart::GeminiToolMetadata {
                            tool_call_id,
                            signature,
                        } => MessagePart::GeminiToolMetadata {
                            tool_call_id: tool_call_id.clone(),
                            signature: signature.clone(),
                        },
                        ContentPart::BedrockReasoning { text, signature } => {
                            MessagePart::BedrockReasoning {
                                text: text.clone(),
                                signature: signature.clone(),
                            }
                        }
                        ContentPart::BedrockRedacted { data } => {
                            MessagePart::BedrockRedacted { data: data.clone() }
                        }
                        ContentPart::Text { text } => MessagePart::Text { text: text.clone() },
                        ContentPart::ImageUrl { url } => MessagePart::Image {
                            url: url.clone(),
                            detail: None,
                        },
                        ContentPart::ImageUrlDetailed { url, detail } => MessagePart::Image {
                            url: url.clone(),
                            detail: Some(detail.clone()),
                        },
                        ContentPart::InlineData { mime_type, data } => MessagePart::File {
                            data: data.clone(),
                            mime_type: mime_type.clone(),
                            filename: None,
                        },
                        ContentPart::InputAudio { data, format } => MessagePart::Audio {
                            data: data.clone(),
                            format: format.clone(),
                        },
                        ContentPart::File {
                            data,
                            mime_type,
                            filename,
                        } => MessagePart::File {
                            data: data.clone(),
                            mime_type: mime_type.clone(),
                            filename: filename.clone(),
                        },
                        ContentPart::Thinking { text, signature } => MessagePart::Thinking {
                            text: text.clone(),
                            signature: signature.clone(),
                        },
                        ContentPart::Refusal { text } => {
                            MessagePart::Refusal { text: text.clone() }
                        }
                    })
                    .collect(),
            ),
        },
        name: message.name.clone(),
        tool_call_id: message.tool_call_id.clone(),
        tool_calls: message
            .tool_calls
            .iter()
            .map(|call| pipecat_core::FunctionCall {
                function_name: call.name.clone(),
                tool_call_id: call.id.clone(),
                arguments: call.arguments.clone(),
            })
            .collect(),
        extra: Default::default(),
    }
}
