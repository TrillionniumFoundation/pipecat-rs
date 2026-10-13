use pipecat_core::{self as core, Setting};
use pipecat_responses::*;
use pipecat_services::Role;
use serde_json::json;
use std::{collections::BTreeMap, io::ErrorKind, net::TcpListener};

fn definition(parameters: serde_json::Value) -> core::ToolDefinition {
    core::ToolDefinition {
        name: "lookup".into(),
        description: "Find an application value".into(),
        parameters,
    }
}
fn tools() -> core::ToolSet {
    core::ToolSet::Functions(vec![definition(json!({"type":"object"}))])
}
fn request() -> ResponsesRequest {
    let mut request = ResponsesRequest::new(
        "gpt-4.1",
        ResponsesContext {
            items: vec![
                InputItem::text(Role::User, "retained input"),
                InputItem::Reasoning(ReasoningItem {
                    id: "reasoning".into(),
                    summary: vec!["retained summary".into()],
                    encrypted_content: Some("opaque".into()),
                }),
                InputItem::FunctionCall(ToolCall {
                    id: "old-call".into(),
                    name: "lookup".into(),
                    arguments: json!({"key":"old"}),
                }),
                InputItem::FunctionOutput {
                    call_id: "old-call".into(),
                    output: "retained output".into(),
                },
            ],
            tools: Some(vec![ToolDefinition {
                name: "lookup".into(),
                description: "original".into(),
                parameters: json!({"type":"object"}),
            }]),
            tool_choice: Some(ToolChoice::Function("lookup".into())),
        },
    );
    request.settings.llm.system_instruction = Setting::Value("retained instruction".into());
    request.settings.max_completion_tokens = Setting::Value(64);
    request.response_schema = Some(json!({"type":"object","properties":{}}));
    request
}
fn processor(websocket: bool, limit: usize) -> (ResponsesProcessor, TcpListener) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap();
    let config = ResponsesConfig {
        http_url: format!("http://{address}/responses"),
        websocket_url: format!("ws://{address}/responses"),
        max_request_bytes: limit,
        ..ResponsesConfig::default()
    };
    let owner = if websocket {
        ResponsesProcessor::websocket(config, request())
    } else {
        ResponsesProcessor::http(config, request())
    }
    .unwrap();
    (owner, listener)
}
fn context(text: &str) -> core::LlmContextData {
    core::LlmContextData {
        messages: vec![core::ChatMessage::new(core::ChatRole::User, text).into()],
        tools: tools(),
        tool_choice: Some(core::ToolChoice::Function("lookup".into())),
    }
}
fn no_io(listener: &TcpListener) {
    assert_eq!(listener.accept().unwrap_err().kind(), ErrorKind::WouldBlock);
}

#[test]
fn valid_preflights_preserve_the_complete_current_request_without_transport_io() {
    for websocket in [false, true] {
        let (owner, listener) = processor(websocket, ResponsesConfig::default().max_request_bytes);
        let mut delta = ResponsesSettings::default();
        delta.llm.base.model = Setting::Value("gpt-4.1-current".into());
        delta.llm.temperature = Setting::Value(0.3);
        delta.max_completion_tokens = Setting::Value(128);
        owner.update_settings(&delta).unwrap();
        let before = owner.request_snapshot().unwrap();
        assert_eq!(before.effective_model().unwrap(), "gpt-4.1-current");
        owner.preflight_context(&context("proposed input")).unwrap();
        assert_eq!(owner.request_snapshot().unwrap(), before);
        owner.preflight_tools(&tools()).unwrap();
        assert_eq!(owner.request_snapshot().unwrap(), before);
        owner
            .preflight_legacy_context(&core::LlmContextFrame {
                messages: vec![core::ChatMessage::new(
                    core::ChatRole::User,
                    "legacy proposal",
                )],
                tools: vec![definition(json!({"type":"object"}))],
                tool_choice: Some(core::ToolChoice::Function("lookup".into())),
            })
            .unwrap();
        assert_eq!(owner.request_snapshot().unwrap(), before);
        no_io(&listener);
    }
}

#[test]
fn invalid_preflights_are_atomic_and_use_the_actual_transport_request_budget() {
    for websocket in [false, true] {
        let (owner, listener) = processor(websocket, ResponsesConfig::default().max_request_bytes);
        let before = owner.request_snapshot().unwrap();
        let invalid_tools = core::ToolSet::Functions(vec![definition(serde_json::Value::Null)]);
        assert!(owner.preflight_tools(&invalid_tools).is_err());
        assert_eq!(owner.request_snapshot().unwrap(), before);
        assert!(owner.preflight_tools(&core::ToolSet::NotGiven).is_err());
        assert_eq!(owner.request_snapshot().unwrap(), before);
        let direct = core::ToolSet::Direct(vec![core::DirectFunction {
            schema: definition(json!({"type":"object"})),
            handler: core::LocalToolFunction::new(|_| async {
                panic!("preflight must never invoke a Direct handler")
            }),
        }]);
        assert!(matches!(
            owner.preflight_tools(&direct),
            Err(ServiceError::Unsupported(_))
        ));
        assert_eq!(owner.request_snapshot().unwrap(), before);
        let custom = core::ToolSet::Schema(core::ToolsSchema {
            standard_tools: vec![],
            custom_tools: Some(BTreeMap::from([(
                core::AdapterType::Openai,
                vec![BTreeMap::from([(
                    "type".into(),
                    json!("web_search_preview"),
                )])],
            )])),
        });
        assert!(matches!(
            owner.preflight_tools(&custom),
            Err(ServiceError::Unsupported(_))
        ));
        assert_eq!(owner.request_snapshot().unwrap(), before);
        let mut invalid_context = context("proposal");
        invalid_context.tools = invalid_tools;
        assert!(owner.preflight_context(&invalid_context).is_err());
        assert_eq!(owner.request_snapshot().unwrap(), before);
        invalid_context = context("proposal");
        invalid_context.tool_choice = Some(core::ToolChoice::Function("unadvertised".into()));
        assert!(owner.preflight_context(&invalid_context).is_err());
        assert_eq!(owner.request_snapshot().unwrap(), before);
        let mut audio: core::ContextMessage =
            core::ChatMessage::new(core::ChatRole::User, "audio").into();
        audio.content = core::MessageContent::Parts(vec![core::MessagePart::Audio {
            data: "AA==".into(),
            format: "wav".into(),
        }]);
        invalid_context = context("proposal");
        invalid_context.messages = vec![audio];
        assert!(matches!(
            owner.preflight_context(&invalid_context),
            Err(ServiceError::Unsupported(_))
        ));
        assert_eq!(owner.request_snapshot().unwrap(), before);
        let mut named = core::ChatMessage::new(core::ChatRole::User, "legacy proposal");
        named.name = Some("application-user".into());
        assert!(matches!(
            owner.preflight_legacy_context(&core::LlmContextFrame {
                messages: vec![named],
                tools: vec![],
                tool_choice: None,
            }),
            Err(ServiceError::Unsupported(_))
        ));
        assert_eq!(owner.request_snapshot().unwrap(), before);
        no_io(&listener);
    }
    // WebSocket's response.create envelope is larger than the HTTP streaming body.
    // Its preflight must use that client's exact budget, not the inference HTTP client.
    let proposal = context(&"x".repeat(2000));
    let mut candidate = request();
    candidate.context = ResponsesContext::from_core(&proposal).unwrap();
    let http_limit = candidate.body(true).unwrap().to_string().len();
    for websocket in [false, true] {
        let (owner, listener) = processor(websocket, http_limit);
        let before = owner.request_snapshot().unwrap();
        let result = owner.preflight_context(&proposal);
        assert_eq!(result.is_err(), websocket);
        assert_eq!(owner.request_snapshot().unwrap(), before);
        no_io(&listener);
    }
}
