//! Source-compatible sparse settings and rich universal message contracts.
use pipecat_core::{self as core, Setting};
use pipecat_services::{adapters::request_body, frame_context, settings, *};
use serde_json::json;

fn request() -> CompletionRequest {
    CompletionRequest::new(
        "old",
        Context::new(vec![Message::text(Role::User, "hello")]),
    )
}
fn rich(parts: Vec<core::MessagePart>) -> core::ContextMessage {
    let mut message: core::ContextMessage = core::ChatMessage::new(core::ChatRole::User, "").into();
    message.content = core::MessageContent::Parts(parts);
    message
}
#[test]
fn sparse_settings_distinguish_omitted_null_and_value() {
    let mut request = request();
    request.temperature = Some(0.25);
    request.max_tokens = Some(32);
    request
        .settings
        .base
        .extra
        .insert("service_tier".into(), json!("auto"));
    let first = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(first["temperature"], 0.25);
    assert_eq!(first["max_completion_tokens"], 32);
    let delta = core::LlmSettings {
        temperature: Setting::Null,
        max_tokens: Setting::Value(64),
        top_p: Setting::Value(0.8),
        ..Default::default()
    };
    settings::merge_settings(&mut request.settings, &delta);
    let second = request_body(Provider::OpenAi, &request).unwrap();
    assert!(second.as_object().unwrap().contains_key("temperature"));
    assert_eq!(second["temperature"], serde_json::Value::Null);
    assert_eq!(second["max_tokens"], 64);
    assert!(second.get("max_completion_tokens").is_none());
    assert_eq!(second["service_tier"], "auto");
    settings::merge_settings(&mut request.settings, &core::LlmSettings::default());
    assert_eq!(request_body(Provider::OpenAi, &request).unwrap(), second);
    settings::merge_settings(
        &mut request.settings,
        &core::LlmSettings {
            temperature: Setting::Value(0.5),
            ..Default::default()
        },
    );
    assert_eq!(
        request_body(Provider::OpenAi, &request).unwrap()["temperature"],
        0.5
    );
    assert_eq!(
        first["temperature"], 0.25,
        "earlier snapshots must remain immutable"
    );
}
#[test]
fn native_providers_receive_settings_in_their_own_wire_locations() {
    let mut request = request();
    request.settings = core::LlmSettings {
        temperature: Setting::Value(0.4),
        top_p: Setting::Null,
        top_k: Setting::Value(9),
        max_tokens: Setting::Value(128),
        ..Default::default()
    };
    let gemini = request_body(Provider::Gemini, &request).unwrap();
    assert_eq!(
        gemini,
        json!({"contents":[{"role":"user","parts":[{"text":"hello"}]}],
        "generationConfig":{"temperature":0.4,"topK":9,"maxOutputTokens":128}})
    );
    let anthropic = request_body(Provider::Anthropic, &request).unwrap();
    assert_eq!(
        anthropic,
        json!({"model":"old","stream":true,"messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}],
        "temperature":0.4,"top_p":null,"top_k":9,"max_tokens":128})
    );
    assert!(matches!(
        request_body(Provider::OpenAi, &request),
        Err(ServiceError::Unsupported(_))
    ));
    request.settings.max_tokens = Setting::Null;
    assert!(matches!(
        request_body(Provider::Anthropic, &request),
        Err(ServiceError::InvalidRequest(_))
    ));
}
#[test]
fn settings_update_is_atomic_and_existing_snapshots_do_not_change() {
    let processor = LlmProcessor::new(
        LlmClient::new(ClientConfig::new(Provider::OpenAi, "synthetic")).unwrap(),
        request(),
    );
    let before = processor.request_snapshot().unwrap();
    let mut delta = core::LlmSettings {
        temperature: Setting::Value(f64::NAN),
        ..Default::default()
    };
    delta.base.model = Setting::Value("changed".into());
    assert!(processor.update_settings(&delta).is_err());
    assert_eq!(
        request_body(Provider::OpenAi, &processor.request_snapshot().unwrap()).unwrap(),
        request_body(Provider::OpenAi, &before).unwrap()
    );
    delta.temperature = Setting::Value(0.6);
    processor.update_settings(&delta).unwrap();
    assert_eq!(
        request_body(Provider::OpenAi, &processor.request_snapshot().unwrap()).unwrap()["model"],
        "changed"
    );
    assert_eq!(
        request_body(Provider::OpenAi, &before).unwrap()["model"],
        "old"
    );
    delta.base.model = Setting::Null;
    assert!(processor.update_settings(&delta).is_err());
    delta.base.model = Setting::NotGiven;
    delta.filter_incomplete_user_turns = Setting::Value(true);
    assert!(matches!(
        processor.update_settings(&delta),
        Err(ServiceError::Unsupported(_))
    ));
}
#[test]
fn deprecated_mapping_checks_types_and_preserves_extra_fields() {
    let map = [
        ("model".into(), json!("new")),
        ("temperature".into(), json!(null)),
        ("max_tokens".into(), json!(42)),
        ("service_tier".into(), json!("flex")),
        (
            "extra".into(),
            json!({"response_format":{"type":"json_object"}}),
        ),
    ]
    .into_iter()
    .collect();
    let delta = settings::settings_from_mapping(&map).unwrap();
    assert_eq!(delta.base.model, Setting::Value("new".into()));
    assert_eq!(delta.temperature, Setting::Null);
    assert_eq!(delta.max_tokens, Setting::Value(42));
    assert_eq!(delta.base.extra["service_tier"], "flex");
    assert_eq!(
        delta.base.extra["response_format"],
        json!({"type":"json_object"})
    );
    assert!(settings::settings_from_mapping(
        &[("temperature".into(), json!("hot"))].into_iter().collect()
    )
    .is_err());
}
#[test]
fn rich_openai_text_image_audio_file_and_tools_are_not_dropped() {
    let mut user = rich(vec![
        core::MessagePart::Text {
            text: "look".into(),
        },
        core::MessagePart::Image {
            url: "https://example.test/a.png".into(),
            detail: Some("high".into()),
        },
        core::MessagePart::Audio {
            data: "YWJj".into(),
            format: "wav".into(),
        },
        core::MessagePart::File {
            data: "cGRm".into(),
            mime_type: "application/pdf".into(),
            filename: Some("note.pdf".into()),
        },
    ]);
    user.name = Some("speaker".into());
    let context = core::LlmContextData {
        messages: vec![user.clone()],
        tools: core::ToolSet::Functions(vec![]),
        tool_choice: Some(core::ToolChoice::None),
    };
    let request = CompletionRequest::new("test", frame_context::context(&context).unwrap());
    let body = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(
        body,
        json!({"model":"test","stream":true,"stream_options":{"include_usage":true},"tools":[],"tool_choice":"none","messages":[
        {"role":"user","name":"speaker","content":[{"type":"text","text":"look"},
        {"type":"image_url","image_url":{"url":"https://example.test/a.png","detail":"high"}},
        {"type":"input_audio","input_audio":{"data":"YWJj","format":"wav"}},
        {"type":"file","file":{"file_data":"data:application/pdf;base64,cGRm","filename":"note.pdf"}}]}]})
    );
    assert_eq!(
        frame_context::to_core_message(&request.context.messages[0]),
        user
    );
}
#[test]
fn rich_tool_calls_and_results_preserve_ids_names_and_json() {
    let mut call: core::ContextMessage =
        core::ChatMessage::new(core::ChatRole::Assistant, "").into();
    call.tool_calls.push(core::FunctionCall {
        function_name: "weather".into(),
        tool_call_id: "abc".into(),
        arguments: json!({"city":"北京"}),
    });
    let mut result: core::ContextMessage =
        core::ChatMessage::new(core::ChatRole::Tool, "{\"temp\":20}").into();
    result.tool_call_id = Some("abc".into());
    let context = core::LlmContextData {
        messages: vec![call, result],
        tools: core::ToolSet::NotGiven,
        tool_choice: None,
    };
    let request = CompletionRequest::new("test", frame_context::context(&context).unwrap());
    let openai = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(
        openai["messages"][0]["tool_calls"][0],
        json!({"id":"abc","type":"function","function":{"name":"weather","arguments":"{\"city\":\"北京\"}"}})
    );
    let gemini = request_body(Provider::Gemini, &request).unwrap();
    assert_eq!(
        gemini["contents"][1]["parts"][0],
        json!({"functionResponse":{"id":"abc","name":"weather","response":{"temp":20}}})
    );
}
#[test]
fn unsupported_rich_forms_are_errors_instead_of_silent_loss() {
    let mut message = rich(vec![core::MessagePart::Text {
        text: "hello".into(),
    }]);
    message
        .extra
        .insert("future_provider_data".into(), json!(true));
    assert!(matches!(
        frame_context::message(&message),
        Err(ServiceError::Unsupported(_))
    ));
    let mut context = request();
    let mut system = Message::text(Role::System, "");
    system.content = Some(Content::Parts(vec![ContentPart::ImageUrl {
        url: "data:image/png;base64,YQ==".into(),
    }]));
    context.context.messages.insert(0, system);
    for provider in [Provider::Anthropic, Provider::Gemini] {
        assert!(matches!(
            request_body(provider, &context),
            Err(ServiceError::Unsupported(_))
        ));
    }
    assert!(matches!(
        frame_context::tools(&core::ToolSet::Grouped(Default::default())),
        Err(ServiceError::Unsupported(_))
    ));
    let schema = core::ToolsSchema {
        standard_tools: vec![],
        custom_tools: Some(
            [(
                core::AdapterType::Openai,
                vec![[("type".into(), json!("web_search"))].into_iter().collect()],
            )]
            .into_iter()
            .collect(),
        ),
    };
    assert!(matches!(
        frame_context::tools(&core::ToolSet::Schema(schema)),
        Err(ServiceError::Unsupported(_))
    ));
}
#[test]
fn signed_thinking_and_refusal_have_explicit_provider_boundaries() {
    let mut thought: core::ContextMessage =
        core::ChatMessage::new(core::ChatRole::Assistant, "").into();
    thought.content = core::MessageContent::Parts(vec![core::MessagePart::Thinking {
        text: "".into(),
        signature: Some("signed".into()),
    }]);
    let request = CompletionRequest::new(
        "test",
        Context::new(vec![frame_context::message(&thought).unwrap()]),
    );
    assert_eq!(
        request_body(Provider::Anthropic, &request).unwrap()["messages"][0]["content"][0],
        json!({"type":"thinking","thinking":"","signature":"signed"})
    );
    for provider in [Provider::OpenAi, Provider::Gemini] {
        assert!(matches!(
            request_body(provider, &request),
            Err(ServiceError::Unsupported(_))
        ));
    }
    thought.content =
        core::MessageContent::Parts(vec![core::MessagePart::Refusal { text: "no".into() }]);
    let request = CompletionRequest::new(
        "test",
        Context::new(vec![frame_context::message(&thought).unwrap()]),
    );
    assert_eq!(
        request_body(Provider::OpenAi, &request).unwrap()["messages"][0]["content"][0],
        json!({"type":"refusal","refusal":"no"})
    );
    assert!(matches!(
        request_body(Provider::Anthropic, &request),
        Err(ServiceError::Unsupported(_))
    ));
}

#[test]
fn empty_system_instruction_matches_python_truthiness_and_null_clears_gemini_sampling() {
    let mut request = request();
    request
        .context
        .messages
        .insert(0, Message::text(Role::System, "context system"));
    request.system_instruction = Some(String::new());
    let openai = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(openai["messages"].as_array().unwrap().len(), 2);
    assert_eq!(
        request_body(Provider::Anthropic, &request).unwrap()["system"],
        "context system"
    );
    assert_eq!(
        request_body(Provider::Gemini, &request).unwrap()["systemInstruction"]["parts"][0]["text"],
        "context system"
    );
    request.temperature = Some(0.4);
    request.max_tokens = Some(32);
    request.settings.temperature = Setting::Null;
    request.settings.max_tokens = Setting::Null;
    assert!(request_body(Provider::Gemini, &request)
        .unwrap()
        .get("generationConfig")
        .is_none());
}
