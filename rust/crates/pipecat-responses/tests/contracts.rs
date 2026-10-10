//! Contracts derived from the pinned Responses adapter and settings tests.
use pipecat_core::Setting;
use pipecat_responses::*;
use pipecat_services::{Content, ContentPart, Context, Message, Role};
use serde_json::json;
mod common;
use common::*;

#[test]
fn request_uses_responses_shape_and_never_mutates_input() {
    let mut req = request();
    req.settings.llm.system_instruction = Setting::Value("system".into());
    req.settings.max_completion_tokens = Setting::Value(80);
    req.settings.service_tier = Setting::Value("fast".into());
    req.context.tools = Some(vec![ToolDefinition {
        name: "lookup".into(),
        description: "Find".into(),
        parameters: json!({"type":"object"}),
    }]);
    req.context.tool_choice = Some(ToolChoice::Function("lookup".into()));
    req.context.items.extend([
        InputItem::FunctionCall(ToolCall {
            id: "call".into(),
            name: "lookup".into(),
            arguments: json!({"a":1}),
        }),
        InputItem::FunctionOutput {
            call_id: "call".into(),
            output: "done".into(),
        },
    ]);
    let before = req.clone();
    let wire = req.body(true).unwrap();
    assert_eq!(wire["store"], false);
    assert_eq!(wire["instructions"], "system");
    assert_eq!(wire["max_output_tokens"], 80);
    assert_eq!(wire["service_tier"], "fast");
    assert_eq!(wire["tools"][0]["name"], "lookup");
    assert!(wire["tools"][0].get("function").is_none());
    assert_eq!(
        wire["tool_choice"],
        json!({"type":"function","name":"lookup"})
    );
    assert_eq!(wire["input"][1]["arguments"], "{\"a\":1}");
    assert_eq!(wire["input"][2]["type"], "function_call_output");
    assert_eq!(req, before);
}
#[test]
fn empty_input_instruction_and_system_role_use_developer() {
    let mut req = request();
    req.context.items.clear();
    req.settings.llm.system_instruction = Setting::Value("instruction".into());
    let body = req.body(true).unwrap();
    assert_eq!(
        body["input"],
        json!([{"role":"developer","content":"instruction"}])
    );
    assert!(body.get("instructions").is_none());
    req.context.items = vec![InputItem::text(Role::System, "context system")];
    assert_eq!(req.body(true).unwrap()["input"][0]["role"], "developer");
}
#[test]
fn reasoning_classification_matches_source_cases() {
    for model in ["gpt-5.1", "gpt-5.4", "gpt-6-sol", "gpt-5.6-2026-01-01"] {
        assert_eq!(default_reasoning_effort(model), Some("none"));
    }
    for model in ["gpt-5", "gpt-5-mini", "gpt-5-nano-2025-08-07"] {
        assert_eq!(default_reasoning_effort(model), Some("minimal"));
    }
    for model in [
        "o3",
        "o4-mini",
        "gpt-6-astra",
        "gpt-6.1-sol",
        "gpt-5.4-pro-2026-01-01",
        "gpt-5-chat-latest",
        "gpt-4.1",
        "custom",
    ] {
        assert_eq!(default_reasoning_effort(model), None, "{model}");
    }
    assert_eq!(model_supports_reasoning("custom"), None);
}
#[test]
fn reasoning_explicit_empty_null_and_known_unsupported() {
    let mut req = request();
    req.model = "gpt-5.4".into();
    req.settings.reasoning = Setting::Value(ReasoningConfig::default());
    assert_eq!(
        req.body(true).unwrap()["reasoning"],
        json!({"effort":"none"})
    );
    req.settings.reasoning = Setting::Value(ReasoningConfig {
        effort: Some("future".into()),
        summary: Some("auto".into()),
        mode: Some("pro".into()),
    });
    assert_eq!(
        req.body(true).unwrap()["include"],
        json!(["reasoning.encrypted_content"])
    );
    assert_eq!(req.body(true).unwrap()["reasoning"]["mode"], "pro");
    req.model = "gpt-4.1".into();
    assert!(matches!(req.body(true), Err(ServiceError::Unsupported(_))));
    req.settings.reasoning = Setting::Null;
    assert!(req.body(true).unwrap().get("reasoning").is_none());
}
#[test]
fn rich_context_maps_media_and_rejects_audio_without_silent_loss() {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(vec![
        ContentPart::Text { text: "see".into() },
        ContentPart::ImageUrlDetailed {
            url: "https://example.test/image".into(),
            detail: "high".into(),
        },
        ContentPart::File {
            data: "eA==".into(),
            mime_type: "application/pdf".into(),
            filename: Some("paper.pdf".into()),
        },
    ]));
    let mut req = request();
    req.context = ResponsesContext::try_from(&Context::new(vec![message.clone()])).unwrap();
    let body = req.body(true).unwrap();
    assert_eq!(body["input"][0]["content"][0]["type"], "input_text");
    assert_eq!(body["input"][0]["content"][1]["detail"], "high");
    assert_eq!(
        body["input"][0]["content"][2]["file_data"],
        "data:application/pdf;base64,eA=="
    );
    message.content = Some(Content::Parts(vec![ContentPart::InputAudio {
        data: "data".into(),
        format: "wav".into(),
    }]));
    assert!(ResponsesContext::try_from(&Context::new(vec![message])).is_err());
}
#[test]
fn encrypted_reasoning_logging_is_redacted_and_core_roundtrip_is_lossless() {
    let item = ReasoningItem {
        id: "rs_1".into(),
        summary: vec!["summary".into()],
        encrypted_content: Some("opaque-secret".into()),
    };
    let context = ResponsesContext::new(vec![
        InputItem::Reasoning(item.clone()),
        InputItem::Message {
            role: Role::User,
            content: InputContent::Parts(vec![InputPart::InputImage {
                image_url: "https://example.test/secret".into(),
                detail: None,
            }]),
        },
    ]);
    let logged = context.for_logging().unwrap().to_string();
    assert!(!logged.contains("opaque-secret"));
    assert!(!logged.contains("example.test"));
    assert!(!format!("{context:?}").contains("opaque-secret"));
    let core = pipecat_core::LlmContextData {
        messages: vec![item.to_core_message()],
        tools: pipecat_core::ToolSet::NotGiven,
        tool_choice: None,
    };
    assert_eq!(
        ResponsesContext::from_core(&core).unwrap().items,
        vec![InputItem::Reasoning(item)]
    );
    assert!(context.items[0].clone() == context.items[0]);
}
#[test]
fn immutable_settings_snapshots_and_reserved_extras() {
    let req = request();
    let mut delta = ResponsesSettings::default();
    delta.llm.temperature = Setting::Value(0.5);
    delta.max_completion_tokens = Setting::Value(20);
    let mut candidate = req.clone();
    candidate.settings.merge(&delta);
    assert!(req.body(true).unwrap().get("temperature").is_none());
    delta.llm.temperature = Setting::Null;
    candidate.settings.merge(&delta);
    assert!(candidate.body(true).unwrap().get("temperature").is_none());
    assert_eq!(candidate.body(true).unwrap()["max_output_tokens"], 20);
    for key in [
        "store",
        "input",
        "previous_response_id",
        "model",
        "stream",
        "tools",
        "background",
        "reasoning",
    ] {
        let mut req = request();
        req.settings.llm.base.extra.insert(key.into(), json!(true));
        assert!(req.body(true).is_err(), "{key}");
    }
}
#[test]
fn unsafe_endpoints_and_header_injection_fail_redacted() {
    for url in [
        "http://example.test/v1/responses",
        "https://u:secret@example.test/",
        "https://example.test/?api_key=secret",
        "https://example.test/#secret",
    ] {
        let cfg = ResponsesConfig {
            http_url: url.into(),
            ..config()
        };
        let error = ResponsesHttpClient::new(cfg).unwrap_err();
        assert!(!error.to_string().contains("secret"));
    }
    let mut cfg = config();
    cfg.headers.insert("Authorization".into(), "secret".into());
    assert!(ResponsesHttpClient::new(cfg).is_err());
    let cfg = config();
    assert!(!format!("{cfg:?}").contains("synthetic-secret"));
}

#[test]
fn runtime_provider_fields_are_typed_and_unknown_reasoning_is_rejected() {
    let mut llm = pipecat_core::LlmSettings::default();
    llm.base
        .extra
        .insert("max_completion_tokens".into(), json!(32));
    llm.base.extra.insert(
        "reasoning".into(),
        json!({"effort":"high","summary":"auto"}),
    );
    llm.base.extra.insert("service_tier".into(), json!("fast"));
    let settings = ResponsesSettings::from_llm(&llm).unwrap();
    let mut req = request();
    req.model = "gpt-5.4".into();
    req.settings = settings;
    let body = req.body(true).unwrap();
    assert_eq!(body["max_output_tokens"], 32);
    assert_eq!(body["reasoning"]["effort"], "high");
    assert_eq!(body["service_tier"], "fast");
    llm.base
        .extra
        .insert("reasoning".into(), json!({"unknown":"private-secret"}));
    let error = ResponsesSettings::from_llm(&llm).unwrap_err();
    assert!(!format!("{error:?}").contains("private-secret"));
}
#[test]
fn response_schema_rejects_source_known_unsupported_models() {
    for model in [
        "gpt-3.5-turbo",
        "gpt-4",
        "gpt-4-0613",
        "gpt-4o-2024-05-13",
        "chatgpt-4o-latest",
        "o1-mini",
    ] {
        let mut req = request();
        req.model = model.into();
        req.response_schema = Some(json!({"type":"object"}));
        assert!(matches!(req.body(false), Err(ServiceError::Unsupported(_))));
    }
    for model in ["gpt-4o-mini", "gpt-4.1", "gpt-5.4", "custom"] {
        assert!(model_supports_response_schema(model));
    }
}
