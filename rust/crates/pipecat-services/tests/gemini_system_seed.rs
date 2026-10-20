//! Pinned Python startup conversion contracts, not live model behavior claims.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn request(messages: Vec<Message>) -> CompletionRequest {
    let mut request = CompletionRequest::new("gemini-3.8-flash", Context::new(messages));
    request.system_instruction = Some("settings system".into());
    request
}
fn wire(request: &CompletionRequest) -> Value {
    let before = serde_json::to_value(request).unwrap();
    let body = request_body(Provider::Gemini, request).unwrap();
    assert_eq!(request_body(Provider::Gemini, request).unwrap(), body);
    assert_eq!(serde_json::to_value(request).unwrap(), before);
    body
}
fn seed(text: &str) -> Value {
    json!({"role":"user","parts":[{"text":text}]})
}
fn call() -> Message {
    Message::assistant_tool_calls(vec![ToolCall {
        id: "call-1".into(),
        name: "echo".into(),
        arguments: json!({"input":"hello"}),
    }])
}
fn parts(parts: Vec<ContentPart>) -> Message {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(parts));
    message
}

#[test]
fn empty_context_seeds_direct_and_typed_settings_without_mutation() {
    for typed in [false, true] {
        let mut request = request(vec![]);
        if typed {
            request.system_instruction = Some("superseded".into());
            request.settings.system_instruction = Setting::Value("settings system".into());
        }
        let body = wire(&request);
        assert_eq!(body["contents"], json!([seed("settings system")]));
        assert_eq!(
            body["systemInstruction"],
            json!({"parts":[{"text":"settings system"}]})
        );
    }
}

#[test]
fn absent_empty_and_null_system_do_not_invent_seed() {
    for system in [None, Some(String::new())] {
        let mut request = request(vec![]);
        request.system_instruction = system;
        let body = wire(&request);
        assert_eq!(body["contents"], json!([]));
        assert!(body.get("systemInstruction").is_none());
    }
    let mut request = request(vec![]);
    request.settings.system_instruction = Setting::Null;
    assert_eq!(wire(&request)["contents"], json!([]));
}

#[test]
fn regular_turns_and_single_system_do_not_duplicate_seed() {
    for role in [Role::User, Role::System, Role::Developer] {
        let request = request(vec![Message::text(role, "existing")]);
        assert_eq!(wire(&request)["contents"], json!([seed("existing")]));
    }
    let mut request = request(vec![Message::text(Role::System, "existing")]);
    request.system_instruction = None;
    let body = wire(&request);
    assert_eq!(body["contents"], json!([seed("existing")]));
    assert!(body.get("systemInstruction").is_none());
}

#[test]
fn tool_only_history_appends_distinct_seed_after_results() {
    let request = request(vec![
        call(),
        Message::tool_result("call-1", r#"{"ok":true}"#),
    ]);
    let body = wire(&request);
    assert_eq!(
        body["contents"],
        json!([
            {"role":"model","parts":[{"functionCall":{"id":"call-1","name":"echo","args":{"input":"hello"}},"thoughtSignature":"skip_thought_signature_validator"}]},
            {"role":"user","parts":[{"functionResponse":{"id":"call-1","name":"echo","response":{"ok":true}}}]},
            seed("settings system")
        ])
    );
}

#[test]
fn extracted_system_wins_seed_but_settings_win_system_instruction() {
    for initial in [Message::text(Role::System, "context system"), {
        let mut m = parts(vec![
            ContentPart::Text {
                text: "context".into(),
            },
            ContentPart::Text {
                text: "system".into(),
            },
        ]);
        m.role = Role::System;
        m
    }] {
        let request = request(vec![initial, call(), Message::tool_result("call-1", "{}")]);
        let body = wire(&request);
        assert_eq!(body["contents"][2], seed("context system"));
        assert_eq!(
            body["systemInstruction"],
            json!({"parts":[{"text":"settings system"}]})
        );
    }
    for initial in ["", "context system"] {
        let mut request = request(vec![Message::text(Role::System, initial), call()]);
        let body = wire(&request);
        assert_eq!(
            body["contents"][1],
            seed(if initial.is_empty() {
                "settings system"
            } else {
                initial
            })
        );
        request.system_instruction = None;
        let body = wire(&request);
        assert_eq!(
            body["contents"][1],
            seed(if initial.is_empty() { "." } else { initial })
        );
    }
}

#[test]
fn regular_predicate_requires_exactly_one_nonempty_text_part_before_role_merge() {
    for content in [
        vec![ContentPart::Text { text: "".into() }],
        vec![
            ContentPart::Text {
                text: "caption".into(),
            },
            ContentPart::InlineData {
                mime_type: "image/png".into(),
                data: "AQ==".into(),
            },
        ],
        vec![
            ContentPart::Text { text: "one".into() },
            ContentPart::Text { text: "two".into() },
        ],
        vec![ContentPart::InlineData {
            mime_type: "image/png".into(),
            data: "AQ==".into(),
        }],
    ] {
        let body = wire(&request(vec![parts(content)]));
        assert_eq!(body["contents"].as_array().unwrap().len(), 2);
        assert_eq!(body["contents"][1], seed("settings system"));
    }
    for text in ["ordinary", " "] {
        assert_eq!(
            wire(&request(vec![Message::text(Role::User, text)]))["contents"],
            json!([seed(text)])
        );
    }
    let body = wire(&request(vec![
        Message::text(Role::User, "one"),
        Message::text(Role::User, "two"),
    ]));
    assert_eq!(
        body["contents"],
        json!([{"role":"user","parts":[{"text":"one"},{"text":"two"}]}])
    );
    let body = wire(&request(vec![Message::text(Role::Assistant, "answer")]));
    assert_eq!(body["contents"][1], seed("."));
}

#[test]
fn seed_precedes_trailing_user_enforcement_even_for_prefill_models() {
    for model in [
        "gemini-3.8-flash",
        "gemini-2.5-flash",
        "gemini-3-pro",
        "gemini-3.1-pro",
        "gemini-pro-latest",
    ] {
        let mut request = request(vec![call()]);
        request.model = model.into();
        let body = wire(&request);
        assert_eq!(body["contents"].as_array().unwrap().len(), 2);
        assert_eq!(body["contents"][1], seed("settings system"));
    }
}

#[test]
fn signed_tool_and_text_histories_keep_anchors_and_exact_regular_predicate() {
    let mut signed = call();
    signed.content = Some(Content::Parts(vec![ContentPart::GeminiToolMetadata {
        tool_call_id: "call-1".into(),
        signature: Some("opaque-anchor".into()),
    }]));
    let body = wire(&request(vec![signed, Message::tool_result("call-1", "{}")]));
    assert_eq!(
        body["contents"][0]["parts"][0]["thoughtSignature"],
        "opaque-anchor"
    );
    assert_eq!(body["contents"][2], seed("settings system"));
    let mut thought = parts(vec![ContentPart::GeminiText {
        text: "thought".into(),
        thought: true,
        signature: Some("opaque-text".into()),
        incomplete: false,
    }]);
    thought.role = Role::Assistant;
    let body = wire(&request(vec![thought]));
    assert_eq!(body["contents"].as_array().unwrap().len(), 2);
    assert_eq!(
        body["contents"][0]["parts"][0]["thoughtSignature"],
        "opaque-text"
    );
    assert_eq!(body["contents"][1], seed("."));
}

#[test]
fn other_providers_and_anthropic_cache_remain_isolated() {
    let request = request(vec![]);
    let openai = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(
        openai["messages"],
        json!([{"role":"system","content":"settings system"}])
    );
    let anthropic = request_body(Provider::Anthropic, &request).unwrap();
    assert_eq!(anthropic["messages"], json!([]));
    assert_eq!(anthropic["system"], "settings system");
    let cached = request.with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
        enabled: true,
        system_ttl: None,
    });
    let anthropic = request_body(Provider::Anthropic, &cached).unwrap();
    assert_eq!(anthropic["messages"], json!([]));
    assert_eq!(
        anthropic["system"][0]["cache_control"],
        json!({"type":"ephemeral"})
    );
    assert!(request_body(Provider::Gemini, &cached).is_err());
}

#[test]
fn empty_parts_and_missing_content_do_not_suppress_seed_or_invent_text() {
    for role in [Role::User, Role::Assistant, Role::System] {
        for content in [None, Some(Content::Parts(vec![]))] {
            let mut message = Message::text(role, "");
            message.content = content;
            let mut request = request(vec![message]);
            assert_eq!(wire(&request)["contents"], json!([seed("settings system")]));
            request.system_instruction = None;
            assert_eq!(wire(&request)["contents"], json!([]));
        }
    }
    let mut request = request(vec![Message::text(Role::User, "")]);
    assert_eq!(
        wire(&request)["contents"],
        json!([seed(""), seed("settings system")])
    );
    request.system_instruction = None;
    assert_eq!(wire(&request)["contents"], json!([seed("")]));
    request.context.messages[0].content = Some(Content::Text(" ".into()));
    request.system_instruction = Some("settings system".into());
    assert_eq!(wire(&request)["contents"], json!([seed(" ")]));
}
