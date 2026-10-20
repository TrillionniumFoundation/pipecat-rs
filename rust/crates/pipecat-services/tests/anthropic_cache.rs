//! Fixed-source Anthropic cache placement with explicit native validation.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn request() -> CompletionRequest {
    let mut request = CompletionRequest::new(
        "claude-fixture",
        Context::new(vec![Message::text(Role::User, "hello")]),
    );
    request.system_instruction = Some("shared prefix".into());
    request
}
fn policy(ttl: Option<AnthropicCacheTtl>) -> AnthropicPromptCachePolicy {
    AnthropicPromptCachePolicy {
        enabled: true,
        system_ttl: ttl,
    }
}
fn body(request: &CompletionRequest) -> Value {
    request_body(Provider::Anthropic, request).unwrap()
}
fn cache_count(value: &Value) -> usize {
    match value {
        Value::Array(v) => v.iter().map(cache_count).sum(),
        Value::Object(v) => {
            usize::from(v.contains_key("cache_control"))
                + v.values().map(cache_count).sum::<usize>()
        }
        _ => 0,
    }
}
#[test]
fn absent_and_explicit_disabled_policy_keep_exact_default_wire_shape() {
    let request = request();
    let expected = json!({"model":"claude-fixture","stream":true,"max_tokens":4096,
        "system":"shared prefix","messages":[{"role":"user","content":[{"type":"text","text":"hello"}]}]});
    assert_eq!(body(&request), expected);
    for ttl in [
        None,
        Some(AnthropicCacheTtl::FiveMinutes),
        Some(AnthropicCacheTtl::OneHour),
    ] {
        let request = request
            .clone()
            .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
                enabled: false,
                system_ttl: ttl,
            });
        assert_eq!(body(&request), expected);
    }
}
#[test]
fn system_ttl_is_separate_from_message_ttl_and_input_never_mutates() {
    for (ttl, expected) in [
        (None, json!({"type":"ephemeral"})),
        (
            Some(AnthropicCacheTtl::FiveMinutes),
            json!({"type":"ephemeral","ttl":"5m"}),
        ),
        (
            Some(AnthropicCacheTtl::OneHour),
            json!({"type":"ephemeral","ttl":"1h"}),
        ),
    ] {
        let request = request().with_anthropic_prompt_cache(policy(ttl));
        let before = serde_json::to_value(&request).unwrap();
        let result = body(&request);
        assert_eq!(
            result["system"][0],
            json!({"type":"text","text":"shared prefix","cache_control":expected})
        );
        assert_eq!(
            result["messages"][0]["content"][0]["cache_control"],
            json!({"type":"ephemeral"})
        );
        assert!(result.get("enable_prompt_caching").is_none());
        assert!(result.get("system_prompt_cache_ttl").is_none());
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
        assert_eq!(body(&request), result);
    }
}
#[test]
fn markers_follow_actual_role_merging_and_only_last_two_user_blocks() {
    let mut request = request().with_anthropic_prompt_cache(policy(None));
    request.context.messages = vec![
        Message::text(Role::User, "old"),
        Message::text(Role::Assistant, "a"),
        Message::text(Role::User, "recent-1"),
        Message::text(Role::User, "recent-2"),
        Message::text(Role::Assistant, "b"),
        Message::text(Role::User, "newest"),
        Message::text(Role::User, "last block"),
        Message::text(Role::Assistant, "prefill"),
    ];
    let result = body(&request);
    let messages = result["messages"].as_array().unwrap();
    assert_eq!(messages.len(), 7);
    assert_eq!(cache_count(&messages[2]), 0);
    assert_eq!(
        messages[6],
        json!({"role":"user","content":[{"type":"text","text":".","cache_control":{"type":"ephemeral"}}]})
    );
    assert!(messages[0]["content"][0].get("cache_control").is_none());
    assert_eq!(messages[4]["content"].as_array().unwrap().len(), 2);
    assert!(messages[4]["content"][0].get("cache_control").is_none());
    assert_eq!(
        messages[4]["content"][1]["cache_control"],
        json!({"type":"ephemeral"})
    );
    for i in [1, 3, 5] {
        assert_eq!(cache_count(&messages[i]), 0);
    }
    assert_eq!(cache_count(&result), 3);
}
#[test]
fn tool_results_are_user_breakpoints_and_signed_assistant_is_unchanged() {
    let mut thinking = Message::text(Role::Assistant, "");
    thinking.content = Some(Content::Parts(vec![ContentPart::Thinking {
        text: "opaque thought".into(),
        signature: Some("opaque signature".into()),
    }]));
    thinking.tool_calls.push(ToolCall {
        id: "call-1".into(),
        name: "lookup".into(),
        arguments: json!({}),
    });
    let mut request = request();
    request.context.messages = vec![
        Message::text(Role::User, "ask"),
        thinking,
        Message::tool_result("call-1", "result"),
    ];
    let before = body(&request);
    let result = body(&request.with_anthropic_prompt_cache(policy(None)));
    assert_eq!(result["messages"][1], before["messages"][1]);
    assert_eq!(result["messages"][2]["content"][0]["type"], "tool_result");
    assert_eq!(
        result["messages"][2]["content"][0]["cache_control"],
        json!({"type":"ephemeral"})
    );
    assert!(result["messages"][2]["content"][0]["content"][0]
        .get("cache_control")
        .is_none());
}
#[test]
fn absent_system_zero_users_and_empty_content_stay_well_defined() {
    let mut request = request().with_anthropic_prompt_cache(policy(None));
    request.system_instruction = None;
    request.context.messages.clear();
    let result = body(&request);
    assert!(result.get("system").is_none());
    assert_eq!(cache_count(&result), 0);
    request
        .context
        .messages
        .push(Message::text(Role::Assistant, "prefill"));
    assert_eq!(cache_count(&body(&request)), 1);
    request.context.messages.push(Message::text(Role::User, ""));
    let result = body(&request);
    assert_eq!(result["messages"][1]["content"][0]["text"], "(empty)");
    assert_eq!(cache_count(&result), 1);
}
#[test]
fn policy_values_fail_closed_even_when_disabled_and_errors_are_payload_free() {
    for (key, value) in [
        ("enable_prompt_caching", json!(null)),
        ("enable_prompt_caching", json!("secret invalid")),
        ("system_prompt_cache_ttl", json!("secret invalid")),
        ("system_prompt_cache_ttl", json!(3600)),
        ("system_prompt_cache_ttl", json!({"ttl":"1h"})),
    ] {
        let mut request = request().with_anthropic_prompt_cache(Default::default());
        request.settings.base.extra.insert(key.into(), value);
        let before = serde_json::to_value(&request).unwrap();
        let error = request_body(Provider::Anthropic, &request).unwrap_err();
        assert!(!error.to_string().contains("secret"));
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
    }
    for provider in [Provider::OpenAi, Provider::Gemini] {
        for enabled in [false, true] {
            let request = request().with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
                enabled,
                system_ttl: None,
            });
            assert!(matches!(
                request_body(provider, &request),
                Err(ServiceError::Unsupported(_))
            ));
        }
    }
}
#[test]
fn canonical_settings_override_raw_policy_and_automatic_conflicts_are_explicit() {
    let mut request = request();
    request
        .extra
        .insert("enable_prompt_caching".into(), json!(false));
    request
        .extra
        .insert("system_prompt_cache_ttl".into(), json!("wrong raw ttl"));
    let mut request = request.with_anthropic_prompt_cache(policy(Some(AnthropicCacheTtl::OneHour)));
    assert_eq!(body(&request)["system"][0]["cache_control"]["ttl"], "1h");
    request
        .extra
        .insert("cache_control".into(), json!({"type":"ephemeral"}));
    assert!(request_body(Provider::Anthropic, &request).is_err());
    request
        .settings
        .base
        .extra
        .insert("enable_prompt_caching".into(), json!(false));
    assert!(request_body(Provider::Anthropic, &request).is_err());
    // Existing automatic-extra escape hatch is unchanged when no explicit policy exists.
    let mut automatic = super_request();
    automatic
        .extra
        .insert("cache_control".into(), json!({"type":"ephemeral"}));
    assert_eq!(
        body(&automatic)["cache_control"],
        json!({"type":"ephemeral"})
    );
}
fn super_request() -> CompletionRequest {
    request()
}
#[test]
fn settings_omission_retains_ttl_null_clears_and_failed_proposals_are_atomic() {
    let processor = LlmProcessor::new(
        LlmClient::new(ClientConfig::new(Provider::Anthropic, "synthetic")).unwrap(),
        request().with_anthropic_prompt_cache(policy(Some(AnthropicCacheTtl::OneHour))),
    );
    let before = processor.request_snapshot().unwrap();
    for enabled in [false, true] {
        let delta = settings::settings_from_mapping(
            &[("enable_prompt_caching".into(), json!(enabled))]
                .into_iter()
                .collect(),
        )
        .unwrap();
        processor.update_settings(&delta).unwrap();
        let result = body(&processor.request_snapshot().unwrap());
        if enabled {
            assert_eq!(result["system"][0]["cache_control"]["ttl"], "1h");
        } else {
            assert_eq!(cache_count(&result), 0);
        }
    }
    let delta = settings::settings_from_mapping(
        &[("system_prompt_cache_ttl".into(), json!(null))]
            .into_iter()
            .collect(),
    )
    .unwrap();
    processor.update_settings(&delta).unwrap();
    let current = processor.request_snapshot().unwrap();
    assert_eq!(
        body(&current)["system"][0]["cache_control"],
        json!({"type":"ephemeral"})
    );
    let bad = settings::settings_from_mapping(
        &[
            ("enable_prompt_caching".into(), json!(false)),
            ("system_prompt_cache_ttl".into(), json!("invalid")),
        ]
        .into_iter()
        .collect(),
    )
    .unwrap();
    assert!(processor.update_settings(&bad).is_err());
    assert_eq!(body(&processor.request_snapshot().unwrap()), body(&current));
    assert_eq!(body(&before)["system"][0]["cache_control"]["ttl"], "1h");
}

// Normalize only the existing native adapter's string-content representation to
// typed text-block arrays. No marker, role, ordering or literal text is changed.
fn block_arrays(value: &mut Value) {
    match value {
        Value::Array(values) => values.iter_mut().for_each(block_arrays),
        Value::Object(object) => {
            if let Some(Value::String(text)) = object.get("content") {
                let text = text.clone();
                object.insert("content".into(), json!([{"type":"text","text":text}]));
            }
            object.values_mut().for_each(block_arrays);
        }
        _ => {}
    }
}
fn from_source_wire(input: &Value) -> Context {
    let mut messages = vec![];
    for wire in input.as_array().unwrap() {
        let role = if wire["role"] == "assistant" {
            Role::Assistant
        } else {
            Role::User
        };
        if let Some(text) = wire["content"].as_str() {
            messages.push(Message::text(role, text));
            continue;
        }
        let blocks = wire["content"].as_array().unwrap();
        if blocks.is_empty() {
            messages.push(Message::text(role, ""));
        }
        for block in blocks {
            if block["type"] == "tool_result" {
                messages.push(Message::tool_result(
                    block["tool_use_id"].as_str().unwrap(),
                    block["content"].as_str().unwrap(),
                ));
            } else {
                let part = if block["type"] == "thinking" {
                    ContentPart::Thinking {
                        text: block["thinking"].as_str().unwrap().into(),
                        signature: Some(block["signature"].as_str().unwrap().into()),
                    }
                } else {
                    assert_eq!(block["type"], "text");
                    ContentPart::Text {
                        text: block["text"].as_str().unwrap().into(),
                    }
                };
                let mut message = Message::text(role, "");
                message.content = Some(Content::Parts(vec![part]));
                messages.push(message);
            }
        }
    }
    Context::new(messages)
}
#[test]
fn original_source_oracle_matches_twelve_supported_message_cases_and_system_ttl() {
    let oracle: Value =
        serde_json::from_str(include_str!("fixtures/anthropic_cache_source.json")).unwrap();
    assert_eq!(
        oracle["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    let mut tested = 0;
    let mut system = 0;
    for case in oracle["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        if case.get("merged_wire").is_some() && !name.starts_with("malformed_") {
            // This oracle tests cache helpers without service-level padding.
            let request =
                CompletionRequest::new("claude-3-fixture", from_source_wire(&case["input"]))
                    .with_anthropic_prompt_cache(policy(None));
            let actual = body(&request)["messages"].clone();
            let mut expected = case["output"].clone();
            block_arrays(&mut expected);
            assert_eq!(actual, expected, "{name}");
            tested += 1;
        } else if name.starts_with("system_ttl_") {
            let mut request = request();
            request.system_instruction = Some(case["input"]["system"].as_str().unwrap().into());
            request
                .extra
                .insert("enable_prompt_caching".into(), json!(true));
            request.extra.insert(
                "system_prompt_cache_ttl".into(),
                case["input"]["ttl"].clone(),
            );
            if name == "system_ttl_invalid" {
                assert!(request_body(Provider::Anthropic, &request).is_err());
            } else {
                assert_eq!(body(&request)["system"], case["output"], "{name}");
            }
            system += 1;
        }
    }
    assert_eq!((tested, system), (12, 4));
}
