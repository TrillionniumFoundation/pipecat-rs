//! Pinned Python request-body parity, not live-provider model acceptance.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn request(model: &str, messages: Vec<Message>) -> CompletionRequest {
    CompletionRequest::new(model, Context::new(messages))
}
fn body(request: &CompletionRequest) -> Value {
    request_body(Provider::Anthropic, request).unwrap()
}
fn padding() -> Value {
    json!({"role":"user","content":[{"type":"text","text":"."}]})
}

#[test]
fn all_eight_legacy_prefixes_preserve_prefill_with_arbitrary_suffixes() {
    for prefix in [
        "claude-2",
        "claude-instant",
        "claude-3",
        "claude-opus-4-0",
        "claude-opus-4-1",
        "claude-sonnet-4-0",
        "claude-sonnet-4-5",
        "claude-haiku-4-5",
    ] {
        for suffix in ["", "-20260101", "unexpected-suffix"] {
            let model = format!("{prefix}{suffix}");
            let result = body(&request(
                &model,
                vec![Message::text(Role::Assistant, "prefill")],
            ));
            assert_eq!(
                result["messages"],
                json!([
                    {"role":"assistant","content":[{"type":"text","text":"prefill"}]}
                ]),
                "{model}"
            );
        }
    }
}

#[test]
fn unknown_and_case_variant_models_inject_exactly_one_wire_turn() {
    for model in [
        "claude-sonnet-4-6",
        "claude-opus-4-6",
        "unknown",
        "Claude-3",
        "CLAUDE-2",
        "xclaude-3",
        "claude-sonnet-4",
        " claude-3",
    ] {
        let request = request(model, vec![Message::text(Role::Assistant, "prefill")]);
        let before = serde_json::to_value(&request).unwrap();
        let result = body(&request);
        assert_eq!(result["messages"].as_array().unwrap().len(), 2, "{model}");
        assert_eq!(result["messages"][1], padding(), "{model}");
        assert_eq!(body(&request), result);
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
    }
}

#[test]
fn normalization_follows_merging_and_preserves_tools_and_options() {
    let mut assistant = Message::text(Role::Assistant, "second");
    assistant.tool_calls.push(ToolCall {
        id: "call-1".into(),
        name: "lookup".into(),
        arguments: json!({"key":"value"}),
    });
    let mut request = request(
        "unknown",
        vec![
            Message::text(Role::User, "hello"),
            Message::text(Role::Assistant, "first"),
            assistant,
        ],
    );
    request.max_tokens = Some(42);
    request.temperature = Some(0.25);
    request.system_instruction = Some("system".into());
    request.context.tool_choice = Some(ToolChoice::Required);
    request
        .extra
        .insert("metadata".into(), json!({"user_id":"synthetic"}));
    let result = body(&request);
    assert_eq!(
        result["messages"],
        json!([
            {"role":"user","content":[{"type":"text","text":"hello"}]},
            {"role":"assistant","content":[{"type":"text","text":"first"},{"type":"text","text":"second"},{"type":"tool_use","id":"call-1","name":"lookup","input":{"key":"value"}}]},
            {"role":"user","content":[{"type":"text","text":"."}]}
        ])
    );
    assert_eq!(result["max_tokens"], 42);
    assert_eq!(result["temperature"], 0.25);
    assert_eq!(result["system"], "system");
    assert_eq!(result["tool_choice"], json!({"type":"any"}));
    assert_eq!(result["metadata"], json!({"user_id":"synthetic"}));
}

#[test]
fn empty_user_and_tool_result_endings_are_not_padded() {
    assert_eq!(body(&request("unknown", vec![]))["messages"], json!([]));
    for messages in [
        vec![Message::text(Role::User, "hello")],
        vec![
            Message::text(Role::Assistant, "prefill"),
            Message::text(Role::User, "."),
        ],
        vec![
            Message::text(Role::Assistant, "lookup"),
            Message::tool_result("call-1", "result"),
        ],
    ] {
        let request = request("unknown", messages);
        let result = body(&request);
        assert_eq!(
            result["messages"].as_array().unwrap().len(),
            request.context.messages.len()
        );
        assert_eq!(
            result["messages"].as_array().unwrap().last().unwrap()["role"],
            "user"
        );
        assert_eq!(body(&request), result);
    }
}

#[test]
fn other_providers_keep_their_existing_trailing_turn_behavior() {
    let request = request("unknown", vec![Message::text(Role::Assistant, "prefill")]);
    let openai = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(
        openai["messages"],
        json!([{"role":"assistant","content":"prefill"}])
    );
    let gemini = request_body(Provider::Gemini, &request).unwrap();
    assert_eq!(
        gemini["contents"],
        // Gemini already has its own model-aware final-user normalization.
        json!([{"role":"model","parts":[{"text":"prefill"}]},{"role":"user","parts":[{"text":"."}]}])
    );
}

#[test]
fn injected_turn_displaces_old_cache_breakpoint_but_not_system_ttl() {
    let mut request = request(
        "unknown",
        vec![
            Message::text(Role::User, "old"),
            Message::text(Role::Assistant, "a"),
            Message::text(Role::User, "recent"),
            Message::text(Role::Assistant, "b"),
            Message::text(Role::User, "newest"),
            Message::text(Role::Assistant, "prefill"),
        ],
    )
    .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
        enabled: true,
        system_ttl: Some(AnthropicCacheTtl::OneHour),
    });
    request.system_instruction = Some("shared prefix".into());
    let before = serde_json::to_value(&request).unwrap();
    let result = body(&request);
    assert_eq!(
        result["system"][0]["cache_control"],
        json!({"type":"ephemeral","ttl":"1h"})
    );
    for index in [0, 1, 2, 3, 5] {
        assert!(result["messages"][index]["content"][0]
            .get("cache_control")
            .is_none());
    }
    for index in [4, 6] {
        assert_eq!(
            result["messages"][index]["content"][0]["cache_control"],
            json!({"type":"ephemeral"})
        );
    }
    assert_eq!(result["messages"][6]["content"][0]["text"], ".");
    assert_eq!(body(&request), result);
    assert_eq!(serde_json::to_value(&request).unwrap(), before);
    request.model = "claude-3-fixture".into();
    let legacy = body(&request);
    assert_eq!(legacy["messages"].as_array().unwrap().len(), 6);
    for index in [2, 4] {
        assert_eq!(
            legacy["messages"][index]["content"][0]["cache_control"],
            json!({"type":"ephemeral"})
        );
    }
}
