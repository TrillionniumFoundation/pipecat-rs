//! Pinned Python tool-role early return, not live-provider acceptance.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn request(messages: Vec<Message>) -> CompletionRequest {
    CompletionRequest::new("claude-3-fixture", Context::new(messages))
}
fn wire(request: &CompletionRequest) -> Value {
    let before = serde_json::to_value(request).unwrap();
    let body = request_body(Provider::Anthropic, request).unwrap();
    assert_eq!(request_body(Provider::Anthropic, request).unwrap(), body);
    assert_eq!(serde_json::to_value(request).unwrap(), before);
    body
}
#[test]
fn text_results_preserve_exact_string_type_and_value_with_or_without_cache() {
    for text in [
        "",
        " \n\t ",
        "ordinary result",
        "你好🙂",
        "{}",
        "[1,false]",
        "42",
        "true",
        "null",
        r#""string""#,
        "(empty)",
    ] {
        for cached in [false, true] {
            let request = request(vec![Message::tool_result("call-1", text)])
                .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
                    enabled: cached,
                    system_ttl: None,
                });
            let mut block = json!({"type":"tool_result","tool_use_id":"call-1","content":text});
            if cached {
                block["cache_control"] = json!({"type":"ephemeral"});
            }
            assert_eq!(
                wire(&request)["messages"],
                json!([{"role":"user","content":[block]}])
            );
        }
    }
}
#[test]
fn adjacent_results_preserve_ids_order_and_empty_output_after_calls() {
    let calls = Message::assistant_tool_calls(vec![
        ToolCall {
            id: "first".into(),
            name: "lookup".into(),
            arguments: json!({}),
        },
        ToolCall {
            id: "second".into(),
            name: "search".into(),
            arguments: json!({"q":1}),
        },
    ]);
    let request = request(vec![
        calls,
        Message::tool_result("first", ""),
        Message::tool_result("second", "{}"),
    ]);
    assert_eq!(
        wire(&request)["messages"],
        json!([
            {"role":"assistant","content":[{"type":"tool_use","id":"first","name":"lookup","input":{}},{"type":"tool_use","id":"second","name":"search","input":{"q":1}}]},
            {"role":"user","content":[{"type":"tool_result","tool_use_id":"first","content":""},{"type":"tool_result","tool_use_id":"second","content":"{}"}]}
        ])
    );
}
#[test]
fn rich_missing_and_empty_parts_keep_existing_array_policy() {
    for (content, expected) in [
        (None, json!([])),
        (Some(Content::Parts(vec![])), json!([])),
        (
            Some(Content::Parts(vec![
                ContentPart::Text { text: "".into() },
                ContentPart::ImageUrl {
                    url: "https://example.com/image.png".into(),
                },
            ])),
            json!([{"type":"text","text":"(empty)"},{"type":"image","source":{"type":"url","url":"https://example.com/image.png"}}]),
        ),
    ] {
        let mut result = Message::tool_result("call-1", "unused");
        result.content = content;
        assert_eq!(
            wire(&request(vec![result]))["messages"],
            json!([{"role":"user","content":[{"type":"tool_result","tool_use_id":"call-1","content":expected}]}])
        );
    }
}
#[test]
fn ordinary_empty_messages_keep_sentinel_and_initial_system_policy() {
    let request = request(vec![
        Message::text(Role::System, "system"),
        Message::text(Role::User, ""),
        Message::text(Role::Assistant, ""),
    ]);
    let body = wire(&request);
    assert_eq!(body["system"], "system");
    assert_eq!(
        body["messages"],
        json!([
            {"role":"user","content":[{"type":"text","text":"(empty)"}]},
            {"role":"assistant","content":[{"type":"text","text":"(empty)"}]}
        ])
    );
}
#[test]
fn missing_and_empty_tool_ids_still_fail_validation() {
    for id in [None, Some(String::new())] {
        let mut result = Message::tool_result("call-1", "");
        result.tool_call_id = id;
        let request = request(vec![result]);
        assert!(matches!(
            request.context.validate(),
            Err(ServiceError::InvalidRequest(_))
        ));
        assert!(matches!(
            request_body(Provider::Anthropic, &request),
            Err(ServiceError::InvalidRequest(_))
        ));
    }
}
#[test]
fn other_providers_keep_their_text_and_json_policies() {
    for (text, response) in [
        ("", json!({"value":""})),
        (" \n\t", json!({"value":" \n\t"})),
        ("{}", json!({})),
        ("null", json!({"value":null})),
    ] {
        let request = request(vec![Message::tool_result("call-1", text)]);
        let before = serde_json::to_value(&request).unwrap();
        assert_eq!(
            request_body(Provider::OpenAi, &request).unwrap()["messages"],
            json!([{"role":"tool","content":text,"tool_call_id":"call-1"}])
        );
        assert_eq!(
            request_body(Provider::Gemini, &request).unwrap()["contents"],
            json!([{"role":"user","parts":[{"functionResponse":{"id":"call-1","name":"tool_call_result","response":response}}]}])
        );
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
    }
}
