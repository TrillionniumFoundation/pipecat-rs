//! Pinned standard-message ordering only; no live-provider acceptance claim.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn text(s: &str) -> ContentPart {
    ContentPart::Text { text: s.into() }
}
fn image() -> ContentPart {
    ContentPart::ImageUrl {
        url: "https://example.invalid/image.png".into(),
    }
}
fn pdf() -> ContentPart {
    ContentPart::File {
        data: "JVBERi0xLjQ=".into(),
        mime_type: "application/pdf".into(),
        filename: Some("original.pdf".into()),
    }
}
fn message(parts: Vec<ContentPart>) -> Message {
    let mut message = Message::text(Role::User, "");
    message.content = Some(Content::Parts(parts));
    message
}
fn request(messages: Vec<Message>) -> CompletionRequest {
    CompletionRequest::new("unknown-fixture", Context::new(messages))
}
fn body(request: &CompletionRequest) -> Value {
    request_body(Provider::Anthropic, request).unwrap()
}
fn converted(part: ContentPart) -> Value {
    body(&request(vec![message(vec![part])]))["messages"][0]["content"][0].clone()
}

#[test]
fn each_single_media_representation_moves_before_first_text_without_payload_changes() {
    for media in [
        image(),
        pdf(),
        ContentPart::ImageUrl {
            url: "data:image/png;base64,aW1hZ2U=".into(),
        },
        ContentPart::InlineData {
            mime_type: "image/jpeg".into(),
            data: "opaque+/=".into(),
        },
        ContentPart::ImageUrlDetailed {
            url: "https://example.invalid/detailed.png".into(),
            detail: "auto".into(),
        },
        ContentPart::InlineData {
            mime_type: "application/pdf".into(),
            data: "JVBERi0xLjQ=".into(),
        },
    ] {
        let expected_media = converted(media.clone());
        let request = request(vec![message(vec![
            text("first"),
            text("second"),
            media,
            text("last"),
        ])]);
        let before = serde_json::to_value(&request).unwrap();
        let result = body(&request);
        assert_eq!(
            result["messages"][0]["content"],
            json!([
                expected_media,
                converted(text("first")),
                converted(text("second")),
                converted(text("last"))
            ])
        );
        assert_eq!(body(&request), result);
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
    }
}

#[test]
fn noops_preserve_order_for_missing_text_missing_media_and_multiple_media() {
    for parts in [
        vec![text("only")],
        vec![image()],
        vec![pdf()],
        vec![image(), text("already"), text("second")],
        vec![text("first"), image(), text("middle"), image()],
        vec![text("first"), pdf(), image(), text("last")],
    ] {
        let expected: Vec<_> = parts.iter().cloned().map(converted).collect();
        assert_eq!(
            body(&request(vec![message(parts)]))["messages"][0]["content"],
            json!(expected)
        );
    }
}

#[test]
fn normalization_is_per_original_message_not_per_merged_role() {
    let result = body(&request(vec![
        message(vec![text("one"), image()]),
        message(vec![text("two"), pdf()]),
    ]));
    assert_eq!(result["messages"].as_array().unwrap().len(), 1);
    assert_eq!(
        result["messages"][0]["content"],
        json!([
            converted(image()),
            converted(text("one")),
            converted(pdf()),
            converted(text("two"))
        ])
    );
    let result = body(&request(vec![
        message(vec![text("separate")]),
        message(vec![pdf()]),
    ]));
    assert_eq!(
        result["messages"][0]["content"],
        json!([converted(text("separate")), converted(pdf())])
    );
}

#[test]
fn nested_tool_results_and_tool_call_content_are_not_normalized() {
    let parts = vec![text("before"), pdf()];
    let expected = json!([converted(text("before")), converted(pdf())]);
    let mut tool = Message::tool_result("call-1", "unused");
    tool.content = Some(Content::Parts(parts.clone()));
    assert_eq!(
        body(&request(vec![tool]))["messages"][0]["content"][0]["content"],
        expected
    );
    let mut call = message(parts);
    call.role = Role::Assistant;
    call.tool_calls.push(ToolCall {
        id: "call-1".into(),
        name: "lookup".into(),
        arguments: json!({}),
    });
    let result = body(&request(vec![call]));
    assert_eq!(
        &result["messages"][0]["content"].as_array().unwrap()[..2],
        expected.as_array().unwrap()
    );
    assert_eq!(result["messages"][0]["content"][2]["type"], "tool_use");
}

#[test]
fn signed_and_redacted_history_keep_all_block_positions() {
    for history in [
        ContentPart::Thinking {
            text: "private".into(),
            signature: Some("opaque-signature+/=".into()),
        },
        ContentPart::RedactedThinking {
            data: "opaque-redacted+/=".into(),
        },
    ] {
        let expected_history = match &history {
            ContentPart::Thinking { text, signature } => {
                json!({"type":"thinking","thinking":text,"signature":signature})
            }
            ContentPart::RedactedThinking { data } => {
                json!({"type":"redacted_thinking","data":data})
            }
            _ => unreachable!(),
        };
        let expected = json!([converted(text("first")), expected_history, converted(pdf())]);
        let mut message = message(vec![text("first"), history, pdf()]);
        message.role = Role::Assistant;
        let request = request(vec![message]);
        let before = serde_json::to_value(&request).unwrap();
        assert_eq!(body(&request)["messages"][0]["content"], expected);
        assert_eq!(serde_json::to_value(&request).unwrap(), before);
    }
}

#[test]
fn cache_marks_last_text_after_ordering_and_trailing_user_normalization() {
    let request = request(vec![
        message(vec![text("first"), text("last"), pdf()]),
        Message::text(Role::Assistant, "prefill"),
    ])
    .with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
        enabled: true,
        system_ttl: None,
    });
    let result = body(&request);
    assert_eq!(result["messages"][0]["content"][0], converted(pdf()));
    assert_eq!(
        result["messages"][0]["content"][2],
        json!({"type":"text","text":"last","cache_control":{"type":"ephemeral"}})
    );
    assert_eq!(
        result["messages"][2],
        json!({"role":"user","content":[{"type":"text","text":".","cache_control":{"type":"ephemeral"}}]})
    );
    let request = request.with_anthropic_prompt_cache(AnthropicPromptCachePolicy {
        enabled: false,
        system_ttl: None,
    });
    assert!(body(&request)["messages"][0]["content"][2]
        .get("cache_control")
        .is_none());
}

#[test]
fn other_provider_order_is_unchanged() {
    let request = request(vec![message(vec![text("read"), pdf()])]);
    let openai = request_body(Provider::OpenAi, &request).unwrap();
    assert_eq!(openai["messages"][0]["content"][0]["type"], "text");
    assert_eq!(openai["messages"][0]["content"][1]["type"], "file");
    let gemini = request_body(Provider::Gemini, &request).unwrap();
    assert_eq!(gemini["contents"][0]["parts"][0], json!({"text":"read"}));
    assert_eq!(
        gemini["contents"][0]["parts"][1],
        json!({"inlineData":{"mimeType":"application/pdf","data":"JVBERi0xLjQ="}})
    );
}

#[test]
fn ordinary_assistant_media_is_normalized_with_or_without_prefill_support() {
    for (model, turns) in [("unknown-fixture", 2), ("claude-3-fixture", 1)] {
        let mut message = message(vec![text("read"), pdf()]);
        message.role = Role::Assistant;
        let request = CompletionRequest::new(model, Context::new(vec![message]));
        let result = body(&request);
        assert_eq!(result["messages"].as_array().unwrap().len(), turns);
        assert_eq!(
            result["messages"][0]["content"],
            json!([converted(pdf()), converted(text("read"))])
        );
    }
}
