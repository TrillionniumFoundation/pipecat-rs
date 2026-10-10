//! Adversarial context contracts ported from the original Python adapters.
use pipecat_services::{adapters::request_body, *};
use serde_json::json;

#[test]
fn gemini_preserves_tool_call_and_result_ids_for_parallel_same_name_calls() {
    let request = CompletionRequest::new(
        "synthetic",
        Context::new(vec![
            Message::assistant_tool_calls(vec![
                ToolCall {
                    id: "first".into(),
                    name: "weather".into(),
                    arguments: json!({"city":"Paris"}),
                },
                ToolCall {
                    id: "second".into(),
                    name: "weather".into(),
                    arguments: json!({"city":"Tokyo"}),
                },
            ]),
            Message::tool_result("second", "{\"temperature\":21}"),
            Message::tool_result("first", "{\"temperature\":12}"),
        ]),
    );
    let body = request_body(Provider::Gemini, &request).unwrap();
    assert_eq!(
        body["contents"][0]["parts"][0]["functionCall"]["id"],
        "first"
    );
    assert_eq!(
        body["contents"][0]["parts"][1]["functionCall"]["id"],
        "second"
    );
    assert_eq!(
        body["contents"][1]["parts"][0]["functionResponse"]["id"],
        "second"
    );
    assert_eq!(
        body["contents"][1]["parts"][1]["functionResponse"]["id"],
        "first"
    );
}

#[test]
fn single_system_message_does_not_become_empty_provider_history() {
    // BaseLLMAdapter._extract_initial_system preserves a sole message as user.
    let request = CompletionRequest::new(
        "synthetic",
        Context::new(vec![Message::text(Role::System, "Introduce yourself")]),
    );
    for (provider, field, parts) in [
        (Provider::Anthropic, "messages", "content"),
        (Provider::Gemini, "contents", "parts"),
    ] {
        let body = request_body(provider, &request).unwrap();
        assert_eq!(
            body[field].as_array().unwrap().len(),
            1,
            "{provider:?}: system-only input must still contain a conversational turn"
        );
        assert_eq!(body[field][0]["role"], "user");
        assert_eq!(body[field][0][parts][0]["text"], "Introduce yourself");
    }
}

#[test]
fn initial_system_text_parts_convert_to_provider_system_instruction() {
    let mut system = Message::text(Role::System, "");
    system.content = Some(Content::Parts(vec![
        ContentPart::Text {
            text: "First rule".into(),
        },
        ContentPart::Text {
            text: "Second rule".into(),
        },
    ]));
    let request = CompletionRequest::new(
        "synthetic",
        Context::new(vec![system, Message::text(Role::User, "Hello")]),
    );
    for provider in [Provider::Anthropic, Provider::Gemini] {
        let body =
            request_body(provider, &request).expect("Python joins initial system text parts");
        let system = if provider == Provider::Anthropic {
            &body["system"]
        } else {
            &body["systemInstruction"]["parts"][0]["text"]
        };
        assert_eq!(system, "First rule Second rule");
    }
}
