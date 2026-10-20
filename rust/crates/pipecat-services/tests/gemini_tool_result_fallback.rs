//! Pinned conversion contracts, not live-provider acceptance of orphan histories.
use pipecat_services::{adapters::request_body, *};
use serde_json::{json, Value};

fn request(messages: Vec<Message>) -> CompletionRequest {
    CompletionRequest::new("gemini-3.8-flash", Context::new(messages))
}
fn wire(request: &CompletionRequest) -> Value {
    let before = serde_json::to_value(request).unwrap();
    request.context.validate().unwrap();
    let body = request_body(Provider::Gemini, request).unwrap();
    assert_eq!(request_body(Provider::Gemini, request).unwrap(), body);
    assert_eq!(serde_json::to_value(request).unwrap(), before);
    body
}
fn response(id: &str, name: &str, value: Value) -> Value {
    json!({"functionResponse":{"id":id,"name":name,"response":value}})
}
fn call(id: &str, name: &str) -> Message {
    Message::assistant_tool_calls(vec![ToolCall {
        id: id.into(),
        name: name.into(),
        arguments: json!({}),
    }])
}
#[test]
fn orphan_result_preserves_content_id_and_history_without_mutation() {
    for (text, expected) in [
        ("plain text", json!({"value":"plain text"})),
        ("", json!({"value":""})),
        ("null", json!({"value":null})),
        ("42", json!({"value":42})),
        ("true", json!({"value":true})),
        (r#""string""#, json!({"value":"string"})),
        ("[1,false]", json!({"value":[1,false]})),
        (r#"{"ok":true}"#, json!({"ok":true})),
    ] {
        let body = wire(&request(vec![Message::tool_result("old-call", text)]));
        assert_eq!(
            body["contents"],
            json!([{"role":"user","parts":[response("old-call", "tool_call_result", expected)]}])
        );
        assert!(body.get("tools").is_none());
    }
}
#[test]
fn matching_and_explicit_names_keep_existing_precedence_and_history_map() {
    for (matched, explicit, expected) in [
        (false, None, "tool_call_result"),
        (true, None, "retained"),
        (false, Some("explicit"), "explicit"),
        (true, Some("explicit"), "explicit"),
        (true, Some(""), ""),
    ] {
        let mut result = Message::tool_result("old-call", "{}");
        result.name = explicit.map(str::to_owned);
        let messages = if matched {
            vec![call("old-call", "retained"), result]
        } else {
            vec![result]
        };
        let body = wire(&request(messages));
        let index = usize::from(matched);
        assert_eq!(
            body["contents"][index]["parts"],
            json!([response("old-call", expected, json!({}))])
        );
    }
    // Existing map covers the whole context, even when the call follows its result.
    let body = wire(&request(vec![
        Message::tool_result("old-call", "{}"),
        call("old-call", "last"),
    ]));
    assert_eq!(
        body["contents"][0]["parts"],
        json!([response("old-call", "last", json!({}))])
    );
    assert_eq!(
        body["contents"][2],
        json!({"role":"user","parts":[{"text":"."}]})
    );
}
#[test]
fn unrelated_calls_do_not_supply_names_and_adjacent_results_still_merge() {
    let body = wire(&request(vec![
        call("other", "unrelated"),
        Message::tool_result("old-call", "{}"),
        Message::tool_result("second", "null"),
    ]));
    assert_eq!(
        body["contents"],
        json!([
            {"role":"model","parts":[{"functionCall":{"id":"other","name":"unrelated","args":{}},"thoughtSignature":"skip_thought_signature_validator"}]},
            {"role":"user","parts":[response("old-call", "tool_call_result", json!({})), response("second", "tool_call_result", json!({"value":null}))]}
        ])
    );
}
#[test]
fn orphan_results_do_not_suppress_system_seed() {
    let mut request = request(vec![
        Message::text(Role::System, "context system"),
        Message::tool_result("old-call", "{}"),
    ]);
    request.system_instruction = Some("settings system".into());
    let body = wire(&request);
    assert_eq!(
        body["contents"],
        json!([
            {"role":"user","parts":[response("old-call", "tool_call_result", json!({}))]},
            {"role":"user","parts":[{"text":"context system"}]}
        ])
    );
    assert_eq!(
        body["systemInstruction"],
        json!({"parts":[{"text":"settings system"}]})
    );
}
#[test]
fn missing_ids_and_nontext_results_remain_rejected() {
    for id in [None, Some(String::new())] {
        let mut result = Message::tool_result("old-call", "{}");
        result.tool_call_id = id;
        let request = request(vec![result]);
        assert!(matches!(
            request.context.validate(),
            Err(ServiceError::InvalidRequest(_))
        ));
        assert!(matches!(
            request_body(Provider::Gemini, &request),
            Err(ServiceError::InvalidRequest(_))
        ));
    }
    for content in [
        None,
        Some(Content::Parts(vec![])),
        Some(Content::Parts(vec![ContentPart::Text {
            text: "result".into(),
        }])),
    ] {
        let mut result = Message::tool_result("old-call", "{}");
        result.content = content;
        assert!(request_body(Provider::Gemini, &request(vec![result])).is_err());
    }
}
#[test]
fn fallback_does_not_change_other_provider_results() {
    let request = request(vec![Message::tool_result("old-call", "{}")]);
    assert_eq!(
        request_body(Provider::OpenAi, &request).unwrap()["messages"],
        json!([{"role":"tool","content":"{}","tool_call_id":"old-call"}])
    );
    assert_eq!(
        request_body(Provider::Anthropic, &request).unwrap()["messages"],
        json!([{"role":"user","content":[{"type":"tool_result","tool_use_id":"old-call","content":[{"type":"text","text":"{}"}]}]}])
    );
}
