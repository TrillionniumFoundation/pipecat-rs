//! Native source contracts for Gemini Part signatures and migration history.
use pipecat_services::{
    adapters::{request_body, request_body_for_logging},
    protocol::{ProtocolDecoder, MAX_SIGNATURE_BYTES},
    sse::{SseDecoder, SseEvent},
    *,
};
use serde_json::{json, Value};
fn decode(decoder: &mut ProtocolDecoder, value: Value) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn signed_call(id: &str, signature: Option<&str>) -> Message {
    let mut message = Message::assistant_tool_calls(vec![ToolCall {
        id: id.into(),
        name: "echo".into(),
        arguments: json!({"id":id}),
    }]);
    message.content = Some(Content::Parts(vec![ContentPart::GeminiToolMetadata {
        tool_call_id: id.into(),
        signature: signature.map(str::to_owned),
    }]));
    message
}
fn body(messages: Vec<Message>) -> Result<Value, ServiceError> {
    request_body(
        Provider::Gemini,
        &CompletionRequest::new("gemini-3-pro", Context::new(messages)),
    )
}
#[test]
fn exact_signed_parts_roundtrip_with_repeated_text_and_empty_signature_tail() {
    let parts = json!([{"text":"same","thoughtSignature":"first+/opaque=="},{"text":"same"},{"text":"","thoughtSignature":"tail+/opaque=="},{"functionCall":{"id":"one","name":"echo","args":{"id":"one"}},"thoughtSignature":"tool+/opaque=="},{"functionCall":{"id":"two","name":"echo","args":{"id":"two"}}}]);
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    let events = decode(
        &mut decoder,
        json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]}),
    )
    .unwrap();
    assert!(events
        .iter()
        .any(|e| matches!(e, StreamEvent::GeminiCheckpoint(_))));
    assert!(!events
        .iter()
        .any(|e| matches!(e, StreamEvent::GeminiHistory(_))));
    let history = decoder
        .end_of_stream()
        .unwrap()
        .into_iter()
        .find_map(|e| {
            if let StreamEvent::GeminiHistory(h) = e {
                Some(h)
            } else {
                None
            }
        })
        .unwrap();
    let request = CompletionRequest::new("gemini-3-pro", Context::new(history));
    assert_eq!(
        request_body(Provider::Gemini, &request).unwrap()["contents"][0]["parts"],
        parts
    );
    let restored: CompletionRequest =
        serde_json::from_value(serde_json::to_value(&request).unwrap()).unwrap();
    assert_eq!(
        request_body(Provider::Gemini, &restored).unwrap(),
        request_body(Provider::Gemini, &request).unwrap()
    );
    for text in [
        format!("{request:?}"),
        request_body_for_logging(Provider::Gemini, &request)
            .unwrap()
            .to_string(),
    ] {
        assert!(!text.contains("opaque"));
    }
    for provider in [Provider::Anthropic, Provider::OpenAi] {
        assert!(request_body(provider, &request).is_err());
    }
}
#[test]
fn parallel_interleaving_regroups_by_identity_and_sequential_signatures_stay_separate() {
    let wire = body(vec![
        signed_call("a", Some("sig-a")),
        Message::tool_result("a", "{}"),
        Message::text(Role::Assistant, "between"),
        signed_call("b", None),
        Message::tool_result("b", "{}"),
        signed_call("c", Some("sig-c")),
        Message::tool_result("c", "{}"),
    ])
    .unwrap();
    let contents = wire["contents"].as_array().unwrap();
    assert_eq!(contents[0]["parts"].as_array().unwrap().len(), 2);
    assert_eq!(contents[0]["parts"][0]["thoughtSignature"], "sig-a");
    assert!(contents[0]["parts"][1].get("thoughtSignature").is_none());
    assert_eq!(contents[1]["parts"][0]["functionResponse"]["id"], "a");
    assert_eq!(contents[1]["parts"][1]["functionResponse"]["id"], "b");
    assert_eq!(contents[2]["parts"][0]["text"], "between");
    assert_eq!(contents[2]["parts"][1]["functionCall"]["id"], "c");
    assert_eq!(contents[2]["parts"][1]["thoughtSignature"], "sig-c");
    assert_eq!(contents[3]["parts"][0]["functionResponse"]["id"], "c");
}
#[test]
fn parallel_results_reorder_to_calls_and_missing_duplicate_or_unrelated_results_fail() {
    let wire = body(vec![
        signed_call("a", Some("sig")),
        signed_call("b", None),
        Message::tool_result("b", "{}"),
        Message::tool_result("a", "{}"),
    ])
    .unwrap();
    assert_eq!(
        wire["contents"][1]["parts"][0]["functionResponse"]["id"],
        "a"
    );
    for results in [
        vec![Message::tool_result("a", "{}")],
        vec![
            Message::tool_result("a", "{}"),
            Message::tool_result("a", "{}"),
        ],
        vec![
            Message::tool_result("a", "{}"),
            Message::tool_result("unknown", "{}"),
        ],
    ] {
        let mut messages = vec![signed_call("a", Some("sig")), signed_call("b", None)];
        messages.extend(results);
        assert!(body(messages).is_err());
    }
}
#[test]
fn migration_sentinel_only_applies_to_entirely_foreign_unsigned_history() {
    let foreign = Message::assistant_tool_calls(vec![ToolCall {
        id: "foreign".into(),
        name: "echo".into(),
        arguments: json!({}),
    }]);
    let wire = body(vec![foreign, Message::tool_result("foreign", "{}")]).unwrap();
    assert_eq!(
        wire["contents"][0]["parts"][0]["thoughtSignature"],
        "skip_thought_signature_validator"
    );
    assert!(body(vec![
        signed_call("native", None),
        Message::tool_result("native", "{}")
    ])
    .is_err());
    let request = CompletionRequest::new(
        "gemini-2.5-pro",
        Context::new(vec![signed_call("native", None)]),
    );
    assert!(
        request_body(Provider::Gemini, &request).unwrap()["contents"][0]["parts"][0]
            .get("thoughtSignature")
            .is_none()
    );
}
#[test]
fn incomplete_signed_text_and_unowned_bookmarks_fail_closed_for_all_providers() {
    for part in [
        ContentPart::GeminiText {
            text: "heard".into(),
            thought: false,
            signature: None,
            incomplete: true,
        },
        ContentPart::GeminiToolMetadata {
            tool_call_id: "missing".into(),
            signature: Some("secret".into()),
        },
    ] {
        let mut message = Message::text(Role::Assistant, "");
        message.content = Some(Content::Parts(vec![part]));
        let request = CompletionRequest::new("gemini-3-pro", Context::new(vec![message]));
        for provider in [Provider::Gemini, Provider::Anthropic, Provider::OpenAi] {
            assert!(request_body(provider, &request).is_err());
        }
    }
}
#[test]
fn malformed_signatures_shapes_ids_and_oversized_parts_poison_decoder() {
    for signature in [
        Value::Null,
        json!(42),
        json!({"opaque":"secret"}),
        json!(""),
        json!("x".repeat(MAX_SIGNATURE_BYTES + 1)),
    ] {
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        let error=decode(&mut decoder,json!({"candidates":[{"content":{"parts":[{"text":"x","thoughtSignature":signature}]}}]})).unwrap_err();
        assert!(!format!("{error}").contains("secret"));
        assert!(decoder.end_of_stream().is_err());
    }
    for part in [
        json!({"text":3}),
        json!({"text":"x","functionCall":{"name":"f"}}),
        json!({"text":"x","thought":"true"}),
        json!({"functionCall":{"id":"bad id","name":"f","args":{}}}),
        json!({"thoughtSignature":"sig"}),
    ] {
        assert!(decode(
            &mut ProtocolDecoder::new(Provider::Gemini),
            json!({"candidates":[{"content":{"parts":[part]}}]})
        )
        .is_err());
    }
    assert!(decode(&mut ProtocolDecoder::new(Provider::Gemini),json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"same","name":"f","args":{}}},{"functionCall":{"id":"same","name":"f","args":{}}}]}}]})).is_err());
}
#[test]
fn an_incomplete_sse_signature_is_never_promoted_to_a_checkpoint() {
    let mut sse = SseDecoder::new(4096);
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    assert!(sse.feed(b"data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"hello\",\"thoughtSignature\":\"partial").unwrap().is_empty());
    assert!(sse.has_pending_data());
    assert!(decoder.end_of_stream().is_err());
}
#[test]
fn native_metadata_accounts_for_argument_bytes_depth_and_duplicate_ownership() {
    let mut duplicate = signed_call("a", Some("sig"));
    if let Some(Content::Parts(parts)) = &mut duplicate.content {
        parts.push(parts[0].clone());
    }
    assert!(body(vec![duplicate]).is_err());
    let mut oversized = signed_call("a", Some("sig"));
    oversized.tool_calls[0].arguments =
        json!({"large":"x".repeat(pipecat_services::protocol::MAX_HISTORY_BYTES)});
    assert!(body(vec![oversized]).is_err());
    let mut deep = signed_call("a", Some("sig"));
    let mut value = json!({});
    for _ in 0..130 {
        value = json!({"nested":value});
    }
    deep.tool_calls[0].arguments = value;
    assert!(body(vec![deep]).is_err());
    let mut text = Message::text(Role::Assistant, "");
    text.content = Some(Content::Parts(vec![ContentPart::GeminiText {
        text: "prefix".into(),
        thought: false,
        signature: Some("text-sig".into()),
        incomplete: false,
    }]));
    assert!(body(vec![
        text,
        signed_call("a", None),
        Message::tool_result("a", "{}")
    ])
    .is_err());
    assert!(body(vec![
        signed_call("a", None),
        signed_call("b", Some("second-sig")),
        Message::tool_result("a", "{}"),
        Message::tool_result("b", "{}")
    ])
    .is_err());
}
#[test]
fn tool_history_requires_stop_even_when_every_part_is_structurally_complete() {
    for reason in [
        "MAX_TOKENS",
        "SAFETY",
        "MALFORMED_FUNCTION_CALL",
        "OTHER_NEW_FINISH",
    ] {
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        decode(&mut decoder,json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"a","name":"echo","args":{}},"thoughtSignature":"opaque"}]},"finishReason":reason}]})).unwrap();
        assert!(decoder.end_of_stream().is_err());
        assert!(decoder.end_of_stream().is_err());
        assert!(decode(
            &mut decoder,
            json!({"candidates":[{"finishReason":"STOP"}]})
        )
        .is_err());
    }
}
