//! Reconstructed deterministic contracts, with pinned source provenance in fixture.
use pipecat_services::{
    adapters::request_body, protocol::ProtocolDecoder, sse::SseEvent, CompletionRequest, Context,
    Provider, StreamEvent,
};
use serde_json::{json, Value};
fn decode(
    decoder: &mut ProtocolDecoder,
    value: Value,
) -> Result<Vec<StreamEvent>, pipecat_services::ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn thought(decoder: &mut ProtocolDecoder, index: u32) {
    decode(
        decoder,
        json!({"type":"content_block_start","index":index,"content_block":{"type":"thinking"}}),
    )
    .unwrap();
}
fn signature(
    decoder: &mut ProtocolDecoder,
    index: u32,
    signature: Value,
) -> Result<Vec<StreamEvent>, pipecat_services::ServiceError> {
    decode(
        decoder,
        json!({"type":"content_block_delta","index":index,"delta":{"type":"signature_delta","signature":signature}}),
    )
}
#[test]
fn fixture_retains_exact_opaque_signatures_and_provider_block_order() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/anthropic_signed_history.json")).unwrap();
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    let events: Vec<_> = fixture["events"]
        .as_array()
        .unwrap()
        .iter()
        .flat_map(|event| decode(&mut decoder, event.clone()).unwrap())
        .collect();
    let history = events
        .iter()
        .find_map(|event| {
            if let StreamEvent::AnthropicHistory(history) = event {
                Some(history.clone())
            } else {
                None
            }
        })
        .unwrap();
    let body = request_body(
        Provider::Anthropic,
        &CompletionRequest::new("fixture", Context::new(history)),
    )
    .unwrap();
    assert_eq!(body["messages"][0]["content"], fixture["expected_content"]);
    assert!(!format!("{events:?}").contains("opaque+/"));
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, StreamEvent::AnthropicThoughtStart))
            .count(),
        2
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| matches!(e, StreamEvent::AnthropicThoughtEnd))
            .count(),
        2
    );
}
#[test]
fn malformed_start_and_stop_poison_decoder_without_history_resurrection() {
    for invalid_start in [true, false] {
        let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
        if invalid_start {
            assert!(decode(&mut decoder,json!({"type":"content_block_start","index":0,"content_block":{"type":"thinking","signature":{}}})).is_err());
        } else {
            thought(&mut decoder, 0);
            assert!(decode(&mut decoder, json!({"type":"content_block_stop","index":0})).is_err());
        }
        assert!(decode(&mut decoder,json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"later"}})).is_err());
        assert!(decode(&mut decoder, json!({"type":"message_stop"})).is_err());
        assert!(decoder.end_of_stream().is_err());
    }
}
#[test]
fn completed_prefix_precedes_real_end_and_survives_later_partial_signature() {
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    thought(&mut decoder, 0);
    signature(&mut decoder, 0, json!("exact-first")).unwrap();
    let events = decode(&mut decoder, json!({"type":"content_block_stop","index":0})).unwrap();
    assert!(matches!(
        events.as_slice(),
        [
            StreamEvent::AnthropicCheckpoint(_),
            StreamEvent::AnthropicThoughtEnd
        ]
    ));
    thought(&mut decoder, 1);
    signature(&mut decoder, 1, json!("incomplete")).unwrap();
    assert!(decoder.end_of_stream().is_err());
}
#[test]
fn malformed_oversize_and_wrong_order_signatures_fail_closed() {
    for value in [Value::Null, json!({}), json!("x".repeat(1024 * 1024 + 1))] {
        let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
        thought(&mut decoder, 0);
        assert!(signature(&mut decoder, 0, value).is_err());
        assert!(decode(&mut decoder, json!({"type":"message_stop"})).is_err());
    }
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    thought(&mut decoder, 0);
    signature(&mut decoder, 0, json!("s")).unwrap();
    assert!(decode(&mut decoder,json!({"type":"content_block_delta","index":0,"delta":{"type":"thinking_delta","thinking":"late"}})).is_err());
}
#[test]
fn delayed_prefix_does_not_fabricate_thought_end() {
    let mut decoder = ProtocolDecoder::new(Provider::Anthropic);
    decode(&mut decoder,json!({"type":"content_block_start","index":0,"content_block":{"type":"text","text":"lead"}})).unwrap();
    thought(&mut decoder, 1);
    signature(&mut decoder, 1, json!("s1")).unwrap();
    assert_eq!(
        decode(&mut decoder, json!({"type":"content_block_stop","index":1})).unwrap(),
        vec![StreamEvent::AnthropicThoughtEnd]
    );
    thought(&mut decoder, 2);
    assert!(matches!(
        decode(&mut decoder, json!({"type":"content_block_stop","index":0}))
            .unwrap()
            .as_slice(),
        [StreamEvent::AnthropicCheckpoint(_)]
    ));
}

#[test]
fn context_exact_byte_bound_and_signature_redaction() {
    use pipecat_services::{Content, ContentPart, Message, Role};
    let signature = "s".repeat(1024 * 1024);
    let mut message = Message::text(Role::Assistant, "");
    message.content = Some(Content::Parts(vec![ContentPart::Thinking {
        text: "t".repeat(7 * 1024 * 1024),
        signature: Some(signature),
    }]));
    let mut context = Context::new(vec![message]);
    assert!(context.validate().is_ok());
    let Some(Content::Parts(parts)) = &mut context.messages[0].content else {
        panic!("parts")
    };
    let ContentPart::Thinking { text, .. } = &mut parts[0] else {
        panic!("thought")
    };
    text.push('x');
    assert!(context.validate().is_err());
    let part = ContentPart::Thinking {
        text: "private thought".into(),
        signature: Some("exact-secret-signature".into()),
    };
    assert!(!format!("{part:?}").contains("exact-secret-signature"));
}
#[test]
fn pinned_sonnet_five_default_is_disabled_without_changing_sonnet_four() {
    for model in ["claude-sonnet-5", "claude-sonnet-5-1-20260101"] {
        let request = CompletionRequest::new(model, Context::default());
        assert_eq!(
            request_body(Provider::Anthropic, &request).unwrap()["thinking"],
            json!({"type":"disabled"})
        );
    }
    let request = CompletionRequest::new("claude-sonnet-4-5", Context::default());
    assert!(request_body(Provider::Anthropic, &request)
        .unwrap()
        .get("thinking")
        .is_none());
}
