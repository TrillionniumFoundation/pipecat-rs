//! Exact native Gemini Parts must stay in their originating model Content.
use pipecat_services::{adapters::request_body, protocol::ProtocolDecoder, sse::SseEvent, *};
use serde_json::{json, Value};
#[test]
fn signed_text_after_function_call_preserves_original_model_part_order() {
    let parts = json!([{"functionCall":{"id":"a","name":"echo","args":{}},"thoughtSignature":"tool-signature"},{"text":"after call","thoughtSignature":"text-signature"}]);
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    decoder
        .decode(SseEvent {
            event: None,
            data: json!({"candidates":[{"content":{"parts":parts},"finishReason":"STOP"}]})
                .to_string(),
        })
        .unwrap();
    let mut history = decoder
        .end_of_stream()
        .unwrap()
        .into_iter()
        .find_map(|event| {
            if let StreamEvent::GeminiHistory(history) = event {
                Some(history)
            } else {
                None
            }
        })
        .unwrap();
    history.push(Message::tool_result("a", "{}"));
    let body = request_body(
        Provider::Gemini,
        &CompletionRequest::new("gemini-3-pro", Context::new(history)),
    )
    .unwrap();
    assert_eq!(body["contents"][0]["parts"], parts);
    assert_eq!(body["contents"][1]["role"], "user");
}
#[test]
fn malformed_native_parts_do_not_silently_disappear() {
    for part in [Value::Null, json!(3), json!("text"), json!([]), json!({})] {
        let mut decoder = ProtocolDecoder::new(Provider::Gemini);
        assert!(decoder
            .decode(SseEvent {
                event: None,
                data: json!({"candidates":[{"content":{"parts":[part]},"finishReason":"STOP"}]})
                    .to_string()
            })
            .is_err());
    }
}
