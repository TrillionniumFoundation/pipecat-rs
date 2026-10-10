use pipecat_services::{protocol::ProtocolDecoder, sse::SseEvent, *};
use serde_json::{json, Value};
fn decode(decoder: &mut ProtocolDecoder, value: Value) -> Result<Vec<StreamEvent>, ServiceError> {
    decoder.decode(SseEvent {
        event: None,
        data: value.to_string(),
    })
}
fn declared() -> ProtocolDecoder {
    let mut decoder = ProtocolDecoder::new(Provider::Gemini);
    decode(&mut decoder,json!({"candidates":[{"content":{"parts":[{"functionCall":{"id":"a","name":"echo","args":{}},"thoughtSignature":"opaque"}]}}]})).unwrap();
    decoder
}
#[test]
fn a_later_stop_cannot_replace_an_unsuccessful_finish() {
    for reason in [
        "SAFETY",
        "MAX_TOKENS",
        "MALFORMED_FUNCTION_CALL",
        "UNKNOWN",
        "STOP",
    ] {
        let mut decoder = declared();
        decode(
            &mut decoder,
            json!({"candidates":[{"finishReason":reason}]}),
        )
        .unwrap();
        assert!(decode(
            &mut decoder,
            json!({"candidates":[{"finishReason":"STOP"}]})
        )
        .is_err());
        assert!(decoder.end_of_stream().is_err());
    }
}
#[test]
fn foreign_protocol_stop_aliases_cannot_authorize_gemini_tools() {
    for reason in ["stop", "end_turn", "stop_sequence"] {
        let mut decoder = declared();
        decode(
            &mut decoder,
            json!({"candidates":[{"finishReason":reason}]}),
        )
        .unwrap();
        assert!(decoder.end_of_stream().is_err());
    }
}
