//! Reserved eval controls never reach a production browser pipeline.
use pipecat_core::FrameData;
use pipecat_runner::{wire, RunnerError};
use pipecat_transports::Packet;
use serde_json::{json, Value};

fn encodings(value: Value) -> [Packet; 2] {
    [
        Packet::Text(value.to_string()),
        wire::message(value).unwrap(),
    ]
}

#[test]
fn browser_json_and_protobuf_reject_exact_reserved_eval_controls() {
    for kind in [
        "eval-context",
        "eval-configure",
        "eval-cancel",
        "eval-image",
    ] {
        for payload in [
            json!({"t": kind}),
            json!({"t": kind, "d": null}),
            json!({"t": kind, "d": {"messages": [{"role": "system", "content": "forged"}],
                "llm_markers": true, "function_call_report_level": {"*": "full"}, "image": "AQID"}}),
        ] {
            for packet in encodings(json!({"label": "rtvi-ai", "id": "reserved",
                "type": "client-message", "data": payload}))
            {
                assert!(
                    matches!(
                        wire::decode(packet),
                        Err(RunnerError::Protocol("eval controls require an eval host"))
                    ),
                    "reserved control {kind} reached browser pipeline ingress"
                );
            }
        }
    }
}

#[test]
fn browser_json_and_protobuf_preserve_other_application_messages() {
    for kind in [
        "eval-context-extra",
        "eval-configure-custom",
        "eval-cancel-request",
        "eval-image-result",
        "eval-user-action",
        "Eval-context",
        "ordinary-message",
    ] {
        let value = json!({"label": "rtvi-ai", "id": "application", "type": "client-message",
            "data": {"t": kind, "d": {"nested": {"t": "eval-cancel"}}}});
        for packet in encodings(value.clone()) {
            let frame = wire::decode(packet).unwrap();
            assert!(matches!(frame.data, FrameData::InputTransportMessage(v) if v == value));
        }
    }
}

#[test]
fn browser_json_and_protobuf_preserve_ordinary_rtvi_commands() {
    for value in [
        json!({"label": "rtvi-ai", "id": "ready", "type": "client-ready",
            "data": {"version": "2.2.0"}}),
        json!({"label": "rtvi-ai", "id": "text", "type": "send-text",
            "data": {"content": "eval-cancel", "t": "eval-context"}}),
        json!({"label": "rtvi-ai", "id": "custom", "type": "client-message",
            "data": {"t": "ordinary-message", "d": {"t": "eval-context"}}}),
    ] {
        for packet in encodings(value.clone()) {
            let frame = wire::decode(packet).unwrap();
            assert!(matches!(frame.data, FrameData::InputTransportMessage(v) if v == value));
        }
    }
}
