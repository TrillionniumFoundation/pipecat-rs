//! Independent JSON payload compatibility checks against the source serializer.
use pipecat_core::{Direction, Frame, FrameData};
use pipecat_network_bus::PythonJsonCodec;
use pipecat_workers::{BusMessage, MessageData};
use serde_json::json;

#[test]
fn partial_type_tag_keys_are_regular_transport_payload_data() {
    // Python _deserialize_value recognizes typed wrappers only when BOTH keys
    // exist. A user dictionary containing one key must survive unchanged.
    let codec = PythonJsonCodec::default();
    for payload in [
        json!({"__type__":"application_record"}),
        json!({"__data__":[1,2,3]}),
    ] {
        let message = BusMessage {
            source: "review".into(),
            target: None,
            data: MessageData::Frame {
                frame: Box::new(Frame::new(FrameData::OutputTransportMessage(
                    payload.clone(),
                ))),
                direction: Direction::Downstream,
                bridge: None,
            },
        };
        let envelope = codec
            .from_bus_message(&message)
            .expect("an untagged user dictionary must not be rejected as a type wrapper");
        let MessageData::Frame { frame, .. } = &envelope.as_bus_message().unwrap().data else {
            panic!()
        };
        assert_eq!(frame.data, FrameData::OutputTransportMessage(payload));
    }
}

#[test]
fn nested_partial_tags_survive_metadata_and_tool_result_projection() {
    let codec = PythonJsonCodec::default();
    for payload in [
        json!({"__type__":null}),
        json!({"__type__":{"name":"custom"}}),
        json!({"__data__":null}),
        json!({"__data__":{"one":1}}),
    ] {
        let mut frame = Frame::new(FrameData::FunctionCallResult(
            pipecat_core::FunctionCallResultFrame {
                function_name: "lookup".into(),
                tool_call_id: "one".into(),
                arguments: json!({}),
                result: json!({"nested":[payload.clone()]}),
                run_llm: Some(false),
                is_final: true,
                error: None,
            },
        ));
        frame
            .metadata
            .insert("nested".into(), json!([payload.clone()]));
        let message = BusMessage {
            source: "review".into(),
            target: None,
            data: MessageData::Frame {
                frame: Box::new(frame),
                direction: Direction::Downstream,
                bridge: None,
            },
        };
        let envelope = codec.from_bus_message(&message).unwrap();
        let MessageData::Frame { frame, .. } = &envelope.as_bus_message().unwrap().data else {
            panic!()
        };
        assert_eq!(frame.metadata["nested"], json!([payload.clone()]));
        let FrameData::FunctionCallResultDetailed(result) = &frame.data else {
            panic!()
        };
        assert_eq!(result.result, json!({"nested":[payload]}));
    }
}

#[test]
fn both_key_unknown_type_wrappers_remain_rejected() {
    let message = BusMessage {
        source: "review".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(Frame::new(FrameData::OutputTransportMessage(
                json!({"nested":{"__type__":"untrusted.Type","__data__":{}}}),
            ))),
            direction: Direction::Downstream,
            bridge: None,
        },
    };
    assert!(
        matches!(PythonJsonCodec::default().from_bus_message(&message),
        Err(pipecat_network_bus::CodecError::UnsupportedType(name)) if name=="untrusted.Type")
    );
}
