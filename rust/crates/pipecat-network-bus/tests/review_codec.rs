use pipecat_core::{FrameData, FramePriority};
use pipecat_network_bus::{PythonJsonCodec, DIRECTION_TYPE};
use pipecat_workers::MessageData;
use serde_json::{json, Value};

fn frame(name: &str, fields: Value) -> Value {
    json!({"__type__":"pipecat.bus.messages.BusFrameMessage", "__data__":{"source":"review", "direction":{"__type__":DIRECTION_TYPE,"__data__":"DOWNSTREAM"}, "frame":{"__type__":format!("pipecat.frames.frames.{name}"),"__data__":fields}}})
}
#[test]
fn review_bus_wrapper_remains_data_while_frame_retains_python_priority() {
    for (name, fields, priority, interruptible) in [
        ("StartFrame", json!({}), FramePriority::Start, true),
        ("CancelFrame", json!({}), FramePriority::System, true),
        ("InterruptionFrame", json!({}), FramePriority::System, true),
        (
            "InputTextRawFrame",
            json!({"text":"input"}),
            FramePriority::System,
            true,
        ),
        (
            "InputAudioRawFrame",
            json!({"audio":{"__type__":"bytes","__data__":"AAA="},"sample_rate":16000,"num_channels":1}),
            FramePriority::System,
            true,
        ),
        (
            "ProposedUserStartedSpeakingFrame",
            json!({}),
            FramePriority::System,
            true,
        ),
        (
            "ProposedUserStoppedSpeakingFrame",
            json!({}),
            FramePriority::Normal,
            true,
        ),
        ("EndFrame", json!({}), FramePriority::Normal, false),
        ("StopFrame", json!({}), FramePriority::Normal, false),
        (
            "LLMThoughtStartFrame",
            json!({}),
            FramePriority::Normal,
            true,
        ),
        ("LLMThoughtEndFrame", json!({}), FramePriority::Normal, true),
        (
            "HeartbeatFrame",
            json!({"timestamp":1}),
            FramePriority::Normal,
            true,
        ),
        (
            "OutputTransportReadyFrame",
            json!({}),
            FramePriority::Normal,
            true,
        ),
        (
            "InputTransportStartAudioStreamingFrame",
            json!({}),
            FramePriority::Normal,
            true,
        ),
        (
            "AudioBufferStartRecordingFrame",
            json!({}),
            FramePriority::Normal,
            false,
        ),
        (
            "AudioBufferStopRecordingFrame",
            json!({}),
            FramePriority::Normal,
            false,
        ),
    ] {
        let envelope = PythonJsonCodec::default()
            .decode_value(frame(name, fields))
            .unwrap();
        assert!(
            !envelope.is_system(),
            "BusFrameMessage priority is independent of its frame"
        );
        let MessageData::Frame { frame, .. } = &envelope.as_bus_message().unwrap().data else {
            panic!()
        };
        assert_eq!(frame.priority(), priority, "{name}");
        assert_eq!(frame.interruptible, interruptible, "{name}");
    }
}
#[test]
fn review_omitted_text_spacing_uses_python_class_default() {
    for (name, expected) in [
        ("TextFrame", false),
        ("InputTextRawFrame", false),
        ("LLMTextFrame", true),
        ("VisionTextFrame", true),
        ("LLMThoughtTextFrame", true),
    ] {
        let envelope = PythonJsonCodec::default()
            .decode_value(frame(name, json!({"text":"hello"})))
            .unwrap();
        let MessageData::Frame { frame, .. } = &envelope.as_bus_message().unwrap().data else {
            panic!()
        };
        let (FrameData::Text(text)
        | FrameData::InputText(text)
        | FrameData::LlmText(text)
        | FrameData::VisionText(text)
        | FrameData::LlmThoughtText(text)) = &frame.data
        else {
            panic!()
        };
        assert_eq!(text.includes_inter_frame_spaces, expected, "{name}");
    }
}

#[test]
fn review_mutated_python_fixture_fields_never_panic_or_invalidate_envelope_accessors() {
    let codec = PythonJsonCodec::default();
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/python_messages.json")).unwrap();
    let substitutions = [
        Value::Null,
        json!(true),
        json!(-1),
        json!(u64::MAX),
        json!([]),
        json!({}),
        json!("invalid"),
        json!({"__type__":"os.system","__data__":"invalid"}),
    ];
    for fixture in fixtures {
        for key in fixture["__data__"].as_object().unwrap().keys() {
            for replacement in &substitutions {
                let mut changed = fixture.clone();
                changed["__data__"][key] = replacement.clone();
                if let Ok(envelope) = codec.decode_value(changed) {
                    let _ = (
                        envelope.source(),
                        envelope.target(),
                        envelope.id(),
                        envelope.name(),
                        envelope.is_system(),
                        envelope.projection_warnings(),
                    );
                    let decoded_again = codec.decode(&codec.encode(&envelope).unwrap()).unwrap();
                    assert_eq!(decoded_again.wire_value(), envelope.wire_value());
                }
            }
        }
    }
}

#[test]
fn review_python_integer_payload_is_explicitly_rejected_instead_of_rounded() {
    let codec = PythonJsonCodec::default();
    for number in [
        "18446744073709551617",
        "-9223372036854775809",
        "1234567890123456789012345678901234567890",
    ] {
        let raw = format!(
            r#"{{"__type__":"pipecat.bus.messages.BusJobRequestMessage","__data__":{{"source":"review","job_id":"number","payload":{{"number":{number}}}}}}}"#
        );
        assert!(matches!(
            codec.decode(raw.as_bytes()),
            Err(pipecat_network_bus::CodecError::UnsupportedInteger)
        ));
    }
    for number in [
        "18446744073709551615",
        "-9223372036854775808",
        "42",
        "-0",
        "1.234e20",
    ] {
        let raw = format!(
            r#"{{"__type__":"pipecat.bus.messages.BusJobRequestMessage","__data__":{{"source":"review","job_id":"number","payload":{{"number":{number}}}}}}}"#
        );
        codec.decode(raw.as_bytes()).unwrap();
    }
    let raw = br#"{"__type__":"pipecat.bus.messages.BusJobRequestMessage","__data__":{"source":"review","job_id":"quoted","payload":{"text":"escaped \" 18446744073709551617"}}}"#;
    codec.decode(raw).unwrap();
}

#[test]
fn review_serde_private_marker_keys_remain_ordinary_payload_objects() {
    let codec = PythonJsonCodec::default();
    for key in [
        "$serde_json::private::Number",
        "$serde_json::private::RawValue",
    ] {
        let object = json!({key: "42"});
        let native = pipecat_workers::BusMessage {
            source: "review".into(),
            target: None,
            data: MessageData::JobRequest {
                job_id: "private-key".into(),
                name: None,
                payload: Some(json!({"nested":[object.clone()]})),
            },
        };
        let encoded = codec
            .encode(&codec.from_bus_message(&native).unwrap())
            .unwrap();
        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded.fields()["payload"]["nested"][0], object);
        assert_eq!(decoded.as_bus_message(), Some(&native));

        let wire = frame(
            "InputTransportMessageFrame",
            json!({"message":object,"metadata":{"marker":object}}),
        );
        let direct = codec.decode_value(wire.clone()).unwrap();
        let decoded = codec.decode(&serde_json::to_vec(&wire).unwrap()).unwrap();
        assert_eq!(decoded.wire_value(), &wire);
        for envelope in [direct, decoded] {
            let MessageData::Frame { frame, .. } = &envelope.as_bus_message().unwrap().data else {
                panic!()
            };
            assert_eq!(frame.data, FrameData::InputTransportMessage(object.clone()));
            assert_eq!(frame.metadata["marker"], object);
        }
    }
}

#[test]
fn review_python_nonfinite_json_extensions_are_explicitly_rejected() {
    let codec = PythonJsonCodec::default();
    for number in ["NaN", "Infinity", "-Infinity", "1e1000", "-1e1000"] {
        let raw = format!("{{\"__type__\":\"pipecat.bus.messages.BusJobRequestMessage\",\"__data__\":{{\"source\":\"review\",\"job_id\":\"number\",\"payload\":{{\"number\":{number}}}}}}}");
        assert!(
            matches!(
                codec.decode(raw.as_bytes()),
                Err(pipecat_network_bus::CodecError::Json(_))
            ),
            "{number}"
        );
    }
}

#[test]
fn review_response_tts_policy_survives_typed_network_projection() {
    // Source frames.py defines skip_tts on both response boundary frames.
    let codec = PythonJsonCodec::default();
    for name in ["LLMFullResponseStartFrame", "LLMFullResponseEndFrame"] {
        for skip in [true, false] {
            let envelope = codec
                .decode_value(frame(name, json!({"skip_tts": skip})))
                .unwrap();
            let message = envelope.as_bus_message().unwrap();
            let MessageData::Frame { frame: native, .. } = &message.data else {
                panic!()
            };
            match &native.data {
                FrameData::LlmFullResponseStart(value) | FrameData::LlmFullResponseEnd(value) => {
                    assert_eq!(value.skip_tts, Some(skip))
                }
                value => panic!("lost TTS policy: {value:?}"),
            }
            let encoded = codec.from_bus_message(message).unwrap();
            assert_eq!(
                encoded.wire_value()["__data__"]["frame"]["__data__"]["skip_tts"],
                skip
            );
        }
        assert!(codec
            .decode_value(frame(name, json!({"skip_tts": "false"})))
            .is_err());
    }
}

#[test]
fn review_json_terminal_reasons_survive_typed_network_projection() {
    // Python EndFrame/CancelFrame.reason is Any, not only a string.
    let codec = PythonJsonCodec::default();
    for name in ["EndFrame", "CancelFrame"] {
        for reason in [
            json!({"status": "finished", "count": 2}),
            json!([1, "two"]),
            json!(42),
            json!(false),
        ] {
            let envelope = codec
                .decode_value(frame(name, json!({"reason": reason})))
                .unwrap();
            let message = envelope.as_bus_message().unwrap();
            let MessageData::Frame { frame: native, .. } = &message.data else {
                panic!()
            };
            match &native.data {
                FrameData::EndDetailed(value) | FrameData::CancelDetailed(value) => {
                    assert_eq!(value.reason.as_ref(), Some(&reason))
                }
                value => panic!("lost structured reason: {value:?}"),
            }
            assert_eq!(native.interruptible, name == "CancelFrame");
            let encoded = codec.from_bus_message(message).unwrap();
            assert_eq!(
                encoded.wire_value()["__data__"]["frame"]["__data__"]["reason"],
                reason
            );
        }
    }
}

#[test]
fn review_unsupported_frame_errors_do_not_disclose_private_payloads() {
    let message = pipecat_workers::BusMessage {
        source: "source".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(pipecat_core::Frame::new(FrameData::Metrics(vec![
                pipecat_core::Metric::TtsUsage {
                    processor: "PRIVATE_CONVERSATION_SENTINEL".into(),
                    characters: 1,
                },
            ]))),
            direction: pipecat_core::Direction::Downstream,
            bridge: None,
        },
    };
    let error = PythonJsonCodec::default()
        .from_bus_message(&message)
        .unwrap_err();
    assert!(error.to_string().contains("MetricsFrame"));
    assert!(!error.to_string().contains("PRIVATE_CONVERSATION_SENTINEL"));
    assert!(!format!("{error:?}").contains("PRIVATE_CONVERSATION_SENTINEL"));
}
