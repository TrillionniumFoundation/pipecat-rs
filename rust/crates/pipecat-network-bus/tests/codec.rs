use pipecat_core::{Direction, Frame, FrameData};
use pipecat_network_bus::*;
use pipecat_workers::{BusMessage, JobStatus, MessageData};
use serde_json::{json, Value};

fn message(name: &str, fields: Value) -> Value {
    let mut fields = fields.as_object().unwrap().clone();
    fields.insert("source".into(), "source".into());
    json!({"__type__":format!("pipecat.bus.messages.{name}"),"__data__":fields})
}

#[test]
fn original_python_serializer_fixtures_roundtrip_losslessly() {
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/python_messages.json")).unwrap();
    assert_eq!(fixtures.len(), 35);
    let codec = PythonJsonCodec::default();
    for fixture in fixtures {
        let decoded = codec
            .decode(&serde_json::to_vec(&fixture).unwrap())
            .unwrap_or_else(|error| panic!("{}: {error}", fixture["__type__"]));
        assert_eq!(decoded.source(), "worker-a");
        assert_eq!(decoded.target(), Some("worker-b"));
        assert!(decoded.id().is_some());
        let encoded: Value = serde_json::from_slice(&codec.encode(&decoded).unwrap()).unwrap();
        assert_eq!(fixture, encoded);
    }
}

#[test]
fn native_message_to_python_wire_uses_correct_fields_and_status_string() {
    let codec = PythonJsonCodec::default();
    let request = BusMessage {
        source: "parent".into(),
        target: Some("worker".into()),
        data: MessageData::JobRequest {
            job_id: "id".into(),
            name: Some("named".into()),
            payload: Some(json!({"key":"value"})),
        },
    };
    let encoded = codec.from_bus_message(&request).unwrap();
    assert_eq!(encoded.as_bus_message(), Some(&request));
    assert_eq!(encoded.fields()["job_name"], "named");
    assert!(encoded.fields().get("job_id").is_some());
    for status in [
        JobStatus::Completed,
        JobStatus::Cancelled,
        JobStatus::Failed,
        JobStatus::Error,
    ] {
        let native = BusMessage {
            source: "w".into(),
            target: None,
            data: MessageData::JobResponse {
                job_id: "id".into(),
                status,
                response: None,
                urgent: true,
            },
        };
        let encoded = codec.from_bus_message(&native).unwrap();
        assert!(
            encoded.fields()["status"].is_string(),
            "Python StrEnum is JSON-native before enum adapter"
        );
        assert!(!encoded.fields().contains_key("target"));
        assert!(!encoded.fields().contains_key("response"));
        assert_eq!(encoded.as_bus_message(), Some(&native));
        assert!(encoded.is_system());
    }
}

#[test]
fn exact_qualified_job_status_enum_is_supported_and_preserved() {
    let codec = PythonJsonCodec::default();
    let value = message(
        "BusJobResponseMessage",
        json!({"job_id":"id","status":{"__type__":JOB_STATUS_TYPE,"__data__":"COMPLETED"}}),
    );
    let decoded = codec.decode_value(value.clone()).unwrap();
    assert_eq!(decoded.wire_value(), &value);
    assert!(matches!(
        decoded.as_bus_message().unwrap().data,
        MessageData::JobResponse {
            status: JobStatus::Completed,
            ..
        }
    ));
    let mut wrong = value;
    wrong["__data__"]["status"]["__type__"] = "pipecat.workers.base_worker.JobStatus".into();
    assert!(matches!(
        codec.decode_value(wrong),
        Err(CodecError::UnsupportedType(_))
    ));
}

#[test]
fn registry_identity_ready_and_stream_source_only_fields_survive() {
    let codec = PythonJsonCodec::default();
    let registry = message(
        "BusWorkerRegistryMessage",
        json!({"id":42,"name":"BusWorkerRegistryMessage#7","runner":"r","workers":[{"__type__":REGISTRY_ENTRY_TYPE,"__data__":{"name":"worker","active":true,"bridged":true,"started_at":123.25}}]}),
    );
    let envelope = codec.decode_value(registry.clone()).unwrap();
    assert_eq!(envelope.id(), Some(42));
    assert_eq!(envelope.name(), Some("BusWorkerRegistryMessage#7"));
    match envelope.decoded() {
        DecodedMessage::Registry { workers, .. } => {
            assert_eq!(workers[0].started_at, Some(123.25));
            assert_eq!(workers[0].parent, None);
        }
        _ => panic!(),
    }
    assert!(envelope.is_system());
    assert_eq!(envelope.wire_value(), &registry);
    for name in ["BusJobStreamStartMessage", "BusJobStreamEndMessage"] {
        let value = message(
            name,
            json!({"job_id":"id","data":{"content_type":"audio/pcm"}}),
        );
        assert_eq!(
            codec.decode_value(value.clone()).unwrap().wire_value(),
            &value
        );
    }
}

#[test]
fn frame_identity_direction_and_audio_bytes_are_typed() {
    let codec = PythonJsonCodec::default();
    let mut frame = Frame::text("hello 世界");
    frame.id = 4242;
    frame.pts = Some(123);
    frame
        .metadata
        .insert("context".into(), json!({"nested":[1,true]}));
    let native = BusMessage {
        source: "worker".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(frame),
            direction: Direction::Upstream,
            bridge: Some("audio".into()),
        },
    };
    let envelope = codec.from_bus_message(&native).unwrap();
    assert_eq!(envelope.as_bus_message(), Some(&native));
    assert_eq!(envelope.fields()["direction"]["__type__"], DIRECTION_TYPE);
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/python_messages.json")).unwrap();
    let audio = fixtures
        .iter()
        .find(|value| {
            value["__data__"]["frame"]["__type__"] == "pipecat.frames.frames.TTSAudioRawFrame"
        })
        .unwrap();
    let envelope = codec.decode_value(audio.clone()).unwrap();
    match &envelope.as_bus_message().unwrap().data {
        MessageData::Frame { frame, .. } => match &frame.data {
            FrameData::TtsAudio(audio) => {
                assert_eq!(audio.audio, vec![1, 0, 255, 127]);
                assert_eq!(audio.context_id.as_deref(), Some("tts-1"));
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
}

#[test]
fn original_context_custom_tools_are_not_erased_on_forward() {
    let codec = PythonJsonCodec::default();
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/python_messages.json")).unwrap();
    let fixture = fixtures.last().unwrap();
    let envelope = codec.decode_value(fixture.clone()).unwrap();
    assert_eq!(
        envelope.fields()["frame"]["__data__"]["context"]["__data__"]["tools"]["custom_tools"]
            ["gemini"],
        json!([{"google_search":{}}])
    );
    match &envelope.as_bus_message().unwrap().data {
        MessageData::Frame { frame, .. } => match &frame.data {
            FrameData::LlmContextDetailed(frame) => {
                let pipecat_core::ToolSet::Schema(tools) = &frame.context.tools else {
                    panic!()
                };
                let pipecat_core::StandardTool::Function(tool) = &tools.standard_tools[0] else {
                    panic!()
                };
                assert_eq!(tool.name, "get_weather");
                assert_eq!(
                    frame.context.messages[0].content,
                    pipecat_core::MessageContent::Text("hi".into())
                );
            }
            _ => panic!(),
        },
        _ => panic!(),
    }
    assert_eq!(
        serde_json::from_slice::<Value>(&codec.encode(&envelope).unwrap()).unwrap(),
        *fixture
    );
}

#[test]
fn unknown_and_malformed_nested_types_fail_without_dynamic_imports() {
    let codec = PythonJsonCodec::default();
    for type_name in [
        "os.system",
        "builtins.eval",
        "pipecat.frames.frames.CustomFrame",
        "bytes.fake",
    ] {
        let value = message(
            "BusJobRequestMessage",
            json!({"job_id":"id","payload":{"nested":[{"__type__":type_name,"__data__":"whatever"}]}}),
        );
        assert!(matches!(
            codec.decode_value(value),
            Err(CodecError::UnsupportedType(_))
        ));
    }
    assert!(matches!(
        codec.decode_value(message("BusAddWorkerMessage", json!({}))),
        Err(CodecError::LocalWorkerReference)
    ));
    assert!(codec
        .decode_value(message(
            "BusJobRequestMessage",
            json!({"job_id":"id","payload":[]})
        ))
        .is_err());
    assert!(codec
        .decode_value(message(
            "BusJobResponseMessage",
            json!({"job_id":"id","status":"COMPLETED"})
        ))
        .is_err());
    assert!(codec
        .decode_value(message(
            "BusJobRequestMessage",
            json!({"job_id":"id","payload":{"a":{"__type__":"bytes","__data__":"!!!"}}})
        ))
        .is_err());
}

#[test]
fn defaults_priority_local_types_and_limits_match_contract() {
    let codec = PythonJsonCodec::default();
    let speak = codec
        .decode_value(message("BusTTSSpeakMessage", json!({"text":"hello"})))
        .unwrap();
    assert!(matches!(
        speak.as_bus_message().unwrap().data,
        MessageData::Speak {
            append_to_context: true,
            ..
        }
    ));
    let update_request = codec
        .decode_value(message(
            "BusJobUpdateRequestMessage",
            json!({"job_id":"id"}),
        ))
        .unwrap();
    assert!(!update_request.is_system());
    let local = codec
        .decode_value(message(
            "BusWorkerLocalErrorMessage",
            json!({"error":"failed"}),
        ))
        .unwrap();
    assert!(local.is_system() && local.is_local_only());
    let tiny = PythonJsonCodec {
        max_message_bytes: 3,
    };
    assert!(matches!(tiny.decode(b"{}  "), Err(CodecError::TooLarge(3))));
    assert!(matches!(tiny.encode(&speak), Err(CodecError::TooLarge(3))));
}

#[test]
fn projection_warnings_and_speculative_context_preservation() {
    let codec = PythonJsonCodec::default();
    let fixtures: Vec<Value> =
        serde_json::from_str(include_str!("fixtures/python_messages.json")).unwrap();
    let context = fixtures.last().unwrap().clone();
    assert_eq!(
        codec
            .decode_value(context.clone())
            .unwrap()
            .projection_warnings(),
        Vec::<&str>::new()
    );
    let mut speculative = context;
    speculative["__data__"]["frame"]["__data__"]["speculation"] = true.into();
    let speculative = codec.decode_value(speculative).unwrap();
    let MessageData::Frame { frame, .. } = &speculative.as_bus_message().unwrap().data else {
        panic!()
    };
    assert!(matches!(&frame.data, FrameData::LlmContextDetailed(value) if value.speculation));
    let ready = message(
        "BusWorkerReadyMessage",
        json!({"runner":"r","started_at":0}),
    );
    assert_eq!(
        codec.decode_value(ready).unwrap().projection_warnings(),
        vec!["ready.started_at"]
    );
    let mut nested = Value::Null;
    for _ in 0..70 {
        nested = json!([nested]);
    }
    assert!(matches!(
        codec.decode_value(message(
            "BusJobRequestMessage",
            json!({"job_id":"id","payload":{"nested":nested}})
        )),
        Err(CodecError::InvalidField(_))
    ));
}
