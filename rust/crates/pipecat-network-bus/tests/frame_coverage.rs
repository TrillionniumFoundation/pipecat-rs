use pipecat_core::*;
use pipecat_network_bus::{CodecError, PythonJsonCodec, DIRECTION_TYPE};
use pipecat_workers::{BusMessage, MessageData};
use serde_json::{json, Value};
use std::collections::BTreeSet;

fn wire(name: &str, fields: Value) -> Value {
    json!({"__type__":"pipecat.bus.messages.BusFrameMessage","__data__":{"source":"source","direction":{"__type__":DIRECTION_TYPE,"__data__":"DOWNSTREAM"},"frame":{"__type__":format!("pipecat.frames.frames.{name}"),"__data__":fields}}})
}
fn native(envelope: &pipecat_network_bus::MessageEnvelope) -> &Frame {
    let MessageData::Frame { frame, .. } = &envelope.as_bus_message().unwrap().data else {
        panic!()
    };
    frame
}
fn message(data: FrameData) -> BusMessage {
    BusMessage {
        source: "source".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(Frame::new(data)),
            direction: Direction::Downstream,
            bridge: None,
        },
    }
}
fn fixtures() -> Vec<Value> {
    serde_json::from_str(include_str!("fixtures/python_frames.json")).unwrap()
}

#[test]
fn all_source_frame_fixtures_have_lossless_wire_replay_and_typed_payloads() {
    let codec = PythonJsonCodec::default();
    for fixture in fixtures() {
        let class = fixture["__data__"]["frame"]["__type__"].as_str().unwrap();
        let mut envelope = codec
            .decode(&serde_json::to_vec(&fixture).unwrap())
            .unwrap_or_else(|e| panic!("{class}: {e}"));
        assert_eq!(
            native(&envelope).data.source_class_name(),
            class.rsplit('.').next().unwrap()
        );
        for _ in 0..3 {
            envelope = codec.decode(&codec.encode(&envelope).unwrap()).unwrap();
            assert_eq!(envelope.wire_value(), &fixture, "{class}");
        }
        assert_eq!(native(&envelope).pts, Some(9001));
        assert_eq!(native(&envelope).broadcast_sibling_id, Some(9002));
        let rebuilt = codec
            .from_bus_message(envelope.as_bus_message().unwrap())
            .unwrap_or_else(|e| panic!("{class}: {e}"));
        // New message identity is expected; every populated source payload field
        // must survive the native projection, including all nested structures.
        for (key, value) in fixture["__data__"]["frame"]["__data__"]
            .as_object()
            .unwrap()
        {
            assert_eq!(
                &rebuilt.wire_value()["__data__"]["frame"]["__data__"][key],
                value,
                "{class}.{key}"
            );
        }
    }
}

#[test]
fn coverage_accounts_for_every_core_source_class() {
    let supported: BTreeSet<_> = fixtures()
        .into_iter()
        .map(|v| {
            v["__data__"]["frame"]["__type__"]
                .as_str()
                .unwrap()
                .rsplit('.')
                .next()
                .unwrap()
                .to_owned()
        })
        .collect();
    assert_eq!(supported.len(), 125);
    let unsupported: BTreeSet<_> = [
        "SpriteFrame",
        "MetricsFrame",
        "LLMMessagesTransformFrame",
        "FrameProcessorPauseFrame",
        "FrameProcessorResumeFrame",
        "FrameProcessorPauseUrgentFrame",
        "FrameProcessorResumeUrgentFrame",
        "ServiceSwitcherRequestMetadataFrame",
        "ManuallySwitchServiceFrame",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    let source: BTreeSet<_> = FRAME_CONTRACTS
        .iter()
        .map(|v| v.source_class.to_owned())
        .collect();
    assert_eq!(source.len(), 134);
    assert_eq!(source, supported.union(&unsupported).cloned().collect());
    for class in unsupported {
        assert!(
            matches!(
                PythonJsonCodec::default().decode_value(wire(&class, json!({}))),
                Err(CodecError::UnsupportedType(_))
            ),
            "{class}"
        );
    }
}

#[test]
fn source_image_dimensions_are_required_and_encoded_as_lists() {
    let codec = PythonJsonCodec::default();
    let image = ImageRawFrame {
        image: vec![0, 1, 2, 255],
        size: (2, 2),
        format: Some("RGBA".into()),
    };
    let envelope = codec
        .from_bus_message(&message(FrameData::OutputImageDetailed(
            OutputImageRawFrame {
                image,
                sync_with_audio: true,
            },
        )))
        .unwrap();
    assert_eq!(
        envelope.wire_value()["__data__"]["frame"]["__data__"]["size"],
        json!([2, 2])
    );
    assert!(
        matches!(&native(&envelope).data,FrameData::OutputImageDetailed(v) if v.sync_with_audio)
    );
    for size in [
        Value::Null,
        json!([]),
        json!([1]),
        json!([1, 2, 3]),
        json!([-1, 2]),
        json!([1, "2"]),
    ] {
        assert!(codec
            .decode_value(wire(
                "InputImageRawFrame",
                json!({"image":{"__type__":"bytes","__data__":"AA=="},"size":size})
            ))
            .is_err());
    }
    assert!(codec
        .decode_value(wire(
            "InputImageRawFrame",
            json!({"image":{"__type__":"bytes","__data__":"AA=="}})
        ))
        .is_err());
}

#[test]
fn local_fields_are_rejected_instead_of_erased() {
    let codec = PythonJsonCodec::default();
    for (class, fields) in [
        (
            "StartFrame",
            json!({"tracing_context":{"conversation_id":"id"}}),
        ),
        ("ErrorFrame", json!({"error":"failure","processor":"name"})),
        ("ErrorFrame", json!({"error":"failure","exception":{}})),
        ("PipelineFlushFrame", json!({"event":{}})),
        (
            "UserImageRequestFrame",
            json!({"user_id":"user","result_callback":"callback"}),
        ),
        (
            "ServiceMetadataFrame",
            json!({"service_name":"test","user_turn_strategies":{}}),
        ),
        ("LLMUpdateSettingsFrame", json!({"service":"llm"})),
        (
            "FunctionCallResultFrame",
            json!({"function_name":"tool","tool_call_id":"id","arguments":{},"result":{},"properties":{"__type__":"pipecat.frames.frames.FunctionCallResultProperties","__data__":{"on_context_updated":"callback"}}}),
        ),
    ] {
        assert!(codec.decode_value(wire(class, fields)).is_err(), "{class}");
    }
    let callback = LocalAsyncCallback::new(|| async { Ok(()) });
    assert!(codec
        .from_bus_message(&message(FrameData::FunctionCallResultDetailed(
            DetailedFunctionCallResultFrame {
                function_name: "tool".into(),
                tool_call_id: "id".into(),
                arguments: json!({}),
                result: json!({}),
                run_llm: None,
                properties: Some(FunctionCallResultProperties {
                    on_context_updated: Some(callback),
                    ..Default::default()
                }),
                error: None
            }
        )))
        .is_err());
    assert!(codec
        .from_bus_message(&message(FrameData::StartDetailed(DetailedStartFrame {
            configuration: StartFrame::default(),
            enable_tracing: true,
            tracing_context: Some(TracingContext::default())
        })))
        .is_err());
}

#[test]
fn detailed_context_preserves_speculation_custom_tools_and_every_supported_part() {
    let codec = PythonJsonCodec::default();
    let messages = json!([
        {"role":"user","content":[{"type":"text","text":"hi"},{"type":"image_url","image_url":{"url":"image","detail":"high"}},{"type":"input_audio","input_audio":{"data":"AA==","format":"wav"}},{"type":"file","file":{"file_data":"data:application/pdf;base64,AA==","filename":"test.pdf"}}],"extension":{"nested":[1,2]}},
        {"role":"assistant","content":[{"type":"thinking","thinking":"reasoning","signature":"signed"},{"type":"refusal","refusal":"cannot"}],"tool_calls":[{"id":"call-1","type":"function","function":{"name":"lookup","arguments":"{\"key\":\"value\"}"}}]}
    ]);
    let context = json!({"__type__":"pipecat.processors.aggregators.llm_context.LLMContext","__data__":{"messages":messages,"tools":{"standard_tools":[],"custom_tools":{"gemini":[{"google_search":{}}],"openai":[{"type":"web_search"}]}},"tool_choice":"required"}});
    let envelope = codec
        .decode_value(wire(
            "LLMContextFrame",
            json!({"context":context,"speculation":true}),
        ))
        .unwrap();
    let FrameData::LlmContextDetailed(v) = &native(&envelope).data else {
        panic!()
    };
    assert!(v.speculation);
    assert_eq!(
        v.context.messages[1].tool_calls[0].arguments,
        json!({"key":"value"})
    );
    let rebuilt = codec
        .from_bus_message(envelope.as_bus_message().unwrap())
        .unwrap();
    assert_eq!(
        rebuilt.wire_value()["__data__"]["frame"]["__data__"]["context"],
        context
    );
    assert!(envelope.projection_warnings().is_empty());
    for bad in [
        json!({"role":"assistant","content":null}),
        json!({"__specific__":true,"llm":"provider","message":{}}),
        json!({"role":"user","content":[{"type":"future_part","payload":42}]}),
        json!({"role":"user","content":[{"type":"text","text":"x","unknown":1}]}),
    ] {
        assert!(codec
            .decode_value(wire("LLMMessagesAppendFrame", json!({"messages":[bad]})))
            .is_err());
    }
}

#[test]
fn context_tools_and_sparse_settings_keep_distinct_omitted_null_and_value_states() {
    let codec = PythonJsonCodec::default();
    let delta = json!({"__type__":"pipecat.services.settings.LLMSettings","__data__":{"model":null,"temperature":0.5,"extra":{"nested":{"clear":null}}}});
    let envelope = codec
        .decode_value(wire(
            "LLMUpdateSettingsFrame",
            json!({"delta":delta,"reach_inactive_services":true}),
        ))
        .unwrap();
    let FrameData::LlmUpdateSettings(v) = &native(&envelope).data else {
        panic!()
    };
    let v = v.delta.as_ref().unwrap();
    assert_eq!(v.base.model, Setting::Null);
    assert_eq!(v.temperature, Setting::Value(0.5));
    assert_eq!(v.max_tokens, Setting::NotGiven);
    let rebuilt = codec
        .from_bus_message(envelope.as_bus_message().unwrap())
        .unwrap();
    assert_eq!(
        rebuilt.wire_value()["__data__"]["frame"]["__data__"]["delta"],
        delta
    );
    for (tools, expected) in [(None, true), (Some(json!({"standard_tools":[]})), false)] {
        let mut context = json!({"__type__":"pipecat.processors.aggregators.llm_context.LLMContext","__data__":{"messages":[]}});
        if let Some(tools) = tools {
            context["__data__"]["tools"] = tools;
        }
        let e = codec
            .decode_value(wire("LLMContextFrame", json!({"context":context})))
            .unwrap();
        let FrameData::LlmContextDetailed(v) = &native(&e).data else {
            panic!()
        };
        assert_eq!(matches!(v.context.tools, ToolSet::NotGiven), expected);
    }
}

#[test]
fn malformed_fields_permutations_unknown_fields_and_size_boundaries_remain_safe() {
    let codec = PythonJsonCodec::default();
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
    for fixture in fixtures() {
        let mut extended = fixture.clone();
        extended["__data__"]["frame"]["__data__"]["future_field"] =
            json!({"bytes":[0,255],"nested":{"ok":true}});
        let admitted = codec.decode_value(extended.clone()).unwrap();
        assert_eq!(
            codec
                .decode(&codec.encode(&admitted).unwrap())
                .unwrap()
                .wire_value(),
            &extended
        );
        for key in fixture["__data__"]["frame"]["__data__"]
            .as_object()
            .unwrap()
            .keys()
        {
            for replacement in &substitutions {
                let mut changed = fixture.clone();
                changed["__data__"]["frame"]["__data__"][key] = replacement.clone();
                if let Ok(envelope) = codec.decode_value(changed) {
                    let replay = codec.decode(&codec.encode(&envelope).unwrap()).unwrap();
                    assert_eq!(replay.wire_value(), envelope.wire_value());
                }
            }
        }
        let bytes = codec.encode(&admitted).unwrap();
        let exact = PythonJsonCodec {
            max_message_bytes: bytes.len(),
        };
        assert!(exact.decode(&bytes).is_ok());
        assert!(matches!(
            PythonJsonCodec {
                max_message_bytes: bytes.len() - 1
            }
            .decode(&bytes),
            Err(CodecError::TooLarge(_))
        ));
        // Reorder top-level and frame fields manually, independent of Map's
        // sorted representation, and retain exactly the same typed result.
        let f = extended["__data__"]["frame"]["__data__"]
            .as_object()
            .unwrap();
        let reversed = format!(
            "{{{}}}",
            f.iter()
                .rev()
                .map(|(k, v)| format!("{}:{}", serde_json::to_string(k).unwrap(), v))
                .collect::<Vec<_>>()
                .join(",")
        );
        let raw=format!("{{\"__data__\":{{\"source\":\"source\",\"direction\":{{\"__type__\":\"{}\",\"__data__\":\"DOWNSTREAM\"}},\"frame\":{{\"__data__\":{},\"__type__\":{}}}}},\"__type__\":\"pipecat.bus.messages.BusFrameMessage\"}}",DIRECTION_TYPE,reversed,extended["__data__"]["frame"]["__type__"]);
        let reordered = codec.decode(raw.as_bytes()).unwrap();
        assert_eq!(native(&reordered), native(&admitted));
    }
}

#[test]
fn dtmf_sequences_defaults_and_float_precision_are_explicit() {
    let codec = PythonJsonCodec::default();
    for class in ["OutputDTMFFrame", "OutputDTMFUrgentFrame"] {
        let envelope = codec
            .decode_value(wire(class, json!({"button":"1","buttons":["2","*","#"]})))
            .unwrap();
        let sequence = match &native(&envelope).data {
            FrameData::OutputDtmfSequence(v) | FrameData::OutputDtmfUrgentSequence(v) => v,
            _ => panic!(),
        };
        assert_eq!(sequence.to_string(), "2*#");
        assert_eq!(sequence.button, Some(KeypadEntry::One));
        for fields in [
            json!({}),
            json!({"buttons":[]}),
            json!({"buttons":["A"]}),
            json!({"button":"X"}),
        ] {
            assert!(codec.decode_value(wire(class, fields)).is_err());
        }
    }
    for data in [
        FrameData::Volume(f32::NAN),
        FrameData::UserIdleTimeoutUpdate {
            timeout: f64::INFINITY,
        },
        FrameData::LlmSummarizeContext(Some(LlmContextSummaryConfig {
            summarization_timeout: f64::NAN,
            ..Default::default()
        })),
        FrameData::LlmUpdateSettings(Box::new(ServiceUpdateSettingsFrame {
            delta: Some(LlmSettings {
                temperature: Setting::Value(f64::NAN),
                ..Default::default()
            }),
            ..Default::default()
        })),
    ] {
        assert!(codec.from_bus_message(&message(data)).is_err());
    }
    assert!(
        codec
            .decode_value(wire("VolumeFrame", json!({"volume":0.1})))
            .is_err(),
        "unrepresentable f64 must not silently round to f32"
    );
    assert!(codec
        .from_bus_message(&message(FrameData::Volume(0.1)))
        .is_ok());
    let e = codec
        .decode_value(wire(
            "VADUserStartedSpeakingFrame",
            json!({"timestamp":1720000000.25}),
        ))
        .unwrap();
    assert!(
        matches!(&native(&e).data,FrameData::VadUserStartedSpeakingDetailed(v) if v.start_secs==0.0)
    );
}

#[test]
fn source_priority_and_omitted_interruptibility_defaults_hold_for_every_class() {
    let codec = PythonJsonCodec::default();
    for mut fixture in fixtures() {
        let class = fixture["__data__"]["frame"]["__type__"]
            .as_str()
            .unwrap()
            .rsplit('.')
            .next()
            .unwrap()
            .to_owned();
        let interruptible = fixture["__data__"]["frame"]["__data__"]
            .as_object_mut()
            .unwrap()
            .remove("interruptible")
            .unwrap();
        let envelope = codec.decode_value(fixture).unwrap();
        assert_eq!(
            json!(native(&envelope).interruptible),
            interruptible,
            "{class}"
        );
        let contract = FRAME_CONTRACTS
            .iter()
            .find(|c| c.source_class == class)
            .unwrap();
        assert_eq!(native(&envelope).priority(), contract.priority, "{class}");
    }
}
