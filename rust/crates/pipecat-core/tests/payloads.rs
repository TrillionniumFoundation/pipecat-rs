use pipecat_core::*;
use serde_json::json;
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[test]
fn dtmf_sequence_respects_precedence_and_rejects_invalid_or_empty_input() {
    let sequence = DtmfSequence::new(
        Some(KeypadEntry::One),
        Some(vec![KeypadEntry::Pound, KeypadEntry::Zero]),
    )
    .unwrap();
    assert_eq!(sequence.to_string(), "#0");
    assert_eq!(
        DtmfSequence::new(Some(KeypadEntry::One), None)
            .unwrap()
            .to_string(),
        "1"
    );
    assert_eq!(
        "0123456789*#".parse::<DtmfSequence>().unwrap().to_string(),
        "0123456789*#"
    );
    assert!("".parse::<DtmfSequence>().is_err());
    assert!("12x".parse::<DtmfSequence>().is_err());
    assert!("A".parse::<DtmfSequence>().is_err());
    assert!(DtmfSequence::new(Some(KeypadEntry::One), Some(vec![])).is_err());
    let ordinary = Frame::new(FrameData::OutputDtmfSequence("12#".parse().unwrap()));
    let urgent = Frame::new(FrameData::OutputDtmfUrgentSequence("12#".parse().unwrap()));
    assert_eq!(ordinary.priority(), FramePriority::Normal);
    assert_eq!(urgent.priority(), FramePriority::System);
}

#[test]
fn settings_keep_omission_null_and_value_distinct_and_are_uninterruptible() {
    let settings = LlmSettings {
        temperature: Setting::Null,
        max_tokens: Setting::Value(100),
        ..LlmSettings::default()
    };
    let frame = Frame::new(FrameData::LlmUpdateSettings(Box::new(
        ServiceUpdateSettingsFrame {
            delta: Some(settings),
            service: Some(ProcessorReference::Name("llm".into())),
            reach_inactive_services: true,
            ..ServiceUpdateSettingsFrame::default()
        },
    )));
    assert!(!frame.interruptible);
    let value = serde_json::to_value(&frame).unwrap();
    let restored: Frame = serde_json::from_value(value).unwrap();
    assert_eq!(restored, frame);
    let FrameData::LlmUpdateSettings(settings) = restored.data else {
        panic!()
    };
    let settings = settings.delta.unwrap();
    assert_eq!(settings.temperature, Setting::Null);
    assert_eq!(settings.max_tokens, Setting::Value(100));
    assert_eq!(settings.top_p, Setting::NotGiven);
}

#[tokio::test]
async fn callbacks_execute_locally_and_never_serialize_as_empty_placeholders() {
    let count = Arc::new(AtomicUsize::new(0));
    let count2 = count.clone();
    let callback = LocalAsyncCallback::new(move || {
        let count = count2.clone();
        async move {
            count.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    callback.call().await.unwrap();
    assert_eq!(count.load(Ordering::SeqCst), 1);
    let result = DetailedFunctionCallResultFrame {
        function_name: "test".into(),
        tool_call_id: "1".into(),
        arguments: json!({}),
        result: json!(42),
        run_llm: None,
        properties: Some(FunctionCallResultProperties {
            on_context_updated: Some(callback),
            ..FunctionCallResultProperties::default()
        }),
        error: None,
    };
    let frame = Frame::new(FrameData::FunctionCallResultDetailed(result));
    assert!(!frame.interruptible);
    assert!(serde_json::to_value(&frame)
        .unwrap_err()
        .to_string()
        .contains("cannot be serialized"));
}

#[test]
fn executable_message_transform_preserves_rich_context_and_errors() {
    let transform = LocalMessageTransform::new(|messages| {
        Ok(messages
            .into_iter()
            .filter(|message| message.role != ChatRole::System)
            .collect())
    });
    let messages = vec![
        ChatMessage::new(ChatRole::System, "policy").into(),
        ContextMessage {
            role: ChatRole::User,
            content: MessageContent::Parts(vec![MessagePart::Image {
                url: "https://example.com/image.png".into(),
                detail: Some("high".into()),
            }]),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
            extra: BTreeMap::new(),
        },
    ];
    let transformed = transform.apply(messages).unwrap();
    assert_eq!(transformed.len(), 1);
    assert!(matches!(transformed[0].content, MessageContent::Parts(_)));
    assert!(
        serde_json::to_value(Frame::new(FrameData::LlmMessagesTransform(
            LlmMessagesTransformFrame {
                transform,
                run_llm: None
            }
        )))
        .is_err()
    );
    let failure = LocalMessageTransform::new(|_| Err(ProcessorError::new("invalid transform")));
    assert!(failure.apply(vec![]).is_err());
}

#[test]
fn sprite_and_image_properties_survive_roundtrip() {
    let image = OutputImageRawFrame {
        image: ImageRawFrame {
            image: vec![255, 0, 0],
            size: (1, 1),
            format: Some("RGB".into()),
        },
        sync_with_audio: true,
    };
    let frame = Frame::new(FrameData::Sprite(SpriteFrame {
        images: vec![image.clone(), image],
    }));
    let restored: Frame = serde_json::from_str(&serde_json::to_string(&frame).unwrap()).unwrap();
    assert_eq!(frame, restored);
}

#[test]
fn source_uninterruptible_control_defaults_are_retained() {
    for data in [
        FrameData::AudioBufferStartRecording,
        FrameData::AudioBufferStopRecording,
        FrameData::Volume(0.5),
        FrameData::VolumeGain(2.0),
        FrameData::EndWorker(ReasonFrame::default()),
        FrameData::StopWorker,
    ] {
        assert!(!Frame::new(data).interruptible);
    }
    let fatal = Frame::new(FrameData::FatalError(ErrorFrame::new("fatal")));
    assert!(fatal.data.as_error_frame().unwrap().fatal);
}

#[tokio::test]
async fn direct_tool_callbacks_return_values_and_custom_tool_schemas_roundtrip() {
    let handler = LocalToolFunction::new(|arguments| async move {
        Ok(json!({"value": arguments["value"].as_i64().unwrap() + 1}))
    });
    assert_eq!(
        handler.call(json!({"value": 3})).await.unwrap(),
        json!({"value": 4})
    );
    assert!(serde_json::to_value(&handler).is_err());
    let schema = ToolsSchema {
        standard_tools: vec![StandardTool::Function(ToolDefinition {
            name: "test".into(),
            description: "test".into(),
            parameters: json!({"type":"object"}),
        })],
        custom_tools: Some(BTreeMap::from([(
            AdapterType::Openai,
            vec![BTreeMap::from([("type".into(), json!("web_search"))])],
        )])),
    };
    let frame = Frame::new(FrameData::LlmSetTools(ToolSet::Schema(schema)));
    let restored: Frame = serde_json::from_value(serde_json::to_value(&frame).unwrap()).unwrap();
    assert_eq!(frame, restored);
}

#[tokio::test]
async fn local_flush_event_supports_late_waiters_clear_and_explicit_serialization_failure() {
    let event = LocalFlushEvent::default();
    assert!(!event.is_set());
    event.set();
    event.wait().await;
    event.clear();
    assert!(!event.is_set());
    assert!(serde_json::to_value(event).is_err());
}
