use pipecat_core::*;
use serde_json::{json, Value};
use std::sync::Arc;

fn function(handler: LocalToolFunction) -> DirectFunction {
    DirectFunction {
        schema: ToolDefinition {
            name: "echo".into(),
            description: String::new(),
            parameters: json!({"type": "object"}),
        },
        handler,
    }
}

fn snapshot(handler: LocalToolFunction) -> Frame {
    Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![],
            tools: ToolSet::Direct(vec![function(handler)]),
            tool_choice: None,
        },
        speculation: false,
    }))
}

fn tools(frame: &mut Frame) -> &mut Vec<DirectFunction> {
    let FrameData::LlmContextDetailed(context) = &mut frame.data else {
        panic!("expected detailed context")
    };
    let ToolSet::Direct(tools) = &mut context.context.tools else {
        panic!("expected direct tools")
    };
    tools
}

#[tokio::test]
async fn cloned_handlers_keep_identity_call_behavior_and_serialization_boundary() {
    let original =
        LocalToolFunction::new(
            |value| async move { Ok(json!({"echo": value, "source": "original"})) },
        );
    let clone = original.clone();
    assert_eq!(original, clone);
    let mut frame = snapshot(original);
    NativeRealtimeContextProducer::default()
        .attach(&mut frame, RealtimeContextIntent::Run, false)
        .unwrap();
    let mut retained = frame.clone();
    drop(frame);
    tools(&mut retained)[0].handler = clone.clone();
    assert!(retained.native_realtime_context_cause().unwrap().is_some());
    assert_eq!(
        clone.call(json!({"value": 7})).await.unwrap(),
        json!({"echo": {"value": 7}, "source": "original"})
    );
    assert!(serde_json::to_value(&clone).is_err());
    for value in [Value::Null, json!(17), json!({"identity": 17})] {
        assert!(serde_json::from_value::<LocalToolFunction>(value).is_err());
    }
    assert!(serde_json::to_value(&retained).is_err());
    let cause = retained
        .take_native_realtime_context_cause()
        .unwrap()
        .unwrap();
    assert_eq!(cause.retained_tool_identity_count(), 1);
    assert!(serde_json::to_value(&retained).is_err());
    tools(&mut retained).clear();
    let decoded: Frame = serde_json::from_value(serde_json::to_value(retained).unwrap()).unwrap();
    assert!(decoded.native_realtime_context_cause().unwrap().is_none());
}

#[test]
fn same_schema_replacement_after_original_callback_drop_invalidates_seal() {
    let resource = Arc::new(vec![0_u8; 1024]);
    let weak = Arc::downgrade(&resource);
    let handler = LocalToolFunction::new(move |value| {
        let held = resource.clone();
        async move {
            let _held = held;
            Ok(value)
        }
    });
    let mut frame = snapshot(handler);
    NativeRealtimeContextProducer::default()
        .attach(&mut frame, RealtimeContextIntent::Run, false)
        .unwrap();
    let cause = frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .clone();
    tools(&mut frame).clear();
    assert!(
        weak.upgrade().is_none(),
        "cause retained the callback's capture"
    );
    tools(&mut frame).push(function(LocalToolFunction::new(|_| async {
        Ok(json!({"replacement": true}))
    })));
    assert!(frame.native_realtime_context_cause().is_err());
    assert_eq!(cause.retained_tool_identity_count(), 1);
}
