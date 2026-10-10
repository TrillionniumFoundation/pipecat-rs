//! Include as a child module of frame_payloads.rs. No production test hooks.
use super::*;

#[test]
fn review_realtime_cause_keeps_only_tiny_handler_identity_until_last_cause_drops() {
    let capture = Arc::new(());
    let weak_capture = Arc::downgrade(&capture);
    let handler = LocalToolFunction::new(move |value| {
        let capture = capture.clone();
        async move {
            drop(capture);
            Ok(value)
        }
    });
    let weak_identity = Arc::downgrade(&handler.1 .0);
    let handler_clone = handler.clone();
    assert!(Arc::ptr_eq(&handler.1 .0, &handler_clone.1 .0));
    drop(handler_clone);
    let mut frame = crate::Frame::new(crate::FrameData::LlmContextDetailed(
        DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![],
                tools: ToolSet::Direct(vec![DirectFunction {
                    schema: crate::ToolDefinition {
                        name: "same-schema".into(),
                        description: String::new(),
                        parameters: serde_json::json!({"type":"object"}),
                    },
                    handler,
                }]),
                tool_choice: None,
            },
            speculation: false,
        },
    ));
    crate::NativeRealtimeContextProducer::default()
        .attach(&mut frame, crate::RealtimeContextIntent::Run, true)
        .unwrap();
    let cause = frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .clone();
    let retained_cause = cause.clone();
    // The retained cause outlives every frame and callback. The callback's
    // captured environment must drop, while its tiny address identity remains
    // allocated so no replacement callback can acquire the same token address.
    drop(frame);
    assert!(
        weak_capture.upgrade().is_none(),
        "cause retained executable callback ownership"
    );
    assert_eq!(
        weak_identity.strong_count(),
        1,
        "cause failed to retain exactly the tiny identity token"
    );
    assert!(
        weak_identity.upgrade().is_some(),
        "released identity permits allocator ABA"
    );
    assert_eq!(cause.retained_tool_identity_count(), 1);
    drop(cause);
    assert!(
        weak_identity.upgrade().is_some(),
        "one cause clone was incorrectly treated as final owner"
    );
    drop(retained_cause);
    assert!(
        weak_identity.upgrade().is_none(),
        "identity token leaked after final cause drop"
    );
}
