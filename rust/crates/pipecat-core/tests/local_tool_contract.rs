use pipecat_core::*;
use serde_json::{json, Value};
use std::{
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

fn sealed(handler: LocalToolFunction) -> Frame {
    let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![],
            tools: ToolSet::Direct(vec![DirectFunction {
                schema: ToolDefinition {
                    name: "local".into(),
                    description: String::new(),
                    parameters: json!({"type": "object"}),
                },
                handler,
            }]),
            tool_choice: None,
        },
        speculation: false,
    }));
    NativeRealtimeContextProducer::default()
        .attach(&mut frame, RealtimeContextIntent::Run, false)
        .unwrap();
    frame
}
fn replace_handler(frame: &mut Frame, handler: LocalToolFunction) {
    let FrameData::LlmContextDetailed(context) = &mut frame.data else {
        panic!("expected detailed context");
    };
    let ToolSet::Direct(tools) = &mut context.context.tools else {
        panic!("expected direct handlers");
    };
    tools[0].handler = handler;
}

#[tokio::test]
async fn cancellation_null_and_errors_remain_distinct_with_legacy_invocation() {
    let cancelled = LocalToolFunction::new_cancellable(|_| async { None });
    assert_eq!(cancelled.call_cancellable(Value::Null).await, None);
    let legacy_error = cancelled.call(Value::Null).await.unwrap_err();
    assert_eq!(legacy_error.message, "direct tool invocation was cancelled");
    assert_eq!(legacy_error.category, ErrorCategory::Unknown);
    assert!(!legacy_error.fatal);
    let null = LocalToolFunction::new_cancellable(|_| async { Some(Ok(Value::Null)) });
    assert_eq!(
        null.call_cancellable(Value::Null).await,
        Some(Ok(Value::Null))
    );
    assert_eq!(null.call(Value::Null).await.unwrap(), Value::Null);
    let error = LocalToolFunction::new_cancellable(|_| async {
        Some(Err(ProcessorError::new(
            "direct tool invocation was cancelled",
        )))
    });
    assert_eq!(
        error.call_cancellable(Value::Null).await,
        Some(Err(legacy_error))
    );
    let ordinary = LocalToolFunction::new(|value| async { Ok(value) });
    assert_eq!(ordinary.call(json!(7)).await.unwrap(), json!(7));
    assert_eq!(
        ordinary.call_cancellable(json!(8)).await,
        Some(Ok(json!(8)))
    );
    assert!(serde_json::to_value(&cancelled).is_err());
    assert!(serde_json::from_value::<LocalToolFunction>(Value::Null).is_err());
}

#[tokio::test]
async fn immutable_contract_changes_refresh_both_identities_and_keep_original_options() {
    let handler = LocalToolFunction::new(|value| async { Ok(value) });
    assert_eq!(handler.options(), LocalToolOptions::default());
    let clone = handler.clone();
    assert_eq!(handler, clone);
    let mut frame = sealed(handler.clone());
    replace_handler(&mut frame, clone);
    assert!(frame.native_realtime_context_cause().unwrap().is_some());
    let options = LocalToolOptions {
        cancel_on_interruption: false,
        timeout: Some(Duration::from_secs(5)),
    };
    let configured = handler.with_options(options).unwrap();
    assert_ne!(handler, configured);
    assert_eq!(configured, configured.clone());
    assert_eq!(handler.options(), LocalToolOptions::default());
    assert_eq!(configured.options(), options);
    assert_eq!(configured.call(json!(9)).await.unwrap(), json!(9));
    replace_handler(&mut frame, configured.clone());
    assert!(frame.native_realtime_context_cause().is_err());
    assert!(handler
        .with_options(LocalToolOptions {
            timeout: Some(Duration::ZERO),
            ..options
        })
        .is_err());
    let cleanup_calls = Arc::new(AtomicUsize::new(0));
    let calls = cleanup_calls.clone();
    let cleanup = LocalAsyncCallback::new(move || {
        let calls = calls.clone();
        async move {
            calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    });
    let with_cleanup = configured.with_cleanup(cleanup.clone());
    assert_ne!(configured, with_cleanup);
    assert_eq!(with_cleanup.options(), options);
    assert!(configured.cleanup_callback().is_none());
    assert_eq!(with_cleanup.cleanup_callback(), Some(cleanup));
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 0);
    let mut frame = sealed(configured);
    replace_handler(&mut frame, with_cleanup.clone());
    assert!(frame.native_realtime_context_cause().is_err());
    with_cleanup
        .cleanup_callback()
        .unwrap()
        .call()
        .await
        .unwrap();
    assert_eq!(cleanup_calls.load(Ordering::SeqCst), 1);
}

#[test]
fn retained_contract_token_does_not_keep_callback_or_cleanup_captures() {
    let capture = Arc::new(());
    let weak_callback = Arc::downgrade(&capture);
    let handler = LocalToolFunction::new(move |value| {
        let capture = capture.clone();
        async move {
            drop(capture);
            Ok(value)
        }
    });
    let cleanup_capture = Arc::new(());
    let weak_cleanup = Arc::downgrade(&cleanup_capture);
    let configured = handler.with_cleanup(LocalAsyncCallback::new(move || {
        let capture = cleanup_capture.clone();
        async move {
            drop(capture);
            Ok(())
        }
    }));
    drop(handler);
    let frame = sealed(configured);
    let cause = frame
        .native_realtime_context_cause()
        .unwrap()
        .unwrap()
        .clone();
    drop(frame);
    assert!(weak_callback.upgrade().is_none());
    assert!(weak_cleanup.upgrade().is_none());
    assert_eq!(cause.retained_tool_identity_count(), 1);
}
