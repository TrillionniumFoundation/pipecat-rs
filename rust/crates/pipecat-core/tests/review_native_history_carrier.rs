//! Independent native-history boundary review.
use pipecat_core::*;
use serde_json::json;
use std::sync::Arc;
fn completed() -> Arc<NativeLlmContinuation> {
    Arc::new(
        NativeLlmContinuation::new(
            81,
            LlmContinuationProvider::Anthropic,
            vec![LlmContinuationBlock::Thinking {
                text: "private thought".into(),
                signature: "opaque-private-signature".into(),
            }],
        )
        .unwrap(),
    )
}
#[test]
fn review_consumption_is_clone_local_and_never_silently_serializes_history() {
    let mut frame = Frame::new(FrameData::LlmResponseEnd);
    let history = completed();
    frame.set_native_llm_continuation(history.clone()).unwrap();
    let untouched = frame.clone();
    let consumed = frame.take_native_llm_continuation().unwrap().unwrap();
    assert!(Arc::ptr_eq(&history, &consumed));
    assert!(frame.native_llm_continuation().unwrap().is_none());
    assert!(serde_json::to_value(&frame).is_ok());
    assert!(serde_json::to_value(&untouched).is_err());
    assert!(!format!("{untouched:?}").contains("opaque-private-signature"));
    assert!(!format!("{untouched:?}").contains("private thought"));
    let wire = serde_json::to_value(&frame).unwrap();
    let restored: Frame = serde_json::from_value(wire).unwrap();
    assert!(restored.native_llm_continuation().unwrap().is_none());
}
#[test]
fn review_mutated_payload_failure_does_not_remove_or_replace_semantics() {
    let mut frame = Frame::new(FrameData::LlmResponseEnd);
    let history = completed();
    frame.set_native_llm_continuation(history.clone()).unwrap();
    frame.data = FrameData::LlmResponseStart;
    assert!(frame.native_llm_continuation().is_err());
    assert!(frame.take_native_llm_continuation().is_err());
    assert_eq!(
        frame.native_llm_continuation_retained_bytes(),
        history.retained_bytes()
    );
    assert!(frame
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::start(82, LlmContinuationProvider::Anthropic).unwrap()
        ))
        .is_err());
    frame.data = FrameData::LlmResponseEnd;
    assert!(Arc::ptr_eq(
        frame.native_llm_continuation().unwrap().unwrap(),
        &history
    ));
    frame.take_native_llm_continuation().unwrap();
    assert_eq!(frame.native_llm_continuation_retained_bytes(), 0);
}
#[test]
fn review_tool_binding_is_ordered_and_exact_before_any_effect() {
    let a = FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: "a".into(),
        arguments: json!({"n": 1}),
    };
    let b = FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: "b".into(),
        arguments: json!({"n": 2}),
    };
    let history = Arc::new(
        NativeLlmContinuation::new(
            9,
            LlmContinuationProvider::Anthropic,
            vec![
                LlmContinuationBlock::ToolCall(a.clone()),
                LlmContinuationBlock::ToolCall(b.clone()),
            ],
        )
        .unwrap(),
    );
    for calls in [
        vec![b.clone(), a.clone()],
        vec![a.clone()],
        vec![
            a.clone(),
            FunctionCall {
                arguments: json!({"n": 3}),
                ..b.clone()
            },
        ],
    ] {
        let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls));
        assert!(frame.set_native_llm_continuation(history.clone()).is_err());
        assert!(frame.native_llm_continuation().unwrap().is_none());
    }
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![a, b]));
    frame.set_native_llm_continuation(history).unwrap();
    let duplicate = frame.duplicate();
    assert!(Arc::ptr_eq(
        frame.native_llm_continuation().unwrap().unwrap(),
        duplicate.native_llm_continuation().unwrap().unwrap()
    ));
    assert_ne!(frame.id, duplicate.id);
}
#[test]
fn review_empty_completion_and_start_have_separate_legal_boundaries() {
    let start =
        Arc::new(NativeLlmContinuation::start(42, LlmContinuationProvider::Anthropic).unwrap());
    let done = Arc::new(
        NativeLlmContinuation::new(42, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
    );
    assert!(!start.is_complete());
    assert!(done.is_complete());
    let mut first = Frame::new(FrameData::LlmResponseStart);
    assert!(first.set_native_llm_continuation(done.clone()).is_err());
    first.set_native_llm_continuation(start.clone()).unwrap();
    let mut last = Frame::new(FrameData::LlmResponseEnd);
    assert!(last.set_native_llm_continuation(start).is_err());
    last.set_native_llm_continuation(done).unwrap();
    assert!(serde_json::to_value(first).is_err());
    assert!(serde_json::to_value(last).is_err());
}
