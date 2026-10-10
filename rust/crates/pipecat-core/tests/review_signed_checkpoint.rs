//! Checkpoint phases retain native-only boundaries independently of completion.
use pipecat_core::*;
use serde_json::json;
use std::sync::Arc;

fn history() -> Arc<NativeLlmContinuation> {
    Arc::new(
        NativeLlmContinuation::checkpoint(
            91,
            LlmContinuationProvider::Anthropic,
            vec![
                LlmContinuationBlock::Thinking {
                    text: "hidden review thought".into(),
                    signature: "hidden review signature".into(),
                },
                LlmContinuationBlock::ToolCall(FunctionCall {
                    function_name: "effect".into(),
                    tool_call_id: "pending-tool".into(),
                    arguments: json!({}),
                }),
            ],
        )
        .unwrap(),
    )
}

#[test]
fn review_checkpoint_is_never_a_completed_tool_admission_or_response_boundary() {
    let history = history();
    assert!(history.is_checkpoint());
    assert!(!history.is_complete());
    let LlmContinuationBlock::ToolCall(call) = &history.blocks()[1] else {
        panic!("tool fixture");
    };
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmResponseEnd,
        FrameData::LlmThoughtEnd,
        FrameData::FunctionCallsStarted(vec![call.clone()]),
    ] {
        let mut frame = Frame::new(data);
        assert_eq!(
            frame.set_native_llm_continuation(history.clone()),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
    }
    let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
    frame.set_native_llm_continuation(history).unwrap();
    assert_eq!(frame.priority(), FramePriority::Normal);
    for other in [
        NativeLlmContinuation::start(91, LlmContinuationProvider::Anthropic).unwrap(),
        NativeLlmContinuation::new(91, LlmContinuationProvider::Anthropic, vec![]).unwrap(),
    ] {
        assert_eq!(
            Frame::new(FrameData::LlmContinuationCheckpoint)
                .set_native_llm_continuation(Arc::new(other)),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
    }
}

#[test]
fn review_checkpoint_clones_remain_local_and_reject_wire_serialization() {
    let history = history();
    let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
    frame.set_native_llm_continuation(history.clone()).unwrap();
    let (down, up) = frame.broadcast_pair();
    for copy in [frame.duplicate(), down, up] {
        assert!(Arc::ptr_eq(
            copy.native_llm_continuation().unwrap().unwrap(),
            &history
        ));
        assert!(serde_json::to_vec(&copy).is_err());
        let debug = format!("{copy:?}");
        assert!(!debug.contains("hidden review signature"));
        assert!(!debug.contains("hidden review thought"));
    }
    frame.take_native_llm_continuation().unwrap();
    assert_eq!(
        frame.native_llm_continuation(),
        Err(NativeLlmContinuationError::MissingCarrier)
    );
    assert!(serde_json::to_vec(&frame).is_err());
    assert!(serde_json::to_vec(&FrameData::LlmContinuationCheckpoint).is_err());
    let bare = Frame::new(FrameData::LlmContinuationCheckpoint);
    assert_eq!(
        bare.native_llm_continuation(),
        Err(NativeLlmContinuationError::MissingCarrier)
    );
    assert!(bare.require_no_native_llm_continuation().is_err());
    assert!(serde_json::to_vec(&bare).is_err());
    assert!(
        serde_json::from_value::<FrameData>(json!({"type":"llm_continuation_checkpoint"})).is_err()
    );
}

#[test]
fn review_checkpoint_limits_count_capacity_before_attachment() {
    let provider = LlmContinuationProvider::Anthropic;
    let mut signature = String::with_capacity(NativeLlmContinuation::MAX_SIGNATURE_BYTES + 1);
    signature.push('s');
    assert_eq!(
        NativeLlmContinuation::checkpoint(
            1,
            provider,
            vec![LlmContinuationBlock::Thinking {
                text: String::new(),
                signature
            }]
        ),
        Err(NativeLlmContinuationError::InvalidSignature)
    );
    assert_eq!(
        NativeLlmContinuation::checkpoint(
            1,
            provider,
            vec![LlmContinuationBlock::Text(String::with_capacity(
                NativeLlmContinuation::MAX_RETAINED_BYTES
            ))]
        ),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
    assert_eq!(
        NativeLlmContinuation::checkpoint(
            1,
            provider,
            vec![LlmContinuationBlock::Text(String::new()); NativeLlmContinuation::MAX_BLOCKS + 1]
        ),
        Err(NativeLlmContinuationError::TooManyBlocks)
    );
}
