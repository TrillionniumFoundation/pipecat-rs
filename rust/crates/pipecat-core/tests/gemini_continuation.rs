//! Gemini continuation identity, opaque provenance, and native-only transport.
use pipecat_core::*;
use serde_json::json;
use std::sync::Arc;

fn text(value: &str, thought: bool, signature: Option<&str>) -> LlmContinuationBlock {
    LlmContinuationBlock::GeminiText {
        text: value.into(),
        thought,
        signature: signature.map(Into::into),
    }
}
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "echo".into(),
        tool_call_id: id.into(),
        arguments: json!({"value":"private argument"}),
    }
}
fn metadata(id: &str, signature: Option<&str>) -> LlmContinuationBlock {
    LlmContinuationBlock::GeminiToolMetadata {
        tool_call_id: id.into(),
        signature: signature.map(Into::into),
    }
}
fn history(blocks: Vec<LlmContinuationBlock>) -> NativeLlmContinuation {
    NativeLlmContinuation::new(17, LlmContinuationProvider::Gemini, blocks).unwrap()
}

#[test]
fn gemini_phases_validate_exact_frame_and_tool_identity() {
    let provider = LlmContinuationProvider::Gemini;
    let start = Arc::new(NativeLlmContinuation::start(17, provider).unwrap());
    let checkpoint = Arc::new(
        NativeLlmContinuation::checkpoint(17, provider, vec![text("", false, Some("tail+/=="))])
            .unwrap(),
    );
    let completed = Arc::new(history(vec![text("", false, Some("tail+/=="))]));
    assert!(!start.is_complete());
    assert!(checkpoint.is_checkpoint());
    assert!(completed.is_complete());
    assert_eq!(completed.provider(), provider);
    assert_eq!(completed.response_id(), 17);
    for (data, carrier) in [
        (FrameData::LlmResponseStart, start),
        (FrameData::LlmContinuationCheckpoint, checkpoint),
        (FrameData::LlmResponseEnd, completed),
    ] {
        let mut frame = Frame::new(data);
        frame.set_native_llm_continuation(carrier.clone()).unwrap();
        for duplicate in [frame.clone(), frame.duplicate()] {
            assert!(Arc::ptr_eq(
                duplicate.native_llm_continuation().unwrap().unwrap(),
                &carrier
            ));
        }
        frame.data = FrameData::Interruption;
        assert_eq!(
            frame.take_native_llm_continuation(),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
        assert!(serde_json::to_vec(&frame).is_err());
    }
    let calls = vec![call("a"), call("b")];
    let carrier = Arc::new(history(vec![
        text("same", false, Some("text-signature")),
        LlmContinuationBlock::ToolCall(calls[0].clone()),
        metadata("a", Some("tool-signature")),
        LlmContinuationBlock::ToolCall(calls[1].clone()),
        metadata("b", None),
    ]));
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls.clone()));
    frame.set_native_llm_continuation(carrier).unwrap();
    let bytes = frame.native_llm_continuation_retained_bytes();
    frame.data = FrameData::FunctionCallsStarted(vec![calls[1].clone(), calls[0].clone()]);
    assert_eq!(
        frame.native_llm_continuation(),
        Err(NativeLlmContinuationError::FrameMismatch)
    );
    assert_eq!(frame.native_llm_continuation_retained_bytes(), bytes);
    frame.data = FrameData::FunctionCallsStarted(calls);
    assert!(frame.take_native_llm_continuation().unwrap().is_some());
    assert_eq!(frame.native_llm_continuation_retained_bytes(), 0);
}

#[test]
fn provider_and_adjacent_tool_bookmarks_cannot_be_forged_or_omitted() {
    let gemini = LlmContinuationProvider::Gemini;
    let anthropic = LlmContinuationProvider::Anthropic;
    for blocks in [
        vec![LlmContinuationBlock::ToolCall(call("a"))],
        vec![metadata("a", Some("signature"))],
        vec![
            LlmContinuationBlock::ToolCall(call("a")),
            metadata("b", Some("signature")),
        ],
        vec![
            LlmContinuationBlock::ToolCall(call("a")),
            text("between", false, None),
            metadata("a", None),
        ],
        vec![
            LlmContinuationBlock::ToolCall(call("a")),
            metadata("a", None),
            metadata("a", None),
        ],
        vec![LlmContinuationBlock::Thinking {
            text: "thought".into(),
            signature: "signature".into(),
        }],
    ] {
        assert!(NativeLlmContinuation::new(1, gemini, blocks).is_err());
    }
    for blocks in [
        vec![text("thought", true, Some("signature"))],
        vec![
            LlmContinuationBlock::ToolCall(call("a")),
            metadata("a", None),
        ],
    ] {
        assert!(NativeLlmContinuation::new(1, anthropic, blocks).is_err());
    }
    assert!(NativeLlmContinuation::new(
        1,
        gemini,
        vec![
            LlmContinuationBlock::ToolCall(call("a")),
            metadata("a", None),
            LlmContinuationBlock::ToolCall(call("a")),
            metadata("a", None)
        ]
    )
    .is_err());
    assert!(
        NativeLlmContinuation::checkpoint(1, gemini, vec![text("unsigned", false, None)]).is_err()
    );
    assert!(NativeLlmContinuation::checkpoint(
        1,
        gemini,
        vec![
            LlmContinuationBlock::ToolCall(call("a")),
            metadata("a", Some("signed-tool"))
        ]
    )
    .is_ok());
}

#[test]
fn signatures_and_reserved_memory_are_bounded_for_both_part_kinds() {
    let provider = LlmContinuationProvider::Gemini;
    assert!(NativeLlmContinuation::new(0, provider, vec![]).is_err());
    for signature in [
        String::new(),
        "s".repeat(NativeLlmContinuation::MAX_SIGNATURE_BYTES + 1),
    ] {
        for blocks in [
            vec![text("", false, Some(&signature))],
            vec![
                LlmContinuationBlock::ToolCall(call("a")),
                metadata("a", Some(&signature)),
            ],
        ] {
            assert_eq!(
                NativeLlmContinuation::new(1, provider, blocks),
                Err(NativeLlmContinuationError::InvalidSignature)
            );
        }
    }
    let boundary = "s".repeat(NativeLlmContinuation::MAX_SIGNATURE_BYTES);
    assert!(NativeLlmContinuation::new(1, provider, vec![text("", true, Some(&boundary))]).is_ok());
    let mut reserved = String::with_capacity(NativeLlmContinuation::MAX_SIGNATURE_BYTES + 1);
    reserved.push('s');
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            provider,
            vec![LlmContinuationBlock::GeminiText {
                text: String::new(),
                thought: false,
                signature: Some(reserved)
            }]
        ),
        Err(NativeLlmContinuationError::InvalidSignature)
    );
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            provider,
            vec![LlmContinuationBlock::GeminiText {
                text: String::with_capacity(NativeLlmContinuation::MAX_RETAINED_BYTES),
                thought: false,
                signature: None
            }]
        ),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            provider,
            vec![text("", false, None); NativeLlmContinuation::MAX_BLOCKS + 1]
        ),
        Err(NativeLlmContinuationError::TooManyBlocks)
    );
}

#[test]
fn native_gemini_details_are_opaque_and_never_silently_wire_encoded() {
    let blocks = vec![
        text("private thought", true, Some("private-signature+/==")),
        LlmContinuationBlock::ToolCall(call("private-id")),
        metadata("private-id", None),
    ];
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![call("private-id")]));
    let carrier = Arc::new(history(blocks.clone()));
    frame.set_native_llm_continuation(carrier.clone()).unwrap();
    let debug = format!("{carrier:?} {blocks:?}");
    for secret in [
        "private thought",
        "private-signature",
        "private-id",
        "private argument",
    ] {
        assert!(!debug.contains(secret));
    }
    let error = serde_json::to_string(&frame).unwrap_err().to_string();
    assert!(error.contains("must be consumed"));
    assert!(!error.contains("private"));
    frame.take_native_llm_continuation().unwrap();
    let wire = serde_json::to_value(&frame).unwrap();
    assert!(wire.get("native_llm_continuation").is_none());
    assert!(!wire.to_string().contains("private-signature"));
}

#[test]
fn persisted_gemini_parts_preserve_bytes_and_redact_debug() {
    let parts = vec![
        MessagePart::GeminiText {
            text: "private thought".into(),
            thought: true,
            signature: Some("exact+/signature==".into()),
            incomplete: false,
        },
        MessagePart::GeminiText {
            text: "heard".into(),
            thought: false,
            signature: None,
            incomplete: true,
        },
        MessagePart::GeminiToolMetadata {
            tool_call_id: "id".into(),
            signature: None,
        },
    ];
    let debug = format!("{parts:?}");
    assert!(!debug.contains("exact+/signature=="));
    assert!(!debug.contains("private thought"));
    let wire = serde_json::to_string(&parts).unwrap();
    assert!(wire.contains("exact+/signature=="));
    assert_eq!(
        serde_json::from_str::<Vec<MessagePart>>(&wire).unwrap(),
        parts
    );
}
