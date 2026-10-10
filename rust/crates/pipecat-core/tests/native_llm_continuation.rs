use pipecat_core::*;
use serde_json::json;
use std::sync::Arc;

fn history(blocks: Vec<LlmContinuationBlock>) -> NativeLlmContinuation {
    NativeLlmContinuation::new(17, LlmContinuationProvider::Anthropic, blocks).unwrap()
}
fn thinking() -> LlmContinuationBlock {
    LlmContinuationBlock::Thinking {
        text: "private reasoning".into(),
        signature: "secret signed continuation".into(),
    }
}
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        arguments: json!({"query": "tool argument"}),
    }
}
fn end() -> Frame {
    let mut frame = Frame::new(FrameData::LlmResponseEnd);
    frame
        .set_native_llm_continuation(Arc::new(history(vec![thinking()])))
        .unwrap();
    frame
}

#[test]
fn phase_and_tool_relationships_are_checked_on_attach_read_and_take() {
    let start =
        Arc::new(NativeLlmContinuation::start(17, LlmContinuationProvider::Anthropic).unwrap());
    let empty = Arc::new(history(vec![]));
    assert!(!start.is_complete());
    assert!(empty.is_complete());
    assert_eq!(start.response_id(), 17);
    assert_eq!(start.provider(), LlmContinuationProvider::Anthropic);
    assert!(start.blocks().is_empty());
    assert_ne!(start, empty);
    assert!(Frame::new(FrameData::LlmResponseEnd)
        .set_native_llm_continuation(start.clone())
        .is_err());
    assert!(Frame::new(FrameData::LlmResponseStart)
        .set_native_llm_continuation(empty.clone())
        .is_err());
    let mut begin = Frame::new(FrameData::LlmResponseStart);
    begin.set_native_llm_continuation(start.clone()).unwrap();
    assert_eq!(
        begin.set_native_llm_continuation(start),
        Err(NativeLlmContinuationError::AlreadyAttached)
    );
    let mut finish = Frame::new(FrameData::LlmResponseEnd);
    finish.set_native_llm_continuation(empty).unwrap();
    assert!(finish
        .take_native_llm_continuation()
        .unwrap()
        .unwrap()
        .is_complete());

    let calls = vec![call("a"), call("b")];
    let blocks = vec![
        thinking(),
        LlmContinuationBlock::ToolCall(calls[0].clone()),
        LlmContinuationBlock::Text("between".into()),
        LlmContinuationBlock::ToolCall(calls[1].clone()),
    ];
    let continuation = Arc::new(history(blocks.clone()));
    assert_eq!(continuation.blocks(), blocks);
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(calls.clone()));
    frame
        .set_native_llm_continuation(continuation.clone())
        .unwrap();
    let bytes = frame.native_llm_continuation_retained_bytes();
    for wrong in [
        FrameData::LlmResponseEnd,
        FrameData::FunctionCallsStarted(vec![]),
        FrameData::FunctionCallsStarted(vec![calls[1].clone(), calls[0].clone()]),
        FrameData::FunctionCallsStarted(vec![call("other")]),
        FrameData::Text(TextFrame::new("wrong")),
    ] {
        frame.data = wrong.clone();
        assert_eq!(
            frame.native_llm_continuation(),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
        assert_eq!(
            frame.take_native_llm_continuation(),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
        assert_eq!(frame.native_llm_continuation_retained_bytes(), bytes);
        assert!(serde_json::to_vec(&frame).is_err());
        assert!(Frame::new(wrong)
            .set_native_llm_continuation(continuation.clone())
            .is_err());
    }
    frame.data = FrameData::FunctionCallsStarted(calls);
    assert!(Arc::ptr_eq(
        frame.native_llm_continuation().unwrap().unwrap(),
        &continuation
    ));
    assert!(Arc::ptr_eq(
        &frame.take_native_llm_continuation().unwrap().unwrap(),
        &continuation
    ));
    assert!(frame.native_llm_continuation().unwrap().is_none());
    assert_eq!(frame.native_llm_continuation_retained_bytes(), 0);
}

#[test]
fn clone_duplicate_and_broadcast_preserve_provider_response_identity() {
    let original = end();
    let continuation = original.native_llm_continuation().unwrap().unwrap();
    let (down, up) = original.broadcast_pair();
    for frame in [original.clone(), original.duplicate(), down, up] {
        let attached = frame.native_llm_continuation().unwrap().unwrap();
        assert!(Arc::ptr_eq(attached, continuation));
        assert_eq!(attached.response_id(), 17);
        assert_eq!(
            frame.native_llm_continuation_retained_bytes(),
            continuation.retained_bytes()
        );
    }
    assert_eq!(original.id, original.clone().id);
    assert_ne!(original.id, original.duplicate().id);
    let mut changed = original;
    changed.data = FrameData::Interruption;
    let (down, up) = changed.broadcast_pair();
    for mut frame in [changed.clone(), changed.duplicate(), down, up] {
        assert_eq!(
            frame.native_llm_continuation(),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
        assert_eq!(
            frame.take_native_llm_continuation(),
            Err(NativeLlmContinuationError::FrameMismatch)
        );
        assert!(serde_json::to_vec(&frame).is_err());
    }
}

#[test]
fn native_history_is_never_wire_serialized_or_debug_disclosed() {
    let mut frame = end();
    let plain = Frame::new(FrameData::LlmResponseEnd);
    let mut ordinary = frame.clone();
    ordinary.take_native_llm_continuation().unwrap();
    assert_ne!(frame, ordinary);
    assert!(frame.require_no_native_llm_continuation().is_err());
    let debug = format!(
        "{frame:?} {:?} {:?}",
        thinking(),
        LlmContinuationBlock::ToolCall(call("secret-tool-id"))
    );
    for secret in [
        "private reasoning",
        "secret signed continuation",
        "secret-tool-id",
        "tool argument",
    ] {
        assert!(!debug.contains(secret));
    }
    let err = serde_json::to_string(&frame).unwrap_err().to_string();
    assert!(err.contains("must be consumed"));
    assert!(!err.contains("secret"));
    frame.take_native_llm_continuation().unwrap();
    let wire = serde_json::to_value(&frame).unwrap();
    assert_eq!(wire.as_object().unwrap().len(), 9);
    assert_eq!(wire["data"], serde_json::to_value(plain).unwrap()["data"]);
    assert!(wire.get("native_llm_continuation").is_none());
    let decoded: Frame = serde_json::from_value(wire).unwrap();
    assert_eq!(frame, decoded);
    assert!(decoded.native_llm_continuation().unwrap().is_none());
}

#[test]
fn ordinary_wire_fixture_and_priorities_are_unchanged() {
    let wire = json!({"id": 7,"name":"LLMFullResponseEndFrame#0","pts":null,"metadata":{},"transport_source":null,"transport_destination":null,"broadcast_sibling_id":null,"interruptible":true,"data":{"type":"llm_response_end"}});
    let frame: Frame = serde_json::from_value(wire.clone()).unwrap();
    assert_eq!(serde_json::to_value(frame).unwrap(), wire);
    assert_eq!(Frame::start().priority(), FramePriority::Start);
    let mut tool = Frame::new(FrameData::FunctionCallsStarted(vec![call("a")]));
    tool.set_native_llm_continuation(Arc::new(history(vec![LlmContinuationBlock::ToolCall(
        call("a"),
    )])))
    .unwrap();
    assert_eq!(tool.priority(), FramePriority::System);
    assert_eq!(end().priority(), FramePriority::Normal);
}

#[test]
fn invalid_ids_signatures_and_tool_arguments_are_rejected() {
    assert_eq!(
        NativeLlmContinuation::new(0, LlmContinuationProvider::Anthropic, vec![]),
        Err(NativeLlmContinuationError::InvalidResponseId)
    );
    assert!(NativeLlmContinuation::start(0, LlmContinuationProvider::Anthropic).is_err());
    for signature in [
        String::new(),
        "s".repeat(NativeLlmContinuation::MAX_SIGNATURE_BYTES + 1),
    ] {
        assert_eq!(
            NativeLlmContinuation::new(
                1,
                LlmContinuationProvider::Anthropic,
                vec![LlmContinuationBlock::Thinking {
                    text: "t".into(),
                    signature
                }]
            ),
            Err(NativeLlmContinuationError::InvalidSignature)
        );
    }
    let boundary = history(vec![LlmContinuationBlock::Thinking {
        text: String::new(),
        signature: "s".repeat(NativeLlmContinuation::MAX_SIGNATURE_BYTES),
    }]);
    assert_eq!(boundary.blocks().len(), 1);
    for bad in ["", " ", "with space", "new\nline", "nul\0byte"] {
        let mut id = call(bad);
        assert!(NativeLlmContinuation::new(
            1,
            LlmContinuationProvider::Anthropic,
            vec![LlmContinuationBlock::ToolCall(id.clone())]
        )
        .is_err());
        id.tool_call_id = "valid".into();
        id.function_name = bad.into();
        assert!(NativeLlmContinuation::new(
            1,
            LlmContinuationProvider::Anthropic,
            vec![LlmContinuationBlock::ToolCall(id)]
        )
        .is_err());
    }
    for arguments in [json!(null), json!([]), json!("{}"), json!(true), json!(1)] {
        let mut bad = call("valid");
        bad.arguments = arguments;
        assert!(NativeLlmContinuation::new(
            1,
            LlmContinuationProvider::Anthropic,
            vec![LlmContinuationBlock::ToolCall(bad)]
        )
        .is_err());
    }
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            LlmContinuationProvider::Anthropic,
            vec![
                LlmContinuationBlock::ToolCall(call("duplicate")),
                LlmContinuationBlock::ToolCall(call("duplicate"))
            ]
        ),
        Err(NativeLlmContinuationError::InvalidToolCall)
    );
}

#[test]
fn retained_byte_bounds_count_spare_capacity_and_json_structure() {
    let provider = LlmContinuationProvider::Anthropic;
    let max = NativeLlmContinuation::MAX_RETAINED_BYTES;
    let overhead = history(vec![LlmContinuationBlock::Text(String::new())]).retained_bytes();
    let exact = history(vec![LlmContinuationBlock::Text("x".repeat(max - overhead))]);
    assert_eq!(exact.retained_bytes(), max);
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            provider,
            vec![LlmContinuationBlock::Text("x".repeat(max - overhead + 1))]
        ),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
    let reserved = String::with_capacity(max);
    assert_eq!(
        NativeLlmContinuation::new(1, provider, vec![LlmContinuationBlock::Text(reserved)]),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
    let reserved_blocks = Vec::with_capacity(max / std::mem::size_of::<LlmContinuationBlock>() + 1);
    assert_eq!(
        NativeLlmContinuation::new(1, provider, reserved_blocks),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
    let mut huge_array = call("array");
    huge_array.arguments = json!({"values": []});
    huge_array.arguments["values"] = serde_json::Value::Array(Vec::with_capacity(
        max / std::mem::size_of::<serde_json::Value>(),
    ));
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            provider,
            vec![LlmContinuationBlock::ToolCall(huge_array)]
        ),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
    let mut large_key = call("key");
    large_key.arguments = serde_json::Value::Object(
        [(String::with_capacity(max), json!(null))]
            .into_iter()
            .collect(),
    );
    assert_eq!(
        NativeLlmContinuation::new(1, provider, vec![LlmContinuationBlock::ToolCall(large_key)]),
        Err(NativeLlmContinuationError::TooManyBytes)
    );
}

#[test]
fn block_count_and_argument_depth_are_bounded() {
    let provider = LlmContinuationProvider::Anthropic;
    let block = LlmContinuationBlock::Text(String::new());
    assert!(NativeLlmContinuation::new(
        1,
        provider,
        vec![block.clone(); NativeLlmContinuation::MAX_BLOCKS]
    )
    .is_ok());
    assert_eq!(
        NativeLlmContinuation::new(
            1,
            provider,
            vec![block; NativeLlmContinuation::MAX_BLOCKS + 1]
        ),
        Err(NativeLlmContinuationError::TooManyBlocks)
    );
    let mut value = json!(null);
    for _ in 0..NativeLlmContinuation::MAX_ARGUMENT_DEPTH + 1 {
        value = serde_json::Value::Array(vec![value]);
    }
    let mut deep = call("deep");
    deep.arguments = serde_json::Value::Object([("nested".into(), value)].into_iter().collect());
    assert_eq!(
        NativeLlmContinuation::new(1, provider, vec![LlmContinuationBlock::ToolCall(deep)]),
        Err(NativeLlmContinuationError::ArgumentsTooDeep)
    );
}

#[test]
fn persisted_rich_context_keeps_signatures_for_replay_but_redacts_debug() {
    let mut frame = end();
    let history = frame.take_native_llm_continuation().unwrap().unwrap();
    let parts = history
        .blocks()
        .iter()
        .map(|block| match block {
            LlmContinuationBlock::Thinking { text, signature } => MessagePart::Thinking {
                text: text.clone(),
                signature: Some(signature.clone()),
            },
            _ => unreachable!(),
        })
        .collect();
    let context = LlmContextData {
        messages: vec![ContextMessage {
            role: ChatRole::Assistant,
            content: MessageContent::Parts(parts),
            name: None,
            tool_call_id: None,
            tool_calls: vec![],
            extra: Default::default(),
        }],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    };
    let frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context,
        speculation: false,
    }));
    let debug = format!("{frame:?}");
    assert!(!debug.contains("secret signed continuation"));
    assert!(debug.contains("[redacted]"));
    let wire = serde_json::to_string(&frame).unwrap();
    assert!(wire.contains("secret signed continuation"));
    assert_eq!(serde_json::from_str::<Frame>(&wire).unwrap(), frame);
    assert_eq!(
        format!(
            "{:?}",
            MessagePart::Text {
                text: "text".into()
            }
        ),
        "Text { text: \"text\" }"
    );
    assert_eq!(
        format!(
            "{:?}",
            MessagePart::Image {
                url: "u".into(),
                detail: None
            }
        ),
        "Image { url: \"u\", detail: None }"
    );
    assert_eq!(
        format!(
            "{:?}",
            MessagePart::Audio {
                data: "d".into(),
                format: "f".into()
            }
        ),
        "Audio { data: \"d\", format: \"f\" }"
    );
    assert_eq!(
        format!(
            "{:?}",
            MessagePart::File {
                data: "d".into(),
                mime_type: "m".into(),
                filename: Some("n".into())
            }
        ),
        "File { data: \"d\", mime_type: \"m\", filename: Some(\"n\") }"
    );
    assert_eq!(
        format!("{:?}", MessagePart::Refusal { text: "t".into() }),
        "Refusal { text: \"t\" }"
    );
    assert_eq!(
        format!(
            "{:?}",
            MessagePart::Thinking {
                text: "t".into(),
                signature: None
            }
        ),
        "Thinking { text: \"t\", signature: None }"
    );
}
