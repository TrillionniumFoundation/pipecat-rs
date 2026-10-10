//! Signed tool-result mirrors fail closed before changing context or ownership.
use pipecat_core::*;
use pipecat_turns::{LLMAssistantAggregator, SharedContext};
use serde_json::json;
use std::sync::Arc;

fn signed(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
fn update(call: &FunctionCall, value: serde_json::Value) -> Frame {
    Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: call.function_name.clone(),
        tool_call_id: call.tool_call_id.clone(),
        arguments: call.arguments.clone(),
        result: value,
        run_llm: Some(false),
        is_final: false,
        error: None,
    }))
}
#[test]
fn independent_signed_mirror_rejects_accumulation_before_context_mutation() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let call = FunctionCall {
        function_name: "stream_work".into(),
        tool_call_id: "signed-mirror-bounded".into(),
        arguments: json!({}),
    };
    assistant
        .process_frame(
            signed(
                FrameData::LlmResponseStart,
                NativeLlmContinuation::start(7321, LlmContinuationProvider::Anthropic).unwrap(),
            ),
            Direction::Downstream,
        )
        .unwrap();
    assistant
        .process_frame(
            signed(
                FrameData::FunctionCallsStarted(vec![call.clone()]),
                NativeLlmContinuation::new(
                    7321,
                    LlmContinuationProvider::Anthropic,
                    vec![
                        LlmContinuationBlock::Thinking {
                            text: "reasoning".into(),
                            signature: "opaque+/==".into(),
                        },
                        LlmContinuationBlock::ToolCall(call.clone()),
                    ],
                )
                .unwrap(),
            ),
            Direction::Downstream,
        )
        .unwrap();
    assistant
        .process_frame(
            Frame::new(FrameData::FunctionCallInProgress(
                FunctionCallInProgressFrame {
                    function_name: call.function_name.clone(),
                    tool_call_id: call.tool_call_id.clone(),
                    arguments: call.arguments.clone(),
                    cancel_on_interruption: false,
                    group_id: Some("signed-mirror-group".into()),
                },
            )),
            Direction::Downstream,
        )
        .unwrap();
    for _ in 0..5 {
        let output = assistant
            .process_frame(
                update(&call, json!("x".repeat(6 * 1024 * 1024))),
                Direction::Downstream,
            )
            .expect("valid signed mirror prefix below 32 MiB must remain usable");
        assert!(
            output.actions.is_empty(),
            "legacy run_llm=false must remain effective"
        );
    }
    let before = context.snapshot_rich().unwrap();
    let error = assistant
        .process_frame(
            update(&call, json!("y".repeat(6 * 1024 * 1024))),
            Direction::Downstream,
        )
        .expect_err("signed result mirrors must not accumulate beyond their byte cap");
    assert!(
        error.to_string().contains("signed"),
        "unexpected rejection: {error}"
    );
    assert_eq!(
        context.snapshot_rich().unwrap(),
        before,
        "capacity rejection changed context before failing"
    );
    assert!(assistant.has_function_calls_in_progress().unwrap());
    assistant
        .process_frame(
            update(&call, json!("small valid tail")),
            Direction::Downstream,
        )
        .expect("rejected mirror update must not corrupt subsequent admission");
    assert_eq!(
        context.snapshot_rich().unwrap().messages.len(),
        before.messages.len() + 1
    );
    assistant
        .process_frame(Frame::new(FrameData::LlmResponseEnd), Direction::Downstream)
        .unwrap();
    let snapshot = context.snapshot().unwrap();
    assert!(snapshot.messages.iter().any(|message| matches!(&message.content, Some(pipecat_services::Content::Parts(parts))
        if parts.iter().any(|part| matches!(part, pipecat_services::ContentPart::Thinking { text, signature }
            if text == "reasoning" && signature.as_deref() == Some("opaque+/=="))))),
        "signed reconciliation must retain the exact signature after a rejected update");
}
