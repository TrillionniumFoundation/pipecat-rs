//! Independent review of signed continuation ownership and delivery boundaries.
use pipecat_core::*;
use pipecat_services::{
    adapters::request_body, CompletionRequest, Context, Message, Provider, Role,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::sync::Arc;

fn process(
    assistant: &LLMAssistantAggregator,
    frame: Frame,
) -> Result<AggregationOutput, ProcessorError> {
    assistant.process_frame(frame, Direction::Downstream)
}
fn start(id: u64) -> Frame {
    attached(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(id, LlmContinuationProvider::Anthropic).unwrap(),
    )
}
fn attached(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
fn checkpoint(id: u64, blocks: Vec<LlmContinuationBlock>) -> Frame {
    attached(
        FrameData::LlmContinuationCheckpoint,
        NativeLlmContinuation::checkpoint(id, LlmContinuationProvider::Anthropic, blocks).unwrap(),
    )
}
fn end(id: u64, blocks: Vec<LlmContinuationBlock>) -> Frame {
    attached(
        FrameData::LlmResponseEnd,
        NativeLlmContinuation::new(id, LlmContinuationProvider::Anthropic, blocks).unwrap(),
    )
}
fn thought(text: &str, signature: &str) -> LlmContinuationBlock {
    LlmContinuationBlock::Thinking {
        text: text.into(),
        signature: signature.into(),
    }
}
fn wire(context: &SharedContext) -> Value {
    request_body(
        Provider::Anthropic,
        &CompletionRequest::new("fixture", context.snapshot().unwrap()),
    )
    .unwrap()
}
fn text(assistant: &LLMAssistantAggregator, value: &str) {
    process(
        assistant,
        Frame::new(FrameData::LlmText(TextFrame::llm(value))),
    )
    .unwrap();
}
fn receipt(id: &str, value: &str) -> [AggregatedTextFrame; 1] {
    [AggregatedTextFrame {
        text: TextFrame::llm(value),
        raw_text: Some(value.into()),
        context_id: Some(id.into()),
        aggregated_by: "review".into(),
        will_be_spoken: true,
    }]
}

#[test]
fn review_cancel_keeps_closed_signature_and_heard_tail_but_no_partial_thought() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(701)).unwrap();
    text(&assistant, "前");
    let prefix = vec![
        LlmContinuationBlock::Text("前".into()),
        thought("一\n二", "opaque+/=="),
    ];
    process(&assistant, checkpoint(701, prefix)).unwrap();
    text(&assistant, "後");
    process(&assistant, Frame::new(FrameData::LlmThoughtStart)).unwrap();
    process(
        &assistant,
        Frame::new(FrameData::LlmThoughtText(TextFrame::llm("partial secret"))),
    )
    .unwrap();
    process(&assistant, Frame::interruption()).unwrap();
    assert_eq!(
        wire(&context)["messages"][0]["content"],
        json!([
            {"type":"text","text":"前"},
            {"type":"thinking","thinking":"一\n二","signature":"opaque+/=="},
            {"type":"text","text":"後"}
        ])
    );
    assert!(!wire(&context).to_string().contains("partial secret"));
}

#[test]
fn review_spoken_checkpoint_does_not_invent_text_or_unadmitted_tool_calls() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(702)).unwrap();
    assistant.begin_playback("pending").unwrap();
    let prefix = vec![
        LlmContinuationBlock::Text("unheard".into()),
        thought("closed first", "first-signature"),
        LlmContinuationBlock::ToolCall(FunctionCall {
            function_name: "effect".into(),
            tool_call_id: "never-admitted".into(),
            arguments: json!({}),
        }),
        thought("closed second", "second-signature"),
    ];
    process(&assistant, checkpoint(702, prefix)).unwrap();
    process(&assistant, Frame::interruption()).unwrap();
    assert_eq!(
        wire(&context)["messages"][0]["content"],
        json!([
            {"type":"thinking","thinking":"closed first","signature":"first-signature"},
            {"type":"thinking","thinking":"closed second","signature":"second-signature"}
        ])
    );
    let before = wire(&context);
    assistant
        .confirm_playback("pending", &receipt("pending", "unheard"))
        .unwrap();
    assert_eq!(
        wire(&context),
        before,
        "late playback cannot reopen an interrupted response"
    );
}

#[test]
fn review_checkpoint_order_is_monotone_and_conflict_does_not_replace_history() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(703)).unwrap();
    let first = vec![thought("first", "sig-a")];
    let full = vec![thought("first", "sig-a"), thought("", "sig-b")];
    process(&assistant, checkpoint(703, full.clone())).unwrap();
    let before = wire(&context);
    process(&assistant, checkpoint(703, first)).unwrap();
    assert_eq!(wire(&context), before);
    assert!(process(
        &assistant,
        checkpoint(703, vec![thought("first", "changed")])
    )
    .is_err());
    assert_eq!(wire(&context), before);
    process(&assistant, end(703, full)).unwrap();
    assert_eq!(wire(&context), before);
}

#[test]
fn review_old_checkpoint_cannot_pollute_new_response() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(704)).unwrap();
    process(&assistant, checkpoint(704, vec![thought("old", "sig-old")])).unwrap();
    process(&assistant, Frame::interruption()).unwrap();
    process(&assistant, start(705)).unwrap();
    process(
        &assistant,
        checkpoint(704, vec![thought("stale replacement", "sig-stale")]),
    )
    .unwrap();
    text(&assistant, "new reply");
    process(
        &assistant,
        end(
            705,
            vec![
                LlmContinuationBlock::Text("new reply".into()),
                thought("new", "sig-new"),
            ],
        ),
    )
    .unwrap();
    let body = wire(&context).to_string();
    assert!(!body.contains("sig-stale"));
    assert_eq!(body.matches("sig-old").count(), 1);
    assert_eq!(body.matches("sig-new").count(), 1);
}

#[test]
fn review_signed_context_rejects_cross_provider_replay_after_rich_round_trip() {
    let context = SharedContext::new(Context::new(vec![Message::text(Role::User, "question")]));
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(706)).unwrap();
    process(&assistant, end(706, vec![thought("", "opaque-signature")])).unwrap();
    let rich = context.snapshot_rich().unwrap();
    let cloned = pipecat_services::frame_context::context(&rich).unwrap();
    let request = CompletionRequest::new("fixture", cloned);
    assert!(request_body(Provider::OpenAi, &request).is_err());
    assert!(request_body(Provider::Gemini, &request).is_err());
    assert_eq!(
        request_body(Provider::Anthropic, &request).unwrap(),
        wire(&context)
    );
}

#[test]
fn review_rejected_nonprefix_completion_does_not_publish_signed_content() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    process(&assistant, start(707)).unwrap();
    text(&assistant, "changed delivery");
    assert!(process(
        &assistant,
        end(
            707,
            vec![
                LlmContinuationBlock::Text("original".into()),
                thought("not admitted", "not-admitted-signature")
            ]
        )
    )
    .is_err());
    assert!(
        !wire(&context)
            .to_string()
            .contains("not-admitted-signature"),
        "rejecting a delivery mismatch must not publish the rejected signed snapshot"
    );
}

#[test]
fn review_interior_whitespace_transformation_is_not_a_delivered_prefix() {
    for (id, generated, delivered) in [(709, "a b", "ab"), (710, "a\nb", "a b")] {
        let context = SharedContext::default();
        let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
        process(&assistant, start(id)).unwrap();
        text(&assistant, delivered);
        assert!(process(
            &assistant,
            end(
                id,
                vec![
                    LlmContinuationBlock::Text(generated.into()),
                    thought("thought", "sig-whitespace")
                ]
            )
        )
        .is_err());
        assert!(!wire(&context).to_string().contains("sig-whitespace"));
    }
}

#[test]
fn review_completion_before_older_checkpoint_keeps_full_order_once() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(708)).unwrap();
    assistant.begin_playback("held").unwrap();
    let first = vec![thought("first", "sig-1")];
    let full = vec![
        thought("first", "sig-1"),
        LlmContinuationBlock::Text("echo".into()),
        thought("last", "sig-2"),
    ];
    process(&assistant, end(708, full)).unwrap();
    process(&assistant, checkpoint(708, first)).unwrap();
    assistant
        .confirm_playback("held", &receipt("held", "echo"))
        .unwrap();
    assert_eq!(
        wire(&context)["messages"][0]["content"],
        json!([
            {"type":"thinking","thinking":"first","signature":"sig-1"},
            {"type":"text","text":"echo"},
            {"type":"thinking","thinking":"last","signature":"sig-2"}
        ])
    );
}

#[test]
fn review_rejected_receipt_can_be_corrected_without_losing_its_pending_identity() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(
        context.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&assistant, start(711)).unwrap();
    assistant.begin_playback("retryable").unwrap();
    process(
        &assistant,
        end(
            711,
            vec![
                LlmContinuationBlock::Text("valid".into()),
                thought("closed", "receipt-signature"),
            ],
        ),
    )
    .unwrap();
    let before = wire(&context);
    assert!(assistant
        .confirm_playback("retryable", &receipt("retryable", "wrong"))
        .is_err());
    assert_eq!(wire(&context), before);
    assistant
        .confirm_playback("retryable", &receipt("retryable", "valid"))
        .unwrap();
    assert_eq!(
        wire(&context)["messages"][0]["content"],
        json!([
            {"type":"text","text":"valid"},
            {"type":"thinking","thinking":"closed","signature":"receipt-signature"}
        ])
    );
}

#[test]
fn review_interruption_recovers_when_completed_system_history_precedes_bad_delivery() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    let call = FunctionCall {
        function_name: "effect".into(),
        tool_call_id: "overtaking-tool".into(),
        arguments: json!({}),
    };
    process(
        &assistant,
        attached(
            FrameData::FunctionCallsStarted(vec![call.clone()]),
            NativeLlmContinuation::new(
                712,
                LlmContinuationProvider::Anthropic,
                vec![
                    LlmContinuationBlock::Text("original".into()),
                    thought("closed", "overtaking-signature"),
                    LlmContinuationBlock::ToolCall(call),
                ],
            )
            .unwrap(),
        ),
    )
    .unwrap();
    process(&assistant, start(712)).unwrap();
    text(&assistant, "transformed");
    assert!(process(&assistant, Frame::new(FrameData::LlmResponseEnd)).is_err());
    process(&assistant, Frame::interruption())
        .expect("an invalid completed response must not permanently disable cancellation");
    process(&assistant, start(713)).expect("cancellation must allow the next response");
}

#[test]
fn review_bare_checkpoint_is_rejected_without_opening_a_response_scope() {
    let context = SharedContext::default();
    let assistant = LLMAssistantAggregator::new(context.clone(), Default::default());
    assert!(process(&assistant, Frame::new(FrameData::LlmContinuationCheckpoint)).is_err());
    assert!(context.snapshot().unwrap().messages.is_empty());
    process(&assistant, start(714)).unwrap();
    text(&assistant, "valid next response");
    process(
        &assistant,
        end(
            714,
            vec![
                LlmContinuationBlock::Text("valid next response".into()),
                thought("closed", "valid-signature"),
            ],
        ),
    )
    .unwrap();
    assert!(wire(&context).to_string().contains("valid-signature"));
}
