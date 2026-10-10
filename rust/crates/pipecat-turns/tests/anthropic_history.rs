//! Reconstructed receipt/history regression coverage after host replacement.
use pipecat_core::*;
use pipecat_services::{
    adapters::request_body, CompletionRequest, Context, Message, Provider, Role,
};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::sync::Arc;
fn attached(data: FrameData, history: NativeLlmContinuation) -> Frame {
    let mut frame = Frame::new(data);
    frame
        .set_native_llm_continuation(Arc::new(history))
        .unwrap();
    frame
}
fn start(id: u64) -> Frame {
    attached(
        FrameData::LlmResponseStart,
        NativeLlmContinuation::start(id, LlmContinuationProvider::Anthropic).unwrap(),
    )
}
fn thought() -> LlmContinuationBlock {
    LlmContinuationBlock::Thinking {
        text: "private".into(),
        signature: "exact+/signature==".into(),
    }
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
fn process(a: &LLMAssistantAggregator, f: Frame) -> Result<AggregationOutput, ProcessorError> {
    a.process_frame(f, Direction::Downstream)
}
fn text(a: &LLMAssistantAggregator, s: &str) {
    process(a, Frame::new(FrameData::LlmText(TextFrame::llm(s)))).unwrap();
}
fn body(c: &SharedContext) -> Value {
    request_body(
        Provider::Anthropic,
        &CompletionRequest::new("fixture", c.snapshot().unwrap()),
    )
    .unwrap()
}
fn receipt(id: &str, text: &str) -> AggregatedTextFrame {
    AggregatedTextFrame {
        text: TextFrame::llm(text),
        raw_text: Some(text.into()),
        context_id: Some(id.into()),
        aggregated_by: "fixture".into(),
        will_be_spoken: true,
    }
}
#[test]
fn duplicate_text_positions_and_rich_clone_preserve_exact_thought_signature() {
    let c = SharedContext::new(Context::new(vec![Message::text(Role::User, "q")]));
    let a = LLMAssistantAggregator::new(c.clone(), Default::default());
    process(&a, start(1)).unwrap();
    text(&a, "samesame");
    process(
        &a,
        end(
            1,
            vec![
                LlmContinuationBlock::Text("same".into()),
                thought(),
                LlmContinuationBlock::Text("same".into()),
            ],
        ),
    )
    .unwrap();
    assert_eq!(
        body(&c)["messages"][1]["content"],
        json!([{"type":"text","text":"same"},{"type":"thinking","thinking":"private","signature":"exact+/signature=="},{"type":"text","text":"same"}])
    );
    let rich = c.snapshot_rich().unwrap();
    let copy = pipecat_services::frame_context::context(&rich).unwrap();
    assert_eq!(
        request_body(
            Provider::Anthropic,
            &CompletionRequest::new("fixture", copy)
        )
        .unwrap(),
        body(&c)
    );
    assert!(request_body(
        Provider::OpenAi,
        &CompletionRequest::new("fixture", c.snapshot().unwrap())
    )
    .is_err());
}
#[test]
fn spoken_mode_keeps_signed_thought_independent_of_unheard_text_and_corrected_receipts() {
    let c = SharedContext::default();
    let a = LLMAssistantAggregator::new(
        c.clone(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    process(&a, start(2)).unwrap();
    a.begin_playback("r").unwrap();
    text(&a, "heard unheard");
    process(
        &a,
        end(
            2,
            vec![
                thought(),
                LlmContinuationBlock::Text("heard unheard".into()),
            ],
        ),
    )
    .unwrap();
    assert_eq!(
        body(&c)["messages"][0]["content"],
        json!([{"type":"thinking","thinking":"private","signature":"exact+/signature=="}])
    );
    let before = c.snapshot_rich().unwrap();
    assert!(a.confirm_playback("r", &[receipt("r", "wrong")]).is_err());
    assert_eq!(c.snapshot_rich().unwrap(), before);
    a.confirm_playback("r", &[receipt("r", "heard")]).unwrap();
    assert!(body(&c).to_string().contains("heard"));
    assert!(!body(&c).to_string().contains("unheard"));
}
#[test]
fn closed_prefix_interruption_keeps_heard_tail_but_never_unadmitted_tool() {
    let c = SharedContext::default();
    let a = LLMAssistantAggregator::new(c.clone(), Default::default());
    process(&a, start(3)).unwrap();
    let call = FunctionCall {
        function_name: "effect".into(),
        tool_call_id: "unadmitted".into(),
        arguments: json!({}),
    };
    process(
        &a,
        checkpoint(3, vec![thought(), LlmContinuationBlock::ToolCall(call)]),
    )
    .unwrap();
    text(&a, "heard tail");
    process(&a, Frame::interruption()).unwrap();
    let b = body(&c);
    assert!(b.to_string().contains("heard tail"));
    assert!(!b.to_string().contains("unadmitted"));
    process(&a, start(4)).unwrap();
}
#[test]
fn completion_validation_is_atomic_and_cancellation_recovers_after_system_overtake() {
    let c = SharedContext::default();
    let a = LLMAssistantAggregator::new(c.clone(), Default::default());
    let call = FunctionCall {
        function_name: "effect".into(),
        tool_call_id: "call".into(),
        arguments: json!({}),
    };
    let history = NativeLlmContinuation::new(
        5,
        LlmContinuationProvider::Anthropic,
        vec![
            LlmContinuationBlock::Text("original".into()),
            thought(),
            LlmContinuationBlock::ToolCall(call.clone()),
        ],
    )
    .unwrap();
    process(
        &a,
        attached(FrameData::FunctionCallsStarted(vec![call]), history),
    )
    .unwrap();
    process(&a, start(5)).unwrap();
    text(&a, "transformed");
    let before = c.snapshot_rich().unwrap();
    assert!(process(&a, Frame::new(FrameData::LlmResponseEnd)).is_err());
    assert_eq!(c.snapshot_rich().unwrap(), before);
    process(&a, Frame::interruption()).unwrap();
    assert_eq!(c.snapshot_rich().unwrap(), before);
    process(&a, start(6)).unwrap();
}
#[test]
fn stale_and_conflicting_checkpoints_do_not_overwrite_valid_history() {
    let c = SharedContext::default();
    let a = LLMAssistantAggregator::new(c.clone(), Default::default());
    process(&a, start(7)).unwrap();
    process(&a, checkpoint(7, vec![thought()])).unwrap();
    let before = c.snapshot_rich().unwrap();
    let changed = LlmContinuationBlock::Thinking {
        text: "private".into(),
        signature: "different".into(),
    };
    assert!(process(&a, checkpoint(7, vec![changed])).is_err());
    assert_eq!(c.snapshot_rich().unwrap(), before);
    process(&a, Frame::interruption()).unwrap();
    process(&a, start(8)).unwrap();
    process(&a, checkpoint(7, vec![thought()])).unwrap();
    assert_eq!(c.snapshot_rich().unwrap(), before);
    assert!(process(&a, Frame::new(FrameData::LlmContinuationCheckpoint)).is_err());
}
#[test]
fn interior_whitespace_transformation_is_rejected_without_signature_publication() {
    let c = SharedContext::default();
    let a = LLMAssistantAggregator::new(c.clone(), Default::default());
    process(&a, start(9)).unwrap();
    text(&a, "ab");
    let before = c.snapshot_rich().unwrap();
    assert!(process(
        &a,
        end(9, vec![LlmContinuationBlock::Text("a b".into()), thought()])
    )
    .is_err());
    assert_eq!(c.snapshot_rich().unwrap(), before);
}
