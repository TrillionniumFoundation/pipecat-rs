//! Failed bounded cause creation must not hide or duplicate an explicit local edit.
use pipecat_core::*;
use pipecat_services::{Content, Context};
use pipecat_turns::*;
use std::time::Duration;
fn pair(context: Context) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        context,
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap()
}
fn apply(
    pair: &LLMContextAggregatorPair,
    data: FrameData,
) -> Result<AggregationOutput, ProcessorError> {
    pair.user()
        .process_at(Frame::new(data), Direction::Downstream, Duration::ZERO)
}
#[test]
fn review_oversized_edit_is_retained_once_and_poisoned_pair_never_silently_skips_or_retries_it() {
    let pair = pair(Context::default());
    let large = "x".repeat(1024 * 1024);
    let edit = FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, large.clone())],
        run_llm: false,
    });
    let error = apply(&pair, edit.clone()).unwrap_err();
    let message = error.to_string();
    assert!(
        message.contains("context") || message.contains("realtime"),
        "{message}"
    );
    let retained = pair.context().snapshot().unwrap();
    assert_eq!(retained.messages.len(), 1);
    assert!(matches!(&retained.messages[0].content, Some(Content::Text(text)) if text == &large));
    for retry in [
        FrameData::LlmRun,
        edit,
        FrameData::InputText(TextFrame::new("new input")),
    ] {
        assert!(
            apply(&pair, retry).is_err(),
            "failed explicit cause stream silently resumed"
        );
        assert_eq!(
            pair.context().snapshot().unwrap(),
            retained,
            "retry duplicated or mutated retained history after stream failure"
        );
    }
    let repaired = super_pair_with_small_context();
    let output = apply(&repaired, FrameData::LlmRun).unwrap();
    let causes: Vec<_> = output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) => frame
                .frame
                .native_realtime_context_cause()
                .unwrap()
                .cloned(),
            _ => None,
        })
        .collect();
    assert_eq!(causes.len(), 1);
    assert_eq!(causes[0].sequence(), 1);
    assert_eq!(causes[0].intent(), RealtimeContextIntent::Run);
}
fn super_pair_with_small_context() -> LLMContextAggregatorPair {
    let pair = pair(Context::default());
    // Seed repaired retained history through SharedContext, before explicit admission.
    pair.context()
        .append(pipecat_services::Message::text(
            pipecat_services::Role::User,
            "repaired retained history",
        ))
        .unwrap();
    pair
}
