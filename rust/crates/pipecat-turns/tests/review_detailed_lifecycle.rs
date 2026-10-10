//! Rich lifecycle frames must preserve the compact-frame aggregation contract.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;
use std::time::Duration;

#[test]
fn detailed_end_commits_pending_user_final_and_closes_the_aggregator() {
    let user = LLMUserAggregator::new(SharedContext::default(), UserAggregatorConfig::default());
    user.process_at(
        Frame::new(FrameData::Transcription(TranscriptionFrame {
            text: "accepted final".into(),
            finalized: true,
            ..Default::default()
        })),
        Direction::Downstream,
        Duration::ZERO,
    )
    .unwrap();
    user.process_at(
        Frame::new(FrameData::EndDetailed(ReasonFrame::default())),
        Direction::Downstream,
        Duration::from_millis(1),
    )
    .unwrap();
    let messages = user.context().snapshot().unwrap().messages;
    assert_eq!(
        messages.len(),
        1,
        "EndDetailed must flush a final transcript exactly as End does"
    );
    assert_eq!(
        messages[0].content,
        Some(Content::Text("accepted final".into()))
    );
    assert!(user
        .process_at(
            Frame::new(FrameData::InputText(TextFrame::new("late"))),
            Direction::Downstream,
            Duration::from_millis(2)
        )
        .unwrap()
        .actions
        .is_empty());
}
#[test]
fn detailed_end_commits_delivered_assistant_prefix_and_closes_the_aggregator() {
    let assistant = LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig::default(),
    );
    for data in [
        FrameData::LlmResponseStart,
        FrameData::LlmText(TextFrame::llm("delivered")),
        FrameData::EndDetailed(ReasonFrame::default()),
    ] {
        assistant
            .process_frame(Frame::new(data), Direction::Downstream)
            .unwrap();
    }
    let messages = assistant.context().snapshot().unwrap().messages;
    assert_eq!(
        messages.len(),
        1,
        "EndDetailed must flush delivered text exactly as End does"
    );
    assert_eq!(messages[0].role, Role::Assistant);
    assert_eq!(messages[0].content, Some(Content::Text("delivered".into())));
    assert!(assistant
        .process_frame(
            Frame::new(FrameData::LlmResponseStart),
            Direction::Downstream
        )
        .unwrap()
        .actions
        .is_empty());
}
#[test]
fn detailed_cancel_prevents_future_user_inference() {
    let user = LLMUserAggregator::new(SharedContext::default(), UserAggregatorConfig::default());
    user.process_at(
        Frame::new(FrameData::CancelDetailed(ReasonFrame::default())),
        Direction::Downstream,
        Duration::ZERO,
    )
    .unwrap();
    let late = user
        .process_at(
            Frame::new(FrameData::InputText(TextFrame::new("must be ignored"))),
            Direction::Downstream,
            Duration::from_millis(1),
        )
        .unwrap();
    assert!(
        late.actions.is_empty(),
        "CancelDetailed must close the aggregator exactly as Cancel does"
    );
    assert!(user.context().snapshot().unwrap().messages.is_empty());
}
