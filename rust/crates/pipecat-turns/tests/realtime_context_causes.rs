use pipecat_core::*;
use pipecat_services::Context;
use pipecat_turns::*;
use std::time::Duration;
fn pair(mode: RealtimeServiceMode) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        mode,
    )
    .unwrap()
}
fn context(output: AggregationOutput) -> Frame {
    output
        .actions
        .into_iter()
        .find_map(|action| match action {
            AggregationAction::Push(frame)
                if matches!(
                    frame.frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                ) =>
            {
                Some(frame.frame)
            }
            _ => None,
        })
        .expect("context")
}
fn append(run_llm: bool) -> Frame {
    Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "explicit")],
        run_llm,
    }))
}
#[test]
fn realtime_no_run_append_has_one_bounded_explicit_intent() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let frame = context(
        pair.user()
            .process_at(append(false), Direction::Downstream, Duration::ZERO)
            .unwrap(),
    );
    let cause = frame.native_realtime_context_cause().unwrap().unwrap();
    assert_eq!(
        cause.intent(),
        RealtimeContextIntent::Append { start: 0, end: 1 }
    );
    assert!(!cause.run_llm());
    assert_eq!(cause.sequence(), 1);
    assert!(!frame.is_realtime_context_update());
    assert!(!frame.interruptible);
    let run = context(
        pair.user()
            .process_at(
                Frame::new(FrameData::LlmRun),
                Direction::Downstream,
                Duration::ZERO,
            )
            .unwrap(),
    );
    let cause = run.native_realtime_context_cause().unwrap().unwrap();
    assert_eq!(cause.intent(), RealtimeContextIntent::Run);
    assert!(cause.run_llm());
    assert_eq!(cause.sequence(), 2);
}
#[test]
fn identical_explicit_typed_input_remains_two_operations() {
    let pair = pair(RealtimeServiceMode::Enabled);
    for index in 0..2 {
        let frame = Frame::new(FrameData::InputText(TextFrame::new("same")));
        let context = context(
            pair.user()
                .process_at(frame, Direction::Downstream, Duration::ZERO)
                .unwrap(),
        );
        let cause = context.native_realtime_context_cause().unwrap().unwrap();
        assert_eq!(
            cause.intent(),
            RealtimeContextIntent::Append {
                start: index,
                end: index + 1
            }
        );
        assert_eq!(cause.sequence(), index as u64 + 1);
        assert!(cause.run_llm());
    }
}
#[test]
fn cascade_no_run_emission_and_ordinary_contexts_are_unchanged() {
    for mode in [RealtimeServiceMode::Auto, RealtimeServiceMode::Disabled] {
        let pair = pair(mode);
        assert!(pair
            .user()
            .process_at(append(false), Direction::Downstream, Duration::ZERO)
            .unwrap()
            .actions
            .is_empty());
        let frame = context(
            pair.user()
                .process_at(
                    Frame::new(FrameData::LlmRun),
                    Direction::Downstream,
                    Duration::ZERO,
                )
                .unwrap(),
        );
        assert!(frame.interruptible);
        assert!(frame.native_realtime_context_cause().unwrap().is_none());
        assert!(serde_json::to_vec(&frame).is_ok());
    }
}
#[test]
fn replacement_has_explicit_intent_even_without_inference() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let frame = context(
        pair.user()
            .process_at(
                Frame::new(FrameData::LlmMessagesUpdate(MessagesFrame {
                    messages: vec![ChatMessage::new(ChatRole::User, "replacement")],
                    run_llm: false,
                })),
                Direction::Downstream,
                Duration::ZERO,
            )
            .unwrap(),
    );
    let cause = frame.native_realtime_context_cause().unwrap().unwrap();
    assert_eq!(cause.intent(), RealtimeContextIntent::Replace);
    assert!(!cause.run_llm());
}

#[test]
fn oversized_edit_preserves_truthful_history_and_blocks_silent_retry() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let frame = Frame::new(FrameData::LlmMessagesAppend(MessagesFrame {
        messages: vec![ChatMessage::new(ChatRole::User, "x".repeat(1024 * 1024))],
        run_llm: false,
    }));
    let error = pair
        .user()
        .process_at(frame.clone(), Direction::Downstream, Duration::ZERO)
        .unwrap_err();
    assert!(error
        .message
        .contains("local history retained as-is but edit not delivered"));
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    assert!(pair
        .user()
        .process_at(frame, Direction::Downstream, Duration::ZERO)
        .is_err());
    assert_eq!(pair.context().snapshot().unwrap().messages.len(), 1);
    let error = pair
        .user()
        .process_at(
            Frame::new(FrameData::LlmRun),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap_err();
    assert!(error.message.contains("new pair and provider session"));
    let repaired = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    let frame = context(
        repaired
            .user()
            .process_at(append(false), Direction::Downstream, Duration::ZERO)
            .unwrap(),
    );
    assert_eq!(
        frame
            .native_realtime_context_cause()
            .unwrap()
            .unwrap()
            .sequence(),
        1
    );
}
