//! Independent contract checks for committed-turn frame production.
use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use std::time::Duration;

fn process(a: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    a.process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
fn pushes(output: &AggregationOutput) -> Vec<&DirectedFrame> {
    output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) => Some(frame),
            _ => None,
        })
        .collect()
}
fn turns(output: &AggregationOutput) -> Vec<&DirectedFrame> {
    pushes(output)
        .into_iter()
        .filter(|frame| matches!(frame.frame.data, FrameData::LlmContextAssistantTurn(_)))
        .collect()
}
fn assert_pair(output: &AggregationOutput, expected: &str) {
    let turns = turns(output);
    assert_eq!(turns.len(), 2, "exactly two outward siblings");
    assert_eq!(turns[0].direction, Direction::Downstream);
    assert_eq!(turns[1].direction, Direction::Upstream);
    assert_ne!(turns[0].frame.id, turns[1].frame.id);
    assert_eq!(turns[0].frame.broadcast_sibling_id, Some(turns[1].frame.id));
    assert_eq!(turns[1].frame.broadcast_sibling_id, Some(turns[0].frame.id));
    let FrameData::LlmContextAssistantTurn(down) = &turns[0].frame.data else {
        unreachable!()
    };
    let FrameData::LlmContextAssistantTurn(up) = &turns[1].frame.data else {
        unreachable!()
    };
    assert_eq!(down.text, expected);
    assert_eq!(up, down);
    assert!(!down.timestamp.is_empty());
}
fn timestamps(output: &AggregationOutput) -> Vec<&DirectedFrame> {
    pushes(output)
        .into_iter()
        .filter(|frame| {
            matches!(
                frame.frame.data,
                FrameData::LlmContextAssistantTimestamp { .. }
            )
        })
        .collect()
}
fn history(a: &LLMAssistantAggregator) -> Vec<(Role, String)> {
    a.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| {
            let Some(Content::Text(text)) = m.content else {
                panic!("text history")
            };
            (m.role, text)
        })
        .collect()
}
fn start_text(a: &LLMAssistantAggregator, text: &str) {
    process(a, FrameData::LlmResponseStart);
    process(a, FrameData::LlmText(TextFrame::llm(text)));
}

#[test]
fn review_committed_context_precedes_timestamp_and_linked_transcript_siblings() {
    let a = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    start_text(&a, "  ● keep ◐  these ○  ");
    let output = process(&a, FrameData::LlmResponseEnd);
    assert_eq!(history(&a), [(Role::Assistant, "● keep ◐  these ○".into())]);
    assert_pair(&output, "keep   these");
    let frames = pushes(&output);
    assert!(matches!(
        frames[0].frame.data,
        FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
    ));
    assert!(matches!(
        frames[1].frame.data,
        FrameData::LlmContextAssistantTimestamp { .. }
    ));
    assert_eq!(frames[1].direction, Direction::Downstream);
    assert_eq!(timestamps(&output).len(), 1);
    assert!(matches!(
        frames[2].frame.data,
        FrameData::LlmContextAssistantTurn(_)
    ));
}

#[test]
fn review_marker_only_commit_has_timestamp_but_no_empty_transcript() {
    let a = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    start_text(&a, "●◐○");
    let output = process(&a, FrameData::LlmResponseEnd);
    assert_eq!(history(&a), [(Role::Assistant, "●◐○".into())]);
    assert_eq!(timestamps(&output).len(), 1);
    assert!(turns(&output).is_empty());
    process(&a, FrameData::LlmResponseStart);
    let empty = process(&a, FrameData::LlmResponseEnd);
    assert!(timestamps(&empty).is_empty());
    assert!(turns(&empty).is_empty());
    assert!(empty.events.iter().any(
        |e| matches!(e, AggregationEvent::AssistantTurnStopped { text, .. } if text.is_empty())
    ));
}

#[test]
fn review_identical_real_turns_repeat_but_end_replay_does_not() {
    let a = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    let end = Frame::new(FrameData::LlmResponseEnd);
    start_text(&a, "same answer");
    let first = a.process_frame(end.clone(), Direction::Downstream).unwrap();
    assert_pair(&first, "same answer");
    assert!(turns(&a.process_frame(end, Direction::Downstream).unwrap()).is_empty());
    assert!(turns(&process(&a, FrameData::LlmResponseEnd)).is_empty());
    start_text(&a, "same answer");
    let second = process(&a, FrameData::LlmResponseEnd);
    assert_pair(&second, "same answer");
    assert_ne!(turns(&first)[0].frame.id, turns(&second)[0].frame.id);
    assert_eq!(history(&a).len(), 2);
}

#[test]
fn review_paired_typed_and_vad_interruptions_preserve_partial_notifications() {
    for trigger in [
        FrameData::InputText(TextFrame::new("next question")),
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        }),
    ] {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            Default::default(),
            Default::default(),
        );
        start_text(&pair.assistant(), "● accepted prefix");
        let effects = pair
            .user()
            .process_at(
                Frame::new(trigger),
                Direction::Downstream,
                Duration::from_secs(1),
            )
            .unwrap();
        assert_pair(&effects, "accepted prefix");
        assert_eq!(timestamps(&effects).len(), 1);
        let interruption = effects
            .actions
            .iter()
            .position(|a| {
                matches!(
                    a,
                    AggregationAction::Interrupt | AggregationAction::InterruptBeforeContext
                )
            })
            .unwrap();
        let notification = effects.actions.iter().position(|a| matches!(a, AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmContextAssistantTurn(_)))).unwrap();
        assert!(
            notification > interruption,
            "interrupt cannot purge its own partial-turn feedback"
        );
        assert_eq!(
            history(&pair.assistant())[0],
            (Role::Assistant, "● accepted prefix".into())
        );
        assert!(turns(&process(&pair.assistant(), FrameData::Interruption)).is_empty());
    }
}

#[test]
fn review_spoken_generated_text_cannot_impersonate_a_verified_receipt() {
    let a = LLMAssistantAggregator::new(
        SharedContext::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
    );
    start_text(&a, "never confirmed generated words");
    a.begin_playback("receipt-1").unwrap();
    assert!(turns(&process(&a, FrameData::LlmResponseEnd)).is_empty());
    assert!(history(&a).is_empty());
    let text = AggregatedTextFrame {
        text: TextFrame::new("display alias"),
        context_id: Some("receipt-1".into()),
        aggregated_by: "sentence".into(),
        raw_text: Some("  actual confirmed text  ".into()),
        will_be_spoken: true,
    };
    assert!(a
        .confirm_playback("wrong-context", std::slice::from_ref(&text))
        .is_err());
    assert!(history(&a).is_empty());
    let output = a
        .confirm_playback("receipt-1", std::slice::from_ref(&text))
        .unwrap();
    assert_pair(&output, "actual confirmed text");
    assert_eq!(timestamps(&output).len(), 1);
    assert_eq!(
        history(&a),
        [(Role::Assistant, "actual confirmed text".into())]
    );
    assert!(turns(&a.confirm_playback("receipt-1", &[text]).unwrap()).is_empty());
}

#[test]
fn review_canceled_and_non_appending_receipts_never_report_a_turn() {
    for canceled in [false, true] {
        let a = LLMAssistantAggregator::new(
            SharedContext::default(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        );
        start_text(&a, "unconfirmed");
        a.begin_playback("receipt-2").unwrap();
        if canceled {
            process(&a, FrameData::Interruption);
        }
        let mut text = TextFrame::new("unconfirmed");
        text.append_to_context = canceled;
        let text = AggregatedTextFrame {
            text,
            context_id: Some("receipt-2".into()),
            aggregated_by: "sentence".into(),
            raw_text: None,
            will_be_spoken: true,
        };
        let output = a.confirm_playback("receipt-2", &[text]).unwrap();
        assert!(turns(&output).is_empty());
        assert!(timestamps(&output).is_empty());
        assert!(history(&a).is_empty());
    }
}
