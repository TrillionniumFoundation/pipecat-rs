//! Completion markers affect transcript events without changing context history.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;

fn assistant() -> LLMAssistantAggregator {
    LLMAssistantAggregator::new(SharedContext::default(), Default::default())
}
fn process(a: &LLMAssistantAggregator, data: FrameData) -> AggregationOutput {
    a.process_frame(Frame::new(data), Direction::Downstream)
        .unwrap()
}
fn marker(value: &str, immediate: bool) -> FrameData {
    FrameData::LlmMarker(LlmMarkerFrame {
        marker: value.into(),
        append_to_context_immediately: immediate,
    })
}
fn end_text(output: &AggregationOutput) -> (&str, bool) {
    output
        .events
        .iter()
        .find_map(|e| match e {
            AggregationEvent::AssistantTurnStopped { text, interrupted } => {
                Some((text.as_str(), *interrupted))
            }
            _ => None,
        })
        .expect("assistant stopped event")
}
fn history(a: &LLMAssistantAggregator) -> Vec<String> {
    a.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|message| {
            assert_eq!(message.role, Role::Assistant);
            let Some(Content::Text(text)) = message.content else {
                panic!("text history")
            };
            text
        })
        .collect()
}

#[test]
fn default_markers_are_removed_from_event_only_even_without_sideband() {
    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    process(
        &a,
        FrameData::LlmText(TextFrame::llm("  ● Hello ◐ friend ○  ")),
    );
    let out = process(&a, FrameData::LlmResponseEnd);
    assert_eq!(end_text(&out), ("Hello  friend", false));
    assert_eq!(history(&a), ["● Hello ◐ friend ○"]);
}

#[test]
fn custom_marker_frame_cleans_event_and_keeps_required_context_marker() {
    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    process(&a, marker("<DONE>", false));
    process(&a, FrameData::LlmText(TextFrame::llm("hello <DONE> world")));
    let out = process(&a, FrameData::LlmResponseEnd);
    assert_eq!(end_text(&out), ("hello  world", false));
    assert_eq!(history(&a), ["<DONE> hello <DONE> world"]);
    process(&a, FrameData::LlmResponseStart);
    process(&a, FrameData::LlmText(TextFrame::llm("<DONE> now literal")));
    assert_eq!(
        end_text(&process(&a, FrameData::LlmResponseEnd)).0,
        "<DONE> now literal",
        "custom markers cannot leak between turns"
    );
}

#[test]
fn no_marker_preserves_existing_aggregation_normalization() {
    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    process(&a, FrameData::LlmText(TextFrame::llm("  keep leading ")));
    process(&a, FrameData::LlmText(TextFrame::llm("and trailing  ")));
    let out = process(&a, FrameData::LlmResponseEnd);
    assert_eq!(end_text(&out).0, "keep leading and trailing");
    assert_eq!(history(&a), ["keep leading and trailing"]);
}

#[test]
fn immediate_context_marker_is_seen_for_this_turn_and_never_duplicated_into_history() {
    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    process(&a, marker("<WAIT>", true));
    process(
        &a,
        FrameData::LlmText(TextFrame::llm("<WAIT> custom literal")),
    );
    let out = process(&a, FrameData::LlmResponseEnd);
    assert_eq!(end_text(&out).0, "custom literal");
    assert_eq!(history(&a), ["<WAIT>", "<WAIT> custom literal"]);
    process(&a, FrameData::LlmResponseStart);
    process(&a, marker("<WAIT>", true));
    assert_eq!(end_text(&process(&a, FrameData::LlmResponseEnd)).0, "");
    assert_eq!(history(&a).last().unwrap(), "<WAIT>");
}

#[test]
fn interruption_projects_current_turn_then_resets_custom_markers() {
    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    process(&a, marker("<CUT>", false));
    process(&a, FrameData::LlmText(TextFrame::llm("delivered")));
    let out = process(&a, FrameData::Interruption);
    assert_eq!(end_text(&out), ("delivered", true));
    assert_eq!(history(&a), ["<CUT> delivered"]);
    process(&a, FrameData::LlmResponseStart);
    process(
        &a,
        FrameData::LlmText(TextFrame::llm("<CUT> is ordinary now")),
    );
    assert_eq!(
        end_text(&process(&a, FrameData::LlmResponseEnd)).0,
        "<CUT> is ordinary now"
    );
}

#[test]
fn every_terminal_projects_events_and_preserves_context() {
    for terminal in [
        FrameData::End(EndFrame { reason: None }),
        FrameData::EndDetailed(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame { reason: None }),
        FrameData::CancelDetailed(Default::default()),
    ] {
        let a = assistant();
        process(&a, FrameData::LlmResponseStart);
        process(&a, marker("CUSTOM", false));
        process(&a, FrameData::LlmText(TextFrame::llm("done")));
        let out = process(&a, terminal);
        assert_eq!(end_text(&out).0, "done");
        assert_eq!(history(&a), ["CUSTOM done"]);
        assert!(process(&a, FrameData::LlmText(TextFrame::llm("late")))
            .events
            .is_empty());
    }
}

#[test]
fn marker_cardinality_and_bytes_are_bounded_before_context_mutation() {
    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    for index in 0..64 {
        process(&a, marker(&format!("<{index}>"), true));
    }
    let before = history(&a);
    assert!(a
        .process_frame(Frame::new(marker("excess", true)), Direction::Downstream)
        .is_err());
    assert_eq!(history(&a), before);
    process(&a, marker("<0>", false)); // A duplicate uses no further budget.
    process(&a, FrameData::LlmResponseEnd);
    process(&a, FrameData::LlmResponseStart);
    process(&a, marker("fresh", true)); // The next turn gets a new budget.

    let a = assistant();
    process(&a, FrameData::LlmResponseStart);
    for index in 0..16 {
        process(&a, marker(&format!("{index:02}{}", "x".repeat(254)), true));
    }
    let before = history(&a);
    for invalid in ["a".into(), "x".repeat(257), String::new()] {
        assert!(a
            .process_frame(Frame::new(marker(&invalid, true)), Direction::Downstream)
            .is_err());
        assert_eq!(history(&a), before);
    }
}

#[test]
fn event_projection_preserves_opaque_anthropic_and_gemini_history_exactly() {
    use pipecat_services::{adapters::request_body, CompletionRequest, Provider};
    use serde_json::json;
    use std::sync::Arc;
    for (provider, wire_provider) in [
        (LlmContinuationProvider::Anthropic, Provider::Anthropic),
        (LlmContinuationProvider::Gemini, Provider::Gemini),
    ] {
        let a = assistant();
        let mut start = Frame::new(FrameData::LlmResponseStart);
        start
            .set_native_llm_continuation(Arc::new(
                NativeLlmContinuation::start(77, provider).unwrap(),
            ))
            .unwrap();
        a.process_frame(start, Direction::Downstream).unwrap();
        process(&a, FrameData::LlmText(TextFrame::llm("● public ◐")));
        let blocks = if provider == LlmContinuationProvider::Anthropic {
            vec![
                LlmContinuationBlock::Thinking {
                    text: "○ opaque thought".into(),
                    signature: "signature+/==".into(),
                },
                LlmContinuationBlock::Text("● public ◐".into()),
            ]
        } else {
            vec![
                LlmContinuationBlock::GeminiText {
                    text: "○ opaque thought".into(),
                    thought: true,
                    signature: Some("thought+/==".into()),
                },
                LlmContinuationBlock::GeminiText {
                    text: "● public ◐".into(),
                    thought: false,
                    signature: Some("text+/==".into()),
                },
            ]
        };
        let mut end = Frame::new(FrameData::LlmResponseEnd);
        end.set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::new(77, provider, blocks).unwrap(),
        ))
        .unwrap();
        let out = a.process_frame(end, Direction::Downstream).unwrap();
        assert_eq!(end_text(&out).0, "public");
        let wire = request_body(
            wire_provider,
            &CompletionRequest::new("fixture", a.context().snapshot().unwrap()),
        )
        .unwrap();
        if provider == LlmContinuationProvider::Anthropic {
            assert_eq!(
                wire["messages"][0]["content"],
                json!([
                    {"type":"thinking","thinking":"○ opaque thought","signature":"signature+/=="},
                    {"type":"text","text":"● public ◐"}
                ])
            );
        } else {
            assert_eq!(
                wire["contents"][0]["parts"],
                json!([
                    {"text":"○ opaque thought","thought":true,"thoughtSignature":"thought+/=="},
                    {"text":"● public ◐","thoughtSignature":"text+/=="}
                ])
            );
        }
    }
}
