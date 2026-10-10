//! Independent no-wire-projection checks for speculative native frames.
use pipecat_core::*;
use pipecat_rtvi::{Observation, RtviObserver, RtviObserverParams};
use serde_json::json;
fn observe(frame: &Frame) -> Vec<pipecat_rtvi::wire::Envelope> {
    RtviObserver::new(RtviObserverParams::default(), None)
        .unwrap()
        .observe(frame, Observation::new("review", Direction::Downstream))
        .unwrap()
}
#[test]
fn review_provisional_context_never_becomes_user_llm_wire_text() {
    for native in [false, true] {
        let owner = SpeculationOwner::new();
        let mut frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
            context: LlmContextData {
                messages: vec![ChatMessage::new(ChatRole::User, "uncommitted prediction").into()],
                tools: ToolSet::NotGiven,
                tool_choice: None,
            },
            speculation: true,
        }));
        if native {
            frame.set_native_speculation(owner.capability()).unwrap();
        }
        assert!(
            observe(&frame).is_empty(),
            "provisional transcript leaked through observer projection"
        );
    }
}
#[test]
fn review_unconsumed_effect_capability_never_becomes_tool_wire_event() {
    let owner = SpeculationOwner::new();
    let mut frame = Frame::new(FrameData::FunctionCallsStarted(vec![FunctionCall {
        function_name: "effect".into(),
        tool_call_id: "e".into(),
        arguments: json!({}),
    }]));
    frame.set_native_speculation(owner.capability()).unwrap();
    assert!(
        observe(&frame).is_empty(),
        "unadmitted tool declaration leaked through observer projection"
    );
}
#[test]
fn review_ordinary_committed_context_still_has_wire_event() {
    let frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: LlmContextData {
            messages: vec![ChatMessage::new(ChatRole::User, "committed text").into()],
            tools: ToolSet::NotGiven,
            tool_choice: None,
        },
        speculation: false,
    }));
    assert_eq!(observe(&frame).len(), 1);
}
