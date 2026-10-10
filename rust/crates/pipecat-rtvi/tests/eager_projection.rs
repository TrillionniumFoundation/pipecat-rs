use pipecat_core::*;
use pipecat_rtvi::*;
#[test]
fn provisional_context_is_never_projected_to_client_transcript() {
    let observer = RtviObserver::new(Default::default(), None).unwrap();
    let mut context = LlmContextData {
        messages: vec![],
        tools: ToolSet::NotGiven,
        tool_choice: None,
    };
    context.messages.push(ContextMessage {
        role: ChatRole::User,
        content: MessageContent::Text("provisional secret".into()),
        name: None,
        tool_call_id: None,
        tool_calls: vec![],
        extra: Default::default(),
    });
    let frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context: context.clone(),
        speculation: true,
    }));
    assert!(observer
        .observe(&frame, Observation::new("user", Direction::Downstream))
        .unwrap()
        .is_empty());
    let final_frame = Frame::new(FrameData::LlmContextDetailed(DetailedLlmContextFrame {
        context,
        speculation: false,
    }));
    assert_eq!(
        observer
            .observe(
                &final_frame,
                Observation::new("user", Direction::Downstream)
            )
            .unwrap()
            .len(),
        1
    );
}
