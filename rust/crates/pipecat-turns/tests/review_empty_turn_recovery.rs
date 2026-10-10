//! Independent recovery review: only delivered speech belongs in the retry context.
use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use std::time::Duration;

fn config() -> UserAggregatorConfig {
    UserAggregatorConfig {
        turn: UserTurnConfig {
            speech_timeout: Duration::from_millis(10),
            stop_timeout: Duration::from_millis(20),
            ..Default::default()
        },
        ..Default::default()
    }
}
fn send(user: &LLMUserAggregator, data: FrameData, now: u64) -> AggregationOutput {
    user.process_at(
        Frame::new(data),
        Direction::Downstream,
        Duration::from_millis(now),
    )
    .unwrap()
}
fn empty(user: &LLMUserAggregator, now: u64) -> AggregationOutput {
    let vad = VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    };
    send(user, FrameData::VadUserStartedSpeaking(vad), now);
    send(user, FrameData::VadUserStoppedSpeaking(vad), now + 1);
    user.advance(Duration::from_millis(now + 50)).unwrap()
}
fn inferences(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|action| matches!(action, AggregationAction::Push(frame)
        if frame.direction == Direction::Downstream && matches!(frame.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
}
#[test]
fn review_recovery_keeps_heard_prefix_and_never_adds_unheard_generated_text() {
    for heard in [false, true] {
        let pair = LLMContextAggregatorPair::new(
            Context::default(),
            config(),
            AssistantAggregatorConfig {
                text_mode: AssistantTextMode::Spoken,
            },
        );
        let assistant = pair.assistant();
        assistant
            .process_frame(
                Frame::new(FrameData::LlmResponseStart),
                Direction::Downstream,
            )
            .unwrap();
        assistant
            .process_frame(
                Frame::new(FrameData::LlmText(TextFrame::llm("generated but unheard"))),
                Direction::Downstream,
            )
            .unwrap();
        for (text, spoken) in [("heard prefix", heard), ("unheard suffix", false)] {
            let mut frame = Frame::new(FrameData::TtsText(AggregatedTextFrame {
                text: TextFrame::llm(text),
                aggregated_by: "word".into(),
                context_id: Some("voice".into()),
                raw_text: None,
                will_be_spoken: true,
            }));
            frame.metadata.insert("spoken".into(), spoken.into());
            assistant
                .process_frame(frame, Direction::Downstream)
                .unwrap();
        }
        let output = empty(&pair.user(), 0);
        assert_eq!(inferences(&output), 1);
        let messages = pair.user().context().snapshot().unwrap().messages;
        assert_eq!(messages.last().unwrap().role, Role::Developer);
        assert_eq!(
            messages.last().unwrap().content,
            Some(Content::Text(
                DEFAULT_EMPTY_USER_TURN_INTERRUPTED_PROMPT.into()
            ))
        );
        let delivered: Vec<_> = messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .collect();
        assert_eq!(delivered.len(), usize::from(heard));
        if heard {
            assert_eq!(
                delivered[0].content,
                Some(Content::Text("heard prefix".into()))
            );
        }
        assert!(!serde_json::to_string(&messages)
            .unwrap()
            .contains("unheard"));
    }
}
#[test]
fn review_replacing_context_or_replaying_speech_boundaries_does_not_reset_budget() {
    let user = LLMUserAggregator::new(SharedContext::default(), config());
    assert_eq!(inferences(&empty(&user, 0)), 1);
    send(
        &user,
        FrameData::LlmMessagesUpdate(MessagesFrame {
            messages: vec![],
            run_llm: false,
        }),
        51,
    );
    assert_eq!(inferences(&empty(&user, 100)), 0);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    send(
        &user,
        FrameData::Transcription(TranscriptionFrame {
            text: "real speech".into(),
            finalized: true,
            ..Default::default()
        }),
        200,
    );
    user.advance(Duration::from_millis(250)).unwrap();
    assert_eq!(inferences(&empty(&user, 300)), 1);
    let messages = user.context().snapshot().unwrap().messages;
    assert_eq!(messages.len(), 2);
    assert_eq!(messages[0].role, Role::User);
    assert_eq!(messages[1].role, Role::Developer);
}
#[test]
fn review_invalid_realtime_metadata_cannot_consume_recovery_or_mutate_history() {
    let user = LLMUserAggregator::new(SharedContext::default(), config());
    let metadata = FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "realtime".into(),
            user_turn_strategies: None,
        },
        is_realtime_service: true,
    });
    assert!(user
        .process_at(
            Frame::new(metadata),
            Direction::Downstream,
            Duration::from_secs(3600)
        )
        .is_err());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(inferences(&empty(&user, 0)), 1);
}
