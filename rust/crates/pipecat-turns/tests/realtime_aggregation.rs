//! Universal aggregator source af9f9da9: realtime trailing writes and mode policy.
use pipecat_core::*;
use pipecat_services::{Content, Context, Role};
use pipecat_turns::*;
use serde_json::json;
use std::time::Duration;
fn ms(value: u64) -> Duration {
    Duration::from_millis(value)
}
fn pair(mode: RealtimeServiceMode) -> LLMContextAggregatorPair {
    LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        Default::default(),
        mode,
    )
    .unwrap()
}
fn user(pair: &LLMContextAggregatorPair, data: FrameData, now: u64) -> AggregationOutput {
    pair.user()
        .process_at(Frame::new(data), Direction::Upstream, ms(now))
        .unwrap()
}
fn assistant(pair: &LLMContextAggregatorPair, data: FrameData, now: u64) -> AggregationOutput {
    pair.assistant()
        .process_at(Frame::new(data), Direction::Downstream, ms(now))
        .unwrap()
}
fn transcript(text: &str) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn metadata(realtime: bool, external: Option<bool>) -> FrameData {
    FrameData::LlmServiceMetadata(LlmServiceMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "test-realtime".into(),
            user_turn_strategies: external.map(|enable_interruptions| UserTurnStrategies {
                start: Some(vec![UserTurnStrategy::External {
                    enable_interruptions,
                }]),
                stop: Some(vec![UserTurnStrategy::External {
                    enable_interruptions,
                }]),
            }),
        },
        is_realtime_service: realtime,
    })
}
fn latency(value: f64) -> FrameData {
    FrameData::SttMetadata(SttMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "internal-stt".into(),
            user_turn_strategies: None,
        },
        ttfs_p99_latency: value,
    })
}
fn messages(pair: &LLMContextAggregatorPair) -> Vec<(Role, String)> {
    pair.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .map(|m| {
            (
                m.role,
                match m.content {
                    Some(Content::Text(text)) => text,
                    _ => String::new(),
                },
            )
        })
        .collect()
}
fn contexts(output: &AggregationOutput) -> usize {
    output
        .actions
        .iter()
        .filter(|action| match action {
            AggregationAction::RealtimeUserContext(_) => true,
            AggregationAction::Push(directed) => matches!(
                directed.frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            ),
            _ => false,
        })
        .count()
}
fn upstream(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|a| matches!(a, AggregationAction::Push(f) if f.direction == Direction::Upstream && matches!(f.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
}
#[test]
fn default_auto_stays_cascade_until_metadata_and_shared_flag_is_immediate() {
    let pair = pair(RealtimeServiceMode::Auto);
    assert!(!pair.user().realtime_service_enabled().unwrap());
    assert!(user(&pair, transcript("cascade"), 0)
        .events
        .contains(&AggregationEvent::UserTurnStarted));
    assert_eq!(contexts(&pair.user().advance(ms(600)).unwrap()), 1);
    assistant(&pair, metadata(true, None), 700);
    assert!(pair.user().realtime_service_enabled().unwrap());
    assert!(pair.assistant().realtime_service_enabled().unwrap());
    assert!(user(&pair, transcript("realtime"), 800).events.is_empty());
    assert_eq!(contexts(&pair.user().advance(ms(10_000)).unwrap()), 0);
    assistant(&pair, FrameData::LlmResponseStart, 10_000);
    assistant(&pair, FrameData::LlmResponseEnd, 10_010);
    assert_eq!(
        messages(&pair),
        [
            (Role::User, "cascade".into()),
            (Role::User, "realtime".into())
        ]
    );
}
#[test]
fn explicit_on_keeps_default_vad_without_transcript_starts_or_recovery() {
    let pair = pair(RealtimeServiceMode::Enabled);
    assert!(user(&pair, transcript("hello"), 0).events.is_empty());
    let start = user(
        &pair,
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        }),
        1,
    );
    assert!(start.events.contains(&AggregationEvent::UserTurnStarted));
    user(
        &pair,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        }),
        10,
    );
    let stop = pair.user().advance(ms(610)).unwrap();
    assert!(stop
        .events
        .iter()
        .any(|e| matches!(e, AggregationEvent::RealtimeUserTurnStopped { .. })));
    assert_eq!(contexts(&stop), 0);
    assert!(messages(&pair).is_empty());
}
#[test]
fn metadata_external_stops_without_transcript_and_rebroadcast_remutates() {
    let pair = pair(RealtimeServiceMode::Auto);
    for turn in 0..2 {
        let now = turn * 100;
        user(&pair, metadata(true, Some(false)), now);
        let start = user(&pair, FrameData::ProposedUserStartedSpeaking, now + 1);
        assert!(!start
            .actions
            .iter()
            .any(|a| matches!(a, AggregationAction::Interrupt)));
        assert!(start.events.contains(&AggregationEvent::UserTurnStarted));
        let stop = user(&pair, FrameData::ProposedUserStoppedSpeaking, now + 2);
        assert!(stop
            .events
            .iter()
            .any(|e| matches!(e, AggregationEvent::RealtimeUserTurnStopped { .. })));
        assert!(messages(&pair).is_empty());
    }
}
#[test]
fn explicit_off_adopts_recommendation_but_preserves_transcript_wait() {
    let pair = pair(RealtimeServiceMode::Disabled);
    user(&pair, metadata(true, Some(false)), 0);
    assistant(&pair, metadata(true, Some(false)), 0);
    assert!(!pair.user().realtime_service_enabled().unwrap());
    assert!(!pair.assistant().realtime_service_enabled().unwrap());
    user(&pair, FrameData::ProposedUserStartedSpeaking, 1);
    assert!(user(&pair, FrameData::ProposedUserStoppedSpeaking, 2)
        .events
        .is_empty());
    let final_text = user(&pair, transcript("waited"), 3);
    assert_eq!(contexts(&final_text), 0);
    assert_eq!(contexts(&pair.user().advance(ms(503)).unwrap()), 1);
    assert_eq!(messages(&pair), [(Role::User, "waited".into())]);
}
#[test]
fn explicit_native_policies_win_even_over_unsupported_recommendations() {
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                start_strategies: Some(
                    UserTurnStartStrategies::new(vec![
                        UserTurnStartStrategy::vad(),
                        UserTurnStartStrategy::transcription(true),
                    ])
                    .unwrap(),
                ),
                stop_strategies: Some(
                    UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(
                        ms(50),
                        true,
                    )])
                    .unwrap(),
                ),
                ..Default::default()
            },
            ..Default::default()
        },
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    user(&pair, metadata(true, Some(true)), 0);
    assert!(!user(&pair, FrameData::ProposedUserStartedSpeaking, 1)
        .events
        .contains(&AggregationEvent::UserTurnStarted));
    assert!(!user(&pair, transcript("no transcript start"), 2)
        .events
        .contains(&AggregationEvent::UserTurnStarted));
    assert!(user(
        &pair,
        FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2
        }),
        3
    )
    .events
    .contains(&AggregationEvent::UserTurnStarted));
    user(
        &pair,
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        }),
        4,
    );
    assert!(pair
        .user()
        .advance(ms(54))
        .unwrap()
        .events
        .iter()
        .any(|e| matches!(e, AggregationEvent::RealtimeUserTurnStopped { .. })));
}
#[test]
fn non_realtime_switch_keeps_mode_but_does_not_remutate_its_recommendation() {
    let pair = pair(RealtimeServiceMode::Auto);
    user(&pair, metadata(true, Some(false)), 0);
    user(&pair, metadata(false, Some(false)), 1);
    assert!(pair.user().realtime_service_enabled().unwrap());
    user(&pair, FrameData::ProposedUserStartedSpeaking, 2);
    assert!(user(&pair, FrameData::ProposedUserStoppedSpeaking, 3)
        .events
        .is_empty());
    user(&pair, transcript("arrived"), 4);
    assert!(pair
        .user()
        .advance(ms(504))
        .unwrap()
        .events
        .iter()
        .any(|e| matches!(e, AggregationEvent::RealtimeUserTurnStopped { .. })));
}
#[test]
fn default_ttfs_defers_and_end_flushes_user_before_assistant() {
    let pair = pair(RealtimeServiceMode::Enabled);
    user(&pair, transcript("hello"), 0);
    assistant(&pair, FrameData::LlmResponseStart, 10);
    assert_eq!(pair.user().deadline().unwrap(), Some(ms(1010)));
    user(&pair, transcript("late part"), 20);
    assistant(&pair, FrameData::LlmText(TextFrame::llm("reply")), 30);
    assert!(messages(&pair).is_empty());
    let end = assistant(&pair, FrameData::LlmResponseEnd, 40);
    assert!(matches!(
        end.actions.first(),
        Some(AggregationAction::RealtimeUserContext(_))
    ));
    assert_eq!(
        messages(&pair),
        [
            (Role::User, "hello late part".into()),
            (Role::Assistant, "reply".into())
        ]
    );
    assert_eq!(pair.user().deadline().unwrap(), None);
    assert_eq!(contexts(&pair.user().advance(ms(10_000)).unwrap()), 0);
}
#[test]
fn ttfs_flush_and_end_straggler_are_once_each() {
    let pair = pair(RealtimeServiceMode::Enabled);
    user(&pair, latency(0.02), 0);
    assistant(&pair, FrameData::LlmResponseStart, 10);
    user(&pair, transcript("early"), 11);
    assert_eq!(contexts(&pair.user().advance(ms(29)).unwrap()), 0);
    assert_eq!(contexts(&pair.user().advance(ms(30)).unwrap()), 1);
    assert_eq!(contexts(&pair.user().advance(ms(31)).unwrap()), 0);
    user(&pair, transcript("straggler"), 32);
    assistant(&pair, FrameData::LlmText(TextFrame::llm("reply")), 33);
    assistant(&pair, FrameData::LlmResponseEnd, 34);
    assert_eq!(
        messages(&pair),
        [
            (Role::User, "early".into()),
            (Role::User, "straggler".into()),
            (Role::Assistant, "reply".into())
        ]
    );
}
#[test]
fn repeated_response_start_supersedes_only_deadline_not_transcript() {
    let pair = pair(RealtimeServiceMode::Enabled);
    user(&pair, latency(0.1), 0);
    user(&pair, transcript("retained"), 1);
    assistant(&pair, FrameData::LlmResponseStart, 10);
    assistant(&pair, FrameData::LlmResponseStart, 90);
    assert_eq!(pair.user().deadline().unwrap(), Some(ms(190)));
    assert_eq!(contexts(&pair.user().advance(ms(110)).unwrap()), 0);
    assert_eq!(contexts(&pair.user().advance(ms(190)).unwrap()), 1);
    assert_eq!(messages(&pair), [(Role::User, "retained".into())]);
}
#[test]
fn response_frame_replay_cannot_postpone_deadline() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let start = Frame::new(FrameData::LlmFullResponseStart(Default::default()));
    pair.assistant()
        .process_at(start.clone(), Direction::Downstream, ms(0))
        .unwrap();
    pair.assistant()
        .process_at(start, Direction::Downstream, ms(500))
        .unwrap();
    assert_eq!(pair.user().deadline().unwrap(), Some(ms(1000)));
}
#[test]
fn empty_audio_turn_does_not_recover_or_invent_user_content() {
    let pair = pair(RealtimeServiceMode::Enabled);
    user(&pair, metadata(true, Some(true)), 0);
    assistant(&pair, FrameData::LlmResponseStart, 1);
    user(&pair, FrameData::ProposedUserStartedSpeaking, 2);
    user(&pair, FrameData::ProposedUserStoppedSpeaking, 3);
    assistant(&pair, FrameData::LlmResponseEnd, 4);
    assert!(messages(&pair).is_empty());
}
#[test]
fn late_transcripts_after_no_external_turn_are_retained_and_replay_is_filtered() {
    let pair = pair(RealtimeServiceMode::Enabled);
    let transcript = Frame::new(transcript("hello"));
    assistant(&pair, FrameData::LlmResponseStart, 0);
    pair.user()
        .process_at(transcript.clone(), Direction::Upstream, ms(1))
        .unwrap();
    pair.user()
        .process_at(transcript, Direction::Upstream, ms(2))
        .unwrap();
    assistant(&pair, FrameData::LlmResponseEnd, 3);
    assert_eq!(messages(&pair), [(Role::User, "hello".into())]);
}
#[test]
fn terminal_boundaries_flush_trailing_text_and_remove_handoff() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        let pair = pair(RealtimeServiceMode::Enabled);
        user(&pair, transcript("trailing"), 0);
        assistant(&pair, FrameData::LlmResponseStart, 1);
        let output = user(&pair, terminal.clone(), 2);
        assert_eq!(contexts(&output), 1);
        assert!(!output
            .events
            .iter()
            .any(|e| matches!(e, AggregationEvent::RealtimeUserTurnStopped { .. })));
        assistant(&pair, terminal, 2);
        user(&pair, transcript("ignored"), 3);
        assert_eq!(contexts(&pair.user().advance(ms(10_000)).unwrap()), 0);
        assert_eq!(messages(&pair), [(Role::User, "trailing".into())]);
    }
}
#[test]
fn interruption_flushes_user_before_already_delivered_assistant() {
    let pair = pair(RealtimeServiceMode::Enabled);
    user(&pair, transcript("question"), 0);
    assistant(&pair, FrameData::LlmResponseStart, 1);
    assistant(&pair, FrameData::LlmText(TextFrame::llm("partial")), 2);
    let out = assistant(&pair, FrameData::Interruption, 3);
    assert!(out
        .events
        .contains(&AggregationEvent::AssistantTurnStopped {
            text: "partial".into(),
            interrupted: true
        }));
    assert_eq!(
        messages(&pair),
        [
            (Role::User, "question".into()),
            (Role::Assistant, "partial".into())
        ]
    );
    assert_eq!(pair.user().deadline().unwrap(), None);
}
#[test]
fn proposed_turn_broadcast_is_resolved_once() {
    let pair = pair(RealtimeServiceMode::Auto);
    user(&pair, metadata(true, Some(true)), 0);
    for proposal in [
        FrameData::ProposedUserStartedSpeaking,
        FrameData::ProposedUserStoppedSpeaking,
    ] {
        assert!(assistant(&pair, proposal, 1).actions.is_empty());
    }
}
#[test]
fn standalone_halves_and_eager_auto_fail_closed() {
    let user = LLMUserAggregator::new(SharedContext::default(), Default::default());
    assert!(user
        .process_at(Frame::new(metadata(true, None)), Direction::Upstream, ms(0))
        .unwrap_err()
        .message
        .contains("Pair"));
    let assistant = LLMAssistantAggregator::new(SharedContext::default(), Default::default());
    assert!(assistant
        .process_frame(Frame::new(metadata(true, None)), Direction::Downstream)
        .is_err());
    let config = UserAggregatorConfig {
        eager: Some(EagerUserTurnConfig::default()),
        ..Default::default()
    };
    assert!(LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        config.clone(),
        Default::default(),
        RealtimeServiceMode::Enabled
    )
    .is_err());
    let pair = LLMContextAggregatorPair::new(Context::default(), config, Default::default());
    assert!(pair
        .user()
        .process_at(Frame::new(metadata(true, None)), Direction::Upstream, ms(0))
        .is_err());
    assert!(!pair.user().realtime_service_enabled().unwrap());
}
#[test]
fn invalid_latency_or_descriptor_is_atomic() {
    let pair = pair(RealtimeServiceMode::Auto);
    for value in [f64::NAN, f64::INFINITY, -0.01, 86_401.0] {
        assert!(pair
            .user()
            .process_at(Frame::new(latency(value)), Direction::Upstream, ms(0))
            .is_err());
    }
    let mut unsupported = metadata(true, Some(true));
    if let FrameData::LlmServiceMetadata(metadata) = &mut unsupported {
        metadata.base.user_turn_strategies.as_mut().unwrap().stop =
            Some(vec![UserTurnStrategy::Eager {
                speculation_timeout: 1.0,
            }]);
    }
    assert!(pair
        .user()
        .process_at(Frame::new(unsupported), Direction::Upstream, ms(0))
        .is_err());
    assert!(!pair.user().realtime_service_enabled().unwrap());
}
#[test]
fn spoken_mode_requires_receipt_and_tool_continuation_waits_for_playback() {
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        Context::default(),
        Default::default(),
        AssistantAggregatorConfig {
            text_mode: AssistantTextMode::Spoken,
        },
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    user(&pair, transcript("question"), 0);
    assistant(&pair, FrameData::LlmResponseStart, 1);
    pair.assistant().begin_playback("audio").unwrap();
    assistant(
        &pair,
        FrameData::TtsText(AggregatedTextFrame {
            context_id: Some("audio".into()),
            text: TextFrame::new("generated"),
            aggregated_by: "test".into(),
            raw_text: None,
            will_be_spoken: true,
        }),
        2,
    );
    assistant(
        &pair,
        FrameData::FunctionCallInProgress(FunctionCallInProgressFrame {
            function_name: "lookup".into(),
            tool_call_id: "call".into(),
            arguments: json!({}),
            cancel_on_interruption: false,
            group_id: None,
        }),
        3,
    );
    assert_eq!(
        upstream(&assistant(
            &pair,
            FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: "lookup".into(),
                tool_call_id: "call".into(),
                arguments: json!({}),
                result: json!("done"),
                run_llm: Some(true),
                is_final: true,
                error: None
            }),
            4
        )),
        0
    );
    assert_eq!(upstream(&assistant(&pair, FrameData::LlmResponseEnd, 5)), 0);
    assert!(!messages(&pair).iter().any(|(_, text)| text == "generated"));
    let receipt = pair
        .assistant()
        .confirm_playback(
            "audio",
            &[AggregatedTextFrame {
                context_id: Some("audio".into()),
                text: TextFrame::new("heard"),
                aggregated_by: "test".into(),
                raw_text: None,
                will_be_spoken: true,
            }],
        )
        .unwrap();
    assert_eq!(upstream(&receipt), 1);
    assert!(messages(&pair)
        .iter()
        .any(|(role, text)| *role == Role::Assistant && text == "heard"));
    assert_eq!(
        upstream(&assistant(&pair, FrameData::BotStoppedSpeaking, 6)),
        0
    );
}

#[test]
fn realtime_buffer_rejects_oversize_before_appending_and_retains_accepted_text() {
    let pair = pair(RealtimeServiceMode::Enabled);
    user(&pair, transcript("accepted"), 0);
    assert!(pair
        .user()
        .process_at(
            Frame::new(transcript(&"x".repeat(1024 * 1024))),
            Direction::Upstream,
            ms(1)
        )
        .unwrap_err()
        .message
        .contains("1048576"));
    let output = assistant(&pair, FrameData::LlmResponseEnd, 2);
    assert_eq!(contexts(&output), 1);
    assert_eq!(messages(&pair), [(Role::User, "accepted".into())]);
    let AggregationAction::RealtimeUserContext(update) = &output.actions[0] else {
        panic!("user route");
    };
    assert_eq!(
        update.frame.metadata.get("realtime_context_update"),
        Some(&true.into())
    );
}
#[test]
fn failed_context_projection_retains_transcript_without_partial_user_commit() {
    use pipecat_services::{ContentPart, Message};
    let context = Context::new(vec![Message {
        role: Role::System,
        content: Some(Content::Parts(vec![ContentPart::InlineData {
            data: "AA==".into(),
            mime_type: "image/png".into(),
        }])),
        name: None,
        tool_calls: vec![],
        tool_call_id: None,
    }]);
    let pair = LLMContextAggregatorPair::with_realtime_mode(
        context.clone(),
        Default::default(),
        Default::default(),
        RealtimeServiceMode::Enabled,
    )
    .unwrap();
    user(&pair, transcript("retained"), 0);
    assistant(&pair, FrameData::LlmResponseStart, 1);
    assert!(pair
        .assistant()
        .process_at(
            Frame::new(FrameData::LlmResponseEnd),
            Direction::Downstream,
            ms(2)
        )
        .is_err());
    assert_eq!(pair.context().snapshot().unwrap(), context);
    pair.context().replace(Context::default()).unwrap();
    assistant(&pair, FrameData::LlmResponseEnd, 3);
    assert_eq!(messages(&pair), [(Role::User, "retained".into())]);
}
