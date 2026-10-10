//! Source-mapped start/mute policies and native integration invariants.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;
use serde_json::{json, Value};
use std::time::Duration;
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn data(kind: &str, text: &str) -> FrameData {
    match kind {
        "vad_start" => FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        }),
        "vad_stop" => FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        }),
        "final" => FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            finalized: true,
            ..Default::default()
        }),
        "interim" => FrameData::InterimTranscription(TranscriptionFrame {
            text: text.into(),
            ..Default::default()
        }),
        "user_start" => FrameData::UserStartedSpeaking,
        "user_stop" => FrameData::UserStoppedSpeaking,
        "proposed_start" => FrameData::ProposedUserStartedSpeaking,
        "proposed_stop" => FrameData::ProposedUserStoppedSpeaking,
        "bot_start" => FrameData::BotStartedSpeaking,
        "bot_stop" => FrameData::BotStoppedSpeaking,
        "bot_speaking" => FrameData::BotSpeaking,
        "user_speaking" => FrameData::UserSpeaking,
        "interruption" => FrameData::Interruption,
        "error" => FrameData::Error(ErrorFrame::new("opening TTS failed")),
        other => panic!("unknown fixture frame {other}"),
    }
}
fn strategies(items: Vec<UserTurnStartStrategy>) -> UserTurnStartStrategies {
    UserTurnStartStrategies::new(items).unwrap()
}
fn wake(single: bool) -> UserTurnStartStrategy {
    UserTurnStartStrategy::wake_phrase(vec!["hey pipecat".into()], ms(100), single).unwrap()
}
fn configured(
    starts: Vec<UserTurnStartStrategy>,
    mute: Vec<UserMuteStrategy>,
) -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                start_strategies: Some(strategies(starts)),
                speech_timeout: ms(10),
                ..Default::default()
            },
            mute_strategies: mute,
            ..Default::default()
        },
    )
}
fn at(user: &LLMUserAggregator, kind: &str, text: &str, time: u64) -> AggregationOutput {
    user.process_at(
        Frame::new(data(kind, text)),
        Direction::Downstream,
        ms(time),
    )
    .unwrap()
}
fn started(output: &AggregationOutput) -> bool {
    output.events.contains(&AggregationEvent::UserTurnStarted)
}
fn texts(user: &LLMUserAggregator) -> Vec<String> {
    user.context()
        .snapshot()
        .unwrap()
        .messages
        .into_iter()
        .filter_map(|m| {
            if m.role == Role::User {
                match m.content {
                    Some(Content::Text(t)) => Some(t),
                    _ => None,
                }
            } else {
                None
            }
        })
        .collect()
}
fn context_count(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|action| matches!(action,
    AggregationAction::Push(d) if matches!(d.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
}

#[test]
fn source_mapped_start_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/user_strategies.json")).unwrap();
    assert_eq!(
        fixture["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    for case in fixture["start_cases"].as_array().unwrap() {
        let strategy = match case["strategy"].as_str().unwrap() {
            "vad" => UserTurnStartStrategy::vad(),
            "transcription" => {
                UserTurnStartStrategy::transcription(case["use_interim"].as_bool().unwrap())
            }
            "external" => UserTurnStartStrategy::external(),
            "min_words" => UserTurnStartStrategy::min_words(
                case["min_words"].as_u64().unwrap() as usize,
                case["use_interim"].as_bool().unwrap(),
            ),
            "wake" => UserTurnStartStrategy::wake_phrase(
                vec!["hey pipecat".into()],
                ms(case["timeout_ms"].as_u64().unwrap()),
                case["single_activation"].as_bool().unwrap(),
            )
            .unwrap(),
            other => panic!("unknown strategy {other}"),
        };
        let mut chain = strategies(vec![strategy]);
        for step in case["steps"].as_array().unwrap() {
            let now = ms(step["ms"].as_u64().unwrap_or(0));
            let output = if step["frame"] == "advance" {
                UserStartStrategyOutput {
                    events: chain.advance(now),
                    ..Default::default()
                }
            } else {
                chain.process(
                    &data(
                        step["frame"].as_str().unwrap(),
                        step["text"].as_str().unwrap_or(""),
                    ),
                    now,
                )
            };
            assert_eq!(
                output.start.is_some(),
                step["start"].as_bool().unwrap(),
                "{} {step}",
                case["name"]
            );
            assert_eq!(
                output.reset_aggregation,
                step["reset"].as_bool().unwrap_or(false),
                "{} {step}",
                case["name"]
            );
            if let Some(params) = output.start {
                assert_eq!(
                    params.enable_interruptions,
                    step["interrupt"].as_bool().unwrap_or(true)
                );
                assert_eq!(
                    params.enable_user_speaking_frames,
                    step["emit"].as_bool().unwrap_or(true)
                );
            }
            match step["event"].as_str() {
                Some("detected") => assert_eq!(
                    output.events,
                    vec![UserStartStrategyEvent::WakePhraseDetected {
                        strategy_index: 0,
                        phrase: "hey pipecat".into()
                    }]
                ),
                Some("timeout") => assert_eq!(
                    output.events,
                    vec![UserStartStrategyEvent::WakePhraseTimeout { strategy_index: 0 }]
                ),
                None => assert!(output.events.is_empty(), "{output:?}"),
                other => panic!("unknown event {other:?}"),
            }
            if step["notify_start"] == true {
                chain.handle_user_turn_started(now);
            }
        }
    }
}
#[test]
fn source_mapped_mute_fixtures() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/user_strategies.json")).unwrap();
    for case in fixture["mute_cases"].as_array().unwrap() {
        let policy = match case["strategy"].as_str().unwrap() {
            "always" => UserMuteStrategy::Always,
            "first" => UserMuteStrategy::FirstSpeech,
            "until" => UserMuteStrategy::UntilFirstBotComplete,
            other => panic!("{other}"),
        };
        let mut policies = UserMuteStrategies::new(vec![policy]);
        for step in case["steps"].as_array().unwrap() {
            assert_eq!(
                policies
                    .process(&data(step[0].as_str().unwrap(), ""))
                    .unwrap(),
                step[1].as_bool().unwrap(),
                "{case} {step}"
            );
        }
        policies.reset();
        assert_eq!(
            policies.is_muted(),
            policy == UserMuteStrategy::UntilFirstBotComplete
        );
    }
}
#[test]
fn ordered_gate_resets_aggregation_and_keeps_only_matching_frame() {
    let user = configured(
        vec![
            wake(false),
            UserTurnStartStrategy::vad(),
            UserTurnStartStrategy::transcription(true),
        ],
        vec![],
    );
    assert!(!started(&at(&user, "vad_start", "", 0)));
    assert!(!started(&at(
        &user,
        "final",
        "irrelevant background speech",
        1
    )));
    at(&user, "vad_stop", "", 2);
    at(&user, "final", "hey", 3);
    assert!(started(&at(&user, "final", "pipecat help me", 4)));
    let output = user.advance(ms(14)).unwrap();
    assert_eq!(context_count(&output), 1);
    assert_eq!(texts(&user), ["pipecat help me"]);
    assert_eq!(user.deadline().unwrap(), Some(ms(104)));
    assert_eq!(
        user.advance(ms(104)).unwrap().events,
        [AggregationEvent::UserStartStrategy(
            UserStartStrategyEvent::WakePhraseTimeout { strategy_index: 0 }
        )]
    );
    assert!(!started(&at(
        &user,
        "final",
        "unrelated after timeout",
        105
    )));
    user.process_at(Frame::end(), Direction::Downstream, ms(110))
        .unwrap();
    assert_eq!(texts(&user), ["pipecat help me"]);
}
#[test]
fn gate_blocks_resolution_but_consumes_proposal_when_external_is_configured() {
    let user = configured(vec![wake(false), UserTurnStartStrategy::external()], vec![]);
    let out = at(&user, "proposed_start", "", 0);
    assert!(out.actions.is_empty());
    assert!(!started(&out));
    let user = configured(vec![UserTurnStartStrategy::vad()], vec![]);
    let frame = Frame::new(FrameData::ProposedUserStartedSpeaking);
    let out = user
        .process_at(frame.clone(), Direction::Upstream, ms(0))
        .unwrap();
    assert_eq!(
        out.actions,
        [AggregationAction::Push(DirectedFrame::new(
            frame,
            Direction::Upstream
        ))]
    );
}
#[test]
fn proposals_resolve_once_and_announced_frames_retain_identity_direction() {
    let user = configured(
        vec![UserTurnStartStrategy::external().with_interruptions(false)],
        vec![],
    );
    let out = at(&user, "proposed_start", "", 0);
    assert!(started(&out));
    assert!(
        matches!(&out.actions[..], [AggregationAction::Broadcast(f)] if f.data == FrameData::UserStartedSpeaking)
    );
    assert!(at(&user, "proposed_start", "", 1).actions.is_empty());
    at(&user, "final", "first", 2);
    at(&user, "user_stop", "", 3);
    let frame = Frame::new(FrameData::UserStartedSpeaking);
    let out = user
        .process_at(frame.clone(), Direction::Upstream, ms(4))
        .unwrap();
    assert!(started(&out));
    assert_eq!(
        out.actions,
        [AggregationAction::Push(DirectedFrame::new(
            frame,
            Direction::Upstream
        ))]
    );
}
#[test]
fn min_words_does_not_accumulate_rejected_fragments_and_preserves_live_vad() {
    let user = configured(vec![UserTurnStartStrategy::min_words(3, true)], vec![]);
    at(&user, "bot_start", "", 0);
    at(&user, "vad_start", "", 1);
    at(&user, "final", "one", 2);
    at(&user, "final", "two", 3);
    assert!(started(&at(&user, "final", "one two three", 4)));
    assert!(user.advance(ms(1000)).unwrap().events.is_empty());
    at(&user, "final", "continued", 1001);
    at(&user, "vad_stop", "", 1002);
    let out = user.advance(ms(1012)).unwrap();
    assert_eq!(context_count(&out), 1);
    assert_eq!(texts(&user), ["one two three continued"]);
}
#[test]
fn min_words_reset_does_not_short_circuit_later_fallback() {
    let user = configured(
        vec![
            UserTurnStartStrategy::min_words(3, true),
            UserTurnStartStrategy::transcription(true),
        ],
        vec![],
    );
    at(&user, "bot_start", "", 0);
    assert!(started(&at(&user, "final", "one", 1)));
    user.advance(ms(11)).unwrap();
    assert!(texts(&user).is_empty());
}
#[test]
fn wake_matching_unicode_punctuation_optional_spaces_and_bounded_tail() {
    let mut chain = strategies(vec![UserTurnStartStrategy::wake_phrase(
        vec!["héy 猫".into()],
        ms(10),
        false,
    )
    .unwrap()]);
    assert!(chain
        .process(&data("final", "HÉY,猫!"), ms(0))
        .start
        .is_some());
    chain.reset();
    assert!(chain
        .process(&data("final", "notHÉY猫"), ms(0))
        .start
        .is_none());
    let mut chain = strategies(vec![wake(false)]);
    chain.process(&data("final", "hey"), ms(0));
    chain.process(&data("final", &"猫".repeat(300)), ms(1));
    assert!(chain
        .process(&data("final", "pipecat"), ms(2))
        .start
        .is_none());
    assert!(chain
        .process(
            &data("final", &format!("{} HeyPipecat!", "猫".repeat(400))),
            ms(3)
        )
        .start
        .is_some());
    chain.cleanup();
    assert_eq!(chain.deadline(), None);
    assert!(chain
        .process(&data("final", "pipecat"), ms(4))
        .start
        .is_none());
    assert!(UserTurnStartStrategy::wake_phrase(vec![], ms(10), false).is_err());
    assert!(UserTurnStartStrategy::wake_phrase(vec!["hey".into()], Duration::ZERO, false).is_err());
    assert!(UserTurnStartStrategies::new(vec![UserTurnStartStrategy::vad(); 65]).is_err());
}
fn call(id: &str) -> FunctionCall {
    FunctionCall {
        tool_call_id: id.into(),
        function_name: "lookup".into(),
        arguments: json!({"id":id}),
    }
}
fn result(id: &str, final_result: bool) -> FrameData {
    FrameData::FunctionCallResult(FunctionCallResultFrame {
        tool_call_id: id.into(),
        function_name: "lookup".into(),
        arguments: json!({"id":id}),
        result: json!("ok"),
        run_llm: Some(true),
        is_final: final_result,
        error: None,
    })
}
#[test]
fn composed_mutes_observe_all_frames_and_require_matching_final_tool_result() {
    let mut policies = UserMuteStrategies::new(vec![
        UserMuteStrategy::FirstSpeech,
        UserMuteStrategy::FunctionCall,
    ]);
    assert!(policies.process(&FrameData::BotStartedSpeaking).unwrap());
    assert!(policies
        .process(&FrameData::FunctionCallsStarted(vec![call("a"), call("b")]))
        .unwrap());
    assert!(policies.process(&FrameData::BotStoppedSpeaking).unwrap());
    assert!(policies.process(&result("a", false)).unwrap());
    let mut wrong = call("a");
    wrong.function_name = "other".into();
    assert!(policies
        .process(&FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: wrong.function_name,
            tool_call_id: wrong.tool_call_id,
            run_llm: false,
        }))
        .unwrap());
    assert!(policies.process(&result("unknown", true)).unwrap());
    assert!(policies.process(&result("a", true)).unwrap());
    assert!(!policies.process(&result("b", true)).unwrap());
    assert!(!policies
        .process(&FrameData::FunctionCallsStarted(vec![call("a")]))
        .unwrap());
    assert!(!policies.process(&FrameData::BotStartedSpeaking).unwrap());
    policies.cleanup();
    assert!(policies
        .process(&FrameData::FunctionCallsStarted(vec![call("a")]))
        .unwrap());
}
#[test]
fn malformed_or_excess_tool_declarations_are_atomic() {
    let mut policies = UserMuteStrategies::new(vec![UserMuteStrategy::FunctionCall]);
    let mut bad = call("bad");
    bad.arguments = Value::Null;
    assert!(policies
        .process(&FrameData::FunctionCallsStarted(vec![call("a"), bad]))
        .is_err());
    assert!(!policies.is_muted());
    assert!(policies
        .process(&FrameData::FunctionCallsStarted(
            (0..512).map(|n| call(&n.to_string())).collect()
        ))
        .unwrap());
    assert!(policies
        .process(&FrameData::FunctionCallsStarted(vec![call("overflow")]))
        .is_err());
    for n in 0..512 {
        policies.process(&result(&n.to_string(), true)).unwrap();
    }
    assert!(!policies.is_muted());
}
#[test]
fn mute_transition_order_lifecycle_and_manual_layer() {
    let user = configured(
        vec![UserTurnStartStrategy::vad()],
        vec![UserMuteStrategy::UntilFirstBotComplete],
    );
    let start = Frame::start();
    let out = user
        .process_at(start.clone(), Direction::Downstream, ms(0))
        .unwrap();
    assert_eq!(
        out.actions,
        [AggregationAction::Push(DirectedFrame::new(
            start,
            Direction::Downstream
        ))]
    );
    assert!(!user.is_muted().unwrap());
    let incoming = Frame::new(FrameData::BotStartedSpeaking);
    let out = user
        .process_at(incoming.clone(), Direction::Upstream, ms(1))
        .unwrap();
    assert!(
        matches!(&out.actions[..], [AggregationAction::Broadcast(m), AggregationAction::Push(original)] if m.data == FrameData::UserMuteStarted && original == &DirectedFrame::new(incoming, Direction::Upstream))
    );
    assert!(at(&user, "proposed_start", "", 2).actions.is_empty());
    assert!(at(&user, "final", "muted speech", 3).actions.is_empty());
    user.process_at(
        Frame::new(FrameData::SttMute(true)),
        Direction::Downstream,
        ms(4),
    )
    .unwrap();
    at(&user, "bot_stop", "", 5);
    assert!(user.is_muted().unwrap());
    user.process_at(
        Frame::new(FrameData::SttMute(false)),
        Direction::Downstream,
        ms(6),
    )
    .unwrap();
    assert!(!user.is_muted().unwrap());
    user.process_at(Frame::end(), Direction::Upstream, ms(7))
        .unwrap();
    assert!(user.deadline().unwrap().is_none());
    assert!(texts(&user).is_empty());
}
#[test]
fn muting_preserves_accepted_context_without_running_llm_and_keeps_idle_guard() {
    let user = configured(
        vec![UserTurnStartStrategy::vad()],
        vec![UserMuteStrategy::Always],
    );
    at(&user, "vad_start", "", 0);
    at(&user, "final", "accepted before mute", 1);
    let out = at(&user, "bot_start", "", 2);
    assert_eq!(context_count(&out), 0);
    assert_eq!(texts(&user), ["accepted before mute"]);
    assert!(out.events.iter().any(|event| matches!(
        event,
        AggregationEvent::UserTurnStopped {
            reason: TurnStopReason::Muted,
            ..
        }
    )));
    assert!(!user.waiting_for_user().unwrap());
    at(&user, "final", "must not enter context", 3);
    at(&user, "bot_stop", "", 4);
    assert!(user.waiting_for_user().unwrap());
    assert_eq!(texts(&user), ["accepted before mute"]);
}

#[test]
fn wake_casefold_and_python_word_classes_are_explicit() {
    for (phrase, text) in [
        ("σ", "ς"),
        ("i", "İ"),
        ("I", "ı"),
        ("s", "ſ"),
        ("k", "K"),
        ("hey cat", "hey\u{1c}cat"),
        ("cat", "cat\u{0345}"),
        ("cat", "cat\u{200c}"),
    ] {
        let mut chain = strategies(vec![UserTurnStartStrategy::wake_phrase(
            vec![phrase.into()],
            ms(10),
            false,
        )
        .unwrap()]);
        assert!(
            chain.process(&data("final", text), ms(0)).start.is_some(),
            "phrase={phrase:?}, text={text:?}"
        );
    }
}
