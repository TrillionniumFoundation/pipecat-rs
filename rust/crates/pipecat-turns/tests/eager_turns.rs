//! Source contracts: pinned test_eager_user_turn_strategies.py and match policies.
use pipecat_core::*;
use pipecat_turns::*;
use std::time::Duration;
fn text(data: fn(TranscriptionFrame) -> FrameData, text: &str) -> Frame {
    Frame::new(data(TranscriptionFrame {
        text: text.into(),
        user_id: "u".into(),
        timestamp: "0".into(),
        language: None,
        result: None,
        finalized: true,
    }))
}
fn user() -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(EagerUserTurnConfig::default()),
            empty_user_turn: None,
            ..Default::default()
        },
    )
}
fn feed(user: &LLMUserAggregator, frame: Frame, ms: u64) -> AggregationOutput {
    user.process_at(frame, Direction::Downstream, Duration::from_millis(ms))
        .unwrap()
}
fn pushed(output: &AggregationOutput) -> Vec<&Frame> {
    output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(d) => Some(&d.frame),
            AggregationAction::Broadcast(f) => Some(f),
            _ => None,
        })
        .collect()
}
fn contexts(output: &AggregationOutput) -> Vec<&Frame> {
    pushed(output)
        .into_iter()
        .filter(|f| {
            matches!(
                f.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .collect()
}
fn begin(user: &LLMUserAggregator, eager: &str) -> Frame {
    feed(user, Frame::new(FrameData::ProposedUserStartedSpeaking), 0);
    contexts(&feed(user, text(FrameData::EagerTranscription, eager), 10))[0].clone()
}
#[test]
fn source_bound_match_and_segmented_turn_traces() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/eager_contract.json")).unwrap();
    assert_eq!(
        fixture["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    let policy = |case: &serde_json::Value| match case["policy"].as_str().unwrap() {
        "exact" => EagerMatchPolicy::Exact,
        "normalized" => EagerMatchPolicy::Normalized,
        other => panic!("unknown source policy {other}"),
    };
    for case in fixture["match_cases"].as_array().unwrap() {
        assert_eq!(
            policy(case).matches(
                case["eager"].as_str().unwrap(),
                case["final"].as_str().unwrap()
            ),
            case["matches"].as_bool().unwrap(),
            "{case}"
        );
    }
    for case in fixture["turn_cases"].as_array().unwrap() {
        let user = LLMUserAggregator::new(
            SharedContext::default(),
            UserAggregatorConfig {
                eager: Some(
                    EagerUserTurnConfig::new(policy(case), Duration::from_secs(5), 1024).unwrap(),
                ),
                empty_user_turn: None,
                ..Default::default()
            },
        );
        feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 0);
        for segment in case["prefix"].as_array().unwrap() {
            feed(
                &user,
                text(FrameData::Transcription, segment.as_str().unwrap()),
                1,
            );
        }
        let output = feed(
            &user,
            text(
                FrameData::EagerTranscription,
                case["eager"].as_str().unwrap(),
            ),
            2,
        );
        let frame = contexts(&output)[0];
        let FrameData::LlmContextDetailed(context) = &frame.data else {
            panic!("detailed prediction")
        };
        assert_eq!(
            context.context.messages.last().unwrap().content,
            MessageContent::Text(case["provisional"].as_str().unwrap().into()),
            "{case}"
        );
        let capability = frame.native_speculation().unwrap().clone();
        for segment in case["final"].as_array().unwrap() {
            feed(
                &user,
                text(FrameData::Transcription, segment.as_str().unwrap()),
                3,
            );
        }
        let stopped = feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 4);
        let confirms = case["confirms"].as_bool().unwrap();
        assert_eq!(
            capability.status(),
            if confirms {
                SpeculationStatus::Confirmed
            } else {
                SpeculationStatus::Cancelled
            },
            "{case}"
        );
        assert_eq!(contexts(&stopped).len(), usize::from(!confirms), "{case}");
        assert_eq!(
            user.context()
                .snapshot()
                .unwrap()
                .messages
                .last()
                .unwrap()
                .content,
            Some(pipecat_services::Content::Text(
                case["committed"].as_str().unwrap().into()
            )),
            "{case}"
        );
    }
    assert_eq!(
        EagerEndOfTurn::new(true)
            .with_match_policy(EagerMatchPolicy::Exact)
            .recommended_eager_config()
            .unwrap()
            .match_policy(),
        EagerMatchPolicy::Exact
    );
}
#[test]
fn raw_transcript_budget_withdraws_without_poisoning_the_next_turn() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(
                EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::from_secs(5), 6)
                    .unwrap(),
            ),
            empty_user_turn: None,
            ..Default::default()
        },
    );
    let prediction = begin(&user, "a");
    let capability = prediction.native_speculation().unwrap();
    let overflow = feed(&user, text(FrameData::Transcription, "1234567"), 20);
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
    assert!(pushed(&overflow)
        .iter()
        .any(|frame| matches!(frame.data, FrameData::EagerEndOfTurnCancel)));
    let stopped = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        30,
    );
    assert_eq!(contexts(&stopped).len(), 1);
    feed(
        &user,
        Frame::new(FrameData::ProposedUserStartedSpeaking),
        40,
    );
    let next = feed(&user, text(FrameData::EagerTranscription, "b"), 50);
    let capability = contexts(&next)[0].native_speculation().unwrap().clone();
    feed(&user, text(FrameData::Transcription, "b"), 60);
    let stopped = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        70,
    );
    assert!(contexts(&stopped).is_empty());
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
}
#[test]
fn final_segments_excluded_from_history_still_participate_in_source_matching() {
    for (eager_text, expected) in [
        ("secret", SpeculationStatus::Confirmed),
        ("other", SpeculationStatus::Cancelled),
    ] {
        let user = user();
        let prediction = begin(&user, eager_text);
        let capability = prediction.native_speculation().unwrap();
        let mut transcript = TextFrame::new("secret");
        transcript.append_to_context = false;
        feed(
            &user,
            Frame::new(FrameData::TranscriptionDetailed(
                DetailedTranscriptionFrame {
                    text: transcript,
                    user_id: "u".into(),
                    timestamp: "0".into(),
                    language: None,
                    result: None,
                    finalized: true,
                },
            )),
            20,
        );
        feed(
            &user,
            Frame::new(FrameData::ProposedUserStoppedSpeaking),
            30,
        );
        assert_eq!(capability.status(), expected);
        assert!(user.context().snapshot().unwrap().messages.is_empty());
        if expected == SpeculationStatus::Confirmed {
            assert!(capability.confirmed_context().unwrap().messages.is_empty());
        }
    }
}
#[test]
fn confirmed_excluded_transcript_does_not_trigger_empty_turn_recovery() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(EagerUserTurnConfig::default()),
            empty_user_turn: Some(EmptyUserTurnConfig {
                idle_prompt: Some("Please repeat the missing speech.".into()),
                ..Default::default()
            }),
            ..Default::default()
        },
    );
    let prediction = begin(&user, "secret");
    let mut transcript = TextFrame::new("secret");
    transcript.append_to_context = false;
    feed(
        &user,
        Frame::new(FrameData::TranscriptionDetailed(
            DetailedTranscriptionFrame {
                text: transcript,
                user_id: "u".into(),
                timestamp: "0".into(),
                language: None,
                result: None,
                finalized: true,
            },
        )),
        20,
    );
    let stopped = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        30,
    );
    assert_eq!(
        prediction.native_speculation().unwrap().status(),
        SpeculationStatus::Confirmed
    );
    assert!(contexts(&stopped).is_empty());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
}
#[test]
fn whitespace_final_clears_interim_and_resolves_the_next_external_stop_immediately() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(
                EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::from_secs(5), 1024)
                    .unwrap(),
            ),
            empty_user_turn: None,
            ..Default::default()
        },
    );
    feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 0);
    feed(&user, text(FrameData::Transcription, "a"), 1);
    feed(&user, text(FrameData::InterimTranscription, "uncertain"), 2);
    let eager = feed(&user, text(FrameData::EagerTranscription, " "), 3);
    let capability = contexts(&eager)[0].native_speculation().unwrap().clone();
    feed(&user, text(FrameData::Transcription, " "), 4);
    let stopped = feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 5);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert!(pushed(&stopped)
        .iter()
        .any(|frame| matches!(frame.data, FrameData::UserStoppedSpeaking)));
    assert!(contexts(&stopped).is_empty());
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(pipecat_services::Content::Text("a".into()))
    );
}
#[test]
fn whitespace_final_bookkeeping_is_scoped_to_external_stop_strategies() {
    let external = |external| UserTurnConfig {
        stop_strategies: Some(
            UserTurnStopStrategies::new(vec![if external {
                UserTurnStopStrategy::external(Duration::from_millis(500), true)
            } else {
                UserTurnStopStrategy::speech_timeout(Duration::from_millis(500), true)
            }])
            .unwrap(),
        ),
        ..Default::default()
    };
    for use_external in [false, true] {
        let user = LLMUserAggregator::new(
            SharedContext::default(),
            UserAggregatorConfig {
                turn: external(use_external),
                empty_user_turn: None,
                ..Default::default()
            },
        );
        feed(&user, Frame::new(FrameData::UserStartedSpeaking), 0);
        feed(&user, text(FrameData::Transcription, "a"), 1);
        feed(&user, text(FrameData::InterimTranscription, "uncertain"), 2);
        feed(&user, text(FrameData::Transcription, " "), 3);
        let output = feed(&user, Frame::new(FrameData::UserStoppedSpeaking), 4);
        assert_eq!(
            output
                .events
                .iter()
                .any(|event| matches!(event, AggregationEvent::UserTurnStopped { .. })),
            use_external
        );
    }
}
#[test]
fn provisional_inference_leaves_live_context_unchanged_and_duplicate_frames_do_not_reinfer() {
    let user = user();
    feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 0);
    let frame = text(FrameData::EagerTranscription, "book a flight");
    let result = feed(&user, frame.clone(), 10);
    let requests = contexts(&result);
    assert_eq!(requests.len(), 1);
    let FrameData::LlmContextDetailed(context) = &requests[0].data else {
        panic!()
    };
    assert!(context.speculation);
    assert_eq!(context.context.messages.len(), 1);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert!(contexts(&feed(&user, frame, 11)).is_empty());
}
#[test]
fn matching_final_commits_exact_final_once_and_suppresses_duplicate_inference() {
    let user = user();
    let inference = begin(&user, "its ready");
    let capability = inference.native_speculation().unwrap();
    feed(&user, text(FrameData::Transcription, "It's ready!"), 20);
    let result = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        30,
    );
    assert!(contexts(&result).is_empty());
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert!(pushed(&result)
        .iter()
        .any(|f| matches!(f.data, FrameData::UserStoppedSpeaking)
            && f.native_speculation() == Some(capability)));
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(pipecat_services::Content::Text("It's ready!".into()))
    );
}
#[test]
fn mismatch_cancels_before_stop_and_reinfers_committed_text() {
    let user = user();
    let inference = begin(&user, "cancel it");
    feed(&user, text(FrameData::Transcription, "reschedule it"), 20);
    let result = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        30,
    );
    let frames = pushed(&result);
    assert_eq!(
        inference.native_speculation().unwrap().status(),
        SpeculationStatus::Cancelled
    );
    assert!(matches!(frames[0].data, FrameData::EagerEndOfTurnCancel));
    assert_eq!(contexts(&result).len(), 1);
}
#[test]
fn timeout_tool_withdrawal_and_replacement_cannot_be_revived_by_matching_final() {
    for mode in [0, 1, 2] {
        let user = user();
        let first = begin(&user, "hello");
        let cap = first.native_speculation().unwrap();
        match mode {
            0 => {
                user.advance(Duration::from_secs(6)).unwrap();
            }
            1 => {
                assert!(!cap.admit_tool_intent());
            }
            _ => {
                feed(&user, text(FrameData::EagerTranscription, "different"), 15);
            }
        }
        feed(&user, text(FrameData::Transcription, "hello"), 7000);
        let out = feed(
            &user,
            Frame::new(FrameData::ProposedUserStoppedSpeaking),
            7100,
        );
        assert_eq!(cap.status(), SpeculationStatus::Cancelled);
        assert_eq!(contexts(&out).len(), 1);
    }
}
#[test]
fn stale_identified_cancel_does_not_withdraw_replacement() {
    let user = user();
    let old = begin(&user, "old");
    let new = contexts(&feed(&user, text(FrameData::EagerTranscription, "new"), 20))[0].clone();
    let mut cancel = Frame::new(FrameData::EagerEndOfTurnCancel);
    cancel
        .set_native_speculation(old.native_speculation().unwrap().clone())
        .unwrap();
    feed(&user, cancel, 21);
    feed(&user, text(FrameData::Transcription, "new"), 30);
    let out = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        40,
    );
    assert!(contexts(&out).is_empty());
    assert_eq!(
        new.native_speculation().unwrap().status(),
        SpeculationStatus::Confirmed
    );
}
#[test]
fn context_revisions_new_turn_mute_shutdown_and_owner_drop_cancel() {
    for mode in 0..5 {
        let user = user();
        let first = begin(&user, "hello");
        let cap = first.native_speculation().unwrap();
        match mode {
            0 => {
                feed(
                    &user,
                    Frame::new(FrameData::ProposedUserStartedSpeaking),
                    20,
                );
            }
            1 => {
                feed(&user, Frame::new(FrameData::UserMuteStarted), 20);
            }
            2 => {
                feed(&user, Frame::end(), 20);
            }
            3 => {
                user.context()
                    .append(pipecat_services::Message::text(
                        pipecat_services::Role::Tool,
                        "late",
                    ))
                    .unwrap();
                feed(&user, text(FrameData::Transcription, "hello"), 20);
                feed(
                    &user,
                    Frame::new(FrameData::ProposedUserStoppedSpeaking),
                    30,
                );
            }
            _ => drop(user),
        }
        assert_eq!(cap.status(), SpeculationStatus::Cancelled);
    }
}
#[test]
fn normalized_match_uses_nfkc_full_casefold_and_python_word_boundaries() {
    let policy = EagerMatchPolicy::Normalized;
    for (a, b) in [
        ("It's ready!", "its ready"),
        ("ＨＥＬＬＯ", "hello"),
        ("Straße", "STRASSE"),
        ("ΟΣ", "οσ"),
        ("\u{fb03}", "ffi"),
        ("a—b", "a b"),
        ("x\u{2019}s", "xs"),
        ("École", "école"),
    ] {
        assert!(policy.matches(a, b), "{a:?} {b:?}");
    }
    assert!(!policy.matches("cancel", "reschedule"));
    assert!(!EagerMatchPolicy::Exact.matches("Hello", "hello"));
}
#[test]
fn opt_in_required_and_limits_validated() {
    assert!(EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::ZERO, 10).is_err());
    let user = LLMUserAggregator::new(SharedContext::default(), Default::default());
    feed(&user, Frame::new(FrameData::UserStartedSpeaking), 0);
    assert!(contexts(&feed(
        &user,
        text(FrameData::EagerTranscription, "hello"),
        10
    ))
    .is_empty());
}
#[test]
fn a_late_final_is_rejected_even_before_the_deadline_task_wakes() {
    let user = user();
    let inference = begin(&user, "hello");
    feed(&user, text(FrameData::Transcription, "hello"), 6000);
    let out = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        6001,
    );
    assert_eq!(
        inference.native_speculation().unwrap().status(),
        SpeculationStatus::Cancelled
    );
    assert_eq!(contexts(&out).len(), 1);
}
#[test]
fn native_stt_helper_is_inert_when_disabled_and_withdraws_only_once() {
    let data = DetailedTranscriptionFrame {
        text: TextFrame::new("hello"),
        user_id: "u".into(),
        timestamp: "0".into(),
        language: None,
        result: None,
        finalized: false,
    };
    let mut disabled = EagerEndOfTurn::default();
    assert!(disabled.predict(data.clone()).is_none());
    assert!(disabled.cancel().is_none());
    assert!(disabled.recommended_eager_config().is_none());
    let mut enabled = EagerEndOfTurn::new(true);
    assert!(enabled.predict(data.clone()).is_some());
    assert!(enabled.is_pending());
    assert!(enabled.cancel().is_some());
    assert!(enabled.cancel().is_none());
    assert!(enabled.predict(data).is_some());
    enabled.clear();
    assert!(enabled.cancel().is_none());
}
#[test]
fn exact_policy_does_not_silently_trim_prediction_or_final_transcript() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(
                EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::from_secs(5), 1024)
                    .unwrap(),
            ),
            empty_user_turn: None,
            ..Default::default()
        },
    );
    let inference = begin(&user, " hello ");
    feed(&user, text(FrameData::Transcription, "hello"), 20);
    let out = feed(
        &user,
        Frame::new(FrameData::ProposedUserStoppedSpeaking),
        30,
    );
    assert_eq!(
        inference.native_speculation().unwrap().status(),
        SpeculationStatus::Cancelled
    );
    assert_eq!(contexts(&out).len(), 1);
}
