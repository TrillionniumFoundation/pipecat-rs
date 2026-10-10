//! Source: turns/user_stop/{speech_timeout,external,external_user_turn_completion,
//! deferred,turn_analyzer}_user_turn_stop_strategy.py at af9f9da9.
//! Deferred notifications intentionally do not run Python's conversational LLM route.
use pipecat_core::*;
use pipecat_services::{Content, Role};
use pipecat_turns::*;
use std::time::Duration;
fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn speech(n: u64) -> UserTurnStopStrategy {
    UserTurnStopStrategy::speech_timeout(ms(n), true)
}
fn external() -> UserTurnStopStrategy {
    UserTurnStopStrategy::external(ms(500), true)
}
fn list(policies: Vec<UserTurnStopStrategy>) -> UserTurnStopStrategies {
    UserTurnStopStrategies::new(policies).unwrap()
}
fn controller(policies: Vec<UserTurnStopStrategy>) -> UserTurnController {
    UserTurnController::new(UserTurnConfig {
        stt_timeout: ms(800),
        stop_timeout: ms(5000),
        stop_strategies: Some(list(policies)),
        ..Default::default()
    })
}
fn frame(name: &str, text: &str) -> FrameData {
    match name {
        "start" => FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        }),
        "stop" => FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            timestamp: 0.0,
            duration_secs: 0.2,
        }),
        "final" | "segment" => FrameData::Transcription(TranscriptionFrame {
            text: text.into(),
            finalized: name == "final",
            ..Default::default()
        }),
        "interim" => FrameData::InterimTranscription(TranscriptionFrame {
            text: text.into(),
            ..Default::default()
        }),
        "external_start" => FrameData::UserStartedSpeaking,
        "external_stop" => FrameData::UserStoppedSpeaking,
        "proposal" => FrameData::ProposedUserStoppedSpeaking,
        "proposed_start" => FrameData::ProposedUserStartedSpeaking,
        "complete" => FrameData::UserTurnInferenceCompleted,
        _ => panic!("unknown frame"),
    }
}
fn at(c: &mut UserTurnController, name: &str, text: &str, now: u64) -> Vec<UserTurnEvent> {
    c.process(&frame(name, text), ms(now))
}
fn stopped(events: &[UserTurnEvent]) -> bool {
    events
        .iter()
        .any(|e| matches!(e, UserTurnEvent::Stopped { .. }))
}
fn contexts(output: &AggregationOutput) -> usize {
    output.actions.iter().filter(|a| matches!(a, AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)))).count()
}
fn request(output: &AggregationOutput) -> UserTurnInferenceRequest {
    output
        .events
        .iter()
        .find_map(|e| match e {
            AggregationEvent::UserTurnInferenceRequested(r) => Some(r.clone()),
            _ => None,
        })
        .expect("external request")
}
fn user(policies: Vec<UserTurnStopStrategy>) -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                stop_strategies: Some(list(policies)),
                speech_timeout: ms(20),
                stt_timeout: ms(800),
                enable_interruptions: false,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}
fn ua(user: &LLMUserAggregator, name: &str, text: &str, now: u64) -> AggregationOutput {
    user.process_at(
        Frame::new(frame(name, text)),
        Direction::Downstream,
        ms(now),
    )
    .unwrap()
}

#[test]
fn source_finalized_short_circuits_stt_but_not_speech_floor() {
    let mut c = controller(vec![speech(100)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "final", "hello", 10);
    at(&mut c, "stop", "", 20);
    assert_eq!(c.deadline(), Some(ms(120)));
    assert!(!stopped(&c.advance(ms(119))));
    assert!(stopped(&c.advance(ms(120))));
}
#[test]
fn source_nonfinalized_waits_full_stt_timeout() {
    let mut c = controller(vec![speech(100)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "segment", "hello", 10);
    at(&mut c, "stop", "", 20);
    assert_eq!(c.deadline(), Some(ms(620)));
    assert!(!stopped(&c.advance(ms(619))));
    assert!(stopped(&c.advance(ms(620))));
}
#[test]
fn source_no_vad_fallback_uses_only_speech_timeout_and_refreshes() {
    let mut c = controller(vec![speech(100)]);
    at(&mut c, "segment", "hello", 0);
    at(&mut c, "segment", "world", 80);
    assert_eq!(c.deadline(), Some(ms(180)));
    assert!(stopped(&c.advance(ms(180))));
}
#[test]
fn interim_revokes_finalized_without_extending_physical_stop_budget() {
    let mut c = controller(vec![speech(100)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "final", "hello", 10);
    at(&mut c, "stop", "", 20);
    at(&mut c, "interim", "world", 50);
    assert_eq!(c.deadline(), Some(ms(620)));
    at(&mut c, "segment", "world", 500);
    assert!(stopped(&c.advance(ms(620))));
}
#[test]
fn source_mid_utterance_start_preserves_live_vad() {
    let mut c = UserTurnController::new(UserTurnConfig {
        start_strategies: Some(
            UserTurnStartStrategies::new(vec![UserTurnStartStrategy::transcription(false)])
                .unwrap(),
        ),
        stop_strategies: Some(list(vec![speech(100)])),
        ..Default::default()
    });
    at(&mut c, "start", "", 0);
    at(&mut c, "final", "hello", 20);
    assert!(c.is_active());
    assert!(c.is_speaking());
    assert_eq!(c.deadline(), None);
    assert!(!stopped(&c.advance(ms(10000))));
    at(&mut c, "stop", "", 10001);
    assert!(stopped(&c.advance(ms(10101))));
}
#[test]
fn speech_no_text_still_waits_stt_when_transcript_requirement_is_disabled() {
    let mut c = controller(vec![UserTurnStopStrategy::speech_timeout(ms(100), false)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "stop", "", 20);
    assert_eq!(c.deadline(), Some(ms(620)));
    assert!(stopped(&c.advance(ms(620))));
}
#[test]
fn source_external_adopts_real_stop_and_resolves_proposal() {
    for (name, emit) in [("external_stop", false), ("proposal", true)] {
        let mut c = controller(vec![external()]);
        at(&mut c, "external_start", "", 0);
        at(&mut c, "final", "hello", 10);
        assert_eq!(
            at(&mut c, name, "", 20),
            vec![UserTurnEvent::Stopped {
                emit_speaking: emit,
                reason: TurnStopReason::External
            }]
        );
        assert_eq!(c.deadline(), None);
    }
}
#[test]
fn source_external_late_final_waits_transcript_inactivity() {
    let mut c = controller(vec![external()]);
    at(&mut c, "external_start", "", 0);
    at(&mut c, "external_stop", "", 20);
    assert!(!stopped(&at(&mut c, "final", "hello", 100)));
    at(&mut c, "final", "world", 300);
    assert_eq!(c.deadline(), Some(ms(800)));
    assert!(stopped(&c.advance(ms(800))));
}
#[test]
fn source_external_interim_holds_stop_until_final() {
    let mut c = controller(vec![external()]);
    at(&mut c, "external_start", "", 0);
    at(&mut c, "final", "hello", 10);
    at(&mut c, "interim", "world", 15);
    assert!(!stopped(&at(&mut c, "external_stop", "", 20)));
    assert!(!stopped(&c.advance(ms(1000))));
    at(&mut c, "final", "world", 1100);
    assert!(stopped(&c.advance(ms(1600))));
}
#[test]
fn external_without_transcript_closes_on_signal_and_never_idle_ticks() {
    let mut c = controller(vec![UserTurnStopStrategy::external(ms(500), false)]);
    assert!(c.advance(ms(10000)).is_empty());
    at(&mut c, "external_start", "", 10001);
    assert!(stopped(&at(&mut c, "proposal", "", 10002)));
    assert!(c.advance(ms(100000)).is_empty());
}
#[test]
fn analyzer_requires_actual_verdict_and_keeps_original_stt_deadline() {
    let mut c = controller(vec![UserTurnStopStrategy::turn_analyzer(true)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "segment", "hello", 10);
    at(&mut c, "stop", "", 20);
    assert!(!stopped(&c.advance(ms(620))));
    assert!(stopped(&c.mark_model_complete(ms(1000))));
}
#[test]
fn analyzer_complete_before_text_waits_for_late_final() {
    let mut c = controller(vec![UserTurnStopStrategy::turn_analyzer(true)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "stop", "", 20);
    c.mark_model_complete(ms(30));
    assert!(stopped(&at(&mut c, "final", "hello", 40)));
}
#[test]
fn analyzer_without_transcript_releases_on_real_verdict() {
    let mut c = controller(vec![UserTurnStopStrategy::turn_analyzer(false)]);
    at(&mut c, "start", "", 0);
    at(&mut c, "stop", "", 20);
    assert!(stopped(&c.mark_model_complete(ms(30))));
}
#[test]
fn analyzer_source_transcript_only_fallback_waits_stt_budget() {
    let mut c = controller(vec![UserTurnStopStrategy::turn_analyzer(true)]);
    assert!(!stopped(&at(&mut c, "final", "hello", 0)));
    assert_eq!(c.deadline(), Some(ms(600)));
    assert!(stopped(&c.advance(ms(600))));
}
#[test]
fn policies_are_ordered_or_and_finalization_cancels_all_other_deadlines() {
    let mut c = controller(vec![speech(100), speech(200), external()]);
    at(&mut c, "final", "hello", 0);
    assert!(stopped(&c.advance(ms(100))));
    assert_eq!(c.deadline(), None);
    assert!(c.advance(ms(10000)).is_empty());
}
#[test]
fn external_completion_is_finalization_only_and_drops_stale_speaking_signal() {
    let mut c = controller(vec![UserTurnStopStrategy::external_completion()]);
    at(&mut c, "start", "", 0);
    assert!(!stopped(&at(&mut c, "complete", "", 10)));
    at(&mut c, "final", "hello", 20);
    at(&mut c, "stop", "", 30);
    assert_eq!(
        at(&mut c, "complete", "", 40),
        vec![UserTurnEvent::Stopped {
            emit_speaking: true,
            reason: TurnStopReason::ExternalCompletion
        }]
    );
}
#[test]
fn empty_policy_list_disables_detectors_but_retains_watchdog() {
    let mut c = controller(vec![]);
    at(&mut c, "final", "hello", 0);
    assert_eq!(c.deadline(), Some(ms(5000)));
    assert!(stopped(&c.advance(ms(5000))));
}
#[test]
fn deferred_request_never_commits_or_runs_context_and_completion_runs_once() {
    let user = user(vec![
        speech(100).deferred(),
        UserTurnStopStrategy::external_completion(),
    ]);
    ua(&user, "final", "hello", 0);
    let out = user.advance(ms(100)).unwrap();
    let r = request(&out);
    assert_eq!(r.text, "hello");
    assert_eq!(contexts(&out), 0);
    assert!(user.context().snapshot().unwrap().messages.is_empty());
    assert_eq!(
        user.pending_inference_request().unwrap(),
        Some(r.request_id)
    );
    assert!(user.advance(ms(101)).unwrap().events.is_empty());
    let done = user
        .process_at(r.completion_frame(), Direction::Downstream, ms(110))
        .unwrap();
    assert_eq!(contexts(&done), 1);
    assert!(done.events.contains(&AggregationEvent::UserTurnStopped {
        text: "hello".into(),
        reason: TurnStopReason::ExternalCompletion
    }));
    assert_eq!(
        contexts(&user.complete_inference_at(r.request_id, ms(120)).unwrap()),
        0
    );
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
}
#[test]
fn deferred_request_contains_all_accepted_finals_no_interim_or_append_false() {
    let user = user(vec![
        speech(100).deferred(),
        UserTurnStopStrategy::external_completion(),
    ]);
    ua(&user, "final", "one", 0);
    ua(&user, "interim", "guess", 10);
    ua(&user, "final", "two", 20);
    user.process_at(
        Frame::new(FrameData::TranscriptionDetailed(
            DetailedTranscriptionFrame {
                text: TextFrame {
                    append_to_context: false,
                    ..TextFrame::new("excluded")
                },
                user_id: String::new(),
                timestamp: String::new(),
                language: None,
                result: None,
                finalized: true,
            },
        )),
        Direction::Downstream,
        ms(30),
    )
    .unwrap();
    let r = request(&user.advance(ms(130)).unwrap());
    assert_eq!(r.text, "one two");
    let done = user.complete_inference_at(r.request_id, ms(140)).unwrap();
    assert_eq!(contexts(&done), 1);
    assert_eq!(
        user.context().snapshot().unwrap().messages[0].content,
        Some(Content::Text("one two".into()))
    );
}
#[test]
fn revised_text_and_resumed_speech_invalidate_correlated_completion() {
    let user = user(vec![
        speech(100).deferred(),
        UserTurnStopStrategy::external_completion(),
    ]);
    ua(&user, "final", "one", 0);
    let first = request(&user.advance(ms(100)).unwrap());
    ua(&user, "final", "two", 110);
    assert!(user
        .process_at(first.completion_frame(), Direction::Downstream, ms(120))
        .unwrap()
        .actions
        .is_empty());
    let second = request(&user.advance(ms(210)).unwrap());
    assert_ne!(first.request_id, second.request_id);
    ua(&user, "start", "", 220);
    ua(&user, "stop", "", 230);
    assert!(user
        .complete_inference_at(second.request_id, ms(240))
        .unwrap()
        .events
        .is_empty());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
}
#[test]
fn deferred_detection_watchdog_is_a_bounded_finalization_fallback() {
    let user = user(vec![
        speech(100).deferred(),
        UserTurnStopStrategy::external_completion(),
    ]);
    ua(&user, "final", "hello", 0);
    let r = request(&user.advance(ms(100)).unwrap());
    let end = user.advance(ms(5100)).unwrap();
    assert_eq!(contexts(&end), 1);
    assert!(user
        .complete_inference_at(r.request_id, ms(5200))
        .unwrap()
        .events
        .is_empty());
}
#[test]
fn mute_and_cancel_never_launch_inference_or_accept_stale_verdict() {
    for data in [
        FrameData::UserMuteStarted,
        FrameData::Cancel(CancelFrame {
            reason: Some("done".into()),
        }),
    ] {
        let user = user(vec![
            speech(100).deferred(),
            UserTurnStopStrategy::external_completion(),
        ]);
        ua(&user, "final", "accepted", 0);
        let r = request(&user.advance(ms(100)).unwrap());
        let out = user
            .process_at(Frame::new(data), Direction::Downstream, ms(110))
            .unwrap();
        assert_eq!(contexts(&out), 0);
        assert_eq!(
            contexts(&user.complete_inference_at(r.request_id, ms(120)).unwrap()),
            0
        );
        assert_eq!(
            user.context().snapshot().unwrap().messages[0].role,
            Role::User
        );
    }
}
#[test]
fn consumed_proposal_does_not_reach_a_second_resolver() {
    let user = user(vec![external()]);
    ua(&user, "external_start", "", 0);
    ua(&user, "final", "hello", 10);
    let out = ua(&user, "proposal", "", 20);
    assert!(!out.actions.iter().any(|a| matches!(a,AggregationAction::Push(f) if matches!(f.frame.data,FrameData::ProposedUserStoppedSpeaking))));
    assert_eq!(contexts(&out), 1);
}
#[test]
fn stop_policy_validation_and_reset() {
    assert!(UserTurnStopStrategies::new(vec![speech(10); 65]).is_err());
    assert!(
        UserTurnStopStrategies::new(vec![UserTurnStopStrategy::external(Duration::ZERO, true)])
            .is_err()
    );
    assert!(UserTurnStopStrategies::new(vec![
        UserTurnStopStrategy::external_completion().deferred()
    ])
    .is_err());
    let mut c = controller(vec![speech(100)]);
    at(&mut c, "final", "hello", 1000);
    c.reset();
    assert_eq!(c.deadline(), None);
    at(&mut c, "final", "new", 0);
    assert!(stopped(&c.advance(ms(100))));
}

#[test]
fn deferred_external_stop_cannot_release_paired_tool_continuation() {
    use serde_json::json;
    let pair = LLMContextAggregatorPair::new(
        pipecat_services::Context::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                enable_interruptions: false,
                stop_strategies: Some(list(vec![
                    external().deferred(),
                    UserTurnStopStrategy::external_completion(),
                ])),
                ..Default::default()
            },
            ..Default::default()
        },
        AssistantAggregatorConfig::default(),
    );
    let assistant = pair.assistant();
    assistant
        .process_frame(
            Frame::new(FrameData::FunctionCallInProgress(
                FunctionCallInProgressFrame {
                    function_name: "lookup".into(),
                    tool_call_id: "background".into(),
                    arguments: json!({}),
                    cancel_on_interruption: false,
                    group_id: None,
                },
            )),
            Direction::Downstream,
        )
        .unwrap();
    ua(&pair.user(), "external_start", "", 0);
    ua(&pair.user(), "final", "hello", 10);
    let pending = ua(&pair.user(), "external_stop", "", 20);
    let r = request(&pending);
    assert!(!pending.actions.iter().any(|a| matches!(a,AggregationAction::Push(f) if matches!(f.frame.data,FrameData::UserStoppedSpeaking))));
    let result = assistant
        .process_frame(
            Frame::new(FrameData::FunctionCallResult(FunctionCallResultFrame {
                function_name: "lookup".into(),
                tool_call_id: "background".into(),
                arguments: json!({}),
                result: json!(42),
                run_llm: None,
                is_final: true,
                error: None,
            })),
            Direction::Downstream,
        )
        .unwrap();
    assert_eq!(
        contexts(&result),
        0,
        "completion is still held by semantic user turn"
    );
    let end = pair
        .user()
        .complete_inference_at(r.request_id, ms(30))
        .unwrap();
    assert_eq!(contexts(&end), 1);
    let context = end
        .actions
        .iter()
        .find_map(|a| match a {
            AggregationAction::Push(f) => match &f.frame.data {
                FrameData::LlmContext(c) => Some(c),
                _ => None,
            },
            _ => None,
        })
        .unwrap();
    assert!(context.messages.iter().any(|m| m.role == ChatRole::Tool));
    assert_eq!(context.messages.last().unwrap().content, "hello");
}
#[test]
fn immediate_adopted_external_stop_is_forwarded_exactly_once() {
    let user = user(vec![external()]);
    ua(&user, "external_start", "", 0);
    ua(&user, "final", "hello", 10);
    let out = ua(&user, "external_stop", "", 20);
    assert_eq!(out.actions.iter().filter(|a| matches!(a,AggregationAction::Push(f) if matches!(f.frame.data,FrameData::UserStoppedSpeaking))).count(),1);
    assert!(!out.actions.iter().any(|a| matches!(a,AggregationAction::Broadcast(f) if matches!(f.data,FrameData::UserStoppedSpeaking))));
}

#[test]
fn immediate_finalizer_supersedes_same_batch_external_notification() {
    let user = user(vec![speech(100).deferred(), speech(100)]);
    ua(&user, "final", "accepted", 0);
    let out = user.advance(ms(100)).unwrap();
    assert!(!out
        .events
        .iter()
        .any(|e| matches!(e, AggregationEvent::UserTurnInferenceRequested(_))));
    assert_eq!(contexts(&out), 1);
    assert_eq!(user.pending_inference_request().unwrap(), None);
}
#[test]
fn several_deferred_detectors_coalesce_and_do_not_repeat_on_unchanged_clock() {
    let user = user(vec![
        speech(100).deferred(),
        speech(100).deferred(),
        UserTurnStopStrategy::external_completion(),
    ]);
    ua(&user, "final", "accepted", 0);
    let out = user.advance(ms(100)).unwrap();
    assert_eq!(
        out.events
            .iter()
            .filter(|e| matches!(e, AggregationEvent::UserTurnInferenceRequested(_)))
            .count(),
        1
    );
    assert!(user.advance(ms(100)).unwrap().events.is_empty());
}

#[test]
fn external_start_requires_external_stop_even_if_vad_stops_first() {
    for start in ["external_start", "proposed_start"] {
        let mut c = UserTurnController::new(UserTurnConfig {
            resolve_proposals: true,
            stop_strategies: Some(list(vec![external()])),
            ..Default::default()
        });
        at(&mut c, start, "", 0);
        at(&mut c, "final", "accepted", 10);
        at(&mut c, "stop", "", 20);
        assert!(!stopped(&c.advance(ms(600))));
        assert!(stopped(&at(&mut c, "external_stop", "", 610)));
    }
}
