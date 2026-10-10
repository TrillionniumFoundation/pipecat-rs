//! Contracts from user_idle_controller.py and test_user_idle_controller.py.
use pipecat_core::*;
use pipecat_turns::*;
use serde_json::json;
use std::time::Duration;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn frame(controller: &mut UserIdleController, data: FrameData, now: u64) {
    controller.process(&data, ms(now)).unwrap();
}
fn calls(count: usize) -> FrameData {
    FrameData::FunctionCallsStarted(
        (0..count)
            .map(|i| FunctionCall {
                function_name: "lookup".into(),
                tool_call_id: i.to_string(),
                arguments: json!({}),
            })
            .collect(),
    )
}
fn result() -> FrameData {
    FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: "lookup".into(),
        tool_call_id: "0".into(),
        arguments: json!({}),
        result: json!("ok"),
        run_llm: None,
        is_final: true,
        error: None,
    })
}
fn cancel() -> FrameData {
    FrameData::FunctionCallCancel(FunctionCallCancelFrame {
        function_name: "lookup".into(),
        tool_call_id: "1".into(),
        run_llm: true,
    })
}

#[test]
fn idle_after_bot_stops_is_one_shot_and_can_repeat_next_cycle() {
    let mut c = UserIdleController::new(ms(200));
    assert!(!c.waiting_for_user());
    assert!(!c.advance(ms(1000))); // No idle before bot speaks.
    frame(&mut c, FrameData::BotStoppedSpeaking, 1000);
    assert!(c.waiting_for_user());
    assert!(!c.advance(ms(1199)));
    assert!(c.advance(ms(1200)));
    assert!(c.waiting_for_user());
    assert!(!c.advance(ms(1500)));
    frame(&mut c, FrameData::BotStartedSpeaking, 1500);
    assert!(!c.waiting_for_user());
    frame(&mut c, FrameData::BotStoppedSpeaking, 1700);
    assert!(c.advance(ms(1900)));
}

#[test]
fn either_speaker_cancels_timer() {
    for start in [
        FrameData::UserStartedSpeaking,
        FrameData::BotStartedSpeaking,
    ] {
        let mut c = UserIdleController::new(ms(200));
        frame(&mut c, FrameData::BotStoppedSpeaking, 0);
        frame(&mut c, start, 199);
        assert!(!c.waiting_for_user());
        assert_eq!(c.deadline(), None);
        assert!(!c.advance(ms(1000)));
    }
}

#[test]
fn interruption_no_false_trigger() {
    let mut c = UserIdleController::new(ms(200));
    frame(&mut c, FrameData::BotStartedSpeaking, 0);
    frame(&mut c, FrameData::UserStartedSpeaking, 20);
    frame(&mut c, FrameData::BotStoppedSpeaking, 30);
    assert!(!c.advance(ms(1000)));
    assert!(!c.waiting_for_user());
    frame(&mut c, FrameData::UserStoppedSpeaking, 1000);
    assert!(!c.advance(ms(2000))); // Now waiting for the bot's response.
}

#[test]
fn function_call_cancels_timer_and_completion_alone_does_not_arm() {
    let mut c = UserIdleController::new(ms(200));
    frame(&mut c, FrameData::BotStoppedSpeaking, 0);
    frame(&mut c, calls(1), 50);
    assert!(c.function_calls_in_progress());
    assert!(!c.waiting_for_user());
    assert!(!c.advance(ms(500)));
    frame(&mut c, result(), 600);
    assert!(!c.function_calls_in_progress());
    assert!(!c.advance(ms(1000)));
}

#[test]
fn function_call_suppresses_timer_in_both_racing_orders() {
    for bot_first in [false, true] {
        let mut c = UserIdleController::new(ms(200));
        if bot_first {
            frame(&mut c, FrameData::BotStoppedSpeaking, 0);
        }
        frame(&mut c, calls(2), 10);
        frame(&mut c, FrameData::BotStartedSpeaking, 20);
        frame(&mut c, FrameData::BotStoppedSpeaking, 30);
        assert!(!c.advance(ms(500)));
        frame(&mut c, result(), 500);
        assert!(c.function_calls_in_progress());
        frame(&mut c, FrameData::BotStoppedSpeaking, 510);
        assert!(!c.advance(ms(1000)));
        frame(&mut c, cancel(), 1000);
        assert!(!c.function_calls_in_progress());
        frame(&mut c, cancel(), 1001); // Unmatched completion cannot underflow.
        frame(&mut c, FrameData::BotStartedSpeaking, 1010);
        frame(&mut c, FrameData::BotStoppedSpeaking, 1020);
        assert!(c.advance(ms(1220)));
    }
}

#[test]
fn detailed_function_frames_obey_the_same_guard() {
    let mut c = UserIdleController::new(ms(200));
    frame(
        &mut c,
        FrameData::FunctionCallsStartedDetailed(vec![FunctionCallFromLlm {
            function_name: "lookup".into(),
            tool_call_id: "0".into(),
            arguments: Default::default(),
            context: None,
        }]),
        0,
    );
    frame(&mut c, FrameData::BotStoppedSpeaking, 10);
    assert!(!c.advance(ms(1000)));
    frame(
        &mut c,
        FrameData::FunctionCallResultDetailed(DetailedFunctionCallResultFrame {
            function_name: "lookup".into(),
            tool_call_id: "0".into(),
            arguments: json!({}),
            result: json!("ok"),
            run_llm: None,
            properties: None,
            error: None,
        }),
        1000,
    );
    assert!(!c.function_calls_in_progress());
    c.wait_for_user(ms(1000));
    assert!(c.advance(ms(1200)));
}

#[test]
fn disabled_waiting_window_can_be_enabled_without_another_bot_stop() {
    let mut c = UserIdleController::default();
    frame(&mut c, FrameData::BotStoppedSpeaking, 0);
    assert!(c.waiting_for_user());
    assert!(!c.advance(ms(1000)));
    frame(
        &mut c,
        FrameData::UserIdleTimeoutUpdate { timeout: 0.2 },
        1000,
    );
    assert!(!c.advance(ms(1199)));
    assert!(c.advance(ms(1200)));
}

#[test]
fn update_restarts_from_update_time_and_disabling_cancels() {
    let mut c = UserIdleController::new(ms(200));
    frame(&mut c, FrameData::BotStoppedSpeaking, 0);
    frame(
        &mut c,
        FrameData::UserIdleTimeoutUpdate { timeout: 0.4 },
        100,
    );
    assert_eq!(c.deadline(), Some(ms(500)));
    assert!(!c.advance(ms(200)));
    frame(
        &mut c,
        FrameData::UserIdleTimeoutUpdate { timeout: 0.05 },
        210,
    );
    assert!(!c.advance(ms(259)));
    assert!(c.advance(ms(260)));
    // The waiting window remains open after a one-shot expiration.
    frame(
        &mut c,
        FrameData::UserIdleTimeoutUpdate { timeout: 0.1 },
        300,
    );
    assert_eq!(c.deadline(), Some(ms(400)));
    for timeout in [0.0, -5.0] {
        frame(&mut c, FrameData::UserIdleTimeoutUpdate { timeout }, 301);
        assert_eq!(c.timeout(), Duration::ZERO);
        assert!(!c.advance(ms(1000)));
    }
}

#[test]
fn update_does_not_arm_before_bot_stop_or_while_activity_is_pending() {
    let cases = [
        vec![],
        vec![FrameData::BotStartedSpeaking],
        vec![
            FrameData::UserStartedSpeaking,
            FrameData::BotStoppedSpeaking,
        ],
        vec![FrameData::BotStoppedSpeaking, calls(1)],
    ];
    for frames in cases {
        let mut c = UserIdleController::default();
        for data in frames {
            frame(&mut c, data, 0);
        }
        frame(&mut c, FrameData::UserIdleTimeoutUpdate { timeout: 0.2 }, 0);
        assert!(!c.advance(ms(1000)));
        assert!(!c.waiting_for_user());
    }
}

#[test]
fn update_while_awaiting_bot_response_does_not_arm() {
    let mut c = UserIdleController::default();
    frame(&mut c, FrameData::UserStartedSpeaking, 0);
    frame(&mut c, FrameData::UserStoppedSpeaking, 100);
    frame(
        &mut c,
        FrameData::UserIdleTimeoutUpdate { timeout: 0.2 },
        200,
    );
    assert!(!c.waiting_for_user());
    assert!(!c.advance(ms(1000)));
}

#[test]
fn wait_for_user_arms_after_unanswered_turn_and_respects_all_guards() {
    let mut c = UserIdleController::new(ms(200));
    frame(&mut c, FrameData::BotStoppedSpeaking, 0);
    frame(&mut c, FrameData::UserStartedSpeaking, 10);
    frame(&mut c, FrameData::UserStoppedSpeaking, 20);
    assert!(!c.waiting_for_user());
    c.wait_for_user(ms(20));
    assert!(c.waiting_for_user());
    assert!(c.advance(ms(220)));
    for active in [
        FrameData::BotStartedSpeaking,
        FrameData::UserStartedSpeaking,
        calls(1),
    ] {
        let mut c = UserIdleController::new(ms(200));
        frame(&mut c, active, 0);
        c.wait_for_user(ms(10));
        assert!(!c.waiting_for_user());
        assert!(!c.advance(ms(1000)));
    }
}

#[test]
fn stop_cancels_and_invalid_timeout_never_changes_controller_state() {
    let mut c = UserIdleController::new(ms(200));
    frame(&mut c, FrameData::BotStoppedSpeaking, 0);
    for timeout in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, f64::MAX] {
        assert!(c
            .process(&FrameData::UserIdleTimeoutUpdate { timeout }, ms(1000))
            .is_err());
        assert_eq!(c.timeout(), ms(200));
        assert_eq!(c.deadline(), Some(ms(200)));
    }
    assert!(!c.advance(ms(100))); // Rejected frames didn't advance the clock.
    c.stop();
    assert!(!c.advance(ms(1000)));
}

fn user(timeout: Duration) -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            user_idle_timeout: timeout,
            turn: UserTurnConfig {
                enable_interruptions: false,
                ..Default::default()
            },
            ..Default::default()
        },
    )
}
fn user_frame(user: &LLMUserAggregator, data: FrameData, now: u64) -> AggregationOutput {
    user.process_at(Frame::new(data), Direction::Downstream, ms(now))
        .unwrap()
}
#[test]
fn aggregator_delivers_deterministic_idle_events_without_context_or_turn_stop() {
    let user = user(ms(200));
    let forwarded = user_frame(&user, FrameData::BotStoppedSpeaking, 0);
    assert_eq!(forwarded.actions.len(), 1);
    assert!(user.waiting_for_user().unwrap());
    assert_eq!(user.deadline().unwrap(), None);
    assert_eq!(user.idle_deadline().unwrap(), Some(ms(200)));
    let output = user.advance(ms(200)).unwrap();
    assert_eq!(output.events, [AggregationEvent::UserTurnIdle]);
    assert!(output.actions.is_empty());
    assert!(user.context().snapshot().unwrap().messages.is_empty());
}

#[test]
fn aggregator_transcript_turn_waits_for_bot_but_empty_turn_rearms() {
    let user = user(ms(200));
    user_frame(
        &user,
        FrameData::Transcription(TranscriptionFrame {
            text: "hello".into(),
            finalized: true,
            ..Default::default()
        }),
        0,
    );
    user.advance(ms(600)).unwrap();
    assert!(!user.waiting_for_user().unwrap());
    user_frame(
        &user,
        FrameData::UserIdleTimeoutUpdate { timeout: 0.1 },
        601,
    );
    assert!(user.advance(ms(1000)).unwrap().events.is_empty());
    user_frame(&user, FrameData::BotStoppedSpeaking, 1000);
    user_frame(&user, FrameData::UserStartedSpeaking, 1010);
    user_frame(&user, FrameData::BotStoppedSpeaking, 1020);
    assert_eq!(user.idle_deadline().unwrap(), None);
    user_frame(&user, FrameData::UserStoppedSpeaking, 1100);
    // The turn watchdog resolves the empty turn. Only then can idle restart.
    let output = user.advance(ms(6100)).unwrap();
    assert!(output.events.iter().any(|e| matches!(e, AggregationEvent::UserTurnStopped { text, reason: TurnStopReason::Watchdog } if text.is_empty())));
    assert_eq!(user.idle_deadline().unwrap(), Some(ms(6200)));
    assert_eq!(
        user.advance(ms(6200)).unwrap().events,
        [AggregationEvent::UserTurnIdle]
    );
}

#[test]
fn aggregator_explicit_wait_and_terminal_frames_never_resurrect_idle() {
    for terminal in [
        FrameData::End(EndFrame::default()),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame {
            reason: Some("test".into()),
        }),
        FrameData::EndDetailed(ReasonFrame::default()),
        FrameData::CancelDetailed(ReasonFrame::default()),
    ] {
        let user = user(ms(200));
        user.wait_for_user_at(ms(0)).unwrap();
        assert_eq!(user.idle_deadline().unwrap(), Some(ms(200)));
        user_frame(&user, terminal, 10);
        user.wait_for_user_at(ms(20)).unwrap();
        user_frame(&user, FrameData::BotStoppedSpeaking, 20);
        user_frame(&user, FrameData::UserIdleTimeoutUpdate { timeout: 0.1 }, 20);
        assert_eq!(user.idle_deadline().unwrap(), None);
        assert!(!user.waiting_for_user().unwrap());
        assert!(user.advance(ms(1000)).unwrap().events.is_empty());
    }
}
