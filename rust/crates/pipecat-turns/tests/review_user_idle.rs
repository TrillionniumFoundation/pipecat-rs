//! Independent idle lifecycle regressions, including malformed/replayed tool traffic.
use pipecat_core::*;
use pipecat_turns::*;
use serde_json::json;
use std::time::Duration;

fn at(c: &mut UserIdleController, frame: FrameData, seconds: u64) {
    c.process(&frame, Duration::from_secs(seconds)).unwrap();
}
fn calls(ids: &[&str]) -> FrameData {
    FrameData::FunctionCallsStarted(
        ids.iter()
            .map(|id| FunctionCall {
                function_name: "lookup".into(),
                tool_call_id: (*id).into(),
                arguments: json!({}),
            })
            .collect(),
    )
}
fn result(id: &str, final_result: bool) -> FrameData {
    FrameData::FunctionCallResult(FunctionCallResultFrame {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        arguments: json!({}),
        result: json!("ok"),
        run_llm: None,
        is_final: final_result,
        error: None,
    })
}
fn cancel(id: &str) -> FrameData {
    FrameData::FunctionCallCancel(FunctionCallCancelFrame {
        function_name: "lookup".into(),
        tool_call_id: id.into(),
        run_llm: true,
    })
}
fn no_idle(c: &mut UserIdleController, seconds: u64) {
    at(c, FrameData::BotStoppedSpeaking, seconds);
    c.wait_for_user(Duration::from_secs(seconds));
    assert!(c.function_calls_in_progress());
    assert!(!c.waiting_for_user());
    assert!(!c.advance(Duration::from_secs(seconds + 10)));
}

#[test]
fn unmatched_duplicate_and_intermediate_results_cannot_release_pending_calls() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    at(&mut c, calls(&["a", "b"]), 0);
    at(&mut c, result("unknown", true), 1);
    at(&mut c, cancel("unknown"), 2);
    no_idle(&mut c, 3);
    at(&mut c, result("a", false), 20);
    at(&mut c, result("a", false), 21);
    no_idle(&mut c, 22);
    at(&mut c, result("a", true), 40);
    at(&mut c, result("a", true), 41);
    at(&mut c, cancel("a"), 42);
    no_idle(&mut c, 43);
    at(&mut c, cancel("b"), 60);
    assert!(!c.function_calls_in_progress());
    assert!(!c.waiting_for_user());
    c.wait_for_user(Duration::from_secs(60));
    assert!(c.advance(Duration::from_secs(61)));
}

#[test]
fn repeated_declarations_do_not_leave_an_unfinishable_counter() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    at(&mut c, calls(&["a", "a", "b"]), 0);
    at(&mut c, calls(&["a", "b"]), 1);
    at(&mut c, result("a", true), 2);
    at(&mut c, cancel("b"), 3);
    assert!(!c.function_calls_in_progress());
    at(&mut c, FrameData::BotStoppedSpeaking, 4);
    let deadline = c.deadline();
    // A replayed declaration after completion must not close a current window.
    at(&mut c, calls(&["a", "b"]), 4);
    assert_eq!(c.deadline(), deadline);
    assert!(c.advance(Duration::from_secs(5)));
}

#[test]
fn detailed_intermediate_result_and_wrong_name_do_not_finish_a_call() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    at(
        &mut c,
        FrameData::FunctionCallsStartedDetailed(vec![FunctionCallFromLlm {
            function_name: "lookup".into(),
            tool_call_id: "a".into(),
            arguments: Default::default(),
            context: None,
        }]),
        0,
    );
    for (name, final_result) in [("lookup", false), ("wrong-function", true)] {
        at(
            &mut c,
            FrameData::FunctionCallResultDetailed(DetailedFunctionCallResultFrame {
                function_name: name.into(),
                tool_call_id: "a".into(),
                arguments: json!({}),
                result: json!("ok"),
                run_llm: Some(true),
                error: None,
                properties: Some(FunctionCallResultProperties {
                    is_final: final_result,
                    ..Default::default()
                }),
            }),
            1,
        );
        no_idle(&mut c, 1);
    }
    at(&mut c, cancel("a"), 20);
    assert!(!c.function_calls_in_progress());
}

#[test]
fn invalid_timeout_does_not_advance_the_aggregator_clock() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            user_idle_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    );
    assert!(user
        .process_at(
            Frame::new(FrameData::UserIdleTimeoutUpdate { timeout: f64::NAN }),
            Direction::Downstream,
            Duration::from_secs(100)
        )
        .is_err());
    user.process_at(
        Frame::new(FrameData::BotStoppedSpeaking),
        Direction::Downstream,
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(user.idle_deadline().unwrap(), Some(Duration::from_secs(2)));
}

#[test]
fn malformed_declaration_batches_are_atomic_and_use_native_call_contracts() {
    for invalid in [
        FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "".into(),
            arguments: json!({}),
        },
        FunctionCall {
            function_name: "".into(),
            tool_call_id: "bad".into(),
            arguments: json!({}),
        },
        FunctionCall {
            function_name: "lookup".into(),
            tool_call_id: "bad".into(),
            arguments: json!(null),
        },
    ] {
        let mut c = UserIdleController::new(Duration::from_secs(1));
        at(&mut c, FrameData::BotStoppedSpeaking, 0);
        let FrameData::FunctionCallsStarted(mut batch) = calls(&["valid"]) else {
            unreachable!()
        };
        batch.push(invalid);
        assert!(c
            .process(
                &FrameData::FunctionCallsStarted(batch),
                Duration::from_secs(100)
            )
            .is_err());
        assert!(!c.function_calls_in_progress());
        assert_eq!(c.deadline(), Some(Duration::from_secs(1)));
        assert!(!c.advance(Duration::ZERO));
    }
}

#[test]
fn conflicting_declarations_and_mismatched_result_arguments_leave_calls_pending() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    at(&mut c, calls(&["a"]), 0);
    let FrameData::FunctionCallsStarted(mut conflicting) = calls(&["b", "a"]) else {
        unreachable!()
    };
    conflicting[1].arguments = json!({"different": true});
    assert!(c
        .process(
            &FrameData::FunctionCallsStarted(conflicting),
            Duration::from_secs(1)
        )
        .is_err());
    let FrameData::FunctionCallResult(mut wrong) = result("a", true) else {
        unreachable!()
    };
    wrong.arguments = json!({"different": true});
    at(&mut c, FrameData::FunctionCallResult(wrong), 2);
    at(
        &mut c,
        FrameData::FunctionCallCancel(FunctionCallCancelFrame {
            function_name: "wrong".into(),
            tool_call_id: "a".into(),
            run_llm: false,
        }),
        2,
    );
    no_idle(&mut c, 3);
    at(&mut c, result("a", true), 20);
    assert!(!c.function_calls_in_progress()); // The rejected batch did not insert b.
}

#[test]
fn pending_call_capacity_never_evicts_an_active_guard() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    let ids: Vec<_> = (0..512).map(|id| id.to_string()).collect();
    let refs: Vec<_> = ids.iter().map(String::as_str).collect();
    at(&mut c, calls(&refs), 0);
    assert!(c
        .process(&calls(&["overflow"]), Duration::from_secs(1))
        .is_err());
    at(&mut c, result("overflow", true), 2);
    no_idle(&mut c, 3);
    for id in &ids {
        at(&mut c, result(id, true), 20);
    }
    assert!(!c.function_calls_in_progress());
    at(&mut c, calls(&["overflow"]), 21);
    assert!(c.function_calls_in_progress());
    at(&mut c, result("overflow", true), 22);
    assert!(!c.function_calls_in_progress());
}

#[test]
fn completed_replay_retention_is_bounded_to_512_identities() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    for index in 0..513 {
        let id = index.to_string();
        at(&mut c, calls(&[&id]), index * 2);
        at(&mut c, result(&id, true), index * 2 + 1);
    }
    at(&mut c, calls(&["1", "512"]), 2000);
    assert!(!c.function_calls_in_progress());
    // Old IDs outside the bounded replay window can be admitted again.
    at(&mut c, calls(&["0"]), 2001);
    assert!(c.function_calls_in_progress());
    at(&mut c, result("0", true), 2002);
    assert!(!c.function_calls_in_progress());
}

#[test]
fn empty_declarations_do_not_invalidate_a_waiting_window() {
    let mut c = UserIdleController::new(Duration::from_secs(1));
    at(&mut c, FrameData::BotStoppedSpeaking, 0);
    at(&mut c, calls(&[]), 0);
    at(&mut c, FrameData::FunctionCallsStartedDetailed(vec![]), 0);
    assert!(c.waiting_for_user());
    assert!(c.advance(Duration::from_secs(1)));
}

#[test]
fn a_typed_user_message_closes_idle_while_awaiting_its_bot_response() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            user_idle_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    );
    user.process_at(
        Frame::new(FrameData::BotStoppedSpeaking),
        Direction::Downstream,
        Duration::ZERO,
    )
    .unwrap();
    user.process_at(
        Frame::new(FrameData::InputText(TextFrame::new("question"))),
        Direction::Downstream,
        Duration::from_millis(500),
    )
    .unwrap();
    assert!(!user.waiting_for_user().unwrap());
    assert_eq!(user.idle_deadline().unwrap(), None);
    let out = user.advance(Duration::from_secs(2)).unwrap();
    assert!(!out.events.contains(&AggregationEvent::UserTurnIdle));
    assert_eq!(user.context().snapshot().unwrap().messages.len(), 1);
    user.process_at(
        Frame::new(FrameData::BotStoppedSpeaking),
        Direction::Downstream,
        Duration::from_secs(3),
    )
    .unwrap();
    assert_eq!(
        user.advance(Duration::from_secs(4)).unwrap().events,
        [AggregationEvent::UserTurnIdle]
    );
}

#[test]
fn rejected_declarations_do_not_advance_the_aggregator_clock() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            user_idle_timeout: Duration::from_secs(1),
            ..Default::default()
        },
    );
    assert!(user
        .process_at(
            Frame::new(calls(&[""])),
            Direction::Downstream,
            Duration::from_secs(100)
        )
        .is_err());
    user.process_at(
        Frame::new(FrameData::BotStoppedSpeaking),
        Direction::Downstream,
        Duration::from_secs(1),
    )
    .unwrap();
    assert_eq!(user.idle_deadline().unwrap(), Some(Duration::from_secs(2)));
}
