//! Native VAD stop duration is observed silence, not utterance length.
//! Source: VADUserStoppedSpeakingFrame and speech_timeout_user_turn_stop_strategy.py
//! at Python af9f9da9. All traces use caller-owned monotonic time and timestamp zero.
use pipecat_core::*;
use pipecat_services::{Content, Message, Role};
use pipecat_turns::*;
use std::time::Duration;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn config(explicit: bool) -> UserTurnConfig {
    UserTurnConfig {
        speech_timeout: ms(100),
        stt_timeout: ms(800),
        stop_timeout: ms(5000),
        enable_interruptions: false,
        stop_strategies: explicit.then(|| {
            UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(ms(100), true)])
                .unwrap()
        }),
        ..Default::default()
    }
}
fn start() -> FrameData {
    FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    })
}
fn stop(detailed: bool, seconds: f64) -> FrameData {
    if detailed {
        FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
            stop_secs: seconds,
            timestamp: 0.0,
        })
    } else {
        FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: seconds,
            timestamp: 0.0,
        })
    }
}
fn transcript() -> Frame {
    Frame::new(FrameData::Transcription(TranscriptionFrame {
        text: "hello".into(),
        finalized: false,
        ..Default::default()
    }))
}
fn stopped() -> Vec<UserTurnEvent> {
    vec![UserTurnEvent::Stopped {
        emit_speaking: true,
        reason: TurnStopReason::SpeechTimeout,
    }]
}
fn user(context: &SharedContext, explicit: bool) -> LLMUserAggregator {
    LLMUserAggregator::new(
        context.clone(),
        UserAggregatorConfig {
            turn: config(explicit),
            ..Default::default()
        },
    )
}
fn process(
    user: &LLMUserAggregator,
    frame: Frame,
    direction: Direction,
    now: u64,
) -> AggregationOutput {
    user.process_at(frame, direction, ms(now)).unwrap()
}
fn assert_context_stop(output: &AggregationOutput, context: &FrameData) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::UserTurnStopped {
            text: "hello".into(),
            reason: TurnStopReason::SpeechTimeout,
        }]
    );
    let [AggregationAction::Broadcast(boundary), AggregationAction::Push(history)] =
        output.actions.as_slice()
    else {
        panic!("one speaking boundary then one context action: {output:?}");
    };
    assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
    assert_eq!(
        boundary.metadata.get("user_context_follows"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(history.direction, Direction::Downstream);
    assert_eq!(&history.frame.data, context);
}

#[test]
fn controller_consumes_legacy_stop_duration_at_the_detailed_deadline() {
    // The Detailed positive control must complete before the legacy assertion.
    for detailed in [true, false] {
        let mut controller = UserTurnController::new(config(true));
        let started = controller.process(&start(), ms(0));
        let text = controller.process(&transcript().data, ms(10));
        let stop_output = controller.process(&stop(detailed, 0.5), ms(20));
        let before = controller.advance(ms(319));
        let due = controller.advance(ms(320));
        let later = controller.advance(ms(620));
        let finish = controller.finish(false, TurnStopReason::SessionEnd);
        controller.cleanup();
        let after_cleanup = controller.advance(ms(10000));

        assert_eq!(
            started,
            vec![UserTurnEvent::Started {
                emit_speaking: true,
                interrupt: false,
            }]
        );
        assert!(text.is_empty());
        assert!(stop_output.is_empty());
        assert!(before.is_empty());
        assert_eq!(due, stopped(), "detailed={detailed}: stop at 320ms");
        assert!(later.is_empty(), "a stop must be emitted exactly once");
        assert!(finish.is_empty());
        assert!(after_cleanup.is_empty());
    }
}

#[test]
fn aggregator_commits_legacy_stop_at_the_detailed_deadline() {
    // As above, a full observable Detailed success precedes the legacy failure.
    for detailed in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, true);
        process(&user, Frame::new(start()), Direction::Downstream, 0);
        let transcript = transcript();
        let accepted = process(&user, transcript.clone(), Direction::Downstream, 10);
        let mut stop_frame = Frame::new(stop(detailed, 0.5));
        stop_frame.interruptible = false;
        stop_frame
            .metadata
            .insert("trace".into(), "vad-stop".into());
        let forwarded = process(&user, stop_frame.clone(), Direction::Upstream, 20);
        let before = user.advance(ms(319)).unwrap();
        let history_before = context.snapshot().unwrap();
        let due = user.advance(ms(320)).unwrap();
        let history_due = context.snapshot().unwrap();
        let context_due = context.frame_data().unwrap();
        let transcripts = user.transcript_frames().unwrap();
        let later = user.advance(ms(620)).unwrap();
        let end_frame = Frame::new(FrameData::End(Default::default()));
        let end = process(&user, end_frame.clone(), Direction::Downstream, 621);
        let after_end = user.advance(ms(10000)).unwrap();
        let history_after_end = context.snapshot().unwrap();

        assert_eq!(accepted, AggregationOutput::default());
        assert_eq!(
            forwarded,
            AggregationOutput {
                actions: vec![AggregationAction::Push(DirectedFrame::new(
                    stop_frame,
                    Direction::Upstream,
                ))],
                events: vec![],
            }
        );
        assert_eq!(before, AggregationOutput::default());
        assert!(history_before.messages.is_empty());
        assert_context_stop(&due, &context_due);
        assert_eq!(
            history_due.messages,
            vec![Message::text(Role::User, "hello")]
        );
        assert_eq!(
            history_due.messages[0].content,
            Some(Content::Text("hello".into()))
        );
        assert_eq!(transcripts, vec![transcript]);
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(
            end,
            AggregationOutput {
                actions: vec![AggregationAction::Push(DirectedFrame::new(
                    end_frame,
                    Direction::Downstream,
                ))],
                events: vec![],
            }
        );
        assert_eq!(after_end, AggregationOutput::default());
        assert_eq!(history_after_end.messages, history_due.messages);
    }
}

#[test]
fn legacy_delay_respects_stt_subtraction_and_policy_floor() {
    // Below, equal to, and above the STT budget, under both public policy routes.
    for explicit in [false, true] {
        for (seconds, deadline) in [(0.05, 770), (0.8, 120), (1.2, 120)] {
            let mut controller = UserTurnController::new(config(explicit));
            controller.process(&start(), ms(0));
            controller.process(&transcript().data, ms(10));
            let at_stop = controller.process(&stop(false, seconds), ms(20));
            let before = controller.advance(ms(deadline - 1));
            let due = controller.advance(ms(deadline));
            let later = controller.advance(ms(6000));
            controller.cleanup();
            let cleaned = controller.advance(ms(10000));
            assert!(at_stop.is_empty());
            assert!(before.is_empty(), "explicit={explicit}, seconds={seconds}");
            assert_eq!(due, stopped(), "explicit={explicit}, seconds={seconds}");
            assert!(later.is_empty());
            assert!(cleaned.is_empty());
        }
    }
}

#[test]
fn mixed_stop_updates_replace_the_effective_delay_and_survive_new_turns() {
    let mut controller = UserTurnController::new(config(false));
    controller.process(&start(), ms(0));
    controller.process(&transcript().data, ms(10));
    let first = controller.process(&stop(false, 0.5), ms(20));
    let detailed = controller.process(&stop(true, 0.05), ms(100));
    let superseded_deadline = controller.advance(ms(320));
    let legacy = controller.process(&stop(false, 1.2), ms(400));
    let before = controller.advance(ms(499));
    let due = controller.advance(ms(500));
    let first_cleanup = controller.finish(false, TurnStopReason::SessionEnd);
    // Invalid direct input must still use the retained last valid delay (1.2s).
    controller.process(&start(), ms(600));
    controller.process(&transcript().data, ms(610));
    let invalid = controller.process(&stop(true, f64::NAN), ms(620));
    let next_before = controller.advance(ms(719));
    let next_due = controller.advance(ms(720));
    controller.cleanup();
    controller.reset();
    // reset keeps the effective config and starts a fresh monotonic-clock session.
    controller.process(&start(), ms(0));
    controller.process(&transcript().data, ms(10));
    let reset_stop = controller.process(&stop(false, f64::NAN), ms(20));
    let reset_before = controller.advance(ms(119));
    let reset_due = controller.advance(ms(120));
    controller.cleanup();
    let cleaned = controller.advance(ms(10000));
    for output in [
        first,
        detailed,
        superseded_deadline,
        legacy,
        before,
        first_cleanup,
        invalid,
        next_before,
        reset_stop,
        reset_before,
        cleaned,
    ] {
        assert!(output.is_empty());
    }
    assert_eq!(due, stopped());
    assert_eq!(next_due, stopped());
    assert_eq!(reset_due, stopped());
}

#[test]
fn resumed_speech_cancels_old_deadline_until_a_new_stop() {
    let context = SharedContext::default();
    let user = user(&context, false);
    process(&user, Frame::new(start()), Direction::Downstream, 0);
    let text = transcript();
    process(&user, text.clone(), Direction::Downstream, 10);
    process(
        &user,
        Frame::new(stop(true, 0.5)),
        Direction::Downstream,
        20,
    );
    process(&user, Frame::new(start()), Direction::Upstream, 200);
    let superseded = user.advance(ms(320)).unwrap();
    let history_while_speaking = context.snapshot().unwrap();
    let new_stop = Frame::new(stop(false, 0.05));
    let forwarded = process(&user, new_stop.clone(), Direction::Downstream, 400);
    let before = user.advance(ms(1149)).unwrap();
    let due = user.advance(ms(1150)).unwrap();
    let expected_context = context.frame_data().unwrap();
    let history = context.snapshot().unwrap();
    let transcripts = user.transcript_frames().unwrap();
    let later = user.advance(ms(1200)).unwrap();
    process(
        &user,
        Frame::new(FrameData::Stop),
        Direction::Upstream,
        1201,
    );
    let cleaned = user.advance(ms(10000)).unwrap();
    assert_eq!(superseded, AggregationOutput::default());
    assert!(history_while_speaking.messages.is_empty());
    assert_eq!(
        forwarded,
        AggregationOutput {
            actions: vec![AggregationAction::Push(DirectedFrame::new(
                new_stop,
                Direction::Downstream
            ))],
            events: vec![],
        }
    );
    assert_eq!(before, AggregationOutput::default());
    assert_context_stop(&due, &expected_context);
    assert_eq!(history.messages, vec![Message::text(Role::User, "hello")]);
    assert_eq!(transcripts, vec![text]);
    assert_eq!(later, AggregationOutput::default());
    assert_eq!(cleaned, AggregationOutput::default());
}

#[test]
fn invalid_direct_legacy_duration_keeps_previous_timing_but_still_stops_speech() {
    for seconds in [-0.1, f64::NAN, f64::INFINITY, f64::NEG_INFINITY, 86400.1] {
        let mut controller = UserTurnController::new(config(true));
        controller.process(&start(), ms(0));
        controller.process(&transcript().data, ms(10));
        controller.process(&stop(true, 0.5), ms(20));
        controller.process(&start(), ms(100));
        let invalid = controller.process(&stop(false, seconds), ms(200));
        let before = controller.advance(ms(499));
        let due = controller.advance(ms(500));
        let later = controller.advance(ms(800));
        controller.cleanup();
        assert!(invalid.is_empty());
        assert!(before.is_empty());
        // Invalid duration is not adopted; the stop still uses retained 500ms.
        assert_eq!(due, stopped(), "seconds={seconds}");
        assert!(later.is_empty());
    }
}

#[test]
fn strict_aggregator_rejects_invalid_stops_without_poisoning_repair() {
    // Representative invalid fields across both variants, all existing bounds.
    for (detailed, seconds, timestamp) in [
        (false, -0.1, 0.0),
        (false, f64::NAN, 0.0),
        (true, f64::INFINITY, 0.0),
        (false, f64::NEG_INFINITY, 0.0),
        (false, 86400.1, 0.0),
        (false, 0.5, -0.1),
        (true, 0.5, f64::NAN),
    ] {
        let context = SharedContext::default();
        let user = user(&context, true);
        process(&user, Frame::new(start()), Direction::Downstream, 0);
        let text = transcript();
        process(&user, text.clone(), Direction::Downstream, 10);
        process(
            &user,
            Frame::new(stop(true, 0.5)),
            Direction::Downstream,
            20,
        );
        let mut payload = stop(detailed, seconds);
        match &mut payload {
            FrameData::VadUserStoppedSpeaking(value) => value.timestamp = timestamp,
            FrameData::VadUserStoppedSpeakingDetailed(value) => value.timestamp = timestamp,
            _ => unreachable!(),
        }
        // A far-future rejected frame may not advance the accepted session clock.
        let rejected = user.process_at(Frame::new(payload), Direction::Upstream, ms(9000));
        let before = user.advance(ms(319)).unwrap();
        let due = user.advance(ms(320)).unwrap();
        let expected_context = context.frame_data().unwrap();
        let first_history = context.snapshot().unwrap();
        // Repair in the same session with a fresh valid legacy stop at the lower bound.
        process(&user, Frame::new(start()), Direction::Downstream, 400);
        let repaired_text = transcript();
        process(&user, repaired_text.clone(), Direction::Downstream, 410);
        process(
            &user,
            Frame::new(stop(false, 0.0)),
            Direction::Downstream,
            420,
        );
        let repair_before = user.advance(ms(1219)).unwrap();
        let repair_due = user.advance(ms(1220)).unwrap();
        let repaired_context = context.frame_data().unwrap();
        let repaired_history = context.snapshot().unwrap();
        let transcripts = user.transcript_frames().unwrap();
        process(
            &user,
            Frame::new(FrameData::End(Default::default())),
            Direction::Downstream,
            1221,
        );
        let cleaned = user.advance(ms(10000)).unwrap();
        assert!(rejected.is_err(), "invalid VAD must fail strict ingress");
        assert_eq!(before, AggregationOutput::default());
        assert_context_stop(&due, &expected_context);
        assert_eq!(
            first_history.messages,
            vec![Message::text(Role::User, "hello")]
        );
        assert_eq!(repair_before, AggregationOutput::default());
        assert_context_stop(&repair_due, &repaired_context);
        assert_eq!(
            repaired_history.messages,
            vec![Message::text(Role::User, "hello"); 2]
        );
        assert_eq!(transcripts, vec![text, repaired_text]);
        assert_eq!(cleaned, AggregationOutput::default());
    }
}

#[test]
fn terminal_boundaries_flush_once_and_never_rearm_legacy_stop() {
    for terminal in [
        FrameData::End(Default::default()),
        FrameData::EndDetailed(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
        FrameData::CancelDetailed(Default::default()),
    ] {
        let cancelled = matches!(
            terminal,
            FrameData::Cancel(_) | FrameData::CancelDetailed(_)
        );
        let context = SharedContext::default();
        let user = user(&context, false);
        process(&user, Frame::new(start()), Direction::Downstream, 0);
        let text = transcript();
        process(&user, text.clone(), Direction::Downstream, 10);
        // Maximum accepted delay saturates the STT wait; terminal wins before floor.
        process(
            &user,
            Frame::new(stop(false, 86400.0)),
            Direction::Upstream,
            20,
        );
        let terminal = Frame::new(terminal);
        let end = process(&user, terminal.clone(), Direction::Upstream, 30);
        let history = context.snapshot().unwrap();
        let expected_context = context.frame_data().unwrap();
        let transcripts = user.transcript_frames().unwrap();
        let late_start = process(&user, Frame::new(start()), Direction::Downstream, 40);
        let late_stop = process(
            &user,
            Frame::new(stop(false, 0.5)),
            Direction::Downstream,
            50,
        );
        let due = user.advance(ms(120)).unwrap();
        let late = user.advance(ms(10000)).unwrap();
        let final_history = context.snapshot().unwrap();
        assert_eq!(
            end.events,
            vec![AggregationEvent::UserTurnStopped {
                text: "hello".into(),
                reason: TurnStopReason::SessionEnd,
            }]
        );
        let expected_actions = if cancelled { 1 } else { 2 };
        assert_eq!(end.actions.len(), expected_actions);
        assert_eq!(
            end.actions.last(),
            Some(&AggregationAction::Push(DirectedFrame::new(
                terminal,
                Direction::Upstream,
            )))
        );
        if !cancelled {
            let AggregationAction::Push(history_action) = &end.actions[0] else {
                panic!("context action")
            };
            assert_eq!(history_action.direction, Direction::Downstream);
            assert_eq!(history_action.frame.data, expected_context);
        }
        assert_eq!(history.messages, vec![Message::text(Role::User, "hello")]);
        assert_eq!(transcripts, vec![text]);
        for output in [late_start, late_stop, due, late] {
            assert_eq!(output, AggregationOutput::default());
        }
        assert_eq!(final_history.messages, history.messages);
    }
}
