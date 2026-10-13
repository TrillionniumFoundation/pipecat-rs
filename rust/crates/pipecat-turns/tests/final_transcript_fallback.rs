//! No-VAD finalized-segment fallback at Python af9f9da9; public deterministic effects.
//! No audio, VAD, model verdict, runtime, or wall-clock sleeps in the baseline.
use pipecat_core::*;
use pipecat_services::{Message, Role};
use pipecat_turns::*;
use std::time::Duration;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn config(explicit: bool) -> UserTurnConfig {
    UserTurnConfig {
        speech_timeout: ms(0),
        stt_timeout: ms(0),
        vad_stop_duration: ms(0),
        stop_timeout: ms(5000),
        wait_for_transcript: true,
        start_on_transcription: true,
        resolve_proposals: false,
        enable_interruptions: false,
        require_model_completion: true,
        start_strategies: None,
        stop_strategies: explicit.then(|| {
            UserTurnStopStrategies::new(vec![UserTurnStopStrategy::turn_analyzer(true)]).unwrap()
        }),
    }
}
fn metadata() -> FrameData {
    FrameData::SttMetadata(SttMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "finalized-fallback-fixture".into(),
            user_turn_strategies: None,
        },
        ttfs_p99_latency: 0.8,
    })
}
fn transcript(text: &str, finalized: bool) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: text.into(),
        finalized,
        ..Default::default()
    })
}
fn started() -> Vec<UserTurnEvent> {
    vec![UserTurnEvent::Started {
        emit_speaking: true,
        interrupt: false,
    }]
}
fn stopped(reason: TurnStopReason) -> Vec<UserTurnEvent> {
    vec![UserTurnEvent::Stopped {
        emit_speaking: true,
        reason,
    }]
}
fn user(context: &SharedContext, explicit: bool) -> LLMUserAggregator {
    LLMUserAggregator::new(
        context.clone(),
        UserAggregatorConfig {
            turn: config(explicit),
            eager: None,
            initially_muted: false,
            user_idle_timeout: ms(0),
            mute_strategies: vec![],
            empty_user_turn: None,
        },
    )
}
fn process(user: &LLMUserAggregator, frame: Frame, now: u64) -> AggregationOutput {
    user.process_at(frame, Direction::Downstream, ms(now))
        .unwrap()
}
fn forwarded(frame: Frame) -> AggregationOutput {
    AggregationOutput {
        actions: vec![AggregationAction::Push(DirectedFrame::new(
            frame,
            Direction::Downstream,
        ))],
        events: vec![],
    }
}
fn assert_started(output: &AggregationOutput) {
    assert_eq!(output.events, vec![AggregationEvent::UserTurnStarted]);
    let [AggregationAction::Broadcast(boundary)] = output.actions.as_slice() else {
        panic!("one generated start, with no transcript forwarding: {output:?}");
    };
    assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
}
fn assert_committed(output: &AggregationOutput, text: &str, context: &FrameData) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::UserTurnStopped {
            text: text.into(),
            reason: TurnStopReason::SpeechTimeout,
        }]
    );
    let [AggregationAction::Broadcast(boundary), AggregationAction::Push(inference)] =
        output.actions.as_slice()
    else {
        panic!("one stop broadcast before one downstream context: {output:?}");
    };
    assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
    assert_eq!(
        boundary.metadata.get("user_context_follows"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(inference.direction, Direction::Downstream);
    assert_eq!(&inference.frame.data, context);
    assert_ne!(boundary.id, inference.frame.id);
}

#[test]
fn controller_second_final_releases_established_analyzer_fallback() {
    // Complete the explicit positive control before entering the scalar case.
    for explicit in [true, false] {
        let mut controller = UserTurnController::new(config(explicit));
        let metadata = controller.process(&metadata(), ms(0));
        let first = controller.process(&transcript("hello", true), ms(0));
        let first_deadline = controller.deadline();
        let before = controller.advance(ms(99));
        let second = controller.process(&transcript("world", true), ms(100));
        let second_deadline = controller.deadline();
        let at_first_due = controller.advance(ms(800));
        let before_old_due = controller.advance(ms(899));
        let at_old_due = controller.advance(ms(900));
        let later = controller.advance(ms(999));
        let terminal = controller.finish(false, TurnStopReason::SessionEnd);
        controller.cleanup();
        let after_cleanup = controller.advance(ms(10000));
        let final_deadline = controller.deadline();
        // Log captured phases before any semantic assertion can end this case.
        eprintln!("controller explicit={explicit} captured_through_cleanup: first={first:?}; first_deadline={first_deadline:?}; second_100ms={second:?}; second_deadline={second_deadline:?}; at_800ms={at_first_due:?}; at_900ms={at_old_due:?}; terminal={terminal:?}; after_cleanup={after_cleanup:?}");

        assert!(metadata.is_empty());
        assert_eq!(first, started());
        assert!(before.is_empty());
        // Primary old-red witness is an actual stop, before deadline assertions.
        assert_eq!(
            second,
            stopped(TurnStopReason::SpeechTimeout),
            "explicit={explicit}: second final must release at 100ms"
        );
        assert_eq!(first_deadline, Some(ms(800)), "first final still waits");
        assert_eq!(second_deadline, None);
        assert!(at_first_due.is_empty());
        assert!(before_old_due.is_empty());
        assert!(at_old_due.is_empty());
        assert!(later.is_empty());
        assert!(terminal.is_empty());
        assert!(after_cleanup.is_empty());
        assert_eq!(final_deadline, None);
        eprintln!("controller explicit={explicit} all_assertions_completed");
    }
}

#[test]
fn aggregator_second_final_commits_both_segments_once_at_release() {
    // Every assertion in the explicit case precedes starting the scalar case.
    for explicit in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, explicit);
        let metadata_frame = Frame::new(metadata());
        let metadata = process(&user, metadata_frame.clone(), 0);
        let hello = Frame::new(transcript("hello", true));
        let world = Frame::new(transcript("world", true));
        let first = process(&user, hello.clone(), 0);
        let first_deadline = user.deadline().unwrap();
        let first_history = context.snapshot().unwrap();
        let before = user.advance(ms(99)).unwrap();
        let second = process(&user, world.clone(), 100);
        let history_at_release = context.snapshot().unwrap();
        let context_at_release = context.frame_data().unwrap();
        let second_deadline = user.deadline().unwrap();
        let at_first_due = user.advance(ms(800)).unwrap();
        let before_old_due = user.advance(ms(899)).unwrap();
        let at_old_due = user.advance(ms(900)).unwrap();
        let later = user.advance(ms(999)).unwrap();
        let end_frame = Frame::new(FrameData::End(Default::default()));
        let end = process(&user, end_frame.clone(), 1000);
        let after_end = user.advance(ms(10000)).unwrap();
        let history = context.snapshot().unwrap();
        let retained = user.transcript_frames().unwrap();
        let final_deadline = user.deadline().unwrap();
        // Captured values remain visible on failure; later assertions are not passes.
        eprintln!("aggregator explicit={explicit} captured_through_End: first={first:?}; first_deadline={first_deadline:?}; second_100ms={second:?}; history_100ms={:?}; second_deadline={second_deadline:?}; at_800ms={at_first_due:?}; at_900ms={at_old_due:?}; End={end:?}; after_End={after_end:?}", history_at_release.messages);

        assert_eq!(metadata, forwarded(metadata_frame));
        assert_started(&first);
        assert!(first_history.messages.is_empty());
        assert_eq!(before, AggregationOutput::default());
        // Primary old-red witness is the actual semantic stop on this process call.
        assert_eq!(
            second.events,
            vec![AggregationEvent::UserTurnStopped {
                text: "hello world".into(),
                reason: TurnStopReason::SpeechTimeout
            }],
            "explicit={explicit}: commit must happen at 100ms"
        );
        assert_committed(&second, "hello world", &context_at_release);
        assert_eq!(
            history_at_release.messages,
            vec![Message::text(Role::User, "hello world")]
        );
        assert_eq!(first_deadline, Some(ms(800)));
        assert_eq!(second_deadline, None);
        assert_eq!(at_first_due, AggregationOutput::default());
        assert_eq!(before_old_due, AggregationOutput::default());
        assert_eq!(at_old_due, AggregationOutput::default());
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(end, forwarded(end_frame));
        assert_eq!(after_end, AggregationOutput::default());
        assert_eq!(history.messages, history_at_release.messages);
        assert_eq!(retained, vec![hello, world]);
        assert_eq!(final_deadline, None);
        eprintln!("aggregator explicit={explicit} all_assertions_completed");
    }
}

#[test]
fn first_final_alone_keeps_the_full_fallback_wait() {
    for explicit in [true, false] {
        let mut controller = UserTurnController::new(config(explicit));
        controller.process(&metadata(), ms(0));
        let first = controller.process(&transcript("hello", true), ms(0));
        let deadline = controller.deadline();
        let before = controller.advance(ms(799));
        let due = controller.advance(ms(800));
        let later = controller.advance(ms(900));
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        assert_eq!(first, started());
        assert_eq!(deadline, Some(ms(800)), "explicit={explicit}");
        assert!(before.is_empty());
        assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
        assert!(later.is_empty());
        assert!(cleaned.is_empty());
    }
}

#[test]
fn current_nonfinal_segment_refreshes_even_after_a_final_segment() {
    for explicit in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, explicit);
        process(&user, Frame::new(metadata()), 0);
        let first = process(&user, Frame::new(transcript("hello", true)), 0);
        let nonfinal = process(&user, Frame::new(transcript("world", false)), 100);
        let refreshed = user.deadline().unwrap();
        let old_due = user.advance(ms(800)).unwrap();
        let before = user.advance(ms(899)).unwrap();
        let uncommitted = context.snapshot().unwrap();
        let due = user.advance(ms(900)).unwrap();
        let committed = context.frame_data().unwrap();
        let history = context.snapshot().unwrap();
        let later = user.advance(ms(999)).unwrap();
        let end_frame = Frame::new(FrameData::End(Default::default()));
        let end = process(&user, end_frame.clone(), 1000);
        let cleaned = user.advance(ms(10000)).unwrap();
        assert_started(&first);
        assert_eq!(nonfinal, AggregationOutput::default());
        assert_eq!(refreshed, Some(ms(900)), "explicit={explicit}");
        assert_eq!(old_due, AggregationOutput::default());
        assert_eq!(before, AggregationOutput::default());
        assert!(uncommitted.messages.is_empty());
        assert_committed(&due, "hello world", &committed);
        assert_eq!(
            history.messages,
            vec![Message::text(Role::User, "hello world")]
        );
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(end, forwarded(end_frame));
        assert_eq!(cleaned, AggregationOutput::default());
    }
}

#[test]
fn final_after_interim_releases_fallback_without_committing_the_preview() {
    for explicit in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, explicit);
        process(&user, Frame::new(metadata()), 0);
        let hello = Frame::new(transcript("hello", true));
        let interim = Frame::new(FrameData::InterimTranscription(TranscriptionFrame {
            text: "uncommitted hypothesis".into(),
            finalized: false,
            ..Default::default()
        }));
        let world = Frame::new(transcript("world", true));
        let first = process(&user, hello.clone(), 0);
        let preview_output = process(&user, interim.clone(), 50);
        let preview = user.interim_text().unwrap();
        let before = user.advance(ms(99)).unwrap();
        let due = process(&user, world.clone(), 100);
        let committed = context.frame_data().unwrap();
        let after_preview = user.interim_text().unwrap();
        let history = context.snapshot().unwrap();
        let later = user.advance(ms(900)).unwrap();
        let retained = user.transcript_frames().unwrap();
        let end_frame = Frame::new(FrameData::End(Default::default()));
        let end = process(&user, end_frame.clone(), 1000);
        let cleaned = user.advance(ms(10000)).unwrap();
        assert_started(&first);
        assert_eq!(preview_output, AggregationOutput::default());
        assert_eq!(preview, "uncommitted hypothesis");
        assert_eq!(before, AggregationOutput::default());
        assert_committed(&due, "hello world", &committed);
        assert!(after_preview.is_empty());
        assert_eq!(
            history.messages,
            vec![Message::text(Role::User, "hello world")]
        );
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(retained, vec![hello, interim, world]);
        assert_eq!(end, forwarded(end_frame));
        assert_eq!(cleaned, AggregationOutput::default());
    }
}

fn vad_start() -> FrameData {
    FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    })
}
fn vad_stop() -> FrameData {
    FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
        stop_secs: 0.0,
        timestamp: 0.0,
    })
}

#[test]
fn resumed_vad_requires_current_model_completion_instead_of_old_fallback() {
    for explicit in [true, false] {
        let mut controller = UserTurnController::new(config(explicit));
        controller.process(&metadata(), ms(0));
        let first = controller.process(&transcript("hello", true), ms(0));
        let resumed = controller.process(&vad_start(), ms(50));
        let while_speaking = controller.process(&transcript("world", true), ms(100));
        let speaking_deadline = controller.deadline();
        let stopped_vad = controller.process(&vad_stop(), ms(120));
        let old_due = controller.advance(ms(800));
        let physical_due = controller.advance(ms(920));
        let verdict = controller.mark_model_complete(ms(950));
        let duplicate = controller.mark_model_complete(ms(960));
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        assert_eq!(first, started());
        assert!(resumed.is_empty());
        assert!(while_speaking.is_empty());
        assert_eq!(speaking_deadline, None);
        assert!(stopped_vad.is_empty());
        assert!(old_due.is_empty());
        assert!(
            physical_due.is_empty(),
            "explicit={explicit}: no fabricated model verdict"
        );
        assert_eq!(verdict, stopped(TurnStopReason::ModelCompletion));
        assert!(duplicate.is_empty());
        assert!(cleaned.is_empty());
    }
}

#[test]
fn pending_fallback_obeys_mute_end_stop_and_cancel_commit_contracts() {
    for explicit in [true, false] {
        for signal in ["mute", "end", "stop", "cancel"] {
            let context = SharedContext::default();
            let user = user(&context, explicit);
            process(&user, Frame::new(metadata()), 0);
            let first = process(&user, Frame::new(transcript("hello", true)), 0);
            let boundary = Frame::new(match signal {
                "mute" => FrameData::UserMuteStarted,
                "end" => FrameData::End(Default::default()),
                "stop" => FrameData::Stop,
                "cancel" => FrameData::Cancel(Default::default()),
                _ => unreachable!(),
            });
            let terminal = process(&user, boundary.clone(), 50);
            let committed = context.frame_data().unwrap();
            let late_final = process(&user, Frame::new(transcript("world", true)), 100);
            let old_due = user.advance(ms(800)).unwrap();
            let later = user.advance(ms(900)).unwrap();
            let history = context.snapshot().unwrap();
            let no_deadline = user.deadline().unwrap();
            let end_frame = Frame::new(FrameData::End(Default::default()));
            let cleanup = process(&user, end_frame.clone(), 1000);
            let after_cleanup = user.advance(ms(10000)).unwrap();
            assert_started(&first);
            assert_eq!(
                terminal.events,
                vec![AggregationEvent::UserTurnStopped {
                    text: "hello".into(),
                    reason: if signal == "mute" {
                        TurnStopReason::Muted
                    } else {
                        TurnStopReason::SessionEnd
                    },
                }],
                "explicit={explicit}, signal={signal}"
            );
            match signal {
                "mute" => {
                    let [AggregationAction::Broadcast(stop), AggregationAction::Push(original)] =
                        terminal.actions.as_slice()
                    else {
                        panic!("mute commits locally and sends stop before mute: {terminal:?}");
                    };
                    assert_eq!(stop.data, FrameData::UserStoppedSpeaking);
                    assert_eq!(
                        stop.metadata.get("user_context_follows"),
                        Some(&serde_json::Value::Bool(false))
                    );
                    assert_eq!(
                        original,
                        &DirectedFrame::new(boundary, Direction::Downstream)
                    );
                }
                "end" | "stop" => {
                    let [AggregationAction::Push(inference), AggregationAction::Push(original)] =
                        terminal.actions.as_slice()
                    else {
                        panic!("End/Stop flush one context before the terminal: {terminal:?}");
                    };
                    assert_eq!(inference.direction, Direction::Downstream);
                    assert_eq!(inference.frame.data, committed);
                    assert_eq!(
                        original,
                        &DirectedFrame::new(boundary, Direction::Downstream)
                    );
                }
                "cancel" => {
                    assert_eq!(terminal.actions, forwarded(boundary).actions);
                }
                _ => unreachable!(),
            }
            assert_eq!(late_final, AggregationOutput::default());
            assert_eq!(old_due, AggregationOutput::default());
            assert_eq!(later, AggregationOutput::default());
            assert_eq!(history.messages, vec![Message::text(Role::User, "hello")]);
            assert_eq!(no_deadline, None);
            assert_eq!(cleanup, forwarded(end_frame));
            assert_eq!(after_cleanup, AggregationOutput::default());
        }
    }
}

fn identified(text: &str, transcript_id: &str) -> Frame {
    let mut frame = Frame::new(transcript(text, true));
    frame
        .metadata
        .insert("transcript_id".into(), transcript_id.into());
    frame
}

#[test]
fn replays_do_not_release_fallback_and_next_turn_has_its_own_initial_wait() {
    for explicit in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, explicit);
        process(&user, Frame::new(metadata()), 0);
        let hello = identified("hello", "a");
        let world = identified("world", "b");
        let first = process(&user, hello.clone(), 0);
        let duplicate_frame = process(&user, hello.clone(), 50);
        let duplicate_transcript = process(&user, identified("must not enter history", "a"), 80);
        let first_deadline = user.deadline().unwrap();
        let due = process(&user, world.clone(), 100);
        let first_context = context.frame_data().unwrap();
        let replay_after_commit = process(&user, world.clone(), 200);
        let replay_id_after_commit = process(&user, identified("must not restart", "b"), 250);
        let old_due = user.advance(ms(900)).unwrap();
        let again = identified("again", "c");
        let now = identified("now", "d");
        let next_start = process(&user, again.clone(), 1000);
        let next_deadline = user.deadline().unwrap();
        let before = user.advance(ms(1099)).unwrap();
        let history_before_second = context.snapshot().unwrap();
        let next_due = process(&user, now.clone(), 1100);
        let next_context = context.frame_data().unwrap();
        let later = user.advance(ms(1900)).unwrap();
        let end_frame = Frame::new(FrameData::End(Default::default()));
        let end = process(&user, end_frame.clone(), 2000);
        let after_end = process(&user, identified("must not restart after End", "e"), 2100);
        let cleaned = user.advance(ms(10000)).unwrap();
        let retained = user.transcript_frames().unwrap();
        let history = context.snapshot().unwrap();
        assert_started(&first);
        assert_eq!(duplicate_frame, AggregationOutput::default());
        assert_eq!(duplicate_transcript, AggregationOutput::default());
        assert_eq!(first_deadline, Some(ms(800)));
        assert_committed(&due, "hello world", &first_context);
        assert_eq!(replay_after_commit, AggregationOutput::default());
        assert_eq!(replay_id_after_commit, AggregationOutput::default());
        assert_eq!(old_due, AggregationOutput::default());
        assert_started(&next_start);
        assert_eq!(
            next_deadline,
            Some(ms(1800)),
            "explicit={explicit}: new turn must not inherit readiness"
        );
        assert_eq!(before, AggregationOutput::default());
        assert_eq!(
            history_before_second.messages,
            vec![Message::text(Role::User, "hello world")]
        );
        assert_committed(&next_due, "again now", &next_context);
        assert_eq!(
            history.messages,
            vec![
                Message::text(Role::User, "hello world"),
                Message::text(Role::User, "again now")
            ]
        );
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(end, forwarded(end_frame));
        assert_eq!(after_end, AggregationOutput::default());
        assert_eq!(cleaned, AggregationOutput::default());
        assert_eq!(retained, vec![hello, world, again, now]);
    }
}

#[test]
fn scalar_final_releases_only_stt_and_preserves_a_nonzero_speech_floor() {
    let mut config = config(false);
    config.speech_timeout = ms(150);
    let mut controller = UserTurnController::new(config);
    controller.process(&metadata(), ms(0));
    let first = controller.process(&transcript("hello", true), ms(0));
    let first_deadline = controller.deadline();
    let second = controller.process(&transcript("world", true), ms(100));
    let second_deadline = controller.deadline();
    let before_floor = controller.advance(ms(249));
    let floor = controller.advance(ms(250));
    let old_due = controller.advance(ms(900));
    controller.cleanup();
    let cleaned = controller.advance(ms(10000));
    assert_eq!(first, started());
    assert_eq!(first_deadline, Some(ms(800)));
    assert!(second.is_empty());
    assert_eq!(second_deadline, Some(ms(250)));
    assert!(before_floor.is_empty());
    assert_eq!(floor, stopped(TurnStopReason::SpeechTimeout));
    assert!(old_due.is_empty());
    assert!(cleaned.is_empty());
}

#[test]
fn scalar_prior_verdict_is_not_a_fallback_and_optional_text_model_release_is_unchanged() {
    // A public verdict can set speech_deadline without any accepted segment.
    // Preserve the existing scalar first-segment wait instead of inferring fallback.
    let mut controller = UserTurnController::new(config(false));
    controller.process(&metadata(), ms(0));
    let interim = FrameData::InterimTranscription(TranscriptionFrame {
        text: "preview".into(),
        finalized: false,
        ..Default::default()
    });
    let first = controller.process(&interim, ms(0));
    let verdict_without_text = controller.mark_model_complete(ms(50));
    let first_final = controller.process(&transcript("hello", true), ms(100));
    let fallback_deadline = controller.deadline();
    let before = controller.advance(ms(899));
    let due = controller.advance(ms(900));
    controller.cleanup();
    let cleaned = controller.advance(ms(10000));
    assert_eq!(first, started());
    assert!(verdict_without_text.is_empty());
    assert!(first_final.is_empty());
    assert_eq!(fallback_deadline, Some(ms(900)));
    assert!(before.is_empty());
    assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
    assert!(cleaned.is_empty());

    let mut config = config(false);
    config.wait_for_transcript = false;
    let mut controller = UserTurnController::new(config);
    controller.process(&metadata(), ms(0));
    let started_vad = controller.process(&vad_start(), ms(0));
    let stopped_vad = controller.process(&vad_stop(), ms(20));
    let no_verdict = controller.advance(ms(30));
    let verdict = controller.mark_model_complete(ms(40));
    let old_due = controller.advance(ms(820));
    controller.cleanup();
    assert_eq!(started_vad, started());
    assert!(stopped_vad.is_empty());
    assert!(no_verdict.is_empty());
    assert_eq!(verdict, stopped(TurnStopReason::ModelCompletion));
    assert!(old_due.is_empty());
}
