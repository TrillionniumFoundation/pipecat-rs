//! Scalar speech-timeout compatibility with explicit SpeechTimeout at Python af9f9da9.
//! Caller-owned monotonic clocks, zero VAD timestamps, and no model/runtime sleeps.
use pipecat_core::*;
use pipecat_turns::*;
use std::time::Duration;

fn ms(n: u64) -> Duration {
    Duration::from_millis(n)
}
fn config(explicit: bool) -> UserTurnConfig {
    UserTurnConfig {
        speech_timeout: ms(100),
        stop_timeout: ms(5000),
        wait_for_transcript: false,
        require_model_completion: false,
        enable_interruptions: false,
        stop_strategies: explicit.then(|| {
            UserTurnStopStrategies::new(vec![UserTurnStopStrategy::speech_timeout(ms(100), false)])
                .unwrap()
        }),
        ..Default::default()
    }
}
fn metadata(seconds: f64) -> FrameData {
    FrameData::SttMetadata(SttMetadataFrame {
        base: ServiceMetadataFrame {
            service_name: "budget-fixture".into(),
            user_turn_strategies: None,
        },
        ttfs_p99_latency: seconds,
    })
}
fn start() -> FrameData {
    FrameData::VadUserStartedSpeaking(VadSpeakingFrame {
        duration_secs: 0.2,
        timestamp: 0.0,
    })
}
fn stop() -> FrameData {
    FrameData::VadUserStoppedSpeakingDetailed(VadUserStoppedSpeakingFrame {
        stop_secs: 0.2,
        timestamp: 0.0,
    })
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
            user_idle_timeout: ms(200),
            empty_user_turn: None,
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
fn tagged(data: FrameData, tag: &str) -> Frame {
    let mut frame = Frame::new(data);
    frame.interruptible = false;
    frame.metadata.insert("trace".into(), tag.into());
    frame
}
fn forwarded(frame: Frame, direction: Direction) -> AggregationOutput {
    AggregationOutput {
        actions: vec![AggregationAction::Push(DirectedFrame::new(
            frame, direction,
        ))],
        events: vec![],
    }
}
fn assert_empty_stop(output: &AggregationOutput, reason: TurnStopReason) {
    assert_eq!(
        output.events,
        vec![AggregationEvent::UserTurnStopped {
            text: String::new(),
            reason,
        }]
    );
    let [AggregationAction::Broadcast(boundary)] = output.actions.as_slice() else {
        panic!("one speaking boundary, no context or inference action: {output:?}");
    };
    assert_eq!(boundary.data, FrameData::UserStoppedSpeaking);
    assert_eq!(
        boundary.metadata.get("user_context_follows"),
        Some(&serde_json::Value::Bool(false))
    );
}

#[test]
fn controller_scalar_keeps_the_explicit_speech_stt_safety_wait() {
    // The complete explicit-policy success is asserted before the scalar witness.
    for explicit in [true, false] {
        let mut controller = UserTurnController::new(config(explicit));
        let metadata = controller.process(&metadata(0.8), ms(0));
        let started = controller.process(&start(), ms(0));
        let vad_stop = controller.process(&stop(), ms(20));
        let deadline = controller.deadline();
        let before_floor = controller.advance(ms(119));
        let old_scalar_due = controller.advance(ms(120));
        let before_safety = controller.advance(ms(619));
        let due = controller.advance(ms(620));
        let later = controller.advance(ms(621));
        let finish = controller.finish(false, TurnStopReason::SessionEnd);
        controller.cleanup();
        let after_cleanup = controller.advance(ms(10000));

        assert!(metadata.is_empty());
        assert_eq!(
            started,
            vec![UserTurnEvent::Started {
                emit_speaking: true,
                interrupt: false,
            }]
        );
        assert!(vad_stop.is_empty());
        assert!(before_floor.is_empty());
        assert!(
            old_scalar_due.is_empty(),
            "explicit={explicit}: no stop before the STT safety budget; got {old_scalar_due:?}"
        );
        assert_eq!(deadline, Some(ms(620)), "explicit={explicit}");
        assert!(before_safety.is_empty());
        assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
        assert!(later.is_empty(), "a turn may stop only once");
        assert!(finish.is_empty());
        assert!(after_cleanup.is_empty());
    }
}

#[test]
fn aggregator_scalar_rearms_empty_turn_idle_only_after_the_stt_wait() {
    // This positive control includes real stop/idle outputs, not just getters.
    for explicit in [true, false] {
        let context = SharedContext::default();
        let user = user(&context, explicit);
        let metadata_frame = tagged(metadata(0.8), "stt");
        let metadata = process(&user, metadata_frame.clone(), Direction::Upstream, 0);
        let start_frame = tagged(start(), "start");
        let started = process(&user, start_frame.clone(), Direction::Downstream, 0);
        let stop_frame = tagged(stop(), "stop");
        let vad_stop = process(&user, stop_frame.clone(), Direction::Upstream, 20);
        let deadline = user.deadline().unwrap();
        let before_floor = user.advance(ms(119)).unwrap();
        let old_scalar_due = user.advance(ms(120)).unwrap();
        let old_scalar_idle = user.idle_deadline().unwrap();
        let history_at_floor = context.snapshot().unwrap();
        let before_safety = user.advance(ms(619)).unwrap();
        let due = user.advance(ms(620)).unwrap();
        let idle_due = user.idle_deadline().unwrap();
        let later = user.advance(ms(621)).unwrap();
        let before_idle = user.advance(ms(819)).unwrap();
        let idle = user.advance(ms(820)).unwrap();
        let idle_again = user.advance(ms(821)).unwrap();
        let end_frame = tagged(FrameData::End(Default::default()), "end");
        let end = process(&user, end_frame.clone(), Direction::Downstream, 900);
        let after_end = user.advance(ms(10000)).unwrap();
        let history = context.snapshot().unwrap();
        let transcripts = user.transcript_frames().unwrap();
        let final_deadline = user.deadline().unwrap();
        let final_idle_deadline = user.idle_deadline().unwrap();

        assert_eq!(metadata, forwarded(metadata_frame, Direction::Upstream));
        assert_eq!(started.events, vec![AggregationEvent::UserTurnStarted]);
        let [AggregationAction::Push(forward), AggregationAction::Broadcast(boundary)] =
            started.actions.as_slice()
        else {
            panic!("one start boundary and original VAD frame: {started:?}");
        };
        assert_eq!(boundary.data, FrameData::UserStartedSpeaking);
        assert_ne!(boundary.id, start_frame.id);
        assert_eq!(
            forward,
            &DirectedFrame::new(start_frame, Direction::Downstream)
        );
        assert_eq!(vad_stop, forwarded(stop_frame, Direction::Upstream));
        assert_eq!(before_floor, AggregationOutput::default());
        assert_eq!(
            old_scalar_due,
            AggregationOutput::default(),
            "explicit={explicit}: the empty turn must remain open at 120ms"
        );
        assert_eq!(deadline, Some(ms(620)), "explicit={explicit}");
        assert_eq!(old_scalar_idle, None, "no idle rearm before actual stop");
        assert!(history_at_floor.messages.is_empty());
        assert_eq!(before_safety, AggregationOutput::default());
        assert_empty_stop(&due, TurnStopReason::SpeechTimeout);
        assert_eq!(idle_due, Some(ms(820)));
        assert_eq!(later, AggregationOutput::default());
        assert_eq!(before_idle, AggregationOutput::default());
        assert_eq!(
            idle,
            AggregationOutput {
                actions: vec![],
                events: vec![AggregationEvent::UserTurnIdle],
            }
        );
        assert_eq!(idle_again, AggregationOutput::default());
        assert_eq!(end, forwarded(end_frame, Direction::Downstream));
        assert_eq!(after_end, AggregationOutput::default());
        assert!(history.messages.is_empty());
        assert!(transcripts.is_empty());
        assert_eq!(final_deadline, None);
        assert_eq!(final_idle_deadline, None);
    }
}

fn transcript(finalized: bool) -> FrameData {
    FrameData::Transcription(TranscriptionFrame {
        text: "hello".into(),
        finalized,
        ..Default::default()
    })
}

#[test]
fn scalar_zero_exhausted_lower_and_equal_stt_budgets_keep_the_speech_floor() {
    // The VAD has already observed 200ms. None of these remaining waits exceeds 100ms.
    for seconds in [0.0, 0.1, 0.25, 0.3] {
        let mut controller = UserTurnController::new(config(false));
        controller.process(&metadata(seconds), ms(0));
        controller.process(&start(), ms(0));
        let vad_stop = controller.process(&stop(), ms(20));
        let deadline = controller.deadline();
        let before = controller.advance(ms(119));
        let due = controller.advance(ms(120));
        let duplicate = controller.advance(ms(620));
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        assert!(vad_stop.is_empty());
        assert_eq!(deadline, Some(ms(120)), "STT seconds={seconds}");
        assert!(before.is_empty());
        assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
        assert!(duplicate.is_empty());
        assert!(cleaned.is_empty());
    }
}

#[test]
fn scalar_finalized_text_short_circuits_only_stt_before_or_after_vad_stop() {
    for final_at in [10, 50, 150] {
        let mut controller = UserTurnController::new(config(false));
        controller.process(&metadata(0.8), ms(0));
        controller.process(&start(), ms(0));
        let mut final_output = vec![];
        if final_at < 20 {
            final_output = controller.process(&transcript(true), ms(final_at));
        }
        let vad_stop = controller.process(&stop(), ms(20));
        if (20..120).contains(&final_at) {
            final_output = controller.process(&transcript(true), ms(final_at));
        }
        let before = controller.advance(ms(119));
        let at_floor = controller.advance(ms(120));
        if final_at > 120 {
            final_output = controller.process(&transcript(true), ms(final_at));
        }
        let duplicate = controller.advance(ms(620));
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        assert!(vad_stop.is_empty());
        assert!(before.is_empty());
        if final_at <= 120 {
            assert!(final_output.is_empty());
            assert_eq!(at_floor, stopped(TurnStopReason::SpeechTimeout));
        } else {
            assert!(at_floor.is_empty());
            assert_eq!(final_output, stopped(TurnStopReason::SpeechTimeout));
        }
        assert!(duplicate.is_empty());
        assert!(cleaned.is_empty());
    }
}

#[test]
fn scalar_interim_revokes_finalization_without_extending_the_original_safety_wait() {
    let mut controller = UserTurnController::new(config(false));
    controller.process(&metadata(0.8), ms(0));
    controller.process(&start(), ms(0));
    controller.process(&transcript(true), ms(10));
    let vad_stop = controller.process(&stop(), ms(20));
    let interim = controller.process(
        &FrameData::InterimTranscription(TranscriptionFrame {
            text: "still arriving".into(),
            ..Default::default()
        }),
        ms(50),
    );
    let at_floor = controller.advance(ms(120));
    let segment = controller.process(&transcript(false), ms(500));
    let deadline = controller.deadline();
    let before = controller.advance(ms(619));
    let due = controller.advance(ms(620));
    let duplicate = controller.advance(ms(1100));
    controller.cleanup();
    let cleaned = controller.advance(ms(10000));
    assert!(vad_stop.is_empty());
    assert!(interim.is_empty());
    assert!(at_floor.is_empty());
    assert!(segment.is_empty());
    assert_eq!(deadline, Some(ms(620)));
    assert!(before.is_empty());
    assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
    assert!(duplicate.is_empty());
    assert!(cleaned.is_empty());
}

#[test]
fn scalar_resumed_speech_cancels_old_wait_and_legacy_stop_starts_a_new_budget() {
    let mut controller = UserTurnController::new(config(false));
    controller.process(&metadata(0.8), ms(0));
    controller.process(&start(), ms(0));
    controller.process(&stop(), ms(20));
    let resumed = controller.process(&start(), ms(100));
    let audible_deadline = controller.deadline();
    let old_due = controller.advance(ms(620));
    let new_stop = controller.process(
        &FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.2,
            timestamp: 0.0,
        }),
        ms(640),
    );
    let new_deadline = controller.deadline();
    let floor = controller.advance(ms(740));
    let before = controller.advance(ms(1239));
    let due = controller.advance(ms(1240));
    let duplicate = controller.advance(ms(1241));
    controller.cleanup();
    let cleaned = controller.advance(ms(10000));
    assert!(resumed.is_empty());
    assert_eq!(audible_deadline, None);
    assert!(old_due.is_empty());
    assert!(new_stop.is_empty());
    assert_eq!(new_deadline, Some(ms(1240)));
    assert!(floor.is_empty());
    assert!(before.is_empty());
    assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
    assert!(duplicate.is_empty());
    assert!(cleaned.is_empty());
}

#[test]
fn scalar_no_vad_transcript_fallback_keeps_inactivity_timing() {
    let mut controller = UserTurnController::new(config(false));
    controller.process(&metadata(0.8), ms(0));
    let started = controller.process(&transcript(false), ms(0));
    let refreshed = controller.process(&transcript(false), ms(80));
    let deadline = controller.deadline();
    let original_due = controller.advance(ms(100));
    let before = controller.advance(ms(179));
    let due = controller.advance(ms(180));
    let duplicate = controller.advance(ms(680));
    controller.cleanup();
    let cleaned = controller.advance(ms(10000));
    assert_eq!(
        started,
        vec![UserTurnEvent::Started {
            emit_speaking: true,
            interrupt: false,
        }]
    );
    assert!(refreshed.is_empty());
    assert_eq!(deadline, Some(ms(180)));
    assert!(original_due.is_empty());
    assert!(before.is_empty());
    assert_eq!(due, stopped(TurnStopReason::SpeechTimeout));
    assert!(duplicate.is_empty());
    assert!(cleaned.is_empty());
}

#[test]
fn scalar_model_gate_still_releases_optional_text_only_on_a_real_verdict() {
    // No model is loaded. A caller supplies the current verdict through the public API.
    for has_verdict in [true, false] {
        let mut controller = UserTurnController::new(UserTurnConfig {
            require_model_completion: true,
            ..config(false)
        });
        controller.process(&metadata(0.8), ms(0));
        controller.process(&start(), ms(0));
        let vad_stop = controller.process(&stop(), ms(20));
        let before_verdict = controller.advance(ms(29));
        let verdict = if has_verdict {
            controller.mark_model_complete(ms(30))
        } else {
            controller.advance(ms(30))
        };
        let deadline = controller.deadline();
        let at_floor = controller.advance(ms(120));
        let safety_due = controller.advance(ms(620));
        let watchdog = controller.advance(ms(5020));
        let duplicate = controller.advance(ms(5021));
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        assert!(vad_stop.is_empty());
        assert!(before_verdict.is_empty());
        assert!(at_floor.is_empty());
        assert!(safety_due.is_empty());
        if has_verdict {
            assert_eq!(verdict, stopped(TurnStopReason::ModelCompletion));
            assert_eq!(deadline, None);
            assert!(watchdog.is_empty());
        } else {
            assert!(verdict.is_empty());
            assert_eq!(deadline, Some(ms(5020)));
            assert_eq!(watchdog, stopped(TurnStopReason::Watchdog));
        }
        assert!(duplicate.is_empty());
        assert!(cleaned.is_empty());
    }
}

#[test]
fn scalar_external_and_watchdog_authority_can_preempt_the_safety_wait() {
    for (external, emit) in [(Some(false), false), (Some(true), true), (None, true)] {
        let mut controller = UserTurnController::new(UserTurnConfig {
            resolve_proposals: true,
            stop_timeout: if external.is_none() { ms(50) } else { ms(5000) },
            ..config(false)
        });
        controller.process(&metadata(0.8), ms(0));
        controller.process(&start(), ms(0));
        controller.process(&stop(), ms(20));
        let signal = match external {
            Some(false) => controller.process(&FrameData::UserStoppedSpeaking, ms(30)),
            Some(true) => controller.process(&FrameData::ProposedUserStoppedSpeaking, ms(30)),
            None => controller.advance(ms(30)),
        };
        let before_watchdog = controller.advance(ms(69));
        let watchdog = controller.advance(ms(70));
        let old_safety = controller.advance(ms(620));
        controller.cleanup();
        let cleaned = controller.advance(ms(10000));
        assert!(before_watchdog.is_empty());
        if external.is_some() {
            assert_eq!(
                signal,
                vec![UserTurnEvent::Stopped {
                    emit_speaking: emit,
                    reason: TurnStopReason::External,
                }]
            );
            assert!(watchdog.is_empty());
        } else {
            assert!(signal.is_empty());
            assert_eq!(watchdog, stopped(TurnStopReason::Watchdog));
        }
        assert!(old_safety.is_empty());
        assert!(cleaned.is_empty());
    }
}

#[test]
fn scalar_pending_safety_is_cancelled_by_mute_end_stop_and_cancel() {
    for boundary in [
        FrameData::UserMuteStarted,
        FrameData::End(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(Default::default()),
    ] {
        let muted = matches!(boundary, FrameData::UserMuteStarted);
        let context = SharedContext::default();
        let user = user(&context, false);
        process(&user, Frame::new(metadata(0.8)), Direction::Downstream, 0);
        process(&user, Frame::new(start()), Direction::Upstream, 0);
        let stop_frame = tagged(stop(), "pending-stop");
        let vad_stop = process(&user, stop_frame.clone(), Direction::Downstream, 20);
        let boundary_frame = tagged(boundary, "terminal-or-mute");
        let closed = process(&user, boundary_frame.clone(), Direction::Downstream, 50);
        let late_start = process(&user, Frame::new(start()), Direction::Downstream, 100);
        let late_stop = process(&user, Frame::new(stop()), Direction::Downstream, 120);
        let obsolete = user.advance(ms(620)).unwrap();
        let old_idle = user.advance(ms(820)).unwrap();
        let deadline = user.deadline().unwrap();
        let idle = user.idle_deadline().unwrap();
        let cleanup = tagged(FrameData::End(Default::default()), "cleanup");
        let ended = process(&user, cleanup.clone(), Direction::Downstream, 900);
        let after_cleanup = user.advance(ms(10000)).unwrap();
        let history = context.snapshot().unwrap();
        let transcripts = user.transcript_frames().unwrap();
        assert_eq!(vad_stop, forwarded(stop_frame, Direction::Downstream));
        assert_eq!(
            closed.events,
            vec![AggregationEvent::UserTurnStopped {
                text: String::new(),
                reason: if muted {
                    TurnStopReason::Muted
                } else {
                    TurnStopReason::SessionEnd
                },
            }]
        );
        if muted {
            let [AggregationAction::Broadcast(speaking), AggregationAction::Push(forward)] =
                closed.actions.as_slice()
            else {
                panic!("one mute stop then forwarded control: {closed:?}");
            };
            assert_eq!(speaking.data, FrameData::UserStoppedSpeaking);
            assert_eq!(
                forward,
                &DirectedFrame::new(boundary_frame, Direction::Downstream)
            );
        } else {
            assert_eq!(
                closed.actions,
                forwarded(boundary_frame, Direction::Downstream).actions
            );
        }
        assert_eq!(late_start, AggregationOutput::default());
        assert_eq!(late_stop, AggregationOutput::default());
        assert_eq!(obsolete, AggregationOutput::default());
        assert_eq!(old_idle, AggregationOutput::default());
        assert_eq!(deadline, None);
        assert_eq!(idle, None);
        assert_eq!(ended, forwarded(cleanup, Direction::Downstream));
        assert_eq!(after_cleanup, AggregationOutput::default());
        assert!(history.messages.is_empty());
        assert!(transcripts.is_empty());
    }
}
