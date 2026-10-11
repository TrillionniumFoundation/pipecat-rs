//! Independent decisive schedules for pinned typed Start contract c561eccf.
//! Source-only preparation; exact final API/format will be verified before execution.
use pipecat_core::*;
use pipecat_turns::*;
use std::time::Duration;

fn user() -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            turn: UserTurnConfig {
                stop_timeout: Duration::from_millis(50),
                ..Default::default()
            },
            eager: Some(EagerUserTurnConfig::default()),
            ..Default::default()
        },
    )
}
fn owner() -> EagerSourceSessionOwner {
    EagerSourceSessionOwner::new(4096).unwrap()
}
fn transcript(eager: bool, text: &str) -> FrameData {
    let data = TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    };
    if eager {
        FrameData::EagerTranscription(data)
    } else {
        FrameData::Transcription(data)
    }
}
fn scoped(data: FrameData, carrier: NativeEagerSource) -> Frame {
    let mut frame = Frame::new(data);
    frame.set_native_eager_source(carrier).unwrap();
    frame
}
fn start(turn: &NativeEagerTurn) -> Frame {
    scoped(
        FrameData::ProposedUserStartedSpeaking,
        NativeEagerSource::Start(turn.clone()),
    )
}
fn final_frame(receipt: &NativeEagerCommit) -> Frame {
    scoped(
        receipt.payload().clone(),
        NativeEagerSource::Final(receipt.clone()),
    )
}
fn stop(turn: &NativeEagerTurn, receipt: Option<&NativeEagerCommit>) -> Frame {
    scoped(
        FrameData::ProposedUserStoppedSpeaking,
        NativeEagerSource::Stop {
            turn: turn.clone(),
            commit: receipt.cloned(),
        },
    )
}
fn feed(user: &LLMUserAggregator, frame: Frame, ms: u64) -> AggregationOutput {
    user.process_at(frame, Direction::Downstream, Duration::from_millis(ms))
        .unwrap()
}
fn inference_count(output: &AggregationOutput) -> usize {
    output
        .actions
        .iter()
        .filter(|action| {
            matches!(action,
                AggregationAction::Push(frame) if matches!(frame.frame.data,
                    FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
                )
            )
        })
        .count()
}
fn pure_noop(output: &AggregationOutput) {
    assert!(
        output.actions.is_empty(),
        "stale source produced actions: {:?}",
        output.actions
    );
    assert!(
        output.events.is_empty(),
        "stale source produced events: {:?}",
        output.events
    );
}
fn vad_stop() -> Frame {
    Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
        duration_secs: 0.0,
        timestamp: 0.0,
    }))
}

#[test]
fn review_never_adopted_retired_receipt_cannot_close_typed_or_unbound_replacement() {
    for typed_b in [false, true] {
        for stop_first in [false, true] {
            for restart_session in [false, true] {
                let user = user();
                let a = owner();
                let a_turn = a.begin_turn().unwrap();
                feed(&user, start(&a_turn), 0);
                let held = a
                    .offer_final(&a_turn, transcript(false, "retired A"))
                    .unwrap();
                let b_session = owner();
                let b = if restart_session {
                    a.revoke();
                    &b_session
                } else {
                    &a
                };
                let b_turn = b.begin_turn().unwrap();
                if typed_b {
                    feed(&user, start(&b_turn), 1);
                } else {
                    feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 1);
                }
                feed(&user, Frame::new(transcript(false, "B before")), 2);
                let deadline = user.deadline().unwrap();
                let idle = user.idle_deadline().unwrap();
                let history = user.context().snapshot_rich().unwrap();
                if stop_first {
                    pure_noop(&feed(&user, stop(&a_turn, Some(&held)), 10_000));
                    pure_noop(&feed(&user, final_frame(&held), 10_001));
                } else {
                    pure_noop(&feed(&user, final_frame(&held), 10_000));
                    pure_noop(&feed(&user, stop(&a_turn, Some(&held)), 10_001));
                }
                assert_eq!(
                    user.deadline().unwrap(),
                    deadline,
                    "stale control changed B timer"
                );
                assert_eq!(
                    user.idle_deadline().unwrap(),
                    idle,
                    "stale control changed B idle state"
                );
                assert_eq!(
                    user.context().snapshot_rich().unwrap(),
                    history,
                    "stale A committed B early"
                );
                let completed = if typed_b {
                    let final_b = b
                        .offer_final(&b_turn, transcript(false, "B final"))
                        .unwrap();
                    feed(&user, final_frame(&final_b), 3);
                    feed(&user, stop(&b_turn, Some(&final_b)), 4)
                } else {
                    feed(&user, Frame::new(transcript(false, "B final")), 3);
                    feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 4)
                };
                assert_eq!(
                    inference_count(&completed),
                    1,
                    "B lost its own ordinary completion"
                );
                let history =
                    serde_json::to_string(&user.context().snapshot_rich().unwrap()).unwrap();
                assert!(history.contains("B before B final"));
                assert!(!history.contains("retired A"));
            }
        }
    }
}

#[test]
fn review_retired_turn_without_prediction_or_delivered_final_denies_watchdog_recovery() {
    for retired in [false, true] {
        for offer_and_drop_final in [false, true] {
            let user = user();
            let source = owner();
            let turn = source.begin_turn().unwrap();
            feed(&user, start(&turn), 0);
            if offer_and_drop_final {
                drop(
                    source
                        .offer_final(&turn, transcript(false, "never delivered"))
                        .unwrap(),
                );
            }
            if retired {
                source.revoke();
            }
            feed(&user, vad_stop(), 1);
            let output = user.advance(Duration::from_millis(100)).unwrap();
            assert_eq!(inference_count(&output), usize::from(!retired),
                "retired source lacked independent no-final recovery fencing, or positive control ineffective");
            let history = user.context().snapshot_rich().unwrap();
            assert_eq!(history.messages.len(), usize::from(!retired));
            if !retired {
                assert_eq!(history.messages[0].role, ChatRole::Developer);
            }
        }
    }
}

#[test]
fn review_old_empty_stop_cannot_suppress_replacement_empty_recovery() {
    for typed_b in [false, true] {
        let user = user();
        let a = owner();
        let a_turn = a.begin_turn().unwrap();
        feed(&user, start(&a_turn), 0);
        a.offer_empty_stop(&a_turn).unwrap();
        let held = stop(&a_turn, None);
        a.revoke();
        let b = owner();
        let b_turn = b.begin_turn().unwrap();
        if typed_b {
            feed(&user, start(&b_turn), 1);
        } else {
            feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 1);
        }
        let deadline = user.deadline().unwrap();
        pure_noop(&feed(&user, held, 10_000));
        assert_eq!(user.deadline().unwrap(), deadline);
        let stopped = if typed_b {
            b.offer_empty_stop(&b_turn).unwrap();
            feed(&user, stop(&b_turn, None), 2)
        } else {
            feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 2)
        };
        let output = user.advance(Duration::from_millis(100)).unwrap();
        assert_eq!(
            inference_count(&stopped) + inference_count(&output),
            1,
            "A empty stop consumed/suppressed B recovery"
        );
        assert_eq!(
            user.context().snapshot_rich().unwrap().messages[0].role,
            ChatRole::Developer
        );
    }
}

#[test]
fn review_passive_turn_cannot_forge_an_empty_stop_before_owner_offer() {
    let user = user();
    let source = owner();
    let turn = source.begin_turn().unwrap();
    feed(&user, start(&turn), 0);
    let deadline = user.deadline().unwrap();
    let mut forged = Frame::new(FrameData::ProposedUserStoppedSpeaking);
    if forged
        .set_native_eager_source(NativeEagerSource::Stop {
            turn: turn.clone(),
            commit: None,
        })
        .is_ok()
    {
        if let Ok(output) =
            user.process_at(forged, Direction::Downstream, Duration::from_millis(10_000))
        {
            pure_noop(&output);
        }
    }
    assert_eq!(
        user.deadline().unwrap(),
        deadline,
        "unoffered stop changed source controller"
    );
    assert!(user.context().snapshot_rich().unwrap().messages.is_empty());
    source.offer_empty_stop(&turn).unwrap();
    let stopped = feed(&user, stop(&turn, None), 1);
    let output = user.advance(Duration::from_millis(100)).unwrap();
    assert_eq!(
        inference_count(&stopped) + inference_count(&output),
        1,
        "failed stop forgery poisoned legitimate owner offer"
    );
}

#[test]
fn review_queued_start_and_prediction_survive_normal_drain_only_for_offered_final() {
    let user = user();
    let source = owner();
    let turn = source.begin_turn().unwrap();
    let prediction = source.predict(&turn, transcript(true, "normal")).unwrap();
    let final_receipt = source
        .offer_final(&turn, transcript(false, "normal"))
        .unwrap();
    assert!(source.begin_drain());
    assert!(source.complete_drain());
    drop(source);
    feed(&user, start(&turn), 0);
    let output = feed(
        &user,
        scoped(
            prediction.payload().clone(),
            NativeEagerSource::Prediction(prediction),
        ),
        1,
    );
    let capability = output
        .actions
        .iter()
        .find_map(|action| match action {
            AggregationAction::Push(frame) => frame.frame.native_speculation().cloned(),
            _ => None,
        })
        .expect("queued offered prediction remains adoptable after normal drain");
    feed(&user, final_frame(&final_receipt), 2);
    feed(&user, stop(&turn, Some(&final_receipt)), 3);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert_eq!(user.context().snapshot_rich().unwrap().messages.len(), 1);
}

#[test]
fn review_duplicate_old_start_cannot_rebind_after_active_or_closed_replacement() {
    let user = user();
    let source = owner();
    let a = source.begin_turn().unwrap();
    let clone_start = start(&a);
    feed(&user, clone_start.clone(), 0);
    // The accepted internal Started event must have retained A's binding.
    let a_final = source.offer_final(&a, transcript(false, "A")).unwrap();
    feed(&user, final_frame(&a_final), 1);
    feed(&user, stop(&a, Some(&a_final)), 2);
    assert_eq!(user.context().snapshot_rich().unwrap().messages.len(), 1);
    let b = source.begin_turn().unwrap();
    feed(&user, start(&b), 3);
    let deadline = user.deadline().unwrap();
    pure_noop(&feed(&user, clone_start.duplicate(), 10_000));
    assert_eq!(user.deadline().unwrap(), deadline);
    let b_final = source.offer_final(&b, transcript(false, "B")).unwrap();
    feed(&user, final_frame(&b_final), 4);
    feed(&user, stop(&b, Some(&b_final)), 5);
    let history = user.context().snapshot_rich().unwrap();
    pure_noop(&feed(&user, clone_start, 20_000));
    assert_eq!(user.context().snapshot_rich().unwrap(), history);
    assert_eq!(history.messages.len(), 2);
}

#[test]
fn review_live_old_scoped_receipt_cannot_close_external_unbound_replacement() {
    for stop_first in [false, true] {
        let user = user();
        let source = owner();
        let turn = source.begin_turn().unwrap();
        feed(&user, start(&turn), 0);
        let receipt = source
            .offer_final(&turn, transcript(false, "old live A"))
            .unwrap();
        assert!(receipt.is_live());
        // No source revoke or new source turn: this isolates consumer ownership.
        feed(&user, Frame::new(FrameData::ProposedUserStartedSpeaking), 1);
        feed(&user, Frame::new(transcript(false, "unbound B")), 2);
        let deadline = user.deadline().unwrap();
        let history = user.context().snapshot_rich().unwrap();
        if stop_first {
            pure_noop(&feed(&user, stop(&turn, Some(&receipt)), 10_000));
            pure_noop(&feed(&user, final_frame(&receipt), 10_001));
        } else {
            pure_noop(&feed(&user, final_frame(&receipt), 10_000));
            pure_noop(&feed(&user, stop(&turn, Some(&receipt)), 10_001));
        }
        assert_eq!(user.deadline().unwrap(), deadline);
        assert_eq!(user.context().snapshot_rich().unwrap(), history);
        let finished = feed(&user, Frame::new(FrameData::ProposedUserStoppedSpeaking), 3);
        assert_eq!(inference_count(&finished), 1);
        let history = serde_json::to_string(&user.context().snapshot_rich().unwrap()).unwrap();
        assert!(history.contains("unbound B"));
        assert!(!history.contains("old live A"));
    }
}

#[test]
fn review_same_turn_prediction_replay_cannot_advance_replacement_clock_or_deadline() {
    let user = user();
    let source = owner();
    let turn = source.begin_turn().unwrap();
    feed(&user, start(&turn), 0);
    let a = source.predict(&turn, transcript(true, "old A")).unwrap();
    let old_frame = scoped(a.payload().clone(), NativeEagerSource::Prediction(a));
    feed(&user, old_frame.clone(), 1);
    let b = source.predict(&turn, transcript(true, "B")).unwrap();
    let new_frame = scoped(b.payload().clone(), NativeEagerSource::Prediction(b));
    let output = feed(&user, new_frame.clone(), 2);
    let capability = output
        .actions
        .iter()
        .find_map(|action| match action {
            AggregationAction::Push(frame) => frame.frame.native_speculation().cloned(),
            _ => None,
        })
        .unwrap();
    let deadline = user.deadline().unwrap();
    pure_noop(&feed(&user, old_frame.duplicate(), 10_000));
    pure_noop(&feed(&user, new_frame.duplicate(), 20_000));
    assert_eq!(user.deadline().unwrap(), deadline);
    assert_eq!(capability.status(), SpeculationStatus::Pending);
    let final_b = source.offer_final(&turn, transcript(false, "B")).unwrap();
    feed(&user, final_frame(&final_b), 3);
    feed(&user, stop(&turn, Some(&final_b)), 4);
    assert_eq!(
        capability.status(),
        SpeculationStatus::Confirmed,
        "late rejected replay poisoned the authoritative B confirmation clock"
    );
}

#[test]
fn review_duplicate_current_final_and_early_stop_are_noops_before_clock_changes() {
    for stop_first in [false, true] {
        let user = user();
        let source = owner();
        let turn = source.begin_turn().unwrap();
        feed(&user, start(&turn), 0);
        let prediction = source.predict(&turn, transcript(true, "same")).unwrap();
        let output = feed(
            &user,
            scoped(
                prediction.payload().clone(),
                NativeEagerSource::Prediction(prediction),
            ),
            1,
        );
        let capability = output
            .actions
            .iter()
            .find_map(|action| match action {
                AggregationAction::Push(frame) => frame.frame.native_speculation().cloned(),
                _ => None,
            })
            .unwrap();
        let receipt = source
            .offer_final(&turn, transcript(false, "same"))
            .unwrap();
        if stop_first {
            feed(&user, stop(&turn, Some(&receipt)), 2);
            let deadline = user.deadline().unwrap();
            pure_noop(&feed(&user, stop(&turn, Some(&receipt)), 20_000));
            assert_eq!(user.deadline().unwrap(), deadline);
            feed(&user, final_frame(&receipt), 3);
        } else {
            feed(&user, final_frame(&receipt), 2);
            let deadline = user.deadline().unwrap();
            pure_noop(&feed(&user, final_frame(&receipt), 20_000));
            assert_eq!(user.deadline().unwrap(), deadline);
            feed(&user, stop(&turn, Some(&receipt)), 3);
        }
        assert_eq!(
            capability.status(),
            SpeculationStatus::Confirmed,
            "duplicate role altered current final/stop timing"
        );
    }
}

#[test]
fn review_global_terminal_flushes_only_independent_generic_text_after_scoped_discard() {
    for prior_watchdog in [false, true] {
        for staged_final in [false, true] {
            for generic_text in [false, true] {
                let user = user();
                let source = owner();
                let turn = source.begin_turn().unwrap();
                feed(&user, start(&turn), 0);
                if generic_text {
                    feed(&user, Frame::new(transcript(false, "generic kept")), 1);
                }
                if staged_final {
                    let receipt = source
                        .offer_final(&turn, transcript(false, "private A rejected"))
                        .unwrap();
                    feed(&user, final_frame(&receipt), 2);
                }
                source.revoke();
                if prior_watchdog {
                    feed(&user, vad_stop(), 3);
                    let discarded = user.advance(Duration::from_millis(100)).unwrap();
                    assert_eq!(inference_count(&discarded), 0);
                    assert!(user.context().snapshot_rich().unwrap().messages.is_empty());
                }
                let terminal = feed(&user, Frame::end(), 200);
                assert_eq!(inference_count(&terminal), usize::from(generic_text));
                let history = user.context().snapshot_rich().unwrap();
                assert_eq!(history.messages.len(), usize::from(generic_text));
                let encoded = serde_json::to_string(&history).unwrap();
                assert!(!encoded.contains("private A rejected"));
                assert!(!history
                    .messages
                    .iter()
                    .any(|message| message.role == ChatRole::Developer));
                if generic_text {
                    assert!(encoded.contains("generic kept"));
                }
                for repeated in [Frame::end(), Frame::new(FrameData::Stop)] {
                    let repeated = feed(&user, repeated, 300);
                    assert_eq!(inference_count(&repeated), 0);
                    assert_eq!(user.context().snapshot_rich().unwrap(), history);
                }
            }
        }
    }
}

#[test]
fn review_cancel_and_typed_input_preserve_independent_generic_authority_after_discard() {
    for typed_input in [false, true] {
        for prior_watchdog in [false, true] {
            let user = user();
            let source = owner();
            let turn = source.begin_turn().unwrap();
            feed(&user, start(&turn), 0);
            feed(&user, Frame::new(transcript(false, "generic kept")), 1);
            let receipt = source
                .offer_final(&turn, transcript(false, "private A rejected"))
                .unwrap();
            feed(&user, final_frame(&receipt), 2);
            source.revoke();
            if prior_watchdog {
                feed(&user, vad_stop(), 3);
                let discarded = user.advance(Duration::from_millis(100)).unwrap();
                assert_eq!(inference_count(&discarded), 0);
                assert!(user.context().snapshot_rich().unwrap().messages.is_empty());
            }
            let operation = if typed_input {
                Frame::new(FrameData::InputText(TextFrame::new("explicit typed")))
            } else {
                Frame::cancel("explicit cancel")
            };
            let output = feed(&user, operation, 200);
            assert_eq!(inference_count(&output), usize::from(typed_input));
            let history = user.context().snapshot_rich().unwrap();
            assert_eq!(history.messages.len(), 1 + usize::from(typed_input));
            let MessageContent::Text(first) = &history.messages[0].content else {
                panic!("text")
            };
            assert_eq!(first, "generic kept");
            assert!(!history
                .messages
                .iter()
                .any(|message| message.role == ChatRole::Developer));
            assert!(!serde_json::to_string(&history)
                .unwrap()
                .contains("private A rejected"));
            if typed_input {
                let MessageContent::Text(last) = &history.messages[1].content else {
                    panic!("text")
                };
                assert_eq!(last, "explicit typed");
            } else {
                assert_eq!(inference_count(&feed(&user, Frame::end(), 300)), 0);
                assert_eq!(user.context().snapshot_rich().unwrap(), history);
            }
        }
    }
}
