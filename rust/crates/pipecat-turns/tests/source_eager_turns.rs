//! Source-only Start/controller ownership and native receipt acceptance cases.
use pipecat_core::*;
use pipecat_turns::*;
use std::time::Duration;

fn data(kind: fn(TranscriptionFrame) -> FrameData, text: &str) -> FrameData {
    kind(TranscriptionFrame {
        text: text.into(),
        finalized: true,
        ..Default::default()
    })
}
fn detailed(text: &str, spaces: bool, append: bool) -> FrameData {
    let mut value = TextFrame::new(text);
    value.includes_inter_frame_spaces = spaces;
    value.append_to_context = append;
    FrameData::TranscriptionDetailed(DetailedTranscriptionFrame {
        text: value,
        user_id: "u".into(),
        timestamp: "0".into(),
        language: None,
        result: None,
        finalized: true,
    })
}
fn user() -> LLMUserAggregator {
    LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(EagerUserTurnConfig::default()),
            ..Default::default()
        },
    )
}
fn source() -> (EagerSourceSessionOwner, NativeEagerTurn) {
    let owner = EagerSourceSessionOwner::new(4096).unwrap();
    let turn = owner.begin_turn().unwrap();
    (owner, turn)
}
fn feed(user: &LLMUserAggregator, frame: Frame, ms: u64) -> AggregationOutput {
    user.process_at(frame, Direction::Downstream, Duration::from_millis(ms))
        .unwrap()
}
fn frames(output: &AggregationOutput) -> Vec<&Frame> {
    output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(directed) => Some(&directed.frame),
            AggregationAction::Broadcast(frame) => Some(frame),
            _ => None,
        })
        .collect()
}
fn contexts(output: &AggregationOutput) -> usize {
    frames(output)
        .iter()
        .filter(|frame| {
            matches!(
                frame.data,
                FrameData::LlmContext(_) | FrameData::LlmContextDetailed(_)
            )
        })
        .count()
}
fn history(user: &LLMUserAggregator) -> Vec<(ChatRole, String)> {
    user.context()
        .snapshot_rich()
        .unwrap()
        .messages
        .into_iter()
        .map(|message| {
            let MessageContent::Text(text) = message.content else {
                panic!("text history")
            };
            (message.role, text)
        })
        .collect()
}
fn tagged(data: FrameData, source: NativeEagerSource) -> Frame {
    let mut frame = Frame::new(data);
    frame.set_native_eager_source(source).unwrap();
    frame
}
fn start_frame(turn: &NativeEagerTurn) -> Frame {
    tagged(
        FrameData::ProposedUserStartedSpeaking,
        NativeEagerSource::Start(turn.clone()),
    )
}
fn prediction_frame(prediction: &NativeEagerPrediction) -> Frame {
    tagged(
        prediction.payload().clone(),
        NativeEagerSource::Prediction(prediction.clone()),
    )
}
fn final_frame(receipt: &NativeEagerCommit) -> Frame {
    tagged(
        receipt.payload().clone(),
        NativeEagerSource::Final(receipt.clone()),
    )
}
fn stop_frame(turn: &NativeEagerTurn, receipt: Option<&NativeEagerCommit>) -> Frame {
    tagged(
        FrameData::ProposedUserStoppedSpeaking,
        NativeEagerSource::Stop {
            turn: turn.clone(),
            commit: receipt.cloned(),
        },
    )
}
fn cancel_frame(prediction: &NativeEagerPrediction) -> Frame {
    tagged(
        FrameData::EagerEndOfTurnCancel,
        NativeEagerSource::Cancel(prediction.clone()),
    )
}
fn start(user: &LLMUserAggregator, turn: &NativeEagerTurn, ms: u64) {
    feed(user, start_frame(turn), ms);
}
fn ordinary_start(user: &LLMUserAggregator, ms: u64) {
    feed(user, Frame::new(FrameData::ProposedUserStartedSpeaking), ms);
}
fn ordinary_final(user: &LLMUserAggregator, text: &str, ms: u64) {
    feed(user, Frame::new(data(FrameData::Transcription, text)), ms);
}
fn ordinary_stop(user: &LLMUserAggregator, ms: u64) -> AggregationOutput {
    feed(user, Frame::new(FrameData::ProposedUserStoppedSpeaking), ms)
}
fn adopt(
    user: &LLMUserAggregator,
    prediction: &NativeEagerPrediction,
    ms: u64,
) -> NativeSpeculation {
    let output = feed(user, prediction_frame(prediction), ms);
    assert_eq!(contexts(&output), 1);
    frames(&output)
        .iter()
        .find_map(|frame| frame.native_speculation().cloned())
        .unwrap()
}
fn no_effect(output: AggregationOutput) {
    assert!(output.actions.is_empty());
    assert!(output.events.is_empty());
}
fn non_speaking(user: &LLMUserAggregator, ms: u64) {
    feed(
        user,
        Frame::new(FrameData::VadUserStoppedSpeaking(VadSpeakingFrame {
            duration_secs: 0.0,
            timestamp: 0.0,
        })),
        ms,
    );
}

#[test]
fn typed_start_internal_events_preserve_exact_confirmation_and_post_transfer_authority() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "its ready"))
        .unwrap();
    let capability = adopt(&user, &prediction, 1);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "It's ready!"))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    assert!(history(&user).is_empty());
    let output = feed(&user, stop_frame(&turn, Some(&receipt)), 3);
    assert_eq!(contexts(&output), 0);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert_eq!(history(&user), vec![(ChatRole::User, "It's ready!".into())]);
    assert!(frames(&output).iter().any(|frame| matches!(
        frame.data,
        FrameData::UserStoppedSpeaking
    ) && frame.native_speculation()
        == Some(&capability)));
    owner.revoke();
    no_effect(feed(&user, cancel_frame(&prediction), 4));
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    no_effect(feed(&user, final_frame(&receipt), 5));
    no_effect(feed(&user, stop_frame(&turn, Some(&receipt)), 6));
}

#[test]
fn absent_start_cannot_be_inferred_from_prediction_final_or_stop() {
    let user = user();
    ordinary_start(&user, 0);
    let (owner, turn) = source();
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "unadmitted"))
        .unwrap();
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "unadmitted"))
        .unwrap();
    no_effect(feed(&user, prediction_frame(&prediction), 100_000));
    no_effect(feed(&user, final_frame(&receipt), 200_000));
    no_effect(feed(&user, stop_frame(&turn, Some(&receipt)), 300_000));
    ordinary_stop(&user, 1);
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
    assert_eq!(history(&user)[0].0, ChatRole::Developer);
}

#[test]
fn never_adopted_a_cannot_touch_typed_or_unbound_b_across_restart_and_same_session() {
    for typed_b in [false, true] {
        for same_session in [false, true] {
            for stop_first in [false, true] {
                let user = user();
                let (a, a_turn) = source();
                start(&user, &a_turn, 0);
                let old = a
                    .offer_final(&a_turn, data(FrameData::Transcription, "A"))
                    .unwrap();
                let (b, separate_turn) = source();
                let b_turn = if same_session {
                    a.begin_turn().unwrap()
                } else {
                    a.revoke();
                    separate_turn
                };
                if typed_b {
                    start(&user, &b_turn, 1);
                } else {
                    ordinary_start(&user, 1);
                }
                ordinary_final(&user, "B prefix", 2);
                let deadline = user.deadline().unwrap();
                let delayed = if stop_first {
                    [stop_frame(&a_turn, Some(&old)), final_frame(&old)]
                } else {
                    [final_frame(&old), stop_frame(&a_turn, Some(&old))]
                };
                for frame in delayed {
                    no_effect(feed(&user, frame, 500_000));
                }
                assert_eq!(user.deadline().unwrap(), deadline);
                assert!(history(&user).is_empty());
                let output = if typed_b {
                    let owner = if same_session { &a } else { &b };
                    let receipt = owner
                        .offer_final(&b_turn, data(FrameData::Transcription, "B final"))
                        .unwrap();
                    feed(&user, final_frame(&receipt), 3);
                    feed(&user, stop_frame(&b_turn, Some(&receipt)), 4)
                } else {
                    ordinary_final(&user, "B final", 3);
                    ordinary_stop(&user, 4)
                };
                assert_eq!(contexts(&output), 1);
                assert_eq!(
                    history(&user),
                    vec![(ChatRole::User, "B prefix B final".into())]
                );
                no_effect(feed(&user, final_frame(&old), 600_000));
                no_effect(feed(&user, stop_frame(&a_turn, Some(&old)), 600_000));
            }
        }
    }
}

#[test]
fn live_a_after_genuine_unbound_start_cannot_poison_b_recovery_clock() {
    let user = user();
    let (a, turn) = source();
    start(&user, &turn, 0);
    let old = a
        .offer_final(&turn, data(FrameData::Transcription, "A"))
        .unwrap();
    ordinary_start(&user, 1);
    assert!(turn.is_live());
    no_effect(feed(&user, final_frame(&old), 900_000));
    no_effect(feed(&user, stop_frame(&turn, Some(&old)), 900_000));
    ordinary_stop(&user, 2);
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
    assert_eq!(history(&user)[0].0, ChatRole::Developer);
}

#[test]
fn old_receipt_or_empty_stop_cannot_suppress_b_default_recovery() {
    for typed_b in [false, true] {
        for empty in [false, true] {
            let user = user();
            let (a, turn) = source();
            start(&user, &turn, 0);
            let old = if empty {
                a.offer_empty_stop(&turn).unwrap();
                None
            } else {
                Some(
                    a.offer_final(&turn, data(FrameData::Transcription, "A"))
                        .unwrap(),
                )
            };
            a.revoke();
            let (b, b_turn) = source();
            if typed_b {
                start(&user, &b_turn, 1);
            } else {
                ordinary_start(&user, 1);
            }
            if let Some(receipt) = &old {
                no_effect(feed(&user, final_frame(receipt), 800_000));
            }
            no_effect(feed(&user, stop_frame(&turn, old.as_ref()), 800_000));
            if typed_b {
                b.offer_empty_stop(&b_turn).unwrap();
                feed(&user, stop_frame(&b_turn, None), 2);
                no_effect(feed(&user, stop_frame(&b_turn, None), 900_000));
            } else {
                ordinary_stop(&user, 2);
            }
            assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
            assert_eq!(history(&user).len(), 1);
            assert_eq!(history(&user)[0].0, ChatRole::Developer);
        }
    }
}

#[test]
fn revoked_no_prediction_no_final_watchdog_has_live_positive_control() {
    for revoke in [false, true] {
        let user = user();
        let (owner, turn) = source();
        start(&user, &turn, 0);
        if revoke {
            owner.revoke();
        }
        non_speaking(&user, 1);
        let output = user.advance(Duration::from_secs(6)).unwrap();
        assert_eq!(contexts(&output), usize::from(!revoke));
        if revoke {
            assert!(history(&user).is_empty());
            assert!(!frames(&output)
                .iter()
                .any(|frame| matches!(frame.data, FrameData::UserStoppedSpeaking)));
        } else {
            assert_eq!(history(&user)[0].0, ChatRole::Developer);
        }
    }
}

#[test]
fn held_dropped_delivered_or_final_only_receipt_does_not_grant_empty_recovery() {
    for mode in 0..5 {
        let user = user();
        let (owner, turn) = source();
        start(&user, &turn, 0);
        let receipt = (mode != 4).then(|| {
            owner
                .offer_final(&turn, data(FrameData::Transcription, "A"))
                .unwrap()
        });
        if mode >= 3 {
            owner.begin_drain();
            owner.complete_drain();
        } else {
            owner.revoke();
        }
        if mode == 1 {
            drop(receipt);
        } else if mode == 2 {
            let receipt = receipt.unwrap();
            no_effect(feed(&user, final_frame(&receipt), 1));
            no_effect(feed(&user, stop_frame(&turn, Some(&receipt)), 2));
        }
        non_speaking(&user, 3);
        assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 0);
        assert!(history(&user).is_empty());
    }
}

#[test]
fn active_supersession_preserves_generic_text_but_drops_a_staged_and_early_controls() {
    for early in [false, true] {
        let user = user();
        let (owner, a) = source();
        start(&user, &a, 0);
        ordinary_final(&user, "keep", 1);
        let old = owner
            .offer_final(&a, data(FrameData::Transcription, "discard"))
            .unwrap();
        if early {
            feed(&user, stop_frame(&a, Some(&old)), 2);
        } else {
            feed(&user, final_frame(&old), 2);
        }
        let b = owner.begin_turn().unwrap();
        start(&user, &b, 3);
        assert!(history(&user).is_empty());
        no_effect(feed(&user, final_frame(&old), 900_000));
        no_effect(feed(&user, stop_frame(&a, Some(&old)), 900_000));
        let receipt = owner
            .offer_final(&b, data(FrameData::Transcription, "new"))
            .unwrap();
        feed(&user, final_frame(&receipt), 4);
        assert_eq!(contexts(&feed(&user, stop_frame(&b, Some(&receipt)), 5)), 1);
        assert_eq!(history(&user), vec![(ChatRole::User, "keep new".into())]);
    }
}

#[test]
fn adopted_a_and_start_clones_cannot_replay_after_b_closes_and_c_starts() {
    let user = user();
    let (owner, a) = source();
    start(&user, &a, 0);
    let prediction = owner
        .predict(&a, data(FrameData::EagerTranscription, "A"))
        .unwrap();
    adopt(&user, &prediction, 1);
    let old = owner
        .offer_final(&a, data(FrameData::Transcription, "A"))
        .unwrap();
    drop(prediction);
    let b = owner.begin_turn().unwrap();
    start(&user, &b, 2);
    no_effect(feed(&user, start_frame(&a).duplicate(), 900_000));
    let receipt = owner
        .offer_final(&b, data(FrameData::Transcription, "B"))
        .unwrap();
    feed(&user, final_frame(&receipt), 3);
    feed(&user, stop_frame(&b, Some(&receipt)), 4);
    no_effect(feed(&user, start_frame(&a), 900_000));
    let c = owner.begin_turn().unwrap();
    start(&user, &c, 5);
    no_effect(feed(&user, final_frame(&old), 900_000));
    no_effect(feed(&user, stop_frame(&a, Some(&old)), 900_000));
    let receipt = owner
        .offer_final(&c, data(FrameData::Transcription, "C"))
        .unwrap();
    feed(&user, final_frame(&receipt), 6);
    assert_eq!(contexts(&feed(&user, stop_frame(&c, Some(&receipt)), 7)), 1);
    assert_eq!(
        history(&user),
        vec![(ChatRole::User, "B".into()), (ChatRole::User, "C".into())]
    );
}

#[test]
fn passive_empty_stop_denial_precedes_clock_mutation_and_valid_owner_stop_recovers() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    no_effect(feed(&user, stop_frame(&turn, None), 999_000));
    owner.offer_empty_stop(&turn).unwrap();
    feed(&user, stop_frame(&turn, None), 1);
    no_effect(feed(&user, stop_frame(&turn, None), 999_000));
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
    assert_eq!(history(&user)[0].0, ChatRole::Developer);
}

#[test]
fn wrong_direction_mutated_start_and_missing_consumer_fail_closed() {
    let user = user();
    let (owner, turn) = source();
    assert!(user
        .process_at(
            start_frame(&turn),
            Direction::Upstream,
            Duration::from_secs(10)
        )
        .is_err());
    let mut modified = start_frame(&turn);
    modified.data = FrameData::UserStartedSpeaking;
    assert!(user
        .process_at(modified, Direction::Downstream, Duration::from_secs(10))
        .is_err());
    let legacy = LLMUserAggregator::new(SharedContext::default(), UserAggregatorConfig::default());
    assert!(legacy
        .process_at(start_frame(&turn), Direction::Downstream, Duration::ZERO)
        .is_err());
    start(&user, &turn, 0);
    owner.offer_empty_stop(&turn).unwrap();
    feed(&user, stop_frame(&turn, None), 1);
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
}

#[test]
fn muted_start_never_claims_and_other_consumer_cannot_share_claimed_turn() {
    let user = user();
    let (owner, turn) = source();
    feed(&user, Frame::new(FrameData::SttMute(true)), 0);
    no_effect(feed(&user, start_frame(&turn), 999_000));
    feed(&user, Frame::new(FrameData::SttMute(false)), 1);
    start(&user, &turn, 2);
    let other = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(EagerUserTurnConfig::default()),
            ..Default::default()
        },
    );
    no_effect(feed(&other, start_frame(&turn), 3));
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "accepted"))
        .unwrap();
    no_effect(feed(&other, final_frame(&receipt), 4));
    feed(&user, final_frame(&receipt), 4);
    assert_eq!(
        contexts(&feed(&user, stop_frame(&turn, Some(&receipt)), 5)),
        1
    );
}

#[test]
fn resume_overtaking_adoption_and_old_prediction_clone_cannot_cancel_replacement() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let old = owner
        .predict(&turn, data(FrameData::EagerTranscription, "old"))
        .unwrap();
    owner.resume(&old);
    no_effect(feed(&user, cancel_frame(&old), 1));
    let current = owner
        .predict(&turn, data(FrameData::EagerTranscription, "new"))
        .unwrap();
    let capability = adopt(&user, &current, 2);
    no_effect(feed(&user, prediction_frame(&old), 900_000));
    no_effect(feed(&user, prediction_frame(&current), 900_000));
    no_effect(feed(&user, cancel_frame(&old), 5));
    assert_eq!(capability.status(), SpeculationStatus::Pending);
    owner.resume(&current);
    let output = feed(&user, cancel_frame(&current), 6);
    assert!(frames(&output)
        .iter()
        .any(|frame| frame.native_speculation() == Some(&capability)));
    owner.offer_empty_stop(&turn).unwrap();
    feed(&user, stop_frame(&turn, None), 7);
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
}

#[test]
fn actual_and_proposed_stop_overtaking_final_keep_exact_binding_in_one_transaction() {
    for proposed in [false, true] {
        let user = user();
        let (owner, turn) = source();
        start(&user, &turn, 0);
        let prediction = owner
            .predict(&turn, data(FrameData::EagerTranscription, "ready"))
            .unwrap();
        let capability = adopt(&user, &prediction, 1);
        let receipt = owner
            .offer_final(&turn, data(FrameData::Transcription, "ready"))
            .unwrap();
        let stop = tagged(
            if proposed {
                FrameData::ProposedUserStoppedSpeaking
            } else {
                FrameData::UserStoppedSpeaking
            },
            NativeEagerSource::Stop {
                turn: turn.clone(),
                commit: Some(receipt.clone()),
            },
        );
        no_effect(feed(&user, stop, 2));
        let output = feed(&user, final_frame(&receipt), 3);
        assert_eq!(capability.status(), SpeculationStatus::Confirmed);
        assert_eq!(contexts(&output), 0);
        assert!(frames(&output)
            .iter()
            .any(|frame| frame.native_speculation() == Some(&capability)));
    }
}

#[test]
fn current_mismatch_and_timeout_fallback_once() {
    for timeout in [false, true] {
        let user = user();
        let (owner, turn) = source();
        start(&user, &turn, 0);
        let prediction = owner
            .predict(&turn, data(FrameData::EagerTranscription, "prediction"))
            .unwrap();
        let capability = adopt(&user, &prediction, 1);
        if timeout {
            user.advance(Duration::from_secs(6)).unwrap();
        }
        let receipt = owner
            .offer_final(
                &turn,
                data(
                    FrameData::Transcription,
                    if timeout { "prediction" } else { "final" },
                ),
            )
            .unwrap();
        let at = if timeout { 6001 } else { 2 };
        feed(&user, final_frame(&receipt), at);
        assert_eq!(
            contexts(&feed(&user, stop_frame(&turn, Some(&receipt)), at + 1)),
            1
        );
        assert_eq!(capability.status(), SpeculationStatus::Cancelled);
        no_effect(feed(&user, final_frame(&receipt), at + 2));
        assert_eq!(history(&user).len(), 1);
    }
}

#[test]
fn foreign_equal_final_is_noop_and_unscoped_equal_final_cannot_confirm() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "same"))
        .unwrap();
    let capability = adopt(&user, &prediction, 1);
    let (foreign, foreign_turn) = source();
    let receipt = foreign
        .offer_final(&foreign_turn, data(FrameData::Transcription, "same"))
        .unwrap();
    no_effect(feed(&user, final_frame(&receipt), 900_000));
    no_effect(feed(
        &user,
        stop_frame(&foreign_turn, Some(&receipt)),
        900_000,
    ));
    assert_eq!(capability.status(), SpeculationStatus::Pending);
    ordinary_final(&user, "same", 2);
    assert_eq!(contexts(&ordinary_stop(&user, 3)), 1);
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
}

#[test]
fn revoked_staged_final_is_silent_and_retains_generic_text_for_later_b() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    ordinary_final(&user, "keep", 1);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "discard"))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    owner.revoke();
    non_speaking(&user, 3);
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 0);
    assert!(history(&user).is_empty());
    ordinary_start(&user, 6001);
    ordinary_final(&user, "new", 6002);
    assert_eq!(contexts(&ordinary_stop(&user, 6003)), 1);
    assert_eq!(history(&user), vec![(ChatRole::User, "keep new".into())]);
}

#[test]
fn normal_eof_preserves_queued_start_prediction_and_offered_final() {
    let user = user();
    let (owner, turn) = source();
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "ready"))
        .unwrap();
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "ready"))
        .unwrap();
    owner.begin_drain();
    owner.complete_drain();
    drop(owner);
    assert!(turn.is_live());
    assert!(!turn.is_active());
    start(&user, &turn, 0);
    let capability = adopt(&user, &prediction, 1);
    feed(&user, final_frame(&receipt), 2);
    feed(&user, stop_frame(&turn, Some(&receipt)), 3);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
}

#[test]
fn staged_insertion_preserves_spacing_order_and_excluded_final_confirmation() {
    for (prefix, prefix_spaces, scoped, source_spaces, suffix, suffix_spaces, expected, retired) in [
        (
            "before",
            false,
            "middle",
            false,
            "after",
            false,
            "before middle after",
            "before after",
        ),
        (
            "before ",
            true,
            "middle ",
            true,
            "after",
            true,
            "before middle after",
            "before after",
        ),
        (
            "before",
            true,
            "middle",
            true,
            "after",
            true,
            "beforemiddleafter",
            "beforeafter",
        ),
        (
            "before",
            true,
            "middle",
            false,
            "after",
            true,
            "before middle after",
            "beforeafter",
        ),
        (
            "before",
            false,
            "middle",
            true,
            "after",
            false,
            "before middle after",
            "before after",
        ),
        (
            "before ",
            false,
            "middle ",
            true,
            "after",
            false,
            "before middle after",
            "before  after",
        ),
        (
            "before",
            false,
            " ",
            false,
            "after",
            false,
            "before after",
            "before after",
        ),
        (
            "",
            false,
            "middle",
            false,
            "after",
            false,
            "middle after",
            "after",
        ),
        (
            "before",
            false,
            "middle",
            false,
            "",
            false,
            "before middle",
            "before",
        ),
    ] {
        for revoke in [false, true] {
            let user = user();
            let (owner, turn) = source();
            start(&user, &turn, 0);
            if !prefix.is_empty() {
                feed(&user, Frame::new(detailed(prefix, prefix_spaces, true)), 1);
            }
            let receipt = owner
                .offer_final(&turn, detailed(scoped, source_spaces, true))
                .unwrap();
            feed(&user, final_frame(&receipt), 2);
            if !suffix.is_empty() {
                feed(&user, Frame::new(detailed(suffix, suffix_spaces, true)), 3);
            }
            if revoke {
                owner.revoke();
                non_speaking(&user, 4);
                assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 0);
                assert!(history(&user).is_empty());
                ordinary_start(&user, 6001);
                ordinary_stop(&user, 6002);
                assert_eq!(contexts(&user.advance(Duration::from_secs(12)).unwrap()), 1);
            } else {
                assert_eq!(
                    contexts(&feed(&user, stop_frame(&turn, Some(&receipt)), 4)),
                    1
                );
            }
            assert_eq!(
                history(&user),
                vec![(
                    ChatRole::User,
                    (if revoke { retired } else { expected }).into()
                )]
            );
        }
    }
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "secret"))
        .unwrap();
    let capability = adopt(&user, &prediction, 1);
    let receipt = owner
        .offer_final(&turn, detailed("secret", false, false))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    assert_eq!(
        contexts(&feed(&user, stop_frame(&turn, Some(&receipt)), 3)),
        0
    );
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert!(history(&user).is_empty());
}

#[test]
fn consumed_source_authority_never_remains_in_transcript_ring_or_output() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "ordinary"))
        .unwrap();
    feed(&user, final_frame(&receipt), 1);
    let output = feed(&user, stop_frame(&turn, Some(&receipt)), 2);
    assert!(frames(&output)
        .iter()
        .all(|frame| frame.native_eager_source().is_none()));
    for frame in user.transcript_frames().unwrap() {
        assert!(frame.native_eager_source().is_none());
        assert!(serde_json::to_string(&frame).is_ok());
    }
}

#[test]
fn passive_live_cancel_carrier_cannot_revoke_current_capability() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "current"))
        .unwrap();
    let capability = adopt(&user, &prediction, 1);
    no_effect(feed(&user, cancel_frame(&prediction), 900_000));
    assert!(prediction.is_live());
    assert_eq!(capability.status(), SpeculationStatus::Pending);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "current"))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    feed(&user, stop_frame(&turn, Some(&receipt)), 3);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
}

#[test]
fn configured_payload_limit_rejects_before_replacing_existing_capability() {
    let user = LLMUserAggregator::new(
        SharedContext::default(),
        UserAggregatorConfig {
            eager: Some(
                EagerUserTurnConfig::new(EagerMatchPolicy::Exact, Duration::from_secs(5), 5)
                    .unwrap(),
            ),
            ..Default::default()
        },
    );
    let (owner, turn) = source();
    start(&user, &turn, 0);
    // A legacy capability is independent of source-side prediction replacement.
    let provisional = feed(
        &user,
        Frame::new(data(FrameData::EagerTranscription, "first")),
        1,
    );
    let capability = frames(&provisional)
        .iter()
        .find_map(|frame| frame.native_speculation().cloned())
        .unwrap();
    let oversized = owner
        .predict(&turn, data(FrameData::EagerTranscription, "too long"))
        .unwrap();
    assert!(user
        .process_at(
            prediction_frame(&oversized),
            Direction::Downstream,
            Duration::from_secs(900)
        )
        .is_err());
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "too long"))
        .unwrap();
    assert!(user
        .process_at(
            final_frame(&receipt),
            Direction::Downstream,
            Duration::from_secs(900)
        )
        .is_err());
    assert_eq!(capability.status(), SpeculationStatus::Pending);
    ordinary_final(&user, "first", 2);
    assert_eq!(contexts(&ordinary_stop(&user, 3)), 0);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
}

#[test]
fn second_foreign_final_stop_and_current_duplicates_have_no_clock_or_state_effects() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "current"))
        .unwrap();
    let capability = adopt(&user, &prediction, 1);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "current"))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    let (other, other_turn) = source();
    let foreign = other
        .offer_final(&other_turn, data(FrameData::Transcription, "foreign"))
        .unwrap();
    no_effect(feed(&user, final_frame(&foreign), 900_000));
    no_effect(feed(
        &user,
        stop_frame(&other_turn, Some(&foreign)),
        900_000,
    ));
    no_effect(feed(&user, final_frame(&receipt).duplicate(), 900_000));
    feed(&user, stop_frame(&turn, Some(&receipt)), 3);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    no_effect(feed(
        &user,
        stop_frame(&turn, Some(&receipt)).duplicate(),
        900_000,
    ));
    assert!(owner
        .offer_final(&turn, data(FrameData::Transcription, "second"))
        .is_err());
    let next = owner.begin_turn().unwrap();
    start(&user, &next, 4);
    owner.offer_empty_stop(&next).unwrap();
    feed(&user, stop_frame(&next, None), 5);
    assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 1);
    assert_eq!(history(&user).len(), 2);
    assert_eq!(history(&user)[1].0, ChatRole::Developer);
}

#[test]
fn staged_empty_final_retirement_survives_timeout_and_watchdog_without_recovery() {
    let user = user();
    let (owner, turn) = source();
    start(&user, &turn, 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "withdrawn"))
        .unwrap();
    let capability = adopt(&user, &prediction, 1);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, ""))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    feed(&user, stop_frame(&turn, Some(&receipt)), 3);
    owner.revoke();
    let output = user.advance(Duration::from_secs(6)).unwrap();
    assert_eq!(capability.status(), SpeculationStatus::Cancelled);
    assert_eq!(contexts(&output), 0);
    assert!(history(&user).is_empty());
    assert!(!frames(&output)
        .iter()
        .any(|frame| matches!(frame.data, FrameData::UserStoppedSpeaking)));
    ordinary_start(&user, 6001);
    ordinary_stop(&user, 6002);
    assert_eq!(contexts(&user.advance(Duration::from_secs(12)).unwrap()), 1);
    assert_eq!(history(&user)[0].0, ChatRole::Developer);
}

#[test]
fn duplicate_actual_start_and_mutated_prediction_reject_before_clock_and_adoption() {
    let user = user();
    let (owner, turn) = source();
    let actual = tagged(
        FrameData::UserStartedSpeaking,
        NativeEagerSource::Start(turn.clone()),
    );
    feed(&user, actual.clone(), 0);
    let prediction = owner
        .predict(&turn, data(FrameData::EagerTranscription, "ready"))
        .unwrap();
    let mut modified = prediction_frame(&prediction);
    modified.data = data(FrameData::EagerTranscription, "forged");
    assert!(user
        .process_at(modified, Direction::Downstream, Duration::from_secs(900))
        .is_err());
    let capability = adopt(&user, &prediction, 1);
    no_effect(feed(&user, actual.duplicate(), 900_000));
    no_effect(feed(&user, prediction_frame(&prediction), 900_000));
    assert_eq!(capability.status(), SpeculationStatus::Pending);
    let receipt = owner
        .offer_final(&turn, data(FrameData::Transcription, "ready"))
        .unwrap();
    feed(&user, final_frame(&receipt), 2);
    feed(&user, stop_frame(&turn, Some(&receipt)), 3);
    assert_eq!(capability.status(), SpeculationStatus::Confirmed);
    assert_eq!(history(&user), vec![(ChatRole::User, "ready".into())]);
}

#[test]
fn global_end_and_stop_flush_only_ordinary_text_before_or_after_scoped_watchdog_discard() {
    for watchdog_first in [false, true] {
        for keep_ordinary in [false, true] {
            for stop in [false, true] {
                let user = user();
                let (owner, turn) = source();
                start(&user, &turn, 0);
                if keep_ordinary {
                    ordinary_final(&user, "keep", 1);
                }
                let receipt = owner
                    .offer_final(&turn, data(FrameData::Transcription, "discard"))
                    .unwrap();
                feed(&user, final_frame(&receipt), 2);
                owner.revoke();
                // A source Stop has no independent global-terminal authority.
                no_effect(feed(&user, stop_frame(&turn, Some(&receipt)), 3));
                if watchdog_first {
                    non_speaking(&user, 4);
                    let output = user.advance(Duration::from_secs(6)).unwrap();
                    assert_eq!(contexts(&output), 0);
                    assert!(!frames(&output)
                        .iter()
                        .any(|frame| matches!(frame.data, FrameData::UserStoppedSpeaking)));
                }
                assert!(history(&user).is_empty());
                let terminal = if stop {
                    FrameData::Stop
                } else {
                    FrameData::End(Default::default())
                };
                let output = feed(&user, Frame::new(terminal), 6001);
                assert_eq!(contexts(&output), usize::from(keep_ordinary));
                let expected = if keep_ordinary {
                    vec![(ChatRole::User, "keep".into())]
                } else {
                    vec![]
                };
                assert_eq!(history(&user), expected);
                assert_eq!(
                    contexts(&feed(
                        &user,
                        Frame::new(FrameData::End(Default::default())),
                        6002
                    )),
                    0
                );
                assert_eq!(contexts(&feed(&user, Frame::new(FrameData::Stop), 6003)), 0);
                assert_eq!(history(&user), expected);
            }
        }
    }
}

#[test]
fn cancel_after_retired_scoped_final_preserves_only_ordinary_history_without_inference() {
    for watchdog_first in [false, true] {
        for keep_ordinary in [false, true] {
            let user = user();
            let (owner, turn) = source();
            start(&user, &turn, 0);
            if keep_ordinary {
                ordinary_final(&user, "keep", 1);
            }
            let receipt = owner
                .offer_final(&turn, data(FrameData::Transcription, "discard"))
                .unwrap();
            feed(&user, final_frame(&receipt), 2);
            owner.revoke();
            if watchdog_first {
                non_speaking(&user, 3);
                assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 0);
            }
            assert!(history(&user).is_empty());
            let output = feed(
                &user,
                Frame::new(FrameData::Cancel(Default::default())),
                6001,
            );
            assert_eq!(contexts(&output), 0);
            let expected = if keep_ordinary {
                vec![(ChatRole::User, "keep".into())]
            } else {
                vec![]
            };
            assert_eq!(history(&user), expected);
            assert_eq!(
                contexts(&feed(
                    &user,
                    Frame::new(FrameData::End(Default::default())),
                    6002
                )),
                0
            );
            assert_eq!(contexts(&feed(&user, Frame::new(FrameData::Stop), 6003)), 0);
            assert_eq!(history(&user), expected);
        }
    }
}

#[test]
fn explicit_typed_input_after_scoped_discard_keeps_ordinary_history_and_its_own_inference() {
    for watchdog_first in [false, true] {
        let user = user();
        let (owner, turn) = source();
        start(&user, &turn, 0);
        ordinary_final(&user, "keep", 1);
        let receipt = owner
            .offer_final(&turn, data(FrameData::Transcription, "discard"))
            .unwrap();
        feed(&user, final_frame(&receipt), 2);
        owner.revoke();
        if watchdog_first {
            non_speaking(&user, 3);
            assert_eq!(contexts(&user.advance(Duration::from_secs(6)).unwrap()), 0);
        }
        assert!(history(&user).is_empty());
        let output = feed(
            &user,
            Frame::new(FrameData::InputText(TextFrame::new("typed"))),
            6001,
        );
        assert_eq!(contexts(&output), 1);
        assert_eq!(
            history(&user),
            vec![
                (ChatRole::User, "keep".into()),
                (ChatRole::User, "typed".into())
            ]
        );
        assert_eq!(
            contexts(&feed(
                &user,
                Frame::new(FrameData::End(Default::default())),
                6002
            )),
            0
        );
        assert_eq!(history(&user).len(), 2);
    }
}

#[test]
fn live_and_unbound_terminal_empty_repeated_and_already_consumed_baselines() {
    for typed in [false, true] {
        for empty in [false, true] {
            for stop in [false, true] {
                let user = user();
                let (owner, turn) = source();
                if typed {
                    start(&user, &turn, 0);
                } else {
                    ordinary_start(&user, 0);
                }
                if !empty {
                    if typed {
                        let receipt = owner
                            .offer_final(&turn, data(FrameData::Transcription, "live"))
                            .unwrap();
                        feed(&user, final_frame(&receipt), 1);
                    } else {
                        ordinary_final(&user, "live", 1);
                    }
                }
                let terminal = if stop {
                    FrameData::Stop
                } else {
                    FrameData::End(Default::default())
                };
                assert_eq!(
                    contexts(&feed(&user, Frame::new(terminal), 2)),
                    usize::from(!empty)
                );
                let expected = if empty {
                    vec![]
                } else {
                    vec![(ChatRole::User, "live".into())]
                };
                assert_eq!(history(&user), expected);
                assert_eq!(
                    contexts(&feed(
                        &user,
                        Frame::new(FrameData::End(Default::default())),
                        3
                    )),
                    0
                );
                assert_eq!(contexts(&feed(&user, Frame::new(FrameData::Stop), 4)), 0);
                assert_eq!(history(&user), expected);
            }
        }
    }
    for confirm in [false, true] {
        let user = user();
        let (owner, turn) = source();
        start(&user, &turn, 0);
        let capability = confirm.then(|| {
            let prediction = owner
                .predict(&turn, data(FrameData::EagerTranscription, "once"))
                .unwrap();
            adopt(&user, &prediction, 1)
        });
        let receipt = owner
            .offer_final(&turn, data(FrameData::Transcription, "once"))
            .unwrap();
        feed(&user, final_frame(&receipt), 2);
        assert_eq!(
            contexts(&feed(&user, stop_frame(&turn, Some(&receipt)), 3)),
            usize::from(!confirm)
        );
        if let Some(capability) = capability {
            assert_eq!(capability.status(), SpeculationStatus::Confirmed);
        }
        owner.revoke();
        assert_eq!(
            contexts(&feed(
                &user,
                Frame::new(FrameData::End(Default::default())),
                4
            )),
            0
        );
        assert_eq!(contexts(&feed(&user, Frame::new(FrameData::Stop), 5)), 0);
        assert_eq!(history(&user), vec![(ChatRole::User, "once".into())]);
    }
}
