//! Draft regression suite. Recovered after workspace replacement; never compiled or run.
use pipecat_core::*;
use pipecat_turns::*;
use std::time::Duration;

fn process(
    strategy: &mut LLMTurnCompletionStrategy,
    data: FrameData,
    seconds: u64,
) -> AggregationOutput {
    strategy
        .process_frame(
            Frame::new(data),
            Direction::Downstream,
            Duration::from_secs(seconds),
        )
        .unwrap()
}
fn text(strategy: &mut LLMTurnCompletionStrategy, value: &str, seconds: u64) -> AggregationOutput {
    process(strategy, FrameData::LlmText(TextFrame::llm(value)), seconds)
}
fn frames(output: &AggregationOutput) -> Vec<&FrameData> {
    output
        .actions
        .iter()
        .filter_map(|action| match action {
            AggregationAction::Push(frame) => Some(&frame.frame.data),
            AggregationAction::Broadcast(frame) => Some(&frame.data),
            _ => None,
        })
        .collect()
}
fn words(output: &AggregationOutput) -> String {
    frames(output)
        .iter()
        .filter_map(|data| match data {
            FrameData::LlmText(t) => Some(t.text.as_str()),
            _ => None,
        })
        .collect()
}
fn strategy() -> LLMTurnCompletionStrategy {
    LLMTurnCompletionStrategy::new(Default::default()).unwrap()
}
fn report(output: &AggregationOutput) -> &Frame {
    output
        .actions
        .iter()
        .find_map(|a| match a {
            AggregationAction::Push(f)
                if matches!(f.frame.data, FrameData::LlmMarkerResponse(_)) =>
            {
                Some(&f.frame)
            }
            _ => None,
        })
        .unwrap()
}
fn vad_start() -> FrameData {
    FrameData::VadUserStartedSpeakingDetailed(Default::default())
}
fn vad_stop() -> FrameData {
    FrameData::VadUserStoppedSpeakingDetailed(Default::default())
}

#[test]
fn complete_signal_precedes_marker_and_speech_then_reports_source_text() {
    let mut s = strategy();
    let out = text(&mut s, "● Hello", 0);
    assert!(matches!(
        frames(&out)[..],
        [
            FrameData::UserTurnInferenceCompleted,
            FrameData::LlmMarker(LlmMarkerFrame {
                append_to_context_immediately: false,
                ..
            }),
            FrameData::LlmText(_)
        ]
    ));
    assert_eq!(words(&out), "Hello");
    assert_eq!(words(&text(&mut s, " world", 1)), " world");
    let end = process(&mut s, FrameData::LlmResponseEnd, 2);
    assert!(
        matches!(&report(&end).data, FrameData::LlmMarkerResponse(r) if r.raw == "● Hello world" && r.kind.as_deref() == Some("complete"))
    );
    assert!(s.deadline().is_none());
}

#[test]
fn incomplete_markers_suppress_all_speech_and_timeout_once_with_source_prompts() {
    for (marker, delay, prompt) in [("◐", 5, "paused briefly"), ("○", 10, "quiet for a while")]
    {
        let mut s = strategy();
        let out = text(&mut s, &format!("{marker} forbidden speech"), 0);
        assert!(matches!(
            frames(&out)[..],
            [FrameData::LlmMarker(LlmMarkerFrame {
                append_to_context_immediately: true,
                ..
            })]
        ));
        assert!(text(&mut s, "● still suppressed", 1).actions.is_empty());
        process(&mut s, FrameData::LlmResponseEnd, 1);
        assert!(s
            .advance(Duration::from_secs(delay - 1))
            .unwrap()
            .actions
            .is_empty());
        let retry = s.advance(Duration::from_secs(delay)).unwrap();
        assert!(
            matches!(frames(&retry)[..], [FrameData::LlmMessagesAppend(m)] if m.run_llm && m.messages[0].role == ChatRole::Developer && m.messages[0].content.contains(prompt))
        );
        assert!(s
            .advance(Duration::from_secs(100))
            .unwrap()
            .actions
            .is_empty());
        process(&mut s, FrameData::LlmResponseStart, 101);
        text(&mut s, marker, 101);
        assert!(
            s.deadline().is_none(),
            "automatic retry budget is exhausted"
        );
    }
}

#[test]
fn custom_markers_span_chunks_and_custom_prompts_are_literal() {
    let config = LLMTurnCompletionConfig {
        complete_marker: "[DONE]".into(),
        incomplete_short_marker: "[WAIT]".into(),
        incomplete_long_marker: "[LATER]".into(),
        incomplete_short_prompt: Some("custom prompt".into()),
        ..Default::default()
    };
    assert!(config
        .completion_instructions()
        .contains("Write [DONE], a space"));
    assert!(!config.completion_instructions().contains('●'));
    assert_eq!(config.short_prompt(), "custom prompt");
    for split in 1..6 {
        let mut s = LLMTurnCompletionStrategy::new(config.clone()).unwrap();
        assert!(text(&mut s, &"[DONE]"[..split], 0).actions.is_empty());
        assert_eq!(
            words(&text(&mut s, &format!("{} okay", &"[DONE]"[split..]), 0)),
            "okay"
        );
    }
}

#[test]
fn missing_partial_nonleading_and_oversized_prefixes_do_not_authorize_completion() {
    for raw in ["unmarked text", "preamble ● no verdict", "[DO"] {
        let mut s = strategy();
        assert!(text(&mut s, raw, 0).actions.is_empty());
        let end = process(&mut s, FrameData::LlmResponseEnd, 1);
        assert_eq!(words(&end), raw);
        assert_eq!(report(&end).metadata["completion_malformed"], true);
        assert!(!frames(&end)
            .iter()
            .any(|f| matches!(f, FrameData::UserTurnInferenceCompleted)));
    }
    let mut s = LLMTurnCompletionStrategy::new(LLMTurnCompletionConfig {
        max_prefix_bytes: 8,
        max_raw_bytes: 9,
        ..Default::default()
    })
    .unwrap();
    let raw = "中文中文中文●";
    let out = text(&mut s, raw, 0);
    assert_eq!(words(&out), raw);
    assert!(s.retained_text_bytes() <= 8 + 9 + 2);
    let end = process(&mut s, FrameData::LlmResponseEnd, 1);
    assert_eq!(report(&end).metadata["completion_raw_truncated"], true);
    assert_eq!(report(&end).metadata["completion_prefix_exceeded"], true);
    assert!(
        matches!(&report(&end).data, FrameData::LlmMarkerResponse(r) if r.raw == "中文中" && r.marker.is_none())
    );
    assert!(s.retained_text_bytes() == 0);
}

#[test]
fn repeated_markers_are_reported_and_never_rebroadcast_completion() {
    let mut s = strategy();
    let first = text(&mut s, "● hi", 0);
    assert_eq!(
        frames(&first)
            .iter()
            .filter(|f| matches!(f, FrameData::UserTurnInferenceCompleted))
            .count(),
        1
    );
    assert_eq!(words(&text(&mut s, " ● again ◐", 0)), " ● again ◐");
    let end = process(&mut s, FrameData::LlmResponseEnd, 1);
    assert_eq!(report(&end).metadata["completion_extra_markers"], true);
    assert_eq!(report(&end).metadata["completion_malformed"], true);
}

#[test]
fn voiced_latch_is_per_speaking_segment_and_explicit_requested_run() {
    let mut s = strategy();
    text(&mut s, "● first", 0);
    process(&mut s, FrameData::LlmResponseEnd, 0);
    process(&mut s, FrameData::LlmResponseStart, 1);
    assert!(text(&mut s, "● duplicate", 1).actions.is_empty());
    let end = process(&mut s, FrameData::LlmResponseEnd, 1);
    assert_eq!(report(&end).metadata["completion_suppressed"], true);
    process(&mut s, vad_start(), 2);
    process(&mut s, vad_stop(), 3);
    process(&mut s, FrameData::LlmResponseStart, 3);
    assert_eq!(words(&text(&mut s, "● fresh", 3)), "fresh");
    process(&mut s, FrameData::LlmResponseEnd, 3);
    process(
        &mut s,
        FrameData::LlmMessagesAppend(MessagesFrame {
            messages: vec![],
            run_llm: true,
        }),
        4,
    );
    process(&mut s, FrameData::LlmResponseStart, 4);
    assert_eq!(words(&text(&mut s, "● check-in", 4)), "check-in");
}

#[test]
fn speech_invalidates_deadline_and_stale_complete_waits_for_silence() {
    let mut s = strategy();
    text(&mut s, "◐", 0);
    process(&mut s, FrameData::LlmResponseEnd, 0);
    process(&mut s, vad_start(), 4);
    assert!(s
        .advance(Duration::from_secs(8))
        .unwrap()
        .actions
        .is_empty());
    process(&mut s, FrameData::LlmResponseStart, 8);
    let out = text(&mut s, "● stale speech", 8);
    assert!(matches!(frames(&out)[..], [FrameData::LlmMarker(m)] if m.marker == "◐"));
    assert!(s.deadline().is_none());
    process(&mut s, FrameData::LlmResponseEnd, 8);
    process(&mut s, vad_stop(), 10);
    assert_eq!(s.deadline(), Some(Duration::from_secs(15)));
    assert_eq!(
        frames(&s.advance(Duration::from_secs(15)).unwrap()).len(),
        1
    );
}

#[test]
fn fresh_response_interruption_and_every_terminal_disarm_deadlines() {
    for control in [
        FrameData::LlmResponseStart,
        FrameData::Interruption,
        FrameData::End(EndFrame { reason: None }),
        FrameData::EndDetailed(Default::default()),
        FrameData::Stop,
        FrameData::Cancel(CancelFrame { reason: None }),
        FrameData::CancelDetailed(Default::default()),
    ] {
        let mut s = strategy();
        text(&mut s, "○", 0);
        process(&mut s, FrameData::LlmResponseEnd, 0);
        process(&mut s, control, 1);
        assert!(s.deadline().is_none());
        assert!(s
            .advance(Duration::from_secs(100))
            .unwrap()
            .actions
            .is_empty());
    }
    for control in [
        FrameData::Interruption,
        FrameData::Stop,
        FrameData::Cancel(CancelFrame { reason: None }),
    ] {
        let mut s = strategy();
        text(&mut s, "discard buffered text", 0);
        assert_eq!(words(&process(&mut s, control, 1)), "");
    }
    let mut s = strategy();
    text(&mut s, "drain on end", 0);
    assert_eq!(
        words(&process(
            &mut s,
            FrameData::End(EndFrame { reason: None }),
            1
        )),
        "drain on end"
    );
    assert!(text(&mut s, "● late", 2).actions.is_empty());
}

#[test]
fn native_continuation_start_fails_closed_before_context_effects() {
    let mut s = strategy();
    let mut start = Frame::new(FrameData::LlmResponseStart);
    start
        .set_native_llm_continuation(std::sync::Arc::new(
            NativeLlmContinuation::start(1, LlmContinuationProvider::Gemini).unwrap(),
        ))
        .unwrap();
    let error = s
        .process_frame(start, Direction::Downstream, Duration::ZERO)
        .unwrap_err();
    assert!(error.fatal);
    assert!(s.is_closed());
    assert!(text(&mut s, "● cannot pass", 0).actions.is_empty());
}

#[test]
fn invalid_configuration_is_rejected_and_clocks_never_go_backwards() {
    let invalid = [
        LLMTurnCompletionConfig {
            complete_marker: "".into(),
            ..Default::default()
        },
        LLMTurnCompletionConfig {
            complete_marker: "◐".into(),
            ..Default::default()
        },
        LLMTurnCompletionConfig {
            complete_marker: "◐more".into(),
            ..Default::default()
        },
        LLMTurnCompletionConfig {
            incomplete_short_timeout: Duration::ZERO,
            ..Default::default()
        },
        LLMTurnCompletionConfig {
            max_raw_bytes: 0,
            ..Default::default()
        },
        LLMTurnCompletionConfig {
            max_reprompts_per_segment: 65,
            ..Default::default()
        },
    ];
    for config in invalid {
        assert!(LLMTurnCompletionStrategy::new(config).is_err());
    }
    let mut s = strategy();
    process(&mut s, FrameData::LlmResponseStart, 20);
    text(&mut s, "◐", 1);
    assert_eq!(s.deadline(), Some(Duration::from_secs(25)));
}

#[test]
fn unmarked_prefix_preserves_each_frames_flags_and_routing_envelope() {
    let mut s = strategy();
    let mut first = Frame::new(FrameData::LlmText(TextFrame {
        text: "first".into(),
        skip_tts: Some(true),
        includes_inter_frame_spaces: false,
        append_to_context: false,
    }));
    first.transport_source = Some("source-a".into());
    first.transport_destination = Some("speaker-a".into());
    first.pts = Some(17);
    first.metadata.insert("tag".into(), "first".into());
    let tag = LocalFrameTag::new();
    tag.mark(&mut first).unwrap();
    let mut second = Frame::new(FrameData::LlmText(TextFrame::llm(" second")));
    second.transport_destination = Some("speaker-b".into());
    assert!(s
        .process_frame(first.clone(), Direction::Downstream, Duration::ZERO)
        .unwrap()
        .actions
        .is_empty());
    assert!(s
        .process_frame(second.clone(), Direction::Downstream, Duration::ZERO)
        .unwrap()
        .actions
        .is_empty());
    let out = process(&mut s, FrameData::LlmResponseEnd, 1);
    let saved: Vec<&Frame> = out
        .actions
        .iter()
        .filter_map(|a| match a {
            AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmText(_)) => {
                Some(&f.frame)
            }
            _ => None,
        })
        .collect();
    assert_eq!(saved, [&first, &second]);
    assert!(tag.contains(saved[0]));
}

#[test]
fn partial_custom_marker_removal_preserves_speech_frame_flags() {
    let mut s = LLMTurnCompletionStrategy::new(LLMTurnCompletionConfig {
        complete_marker: "[DONE]".into(),
        ..Default::default()
    })
    .unwrap();
    text(&mut s, "[DO", 0);
    let mut body = Frame::new(FrameData::LlmText(TextFrame {
        text: "NE] hello".into(),
        skip_tts: Some(true),
        includes_inter_frame_spaces: false,
        append_to_context: false,
    }));
    body.transport_destination = Some("private".into());
    let id = body.id;
    let out = s
        .process_frame(body, Direction::Downstream, Duration::ZERO)
        .unwrap();
    let body = out
        .actions
        .iter()
        .find_map(|a| match a {
            AggregationAction::Push(f) if matches!(f.frame.data, FrameData::LlmText(_)) => {
                Some(&f.frame)
            }
            _ => None,
        })
        .unwrap();
    assert_eq!(body.id, id);
    assert_eq!(body.transport_destination.as_deref(), Some("private"));
    assert!(
        matches!(&body.data, FrameData::LlmText(t) if t.text == "hello" && t.skip_tts == Some(true) && !t.includes_inter_frame_spaces && !t.append_to_context)
    );
}

#[test]
fn late_chunks_cannot_start_a_response_after_end_or_interruption() {
    for boundary in [
        FrameData::LlmResponseEnd,
        FrameData::LlmFullResponseEnd(Default::default()),
        FrameData::Interruption,
    ] {
        let mut s = strategy();
        text(&mut s, "prefix", 0);
        process(&mut s, boundary, 1);
        assert!(text(&mut s, "● late", 2).actions.is_empty());
        assert_eq!(s.retained_text_bytes(), 0);
        process(
            &mut s,
            FrameData::LlmFullResponseStart(Default::default()),
            3,
        );
        assert_eq!(words(&text(&mut s, "● fresh", 3)), "fresh");
    }
}

#[test]
fn a_stalled_incomplete_response_closes_without_a_synthetic_response_end_or_retry() {
    let mut s = strategy();
    text(&mut s, "◐ unfinished", 0);
    let error = s.advance(Duration::from_secs(5)).unwrap_err();
    assert!(error.fatal && error.message.contains("did not end"));
    assert!(s.is_closed());
    assert_eq!(s.retained_text_bytes(), 0);
    assert!(s
        .advance(Duration::from_secs(100))
        .unwrap()
        .actions
        .is_empty());
    process(&mut s, FrameData::LlmResponseStart, 100);
    assert!(text(&mut s, "● too late", 100).actions.is_empty());
}

#[test]
fn empty_chunks_report_nothing_and_replayed_silence_does_not_extend_wait() {
    let mut s = strategy();
    text(&mut s, "", 0);
    let end = process(&mut s, FrameData::LlmResponseEnd, 0);
    assert!(!frames(&end)
        .iter()
        .any(|f| matches!(f, FrameData::LlmMarkerResponse(_))));
    process(&mut s, vad_start(), 1);
    process(&mut s, FrameData::LlmResponseStart, 2);
    text(&mut s, "● stale", 2);
    process(&mut s, FrameData::LlmResponseEnd, 2);
    process(&mut s, vad_stop(), 3);
    process(&mut s, vad_stop(), 4);
    assert_eq!(s.deadline(), Some(Duration::from_secs(8)));
}

#[test]
fn invalid_native_checkpoint_closes_the_reducer_and_no_text_follows() {
    let mut s = strategy();
    let error = s
        .process_frame(
            Frame::new(FrameData::LlmContinuationCheckpoint),
            Direction::Downstream,
            Duration::ZERO,
        )
        .unwrap_err();
    assert!(error.fatal);
    assert!(s.is_closed());
    assert!(text(&mut s, "● forbidden", 1).actions.is_empty());
}

#[test]
fn core_wire_config_conversion_is_checked_and_marker_rendering_is_one_pass() {
    let config =
        LLMTurnCompletionConfig::try_from(pipecat_core::UserTurnCompletionConfig::default())
            .unwrap();
    assert_eq!(config.incomplete_short_timeout, Duration::from_secs(5));
    for invalid in [
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -1.0,
        0.0,
        86_401.0,
        f64::MIN_POSITIVE,
    ] {
        assert!(
            LLMTurnCompletionConfig::try_from(pipecat_core::UserTurnCompletionConfig {
                incomplete_short_timeout: invalid,
                ..Default::default()
            })
            .is_err()
        );
    }
    let config = LLMTurnCompletionConfig {
        complete_marker: "{short}".into(),
        incomplete_short_marker: "N".into(),
        incomplete_long_marker: "W".into(),
        ..Default::default()
    };
    assert!(config.short_prompt().contains("respond with {short}"));
    assert!(LLMTurnCompletionStrategy::new(config).is_ok());
}

#[test]
fn strategy_completion_signal_finalizes_an_existing_deferred_stop_policy() {
    let mut s = strategy();
    let mut controller = UserTurnController::new(UserTurnConfig {
        enable_interruptions: false,
        stop_strategies: Some(
            UserTurnStopStrategies::new(vec![
                UserTurnStopStrategy::speech_timeout(Duration::from_millis(100), true).deferred(),
                s.stop_strategy(),
            ])
            .unwrap(),
        ),
        ..Default::default()
    });
    controller.process(
        &FrameData::Transcription(TranscriptionFrame {
            text: "hello".into(),
            finalized: true,
            ..Default::default()
        }),
        Duration::ZERO,
    );
    let proposed = controller.advance(Duration::from_millis(100));
    assert!(proposed
        .iter()
        .any(|e| matches!(e, UserTurnEvent::InferenceRequested { .. })));
    assert!(!proposed
        .iter()
        .any(|e| matches!(e, UserTurnEvent::Stopped { .. })));
    let incomplete = text(&mut s, "◐", 1);
    assert!(!frames(&incomplete)
        .iter()
        .any(|f| matches!(f, FrameData::UserTurnInferenceCompleted)));
    process(&mut s, FrameData::LlmResponseEnd, 1);
    process(&mut s, FrameData::LlmResponseStart, 2);
    let complete = text(&mut s, "● final", 2);
    let signal = frames(&complete)
        .into_iter()
        .find(|f| matches!(f, FrameData::UserTurnInferenceCompleted))
        .unwrap();
    let stopped = controller.process(signal, Duration::from_secs(2));
    assert!(stopped.iter().any(|e| matches!(
        e,
        UserTurnEvent::Stopped {
            reason: TurnStopReason::ExternalCompletion,
            ..
        }
    )));
}
