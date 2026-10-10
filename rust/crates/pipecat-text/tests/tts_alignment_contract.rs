use pipecat_text::*;

fn alignment(reporting: TokenReporting) -> TtsAlignment {
    TtsAlignment::new(
        7,
        true,
        reporting,
        Some("en"),
        TtsAlignmentLimits::default(),
    )
    .unwrap()
}
fn words(events: &[TtsAlignmentEvent]) -> Vec<(&str, u64)> {
    events
        .iter()
        .filter_map(|e| match &e.event {
            TokenSequencerEvent::Alignment(SequencerEvent::Word { completion, .. }) => {
                Some((completion.word.as_deref().unwrap_or(""), e.pts_ns))
            }
            _ => None,
        })
        .collect()
}
#[test]
fn early_words_wait_for_audio_then_follow_exact_anchor_with_zero_pts() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    assert!(bridge
        .register(&TtsTextChannels::identical("Hello world"), true)
        .unwrap()
        .is_empty());
    assert!(bridge.word("Hello", 0.0, false).unwrap().is_empty());
    assert!(bridge.word("world", 0.25, false).unwrap().is_empty());
    let out = bridge.start_audio(0, 0).unwrap();
    let TokenSequencerEvent::Promoted {
        context, segment, ..
    } = out[0].event
    else {
        panic!("anchor first");
    };
    assert_eq!(context, bridge.context());
    assert_eq!(out[0].pts_ns, 0);
    assert_eq!(words(&out), [("Hello", 0), ("world", 250_000_000)]);
    for event in &out[1..] {
        match event.event {
            TokenSequencerEvent::Alignment(SequencerEvent::Word { segment: id, .. })
            | TokenSequencerEvent::Progress { segment: id, .. } => assert_eq!(id, segment),
            _ => panic!("unexpected event"),
        }
    }
    assert!(bridge.start_audio(99, 100).unwrap().is_empty());
    assert_eq!(bridge.baseline(), Some(0));
    assert_eq!(bridge.boundary_pts().unwrap(), 250_000_001);
}
#[test]
fn early_words_before_sentence_promotion_replay_under_same_baseline() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    bridge
        .register(&TtsTextChannels::identical("Hello"), false)
        .unwrap();
    bridge.word("Hello", 0.0, false).unwrap();
    assert!(bridge.start_audio(100, 80).unwrap().is_empty());
    let out = bridge.finalize().unwrap();
    assert!(matches!(out[0].event, TokenSequencerEvent::Promoted { .. }));
    assert_eq!(out[0].pts_ns, 100);
    assert_eq!(words(&out), [("Hello", 100)]);
}
#[test]
fn transforms_preserve_original_markup_and_atomic_display_progress() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    bridge
        .register(
            &TtsTextChannels {
                synthesis: "Pay five dollars".into(),
                display: "Pay $5".into(),
                original: "Pay <b>$5</b>".into(),
            },
            true,
        )
        .unwrap();
    bridge.start_audio(0, 0).unwrap();
    let first = bridge.word("Pay", 0.0, false).unwrap();
    let TokenSequencerEvent::Alignment(SequencerEvent::Word { completion, .. }) = &first[0].event
    else {
        panic!();
    };
    assert_eq!(completion.source_text.as_deref(), Some("Pay"));
    let middle = bridge.word("five", 0.1, false).unwrap();
    assert!(!middle
        .iter()
        .any(|e| matches!(e.event, TokenSequencerEvent::Progress { .. })));
    let last = bridge.word("dollars", 0.2, false).unwrap();
    let TokenSequencerEvent::Alignment(SequencerEvent::Word { completion, .. }) = &last[0].event
    else {
        panic!();
    };
    assert_eq!(completion.source_text.as_deref(), Some(" <b>$5</b>"));
    assert!(
        matches!(&last[1].event, TokenSequencerEvent::Progress { accumulated_text, remaining_text, .. } if accumulated_text == "Pay $5" && remaining_text.is_empty())
    );
}
#[test]
fn skipped_text_follows_completion_and_never_becomes_word() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    bridge
        .register(&TtsTextChannels::identical("Hello"), true)
        .unwrap();
    bridge
        .skipped("secret code", "```secret code```", Some("speaker"))
        .unwrap();
    bridge.start_audio(0, 0).unwrap();
    let out = bridge.word("Hello", 0.0, false).unwrap();
    assert_eq!(words(&out), [("Hello", 0)]);
    assert!(
        matches!(&out.last().unwrap().event, TokenSequencerEvent::Alignment(SequencerEvent::Skipped { raw_text, transport_destination, .. }) if raw_text == "```secret code```" && transport_destination.as_deref() == Some("speaker"))
    );
}
#[test]
fn no_timestamp_completion_requires_success_and_is_always_diagnostic() {
    let mut bridge = alignment(TokenReporting::SynthesisCompletion);
    bridge
        .register(&TtsTextChannels::identical("Hello"), true)
        .unwrap();
    let anchors = bridge.start_audio(10, 0).unwrap();
    assert!(words(&anchors).is_empty());
    let done = bridge.finish_success(90, 0).unwrap();
    assert!(words(&done).is_empty());
    assert!(done.iter().any(|e| matches!(&e.event, TokenSequencerEvent::Alignment(SequencerEvent::Unreported { completion, .. }) if completion.source_text.as_deref() == Some("Hello"))));
    assert!(done
        .iter()
        .all(|e| e.synthesis_outcome == SynthesisOutcome::Succeeded));
    assert_eq!(
        bridge.word("Hello", 0.0, false),
        Err(TtsAlignmentError::Closed)
    );
}
#[test]
fn missing_provider_words_and_zero_audio_are_not_fabricated_word_events() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    bridge
        .register(&TtsTextChannels::identical("Hello world"), true)
        .unwrap();
    bridge.word("Hello", 0.0, false).unwrap();
    let done = bridge.finish_success(1, 0).unwrap();
    assert!(words(&done).is_empty());
    assert!(done.iter().any(|e| matches!(&e.event, TokenSequencerEvent::Alignment(SequencerEvent::Unreported { completion, .. }) if completion.source_text.as_deref() == Some("Hello world"))));
}
#[test]
fn cancel_discards_early_words_and_pending_anchors() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    bridge
        .register(&TtsTextChannels::identical("Hello"), true)
        .unwrap();
    bridge.word("Hello", 0.0, false).unwrap();
    bridge.cancel();
    assert_eq!(bridge.start_audio(0, 0), Err(TtsAlignmentError::Closed));
    assert_eq!(bridge.finish_success(0, 0), Err(TtsAlignmentError::Closed));
}
#[test]
fn timestamp_validation_and_baseline_addition_fail_closed() {
    for invalid in [f64::NAN, f64::INFINITY, -0.1, u64::MAX as f64 / 1e9] {
        assert_eq!(
            TtsAlignment::seconds_to_ns(invalid),
            Err(TtsAlignmentError::InvalidTimestamp)
        );
    }
    assert_eq!(TtsAlignment::seconds_to_ns(0.0).unwrap(), 0);
    assert_eq!(TtsAlignment::seconds_to_ns(0.000_000_000_9).unwrap(), 0);
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    bridge.word("Hello", 1.0, false).unwrap();
    assert_eq!(
        bridge.start_audio(u64::MAX - 10, 0),
        Err(TtsAlignmentError::TimestampOverflow)
    );
    assert!(bridge.is_closed());
}
#[test]
fn cross_context_baseline_respects_previous_terminal_word_even_if_clock_lags() {
    let mut first = alignment(TokenReporting::WordTimestamps);
    first
        .register(&TtsTextChannels::identical("one"), true)
        .unwrap();
    first.start_audio(100, 0).unwrap();
    first.word("one", 1.0, false).unwrap();
    first.finish_success(200, 0).unwrap();
    let terminal = first.boundary_pts().unwrap();
    let mut second = alignment(TokenReporting::WordTimestamps);
    second
        .register(&TtsTextChannels::identical("two"), true)
        .unwrap();
    second.word("two", 0.0, false).unwrap();
    let out = second.start_audio(200, terminal).unwrap();
    assert_eq!(words(&out), [("two", terminal)]);
    assert_ne!(first.context(), second.context());
}
#[test]
fn preaudio_and_output_budget_exhaustion_poison_instead_of_eviction() {
    let mut limits = TtsAlignmentLimits::default();
    limits.sequencer.max_buffered_words = 1;
    let mut bridge =
        TtsAlignment::new(0, true, TokenReporting::WordTimestamps, None, limits).unwrap();
    bridge.word("a", 0.0, false).unwrap();
    assert!(matches!(
        bridge.word("b", 0.0, false),
        Err(TtsAlignmentError::Alignment(AlignmentError::LimitExceeded(
            "pre-audio words"
        )))
    ));
    assert!(bridge.is_closed());
    let limits = TtsAlignmentLimits {
        max_pending_event_bytes: 2,
        ..Default::default()
    };
    let mut bridge =
        TtsAlignment::new(0, true, TokenReporting::WordTimestamps, None, limits).unwrap();
    assert!(bridge
        .register(&TtsTextChannels::identical("hello"), true)
        .is_err());
    assert!(bridge.is_closed());
}

#[test]
fn completed_aggregate_preserves_its_explicit_boundary_and_mapper_anchor_lifetime() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    let mut mapper = TtsAlignmentFrames::new("ctx".into(), bridge.context(), true, 1).unwrap();
    bridge.start_audio(0, 0).unwrap();
    for _ in 0..20 {
        let out = bridge
            .register_named(&TtsTextChannels::identical("One. Two."), true, "custom")
            .unwrap();
        let anchors = mapper.convert(out).unwrap();
        assert_eq!(anchors.len(), 1);
        assert!(
            matches!(&anchors[0].data, pipecat_core::FrameData::AggregatedText(t) if t.text.text == "One. Two." && t.aggregated_by == "custom")
        );
        let out = bridge.word("One. Two.", 0.0, false).unwrap();
        let frames = mapper.convert(out).unwrap();
        assert!(frames.iter().any(|f| matches!(&f.data, pipecat_core::FrameData::AggregatedTextProgress(p) if p.segment_id == anchors[0].id && p.aggregated_by == "custom")));
    }
}
#[test]
fn resynchronization_and_unreported_progress_never_convert_to_ordinary_word_frames() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    let mut mapper = TtsAlignmentFrames::new("ctx".into(), bridge.context(), true, 4).unwrap();
    bridge
        .register(&TtsTextChannels::identical("one two three"), true)
        .unwrap();
    mapper.convert(bridge.start_audio(0, 0).unwrap()).unwrap();
    let frames = mapper
        .convert(bridge.word("two", 0.1, false).unwrap())
        .unwrap();
    assert!(frames.iter().any(|f| matches!(&f.data, pipecat_core::FrameData::TtsAlignmentDiagnostic(d) if d.evidence == pipecat_core::TtsAlignmentEvidence::Resynchronized)));
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        pipecat_core::FrameData::TtsText(_) | pipecat_core::FrameData::AggregatedTextProgress(_)
    )));
    let frames = mapper
        .convert(bridge.finish_success(0, 0).unwrap())
        .unwrap();
    assert!(!frames.iter().any(|f| matches!(
        f.data,
        pipecat_core::FrameData::TtsText(_) | pipecat_core::FrameData::AggregatedTextProgress(_)
    )));
}

#[test]
fn mapper_rejects_another_opaque_context_even_with_identical_wire_context_name() {
    let first = alignment(TokenReporting::WordTimestamps);
    let mut other = alignment(TokenReporting::WordTimestamps);
    let mut mapper =
        TtsAlignmentFrames::new("same-wire-name".into(), first.context(), true, 4).unwrap();
    other
        .register(&TtsTextChannels::identical("hello"), true)
        .unwrap();
    let events = other.start_audio(0, 0).unwrap();
    assert_eq!(
        mapper.convert(events),
        Err(TtsAlignmentError::Alignment(AlignmentError::UnknownContext))
    );
}

#[test]
fn leading_skipped_text_releases_without_starting_word_clock() {
    let mut bridge = alignment(TokenReporting::WordTimestamps);
    let out = bridge
        .skipped_named("code", "```code```", None, "code")
        .unwrap();
    assert_eq!(out.len(), 1);
    assert!(matches!(
        &out[0].event,
        TokenSequencerEvent::Alignment(SequencerEvent::Skipped { .. })
    ));
    assert_eq!(out[0].aggregated_by, "code");
    assert_eq!(bridge.baseline(), None);
    bridge
        .register(&TtsTextChannels::identical("hello"), true)
        .unwrap();
    bridge.word("hello", 0.0, false).unwrap();
    let out = bridge.start_audio(50, 0).unwrap();
    assert_eq!(words(&out), [("hello", 50)]);
}
