//! Native TOKEN sequencer contracts: source behavior plus bounded admission and evidence.
use pipecat_text::*;
fn seq() -> TokenFrameSequencer {
    TokenFrameSequencer::new(12, TokenSequencerLimits::default()).unwrap()
}
fn open(s: &mut TokenFrameSequencer) -> ContextId {
    s.open_context(true, TokenReporting::WordTimestamps, None)
        .unwrap()
}
fn token(s: &mut TokenFrameSequencer, c: ContextId, t: &str) -> Vec<TokenSequencerEvent> {
    s.register_token(c, t, t, t).unwrap()
}
fn anchors(e: &[TokenSequencerEvent]) -> Vec<&ParallelSentence> {
    e.iter()
        .filter_map(|e| match e {
            TokenSequencerEvent::Promoted { sentence, .. } => Some(sentence),
            _ => None,
        })
        .collect()
}
fn words(e: &[TokenSequencerEvent]) -> Vec<&WordCompletion> {
    e.iter()
        .filter_map(|e| match e {
            TokenSequencerEvent::Alignment(
                SequencerEvent::Word { completion, .. }
                | SequencerEvent::Unreported { completion, .. },
            ) => Some(completion),
            _ => None,
        })
        .collect()
}
#[test]
fn punctuation_waits_for_lookahead_and_final_partial_stays_live() {
    let mut s = seq();
    let c = open(&mut s);
    for t in ["Hi", " there", "!"] {
        assert!(token(&mut s, c, t).is_empty());
    }
    assert_eq!(anchors(&token(&mut s, c, " How"))[0].tts_text, "Hi there!");
    assert_eq!(
        words(&s.process_word(c, "Hi", 10, false).unwrap())[0]
            .source_text
            .as_deref(),
        Some("Hi")
    );
    assert_eq!(
        words(&s.process_word(c, "there!", 20, false).unwrap())[0]
            .source_text
            .as_deref(),
        Some(" there!")
    );
    assert_eq!(anchors(&s.finalize(c).unwrap())[0].tts_text, " How");
    assert_eq!(s.live_contexts(), 1);
    assert!(s.finalize(c).unwrap().is_empty());
    assert_eq!(
        words(&s.process_word(c, "How", 30, false).unwrap())[0]
            .source_text
            .as_deref(),
        Some(" How")
    );
    assert!(s.close_context(c, 40).unwrap().is_empty());
    assert_eq!(
        (s.pending_slots(), s.pending_text_bytes(), s.live_contexts()),
        (0, 0, 0)
    );
}
#[test]
fn coarse_chunks_preserve_spacing_and_transform_cuts_recover() {
    let mut s = seq();
    let c = open(&mut s);
    let mut out = token(&mut s, c, "One. Two. Three");
    out.extend(s.finalize(c).unwrap());
    assert_eq!(
        anchors(&out)
            .iter()
            .map(|a| a.tts_text.as_str())
            .collect::<Vec<_>>(),
        ["One.", " Two.", " Three"]
    );
    for a in anchors(&out) {
        assert_eq!(a.tts_text, a.llm_text);
        assert_eq!(a.tts_text, a.user_facing_text);
    }
    s.interrupt().unwrap();
    let c = open(&mut s);
    s.register_token(c, "five dollars", "$5", "$5").unwrap();
    let mut out = token(&mut s, c, " spent! More");
    out.extend(token(&mut s, c, " stuff."));
    out.extend(token(&mut s, c, " X"));
    out.extend(s.finalize(c).unwrap());
    assert_eq!(
        anchors(&out)
            .iter()
            .map(|a| a.tts_text.as_str())
            .collect::<Vec<_>>(),
        ["five dollars", " spent! More stuff.", " X"]
    );
    assert_eq!(
        anchors(&out)
            .iter()
            .map(|a| a.llm_text.as_str())
            .collect::<String>(),
        "$5 spent! More stuff. X"
    );
    s.interrupt().unwrap();
    let c = open(&mut s);
    s.register_token(c, "five dollars", "$5", "$5").unwrap();
    token(&mut s, c, " spent!");
    let mut out = token(&mut s, c, " Buy");
    out.extend(token(&mut s, c, " more! Now"));
    assert_eq!(
        anchors(&out)
            .iter()
            .map(|a| a.tts_text.as_str())
            .collect::<Vec<_>>(),
        ["five dollars spent!", " Buy more!"]
    );
}
#[test]
fn early_replay_has_anchor_first_original_zero_pts_and_progress() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "Hello world!");
    assert!(s.process_word(c, "Hello", 0, false).unwrap().is_empty());
    assert_eq!(s.buffered_words(), 1);
    let out = token(&mut s, c, " Next");
    let TokenSequencerEvent::Promoted { segment, .. } = out[0] else {
        panic!()
    };
    assert!(
        matches!(&out[1],TokenSequencerEvent::Alignment(SequencerEvent::Word {segment:id,pts_ns:0,..}) if *id==segment)
    );
    assert!(
        matches!(&out[2],TokenSequencerEvent::Progress {segment:id,accumulated_text,remaining_text,..} if *id==segment&&accumulated_text=="Hello"&&remaining_text==" world!")
    );
    assert_eq!(s.buffered_words(), 0);
}
#[test]
fn still_early_word_rebuffers_until_later_promotion_without_spinning() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "Hello!");
    s.process_word(c, "Later", 4, false).unwrap();
    let out = token(&mut s, c, " Later");
    assert_eq!(anchors(&out).len(), 1);
    assert!(words(&out).is_empty());
    assert_eq!(s.buffered_words(), 1);
    let out = s.finalize(c).unwrap();
    assert_eq!(words(&out).len(), 2);
    assert_eq!(
        words(&out)[0].evidence,
        AlignmentEvidence::UnreportedRemainder
    );
    assert_eq!(words(&out)[1].word.as_deref(), Some("Later"));
    assert_eq!(s.buffered_words(), 0);
}
#[test]
fn skipped_finalizes_before_registering_and_waits_for_words() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "prefix");
    let (_, out) = s
        .register_skipped(c, "code", "<code>code</code>", Some("speaker"))
        .unwrap();
    assert_eq!(anchors(&out)[0].tts_text, "prefix");
    assert_eq!(out.len(), 1);
    assert_eq!(s.pending_slots(), 2);
    let out = s.process_word(c, "prefix", 0, false).unwrap();
    assert!(
        matches!(&out[..],[TokenSequencerEvent::Alignment(SequencerEvent::Word {..}),TokenSequencerEvent::Progress {..},TokenSequencerEvent::Alignment(SequencerEvent::Skipped {pts_ns:Some(0),transport_destination:Some(d),..})] if d=="speaker")
    );
    token(&mut s, c, "after");
    assert_eq!(anchors(&s.finalize(c).unwrap())[0].tts_text, "after");
}
#[test]
fn skipped_registration_replays_words_before_immediate_release() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "prefix");
    s.process_word(c, "prefix", 0, false).unwrap();
    let (_, out) = s.register_skipped(c, "code", "code", None).unwrap();
    assert!(matches!(
        &out[..],
        [
            TokenSequencerEvent::Promoted { .. },
            TokenSequencerEvent::Alignment(SequencerEvent::Word { .. }),
            TokenSequencerEvent::Progress { .. },
            TokenSequencerEvent::Alignment(SequencerEvent::Skipped { .. })
        ]
    ));
}
#[test]
fn concurrent_pending_contexts_keep_words_spacing_and_append_intent_separate() {
    let mut s = seq();
    let a = open(&mut s);
    let b = s
        .open_context(false, TokenReporting::WordTimestamps, Some("zh"))
        .unwrap();
    token(&mut s, a, "one");
    token(&mut s, b, "你好世界");
    s.process_word(b, "你好", 8, true).unwrap();
    s.process_word(a, "one", 9, false).unwrap();
    let out = s.finalize(a).unwrap();
    assert!(
        matches!(&out[1],TokenSequencerEvent::Alignment(SequencerEvent::Word {context,append_to_context:true,includes_inter_frame_spaces:false,..}) if *context==a)
    );
    assert_eq!(s.buffered_words(), 1);
    let out = s.finalize(b).unwrap();
    assert!(
        matches!(&out[1],TokenSequencerEvent::Alignment(SequencerEvent::Word {context,append_to_context:false,includes_inter_frame_spaces:true,..}) if *context==b)
    );
    let out = s.close_context(b, 10).unwrap();
    assert!(
        matches!(&out[0],TokenSequencerEvent::Alignment(SequencerEvent::Unreported {context,includes_inter_frame_spaces:true,..}) if *context==b)
    );
    assert_eq!(s.live_contexts(), 1);
}
#[test]
fn transformed_progress_holds_then_jumps_with_exact_source_span() {
    let mut s = seq();
    let c = open(&mut s);
    s.register_token(
        c,
        "Total is five dollars.",
        "Total is $5.",
        "Total is <b>$5</b>.",
    )
    .unwrap();
    let out = s.finalize(c).unwrap();
    let TokenSequencerEvent::Promoted { segment, .. } = out[0] else {
        panic!()
    };
    s.process_word(c, "Total", 0, false).unwrap();
    s.process_word(c, "is", 1, false).unwrap();
    let held = s.process_word(c, "five", 2, false).unwrap();
    assert_eq!(held.len(), 1);
    assert!(words(&held)[0].suppress_in_context);
    assert_eq!(words(&held)[0].source_span, None);
    let out = s.process_word(c, "dollars", 3, false).unwrap();
    assert_eq!(words(&out)[0].source_text.as_deref(), Some(" <b>$5</b>."));
    assert!(
        matches!(&out[1],TokenSequencerEvent::Progress {segment:id,accumulated_text,remaining_text,..} if *id==segment&&accumulated_text=="Total is $5."&&remaining_text.is_empty())
    );
}
#[test]
fn no_timestamp_completion_is_explicit_unreported_and_never_a_word() {
    let mut s = seq();
    let c = s
        .open_context(true, TokenReporting::SynthesisCompletion, None)
        .unwrap();
    assert_eq!(token(&mut s, c, "Hello. Next").len(), 1);
    let (_, out) = s.register_skipped(c, "code", "code", None).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(s.pending_slots(), 3);
    assert_eq!(
        s.process_word(c, "Hello", 0, false),
        Err(AlignmentError::UnsupportedOperation(
            "word timestamps disabled"
        ))
    );
    let out = s.complete_synthesis(c, 40).unwrap();
    assert_eq!(words(&out).len(), 2);
    for w in words(&out) {
        assert_eq!(w.evidence, AlignmentEvidence::UnreportedRemainder);
        assert_eq!(w.raw_pos, 0);
    }
    assert!(!out.iter().any(|e| matches!(
        e,
        TokenSequencerEvent::Alignment(SequencerEvent::Word { .. })
    )));
    assert!(matches!(
        out.last(),
        Some(TokenSequencerEvent::Alignment(
            SequencerEvent::Skipped { .. }
        ))
    ));
    assert_eq!(
        (s.pending_slots(), s.pending_text_bytes(), s.live_contexts()),
        (0, 0, 0)
    );
    assert_eq!(
        s.complete_synthesis(c, 41),
        Err(AlignmentError::ContextClosed)
    );
}
#[test]
fn closing_exposes_unmatched_diagnostics_and_revokes_context() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "hello");
    s.process_word(c, "intruder", 0, false).unwrap();
    let out = s.close_context(c, 1).unwrap();
    assert!(
        matches!(&out[1],TokenSequencerEvent::Alignment(SequencerEvent::UnmatchedWord {word,..}) if word=="intruder")
    );
    assert_eq!(
        words(&out)[0].evidence,
        AlignmentEvidence::UnreportedRemainder
    );
    assert_eq!(s.buffered_words(), 0);
    assert_eq!(
        s.process_word(c, "hello", 2, false),
        Err(AlignmentError::ContextClosed)
    );
}
#[test]
fn interruption_clears_every_pending_layer_and_fences_all_old_ids() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "one. pending");
    s.process_word(c, "future", 0, false).unwrap();
    assert_eq!(s.interrupt().unwrap(), 13);
    assert_eq!(
        (
            s.pending_slots(),
            s.pending_text_bytes(),
            s.buffered_words(),
            s.live_contexts()
        ),
        (0, 0, 0, 0)
    );
    assert_eq!(s.finalize(c), Err(AlignmentError::StaleGeneration));
    assert_eq!(
        s.process_word(c, "one", 0, false),
        Err(AlignmentError::StaleGeneration)
    );
    let fresh = open(&mut s);
    assert_ne!(fresh, c);
    token(&mut s, fresh, "new");
    assert_eq!(anchors(&s.finalize(fresh).unwrap())[0].tts_text, "new");
}
#[test]
fn foreign_contexts_fail_closed_and_generation_exhaustion_is_atomic() {
    let mut a = seq();
    let mut b = seq();
    let ca = open(&mut a);
    let cb = open(&mut b);
    assert_eq!(
        a.register_token(cb, "x", "x", "x"),
        Err(AlignmentError::UnknownContext)
    );
    assert_eq!(
        b.process_word(ca, "x", 0, false),
        Err(AlignmentError::UnknownContext)
    );
    let mut s = TokenFrameSequencer::new(u64::MAX, TokenSequencerLimits::default()).unwrap();
    let c = open(&mut s);
    token(&mut s, c, "one");
    s.process_word(c, "one", 0, false).unwrap();
    let bytes = s.pending_text_bytes();
    assert_eq!(s.interrupt(), Err(AlignmentError::IdentityExhausted));
    assert_eq!(s.pending_text_bytes(), bytes);
    assert_eq!(
        words(&s.finalize(c).unwrap())[0].word.as_deref(),
        Some("one")
    );
}
#[test]
fn early_word_count_and_byte_backpressure_preserve_retry_state() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            max_buffered_words: 1,
            max_buffered_word_bytes: 5,
            ..Default::default()
        },
    )
    .unwrap();
    let c = open(&mut s);
    assert_eq!(
        s.process_word(c, "longer", 0, false),
        Err(AlignmentError::LimitExceeded("buffered word bytes"))
    );
    assert_eq!(s.buffered_words(), 0);
    s.process_word(c, "hello", 1, false).unwrap();
    let bytes = s.pending_text_bytes();
    assert_eq!(
        s.process_word(c, "world", 2, false),
        Err(AlignmentError::LimitExceeded("buffered words"))
    );
    assert_eq!(s.pending_text_bytes(), bytes);
    token(&mut s, c, "hello. world");
    assert_eq!(s.buffered_words(), 0);
    assert!(s.process_word(c, "world", 2, false).unwrap().is_empty());
    assert_eq!(
        words(&s.finalize(c).unwrap())[0].word.as_deref(),
        Some("world")
    );
}
#[test]
fn multi_promotion_slot_backpressure_rolls_back_input_and_segment_identity() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            sequencer: SequencerLimits {
                max_slots: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let c = open(&mut s);
    assert_eq!(
        s.register_token(c, "One. Two. Three", "One. Two. Three", "One. Two. Three"),
        Err(AlignmentError::LimitExceeded("slots"))
    );
    assert_eq!((s.pending_slots(), s.pending_text_bytes()), (0, 0));
    let out = token(&mut s, c, "One. Two.");
    let TokenSequencerEvent::Promoted { segment, .. } = out[0] else {
        panic!()
    };
    assert_eq!(segment.serial(), 0);
    s.process_word(c, "One", 0, false).unwrap();
    assert_eq!(anchors(&token(&mut s, c, " Three"))[0].tts_text, " Two.");
}
#[test]
fn rejected_skipped_registration_does_not_commit_finalization_or_replay() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            sequencer: SequencerLimits {
                max_slots: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let c = open(&mut s);
    token(&mut s, c, "one two");
    s.process_word(c, "one", 0, false).unwrap();
    let bytes = s.pending_text_bytes();
    assert_eq!(
        s.register_skipped(c, "code", "code", None),
        Err(AlignmentError::LimitExceeded("slots"))
    );
    assert_eq!(
        (
            s.pending_slots(),
            s.pending_text_bytes(),
            s.buffered_words()
        ),
        (0, bytes, 1)
    );
    assert_eq!(
        words(&s.finalize(c).unwrap())[0].source_text.as_deref(),
        Some("one")
    );
}
#[test]
fn total_byte_budget_includes_early_words_and_sentence_driver_copies() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            sequencer: SequencerLimits {
                max_pending_text_bytes: 20,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let c = open(&mut s);
    token(&mut s, c, "hello");
    assert_eq!(s.pending_text_bytes(), 20);
    assert_eq!(
        s.process_word(c, "hello", 0, false),
        Err(AlignmentError::LimitExceeded("pending text bytes"))
    );
    assert_eq!(
        s.register_token(c, "x", "x", "x"),
        Err(AlignmentError::LimitExceeded("pending text bytes"))
    );
    assert_eq!(s.buffered_words(), 0);
    assert_eq!(s.pending_text_bytes(), 20);
    s.finalize(c).unwrap();
    s.process_word(c, "hello", 0, false).unwrap();
    assert_eq!(s.pending_text_bytes(), 0);
}
#[test]
fn punctuation_free_input_has_byte_and_token_limits_before_sentence_detection() {
    let limits = AlignmentLimits {
        max_text_bytes: 5,
        max_tokens: 2,
        ..Default::default()
    };
    let mut a = ParallelSentenceAggregator::new(None, limits).unwrap();
    a.aggregate("abc", "abc", "abc").unwrap();
    assert_eq!(
        a.aggregate("def", "def", "def"),
        Err(AlignmentError::LimitExceeded("text bytes"))
    );
    assert_eq!(a.pending().tts_text, "abc");
    a.interrupt();
    a.aggregate("a b", "a b", "a b").unwrap();
    assert_eq!(
        a.aggregate(" c", " c", " c"),
        Err(AlignmentError::LimitExceeded("tokens"))
    );
    assert_eq!(a.pending().tts_text, "a b");
}
#[test]
fn bad_projection_does_not_consume_identity_or_clear_pending_text() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "hello");
    s.register_token(c, " world", " world", " wrong").unwrap();
    let bytes = s.pending_text_bytes();
    assert_eq!(s.finalize(c), Err(AlignmentError::UnalignedLlmText));
    assert_eq!(s.pending_slots(), 0);
    assert_eq!(s.pending_text_bytes(), bytes);
    s.interrupt().unwrap();
    let c = open(&mut s);
    token(&mut s, c, "valid");
    let out = s.finalize(c).unwrap();
    assert!(matches!(out[0],TokenSequencerEvent::Promoted{segment,..} if segment.serial()==0));
}
#[test]
fn overflow_suffix_replays_with_original_spacing_metadata_and_pts() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "11");
    s.finalize(c).unwrap();
    let out = s.process_word(c, "1122", 5, true).unwrap();
    assert_eq!(words(&out)[0].word.as_deref(), Some("11"));
    assert_eq!(s.buffered_words(), 1);
    token(&mut s, c, "22");
    let out = s.finalize(c).unwrap();
    assert_eq!(words(&out)[0].word.as_deref(), Some("22"));
    assert!(matches!(
        &out[1],
        TokenSequencerEvent::Alignment(SequencerEvent::Word {
            pts_ns: 5,
            includes_inter_frame_spaces: true,
            ..
        })
    ));
}
#[test]
fn whitespace_finalize_releases_storage_without_an_anchor() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "   ");
    assert!(s.finalize(c).unwrap().is_empty());
    assert_eq!(s.pending_text_bytes(), 0);
    token(&mut s, c, "hello");
    assert_eq!(anchors(&s.finalize(c).unwrap())[0].tts_text, "hello");
}
#[test]
fn repeated_provider_tokens_cannot_be_deduplicated_by_pts_or_text() {
    let mut s = seq();
    let c = open(&mut s);
    token(&mut s, c, "ha ha");
    s.finalize(c).unwrap();
    assert_eq!(
        words(&s.process_word(c, "ha", 0, false).unwrap())[0]
            .source_text
            .as_deref(),
        Some("ha")
    );
    assert_eq!(
        words(&s.process_word(c, "ha", 0, false).unwrap())[0]
            .source_text
            .as_deref(),
        Some(" ha")
    );
}
#[test]
fn exact_no_timestamp_segment_completion_releases_slots_without_closing_context() {
    let mut s = seq();
    let a = s
        .open_context(true, TokenReporting::SynthesisCompletion, None)
        .unwrap();
    let b = s
        .open_context(true, TokenReporting::SynthesisCompletion, None)
        .unwrap();
    let out = token(&mut s, a, "One. More");
    let TokenSequencerEvent::Promoted { segment, .. } = out[0] else {
        panic!()
    };
    assert_eq!(
        s.complete_synthesis_segment(b, segment, 0),
        Err(AlignmentError::UnknownSegment)
    );
    let out = s.complete_synthesis_segment(a, segment, 0).unwrap();
    assert_eq!(
        words(&out)[0].evidence,
        AlignmentEvidence::UnreportedRemainder
    );
    assert_eq!(words(&out)[0].raw_pos, 0);
    assert_eq!(s.live_contexts(), 2);
    assert_eq!(s.pending_slots(), 0);
    assert_eq!(
        s.complete_synthesis_segment(a, segment, 0),
        Err(AlignmentError::UnknownSegment)
    );
    token(&mut s, a, " text");
    assert_eq!(anchors(&s.finalize(a).unwrap())[0].tts_text, " More text");
    s.complete_synthesis(a, 10).unwrap();
    s.complete_synthesis(b, 10).unwrap();
    assert_eq!(s.pending_text_bytes(), 0);
}
#[test]
fn matched_prefix_and_early_overflow_backpressure_are_one_atomic_word_event() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            max_buffered_words: 1,
            ..Default::default()
        },
    )
    .unwrap();
    let c = open(&mut s);
    token(&mut s, c, "11");
    s.finalize(c).unwrap();
    s.process_word(c, "future", 0, false).unwrap();
    let bytes = s.pending_text_bytes();
    assert_eq!(
        s.process_word(c, "1122", 1, false),
        Err(AlignmentError::LimitExceeded("buffered words"))
    );
    assert_eq!(s.pending_text_bytes(), bytes);
    assert_eq!(s.pending_slots(), 1);
    let out = s.process_word(c, "11", 2, false).unwrap();
    assert_eq!(words(&out)[0].source_text.as_deref(), Some("11"));
}

#[test]
fn unreported_completion_of_transform_has_explicit_reconstruction_progress() {
    for reported_prefix in [false, true] {
        let mut s = seq();
        let c = s
            .open_context(
                true,
                if reported_prefix {
                    TokenReporting::WordTimestamps
                } else {
                    TokenReporting::SynthesisCompletion
                },
                None,
            )
            .unwrap();
        s.register_token(c, "five dollars", "$5", "<b>$5</b>")
            .unwrap();
        s.finalize(c).unwrap();
        if reported_prefix {
            s.process_word(c, "five", 0, false).unwrap();
        }
        let out = s.close_context(c, 1).unwrap();
        assert!(
            matches!(&out[0],TokenSequencerEvent::Alignment(SequencerEvent::Unreported {completion,..}) if completion.source_text.as_deref()==Some("<b>$5</b>"))
        );
        assert!(
            matches!(&out[1],TokenSequencerEvent::Progress {accumulated_text,remaining_text,evidence:AlignmentEvidence::UnreportedRemainder,..} if accumulated_text=="$5" && remaining_text.is_empty())
        );
        assert!(!out.iter().any(|event| matches!(
            event,
            TokenSequencerEvent::Alignment(SequencerEvent::Word { .. })
        )));
    }
}

#[test]
fn timestamp_free_completions_cannot_reorder_same_context_source_text() {
    let mut s = seq();
    let c = s
        .open_context(true, TokenReporting::SynthesisCompletion, None)
        .unwrap();
    let out = token(&mut s, c, "One. Two. Three");
    let ids: Vec<_> = out
        .iter()
        .filter_map(|e| match e {
            TokenSequencerEvent::Promoted { segment, .. } => Some(*segment),
            _ => None,
        })
        .collect();
    let bytes = s.pending_text_bytes();
    assert_eq!(
        s.complete_synthesis_segment(c, ids[1], 0),
        Err(AlignmentError::UnsupportedOperation(
            "synthesis completion out of order"
        ))
    );
    assert_eq!(s.pending_text_bytes(), bytes);
    assert_eq!(s.pending_slots(), 2);
    assert_eq!(
        words(&s.complete_synthesis_segment(c, ids[0], 0).unwrap())[0]
            .source_text
            .as_deref(),
        Some("One.")
    );
    assert_eq!(
        words(&s.complete_synthesis_segment(c, ids[1], 0).unwrap())[0]
            .source_text
            .as_deref(),
        Some(" Two.")
    );
}
#[test]
fn pending_token_admission_reserves_display_anchor_for_later_promotion() {
    let limits = TokenSequencerLimits {
        sequencer: SequencerLimits {
            max_pending_text_bytes: 15,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut s = TokenFrameSequencer::new(0, limits).unwrap();
    let c = open(&mut s);
    // Pending storage alone is twelve bytes; promoted storage needs sixteen.
    assert_eq!(
        s.register_token(c, "x", "hello", "hello"),
        Err(AlignmentError::LimitExceeded("pending text bytes"))
    );
    assert_eq!(s.pending_text_bytes(), 0);
    let limits = TokenSequencerLimits {
        sequencer: SequencerLimits {
            max_pending_text_bytes: 16,
            ..Default::default()
        },
        ..Default::default()
    };
    let mut s = TokenFrameSequencer::new(0, limits).unwrap();
    let c = open(&mut s);
    s.register_token(c, "x", "hello", "hello").unwrap();
    assert_eq!(s.pending_text_bytes(), 16);
    let out = s.finalize(c).unwrap();
    assert_eq!(anchors(&out)[0].user_facing_text, "hello");
    assert_eq!(s.pending_text_bytes(), 16);
}

#[test]
fn locale_metadata_is_bounded_before_normalization_and_replacement_is_atomic() {
    let large = "x".repeat(65);
    assert!(matches!(
        ParallelSentenceAggregator::new(Some(&large), AlignmentLimits::default()),
        Err(AlignmentError::LimitExceeded("language bytes"))
    ));
    let mut a = ParallelSentenceAggregator::new(Some("de"), AlignmentLimits::default()).unwrap();
    assert_eq!(
        a.set_language(Some(&large)),
        Err(AlignmentError::LimitExceeded("language bytes"))
    );
    assert_eq!(a.language(), "de");
    let mut s = seq();
    assert_eq!(
        s.open_context(true, TokenReporting::WordTimestamps, Some(&large)),
        Err(AlignmentError::LimitExceeded("language bytes"))
    );
    assert_eq!(s.live_contexts(), 0);
    let c = open(&mut s);
    assert_eq!(
        s.set_language(c, Some(&large)),
        Err(AlignmentError::LimitExceeded("language bytes"))
    );
    assert_eq!(c.serial(), 0);
}
#[test]
fn full_slot_queue_can_close_with_unreported_pending_tail() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            sequencer: SequencerLimits {
                max_slots: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let c = open(&mut s);
    token(&mut s, c, "One. Two");
    assert_eq!(s.pending_slots(), 1);
    let out = s.close_context(c, 0).unwrap();
    assert_eq!(
        words(&out)
            .iter()
            .map(|w| w.source_text.as_deref().unwrap())
            .collect::<Vec<_>>(),
        ["One.", " Two"]
    );
    assert_eq!(
        (s.pending_slots(), s.pending_text_bytes(), s.live_contexts()),
        (0, 0, 0)
    );
}
#[test]
fn full_foreign_context_cannot_deadlock_close_or_be_completed_by_it() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            sequencer: SequencerLimits {
                max_slots: 1,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let a = open(&mut s);
    let b = open(&mut s);
    token(&mut s, a, "earlier");
    s.finalize(a).unwrap();
    token(&mut s, b, "later");
    let out = s.close_context(b, 0).unwrap();
    assert_eq!(words(&out).len(), 1);
    assert_eq!(words(&out)[0].source_text.as_deref(), Some("later"));
    assert_eq!(s.pending_slots(), 1);
    assert_eq!(s.live_contexts(), 1);
    assert_eq!(
        words(&s.process_word(a, "earlier", 1, false).unwrap())[0]
            .source_text
            .as_deref(),
        Some("earlier")
    );
    assert_eq!(s.pending_slots(), 0);
}

#[test]
fn closing_behind_foreign_context_keeps_its_skipped_barrier_in_place() {
    let mut s = TokenFrameSequencer::new(
        0,
        TokenSequencerLimits {
            sequencer: SequencerLimits {
                max_slots: 2,
                ..Default::default()
            },
            ..Default::default()
        },
    )
    .unwrap();
    let a = open(&mut s);
    let b = open(&mut s);
    token(&mut s, a, "earlier");
    s.register_skipped(a, "code", "code", None).unwrap();
    token(&mut s, b, "later");
    let out = s.close_context(b, 0).unwrap();
    assert!(!out.iter().any(|e| matches!(
        e,
        TokenSequencerEvent::Alignment(SequencerEvent::Skipped { .. })
    )));
    assert_eq!(s.pending_slots(), 2);
    let out = s.process_word(a, "earlier", 1, false).unwrap();
    assert!(
        matches!(out.last(),Some(TokenSequencerEvent::Alignment(SequencerEvent::Skipped{text,..})) if text=="code")
    );
    assert_eq!(s.pending_slots(), 0);
}

#[test]
fn invalid_slot_limits_cannot_bypass_constructor_checks_via_working_reservation() {
    for max_slots in [0, usize::MAX] {
        assert!(matches!(
            TokenFrameSequencer::new(
                0,
                TokenSequencerLimits {
                    sequencer: SequencerLimits {
                        max_slots,
                        ..Default::default()
                    },
                    ..Default::default()
                }
            ),
            Err(AlignmentError::InvalidLimits)
        ));
    }
}
