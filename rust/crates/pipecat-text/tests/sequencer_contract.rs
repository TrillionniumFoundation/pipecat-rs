//! Completed-aggregate cases mapped to test_aggregated_frame_sequencer.py.
//! TOKEN promotion has its own contract and source-trace replay suites.
use pipecat_text::*;
fn seq() -> AggregatedFrameSequencer {
    AggregatedFrameSequencer::new(50, SequencerLimits::default()).unwrap()
}
fn completion(e: &SequencerEvent) -> &WordCompletion {
    match e {
        SequencerEvent::Word { completion, .. } | SequencerEvent::Unreported { completion, .. } => {
            completion
        }
        _ => panic!("expected word: {e:?}"),
    }
}
#[test]
fn skipped_slot_waits_behind_words_and_inherits_even_zero_pts() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    let a = s
        .register_spoken(c, "Hello world", "Hello world", "Hello world", false)
        .unwrap();
    let (skipped, initial) = s
        .register_skipped(c, "code", "<code>code</code>", Some("speaker"))
        .unwrap();
    assert!(initial.is_empty());
    let first = s.process_word(c, "Hello", 0, false).unwrap();
    assert_eq!(first.len(), 1);
    assert!(!completion(&first[0]).complete);
    let last = s.process_word(c, "world", 0, false).unwrap();
    assert_eq!(last.len(), 2);
    assert!(matches!(&last[0],SequencerEvent::Word {segment,..} if *segment==a));
    assert!(
        matches!(&last[1],SequencerEvent::Skipped {segment,pts_ns:Some(0),transport_destination:Some(d),..} if *segment==skipped && d=="speaker")
    );
    assert_eq!(s.pending_slots(), 0);
    assert_eq!(s.pending_text_bytes(), 0);
}
#[test]
fn skipped_only_text_is_never_a_provider_report() {
    let mut s = seq();
    let c = s.open_context(false).unwrap();
    let (_, out) = s.register_skipped(c, "code", "code", None).unwrap();
    assert!(matches!(
        &out[..],
        [SequencerEvent::Skipped { pts_ns: None, .. }]
    ));
    assert!(s.close_context(c, 123).unwrap().is_empty());
}
#[test]
fn merged_overflow_crosses_slots_without_recursion() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    for t in ["11", "22", "33", "44"] {
        s.register_spoken(c, t, t, t, false).unwrap();
    }
    let out = s.process_word(c, "11223344", 10, false).unwrap();
    assert_eq!(out.len(), 4);
    assert_eq!(
        out.iter()
            .map(|e| completion(e).word.as_deref().unwrap())
            .collect::<Vec<_>>(),
        vec!["11", "22", "33", "44"]
    );
    assert!(out
        .iter()
        .all(|e| completion(e).evidence == AlignmentEvidence::ProviderReport));
    assert_eq!(s.pending_slots(), 0);
}
#[test]
fn missing_event_completes_only_when_the_next_slot_matches() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    s.register_spoken(c, "number is", "number is", "number is", false)
        .unwrap();
    s.register_spoken(c, "4111", "4111", "<card>4111</card>", false)
        .unwrap();
    s.process_word(c, "number", 0, false).unwrap();
    let out = s.process_word(c, "4111", 5, false).unwrap();
    assert_eq!(out.len(), 2);
    assert_eq!(completion(&out[0]).word.as_deref(), Some("is"));
    assert_eq!(
        completion(&out[0]).evidence,
        AlignmentEvidence::UnreportedRemainder
    );
    assert_eq!(
        completion(&out[1]).evidence,
        AlignmentEvidence::ProviderReport
    );
    assert_eq!(
        completion(&out[1]).source_text.as_deref(),
        Some("<card>4111</card>")
    );
}
#[test]
fn arbitrary_words_do_not_force_complete_or_leak_into_context() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    s.register_spoken(c, "hello world", "hello world", "hello world", false)
        .unwrap();
    let before = s.pending_text_bytes();
    let out = s.process_word(c, "secret", 0, false).unwrap();
    assert!(matches!(&out[..], [SequencerEvent::UnmatchedWord { .. }]));
    assert_eq!(s.pending_text_bytes(), before);
    assert_eq!(s.pending_slots(), 1);
    let out = s.process_word(c, "hello", 1, false).unwrap();
    assert_eq!(completion(&out[0]).source_text.as_deref(), Some("hello"));
}
#[test]
fn context_close_exposes_unreported_suffix_and_revokes_late_events() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    s.register_spoken(c, "hello world", "hello world", "hello world", false)
        .unwrap();
    s.process_word(c, "hello", 100, false).unwrap();
    let out = s.close_context(c, 200).unwrap();
    assert_eq!(
        completion(&out[0]).evidence,
        AlignmentEvidence::UnreportedRemainder
    );
    assert_eq!(completion(&out[0]).word.as_deref(), Some("world"));
    assert_eq!(completion(&out[0]).raw_pos, 5);
    assert_eq!(
        s.process_word(c, "world", 201, false),
        Err(AlignmentError::ContextClosed)
    );
    assert_eq!(s.live_contexts(), 0);
}
#[test]
fn closing_multiple_slots_releases_skipped_in_registration_order() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    s.register_spoken(c, "one", "one", "one", false).unwrap();
    s.register_skipped(c, "code", "code", None).unwrap();
    s.register_spoken(c, "two", "two", "two", false).unwrap();
    let out = s.close_context(c, 0).unwrap();
    assert_eq!(out.len(), 3);
    assert_eq!(completion(&out[0]).word.as_deref(), Some("one"));
    assert!(matches!(out[1], SequencerEvent::Skipped { .. }));
    assert_eq!(completion(&out[2]).word.as_deref(), Some("two"));
}
#[test]
fn simultaneous_contexts_never_consume_each_others_words() {
    let mut s = seq();
    let a = s.open_context(true).unwrap();
    let b = s.open_context(false).unwrap();
    let sa = s.register_spoken(a, "one", "one", "one", false).unwrap();
    let (_, waiting) = s.register_skipped(a, "code", "code", None).unwrap();
    assert!(waiting.is_empty());
    let sb = s.register_spoken(b, "two", "two", "two", false).unwrap();
    assert!(matches!(
        &s.process_word(a, "two", 0, false).unwrap()[..],
        [SequencerEvent::UnmatchedWord { .. }]
    ));
    let out = s.process_word(b, "two", 1, false).unwrap();
    assert!(
        matches!(&out[..],[SequencerEvent::Word {context,segment,append_to_context:false,..}] if *context==b&&*segment==sb)
    );
    assert_eq!(s.pending_slots(), 3);
    let out = s.process_word(a, "one", 2, false).unwrap();
    assert!(matches!(&out[0],SequencerEvent::Word {segment,..} if *segment==sa));
    assert!(matches!(out[1], SequencerEvent::Skipped { .. }));
    assert_eq!(s.pending_slots(), 0);
    s.close_context(a, 3).unwrap();
    assert_eq!(s.live_contexts(), 1);
    s.close_context(b, 3).unwrap();
}
#[test]
fn context_close_does_not_finish_other_context() {
    let mut s = seq();
    let a = s.open_context(true).unwrap();
    let b = s.open_context(true).unwrap();
    s.register_spoken(a, "one", "one", "one", false).unwrap();
    s.register_spoken(b, "two", "two", "two", false).unwrap();
    s.close_context(b, 0).unwrap();
    assert_eq!(s.pending_slots(), 2);
    assert_eq!(s.live_contexts(), 1);
    let out = s.process_word(a, "one", 1, false).unwrap();
    assert_eq!(out.len(), 1);
    assert_eq!(s.pending_slots(), 0);
}
#[test]
fn spacing_and_append_intent_are_metadata_not_spoken_history() {
    let mut s = seq();
    let c = s.open_context(false).unwrap();
    s.register_spoken(c, "你好世界", "你好世界", "你好世界", false)
        .unwrap();
    let out = s.process_word(c, "你好", 0, true).unwrap();
    assert!(matches!(
        &out[..],
        [SequencerEvent::Word {
            includes_inter_frame_spaces: true,
            append_to_context: false,
            ..
        }]
    ));
    let out = s.close_context(c, 0).unwrap();
    assert!(matches!(
        &out[..],
        [SequencerEvent::Unreported {
            includes_inter_frame_spaces: true,
            append_to_context: false,
            ..
        }]
    ));
    assert_eq!(
        completion(&out[0]).evidence,
        AlignmentEvidence::UnreportedRemainder
    );
}
#[test]
fn interruption_drops_every_slot_and_old_generation_ids() {
    let mut s = seq();
    let old = s.open_context(true).unwrap();
    let old_seg = s
        .register_spoken(old, "hello", "hello", "hello", false)
        .unwrap();
    s.register_skipped(old, "code", "code", None).unwrap();
    assert_eq!(s.interrupt(), Ok(51));
    assert_eq!(
        (s.pending_slots(), s.pending_text_bytes(), s.live_contexts()),
        (0, 0, 0)
    );
    assert_eq!(
        s.process_word(old, "hello", 0, false),
        Err(AlignmentError::StaleGeneration)
    );
    assert_eq!(
        s.register_spoken(old, "hello", "hello", "hello", false),
        Err(AlignmentError::StaleGeneration)
    );
    let new = s.open_context(true).unwrap();
    let new_seg = s
        .register_spoken(new, "hello", "hello", "hello", false)
        .unwrap();
    assert_ne!(old, new);
    assert_ne!(old_seg, new_seg);
    assert_eq!(s.process_word(new, "hello", 1, false).unwrap().len(), 1);
}
#[test]
fn word_and_registration_limits_are_transactional() {
    let limits = SequencerLimits {
        max_contexts: 1,
        max_slots: 1,
        max_pending_text_bytes: 15,
        alignment: AlignmentLimits {
            max_word_bytes: 5,
            ..AlignmentLimits::default()
        },
    };
    let mut s = AggregatedFrameSequencer::new(0, limits).unwrap();
    let c = s.open_context(true).unwrap();
    assert_eq!(
        s.open_context(true),
        Err(AlignmentError::LimitExceeded("contexts"))
    );
    assert!(matches!(
        s.register_spoken(c, "hello!", "hello!", "hello!", false),
        Err(AlignmentError::LimitExceeded("pending text bytes"))
    ));
    assert_eq!(s.pending_slots(), 0);
    s.register_spoken(c, "hello", "hello", "hello", false)
        .unwrap();
    assert_eq!(s.pending_text_bytes(), 15);
    assert!(matches!(
        s.register_skipped(c, "x", "x", None),
        Err(AlignmentError::LimitExceeded("slots"))
    ));
    assert_eq!(
        s.process_word(c, "hello!", 0, false),
        Err(AlignmentError::LimitExceeded("word bytes"))
    );
    assert_eq!(s.pending_slots(), 1);
    assert_eq!(s.process_word(c, "hello", 0, false).unwrap().len(), 1);
    assert_eq!(s.pending_text_bytes(), 0);
}
#[test]
fn invalid_projection_does_not_allocate_slot_or_consume_segment_identity() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    assert_eq!(
        s.register_spoken(c, "hello", "hello", "wrong", false),
        Err(AlignmentError::UnalignedLlmText)
    );
    assert_eq!((s.pending_slots(), s.pending_text_bytes()), (0, 0));
    let id = s
        .register_spoken(c, "hello", "hello", "hello", false)
        .unwrap();
    assert_eq!(id.serial(), 0);
}
#[test]
fn completed_aggregate_api_has_no_speculative_streaming_passthrough() {
    let mut s = seq();
    let c = s.open_context(true).unwrap();
    let out = s.process_word(c, "hello", 0, false).unwrap();
    assert!(matches!(&out[..], [SequencerEvent::UnmatchedWord { .. }]));
    s.register_spoken(c, "hello", "hello", "hello", false)
        .unwrap();
    assert_eq!(s.pending_slots(), 1);
    assert_eq!(
        completion(&s.close_context(c, 0).unwrap()[0]).evidence,
        AlignmentEvidence::UnreportedRemainder
    );
}
#[test]
fn generation_exhaustion_preserves_pending_state() {
    let mut s = AggregatedFrameSequencer::new(u64::MAX, SequencerLimits::default()).unwrap();
    let c = s.open_context(true).unwrap();
    s.register_spoken(c, "one", "one", "one", false).unwrap();
    assert_eq!(s.interrupt(), Err(AlignmentError::IdentityExhausted));
    assert_eq!(s.pending_slots(), 1);
    assert_eq!(s.process_word(c, "one", 0, false).unwrap().len(), 1);
}

#[test]
fn independent_sequencers_cannot_consume_each_others_context_ids() {
    let mut a = seq();
    let mut b = seq();
    let ca = a.open_context(true).unwrap();
    let cb = b.open_context(true).unwrap();
    assert_eq!(
        (ca.generation(), ca.serial()),
        (cb.generation(), cb.serial())
    );
    assert_ne!(ca, cb);
    a.register_spoken(ca, "one", "one", "one", false).unwrap();
    b.register_spoken(cb, "one", "one", "one", false).unwrap();
    assert_eq!(
        b.process_word(ca, "one", 0, false),
        Err(AlignmentError::UnknownContext)
    );
    assert_eq!(b.pending_slots(), 1);
}
