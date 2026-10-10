//! Native cases mapped to test_text_segment_map.py and
//! test_word_completion_tracker.py at af9f9da9; see ALIGNMENT.md.
use pipecat_text::*;

fn map(tts: &str, original: &str, llm: Option<&str>) -> TextSegmentMap {
    TextSegmentMap::new(tts, original, llm, AlignmentLimits::default()).unwrap()
}
fn tracker(tts: &str, llm: Option<&str>, user: Option<&str>) -> WordCompletionTracker {
    WordCompletionTracker::new(7, tts, llm, user, AlignmentLimits::default()).unwrap()
}
fn add(t: &mut WordCompletionTracker, word: &str) -> WordCompletion {
    t.add_word(7, word).unwrap()
}

#[test]
fn source_map_builds_currency_span_and_counts() {
    let m = map(
        "Your balance is forty two dollars",
        "Your balance is $42",
        None,
    );
    assert_eq!(m.segments()[0].original, "Your balance is ");
    assert!(!m.segments()[0].is_transformed());
    let s = m.segments().last().unwrap();
    assert_eq!(
        (s.original.as_str(), s.original_start, s.original_end),
        ("$42", 16, 19)
    );
    assert!(s.is_transformed());
    assert_eq!(s.tts_alnum_count(), 15);
    assert_eq!(s.original_alnum_count(), 2);
}
#[test]
fn source_transform_holds_and_commits_original_atomically() {
    let mut t = tracker(
        "Your balance is forty two dollars and fifty cents",
        Some("Your balance is <b>$42.50</b>"),
        Some("Your balance is $42.50"),
    );
    for w in ["Your", "balance", "is"] {
        add(&mut t, w);
    }
    let prefix = t.accumulated_user_facing_text().to_owned();
    for w in ["forty", "two", "dollars", "and", "fifty"] {
        let r = add(&mut t, w);
        assert!(!r.complete);
        assert!(r.suppress_in_context);
        assert_eq!(t.accumulated_user_facing_text(), prefix);
        assert_eq!(r.source_text, None);
    }
    let r = add(&mut t, "cents");
    assert!(r.complete);
    assert!(!r.suppress_in_context);
    assert_eq!(r.source_text.as_deref(), Some(" <b>$42.50</b>"));
    assert_eq!(t.remaining_user_facing_text(), "");
}
#[test]
fn source_map_llm_cursor_stops_before_closing_tag() {
    let mut m = map("forty two dollars", "$42", Some("<card>$42</card>"));
    for w in ["forty", "two", "dollars"] {
        m.advance_word(w).unwrap();
    }
    assert_eq!(m.llm_pos(), 9);
    assert_eq!(m.llm_reported_pos(), 9);
    assert_eq!(m.last_completed_segment().unwrap().original, "$42");
}
#[test]
fn source_tokenization_changes_are_atomic_case_and_connector_are_not() {
    for (tts, original, expected) in [
        ("body pump", "BODYPUMP", true),
        ("A P I", "API", true),
        ("sql", "SQL", false),
        ("body-pump", "BODYPUMP", false),
        ("hit", "HIIT", true),
    ] {
        assert_eq!(
            map(tts, original, None).segments()[0].is_transformed(),
            expected,
            "{tts}"
        );
    }
}
#[test]
fn source_ssml_fragments_hold_original_and_do_not_match_tag_names() {
    let tts = "My name is <phoneme alphabet=\"ipa\" ph=\"ʃəˈvɔːn\">Siobhan</phoneme>.";
    let mut m = map(tts, "My name is Siobhan.", None);
    for w in ["My", "name", "is", "<phoneme"] {
        m.advance_word(w).unwrap();
    }
    assert!(m.in_transformed_segment());
    assert!(!map("<break/>hello", "hello", None)
        .word_belongs_current_segment("break")
        .unwrap());
    let mut m = map("<spell>4111</spell>", "4111", None);
    m.advance_word("4111").unwrap();
    assert!(m.is_complete());
    assert_eq!(m.user_facing_pos(), 4);
}
#[test]
fn source_markup_stripping_keeps_plain_prefix_progressive() {
    let text = "I love to count <spell>1234</spell>\n\nHow can I help you today?";
    let mut t = tracker(text, Some(text), None);
    for w in [
        "I", "love", "to", "count", "1234.", "How", "can", "I", "help", "you", "today?",
    ] {
        assert!(t.word_belongs_here(w).unwrap(), "{w}");
        let r = add(&mut t, w);
        assert_eq!(r.overflow, None);
        assert_ne!(r.evidence, AlignmentEvidence::UnreportedRemainder);
    }
    assert!(t.is_complete());
    assert_eq!(t.accumulated_llm_text(), Some(text));
}
#[test]
fn source_stray_angle_bracket_is_literal() {
    let text = "I love you <3 always";
    let mut m = map(text, text, None);
    assert!(!m.segments()[0].is_transformed());
    for w in ["I", "love", "you", "<3", "always"] {
        assert!(m.word_belongs_current_segment(w).unwrap());
        m.advance_word(w).unwrap();
    }
    assert!(m.is_complete());
}
#[test]
fn source_typography_case_and_accent_fold_in_both_directions() {
    for (text, words) in [
        ("don’t worry", vec!["don't", "worry"]),
        ("don't worry", vec!["don’t", "worry"]),
        ("2020–2021 report", vec!["2020-2021", "report"]),
        ("“hello” there", vec!["\"hello\"", "there"]),
        (
            "Visit the café today",
            vec!["VISIT", "the", "cafe", "today"],
        ),
        (
            "Visit the cafe today",
            vec!["Visit", "the", "café", "today"],
        ),
        ("İstanbul today", vec!["istanbul", "today"]),
    ] {
        let mut t = tracker(text, Some(text), None);
        for w in words {
            assert!(t.word_belongs_here(w).unwrap(), "{text}: {w}");
            assert_eq!(add(&mut t, w).evidence, AlignmentEvidence::ProviderReport);
        }
        assert!(t.is_complete());
        assert_eq!(t.accumulated_llm_text(), Some(text));
    }
}
#[test]
fn source_folded_prefix_requires_whole_word_but_literal_cjk_fragments_do_not() {
    let t = tracker("Please talk to the Accountant", None, None);
    let mut t = t;
    for w in ["Please", "talk", "to", "the"] {
        add(&mut t, w);
    }
    assert!(!t.word_belongs_here("account").unwrap());
    let mut t = tracker("こんにちは、私はあなたの", None, None);
    assert!(!add(&mut t, "こんにちは、私").complete);
    assert_eq!(t.remaining_tts_text(), "はあなたの");
    assert!(add(&mut t, "はあなたの").complete);
}
#[test]
fn source_repeated_punctuation_has_distinct_attributed_and_reported_cursors() {
    let text = "Yeah, I can do that.";
    let mut t = tracker(text, Some(text), None);
    let r = add(&mut t, "Yeah");
    assert_eq!(r.source_text.as_deref(), Some("Yeah,"));
    assert_eq!(t.segment_map().raw_pos(), 4);
    assert_eq!(t.segment_map().llm_pos(), 5);
    assert_eq!(t.segment_map().llm_reported_pos(), 4);
    let r = add(&mut t, ", I");
    assert_eq!(r.word.as_deref(), Some("I"));
    assert_eq!(r.source_text.as_deref(), Some(" I"));
    assert_eq!(t.segment_map().last_leading_duplicate(), 2);
    let mut t = tracker(text, None, None);
    add(&mut t, "Yeah");
    assert_eq!(add(&mut t, ", I").word.as_deref(), Some(", I"));
}
#[test]
fn source_punctuation_only_duplicate_suppresses_context() {
    let mut t = tracker("Yeah, I can", Some("Yeah, I can"), None);
    add(&mut t, "Yeah");
    let r = add(&mut t, ",");
    assert!(r.suppress_in_context);
    assert_eq!(r.source_text.as_deref(), Some(""));
    assert_eq!(t.segment_map().last_leading_duplicate(), 0);
}
#[test]
fn source_separated_french_punctuation_remains_pending() {
    for (text, words) in [
        ("Comment ça va ?", vec!["Comment", "ça", "va"]),
        ("Bonjour !", vec!["Bonjour"]),
        ("Ça va\u{a0}?", vec!["Ça", "va"]),
    ] {
        let mut t = tracker(text, Some(text), None);
        for w in words {
            assert!(!add(&mut t, w).complete);
        }
        let mark = text.chars().last().unwrap().to_string();
        assert!(add(&mut t, &mark).complete);
        assert_eq!(t.accumulated_user_facing_text(), text);
    }
    let mut t = tracker("How are you?", None, None);
    for w in ["How", "are"] {
        add(&mut t, w);
    }
    assert!(add(&mut t, "you").complete);
}
#[test]
fn source_multilingual_character_offsets_are_not_utf8_bytes() {
    for (text, words) in [
        (
            "저는 여러분의 AI 어시스턴트입니다.",
            vec!["저는", "여러분의", "AI", "어시스턴트입니다."],
        ),
        ("你好，我是你的智能", vec!["你好，我是", "你的智能"]),
    ] {
        let mut t = tracker(text, Some(text), None);
        for w in words {
            add(&mut t, w);
            assert_eq!(
                format!("{}{}", t.accumulated_tts_text(), t.remaining_tts_text()),
                text
            );
        }
        assert_eq!(t.segment_map().raw_pos(), text.chars().count());
        assert!(t.is_complete());
    }
}
#[test]
fn source_transformed_expansion_at_end_carries_original_only_once() {
    let mut t = tracker("A P I", Some("<b>API</b>"), Some("API"));
    for w in ["A", "P"] {
        let r = add(&mut t, w);
        assert!(r.suppress_in_context);
        assert_eq!(r.source_text, None);
    }
    let r = add(&mut t, "I");
    assert_eq!(r.source_text.as_deref(), Some("<b>API</b>"));
    assert!(r.complete);
}
#[test]
fn source_overflow_retains_provider_casing_and_punctuation() {
    let mut a = tracker("1111", Some("<card>1111</card>"), Some("1111"));
    let r = add(&mut a, "1111And");
    assert!(r.complete);
    assert_eq!(r.word.as_deref(), Some("1111"));
    assert_eq!(r.overflow.as_deref(), Some("And"));
    assert_eq!(r.source_text.as_deref(), Some("<card>1111</card>"));
    let mut b = tracker("And your", None, None);
    assert!(!add(&mut b, r.overflow.as_deref().unwrap()).complete);
}
#[test]
fn source_no_match_returns_unreported_suffix_and_leaves_raw_cursor_unchanged() {
    let mut t = tracker("number is", Some("number is"), None);
    add(&mut t, "number");
    let before = t.segment_map().raw_pos();
    let r = add(&mut t, "4111");
    assert_eq!(r.evidence, AlignmentEvidence::UnreportedRemainder);
    assert_eq!(r.word.as_deref(), Some("is"));
    assert_eq!(r.overflow.as_deref(), Some("4111"));
    assert_eq!(t.segment_map().raw_pos(), before);
    assert!(t.has_unreported_text());
    assert!(r.complete);
}
#[test]
fn source_missing_word_resync_is_bounded_and_explicit() {
    let mut t = tracker(
        "one two three four five six",
        Some("one two three four five six"),
        None,
    );
    let r = add(&mut t, "three");
    assert_eq!(r.evidence, AlignmentEvidence::Resynchronized);
    assert_eq!(r.word.as_deref(), Some("one two three"));
    assert!(t.has_unreported_text());
    assert!(!tracker("one two three four five six", None, None)
        .word_belongs_here("five")
        .unwrap());
    let mut ko = tracker("저는 여러분의", None, None);
    assert_eq!(
        add(&mut ko, "여러분의").evidence,
        AlignmentEvidence::Resynchronized
    );
}
#[test]
fn source_symbol_substitution_does_not_swallow_next_markdown_token() {
    let text = "step → ### **Title** done";
    let mut t = tracker(text, Some(text), Some(text));
    add(&mut t, "step");
    let r = add(&mut t, "-");
    assert_eq!(r.word.as_deref(), Some("-"));
    assert_eq!(t.remaining_tts_text(), " ### **Title** done");
    assert!(t.word_belongs_here("###").unwrap());
    add(&mut t, "###");
    assert!(t.word_belongs_here("**Title**").unwrap());
    add(&mut t, "**Title**");
    assert!(add(&mut t, "done").complete);
}
#[test]
fn source_trailing_emoji_is_not_an_alphanumeric_completion_barrier() {
    let mut t = tracker("Hello 😊", Some("Hello"), Some("Hello 😊"));
    assert!(add(&mut t, "Hello").complete);
    assert_eq!(t.remaining_tts_text(), " 😊");
    let r = add(&mut t, "😊");
    assert!(r.complete);
    assert!(r.suppress_in_context);
    assert_eq!(r.word.as_deref(), Some("😊"));
}
#[test]
fn source_late_words_are_ignored_not_overflowed() {
    let mut t = tracker("Hi", None, None);
    add(&mut t, "Hi");
    let r = add(&mut t, "extra");
    assert_eq!(r.evidence, AlignmentEvidence::AlreadyComplete);
    assert_eq!(r.word, None);
    assert_eq!(r.overflow, None);
}
#[test]
fn exact_whitespace_partition_and_source_ranges_survive_every_word() {
    let raw = "\n<card>4111\t1111</card>  ";
    let user = "\n4111\t1111  ";
    let mut t = tracker(user, Some(raw), Some(user));
    let mut reconstructed = String::new();
    for w in ["4111", "1111"] {
        let r = add(&mut t, w);
        reconstructed.push_str(r.source_text.as_deref().unwrap_or(""));
        assert_eq!(
            format!(
                "{}{}",
                t.accumulated_user_facing_text(),
                t.remaining_user_facing_text()
            ),
            user
        );
    }
    assert_eq!(reconstructed, raw);
}
#[test]
fn generation_reset_rejects_old_event_without_mutating() {
    let mut t = tracker("hello world", None, None);
    add(&mut t, "hello");
    t.reset(8).unwrap();
    assert_eq!(t.add_word(7, "world"), Err(AlignmentError::StaleGeneration));
    assert_eq!(t.accumulated_tts_text(), "");
    assert_eq!(t.reset(8), Err(AlignmentError::StaleGeneration));
    assert!(!t.add_word(8, "hello").unwrap().complete);
}
#[test]
fn invalid_llm_projection_fails_closed() {
    assert!(matches!(
        TextSegmentMap::new("hello", "hello", Some("secret"), AlignmentLimits::default()),
        Err(AlignmentError::UnalignedLlmText)
    ));
}
#[test]
fn explicit_segment_provenance_is_contiguous_and_validated() {
    let segs = vec![TextSegment {
        original: "$42".into(),
        tts: "forty two".into(),
        original_start: 0,
        original_end: 3,
    }];
    let mut m = TextSegmentMap::from_segments(
        "forty two",
        "$42",
        None,
        segs.clone(),
        AlignmentLimits::default(),
    )
    .unwrap();
    m.advance_word("forty").unwrap();
    assert_eq!(m.user_facing_pos(), 0);
    m.advance_word("two").unwrap();
    assert_eq!(m.user_facing_pos(), 3);
    let mut bad = segs;
    bad[0].original_end = 4;
    assert!(matches!(
        TextSegmentMap::from_segments("forty two", "$42", None, bad, AlignmentLimits::default()),
        Err(AlignmentError::InvalidSegments)
    ));
}
#[test]
fn all_alignment_budgets_fail_without_state_mutation() {
    let default = AlignmentLimits::default();
    for limits in [
        AlignmentLimits {
            max_text_bytes: 2,
            ..default
        },
        AlignmentLimits {
            max_tokens: 1,
            ..default
        },
        AlignmentLimits {
            max_diff_work: 1,
            ..default
        },
    ] {
        assert!(matches!(
            TextSegmentMap::new("a b a b", "a b a b", None, limits),
            Err(AlignmentError::LimitExceeded(_))
        ));
    }
    assert!(matches!(
        TextSegmentMap::new(
            "one two",
            "1 two",
            None,
            AlignmentLimits {
                max_segments: 1,
                ..default
            }
        ),
        Err(AlignmentError::LimitExceeded("segments"))
    ));
    let mut t = WordCompletionTracker::new(
        7,
        "hello world",
        None,
        None,
        AlignmentLimits {
            max_word_bytes: 5,
            ..default
        },
    )
    .unwrap();
    add(&mut t, "hello");
    let pos = t.segment_map().raw_pos();
    assert_eq!(
        t.add_word(7, "world!"),
        Err(AlignmentError::LimitExceeded("word bytes"))
    );
    assert_eq!(t.segment_map().raw_pos(), pos);
}

#[test]
fn python_control_separator_whitespace_is_not_word_punctuation() {
    for sep in [
        "\u{1c}", "\u{1d}", "\u{1e}", "\u{1f}", "\u{a0}", "\u{2003}", "\t\n",
    ] {
        let text = format!("hello{sep}world");
        let mut t = tracker(&text, Some(&text), Some(&text));
        let a = add(&mut t, "hello");
        assert_eq!(a.source_text.as_deref(), Some("hello"));
        assert_eq!(t.accumulated_user_facing_text(), "hello");
        let b = add(&mut t, "world");
        assert!(b.complete);
        assert_eq!(b.source_text, Some(format!("{sep}world")));
        assert_eq!(
            format!("{}{}", a.source_text.unwrap(), b.source_text.unwrap()),
            text
        );
    }
}

#[test]
fn incremental_synthesis_only_punctuation_cannot_spend_source_letters() {
    let mut t = tracker("a------ b", Some("a b"), Some("a b"));
    let mut source = String::new();
    for word in ["a", "-", "-", "-", "-", "-", "-"] {
        let r = add(&mut t, word);
        source.push_str(r.source_text.as_deref().unwrap_or(""));
        assert!(!source.contains('b'), "word={word:?}: {r:?}");
    }
    let r = add(&mut t, "b");
    source.push_str(r.source_text.as_deref().unwrap_or(""));
    assert_eq!(source, "a b");
}
