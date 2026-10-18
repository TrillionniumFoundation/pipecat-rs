//! Source fixtures for Gemini's input handler and timeout handler at pinned Python
//! af9f9da9d7afddaa8811c0e0251b32566c980bdf. Sentence fixtures use the existing
//! pipecat-text source contract, not a separate approximate punctuation splitter.
//! These are deterministic clock observations; no test creates or sleeps a timer.

use super::*;

fn source(text: &str) -> Transcript {
    Transcript {
        text: text.to_owned(),
        language_code: Some("en-US".to_owned()),
    }
}

fn finalizer() -> SourceFinalizer {
    SourceFinalizer::new(64 * 1024, 64 * 1024).unwrap()
}

#[test]
fn split_words_keep_exact_concatenation_and_fragment_coverage() {
    let now = Instant::now();
    let mut state = finalizer();
    assert!(state.ingest(1, &source("hel"), now).unwrap().is_empty());
    let trigger = source("lo. world");
    let finals = state.ingest(2, &trigger, now).unwrap();
    assert_eq!(finals.len(), 1);
    assert_eq!(finals[0].text, "hello.");
    assert_eq!(finals[0].source_through, 1);
    assert_eq!(finals[0].result.as_deref(), Some(&trigger));
    assert_eq!(state.covered_watermark(), 1);
    assert_eq!(state.buffer, " world");
    assert_eq!(state.fragments.len(), 1);
    assert_eq!(state.fragments[0].watermark, 2);
    assert_eq!(state.fragments[0].end, " world".len());

    let finals = state.expire(now + INACTIVITY).unwrap();
    assert_eq!(finals.len(), 1);
    assert_eq!(finals[0].text, " world");
    assert_eq!(finals[0].source_through, 2);
    assert!(finals[0].result.is_none());
    assert_eq!(state.covered_watermark(), 2);
    assert!(state.deadline().is_none());
    assert!(state.expire(now + INACTIVITY).unwrap().is_empty());
}

#[test]
fn multiple_finals_in_one_fragment_share_provenance_but_not_coverage() {
    let now = Instant::now();
    let mut state = finalizer();
    let trigger = source("One. Two! suffix");
    let finals = state.ingest(1, &trigger, now).unwrap();
    assert_eq!(finals.len(), 2);
    assert_eq!(finals[0].text, "One.");
    assert_eq!(finals[1].text, " Two!");
    assert_eq!(finals[0].source_through, 0);
    assert_eq!(finals[1].source_through, 0);
    assert!(Arc::ptr_eq(
        finals[0].result.as_ref().unwrap(),
        finals[1].result.as_ref().unwrap()
    ));
    assert_eq!(finals[0].result.as_deref(), Some(&trigger));
    assert_eq!(state.buffer, " suffix");
    let finals = state.expire(now + INACTIVITY).unwrap();
    assert_eq!(finals[0].text, " suffix");
    assert_eq!(finals[0].source_through, 1);
    assert!(finals[0].result.is_none());
}

#[test]
fn later_fragment_can_complete_several_earlier_watermarks() {
    let now = Instant::now();
    let mut state = finalizer();
    for (watermark, text) in [(1, "h"), (2, "el"), (3, "lo")] {
        assert!(state
            .ingest(watermark, &source(text), now)
            .unwrap()
            .is_empty());
    }
    let finals = state.ingest(4, &source(". Next"), now).unwrap();
    assert_eq!(finals[0].text, "hello.");
    assert_eq!(finals[0].source_through, 3);
    assert_eq!(state.covered_watermark(), 3);
    assert_eq!(state.fragments.len(), 1);
    assert_eq!(state.fragments[0].watermark, 4);
}

#[test]
fn complete_fragment_reaches_its_own_watermark() {
    let now = Instant::now();
    let mut state = finalizer();
    let finals = state.ingest(1, &source("One. Two!"), now).unwrap();
    assert_eq!(finals.len(), 2);
    assert_eq!(finals[0].source_through, 0);
    assert_eq!(finals[1].source_through, 1);
    assert_eq!(state.covered_watermark(), 1);
    assert!(state.buffer.is_empty());
    assert!(state.fragments.is_empty());
    assert!(state.deadline().is_none());
}

#[test]
fn source_timeout_is_500ms_reset_only_by_nonempty_actual_text() {
    let now = Instant::now();
    let mut state = finalizer();
    state.ingest(1, &source("hel"), now).unwrap();
    assert_eq!(state.deadline(), Some(now + INACTIVITY));
    let later = now + Duration::from_millis(400);
    state.ingest(2, &source("lo"), later).unwrap();
    assert_eq!(state.deadline(), Some(later + INACTIVITY));
    // An empty actual field contributes no source identity or timer reset.
    state.ingest(2, &source(""), now + INACTIVITY).unwrap();
    assert_eq!(state.deadline(), Some(later + INACTIVITY));
    assert!(state.expire(now + INACTIVITY).unwrap().is_empty());
    assert!(state
        .expire(later + INACTIVITY - Duration::from_nanos(1))
        .unwrap()
        .is_empty());
    let finals = state.expire(later + INACTIVITY).unwrap();
    assert_eq!(finals[0].text, "hello");
    assert_eq!(finals[0].source_through, 2);
    assert!(finals[0].result.is_none());
}

#[test]
fn leading_strip_is_conditional_and_does_not_rewrite_suffixes() {
    let now = Instant::now();
    for (input, expected) in [
        (" \tHello.", "Hello."),
        (" \u{2003}Hello.", "Hello."),
        (" \u{1c}\u{1d}\u{1e}\u{1f}Hello.", "Hello."),
        ("\tHello.", "\tHello."),
        ("\nHello.", "\nHello."),
        ("\u{2003}Hello.", "\u{2003}Hello."),
    ] {
        let mut state = finalizer();
        let finals = state.ingest(1, &source(input), now).unwrap();
        assert_eq!(finals.len(), 1, "{input:?}");
        assert_eq!(finals[0].text, expected, "{input:?}");
        assert_eq!(finals[0].source_through, 1);
        // The provenance remains the unstripped normalized contribution.
        assert_eq!(finals[0].result.as_deref(), Some(&source(input)));
    }

    let mut state = finalizer();
    state.ingest(1, &source("Hello"), now).unwrap();
    let finals = state.ingest(2, &source(" \tworld."), now).unwrap();
    assert_eq!(finals[0].text, "Hello \tworld.");

    let mut state = finalizer();
    let finals = state.ingest(1, &source("Hello.  Next"), now).unwrap();
    assert_eq!(finals[0].text, "Hello.");
    assert_eq!(state.buffer, "  Next");
    let finals = state.ingest(2, &source(" sentence."), now).unwrap();
    assert_eq!(finals[0].text, "  Next sentence.");
}

#[test]
fn whitespace_only_source_has_coverage_but_no_content_final() {
    let now = Instant::now();
    let mut state = finalizer();
    assert!(state.ingest(1, &source(" \t\n"), now).unwrap().is_empty());
    assert_eq!(state.covered_watermark(), 1);
    assert!(state.deadline().is_none());
    assert!(state.fragments.is_empty());

    assert!(state.ingest(2, &source("\t\n"), now).unwrap().is_empty());
    assert_eq!(state.covered_watermark(), 1);
    assert_eq!(state.deadline(), Some(now + INACTIVITY));
    assert!(state.expire(now + INACTIVITY).unwrap().is_empty());
    assert_eq!(state.covered_watermark(), 2);
    assert!(state.fragments.is_empty());

    let finals = state.ingest(3, &source("Hello.  "), now).unwrap();
    assert_eq!(finals[0].text, "Hello.");
    assert_eq!(finals[0].source_through, 2);
    assert_eq!(state.covered_watermark(), 2);
    assert!(state.expire(now + INACTIVITY).unwrap().is_empty());
    assert_eq!(state.covered_watermark(), 3);
}

#[test]
fn nonempty_whitespace_fragment_resets_a_pending_suffix_timer() {
    let now = Instant::now();
    let mut state = finalizer();
    state.ingest(1, &source("Hello"), now).unwrap();
    let later = now + Duration::from_millis(450);
    state.ingest(2, &source(" "), later).unwrap();
    assert_eq!(state.deadline(), Some(later + INACTIVITY));
    assert!(state.expire(now + INACTIVITY).unwrap().is_empty());
    let finals = state.expire(later + INACTIVITY).unwrap();
    assert_eq!(finals[0].text, "Hello ");
    assert_eq!(finals[0].source_through, 2);
}

#[test]
fn pinned_sentence_cases_preserve_offsets_abbreviations_and_quotes() {
    let now = Instant::now();
    for (input, prefix, suffix) in [
        ("Dr. Smith is here. Next", "Dr. Smith is here.", " Next"),
        ("It costs $29.95. Next", "It costs $29.95.", " Next"),
        (
            "She said, \"Dr. Smith is here.\" Next",
            "She said, \"Dr. Smith is here.\"",
            " Next",
        ),
        ("👋 Hello. 😀 Next", "👋 Hello.", " 😀 Next"),
        ("Cafe\u{301} is open. Next", "Cafe\u{301} is open.", " Next"),
        ("\n\nHello. Next", "\n\nHello.", " Next"),
        ("Hello.\n\nNext", "Hello.", "\n\nNext"),
        ("こんにちは。次", "こんにちは。", "次"),
        ("你好。下一句", "你好。", "下一句"),
    ] {
        let mut state = finalizer();
        let finals = state.ingest(1, &source(input), now).unwrap();
        assert_eq!(finals.len(), 1, "{input}");
        assert_eq!(finals[0].text, prefix, "{input}");
        assert_eq!(finals[0].source_through, 0, "{input}");
        assert_eq!(state.buffer, suffix, "{input}");
        let finals = state.expire(now + INACTIVITY).unwrap();
        assert_eq!(finals[0].text, suffix, "{input}");
        assert_eq!(finals[0].source_through, 1, "{input}");
    }
}

#[test]
fn unfinished_quoted_abbreviation_waits_for_more_source() {
    let now = Instant::now();
    let mut state = finalizer();
    assert!(state
        .ingest(1, &source("She said, \"Dr. S"), now)
        .unwrap()
        .is_empty());
    let trigger = source("mith is here.\" Next");
    let finals = state.ingest(2, &trigger, now).unwrap();
    assert_eq!(finals[0].text, "She said, \"Dr. Smith is here.\"");
    assert_eq!(finals[0].source_through, 1);
    assert_eq!(finals[0].result.as_deref(), Some(&trigger));
}

#[test]
fn transcript_language_is_provenance_not_matcher_configuration() {
    let now = Instant::now();
    let mut source = source("Das ist bzw. wichtig");
    source.language_code = Some("de-DE".to_owned());
    let expected = match_endofsentence(&source.text, None);
    let mut state = finalizer();
    let finals = state.ingest(1, &source, now).unwrap();
    assert_ne!(expected, 0);
    assert_eq!(finals[0].text.chars().count(), expected);
    assert_eq!(finals[0].result.as_deref(), Some(&source));
}

#[test]
fn a_late_fragment_after_timeout_is_a_new_segment() {
    let now = Instant::now();
    let mut state = finalizer();
    state.ingest(1, &source("hel"), now).unwrap();
    let finals = state.expire(now + INACTIVITY).unwrap();
    assert_eq!(finals[0].text, "hel");
    assert_eq!(finals[0].source_through, 1);
    let later = now + INACTIVITY + Duration::from_nanos(1);
    let finals = state.ingest(2, &source("lo."), later).unwrap();
    assert_eq!(finals[0].text, "lo.");
    assert_eq!(finals[0].source_through, 2);
}

#[test]
fn completion_or_eof_clock_observation_cannot_force_an_early_final() {
    let now = Instant::now();
    let mut state = finalizer();
    state.ingest(1, &source("pending"), now).unwrap();
    // The helper exposes no turn-complete/EOF flush. The owner's only expiration
    // operation must still honor the source deadline after either event.
    assert!(state.expire(now).unwrap().is_empty());
    assert!(state
        .expire(now + Duration::from_millis(499))
        .unwrap()
        .is_empty());
    assert_eq!(state.covered_watermark(), 0);
    assert_eq!(state.buffer, "pending");
    assert_eq!(state.expire(now + INACTIVITY).unwrap()[0].text, "pending");
}

#[test]
fn default_byte_bounds_accept_exact_limit_and_reject_overflow() {
    let now = Instant::now();
    let limit = 64 * 1024;
    let mut state = finalizer();
    let exact = "x".repeat(limit);
    state.ingest(1, &source(&exact), now).unwrap();
    assert_eq!(state.buffer.len(), limit);
    assert_eq!(state.expire(now + INACTIVITY).unwrap()[0].text, exact);

    let mut state = finalizer();
    assert_eq!(
        state
            .ingest(1, &source(&"x".repeat(limit + 1)), now)
            .unwrap_err(),
        GeminiError::TooLarge
    );
    assert!(state.buffer.is_empty());
    assert!(state.deadline().is_none());
    assert_eq!(
        state.ingest(1, &source("Okay."), now).unwrap_err(),
        GeminiError::Closed
    );
    assert_eq!(
        state.expire(now + INACTIVITY).unwrap_err(),
        GeminiError::Closed
    );
}

#[test]
fn suffix_overflow_fails_closed_without_truncation_or_forced_finalization() {
    let now = Instant::now();
    let mut state = SourceFinalizer::new(16, 5).unwrap();
    state.ingest(1, &source("hello"), now).unwrap();
    assert_eq!(
        state.ingest(2, &source("x"), now).unwrap_err(),
        GeminiError::TooLarge
    );
    assert!(state.buffer.is_empty());
    assert!(state.fragments.is_empty());
    assert!(state.deadline().is_none());
    assert_eq!(
        state.expire(now + INACTIVITY).unwrap_err(),
        GeminiError::Closed
    );
}

#[test]
fn temporary_buffer_can_exceed_suffix_bound_when_punctuation_consumes_it() {
    let now = Instant::now();
    let mut state = SourceFinalizer::new(16, 5).unwrap();
    state.ingest(1, &source("hello"), now).unwrap();
    let finals = state.ingest(2, &source(". Next"), now).unwrap();
    assert_eq!(finals[0].text, "hello.");
    assert_eq!(finals[0].source_through, 1);
    assert_eq!(state.buffer, " Next");
}

#[test]
fn source_watermarks_reject_gaps_replay_and_counter_exhaustion() {
    let now = Instant::now();
    for invalid in [0, 2, u64::MAX] {
        let mut state = finalizer();
        assert!(matches!(
            state.ingest(invalid, &source("x"), now),
            Err(GeminiError::Protocol(_))
        ));
        assert!(state.failed);
    }
    let mut state = finalizer();
    state.ingest(1, &source("x"), now).unwrap();
    assert!(matches!(
        state.ingest(1, &source("x"), now),
        Err(GeminiError::Protocol(_))
    ));

    let mut state = finalizer();
    state.received_watermark = u64::MAX;
    state.covered_watermark = u64::MAX;
    assert!(matches!(
        state.ingest(0, &source("x"), now),
        Err(GeminiError::Protocol(_))
    ));
    assert!(state.failed);
}

#[test]
fn language_provenance_and_constructor_bounds_are_finite() {
    assert!(SourceFinalizer::new(0, 1).is_err());
    assert!(SourceFinalizer::new(1, 0).is_err());
    assert!(SourceFinalizer::new(usize::MAX, 1).is_err());
    assert!(SourceFinalizer::new(isize::MAX as usize, 1).is_err());
    let mut state = SourceFinalizer::new(8, 8).unwrap();
    let mut transcript = source("Hi.");
    transcript.language_code = Some("x".repeat(9));
    assert_eq!(
        state.ingest(1, &transcript, Instant::now()).unwrap_err(),
        GeminiError::TooLarge
    );
}

#[test]
fn retained_fragment_count_is_bounded_by_retained_source_bytes() {
    let now = Instant::now();
    let mut state = SourceFinalizer::new(8, 8).unwrap();
    for watermark in 1..=8 {
        state.ingest(watermark, &source("x"), now).unwrap();
        assert_eq!(state.fragments.len(), watermark as usize);
        assert_eq!(state.buffer.len(), watermark as usize);
    }
    assert_eq!(
        state.ingest(9, &source("x"), now).unwrap_err(),
        GeminiError::TooLarge
    );
}

#[test]
fn unicode_boundary_conversion_is_checked_instead_of_clamped() {
    assert_eq!(character_boundary("👋你a", 0).unwrap(), 0);
    assert_eq!(character_boundary("👋你a", 1).unwrap(), 4);
    assert_eq!(character_boundary("👋你a", 2).unwrap(), 7);
    assert_eq!(character_boundary("👋你a", 3).unwrap(), 8);
    assert!(character_boundary("👋你a", 4).is_err());
}

#[test]
fn multibyte_fragment_offsets_remain_correct_after_leading_strip_and_consumption() {
    let now = Instant::now();
    let mut state = finalizer();
    assert!(state.ingest(1, &source("  你好"), now).unwrap().is_empty());
    let finals = state.ingest(2, &source("。下一句"), now).unwrap();
    assert_eq!(finals.len(), 1);
    assert_eq!(finals[0].text, "你好。");
    assert_eq!(finals[0].source_through, 1);
    assert_eq!(state.buffer, "下一句");
    assert_eq!(state.fragments.len(), 1);
    assert_eq!(state.fragments[0].end, "下一句".len());
    assert_eq!(state.fragments[0].watermark, 2);
    let finals = state.expire(now + INACTIVITY).unwrap();
    assert_eq!(finals[0].text, "下一句");
    assert_eq!(finals[0].source_through, 2);
}
