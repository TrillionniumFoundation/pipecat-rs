//! Independent alignment safety and source-range regressions.
use pipecat_text::*;

fn tracker(text: &str) -> WordCompletionTracker {
    WordCompletionTracker::new(9, text, Some(text), None, AlignmentLimits::default()).unwrap()
}

#[test]
fn closing_after_attributed_punctuation_emits_each_source_character_once() {
    let text = "Yeah, I can do that.";
    let mut t = tracker(text);
    let first = t.add_word(9, "Yeah").unwrap();
    assert_eq!(first.source_text.as_deref(), Some("Yeah,"));
    let rest = t.complete_unreported(9, None).unwrap();
    assert_eq!(rest.evidence, AlignmentEvidence::UnreportedRemainder);
    assert_eq!(rest.raw_pos, 4);
    assert_eq!(
        rest.source_span.unwrap().start,
        first.source_span.unwrap().end
    );
    assert_eq!(
        format!(
            "{}{}",
            first.source_text.unwrap(),
            rest.source_text.unwrap()
        ),
        text
    );
}

#[test]
fn forced_overflow_does_not_repeat_attributed_punctuation() {
    let text = "Yeah, I can do that.";
    let mut t = tracker(text);
    let first = t.add_word(9, "Yeah").unwrap();
    let rest = t.add_word(9, "unmatched").unwrap();
    assert_eq!(rest.evidence, AlignmentEvidence::UnreportedRemainder);
    assert_eq!(rest.overflow.as_deref(), Some("unmatched"));
    assert_eq!(
        format!(
            "{}{}",
            first.source_text.unwrap(),
            rest.source_text.unwrap()
        ),
        text
    );
}

#[test]
fn transformed_punctuation_never_attributes_the_next_source_word() {
    let mut t = WordCompletionTracker::new(
        9,
        "a------ b",
        Some("a b"),
        Some("a b"),
        AlignmentLimits::default(),
    )
    .unwrap();
    let first = t.add_word(9, "a------").unwrap();
    assert!(!first.complete);
    assert!(first.source_span.unwrap().end <= 2);
    assert!(!first.source_text.as_deref().unwrap().contains('b'));
    let last = t.add_word(9, "b").unwrap();
    assert!(last.complete);
    assert_eq!(
        format!(
            "{}{}",
            first.source_text.unwrap(),
            last.source_text.unwrap()
        ),
        "a b"
    );
}

#[test]
fn punctuation_and_markup_variants_keep_scalar_cursors_in_bounds() {
    let displays = ["a b", "a, b", "a— b", "a------ b", "\na\t\tb  "];
    let synthesis = ["a b", "a, b", "a— b", "a------ b", "a\u{1c}b"];
    for display in displays {
        for tts in synthesis {
            for tagged in [false, true] {
                let llm = if tagged {
                    format!("<card>{display}</card>")
                } else {
                    display.to_owned()
                };
                let mut t = WordCompletionTracker::new(
                    9,
                    tts,
                    Some(&llm),
                    Some(display),
                    AlignmentLimits::default(),
                )
                .unwrap();
                let mut source = String::new();
                let mut end = 0;
                for word in tts.split(|c: char| c.is_whitespace() || c == '\u{1c}') {
                    let r = t.add_word(9, word).unwrap();
                    assert!(r.raw_pos <= tts.chars().count(), "{tts:?}: {r:?}");
                    assert!(
                        r.display_pos <= display.chars().count(),
                        "tts={tts:?} display={display:?}: {r:?}"
                    );
                    if let Some(span) = r.source_span {
                        assert_eq!(span.start, end, "{tts:?} {display:?}");
                        assert!(span.end >= span.start);
                        assert!(span.end <= llm.chars().count());
                        end = span.end;
                    }
                    source.push_str(r.source_text.as_deref().unwrap_or(""));
                }
                let r = t.complete_unreported(9, None).unwrap();
                source.push_str(r.source_text.as_deref().unwrap_or(""));
                assert_eq!(source, llm, "tts={tts:?} display={display:?}");
            }
        }
    }
}

#[test]
fn incremental_expanded_punctuation_cannot_overrun_display_offsets() {
    let mut t = WordCompletionTracker::new(
        9,
        "a------ b",
        Some("a b"),
        Some("a b"),
        AlignmentLimits::default(),
    )
    .unwrap();
    for word in ["a", "-", "-", "-", "-", "-", "-", "b"] {
        let result = t.add_word(9, word).unwrap();
        assert!(result.display_pos <= 3, "word={word:?}: {result:?}");
        if word != "b" {
            assert!(
                !result.source_text.as_deref().unwrap_or("").contains('b'),
                "unreported next source word attributed by {word:?}: {result:?}"
            );
        }
    }
}
