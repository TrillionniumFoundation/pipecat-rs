//! Legacy pure-text matching only. This does not establish parity for the
//! recommended wake-phrase turn strategy or for any pipeline/frame authority.
use pipecat_text::{WakeCheckError, WakeCheckLimits, WakeCheckMatch, WakeCheckMatcher};

fn matcher(phrases: &[&str]) -> WakeCheckMatcher {
    WakeCheckMatcher::new(phrases, WakeCheckLimits::default()).unwrap()
}

fn found(phrases: &[&str], text: &str) -> Option<WakeCheckMatch> {
    matcher(phrases).find(text).unwrap()
}

fn suffix<'a>(phrases: &[&str], text: &'a str) -> Option<&'a str> {
    found(phrases, text).map(|matched| &text[matched.suffix_range])
}

#[test]
fn fixed_original_python_matcher_oracle_for_every_supported_case() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/wake_check_source.json")).unwrap();
    assert_eq!(fixture["schema_version"], 1);
    assert_eq!(
        fixture["source_revision"],
        "af9f9da9d7afddaa8811c0e0251b32566c980bdf"
    );
    assert_eq!(
        fixture["source_blob"],
        "70a233000b5498c98b1747e15d94ab58bae7c844"
    );
    assert_eq!(fixture["unicode"], "15.0.0");
    let cases = fixture["match_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 72);
    let mut supported = 0;
    let mut rejected = 0;
    for case in cases {
        let name = case["name"].as_str().unwrap();
        let phrases: Vec<_> = case["phrases"]
            .as_array()
            .unwrap()
            .iter()
            .map(|phrase| phrase.as_str().unwrap())
            .collect();
        let matcher = WakeCheckMatcher::new(phrases, WakeCheckLimits::default());
        // Reject exactly the documented migration exclusions, rather than
        // silently skipping any fixture whose constructor unexpectedly fails.
        let excluded = match name {
            "empty_phrase_source_0" => Some(WakeCheckError::EmptyPhrase),
            "empty_phrase_list_source_0" => Some(WakeCheckError::EmptyPhrases),
            "python_no_normalization_0" => Some(WakeCheckError::NonAsciiToken),
            _ => None,
        };
        if let Some(expected) = excluded {
            assert_eq!(matcher.unwrap_err(), expected, "{name}");
            rejected += 1;
            continue;
        }
        let matcher = matcher.unwrap();
        let text = case["text"].as_str().unwrap();
        let actual = matcher.find(text).unwrap();
        let expected = &case["match"];
        if expected.is_null() {
            assert!(actual.is_none(), "{name}: {actual:?}");
        } else {
            let actual = actual.unwrap_or_else(|| panic!("missing match: {name}"));
            assert_eq!(
                actual.phrase_index,
                expected["phrase_index"].as_u64().unwrap() as usize,
                "{name}"
            );
            assert_eq!(
                actual.match_range.start,
                expected["start_byte"].as_u64().unwrap() as usize,
                "{name}"
            );
            assert_eq!(
                actual.match_range.end,
                expected["end_byte"].as_u64().unwrap() as usize,
                "{name}"
            );
            assert_eq!(
                text[..actual.match_range.start].chars().count(),
                expected["start_char"].as_u64().unwrap() as usize,
                "{name}"
            );
            assert_eq!(
                text[..actual.match_range.end].chars().count(),
                expected["end_char"].as_u64().unwrap() as usize,
                "{name}"
            );
            assert_eq!(
                actual.suffix_range,
                actual.match_range.start..text.len(),
                "{name}"
            );
            assert_eq!(
                &text[actual.suffix_range],
                expected["suffix"].as_str().unwrap(),
                "{name}"
            );
        }
        supported += 1;
    }
    assert_eq!((supported, rejected), (69, 3));
}

#[test]
fn configured_order_precedes_global_position_and_same_phrase_uses_first_start() {
    let text = "alpha here, BETA then beta";
    let matched = found(&["beta", "alpha"], text).unwrap();
    assert_eq!(matched.phrase_index, 0);
    assert_eq!(matched.match_range, 12..16);
    assert_eq!(&text[matched.suffix_range], "BETA then beta");
    assert_eq!(
        found(&["missing", "alpha", "beta"], text)
            .unwrap()
            .phrase_index,
        1
    );
    assert_eq!(suffix(&["a b", "ab"], "xab ab ab"), Some("ab ab"));
    assert_eq!(found(&["a", "a b"], "ab a b").unwrap().match_range, 3..4);
    assert_eq!(found(&["a b", "a"], "ab a b").unwrap().match_range, 0..2);
    assert_eq!(found(&["a", "a"], "a").unwrap().phrase_index, 0);
}

#[test]
fn whitespace_is_python_split_and_zero_or_more_between_tokens() {
    for gap in [
        "", " ", "\t", "\n\r", "\u{b}", "\u{c}", "\u{1c}", "\u{1d}", "\u{1e}", "\u{1f}", "\u{85}",
        "\u{a0}", "\u{1680}", "\u{2000}", "\u{2007}", "\u{2028}", "\u{2029}", "\u{202f}",
        "\u{205f}", "\u{3000}",
    ] {
        let text = format!("prefix hey{gap}bot tail");
        let matched = found(&["hey bot"], &text).unwrap();
        assert_eq!(&text[matched.match_range], format!("hey{gap}bot"));
        assert_eq!(&text[matched.suffix_range], format!("hey{gap}bot tail"));
    }
    for gap in ["\u{180e}", "\u{200b}", "\u{200d}", "\u{feff}", "-"] {
        assert!(found(&["hey bot"], &format!("hey{gap}bot")).is_none());
    }
    assert_eq!(
        suffix(&["\u{3000}hey\u{1c}\u{a0}bot\u{1f}"], "heybOt!"),
        Some("heybOt!")
    );
    assert_eq!(suffix(&["a \t b\n c"], "abc"), Some("abc"));
}

#[test]
fn literal_punctuation_uses_true_xor_word_boundaries() {
    // A leading nonword literal needs a preceding word; a trailing nonword
    // literal needs a following word. Python \b is false at nonword/text edges.
    for text in ["!hey", " !hey", "!!hey", "x!hey_", "x!hey中"] {
        assert!(found(&["!hey"], text).is_none(), "{text:?}");
    }
    for text in ["x!hey", "x!hey!", "中!hey ", "_!HEY"] {
        assert_eq!(
            suffix(&["!hey"], text),
            Some(&text[text.find('!').unwrap()..])
        );
    }
    for text in ["hey!", "hey! ", "hey!!", "_hey!x", "中hey!x"] {
        assert!(found(&["hey!"], text).is_none(), "{text:?}");
    }
    assert_eq!(suffix(&["hey!"], "hey!x tail"), Some("hey!x tail"));
    assert_eq!(suffix(&["!"], "x!y"), Some("!y"));
    assert!(found(&["!"], "!").is_none());
    assert_eq!(suffix(&["!hey!"], "x!hey!y"), Some("!hey!y"));
    assert_eq!(suffix(&["[a].*"], "x[a].*y"), Some("[a].*y"));
    assert!(found(&["a.b"], "aXb").is_none());
    assert_eq!(suffix(&["a.b"], "a.b"), Some("a.b"));
    assert_eq!(suffix(&["a\\b"], "a\\b"), Some("a\\b"));
    assert_eq!(suffix(&["a\0b"], "a\0b"), Some("a\0b"));
    assert_eq!(suffix(&["a - b"], "a-b"), Some("a-b"));
}

#[test]
fn unicode_15_word_alphabet_is_not_rust_regex_or_pronunciation_word_alphabet() {
    // Numbers include Python \w's nondecimal number categories, such as ².
    // Kawi U+11F02 and Nag Mundari U+1E4D0 were added in Unicode 15.
    for adjacent in ["_", "中", "é", "²", "Ⅷ", "١", "\u{11f02}", "\u{1e4d0}"] {
        assert!(found(&["hey"], &format!("{adjacent}hey")).is_none());
        assert!(found(&["hey"], &format!("hey{adjacent}")).is_none());
    }
    // Combining marks/ZWJ are not Python word chars; ASCII hyphen is not a
    // word char either, unlike the separately bounded pronunciation profile.
    for adjacent in ["-", "\u{301}", "\u{345}", "\u{200d}", "\u{203f}", "🙂"] {
        let text = format!("{adjacent}hey{adjacent}");
        let matched = found(&["hey"], &text).unwrap();
        assert_eq!(&text[matched.match_range], "hey");
        assert_eq!(matched.suffix_range.start, adjacent.len());
    }
    assert_eq!(suffix(&["hey_bot"], "hey_bot"), Some("hey_bot"));
    assert!(found(&["hey"], "hey_bot").is_none());
}

#[test]
fn python_ascii_ignorecase_extras_keep_original_multibyte_ranges() {
    for (phrase, input) in [("i", "İ"), ("I", "ı"), ("s", "ſ"), ("K", "K")] {
        let text = format!("🙂 {input}! tail");
        let matched = found(&[phrase], &text).unwrap();
        assert_eq!(matched.match_range, 5..5 + input.len());
        assert_eq!(matched.suffix_range, 5..text.len());
        assert_eq!(&text[matched.match_range], input);
    }
    let text = "é 🙂 İıſK!";
    let matched = found(&["I I S K"], text).unwrap();
    assert_eq!(matched.match_range, 8..17);
    assert_eq!(matched.suffix_range, 8..18);
    assert_eq!(&text[matched.suffix_range], "İıſK!");
    assert_eq!(suffix(&["hey bot"], "HEYbOt"), Some("HEYbOt"));
    for (phrase, text) in [
        ("ss", "ß"),
        ("ff", "ﬀ"),
        ("e", "é"),
        ("cafe", "café"),
        ("ab", "a\u{301}b"),
    ] {
        assert!(found(&[phrase], text).is_none(), "{phrase:?} in {text:?}");
    }
    assert_eq!(suffix(&["e"], "e\u{301}"), Some("e\u{301}"));
}

#[test]
fn explicit_migration_profile_rejects_empty_and_non_ascii_tokens() {
    let limits = WakeCheckLimits::default();
    assert!(matches!(
        WakeCheckMatcher::new(Vec::<String>::new(), limits),
        Err(WakeCheckError::EmptyPhrases)
    ));
    for phrase in ["", " \t\r\n", "\u{1c}\u{a0}\u{3000}"] {
        assert!(matches!(
            WakeCheckMatcher::new([phrase], limits),
            Err(WakeCheckError::EmptyPhrase)
        ));
    }
    for phrase in ["café", "中", "İ", "ı", "ſ", "K", "hey\u{200b}bot"] {
        assert!(matches!(
            WakeCheckMatcher::new([phrase], limits),
            Err(WakeCheckError::NonAsciiToken)
        ));
    }
    assert!(found(&["hey"], "").is_none());
}

#[test]
fn raw_configuration_bytes_duplicates_and_iterator_visits_are_bounded() {
    let defaults = WakeCheckLimits::default();
    assert!(matches!(
        WakeCheckMatcher::new(
            ["a", "a"],
            WakeCheckLimits {
                max_phrases: 1,
                ..defaults
            }
        ),
        Err(WakeCheckError::TooManyPhrases)
    ));
    assert!(matches!(
        WakeCheckMatcher::new(
            [" a "],
            WakeCheckLimits {
                max_phrase_bytes: 2,
                ..defaults
            }
        ),
        Err(WakeCheckError::PhraseTooLong)
    ));
    assert!(matches!(
        WakeCheckMatcher::new(
            ["\u{a0}a"],
            WakeCheckLimits {
                max_phrase_bytes: 2,
                ..defaults
            }
        ),
        Err(WakeCheckError::PhraseTooLong)
    ));
    assert!(matches!(
        WakeCheckMatcher::new(
            [" a ", "a"],
            WakeCheckLimits {
                max_total_phrase_bytes: 3,
                ..defaults
            }
        ),
        Err(WakeCheckError::TotalPhraseBytesExceeded)
    ));
    assert!(WakeCheckMatcher::new(
        [" a ", "a"],
        WakeCheckLimits {
            max_total_phrase_bytes: 4,
            ..defaults
        }
    )
    .is_ok());
    let visited = std::cell::Cell::new(0);
    let infinite = std::iter::repeat_with(|| {
        visited.set(visited.get() + 1);
        "a"
    });
    assert!(matches!(
        WakeCheckMatcher::new(
            infinite,
            WakeCheckLimits {
                max_phrases: 2,
                ..defaults
            }
        ),
        Err(WakeCheckError::TooManyPhrases)
    ));
    assert_eq!(visited.get(), 3);
}

#[test]
fn every_limit_is_positive_and_has_a_hard_ceiling() {
    let defaults = WakeCheckLimits::default();
    for invalid in [0, usize::MAX] {
        for limits in [
            WakeCheckLimits {
                max_phrases: invalid,
                ..defaults
            },
            WakeCheckLimits {
                max_phrase_bytes: invalid,
                ..defaults
            },
            WakeCheckLimits {
                max_total_phrase_bytes: invalid,
                ..defaults
            },
            WakeCheckLimits {
                max_input_bytes: invalid,
                ..defaults
            },
            WakeCheckLimits {
                max_match_work: invalid,
                ..defaults
            },
        ] {
            assert!(matches!(
                WakeCheckMatcher::new(["a"], limits),
                Err(WakeCheckError::InvalidLimits)
            ));
        }
    }
    let ceilings = WakeCheckLimits {
        max_phrases: 128,
        max_phrase_bytes: 1024,
        max_total_phrase_bytes: 64 * 1024,
        max_input_bytes: 64 * 1024,
        max_match_work: 4 * 1024 * 1024,
    };
    assert!(WakeCheckMatcher::new(["a"], ceilings).is_ok());
    for limits in [
        WakeCheckLimits {
            max_phrases: 129,
            ..defaults
        },
        WakeCheckLimits {
            max_phrase_bytes: 1025,
            ..defaults
        },
        WakeCheckLimits {
            max_total_phrase_bytes: 64 * 1024 + 1,
            ..defaults
        },
        WakeCheckLimits {
            max_input_bytes: 64 * 1024 + 1,
            ..defaults
        },
        WakeCheckLimits {
            max_match_work: 4 * 1024 * 1024 + 1,
            ..defaults
        },
    ] {
        assert!(matches!(
            WakeCheckMatcher::new(["a"], limits),
            Err(WakeCheckError::InvalidLimits)
        ));
    }
}

#[test]
fn byte_input_limits_fail_before_search_and_do_not_mutate_input() {
    let matcher = WakeCheckMatcher::new(
        ["i"],
        WakeCheckLimits {
            max_input_bytes: 2,
            ..WakeCheckLimits::default()
        },
    )
    .unwrap();
    assert_eq!(matcher.find("İ").unwrap().unwrap().match_range, 0..2);
    let text = String::from("İ!");
    assert_eq!(matcher.find(&text), Err(WakeCheckError::InputTooLong));
    assert_eq!(text, "İ!");
    assert_eq!(matcher.find("i").unwrap().unwrap().match_range, 0..1);
}

#[test]
fn work_budget_has_exact_edges_and_resets_after_success_and_failure() {
    let limits = WakeCheckLimits::default();
    // One phrase + one start scalar + one literal + final boundary = four.
    let tight = WakeCheckMatcher::new(
        ["a"],
        WakeCheckLimits {
            max_match_work: 3,
            ..limits
        },
    )
    .unwrap();
    assert_eq!(tight.find("a"), Err(WakeCheckError::WorkLimitExceeded));
    assert_eq!(tight.find("a"), Err(WakeCheckError::WorkLimitExceeded));
    assert_eq!(tight.find(""), Ok(None));
    assert_eq!(tight.find("!"), Ok(None));
    let exact = WakeCheckMatcher::new(
        ["a"],
        WakeCheckLimits {
            max_match_work: 4,
            ..limits
        },
    )
    .unwrap();
    assert_eq!(exact.find("a"), exact.find("a"));
    assert!(exact.find("a").unwrap().is_some());
    assert_eq!(exact.find("x a"), Err(WakeCheckError::WorkLimitExceeded));
    assert!(exact.find("a").unwrap().is_some());

    // Searching z costs seven operations; searching b then costs eight. The
    // budget must be cumulative across phrases rather than resetting per phrase.
    for (budget, succeeds) in [(14, false), (15, true)] {
        let matcher = WakeCheckMatcher::new(
            ["z", "b"],
            WakeCheckLimits {
                max_match_work: budget,
                ..limits
            },
        )
        .unwrap();
        if succeeds {
            assert_eq!(matcher.find("a b").unwrap().unwrap().match_range, 2..3);
        } else {
            assert_eq!(matcher.find("a b"), Err(WakeCheckError::WorkLimitExceeded));
        }
    }
    // One phrase + start + two literals + two gap lookaheads + end = seven.
    let whitespace = WakeCheckMatcher::new(
        ["a b"],
        WakeCheckLimits {
            max_match_work: 7,
            ..limits
        },
    )
    .unwrap();
    assert!(whitespace.find("a b").unwrap().is_some());
    assert_eq!(
        whitespace.find("a  b"),
        Err(WakeCheckError::WorkLimitExceeded)
    );
    assert!(whitespace.find("ab").unwrap().is_some());
}

#[test]
fn adversarial_shared_prefixes_hit_work_limit_instead_of_returning_later_match() {
    let prefix = "a!".repeat(127);
    let phrase = format!("{prefix}ab");
    let text = "a!".repeat(8 * 1024);
    let matcher =
        WakeCheckMatcher::new([phrase.as_str(), "a"], WakeCheckLimits::default()).unwrap();
    assert_eq!(matcher.find(&text), Err(WakeCheckError::WorkLimitExceeded));
    assert_eq!(matcher.find("a").unwrap().unwrap().phrase_index, 1);

    // Shared prefixes must also be charged repeatedly across duplicate phrases.
    let phrase = format!("{}b", "a".repeat(254));
    let text = format!("{}c ", "a".repeat(254)).repeat(64);
    assert_eq!(text.len(), 16 * 1024);
    let matcher =
        WakeCheckMatcher::new(std::iter::repeat_n(phrase, 32), WakeCheckLimits::default()).unwrap();
    assert_eq!(matcher.find(&text), Err(WakeCheckError::WorkLimitExceeded));
}

#[test]
fn debug_and_errors_do_not_disclose_input_or_configuration() {
    let matcher = matcher(&["private-phrase"]);
    assert!(!format!("{matcher:?}").contains("private-phrase"));
    for error in [
        WakeCheckError::InvalidLimits,
        WakeCheckError::EmptyPhrases,
        WakeCheckError::TooManyPhrases,
        WakeCheckError::PhraseTooLong,
        WakeCheckError::TotalPhraseBytesExceeded,
        WakeCheckError::EmptyPhrase,
        WakeCheckError::NonAsciiToken,
        WakeCheckError::InputTooLong,
        WakeCheckError::WorkLimitExceeded,
    ] {
        assert!(!format!("{error}: {error:?}").contains("private-phrase"));
    }
}
