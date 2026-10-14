//! Bounded native pronunciation slice. Oracle executes fixed original Python AST;
//! this is not provider/gating/pipeline equivalence or all-Unicode regex parity.
use pipecat_core::TtsTextInputFrame;
use pipecat_text::*;
use serde_json::Value;

fn entries(items: &[(&str, &str)]) -> Vec<(String, String)> {
    items
        .iter()
        .map(|(a, b)| ((*a).into(), (*b).into()))
        .collect()
}
fn transform(items: &[(&str, &str)]) -> CartesiaPronunciationTransform {
    CartesiaPronunciationTransform::new(entries(items), PronunciationLimits::default()).unwrap()
}
#[test]
fn fixed_original_python_formatter_and_transform_oracle() {
    let fixture: Value =
        serde_json::from_str(include_str!("fixtures/pronunciations_source.json")).unwrap();
    assert_eq!(fixture["source_tests_run"], 9);
    assert_eq!(
        fixture["source_blobs"]["phonemes.py"],
        "a71d06854e74a2069b4ba7e550aa4126b17fbf6d"
    );
    assert_eq!(
        fixture["source_blobs"]["pronunciations.py"],
        "47591a62188c7f873bcca62f76267c2a27a60d0b"
    );
    assert_eq!(
        fixture["source_blobs"]["cartesia_tts.py"],
        "70c492f5bb63402b6ba7d7c3a3f049fc8d84b86c"
    );
    let formats = fixture["format_cases"].as_array().unwrap();
    assert_eq!(formats.len(), 43);
    for case in formats {
        let ipa = case["ipa"].as_str().unwrap();
        let actual = format_cartesia_pronunciation(ipa, PronunciationLimits::default()).unwrap();
        assert_eq!(actual.as_deref(), case["formatted"].as_str(), "IPA {ipa:?}");
    }
    let cases = fixture["transform_cases"].as_array().unwrap();
    assert_eq!(cases.len(), 15);
    for case in cases {
        let entries: Vec<_> = case["entries"]
            .as_array()
            .unwrap()
            .iter()
            .map(|item| {
                (
                    item[0].as_str().unwrap().to_owned(),
                    item[1].as_str().unwrap().to_owned(),
                )
            })
            .collect();
        let transform =
            CartesiaPronunciationTransform::new(entries, PronunciationLimits::default()).unwrap();
        assert_eq!(
            transform.apply(case["text"].as_str().unwrap()).unwrap(),
            case["expected"].as_str().unwrap(),
            "{}",
            case["name"]
        );
    }
}
#[test]
fn native_profile_rejects_non_ascii_dictionary_and_does_not_python_unicode_casefold() {
    for key in ["café", "中", "K", "ſ", "İ", "ı"] {
        assert!(
            matches!(
                CartesiaPronunciationTransform::new(
                    entries(&[(key, "a")]),
                    PronunciationLimits::default()
                ),
                Err(PronunciationError::NonAsciiWord)
            ),
            "{key}"
        );
    }
    let transform = transform(&[("k", "a"), ("s", "b"), ("i", "c")]);
    assert_eq!(
        transform.apply("K S I K ſ İ ı").unwrap(),
        "<<a>> <<b>> <<c>> K ſ İ ı"
    );
    // U+11F02 Kawi letter is new in Unicode 15. The native pinned profile must
    // treat it as a word boundary blocker, independently of Python's UCD age.
    assert_eq!(
        transform.apply("\u{11f02}k k\u{11f02}").unwrap(),
        "\u{11f02}k k\u{11f02}"
    );
}
#[test]
fn pure_stress_empty_unknown_and_duplicate_entries_preserve_source_semantics() {
    let transform = transform(&[
        ("cat", "a"),
        ("CAT", "b"),
        (" cat ", "[]"),
        ("empty", " "),
        ("stress", "ˈ"),
        ("unknown", "?"),
    ]);
    assert_eq!(
        transform.apply("cat empty stress unknown").unwrap(),
        "<<b>> empty <<>> <<?>>"
    );
    assert_eq!(
        format_cartesia_pronunciation("", PronunciationLimits::default()).unwrap(),
        None
    );
    assert_eq!(
        format_cartesia_pronunciation("ˈˌ", PronunciationLimits::default())
            .unwrap()
            .as_deref(),
        Some("<<>>")
    );
}
#[test]
fn only_synthesis_is_changed_even_with_preexisting_markup_and_distinct_channels() {
    let transform = transform(&[("cat", "kæt")]);
    let mut frame = TtsTextInputFrame::new("<<cat>> cat");
    frame.display_text = "Visible cat".into();
    frame.original_text = "<b>Original cat</b>".into();
    frame.append_to_context = false;
    frame.includes_inter_frame_spaces = false;
    frame.aggregated_by = "custom".into();
    let before = frame.clone();
    transform.apply_to_input(&mut frame).unwrap();
    assert_eq!(frame.synthesis_text, "<<<<k|æ|t>>>> <<k|æ|t>>");
    frame.synthesis_text = before.synthesis_text.clone();
    assert_eq!(frame, before);
}
#[test]
fn constructor_limits_account_for_raw_ignored_and_duplicate_entries() {
    let defaults = PronunciationLimits::default();
    assert!(matches!(
        CartesiaPronunciationTransform::new(
            entries(&[("a", "a"), ("a", "b")]),
            PronunciationLimits {
                max_entries: 1,
                ..defaults
            }
        ),
        Err(PronunciationError::TooManyEntries)
    ));
    assert!(matches!(
        CartesiaPronunciationTransform::new(
            entries(&[(" a ", "a")]),
            PronunciationLimits {
                max_word_bytes: 2,
                ..defaults
            }
        ),
        Err(PronunciationError::WordTooLong)
    ));
    assert!(matches!(
        CartesiaPronunciationTransform::new(
            entries(&[("", "æ")]),
            PronunciationLimits {
                max_ipa_bytes: 1,
                ..defaults
            }
        ),
        Err(PronunciationError::IpaTooLong)
    ));
    assert!(matches!(
        CartesiaPronunciationTransform::new(
            entries(&[("a", "a")]),
            PronunciationLimits {
                max_total_bytes: 6,
                ..defaults
            }
        ),
        Err(PronunciationError::TotalBytesExceeded)
    ));
    assert!(CartesiaPronunciationTransform::new(
        entries(&[("a", "a")]),
        PronunciationLimits {
            max_total_bytes: 7,
            ..defaults
        }
    )
    .is_ok());
    assert!(matches!(
        CartesiaPronunciationTransform::new(
            entries(&[("a", "a"), ("a", "a")]),
            PronunciationLimits {
                max_total_bytes: 13,
                ..defaults
            }
        ),
        Err(PronunciationError::TotalBytesExceeded)
    ));
    for limits in [
        PronunciationLimits {
            max_entries: 0,
            ..defaults
        },
        PronunciationLimits {
            max_word_bytes: 0,
            ..defaults
        },
        PronunciationLimits {
            max_ipa_bytes: 0,
            ..defaults
        },
        PronunciationLimits {
            max_total_bytes: 0,
            ..defaults
        },
        PronunciationLimits {
            max_input_bytes: 0,
            ..defaults
        },
        PronunciationLimits {
            max_output_bytes: 0,
            ..defaults
        },
        PronunciationLimits {
            max_entries: usize::MAX,
            ..defaults
        },
        PronunciationLimits {
            max_word_bytes: usize::MAX,
            ..defaults
        },
        PronunciationLimits {
            max_ipa_bytes: usize::MAX,
            ..defaults
        },
        PronunciationLimits {
            max_total_bytes: usize::MAX,
            ..defaults
        },
        PronunciationLimits {
            max_input_bytes: usize::MAX,
            ..defaults
        },
        PronunciationLimits {
            max_output_bytes: usize::MAX,
            ..defaults
        },
    ] {
        assert!(matches!(
            CartesiaPronunciationTransform::new(Vec::<(String, String)>::new(), limits),
            Err(PronunciationError::InvalidLimits)
        ));
    }
}
#[test]
fn input_and_expanded_output_limits_are_bytes_and_failure_is_atomic() {
    let defaults = PronunciationLimits::default();
    let transform = CartesiaPronunciationTransform::new(
        entries(&[("cat", "a")]),
        PronunciationLimits {
            max_input_bytes: 7,
            max_output_bytes: 10,
            ..defaults
        },
    )
    .unwrap();
    assert_eq!(transform.apply("cat").unwrap(), "<<a>>");
    assert_eq!(
        transform.apply("cat cat"),
        Err(PronunciationError::OutputTooLong)
    );
    assert_eq!(
        transform.apply("cat cat "),
        Err(PronunciationError::InputTooLong)
    );
    for synthesis in ["cat cat", "cat cat "] {
        let mut frame = TtsTextInputFrame::new(synthesis);
        frame.display_text = "display".into();
        frame.original_text = "<original>".into();
        frame.append_to_context = false;
        let before = frame.clone();
        assert!(transform.apply_to_input(&mut frame).is_err());
        assert_eq!(frame, before);
    }
    let identity = CartesiaPronunciationTransform::new(
        Vec::<(String, String)>::new(),
        PronunciationLimits {
            max_input_bytes: 4,
            max_output_bytes: 3,
            ..defaults
        },
    )
    .unwrap();
    assert_eq!(identity.apply("ææ"), Err(PronunciationError::OutputTooLong));
    assert_eq!(identity.apply("ææx"), Err(PronunciationError::InputTooLong));
    assert_eq!(identity.apply("æa").unwrap(), "æa");
    assert_eq!(
        format_cartesia_pronunciation(
            "æ",
            PronunciationLimits {
                max_ipa_bytes: 1,
                ..defaults
            }
        ),
        Err(PronunciationError::IpaTooLong)
    );
    assert_eq!(
        format_cartesia_pronunciation(
            "a",
            PronunciationLimits {
                max_output_bytes: 4,
                ..defaults
            }
        ),
        Err(PronunciationError::OutputTooLong)
    );
    assert_eq!(
        format_cartesia_pronunciation(
            "a",
            PronunciationLimits {
                max_output_bytes: 5,
                ..defaults
            }
        )
        .unwrap()
        .as_deref(),
        Some("<<a>>")
    );
}

#[test]
fn skipped_frame_is_noop_even_when_synthesis_exceeds_limits() {
    let transform = CartesiaPronunciationTransform::new(
        entries(&[("cat", "a")]),
        PronunciationLimits {
            max_input_bytes: 3,
            ..PronunciationLimits::default()
        },
    )
    .unwrap();
    let mut frame = TtsTextInputFrame::new("cat cat cat");
    frame.skip_tts = true;
    frame.display_text = "different display".into();
    frame.original_text = "<b>original</b>".into();
    frame.append_to_context = false;
    let before = frame.clone();
    transform.apply_to_input(&mut frame).unwrap();
    assert_eq!(frame, before);
    assert_eq!(
        transform.apply(&frame.synthesis_text),
        Err(PronunciationError::InputTooLong)
    );
}
