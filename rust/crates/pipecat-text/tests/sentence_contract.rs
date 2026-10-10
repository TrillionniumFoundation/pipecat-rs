//! Cases from test_sentencex_tokenizer.py and test_sentence_tokenizer_language.py.
use pipecat_text::{string::*, *};
#[test]
fn unicode_source_character_offsets() {
    for (text, language, prefix) in [
        ("Hello.   Next", "en", "Hello."),
        ("Hello.\n\nNext", "en", "Hello."),
        ("\n\nHello. Next", "en", "\n\nHello."),
        ("👋 Hello. 😀 Next", "en", "👋 Hello."),
        ("Cafe\u{301} is open. Next", "en", "Cafe\u{301} is open."),
        ("こんにちは。次", "ja", "こんにちは。"),
        ("Dr. 田中が来ます。次の文です。", "ja", "Dr. 田中が来ます。"),
        ("U.S.のAWSです。次の文です。", "ja", "U.S.のAWSです。"),
        ("你好。下一句", "zh", "你好。"),
        ("नमस्ते। यह", "hi", "नमस्ते।"),
        ("هل أنت بخير؟ نعم", "ar", "هل أنت بخير؟"),
        ("Hello. Next", "unrecognized", "Hello."),
    ] {
        assert_eq!(
            match_endofsentence(text, Some(language)),
            prefix.chars().count(),
            "{language}: {text}"
        );
    }
}
fn stream(language: &str, sentences: &[&str], chunk_size: usize) {
    let text: Vec<char> = sentences.join(" ").chars().collect();
    let mut a = SimpleTextAggregator::new(AggregationType::Sentence, Some(language));
    let mut actual = vec![];
    for chunk in text.chunks(chunk_size) {
        actual.extend(
            a.aggregate(&chunk.iter().collect::<String>())
                .into_iter()
                .map(|a| a.text),
        );
    }
    if let Some(tail) = a.flush() {
        actual.push(tail.text);
    }
    assert_eq!(
        actual, sentences,
        "language {language}, chunk size {chunk_size}"
    );
}
#[test]
fn chunk_boundaries_preserve_language_abbreviations_and_retry() {
    let cases: &[(&str, &[&str])] = &[
        ("en", &["Dr. Smith is here.", "It costs $29.95."]),
        ("de-DE", &["Das ist z.B. wichtig.", "Weiter geht es."]),
        ("pt-BR", &["O Sr. Silva chegou.", "Depois saiu."]),
        (
            "fr",
            &["Voir p. 12 pour les détails.", "Ensuite continuez."],
        ),
        ("pl", &["Prof. Kowalski przyszedł.", "Potem wyszedł."]),
        ("it", &["Il dott. Rossi arriva.", "Poi parte."]),
        ("nl", &["Dr. Jansen komt.", "Daarna vertrekt hij."]),
        ("ja", &["こんにちは。", "次の文です。"]),
        ("ja", &["Dr. 田中が来ます。", "次の文です。"]),
        ("ja", &["U.S.のAWSです。", "次の文です。"]),
        (
            "en",
            &["We make a good team, you and I.", "Did you see him?"],
        ),
        ("en", &["Albert I. Jones is here.", "Hello."]),
        ("en", &["Albert I. Douglas is here.", "Hello."]),
        ("en", &["He said \"Hello.\"", "Then he left."]),
        ("en", &["This is the U.S. Department of State.", "Next."]),
        ("en", &["Hello!?", "Next."]),
        ("en", &["We make a good team, you and I.", "Did?"]),
        (
            "en",
            &["She said, \"Dr. Smith is here.\"", "Then she left."],
        ),
        ("en", &["She said, “Dr. Smith is here.”", "Then she left."]),
        ("en", &["She said, \"Hello.", "Next sentence.\""]),
        ("en", &["Don't call Dr. Smith.", "He's busy."]),
        ("en", &["The doctor's here.", "Let's go."]),
    ];
    for (language, sentences) in cases {
        for chunk in [1, 3, 1000] {
            stream(language, sentences, chunk);
        }
    }
}
#[test]
fn open_quotes_preserve_abbreviations() {
    for quote in ['"', '\'', '“', '‘', '«', '「'] {
        let first = format!("She said, {quote}Dr. Smith is here.");
        for chunk in [1, 3, 1000] {
            stream("en", &[&first, "Next sentence"], chunk);
        }
        assert_eq!(
            match_endofsentence(&format!("She said, {quote}Dr. S"), None),
            0
        );
    }
}
#[test]
fn retry_waits_until_full_word_and_fast_path_emits_immediately() {
    let mut a = SimpleTextAggregator::default();
    assert!(a.aggregate("We make a good team, you and I. D").is_empty());
    assert!(a.aggregate("id").is_empty());
    assert_eq!(a.aggregate(" ")[0].text, "We make a good team, you and I.");
    assert_eq!(a.text().text, "Did");
    a.reset();
    assert!(a.aggregate("Albert I. Do").is_empty());
    assert!(a.aggregate("uglas ").is_empty());
    assert_eq!(a.flush().unwrap().text, "Albert I. Douglas");
    assert!(a.aggregate("Hello. ").is_empty());
    assert_eq!(a.aggregate("N")[0].text, "Hello.");
}
#[test]
fn pending_retry_clears_between_generations() {
    for reset_kind in 0..3 {
        let mut a = SimpleTextAggregator::default();
        assert!(a
            .aggregate("We make a good team, you and I. Did")
            .is_empty());
        match reset_kind {
            0 => a.reset(),
            1 => a.handle_interruption(),
            _ => {
                a.flush();
            }
        }
        assert_eq!(
            a.aggregate("Dr. Smith is here. Next")[0].text,
            "Dr. Smith is here."
        );
        assert_eq!(a.flush().unwrap().text, "Next");
    }
}
#[test]
fn language_updates_preserve_buffers_and_aggregators_are_independent() {
    let mut german = SimpleTextAggregator::new(AggregationType::Sentence, Some("de"));
    let mut english = SimpleTextAggregator::default();
    assert!(german.aggregate("Das ist bzw. w").is_empty());
    assert_eq!(english.aggregate("Das ist bzw. w")[0].text, "Das ist bzw.");
    assert_eq!(
        german.aggregate("ichtig. Weiter")[0].text,
        "Das ist bzw. wichtig."
    );
    german.reset();
    german.aggregate("Das ist bzw.");
    german.set_language(Some("en"));
    assert_eq!(german.text().text, "Das ist bzw.");
    assert_eq!(german.aggregate(" wichtig.")[0].text, "Das ist bzw.");
}
#[test]
fn source_word_mode_is_sentence_mode_and_token_preserves_spaces() {
    let mut a = SimpleTextAggregator::new(AggregationType::Word, None);
    assert!(a.aggregate("Hello ").is_empty());
    assert_eq!(a.aggregate("world. Next")[0].kind, "sentence");
    let mut tokens = SimpleTextAggregator::new(AggregationType::Token, None);
    assert_eq!(
        tokens.aggregate(" hi ")[0],
        Aggregation::new(" hi ", AggregationType::Token)
    );
    assert!(tokens.flush().is_none());
}
