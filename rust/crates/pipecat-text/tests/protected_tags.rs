use pipecat_text::{AggregationType, SkipTagsAggregator, TextAggregator};

#[test]
fn closed_spell_spans_cannot_be_split_by_later_sentence_lookahead() {
    let mut aggregator = SkipTagsAggregator::new(
        vec![("<spell>".into(), "</spell>".into())],
        AggregationType::Sentence,
        Some("en"),
    );
    let mut text = Vec::new();
    for chunk in [
        "Call <sp",
        "ell>A.",
        "B.</spell> now.",
        " N",
        "ext sentence.",
    ] {
        text.extend(aggregator.aggregate(chunk).into_iter().map(|a| a.text));
    }
    text.extend(aggregator.flush().map(|a| a.text));
    assert_eq!(text, ["Call <spell>A.B.</spell> now.", "Next sentence."]);
}

#[test]
fn protected_spans_survive_unicode_and_tag_scan_rebases_after_each_sentence() {
    let source = "Say <spell>你。好！</spell> now. Next <spell>C.D.</spell> please. Last.";
    for width in [1, 2, 5, source.chars().count()] {
        let mut aggregator = SkipTagsAggregator::new(
            vec![("<spell>".into(), "</spell>".into())],
            AggregationType::Sentence,
            Some("en"),
        );
        let mut text = Vec::new();
        for chars in source.chars().collect::<Vec<_>>().chunks(width) {
            text.extend(
                aggregator
                    .aggregate(&chars.iter().collect::<String>())
                    .into_iter()
                    .map(|a| a.text),
            );
        }
        text.extend(aggregator.flush().map(|a| a.text));
        assert_eq!(
            text,
            [
                "Say <spell>你。好！</spell> now.",
                "Next <spell>C.D.</spell> please.",
                "Last."
            ],
            "chunk width {width}"
        );
    }
}
