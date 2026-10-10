use pipecat_core::FrameData;
use pipecat_text::*;

fn setup() -> (TtsAlignment, TtsAlignmentFrames) {
    let bridge = TtsAlignment::new(
        0,
        true,
        TokenReporting::WordTimestamps,
        None,
        TtsAlignmentLimits::default(),
    )
    .unwrap();
    let mapper = TtsAlignmentFrames::new("spacing".into(), bridge.context(), true, 16).unwrap();
    (bridge, mapper)
}
#[test]
fn explicit_aggregate_and_skipped_spacing_survives_conversion() {
    for spaces in [false, true] {
        let (mut bridge, mut mapper) = setup();
        let events = bridge
            .register_prepared(
                &TtsTextChannels::identical("Hello"),
                true,
                "sentence",
                spaces,
            )
            .unwrap();
        mapper.convert(events).unwrap();
        let events = bridge
            .skipped_prepared("code", "```code```", Some("speaker"), "code", spaces)
            .unwrap();
        mapper.convert(events).unwrap();
        let events = bridge.start_audio(0, 0).unwrap();
        let mut frames = mapper.convert(events).unwrap();
        let events = bridge.word("Hello", 0.0, false).unwrap();
        frames.extend(mapper.convert(events).unwrap());
        let aggregates: Vec<_> = frames
            .iter()
            .filter_map(|frame| match &frame.data {
                FrameData::AggregatedText(text) => Some((frame, text)),
                _ => None,
            })
            .collect();
        assert_eq!(aggregates.len(), 2);
        assert!(aggregates
            .iter()
            .all(|(_, text)| text.text.includes_inter_frame_spaces == spaces));
        assert!(aggregates[0].1.will_be_spoken);
        assert!(!aggregates[1].1.will_be_spoken);
        assert_eq!(
            aggregates[1].0.transport_destination.as_deref(),
            Some("speaker")
        );
    }
}
#[test]
fn exact_source_partitions_preserve_internal_spacing_and_reset_at_segment_boundary() {
    let (mut bridge, mut mapper) = setup();
    for text in ["Hello  from\tRust!", "Next item"] {
        mapper
            .convert(
                bridge
                    .register(&TtsTextChannels::identical(text), true)
                    .unwrap(),
            )
            .unwrap();
    }
    let mut frames = mapper.convert(bridge.start_audio(0, 0).unwrap()).unwrap();
    for word in ["Hello", "from", "Rust!", "Next", "item"] {
        frames.extend(
            mapper
                .convert(bridge.word(word, 0.0, false).unwrap())
                .unwrap(),
        );
    }
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsText(t) => Some(t),
            _ => None,
        })
        .collect();
    assert_eq!(
        words
            .iter()
            .map(|t| t.text.includes_inter_frame_spaces)
            .collect::<Vec<_>>(),
        [false, true, true, false, true]
    );
    let mut original = String::new();
    for word in words {
        if !word.text.includes_inter_frame_spaces && !original.is_empty() {
            original.push(' ');
        }
        original.push_str(word.raw_text.as_deref().unwrap());
    }
    assert_eq!(original, "Hello  from\tRust! Next item");
}
#[test]
fn suppressed_transform_reports_do_not_consume_first_source_boundary() {
    let (mut bridge, mut mapper) = setup();
    let text = TtsTextChannels {
        synthesis: "five dollars".into(),
        display: "$5".into(),
        original: "<b>$5</b>".into(),
    };
    mapper
        .convert(bridge.register(&text, true).unwrap())
        .unwrap();
    let mut frames = mapper.convert(bridge.start_audio(0, 0).unwrap()).unwrap();
    for word in ["five", "dollars"] {
        frames.extend(
            mapper
                .convert(bridge.word(word, 0.0, false).unwrap())
                .unwrap(),
        );
    }
    let words: Vec<_> = frames
        .iter()
        .filter_map(|f| match &f.data {
            FrameData::TtsText(t) => Some(t),
            _ => None,
        })
        .collect();
    assert_eq!(words.len(), 2);
    assert!(!words[0].text.append_to_context);
    assert!(words[1].text.append_to_context);
    assert!(!words[1].text.includes_inter_frame_spaces);
    assert_eq!(words[1].raw_text.as_deref(), Some("<b>$5</b>"));
}
