use pipecat_hepta::speech_segments;

#[test]
fn chinese_multi_item_response_preserves_text_at_utf8_boundaries() {
    let text = format!(
        "{}。\n{}！",
        "中文回复".repeat(300),
        "第二条输出".repeat(400)
    );
    let segments = speech_segments(&text).unwrap();
    assert!(segments.len() > 1);
    assert_eq!(segments.concat(), text);
    assert!(segments
        .iter()
        .all(|part| !part.is_empty() && part.len() <= 4096));
    assert!(segments
        .iter()
        .take(segments.len() - 1)
        .any(|part| part.ends_with('。') || part.ends_with('\n')));
}

#[test]
fn no_boundary_response_is_bounded_and_validated_before_rendering() {
    let text = "界".repeat(5461);
    let segments = speech_segments(&text).unwrap();
    assert_eq!(segments.concat(), text);
    assert_eq!(segments.len(), 5);
    for invalid in [
        " ".into(),
        "x".repeat(16385),
        "valid prefix\0invalid suffix".into(),
        format!("{}[[unsupported]]", "valid prefix".repeat(400)),
    ] {
        assert!(speech_segments(&invalid).is_err());
    }
}
