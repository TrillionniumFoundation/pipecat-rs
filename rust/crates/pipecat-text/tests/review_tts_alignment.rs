//! Reviewer-owned regressions for context-local TTS reconstruction.
use pipecat_text::*;

#[test]
fn flushing_a_token_tail_keeps_its_own_aggregation_name() {
    let mut bridge = TtsAlignment::new(
        7,
        true,
        TokenReporting::WordTimestamps,
        None,
        TtsAlignmentLimits::default(),
    )
    .unwrap();
    bridge
        .register_named(&TtsTextChannels::identical("first"), false, "sentence")
        .unwrap();
    bridge.start_audio(0, 0).unwrap();
    let events = bridge
        .register_named(&TtsTextChannels::identical("next"), true, "custom")
        .unwrap();
    let anchors: Vec<_> = events
        .iter()
        .filter_map(|event| match &event.event {
            TokenSequencerEvent::Promoted { sentence, .. } => Some((
                sentence.user_facing_text.as_str(),
                event.aggregated_by.as_str(),
            )),
            _ => None,
        })
        .collect();
    assert_eq!(anchors, [("first", "sentence"), ("next", "custom")]);
}

#[test]
fn final_token_tail_preserves_explicit_aggregation_name() {
    let mut bridge = TtsAlignment::new(
        7,
        true,
        TokenReporting::WordTimestamps,
        None,
        TtsAlignmentLimits::default(),
    )
    .unwrap();
    bridge
        .register_named(&TtsTextChannels::identical("first"), false, "custom")
        .unwrap();
    bridge.start_audio(0, 0).unwrap();
    let events = bridge.finalize().unwrap();
    assert_eq!(events[0].aggregated_by, "custom");
}
