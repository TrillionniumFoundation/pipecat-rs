//! Independent source-contract fixtures. No upstream SDK is executed.
use pipecat_speechmatics::*;
use serde_json::json;

#[test]
fn review_default_start_shape_is_agent_contract_not_realtime_or_flux() {
    let settings = SpeechmaticsSttSettings::default();
    assert_eq!(
        settings.start_message().unwrap(),
        json!({
            "message": "StartRecognition",
            "audio_format": {"type":"raw","encoding":"pcm_s16le","sample_rate":16000},
            "transcription_config": {"language":"en","model":"linden-1"},
            "turn_config": {"turn_detection_mode":"external"}
        })
    );
}

#[test]
fn review_locale_diarization_false_zero_and_nested_vocabulary_are_preserved() {
    let settings = SpeechmaticsSttSettings::from_value(json!({
        "language":"en-US", "enable_partials":false, "enable_diarization":true,
        "speaker_sensitivity":0.0,"prefer_current_speaker":false,
        "known_speakers":[{"label":"Guide","speaker_identifiers":["opaque-fixture"]}],
        "additional_vocab":[{"content":"Pipecat","sounds_like":["pie pe cat"]}],
        "punctuation_overrides":{"permitted_marks":["?"],"sensitivity":0.0},
        "turn_detection_mode":"vad"
    }))
    .unwrap();
    let wire = settings.start_message().unwrap();
    assert_eq!(
        wire["transcription_config"],
        json!({
            "language":"en", "output_locale":"en-US", "model":"linden-1",
            "enable_partials":false,"diarization":"speaker",
            "speaker_diarization_config":{
                "speaker_sensitivity":0.0,"prefer_current_speaker":false,
                "speakers":[{"label":"Guide","speaker_identifiers":["opaque-fixture"]}]
            },
            "additional_vocab":[{"content":"Pipecat","sounds_like":["pie pe cat"]}],
            "punctuation_overrides":{"permitted_marks":["?"],"sensitivity":0.0}
        })
    );
    assert_eq!(wire["turn_config"], json!({"turn_detection_mode":"vad"}));
    assert_eq!(settings.language, "en-US");
    assert!(!format!("{settings:?}").contains("opaque-fixture"));
    for (language, expected) in [
        ("fil-PH", "tl"),
        ("cmn", "cmn"),
        ("yue", "yue"),
        ("en-NZ", "en"),
    ] {
        let s = SpeechmaticsSttSettings::from_value(json!({"language":language})).unwrap();
        assert_eq!(s.wire_language().unwrap(), expected);
        assert!(s.start_message().unwrap()["transcription_config"]
            .get("output_locale")
            .is_none());
    }
}

#[test]
fn review_alias_and_null_merge_validate_whole_candidate_without_touching_original() {
    let original = SpeechmaticsSttSettings::from_value(
        json!({"model":"model-old","domain":"finance","enable_partials":true}),
    )
    .unwrap();
    let changed = original
        .merge(
            json!({"operating_point":"model-new","domain":null,"enable_partials":null})
                .as_object()
                .unwrap(),
        )
        .unwrap();
    assert_eq!(changed.model, "model-new");
    assert_eq!(changed.domain, None);
    assert_eq!(changed.enable_partials, None);
    assert_eq!(original.model, "model-old");
    assert_eq!(original.domain.as_deref(), Some("finance"));
    for delta in [
        json!({"model":"other","operating_point":"conflict"}),
        json!({"model":"other","speaker_active_format":"{unknown}"}),
        json!({"domain":null,"speaker_sensitivity":-0.01}),
        json!({"enable_partials":false,"unrecognized":true}),
        json!({"language":"not-a-supported-language"}),
    ] {
        assert!(original.merge(delta.as_object().unwrap()).is_err());
    }
    assert_eq!(original.enable_partials, Some(true));
}

#[test]
fn review_template_expansion_is_bounded_and_does_not_reparse_inserted_text() {
    let settings = SpeechmaticsSttSettings::from_value(
        json!({"language":"en-US","speaker_active_format":"{{{speaker_id}}} {text} [{lang}] {ts}"}),
    )
    .unwrap();
    let rendered = settings
        .format_text(
            "Unicode 世界 {speaker_id}",
            None,
            "2026-10-10T00:00:00Z",
            1024,
        )
        .unwrap();
    assert_eq!(
        rendered,
        "{UU} Unicode 世界 {speaker_id} [en-US] 2026-10-10T00:00:00Z"
    );
    assert!(settings
        .format_text(&"x".repeat(1024), None, "now", 1024)
        .is_err());
    for template in [
        "{",
        "}",
        "{text!r}",
        "{text:.2}",
        "{text.__class__}",
        "{speaker}",
    ] {
        assert!(
            SpeechmaticsSttSettings::from_value(json!({"speaker_active_format":template})).is_err()
        );
    }
}
