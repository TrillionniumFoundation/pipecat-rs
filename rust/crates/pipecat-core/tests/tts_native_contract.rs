use pipecat_core::*;
#[test]
fn native_tts_contracts_are_interruptible_normal_and_not_source_catalog_claims() {
    for data in [
        FrameData::TtsTextInput(TtsTextInputFrame::new("hi")),
        FrameData::TtsAlignmentDiagnostic(TtsAlignmentDiagnosticFrame {
            context_id: "ctx".into(),
            segment_id: None,
            evidence: TtsAlignmentEvidence::Unmatched,
            synthesis_text: "hi".into(),
            original_text: None,
            source_span: None,
            display_position: None,
            synthesis_outcome: TtsSynthesisOutcome::Pending,
        }),
    ] {
        assert_eq!(data.priority(), FramePriority::Normal);
        assert!(data.default_interruptible());
        assert!(data.source_class_name().starts_with("Native"));
        let frame = Frame::new(data);
        let roundtrip: Frame =
            serde_json::from_value(serde_json::to_value(&frame).unwrap()).unwrap();
        assert_eq!(roundtrip, frame);
    }
}
#[test]
fn pipeline_pts_marker_survives_clone_duplicate_and_refuses_wire_until_checked_rebase() {
    let mut frame = Frame::text("timed");
    frame.set_pipeline_pts(0);
    assert!(frame.has_pipeline_pts());
    assert!(frame.clone().has_pipeline_pts());
    assert!(frame.duplicate().has_pipeline_pts());
    assert!(serde_json::to_value(&frame).is_err());
    assert_eq!(
        frame.rebase_pipeline_pts(1),
        Err(FrameTimingError::BeforeOrigin)
    );
    assert!(frame.has_pipeline_pts());
    frame.rebase_pipeline_pts(0).unwrap();
    assert_eq!(frame.pts, Some(0));
    assert!(!frame.has_pipeline_pts());
    let decoded: Frame = serde_json::from_value(serde_json::to_value(&frame).unwrap()).unwrap();
    assert_eq!(decoded, frame);
    frame.set_pipeline_pts(10);
    frame.set_relative_pts(Some(3));
    assert!(frame.require_wire_pts().is_ok());
}
