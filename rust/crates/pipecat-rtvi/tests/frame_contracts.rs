use pipecat_core::*;
#[test]
fn all_nine_framework_frames_match_source_ast_and_roundtrip_without_code() {
    let fixture: serde_json::Value =
        serde_json::from_str(include_str!("fixtures/frames.json")).unwrap();
    let classes = fixture["classes"].as_array().unwrap();
    let examples = vec![
        FrameData::RtviClientMessage(Default::default()),
        FrameData::RtviServerMessage(serde_json::Value::Null),
        FrameData::RtviServerResponse(Default::default()),
        FrameData::RtviConfigureObserver(Default::default()),
        FrameData::RtviUiCommand(Default::default()),
        FrameData::RtviUiJobGroup(None),
        FrameData::RtviUiEvent(Default::default()),
        FrameData::RtviUiSnapshot(Default::default()),
        FrameData::RtviUiCancelJobGroup(Default::default()),
    ];
    assert_eq!(examples.len(), classes.len());
    assert_eq!(RTVI_FRAME_CONTRACTS.len(), classes.len());
    for data in examples {
        let name = data.source_class_name();
        let source = classes.iter().find(|c| c["name"] == name).unwrap();
        assert_eq!(source["priority"], "system");
        assert_eq!(data.priority(), FramePriority::System);
        assert!(data.default_interruptible());
        assert!(RTVI_FRAME_CONTRACTS.iter().any(|c| c.source_class == name));
        let frame = Frame::new(data);
        let encoded = serde_json::to_vec(&frame).unwrap();
        let decoded: Frame = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(frame, decoded);
    }
}
