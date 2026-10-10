use pipecat_core::*;
use serde_json::json;

#[test]
fn review_all_typed_openclaw_frames_preserve_payload_identity_priority_and_serde() {
    let cases = [
        (
            FrameData::OpenClawSend(OpenClawSendFrame {
                message: "verbatim\nagent prompt".into(),
                session_key: Some("session".into()),
            }),
            "OpenClawSendFrame",
            FramePriority::Normal,
        ),
        (
            FrameData::OpenClawSteer(OpenClawSteerFrame {
                message: "followup".into(),
            }),
            "OpenClawSteerFrame",
            FramePriority::Normal,
        ),
        (
            FrameData::OpenClawAbort(OpenClawAbortFrame {
                reason: Some("reason".into()),
            }),
            "OpenClawAbortFrame",
            FramePriority::System,
        ),
        (
            FrameData::OpenClawStarted(OpenClawStartedFrame {
                run_id: "remote-start".into(),
            }),
            "OpenClawStartedFrame",
            FramePriority::Normal,
        ),
        (
            FrameData::OpenClawText(OpenClawTextFrame {
                run_id: "replacement".into(),
                text: "prose".into(),
            }),
            "OpenClawTextFrame",
            FramePriority::Normal,
        ),
        (
            FrameData::OpenClawEnd(OpenClawEndFrame {
                run_id: "replacement".into(),
                status: OpenClawRunStatus::Failed,
                text: "bounded failure".into(),
                remote_outcome: OpenClawRemoteOutcome::Unknown,
            }),
            "OpenClawEndFrame",
            FramePriority::Normal,
        ),
    ];
    for (data, name, priority) in cases {
        assert_eq!(data.source_class_name(), name);
        let frame = Frame::new(data);
        assert_eq!(frame.priority(), priority);
        assert!(frame.interruptible);
        assert!(
            !frame.is_terminal(),
            "OpenClaw run terminal must not stop pipeline"
        );
        assert!(frame.name.starts_with(name));
        let serialized = serde_json::to_value(&frame).unwrap();
        let restored: Frame = serde_json::from_value(serialized).unwrap();
        assert_eq!(restored, frame);
        assert_eq!(restored.priority(), priority);
    }
    assert_eq!(
        serde_json::to_value(OpenClawRunStatus::Cancelled).unwrap(),
        json!("cancelled")
    );
    assert_eq!(
        serde_json::to_value(OpenClawRemoteOutcome::Settled).unwrap(),
        json!("settled")
    );
    assert!(serde_json::from_value::<OpenClawRunStatus>(json!("success")).is_err());
    let absent: OpenClawEndFrame =
        serde_json::from_value(json!({"run_id":"unverified","status":"completed","text":"answer"}))
            .unwrap();
    assert_eq!(absent.remote_outcome, OpenClawRemoteOutcome::Unknown);
}
