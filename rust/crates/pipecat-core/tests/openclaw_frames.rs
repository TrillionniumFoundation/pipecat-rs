use pipecat_core::*;
#[test]
fn openclaw_priority_names_and_serde_are_explicit() {
    let samples = vec![
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "verbatim".into(),
            session_key: Some("session".into()),
        }),
        FrameData::OpenClawSteer(OpenClawSteerFrame {
            message: "steer".into(),
        }),
        FrameData::OpenClawAbort(OpenClawAbortFrame {
            reason: Some("reason".into()),
        }),
        FrameData::OpenClawStarted(OpenClawStartedFrame {
            run_id: "run".into(),
        }),
        FrameData::OpenClawText(OpenClawTextFrame {
            run_id: "run".into(),
            text: "delta".into(),
        }),
        FrameData::OpenClawEnd(OpenClawEndFrame {
            run_id: "run".into(),
            status: OpenClawRunStatus::Failed,
            text: String::new(),
            remote_outcome: OpenClawRemoteOutcome::Unknown,
        }),
    ];
    for (data, name) in samples
        .into_iter()
        .zip(["Send", "Steer", "Abort", "Started", "Text", "End"])
    {
        assert_eq!(data.source_class_name(), format!("OpenClaw{name}Frame"));
        assert_eq!(
            data.priority(),
            if name == "Abort" {
                FramePriority::System
            } else {
                FramePriority::Normal
            }
        );
        assert!(data.default_interruptible());
        let encoded = serde_json::to_vec(&data).unwrap();
        let decoded: FrameData = serde_json::from_slice(&encoded).unwrap();
        assert_eq!(decoded, data);
        assert!(!Frame::new(data).is_terminal());
    }
}
