use pipecat_core::*;
use pipecat_network_bus::{CodecError, PythonJsonCodec};
use pipecat_workers::{BusMessage, MessageData};
#[test]
fn review_openclaw_manual_python_bus_is_explicitly_rejected_not_coerced() {
    let codec = PythonJsonCodec::default();
    let frames = [
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "secret prompt".into(),
            session_key: Some("secret session".into()),
        }),
        FrameData::OpenClawSteer(OpenClawSteerFrame {
            message: "secret steer".into(),
        }),
        FrameData::OpenClawAbort(OpenClawAbortFrame {
            reason: Some("secret abort".into()),
        }),
        FrameData::OpenClawStarted(OpenClawStartedFrame {
            run_id: "secret run".into(),
        }),
        FrameData::OpenClawText(OpenClawTextFrame {
            run_id: "secret run".into(),
            text: "secret prose".into(),
        }),
        FrameData::OpenClawEnd(OpenClawEndFrame {
            run_id: "secret run".into(),
            status: OpenClawRunStatus::Completed,
            text: "secret answer".into(),
            remote_outcome: OpenClawRemoteOutcome::Settled,
        }),
    ];
    for data in frames {
        let expected = data.source_class_name();
        let message = BusMessage {
            source: "review".into(),
            target: None,
            data: MessageData::Frame {
                frame: Box::new(Frame::new(data)),
                direction: Direction::Downstream,
                bridge: None,
            },
        };
        let error = codec.from_bus_message(&message).unwrap_err();
        assert!(matches!(&error,CodecError::UnsupportedFrame(name) if name==expected));
        assert!(!format!("{error:?} {error}").contains("secret"));
    }
}
