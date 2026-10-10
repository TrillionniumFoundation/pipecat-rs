use pipecat_core::*;
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    genesys::{AudioHookConfig, AudioHookSerializer},
    telephony::TelephonyCodec,
};
#[test]
fn gateway_frames_are_rejected_by_unrelated_wire_codecs() {
    let mut codecs: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("stream").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("stream", AudioHookConfig::default()).unwrap()),
    ];
    let frames = vec![
        FrameData::OpenClawSend(OpenClawSendFrame {
            message: "secret".into(),
            session_key: None,
        }),
        FrameData::OpenClawSteer(OpenClawSteerFrame {
            message: "secret".into(),
        }),
        FrameData::OpenClawAbort(OpenClawAbortFrame::default()),
        FrameData::OpenClawStarted(OpenClawStartedFrame {
            run_id: "run".into(),
        }),
        FrameData::OpenClawText(OpenClawTextFrame {
            run_id: "run".into(),
            text: "secret".into(),
        }),
        FrameData::OpenClawEnd(OpenClawEndFrame {
            run_id: "run".into(),
            text: "secret".into(),
            status: OpenClawRunStatus::Failed,
            remote_outcome: OpenClawRemoteOutcome::Unknown,
        }),
    ];
    for codec in &mut codecs {
        for data in &frames {
            let error = codec
                .serialize(&Frame::new(data.clone()))
                .unwrap_err()
                .to_string();
            assert!(error.contains("OpenClaw"));
            assert!(!error.contains("secret"));
        }
    }
}
