use pipecat_core::*;
use pipecat_network_bus::*;
use pipecat_workers::{BusMessage, MessageData};
use serde_json::json;
#[test]
fn openclaw_source_module_is_explicitly_unsupported() {
    let codec = PythonJsonCodec::default();
    let frame = Frame::new(FrameData::OpenClawSend(OpenClawSendFrame {
        message: "secret-payload".into(),
        session_key: None,
    }));
    let message = BusMessage {
        source: "source".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(frame),
            direction: Direction::Downstream,
            bridge: None,
        },
    };
    let error = codec.from_bus_message(&message).unwrap_err();
    assert!(matches!(error, CodecError::UnsupportedFrame(_)));
    assert!(!error.to_string().contains("secret-payload"));
    for module in ["pipecat.services.openclaw.frames", "pipecat.frames.frames"] {
        let wire = json!({"__type__":"pipecat.bus.messages.BusFrameMessage","__data__":{"source":"source","frame":{"__type__":format!("{module}.OpenClawSendFrame"),"__data__":{"message":"secret-payload"}},"direction":{"__type__":"pipecat.processors.frame_processor.FrameDirection","__data__":"DOWNSTREAM"}}});
        assert!(codec.decode_value(wire).is_err());
    }
}
