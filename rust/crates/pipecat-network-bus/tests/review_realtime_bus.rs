use pipecat_core::*;
use pipecat_network_bus::PythonJsonCodec;
use pipecat_workers::{BusMessage, MessageData};
#[test]
fn review_python_bus_rejects_context_cause_until_explicit_consumption() {
    let codec = PythonJsonCodec::default();
    let mut frame = Frame::new(FrameData::LlmContext(Default::default()));
    NativeRealtimeContextProducer::default()
        .attach(&mut frame, RealtimeContextIntent::Run, true)
        .unwrap();
    let envelope = |frame| BusMessage {
        source: "review".into(),
        target: None,
        data: MessageData::Frame {
            frame: Box::new(frame),
            direction: Direction::Downstream,
            bridge: None,
        },
    };
    assert!(codec.from_bus_message(&envelope(frame.clone())).is_err());
    let mut forged = frame.clone();
    forged.data = FrameData::Text(TextFrame::new("supported bus projection"));
    assert!(codec.from_bus_message(&envelope(forged)).is_err());
    frame.take_native_realtime_context_cause().unwrap();
    let admitted = codec.from_bus_message(&envelope(frame)).unwrap();
    let decoded = codec.decode(&codec.encode(&admitted).unwrap()).unwrap();
    let MessageData::Frame { frame, .. } = &decoded.as_bus_message().unwrap().data else {
        panic!("wrong bus projection")
    };
    assert!(frame.native_realtime_context_cause().unwrap().is_none());
}
