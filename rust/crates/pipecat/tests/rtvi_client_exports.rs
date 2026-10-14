use pipecat::{
    core::FrameProcessor,
    transports::{rtvi_client::RtviClientStatus, RtviClientConfig, RtviClientProcessor},
};

#[test]
fn facade_exposes_explicit_client_without_connecting() {
    let processor = RtviClientProcessor::new(RtviClientConfig::new("ws://127.0.0.1:1/")).unwrap();
    assert_eq!(processor.name(), "RtviClientProcessor");
    let control = processor.control();
    assert_eq!(control.status(), RtviClientStatus::Idle);
    assert!(!control.is_ready());
    control.close();
    assert!(control.is_closed());
    assert!(RtviClientProcessor::new(RtviClientConfig::new("ws://localhost/")).is_err());
}
