//! Every manual encoder checks private eager capabilities before kind dispatch.
use pipecat_core::*;
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    genesys::{AudioHookConfig, AudioHookSerializer},
    telephony::TelephonyCodec,
};
#[test]
fn manual_codecs_never_strip_a_live_or_cancelled_eager_capability() {
    let mut codecs: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("stream").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("stream", AudioHookConfig::default()).unwrap()),
    ];
    for state in 0..3 {
        let owner = SpeculationOwner::new();
        let capability = owner.capability();
        if state == 1 {
            owner.confirm();
            capability.release_after_boundary();
        }
        if state == 2 {
            owner.cancel();
        }
        let mut frame = Frame::new(FrameData::Interruption);
        frame.set_native_speculation(capability).unwrap();
        for codec in &mut codecs {
            assert!(codec
                .serialize(&frame)
                .unwrap_err()
                .to_string()
                .contains("native"));
            assert!(codec.serialize(&frame.duplicate()).is_err());
        }
    }
}
