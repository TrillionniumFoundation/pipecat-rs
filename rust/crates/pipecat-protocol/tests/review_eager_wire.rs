//! Independent manual wire boundaries must not silently erase local semantics.
use pipecat_core::*;
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    genesys::{AudioHookConfig, AudioHookSerializer},
    telephony::TelephonyCodec,
};
#[test]
fn review_all_manual_serializers_reject_supported_and_ignored_capability_carriers() {
    let mut serializers: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("review").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("review", AudioHookConfig::default()).unwrap()),
    ];
    let owner = SpeculationOwner::new();
    for serializer in &mut serializers {
        for data in [
            FrameData::UserStoppedSpeaking,
            FrameData::Interruption,
            FrameData::OutputAudio(AudioRawFrame::new(vec![0, 0], 8000, 1).unwrap()),
            FrameData::FunctionCallsStarted(vec![]),
            FrameData::Text(TextFrame::new("test")),
        ] {
            let mut frame = Frame::new(data);
            frame.set_native_speculation(owner.capability()).unwrap();
            assert!(
                serializer.serialize(&frame).is_err(),
                "manual encoder dropped a capability for {}",
                frame.name
            );
            assert!(serde_json::to_value(&frame).is_err());
            let mut clone = frame.duplicate();
            assert_eq!(clone.native_speculation(), frame.native_speculation());
            clone.take_native_speculation();
            assert!(clone.require_no_native_llm_continuation().is_ok());
            assert!(
                frame.native_speculation().is_some(),
                "consumption must remain clone-local"
            );
        }
    }
}
