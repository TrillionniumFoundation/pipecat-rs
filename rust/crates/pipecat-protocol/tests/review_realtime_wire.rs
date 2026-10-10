//! Actual manual wire serializers must not erase a realtime cause on supported or ignored payloads.
use pipecat_core::*;
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    genesys::{AudioHookConfig, AudioHookSerializer},
    telephony::TelephonyCodec,
};
#[test]
fn review_manual_protocol_serializers_reject_valid_and_tampered_context_causes() {
    let mut serializers: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("review").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("review", AudioHookConfig::default()).unwrap()),
    ];
    let mut context = Frame::new(FrameData::LlmContext(Default::default()));
    NativeRealtimeContextProducer::default()
        .attach(&mut context, RealtimeContextIntent::Run, true)
        .unwrap();
    for serializer in &mut serializers {
        for replacement in [
            None,
            Some(FrameData::Text(TextFrame::new("wire text"))),
            Some(FrameData::OutputAudio(
                AudioRawFrame::new(vec![0, 0], 8000, 1).unwrap(),
            )),
            Some(FrameData::Interruption),
        ] {
            let mut frame = context.clone();
            if let Some(data) = replacement {
                frame.data = data;
            }
            assert!(
                serializer.serialize(&frame).is_err(),
                "manual serializer silently erased local intent on {}",
                frame.name
            );
        }
        let mut consumed = context.clone();
        consumed.take_native_realtime_context_cause().unwrap();
        assert!(
            serializer.serialize(&consumed).is_ok(),
            "consumed context should preserve normal ignored-frame behavior"
        );
        assert!(context.native_realtime_context_cause().unwrap().is_some());
    }
}
