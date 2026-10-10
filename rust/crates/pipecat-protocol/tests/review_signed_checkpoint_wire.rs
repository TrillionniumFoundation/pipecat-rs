//! Native checkpoints must be refused before manual codec frame dispatch.
use pipecat_core::{
    Frame, FrameData, LlmContinuationBlock, LlmContinuationProvider, NativeLlmContinuation,
};
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    genesys::{AudioHookConfig, AudioHookSerializer},
    telephony::TelephonyCodec,
};
use std::sync::Arc;

fn checkpoint() -> Frame {
    let mut frame = Frame::new(FrameData::LlmContinuationCheckpoint);
    frame
        .set_native_llm_continuation(Arc::new(
            NativeLlmContinuation::checkpoint(
                901,
                LlmContinuationProvider::Anthropic,
                vec![LlmContinuationBlock::Thinking {
                    text: "private-thought".into(),
                    signature: "opaque-private-signature".into(),
                }],
            )
            .unwrap(),
        ))
        .unwrap();
    frame
}

#[test]
fn review_manual_codecs_refuse_attached_consumed_and_bare_native_checkpoints() {
    let mut codecs: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("stream").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("stream", AudioHookConfig::default()).unwrap()),
    ];
    let original = checkpoint();
    let mut consumed = checkpoint();
    consumed.take_native_llm_continuation().unwrap();
    let mut mutated = checkpoint();
    mutated.data = FrameData::Interruption;
    let frames = [
        original.clone(),
        original.duplicate(),
        consumed,
        mutated,
        Frame::new(FrameData::LlmContinuationCheckpoint),
    ];
    for codec in &mut codecs {
        for frame in &frames {
            let error = codec
                .serialize(frame)
                .expect_err("local checkpoint must not reach wire or be silently ignored")
                .to_string();
            assert!(error.contains("native LLM continuation"));
            assert!(!error.contains("opaque-private-signature"));
            assert!(!error.contains("private-thought"));
        }
        assert!(codec
            .serialize(&Frame::new(FrameData::LlmThoughtEnd))
            .unwrap()
            .is_none());
    }
}
