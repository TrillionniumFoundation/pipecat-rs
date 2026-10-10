use pipecat_core::{AudioRawFrame, Frame, FrameData, KeypadEntry};
use pipecat_protocol::{
    adapters::{FrameSerializer, RtviClientFrameSerializer, TelephonyFrameSerializer},
    telephony::TelephonyCodec,
    WireMessage,
};
use serde_json::json;
fn output(pcm: Vec<u8>, rate: u32) -> Frame {
    Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(pcm, rate, 1).unwrap(),
    ))
}
#[test]
fn telephony_frame_roundtrip_at_wire_rate() {
    let mut codec =
        TelephonyFrameSerializer::new(TelephonyCodec::twilio("s").unwrap(), 8000).unwrap();
    let wire = codec
        .serialize(&output(vec![0, 0, 232, 3], 8000))
        .unwrap()
        .unwrap();
    let FrameData::InputAudio(audio) = codec.deserialize(&wire).unwrap().unwrap().data else {
        panic!()
    };
    assert_eq!(audio.audio, vec![0, 0, 220, 3]);
    assert_eq!(audio.sample_rate, 8000);
    assert_eq!(audio.num_channels, 1);
}
#[test]
fn telephony_input_and_output_resampling_flush_exact_frame_counts() {
    let mut codec =
        TelephonyFrameSerializer::new(TelephonyCodec::vonage(8000).unwrap(), 16000).unwrap();
    let pcm = pipecat_audio::encode_pcm16_le(&vec![1000; 1600]);
    let wire = codec.serialize(&output(pcm, 16000)).unwrap().unwrap();
    let tail = codec.flush_output().unwrap().unwrap();
    let (WireMessage::Binary(mut bytes), WireMessage::Binary(tail)) = (wire, tail) else {
        panic!()
    };
    bytes.extend(tail);
    assert_eq!(bytes.len(), 800 * 2);
    let frame = codec
        .deserialize(&WireMessage::Binary(bytes))
        .unwrap()
        .unwrap();
    let FrameData::InputAudio(audio) = frame.data else {
        panic!()
    };
    let FrameData::InputAudio(tail) = codec.flush_input().unwrap().unwrap().data else {
        panic!()
    };
    assert_eq!(audio.sample_rate, 16000);
    assert_eq!(audio.audio.len() + tail.audio.len(), 1600 * 2);
}
#[test]
fn interruption_discards_resampler_tail_and_sends_clear() {
    let mut codec =
        TelephonyFrameSerializer::new(TelephonyCodec::twilio("s").unwrap(), 8000).unwrap();
    codec.serialize(&output(vec![0; 400], 16000)).unwrap();
    let wire = codec.serialize(&Frame::interruption()).unwrap().unwrap();
    let WireMessage::Text(text) = wire else {
        panic!()
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&text).unwrap(),
        json!({"event":"clear","streamSid":"s"})
    );
    assert!(codec.flush_output().unwrap().is_none());
}
#[test]
fn telephony_errors_drop_on_compatibility_path_but_report_on_strict_path() {
    let mut codec =
        TelephonyFrameSerializer::new(TelephonyCodec::twilio("s").unwrap(), 8000).unwrap();
    let bad = WireMessage::Text("bad".into());
    assert!(codec.deserialize(&bad).unwrap().is_none());
    assert!(codec.try_deserialize(&bad).is_err());
    let digit = WireMessage::Text(json!({"event":"dtmf","dtmf":{"digit":"#"}}).to_string());
    assert!(matches!(
        codec.deserialize(&digit).unwrap().unwrap().data,
        FrameData::InputDtmf(KeypadEntry::Pound)
    ));
}
#[test]
fn rtvi_original_client_tests_map_lifecycle_and_defaults() {
    let mut codec = RtviClientFrameSerializer::default();
    for (kind, expected) in [
        ("bot-llm-started", FrameData::LlmResponseStart),
        ("bot-llm-stopped", FrameData::LlmResponseEnd),
        ("bot-started-speaking", FrameData::BotStartedSpeaking),
        ("bot-stopped-speaking", FrameData::BotStoppedSpeaking),
        ("bot-interrupted", FrameData::Interruption),
    ] {
        let msg = WireMessage::Text(json!({"label":"rtvi-ai","type":kind,"data":null}).to_string());
        assert_eq!(codec.deserialize(&msg).unwrap().unwrap().data, expected);
    }
    let msg = WireMessage::Text(
        json!({"label":"rtvi-ai","type":"user-transcription","data":{"text":"hel","final":false}})
            .to_string(),
    );
    let FrameData::InterimTranscription(frame) = codec.deserialize(&msg).unwrap().unwrap().data
    else {
        panic!()
    };
    assert_eq!(frame.text, "hel");
    assert_eq!(frame.user_id, "");
}
#[test]
fn rtvi_audio_and_custom_passthrough_keep_original_mapping() {
    let mut codec = RtviClientFrameSerializer::default();
    let WireMessage::Text(audio) = codec
        .serialize(&output(vec![1, 2, 3, 4], 16000))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&audio).unwrap()["data"]["base64Audio"],
        "AQIDBA=="
    );
    assert!(codec
        .serialize(&Frame::new(FrameData::OutputTransportMessage(
            json!({"foo":"bar"})
        )))
        .unwrap()
        .is_none());
    let custom = json!({"label":"rtvi-ai","type":"eval-future","id":"9","data":{"x":1}});
    let WireMessage::Text(out) = codec
        .serialize(&Frame::new(FrameData::OutputTransportMessage(
            custom.clone(),
        )))
        .unwrap()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(&out).unwrap(),
        custom
    );
}
#[test]
fn rtvi_function_cancel_and_result_remain_distinct() {
    let mut codec = RtviClientFrameSerializer::default();
    for cancelled in [true, false] {
        let msg=WireMessage::Text(json!({"label":"rtvi-ai","type":"llm-function-call-stopped","data":{"tool_call_id":"c","cancelled":cancelled,"result":7}}).to_string());
        let frame = codec.deserialize(&msg).unwrap().unwrap();
        if cancelled {
            assert!(matches!(frame.data, FrameData::FunctionCallCancel(_)));
        } else {
            assert!(matches!(frame.data,FrameData::FunctionCallResult(data) if data.result==7));
        }
    }
}
#[test]
fn core_audio_and_malformed_payloads_never_panic() {
    let mut codec = RtviClientFrameSerializer::default();
    for text in [
        "bad",
        "[]",
        r#"{"label":"rtvi-ai","type":"bot-llm-text","data":[]}"#,
        r#"{"type":"bot-llm-text"}"#,
    ] {
        assert!(codec
            .deserialize(&WireMessage::Text(text.into()))
            .unwrap()
            .is_none());
    }
}
#[test]
fn rtvi_llm_marker_full_and_missing_fields() {
    let mut codec = RtviClientFrameSerializer::default();
    let msg=WireMessage::Text(json!({"label":"rtvi-ai","type":"bot-llm-marker","data":{"text":"◐","kind":"short","raw":"◐ wait","markers":["●","◐","○"]}}).to_string());
    let FrameData::LlmMarkerResponse(frame) = codec.deserialize(&msg).unwrap().unwrap().data else {
        panic!()
    };
    assert_eq!(frame.marker.as_deref(), Some("◐"));
    assert_eq!(frame.kind.as_deref(), Some("short"));
    assert_eq!(frame.raw, "◐ wait");
    assert_eq!(frame.markers, ["●", "◐", "○"]);
    let msg = WireMessage::Text(
        json!({"label":"rtvi-ai","type":"bot-llm-marker","data":{"text":"","raw":"Hi"}})
            .to_string(),
    );
    let FrameData::LlmMarkerResponse(frame) = codec.deserialize(&msg).unwrap().unwrap().data else {
        panic!()
    };
    assert_eq!(frame.marker, None);
    assert_eq!(frame.kind, None);
}

#[test]
fn every_frame_serializer_rejects_unconsumed_native_history_before_dispatch() {
    use pipecat_core::{LlmContinuationBlock, LlmContinuationProvider, NativeLlmContinuation};
    use pipecat_protocol::genesys::{AudioHookConfig, AudioHookSerializer};
    use std::sync::Arc;
    let mut serializers: Vec<Box<dyn FrameSerializer>> = vec![
        Box::new(
            TelephonyFrameSerializer::new(TelephonyCodec::twilio("s").unwrap(), 8000).unwrap(),
        ),
        Box::new(RtviClientFrameSerializer::default()),
        Box::new(AudioHookSerializer::new("s", AudioHookConfig::default()).unwrap()),
    ];
    for serializer in &mut serializers {
        let mut frame = Frame::new(FrameData::LlmResponseEnd);
        let history = NativeLlmContinuation::new(
            1,
            LlmContinuationProvider::Anthropic,
            vec![LlmContinuationBlock::Thinking {
                text: "private".into(),
                signature: "secret signature".into(),
            }],
        )
        .unwrap();
        frame
            .set_native_llm_continuation(Arc::new(history))
            .unwrap();
        let error = serializer.serialize(&frame).unwrap_err().to_string();
        assert!(error.contains("native LLM continuation"));
        assert!(!error.contains("secret signature"));
        // Payload mutation cannot make semantic state disappear at dispatch.
        frame.data = FrameData::Interruption;
        assert!(serializer.serialize(&frame).is_err());
        frame.data = FrameData::LlmResponseEnd;
        frame.take_native_llm_continuation().unwrap();
        assert!(serializer.serialize(&frame).unwrap().is_none());
    }
}
