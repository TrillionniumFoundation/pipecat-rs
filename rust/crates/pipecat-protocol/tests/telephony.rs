use pipecat_protocol::telephony::*;
use pipecat_protocol::{DtmfDigit, ProtocolError, WireMessage};
use serde_json::{json, Value};
fn wire(value: Value) -> WireMessage {
    WireMessage::Text(value.to_string())
}
fn value(message: WireMessage) -> Value {
    match message {
        WireMessage::Text(text) => serde_json::from_str(&text).unwrap(),
        _ => panic!("expected JSON"),
    }
}
fn fixture(name: &str) -> WireMessage {
    let all: Value = serde_json::from_str(include_str!("fixtures/telephony.json")).unwrap();
    wire(all[name].clone())
}
fn codecs() -> Vec<TelephonyCodec> {
    vec![
        TelephonyCodec::twilio("stream123").unwrap(),
        TelephonyCodec::plivo("stream123").unwrap(),
        TelephonyCodec::telnyx("stream123", "PCMU", "PCMU").unwrap(),
        TelephonyCodec::exotel("stream123", 8000).unwrap(),
    ]
}

#[test]
fn golden_twilio_start_media_mark_stop() {
    let mut session = TelephonySession::new(TelephonyCodec::twilio("stream123").unwrap());
    let event = session.accept(&fixture("twilio_start")).unwrap();
    let EventKind::Start(start) = event.kind else {
        panic!()
    };
    assert_eq!(start.call_id.as_deref(), Some("call123"));
    assert_eq!(start.custom_parameters["user_id"], "test_user");
    assert_eq!(event.sequence, Some(1));
    let EventKind::Media {
        audio,
        track,
        chunk,
        timestamp_ms,
    } = session.accept(&fixture("twilio_media")).unwrap().kind
    else {
        panic!()
    };
    assert_eq!(audio.data, [255, 127, 0]);
    assert_eq!(audio.to_pcm16().unwrap(), [0, 0, 0, 0, 132, 130]);
    assert_eq!(track.as_deref(), Some("inbound"));
    assert_eq!(chunk, Some(1));
    assert_eq!(timestamp_ms, Some(20));
    assert_eq!(
        session.accept(&fixture("twilio_mark")).unwrap().kind,
        EventKind::Mark {
            name: "segment-1".into()
        }
    );
    assert!(matches!(
        session.accept(&fixture("twilio_stop")).unwrap().kind,
        EventKind::Stop { .. }
    ));
    assert_eq!(session.state(), StreamState::Stopped);
    assert!(session.accept(&fixture("twilio_media")).is_err());
}
#[test]
fn golden_plivo_numeric_sequence_and_checkpoint() {
    let codec = TelephonyCodec::plivo("stream123").unwrap();
    let mut session = TelephonySession::new(codec.clone());
    assert!(matches!(
        session.accept(&fixture("plivo_start")).unwrap().kind,
        EventKind::Start(_)
    ));
    let EventKind::Media {
        timestamp_ms,
        chunk,
        ..
    } = session.accept(&fixture("plivo_media")).unwrap().kind
    else {
        panic!()
    };
    assert_eq!(timestamp_ms, Some(1705312200000));
    assert_eq!(chunk, Some(1));
    assert_eq!(
        session.accept(&fixture("plivo_mark")).unwrap().kind,
        EventKind::Mark {
            name: "segment-1".into()
        }
    );
    assert_eq!(
        value(codec.mark("segment-1").unwrap()),
        json!({"event":"checkpoint","streamId":"stream123","name":"segment-1"})
    );
    assert_eq!(
        codec
            .parse(&wire(json!({"event":"clearedAudio"})))
            .unwrap()
            .kind,
        EventKind::Cleared
    );
}
#[test]
fn golden_telnyx_alaw_and_reordered_events() {
    let codec = TelephonyCodec::telnyx("stream123", "PCMA", "PCMU").unwrap();
    let mut session = TelephonySession::new(codec);
    session.accept(&fixture("telnyx_start")).unwrap();
    let EventKind::Media { audio, .. } = session.accept(&fixture("telnyx_media")).unwrap().kind
    else {
        panic!()
    };
    assert_eq!(audio.encoding, AudioEncoding::Pcma);
    assert_eq!(audio.data, [213, 85, 0]);
    assert_eq!(audio.to_pcm16().unwrap(), [8, 0, 248, 255, 128, 234]);
    session.accept(&wire(json!({"event":"dtmf","sequence_number":"3","stream_id":"stream123","dtmf":{"digit":"#"}}))).unwrap();
    session.accept(&fixture("telnyx_mark")).unwrap();
    session.accept(&fixture("telnyx_stop")).unwrap();
    assert_eq!(session.state(), StreamState::Stopped);
}
#[test]
fn golden_vonage_binary_pcm_and_control() {
    let codec = TelephonyCodec::vonage(16000).unwrap();
    let mut session = TelephonySession::new(codec.clone());
    session.accept(&fixture("vonage_connected")).unwrap();
    let audio = WireMessage::Binary(vec![0, 0, 1, 0, 255, 127]);
    let EventKind::Media { audio: parsed, .. } = session.accept(&audio).unwrap().kind else {
        panic!()
    };
    assert_eq!(parsed.to_pcm16().unwrap(), [0, 0, 1, 0, 255, 127]);
    assert_eq!(
        codec.encode_pcm16(&parsed.data, 16000).unwrap(),
        Some(audio)
    );
    assert_eq!(
        session.accept(&fixture("vonage_dtmf")).unwrap().kind,
        EventKind::Dtmf {
            digit: DtmfDigit::Five,
            track: None,
            duration_ms: Some(260)
        }
    );
    assert_eq!(value(codec.clear().unwrap()), json!({"action":"clear"}));
    assert_eq!(
        value(codec.mark("m").unwrap()),
        json!({"action":"notify","payload":{"name":"m"}})
    );
}
#[test]
fn vonage_alternate_dtmf_number_and_nested_formats() {
    let codec = TelephonyCodec::vonage(8000).unwrap();
    for message in [
        json!({"event":"websocket:dtmf","digit":5}),
        json!({"event":"websocket:dtmf","dtmf":{"digit":"5"}}),
    ] {
        assert!(matches!(
            codec.parse(&wire(message)).unwrap().kind,
            EventKind::Dtmf {
                digit: DtmfDigit::Five,
                ..
            }
        ));
    }
}
#[test]
fn exact_provider_outgoing_media_and_clear() {
    let payload = [255, 127, 0];
    let audio = EncodedAudio {
        data: payload.to_vec(),
        encoding: AudioEncoding::Pcmu,
        sample_rate: 8000,
        channels: 1,
    };
    let t = TelephonyCodec::twilio("stream123").unwrap();
    assert_eq!(
        value(t.encode_audio(&audio).unwrap().unwrap()),
        json!({"event":"media","streamSid":"stream123","media":{"payload":"/38A"}})
    );
    assert_eq!(
        value(t.clear().unwrap()),
        json!({"event":"clear","streamSid":"stream123"})
    );
    assert_eq!(
        value(t.mark("m").unwrap()),
        json!({"event":"mark","streamSid":"stream123","mark":{"name":"m"}})
    );
    let p = TelephonyCodec::plivo("stream123").unwrap();
    assert_eq!(
        value(p.encode_audio(&audio).unwrap().unwrap()),
        json!({"event":"playAudio","streamId":"stream123","media":{"contentType":"audio/x-mulaw","sampleRate":8000,"payload":"/38A"}})
    );
    assert_eq!(
        value(p.clear().unwrap()),
        json!({"event":"clearAudio","streamId":"stream123"})
    );
    let n = TelephonyCodec::telnyx("stream123", "PCMU", "PCMU").unwrap();
    assert_eq!(
        value(n.encode_audio(&audio).unwrap().unwrap()),
        json!({"event":"media","media":{"payload":"/38A"}})
    );
    assert_eq!(value(n.clear().unwrap()), json!({"event":"clear"}));
    assert_eq!(
        value(n.mark("m").unwrap()),
        json!({"event":"mark","mark":{"name":"m"}})
    );
}
#[test]
fn pcm_encode_delegates_to_shared_codec() {
    let pcm = [0, 0, 232, 3, 24, 252];
    for codec in [
        TelephonyCodec::twilio("stream123").unwrap(),
        TelephonyCodec::telnyx("stream123", "PCMA", "PCMA").unwrap(),
    ] {
        let msg = codec.encode_pcm16(&pcm, 8000).unwrap().unwrap();
        let EventKind::Media { audio, .. } = codec.parse(&msg).unwrap().kind else {
            panic!()
        };
        assert_eq!(audio.to_pcm16().unwrap().len(), pcm.len());
    }
}
#[test]
fn exotel_raw_pcm_and_clear() {
    let codec = TelephonyCodec::exotel("stream123", 8000).unwrap();
    assert_eq!(
        value(codec.encode_pcm16(&[1, 2, 3, 4], 8000).unwrap().unwrap()),
        json!({"event":"media","stream_sid":"stream123","media":{"payload":"AQIDBA=="}})
    );
    assert_eq!(
        value(codec.clear().unwrap()),
        json!({"event":"clear","stream_sid":"stream123"})
    );
}
#[test]
fn original_malformed_receive_regressions_are_dropped() {
    for codec in codecs() {
        for text in [
            "not json",
            "null",
            "[]",
            "1",
            r#"{"event":"media","media":{}}"#,
            r#"{"foo":"bar"}"#,
            r#"{"event":"media","media":null}"#,
            r#"{"event":"media","media":{"payload":""}}"#,
        ] {
            assert!(
                codec.deserialize(&WireMessage::Text(text.into())).is_none(),
                "{text}"
            );
        }
    }
}
#[test]
fn invalid_base64_does_not_panic_or_advance_session() {
    let mut session = TelephonySession::new(TelephonyCodec::twilio("stream123").unwrap());
    session.accept(&fixture("twilio_start")).unwrap();
    for payload in ["!@#$", "A", "====", "AQ=", "AQIDBA=", "AQID BA=="] {
        assert!(session
            .accept(&wire(
                json!({"event":"media","sequenceNumber":"2","media":{"payload":payload}})
            ))
            .is_err());
        assert_eq!(session.last_sequence(), Some(1));
    }
    session.accept(&fixture("twilio_media")).unwrap();
}
#[test]
fn wrong_stream_and_conflicting_start_identifiers_are_rejected() {
    let codec = TelephonyCodec::twilio("stream123").unwrap();
    for message in [
        json!({"event":"dtmf","streamSid":"other","dtmf":{"digit":"1"}}),
        json!({"event":"start","streamSid":"stream123","start":{"streamSid":"other"}}),
    ] {
        assert!(matches!(
            codec.parse(&wire(message)),
            Err(ProtocolError::StreamMismatch)
        ));
    }
}
#[test]
fn invalid_dtmf_rejected_all_providers() {
    for codec in codecs() {
        for digit in ["", "12", "A", "x", " ", "#*"] {
            assert!(codec
                .parse(&wire(json!({"event":"dtmf","dtmf":{"digit":digit}})))
                .is_err());
        }
    }
    for digit in ["0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "*", "#"] {
        assert!(TelephonyCodec::twilio("s")
            .unwrap()
            .parse(&wire(json!({"event":"dtmf","dtmf":{"digit":digit}})))
            .is_ok());
    }
}
#[test]
fn sequence_monotonic_is_atomic_and_does_not_require_contiguous_numbers() {
    let mut session = TelephonySession::new(TelephonyCodec::twilio("stream123").unwrap());
    session.accept(&fixture("twilio_start")).unwrap();
    assert!(session.accept(&fixture("twilio_start")).is_err());
    assert_eq!(session.state(), StreamState::Started);
    assert_eq!(session.last_sequence(), Some(1));
    session.accept(&fixture("twilio_mark")).unwrap();
    assert!(matches!(
        session.accept(&fixture("twilio_media")),
        Err(ProtocolError::Sequence { .. })
    ));
    assert_eq!(session.last_sequence(), Some(3));
}
#[test]
fn sequence_can_be_explicitly_unchecked() {
    let mut session = TelephonySession::new(TelephonyCodec::twilio("stream123").unwrap())
        .with_sequence_policy(SequencePolicy::Unchecked);
    session.accept(&fixture("twilio_start")).unwrap();
    session.accept(&fixture("twilio_mark")).unwrap();
    session.accept(&fixture("twilio_media")).unwrap();
}
#[test]
fn media_before_start_is_only_rejected_by_session() {
    let codec = TelephonyCodec::twilio("stream123").unwrap();
    assert!(codec.parse(&fixture("twilio_media")).is_ok());
    assert!(TelephonySession::new(codec)
        .accept(&fixture("twilio_media"))
        .is_err());
}
#[test]
fn wrong_codec_and_rate_fail_instead_of_relabeling() {
    assert!(TelephonyCodec::telnyx("s", "OPUS", "PCMU").is_err());
    assert!(TelephonyCodec::vonage(0).is_err());
    let codec = TelephonyCodec::twilio("s").unwrap();
    assert!(matches!(
        codec.encode_pcm16(&[0, 0], 16000),
        Err(ProtocolError::SampleRateMismatch { .. })
    ));
    assert!(codec
        .parse(&wire(
            json!({"event":"start","start":{"streamSid":"s","mediaFormat":{"encoding":"PCMA"}}})
        ))
        .is_err());
}
#[test]
fn odd_pcm_channels_and_empty_audio_are_checked() {
    let codec = TelephonyCodec::vonage(16000).unwrap();
    assert!(codec.parse(&WireMessage::Binary(vec![1])).is_err());
    assert!(codec.encode_pcm16(&[1], 16000).is_err());
    assert_eq!(codec.encode_pcm16(&[], 16000).unwrap(), None);
    assert!(codec
        .encode_audio(&EncodedAudio {
            data: vec![0, 0, 0, 0],
            encoding: AudioEncoding::Pcm16Le,
            sample_rate: 16000,
            channels: 2
        })
        .is_err());
}
#[test]
fn message_sizes_and_invalid_utf8_are_bounded() {
    let codec = TelephonyCodec::twilio("s")
        .unwrap()
        .with_max_message_bytes(20);
    assert!(matches!(
        codec.parse(&WireMessage::Text("x".repeat(21))),
        Err(ProtocolError::MessageTooLarge { .. })
    ));
    assert!(codec.parse(&WireMessage::Binary(vec![255])).is_err());
    assert!(codec
        .transport_message(&json!({"big":"x".repeat(30)}))
        .is_err());
}
#[test]
fn passthrough_preserves_regular_messages_and_filters_rtvi_by_default() {
    for mut codec in codecs() {
        let message = json!({"foo":"urgent","nested":[1,true]});
        assert_eq!(
            value(codec.transport_message(&message).unwrap().unwrap()),
            message
        );
        let rtvi = json!({"label":"rtvi-ai","type":"bot-started-speaking"});
        assert!(codec.transport_message(&rtvi).unwrap().is_none());
        codec.ignore_rtvi_messages = false;
        assert_eq!(
            value(codec.transport_message(&rtvi).unwrap().unwrap()),
            rtvi
        );
    }
}
#[test]
fn unknown_events_preserve_payload_for_forward_compatibility() {
    let codec = TelephonyCodec::twilio("s").unwrap();
    let msg = json!({"event":"future-event","data":{"a":1}});
    assert!(
        matches!(codec.parse(&wire(msg.clone())).unwrap().kind,EventKind::Unknown{message,..} if message==msg)
    );
}
#[test]
fn invalid_numeric_fields_rejected_without_coercion() {
    let codec = TelephonyCodec::twilio("s").unwrap();
    for seq in [
        json!(-1),
        json!(1.2),
        json!("+1"),
        json!(" 1"),
        json!("1e3"),
        json!(true),
        Value::Null,
    ] {
        assert!(codec
            .parse(&wire(
                json!({"event":"dtmf","sequenceNumber":seq,"dtmf":{"digit":"1"}})
            ))
            .is_err());
    }
}
#[test]
fn provider_specific_events_cannot_be_misclassified() {
    let twilio = TelephonyCodec::twilio("s").unwrap();
    assert!(matches!(
        twilio
            .parse(&wire(json!({"event":"websocket:dtmf","digit":"1"})))
            .unwrap()
            .kind,
        EventKind::Unknown { .. }
    ));
    assert!(twilio
        .parse(&wire(
            json!({"event":"dtmf","sequenceNumber":"0","dtmf":{"digit":"1"}})
        ))
        .is_err());
    let vonage = TelephonyCodec::vonage(16000).unwrap();
    assert!(matches!(
        vonage
            .parse(&wire(json!({"event":"media","media":{"payload":"AQI="}})))
            .unwrap()
            .kind,
        EventKind::Unknown { .. }
    ));
}
