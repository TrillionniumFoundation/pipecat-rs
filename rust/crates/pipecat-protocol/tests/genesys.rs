use pipecat_core::{AudioRawFrame, Frame, FrameData, KeypadEntry};
use pipecat_protocol::{adapters::FrameSerializer, genesys::*, ProtocolError, WireMessage};
use serde_json::{json, Map, Value};
fn config() -> AudioHookConfig {
    AudioHookConfig {
        pipeline_input_rate: 8000,
        ..Default::default()
    }
}
fn serializer() -> AudioHookSerializer {
    AudioHookSerializer::new("initial", config()).unwrap()
}
fn wire(value: Value) -> WireMessage {
    WireMessage::Text(value.to_string())
}
fn open() -> Value {
    json!({"version":"2","type":"open","seq":1,"id":"test-session-123","parameters":{"conversationId":"conv-456","participant":{"ani":"+1234567890","dnis":"+0987654321"},"media":[{"type":"audio","format":"PCMU","channels":["external"],"rate":8000}],"inputVariables":{"customer_id":"cust-789","queue_name":"billing"}}})
}
fn command(kind: &str, seq: u64, params: Value) -> WireMessage {
    wire(
        json!({"version":"2","type":kind,"seq":seq,"id":"test-session-123","position":"PT10.5S","parameters":params}),
    )
}
fn urgent(frame: Option<Frame>) -> Value {
    let FrameData::OutputTransportMessageUrgent(value) = frame.unwrap().data else {
        panic!()
    };
    value
}
fn output() -> Frame {
    Frame::new(FrameData::OutputAudio(
        AudioRawFrame::new(vec![0, 0, 232, 3], 8000, 1).unwrap(),
    ))
}
#[test]
fn original_open_fixture_builds_exact_response() {
    let mut codec = serializer();
    let value = urgent(codec.try_deserialize(&wire(open())).unwrap());
    assert_eq!(
        value,
        json!({"version":"2","type":"opened","seq":1,"clientseq":1,"id":"test-session-123","parameters":{"startPaused":false,"media":[{"type":"audio","format":"PCMU","channels":["external"],"rate":8000}]}})
    );
    assert_eq!(codec.session_id(), "test-session-123");
    assert_eq!(codec.conversation_id.as_deref(), Some("conv-456"));
    assert!(codec.is_open());
    assert_eq!(codec.participant.as_ref().unwrap()["ani"], "+1234567890");
    assert_eq!(
        codec.input_variables.as_ref().unwrap()["customer_id"],
        "cust-789"
    );
}
#[test]
fn configured_languages_and_start_paused_are_effective() {
    let mut codec = AudioHookSerializer::new(
        "initial",
        AudioHookConfig {
            start_paused: true,
            supported_languages: vec!["es-ES".into(), "en-US".into()],
            selected_language: Some("es-ES".into()),
            ..config()
        },
    )
    .unwrap();
    let response = urgent(codec.try_deserialize(&wire(open())).unwrap());
    assert_eq!(
        response["parameters"]["supportedLanguages"],
        json!(["es-ES", "en-US"])
    );
    assert_eq!(response["parameters"]["selectedLanguage"], "es-ES");
    assert_eq!(response["parameters"]["startPaused"], true);
    assert!(codec.is_paused());
    assert!(codec.serialize(&output()).unwrap().is_none());
    codec.resumed().unwrap();
    assert!(!codec.is_paused());
    assert!(codec.serialize(&output()).unwrap().is_some());
}
#[test]
fn original_ping_position_and_sequence_echo() {
    let mut codec = serializer();
    codec.try_deserialize(&wire(open())).unwrap();
    let response = urgent(
        codec
            .try_deserialize(&command("ping", 5, json!({})))
            .unwrap(),
    );
    assert_eq!(
        response,
        json!({"version":"2","type":"pong","seq":2,"clientseq":5,"id":"test-session-123","position":"PT10.500S","parameters":{}})
    );
    assert_eq!(codec.pong().unwrap().seq, 3);
    assert_eq!(codec.pong().unwrap().seq, 4);
}
#[test]
fn close_returns_output_variables_and_closes_audio_gate() {
    let mut codec = serializer();
    codec.try_deserialize(&wire(open())).unwrap();
    codec.set_output_variables(
        serde_json::from_value(json!({"intent":"support","resolved":true})).unwrap(),
    );
    let response = urgent(
        codec
            .try_deserialize(&command("close", 10, json!({"reason":"disconnect"})))
            .unwrap(),
    );
    assert_eq!(response["type"], "closed");
    assert_eq!(response["parameters"]["outputVariables"]["resolved"], true);
    assert!(!codec.is_open());
    assert!(codec.serialize(&output()).unwrap().is_none());
}
#[test]
fn output_variables_replace_previous_values() {
    let mut codec = serializer();
    codec.set_output_variables(serde_json::from_value(json!({"first":1})).unwrap());
    codec.set_output_variables(serde_json::from_value(json!({"second":2})).unwrap());
    assert!(!codec
        .output_variables
        .as_ref()
        .unwrap()
        .contains_key("first"));
}
#[test]
fn disconnect_barge_in_and_errors_match_wire_shapes() {
    let mut codec = serializer();
    codec.try_deserialize(&wire(open())).unwrap();
    codec.set_output_variables(serde_json::from_value(json!({"result":"success"})).unwrap());
    let disconnect = codec
        .disconnect("completed", "finished", Some("done"))
        .unwrap();
    assert_eq!(disconnect.parameters,serde_json::from_value::<Map<String,Value>>(json!({"reason":"completed","outputVariables":{"action":"finished","result":"success"},"info":"done"})).unwrap());
    let barge = codec.barge_in().unwrap();
    assert_eq!(
        barge.parameters["entities"],
        json!([{"type":"barge_in","data":{}}])
    );
    let error = codec.error(500, "Internal error", true).unwrap();
    assert_eq!(error.parameters["retryable"], true);
    assert_eq!(error.parameters["code"], 500);
}
#[test]
fn pause_resume_and_update_metadata() {
    let mut codec = serializer();
    codec.try_deserialize(&wire(open())).unwrap();
    assert!(codec
        .try_deserialize(&command("pause", 7, json!({"reason":"hold"})))
        .unwrap()
        .is_none());
    assert!(codec.is_paused());
    assert!(codec
        .try_deserialize(&WireMessage::Binary(vec![255]))
        .unwrap()
        .is_none());
    let resumed = codec.resumed().unwrap();
    assert_eq!(resumed.message_type, "resumed");
    assert!(!codec.is_paused());
    codec
        .try_deserialize(&command(
            "update",
            8,
            json!({"participant":{"name":"John Doe"}}),
        ))
        .unwrap();
    assert_eq!(codec.participant.as_ref().unwrap()["name"], "John Doe");
}
#[test]
fn dtmf_buttons_and_invalid_events_do_not_corrupt_sequence() {
    let mut codec = serializer();
    codec.try_deserialize(&wire(open())).unwrap();
    for (seq, digit, expected) in [
        (2, "5", KeypadEntry::Five),
        (3, "*", KeypadEntry::Star),
        (4, "#", KeypadEntry::Pound),
    ] {
        assert!(
            matches!(codec.try_deserialize(&command("dtmf",seq,json!({"digit":digit}))).unwrap().unwrap().data,FrameData::InputDtmf(actual) if actual==expected)
        );
    }
    assert!(codec
        .try_deserialize(&command("dtmf", 5, json!({"digit":"A"})))
        .is_err());
    assert_eq!(codec.client_sequence(), 4);
    assert!(codec
        .deserialize(&command("dtmf", 5, json!({})))
        .unwrap()
        .is_none());
    assert_eq!(codec.client_sequence(), 4);
}
#[test]
fn binary_pcmu_in_and_out_use_shared_codec() {
    let mut codec = serializer();
    assert!(codec.serialize(&output()).unwrap().is_none());
    codec.try_deserialize(&wire(open())).unwrap();
    assert_eq!(
        codec.serialize(&output()).unwrap(),
        Some(WireMessage::Binary(vec![255, 206]))
    );
    let FrameData::InputAudio(audio) = codec
        .try_deserialize(&WireMessage::Binary(vec![255, 206]))
        .unwrap()
        .unwrap()
        .data
    else {
        panic!()
    };
    assert_eq!(audio.audio, [0, 0, 220, 3]);
    assert_eq!(audio.sample_rate, 8000);
}
#[test]
fn negotiated_stereo_extracts_external_channel_and_rejects_partial_pairs() {
    let mut codec = serializer();
    let mut start = open();
    start["parameters"]["media"][0]["channels"] = json!(["external", "internal"]);
    codec.try_deserialize(&wire(start)).unwrap();
    assert_eq!(codec.channel(), AudioHookChannel::Both);
    let FrameData::InputAudio(audio) = codec
        .try_deserialize(&WireMessage::Binary(vec![255, 0, 206, 0]))
        .unwrap()
        .unwrap()
        .data
    else {
        panic!()
    };
    assert_eq!(audio.audio, [0, 0, 220, 3]);
    assert!(codec
        .try_deserialize(&WireMessage::Binary(vec![255, 0, 206]))
        .is_err());
}
#[test]
fn l16_and_mismatched_rate_fail_before_session_mutation() {
    for (field, value) in [("format", json!("L16")), ("rate", json!(16000))] {
        let mut codec = serializer();
        let mut start = open();
        start["parameters"]["media"][0][field] = value;
        assert!(codec.try_deserialize(&wire(start)).is_err());
        assert!(!codec.is_open());
        assert_eq!(codec.client_sequence(), 0);
    }
}
#[test]
fn wrong_id_replays_invalid_json_and_bad_version_rejected() {
    let mut codec = serializer();
    codec.try_deserialize(&wire(open())).unwrap();
    assert!(matches!(
        codec.try_deserialize(&command("ping", 1, json!({}))),
        Err(ProtocolError::Sequence { .. })
    ));
    let mut bad = open();
    bad["type"] = json!("ping");
    bad["seq"] = json!(2);
    bad["id"] = json!("other");
    assert!(matches!(
        codec.try_deserialize(&wire(bad)),
        Err(ProtocolError::StreamMismatch)
    ));
    for text in [
        "bad",
        "[]",
        "null",
        r#"{"version":"1","type":"ping","seq":2,"id":"test-session-123"}"#,
    ] {
        assert!(codec
            .deserialize(&WireMessage::Text(text.into()))
            .unwrap()
            .is_none());
    }
    assert_eq!(codec.client_sequence(), 1);
}
#[test]
fn invalid_positions_and_oversize_messages_rejected() {
    for text in ["10S", "PTNaNS", "PTinfS", "PT-1S", "PT1M", "PTS"] {
        assert!(parse_position(text).is_err());
    }
    assert_eq!(parse_position("PT10.500S").unwrap(), 10.5);
    assert!(ControlMessage::parse(&open().to_string(), 10).is_err());
}
#[test]
fn passthrough_filters_non_audiohook_and_end_emits_disconnect() {
    let mut codec = serializer();
    for payload in [
        json!({"label":"rtvi-ai","type":"bot-ready"}),
        json!({"foo":"bar"}),
    ] {
        assert!(codec
            .serialize(&Frame::new(FrameData::OutputTransportMessage(payload)))
            .unwrap()
            .is_none());
    }
    let payload = json!({"version":"2","type":"pong","custom":true});
    let frame = Frame::new(FrameData::OutputTransportMessageUrgent(payload.clone()));
    let Some(WireMessage::Text(encoded)) = codec.serialize(&frame).unwrap() else {
        panic!()
    };
    assert_eq!(serde_json::from_str::<Value>(&encoded).unwrap(), payload);
    let Some(WireMessage::Text(end)) = codec.serialize(&Frame::end()).unwrap() else {
        panic!()
    };
    assert_eq!(
        serde_json::from_str::<Value>(&end).unwrap()["type"],
        "disconnect"
    );
}
