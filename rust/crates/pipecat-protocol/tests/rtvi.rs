use pipecat_protocol::rtvi::*;
use pipecat_protocol::{DtmfDigit, ProtocolError};
use serde_json::{json, Value};
fn parse(value: Value) -> Envelope {
    Envelope::parse(value.to_string().as_bytes()).unwrap()
}
fn client(kind: &str, data: Value) -> Result<ClientCommand, ProtocolError> {
    parse(json!({"label":"rtvi-ai","type":kind,"id":"1","data":data})).client_command()
}
#[test]
fn raw_audio_golden_camelcase_and_incremental_ids() {
    let mut codec = RtviClientCodec::default();
    let encoded = codec.encode_audio(&[1, 2, 3, 4], 16000, 1).unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&encoded).unwrap(),
        json!({"label":"rtvi-ai","type":"raw-audio","id":"1","data":{"base64Audio":"AQIDBA==","sampleRate":16000,"numChannels":1}})
    );
    assert_eq!(
        Envelope::parse(codec.encode_audio(&[], 16000, 1).unwrap().as_bytes())
            .unwrap()
            .id
            .as_deref(),
        Some("2")
    );
    let ClientCommand::RawAudio(audio) = Envelope::parse(encoded.as_bytes())
        .unwrap()
        .client_command()
        .unwrap()
    else {
        panic!()
    };
    assert_eq!(audio.decode().unwrap(), [1, 2, 3, 4]);
}
#[test]
fn failed_audio_encode_does_not_consume_id() {
    let mut codec = RtviClientCodec::default();
    assert!(codec.encode_audio(&[1], 16000, 1).is_err());
    assert_eq!(
        Envelope::parse(codec.encode_audio(&[1, 2], 16000, 1).unwrap().as_bytes())
            .unwrap()
            .id
            .as_deref(),
        Some("1")
    );
}
#[test]
fn send_text_golden_and_defaults() {
    let message = parse(
        json!({"label":"rtvi-ai","type":"send-text","id":"1","data":{"content":"hi","options":{"run_immediately":true}}}),
    );
    let ClientCommand::SendText(data) = message.client_command().unwrap() else {
        panic!()
    };
    assert_eq!(data.content, "hi");
    assert_eq!(data.options.unwrap(), SendTextOptions::default());
    let msg = Envelope::client(
        "1",
        &ClientCommand::SendText(SendTextData {
            content: "hello".into(),
            options: None,
        }),
    )
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&msg.encode().unwrap()).unwrap(),
        json!({"label":"rtvi-ai","type":"send-text","id":"1","data":{"content":"hello"}})
    );
}
#[test]
fn client_id_and_label_are_required() {
    assert!(Envelope::parse(br#"{"type":"send-text","id":"1"}"#).is_err());
    assert!(Envelope::parse(br#"{"label":"other","type":"send-text","id":"1"}"#).is_err());
    assert!(
        parse(json!({"label":"rtvi-ai","type":"send-text","data":{"content":"x"}}))
            .client_command()
            .is_err()
    );
    assert!(Envelope::parse(br#"{"label":"rtvi-ai","type":"send-text","id":7}"#).is_err());
}
#[test]
fn unit_server_messages_have_no_id_or_data() {
    for event in [
        ServerEvent::BotLlmStarted,
        ServerEvent::BotLlmStopped,
        ServerEvent::BotTtsStarted,
        ServerEvent::BotTtsStopped,
        ServerEvent::BotStartedSpeaking,
        ServerEvent::BotStoppedSpeaking,
        ServerEvent::BotInterrupted,
        ServerEvent::UserMuteStarted,
        ServerEvent::UserMuteStopped,
    ] {
        let envelope = Envelope::server(&event).unwrap();
        assert!(envelope.id.is_none());
        assert!(envelope.data.is_none());
        assert_eq!(
            Envelope::parse(envelope.encode().unwrap().as_bytes())
                .unwrap()
                .server_event()
                .unwrap(),
            event
        );
    }
}
#[test]
fn llm_marker_golden_matches_original_client_fixture() {
    let msg = parse(
        json!({"label":"rtvi-ai","type":"bot-llm-marker","data":{"text":"◐","kind":"short","raw":"◐ wait","markers":["●","◐","○"]}}),
    );
    let ServerEvent::BotLlmMarker(marker) = msg.server_event().unwrap() else {
        panic!()
    };
    assert_eq!(marker.text, "◐");
    assert_eq!(marker.kind.as_deref(), Some("short"));
    assert_eq!(marker.markers, ["●", "◐", "○"]);
    assert_eq!(
        Envelope::server(&ServerEvent::BotLlmMarker(marker)).unwrap(),
        msg
    );
}
#[test]
fn final_and_interim_transcription_keep_fields() {
    for final_value in [true, false] {
        let msg = parse(
            json!({"label":"rtvi-ai","type":"user-transcription","data":{"text":"hello","user_id":"u","timestamp":"t","final":final_value}}),
        );
        let ServerEvent::UserTranscription(transcription) = msg.server_event().unwrap() else {
            panic!()
        };
        assert_eq!(transcription.is_final, final_value);
        assert_eq!(transcription.user_id, "u");
    }
}
#[test]
fn function_call_report_levels_preserve_omissions() {
    let event = ServerEvent::LlmFunctionCallInProgress(FunctionCallInProgressData {
        tool_call_id: "c1".into(),
        function_name: None,
        arguments: None,
    });
    let encoded =
        serde_json::from_str::<Value>(&Envelope::server(&event).unwrap().encode().unwrap())
            .unwrap();
    assert_eq!(
        encoded,
        json!({"label":"rtvi-ai","type":"llm-function-call-in-progress","data":{"tool_call_id":"c1"}})
    );
    assert_eq!(parse(encoded).server_event().unwrap(), event);
}
#[test]
fn function_call_cancelled_and_result_are_distinct() {
    for cancelled in [true, false] {
        let event=client("llm-function-call-result",json!({"function_name":"weather","tool_call_id":"c1","arguments":{"city":"Paris"},"result":{"temp":18}})).unwrap();
        assert!(matches!(event, ClientCommand::LlmFunctionCallResult(_)));
        let msg = parse(
            json!({"label":"rtvi-ai","type":"llm-function-call-stopped","data":{"tool_call_id":"c1","cancelled":cancelled,"result":{"ok":true}}}),
        );
        let ServerEvent::LlmFunctionCallStopped(data) = msg.server_event().unwrap() else {
            panic!()
        };
        assert_eq!(data.cancelled, cancelled);
    }
}
#[test]
fn ordered_dtmf_requires_nonempty_buttons_array() {
    let ClientCommand::Dtmf(data) = client("dtmf", json!({"buttons":["1","#","0","*"]})).unwrap()
    else {
        panic!()
    };
    assert_eq!(
        data.buttons,
        [
            DtmfDigit::One,
            DtmfDigit::Pound,
            DtmfDigit::Zero,
            DtmfDigit::Star
        ]
    );
    for data in [
        json!({"buttons":[]}),
        json!({"button":"1"}),
        json!({"buttons":["A"]}),
        json!({"buttons":[1]}),
    ] {
        assert!(client("dtmf", data).is_err());
    }
}
#[test]
fn raw_audio_batch_preserves_chunk_boundaries_and_rejects_partial_invalid_batch() {
    let ClientCommand::RawAudioBatch(data) = client(
        "raw-audio-batch",
        json!({"base64AudioBatch":["AQI=","AwQ="],"sampleRate":16000,"numChannels":1}),
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(data.decode().unwrap(), [vec![1, 2], vec![3, 4]]);
    assert!(client(
        "raw-audio-batch",
        json!({"base64AudioBatch":["AQI=","!"],"sampleRate":16000,"numChannels":1})
    )
    .is_err());
}
#[test]
fn invalid_audio_format_and_base64_are_rejected() {
    for data in [
        json!({"base64Audio":"!","sampleRate":16000,"numChannels":1}),
        json!({"base64Audio":"AQ==","sampleRate":16000,"numChannels":1}),
        json!({"base64Audio":"AQI=","sampleRate":0,"numChannels":1}),
        json!({"base64Audio":"AQI=","sampleRate":16000,"numChannels":0}),
        json!({"base64Audio":"AQI=","sampleRate":16000,"numChannels":2}),
        json!({"base64Audio":"AQI=","sample_rate":16000,"num_channels":1}),
    ] {
        assert!(client("raw-audio", data).is_err());
    }
}
#[test]
fn protocol_negotiation_preserves_legacy_and_advises_on_invalid_versions() {
    assert_eq!(PROTOCOL_VERSION, "2.2.0");
    let legacy = negotiate_version(Some("1.4.5"));
    assert!(legacy.legacy);
    assert_eq!(legacy.advertised, "1.4.5");
    assert!(legacy.warning.is_none());
    let current = negotiate_version(Some("2.99.1"));
    assert!(!current.legacy);
    assert_eq!(current.advertised, "2.2.0");
    assert!(current.warning.is_none());
    for version in [
        None,
        Some(""),
        Some("2.0"),
        Some("2.0.0-beta"),
        Some("3.0.0"),
        Some("2.-1.0"),
    ] {
        assert!(negotiate_version(version).warning.is_some());
    }
}
#[test]
fn bot_ready_correlates_request_and_advertises_capabilities() {
    let event = ServerEvent::BotReady(BotReadyData {
        version: PROTOCOL_VERSION.into(),
        about: None,
        capabilities: Some(serde_json::from_value(json!({"audio":{"send":true}})).unwrap()),
    });
    assert!(Envelope::server(&event).is_err());
    let message = Envelope::response("ready-7", &event).unwrap();
    assert_eq!(message.id.as_deref(), Some("ready-7"));
    assert_eq!(message.server_event().unwrap(), event);
}
#[test]
fn file_url_is_preserved_without_fetching_and_bytes_are_validated() {
    let command=client("send-file",json!({"content":"review","file":{"format":"application/pdf","source":{"type":"url","url":"pipecat:file1"}}})).unwrap();
    assert!(
        matches!(command,ClientCommand::SendFile(SendFileData{file:File{source:FileSource::Url{url},..},..}) if url=="pipecat:file1")
    );
    assert!(client(
        "send-file",
        json!({"content":"","file":{"format":"image/png","source":{"type":"bytes","bytes":"!"}}})
    )
    .is_err());
}
#[test]
fn bot_output_v1_and_v2_round_trip_without_added_nulls() {
    for data in [
        json!({"text":"hello","aggregated_by":"sentence","spoken":true}),
        json!({"text":"hello","aggregated_by":"sentence","segment_id":1,"will_be_spoken":true,"spoken_status":"in-progress","spoken_progress":{"accumulated_text":"he","remaining_text":"llo"}}),
    ] {
        let msg = parse(json!({"label":"rtvi-ai","type":"bot-output","data":data}));
        assert_eq!(Envelope::server(&msg.server_event().unwrap()).unwrap(), msg);
    }
}
#[test]
fn ui_snapshot_preserves_forward_compatible_attributes() {
    let data = json!({"tree":{"root":{"ref":"e1","role":"button","name":"Go","platform":"web","children":[{"ref":"e2","role":"text","custom":42}]},"captured_at":1,"viewport":{"width":800}}});
    let command = client("ui-snapshot", data.clone()).unwrap();
    assert_eq!(Envelope::client("1", &command).unwrap().data, Some(data));
}
#[test]
fn ui_job_group_discriminator_and_lifecycle() {
    for data in [
        json!({"kind":"group_started","job_id":"j","workers":["a"],"cancellable":true,"at":1}),
        json!({"kind":"job_update","job_id":"j","worker_name":"a","data":{"percent":10},"at":2}),
        json!({"kind":"job_completed","job_id":"j","worker_name":"a","status":"completed","response":"ok","at":3}),
        json!({"kind":"group_completed","job_id":"j","at":4}),
    ] {
        let message = parse(json!({"label":"rtvi-ai","type":"ui-job-group","data":data}));
        assert_eq!(
            Envelope::server(&message.server_event().unwrap()).unwrap(),
            message
        );
    }
    assert!(parse(
        json!({"label":"rtvi-ai","type":"ui-job-group","data":{"kind":"invented","job_id":"j"}})
    )
    .server_event()
    .is_err());
}
#[test]
fn envelope_limits_and_invalid_json() {
    assert!(matches!(
        Envelope::parse_with_limit(b"{}", 1),
        Err(ProtocolError::MessageTooLarge { .. })
    ));
    for data in [
        "not json",
        "[]",
        "null",
        "{\"label\":\"rtvi-ai\",\"type\":\"\"}",
    ] {
        assert!(Envelope::parse(data.as_bytes()).is_err());
    }
    let codec = RtviClientCodec::default();
    assert!(codec.deserialize(b"bad").is_none());
}
#[test]
fn unknown_envelope_round_trips_but_typed_dispatch_rejects() {
    let msg =
        parse(json!({"label":"rtvi-ai","type":"future-extension","id":"1","data":{"x":true}}));
    assert!(msg.client_command().is_err());
    assert!(msg.server_event().is_err());
    assert_eq!(
        Envelope::parse(msg.encode().unwrap().as_bytes()).unwrap(),
        msg
    );
}
#[test]
fn custom_messages_and_errors_preserve_arbitrary_data() {
    let event = ServerEvent::ServerResponse(CustomMessageData {
        t: "answer".into(),
        d: Some(json!([1, "x", false])),
    });
    let envelope = Envelope::response("1", &event).unwrap();
    assert_eq!(envelope.server_event().unwrap(), event);
    let error = ServerEvent::Error(ErrorData {
        error: "unavailable".into(),
        fatal: false,
    });
    assert_eq!(
        Envelope::server(&error).unwrap().server_event().unwrap(),
        error
    );
}
#[test]
fn client_ready_accepts_legacy_empty_data_and_ignores_invalid_about() {
    let command = parse(json!({"label":"rtvi-ai","type":"client-ready","id":"1"}))
        .client_command()
        .unwrap();
    assert_eq!(
        command,
        ClientCommand::ClientReady(ClientReadyData::default())
    );
    let ClientCommand::ClientReady(data) = client(
        "client-ready",
        json!({"version":"2.2.0","about":{"invalid":42}}),
    )
    .unwrap() else {
        panic!()
    };
    assert_eq!(data.version.as_deref(), Some("2.2.0"));
    assert_eq!(data.about, None);
}
